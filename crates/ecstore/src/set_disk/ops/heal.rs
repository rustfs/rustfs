// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::super::{
    Bytes, CHECK_PART_FILE_CORRUPT, CHECK_PART_SUCCESS, DeleteOptions, DiskError, DiskStore, DriveState, EVENT_SET_DISK_HEAL,
    Error, FileInfo, HashAlgorithm, HashMap, HealDriveInfo, HealItemType, HealOpts, HealResultItem, HealScanMode, Infos,
    LOG_SUBSYSTEM_SET_DISK, ObjectInfo, ObjectOptions, ObjectPartInfo, Path, RUSTFS_META_TMP_BUCKET, ReadOptions, Result,
    SLASH_SEPARATOR, SetDisks, StorageError, Uuid, coding, count_errs, count_part_not_success, create_bitrot_reader,
    create_bitrot_writer, debug, disk, disks_with_all_parts, encode_dir_object, error, file_info_is_valid_for_metadata,
    formats_match_reference_slots, get_format_erasure_in_quorum, get_lock_acquire_timeout, has_suffix,
    heal_bucket_local_on_disks, is_object_dir_dangling, join_all, load_format_erasure_all, path_join_buf, save_format_file,
    should_heal_object_on_disk, stat_all_dirs, to_object_err, warn,
};
use crate::bucket::retirement::MarkerRetirementContext;
use crate::diagnostics::object_lock::ObjectLockAttempt;
use crate::set_disk::{ObjectLockDiagGuard, is_object_lock_diag_enabled};
use std::sync::Arc;

#[cfg(test)]
#[path = "heal_lock_wait_tests.rs"]
mod heal_lock_wait_tests;

#[derive(Clone, Copy)]
struct ExplicitVersionHeal<'a> {
    opts: &'a HealOpts,
    allow_regeneration: bool,
    preserve_unrepairable: bool,
    commit_fence: Option<&'a (dyn Fn() -> bool + Sync)>,
    namespace_owner: Option<&'a Arc<dyn Send + Sync>>,
    retirement: Option<&'a MarkerRetirementContext<'a>>,
}

// Keep the large heal state machine and its construction temporary out of
// each dispatch caller's poll frame. Ordinary object writes do not use this
// heal-only allocation boundary.
#[inline(never)]
fn pin_object_heal_future<F: std::future::Future>(build: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
    Box::pin(build())
}

use crate::disk::DataDirDeleteStatus;
use crate::disk::DiskAPI;
use crate::disk::local::{DELETE_DATA_DIR_MARKER_PREFIX, metadata_less_part_file};
use crate::io_support::bitrot::object_mmap_read_enabled;
use crate::storage_api_contracts::namespace::NamespaceLocking as _;
use rustfs_common::mrf_channel::MrfDeleteMarkerPurge;
use rustfs_common::trace_bus::{TraceEvent, TraceFunc, TraceKind, trace_emit};
use tracing::trace;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_HEAL: &str = "heal";
const EVENT_HEAL_OBJECT_RENAME: &str = "heal_object_rename";
const HEAL_RENAME_INCOMPLETE: &str = "heal rename incomplete";
const READ_REPAIR_DATA_PHASE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60 * 60);

// Observation state never owns a namespace guard. A dropped pending future
// records a censored wait separately from completed acquisitions.
struct HealObjectLockWait {
    started_at: std::time::Instant,
    op: &'static str,
    mode: &'static str,
    result: &'static str,
}

impl Drop for HealObjectLockWait {
    fn drop(&mut self) {
        let elapsed = self.started_at.elapsed();
        metrics::histogram!(
            "rustfs_heal_object_lock_wait_seconds",
            "op" => self.op,
            "mode" => self.mode,
            "result" => self.result,
        )
        .record(elapsed.as_secs_f64());
    }
}

async fn measure_heal_object_lock(
    op: &'static str,
    kind: HealObjectLockKind,
    acquire: impl std::future::Future<Output = std::result::Result<rustfs_lock::NamespaceLockGuard, rustfs_lock::LockError>>,
) -> std::result::Result<rustfs_lock::NamespaceLockGuard, rustfs_lock::LockError> {
    let mut wait = HealObjectLockWait {
        started_at: std::time::Instant::now(),
        op,
        mode: kind.as_str(),
        result: "cancelled",
    };
    let result = acquire.await;
    wait.result = match &result {
        Ok(rustfs_lock::NamespaceLockGuard::Fast(guard)) if guard.is_disabled() => "disabled",
        Ok(_) => "acquired",
        Err(_) => "failed",
    };
    drop(wait);
    result
}

/// Exact set-local absence, established while the object mutation lock is held.
#[derive(Debug)]
pub(crate) struct HealedObjectAbsence {
    pub pool_index: usize,
    pub set_index: usize,
    pub removed: bool,
}

/// Conditions under which a missing object may become a storage-owned proof.
#[derive(Clone, Copy)]
pub(crate) struct AbsenceProofRequest<'a> {
    pub retirement: Option<&'a MarkerRetirementContext<'a>>,
    pub allow_unversioned: bool,
}

fn heal_rollback_backup_dir(response: &disk::RenameDataResp) -> Option<Uuid> {
    // A dedicated cleanup directory identifies a current response whose absent
    // rollback is intentional. Older peers report only old_data_dir.
    response
        .rollback_data_dir
        .or_else(|| response.old_data_dir.filter(|_| response.cleanup_data_dir.is_none()))
}

fn is_unavailable_heal_rpc(error: &DiskError) -> bool {
    let DiskError::Io(error) = error else {
        return false;
    };
    let Some(status) = crate::cluster::rpc::client::embedded_tonic_status(error) else {
        return false;
    };
    // Tonic can report a locally observed HTTP/2 GOAWAY as Internal. Only a
    // typed transport source identifies that case; peer-supplied text cannot.
    status.code() == tonic::Code::Unavailable
        || (matches!(status.code(), tonic::Code::Internal | tonic::Code::Unknown)
            && std::error::Error::source(status).is_some_and(|source| source.is::<tonic::transport::Error>()))
}

fn heal_drive_state_for_error(error: &DiskError) -> DriveState {
    match error {
        DiskError::DiskNotFound | DiskError::RemoteClientUnavailable(_) => DriveState::Offline,
        DiskError::FaultyDisk | DiskError::FaultyRemoteDisk => DriveState::Faulty,
        DiskError::FileNotFound
        | DiskError::FileVersionNotFound
        | DiskError::VolumeNotFound
        | DiskError::PartMissingOrCorrupt
        | DiskError::OutdatedXLMeta => DriveState::Missing,
        DiskError::FileCorrupt => DriveState::Corrupt,
        _ if is_unavailable_heal_rpc(error) => DriveState::Offline,
        _ => DriveState::Unknown(error.to_string()),
    }
}

#[cfg(test)]
static HEAL_RENAME_FAILURES: std::sync::Mutex<Vec<(String, String, usize)>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadRepairPausePhase {
    Data,
    PartStaging(usize),
    Commit,
    IncarnationCommit,
}

#[cfg(test)]
struct ReadRepairPause {
    bucket: String,
    object: String,
    phase: ReadRepairPausePhase,
    arrived: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static READ_REPAIR_PAUSES: std::sync::Mutex<Vec<ReadRepairPause>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) struct ReadRepairPauseScope {
    bucket: String,
    object: String,
    phase: ReadRepairPausePhase,
}

#[cfg(test)]
impl ReadRepairPauseScope {
    fn install(bucket: &str, object: &str) -> (Self, std::sync::Arc<tokio::sync::Notify>, std::sync::Arc<tokio::sync::Notify>) {
        Self::install_at(bucket, object, ReadRepairPausePhase::Commit)
    }

    pub(crate) fn install_incarnation_commit(
        bucket: &str,
        object: &str,
    ) -> (Self, std::sync::Arc<tokio::sync::Notify>, std::sync::Arc<tokio::sync::Notify>) {
        Self::install_at(bucket, object, ReadRepairPausePhase::IncarnationCommit)
    }

    fn install_at(
        bucket: &str,
        object: &str,
        phase: ReadRepairPausePhase,
    ) -> (Self, std::sync::Arc<tokio::sync::Notify>, std::sync::Arc<tokio::sync::Notify>) {
        let arrived = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        READ_REPAIR_PAUSES
            .lock()
            .expect("read-repair commit pause registry should not poison")
            .push(ReadRepairPause {
                bucket: bucket.to_string(),
                object: object.to_string(),
                phase,
                arrived: arrived.clone(),
                release: release.clone(),
            });
        (
            Self {
                bucket: bucket.to_string(),
                object: object.to_string(),
                phase,
            },
            arrived,
            release,
        )
    }
}

#[cfg(test)]
impl Drop for ReadRepairPauseScope {
    fn drop(&mut self) {
        READ_REPAIR_PAUSES
            .lock()
            .expect("read-repair commit pause registry should not poison")
            .retain(|pause| {
                let keep = pause.bucket != self.bucket || pause.object != self.object || pause.phase != self.phase;
                if !keep {
                    pause.release.notify_one();
                }
                keep
            });
    }
}

#[cfg(test)]
async fn pause_read_repair(bucket: &str, object: &str, phase: ReadRepairPausePhase) {
    let pause = READ_REPAIR_PAUSES
        .lock()
        .expect("read-repair commit pause registry should not poison")
        .iter()
        .find(|pause| pause.bucket == bucket && pause.object == object && pause.phase == phase)
        .map(|pause| (pause.arrived.clone(), pause.release.clone()));
    if let Some((arrived, release)) = pause {
        arrived.notify_one();
        release.notified().await;
    }
}

#[cfg(test)]
struct HealRenameFailureScope {
    bucket: String,
    object: String,
}

#[cfg(test)]
impl HealRenameFailureScope {
    fn install(bucket: &str, object: &str, disk_indexes: &[usize]) -> Self {
        let mut failures = HEAL_RENAME_FAILURES
            .lock()
            .expect("heal rename failure registry should not poison");
        assert!(
            !failures
                .iter()
                .any(|(registered_bucket, registered_object, _)| { registered_bucket == bucket && registered_object == object }),
            "heal rename failures must be installed once per object"
        );
        failures.extend(
            disk_indexes
                .iter()
                .map(|index| (bucket.to_string(), object.to_string(), *index)),
        );
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
        }
    }
}

#[cfg(test)]
impl Drop for HealRenameFailureScope {
    fn drop(&mut self) {
        HEAL_RENAME_FAILURES
            .lock()
            .expect("heal rename failure registry should not poison")
            .retain(|(bucket, object, _)| bucket != &self.bucket || object != &self.object);
    }
}

#[cfg(test)]
fn should_fail_heal_rename(bucket: &str, object: &str, disk_index: usize) -> bool {
    let mut failures = HEAL_RENAME_FAILURES
        .lock()
        .expect("heal rename failure registry should not poison");
    if let Some(position) = failures
        .iter()
        .position(|(registered_bucket, registered_object, registered_index)| {
            registered_bucket == bucket && registered_object == object && *registered_index == disk_index
        })
    {
        failures.swap_remove(position);
        true
    } else {
        false
    }
}

#[cfg(not(test))]
fn should_fail_heal_rename(_bucket: &str, _object: &str, _disk_index: usize) -> bool {
    false
}

#[cfg(test)]
struct HealWriterFailure {
    bucket: String,
    object: String,
    part_number: Option<usize>,
    disk_index: usize,
    error: DiskError,
}

#[cfg(test)]
static HEAL_WRITER_FAILURES: std::sync::Mutex<Vec<HealWriterFailure>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
struct HealWriterFailureScope {
    bucket: String,
    object: String,
}

#[cfg(test)]
impl HealWriterFailureScope {
    fn install(bucket: &str, object: &str, part_number: Option<usize>, disk_indexes: &[usize], error: DiskError) -> Self {
        let mut failures = HEAL_WRITER_FAILURES
            .lock()
            .expect("heal writer failure registry should not poison");
        assert!(
            !failures
                .iter()
                .any(|failure| failure.bucket == bucket && failure.object == object),
            "heal writer failures must be installed once per object"
        );
        failures.extend(disk_indexes.iter().map(|index| HealWriterFailure {
            bucket: bucket.to_string(),
            object: object.to_string(),
            part_number,
            disk_index: *index,
            error: error.clone(),
        }));
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
        }
    }
}

#[cfg(test)]
impl Drop for HealWriterFailureScope {
    fn drop(&mut self) {
        HEAL_WRITER_FAILURES
            .lock()
            .expect("heal writer failure registry should not poison")
            .retain(|failure| failure.bucket != self.bucket || failure.object != self.object);
    }
}

#[cfg(test)]
fn injected_heal_writer_error(bucket: &str, object: &str, part_number: usize, disk_index: usize) -> Option<DiskError> {
    let mut failures = HEAL_WRITER_FAILURES
        .lock()
        .expect("heal writer failure registry should not poison");
    failures
        .iter()
        .position(|failure| {
            failure.bucket == bucket
                && failure.object == object
                && failure.disk_index == disk_index
                && failure.part_number.is_none_or(|part| part == part_number)
        })
        .map(|position| failures.swap_remove(position).error)
}

#[cfg(not(test))]
fn injected_heal_writer_error(_bucket: &str, _object: &str, _part_number: usize, _disk_index: usize) -> Option<DiskError> {
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PartFailureSummary {
    part_number: usize,
    failed_shards: usize,
    bitrot_failure: bool,
}

#[derive(Clone)]
struct RecoverableMetaCandidate {
    identity: [u8; 32],
    file_info: FileInfo,
    data_count: usize,
    local_payload: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct ReadRepairCommitFingerprint {
    identity: [u8; 32],
}

impl ReadRepairCommitFingerprint {
    fn from_file_info(fi: &FileInfo) -> Self {
        Self {
            identity: SetDisks::file_info_quorum_hash(fi),
        }
    }
}

#[derive(Clone, Copy)]
enum HealObjectLockKind {
    Read,
    Write,
}

impl HealObjectLockKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DanglingDeleteSafety {
    UnsafeToDelete,
    NoRecoverableCandidate,
}

#[derive(Debug, Default)]
struct MetadataLessDataDirCleanup {
    matched: usize,
    removed: usize,
    first_error: Option<DiskError>,
    delete_errs: Vec<Option<DiskError>>,
    touched_disks: Vec<bool>,
}

#[cfg(test)]
struct DanglingCheckPartsFailure {
    key: DanglingCheckPartsFailureKey,
}

#[cfg(test)]
type DanglingCheckPartsFailureKey = (String, String, usize);

#[cfg(test)]
type DanglingCheckPartsFailures = HashMap<DanglingCheckPartsFailureKey, DiskError>;

#[cfg(test)]
fn dangling_check_parts_failures() -> &'static std::sync::Mutex<DanglingCheckPartsFailures> {
    static FAILURES: std::sync::OnceLock<std::sync::Mutex<DanglingCheckPartsFailures>> = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[cfg(test)]
impl DanglingCheckPartsFailure {
    fn install(bucket: &str, object: &str, disk_index: usize, error: DiskError) -> Self {
        let key = (bucket.to_string(), object.to_string(), disk_index);
        let previous = dangling_check_parts_failures()
            .lock()
            .expect("dangling check-parts failure registry should not poison")
            .insert(key.clone(), error);
        assert!(previous.is_none(), "dangling check-parts failure already installed");
        Self { key }
    }
}

#[cfg(test)]
impl Drop for DanglingCheckPartsFailure {
    fn drop(&mut self) {
        dangling_check_parts_failures()
            .lock()
            .expect("dangling check-parts failure registry should not poison")
            .remove(&self.key);
    }
}

#[cfg(test)]
fn injected_dangling_check_parts_error(bucket: &str, object: &str, disk_index: usize) -> Option<DiskError> {
    dangling_check_parts_failures()
        .lock()
        .expect("dangling check-parts failure registry should not poison")
        .get(&(bucket.to_string(), object.to_string(), disk_index))
        .cloned()
}

#[cfg(test)]
pub(crate) struct DanglingDeleteFailure {
    key: DanglingDeleteFailureKey,
}

#[cfg(test)]
type DanglingDeleteFailureKey = (String, String, usize);

#[cfg(test)]
type DanglingDeleteFailures = HashMap<DanglingDeleteFailureKey, DiskError>;

#[cfg(test)]
fn dangling_delete_failures() -> &'static std::sync::Mutex<DanglingDeleteFailures> {
    static FAILURES: std::sync::OnceLock<std::sync::Mutex<DanglingDeleteFailures>> = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[cfg(test)]
impl DanglingDeleteFailure {
    pub(crate) fn install(bucket: &str, object: &str, disk_index: usize, error: DiskError) -> Self {
        let key = (bucket.to_string(), object.to_string(), disk_index);
        let previous = dangling_delete_failures()
            .lock()
            .expect("dangling delete failure registry should not poison")
            .insert(key.clone(), error);
        assert!(previous.is_none(), "dangling delete failure already installed");
        Self { key }
    }
}

#[cfg(test)]
impl Drop for DanglingDeleteFailure {
    fn drop(&mut self) {
        dangling_delete_failures()
            .lock()
            .expect("dangling delete failure registry should not poison")
            .remove(&self.key);
    }
}

#[cfg(test)]
pub(in crate::set_disk) fn injected_dangling_delete_error(bucket: &str, object: &str, disk_index: usize) -> Option<DiskError> {
    dangling_delete_failures()
        .lock()
        .expect("dangling delete failure registry should not poison")
        .get(&(bucket.to_string(), object.to_string(), disk_index))
        .cloned()
}

#[cfg(not(test))]
fn injected_dangling_delete_error(_bucket: &str, _object: &str, _disk_index: usize) -> Option<DiskError> {
    None
}

fn first_unhealthy_part_summary(
    data_errs_by_part: &HashMap<usize, Vec<usize>>,
    parts: &[ObjectPartInfo],
) -> Option<PartFailureSummary> {
    data_errs_by_part
        .iter()
        .filter_map(|(part_index, part_errs)| {
            let failed_shards = count_part_not_success(part_errs);
            if failed_shards == 0 {
                return None;
            }
            Some((
                *part_index,
                PartFailureSummary {
                    part_number: parts.get(*part_index).map(|part| part.number).unwrap_or(part_index + 1),
                    failed_shards,
                    bitrot_failure: part_errs.contains(&CHECK_PART_FILE_CORRUPT),
                },
            ))
        })
        .min_by_key(|(part_index, _)| *part_index)
        .map(|(_, summary)| summary)
}

fn heal_writer_error_summary(error: &DiskError) -> String {
    match error {
        DiskError::Io(io_error) => format!("io::{:?}", io_error.kind()),
        _ => error.to_string(),
    }
}

/// Whether a writer failure can be retried after the target disk or peer
/// becomes available again.  Keep this decision typed: the aggregate
/// `all_targets_unavailable` path must not turn permanent errors such as
/// `DiskFull` into retryable failures.
fn is_retryable_heal_writer_error(error: &DiskError) -> bool {
    matches!(
        error,
        DiskError::DiskNotFound
            | DiskError::Timeout
            | DiskError::SourceStalled
            | DiskError::FaultyRemoteDisk
            | DiskError::FaultyDisk
            | DiskError::RemoteClientUnavailable(_)
    ) || error.is_retryable_internode_write_failure()
}

struct HealWriterFailures {
    target_count: usize,
    count: usize,
    first: Option<(usize, usize, String)>,
    error: Option<DiskError>,
}

impl HealWriterFailures {
    fn new(target_count: usize) -> Self {
        Self {
            target_count,
            count: 0,
            first: None,
            error: None,
        }
    }

    fn record(&mut self, part_number: usize, disk_index: usize, error: DiskError) {
        self.count += 1;
        if self.first.is_none() {
            self.first = Some((part_number, disk_index, heal_writer_error_summary(&error)));
        }
        // A permanent failure must not be hidden behind an earlier transient
        // target failure when the remaining targets also become unavailable.
        if self
            .error
            .as_ref()
            .is_none_or(|first| is_retryable_heal_writer_error(first) && !is_retryable_heal_writer_error(&error))
        {
            self.error = Some(error);
        }
    }

    fn into_error(self) -> DiskError {
        self.error.unwrap_or(DiskError::ErasureWriteQuorum)
    }

    fn warn(&self, bucket: &str, object: &str, version_id: &str, result: &'static str) {
        let Some((first_part_number, first_disk_index, first_error)) = &self.first else {
            return;
        };
        warn!(
            event = EVENT_SET_DISK_HEAL,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_SET_DISK,
            state = "writer_unavailable",
            result,
            bucket,
            object,
            version_id,
            target_count = self.target_count,
            writer_failure_count = self.count,
            first_part_number,
            first_disk_index,
            error = %first_error,
            "Set disk object heal writer failures"
        );
    }
}

impl SetDisks {
    pub(crate) async fn purge_delete_marker_exact(
        &self,
        bucket: &str,
        object: &str,
        version: Uuid,
        marker: rustfs_filemeta::MetaDeleteMarker,
    ) -> Result<bool> {
        let disks = self.get_disks_internal().await;
        if disks.len() != self.set_drive_count || disks.iter().any(Option::is_none) {
            return Err(StorageError::SlowDown);
        }
        let _object_guard = self.acquire_write_lock_diag("delete_marker_purge", bucket, object).await?;
        let scope = crate::store::bucket_heal_scope(bucket).ok_or(StorageError::PreconditionFailed)?;
        scope.check()?;

        let request = FileInfo {
            volume: bucket.to_owned(),
            name: object.to_owned(),
            version_id: Some(version),
            ..Default::default()
        };
        let results = join_all(disks.iter().enumerate().map(|(disk_index, disk)| {
            let request = request.clone();
            let marker = marker.clone();
            async move {
                if let Some(error) = injected_dangling_delete_error(bucket, object, disk_index) {
                    return Err(error);
                }
                let Some(disk) = disk else { return Err(DiskError::DiskNotFound) };
                disk.delete_version(
                    bucket,
                    object,
                    request,
                    false,
                    DeleteOptions {
                        expected_delete_marker: Some(marker),
                        ..Default::default()
                    },
                )
                .await
            }
        }))
        .await;
        let mut removed = false;
        for result in results {
            match result {
                Ok(()) => removed = true,
                Err(DiskError::FileNotFound | DiskError::FileVersionNotFound) => {}
                Err(error) => return Err(error.into()),
            }
        }

        scope.check()?;
        let metadata_read =
            Self::read_metadata(disks.as_slice(), "", bucket, object, &version.to_string(), false, false, false).await?;
        let all_absent = metadata_read
            .slots
            .iter()
            .all(|slot| matches!(&slot.result, Some(Err(DiskError::FileNotFound | DiskError::FileVersionNotFound))));

        let selected = self.get_disks_internal().await;
        let same_targets = selected.len() == disks.len() && selected.iter().zip(&disks).all(|(current, original)| {
            matches!((current, original), (Some(current), Some(original)) if std::sync::Arc::ptr_eq(current, original))
        });
        if !same_targets || !all_absent {
            return Err(StorageError::SlowDown);
        }
        scope.check()?;
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        Ok(removed)
    }

    /// Read back one healed version from every explicitly admitted replacement
    /// target. This is intentionally separate from the normal heal result: a
    /// successful result describes the transaction attempt, while automatic
    /// replacement completion needs physical evidence that survives a crash
    /// before its checkpoint is persisted.
    pub(crate) async fn replacement_targets_have_version(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        targets: &[String],
    ) -> disk::error::Result<bool> {
        let disks = self.get_disks_internal().await;

        let mut target_disks = Vec::with_capacity(targets.len());

        for target in targets {
            let Some(index) = self.set_endpoints.iter().position(|endpoint| endpoint.to_string() == *target) else {
                return Ok(false);
            };
            let Some(disk) = disks.get(index).and_then(Option::as_ref) else {
                return Ok(false);
            };
            target_disks.push(disk.clone());
        }

        let read_options = ReadOptions {
            incl_free_versions: false,
            read_data: true,
            healing: true,
        };
        let checks = target_disks.into_iter().map(|disk| {
            let task_read_options = read_options;
            async move {
                let file_info = match disk.read_version("", bucket, object, version_id, &task_read_options).await {
                    Ok(file_info) => file_info,
                    Err(
                        DiskError::DiskNotFound
                        | DiskError::VolumeNotFound
                        | DiskError::FileNotFound
                        | DiskError::FileVersionNotFound
                        | DiskError::PathNotFound,
                    ) => return Ok(false),
                    Err(err) => return Err(err),
                };
                if !file_info_is_valid_for_metadata(&file_info) {
                    return Ok(false);
                }
                if !version_id.is_empty() {
                    let Ok(requested_version) = Uuid::parse_str(version_id) else {
                        return Ok(false);
                    };
                    // Treat absent and nil UUID metadata as the same null slot.
                    let actual_version = file_info.version_id.filter(|version_id| !version_id.is_nil());
                    if (requested_version.is_nil() && actual_version.is_some())
                        || (!requested_version.is_nil() && actual_version != Some(requested_version))
                    {
                        return Ok(false);
                    }
                }
                if file_info.is_canonical_delete_marker() || file_info.is_remote() {
                    return Ok(true);
                }
                if (file_info.data.is_some() || file_info.size == 0) && !file_info.parts.is_empty() {
                    return Ok(true);
                }

                let check = match disk.check_parts(bucket, object, &file_info).await {
                    Ok(check) => check,
                    Err(
                        DiskError::DiskNotFound
                        | DiskError::VolumeNotFound
                        | DiskError::FileNotFound
                        | DiskError::FileVersionNotFound
                        | DiskError::PathNotFound,
                    ) => return Ok(false),
                    Err(err) => return Err(err),
                };
                Ok(!check.results.is_empty() && check.results.iter().all(|result| *result == CHECK_PART_SUCCESS))
            }
        });

        Ok(futures::future::try_join_all(checks)
            .await?
            .into_iter()
            .all(|committed| committed))
    }

    #[cfg(test)]
    #[tracing::instrument(level = "trace", skip(self, opts), fields(bucket = %bucket, object = %object, version_id = %version_id))]
    pub(in crate::set_disk) async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> disk::error::Result<(HealResultItem, Option<DiskError>)> {
        Box::pin(self.heal_object_with_explicit_version_regen(
            bucket,
            object,
            version_id,
            ExplicitVersionHeal {
                opts,
                allow_regeneration: true,
                preserve_unrepairable: false,
                commit_fence: None,
                namespace_owner: None,
                retirement: None,
            },
            &mut None,
        ))
        .await
    }

    async fn read_repair_commit_fingerprint(
        &self,
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        version_id: &str,
    ) -> disk::error::Result<ReadRepairCommitFingerprint> {
        let metadata_read = Self::read_metadata_observed(
            disks,
            "",
            bucket,
            object,
            version_id,
            true,
            false,
            false,
            true,
            self.default_parity_count,
        );
        #[cfg(feature = "diagnostic-read-version-caller")]
        let metadata_read = crate::cluster::rpc::runtime_sources::with_read_version_request_caller_if_absent(
            rustfs_io_metrics::internode_metrics::ReadVersionRequestCaller::Heal,
            metadata_read,
        )
        .await?;
        #[cfg(not(feature = "diagnostic-read-version-caller"))]
        let metadata_read = metadata_read.await?;
        let (parts_metadata, errs, _) = metadata_read.into_legacy();
        let (read_quorum, _) = Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count)?;
        let read_quorum = usize::try_from(read_quorum).map_err(|_| DiskError::ErasureReadQuorum)?;
        let (_, quorum_mod_time, quorum_etag) = Self::list_online_disks(disks, &parts_metadata, &errs, read_quorum);
        let mut latest_meta = Self::pick_valid_fileinfo(&parts_metadata, quorum_mod_time, quorum_etag, read_quorum)?;
        Self::hydrate_selected_fileinfo_part_checksums(&mut latest_meta)?;
        Ok(ReadRepairCommitFingerprint::from_file_info(&latest_meta))
    }

    async fn acquire_heal_object_lock(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        kind: HealObjectLockKind,
        op: &'static str,
    ) -> disk::error::Result<ObjectLockDiagGuard> {
        let ns_lock = self
            .new_ns_lock(bucket, object)
            .await
            .map_err(|e| e.narrow_to_disk().unwrap_or_else(DiskError::other))?;
        let timeout = get_lock_acquire_timeout();
        let mut attempt = ObjectLockAttempt::start(op, bucket, object, Some(version_id), &ns_lock, kind.as_str(), timeout);
        let lock_result = measure_heal_object_lock(op, kind, async {
            match kind {
                HealObjectLockKind::Read => ns_lock.get_read_lock(timeout).await,
                HealObjectLockKind::Write => ns_lock.get_write_lock(timeout).await,
            }
        })
        .await;
        attempt.observe(&lock_result);
        let guard = lock_result.map_err(|e| {
            self.map_namespace_lock_error(bucket, object, kind.as_str(), e)
                .narrow_to_disk()
                .unwrap_or_else(DiskError::other)
        })?;
        let enabled = is_object_lock_diag_enabled();
        Ok(ObjectLockDiagGuard::new(
            guard,
            enabled,
            op,
            enabled.then(|| bucket.to_owned()),
            enabled.then(|| object.to_owned()),
            enabled.then(|| ns_lock.owner().to_owned()),
            kind.as_str(),
        )
        .with_attempt(attempt))
    }

    async fn acquire_revalidated_read_repair_commit_lock(
        &self,
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        version_id: &str,
        expected_fingerprint: ReadRepairCommitFingerprint,
    ) -> disk::error::Result<Option<ObjectLockDiagGuard>> {
        #[cfg(test)]
        pause_read_repair(bucket, object, ReadRepairPausePhase::Commit).await;

        let write_lock_wait_start = std::time::Instant::now();
        let guard = self
            .acquire_heal_object_lock(bucket, object, version_id, HealObjectLockKind::Write, "read_repair_commit")
            .await?;
        let write_lock_wait_ms = u64::try_from(write_lock_wait_start.elapsed().as_millis()).unwrap_or(u64::MAX);

        let revalidation_start = std::time::Instant::now();
        let current_fingerprint = self.read_repair_commit_fingerprint(disks, bucket, object, version_id).await?;
        let revalidation_duration_ms = u64::try_from(revalidation_start.elapsed().as_millis()).unwrap_or(u64::MAX);

        if current_fingerprint == expected_fingerprint {
            debug!(
                event = EVENT_SET_DISK_HEAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                state = "read_repair_commit_revalidated",
                bucket,
                object,
                version_id,
                write_lock_wait_ms,
                revalidation_duration_ms,
                "Read-repair heal revalidated metadata before rename"
            );
            Ok(Some(guard))
        } else {
            warn!(
                event = EVENT_SET_DISK_HEAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                state = "read_repair_commit_stale",
                bucket,
                object,
                version_id,
                write_lock_wait_ms,
                revalidation_duration_ms,
                "Read-repair heal abandoned stale metadata before rename"
            );
            Ok(None)
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn heal_object_with_explicit_version_regen(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        run: ExplicitVersionHeal<'_>,
        absence: &mut Option<HealedObjectAbsence>,
    ) -> disk::error::Result<(HealResultItem, Option<DiskError>)> {
        let ExplicitVersionHeal {
            opts,
            allow_regeneration: allow_explicit_version_regen,
            preserve_unrepairable,
            commit_fence,
            namespace_owner,
            retirement,
        } = run;
        trace!(
            event = EVENT_SET_DISK_HEAL,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_SET_DISK,
            scan_mode = %opts.scan_mode.as_str(),
            dry_run = opts.dry_run,
            remove = opts.remove,
            state = "started",
            "Set disk object heal started"
        );

        let disks = self.get_disks_internal().await;
        let bucket_heal_scope = crate::store::bucket_heal_scope_for_object(bucket, object);
        if let Some(scope) = &bucket_heal_scope {
            scope.check()?;
        }
        if commit_fence.is_some_and(|check| !check()) {
            return Err(DiskError::DiskOngoingReq);
        }

        let mut result = HealResultItem {
            heal_item_type: HealItemType::Object.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.to_string(),
            disk_count: disks.len(),
            ..Default::default()
        };

        let read_repair_uses_shared_lock = opts.read_repair && !opts.no_lock;
        // Read-repair heals are triggered by successful reads of still-readable
        // objects. Keep readers flowing during the long reconstruction phase;
        // take the write lock only for the final verify-and-rename fence.
        let mut _read_repair_read_lock_guard = if read_repair_uses_shared_lock {
            let guard = self
                .acquire_heal_object_lock(bucket, object, version_id, HealObjectLockKind::Read, "read_repair_data")
                .await?;
            debug!(
                event = EVENT_SET_DISK_HEAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                state = "read_repair_shared_data_lock",
                bucket,
                object,
                version_id,
                "Read-repair heal using shared namespace lock for data phase"
            );
            Some(guard)
        } else {
            None
        };

        #[cfg(test)]
        if read_repair_uses_shared_lock {
            pause_read_repair(bucket, object, ReadRepairPausePhase::Data).await;
        }

        // Bound, not `_`: this guard must live to the end of the scope. A bare
        // `_` would drop it here and release the namespace write lock.
        let _write_lock_guard = if !opts.no_lock && !read_repair_uses_shared_lock {
            Some(Arc::new(
                self.acquire_heal_object_lock(bucket, object, version_id, HealObjectLockKind::Write, "heal_object")
                    .await?,
            ))
        } else {
            None
        };

        let version_id_op = {
            if version_id.is_empty() {
                None
            } else {
                Some(version_id.to_string())
            }
        };

        let (mut parts_metadata, errs, _) = Self::read_metadata(&disks, "", bucket, object, version_id, true, true, false)
            .await?
            .into_legacy();

        trace!(
            event = EVENT_SET_DISK_HEAL,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_SET_DISK,
            parts_count = parts_metadata.len(),
            error_count = errs.iter().flatten().count(),
            state = "metadata_read",
            "Set disk object metadata read"
        );
        if DiskError::is_all_not_found(&errs) {
            debug!(
                event = EVENT_SET_DISK_HEAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket,
                object,
                version_id,
                state = "missing_object_skipped",
                "Set disk heal skipped missing object"
            );
            let err = if !version_id.is_empty() {
                DiskError::FileVersionNotFound
            } else {
                DiskError::FileNotFound
            };
            if version_id.is_empty()
                && (opts.remove || opts.dry_run)
                && let Some(cleanup) = self
                    .cleanup_metadata_less_data_dirs(bucket, object, &disks, opts.dry_run)
                    .await?
            {
                let mut result = self
                    .metadata_less_data_dir_heal_result(bucket, object, &cleanup, opts.dry_run)
                    .await;
                result.detail = format!(
                    "metadata-less data directories matched={}, removed={}, dry_run={}",
                    cleanup.matched, cleanup.removed, opts.dry_run
                );
                return Ok((result, cleanup.first_error.or(Some(err))));
            }
            // Nothing to do, file is already gone.
            return Ok((
                self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                    .await,
                Some(err),
            ));
        }

        trace!(
            event = EVENT_SET_DISK_HEAL,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_SET_DISK,
            parts_count = parts_metadata.len(),
            state = "quorum_check",
            "Set disk object quorum check started"
        );
        match Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count) {
            Ok((read_quorum, _)) => {
                result.parity_blocks = result.disk_count - read_quorum as usize;
                result.data_blocks = read_quorum as usize;

                let ((quorum_disks, quorum_mod_time, quorum_etag), disk_len) = {
                    let disks = self.disks.read().await;
                    let disk_len = disks.len();
                    (Self::list_online_disks(&disks, &parts_metadata, &errs, read_quorum as usize), disk_len)
                };

                // A deep heal must inspect every reachable member, not only the
                // quorum that agreed on the canonical metadata. A returning disk
                // can legitimately carry an older xl.meta (or an explicit-version
                // absence) while still being the exact stale target that needs
                // repair. Restricting the repair set to quorum members silently
                // turns that stale member into `verified_healthy`/`unchanged`
                // (backlog#2612, #2596). Normal scans retain the quorum fast path.
                let mut online_disks = if opts.scan_mode == HealScanMode::Deep {
                    self.get_disks_internal().await
                } else {
                    quorum_disks
                };

                trace!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    metadata_count = parts_metadata.len(),
                    error_count = errs.iter().flatten().count(),
                    read_quorum,
                    disk_count = disk_len,
                    online_disk_count = online_disks.iter().flatten().count(),
                    state = "disk_metadata_resolved",
                    "Set disk object metadata resolved"
                );

                let filter_by_etag = quorum_etag.is_some();
                match Self::pick_valid_fileinfo(&parts_metadata, quorum_mod_time, quorum_etag.clone(), read_quorum as usize) {
                    Ok(mut latest_meta) => {
                        Self::hydrate_selected_fileinfo_part_checksums(&mut latest_meta)?;
                        let protected =
                            !latest_meta.parts.is_empty() && latest_meta.parts.iter().all(|part| part.integrity.is_some());
                        result.integrity_verified = protected
                            && !latest_meta.deleted
                            && !latest_meta.is_remote()
                            && opts.scan_mode == HealScanMode::Deep
                            && !read_repair_uses_shared_lock;
                        trace!(
                            event = EVENT_SET_DISK_HEAL,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_SET_DISK,
                            deleted = latest_meta.deleted,
                            remote = latest_meta.is_remote(),
                            inline = latest_meta.inline_data(),
                            part_count = latest_meta.parts.len(),
                            data_shards = latest_meta.erasure.data_blocks,
                            parity_shards = latest_meta.erasure.parity_blocks,
                            state = "canonical_metadata_selected",
                            "Set disk canonical object metadata selected"
                        );

                        let (data_errs_by_disk, data_errs_by_part) = disks_with_all_parts(
                            &mut online_disks,
                            &mut parts_metadata,
                            &errs,
                            &latest_meta,
                            filter_by_etag,
                            bucket,
                            object,
                            opts.scan_mode,
                        )
                        .await?;

                        trace!(
                            event = EVENT_SET_DISK_HEAL,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_SET_DISK,
                            available_disk_count = online_disks.iter().flatten().count(),
                            disk_count = online_disks.len(),
                            state = "parts_checked",
                            "Set disk object parts checked"
                        );

                        let erasure = if !latest_meta.deleted && !latest_meta.is_remote() {
                            // Initialize erasure coding; use legacy mode for old-version files
                            coding::Erasure::try_new_with_options(
                                latest_meta.erasure.data_blocks,
                                latest_meta.erasure.parity_blocks,
                                latest_meta.erasure.block_size,
                                latest_meta.uses_legacy_checksum,
                            )
                            .map_err(DiskError::from)?
                        } else {
                            coding::Erasure::default()
                        };

                        result.object_size =
                            ObjectInfo::from_file_info(&latest_meta, bucket, object, true).get_actual_size()? as usize;
                        result.resolved_version_id = Some(*latest_meta.version_id.unwrap_or_default().as_bytes());
                        result.quorum_selected_data_dir = selected_heal_data_dir(&latest_meta, opts);
                        // Loop to find number of disks with valid data, per-drive
                        // data state and a list of outdated disks on which data needs
                        // to be healed.
                        let mut out_dated_disks = vec![None; disk_len];
                        let mut disks_to_heal_count = 0;
                        let mut meta_to_heal_count = 0;

                        for index in 0..online_disks.len() {
                            let (yes, is_meta, reason) = should_heal_object_on_disk(
                                &errs[index],
                                &data_errs_by_disk[&index],
                                &parts_metadata[index],
                                &latest_meta,
                            );

                            if yes {
                                out_dated_disks[index] = disks[index].clone();
                                disks_to_heal_count += 1;
                                if is_meta {
                                    meta_to_heal_count += 1;
                                }
                                debug!(
                                    event = EVENT_SET_DISK_HEAL,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                                    bucket,
                                    object,
                                    version_id,
                                    disk_index = index,
                                    endpoint = %self.set_endpoints[index],
                                    state = "disk_marked_for_healing",
                                    "Set disk marked for healing"
                                );
                            }

                            let drive_state = match reason {
                                Some(err) => heal_drive_state_for_error(&err).to_string(),
                                None => DriveState::Ok.to_string(),
                            };
                            result.before.drives.push(HealDriveInfo {
                                uuid: "".to_string(),
                                endpoint: self.set_endpoints[index].to_string(),
                                state: drive_state.to_string(),
                            });

                            result.after.drives.push(HealDriveInfo {
                                uuid: "".to_string(),
                                endpoint: self.set_endpoints[index].to_string(),
                                state: drive_state.to_string(),
                            });
                        }

                        let requested_nil =
                            version_id.is_empty() || Uuid::parse_str(version_id).is_ok_and(|requested| requested.is_nil());
                        let selected_nil = latest_meta.version_id.is_none_or(|selected| selected.is_nil());
                        let selected_version_matches = if requested_nil {
                            selected_nil
                        } else {
                            latest_meta
                                .version_id
                                .is_some_and(|selected| selected.to_string().eq_ignore_ascii_case(version_id))
                        };
                        result.metadata_verified = !opts.dry_run
                            && !latest_meta.is_remote()
                            && !read_repair_uses_shared_lock
                            && selected_version_matches
                            && (protected || latest_meta.deleted)
                            && !result.after.drives.is_empty()
                            && result
                                .after
                                .drives
                                .iter()
                                .all(|drive| drive.state == DriveState::Ok.to_string());

                        if !latest_meta.deleted && !latest_meta.is_remote() && !protected {
                            result.legacy_identity_unverified = true;
                            result.detail =
                                rustfs_heal_contracts::heal_channel::LEGACY_OBJECT_IDENTITY_UNVERIFIED_DETAIL.to_owned();
                        }

                        if disks_to_heal_count == 0 {
                            if result.integrity_verified && !opts.dry_run {
                                match crate::io_support::shard_integrity::restore_proof_replicas(
                                    &latest_meta,
                                    &disks,
                                    bucket,
                                    object,
                                )
                                .await
                                {
                                    Ok(repaired) if repaired > 0 => {
                                        result.detail = format!("Restored {repaired} independent integrity indexes")
                                    }
                                    Ok(_) => {}
                                    Err(error) => {
                                        result.integrity_verified = false;
                                        return Ok((result, Some(error)));
                                    }
                                }
                            }
                            // The object is already healthy: no disk needs healing.
                            // This is the common case for the very objects PR #4356
                            // targets — a valid `xl.meta` plus a leaked pre-#3510
                            // data dir needs no shard healing, so it would otherwise
                            // return here and never reach the post-heal reclaim tail
                            // below. Sweep the strays on this path too (issues #3231,
                            // #3191). Skipped on dry-run, like every mutating step.
                            if !opts.dry_run && !preserve_unrepairable {
                                self.reclaim_orphan_data_dirs_best_effort(bucket, object).await;
                            }
                            return Ok((result, None));
                        }

                        if opts.dry_run {
                            return Ok((result, None));
                        }

                        let mut cannot_heal = !latest_meta.deleted && meta_to_heal_count > latest_meta.erasure.parity_blocks;
                        if cannot_heal && quorum_etag.is_some() {
                            cannot_heal = false;
                        }

                        if !latest_meta.deleted && !latest_meta.is_remote() {
                            for part_errs in data_errs_by_part.values() {
                                if count_part_not_success(part_errs) > latest_meta.erasure.parity_blocks {
                                    cannot_heal = true;
                                    break;
                                }
                            }
                        }

                        if cannot_heal {
                            let total_disks = parts_metadata.len();
                            let healthy_count = total_disks.saturating_sub(disks_to_heal_count);
                            let required_data = total_disks.saturating_sub(latest_meta.erasure.parity_blocks);
                            let no_parity_failure =
                                (!latest_meta.deleted && !latest_meta.is_remote() && latest_meta.erasure.parity_blocks == 0)
                                    .then(|| first_unhealthy_part_summary(&data_errs_by_part, &latest_meta.parts))
                                    .flatten();
                            let cannot_heal_err = if no_parity_failure.is_some_and(|failure| failure.bitrot_failure) {
                                DiskError::FileCorrupt
                            } else {
                                DiskError::ErasureReadQuorum
                            };

                            if let Some(failure) = no_parity_failure {
                                result.detail = format!(
                                    "no-parity object is unrecoverable: part {} has {} missing or corrupt data shard(s), bitrot_failure={}, data_blocks={}, parity_blocks=0",
                                    failure.part_number,
                                    failure.failed_shards,
                                    failure.bitrot_failure,
                                    latest_meta.erasure.data_blocks
                                );
                                error!(
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_HEAL,
                                    bucket,
                                    object,
                                    version_id,
                                    data_shards = latest_meta.erasure.data_blocks,
                                    parity_shards = latest_meta.erasure.parity_blocks,
                                    required_data_shards = required_data,
                                    healthy_shards = healthy_count,
                                    missing_or_corrupt_shards = disks_to_heal_count,
                                    part_number = failure.part_number,
                                    bitrot_failure = failure.bitrot_failure,
                                    "No-parity object failed integrity or availability validation and cannot be reconstructed"
                                );
                            } else {
                                result.detail = format!(
                                    "object cannot be reconstructed with available shards: required_data_shards={required_data}, healthy_shards={healthy_count}, missing_or_corrupt_shards={disks_to_heal_count}, parity_shards={}",
                                    latest_meta.erasure.parity_blocks
                                );
                                error!(
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_HEAL,
                                    bucket,
                                    object,
                                    version_id,
                                    required_data_shards = required_data,
                                    healthy_shards = healthy_count,
                                    missing_or_corrupt_shards = disks_to_heal_count,
                                    parity_shards = latest_meta.erasure.parity_blocks,
                                    "Heal object cannot reconstruct with available shards"
                                );
                            }

                            // A no-remove request cannot turn surviving data into dangling
                            // cleanup or its grace deferral.
                            if read_repair_uses_shared_lock || preserve_unrepairable || !opts.remove {
                                return Ok((result, Some(cannot_heal_err)));
                            }

                            // `disks_with_all_parts` normalizes conflicting entries
                            // in `parts_metadata` to defaults. Re-read only before
                            // destructive cleanup so the guard sees every original
                            // identity.
                            let (delete_guard_metadata, delete_guard_errs, _) =
                                Self::read_metadata(&disks, "", bucket, object, version_id, true, true, false)
                                    .await?
                                    .into_legacy();
                            if self
                                .dangling_delete_safety(bucket, object, &delete_guard_metadata, &delete_guard_errs, &disks)
                                .await?
                                == DanglingDeleteSafety::UnsafeToDelete
                            {
                                return Ok((result, Some(cannot_heal_err)));
                            }

                            // Allow for dangling deletes, on versions that have DataDir missing etc.
                            // this would end up restoring the correct readable versions.
                            return match self
                                .delete_if_dangling_with_proof(
                                    bucket,
                                    object,
                                    &parts_metadata,
                                    &errs,
                                    &data_errs_by_part,
                                    ObjectOptions {
                                        version_id: version_id_op.clone(),
                                        ..Default::default()
                                    },
                                )
                                .await
                            {
                                Ok((m, absent)) => {
                                    if absent {
                                        *absence = Some(HealedObjectAbsence {
                                            pool_index: self.pool_index,
                                            set_index: self.set_index,
                                            removed: true,
                                        });
                                    }
                                    Ok((
                                        self.dangling_heal_result(m, &errs, bucket, object, version_id, absent).await,
                                        (!absent).then_some(DiskError::ErasureWriteQuorum),
                                    ))
                                }
                                Err(err) => {
                                    error!(
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_HEAL,
                                        bucket,
                                        object,
                                        version_id,
                                        error = %err,
                                        "Heal object dangling cleanup could not prove object deletion"
                                    );
                                    Ok((result, Some(err)))
                                }
                            };
                        }

                        if !latest_meta.deleted && latest_meta.erasure.distribution.len() != online_disks.len() {
                            let distribution_len = latest_meta.erasure.distribution.len();
                            let disk_slot_count = online_disks.len();
                            let err_str = format!(
                                "unexpected file distribution length {distribution_len} for {disk_slot_count} disk slots; backend disks may have been manually modified; refusing to heal {bucket}/{object}({version_id})"
                            );
                            warn!(
                                event = EVENT_SET_DISK_HEAL,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_SET_DISK,
                                bucket,
                                object,
                                version_id,
                                distribution_len,
                                disk_slot_count,
                                state = "invalid_distribution",
                                "Set disk object heal refused due to invalid erasure distribution"
                            );
                            let err = DiskError::other(err_str);
                            return Ok((
                                self.default_heal_result(latest_meta, &errs, bucket, object, version_id).await,
                                Some(err),
                            ));
                        }

                        let latest_disks = Self::shuffle_disks(&online_disks, &latest_meta.erasure.distribution);
                        if !latest_meta.deleted && latest_meta.erasure.distribution.len() != out_dated_disks.len() {
                            let distribution_len = latest_meta.erasure.distribution.len();
                            let disk_slot_count = out_dated_disks.len();
                            let err_str = format!(
                                "unexpected file distribution length {distribution_len} for {disk_slot_count} disk slots; backend disks may have been manually modified; refusing to heal {bucket}/{object}({version_id})"
                            );
                            warn!(
                                event = EVENT_SET_DISK_HEAL,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_SET_DISK,
                                bucket,
                                object,
                                version_id,
                                distribution_len,
                                disk_slot_count,
                                state = "invalid_distribution",
                                "Set disk object heal refused due to invalid erasure distribution"
                            );
                            let err = DiskError::other(err_str);
                            return Ok((
                                self.default_heal_result(latest_meta, &errs, bucket, object, version_id).await,
                                Some(err),
                            ));
                        }

                        if !latest_meta.deleted && latest_meta.erasure.distribution.len() != parts_metadata.len() {
                            let distribution_len = latest_meta.erasure.distribution.len();
                            let metadata_count = parts_metadata.len();
                            let err_str = format!(
                                "unexpected file distribution length {distribution_len} for {metadata_count} metadata entries; backend disks may have been manually modified; refusing to heal {bucket}/{object}({version_id})"
                            );
                            warn!(
                                event = EVENT_SET_DISK_HEAL,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_SET_DISK,
                                bucket,
                                object,
                                version_id,
                                distribution_len,
                                metadata_count,
                                state = "invalid_distribution",
                                "Set disk object heal refused due to invalid erasure distribution"
                            );
                            let err = DiskError::other(err_str);
                            return Ok((
                                self.default_heal_result(latest_meta, &errs, bucket, object, version_id).await,
                                Some(err),
                            ));
                        }

                        // `errs` is reported in physical-disk order while the
                        // reconstruction loop below uses the shard order from
                        // the selected metadata. Preserve which targets are
                        // missing their bucket volume across that permutation.
                        let mut missing_bucket_volumes = vec![false; out_dated_disks.len()];
                        for (physical_index, error) in errs.iter().enumerate() {
                            if error.as_ref() != Some(&DiskError::VolumeNotFound) {
                                continue;
                            }
                            let shard_index = latest_meta
                                .erasure
                                .distribution
                                .get(physical_index)
                                .and_then(|index| index.checked_sub(1))
                                .unwrap_or(physical_index);
                            if let Some(target) = missing_bucket_volumes.get_mut(shard_index) {
                                *target = true;
                            }
                        }

                        out_dated_disks = Self::shuffle_disks(&out_dated_disks, &latest_meta.erasure.distribution);
                        let mut parts_metadata = Self::shuffle_parts_metadata(&parts_metadata, &latest_meta.erasure.distribution);
                        let mut copy_parts_metadata = vec![None; parts_metadata.len()];
                        for (index, disk) in latest_disks.iter().enumerate() {
                            if disk.is_some() {
                                copy_parts_metadata[index] = Some(parts_metadata[index].clone());
                            }
                        }

                        let clean_file_info = |fi: &FileInfo| -> FileInfo {
                            let mut nfi = fi.clone();
                            if !nfi.is_remote() {
                                nfi.data = None;
                                nfi.erasure.index = 0;
                                nfi.erasure.checksums = Vec::new();
                            }
                            nfi
                        };
                        for (index, disk) in out_dated_disks.iter().enumerate() {
                            if disk.is_some() {
                                // Make sure to write the FileInfo information
                                // that is expected to be in quorum.
                                parts_metadata[index] = clean_file_info(&latest_meta);
                            }
                        }

                        // A missing bucket volume is a repairable object target,
                        // but the final rename cannot create its parent volume.
                        // Recreate only the affected volumes after quorum and
                        // lifecycle checks have passed. Keep failures visible so
                        // a partial repair cannot be reported as successful.
                        let mut volume_creation_error = None;
                        for (index, outdated_disk) in out_dated_disks.iter_mut().enumerate() {
                            if !missing_bucket_volumes.get(index).copied().unwrap_or(false) || outdated_disk.is_none() {
                                continue;
                            }
                            if let Some(scope) = &bucket_heal_scope {
                                scope.check()?;
                            }
                            if commit_fence.is_some_and(|check| !check()) {
                                return Err(DiskError::DiskOngoingReq);
                            }
                            let disk = outdated_disk.as_ref().expect("checked above").clone();
                            let endpoint = disk.endpoint().to_string();
                            match disk.make_volume(bucket).await {
                                Ok(()) | Err(DiskError::VolumeExists) => {}
                                Err(error) => {
                                    warn!(
                                        event = EVENT_SET_DISK_HEAL,
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_HEAL,
                                        bucket,
                                        object,
                                        version_id,
                                        disk_index = index,
                                        endpoint = %endpoint,
                                        error = %error,
                                        state = "bucket_volume_recreate_failed",
                                        "Heal object could not recreate the target bucket volume"
                                    );
                                    if volume_creation_error.is_none() {
                                        volume_creation_error = Some(error.clone());
                                    }
                                    *outdated_disk = None;
                                    disks_to_heal_count = disks_to_heal_count.saturating_sub(1);
                                    for drive in &mut result.after.drives {
                                        if drive.endpoint == endpoint {
                                            drive.state = heal_drive_state_for_error(&error).to_string();
                                        }
                                    }
                                }
                            }
                        }

                        if let Some(scope) = &bucket_heal_scope {
                            scope.check()?;
                        }
                        if commit_fence.is_some_and(|check| !check()) {
                            return Err(DiskError::DiskOngoingReq);
                        }
                        if disks_to_heal_count == 0 {
                            return Ok((result, volume_creation_error.or(Some(DiskError::ErasureWriteQuorum))));
                        }

                        // We write at temporary location and then rename to final location.
                        let tmp_id = Uuid::new_v4().to_string();
                        // Delete markers and remote (transitioned) objects carry no data_dir and
                        // skip the data-heal block below, so a nil placeholder is safe for them.
                        // For a regular object a missing data_dir means the latest metadata is
                        // corrupt; fail this object's heal with a clear error instead of building
                        // part paths under a nil UUID directory.
                        let read_repair_commit_fingerprint =
                            read_repair_uses_shared_lock.then(|| ReadRepairCommitFingerprint::from_file_info(&latest_meta));

                        let data_dir = match latest_meta.data_dir {
                            Some(data_dir) => data_dir,
                            None => {
                                if !latest_meta.deleted && !latest_meta.is_remote() {
                                    error!(
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_HEAL,
                                        bucket,
                                        object,
                                        version_id,
                                        "Heal object latest metadata has no data_dir, cannot heal object data"
                                    );
                                    return Err(DiskError::FileCorrupt);
                                }
                                Uuid::nil()
                            }
                        };
                        let src_data_dir = data_dir.to_string();
                        let dst_data_dir = data_dir;

                        if !latest_meta.deleted && !latest_meta.is_remote() {
                            let stage_result: disk::error::Result<Option<DiskError>> = async {
                                let erasure_info = latest_meta.erasure.clone();
                                let mut writer_failures = HealWriterFailures::new(disks_to_heal_count);

                                for (part_index, part) in latest_meta.parts.iter().enumerate() {
                                    use crate::io_support::shard_integrity::{PartProofReader, PreparedIntegrity, ShardVerifier};
                                    let proof = part
                                        .integrity
                                        .as_ref()
                                        .map(|part| {
                                            PartProofReader::new(part.clone(), &parts_metadata, &latest_disks, bucket, object)
                                        })
                                        .transpose()?;
                                    // Cover the complete encoded shard, including a logical
                                    // part's partial final data block, when checking truncation.
                                    let till_offset = usize::try_from(erasure.shard_file_size(part.size as i64))
                                        .map_err(|_| DiskError::FileCorrupt)?;
                                    let use_mmap_read = object_mmap_read_enabled();

                                    let mut writers = Vec::with_capacity(out_dated_disks.len());

                                    // Preserve the committed layout: recomputing inline-ness here
                                    // (with a hardcoded unversioned threshold) makes healed replicas
                                    // diverge from healthy ones in quorum identity, so heal would
                                    // flag them forever.
                                    let is_inline_buffer = latest_meta.inline_data();
                                    // create writers for all disk positions, but only for outdated disks
                                    for (index, disk_op) in out_dated_disks.iter().enumerate() {
                                        if let Some(outdated_disk) = disk_op {
                                            let writer_result = if let Some(error) =
                                                injected_heal_writer_error(bucket, object, part.number, index)
                                            {
                                                Err(error)
                                            } else {
                                                create_bitrot_writer(
                                                    is_inline_buffer,
                                                    Some(outdated_disk),
                                                    RUSTFS_META_TMP_BUCKET,
                                                    &path_join_buf(&[
                                                        &tmp_id.to_string(),
                                                        &dst_data_dir.to_string(),
                                                        &format!("part.{}", part.number),
                                                    ]),
                                                    erasure.shard_file_size(part.size as i64),
                                                    erasure.shard_size(),
                                                    HashAlgorithm::HighwayHash256S,
                                                )
                                                .await
                                            };
                                            let writer = match writer_result {
                                                Ok(writer) => writer,
                                                Err(err) => {
                                                    writer_failures.record(part.number, index, err);
                                                    writers.push(None);
                                                    continue;
                                                }
                                            };
                                            let mut writer = writer;
                                            if let Some(proof) = &proof {
                                                let verifier = ShardVerifier::new(std::sync::Arc::clone(proof), index, 0, None)?;
                                                writer.set_integrity(verifier);
                                            }
                                            writers.push(Some(writer));
                                        } else {
                                            writers.push(None);
                                        }
                                    }

                                    // Heal each part. erasure.Heal() will write the healed
                                    // part to .rustfs/tmp/uuid/ which needs to be renamed
                                    // later to the final location.
                                    if writers.iter().all(Option::is_none) {
                                        writer_failures.warn(bucket, object, version_id, "all_targets_unavailable");
                                        // No target can accept this part. Preserve the setup
                                        // cause before opening source readers or reconstructing.
                                        return Ok(Some(writer_failures.into_error()));
                                    }
                                    #[cfg(test)]
                                    pause_read_repair(bucket, object, ReadRepairPausePhase::PartStaging(part.number)).await;

                                    let mut readers = Vec::with_capacity(latest_disks.len());

                                    let mut prefer = vec![false; latest_disks.len()];
                                    let this_part_errs =
                                        Self::shuffle_check_parts(&data_errs_by_part[&part_index], &erasure_info.distribution);
                                    for (index, disk) in latest_disks.iter().enumerate() {
                                        if this_part_errs[index] != CHECK_PART_SUCCESS {
                                            trace!(
                                                event = EVENT_SET_DISK_HEAL,
                                                component = LOG_COMPONENT_ECSTORE,
                                                subsystem = LOG_SUBSYSTEM_SET_DISK,
                                                part_number = part.number,
                                                disk_index = index,
                                                part_status = this_part_errs[index],
                                                state = "source_shard_skipped",
                                                "Set disk source shard skipped"
                                            );
                                            readers.push(None);
                                            continue;
                                        }

                                        if let (Some(disk), Some(metadata)) = (disk, &copy_parts_metadata[index]) {
                                            let checksum_info = metadata.erasure.get_checksum_info(part.number);
                                            let checksum_algo = if metadata.uses_legacy_checksum
                                                && checksum_info.algorithm == HashAlgorithm::HighwayHash256S
                                            {
                                                HashAlgorithm::HighwayHash256SLegacy
                                            } else {
                                                checksum_info.algorithm
                                            };

                                            match create_bitrot_reader(
                                                metadata.data.as_deref(),
                                                Some(disk),
                                                bucket,
                                                &path_join_buf(&[object, &src_data_dir, &format!("part.{}", part.number)]),
                                                0,
                                                till_offset,
                                                erasure.shard_size(),
                                                checksum_algo.clone(),
                                                false,
                                                use_mmap_read,
                                            )
                                            .await
                                            {
                                                Ok(Some(mut reader)) => {
                                                    if let Some(proof) = &proof {
                                                        let verifier =
                                                            ShardVerifier::new(std::sync::Arc::clone(proof), index, 0, None)?;
                                                        reader.set_integrity(verifier)?;
                                                    }
                                                    readers.push(Some(reader));
                                                }
                                                Ok(None) => {
                                                    readers.push(None);
                                                    continue;
                                                }
                                                Err(_e) => {
                                                    readers.push(None);
                                                    continue;
                                                }
                                            }

                                            prefer[index] = disk.host_name().is_empty();
                                        } else {
                                            readers.push(None);
                                            // errors.push(Some(DiskError::DiskNotFound));
                                        }
                                    }

                                    let heal_part = erasure.heal(&mut writers, readers, part.size, &prefer);
                                    let heal_part_result = if read_repair_uses_shared_lock {
                                        match tokio::time::timeout(READ_REPAIR_DATA_PHASE_TIMEOUT, heal_part).await {
                                            Ok(result) => result,
                                            Err(_) => Err(DiskError::Timeout),
                                        }
                                    } else {
                                        heal_part.await
                                    };
                                    heal_part_result?;
                                    // close_bitrot_writers(&mut writers).await?;
                                    if !is_inline_buffer && let Some(proof) = &proof {
                                        let before = out_dated_disks.iter().filter(|disk| disk.is_some()).count();
                                        PreparedIntegrity::copy_from(proof)
                                            .await?
                                            .write(
                                                &mut out_dated_disks,
                                                bucket,
                                                RUSTFS_META_TMP_BUCKET,
                                                &format!("{tmp_id}/{dst_data_dir}"),
                                            )
                                            .await?;
                                        disks_to_heal_count -=
                                            before - out_dated_disks.iter().filter(|disk| disk.is_some()).count();
                                    }

                                    for (index, disk_op) in out_dated_disks.iter_mut().enumerate() {
                                        if disk_op.is_none() {
                                            continue;
                                        }

                                        if writers[index].is_none() {
                                            *disk_op = None;
                                            disks_to_heal_count -= 1;
                                            continue;
                                        }

                                        parts_metadata[index].data_dir = Some(dst_data_dir);
                                        parts_metadata[index].add_object_part(
                                            part.number,
                                            part.etag.clone(),
                                            part.size,
                                            part.mod_time,
                                            part.actual_size,
                                            part.index.clone(),
                                            part.checksums.clone(),
                                        );
                                        if let Some(updated) = parts_metadata[index]
                                            .parts
                                            .iter_mut()
                                            .find(|updated| updated.number == part.number)
                                        {
                                            updated.integrity.clone_from(&part.integrity);
                                        }
                                        if is_inline_buffer {
                                            if let Some(writer) = writers[index].take() {
                                                // if let Some(w) = writer.as_any().downcast_ref::<BitrotFileWriter>() {
                                                //     parts_metadata[index].data = Some(w.inline_data().to_vec());
                                                // }
                                                parts_metadata[index].data =
                                                    Some(writer.into_inline_data().map(Bytes::from).unwrap_or_default());
                                            }
                                            parts_metadata[index].set_inline_data();
                                        } else {
                                            parts_metadata[index].data = None;
                                        }
                                    }

                                    if disks_to_heal_count == 0 {
                                        writer_failures.warn(bucket, object, version_id, "all_targets_unavailable");
                                        return Ok(Some(writer_failures.into_error()));
                                    }
                                }

                                writer_failures.warn(bucket, object, version_id, "partial_targets_unavailable");
                                Ok(None)
                            }
                            .await;
                            match stage_result {
                                Ok(None) => {}
                                failure => {
                                    // Release readers and writers before removing all parts
                                    // staged under this heal's exclusively owned prefix.
                                    let _ = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id).await;
                                    return failure.map(|error| (result, error));
                                }
                            }
                        }
                        let _read_repair_write_lock_guard = if let Some(expected_fingerprint) = read_repair_commit_fingerprint {
                            if _read_repair_read_lock_guard
                                .as_ref()
                                .is_some_and(|guard| guard.is_lock_lost())
                            {
                                let _ = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id).await;
                                return Err(DiskError::DiskOngoingReq);
                            }
                            drop(_read_repair_read_lock_guard.take());
                            match self
                                .acquire_revalidated_read_repair_commit_lock(
                                    &disks,
                                    bucket,
                                    object,
                                    version_id,
                                    expected_fingerprint,
                                )
                                .await
                            {
                                Ok(Some(guard)) => Some(Arc::new(guard)),
                                Ok(None) => {
                                    let _ = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id).await;
                                    return Ok((result, Some(DiskError::Timeout)));
                                }
                                Err(err) => {
                                    let _ = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id).await;
                                    return Err(err);
                                }
                            }
                        } else {
                            None
                        };

                        // Rename from tmp location to the actual location.
                        // MinIO stops on the first RenameData error. RustFS intentionally
                        // continues per target, but reports any residue after all attempts
                        // so successful repairs survive and failed targets remain retryable.
                        #[cfg(test)]
                        if bucket_heal_scope.is_some() {
                            pause_read_repair(bucket, object, ReadRepairPausePhase::IncarnationCommit).await;
                        }
                        let mut rename_attempts = 0usize;
                        let mut rename_successes = 0usize;
                        let mut healed_disks = vec![None; out_dated_disks.len()];
                        let mut backup_cleanup_owner: Option<Arc<dyn Send + Sync>> = None;
                        for (index, outdated_disk) in out_dated_disks.iter().enumerate() {
                            if let Some(disk) = outdated_disk {
                                if let Some(scope) = &bucket_heal_scope {
                                    scope.check()?;
                                }
                                if commit_fence.is_some_and(|check| !check())
                                    || _write_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                                    || _read_repair_write_lock_guard
                                        .as_ref()
                                        .is_some_and(|guard| guard.is_lock_lost())
                                {
                                    let _ = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id).await;
                                    return Err(DiskError::DiskOngoingReq);
                                }
                                rename_attempts += 1;
                                // record the index of the updated disks
                                parts_metadata[index].erasure.index = index + 1;
                                // Attempt a rename now from healed data to final location.
                                parts_metadata[index].set_healing();

                                let rename_result = if should_fail_heal_rename(bucket, object, index) {
                                    Err(DiskError::Unexpected)
                                } else {
                                    disk.rename_data_borrowed(
                                        RUSTFS_META_TMP_BUCKET,
                                        &tmp_id,
                                        &parts_metadata[index],
                                        bucket,
                                        object,
                                    )
                                    .await
                                };

                                match rename_result {
                                    Err(err) => {
                                        warn!(
                                            event = EVENT_HEAL_OBJECT_RENAME,
                                            component = LOG_COMPONENT_ECSTORE,
                                            subsystem = LOG_SUBSYSTEM_HEAL,
                                            result = "failed",
                                            bucket,
                                            object,
                                            version_id,
                                            disk_index = index,
                                            endpoint = %disk.endpoint(),
                                            tmp_id,
                                            error = %err,
                                            "Heal object rename failed"
                                        );
                                    }
                                    Ok(rename_response) => {
                                        rename_successes += 1;
                                        healed_disks[index] = Some(disk.clone());
                                        // Heal can reuse a live data directory, and a synthetic
                                        // rollback directory can also contain a child key.
                                        // Retire only this publication's backup while fenced.
                                        if let Some(rollback_dir) = heal_rollback_backup_dir(&rename_response)
                                            && bucket_heal_scope.as_ref().is_none_or(|scope| scope.check().is_ok())
                                            && commit_fence.is_none_or(|check| check())
                                            && !_write_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                                            && !_read_repair_write_lock_guard
                                                .as_ref()
                                                .is_some_and(|guard| guard.is_lock_lost())
                                        {
                                            let backup_path = format!(
                                                "{}/{rollback_dir}/{}",
                                                rustfs_utils::path::encode_dir_object_ref(object),
                                                disk::STORAGE_FORMAT_FILE_BACKUP,
                                            );
                                            // The syscall can outlive this future; retain the exact-key
                                            // locks until physical cleanup has finished.
                                            let cleanup_owner = backup_cleanup_owner.get_or_insert_with(|| {
                                                Arc::new((
                                                    namespace_owner.cloned(),
                                                    _write_lock_guard.clone(),
                                                    _read_repair_write_lock_guard.clone(),
                                                    bucket_heal_scope.clone(),
                                                    self.ctx.begin_namespace_commit(),
                                                ))
                                            });
                                            match disk
                                                .delete_with_namespace_owner(
                                                    bucket,
                                                    &backup_path,
                                                    DeleteOptions::default(),
                                                    Some(cleanup_owner.clone()),
                                                )
                                                .await
                                            {
                                                Ok(()) | Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => {}
                                                Err(err) => warn!(
                                                    event = EVENT_HEAL_OBJECT_RENAME,
                                                    component = LOG_COMPONENT_ECSTORE,
                                                    subsystem = LOG_SUBSYSTEM_HEAL,
                                                    result = "cleanup_failed",
                                                    bucket,
                                                    object,
                                                    version_id,
                                                    disk_index = index,
                                                    error = %err,
                                                    "Heal rollback backup cleanup failed"
                                                ),
                                            }
                                        }
                                        if parts_metadata[index].is_remote() {
                                            let rm_data_dir =
                                                parts_metadata[index].data_dir.expect("operation should succeed").to_string();

                                            let d_path = Path::new(&encode_dir_object(object)).join(rm_data_dir);

                                            if let Err(e) = disk
                                                .delete(
                                                    bucket,
                                                    d_path.to_str().expect("operation should succeed"),
                                                    DeleteOptions {
                                                        immediate: true,
                                                        recursive: true,
                                                        ..Default::default()
                                                    },
                                                )
                                                .await
                                            {
                                                // The healed shard has already been renamed into place; a
                                                // failure cleaning up the old remote data dir must not abort
                                                // the heal and leak the tmp shards (backlog#799 B20).
                                                warn!(
                                                    component = LOG_COMPONENT_ECSTORE,
                                                    subsystem = LOG_SUBSYSTEM_HEAL,
                                                    bucket,
                                                    object,
                                                    error = %e,
                                                    "Heal remote data-dir cleanup failed"
                                                );
                                            }
                                        }

                                        for (i, v) in result.before.drives.iter().enumerate() {
                                            if v.endpoint == disk.endpoint().to_string() {
                                                result.after.drives[i].state = DriveState::Ok.to_string();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id)
                            .await
                            .map_err(DiskError::other)?;
                        if _read_repair_write_lock_guard
                            .as_ref()
                            .is_some_and(|guard| guard.is_lock_lost())
                        {
                            return Err(DiskError::DiskOngoingReq);
                        }

                        self.record_healed_capacity_scope(&healed_disks);

                        if rename_successes < rename_attempts {
                            return Ok((
                                result,
                                Some(DiskError::other(format!(
                                    "{HEAL_RENAME_INCOMPLETE}: {rename_successes} of {rename_attempts} targets committed for \
                                     {bucket}/{object}"
                                ))),
                            ));
                        }

                        if let Some(error) = volume_creation_error {
                            return Ok((result, Some(error)));
                        }

                        if result.integrity_verified
                            && let Err(error) =
                                crate::io_support::shard_integrity::restore_proof_replicas(&latest_meta, &disks, bucket, object)
                                    .await
                        {
                            result.integrity_verified = false;
                            return Ok((result, Some(error)));
                        }

                        result.repair_verified = protected
                            && !latest_meta.deleted
                            && !latest_meta.is_remote()
                            && !read_repair_uses_shared_lock
                            && result.drives_healed().is_some_and(|healed| healed > 0)
                            && result
                                .after
                                .drives
                                .iter()
                                .all(|drive| drive.state == DriveState::Ok.to_string());
                        result.metadata_repair_verified = latest_meta.deleted
                            && !opts.dry_run
                            && !latest_meta.is_remote()
                            && !read_repair_uses_shared_lock
                            && result.drives_healed().is_some_and(|healed| healed > 0)
                            && result
                                .after
                                .drives
                                .iter()
                                .all(|drive| drive.state == DriveState::Ok.to_string());

                        // The object is healthy here; sweep any data dirs left behind
                        // by pre-#3510 unversioned overwrites, which the dangling paths
                        // above never touch (issues #3231, #3191). Best effort — a
                        // failure must not fail the heal.
                        if !preserve_unrepairable {
                            self.reclaim_orphan_data_dirs_best_effort(bucket, object).await;
                        }

                        Ok((result, None))
                    }
                    Err(err) => Ok((result, Some(err))),
                }
            }
            Err(err) => {
                // All replicas explicitly missing was handled before the
                // quorum check. A partial read cannot prove object absence.
                let err = if matches!(err, DiskError::FileNotFound | DiskError::FileVersionNotFound) {
                    DiskError::ErasureReadQuorum
                } else {
                    err
                };
                if let Some(retirement) = retirement
                    && parts_metadata
                        .iter()
                        .zip(&errs)
                        .any(|(info, error)| error.is_none() && info.is_canonical_delete_marker())
                {
                    // A current-generation delete marker can be left behind on
                    // an inaccessible member after an acknowledged S3 DELETE.
                    // That case has no bucket-retirement record, so evaluate its
                    // stricter same-incarnation proof before the retired-marker
                    // path. The proof requires an exact marker identity, at
                    // least one replica already absent, and every target online.
                    let cleanup =
                        match Self::same_incarnation_marker_candidate(&parts_metadata, &errs, retirement.current_incarnation) {
                            Ok(marker) => {
                                self.remove_current_marker(retirement, bucket, object, marker, opts, &disks)
                                    .await
                            }
                            Err(_) => match Self::retired_marker_candidate(&parts_metadata, &errs) {
                                Ok(marker) => {
                                    self.remove_retired_marker(retirement, bucket, object, marker, opts, &disks)
                                        .await
                                }
                                Err(error) => Err(error),
                            },
                        };
                    let absent = cleanup.is_ok();
                    let item = self
                        .dangling_heal_result(FileInfo::default(), &errs, bucket, object, version_id, absent)
                        .await;
                    if absent {
                        *absence = Some(HealedObjectAbsence {
                            pool_index: self.pool_index,
                            set_index: self.set_index,
                            removed: true,
                        });
                    }
                    return Ok((item, cleanup.err()));
                }
                if !opts.dry_run
                    && allow_explicit_version_regen
                    && !version_id.is_empty()
                    && self
                        .try_regenerate_explicit_version_meta(bucket, object, version_id, &parts_metadata, &errs, &disks)
                        .await?
                {
                    return Box::pin(self.heal_object_with_explicit_version_regen(
                        bucket,
                        object,
                        version_id,
                        ExplicitVersionHeal {
                            allow_regeneration: false,
                            ..run
                        },
                        absence,
                    ))
                    .await;
                }

                if opts.dry_run || preserve_unrepairable || !opts.remove {
                    return Ok((
                        self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                            .await,
                        Some(err),
                    ));
                }

                if self
                    .dangling_delete_safety(bucket, object, &parts_metadata, &errs, &disks)
                    .await?
                    == DanglingDeleteSafety::UnsafeToDelete
                {
                    return Ok((
                        self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                            .await,
                        Some(err),
                    ));
                }

                let data_errs_by_part = HashMap::new();
                match self
                    .delete_if_dangling_with_proof(
                        bucket,
                        object,
                        &parts_metadata,
                        &errs,
                        &data_errs_by_part,
                        ObjectOptions {
                            version_id: version_id_op.clone(),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok((m, absent)) => {
                        if absent {
                            *absence = Some(HealedObjectAbsence {
                                pool_index: self.pool_index,
                                set_index: self.set_index,
                                removed: true,
                            });
                        }
                        Ok((
                            self.dangling_heal_result(m, &errs, bucket, object, version_id, absent).await,
                            (!absent).then_some(DiskError::ErasureWriteQuorum),
                        ))
                    }
                    Err(cleanup_err) => Ok((
                        self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                            .await,
                        Some(cleanup_err),
                    )),
                }
            }
        }
    }

    fn retired_marker_candidate(
        metadata: &[FileInfo],
        errors: &[Option<DiskError>],
    ) -> disk::error::Result<rustfs_filemeta::MetaDeleteMarker> {
        let mut candidate = None;
        for (info, error) in metadata.iter().zip(errors) {
            match error {
                Some(DiskError::FileNotFound | DiskError::FileVersionNotFound) => continue,
                Some(_) => return Err(DiskError::retired_marker_deferred("not every replica is readable")),
                None => {}
            }
            if !info.is_canonical_delete_marker() || info.delete_marker_incarnation().is_none() {
                return Err(DiskError::retired_marker_deferred("marker has no trustworthy bucket incarnation"));
            }
            let marker = rustfs_filemeta::MetaDeleteMarker::from(info.clone());
            if candidate.as_ref().is_some_and(|previous| previous != &marker) {
                return Err(DiskError::retired_marker_deferred("surviving marker identities conflict"));
            }
            candidate = Some(marker);
        }
        candidate.ok_or_else(|| DiskError::retired_marker_deferred("no exact marker candidate"))
    }

    fn same_incarnation_marker_candidate(
        metadata: &[FileInfo],
        errors: &[Option<DiskError>],
        current_incarnation: Option<Uuid>,
    ) -> disk::error::Result<rustfs_filemeta::MetaDeleteMarker> {
        let defer = DiskError::retired_marker_deferred;
        let Some(current) = current_incarnation.filter(|id| !id.is_nil()) else {
            return Err(defer("current bucket incarnation is unavailable"));
        };
        let mut candidate = None;
        let mut missing_replica = false;
        for (info, error) in metadata.iter().zip(errors) {
            match error {
                Some(DiskError::FileNotFound | DiskError::FileVersionNotFound) => {
                    missing_replica = true;
                    continue;
                }
                Some(_) => return Err(defer("not every replica is readable")),
                None => {}
            }
            if !info.is_canonical_delete_marker() || info.delete_marker_incarnation() != Some(current) {
                return Err(defer("marker is not a canonical current-generation marker"));
            }
            let marker = rustfs_filemeta::MetaDeleteMarker::from(info.clone());
            if candidate.as_ref().is_some_and(|previous| previous != &marker) {
                return Err(defer("surviving marker identities conflict"));
            }
            candidate = Some(marker);
        }
        if !missing_replica {
            return Err(defer("marker is present on every replica"));
        }
        candidate.ok_or_else(|| defer("no exact current-generation marker candidate"))
    }

    async fn remove_retired_marker(
        &self,
        context: &MarkerRetirementContext<'_>,
        bucket: &str,
        object: &str,
        marker: rustfs_filemeta::MetaDeleteMarker,
        opts: &HealOpts,
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<()> {
        let defer = DiskError::retired_marker_deferred;
        if opts.dry_run || !opts.remove {
            return Err(defer("cleanup requires remove=true and dry_run=false"));
        }
        if disks.len() != self.set_drive_count || disks.iter().any(Option::is_none) {
            return Err(defer("every target disk must be online"));
        }
        let incarnation = marker.into_fileinfo(bucket, object, false)?.delete_marker_incarnation();
        let Some(old) = incarnation else { return Err(defer("marker incarnation is unavailable")) };
        let Some(current) = context.current_incarnation else {
            return Err(defer("current bucket incarnation is unavailable"));
        };
        if old == current || context.lifecycle_guard.is_lock_lost() {
            return Err(defer("current generation or lost bucket lifecycle fence"));
        }
        let Some(store) = context.store.as_ref() else {
            return Err(defer("retirement owner is unavailable"));
        };
        match crate::bucket::retirement::is_retired(store.clone(), bucket, old).await {
            Ok(true) => {}
            Ok(false) => return Err(defer("no committed bucket retirement record")),
            Err(error) => return Err(DiskError::retired_marker_deferred(format!("retirement read failed: {error}"))),
        }
        let Some(version) = marker.version_id.filter(|id| !id.is_nil()) else {
            return Err(defer("cleanup requires an explicit non-null version"));
        };
        if context.lifecycle_guard.is_lock_lost() {
            return Err(defer("bucket lifecycle fence was lost"));
        }
        let result = self
            .remove_marker_exact_under_fence(bucket, object, marker, version, disks)
            .await;
        if context.lifecycle_guard.is_lock_lost() {
            return Err(defer("bucket lifecycle fence was lost"));
        }
        result
    }

    async fn remove_current_marker(
        &self,
        context: &MarkerRetirementContext<'_>,
        bucket: &str,
        object: &str,
        marker: rustfs_filemeta::MetaDeleteMarker,
        opts: &HealOpts,
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<()> {
        let defer = DiskError::retired_marker_deferred;
        if opts.dry_run || !opts.remove {
            return Err(defer("cleanup requires remove=true and dry_run=false"));
        }
        if disks.len() != self.set_drive_count || disks.iter().any(Option::is_none) {
            return Err(defer("every target disk must be online"));
        }
        let Some(current) = context.current_incarnation.filter(|id| !id.is_nil()) else {
            return Err(defer("current bucket incarnation is unavailable"));
        };
        if context.lifecycle_guard.is_lock_lost() {
            return Err(defer("bucket lifecycle fence was lost"));
        }
        let incarnation = marker.into_fileinfo(bucket, object, false)?.delete_marker_incarnation();
        if incarnation != Some(current) {
            return Err(defer("marker is not from the current bucket incarnation"));
        }
        let Some(version) = marker.version_id.filter(|id| !id.is_nil()) else {
            return Err(defer("cleanup requires an explicit non-null version"));
        };
        let marker_identity = marker.stable_identity();
        let marker_bytes = marker.marshal_msg().map_err(DiskError::other)?;
        let Some(purge) = MrfDeleteMarkerPurge::new(current, current, marker_identity, marker_bytes) else {
            return Err(defer("marker purge proof payload is invalid"));
        };
        if !self.has_marker_purge_receipt(bucket, object, version, &purge).await {
            return Err(defer("same-incarnation marker lacks an acknowledged delete receipt"));
        }
        let result = self
            .remove_marker_exact_under_fence(bucket, object, marker, version, disks)
            .await;
        if context.lifecycle_guard.is_lock_lost() {
            return Err(defer("bucket lifecycle fence was lost"));
        }
        if result.is_ok() {
            self.consume_marker_purge_receipt(bucket, object, version, &purge).await;
        }
        result
    }

    async fn remove_marker_exact_under_fence(
        &self,
        bucket: &str,
        object: &str,
        marker: rustfs_filemeta::MetaDeleteMarker,
        version: Uuid,
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<()> {
        let request = FileInfo {
            volume: bucket.to_owned(),
            name: object.to_owned(),
            version_id: Some(version),
            ..Default::default()
        };
        let results = join_all(disks.iter().enumerate().map(|(disk_index, disk)| {
            let request = request.clone();
            let marker = marker.clone();
            async move {
                if let Some(error) = injected_dangling_delete_error(bucket, object, disk_index) {
                    return Err(error);
                }
                let Some(disk) = disk else { return Err(DiskError::DiskNotFound) };
                disk.delete_version(
                    bucket,
                    object,
                    request,
                    false,
                    DeleteOptions {
                        expected_delete_marker: Some(marker),
                        ..Default::default()
                    },
                )
                .await
            }
        }))
        .await;
        for result in results {
            if let Err(error) = result
                && !matches!(error, DiskError::FileNotFound | DiskError::FileVersionNotFound)
            {
                return Err(DiskError::retired_marker_deferred(format!("conditional marker deletion failed: {error}")));
            }
        }
        let metadata_read = Self::read_metadata(disks, "", bucket, object, &version.to_string(), false, false, false).await?;
        let all_absent = metadata_read
            .slots
            .iter()
            .all(|slot| matches!(&slot.result, Some(Err(DiskError::FileNotFound | DiskError::FileVersionNotFound))));
        let selected = self.get_disks_internal().await;
        let same_targets = selected.len() == disks.len() && selected.iter().zip(disks).all(|(current, original)| {
            matches!((current, original), (Some(current), Some(original)) if std::sync::Arc::ptr_eq(current, original))
        });
        if !same_targets || !all_absent {
            return Err(DiskError::retired_marker_deferred(
                "complete marker absence could not be verified under the original target set",
            ));
        }
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        Ok(())
    }

    async fn try_regenerate_explicit_version_meta(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<bool> {
        let Ok(version_id) = Uuid::parse_str(version_id) else {
            return Ok(false);
        };
        let candidates = parts_metadata
            .iter()
            .zip(errs.iter())
            .filter_map(|(file_info, err)| {
                (err.is_none()
                    && file_info_is_valid_for_metadata(file_info)
                    && file_info.version_id == Some(version_id)
                    && file_info.has_valid_erasure_geometry()
                    && !file_info.deleted
                    && !file_info.is_remote()
                    && file_info.data_dir.is_some()
                    && !file_info.parts.is_empty()
                    && file_info.erasure.data_blocks > 0
                    && file_info
                        .erasure
                        .data_blocks
                        .checked_add(file_info.erasure.parity_blocks)
                        .is_some_and(|shards| shards == disks.len()))
                .then_some(file_info)
            })
            .collect::<Vec<_>>();
        let Some(candidate) = candidates.first().copied() else {
            return Ok(false);
        };
        let protected = rustfs_filemeta::shard_integrity::descriptor_from_metadata(&candidate.metadata)
            .map_err(DiskError::from)?
            .is_some()
            || candidate.parts.iter().any(|part| part.integrity.is_some());
        // A minority metadata copy cannot establish an independent commitment.
        // Legacy recovery retains its existing bounds without certifying identity.
        if protected
            && (candidates.len() < candidate.erasure.data_blocks || candidate.parts.iter().any(|part| part.integrity.is_none()))
        {
            return Ok(false);
        }
        let identity = Self::file_info_quorum_hash(candidate);
        if candidates
            .iter()
            .any(|file_info| Self::file_info_quorum_hash(file_info) != identity)
        {
            return Ok(false);
        }

        let mut available = 0usize;
        for disk in disks {
            let Some(disk) = disk else {
                return Ok(false);
            };
            match disk.check_parts(bucket, object, candidate).await {
                Ok(response)
                    if !response.results.is_empty() && response.results.iter().all(|result| *result == CHECK_PART_SUCCESS) =>
                {
                    available += 1;
                }
                Ok(_)
                | Err(
                    DiskError::FileNotFound
                    | DiskError::FileVersionNotFound
                    | DiskError::PathNotFound
                    | DiskError::VolumeNotFound,
                ) => {}
                Err(_) => return Ok(false),
            }
        }
        if available < candidate.erasure.data_blocks {
            return Ok(false);
        }

        let mut verified_disks = vec![true; disks.len()];
        if protected {
            let mut proof_files = parts_metadata.to_vec();
            for (index, file) in proof_files.iter_mut().enumerate() {
                if matches!(
                    errs.get(index).and_then(Option::as_ref),
                    Some(DiskError::FileNotFound | DiskError::FileVersionNotFound)
                ) {
                    *file = candidate.clone();
                    file.erasure.index = candidate.erasure.distribution[index];
                }
            }
            let mut verified = (0..candidate.parts.len())
                .map(|part| (part, vec![crate::disk::CHECK_PART_UNKNOWN; disks.len()]))
                .collect();
            crate::io_support::shard_integrity::verify_deep_parts(&proof_files, disks, candidate, bucket, object, &mut verified)
                .await?;
            for (index, valid) in verified_disks.iter_mut().enumerate() {
                *valid = verified.values().all(|parts| parts.get(index) == Some(&CHECK_PART_SUCCESS));
            }
            if verified_disks.iter().filter(|valid| **valid).count() < candidate.erasure.data_blocks {
                return Ok(false);
            }
        }

        let mut wrote = 0usize;
        for (index, disk) in disks.iter().enumerate() {
            let Some(disk) = disk else {
                return Ok(false);
            };
            let metadata_absent = matches!(
                errs.get(index).and_then(Option::as_ref),
                Some(DiskError::FileNotFound | DiskError::FileVersionNotFound)
            );
            if !metadata_absent || !verified_disks[index] {
                continue;
            }
            let Some(&shard_index) = candidate.erasure.distribution.get(index) else {
                return Ok(false);
            };
            let mut regenerated = candidate.clone();
            regenerated.fresh = false;
            regenerated.erasure.index = shard_index;
            match disk.write_metadata("", bucket, object, regenerated).await {
                Ok(()) => wrote += 1,
                Err(error) => {
                    warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_HEAL,
                        bucket,
                        object,
                        disk_index = index,
                        error = %error,
                        "failed to regenerate recoverable xl.meta"
                    );
                }
            }
        }
        Ok(wrote > 0)
    }

    /// Best-effort orphan-data-dir reclaim for an object that is healthy on this
    /// set. Wraps [`Self::reclaim_orphan_data_dirs`] with the shared logging so
    /// both `heal_object` exits — the already-healthy early return and the
    /// post-heal tail — reclaim identically. Never fails the heal: delete errors
    /// are logged and swallowed. Callers must gate this on `!opts.dry_run`.
    async fn reclaim_orphan_data_dirs_best_effort(&self, bucket: &str, object: &str) {
        match self.reconcile_old_data_cleanup_receipts(bucket, object).await {
            Ok(removed) if removed > 0 => {
                debug!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    removed,
                    state = "old_data_cleanup_receipt_reconciled",
                    "Set disk old-data cleanup receipts reconciled"
                );
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    error = %e,
                    state = "old_data_cleanup_receipt_reconcile_failed",
                    "Set disk old-data cleanup receipt reconcile failed"
                );
            }
        }
        match self.reclaim_orphan_data_dirs(bucket, object).await {
            Ok(removed) if removed > 0 => {
                debug!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    removed,
                    state = "orphan_data_reclaimed",
                    "Set disk orphaned data reclaimed"
                );
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    event = EVENT_SET_DISK_HEAL,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    error = %e,
                    state = "orphan_data_reclaim_failed",
                    "Set disk orphan data-dir reclaim failed"
                );
            }
        }
    }

    async fn cleanup_metadata_less_data_dirs(
        &self,
        bucket: &str,
        object: &str,
        disks: &[Option<DiskStore>],
        dry_run: bool,
    ) -> disk::error::Result<Option<MetadataLessDataDirCleanup>> {
        if disks.iter().any(Option::is_none) {
            return Ok(None);
        }

        let mut per_disk_dirs = Vec::new();
        for (disk_index, disk) in disks.iter().enumerate() {
            let Some(disk) = disk.as_ref() else {
                return Ok(None);
            };
            let entries = match disk.list_dir("", bucket, object, -1).await {
                Ok(entries) => entries,
                Err(
                    DiskError::FileNotFound
                    | DiskError::FileVersionNotFound
                    | DiskError::PathNotFound
                    | DiskError::VolumeNotFound,
                ) => continue,
                Err(err) => return Err(err),
            };
            for entry in entries {
                let data_dir = entry.trim_end_matches(SLASH_SEPARATOR);
                if !entry.ends_with(SLASH_SEPARATOR) || Uuid::parse_str(data_dir).is_err() {
                    return Ok(None);
                }
                let data_dir_path = format!("{object}{SLASH_SEPARATOR}{data_dir}");
                if !Self::metadata_less_data_dir_is_reclaimable(disk, bucket, &data_dir_path).await? {
                    return Ok(None);
                }
                per_disk_dirs.push((disk_index, data_dir_path));
            }
        }

        let matched = per_disk_dirs.len();
        if matched == 0 {
            return Ok(None);
        }

        let mut cleanup = MetadataLessDataDirCleanup {
            matched,
            delete_errs: vec![None; disks.len()],
            touched_disks: vec![false; disks.len()],
            ..Default::default()
        };
        for (disk_index, _) in &per_disk_dirs {
            cleanup.touched_disks[*disk_index] = true;
        }

        if dry_run {
            return Ok(Some(cleanup));
        }

        for (disk_index, data_dir_path) in per_disk_dirs {
            let Some(disk) = disks[disk_index].as_ref() else {
                return Ok(None);
            };
            match disk
                .delete_data_dir(
                    bucket,
                    &data_dir_path,
                    DeleteOptions {
                        recursive: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(DataDirDeleteStatus::Deleted) => {
                    cleanup.removed = cleanup.removed.saturating_add(1);
                }
                Ok(DataDirDeleteStatus::Deferred) => {
                    let err = DiskError::FileAccessDenied;
                    cleanup.first_error.get_or_insert_with(|| err.clone());
                    cleanup.delete_errs[disk_index] = Some(err);
                }
                Err(DiskError::FileNotFound | DiskError::PathNotFound | DiskError::VolumeNotFound) => {
                    cleanup.removed = cleanup.removed.saturating_add(1);
                }
                Err(err) => {
                    cleanup.first_error.get_or_insert_with(|| err.clone());
                    cleanup.delete_errs[disk_index] = Some(err);
                }
            }
        }

        Ok(Some(cleanup))
    }

    async fn metadata_less_data_dir_is_reclaimable(
        disk: &DiskStore,
        bucket: &str,
        data_dir_path: &str,
    ) -> disk::error::Result<bool> {
        let entries = match disk.list_dir("", bucket, data_dir_path, -1).await {
            Ok(entries) => entries,
            Err(
                DiskError::FileNotFound | DiskError::FileVersionNotFound | DiskError::PathNotFound | DiskError::VolumeNotFound,
            ) => return Ok(false),
            Err(err) => return Err(err),
        };

        let mut has_part = false;
        for entry in entries {
            if entry.ends_with(SLASH_SEPARATOR) {
                return Ok(false);
            }
            let entry_name = entry.trim_end_matches(SLASH_SEPARATOR);
            if metadata_less_part_file(entry_name) {
                has_part = true;
                continue;
            }
            if entry_name
                .strip_prefix(DELETE_DATA_DIR_MARKER_PREFIX)
                .is_some_and(|transaction| Uuid::parse_str(transaction).is_ok())
            {
                continue;
            }
            return Ok(false);
        }

        Ok(has_part)
    }

    async fn metadata_less_data_dir_heal_result(
        &self,
        bucket: &str,
        object: &str,
        cleanup: &MetadataLessDataDirCleanup,
        dry_run: bool,
    ) -> HealResultItem {
        let mut result = HealResultItem {
            heal_item_type: HealItemType::Object.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            disk_count: self.set_endpoints.len(),
            parity_blocks: self.default_parity_count,
            data_blocks: self.set_endpoints.len().saturating_sub(self.default_parity_count),
            ..Default::default()
        };
        result.before.drives = Vec::with_capacity(self.set_endpoints.len());
        result.after.drives = Vec::with_capacity(self.set_endpoints.len());
        for (index, drive) in self.set_endpoints.iter().enumerate() {
            let endpoint = drive.to_string();
            let before_state = if cleanup.touched_disks.get(index).copied().unwrap_or(false) {
                DriveState::Corrupt.to_string()
            } else {
                DriveState::Missing.to_string()
            };
            let after_state = if dry_run {
                before_state.clone()
            } else {
                match cleanup.delete_errs.get(index).and_then(Option::as_ref) {
                    None => DriveState::Missing.to_string(),
                    Some(DiskError::DiskNotFound) => DriveState::Offline.to_string(),
                    Some(_) => DriveState::Corrupt.to_string(),
                }
            };
            result.before.drives.push(HealDriveInfo {
                uuid: String::new(),
                endpoint: endpoint.clone(),
                state: before_state,
            });
            result.after.drives.push(HealDriveInfo {
                uuid: String::new(),
                endpoint,
                state: after_state,
            });
        }
        result
    }

    /// Prevent dangling cleanup when surviving state cannot prove that deletion
    /// is safe. Part presence proves only recoverability, never commit: the write
    /// path can durably rename data before xl.meta is committed.
    async fn dangling_delete_safety(
        &self,
        bucket: &str,
        object: &str,
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<DanglingDeleteSafety> {
        if disks.iter().any(Option::is_none)
            || errs.iter().flatten().any(|err| {
                !matches!(
                    err,
                    DiskError::FileNotFound
                        | DiskError::FileVersionNotFound
                        | DiskError::PathNotFound
                        | DiskError::VolumeNotFound
                )
            })
        {
            return Ok(DanglingDeleteSafety::UnsafeToDelete);
        }

        let mut candidates = Vec::<RecoverableMetaCandidate>::with_capacity(parts_metadata.len());
        for (fi, err) in parts_metadata.iter().zip(errs.iter()) {
            if err.is_some() || !file_info_is_valid_for_metadata(fi) {
                continue;
            }

            let identity = Self::file_info_quorum_hash(fi);
            if !candidates.iter().any(|candidate| candidate.identity == identity) {
                let local_payload = fi.has_valid_erasure_geometry()
                    && !fi.deleted
                    && !fi.is_remote()
                    && fi.data_dir.is_some()
                    && !fi.parts.is_empty()
                    && fi.erasure.data_blocks > 0
                    && fi
                        .erasure
                        .data_blocks
                        .checked_add(fi.erasure.parity_blocks)
                        .is_some_and(|shards| shards == disks.len());
                candidates.push(RecoverableMetaCandidate {
                    identity,
                    file_info: fi.clone(),
                    data_count: 0,
                    local_payload,
                });
            }
        }

        if candidates
            .iter()
            .any(|candidate| candidate.file_info.deleted || candidate.file_info.is_remote())
            || candidates.len() > 1
        {
            return Ok(DanglingDeleteSafety::UnsafeToDelete);
        }

        for candidate in candidates.iter_mut().filter(|candidate| candidate.local_payload) {
            for (disk_index, disk) in disks.iter().enumerate() {
                // Only the #[cfg(test)] fault-injection branch below reads this.
                #[cfg(not(test))]
                let _ = disk_index;
                let Some(disk) = disk else {
                    return Ok(DanglingDeleteSafety::UnsafeToDelete);
                };
                #[cfg(test)]
                let check_result = match injected_dangling_check_parts_error(bucket, object, disk_index) {
                    Some(error) => Err(error),
                    None => disk.check_parts(bucket, object, &candidate.file_info).await,
                };
                #[cfg(not(test))]
                let check_result = disk.check_parts(bucket, object, &candidate.file_info).await;

                match check_result {
                    Ok(resp) if !resp.results.is_empty() && resp.results.iter().all(|result| *result == CHECK_PART_SUCCESS) => {
                        candidate.data_count += 1;
                    }
                    Ok(_) => {}
                    Err(
                        DiskError::FileNotFound
                        | DiskError::FileVersionNotFound
                        | DiskError::PathNotFound
                        | DiskError::VolumeNotFound,
                    ) => {}
                    Err(_) => return Ok(DanglingDeleteSafety::UnsafeToDelete),
                }
            }
        }

        Ok(
            if candidates
                .iter()
                .any(|candidate| candidate.local_payload && candidate.data_count >= candidate.file_info.erasure.data_blocks)
            {
                DanglingDeleteSafety::UnsafeToDelete
            } else {
                DanglingDeleteSafety::NoRecoverableCandidate
            },
        )
    }

    pub(in crate::set_disk) async fn heal_object_dir_locked(
        &self,
        bucket: &str,
        object: &str,
        dry_run: bool,
        remove: bool,
    ) -> Result<(HealResultItem, Option<DiskError>)> {
        let disks = {
            let disks = self.disks.read().await;
            disks.clone()
        };
        let mut result = HealResultItem {
            heal_item_type: HealItemType::Object.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            disk_count: self.disks.read().await.len(),
            parity_blocks: self.default_parity_count,
            data_blocks: disks.len() - self.default_parity_count,
            object_size: 0,
            ..Default::default()
        };

        // Filled below by pushing one entry per disk while zipping the (index-aligned) `errs`.
        // Pre-filling here would double the reported drive list once the push loop runs.
        result.before.drives = Vec::with_capacity(disks.len());
        result.after.drives = Vec::with_capacity(disks.len());

        let errs = stat_all_dirs(&disks, bucket, object).await;
        let dangling_object = is_object_dir_dangling(&errs);
        let delete_errs = if dangling_object && !dry_run && remove {
            let mut futures = Vec::with_capacity(disks.len());
            for (disk_index, disk) in disks.iter().enumerate() {
                let disk = disk.clone();
                futures.push(async move {
                    let Some(disk) = disk else {
                        return (disk_index, Some(DiskError::DiskNotFound));
                    };
                    if let Some(error) = injected_dangling_delete_error(bucket, object, disk_index) {
                        return (disk_index, Some(error));
                    }
                    (
                        disk_index,
                        disk.delete(
                            bucket,
                            object,
                            DeleteOptions {
                                recursive: false,
                                immediate: false,
                                ..Default::default()
                            },
                        )
                        .await
                        .err(),
                    )
                });
            }
            Some(join_all(futures).await)
        } else {
            None
        };

        for (err, drive) in errs.iter().zip(self.set_endpoints.iter()) {
            let endpoint = drive.to_string();
            let drive_state = match err {
                Some(err) => match err {
                    DiskError::DiskNotFound => DriveState::Offline.to_string(),
                    DiskError::FileNotFound | DiskError::VolumeNotFound => DriveState::Missing.to_string(),
                    _ => DriveState::Corrupt.to_string(),
                },
                None => DriveState::Ok.to_string(),
            };
            result.before.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: endpoint.clone(),
                state: drive_state.to_string(),
            });

            result.after.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint,
                state: drive_state.to_string(),
            });
        }

        if let Some(delete_errs) = delete_errs {
            let mut delete_failure = None;
            for (index, err) in delete_errs {
                match err {
                    None | Some(DiskError::FileNotFound) => {
                        result.after.drives[index].state = DriveState::Missing.to_string();
                    }
                    Some(err) => {
                        result.after.drives[index].state = if matches!(&err, DiskError::DiskNotFound) {
                            DriveState::Offline.to_string()
                        } else {
                            DriveState::Corrupt.to_string()
                        };
                        if delete_failure.is_none() {
                            delete_failure = Some(err);
                        }
                    }
                }
            }
            return Ok((result, Some(delete_failure.unwrap_or(DiskError::FileNotFound))));
        }

        if dangling_object || DiskError::is_all_not_found(&errs) {
            return Ok((result, Some(DiskError::FileNotFound)));
        }

        if dry_run {
            // Quit without try to heal the object dir
            return Ok((result, None));
        }
        for (index, (err, disk)) in errs.iter().zip(disks.iter()).enumerate() {
            if let (Some(DiskError::VolumeNotFound | DiskError::FileNotFound), Some(disk)) = (err, disk) {
                let vol_path = Path::new(bucket).join(object);
                let drive_state = match disk.make_volume(vol_path.to_str().expect("operation should succeed")).await {
                    Ok(_) => DriveState::Ok.to_string(),
                    Err(merr) => match merr {
                        DiskError::VolumeExists => DriveState::Ok.to_string(),
                        DiskError::DiskNotFound => DriveState::Offline.to_string(),
                        _ => DriveState::Corrupt.to_string(),
                    },
                };
                result.after.drives[index].state = drive_state.to_string();
            }
        }

        Ok((result, None))
    }

    #[allow(
        dead_code,
        reason = "lock-taking wrapper over the live heal_object_dir_locked; only comments reference it (backlog#1823)"
    )]
    #[tracing::instrument(level = "trace", skip(self), fields(bucket = %bucket, object = %object))]
    pub(in crate::set_disk) async fn heal_object_dir(
        &self,
        bucket: &str,
        object: &str,
        dry_run: bool,
        remove: bool,
    ) -> Result<(HealResultItem, Option<DiskError>)> {
        let _write_lock_guard = {
            let ns_lock = self.new_ns_lock(bucket, object).await?;
            measure_heal_object_lock(
                "heal_object_dir",
                HealObjectLockKind::Write,
                ns_lock.get_write_lock(get_lock_acquire_timeout()),
            )
            .await
            .map_err(|e| DiskError::other(self.map_namespace_lock_error(bucket, object, "write", e).to_string()))?
        };

        self.heal_object_dir_locked(bucket, object, dry_run, remove).await
    }

    pub(in crate::set_disk) async fn default_heal_result(
        &self,
        lfi: FileInfo,
        errs: &[Option<DiskError>],
        bucket: &str,
        object: &str,
        version_id: &str,
    ) -> HealResultItem {
        // Take a single snapshot of the disk vector and drive both `disk_len` and
        // the per-drive loop below from it, so the reported `disk_count` and the
        // pushed drive records always agree (previously two independent
        // `self.disks.read()` calls could observe different lengths).
        let disks = self.disks.read().await;
        let disk_len = disks.len();
        let mut result = HealResultItem {
            heal_item_type: HealItemType::Object.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            object_size: lfi.size as usize,
            version_id: version_id.to_string(),
            disk_count: disk_len,
            ..Default::default()
        };

        // Report the object's own parity only when it actually carries erasure
        // geometry; delete markers and geometry-less versions fall back to the
        // pool default. Uses `has_valid_erasure_geometry()` (not `is_valid()`)
        // to stay in step with the rest of the metadata-predicate migration —
        // `is_valid()` now requires full payload validation and returns `false`
        // for delete markers, which would misreport their parity here.
        if lfi.has_valid_erasure_geometry() {
            result.parity_blocks = lfi.erasure.parity_blocks;
        } else {
            result.parity_blocks = self.default_parity_count;
        }

        result.data_blocks = disk_len - result.parity_blocks;

        // `errs` is index-aligned with the disk vector; only the online path below
        // indexes into it (the offline branch `continue`s before touching it).
        debug_assert_eq!(errs.len(), disk_len, "errs length must match the disk count");

        for (index, disk) in disks.iter().enumerate() {
            if disk.is_none() {
                result.before.drives.push(HealDriveInfo {
                    uuid: "".to_string(),
                    endpoint: self.set_endpoints[index].to_string(),
                    state: DriveState::Offline.to_string(),
                });

                result.after.drives.push(HealDriveInfo {
                    uuid: "".to_string(),
                    endpoint: self.set_endpoints[index].to_string(),
                    state: DriveState::Offline.to_string(),
                });
                // Offline disks contribute exactly one record; without this the
                // control flow fell through and pushed a second (Corrupt) record
                // for the same disk, doubling the list and breaking index alignment.
                continue;
            }

            let mut drive_state = DriveState::Corrupt;
            if let Some(err) = &errs[index] {
                if err == &DiskError::FileNotFound || err == &DiskError::VolumeNotFound {
                    drive_state = DriveState::Missing;
                }
            } else {
                drive_state = DriveState::Ok;
            }

            result.before.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: self.set_endpoints[index].to_string(),
                state: drive_state.to_string(),
            });
            result.after.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: self.set_endpoints[index].to_string(),
                state: drive_state.to_string(),
            });
        }
        result
    }
}

fn selected_heal_data_dir(metadata: &FileInfo, opts: &HealOpts) -> Option<[u8; 16]> {
    if opts.read_repair || opts.dry_run || metadata.deleted || metadata.is_remote() {
        return None;
    }
    metadata.data_dir.filter(|id| !id.is_nil()).map(|id| *id.as_bytes())
}

fn finalize_object_heal_result(
    mut result: HealResultItem,
    mut error: Option<Error>,
    absence: &mut Option<HealedObjectAbsence>,
    lock_lost: bool,
    bucket: &str,
    object: &str,
) -> (HealResultItem, Option<Error>) {
    if lock_lost {
        result.integrity_verified = false;
        result.repair_verified = false;
        result.metadata_verified = false;
        result.metadata_repair_verified = false;
        result.legacy_identity_unverified = false;
        *absence = None;
        error = Some(Error::NamespaceLockQuorumUnavailable {
            mode: "write",
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            required: 1,
            achieved: 0,
        });
    }
    if error.is_some() {
        result.quorum_selected_data_dir = None;
    }
    (result, error)
}

impl SetDisks {
    #[cfg(test)]
    pub(crate) async fn heal_replacement_format(
        &self,
        dry_run: bool,
        targets: &[String],
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.heal_replacement_format_with_fence(dry_run, targets, || false).await
    }

    pub(crate) async fn heal_replacement_format_with_fence<F>(
        &self,
        dry_run: bool,
        targets: &[String],
        fence_lost: F,
    ) -> Result<(HealResultItem, Option<Error>)>
    where
        F: FnMut() -> bool + Send,
    {
        if targets.is_empty() {
            return Err(Error::other("replacement format requires at least one target"));
        }

        let mut target_slots = Vec::with_capacity(targets.len());
        for target in targets {
            let Some(slot) = self.set_endpoints.iter().position(|endpoint| endpoint.to_string() == *target) else {
                return Err(Error::other("replacement format target does not belong to the set"));
            };
            if target_slots.contains(&slot) {
                return Err(Error::other("replacement format target is duplicated"));
            }
            target_slots.push(slot);
        }

        self.heal_format_for_slots(dry_run, Some(&target_slots), fence_lost).await
    }

    async fn heal_format_for_slots<F>(
        &self,
        dry_run: bool,
        target_slots: Option<&[usize]>,
        mut fence_lost: F,
    ) -> Result<(HealResultItem, Option<Error>)>
    where
        F: FnMut() -> bool + Send,
    {
        let disks = self.disks.read().await.clone();
        let (formats, errs) = load_format_erasure_all(&disks, true).await;
        if errs.iter().any(|err| {
            matches!(
                err,
                Some(DiskError::InconsistentDisk | DiskError::CorruptedFormat | DiskError::CorruptedBackend)
            )
        }) {
            return Ok((HealResultItem::default(), Some(StorageError::CorruptedFormat)));
        }
        let slot_offset = self
            .set_index
            .checked_mul(self.set_drive_count)
            .ok_or_else(|| Error::other("erasure set slot offset overflow"))?;
        let ref_format = match get_format_erasure_in_quorum(&formats, slot_offset) {
            Ok(format) if format.shared_identity() == self.format.shared_identity() => format,
            Ok(_) => return Ok((HealResultItem::default(), Some(StorageError::CorruptedFormat))),
            Err(err) => {
                let can_use_cached_layout = count_errs(&errs, &DiskError::UnformattedDisk) > 0
                    && formats_match_reference_slots(&formats, &self.format, slot_offset)
                    && errs
                        .iter()
                        .all(|err| err.is_none() || matches!(err, Some(DiskError::UnformattedDisk)));
                if can_use_cached_layout {
                    self.format.clone()
                } else {
                    return Ok((HealResultItem::default(), Some(err)));
                }
            }
        };
        if !formats_match_reference_slots(&formats, &ref_format, slot_offset) {
            return Ok((HealResultItem::default(), Some(StorageError::CorruptedFormat)));
        }

        let endpoints = crate::layout::endpoints::Endpoints::from(self.set_endpoints.clone());
        let before_drives = crate::layout::set_heal::formats_to_drives_info(&endpoints, &formats, &errs);
        let mut result = HealResultItem {
            heal_item_type: HealItemType::Metadata.to_string(),
            detail: "disk-format".to_string(),
            disk_count: self.set_drive_count,
            set_count: 1,
            before: Infos {
                drives: before_drives.clone(),
            },
            after: Infos { drives: before_drives },
            ..Default::default()
        };

        if count_errs(&errs, &DiskError::UnformattedDisk) == 0 {
            debug!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_HEAL,
                error_count = errs.iter().flatten().count(),
                result = "no_heal_required",
                "set disk formats success"
            );
            return Ok((result, Some(StorageError::NoHealRequired)));
        }

        if !dry_run {
            for (disk_idx, err) in errs.iter().enumerate() {
                if !matches!(err, Some(DiskError::UnformattedDisk))
                    || target_slots.is_some_and(|slots| !slots.contains(&disk_idx))
                {
                    continue;
                }

                let mut new_format = ref_format.clone();
                new_format.erasure.this = ref_format.erasure.sets[self.set_index][disk_idx];
                if fence_lost() {
                    return Ok((result, Some(StorageError::SlowDown)));
                }
                match save_format_file(&disks[disk_idx], &Some(new_format.clone())).await {
                    Ok(()) => {
                        if fence_lost() {
                            return Ok((result, Some(StorageError::SlowDown)));
                        }
                        result.after.drives[disk_idx].uuid = new_format.erasure.this.to_string();
                        result.after.drives[disk_idx].state = DriveState::Ok.to_string();
                    }
                    Err(err) => return Ok((result, Some(err.into()))),
                }
            }
        }

        Ok((result, None))
    }
}

// Heal operation family: the storage-api `HealOperations` contract stays
// implemented `for SetDisks` (contract bounds unchanged) but now lives beside
// its inherent helpers in the `set_disk::ops::heal` module. Bodies are moved
// unchanged; `get_pool_and_set` reads the core through `SetDisksCtx` to keep
// the Heal family aligned with the borrow pattern from #816.
#[async_trait::async_trait]
impl crate::storage_api_contracts::heal::HealOperations for SetDisks {
    type Error = Error;
    type HealResultItem = HealResultItem;
    type HealOptions = HealOpts;

    #[tracing::instrument(skip(self))]
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        self.heal_format_for_slots(dry_run, None, || false).await
    }

    #[tracing::instrument(skip(self))]
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let mut result = heal_bucket_local_on_disks(bucket, opts, self.disk_inventory().await).await?;
        result.set_count = 1;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self, opts), fields(bucket = %bucket, object = %object, version_id = %version_id))]
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.heal_object_with_absence(
            bucket,
            object,
            version_id,
            opts,
            &mut None,
            AbsenceProofRequest {
                retirement: None,
                allow_unversioned: false,
            },
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
        let ctx = self.ctx();
        for (set_idx, set) in ctx.format().erasure.sets.iter().enumerate() {
            for (disk_idx, disk_id) in set.iter().enumerate() {
                if disk_id.to_string() == id {
                    return Ok((Some(ctx.pool_index()), Some(set_idx), Some(disk_idx)));
                }
            }
        }

        Err(Error::DiskNotFound)
    }

    #[tracing::instrument(level = "debug", skip(self, opts), fields(bucket = %bucket, object = %object, dry_run = opts.dry_run))]
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()> {
        let started_at = std::time::Instant::now();
        let _write_lock_guard = if !opts.no_lock {
            let ns_lock = self
                .new_ns_lock(bucket, object)
                .await
                .map_err(|e| e.narrow_to_disk().unwrap_or_else(DiskError::other))?;
            Some(
                measure_heal_object_lock(
                    "check_abandoned_parts",
                    HealObjectLockKind::Write,
                    ns_lock.get_write_lock(get_lock_acquire_timeout()),
                )
                .await
                .map_err(|e| {
                    self.map_namespace_lock_error(bucket, object, "write", e)
                        .narrow_to_disk()
                        .unwrap_or_else(DiskError::other)
                })?,
            )
        } else {
            None
        };

        let removed = if opts.dry_run {
            self.dry_run_reclaim_orphan_data_dirs(bucket, object).await?
        } else {
            self.reclaim_orphan_data_dirs(bucket, object).await?
        };
        let state = if opts.dry_run && removed > 0 {
            "dry_run_matched"
        } else if removed > 0 {
            "reclaimed"
        } else {
            "checked"
        };
        let data_dirs = u64::try_from(removed).unwrap_or(u64::MAX);

        trace_emit(|| {
            TraceEvent::new(TraceKind::Heal, TraceFunc::HealCheckAbandonedParts)
                .with_bucket(bucket)
                .with_object(object)
                .with_duration(started_at.elapsed())
                .with_attr("state", state)
                .with_attr("dry_run", opts.dry_run)
                .with_attr("data_dirs", data_dirs)
        });

        if removed > 0 {
            trace!(
                event = "heal_abandoned_parts",
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_HEAL,
                state = if opts.dry_run { "dry_run_matched" } else { "reclaimed" },
                result = "ok",
                bucket,
                object,
                dry_run = opts.dry_run,
                data_dirs = removed,
                "Heal abandoned parts checked object data directories"
            );
        }

        Ok(())
    }
}

impl SetDisks {
    /// The recovery coordinator retains the key write lock and lifecycle fence.
    /// Only current live metadata can supply this exact source; backups and
    /// dangling cleanup cannot authorize prerequisite repair.
    pub(in crate::set_disk) async fn heal_delete_recovery_source(
        &self,
        bucket: &str,
        object: &str,
        source: &crate::disk::DeleteRecoveryVersionIdentity,
        can_commit: &(dyn Fn() -> bool + Sync),
        namespace_owner: Option<&Arc<dyn Send + Sync>>,
    ) -> disk::error::Result<()> {
        if !can_commit() {
            return Err(DiskError::DiskOngoingReq);
        }
        let version = source.version_id.ok_or(DiskError::OutdatedXLMeta)?.to_string();
        let disks = self.get_disks_internal().await;
        let (metadata, errors, _) = Self::read_metadata(&disks, "", bucket, object, &version, true, false, false)
            .await?
            .into_legacy();
        let (read_quorum, write_quorum) = Self::object_quorum_from_meta(&metadata, &errors, self.default_parity_count)?;
        let read_quorum = usize::try_from(read_quorum).map_err(|_| DiskError::ErasureReadQuorum)?;
        let write_quorum = usize::try_from(write_quorum).map_err(|_| DiskError::ErasureWriteQuorum)?;
        let live_sources = metadata
            .iter()
            .zip(&errors)
            .filter(|(info, error)| {
                error.is_none()
                    && !info.deleted
                    && !info.is_remote()
                    && crate::disk::DeleteRecoveryVersionIdentity::from_file_info((*info).clone())
                        .is_ok_and(|identity| identity == *source)
            })
            .count();
        if live_sources < write_quorum || !can_commit() {
            return Err(DiskError::OutdatedXLMeta);
        }
        let (_, mod_time, etag) = Self::list_online_disks(&disks, &metadata, &errors, read_quorum);
        let selected = Self::pick_valid_fileinfo(&metadata, mod_time, etag, read_quorum)?;
        if crate::disk::DeleteRecoveryVersionIdentity::from_file_info(selected)? != *source {
            return Err(DiskError::OutdatedXLMeta);
        }
        let opts = HealOpts {
            no_lock: true,
            scan_mode: HealScanMode::Deep,
            ..Default::default()
        };
        let (_, error) = Box::pin(self.heal_object_with_explicit_version_regen(
            bucket,
            object,
            &version,
            ExplicitVersionHeal {
                opts: &opts,
                allow_regeneration: false,
                preserve_unrepairable: true,
                commit_fence: Some(can_commit),
                namespace_owner,
                retirement: None,
            },
            &mut None,
        ))
        .await?;
        if let Some(error) = error {
            return Err(error);
        }
        if !can_commit() {
            return Err(DiskError::DiskOngoingReq);
        }
        Ok(())
    }

    pub(crate) async fn heal_object_with_absence(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
        absence: &mut Option<HealedObjectAbsence>,
        proof: AbsenceProofRequest<'_>,
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.heal_object_with_retirement(bucket, object, version_id, opts, absence, proof)
            .await
    }

    pub(crate) async fn heal_object_with_retirement(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
        absence: &mut Option<HealedObjectAbsence>,
        proof: AbsenceProofRequest<'_>,
    ) -> Result<(HealResultItem, Option<Error>)> {
        *absence = None;
        if opts.read_repair
            && !opts.no_lock
            && !opts.dry_run
            && !opts.remove
            && !has_suffix(object, SLASH_SEPARATOR)
            && !crate::bucket::utils::is_meta_bucketname(bucket)
        {
            // Read repair may stage under a shared lock, but must not perform
            // destructive cleanup or produce an authoritative absence proof.
            let can_commit = || proof.retirement.is_none_or(|context| !context.lifecycle_guard.is_lock_lost());
            let mut read_opts = *opts;
            loop {
                let (result, error) = pin_object_heal_future(|| {
                    self.heal_object_with_explicit_version_regen(
                        bucket,
                        object,
                        version_id,
                        ExplicitVersionHeal {
                            opts: &read_opts,
                            allow_regeneration: false,
                            preserve_unrepairable: true,
                            commit_fence: Some(&can_commit),
                            namespace_owner: None,
                            retirement: None,
                        },
                        absence,
                    )
                })
                .await
                .map_err(|error| to_object_err(error.into(), vec![bucket, object]))?;
                if !can_commit() {
                    return Err(DiskError::DiskOngoingReq.into());
                }
                if matches!(error, Some(DiskError::FileCorrupt)) && read_opts.scan_mode != HealScanMode::Deep {
                    read_opts.scan_mode = HealScanMode::Deep;
                    continue;
                }
                return Ok((result, error.map(Into::into)));
            }
        }
        let _write_lock_guard = if !opts.no_lock {
            let ns_lock = self
                .new_ns_lock(bucket, object)
                .await
                .map_err(|e| e.narrow_to_disk().unwrap_or_else(DiskError::other))?;
            Some(Arc::new(
                measure_heal_object_lock(
                    "heal_object",
                    HealObjectLockKind::Write,
                    ns_lock.get_write_lock(get_lock_acquire_timeout()),
                )
                .await
                .map_err(|e| {
                    self.map_namespace_lock_error(bucket, object, "write", e)
                        .narrow_to_disk()
                        .unwrap_or_else(DiskError::other)
                })?,
            ))
        } else {
            None
        };

        if has_suffix(object, SLASH_SEPARATOR) {
            let (result, err) = self.heal_object_dir_locked(bucket, object, opts.dry_run, opts.remove).await?;
            return Ok((result, err.map(|e| e.into())));
        }

        // Keep the transaction key through both replay and the metadata heal
        // based on that replay. The raw target key always precedes this key.
        let transaction_guard =
            if _write_lock_guard.is_some() && !opts.read_repair && !crate::bucket::utils::is_meta_bucketname(bucket) {
                Some(self.acquire_delete_transaction_lock(bucket, object).await?)
            } else {
                None
            };
        let locks_lost = || {
            _write_lock_guard.as_deref().is_some_and(|guard| guard.is_lock_lost())
                || transaction_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
        };
        let mut recovery_removed_version = false;
        if !opts.read_repair
            && _write_lock_guard.is_some()
            && let Some(scope) = crate::store::bucket_heal_scope_for_object(bucket, object)
            && !crate::bucket::utils::is_meta_bucketname(bucket)
        {
            let explicit_delete = opts.remove
                && version_id.is_empty()
                && crate::bucket::metadata_sys::has_authoritative_never_versioned_state_in(&self.ctx, bucket).await?
                && matches!(
                    crate::bucket::metadata_sys::get_object_lock_config_state_in(&self.ctx, bucket).await?,
                    crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent
                );
            let recovery = self
                .recover_delete_key(
                    bucket,
                    object,
                    crate::set_disk::DeleteRecoveryContext {
                        dry_run: opts.dry_run,
                        explicit_delete,
                        repair_marker_source: true,
                        transaction_guard: transaction_guard.clone(),
                        namespace_owner: (!opts.dry_run).then(|| {
                            Arc::new((
                                scope.clone(),
                                self.ctx.begin_namespace_commit(),
                                _write_lock_guard.clone(),
                                transaction_guard.clone(),
                            )) as Arc<dyn Send + Sync>
                        }),
                        ..crate::set_disk::DeleteRecoveryContext::new(Some(scope.incarnation), &|| {
                            scope.check().is_ok() && !locks_lost()
                        })
                    },
                )
                .await?;
            recovery_removed_version = if version_id.is_empty() {
                !recovery.removed_versions.is_empty()
            } else {
                let target = if version_id == "null" {
                    Some(Uuid::nil())
                } else {
                    Uuid::parse_str(version_id).ok()
                };
                target.is_some_and(|id| recovery.removed_versions.contains(&id))
            };
            if opts.dry_run && recovery.discovered > 0 {
                return Ok((
                    HealResultItem {
                        heal_item_type: HealItemType::Object.to_string(),
                        bucket: bucket.to_owned(),
                        object: object.to_owned(),
                        version_id: version_id.to_owned(),
                        detail: format!(
                            "delete recovery (dry run): discovered={}, committed={}, rolled_back={}, reclaimed={}, blocked={}, pending={}",
                            recovery.discovered,
                            recovery.committed,
                            recovery.rolled_back,
                            recovery.reclaimed,
                            recovery.blocked,
                            recovery.pending
                        ),
                        ..Default::default()
                    },
                    None,
                ));
            }
            if let Some(error) = recovery.unresolved_heal_error() {
                return Err(error);
            }
        }

        // The inner heal and missing-object report read the registry again;
        // release this snapshot guard before a topology writer can queue between reads.
        let disks = self.get_disks_internal().await;
        let (_, errs, _) = Self::read_metadata(&disks, "", bucket, object, version_id, false, false, false)
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?
            .into_legacy();
        if DiskError::is_all_not_found(&errs) {
            debug!(
                event = EVENT_SET_DISK_HEAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket,
                object,
                version_id,
                state = "missing_object_skipped",
                "Set disk heal skipped missing object"
            );
            let err = if !version_id.is_empty() {
                Error::FileVersionNotFound
            } else {
                Error::FileNotFound
            };
            if version_id.is_empty()
                && (opts.remove || opts.dry_run)
                && let Some(cleanup) = self
                    .cleanup_metadata_less_data_dirs(bucket, object, &disks, opts.dry_run)
                    .await
                    .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?
            {
                let mut result = self
                    .metadata_less_data_dir_heal_result(bucket, object, &cleanup, opts.dry_run)
                    .await;
                result.detail = format!(
                    "metadata-less data directories matched={}, removed={}, dry_run={}",
                    cleanup.matched, cleanup.removed, opts.dry_run
                );
                let err = cleanup.first_error.map(Error::from).or(Some(err));
                return Ok((result, err));
            }
            let result = self
                .default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                .await;
            // Check the lease after the final await before publishing the proof.
            if !opts.dry_run
                && (proof.allow_unversioned || !version_id.is_empty())
                && !disks.is_empty()
                && disks.iter().all(Option::is_some)
                && errs
                    .iter()
                    .all(|err| matches!(err, Some(DiskError::FileNotFound | DiskError::FileVersionNotFound)))
                && !locks_lost()
            {
                *absence = Some(HealedObjectAbsence {
                    pool_index: self.pool_index,
                    set_index: self.set_index,
                    removed: recovery_removed_version,
                });
            }
            return Ok((result, Some(err)));
        }

        // Heal the object.
        // Pass no_lock=true since we already obtained write lock (or are already called with no_lock=true)
        let mut inner_opts = *opts;
        inner_opts.no_lock = true;
        let commit_fence = || !locks_lost();
        let namespace_owner = _write_lock_guard
            .as_ref()
            .map(|guard| Arc::new((Arc::clone(guard), transaction_guard.clone())) as Arc<dyn Send + Sync>);
        let (result, err) = pin_object_heal_future(|| {
            self.heal_object_with_explicit_version_regen(
                bucket,
                object,
                version_id,
                ExplicitVersionHeal {
                    opts: &inner_opts,
                    allow_regeneration: true,
                    preserve_unrepairable: false,
                    commit_fence: Some(&commit_fence),
                    namespace_owner: namespace_owner.as_ref(),
                    retirement: proof.retirement,
                },
                absence,
            )
        })
        .await
        .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;
        if let Some(err) = err.as_ref() {
            match err {
                &DiskError::FileCorrupt if opts.scan_mode != HealScanMode::Deep => {
                    // Instead of returning an error when a bitrot error is detected
                    // during a normal heal scan, heal again with bitrot flag enabled.
                    inner_opts.scan_mode = HealScanMode::Deep;
                    let (result, err) = pin_object_heal_future(|| {
                        self.heal_object_with_explicit_version_regen(
                            bucket,
                            object,
                            version_id,
                            ExplicitVersionHeal {
                                opts: &inner_opts,
                                allow_regeneration: true,
                                preserve_unrepairable: false,
                                commit_fence: Some(&commit_fence),
                                namespace_owner: namespace_owner.as_ref(),
                                retirement: proof.retirement,
                            },
                            absence,
                        )
                    })
                    .await
                    .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;
                    return Ok(finalize_object_heal_result(
                        result,
                        err.map(Into::into),
                        absence,
                        locks_lost(),
                        bucket,
                        object,
                    ));
                }
                _ => {}
            }
        }
        Ok(finalize_object_heal_result(
            result,
            err.map(Into::into),
            absence,
            locks_lost(),
            bucket,
            object,
        ))
    }
}

impl SetDisks {
    async fn dangling_heal_result(
        &self,
        metadata: FileInfo,
        errs: &[Option<DiskError>],
        bucket: &str,
        object: &str,
        version_id: &str,
        absent: bool,
    ) -> HealResultItem {
        let mut item = self.default_heal_result(metadata, errs, bucket, object, version_id).await;
        if absent {
            for drive in &mut item.after.drives {
                drive.state = DriveState::Missing.to_string();
            }
        }
        item
    }
}

#[cfg(test)]
mod heal_result_report_tests {
    use super::{
        AbsenceProofRequest, DanglingCheckPartsFailure, DanglingDeleteFailure, DanglingDeleteSafety, ExplicitVersionHeal,
        HealWriterFailures, ReadRepairCommitFingerprint, ReadRepairPauseScope, SetDisks, heal_writer_error_summary,
        is_retryable_heal_writer_error,
    };
    use super::{HEAL_RENAME_INCOMPLETE, HealRenameFailureScope, HealWriterFailureScope, heal_rollback_backup_dir};
    use crate::disk::endpoint::Endpoint;
    use crate::disk::error::DiskError;
    use crate::disk::format::FormatV3;
    use crate::disk::local::RESERVED_DELETE_DATA_DIR_MARKER_PREFIX;
    use crate::disk::{DiskAPI as _, DiskOption, DiskStore, RUSTFS_META_TMP_BUCKET, ReadOptions, STORAGE_FORMAT_FILE, new_disk};
    use crate::error::Error;
    use crate::object_api::{ObjectOptions, PutObjReader};
    use crate::set_disk::ops::object::hermetic_set_disks_support::{
        hermetic_set_disks_for_pool_with_default_parity_isolated, hermetic_set_disks_isolated,
    };
    use crate::storage_api_contracts::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
    use crate::storage_api_contracts::heal::HealOperations as _;
    use crate::storage_api_contracts::namespace::NamespaceLocking as _;
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
    use crate::{
        config::storageclass,
        store::init_format::{load_format_erasure, save_format_file},
    };
    use bytes::Bytes;
    use rustfs_filemeta::{BLOCK_SIZE_V2, FileInfo, ObjectPartInfo, TRANSITION_COMPLETE};
    use rustfs_heal_contracts::heal_channel::{DriveState, HealOpts, HealScanMode};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use time::OffsetDateTime;
    use tokio::sync::RwLock;
    use tracing_subscriber::fmt::MakeWriter;
    use uuid::Uuid;

    #[test]
    fn lost_object_lock_revokes_storage_proofs_and_returns_retryable_error() {
        let mut absence = Some(super::HealedObjectAbsence {
            pool_index: 2,
            set_index: 3,
            removed: true,
        });
        let result = rustfs_madmin::heal_commands::HealResultItem {
            integrity_verified: true,
            repair_verified: true,
            metadata_verified: true,
            metadata_repair_verified: true,
            legacy_identity_unverified: true,
            quorum_selected_data_dir: Some([1; 16]),
            ..Default::default()
        };

        let (result, error) = super::finalize_object_heal_result(result, None, &mut absence, true, "bucket", "object");

        assert!(!result.integrity_verified);
        assert!(!result.repair_verified);
        assert!(!result.metadata_verified);
        assert!(!result.metadata_repair_verified);
        assert!(!result.legacy_identity_unverified);
        assert_eq!(result.quorum_selected_data_dir, None);
        assert!(absence.is_none());
        assert!(matches!(
            error,
            Some(Error::NamespaceLockQuorumUnavailable {
                mode: "write",
                bucket,
                object,
                required: 1,
                achieved: 0,
            }) if bucket == "bucket" && object == "object"
        ));
    }

    #[test]
    fn selected_data_dir_is_a_quorum_metadata_observation() {
        let generation = Uuid::new_v4();
        let mut metadata = FileInfo {
            data_dir: Some(generation),
            ..Default::default()
        };
        let opts = HealOpts::default();
        assert_eq!(super::selected_heal_data_dir(&metadata, &opts), Some(*generation.as_bytes()));
        assert_eq!(
            super::selected_heal_data_dir(&metadata, &HealOpts { no_lock: true, ..opts }),
            Some(*generation.as_bytes())
        );
        assert_eq!(super::selected_heal_data_dir(&metadata, &HealOpts { dry_run: true, ..opts }), None);
        assert_eq!(
            super::selected_heal_data_dir(
                &metadata,
                &HealOpts {
                    read_repair: true,
                    ..opts
                }
            ),
            None
        );
        metadata.data_dir = Some(Uuid::nil());
        assert_eq!(super::selected_heal_data_dir(&metadata, &opts), None);
        metadata.data_dir = None;
        assert_eq!(super::selected_heal_data_dir(&metadata, &opts), None);
        metadata.data_dir = Some(generation);
        metadata.deleted = true;
        assert_eq!(super::selected_heal_data_dir(&metadata, &opts), None);
        metadata.deleted = false;
        metadata.transition_status = TRANSITION_COMPLETE.to_owned();
        assert_eq!(super::selected_heal_data_dir(&metadata, &opts), None);
    }

    #[test]
    fn failed_heal_does_not_publish_selected_generation() {
        let result = rustfs_madmin::heal_commands::HealResultItem {
            quorum_selected_data_dir: Some([1; 16]),
            ..Default::default()
        };
        let mut absence = None;
        let (result, error) =
            super::finalize_object_heal_result(result, Some(Error::FileNotFound), &mut absence, false, "b", "o");
        assert!(error.is_some());
        assert_eq!(result.quorum_selected_data_dir, None);
    }

    #[tokio::test]
    async fn heal_result_observes_current_generation_after_unversioned_overwrite() {
        let (_temp_dirs, _disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "heal-result-generation";
        let object = "object.bin";
        set.make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create test bucket");

        let put_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let mut first = PutObjReader::from_vec(vec![0x11; 1024 * 1024]);
        let first_info = set
            .put_object(bucket, object, &mut first, &put_opts)
            .await
            .expect("write first generation");
        let first_generation = first_info.data_dir.expect("first object data directory");
        let heal_opts = HealOpts {
            scan_mode: HealScanMode::Deep,
            ..Default::default()
        };
        let (first_result, first_error) =
            crate::storage_api_contracts::heal::HealOperations::heal_object(set.as_ref(), bucket, object, "", &heal_opts)
                .await
                .expect("heal first generation");
        assert!(first_error.is_none());
        assert_eq!(first_result.quorum_selected_data_dir, Some(*first_generation.as_bytes()));

        let mut second = PutObjReader::from_vec(vec![0x22; 1024 * 1024]);
        let second_info = set
            .put_object(bucket, object, &mut second, &put_opts)
            .await
            .expect("overwrite unversioned object");
        let second_generation = second_info.data_dir.expect("second object data directory");
        assert_ne!(first_generation, second_generation);
        let (second_result, second_error) =
            crate::storage_api_contracts::heal::HealOperations::heal_object(set.as_ref(), bucket, object, "", &heal_opts)
                .await
                .expect("heal second generation");
        assert!(second_error.is_none());
        assert_eq!(second_result.quorum_selected_data_dir, Some(*second_generation.as_bytes()));
        assert_ne!(second_result.quorum_selected_data_dir, first_result.quorum_selected_data_dir);

        let (unlocked_result, unlocked_error) = crate::storage_api_contracts::heal::HealOperations::heal_object(
            set.as_ref(),
            bucket,
            object,
            "",
            &HealOpts {
                no_lock: true,
                ..heal_opts
            },
        )
        .await
        .expect("observe quorum selection without a namespace lock");
        assert!(unlocked_error.is_none());
        assert_eq!(unlocked_result.quorum_selected_data_dir, Some(*second_generation.as_bytes()));

        for opts in [
            HealOpts {
                read_repair: true,
                ..heal_opts
            },
            HealOpts {
                dry_run: true,
                ..heal_opts
            },
        ] {
            let (result, error) =
                crate::storage_api_contracts::heal::HealOperations::heal_object(set.as_ref(), bucket, object, "", &opts)
                    .await
                    .expect("observe heal without an eligible lock");
            assert!(error.is_none());
            assert_eq!(result.quorum_selected_data_dir, None);
        }
    }

    #[test]
    fn absent_and_nil_selected_versions_are_the_same_null_identity() {
        let requested_nil = Uuid::nil().to_string();
        let requested = Uuid::parse_str(&requested_nil).expect("nil UUID string");
        assert!(requested.is_nil());

        let absent: Option<Uuid> = None;
        let selected_nil = absent.is_none_or(|selected| selected.is_nil());
        assert!(selected_nil, "absent metadata version selects the null identity");

        let selected = Uuid::new_v4();
        assert!(!selected.is_nil(), "a concrete UUID must not satisfy a requested null selector");
    }

    #[test]
    fn metadata_less_part_file_accepts_positive_part_numbers_only() {
        assert!(super::metadata_less_part_file("part.1"));
        assert!(super::metadata_less_part_file("part.42"));
        assert!(!super::metadata_less_part_file("part.0"));
        assert!(!super::metadata_less_part_file("part."));
        assert!(!super::metadata_less_part_file("part.x"));
        assert!(!super::metadata_less_part_file("xl.meta"));
    }

    #[test]
    fn unavailable_heal_errors_use_stable_drive_states() {
        for error in [DiskError::FaultyDisk, DiskError::FaultyRemoteDisk] {
            assert_eq!(super::heal_drive_state_for_error(&error).to_string(), DriveState::Faulty.to_string());
        }
        assert_eq!(
            super::heal_drive_state_for_error(&DiskError::RemoteClientUnavailable("peer restarting".to_string())).to_string(),
            DriveState::Offline.to_string()
        );
    }

    #[tokio::test]
    async fn heal_preserves_rpc_transport_failure_through_metadata_classification() {
        let transport = tonic::transport::Endpoint::from_static("http://127.0.0.1:0")
            .connect()
            .await
            .expect_err("port zero cannot serve an internode connection");
        let mut interrupted = tonic::Status::internal("h2 protocol error: http2 error");
        interrupted.set_source(Arc::new(transport));
        for (status, offline) in [
            (interrupted, true),
            (tonic::Status::unavailable("peer restarting"), true),
            (tonic::Status::internal("h2 protocol error: http2 error"), false),
            (tonic::Status::permission_denied("connection reset"), false),
            (tonic::Status::data_loss("broken pipe"), false),
        ] {
            let error = DiskError::from(status);
            let (_, _, reason) = super::should_heal_object_on_disk(&Some(error), &[], &FileInfo::default(), &FileInfo::default());
            let reason = reason.expect("metadata probe failure must survive classification");
            assert_eq!(
                matches!(super::heal_drive_state_for_error(&reason), DriveState::Offline),
                offline,
                "{reason:?}"
            );
        }
    }

    #[test]
    fn read_repair_commit_fingerprint_tracks_commit_identity_only() {
        let data_dir = Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("data dir should parse");
        let mut base = meta_regen_test_fileinfo("object.bin", data_dir, 42, 0);
        base.volume = "bucket-a".to_string();
        base.metadata.insert("etag".to_string(), "etag-a".to_string());

        let mut observed = base.clone();
        observed.idx = 3;
        observed.fresh = true;
        observed.num_versions = 7;
        observed.erasure.index = base.erasure.index.saturating_add(1);
        observed.successor_mod_time = Some(OffsetDateTime::from_unix_timestamp(43).expect("timestamp should parse"));

        assert!(
            ReadRepairCommitFingerprint::from_file_info(&base) == ReadRepairCommitFingerprint::from_file_info(&observed),
            "read-observation and disk-local fields must not make an unchanged object look stale"
        );

        let mut overwritten = base.clone();
        overwritten.data_dir = Some(Uuid::parse_str("22222222-2222-2222-2222-222222222222").expect("data dir should parse"));
        assert!(
            ReadRepairCommitFingerprint::from_file_info(&base) != ReadRepairCommitFingerprint::from_file_info(&overwritten),
            "a new committed data directory must abort the stale read-repair commit"
        );

        let mut metadata_changed = base.clone();
        metadata_changed.metadata.insert("etag".to_string(), "etag-b".to_string());
        assert!(
            ReadRepairCommitFingerprint::from_file_info(&base) != ReadRepairCommitFingerprint::from_file_info(&metadata_changed),
            "metadata changes must abort the stale read-repair commit"
        );
    }

    #[derive(Clone, Default)]
    struct CapturedLogs {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            let buffer = self
                .buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .clone();
            String::from_utf8(buffer).expect("captured logs should be valid UTF-8")
        }
    }

    impl std::io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    async fn remove_current_object_part(temp_dir: &TempDir, bucket: &str, object: &str) -> std::io::Result<()> {
        let object_dir = temp_dir.path().join(bucket).join(object);
        let mut entries = tokio::fs::read_dir(&object_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let part = entry.path().join("part.1");
            match tokio::fs::remove_file(&part).await {
                Ok(()) => return Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("no current part.1 found under {}", object_dir.display()),
        ))
    }

    #[test]
    fn heal_writer_error_summary_redacts_io_message() {
        let error = DiskError::Io(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "/sensitive/storage/path"));

        let summary = heal_writer_error_summary(&error);

        assert_eq!(summary, "io::PermissionDenied");
        assert!(!summary.contains("sensitive"));
    }

    #[test]
    fn retryable_heal_writer_error_keeps_permanent_errors_out() {
        for error in [
            DiskError::DiskNotFound,
            DiskError::Timeout,
            DiskError::SourceStalled,
            DiskError::FaultyRemoteDisk,
            DiskError::FaultyDisk,
            DiskError::RemoteClientUnavailable("peer restarting".to_string()),
        ] {
            assert!(is_retryable_heal_writer_error(&error), "{error:?} should be retryable");
        }

        for error in [DiskError::DiskFull, DiskError::DiskAccessDenied, DiskError::FileCorrupt] {
            assert!(!is_retryable_heal_writer_error(&error), "{error:?} should remain permanent");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial]
    async fn deep_heal_preserves_below_quorum_external_parts_without_remove() {
        use tokio::io::AsyncReadExt;

        for grace in ["3600", "0"] {
            let scenario = async {
                for versioned in [false, true] {
                    let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
                    let bucket = "heal-below-read-quorum";
                    let object = "external.bin";
                    for disk in &disks {
                        disk.make_volume(bucket).await.expect("create each bucket volume");
                    }
                    let body = vec![0x5a; 1024 * 1024];
                    let version = versioned.then(|| Uuid::new_v4().to_string());
                    let put_options = ObjectOptions {
                        no_lock: true,
                        versioned,
                        version_id: version.clone(),
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    };
                    let mut reader = PutObjReader::from_vec(body.clone());
                    set.put_object(bucket, object, &mut reader, &put_options)
                        .await
                        .expect("commit every original external shard");
                    if versioned {
                        let mut newer = PutObjReader::from_vec(vec![0xa5; body.len()]);
                        set.put_object(
                            bucket,
                            object,
                            &mut newer,
                            &ObjectOptions {
                                version_id: Some(Uuid::new_v4().to_string()),
                                ..put_options.clone()
                            },
                        )
                        .await
                        .expect("commit a distinct current version above the historical target");
                    }
                    let version_id = version.as_deref().unwrap_or("");
                    let source = disks[3]
                        .read_version("", bucket, object, version_id, &ReadOptions::default())
                        .await
                        .expect("read the exact source version");
                    assert_eq!(source.erasure.data_blocks, 2, "fixture must have R=2");
                    let part_path = format!("{object}/{}/part.1", source.data_dir.expect("external data directory"));
                    let mut metadata = Vec::new();
                    let mut original_parts = Vec::new();
                    for disk in &disks {
                        metadata.push(
                            disk.read_all(bucket, &format!("{object}/xl.meta"))
                                .await
                                .expect("original metadata"),
                        );
                        original_parts.push(disk.read_all(bucket, &part_path).await.expect("original external part"));
                    }
                    for dir in temp_dirs.iter().take(3) {
                        tokio::fs::remove_file(dir.path().join(bucket).join(&part_path))
                            .await
                            .expect("leave exactly R-1 external shards");
                    }
                    let opts = HealOpts {
                        no_lock: true,
                        scan_mode: HealScanMode::Deep,
                        remove: false,
                        ..Default::default()
                    };
                    let (result, error) = set
                        .heal_object(bucket, object, version_id, &opts)
                        .await
                        .expect("negative deep heal");
                    assert!(
                        matches!(error, Some(DiskError::ErasureReadQuorum)),
                        "a no-remove heal with surviving same-version data must retain the quorum failure, not classify cleanup grace: {error:?}"
                    );
                    assert!(!result.integrity_verified, "below-R data cannot carry a positive integrity proof");
                    for (index, disk) in disks.iter().enumerate() {
                        assert_eq!(
                            disk.read_all(bucket, &format!("{object}/xl.meta"))
                                .await
                                .expect("retained metadata"),
                            metadata[index],
                            "negative heal must preserve every original metadata byte"
                        );
                        if index < 3 {
                            assert!(
                                disk.read_all(bucket, &part_path).await.is_err(),
                                "below-R heal cannot invent a missing shard"
                            );
                        } else {
                            assert_eq!(disk.read_all(bucket, &part_path).await.expect("surviving shard"), original_parts[index]);
                        }
                    }
                    disks[0]
                        .write_all(bucket, &part_path, original_parts[0].clone())
                        .await
                        .expect("restore one original source");
                    let (_, error) = set
                        .heal_object(bucket, object, version_id, &opts)
                        .await
                        .expect("fresh heal at exactly R");
                    assert!(error.is_none(), "exactly R original sources must repair independently: {error:?}");
                    let mut restored = set
                        .get_object_reader(bucket, object, None, Default::default(), &put_options)
                        .await
                        .expect("read the repaired exact version");
                    let mut actual = Vec::new();
                    restored
                        .stream
                        .read_to_end(&mut actual)
                        .await
                        .expect("read every repaired payload byte");
                    assert_eq!(actual, body, "later successful repair cannot hide data loss");
                    if grace == "0" {
                        drop(restored);
                        for dir in temp_dirs.iter().take(3) {
                            tokio::fs::remove_file(dir.path().join(bucket).join(&part_path))
                                .await
                                .expect("prepare the explicit removal control");
                        }
                        let (_, error) = set
                            .heal_object(bucket, object, version_id, &HealOpts { remove: true, ..opts })
                            .await
                            .expect("explicit dangling cleanup control");
                        assert!(
                            error.is_none(),
                            "explicit removal with zero grace must still clean dangling data: {error:?}"
                        );
                        for disk in &disks {
                            assert!(
                                disk.read_version("", bucket, object, version_id, &ReadOptions::default())
                                    .await
                                    .is_err(),
                                "explicit cleanup must remove the target version"
                            );
                        }
                    }
                }
            };
            temp_env::async_with_vars([("RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS", Some(grace))], scenario).await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial]
    async fn heal_writer_failures_emit_one_aggregate_warning_per_object() {
        for (case, target_count, failed_target_count, injected_error) in [
            ("partial", 2usize, 1usize, DiskError::DiskFull),
            ("all-full", 2, 2, DiskError::DiskFull),
            ("all-timeout", 2, 2, DiskError::Timeout),
            ("single-faulty", 1, 1, DiskError::FaultyDisk),
        ] {
            let expected_result = if target_count == failed_target_count {
                "all_targets_unavailable"
            } else {
                "partial_targets_unavailable"
            };
            let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
            let bucket = format!("heal-writer-{case}");
            let object = "object.bin";
            for disk in &disks {
                disk.make_volume(&bucket).await.expect("bucket volume should be created");
            }

            let mut reader = PutObjReader::from_vec(vec![0x5a; 1024 * 1024]);
            // This fixture reads and removes physical shards immediately after
            // PUT. A lock-owning PUT may quorum-ack before its rename tail
            // drains, so keep the setup on the full-fanout commit path.
            set.put_object(
                &bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source object should be written");
            let source = disks[2]
                .read_version("", &bucket, object, "", &ReadOptions::default())
                .await
                .expect("source metadata should be readable");
            let mut target_slots = (0..target_count)
                .map(|index| source.erasure.distribution[index] - 1)
                .collect::<Vec<_>>();
            target_slots.sort_unstable();

            for dir in temp_dirs.iter().take(target_count) {
                remove_current_object_part(dir, &bucket, object)
                    .await
                    .expect("target shard should be removed before heal");
            }

            let failed_slots = &target_slots[..failed_target_count];
            let logs = CapturedLogs::default();
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::WARN)
                .with_writer(logs.clone())
                .with_ansi(false)
                .without_time()
                .finish();
            let subscriber_guard = tracing::subscriber::set_default(subscriber);
            let tmp_before = non_trash_tmp_entries(&temp_dirs).await;
            let injected_summary = heal_writer_error_summary(&injected_error);
            let failure_scope = HealWriterFailureScope::install(&bucket, object, None, failed_slots, injected_error.clone());

            let heal_outcome = set
                .heal_object(
                    &bucket,
                    object,
                    "",
                    &HealOpts {
                        no_lock: true,
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await;
            drop(failure_scope);
            drop(subscriber_guard);

            let (_, error) = heal_outcome.expect("writer setup failures must retain a heal report and their typed error");
            assert_eq!(
                error,
                (target_count == failed_target_count).then_some(injected_error),
                "{case}: partial repair succeeds and all-target failure retains its cause"
            );
            assert_eq!(
                non_trash_tmp_entries(&temp_dirs).await,
                tmp_before,
                "{case}: heal must clean temporary shards"
            );
            let output = logs.contents();
            assert!(
                !output.contains("Erasure encode write quorum unavailable"),
                "{case}: unavailable heal targets must not fabricate an all-disk encode failure: {output}"
            );
            assert_eq!(
                output.matches("Set disk object heal writer failures").count(),
                1,
                "{case}: writer failures must emit one aggregate warning per object: {output}"
            );
            assert!(
                output.contains(&format!("writer_failure_count={failed_target_count}")),
                "{case}: warning must report the aggregate failure count: {output}"
            );
            assert!(
                output.contains(&format!("first_disk_index={}", failed_slots[0])),
                "{case}: warning must report the first failed target: {output}"
            );
            assert!(
                output.contains("first_part_number=1"),
                "{case}: warning must report the first failed part: {output}"
            );
            assert!(
                output.contains(&format!("result=\"{expected_result}\"")),
                "{case}: warning must distinguish partial from all-target failure: {output}"
            );
            assert!(
                output.contains(&format!("error={injected_summary}")),
                "{case}: warning must preserve a redacted failure reason: {output}"
            );
        }
    }

    #[test]
    fn heal_writer_failure_aggregation_retains_permanent_cause() {
        for errors in [
            [DiskError::Timeout, DiskError::DiskFull],
            [DiskError::DiskFull, DiskError::Timeout],
        ] {
            let mut failures = HealWriterFailures::new(2);
            for (index, error) in errors.into_iter().enumerate() {
                failures.record(1, index, error);
            }
            assert_eq!(failures.count, 2);
            assert_eq!(failures.into_error(), DiskError::DiskFull);
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn heal_second_part_failures_clean_staged_parts_and_retry() {
        use crate::set_disk::CompletePart;
        use crate::storage_api_contracts::multipart::MultipartOperations as _;
        use tokio::io::AsyncReadExt as _;

        let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "heal-writer-second-part";
        let object = "multipart.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("fixture bucket");
        }
        let options = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let upload = set
            .new_multipart_upload(bucket, object, &options)
            .await
            .expect("multipart upload");
        let payloads = [vec![0x35; 5 * 1024 * 1024], vec![0x69; 1024 * 1024]];
        let mut completed = Vec::new();
        for (index, payload) in payloads.iter().enumerate() {
            let part = set
                .put_object_part(
                    bucket,
                    object,
                    &upload.upload_id,
                    index + 1,
                    &mut PutObjReader::from_vec(payload.clone()),
                    &options,
                )
                .await
                .expect("multipart part");
            completed.push(CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            });
        }
        set.clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, completed, &options)
            .await
            .expect("completed multipart object");
        let source = disks[2]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("healthy source metadata");
        let data_dir = source.data_dir.expect("multipart data directory").to_string();
        let target_slots = [source.erasure.distribution[0] - 1, source.erasure.distribution[1] - 1];
        for dir in &dirs[..2] {
            tokio::fs::remove_dir_all(dir.path().join(bucket).join(object).join(&data_dir))
                .await
                .expect("remove target shards");
        }
        let tmp_before = non_trash_tmp_entries(&dirs).await;
        let scope = HealWriterFailureScope::install(bucket, object, Some(2), &target_slots, DiskError::Timeout);
        let heal_options = HealOpts {
            no_lock: true,
            scan_mode: HealScanMode::Deep,
            ..Default::default()
        };
        let (_, error) = set
            .heal_object(bucket, object, "", &heal_options)
            .await
            .expect("failed heal report");
        drop(scope);
        assert_eq!(error, Some(DiskError::Timeout));
        assert_eq!(
            non_trash_tmp_entries(&dirs).await,
            tmp_before,
            "first-part staging must be cleaned after the second-part setup failure"
        );

        let source_path = dirs[2].path().join(bucket).join(object).join(&data_dir).join("part.2");
        let source_bytes = tokio::fs::read(&source_path).await.expect("committed source shard");
        let mut metadata_before = Vec::new();
        for dir in &dirs {
            metadata_before.push(
                tokio::fs::read(dir.path().join(bucket).join(object).join("xl.meta"))
                    .await
                    .expect("committed metadata before reconstruction failure"),
            );
        }
        let (pause_scope, arrived, release) =
            ReadRepairPauseScope::install_at(bucket, object, super::ReadRepairPausePhase::PartStaging(2));
        let lose_source = async {
            arrived.notified().await;
            assert_ne!(
                non_trash_tmp_entries(&dirs).await,
                tmp_before,
                "the first part must already be staged before the source loss"
            );
            tokio::fs::remove_file(&source_path)
                .await
                .expect("lose one remaining source shard");
            release.notify_one();
        };
        let (outcome, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(set.heal_object(bucket, object, "", &heal_options), lose_source)
        })
        .await
        .expect("heal must report source loss without waiting indefinitely");
        drop(pause_scope);
        assert_eq!(
            outcome.expect_err("source loss after writer admission must preserve the outer failure"),
            DiskError::ErasureReadQuorum
        );
        assert_eq!(
            non_trash_tmp_entries(&dirs).await,
            tmp_before,
            "reconstruction failure must clean every staged part"
        );
        for (dir, expected) in dirs.iter().zip(metadata_before) {
            assert_eq!(
                tokio::fs::read(dir.path().join(bucket).join(object).join("xl.meta"))
                    .await
                    .expect("metadata after reconstruction failure"),
                expected,
                "a failed data stage must not publish replacement metadata"
            );
        }
        tokio::fs::write(&source_path, source_bytes)
            .await
            .expect("restore the committed source shard");

        let (_, error) = set
            .heal_object(bucket, object, "", &heal_options)
            .await
            .expect("retry after target recovery");
        assert!(error.is_none(), "recovered targets should heal both parts: {error:?}");
        let mut reader = set
            .get_object_reader(bucket, object, None, Default::default(), &options)
            .await
            .expect("read repaired multipart object");
        let mut actual = Vec::new();
        reader
            .stream
            .read_to_end(&mut actual)
            .await
            .expect("complete repaired object body");
        assert_eq!(actual, payloads.concat());
    }

    async fn real_disk() -> (TempDir, Endpoint, DiskStore) {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");
        (dir, endpoint, disk)
    }

    async fn set_disks_with(
        disks: Vec<Option<DiskStore>>,
        endpoints: Vec<Endpoint>,
        default_parity_count: usize,
    ) -> Arc<SetDisks> {
        let set_drive_count = disks.len();
        SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            set_drive_count,
            default_parity_count,
            0,
            0,
            endpoints,
            FormatV3::new(1, set_drive_count),
            vec![],
        )
        .await
    }

    fn meta_regen_test_fileinfo(object: &str, data_dir: Uuid, mod_time: i64, disk_index: usize) -> FileInfo {
        let mut fi = FileInfo::new(object, 2, 2);
        fi.data_dir = Some(data_dir);
        fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(mod_time).expect("test timestamp should parse"));
        fi.size = 1;
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            size: 1,
            actual_size: 1,
            ..Default::default()
        }];
        fi.erasure.index = fi.erasure.distribution[disk_index];
        fi
    }

    async fn meta_regen_test_set(
        bucket: &str,
        object: &str,
        data_dirs: &[(Uuid, usize)],
    ) -> (Vec<TempDir>, Arc<SetDisks>, Vec<Option<DiskStore>>) {
        let mut temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();
        for disk_index in 0..4 {
            let (temp_dir, endpoint, disk) = real_disk().await;
            disk.make_volume(bucket).await.expect("test bucket should be created");
            for (data_dir, shard_count) in data_dirs {
                if disk_index >= *shard_count {
                    continue;
                }
                let part_dir = temp_dir.path().join(bucket).join(object).join(data_dir.to_string());
                tokio::fs::create_dir_all(&part_dir)
                    .await
                    .expect("test data directory should be created");
                tokio::fs::write(part_dir.join("part.1"), [1u8; 2])
                    .await
                    .expect("test data shard should be written");
            }
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        let set = set_disks_with(disks.clone(), endpoints, 2).await;
        (temp_dirs, set, disks)
    }

    async fn seed_meta_regen_test_metadata(
        disks: &[Option<DiskStore>],
        disk_index: usize,
        bucket: &str,
        object: &str,
        file_info: &FileInfo,
    ) {
        disks[disk_index]
            .as_ref()
            .expect("metadata test disk should be online")
            .write_metadata("", bucket, object, file_info.clone())
            .await
            .expect("test metadata should be written");
    }

    async fn dangling_inline_test_fixture(
        bucket: &str,
        object: &str,
        mod_time: OffsetDateTime,
    ) -> (Vec<TempDir>, Arc<SetDisks>, Vec<Option<DiskStore>>) {
        let (temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[]).await;
        let mut metadata = meta_regen_test_fileinfo(object, Uuid::nil(), mod_time.unix_timestamp(), 0);
        metadata.data_dir = None;
        metadata.set_inline_data();
        metadata.data = Some(Bytes::from_static(b"stale-inline-shard"));
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &metadata).await;
        (temp_dirs, set, disks)
    }

    async fn formatted_single_disk_no_parity_set() -> (TempDir, Arc<SetDisks>) {
        let format = FormatV3::new(1, 1);
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);

        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        let mut disk_format = format.clone();
        disk_format.erasure.this = format.erasure.sets[0][0];
        save_format_file(&Some(disk.clone()), &Some(disk_format))
            .await
            .expect("format should be saved");

        let set = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(vec![Some(disk)])),
            1,
            0,
            0,
            0,
            vec![endpoint],
            format,
            vec![],
        )
        .await;
        set.set_test_storage_class_config(
            storageclass::lookup_config_for_pools_without_env(&rustfs_config::server_config::KVS::new(), &[1])
                .expect("test storage class should resolve for one local drive"),
        );
        (dir, set)
    }

    async fn non_trash_tmp_entries(temp_dirs: &[TempDir]) -> Vec<String> {
        let mut entries = Vec::new();
        for dir in temp_dirs {
            let tmp = dir.path().join(RUSTFS_META_TMP_BUCKET);
            let mut read_dir = match tokio::fs::read_dir(&tmp).await {
                Ok(read_dir) => read_dir,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => panic!("tmp directory should be readable: {err}"),
            };
            while let Some(entry) = read_dir.next_entry().await.expect("tmp entry should be readable") {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name != ".trash" {
                    entries.push(name);
                }
            }
        }
        entries
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn heal_rename_outcome_matrix_reports_partial_and_retries_failed_targets() {
        for (case, failed_attempts, expect_error) in [
            ("ok-ok", Vec::new(), false),
            ("ok-err", vec![1], true),
            ("err-ok", vec![0], true),
            ("err-err", vec![0, 1], true),
        ] {
            let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
            let bucket = format!("heal-rename-{case}");
            let object = "object.bin";
            for disk in &disks {
                disk.make_volume(&bucket).await.expect("bucket volume should be created");
            }

            let payload = vec![0x5a; 1024 * 1024];
            let mut reader = PutObjReader::from_vec(payload);
            // This fixture removes physical shards immediately after PUT. A
            // lock-owning PUT may quorum-ack before its rename tail drains, so
            // keep the isolated setup on the full-fanout commit path.
            set.put_object(
                &bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source object should be written");
            let source = disks[2]
                .read_version("", &bucket, object, "", &ReadOptions::default())
                .await
                .expect("source metadata should be readable");
            let data_dir = source.data_dir.expect("non-inline source should have a data directory");
            let tmp_entries_before_heal = non_trash_tmp_entries(&temp_dirs).await;
            let target_slots = {
                let mut slots = [source.erasure.distribution[0] - 1, source.erasure.distribution[1] - 1];
                slots.sort_unstable();
                slots
            };
            let failed_slots = failed_attempts
                .iter()
                .map(|attempt| target_slots[*attempt])
                .collect::<Vec<_>>();
            let failed_physical_indexes = [0, 1]
                .into_iter()
                .filter(|index| failed_slots.contains(&(source.erasure.distribution[*index] - 1)))
                .collect::<Vec<_>>();

            for index in [0, 1] {
                tokio::fs::remove_file(
                    temp_dirs[index]
                        .path()
                        .join(&bucket)
                        .join(object)
                        .join(data_dir.to_string())
                        .join("part.1"),
                )
                .await
                .expect("target shard should be removed before heal");
            }

            let failure_scope = HealRenameFailureScope::install(&bucket, object, &failed_slots);
            let (first_result, first_error) = set
                .heal_object(
                    &bucket,
                    object,
                    "",
                    &HealOpts {
                        no_lock: true,
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await
                .expect("heal should report its per-target rename outcome");
            drop(failure_scope);

            assert_eq!(first_error.is_some(), expect_error, "{case}: aggregate status must match target outcomes");
            if let Some(error) = first_error {
                let error = error.to_string();
                assert!(
                    error.contains(HEAL_RENAME_INCOMPLETE),
                    "{case}: partial/all failure must have an explicit retryable status: {error}"
                );
                assert!(
                    error.contains(&format!("{} of 2 targets committed", 2 - failed_slots.len())),
                    "{case}: aggregate status must distinguish partial from all-target failure: {error}"
                );
            }
            for index in [0, 1] {
                let expected = if failed_physical_indexes.contains(&index) {
                    DriveState::Missing
                } else {
                    DriveState::Ok
                };
                assert_eq!(
                    first_result.after.drives[index].state,
                    expected.to_string(),
                    "{case}: after.drives must reflect the actual rename outcome at index {index}"
                );
                let data_path = temp_dirs[index].path().join(&bucket).join(object).join(data_dir.to_string());
                assert!(
                    !data_path.join(crate::disk::STORAGE_FORMAT_FILE_BACKUP).exists(),
                    "{case}: heal must retire the backup even when it shares the live data directory"
                );
                assert_eq!(
                    temp_dirs[index]
                        .path()
                        .join(&bucket)
                        .join(object)
                        .join(data_dir.to_string())
                        .join("part.1")
                        .exists(),
                    !failed_physical_indexes.contains(&index),
                    "{case}: tmp cleanup must neither delete committed shards nor expose failed targets"
                );
            }
            let tmp_entries_after_heal = non_trash_tmp_entries(&temp_dirs).await;
            assert!(
                tmp_entries_after_heal
                    .iter()
                    .all(|entry| tmp_entries_before_heal.contains(entry)),
                "{case}: first heal must not leave a new temporary shard: {tmp_entries_after_heal:?}"
            );

            if !failed_slots.is_empty() {
                let (retry_result, retry_error) = set
                    .heal_object(
                        &bucket,
                        object,
                        "",
                        &HealOpts {
                            no_lock: true,
                            scan_mode: HealScanMode::Deep,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("second heal should retry failed targets");
                assert!(retry_error.is_none(), "{case}: second heal should complete remaining targets");
                for index in [0, 1] {
                    assert_eq!(
                        retry_result.after.drives[index].state,
                        DriveState::Ok.to_string(),
                        "{case}: second heal must converge target {index}"
                    );
                }
                let tmp_entries_after_retry = non_trash_tmp_entries(&temp_dirs).await;
                assert!(
                    tmp_entries_after_retry
                        .iter()
                        .all(|entry| tmp_entries_before_heal.contains(entry)),
                    "{case}: retry must not leave a new temporary shard: {tmp_entries_after_retry:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn replacement_target_readback_requires_the_committed_shard() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "replacement-target-readback";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(vec![0x5a; 1024 * 1024]);
        // The target-evidence readback below asserts per-disk state right
        // after PUT. A lock-owning PUT may quorum-ack before its rename tail
        // drains, so keep the setup on the full-fanout commit path.
        set.put_object(
            bucket,
            object,
            &mut reader,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("source object should be written");
        let source = disks[2]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("source metadata should be readable");
        let data_dir = source.data_dir.expect("non-inline source should have a data directory");
        let targets = vec![set.set_endpoints[0].to_string(), set.set_endpoints[1].to_string()];

        for disk in disks.iter().take(targets.len()) {
            let mut metadata = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("target metadata should be readable");
            metadata.version_id = None;
            disk.write_metadata("", bucket, object, metadata)
                .await
                .expect("target metadata should be rewritten as a legacy null version");
        }
        let null_version = Uuid::nil().to_string();

        assert!(
            set.replacement_targets_have_version(bucket, object, &null_version, &targets)
                .await
                .expect("nil selector should confirm healthy legacy null-version target shards")
        );

        tokio::fs::remove_file(
            temp_dirs[1]
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join("part.1"),
        )
        .await
        .expect("target shard should be removed after the initial commit");

        assert!(
            !set.replacement_targets_have_version(bucket, object, &null_version, &targets)
                .await
                .expect("missing target shard should be observable")
        );
    }

    #[tokio::test]
    async fn deep_heal_rebuilds_missing_part_when_metadata_remains_current() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "deep-heal-missing-part-current-meta";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let payload = vec![0x7b; 1024 * 1024];
        let mut reader = PutObjReader::from_vec(payload);
        set.put_object(
            bucket,
            object,
            &mut reader,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("source object should be written before shard loss");
        let source = disks[2]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("source metadata should be readable");
        let data_dir = source.data_dir.expect("non-inline source should have a data directory");
        let missing_part = temp_dirs[1]
            .path()
            .join(bucket)
            .join(object)
            .join(data_dir.to_string())
            .join("part.1");
        tokio::fs::remove_file(&missing_part)
            .await
            .expect("target shard should be removed while xl.meta remains current");

        let (result, error) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("deep heal should finish after a single shard is removed");

        assert!(error.is_none(), "deep heal should recover the missing shard: {error:?}");
        assert_eq!(result.after.drives[1].state, DriveState::Ok.to_string());
        assert!(
            missing_part.exists(),
            "deep heal must reconstruct the missing shard on the original disk slot"
        );
    }

    #[tokio::test]
    async fn deep_heal_recreates_missing_bucket_volume_before_rebuilding_shard() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "deep-heal-missing-bucket-volume";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(vec![0x5c; 1024 * 1024]);
        set.put_object(
            bucket,
            object,
            &mut reader,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("source object should be written before volume loss");
        let source = disks[2]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("source metadata should be readable");
        let data_dir = source.data_dir.expect("non-inline source should have a data directory");

        disks[1]
            .delete_volume(bucket, true)
            .await
            .expect("target bucket volume should be removable for the regression setup");
        assert!(matches!(disks[1].stat_volume(bucket).await, Err(DiskError::VolumeNotFound)));

        let (result, error) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("deep heal should finish after bucket volume loss");

        assert!(error.is_none(), "deep heal should recreate the missing bucket volume: {error:?}");
        assert!(disks[1].stat_volume(bucket).await.is_ok(), "target bucket volume should be restored");
        let healed = disks[1]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("healed target metadata should be readable");
        assert_eq!(healed.data_dir, Some(data_dir));
        assert_eq!(result.after.drives[1].state, DriveState::Ok.to_string());
        assert!(
            temp_dirs[1]
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join("part.1")
                .exists(),
            "deep heal should rebuild the object shard after recreating its volume"
        );

        let (_, retry_error) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("repeat heal should complete");
        assert!(retry_error.is_none(), "repeat heal should be idempotent: {retry_error:?}");
    }

    #[tokio::test]
    async fn deep_heal_rebuilds_a_stale_current_delete_marker_metadata_replica() {
        let (_temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "deep-heal-stale-current-marker";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("test bucket should be created");
        }

        let versioned = ObjectOptions {
            no_lock: true,
            versioned: true,
            ..Default::default()
        };
        let original = set
            .put_object(bucket, object, &mut PutObjReader::from_vec(vec![0x41; 1024]), &versioned)
            .await
            .expect("versioned source object should be written");
        let stale_metadata = disks[0]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("pre-marker metadata should be readable");

        let marker = set
            .delete_object(bucket, object, versioned)
            .await
            .expect("versioned delete should create a current marker");
        let marker_version = marker.version_id.expect("delete marker should have a version id");

        // Simulate a member that was offline while the acknowledged marker was
        // committed on the other members: its physical root still contains the
        // pre-delete metadata and the original data version.
        let mut stale_metadata = stale_metadata;
        stale_metadata.fresh = true;
        disks[0]
            .write_metadata("", bucket, object, stale_metadata)
            .await
            .expect("stale member metadata should be restored for the fixture");
        assert_eq!(
            disks[0]
                .read_version("", bucket, object, &marker_version.to_string(), &ReadOptions::default())
                .await
                .expect_err("stale member must not contain the current marker")
                .to_string(),
            "file version not found"
        );

        let (result, error) = set
            .heal_object(
                bucket,
                object,
                &marker_version.to_string(),
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("deep heal should report the stale marker result");

        assert!(error.is_none(), "deep heal should repair stale marker metadata: {error:?}");
        assert_eq!(result.drives_healed(), Some(1));
        let repaired = disks[0]
            .read_version("", bucket, object, &marker_version.to_string(), &ReadOptions::default())
            .await
            .expect("rejoined member should contain the marker");
        assert!(repaired.deleted, "repaired metadata must remain a delete marker");
        assert_eq!(repaired.version_id, Some(marker_version));
        assert!(original.version_id.is_some(), "source object should have a version id");
    }

    #[tokio::test]
    async fn deep_heal_rebuilds_near_tail_truncated_part() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "deep-heal-near-tail-truncation";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let expected_payload = vec![0x6d; 1024 * 1024];
        set.put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(expected_payload.clone()),
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("source object should be written before shard truncation");
        let source = disks[2]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("source metadata should be readable");
        let data_dir = source.data_dir.expect("non-inline source should have a data directory");
        let truncated_part = temp_dirs[1]
            .path()
            .join(bucket)
            .join(object)
            .join(data_dir.to_string())
            .join("part.1");
        let original_len = tokio::fs::metadata(&truncated_part)
            .await
            .expect("target shard should exist")
            .len();
        assert!(original_len > 1, "test shard must be large enough to truncate");
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&truncated_part)
            .await
            .expect("target shard should be writable");
        file.set_len(original_len - 1)
            .await
            .expect("target shard should be truncated");

        let mut reader = set
            .get_object_reader(bucket, object, None, Default::default(), &ObjectOptions::default())
            .await
            .expect("GET should remain readable after a one-byte shard truncation");
        let mut read_back = Vec::new();
        tokio::io::copy(&mut reader, &mut read_back)
            .await
            .expect("GET should reconstruct the truncated shard through EC");
        assert_eq!(read_back, expected_payload, "EC GET must preserve the object bytes");

        let (result, error) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("deep heal should finish after a one-byte tail truncation");

        assert!(error.is_none(), "deep heal should recover the truncated shard: {error:?}");
        assert_eq!(result.drives_healed(), Some(1));
        assert_eq!(result.before.drives[1].state, DriveState::Corrupt.to_string());
        assert_eq!(
            tokio::fs::metadata(&truncated_part)
                .await
                .expect("repaired shard should exist")
                .len(),
            original_len,
            "deep heal must restore the complete shard length"
        );
    }

    #[derive(Debug)]
    struct FailedIntegrityReadTransport {
        error: DiskError,
        reads: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::cluster::rpc::internode_data_transport::InternodeDataTransport for FailedIntegrityReadTransport {
        async fn open_read(
            &self,
            _request: crate::cluster::rpc::internode_data_transport::ReadStreamRequest,
        ) -> crate::disk::error::Result<crate::disk::FileReader> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let error = match &self.error {
                DiskError::FileCorrupt => DiskError::from(rustfs_rio::new_test_remote_file_corrupt_http_io_error()),
                DiskError::FileNotFound => DiskError::from(rustfs_rio::new_test_remote_file_not_found_http_io_error()),
                DiskError::VolumeNotFound => DiskError::from(rustfs_rio::new_test_remote_volume_not_found_http_io_error()),
                error => error.clone(),
            };
            Err(error)
        }

        async fn open_write(
            &self,
            _request: crate::cluster::rpc::internode_data_transport::WriteStreamRequest,
        ) -> crate::disk::error::Result<crate::disk::FileWriter> {
            panic!("an integrity scan must not write remote data");
        }

        async fn open_walk_dir(
            &self,
            _request: crate::cluster::rpc::internode_data_transport::WalkDirStreamRequest,
        ) -> crate::disk::error::Result<crate::disk::FileReader> {
            panic!("an object integrity scan must not walk remote directories");
        }

        fn name(&self) -> &'static str {
            "failed-integrity-read"
        }

        fn capabilities(&self) -> crate::cluster::rpc::internode_data_transport::InternodeDataTransportCapabilities {
            crate::cluster::rpc::internode_data_transport::InternodeDataTransportCapabilities::tcp_http()
        }
    }

    async fn assert_remote_integrity_read_failure(expected_error: DiskError) {
        use crate::disk::{CHECK_PART_FILE_CORRUPT, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN};
        use crate::object_api::ShardIntegrityWriteMode;

        let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
        let bucket = "remote-integrity-read-failure";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("create fixture bucket");
        }
        set.put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(vec![0x6d; 1024 * 1024 + 123]),
            &ObjectOptions {
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("write protected external fixture");

        let mut files = Vec::new();
        let mut snapshots = Vec::new();
        for (dir, disk) in dirs.iter().zip(&disks) {
            let file = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("read fixture metadata");
            assert!(file.data.is_none(), "the remote test must open an external shard");
            let object_dir = dir.path().join(bucket).join(object);
            let part_path = object_dir
                .join(file.data_dir.expect("external shard directory").to_string())
                .join("part.1");
            for path in [part_path, object_dir.join("xl.meta")] {
                let bytes = tokio::fs::read(&path).await.expect("snapshot fixture before scan");
                snapshots.push((path, bytes));
            }
            files.push(file);
        }
        let transport = Arc::new(FailedIntegrityReadTransport {
            error: expected_error.clone(),
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        let endpoint = Endpoint {
            url: url::Url::parse("http://remote-integrity.invalid:9000/data/disk0").expect("fixture endpoint"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let remote = crate::cluster::rpc::RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            transport.clone(),
        )
        .await
        .expect("construct remote disk with failing data transport");
        let mut verification_disks: Vec<_> = disks.iter().cloned().map(Some).collect();
        verification_disks[0] = Some(Arc::new(crate::disk::Disk::Remote(Box::new(remote))));
        let mut statuses = [(0, vec![CHECK_PART_UNKNOWN; disks.len()])].into_iter().collect();
        let result = crate::io_support::shard_integrity::verify_deep_parts(
            &files,
            &verification_disks,
            &files[0],
            bucket,
            object,
            &mut statuses,
        )
        .await;
        assert!(transport.reads.load(std::sync::atomic::Ordering::Relaxed) > 0);
        if matches!(expected_error, DiskError::FileCorrupt | DiskError::FileNotFound) {
            result.expect("typed remote payload damage must identify a repairable shard");
            let expected_status = if expected_error == DiskError::FileCorrupt {
                CHECK_PART_FILE_CORRUPT
            } else {
                CHECK_PART_FILE_NOT_FOUND
            };
            assert_eq!(statuses[&0][0], expected_status);
            assert!(statuses[&0][1..].iter().all(|status| *status == CHECK_PART_SUCCESS));
            let (heal, metadata, reason) = super::should_heal_object_on_disk(&None, &[statuses[&0][0]], &files[0], &files[0]);
            assert!(heal, "remote payload damage must schedule repair: {expected_error:?}");
            assert!(!metadata);
            let expected_reason = if expected_error == DiskError::FileCorrupt {
                DiskError::FileCorrupt
            } else {
                DiskError::PartMissingOrCorrupt
            };
            assert_eq!(reason, Some(expected_reason));
        } else {
            let error = result.expect_err("unavailable payload storage must abort health verification");
            if expected_error.is_internode_http_status(500) {
                assert!(error.is_internode_http_status(500), "preserve the original transport error: {error:?}");
            } else {
                assert_eq!(error, expected_error, "preserve the original storage failure");
            }
            assert_eq!(statuses[&0][0], CHECK_PART_UNKNOWN, "failed verification cannot certify the shard");
        }
        for (path, before) in snapshots {
            assert_eq!(tokio::fs::read(path).await.expect("read fixture after scan"), before);
        }
    }

    #[tokio::test]
    async fn deep_scan_remote_payload_damage_schedules_repair() {
        for error in [DiskError::FileCorrupt, DiskError::FileNotFound] {
            assert_remote_integrity_read_failure(error).await;
        }
    }

    #[tokio::test]
    async fn deep_scan_remote_storage_failure_cannot_certify_healthy() {
        for error in [
            DiskError::from(rustfs_rio::new_test_internode_http_io_error(
                rustfs_rio::InternodeHttpErrorKind::HttpStatus(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
            )),
            DiskError::DiskNotFound,
            DiskError::VolumeNotFound,
            DiskError::FileAccessDenied,
        ] {
            assert_remote_integrity_read_failure(error).await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn deep_heal_legacy_verification_error_cannot_certify_healthy() {
        use crate::object_api::ShardIntegrityWriteMode;

        let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "legacy-deep-scan-verification-error";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("create legacy fixture bucket");
        }
        set.put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(vec![0x6d; 1024 * 1024 + 123]),
            &ObjectOptions {
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("write external legacy fixture");

        let mut files = Vec::new();
        let mut part_paths = Vec::new();
        let mut preserved = Vec::new();
        for (index, (dir, disk)) in dirs.iter().zip(&disks).enumerate() {
            let file = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("read legacy fixture metadata");
            assert!(file.data.is_none(), "legacy verification must open the physical shard");
            assert!(file.parts.iter().all(|part| part.integrity.is_none()));
            let object_dir = dir.path().join(bucket).join(object);
            let part = object_dir
                .join(file.data_dir.expect("external legacy shard directory").to_string())
                .join("part.1");
            let metadata = object_dir.join("xl.meta");
            let bytes = tokio::fs::read(&metadata).await.expect("snapshot legacy metadata");
            preserved.push((metadata, bytes));
            if index != 0 {
                let bytes = tokio::fs::read(&part).await.expect("snapshot healthy legacy shard");
                preserved.push((part.clone(), bytes));
            }
            part_paths.push(part);
            files.push(file);
        }
        let broken_part = &part_paths[0];
        tokio::fs::remove_file(broken_part)
            .await
            .expect("remove fixture shard before symlink fault");
        std::os::unix::fs::symlink("part.1", broken_part).expect("create self-referential shard symlink");
        let open_error = tokio::fs::File::open(broken_part)
            .await
            .expect_err("self-referential symlink must fail to open");
        assert_eq!(open_error.raw_os_error(), Some(libc::ELOOP));

        let verification_error = disks[0]
            .verify_file(bucket, object, &files[0])
            .await
            .expect_err("legacy disk verifier must reject the symlink path");
        assert_eq!(verification_error, DiskError::InvalidPath);
        let error = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect_err("an unverified legacy part must fail the complete heal operation");
        assert_eq!(error, DiskError::InvalidPath, "complete heal must preserve the verifier's path error");
        assert_eq!(
            tokio::fs::read_link(broken_part)
                .await
                .expect("failed heal must preserve the symlink fault"),
            std::path::Path::new("part.1")
        );
        for (path, before) in preserved {
            assert_eq!(tokio::fs::read(path).await.expect("read unchanged legacy fixture"), before);
        }
    }

    async fn assert_h2_truncated_shards(
        mode: crate::object_api::ShardIntegrityWriteMode,
        coding_indexes: &[usize],
        retained_len: fn(usize, usize) -> usize,
    ) {
        use crate::object_api::ShardIntegrityWriteMode;

        let (temp_dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(16, 0, 4).await;
        let bucket = "deep-heal-h2-truncation";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("create bucket volume");
        }
        let expected_payload: Vec<_> = (0..5 * 1024 * 1024 + 123)
            .map(|index| u8::try_from((index / 4096 + index) % 251).expect("payload byte fits"))
            .collect();
        set.put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(expected_payload.clone()),
            &ObjectOptions {
                shard_integrity_write_mode: Some(mode),
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("commit source object");

        // PUT can succeed at quorum while minority shards remain unavailable.
        // Require every source disk before modifying any shard in this fixture.
        let mut source_metadata = Vec::with_capacity(disks.len());
        for disk in &disks {
            source_metadata.push(disk.read_version("", bucket, object, "", &ReadOptions::default()).await);
        }
        assert!(
            source_metadata.iter().all(|source| source.is_ok()),
            "source fixture must populate all {} disks before truncation in {mode:?} mode; per-disk errors: {:?}",
            disks.len(),
            source_metadata
                .iter()
                .enumerate()
                .map(|(slot, source)| (slot, source.as_ref().err()))
                .collect::<Vec<_>>()
        );
        let files: Vec<_> = source_metadata
            .into_iter()
            .map(|source| source.expect("source metadata checked above"))
            .collect();
        let mut paths = Vec::new();
        let mut original_shards = Vec::new();
        let mut original_metadata = Vec::new();
        let mut damaged = Vec::new();
        for (slot, source) in files.iter().enumerate() {
            assert_eq!((source.erasure.data_blocks, source.erasure.parity_blocks), (12, 4));
            assert_eq!(source.parts[0].integrity.is_some(), mode == ShardIntegrityWriteMode::Protected);
            let directory = temp_dirs[slot].path().join(bucket).join(object);
            let path = directory
                .join(source.data_dir.expect("external shard data directory").to_string())
                .join("part.1");
            let original = tokio::fs::read(&path).await.expect("read original shard");
            original_metadata.push(tokio::fs::read(directory.join("xl.meta")).await.expect("read xl.meta"));
            if coding_indexes.contains(&source.erasure.index) {
                let erasure = crate::erasure::coding::Erasure::try_new_with_options(
                    12,
                    4,
                    source.erasure.block_size,
                    source.uses_legacy_checksum,
                )
                .expect("valid erasure layout");
                let stripe_size = erasure.shard_size() + source.erasure.get_checksum_info(1).algorithm.size();
                let keep = retained_len(original.len(), stripe_size);
                assert!(keep < original.len(), "fixture must truncate the shard");
                tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .await
                    .expect("open target shard")
                    .set_len(u64::try_from(keep).expect("shard length fits"))
                    .await
                    .expect("truncate target shard");
                damaged.push((slot, keep));
            }
            paths.push(path);
            original_shards.push(original);
        }
        assert_eq!(damaged.len(), coding_indexes.len());
        if mode == ShardIntegrityWriteMode::Protected {
            let mut statuses = [(0, vec![crate::disk::CHECK_PART_UNKNOWN; disks.len()])]
                .into_iter()
                .collect();
            let online_disks: Vec<_> = disks.iter().cloned().map(Some).collect();
            crate::io_support::shard_integrity::verify_deep_parts(
                &files,
                &online_disks,
                &files[0],
                bucket,
                object,
                &mut statuses,
            )
            .await
            .expect("verify protected shards directly");
            for (slot, status) in statuses[&0].iter().enumerate() {
                let expected = if damaged.iter().any(|(index, _)| *index == slot) {
                    crate::disk::CHECK_PART_FILE_CORRUPT
                } else {
                    crate::disk::CHECK_PART_SUCCESS
                };
                assert_eq!(*status, expected, "protected shard {slot}");
            }
        }

        let opts = HealOpts {
            no_lock: true,
            scan_mode: HealScanMode::Deep,
            ..Default::default()
        };
        let (dry_run, error) = set
            .heal_object(bucket, object, "", &HealOpts { dry_run: true, ..opts })
            .await
            .expect("dry-run should report corruption");
        assert!(error.is_none(), "dry-run error: {error:?}");
        assert_eq!(dry_run.drives_healed(), Some(0));
        for (slot, keep) in &damaged {
            assert_eq!(dry_run.after.drives[*slot].state, DriveState::Corrupt.to_string());
            assert_eq!(
                tokio::fs::read(&paths[*slot]).await.expect("dry-run preserved shard"),
                original_shards[*slot][..*keep]
            );
        }

        let recoverable = damaged.len() <= 4;
        if recoverable {
            let mut reader = set
                .get_object_reader(bucket, object, None, Default::default(), &ObjectOptions::default())
                .await
                .expect("degraded GET should remain readable");
            let mut read_back = Vec::new();
            tokio::io::copy(&mut reader, &mut read_back)
                .await
                .expect("read complete degraded GET");
            assert_eq!(read_back, expected_payload);
        }
        let (result, error) = set
            .heal_object(bucket, object, "", &opts)
            .await
            .expect("heal should report outcome");
        if recoverable {
            assert!(error.is_none(), "heal error: {error:?}");
            assert_eq!(result.drives_healed(), Some(damaged.len()));
            for (slot, _) in &damaged {
                assert_eq!(result.before.drives[*slot].state, DriveState::Corrupt.to_string());
            }
        } else {
            assert_eq!(error, Some(DiskError::ErasureReadQuorum));
            assert_eq!(result.drives_healed(), Some(0));
        }
        for (slot, path) in paths.iter().enumerate() {
            let expected = if !recoverable && let Some((_, keep)) = damaged.iter().find(|(index, _)| *index == slot) {
                &original_shards[slot][..*keep]
            } else {
                original_shards[slot].as_slice()
            };
            assert_eq!(
                tokio::fs::read(path).await.expect("read shard after heal"),
                expected,
                "physical shard {slot}"
            );
            if !recoverable {
                let metadata_path = temp_dirs[slot].path().join(bucket).join(object).join("xl.meta");
                assert_eq!(tokio::fs::read(metadata_path).await.expect("preserved metadata"), original_metadata[slot]);
            }
        }
        if recoverable {
            let mut reader = set
                .get_object_reader(bucket, object, None, Default::default(), &ObjectOptions::default())
                .await
                .expect("GET after repair");
            let mut read_back = Vec::new();
            tokio::io::copy(&mut reader, &mut read_back)
                .await
                .expect("read repaired object");
            assert_eq!(read_back, expected_payload);
            let (healthy, error) = set.heal_object(bucket, object, "", &opts).await.expect("second deep scan");
            assert!(error.is_none());
            assert_eq!(healthy.drives_healed(), Some(0), "repair must converge");
            assert_eq!(healthy.integrity_verified, mode == ShardIntegrityWriteMode::Protected);
        }
    }

    #[tokio::test]
    async fn deep_heal_rebuilds_h2_near_tail_truncated_part() {
        use crate::object_api::ShardIntegrityWriteMode::{Legacy, Protected};
        for mode in [Legacy, Protected] {
            for coding_index in [1, 13] {
                assert_h2_truncated_shards(mode, &[coding_index], |length, _| length - 1).await;
            }
        }
    }

    #[tokio::test]
    async fn deep_heal_rebuilds_h2_half_truncated_part() {
        use crate::object_api::ShardIntegrityWriteMode::{Legacy, Protected};
        for mode in [Legacy, Protected] {
            for coding_index in [1, 13] {
                assert_h2_truncated_shards(mode, &[coding_index], |length, _| length / 2).await;
            }
        }
    }

    #[tokio::test]
    async fn deep_heal_rebuilds_h2_stripe_boundary_truncated_parts_at_read_quorum() {
        use crate::object_api::ShardIntegrityWriteMode::{Legacy, Protected};
        for mode in [Legacy, Protected] {
            assert_h2_truncated_shards(mode, &[1, 2, 13, 16], |_, stripe| stripe).await;
        }
    }

    #[tokio::test]
    async fn deep_heal_rejects_h2_truncated_parts_below_read_quorum() {
        use crate::object_api::ShardIntegrityWriteMode::{Legacy, Protected};
        for mode in [Legacy, Protected] {
            assert_h2_truncated_shards(mode, &[1, 2, 3, 13, 16], |length, _| length / 2).await;
        }
    }

    #[test]
    fn heal_rollback_backup_selection_preserves_legacy_peer_semantics() {
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        for (old, rollback, cleanup, expected) in [
            (None, None, None, None),
            (Some(a), None, Some(a), None),
            (Some(a), Some(a), Some(a), Some(a)),
            (Some(a), Some(b), Some(a), Some(b)),
            (Some(a), None, None, Some(a)),
        ] {
            let response = crate::disk::RenameDataResp {
                old_data_dir: old,
                rollback_data_dir: rollback,
                cleanup_data_dir: cleanup,
                ..Default::default()
            };
            assert_eq!(heal_rollback_backup_dir(&response), expected);
        }
        let legacy: crate::disk::RenameDataResp =
            serde_json::from_str(r#"{"old_data_dir":"00000000-0000-0000-0000-000000000001","sign":null}"#)
                .expect("older peer response without rollback or cleanup fields");
        assert_eq!(heal_rollback_backup_dir(&legacy), Some(a));
    }

    #[tokio::test]
    async fn non_inline_heal_preserves_backup_after_publication_fence_loss() {
        let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "heal-non-inline-fence";
        let object = "object";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume");
        }
        let options = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        set.put_object(bucket, object, &mut PutObjReader::from_vec(vec![0x11; 1024 * 1024]), &options)
            .await
            .expect("seed external generation");
        let old = disks[0]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("old target");
        assert!(old.data.is_none());
        let old_dir = old.data_dir.expect("external old data directory");
        let metadata_path = dirs[0].path().join(bucket).join(object).join(STORAGE_FORMAT_FILE);
        let old_metadata = tokio::fs::read(&metadata_path).await.expect("save stale metadata");
        let payload = vec![0x22; 1024 * 1024];
        set.put_object(bucket, object, &mut PutObjReader::from_vec(payload.clone()), &options)
            .await
            .expect("write external current generation");
        let current = disks[0]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("current target");
        assert!(current.data.is_none());
        let part = format!("{object}/{}/part.1", current.data_dir.expect("current external directory"));
        let expected_shard = disks[0].read_all(bucket, &part).await.expect("current encoded shard");
        tokio::fs::write(&metadata_path, old_metadata)
            .await
            .expect("restore stale metadata");
        let backup = metadata_path
            .parent()
            .expect("object directory")
            .join(old_dir.to_string())
            .join(crate::disk::STORAGE_FORMAT_FILE_BACKUP);
        let can_commit = || !backup.exists();
        let opts = HealOpts {
            no_lock: true,
            scan_mode: HealScanMode::Deep,
            ..Default::default()
        };
        let (_, error) = set
            .heal_object_with_explicit_version_regen(
                bucket,
                object,
                "",
                ExplicitVersionHeal {
                    opts: &opts,
                    allow_regeneration: true,
                    preserve_unrepairable: false,
                    commit_fence: Some(&can_commit),
                    namespace_owner: None,
                    retirement: None,
                },
                &mut None,
            )
            .await
            .expect("report committed external repair");
        assert!(
            error.is_none(),
            "post-publication fence loss must preserve the committed target: {error:?}"
        );
        assert!(backup.exists(), "lost fence must retain the actual rollback witness");
        assert_eq!(disks[0].read_all(bucket, &part).await.expect("repaired physical shard"), expected_shard);
        let mut reader = set
            .get_object_reader(bucket, object, None, Default::default(), &options)
            .await
            .expect("committed object remains readable");
        let mut body = Vec::new();
        tokio::io::copy(&mut reader, &mut body).await.expect("complete repaired GET");
        assert_eq!(body, payload);
    }

    #[cfg(not(windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_heal_backup_cleanup_retains_key_owner_until_unlink_finishes() {
        use crate::disk::os::prepared_publication_test_hooks::{Stage, install_at};
        use std::time::Duration;

        for mode in ["outer", "inner", "read-repair"] {
            let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
            let bucket = format!("heal-cleanup-cancel-{mode}");
            let object = "object";
            for disk in &disks {
                disk.make_volume(&bucket).await.expect("create bucket volume");
            }
            let options = ObjectOptions {
                no_lock: true,
                ..Default::default()
            };
            set.put_object(&bucket, object, &mut PutObjReader::from_vec(vec![0x11; 1024 * 1024]), &options)
                .await
                .expect("write old generation on every disk");
            let metadata_path = dirs[0].path().join(&bucket).join(object).join(STORAGE_FORMAT_FILE);
            let old_metadata = tokio::fs::read(&metadata_path).await.expect("save stale replica");
            let old = disks[0]
                .read_version("", &bucket, object, "", &ReadOptions::default())
                .await
                .expect("read old non-inline target");
            assert!(old.data.is_none(), "cancellation fixture must use non-inline publication");
            let rollback_dir = old.data_dir.expect("old external data directory");
            let payload = vec![0x22; 1024 * 1024];
            set.put_object(&bucket, object, &mut PutObjReader::from_vec(payload.clone()), &options)
                .await
                .expect("write current generation on every disk");
            tokio::fs::write(&metadata_path, old_metadata)
                .await
                .expect("restore stale target");
            let backup_path = disks[0]
                .get_object_path_for_io_if_local(
                    &bucket,
                    &format!("{object}/{rollback_dir}/{}", crate::disk::STORAGE_FORMAT_FILE_BACKUP),
                )
                .expect("fixture disk is local")
                .expect("physical backup path");
            let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let _hook = install_at(Stage::Remove, &backup_path, move || {
                let _ = arrived_tx.send(());
                let _ = release_rx.recv_timeout(Duration::from_secs(30));
            });
            let task_set = set.clone();
            let task_bucket = bucket.clone();
            let heal = tokio::spawn(async move {
                let opts = HealOpts {
                    read_repair: mode == "read-repair",
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                };
                if mode == "outer" {
                    let (_, error) = task_set
                        .heal_object_with_retirement(
                            &task_bucket,
                            object,
                            "",
                            &opts,
                            &mut None,
                            AbsenceProofRequest {
                                retirement: None,
                                allow_unversioned: false,
                            },
                        )
                        .await
                        .expect("production wrapper heal should reach cleanup");
                    assert!(error.is_none(), "production wrapper heal error: {error:?}");
                } else {
                    let (_, error) = task_set
                        .heal_object(&task_bucket, object, "", &opts)
                        .await
                        .expect("inner/read-repair heal should reach cleanup");
                    assert!(error.is_none(), "inner/read-repair heal error: {error:?}");
                }
            });
            let arrived = tokio::time::timeout(Duration::from_secs(10), arrived_rx).await;
            if !matches!(arrived, Ok(Ok(()))) {
                heal.abort();
                let outcome = heal.await;
                let _ = release_tx.send(());
                panic!("{mode}: cleanup must reach the owned physical unlink hook; task: {outcome:?}");
            }
            heal.abort();
            assert!(heal.await.expect_err("heal must be cancelled at the unlink").is_cancelled());
            assert!(
                set.ctx.namespace_commits_pending(),
                "{mode}: physical cleanup must retain namespace ownership"
            );
            let namespace = set.new_ns_lock(&bucket, object).await.expect("successor namespace");
            let mut successor = Box::pin(namespace.get_write_lock(Duration::from_secs(30)));
            assert!(
                tokio::time::timeout(Duration::from_secs(1), &mut successor).await.is_err(),
                "{mode}: a successor must not enter while the old unlink is pending"
            );
            release_tx.send(()).expect("release physical cleanup");
            let successor = tokio::time::timeout(Duration::from_secs(10), successor)
                .await
                .expect("physical completion should release the key lock")
                .expect("successor should acquire the released lock");
            assert!(!backup_path.exists(), "{mode}: owned unlink must finish before releasing the key");
            drop(successor);
            let mut reader = set
                .get_object_reader(&bucket, object, None, Default::default(), &options)
                .await
                .expect("committed object remains readable after cancellation");
            let mut body = Vec::new();
            tokio::io::copy(&mut reader, &mut body)
                .await
                .expect("read full committed body");
            assert_eq!(body, payload, "{mode}: cancellation must preserve committed data");
        }
    }

    #[tokio::test]
    async fn heal_reclaims_published_inline_backup_and_preserves_child_key() {
        let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "heal-inline-rollback";
        let object = "nested/object";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("create bucket volume");
        }
        let options = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let rollback_dir = crate::disk::local::inline_metadata_rollback_dir(Uuid::nil(), &rustfs_filemeta::FileMeta::new());
        let child = format!("{object}/{rollback_dir}");
        let child_payload = b"a child key must survive parent repair".to_vec();
        set.put_object(bucket, &child, &mut PutObjReader::from_vec(child_payload.clone()), &options)
            .await
            .expect("write child key in the synthetic rollback directory");
        set.put_object(bucket, object, &mut PutObjReader::from_vec(vec![0x11; 592]), &options)
            .await
            .expect("write old inline generation on every disk");
        let metadata_path = dirs[0].path().join(bucket).join(object).join(STORAGE_FORMAT_FILE);
        let old_metadata = tokio::fs::read(&metadata_path).await.expect("save stale target metadata");
        let payload = vec![0x22; 592];
        set.put_object(bucket, object, &mut PutObjReader::from_vec(payload.clone()), &options)
            .await
            .expect("write current inline generation on every disk");
        let read_options = ReadOptions {
            read_data: true,
            ..Default::default()
        };
        let expected = disks[0]
            .read_version("", bucket, object, "", &read_options)
            .await
            .expect("read current target generation");
        assert!(expected.data.is_some(), "fixture must exercise inline publication");
        tokio::fs::write(&metadata_path, &old_metadata)
            .await
            .expect("restore one stale replica without a pre-existing delete transaction");
        let backup_path = metadata_path
            .parent()
            .expect("object directory")
            .join(rollback_dir.to_string())
            .join(crate::disk::STORAGE_FORMAT_FILE_BACKUP);
        let opts = HealOpts {
            no_lock: true,
            scan_mode: HealScanMode::Deep,
            ..Default::default()
        };
        let (_, error) = set
            .heal_object(bucket, object, "", &HealOpts { dry_run: true, ..opts })
            .await
            .expect("dry-run stale inline repair");
        assert!(error.is_none(), "dry-run error: {error:?}");
        assert_eq!(tokio::fs::read(&metadata_path).await.expect("dry-run target"), old_metadata);
        assert!(!backup_path.exists(), "dry-run must not publish a backup");

        let failure_scope = HealRenameFailureScope::install(bucket, object, &[expected.erasure.distribution[0] - 1]);
        let (_, error) = set
            .heal_object(bucket, object, "", &opts)
            .await
            .expect("report failed publication");
        drop(failure_scope);
        assert!(error.is_some(), "failed rename must remain retryable");
        assert_eq!(tokio::fs::read(&metadata_path).await.expect("failed target"), old_metadata);
        assert!(!backup_path.exists(), "failed publication must not reclaim or manufacture a backup");

        let (result, error) = set
            .heal_object(bucket, object, "", &opts)
            .await
            .expect("repair stale inline replica");
        assert!(error.is_none(), "repair error: {error:?}");
        assert_eq!(result.drives_healed(), Some(1));
        let repaired = disks[0]
            .read_version("", bucket, object, "", &read_options)
            .await
            .expect("read repaired physical target");
        assert_eq!(repaired.mod_time, expected.mod_time);
        assert_eq!(repaired.data_dir, expected.data_dir);
        assert_eq!(repaired.data, expected.data);
        assert!(!backup_path.exists(), "successful heal must retire its old-generation backup");
        let evidence = disks[0]
            .discover_delete_transactions(bucket, Some(object), None, 128)
            .await
            .expect("discover delete evidence without treating a child key as a transaction");
        assert!(evidence.transactions.is_empty(), "successful repair must leave no delete blocker");
        for (key, expected_body) in [(object, payload), (child.as_str(), child_payload)] {
            let mut reader = set
                .get_object_reader(bucket, key, None, Default::default(), &options)
                .await
                .expect("read repaired parent and preserved child");
            let mut body = Vec::new();
            tokio::io::copy(&mut reader, &mut body)
                .await
                .expect("read complete object body");
            assert_eq!(body, expected_body, "object {key}");
        }
        let (healthy, error) = set.heal_object(bucket, object, "", &opts).await.expect("repeat repair");
        assert!(error.is_none(), "repeat repair error: {error:?}");
        assert_eq!(healthy.drives_healed(), Some(0));
    }

    #[tokio::test]
    async fn deep_heal_inline_bitrot_rebuilds_physical_data_and_parity_shards() {
        use crate::storage_api_contracts::range::HTTPRangeSpec;
        use tokio::io::AsyncReadExt;

        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "deep-heal-inline-bitrot";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("test bucket should be created");
        }
        let payload: Vec<u8> = (0..4113)
            .map(|index| u8::try_from(index % 251).expect("payload byte"))
            .collect();
        let read_options = ReadOptions {
            read_data: true,
            ..Default::default()
        };
        let mut object_options = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        for parity in [false, true] {
            object_options.versioned = parity;
            for corrupt_prefix in [false, true] {
                let object = format!("parity-{parity}-prefix-{corrupt_prefix}.bin");
                set.put_object(bucket, &object, &mut PutObjReader::from_vec(payload.clone()), &object_options)
                    .await
                    .expect("full-fanout PUT should persist every inline shard");

                let mut target = None;
                for (disk_index, disk) in disks.iter().enumerate() {
                    let metadata = disk
                        .read_version("", bucket, &object, "", &read_options)
                        .await
                        .expect("inline metadata should be readable");
                    let target_shard = if parity { metadata.erasure.data_blocks + 1 } else { 1 };
                    if metadata.erasure.index == target_shard {
                        target = Some((disk_index, metadata));
                        break;
                    }
                }
                let (disk_index, original_meta) = target.expect("requested data/parity shard should exist");
                let original_inline = original_meta.data.as_ref().expect("object should be inline");
                let metadata_path = temp_dirs[disk_index]
                    .path()
                    .join(bucket)
                    .join(&object)
                    .join(STORAGE_FORMAT_FILE);
                let mut damaged_raw = tokio::fs::read(&metadata_path).await.expect("physical xl.meta should exist");
                let offsets: Vec<_> = damaged_raw
                    .windows(original_inline.len())
                    .enumerate()
                    .filter_map(|(offset, bytes)| (bytes == original_inline.as_ref()).then_some(offset))
                    .collect();
                assert_eq!(offsets.len(), 1, "inline shard must have a unique physical byte range");
                let bitflip_offset = if corrupt_prefix { 0 } else { original_inline.len() - 1 };
                damaged_raw[offsets[0] + bitflip_offset] ^= 1;
                tokio::fs::write(&metadata_path, &damaged_raw)
                    .await
                    .expect("only the encoded inline shard should be damaged");
                let damaged_meta = disks[disk_index]
                    .read_version("", bucket, &object, "", &read_options)
                    .await
                    .expect("payload corruption must leave metadata parseable");
                assert!(damaged_meta.equals(&original_meta));
                assert_eq!(damaged_meta.version_id, original_meta.version_id);
                assert_eq!(damaged_meta.data_dir, original_meta.data_dir);
                assert_ne!(damaged_meta.data, original_meta.data);

                for scan_mode in [HealScanMode::Normal, HealScanMode::Deep] {
                    let (result, error) = set
                        .heal_object(
                            bucket,
                            &object,
                            "",
                            &HealOpts {
                                no_lock: true,
                                scan_mode,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("inline heal should complete");
                    assert!(error.is_none(), "inline heal should remain recoverable: {error:?}");
                    let deep = scan_mode == HealScanMode::Deep;
                    assert_eq!(result.drives_healed(), Some(usize::from(deep)), "{object}: {scan_mode:?}");
                    assert_eq!(result.after.drives[disk_index].state, DriveState::Ok.to_string());
                    if deep {
                        assert_eq!(result.before.drives[disk_index].state, DriveState::Corrupt.to_string());
                        let repaired = disks[disk_index]
                            .read_version("", bucket, &object, "", &read_options)
                            .await
                            .expect("repaired physical shard should be readable");
                        assert_eq!(repaired.data, original_meta.data, "heal must restore the exact encoded shard");
                        assert_eq!(repaired.version_id, original_meta.version_id);
                        assert_eq!(repaired.data_dir, original_meta.data_dir);
                        let rollback_dir = crate::disk::local::inline_metadata_rollback_dir(
                            repaired.version_id.unwrap_or_default(),
                            &rustfs_filemeta::FileMeta::new(),
                        );
                        assert!(
                            !metadata_path
                                .parent()
                                .expect("object directory")
                                .join(rollback_dir.to_string())
                                .join(crate::disk::STORAGE_FORMAT_FILE_BACKUP)
                                .exists(),
                            "inline bitrot repair must not retain a rollback backup"
                        );
                    }

                    for (range, expected) in [
                        (None, payload.as_slice()),
                        (
                            Some(HTTPRangeSpec {
                                start: 113,
                                end: 1023,
                                is_suffix_length: false,
                            }),
                            &payload[113..1024],
                        ),
                    ] {
                        let mut reader = set
                            .get_object_reader(bucket, &object, range, Default::default(), &object_options)
                            .await
                            .expect("redundant shards should serve full and range reads");
                        let mut body = Vec::new();
                        reader
                            .stream
                            .read_to_end(&mut body)
                            .await
                            .expect("GET should finish without truncation");
                        assert_eq!(body, expected);
                    }
                    if !deep {
                        assert_eq!(
                            tokio::fs::read(&metadata_path)
                                .await
                                .expect("damaged shard should remain on disk"),
                            damaged_raw,
                            "successful GET and normal heal do not prove the damaged shard was repaired"
                        );
                    }
                }

                let (healthy, error) = set
                    .heal_object(
                        bucket,
                        &object,
                        "",
                        &HealOpts {
                            no_lock: true,
                            scan_mode: HealScanMode::Deep,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("healthy inline object should pass a second deep scan");
                assert!(error.is_none());
                assert_eq!(healthy.drives_healed(), Some(0));
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn deep_heal_rebuilds_half_truncated_inline_metadata() {
        use tokio::io::AsyncReadExt;

        let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "heal-half-truncated-inline-metadata";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("create fixture volume");
        }
        let payload: Vec<u8> = (0..4095)
            .map(|index| u8::try_from(index % 251).expect("payload byte"))
            .collect();
        let options = ObjectOptions {
            write_completion: crate::object_api::WriteCompletion::TailDrained,
            ..Default::default()
        };
        set.put_object(bucket, object, &mut PutObjReader::from_vec(payload.clone()), &options)
            .await
            .expect("persist every inline shard before physical injection");
        let read_options = ReadOptions {
            read_data: true,
            ..Default::default()
        };
        let mut before = Vec::new();
        let mut metadata = Vec::new();
        for (dir, disk) in dirs.iter().zip(&disks) {
            before.push(
                tokio::fs::read(dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE))
                    .await
                    .expect("read original physical metadata"),
            );
            let info = disk
                .read_version("", bucket, object, "", &read_options)
                .await
                .expect("read original inline shard");
            assert!(info.data.as_ref().is_some_and(|bytes| !bytes.is_empty()));
            metadata.push(info);
        }
        let target = dirs[0].path().join(bucket).join(object).join(STORAGE_FORMAT_FILE);
        let truncated = &before[0][..before[0].len() / 2];
        tokio::fs::write(&target, truncated)
            .await
            .expect("truncate exact original metadata at half length");

        let (result, error) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("heal one truncated metadata replica with three healthy sources");
        assert!(error.is_none(), "truncated inline metadata must be repairable: {error:?}");
        assert_eq!(result.before.drives[0].state, DriveState::Corrupt.to_string());
        assert_eq!(result.drives_healed(), Some(1), "the damaged replica must actually be rebuilt");
        assert!(
            result
                .after
                .drives
                .iter()
                .all(|drive| drive.state == DriveState::Ok.to_string())
        );
        let repaired = disks[0]
            .read_version("", bucket, object, "", &read_options)
            .await
            .expect("repaired target metadata must parse");
        assert!(repaired.equals(&metadata[0]), "restore the original logical metadata");
        assert_eq!(repaired.erasure.index, metadata[0].erasure.index);
        assert_eq!(repaired.data, metadata[0].data, "restore the original encoded inline shard");
        assert_eq!(repaired.version_id, metadata[0].version_id);
        assert_eq!(repaired.data_dir, metadata[0].data_dir);
        for (dir, original) in dirs.iter().zip(&before).skip(1) {
            assert_eq!(
                tokio::fs::read(dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE))
                    .await
                    .expect("read healthy metadata after heal"),
                *original,
                "healthy metadata must remain byte-identical"
            );
        }
        let mut reader = set
            .get_object_reader(bucket, object, None, Default::default(), &options)
            .await
            .expect("read reconstructed object");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("read complete reconstructed payload");
        assert_eq!(body, payload);
    }

    #[tokio::test]
    async fn deep_heal_inline_bitrot_preserves_quorum_and_dry_run_boundaries() {
        for corrupt_shards in [2, 3] {
            let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
            let bucket = "deep-heal-inline-quorum";
            let object = "object.bin";
            for disk in &disks {
                disk.make_volume(bucket).await.expect("test bucket should be created");
            }
            set.put_object(
                bucket,
                object,
                &mut PutObjReader::from_vec(vec![0x71; 4113]),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("all inline shards should be committed");
            let read_options = ReadOptions {
                read_data: true,
                ..Default::default()
            };
            let mut originals = Vec::new();
            let mut damaged_files = Vec::new();
            for disk_index in 0..corrupt_shards {
                let metadata = disks[disk_index]
                    .read_version("", bucket, object, "", &read_options)
                    .await
                    .expect("source shard should be readable");
                let inline = metadata.data.expect("source shard should be inline");
                let path = temp_dirs[disk_index]
                    .path()
                    .join(bucket)
                    .join(object)
                    .join(STORAGE_FORMAT_FILE);
                let mut raw = tokio::fs::read(&path).await.expect("physical metadata should exist");
                let offsets: Vec<_> = raw
                    .windows(inline.len())
                    .enumerate()
                    .filter_map(|(offset, bytes)| (bytes == inline.as_ref()).then_some(offset))
                    .collect();
                assert_eq!(offsets.len(), 1, "shard byte range must be unique");
                raw[offsets[0] + inline.len() - 1] ^= 1;
                tokio::fs::write(&path, &raw).await.expect("inline shard should be damaged");
                originals.push(inline);
                damaged_files.push((path, raw));
            }
            let opts = HealOpts {
                no_lock: true,
                scan_mode: HealScanMode::Deep,
                ..Default::default()
            };
            let (dry_run, error) = set
                .heal_object(bucket, object, "", &HealOpts { dry_run: true, ..opts })
                .await
                .expect("dry-run should report inline corruption");
            assert!(error.is_none());
            assert_eq!(dry_run.drives_healed(), Some(0));
            for (disk_index, (path, raw)) in damaged_files.iter().enumerate() {
                assert_eq!(dry_run.after.drives[disk_index].state, DriveState::Corrupt.to_string());
                assert_eq!(tokio::fs::read(path).await.expect("dry-run should preserve metadata"), *raw);
            }

            let (result, error) = set
                .heal_object(bucket, object, "", &opts)
                .await
                .expect("heal should report its outcome");
            if corrupt_shards == 2 {
                assert!(error.is_none(), "exact read quorum should reconstruct: {error:?}");
                assert_eq!(result.drives_healed(), Some(2));
                for (disk_index, original) in originals.iter().enumerate() {
                    let repaired = disks[disk_index]
                        .read_version("", bucket, object, "", &read_options)
                        .await
                        .expect("reconstructed shard should be persisted");
                    assert_eq!(repaired.data.as_ref(), Some(original));
                }
            } else {
                assert_eq!(error, Some(DiskError::ErasureReadQuorum));
                assert_eq!(result.drives_healed(), Some(0));
                for (path, raw) in damaged_files {
                    assert_eq!(
                        tokio::fs::read(path)
                            .await
                            .expect("unrecoverable evidence must remain on disk"),
                        raw
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn deep_heal_inline_bitrot_rejects_unsupported_algorithms() {
        use crate::set_disk::disks_with_all_parts;
        use rustfs_filemeta::ChecksumInfo;
        use rustfs_utils::HashAlgorithm;

        let (_temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "deep-heal-inline-algorithm";
        let object = "object.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("test bucket should be created");
        }
        set.put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(vec![0x71; 4113]),
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("inline fixture should be committed");
        let mut metadata = Vec::new();
        for disk in &disks {
            metadata.push(
                disk.read_version(
                    "",
                    bucket,
                    object,
                    "",
                    &ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("inline metadata should decode"),
            );
        }
        let latest = metadata[1].clone();
        let mut online: Vec<_> = disks.into_iter().map(Some).collect();
        for algorithm in [
            HashAlgorithm::SHA256,
            HashAlgorithm::HighwayHash256,
            HashAlgorithm::BLAKE2b512,
        ] {
            metadata[0].erasure.checksums = vec![ChecksumInfo {
                part_number: 1,
                algorithm,
                ..Default::default()
            }];
            for mode in [HealScanMode::Normal, HealScanMode::Deep] {
                let result = disks_with_all_parts(
                    &mut online,
                    &mut metadata,
                    &[None, None, None, None],
                    &latest,
                    false,
                    bucket,
                    object,
                    mode,
                )
                .await;
                if mode == HealScanMode::Deep {
                    assert_eq!(
                        result.expect_err("unverifiable inline data must not become healthy"),
                        DiskError::BitrotHashAlgoInvalid
                    );
                } else {
                    let (by_disk, _) = result.expect("normal scan should retain presence-only behavior");
                    assert_eq!(by_disk[&0], vec![crate::disk::CHECK_PART_SUCCESS]);
                }
            }
        }
    }

    #[tokio::test]
    async fn deep_heal_inline_bitrot_accepts_persisted_empty_objects() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "deep-heal-inline-empty";
        let object = "empty.bin";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("test bucket should be created");
        }
        set.put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(Vec::new()),
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("empty object should be committed");
        let mut originals = Vec::new();
        for dir in &temp_dirs {
            let path = dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE);
            originals.push((path.clone(), tokio::fs::read(path).await.expect("empty object metadata should exist")));
        }
        let (result, error) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("empty object should pass deep heal");
        assert!(error.is_none());
        assert_eq!(result.drives_healed(), Some(0));
        for (path, raw) in originals {
            assert_eq!(tokio::fs::read(path).await.expect("healthy metadata should remain on disk"), raw);
        }
    }

    #[tokio::test]
    async fn replacement_target_readback_checks_the_requested_historical_version() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "replacement-target-readback-versioned";
        let object = "object.bin";
        set.make_bucket(
            bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("versioned bucket should be created");

        // The per-version target-evidence readback below asserts per-disk
        // state right after PUT. A lock-owning PUT may quorum-ack before its
        // rename tail drains, so keep the setup on the full-fanout commit path.
        let mut old_reader = PutObjReader::from_vec(vec![0x5a; 1024 * 1024]);
        let old_info = set
            .put_object(
                bucket,
                object,
                &mut old_reader,
                &ObjectOptions {
                    versioned: true,
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("old object version should be written");
        let old_version = old_info
            .version_id
            .expect("versioned put should return the old version id")
            .to_string();
        let mut latest_reader = PutObjReader::from_vec(vec![0x33; 1024 * 1024]);
        let latest_info = set
            .put_object(
                bucket,
                object,
                &mut latest_reader,
                &ObjectOptions {
                    versioned: true,
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("latest object version should be written");
        let latest_version = latest_info
            .version_id
            .expect("versioned put should return the latest version id")
            .to_string();
        let old_source = disks[2]
            .read_version("", bucket, object, &old_version, &ReadOptions::default())
            .await
            .expect("old version metadata should be readable");
        let old_data_dir = old_source.data_dir.expect("old version should have a data directory");
        let targets = vec![set.set_endpoints[0].to_string(), set.set_endpoints[1].to_string()];

        assert!(
            set.replacement_targets_have_version(bucket, object, &old_version, &targets)
                .await
                .expect("healthy historical target shards should be readable")
        );
        assert!(
            set.replacement_targets_have_version(bucket, object, &latest_version, &targets)
                .await
                .expect("healthy latest target shards should be readable")
        );

        tokio::fs::remove_file(
            temp_dirs[1]
                .path()
                .join(bucket)
                .join(object)
                .join(old_data_dir.to_string())
                .join("part.1"),
        )
        .await
        .expect("old target shard should be removed after the initial commit");

        assert!(
            !set.replacement_targets_have_version(bucket, object, &old_version, &targets)
                .await
                .expect("missing old target shard should be observable")
        );
        assert!(
            set.replacement_targets_have_version(bucket, object, &latest_version, &targets)
                .await
                .expect("latest target evidence should remain independent")
        );
    }

    #[tokio::test]
    async fn format_heal_cached_layout_rejects_a_disk_from_another_slot() {
        let mut _temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();
        for disk_index in 0..3 {
            let (temp_dir, mut endpoint, disk) = real_disk().await;
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            _temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }
        let set = set_disks_with(disks.clone(), endpoints, 1).await;
        let mut wrong_slot = set.format.clone();
        wrong_slot.erasure.this = set.format.erasure.sets[0][1];
        save_format_file(&disks[0], &Some(wrong_slot))
            .await
            .expect("wrong-slot format fixture should be saved");
        let mut correct_slot = set.format.clone();
        correct_slot.erasure.this = set.format.erasure.sets[0][2];
        save_format_file(&disks[2], &Some(correct_slot))
            .await
            .expect("correct format fixture should be saved");

        let (_, heal_err) = set
            .heal_format(false)
            .await
            .expect("format heal should report the quorum failure in its result");

        assert!(matches!(heal_err, Some(Error::CorruptedFormat)));
        let unformatted = load_format_erasure(disks[1].as_ref().expect("second disk should be online"), true)
            .await
            .expect_err("a rejected fallback must not format the missing slot");
        assert_eq!(unformatted, DiskError::UnformattedDisk);
    }

    #[derive(Clone, Copy)]
    enum InventoryWriterHealCase {
        Existing,
        Missing,
        MissingVersion,
    }

    async fn assert_heal_object_inventory_writer(case: InventoryWriterHealCase) {
        use crate::set_disk::core::io_primitives::disk_call_counters;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        let (_temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "heal-inventory-writer-bucket";
        let object = match case {
            InventoryWriterHealCase::Existing => "heal-inventory-writer-existing",
            InventoryWriterHealCase::Missing => "heal-inventory-writer-missing",
            InventoryWriterHealCase::MissingVersion => "heal-inventory-writer-missing-version",
        };
        set.make_bucket(
            bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("heal fixture bucket should be created");
        let body = vec![0x67; 64 * 1024];
        let stored_version = Uuid::new_v4();
        let stored_version_string = stored_version.to_string();
        let published = if matches!(case, InventoryWriterHealCase::Missing) {
            None
        } else {
            let mut reader = PutObjReader::from_vec(body.clone());
            let info = set
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        no_lock: true,
                        versioned: true,
                        version_id: Some(stored_version_string.clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("full-fanout PUT should seed the heal fixture");
            for disk in &disks {
                let metadata = disk
                    .read_version("", bucket, object, &stored_version_string, &ReadOptions::default())
                    .await
                    .expect("the seeded version must be present on every disk");
                assert_eq!(metadata.version_id, Some(stored_version));
                assert_eq!(metadata.size, i64::try_from(body.len()).expect("fixture size should fit i64"));
            }
            Some(info)
        };
        let requested_version = match case {
            InventoryWriterHealCase::Existing => stored_version_string.clone(),
            InventoryWriterHealCase::Missing => String::new(),
            InventoryWriterHealCase::MissingVersion => Uuid::new_v4().to_string(),
        };
        let opts = HealOpts {
            no_lock: true,
            ..Default::default()
        };
        let calls = disk_call_counters::observe(object);
        let read_gate = set.disks.read().await;
        // UFCS selects the trait's outer precheck, not the same-named inherent heal.
        let heal = <SetDisks as crate::storage_api_contracts::heal::HealOperations>::heal_object(
            set.as_ref(),
            bucket,
            object,
            &requested_version,
            &opts,
        );
        tokio::pin!(heal);
        assert!(matches!(
            futures::poll!(tokio::task::unconstrained(heal.as_mut())),
            std::task::Poll::Pending
        ));
        // These tests use the current-thread runtime: full-wait metadata tasks
        // have been spawned, but cannot run during the single unconstrained poll.
        assert_eq!(calls.total(disk_call_counters::KIND_READ_VERSION), 0);
        let writer = set.disks.write();
        tokio::pin!(writer);
        assert!(matches!(
            futures::poll!(tokio::task::unconstrained(writer.as_mut())),
            std::task::Poll::Pending
        ));
        assert!(set.disks.try_read().is_err(), "the writer must already block new inventory readers");
        tokio::time::timeout(Duration::from_secs(5), async {
            while calls.total(disk_call_counters::KIND_READ_VERSION) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the suspended trait heal must have started the real metadata fanout");
        for disk_index in 0..4 {
            assert_eq!(calls.for_disk(disk_call_counters::KIND_READ_VERSION, disk_index), 1);
        }
        drop(read_gate);

        let (_, outcome) =
            tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(async { drop(writer.await) }, heal) })
                .await
                .expect("trait heal must not deadlock its nested inventory read with the queued writer");
        let (result, error) = outcome.expect("heal should report the object's outcome");
        match case {
            InventoryWriterHealCase::Existing => assert!(error.is_none(), "existing object heal failed: {error:?}"),
            InventoryWriterHealCase::Missing => assert!(matches!(error, Some(Error::FileNotFound))),
            InventoryWriterHealCase::MissingVersion => assert!(matches!(error, Some(Error::FileVersionNotFound))),
        }
        assert_eq!(result.bucket, bucket);
        assert_eq!(result.object, object);
        assert_eq!(result.version_id, requested_version);
        assert_eq!(result.disk_count, 4);
        assert_eq!(result.before.drives.len(), 4);
        assert_eq!(result.after.drives.len(), 4);
        for disk_index in 0..4 {
            let endpoint = set.set_endpoints[disk_index].to_string();
            assert_eq!(result.before.drives[disk_index].endpoint, endpoint);
            assert_eq!(result.after.drives[disk_index].endpoint, endpoint);
        }
        if let Some(published) = published {
            tokio::time::timeout(Duration::from_secs(10), async {
                let mut reader = set
                    .get_object_reader(
                        bucket,
                        object,
                        None,
                        Default::default(),
                        &ObjectOptions {
                            versioned: true,
                            version_id: Some(stored_version_string),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("the stored version must remain readable after heal");
                assert_eq!(reader.object_info.etag, published.etag);
                assert_eq!(reader.object_info.version_id, Some(stored_version));
                let mut observed_body = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut observed_body)
                    .await
                    .expect("stored body should stream");
                assert_eq!(observed_body, body);
            })
            .await
            .expect("GET must finish after the inventory writer and heal");
        }
    }

    #[tokio::test]
    async fn heal_object_inventory_writer_existing() {
        assert_heal_object_inventory_writer(InventoryWriterHealCase::Existing).await;
    }

    #[tokio::test]
    async fn heal_object_inventory_writer_missing() {
        assert_heal_object_inventory_writer(InventoryWriterHealCase::Missing).await;
    }

    #[tokio::test]
    async fn heal_object_inventory_writer_missing_version() {
        assert_heal_object_inventory_writer(InventoryWriterHealCase::MissingVersion).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn heal_object_with_queued_disk_renewal() {
        use crate::layout::endpoints::SetupType;
        use crate::runtime::instance::InstanceContext;
        use crate::set_disk::core::io_primitives::disk_call_counters;
        use std::collections::HashMap;
        use std::future::Future;
        use std::task::Poll;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        // renew_disk still registers local disks on the ambient context. Match
        // the default serial group used by its other setup/registry fixtures,
        // and restore only this temporary endpoint, including on a failed join.
        struct RenewDiskTestState {
            ctx: Arc<InstanceContext>,
            was_dist_erasure: bool,
            map: Arc<RwLock<HashMap<String, Option<DiskStore>>>>,
            endpoint: String,
            previous_disk: Option<Option<DiskStore>>,
        }

        impl Drop for RenewDiskTestState {
            fn drop(&mut self) {
                let ctx = self.ctx.clone();
                let was_dist_erasure = self.was_dist_erasure;
                let map = self.map.clone();
                let endpoint = self.endpoint.clone();
                let previous_disk = self.previous_disk.take();
                let handle = tokio::runtime::Handle::current();
                std::thread::spawn(move || {
                    handle.block_on(async move {
                        let mut map = map.write().await;
                        match previous_disk {
                            Some(disk) => {
                                map.insert(endpoint, disk);
                            }
                            None => {
                                map.remove(&endpoint);
                            }
                        }
                        drop(map);
                        if was_dist_erasure {
                            ctx.update_erasure_type(SetupType::DistErasure).await;
                        }
                    });
                })
                .join()
                .expect("renew fixture state restoration should finish");
            }
        }

        let (_temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let endpoint = set.set_endpoints[0].clone();
        let ctx = crate::runtime::global::current_ctx();
        let map = ctx.local_disk_map();
        let restore = RenewDiskTestState {
            ctx: ctx.clone(),
            was_dist_erasure: ctx.is_dist_erasure().await,
            map: map.clone(),
            endpoint: endpoint.to_string(),
            previous_disk: map.read().await.get(&endpoint.to_string()).cloned(),
        };
        // Only distributed erasure needs an override to avoid the ambient slot array.
        if restore.was_dist_erasure {
            ctx.update_erasure_type(SetupType::Erasure).await;
        }

        let bucket = "heal-disk-renewal-bucket";
        let object = "heal-disk-renewal-object";
        set.make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("renew fixture bucket should be created");
        let body = vec![0x73; 64 * 1024];
        let mut reader = PutObjReader::from_vec(body.clone());
        let published = set
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("full-fanout PUT should seed the renewal fixture");
        for disk in &disks {
            let metadata = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("the seeded object must be present on every disk");
            assert_eq!(metadata.size, i64::try_from(body.len()).expect("fixture size should fit i64"));
        }

        let opts = HealOpts {
            no_lock: true,
            ..Default::default()
        };
        let calls = disk_call_counters::observe(object);
        let read_gate = set.disks.read().await;
        let heal = <SetDisks as crate::storage_api_contracts::heal::HealOperations>::heal_object(
            set.as_ref(),
            bucket,
            object,
            "",
            &opts,
        );
        tokio::pin!(heal);
        assert!(matches!(futures::poll!(tokio::task::unconstrained(heal.as_mut())), Poll::Pending));
        assert_eq!(calls.total(disk_call_counters::KIND_READ_VERSION), 0);

        let renew = set.renew_disk(&endpoint);
        tokio::pin!(renew);
        tokio::time::timeout(
            Duration::from_secs(5),
            futures::future::poll_fn(|cx| {
                assert!(
                    std::pin::pin!(tokio::task::unconstrained(renew.as_mut()))
                        .poll(cx)
                        .is_pending(),
                    "renewal must reach its inventory write before returning"
                );
                if set.disks.try_read().is_err() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("real renewal must queue its topology writer behind the read gate");
        let registered = map
            .read()
            .await
            .get(&endpoint.to_string())
            .cloned()
            .flatten()
            .expect("renewal must register the connected disk before its inventory write");
        assert!(!Arc::ptr_eq(&registered, &disks[0]), "renewal must construct a new disk handle");
        tokio::time::timeout(Duration::from_secs(5), async {
            while calls.total(disk_call_counters::KIND_READ_VERSION) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the suspended trait heal must have started the real metadata fanout");
        for disk_index in 0..4 {
            assert_eq!(calls.for_disk(disk_call_counters::KIND_READ_VERSION, disk_index), 1);
        }
        drop(read_gate);

        let (_, outcome) = tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(renew, heal) })
            .await
            .expect("trait heal and real disk renewal must finish without a nested inventory read deadlock");
        let (report, error) = outcome.expect("heal should report the existing object");
        assert!(error.is_none(), "existing object heal failed after renewal: {error:?}");
        assert_eq!(report.bucket, bucket);
        assert_eq!(report.object, object);
        assert_eq!(report.disk_count, 4);
        let renewed = set.get_disks_internal().await[0]
            .clone()
            .expect("the renewed slot must remain online");
        assert!(Arc::ptr_eq(&renewed, &registered), "the set must publish the newly connected handle");
        assert_eq!(renewed.endpoint(), endpoint);
        let format = load_format_erasure(&renewed, false)
            .await
            .expect("renewed disk format should remain readable");
        assert_eq!(format.erasure.this, set.format.erasure.sets[0][0]);
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut reader = set
                .get_object_reader(bucket, object, None, Default::default(), &ObjectOptions::default())
                .await
                .expect("the object must remain readable after renewal and heal");
            assert_eq!(reader.object_info.etag, published.etag);
            let mut observed_body = Vec::new();
            reader
                .stream
                .read_to_end(&mut observed_body)
                .await
                .expect("stored body should stream");
            assert_eq!(observed_body, body);
        })
        .await
        .expect("GET must finish after renewal and heal");
    }

    // Regression for #955: an offline disk must contribute exactly one drive
    // record. Before the fix the offline branch fell through and pushed a second
    // (Corrupt) record for the same disk, so `before/after.drives` grew to
    // `disk_count + offline_count` and every entry after the first offline slot
    // was misaligned relative to its disk index.
    #[tokio::test]
    async fn default_heal_result_reports_one_record_per_disk_and_stays_aligned() {
        // index 0: online, no error       -> Ok
        // index 1: offline (None)         -> Offline (single record)
        // index 2: online, FileNotFound   -> Missing
        // index 3: online, DiskAccessDenied-> Corrupt
        let (_d0, ep0, disk0) = real_disk().await;
        let (_d2, ep2, disk2) = real_disk().await;
        let (_d3, ep3, disk3) = real_disk().await;
        let ep1 = Endpoint::try_from("http://127.0.0.1:9001/data").expect("endpoint should parse");

        let disks = vec![Some(disk0), None, Some(disk2), Some(disk3)];
        let endpoints = vec![ep0, ep1, ep2, ep3];
        let set = set_disks_with(disks, endpoints, 1).await;

        let errs = vec![
            None,
            Some(DiskError::DiskNotFound),
            Some(DiskError::FileNotFound),
            Some(DiskError::DiskAccessDenied),
        ];

        let result = set
            .default_heal_result(FileInfo::default(), &errs, "bucket", "object", "")
            .await;

        // Exactly one record per disk (not disk_count + offline_count).
        assert_eq!(result.disk_count, 4);
        assert_eq!(result.before.drives.len(), 4, "one before record per disk");
        assert_eq!(result.after.drives.len(), 4, "one after record per disk");

        // Records stay index-aligned with the disk vector and set_endpoints.
        let expected_states = [
            DriveState::Ok.to_string(),
            DriveState::Offline.to_string(),
            DriveState::Missing.to_string(),
            DriveState::Corrupt.to_string(),
        ];
        for (i, expected) in expected_states.iter().enumerate() {
            assert_eq!(&result.before.drives[i].state, expected, "before state at {i}");
            assert_eq!(&result.after.drives[i].state, expected, "after state at {i}");
            assert_eq!(
                result.before.drives[i].endpoint,
                set.set_endpoints[i].to_string(),
                "before endpoint aligned at {i}"
            );
            assert_eq!(
                result.after.drives[i].endpoint,
                set.set_endpoints[i].to_string(),
                "after endpoint aligned at {i}"
            );
        }

        // The offline endpoint appears exactly once, never as a second Corrupt row.
        let offline_ep = set.set_endpoints[1].to_string();
        assert_eq!(
            result.before.drives.iter().filter(|d| d.endpoint == offline_ep).count(),
            1,
            "offline disk must not produce a duplicate record"
        );
    }

    // Two interleaved offline disks: assert every record still maps to its own
    // set_endpoints[index] (no cumulative drift after the first offline slot).
    #[tokio::test]
    async fn default_heal_result_alignment_with_multiple_offline_disks() {
        let (_d1, ep1, disk1) = real_disk().await;
        let (_d3, ep3, disk3) = real_disk().await;
        let ep0 = Endpoint::try_from("http://127.0.0.1:9000/data").expect("endpoint should parse");
        let ep2 = Endpoint::try_from("http://127.0.0.1:9002/data").expect("endpoint should parse");

        // index 0 offline, 1 online, 2 offline, 3 online.
        let disks = vec![None, Some(disk1), None, Some(disk3)];
        let endpoints = vec![ep0, ep1, ep2, ep3];
        let set = set_disks_with(disks, endpoints, 1).await;

        let errs = vec![Some(DiskError::DiskNotFound), None, Some(DiskError::DiskNotFound), None];

        let result = set
            .default_heal_result(FileInfo::default(), &errs, "bucket", "object", "")
            .await;

        assert_eq!(result.before.drives.len(), 4);
        assert_eq!(result.after.drives.len(), 4);
        for i in 0..4 {
            assert_eq!(result.before.drives[i].endpoint, set.set_endpoints[i].to_string(), "aligned at {i}");
        }
        assert_eq!(result.before.drives[0].state, DriveState::Offline.to_string());
        assert_eq!(result.before.drives[1].state, DriveState::Ok.to_string());
        assert_eq!(result.before.drives[2].state, DriveState::Offline.to_string());
        assert_eq!(result.before.drives[3].state, DriveState::Ok.to_string());
    }

    #[tokio::test]
    async fn dangling_object_dir_delete_preserves_results_and_propagates_failure() {
        let bucket = "bucket-dangling-dir-delete";
        let object = "dangling__XLDIR__";
        let mut temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();
        for _ in 0..8 {
            let (temp_dir, endpoint, disk) = real_disk().await;
            disk.make_volume(bucket).await.expect("test bucket should be created");
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }
        disks[0] = None;
        let set = set_disks_with(disks, endpoints, 4).await;
        for disk_index in [1, 2] {
            tokio::fs::create_dir_all(temp_dirs[disk_index].path().join(bucket).join(object))
                .await
                .expect("dangling object directory should be created");
        }

        let _delete_failure = DanglingDeleteFailure::install(bucket, object, 2, DiskError::DiskAccessDenied);
        let _file_missing = DanglingDeleteFailure::install(bucket, object, 3, DiskError::FileNotFound);
        let _version_missing = DanglingDeleteFailure::install(bucket, object, 4, DiskError::FileVersionNotFound);
        let _path_missing = DanglingDeleteFailure::install(bucket, object, 5, DiskError::PathNotFound);
        let _volume_missing = DanglingDeleteFailure::install(bucket, object, 6, DiskError::VolumeNotFound);
        let _disk_missing = DanglingDeleteFailure::install(bucket, object, 7, DiskError::DiskNotFound);

        let (result, err) = set
            .heal_object_dir_locked(bucket, object, false, true)
            .await
            .expect("dangling directory heal should report its per-disk delete results");

        assert_eq!(err, Some(DiskError::DiskNotFound));
        assert_eq!(result.before.drives.len(), 8);
        assert_eq!(result.after.drives.len(), 8);
        assert_eq!(result.before.drives[0].state, DriveState::Offline.to_string());
        assert_eq!(result.after.drives[0].state, DriveState::Offline.to_string());
        assert_eq!(result.before.drives[1].state, DriveState::Ok.to_string());
        assert_eq!(result.after.drives[1].state, DriveState::Missing.to_string());
        assert_eq!(result.before.drives[2].state, DriveState::Ok.to_string());
        assert_eq!(result.after.drives[2].state, DriveState::Corrupt.to_string());
        assert_eq!(result.before.drives[3].state, DriveState::Missing.to_string());
        assert_eq!(result.after.drives[3].state, DriveState::Missing.to_string());
        for disk_index in [4, 5, 6] {
            assert_eq!(result.before.drives[disk_index].state, DriveState::Missing.to_string());
            assert_eq!(result.after.drives[disk_index].state, DriveState::Corrupt.to_string());
        }
        assert_eq!(result.before.drives[7].state, DriveState::Missing.to_string());
        assert_eq!(result.after.drives[7].state, DriveState::Offline.to_string());
        assert!(
            !temp_dirs[1].path().join(bucket).join(object).exists(),
            "successful delete must remove the dangling directory"
        );
        assert!(
            temp_dirs[2].path().join(bucket).join(object).is_dir(),
            "failed delete must leave the dangling directory for retry"
        );
    }

    async fn write_metadata_less_data_dir(root: &std::path::Path, bucket: &str, object: &str, data_dir: Uuid) {
        let data_dir_path = root.join(bucket).join(object).join(data_dir.to_string());
        tokio::fs::create_dir_all(&data_dir_path)
            .await
            .expect("metadata-less data dir should be created");
        tokio::fs::write(data_dir_path.join("part.1"), b"orphaned-shard")
            .await
            .expect("metadata-less part should be written");
    }

    #[tokio::test]
    async fn heal_object_remove_reclaims_metadata_less_data_dirs() {
        let bucket = "bucket-metadata-less-remove";
        let object = "object.bin";
        let mut temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();
        let mut data_dirs = Vec::new();
        for _ in 0..2 {
            let (temp_dir, endpoint, disk) = real_disk().await;
            disk.make_volume(bucket).await.expect("test bucket should be created");
            let data_dir = Uuid::new_v4();
            write_metadata_less_data_dir(temp_dir.path(), bucket, object, data_dir).await;
            data_dirs.push(data_dir);
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }
        let set = set_disks_with(disks, endpoints, 1).await;

        let (result, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    remove: true,
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("metadata-less remove should complete");

        assert_eq!(err, Some(DiskError::FileNotFound));
        assert!(
            result.detail.contains("matched=2, removed=2"),
            "heal result should report reclaimed metadata-less dirs: {}",
            result.detail
        );
        for (temp_dir, data_dir) in temp_dirs.iter().zip(data_dirs.iter()) {
            assert!(
                !temp_dir.path().join(bucket).join(object).join(data_dir.to_string()).exists(),
                "metadata-less data dir should be removed"
            );
        }

        set.delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("metadata-less data-dir cleanup should unblock non-force bucket deletion");
    }

    #[tokio::test]
    async fn heal_object_remove_dry_run_reports_metadata_less_data_dirs_without_deleting() {
        let bucket = "bucket-metadata-less-dry-run";
        let object = "object.bin";
        let (temp_dir, endpoint, disk) = real_disk().await;
        disk.make_volume(bucket).await.expect("test bucket should be created");
        let data_dir = Uuid::new_v4();
        write_metadata_less_data_dir(temp_dir.path(), bucket, object, data_dir).await;
        let set = set_disks_with(vec![Some(disk)], vec![endpoint], 0).await;

        let (result, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    dry_run: true,
                    remove: true,
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("metadata-less dry-run should complete");

        assert_eq!(err, Some(DiskError::FileNotFound));
        assert!(
            result.detail.contains("matched=1, removed=0, dry_run=true"),
            "dry-run should report a match without deletion: {}",
            result.detail
        );
        assert!(
            temp_dir.path().join(bucket).join(object).join(data_dir.to_string()).exists(),
            "dry-run must preserve metadata-less data dir"
        );
    }

    #[tokio::test]
    async fn heal_object_remove_preserves_metadata_less_data_dir_with_precommit_marker() {
        let bucket = "bucket-metadata-less-precommit";
        let object = "object.bin";
        let (temp_dir, endpoint, disk) = real_disk().await;
        disk.make_volume(bucket).await.expect("test bucket should be created");
        let data_dir = Uuid::new_v4();
        write_metadata_less_data_dir(temp_dir.path(), bucket, object, data_dir).await;
        let data_dir_path = temp_dir.path().join(bucket).join(object).join(data_dir.to_string());
        tokio::fs::write(
            data_dir_path.join(format!("{}{}", RESERVED_DELETE_DATA_DIR_MARKER_PREFIX, Uuid::new_v4())),
            [],
        )
        .await
        .expect("pre-commit reservation marker should be written");
        let set = set_disks_with(vec![Some(disk)], vec![endpoint], 0).await;

        let (result, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    remove: true,
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("metadata-less precommit state should fail closed");

        assert_eq!(err, Some(DiskError::FileNotFound));
        assert!(
            result.detail.is_empty(),
            "pre-commit residue must not be classified as reclaimable metadata-less data"
        );
        assert!(data_dir_path.exists(), "pre-commit residue must be preserved");
    }

    #[tokio::test]
    async fn dangling_delete_guard_preserves_conflicting_identities_without_writing_metadata() {
        let bucket = "bucket-delete-guard-conflict";
        let object = "object.bin";
        let old_data_dir = Uuid::parse_str("99999999-9999-9999-9999-999999999999").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("new data dir should parse");
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(old_data_dir, 4), (new_data_dir, 2)]).await;
        let version_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("version id should parse");
        let mut metadata = vec![
            meta_regen_test_fileinfo(object, old_data_dir, 9, 0),
            meta_regen_test_fileinfo(object, new_data_dir, 10, 1),
            FileInfo::default(),
            FileInfo::default(),
        ];
        metadata[0].version_id = Some(version_id);
        metadata[1].version_id = Some(version_id);
        assert_eq!(
            metadata[0].version_id, metadata[1].version_id,
            "the conflicting candidates must share one version id"
        );
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &metadata[0]).await;
        seed_meta_regen_test_metadata(&disks, 1, bucket, object, &metadata[1]).await;
        let errs = vec![None, None, Some(DiskError::FileNotFound), Some(DiskError::FileNotFound)];

        assert!(
            set.dangling_delete_safety(bucket, object, &metadata, &errs, &disks)
                .await
                .expect("conflicting identities should be classified")
                == DanglingDeleteSafety::UnsafeToDelete
        );
        let reversed = vec![
            metadata[1].clone(),
            metadata[0].clone(),
            FileInfo::default(),
            FileInfo::default(),
        ];
        assert!(
            set.dangling_delete_safety(bucket, object, &reversed, &errs, &disks)
                .await
                .expect("reversed identities should be classified")
                == DanglingDeleteSafety::UnsafeToDelete
        );
        let version_id = version_id.to_string();
        assert!(
            !set.try_regenerate_explicit_version_meta(bucket, object, &version_id, &metadata, &errs, &disks)
                .await
                .expect("conflicting explicit-version candidates should be rejected"),
            "an explicit version must not select between conflicting metadata identities"
        );
        for disk_index in [2, 3] {
            assert!(
                matches!(
                    disks[disk_index]
                        .as_ref()
                        .expect("test disk should be online")
                        .read_version("", bucket, object, "", &ReadOptions::default())
                        .await,
                    Err(DiskError::FileNotFound)
                ),
                "the delete guard must not manufacture metadata on missing disks"
            );
        }
        let old = disks[0]
            .as_ref()
            .expect("first test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("old metadata should remain readable");
        let new = disks[1]
            .as_ref()
            .expect("second test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("new metadata should remain readable");
        assert_eq!(old.data_dir, Some(old_data_dir));
        assert_eq!(new.data_dir, Some(new_data_dir));
    }

    #[tokio::test]
    async fn heal_meta_quorum_failure_preserves_reconstructable_uncommitted_candidate() {
        let bucket = "bucket-delete-guard-reconstructable";
        let object = "object.bin";
        let data_dir = Uuid::parse_str("33333333-3333-3333-3333-333333333333").expect("data dir should parse");
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(data_dir, 2)]).await;
        let metadata = [
            meta_regen_test_fileinfo(object, data_dir, 3, 0),
            FileInfo::default(),
            FileInfo::default(),
            FileInfo::default(),
        ];
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &metadata[0]).await;
        let (observed_metadata, observed_errs) = SetDisks::read_all_fileinfo(&disks, "", bucket, object, "", true, true, false)
            .await
            .expect("test metadata should be readable across the set");
        assert_eq!(
            set.dangling_delete_safety(bucket, object, &observed_metadata, &observed_errs, &disks)
                .await
                .expect("observed reconstructable candidate should be classified"),
            DanglingDeleteSafety::UnsafeToDelete
        );

        let (_, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    remove: true,
                    ..Default::default()
                },
            )
            .await
            .expect("unsafe dangling state should be reported without deletion");
        assert_eq!(err, Some(DiskError::ErasureReadQuorum));
        let surviving = disks[0]
            .as_ref()
            .expect("first test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("the only metadata copy must be preserved");
        assert_eq!(surviving.data_dir, Some(data_dir));
        assert!(
            matches!(
                disks[1]
                    .as_ref()
                    .expect("second test disk should be online")
                    .read_version("", bucket, object, "", &ReadOptions::default())
                    .await,
                Err(DiskError::FileNotFound)
            ),
            "the delete guard must not propagate metadata"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn dangling_absence_proof_rejects_failed_stale_replica_and_accepts_retry() {
        temp_env::async_with_vars([("RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS", Some("0"))], async {
            let bucket = "dangling-absence-partial-delete";
            let object = "history.txt";
            let (_temp_dirs, set, disks) =
                dangling_inline_test_fixture(bucket, object, OffsetDateTime::now_utc() - time::Duration::hours(2)).await;
            let version = Uuid::new_v4();
            let disk = disks[0].as_ref().expect("stale disk must be online");
            let mut metadata = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("load stale inline metadata");
            metadata.version_id = Some(version);
            disk.write_metadata("", bucket, object, metadata)
                .await
                .expect("seed exact stale historical version");
            let opts = HealOpts {
                no_lock: true,
                remove: true,
                scan_mode: HealScanMode::Deep,
                ..Default::default()
            };
            let failure = DanglingDeleteFailure::install(bucket, object, 0, DiskError::FaultyDisk);
            let mut proof = None;
            let (_, error) = set
                .heal_object_with_absence(
                    bucket,
                    object,
                    &version.to_string(),
                    &opts,
                    &mut proof,
                    AbsenceProofRequest {
                        retirement: None,
                        allow_unversioned: false,
                    },
                )
                .await
                .expect("heal should return a per-object failure");
            assert!(
                error.is_some(),
                "three absent slots meeting write quorum cannot hide the failed stale slot"
            );
            assert!(proof.is_none(), "partial cleanup must not produce an absence proof");
            assert!(
                disk.read_version("", bucket, object, &version.to_string(), &ReadOptions::default())
                    .await
                    .is_ok(),
                "the failed historical version must remain for retry"
            );
            drop(failure);
            let (result, error) = set
                .heal_object_with_absence(
                    bucket,
                    object,
                    &version.to_string(),
                    &opts,
                    &mut proof,
                    AbsenceProofRequest {
                        retirement: None,
                        allow_unversioned: false,
                    },
                )
                .await
                .expect("retry should execute cleanup");
            assert!(error.is_none(), "retry should complete: {error:?}");
            let receipt = proof.take().expect("successful exact cleanup must produce proof");
            assert!(receipt.removed);
            assert_eq!((receipt.pool_index, receipt.set_index), (set.pool_index, set.set_index));
            assert!(
                result
                    .after
                    .drives
                    .iter()
                    .all(|drive| drive.state == DriveState::Missing.to_string())
            );
            let (_, _) = set
                .heal_object_with_absence(
                    bucket,
                    object,
                    &version.to_string(),
                    &opts,
                    &mut proof,
                    AbsenceProofRequest {
                        retirement: None,
                        allow_unversioned: false,
                    },
                )
                .await
                .expect("already absent replay should execute");
            assert!(!proof.expect("exact already-absent replay must remain provable").removed);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heal_without_remove_preserves_unrecoverable_unversioned_shards() {
        use crate::disk::{CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS};
        use crate::object_api::WriteCompletion;
        use futures::FutureExt as _;
        use std::panic::AssertUnwindSafe;
        use std::time::Duration;

        // Exercise the same on-disk layout with explicit removal first, so
        // an unrelated early failure cannot masquerade as preservation.
        for remove in [true, false] {
            let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
            let outcome = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(45), async {
                let bucket = if remove {
                    "heal-remove-unversioned"
                } else {
                    "heal-preserve-unversioned"
                };
                let object = "object.bin";
                let old_time = OffsetDateTime::from_unix_timestamp(1).expect("fixture modification time");
                let elapsed = (OffsetDateTime::now_utc() - old_time).whole_seconds();
                let grace = rustfs_utils::get_env_u64("RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS", 3600);
                assert!(
                    elapsed > i64::try_from(grace).unwrap_or(i64::MAX),
                    "fixture must be outside the configured dangling-delete grace period"
                );
                set.make_bucket(bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create fixture bucket");
                let options = ObjectOptions {
                    mod_time: Some(old_time),
                    write_completion: WriteCompletion::TailDrained,
                    ..Default::default()
                };
                set.put_object(bucket, object, &mut PutObjReader::from_vec(vec![0x5a; 1024 * 1024]), &options)
                    .await
                    .expect("write all four fixture shards");

                let mut metadata = Vec::new();
                let mut infos = Vec::new();
                for (dir, disk) in dirs.iter().zip(&disks) {
                    metadata.push(
                        tokio::fs::read(dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE))
                            .await
                            .expect("read each committed metadata file"),
                    );
                    infos.push(
                        disk.read_version("", bucket, object, "", &ReadOptions::default())
                            .await
                            .expect("read each committed unversioned metadata replica"),
                    );
                }
                let selected = &infos[0];
                let data_dir = selected.data_dir.expect("fixture must be non-inline");
                assert!(selected.version_id.is_none_or(|id| id.is_nil()));
                assert_eq!(selected.mod_time, Some(old_time));
                let missing_count = selected.erasure.parity_blocks + 1;
                assert!(missing_count < disks.len(), "at least one physical shard must survive");
                assert!(infos.iter().all(|info| {
                    info.data_dir == Some(data_dir)
                        && info.version_id.is_none_or(|id| id.is_nil())
                        && info.erasure.parity_blocks == selected.erasure.parity_blocks
                }));
                let part_path = |dir: &TempDir| dir.path().join(bucket).join(object).join(data_dir.to_string()).join("part.1");
                let surviving_shard = tokio::fs::read(part_path(&dirs[missing_count]))
                    .await
                    .expect("read the shard that must survive");
                for dir in dirs.iter().take(missing_count) {
                    tokio::fs::remove_file(part_path(dir)).await.expect("remove one exact shard");
                }
                for (index, (disk, info)) in disks.iter().zip(&infos).enumerate() {
                    let checked = disk.check_parts(bucket, object, info).await.expect("check fixture shards");
                    let expected = if index < missing_count {
                        CHECK_PART_FILE_NOT_FOUND
                    } else {
                        CHECK_PART_SUCCESS
                    };
                    assert_eq!(checked.results, vec![expected], "disk {index} has the intended shard state");
                }
                let visible = set
                    .get_object_info(
                        bucket,
                        object,
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("metadata quorum remains visible before heal");
                assert_eq!(visible.data_dir, Some(data_dir));

                let (result, error) = <SetDisks as crate::storage_api_contracts::heal::HealOperations>::heal_object(
                    set.as_ref(),
                    bucket,
                    object,
                    "",
                    &HealOpts {
                        remove,
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await
                .expect("heal returns an object result");
                if remove {
                    assert!(error.is_none(), "explicit dangling cleanup must succeed: {error:?}");
                    for dir in &dirs {
                        assert!(
                            tokio::fs::metadata(dir.path().join(bucket))
                                .await
                                .expect("bucket remains readable")
                                .is_dir()
                        );
                        assert!(
                            matches!(
                                tokio::fs::symlink_metadata(dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE)).await,
                                Err(error) if error.kind() == std::io::ErrorKind::NotFound
                            ),
                            "explicit removal must remove the metadata replica"
                        );
                    }
                    assert!(
                        matches!(tokio::fs::symlink_metadata(part_path(&dirs[missing_count])).await,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound),
                        "explicit removal must remove the surviving shard"
                    );
                    assert_eq!(result.after.drives.len(), disks.len());
                    assert!(
                        result
                            .after
                            .drives
                            .iter()
                            .all(|drive| drive.state == DriveState::Missing.to_string())
                    );
                } else {
                    for (index, (dir, before)) in dirs.iter().zip(&metadata).enumerate() {
                        let after = tokio::fs::read(dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE))
                            .await
                            .expect("remove=false must keep each metadata replica");
                        assert_eq!(&after, before, "remove=false changed metadata on disk {index}");
                    }
                    assert_eq!(
                        tokio::fs::read(part_path(&dirs[missing_count]))
                            .await
                            .expect("remove=false must keep the surviving shard"),
                        surviving_shard
                    );
                    assert!(matches!(&error, Some(Error::ErasureReadQuorum)), "unexpected heal result: {error:?}");
                    assert_eq!(result.before.drives.len(), disks.len());
                    assert_eq!(
                        result
                            .before
                            .drives
                            .iter()
                            .filter(|drive| drive.state == DriveState::Missing.to_string())
                            .count(),
                        missing_count
                    );
                    let mut absence = None;
                    let (_, proof_error) = set
                        .heal_object_with_absence(
                            bucket,
                            object,
                            "",
                            &HealOpts {
                                scan_mode: HealScanMode::Deep,
                                ..Default::default()
                            },
                            &mut absence,
                            AbsenceProofRequest {
                                retirement: None,
                                allow_unversioned: true,
                            },
                        )
                        .await
                        .expect("repeat through the absence-proof path");
                    assert!(matches!(proof_error, Some(Error::ErasureReadQuorum)));
                    assert!(absence.is_none(), "surviving shards cannot produce an absence proof");
                }
            }))
            .catch_unwind()
            .await;
            drop(set);
            drop(disks);
            if !matches!(&outcome, Ok(Ok(()))) {
                let retained: Vec<_> = dirs.into_iter().map(TempDir::keep).collect();
                eprintln!("unrecoverable-shard fixture retained: {retained:?}");
                match outcome {
                    Ok(Err(_)) => panic!("unrecoverable-shard fixture timed out"),
                    Err(panic) => std::panic::resume_unwind(panic),
                    Ok(Ok(())) => unreachable!(),
                }
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn heal_without_remove_preserves_explicit_version_metadata_during_dry_run() {
        use crate::object_api::{ShardIntegrityWriteMode, WriteCompletion};
        use futures::FutureExt as _;
        use std::panic::AssertUnwindSafe;
        use std::time::Duration;

        let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let outcome = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(45), async {
            let bucket = "heal-dry-run-versioned";
            let object = "object.bin";
            set.make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("create fixture bucket");
            let original = set
                .put_object(
                    bucket,
                    object,
                    &mut PutObjReader::from_vec(vec![0x67; 1024 * 1024]),
                    &ObjectOptions {
                        versioned: true,
                        shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
                        write_completion: WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("write a real version on all four disks");
            let version = original.version_id.expect("explicit version");
            assert!(!version.is_nil());
            let generation = original.data_dir.expect("external data generation");
            let version_string = version.to_string();
            let metadata_path = |dir: &TempDir| dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE);
            let shard_path = |dir: &TempDir| {
                dir.path()
                    .join(bucket)
                    .join(object)
                    .join(generation.to_string())
                    .join("part.1")
            };
            let mut shards = Vec::new();
            for (dir, disk) in dirs.iter().zip(&disks) {
                let info = disk
                    .read_version("", bucket, object, &version_string, &ReadOptions::default())
                    .await
                    .expect("initial metadata on each disk");
                assert_eq!(info.version_id, Some(version));
                assert_eq!(info.data_dir, Some(generation));
                assert!(info.parts.iter().all(|part| part.integrity.is_none()));
                assert!(
                    rustfs_filemeta::shard_integrity::descriptor_from_metadata(&info.metadata)
                        .expect("metadata protection descriptor")
                        .is_none()
                );
                shards.push(tokio::fs::read(shard_path(dir)).await.expect("save original shard"));
            }
            let surviving_metadata = tokio::fs::read(metadata_path(&dirs[0]))
                .await
                .expect("save surviving metadata");
            for dir in dirs.iter().skip(1) {
                tokio::fs::remove_file(metadata_path(dir))
                    .await
                    .expect("remove one metadata replica");
            }
            for disk in disks.iter().skip(1) {
                assert!(matches!(
                    disk.read_version("", bucket, object, &version_string, &ReadOptions::default())
                        .await,
                    Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)
                ));
            }

            let mut absence = None;
            let (_, error) = set
                .heal_object_with_absence(
                    bucket,
                    object,
                    &version_string,
                    &HealOpts {
                        dry_run: true,
                        ..Default::default()
                    },
                    &mut absence,
                    AbsenceProofRequest {
                        retirement: None,
                        allow_unversioned: false,
                    },
                )
                .await
                .expect("dry-run reports the metadata quorum failure");
            assert!(matches!(error, Some(Error::ErasureReadQuorum)), "partial metadata must not look absent");
            assert!(absence.is_none());
            assert_eq!(
                tokio::fs::read(metadata_path(&dirs[0])).await.expect("surviving metadata"),
                surviving_metadata
            );
            for (index, dir) in dirs.iter().enumerate() {
                if index != 0 {
                    assert!(matches!(
                        tokio::fs::symlink_metadata(metadata_path(dir)).await,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound
                    ));
                }
                assert_eq!(tokio::fs::read(shard_path(dir)).await.expect("original shard"), shards[index]);
            }

            let (_, error) = set
                .heal_object_with_absence(
                    bucket,
                    object,
                    &version_string,
                    &HealOpts::default(),
                    &mut absence,
                    AbsenceProofRequest {
                        retirement: None,
                        allow_unversioned: false,
                    },
                )
                .await
                .expect("normal heal attempts metadata regeneration");
            assert!(error.is_none(), "normal heal must repair the same layout: {error:?}");
            assert!(absence.is_none());
            for (index, (dir, disk)) in dirs.iter().zip(&disks).enumerate() {
                let info = disk
                    .read_version("", bucket, object, &version_string, &ReadOptions::default())
                    .await
                    .expect("normal heal restored metadata");
                assert_eq!(info.version_id, Some(version));
                assert_eq!(info.data_dir, Some(generation));
                assert_eq!(tokio::fs::read(shard_path(dir)).await.expect("original shard"), shards[index]);
            }
        }))
        .catch_unwind()
        .await;
        drop(set);
        drop(disks);
        if !matches!(&outcome, Ok(Ok(()))) {
            let retained: Vec<_> = dirs.into_iter().map(TempDir::keep).collect();
            eprintln!("dry-run versioned fixture retained: {retained:?}");
            match outcome {
                Ok(Err(_)) => panic!("dry-run versioned fixture timed out"),
                Err(panic) => std::panic::resume_unwind(panic),
                Ok(Ok(())) => unreachable!(),
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn heal_without_remove_preserves_dangling_inline_metadata_and_emits_no_absence_proof() {
        use futures::FutureExt as _;
        use std::panic::AssertUnwindSafe;
        use std::time::Duration;

        temp_env::async_with_vars([("RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS", Some("0"))], async {
            for remove in [true, false] {
                let bucket = if remove {
                    "inline-dangling-remove"
                } else {
                    "inline-dangling-preserve"
                };
                let object = "stale.txt";
                let (dirs, set, disks) =
                    dangling_inline_test_fixture(bucket, object, OffsetDateTime::now_utc() - time::Duration::hours(2)).await;
                let outcome = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(45), async {
                    let metadata = dirs[0].path().join(bucket).join(object).join(STORAGE_FORMAT_FILE);
                    let before = tokio::fs::read(&metadata).await.expect("read surviving inline metadata");
                    let mut absence = None;
                    let (_, error) = set
                        .heal_object_with_absence(
                            bucket,
                            object,
                            "",
                            &HealOpts {
                                no_lock: true,
                                remove,
                                ..Default::default()
                            },
                            &mut absence,
                            AbsenceProofRequest {
                                retirement: None,
                                allow_unversioned: true,
                            },
                        )
                        .await
                        .expect("dangling inline heal returns an object result");
                    if remove {
                        assert!(error.is_none(), "explicit cleanup must succeed: {error:?}");
                        assert!(
                            absence.as_ref().is_some_and(|proof| proof.removed),
                            "successful cleanup must prove removal"
                        );
                        assert!(matches!(
                            tokio::fs::symlink_metadata(metadata).await,
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound
                        ));
                        let (_, missing_error) = set
                            .heal_object_with_absence(
                                bucket,
                                object,
                                "",
                                &HealOpts {
                                    no_lock: true,
                                    ..Default::default()
                                },
                                &mut absence,
                                AbsenceProofRequest {
                                    retirement: None,
                                    allow_unversioned: true,
                                },
                            )
                            .await
                            .expect("fully absent object returns a result");
                        assert!(matches!(missing_error, Some(Error::FileNotFound)));
                        assert!(absence.is_some_and(|proof| !proof.removed));
                    } else {
                        assert!(matches!(error, Some(Error::ErasureReadQuorum)), "partial metadata must not look absent");
                        assert!(absence.is_none(), "a surviving candidate cannot prove authoritative absence");
                        assert_eq!(tokio::fs::read(metadata).await.expect("metadata must remain"), before);
                    }
                }))
                .catch_unwind()
                .await;
                drop(set);
                drop(disks);
                if !matches!(&outcome, Ok(Ok(()))) {
                    let retained: Vec<_> = dirs.into_iter().map(TempDir::keep).collect();
                    eprintln!("dangling-inline fixture retained: {retained:?}");
                    match outcome {
                        Ok(Err(_)) => panic!("dangling-inline fixture timed out"),
                        Err(panic) => std::panic::resume_unwind(panic),
                        Ok(Ok(())) => unreachable!(),
                    }
                }
            }
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn heal_reports_success_after_dangling_inline_cleanup() {
        temp_env::async_with_vars([("RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS", Some("0"))], async {
            let bucket = "bucket-dangling-inline-cleanup";
            let object = "stale.txt";
            let (temp_dirs, set, _disks) =
                dangling_inline_test_fixture(bucket, object, OffsetDateTime::now_utc() - time::Duration::hours(2)).await;

            let (_, error) = set
                .heal_object(
                    bucket,
                    object,
                    "",
                    &HealOpts {
                        no_lock: true,
                        remove: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("dangling cleanup should complete");

            assert!(error.is_none(), "successful dangling cleanup must not be reported as FileNotFound");
            assert!(
                temp_dirs.iter().all(|dir| !dir.path().join(bucket).join(object).exists()),
                "successful dangling cleanup must remove the stale object from every disk"
            );
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn heal_preserves_recent_dangling_inline_metadata_as_retryable() {
        temp_env::async_with_vars([("RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS", Some("3600"))], async {
            let bucket = "bucket-dangling-inline-grace";
            let object = "recent.txt";
            let (temp_dirs, set, _disks) = dangling_inline_test_fixture(bucket, object, OffsetDateTime::now_utc()).await;

            let (_, error) = set
                .heal_object(
                    bucket,
                    object,
                    "",
                    &HealOpts {
                        no_lock: true,
                        remove: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("grace-protected dangling metadata should return a typed heal result");

            let error = error.expect("grace-protected dangling metadata should be reported as deferred");
            assert!(
                error.is_dangling_delete_grace(),
                "grace-protected dangling metadata should keep a typed deferred-cleanup marker: {error}"
            );
            let message = error.to_string();
            assert!(
                message.contains("dangling object deletion deferred by heal grace window"),
                "grace-protected dangling metadata should explain that cleanup was deferred: {message}"
            );
            assert!(
                message.contains("retry_after_secs="),
                "grace-protected dangling metadata should include retry timing: {message}"
            );
            assert!(
                temp_dirs[0]
                    .path()
                    .join(bucket)
                    .join(object)
                    .join(STORAGE_FORMAT_FILE)
                    .is_file(),
                "grace-protected metadata must remain for a later heal pass"
            );
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn dry_run_never_deletes_dangling_inline_metadata() {
        temp_env::async_with_vars([("RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS", Some("0"))], async {
            let bucket = "bucket-dangling-inline-dry-run";
            let object = "stale.txt";
            let (temp_dirs, set, _disks) =
                dangling_inline_test_fixture(bucket, object, OffsetDateTime::now_utc() - time::Duration::hours(2)).await;

            let (_, error) = set
                .heal_object(
                    bucket,
                    object,
                    "",
                    &HealOpts {
                        dry_run: true,
                        no_lock: true,
                        remove: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("dry-run should report the dangling candidate without mutation");

            assert_eq!(error, Some(DiskError::ErasureReadQuorum));
            assert!(
                temp_dirs[0]
                    .path()
                    .join(bucket)
                    .join(object)
                    .join(STORAGE_FORMAT_FILE)
                    .is_file(),
                "dry-run must preserve dangling metadata even when grace is disabled"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn heal_meta_quorum_failure_preserves_candidate_when_required_shard_disk_is_offline() {
        let bucket = "bucket-delete-guard-offline";
        let object = "object.bin";
        let data_dir = Uuid::parse_str("44444444-4444-4444-4444-444444444444").expect("data dir should parse");
        let (temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(data_dir, 2)]).await;
        let metadata = meta_regen_test_fileinfo(object, data_dir, 4, 0);
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &metadata).await;
        set.disks.write().await[1] = None;

        let mut absence = None;
        let (_, error) = set
            .heal_object_with_absence(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    ..Default::default()
                },
                &mut absence,
                AbsenceProofRequest {
                    retirement: None,
                    allow_unversioned: true,
                },
            )
            .await
            .expect("offline shard state should be reported without removal");
        assert!(matches!(error, Some(Error::ErasureReadQuorum)));
        assert!(absence.is_none(), "offline members cannot prove absence");

        let (_, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    remove: true,
                    ..Default::default()
                },
            )
            .await
            .expect("offline shard state should be reported without deletion");
        assert_eq!(err, Some(DiskError::ErasureReadQuorum));
        let surviving = disks[0]
            .as_ref()
            .expect("first test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("offline uncertainty must preserve the surviving metadata");
        assert_eq!(surviving.data_dir, Some(data_dir));
        assert!(
            temp_dirs[0]
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join("part.1")
                .is_file(),
            "offline uncertainty must preserve the last online shard"
        );
    }

    #[tokio::test]
    async fn heal_meta_quorum_failure_preserves_candidate_when_part_probe_times_out() {
        let bucket = "bucket-delete-guard-timeout";
        let object = "object.bin";
        let data_dir = Uuid::parse_str("55555555-5555-5555-5555-555555555555").expect("data dir should parse");
        let (temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(data_dir, 2)]).await;
        let metadata = meta_regen_test_fileinfo(object, data_dir, 5, 0);
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &metadata).await;
        let _failure = DanglingCheckPartsFailure::install(bucket, object, 1, DiskError::Timeout);

        let (_, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    remove: true,
                    ..Default::default()
                },
            )
            .await
            .expect("part probe timeout should be reported without deletion");
        assert_eq!(err, Some(DiskError::ErasureReadQuorum));
        let surviving = disks[0]
            .as_ref()
            .expect("first test disk should be online")
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("probe uncertainty must preserve the surviving metadata");
        assert_eq!(surviving.data_dir, Some(data_dir));
        assert!(
            temp_dirs[0]
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join("part.1")
                .is_file(),
            "probe uncertainty must preserve the last confirmed shard"
        );
    }

    #[tokio::test]
    async fn dangling_delete_guard_ignores_set_incompatible_geometry() {
        let bucket = "bucket-delete-guard-short-geometry";
        let object = "object.bin";
        let data_dir = Uuid::parse_str("abababab-abab-abab-abab-abababababab").expect("data dir should parse");
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[(data_dir, 1)]).await;
        let mut candidate = FileInfo::new(object, 1, 0);
        candidate.data_dir = Some(data_dir);
        candidate.mod_time = Some(OffsetDateTime::from_unix_timestamp(18).expect("timestamp should parse"));
        candidate.size = 1;
        candidate.parts = vec![ObjectPartInfo {
            number: 1,
            size: 1,
            actual_size: 1,
            ..Default::default()
        }];
        candidate.erasure.index = candidate.erasure.distribution[0];
        seed_meta_regen_test_metadata(&disks, 0, bucket, object, &candidate).await;
        let metadata = vec![candidate, FileInfo::default(), FileInfo::default(), FileInfo::default()];
        let errs = vec![
            None,
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
        ];

        assert!(
            set.dangling_delete_safety(bucket, object, &metadata, &errs, &disks)
                .await
                .expect("set-incompatible geometry should be classified")
                == DanglingDeleteSafety::NoRecoverableCandidate
        );
    }

    #[tokio::test]
    async fn dangling_delete_guard_preserves_delete_marker_and_remote_metadata() {
        let bucket = "bucket-delete-guard-nonlocal";
        let object = "object.bin";
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[]).await;
        let marker = FileInfo {
            name: object.to_string(),
            version_id: Some(Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").expect("version id should parse")),
            deleted: true,
            mod_time: Some(OffsetDateTime::from_unix_timestamp(14).expect("marker timestamp should parse")),
            ..Default::default()
        };
        let remote_dir = Uuid::parse_str("89898989-8989-8989-8989-898989898989").expect("remote data dir should parse");
        let mut remote = meta_regen_test_fileinfo(object, remote_dir, 15, 1);
        remote.transition_status = TRANSITION_COMPLETE.to_string();
        remote.transition_tier = "WARM".to_string();
        remote.transitioned_objname = "remote/object.bin".to_string();

        for metadata in [marker, remote] {
            let candidates = vec![metadata, FileInfo::default(), FileInfo::default(), FileInfo::default()];
            let errs = vec![
                None,
                Some(DiskError::FileNotFound),
                Some(DiskError::FileNotFound),
                Some(DiskError::FileNotFound),
            ];
            assert_eq!(
                set.dangling_delete_safety(bucket, object, &candidates, &errs, &disks)
                    .await
                    .expect("non-local metadata should be classified"),
                DanglingDeleteSafety::UnsafeToDelete
            );
        }
    }

    #[tokio::test]
    async fn dangling_delete_guard_preserves_metadata_read_uncertainty() {
        let bucket = "bucket-delete-guard-read-error";
        let object = "object.bin";
        let (_temp_dirs, set, disks) = meta_regen_test_set(bucket, object, &[]).await;
        let metadata = vec![FileInfo::default(); disks.len()];

        for read_error in [DiskError::Timeout, DiskError::DiskAccessDenied, DiskError::DiskNotFound] {
            let mut errs = vec![Some(DiskError::FileNotFound); disks.len()];
            errs[0] = Some(read_error);
            assert_eq!(
                set.dangling_delete_safety(bucket, object, &metadata, &errs, &disks)
                    .await
                    .expect("metadata read uncertainty should be classified"),
                DanglingDeleteSafety::UnsafeToDelete
            );
        }
    }

    #[tokio::test]
    async fn heal_no_parity_bitrot_reports_unrecoverable_integrity_failure() {
        let (dir, set) = formatted_single_disk_no_parity_set().await;
        let bucket = "bucket-no-parity-bitrot";
        let object = "bad-object.bin";
        let payload = (0..(BLOCK_SIZE_V2 + 17)).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set.make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(payload);
        set.put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("object should be written");

        let snapshot = set
            .get_object_fileinfo(bucket, object, &opts, true, false)
            .await
            .expect("object metadata should resolve");
        let fi = snapshot.fi();
        assert_eq!(fi.erasure.parity_blocks, 0);
        let data_dir = fi.data_dir.expect("non-inline object should have a data directory");
        let part_path = dir.path().join(bucket).join(object).join(data_dir.to_string()).join("part.1");
        let mut part = tokio::fs::read(&part_path).await.expect("part should be readable");
        part[0] ^= 0xff;
        tokio::fs::write(&part_path, part)
            .await
            .expect("part corruption should be written");

        let (result, err) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("heal should report the unrecoverable object without panicking");

        assert_eq!(err, Some(DiskError::FileCorrupt));
        assert_eq!(result.bucket, bucket);
        assert_eq!(result.object, object);
        assert_eq!(result.data_blocks, 1);
        assert_eq!(result.parity_blocks, 0);
        assert_eq!(result.before.drives[0].state, DriveState::Corrupt.to_string());
        assert!(result.detail.contains("no-parity object is unrecoverable"));
        assert!(result.detail.contains("part 1"));
        assert!(result.detail.contains("bitrot_failure=true"));
    }

    // HS-12 (backlog#1874): a versioned DELETE racing an object heal must never
    // resurrect the deleted version. The heal has real reconstruction work (a
    // shard of the doomed version is removed), so both sides touch the same
    // (bucket, object, data_dir); whichever order the ns write lock serializes
    // them in, the committed delete must win.
    #[tokio::test]
    #[serial_test::serial]
    async fn heal_racing_version_delete_never_resurrects_the_deleted_version() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "heal-race-delete-no-resurrect";
        let object = "object.bin";
        set.make_bucket(
            bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("versioned bucket should be created");

        let mut first_reader = PutObjReader::from_vec(vec![0x11; 1024 * 1024]);
        let first_info = set
            .put_object(
                bucket,
                object,
                &mut first_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("first version should be written");
        let first_version = first_info
            .version_id
            .expect("versioned put should return the first version id")
            .to_string();

        let mut second_reader = PutObjReader::from_vec(vec![0x22; 1024 * 1024]);
        let second_info = set
            .put_object(
                bucket,
                object,
                &mut second_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("second version should be written");
        let second_version = second_info
            .version_id
            .expect("versioned put should return the second version id")
            .to_string();

        // Damage one shard of the doomed version so the racing heal performs an
        // actual reconstruction over its data dir instead of an early exit.
        let doomed_source = disks[0]
            .read_version("", bucket, object, &first_version, &ReadOptions::default())
            .await
            .expect("doomed version metadata should be readable");
        let doomed_data_dir = doomed_source
            .data_dir
            .expect("non-inline version should have a data directory");
        tokio::fs::remove_file(
            temp_dirs[1]
                .path()
                .join(bucket)
                .join(object)
                .join(doomed_data_dir.to_string())
                .join("part.1"),
        )
        .await
        .expect("shard damage should be injected before the race");

        let delete_set = set.clone();
        let (delete_res, heal_res) = tokio::join!(
            async {
                delete_set
                    .delete_object(
                        bucket,
                        object,
                        ObjectOptions {
                            versioned: true,
                            version_id: Some(first_version.clone()),
                            object_lock_config_snapshot: Some(Arc::new(crate::set_disk::ObjectLockConfigSnapshot::new(
                                crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
                            ))),
                            ..Default::default()
                        },
                    )
                    .await
            },
            async {
                set.heal_object(
                    bucket,
                    object,
                    "",
                    &HealOpts {
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await
            },
        );
        delete_res.expect("version delete must succeed under lock serialization");
        // The heal may legitimately report a transient failure when the version
        // it was rebuilding disappears mid-flight; only the end state matters.
        drop(heal_res);

        let resurrected = set
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(first_version.clone()),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(&resurrected, Err(Error::VersionNotFound(b, o, v))
                if b == bucket && o == object && v == &first_version),
            "a racing heal must not resurrect the deleted version: {resurrected:?}"
        );

        let survivor = set
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(second_version.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("surviving version must remain readable after the race");
        assert_eq!(survivor.size, 1024 * 1024, "survivor size must be intact");
    }

    #[derive(Clone, Copy)]
    enum VersionCommitMutation {
        Append,
        DeleteHistorical,
        DeleteCurrent,
    }

    struct VersionCommitFixture {
        version: Uuid,
        data_dir: Uuid,
        body: Vec<u8>,
        parts: Vec<Vec<u8>>,
    }

    async fn explicit_version_commit_fence(mutation: VersionCommitMutation) {
        use crate::object_api::{ShardIntegrityWriteMode, WriteCompletion};
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        for (disk_count, parity) in [(4, 2), (16, 4)] {
            let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(disk_count, 0, parity).await;
            let label = match mutation {
                VersionCommitMutation::Append => "append",
                VersionCommitMutation::DeleteHistorical => "delete-historical",
                VersionCommitMutation::DeleteCurrent => "delete-current",
            };
            let prefix = if matches!(mutation, VersionCommitMutation::Append) {
                "native_g02"
            } else {
                "native_g03"
            };
            let bucket = format!("version-commit-{label}-{disk_count}");
            let object = "object.bin";
            set.make_bucket(
                &bucket,
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create isolated versioned bucket");
            let options = ObjectOptions {
                versioned: true,
                write_completion: WriteCompletion::TailDrained,
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                ..Default::default()
            };
            let mut versions = Vec::new();
            for byte in [0x11, 0x22, 0x33] {
                let body = vec![byte; 1024 * 1024 + 123];
                let info = set
                    .put_object(&bucket, object, &mut PutObjReader::from_vec(body.clone()), &options)
                    .await
                    .expect("ACK original version after its full rename tail");
                let version = info.version_id.expect("independent version id");
                let meta = disks[0]
                    .read_version("", &bucket, object, &version.to_string(), &ReadOptions::default())
                    .await
                    .expect("read original version metadata");
                assert!(meta.data.is_none(), "native version fence must reconstruct external data");
                assert_eq!(meta.erasure.data_blocks, disk_count - parity);
                assert_eq!(meta.erasure.parity_blocks, parity);
                let data_dir = meta.data_dir.expect("actual external generation");
                let mut parts = Vec::new();
                for dir in &dirs {
                    parts.push(
                        tokio::fs::read(
                            dir.path()
                                .join(&bucket)
                                .join(object)
                                .join(data_dir.to_string())
                                .join("part.1"),
                        )
                        .await
                        .expect("every original encoded shard after ACK"),
                    );
                }
                versions.push(VersionCommitFixture {
                    version,
                    data_dir,
                    body,
                    parts,
                });
            }
            let selected = if matches!(mutation, VersionCommitMutation::DeleteCurrent) {
                2
            } else {
                0
            };
            let selected_version = versions[selected].version;
            let selected_dir = versions[selected].data_dir;
            let missing = dirs[0]
                .path()
                .join(&bucket)
                .join(object)
                .join(selected_dir.to_string())
                .join("part.1");
            tokio::fs::remove_file(&missing)
                .await
                .expect("remove exact selected-version shard");
            let (_pause, arrived, release) = ReadRepairPauseScope::install(&bucket, object);
            let heal_set = set.clone();
            let heal_bucket = bucket.clone();
            let heal = tokio::spawn(async move {
                heal_set
                    .heal_object_with_retirement(
                        &heal_bucket,
                        object,
                        &selected_version.to_string(),
                        &HealOpts {
                            scan_mode: HealScanMode::Deep,
                            read_repair: true,
                            ..Default::default()
                        },
                        &mut None,
                        AbsenceProofRequest {
                            retirement: None,
                            allow_unversioned: false,
                        },
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(30), arrived.notified())
                .await
                .expect("actual selected-version reconstruction reaches final commit fence");
            assert!(!heal.is_finished() && !missing.exists());
            println!("{prefix}: geometry={disk_count}/{parity} mutation={label} reconstruction_paused_before_final_rename");

            if matches!(mutation, VersionCommitMutation::Append) {
                let body = vec![0x44; 1024 * 1024 + 123];
                let info = set
                    .put_object(&bucket, object, &mut PutObjReader::from_vec(body.clone()), &options)
                    .await
                    .expect("new version ACK while historical reconstruction is paused");
                let version = info.version_id.expect("new acknowledged version id");
                assert!(versions.iter().all(|v| v.version != version));
                let meta = disks[0]
                    .read_version("", &bucket, object, &version.to_string(), &ReadOptions::default())
                    .await
                    .expect("new physical version metadata");
                let data_dir = meta.data_dir.expect("new external generation");
                let mut parts = Vec::new();
                for dir in &dirs {
                    parts.push(
                        tokio::fs::read(
                            dir.path()
                                .join(&bucket)
                                .join(object)
                                .join(data_dir.to_string())
                                .join("part.1"),
                        )
                        .await
                        .expect("new version ACK has every physical shard"),
                    );
                }
                versions.push(VersionCommitFixture {
                    version,
                    data_dir,
                    body,
                    parts,
                });
            } else {
                set.delete_object(
                    &bucket,
                    object,
                    ObjectOptions {
                        versioned: true,
                        version_id: Some(selected_version.to_string()),
                        object_lock_config_snapshot: Some(Arc::new(crate::set_disk::ObjectLockConfigSnapshot::new(
                            crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
                        ))),
                        ..Default::default()
                    },
                )
                .await
                .expect("exact version delete ACK before releasing stale reconstruction");
            }
            // Commit pause has dropped the Heal read guard. Acquire this key's
            // write fence only after the foreground ACK, draining its physical
            // cleanup before snapshots; release it before resuming the Heal.
            let namespace = set.new_ns_lock(&bucket, object).await.expect("foreground drain fence");
            let drained = namespace
                .get_write_lock(Duration::from_secs(30))
                .await
                .expect("foreground physical tail drains");
            let mut committed_metadata = Vec::new();
            for dir in &dirs {
                committed_metadata.push(
                    tokio::fs::read(dir.path().join(&bucket).join(object).join(STORAGE_FORMAT_FILE))
                        .await
                        .expect("foreground committed metadata on every drive"),
                );
                if !matches!(mutation, VersionCommitMutation::Append) {
                    assert!(!dir.path().join(&bucket).join(object).join(selected_dir.to_string()).exists());
                }
            }
            drop(drained);
            assert!(!heal.is_finished());
            println!(
                "{prefix}: geometry={disk_count}/{parity} mutation={label} foreground_ack_and_physical_tail_verified_before_release"
            );
            release.notify_one();
            let outcome = tokio::time::timeout(Duration::from_secs(30), heal)
                .await
                .expect("Heal leaves its released final fence")
                .expect("Heal task joins");
            if matches!(mutation, VersionCommitMutation::Append) {
                assert!(outcome.expect("historical version remains healable").1.is_none());
            } else {
                assert!(
                    matches!(outcome, Err(Error::VersionNotFound(..))),
                    "deleted version must remain typed absent: {outcome:?}"
                );
            }
            for (slot, dir) in dirs.iter().enumerate() {
                if slot != 0 || !matches!(mutation, VersionCommitMutation::Append) {
                    assert_eq!(
                        tokio::fs::read(dir.path().join(&bucket).join(object).join(STORAGE_FORMAT_FILE))
                            .await
                            .expect("metadata after Heal"),
                        committed_metadata[slot],
                        "Heal must preserve every committed successor metadata byte outside its repaired target"
                    );
                }
                if !matches!(mutation, VersionCommitMutation::Append) {
                    assert!(!dir.path().join(&bucket).join(object).join(selected_dir.to_string()).exists());
                }
                for version in &versions {
                    if !matches!(mutation, VersionCommitMutation::Append) && version.version == selected_version {
                        assert!(matches!(
                            disks[slot]
                                .read_version("", &bucket, object, &version.version.to_string(), &ReadOptions::default())
                                .await,
                            Err(DiskError::FileVersionNotFound)
                        ));
                        continue;
                    }
                    let meta = disks[slot]
                        .read_version("", &bucket, object, &version.version.to_string(), &ReadOptions::default())
                        .await
                        .expect("every acknowledged surviving version metadata");
                    assert_eq!(meta.data_dir, Some(version.data_dir));
                    assert_eq!(meta.size, version.body.len() as i64);
                    assert_eq!(
                        tokio::fs::read(
                            dir.path()
                                .join(&bucket)
                                .join(object)
                                .join(version.data_dir.to_string())
                                .join("part.1")
                        )
                        .await
                        .expect("physical shard before any GET"),
                        version.parts[slot],
                        "every surviving version must retain its exact original encoded shard"
                    );
                }
            }
            for version in &versions {
                if !matches!(mutation, VersionCommitMutation::Append) && version.version == selected_version {
                    continue;
                }
                let mut reader = set
                    .get_object_reader(
                        &bucket,
                        object,
                        None,
                        Default::default(),
                        &ObjectOptions {
                            versioned: true,
                            version_id: Some(version.version.to_string()),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("read exact surviving version after physical proof");
                let mut actual = Vec::new();
                reader
                    .read_to_end(&mut actual)
                    .await
                    .expect("read complete surviving version");
                assert_eq!(actual, version.body);
            }
            let current = if matches!(mutation, VersionCommitMutation::DeleteCurrent) {
                &versions[1]
            } else {
                versions.last().unwrap()
            };
            let mut reader = set
                .get_object_reader(
                    &bucket,
                    object,
                    None,
                    Default::default(),
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("read exact last surviving current generation");
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.expect("read full current generation");
            assert_eq!(actual, current.body);
            println!(
                "{prefix}: geometry={disk_count}/{parity} mutation={label} exact_version_outcome_physical_generations_and_full_bodies_verified"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn native_explicit_version_commit_preserves_acknowledged_new_version() {
        explicit_version_commit_fence(VersionCommitMutation::Append).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn native_explicit_version_commit_refuses_deleted_historical_and_current_versions() {
        explicit_version_commit_fence(VersionCommitMutation::DeleteHistorical).await;
        explicit_version_commit_fence(VersionCommitMutation::DeleteCurrent).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn native_marker_and_batch_delete_fence_preserves_acknowledged_version_set() {
        use crate::object_api::{ShardIntegrityWriteMode, WriteCompletion};
        use crate::storage_api_contracts::object::ObjectToDelete;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        for (disk_count, parity) in [(4, 2), (16, 4)] {
            for mutation in ["create-marker", "delete-marker", "batch-version-delete", "unversioned-delete"] {
                let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(disk_count, 0, parity).await;
                let bucket = format!("marker-fence-{mutation}-{disk_count}");
                let object = "object.bin";
                let versioned = mutation != "unversioned-delete";
                set.make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: versioned,
                        ..Default::default()
                    },
                )
                .await
                .expect("create isolated marker fence bucket");
                let options = ObjectOptions {
                    versioned,
                    write_completion: WriteCompletion::TailDrained,
                    shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                    object_lock_config_snapshot: Some(Arc::new(crate::set_disk::ObjectLockConfigSnapshot::new(
                        crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
                    ))),
                    ..Default::default()
                };
                let mut versions = Vec::new();
                for byte in if versioned { vec![0x51, 0x62, 0x73] } else { vec![0x51] } {
                    let body = vec![byte; 1024 * 1024 + 123];
                    let info = set
                        .put_object(&bucket, object, &mut PutObjReader::from_vec(body.clone()), &options)
                        .await
                        .expect("ACK complete original external version");
                    let version = info.version_id.unwrap_or(Uuid::nil());
                    assert_eq!(!version.is_nil(), versioned);
                    let meta = disks[0]
                        .read_version("", &bucket, object, &version.to_string(), &ReadOptions::default())
                        .await
                        .expect("read independent acknowledged metadata");
                    assert!(meta.data.is_none());
                    let data_dir = meta.data_dir.expect("external data generation");
                    let mut parts = Vec::new();
                    for dir in &dirs {
                        parts.push(
                            tokio::fs::read(
                                dir.path()
                                    .join(&bucket)
                                    .join(object)
                                    .join(data_dir.to_string())
                                    .join("part.1"),
                            )
                            .await
                            .expect("full encoded original shard after ACK"),
                        );
                    }
                    versions.push(VersionCommitFixture {
                        version,
                        data_dir,
                        body,
                        parts,
                    });
                }
                let mut marker = None;
                if mutation == "delete-marker" {
                    marker = Some(
                        set.delete_object(&bucket, object, options.clone())
                            .await
                            .expect("ACK original current marker")
                            .version_id
                            .expect("real marker identity"),
                    );
                }
                let selected = versions[0].version;
                let missing = dirs[0]
                    .path()
                    .join(&bucket)
                    .join(object)
                    .join(versions[0].data_dir.to_string())
                    .join("part.1");
                tokio::fs::remove_file(&missing)
                    .await
                    .expect("remove selected original data shard");
                let (_pause, arrived, release) = ReadRepairPauseScope::install(&bucket, object);
                let heal_set = set.clone();
                let heal_bucket = bucket.clone();
                let heal = tokio::spawn(async move {
                    heal_set
                        .heal_object(
                            &heal_bucket,
                            object,
                            &selected.to_string(),
                            &HealOpts {
                                scan_mode: HealScanMode::Deep,
                                read_repair: true,
                                ..Default::default()
                            },
                        )
                        .await
                });
                tokio::time::timeout(Duration::from_secs(30), arrived.notified())
                    .await
                    .expect("real original reconstruction pauses before final metadata rename");
                assert!(!heal.is_finished() && !missing.exists());
                println!(
                    "native_g04: geometry={disk_count}/{parity} mutation={mutation} reconstruction_paused_before_final_rename"
                );
                let mut removed = Vec::new();
                match mutation {
                    "create-marker" => {
                        marker = Some(
                            set.delete_object(&bucket, object, options.clone())
                                .await
                                .expect("ACK marker creation before stale Heal resumes")
                                .version_id
                                .expect("new marker identity"),
                        );
                    }
                    "delete-marker" => {
                        let version = marker.expect("fixture marker identity");
                        let ack = set
                            .delete_object(
                                &bucket,
                                object,
                                ObjectOptions {
                                    version_id: Some(version.to_string()),
                                    ..options.clone()
                                },
                            )
                            .await
                            .expect("ACK exact current marker removal before stale Heal resumes");
                        assert_eq!(ack.version_id, Some(version));
                    }
                    "batch-version-delete" => {
                        removed.extend([versions[0].version, versions[1].version]);
                        let requests = removed
                            .iter()
                            .map(|version| ObjectToDelete {
                                object_name: object.to_owned(),
                                version_id: Some(*version),
                                ..Default::default()
                            })
                            .collect();
                        let (deleted, errors) = set.delete_objects(&bucket, requests, options.clone()).await;
                        assert_eq!(deleted.len(), removed.len());
                        assert_eq!(errors.len(), removed.len());
                        for ((ack, error), version) in deleted.iter().zip(&errors).zip(&removed) {
                            assert!(error.is_none(), "every batch item must ACK separately: {error:?}");
                            assert_eq!(ack.object_name, object);
                            assert_eq!(ack.version_id, Some(*version));
                            assert!(!ack.delete_marker);
                        }
                    }
                    "unversioned-delete" => {
                        set.delete_object(&bucket, object, options.clone())
                            .await
                            .expect("ACK unversioned delete");
                        removed.push(selected);
                    }
                    _ => unreachable!(),
                }
                if let Some(marker) = marker {
                    assert!(
                        !marker.is_nil() && versions.iter().all(|version| version.version != marker),
                        "marker ACK must have its own real version identity"
                    );
                }
                // Drain the actual foreground metadata/cleanup tail under this
                // key's write fence before releasing the older reconstruction.
                let namespace = set.new_ns_lock(&bucket, object).await.expect("foreground drain fence");
                let drained = namespace
                    .get_write_lock(Duration::from_secs(30))
                    .await
                    .expect("foreground tail drains");
                let mut committed = Vec::new();
                for dir in &dirs {
                    let metadata = dir.path().join(&bucket).join(object).join(STORAGE_FORMAT_FILE);
                    committed.push(match tokio::fs::read(metadata).await {
                        Ok(bytes) => Some(bytes),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !versioned => None,
                        Err(error) => panic!("foreground metadata cannot be read: {error}"),
                    });
                    for version in &versions {
                        if removed.contains(&version.version) {
                            assert!(
                                !dir.path()
                                    .join(&bucket)
                                    .join(object)
                                    .join(version.data_dir.to_string())
                                    .exists()
                            );
                        }
                    }
                }
                drop(drained);
                assert!(!heal.is_finished());
                println!(
                    "native_g04: geometry={disk_count}/{parity} mutation={mutation} exact_foreground_acks_and_physical_tail_verified_before_release"
                );
                release.notify_one();
                let outcome = tokio::time::timeout(Duration::from_secs(30), heal)
                    .await
                    .expect("stale Heal leaves the released fence")
                    .expect("Heal task joins");
                if removed.contains(&selected) {
                    assert!(
                        matches!(outcome, Err(DiskError::FileVersionNotFound)),
                        "deleted target must remain typed absent: {outcome:?}"
                    );
                } else {
                    assert!(outcome.expect("surviving explicit data version remains healable").1.is_none());
                }
                let mut expected_versions: std::collections::HashSet<_> = versions
                    .iter()
                    .filter(|version| !removed.contains(&version.version))
                    .map(|version| Some(version.version))
                    .collect();
                if mutation == "create-marker" {
                    expected_versions.insert(marker);
                }
                for (slot, dir) in dirs.iter().enumerate() {
                    let metadata = dir.path().join(&bucket).join(object).join(STORAGE_FORMAT_FILE);
                    if !expected_versions.is_empty() {
                        let mut decoded = rustfs_filemeta::FileMeta::default();
                        decoded
                            .unmarshal_msg(&tokio::fs::read(&metadata).await.expect("complete physical metadata"))
                            .expect("decode final version inventory independently");
                        assert_eq!(
                            decoded.versions.len(),
                            expected_versions.len(),
                            "no duplicate or invented physical versions"
                        );
                        let actual: std::collections::HashSet<_> =
                            decoded.versions.iter().map(|version| version.header.version_id).collect();
                        assert_eq!(actual, expected_versions, "exact data and marker version set on every member");
                    }
                    if removed.contains(&selected) || slot != 0 {
                        let after = tokio::fs::read(&metadata).await;
                        if let Some(expected) = &committed[slot] {
                            assert_eq!(&after.expect("committed metadata survives"), expected);
                        } else {
                            assert_eq!(
                                after.expect_err("deleted unversioned metadata must not resurrect").kind(),
                                std::io::ErrorKind::NotFound
                            );
                        }
                    }
                    if let Some(marker) = marker {
                        let observed = disks[slot]
                            .read_version("", &bucket, object, &marker.to_string(), &ReadOptions::default())
                            .await;
                        if mutation == "create-marker" {
                            let info = observed.expect("new marker survives on every disk");
                            assert!(info.deleted);
                            assert_eq!(info.version_id, Some(marker));
                            let current = disks[slot]
                                .read_version("", &bucket, object, "", &ReadOptions::default())
                                .await
                                .expect("read current marker");
                            assert!(current.deleted);
                            assert_eq!(current.version_id, Some(marker));
                        } else {
                            assert!(matches!(observed, Err(DiskError::FileVersionNotFound)));
                        }
                    }
                    for version in &versions {
                        let observed = disks[slot]
                            .read_version("", &bucket, object, &version.version.to_string(), &ReadOptions::default())
                            .await;
                        let part = dir
                            .path()
                            .join(&bucket)
                            .join(object)
                            .join(version.data_dir.to_string())
                            .join("part.1");
                        if removed.contains(&version.version) {
                            assert!(observed.is_err());
                            assert!(!part.parent().unwrap().exists(), "deleted data directory must not resurrect");
                        } else {
                            assert_eq!(observed.expect("surviving exact version").data_dir, Some(version.data_dir));
                            assert_eq!(tokio::fs::read(part).await.expect("surviving part before any GET"), version.parts[slot]);
                        }
                    }
                }
                for version in &versions {
                    if removed.contains(&version.version) {
                        continue;
                    }
                    let mut reader = set
                        .get_object_reader(
                            &bucket,
                            object,
                            None,
                            Default::default(),
                            &ObjectOptions {
                                versioned,
                                version_id: Some(version.version.to_string()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read exact surviving original version");
                    let mut body = Vec::new();
                    reader.read_to_end(&mut body).await.expect("complete surviving body");
                    assert_eq!(body, version.body);
                }
                if mutation == "create-marker" {
                    // The storage info API exposes marker identity to its S3
                    // caller; it must not substitute the previous live data.
                    let current = set
                        .get_object_info(
                            &bucket,
                            object,
                            &ObjectOptions {
                                versioned,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("current storage info retains marker identity");
                    assert!(current.delete_marker && current.is_latest);
                    assert_eq!(current.version_id, marker);
                } else if !versioned {
                    let absent = set
                        .get_object_info(
                            &bucket,
                            object,
                            &ObjectOptions {
                                versioned,
                                ..Default::default()
                            },
                        )
                        .await;
                    assert!(
                        matches!(absent, Err(Error::ObjectNotFound(_, _))),
                        "current deleted object must remain typed absent: {absent:?}"
                    );
                } else {
                    let expected = versions.last().expect("surviving current version");
                    let mut reader = set
                        .get_object_reader(
                            &bucket,
                            object,
                            None,
                            Default::default(),
                            &ObjectOptions {
                                versioned,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("current read after marker or batch deletion");
                    assert_eq!(reader.object_info.version_id, Some(expected.version));
                    let mut body = Vec::new();
                    reader.read_to_end(&mut body).await.expect("full current body");
                    assert_eq!(body, expected.body);
                }
                println!(
                    "native_g04: geometry={disk_count}/{parity} mutation={mutation} marker_set_deleted_directories_surviving_shards_and_full_bodies_verified"
                );
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn native_multipart_continue_complete_abort_fences_stale_heal() {
        use crate::object_api::{ShardIntegrityWriteMode, WriteCompletion};
        use crate::storage_api_contracts::multipart::{CompletePart, MultipartOperations as _};
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        for (disk_count, parity) in [(4, 2), (16, 4)] {
            let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(disk_count, 0, parity).await;
            let bucket = format!("multipart-heal-fence-{disk_count}");
            let object = "object.bin";
            set.make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create isolated MPU fence bucket");
            let options = ObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                ..Default::default()
            };
            let first_size = 5 * 1024 * 1024 + 17;
            let middle_size = 5 * 1024 * 1024 + 93;
            let last_size = 1024 * 1024 + 47;
            let original = set
                .new_multipart_upload(&bucket, object, &options)
                .await
                .expect("original multipart upload");
            let mut original_parts = Vec::new();
            for (number, byte, size) in [(1, 0x21, first_size), (2, 0x32, middle_size), (3, 0x43, last_size)] {
                let part = set
                    .put_object_part(
                        &bucket,
                        object,
                        &original.upload_id,
                        number,
                        &mut PutObjReader::from_vec(vec![byte; size]),
                        &options,
                    )
                    .await
                    .expect("ACK original multipart part");
                assert!(part.etag.as_ref().is_some_and(|value| !value.is_empty()));
                original_parts.push(CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                });
            }
            let original_ack = set
                .clone()
                .complete_multipart_upload(&bucket, object, &original.upload_id, original_parts, &options)
                .await
                .expect("ACK original completed multipart object");
            let old = disks[0]
                .read_version("", &bucket, object, "", &ReadOptions::default())
                .await
                .expect("original external metadata");
            assert_eq!(old.parts.len(), 3);
            let old_dir = old.data_dir.expect("original external generation");
            let mut original_reader = set
                .get_object_reader(&bucket, object, None, Default::default(), &options)
                .await
                .expect("read complete committed multipart baseline before fault");
            let mut original_body = Vec::new();
            original_reader
                .read_to_end(&mut original_body)
                .await
                .expect("full original multipart body");
            assert_eq!(
                original_body,
                [vec![0x21; first_size], vec![0x32; middle_size], vec![0x43; last_size]].concat()
            );
            drop(original_reader);
            let mut old_metadata = Vec::new();
            let mut old_shards = Vec::new();
            for dir in &dirs {
                let mut parts = Vec::new();
                for number in [1, 2, 3] {
                    parts.push(
                        tokio::fs::read(
                            dir.path()
                                .join(&bucket)
                                .join(object)
                                .join(old_dir.to_string())
                                .join(format!("part.{number}")),
                        )
                        .await
                        .expect("original complete encoded multipart part"),
                    );
                }
                old_shards.push(parts);
                old_metadata.push(
                    tokio::fs::read(dir.path().join(&bucket).join(object).join(STORAGE_FORMAT_FILE))
                        .await
                        .expect("original metadata after full-tail completion"),
                );
            }
            let active = set
                .new_multipart_upload(&bucket, object, &options)
                .await
                .expect("pending upload A");
            let cancelled = set
                .new_multipart_upload(&bucket, object, &options)
                .await
                .expect("pending upload B");
            assert_ne!(active.upload_id, cancelled.upload_id);
            assert_ne!(active.upload_id, original.upload_id);
            let first_body = vec![0x63; first_size];
            let middle_body = vec![0x69; middle_size];
            let last_body = vec![0x74; last_size];
            let first = set
                .put_object_part(
                    &bucket,
                    object,
                    &active.upload_id,
                    1,
                    &mut PutObjReader::from_vec(first_body.clone()),
                    &options,
                )
                .await
                .expect("ACK A first part");
            let middle = set
                .put_object_part(
                    &bucket,
                    object,
                    &active.upload_id,
                    2,
                    &mut PutObjReader::from_vec(middle_body.clone()),
                    &options,
                )
                .await
                .expect("ACK A second initial part");
            let mut other_parts = Vec::new();
            for (number, byte, size) in [(1, 0x85, first_size), (2, 0x96, middle_size), (3, 0xa7, last_size)] {
                other_parts.push(
                    set.put_object_part(
                        &bucket,
                        object,
                        &cancelled.upload_id,
                        number,
                        &mut PutObjReader::from_vec(vec![byte; size]),
                        &options,
                    )
                    .await
                    .expect("ACK B independent pending part"),
                );
            }
            assert_ne!(first.etag, other_parts[0].etag);
            let (active_meta, _) = set
                .check_upload_id_exists(&bucket, object, &active.upload_id, false)
                .await
                .expect("independent active upload metadata");
            let active_dir = active_meta.data_dir.expect("A physical staging generation");
            let active_path = SetDisks::get_multipart_upload_dir(&bucket, object, &active.upload_id, false);
            let cancelled_path = SetDisks::get_multipart_upload_dir(&bucket, object, &cancelled.upload_id, false);
            let mut active_original_shards = Vec::new();
            for dir in &dirs {
                let mut parts = Vec::new();
                for number in [1, 2] {
                    parts.push(
                        tokio::fs::read(
                            dir.path()
                                .join(crate::disk::RUSTFS_META_MULTIPART_BUCKET)
                                .join(&active_path)
                                .join(active_dir.to_string())
                                .join(format!("part.{number}")),
                        )
                        .await
                        .expect("A original acknowledged staging shard"),
                    );
                }
                active_original_shards.push(parts);
            }
            println!("native_g05: geometry={disk_count}/{parity} committed_multipart_and_two_distinct_pending_uploads_seeded");
            let missing = dirs[0]
                .path()
                .join(&bucket)
                .join(object)
                .join(old_dir.to_string())
                .join("part.1");
            tokio::fs::remove_file(&missing)
                .await
                .expect("remove original committed multipart shard");
            let (_pause, arrived, release) = ReadRepairPauseScope::install(&bucket, object);
            let heal_set = set.clone();
            let heal_bucket = bucket.clone();
            let heal = tokio::spawn(async move {
                heal_set
                    .heal_object(
                        &heal_bucket,
                        object,
                        "",
                        &HealOpts {
                            read_repair: true,
                            scan_mode: HealScanMode::Deep,
                            ..Default::default()
                        },
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(30), arrived.notified())
                .await
                .expect("real MPU reconstruction reaches final rename pause");
            assert!(!heal.is_finished() && !missing.exists());
            println!("native_g05: geometry={disk_count}/{parity} real_committed_object_heal_paused_before_final_rename");
            let last = set
                .put_object_part(
                    &bucket,
                    object,
                    &active.upload_id,
                    3,
                    &mut PutObjReader::from_vec(last_body.clone()),
                    &options,
                )
                .await
                .expect("ACK continued A part during paused Heal");
            set.abort_multipart_upload(&bucket, object, &cancelled.upload_id, &options)
                .await
                .expect("ACK B abort during paused Heal");
            let gone = set
                .list_object_parts(&bucket, object, &cancelled.upload_id, None, 1000, &options)
                .await;
            assert!(
                matches!(gone, Err(Error::InvalidUploadID(ref b, ref o, ref id)) if b==&bucket && o==object && id==&cancelled.upload_id)
            );
            let pending = set
                .list_object_parts(&bucket, object, &active.upload_id, None, 1000, &options)
                .await
                .expect("B abort cannot remove A");
            assert!(!pending.is_truncated);
            assert_eq!(pending.upload_id, active.upload_id);
            assert_eq!(pending.parts.len(), 3);
            for (actual, expected) in pending.parts.iter().zip([&first, &middle, &last]) {
                assert_eq!(actual.part_num, expected.part_num);
                assert_eq!(actual.etag, expected.etag);
                assert_eq!(actual.size, expected.size);
            }
            for (slot, dir) in dirs.iter().enumerate() {
                assert_eq!(
                    tokio::fs::read(dir.path().join(&bucket).join(object).join(STORAGE_FORMAT_FILE))
                        .await
                        .expect("pending parts cannot publish an object"),
                    old_metadata[slot]
                );
                for number in [1, 2, 3] {
                    let path = dir
                        .path()
                        .join(&bucket)
                        .join(object)
                        .join(old_dir.to_string())
                        .join(format!("part.{number}"));
                    if slot == 0 && number == 1 {
                        assert!(!path.exists(), "paused Heal cannot have published its missing old shard");
                    } else {
                        assert_eq!(
                            tokio::fs::read(path)
                                .await
                                .expect("Abort preserves committed data before Complete"),
                            old_shards[slot][number - 1]
                        );
                    }
                }
                for number in [1, 2] {
                    assert_eq!(
                        tokio::fs::read(
                            dir.path()
                                .join(crate::disk::RUSTFS_META_MULTIPART_BUCKET)
                                .join(&active_path)
                                .join(active_dir.to_string())
                                .join(format!("part.{number}"))
                        )
                        .await
                        .expect("B abort preserves each original A shard"),
                        active_original_shards[slot][number - 1]
                    );
                }
                assert!(
                    !dir.path()
                        .join(crate::disk::RUSTFS_META_MULTIPART_BUCKET)
                        .join(&cancelled_path)
                        .exists()
                );
            }
            assert!(!heal.is_finished() && !missing.exists());
            println!(
                "native_g05: geometry={disk_count}/{parity} continue_ack_and_other_abort_preserve_pending_parts_and_original_commit"
            );
            let completed = set
                .clone()
                .complete_multipart_upload(
                    &bucket,
                    object,
                    &active.upload_id,
                    vec![
                        CompletePart {
                            part_num: first.part_num,
                            etag: first.etag.clone(),
                            ..Default::default()
                        },
                        CompletePart {
                            part_num: middle.part_num,
                            etag: middle.etag.clone(),
                            ..Default::default()
                        },
                        CompletePart {
                            part_num: last.part_num,
                            etag: last.etag.clone(),
                            ..Default::default()
                        },
                    ],
                    &options,
                )
                .await
                .expect("ACK A completion before releasing obsolete Heal");
            assert!(completed.etag.as_ref().is_some_and(|value| !value.is_empty()));
            assert_ne!(completed.etag, original_ack.etag);
            let namespace = set
                .new_ns_lock(&bucket, object)
                .await
                .expect("completed MPU physical drain fence");
            let drained = namespace
                .get_write_lock(Duration::from_secs(30))
                .await
                .expect("completed MPU tail drains");
            let current = disks[0]
                .read_version("", &bucket, object, "", &ReadOptions::default())
                .await
                .expect("new completion metadata");
            let new_dir = current.data_dir.expect("new multipart physical generation");
            assert_ne!(new_dir, old_dir);
            assert_eq!(current.parts.len(), 3);
            let mut committed = Vec::new();
            let mut new_shards = Vec::new();
            for dir in &dirs {
                let root = dir.path().join(&bucket).join(object);
                committed.push(
                    tokio::fs::read(root.join(STORAGE_FORMAT_FILE))
                        .await
                        .expect("ACK completion metadata"),
                );
                let mut parts = Vec::new();
                for number in [1, 2, 3] {
                    parts.push(
                        tokio::fs::read(root.join(new_dir.to_string()).join(format!("part.{number}")))
                            .await
                            .expect("ACK completion publishes every encoded part"),
                    );
                }
                new_shards.push(parts);
                assert!(
                    !root.join(old_dir.to_string()).exists(),
                    "completion reclaims the old multipart generation"
                );
                assert!(
                    !dir.path()
                        .join(crate::disk::RUSTFS_META_MULTIPART_BUCKET)
                        .join(&active_path)
                        .exists()
                );
            }
            drop(drained);
            assert!(!heal.is_finished());
            println!("native_g05: geometry={disk_count}/{parity} complete_ack_and_all_physical_tails_verified_before_release");
            release.notify_one();
            let (_, error) = tokio::time::timeout(Duration::from_secs(30), heal)
                .await
                .expect("stale MPU Heal exits its fence")
                .expect("Heal task joins")
                .expect("stale reconstruction returns its typed retry disposition");
            assert_eq!(error, Some(DiskError::Timeout));
            for (slot, dir) in dirs.iter().enumerate() {
                let root = dir.path().join(&bucket).join(object);
                assert_eq!(
                    tokio::fs::read(root.join(STORAGE_FORMAT_FILE))
                        .await
                        .expect("completion metadata after stale Heal"),
                    committed[slot]
                );
                let mut decoded = rustfs_filemeta::FileMeta::default();
                decoded
                    .unmarshal_msg(&committed[slot])
                    .expect("decode final generation independently");
                assert_eq!(decoded.versions.len(), 1, "completion must not publish another object version");
                for number in [1, 2, 3] {
                    assert_eq!(
                        tokio::fs::read(root.join(new_dir.to_string()).join(format!("part.{number}")))
                            .await
                            .expect("new parts before any GET"),
                        new_shards[slot][number - 1]
                    );
                }
                assert!(!root.join(old_dir.to_string()).exists(), "stale Heal cannot resurrect the old generation");
                for path in [&active_path, &cancelled_path] {
                    assert!(!dir.path().join(crate::disk::RUSTFS_META_MULTIPART_BUCKET).join(path).exists());
                }
            }
            for id in [&active.upload_id, &cancelled.upload_id] {
                let gone = set.list_object_parts(&bucket, object, id, None, 1000, &options).await;
                assert!(
                    matches!(gone, Err(Error::InvalidUploadID(ref b, ref o, ref upload)) if b==&bucket && o==object && upload==id)
                );
            }
            let mut reader = set
                .get_object_reader(&bucket, object, None, Default::default(), &options)
                .await
                .expect("read exact completed current object");
            assert_eq!(reader.object_info.etag, completed.etag);
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.expect("full completed multipart body");
            assert_eq!(actual, [first_body, middle_body, last_body].concat());
            println!(
                "native_g05: geometry={disk_count}/{parity} stale_heal_rejected_exact_completed_parts_full_body_and_upload_retirement_verified"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn native_default_heal_owner_fences_queued_put_and_version_delete() {
        use crate::object_api::{ShardIntegrityWriteMode, WriteCompletion};
        use crate::set_disk::ops::object::{DeleteObjectCommitBarrier, PutObjectCommitBarrier, PutObjectCommitPause};
        use std::collections::HashMap;
        use std::task::Poll;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        for (disk_count, parity) in [(4, 2), (16, 4)] {
            for mutation in ["put", "version-delete"] {
                let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(disk_count, 0, parity).await;
                let bucket = format!("default-heal-owner-{disk_count}-{mutation}");
                let object = "locked/key.bin";
                let reference = "independent/reference.bin";
                set.make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("isolated owner bucket");
                let options = ObjectOptions {
                    versioned: mutation == "version-delete",
                    write_completion: WriteCompletion::TailDrained,
                    shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                    ..Default::default()
                };
                let original_body = vec![0x18; 1024 * 1024 + 113];
                let successor_body = vec![0x29; 1024 * 1024 + 157];
                let original = set
                    .put_object(&bucket, object, &mut PutObjReader::from_vec(original_body.clone()), &options)
                    .await
                    .expect("original acknowledged version");
                let selected_version = original.version_id.map(|v| v.to_string()).unwrap_or_default();
                let current = if mutation == "version-delete" {
                    Some(
                        set.put_object(&bucket, object, &mut PutObjReader::from_vec(successor_body.clone()), &options)
                            .await
                            .expect("distinct current version remains protected"),
                    )
                } else {
                    None
                };
                let mut current_parts = Vec::new();
                let current_data_dir = if let Some(current) = &current {
                    let info = disks[0]
                        .read_version(
                            "",
                            &bucket,
                            object,
                            &current.version_id.expect("retained current version").to_string(),
                            &ReadOptions::default(),
                        )
                        .await
                        .expect("independent retained-current physical generation");
                    let data_dir = info.data_dir.expect("retained current external generation");
                    for dir in &dirs {
                        current_parts.push(
                            tokio::fs::read(
                                dir.path()
                                    .join(&bucket)
                                    .join(object)
                                    .join(data_dir.to_string())
                                    .join("part.1"),
                            )
                            .await
                            .expect("independent retained-current shard oracle"),
                        );
                    }
                    Some(data_dir)
                } else {
                    None
                };
                let mut reference_parts = HashMap::new();
                if mutation == "put" {
                    set.put_object(&bucket, reference, &mut PutObjReader::from_vec(successor_body.clone()), &options)
                        .await
                        .expect("independent successor payload coding oracle");
                    for (slot, disk) in disks.iter().enumerate() {
                        let info = disk
                            .read_version("", &bucket, reference, "", &ReadOptions::default())
                            .await
                            .expect("reference coding index");
                        let path = dirs[slot]
                            .path()
                            .join(&bucket)
                            .join(reference)
                            .join(info.data_dir.expect("reference external data").to_string())
                            .join("part.1");
                        assert!(
                            reference_parts
                                .insert(info.erasure.index, tokio::fs::read(path).await.expect("reference encoded part"))
                                .is_none()
                        );
                    }
                }
                let old = disks[0]
                    .read_version("", &bucket, object, &selected_version, &ReadOptions::default())
                    .await
                    .expect("selected original generation");
                assert_eq!((old.erasure.data_blocks, old.erasure.parity_blocks), (disk_count - parity, parity));
                let old_dir = old.data_dir.expect("original external generation");
                let mut original_parts = Vec::new();
                let mut original_metadata = Vec::new();
                for dir in &dirs {
                    let root = dir.path().join(&bucket).join(object);
                    original_parts.push(
                        tokio::fs::read(root.join(old_dir.to_string()).join("part.1"))
                            .await
                            .expect("original encoded part"),
                    );
                    original_metadata.push(
                        tokio::fs::read(root.join(STORAGE_FORMAT_FILE))
                            .await
                            .expect("original complete version metadata"),
                    );
                }
                let namespace = set.new_ns_lock(&bucket, object).await.expect("same actual object namespace");
                let mut available_reader = Box::pin(namespace.get_read_lock(Duration::from_secs(30)));
                assert!(
                    matches!(futures::poll!(available_reader.as_mut()), Poll::Ready(Ok(_))),
                    "positive control: an unowned local namespace admits a reader immediately"
                );
                drop(available_reader);
                let missing = dirs[0]
                    .path()
                    .join(&bucket)
                    .join(object)
                    .join(old_dir.to_string())
                    .join("part.1");
                tokio::fs::remove_file(&missing)
                    .await
                    .expect("one original external shard missing");
                let (pause, arrived, release) =
                    ReadRepairPauseScope::install_at(&bucket, object, super::ReadRepairPausePhase::PartStaging(1));
                let heal_set = set.clone();
                let heal_bucket = bucket.clone();
                let heal_version = selected_version.clone();
                let heal = tokio::spawn(async move {
                    heal_set
                        .heal_object(
                            &heal_bucket,
                            object,
                            &heal_version,
                            &HealOpts {
                                no_lock: false,
                                scan_mode: HealScanMode::Deep,
                                ..Default::default()
                            },
                        )
                        .await
                });
                tokio::time::timeout(Duration::from_secs(30), arrived.notified())
                    .await
                    .expect("default Deep Heal owns namespace during actual staging");
                let mut blocked_reader = Box::pin(namespace.get_read_lock(Duration::from_secs(30)));
                assert!(
                    matches!(futures::poll!(blocked_reader.as_mut()), Poll::Pending),
                    "default Heal must own an exclusive namespace before any queued writer exists"
                );
                drop(blocked_reader);
                println!(
                    "native_a09: geometry={disk_count}/{parity} mutation={mutation} default_deep_heal_staged_exclusive_owner_read_blocked"
                );
                let put_before = (mutation == "put")
                    .then(|| PutObjectCommitBarrier::install(&bucket, object, PutObjectCommitPause::BeforeNamespace));
                let put_commit = (mutation == "put")
                    .then(|| PutObjectCommitBarrier::install(&bucket, object, PutObjectCommitPause::AfterNamespace));
                let mut delete_before =
                    (mutation == "version-delete").then(|| DeleteObjectCommitBarrier::install_before_namespace(&bucket, object));
                let foreground_set = set.clone();
                let foreground_bucket = bucket.clone();
                let foreground_options = options.clone();
                let body = successor_body.clone();
                let version = selected_version.clone();
                let mut foreground = tokio::spawn(async move {
                    if mutation == "put" {
                        foreground_set
                            .put_object(&foreground_bucket, object, &mut PutObjReader::from_vec(body), &foreground_options)
                            .await
                    } else {
                        foreground_set
                            .delete_object(
                                &foreground_bucket,
                                object,
                                ObjectOptions {
                                    version_id: Some(version),
                                    object_lock_config_snapshot: Some(Arc::new(crate::set_disk::ObjectLockConfigSnapshot::new(
                                        crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
                                    ))),
                                    ..foreground_options
                                },
                            )
                            .await
                    }
                });
                let delete_commit;
                if let Some(before) = &put_before {
                    before.wait_until_paused().await;
                    before.release_and_wait_until_namespace_pending().await;
                    assert!(!before.namespace_acquired());
                    delete_commit = None;
                } else {
                    let before = delete_before.take().expect("actual DELETE before-namespace observer");
                    before.wait_until_paused().await;
                    before.release_and_wait_until_namespace_pending().await;
                    assert!(!before.namespace_acquired());
                    drop(before);
                    delete_commit = Some(DeleteObjectCommitBarrier::install(&bucket, object));
                }
                assert!(!foreground.is_finished() && !heal.is_finished() && !missing.exists());
                for (slot, dir) in dirs.iter().enumerate() {
                    let root = dir.path().join(&bucket).join(object);
                    assert_eq!(
                        tokio::fs::read(root.join(STORAGE_FORMAT_FILE))
                            .await
                            .expect("queued writer cannot change original metadata"),
                        original_metadata[slot]
                    );
                    if slot != 0 {
                        assert_eq!(
                            tokio::fs::read(root.join(old_dir.to_string()).join("part.1"))
                                .await
                                .expect("surviving original part"),
                            original_parts[slot]
                        );
                    }
                }
                println!(
                    "native_a09: geometry={disk_count}/{parity} mutation={mutation} actual_foreground_namespace_future_pending_original_bytes_unchanged"
                );
                release.notify_one();
                let (result, error) = tokio::time::timeout(Duration::from_secs(30), heal)
                    .await
                    .expect("default Heal completes before queued foreground publishes")
                    .expect("Heal joins")
                    .expect("actual default Heal result");
                assert_eq!(error, None);
                assert!(result.integrity_verified);
                drop(pause);
                if let Some(commit) = &put_commit {
                    tokio::select! {
                        _ = commit.wait_until_paused() => {},
                        early = &mut foreground => panic!("PUT must reach its real post-lock barrier before publishing: {early:?}"),
                    }
                }
                if let Some(commit) = &delete_commit {
                    tokio::select! {
                        _ = commit.wait_until_paused() => {},
                        early = &mut foreground => panic!("DELETE must reach its real pre-commit barrier after the Heal owner drains: {early:?}"),
                    }
                }
                assert!(!foreground.is_finished());
                for (slot, dir) in dirs.iter().enumerate() {
                    assert_eq!(
                        tokio::fs::read(dir.path().join(&bucket).join(object).join(old_dir.to_string()).join("part.1"))
                            .await
                            .expect("Heal physically reconstructs before foreground commit"),
                        original_parts[slot]
                    );
                }
                println!(
                    "native_a09: geometry={disk_count}/{parity} mutation={mutation} heal_reconstructed_before_foreground_commit"
                );
                if let Some(commit) = &put_commit {
                    commit.release();
                }
                if let Some(commit) = &delete_commit {
                    commit.release();
                }
                let ack = tokio::time::timeout(Duration::from_secs(30), foreground)
                    .await
                    .expect("queued foreground ACK after Heal releases ownership")
                    .expect("foreground joins")
                    .expect("real foreground ACK");
                let drained = namespace
                    .get_write_lock(Duration::from_secs(30))
                    .await
                    .expect("ACK physical tail drain");
                for (slot, disk) in disks.iter().enumerate() {
                    let root = dirs[slot].path().join(&bucket).join(object);
                    assert!(
                        !root.join(old_dir.to_string()).exists(),
                        "overwritten/deleted original generation cannot remain"
                    );
                    let raw = tokio::fs::read(root.join(STORAGE_FORMAT_FILE))
                        .await
                        .expect("final single acknowledged version metadata");
                    let mut decoded = rustfs_filemeta::FileMeta::default();
                    decoded.unmarshal_msg(&raw).expect("decode actual final version population");
                    assert_eq!(decoded.versions.len(), 1);
                    let final_info = disk
                        .read_version("", &bucket, object, "", &ReadOptions::default())
                        .await
                        .expect("final current generation");
                    let data_dir = final_info.data_dir.expect("external successor generation");
                    let part = tokio::fs::read(root.join(data_dir.to_string()).join("part.1"))
                        .await
                        .expect("final part before any GET");
                    if mutation == "put" {
                        assert_ne!(data_dir, old_dir);
                        assert_eq!(part, reference_parts[&final_info.erasure.index]);
                    } else {
                        assert_eq!(Some(data_dir), current_data_dir);
                        assert_eq!(
                            part, current_parts[slot],
                            "exact version DELETE must preserve every retained-current shard"
                        );
                        assert_eq!(ack.version_id, original.version_id);
                        assert_eq!(final_info.version_id, current.as_ref().expect("retained current ACK").version_id);
                        assert!(matches!(
                            disk.read_version("", &bucket, object, &selected_version, &ReadOptions::default())
                                .await,
                            Err(DiskError::FileVersionNotFound)
                        ));
                    }
                }
                drop(drained);
                let final_options = if let Some(current) = &current {
                    ObjectOptions {
                        versioned: true,
                        version_id: Some(current.version_id.expect("current version").to_string()),
                        suppress_read_repair: true,
                        ..options.clone()
                    }
                } else {
                    ObjectOptions {
                        suppress_read_repair: true,
                        ..options.clone()
                    }
                };
                let mut reader = set
                    .get_object_reader(&bucket, object, None, Default::default(), &final_options)
                    .await
                    .expect("full acknowledged successor/current body");
                let mut actual = Vec::new();
                reader
                    .read_to_end(&mut actual)
                    .await
                    .expect("full body with incidental repair suppressed");
                assert_eq!(actual, successor_body);
                assert_eq!(
                    reader.object_info.etag,
                    if let Some(current) = &current {
                        current.etag.clone()
                    } else {
                        ack.etag.clone()
                    }
                );
                println!(
                    "native_a09: geometry={disk_count}/{parity} mutation={mutation} foreground_ack_final_versions_parts_and_full_body_verified"
                );
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn native_copy_and_metadata_mutations_preserve_acknowledged_state_during_heal() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks_with_count};
        use crate::object_api::{ShardIntegrityWriteMode, WriteCompletion};
        use std::collections::HashMap;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        for (disk_count, parity) in [(4, 2), (16, 4)] {
            for mutation in ["copy-source", "copy-destination", "replace-metadata", "update-tags"] {
                let (dirs, store) = isolated_store_over_temp_disks_with_count(disk_count).await;
                init_bucket_metadata_sys(store.clone(), Vec::new()).await;
                let bucket = format!("copy-heal-fence-{disk_count}-{mutation}");
                let source = "source/actual key.bin";
                let destination = "destination/actual key.bin";
                let control = "unrelated/control.bin";
                store
                    .make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("isolated copy bucket");
                let options = ObjectOptions {
                    write_completion: WriteCompletion::TailDrained,
                    shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                    ..Default::default()
                };
                let source_body = vec![0x31; 1024 * 1024 + 113];
                let destination_body = vec![0x42; 1024 * 1024 + 137];
                let control_body = vec![0x53; 1024 * 1024 + 157];
                let mut seed_acks = HashMap::new();
                for (key, body, role) in [
                    (source, &source_body, "source"),
                    (destination, &destination_body, "destination"),
                    (control, &control_body, "control"),
                ] {
                    let mut opts = options.clone();
                    opts.user_defined = HashMap::from([
                        ("content-type".to_owned(), format!("application/{role}")),
                        ("x-amz-meta-role".to_owned(), role.to_owned()),
                    ]);
                    let ack = store
                        .put_object(&bucket, key, &mut PutObjReader::from_vec(body.clone()), &opts)
                        .await
                        .expect("ACK complete external fixture");
                    seed_acks.insert(key, ack);
                }
                let set = store.pools[0].disk_set[0].clone();
                let disks = set.disk_inventory().await;
                let first_disk = disks[0].as_ref().expect("online first physical member");
                let original_destination = first_disk
                    .read_version("", &bucket, destination, "", &ReadOptions::default())
                    .await
                    .expect("independent original COPY destination generation");
                let original_destination_dir = original_destination
                    .data_dir
                    .expect("original destination external generation");
                let heal_key = if mutation == "copy-source" { source } else { destination };
                let old = first_disk
                    .read_version("", &bucket, heal_key, "", &ReadOptions::default())
                    .await
                    .expect("old healed generation metadata");
                assert_eq!((old.erasure.data_blocks, old.erasure.parity_blocks), (disk_count - parity, parity));
                assert!(!old.inline_data());
                let old_dir = old.data_dir.expect("external healed generation");
                let original_parts = futures::future::join_all(dirs.iter().map(|dir| {
                    tokio::fs::read(
                        dir.path()
                            .join(&bucket)
                            .join(heal_key)
                            .join(old_dir.to_string())
                            .join("part.1"),
                    )
                }))
                .await
                .into_iter()
                .map(|v| v.expect("original same-role part"))
                .collect::<Vec<_>>();
                let missing = dirs[0]
                    .path()
                    .join(&bucket)
                    .join(heal_key)
                    .join(old_dir.to_string())
                    .join("part.1");
                tokio::fs::remove_file(&missing)
                    .await
                    .expect("inject one real missing external shard");
                let (pause, arrived, release) = ReadRepairPauseScope::install(&bucket, heal_key);
                let heal_set = set.clone();
                let heal_bucket = bucket.clone();
                let heal = tokio::spawn(async move {
                    heal_set
                        .heal_object(
                            &heal_bucket,
                            heal_key,
                            "",
                            &HealOpts {
                                read_repair: true,
                                scan_mode: HealScanMode::Deep,
                                ..Default::default()
                            },
                        )
                        .await
                });
                tokio::time::timeout(Duration::from_secs(30), arrived.notified())
                    .await
                    .expect("actual copy/metadata Heal reconstruction reaches final rename");
                assert!(!heal.is_finished() && !missing.exists());
                println!(
                    "native_g06: geometry={disk_count}/{parity} mutation={mutation} real_reconstruction_paused_before_final_rename"
                );
                let ack = if matches!(mutation, "copy-source" | "copy-destination") {
                    // Keep the COPY source read from supplying a second repair;
                    // the separately paused production Heal owns this oracle.
                    let read_options = ObjectOptions {
                        suppress_read_repair: true,
                        ..options.clone()
                    };
                    let reader = store
                        .get_object_reader(&bucket, source, None, Default::default(), &read_options)
                        .await
                        .expect("actual COPY source reader");
                    let mut info = reader.object_info.clone();
                    let size = info.get_actual_size().expect("COPY source logical size");
                    assert_eq!(size, source_body.len() as i64);
                    let hash = rustfs_rio::HashReader::from_stream(reader.stream, size, size, None, None, false)
                        .expect("actual streamed COPY source");
                    info.put_object_reader = Some(PutObjReader::new(hash));
                    let ack = tokio::time::timeout(
                        Duration::from_secs(30),
                        store.copy_object(&bucket, source, &bucket, destination, &mut info, &read_options, &options),
                    )
                    .await
                    .expect("COPY ACK before paused Heal release")
                    .expect("production cross-key COPY ACK");
                    assert_eq!(ack.etag, seed_acks[source].etag);
                    ack
                } else if mutation == "replace-metadata" {
                    let mut info = store
                        .get_object_info(&bucket, destination, &options)
                        .await
                        .expect("self-COPY source metadata");
                    info.metadata_only = true;
                    info.user_defined = Arc::new(HashMap::from([
                        ("content-type".to_owned(), "application/replaced".to_owned()),
                        ("x-amz-meta-race".to_owned(), "metadata-replaced".to_owned()),
                    ]));
                    tokio::time::timeout(
                        Duration::from_secs(30),
                        store.copy_object(&bucket, destination, &bucket, destination, &mut info, &options, &options),
                    )
                    .await
                    .expect("self metadata COPY ACK before Heal release")
                    .expect("production self-COPY metadata ACK")
                } else {
                    tokio::time::timeout(
                        Duration::from_secs(30),
                        store.put_object_tags(&bucket, destination, "race=updated&nonce=g06", &options),
                    )
                    .await
                    .expect("tag update ACK before Heal release")
                    .expect("production tag update ACK")
                };
                assert!(ack.etag.as_ref().is_some_and(|v| !v.is_empty()));
                let namespace = set.new_ns_lock(&bucket, destination).await.expect("COPY/mutation namespace");
                let drained = namespace
                    .get_write_lock(Duration::from_secs(30))
                    .await
                    .expect("foreground physical tails drain");
                let mut acknowledged = Vec::new();
                for key in [source, destination, control] {
                    let current = first_disk
                        .read_version("", &bucket, key, "", &ReadOptions::default())
                        .await
                        .expect("foreground committed metadata");
                    let data_dir = current.data_dir.expect("external foreground generation");
                    let mut members = Vec::new();
                    for dir in &dirs {
                        let root = dir.path().join(&bucket).join(key);
                        let meta = tokio::fs::read(root.join(STORAGE_FORMAT_FILE))
                            .await
                            .expect("foreground metadata bytes");
                        let part_path = root.join(data_dir.to_string()).join("part.1");
                        let part = if part_path.exists() {
                            Some(tokio::fs::read(part_path).await.expect("foreground part bytes"))
                        } else {
                            None
                        };
                        members.push((meta, part));
                    }
                    acknowledged.push((key, data_dir, members));
                }
                if matches!(mutation, "copy-source" | "copy-destination") {
                    assert_ne!(acknowledged[1].1, original_destination_dir);
                    assert!(dirs.iter().all(|dir| {
                        !dir.path()
                            .join(&bucket)
                            .join(destination)
                            .join(original_destination_dir.to_string())
                            .exists()
                    }));
                }
                if mutation != "copy-destination" {
                    assert_eq!(acknowledged.iter().find(|row| row.0 == heal_key).expect("healed key").1, old_dir);
                    assert!(!missing.exists(), "foreground metadata/source read must not supply the missing repair");
                }
                drop(drained);
                assert!(!heal.is_finished());
                println!(
                    "native_g06: geometry={disk_count}/{parity} mutation={mutation} foreground_ack_and_all_physical_tails_verified_before_release"
                );
                release.notify_one();
                let (_, error) = tokio::time::timeout(Duration::from_secs(30), heal)
                    .await
                    .expect("copy/metadata Heal exits its fence")
                    .expect("Heal joins")
                    .expect("actual Heal result");
                assert_eq!(
                    error,
                    if mutation == "copy-source" {
                        None
                    } else {
                        Some(DiskError::Timeout)
                    }
                );
                drop(pause);
                for (key, data_dir, members) in &acknowledged {
                    for (slot, dir) in dirs.iter().enumerate() {
                        let root = dir.path().join(&bucket).join(key);
                        let metadata = tokio::fs::read(root.join(STORAGE_FORMAT_FILE))
                            .await
                            .expect("metadata after Heal release");
                        if *key != heal_key || mutation != "copy-source" {
                            assert_eq!(metadata, members[slot].0, "stale Heal must preserve foreground metadata exactly");
                        }
                        let part_path = root.join(data_dir.to_string()).join("part.1");
                        match &members[slot].1 {
                            Some(expected) => {
                                assert_eq!(tokio::fs::read(part_path).await.expect("acknowledged part survives Heal"), *expected)
                            }
                            None if mutation == "copy-source" => assert_eq!(
                                tokio::fs::read(part_path)
                                    .await
                                    .expect("real source Heal rebuilds before any GET"),
                                original_parts[slot]
                            ),
                            None => assert!(!part_path.exists(), "stale Heal cannot publish its obsolete staged part"),
                        }
                    }
                }
                if matches!(mutation, "replace-metadata" | "update-tags") {
                    let (_, error) = set
                        .heal_object(
                            &bucket,
                            destination,
                            "",
                            &HealOpts {
                                scan_mode: HealScanMode::Deep,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("independent current-metadata Heal after stale refusal");
                    assert_eq!(error, None);
                    for (slot, dir) in dirs.iter().enumerate() {
                        assert_eq!(
                            tokio::fs::read(
                                dir.path()
                                    .join(&bucket)
                                    .join(destination)
                                    .join(old_dir.to_string())
                                    .join("part.1")
                            )
                            .await
                            .expect("fresh Heal rebuilds original data with acknowledged new metadata"),
                            original_parts[slot]
                        );
                    }
                }
                for (key, expected) in [
                    (source, &source_body),
                    (
                        destination,
                        if matches!(mutation, "copy-source" | "copy-destination") {
                            &source_body
                        } else {
                            &destination_body
                        },
                    ),
                    (control, &control_body),
                ] {
                    let mut reader = store
                        .get_object_reader(&bucket, key, None, Default::default(), &options)
                        .await
                        .expect("full final COPY/mutation/control read");
                    let mut body = Vec::new();
                    reader
                        .stream
                        .read_to_end(&mut body)
                        .await
                        .expect("read complete acknowledged body");
                    assert_eq!(&body, expected);
                    if !(key == destination && mutation == "replace-metadata") {
                        let role =
                            if key == source || key == destination && matches!(mutation, "copy-source" | "copy-destination") {
                                "source"
                            } else if key == destination {
                                "destination"
                            } else {
                                "control"
                            };
                        assert_eq!(reader.object_info.user_defined.get("x-amz-meta-role").map(String::as_str), Some(role));
                        assert_eq!(reader.object_info.user_defined.get("content-type"), Some(&format!("application/{role}")));
                    }
                    if key == destination && mutation == "replace-metadata" {
                        let metadata = reader.object_info.user_defined.as_ref();
                        assert_eq!(metadata.get("x-amz-meta-race").map(String::as_str), Some("metadata-replaced"));
                        assert_eq!(metadata.get("content-type").map(String::as_str), Some("application/replaced"));
                        assert!(!metadata.contains_key("x-amz-meta-role"));
                    }
                }
                for key in [source, destination, control] {
                    let expected = if key == destination && mutation == "update-tags" {
                        "race=updated&nonce=g06"
                    } else {
                        ""
                    };
                    assert_eq!(
                        store
                            .get_object_tags(&bucket, key, &options)
                            .await
                            .expect("exact acknowledged and control tags after Heal"),
                        expected
                    );
                }
                println!(
                    "native_g06: geometry={disk_count}/{parity} mutation={mutation} exact_copy_metadata_tags_generations_and_full_bodies_verified"
                );
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn read_repair_storage_entry_keeps_readers_available_during_reconstruction() {
        use crate::object_api::WriteCompletion;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "read-repair-storage-entry";
        let object = "object.bin";
        let expected = vec![0x37; 1024 * 1024];
        set.make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        set.put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(expected.clone()),
            &ObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("write object");
        let metadata = disks[0]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("read metadata");
        let missing = dirs[0]
            .path()
            .join(bucket)
            .join(object)
            .join(metadata.data_dir.expect("external object data").to_string())
            .join("part.1");
        tokio::fs::remove_file(&missing).await.expect("remove shard");

        let (_pause, arrived, release) =
            ReadRepairPauseScope::install_at(bucket, object, super::ReadRepairPausePhase::PartStaging(1));
        let heal_set = set.clone();
        let heal = tokio::spawn(async move {
            let mut absence = None;
            let result = heal_set
                .heal_object_with_retirement(
                    bucket,
                    object,
                    "",
                    &HealOpts {
                        scan_mode: HealScanMode::Deep,
                        read_repair: true,
                        ..Default::default()
                    },
                    &mut absence,
                    AbsenceProofRequest {
                        retirement: None,
                        allow_unversioned: false,
                    },
                )
                .await;
            (result, absence)
        });
        tokio::time::timeout(Duration::from_secs(10), arrived.notified())
            .await
            .expect("read-repair reached shard reconstruction");
        let namespace = set.new_ns_lock(bucket, object).await.expect("object namespace");
        let reader = namespace
            .get_read_lock(Duration::from_millis(200))
            .await
            .expect("read-repair reconstruction should allow concurrent readers");
        drop(reader);
        release.notify_one();
        let (result, absence) = tokio::time::timeout(Duration::from_secs(20), heal)
            .await
            .expect("repair finishes")
            .expect("repair task joins");
        let (_, error) = result.expect("repair operation succeeds");
        assert!(error.is_none(), "repair failed: {error:?}");
        assert!(absence.is_none());
        assert!(missing.exists(), "missing shard was not restored");
        let mut reader = set
            .get_object_reader(bucket, object, None, http::HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("read repaired object");
        let mut actual = Vec::new();
        reader.stream.read_to_end(&mut actual).await.expect("read full object");
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn read_repair_storage_entry_does_not_prove_missing_versions_absent() {
        let (_dirs, _disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "read-repair-no-absence-proof";
        set.make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        let mut absence = None;
        let (_, error) = set
            .heal_object_with_retirement(
                bucket,
                "missing.bin",
                &Uuid::new_v4().to_string(),
                &HealOpts {
                    read_repair: true,
                    ..Default::default()
                },
                &mut absence,
                AbsenceProofRequest {
                    retirement: None,
                    allow_unversioned: false,
                },
            )
            .await
            .expect("missing version report");
        assert!(matches!(error, Some(Error::FileVersionNotFound)));
        assert!(absence.is_none(), "read repair must not publish an absence proof");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn read_repair_commit_aborts_when_object_changes_before_final_rename() {
        let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
        let bucket = "read-repair-stale-commit";
        let object = "object.bin";
        set.make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        const PAYLOAD_SIZE: usize = 1024 * 1024;
        let mut initial_reader = PutObjReader::from_vec(vec![0x11; PAYLOAD_SIZE]);
        // This fixture reads and removes physical shards immediately after
        // PUT. A lock-owning PUT may quorum-ack before its rename tail drains,
        // so keep the setup on the full-fanout commit path.
        set.put_object(
            bucket,
            object,
            &mut initial_reader,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("initial object should be written");

        let current = disks[2]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("current metadata should be readable");
        let data_dir = current.data_dir.expect("non-inline object should have a data dir");
        let missing_part = temp_dirs
            .iter()
            .map(|dir| dir.path().join(bucket).join(object).join(data_dir.to_string()).join("part.1"))
            .find(|part| part.is_file())
            .expect("part.1 should exist on at least one erasure disk");
        tokio::fs::remove_file(missing_part)
            .await
            .expect("shard damage should force read-repair data work");

        let (_pause, arrived, release) = ReadRepairPauseScope::install(bucket, object);
        let heal_set = set.clone();
        let heal_task = tokio::spawn(async move {
            heal_set
                .heal_object_with_retirement(
                    bucket,
                    object,
                    "",
                    &HealOpts {
                        scan_mode: HealScanMode::Deep,
                        read_repair: true,
                        ..Default::default()
                    },
                    &mut None,
                    AbsenceProofRequest {
                        retirement: None,
                        allow_unversioned: false,
                    },
                )
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(5), arrived.notified())
            .await
            .expect("read-repair should reach the final commit fence");

        let mut overwrite_reader = PutObjReader::from_vec(vec![0x22; PAYLOAD_SIZE]);
        let overwrite = set
            .put_object(bucket, object, &mut overwrite_reader, &ObjectOptions::default())
            .await
            .expect("overwrite should commit while read-repair waits before final fence");
        release.notify_one();

        let (_result, err) = tokio::time::timeout(std::time::Duration::from_secs(5), heal_task)
            .await
            .expect("read-repair should finish after release")
            .expect("read-repair task should join")
            .expect("stale read-repair should report a typed retryable error");
        assert!(matches!(err, Some(Error::Timeout)), "stale repair must retry: {err:?}");

        let final_info = set
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("object must remain readable after stale read-repair abort");
        assert_eq!(final_info.etag, overwrite.etag);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn object_lock_diagnostics_distinguish_read_repair_from_queued_put() {
        use crate::diagnostics::object_lock::tests::CapturedLockEvents;
        use crate::object_api::WriteCompletion;
        use crate::set_disk::ops::object::{PutObjectCommitBarrier, PutObjectCommitPause};
        use crate::storage_api_contracts::namespace::NamespaceLocking as _;
        use tokio::io::AsyncReadExt;
        use tracing::{Instrument, instrument::WithSubscriber};

        let capture = CapturedLockEvents::default();
        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_LOCK_DIAG_ENABLE, Some("true")),
                (rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1")),
                // Keep the writer queued while allowing slow CI runs to finish the read repair.
                (rustfs_config::ENV_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS, Some("30000")),
            ],
            async {
                let (dirs, disks, set) = hermetic_set_disks_isolated(4).await;
                let bucket = "read-repair-lock-diagnostics";
                let object = "object.bin";
                set.make_bucket(bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create bucket");
                let opts = ObjectOptions {
                    versioned: true,
                    write_completion: WriteCompletion::TailDrained,
                    ..Default::default()
                };
                let size = 1024 * 1024;
                let original = set
                    .put_object(bucket, object, &mut PutObjReader::from_vec(vec![0x11; size]), &opts)
                    .await
                    .expect("seed historical version")
                    .version_id
                    .expect("historical version id");
                let newest = set
                    .put_object(bucket, object, &mut PutObjReader::from_vec(vec![0x22; size]), &opts)
                    .await
                    .expect("seed current version")
                    .version_id
                    .expect("current version id");
                let metadata = disks[3]
                    .read_version("", bucket, object, &original.to_string(), &ReadOptions::default())
                    .await
                    .expect("historical shard metadata");
                let missing = dirs[3]
                    .path()
                    .join(bucket)
                    .join(object)
                    .join(metadata.data_dir.expect("non-inline data directory").to_string())
                    .join("part.1");
                tokio::fs::remove_file(&missing).await.expect("damage one historical shard");

                let (_pause, arrived, release) =
                    ReadRepairPauseScope::install_at(bucket, object, super::ReadRepairPausePhase::Data);
                let heal_set = set.clone();
                let heal = tokio::spawn(
                    async move {
                        heal_set
                            .heal_object(
                                bucket,
                                object,
                                &original.to_string(),
                                &HealOpts {
                                    read_repair: true,
                                    scan_mode: HealScanMode::Deep,
                                    ..Default::default()
                                },
                            )
                            .await
                    }
                    .instrument(tracing::info_span!("heal_task", task_id = "diagnostic-read-repair"))
                    .with_current_subscriber(),
                );
                tokio::time::timeout(std::time::Duration::from_secs(10), arrived.notified())
                    .await
                    .expect("read-repair must own its shared data-phase lock");

                // A second reader can enter before there is a queued writer.
                let namespace = set.new_ns_lock(bucket, object).await.expect("reader namespace");
                drop(
                    namespace
                        .get_read_lock(std::time::Duration::from_secs(1))
                        .await
                        .expect("shared reconstruction alone permits readers"),
                );
                let barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeNamespace);
                let put_set = set.clone();
                let put = tokio::spawn(
                    async move {
                        put_set
                            .put_object(bucket, object, &mut PutObjReader::from_vec(vec![0x33; size]), &opts)
                            .await
                    }
                    .with_current_subscriber(),
                );
                barrier.wait_until_paused().await;
                barrier.release_and_wait_until_namespace_pending().await;
                assert!(!barrier.namespace_acquired(), "PUT must still be waiting behind read-repair");

                let result = set
                    .get_object_reader(
                        bucket,
                        object,
                        None,
                        http::HeaderMap::new(),
                        &ObjectOptions {
                            version_id: Some(original.to_string()),
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await;
                assert!(
                    matches!(result, Err(crate::error::StorageError::Lock(rustfs_lock::LockError::Timeout { .. }))),
                    "a queued writer must block the later historical reader"
                );
                assert!(!barrier.namespace_acquired(), "GET can time out before PUT owns the lock");
                let rows = capture.rows();
                let shared = rows
                    .iter()
                    .find(|row| row["fields"]["op"] == "read_repair_data" && row["fields"]["state"] == "acquired")
                    .expect("read-repair shared holder event");
                assert_eq!(shared["fields"]["requested_version_id"], original.to_string());
                assert!(
                    !rows
                        .iter()
                        .any(|row| row["fields"]["lock_attempt_id"] == shared["fields"]["lock_attempt_id"]
                            && row["fields"]["state"] == "guard_dropped")
                );
                assert!(
                    rows.iter()
                        .any(|row| row["fields"]["mode"] == "read" && row["fields"]["failure"] == "timeout"),
                    "failed reader acquisition must be retained"
                );

                release.notify_one();
                let added = put
                    .await
                    .expect("PUT task")
                    .expect("queued PUT commits")
                    .version_id
                    .expect("added version id");
                let (_, error) = heal.await.expect("read-repair task").expect("read-repair operation");
                assert!(
                    error.is_none(),
                    "unchanged historical version should heal after the writer completes: {error:?}"
                );
                assert!(tokio::fs::try_exists(&missing).await.expect("repaired shard path"));
                for (version, value) in [(original, 0x11), (newest, 0x22), (added, 0x33)] {
                    let mut reader = set
                        .get_object_reader(
                            bucket,
                            object,
                            None,
                            http::HeaderMap::new(),
                            &ObjectOptions {
                                version_id: Some(version.to_string()),
                                versioned: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read each acknowledged version");
                    assert_eq!(reader.object_info.version_id, Some(version));
                    let mut body = Vec::new();
                    reader.stream.read_to_end(&mut body).await.expect("complete version body");
                    assert_eq!(body, vec![value; size]);
                }
            }
            .with_subscriber(capture.subscriber()),
        )
        .await;
    }

    // HS-12 (backlog#1874): unversioned overwrite commits race a Deep heal on
    // the same object. The overwrite's post-commit tail deletes the replaced
    // data dir without the ns lock (object.rs commit tail), which is exactly
    // the intersection the audit flagged: the heal must tolerate the tail race
    // (retryable outcome) and every committed overwrite must survive — the
    // final current version is exactly the last payload written. Also cover the
    // read-repair path, which reconstructs under a shared data-phase lock and
    // revalidates before the final rename.
    #[tokio::test]
    #[serial_test::serial]
    async fn heal_racing_unversioned_overwrites_preserves_the_last_commit() {
        for read_repair in [false, true] {
            let (temp_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
            let bucket = if read_repair {
                "read-repair-race-put-overwrite"
            } else {
                "heal-race-put-overwrite"
            };
            let object = "object.bin";
            set.make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");

            const ROUNDS: usize = 8;
            const PAYLOAD_SIZE: usize = 256 * 1024;
            let mut last_etag = String::new();
            for round in 0..ROUNDS {
                // Give the heal something to rebuild on alternating rounds: remove a
                // shard of the current data dir right before the race.
                if round % 2 == 1 {
                    // The previous round's lock-owning PUT may still be
                    // draining its rename tail on this disk; shard damage is
                    // best-effort here, so skip injection when it lags.
                    if let Ok(current) = disks[2].read_version("", bucket, object, "", &ReadOptions::default()).await
                        && let Some(data_dir) = current.data_dir
                    {
                        let shard = temp_dirs[3]
                            .path()
                            .join(bucket)
                            .join(object)
                            .join(data_dir.to_string())
                            .join("part.1");
                        if shard.exists() {
                            tokio::fs::remove_file(&shard)
                                .await
                                .expect("shard damage should be injectable mid-race");
                        }
                    }
                }

                let payload = vec![round as u8; PAYLOAD_SIZE];
                let mut put_reader = PutObjReader::from_vec(payload);
                let put_opts = ObjectOptions::default();
                let heal_opts = HealOpts {
                    scan_mode: HealScanMode::Deep,
                    read_repair,
                    ..Default::default()
                };
                let (put_res, heal_res) = tokio::join!(
                    set.put_object(bucket, object, &mut put_reader, &put_opts),
                    set.heal_object(bucket, object, "", &heal_opts),
                );
                let put_info = put_res.expect("overwrite must succeed under lock serialization");
                last_etag = put_info.etag.clone().unwrap_or_default();
                // Heal outcome is unconstrained (may hit the tail race and report a
                // retryable error); the invariant is checked on the end state.
                drop(heal_res);
            }

            let final_info = set
                .get_object_info(bucket, object, &ObjectOptions::default())
                .await
                .expect("object must remain readable after the race loop");
            assert_eq!(
                final_info.size, PAYLOAD_SIZE as i64,
                "final current version must be the last committed overwrite"
            );
            assert_eq!(
                final_info.etag.unwrap_or_default(),
                last_etag,
                "the racing heal loop must never leave a stale or resurrected current version"
            );
        }
    }
}

#[cfg(test)]
mod shard_integrity_rollout_tests;

#[cfg(test)]
mod shard_identity_tests {
    use crate::disk::{DiskAPI as _, ReadOptions};
    use crate::object_api::{ObjectOptions, PutObjReader};
    use crate::set_disk::ops::object::hermetic_set_disks_support::hermetic_set_disks_for_pool_with_default_parity_isolated;
    use crate::storage_api_contracts::multipart::MultipartOperations as _;
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
    use crate::storage_api_contracts::range::HTTPRangeSpec;
    use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn shard_integrity_heal_rejects_complete_donor_shards_ec12_4() {
        let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(16, 0, 4).await;
        let bucket = "donor-shard-identity";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("create fixture bucket");
        }
        let expected = vec![0x5a; 5 * 1024 * 1024 + 123];
        let donor_body = vec![0xa6; expected.len()];
        let options = ObjectOptions {
            shard_integrity_write_mode: Some(crate::object_api::ShardIntegrityWriteMode::Protected),
            no_lock: true,
            versioned: true,
            ..Default::default()
        };
        for (case, (coding_indexes, replace_descriptor)) in [
            (vec![1], false),
            (vec![6], false),
            (vec![12], false),
            (vec![13], false),
            (vec![16], false),
            (vec![1, 2, 3, 4], false),
            (vec![1, 2, 3, 4, 5], false),
            (vec![1], true),
        ]
        .into_iter()
        .enumerate()
        {
            let target = format!("target-{case}");
            let donor = format!("donor-{case}");
            set.put_object(bucket, &target, &mut PutObjReader::from_vec(expected.clone()), &options)
                .await
                .expect("commit every target shard");
            set.put_object(bucket, &donor, &mut PutObjReader::from_vec(donor_body.clone()), &options)
                .await
                .expect("commit every donor shard");
            let mut target_meta = Vec::new();
            let mut donor_meta = Vec::new();
            for disk in &disks {
                target_meta.push(
                    disk.read_version("", bucket, &target, "", &ReadOptions::default())
                        .await
                        .expect("target metadata"),
                );
                donor_meta.push(
                    disk.read_version("", bucket, &donor, "", &ReadOptions::default())
                        .await
                        .expect("donor metadata"),
                );
            }
            assert_eq!((target_meta[0].erasure.data_blocks, target_meta[0].erasure.parity_blocks), (12, 4));
            let mut before = Vec::new();
            let mut paths = Vec::new();
            let mut meta_before = Vec::new();
            for (slot, meta) in target_meta.iter().enumerate() {
                let object_path = dirs[slot].path().join(bucket).join(&target);
                let path = object_path
                    .join(meta.data_dir.expect("external data dir").to_string())
                    .join("part.1");
                before.push(tokio::fs::read(&path).await.expect("original shard"));
                meta_before.push(tokio::fs::read(object_path.join("xl.meta")).await.expect("original xl.meta"));
                paths.push(path);
            }
            for index in &coding_indexes {
                let target_slot = target_meta
                    .iter()
                    .position(|m| m.erasure.index == *index)
                    .expect("target coding index");
                let donor_slot = donor_meta
                    .iter()
                    .position(|m| m.erasure.index == *index)
                    .expect("donor coding index");
                let path = dirs[donor_slot]
                    .path()
                    .join(bucket)
                    .join(&donor)
                    .join(donor_meta[donor_slot].data_dir.expect("donor data dir").to_string())
                    .join("part.1");
                let replacement = tokio::fs::read(path).await.expect("complete valid donor");
                assert_eq!(
                    replacement.len(),
                    before[target_slot].len(),
                    "the adversarial file must have the same physical length"
                );
                assert_ne!(replacement, before[target_slot]);
                tokio::fs::write(&paths[target_slot], replacement)
                    .await
                    .expect("replace the complete shard without changing metadata");
                if replace_descriptor {
                    let mut altered = target_meta[target_slot].clone();
                    let suffix = rustfs_filemeta::shard_integrity::SUFFIX_SHARD_INTEGRITY;
                    let descriptor = rustfs_utils::http::get_consistent_str(&donor_meta[donor_slot].metadata, suffix)
                        .expect("donor descriptor");
                    rustfs_utils::http::insert_str(&mut altered.metadata, suffix, descriptor.to_owned());
                    disks[target_slot]
                        .write_metadata("", bucket, &target, altered)
                        .await
                        .expect("a minority descriptor must not authorize its own donor payload");
                }
            }
            let version = target_meta[0].version_id.expect("versioned fixture").to_string();
            if coding_indexes.len() <= 4 {
                let mut reader = set
                    .get_object_reader(bucket, &target, None, Default::default(), &options)
                    .await
                    .expect("recoverable GET before explicit heal");
                let mut body = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut body)
                    .await
                    .expect("verify and reconstruct every GET block");
                assert_eq!(body, expected, "GET must not expose donor data before explicit heal");
            }
            let heal = set
                .heal_object(
                    bucket,
                    &target,
                    &version,
                    &HealOpts {
                        no_lock: true,
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await;
            if coding_indexes.len() <= 4 {
                let (result, error) = heal.expect("recoverable donor mismatch");
                assert!(error.is_none(), "{error:?}");
                assert_eq!(
                    result.drives_healed(),
                    Some(coding_indexes.len()),
                    "every complete donor must be detected"
                );
                for (slot, path) in paths.iter().enumerate() {
                    assert_eq!(
                        tokio::fs::read(path).await.expect("post-heal shard"),
                        before[slot],
                        "reconstruct exact original bytes at slot {slot}"
                    );
                }
                for (range, expected_range) in [
                    (None, expected.as_slice()),
                    (
                        Some(HTTPRangeSpec {
                            start: 1024 * 1024 + 17,
                            end: 2 * 1024 * 1024 + 99,
                            is_suffix_length: false,
                        }),
                        &expected[1024 * 1024 + 17..2 * 1024 * 1024 + 100],
                    ),
                ] {
                    let mut reader = set
                        .get_object_reader(
                            bucket,
                            &target,
                            range,
                            Default::default(),
                            &ObjectOptions {
                                version_id: Some(version.clone()),
                                ..options.clone()
                            },
                        )
                        .await
                        .expect("read repaired version");
                    let mut body = Vec::new();
                    reader
                        .stream
                        .read_to_end(&mut body)
                        .await
                        .expect("complete verified GET body");
                    assert_eq!(body, expected_range, "full and range GET must match original bytes");
                }
            } else {
                assert!(
                    heal.as_ref().map_or(true, |(_, error)| error.is_some()),
                    "eleven authoritative sources must not produce a successful heal"
                );
                let read = set
                    .get_object_reader(bucket, &target, None, Default::default(), &options)
                    .await;
                if let Ok(mut reader) = read {
                    let mut body = Vec::new();
                    assert!(reader.stream.read_to_end(&mut body).await.is_err(), "quorum-minus-one must fail the body");
                    assert!(body.is_empty(), "unverified first-stripe bytes must not escape");
                }
                for (slot, meta) in target_meta.iter().enumerate() {
                    if !coding_indexes.contains(&meta.erasure.index) {
                        assert_eq!(tokio::fs::read(&paths[slot]).await.expect("retained correct shard"), before[slot]);
                    }
                    assert_eq!(
                        tokio::fs::read(dirs[slot].path().join(bucket).join(&target).join("xl.meta"))
                            .await
                            .expect("retained metadata"),
                        meta_before[slot]
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn shard_integrity_inline_donor_repair_and_late_external_get() {
        let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
        let bucket = "bound-inline-and-late";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("fixture bucket");
        }
        let options = ObjectOptions {
            shard_integrity_write_mode: Some(crate::object_api::ShardIntegrityWriteMode::Protected),
            no_lock: true,
            ..Default::default()
        };
        let read_options = ReadOptions {
            read_data: true,
            ..Default::default()
        };
        for (size, late) in [(4113usize, false), (3 * 1024 * 1024 + 123, true)] {
            let target = format!("target-{size}");
            let donor = format!("donor-{size}");
            let expected = vec![0x4d; size];
            set.put_object(bucket, &target, &mut PutObjReader::from_vec(expected.clone()), &options)
                .await
                .expect("target PUT");
            set.put_object(bucket, &donor, &mut PutObjReader::from_vec(vec![0x9a; size]), &options)
                .await
                .expect("donor PUT");
            let mut target_parts = Vec::new();
            let mut donor_parts = Vec::new();
            for disk in &disks {
                target_parts.push(
                    disk.read_version("", bucket, &target, "", &read_options)
                        .await
                        .expect("target meta"),
                );
                donor_parts.push(
                    disk.read_version("", bucket, &donor, "", &read_options)
                        .await
                        .expect("donor meta"),
                );
            }
            let slot = target_parts
                .iter()
                .position(|fi| fi.erasure.index == 1)
                .expect("target data slot");
            let donor_slot = donor_parts
                .iter()
                .position(|fi| fi.erasure.index == 1)
                .expect("donor data slot");
            if !late {
                let original = target_parts[slot].data.clone().expect("inline target");
                let donor_bytes = donor_parts[donor_slot].data.as_ref().expect("inline donor");
                assert_eq!(donor_bytes.len(), original.len());
                let path = dirs[slot].path().join(bucket).join(&target).join("xl.meta");
                let mut raw = tokio::fs::read(&path).await.expect("physical inline metadata");
                let offsets: Vec<_> = raw
                    .windows(original.len())
                    .enumerate()
                    .filter_map(|(offset, bytes)| (bytes == original.as_ref()).then_some(offset))
                    .collect();
                assert_eq!(offsets.len(), 1, "unique inline payload range");
                raw[offsets[0]..offsets[0] + original.len()].copy_from_slice(donor_bytes);
                tokio::fs::write(path, raw)
                    .await
                    .expect("replace inline bytes with target identity retained");
                let (result, error) = set
                    .heal_object(
                        bucket,
                        &target,
                        "",
                        &HealOpts {
                            no_lock: true,
                            scan_mode: HealScanMode::Deep,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("inline heal");
                assert!(error.is_none(), "{error:?}");
                assert_eq!(result.drives_healed(), Some(1));
                let repaired = disks[slot]
                    .read_version("", bucket, &target, "", &read_options)
                    .await
                    .expect("repaired inline");
                assert_eq!(repaired.data, Some(original));
            } else {
                let target_path = dirs[slot]
                    .path()
                    .join(bucket)
                    .join(&target)
                    .join(target_parts[slot].data_dir.expect("target dir").to_string())
                    .join("part.1");
                let donor_path = dirs[donor_slot]
                    .path()
                    .join(bucket)
                    .join(&donor)
                    .join(donor_parts[donor_slot].data_dir.expect("donor dir").to_string())
                    .join("part.1");
                let mut bytes = tokio::fs::read(&target_path).await.expect("target bytes");
                let donor_bytes = tokio::fs::read(donor_path).await.expect("donor bytes");
                let frame = 32 + target_parts[slot].erasure.shard_size();
                bytes[frame..2 * frame].copy_from_slice(&donor_bytes[frame..2 * frame]);
                tokio::fs::write(target_path, bytes)
                    .await
                    .expect("replace a later complete frame");
            }
            let mut reader = set
                .get_object_reader(bucket, &target, None, Default::default(), &options)
                .await
                .expect("GET after corruption");
            let mut body = Vec::new();
            reader
                .stream
                .read_to_end(&mut body)
                .await
                .expect("late corruption must reconstruct without leaking donor bytes");
            assert_eq!(body, expected);
        }
    }

    #[tokio::test]
    async fn shard_integrity_legacy_requires_target_digest_not_only_parity_consistency() {
        let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
        let bucket = "legacy-shard-authority";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("fixture bucket");
        }
        let options = ObjectOptions {
            shard_integrity_write_mode: Some(crate::object_api::ShardIntegrityWriteMode::Protected),
            no_lock: true,
            ..Default::default()
        };
        let expected = vec![0x4d; 1024 * 1024 + 123];
        let mut parts = Vec::new();
        let mut paths = Vec::new();
        for (object, body) in [("target", expected.clone()), ("donor", vec![0x9a; expected.len()])] {
            set.put_object(bucket, object, &mut PutObjReader::from_vec(body), &options)
                .await
                .expect("seed legacy fixture");
            let mut object_parts = Vec::new();
            let mut object_paths = Vec::new();
            for (slot, disk) in disks.iter().enumerate() {
                let mut fi = disk
                    .read_version("", bucket, object, "", &ReadOptions::default())
                    .await
                    .expect("source meta");
                let path = dirs[slot]
                    .path()
                    .join(bucket)
                    .join(object)
                    .join(fi.data_dir.expect("data dir").to_string())
                    .join("part.1");
                let encoded = tokio::fs::read(&path).await.expect("encoded source");
                let mut legacy = Vec::with_capacity(encoded.len());
                for frame in encoded.chunks(32 + fi.erasure.shard_size()) {
                    let payload = &frame[32..];
                    legacy.extend_from_slice(rustfs_utils::HashAlgorithm::HighwayHash256S.hash_encode(payload).as_ref());
                    legacy.extend_from_slice(payload);
                }
                rustfs_filemeta::shard_integrity::clear_integrity_metadata(&mut fi.metadata);
                for part in &mut fi.parts {
                    part.integrity = None;
                }
                tokio::fs::write(&path, legacy).await.expect("legacy framing");
                disk.write_metadata("", bucket, object, fi.clone())
                    .await
                    .expect("legacy metadata");
                object_parts.push(fi);
                object_paths.push(path);
            }
            parts.push(object_parts);
            paths.push(object_paths);
        }
        let mut reader = set
            .get_object_reader(bucket, "target", None, Default::default(), &options)
            .await
            .expect("healthy legacy object remains readable");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("legacy body");
        assert_eq!(body, expected);
        for (slot, fi) in parts[0].iter().enumerate() {
            let source = parts[1]
                .iter()
                .position(|donor| donor.erasure.index == fi.erasure.index)
                .expect("coding index");
            tokio::fs::copy(&paths[1][source], &paths[0][slot])
                .await
                .expect("install a complete parity-consistent foreign codeword");
        }
        // Compatibility keeps legacy GET semantics. A complete foreign codeword
        // remains an explicit residual risk until a trusted source rewrites it.
        let mut legacy = set
            .get_object_reader(bucket, "target", None, Default::default(), &options)
            .await
            .expect("legacy GET remains available");
        let mut foreign = Vec::new();
        legacy
            .stream
            .read_to_end(&mut foreign)
            .await
            .expect("legacy donor is internally consistent");
        assert_ne!(foreign, expected);
        for scan_mode in [HealScanMode::Normal, HealScanMode::Deep] {
            let heal = set
                .heal_object(
                    bucket,
                    "target",
                    "",
                    &HealOpts {
                        no_lock: true,
                        scan_mode,
                        ..Default::default()
                    },
                )
                .await;
            let (result, error) = heal.expect("legacy scan");
            assert!(error.is_none(), "legacy scan remains observable");
            assert!(!result.integrity_verified, "a foreign codeword must not be certified");
            assert!(result.legacy_identity_unverified, "format classification is not an integrity proof");
            assert_eq!(result.drives_healed(), Some(0), "legacy scan must not rewrite data");
        }
    }

    #[tokio::test]
    async fn shard_integrity_multipart_replacement_and_metadata_copy_preserve_identity() {
        use crate::set_disk::{CompletePart, RUSTFS_META_MULTIPART_BUCKET, SetDisks};
        use rustfs_filemeta::ObjectPartInfo;

        let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
        let bucket = "bound-multipart-copy";
        let object = "target";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("fixture bucket");
        }
        let options = ObjectOptions {
            shard_integrity_write_mode: Some(crate::object_api::ShardIntegrityWriteMode::Protected),
            no_lock: true,
            versioned: true,
            ..Default::default()
        };
        let upload = set.new_multipart_upload(bucket, object, &options).await.expect("new upload");
        let (upload_meta, _) = set
            .check_upload_id_exists(bucket, object, &upload.upload_id, false)
            .await
            .expect("upload metadata");
        let upload_path = SetDisks::get_multipart_upload_dir(bucket, object, &upload.upload_id, false);
        let part_path = dirs[0]
            .path()
            .join(RUSTFS_META_MULTIPART_BUCKET)
            .join(upload_path)
            .join(upload_meta.data_dir.expect("staging directory").to_string())
            .join("part.2");
        let first = vec![0x36; 5 * 1024 * 1024];
        let second = vec![0x69; 1024 * 1024 + 123];
        let p1 = set
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut PutObjReader::from_vec(first.clone()), &options)
            .await
            .expect("first part");
        set.put_object_part(
            bucket,
            object,
            &upload.upload_id,
            2,
            &mut PutObjReader::from_vec(vec![0xa5; second.len()]),
            &options,
        )
        .await
        .expect("original second part");
        let old_frame = tokio::fs::read(&part_path).await.expect("old part shard");
        let old_meta: ObjectPartInfo = rmp_serde::from_slice(
            &tokio::fs::read(part_path.with_extension("2.meta"))
                .await
                .expect("old part descriptor"),
        )
        .expect("decode old part descriptor");
        let p2 = set
            .put_object_part(
                bucket,
                object,
                &upload.upload_id,
                2,
                &mut PutObjReader::from_vec(second.clone()),
                &options,
            )
            .await
            .expect("replace second part");
        let mut completed = set
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload.upload_id,
                vec![
                    CompletePart {
                        part_num: p1.part_num,
                        etag: p1.etag,
                        ..Default::default()
                    },
                    CompletePart {
                        part_num: p2.part_num,
                        etag: p2.etag,
                        ..Default::default()
                    },
                ],
                &options,
            )
            .await
            .expect("complete replacement");
        let original = disks[0]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("completed descriptor");
        assert_ne!(
            original.parts[1].integrity.as_ref().map(|proof| proof.generation),
            old_meta.integrity.as_ref().map(|proof| proof.generation)
        );
        assert_ne!(
            original.parts[0].integrity.as_ref().map(|proof| proof.generation),
            original.parts[1].integrity.as_ref().map(|proof| proof.generation)
        );
        let final_path = dirs[0]
            .path()
            .join(bucket)
            .join(object)
            .join(original.data_dir.expect("completed directory").to_string())
            .join("part.2");
        assert_eq!(old_frame.len(), tokio::fs::metadata(&final_path).await.unwrap().len() as usize);
        tokio::fs::write(final_path, old_frame)
            .await
            .expect("replay replaced part shard");
        let (result, error) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("heal replayed part");
        assert!(error.is_none(), "{error:?}");
        assert_eq!(result.drives_healed(), Some(1));

        // REPLACE may omit every internal field; metadata-only COPY must keep
        // the existing payload domain even when it creates another version.
        completed.metadata_only = true;
        completed.user_defined = std::sync::Arc::new(std::collections::HashMap::from([(
            "x-amz-meta-label".to_string(),
            "replacement".to_string(),
        )]));
        set.copy_object(bucket, object, bucket, object, &mut completed, &options, &options)
            .await
            .expect("metadata replacement copy");
        let copied = disks[0]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("copied descriptor");
        for part in [1, 2] {
            assert_eq!(copied.parts[part - 1].integrity, original.parts[part - 1].integrity);
        }
        let expected = [first, second].concat();
        for version in [None, original.version_id.map(|id| id.to_string())] {
            for range in [
                None,
                Some(HTTPRangeSpec {
                    start: 5 * 1024 * 1024 - 17,
                    end: 5 * 1024 * 1024 + 99,
                    is_suffix_length: false,
                }),
            ] {
                let expected_bytes = if range.is_some() {
                    &expected[5 * 1024 * 1024 - 17..5 * 1024 * 1024 + 100]
                } else {
                    &expected[..]
                };
                let mut reader = set
                    .get_object_reader(
                        bucket,
                        object,
                        range,
                        Default::default(),
                        &ObjectOptions {
                            version_id: version.clone(),
                            ..options.clone()
                        },
                    )
                    .await
                    .expect("multipart GET");
                let mut actual = Vec::new();
                reader.stream.read_to_end(&mut actual).await.expect("verified multipart body");
                assert_eq!(actual, expected_bytes);
            }
        }
    }
    #[tokio::test]
    async fn shard_integrity_index_repair_preserves_payload_and_metadata() {
        let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
        let bucket = "integrity-index-repair";
        let object = "target";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket");
        }
        let expected = vec![0x6a; 2 * 1024 * 1024 + 123];
        let opts = ObjectOptions {
            shard_integrity_write_mode: Some(crate::object_api::ShardIntegrityWriteMode::Protected),
            no_lock: true,
            ..Default::default()
        };
        set.put_object(bucket, object, &mut PutObjReader::from_vec(expected.clone()), &opts)
            .await
            .expect("PUT");
        let mut snapshots = Vec::new();
        for (dir, disk) in dirs.iter().zip(&disks) {
            let info = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("metadata");
            let root = dir.path().join(bucket).join(object);
            let data = root.join(info.data_dir.expect("data dir").to_string());
            let proof = data.join(info.parts[0].integrity.as_ref().expect("commitment").file_name());
            snapshots.push((
                data.join("part.1"),
                tokio::fs::read(data.join("part.1")).await.expect("payload"),
                root.join("xl.meta"),
                tokio::fs::read(root.join("xl.meta")).await.expect("xl.meta"),
                proof.clone(),
                tokio::fs::read(proof).await.expect("index"),
            ));
        }
        tokio::fs::remove_file(&snapshots[0].4).await.expect("missing index");
        let mut bad = snapshots[1].5.clone();
        bad[70] ^= 1;
        tokio::fs::write(&snapshots[1].4, bad).await.expect("corrupt index");
        let mut reader = set
            .get_object_reader(bucket, object, None, Default::default(), &opts)
            .await
            .expect("GET with proof fallback");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("verified fallback body");
        assert_eq!(body, expected);
        let (healed, error) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("index repair");
        assert!(error.is_none(), "{error:?}");
        assert!(healed.integrity_verified);
        for ((path, payload, meta_path, meta, proof_path, proof), (disk, dir)) in snapshots.iter().zip(disks.iter().zip(&dirs)) {
            assert_eq!(tokio::fs::read(path).await.expect("payload"), *payload);
            assert_eq!(tokio::fs::read(meta_path).await.expect("metadata"), *meta);
            assert_eq!(tokio::fs::read(proof_path).await.expect("restored proof"), *proof);
            let relative = proof_path.strip_prefix(dir.path().join(bucket)).expect("relative proof path");
            disk.delete(bucket, relative.to_str().expect("proof path"), crate::disk::DeleteOptions::default())
                .await
                .expect("lose all independent proofs");
        }
        // All missing proofs cannot be recreated by trusting the surviving
        // payload's self-contained bitrot checksum.
        if let Ok(mut reader) = set.get_object_reader(bucket, object, None, Default::default(), &opts).await {
            let mut body = Vec::new();
            assert!(reader.stream.read_to_end(&mut body).await.is_err());
        }
        if let Ok((result, error)) = set
            .heal_object(
                bucket,
                object,
                "",
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
        {
            assert!(error.is_some() || !result.integrity_verified);
        }
        for (path, payload, meta_path, meta, proof_path, _) in snapshots {
            assert_eq!(tokio::fs::read(path).await.expect("retained payload"), payload);
            assert_eq!(tokio::fs::read(meta_path).await.expect("retained metadata"), meta);
            assert!(!proof_path.exists(), "no proof minted from unverified payload");
        }
    }
    #[tokio::test]
    async fn shard_integrity_empty_and_no_parity_objects_remain_usable() {
        for parity in [0, 2] {
            let (_dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, parity).await;
            let bucket = "integrity-empty-no-parity";
            for disk in &disks {
                disk.make_volume(bucket).await.expect("bucket");
            }
            for size in [0, 1, 1024 * 1024 + 123] {
                let object = format!("target-{size}");
                let expected = vec![0x51; size];
                let opts = ObjectOptions {
                    shard_integrity_write_mode: Some(crate::object_api::ShardIntegrityWriteMode::Protected),
                    no_lock: true,
                    ..Default::default()
                };
                set.put_object(bucket, &object, &mut PutObjReader::from_vec(expected.clone()), &opts)
                    .await
                    .expect("PUT");
                let mut reader = set
                    .get_object_reader(bucket, &object, None, Default::default(), &opts)
                    .await
                    .expect("GET");
                let mut body = Vec::new();
                reader.stream.read_to_end(&mut body).await.expect("body");
                assert_eq!(body, expected);
                let (result, error) = set
                    .heal_object(
                        bucket,
                        &object,
                        "",
                        &HealOpts {
                            no_lock: true,
                            scan_mode: HealScanMode::Deep,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("Deep scan");
                assert!(error.is_none(), "{parity} {size}: {error:?}");
                assert!(result.integrity_verified);
            }
        }
    }
}
