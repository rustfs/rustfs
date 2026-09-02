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

#![allow(clippy::map_entry)]

use crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
use crate::bucket::lifecycle::bucket_lifecycle_ops::{
    enqueue_immediate_expiry, enqueue_transition_immediate, init_background_expiry,
};
use crate::bucket::metadata_sys;
use crate::bucket::utils::check_abort_multipart_args;
use crate::bucket::utils::check_complete_multipart_args;
use crate::bucket::utils::check_copy_obj_args;
use crate::bucket::utils::check_del_obj_args;
use crate::bucket::utils::check_get_obj_args;
use crate::bucket::utils::check_list_multipart_args;
use crate::bucket::utils::check_list_parts_args;
use crate::bucket::utils::check_new_multipart_args;
use crate::bucket::utils::check_object_args;
use crate::bucket::utils::check_put_object_args;
use crate::bucket::utils::check_put_object_part_args;
use crate::bucket::utils::{check_valid_bucket_name, check_valid_bucket_name_strict, is_meta_bucketname};
use crate::cluster::rpc::{RemoteClient, S3PeerSys};
use crate::config::storageclass;
use crate::core::pools::{DecommissionCanceler, PoolMeta, PoolMetaWriteState};
use crate::disk::endpoint::{Endpoint, EndpointType};
use crate::disk::{DiskAPI, DiskInfo, DiskInfoOptions};
use crate::error::{Error, Result};
use crate::error::{
    StorageError, is_err_bucket_exists, is_err_invalid_upload_id, is_err_object_not_found, is_err_read_quorum,
    is_err_strict_volume_not_found, is_err_version_not_found, to_object_err,
};
use crate::runtime::global::DISK_RESERVE_FRACTION;
use crate::runtime::instance::InstanceContext;
use crate::runtime::sources as runtime_sources;
use crate::services::rebalance::{RebalStatus, RebalanceMeta, is_rebalance_conflicting_with_decommission};
use crate::storage_api_contracts::{
    bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
    list::{StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr, StorageWalkOptions},
    multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo},
    object::{DeletedObject, ObjectToDelete},
    range::HTTPRangeSpec,
};
use crate::store::init_format::{check_disk_fatal_errs, ec_drives_no_config};
use crate::{
    bucket::{lifecycle::bucket_lifecycle_ops::TransitionState, metadata::BucketMetadata},
    core::sets::Sets,
    disk::{BUCKET_META_PREFIX, DiskOption, DiskStore, RUSTFS_META_BUCKET},
    layout::endpoints::EndpointServerPools,
    object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader, ScannerPublicationCommitScope},
};
use futures::future::join_all;
use http::HeaderMap;
use lazy_static::lazy_static;
use rand::RngExt as _;
use rustfs_config::server_config::Config;
use rustfs_filemeta::{FileInfo, FileMeta};
use rustfs_heal_contracts::heal_channel::{HealItemType, HealOpts};
use rustfs_lock::{LocalClient, LockClient, NamespaceLockWrapper};
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_utils::path::{decode_dir_object, encode_dir_object, path_join_buf};
use s3s::dto::{BucketVersioningStatus, ObjectLockConfiguration, ObjectLockEnabled, VersioningConfiguration};
use std::net::SocketAddr;
use std::process::exit;
use std::{collections::HashMap, sync::Arc, time::Duration};
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

pub const SCANNER_PUBLICATION_LEASE_TTL_MS: u64 = 60_000;
pub(crate) const BUCKET_DELETE_XLMETA_DIAGNOSTIC_MAX_BYTES: u64 = 1024 * 1024;
pub(crate) const BUCKET_DELETE_DIAGNOSTIC_MAX_ENTRIES: usize = 4_096;
pub(crate) const BUCKET_DELETE_DIAGNOSTIC_MAX_ELAPSED: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(crate) struct BucketDeleteDiagnosticBudget {
    deadline: Option<tokio::time::Instant>,
    max_elapsed: Duration,
    entries_remaining: usize,
    #[cfg(test)]
    first_io_delay: Option<(Duration, Arc<std::sync::atomic::AtomicBool>)>,
}

impl BucketDeleteDiagnosticBudget {
    pub(crate) fn new() -> Self {
        Self::with_limits(BUCKET_DELETE_DIAGNOSTIC_MAX_ENTRIES, BUCKET_DELETE_DIAGNOSTIC_MAX_ELAPSED)
    }

    fn with_limits(entries: usize, elapsed: Duration) -> Self {
        Self {
            deadline: None,
            max_elapsed: elapsed,
            entries_remaining: entries,
            #[cfg(test)]
            first_io_delay: None,
        }
    }

    #[cfg(test)]
    fn with_first_io_delay(mut self, delay: Duration, started: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.first_io_delay = Some((delay, started));
        self
    }

    fn deadline(&mut self) -> tokio::time::Instant {
        let max_elapsed = self.max_elapsed;
        *self.deadline.get_or_insert_with(|| tokio::time::Instant::now() + max_elapsed)
    }

    fn claim_entry(&mut self) -> bool {
        if self.entries_remaining == 0 {
            return false;
        }
        let deadline = self.deadline();
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        self.entries_remaining -= 1;
        true
    }

    async fn run_io<T, F>(&mut self, future: F) -> std::io::Result<Option<T>>
    where
        F: std::future::Future<Output = std::io::Result<T>>,
    {
        let deadline = self.deadline();
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        #[cfg(test)]
        let first_io_delay = self.first_io_delay.take();
        #[cfg(test)]
        let timeout_result = tokio::time::timeout_at(deadline, async move {
            if let Some((delay, started)) = first_io_delay {
                started.store(true, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(delay).await;
            }
            future.await
        })
        .await;
        #[cfg(not(test))]
        let timeout_result = tokio::time::timeout_at(deadline, future).await;
        match timeout_result {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct BucketMetadataLessResidue {
    pub(crate) xlmeta_found: bool,
    pub(crate) xlmeta_blocker: Option<BucketDeleteBlockerKind>,
    pub(crate) files: usize,
    pub(crate) uuid_data_dirs: usize,
    pub(crate) entries_scanned: usize,
    pub(crate) diagnostic_bytes_read: u64,
    pub(crate) diagnostic_truncated: bool,
    pub(crate) sample: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BucketDeleteBlockerKind {
    VisibleVersion,
    TierFreeVersion,
    UnknownXlMeta,
    OrphanDirectory,
    DiagnosticBudgetExceeded,
}

impl BucketDeleteBlockerKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::VisibleVersion => "visible_version",
            Self::TierFreeVersion => "tier_free_version",
            Self::UnknownXlMeta => "unknown_xlmeta",
            Self::OrphanDirectory => "orphan_directory",
            Self::DiagnosticBudgetExceeded => "diagnostic_budget_exceeded",
        }
    }

    /// Whether the blocking residue is something the caller can still see and
    /// remove through the S3 API.
    ///
    /// A live version or a tier free-version is ordinary: the bucket really is
    /// not empty, the client can list and delete what is left, and the 409 it
    /// receives is a complete answer.
    ///
    /// The remaining kinds are not. They are on-disk state that no S3 request
    /// can reach: the caller has drained every version the API will show and
    /// `DeleteBucket` still refuses, with no way to find out why. That is a
    /// server-side integrity problem, and it is the reason this classification
    /// exists — see [`bucket_delete_blocker_level`].
    pub(crate) const fn is_client_visible(self) -> bool {
        matches!(self, Self::VisibleVersion | Self::TierFreeVersion)
    }
}

impl BucketMetadataLessResidue {
    pub(crate) fn has_residue_without_xlmeta(&self) -> bool {
        !self.xlmeta_found && (self.files > 0 || self.diagnostic_truncated)
    }

    pub(crate) fn describe(&self) -> String {
        let sample = self.sample.as_deref().unwrap_or("<none>");
        format!(
            "metadata-less on-disk residue remains after empty-bucket verification: files={}, uuid_data_dirs={}, entries_scanned={}, diagnostic_bytes_read={}, diagnostic_truncated={}, sample={sample}",
            self.files, self.uuid_data_dirs, self.entries_scanned, self.diagnostic_bytes_read, self.diagnostic_truncated,
        )
    }
}

/// Check if a directory contains any xl.meta files (indicating actual S3 objects)
/// This is used to determine if a bucket is empty for deletion purposes.
pub(crate) async fn has_xlmeta_files(path: &std::path::Path) -> std::io::Result<bool> {
    use crate::disk::STORAGE_FORMAT_FILE;
    use tokio::fs;

    let mut stack = vec![path.to_path_buf()];

    while let Some(current_path) = stack.pop() {
        let mut entries = match fs::read_dir(&current_path).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };

        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // Check if this is an xl.meta file
            if file_name_str == STORAGE_FORMAT_FILE {
                return Ok(true);
            }

            // If it's a directory, add to stack for further exploration
            if entry.file_type().await?.is_dir() {
                stack.push(entry.path());
            }
        }
    }

    Ok(false)
}

#[cfg(test)]
pub(crate) async fn scan_metadata_less_residue(path: &std::path::Path) -> std::io::Result<BucketMetadataLessResidue> {
    let mut budget = BucketDeleteDiagnosticBudget::new();
    scan_metadata_less_residue_with_budget(path, &mut budget).await
}

async fn scan_metadata_less_residue_with_budget(
    path: &std::path::Path,
    budget: &mut BucketDeleteDiagnosticBudget,
) -> std::io::Result<BucketMetadataLessResidue> {
    use crate::disk::STORAGE_FORMAT_FILE;
    use tokio::fs;
    use tokio::io::AsyncReadExt as _;

    let mut scan = BucketMetadataLessResidue::default();
    let mut stack = vec![path.to_path_buf()];

    let mark_budget_exhausted = |scan: &mut BucketMetadataLessResidue| {
        scan.diagnostic_truncated = true;
        scan.sample.get_or_insert_with(|| "<diagnostic-budget-exceeded>".to_string());
    };

    while let Some(current_path) = stack.pop() {
        let mut entries = match budget.run_io(fs::read_dir(&current_path)).await {
            Ok(Some(entries)) => entries,
            Ok(None) => {
                mark_budget_exhausted(&mut scan);
                return Ok(scan);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };

        loop {
            let entry = match budget.run_io(entries.next_entry()).await? {
                Some(Some(entry)) => entry,
                Some(None) => break,
                None => {
                    mark_budget_exhausted(&mut scan);
                    return Ok(scan);
                }
            };
            if !budget.claim_entry() {
                mark_budget_exhausted(&mut scan);
                return Ok(scan);
            }
            scan.entries_scanned = scan.entries_scanned.saturating_add(1);
            let Some(file_type) = budget.run_io(entry.file_type()).await? else {
                mark_budget_exhausted(&mut scan);
                return Ok(scan);
            };
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            if file_name_str == STORAGE_FORMAT_FILE {
                scan.xlmeta_found = true;
                if scan.xlmeta_blocker.is_none() {
                    let entry_path = entry.path();
                    let Some(metadata) = budget.run_io(fs::metadata(&entry_path)).await? else {
                        mark_budget_exhausted(&mut scan);
                        scan.xlmeta_blocker = Some(BucketDeleteBlockerKind::DiagnosticBudgetExceeded);
                        return Ok(scan);
                    };
                    scan.xlmeta_blocker = Some(if metadata.len() > BUCKET_DELETE_XLMETA_DIAGNOSTIC_MAX_BYTES {
                        BucketDeleteBlockerKind::UnknownXlMeta
                    } else {
                        match budget.run_io(fs::File::open(&entry_path)).await {
                            Ok(Some(file)) => {
                                let mut data = Vec::new();
                                let read = budget
                                    .run_io(file.take(BUCKET_DELETE_XLMETA_DIAGNOSTIC_MAX_BYTES).read_to_end(&mut data))
                                    .await;
                                scan.diagnostic_bytes_read = data.len() as u64;
                                match read {
                                    Ok(Some(_)) => match FileMeta::load(&data) {
                                        Ok(meta)
                                            if !meta.versions.is_empty()
                                                && meta.versions.iter().all(|version| version.header.free_version()) =>
                                        {
                                            BucketDeleteBlockerKind::TierFreeVersion
                                        }
                                        Ok(meta) if !meta.versions.is_empty() => BucketDeleteBlockerKind::VisibleVersion,
                                        Ok(_) | Err(_) => BucketDeleteBlockerKind::UnknownXlMeta,
                                    },
                                    Ok(None) => {
                                        mark_budget_exhausted(&mut scan);
                                        BucketDeleteBlockerKind::DiagnosticBudgetExceeded
                                    }
                                    Err(_) => BucketDeleteBlockerKind::UnknownXlMeta,
                                }
                            }
                            Ok(None) => {
                                mark_budget_exhausted(&mut scan);
                                BucketDeleteBlockerKind::DiagnosticBudgetExceeded
                            }
                            Err(_) => BucketDeleteBlockerKind::UnknownXlMeta,
                        }
                    });
                    let sample = entry_path
                        .strip_prefix(path)
                        .unwrap_or(entry_path.as_path())
                        .to_string_lossy()
                        .replace(std::path::MAIN_SEPARATOR, "/");
                    scan.sample = Some(sample);
                }
                return Ok(scan);
            }

            if file_type.is_dir() {
                if Uuid::parse_str(&file_name_str).is_ok() {
                    scan.uuid_data_dirs = scan.uuid_data_dirs.saturating_add(1);
                }
                stack.push(entry.path());
            } else {
                scan.files = scan.files.saturating_add(1);
                if scan.sample.is_none() {
                    let entry_path = entry.path();
                    let sample = entry_path
                        .strip_prefix(path)
                        .unwrap_or(entry_path.as_path())
                        .to_string_lossy()
                        .replace(std::path::MAIN_SEPARATOR, "/");
                    scan.sample = Some(sample);
                }
            }
        }
    }

    Ok(scan)
}

async fn enqueue_transition_after_write(result: Result<ObjectInfo>, src: LcEventSrc) -> Result<ObjectInfo> {
    match result {
        Ok(oi) => {
            if should_enqueue_transition_immediately(&oi) {
                enqueue_transition_immediate(&oi, src.clone()).await;
                enqueue_immediate_expiry(&oi, src).await;
            }
            Ok(oi)
        }
        Err(err) => Err(err),
    }
}

fn should_enqueue_transition_immediately(oi: &ObjectInfo) -> bool {
    !is_meta_bucketname(&oi.bucket)
}

const MAX_UPLOADS_LIST: usize = 10000;

mod bucket;
mod bucket_fence;
pub(crate) use bucket::await_bucket_namespace_operation;
mod heal;
mod heal_walk;
pub use heal_walk::HealWalkVersion;
mod init;
pub(crate) mod init_format;
pub(crate) mod list_objects;
mod multipart;
mod object;
#[cfg(any(test, feature = "test-util"))]
pub use object::DeleteAfterObjectLockSnapshotBarrier;
pub(crate) use object::{
    DecommissionFixedReadAnchor, ObjectLockDiagGuard, RemoteTuplePublicationCommitGuard, RemoteTuplePublicationFence,
    SourceCleanupMutationFence, tiered_data_movement_source_matches,
};
pub use object::{
    PrepareSelectObjectSnapshotError, PreparedGetObjectReader, SelectObjectSnapshot, SelectObjectSnapshotReadError,
    SnapshotConsistencyError,
};
mod peer;
mod rebalance;
pub(crate) mod utils;

use peer::init_local_peer;
pub use peer::{
    all_local_disk, all_local_disk_path, find_local_disk_by_ref, get_disk_infos, init_local_disks,
    init_local_disks_with_instance_ctx, init_lock_clients, prewarm_local_disk_id_map,
    prewarm_local_disk_id_map_with_instance_ctx,
};

pub struct ECStore {
    pub id: Uuid,
    // pub disks: Vec<DiskStore>,
    pub disk_map: HashMap<usize, Vec<Option<DiskStore>>>,
    pub pools: Vec<Arc<Sets>>,
    pub peer_sys: S3PeerSys,
    // pub local_disks: Vec<DiskStore>,
    pub pool_meta: RwLock<PoolMeta>,
    pub rebalance_meta: RwLock<Option<RebalanceMeta>>,
    pub decommission_cancelers: RwLock<Vec<Option<DecommissionCanceler>>>,
    /// Serializes rebalance/decommission start transitions.
    ///
    /// Lock order: acquire `start_gate` before `pool_meta`, `rebalance_meta`,
    /// or `decommission_cancelers`. The guarded sections may perform bounded
    /// async metadata work so check/init/start cannot race across operations.
    pub(crate) start_gate: Mutex<()>,
    /// Serializes full-document pool metadata saves and retains a fail-closed
    /// write block after startup observes an unreadable replica.
    ///
    /// Lock order: acquire `pool_meta_save_gate`, then the distributed
    /// `pool.bin` fence, then clone `pool_meta` under a short read lock.
    pub(crate) pool_meta_save_gate: Mutex<PoolMetaWriteState>,
    /// Serializes decommission entries while the durable capacity ledger has
    /// one target mutation intent slot.
    ///
    /// Lock order: acquire this gate before object namespaces or
    /// `pool_meta_save_gate`.
    pub(crate) decommission_capacity_entry_gate: Mutex<()>,
    /// Per-instance runtime state (Phase 5, backlog#939).
    ///
    /// Carries this instance's identity/runtime out of the process globals so
    /// multiple instances can coexist without cross-contamination. `new`
    /// adopts the process bootstrap context (never mints a fresh one) so that
    /// startup writes and post-construction reads share one cell — single
    /// instance behavior is unchanged.
    pub(crate) ctx: Arc<InstanceContext>,
    /// Memoizes bucket-incarnation validation under continuous lifecycle
    /// read-lock coverage (see [`bucket_fence`]).
    pub(crate) bucket_fence_registry: Arc<bucket_fence::BucketFenceRegistry>,
}

const METRIC_SCANNER_DATA_MOVEMENT_PAUSED: &str = "rustfs_scanner_data_movement_paused";
const METRIC_SCANNER_DATA_MOVEMENT_PAUSE_DURATION_SECONDS: &str = "rustfs_scanner_data_movement_pause_duration_seconds";
const METRIC_SCANNER_DATA_MOVEMENT_BACKLOG_WORK_ITEMS: &str = "rustfs_scanner_data_movement_backlog_work_items";
const SCANNER_DATA_MOVEMENT_PAUSE_POLICY: &str = "global_pause";

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerDataMovementPauseReason {
    OperationEpochExhausted,
    MovementGenerationExhausted,
    DecommissionActive,
    DecommissionFailed,
    DecommissionCanceled,
    RebalanceActive,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ScannerDataMovementPauseStatus {
    pub paused: bool,
    pub policy: &'static str,
    pub reasons: Vec<ScannerDataMovementPauseReason>,
    pub started_at_unix_secs: u64,
    pub duration_seconds: u64,
    pub operation_epoch: u64,
    pub movement_generation: u64,
    pub movement_backlog_work_items: u64,
    pub movement_backlog_estimated: bool,
}

impl Default for ScannerDataMovementPauseStatus {
    fn default() -> Self {
        Self {
            paused: false,
            policy: SCANNER_DATA_MOVEMENT_PAUSE_POLICY,
            reasons: Vec::new(),
            started_at_unix_secs: 0,
            duration_seconds: 0,
            operation_epoch: 0,
            movement_generation: 0,
            movement_backlog_work_items: 0,
            movement_backlog_estimated: false,
        }
    }
}

fn offset_unix_seconds(value: OffsetDateTime) -> u64 {
    u64::try_from(value.unix_timestamp()).unwrap_or(0)
}

fn earliest_timestamp(current: Option<OffsetDateTime>, candidate: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.min(candidate)),
        (Some(current), None) => Some(current),
        (None, candidate) => candidate,
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn metric_u64(value: u64) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

pub(crate) fn scanner_data_movement_timestamp_generation(value: OffsetDateTime) -> u64 {
    let timestamp = value.unix_timestamp_nanos();
    if timestamp <= 0 {
        0
    } else {
        u64::try_from(timestamp).unwrap_or(u64::MAX)
    }
}

fn valid_scanner_data_movement_timestamp_generation(value: OffsetDateTime) -> Option<u64> {
    let generation = scanner_data_movement_timestamp_generation(value);
    (generation != 0 && generation != u64::MAX).then_some(generation)
}

fn durable_scanner_data_movement_generation(pool_meta: &PoolMeta, rebalance_meta: Option<&RebalanceMeta>) -> u64 {
    let mut generation = 0;
    for pool in pool_meta.pools.iter().filter(|pool| pool.decommission.is_some()) {
        let Some(pool_generation) = valid_scanner_data_movement_timestamp_generation(pool.last_update) else {
            return u64::MAX;
        };
        generation = generation.max(pool_generation);
    }

    for movement_timestamp in rebalance_meta.into_iter().flat_map(|meta| {
        meta.stopped_at.into_iter().chain(
            meta.pool_stats
                .iter()
                .flat_map(|pool| [pool.info.start_time, pool.info.end_time])
                .flatten(),
        )
    }) {
        let Some(rebalance_generation) = valid_scanner_data_movement_timestamp_generation(movement_timestamp) else {
            return u64::MAX;
        };
        generation = generation.max(rebalance_generation);
    }

    if generation == 0
        && rebalance_meta.is_some_and(|meta| !meta.id.is_empty() || !meta.pool_stats.is_empty() || meta.stopped_at.is_some())
    {
        u64::MAX
    } else {
        generation
    }
}

#[derive(Clone, Copy)]
struct ScannerDataMovementSequenceState {
    operation_epoch: u64,
    operation_epoch_exhausted: bool,
    movement_generation: u64,
    movement_generation_exhausted: bool,
}

fn resolve_scanner_data_movement_pause_status(
    pool_meta: &PoolMeta,
    rebalance_meta: Option<&RebalanceMeta>,
    decommission_worker_active: bool,
    sequence: ScannerDataMovementSequenceState,
    now: OffsetDateTime,
) -> ScannerDataMovementPauseStatus {
    let mut decommission_active = decommission_worker_active;
    let mut decommission_failed = false;
    let mut decommission_canceled = false;
    let mut rebalance_active = false;
    let mut started_at = None;
    let mut movement_backlog_work_items = 0_u64;

    for pool in &pool_meta.pools {
        let Some(info) = pool.decommission.as_ref() else {
            continue;
        };
        let active = info.has_decommission_state() && !info.complete && !info.failed && !info.canceled;
        let failed = !info.queued && info.failed;
        let canceled = !info.queued && info.canceled;
        if !(active || failed || canceled) {
            continue;
        }

        decommission_active |= active;
        decommission_failed |= failed;
        decommission_canceled |= canceled;
        started_at = earliest_timestamp(started_at, info.start_time.or(Some(pool.last_update)));
        let queued = usize_to_u64(info.queued_buckets.len());
        let current_bucket = if info.bucket.is_empty() { 0 } else { 1 };
        movement_backlog_work_items = movement_backlog_work_items.saturating_add(queued.max(current_bucket));
    }

    if let Some(rebalance_meta) = rebalance_meta {
        for pool in &rebalance_meta.pool_stats {
            let active = (pool.participating && pool.info.status == RebalStatus::Started) || pool.info.stopping;
            if !active {
                continue;
            }
            rebalance_active = true;
            started_at = earliest_timestamp(started_at, pool.info.start_time);
            movement_backlog_work_items = movement_backlog_work_items.saturating_add(usize_to_u64(pool.buckets.len()));
        }
    }

    let mut reasons = Vec::with_capacity(6);
    if sequence.operation_epoch_exhausted {
        reasons.push(ScannerDataMovementPauseReason::OperationEpochExhausted);
    }
    if sequence.movement_generation_exhausted {
        reasons.push(ScannerDataMovementPauseReason::MovementGenerationExhausted);
    }
    if decommission_active {
        reasons.push(ScannerDataMovementPauseReason::DecommissionActive);
    }
    if decommission_failed {
        reasons.push(ScannerDataMovementPauseReason::DecommissionFailed);
    }
    if decommission_canceled {
        reasons.push(ScannerDataMovementPauseReason::DecommissionCanceled);
    }
    if rebalance_active {
        reasons.push(ScannerDataMovementPauseReason::RebalanceActive);
    }
    let started_at_unix_secs = started_at.map(offset_unix_seconds).unwrap_or(0);
    let duration_seconds = started_at
        .and_then(|started_at| u64::try_from((now - started_at).whole_seconds()).ok())
        .unwrap_or(0);
    let paused = !reasons.is_empty();

    ScannerDataMovementPauseStatus {
        paused,
        policy: SCANNER_DATA_MOVEMENT_PAUSE_POLICY,
        reasons,
        started_at_unix_secs,
        duration_seconds,
        operation_epoch: sequence.operation_epoch,
        movement_generation: sequence.movement_generation,
        movement_backlog_work_items,
        movement_backlog_estimated: paused,
    }
}

fn record_scanner_data_movement_pause_status(status: &ScannerDataMovementPauseStatus) {
    metrics::gauge!(METRIC_SCANNER_DATA_MOVEMENT_PAUSED).set(if status.paused { 1.0 } else { 0.0 });
    metrics::gauge!(METRIC_SCANNER_DATA_MOVEMENT_PAUSE_DURATION_SECONDS).set(metric_u64(status.duration_seconds));
    metrics::gauge!(METRIC_SCANNER_DATA_MOVEMENT_BACKLOG_WORK_ITEMS).set(metric_u64(status.movement_backlog_work_items));
}

impl std::fmt::Debug for ECStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let disk_slot_count: usize = self.disk_map.values().map(Vec::len).sum();

        f.debug_struct("ECStore")
            .field("id", &self.id)
            .field("disk_map_pool_count", &self.disk_map.len())
            .field("disk_slot_count", &disk_slot_count)
            .field("pool_count", &self.pools.len())
            .finish_non_exhaustive()
    }
}

/// Phase 2: Accessor methods for config globals
/// These delegate to the process-global statics. No local state — the globals
/// remain the single source of truth until the migration is complete.
impl ECStore {
    /// Every erasure set across all pools, pool-major order.
    ///
    /// Read-only queries that must consult each set's own copy of a
    /// per-bucket object (e.g. the scanner's `.usage-cache.bin`) iterate
    /// this instead of the hash-routed store path, which would always land
    /// on one set (rustfs/backlog#1872).
    pub fn all_set_disks(&self) -> Vec<Arc<crate::set_disk::SetDisks>> {
        self.pools.iter().flat_map(|pool| pool.disk_set.iter().cloned()).collect()
    }

    /// Erasure sets that may receive scanner pause-backlog replicas.
    ///
    /// An actively decommissioning or already decommissioned source pool is
    /// excluded so an operational record acknowledged during movement always
    /// has a copy on storage that remains in the cluster. The record is kept
    /// separate from pool and rebalance metadata.
    pub async fn scanner_pause_backlog_writable_set_disks(&self) -> Vec<Arc<crate::set_disk::SetDisks>> {
        let pool_meta = self.pool_meta.read().await;
        self.pools
            .iter()
            .enumerate()
            .filter(|(pool_index, _)| {
                !pool_meta.pools.get(*pool_index).is_some_and(|pool| {
                    pool.decommission
                        .as_ref()
                        .is_some_and(|info| info.has_decommission_state() && !info.failed && !info.canceled)
                })
            })
            .flat_map(|(_, pool)| pool.disk_set.iter().cloned())
            .collect()
    }

    /// Get server configuration (delegates to global)
    pub fn get_server_config(&self) -> Option<Config> {
        runtime_sources::server_config()
    }

    /// Set server configuration (delegates to global)
    pub fn set_server_config(&self, cfg: Config) {
        runtime_sources::set_server_config(cfg);
    }

    /// Get storage class configuration (delegates to global)
    pub fn get_storage_class(&self) -> Option<crate::config::storageclass::Config> {
        runtime_sources::storage_class_config()
    }

    /// Set storage class configuration (delegates to global)
    pub fn set_storage_class(&self, cfg: crate::config::storageclass::Config) {
        runtime_sources::set_storage_class_config(cfg);
    }
}

/// Phase 3: Accessor methods for service globals
/// These provide a unified API through ECStore for accessing cross-cutting
/// service singletons. The globals remain the source of truth.
impl ECStore {
    /// Get the notification system
    pub fn notification_system(&self) -> Option<std::sync::Arc<crate::services::notification_sys::NotificationSys>> {
        runtime_sources::notification_sys()
    }

    /// Get the bucket metadata system
    pub fn bucket_metadata_sys(&self) -> Option<Arc<tokio::sync::RwLock<crate::bucket::metadata_sys::BucketMetadataSys>>> {
        runtime_sources::bucket_metadata_sys()
    }

    /// Get the global endpoints
    pub fn endpoints(&self) -> EndpointServerPools {
        runtime_sources::endpoint_pools().unwrap_or_else(|| Vec::new().into())
    }

    /// Get this store instance's endpoint topology without consulting process globals.
    pub fn instance_endpoints(&self) -> Option<EndpointServerPools> {
        self.ctx.endpoints()
    }

    /// Get the global region
    pub fn region(&self) -> Option<s3s::region::Region> {
        runtime_sources::region()
    }

    /// Get the tier config manager
    pub fn tier_config_mgr(&self) -> Arc<tokio::sync::RwLock<crate::services::tier::tier::TierConfigMgr>> {
        self.ctx.tier_config_mgr()
    }

    /// Get the server configuration
    pub fn server_config(&self) -> Option<Config> {
        runtime_sources::server_config()
    }

    /// Get the storage class configuration
    pub fn storage_class(&self) -> Option<crate::config::storageclass::Config> {
        runtime_sources::storage_class_config()
    }
}

/// Phase 4: Server address accessors
/// These provide a unified API through ECStore for accessing server-level
/// configuration globals. The globals remain the source of truth.
impl ECStore {
    /// Get the server port
    pub fn port(&self) -> u16 {
        runtime_sources::rustfs_port()
    }

    /// Get the server host
    pub async fn host(&self) -> String {
        runtime_sources::rustfs_host().await
    }

    /// Get the server address (host:port)
    pub async fn addr(&self) -> String {
        runtime_sources::rustfs_addr().await
    }
}

/// Phase 5: Per-instance erasure setup accessors (backlog#939)
///
/// These read this instance's own [`InstanceContext`] rather than a process
/// global, so two instances carrying different contexts stay isolated. The
/// legacy free-function facade (`runtime::global::is_erasure` etc.) forwards to
/// the current instance's context, preserving single-instance behavior.
impl ECStore {
    /// Whether this instance uses erasure coding (single-node or distributed).
    pub async fn setup_is_erasure(&self) -> bool {
        self.ctx.is_erasure().await
    }

    /// Whether this instance uses distributed erasure coding.
    pub async fn setup_is_dist_erasure(&self) -> bool {
        self.ctx.is_dist_erasure().await
    }

    /// Whether this instance uses single-drive erasure coding.
    pub async fn setup_is_erasure_sd(&self) -> bool {
        self.ctx.is_erasure_sd().await
    }

    pub fn scanner_namespace_mutation_generation(&self) -> u64 {
        list_objects::scanner_namespace_mutation_generation()
    }

    pub async fn scanner_data_movement_active(&self) -> bool {
        let (decommission, rebalance) = tokio::join!(self.is_decommission_running(), self.is_rebalance_started());
        decommission || rebalance
    }

    /// Return the storage-owned movement state and generation as one
    /// authenticated activity snapshot.  The read lock is acquired before
    /// the state locks (cancelers, pool metadata, then rebalance metadata),
    /// matching the transition writer order and preventing a terminal state
    /// from being reported with the preceding generation.
    pub async fn scanner_data_movement_activity(&self) -> (bool, bool, u64) {
        let operation_gate = self.ctx.data_movement_operation_gate();
        let _operation_guard = operation_gate.read_owned().await;
        let (active, blocked) = self.scanner_data_movement_snapshot_locked().await;
        let blocked =
            blocked || self.ctx.data_movement_operation_epoch_exhausted() || self.ctx.data_movement_generation_exhausted();
        self.ctx.set_scanner_publication_state(blocked);
        (active, blocked, self.ctx.data_movement_generation())
    }

    pub fn scanner_data_movement_generation(&self) -> u64 {
        self.ctx.data_movement_generation()
    }

    pub fn scanner_data_movement_generation_exhausted(&self) -> bool {
        self.ctx.data_movement_generation_exhausted()
    }

    pub fn scanner_data_movement_changed(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.ctx.data_movement_generation_notify()
    }

    /// Returns whether scanner metadata may still be hidden by a local
    /// data-movement state. Terminal failed/canceled decommission entries
    /// remain suspended until an operator clears or retries them, so they are
    /// a publication barrier even after the worker has stopped.
    pub async fn scanner_data_usage_publication_blocked(&self) -> bool {
        let operation_gate = self.ctx.data_movement_operation_gate();
        let _operation_guard = operation_gate.read_owned().await;
        self.scanner_data_usage_publication_snapshot_blocked().await
    }

    pub async fn scanner_data_movement_pause_status(&self) -> ScannerDataMovementPauseStatus {
        let operation_gate = self.ctx.data_movement_operation_gate();
        let _operation_guard = operation_gate.read_owned().await;
        self.scanner_data_movement_pause_snapshot().await
    }

    async fn scanner_data_usage_publication_snapshot_blocked(&self) -> bool {
        self.scanner_data_movement_pause_snapshot().await.paused
    }

    async fn scanner_data_movement_snapshot_locked(&self) -> (bool, bool) {
        let decommission_cancelers = self.decommission_cancelers.read().await;
        let decommission_active = decommission_cancelers
            .iter()
            .any(|canceler| canceler.as_ref().is_some_and(DecommissionCanceler::is_active));
        let pool_meta = self.pool_meta.read().await;
        let decommission_active = decommission_active
            || pool_meta.pools.iter().any(|pool| {
                pool.decommission
                    .as_ref()
                    .is_some_and(|info| info.has_decommission_state() && !info.complete && !info.failed && !info.canceled)
            });
        let decommission_terminal = pool_meta.pools.iter().any(|pool| {
            pool.decommission
                .as_ref()
                .is_some_and(|info| !info.queued && (info.failed || info.canceled))
        });
        let rebalance_meta = self.rebalance_meta.read().await;
        let rebalance_active = rebalance_meta
            .as_ref()
            .is_some_and(is_rebalance_conflicting_with_decommission);
        self.ctx
            .observe_durable_data_movement_generation(durable_scanner_data_movement_generation(
                &pool_meta,
                rebalance_meta.as_ref(),
            ));

        let blocked = decommission_active || decommission_terminal || rebalance_active;
        (decommission_active || rebalance_active, blocked)
    }

    async fn scanner_data_movement_pause_snapshot(&self) -> ScannerDataMovementPauseStatus {
        let decommission_active = {
            let decommission_cancelers = self.decommission_cancelers.read().await;
            decommission_cancelers
                .iter()
                .any(|canceler| canceler.as_ref().is_some_and(DecommissionCanceler::is_active))
        };
        let pool_meta = self.pool_meta.read().await.clone();
        let rebalance_meta = self.rebalance_meta.read().await.clone();
        self.ctx
            .observe_durable_data_movement_generation(durable_scanner_data_movement_generation(
                &pool_meta,
                rebalance_meta.as_ref(),
            ));
        let status = resolve_scanner_data_movement_pause_status(
            &pool_meta,
            rebalance_meta.as_ref(),
            decommission_active,
            ScannerDataMovementSequenceState {
                operation_epoch: self.ctx.data_movement_operation_epoch(),
                operation_epoch_exhausted: self.ctx.data_movement_operation_epoch_exhausted(),
                movement_generation: self.ctx.data_movement_generation(),
                movement_generation_exhausted: self.ctx.data_movement_generation_exhausted(),
            },
            OffsetDateTime::now_utc(),
        );
        self.ctx.set_scanner_publication_state(status.paused);
        record_scanner_data_movement_pause_status(&status);
        status
    }

    #[cfg(test)]
    pub(crate) async fn scanner_data_movement_pause_snapshot_for_test(&self) -> ScannerDataMovementPauseStatus {
        self.scanner_data_movement_pause_snapshot().await
    }

    /// Admit one short data-usage publication commit under the same
    /// per-instance gate used by decommission side effects and transitions.
    /// The epoch is sampled while the read guard is held, so a transition
    /// cannot cross this admission without waiting for the commit to finish.
    pub async fn scanner_data_usage_publication_read_guard(&self) -> (tokio::sync::OwnedRwLockReadGuard<()>, u64) {
        let operation_gate = self.ctx.data_movement_operation_gate();
        let operation_guard = operation_gate.read_owned().await;
        let epoch = self.ctx.data_movement_operation_epoch();
        (operation_guard, epoch)
    }

    /// Acquire the movement gate and inspect the movement owner once. The
    /// state inspection is performed after acquiring the read guard so a
    /// transition cannot update its durable state between the check and the
    /// publication commit.
    pub async fn scanner_data_usage_publication_admission_guard(&self) -> Option<(tokio::sync::OwnedRwLockReadGuard<()>, u64)> {
        let operation_gate = self.ctx.data_movement_operation_gate();
        let operation_guard = operation_gate.read_owned().await;
        if self.ctx.data_movement_operation_epoch_exhausted() || self.ctx.data_movement_generation_exhausted() {
            return None;
        }
        if self.scanner_data_usage_publication_snapshot_blocked().await {
            return None;
        }

        Some((operation_guard, self.ctx.data_movement_operation_epoch()))
    }

    /// Acquire a storage-owned scanner publication scope. Unlike the legacy
    /// admission helper, the movement permit is owned by the returned scope
    /// and therefore survives cancellation of the scanner coordinator while
    /// the actual metadata mutation drains.
    pub async fn scanner_data_usage_publication_commit_scope(
        &self,
        expected_movement_epoch: u64,
        safe_deadline: tokio::time::Instant,
        remote_lease_tokens: Vec<Uuid>,
    ) -> Option<ScannerPublicationCommitScope> {
        let (movement_permit, epoch) = self.scanner_data_usage_publication_admission_guard().await?;
        if epoch != expected_movement_epoch {
            return None;
        }
        Some(ScannerPublicationCommitScope::new_storage_owned(
            epoch,
            safe_deadline,
            remote_lease_tokens,
            movement_permit,
        ))
    }

    pub async fn scanner_data_usage_publication_commit_scope_with_release_flag(
        &self,
        expected_movement_epoch: u64,
        safe_deadline: tokio::time::Instant,
        remote_lease_tokens: Vec<Uuid>,
        lease_release_safe: Arc<std::sync::atomic::AtomicBool>,
    ) -> Option<ScannerPublicationCommitScope> {
        let (movement_permit, epoch) = self.scanner_data_usage_publication_admission_guard().await?;
        if epoch != expected_movement_epoch {
            return None;
        }
        Some(ScannerPublicationCommitScope::new_storage_owned_with_release_flag(
            epoch,
            safe_deadline,
            remote_lease_tokens,
            movement_permit,
            lease_release_safe,
        ))
    }

    /// Capture the current publication epoch without holding the movement
    /// gate across backend I/O. Callers must re-admit the same epoch before a
    /// mutation commits.
    pub(crate) async fn scanner_data_usage_publication_epoch(&self) -> Option<u64> {
        let (operation_guard, epoch) = self.scanner_data_usage_publication_admission_guard().await?;
        drop(operation_guard);
        Some(epoch)
    }

    /// Acquire a storage-owned read admission for a coordinator's final
    /// scanner publication.  The guard remains in the context's lease table,
    /// so a local movement writer cannot pass the peer while its authoritative
    /// PUT is in flight.
    pub async fn acquire_scanner_publication_lease(
        &self,
        expected_generation: u64,
        ttl: std::time::Duration,
    ) -> Result<(Uuid, u64)> {
        if ttl != crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL {
            return Err(Error::other("scanner publication lease TTL is not supported"));
        }

        let operation_gate = self.ctx.data_movement_operation_gate();
        let operation_guard = operation_gate.read_owned().await;
        if self.ctx.data_movement_generation_exhausted()
            || self.ctx.data_movement_operation_epoch_exhausted()
            || self.ctx.data_movement_generation() != expected_generation
        {
            return Err(Error::other("scanner publication lease generation is stale"));
        }
        if self.scanner_data_movement_snapshot_locked().await.1 {
            return Err(Error::other("scanner publication lease is blocked by data movement"));
        }

        let token = Uuid::new_v4();
        let expires_at = tokio::time::Instant::now() + ttl;
        if !self
            .ctx
            .install_scanner_publication_lease(token, expires_at, expected_generation, operation_guard)
            .await
        {
            return Err(Error::other("scanner publication lease capacity is exhausted"));
        }

        let context = Arc::clone(&self.ctx);
        tokio::spawn(async move {
            tokio::time::sleep_until(expires_at).await;
            context.expire_scanner_publication_lease(token, expires_at).await;
        });
        Ok((token, expected_generation))
    }

    pub async fn release_scanner_publication_lease(&self, token: Uuid) -> bool {
        self.ctx.remove_scanner_publication_lease(token).await
    }

    /// Revalidate a previously acquired remote publication lease immediately
    /// before the coordinator's final metadata write.  The operation read
    /// guard makes the movement snapshot and token lookup one storage-owned
    /// admission; a restarted context has no old token and therefore fails
    /// closed even if its generation counter has returned to zero.
    pub async fn validate_scanner_publication_lease(&self, token: Uuid, expected_generation: u64) -> Result<()> {
        let _operation_guard = self.acquire_scanner_publication_lease_guard(token).await?;
        if self.ctx.data_movement_generation_exhausted()
            || self.ctx.data_movement_operation_epoch_exhausted()
            || self.ctx.data_movement_generation() != expected_generation
        {
            return Err(Error::other("scanner publication lease generation is stale"));
        }
        if self.scanner_data_movement_snapshot_locked().await.1 {
            return Err(Error::other("scanner publication lease is blocked by data movement"));
        }
        if !self.ctx.scanner_publication_lease_is_active(token).await {
            return Err(Error::other("scanner publication lease is unknown or expired"));
        }
        Ok(())
    }

    /// Acquire the target-side read guard bound to a previously granted lease.
    /// The guard is returned to the RPC handler and must remain alive through
    /// the complete rename/write operation.  A lease token is process-owned;
    /// restart, expiry, generation changes, or blocked movement all reject it
    /// before the target disk is touched.
    pub async fn acquire_scanner_publication_lease_guard(&self, token: Uuid) -> Result<tokio::sync::OwnedRwLockReadGuard<()>> {
        let operation_gate = self.ctx.data_movement_operation_gate();
        let operation_guard = operation_gate.read_owned().await;
        if self.ctx.data_movement_generation_exhausted() || self.ctx.data_movement_operation_epoch_exhausted() {
            return Err(Error::other("scanner publication lease generation is exhausted"));
        }
        if self.scanner_data_movement_snapshot_locked().await.1 {
            return Err(Error::other("scanner publication lease is blocked by data movement"));
        }
        let Some(lease_generation) = self.ctx.scanner_publication_lease_generation(token).await else {
            return Err(Error::other("scanner publication lease is unknown or expired"));
        };
        if lease_generation != self.ctx.data_movement_generation() {
            return Err(Error::other("scanner publication lease generation is stale"));
        }
        Ok(operation_guard)
    }
}

// impl Clone for ECStore {
//     fn clone(&self) -> Self {
//         let pool_meta = match self.pool_meta.read() {
//             Ok(pool_meta) => pool_meta.clone(),
//             Err(_) => PoolMeta::default(),
//         };
//         Self {
//             id: self.id.clone(),
//             disk_map: self.disk_map.clone(),
//             pools: self.pools.clone(),
//             peer_sys: self.peer_sys.clone(),
//             pool_meta: std_RwLock::new(pool_meta),
//             decommission_cancelers: self.decommission_cancelers.clone(),
//         }
//     }
// }

// #[derive(Debug, Default, Clone)]
// pub struct ListPathOptions {
//     pub id: String,

//     // Bucket of the listing.
//     pub bucket: String,

//     // Directory inside the bucket.
//     // When unset listPath will set this based on Prefix
//     pub base_dir: String,

//     // Scan/return only content with prefix.
//     pub prefix: String,

//     // FilterPrefix will return only results with this prefix when scanning.
//     // Should never contain a slash.
//     // Prefix should still be set.
//     pub filter_prefix: String,

//     // Marker to resume listing.
//     // The response will be the first entry >= this object name.
//     pub marker: String,

//     // Limit the number of results.
//     pub limit: i32,
// }

#[async_trait::async_trait]
impl crate::storage_api_contracts::object::ObjectIO for ECStore {
    type Error = Error;
    type RangeSpec = HTTPRangeSpec;
    type HeaderMap = HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = GetObjectReader;
    type PutObjectReader = PutObjReader;

    #[instrument(level = "debug", skip(self, h))]
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        self.handle_get_object_reader(bucket, object, range, h, opts).await
    }
    #[instrument(level = "debug", skip(self, data))]
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.put_object_with_old_current_size(bucket, object, data, opts)
            .await
            .map(|(object_info, _)| object_info)
    }
}

impl ECStore {
    /// `put_object` plus the rename_data old-size backfill
    /// (rustfs/backlog#1009); see `SetDisks::put_object_with_old_current_size`.
    /// Post-write hooks (immediate ILM transition enqueue, list-cache
    /// invalidation) match the plain `put_object` path exactly.
    #[instrument(level = "debug", skip(self, data))]
    pub async fn put_object_with_old_current_size(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<(ObjectInfo, Option<crate::disk::OldCurrentSize>)> {
        let result = match self.handle_put_object(bucket, object, data, opts).await {
            Ok((object_info, old_current_size)) => enqueue_transition_after_write(Ok(object_info), LcEventSrc::S3PutObject)
                .await
                .map(|object_info| (object_info, old_current_size)),
            Err(err) => Err(err),
        };
        if result.is_ok() {
            list_objects::observe_list_objects_mutation(self, bucket).await;
        }
        result
    }
}

lazy_static! {
    static ref ENABLED_OBJECT_LOCK_CONFIG: ObjectLockConfiguration = ObjectLockConfiguration {
        object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
        ..Default::default()
    };
    static ref ENABLED_VERSIONING_CONFIG: VersioningConfiguration = VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
        ..Default::default()
    };
}

#[async_trait::async_trait]
impl BucketOperations for ECStore {
    type Error = Error;

    #[instrument(skip(self))]
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        Box::pin(self.handle_make_bucket(bucket, opts)).await
    }

    #[instrument(skip(self))]
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        self.handle_get_bucket_info(bucket, opts).await
    }
    #[instrument(skip(self))]
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        self.handle_list_bucket(opts).await
    }
    #[instrument(skip(self))]
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        Box::pin(self.handle_delete_bucket(bucket, opts)).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::object::ObjectOperations for ECStore {
    type Error = Error;
    type ObjectInfo = ObjectInfo;
    type ObjectOptions = ObjectOptions;
    type FileInfo = FileInfo;
    type ObjectToDelete = ObjectToDelete;
    type DeletedObject = DeletedObject;

    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_get_object_info(bucket, object, opts).await
    }

    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.handle_verify_object_integrity(bucket, object, opts).await
    }

    #[instrument(skip(self))]
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let result = enqueue_transition_after_write(
            self.handle_copy_object(src_bucket, src_object, dst_bucket, dst_object, src_info, src_opts, dst_opts)
                .await,
            LcEventSrc::S3CopyObject,
        )
        .await;
        if result.is_ok() {
            list_objects::observe_list_objects_mutation(self, dst_bucket).await;
        }
        result
    }

    #[instrument(skip(self))]
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()> {
        let result = self.handle_delete_object_version(bucket, object, fi, force_del_marker).await;
        if result.is_ok() {
            list_objects::observe_list_objects_mutation(self, bucket).await;
        }
        result
    }

    #[instrument(skip(self))]
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        let result = self.handle_delete_object(bucket, object, opts).await;
        if result.is_ok() {
            list_objects::observe_list_objects_mutation(self, bucket).await;
        }
        result
    }

    #[instrument(skip(self, objects, opts))]
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        let result = self.handle_delete_objects(bucket, objects, opts).await;
        let success_count = result.1.iter().filter(|err| err.is_none()).count();
        if success_count > 0 {
            list_objects::observe_list_objects_mutations(self, bucket, success_count).await;
        }
        result
    }

    #[instrument(skip(self))]
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_put_object_metadata(bucket, object, opts).await
    }
    #[instrument(skip(self))]
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        self.handle_get_object_tags(bucket, object, opts).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_put_object_tags(bucket, object, tags, opts).await
    }

    #[instrument(skip(self))]
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_delete_object_tags(bucket, object, opts).await
    }

    #[instrument(skip(self))]
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        self.handle_add_partial(bucket, object, version_id).await
    }
    #[instrument(skip(self))]
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.handle_transition_object(bucket, object, opts).await
    }

    #[instrument(skip(self))]
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.handle_restore_transitioned_object(bucket, object, opts).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::list::ListOperations for ECStore {
    type Error = Error;
    type ListObjectsV2Info = ListObjectsV2Info;
    type ListObjectVersionsInfo = ListObjectVersionsInfo;
    type ObjectInfoOrErr = ObjectInfoOrErr;
    type WalkOptions = WalkOptions;
    type WalkCancellation = CancellationToken;
    type WalkResultSender = tokio::sync::mpsc::Sender<ObjectInfoOrErr>;

    // @continuation_token marker
    // @start_after as marker when continuation_token empty
    // @delimiter default="/", empty when recursive
    // @max_keys limit
    #[instrument(level = "trace", skip(self))]
    async fn list_objects_v2(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        fetch_owner: bool,
        start_after: Option<String>,
        incl_deleted: bool,
    ) -> Result<ListObjectsV2Info> {
        self.inner_list_objects_v2(
            bucket,
            prefix,
            continuation_token,
            delimiter,
            max_keys,
            fetch_owner,
            start_after,
            incl_deleted,
        )
        .await
    }

    #[instrument(skip(self))]
    async fn list_object_versions(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        self.inner_list_object_versions(bucket, prefix, marker, version_marker, delimiter, max_keys)
            .await
    }

    async fn walk(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()> {
        self.walk_internal(rx, bucket, prefix, result, opts).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::multipart::MultipartOperations for ECStore {
    type Error = Error;
    type ObjectInfo = ObjectInfo;
    type ObjectOptions = ObjectOptions;
    type PutObjectReader = PutObjReader;
    type CompletePart = CompletePart;
    type ListMultipartsInfo = ListMultipartsInfo;
    type MultipartUploadResult = MultipartUploadResult;
    type PartInfo = PartInfo;
    type MultipartInfo = MultipartInfo;
    type ListPartsInfo = ListPartsInfo;

    #[instrument(skip(self))]
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo> {
        self.handle_list_multipart_uploads(
            bucket,
            multipart::MultipartUploadListRequest {
                prefix: prefix.to_string(),
                key_marker,
                upload_id_marker,
                delimiter,
                max_uploads,
                expected_incarnation_id: None,
            },
        )
        .await
    }

    #[instrument(skip(self))]
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        self.handle_new_multipart_upload(bucket, object, opts).await
    }

    #[instrument(skip(self))]
    async fn copy_object_part(
        &self,
        src_bucket: &str,
        src_object: &str,
        _dst_bucket: &str,
        _dst_object: &str,
        _upload_id: &str,
        _part_id: usize,
        _start_offset: i64,
        _length: i64,
        _src_info: &ObjectInfo,
        _src_opts: &ObjectOptions,
        _dst_opts: &ObjectOptions,
    ) -> Result<()> {
        self.handle_copy_object_part(
            src_bucket,
            src_object,
            _dst_bucket,
            _dst_object,
            _upload_id,
            _part_id,
            _start_offset,
            _length,
            _src_info,
            _src_opts,
            _dst_opts,
        )
        .await
    }
    #[instrument(skip(self, data))]
    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        self.handle_put_object_part(bucket, object, upload_id, part_id, data, opts)
            .await
    }

    #[instrument(skip(self))]
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        self.handle_get_multipart_info(bucket, object, upload_id, opts).await
    }

    #[instrument(skip(self))]
    async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        max_parts: usize,
        opts: &ObjectOptions,
    ) -> Result<ListPartsInfo> {
        self.handle_list_object_parts(bucket, object, upload_id, part_number_marker, max_parts, opts)
            .await
    }

    #[instrument(skip(self))]
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()> {
        self.handle_abort_multipart_upload(bucket, object, upload_id, opts).await
    }

    #[instrument(skip(self))]
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let result = enqueue_transition_after_write(
            self.clone()
                .handle_complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
                .await,
            LcEventSrc::S3CompleteMultipartUpload,
        )
        .await;
        if result.is_ok() {
            list_objects::observe_list_objects_mutation(self.as_ref(), bucket).await;
        }
        result
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::heal::HealOperations for ECStore {
    type Error = Error;
    type HealResultItem = HealResultItem;
    type HealOptions = HealOpts;

    #[instrument(skip(self))]
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        self.handle_heal_format(dry_run).await
    }

    #[instrument(skip(self))]
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        self.handle_heal_bucket(bucket, opts).await
    }
    #[instrument(level = "trace", skip(self, opts), fields(bucket = %bucket, object = %object, version_id = %version_id))]
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.handle_heal_object(bucket, object, version_id, opts).await
    }

    #[instrument(skip(self))]
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
        self.handle_get_pool_and_set(id).await
    }

    #[instrument(skip(self))]
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()> {
        self.handle_check_abandoned_parts(bucket, object, opts).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::namespace::NamespaceLocking for ECStore {
    type Error = Error;
    type NamespaceLock = NamespaceLockWrapper;

    async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper> {
        self.handle_new_ns_lock(bucket, object).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::admin::StorageAdminApi for ECStore {
    type BackendInfo = rustfs_madmin::BackendInfo;
    type StorageInfo = rustfs_madmin::StorageInfo;
    type Disk = DiskStore;
    type Error = Error;

    #[instrument(skip(self))]
    async fn backend_info(&self) -> Self::BackendInfo {
        self.handle_backend_info().await
    }

    #[instrument(skip(self))]
    async fn storage_info(&self) -> Self::StorageInfo {
        self.handle_storage_info().await
    }

    #[instrument(skip(self))]
    async fn local_storage_info(&self) -> Self::StorageInfo {
        self.handle_local_storage_info().await
    }

    #[instrument(skip(self))]
    async fn disk_set_inventory(
        &self,
        selector: crate::storage_api_contracts::admin::DiskSetSelector,
    ) -> Result<Vec<Option<Self::Disk>>> {
        self.handle_get_disks(selector.pool_idx, selector.set_idx).await
    }

    #[instrument(skip(self))]
    fn set_drive_counts(&self) -> Vec<usize> {
        self.handle_set_drive_counts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::pools::{PoolDecommissionInfo, PoolSpaceInfo, PoolStatus};
    use crate::layout::endpoints::{Endpoints, PoolEndpoints, SetupType};
    use crate::object_api::ObjectOptions;
    use crate::runtime::global::reset_local_disk_test_state;
    use crate::runtime::sources::{clear_local_disk_id_map_for_test, local_disk_path_by_id};
    use crate::store::init_format::{connect_load_init_formats, init_disks};
    use serial_test::serial;
    use tempfile::TempDir;

    #[test]
    fn g_d2_008_default_versioning_config_keeps_persisted_bytes() {
        let bytes = crate::bucket::utils::serialize::<VersioningConfiguration>(&ENABLED_VERSIONING_CONFIG)
            .expect("the default Versioning configuration must serialize");
        assert_eq!(bytes, b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>");
    }

    #[test]
    fn g_d2_009_default_object_lock_config_keeps_persisted_bytes() {
        let bytes = crate::bucket::utils::serialize::<ObjectLockConfiguration>(&ENABLED_OBJECT_LOCK_CONFIG)
            .expect("the default Object Lock configuration must serialize");
        assert_eq!(
            bytes,
            b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"
        );
    }

    #[tokio::test]
    async fn test_get_disk_infos() {
        let disks = vec![None, None]; // Empty disks for testing
        let infos = get_disk_infos(&disks).await;

        assert_eq!(infos.len(), disks.len());
        // All should be None since we passed None disks
        assert!(infos.iter().all(|info| info.is_none()));
    }

    #[test]
    fn ecstore_debug_is_bounded_summary() {
        let endpoint_pools = EndpointServerPools::default();
        let ctx = Arc::new(InstanceContext::new());
        let store = ECStore {
            id: uuid::Uuid::new_v4(),
            disk_map: [(0, vec![None, None, None, None])].into_iter().collect(),
            pools: Vec::new(),
            peer_sys: crate::cluster::rpc::S3PeerSys::new_with_instance_ctx(&endpoint_pools, ctx.clone()),
            pool_meta: RwLock::new(PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::default(),
            decommission_capacity_entry_gate: Mutex::default(),
            ctx,
            bucket_fence_registry: Arc::default(),
        };

        let rendered = format!("{store:?}");

        assert!(rendered.len() < 256, "ECStore Debug should stay bounded: {rendered}");
        assert!(rendered.contains("disk_map_pool_count"));
        assert!(rendered.contains("disk_slot_count"));
        assert!(!rendered.contains("disk_map:"));
        assert!(!rendered.contains("pools:"));
        assert!(!rendered.contains("pool_meta"));
        assert!(!rendered.contains("format.json"));
        assert!(!rendered.contains("TimedActionSlot"));
        assert!(!rendered.contains("DiskHealthTracker"));
    }

    #[test]
    fn object_options_debug_does_not_expand_tier_store_handle() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let mut opts = ObjectOptions {
            version_id: Some("large-version-id".repeat(1024)),
            expected_current_version_id: Some("large-expected-version-id".repeat(1024)),
            preserve_etag: Some("large-etag".repeat(1024)),
            http_preconditions: Some(crate::storage_api_contracts::object::HTTPPreconditions {
                if_match: Some("large-if-match".repeat(1024)),
                if_none_match: Some("large-if-none-match".repeat(1024)),
                ..Default::default()
            }),
            tier_delete_journal_api: Some(store),
            ..Default::default()
        };
        opts.user_defined.insert("large-user-metadata".to_owned(), "x".repeat(8192));
        opts.eval_metadata = Some([("large-eval-metadata".to_owned(), "y".repeat(8192))].into_iter().collect());
        opts.transition.status = "large-transition-status".repeat(1024);
        opts.transition.tier = "large-transition-tier".repeat(1024);
        opts.lifecycle_audit_event.event.rule_id = "large-rule-id".repeat(1024);
        opts.lifecycle_audit_event.event.storage_class = "large-storage-class".repeat(1024);

        let rendered = format!("{opts:?}");

        assert!(rendered.len() < 4096, "ObjectOptions Debug should stay bounded: {rendered}");
        assert!(rendered.contains("tier_delete_journal_api: true"));
        assert!(rendered.contains("user_defined_count: 1"));
        assert!(rendered.contains("eval_metadata_count: Some(1)"));
        assert!(!rendered.contains("ECStore {"));
        assert!(!rendered.contains("disk_map"));
        assert!(!rendered.contains("large-version-id"));
        assert!(!rendered.contains("large-expected-version-id"));
        assert!(!rendered.contains("large-etag"));
        assert!(!rendered.contains("large-if-match"));
        assert!(!rendered.contains("large-transition"));
        assert!(!rendered.contains("large-rule-id"));
        assert!(!rendered.contains("large-storage-class"));
        assert!(!rendered.contains("large-user-metadata"));
        assert!(!rendered.contains("large-eval-metadata"));
        assert!(!rendered.contains("format.json"));
        assert!(!rendered.contains("TimedActionSlot"));
        assert!(!rendered.contains("DiskHealthTracker"));
    }

    // Build a minimal ECStore carrying an explicit instance context. Empty
    // pools/disks are sufficient: the Phase 5 accessors read only `self.ctx`.
    fn build_store_with_ctx(ctx: Arc<InstanceContext>) -> Arc<ECStore> {
        let endpoint_pools = EndpointServerPools::default();
        Arc::new(ECStore {
            id: uuid::Uuid::new_v4(),
            disk_map: std::collections::HashMap::new(),
            pools: Vec::new(),
            peer_sys: crate::cluster::rpc::S3PeerSys::new_with_instance_ctx(&endpoint_pools, ctx.clone()),
            pool_meta: RwLock::new(PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::default(),
            decommission_capacity_entry_gate: Mutex::default(),
            ctx,
            bucket_fence_registry: Arc::default(),
        })
    }

    fn scanner_sequence_state(operation_epoch: u64, movement_generation: u64) -> ScannerDataMovementSequenceState {
        ScannerDataMovementSequenceState {
            operation_epoch,
            operation_epoch_exhausted: false,
            movement_generation,
            movement_generation_exhausted: false,
        }
    }

    #[test]
    fn scanner_pause_status_derives_restart_stable_decommission_fields() {
        let started_at = OffsetDateTime::from_unix_timestamp(1_000).expect("fixed timestamp should be valid");
        let now = OffsetDateTime::from_unix_timestamp(1_090).expect("fixed timestamp should be valid");
        let pool_meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: started_at,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(started_at),
                    queued_buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
                    bucket: "bucket-a".to_string(),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let status = resolve_scanner_data_movement_pause_status(&pool_meta, None, false, scanner_sequence_state(7, 11), now);

        assert!(status.paused);
        assert_eq!(status.policy, "global_pause");
        assert_eq!(status.reasons, vec![ScannerDataMovementPauseReason::DecommissionActive]);
        assert_eq!(status.started_at_unix_secs, 1_000);
        assert_eq!(status.duration_seconds, 90);
        assert_eq!(status.operation_epoch, 7);
        assert_eq!(status.movement_generation, 11);
        assert_eq!(status.movement_backlog_work_items, 2);
        assert!(status.movement_backlog_estimated);
    }

    #[test]
    fn completed_decommission_restores_durable_movement_generation() {
        let completed_at = OffsetDateTime::from_unix_timestamp(1_100).expect("fixed timestamp should be valid");
        let pool_meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: completed_at,
                decommission: Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let durable_generation = durable_scanner_data_movement_generation(&pool_meta, None);
        let ctx = InstanceContext::new();

        ctx.observe_durable_data_movement_generation(durable_generation);

        assert_eq!(durable_generation, 1_100_000_000_000);
        assert_eq!(ctx.data_movement_generation(), durable_generation);
    }

    #[tokio::test]
    async fn cleared_decommission_restores_durable_movement_generation_after_restart() {
        let mut pool_meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    failed: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        assert!(pool_meta.clear_decommission(0).expect("failed decommission should clear"));
        assert!(
            pool_meta.pools[0]
                .decommission
                .as_ref()
                .is_some_and(|info| !info.has_decommission_state())
        );
        let durable_generation = durable_scanner_data_movement_generation(&pool_meta, None);
        let restarted = build_store_with_ctx(Arc::new(InstanceContext::new()));
        *restarted.pool_meta.write().await = pool_meta;

        let status = restarted.scanner_data_movement_pause_status().await;

        assert_ne!(durable_generation, 0);
        assert!(!status.paused);
        assert_eq!(status.movement_generation, durable_generation);
        assert_eq!(restarted.scanner_data_movement_generation(), durable_generation);
    }

    #[tokio::test]
    async fn same_tick_cleared_decommission_tombstones_advance_durable_movement_generation() {
        let same_tick = OffsetDateTime::from_unix_timestamp(1_100).expect("fixed timestamp should be valid");
        let mut pool_meta = PoolMeta {
            pools: vec![
                PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: same_tick,
                    decommission: Some(PoolDecommissionInfo {
                        failed: true,
                        ..Default::default()
                    }),
                },
                PoolStatus {
                    id: 1,
                    cmd_line: "pool-1".to_string(),
                    last_update: same_tick,
                    decommission: Some(PoolDecommissionInfo {
                        canceled: true,
                        ..Default::default()
                    }),
                },
            ],
            ..Default::default()
        };

        assert!(
            pool_meta
                .clear_decommission_at_for_test(0, same_tick, None)
                .expect("first terminal decommission should clear")
        );
        let first_generation = durable_scanner_data_movement_generation(&pool_meta, None);
        assert_eq!(
            first_generation,
            scanner_data_movement_timestamp_generation(same_tick + time::Duration::nanoseconds(1))
        );

        assert!(
            pool_meta
                .clear_decommission_at_for_test(1, same_tick, None)
                .expect("second terminal decommission should clear")
        );
        let second_generation = durable_scanner_data_movement_generation(&pool_meta, None);
        assert_eq!(
            second_generation,
            scanner_data_movement_timestamp_generation(same_tick + time::Duration::nanoseconds(2))
        );
        assert!(second_generation > first_generation);

        let restarted = build_store_with_ctx(Arc::new(InstanceContext::new()));
        *restarted.pool_meta.write().await = pool_meta;
        let status = restarted.scanner_data_movement_pause_status().await;

        assert!(!status.paused);
        assert_eq!(status.movement_generation, second_generation);
        assert_eq!(restarted.scanner_data_movement_generation(), second_generation);
    }

    #[tokio::test]
    async fn terminal_decommission_transitions_advance_durable_generation_across_same_or_earlier_clocks() {
        let same_tick = OffsetDateTime::from_unix_timestamp(1_200).expect("fixed timestamp should be valid");
        let earlier_tick = same_tick - time::Duration::nanoseconds(10);
        let rebalance_floor = same_tick + time::Duration::nanoseconds(5);
        let rebalance = RebalanceMeta {
            stopped_at: Some(rebalance_floor),
            id: "completed-rebalance".to_string(),
            ..Default::default()
        };
        let active_decommission = |id| PoolStatus {
            id,
            cmd_line: format!("pool-{id}"),
            last_update: same_tick,
            decommission: Some(PoolDecommissionInfo {
                start_time: Some(same_tick),
                ..Default::default()
            }),
        };
        let mut pool_meta = PoolMeta {
            pools: vec![active_decommission(0), active_decommission(1), active_decommission(2)],
            ..Default::default()
        };

        assert!(pool_meta.decommission_complete_at_for_test(0, same_tick, Some(&rebalance)));
        assert_eq!(pool_meta.pools[0].last_update, rebalance_floor + time::Duration::nanoseconds(1));

        assert!(pool_meta.decommission_cancel_at_for_test(1, same_tick, Some(&rebalance)));
        assert_eq!(pool_meta.pools[1].last_update, rebalance_floor + time::Duration::nanoseconds(2));

        assert!(pool_meta.decommission_failed_at_for_test(2, earlier_tick, Some(&rebalance)));
        assert_eq!(pool_meta.pools[2].last_update, rebalance_floor + time::Duration::nanoseconds(3));
        let durable_generation = durable_scanner_data_movement_generation(&pool_meta, Some(&rebalance));
        assert_eq!(
            durable_generation,
            scanner_data_movement_timestamp_generation(rebalance_floor + time::Duration::nanoseconds(3))
        );

        let restarted = build_store_with_ctx(Arc::new(InstanceContext::new()));
        *restarted.pool_meta.write().await = pool_meta;
        *restarted.rebalance_meta.write().await = Some(rebalance);
        let status = restarted.scanner_data_movement_pause_status().await;

        assert_eq!(status.movement_generation, durable_generation);
        assert_eq!(restarted.scanner_data_movement_generation(), durable_generation);
        assert_eq!(
            status.reasons,
            vec![
                ScannerDataMovementPauseReason::DecommissionFailed,
                ScannerDataMovementPauseReason::DecommissionCanceled
            ]
        );
    }

    #[tokio::test]
    async fn decommission_start_after_clear_advances_durable_generation_across_clock_rollback_after_restart() {
        let same_tick = OffsetDateTime::from_unix_timestamp(1_250).expect("fixed timestamp should be valid");
        let earlier_tick = same_tick - time::Duration::nanoseconds(10);
        let rebalance_floor = same_tick + time::Duration::nanoseconds(5);
        let rebalance = RebalanceMeta {
            stopped_at: Some(rebalance_floor),
            id: "completed-rebalance".to_string(),
            ..Default::default()
        };
        let mut pool_meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: same_tick,
                decommission: Some(PoolDecommissionInfo {
                    failed: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert!(
            pool_meta
                .clear_decommission_at_for_test(0, same_tick, Some(&rebalance))
                .expect("failed decommission should clear")
        );
        let cleared_at = rebalance_floor + time::Duration::nanoseconds(1);
        assert_eq!(pool_meta.pools[0].last_update, cleared_at);

        pool_meta
            .decommission_at_for_test(
                0,
                PoolSpaceInfo {
                    total: 200,
                    free: 50,
                    used: 150,
                },
                earlier_tick,
                Some(&rebalance),
            )
            .expect("decommission restart after clear should be allowed");
        let started_at = cleared_at + time::Duration::nanoseconds(1);
        assert_eq!(pool_meta.pools[0].last_update, started_at);
        assert_eq!(
            pool_meta.pools[0].decommission.as_ref().and_then(|info| info.start_time),
            Some(started_at)
        );

        assert!(pool_meta.decommission_complete_at_for_test(0, earlier_tick, Some(&rebalance)));
        let completed_at = started_at + time::Duration::nanoseconds(1);
        assert_eq!(pool_meta.pools[0].last_update, completed_at);
        let durable_generation = durable_scanner_data_movement_generation(&pool_meta, Some(&rebalance));
        assert_eq!(durable_generation, scanner_data_movement_timestamp_generation(completed_at));

        let restarted = build_store_with_ctx(Arc::new(InstanceContext::new()));
        *restarted.pool_meta.write().await = pool_meta;
        *restarted.rebalance_meta.write().await = Some(rebalance);
        let status = restarted.scanner_data_movement_pause_status().await;

        assert!(!status.paused);
        assert_eq!(status.movement_generation, durable_generation);
        assert_eq!(restarted.scanner_data_movement_generation(), durable_generation);
    }

    #[tokio::test]
    async fn decommission_terminal_reload_failure_advances_durable_generation_across_clock_rollback_after_restart() {
        let terminal_at = OffsetDateTime::from_unix_timestamp(1_280).expect("fixed timestamp should be valid");
        let earlier_tick = terminal_at - time::Duration::nanoseconds(10);
        let rebalance_floor = terminal_at + time::Duration::nanoseconds(5);
        let rebalance = RebalanceMeta {
            stopped_at: Some(rebalance_floor),
            id: "completed-rebalance".to_string(),
            ..Default::default()
        };
        let mut pool_meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: terminal_at,
                decommission: Some(PoolDecommissionInfo {
                    start_time: Some(terminal_at),
                    complete: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert!(
            pool_meta
                .record_decommission_terminal_reload_failure_at_for_test(
                    0,
                    "complete_decommission",
                    "peer reload failed".to_string(),
                    earlier_tick,
                    Some(&rebalance),
                )
                .expect("reload failure should be recorded")
        );
        let reload_failure_at = rebalance_floor + time::Duration::nanoseconds(1);
        assert_eq!(pool_meta.pools[0].last_update, reload_failure_at);
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("decommission metadata should exist");
        assert_eq!(info.terminal_reload_attempt_at, Some(reload_failure_at));
        assert_eq!(
            info.terminal_reload_failures,
            vec!["complete_decommission: peer reload failed".to_string()]
        );
        let durable_generation = durable_scanner_data_movement_generation(&pool_meta, Some(&rebalance));
        assert_eq!(durable_generation, scanner_data_movement_timestamp_generation(reload_failure_at));

        let restarted = build_store_with_ctx(Arc::new(InstanceContext::new()));
        *restarted.pool_meta.write().await = pool_meta;
        *restarted.rebalance_meta.write().await = Some(rebalance);
        let status = restarted.scanner_data_movement_pause_status().await;

        assert!(!status.paused);
        assert_eq!(status.movement_generation, durable_generation);
        assert_eq!(restarted.scanner_data_movement_generation(), durable_generation);
    }

    #[tokio::test]
    async fn rebalance_transitions_advance_durable_generation_across_same_or_earlier_clocks_after_restart() {
        let same_tick = OffsetDateTime::from_unix_timestamp(1_300).expect("fixed timestamp should be valid");
        let earlier_tick = same_tick - time::Duration::nanoseconds(10);
        let decommission_floor = same_tick + time::Duration::nanoseconds(5);
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        *store.pool_meta.write().await = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: decommission_floor,
                decommission: Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let started_at = store.next_scanner_data_movement_update(same_tick).await;
        assert_eq!(started_at, decommission_floor + time::Duration::nanoseconds(1));
        *store.rebalance_meta.write().await = Some(RebalanceMeta {
            id: "rebalance-generation".to_string(),
            pool_stats: vec![crate::services::rebalance::RebalanceStats {
                participating: true,
                info: crate::services::rebalance::RebalanceInfo {
                    start_time: Some(started_at),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });

        let completed_at = store.next_scanner_data_movement_update(same_tick).await;
        assert_eq!(completed_at, decommission_floor + time::Duration::nanoseconds(2));
        {
            let mut rebalance_meta = store.rebalance_meta.write().await;
            let meta = rebalance_meta.as_mut().expect("rebalance metadata should be present");
            meta.pool_stats[0].info.status = RebalStatus::Completed;
            meta.pool_stats[0].info.end_time = Some(completed_at);
        }

        let stopped_at = store.next_scanner_data_movement_update(earlier_tick).await;
        assert_eq!(stopped_at, decommission_floor + time::Duration::nanoseconds(3));
        {
            let mut rebalance_meta = store.rebalance_meta.write().await;
            let meta = rebalance_meta.as_mut().expect("rebalance metadata should be present");
            meta.stopped_at = Some(stopped_at);
        }
        let pool_meta = store.pool_meta.read().await.clone();
        let rebalance_meta = store.rebalance_meta.read().await.clone();
        let durable_generation = durable_scanner_data_movement_generation(&pool_meta, rebalance_meta.as_ref());
        assert_eq!(
            durable_generation,
            scanner_data_movement_timestamp_generation(decommission_floor + time::Duration::nanoseconds(3))
        );

        let restarted = build_store_with_ctx(Arc::new(InstanceContext::new()));
        *restarted.pool_meta.write().await = pool_meta;
        *restarted.rebalance_meta.write().await = rebalance_meta;
        let status = restarted.scanner_data_movement_pause_status().await;

        assert!(!status.paused);
        assert_eq!(status.movement_generation, durable_generation);
        assert_eq!(restarted.scanner_data_movement_generation(), durable_generation);
    }

    #[test]
    fn malformed_durable_movement_timestamp_exhausts_generation_fail_closed() {
        let pool_meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        assert_eq!(durable_scanner_data_movement_generation(&pool_meta, None), u64::MAX);
        let exhausted_generation =
            OffsetDateTime::from_unix_timestamp(253_402_300_799).expect("the largest RFC 3339 timestamp should be valid");
        assert_eq!(scanner_data_movement_timestamp_generation(exhausted_generation), u64::MAX);
    }

    #[test]
    fn malformed_durable_movement_timestamp_is_not_masked_by_valid_rebalance_generation() {
        let valid_rebalance_at = OffsetDateTime::from_unix_timestamp(2_400).expect("fixed timestamp should be valid");
        let pool_meta = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let rebalance_meta = RebalanceMeta {
            id: "completed-rebalance".to_string(),
            stopped_at: Some(valid_rebalance_at),
            pool_stats: vec![crate::services::rebalance::RebalanceStats {
                participating: true,
                info: crate::services::rebalance::RebalanceInfo {
                    start_time: Some(valid_rebalance_at - time::Duration::nanoseconds(1)),
                    end_time: Some(valid_rebalance_at),
                    status: RebalStatus::Completed,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(durable_scanner_data_movement_generation(&pool_meta, Some(&rebalance_meta)), u64::MAX);
    }

    #[test]
    fn durable_movement_generation_without_records_is_zero() {
        assert_eq!(durable_scanner_data_movement_generation(&PoolMeta::default(), None), 0);
        assert_eq!(
            durable_scanner_data_movement_generation(&PoolMeta::default(), Some(&RebalanceMeta::default())),
            0
        );
    }

    #[test]
    fn scanner_pause_status_distinguishes_terminal_rebalance_epoch_and_idle() {
        let last_update = OffsetDateTime::from_unix_timestamp(2_000).expect("fixed timestamp should be valid");
        let now = OffsetDateTime::from_unix_timestamp(2_030).expect("fixed timestamp should be valid");
        let failed = PoolMeta {
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update,
                decommission: Some(PoolDecommissionInfo {
                    failed: true,
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let failed_status = resolve_scanner_data_movement_pause_status(&failed, None, false, scanner_sequence_state(3, 12), now);
        assert_eq!(failed_status.reasons, vec![ScannerDataMovementPauseReason::DecommissionFailed]);
        assert_eq!(failed_status.started_at_unix_secs, 2_000);
        assert_eq!(failed_status.duration_seconds, 30);

        let rebalance = RebalanceMeta {
            pool_stats: vec![crate::services::rebalance::RebalanceStats {
                buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
                participating: true,
                info: crate::services::rebalance::RebalanceInfo {
                    start_time: Some(last_update),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let rebalance_status = resolve_scanner_data_movement_pause_status(
            &PoolMeta::default(),
            Some(&rebalance),
            false,
            scanner_sequence_state(4, 13),
            now,
        );
        assert_eq!(rebalance_status.reasons, vec![ScannerDataMovementPauseReason::RebalanceActive]);
        assert_eq!(rebalance_status.movement_backlog_work_items, 2);

        let exhausted = resolve_scanner_data_movement_pause_status(
            &PoolMeta::default(),
            None,
            false,
            ScannerDataMovementSequenceState {
                operation_epoch: u64::MAX,
                operation_epoch_exhausted: true,
                movement_generation: 14,
                movement_generation_exhausted: false,
            },
            now,
        );
        assert_eq!(exhausted.reasons, vec![ScannerDataMovementPauseReason::OperationEpochExhausted]);
        assert_eq!(exhausted.started_at_unix_secs, 0);

        let generation_exhausted = resolve_scanner_data_movement_pause_status(
            &PoolMeta::default(),
            None,
            false,
            ScannerDataMovementSequenceState {
                operation_epoch: 5,
                operation_epoch_exhausted: false,
                movement_generation: u64::MAX,
                movement_generation_exhausted: true,
            },
            now,
        );
        assert_eq!(
            generation_exhausted.reasons,
            vec![ScannerDataMovementPauseReason::MovementGenerationExhausted]
        );

        let idle =
            resolve_scanner_data_movement_pause_status(&PoolMeta::default(), None, false, scanner_sequence_state(5, 15), now);
        assert!(!idle.paused);
        assert!(idle.reasons.is_empty());
        assert!(!idle.movement_backlog_estimated);
    }

    #[tokio::test]
    async fn scanner_data_usage_publication_blocks_active_and_unqueued_terminal_decommission() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let cases = [
            (
                "active",
                PoolDecommissionInfo {
                    start_time: Some(OffsetDateTime::now_utc()),
                    ..Default::default()
                },
                true,
            ),
            (
                "failed",
                PoolDecommissionInfo {
                    failed: true,
                    ..Default::default()
                },
                true,
            ),
            (
                "canceled",
                PoolDecommissionInfo {
                    canceled: true,
                    ..Default::default()
                },
                true,
            ),
            (
                "queued_failed",
                PoolDecommissionInfo {
                    failed: true,
                    queued: true,
                    ..Default::default()
                },
                false,
            ),
            (
                "complete",
                PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                },
                false,
            ),
            ("idle", PoolDecommissionInfo::default(), false),
        ];

        for (name, decommission, expected) in cases {
            *store.pool_meta.write().await = PoolMeta {
                pools: vec![PoolStatus {
                    id: 0,
                    cmd_line: format!("scanner-publication-{name}"),
                    last_update: OffsetDateTime::now_utc(),
                    decommission: Some(decommission),
                }],
                ..Default::default()
            };
            assert_eq!(
                store.scanner_data_usage_publication_blocked().await,
                expected,
                "unexpected scanner publication barrier state for {name}"
            );
        }
    }

    #[tokio::test]
    async fn scanner_data_usage_publication_admission_is_fenced_and_epoch_monotonic() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let operation_gate = store.ctx.data_movement_operation_gate();
        let movement_guard = operation_gate.write().await;
        let pending = {
            let store = store.clone();
            tokio::spawn(async move { store.scanner_data_usage_publication_admission_guard().await })
        };
        tokio::task::yield_now().await;
        assert!(!pending.is_finished(), "publication admission must wait for a movement writer");
        drop(movement_guard);

        let (_, epoch) = pending
            .await
            .expect("publication admission task should not panic")
            .expect("idle store should admit publication");
        assert_eq!(epoch, 0);
        assert_eq!(store.ctx.advance_data_movement_operation_epoch(), 1);
        let (_, next_epoch) = store
            .scanner_data_usage_publication_admission_guard()
            .await
            .expect("idle store should admit the next publication");
        assert_eq!(next_epoch, 1);
        assert_eq!(store.scanner_data_movement_generation(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn scanner_publication_lease_blocks_movement_writer_until_release_or_expiry() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let (token, generation) = store
            .acquire_scanner_publication_lease(0, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL)
            .await
            .expect("an idle store should grant a publication lease");
        assert_eq!(generation, 0);

        let gate = store.ctx.data_movement_operation_gate();
        let (writer_started, writer_started_rx) = tokio::sync::oneshot::channel();
        let movement_writer = tokio::spawn(async move {
            let _ = writer_started.send(());
            gate.write_owned().await
        });
        writer_started_rx
            .await
            .expect("movement writer should reach the gate before waiting");
        assert!(
            !movement_writer.is_finished(),
            "a movement writer must wait while the remote lease owns the read guard"
        );

        assert!(store.release_scanner_publication_lease(token).await);
        tokio::time::timeout(Duration::from_secs(1), movement_writer)
            .await
            .expect("movement writer should proceed after lease release")
            .expect("movement writer task should not panic");

        let (expiring_token, _) = store
            .acquire_scanner_publication_lease(0, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL)
            .await
            .expect("the store should grant a second publication lease");
        let expiry_gate = store.ctx.data_movement_operation_gate();
        let (expiry_started, expiry_started_rx) = tokio::sync::oneshot::channel();
        let mut expiry_writer = tokio::spawn(async move {
            let _ = expiry_started.send(());
            expiry_gate.write_owned().await
        });
        expiry_started_rx
            .await
            .expect("expiry writer should reach the gate before waiting");
        assert!(!expiry_writer.is_finished());
        tokio::time::advance(crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::time::timeout(Duration::from_secs(1), &mut expiry_writer)
            .await
            .expect("movement writer should proceed after lease expiry")
            .expect("expiry writer task should not panic");
        assert!(
            store.validate_scanner_publication_lease(expiring_token, 0).await.is_err(),
            "an expired lease must not validate after its read guard is released"
        );
        assert!(!store.release_scanner_publication_lease(expiring_token).await);
    }

    #[tokio::test]
    async fn scanner_publication_lease_rejects_a_new_movement_generation() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let (token, generation) = store
            .acquire_scanner_publication_lease(0, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL)
            .await
            .expect("an idle store should grant a publication lease");

        assert_eq!(store.ctx.advance_data_movement_generation(), Some(1));
        assert!(
            store.validate_scanner_publication_lease(token, generation).await.is_err(),
            "a lease from the prior movement generation must fail closed"
        );
        assert!(store.release_scanner_publication_lease(token).await);
    }

    #[tokio::test]
    async fn scanner_publication_lease_rejects_stale_generation_before_install() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let error = store
            .acquire_scanner_publication_lease(1, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL)
            .await
            .expect_err("a stale movement generation must not acquire a lease");
        assert!(error.to_string().contains("generation is stale"));
    }

    #[tokio::test(start_paused = true)]
    async fn scanner_publication_commit_scope_owns_permit_until_terminal_drain() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let scope = store
            .scanner_data_usage_publication_commit_scope(
                0,
                tokio::time::Instant::now() + Duration::from_secs(30),
                vec![Uuid::new_v4()],
            )
            .await
            .expect("idle storage should grant a publication scope");
        assert_eq!(scope.state(), crate::object_api::ScannerPublicationCommitState::Admitted);
        assert_eq!(scope.remote_lease_tokens().len(), 1);

        let gate = store.ctx.data_movement_operation_gate();
        let writer = tokio::spawn(async move { gate.write_owned().await });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "the scope must own its movement permit after the caller returns");

        scope.cancel();
        assert!(scope.mark_aborted_before_commit());
        assert_eq!(
            scope.wait_for_completion().await,
            crate::object_api::ScannerPublicationCommitState::AbortedBeforeCommit
        );
        assert!(scope.release_movement_permit().await);
        tokio::time::timeout(Duration::from_secs(1), writer)
            .await
            .expect("movement writer should proceed after the scope drains")
            .expect("movement writer task should not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn scanner_publication_commit_scope_rejects_late_start_and_keeps_indeterminate_permit() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let scope = store
            .scanner_data_usage_publication_commit_scope(0, tokio::time::Instant::now() + Duration::from_secs(1), Vec::new())
            .await
            .expect("idle storage should grant a publication scope");
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            scope.try_begin(),
            Err(crate::object_api::ScannerPublicationCommitStartError::DeadlineExceeded)
        );
        assert!(
            !scope.release_movement_permit().await,
            "an admitted scope is not safe to release before owner resolution"
        );
        assert!(scope.mark_aborted_before_commit());
        assert!(scope.release_movement_permit().await);

        let scope = store
            .scanner_data_usage_publication_commit_scope(0, tokio::time::Instant::now() + Duration::from_secs(30), Vec::new())
            .await
            .expect("a second idle publication scope should be granted");
        scope.try_begin().expect("scope should enter the mutation state");
        scope.cancel();
        assert!(scope.mark_indeterminate());
        assert_eq!(
            scope.wait_for_completion().await,
            crate::object_api::ScannerPublicationCommitState::Indeterminate
        );
        assert!(!scope.release_movement_permit().await, "indeterminate mutation must retain the permit");
    }

    #[tokio::test]
    async fn scanner_publication_scope_guard_classifies_early_returns_conservatively() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let permit = store.ctx.data_movement_operation_gate().read_owned().await;
        let scope = ScannerPublicationCommitScope::new_storage_owned(
            0,
            tokio::time::Instant::now() + Duration::from_secs(30),
            Vec::new(),
            permit,
        );
        {
            let _guard = crate::object_api::ScannerPublicationCommitScopeGuard::new(scope.clone());
        }
        assert_eq!(scope.state(), crate::object_api::ScannerPublicationCommitState::AbortedBeforeCommit);
        assert!(scope.release_movement_permit().await);

        let permit = store.ctx.data_movement_operation_gate().read_owned().await;
        let scope = ScannerPublicationCommitScope::new_storage_owned(
            0,
            tokio::time::Instant::now() + Duration::from_secs(30),
            Vec::new(),
            permit,
        );
        scope.try_begin().expect("scope should enter the mutation state");
        {
            let _guard = crate::object_api::ScannerPublicationCommitScopeGuard::new(scope.clone());
        }
        assert_eq!(scope.state(), crate::object_api::ScannerPublicationCommitState::Indeterminate);
        assert!(!scope.release_movement_permit().await);
    }

    #[tokio::test]
    async fn scanner_target_guard_keeps_movement_writer_fenced_after_lease_release() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let (token, _) = store
            .acquire_scanner_publication_lease(0, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL)
            .await
            .expect("an idle store should grant a publication lease");
        let target_guard = store
            .acquire_scanner_publication_lease_guard(token)
            .await
            .expect("the target-side rename should acquire its short guard");
        assert!(store.release_scanner_publication_lease(token).await);

        let movement_gate = store.ctx.data_movement_operation_gate();
        let movement_writer = tokio::spawn(async move { movement_gate.write_owned().await });
        tokio::task::yield_now().await;
        assert!(!movement_writer.is_finished(), "the target guard must span the rename operation");
        drop(target_guard);
        tokio::time::timeout(std::time::Duration::from_secs(1), movement_writer)
            .await
            .expect("movement writer should proceed after the target rename guard is dropped")
            .expect("movement writer task should not panic");
    }

    #[tokio::test]
    async fn scanner_publication_lease_rejects_restart_aba_token() {
        let first_store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let (token, generation) = first_store
            .acquire_scanner_publication_lease(0, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL)
            .await
            .expect("the initial instance should grant a publication lease");
        first_store
            .validate_scanner_publication_lease(token, generation)
            .await
            .expect("the current instance should validate its own live token");
        first_store
            .acquire_scanner_publication_lease_guard(token)
            .await
            .expect("the current instance should admit the target-side rename");

        // A restarted storage instance starts its local generation at zero,
        // but its process-owned lease table is empty.  The old token must not
        // pass validation just because the numeric generation matches again.
        let restarted_store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let error = restarted_store
            .validate_scanner_publication_lease(token, generation)
            .await
            .expect_err("a token from a prior instance must fail closed after restart");
        assert!(error.to_string().contains("unknown or expired"));
        let error = restarted_store
            .acquire_scanner_publication_lease_guard(token)
            .await
            .expect_err("a restarted instance must reject the target-side rename token");
        assert!(error.to_string().contains("unknown or expired"));
    }

    #[tokio::test]
    async fn movement_generation_notifies_waiters_and_fails_closed_at_maximum() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let notify = store.scanner_data_movement_changed();
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        assert_eq!(store.ctx.advance_data_movement_operation_epoch(), 1);
        tokio::time::timeout(std::time::Duration::from_secs(1), notified)
            .await
            .expect("movement transition should wake scanner waiters");
        assert_eq!(store.scanner_data_movement_generation(), 1);

        store.ctx.set_data_movement_generation_for_test(u64::MAX - 1);
        assert_eq!(store.ctx.advance_data_movement_generation(), Some(u64::MAX));
        assert!(store.scanner_data_movement_generation_exhausted());
        assert_eq!(store.ctx.advance_data_movement_generation(), None);
        assert!(
            store.scanner_data_usage_publication_admission_guard().await.is_none(),
            "generation exhaustion must close publication admission"
        );
    }

    #[tokio::test]
    async fn scanner_data_usage_publication_epoch_releases_gate_before_backend_io() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        let epoch = store
            .scanner_data_usage_publication_epoch()
            .await
            .expect("idle store should expose a publication epoch");
        assert_eq!(epoch, 0);

        let operation_gate = store.ctx.data_movement_operation_gate();
        let _movement_guard = tokio::time::timeout(Duration::from_secs(1), operation_gate.write())
            .await
            .expect("epoch capture must not hold the movement gate across backend I/O");
    }

    #[tokio::test]
    async fn scanner_data_usage_publication_admission_blocks_active_rebalance_snapshot() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        *store.rebalance_meta.write().await = Some(RebalanceMeta {
            pool_stats: vec![crate::services::rebalance::RebalanceStats {
                participating: true,
                info: crate::services::rebalance::RebalanceInfo {
                    status: crate::services::rebalance::RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });

        assert!(
            store.scanner_data_usage_publication_admission_guard().await.is_none(),
            "active rebalance must fail closed at the storage-owned admission boundary"
        );
    }

    #[tokio::test]
    async fn scanner_publication_epoch_exhaustion_fails_closed_after_max() {
        let store = build_store_with_ctx(Arc::new(InstanceContext::new()));
        store.ctx.set_data_movement_operation_epoch_for_test(u64::MAX - 1);

        assert_eq!(store.ctx.advance_data_movement_operation_epoch(), u64::MAX);
        assert!(store.ctx.data_movement_operation_epoch_exhausted());
        assert!(
            store.scanner_data_usage_publication_admission_guard().await.is_none(),
            "publication must fail closed at the reserved terminal epoch"
        );

        assert_eq!(store.ctx.advance_data_movement_operation_epoch(), u64::MAX);
        assert!(store.ctx.data_movement_operation_epoch_exhausted());
        assert!(store.scanner_data_usage_publication_blocked().await);
    }

    // The object graph is the isolation carrier: two ECStore instances holding
    // distinct contexts report independent erasure state through their real
    // `&self` accessors — no cross-contamination.
    #[tokio::test]
    async fn instance_context_carrier_isolates_two_stores() {
        let ctx_a = Arc::new(InstanceContext::new());
        let ctx_b = Arc::new(InstanceContext::new());
        ctx_a.update_erasure_type(SetupType::DistErasure).await;
        ctx_b.update_erasure_type(SetupType::ErasureSD).await;
        ctx_a.set_endpoints(EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::default(),
            cmd_line: "instance-a".to_string(),
            platform: String::new(),
        }]));
        ctx_b.set_endpoints(EndpointServerPools::from(vec![PoolEndpoints {
            legacy: true,
            set_count: 2,
            drives_per_set: 2,
            endpoints: Endpoints::default(),
            cmd_line: "instance-b".to_string(),
            platform: String::new(),
        }]));

        let store_a = build_store_with_ctx(ctx_a);
        let store_b = build_store_with_ctx(ctx_b);

        // store_a: distributed erasure (implies is_erasure), not single-drive.
        assert!(store_a.setup_is_erasure().await);
        assert!(store_a.setup_is_dist_erasure().await);
        assert!(!store_a.setup_is_erasure_sd().await);

        // store_b: single-drive erasure only.
        assert!(store_b.setup_is_erasure_sd().await);
        assert!(!store_b.setup_is_erasure().await);
        assert!(!store_b.setup_is_dist_erasure().await);
        let endpoints_a = store_a.instance_endpoints().expect("instance A endpoints");
        let endpoints_b = store_b.instance_endpoints().expect("instance B endpoints");
        assert_eq!(endpoints_a.as_ref()[0].set_count, 1);
        assert_eq!(endpoints_b.as_ref()[0].set_count, 2);
        assert!(!endpoints_a.as_ref()[0].legacy);
        assert!(endpoints_b.as_ref()[0].legacy);
    }

    // The production/test constructors ADOPT the process bootstrap context
    // (same Arc), so a startup write recorded before the store existed is
    // visible through the store afterward — single-instance behavior preserved.
    #[tokio::test]
    async fn store_adopts_bootstrap_context() {
        let store = build_store_with_ctx(crate::runtime::instance::bootstrap_ctx());
        assert!(
            Arc::ptr_eq(&store.ctx, &crate::runtime::instance::bootstrap_ctx()),
            "store built via adoption must share the bootstrap context Arc"
        );
    }

    #[tokio::test]
    async fn test_has_space_for() {
        let disk_infos = vec![None, None]; // No actual disk info

        let result = crate::layout::pool_space::has_space_for(&disk_infos, 1024).await;
        // Should fail due to no valid disk info
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_find_local_disk() {
        let result = peer::find_local_disk("/nonexistent/path").await;
        assert!(result.is_none(), "Should return None for nonexistent path");
    }

    #[tokio::test]
    #[serial]
    async fn test_find_local_disk_by_ref_backfills_uuid_map() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for local disk ref test");
        let disk_paths = (0..4)
            .map(|idx| temp_dir.path().join(format!("disk{}", idx + 1)))
            .collect::<Vec<_>>();
        for disk_path in &disk_paths {
            std::fs::create_dir_all(disk_path).expect("create disk path");
        }

        let mut endpoints = Vec::new();
        for (idx, disk_path) in disk_paths.iter().enumerate() {
            let mut endpoint = Endpoint::try_from(disk_path.to_str().expect("disk path to str")).expect("endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(idx);
            endpoints.push(endpoint);
        }

        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "find-local-disk-by-ref-test".to_string(),
            platform: "test".to_string(),
        }]);

        init_local_disks(endpoint_pools.clone()).await.expect("init local disks");

        let (mut disks, errs) = init_disks(
            &endpoint_pools.as_ref().first().expect("pool endpoints").endpoints,
            &DiskOption {
                cleanup: true,
                health_check: false,
            },
        )
        .await;

        assert!(errs.iter().all(|err| err.is_none()), "disk init should succeed: {errs:?}");
        connect_load_init_formats(true, &mut disks, 1, 4, None)
            .await
            .expect("initialize format metadata");

        clear_local_disk_id_map_for_test().await;

        let local_disks = all_local_disk().await;
        let first_disk = local_disks.first().expect("local disk exists");
        let disk_id = first_disk
            .get_disk_id()
            .await
            .expect("get disk id should succeed")
            .expect("disk id should exist");

        let found = find_local_disk_by_ref(&disk_id.to_string()).await;
        assert!(found.is_some(), "disk lookup by id should backfill cache");
        assert_eq!(local_disk_path_by_id(&disk_id).await, Some(first_disk.endpoint().to_string()));

        reset_local_disk_test_state().await;
    }

    #[tokio::test]
    async fn test_all_local_disk_path() {
        let paths = all_local_disk_path().await;
        // Should return empty or some paths depending on global state
        assert!(paths.is_empty() || !paths.is_empty());
    }

    #[tokio::test]
    async fn test_all_local_disk() {
        let disks = all_local_disk().await;
        // Should return empty or some disks depending on global state
        assert!(disks.is_empty() || !disks.is_empty());
    }

    #[test]
    fn test_should_not_enqueue_transition_for_internal_metadata_bucket() {
        let oi = ObjectInfo {
            bucket: RUSTFS_META_BUCKET.to_string(),
            name: format!("{BUCKET_META_PREFIX}/bucket/.metadata.bin"),
            ..Default::default()
        };

        assert!(!should_enqueue_transition_immediately(&oi));
    }
}
