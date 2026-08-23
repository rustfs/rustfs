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

//! `MultipartOperations` for `SetDisks` plus its multipart helpers.
//!
//! P2 of the SetDisks God-Object split (tracking backlog#815, issue #818).
//! The storage-api contract impl and the multipart list/quorum helpers are
//! relocated here from `set_disk/mod.rs` and `set_disk/multipart.rs`. The
//! contract stays implemented `for SetDisks`, so its associated-type bounds are
//! unchanged; method bodies are moved verbatim and runtime behavior is the same.

use super::super::*;
use super::bitrot_self_verify::{BitrotSelfVerifyTarget, drop_failed_writer_disks, verify_written_bitrot_shards};
use super::object::{
    assign_object_transaction_epoch, object_transaction_fencing_fleet_proof, object_transaction_fencing_fleet_proof_matches,
    object_transaction_fencing_requested, old_data_cleanup_receipt_path, read_object_transaction_epoch_fence,
    verify_object_transaction_epoch_fence,
};
use crate::bucket::quota::reservation;
use crate::crash_inject::{self, CrashPoint};
use crate::multipart_listing::paginate_multipart_listing;
use futures::{StreamExt, stream};
use std::future::Future;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::task::JoinSet;

const MULTIPART_LIST_IO_CONCURRENCY: usize = 16;

pub(crate) struct StaleMultipartCleanupGuard {
    file_info: FileInfo,
    upload_path: String,
    write_quorum: usize,
    lock_guard: ObjectLockDiagGuard,
}

impl StaleMultipartCleanupGuard {
    pub(crate) fn file_info(&self) -> &FileInfo {
        &self.file_info
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        self.lock_guard.is_lock_lost()
    }

    pub(crate) async fn delete(self, set: &SetDisks) -> Result<()> {
        fence_commit_on_lock_loss(Some(&self.lock_guard), "stale_multipart_cleanup", &self.upload_path)?;
        set.delete_all_with_quorum(RUSTFS_META_MULTIPART_BUCKET, &self.upload_path, self.write_quorum)
            .await
    }
}

#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum MultipartCommitPause {
    NewUploadBeforeLockLost,
    PutPartBeforeLockAcquire,
    PutPartBeforeLockLost,
    PutPartAfterRename,
    BeforeLockLost,
    BeforeQuotaRename,
    BeforeTransactionEpochVerify,
    BeforeObjectPublication,
    AfterObjectPublication,
    AfterRename,
}

#[cfg(any(test, feature = "test-util"))]
struct MultipartCommitBarrierState {
    bucket: String,
    object: String,
    pause: MultipartCommitPause,
    expected_arrivals: usize,
    arrivals: AtomicUsize,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

#[cfg(any(test, feature = "test-util"))]
pub struct MultipartCommitBarrier {
    state: Arc<MultipartCommitBarrierState>,
}

#[cfg(any(test, feature = "test-util"))]
static MULTIPART_COMMIT_BARRIER: std::sync::OnceLock<std::sync::Mutex<Option<Arc<MultipartCommitBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-util"))]
impl MultipartCommitBarrier {
    pub fn install(bucket: &str, object: &str, pause: MultipartCommitPause) -> Self {
        Self::install_for_arrivals(bucket, object, pause, 1)
    }

    pub fn install_for_arrivals(bucket: &str, object: &str, pause: MultipartCommitPause, expected_arrivals: usize) -> Self {
        assert!(expected_arrivals > 0, "multipart commit barrier must wait for at least one arrival");
        let state = Arc::new(MultipartCommitBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            pause,
            expected_arrivals,
            arrivals: AtomicUsize::new(0),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        let mut slot = MULTIPART_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("multipart commit barrier mutex should not poison");
        assert!(slot.is_none(), "multipart commit barrier must be installed by one test at a time");
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    pub async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let arrived = self.state.arrived.notified();
                if self.state.arrivals.load(Ordering::Acquire) >= self.state.expected_arrivals {
                    return;
                }
                arrived.await;
            }
        })
        .await
        .expect("multipart completion should reach the deterministic commit barrier");
    }

    pub fn release(&self) {
        self.state.release.add_permits(self.state.expected_arrivals);
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Drop for MultipartCommitBarrier {
    fn drop(&mut self) {
        self.release();
        let mut slot = MULTIPART_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("multipart commit barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
struct NewMultipartUploadCommitObservationState {
    bucket: String,
    object: String,
    committed: AtomicBool,
}

#[cfg(test)]
pub(crate) struct NewMultipartUploadCommitObservation {
    state: Arc<NewMultipartUploadCommitObservationState>,
}

#[cfg(test)]
static NEW_MULTIPART_UPLOAD_COMMIT_OBSERVATION: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<NewMultipartUploadCommitObservationState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
impl NewMultipartUploadCommitObservation {
    pub(crate) fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(NewMultipartUploadCommitObservationState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            committed: AtomicBool::new(false),
        });
        let mut slot = NEW_MULTIPART_UPLOAD_COMMIT_OBSERVATION
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("new multipart upload commit observation mutex should not poison");
        assert!(slot.is_none(), "new multipart upload commit observation must be unique");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) fn committed(&self) -> bool {
        self.state.committed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl Drop for NewMultipartUploadCommitObservation {
    fn drop(&mut self) {
        let mut slot = NEW_MULTIPART_UPLOAD_COMMIT_OBSERVATION
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("new multipart upload commit observation mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
fn observe_new_multipart_upload_commit(bucket: &str, object: &str) {
    let state = NEW_MULTIPART_UPLOAD_COMMIT_OBSERVATION
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("new multipart upload commit observation mutex should not poison")
        .as_ref()
        .filter(|state| state.bucket == bucket && state.object == object)
        .cloned();
    if let Some(state) = state {
        state.committed.store(true, Ordering::Release);
    }
}

#[cfg(any(test, feature = "test-util"))]
async fn pause_multipart_commit(bucket: &str, object: &str, pause: MultipartCommitPause) {
    let barrier = {
        let mut slot = MULTIPART_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("multipart commit barrier mutex should not poison");
        if slot
            .as_ref()
            .is_some_and(|barrier| barrier.bucket == bucket && barrier.object == object && barrier.pause == pause)
        {
            if pause == MultipartCommitPause::BeforeTransactionEpochVerify {
                slot.take()
            } else {
                slot.clone()
            }
        } else {
            None
        }
    };
    if let Some(barrier) = barrier
        && let Ok(previous) = barrier.arrivals.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < barrier.expected_arrivals).then_some(current + 1)
        })
    {
        let arrival = previous + 1;
        if arrival == barrier.expected_arrivals {
            barrier.arrived.notify_one();
        }
        barrier
            .release
            .acquire()
            .await
            .expect("multipart commit barrier should remain open")
            .forget();
    }
}

fn map_upload_id_metadata_error(bucket: &str, object: &str, upload_id: &str, err: DiskError) -> Error {
    if err == DiskError::FileNotFound {
        return StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned());
    }
    err.into()
}

/// Abort a multipart commit when the guard's refresh heartbeat has observed a
/// refresh-quorum loss (backlog#899 Phase 2): a stale holder must not race a
/// concurrent committer past its fenced commit point.
fn fence_commit_on_lock_loss(guard: Option<&ObjectLockDiagGuard>, mode: &'static str, lock_path: &str) -> Result<()> {
    if guard.is_some_and(|guard| guard.is_lock_lost()) {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode,
            bucket: RUSTFS_META_MULTIPART_BUCKET.to_string(),
            object: lock_path.to_string(),
            required: 1,
            achieved: 0,
        });
    }
    Ok(())
}

fn multipart_bucket_incarnation_id(metadata: &HashMap<String, String>) -> Result<Option<Uuid>> {
    let Some(value) = rustfs_utils::http::metadata_compat::get_consistent_str(metadata, SUFFIX_BUCKET_INCARNATION_ID) else {
        if rustfs_utils::http::metadata_compat::contains_key_str(metadata, SUFFIX_BUCKET_INCARNATION_ID) {
            return Err(Error::other("invalid multipart bucket incarnation metadata"));
        }
        return Ok(None);
    };
    let incarnation = Uuid::parse_str(value).map_err(|_| Error::other("invalid multipart bucket incarnation metadata"))?;
    if incarnation.is_nil() {
        return Err(Error::other("invalid multipart bucket incarnation metadata"));
    }
    Ok(Some(incarnation))
}

fn multipart_bucket_incarnation_matches(metadata: &HashMap<String, String>, expected: Uuid) -> bool {
    matches!(multipart_bucket_incarnation_id(metadata), Ok(Some(actual)) if actual == expected)
}

fn validate_multipart_bucket_incarnation(
    metadata: &HashMap<String, String>,
    bucket: &str,
    object: &str,
    upload_id: &str,
    expected: Option<Uuid>,
    upload_initiated: Option<OffsetDateTime>,
    bucket_created: Option<OffsetDateTime>,
) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    match multipart_bucket_incarnation_id(metadata) {
        Ok(Some(actual)) if actual == expected => return Ok(()),
        Ok(None)
            if matches!(
                (upload_initiated, bucket_created),
                (Some(upload_initiated), Some(bucket_created)) if upload_initiated >= bucket_created
            ) =>
        {
            return Ok(());
        }
        _ => {}
    }
    Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
}

fn ensure_data_movement_upload_access(
    fi: &FileInfo,
    bucket: &str,
    object: &str,
    upload_id: &str,
    opts: &ObjectOptions,
) -> Result<()> {
    if rustfs_utils::http::contains_key_str(&fi.metadata, rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD) && !opts.data_movement
    {
        return Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()));
    }
    Ok(())
}

async fn ensure_multipart_bucket_incarnation(
    ctx: &crate::runtime::instance::InstanceContext,
    fi: &FileInfo,
    bucket: &str,
    object: &str,
    upload_id: &str,
    expected: Option<Uuid>,
) -> Result<()> {
    let bucket_created = if expected.is_some() && matches!(multipart_bucket_incarnation_id(&fi.metadata), Ok(None)) {
        Some(metadata_sys::created_at_in(ctx, bucket).await?)
    } else {
        None
    };
    validate_multipart_bucket_incarnation(&fi.metadata, bucket, object, upload_id, expected, fi.mod_time, bucket_created)
}

fn ensure_multipart_bucket_lifecycle_lock_held(bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
    let Some(fence) = opts.bucket_lifecycle_lock_fence.as_ref() else {
        if opts.expected_bucket_incarnation_id.is_some() && !crate::bucket::utils::is_meta_bucketname(bucket) {
            return Err(Error::other("multipart bucket lifecycle lock fence is missing"));
        }
        return Ok(());
    };
    if fence.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "multipart_bucket_generation",
            bucket: bucket.to_string(),
            object: object.to_string(),
            required: 1,
            achieved: 0,
        });
    }
    Ok(())
}

fn empty_upload_fallback_possible(successful_responses: usize, errs: &[Option<DiskError>]) -> bool {
    successful_responses == 0
        && errs.iter().any(|err| matches!(err, Some(DiskError::FileNotFound)))
        && errs.iter().all(|err| match err {
            Some(DiskError::FileNotFound) => true,
            Some(err) => OBJECT_OP_IGNORED_ERRS.contains(err),
            None => false,
        })
}

async fn collect_list_parts_results<F>(
    tasks: Vec<F>,
    read_quorum: usize,
) -> disk::error::Result<(Vec<Option<DiskError>>, Vec<Vec<String>>)>
where
    F: Future<Output = disk::error::Result<Vec<String>>> + Send + 'static,
{
    let mut errs = vec![Some(DiskError::DiskNotFound); tasks.len()];
    let mut object_parts = vec![Vec::new(); tasks.len()];
    let mut successful_responses = 0usize;
    let mut pending = tasks.len();
    let mut join_set = JoinSet::new();

    for (index, task) in tasks.into_iter().enumerate() {
        join_set.spawn(async move { (index, task.await) });
    }

    while let Some(join_result) = join_set.join_next().await {
        pending = pending.saturating_sub(1);

        match join_result {
            Ok((index, Ok(parts))) => {
                errs[index] = None;
                object_parts[index] = parts;
                successful_responses += 1;
            }
            Ok((index, Err(err))) => {
                errs[index] = Some(err);
            }
            Err(_) => {}
        }

        if successful_responses + pending < read_quorum && !empty_upload_fallback_possible(successful_responses, &errs) {
            return Err(DiskError::ErasureReadQuorum);
        }
    }

    if successful_responses < read_quorum {
        if empty_upload_fallback_possible(successful_responses, &errs) {
            return Err(DiskError::FileNotFound);
        }

        return Err(DiskError::ErasureReadQuorum);
    }

    Ok((errs, object_parts))
}

fn reduce_quorum_part_numbers(object_parts: Vec<Vec<String>>, read_quorum: usize) -> Vec<usize> {
    let mut part_quorum_map: HashMap<usize, usize> = HashMap::new();

    for drive_parts in object_parts {
        let mut parts_with_meta_count: HashMap<usize, usize> = HashMap::new();

        // part files can be either part.N or part.N.meta
        for part_path in drive_parts {
            if let Some(num_str) = part_path.strip_prefix("part.") {
                if let Some(meta_idx) = num_str.find(".meta") {
                    if let Ok(part_num) = num_str[..meta_idx].parse::<usize>() {
                        *parts_with_meta_count.entry(part_num).or_insert(0) += 1;
                    }
                } else if let Ok(part_num) = num_str.parse::<usize>() {
                    *parts_with_meta_count.entry(part_num).or_insert(0) += 1;
                }
            }
        }

        // Include only part.N.meta files with corresponding part.N
        for (&part_num, &cnt) in &parts_with_meta_count {
            if cnt >= 2 {
                *part_quorum_map.entry(part_num).or_insert(0) += 1;
            }
        }
    }

    let mut part_numbers = Vec::with_capacity(part_quorum_map.len());
    for (part_num, count) in part_quorum_map {
        if count >= read_quorum {
            part_numbers.push(part_num);
        }
    }

    part_numbers.sort();
    part_numbers
}

/// Applies the `max_uploads` page cap to an already marker-offset, sorted slice
/// of multipart uploads.
///
/// At most `max_uploads` entries are returned. When more uploads remain beyond
/// the page, the first overflow element acts purely as a truncation probe: it is
/// never returned, but flips `is_truncated` to `true` and yields a
/// `next_upload_id_marker` pointing at the last returned upload so the caller can
/// resume paging.
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
fn paginate_upload_page(remaining: &[MultipartInfo], max_uploads: usize) -> (Vec<MultipartInfo>, bool, Option<String>) {
    let is_truncated = remaining.len() > max_uploads;
    let page: Vec<MultipartInfo> = remaining.iter().take(max_uploads).cloned().collect();
    let next_upload_id_marker = if is_truncated {
        page.last().map(|upload| upload.upload_id.clone())
    } else {
        None
    };
    (page, is_truncated, next_upload_id_marker)
}

/// Deterministic per-upload damage during a multipart listing: the metadata
/// exists but is torn, undecodable, or not identifiable as an upload. The same
/// corrupt family as `classify_metadata_response_error`'s corrupt group. This
/// deliberately excludes transient fault shapes (`DiskNotFound`, `Timeout`,
/// `Io`, `ErasureReadQuorum` from offline-disk quorum loss, ...): degrading on
/// those would silently drop healthy uploads from a 200 listing while disks
/// are merely unreachable (issue #5716 review).
fn is_corrupt_upload_metadata_error(err: &DiskError) -> bool {
    matches!(
        err,
        DiskError::FileCorrupt | DiskError::CorruptedFormat | DiskError::CorruptedBackend | DiskError::OutdatedXLMeta
    )
}

async fn multipart_upload_paths_on_disk(disk: DiskStore, bucket: &str) -> disk::error::Result<Vec<String>> {
    if !disk.is_online().await {
        return Err(DiskError::DiskNotFound);
    }

    let sha_dirs = match disk.list_dir(bucket, RUSTFS_META_MULTIPART_BUCKET, "", -1).await {
        Ok(entries) => entries,
        Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };

    let sha_dirs = sha_dirs
        .into_iter()
        .map(|sha_dir| sha_dir.trim_end_matches('/').to_owned())
        .filter(|sha_dir| sha_dir.len() == 64 && sha_dir.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .collect::<Vec<_>>();
    let listed_upload_dirs = stream::iter(sha_dirs)
        .map(|sha_dir| {
            let disk = disk.clone();
            async move {
                match disk.list_dir(bucket, RUSTFS_META_MULTIPART_BUCKET, &sha_dir, -1).await {
                    Ok(entries) => Ok((sha_dir, entries)),
                    Err(DiskError::FileNotFound) => Ok((sha_dir, Vec::new())),
                    Err(err) => Err(err),
                }
            }
        })
        .buffer_unordered(MULTIPART_LIST_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut upload_paths = Vec::new();
    for listed_upload_dirs in listed_upload_dirs {
        let (sha_dir, upload_dirs) = listed_upload_dirs?;
        upload_paths.reserve(upload_dirs.len());
        upload_paths.extend(upload_dirs.into_iter().filter_map(|upload_dir| {
            let upload_dir = upload_dir.trim_end_matches('/');
            (!upload_dir.is_empty() && upload_dir != "." && upload_dir != ".." && !upload_dir.contains(['/', '\\']))
                .then(|| format!("{sha_dir}/{upload_dir}"))
        }));
    }

    Ok(upload_paths)
}

impl SetDisks {
    async fn acquire_multipart_upload_read_lock(
        &self,
        op: &'static str,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<Option<ObjectLockDiagGuard>> {
        if opts.no_lock {
            return Ok(None);
        }

        let upload_id_path = Self::get_multipart_upload_dir(bucket, object, upload_id, opts.data_movement);
        self.acquire_read_lock_diag(op, RUSTFS_META_MULTIPART_BUCKET, &upload_id_path)
            .await
            .map(Some)
    }

    async fn acquire_multipart_upload_write_lock(
        &self,
        op: &'static str,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<Option<ObjectLockDiagGuard>> {
        if opts.no_lock {
            return Ok(None);
        }

        let upload_id_path = Self::get_multipart_upload_dir(bucket, object, upload_id, opts.data_movement);
        self.acquire_write_lock_diag(op, RUSTFS_META_MULTIPART_BUCKET, &upload_id_path)
            .await
            .map(Some)
    }

    pub(super) async fn list_parts(
        disks: &[Option<DiskStore>],
        part_path: &str,
        read_quorum: usize,
    ) -> disk::error::Result<Vec<usize>> {
        let mut futures = Vec::with_capacity(disks.len());
        let part_path = part_path.to_string();
        for disk in disks.iter() {
            let disk = disk.clone();
            let part_path = part_path.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, part_path.as_str(), -1)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let mut errs = Vec::with_capacity(disks.len());
        let mut object_parts = Vec::with_capacity(disks.len());

        let (collected_errs, collected_parts) = collect_list_parts_results(futures, read_quorum).await?;
        errs.extend(collected_errs);
        object_parts.extend(collected_parts);

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            return Err(err);
        }

        Ok(reduce_quorum_part_numbers(object_parts, read_quorum))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    pub(super) async fn check_upload_id_exists(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        write: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>)> {
        self.check_upload_id_exists_for_data_movement(bucket, object, upload_id, write, false)
            .await
    }

    async fn check_upload_id_exists_with_opts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        write: bool,
        opts: &ObjectOptions,
    ) -> Result<(FileInfo, Vec<FileInfo>)> {
        self.check_upload_id_exists_for_data_movement(bucket, object, upload_id, write, opts.data_movement)
            .await
    }

    async fn check_upload_id_exists_for_data_movement(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        write: bool,
        data_movement: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>)> {
        let upload_id_path = Self::get_multipart_upload_dir(bucket, object, upload_id, data_movement);
        self.check_multipart_upload_path_exists(bucket, object, upload_id, &upload_id_path, write)
            .await
    }

    async fn check_multipart_upload_path_exists(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        upload_id_path: &str,
        write: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>)> {
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let (parts_metadata, errs) =
            Self::read_all_fileinfo(&disks, bucket, RUSTFS_META_MULTIPART_BUCKET, upload_id_path, "", false, false, false)
                .await?;

        let (read_quorum, write_quorum) = Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count)
            .map_err(|err| map_upload_id_metadata_error(bucket, object, upload_id, err))?;

        if read_quorum < 0 {
            error!("check_upload_id_exists: read_quorum < 0, errs={:?}", errs);
            return Err(Error::ErasureReadQuorum);
        }

        if write_quorum < 0 {
            return Err(Error::ErasureWriteQuorum);
        }

        let mut quorum = read_quorum as usize;
        if write {
            quorum = write_quorum as usize;

            if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, quorum) {
                log_multipart_write_quorum_failure(
                    MultipartWriteQuorumContext {
                        stage: MULTIPART_WRITE_QUORUM_UPLOAD_METADATA,
                        bucket,
                        object,
                        upload_id,
                        part_number: None,
                    },
                    &errs,
                    quorum,
                    &err,
                );
                return Err(map_upload_id_metadata_error(bucket, object, upload_id, err));
            }
        } else if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, quorum) {
            return Err(map_upload_id_metadata_error(bucket, object, upload_id, err));
        }

        let (_, mod_time, etag) = Self::list_online_disks(&disks, &parts_metadata, &errs, quorum);

        let fi = Self::pick_valid_fileinfo(&parts_metadata, mod_time, etag, quorum)?;

        Ok((fi, parts_metadata))
    }

    pub(crate) async fn lock_stale_multipart_cleanup(&self, upload_path: &str) -> Result<StaleMultipartCleanupGuard> {
        let lock_guard = self
            .acquire_write_lock_diag("stale_multipart_cleanup", RUSTFS_META_MULTIPART_BUCKET, upload_path)
            .await?;
        let (file_info, _) = self.check_multipart_upload_path_exists("", "", "", upload_path, true).await?;
        let write_quorum = file_info.write_quorum(self.default_write_quorum());
        Ok(StaleMultipartCleanupGuard {
            file_info,
            upload_path: upload_path.to_string(),
            write_quorum,
            lock_guard,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn list_multipart_uploads_for_incarnation(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
        expected_incarnation_id: Option<Uuid>,
    ) -> Result<ListMultipartsInfo> {
        let disks = self.disks.read().await.clone();
        if disks.is_empty() {
            return Err(Error::ErasureReadQuorum);
        }
        let discovery_quorum = if self.default_parity_count == 0 {
            disks.len()
        } else {
            (disks.len() / 2).max(1)
        };
        let mut discovery_errors = (0..disks.len()).map(|_| Some(DiskError::DiskNotFound)).collect::<Vec<_>>();
        let mut candidate_counts = HashMap::<String, usize>::new();
        let mut discovery_tasks = JoinSet::new();
        for (index, disk) in disks.iter().enumerate() {
            let disk = disk.clone();
            let bucket = bucket.to_string();
            discovery_tasks.spawn(async move {
                let result = match disk {
                    Some(disk) => multipart_upload_paths_on_disk(disk, &bucket).await,
                    None => Err(DiskError::DiskNotFound),
                };
                (index, result)
            });
        }

        while let Some(task_result) = discovery_tasks.join_next().await {
            let Ok((index, result)) = task_result else {
                continue;
            };
            match result {
                Ok(paths) => {
                    discovery_errors[index] = None;
                    for path in paths {
                        *candidate_counts.entry(path).or_insert(0) += 1;
                    }
                }
                Err(err) => discovery_errors[index] = Some(err),
            }
        }

        if let Some(err) = reduce_read_quorum_errs(&discovery_errors, OBJECT_OP_IGNORED_ERRS, discovery_quorum) {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        let candidate_paths = candidate_counts
            .into_iter()
            .filter_map(|(path, count)| (count >= discovery_quorum).then_some(path))
            .collect::<Vec<_>>();
        let listed_uploads = stream::iter(candidate_paths)
            .map(|upload_path| {
                let disks = &disks;
                async move {
                    let (sha_dir, raw_upload_id) = upload_path
                        .rsplit_once('/')
                        .filter(|(sha_dir, upload_id)| !sha_dir.is_empty() && !upload_id.is_empty())
                        .ok_or(DiskError::CorruptedFormat)?;
                    let (parts_metadata, errs) = Self::read_all_fileinfo(
                        disks,
                        bucket,
                        RUSTFS_META_MULTIPART_BUCKET,
                        &upload_path,
                        "",
                        false,
                        false,
                        false,
                    )
                    .await?;
                    let missing_metadata = errs
                        .iter()
                        .filter(|err| matches!(err, Some(DiskError::FileNotFound | DiskError::VolumeNotFound)))
                        .count();
                    if missing_metadata >= discovery_quorum {
                        if expected_incarnation_id.is_some() {
                            return Ok(None);
                        }
                        // Completion moves the authoritative upload metadata into the
                        // committed object before it removes the staging directory. A
                        // crash in that window intentionally leaves a reclaimable
                        // upload directory whose object name can still be proven for
                        // an exact-key listing by matching the namespace hash.
                        if !prefix.is_empty() && sha_dir == Self::get_multipart_sha_dir(bucket, prefix) {
                            let initiated = raw_upload_id
                                .rsplit_once('x')
                                .and_then(|(_, timestamp)| timestamp.parse::<i128>().ok())
                                .and_then(|timestamp| OffsetDateTime::from_unix_timestamp_nanos(timestamp).ok());
                            return Ok(Some(MultipartInfo {
                                bucket: bucket.to_owned(),
                                object: prefix.to_owned(),
                                upload_id: runtime_sources::deployment_upload_id(raw_upload_id),
                                initiated,
                                ..Default::default()
                            }));
                        }
                        return Ok(None);
                    }
                    // Affirmative per-upload damage (issue #5716): the staging
                    // namespace is one flat set of sha256(bucket/object)
                    // directories shared by every bucket, so a directory whose
                    // metadata is torn or undecodable must degrade to that
                    // upload alone. Erroring out instead turns one damaged
                    // directory into a permanent InternalError for every
                    // ListMultipartUploads of the bucket. Only the corrupt
                    // error family counts as damage — transient faults (offline
                    // disks, timeouts, IO churn) keep failing the listing below
                    // so clients retry instead of silently losing entries.
                    let corrupt_metadata = errs
                        .iter()
                        .filter(|err| err.as_ref().is_some_and(is_corrupt_upload_metadata_error))
                        .count();
                    if corrupt_metadata > 0 && missing_metadata + corrupt_metadata >= discovery_quorum {
                        debug!(
                            bucket,
                            upload_path = %upload_path,
                            missing_metadata,
                            corrupt_metadata,
                            "skipping multipart upload directory with corrupt metadata during listing"
                        );
                        return Ok(None);
                    }
                    let decoded: disk::error::Result<Option<(FileInfo, String)>> = (|| {
                        let (read_quorum, _) = Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count)?;
                        let read_quorum = usize::try_from(read_quorum).map_err(|_| DiskError::ErasureReadQuorum)?;
                        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
                            return Err(err);
                        }
                        let (_, mod_time, etag) = Self::list_online_disks(disks, &parts_metadata, &errs, read_quorum);
                        let file_info = Self::pick_valid_fileinfo(&parts_metadata, mod_time, etag, read_quorum)?;
                        if expected_incarnation_id
                            .is_some_and(|expected| !multipart_bucket_incarnation_matches(&file_info.metadata, expected))
                        {
                            return Ok(None);
                        }
                        if rustfs_utils::http::contains_key_str(
                            &file_info.metadata,
                            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
                        ) {
                            return Ok(None);
                        }

                        let object = match (
                            file_info.metadata.get(RUSTFS_MULTIPART_BUCKET_KEY),
                            file_info.metadata.get(RUSTFS_MULTIPART_OBJECT_KEY),
                        ) {
                            (Some(stored_bucket), Some(object)) if stored_bucket == bucket && !object.is_empty() => {
                                object.clone()
                            }
                            // A healthy upload that belongs to another bucket:
                            // not ours to list, and not corruption.
                            (Some(stored_bucket), Some(_)) if stored_bucket != bucket => return Ok(None),
                            _ => return Err(DiskError::CorruptedFormat),
                        };
                        Ok(Some((file_info, object)))
                    })();
                    let (file_info, object) = match decoded {
                        Ok(Some(decoded)) => decoded,
                        Ok(None) => return Ok(None),
                        // Deterministic damage (undecodable metadata that still
                        // reached quorum, or metadata without its owner keys):
                        // skip this upload only.
                        Err(err) if is_corrupt_upload_metadata_error(&err) => {
                            debug!(
                                bucket,
                                upload_path = %upload_path,
                                error = %err,
                                "skipping multipart upload directory with unidentifiable metadata during listing"
                            );
                            return Ok(None);
                        }
                        // Everything else — quorum loss from offline disks,
                        // timeouts, transport errors — can hide uploads that are
                        // actually fine; keep failing the listing so clients
                        // retry instead of silently losing entries.
                        Err(err) => return Err(err),
                    };
                    if !object.starts_with(prefix) {
                        return Ok(None);
                    }

                    let initiated = raw_upload_id
                        .rsplit_once('x')
                        .and_then(|(_, timestamp)| timestamp.parse::<i128>().ok())
                        .and_then(|timestamp| OffsetDateTime::from_unix_timestamp_nanos(timestamp).ok())
                        .or(file_info.mod_time);

                    Ok(Some(MultipartInfo {
                        bucket: bucket.to_owned(),
                        object,
                        upload_id: runtime_sources::deployment_upload_id(raw_upload_id),
                        initiated,
                        ..Default::default()
                    }))
                }
            })
            .buffer_unordered(MULTIPART_LIST_IO_CONCURRENCY)
            .collect::<Vec<disk::error::Result<Option<MultipartInfo>>>>()
            .await;

        let mut uploads = Vec::with_capacity(listed_uploads.len());
        for result in listed_uploads {
            if let Some(upload) = result.map_err(Error::from)? {
                uploads.push(upload);
            }
        }

        let mut common_prefixes = HashSet::new();
        let mut unfolded_uploads = Vec::with_capacity(uploads.len());
        let delimiter_value = delimiter.as_deref().filter(|delimiter| !delimiter.is_empty());
        for upload in uploads {
            let Some(delimiter) = delimiter_value else {
                unfolded_uploads.push(upload);
                continue;
            };
            let suffix = upload.object.strip_prefix(prefix).ok_or(DiskError::CorruptedFormat)?;
            if let Some((common_prefix, _)) = suffix.split_once(delimiter) {
                common_prefixes.insert(format!("{prefix}{common_prefix}{delimiter}"));
            } else {
                unfolded_uploads.push(upload);
            }
        }

        let page = paginate_multipart_listing(
            unfolded_uploads,
            common_prefixes.into_iter().collect(),
            key_marker.as_deref(),
            key_marker.as_ref().and(upload_id_marker.as_deref()),
            max_uploads,
            false,
        );

        Ok(ListMultipartsInfo {
            key_marker: key_marker.to_owned(),
            upload_id_marker: upload_id_marker.to_owned(),
            next_key_marker: page.next_key_marker,
            next_upload_id_marker: page.next_upload_id_marker,
            max_uploads,
            is_truncated: page.is_truncated,
            uploads: page.uploads,
            common_prefixes: page.common_prefixes,
            prefix: prefix.to_owned(),
            delimiter: delimiter.to_owned(),
        })
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::multipart::MultipartOperations for SetDisks {
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

    #[tracing::instrument(skip(self))]
    async fn copy_object_part(
        &self,
        _src_bucket: &str,
        _src_object: &str,
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
        Err(StorageError::NotImplemented)
    }

    #[tracing::instrument(level = "debug", skip(self, data, opts))]
    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        crate::hp_guard!("SetDisks::put_object_part");
        let upload_id_path = Self::get_multipart_upload_dir(bucket, object, upload_id, opts.data_movement);

        let (fi, _) = self
            .check_upload_id_exists_with_opts(bucket, object, upload_id, true, opts)
            .await?;
        ensure_data_movement_upload_access(&fi, bucket, object, upload_id, opts)?;
        ensure_multipart_bucket_incarnation(&self.ctx, &fi, bucket, object, upload_id, opts.expected_bucket_incarnation_id)
            .await?;

        let write_quorum = fi.write_quorum(self.default_write_quorum());

        if let Some(checksum) = fi.metadata.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM)
            && !checksum.is_empty()
            && data
                .as_hash_reader()
                .content_crc_type()
                .is_none_or(|v| v.to_string() != *checksum)
        {
            return Err(Error::other(format!("checksum mismatch: {checksum}")));
        }

        let disks = self.get_disks_internal().await;
        // let (disks, filtered_online) = self.filter_online_disks(disks_snapshot).await;

        // if filtered_online < write_quorum {
        //     warn!(
        //         "online disk snapshot {} below write quorum {} for multipart {}/{}; returning erasure write quorum error",
        //         filtered_online, write_quorum, bucket, object
        //     );
        //     return Err(to_object_err(Error::ErasureWriteQuorum, vec![bucket, object]));
        // }

        let mut shuffle_disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

        let part_suffix = format!("part.{part_id}");
        let tmp_part = format!("{}x{}", Uuid::new_v4(), OffsetDateTime::now_utc().unix_timestamp());
        let tmp_part_path = Arc::new(format!("{tmp_part}/{part_suffix}"));

        let result: Result<PartInfo> = async {
            let erasure =
                Arc::new(coding::Erasure::try_new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size)
                    .map_err(Error::from)?);
            let writer_setup_stage_start = rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now);

            let mut writers = Vec::with_capacity(shuffle_disks.len());
            let mut errors = Vec::with_capacity(shuffle_disks.len());
            for disk_op in shuffle_disks.iter() {
                if let Some(disk) = disk_op {
                    let writer = match create_bitrot_writer(
                        false,
                        Some(disk),
                        RUSTFS_META_TMP_BUCKET,
                        &tmp_part_path,
                        erasure.shard_file_size(data.size()),
                        erasure.shard_size(),
                        HashAlgorithm::HighwayHash256S,
                    )
                    .await
                    {
                        Ok(writer) => writer,
                        Err(err) => {
                            warn!(
                                event = EVENT_SET_DISK_MULTIPART,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_SET_DISK,
                                disk = ?disk,
                                state = "bitrot_writer_skipped",
                                error = ?err,
                                "Set disk multipart bitrot writer skipped"
                            );
                            errors.push(Some(err));
                            writers.push(None);
                            continue;
                        }
                    };

                    writers.push(Some(writer));
                    errors.push(None);
                } else {
                    errors.push(Some(DiskError::DiskNotFound));
                    writers.push(None);
                }
            }

            if let Some(stage_start) = writer_setup_stage_start {
                rustfs_io_metrics::record_put_object_stage_duration(
                    "multipart_set_disk_writer_setup",
                    stage_start.elapsed().as_secs_f64() * 1000.0,
                );
            }

            let nil_count = errors.iter().filter(|&e| e.is_none()).count();
            if nil_count < write_quorum {
                if let Some(write_err) = reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, write_quorum) {
                    log_multipart_write_quorum_failure(
                        MultipartWriteQuorumContext {
                            stage: MULTIPART_WRITE_QUORUM_WRITER_SETUP,
                            bucket,
                            object,
                            upload_id,
                            part_number: Some(part_id),
                        },
                        &errors,
                        write_quorum,
                        &write_err,
                    );
                    Err(to_object_err(write_err.into(), vec![bucket, object]))?;
                }

                Err(Error::other(format!("not enough disks to write: {errors:?}")))?;
            }

            // Capture the original part size before swapping the stream out for encoding.
            let multipart_part_size = data.size();
            let stream = mem::replace(
                &mut data.stream,
                HashReader::from_stream(Cursor::new(Vec::new()), 0, 0, None, None, false)?,
            );

            let write_path = classify_multipart_part_write_path(multipart_part_size, fi.erasure.block_size);
            rustfs_io_metrics::record_put_object_path(write_path.multipart_metric_label());
            let small_size_hint = if matches!(write_path, SmallWritePath::SingleBlockNonInline) {
                usize::try_from(multipart_part_size).map_err(Error::other)?
            } else {
                0
            };
            let encode_stage_start = rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now);

            let (reader, w_size) = match write_path {
                SmallWritePath::SingleBlockNonInline => {
                    Arc::clone(&erasure)
                        .encode_single_block_non_inline_with_size_hint(stream, &mut writers, write_quorum, small_size_hint)
                        .await?
                }
                SmallWritePath::PipelineBatchedLarge => {
                    Arc::clone(&erasure).encode_batched(stream, &mut writers, write_quorum).await?
                }
                SmallWritePath::Inline | SmallWritePath::Pipeline => Arc::clone(&erasure).encode(stream, &mut writers, write_quorum).await?,
            };

            if let Some(stage_start) = encode_stage_start {
                rustfs_io_metrics::record_put_object_stage_duration(
                    "multipart_set_disk_encode",
                    stage_start.elapsed().as_secs_f64() * 1000.0,
                );
            }

            let _ = mem::replace(&mut data.stream, reader);

            if (w_size as i64) < data.size() {
                warn!(
                    event = EVENT_SET_DISK_MULTIPART,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    part_number = part_id,
                    written_size = w_size,
                    expected_size = data.size(),
                    state = "short_write",
                    "Set disk multipart write produced fewer bytes than expected"
                );
                Err(Error::other(format!(
                    "put_object_part write size < data.size(), w_size={}, data.size={}",
                    w_size,
                    data.size()
                )))?;
            }

            let committed_shards = drop_failed_writer_disks(&mut shuffle_disks, &writers);
            if committed_shards < write_quorum {
                Err(Error::other(format!(
                    "put_object_part write quorum unavailable after encode: {committed_shards} shard(s) committed, need {write_quorum}"
                )))?;
            }

            let index_op = data
                .stream
                .try_get_index()
                .map(crate::io_support::rio::compression_index_storage_bytes);

            let mut etag = data.stream.try_resolve_etag().unwrap_or_default();

            if let Some(ref tag) = opts.preserve_etag {
                etag = tag.clone();
            }

            let mut actual_size = data.actual_size();
            if actual_size < 0 {
                let is_compressed = fi.is_compressed();
                if !is_compressed {
                    actual_size = w_size as i64;
                }
            }

            if fi.is_compressed() {
                record_compression_total_memory(actual_size as u64, w_size as u64).await;
            }
            let checksums = data.as_hash_reader().content_crc();

            let part_info = ObjectPartInfo {
                etag: etag.clone(),
                number: part_id,
                size: w_size,
                mod_time: Some(OffsetDateTime::now_utc()),
                actual_size,
                index: index_op,
                checksums: if checksums.is_empty() { None } else { Some(checksums) },
                ..Default::default()
            };

            let part_info_buff = part_info.marshal_msg()?;

            drop(writers); // drop writers to close all files

            if fi.erasure.parity_blocks == 0 {
                let written_size = i64::try_from(w_size).map_err(|_| Error::other("put_object_part written size overflows i64"))?;
                let logical_shard_size = usize::try_from(erasure.shard_file_size(written_size))
                    .map_err(|_| Error::other("put_object_part shard size overflows usize"))?;
                verify_written_bitrot_shards(
                    &shuffle_disks,
                    None,
                    BitrotSelfVerifyTarget {
                        operation: "put_object_part",
                        bucket,
                        object,
                        part_number: Some(part_id),
                        volume: RUSTFS_META_TMP_BUCKET,
                        path: &tmp_part_path,
                        logical_shard_size,
                        shard_size: erasure.shard_size(),
                        write_quorum,
                    },
                )
                .await?;
            }

            let part_path = format!("{}/{}/{}", upload_id_path, fi.data_dir.unwrap_or_default(), part_suffix);
            let part_lock_path = format!("{upload_id_path}/{part_suffix}");

            #[cfg(test)]
            pause_multipart_commit(bucket, object, MultipartCommitPause::PutPartBeforeLockAcquire).await;
            // Serialize only same-part commits (rename_part), not the whole upload.
            // Each concurrent stream writes to its own unique temp dir (see
            // `tmp_part` above), so the encode/stream phase never conflicts and must
            // stay lock-free — holding a lock across it would serialize slow
            // re-transmits of the same part and defeat the S3 "last finisher wins"
            // semantics. The mixed-generation hazard is confined to rename_part,
            // where two temp parts are moved cross-disk onto the SAME final
            // part_path: interleaving there can leave shards from two generations,
            // each individually bitrot-valid, that only surface as silent corruption
            // at read time (backlog#853). A write lock scoped to this part number
            // makes each same-part commit atomic across disks, so the last committer
            // wins consistently, while different part numbers commit onto disjoint
            // part paths and stay concurrent (issue#5961 — an uploadId-wide write
            // lock serialized them into 503 lock-acquire timeouts). The shared
            // uploadId read lock keeps completion/abort (which take the uploadId
            // write lock) from racing any in-flight part commit; a guarded
            // completion takes the object lock before the upload lock to preserve
            // global ordering.
            let (_upload_commit_guard, _part_commit_guard) = if opts.no_lock {
                (None, None)
            } else {
                let upload_guard = self
                    .acquire_read_lock_diag("put_object_part_commit", RUSTFS_META_MULTIPART_BUCKET, &upload_id_path)
                    .await?;
                let part_guard = self
                    .acquire_write_lock_diag("put_object_part_commit", RUSTFS_META_MULTIPART_BUCKET, &part_lock_path)
                    .await?;
                (Some(upload_guard), Some(part_guard))
            };
            let (commit_fi, _) = self
                .check_upload_id_exists_with_opts(bucket, object, upload_id, false, opts)
                .await?;
            ensure_data_movement_upload_access(&commit_fi, bucket, object, upload_id, opts)?;
            ensure_multipart_bucket_incarnation(
                &self.ctx,
                &commit_fi,
                bucket,
                object,
                upload_id,
                opts.expected_bucket_incarnation_id,
            )
            .await?;
            #[cfg(test)]
            pause_multipart_commit(bucket, object, MultipartCommitPause::PutPartBeforeLockLost).await;
            fence_commit_on_lock_loss(_upload_commit_guard.as_ref(), "put_object_part_commit", &upload_id_path)?;
            fence_commit_on_lock_loss(_part_commit_guard.as_ref(), "put_object_part_commit", &part_lock_path)?;
            ensure_multipart_bucket_lifecycle_lock_held(bucket, object, opts)?;

            let _ = self
                .rename_part(
                    &shuffle_disks,
                    RUSTFS_META_TMP_BUCKET,
                    &tmp_part_path,
                    RUSTFS_META_MULTIPART_BUCKET,
                    &part_path,
                    part_info_buff.into(),
                    write_quorum,
                    Some(MultipartWriteQuorumContext {
                        stage: MULTIPART_WRITE_QUORUM_RENAME_PART,
                        bucket,
                        object,
                        upload_id,
                        part_number: Some(part_id),
                    }),
                )
                .await?;

            #[cfg(test)]
            pause_multipart_commit(bucket, object, MultipartCommitPause::PutPartAfterRename).await;
            drop(_part_commit_guard);
            drop(_upload_commit_guard);

            let ret: PartInfo = PartInfo {
                etag: Some(etag.clone()),
                part_num: part_id,
                last_mod: Some(OffsetDateTime::now_utc()),
                size: w_size,
                actual_size,
            };

            // error!("put_object_part ret {:?}", &ret);

            Ok(ret)
        }
        .await;

        if result.is_err()
            && let Err(err) = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_part).await
        {
            warn!(tmp_part = %tmp_part, error = ?err, "failed to cleanup multipart temporary data");
        }

        result
    }

    #[tracing::instrument(skip(self))]
    async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        mut max_parts: usize,
        opts: &ObjectOptions,
    ) -> Result<ListPartsInfo> {
        let _upload_guard = self
            .acquire_multipart_upload_read_lock("list_object_parts", bucket, object, upload_id, opts)
            .await?;
        let (fi, _) = self
            .check_upload_id_exists_with_opts(bucket, object, upload_id, false, opts)
            .await?;
        ensure_data_movement_upload_access(&fi, bucket, object, upload_id, opts)?;
        ensure_multipart_bucket_incarnation(&self.ctx, &fi, bucket, object, upload_id, opts.expected_bucket_incarnation_id)
            .await?;

        let upload_id_path = Self::get_multipart_upload_dir(bucket, object, upload_id, opts.data_movement);

        if max_parts > MAX_PARTS_COUNT {
            max_parts = MAX_PARTS_COUNT;
        }

        let part_number_marker = part_number_marker.unwrap_or_default();

        // Extract storage class from metadata, default to STANDARD if not found
        let storage_class = fi
            .metadata
            .get(AMZ_STORAGE_CLASS)
            .cloned()
            .unwrap_or_else(|| storageclass::STANDARD.to_string());

        let mut ret = ListPartsInfo {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            upload_id: upload_id.to_owned(),
            storage_class,
            max_parts,
            part_number_marker,
            user_defined: {
                let mut metadata = fi.metadata.clone();
                strip_internal_multipart_metadata(&mut metadata);
                metadata
            },
            ..Default::default()
        };

        if max_parts == 0 {
            return Ok(ret);
        }

        let online_disks = self.get_disks_internal().await;

        let read_quorum = fi.read_quorum(self.default_read_quorum());

        let part_path = format!(
            "{}{}",
            path_join_buf(&[
                &upload_id_path,
                fi.data_dir.map(|v| v.to_string()).unwrap_or_default().as_str(),
            ]),
            SLASH_SEPARATOR
        );

        let mut part_numbers = match Self::list_parts(&online_disks, &part_path, read_quorum).await {
            Ok(parts) => parts,
            Err(err) => {
                if err == DiskError::FileNotFound {
                    return Ok(ret);
                }

                return Err(to_object_err(err.into(), vec![bucket, object]));
            }
        };

        if part_numbers.is_empty() {
            return Ok(ret);
        }
        let Some(remaining_part_numbers) = parts_after_marker(&part_numbers, part_number_marker) else {
            return Ok(ret);
        };
        part_numbers = remaining_part_numbers.to_vec();

        let mut parts = Vec::with_capacity(part_numbers.len());

        let part_meta_paths = part_numbers
            .iter()
            .map(|v| format!("{part_path}part.{v}.meta"))
            .collect::<Vec<String>>();

        let object_parts =
            Self::read_parts(&online_disks, RUSTFS_META_MULTIPART_BUCKET, &part_meta_paths, &part_numbers, read_quorum)
                .await
                .map_err(|e| to_object_err(e.into(), vec![bucket, object, upload_id]))?;

        let mut count = max_parts;

        for part in object_parts.iter() {
            if let Some(err) = &part.error {
                warn!("list_object_parts part error: {:?}", &err);
            }

            parts.push(PartInfo {
                etag: Some(part.etag.clone()),
                part_num: part.number,
                last_mod: part.mod_time,
                size: part.size,
                actual_size: part.actual_size,
            });

            count -= 1;
            if count == 0 {
                break;
            }
        }

        ret.parts = parts;

        if object_parts.len() > ret.parts.len() {
            ret.is_truncated = true;
            ret.next_part_number_marker = ret.parts.last().map(|v| v.part_num).unwrap_or_default();
        }

        ensure_multipart_bucket_lifecycle_lock_held(bucket, object, opts)?;
        Ok(ret)
    }

    #[tracing::instrument(skip(self))]
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo> {
        let bucket_lifecycle_guard = if crate::bucket::utils::is_meta_bucketname(bucket) {
            None
        } else {
            Some(
                metadata_sys::object_store_in(&self.ctx)
                    .await?
                    .acquire_bucket_lifecycle_read_lock(bucket)
                    .await?,
            )
        };
        let expected_incarnation_id = if bucket_lifecycle_guard.is_some() {
            Some(metadata_sys::get_bucket_incarnation_id_in(&self.ctx, bucket).await?)
        } else {
            None
        };
        let result = self
            .list_multipart_uploads_for_incarnation(
                bucket,
                prefix,
                key_marker,
                upload_id_marker,
                delimiter,
                max_uploads,
                expected_incarnation_id,
            )
            .await;

        if bucket_lifecycle_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "multipart_bucket_generation",
                bucket: bucket.to_string(),
                object: prefix.to_string(),
                required: 1,
                achieved: 0,
            });
        }

        result
    }

    #[tracing::instrument(skip(self))]
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        crate::hp_guard!("SetDisks::new_multipart_upload");
        let storage_class_config = self.storage_class_config_snapshot();
        let mut _object_lock_guard = None;

        if opts.http_preconditions.is_some() {
            if !opts.no_lock {
                _object_lock_guard = Some(
                    self.acquire_write_lock_diag("new_multipart_upload_precondition", bucket, object)
                        .await?,
                );
            }

            if let Some(err) = self.check_write_precondition(bucket, object, opts).await {
                return Err(err);
            }
        }

        let disks = self.disks.read().await;

        let disks = disks.clone();

        let mut user_defined = opts.user_defined.clone();
        rustfs_utils::http::remove_str(&mut user_defined, rustfs_utils::http::SUFFIX_PART_CHECKSUMS);
        if !opts.data_movement {
            rustfs_utils::http::remove_str(&mut user_defined, rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD);
        }
        rustfs_utils::http::metadata_compat::remove_str(
            &mut user_defined,
            crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
        );
        rustfs_utils::http::metadata_compat::remove_str(
            &mut user_defined,
            crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
        );

        if let Some(ref etag) = opts.preserve_etag {
            user_defined.insert("etag".to_owned(), etag.clone());
        }

        if let Some(sc) = user_defined.get(AMZ_STORAGE_CLASS)
            && sc == storageclass::STANDARD
        {
            let _ = user_defined.remove(AMZ_STORAGE_CLASS);
        }

        let WriteLayout {
            data_drives,
            parity_drives,
            write_quorum,
        } = resolve_write_layout(
            &storage_class_config,
            self.pool_index,
            disks.len(),
            self.default_parity_count,
            user_defined.get(AMZ_STORAGE_CLASS).map(String::as_str),
            opts.max_parity,
        )?;

        let mut fi = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives);

        fi.version_id = if let Some(vid) = &opts.version_id {
            Some(Uuid::parse_str(vid)?)
        } else {
            None
        };

        if opts.versioned && opts.version_id.is_none() {
            fi.version_id = Some(Uuid::new_v4());
        }

        let data_dir = Uuid::new_v4();
        fi.data_dir = Some(data_dir);
        if crate::object_api::legacy_encrypted_range_seek_enabled()
            && !opts.no_lock
            && should_persist_encryption_original_size(&user_defined)
        {
            insert_str(
                &mut user_defined,
                crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
                data_dir.to_string(),
            );
        }

        if let Some(cssum) = get_header_map(&user_defined, SUFFIX_REPLICATION_SSEC_CRC)
            && !cssum.is_empty()
        {
            fi.checksum = base64_simd::STANDARD.decode_to_vec(&cssum).ok().map(Bytes::from);
            remove_header_map(&mut user_defined, SUFFIX_REPLICATION_SSEC_CRC);
        }

        let parts_metadata = vec![fi.clone(); disks.len()];

        if !user_defined.contains_key("content-type") {
            // TODO(backlog): detect content-type from part data when header is missing
        }

        if let Some(sc) = user_defined.get(AMZ_STORAGE_CLASS)
            && sc == storageclass::STANDARD
        {
            let _ = user_defined.remove(AMZ_STORAGE_CLASS);
        }

        if let Some(checksum) = &opts.want_checksum {
            user_defined.insert(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM.to_string(), checksum.checksum_type.to_string());
            user_defined.insert(
                rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE.to_string(),
                checksum.checksum_type.obj_type().to_string(),
            );
        }

        user_defined.insert(RUSTFS_MULTIPART_BUCKET_KEY.to_string(), bucket.to_string());
        user_defined.insert(RUSTFS_MULTIPART_OBJECT_KEY.to_string(), object.to_string());
        if let Some(incarnation_id) = opts.expected_bucket_incarnation_id {
            insert_str(&mut user_defined, SUFFIX_BUCKET_INCARNATION_ID, incarnation_id.to_string());
        }

        let (shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);
        let mod_time = opts.mod_time.unwrap_or_else(OffsetDateTime::now_utc);

        for f in parts_metadatas.iter_mut() {
            f.metadata = user_defined.clone();
            f.mod_time = Some(mod_time);
            f.fresh = true;
        }

        let upload_uuid = format!("{}x{}", Uuid::new_v4(), mod_time.unix_timestamp_nanos());

        // fi.mod_time = Some(now);

        let upload_id = runtime_sources::deployment_upload_id(&upload_uuid);

        let upload_path = Self::get_multipart_upload_dir(bucket, object, upload_uuid.as_str(), opts.data_movement);

        #[cfg(any(test, feature = "test-util"))]
        pause_multipart_commit(bucket, object, MultipartCommitPause::NewUploadBeforeLockLost).await;
        if _object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "new_multipart_upload_commit",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
        if opts
            .namespace_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
        {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "new_multipart_upload_outer_lock",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
        ensure_multipart_bucket_lifecycle_lock_held(bucket, object, opts)?;
        Self::write_unique_file_info(
            &shuffle_disks,
            bucket,
            RUSTFS_META_MULTIPART_BUCKET,
            upload_path.as_str(),
            &parts_metadatas,
            write_quorum,
        )
        .await
        .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;
        #[cfg(test)]
        observe_new_multipart_upload_commit(bucket, object);

        // evalDisks

        Ok(MultipartUploadResult {
            upload_id,
            checksum_algo: user_defined.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM).cloned(),
            checksum_type: user_defined.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE).cloned(),
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        let _upload_guard = self
            .acquire_multipart_upload_read_lock("get_multipart_info", bucket, object, upload_id, opts)
            .await?;
        let (mut fi, _) = self
            .check_upload_id_exists_with_opts(bucket, object, upload_id, false, opts)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object, upload_id]))?;
        ensure_data_movement_upload_access(&fi, bucket, object, upload_id, opts)?;
        ensure_multipart_bucket_incarnation(&self.ctx, &fi, bucket, object, upload_id, opts.expected_bucket_incarnation_id)
            .await?;
        ensure_multipart_bucket_lifecycle_lock_held(bucket, object, opts)?;

        Ok(MultipartInfo {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            upload_id: upload_id.to_owned(),
            user_defined: {
                strip_internal_multipart_metadata(&mut fi.metadata);
                fi.metadata.clone()
            },
            ..Default::default()
        })
    }

    #[tracing::instrument(skip(self))]
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()> {
        let _upload_guard = self
            .acquire_multipart_upload_write_lock("abort_multipart_upload", bucket, object, upload_id, opts)
            .await?;
        let (fi, _) = self
            .check_upload_id_exists_with_opts(bucket, object, upload_id, true, opts)
            .await?;
        ensure_data_movement_upload_access(&fi, bucket, object, upload_id, opts)?;
        ensure_multipart_bucket_incarnation(&self.ctx, &fi, bucket, object, upload_id, opts.expected_bucket_incarnation_id)
            .await?;
        ensure_multipart_bucket_lifecycle_lock_held(bucket, object, opts)?;
        let upload_id_path = Self::get_multipart_upload_dir(bucket, object, upload_id, opts.data_movement);

        self.delete_all_with_quorum(
            RUSTFS_META_MULTIPART_BUCKET,
            &upload_id_path,
            fi.write_quorum(self.default_write_quorum()),
        )
        .await
    }
    // complete_multipart_upload finished
    #[tracing::instrument(skip(self))]
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        crate::hp_guard!("SetDisks::complete_multipart_upload");
        self.invalidate_get_object_metadata_cache(bucket, object).await;

        let upload_id_path = Self::get_multipart_upload_dir(bucket, object, upload_id, opts.data_movement);
        let range_seek_rollout_enabled = crate::object_api::legacy_encrypted_range_seek_enabled() && !opts.no_lock;
        let mut object_lock_guard = None;

        if opts.http_preconditions.is_some() {
            if !opts.no_lock {
                object_lock_guard = Some(
                    self.acquire_write_lock_diag("complete_multipart_upload_precondition", bucket, object)
                        .await?,
                );
            }

            if let Some(err) = self.check_write_precondition(bucket, object, opts).await {
                return Err(err);
            }
        }

        if !opts.no_lock && object_lock_guard.is_none() {
            object_lock_guard = Some(
                self.acquire_write_lock_diag("complete_multipart_upload_commit", bucket, object)
                    .await?,
            );
        }
        let upload_guard = self
            .acquire_multipart_upload_write_lock("complete_multipart_upload_commit", bucket, object, upload_id, opts)
            .await?;

        let expected_restore_operation_id = restore_commit_operation_id_from_metadata(&opts.user_defined)?;
        let (mut fi, files_metas) = self
            .check_upload_id_exists_with_opts(bucket, object, upload_id, true, opts)
            .await?;
        ensure_data_movement_upload_access(&fi, bucket, object, upload_id, opts)?;
        ensure_multipart_bucket_incarnation(&self.ctx, &fi, bucket, object, upload_id, opts.expected_bucket_incarnation_id)
            .await?;
        let has_layout_candidate = range_seek_rollout_enabled
            && fi
                .data_dir
                .filter(|data_dir| !data_dir.is_nil())
                .map(|data_dir| data_dir.to_string())
                .is_some_and(|token| {
                    crate::object_api::has_encrypted_part_layout_marker(
                        &fi.metadata,
                        crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
                        &token,
                    )
                });
        let quorum_validated_layout_token = if has_layout_candidate {
            fi.data_dir
                .filter(|data_dir| !data_dir.is_nil())
                .map(|data_dir| data_dir.to_string())
                .filter(|token| {
                    crate::object_api::has_encrypted_part_layout_marker(
                        &fi.metadata,
                        crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
                        token,
                    )
                })
        } else {
            None
        };
        rustfs_utils::http::metadata_compat::remove_str(
            &mut fi.metadata,
            crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
        );
        rustfs_utils::http::metadata_compat::remove_str(&mut fi.metadata, crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX);
        if expected_restore_operation_id.is_some() {
            rustfs_utils::http::metadata_compat::remove_str(&mut fi.metadata, SUFFIX_RESTORE_OPERATION_ID);
        }
        if opts.versioned {
            fi.version_id = Some(
                fi.version_id
                    .filter(|version_id| !version_id.is_nil())
                    .unwrap_or_else(Uuid::new_v4),
            );
        } else {
            fi.version_id = None;
        }
        let write_quorum = fi.write_quorum(self.default_write_quorum());
        let read_quorum = fi.read_quorum(self.default_read_quorum());

        let disks = self.disks.read().await;

        let disks = disks.clone();
        // let disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

        let part_path = format!("{}/{}/", upload_id_path, fi.data_dir.unwrap_or(Uuid::nil()));
        self.recover_part_transactions(&part_path, read_quorum, write_quorum)
            .await
            .map_err(|err| to_object_err(err.into(), vec![bucket, object]))?;

        let part_meta_paths = uploaded_parts
            .iter()
            .map(|v| format!("{part_path}part.{0}.meta", v.part_num))
            .collect::<Vec<String>>();

        let part_numbers = uploaded_parts.iter().map(|v| v.part_num).collect::<Vec<usize>>();

        let object_parts = Self::read_parts(&disks, RUSTFS_META_MULTIPART_BUCKET, &part_meta_paths, &part_numbers, read_quorum)
            .await
            .map_err(|err| to_object_err(err.into(), vec![bucket, object]))?;

        if object_parts.len() != uploaded_parts.len() {
            return Err(Error::other("part result number err"));
        }

        let mut checksum_type = rustfs_rio::ChecksumType::NONE;

        if let Some(cs) = fi.metadata.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM) {
            let Some(ct) = fi.metadata.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE) else {
                return Err(Error::other("checksum type not found"));
            };

            checksum_type = rustfs_rio::ChecksumType::from_string_with_obj_type(cs, ct);
            if let Some(want) = opts.want_checksum.as_ref()
                && !want.checksum_type.is(checksum_type)
            {
                return Err(Error::other(format!("checksum type mismatch, got {:?}, want {:?}", want, checksum_type)));
            }
        }

        for (i, part) in object_parts.iter().enumerate() {
            if let Some(err) = &part.error {
                let mapped_err = complete_multipart_part_error(uploaded_parts[i].part_num, err, bucket, object);
                let result = complete_multipart_part_error_result(&mapped_err);
                if matches!(mapped_err, Error::InvalidPart(_, _, _)) {
                    debug!(
                        target: "rustfs_ecstore::set_disk",
                        event = EVENT_SET_DISK_MULTIPART,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        op = "complete_multipart_upload",
                        result = result,
                        bucket = %bucket,
                        object = %object,
                        upload_id = %upload_id,
                        uploaded_part_num = uploaded_parts[i].part_num,
                        observed_part_num = part.number,
                        read_quorum = read_quorum,
                        write_quorum = write_quorum,
                        error = %err,
                        "Set disk multipart part missing"
                    );
                } else {
                    warn!(
                        target: "rustfs_ecstore::set_disk",
                        event = EVENT_SET_DISK_MULTIPART,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        op = "complete_multipart_upload",
                        result = result,
                        bucket = %bucket,
                        object = %object,
                        upload_id = %upload_id,
                        uploaded_part_num = uploaded_parts[i].part_num,
                        observed_part_num = part.number,
                        read_quorum = read_quorum,
                        write_quorum = write_quorum,
                        error = %err,
                        "Set disk multipart part resolution failed"
                    );
                }
                return Err(mapped_err);
            }

            if uploaded_parts[i].part_num != part.number {
                error!(
                    "complete_multipart_upload part_id err part_id != part_num {} != {}",
                    uploaded_parts[i].part_num, part.number
                );
                return Err(Error::InvalidPart(uploaded_parts[i].part_num, bucket.to_owned(), object.to_owned()));
            }

            fi.add_object_part(
                part.number,
                part.etag.clone(),
                part.size,
                part.mod_time,
                part.actual_size,
                part.index.clone(),
                part.checksums.clone(),
            );
        }

        let (shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata_by_index(&disks, &files_metas, &fi);

        let curr_fi = fi.clone();

        fi.parts = Vec::with_capacity(uploaded_parts.len());

        let quota_context = reservation::begin(
            &self.ctx,
            bucket,
            object,
            opts.quota_admission,
            opts.data_movement,
            self.pool_index,
            self.set_index,
        )
        .await?;
        let quota_mutation_fence = quota_context.is_enforced() || opts.quota_admission.is_some();
        let preserve_replication_ciphertext = opts.replication_request
            && contains_key_str(&fi.metadata, rustfs_utils::http::SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT);
        if quota_context.is_enforced() && preserve_replication_ciphertext {
            return Err(Error::PartMissingOrCorrupt);
        }
        let transformed_object = fi.is_compressed() || should_persist_encryption_original_size(&fi.metadata);
        let data_movement_actual_size = if opts.data_movement {
            rustfs_utils::http::get_consistent_str(&opts.user_defined, SUFFIX_ACTUAL_SIZE)
                .map(|value| {
                    value
                        .parse::<i64>()
                        .ok()
                        .filter(|value| *value >= 0)
                        .ok_or(Error::PartMissingOrCorrupt)
                })
                .transpose()?
        } else {
            None
        };
        let data_movement_actual_size_u64 = data_movement_actual_size
            .map(u64::try_from)
            .transpose()
            .map_err(|_| Error::PartMissingOrCorrupt)?;

        let mut object_size: usize = 0;
        let mut object_actual_size: i64 = 0;

        let mut checksum_combined = bytes::BytesMut::new();
        let mut checksum = rustfs_rio::Checksum {
            checksum_type,
            ..Default::default()
        };

        // Build a lookup map for O(1) part resolution instead of O(n) find() in the loop
        // This optimizes from O(n^2) to O(n) when processing many parts
        use std::collections::{HashMap, HashSet};
        let part_lookup: HashMap<usize, &ObjectPartInfo> = curr_fi.parts.iter().map(|part| (part.number, part)).collect();

        for (i, p) in uploaded_parts.iter().enumerate() {
            let Some(ext_part) = part_lookup.get(&p.part_num) else {
                error!(
                    "complete_multipart_upload part not found: part_id={}, bucket={}, object={}",
                    p.part_num, bucket, object
                );
                return Err(Error::InvalidPart(p.part_num, "".to_owned(), p.etag.clone().unwrap_or_default()));
            };
            debug!(
                target:"rustfs_ecstore::set_disk",
                event = EVENT_SET_DISK_MULTIPART,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                part_number = p.part_num,
                part_size = ext_part.size,
                part_actual_size = ext_part.actual_size,
                state = "part_validated",
                "Set disk multipart part validated"
            );

            // Normalize ETags by removing quotes before comparison (PR #592 compatibility)
            let client_etag = p.etag.as_ref().map(|e| rustfs_utils::path::trim_etag(e));
            let stored_etag = Some(rustfs_utils::path::trim_etag(&ext_part.etag));
            if client_etag != stored_etag {
                error!(
                    "complete_multipart_upload etag err client={:?}, stored={:?}, part_id={}, bucket={}, object={}",
                    p.etag, ext_part.etag, p.part_num, bucket, object
                );
                return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
            }

            // TODO(backlog): integrate encryption verification during complete multipart

            if (i < uploaded_parts.len() - 1)
                && !(opts.data_movement && ext_part.actual_size < 0)
                && !is_min_allowed_part_size(ext_part.actual_size)
            {
                error!(
                    "complete_multipart_upload part size too small: part {} size {} is less than minimum {}",
                    p.part_num,
                    ext_part.actual_size,
                    GLOBAL_MIN_PART_SIZE.as_u64()
                );
                return Err(Error::EntityTooSmall(
                    p.part_num,
                    ext_part.actual_size,
                    GLOBAL_MIN_PART_SIZE.as_u64() as i64,
                ));
            }

            if checksum_type.is_set() {
                let Some(crc) = ext_part
                    .checksums
                    .as_ref()
                    .and_then(|f| f.get(checksum_type.to_string().as_str()))
                    .cloned()
                else {
                    error!(
                        "complete_multipart_upload fi.checksum not found type={checksum_type}, part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                };

                let Some(part_crc) = complete_part_checksum(p, checksum_type) else {
                    error!(
                        "complete_multipart_upload checksum type={checksum_type}, part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                };

                if let Some(part_crc) = part_crc
                    && part_crc != crc
                {
                    error!("complete_multipart_upload checksum_type={checksum_type:?}, part_crc={part_crc:?}, crc={crc:?}");
                    error!(
                        "complete_multipart_upload checksum mismatch part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                }

                let Some(cs) = rustfs_rio::Checksum::new_with_type(checksum_type, &crc) else {
                    error!(
                        "complete_multipart_upload checksum new_with_type failed part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                };

                if !cs.valid() {
                    error!(
                        "complete_multipart_upload checksum valid failed part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                }

                if checksum_type.full_object_requested()
                    && let Err(err) = checksum.add_part(&cs, ext_part.actual_size)
                {
                    error!(
                        "complete_multipart_upload checksum add_part failed part_id={}, bucket={}, object={}, err={}",
                        p.part_num, bucket, object, err
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                }

                checksum_combined.extend_from_slice(cs.raw.as_slice());
            }

            object_size = object_size.checked_add(ext_part.size).ok_or(Error::PartMissingOrCorrupt)?;
            let unknown_actual_size_allowed = opts.data_movement && transformed_object && data_movement_actual_size.is_some()
                || opts.replication_request && !quota_context.is_enforced();
            if ext_part.actual_size < 0 && !unknown_actual_size_allowed {
                return Err(Error::PartMissingOrCorrupt);
            }
            let normalized_actual_size = if ext_part.actual_size >= 0 && !transformed_object {
                ext_part
                    .actual_size
                    .max(i64::try_from(ext_part.size).map_err(|_| Error::PartMissingOrCorrupt)?)
            } else {
                ext_part.actual_size
            };
            object_actual_size = object_actual_size
                .checked_add(normalized_actual_size)
                .ok_or(Error::PartMissingOrCorrupt)?;
            let mut completed_part = completed_multipart_object_part(p.part_num, ext_part);
            completed_part.actual_size = normalized_actual_size;
            fi.parts.push(completed_part);
        }
        if !transformed_object && data_movement_actual_size.is_some_and(|actual_size| actual_size < object_actual_size) {
            return Err(Error::PartMissingOrCorrupt);
        }
        if let Some(wtcs) = opts.want_checksum.as_ref() {
            if checksum_type.full_object_requested() {
                if wtcs.encoded != checksum.encoded {
                    error!(
                        "complete_multipart_upload checksum mismatch want={}, got={}",
                        wtcs.encoded, checksum.encoded
                    );
                    return Err(Error::other(format!(
                        "complete_multipart_upload checksum mismatch want={}, got={}",
                        wtcs.encoded, checksum.encoded
                    )));
                }
            } else if let Err(err) = wtcs.matches(&checksum_combined, uploaded_parts.len() as i32) {
                error!(
                    "complete_multipart_upload checksum matches failed want={}, got={}, err={}",
                    wtcs.encoded, checksum.encoded, err
                );
                return Err(Error::other(format!(
                    "complete_multipart_upload checksum matches failed want={}, got={}",
                    wtcs.encoded, checksum.encoded
                )));
            }
        }

        let declared_replication_actual_size = opts
            .replication_request
            .then(|| get_str(&opts.user_defined, SUFFIX_ACTUAL_OBJECT_SIZE_CAP))
            .flatten();
        let replication_actual_size = if opts.replication_request && quota_context.is_enforced() {
            let observed_size = u64::try_from(object_actual_size).map_err(|_| Error::PartMissingOrCorrupt)?;
            let declared_cap = declared_replication_actual_size
                .as_deref()
                .map(|value| value.parse::<u64>().map_err(|_| Error::PartMissingOrCorrupt))
                .transpose()?
                .unwrap_or(0);
            let declared_encryption_size = rustfs_utils::http::get_object_encryption_original_size(&fi.metadata)
                .map_err(Error::other)?
                .map(u64::try_from)
                .transpose()
                .map_err(|_| Error::PartMissingOrCorrupt)?
                .unwrap_or(0);
            Some(observed_size.max(declared_cap).max(declared_encryption_size))
        } else {
            None
        };
        let data_movement_quota_size = data_movement_actual_size_u64.filter(|_| quota_context.is_enforced());
        let quota_new_size = match data_movement_quota_size.or(replication_actual_size) {
            Some(size) => size.max(u64::try_from(object_size).map_err(|_| Error::PartMissingOrCorrupt)?),
            None if quota_context.is_enforced() => u64::try_from(object_actual_size)
                .map_err(|_| Error::PartMissingOrCorrupt)?
                .max(u64::try_from(object_size).map_err(|_| Error::PartMissingOrCorrupt)?),
            None => 0,
        };
        if let Some(rc_crc) = get_header_map(&opts.user_defined, SUFFIX_REPLICATION_SSEC_CRC) {
            if let Ok(rc_crc_bytes) = base64_simd::STANDARD.decode_to_vec(&rc_crc) {
                fi.checksum = Some(Bytes::from(rc_crc_bytes));
            } else {
                error!("complete_multipart_upload decode rc_crc failed rc_crc={}", rc_crc);
            }
        }

        // The SSE-C passthrough session marker is upload-scoped; drop it from
        // the completed object's metadata.
        rustfs_utils::http::metadata_compat::remove_str(
            &mut fi.metadata,
            rustfs_utils::http::SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT,
        );

        if checksum_type.is_set() {
            checksum_type
                .merge(rustfs_rio::ChecksumType::MULTIPART)
                .merge(rustfs_rio::ChecksumType::INCLUDES_MULTIPART);
            if !checksum_type.full_object_requested() {
                checksum = rustfs_rio::Checksum::new_from_data(checksum_type, &checksum_combined)
                    .ok_or_else(|| Error::other("checksum new_from_data failed"))?;
            }
            fi.checksum = Some(checksum.to_bytes(&checksum_combined));
        }

        fi.metadata.remove(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM);
        fi.metadata.remove(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE);
        strip_internal_multipart_metadata(&mut fi.metadata);

        fi.size = object_size as i64;
        fi.mod_time = opts.mod_time;
        if fi.mod_time.is_none() {
            fi.mod_time = Some(OffsetDateTime::now_utc());
        }

        // etag
        let etag = resolve_complete_etag(opts, &uploaded_parts);

        fi.metadata.insert("etag".to_owned(), etag);

        let persist_encryption_original_size = should_persist_encryption_original_size(&fi.metadata);
        if persist_encryption_original_size && let Some(layout_token) = quorum_validated_layout_token {
            insert_str(&mut fi.metadata, crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX, layout_token);
        } else {
            rustfs_utils::http::metadata_compat::remove_str(
                &mut fi.metadata,
                crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
            );
        }

        if let Some(actual_size) = data_movement_actual_size {
            insert_str(&mut fi.metadata, SUFFIX_ACTUAL_SIZE, actual_size.to_string());
            if persist_encryption_original_size {
                fi.metadata
                    .insert("x-rustfs-encryption-original-size".to_string(), actual_size.to_string());
            }
        } else if opts.replication_request {
            if let Some(actual_size) = replication_actual_size {
                insert_str(&mut fi.metadata, SUFFIX_ACTUAL_SIZE, actual_size.to_string());
                if persist_encryption_original_size {
                    fi.metadata
                        .insert("x-rustfs-encryption-original-size".to_string(), actual_size.to_string());
                }
            } else if let Some(actual_size) = declared_replication_actual_size {
                insert_str(&mut fi.metadata, SUFFIX_ACTUAL_SIZE, actual_size.clone());
                if persist_encryption_original_size {
                    fi.metadata
                        .insert("x-rustfs-encryption-original-size".to_string(), actual_size);
                }
            }
        } else {
            insert_str(&mut fi.metadata, SUFFIX_ACTUAL_SIZE, object_actual_size.to_string());
            if persist_encryption_original_size {
                fi.metadata
                    .insert("x-rustfs-encryption-original-size".to_string(), object_actual_size.to_string());
            }
        }

        if fi.is_compressed() {
            insert_str(&mut fi.metadata, SUFFIX_COMPRESSION_SIZE, object_size.to_string());
        }

        rustfs_utils::http::remove_str(&mut fi.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS);
        if let Some(part_checksums) =
            rustfs_utils::http::get_consistent_str(&opts.user_defined, rustfs_utils::http::SUFFIX_PART_CHECKSUMS)
        {
            insert_str(&mut fi.metadata, rustfs_utils::http::SUFFIX_PART_CHECKSUMS, part_checksums.to_string());
        }

        if opts.data_movement {
            rustfs_utils::http::remove_str(&mut fi.metadata, rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD);
            fi.set_data_moved();
        }

        for meta in parts_metadatas.iter_mut() {
            if meta.has_valid_erasure_geometry() {
                meta.size = fi.size;
                meta.mod_time = fi.mod_time;
                meta.parts.clone_from(&fi.parts);
                meta.metadata = fi.metadata.clone();
                meta.versioned = opts.versioned || opts.version_suspended;
                meta.version_id = fi.version_id;
                meta.checksum = fi.checksum.clone();
            }
        }

        let completed_part_numbers = fi.parts.iter().map(|part| part.number).collect::<HashSet<_>>();
        let upload_data_dir = curr_fi.data_dir.unwrap_or(Uuid::nil()).to_string();
        let mut parts = Vec::with_capacity(curr_fi.parts.len());

        for p in curr_fi.parts.iter() {
            parts.push(path_join_buf(&[
                &upload_id_path,
                upload_data_dir.as_str(),
                format!("part.{}.meta", p.number).as_str(),
            ]));

            if !completed_part_numbers.contains(&p.number) {
                parts.push(path_join_buf(&[
                    &upload_id_path,
                    upload_data_dir.as_str(),
                    format!("part.{}", p.number).as_str(),
                ]));
            }
        }

        // Phase 2 (backlog#899): fence the commit on lock loss before any destructive
        // step. If the refresh heartbeat has observed a refresh-quorum loss, another
        // writer may have re-acquired this object's lock; proceeding would race a
        // double-write. Abort with a retryable error *before* cleanup_multipart_path
        // (which removes the source parts) and rename_data (which commits the object),
        // so a lost lock leaves the upload intact and retryable.
        #[cfg(test)]
        pause_multipart_commit(bucket, object, MultipartCommitPause::BeforeLockLost).await;
        if object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "complete_multipart_upload_commit",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
        if opts
            .namespace_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
        {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "complete_multipart_upload_outer_lock",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
        if upload_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "complete_multipart_upload_commit",
                bucket: RUSTFS_META_MULTIPART_BUCKET.to_string(),
                object: upload_id_path.clone(),
                required: 1,
                achieved: 0,
            });
        }
        ensure_multipart_bucket_lifecycle_lock_held(bucket, object, opts)?;

        self.require_current_restore_operation_id(
            bucket,
            object,
            opts,
            expected_restore_operation_id,
            "complete_multipart_upload_commit",
        )
        .await?;

        if opts.data_movement
            && opts.http_preconditions.is_some()
            && let Some(err) = self.check_write_precondition(bucket, object, opts).await
        {
            return Err(err);
        }
        if opts.data_movement && opts.http_preconditions.is_some() && !crate::bucket::utils::is_meta_bucketname(bucket) {
            let current = self
                .get_object_info(
                    bucket,
                    object,
                    &ObjectOptions {
                        version_id: opts.version_id.clone(),
                        no_lock: true,
                        metadata_cache_safe: false,
                        versioned: opts.versioned,
                        version_suspended: opts.version_suspended,
                        ..Default::default()
                    },
                )
                .await;
            match current {
                Ok(existing) if crate::data_movement::can_replace_stale_data_movement_target(&existing, opts) => {
                    let object_lock_config = opts.object_lock_config_snapshot.as_deref().ok_or_else(|| {
                        Error::other("data movement completion is missing its Object Lock configuration snapshot")
                    })?;
                    if check_object_lock_for_deletion_with_state(object_lock_config.state(), &existing, false)?.is_some() {
                        return Err(StorageError::PrefixAccessDenied(bucket.to_string(), object.to_string()));
                    }
                }
                Ok(_) => return Err(StorageError::PreconditionFailed),
                Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {}
                Err(err) => return Err(err),
            }
        }
        if object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "complete_multipart_upload_commit",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
        if opts
            .namespace_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
        {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "complete_multipart_upload_outer_lock",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
        if upload_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "complete_multipart_upload_commit",
                bucket: RUSTFS_META_MULTIPART_BUCKET.to_string(),
                object: upload_id_path.clone(),
                required: 1,
                achieved: 0,
            });
        }
        ensure_multipart_bucket_lifecycle_lock_held(bucket, object, opts)?;

        let transaction_fencing_proof = object_transaction_fencing_fleet_proof();
        if object_transaction_fencing_requested() && transaction_fencing_proof.is_none() {
            return Err(Error::other("object transaction fencing requires a live fleet capability proof"));
        }
        let transaction_epoch_fence = if transaction_fencing_proof.is_some() {
            Some(read_object_transaction_epoch_fence(self.as_ref(), bucket, object).await?)
        } else {
            None
        };

        let quota_old_size = if quota_context.is_enforced() {
            if opts.data_movement {
                quota_new_size
            } else {
                reservation::replaced_logical_size(&self, bucket, object, opts).await?
            }
        } else {
            0
        };
        let quota_reservation = quota_context.reserve(quota_old_size, quota_new_size).await?;
        let (commit_disks, quota_fence_tokens) = if quota_mutation_fence {
            match Self::prepare_quota_mutation_fences(&shuffle_disks, bucket, object, write_quorum).await {
                Ok((disks, tokens)) => {
                    for (metadata, token) in parts_metadatas.iter_mut().zip(tokens.iter().copied()) {
                        if let Some(token) = token {
                            insert_str(
                                &mut metadata.metadata,
                                crate::disk::QUOTA_MUTATION_FENCE_METADATA_SUFFIX,
                                token.as_uuid().to_string(),
                            );
                        }
                    }
                    (disks, tokens)
                }
                Err(err) => {
                    quota_reservation.abort().await;
                    return Err(err);
                }
            }
        } else {
            (shuffle_disks.clone(), vec![None; shuffle_disks.len()])
        };
        let transaction_epoch =
            transaction_epoch_fence.map(|_| assign_object_transaction_epoch(&commit_disks, &mut parts_metadatas));

        let commit_set = self.clone();
        let commit_bucket = bucket.to_owned();
        let commit_object = object.to_owned();
        let commit_upload_id = upload_id.to_owned();
        let commit_upload_id_path = upload_id_path.clone();
        let commit_version_suspended = opts.version_suspended;
        let commit_versioned = opts.versioned;
        let commit_version_id = opts.version_id.clone();
        let commit_namespace_lock_fence = opts.namespace_lock_fence.clone();
        let commit_bucket_lifecycle_lock_fence = opts.bucket_lifecycle_lock_fence.clone();
        let commit_is_versioned = opts.versioned || opts.version_suspended;
        let commit_capacity_scope_token = opts.capacity_scope_token;
        let commit_object_lock_guard = object_lock_guard.take();
        let detach_commit_owner = commit_object_lock_guard.is_some() || upload_guard.is_some() || quota_mutation_fence;
        let commit = async move {
            let mut _object_lock_guard = commit_object_lock_guard;
            let mut _upload_guard = upload_guard;
            let mut quota_reservation = quota_reservation;
            let complete_tail_stage_start = rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now);

            let pre_rename_result: Result<()> = async {
                // Crash-consistency injection: hard power loss after the upload is fully
                // staged and locked but before the authoritative rename_data commit. No
                // disk has moved the staged data, so a crash here must leave any prior
                // committed version byte-for-byte intact (rustfs/backlog#864) and the
                // upload fully retryable. Compiles to a no-op outside `#[cfg(test)]`.
                if crash_inject::should_crash_at(CrashPoint::MultipartBeforeCommitRename, &commit_object) {
                    return Err(StorageError::Unexpected);
                }
                quota_reservation.mark_commit_started().await?;
                #[cfg(any(test, feature = "test-util"))]
                pause_multipart_commit(&commit_bucket, &commit_object, MultipartCommitPause::BeforeQuotaRename).await;
                if quota_reservation.is_lock_lost()
                    || !quota_reservation.capability_proof_matches()
                    || _object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                    || _upload_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                    || commit_namespace_lock_fence
                        .as_ref()
                        .is_some_and(NamespaceLockFence::is_lock_lost)
                    || commit_bucket_lifecycle_lock_fence
                        .as_ref()
                        .is_some_and(NamespaceLockFence::is_lock_lost)
                {
                    return Err(StorageError::NamespaceLockQuorumUnavailable {
                        mode: "quota_reservation",
                        bucket: commit_bucket.clone(),
                        object: commit_object.clone(),
                        required: 1,
                        achieved: 0,
                    });
                }
                let restore_opts = ObjectOptions {
                    version_id: commit_version_id.clone(),
                    versioned: commit_versioned,
                    version_suspended: commit_version_suspended,
                    no_lock: true,
                    ..Default::default()
                };
                commit_set
                    .require_current_restore_operation_id(
                        &commit_bucket,
                        &commit_object,
                        &restore_opts,
                        expected_restore_operation_id,
                        "complete_multipart_upload_quota_reservation",
                    )
                    .await?;
                if let Some(proof) = transaction_fencing_proof.as_ref()
                    && !object_transaction_fencing_fleet_proof_matches(proof)
                {
                    return Err(Error::other(
                        "object transaction fencing fleet capability changed during complete_multipart_upload",
                    ));
                }
                if let Some(expected) = transaction_epoch_fence {
                    #[cfg(test)]
                    pause_multipart_commit(&commit_bucket, &commit_object, MultipartCommitPause::BeforeTransactionEpochVerify)
                        .await;
                    verify_object_transaction_epoch_fence(&commit_set, &commit_bucket, &commit_object, expected).await?;
                }
                if quota_reservation.is_lock_lost()
                    || !quota_reservation.capability_proof_matches()
                    || _object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                    || _upload_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
                    || commit_namespace_lock_fence
                        .as_ref()
                        .is_some_and(NamespaceLockFence::is_lock_lost)
                    || commit_bucket_lifecycle_lock_fence
                        .as_ref()
                        .is_some_and(NamespaceLockFence::is_lock_lost)
                {
                    return Err(StorageError::NamespaceLockQuorumUnavailable {
                        mode: "quota_reservation",
                        bucket: commit_bucket.clone(),
                        object: commit_object.clone(),
                        required: 1,
                        achieved: 0,
                    });
                }
                Ok(())
            }
            .await;
            if let Err(err) = pre_rename_result {
                SetDisks::abort_quota_reservation_after_fence(
                    quota_reservation,
                    &commit_disks,
                    &quota_fence_tokens,
                    &commit_bucket,
                    &commit_object,
                    write_quorum,
                    quota_mutation_fence,
                )
                .await;
                return Err(err);
            }

            // The trailing `_` drops the rename_data old-size backfill
            // (rustfs/backlog#1009): CompleteMultipartUpload keeps its pre-commit
            // `get_object_info` lookup, so the backfill has no consumer here yet.
            Self::assign_rename_data_indexes(&mut parts_metadatas);
            let rename_result = SetDisks::rename_data_owned(
                &commit_disks,
                RUSTFS_META_MULTIPART_BUCKET,
                &commit_upload_id_path,
                parts_metadatas,
                &commit_bucket,
                &commit_object,
                write_quorum,
            )
            .await;
            if quota_mutation_fence {
                let _ = SetDisks::release_quota_mutation_fences(
                    &commit_disks,
                    &quota_fence_tokens,
                    &commit_bucket,
                    &commit_object,
                    write_quorum,
                )
                .await;
            }
            if rename_result.is_ok() {
                quota_reservation.commit().await;
            }
            let rename_commit = match rename_result {
                Ok(result) => result,
                Err(err) => return Err(err.into()),
            };
            let online_disks = rename_commit.online_disks;
            let convergence = rename_commit.convergence;
            let op_old_dir = rename_commit.data_dir;
            let cleanup_disks = rename_commit.cleanup_disks;
            let committed_file_info = rename_commit.committed_file_info;
            let rename_tail_drain = rename_commit.tail_drain;

            // Detach admission before any post-commit await: client cancellation
            // must not couple durable convergence repair to cleanup work.
            if convergence.needs_heal() {
                let mut request = rustfs_common::heal_channel::create_heal_request_with_options(
                    commit_bucket.clone(),
                    Some(commit_object.clone()),
                    false,
                    Some(HealChannelPriority::Normal),
                    Some(commit_set.pool_index),
                    Some(commit_set.set_index),
                );
                request.object_version_id = fi
                    .version_id
                    .or_else(|| commit_version_suspended.then(Uuid::nil))
                    .map(|version_id| version_id.to_string());
                tokio::spawn(async move {
                    let _ = rustfs_common::heal_channel::send_heal_request(request).await;
                });
            }

            if let Some(old_dir) = op_old_dir {
                commit_set
                    .persist_old_data_cleanup_receipts(
                        &cleanup_disks,
                        &commit_bucket,
                        &commit_object,
                        old_dir,
                        fi.data_dir,
                        transaction_epoch,
                    )
                    .await;
            }

            // Crash-consistency injection: hard power loss after the authoritative
            // rename_data commit succeeded but before the stale part.N.meta cleanup.
            // The new version is durably committed and visible, so a crash here must
            // leave the object readable as the new version; the un-reclaimed staging
            // parts are swept by a retried completion or upload GC (rustfs/backlog#946).
            // Compiles to a no-op outside `#[cfg(test)]`.
            if crash_inject::should_crash_at(CrashPoint::MultipartAfterCommitBeforePartsCleanup, &commit_object) {
                return Err(StorageError::Unexpected);
            }

            fi = committed_file_info;
            let committed_dir = fi.data_dir.unwrap_or_default().to_string();

            commit_set.record_capacity_scope_if_needed(commit_capacity_scope_token, &online_disks);

            fi.is_latest = true;

            #[cfg(test)]
            pause_multipart_commit(&commit_bucket, &commit_object, MultipartCommitPause::BeforeObjectPublication).await;

            commit_set
                .invalidate_get_object_metadata_cache(&commit_bucket, &commit_object)
                .await;

            if let Some(rename_tail_drain) = rename_tail_drain {
                let object_lock_guard = _object_lock_guard.take();
                let upload_guard = _upload_guard.take();
                let tail_bucket = commit_bucket.clone();
                let tail_object = commit_object.clone();
                tokio::spawn(async move {
                    let _object_lock_guard = object_lock_guard;
                    let _upload_guard = upload_guard;
                    if let Err(err) = rename_tail_drain.await {
                        warn!(
                            event = EVENT_SET_DISK_RENAME_TAIL_DRAIN_FAILED,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_SET_DISK,
                            state = "failed",
                            bucket = %tail_bucket,
                            object = %tail_object,
                            error = %err,
                            "rename tail drain failed"
                        );
                    }
                });
            } else {
                drop(_object_lock_guard.take()); // release the object lock before multipart cleanup tail IO.
            }

            #[cfg(test)]
            pause_multipart_commit(&commit_bucket, &commit_object, MultipartCommitPause::AfterObjectPublication).await;

            // backlog#946: reclaim the stale per-part metadata (and any superfluous
            // part.N data files no longer in the completed set) only *after* the
            // authoritative rename_data commit above has succeeded. If rename_data
            // fails write quorum and returns via `?`, the upload directory must keep
            // its part.N.meta so a retried CompleteMultipartUpload can still read the
            // parts; deleting them before the commit would strand the upload
            // permanently. This mirrors the "clean up only after commit" pattern
            // already used for the old data-dir GC and the upload-dir delete_all below.
            commit_set.cleanup_multipart_path(&parts).await;

            if let Some(old_dir) = op_old_dir {
                // backlog#898: best-effort reclaim of the dereferenced old data dir.
                // Returns a receipt (never `Err`); a failed GC must not turn an
                // already-committed multipart completion into a 503.
                let cleanup = commit_set
                    .commit_rename_data_dir(
                        &cleanup_disks,
                        &commit_bucket,
                        &commit_object,
                        &old_dir.to_string(),
                        &committed_dir,
                        write_quorum,
                    )
                    .await;
                commit_set
                    .report_old_data_dir_cleanup(&commit_bucket, &commit_object, &old_dir.to_string(), &cleanup)
                    .await;
            }

            if let Some(stage_start) = complete_tail_stage_start {
                rustfs_io_metrics::record_put_object_stage_duration(
                    "multipart_complete_tail",
                    stage_start.elapsed().as_secs_f64() * 1000.0,
                );
            }

            #[cfg(test)]
            pause_multipart_commit(&commit_bucket, &commit_object, MultipartCommitPause::AfterRename).await;

            if let Err(err) = commit_set
                .delete_all_with_quorum(RUSTFS_META_MULTIPART_BUCKET, &commit_upload_id_path, write_quorum)
                .await
            {
                warn!(
                    bucket = %commit_bucket,
                    object = %commit_object,
                    upload_id = %commit_upload_id,
                    error = ?err,
                    "completed multipart upload staging cleanup did not reach write quorum"
                );
            }

            drop(_upload_guard.take());

            Ok(ObjectInfo::from_file_info(&fi, &commit_bucket, &commit_object, commit_is_versioned))
        };

        if detach_commit_owner {
            tokio::spawn(commit)
                .await
                .map_err(|err| Error::other(format!("complete_multipart_upload commit task failed: {err}")))?
        } else {
            commit.await
        }
    }
}

/// Final ETag for a completed multipart object. An authorized replication
/// request preserves the source ETag so the replication HEAD comparison
/// converges even when the source ETag is not derivable from the uploaded
/// parts (foreign-origin objects, ciphertext-derived ETags); the internal
/// metadata override comes next; otherwise the ETag is computed from parts.
fn resolve_complete_etag(opts: &ObjectOptions, uploaded_parts: &[CompletePart]) -> String {
    if let Some(etag) = opts.preserve_etag.as_ref().filter(|etag| !etag.is_empty()) {
        return etag.clone();
    }
    if let Some(etag) = opts.user_defined.get("etag") {
        return etag.clone();
    }
    get_complete_multipart_md5(uploaded_parts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::storageclass::lookup_config_for_pools_without_env;
    use crate::disk::{DiskAPI as _, ReadOptions};
    use crate::disk::{endpoint::Endpoint, format::FormatV3};
    use crate::layout::endpoints::SetupType;
    use crate::services::notification_sys::install_remote_version_state_fleet_proof_for_test;
    // No-locker helpers resolve to the isolated-context variants (see
    // `hermetic_set_disks_isolated`); the guard-based tests build through
    // `hermetic_set_disks_with_lockers`, which stays on the bootstrap context
    // so their own ambient SetupTypeGuard flip remains visible to them.
    use crate::set_disk::ops::object::hermetic_set_disks_support::{
        hermetic_set_disks_for_pool_with_default_parity_isolated as hermetic_set_disks_for_pool_with_default_parity,
        hermetic_set_disks_isolated as hermetic_set_disks, hermetic_set_disks_with_lockers,
    };
    use crate::set_disk::ops::object::{PutObjectCommitBarrier, PutObjectCommitPause};
    use crate::storage_api_contracts::namespace::NamespaceLocking as _;
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
    use rustfs_config::server_config::KVS;
    use rustfs_lock::{LockClient, client::local::LocalClient};
    use serial_test::serial;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use tokio::sync::{Notify, RwLock};

    #[test]
    fn multipart_bucket_incarnation_metadata_is_consistent_and_non_nil() {
        let incarnation = Uuid::new_v4();
        let mut metadata = HashMap::new();
        insert_str(&mut metadata, SUFFIX_BUCKET_INCARNATION_ID, incarnation.to_string());
        assert_eq!(multipart_bucket_incarnation_id(&metadata).unwrap(), Some(incarnation));

        metadata.insert("x-minio-internal-bucket-incarnation-id".to_string(), Uuid::new_v4().to_string());
        assert!(multipart_bucket_incarnation_id(&metadata).is_err());

        let mut nil_metadata = HashMap::new();
        insert_str(&mut nil_metadata, SUFFIX_BUCKET_INCARNATION_ID, Uuid::nil().to_string());
        assert!(multipart_bucket_incarnation_id(&nil_metadata).is_err());
    }

    #[test]
    fn multipart_bucket_incarnation_gate_accepts_only_current_or_same_lifetime_legacy_uploads() {
        let expected = Uuid::new_v4();
        let stale = Uuid::new_v4();
        let bucket_created = OffsetDateTime::now_utc();
        let upload_initiated = bucket_created + time::Duration::seconds(1);
        let mut current_metadata = HashMap::new();
        insert_str(&mut current_metadata, SUFFIX_BUCKET_INCARNATION_ID, expected.to_string());
        assert!(multipart_bucket_incarnation_matches(&current_metadata, expected));
        validate_multipart_bucket_incarnation(&current_metadata, "bucket", "object", "upload", Some(expected), None, None)
            .expect("a matching stamped upload should pass");

        let missing_metadata = HashMap::new();
        assert!(!multipart_bucket_incarnation_matches(&missing_metadata, expected));
        validate_multipart_bucket_incarnation(
            &missing_metadata,
            "bucket",
            "object",
            "upload",
            Some(expected),
            Some(upload_initiated),
            Some(bucket_created),
        )
        .expect("a legacy upload initiated during the current bucket lifetime should pass");
        assert!(matches!(
            validate_multipart_bucket_incarnation(
                &missing_metadata,
                "bucket",
                "object",
                "upload",
                Some(expected),
                Some(bucket_created - time::Duration::seconds(1)),
                Some(bucket_created),
            ),
            Err(StorageError::InvalidUploadID(..))
        ));
        assert!(matches!(
            validate_multipart_bucket_incarnation(
                &missing_metadata,
                "bucket",
                "object",
                "upload",
                Some(expected),
                None,
                Some(bucket_created),
            ),
            Err(StorageError::InvalidUploadID(..))
        ));

        let mut stale_metadata = HashMap::new();
        insert_str(&mut stale_metadata, SUFFIX_BUCKET_INCARNATION_ID, stale.to_string());
        assert!(!multipart_bucket_incarnation_matches(&stale_metadata, expected));
        assert!(matches!(
            validate_multipart_bucket_incarnation(
                &stale_metadata,
                "bucket",
                "object",
                "upload",
                Some(expected),
                Some(upload_initiated),
                Some(bucket_created),
            ),
            Err(StorageError::InvalidUploadID(..))
        ));
    }

    #[test]
    fn multipart_commit_rejects_missing_or_lost_bucket_lifecycle_fence() {
        let expected = Uuid::new_v4();
        let missing = ObjectOptions {
            expected_bucket_incarnation_id: Some(expected),
            ..Default::default()
        };
        assert!(ensure_multipart_bucket_lifecycle_lock_held("bucket", "object", &missing).is_err());

        let lost = ObjectOptions {
            expected_bucket_incarnation_id: Some(expected),
            bucket_lifecycle_lock_fence: Some(NamespaceLockFence::lost_for_test()),
            ..Default::default()
        };
        assert!(matches!(
            ensure_multipart_bucket_lifecycle_lock_held("bucket", "object", &lost),
            Err(StorageError::NamespaceLockQuorumUnavailable { .. })
        ));
    }

    struct SetupTypeGuard {
        previous: SetupType,
    }

    impl SetupTypeGuard {
        async fn switch_to(next: SetupType) -> Self {
            let previous = crate::runtime::sources::current_setup_type().await;
            crate::runtime::sources::set_setup_type(next).await;
            Self { previous }
        }
    }

    impl Drop for SetupTypeGuard {
        fn drop(&mut self) {
            let previous = self.previous.clone();
            let handle = tokio::runtime::Handle::current();
            std::thread::spawn(move || {
                handle.block_on(async {
                    crate::runtime::sources::set_setup_type(previous).await;
                });
            })
            .join()
            .expect("setup type restore thread should not panic");
        }
    }

    #[derive(Debug)]
    struct SignalingLockClient {
        inner: Arc<dyn LockClient>,
        target: std::sync::RwLock<Option<rustfs_lock::ObjectKey>>,
        observed: std::sync::Mutex<Vec<rustfs_lock::ObjectKey>>,
        attempts: AtomicUsize,
        changed: Notify,
    }

    impl SignalingLockClient {
        fn new(inner: Arc<dyn LockClient>) -> Self {
            Self {
                inner,
                target: std::sync::RwLock::new(None),
                observed: std::sync::Mutex::new(Vec::new()),
                attempts: AtomicUsize::new(0),
                changed: Notify::new(),
            }
        }

        fn set_target(&self, target: rustfs_lock::ObjectKey) {
            *self.target.write().expect("signaling lock target should be writable") = Some(target);
        }

        fn clear_observed(&self) {
            self.observed
                .lock()
                .expect("observed lock resources should be writable")
                .clear();
        }

        fn observed(&self) -> Vec<rustfs_lock::ObjectKey> {
            self.observed
                .lock()
                .expect("observed lock resources should be readable")
                .clone()
        }

        async fn wait_for_attempts(&self, expected: usize) {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let changed = self.changed.notified();
                    if self.attempts.load(Ordering::Acquire) >= expected {
                        return;
                    }
                    changed.await;
                }
            })
            .await
            .expect("target lock attempt should arrive");
        }
    }

    #[async_trait::async_trait]
    impl LockClient for SignalingLockClient {
        async fn acquire_lock(&self, request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<rustfs_lock::LockResponse> {
            self.observed
                .lock()
                .expect("observed lock resources should be writable")
                .push(request.resource.clone());
            if self.target.read().expect("signaling lock target should be readable").as_ref() == Some(&request.resource) {
                self.attempts.fetch_add(1, Ordering::Release);
                self.changed.notify_waiters();
            }
            self.inner.acquire_lock(request).await
        }

        async fn release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            self.inner.release(lock_id).await
        }

        async fn refresh(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            self.inner.refresh(lock_id).await
        }

        async fn force_release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            self.inner.force_release(lock_id).await
        }

        async fn check_status(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<Option<rustfs_lock::LockInfo>> {
            self.inner.check_status(lock_id).await
        }

        async fn get_stats(&self) -> rustfs_lock::Result<rustfs_lock::LockStats> {
            self.inner.get_stats().await
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            self.inner.close().await
        }

        async fn is_online(&self) -> bool {
            self.inner.is_online().await
        }

        async fn is_local(&self) -> bool {
            self.inner.is_local().await
        }
    }

    #[derive(Debug)]
    struct SelectiveLockLossClient {
        target: Arc<std::sync::RwLock<Option<rustfs_lock::ObjectKey>>>,
        refresh_calls: Arc<AtomicUsize>,
        active: tokio::sync::Mutex<HashMap<rustfs_lock::LockId, rustfs_lock::ObjectKey>>,
    }

    impl SelectiveLockLossClient {
        fn new(target: Arc<std::sync::RwLock<Option<rustfs_lock::ObjectKey>>>, refresh_calls: Arc<AtomicUsize>) -> Self {
            Self {
                target,
                refresh_calls,
                active: tokio::sync::Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl LockClient for SelectiveLockLossClient {
        async fn acquire_lock(&self, request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<rustfs_lock::LockResponse> {
            self.active
                .lock()
                .await
                .insert(request.lock_id.clone(), request.resource.clone());
            Ok(rustfs_lock::LockResponse::success(
                rustfs_lock::LockInfo {
                    id: request.lock_id.clone(),
                    resource: request.resource.clone(),
                    lock_type: request.lock_type,
                    status: rustfs_lock::LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.ttl,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                },
                Duration::ZERO,
            ))
        }

        async fn release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(self.active.lock().await.remove(lock_id).is_some())
        }

        async fn refresh(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            let resource = self.active.lock().await.get(lock_id).cloned();
            let target = self.target.read().expect("lock-loss target should be readable").clone();
            if resource == target {
                self.refresh_calls.fetch_add(1, Ordering::Release);
                return Ok(false);
            }
            Ok(resource.is_some())
        }

        async fn force_release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            self.release(lock_id).await
        }

        async fn check_status(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<Option<rustfs_lock::LockInfo>> {
            Ok(None)
        }

        async fn get_stats(&self) -> rustfs_lock::Result<rustfs_lock::LockStats> {
            Ok(rustfs_lock::LockStats::default())
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            true
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    async fn non_trash_tmp_entries(temp_dirs: &[TempDir]) -> Vec<String> {
        let mut leftovers = Vec::new();
        for temp_dir in temp_dirs {
            let tmp_path = temp_dir.path().join(RUSTFS_META_TMP_BUCKET);
            let mut read_dir = match tokio::fs::read_dir(&tmp_path).await {
                Ok(read_dir) => read_dir,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => panic!("tmp dir {tmp_path:?} should be listable: {err}"),
            };
            while let Some(entry) = read_dir.next_entry().await.expect("tmp dir entry should be readable") {
                let name = entry.file_name().to_string_lossy().to_string();
                if name != ".trash" {
                    leftovers.push(format!("{}/{name}", tmp_path.display()));
                }
            }
        }
        leftovers
    }

    async fn make_bucket_on_all(disks: &[DiskStore], bucket: &str) {
        for disk in disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
    }

    async fn stage_upload_with_create_opts(
        set_disks: &Arc<SetDisks>,
        bucket: &str,
        object: &str,
        content: &[u8],
        create_opts: &ObjectOptions,
    ) -> (String, Vec<CompletePart>) {
        let upload = set_disks
            .new_multipart_upload(bucket, object, create_opts)
            .await
            .expect("multipart upload should be created");
        let mut reader = PutObjReader::new(
            HashReader::from_stream(
                Cursor::new(content.to_vec()),
                content.len() as i64,
                content.len() as i64,
                None,
                None,
                false,
            )
            .expect("hash reader should be constructed"),
        );
        let part = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut reader, create_opts)
            .await
            .expect("uploading the part should succeed");
        (
            upload.upload_id,
            vec![CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            }],
        )
    }

    async fn put_test_part(
        set_disks: &Arc<SetDisks>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number: usize,
        content: &[u8],
        actual_size: i64,
    ) -> CompletePart {
        put_test_part_with_opts(
            set_disks,
            bucket,
            object,
            upload_id,
            part_number,
            (content, actual_size),
            &ObjectOptions::default(),
        )
        .await
    }

    async fn put_test_part_with_opts(
        set_disks: &Arc<SetDisks>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number: usize,
        part: (&[u8], i64),
        opts: &ObjectOptions,
    ) -> CompletePart {
        let (content, actual_size) = part;
        let mut reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(content.to_vec()), content.len() as i64, actual_size, None, None, false)
                .expect("hash reader should be constructed"),
        );
        let part = set_disks
            .put_object_part(bucket, object, upload_id, part_number, &mut reader, opts)
            .await
            .expect("uploading the part should succeed");
        CompletePart {
            part_num: part.part_num,
            etag: part.etag,
            ..Default::default()
        }
    }

    async fn object_transaction_epochs(disks: &[DiskStore], bucket: &str, object: &str) -> Vec<Option<Uuid>> {
        let mut epochs = Vec::with_capacity(disks.len());
        for (disk_index, disk) in disks.iter().enumerate() {
            let file_info = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .unwrap_or_else(|err| panic!("disk {disk_index} should persist object metadata: {err}"));
            epochs.push(
                file_info
                    .object_transaction_epoch()
                    .unwrap_or_else(|err| panic!("disk {disk_index} transaction epoch should decode: {err}")),
            );
        }
        epochs
    }

    #[tokio::test]
    #[serial(storage_class_env)]
    async fn object_transaction_fencing_requires_live_fleet_proof_before_multipart_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-transaction-fencing-no-proof";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let (upload_id, parts) = stage_upload_with_create_opts(
            &set_disks,
            bucket,
            object,
            b"must-not-complete-without-proof",
            &ObjectOptions::default(),
        )
        .await;

        let err = temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE, Some("true")),
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED, Some("true")),
            ],
            async {
                set_disks
                    .clone()
                    .complete_multipart_upload(bucket, object, &upload_id, parts.clone(), &ObjectOptions::default())
                    .await
            },
        )
        .await
        .expect_err("multipart completion must fail closed without a live fleet proof");

        assert!(
            err.to_string()
                .contains("object transaction fencing requires a live fleet capability proof"),
            "unexpected error: {err:?}"
        );
        set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect_err("failed fenced completion must not publish object metadata");
    }

    #[tokio::test]
    #[serial(storage_class_env)]
    async fn object_transaction_fencing_persists_epoch_on_multipart_commit() {
        let _proof = install_remote_version_state_fleet_proof_for_test("object-transaction-fencing-test");
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-object-transaction-epoch";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let (upload_id, parts) =
            stage_upload_with_create_opts(&set_disks, bucket, object, b"multipart fenced epoch", &ObjectOptions::default()).await;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE, Some("true")),
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED, Some("true")),
            ],
            async {
                set_disks
                    .clone()
                    .complete_multipart_upload(bucket, object, &upload_id, parts.clone(), &ObjectOptions::default())
                    .await
                    .expect("fenced multipart completion should commit with a live proof");
            },
        )
        .await;

        let epochs = object_transaction_epochs(&disk_stores, bucket, object).await;
        let first = epochs[0].expect("fenced multipart completion should persist an epoch");
        assert!(!first.is_nil());
        assert!(epochs.into_iter().all(|epoch| epoch == Some(first)));
    }

    #[tokio::test]
    #[serial(storage_class_env)]
    async fn object_transaction_fencing_rejects_stale_multipart_epoch() {
        let _proof = install_remote_version_state_fleet_proof_for_test("object-transaction-fencing-test");
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-object-transaction-stale-epoch";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE, Some("true")),
                (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED, Some("true")),
            ],
            async {
                let mut initial_reader = PutObjReader::from_vec(b"initial fenced object".to_vec());
                set_disks
                    .put_object(
                        bucket,
                        object,
                        &mut initial_reader,
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("initial fenced PUT should commit");
                let initial_epoch = object_transaction_epochs(&disk_stores, bucket, object)
                    .await
                    .into_iter()
                    .next()
                    .flatten()
                    .expect("initial fenced PUT should persist an epoch");

                let (upload_id, parts) =
                    stage_upload_with_create_opts(&set_disks, bucket, object, b"stale multipart body", &ObjectOptions::default())
                        .await;
                let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::BeforeTransactionEpochVerify);
                let stale_set = Arc::clone(&set_disks);
                let stale = tokio::spawn(async move {
                    stale_set
                        .clone()
                        .complete_multipart_upload(
                            bucket,
                            object,
                            &upload_id,
                            parts,
                            &ObjectOptions {
                                no_lock: true,
                                ..Default::default()
                            },
                        )
                        .await
                });
                barrier.wait_until_paused().await;

                let mut winner_reader = PutObjReader::from_vec(b"winning put body".to_vec());
                set_disks
                    .put_object(
                        bucket,
                        object,
                        &mut winner_reader,
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("concurrent fenced PUT should advance the epoch");
                let winning_epoch = object_transaction_epochs(&disk_stores, bucket, object)
                    .await
                    .into_iter()
                    .next()
                    .flatten()
                    .expect("winning fenced PUT should persist an epoch");
                assert_ne!(winning_epoch, initial_epoch);

                barrier.release();
                let err = stale
                    .await
                    .expect("stale multipart task should not panic")
                    .expect_err("stale epoch multipart completion must be rejected");
                assert_eq!(err, StorageError::PreconditionFailed);

                let final_epochs = object_transaction_epochs(&disk_stores, bucket, object).await;
                assert!(final_epochs.into_iter().all(|epoch| epoch == Some(winning_epoch)));
                let mut reader = set_disks
                    .get_object_reader(
                        bucket,
                        object,
                        None,
                        HeaderMap::new(),
                        &ObjectOptions {
                            no_lock: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("winning object should remain readable");
                let mut restored = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut restored)
                    .await
                    .expect("winning body should stream");
                assert_eq!(restored, b"winning put body");
            },
        )
        .await;
    }

    #[tokio::test]
    async fn complete_multipart_quota_rejection_preserves_destination_and_upload() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-quota-admission-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;

        let existing_payload = b"existing object";
        let mut existing_reader = PutObjReader::from_vec(existing_payload.to_vec());
        let existing = set_disks
            .put_object(bucket, object, &mut existing_reader, &ObjectOptions::default())
            .await
            .expect("existing object should be stored");

        let payload = vec![0x51; 4096];
        let (upload_id, parts) =
            stage_upload_with_create_opts(&set_disks, bucket, object, &payload, &ObjectOptions::default()).await;
        let mut denied_opts = ObjectOptions::default();
        assert!(denied_opts.set_quota_admission(100, 4180));

        let err = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload_id, parts.clone(), &denied_opts)
            .await
            .expect_err("completion larger than the remaining quota must be rejected");
        assert!(matches!(
            err,
            StorageError::QuotaExceeded {
                current: 100,
                limit: 4180
            }
        ));

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("quota rejection must preserve the existing destination");
        assert_eq!(current.etag, existing.etag);
        assert!(
            set_disks
                .check_upload_id_exists(bucket, object, &upload_id, false)
                .await
                .is_ok(),
            "quota rejection must leave the multipart upload retryable"
        );

        let mut allowed_opts = ObjectOptions::default();
        assert!(allowed_opts.set_quota_admission(100, 4181));
        let completed = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload_id, parts, &allowed_opts)
            .await
            .expect("completion at the exact remaining-quota boundary should succeed");
        assert_eq!(completed.get_actual_size().expect("completed logical size should resolve"), 4096);
    }

    #[tokio::test]
    async fn complete_multipart_quota_uses_compressed_logical_size() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-compressed-quota-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;

        let mut create_opts = ObjectOptions::default();
        insert_str(&mut create_opts.user_defined, SUFFIX_COMPRESSION, "S2".to_string());
        let upload = set_disks
            .new_multipart_upload(bucket, object, &create_opts)
            .await
            .expect("multipart upload should be created");
        let part = put_test_part(&set_disks, bucket, object, &upload.upload_id, 1, &[0x52; 128], 8192).await;
        let mut complete_opts = ObjectOptions::default();
        assert!(complete_opts.set_quota_admission(0, 4096));

        let err = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, vec![part], &complete_opts)
            .await
            .expect_err("logical size above the remaining quota must be rejected");
        assert!(matches!(err, StorageError::QuotaExceeded { current: 0, limit: 4096 }));
        assert!(
            set_disks
                .check_upload_id_exists(bucket, object, &upload.upload_id, false)
                .await
                .is_ok(),
            "quota rejection must leave compressed parts retryable"
        );
    }

    #[tokio::test]
    async fn replication_quota_uses_server_observed_part_size_as_lower_bound() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-replication-quota-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;

        let mut create_opts = ObjectOptions::default();
        insert_str(&mut create_opts.user_defined, SUFFIX_COMPRESSION, "S2".to_string());
        let upload = set_disks
            .new_multipart_upload(bucket, object, &create_opts)
            .await
            .expect("replication multipart upload should be created");
        let part = put_test_part(&set_disks, bucket, object, &upload.upload_id, 1, &[0x72; 4096], 1).await;
        let mut complete_opts = ObjectOptions {
            replication_request: true,
            ..Default::default()
        };
        assert!(complete_opts.set_quota_admission(0, 4095));

        let err = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, vec![part.clone()], &complete_opts)
            .await
            .expect_err("a forged tiny replication logical size must not reduce quota admission");
        assert!(matches!(err, StorageError::QuotaExceeded { current: 0, limit: 4095 }));
        assert!(
            set_disks
                .check_upload_id_exists(bucket, object, &upload.upload_id, false)
                .await
                .is_ok(),
            "quota rejection must leave replicated multipart parts retryable"
        );

        assert!(complete_opts.set_quota_admission(0, 4096));
        let completed = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, vec![part], &complete_opts)
            .await
            .expect("the physical safety boundary should admit the transformed replica");
        assert_eq!(completed.get_actual_size().expect("replica logical size should parse"), 1);
    }

    #[tokio::test]
    async fn direct_multipart_quota_uses_server_observed_part_size_as_lower_bound() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-direct-quota-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;

        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        let part = put_test_part(&set_disks, bucket, object, &upload.upload_id, 1, &[0x73; 4096], 1).await;
        let mut complete_opts = ObjectOptions::default();
        assert!(complete_opts.set_quota_admission(0, 4095));

        let err = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, vec![part], &complete_opts)
            .await
            .expect_err("a forged tiny direct logical size must not reduce quota admission");
        assert!(matches!(err, StorageError::QuotaExceeded { current: 0, limit: 4095 }));
        assert!(
            set_disks
                .check_upload_id_exists(bucket, object, &upload.upload_id, false)
                .await
                .is_ok(),
            "quota rejection must leave direct multipart parts retryable"
        );
    }

    #[tokio::test]
    async fn quota_rejects_ciphertext_replication_without_a_server_observed_logical_size() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-compressed-ciphertext-quota-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;

        let mut create_opts = ObjectOptions::default();
        insert_str(&mut create_opts.user_defined, SUFFIX_COMPRESSION, "S2".to_string());
        insert_str(
            &mut create_opts.user_defined,
            rustfs_utils::http::SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT,
            "true".to_string(),
        );
        let upload = set_disks
            .new_multipart_upload(bucket, object, &create_opts)
            .await
            .expect("ciphertext multipart upload should be created");
        let payload = vec![0x74; 4096];
        let part = put_test_part(&set_disks, bucket, object, &upload.upload_id, 1, &payload, 1).await;
        let mut complete_opts = ObjectOptions {
            replication_request: true,
            ..Default::default()
        };
        assert!(complete_opts.set_quota_admission(0, u64::MAX));

        let err = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, vec![part], &complete_opts)
            .await
            .expect_err("ciphertext replication has no server-observed logical quota size");
        assert!(matches!(err, StorageError::PartMissingOrCorrupt));
        assert!(
            set_disks
                .check_upload_id_exists(bucket, object, &upload.upload_id, false)
                .await
                .is_ok(),
            "rejection must leave ciphertext multipart parts retryable"
        );
    }

    #[tokio::test]
    async fn complete_multipart_quota_rejects_invalid_logical_sizes() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-invalid-logical-size-bucket";
        make_bucket_on_all(&disk_stores, bucket).await;

        let mut create_opts = ObjectOptions::default();
        insert_str(&mut create_opts.user_defined, SUFFIX_COMPRESSION, "S2".to_string());
        let mut complete_opts = ObjectOptions::default();
        assert!(complete_opts.set_quota_admission(0, u64::MAX));

        let negative_upload = set_disks
            .new_multipart_upload(bucket, "negative", &create_opts)
            .await
            .expect("negative-size upload should be created");
        let negative_part = put_test_part(&set_disks, bucket, "negative", &negative_upload.upload_id, 1, &[0x53], -1).await;
        let negative_err = set_disks
            .clone()
            .complete_multipart_upload(bucket, "negative", &negative_upload.upload_id, vec![negative_part], &complete_opts)
            .await
            .expect_err("negative logical size must fail closed");
        assert!(matches!(negative_err, StorageError::PartMissingOrCorrupt));

        let overflow_upload = set_disks
            .new_multipart_upload(bucket, "overflow", &create_opts)
            .await
            .expect("overflow upload should be created");
        let first = put_test_part(&set_disks, bucket, "overflow", &overflow_upload.upload_id, 1, &[0x54], i64::MAX).await;
        let second = put_test_part(&set_disks, bucket, "overflow", &overflow_upload.upload_id, 2, &[0x55], 1).await;
        let overflow_err = set_disks
            .clone()
            .complete_multipart_upload(bucket, "overflow", &overflow_upload.upload_id, vec![first, second], &complete_opts)
            .await
            .expect_err("overflowing logical size must fail closed");
        assert!(matches!(overflow_err, StorageError::PartMissingOrCorrupt));
    }

    #[tokio::test]
    #[serial]
    async fn data_movement_upload_is_hidden_from_external_multipart_operations() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "data-movement-upload-complete-bucket";
        make_bucket_on_all(&disk_stores, bucket).await;
        let mut durable_metadata = HashMap::new();
        rustfs_utils::http::insert_str(&mut durable_metadata, rustfs_utils::http::SUFFIX_DATA_MOVED, "true".to_string());
        rustfs_utils::http::insert_str(
            &mut durable_metadata,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
            r#"[[1,[["CRC32C","AAAAAA=="]]]]"#.to_string(),
        );
        rustfs_utils::http::insert_str(
            &mut durable_metadata,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
            "residual-final-marker".to_string(),
        );
        let ordinary_opts = ObjectOptions {
            user_defined: durable_metadata,
            ..Default::default()
        };
        let (upload_id, parts) =
            stage_upload_with_create_opts(&set_disks, bucket, "ordinary-object", b"ordinary multipart body", &ordinary_opts)
                .await;
        set_disks
            .clone()
            .complete_multipart_upload(bucket, "ordinary-object", &upload_id, parts, &ordinary_opts)
            .await
            .expect("durable object metadata must not claim upload ownership");
        let completed = set_disks
            .get_object_info(
                bucket,
                "ordinary-object",
                &ObjectOptions {
                    include_part_checksums: true,
                    ..Default::default()
                },
            )
            .await
            .expect("completed staging checksum metadata should remain readable");
        assert_eq!(
            completed.parts[0]
                .checksums
                .as_ref()
                .and_then(|checksums| checksums.get("CRC32C"))
                .map(String::as_str),
            Some("AAAAAA==")
        );

        let object = "owned-object";
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD, "true".to_string());
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
            r#"[[1,[["CRC32C","AAAAAA=="]]]]"#.to_string(),
        );
        let create_opts = ObjectOptions {
            data_movement: true,
            user_defined: metadata,
            ..Default::default()
        };
        let upload = set_disks
            .new_multipart_upload(bucket, object, &create_opts)
            .await
            .expect("data movement upload should be created");
        let upload_id = upload.upload_id;
        let ordinary_upload_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        let data_movement_upload_path = SetDisks::get_multipart_upload_dir(bucket, object, &upload_id, true);
        assert_eq!(data_movement_upload_path, format!("data-movement/{ordinary_upload_path}"));
        assert!(matches!(
            set_disks.check_upload_id_exists(bucket, object, &upload_id, false).await,
            Err(StorageError::InvalidUploadID(..))
        ));
        set_disks
            .check_upload_id_exists_with_opts(bucket, object, &upload_id, false, &create_opts)
            .await
            .expect("data movement lookup should find the isolated upload");
        let mut part_reader = PutObjReader::from_vec(b"data movement multipart body".to_vec());
        let uploaded_part = set_disks
            .put_object_part(bucket, object, &upload_id, 1, &mut part_reader, &create_opts)
            .await
            .expect("data movement part upload should retain ownership of its upload");
        let parts = vec![CompletePart {
            part_num: uploaded_part.part_num,
            etag: uploaded_part.etag,
            ..Default::default()
        }];

        let listed = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "", None, None, None, 1000, None)
            .await
            .expect("external multipart listing should succeed");
        assert!(!listed.uploads.iter().any(|upload| upload.upload_id == upload_id));

        let get_err = set_disks
            .get_multipart_info(bucket, object, &upload_id, &ObjectOptions::default())
            .await
            .expect_err("external multipart metadata reads must not expose a data movement upload");
        assert!(matches!(get_err, StorageError::InvalidUploadID(..)));

        let list_parts_err = set_disks
            .list_object_parts(bucket, object, &upload_id, None, MAX_PARTS_COUNT, &ObjectOptions::default())
            .await
            .expect_err("external part listings must not expose a data movement upload");
        assert!(matches!(list_parts_err, StorageError::InvalidUploadID(..)));

        let mut external_part = PutObjReader::from_vec(b"external overwrite".to_vec());
        let put_err = set_disks
            .put_object_part(bucket, object, &upload_id, 1, &mut external_part, &ObjectOptions::default())
            .await
            .expect_err("external part uploads must not modify a data movement upload");
        assert!(matches!(put_err, StorageError::InvalidUploadID(..)));

        let abort_err = set_disks
            .abort_multipart_upload(bucket, object, &upload_id, &ObjectOptions::default())
            .await
            .expect_err("external aborts must not remove a data movement upload");
        assert!(matches!(abort_err, StorageError::InvalidUploadID(..)));

        let external_err = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload_id, parts.clone(), &ObjectOptions::default())
            .await
            .expect_err("external completion must not finalize a data movement upload");
        assert!(matches!(external_err, StorageError::InvalidUploadID(..)));

        let internal_parts = set_disks
            .list_object_parts(bucket, object, &upload_id, None, MAX_PARTS_COUNT, &create_opts)
            .await
            .expect("data movement part listing should retain ownership of its upload");
        assert_eq!(internal_parts.parts.len(), 1);

        set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload_id, parts, &create_opts)
            .await
            .expect("data movement completion should retain ownership of its upload");
        let completed = set_disks
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    include_part_checksums: true,
                    ..Default::default()
                },
            )
            .await
            .expect("completed data movement object should be readable");
        assert!(!rustfs_utils::http::contains_key_str(
            &completed.user_defined,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD
        ));
        for suffix in [
            rustfs_utils::http::SUFFIX_DATA_MOVED,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
        ] {
            assert!(
                completed
                    .user_defined
                    .contains_key(&rustfs_utils::http::internal_key_rustfs(suffix))
            );
            assert!(
                completed
                    .user_defined
                    .contains_key(&format!("{}{suffix}", rustfs_utils::http::MINIO_INTERNAL_PREFIX))
            );
        }
        assert_eq!(
            completed.parts[0]
                .checksums
                .as_ref()
                .and_then(|checksums| checksums.get("CRC32C"))
                .map(String::as_str),
            Some("AAAAAA==")
        );

        let abort_object = "owned-abort-object";
        let abort_upload = set_disks
            .new_multipart_upload(bucket, abort_object, &create_opts)
            .await
            .expect("data movement abort upload should be created");
        let abort_err = set_disks
            .abort_multipart_upload(bucket, abort_object, &abort_upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("external abort must not remove the second data movement upload");
        assert!(matches!(abort_err, StorageError::InvalidUploadID(..)));
        set_disks
            .abort_multipart_upload(bucket, abort_object, &abort_upload.upload_id, &create_opts)
            .await
            .expect("data movement abort should retain ownership of its upload");
    }

    #[tokio::test]
    async fn stale_data_movement_replacement_fails_before_commit_on_outer_fence_loss() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "data-movement-stale-fence-bucket";
        make_bucket_on_all(&disk_stores, bucket).await;
        let old_time = OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND;
        let new_time = old_time + time::Duration::SECOND;

        for (object, namespace_lock_fence, bucket_lifecycle_lock_fence) in [
            ("metadata-fence", Some(NamespaceLockFence::lost_for_test()), None),
            ("bucket-fence", None, Some(NamespaceLockFence::lost_for_test())),
        ] {
            let version_id = Uuid::new_v4();
            let old_body = format!("old-{object}").into_bytes();
            let mut old_reader = PutObjReader::from_vec(old_body.clone());
            set_disks
                .put_object(
                    bucket,
                    object,
                    &mut old_reader,
                    &ObjectOptions {
                        data_movement: true,
                        versioned: true,
                        version_id: Some(version_id.to_string()),
                        mod_time: Some(old_time),
                        ..Default::default()
                    },
                )
                .await
                .expect("seed old data movement target");

            let replacement_body = format!("new-{object}").into_bytes();
            let create_opts = ObjectOptions {
                data_movement: true,
                ..Default::default()
            };
            let (upload_id, parts) =
                stage_upload_with_create_opts(&set_disks, bucket, object, &replacement_body, &create_opts).await;
            let complete_opts = ObjectOptions {
                data_movement: true,
                versioned: true,
                version_id: Some(version_id.to_string()),
                mod_time: Some(new_time),
                http_preconditions: Some(crate::data_movement::data_movement_target_precondition()),
                namespace_lock_fence,
                bucket_lifecycle_lock_fence,
                object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                    ObjectLockConfigState::ConfirmedAbsent,
                ))),
                ..Default::default()
            };
            let err = set_disks
                .clone()
                .complete_multipart_upload(bucket, object, &upload_id, parts, &complete_opts)
                .await
                .expect_err("a lost outer fence must abort stale target replacement");
            assert!(matches!(err, StorageError::NamespaceLockQuorumUnavailable { .. }));

            let mut preserved = set_disks
                .get_object_reader(
                    bucket,
                    object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        versioned: true,
                        version_id: Some(version_id.to_string()),
                        ..Default::default()
                    },
                )
                .await
                .expect("read target after rejected replacement");
            let mut preserved_body = Vec::new();
            preserved
                .stream
                .read_to_end(&mut preserved_body)
                .await
                .expect("drain target after rejected replacement");
            assert_eq!(preserved_body, old_body);
            set_disks
                .check_upload_id_exists_with_opts(bucket, object, &upload_id, true, &complete_opts)
                .await
                .expect("fence loss must leave replacement staging retryable");
        }
    }

    #[tokio::test]
    #[serial]
    async fn data_movement_complete_accepts_unknown_compressed_part_actual_size() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "data-movement-unknown-actual-size-bucket";
        make_bucket_on_all(&disk_stores, bucket).await;

        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            crate::io_support::rio::compression_metadata_value(rustfs_utils::CompressionAlgorithm::default()),
        );
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
            "source-generation".to_string(),
        );

        for (object, quota_limit) in [("without-quota", None), ("with-quota", Some(u64::MAX))] {
            let create_opts = ObjectOptions {
                data_movement: true,
                user_defined: metadata.clone(),
                ..Default::default()
            };
            let upload = set_disks
                .new_multipart_upload(bucket, object, &create_opts)
                .await
                .expect("data movement upload should be created");

            let mut completed_parts = Vec::new();
            for (number, actual_size) in [(1, -1), (2, 1)] {
                let mut reader = PutObjReader::new(
                    HashReader::from_stream(Cursor::new(vec![number as u8]), 1, actual_size, None, None, false)
                        .expect("part reader should be constructed"),
                );
                let part = set_disks
                    .put_object_part(bucket, object, &upload.upload_id, number, &mut reader, &create_opts)
                    .await
                    .expect("data movement part should be written");
                completed_parts.push(CompletePart {
                    part_num: number,
                    etag: part.etag,
                    ..Default::default()
                });
            }

            let missing_size_err = set_disks
                .clone()
                .complete_multipart_upload(bucket, object, &upload.upload_id, completed_parts.clone(), &create_opts)
                .await
                .expect_err("data movement completion must require an authoritative total size");
            assert!(matches!(missing_size_err, StorageError::PartMissingOrCorrupt));

            let mut complete_opts = create_opts.clone();
            rustfs_utils::http::insert_str(
                &mut complete_opts.user_defined,
                rustfs_utils::http::SUFFIX_ACTUAL_SIZE,
                "2".to_string(),
            );
            if let Some(quota_limit) = quota_limit {
                assert!(complete_opts.set_quota_admission(0, quota_limit));
            }
            let completion = set_disks
                .clone()
                .complete_multipart_upload(bucket, object, &upload.upload_id, completed_parts, &complete_opts)
                .await;

            let completed = completion.expect("data movement completion should accept the persisted unknown-size sentinel");

            assert_eq!(completed.parts[0].actual_size, -1);
            assert_eq!(completed.get_actual_size().expect("completed object actual size"), 2);
        }

        let object = "all-unknown-with-quota";
        let create_opts = ObjectOptions {
            data_movement: true,
            user_defined: metadata.clone(),
            ..Default::default()
        };
        let upload = set_disks
            .new_multipart_upload(bucket, object, &create_opts)
            .await
            .expect("data movement upload should be created");
        let part = put_test_part_with_opts(&set_disks, bucket, object, &upload.upload_id, 1, (&[0x40], -1), &create_opts).await;
        let mut complete_opts = create_opts;
        rustfs_utils::http::insert_str(&mut complete_opts.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "2".to_string());
        assert!(complete_opts.set_quota_admission(0, u64::MAX));
        let completed = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, vec![part], &complete_opts)
            .await
            .expect("quota must accept an all-unknown data movement upload with an authoritative total");
        assert_eq!(completed.parts[0].actual_size, -1);
        assert_eq!(completed.get_actual_size().expect("completed object actual size"), 2);

        let object = "legacy-zero-fallback";
        let create_opts = ObjectOptions {
            data_movement: true,
            user_defined: metadata.clone(),
            ..Default::default()
        };
        let upload = set_disks
            .new_multipart_upload(bucket, object, &create_opts)
            .await
            .expect("data movement upload should be created");
        let part =
            put_test_part_with_opts(&set_disks, bucket, object, &upload.upload_id, 1, (&[0x41; 128], 128), &create_opts).await;
        let mut complete_opts = create_opts;
        rustfs_utils::http::insert_str(
            &mut complete_opts.user_defined,
            rustfs_utils::http::SUFFIX_ACTUAL_SIZE,
            "100".to_string(),
        );
        assert!(complete_opts.set_quota_admission(0, u64::MAX));
        let completed = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, vec![part], &complete_opts)
            .await
            .expect("legacy zero-to-physical part fallback must remain migratable");
        assert_eq!(completed.parts[0].actual_size, 128);
        assert_eq!(completed.get_actual_size().expect("completed object actual size"), 100);

        let object = "inconsistent-untransformed-total";
        let mut untransformed_metadata = metadata.clone();
        rustfs_utils::http::remove_str(&mut untransformed_metadata, rustfs_utils::http::SUFFIX_COMPRESSION);
        let create_opts = ObjectOptions {
            data_movement: true,
            user_defined: untransformed_metadata,
            ..Default::default()
        };
        let upload = set_disks
            .new_multipart_upload(bucket, object, &create_opts)
            .await
            .expect("data movement upload should be created");
        let part = put_test_part_with_opts(&set_disks, bucket, object, &upload.upload_id, 1, (&[0x42; 2], 2), &create_opts).await;
        let mut complete_opts = create_opts;
        rustfs_utils::http::insert_str(&mut complete_opts.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "1".to_string());
        let err = set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, vec![part], &complete_opts)
            .await
            .expect_err("authoritative total below known logical part sizes must fail closed");
        assert!(matches!(err, StorageError::PartMissingOrCorrupt));

        for (object, declared_size) in [("invalid-total", "invalid"), ("negative-total", "-1")] {
            let create_opts = ObjectOptions {
                data_movement: true,
                user_defined: metadata.clone(),
                ..Default::default()
            };
            let upload = set_disks
                .new_multipart_upload(bucket, object, &create_opts)
                .await
                .expect("data movement upload should be created");
            let part =
                put_test_part_with_opts(&set_disks, bucket, object, &upload.upload_id, 1, (&[0x41], 1), &create_opts).await;
            let mut complete_opts = create_opts.clone();
            rustfs_utils::http::insert_str(
                &mut complete_opts.user_defined,
                rustfs_utils::http::SUFFIX_ACTUAL_SIZE,
                declared_size.to_string(),
            );

            let err = set_disks
                .clone()
                .complete_multipart_upload(bucket, object, &upload.upload_id, vec![part], &complete_opts)
                .await
                .expect_err("invalid authoritative total size must fail closed");
            assert!(matches!(err, StorageError::PartMissingOrCorrupt));
        }
    }

    async fn assert_complete_first_linearizes(bucket: &'static str, object: &'static str, create_opts: ObjectOptions) {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
        let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        make_bucket_on_all(&disk_stores, bucket).await;
        let (upload_id, parts) = stage_upload_with_create_opts(&set_disks, bucket, object, &[0x47; 4096], &create_opts).await;
        let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        signaling.set_target(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path));
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::AfterRename);

        let complete_store = set_disks.clone();
        let complete_upload_id = upload_id.clone();
        let complete = tokio::spawn(async move {
            complete_store
                .complete_multipart_upload(bucket, object, &complete_upload_id, parts, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;

        let abort_store = set_disks.clone();
        let abort_upload_id = upload_id.clone();
        let abort = tokio::spawn(async move {
            abort_store
                .abort_multipart_upload(bucket, object, &abort_upload_id, &ObjectOptions::default())
                .await
        });
        signaling.wait_for_attempts(2).await;
        assert!(!abort.is_finished(), "abort must wait for the completion upload lock");

        barrier.release();
        complete
            .await
            .expect("completion task should not panic")
            .expect("completion should win the upload finalization");
        let abort_err = abort
            .await
            .expect("abort task should not panic")
            .expect_err("abort must observe the upload as finalized");
        assert!(matches!(abort_err, StorageError::InvalidUploadID(..)));
        set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("complete-first must leave the committed object readable");
        assert!(matches!(
            set_disks.check_upload_id_exists(bucket, object, &upload_id, false).await,
            Err(StorageError::InvalidUploadID(..))
        ));
    }

    async fn assert_abort_first_linearizes(bucket: &'static str, object: &'static str, create_opts: ObjectOptions) {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
        let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        make_bucket_on_all(&disk_stores, bucket).await;
        let (upload_id, parts) = stage_upload_with_create_opts(&set_disks, bucket, object, &[0x48; 4096], &create_opts).await;
        let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        signaling.set_target(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path.clone()));
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let object_holder = set_disks
            .new_ns_lock(bucket, object)
            .await
            .expect("object namespace lock should be created")
            .get_write_lock(Duration::from_secs(5))
            .await
            .expect("test should hold the object lock");
        let holder = set_disks
            .new_ns_lock(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path)
            .await
            .expect("upload namespace lock should be created")
            .get_write_lock(Duration::from_secs(5))
            .await
            .expect("test should hold the upload lock");
        signaling.wait_for_attempts(1).await;

        let abort_store = set_disks.clone();
        let abort_upload_id = upload_id.clone();
        let abort = tokio::spawn(async move {
            abort_store
                .abort_multipart_upload(bucket, object, &abort_upload_id, &ObjectOptions::default())
                .await
        });
        signaling.wait_for_attempts(2).await;

        let complete_store = set_disks.clone();
        let complete_upload_id = upload_id.clone();
        let complete = tokio::spawn(async move {
            complete_store
                .complete_multipart_upload(bucket, object, &complete_upload_id, parts, &ObjectOptions::default())
                .await
        });
        drop(holder);

        abort
            .await
            .expect("abort task should not panic")
            .expect("abort should win the upload finalization");
        drop(object_holder);
        let complete_err = complete
            .await
            .expect("completion task should not panic")
            .expect_err("completion must observe the aborted upload");
        assert!(matches!(complete_err, StorageError::InvalidUploadID(..)));
        let object_err = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect_err("abort-first must not publish an object");
        assert!(matches!(object_err, StorageError::ObjectNotFound(..)));
        assert!(matches!(
            set_disks.check_upload_id_exists(bucket, object, &upload_id, false).await,
            Err(StorageError::InvalidUploadID(..))
        ));
    }

    async fn assert_quorum_minus_one_retry_preserves_completable_part(
        disk_count: usize,
        parity: usize,
        success_indices: &[usize],
    ) {
        use tokio::io::AsyncReadExt as _;

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_for_pool_with_default_parity(disk_count, 0, parity).await;
        let bucket = format!("multipart-retry-{disk_count}-{}", success_indices[0]);
        let object = "object";
        make_bucket_on_all(&disk_stores, &bucket).await;

        let payload = vec![0x41; 1 << 20];
        let (upload_id, parts) =
            stage_upload_with_create_opts(&set_disks, &bucket, object, &payload, &ObjectOptions::default()).await;
        let (upload_meta, _) = set_disks
            .check_upload_id_exists(&bucket, object, &upload_id, false)
            .await
            .expect("staged upload metadata should be readable");
        let upload_path = SetDisks::get_upload_id_dir(&bucket, object, &upload_id);
        let write_quorum = upload_meta.write_quorum(set_disks.default_write_quorum());
        assert_eq!(success_indices.len() + 1, write_quorum);
        let part_path = format!(
            "{}/{}/part.1",
            upload_path,
            upload_meta.data_dir.expect("multipart upload should have a data directory")
        );
        let acknowledged_meta = disk_stores[0]
            .read_all(RUSTFS_META_MULTIPART_BUCKET, &format!("{part_path}.meta"))
            .await
            .expect("acknowledged part metadata should be readable");
        let retry_path = format!("{}/part.1", Uuid::new_v4());

        let mut retry_disks = vec![None; disk_stores.len()];
        for &index in success_indices {
            disk_stores[index]
                .write_all(RUSTFS_META_TMP_BUCKET, &retry_path, Bytes::from_static(b"retry shard"))
                .await
                .expect("retry shard should be staged");
            retry_disks[index] = Some(disk_stores[index].clone());
        }

        let err = set_disks
            .rename_part(
                &retry_disks,
                RUSTFS_META_TMP_BUCKET,
                &retry_path,
                RUSTFS_META_MULTIPART_BUCKET,
                &part_path,
                acknowledged_meta,
                write_quorum,
                None,
            )
            .await
            .expect_err("quorum-minus-one renamed shards must remain below write quorum");
        assert_eq!(err, DiskError::ErasureWriteQuorum);

        for (index, disk) in disk_stores.iter().enumerate() {
            assert!(
                disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &part_path).await.is_ok(),
                "old acknowledged shard on disk {index} must survive"
            );
        }

        set_disks
            .clone()
            .complete_multipart_upload(&bucket, object, &upload_id, parts, &ObjectOptions::default())
            .await
            .expect("the old acknowledged part should remain completable");
        let mut reader = set_disks
            .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("completed object should be readable");
        let mut restored = Vec::new();
        reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("completed object should stream fully");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    async fn upload_part_retry_quorum_failure_preserves_old_part_across_ec_geometries() {
        assert_quorum_minus_one_retry_preserves_completable_part(4, 2, &[0, 1]).await;
        assert_quorum_minus_one_retry_preserves_completable_part(4, 2, &[2, 3]).await;
        assert_quorum_minus_one_retry_preserves_completable_part(6, 2, &[0, 1, 2]).await;
        assert_quorum_minus_one_retry_preserves_completable_part(6, 2, &[3, 4, 5]).await;
    }

    #[tokio::test]
    async fn complete_multipart_upload_recovers_interrupted_part_retry() {
        use tokio::io::AsyncReadExt as _;

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_for_pool_with_default_parity(6, 0, 2).await;
        let bucket = "multipart-retry-recovery";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;

        let payload = vec![0x42; 1 << 20];
        let (upload_id, parts) =
            stage_upload_with_create_opts(&set_disks, bucket, object, &payload, &ObjectOptions::default()).await;
        let (upload_meta, _) = set_disks
            .check_upload_id_exists(bucket, object, &upload_id, false)
            .await
            .expect("staged upload metadata should be readable");
        let upload_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        let part_path = format!(
            "{}/{}/part.1",
            upload_path,
            upload_meta.data_dir.expect("multipart upload should have a data directory")
        );
        let retry_meta = Bytes::from_static(b"interrupted retry metadata");

        for (index, disk) in disk_stores.iter().enumerate().take(3) {
            let retry_path = format!("{}/part.1", Uuid::new_v4());
            disk.write_all(RUSTFS_META_TMP_BUCKET, &retry_path, Bytes::from_static(b"interrupted retry shard"))
                .await
                .expect("retry shard should be staged");
            disk.prepare_part_transaction(
                RUSTFS_META_TMP_BUCKET,
                &retry_path,
                RUSTFS_META_MULTIPART_BUCKET,
                &part_path,
                retry_meta.clone(),
            )
            .await
            .expect("part transaction should be prepared");
            disk.rename_part(
                RUSTFS_META_TMP_BUCKET,
                &retry_path,
                RUSTFS_META_MULTIPART_BUCKET,
                &part_path,
                retry_meta.clone(),
            )
            .await
            .unwrap_or_else(|err| panic!("retry shard {index} should be published: {err}"));
        }

        set_disks
            .clone()
            .complete_multipart_upload(bucket, object, &upload_id, parts, &ObjectOptions::default())
            .await
            .expect("completion should roll back the interrupted quorum-minus-one retry");

        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("completed object should be readable");
        let mut restored = Vec::new();
        reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("completed object should stream fully");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    async fn rename_part_quorum_failure_without_old_part_removes_new_shards() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_for_pool_with_default_parity(4, 0, 2).await;
        let src_path = format!("{}/part.1", Uuid::new_v4());
        let dst_path = format!("{}/part.1", Uuid::new_v4());

        let mut retry_disks = vec![None; disk_stores.len()];
        for index in [0, 1] {
            disk_stores[index]
                .write_all(RUSTFS_META_TMP_BUCKET, &src_path, Bytes::from_static(b"new shard"))
                .await
                .expect("new shard should be staged");
            retry_disks[index] = Some(disk_stores[index].clone());
        }

        let err = set_disks
            .rename_part(
                &retry_disks,
                RUSTFS_META_TMP_BUCKET,
                &src_path,
                RUSTFS_META_MULTIPART_BUCKET,
                &dst_path,
                Bytes::from_static(b"retry metadata"),
                3,
                None,
            )
            .await
            .expect_err("two renamed shards must remain below write quorum");
        assert_eq!(err, DiskError::ErasureWriteQuorum);
        for (index, disk) in disk_stores.iter().enumerate() {
            assert!(
                matches!(disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &dst_path).await, Err(DiskError::FileNotFound)),
                "failed first upload must not leave a destination on disk {index}"
            );
        }
    }

    async fn make_multipart_lock_test_set_disks() -> Arc<SetDisks> {
        let endpoints = vec![
            Endpoint::try_from("http://127.0.0.1:9000/data").expect("first endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9001/data").expect("second endpoint should parse"),
        ];
        let lockers: Vec<Arc<dyn LockClient>> = vec![Arc::new(LocalClient::with_manager(Arc::new(
            rustfs_lock::GlobalLockManager::new(),
        )))];

        SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(vec![None, None])),
            2,
            1,
            0,
            0,
            endpoints,
            FormatV3::new(1, 2),
            lockers,
        )
        .await
    }

    #[tokio::test]
    #[serial(metadata_cache_invalidation_probe)]
    async fn complete_multipart_generation_retires_cached_snapshot() {
        use crate::storage_api_contracts::multipart::MultipartOperations as _;
        use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-metadata-generation-bucket";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut initial_reader = PutObjReader::from_vec(b"old multipart body".to_vec());
        set_disks
            .put_object(bucket, object, &mut initial_reader, &ObjectOptions::default())
            .await
            .expect("initial object should be written");
        set_disks
            .get_object_fileinfo(bucket, object, &ObjectOptions::default(), true, false)
            .await
            .expect("initial metadata should resolve");
        let generation = set_disks
            .get_object_metadata_cache_generation(bucket, object)
            .expect("metadata cache generation should be active");
        let retired_key = GetObjectMetadataCacheKey::new(bucket, object, generation);
        assert!(set_disks.get_object_metadata_cache.get(&retired_key).await.is_some());

        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        let payload = vec![9u8; 4096];
        let payload_len = i64::try_from(payload.len()).expect("test payload length should fit i64");
        let mut part_reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(payload), payload_len, payload_len, None, None, false)
                .expect("part hash reader should be created"),
        );
        let part = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
            .await
            .expect("multipart part should be written");

        let invalidations = MetadataCacheInvalidationProbe::install(bucket, object);
        set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("multipart completion should succeed");

        assert_eq!(
            invalidations.count(),
            2,
            "multipart completion must invalidate before mutation and after commit"
        );
        set_disks.get_object_metadata_cache.run_pending_tasks().await;
        assert!(set_disks.get_object_metadata_cache.get(&retired_key).await.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn multipart_upload_read_lock_waits_for_upload_writer() {
        let set_disks = make_multipart_lock_test_set_disks().await;
        let upload_id_path = SetDisks::get_upload_id_dir("bucket", "object", "upload-id");
        let _writer = set_disks
            .new_ns_lock(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path)
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer upload write lock should be acquired");

        let result = temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            set_disks
                .acquire_multipart_upload_read_lock(
                    "test_multipart_upload_read_lock",
                    "bucket",
                    "object",
                    "upload-id",
                    &ObjectOptions::default(),
                )
                .await
        })
        .await;
        let err = match result {
            Ok(_) => panic!("read lock must wait behind an upload write lock"),
            Err(err) => err,
        };

        assert!(matches!(err, StorageError::Lock(rustfs_lock::LockError::Timeout { .. })));

        let no_lock_guard = set_disks
            .acquire_multipart_upload_read_lock(
                "test_multipart_upload_read_lock_no_lock",
                "bucket",
                "object",
                "upload-id",
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("no_lock read path should not acquire an upload lock");
        assert!(no_lock_guard.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn multipart_upload_write_lock_waits_for_upload_reader() {
        let set_disks = make_multipart_lock_test_set_disks().await;
        let upload_id_path = SetDisks::get_upload_id_dir("bucket", "object", "upload-id");
        let _reader = set_disks
            .new_ns_lock(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path)
            .await
            .expect("namespace lock should be created")
            .get_read_lock(Duration::from_secs(1))
            .await
            .expect("outer upload read lock should be acquired");

        let result = temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            set_disks
                .acquire_multipart_upload_write_lock(
                    "test_multipart_upload_write_lock",
                    "bucket",
                    "object",
                    "upload-id",
                    &ObjectOptions::default(),
                )
                .await
        })
        .await;
        let err = match result {
            Ok(_) => panic!("write lock must wait behind an upload read lock"),
            Err(err) => err,
        };

        assert!(matches!(err, StorageError::Lock(rustfs_lock::LockError::Timeout { .. })));

        let no_lock_guard = set_disks
            .acquire_multipart_upload_write_lock(
                "test_multipart_upload_write_lock_no_lock",
                "bucket",
                "object",
                "upload-id",
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("no_lock write path should not acquire an upload lock");
        assert!(no_lock_guard.is_none());
    }

    #[tokio::test]
    async fn collect_list_parts_results_fails_early_when_quorum_is_impossible() {
        let started = Instant::now();
        let tasks: Vec<_> = vec![
            (10_u64, Err(DiskError::DiskNotFound)),
            (15, Err(DiskError::DiskNotFound)),
            (250, Ok::<Vec<String>, DiskError>(vec!["part.1".to_string(), "part.1.meta".to_string()])),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let err = collect_list_parts_results(tasks, 2)
            .await
            .expect_err("quorum should become impossible before slow tail completes");

        assert_eq!(err, DiskError::ErasureReadQuorum);
        assert!(started.elapsed() < Duration::from_millis(120));
    }

    #[tokio::test]
    async fn collect_list_parts_results_tolerates_single_panicked_task_when_quorum_is_met() {
        let tasks: Vec<_> = vec![(5_u64, true), (10, false), (12, false)]
            .into_iter()
            .map(|(delay_ms, should_panic)| async move {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                if should_panic {
                    panic!("simulated task panic");
                }
                Ok::<Vec<String>, DiskError>(vec!["part.1".to_string(), "part.1.meta".to_string()])
            })
            .collect();

        let (errs, object_parts) = collect_list_parts_results(tasks, 2)
            .await
            .expect("quorum should still succeed");
        assert_eq!(errs.iter().filter(|err| err.is_none()).count(), 2);
        assert_eq!(object_parts.iter().filter(|parts| !parts.is_empty()).count(), 2);
    }

    #[tokio::test]
    async fn collect_list_parts_results_returns_file_not_found_for_empty_upload_dirs() {
        let tasks: Vec<_> = vec![
            (5_u64, Err(DiskError::FileNotFound)),
            (10, Err(DiskError::DiskNotFound)),
            (12, Err(DiskError::DiskNotFound)),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let err = collect_list_parts_results(tasks, 2)
            .await
            .expect_err("missing multipart directories should be treated as empty uploads");

        assert_eq!(err, DiskError::FileNotFound);
    }

    #[tokio::test]
    async fn collect_list_parts_results_fails_early_when_file_not_found_fallback_is_impossible() {
        let started = Instant::now();
        let tasks: Vec<_> = vec![
            (5_u64, Err(DiskError::FileNotFound)),
            (10, Err(DiskError::FileCorrupt)),
            (250, Err(DiskError::DiskNotFound)),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let err = collect_list_parts_results(tasks, 2)
            .await
            .expect_err("non-ignored errors should preserve early quorum failure");

        assert_eq!(err, DiskError::ErasureReadQuorum);
        assert!(started.elapsed() < Duration::from_millis(120));
    }

    #[tokio::test]
    async fn put_object_part_failure_cleans_tmp_workspace_inline() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-tmp-clean-bucket";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        let declared_size = 1024 * 1024;
        let short_stream = Cursor::new(vec![7u8; 512]);
        let mut reader = PutObjReader::new(
            HashReader::from_stream(short_stream, declared_size, declared_size, None, None, false)
                .expect("hash reader should be constructed"),
        );

        let err = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
            .await
            .expect_err("short multipart stream should fail");

        let leftovers = non_trash_tmp_entries(&temp_dirs).await;
        assert!(
            leftovers.is_empty(),
            "failed multipart upload part must not leave tmp shards behind, leftovers: {leftovers:?}, err: {err}"
        );
    }

    #[tokio::test]
    async fn put_object_part_recovers_transaction_with_one_faulty_disk_at_write_quorum() {
        use tokio::io::AsyncReadExt as _;

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_for_pool_with_default_parity(4, 0, 2).await;
        assert_eq!(set_disks.default_read_quorum(), 2);
        assert_eq!(set_disks.default_write_quorum(), 3);

        let bucket = "multipart-degraded-upload-part-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;

        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created before the disk fault");
        disk_stores[0]
            .set_disk_id_state(Some(Uuid::new_v4()))
            .await
            .expect("test should mark one disk stale");

        let payload = vec![0x5b; 4096];
        let mut reader = PutObjReader::from_vec(payload.clone());
        let part = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
            .await
            .expect("upload part should commit with exactly write quorum healthy disks");

        set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("completion should settle the write-quorum part");

        let mut object_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("completed object should be readable through read quorum");
        let mut restored = Vec::new();
        object_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("completed object should stream fully");
        assert_eq!(restored, payload);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn put_object_part_rechecks_upload_after_commit_lock() {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
        let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "multipart-post-lock-recheck-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        let upload_id = upload.upload_id;
        let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        signaling.set_target(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path.clone()));
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let holder = set_disks
            .new_ns_lock(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path)
            .await
            .expect("upload namespace lock should be created")
            .get_write_lock(Duration::from_secs(5))
            .await
            .expect("test should hold the upload commit lock");
        signaling.wait_for_attempts(1).await;

        let mut reader = PutObjReader::from_vec(vec![0x5a; 4096]);
        let put_store = set_disks.clone();
        let put_upload_id = upload_id.clone();
        let put = tokio::spawn(async move {
            put_store
                .put_object_part(bucket, object, &put_upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
        });

        signaling.wait_for_attempts(2).await;
        set_disks
            .delete_all(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path)
            .await
            .expect("test should remove the upload after the part waits on its commit lock");
        drop(holder);

        let err = tokio::time::timeout(Duration::from_secs(10), put)
            .await
            .expect("part upload should finish")
            .expect("part task should not panic")
            .expect_err("part upload must recheck the upload after acquiring its commit lock");
        assert!(matches!(err, StorageError::InvalidUploadID(..)));
        assert!(matches!(
            set_disks.check_upload_id_exists(bucket, object, &upload_id, false).await,
            Err(StorageError::InvalidUploadID(..))
        ));
        for temp_dir in temp_dirs {
            assert!(
                !temp_dir
                    .path()
                    .join(RUSTFS_META_MULTIPART_BUCKET)
                    .join(&upload_id_path)
                    .exists(),
                "late UploadPart must not recreate multipart staging"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn put_object_part_holds_upload_lock_through_rename() {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
        let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "multipart-put-part-lock-lifetime-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        let upload_id = upload.upload_id;
        let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        signaling.set_target(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path));
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::PutPartAfterRename);

        let put_store = set_disks.clone();
        let put_upload_id = upload_id.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x45; 4096]);
            put_store
                .put_object_part(bucket, object, &put_upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;

        let abort_store = set_disks.clone();
        let abort_upload_id = upload_id.clone();
        let abort = tokio::spawn(async move {
            abort_store
                .abort_multipart_upload(bucket, object, &abort_upload_id, &ObjectOptions::default())
                .await
        });
        signaling.wait_for_attempts(2).await;
        tokio::task::yield_now().await;
        assert!(!abort.is_finished(), "abort must wait until UploadPart releases the upload lock");

        barrier.release();
        put.await
            .expect("UploadPart task should not panic")
            .expect("UploadPart should return after releasing the barrier");
        abort
            .await
            .expect("abort task should not panic")
            .expect("abort should delete the upload after UploadPart releases the lock");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn put_object_part_different_part_numbers_commit_concurrently() {
        use tokio::io::AsyncReadExt as _;

        const PART1_SIZE: usize = 5 * 1024 * 1024; // non-final parts must be >= 5MiB to complete
        const PART2_SIZE: usize = 4096;

        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let locker: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager));
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, vec![locker]).await;
        let bucket = "multipart-concurrent-part-numbers-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        let upload_id = upload.upload_id;
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        // issue#5961: the barrier releases only once BOTH commits are paused
        // inside their commit sections, so reaching wait_until_paused proves the
        // two part numbers held their commit locks concurrently. Under an
        // uploadId-wide exclusive commit lock the second put errors at the 5s
        // lock-acquire timeout instead of arriving, and wait_until_paused fails
        // deterministically. No wall-clock bound on the success path.
        let barrier = MultipartCommitBarrier::install_for_arrivals(bucket, object, MultipartCommitPause::PutPartAfterRename, 2);

        let put1_store = set_disks.clone();
        let put1_upload_id = upload_id.clone();
        let put1 = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x51; PART1_SIZE]);
            put1_store
                .put_object_part(bucket, object, &put1_upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
        });
        let put2_store = set_disks.clone();
        let put2_upload_id = upload_id.clone();
        let put2 = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x52; PART2_SIZE]);
            put2_store
                .put_object_part(bucket, object, &put2_upload_id, 2, &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;

        barrier.release();
        let part1 = put1
            .await
            .expect("part 1 task should not panic")
            .expect("part 1 should commit after the barrier is released");
        let part2 = put2
            .await
            .expect("part 2 task should not panic")
            .expect("part 2 should commit after the barrier is released");
        assert_eq!(part1.part_num, 1);
        assert_eq!(part2.part_num, 2);

        set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload_id,
                vec![
                    CompletePart {
                        part_num: part1.part_num,
                        etag: part1.etag.clone(),
                        ..Default::default()
                    },
                    CompletePart {
                        part_num: part2.part_num,
                        etag: part2.etag.clone(),
                        ..Default::default()
                    },
                ],
                &ObjectOptions::default(),
            )
            .await
            .expect("completion should succeed with both concurrently committed parts");

        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("completed object should open");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("completed object should stream fully");
        assert_eq!(body.len(), PART1_SIZE + PART2_SIZE);
        assert!(body[..PART1_SIZE].iter().all(|b| *b == 0x51), "part 1 bytes must round-trip");
        assert!(body[PART1_SIZE..].iter().all(|b| *b == 0x52), "part 2 bytes must round-trip");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn put_object_part_same_part_retries_serialize_on_part_lock() {
        use tokio::io::AsyncReadExt as _;

        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
        let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "multipart-same-part-retry-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        let upload_id = upload.upload_id;
        let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        let part_lock_path = format!("{upload_id_path}/part.1");
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::PutPartAfterRename);

        let first_store = set_disks.clone();
        let first_upload_id = upload_id.clone();
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x53; 4096]);
            first_store
                .put_object_part(bucket, object, &first_upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;

        // The paused commit must hold its part lock EXCLUSIVELY: even a shared
        // probe on the part key has to time out. This pins the write-ness of the
        // part lock — a shared part lock would let two same-part rename_part
        // calls interleave into mixed-generation shards (backlog#853).
        let probe = set_disks
            .new_ns_lock(RUSTFS_META_MULTIPART_BUCKET, &part_lock_path)
            .await
            .expect("part namespace lock should be created")
            .get_read_lock(Duration::from_secs(1))
            .await;
        assert!(
            probe.is_err(),
            "the in-flight part commit must hold an exclusive write lock on its part key"
        );

        signaling.set_target(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, part_lock_path));
        let retry_store = set_disks.clone();
        let retry_upload_id = upload_id.clone();
        let retry = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x54; 4096]);
            retry_store
                .put_object_part(bucket, object, &retry_upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
        });
        signaling.wait_for_attempts(1).await;
        tokio::task::yield_now().await;
        assert!(
            !retry.is_finished(),
            "a retry of the same part number must wait for the in-flight commit (backlog#853)"
        );

        barrier.release();
        first
            .await
            .expect("first attempt task should not panic")
            .expect("first attempt should commit after the barrier is released");
        let retry_part = retry
            .await
            .expect("retry task should not panic")
            .expect("the retry should commit after the first attempt releases the part lock");

        set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload_id,
                vec![CompletePart {
                    part_num: retry_part.part_num,
                    etag: retry_part.etag.clone(),
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("the last committed retry must win the final part generation");
        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("completed object should open");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("completed object should stream fully");
        assert_eq!(body, vec![0x54; 4096], "the retry's generation must be the one served");
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn put_object_part_fences_part_lock_loss_before_rename() {
        let target = Arc::new(std::sync::RwLock::new(None));
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let lockers: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| {
                Arc::new(SelectiveLockLossClient::new(Arc::clone(&target), Arc::clone(&refresh_calls))) as Arc<dyn LockClient>
            })
            .collect();
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "multipart-put-part-part-lock-loss-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        let upload_id = upload.upload_id;
        let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        let part_lock_path = format!("{upload_id_path}/part.1");
        *target.write().expect("lock-loss target should be writable") =
            Some(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, part_lock_path.clone()));
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::PutPartBeforeLockLost);

        let put_store = set_disks.clone();
        let put_upload_id = upload_id.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x47; 4096]);
            put_store
                .put_object_part(bucket, object, &put_upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "part lock heartbeat should reach the test client"
        );
        barrier.release();

        let err = put
            .await
            .expect("UploadPart task should not panic")
            .expect_err("UploadPart must fail after losing the part lock");
        match err {
            StorageError::NamespaceLockQuorumUnavailable {
                bucket: lock_bucket,
                object: lock_object,
                ..
            } => {
                assert_eq!(lock_bucket, RUSTFS_META_MULTIPART_BUCKET);
                assert_eq!(lock_object, part_lock_path);
            }
            other => panic!("unexpected lock-loss error: {other:?}"),
        }
        let listed = set_disks
            .list_object_parts(bucket, object, &upload_id, None, MAX_PARTS_COUNT, &ObjectOptions::default())
            .await
            .expect("part lock loss before rename must leave the upload readable");
        assert!(listed.parts.is_empty(), "part lock loss before rename must not publish the part");
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn put_object_part_fences_upload_lock_loss_before_rename() {
        let target = Arc::new(std::sync::RwLock::new(None));
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let lockers: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| {
                Arc::new(SelectiveLockLossClient::new(Arc::clone(&target), Arc::clone(&refresh_calls))) as Arc<dyn LockClient>
            })
            .collect();
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "multipart-put-part-lock-loss-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        let upload_id = upload.upload_id;
        let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        *target.write().expect("lock-loss target should be writable") =
            Some(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path.clone()));
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::PutPartBeforeLockLost);

        let put_store = set_disks.clone();
        let put_upload_id = upload_id.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x46; 4096]);
            put_store
                .put_object_part(bucket, object, &put_upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::Acquire) > 0,
            "upload lock heartbeat should reach the test client"
        );
        barrier.release();

        let err = put
            .await
            .expect("UploadPart task should not panic")
            .expect_err("UploadPart must fail after losing the upload lock");
        match err {
            StorageError::NamespaceLockQuorumUnavailable {
                bucket: lock_bucket,
                object: lock_object,
                ..
            } => {
                assert_eq!(lock_bucket, RUSTFS_META_MULTIPART_BUCKET);
                assert_eq!(lock_object, upload_id_path);
            }
            other => panic!("unexpected lock-loss error: {other:?}"),
        }
        let listed = set_disks
            .list_object_parts(bucket, object, &upload_id, None, MAX_PARTS_COUNT, &ObjectOptions::default())
            .await
            .expect("lock loss before rename must leave the upload readable");
        assert!(listed.parts.is_empty(), "lock loss before rename must not publish the part");
    }

    #[tokio::test]
    async fn complete_encrypted_multipart_marks_quorum_validated_layout() {
        temp_env::async_with_vars([(crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            assert_eq!(
                crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
                "encrypted-part-layout-quorum-candidate-v1"
            );
            assert_eq!(crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX, "encrypted-part-layout-quorum-v1");
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "multipart-layout-quorum-marker-bucket";
            make_bucket_on_all(&disk_stores, bucket).await;
            let encrypted_opts = ObjectOptions {
                user_defined: HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]),
                ..Default::default()
            };

            let (upload_id, parts) =
                stage_upload_with_create_opts(&set_disks, bucket, "encrypted", &[0x31; 4096], &encrypted_opts).await;
            let (staged, _) = set_disks
                .check_upload_id_exists(bucket, "encrypted", &upload_id, false)
                .await
                .expect("encrypted multipart staging metadata should be readable");
            let staged_layout_token = staged.data_dir.expect("staging data dir should exist").to_string();
            assert_eq!(
                rustfs_utils::http::get_consistent_str(
                    &staged.metadata,
                    crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX
                ),
                Some(staged_layout_token.as_str())
            );
            assert_eq!(
                rustfs_utils::http::get_consistent_str(&staged.metadata, crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX),
                None
            );
            let completed = set_disks
                .clone()
                .complete_multipart_upload(bucket, "encrypted", &upload_id, parts, &ObjectOptions::default())
                .await
                .expect("encrypted multipart upload should complete");
            let reread = set_disks
                .get_object_info(bucket, "encrypted", &ObjectOptions::default())
                .await
                .expect("completed object metadata should be readable");
            let completed_layout_token = completed.data_dir.expect("completed data dir should exist").to_string();
            assert_eq!(reread.data_dir, completed.data_dir);
            for metadata in [&completed.user_defined, &reread.user_defined] {
                for prefix in [
                    rustfs_utils::http::RUSTFS_INTERNAL_PREFIX,
                    rustfs_utils::http::MINIO_INTERNAL_PREFIX,
                ] {
                    let key = format!("{prefix}{}", crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX);
                    assert_eq!(metadata.get(&key).map(String::as_str), Some(completed_layout_token.as_str()));
                }
                assert_eq!(
                    rustfs_utils::http::get_consistent_str(metadata, crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX),
                    None
                );
            }

            let (upload_id, parts) =
                stage_upload_with_create_opts(&set_disks, bucket, "unencrypted", &[0x32; 4096], &ObjectOptions::default()).await;
            let unencrypted = set_disks
                .clone()
                .complete_multipart_upload(bucket, "unencrypted", &upload_id, parts, &ObjectOptions::default())
                .await
                .expect("unencrypted multipart upload should complete");
            assert_eq!(
                rustfs_utils::http::get_consistent_str(
                    &unencrypted.user_defined,
                    crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX
                ),
                None
            );

            let (upload_id, parts) =
                stage_upload_with_create_opts(&set_disks, bucket, "unlocked", &[0x33; 4096], &encrypted_opts).await;
            let unlocked = set_disks
                .clone()
                .complete_multipart_upload(
                    bucket,
                    "unlocked",
                    &upload_id,
                    parts,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("unlocked encrypted multipart upload should complete");
            assert_eq!(
                rustfs_utils::http::get_consistent_str(
                    &unlocked.user_defined,
                    crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX
                ),
                None
            );

            let unlocked_create_opts = ObjectOptions {
                no_lock: true,
                user_defined: encrypted_opts.user_defined.clone(),
                ..Default::default()
            };
            let (upload_id, parts) =
                stage_upload_with_create_opts(&set_disks, bucket, "unlocked-create", &[0x34; 4096], &unlocked_create_opts).await;
            let unlocked_create = set_disks
                .clone()
                .complete_multipart_upload(bucket, "unlocked-create", &upload_id, parts, &ObjectOptions::default())
                .await
                .expect("multipart upload created without locking should complete");
            assert_eq!(
                rustfs_utils::http::get_consistent_str(
                    &unlocked_create.user_defined,
                    crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX
                ),
                None
            );

            let mut pre_upgrade_opts = encrypted_opts.clone();
            insert_str(
                &mut pre_upgrade_opts.user_defined,
                crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
                "forged".to_string(),
            );
            insert_str(
                &mut pre_upgrade_opts.user_defined,
                crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
                "forged-final".to_string(),
            );
            let (upload_id, parts) = temp_env::async_with_vars(
                [(crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("false"))],
                stage_upload_with_create_opts(&set_disks, bucket, "pre-upgrade", &[0x35; 4096], &pre_upgrade_opts),
            )
            .await;
            let (staged, _) = set_disks
                .check_upload_id_exists(bucket, "pre-upgrade", &upload_id, false)
                .await
                .expect("pre-upgrade multipart staging metadata should be readable");
            for suffix in [
                crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
                crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
            ] {
                assert_eq!(rustfs_utils::http::get_consistent_str(&staged.metadata, suffix), None);
            }
            let pre_upgrade = set_disks
                .clone()
                .complete_multipart_upload(bucket, "pre-upgrade", &upload_id, parts, &ObjectOptions::default())
                .await
                .expect("pre-upgrade multipart upload should complete");
            assert_eq!(
                rustfs_utils::http::get_consistent_str(
                    &pre_upgrade.user_defined,
                    crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX
                ),
                None
            );

            let (upload_id, parts) =
                stage_upload_with_create_opts(&set_disks, bucket, "disabled-complete", &[0x36; 4096], &encrypted_opts).await;
            let disabled_complete = temp_env::async_with_vars(
                [(crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("false"))],
                set_disks.clone().complete_multipart_upload(
                    bucket,
                    "disabled-complete",
                    &upload_id,
                    parts,
                    &ObjectOptions::default(),
                ),
            )
            .await
            .expect("multipart completion with rollout disabled should succeed");
            for suffix in [
                crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
                crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
            ] {
                assert_eq!(rustfs_utils::http::get_consistent_str(&disabled_complete.user_defined, suffix), None);
            }
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn abort_and_complete_linearize_for_plain_sse_and_legacy_layouts() {
        assert_complete_first_linearizes("multipart-complete-first-plain", "object", ObjectOptions::default()).await;
        assert_abort_first_linearizes("multipart-abort-first-plain", "object", ObjectOptions::default()).await;

        let encrypted_opts = ObjectOptions {
            user_defined: HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]),
            ..Default::default()
        };
        temp_env::async_with_vars([(crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            assert_complete_first_linearizes("multipart-complete-first-sse", "object", encrypted_opts.clone()).await;
            assert_abort_first_linearizes("multipart-abort-first-sse", "object", encrypted_opts.clone()).await;
        })
        .await;
        temp_env::async_with_vars([(crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("false"))], async {
            assert_complete_first_linearizes("multipart-complete-first-legacy", "object", encrypted_opts.clone()).await;
            assert_abort_first_linearizes("multipart-abort-first-legacy", "object", encrypted_opts).await;
        })
        .await;
    }

    #[tokio::test]
    async fn abort_enforces_delete_write_quorum_boundary() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-abort-delete-quorum";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let quorum_upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");

        let saved_disks = {
            let mut disks = set_disks.disks.write().await;
            let saved = disks.clone();
            disks[3] = None;
            saved
        };
        set_disks
            .abort_multipart_upload(bucket, object, &quorum_upload.upload_id, &ObjectOptions::default())
            .await
            .expect("abort should succeed at the exact delete write quorum");
        *set_disks.disks.write().await = saved_disks;
        assert!(matches!(
            set_disks
                .check_upload_id_exists(bucket, object, &quorum_upload.upload_id, false)
                .await,
            Err(StorageError::InvalidUploadID(..))
        ));

        let below_quorum_upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("second multipart upload should be created");
        let saved_disks = {
            let mut disks = set_disks.disks.write().await;
            let saved = disks.clone();
            disks[2] = None;
            disks[3] = None;
            saved
        };
        let err = set_disks
            .abort_multipart_upload(bucket, object, &below_quorum_upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("abort must report a delete below write quorum");
        assert!(matches!(err, StorageError::ErasureWriteQuorum));

        *set_disks.disks.write().await = saved_disks;
        set_disks
            .check_upload_id_exists(bucket, object, &below_quorum_upload.upload_id, false)
            .await
            .expect("failed abort must leave quorum-visible staging on the restored disks");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn complete_revalidates_layout_candidate_after_upload_lock() {
        temp_env::async_with_vars([(crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
            let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
            let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
            let bucket = "multipart-layout-recheck-bucket";
            let object = "object";
            make_bucket_on_all(&disk_stores, bucket).await;
            let create_opts = ObjectOptions {
                user_defined: HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]),
                ..Default::default()
            };
            let (upload_id, parts) = stage_upload_with_create_opts(&set_disks, bucket, object, &[0x42; 4096], &create_opts).await;
            let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
            signaling.set_target(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path.clone()));
            let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
            let holder = set_disks
                .new_ns_lock(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path)
                .await
                .expect("upload namespace lock should be created")
                .get_write_lock(Duration::from_secs(5))
                .await
                .expect("test should hold the upload commit lock");
            signaling.wait_for_attempts(1).await;

            let complete_store = set_disks.clone();
            let complete_upload_id = upload_id.clone();
            let complete = tokio::spawn(async move {
                complete_store
                    .complete_multipart_upload(bucket, object, &complete_upload_id, parts, &ObjectOptions::default())
                    .await
            });
            signaling.wait_for_attempts(2).await;

            let disks = set_disks.disks.read().await.clone();
            let (fi, mut files_metas) = set_disks
                .check_upload_id_exists(bucket, object, &upload_id, true)
                .await
                .expect("staged upload metadata should be readable");
            for meta in &mut files_metas {
                rustfs_utils::http::metadata_compat::remove_str(
                    &mut meta.metadata,
                    crate::object_api::ENCRYPTED_PART_LAYOUT_CANDIDATE_SUFFIX,
                );
            }
            SetDisks::write_unique_file_info(
                &disks,
                bucket,
                RUSTFS_META_MULTIPART_BUCKET,
                &upload_id_path,
                &files_metas,
                fi.write_quorum(set_disks.default_write_quorum()),
            )
            .await
            .expect("staged candidate should be removed while completion waits");
            drop(holder);

            let completed = tokio::time::timeout(Duration::from_secs(10), complete)
                .await
                .expect("completion should finish")
                .expect("completion task should not panic")
                .expect("completion should safely fall back without a candidate");
            assert_eq!(
                rustfs_utils::http::get_consistent_str(
                    &completed.user_defined,
                    crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX
                ),
                None
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn complete_holds_object_then_upload_lock_through_commit() {
        temp_env::async_with_vars(
            [
                (crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true")),
                (rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("60")),
            ],
            async {
                let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
                let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
                let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
                let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
                let bucket = "multipart-layout-lock-order-bucket";
                let object = "object";
                make_bucket_on_all(&disk_stores, bucket).await;
                let create_opts = ObjectOptions {
                    user_defined: HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]),
                    ..Default::default()
                };
                let (upload_id, parts) =
                    stage_upload_with_create_opts(&set_disks, bucket, object, &[0x43; 4096], &create_opts).await;
                let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
                let upload_resource = rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path);
                let object_resource = rustfs_lock::ObjectKey::new(bucket, object);
                signaling.set_target(upload_resource.clone());
                signaling.clear_observed();
                let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
                let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::AfterRename);

                let complete_store = set_disks.clone();
                let complete_upload_id = upload_id.clone();
                let complete = tokio::spawn(async move {
                    complete_store
                        .complete_multipart_upload(bucket, object, &complete_upload_id, parts, &ObjectOptions::default())
                        .await
                });
                barrier.wait_until_paused().await;

                let observed = signaling.observed();
                let object_position = observed
                    .iter()
                    .position(|resource| resource == &object_resource)
                    .expect("completion should acquire the object lock");
                let upload_position = observed
                    .iter()
                    .position(|resource| resource == &upload_resource)
                    .expect("completion should acquire the upload lock");
                assert!(object_position < upload_position, "completion must acquire object before upload");

                let abort_store = set_disks.clone();
                let abort_upload_id = upload_id.clone();
                let abort = tokio::spawn(async move {
                    abort_store
                        .abort_multipart_upload(bucket, object, &abort_upload_id, &ObjectOptions::default())
                        .await
                });
                signaling.wait_for_attempts(2).await;
                tokio::task::yield_now().await;
                assert!(!abort.is_finished(), "abort must wait until completion releases the upload lock");

                let list_store = set_disks.clone();
                let list_upload_id = upload_id.clone();
                let list = tokio::spawn(async move {
                    list_store
                        .list_object_parts(bucket, object, &list_upload_id, None, MAX_PARTS_COUNT, &ObjectOptions::default())
                        .await
                });
                signaling.wait_for_attempts(3).await;
                tokio::task::yield_now().await;
                assert!(!list.is_finished(), "ListParts must wait until completion releases the upload lock");

                barrier.release();
                complete
                    .await
                    .expect("completion task should not panic")
                    .expect("completion should commit after the barrier is released");
                let abort_err = abort
                    .await
                    .expect("abort task should not panic")
                    .expect_err("the committed upload should no longer exist when abort acquires the lock");
                assert!(
                    matches!(abort_err, StorageError::InvalidUploadID(..)),
                    "abort should return InvalidUploadID after completion, got {abort_err:?}"
                );
                let list_err = list
                    .await
                    .expect("ListParts task should not panic")
                    .expect_err("the committed upload should no longer exist when ListParts acquires the lock");
                assert!(
                    matches!(list_err, StorageError::InvalidUploadID(..)),
                    "ListParts should return InvalidUploadID after completion, got {list_err:?}"
                );
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn cancelled_complete_keeps_upload_lock_through_tail_cleanup() {
        temp_env::async_with_vars(
            [
                (crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true")),
                (rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("60")),
            ],
            async {
                let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
                let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
                let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
                let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
                let bucket = "multipart-cancelled-tail-lock-bucket";
                let object = "object";
                make_bucket_on_all(&disk_stores, bucket).await;
                let (upload_id, parts) =
                    stage_upload_with_create_opts(&set_disks, bucket, object, &[0x53; 4096], &ObjectOptions::default()).await;
                let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
                signaling.set_target(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path));
                signaling.clear_observed();
                let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
                let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::AfterRename);

                let complete_store = set_disks.clone();
                let complete_upload_id = upload_id.clone();
                let complete = tokio::spawn(async move {
                    complete_store
                        .complete_multipart_upload(bucket, object, &complete_upload_id, parts, &ObjectOptions::default())
                        .await
                });
                barrier.wait_until_paused().await;

                let abort_store = set_disks.clone();
                let abort_upload_id = upload_id.clone();
                let abort = tokio::spawn(async move {
                    abort_store
                        .abort_multipart_upload(bucket, object, &abort_upload_id, &ObjectOptions::default())
                        .await
                });
                signaling.wait_for_attempts(2).await;
                tokio::task::yield_now().await;
                assert!(!abort.is_finished(), "abort must wait while completion tail owns the upload lock");

                complete.abort();
                assert!(
                    complete
                        .await
                        .expect_err("the completion request should be cancellable while the tail is paused")
                        .is_cancelled()
                );
                tokio::task::yield_now().await;
                assert!(!abort.is_finished(), "cancelling the completion waiter must not release the upload lock");

                barrier.release();
                let abort_err = abort
                    .await
                    .expect("abort task should not panic")
                    .expect_err("the committed upload should no longer exist when abort acquires the lock");
                assert!(
                    matches!(abort_err, StorageError::InvalidUploadID(..)),
                    "abort should return InvalidUploadID after the detached completion tail, got {abort_err:?}"
                );
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn complete_releases_object_lock_before_cleanup_and_keeps_upload_lock() {
        temp_env::async_with_vars(
            [
                (crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true")),
                (rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("60")),
            ],
            async {
                let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
                let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
                let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
                let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
                let bucket = "multipart-object-lock-short-tail-bucket";
                let object = "object";
                make_bucket_on_all(&disk_stores, bucket).await;
                let completed_body = vec![0x63; 4096];
                let replacement_body = vec![0x64; 4096];
                let (upload_id, parts) =
                    stage_upload_with_create_opts(&set_disks, bucket, object, &completed_body, &ObjectOptions::default()).await;
                let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
                signaling.set_target(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path));
                signaling.clear_observed();
                let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
                let completion_barrier =
                    MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::AfterObjectPublication);

                let complete_store = set_disks.clone();
                let complete_upload_id = upload_id.clone();
                let complete = tokio::spawn(async move {
                    complete_store
                        .complete_multipart_upload(bucket, object, &complete_upload_id, parts, &ObjectOptions::default())
                        .await
                });
                completion_barrier.wait_until_paused().await;

                let mut reader = tokio::time::timeout(
                    Duration::from_secs(10),
                    set_disks.get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default()),
                )
                .await
                .expect("GET should not wait for multipart cleanup after object publication")
                .expect("completed object should be readable while the upload tail is paused");
                let mut observed_body = Vec::new();
                tokio::time::timeout(Duration::from_secs(10), reader.stream.read_to_end(&mut observed_body))
                    .await
                    .expect("completed object body should stream while the upload tail is paused")
                    .expect("completed object body should read successfully");
                assert_eq!(observed_body, completed_body);

                let put_barrier = PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
                let put_store = set_disks.clone();
                let put_payload = replacement_body.clone();
                let put = tokio::spawn(async move {
                    let mut reader = PutObjReader::from_vec(put_payload);
                    put_store
                        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                        .await
                });
                put_barrier.wait_until_paused().await;

                let abort_store = set_disks.clone();
                let abort_upload_id = upload_id.clone();
                let abort = tokio::spawn(async move {
                    abort_store
                        .abort_multipart_upload(bucket, object, &abort_upload_id, &ObjectOptions::default())
                        .await
                });
                signaling.wait_for_attempts(2).await;
                tokio::task::yield_now().await;
                assert!(
                    !abort.is_finished(),
                    "abort must still wait while the completion tail owns the upload lock"
                );

                complete.abort();
                assert!(
                    complete
                        .await
                        .expect_err("the completion waiter should remain cancellable after object publication")
                        .is_cancelled()
                );
                tokio::task::yield_now().await;
                assert!(!abort.is_finished(), "cancelling the waiter must not release the upload lock");

                completion_barrier.release();
                let abort_err = abort
                    .await
                    .expect("abort task should not panic")
                    .expect_err("the committed upload should no longer exist after the detached tail drains");
                assert!(
                    matches!(abort_err, StorageError::InvalidUploadID(..)),
                    "abort should return InvalidUploadID after the completion tail, got {abort_err:?}"
                );

                put_barrier.release();
                put.await
                    .expect("same-key PUT task should not panic")
                    .expect("same-key PUT should commit after the object lock is released early");

                let mut reader = set_disks
                    .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("final object should be readable");
                let mut final_body = Vec::new();
                reader
                    .stream
                    .read_to_end(&mut final_body)
                    .await
                    .expect("final object should stream fully");
                assert_eq!(final_body, replacement_body);
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn complete_keeps_object_lock_until_publication_fence() {
        temp_env::async_with_vars(
            [
                (crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true")),
                (rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("60")),
            ],
            async {
                let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
                let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
                let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
                let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
                let bucket = "multipart-publication-fence-lock-bucket";
                let object = "object";
                make_bucket_on_all(&disk_stores, bucket).await;
                let (upload_id, parts) =
                    stage_upload_with_create_opts(&set_disks, bucket, object, &[0x65; 4096], &ObjectOptions::default()).await;
                let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
                let completion_barrier =
                    MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::BeforeObjectPublication);

                let complete_store = set_disks.clone();
                let complete_upload_id = upload_id.clone();
                let complete = tokio::spawn(async move {
                    complete_store
                        .complete_multipart_upload(bucket, object, &complete_upload_id, parts, &ObjectOptions::default())
                        .await
                });
                completion_barrier.wait_until_paused().await;

                let before_namespace_barrier =
                    PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::BeforeNamespace);
                let after_namespace_barrier =
                    PutObjectCommitBarrier::install(bucket, object, PutObjectCommitPause::AfterNamespace);
                let put_store = set_disks.clone();
                let put = tokio::spawn(async move {
                    let mut reader = PutObjReader::from_vec(vec![0x66; 4096]);
                    put_store
                        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                        .await
                });
                before_namespace_barrier.wait_until_paused().await;
                before_namespace_barrier.release_and_wait_until_namespace_pending().await;

                completion_barrier.release();
                after_namespace_barrier.wait_until_paused().await;
                after_namespace_barrier.release();
                complete
                    .await
                    .expect("completion task should not panic")
                    .expect("completion should commit after publication fence");
                put.await
                    .expect("same-key PUT task should not panic")
                    .expect("same-key PUT should commit after completion publishes and releases the object lock");
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn complete_validates_parts_after_an_inflight_upload_part_commit() {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let signaling = Arc::new(SignalingLockClient::new(Arc::new(LocalClient::with_manager(manager))));
        let lockers: Vec<Arc<dyn LockClient>> = vec![signaling.clone()];
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "multipart-complete-put-part-race-bucket";
        let object = "object";
        make_bucket_on_all(&disk_stores, bucket).await;
        let (upload_id, original_parts) =
            stage_upload_with_create_opts(&set_disks, bucket, object, &[0x49; 4096], &ObjectOptions::default()).await;
        let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
        signaling.set_target(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path));
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::PutPartBeforeLockLost);

        let put_store = set_disks.clone();
        let put_upload_id = upload_id.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x4a; 4096]);
            put_store
                .put_object_part(bucket, object, &put_upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;

        let complete_store = set_disks.clone();
        let complete_upload_id = upload_id.clone();
        let complete = tokio::spawn(async move {
            complete_store
                .complete_multipart_upload(bucket, object, &complete_upload_id, original_parts, &ObjectOptions::default())
                .await
        });
        signaling.wait_for_attempts(2).await;
        tokio::task::yield_now().await;
        assert!(!complete.is_finished(), "completion must wait for the UploadPart commit lock");

        barrier.release();
        put.await
            .expect("UploadPart task should not panic")
            .expect("UploadPart replacement should commit");
        let err = complete
            .await
            .expect("completion task should not panic")
            .expect_err("completion must reject the stale ETag after UploadPart wins");
        assert!(matches!(err, StorageError::InvalidPart(..)));
        set_disks
            .list_object_parts(bucket, object, &upload_id, None, MAX_PARTS_COUNT, &ObjectOptions::default())
            .await
            .expect("failed completion must leave the upload retryable");
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn complete_fences_upload_lock_loss_before_commit() {
        temp_env::async_with_vars([(crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true"))], async {
            let target = Arc::new(std::sync::RwLock::new(None));
            let refresh_calls = Arc::new(AtomicUsize::new(0));
            let lockers: Vec<Arc<dyn LockClient>> = (0..4)
                .map(|_| {
                    Arc::new(SelectiveLockLossClient::new(Arc::clone(&target), Arc::clone(&refresh_calls))) as Arc<dyn LockClient>
                })
                .collect();
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
            let bucket = "multipart-layout-lock-loss-bucket";
            let object = "object";
            make_bucket_on_all(&disk_stores, bucket).await;
            let create_opts = ObjectOptions {
                user_defined: HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]),
                ..Default::default()
            };
            let (upload_id, parts) = stage_upload_with_create_opts(&set_disks, bucket, object, &[0x44; 4096], &create_opts).await;
            let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, &upload_id);
            *target.write().expect("lock-loss target should be writable") =
                Some(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, upload_id_path.clone()));
            let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
            let barrier = MultipartCommitBarrier::install(bucket, object, MultipartCommitPause::BeforeLockLost);

            let complete_store = set_disks.clone();
            let complete_upload_id = upload_id.clone();
            let complete = tokio::spawn(async move {
                complete_store
                    .complete_multipart_upload(bucket, object, &complete_upload_id, parts, &ObjectOptions::default())
                    .await
            });
            barrier.wait_until_paused().await;
            tokio::time::advance(Duration::from_secs(11)).await;
            tokio::task::yield_now().await;
            assert!(
                refresh_calls.load(Ordering::Acquire) > 0,
                "upload lock heartbeat should reach the test client"
            );
            barrier.release();

            let err = complete
                .await
                .expect("completion task should not panic")
                .expect_err("completion must fail after losing the upload lock");
            match err {
                StorageError::NamespaceLockQuorumUnavailable {
                    bucket: lock_bucket,
                    object: lock_object,
                    ..
                } => {
                    assert_eq!(lock_bucket, RUSTFS_META_MULTIPART_BUCKET);
                    assert_eq!(lock_object, upload_id_path);
                }
                other => panic!("unexpected lock-loss error: {other:?}"),
            }
            set_disks
                .check_upload_id_exists(bucket, object, &upload_id, true)
                .await
                .expect("lock loss before commit must leave the staged upload retryable");
        })
        .await;
    }

    #[tokio::test]
    async fn complete_encrypted_multipart_reuses_precondition_object_lock() {
        temp_env::async_with_vars(
            [
                (crate::object_api::ENV_RUSTFS_ENCRYPTED_RANGE_SEEK, Some("true")),
                (rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1")),
            ],
            async {
                let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
                let bucket = "multipart-layout-precondition-bucket";
                let object = "object";
                make_bucket_on_all(&disk_stores, bucket).await;
                let create_opts = ObjectOptions {
                    user_defined: HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]),
                    ..Default::default()
                };
                let (upload_id, parts) =
                    stage_upload_with_create_opts(&set_disks, bucket, object, &[0x41; 4096], &create_opts).await;

                let completed = set_disks
                    .clone()
                    .complete_multipart_upload(
                        bucket,
                        object,
                        &upload_id,
                        parts,
                        &ObjectOptions {
                            http_preconditions: Some(HTTPPreconditions {
                                if_none_match: Some("*".to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("successful precondition should reuse the held object lock");
                let layout_token = completed.data_dir.expect("completed data dir should exist").to_string();
                assert!(crate::object_api::has_encrypted_part_layout_marker(
                    &completed.user_defined,
                    crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
                    &layout_token,
                ));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn second_pool_multipart_uses_its_own_layout_and_round_trips() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_for_pool_with_default_parity(2, 1, 2).await;
        set_disks.set_test_storage_class_config(
            lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("heterogeneous pool storage class should resolve"),
        );

        let bucket = "multipart-second-pool-bucket";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("second-pool multipart upload should be created");
        let (upload_info, _) = set_disks
            .check_upload_id_exists(bucket, object, &upload.upload_id, false)
            .await
            .expect("stored multipart layout should be readable");
        assert_eq!(upload_info.erasure.data_blocks, 1);
        assert_eq!(upload_info.erasure.parity_blocks, 1);

        let payload = vec![0x5a; 4096];
        let mut reader = PutObjReader::from_vec(payload.clone());
        let part = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
            .await
            .expect("second-pool part should encode without zero data shards");
        set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("second-pool multipart upload should complete");

        let mut object_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("completed second-pool object should be readable");
        let mut restored = Vec::new();
        object_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("completed second-pool object should stream");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    async fn list_multipart_uploads_caps_each_page_at_max_uploads_and_paginates_cleanly() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-list-cap-bucket";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        // Start more in-progress uploads on the same object than a single page holds.
        // Track only the decoded `<uuid>x<timestamp>` suffixes: the full upload id
        // embeds the process-global deployment id, which a concurrently running
        // test can swap between create and list time.
        let total = 5usize;
        let mut created = HashSet::new();
        for _ in 0..total {
            let res = set_disks
                .new_multipart_upload(bucket, object, &ObjectOptions::default())
                .await
                .expect("multipart upload should be created");
            assert!(
                created.insert(runtime_sources::upload_uuid_suffix(&res.upload_id)),
                "each upload id must be unique"
            );
        }

        // A single page must never return more than max_uploads entries.
        let max_uploads = 2usize;
        let page = set_disks
            .list_multipart_uploads_for_incarnation(bucket, object, None, None, None, max_uploads, None)
            .await
            .expect("list should succeed");
        assert_eq!(
            page.uploads.len(),
            max_uploads,
            "page must return exactly max_uploads uploads, not max_uploads + 1"
        );
        assert!(page.is_truncated, "uploads remain, so the page must be truncated");
        assert_eq!(
            page.next_upload_id_marker.as_ref(),
            page.uploads.last().map(|u| &u.upload_id),
            "next marker must point at the last returned upload, not one past it"
        );

        // Exact boundary: max_uploads == total must not falsely report truncation.
        let exact = set_disks
            .list_multipart_uploads_for_incarnation(bucket, object, None, None, None, total, None)
            .await
            .expect("list should succeed");
        assert_eq!(exact.uploads.len(), total, "exact boundary must return every upload");
        assert!(!exact.is_truncated, "exact boundary must not be marked truncated");
        assert!(
            exact.next_upload_id_marker.is_none(),
            "a non-truncated listing must not carry a next marker"
        );

        // Paginating one upload at a time must enumerate every upload exactly once,
        // with no gaps and no duplicates, and terminate.
        let mut seen = HashSet::new();
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        let mut pages = 0usize;
        loop {
            let page = set_disks
                .list_multipart_uploads_for_incarnation(
                    bucket,
                    object,
                    key_marker.clone(),
                    upload_id_marker.clone(),
                    None,
                    1,
                    None,
                )
                .await
                .expect("list should succeed");
            assert!(page.uploads.len() <= 1, "max_uploads=1 must never return more than one upload");
            for upload in &page.uploads {
                assert!(
                    seen.insert(runtime_sources::upload_uuid_suffix(&upload.upload_id)),
                    "upload {} was returned more than once across pages",
                    upload.upload_id
                );
            }
            pages += 1;
            assert!(pages <= total + 1, "pagination failed to terminate");
            if !page.is_truncated {
                break;
            }
            key_marker = page.next_key_marker.clone();
            upload_id_marker = page.next_upload_id_marker.clone();
            assert!(key_marker.is_some(), "a truncated page must carry a next key marker");
            assert!(upload_id_marker.is_some(), "a truncated upload page must carry a next upload marker");
        }
        assert_eq!(
            seen, created,
            "single-upload pagination must enumerate every upload with no loss or duplication"
        );
    }

    #[tokio::test]
    async fn list_multipart_uploads_enumerates_bucket_and_prefix() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-prefix-list-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        // Compare only the decoded `<uuid>x<timestamp>` suffixes: the full
        // upload id embeds the process-global deployment id, which a
        // concurrently running test can swap between create and list time.
        let mut expected = Vec::new();
        for object in ["logs/a.bin", "logs/a.bin", "logs/b.bin", "other/c.bin"] {
            let upload = set_disks
                .new_multipart_upload(bucket, object, &ObjectOptions::default())
                .await
                .expect("multipart upload should be created");
            expected.push((object.to_string(), runtime_sources::upload_uuid_suffix(&upload.upload_id)));
        }
        expected.sort();

        let all = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "", None, None, None, 1000, None)
            .await
            .expect("bucket-wide multipart listing should succeed");
        let mut listed = all
            .uploads
            .iter()
            .map(|upload| (upload.object.clone(), runtime_sources::upload_uuid_suffix(&upload.upload_id)))
            .collect::<Vec<_>>();
        listed.sort();
        assert_eq!(listed, expected);
        assert!(!all.is_truncated);

        let logs = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "logs/", None, None, None, 1000, None)
            .await
            .expect("prefix multipart listing should succeed");
        assert_eq!(logs.uploads.len(), 3);
        assert!(logs.uploads.iter().all(|upload| upload.object.starts_with("logs/")));

        let exact = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "logs/a.bin", None, None, None, 1000, None)
            .await
            .expect("exact-key multipart listing should remain supported");
        assert_eq!(exact.uploads.len(), 2);
        assert!(exact.uploads.iter().all(|upload| upload.object == "logs/a.bin"));
    }

    #[tokio::test]
    async fn list_multipart_uploads_paginates_across_same_key_boundary() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-prefix-page-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut expected = Vec::new();
        for object in ["logs/a.bin", "logs/a.bin", "logs/b.bin"] {
            let upload = set_disks
                .new_multipart_upload(bucket, object, &ObjectOptions::default())
                .await
                .expect("multipart upload should be created");
            expected.push((object.to_string(), upload.upload_id));
        }
        expected.sort();

        let mut key_marker = None;
        let mut upload_id_marker = None;
        let mut listed = Vec::new();
        for _ in 0..expected.len() {
            let page = set_disks
                .list_multipart_uploads_for_incarnation(
                    bucket,
                    "logs/",
                    key_marker.clone(),
                    upload_id_marker.clone(),
                    None,
                    1,
                    None,
                )
                .await
                .expect("multipart page should succeed");
            assert_eq!(page.uploads.len(), 1);
            listed.push((page.uploads[0].object.clone(), page.uploads[0].upload_id.clone()));
            if !page.is_truncated {
                break;
            }
            key_marker = page.next_key_marker;
            upload_id_marker = page.next_upload_id_marker;
            assert!(key_marker.is_some());
            assert!(upload_id_marker.is_some());
        }

        // Compare only the decoded `<uuid>x<timestamp>` suffixes: the full
        // upload id embeds the process-global deployment id, which a
        // concurrently running test can swap between create and list time.
        let normalize = |uploads: &[(String, String)]| {
            let mut normalized = uploads
                .iter()
                .map(|(object, upload_id)| (object.clone(), runtime_sources::upload_uuid_suffix(upload_id)))
                .collect::<Vec<_>>();
            normalized.sort();
            normalized
        };
        assert_eq!(normalize(&listed), normalize(&expected));

        let key_only = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "logs/", Some("logs/a.bin".to_string()), None, None, 1000, None)
            .await
            .expect("key-only marker should succeed");
        assert_eq!(
            key_only
                .uploads
                .iter()
                .map(|upload| upload.object.as_str())
                .collect::<Vec<_>>(),
            vec!["logs/b.bin"]
        );

        let upload_only = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "logs/", None, Some(expected[0].1.clone()), None, 1000, None)
            .await
            .expect("an upload marker without a key marker should be ignored");
        assert_eq!(upload_only.uploads.len(), expected.len());
    }

    #[tokio::test]
    async fn list_multipart_uploads_folds_delimiter_and_paginates_prefixes() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-delimiter-list-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        for object in [
            "logs/2025",
            "logs/2025/a.bin",
            "logs/2026/b.bin",
            "logs/root.bin",
            "other/c.bin",
        ] {
            set_disks
                .new_multipart_upload(bucket, object, &ObjectOptions::default())
                .await
                .expect("multipart upload should be created");
        }

        let first = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "logs/", None, None, Some("/".to_string()), 2, None)
            .await
            .expect("delimiter multipart listing should succeed");
        assert_eq!(first.uploads.len(), 1);
        assert_eq!(first.uploads[0].object, "logs/2025");
        assert_eq!(first.common_prefixes, vec!["logs/2025/".to_string()]);
        assert!(first.is_truncated);
        assert_eq!(first.next_key_marker.as_deref(), Some("logs/2025/"));
        assert!(first.next_upload_id_marker.is_none());

        let second = set_disks
            .list_multipart_uploads_for_incarnation(
                bucket,
                "logs/",
                first.next_key_marker,
                first.next_upload_id_marker,
                Some("/".to_string()),
                2,
                None,
            )
            .await
            .expect("delimiter continuation should succeed");
        assert_eq!(second.uploads.len(), 1);
        assert_eq!(second.uploads[0].object, "logs/root.bin");
        assert_eq!(second.common_prefixes, vec!["logs/2026/".to_string()]);
        assert!(!second.is_truncated);

        let exact_boundary = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "logs/", None, None, Some("/".to_string()), 4, None)
            .await
            .expect("delimiter exact boundary should succeed");
        assert_eq!(exact_boundary.uploads.len(), 2);
        assert_eq!(exact_boundary.common_prefixes.len(), 2);
        assert!(!exact_boundary.is_truncated);
    }

    #[tokio::test]
    async fn list_multipart_uploads_hides_uploads_from_another_incarnation() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-incarnation-list-bucket";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let incarnation = Uuid::new_v4();
        let (fence, _loss_handle) = NamespaceLockFence::loss_handle_for_test();
        let current = set_disks
            .new_multipart_upload(
                bucket,
                object,
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    bucket_lifecycle_lock_fence: Some(fence),
                    ..Default::default()
                },
            )
            .await
            .expect("current multipart upload should be created");
        set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("legacy multipart upload should be created");

        let unscoped = set_disks
            .list_multipart_uploads_for_incarnation(bucket, object, None, None, None, 1000, None)
            .await
            .expect("unscoped multipart listing should succeed");
        assert_eq!(unscoped.uploads.len(), 2);

        let scoped = set_disks
            .list_multipart_uploads_for_incarnation(bucket, object, None, None, None, 1000, Some(incarnation))
            .await
            .expect("incarnation-scoped multipart listing should succeed");
        assert_eq!(scoped.uploads.len(), 1);
        // Compare only the decoded `<uuid>x<timestamp>` suffixes: the full
        // upload id embeds the process-global deployment id, which a
        // concurrently running test can swap between create and list time.
        assert_eq!(
            runtime_sources::upload_uuid_suffix(&scoped.uploads[0].upload_id),
            runtime_sources::upload_uuid_suffix(&current.upload_id)
        );
    }

    /// The `<deployment-id>.` prefix inside an upload id is read from a
    /// process-global that concurrently-running tests reinitialize, so id
    /// assertions compare only the stable `<uuid>x<timestamp>` suffix.
    fn upload_uuid_suffix(upload_id: &str) -> String {
        let decoded = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(upload_id.as_bytes())
            .expect("upload id should be url-safe base64");
        let decoded = String::from_utf8(decoded).expect("upload id should decode to utf8");
        decoded
            .split_once('.')
            .map(|(_, suffix)| suffix.to_owned())
            .unwrap_or(decoded)
    }

    /// Regression (issue #5716): the multipart staging namespace is flat —
    /// `sha256(bucket/object)` directories from every bucket share one volume —
    /// so a bucket-scoped listing reads metadata belonging to other buckets'
    /// in-flight uploads. Those entries must be filtered out, not treated as
    /// corruption: with the `_ => CorruptedFormat` arm, any concurrent upload
    /// in another bucket turned every ListMultipartUploads for this bucket
    /// into an InternalError.
    #[tokio::test]
    async fn list_multipart_uploads_ignores_other_buckets_uploads() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-cross-bucket-a";
        let other_bucket = "multipart-cross-bucket-b";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
            disk.make_volume(other_bucket)
                .await
                .expect("other bucket volume should be created");
        }

        let mine = set_disks
            .new_multipart_upload(bucket, "blobs/data/layer.bin", &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");
        set_disks
            .new_multipart_upload(other_bucket, "cache/other-layer.bin", &ObjectOptions::default())
            .await
            .expect("other bucket multipart upload should be created");

        let listed = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "blobs/", None, None, None, 1000, None)
            .await
            .expect("a concurrent upload in another bucket must not poison this bucket's listing");
        assert_eq!(listed.uploads.len(), 1);
        assert_eq!(upload_uuid_suffix(&listed.uploads[0].upload_id), upload_uuid_suffix(&mine.upload_id));

        let bucket_wide = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "", None, None, None, 1000, None)
            .await
            .expect("bucket-wide listing must also skip other buckets' uploads");
        assert_eq!(bucket_wide.uploads.len(), 1);
        assert_eq!(bucket_wide.uploads[0].object, "blobs/data/layer.bin");
    }

    /// Regression (issue #5716): a single upload directory whose `xl.meta` was
    /// destroyed (crash mid-write, torn disk state) must degrade to that upload
    /// alone. Failing the whole ListMultipartUploads turns one piece of stale
    /// debris into a permanent outage for every multipart client of the bucket.
    #[tokio::test]
    async fn list_multipart_uploads_skips_undecodable_upload_dirs() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-corrupt-dir-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let healthy = set_disks
            .new_multipart_upload(bucket, "healthy/object.bin", &ObjectOptions::default())
            .await
            .expect("healthy multipart upload should be created");
        set_disks
            .new_multipart_upload(bucket, "debris/object.bin", &ObjectOptions::default())
            .await
            .expect("debris multipart upload should be created");

        // Destroy the debris upload's metadata on every disk, as an unclean
        // shutdown mid-create can. The directory stays listable while its
        // xl.meta no longer decodes.
        let debris_sha = SetDisks::get_multipart_sha_dir(bucket, "debris/object.bin");
        let mut corrupted = 0usize;
        for temp_dir in &temp_dirs {
            let sha_dir = temp_dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&debris_sha);
            for meta in multipart_meta_files_on_disk(temp_dir, "xl.meta").await {
                if meta.starts_with(sha_dir.to_string_lossy().as_ref()) {
                    tokio::fs::write(&meta, b"not an xl.meta")
                        .await
                        .expect("corrupting xl.meta should succeed");
                    corrupted += 1;
                }
            }
        }
        assert!(corrupted > 0, "the debris upload must exist on disk before corruption");

        let listed = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "healthy/", None, None, None, 1000, None)
            .await
            .expect("one undecodable upload dir must not fail the whole listing");
        assert_eq!(listed.uploads.len(), 1);
        assert_eq!(upload_uuid_suffix(&listed.uploads[0].upload_id), upload_uuid_suffix(&healthy.upload_id));

        let bucket_wide = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "", None, None, None, 1000, None)
            .await
            .expect("bucket-wide listing must skip the undecodable upload dir");
        assert_eq!(bucket_wide.uploads.len(), 1);
        assert_eq!(
            upload_uuid_suffix(&bucket_wide.uploads[0].upload_id),
            upload_uuid_suffix(&healthy.upload_id)
        );
    }

    /// Companion boundary to `list_multipart_uploads_skips_undecodable_upload_dirs`:
    /// corruption BELOW the discovery quorum must not hide the upload — the
    /// surviving disks still identify it, so it stays listed.
    #[tokio::test]
    async fn list_multipart_uploads_survives_sub_quorum_corruption() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-partial-corrupt-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let upload = set_disks
            .new_multipart_upload(bucket, "partial/object.bin", &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");

        // Corrupt the upload's metadata on exactly one disk (discovery quorum
        // on this 4-disk set is 2): the other replicas keep it identifiable.
        let sha = SetDisks::get_multipart_sha_dir(bucket, "partial/object.bin");
        let mut corrupted = 0usize;
        for temp_dir in &temp_dirs {
            let sha_dir = temp_dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&sha);
            for meta in multipart_meta_files_on_disk(temp_dir, "xl.meta").await {
                if meta.starts_with(sha_dir.to_string_lossy().as_ref()) {
                    tokio::fs::write(&meta, b"not an xl.meta")
                        .await
                        .expect("corrupting xl.meta should succeed");
                    corrupted += 1;
                }
            }
            if corrupted > 0 {
                break;
            }
        }
        assert_eq!(corrupted, 1, "exactly one disk's metadata should be corrupted");

        let listed = set_disks
            .list_multipart_uploads_for_incarnation(bucket, "partial/", None, None, None, 1000, None)
            .await
            .expect("sub-quorum corruption must not fail the listing");
        assert_eq!(listed.uploads.len(), 1);
        assert_eq!(upload_uuid_suffix(&listed.uploads[0].upload_id), upload_uuid_suffix(&upload.upload_id));
    }

    /// Pins the degrade-vs-propagate classification (issue #5716 review): only
    /// affirmative corruption may skip an upload during listing; every
    /// transient fault shape must keep failing the listing so clients retry
    /// instead of silently losing entries.
    #[test]
    fn corrupt_upload_metadata_classification_excludes_transient_faults() {
        for corrupt in [
            DiskError::FileCorrupt,
            DiskError::CorruptedFormat,
            DiskError::CorruptedBackend,
            DiskError::OutdatedXLMeta,
        ] {
            assert!(is_corrupt_upload_metadata_error(&corrupt), "{corrupt:?} is deterministic damage");
        }
        for transient in [
            DiskError::DiskNotFound,
            DiskError::Timeout,
            DiskError::FaultyDisk,
            DiskError::FaultyRemoteDisk,
            DiskError::DiskAccessDenied,
            DiskError::VolumeAccessDenied,
            DiskError::ErasureReadQuorum,
            DiskError::FileNotFound,
            DiskError::Io(std::io::Error::other("connection refused")),
        ] {
            assert!(
                !is_corrupt_upload_metadata_error(&transient),
                "{transient:?} must keep failing the listing"
            );
        }
    }

    /// Recursively collect every file named `file_name` under the multipart
    /// staging bucket on a single disk. Used to observe whether a failed commit
    /// left the per-part metadata intact for a retry.
    async fn multipart_meta_files_on_disk(temp_dir: &TempDir, file_name: &str) -> Vec<String> {
        let mut found = Vec::new();
        let mut stack = vec![temp_dir.path().join(RUSTFS_META_MULTIPART_BUCKET)];
        while let Some(dir) = stack.pop() {
            let mut read_dir = match tokio::fs::read_dir(&dir).await {
                Ok(read_dir) => read_dir,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => panic!("multipart dir {dir:?} should be listable: {err}"),
            };
            while let Some(entry) = read_dir.next_entry().await.expect("multipart dir entry should be readable") {
                let file_type = entry.file_type().await.expect("dir entry file type should be readable");
                if file_type.is_dir() {
                    stack.push(entry.path());
                } else if entry.file_name().to_string_lossy() == file_name {
                    found.push(entry.path().display().to_string());
                }
            }
        }
        found
    }

    /// Total number of `part.1.meta` files across every disk's multipart staging area.
    async fn total_part1_meta(temp_dirs: &[TempDir]) -> usize {
        let mut n = 0;
        for dir in temp_dirs {
            n += multipart_meta_files_on_disk(dir, "part.1.meta").await.len();
        }
        n
    }

    /// backlog#946: `complete_multipart_upload` must clean up the stale
    /// `part.N.meta` files only *after* the authoritative `rename_data` commit
    /// succeeds. If the commit fails write quorum, the upload directory has to
    /// keep its `part.N.meta` so a retried CompleteMultipartUpload can still read
    /// the parts; deleting them beforehand permanently strands the upload.
    ///
    /// The commit is forced to fail *at rename_data* (not earlier): the multipart
    /// staging dir stays readable on every disk so `check_upload_id_exists` and
    /// `read_parts` pass write/read quorum, while the destination bucket
    /// directory is made unwritable on *every* disk so `rename_data` fails the
    /// commit on all disks (never moving the staging data) and returns below
    /// write quorum. That reproduces the issue's real failure mode, where the
    /// staging `part.N.meta` would still be present if not deleted prematurely.
    #[cfg(unix)]
    #[tokio::test]
    async fn complete_multipart_keeps_part_meta_when_commit_fails_for_retry() {
        use std::os::unix::fs::PermissionsExt;

        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "multipart-commit-retry-bucket";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let upload = set_disks
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");

        // Upload one full, valid part while every disk is online. A single part
        // is the last part, so the minimum-part-size gate does not apply and a
        // small payload keeps the test fast.
        let part_size = 4096usize;
        let mut reader = PutObjReader::new(
            HashReader::from_stream(Cursor::new(vec![9u8; part_size]), part_size as i64, part_size as i64, None, None, false)
                .expect("hash reader should be constructed"),
        );
        let part_info = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
            .await
            .expect("uploading the part should succeed");

        // Every disk must hold part.1.meta in the staging area now.
        let before = total_part1_meta(&temp_dirs).await;
        assert_eq!(before, temp_dirs.len(), "the uploaded part must write part.1.meta to every disk");

        // Lock the destination bucket directory on *all* disks so the commit's
        // rename into `bucket/object` fails everywhere: rename_data cannot reach
        // write quorum and no disk moves the readable staging metadata.
        let locked_buckets: Vec<std::path::PathBuf> = temp_dirs.iter().map(|d| d.path().join(bucket)).collect();
        for path in &locked_buckets {
            let mut perms = std::fs::metadata(path).expect("bucket dir metadata").permissions();
            perms.set_mode(0o555);
            std::fs::set_permissions(path, perms).expect("bucket dir should be made read-only");
        }

        let completed = set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: part_info.part_num,
                    etag: part_info.etag.clone(),
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await;

        // Restore write permission so the temp dirs can be cleaned up and the
        // staging metadata re-read below.
        for path in &locked_buckets {
            let mut perms = std::fs::metadata(path).expect("bucket dir metadata").permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(path, perms).expect("bucket dir should be made writable again");
        }

        assert!(
            completed.is_err(),
            "commit must fail once the destination is unwritable on every disk, got {completed:?}"
        );

        // Regression assertion: the failed commit must NOT have deleted the
        // staging part.1.meta. Old code cleaned up before rename_data and would
        // leave zero here, permanently breaking a retried CompleteMultipartUpload.
        let after = total_part1_meta(&temp_dirs).await;
        assert_eq!(
            after,
            temp_dirs.len(),
            "part.1.meta must survive a failed commit so CompleteMultipartUpload stays retryable"
        );
    }

    #[test]
    fn reduce_quorum_part_numbers_only_keeps_parts_present_on_quorum_of_drives() {
        let object_parts = vec![
            vec![
                "part.1".to_string(),
                "part.1.meta".to_string(),
                "part.2".to_string(),
                "part.2.meta".to_string(),
            ],
            vec![
                "part.1".to_string(),
                "part.1.meta".to_string(),
                "part.3".to_string(),
                "part.3.meta".to_string(),
            ],
            vec![
                "part.1".to_string(),
                "part.1.meta".to_string(),
                "part.2".to_string(),
                "part.2.meta".to_string(),
            ],
        ];

        let parts = reduce_quorum_part_numbers(object_parts, 2);
        assert_eq!(parts, vec![1, 2]);
    }

    fn test_multipart_fileinfo(object: &str, data_blocks: usize, parity_blocks: usize, index: usize) -> FileInfo {
        let mut file_info = FileInfo::new(object, data_blocks, parity_blocks);
        file_info.erasure.index = index;
        file_info.data_dir = Some(Uuid::new_v4());
        file_info
    }

    #[test]
    fn upload_id_write_quorum_fails_when_only_read_quorum_metadata_is_visible() {
        let parts_metadata = vec![
            test_multipart_fileinfo("bucket/object", 2, 2, 1),
            test_multipart_fileinfo("bucket/object", 2, 2, 2),
            FileInfo::default(),
            FileInfo::default(),
        ];
        let errs = vec![None, None, Some(DiskError::DiskNotFound), Some(DiskError::DiskNotFound)];

        let (read_quorum, write_quorum) =
            SetDisks::object_quorum_from_meta(&parts_metadata, &errs, 2).expect("read quorum should resolve metadata geometry");

        assert_eq!(read_quorum, 2);
        assert_eq!(write_quorum, 3);
        assert!(reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum as usize).is_none());

        let err = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum as usize)
            .expect("write quorum should fail with only two writable metadata copies");

        assert_eq!(err, DiskError::ErasureWriteQuorum);
    }

    #[test]
    fn upload_id_all_not_found_maps_to_invalid_upload_id() {
        let parts_metadata = vec![
            FileInfo::default(),
            FileInfo::default(),
            FileInfo::default(),
            FileInfo::default(),
        ];
        let errs = vec![
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
        ];

        let err = SetDisks::object_quorum_from_meta(&parts_metadata, &errs, 2)
            .map(|_| ())
            .map_err(|err| map_upload_id_metadata_error("bucket", "object", "upload-id", err))
            .expect_err("all missing upload metadata should remain an invalid upload id");

        assert!(matches!(err, StorageError::InvalidUploadID(bucket, object, upload_id)
            if bucket == "bucket" && object == "object" && upload_id == "upload-id"));
    }

    fn test_multipart_upload(object: &str, upload_id: &str) -> MultipartInfo {
        MultipartInfo {
            bucket: "bucket".to_string(),
            object: object.to_string(),
            upload_id: upload_id.to_string(),
            initiated: None,
            ..Default::default()
        }
    }

    #[test]
    fn paginate_upload_page_caps_at_max_uploads_and_probes_truncation() {
        // max_uploads = N (3) with N + 1 (4) uploads must return exactly N,
        // report truncation, and expose the last returned id as the next marker.
        let uploads: Vec<MultipartInfo> = (0..4).map(|i| test_multipart_upload("obj", &format!("u{i}"))).collect();

        let (page, is_truncated, next_upload_id_marker) = paginate_upload_page(&uploads, 3);

        assert_eq!(page.len(), 3);
        assert!(is_truncated);
        assert_eq!(next_upload_id_marker.as_deref(), Some("u2"));
        // The overflow element `u3` is only a truncation probe and must not leak
        // into the returned page.
        assert!(page.iter().all(|upload| upload.upload_id != "u3"));
    }

    #[test]
    fn paginate_upload_page_reports_complete_when_within_cap() {
        let uploads: Vec<MultipartInfo> = (0..3).map(|i| test_multipart_upload("obj", &format!("u{i}"))).collect();

        let (page, is_truncated, next_upload_id_marker) = paginate_upload_page(&uploads, 3);

        assert_eq!(page.len(), 3);
        assert!(!is_truncated);
        assert!(next_upload_id_marker.is_none());
    }

    #[test]
    fn paginate_upload_page_marker_resumes_without_loss() {
        let uploads: Vec<MultipartInfo> = (0..4).map(|i| test_multipart_upload("obj", &format!("u{i}"))).collect();

        // First page stops at the cap and hands back `u2` as the resume marker.
        let (first, first_truncated, first_marker) = paginate_upload_page(&uploads, 3);
        assert!(first_truncated);
        assert_eq!(first_marker.as_deref(), Some("u2"));

        // The caller applies the marker offset (drops through `u2`) and pages the
        // remainder; the final upload must appear exactly once.
        let resume_offset = uploads
            .iter()
            .position(|u| u.upload_id == "u2")
            .expect("marker must be present")
            + 1;
        let (second, second_truncated, _) = paginate_upload_page(&uploads[resume_offset..], 3);
        assert!(!second_truncated);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].upload_id, "u3");

        let paged: Vec<&str> = first.iter().chain(second.iter()).map(|u| u.upload_id.as_str()).collect();
        assert_eq!(paged, vec!["u0", "u1", "u2", "u3"]);
    }

    /// Crash-consistency for the two `complete_multipart_upload` commit windows.
    ///
    /// rustfs/backlog#864: a fault that interrupts a completion must never mutate
    /// an already-committed object; the object must read back as the whole old
    /// version or the whole new version, never a torn mix, and the upload must
    /// stay recoverable.
    ///
    /// [`CrashPoint::MultipartBeforeCommitRename`] fires after the upload is fully
    /// staged and locked but before the authoritative `rename_data` commit: a
    /// prior committed version survives byte-for-byte and the upload is retryable.
    /// [`CrashPoint::MultipartAfterCommitBeforePartsCleanup`] fires after the
    /// commit lands but before the stale `part.N.meta` cleanup: the new version is
    /// readable and the un-reclaimed staging is harmless and reclaimable.
    mod crash_consistency {
        use super::*;
        use crate::crash_inject::{self, CrashPoint};
        use crate::storage_api_contracts::object::ObjectIO as _;
        use http::HeaderMap;
        use tokio::io::AsyncReadExt as _;

        /// 1 MiB keeps every object off the 128 KiB inline fast path, so the
        /// commit moves real erasure shards through `rename_data`.
        const PART_SIZE: usize = 1 << 20;

        fn payload(fill: u8) -> Vec<u8> {
            vec![fill; PART_SIZE]
        }

        async fn stage_upload(
            set_disks: &Arc<SetDisks>,
            bucket: &str,
            object: &str,
            content: &[u8],
        ) -> (String, Vec<CompletePart>) {
            stage_upload_with_create_opts(set_disks, bucket, object, content, &ObjectOptions::default()).await
        }

        async fn rewrite_staged_upload_version_id(
            set_disks: &Arc<SetDisks>,
            bucket: &str,
            object: &str,
            upload_id: &str,
            version_id: Option<Uuid>,
        ) {
            let upload_id_path = SetDisks::get_upload_id_dir(bucket, object, upload_id);
            let disks = set_disks.disks.read().await.clone();
            let (fi, mut files_metas) = set_disks
                .check_upload_id_exists(bucket, object, upload_id, true)
                .await
                .expect("staged upload metadata should be readable");
            for meta in files_metas.iter_mut() {
                meta.version_id = version_id;
            }
            SetDisks::write_unique_file_info(
                &disks,
                bucket,
                RUSTFS_META_MULTIPART_BUCKET,
                &upload_id_path,
                &files_metas,
                fi.write_quorum(set_disks.default_write_quorum()),
            )
            .await
            .expect("staged upload metadata should be rewritten");
        }

        async fn complete(
            set_disks: &Arc<SetDisks>,
            bucket: &str,
            object: &str,
            upload_id: &str,
            parts: Vec<CompletePart>,
        ) -> Result<ObjectInfo> {
            set_disks
                .clone()
                .complete_multipart_upload(bucket, object, upload_id, parts, &ObjectOptions::default())
                .await
        }

        #[tokio::test]
        #[serial]
        async fn complete_multipart_upload_assigns_completion_version_id() {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "multipart-versioned-complete-bucket";
            let object = "object";
            make_bucket_on_all(&disk_stores, bucket).await;

            let (upload_id, parts) = stage_upload(&set_disks, bucket, object, b"versioned multipart body").await;
            let complete_opts = ObjectOptions {
                versioned: true,
                ..Default::default()
            };

            let completed = set_disks
                .clone()
                .complete_multipart_upload(bucket, object, &upload_id, parts, &complete_opts)
                .await
                .expect("versioned multipart completion should succeed");
            let version_id = completed.version_id.expect("versioned completion must return a version id");

            assert!(!version_id.is_nil(), "versioned completion must not use a null version id");

            let lookup_opts = ObjectOptions {
                versioned: true,
                version_id: Some(version_id.to_string()),
                ..Default::default()
            };
            let current = set_disks
                .get_object_info(bucket, object, &lookup_opts)
                .await
                .expect("completed version should be addressable by exact version id");

            assert_eq!(current.version_id, Some(version_id));
        }

        #[tokio::test]
        #[serial]
        async fn complete_multipart_upload_replaces_staged_nil_version_id() {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "multipart-nil-versioned-complete-bucket";
            let object = "object";
            make_bucket_on_all(&disk_stores, bucket).await;

            let (upload_id, parts) = stage_upload(&set_disks, bucket, object, b"versioned multipart body").await;
            rewrite_staged_upload_version_id(&set_disks, bucket, object, &upload_id, Some(Uuid::nil())).await;
            let complete_opts = ObjectOptions {
                versioned: true,
                ..Default::default()
            };

            let completed = set_disks
                .clone()
                .complete_multipart_upload(bucket, object, &upload_id, parts, &complete_opts)
                .await
                .expect("versioned multipart completion should succeed");
            let version_id = completed.version_id.expect("versioned completion must return a version id");

            assert!(!version_id.is_nil(), "versioned completion must replace a staged null version id");

            let lookup_opts = ObjectOptions {
                versioned: true,
                version_id: Some(version_id.to_string()),
                ..Default::default()
            };
            let current = set_disks
                .get_object_info(bucket, object, &lookup_opts)
                .await
                .expect("completed version should be addressable by exact version id");

            assert_eq!(current.version_id, Some(version_id));
        }

        #[tokio::test]
        #[serial]
        async fn complete_multipart_upload_suspended_clears_staged_version_id() {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "multipart-suspended-complete-bucket";
            let object = "object";
            make_bucket_on_all(&disk_stores, bucket).await;
            let create_opts = ObjectOptions {
                versioned: true,
                ..Default::default()
            };

            let (upload_id, parts) =
                stage_upload_with_create_opts(&set_disks, bucket, object, b"suspended multipart body", &create_opts).await;
            let (staged_fi, _) = set_disks
                .check_upload_id_exists(bucket, object, &upload_id, true)
                .await
                .expect("staged upload metadata should be readable");
            assert!(
                staged_fi.version_id.is_some_and(|version_id| !version_id.is_nil()),
                "enabled multipart create should stage a concrete version id"
            );
            let complete_opts = ObjectOptions {
                version_suspended: true,
                ..Default::default()
            };

            let completed = set_disks
                .clone()
                .complete_multipart_upload(bucket, object, &upload_id, parts, &complete_opts)
                .await
                .expect("suspended multipart completion should succeed");

            assert!(
                completed.version_id.is_some_and(|version_id| version_id.is_nil()),
                "suspended completion must publish a null-version object internally"
            );
        }

        /// Read the whole committed object back; returns `(body, etag)`. The full
        /// body read also proves the served length matches (never a torn/short body).
        async fn read_object(set_disks: &Arc<SetDisks>, bucket: &str, object: &str) -> (Vec<u8>, Option<String>) {
            let mut reader = set_disks
                .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("object reader should open");
            let etag = reader.object_info.etag.clone();
            let mut body = Vec::new();
            reader
                .stream
                .read_to_end(&mut body)
                .await
                .expect("object should stream fully");
            (body, etag)
        }

        async fn current_data_dir(disk: &DiskStore, bucket: &str, object: &str) -> Uuid {
            disk.read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("current object metadata should read")
                .data_dir
                .expect("test object should be stored out-of-line")
        }

        async fn data_dir_exists(disk: &DiskStore, bucket: &str, object: &str, data_dir: Uuid) -> bool {
            disk.read_all(bucket, &format!("{object}/{data_dir}/part.1")).await.is_ok()
        }

        async fn cleanup_receipt_exists(disk: &DiskStore, bucket: &str, object: &str, data_dir: Uuid) -> bool {
            disk.read_all(bucket, &old_data_cleanup_receipt_path(object, data_dir))
                .await
                .is_ok()
        }

        async fn upload_is_listed(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, upload_id: &str) -> bool {
            let page = set_disks
                .list_multipart_uploads_for_incarnation(bucket, object, None, None, None, 1000, None)
                .await
                .expect("listing multipart uploads should succeed");
            // Compare only the decoded `<uuid>x<timestamp>` suffixes: the full
            // upload id embeds the process-global deployment id, which a
            // concurrently running test can swap between create and list time.
            let expected_suffix = runtime_sources::upload_uuid_suffix(upload_id);
            page.uploads
                .iter()
                .any(|u| runtime_sources::upload_uuid_suffix(&u.upload_id) == expected_suffix)
        }

        #[tokio::test]
        #[serial]
        async fn pre_commit_crash_keeps_committed_old_version() {
            let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "multipart-crash-pre";
            let object = "crash-pre-object";
            make_bucket_on_all(&disk_stores, bucket).await;

            // Commit an OLD version through the multipart path.
            let old = payload(0xA1);
            let (u_old, parts_old) = stage_upload(&set_disks, bucket, object, &old).await;
            complete(&set_disks, bucket, object, &u_old, parts_old)
                .await
                .expect("the old version should commit");
            let (old_body, old_etag) = read_object(&set_disks, bucket, object).await;
            assert_eq!(old_body, old, "the old version must round-trip before the crash");

            // Stage a replacement upload with NEW content, then crash before commit.
            let new = payload(0xB2);
            let (u_new, parts_new) = stage_upload(&set_disks, bucket, object, &new).await;
            crash_inject::arm(CrashPoint::MultipartBeforeCommitRename, object);
            let crashed = complete(&set_disks, bucket, object, &u_new, parts_new.clone()).await;
            assert!(
                matches!(crashed, Err(StorageError::Unexpected)),
                "the armed pre-commit crash point must be the failure that surfaced, got {crashed:?}"
            );

            // rustfs/backlog#864: the committed old version is untouched, byte-for-byte.
            let (after_body, after_etag) = read_object(&set_disks, bucket, object).await;
            assert_eq!(after_body, old, "the interrupted completion must not mutate the committed old version");
            assert_eq!(after_etag, old_etag, "the old version's ETag must be unchanged");

            // The staged upload survived intact (part.N.meta present on quorum), so
            // a retried completion commits the new version cleanly (retryable).
            assert!(
                total_part1_meta(&temp_dirs).await > 0,
                "the un-committed upload must keep its staging parts for retry"
            );
            crash_inject::disarm(CrashPoint::MultipartBeforeCommitRename, object);
            complete(&set_disks, bucket, object, &u_new, parts_new)
                .await
                .expect("a retried completion must succeed");
            let (retry_body, retry_etag) = read_object(&set_disks, bucket, object).await;
            assert_eq!(retry_body, new, "the retried completion must publish the whole new version");
            assert_ne!(retry_etag, old_etag, "the new version must carry a distinct ETag");
        }

        #[tokio::test]
        #[serial]
        async fn post_commit_crash_publishes_new_version_and_leaves_reclaimable_upload() {
            let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "multipart-crash-post";
            let object = "crash-post-object";
            make_bucket_on_all(&disk_stores, bucket).await;

            // Stage an upload and crash AFTER the commit rename but BEFORE cleanup.
            let new = payload(0xC3);
            let (u_new, parts_new) = stage_upload(&set_disks, bucket, object, &new).await;
            let parts_retry = parts_new.clone();
            crash_inject::arm(CrashPoint::MultipartAfterCommitBeforePartsCleanup, object);
            let crashed = complete(&set_disks, bucket, object, &u_new, parts_new).await;
            assert!(
                matches!(crashed, Err(StorageError::Unexpected)),
                "the armed post-commit crash point must be the failure that surfaced, got {crashed:?}"
            );
            crash_inject::disarm(CrashPoint::MultipartAfterCommitBeforePartsCleanup, object);

            // The commit landed: the new version reads back whole and correct.
            let (body, _etag) = read_object(&set_disks, bucket, object).await;
            assert_eq!(body, new, "a post-commit crash must leave the whole new version readable");

            // The completion never reached its cleanup, so the upload's staging
            // directory is still listable — leftover that must be reclaimable.
            assert!(
                upload_is_listed(&set_disks, bucket, object, &u_new).await,
                "the post-commit crash must leave the upload listable for reclamation"
            );

            // A retried CompleteMultipartUpload is answered deterministically: the
            // commit rename already consumed the upload's metadata, so the retry
            // resolves to InvalidUploadID (NoSuchUpload to the S3 client, the
            // standard answer for a completed-then-retried upload) — never a torn
            // state, and the committed object is untouched by the retry.
            let retried = complete(&set_disks, bucket, object, &u_new, parts_retry).await;
            assert!(
                matches!(retried, Err(StorageError::InvalidUploadID(..))),
                "a retried complete after the commit landed must resolve to InvalidUploadID, got {retried:?}"
            );
            let (body_after_retry, _) = read_object(&set_disks, bucket, object).await;
            assert_eq!(body_after_retry, new, "the failed retry must not disturb the committed object");

            // Reclaim the leftover exactly as the production tail does: delete_all
            // on the upload path (abort_multipart_upload cannot — the upload's
            // metadata is gone, so it too answers InvalidUploadID).
            let upload_dir = SetDisks::get_upload_id_dir(bucket, object, &u_new);
            set_disks
                .delete_all(RUSTFS_META_MULTIPART_BUCKET, &upload_dir)
                .await
                .expect("sweeping the leftover upload staging should succeed");
            assert!(
                !upload_is_listed(&set_disks, bucket, object, &u_new).await,
                "reclaiming the upload must drop it from the listing"
            );
            let (body_after, _) = read_object(&set_disks, bucket, object).await;
            assert_eq!(body_after, new, "reclaiming the leftover upload must not disturb the committed object");
        }

        #[tokio::test]
        #[serial(storage_class_env)]
        async fn post_commit_crash_receipt_reclaims_old_data_after_restart() {
            let _proof = install_remote_version_state_fleet_proof_for_test("object-transaction-fencing-test");
            let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
            let bucket = "multipart-crash-old-data-receipt";
            let object = "crash-old-data-object";
            make_bucket_on_all(&disk_stores, bucket).await;

            temp_env::async_with_vars(
                [
                    (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_WRITE, Some("true")),
                    (rustfs_config::ENV_OBJECT_TRANSACTION_FENCING_FLEET_CONFIRMED, Some("true")),
                ],
                async {
                    let old = payload(0x51);
                    let (u_old, parts_old) = stage_upload(&set_disks, bucket, object, &old).await;
                    complete(&set_disks, bucket, object, &u_old, parts_old)
                        .await
                        .expect("the old version should commit");
                    let old_dir = current_data_dir(&disk_stores[0], bucket, object).await;

                    let new = payload(0x52);
                    let (u_new, parts_new) = stage_upload(&set_disks, bucket, object, &new).await;
                    crash_inject::arm(CrashPoint::MultipartAfterCommitBeforePartsCleanup, object);
                    let crashed = complete(&set_disks, bucket, object, &u_new, parts_new).await;
                    assert!(
                        matches!(crashed, Err(StorageError::Unexpected)),
                        "the post-commit crash point must surface as unexpected, got {crashed:?}"
                    );
                    crash_inject::disarm(CrashPoint::MultipartAfterCommitBeforePartsCleanup, object);

                    let (body, _) = read_object(&set_disks, bucket, object).await;
                    assert_eq!(body, new, "the committed replacement must remain readable after the crash");
                    for disk in &disk_stores {
                        assert!(
                            cleanup_receipt_exists(disk, bucket, object, old_dir).await,
                            "post-commit crash must leave a durable old-data cleanup receipt"
                        );
                        assert!(
                            data_dir_exists(disk, bucket, object, old_dir).await,
                            "post-commit crash must leave old data for restart reconciliation"
                        );
                    }

                    let restarted_endpoints = temp_dirs
                        .iter()
                        .enumerate()
                        .map(|(disk_idx, dir)| {
                            let mut endpoint = Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8"))
                                .expect("endpoint should parse");
                            endpoint.set_pool_index(0);
                            endpoint.set_set_index(0);
                            endpoint.set_disk_index(disk_idx);
                            endpoint
                        })
                        .collect::<Vec<_>>();
                    let mut reloaded = Vec::with_capacity(restarted_endpoints.len());
                    for endpoint in &restarted_endpoints {
                        reloaded.push(
                            new_disk(
                                endpoint,
                                &DiskOption {
                                    cleanup: false,
                                    health_check: false,
                                },
                            )
                            .await
                            .expect("disk should restart"),
                        );
                    }
                    let restarted_set = SetDisks::new_with_instance_ctx(
                        "restart-cleanup-receipt-test-owner".to_string(),
                        Arc::new(RwLock::new(reloaded.iter().cloned().map(Some).collect())),
                        4,
                        2,
                        0,
                        0,
                        restarted_endpoints,
                        set_disks.format.clone(),
                        Vec::new(),
                        Arc::new(crate::runtime::instance::InstanceContext::new()),
                    )
                    .await;
                    let removed = restarted_set
                        .reconcile_old_data_cleanup_receipts(bucket, object)
                        .await
                        .expect("restart receipt reconciliation should succeed");
                    assert_eq!(removed, 4, "restart reconciler should delete all receipt targets");
                    for disk in &reloaded {
                        assert!(
                            !data_dir_exists(disk, bucket, object, old_dir).await,
                            "restart reconciler must reclaim the old data dir"
                        );
                    }
                },
            )
            .await;
        }
    }

    #[test]
    fn resolve_complete_etag_prefers_preserved_source_etag() {
        // A replication-preserved ETag that no part combination can derive
        // (foreign-origin object) must win over the computed md5-of-parts.
        let foreign_etag = "11111111111111111111111111111111-7".to_string();
        let opts = ObjectOptions {
            preserve_etag: Some(foreign_etag.clone()),
            ..Default::default()
        };
        assert_eq!(resolve_complete_etag(&opts, &[]), foreign_etag);

        // Empty preserve value degrades to the next source.
        let opts_empty = ObjectOptions {
            preserve_etag: Some(String::new()),
            user_defined: std::collections::HashMap::from([("etag".to_string(), "override-etag".to_string())]),
            ..Default::default()
        };
        assert_eq!(resolve_complete_etag(&opts_empty, &[]), "override-etag");

        // Without either source the ETag is computed from the parts.
        let computed = resolve_complete_etag(&ObjectOptions::default(), &[]);
        assert_eq!(computed, get_complete_multipart_md5(&[]));
    }
}
