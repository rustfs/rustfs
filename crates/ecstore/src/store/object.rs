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

use super::*;
use crate::bucket::lifecycle::{
    bucket_lifecycle_ops::eval_action_from_lifecycle,
    get_expiry_configs,
    tier_delete_journal::{
        abort_prepared_tier_delete_journal_entry as abort_prepared_journal_entry_if_current, commit_tier_delete_journal_entry,
        enqueue_committed_tier_delete_journal_entry, persist_tier_delete_journal_entry,
        record_tier_delete_journal_backend_identity,
    },
    tier_sweeper::{
        Jentry, attach_tier_delete_source, transitioned_delete_journal_entry_for_source, transitioned_force_delete_journal_entry,
    },
};
use crate::bucket::metadata_sys::{
    acquire_bucket_metadata_transaction_read_lock_in, get_bucket_incarnation_id_in, get_cached_bucket_incarnation_id_in,
    get_object_lock_config_and_incarnation_from_disk_in,
};
use crate::bucket::object_lock::objectlock_sys::{
    check_object_lock_for_deletion_with_state, ensure_recursive_force_delete_allowed_for_state,
};
use crate::bucket::replication::{DeleteReplicationConfigSnapshot, ReplicationObjectBridge};
use crate::bucket::versioning::VersioningApi;
use crate::disk::OldCurrentSize;
use crate::object_api::{NamespaceLockFence, ObjectLockConfigSnapshot};
use crate::set_disk::{
    SetDisks, get_lock_acquire_timeout, get_object_lock_diag_slow_acquire_threshold, get_object_lock_diag_slow_hold_threshold,
    is_lock_optimization_enabled, is_object_lock_diag_enabled, same_distributed_lock_domain,
};
use crate::storage_api_contracts::{
    namespace::NamespaceLocking as _,
    object::{DeleteAccounting, ObjectIO as _, ObjectOperations as _},
};
use parking_lot::Mutex as ParkingMutex;
use rustfs_io_metrics::{
    record_object_lock_diag_acquire_duration, record_object_lock_diag_hold_duration, record_object_lock_diag_slow_acquire,
    record_object_lock_diag_slow_hold,
};
use std::{
    fmt,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, ReadBuf};

#[cfg(not(test))]
const RECURSIVE_DELETE_VERSION_SCAN_PAGE_SIZE: i32 = 1000;
#[cfg(test)]
const RECURSIVE_DELETE_VERSION_SCAN_PAGE_SIZE: i32 = 2;
const FORCE_DELETE_LIST_PAGE_SIZE: i32 = 1_000;

fn build_tier_delete_journal_entry(
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
    source: &ObjectInfo,
) -> Result<Option<Jentry>> {
    let version_id = opts.version_id.as_deref().map(Uuid::parse_str).transpose()?;
    let source_object = decode_dir_object(object);
    let Some(mut je) = (if opts.delete_prefix {
        transitioned_force_delete_journal_entry(&source.transitioned_object, source.transition_version_state).map(|mut je| {
            attach_tier_delete_source(&mut je, bucket, source_object.as_str(), source, opts.versioned, opts.version_suspended);
            je
        })
    } else {
        transitioned_delete_journal_entry_for_source(
            version_id,
            opts.versioned,
            opts.version_suspended,
            bucket,
            source_object.as_str(),
            source,
        )
    }) else {
        return Ok(None);
    };
    record_tier_delete_journal_backend_identity(&mut je, &source.user_defined).map_err(Error::other)?;
    Ok(Some(je))
}

async fn prepare_tier_delete_journal_entry(
    api: &Arc<ECStore>,
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
    source: &ObjectInfo,
) -> Result<Option<Jentry>> {
    let Some(je) = build_tier_delete_journal_entry(bucket, object, opts, source)? else {
        return Ok(None);
    };
    persist_tier_delete_journal_entry(Arc::clone(api), &je)
        .await
        .map_err(Error::other)?;
    Ok(Some(je))
}

async fn abort_prepared_tier_delete_journal_entry(api: &Arc<ECStore>, je: &Jentry) {
    if let Err(err) = abort_prepared_journal_entry_if_current(Arc::clone(api), je).await {
        warn!(
            object = %je.obj_name,
            tier = %je.tier_name,
            error = ?err,
            "failed to remove aborted tier delete journal"
        );
    }
}

async fn abort_prepared_tier_delete_journal_entries(api: &Arc<ECStore>, entries: &[Jentry]) {
    for entry in entries {
        abort_prepared_tier_delete_journal_entry(api, entry).await;
    }
}

async fn commit_prepared_tier_delete_journal_entry(api: &Arc<ECStore>, je: &Jentry) {
    if let Err(err) = commit_tier_delete_journal_entry(Arc::clone(api), je).await {
        warn!(
            object = %je.obj_name,
            tier = %je.tier_name,
            error = ?err,
            "tier delete committed locally but journal commit failed; recovery will retry"
        );
        return;
    }
    let mut committed = je.clone();
    committed.state = crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed;
    if let Err(err) = enqueue_committed_tier_delete_journal_entry(&committed).await {
        warn!(
            object = %je.obj_name,
            tier = %je.tier_name,
            error = ?err,
            "tier delete journal committed but could not be queued; recovery will retry"
        );
    }
}

async fn commit_prepared_tier_delete_journal_entries(api: &Arc<ECStore>, entries: &[Jentry]) {
    for entry in entries {
        commit_prepared_tier_delete_journal_entry(api, entry).await;
    }
}

async fn prepare_prefix_tier_delete_journal_entries(
    api: &Arc<ECStore>,
    bucket: &str,
    prefix: &str,
    opts: &ObjectOptions,
) -> Result<Vec<Jentry>> {
    let mut marker = None;
    let mut version_marker = None;
    let mut entries = Vec::new();

    loop {
        let page = Arc::clone(api)
            .list_object_versions_for_lifecycle(
                bucket,
                prefix,
                marker.clone(),
                version_marker.clone(),
                None,
                FORCE_DELETE_LIST_PAGE_SIZE,
            )
            .await?;

        for source in page.objects {
            if let Some(entry) = build_tier_delete_journal_entry(bucket, &source.name, opts, &source)? {
                entries.push(entry);
            }
        }

        if !page.is_truncated {
            break;
        }

        let next_marker = page
            .next_marker
            .ok_or_else(|| Error::other("truncated force delete listing has no next marker"))?;
        let next_version_marker = page.next_version_idmarker;
        if marker.as_deref() == Some(next_marker.as_str()) && version_marker == next_version_marker {
            return Err(Error::other("force delete listing marker did not advance"));
        }
        marker = Some(next_marker);
        version_marker = next_version_marker;
    }

    let mut persisted = Vec::with_capacity(entries.len());
    for entry in entries {
        if let Err(err) = persist_tier_delete_journal_entry(Arc::clone(api), &entry).await {
            abort_prepared_tier_delete_journal_entries(api, &persisted).await;
            return Err(Error::other(err));
        }
        persisted.push(entry);
    }
    Ok(persisted)
}
async fn delete_prefix_with_tier_delete_journal(
    store: &ECStore,
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
    tier_journal_api: Option<&Arc<ECStore>>,
) -> Result<()> {
    let lifecycle_delete_all = opts.lifecycle_delete_all.is_some();
    let journal_entry = if !lifecycle_delete_all && let Some(api) = tier_journal_api {
        Some(prepare_prefix_tier_delete_journal_entries(api, bucket, object, opts).await?)
    } else {
        None
    };

    let result = store.delete_prefix(bucket, object, opts).await;
    match result {
        Ok(()) => {
            let lifecycle_entries = if lifecycle_delete_all {
                opts.lifecycle_delete_all_journal()
                    .ok_or(StorageError::PreconditionFailed)?
                    .lock()
                    .prepared_entries()
            } else {
                Vec::new()
            };
            let entries = journal_entry.as_deref().unwrap_or(&lifecycle_entries);
            if let Some(api) = tier_journal_api {
                commit_prepared_tier_delete_journal_entries(api, entries).await;
            }
            Ok(())
        }
        Err(err) => {
            if let Some(api) = tier_journal_api {
                if lifecycle_delete_all {
                    let (abort, entries) = {
                        let journal = opts.lifecycle_delete_all_journal().ok_or(StorageError::PreconditionFailed)?;
                        let state = journal.lock();
                        (!state.mutation_started(), state.prepared_entries())
                    };
                    if abort {
                        abort_prepared_tier_delete_journal_entries(api, &entries).await;
                    }
                } else if let Some(entries) = journal_entry.as_ref() {
                    abort_prepared_tier_delete_journal_entries(api, entries).await;
                }
            }
            Err(err)
        }
    }
}

/// A GET whose object identity has been resolved while its namespace read lock
/// remains held, but whose body reader has not been constructed yet.
///
/// The application can evaluate request preconditions and cache coordination
/// against [`Self::object_info`] before consuming this value. Dropping it
/// releases the read lock without constructing a body reader.
pub struct PreparedGetObjectReader {
    pool: Arc<Sets>,
    bucket: String,
    object: String,
    range: Option<HTTPRangeSpec>,
    headers: HeaderMap,
    opts: ObjectOptions,
    metadata: crate::set_disk::PreparedGetObjectMetadata,
    read_lock_guard: Option<ObjectLockDiagGuard>,
}

impl PreparedGetObjectReader {
    /// Returns the fresh metadata snapshot protected by this prepared read.
    pub fn object_info(&self) -> &ObjectInfo {
        self.metadata.object_info()
    }

    /// Finishes a metadata-only decision without constructing a body reader.
    pub fn into_object_info(mut self) -> ObjectInfo {
        self.metadata.take_object_info()
    }

    /// Replaces the headers used when this prepared value constructs its body reader.
    #[must_use]
    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Constructs the body reader while retaining the metadata snapshot's read
    /// lock, then transfers that lock to the returned stream as usual. The
    /// staged caller already performed the authoritative app-layer cache probe,
    /// so the nested set-disk reader must not probe the same hook again.
    pub async fn into_reader(self) -> Result<GetObjectReader> {
        let mut reader =
            crate::object_api::without_get_object_body_cache_hook(self.pool.get_object_reader_with_prepared_metadata(
                &self.bucket,
                &self.object,
                self.range,
                self.headers,
                &self.opts,
                self.metadata,
            ))
            .await?;
        reader.body_source = crate::object_api::GetObjectBodySource::HookMissed;
        Ok(ECStore::attach_read_lock_guard(reader, self.read_lock_guard))
    }
}

struct LockGuardedReader {
    inner: Box<dyn AsyncRead + Unpin + Send + Sync>,
    guard: Option<ObjectLockDiagGuard>,
}

impl AsyncRead for LockGuardedReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let had_capacity = buf.remaining() > 0;
        let filled_before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if had_capacity && matches!(poll, Poll::Ready(Ok(()))) && buf.filled().len() == filled_before {
            self.guard.take();
        }
        poll
    }
}

#[derive(Clone, Copy, Debug)]
enum ObjectLockDiagMode {
    Read,
    Write,
}

impl ObjectLockDiagMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

impl fmt::Display for ObjectLockDiagMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(crate) struct ObjectLockDiagGuard {
    guard: rustfs_lock::NamespaceLockGuard,
    #[cfg(test)]
    test_namespace_lock_fence: Option<NamespaceLockFence>,
    enabled: bool,
    op: &'static str,
    bucket: Option<String>,
    object: Option<String>,
    owner: Option<String>,
    mode: ObjectLockDiagMode,
    acquired_at: Instant,
}

impl ObjectLockDiagGuard {
    fn new(
        guard: rustfs_lock::NamespaceLockGuard,
        enabled: bool,
        op: &'static str,
        bucket: Option<String>,
        object: Option<String>,
        owner: Option<String>,
        mode: ObjectLockDiagMode,
    ) -> Self {
        Self {
            guard,
            #[cfg(test)]
            test_namespace_lock_fence: None,
            enabled,
            op,
            bucket,
            object,
            owner,
            mode,
            acquired_at: Instant::now(),
        }
    }

    pub(crate) fn lock_lost_signal(&self) -> Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>> {
        match &self.guard {
            rustfs_lock::NamespaceLockGuard::Standard(guard) => Some(guard.lock_lost()),
            rustfs_lock::NamespaceLockGuard::Fast(_) => None,
        }
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        self.guard.is_lock_lost()
    }

    pub(crate) fn add_namespace_lock_fence(&self, opts: &mut ObjectOptions) {
        opts.ensure_namespace_lock_fence();
        if let Some(signal) = self.lock_lost_signal() {
            opts.add_namespace_lock_lost_signal(signal);
        }
        #[cfg(test)]
        if let Some(fence) = self.test_namespace_lock_fence.as_ref() {
            opts.add_namespace_lock_fence_for_test(fence);
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecommissionMutationFenceTestPhase {
    Migration,
    SourceCleanup,
}

#[cfg(test)]
struct DecommissionMutationFenceLossState {
    bucket: String,
    object: String,
    phase: DecommissionMutationFenceTestPhase,
    fence: NamespaceLockFence,
    loss_handle: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
pub(crate) struct DecommissionMutationFenceLossHook {
    state: Arc<DecommissionMutationFenceLossState>,
}

#[cfg(test)]
static DECOMMISSION_MUTATION_FENCE_LOSS_HOOK: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<DecommissionMutationFenceLossState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
impl DecommissionMutationFenceLossHook {
    pub(crate) fn install(bucket: &str, object: &str, phase: DecommissionMutationFenceTestPhase) -> Self {
        let (fence, loss_handle) = NamespaceLockFence::loss_handle_for_test();
        let state = Arc::new(DecommissionMutationFenceLossState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            phase,
            fence,
            loss_handle,
        });
        let mut slot = DECOMMISSION_MUTATION_FENCE_LOSS_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("decommission mutation fence loss hooks should not poison");
        assert!(slot.is_none(), "decommission mutation fence loss hook must be unique");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) fn mark_lost(&self) {
        self.state.loss_handle.store(true, Ordering::Release);
    }
}

#[cfg(test)]
impl Drop for DecommissionMutationFenceLossHook {
    fn drop(&mut self) {
        let mut slot = DECOMMISSION_MUTATION_FENCE_LOSS_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("decommission mutation fence loss hooks should not poison");
        if slot.as_ref().is_some_and(|hook| Arc::ptr_eq(hook, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
fn decommission_mutation_fence_for_test(
    bucket: &str,
    object: &str,
    phase: DecommissionMutationFenceTestPhase,
) -> Option<NamespaceLockFence> {
    DECOMMISSION_MUTATION_FENCE_LOSS_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("decommission mutation fence loss hooks should not poison")
        .as_ref()
        .filter(|hook| hook.bucket == bucket && hook.object == object && hook.phase == phase)
        .map(|hook| hook.fence.clone())
}

pub(crate) struct SourceCleanupMutationFence {
    guard: ObjectLockDiagGuard,
    source_lock_covered: bool,
}

impl SourceCleanupMutationFence {
    pub(crate) fn source_lock_covered(&self) -> bool {
        self.source_lock_covered
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        self.guard.is_lock_lost()
    }

    pub(crate) fn add_namespace_lock_fence(&self, opts: &mut ObjectOptions) {
        self.guard.add_namespace_lock_fence(opts);
    }
}

/// Opaque write-lock guard for the RestoreObject accept path; see
/// [`ECStore::acquire_restore_accept_guard`]. Deliberately not a general lock
/// API — it only exists so the accept path's restore-status compare-and-set
/// can span the ecstore/API layer boundary.
pub struct RestoreAcceptGuard(ObjectLockDiagGuard);

impl RestoreAcceptGuard {
    /// True when the underlying namespace lock was lost (e.g. heartbeat
    /// refresh lost quorum). Callers must check this before committing the
    /// restore-status write the guard exists to serialize.
    pub fn is_lock_lost(&self) -> bool {
        self.0.guard.is_lock_lost()
    }

    pub fn add_namespace_lock_fence(&self, opts: &mut ObjectOptions) {
        self.0.add_namespace_lock_fence(opts);
    }
}

impl Drop for ObjectLockDiagGuard {
    fn drop(&mut self) {
        if !self.enabled || self.guard.is_released() {
            return;
        }

        let hold = self.acquired_at.elapsed();
        record_object_lock_diag_hold_duration(self.op, self.mode.as_str(), hold);
        let threshold = get_object_lock_diag_slow_hold_threshold();
        if hold >= threshold {
            record_object_lock_diag_slow_hold(self.op, self.mode.as_str());
            warn!(
                target: "rustfs_ecstore::object_lock_diag",
                op = self.op,
                bucket = %self.bucket.as_deref().unwrap_or_default(),
                object = %self.object.as_deref().unwrap_or_default(),
                mode = %self.mode,
                owner = %self.owner.as_deref().unwrap_or_default(),
                hold_ms = hold.as_millis(),
                threshold_ms = threshold.as_millis(),
                "object namespace lock held longer than threshold"
            );
        }
    }
}

/// A failure to preserve one object generation for a SelectObjectContent read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SnapshotConsistencyError {
    #[error("namespace locking is disabled for SelectObjectContent")]
    LockingDisabled,
    #[error("SelectObjectContent namespace lock was lost")]
    LockLost,
    #[error("object read semantics changed while SelectObjectContent was running")]
    ObjectChanged,
}

/// Failure while creating a SelectObjectContent snapshot.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PrepareSelectObjectSnapshotError {
    #[error("storage failed while preparing SelectObjectContent snapshot: {0}")]
    Storage(#[source] StorageError),
    #[error("SelectObjectContent snapshot consistency failure: {0}")]
    Consistency(#[source] SnapshotConsistencyError),
    #[error("SelectObjectContent object has invalid logical size {size}")]
    InvalidLogicalSize { size: i64 },
}

impl From<StorageError> for PrepareSelectObjectSnapshotError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<SnapshotConsistencyError> for PrepareSelectObjectSnapshotError {
    fn from(error: SnapshotConsistencyError) -> Self {
        Self::Consistency(error)
    }
}

/// Failure while opening a reader from a SelectObjectContent snapshot.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SelectObjectSnapshotReadError {
    #[error("storage failed while opening SelectObjectContent snapshot reader: {0}")]
    Storage(#[source] StorageError),
    #[error("SelectObjectContent snapshot consistency failure: {0}")]
    Consistency(#[source] SnapshotConsistencyError),
}

impl From<StorageError> for SelectObjectSnapshotReadError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<SnapshotConsistencyError> for SelectObjectSnapshotReadError {
    fn from(error: SnapshotConsistencyError) -> Self {
        Self::Consistency(error)
    }
}

struct SelectObjectSnapshotLease {
    guards: Vec<ObjectLockDiagGuard>,
    lost: Arc<AtomicBool>,
    lock_loss: tokio::sync::watch::Sender<bool>,
    _monitor_shutdown: tokio::sync::watch::Sender<()>,
}

impl SelectObjectSnapshotLease {
    fn new(guards: Vec<ObjectLockDiagGuard>) -> Self {
        let signals = guards.iter().filter_map(ObjectLockDiagGuard::lock_lost_signal);
        let mut waits = signals
            .map(|signal| async move { signal.notified().await })
            .collect::<futures::stream::FuturesUnordered<_>>();
        let lost = Arc::new(AtomicBool::new(false));
        let (lock_loss, _) = tokio::sync::watch::channel(false);
        let (monitor_shutdown, mut shutdown_rx) = tokio::sync::watch::channel(());
        if !waits.is_empty() {
            let task_lost = Arc::clone(&lost);
            let task_lock_loss = lock_loss.clone();
            tokio::spawn(async move {
                tokio::select! {
                    lost = futures::StreamExt::next(&mut waits) => {
                        if lost.is_some() {
                            task_lost.store(true, Ordering::Release);
                            task_lock_loss.send_replace(true);
                        }
                    }
                    _ = shutdown_rx.changed() => {}
                }
            });
        }
        Self {
            guards,
            lost,
            lock_loss,
            _monitor_shutdown: monitor_shutdown,
        }
    }

    fn check(&self) -> std::result::Result<(), SnapshotConsistencyError> {
        if self.is_lost() || self.guards.iter().any(ObjectLockDiagGuard::is_lock_lost) {
            self.lost.store(true, Ordering::Release);
            self.lock_loss.send_replace(true);
            return Err(SnapshotConsistencyError::LockLost);
        }
        Ok(())
    }

    fn is_lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }

    fn subscribe_lock_loss(&self) -> tokio::sync::watch::Receiver<bool> {
        self.lock_loss.subscribe()
    }
}

/// Opaque, lock-backed object generation used by SelectObjectContent.
pub struct SelectObjectSnapshot {
    pool: Arc<Sets>,
    bucket: String,
    object: String,
    headers: HeaderMap,
    opts: ObjectOptions,
    object_info: ObjectInfo,
    logical_size: u64,
    read_semantics_identity: [u8; 32],
    first_metadata: ParkingMutex<Option<crate::set_disk::PreparedGetObjectMetadata>>,
    lease: Arc<SelectObjectSnapshotLease>,
}

impl fmt::Debug for SelectObjectSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SelectObjectSnapshot")
            .field("bucket", &self.bucket)
            .field("object", &self.object)
            .field("logical_size", &self.logical_size)
            .finish_non_exhaustive()
    }
}

impl SelectObjectSnapshot {
    pub fn is_for(&self, bucket: &str, object: &str) -> bool {
        self.bucket == bucket && self.object == encode_dir_object(object)
    }

    pub fn object_info(&self) -> &ObjectInfo {
        &self.object_info
    }

    pub fn logical_size(&self) -> u64 {
        self.logical_size
    }

    pub fn matches_version(&self, requested: &str) -> bool {
        select_snapshot_version_matches(self.object_info.version_id, requested)
    }

    pub fn ensure_valid(&self) -> std::result::Result<(), SnapshotConsistencyError> {
        self.lease.check()
    }

    #[cfg(feature = "test-util")]
    pub fn mark_lost_for_test(&self) {
        self.lease.lost.store(true, Ordering::Release);
        self.lease.lock_loss.send_replace(true);
    }

    pub async fn open_reader(
        &self,
        range: Option<HTTPRangeSpec>,
    ) -> std::result::Result<GetObjectReader, SelectObjectSnapshotReadError> {
        self.lease.check()?;
        let first_metadata = self.first_metadata.lock().take();
        let metadata = match first_metadata {
            Some(metadata) => metadata,
            None => {
                self.pool
                    .prepare_get_object_reader_metadata(&self.bucket, &self.object, &self.opts)
                    .await?
            }
        };
        self.lease.check()?;
        if metadata.read_semantics_identity() != self.read_semantics_identity {
            return Err(SnapshotConsistencyError::ObjectChanged.into());
        }

        let mut reader =
            crate::object_api::without_get_object_body_cache_hook(self.pool.get_object_reader_with_prepared_metadata(
                &self.bucket,
                &self.object,
                range,
                self.headers.clone(),
                &self.opts,
                metadata,
            ))
            .await?;
        self.lease.check()?;
        reader.body_source = crate::object_api::GetObjectBodySource::HookMissed;
        reader.stream = Box::new(SelectObjectSnapshotReader {
            inner: reader.stream,
            lock_loss_wake: SelectObjectSnapshotLockLossWake::new(self.lease.subscribe_lock_loss()),
            lease: Arc::clone(&self.lease),
        });
        Ok(reader)
    }
}

fn select_snapshot_version_matches(actual: Option<Uuid>, requested: &str) -> bool {
    let requested = requested.trim();
    let requested = if requested.eq_ignore_ascii_case("null") {
        Uuid::nil()
    } else if let Ok(requested) = Uuid::parse_str(requested) {
        requested
    } else {
        return false;
    };
    actual.unwrap_or_else(Uuid::nil) == requested
}

struct SelectObjectSnapshotReader {
    inner: Box<dyn AsyncRead + Unpin + Send + Sync>,
    lock_loss_wake: SelectObjectSnapshotLockLossWake,
    lease: Arc<SelectObjectSnapshotLease>,
}

struct SelectObjectSnapshotLockLossWake {
    stream: tokio_stream::wrappers::WatchStream<bool>,
}

impl SelectObjectSnapshotLockLossWake {
    fn new(receiver: tokio::sync::watch::Receiver<bool>) -> Self {
        Self {
            stream: tokio_stream::wrappers::WatchStream::new(receiver),
        }
    }

    fn poll_lost(&mut self, cx: &mut Context<'_>) -> bool {
        loop {
            match futures::Stream::poll_next(Pin::new(&mut self.stream), cx) {
                Poll::Ready(Some(true)) => return true,
                Poll::Ready(Some(false)) => {}
                Poll::Ready(None) | Poll::Pending => return false,
            }
        }
    }
}

impl AsyncRead for SelectObjectSnapshotReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.lock_loss_wake.poll_lost(cx) || self.lease.is_lost() {
            return Poll::Ready(Err(std::io::Error::other(SnapshotConsistencyError::LockLost)));
        }
        let filled_before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        let reached_eof = matches!(&poll, Poll::Ready(Ok(()))) && buf.filled().len() == filled_before;
        if self.lease.is_lost() || (reached_eof && self.lease.check().is_err()) {
            buf.set_filled(filled_before);
            return Poll::Ready(Err(std::io::Error::other(SnapshotConsistencyError::LockLost)));
        }
        poll
    }
}

fn log_object_lock_acquire_if_slow(
    op: &'static str,
    bucket: &str,
    object: &str,
    owner: Option<&str>,
    mode: ObjectLockDiagMode,
    elapsed: Duration,
    diag_enabled: bool,
) {
    if !diag_enabled {
        return;
    }

    let threshold = get_object_lock_diag_slow_acquire_threshold();
    record_object_lock_diag_acquire_duration(op, mode.as_str(), elapsed);
    if elapsed >= threshold {
        record_object_lock_diag_slow_acquire(op, mode.as_str());
        warn!(
            target: "rustfs_ecstore::object_lock_diag",
            op,
            bucket,
            object,
            mode = %mode,
            owner = owner.unwrap_or_default(),
            acquire_ms = elapsed.as_millis(),
            threshold_ms = threshold.as_millis(),
            "object namespace lock acquisition exceeded threshold"
        );
    }
}

fn select_data_movement_target_pool(
    existing_pool_idx: Result<usize>,
    src_pool_idx: usize,
    delete_marker: bool,
) -> Result<Option<usize>> {
    match existing_pool_idx {
        Ok(pool_idx) => {
            if delete_marker && pool_idx == src_pool_idx {
                Ok(None)
            } else {
                Ok(Some(pool_idx))
            }
        }
        Err(err) => {
            if is_err_read_quorum(&err) {
                return Err(StorageError::ErasureWriteQuorum);
            }
            if delete_marker && (is_err_object_not_found(&err) || is_err_version_not_found(&err)) {
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}

fn latest_object_access_delete_marker_error(
    bucket: &str,
    object: &str,
    info: &ObjectInfo,
    opts: &ObjectOptions,
) -> Option<Error> {
    if !info.delete_marker {
        return None;
    }

    Some(if opts.version_id.is_none() || opts.delete_marker {
        to_object_err(StorageError::FileNotFound, vec![bucket, object])
    } else {
        to_object_err(StorageError::MethodNotAllowed, vec![bucket, object])
    })
}

fn resolve_latest_object_access(
    bucket: &str,
    object: &str,
    info: ObjectInfo,
    idx: usize,
    opts: &ObjectOptions,
) -> Result<(ObjectInfo, usize)> {
    if let Some(err) = latest_object_access_delete_marker_error(bucket, object, &info, opts) {
        return Err(err);
    }

    Ok((info, idx))
}

fn should_create_delete_marker_for_missing_object(opts: &ObjectOptions) -> bool {
    (opts.versioned || opts.version_suspended) && opts.version_id.is_none() && !opts.delete_marker && !opts.data_movement
}

#[cfg(test)]
struct DeleteAfterObjectLockSnapshotBarrierState {
    bucket: String,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
    namespace_pending: tokio::sync::Notify,
    namespace_acquired: AtomicBool,
}

#[cfg(test)]
pub(crate) struct DeleteAfterObjectLockSnapshotBarrier {
    state: Arc<DeleteAfterObjectLockSnapshotBarrierState>,
}

#[cfg(test)]
static DELETE_AFTER_OBJECT_LOCK_SNAPSHOT_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<DeleteAfterObjectLockSnapshotBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
impl DeleteAfterObjectLockSnapshotBarrier {
    pub(crate) fn install(bucket: &str) -> Self {
        let state = Arc::new(DeleteAfterObjectLockSnapshotBarrierState {
            bucket: bucket.to_string(),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            namespace_pending: tokio::sync::Notify::new(),
            namespace_acquired: AtomicBool::new(false),
        });
        let mut slot = DELETE_AFTER_OBJECT_LOCK_SNAPSHOT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("delete snapshot barrier mutex should not poison");
        assert!(slot.is_none(), "delete snapshot barrier must not already be installed");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        self.state.arrived.notified().await;
    }

    pub(crate) fn release(&self) {
        self.state.release.notify_one();
    }

    pub(crate) async fn release_and_wait_until_namespace_pending(&self) {
        let namespace_pending = self.state.namespace_pending.notified();
        self.release();
        tokio::time::timeout(Duration::from_secs(5), namespace_pending)
            .await
            .expect("delete should proceed to its namespace lock after leaving the snapshot barrier");
    }

    pub(crate) fn namespace_acquired(&self) -> bool {
        self.state.namespace_acquired.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl Drop for DeleteAfterObjectLockSnapshotBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        if let Some(slot) = DELETE_AFTER_OBJECT_LOCK_SNAPSHOT_BARRIER.get() {
            let mut slot = slot.lock().expect("delete snapshot barrier mutex should not poison");
            if slot.as_ref().is_some_and(|installed| Arc::ptr_eq(installed, &self.state)) {
                *slot = None;
            }
        }
    }
}

#[cfg(test)]
async fn pause_delete_after_object_lock_snapshot(bucket: &str) {
    let state = DELETE_AFTER_OBJECT_LOCK_SNAPSHOT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("delete snapshot barrier mutex should not poison")
        .as_ref()
        .filter(|state| state.bucket == bucket)
        .cloned();
    if let Some(state) = state {
        state.arrived.notify_one();
        state.release.notified().await;
        state.namespace_pending.notify_one();
    }
}

#[cfg(test)]
fn notify_delete_namespace_acquired(bucket: &str) {
    let state = DELETE_AFTER_OBJECT_LOCK_SNAPSHOT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("delete snapshot barrier mutex should not poison")
        .as_ref()
        .filter(|state| state.bucket == bucket)
        .cloned();
    if let Some(state) = state {
        state.namespace_acquired.store(true, Ordering::Release);
    }
}

#[cfg(test)]
struct VersionedDeleteMarkerCommitBarrierState {
    bucket: String,
    object: String,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
pub(crate) struct VersionedDeleteMarkerCommitBarrier {
    state: Arc<VersionedDeleteMarkerCommitBarrierState>,
}

#[cfg(test)]
static VERSIONED_DELETE_MARKER_COMMIT_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<VersionedDeleteMarkerCommitBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
impl VersionedDeleteMarkerCommitBarrier {
    pub(crate) fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(VersionedDeleteMarkerCommitBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = VERSIONED_DELETE_MARKER_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("versioned delete-marker commit barrier mutex should not poison");
        assert!(slot.is_none(), "versioned delete-marker commit barrier must be unique");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("versioned DELETE should reach the post-marker-commit barrier");
    }

    pub(crate) fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(test)]
impl Drop for VersionedDeleteMarkerCommitBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = VERSIONED_DELETE_MARKER_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("versioned delete-marker commit barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn pause_versioned_delete_marker_after_commit(bucket: &str, object: &str) {
    let state = VERSIONED_DELETE_MARKER_COMMIT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("versioned delete-marker commit barrier mutex should not poison")
        .as_ref()
        .filter(|state| state.bucket == bucket && state.object == object)
        .cloned();
    if let Some(state) = state {
        state.arrived.notify_one();
        state.release.notified().await;
    }
}

/// Whether a delete-time lookup miss on a directory key should trigger an orphan
/// empty-directory tree purge (issue #4189).
///
/// The lookup surfaces *version*-not-found here, not object-not-found: `del_opts`
/// pins `version_id = Uuid::nil()` for directory keys, so a missing dir object fails
/// the specific-version lookup. Both misses must be accepted, otherwise the real
/// HTTP delete path (which always sets the nil version) never reaches the purge and
/// the ghost folder survives with a fake 204 — the exact #4189 symptom.
fn should_purge_orphan_dir_on_missing(err: &Error, object: &str) -> bool {
    (is_err_object_not_found(err) || is_err_version_not_found(err)) && rustfs_utils::path::is_dir_object(object)
}

fn version_aware_lookup_opts(opts: &ObjectOptions, no_lock: bool) -> ObjectOptions {
    let mut lookup_opts = opts.clone();
    lookup_opts.no_lock = no_lock;
    if lookup_opts.version_id.is_some() {
        lookup_opts.metadata_chg = true;
    }

    lookup_opts
}

fn data_movement_pool_lookup_opts(opts: &ObjectOptions, no_lock: bool) -> ObjectOptions {
    writer_pool_lookup_opts(opts, no_lock)
}

fn writer_pool_lookup_opts(opts: &ObjectOptions, no_lock: bool) -> ObjectOptions {
    let mut lookup_opts = version_aware_lookup_opts(opts, no_lock);
    lookup_opts.skip_decommissioned = true;
    lookup_opts.skip_rebalancing = true;

    lookup_opts
}

fn delete_pool_lookup_opts(opts: &ObjectOptions, no_lock: bool) -> ObjectOptions {
    let mut lookup_opts = writer_pool_lookup_opts(opts, no_lock);
    lookup_opts.skip_decommissioned = opts.data_movement;
    lookup_opts
}

fn should_delete_from_all_pools(opts: &ObjectOptions, pool_count: usize) -> bool {
    pool_count > 0 && (!opts.versioned && !opts.version_suspended || opts.version_id.is_some())
}

fn batch_delete_creates_latest_marker(object: &ObjectToDelete, delete_config_snapshot: &DeleteReplicationConfigSnapshot) -> bool {
    if object.version_id.is_some() {
        return false;
    }

    let object_name = decode_dir_object(&object.object_name);
    let (versioned, version_suspended) = delete_config_snapshot.versioning_config().delete_state(&object_name);
    versioned || version_suspended
}

fn batch_delete_targets_pool(creates_latest_marker: bool, marker_target_pool_idx: Option<usize>, pool_idx: usize) -> bool {
    !creates_latest_marker || marker_target_pool_idx == Some(pool_idx)
}

#[cfg(test)]
struct BatchDeletePoolErrorInjectionState {
    bucket: String,
    pool_idx: usize,
    errors: std::collections::HashMap<String, Error>,
    observed: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
pub(crate) struct BatchDeletePoolErrorInjection {
    state: Arc<BatchDeletePoolErrorInjectionState>,
}

#[cfg(test)]
static BATCH_DELETE_POOL_ERROR_INJECTION: std::sync::OnceLock<std::sync::Mutex<Option<Arc<BatchDeletePoolErrorInjectionState>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
impl BatchDeletePoolErrorInjection {
    pub(crate) fn install(bucket: &str, pool_idx: usize, errors: Vec<(String, Error)>) -> Self {
        let state = Arc::new(BatchDeletePoolErrorInjectionState {
            bucket: bucket.to_string(),
            pool_idx,
            errors: errors.into_iter().collect(),
            observed: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut slot = BATCH_DELETE_POOL_ERROR_INJECTION
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("batch delete pool error injection mutex should not poison");
        assert!(slot.is_none(), "batch delete pool error injection must be unique");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) fn observed(&self) -> usize {
        self.state.observed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl Drop for BatchDeletePoolErrorInjection {
    fn drop(&mut self) {
        let mut slot = BATCH_DELETE_POOL_ERROR_INJECTION
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("batch delete pool error injection mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
fn inject_batch_delete_pool_errors(
    bucket: &str,
    pool_idx: usize,
    object_names: &[String],
    deleted: &[DeletedObject],
    errors: &mut [Option<Error>],
) {
    let state = BATCH_DELETE_POOL_ERROR_INJECTION
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("batch delete pool error injection mutex should not poison")
        .as_ref()
        .filter(|state| state.bucket == bucket && state.pool_idx == pool_idx)
        .cloned();
    let Some(state) = state else {
        return;
    };

    for (idx, object_name) in object_names.iter().enumerate() {
        let Some(error) = state.errors.get(object_name) else {
            continue;
        };
        if errors[idx].is_none() && deleted[idx].found {
            errors[idx] = Some(error.clone());
            state.observed.fetch_add(1, Ordering::AcqRel);
        }
    }
}

fn resolve_batch_delete_pool_results<'a>(
    initial_error: Option<Error>,
    pool_results: impl IntoIterator<Item = (&'a DeletedObject, &'a Option<Error>)>,
) -> (Option<DeletedObject>, Option<Error>, bool) {
    let mut failure = initial_error.map(|err| (None, err));
    let mut deleted = None;
    let mut fallback: Option<(DeletedObject, Option<Error>)> = None;
    let mut attempted = false;

    for (pool_delete, pool_error) in pool_results {
        attempted = true;
        match pool_error {
            Some(err) if is_err_object_not_found(err) || is_err_version_not_found(err) => {
                if fallback.as_ref().is_none_or(|(_, error)| error.is_none()) {
                    fallback = Some(((*pool_delete).clone(), Some(err.clone())));
                }
            }
            Some(err) => {
                if failure.is_none() {
                    failure = Some((Some((*pool_delete).clone()), err.clone()));
                }
            }
            None if pool_delete.found => {
                if deleted.is_none() {
                    deleted = Some((*pool_delete).clone());
                }
            }
            None => {
                if fallback.is_none() {
                    fallback = Some(((*pool_delete).clone(), None));
                }
            }
        }
    }

    if let Some((failed_delete, err)) = failure {
        return (failed_delete, Some(err), attempted);
    }
    if let Some(deleted) = deleted {
        return (Some(deleted), None, attempted);
    }
    if let Some((deleted, err)) = fallback {
        return (Some(deleted), err, attempted);
    }

    (None, None, attempted)
}

fn transition_restore_pool_opts(opts: &ObjectOptions) -> ObjectOptions {
    let mut lookup_opts = opts.clone();
    lookup_opts.skip_decommissioned = true;
    lookup_opts.skip_rebalancing = true;
    lookup_opts
}

fn effective_object_actual_size(info: &ObjectInfo) -> Option<i64> {
    info.get_actual_size().ok()
}

fn is_equivalent_data_movement_delete_marker(source: &ObjectInfo, target: &ObjectInfo) -> bool {
    is_data_movement_delete_marker(source)
        && is_data_movement_delete_marker(target)
        && source.version_id == target.version_id
        && source.mod_time == target.mod_time
        && is_equivalent_data_movement_delete_marker_metadata(&source.user_defined, &target.user_defined)
        && source.user_tags == target.user_tags
        && source.replication_status_internal == target.replication_status_internal
        && source.replication_status == target.replication_status
        && source.version_purge_status_internal == target.version_purge_status_internal
        && source.version_purge_status == target.version_purge_status
}

fn is_equivalent_data_movement_delete_marker_metadata(
    source: &HashMap<String, String>,
    target: &HashMap<String, String>,
) -> bool {
    matches!(
        (
            data_movement_delete_marker_metadata_identity(source),
            data_movement_delete_marker_metadata_identity(target)
        ),
        (Some(source), Some(target)) if source == target
    )
}

fn data_movement_delete_marker_metadata_identity(metadata: &HashMap<String, String>) -> Option<HashMap<String, String>> {
    let mut identity = HashMap::with_capacity(metadata.len());
    let mut local_tier_free_version_id = None;
    for (key, value) in metadata {
        let Some(suffix) = rustfs_utils::http::strip_internal_prefix_preserving_case(key) else {
            identity.insert(key.clone(), value.clone());
            continue;
        };

        if suffix.eq_ignore_ascii_case(rustfs_utils::http::SUFFIX_TIER_FV_ID) {
            let version_id = Uuid::parse_str(value).ok().filter(|version_id| !version_id.is_nil())?;
            if local_tier_free_version_id.is_some_and(|expected| expected != version_id) {
                return None;
            }
            local_tier_free_version_id = Some(version_id);
            continue;
        }

        let canonical_suffix = [
            rustfs_utils::http::SUFFIX_REPLICA_TIMESTAMP,
            rustfs_utils::http::SUFFIX_REPLICA_STATUS,
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS,
            rustfs_utils::http::SUFFIX_PURGESTATUS,
        ]
        .into_iter()
        .find(|candidate| suffix.eq_ignore_ascii_case(candidate))
        .map(str::to_string)
        .or_else(|| {
            [
                rustfs_utils::http::SUFFIX_REPLICATION_RESET_ARN_PREFIX,
                rustfs_utils::http::SUFFIX_REPLICATION_DELETE_MARKER_VERSION_ARN_PREFIX,
            ]
            .into_iter()
            .find_map(|prefix| {
                suffix
                    .get(..prefix.len())
                    .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
                    .then(|| format!("{prefix}{}", &suffix[prefix.len()..]))
            })
        })
        .unwrap_or_else(|| suffix.to_string());
        let canonical_value = if canonical_suffix.eq_ignore_ascii_case(rustfs_utils::http::SUFFIX_REPLICA_TIMESTAMP)
            || canonical_suffix.eq_ignore_ascii_case(rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP)
        {
            rustfs_filemeta::parse_replication_timestamp(value)?
                .unix_timestamp_nanos()
                .to_string()
        } else {
            value.clone()
        };
        let canonical_key = format!("{}{canonical_suffix}", rustfs_utils::http::RUSTFS_INTERNAL_PREFIX);
        if identity
            .insert(canonical_key, canonical_value.clone())
            .is_some_and(|existing| existing != canonical_value)
        {
            return None;
        }
    }
    for (status_suffix, timestamp_suffix) in [
        (rustfs_utils::http::SUFFIX_REPLICA_STATUS, rustfs_utils::http::SUFFIX_REPLICA_TIMESTAMP),
        (
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS,
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,
        ),
    ] {
        let status_key = format!("{}{status_suffix}", rustfs_utils::http::RUSTFS_INTERNAL_PREFIX);
        let timestamp_key = format!("{}{timestamp_suffix}", rustfs_utils::http::RUSTFS_INTERNAL_PREFIX);
        match (identity.contains_key(&status_key), identity.contains_key(&timestamp_key)) {
            (true, false) => {
                identity.insert(timestamp_key, OffsetDateTime::UNIX_EPOCH.unix_timestamp_nanos().to_string());
            }
            (false, true) => return None,
            _ => {}
        }
    }
    Some(identity)
}

fn is_data_movement_delete_marker(info: &ObjectInfo) -> bool {
    info.delete_marker
}

fn is_expected_data_movement_delete_marker_source(source: &ObjectInfo, expected_mod_time: Option<OffsetDateTime>) -> bool {
    is_data_movement_delete_marker(source)
        && source.mod_time.is_some()
        && source.mod_time == expected_mod_time
        && data_movement_delete_marker_metadata_identity(&source.user_defined).is_some()
}

fn current_data_movement_delete_marker_opts(source: &ObjectInfo, opts: &ObjectOptions) -> Option<ObjectOptions> {
    let replica_status = rustfs_utils::http::get_str(&source.user_defined, rustfs_utils::http::SUFFIX_REPLICA_STATUS);
    let replica_timestamp = rustfs_utils::http::get_str(&source.user_defined, rustfs_utils::http::SUFFIX_REPLICA_TIMESTAMP);
    let (replica_status, replica_timestamp) = match (replica_status, replica_timestamp) {
        (None, None) => Default::default(),
        (Some(status), timestamp) => {
            let status = crate::bucket::replication::ReplicationStatusType::from(status.as_str());
            if status.is_empty() {
                return None;
            }
            let timestamp = match timestamp {
                Some(timestamp) => rustfs_filemeta::parse_replication_timestamp(&timestamp)?,
                None => OffsetDateTime::UNIX_EPOCH,
            };
            (status, Some(timestamp))
        }
        (None, Some(_)) => return None,
    };
    let replication_status = rustfs_utils::http::get_str(&source.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_STATUS);
    let replication_timestamp =
        rustfs_utils::http::get_str(&source.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP);
    let (replication_status, replication_timestamp, replication_targets) = match (replication_status, replication_timestamp) {
        (None, None) => Default::default(),
        (Some(status), timestamp) => {
            let direct_status = crate::bucket::replication::ReplicationStatusType::from(status.as_str());
            let targets = crate::bucket::replication::replication_statuses_map(status.as_str());
            if direct_status.is_empty() && targets.is_empty() {
                return None;
            }
            let timestamp = match timestamp {
                Some(timestamp) => rustfs_filemeta::parse_replication_timestamp(&timestamp)?,
                None => OffsetDateTime::UNIX_EPOCH,
            };
            (Some(status), Some(timestamp), targets)
        }
        (None, Some(_)) => return None,
    };
    let mut state = source.replication_state();
    if state.target_delete_marker_version_ids_corrupt {
        return None;
    }
    state.replica_status = replica_status;
    state.replica_timestamp = replica_timestamp;
    state.replication_status_internal = replication_status;
    state.replication_timestamp = replication_timestamp;
    state.targets = replication_targets;
    state.replicate_decision_str = source.replication_decision.clone();
    state.delete_marker = true;

    let mut target_opts = opts.clone();
    target_opts.mod_time = source.mod_time;
    target_opts.delete_replication = Some(state);
    Some(target_opts)
}

fn expected_data_movement_tiered_object(source: &rustfs_filemeta::FileInfo) -> ObjectInfo {
    ObjectInfo::from_file_info(source, "", &source.name, source.version_id.is_some())
}

fn is_equivalent_data_movement_tiered_object(source: &rustfs_filemeta::FileInfo, target: &ObjectInfo) -> bool {
    let expected = expected_data_movement_tiered_object(source);
    let Some(source_actual_size) = effective_object_actual_size(&expected) else {
        return false;
    };
    let Some(target_actual_size) = effective_object_actual_size(target) else {
        return false;
    };

    source.version_id == target.version_id
        && !target.delete_marker
        && source.size == target.size
        && source.get_etag() == target.etag
        && source.checksum == target.checksum
        && crate::data_movement::are_equivalent_data_movement_parts(&source.parts, &target.parts)
        && source.mod_time == target.mod_time
        && crate::data_movement::is_equivalent_data_movement_metadata(&expected, target, source_actual_size, target_actual_size)
        && expected.user_tags == target.user_tags
        && expected.expires == target.expires
        && expected.storage_class == target.storage_class
        && expected.replication_status_internal == target.replication_status_internal
        && expected.replication_status == target.replication_status
        && expected.version_purge_status_internal == target.version_purge_status_internal
        && expected.version_purge_status == target.version_purge_status
        && expected.transitioned_object.status == target.transitioned_object.status
        && expected.transition_version_state == target.transition_version_state
        && expected.transitioned_object.name == target.transitioned_object.name
        && expected.transitioned_object.tier == target.transitioned_object.tier
        && expected.transitioned_object.version_id == target.transitioned_object.version_id
        && expected.transitioned_object.free_version == target.transitioned_object.free_version
        && source_actual_size == target_actual_size
}

fn tiered_data_movement_source_matches(
    expected: &rustfs_filemeta::FileInfo,
    current: &rustfs_filemeta::FileInfo,
) -> Result<bool> {
    let expected_backend = crate::services::tier::tier::tier_destination_id_from_metadata(&expected.metadata)?;
    let current_backend = crate::services::tier::tier::tier_destination_id_from_metadata(&current.metadata)?;
    Ok(expected.version_id == current.version_id
        && expected.data_dir == current.data_dir
        && expected.mod_time == current.mod_time
        && expected.size == current.size
        && expected.get_etag() == current.get_etag()
        && expected.transition_status == current.transition_status
        && expected.transitioned_objname == current.transitioned_objname
        && expected.transition_tier == current.transition_tier
        && expected.transition_version_id == current.transition_version_id
        && expected.transition_version == current.transition_version
        && expected.transition_version_state == current.transition_version_state
        && expected_backend == current_backend)
}

fn should_check_data_movement_resume_target(src_pool_idx: usize, target_pool_idx: usize) -> bool {
    target_pool_idx != src_pool_idx
}

fn resolve_data_movement_resume_target_pool(
    selected_target_pool_idx: usize,
    resume_target_pool_idx: Option<usize>,
    src_pool_idx: usize,
) -> usize {
    if should_check_data_movement_resume_target(src_pool_idx, selected_target_pool_idx) {
        selected_target_pool_idx
    } else {
        resume_target_pool_idx.unwrap_or(selected_target_pool_idx)
    }
}

fn resolve_data_movement_delete_marker_resume_result(
    target_result: Result<Option<ObjectInfo>>,
    source: &ObjectInfo,
    src_pool_idx: usize,
    target_pool_idx: usize,
) -> Result<bool> {
    if !should_check_data_movement_resume_target(src_pool_idx, target_pool_idx) {
        return Ok(false);
    }

    let Some(target) = target_result? else {
        return Ok(false);
    };

    Ok(is_equivalent_data_movement_delete_marker(source, &target))
}

fn resolve_data_movement_tiered_resume_result(
    target_result: Result<Option<ObjectInfo>>,
    source: &rustfs_filemeta::FileInfo,
    src_pool_idx: usize,
    target_pool_idx: usize,
) -> Result<bool> {
    if !should_check_data_movement_resume_target(src_pool_idx, target_pool_idx) {
        return Ok(false);
    }

    let Some(target) = target_result? else {
        return Ok(false);
    };

    Ok(is_equivalent_data_movement_tiered_object(source, &target))
}

fn return_batch_delete_lock_error(objects: &[ObjectToDelete], err: Error) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
    let del_objects = objects
        .iter()
        .map(|object| DeletedObject {
            object_name: decode_dir_object(&object.object_name),
            version_id: object.version_id,
            ..Default::default()
        })
        .collect();
    let del_errs = objects.iter().map(|_| Some(err.clone())).collect();

    (del_objects, del_errs)
}

fn return_batch_delete_lock_error_with_accounting(
    objects: &[ObjectToDelete],
    err: Error,
) -> (Vec<DeletedObject>, Vec<Option<Error>>, Vec<Option<DeleteAccounting>>) {
    let (deleted, errors) = return_batch_delete_lock_error(objects, err);
    (deleted, errors, vec![None; objects.len()])
}

fn sorted_unique_delete_object_names(objects: &[ObjectToDelete]) -> Vec<&str> {
    let mut object_names: Vec<&str> = objects.iter().map(|object| object.object_name.as_str()).collect();
    object_names.sort_unstable();
    object_names.dedup();
    object_names
}

impl ECStore {
    /// Captures Object Lock state once for a batch of PUTs to the same bucket.
    /// `handle_put_object` only reuses the token for the same store, bucket,
    /// bucket incarnation, and Object Lock configuration revision.
    pub async fn object_lock_config_snapshot(&self, bucket: &str) -> Result<Arc<ObjectLockConfigSnapshot>> {
        check_valid_bucket_name(bucket)?;
        let lifecycle_guard = self.acquire_bucket_lifecycle_read_lock(bucket).await?;
        let metadata_guard = acquire_bucket_metadata_transaction_read_lock_in(&self.ctx, bucket).await?;
        let (state, bucket_incarnation_id, config_revision) =
            get_object_lock_config_and_incarnation_from_disk_in(&self.ctx, bucket).await?;
        if lifecycle_guard.is_lock_lost() || metadata_guard.is_lock_lost() {
            return Err(Error::other("bucket lifecycle lock was lost while loading the Object Lock snapshot"));
        }
        Ok(Arc::new(ObjectLockConfigSnapshot::for_guarded_store_bucket(
            self.id,
            bucket,
            bucket_incarnation_id,
            config_revision,
            state,
            lifecycle_guard,
            metadata_guard,
        )))
    }

    pub(super) async fn object_lock_config_snapshot_under_lifecycle_fence(
        &self,
        bucket: &str,
        lifecycle_fence: &NamespaceLockFence,
    ) -> Result<Arc<ObjectLockConfigSnapshot>> {
        if lifecycle_fence.is_lock_lost() {
            return Err(Error::other("bucket lifecycle lock was lost before loading the Object Lock snapshot"));
        }
        let metadata_guard = acquire_bucket_metadata_transaction_read_lock_in(&self.ctx, bucket).await?;
        let (state, bucket_incarnation_id, config_revision) =
            get_object_lock_config_and_incarnation_from_disk_in(&self.ctx, bucket).await?;
        if lifecycle_fence.is_lock_lost() || metadata_guard.is_lock_lost() {
            return Err(Error::other("bucket lock was lost while loading the Object Lock snapshot"));
        }
        Ok(Arc::new(ObjectLockConfigSnapshot::for_store_bucket_under_lifecycle_fence(
            self.id,
            bucket,
            bucket_incarnation_id,
            config_revision,
            state,
            lifecycle_fence.clone(),
            metadata_guard,
        )))
    }

    /// Resolves a GET's object identity without constructing its body reader.
    ///
    /// This is an additive two-stage counterpart to `get_object_reader`. The
    /// existing method remains the compatibility path for callers that do not
    /// need a pre-reader decision point.
    pub async fn prepare_select_object_snapshot(
        &self,
        bucket: &str,
        object: &str,
        headers: &HeaderMap,
        opts: &ObjectOptions,
    ) -> std::result::Result<SelectObjectSnapshot, PrepareSelectObjectSnapshotError> {
        check_get_obj_args(bucket, object)?;

        let object = encode_dir_object(object);
        let mut opts = opts.clone();
        opts.no_lock = false;
        opts.metadata_cache_safe = false;
        let read_lock_guards = self
            .acquire_all_object_read_locks("select_object", bucket, &object, &mut opts)
            .await?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(SnapshotConsistencyError::LockingDisabled.into());
        }
        if read_lock_guards.iter().any(ObjectLockDiagGuard::is_lock_lost) {
            return Err(SnapshotConsistencyError::LockLost.into());
        }

        let pool = if self.single_pool() {
            Arc::clone(&self.pools[0])
        } else {
            let (_, pool_idx) = self.get_latest_object_info_with_idx(bucket, &object, &opts).await?;
            self.pools.get(pool_idx).cloned().ok_or_else(|| {
                StorageError::other(format!("resolved SelectObjectContent pool index {pool_idx} is out of bounds"))
            })?
        };
        let mut metadata = pool.prepare_get_object_reader_metadata(bucket, &object, &opts).await?;
        if read_lock_guards.iter().any(ObjectLockDiagGuard::is_lock_lost) {
            return Err(SnapshotConsistencyError::LockLost.into());
        }
        if let Some(error) = latest_object_access_delete_marker_error(bucket, &object, metadata.object_info(), &opts) {
            return Err(error.into());
        }

        let logical_size_i64 = metadata.object_info().get_actual_size().map_err(StorageError::from)?;
        let logical_size = u64::try_from(logical_size_i64)
            .map_err(|_| PrepareSelectObjectSnapshotError::InvalidLogicalSize { size: logical_size_i64 })?;
        let read_semantics_identity = metadata.read_semantics_identity();
        let object_info = metadata.take_object_info();
        if read_lock_guards.iter().any(ObjectLockDiagGuard::is_lock_lost) {
            return Err(SnapshotConsistencyError::LockLost.into());
        }

        Ok(SelectObjectSnapshot {
            pool,
            bucket: bucket.to_owned(),
            object,
            headers: rustfs_utils::http::project_ssec_transport_headers(headers),
            opts,
            object_info,
            logical_size,
            read_semantics_identity,
            first_metadata: ParkingMutex::new(Some(metadata)),
            lease: Arc::new(SelectObjectSnapshotLease::new(read_lock_guards)),
        })
    }

    pub async fn prepare_get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        headers: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<PreparedGetObjectReader> {
        check_get_obj_args(bucket, object)?;

        let object = encode_dir_object(object);
        let mut opts = opts.clone();
        let read_lock_guard = self
            .acquire_object_read_lock_if_needed("prepare_get_object", bucket, &object, &mut opts)
            .await?;

        let (metadata, pool) = if self.single_pool() {
            let pool = Arc::clone(&self.pools[0]);
            let metadata = pool.prepare_get_object_reader_metadata(bucket, &object, &opts).await?;
            (metadata, pool)
        } else {
            let (_, pool_idx) = self
                .get_latest_accessible_object_info_with_idx(bucket, &object, &opts)
                .await?;
            let pool = self
                .pools
                .get(pool_idx)
                .cloned()
                .ok_or_else(|| Error::other(format!("resolved GET pool index {pool_idx} is out of bounds")))?;
            let metadata = pool.prepare_get_object_reader_metadata(bucket, &object, &opts).await?;
            (metadata, pool)
        };

        Ok(PreparedGetObjectReader {
            pool,
            bucket: bucket.to_owned(),
            object,
            range,
            headers,
            opts,
            metadata,
            read_lock_guard,
        })
    }

    fn map_namespace_lock_error(bucket: &str, object: &str, mode: &'static str, err: rustfs_lock::LockError) -> StorageError {
        match err {
            rustfs_lock::LockError::QuorumNotReached { required, achieved } => StorageError::NamespaceLockQuorumUnavailable {
                mode,
                bucket: bucket.to_string(),
                object: object.to_string(),
                required,
                achieved,
            },
            other => StorageError::Lock(other),
        }
    }

    async fn acquire_object_write_lock(&self, op: &'static str, bucket: &str, object: &str) -> Result<ObjectLockDiagGuard> {
        let diag_enabled = is_object_lock_diag_enabled();
        let ns_lock = self.handle_new_ns_lock(bucket, object).await?;
        let acquire_start = Instant::now();
        let guard = ns_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| Self::map_namespace_lock_error(bucket, object, "write", err))?;
        let owner = diag_enabled.then(|| ns_lock.owner().to_string());
        log_object_lock_acquire_if_slow(
            op,
            bucket,
            object,
            owner.as_deref(),
            ObjectLockDiagMode::Write,
            acquire_start.elapsed(),
            diag_enabled,
        );

        Ok(ObjectLockDiagGuard::new(
            guard,
            diag_enabled,
            op,
            diag_enabled.then(|| bucket.to_string()),
            diag_enabled.then(|| object.to_string()),
            owner,
            ObjectLockDiagMode::Write,
        ))
    }

    async fn acquire_object_write_lock_if_needed(
        &self,
        op: &'static str,
        bucket: &str,
        object: &str,
        opts: &mut ObjectOptions,
    ) -> Result<Option<ObjectLockDiagGuard>> {
        if opts.no_lock {
            return Ok(None);
        }

        let guard = self.acquire_object_write_lock(op, bucket, object).await?;
        if let Some(signal) = guard.lock_lost_signal() {
            opts.add_namespace_lock_lost_signal(signal);
        }
        opts.ensure_namespace_lock_fence();
        opts.no_lock = true;

        Ok(Some(guard))
    }

    /// Serializes the RestoreObject accept path — the read of the current
    /// `x-amz-restore` status, the ongoing/already-restored decision, and the
    /// metadata write that flips `ongoing-request="true"` — against concurrent
    /// accepts of the same object, making that read-check-write an atomic
    /// compare-and-set (backlog#1304). While the guard is held the caller must
    /// pass `no_lock: true` on its reads/writes of this object, check
    /// [`RestoreAcceptGuard::is_lock_lost`] before the status write, and drop
    /// the guard before starting the tier copy-back so concurrent
    /// HEAD/`get_object_info` stay non-blocking during the restore.
    pub async fn acquire_restore_accept_guard(&self, bucket: &str, object: &str) -> Result<RestoreAcceptGuard> {
        let object = encode_dir_object(object);
        let guard = self
            .acquire_object_write_lock("restore_object_accept", bucket, &object)
            .await?;
        Ok(RestoreAcceptGuard(guard))
    }

    async fn acquire_delete_objects_write_locks(
        &self,
        bucket: &str,
        objects: &[ObjectToDelete],
        opts: &mut ObjectOptions,
    ) -> Result<Vec<ObjectLockDiagGuard>> {
        if opts.no_lock || objects.is_empty() {
            return Ok(Vec::new());
        }

        let object_names = sorted_unique_delete_object_names(objects);
        // Lock order: encoded object names are acquired in ascending order, then
        // the set-layer calls receive no_lock so they do not reacquire them.
        let mut guards = Vec::with_capacity(object_names.len());
        for object in object_names {
            guards.push(self.acquire_object_write_lock("delete_objects", bucket, object).await?);
        }
        opts.no_lock = true;
        for signal in guards.iter().filter_map(ObjectLockDiagGuard::lock_lost_signal) {
            opts.add_namespace_lock_lost_signal(signal);
        }
        opts.ensure_namespace_lock_fence();

        Ok(guards)
    }

    async fn acquire_object_read_lock_if_needed(
        &self,
        op: &'static str,
        bucket: &str,
        object: &str,
        opts: &mut ObjectOptions,
    ) -> Result<Option<ObjectLockDiagGuard>> {
        if opts.no_lock {
            return Ok(None);
        }

        let diag_enabled = is_object_lock_diag_enabled();
        let ns_lock = self.handle_new_ns_lock(bucket, object).await?;
        let acquire_start = Instant::now();
        let guard = ns_lock
            .get_read_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| Self::map_namespace_lock_error(bucket, object, "read", err))?;
        let owner = diag_enabled.then(|| ns_lock.owner().to_string());
        log_object_lock_acquire_if_slow(
            op,
            bucket,
            object,
            owner.as_deref(),
            ObjectLockDiagMode::Read,
            acquire_start.elapsed(),
            diag_enabled,
        );
        opts.no_lock = true;
        opts.metadata_cache_safe = true;

        Ok(Some(ObjectLockDiagGuard::new(
            guard,
            diag_enabled,
            op,
            diag_enabled.then(|| bucket.to_string()),
            diag_enabled.then(|| object.to_string()),
            owner,
            ObjectLockDiagMode::Read,
        )))
    }

    pub(crate) async fn acquire_decommission_object_mutation_fence(
        &self,
        bucket: &str,
        object: &str,
    ) -> Result<ObjectLockDiagGuard> {
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("decommission object migration requires namespace locking"));
        }

        #[cfg(test)]
        let test_namespace_lock_fence =
            decommission_mutation_fence_for_test(bucket, object, DecommissionMutationFenceTestPhase::Migration);
        let object = encode_dir_object(object);
        let mut opts = ObjectOptions::default();
        let guard = self
            .acquire_object_read_lock_if_needed("decommission_object", bucket, &object, &mut opts)
            .await?
            .ok_or_else(|| Error::other("decommission object migration failed to acquire its namespace fence"))?;
        #[cfg(test)]
        let guard = {
            let mut guard = guard;
            guard.test_namespace_lock_fence = test_namespace_lock_fence;
            guard
        };
        Ok(guard)
    }

    pub(super) async fn apply_decommission_target_mutation_fence(
        &self,
        target_pool_idx: usize,
        object: &str,
        opts: &mut ObjectOptions,
        mutation_fence: Option<&ObjectLockDiagGuard>,
    ) {
        let Some(mutation_fence) = mutation_fence else {
            return;
        };

        mutation_fence.add_namespace_lock_fence(opts);
        let fixed_set = self.pools.first().and_then(|pool| pool.disk_set.first());
        let target_set = self.pools.get(target_pool_idx).map(|pool| pool.get_disks_by_key(object));
        opts.no_lock = match (fixed_set, target_set) {
            (Some(fixed), Some(target)) => fixed.shares_namespace_lock_domain(&target).await,
            _ => false,
        };
    }

    pub(crate) async fn acquire_decommission_source_cleanup_fence(
        &self,
        bucket: &str,
        object: &str,
        source_set: &SetDisks,
    ) -> Result<SourceCleanupMutationFence> {
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("decommission source cleanup requires namespace locking"));
        }

        #[cfg(test)]
        crate::data_movement::notify_source_cleanup_mutation_fence_pending(bucket, object);
        #[cfg(test)]
        let test_namespace_lock_fence =
            decommission_mutation_fence_for_test(bucket, object, DecommissionMutationFenceTestPhase::SourceCleanup);
        let object = encode_dir_object(object);
        let fixed_set = Arc::clone(&self.pools[0].disk_set[0]);
        let source_lock_covered = fixed_set.shares_namespace_lock_domain(source_set).await;
        // Lock order: fixed store mutation domain first; source cleanup takes its
        // hashed source-domain lock second only when this guard does not cover it.
        let guard = self
            .acquire_object_write_lock("decommission_source_cleanup", bucket, &object)
            .await?;
        #[cfg(test)]
        let guard = {
            let mut guard = guard;
            guard.test_namespace_lock_fence = test_namespace_lock_fence;
            guard
        };

        Ok(SourceCleanupMutationFence {
            guard,
            source_lock_covered,
        })
    }

    pub(crate) async fn acquire_all_object_read_locks(
        &self,
        op: &'static str,
        bucket: &str,
        object: &str,
        opts: &mut ObjectOptions,
    ) -> Result<Vec<ObjectLockDiagGuard>> {
        let diag_enabled = is_object_lock_diag_enabled();
        let mut guards = Vec::with_capacity(self.pools.len() + 1);

        // Lock order is the store fixed domain first, then pool index ascending
        // for each object's hashed set. DELETE and same-key CopyObject use the
        // fixed domain, while PUT commits and data movement use the hashed set.
        let distributed = self.ctx.is_dist_erasure().await;
        if let Some(guard) = self.acquire_object_read_lock_if_needed(op, bucket, object, opts).await? {
            guards.push(guard);
        }
        let fixed_set = Arc::clone(&self.pools[0].disk_set[0]);
        let mut locked_sets = vec![fixed_set];

        for pool in &self.pools {
            let hashed_set = pool.get_disks_by_key(object);
            let lock_domain_already_held = !distributed
                || locked_sets
                    .iter()
                    .any(|locked_set| same_distributed_lock_domain(&locked_set.lockers, &hashed_set.lockers));
            if lock_domain_already_held {
                continue;
            }
            let ns_lock = hashed_set.new_ns_lock(bucket, object).await?;
            let acquire_start = Instant::now();
            let guard = ns_lock
                .get_read_lock(get_lock_acquire_timeout())
                .await
                .map_err(|err| Self::map_namespace_lock_error(bucket, object, "read", err))?;
            let owner = diag_enabled.then(|| ns_lock.owner().to_string());
            log_object_lock_acquire_if_slow(
                op,
                bucket,
                object,
                owner.as_deref(),
                ObjectLockDiagMode::Read,
                acquire_start.elapsed(),
                diag_enabled,
            );
            guards.push(ObjectLockDiagGuard::new(
                guard,
                diag_enabled,
                op,
                diag_enabled.then(|| bucket.to_string()),
                diag_enabled.then(|| object.to_string()),
                owner,
                ObjectLockDiagMode::Read,
            ));
            locked_sets.push(hashed_set);
        }
        Ok(guards)
    }

    async fn acquire_data_movement_object_write_locks(
        &self,
        bucket: &str,
        object: &str,
        source_pool_idx: usize,
        target_pool_idx: usize,
        opts: &mut ObjectOptions,
    ) -> Result<Vec<ObjectLockDiagGuard>> {
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("tiered data movement requires namespace locking"));
        }
        let distributed = self.ctx.is_dist_erasure().await;
        let diag_enabled = is_object_lock_diag_enabled();
        let mut pool_indices = [source_pool_idx, target_pool_idx];
        pool_indices.sort_unstable();
        let fixed_set = Arc::clone(&self.pools[0].disk_set[0]);
        let mut locked_sets = vec![fixed_set];
        let mut guards = Vec::with_capacity(3);

        // Lock order matches journal recovery: fixed store domain first, then
        // hashed domains by ascending pool index. This also serializes source
        // revalidation and target publication against ordinary object deletes.
        guards.push(self.acquire_object_write_lock("tiered_data_movement", bucket, object).await?);
        for pool_idx in pool_indices {
            let pool = self
                .pools
                .get(pool_idx)
                .ok_or_else(|| Error::other(format!("invalid tiered data movement pool {pool_idx}")))?;
            let set = pool.get_disks_by_key(object);
            let lock_domain_already_held = !distributed
                || locked_sets.iter().any(|locked_set: &Arc<crate::set_disk::SetDisks>| {
                    same_distributed_lock_domain(&locked_set.lockers, &set.lockers)
                });
            if lock_domain_already_held {
                continue;
            }
            let ns_lock = set.new_ns_lock(bucket, object).await?;
            let acquire_start = Instant::now();
            let guard = ns_lock
                .get_write_lock(get_lock_acquire_timeout())
                .await
                .map_err(|err| Self::map_namespace_lock_error(bucket, object, "write", err))?;
            let owner = diag_enabled.then(|| ns_lock.owner().to_string());
            log_object_lock_acquire_if_slow(
                "tiered_data_movement",
                bucket,
                object,
                owner.as_deref(),
                ObjectLockDiagMode::Write,
                acquire_start.elapsed(),
                diag_enabled,
            );
            guards.push(ObjectLockDiagGuard::new(
                guard,
                diag_enabled,
                "tiered_data_movement",
                diag_enabled.then(|| bucket.to_string()),
                diag_enabled.then(|| object.to_string()),
                owner,
                ObjectLockDiagMode::Write,
            ));
            locked_sets.push(set);
        }
        opts.no_lock = true;
        for signal in guards.iter().filter_map(ObjectLockDiagGuard::lock_lost_signal) {
            opts.add_namespace_lock_lost_signal(signal);
        }
        opts.ensure_namespace_lock_fence();
        Ok(guards)
    }

    fn attach_read_lock_guard(mut reader: GetObjectReader, guard: Option<ObjectLockDiagGuard>) -> GetObjectReader {
        if is_lock_optimization_enabled() || reader.buffered_body.is_some() {
            return reader;
        }

        if let Some(guard) = guard {
            reader.stream = Box::new(LockGuardedReader {
                inner: reader.stream,
                guard: Some(guard),
            });
        }

        reader
    }

    async fn get_latest_accessible_object_info_with_idx(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<(ObjectInfo, usize)> {
        let (info, idx) = self.get_latest_object_info_with_idx(bucket, object, opts).await?;
        resolve_latest_object_access(bucket, object, info, idx, opts)
    }

    pub(super) async fn select_data_movement_pool_idx(
        &self,
        bucket: &str,
        object: &str,
        size: i64,
        opts: &ObjectOptions,
        no_lock: bool,
    ) -> Result<usize> {
        match self
            .get_pool_info_existing_with_opts(bucket, object, &data_movement_pool_lookup_opts(opts, no_lock))
            .await
        {
            Ok((pinfo, _)) => Ok(pinfo.index),
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(err);
                }

                self.get_available_pool_idx(bucket, object, size).await.ok_or(Error::DiskFull)
            }
        }
    }

    async fn find_data_movement_target_info(
        &self,
        bucket: &str,
        object: &str,
        target_pool_idx: usize,
        opts: &ObjectOptions,
    ) -> Result<Option<ObjectInfo>> {
        let mut lookup_opts = version_aware_lookup_opts(opts, true);
        lookup_opts.include_part_checksums = true;

        let Some(pool) = self.pools.get(target_pool_idx) else {
            return Err(Error::other(format!(
                "data movement resume target pool {target_pool_idx} is out of range for {bucket}/{object}"
            )));
        };

        match pool.get_object_info(bucket, object, &lookup_opts).await {
            Ok(info) => Ok(Some(info)),
            Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn has_equivalent_data_movement_delete_marker(
        &self,
        bucket: &str,
        object: &str,
        source: &ObjectInfo,
        opts: &ObjectOptions,
        target_pool_idx: usize,
    ) -> Result<bool> {
        resolve_data_movement_delete_marker_resume_result(
            self.find_data_movement_target_info(bucket, object, target_pool_idx, opts)
                .await,
            source,
            opts.src_pool_idx,
            target_pool_idx,
        )
    }

    async fn has_equivalent_data_movement_tiered_object(
        &self,
        bucket: &str,
        object: &str,
        source: &rustfs_filemeta::FileInfo,
        opts: &ObjectOptions,
        target_pool_idx: usize,
    ) -> Result<bool> {
        resolve_data_movement_tiered_resume_result(
            self.find_data_movement_target_info(bucket, object, target_pool_idx, opts)
                .await,
            source,
            opts.src_pool_idx,
            target_pool_idx,
        )
    }

    fn resolve_decommission_target_pool_idx_result(result: Result<usize>, bucket: &str, object: &str) -> Result<usize> {
        result.map_err(|err| Error::other(format!("failed to select decommission target pool for {bucket}/{object}: {err}")))
    }

    fn resolve_decommission_tiered_object_result(result: Result<()>, bucket: &str, object: &str) -> Result<()> {
        result.map_err(|err| Error::other(format!("failed to decommission tiered object for {bucket}/{object}: {err}")))
    }

    #[instrument(skip(self, fi, opts))]
    pub(crate) async fn decommission_tiered_object(
        &self,
        bucket: &str,
        object: &str,
        fi: &rustfs_filemeta::FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        check_put_object_args(bucket, object)?;

        let mut opts = opts.clone();
        let bucket_incarnation_fence = if is_meta_bucketname(bucket) {
            None
        } else {
            let expected = opts
                .expected_bucket_incarnation_id
                .ok_or_else(|| Error::other("tiered data movement is missing its bucket incarnation snapshot"))?;
            let guard = self.acquire_bucket_incarnation_fence(bucket, expected).await?;
            if let Some(namespace_guard) = guard.namespace_lock_guard() {
                opts.add_bucket_lifecycle_lock_guard(namespace_guard);
            }
            Some(guard)
        };

        let logical_object = object;
        let object = encode_dir_object(logical_object);
        if self.single_pool() {
            return Self::resolve_decommission_tiered_object_result(
                Err(Error::other("single pool deployments cannot decommission tiered objects")),
                bucket,
                &object,
            );
        }

        let idx = if opts.data_movement && opts.version_id.is_some() {
            Self::resolve_decommission_target_pool_idx_result(
                self.select_data_movement_pool_idx(bucket, &object, fi.size, &opts, true)
                    .await,
                bucket,
                &object,
            )?
        } else {
            Self::resolve_decommission_target_pool_idx_result(
                self.get_pool_idx_no_lock(bucket, &object, fi.size).await,
                bucket,
                &object,
            )?
        };
        let _object_guards = self
            .acquire_data_movement_object_write_locks(bucket, &object, opts.src_pool_idx, idx, &mut opts)
            .await?;
        let source_pool = self
            .pools
            .get(opts.src_pool_idx)
            .ok_or_else(|| Error::other(format!("invalid tiered data movement source pool {}", opts.src_pool_idx)))?;
        let source_versions = source_pool
            .get_disks_by_key(&object)
            .load_file_info_versions_exact(bucket, logical_object)
            .await?;
        let current_source = source_versions
            .as_ref()
            .and_then(|versions| {
                versions
                    .versions
                    .iter()
                    .find(|current| current.version_id == fi.version_id && !current.tier_free_version())
            })
            .ok_or_else(|| to_object_err(StorageError::FileNotFound, vec![bucket, object.as_str()]))?;
        if !tiered_data_movement_source_matches(fi, current_source)? {
            return Err(to_object_err(StorageError::FileNotFound, vec![bucket, object.as_str()]));
        }
        let mut fi = current_source.clone();
        if opts.data_movement {
            crate::data_movement::prepare_tiered_data_movement_file_info(&mut fi)?;
        }
        if opts.data_movement && idx == opts.src_pool_idx {
            let resume_target_pool_idx = self
                .get_available_pool_idx_excluding(bucket, &object, fi.size, opts.src_pool_idx)
                .await;
            let target_pool_idx = resolve_data_movement_resume_target_pool(idx, resume_target_pool_idx, opts.src_pool_idx);
            if self
                .has_equivalent_data_movement_tiered_object(bucket, &object, &fi, &opts, target_pool_idx)
                .await?
            {
                return Ok(());
            }

            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            ));
        }

        let result = self.pools[idx]
            .get_disks_by_key(&object)
            .decommission_tiered_object(bucket, &object, &fi, &opts)
            .await;
        if matches!(result, Err(Error::PreconditionFailed)) {
            if self
                .has_equivalent_data_movement_tiered_object(bucket, &object, &fi, &opts, idx)
                .await?
            {
                return Ok(());
            }
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object,
                opts.version_id.clone().unwrap_or_default(),
            ));
        }
        if bucket_incarnation_fence.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(Error::other("tiered data movement bucket incarnation fence was lost during target write"));
        }
        Self::resolve_decommission_tiered_object_result(result, bucket, &object)
    }

    #[instrument(level = "debug", skip(self, h))]
    #[hotpath::measure(impl_type = "ECStore")]
    pub(super) async fn handle_get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        check_get_obj_args(bucket, object)?;

        let object = rustfs_utils::path::encode_dir_object_ref(object);
        let mut opts = opts.clone();
        let read_lock_guard = self
            .acquire_object_read_lock_if_needed("get_object", bucket, &object, &mut opts)
            .await?;

        let reader = if self.single_pool() {
            self.pools[0]
                .get_object_reader(bucket, object.as_ref(), range, h, &opts)
                .await?
        } else {
            let (_, idx) = self
                .get_latest_accessible_object_info_with_idx(bucket, &object, &opts)
                .await?;
            self.pools[idx]
                .get_object_reader(bucket, object.as_ref(), range, h, &opts)
                .await?
        };

        Ok(Self::attach_read_lock_guard(reader, read_lock_guard))
    }

    async fn prepare_put_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<(String, ObjectOptions)> {
        check_put_object_args(bucket, object)?;

        let object = encode_dir_object(object);
        let mut opts = opts.clone();
        if !is_meta_bucketname(bucket) && opts.expected_bucket_incarnation_id.is_none() {
            opts.expected_bucket_incarnation_id = Some(self.bucket_incarnation_id(bucket).await?);
        }
        if opts.overwrites_existing_version() && !is_meta_bucketname(bucket) {
            let expected_incarnation_id = opts
                .expected_bucket_incarnation_id
                .ok_or_else(|| Error::other("destructive PUT is missing its bucket incarnation"))?;
            if opts.object_lock_config_snapshot.is_none() {
                opts.object_lock_config_snapshot = Some(self.object_lock_config_snapshot(bucket).await?);
            }
            let snapshot = match opts.object_lock_config_snapshot.as_ref() {
                Some(snapshot) if snapshot.is_valid_for_destructive_put(self.id, bucket, expected_incarnation_id) => {
                    Arc::clone(snapshot)
                }
                _ => {
                    return Err(Error::other(
                        "Object Lock snapshot does not hold valid target bucket generation and configuration fences",
                    ));
                }
            };
            snapshot.add_lock_fences(&mut opts);
        }
        Ok((object, opts))
    }

    async fn select_put_object_pool_idx(&self, bucket: &str, object: &str, size: i64, opts: &ObjectOptions) -> Result<usize> {
        if self.single_pool() {
            return Ok(0);
        }

        let idx = if opts.data_movement && opts.version_id.is_some() {
            self.select_data_movement_pool_idx(bucket, object, size, opts, false).await?
        } else if opts.no_lock {
            self.get_pool_idx_no_lock(bucket, object, size).await?
        } else {
            self.get_pool_idx(bucket, object, size).await?
        };

        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            ));
        }
        Ok(idx)
    }

    pub(crate) async fn put_object_for_data_movement(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
        mutation_fence: Option<&ObjectLockDiagGuard>,
    ) -> Result<(usize, Result<ObjectInfo>)> {
        if !opts.data_movement {
            return Err(Error::other("data movement PUT requires data_movement options"));
        }
        let (object, mut opts) = self.prepare_put_object(bucket, object, opts).await?;
        let idx = self
            .select_put_object_pool_idx(bucket, object.as_str(), data.size(), &opts)
            .await?;
        self.apply_decommission_target_mutation_fence(idx, object.as_str(), &mut opts, mutation_fence)
            .await;
        let result = self.pools[idx]
            .put_object_with_old_current_size(bucket, &object, data, &opts)
            .await
            .map(|(object_info, _)| object_info);
        let result = enqueue_transition_after_write(result, LcEventSrc::S3PutObject).await;
        if result.is_ok() {
            list_objects::observe_list_objects_mutation(self, bucket).await;
        }
        Ok((idx, result))
    }

    #[instrument(level = "debug", skip(self, data))]
    #[hotpath::measure(impl_type = "ECStore")]
    pub(super) async fn handle_put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<(ObjectInfo, Option<OldCurrentSize>)> {
        let (object, opts) = self.prepare_put_object(bucket, object, opts).await?;
        let idx = self
            .select_put_object_pool_idx(bucket, object.as_str(), data.size(), &opts)
            .await?;

        // Keep PUT atomic-read friendly: SetDisks takes the object write lock only
        // around precondition checks and the final rename/commit.
        self.pools[idx]
            .put_object_with_old_current_size(bucket, &object, data, &opts)
            .await
    }

    #[instrument(level = "trace", skip(self))]
    pub(super) async fn handle_get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        check_object_args(bucket, object)?;

        let object = encode_dir_object(object);
        let mut opts = opts.clone();
        let _object_lock_guard = self
            .acquire_object_read_lock_if_needed("get_object_info", bucket, &object, &mut opts)
            .await?;

        let info = if self.single_pool() {
            self.pools[0].get_object_info(bucket, object.as_str(), &opts).await?
        } else {
            self.get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &opts)
                .await?
                .0
        };
        opts.precondition_check(&info)?;
        Ok(info)
    }

    #[instrument(skip(self))]
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        check_copy_obj_args(src_bucket, src_object)?;
        check_copy_obj_args(dst_bucket, dst_object)?;

        let src_object = encode_dir_object(src_object);
        let dst_object = encode_dir_object(dst_object);

        let cp_src_dst_same = path_join_buf(&[src_bucket, &src_object]) == path_join_buf(&[dst_bucket, &dst_object]);

        let mut dst_opts = dst_opts.clone();
        if !is_meta_bucketname(dst_bucket) && dst_opts.expected_bucket_incarnation_id.is_none() {
            dst_opts.expected_bucket_incarnation_id = Some(self.bucket_incarnation_id(dst_bucket).await?);
        }
        let _bucket_lifecycle_guard = if is_meta_bucketname(dst_bucket) || dst_opts.bucket_lifecycle_lock_fence.is_some() {
            None
        } else {
            Some(self.acquire_bucket_lifecycle_read_lock(dst_bucket).await?)
        };
        let current_bucket_incarnation_id = if let Some(guard) = _bucket_lifecycle_guard.as_ref() {
            dst_opts.add_bucket_lifecycle_lock_guard(guard);
            let current_incarnation_id = get_bucket_incarnation_id_in(&self.ctx, dst_bucket).await?;
            if dst_opts
                .expected_bucket_incarnation_id
                .is_some_and(|expected| expected != current_incarnation_id)
            {
                return Err(StorageError::BucketNotFound(dst_bucket.to_string()));
            }
            Some(current_incarnation_id)
        } else {
            dst_opts.expected_bucket_incarnation_id
        };
        if dst_opts
            .bucket_lifecycle_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
        {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "copy_object_bucket_generation",
                bucket: dst_bucket.to_string(),
                object: dst_object.clone(),
                required: 1,
                achieved: 0,
            });
        }
        if dst_opts.overwrites_existing_version() && !is_meta_bucketname(dst_bucket) {
            let incarnation_id =
                current_bucket_incarnation_id.ok_or_else(|| Error::other("copy is missing its bucket incarnation snapshot"))?;
            let lifecycle_fence = dst_opts
                .bucket_lifecycle_lock_fence
                .as_ref()
                .ok_or_else(|| Error::other("copy is missing its bucket lifecycle fence"))?;
            let snapshot = match dst_opts.object_lock_config_snapshot.as_ref() {
                Some(snapshot) => Arc::clone(snapshot),
                None => {
                    self.object_lock_config_snapshot_under_lifecycle_fence(dst_bucket, lifecycle_fence)
                        .await?
                }
            };
            if !snapshot.is_valid_for_destructive_put(self.id, dst_bucket, incarnation_id) {
                return Err(Error::other("copy Object Lock snapshot does not match the target bucket generation"));
            }
            snapshot.add_lock_fences(&mut dst_opts);
            dst_opts.object_lock_config_snapshot = Some(snapshot);
        }
        let _dst_lock_guard = if cp_src_dst_same && dst_opts.expected_current_version_id.is_none() {
            self.acquire_object_write_lock_if_needed("copy_object", dst_bucket, &dst_object, &mut dst_opts)
                .await?
        } else {
            None
        };

        if cp_src_dst_same {
            let pool_idx = self
                .get_pool_info_existing_with_opts(src_bucket, &src_object, &writer_pool_lookup_opts(src_opts, true))
                .await?
                .0
                .index;

            if let (Some(src_vid), Some(dst_vid)) = (&src_opts.version_id, &dst_opts.version_id)
                && src_vid == dst_vid
            {
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, &dst_opts)
                    .await;
            }

            if !dst_opts.versioned && src_opts.version_id.is_none() {
                if src_info.metadata_only {
                    // Zero-copy update: only xl.meta is rewritten, the data blocks stay as they
                    // are. The caller must therefore guarantee that the destination metadata
                    // still describes the stored bytes. In particular a copy that re-derives
                    // encryption material may NOT set metadata_only — that would leave a fresh
                    // DEK beside ciphertext sealed under the old one, permanently destroying the
                    // object. The S3 handler enforces this before calling in (see the
                    // metadata_only decision in rustfs/src/app/object_usecase.rs); the sibling
                    // versioned branch below resolves the same risk by rewriting through
                    // put_object (issue #4238).
                    return self.pools[pool_idx]
                        .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, &dst_opts)
                        .await;
                }
                // Transitioned object self-copy: restore from tier into the same pool.
                let put_opts = ObjectOptions {
                    user_defined: (*src_info.user_defined).clone(),
                    versioned: dst_opts.versioned,
                    version_id: dst_opts.version_id.clone(),
                    no_lock: dst_opts.no_lock,
                    mod_time: dst_opts.mod_time,
                    http_preconditions: dst_opts.http_preconditions.clone(),
                    expected_current_version_id: dst_opts.expected_current_version_id.clone(),
                    expected_bucket_incarnation_id: dst_opts.expected_bucket_incarnation_id,
                    namespace_lock_fence: dst_opts.namespace_lock_fence.clone(),
                    bucket_lifecycle_lock_fence: dst_opts.bucket_lifecycle_lock_fence.clone(),
                    object_lock_config_snapshot: dst_opts.object_lock_config_snapshot.clone(),
                    ..Default::default()
                };
                return if let Some(reader) = src_info.put_object_reader.as_mut() {
                    self.pools[pool_idx]
                        .put_object(dst_bucket, &dst_object, reader, &put_opts)
                        .await
                } else {
                    Err(StorageError::InvalidArgument(
                        src_bucket.to_owned(),
                        src_object.to_owned(),
                        "put_object_reader is none".to_owned(),
                    ))
                };
            }

            if dst_opts.versioned && src_opts.version_id != dst_opts.version_id {
                // Restoring a specific historical version onto the current key creates a NEW
                // version. When the caller supplies a reader (S3 CopyObject), write the fetched
                // bytes through put_object so any re-encryption/compression applied to the reader
                // stays consistent with the new version's metadata. Sharing the source data_dir via
                // a metadata-only version copy would corrupt SSE/compressed objects (issue #4238).
                if let Some(reader) = src_info.put_object_reader.as_mut() {
                    let put_opts = ObjectOptions {
                        user_defined: (*src_info.user_defined).clone(),
                        versioned: dst_opts.versioned,
                        version_id: dst_opts.version_id.clone(),
                        no_lock: dst_opts.no_lock,
                        mod_time: dst_opts.mod_time,
                        http_preconditions: dst_opts.http_preconditions.clone(),
                        expected_current_version_id: dst_opts.expected_current_version_id.clone(),
                        expected_bucket_incarnation_id: dst_opts.expected_bucket_incarnation_id,
                        namespace_lock_fence: dst_opts.namespace_lock_fence.clone(),
                        bucket_lifecycle_lock_fence: dst_opts.bucket_lifecycle_lock_fence.clone(),
                        object_lock_config_snapshot: dst_opts.object_lock_config_snapshot.clone(),
                        ..Default::default()
                    };
                    return self.pools[pool_idx]
                        .put_object(dst_bucket, &dst_object, reader, &put_opts)
                        .await;
                }
                src_info.version_only = true;
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, &dst_opts)
                    .await;
            }
        }

        let pool_idx = if dst_opts.no_lock {
            self.get_pool_idx_no_lock(dst_bucket, &dst_object, src_info.size).await?
        } else {
            self.get_pool_idx(dst_bucket, &dst_object, src_info.size).await?
        };

        let put_opts = ObjectOptions {
            user_defined: (*src_info.user_defined).clone(),
            versioned: dst_opts.versioned,
            version_id: dst_opts.version_id.clone(),
            no_lock: dst_opts.no_lock,
            mod_time: dst_opts.mod_time,
            http_preconditions: dst_opts.http_preconditions.clone(),
            expected_current_version_id: dst_opts.expected_current_version_id.clone(),
            expected_bucket_incarnation_id: dst_opts.expected_bucket_incarnation_id,
            namespace_lock_fence: dst_opts.namespace_lock_fence.clone(),
            bucket_lifecycle_lock_fence: dst_opts.bucket_lifecycle_lock_fence.clone(),
            object_lock_config_snapshot: dst_opts.object_lock_config_snapshot.clone(),
            ..Default::default()
        };

        if let Some(put_object_reader) = src_info.put_object_reader.as_mut() {
            return self.pools[pool_idx]
                .put_object(dst_bucket, &dst_object, put_object_reader, &put_opts)
                .await;
        }

        Err(StorageError::InvalidArgument(
            src_bucket.to_owned(),
            src_object.to_owned(),
            "put_object_reader is none".to_owned(),
        ))
    }

    /// Best-effort purge of an orphan directory prefix — an on-disk tree of empty
    /// directories with no `xl.meta` anywhere (issue #4189). Orphan fragments can sit
    /// on any erasure set of any pool (they are left behind by whichever sets stored
    /// the now-deleted children), so every set is swept. Returns true when at least
    /// one set removed an orphan tree. Hard per-set failures are logged and skipped:
    /// the caller falls back to surfacing the original NotFound.
    async fn purge_orphan_dir_object(&self, bucket: &str, object: &str) -> bool {
        let prefix = decode_dir_object(object);
        let mut purged = false;
        for pool in self.pools.iter() {
            for set in pool.disk_set.iter() {
                match set.purge_orphan_dir_object(bucket, &prefix).await {
                    Ok(set_purged) => purged |= set_purged,
                    Err(err) => {
                        warn!(
                            bucket,
                            prefix,
                            pool_index = pool.pool_idx,
                            error = ?err,
                            "failed to purge orphan directory prefix"
                        );
                    }
                }
            }
        }
        purged
    }

    pub async fn delete_object_with_tier_delete_journal(
        self: &Arc<Self>,
        bucket: &str,
        object: &str,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        let result = self
            .handle_delete_object_with_journal(bucket, object, opts, Some(Arc::clone(self)))
            .await;
        if result.is_ok() {
            list_objects::observe_list_objects_mutation(self, bucket).await;
        }
        result
    }

    pub async fn delete_objects_with_tier_delete_journal(
        self: &Arc<Self>,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        let result = self
            .handle_delete_objects_with_journal(bucket, objects, opts, Some(Arc::clone(self)))
            .await;
        let success_count = result.1.iter().filter(|err| err.is_none()).count();
        if success_count > 0 {
            list_objects::observe_list_objects_mutations(self, bucket, success_count).await;
        }
        result
    }

    pub async fn delete_objects_with_tier_delete_journal_and_accounting(
        self: &Arc<Self>,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>, Vec<Option<DeleteAccounting>>) {
        let result = self
            .handle_delete_objects_with_journal_and_accounting(bucket, objects, opts, Some(Arc::clone(self)))
            .await;
        let success_count = result.1.iter().filter(|err| err.is_none()).count();
        if success_count > 0 {
            list_objects::observe_list_objects_mutations(self, bucket, success_count).await;
        }
        result
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        self.handle_delete_object_with_journal(bucket, object, opts, None).await
    }

    pub(super) async fn handle_delete_object_with_journal(
        &self,
        bucket: &str,
        object: &str,
        opts: ObjectOptions,
        tier_journal_api: Option<Arc<ECStore>>,
    ) -> Result<ObjectInfo> {
        check_del_obj_args(bucket, object)?;

        if opts.lifecycle_delete_all.is_some() && self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("lifecycle delete-all requires namespace locking"));
        }

        let _bucket_lifecycle_guard = if is_meta_bucketname(bucket) {
            None
        } else if opts.delete_prefix {
            Some(self.acquire_bucket_lifecycle_write_lock(bucket).await?)
        } else {
            Some(self.acquire_bucket_lifecycle_read_lock(bucket).await?)
        };
        let object = if opts.delete_prefix && !opts.delete_prefix_object {
            object.to_owned()
        } else {
            encode_dir_object(object)
        };
        let object = object.as_str();
        let mut opts = opts;
        let delete_all_configs = if opts.lifecycle_delete_all.is_some() {
            Some(get_expiry_configs(self, bucket).await?)
        } else {
            None
        };
        opts.tier_delete_journal_api = tier_journal_api.clone();
        if let Some(guard) = _bucket_lifecycle_guard.as_ref() {
            opts.add_bucket_lifecycle_lock_guard(guard);
        }

        if !is_meta_bucketname(bucket) {
            get_cached_bucket_incarnation_id_in(&self.ctx, bucket).await?;
        }
        let _object_lock_metadata_guard = if !is_meta_bucketname(bucket) {
            Some(acquire_bucket_metadata_transaction_read_lock_in(&self.ctx, bucket).await?)
        } else {
            None
        };
        if let Some(guard) = _object_lock_metadata_guard.as_ref() {
            opts.add_namespace_lock_guard(guard);
        }
        let current_bucket_incarnation_id = if _object_lock_metadata_guard.is_some() {
            let (state, incarnation_id, config_revision) =
                get_object_lock_config_and_incarnation_from_disk_in(&self.ctx, bucket).await?;
            opts.object_lock_config_snapshot = Some(Arc::new(ObjectLockConfigSnapshot::for_store_bucket(
                self.id,
                bucket,
                incarnation_id,
                config_revision,
                state,
            )));
            Some(incarnation_id)
        } else {
            None
        };
        if let (Some(expected), Some(current)) = (opts.expected_bucket_incarnation_id, current_bucket_incarnation_id)
            && expected != current
        {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        #[cfg(test)]
        pause_delete_after_object_lock_snapshot(bucket).await;

        if opts.delete_prefix && !opts.delete_prefix_object {
            // Prefix deletes cover multiple object keys; an exact lock on the prefix string
            // would not protect child objects.
            if !is_meta_bucketname(bucket) {
                let state = opts
                    .object_lock_config_snapshot
                    .as_deref()
                    .ok_or_else(|| Error::other("recursive delete is missing its Object Lock configuration snapshot"))?
                    .state();
                ensure_recursive_force_delete_allowed_for_state(bucket, state)?;
                let bypass_governance = opts
                    .object_lock_delete
                    .as_ref()
                    .is_some_and(|delete_opts| delete_opts.bypass_governance);
                for pool in &self.pools {
                    for set in &pool.disk_set {
                        let mut marker = None;
                        let mut version_marker = None;
                        loop {
                            let page = set
                                .clone()
                                .inner_list_object_versions_for_recursive_delete(
                                    bucket,
                                    object,
                                    marker.clone(),
                                    version_marker.clone(),
                                    RECURSIVE_DELETE_VERSION_SCAN_PAGE_SIZE,
                                )
                                .await?;
                            for object_info in &page.objects {
                                if check_object_lock_for_deletion_with_state(state, object_info, bypass_governance)?.is_some() {
                                    return Err(StorageError::PrefixAccessDenied(bucket.to_string(), object_info.name.clone()));
                                }
                            }
                            if !page.is_truncated {
                                break;
                            }
                            let next_marker = page.next_marker.ok_or_else(|| {
                                Error::other("recursive delete version scan did not return a continuation marker")
                            })?;
                            if marker.as_ref() == Some(&next_marker) && version_marker == page.next_version_idmarker {
                                return Err(Error::other("recursive delete version scan did not advance"));
                            }
                            marker = Some(next_marker);
                            version_marker = page.next_version_idmarker;
                        }
                    }
                }
            }
            delete_prefix_with_tier_delete_journal(self, bucket, object, &opts, tier_journal_api.as_ref()).await?;
            return Ok(ObjectInfo::default());
        }

        let _object_lock_guard = if opts.expected_current_version_id.is_none() {
            self.acquire_object_write_lock_if_needed("delete_object", bucket, object, &mut opts)
                .await?
        } else {
            None
        };
        #[cfg(test)]
        if _object_lock_guard.is_some() {
            notify_delete_namespace_acquired(bucket);
        }
        if let Some(trigger) = opts.lifecycle_delete_all.as_ref() {
            let configs = delete_all_configs.as_ref().ok_or(StorageError::PreconditionFailed)?;
            let expected_bucket_incarnation_id = opts.expected_bucket_incarnation_id.ok_or(StorageError::PreconditionFailed)?;
            if configs.table_bucket_enabled || configs.bucket_incarnation_id != expected_bucket_incarnation_id {
                return Err(StorageError::PreconditionFailed);
            }
            let lifecycle = configs.lifecycle.as_ref().ok_or(StorageError::PreconditionFailed)?;
            let (mut current, _) = self
                .get_latest_object_info_with_idx(
                    bucket,
                    object,
                    &ObjectOptions {
                        no_lock: true,
                        metadata_cache_safe: false,
                        ..Default::default()
                    },
                )
                .await?;
            let current_version_id = current.version_id.filter(|version_id| !version_id.is_nil());
            if current_version_id != trigger.version_id || current.delete_marker != trigger.delete_marker {
                return Err(StorageError::PreconditionFailed);
            }
            current.name = decode_dir_object(&current.name);
            let current_event = eval_action_from_lifecycle(lifecycle, configs.object_lock.as_deref(), &current).await;
            if current_event.action != trigger.action || current_event.rule_id != trigger.rule_id {
                return Err(StorageError::PreconditionFailed);
            }
        }
        if opts.delete_prefix {
            delete_prefix_with_tier_delete_journal(self, bucket, object, &opts, tier_journal_api.as_ref()).await?;
            return Ok(ObjectInfo::default());
        }

        let gopts = delete_pool_lookup_opts(&opts, true);

        if opts.data_movement {
            let existing_pool_info = self.get_pool_info_existing_with_opts(bucket, object, &gopts).await;
            let existing_pool_idx = existing_pool_info
                .as_ref()
                .map(|(pinfo, _)| pinfo.index)
                .map_err(Clone::clone);
            let selected_target_pool_idx =
                match select_data_movement_target_pool(existing_pool_idx, opts.src_pool_idx, opts.delete_marker)? {
                    Some(pool_idx) => pool_idx,
                    None => self.get_pool_idx_no_lock(bucket, object, 0).await?,
                };
            let resume_target_pool_idx = if selected_target_pool_idx == opts.src_pool_idx {
                self.get_available_pool_idx_excluding(bucket, object, 0, opts.src_pool_idx)
                    .await
            } else {
                None
            };
            let target_pool_idx =
                resolve_data_movement_resume_target_pool(selected_target_pool_idx, resume_target_pool_idx, opts.src_pool_idx);
            let mut delete_marker_target_opts = None;

            if opts.delete_marker && should_check_data_movement_resume_target(opts.src_pool_idx, target_pool_idx) {
                let source = self
                    .find_data_movement_target_info(bucket, object, opts.src_pool_idx, &opts)
                    .await?;
                let Some(source) = source else {
                    return Err(StorageError::DataMovementOverwriteErr(
                        bucket.to_owned(),
                        object.to_owned(),
                        opts.version_id.unwrap_or_default(),
                    ));
                };
                if !is_expected_data_movement_delete_marker_source(&source, opts.mod_time) {
                    return Err(StorageError::DataMovementOverwriteErr(
                        bucket.to_owned(),
                        object.to_owned(),
                        opts.version_id.unwrap_or_default(),
                    ));
                }
                let Some(target_opts) = current_data_movement_delete_marker_opts(&source, &opts) else {
                    return Err(StorageError::DataMovementOverwriteErr(
                        bucket.to_owned(),
                        object.to_owned(),
                        opts.version_id.unwrap_or_default(),
                    ));
                };
                let target = self
                    .find_data_movement_target_info(bucket, object, target_pool_idx, &target_opts)
                    .await?;
                if let Some(target) = target {
                    if is_equivalent_data_movement_delete_marker(&source, &target) {
                        let mut target = target;
                        target.name = decode_dir_object(object);
                        return Ok(target);
                    }
                    return Err(StorageError::DataMovementOverwriteErr(
                        bucket.to_owned(),
                        object.to_owned(),
                        opts.version_id.unwrap_or_default(),
                    ));
                }
                delete_marker_target_opts = Some(target_opts);
            }

            if !should_check_data_movement_resume_target(opts.src_pool_idx, target_pool_idx) {
                if let Ok((source_pool_info, _)) = existing_pool_info
                    && opts.delete_marker
                    && is_data_movement_delete_marker(&source_pool_info.object_info)
                    && self
                        .has_equivalent_data_movement_delete_marker(
                            bucket,
                            object,
                            &source_pool_info.object_info,
                            &opts,
                            target_pool_idx,
                        )
                        .await?
                {
                    let mut obj = source_pool_info.object_info;
                    obj.name = decode_dir_object(object);
                    return Ok(obj);
                }

                return Err(StorageError::DataMovementOverwriteErr(
                    bucket.to_owned(),
                    object.to_owned(),
                    opts.version_id.unwrap_or_default(),
                ));
            }

            let target_opts = delete_marker_target_opts.unwrap_or(opts);
            let mut obj = self.pools[target_pool_idx].delete_object(bucket, object, target_opts).await?;
            obj.name = decode_dir_object(obj.name.as_str());
            return Ok(obj);
        }

        // Determine which pool contains it
        let (mut pinfo, errs) = match self.get_pool_info_existing_with_opts(bucket, object, &gopts).await {
            Ok(res) => res,
            Err(err) if is_err_read_quorum(&err) => return Err(StorageError::ErasureWriteQuorum),
            Err(err) if is_err_object_not_found(&err) && should_create_delete_marker_for_missing_object(&opts) => {
                let target_pool_idx = self.get_pool_idx_no_lock(bucket, object, 0).await?;
                let mut obj = self.pools[target_pool_idx].delete_object(bucket, object, opts).await?;
                #[cfg(test)]
                pause_versioned_delete_marker_after_commit(bucket, object).await;
                obj.name = decode_dir_object(object);
                return Ok(obj);
            }
            Err(err) => {
                // A folder key (`prefix/`) with no object metadata may still exist on
                // disk as an orphan empty-directory tree (issue #4189): listings show
                // it as a common prefix, but no regular delete path can remove it.
                // Purge the orphan tree so folder deletes actually take effect.
                if should_purge_orphan_dir_on_missing(&err, object) && self.purge_orphan_dir_object(bucket, object).await {
                    return Ok(ObjectInfo {
                        bucket: bucket.to_owned(),
                        name: decode_dir_object(object),
                        ..Default::default()
                    });
                }
                return Err(err);
            }
        };

        if pinfo.object_info.delete_marker && opts.version_id.is_none() {
            pinfo.object_info.name = decode_dir_object(object);
            return Ok(pinfo.object_info);
        }

        if opts.data_movement && opts.src_pool_idx == pinfo.index {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.unwrap_or_default(),
            ));
        }

        let journal_entry = if let Some(api) = tier_journal_api.as_ref() {
            prepare_tier_delete_journal_entry(api, bucket, object, &opts, &pinfo.object_info).await?
        } else {
            None
        };

        if should_delete_from_all_pools(&opts, errs.len()) {
            let mut obj = match self.delete_object_from_all_pools(bucket, object, &opts, errs).await {
                Ok(obj) => obj,
                Err(err) => {
                    if let (Some(api), Some(je)) = (tier_journal_api.as_ref(), journal_entry.as_ref()) {
                        abort_prepared_tier_delete_journal_entry(api, je).await;
                    }
                    return Err(err);
                }
            };
            if let (Some(api), Some(je)) = (tier_journal_api.as_ref(), journal_entry.as_ref()) {
                commit_prepared_tier_delete_journal_entry(api, je).await;
            }
            obj.name = decode_dir_object(object);
            return Ok(obj);
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await || self.is_pool_rebalancing(pool.pool_idx).await {
                continue;
            }

            match pool.delete_object(bucket, object, opts.clone()).await {
                Ok(res) => {
                    #[cfg(test)]
                    pause_versioned_delete_marker_after_commit(bucket, object).await;
                    if let (Some(api), Some(je)) = (tier_journal_api.as_ref(), journal_entry.as_ref()) {
                        commit_prepared_tier_delete_journal_entry(api, je).await;
                    }
                    let mut obj = res;
                    obj.name = decode_dir_object(object);
                    return Ok(obj);
                }
                Err(err) => {
                    if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                        return Err(err);
                    }
                }
            }
        }

        if let (Some(api), Some(je)) = (tier_journal_api.as_ref(), journal_entry.as_ref()) {
            abort_prepared_tier_delete_journal_entry(api, je).await;
        }

        if let Some(ver) = opts.version_id {
            return Err(StorageError::VersionNotFound(bucket.to_owned(), object.to_owned(), ver));
        }

        Err(StorageError::ObjectNotFound(bucket.to_owned(), object.to_owned()))
    }

    #[instrument(skip(self, objects, opts))]
    pub(super) async fn handle_delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        self.handle_delete_objects_with_journal(bucket, objects, opts, None).await
    }

    pub(super) async fn handle_delete_objects_with_journal(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
        tier_journal_api: Option<Arc<ECStore>>,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        let (deleted, errors, _) = self
            .handle_delete_objects_with_journal_and_accounting(bucket, objects, opts, tier_journal_api)
            .await;
        (deleted, errors)
    }

    pub(super) async fn handle_delete_objects_with_journal_and_accounting(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
        tier_journal_api: Option<Arc<ECStore>>,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>, Vec<Option<DeleteAccounting>>) {
        // encode object name
        let objects: Vec<ObjectToDelete> = objects
            .iter()
            .map(|v| {
                let mut v = v.clone();
                v.object_name = encode_dir_object(v.object_name.as_str());
                v
            })
            .collect();

        // Default return value
        let mut del_objects = vec![DeletedObject::default(); objects.len()];
        let mut accounting = vec![None; objects.len()];

        let mut del_errs = Vec::with_capacity(objects.len());
        for _ in 0..objects.len() {
            del_errs.push(None)
        }

        let mut opts = opts;
        opts.tier_delete_journal_api = tier_journal_api;
        let _bucket_lifecycle_guard = if is_meta_bucketname(bucket) {
            None
        } else {
            match self.acquire_bucket_lifecycle_read_lock(bucket).await {
                Ok(guard) => Some(guard),
                Err(err) => return return_batch_delete_lock_error_with_accounting(objects.as_slice(), err),
            }
        };
        if let Some(guard) = _bucket_lifecycle_guard.as_ref() {
            opts.add_bucket_lifecycle_lock_guard(guard);
        }
        if opts.delete_replication_config_snapshot.is_none() {
            match ReplicationObjectBridge::delete_request_config_in(&self.ctx, bucket).await {
                Ok(snapshot) => opts.delete_replication_config_snapshot = Some(Arc::new(snapshot)),
                Err(err) => {
                    let message = err.to_string();
                    let errors = (0..objects.len()).map(|_| Some(Error::other(message.clone()))).collect();
                    return (del_objects, errors, accounting);
                }
            }
        }
        if !is_meta_bucketname(bucket)
            && let Err(err) = get_cached_bucket_incarnation_id_in(&self.ctx, bucket).await
        {
            return return_batch_delete_lock_error_with_accounting(objects.as_slice(), err);
        }
        let _object_lock_metadata_guard = if is_meta_bucketname(bucket) {
            None
        } else {
            Some(match acquire_bucket_metadata_transaction_read_lock_in(&self.ctx, bucket).await {
                Ok(guard) => guard,
                Err(err) => return return_batch_delete_lock_error_with_accounting(objects.as_slice(), err),
            })
        };
        if let Some(guard) = _object_lock_metadata_guard.as_ref() {
            opts.add_namespace_lock_guard(guard);
        }
        let current_bucket_incarnation_id = if _object_lock_metadata_guard.is_some() {
            let (state, incarnation_id, config_revision) =
                match get_object_lock_config_and_incarnation_from_disk_in(&self.ctx, bucket).await {
                    Ok(snapshot) => snapshot,
                    Err(err) => return return_batch_delete_lock_error_with_accounting(objects.as_slice(), err),
                };
            opts.object_lock_config_snapshot = Some(Arc::new(ObjectLockConfigSnapshot::for_store_bucket(
                self.id,
                bucket,
                incarnation_id,
                config_revision,
                state,
            )));
            Some(incarnation_id)
        } else {
            None
        };
        if let (Some(expected), Some(current)) = (opts.expected_bucket_incarnation_id, current_bucket_incarnation_id)
            && expected != current
        {
            return return_batch_delete_lock_error_with_accounting(
                objects.as_slice(),
                StorageError::BucketNotFound(bucket.to_string()),
            );
        }
        #[cfg(test)]
        if current_bucket_incarnation_id.is_some() {
            pause_delete_after_object_lock_snapshot(bucket).await;
        }
        let _object_lock_guards = match self.acquire_delete_objects_write_locks(bucket, &objects, &mut opts).await {
            Ok(guards) => guards,
            Err(err) => return return_batch_delete_lock_error_with_accounting(objects.as_slice(), err),
        };
        #[cfg(test)]
        if !_object_lock_guards.is_empty() {
            notify_delete_namespace_acquired(bucket);
        }

        let delete_config_snapshot = opts
            .delete_replication_config_snapshot
            .as_deref()
            .expect("batch delete replication config snapshot should be loaded");
        let latest_marker_objects = objects
            .iter()
            .map(|object| batch_delete_creates_latest_marker(object, delete_config_snapshot))
            .collect::<Vec<_>>();
        let marker_target_results = join_all(objects.iter().zip(&latest_marker_objects).map(
            |(object, creates_marker)| async move {
                if *creates_marker {
                    Some(self.get_pool_idx_no_lock(bucket, &object.object_name, 0).await)
                } else {
                    None
                }
            },
        ))
        .await;
        let mut marker_target_pool_indices = Vec::with_capacity(objects.len());
        for (idx, target_result) in marker_target_results.into_iter().enumerate() {
            match target_result {
                Some(Ok(pool_idx)) => marker_target_pool_indices.push(Some(pool_idx)),
                Some(Err(err)) => {
                    del_errs[idx] = Some(err);
                    marker_target_pool_indices.push(None);
                }
                None => marker_target_pool_indices.push(None),
            }
        }

        let mut futures = Vec::with_capacity(self.pools.len());
        for pool in self.pools.iter() {
            if self.is_pool_rebalancing(pool.pool_idx).await {
                continue;
            }

            let (object_indices, pool_objects): (Vec<_>, Vec<_>) = objects
                .iter()
                .enumerate()
                .filter(|(idx, _)| {
                    batch_delete_targets_pool(latest_marker_objects[*idx], marker_target_pool_indices[*idx], pool.pool_idx)
                })
                .map(|(idx, object)| (idx, object.clone()))
                .unzip();
            if pool_objects.is_empty() {
                continue;
            }

            let pool_opts = opts.clone();
            futures.push(async move {
                #[cfg(test)]
                let pool_object_names = pool_objects
                    .iter()
                    .map(|object| object.object_name.clone())
                    .collect::<Vec<_>>();
                let result = pool.delete_objects_with_accounting(bucket, pool_objects, pool_opts).await;
                #[cfg(test)]
                let result = {
                    let mut result = result;
                    let (deleted, errors, _) = &mut result;
                    inject_batch_delete_pool_errors(bucket, pool.pool_idx, &pool_object_names, deleted, errors);
                    result
                };
                (object_indices, result)
            });
        }

        let results = join_all(futures).await;

        for idx in 0..del_objects.len() {
            let pool_results = results.iter().filter_map(|(object_indices, (dels, errs, _))| {
                let pool_object_idx = object_indices.binary_search(&idx).ok()?;
                Some((&dels[pool_object_idx], &errs[pool_object_idx]))
            });
            let (deleted, error, attempted) = resolve_batch_delete_pool_results(del_errs[idx].take(), pool_results);
            if let Some(deleted) = deleted {
                del_objects[idx] = deleted;
            }
            del_errs[idx] = error;

            if !attempted && del_errs[idx].is_none() && latest_marker_objects[idx] {
                del_objects[idx] = DeletedObject {
                    object_name: objects[idx].object_name.clone(),
                    version_id: objects[idx].version_id,
                    ..Default::default()
                };
                del_errs[idx] = Some(StorageError::ObjectNotFound(bucket.to_owned(), objects[idx].object_name.clone()));
            }
        }

        for (object_indices, (_, _, pool_accounting)) in &results {
            for (pool_object_idx, object_idx) in object_indices.iter().enumerate() {
                accounting[*object_idx] = pool_accounting.get(pool_object_idx).cloned().flatten();
            }
        }

        #[cfg(test)]
        for (idx, object) in objects.iter().enumerate() {
            if del_errs[idx].is_none() && del_objects[idx].delete_marker {
                pause_versioned_delete_marker_after_commit(bucket, &object.object_name).await;
            }
        }

        del_objects.iter_mut().for_each(|v| {
            v.object_name = decode_dir_object(&v.object_name);
        });

        (del_objects, del_errs, accounting)

        // let mut futures = Vec::with_capacity(objects.len());

        // for obj in objects.iter() {
        //     futures.push(async move {
        //         self.internal_get_pool_info_existing_with_opts(
        //             bucket,
        //             &obj.object_name,
        //             &ObjectOptions {
        //                 no_lock: true,
        //                 ..Default::default()
        //             },
        //         )
        //         .await
        //     });
        // }

        // let results = join_all(futures).await;

        // // let mut jhs = Vec::new();
        // // let semaphore = Arc::new(Semaphore::new(num_cpus::get()));
        // // let pools = Arc::new(self.pools.clone());

        // // for obj in objects.iter() {
        // //     let (semaphore, pools, bucket, object_name, opt) = (
        // //         semaphore.clone(),
        // //         pools.clone(),
        // //         bucket.to_string(),
        // //         obj.object_name.to_string(),
        // //         ObjectOptions::default(),
        // //     );

        // //     let jh = tokio::spawn(async move {
        // //         let _permit = semaphore.acquire().await.unwrap();
        // //         self.internal_get_pool_info_existing_with_opts(pools.as_ref(), &bucket, &object_name, &opt)
        // //             .await
        // //     });
        // //     jhs.push(jh);
        // // }
        // // let mut results = Vec::new();
        // // for jh in jhs {
        // //     results.push(jh.await.unwrap());
        // // }

        // // Record the mapping pool_idx -> object index
        // let mut pool_obj_idx_map = HashMap::new();
        // let mut orig_index_map = HashMap::new();

        // for (i, res) in results.into_iter().enumerate() {
        //     match res {
        //         Ok((pinfo, _)) => {
        //             if let Some(obj) = objects.get(i) {
        //                 if pinfo.object_info.delete_marker && obj.version_id.is_none() {
        //                     del_objects[i] = DeletedObject {
        //                         delete_marker: pinfo.object_info.delete_marker,
        //                         delete_marker_version_id: pinfo.object_info.version_id.map(|v| v.to_string()),
        //                         object_name: decode_dir_object(&pinfo.object_info.name),
        //                         delete_marker_mtime: pinfo.object_info.mod_time,
        //                         ..Default::default()
        //                     };
        //                     continue;
        //                 }

        //                 if !pool_obj_idx_map.contains_key(&pinfo.index) {
        //                     pool_obj_idx_map.insert(pinfo.index, vec![obj.clone()]);
        //                 } else if let Some(val) = pool_obj_idx_map.get_mut(&pinfo.index) {
        //                     val.push(obj.clone());
        //                 }

        //                 if !orig_index_map.contains_key(&pinfo.index) {
        //                     orig_index_map.insert(pinfo.index, vec![i]);
        //                 } else if let Some(val) = orig_index_map.get_mut(&pinfo.index) {
        //                     val.push(i);
        //                 }
        //             }
        //         }
        //         Err(e) => {
        //             if !is_err_object_not_found(&e) && is_err_version_not_found(&e) {
        //                 del_errs[i] = Some(e)
        //             }

        //             if let Some(obj) = objects.get(i) {
        //                 del_objects[i] = DeletedObject {
        //                     object_name: decode_dir_object(&obj.object_name),
        //                     version_id: obj.version_id.map(|v| v.to_string()),
        //                     ..Default::default()
        //                 }
        //             }
        //         }
        //     }
        // }

        // if !pool_obj_idx_map.is_empty() {
        //     for (i, sets) in self.pools.iter().enumerate() {
        //         // Retrieve the object index for a pool idx
        //         if let Some(objs) = pool_obj_idx_map.get(&i) {
        //             // Fetch the corresponding object (should never be None)
        //             // let objs: Vec<ObjectToDelete> = obj_idxs.iter().filter_map(|&idx| objects.get(idx).cloned()).collect();

        //             if objs.is_empty() {
        //                 continue;
        //             }

        //             let (pdel_objs, perrs) = sets.delete_objects(bucket, objs.clone(), opts.clone()).await?;

        //             // Insert simultaneously (should never be None)
        //             let org_indexes = orig_index_map.get(&i).unwrap();

        //             // perrs should follow the same order as obj_idxs
        //             for (i, err) in perrs.into_iter().enumerate() {
        //                 let obj_idx = org_indexes[i];

        //                 if err.is_some() {
        //                     del_errs[obj_idx] = err;
        //                 }

        //                 let mut dobj = pdel_objs.get(i).unwrap().clone();
        //                 dobj.object_name = decode_dir_object(&dobj.object_name);

        //                 del_objects[obj_idx] = dobj;
        //             }
        //         }
        //     }
        // }

        // Ok((del_objects, del_errs))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            let _ = self.pools[0].add_partial(bucket, object.as_str(), version_id).await;
            return Ok(());
        }

        let opts = ObjectOptions {
            version_id: Some(version_id.to_string()),
            ..Default::default()
        };
        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &writer_pool_lookup_opts(&opts, opts.no_lock))
            .await?;

        let _ = self.pools[idx].add_partial(bucket, object.as_str(), version_id).await;
        Ok(())
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].transition_object(bucket, &object, opts).await;
        }

        let opts = transition_restore_pool_opts(opts);
        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, &object, &opts)
            .await?;

        self.pools[idx].transition_object(bucket, &object, &opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_restore_transitioned_object(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<()> {
        let object = encode_dir_object(object);
        let mut opts = transition_restore_pool_opts(opts);
        if !is_meta_bucketname(bucket) && opts.expected_bucket_incarnation_id.is_none() {
            opts.expected_bucket_incarnation_id = Some(self.bucket_incarnation_id(bucket).await?);
        }
        let bucket_lifecycle_guard = if is_meta_bucketname(bucket) {
            None
        } else {
            Some(self.acquire_bucket_lifecycle_read_lock(bucket).await?)
        };
        if let Some(guard) = bucket_lifecycle_guard.as_ref() {
            opts.add_bucket_lifecycle_lock_guard(guard);
        }
        if !is_meta_bucketname(bucket) {
            let current_incarnation_id = get_bucket_incarnation_id_in(&self.ctx, bucket).await?;
            if opts.expected_bucket_incarnation_id != Some(current_incarnation_id) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
        }
        if opts.overwrites_existing_version() && !is_meta_bucketname(bucket) {
            let expected_incarnation_id = opts
                .expected_bucket_incarnation_id
                .ok_or_else(|| Error::other("restore is missing its bucket incarnation snapshot"))?;
            let lifecycle_fence = opts
                .bucket_lifecycle_lock_fence
                .as_ref()
                .ok_or_else(|| Error::other("restore is missing its bucket lifecycle fence"))?;
            let snapshot = match opts.object_lock_config_snapshot.as_ref() {
                Some(snapshot) => Arc::clone(snapshot),
                None => {
                    self.object_lock_config_snapshot_under_lifecycle_fence(bucket, lifecycle_fence)
                        .await?
                }
            };
            if !snapshot.is_valid_for_destructive_put(self.id, bucket, expected_incarnation_id) {
                return Err(Error::other("restore Object Lock snapshot does not match the target bucket generation"));
            }
            snapshot.add_lock_fences(&mut opts);
            opts.object_lock_config_snapshot = Some(snapshot);
        }
        // Deliberately NOT holding the object write lock across the tier
        // copy-back (backlog#1304): non-SELECT restore-vs-restore is
        // serialized by the accept path's compare-and-set of the ongoing flag
        // (see acquire_restore_accept_guard), and while ongoing-request="true"
        // the restore header parses with no expiry, so DeleteRestoredAction
        // cannot fire mid-copy-back. Torn-write protection against concurrent
        // readers and writers stays with the inner put_object /
        // complete_multipart_upload commit phases, which take this object's
        // write lock themselves. A delete (user or lifecycle) landing between
        // the tier read and that commit can still be overwritten by the
        // commit — the same window MinIO accepts; a commit-time existence
        // re-check is tracked separately. Holding the lock here instead
        // (#4877) blocked HEAD/get_object_info for the whole copy-back and
        // self-deadlocked on the inner commits.
        if self.single_pool() {
            return self.pools[0]
                .clone()
                .restore_transitioned_object(bucket, &object, &opts)
                .await;
        }

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &writer_pool_lookup_opts(&opts, opts.no_lock))
            .await?;

        self.pools[idx]
            .clone()
            .restore_transitioned_object(bucket, &object, &opts)
            .await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_put_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);
        let mut opts = opts.clone();
        opts.metadata_chg = true;
        let bucket_lifecycle_guard = if is_meta_bucketname(bucket) {
            None
        } else {
            let guard = self.acquire_bucket_lifecycle_read_lock(bucket).await?;
            let current_incarnation_id = get_bucket_incarnation_id_in(&self.ctx, bucket).await?;
            if opts
                .expected_bucket_incarnation_id
                .is_some_and(|expected| expected != current_incarnation_id)
            {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
            opts.expected_bucket_incarnation_id = Some(current_incarnation_id);
            opts.add_bucket_lifecycle_lock_guard(&guard);
            if guard.is_lock_lost() {
                return Err(Error::other("bucket lifecycle lock was lost before the metadata update"));
            }
            Some(guard)
        };

        if self.single_pool() {
            return self.pools[0].put_object_metadata(bucket, object.as_str(), &opts).await;
        }

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &writer_pool_lookup_opts(&opts, opts.no_lock))
            .await?;

        let result = self.pools[idx].put_object_metadata(bucket, object.as_str(), &opts).await;
        drop(bucket_lifecycle_guard);
        result
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_tags(bucket, object.as_str(), opts).await;
        }

        let (oi, _) = self.get_latest_accessible_object_info_with_idx(bucket, &object, opts).await?;
        Ok((*oi.user_tags).clone())
    }

    #[instrument(level = "debug", skip(self))]
    pub(super) async fn handle_put_object_tags(
        &self,
        bucket: &str,
        object: &str,
        tags: &str,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object_tags(bucket, object.as_str(), tags, opts).await;
        }

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &writer_pool_lookup_opts(opts, opts.no_lock))
            .await?;

        self.pools[idx].put_object_tags(bucket, object.as_str(), tags, opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_object_version(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        force_del_marker: bool,
    ) -> Result<()> {
        check_del_obj_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0]
                .delete_object_version(bucket, object.as_str(), fi, force_del_marker)
                .await;
        }
        Err(StorageError::NotImplemented)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].delete_object_tags(bucket, object.as_str(), opts).await;
        }

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &writer_pool_lookup_opts(opts, opts.no_lock))
            .await?;

        self.pools[idx].delete_object_tags(bucket, object.as_str(), opts).await
    }

    pub(super) async fn handle_verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let get_object_reader = <Self as crate::storage_api_contracts::object::ObjectIO>::get_object_reader(
            self,
            bucket,
            object,
            None,
            HeaderMap::new(),
            opts,
        )
        .await?;
        // Stream to sink to avoid loading entire object into memory during verification
        let mut reader = get_object_reader.stream;
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::lifecycle::core::TRANSITION_COMPLETE;
    use crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState;
    use crate::bucket::metadata_sys::ObjectLockConfigState;
    use crate::bucket::replication::{
        ReplicationState, ReplicationStatusType, VersionPurgeStatusType, replication_state_to_filemeta, replication_statuses_map,
        version_purge_statuses_map,
    };
    use crate::core::sets::make_local_two_set_sets_with_ctx;
    use crate::ecstore_validation_blackbox::{make_local_set_disks, make_local_set_disks_with_ctx};
    use crate::layout::{
        endpoints::{Endpoints, PoolEndpoints, SetupType},
        format::FormatV3,
    };
    use crate::object_api::{
        GetObjectBodyCacheHook, GetObjectBodyCacheHookLookup, GetObjectBodySource, clear_get_object_body_cache_hook,
        lookup_get_object_body_cache_hook, register_get_object_body_cache_hook,
    };
    use crate::set_disk::{SetDisks, disk_call_counters};
    use crate::storage_api_contracts::bucket::MakeBucketOptions;
    use crate::storage_api_contracts::lifecycle::TransitionedObject;
    use bytes::Bytes;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt;

    struct WaitForLockLossReader {
        inner: Cursor<Vec<u8>>,
        poll_started: Option<tokio::sync::oneshot::Sender<()>>,
        resume: ParkingMutex<std::sync::mpsc::Receiver<()>>,
    }

    impl AsyncRead for WaitForLockLossReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
            if let Some(poll_started) = self.poll_started.take() {
                let _ = poll_started.send(());
                if self.resume.lock().recv_timeout(Duration::from_secs(10)).is_err() {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "lock-loss signal was not observed during read",
                    )));
                }
            }
            poll
        }
    }

    struct PermanentlyPendingReader {
        poll_started: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl AsyncRead for PermanentlyPendingReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if let Some(poll_started) = self.poll_started.take() {
                let _ = poll_started.send(());
            }
            Poll::Pending
        }
    }

    struct CountingMissHook {
        calls: AtomicUsize,
    }

    #[derive(Debug)]
    struct RefreshFailureLockClient {
        inner: LocalClient,
        fail_refresh: AtomicBool,
    }

    #[async_trait::async_trait]
    impl rustfs_lock::LockClient for RefreshFailureLockClient {
        async fn acquire_lock(&self, request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<rustfs_lock::LockResponse> {
            rustfs_lock::LockClient::acquire_lock(&self.inner, request).await
        }

        async fn release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            rustfs_lock::LockClient::release(&self.inner, lock_id).await
        }

        async fn refresh(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            if self.fail_refresh.load(Ordering::Acquire) {
                return Ok(false);
            }
            rustfs_lock::LockClient::refresh(&self.inner, lock_id).await
        }

        async fn force_release(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            rustfs_lock::LockClient::force_release(&self.inner, lock_id).await
        }

        async fn check_status(&self, lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<Option<rustfs_lock::LockInfo>> {
            rustfs_lock::LockClient::check_status(&self.inner, lock_id).await
        }

        async fn get_stats(&self) -> rustfs_lock::Result<rustfs_lock::LockStats> {
            rustfs_lock::LockClient::get_stats(&self.inner).await
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            rustfs_lock::LockClient::close(&self.inner).await
        }

        async fn is_online(&self) -> bool {
            rustfs_lock::LockClient::is_online(&self.inner).await
        }

        async fn is_local(&self) -> bool {
            rustfs_lock::LockClient::is_local(&self.inner).await
        }
    }

    async fn refresh_failure_test_guard(
        owner: &'static str,
    ) -> (
        ObjectLockDiagGuard,
        Arc<rustfs_lock::distributed_lock::LockLostSignal>,
        Arc<RefreshFailureLockClient>,
    ) {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::Enabled(Arc::new(
            rustfs_lock::FastObjectLockManager::new(),
        )));
        let client = Arc::new(RefreshFailureLockClient {
            inner: LocalClient::with_manager(manager),
            fail_refresh: AtomicBool::new(false),
        });
        let namespace_lock = rustfs_lock::NamespaceLock::with_clients_and_quorum(
            owner.to_string(),
            vec![Arc::clone(&client) as Arc<dyn rustfs_lock::LockClient>],
            1,
        );
        let request =
            rustfs_lock::LockRequest::new(rustfs_lock::ObjectKey::new("bucket", "object"), rustfs_lock::LockType::Shared, owner)
                .with_ttl(Duration::from_secs(2))
                .with_refresh_interval(Duration::from_millis(20));
        let guard = namespace_lock
            .acquire_guard(&request)
            .await
            .expect("distributed read lock acquisition should succeed")
            .expect("distributed read lock should reach quorum");
        let guard = ObjectLockDiagGuard::new(
            guard,
            false,
            "SelectObjectContent",
            Some("bucket".to_string()),
            Some("object".to_string()),
            Some(owner.to_string()),
            ObjectLockDiagMode::Read,
        );
        let signal = guard
            .lock_lost_signal()
            .expect("a distributed guard should expose a lock-loss signal");
        (guard, signal, client)
    }

    async fn refresh_failure_test_lease(
        owner: &'static str,
    ) -> (
        Arc<SelectObjectSnapshotLease>,
        Arc<rustfs_lock::distributed_lock::LockLostSignal>,
        Arc<RefreshFailureLockClient>,
    ) {
        let (guard, signal, client) = refresh_failure_test_guard(owner).await;
        (Arc::new(SelectObjectSnapshotLease::new(vec![guard])), signal, client)
    }

    #[async_trait::async_trait]
    impl GetObjectBodyCacheHook for CountingMissHook {
        async fn lookup(&self, _bucket: &str, _object: &str, _info: &ObjectInfo) -> Option<Bytes> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    struct BodyCacheHookGuard;

    #[test]
    fn select_snapshot_deduplicates_same_distributed_lock_domain() {
        let first: Arc<dyn rustfs_lock::LockClient> = Arc::new(rustfs_lock::LocalClient::new());
        let second: Arc<dyn rustfs_lock::LockClient> = Arc::new(rustfs_lock::LocalClient::new());
        let other: Arc<dyn rustfs_lock::LockClient> = Arc::new(rustfs_lock::LocalClient::new());

        assert!(same_distributed_lock_domain(
            &[Arc::clone(&first), Arc::clone(&second)],
            &[Arc::clone(&second), Arc::clone(&first)]
        ));
        assert!(same_distributed_lock_domain(
            &[Arc::clone(&first), Arc::clone(&first), Arc::clone(&second)],
            &[Arc::clone(&second), Arc::clone(&first)]
        ));
        assert!(!same_distributed_lock_domain(&[first, second], &[other]));
    }

    #[tokio::test]
    async fn decommission_fence_covers_dist_sets_with_same_clients_despite_different_namespaces() {
        let ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        let (_dirs, original_sets) = make_local_two_set_sets_with_ctx(Arc::clone(&ctx)).await;
        let mut second_set = (*original_sets.disk_set[1]).clone();
        second_set.lockers = original_sets.disk_set[0].lockers.clone();
        let mut sets = (*original_sets).clone();
        sets.disk_set[1] = Arc::new(second_set);
        let sets = Arc::new(sets);
        ctx.update_erasure_type(SetupType::DistErasure).await;

        assert!(
            sets.disk_set[0]
                .lockers
                .iter()
                .zip(&sets.disk_set[1].lockers)
                .all(|(fixed, hashed)| Arc::ptr_eq(fixed, hashed)),
            "the regression requires identical distributed lock clients"
        );
        assert_ne!(sets.disk_set[0].set_index, sets.disk_set[1].set_index);

        let pool_config = sets.endpoints.clone();
        let store = new_prepared_reader_test_store_from_pools(vec![Arc::clone(&sets)], vec![pool_config], ctx);
        let object = (0..1_000)
            .map(|index| format!("decommission-dist-domain-{index}.bin"))
            .find(|candidate| Arc::ptr_eq(&sets.get_disks_by_key(candidate), &sets.disk_set[1]))
            .expect("a key should hash to the second set namespace");
        let mutation_fence = store
            .acquire_decommission_object_mutation_fence("bucket", &object)
            .await
            .expect("the fixed distributed mutation fence should be acquired");
        let target_lock = sets.disk_set[1]
            .new_ns_lock("bucket", &object)
            .await
            .expect("the hashed-set namespace lock should be created");
        let target_err = target_lock
            .get_write_lock(Duration::from_millis(50))
            .await
            .expect_err("the fixed read fence must conflict through the shared clients");
        assert!(matches!(target_err, rustfs_lock::LockError::Timeout { .. }));

        let mut put_opts = ObjectOptions::default();
        store
            .apply_decommission_target_mutation_fence(0, &object, &mut put_opts, Some(&mutation_fence))
            .await;
        assert!(put_opts.no_lock, "migration target PUT must reuse the covering fixed fence");

        let mut multipart_opts = ObjectOptions::default();
        store
            .apply_decommission_target_mutation_fence(0, &object, &mut multipart_opts, Some(&mutation_fence))
            .await;
        assert!(multipart_opts.no_lock, "migration target multipart must reuse the covering fixed fence");
        drop(mutation_fence);

        let cleanup_object = (0..1_000)
            .map(|index| format!("decommission-dist-cleanup-{index}.bin"))
            .find(|candidate| Arc::ptr_eq(&sets.get_disks_by_key(candidate), &sets.disk_set[1]))
            .expect("a cleanup key should hash to the second set namespace");
        let source_fence = store
            .acquire_decommission_source_cleanup_fence("bucket", &cleanup_object, sets.disk_set[1].as_ref())
            .await
            .expect("the fixed distributed cleanup fence should be acquired");
        assert!(source_fence.source_lock_covered(), "source cleanup must reuse the covering fixed fence");
        let source_lock = sets.disk_set[1]
            .new_ns_lock("bucket", &cleanup_object)
            .await
            .expect("the source-set namespace lock should be created");
        let source_err = source_lock
            .get_read_lock(Duration::from_millis(50))
            .await
            .expect_err("the fixed write fence must conflict through the shared clients");
        assert!(matches!(source_err, rustfs_lock::LockError::Timeout { .. }));
    }

    #[test]
    fn select_snapshot_version_matching_normalizes_null_and_uuid_forms() {
        let nil = Uuid::nil();
        for actual in [None, Some(nil)] {
            assert!(select_snapshot_version_matches(actual, "null"));
            assert!(select_snapshot_version_matches(actual, "NULL"));
            assert!(select_snapshot_version_matches(actual, &nil.to_string()));
        }

        let version = Uuid::new_v4();
        assert!(select_snapshot_version_matches(Some(version), &version.to_string().to_uppercase()));
        assert!(!select_snapshot_version_matches(Some(version), "null"));
        assert!(!select_snapshot_version_matches(Some(version), "not-a-version"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn select_snapshot_reader_rolls_back_bytes_when_lock_is_lost_during_poll() {
        let (lease, signal, client) = refresh_failure_test_lease("select-snapshot-lock-loss").await;

        let (poll_started_tx, poll_started_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let release_client = Arc::clone(&client);
        let release_lease = Arc::clone(&lease);
        let release_signal = Arc::clone(&signal);
        let release_task = tokio::spawn(async move {
            poll_started_rx.await.expect("reader poll should start");
            release_client.fail_refresh.store(true, Ordering::Release);
            tokio::time::timeout(Duration::from_secs(5), release_signal.notified())
                .await
                .expect("heartbeat should observe the rejected refresh");
            tokio::time::timeout(Duration::from_secs(5), async {
                while !release_lease.is_lost() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("snapshot monitor should publish lock loss");
            resume_tx.send(()).expect("reader poll should still be waiting");
        });

        let mut reader = SelectObjectSnapshotReader {
            inner: Box::new(WaitForLockLossReader {
                inner: Cursor::new(b"must-not-escape".to_vec()),
                poll_started: Some(poll_started_tx),
                resume: ParkingMutex::new(resume_rx),
            }),
            lock_loss_wake: SelectObjectSnapshotLockLossWake::new(lease.subscribe_lock_loss()),
            lease,
        };
        let mut output = Vec::new();
        let error = reader
            .read_to_end(&mut output)
            .await
            .expect_err("a read that loses its lease must fail");
        release_task.await.expect("backend release task should not panic");

        assert!(signal.is_lost(), "heartbeat should report the rejected refresh");
        assert!(output.is_empty(), "bytes read during the failed poll must be rolled back");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn select_snapshot_reader_checks_guards_at_eof_before_monitor_runs() {
        let (guard, signal, client) = refresh_failure_test_guard("select-snapshot-eof-fence").await;
        client.fail_refresh.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), signal.notified())
            .await
            .expect("heartbeat should observe the rejected refresh");

        let lease = Arc::new(SelectObjectSnapshotLease::new(vec![guard]));
        assert!(!lease.is_lost(), "current-thread monitor must not run before the synchronous read");
        let mut reader = SelectObjectSnapshotReader {
            inner: Box::new(Cursor::new(b"old-generation".to_vec())),
            lock_loss_wake: SelectObjectSnapshotLockLossWake::new(lease.subscribe_lock_loss()),
            lease,
        };
        let mut output = Vec::new();

        let error = reader
            .read_to_end(&mut output)
            .await
            .expect_err("EOF fence must reject a lease lost before its monitor is scheduled");

        assert_eq!(output, b"old-generation");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn select_snapshot_lock_loss_is_broadcast_to_remaining_pending_readers() {
        let (first_guard, first_signal, first_client) = refresh_failure_test_guard("select-snapshot-pending-first-lock").await;
        let (second_guard, second_signal, second_client) =
            refresh_failure_test_guard("select-snapshot-pending-second-lock").await;
        let lease = Arc::new(SelectObjectSnapshotLease::new(vec![first_guard, second_guard]));
        let dropped_reader = SelectObjectSnapshotReader {
            inner: Box::new(PermanentlyPendingReader { poll_started: None }),
            lock_loss_wake: SelectObjectSnapshotLockLossWake::new(lease.subscribe_lock_loss()),
            lease: Arc::clone(&lease),
        };
        drop(dropped_reader);

        let (first_started_tx, first_started_rx) = tokio::sync::oneshot::channel();
        let (second_started_tx, second_started_rx) = tokio::sync::oneshot::channel();
        let mut first_reader = SelectObjectSnapshotReader {
            inner: Box::new(PermanentlyPendingReader {
                poll_started: Some(first_started_tx),
            }),
            lock_loss_wake: SelectObjectSnapshotLockLossWake::new(lease.subscribe_lock_loss()),
            lease: Arc::clone(&lease),
        };
        let mut second_reader = SelectObjectSnapshotReader {
            inner: Box::new(PermanentlyPendingReader {
                poll_started: Some(second_started_tx),
            }),
            lock_loss_wake: SelectObjectSnapshotLockLossWake::new(lease.subscribe_lock_loss()),
            lease,
        };
        let first_read_task = tokio::spawn(async move {
            let mut byte = [0_u8; 1];
            first_reader.read(&mut byte).await
        });
        let second_read_task = tokio::spawn(async move {
            let mut byte = [0_u8; 1];
            second_reader.read(&mut byte).await
        });

        first_started_rx.await.expect("first inner reader should reach Poll::Pending");
        second_started_rx
            .await
            .expect("second inner reader should reach Poll::Pending");
        second_client.fail_refresh.store(true, Ordering::Release);
        let (first_result, second_result) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(5), first_read_task),
            tokio::time::timeout(Duration::from_secs(5), second_read_task),
        );
        for result in [first_result, second_result] {
            let error = result
                .expect("lock loss should wake every reader whose inner I/O remains pending")
                .expect("reader task should not panic")
                .expect_err("lost snapshot lease must fail every pending read");
            assert_eq!(error.kind(), std::io::ErrorKind::Other);
        }

        assert!(!first_client.fail_refresh.load(Ordering::Acquire));
        assert!(!first_signal.is_lost());
        assert!(second_signal.is_lost());
    }

    #[test]
    fn tier_delete_entry_is_prepared_and_bound_to_source_generation() {
        let identity = [9_u8; 32];
        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(identity),
        );
        let version_id = Uuid::from_u128(1);
        let data_dir = Uuid::from_u128(2);
        let source = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            data_dir: Some(data_dir),
            user_defined: Arc::new(metadata),
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: "remote-version".to_string(),
                tier: "WARM".to_string(),
                status: TRANSITION_COMPLETE.to_string(),
                ..Default::default()
            },
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            ..Default::default()
        };
        let entry = build_tier_delete_journal_entry(
            "bucket",
            "object",
            &ObjectOptions {
                version_id: Some(version_id.to_string()),
                versioned: true,
                ..Default::default()
            },
            &source,
        )
        .expect("transition source should produce a journal entry")
        .expect("completed transition should be journaled");

        assert_eq!(entry.state, TierDeleteJournalState::Prepared);
        assert_eq!(entry.backend_identity, Some(identity));
        let data_dir_string = data_dir.to_string();
        assert_eq!(
            entry.source.as_ref().and_then(|source| source.data_dir.as_deref()),
            Some(data_dir_string.as_str())
        );
    }

    impl Drop for BodyCacheHookGuard {
        fn drop(&mut self) {
            clear_get_object_body_cache_hook();
        }
    }

    #[test]
    fn delete_marker_data_movement_falls_back_when_only_source_pool_has_object() {
        let target = select_data_movement_target_pool(Ok(1), 1, true).unwrap();
        assert_eq!(target, None);
    }

    #[test]
    fn delete_marker_data_movement_falls_back_when_version_does_not_exist_yet() {
        let err = StorageError::ObjectNotFound("bucket".to_string(), "object".to_string());
        let target = select_data_movement_target_pool(Err(err), 1, true).unwrap();
        assert_eq!(target, None);
    }

    #[test]
    fn non_delete_marker_data_movement_keeps_existing_pool() {
        let target = select_data_movement_target_pool(Ok(0), 1, false).unwrap();
        assert_eq!(target, Some(0));
    }

    #[test]
    fn equivalent_data_movement_delete_marker_requires_same_version_and_mod_time() {
        let version_id = Uuid::nil();
        let mod_time = OffsetDateTime::UNIX_EPOCH;
        let source = ObjectInfo {
            version_id: Some(version_id),
            delete_marker: true,
            mod_time: Some(mod_time),
            ..Default::default()
        };
        let target = source.clone();

        assert!(is_equivalent_data_movement_delete_marker(&source, &target));

        let mismatched = ObjectInfo {
            mod_time: Some(mod_time + Duration::from_secs(1)),
            ..target
        };
        assert!(!is_equivalent_data_movement_delete_marker(&source, &mismatched));
    }

    #[test]
    fn equivalent_data_movement_delete_marker_accepts_distinct_local_free_version_ids() {
        let mut source = ObjectInfo {
            version_id: Some(Uuid::from_u128(1)),
            delete_marker: true,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut source.user_defined),
            rustfs_utils::http::SUFFIX_TIER_FV_ID,
            Uuid::from_u128(2).to_string(),
        );
        let mut target = source.clone();
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut target.user_defined),
            rustfs_utils::http::SUFFIX_TIER_FV_ID,
            Uuid::from_u128(3).to_string(),
        );

        assert!(is_equivalent_data_movement_delete_marker(&source, &target));

        Arc::make_mut(&mut target.user_defined).insert(
            format!("{}{}", rustfs_utils::http::MINIO_INTERNAL_PREFIX, rustfs_utils::http::SUFFIX_TIER_FV_ID),
            Uuid::from_u128(4).to_string(),
        );
        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_delete_marker_accepts_replication_alias_expansion() {
        let key = format!(
            "{}{}",
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS
        );
        let timestamp_key = format!(
            "{}{}",
            rustfs_utils::http::MINIO_INTERNAL_PREFIX,
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP
        );
        let source = ObjectInfo {
            version_id: Some(Uuid::from_u128(1)),
            delete_marker: true,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            user_defined: Arc::new(HashMap::from([
                (key.clone(), "arn=COMPLETED;".to_string()),
                (timestamp_key, "1970-01-01T00:00:01Z".to_string()),
            ])),
            ..Default::default()
        };
        let mut target = source.clone();
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut target.user_defined),
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS,
            "arn=COMPLETED;".to_string(),
        );
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut target.user_defined),
            rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP,
            (OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND).to_string(),
        );
        assert!(is_equivalent_data_movement_delete_marker(&source, &target));

        Arc::make_mut(&mut target.user_defined).insert(key, "arn=FAILED;".to_string());
        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));
    }

    #[test]
    fn data_movement_delete_marker_source_requires_persisted_mod_time() {
        let source = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        assert!(!is_expected_data_movement_delete_marker_source(&source, None));

        let source = ObjectInfo {
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..source
        };
        assert!(is_expected_data_movement_delete_marker_source(&source, Some(OffsetDateTime::UNIX_EPOCH)));
        assert!(!is_expected_data_movement_delete_marker_source(&source, None));
    }

    #[test]
    fn data_movement_delete_marker_uses_current_source_replication_state() {
        let expected_timestamp = OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND;
        let timestamp = expected_timestamp.to_string();
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_REPLICA_STATUS,
            ReplicationStatusType::Replica.to_string(),
        );
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_REPLICA_TIMESTAMP, timestamp.clone());
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP, timestamp);
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS,
            "arn=COMPLETED;".to_string(),
        );
        rustfs_utils::http::insert_str(
            &mut metadata,
            &format!(
                "{}{}",
                rustfs_utils::http::SUFFIX_REPLICATION_RESET_ARN_PREFIX,
                "arn:minio:replication::TenantA:bucket"
            ),
            "reset-id".to_string(),
        );
        rustfs_utils::http::insert_str(
            &mut metadata,
            &format!(
                "{}{}",
                rustfs_utils::http::SUFFIX_REPLICATION_DELETE_MARKER_VERSION_ARN_PREFIX,
                "arn:minio:replication::TenantA:bucket"
            ),
            "target-version".to_string(),
        );
        let source = ObjectInfo {
            delete_marker: true,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            replication_status_internal: Some("arn=COMPLETED;".to_string()),
            replication_decision: "arn=replicate;".to_string(),
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        let opts = ObjectOptions {
            mod_time: source.mod_time,
            delete_replication: Some(ReplicationState {
                replication_status_internal: Some("arn=PENDING;".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let target_opts = current_data_movement_delete_marker_opts(&source, &opts).expect("valid current source state");
        let state = target_opts.delete_replication.as_ref().expect("current replication state");
        assert_eq!(state.replication_status_internal.as_deref(), Some("arn=COMPLETED;"));
        assert_eq!(state.replica_status, crate::bucket::replication::ReplicationStatusType::Replica);
        assert_eq!(state.replica_timestamp, Some(expected_timestamp));
        assert_eq!(state.replication_timestamp, state.replica_timestamp);
        assert_eq!(state.replicate_decision_str, "arn=replicate;");
        assert_eq!(
            state
                .reset_statuses_map
                .get("arn:minio:replication::TenantA:bucket")
                .map(String::as_str),
            Some("reset-id")
        );
        assert_eq!(
            state
                .target_delete_marker_version_ids
                .get("arn:minio:replication::TenantA:bucket")
                .map(String::as_str),
            Some("target-version")
        );
    }

    #[test]
    fn data_movement_delete_marker_rejects_corrupt_target_version_maps() {
        let suffix = format!("{}not-an-arn", rustfs_utils::http::SUFFIX_REPLICATION_DELETE_MARKER_VERSION_ARN_PREFIX);
        let mut malformed = HashMap::new();
        rustfs_utils::http::insert_str(&mut malformed, &suffix, "target-version".to_string());
        let malformed_source = ObjectInfo {
            delete_marker: true,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            user_defined: Arc::new(malformed),
            ..Default::default()
        };
        assert!(current_data_movement_delete_marker_opts(&malformed_source, &ObjectOptions::default()).is_none());

        let mut conflicted = HashMap::new();
        let suffix = format!(
            "{}arn:minio:replication::target:bucket",
            rustfs_utils::http::SUFFIX_REPLICATION_DELETE_MARKER_VERSION_ARN_PREFIX
        );
        rustfs_utils::http::insert_str(&mut conflicted, &suffix, "target-version-a".to_string());
        conflicted.insert(
            format!("{}{suffix}", rustfs_utils::http::MINIO_INTERNAL_PREFIX),
            "target-version-b".to_string(),
        );
        let conflicted_source = ObjectInfo {
            user_defined: Arc::new(conflicted),
            ..malformed_source.clone()
        };
        assert!(current_data_movement_delete_marker_opts(&conflicted_source, &ObjectOptions::default()).is_none());

        let mut over_cap = HashMap::new();
        for index in 0..=1_000 {
            let suffix = format!(
                "{}arn:minio:replication::target:bucket-{index}",
                rustfs_utils::http::SUFFIX_REPLICATION_DELETE_MARKER_VERSION_ARN_PREFIX
            );
            rustfs_utils::http::insert_str(&mut over_cap, &suffix, format!("target-version-{index}"));
        }
        let over_cap_source = ObjectInfo {
            user_defined: Arc::new(over_cap),
            ..malformed_source
        };
        assert!(current_data_movement_delete_marker_opts(&over_cap_source, &ObjectOptions::default()).is_none());
    }

    #[test]
    fn data_movement_delete_marker_normalizes_legacy_missing_replication_timestamps() {
        let mut source_metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut source_metadata,
            rustfs_utils::http::SUFFIX_REPLICA_STATUS,
            ReplicationStatusType::Replica.to_string(),
        );
        rustfs_utils::http::insert_str(
            &mut source_metadata,
            rustfs_utils::http::SUFFIX_REPLICATION_STATUS,
            "arn=COMPLETED;".to_string(),
        );
        let source = ObjectInfo {
            delete_marker: true,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            replication_status_internal: Some("arn=COMPLETED;".to_string()),
            user_defined: Arc::new(source_metadata),
            ..Default::default()
        };

        let target_opts = current_data_movement_delete_marker_opts(&source, &ObjectOptions::default())
            .expect("legacy status-only metadata should remain migratable");
        let state = target_opts
            .delete_replication
            .expect("replication state should be reconstructed");
        assert_eq!(state.replica_timestamp, Some(OffsetDateTime::UNIX_EPOCH));
        assert_eq!(state.replication_timestamp, Some(OffsetDateTime::UNIX_EPOCH));

        let mut target_metadata = (*source.user_defined).clone();
        let epoch = OffsetDateTime::UNIX_EPOCH
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        rustfs_utils::http::insert_str(&mut target_metadata, rustfs_utils::http::SUFFIX_REPLICA_TIMESTAMP, epoch.clone());
        rustfs_utils::http::insert_str(&mut target_metadata, rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP, epoch);
        assert!(is_equivalent_data_movement_delete_marker_metadata(&source.user_defined, &target_metadata));
    }

    #[test]
    fn equivalent_data_movement_delete_marker_rejects_metadata_and_replication_mismatch() {
        let version_id = Uuid::nil();
        let mod_time = OffsetDateTime::UNIX_EPOCH;
        let source = ObjectInfo {
            version_id: Some(version_id),
            delete_marker: true,
            mod_time: Some(mod_time),
            user_defined: Arc::new(HashMap::from([("x-amz-meta-source".to_string(), "true".to_string())])),
            replication_status_internal: Some("arn:minio:replication:target=COMPLETED;".to_string()),
            version_purge_status_internal: Some("arn:minio:replication:target=PENDING;".to_string()),
            ..Default::default()
        };

        let mut target = source.clone();
        target.user_defined = Arc::new(HashMap::from([("x-amz-meta-source".to_string(), "false".to_string())]));
        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));

        let mut target = source.clone();
        target.replication_status_internal = Some("arn:minio:replication:target=FAILED;".to_string());
        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));

        let mut target = source.clone();
        target.version_purge_status_internal = Some("arn:minio:replication:target=COMPLETE;".to_string());
        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_delete_marker_rejects_live_object() {
        let source = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let target = ObjectInfo {
            delete_marker: false,
            ..source.clone()
        };

        assert!(!is_equivalent_data_movement_delete_marker(&source, &target));
    }

    #[test]
    fn data_movement_delete_marker_resume_accepts_equivalent_target() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            delete_marker: true,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };

        let should_resume = resolve_data_movement_delete_marker_resume_result(Ok(Some(source.clone())), &source, 0, 1)
            .expect("equivalent delete marker target should be evaluated");

        assert!(should_resume);
    }

    #[test]
    fn data_movement_delete_marker_resume_rejects_source_pool_target() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            delete_marker: true,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };

        let should_resume = resolve_data_movement_delete_marker_resume_result(Ok(Some(source.clone())), &source, 0, 0)
            .expect("source-pool target should be rejected before target lookup");

        assert!(!should_resume);
    }

    #[test]
    fn data_movement_resume_target_prefers_selected_non_source_pool() {
        let target_pool_idx = resolve_data_movement_resume_target_pool(2, Some(3), 1);
        assert_eq!(target_pool_idx, 2);
    }

    #[test]
    fn data_movement_resume_target_uses_resolved_non_source_pool_when_selected_is_source() {
        let target_pool_idx = resolve_data_movement_resume_target_pool(1, Some(3), 1);
        assert_eq!(target_pool_idx, 3);
        assert!(should_check_data_movement_resume_target(1, target_pool_idx));
    }

    #[test]
    fn data_movement_resume_target_keeps_source_when_no_other_pool_is_available() {
        let target_pool_idx = resolve_data_movement_resume_target_pool(1, None, 1);
        assert_eq!(target_pool_idx, 1);
    }

    #[test]
    fn data_movement_delete_marker_resume_propagates_target_lookup_error() {
        let source = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let result = resolve_data_movement_delete_marker_resume_result(Err(Error::SlowDown), &source, 0, 1);

        assert!(matches!(result, Err(Error::SlowDown)));
    }

    fn tiered_equivalence_source() -> FileInfo {
        let version_id = Uuid::nil();
        let transition_version_id = Uuid::new_v4();
        let mod_time = OffsetDateTime::UNIX_EPOCH;

        FileInfo {
            version_id: Some(version_id),
            size: 1024,
            mod_time: Some(mod_time),
            checksum: Some(Bytes::from_static(b"checksum")),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_tier: "WARM".to_string(),
            transition_version_id: Some(transition_version_id),
            replication_state_internal: Some(replication_state_to_filemeta(&ReplicationState {
                replication_status_internal: Some("arn:minio:replication:target=COMPLETED;".to_string()),
                targets: replication_statuses_map("arn:minio:replication:target=COMPLETED;"),
                version_purge_status_internal: Some("arn:minio:replication:target=PENDING;".to_string()),
                purge_targets: version_purge_statuses_map("arn:minio:replication:target=PENDING;"),
                ..Default::default()
            })),
            metadata: HashMap::from([
                ("etag".to_string(), "etag-value".to_string()),
                ("x-amz-meta-key".to_string(), "metadata-value".to_string()),
                (rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(), "tag=value".to_string()),
                ("expires".to_string(), "1970-01-01T00:33:20Z".to_string()),
            ]),
            ..Default::default()
        }
    }

    fn tiered_equivalence_target(source: &FileInfo) -> ObjectInfo {
        ObjectInfo::from_file_info(source, "bucket", "object", source.version_id.is_some())
    }

    #[test]
    fn equivalent_data_movement_tiered_object_accepts_matching_persisted_metadata() {
        let source = tiered_equivalence_source();
        let target = tiered_equivalence_target(&source);

        assert!(is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn tiered_data_movement_source_match_rejects_transition_identity_changes() {
        let source = tiered_equivalence_source();
        assert!(tiered_data_movement_source_matches(&source, &source).expect("matching source metadata should parse"));

        let mut changed_remote = source.clone();
        changed_remote.transitioned_objname = "remote/replaced".to_string();
        assert!(!tiered_data_movement_source_matches(&source, &changed_remote).expect("changed remote metadata should parse"));

        let mut changed_backend = source.clone();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut changed_backend.metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex([9; 32]),
        );
        assert!(!tiered_data_movement_source_matches(&source, &changed_backend).expect("backend metadata should parse"));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_uses_logical_compressed_and_encrypted_sizes() {
        let mut compressed = tiered_equivalence_source();
        compressed.size = 600;
        rustfs_utils::http::insert_str(&mut compressed.metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_string());
        rustfs_utils::http::insert_str(&mut compressed.metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "1024".to_string());
        let compressed_target = tiered_equivalence_target(&compressed);
        assert!(is_equivalent_data_movement_tiered_object(&compressed, &compressed_target));

        let mut encrypted = tiered_equivalence_source();
        encrypted.size = 640;
        encrypted.metadata.insert(
            rustfs_utils::http::object_encryption_keys::INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(),
            "key-id".to_string(),
        );
        encrypted.metadata.insert(
            rustfs_utils::http::object_encryption_keys::INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER.to_string(),
            "1024".to_string(),
        );
        let encrypted_target = tiered_equivalence_target(&encrypted);
        assert!(is_equivalent_data_movement_tiered_object(&encrypted, &encrypted_target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_accepts_transition_alias_expansion() {
        let mut source = tiered_equivalence_source();
        let suffix = rustfs_utils::http::SUFFIX_TRANSITION_TIER;
        source.metadata.insert(
            format!("{}{suffix}", rustfs_utils::http::MINIO_INTERNAL_PREFIX),
            source.transition_tier.clone(),
        );
        let mut target = tiered_equivalence_target(&source);
        Arc::make_mut(&mut target.user_defined)
            .insert(rustfs_utils::http::internal_key_rustfs(suffix), source.transition_tier.clone());

        assert!(is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_requires_hydrated_part_checksums() {
        let mut source = tiered_equivalence_source();
        source.parts = vec![rustfs_filemeta::ObjectPartInfo {
            number: 1,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND),
            checksums: Some(HashMap::from([("CRC32C".to_string(), "AAAAAA==".to_string())])),
            ..Default::default()
        }];
        rustfs_utils::http::insert_str(
            &mut source.metadata,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
            r#"[[1,[["CRC32C","AAAAAA=="]]]]"#.to_string(),
        );
        let mut target = tiered_equivalence_target(&source);
        Arc::make_mut(&mut target.parts)[0].mod_time = None;
        assert!(is_equivalent_data_movement_tiered_object(&source, &target));

        let mut missing = target;
        Arc::make_mut(&mut missing.parts)[0].checksums = None;
        assert!(!is_equivalent_data_movement_tiered_object(&source, &missing));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_transition_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.transitioned_object.name = "remote/target".to_string();

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_transition_version_state_mismatch() {
        let mut source = tiered_equivalence_source();
        source.transition_version_state = rustfs_filemeta::TransitionVersionState::Exact;
        let mut target = tiered_equivalence_target(&source);
        target.transition_version_state = rustfs_filemeta::TransitionVersionState::Unknown;

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_user_metadata_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.user_defined = Arc::new(HashMap::from([("x-amz-meta-key".to_string(), "target-value".to_string())]));

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_tag_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.user_tags = Arc::new("tag=target".to_string());

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_replication_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.replication_status_internal = Some("arn:minio:replication:target=FAILED;".to_string());
        target.replication_status = ReplicationStatusType::Failed;

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn equivalent_data_movement_tiered_object_rejects_version_purge_mismatch() {
        let source = tiered_equivalence_source();
        let mut target = tiered_equivalence_target(&source);
        target.version_purge_status_internal = Some("arn:minio:replication:target=COMPLETE;".to_string());
        target.version_purge_status = VersionPurgeStatusType::Complete;

        assert!(!is_equivalent_data_movement_tiered_object(&source, &target));
    }

    #[test]
    fn data_movement_tiered_resume_accepts_equivalent_target() {
        let source = tiered_equivalence_source();
        let target = tiered_equivalence_target(&source);

        let should_resume = resolve_data_movement_tiered_resume_result(Ok(Some(target)), &source, 0, 1)
            .expect("equivalent tiered target should be evaluated");

        assert!(should_resume);
    }

    #[test]
    fn data_movement_tiered_resume_rejects_source_pool_target() {
        let source = tiered_equivalence_source();
        let target = tiered_equivalence_target(&source);

        let should_resume = resolve_data_movement_tiered_resume_result(Ok(Some(target)), &source, 0, 0)
            .expect("source-pool target should be rejected before target lookup");

        assert!(!should_resume);
    }

    #[test]
    fn data_movement_tiered_resume_rejects_missing_target() {
        let source = FileInfo {
            version_id: Some(Uuid::nil()),
            size: 1024,
            ..Default::default()
        };

        let should_resume = resolve_data_movement_tiered_resume_result(Ok(None), &source, 0, 1)
            .expect("missing tiered target should be evaluated");

        assert!(!should_resume);
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_none_for_live_object() {
        let info = ObjectInfo::default();
        let opts = ObjectOptions::default();

        assert!(latest_object_access_delete_marker_error("bucket", "object", &info, &opts).is_none());
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_not_found_without_version_id() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions::default();

        let err = latest_object_access_delete_marker_error("bucket", "object", &info, &opts)
            .expect("delete marker should stop latest-object reads");

        assert!(crate::error::is_err_object_not_found(&err));
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_method_not_allowed_for_version_read() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let err = latest_object_access_delete_marker_error("bucket", "object", &info, &opts)
            .expect("delete marker version reads should be rejected");

        assert!(matches!(err, Error::MethodNotAllowed));
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_not_found_for_delete_marker_lookup() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            delete_marker: true,
            ..Default::default()
        };

        let err = latest_object_access_delete_marker_error("bucket", "object", &info, &opts)
            .expect("delete marker lookup should keep not-found semantics");

        assert!(crate::error::is_err_object_not_found(&err));
    }

    #[test]
    fn resolve_latest_object_access_returns_live_object_and_pool_idx() {
        let info = ObjectInfo::default();
        let opts = ObjectOptions::default();

        let (resolved, idx) = resolve_latest_object_access("bucket", "object", info, 7, &opts).unwrap();

        assert_eq!(idx, 7);
        assert!(!resolved.delete_marker);
    }

    #[test]
    fn resolve_latest_object_access_rejects_delete_marker_without_version_id() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions::default();

        let err = resolve_latest_object_access("bucket", "object", info, 2, &opts).unwrap_err();

        assert!(crate::error::is_err_object_not_found(&err));
    }

    #[test]
    fn resolve_latest_object_access_rejects_delete_marker_version_read() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let err = resolve_latest_object_access("bucket", "object", info, 2, &opts).unwrap_err();

        assert!(matches!(err, Error::MethodNotAllowed));
    }

    #[test]
    fn should_create_delete_marker_for_missing_object_allows_latest_versioned_delete() {
        let opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };

        assert!(should_create_delete_marker_for_missing_object(&opts));
    }

    #[test]
    fn should_create_delete_marker_for_missing_object_rejects_specialized_deletes() {
        let version_delete = ObjectOptions {
            versioned: true,
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };
        let delete_marker_replication = ObjectOptions {
            versioned: true,
            delete_marker: true,
            ..Default::default()
        };
        let data_movement = ObjectOptions {
            versioned: true,
            data_movement: true,
            ..Default::default()
        };

        assert!(!should_create_delete_marker_for_missing_object(&version_delete));
        assert!(!should_create_delete_marker_for_missing_object(&delete_marker_replication));
        assert!(!should_create_delete_marker_for_missing_object(&data_movement));
    }

    // issue #4189 regression: `del_opts` pins `version_id = Uuid::nil()` on directory
    // keys, so deleting a ghost folder over HTTP fails the lookup with *version*-not-found
    // (not object-not-found). The orphan-purge guard must accept both misses, or the
    // ghost tree survives behind a fake 204 — the exact reported symptom.
    #[test]
    fn should_purge_orphan_dir_on_version_not_found_for_dir_key() {
        assert!(
            should_purge_orphan_dir_on_missing(&StorageError::FileVersionNotFound, "ghost/"),
            "the real HTTP delete path yields version-not-found on dir keys and must reach the purge"
        );
        assert!(
            should_purge_orphan_dir_on_missing(
                &StorageError::VersionNotFound("bucket".into(), "ghost/".into(), Uuid::nil().to_string()),
                "ghost/"
            ),
            "typed VersionNotFound on a dir key must also reach the purge"
        );
    }

    #[test]
    fn should_purge_orphan_dir_on_object_not_found_for_dir_key() {
        assert!(should_purge_orphan_dir_on_missing(&StorageError::FileNotFound, "ghost/"));
        assert!(should_purge_orphan_dir_on_missing(
            &StorageError::ObjectNotFound("bucket".into(), "ghost/".into()),
            "ghost/"
        ));
    }

    #[test]
    fn should_not_purge_orphan_dir_for_regular_key_or_other_errors() {
        // A regular (non-directory) key must never trigger a prefix purge, even on a miss.
        assert!(!should_purge_orphan_dir_on_missing(&StorageError::FileVersionNotFound, "regular.txt"));
        assert!(!should_purge_orphan_dir_on_missing(&StorageError::FileNotFound, "regular.txt"));
        // Non-miss errors (e.g. quorum failures) must not be masked by a purge attempt.
        assert!(!should_purge_orphan_dir_on_missing(&StorageError::ErasureReadQuorum, "ghost/"));
    }

    #[test]
    fn resolve_decommission_target_pool_idx_result_passthrough_ok() {
        let idx = ECStore::resolve_decommission_target_pool_idx_result(Ok(3), "bucket", "object").unwrap();

        assert_eq!(idx, 3);
    }

    #[test]
    fn resolve_decommission_target_pool_idx_result_wraps_error_context() {
        let err = ECStore::resolve_decommission_target_pool_idx_result(Err(Error::other("boom")), "bucket", "object")
            .expect_err("expected contextual error");
        let rendered = err.to_string();

        assert!(rendered.contains("failed to select decommission target pool"), "{rendered}");
        assert!(rendered.contains("bucket"), "{rendered}");
        assert!(rendered.contains("object"), "{rendered}");
        assert!(rendered.contains("boom"), "{rendered}");
    }

    #[test]
    fn resolve_decommission_tiered_object_result_passthrough_ok() {
        ECStore::resolve_decommission_tiered_object_result(Ok(()), "bucket", "object")
            .expect("successful decommission result should pass through");
    }

    #[test]
    fn resolve_decommission_tiered_object_result_wraps_error_context() {
        let err = ECStore::resolve_decommission_tiered_object_result(Err(Error::other("boom")), "bucket", "object")
            .expect_err("expected contextual error");
        let rendered = err.to_string();

        assert!(rendered.contains("failed to decommission tiered object"), "{rendered}");
        assert!(rendered.contains("bucket"), "{rendered}");
        assert!(rendered.contains("object"), "{rendered}");
        assert!(rendered.contains("boom"), "{rendered}");
    }

    #[test]
    fn version_aware_lookup_opts_enables_version_aware_lookup() {
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let lookup_opts = version_aware_lookup_opts(&opts, true);

        assert!(lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert_eq!(lookup_opts.version_id.as_deref(), Some("vid-1"));
    }

    #[test]
    fn version_aware_lookup_opts_keeps_latest_lookup_for_unversioned_requests() {
        let lookup_opts = version_aware_lookup_opts(&ObjectOptions::default(), true);

        assert!(lookup_opts.no_lock);
        assert!(!lookup_opts.metadata_chg);
        assert!(lookup_opts.version_id.is_none());
    }

    #[test]
    fn data_movement_pool_lookup_opts_enables_version_aware_lookup_and_skip_flags() {
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let lookup_opts = data_movement_pool_lookup_opts(&opts, false);

        assert!(!lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);
        assert_eq!(lookup_opts.version_id.as_deref(), Some("vid-1"));
    }

    #[test]
    fn writer_pool_lookup_opts_skips_rebalance_sources() {
        let lookup_opts = writer_pool_lookup_opts(
            &ObjectOptions {
                version_id: Some("vid-1".to_string()),
                ..Default::default()
            },
            true,
        );

        assert!(lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);
        assert_eq!(lookup_opts.version_id.as_deref(), Some("vid-1"));
    }

    #[test]
    fn ordinary_delete_lookup_includes_decommission_source_and_skips_rebalance_source() {
        let lookup_opts = delete_pool_lookup_opts(&ObjectOptions::default(), true);

        assert!(lookup_opts.no_lock);
        assert!(!lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);

        let explicit_version = delete_pool_lookup_opts(
            &ObjectOptions {
                versioned: true,
                version_id: Some(uuid::Uuid::new_v4().to_string()),
                ..Default::default()
            },
            true,
        );
        assert!(!explicit_version.skip_decommissioned);
    }

    #[test]
    fn delete_fans_out_for_unversioned_and_explicit_version_mutations() {
        assert!(should_delete_from_all_pools(&ObjectOptions::default(), 1));
        assert!(should_delete_from_all_pools(
            &ObjectOptions {
                versioned: true,
                version_id: Some(uuid::Uuid::new_v4().to_string()),
                ..Default::default()
            },
            2,
        ));
        assert!(!should_delete_from_all_pools(
            &ObjectOptions {
                versioned: true,
                ..Default::default()
            },
            1,
        ));
        assert!(!should_delete_from_all_pools(&ObjectOptions::default(), 0));
    }

    #[test]
    fn batch_delete_identifies_only_latest_versioned_markers() {
        let versioned = DeleteReplicationConfigSnapshot::from_configs_for_test(
            s3s::dto::VersioningConfiguration {
                status: Some(s3s::dto::BucketVersioningStatus::from_static(s3s::dto::BucketVersioningStatus::ENABLED)),
                ..Default::default()
            },
            None,
        );
        let latest = ObjectToDelete {
            object_name: "latest".to_string(),
            ..Default::default()
        };
        assert!(batch_delete_creates_latest_marker(&latest, &versioned));
        assert!(!batch_delete_targets_pool(true, Some(1), 0));
        assert!(batch_delete_targets_pool(true, Some(1), 1));
        assert!(!batch_delete_targets_pool(true, Some(1), 2));

        let explicit = ObjectToDelete {
            object_name: "explicit".to_string(),
            version_id: Some(uuid::Uuid::new_v4()),
            ..Default::default()
        };
        assert!(!batch_delete_creates_latest_marker(&explicit, &versioned));
        assert!(batch_delete_targets_pool(false, Some(1), 0));

        let unversioned = DeleteReplicationConfigSnapshot::default();
        assert!(!batch_delete_creates_latest_marker(&latest, &unversioned));
        assert!(batch_delete_targets_pool(false, None, 0));
    }

    #[test]
    fn batch_delete_pool_failures_override_success_in_any_pool_order() {
        let success = DeletedObject {
            object_name: "object".to_string(),
            found: true,
            ..Default::default()
        };
        let source_errors = [
            StorageError::ErasureWriteQuorum,
            StorageError::NamespaceLockQuorumUnavailable {
                mode: "delete_objects_commit",
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                required: 1,
                achieved: 0,
            },
        ];

        for source_error in source_errors {
            for source_first in [true, false] {
                let failed = (DeletedObject::default(), Some(source_error.clone()));
                let succeeded = (success.clone(), None);
                let pool_results = if source_first {
                    vec![failed, succeeded]
                } else {
                    vec![succeeded, failed]
                };

                let (_, error, attempted) =
                    resolve_batch_delete_pool_results(None, pool_results.iter().map(|(deleted, error)| (deleted, error)));

                assert!(attempted);
                assert_eq!(error, Some(source_error.clone()));
            }
        }
    }

    #[test]
    fn batch_delete_ignores_missing_pool_only_after_another_pool_succeeds() {
        let success = DeletedObject {
            object_name: "object".to_string(),
            found: true,
            ..Default::default()
        };
        let missing_errors = [
            StorageError::ObjectNotFound("bucket".to_string(), "object".to_string()),
            StorageError::VersionNotFound("bucket".to_string(), "object".to_string(), "version".to_string()),
        ];

        for missing_error in missing_errors {
            let missing = (DeletedObject::default(), Some(missing_error.clone()));
            for missing_first in [true, false] {
                let succeeded = (success.clone(), None);
                let pool_results = if missing_first {
                    vec![missing.clone(), succeeded]
                } else {
                    vec![succeeded, missing.clone()]
                };
                let (deleted, error, attempted) =
                    resolve_batch_delete_pool_results(None, pool_results.iter().map(|(deleted, error)| (deleted, error)));

                assert!(attempted);
                let deleted = deleted.expect("successful pool result should be retained");
                assert!(deleted.found);
                assert_eq!(deleted.object_name, success.object_name.as_str());
                assert!(error.is_none());
            }

            let missing_only = [missing];
            let (_, error, attempted) =
                resolve_batch_delete_pool_results(None, missing_only.iter().map(|(deleted, error)| (deleted, error)));
            assert!(attempted);
            assert_eq!(error, Some(missing_error));
        }

        let silent_missing = [(DeletedObject::default(), None)];
        let (_, error, attempted) =
            resolve_batch_delete_pool_results(None, silent_missing.iter().map(|(deleted, error)| (deleted, error)));
        assert!(attempted);
        assert!(error.is_none());
    }

    #[test]
    fn data_movement_pool_lookup_opts_keeps_no_lock_for_tiered_moves() {
        let lookup_opts = data_movement_pool_lookup_opts(
            &ObjectOptions {
                version_id: Some("vid-1".to_string()),
                ..Default::default()
            },
            true,
        );

        assert!(lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);
    }

    #[test]
    fn transition_restore_pool_opts_skips_decommissioned_and_preserves_locking() {
        let lookup_opts = transition_restore_pool_opts(&ObjectOptions {
            no_lock: false,
            skip_decommissioned: false,
            ..Default::default()
        });

        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);
        assert!(!lookup_opts.no_lock);
    }

    #[test]
    fn transition_restore_pool_opts_preserves_existing_no_lock() {
        let lookup_opts = transition_restore_pool_opts(&ObjectOptions {
            no_lock: true,
            ..Default::default()
        });

        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);
        assert!(lookup_opts.no_lock);
    }

    #[test]
    fn delete_objects_lock_names_are_sorted_and_unique() {
        let objects = vec![
            ObjectToDelete {
                object_name: "beta".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "alpha".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "beta".to_string(),
                ..Default::default()
            },
        ];

        assert_eq!(sorted_unique_delete_object_names(&objects), vec!["alpha", "beta"]);
    }

    async fn new_read_lock_test_store() -> ECStore {
        let format = FormatV3::new(1, 2);
        let endpoints = vec![
            Endpoint::try_from("http://127.0.0.1:9000/data0").expect("first endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9001/data1").expect("second endpoint should parse"),
        ];
        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "read-lock-metadata-cache-safe-test".to_string(),
            platform: "test".to_string(),
        };
        let endpoint_pools = EndpointServerPools::from(vec![pool_endpoints.clone()]);
        let sets = Sets::new(vec![None, None], &pool_endpoints, &format, 0, 1)
            .await
            .expect("test sets should be created with empty disks");

        ECStore {
            id: Uuid::new_v4(),
            disk_map: HashMap::new(),
            pools: vec![sets],
            peer_sys: S3PeerSys::new(&endpoint_pools),
            pool_meta: RwLock::new(PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::new(()),
            ctx: crate::runtime::instance::bootstrap_ctx(),
            bucket_fence_registry: std::sync::Arc::default(),
        }
    }

    async fn new_prepared_reader_test_store(set_disks: &[Arc<SetDisks>]) -> ECStore {
        new_prepared_reader_test_store_with_ctx(set_disks, crate::runtime::instance::bootstrap_ctx()).await
    }

    async fn new_prepared_reader_test_store_with_ctx(
        set_disks: &[Arc<SetDisks>],
        ctx: Arc<crate::runtime::instance::InstanceContext>,
    ) -> ECStore {
        let mut pool_configs = Vec::with_capacity(set_disks.len());
        let mut pools = Vec::with_capacity(set_disks.len());

        for (pool_idx, set_disks) in set_disks.iter().enumerate() {
            let mut endpoints = Endpoints::from(set_disks.set_endpoints.clone());
            for endpoint in endpoints.as_mut() {
                endpoint.set_pool_index(pool_idx);
            }
            let pool_config = PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: set_disks.set_drive_count,
                endpoints,
                cmd_line: format!("prepared-reader-test-pool-{pool_idx}"),
                platform: "test".to_string(),
            };
            let disks = set_disks.disks.read().await.clone();
            let pool = Sets::new_with_instance_ctx(
                disks,
                &pool_config,
                &set_disks.format,
                pool_idx,
                set_disks.default_parity_count,
                Arc::clone(&ctx),
            )
            .await
            .expect("prepared-reader test pool should be created from local disks");
            pool_configs.push(pool_config);
            pools.push(pool);
        }

        new_prepared_reader_test_store_from_pools(pools, pool_configs, ctx)
    }

    fn new_prepared_reader_test_store_from_pools(
        pools: Vec<Arc<Sets>>,
        pool_configs: Vec<PoolEndpoints>,
        ctx: Arc<crate::runtime::instance::InstanceContext>,
    ) -> ECStore {
        let endpoint_pools = EndpointServerPools::from(pool_configs);
        ECStore {
            id: Uuid::new_v4(),
            disk_map: HashMap::new(),
            pools,
            peer_sys: S3PeerSys::new_with_instance_ctx(&endpoint_pools, Arc::clone(&ctx)),
            pool_meta: RwLock::new(PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::new(()),
            ctx,
            bucket_fence_registry: std::sync::Arc::default(),
        }
    }

    async fn assert_prepared_reader_blocks_writer(store: &ECStore, bucket: &str, object: &str) {
        assert_pool_writer_is_blocked(store, 0, bucket, object).await;
    }

    async fn assert_pool_writer_is_blocked(store: &ECStore, pool_idx: usize, bucket: &str, object: &str) {
        let manager = Arc::clone(store.pools[pool_idx].get_disks_by_key(object).local_lock_manager_for_test());
        let lock = rustfs_lock::NamespaceLock::with_local_manager("prepared-reader-writer".to_string(), manager);
        let err = lock
            .get_write_lock(rustfs_lock::ObjectKey::new(bucket, object), "competing-writer", Duration::from_millis(50))
            .await
            .expect_err("prepared read lock should block the writer");
        assert!(matches!(err, rustfs_lock::LockError::Timeout { .. }));
    }

    async fn acquire_prepared_reader_writer(store: &ECStore, bucket: &str, object: &str) -> rustfs_lock::NamespaceLockGuard {
        acquire_pool_writer(store, 0, bucket, object).await
    }

    async fn acquire_pool_writer(
        store: &ECStore,
        pool_idx: usize,
        bucket: &str,
        object: &str,
    ) -> rustfs_lock::NamespaceLockGuard {
        let manager = Arc::clone(store.pools[pool_idx].get_disks_by_key(object).local_lock_manager_for_test());
        let lock = rustfs_lock::NamespaceLock::with_local_manager("prepared-reader-writer".to_string(), manager);
        lock.get_write_lock(rustfs_lock::ObjectKey::new(bucket, object), "competing-writer", Duration::from_secs(1))
            .await
            .expect("prepared read lock should have been released")
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn prepared_reader_uses_authoritative_hook_miss_once_and_streams_full_body() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let store = new_prepared_reader_test_store(&[set_disks]).await;
        let bucket = "prepared-reader-hook-miss";
        let object = "object.bin";
        let payload = b"prepared-reader-hook-miss-payload-".repeat(40_000);
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        store.pools[0]
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut put_reader = PutObjReader::from_vec(payload.clone());
        store.pools[0]
            .put_object(bucket, object, &mut put_reader, &opts)
            .await
            .expect("object should be written");

        clear_get_object_body_cache_hook();
        let hook = Arc::new(CountingMissHook {
            calls: AtomicUsize::new(0),
        });
        register_get_object_body_cache_hook(Arc::clone(&hook) as Arc<dyn GetObjectBodyCacheHook>);
        let _hook_guard = BodyCacheHookGuard;

        let prepared = store
            .prepare_get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("prepared reader metadata should resolve");
        assert!(matches!(
            lookup_get_object_body_cache_hook(bucket, object, &None, &opts, prepared.object_info()).await,
            GetObjectBodyCacheHookLookup::Miss
        ));
        assert_eq!(hook.calls.load(Ordering::Relaxed), 1, "the authoritative probe should call the hook once");

        let mut reader = prepared.into_reader().await.expect("prepared body reader should open");
        assert_eq!(reader.body_source, GetObjectBodySource::HookMissed);
        let mut restored = Vec::new();
        reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("prepared body should stream");

        assert_eq!(restored, payload);
        assert_eq!(hook.calls.load(Ordering::Relaxed), 1, "reader construction must not probe the hook again");
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn prepared_reader_holds_namespace_lock_until_eof_or_drop() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("false"))], async {
            let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
            let store = new_prepared_reader_test_store(&[set_disks]).await;
            let bucket = "prepared-reader-lock-lifetime";
            let object = "object.bin";
            let payload = b"prepared-reader-lock-lifetime-payload-".repeat(40_000);
            let put_opts = ObjectOptions {
                no_lock: true,
                ..Default::default()
            };

            store.pools[0]
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
            let mut put_reader = PutObjReader::from_vec(payload.clone());
            store.pools[0]
                .put_object(bucket, object, &mut put_reader, &put_opts)
                .await
                .expect("object should be written");

            let prepared = store
                .prepare_get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("prepared reader metadata should resolve");
            assert!(prepared.read_lock_guard.is_some());
            assert_prepared_reader_blocks_writer(&store, bucket, object).await;

            let mut reader = prepared.into_reader().await.expect("prepared body reader should open");
            assert_prepared_reader_blocks_writer(&store, bucket, object).await;
            let mut restored = Vec::new();
            reader
                .stream
                .read_to_end(&mut restored)
                .await
                .expect("prepared body should stream");
            assert_eq!(restored, payload);
            drop(acquire_prepared_reader_writer(&store, bucket, object).await);

            let prepared = store
                .prepare_get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("second prepared reader metadata should resolve");
            let reader = prepared.into_reader().await.expect("second prepared body reader should open");
            assert_prepared_reader_blocks_writer(&store, bucket, object).await;
            drop(reader);
            drop(acquire_prepared_reader_writer(&store, bucket, object).await);
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn select_snapshot_holds_namespace_lock_independent_of_get_optimization() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true"))], async {
            let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
            let store = new_prepared_reader_test_store(&[set_disks]).await;
            let bucket = "select-snapshot-lock-lifetime";
            let object = "object.bin";
            let payload = b"select-snapshot-lock-lifetime-payload-".repeat(40_000);
            let put_opts = ObjectOptions {
                no_lock: true,
                ..Default::default()
            };

            store.pools[0]
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
            let mut put_reader = PutObjReader::from_vec(payload.clone());
            store.pools[0]
                .put_object(bucket, object, &mut put_reader, &put_opts)
                .await
                .expect("object should be written");

            use rustfs_utils::http::headers::{SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER};
            let mut request_headers = HeaderMap::new();
            request_headers.insert(SSEC_ALGORITHM_HEADER, "AES256".parse().expect("valid SSE-C algorithm header"));
            request_headers.insert(SSEC_KEY_HEADER, "secret-key".parse().expect("valid SSE-C key header"));
            request_headers.insert(SSEC_KEY_MD5_HEADER, "key-md5".parse().expect("valid SSE-C key digest header"));
            request_headers.insert("authorization", "credential".parse().expect("valid authorization header"));
            let snapshot = store
                .prepare_select_object_snapshot(bucket, object, &request_headers, &ObjectOptions::default())
                .await
                .expect("SelectObjectContent snapshot should be prepared");
            assert_eq!(snapshot.headers.len(), 3);
            assert_eq!(snapshot.headers.get(SSEC_ALGORITHM_HEADER), request_headers.get(SSEC_ALGORITHM_HEADER));
            assert_eq!(snapshot.headers.get(SSEC_KEY_HEADER), request_headers.get(SSEC_KEY_HEADER));
            assert_eq!(snapshot.headers.get(SSEC_KEY_MD5_HEADER), request_headers.get(SSEC_KEY_MD5_HEADER));
            assert!(snapshot.headers.get("authorization").is_none());
            assert!(snapshot.headers.values().all(http::HeaderValue::is_sensitive));
            assert!(!format!("{:?}", snapshot.headers).contains("secret-key"));
            assert_eq!(
                snapshot.logical_size(),
                u64::try_from(payload.len()).expect("test payload length should fit in u64")
            );
            assert_eq!(
                snapshot.object_info().size,
                i64::try_from(payload.len()).expect("test payload length should fit in i64")
            );
            assert_prepared_reader_blocks_writer(&store, bucket, object).await;

            let mut reader = snapshot.open_reader(None).await.expect("snapshot reader should open");
            let mut restored = Vec::new();
            reader
                .stream
                .read_to_end(&mut restored)
                .await
                .expect("snapshot body should stream");
            assert_eq!(restored, payload);
            drop(reader);
            assert_prepared_reader_blocks_writer(&store, bucket, object).await;

            drop(snapshot);
            drop(acquire_prepared_reader_writer(&store, bucket, object).await);
        })
        .await;
    }

    #[tokio::test]
    async fn select_snapshot_identity_compares_encoded_directory_object_key() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let store = new_prepared_reader_test_store(&[set_disks]).await;
        let bucket = "select-snapshot-directory-identity";
        let source_object = "source.bin";

        store.pools[0]
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut put_reader = PutObjReader::from_vec(b"source".to_vec());
        store.pools[0]
            .put_object(
                bucket,
                source_object,
                &mut put_reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source object should be written");

        let mut snapshot = store
            .prepare_select_object_snapshot(bucket, source_object, &HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("source object snapshot should be prepared");
        snapshot.object = encode_dir_object("directory/");

        assert!(snapshot.is_for(bucket, "directory/"));
        assert!(!snapshot.is_for(bucket, "different/"));
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn select_snapshot_reuses_initial_metadata_fanout_for_first_reader() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let store = new_prepared_reader_test_store(&[set_disks]).await;
        let bucket = "select-snapshot-initial-metadata";
        let object = "initial-metadata.bin";
        let payload = b"select snapshot initial metadata".repeat(4_000);
        let no_lock_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        store.pools[0]
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut put_reader = PutObjReader::from_vec(payload.clone());
        store.pools[0]
            .put_object(bucket, object, &mut put_reader, &no_lock_opts)
            .await
            .expect("object should be written");

        let calls = disk_call_counters::observe(object);
        let snapshot = store
            .prepare_select_object_snapshot(bucket, object, &HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("snapshot should prepare metadata");
        assert_eq!(
            calls.total(disk_call_counters::KIND_READ_VERSION),
            4,
            "snapshot preparation should fan out metadata once"
        );

        let mut reader = snapshot.open_reader(None).await.expect("first snapshot reader should open");
        assert_eq!(
            calls.total(disk_call_counters::KIND_READ_VERSION),
            4,
            "first reader should consume the metadata captured during preparation"
        );
        let mut restored = Vec::new();
        reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("snapshot body should stream");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn select_snapshot_rejects_identity_change_before_second_reader() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let store = new_prepared_reader_test_store(&[Arc::clone(&set_disks)]).await;
        let bucket = "select-snapshot-identity-change";
        let object = "identity-change.bin";
        let no_lock_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut initial_reader = PutObjReader::from_vec(b"first generation".to_vec());
        set_disks
            .put_object(bucket, object, &mut initial_reader, &no_lock_opts)
            .await
            .expect("initial generation should be written");
        let mut snapshot = store
            .prepare_select_object_snapshot(bucket, object, &HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("snapshot should be prepared");
        drop(
            snapshot
                .open_reader(None)
                .await
                .expect("first snapshot reader should consume captured metadata"),
        );
        snapshot.opts.metadata_cache_safe = false;

        let mut replacement_reader = PutObjReader::from_vec(b"replacement generation".to_vec());
        set_disks
            .put_object(bucket, object, &mut replacement_reader, &no_lock_opts)
            .await
            .expect("test-only no-lock write should replace the object");
        let error = match snapshot.open_reader(None).await {
            Ok(_) => panic!("a later reader must reject the replacement generation"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            SelectObjectSnapshotReadError::Consistency(SnapshotConsistencyError::ObjectChanged)
        ));
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn select_snapshot_rejects_latest_versioned_delete_marker_during_prepare() {
        let ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        let (_first_dirs, first_set) = make_local_set_disks_with_ctx(4, 2, Arc::clone(&ctx)).await;
        let (_second_dirs, second_set) = make_local_set_disks_with_ctx(4, 2, Arc::clone(&ctx)).await;
        let store = new_prepared_reader_test_store_with_ctx(&[Arc::clone(&first_set), Arc::clone(&second_set)], ctx).await;
        let bucket = "select-snapshot-latest-delete-marker";
        let object = "versioned-object.bin";
        let versioned_opts = ObjectOptions {
            no_lock: true,
            versioned: true,
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(ObjectLockConfigState::ConfirmedAbsent))),
            ..Default::default()
        };

        for set_disks in [&first_set, &second_set] {
            set_disks
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
        }
        let mut older_reader = PutObjReader::from_vec(b"older visible generation".to_vec());
        first_set
            .put_object(bucket, object, &mut older_reader, &versioned_opts)
            .await
            .expect("older versioned object should be written");
        let mut put_reader = PutObjReader::from_vec(b"hidden generation".to_vec());
        second_set
            .put_object(bucket, object, &mut put_reader, &versioned_opts)
            .await
            .expect("versioned object should be written");
        let marker = second_set
            .delete_object(bucket, object, versioned_opts.clone())
            .await
            .expect("versioned delete should create a marker");
        assert!(marker.delete_marker);
        assert!(marker.version_id.is_some_and(|version_id| !version_id.is_nil()));

        let error = store
            .prepare_select_object_snapshot(
                bucket,
                object,
                &HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("latest delete marker should hide the prior object generation");
        assert!(matches!(
            error,
            PrepareSelectObjectSnapshotError::Storage(ref error) if is_err_object_not_found(error)
        ));
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn select_snapshot_blocks_store_delete_for_object_in_nonzero_set() {
        let ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        let (_dirs, sets) = make_local_two_set_sets_with_ctx(Arc::clone(&ctx)).await;
        ctx.update_erasure_type(SetupType::DistErasure).await;
        assert!(
            sets.disk_set[0]
                .lockers
                .iter()
                .all(|first| sets.disk_set[1].lockers.iter().all(|second| !Arc::ptr_eq(first, second)))
        );
        let pool_config = sets.endpoints.clone();
        let store = Arc::new(new_prepared_reader_test_store_from_pools(vec![Arc::clone(&sets)], vec![pool_config], ctx));
        let bucket = RUSTFS_META_BUCKET;
        let object = (0..1_000)
            .map(|index| format!("nonzero-set-{index}.bin"))
            .find(|candidate| Arc::ptr_eq(&sets.get_disks_by_key(candidate), &sets.disk_set[1]))
            .expect("a key should hash to the second set");
        let mut put_reader = PutObjReader::from_vec(b"stable snapshot body".to_vec());
        sets.put_object(
            bucket,
            &object,
            &mut put_reader,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("object should be written to the second set");
        assert!(Arc::ptr_eq(&sets.get_disks_by_key(&object), &sets.disk_set[1]));

        let snapshot = store
            .prepare_select_object_snapshot(bucket, &object, &HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("snapshot should acquire both lock domains");
        let hashed_set_writer = rustfs_lock::NamespaceLock::with_clients_and_quorum(
            "select-nonzero-set-writer".to_string(),
            sets.disk_set[1].lockers.clone(),
            2,
        );
        let writer_error = hashed_set_writer
            .get_write_lock(
                rustfs_lock::ObjectKey::new(bucket, object.as_str()),
                "competing-hashed-set-writer",
                Duration::from_millis(50),
            )
            .await
            .expect_err("Select must hold the nonzero hashed-set read lock");
        assert!(matches!(writer_error, rustfs_lock::LockError::Timeout { .. }));
        let barrier = DeleteAfterObjectLockSnapshotBarrier::install(bucket);
        let delete_store = Arc::clone(&store);
        let delete_object = object.clone();
        let mut delete = tokio::spawn(async move {
            delete_store
                .delete_object(bucket, &delete_object, ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        barrier.release();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut delete).await.is_err(),
            "store DELETE must wait for the fixed-domain Select read lock"
        );

        drop(snapshot);
        let deleted = tokio::time::timeout(Duration::from_secs(10), delete)
            .await
            .expect("DELETE should resume after the snapshot is dropped")
            .expect("DELETE task should not panic")
            .expect("DELETE should complete");
        assert_eq!(deleted.name, object);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn select_snapshot_fails_closed_when_local_locking_is_disabled() {
        let ctx = Arc::new(crate::runtime::instance::InstanceContext::with_lock_manager_for_test(Arc::new(
            rustfs_lock::GlobalLockManager::Disabled(rustfs_lock::DisabledLockManager::new()),
        )));
        let (_dirs, set_disks) = make_local_set_disks_with_ctx(4, 2, Arc::clone(&ctx)).await;
        let store = new_prepared_reader_test_store_with_ctx(&[set_disks], ctx).await;
        let bucket = "select-snapshot-lock-disabled";
        let object = "object.bin";
        let mut put_reader = PutObjReader::from_vec(b"payload".to_vec());
        let no_lock_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        store.pools[0]
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        store.pools[0]
            .put_object(bucket, object, &mut put_reader, &no_lock_opts)
            .await
            .expect("object should be written without namespace locking");

        let error = store
            .prepare_select_object_snapshot(bucket, object, &HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect_err("SelectObjectContent must reject a disabled lock manager");
        assert!(matches!(
            error,
            PrepareSelectObjectSnapshotError::Consistency(SnapshotConsistencyError::LockingDisabled)
        ));
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn prepared_object_info_releases_namespace_lock_immediately() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let store = new_prepared_reader_test_store(&[set_disks]).await;
        let bucket = "prepared-object-info-lock-release";
        let object = "object.bin";
        let payload = b"prepared-object-info-lock-release".to_vec();
        let put_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        store.pools[0]
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut put_reader = PutObjReader::from_vec(payload.clone());
        store.pools[0]
            .put_object(bucket, object, &mut put_reader, &put_opts)
            .await
            .expect("object should be written");

        let prepared = store
            .prepare_get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("prepared reader metadata should resolve");
        assert_prepared_reader_blocks_writer(&store, bucket, object).await;
        assert_eq!(prepared.into_object_info().size, payload.len() as i64);

        drop(acquire_prepared_reader_writer(&store, bucket, object).await);
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn prepared_reader_resolves_object_from_second_pool() {
        let (_first_dirs, first_set) = make_local_set_disks(4, 2).await;
        let (_second_dirs, second_set) = make_local_set_disks(4, 2).await;
        let store = new_prepared_reader_test_store(&[first_set, second_set]).await;
        let bucket = "prepared-reader-second-pool";
        let object = "object.bin";
        let payload = b"prepared-reader-second-pool-payload-".repeat(40_000);
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        for pool in &store.pools {
            pool.make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created in each pool");
        }
        let mut put_reader = PutObjReader::from_vec(payload.clone());
        store.pools[1]
            .put_object(bucket, object, &mut put_reader, &opts)
            .await
            .expect("object should be written only to the second pool");

        let prepared = store
            .prepare_get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("prepared reader should resolve the second-pool object");
        assert_eq!(prepared.object_info().size, payload.len() as i64);
        let mut reader = prepared.into_reader().await.expect("prepared body reader should open");
        let mut restored = Vec::new();
        reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("prepared body should stream");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn select_snapshot_locks_the_hashed_set_in_every_pool() {
        let (_first_dirs, first_set) = make_local_set_disks(4, 2).await;
        let (_second_dirs, second_set) = make_local_set_disks(4, 2).await;
        let store = new_prepared_reader_test_store(&[first_set, second_set]).await;
        let bucket = "select-snapshot-all-pools";
        let object = "object.bin";
        let payload = b"second-pool-snapshot".to_vec();
        let no_lock_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        for pool in &store.pools {
            pool.make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created in each pool");
        }
        let mut put_reader = PutObjReader::from_vec(payload.clone());
        store.pools[1]
            .put_object(bucket, object, &mut put_reader, &no_lock_opts)
            .await
            .expect("object should be written only to the second pool");

        let snapshot = store
            .prepare_select_object_snapshot(bucket, object, &HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("snapshot should resolve the second-pool object");
        assert_pool_writer_is_blocked(&store, 0, bucket, object).await;
        assert_pool_writer_is_blocked(&store, 1, bucket, object).await;

        let mut reader = snapshot.open_reader(None).await.expect("snapshot reader should open");
        let mut restored = Vec::new();
        reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("snapshot body should stream");
        assert_eq!(restored, payload);
        drop(reader);
        drop(snapshot);

        drop(acquire_pool_writer(&store, 0, bucket, object).await);
        drop(acquire_pool_writer(&store, 1, bucket, object).await);
    }

    // Phase 5 Slice 2 (backlog#939): the instance context flows down the whole
    // object graph — ECStore, its Sets, and their SetDisks must all carry the
    // same `Arc<InstanceContext>` in a single-instance deployment.
    #[tokio::test]
    async fn instance_context_flows_through_object_graph() {
        let store = new_read_lock_test_store().await;

        let sets = store.pools.first().expect("test store has one pool");
        assert!(
            std::sync::Arc::ptr_eq(&store.ctx, sets.instance_ctx()),
            "Sets must carry the store's instance context"
        );

        let set_disks = sets.disk_set.first().expect("pool has one set");
        assert!(
            std::sync::Arc::ptr_eq(sets.instance_ctx(), set_disks.instance_ctx()),
            "SetDisks must carry the Sets' instance context"
        );
    }

    // Phase 5 Slice 3 (backlog#939): a SetDisks sources its lock manager from
    // its instance context (not an independent process lookup), and in a
    // single-instance build that context aliases the process lock-manager
    // singleton — so the lock namespace is unchanged.
    #[tokio::test]
    async fn set_disks_lock_manager_comes_from_instance_context() {
        let store = new_read_lock_test_store().await;
        let set_disks = store.pools[0].disk_set.first().expect("pool has one set");

        assert!(
            std::sync::Arc::ptr_eq(set_disks.local_lock_manager_for_test(), &set_disks.instance_ctx().lock_manager()),
            "SetDisks lock manager must be sourced from its instance context"
        );
        assert!(
            std::sync::Arc::ptr_eq(set_disks.local_lock_manager_for_test(), &rustfs_lock::get_global_lock_manager()),
            "single-instance lock manager must alias the process singleton"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn delete_objects_write_locks_cover_each_unique_object() {
        let store = new_read_lock_test_store().await;
        let objects = vec![
            ObjectToDelete {
                object_name: "beta".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "alpha".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "beta".to_string(),
                ..Default::default()
            },
        ];
        let mut opts = ObjectOptions::default();

        let guards = store
            .acquire_delete_objects_write_locks("bucket", &objects, &mut opts)
            .await
            .expect("delete object locks should be acquired");

        assert_eq!(guards.len(), 2, "duplicate object names should share one namespace lock");
        assert!(opts.no_lock, "set layer should not reacquire locks already held by ECStore");
        assert!(
            opts.namespace_lock_fence.is_some(),
            "set layer must receive the outer write-lock loss fence"
        );

        let alpha_lock = store
            .handle_new_ns_lock("bucket", "alpha")
            .await
            .expect("alpha namespace lock should be created");
        let err = alpha_lock
            .get_read_lock(Duration::from_millis(20))
            .await
            .expect_err("batch delete write guard should block alpha readers");
        assert!(matches!(err, rustfs_lock::LockError::Timeout { .. }));

        drop(guards);
        alpha_lock
            .get_read_lock(Duration::from_secs(1))
            .await
            .expect("alpha read lock should be available after dropping batch guards");
    }

    #[tokio::test]
    async fn acquired_read_lock_marks_metadata_cache_safe_for_set_layer() {
        let store = new_read_lock_test_store().await;
        let mut opts = ObjectOptions::default();

        let guard = store
            .acquire_object_read_lock_if_needed("get_object", "bucket", "object", &mut opts)
            .await
            .expect("read lock should be acquired");

        assert!(guard.is_some(), "read lock should be held by the outer store layer");
        assert!(opts.no_lock, "set layer should not reacquire the object lock");
        assert!(
            opts.metadata_cache_safe,
            "metadata cache is safe only because the outer store layer acquired the read lock"
        );
    }

    #[tokio::test]
    async fn prelocked_read_request_does_not_mark_metadata_cache_safe() {
        let store = new_read_lock_test_store().await;
        let mut opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        let guard = store
            .acquire_object_read_lock_if_needed("get_object", "bucket", "object", &mut opts)
            .await
            .expect("prelocked read should not acquire another lock");

        assert!(guard.is_none(), "prelocked caller should keep lock ownership outside ECStore");
        assert!(
            !opts.metadata_cache_safe,
            "generic no_lock callers must stay ineligible for metadata cache unless explicitly marked safe"
        );
    }

    // NOTE: #4877's `restore_transitioned_object_waits_for_existing_reader`
    // was removed with the whole-copy-back write lock it asserted
    // (backlog#1304): restore entry no longer serializes on the object lock.
    // The replacement semantics — non-blocking reads during the copy-back and
    // fast rejection of a concurrent restore — are covered end-to-end by
    // `restore_object_usecase_reports_ongoing_conflict`
    // (rustfs/src/app/lifecycle_transition_api_test.rs), while the SetDisks
    // transition matrix covers the final local commit. Restore-vs-reader data
    // protection lives in the inner put_object/complete_multipart_upload locks.
    #[tokio::test]
    #[serial_test::serial]
    async fn restore_accept_guard_serializes_concurrent_accepts() {
        // backlog#1304: the accept guard is the compare-and-set boundary for
        // the restore ongoing flag — a second accept of the same object must
        // wait behind (here: time out on) the first.
        let store = Arc::new(new_read_lock_test_store().await);
        let _first = store
            .acquire_restore_accept_guard("bucket", "object")
            .await
            .expect("first accept guard should be acquired");

        let err = temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            match store.acquire_restore_accept_guard("bucket", "object").await {
                Ok(_) => panic!("second accept guard must wait behind the first"),
                Err(err) => err,
            }
        })
        .await;

        assert!(matches!(err, StorageError::Lock(rustfs_lock::LockError::Timeout { .. })));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reader_lock_is_held_when_optimization_is_disabled() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("false"))], async {
            let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
            let lock = rustfs_lock::NamespaceLock::with_local_manager("test".to_string(), manager);
            let key = rustfs_lock::ObjectKey::new("bucket", "object");
            let read_guard = lock
                .get_read_lock(key.clone(), "reader", Duration::from_secs(1))
                .await
                .expect("read lock should be acquired");
            let read_guard = ObjectLockDiagGuard::new(
                read_guard,
                true,
                "test_get_object",
                Some("bucket".to_string()),
                Some("object".to_string()),
                Some("reader".to_string()),
                ObjectLockDiagMode::Read,
            );
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(Vec::<u8>::new())),
                object_info: ObjectInfo::default(),
                buffered_body: None,
                body_source: Default::default(),
            };

            let reader = ECStore::attach_read_lock_guard(reader, Some(read_guard));

            lock.get_write_lock(key.clone(), "writer", Duration::from_millis(20))
                .await
                .expect_err("reader should hold the read lock");
            drop(reader);
            lock.get_write_lock(key, "writer", Duration::from_secs(1))
                .await
                .expect("dropping the reader should release the read lock");
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reader_lock_is_not_held_for_stream_when_optimization_is_enabled() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true"))], async {
            let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
            let lock = rustfs_lock::NamespaceLock::with_local_manager("test".to_string(), manager);
            let key = rustfs_lock::ObjectKey::new("bucket", "object");
            let read_guard = lock
                .get_read_lock(key.clone(), "reader", Duration::from_secs(1))
                .await
                .expect("read lock should be acquired");
            let read_guard = ObjectLockDiagGuard::new(
                read_guard,
                true,
                "test_get_object",
                Some("bucket".to_string()),
                Some("object".to_string()),
                Some("reader".to_string()),
                ObjectLockDiagMode::Read,
            );
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(vec![1, 2, 3])),
                object_info: ObjectInfo::default(),
                buffered_body: None,
                body_source: Default::default(),
            };

            let reader = ECStore::attach_read_lock_guard(reader, Some(read_guard));

            lock.get_write_lock(key, "writer", Duration::from_secs(1))
                .await
                .expect("lock optimization should release the read lock before returning the stream");
            drop(reader);
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reader_lock_is_not_held_for_buffered_body_when_optimization_is_enabled() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("true"))], async {
            let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
            let lock = rustfs_lock::NamespaceLock::with_local_manager("test".to_string(), manager);
            let key = rustfs_lock::ObjectKey::new("bucket", "object");
            let read_guard = lock
                .get_read_lock(key.clone(), "reader", Duration::from_secs(1))
                .await
                .expect("read lock should be acquired");
            let read_guard = ObjectLockDiagGuard::new(
                read_guard,
                true,
                "test_get_object",
                Some("bucket".to_string()),
                Some("object".to_string()),
                Some("reader".to_string()),
                ObjectLockDiagMode::Read,
            );
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(vec![1, 2, 3])),
                object_info: ObjectInfo::default(),
                buffered_body: Some(Bytes::from_static(b"123")),
                body_source: Default::default(),
            };

            let reader = ECStore::attach_read_lock_guard(reader, Some(read_guard));

            lock.get_write_lock(key, "writer", Duration::from_secs(1))
                .await
                .expect("buffered reader should release the read lock immediately");
            drop(reader);
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reader_lock_is_released_after_stream_eof() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE, Some("false"))], async {
            let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
            let lock = rustfs_lock::NamespaceLock::with_local_manager("test".to_string(), manager);
            let key = rustfs_lock::ObjectKey::new("bucket", "object");
            let read_guard = lock
                .get_read_lock(key.clone(), "reader", Duration::from_secs(1))
                .await
                .expect("read lock should be acquired");
            let read_guard = ObjectLockDiagGuard::new(
                read_guard,
                true,
                "test_get_object",
                Some("bucket".to_string()),
                Some("object".to_string()),
                Some("reader".to_string()),
                ObjectLockDiagMode::Read,
            );
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(vec![1, 2, 3])),
                object_info: ObjectInfo::default(),
                buffered_body: None,
                body_source: Default::default(),
            };

            let mut reader = ECStore::attach_read_lock_guard(reader, Some(read_guard));
            let mut output = Vec::new();
            reader.stream.read_to_end(&mut output).await.expect("reader should reach EOF");
            assert_eq!(output, vec![1, 2, 3]);

            lock.get_write_lock(key, "writer", Duration::from_secs(1))
                .await
                .expect("EOF should release the read lock before the reader is dropped");
            drop(reader);
        })
        .await;
    }
}
