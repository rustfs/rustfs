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

//! Per-instance runtime state ("instance context").
//!
//! Phase 5 of the global-singleton consolidation (backlog#939). State that
//! carries an `ECStore` instance's identity/runtime is being moved out of the
//! process-level statics in [`super::global`] into this context so that
//! multiple `ECStore` instances can coexist in one process without
//! cross-contaminating each other's state.
//!
//! ## Isolation carrier
//!
//! Multi-instance disambiguation is carried by the **object graph**
//! (`ECStore` → `Vec<Arc<Sets>>` → `SetDisks`), each holding an
//! `Arc<InstanceContext>`. Instance-scoped reads resolve through `self.ctx`,
//! so no call-site signatures change. (A `task_local` was rejected during
//! design review: it does not propagate across the many internal
//! `tokio::spawn` boundaries in the data/background paths.)
//!
//! ## Backward compatibility
//!
//! The legacy free-function facade in [`super::global`]
//! (`is_erasure`/`update_erasure_type`/…) resolves the *current* instance via
//! the object-store resolver and falls back to the process-level
//! [`bootstrap_ctx`] when no store is published yet. Because `ECStore::new`
//! **adopts** the same bootstrap `Arc` (rather than minting a fresh one),
//! startup writes and post-construction reads hit the same cell and
//! single-instance behavior is byte-for-byte unchanged.

use super::global::TypeLocalDiskSetDrives;
use crate::bucket::bandwidth::monitor::Monitor;
use crate::bucket::lifecycle::bucket_lifecycle_ops::{ExpiryState, TransitionState};
use crate::bucket::metadata_sys::BucketMetadataSys;
use crate::bucket::replication::{DynReplicationPool, ReplicationStats};
use crate::disk::DiskStore;
use crate::disk::multipart_source::ProofVerificationContext;
use crate::layout::endpoints::{EndpointServerPools, SetupType};
use crate::object_api::ObjectEncryptionResolver;
use crate::services::event_notification::EventNotifier;
use crate::services::tier::tier::TierConfigMgr;
use crate::store::delete_recovery_runtime::DeleteRecoveryHints;
use rustfs_lock::{GlobalLockManager, get_global_lock_manager};
use s3s::region::Region;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};
use tokio::sync::{Mutex, Notify, OnceCell, OwnedRwLockReadGuard, RwLock};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::Instrument as _;
use uuid::Uuid;

const SCANNER_PUBLICATION_STATE_UNKNOWN: u8 = 0;
const SCANNER_PUBLICATION_STATE_ALLOWED: u8 = 1;
const SCANNER_PUBLICATION_STATE_BLOCKED: u8 = 2;

pub(crate) const SCANNER_PUBLICATION_LEASE_MAX_ENTRIES: usize = 256;

/// A lease is deliberately short-lived.  The coordinator treats expiry as a
/// failed publication rather than silently continuing with a peer that may
/// have started movement after the lease was abandoned.
pub(crate) const SCANNER_PUBLICATION_LEASE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Observational leases hold data-movement fencing but may coexist with
/// ordinary namespace commits; authoritative leases require a stable namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScannerPublicationLeasePurpose {
    Authoritative,
    Observational,
}

/// Movement read admission granted with a publication lease. Lease-scoped
/// target operations share it rather than queueing a second read on the gate.
pub(crate) type ScannerPublicationLeasePermit = Arc<OwnedRwLockReadGuard<()>>;

pub(crate) struct ScannerPublicationLeaseEntry {
    pub(crate) expires_at: Instant,
    pub(crate) movement_generation: u64,
    pub(crate) namespace_generation: Option<u64>,
    pub(crate) purpose: ScannerPublicationLeasePurpose,
    pub(crate) operation_guard: ScannerPublicationLeasePermit,
}

pub(crate) struct NamespaceCommitGuard {
    ctx: Arc<InstanceContext>,
    counted: bool,
}

impl Drop for NamespaceCommitGuard {
    fn drop(&mut self) {
        if self.counted {
            // Publish the new generation before a zero-pending publication probe.
            self.ctx.advance_namespace_commit_generation();
            self.ctx.namespace_commits.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// A durable decommission pause as seen by this instance's worker watchdog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecommissionPauseRecord {
    pub(crate) reason: String,
    /// The pool's settled progress when it paused; see
    /// [`DecommissionCountLedger::progress`].
    pub(crate) progress: u64,
    /// Consecutive pauses on `reason` without progress in between.
    pub(crate) repeats: u32,
    /// The watchdog does not relaunch the pool before this instant.
    pub(crate) relaunch_after: Instant,
}

/// What this instance counted for one decommission generation of a pool.
///
/// Progress counters persist only at checkpoints and a resumed pass relists
/// the bucket, so a retained entry (one whose source stays) is seen again on
/// every pass. The ledger keeps its counted versions so a relist in the same
/// process does not count them twice.
#[derive(Debug, Default)]
pub(crate) struct DecommissionCountLedger {
    generation: Option<time::OffsetDateTime>,
    /// (bucket, object, version id, delete marker) of retained versions.
    retained: HashSet<(String, String, Option<uuid::Uuid>, bool)>,
    retained_key_bytes: usize,
    /// Entries whose source was cleaned up. Only moving an entry off the pool
    /// advances it; a relisted retained entry cannot.
    cleaned_entries: u64,
}

impl DecommissionCountLedger {
    const MAX_RETAINED_VERSIONS: usize = 1024;
    const MAX_RETAINED_KEY_BYTES: usize = 1024 * 1024;

    /// Whether this version is counted now: a retained version once, and a
    /// cleaned-up version unless it was already counted while retained.
    /// When the retained ledger is full, omit that progress count until source
    /// cleanup. This bounds diagnostic memory without evicting deduplication
    /// evidence or blocking data movement.
    pub(crate) fn admit(&mut self, bucket: &str, object: &str, version: (Option<uuid::Uuid>, bool), cleaned: bool) -> bool {
        let key_bytes = bucket.len().saturating_add(object.len());
        if cleaned {
            // Most successful entries never needed retained-version tracking.
            if self.retained.is_empty() || key_bytes > Self::MAX_RETAINED_KEY_BYTES {
                return true;
            }
            let key = (bucket.to_owned(), object.to_owned(), version.0, version.1);
            if self.retained.remove(&key) {
                self.retained_key_bytes -= key_bytes;
                return false;
            }
            return true;
        }
        if self.retained.len() >= Self::MAX_RETAINED_VERSIONS
            || key_bytes > Self::MAX_RETAINED_KEY_BYTES.saturating_sub(self.retained_key_bytes)
        {
            return false;
        }
        let key = (bucket.to_owned(), object.to_owned(), version.0, version.1);
        if self.retained.insert(key) {
            self.retained_key_bytes += key_bytes;
            true
        } else {
            false
        }
    }

    pub(crate) fn record_cleaned_entry(&mut self) {
        self.cleaned_entries = self.cleaned_entries.saturating_add(1);
    }

    pub(crate) fn progress(&self) -> u64 {
        self.cleaned_entries
    }
}

/// Runtime state owned by a single `ECStore` instance.
///
/// This is intentionally minimal in the first migration slice; subsequent
/// slices move additional identity/runtime state (topology, disk registry,
/// service handles, cancellation token) into this struct.
pub struct InstanceContext {
    /// The deployment's erasure setup type.
    ///
    /// Single source of truth for the derived `is_erasure` /
    /// `is_dist_erasure` / `is_erasure_sd` predicates, replacing the three
    /// previously-independent process-global erasure-mode bools (which could
    /// drift out of sync). Stored as one value so the three predicates can
    /// never observe a torn intermediate state.
    erasure_kind: RwLock<SetupType>,
    /// This instance's namespace lock manager (Phase 5 Slice 3, backlog#939).
    ///
    /// Owned per-instance so two instances no longer share a lock namespace
    /// (which would make same-named objects in different instances falsely
    /// contend). Single-instance aliases the process singleton, so behavior is
    /// unchanged; see [`bootstrap_ctx`].
    lock_manager: Arc<GlobalLockManager>,
    /// This instance's S3 region (Phase 5 Slice 4, backlog#939).
    ///
    /// Write-once like the process global it replaces: startup sets it exactly
    /// once and a duplicate write fails fast (see [`InstanceContext::set_region`]).
    region: OnceLock<Region>,
    /// This instance's deployment id (Phase 5 Slice 5, backlog#939).
    ///
    /// Write-once identity, mirrored by `ECStore::id` (both come from the same
    /// startup value). Replaces the process-global deployment-id static.
    deployment_id: OnceLock<Uuid>,
    /// This instance's endpoint topology (Phase 5 Slice 6, backlog#939).
    ///
    /// Write-once: storage startup publishes the server pools exactly once.
    /// Replaces the process-global endpoints static.
    endpoints: OnceLock<EndpointServerPools>,
    /// Only a complete validated topology may enable native local proof origin.
    multipart_proof_topology: OnceLock<([u8; 32], ProofVerificationContext)>,
    /// This instance's tier configuration manager (Phase 5 Slice 7, backlog#939).
    ///
    /// Lazily materialized on first access so `InstanceContext::new()` stays
    /// cheap: single-instance creates one shared manager on first use — exactly
    /// the "create-once, then share" semantics of the eager process static it
    /// replaces — while a genuinely independent instance gets its own.
    tier_config_mgr: OnceLock<Arc<RwLock<TierConfigMgr>>>,
    /// This instance's event notifier (Phase 5 Slice 8, backlog#939).
    ///
    /// Lazily materialized like the tier config manager; holds the instance's
    /// notification target list. Replaces the eager process static.
    event_notifier: OnceLock<Arc<RwLock<EventNotifier>>>,
    /// This instance's lifecycle expiry state (Phase 5 Slice 9, backlog#939).
    ///
    /// Lazily materialized; holds the instance's expiry worker channels/stats.
    /// Replaces the eager process static.
    expiry_state: OnceLock<Arc<RwLock<ExpiryState>>>,
    /// This instance's lifecycle transition state (Phase 5 Slice 9, backlog#939).
    ///
    /// Lazily materialized; holds the instance's transition workers/stats.
    /// Replaces the eager process static.
    transition_state: OnceLock<Arc<TransitionState>>,
    /// This instance's bucket bandwidth monitor (Phase 5 Slice 10, backlog#939).
    ///
    /// Set once at startup with the cluster node count (see
    /// [`InstanceContext::init_bucket_monitor`]); read as `None` until then.
    /// Replaces the process-global bucket-monitor static.
    bucket_monitor: OnceLock<Arc<Monitor>>,
    /// This instance's background replication stats (Phase 5 Slice 11, backlog#939).
    ///
    /// Async set-once (workers are spawned during init); read as `None` until
    /// `init_background_replication` runs. Replaces the process-global static.
    replication_stats: OnceCell<Arc<ReplicationStats>>,
    /// This instance's background replication pool (Phase 5 Slice 11, backlog#939).
    ///
    /// Async set-once, built with the live storage during init. Replaces the
    /// process-global static.
    replication_pool: OnceCell<Arc<DynReplicationPool>>,
    /// This instance's local disk registry (Phase 5 Slice 12, backlog#939).
    ///
    /// Populated during storage startup (before the `ECStore` exists) via the
    /// bootstrap context that the store then adopts, so startup writes and later
    /// reads share one registry. Replaces the three `GLOBAL_LOCAL_DISK_*`
    /// statics: the path→disk map, the disk-id→endpoint map, and the
    /// pool/set/drive layout.
    local_disk_map: Arc<RwLock<HashMap<String, Option<DiskStore>>>>,
    local_disk_id_map: Arc<RwLock<HashMap<Uuid, String>>>,
    local_disk_set_drives: Arc<RwLock<TypeLocalDiskSetDrives>>,
    /// This instance's bucket metadata system (backlog#1052 S3).
    ///
    /// Set once when storage startup initializes bucket metadata for this
    /// instance's store; holds the store handle plus the bucket→metadata
    /// cache, so it is inherently per-store. Replaces the process-global
    /// bucket-metadata static (whose double-init panicked process-wide).
    ///
    /// Uses `std::sync::Mutex` instead of `OnceLock` so test fixtures that
    /// share the bootstrap context can reinitialize the metadata system with
    /// their own `ECStore` without panicking. Production startup still sets
    /// this exactly once.
    bucket_metadata_sys: std::sync::Mutex<Option<Arc<RwLock<BucketMetadataSys>>>>,
    /// This instance's background-services cancellation token (Phase 5 Slice 13,
    /// backlog#939).
    ///
    /// Set once at startup; cancelling it stops this instance's background
    /// workers (scanner/heal/tier/lifecycle) without touching another instance.
    /// Replaces the process-global cancel-token static.
    background_cancel_token: OnceLock<CancellationToken>,
    /// Serializes data-movement transitions with scanner publication commits.
    /// Readers are held across one publication commit; movement transitions
    /// take the writer at their durable state commit boundary.
    data_movement_operation_gate: Arc<RwLock<()>>,
    /// Set while an operator decommission start runs on this instance. A
    /// start runs detached from its caller, so this keeps abandoned requests
    /// and their retries from stacking concurrent starts and preflight heals.
    decommission_start_in_flight: AtomicBool,
    /// The last durable pause of each local decommission, keyed by pool
    /// index. The worker watchdog backs off relaunching a pool that keeps
    /// pausing on the same cause without progress.
    decommission_pauses: std::sync::Mutex<HashMap<usize, DecommissionPauseRecord>>,
    /// Per-pool counting ledger of the current decommission generation.
    decommission_count_ledgers: std::sync::Mutex<HashMap<usize, DecommissionCountLedger>>,
    /// Remote scanner publication leases own a read guard until explicit
    /// release or bounded expiry.  Keeping the guard in storage-owned state
    /// makes a remote movement transition wait on the same fence as a local
    /// scanner commit.
    scanner_publication_leases: Arc<Mutex<HashMap<Uuid, ScannerPublicationLeaseEntry>>>,
    /// Monotonic admission epoch paired with the operation gate. A
    /// publication admitted before a movement transition must never be
    /// mistaken for one admitted after the transition.
    data_movement_operation_epoch: AtomicU64,
    /// Once the admission epoch reaches its reserved terminal value, no new
    /// publication may be admitted. Keeping this state separate from the
    /// saturating counter prevents an unchanged `u64::MAX` value from being
    /// mistaken for a fresh epoch after overflow.
    data_movement_operation_epoch_exhausted: AtomicBool,
    /// Storage-owned generation for movement state changes.  This is separate
    /// from the publication admission epoch so scanners can wait for a
    /// terminal/clear transition without treating the wake as a publication
    /// permit.
    data_movement_generation: AtomicU64,
    data_movement_generation_exhausted: AtomicBool,
    data_movement_generation_notify: Arc<Notify>,
    /// Last storage-owned movement snapshot observed under the operation
    /// gate. SetDisks cache writers fail closed until ECStore refreshes it.
    scanner_publication_state: AtomicU8,
    namespace_commits: AtomicU64,
    namespace_commit_generation: AtomicU64,
    /// Resolves object-encryption material at the application boundary.
    object_encryption_resolver: OnceLock<Arc<dyn ObjectEncryptionResolver>>,
    delete_recovery_hints: DeleteRecoveryHints,
    tier_delete_journal_recovery_stores: std::sync::Mutex<HashSet<Uuid>>,
    #[cfg(test)]
    suppress_tier_delete_journal_recovery: bool,
    transition_transaction_recovery_stores: std::sync::Mutex<HashSet<Uuid>>,
    tier_delete_journal_recovery_wakeup: tokio::sync::Notify,
    /// Object mutations that run on their own task so that dropping the
    /// caller (for example, an S3 client disconnect) cannot stop them between
    /// their on-disk steps. Shutdown waits for them after the HTTP drain.
    detached_mutations: TaskTracker,
}

impl InstanceContext {
    /// Create a fresh instance context in the initial [`SetupType::Unknown`]
    /// state with its own lock manager — byte-for-byte equivalent to the old
    /// all-`false` erasure globals.
    pub fn new() -> Self {
        Self::with_lock_manager(Arc::new(GlobalLockManager::new()))
    }

    /// Build a context bound to a specific lock manager.
    ///
    /// [`bootstrap_ctx`] uses this to alias the process-global lock manager so
    /// single-instance deployments keep one shared namespace; `new` mints a
    /// fresh manager for a genuinely independent instance.
    fn with_lock_manager(lock_manager: Arc<GlobalLockManager>) -> Self {
        Self {
            erasure_kind: RwLock::new(SetupType::Unknown),
            lock_manager,
            region: OnceLock::new(),
            deployment_id: OnceLock::new(),
            endpoints: OnceLock::new(),
            multipart_proof_topology: OnceLock::new(),
            tier_config_mgr: OnceLock::new(),
            event_notifier: OnceLock::new(),
            expiry_state: OnceLock::new(),
            transition_state: OnceLock::new(),
            bucket_monitor: OnceLock::new(),
            replication_stats: OnceCell::new(),
            replication_pool: OnceCell::new(),
            local_disk_map: Arc::new(RwLock::new(HashMap::new())),
            local_disk_id_map: Arc::new(RwLock::new(HashMap::new())),
            local_disk_set_drives: Arc::new(RwLock::new(Vec::new())),
            bucket_metadata_sys: std::sync::Mutex::new(None),
            background_cancel_token: OnceLock::new(),
            data_movement_operation_gate: Arc::new(RwLock::new(())),
            decommission_start_in_flight: AtomicBool::new(false),
            decommission_pauses: std::sync::Mutex::new(HashMap::new()),
            decommission_count_ledgers: std::sync::Mutex::new(HashMap::new()),
            scanner_publication_leases: Arc::new(Mutex::new(HashMap::new())),
            data_movement_operation_epoch: AtomicU64::new(0),
            data_movement_operation_epoch_exhausted: AtomicBool::new(false),
            data_movement_generation: AtomicU64::new(0),
            data_movement_generation_exhausted: AtomicBool::new(false),
            data_movement_generation_notify: Arc::new(Notify::new()),
            scanner_publication_state: AtomicU8::new(SCANNER_PUBLICATION_STATE_UNKNOWN),
            namespace_commits: AtomicU64::new(0),
            namespace_commit_generation: AtomicU64::new(0),
            object_encryption_resolver: OnceLock::new(),
            delete_recovery_hints: DeleteRecoveryHints::default(),
            tier_delete_journal_recovery_stores: std::sync::Mutex::new(HashSet::new()),
            #[cfg(test)]
            suppress_tier_delete_journal_recovery: false,
            transition_transaction_recovery_stores: std::sync::Mutex::new(HashSet::new()),
            tier_delete_journal_recovery_wakeup: tokio::sync::Notify::new(),
            detached_mutations: TaskTracker::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_lock_manager_for_test(lock_manager: Arc<GlobalLockManager>) -> Self {
        Self::with_lock_manager(lock_manager)
    }

    /// This instance's namespace lock manager.
    pub fn lock_manager(&self) -> Arc<GlobalLockManager> {
        self.lock_manager.clone()
    }

    /// Run a multi-step mutation on its own task and wait for its output.
    ///
    /// Dropping the returned future leaves the task running to completion, so
    /// the mutation, together with the locks and guards it owns, never stops
    /// between two of its on-disk steps because its caller went away.
    pub(crate) async fn run_detached_mutation<F>(&self, mutation: F) -> Result<F::Output, tokio::task::JoinError>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // Capture before spawning: Tokio tasks do not inherit request task-locals.
        #[cfg(feature = "diagnostic-read-version-caller")]
        let mutation = crate::cluster::rpc::runtime_sources::with_captured_read_version_context(
            rustfs_io_metrics::internode_metrics::ReadVersionContextBoundary::DetachedMutation,
            mutation,
        );
        #[cfg(not(feature = "diagnostic-read-version-caller"))]
        let context = rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context();
        #[cfg(not(feature = "diagnostic-read-version-caller"))]
        let mutation = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(context, mutation);
        self.detached_mutations.spawn(mutation.in_current_span()).await
    }

    /// Wait up to `timeout` for every detached mutation to finish. Returns
    /// `false` when some are still running at the deadline.
    pub async fn wait_for_detached_mutations(&self, timeout: std::time::Duration) -> bool {
        self.detached_mutations.close();
        tokio::time::timeout(timeout, self.detached_mutations.wait()).await.is_ok()
    }

    #[cfg(test)]
    pub(crate) fn detached_mutation_count(&self) -> usize {
        self.detached_mutations.len()
    }

    pub(crate) fn data_movement_operation_gate(&self) -> Arc<RwLock<()>> {
        Arc::clone(&self.data_movement_operation_gate)
    }

    /// Claim the single decommission start slot; `false` when a start is
    /// already running. Release it with [`Self::end_decommission_start`].
    pub(crate) fn try_begin_decommission_start(&self) -> bool {
        self.decommission_start_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn end_decommission_start(&self) {
        self.decommission_start_in_flight.store(false, Ordering::Release);
    }

    pub(crate) fn decommission_start_running(&self) -> bool {
        self.decommission_start_in_flight.load(Ordering::Acquire)
    }

    /// Record a durable pause of `pool_index`; `update` derives the new record
    /// from the previous one.
    pub(crate) fn record_decommission_pause(
        &self,
        pool_index: usize,
        update: impl FnOnce(Option<&DecommissionPauseRecord>) -> DecommissionPauseRecord,
    ) -> DecommissionPauseRecord {
        let mut pauses = self
            .decommission_pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = update(pauses.get(&pool_index));
        pauses.insert(pool_index, record.clone());
        record
    }

    /// Run `update` on `pool_index`'s counting ledger for `generation`,
    /// starting a fresh ledger when the generation changed.
    pub(crate) fn with_decommission_count_ledger<T>(
        &self,
        pool_index: usize,
        generation: time::OffsetDateTime,
        update: impl FnOnce(&mut DecommissionCountLedger) -> T,
    ) -> T {
        let mut ledgers = self
            .decommission_count_ledgers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ledger = ledgers.entry(pool_index).or_default();
        if ledger.generation != Some(generation) {
            *ledger = DecommissionCountLedger {
                generation: Some(generation),
                ..Default::default()
            };
        }
        update(ledger)
    }

    pub(crate) fn decommission_pause(&self, pool_index: usize) -> Option<DecommissionPauseRecord> {
        self.decommission_pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pool_index)
            .cloned()
    }

    pub(crate) async fn install_scanner_publication_lease(
        &self,
        token: Uuid,
        expires_at: Instant,
        movement_generation: u64,
        namespace_generation: Option<u64>,
        purpose: ScannerPublicationLeasePurpose,
        operation_guard: OwnedRwLockReadGuard<()>,
    ) -> bool {
        let mut leases = self.scanner_publication_leases.lock().await;
        if leases.len() >= SCANNER_PUBLICATION_LEASE_MAX_ENTRIES {
            return false;
        }
        leases.insert(
            token,
            ScannerPublicationLeaseEntry {
                expires_at,
                movement_generation,
                namespace_generation,
                purpose,
                operation_guard: Arc::new(operation_guard),
            },
        );
        true
    }

    pub(crate) async fn remove_scanner_publication_lease(&self, token: Uuid) -> bool {
        self.scanner_publication_leases.lock().await.remove(&token).is_some()
    }

    /// Return the generations and purpose from the same live lease while the
    /// caller holds the movement read guard. Namespace commits do not take that
    /// guard, so strict leases compare their saved generation after this await.
    /// The process-owned table rejects tokens from a prior instance or expiry.
    pub(crate) async fn scanner_publication_lease_generations(
        &self,
        token: Uuid,
    ) -> Option<(u64, Option<u64>, ScannerPublicationLeasePurpose)> {
        self.scanner_publication_lease_admission(token)
            .await
            .map(|(movement_generation, namespace_generation, purpose, _)| (movement_generation, namespace_generation, purpose))
    }

    /// Return the live lease's generations and purpose together with a share
    /// of its movement read admission. The share keeps movement writers out
    /// for as long as the caller holds it, even if the lease is released or
    /// expires meanwhile, without queueing a new read behind a waiting writer.
    pub(crate) async fn scanner_publication_lease_admission(
        &self,
        token: Uuid,
    ) -> Option<(u64, Option<u64>, ScannerPublicationLeasePurpose, ScannerPublicationLeasePermit)> {
        let mut leases = self.scanner_publication_leases.lock().await;
        let now = Instant::now();
        let (expires_at, movement_generation, namespace_generation, purpose, permit) = leases.get(&token).map(|entry| {
            (
                entry.expires_at,
                entry.movement_generation,
                entry.namespace_generation,
                entry.purpose,
                Arc::clone(&entry.operation_guard),
            )
        })?;
        if expires_at <= now {
            leases.remove(&token);
            return None;
        }
        Some((movement_generation, namespace_generation, purpose, permit))
    }

    pub(crate) async fn expire_scanner_publication_lease(&self, token: Uuid, expires_at: Instant) {
        let mut leases = self.scanner_publication_leases.lock().await;
        let should_remove = leases.get(&token).is_some_and(|entry| entry.expires_at <= expires_at);
        if should_remove {
            leases.remove(&token);
        }
    }

    pub(crate) fn data_movement_operation_epoch(&self) -> u64 {
        self.data_movement_operation_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn data_movement_operation_epoch_exhausted(&self) -> bool {
        self.data_movement_operation_epoch_exhausted.load(Ordering::Acquire)
    }

    pub(crate) fn data_movement_generation(&self) -> u64 {
        self.data_movement_generation.load(Ordering::Acquire)
    }

    pub(crate) fn data_movement_generation_exhausted(&self) -> bool {
        self.data_movement_generation_exhausted.load(Ordering::Acquire)
    }

    pub(crate) fn data_movement_generation_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.data_movement_generation_notify)
    }

    pub(crate) fn observe_durable_data_movement_generation(&self, generation: u64) {
        if generation == 0 || self.data_movement_generation_exhausted.load(Ordering::Acquire) {
            return;
        }
        let previous = self.data_movement_generation.fetch_max(generation, Ordering::AcqRel);
        if generation == u64::MAX {
            self.data_movement_generation_exhausted.store(true, Ordering::Release);
        }
        if generation > previous {
            self.data_movement_generation_notify.notify_waiters();
        }
    }

    pub(crate) fn scanner_publication_state_allowed(&self) -> bool {
        !self.data_movement_operation_epoch_exhausted()
            && !self.data_movement_generation_exhausted()
            && self.scanner_publication_state.load(Ordering::Acquire) == SCANNER_PUBLICATION_STATE_ALLOWED
    }

    pub(crate) fn begin_namespace_commit(self: &Arc<Self>) -> Arc<NamespaceCommitGuard> {
        let counted = self
            .namespace_commits
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_add(1))
            .is_ok();
        if counted {
            self.advance_namespace_commit_generation();
        } else {
            self.namespace_commit_generation.store(u64::MAX, Ordering::Release);
        }
        Arc::new(NamespaceCommitGuard {
            ctx: Arc::clone(self),
            counted,
        })
    }

    fn advance_namespace_commit_generation(&self) {
        let _ = self
            .namespace_commit_generation
            .try_update(Ordering::AcqRel, Ordering::Acquire, |generation| Some(generation.saturating_add(1)));
    }

    pub(crate) fn namespace_commit_generation(&self) -> u64 {
        self.namespace_commit_generation.load(Ordering::Acquire)
    }

    /// Whether a namespace commit is still pending. The scanner publication
    /// barrier no longer consults it (an ordinary PUT shades its bucket
    /// instead); fixtures use it to drain rename tails deterministically.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn namespace_commits_pending(&self) -> bool {
        self.namespace_commits.load(Ordering::Acquire) != 0 || self.namespace_commit_generation() == u64::MAX
    }

    pub(crate) fn set_scanner_publication_state(&self, blocked: bool) {
        self.scanner_publication_state.store(
            if blocked {
                SCANNER_PUBLICATION_STATE_BLOCKED
            } else {
                SCANNER_PUBLICATION_STATE_ALLOWED
            },
            Ordering::Release,
        );
    }

    pub(crate) fn advance_data_movement_operation_epoch(&self) -> u64 {
        let (previous, result) = self.advance_data_movement_operation_epoch_only();
        if result != previous {
            let _ = self.advance_data_movement_generation();
        }
        result
    }

    pub(crate) fn advance_data_movement_operation_epoch_to_durable_generation(&self, generation: u64) -> u64 {
        let (_, result) = self.advance_data_movement_operation_epoch_only();
        self.observe_durable_data_movement_generation(generation);
        result
    }

    fn advance_data_movement_operation_epoch_only(&self) -> (u64, u64) {
        self.scanner_publication_state
            .store(SCANNER_PUBLICATION_STATE_UNKNOWN, Ordering::Release);
        let previous = self.data_movement_operation_epoch.load(Ordering::Acquire);
        let _ = self
            .data_movement_operation_epoch
            .try_update(Ordering::AcqRel, Ordering::Acquire, |epoch| Some(epoch.saturating_add(1)));
        let result = self.data_movement_operation_epoch.load(Ordering::Acquire);
        if result == u64::MAX {
            self.data_movement_operation_epoch_exhausted.store(true, Ordering::Release);
        }
        (previous, result)
    }

    /// Advance the movement generation after a durable movement transition.
    /// The generation is deliberately bounded: once it reaches `u64::MAX`,
    /// publication and generation-based waits fail closed rather than reusing
    /// an indistinguishable saturated value.
    pub(crate) fn advance_data_movement_generation(&self) -> Option<u64> {
        if self.data_movement_generation_exhausted.load(Ordering::Acquire) {
            return None;
        }
        let updated = self
            .data_movement_generation
            .try_update(Ordering::AcqRel, Ordering::Acquire, |generation| generation.checked_add(1));
        match updated {
            Ok(previous) => {
                let Some(generation) = previous.checked_add(1) else {
                    self.data_movement_generation_exhausted.store(true, Ordering::Release);
                    return None;
                };
                if generation == u64::MAX {
                    self.data_movement_generation_exhausted.store(true, Ordering::Release);
                }
                self.data_movement_generation_notify.notify_waiters();
                Some(generation)
            }
            Err(_) => {
                self.data_movement_generation_exhausted.store(true, Ordering::Release);
                None
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn set_data_movement_operation_epoch_for_test(&self, epoch: u64) {
        self.data_movement_operation_epoch.store(epoch, Ordering::Release);
        self.data_movement_operation_epoch_exhausted
            .store(epoch == u64::MAX, Ordering::Release);
        self.scanner_publication_state
            .store(SCANNER_PUBLICATION_STATE_UNKNOWN, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn set_namespace_commit_generation_for_test(&self, generation: u64) {
        self.namespace_commit_generation.store(generation, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn set_data_movement_generation_for_test(&self, generation: u64) {
        self.data_movement_generation.store(generation, Ordering::Release);
        self.data_movement_generation_exhausted
            .store(generation == u64::MAX, Ordering::Release);
    }

    /// Install the application-owned object-encryption resolver once.
    pub fn set_object_encryption_resolver(
        &self,
        resolver: Arc<dyn ObjectEncryptionResolver>,
    ) -> Result<(), Arc<dyn ObjectEncryptionResolver>> {
        self.object_encryption_resolver.set(resolver)
    }

    /// Return the configured object-encryption resolver, if startup installed one.
    pub fn object_encryption_resolver(&self) -> Option<&dyn ObjectEncryptionResolver> {
        self.object_encryption_resolver.get().map(Arc::as_ref)
    }

    /// Set this instance's S3 region.
    ///
    /// Write-once: panics on a second write, preserving the startup fail-fast
    /// contract of the process global it replaces.
    pub fn set_region(&self, region: Region) {
        self.region
            .set(region)
            .expect("instance region should be initialized once during startup");
    }

    /// This instance's S3 region, if it has been set.
    pub fn region(&self) -> Option<Region> {
        self.region.get().cloned()
    }

    /// Set this instance's deployment id.
    ///
    /// Write-once: panics on a second write, preserving the startup fail-fast
    /// contract of the process global it replaces.
    pub fn set_deployment_id(&self, id: Uuid) {
        self.deployment_id
            .set(id)
            .expect("instance deployment id should be initialized once during startup");
    }

    /// This instance's deployment id, if it has been set.
    pub fn deployment_id(&self) -> Option<Uuid> {
        self.deployment_id.get().copied()
    }

    /// Set this instance's endpoint topology.
    ///
    /// Write-once: panics on a second write, preserving the startup fail-fast
    /// contract of the process global it replaces.
    pub fn set_endpoints(&self, endpoints: EndpointServerPools) {
        self.endpoints
            .set(endpoints)
            .expect("instance endpoints should be initialized once during storage startup");
    }

    /// This instance's endpoint topology, if it has been set.
    pub fn endpoints(&self) -> Option<EndpointServerPools> {
        self.endpoints.get().cloned()
    }

    /// Keep one authority identity across complete local registry reinitializations.
    /// Changing the instance or crossing the local/RPC mode boundary is forbidden.
    pub(crate) fn bind_multipart_proof_topology(
        &self,
        endpoints: &EndpointServerPools,
    ) -> crate::disk::error::Result<ProofVerificationContext> {
        // Registry vectors are indexed by pool_idx. Validate every index before
        // adding endpoint permissions or allowing a caller to clear the registry.
        if endpoints.as_ref().iter().enumerate().any(|(pool, entry)| {
            entry
                .endpoints
                .as_ref()
                .iter()
                .any(|endpoint| usize::try_from(endpoint.pool_idx).ok() != Some(pool))
        }) {
            return Err(crate::disk::error::DiskError::InconsistentDisk);
        }
        let (identity, authority) = ProofVerificationContext::for_topology(endpoints)?;
        let stored = self.multipart_proof_topology.get_or_init(|| (identity, authority.clone()));
        if stored.0 != identity {
            stored.1.register_topology(&authority)?;
        }
        Ok(stored.1.clone())
    }

    pub(crate) fn multipart_proof_verification_context(&self) -> ProofVerificationContext {
        self.multipart_proof_topology
            .get()
            .map(|(_, authority)| authority.clone())
            .unwrap_or_default()
    }

    /// This instance's tier configuration manager, materializing it on first
    /// access. Shared for the lifetime of the instance.
    pub fn tier_config_mgr(&self) -> Arc<RwLock<TierConfigMgr>> {
        self.tier_config_mgr.get_or_init(TierConfigMgr::new).clone()
    }

    /// This instance's event notifier, materializing it on first access.
    /// Shared for the lifetime of the instance.
    pub fn event_notifier(&self) -> Arc<RwLock<EventNotifier>> {
        self.event_notifier.get_or_init(EventNotifier::new).clone()
    }

    /// This instance's lifecycle expiry state, materializing it on first
    /// access. Shared for the lifetime of the instance.
    pub fn expiry_state(&self) -> Arc<RwLock<ExpiryState>> {
        self.expiry_state.get_or_init(ExpiryState::new).clone()
    }

    /// This instance's lifecycle transition state, materializing it on first
    /// access. Shared for the lifetime of the instance.
    pub fn transition_state(&self) -> Arc<TransitionState> {
        self.transition_state.get_or_init(TransitionState::new).clone()
    }

    /// Initialize this instance's bucket bandwidth monitor with the cluster
    /// node count. Returns `false` if it was already initialized (the caller
    /// logs; matches the process global's ignore-and-warn behavior).
    pub fn init_bucket_monitor(&self, num_nodes: u64) -> bool {
        self.bucket_monitor.set(Monitor::new(num_nodes)).is_ok()
    }

    /// This instance's bucket bandwidth monitor, if it has been initialized.
    pub fn bucket_monitor(&self) -> Option<Arc<Monitor>> {
        self.bucket_monitor.get().cloned()
    }

    /// This instance's background replication stats, if initialized.
    pub fn replication_stats(&self) -> Option<Arc<ReplicationStats>> {
        self.replication_stats.get().cloned()
    }

    /// This instance's background replication pool, if initialized.
    pub fn replication_pool(&self) -> Option<Arc<DynReplicationPool>> {
        self.replication_pool.get().cloned()
    }

    /// Whether background replication has been initialized for this instance.
    pub fn replication_initialized(&self) -> bool {
        self.replication_stats.get().is_some() && self.replication_pool.get().is_some()
    }

    /// The replication stats cell, for the async `init_background_replication`
    /// owner to `get_or_init`. Exposed to the replication module only.
    pub(crate) fn replication_stats_cell(&self) -> &OnceCell<Arc<ReplicationStats>> {
        &self.replication_stats
    }

    /// The replication pool cell, for the async `init_background_replication`
    /// owner to `get_or_init`. Exposed to the replication module only.
    pub(crate) fn replication_pool_cell(&self) -> &OnceCell<Arc<DynReplicationPool>> {
        &self.replication_pool
    }

    /// Handle to this instance's local path→disk map.
    pub(crate) fn local_disk_map(&self) -> Arc<RwLock<HashMap<String, Option<DiskStore>>>> {
        self.local_disk_map.clone()
    }

    /// Handle to this instance's local disk-id→endpoint map.
    pub(crate) fn local_disk_id_map(&self) -> Arc<RwLock<HashMap<Uuid, String>>> {
        self.local_disk_id_map.clone()
    }

    /// Handle to this instance's local pool/set/drive layout.
    pub(crate) fn local_disk_set_drives(&self) -> Arc<RwLock<TypeLocalDiskSetDrives>> {
        self.local_disk_set_drives.clone()
    }

    /// Set this instance's bucket metadata system. Replaces any existing
    /// value so test fixtures that share the bootstrap context can
    /// reinitialize with their own store. Returns `true` if this is the
    /// first initialization (no previous value existed).
    pub fn init_bucket_metadata_sys(&self, sys: Arc<RwLock<BucketMetadataSys>>) -> bool {
        let mut guard = self.bucket_metadata_sys.lock().expect("bucket_metadata_sys lock poisoned");
        let is_first = guard.is_none();
        *guard = Some(sys);
        is_first
    }

    /// This instance's bucket metadata system, if initialized.
    pub fn bucket_metadata_sys(&self) -> Option<Arc<RwLock<BucketMetadataSys>>> {
        self.bucket_metadata_sys
            .lock()
            .expect("bucket_metadata_sys lock poisoned")
            .clone()
    }

    /// Remove the `BucketMetadataSys` when its backing `ECStore` is gone.
    ///
    /// Called from `ECStore::drop`. When test fixtures share the bootstrap
    /// context, the `BucketMetadataSys` stores a `Weak<ECStore>` to prevent a
    /// reference cycle; once the store drops the weak is dangling and any
    /// `object_store()` call panics. Clearing the slot here lets the next
    /// fixture install a fresh `BucketMetadataSys` without hitting the stale one.
    pub fn clear_bucket_metadata_sys_if_stale(&self) {
        let mut guard = self.bucket_metadata_sys.lock().expect("bucket_metadata_sys lock poisoned");
        if let Some(sys) = guard.as_ref() {
            // `read()` may block briefly; the async RwLock is acceptable here
            // because Drop is sync and the lock is held only long enough to
            // check the weak reference.
            let is_stale = {
                let s = sys.try_read();
                match s {
                    Ok(s) => s.is_store_stale(),
                    Err(_) => true, // contended: treat as stale-safe to clear
                }
            };
            if is_stale {
                *guard = None;
            }
        }
    }

    /// Set this instance's background-services cancellation token (once).
    pub fn init_background_cancel_token(&self, token: CancellationToken) -> Result<(), CancellationToken> {
        self.background_cancel_token.set(token)
    }

    /// This instance's background-services cancellation token, if initialized.
    pub fn background_cancel_token(&self) -> Option<CancellationToken> {
        self.background_cancel_token.get().cloned()
    }

    pub(crate) fn bind_background_cancel_token(&self, token: CancellationToken) -> CancellationToken {
        self.background_cancel_token.get_or_init(|| token).clone()
    }

    pub(crate) fn delete_recovery_hints(&self) -> &DeleteRecoveryHints {
        &self.delete_recovery_hints
    }

    pub(crate) fn mark_tier_delete_journal_recovery_started(&self, store_id: Uuid) -> bool {
        #[cfg(test)]
        if self.suppress_tier_delete_journal_recovery {
            return false;
        }
        self.tier_delete_journal_recovery_stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(store_id)
    }

    #[cfg(test)]
    pub(crate) fn suppress_tier_delete_journal_recovery_for_test(&mut self) {
        self.suppress_tier_delete_journal_recovery = true;
    }

    pub(crate) fn mark_transition_transaction_recovery_started(&self, store_id: Uuid) -> bool {
        self.transition_transaction_recovery_stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(store_id)
    }

    pub(crate) fn wake_tier_delete_journal_recovery(&self) {
        self.tier_delete_journal_recovery_wakeup.notify_one();
    }

    pub(crate) async fn wait_for_tier_delete_journal_recovery(&self) {
        self.tier_delete_journal_recovery_wakeup.notified().await;
    }

    /// Update this instance's erasure setup type.
    pub async fn update_erasure_type(&self, setup_type: SetupType) {
        *self.erasure_kind.write().await = setup_type;
    }

    /// Whether this instance uses erasure coding.
    ///
    /// True for both single-node ([`SetupType::Erasure`]) and distributed
    /// ([`SetupType::DistErasure`]) erasure setups, matching the original
    /// `update_erasure_type` derivation where `DistErasure` implied
    /// `is_erasure == true`.
    pub async fn is_erasure(&self) -> bool {
        matches!(*self.erasure_kind.read().await, SetupType::Erasure | SetupType::DistErasure)
    }

    /// Whether this instance uses distributed erasure coding.
    pub async fn is_dist_erasure(&self) -> bool {
        *self.erasure_kind.read().await == SetupType::DistErasure
    }

    /// Whether this instance uses single-drive erasure coding.
    pub async fn is_erasure_sd(&self) -> bool {
        *self.erasure_kind.read().await == SetupType::ErasureSD
    }
}

impl Default for InstanceContext {
    fn default() -> Self {
        Self::new()
    }
}

// Manual Debug: service handles (e.g. the tier config manager) are not Debug,
// so summarize their materialization state rather than their contents.
impl std::fmt::Debug for InstanceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceContext")
            .field("erasure_kind", &self.erasure_kind)
            .field("region", &self.region)
            .field("deployment_id", &self.deployment_id)
            .field("endpoints", &self.endpoints)
            .field("tier_config_mgr_set", &self.tier_config_mgr.get().is_some())
            .field("event_notifier_set", &self.event_notifier.get().is_some())
            .field("expiry_state_set", &self.expiry_state.get().is_some())
            .field("transition_state_set", &self.transition_state.get().is_some())
            .field("bucket_monitor_set", &self.bucket_monitor.get().is_some())
            .field(
                "bucket_metadata_sys_set",
                &self.bucket_metadata_sys.lock().map(|g| g.is_some()).unwrap_or(false),
            )
            .field("replication_stats_set", &self.replication_stats.get().is_some())
            .field("replication_pool_set", &self.replication_pool.get().is_some())
            .field("background_cancel_token_set", &self.background_cancel_token.get().is_some())
            .field(
                "tier_delete_journal_recovery_store_count",
                &self
                    .tier_delete_journal_recovery_stores
                    .lock()
                    .map_or(0, |stores| stores.len()),
            )
            .field(
                "transition_transaction_recovery_store_count",
                &self
                    .transition_transaction_recovery_stores
                    .lock()
                    .map_or(0, |stores| stores.len()),
            )
            .finish_non_exhaustive()
    }
}

/// Process-level bootstrap instance context.
///
/// Storage startup writes erasure state before any `ECStore` exists (see
/// `init_startup_storage_foundation`), so the context must exist ahead of the
/// object graph. `ECStore::new` adopts this same `Arc` via [`bootstrap_ctx`],
/// guaranteeing that startup writes and post-construction reads share one cell.
static BOOTSTRAP_CTX: OnceLock<Arc<InstanceContext>> = OnceLock::new();

/// Return the process-level bootstrap instance context, creating it on first
/// access.
///
/// This is the single-instance default that the legacy free-function facade
/// falls back to before an `ECStore` is published, and the `Arc` that
/// `ECStore::new` adopts as its own `ctx`.
pub fn bootstrap_ctx() -> Arc<InstanceContext> {
    BOOTSTRAP_CTX
        .get_or_init(|| Arc::new(InstanceContext::with_lock_manager(get_global_lock_manager())))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decommission_count_ledger_bounds_retained_entries() {
        let mut ledger = super::DecommissionCountLedger::default();
        for index in 0..1024 {
            assert!(ledger.admit("bucket", &format!("key-{index}"), (None, false), false));
        }
        assert!(!ledger.admit("bucket", "overflow", (None, false), false));
        assert_eq!(ledger.retained.len(), 1024);
        // An uncounted retained version can still be counted after cleanup.
        assert!(ledger.admit("bucket", "overflow", (None, false), true));
        assert!(!ledger.admit("bucket", "key-0", (None, false), true));
        assert!(ledger.admit("bucket", "next", (None, false), false));
        assert!(!ledger.admit("bucket", "next", (None, false), false));
        assert_eq!(ledger.retained.len(), 1024);
    }

    #[test]
    fn decommission_count_ledger_bounds_retained_key_bytes() {
        let mut ledger = super::DecommissionCountLedger::default();
        let bucket = "b".repeat(63);
        let object_prefix = "x".repeat(1020);
        let mut admitted = 0;
        for index in 0..1024 {
            admitted += usize::from(ledger.admit(&bucket, &format!("{object_prefix}{index:04}"), (None, false), false));
        }
        assert!(admitted > 0 && admitted < 1024);
        assert!(ledger.retained.iter().map(|(b, o, _, _)| b.len() + o.len()).sum::<usize>() <= 1024 * 1024);
        // Removal releases the byte reservation as well as the entry slot.
        assert!(!ledger.admit(&bucket, &format!("{object_prefix}0000"), (None, false), true));
        assert!(ledger.admit(&bucket, &format!("{object_prefix}1024"), (None, false), false));
    }

    #[test]
    fn namespace_commit_guards_are_instance_local_and_count_until_last_owner() {
        let first = Arc::new(InstanceContext::new());
        let other = Arc::new(InstanceContext::new());
        first.set_scanner_publication_state(false);
        other.set_scanner_publication_state(false);
        assert!(first.scanner_publication_state_allowed());
        let one = first.begin_namespace_commit();
        let shared_owner = Arc::clone(&one);
        let two = first.begin_namespace_commit();
        assert!(first.namespace_commits_pending());
        assert!(first.scanner_publication_state_allowed(), "pending writes must not block scan admission");
        assert_eq!(first.namespace_commit_generation(), 2);
        assert!(!other.namespace_commits_pending());
        assert_eq!(other.namespace_commit_generation(), 0);
        assert!(other.scanner_publication_state_allowed());
        drop(one);
        assert_eq!(first.namespace_commit_generation(), 2);
        drop(shared_owner);
        assert!(first.namespace_commits_pending());
        assert_eq!(first.namespace_commit_generation(), 3);
        drop(two);
        assert!(!first.namespace_commits_pending());
        assert_eq!(first.namespace_commit_generation(), 4);
        assert!(first.scanner_publication_state_allowed());
    }

    #[test]
    fn namespace_commit_counter_exhaustion_keeps_publication_blocked() {
        for (count, generation) in [(0, u64::MAX - 1), (u64::MAX, 0)] {
            let ctx = Arc::new(InstanceContext::new());
            ctx.set_scanner_publication_state(false);
            ctx.namespace_commits.store(count, Ordering::Release);
            ctx.namespace_commit_generation.store(generation, Ordering::Release);
            let guard = ctx.begin_namespace_commit();
            assert!(ctx.namespace_commits_pending());
            assert_eq!(ctx.namespace_commit_generation(), u64::MAX);
            drop(guard);
            assert!(ctx.namespace_commits_pending());
            assert_eq!(ctx.namespace_commit_generation(), u64::MAX);
            assert_eq!(ctx.namespace_commits.load(Ordering::Acquire), count);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn scanner_lease_generations_remain_bound_until_expiry() {
        let ctx = Arc::new(InstanceContext::new());
        let token = Uuid::new_v4();
        let gate = ctx.data_movement_operation_gate();
        let permit = gate.clone().read_owned().await;
        assert!(
            ctx.install_scanner_publication_lease(
                token,
                Instant::now() + SCANNER_PUBLICATION_LEASE_TTL,
                7,
                Some(11),
                ScannerPublicationLeasePurpose::Authoritative,
                permit,
            )
            .await
        );
        drop(ctx.begin_namespace_commit());
        assert_eq!(ctx.namespace_commit_generation(), 2);
        assert_eq!(
            ctx.scanner_publication_lease_generations(token).await,
            Some((7, Some(11), ScannerPublicationLeasePurpose::Authoritative))
        );
        assert!(gate.clone().try_write_owned().is_err(), "lookup must retain the stored permit");
        tokio::time::advance(SCANNER_PUBLICATION_LEASE_TTL).await;
        assert_eq!(ctx.scanner_publication_lease_generations(token).await, None);
        assert!(!ctx.remove_scanner_publication_lease(token).await);
        assert!(gate.try_write_owned().is_ok(), "expiry releases the stored permit");
    }

    // The SetupType inputs must derive the exact (is_erasure,
    // is_dist_erasure, is_erasure_sd) triples that the original three
    // process-global erasure bools produced via update_erasure_type().
    #[tokio::test]
    async fn erasure_predicates_match_legacy_derivation() {
        let cases = [
            // (input, is_erasure, is_dist_erasure, is_erasure_sd)
            (SetupType::Unknown, false, false, false),
            (SetupType::FS, false, false, false),
            (SetupType::Erasure, true, false, false),
            // DistErasure implies is_erasure == true (legacy global.rs:267-269).
            (SetupType::DistErasure, true, true, false),
            (SetupType::ErasureSD, false, false, true),
        ];

        for (input, want_erasure, want_dist, want_sd) in cases {
            let ctx = InstanceContext::new();
            ctx.update_erasure_type(input.clone()).await;
            assert_eq!(ctx.is_erasure().await, want_erasure, "is_erasure for {input:?}");
            assert_eq!(ctx.is_dist_erasure().await, want_dist, "is_dist_erasure for {input:?}");
            assert_eq!(ctx.is_erasure_sd().await, want_sd, "is_erasure_sd for {input:?}");
        }
    }

    // A detached mutation keeps running after its caller is dropped, and
    // shutdown waits for it instead of tearing it down mid-way.
    #[tokio::test]
    async fn detached_mutation_outlives_dropped_caller_and_is_awaited() {
        let ctx = InstanceContext::new();
        let finished = Arc::new(AtomicBool::new(false));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let mut caller = Box::pin(ctx.run_detached_mutation({
            let finished = Arc::clone(&finished);
            async move {
                let _ = release_rx.await;
                finished.store(true, Ordering::Release);
            }
        }));
        assert!(futures::poll!(caller.as_mut()).is_pending());
        drop(caller);
        assert_eq!(ctx.detached_mutation_count(), 1, "dropping the caller must not cancel the mutation");

        assert!(
            !ctx.wait_for_detached_mutations(std::time::Duration::from_millis(50)).await,
            "the wait must report a mutation that is still running"
        );
        release_tx.send(()).expect("the detached mutation should still be waiting");
        assert!(ctx.wait_for_detached_mutations(std::time::Duration::from_secs(5)).await);
        assert!(finished.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn detached_mutations_keep_operation_and_guards_after_caller_drops() {
        use rustfs_io_metrics::s3_http_metrics::{
            S3HttpRequestGuard, current_s3_http_operation_context, current_s3_http_operation_name, with_s3_http_operation_context,
        };

        let mut request = S3HttpRequestGuard::new("DELETE");
        let operation = request.in_scope(|| {
            rustfs_io_metrics::s3_http_metrics::observe_s3_http_operation(rustfs_s3_ops::S3Operation::DeleteObject);
            current_s3_http_operation_context().expect("delete operation was captured")
        });
        for parent in [Some(operation), None] {
            let ctx = InstanceContext::new();
            let gate = Arc::new(RwLock::new(()));
            let guard = gate.clone().read_owned().await;
            let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
            let (result_tx, result_rx) = tokio::sync::oneshot::channel();
            let expected = parent.map(|_| "s3:DeleteObject");
            let mut caller = Box::pin(with_s3_http_operation_context(
                parent,
                ctx.run_detached_mutation(async move {
                    release_rx.await.expect("release detached mutation");
                    result_tx
                        .send(current_s3_http_operation_name())
                        .expect("report detached operation");
                    drop(guard);
                }),
            ));
            assert!(futures::poll!(caller.as_mut()).is_pending());
            drop(caller);
            assert_eq!(current_s3_http_operation_name(), None, "parent scope was restored");
            assert_eq!(ctx.detached_mutation_count(), 1);
            assert!(gate.try_write().is_err(), "mutation retains the owned guard after caller cancellation");
            release_tx.send(()).expect("detached mutation is still alive");
            assert_eq!(result_rx.await.expect("detached operation result"), expected);
            assert!(ctx.wait_for_detached_mutations(std::time::Duration::from_secs(5)).await);
            assert!(gate.try_write().is_ok(), "completion releases the mutation guard");
        }
    }

    #[tokio::test]
    async fn detached_mutation_captures_operation_when_polled_and_keeps_results() {
        use rustfs_io_metrics::s3_http_metrics::{
            S3HttpRequestGuard, current_s3_http_operation_context, current_s3_http_operation_name, observe_s3_http_operation,
            with_s3_http_operation_context,
        };

        let mut request = S3HttpRequestGuard::new("PUT");
        let operation = request.in_scope(|| {
            observe_s3_http_operation(rustfs_s3_ops::S3Operation::PutObject);
            current_s3_http_operation_context().expect("PUT operation context")
        });
        let ctx = InstanceContext::new();
        let mutation = ctx.run_detached_mutation(async { (current_s3_http_operation_name(), Err::<(), _>("mutation failure")) });
        let result = with_s3_http_operation_context(Some(operation), mutation)
            .await
            .expect("mutation task completes");
        assert_eq!(result, (Some("s3:PutObject"), Err("mutation failure")));
        assert_eq!(current_s3_http_operation_name(), None);

        let mutation = request.in_scope(|| ctx.run_detached_mutation(async { current_s3_http_operation_name() }));
        assert_eq!(mutation.await.expect("unscoped poll completes"), None);

        let failure =
            with_s3_http_operation_context(Some(operation), ctx.run_detached_mutation(async { panic!("mutation panic") }))
                .await
                .expect_err("child panic remains a join failure");
        assert!(failure.is_panic());
        assert_eq!(current_s3_http_operation_name(), None);
        assert!(ctx.wait_for_detached_mutations(std::time::Duration::from_secs(5)).await);
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[tokio::test]
    async fn detached_mutations_keep_existing_context_after_waiter_drop_and_nested_spawn() {
        use crate::cluster::rpc::runtime_sources::{
            current_read_version_entry_source, current_read_version_request_caller, with_read_version_entry,
            with_read_version_request_caller,
        };
        use rustfs_io_metrics::internode_metrics::{ReadVersionEntrySource, ReadVersionRequestCaller};

        let ctx = Arc::new(InstanceContext::new());
        let child_ctx = Arc::clone(&ctx);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let mut caller = Box::pin(with_read_version_request_caller(ReadVersionRequestCaller::Object, async {
            with_read_version_entry(ReadVersionEntrySource::LifecycleRecovery, async {
                ctx.run_detached_mutation(async move {
                    entered_tx.send(()).expect("mutation caller is present");
                    release_rx.await.expect("mutation may finish");
                    let observed = with_read_version_entry(ReadVersionEntrySource::ConfigRead, async {
                        child_ctx
                            .run_detached_mutation(async {
                                (current_read_version_request_caller(), current_read_version_entry_source())
                            })
                            .await
                            .expect("nested mutation joins")
                    })
                    .await;
                    finished_tx.send(observed).expect("completion observer remains live");
                })
                .await
            })
            .await
        }));
        assert!(futures::poll!(caller.as_mut()).is_pending());
        entered_rx.await.expect("mutation entered");
        drop(caller);
        assert_eq!(current_read_version_request_caller(), None);
        assert_eq!(current_read_version_entry_source(), None);
        assert_eq!(ctx.detached_mutation_count(), 1);
        release_tx.send(()).expect("mutation remains alive after waiter drop");
        assert_eq!(
            finished_rx.await.expect("nested mutation completes"),
            (Some(ReadVersionRequestCaller::Object), Some(ReadVersionEntrySource::LifecycleRecovery))
        );
        assert_eq!(
            ctx.run_detached_mutation(async { (current_read_version_request_caller(), current_read_version_entry_source()) })
                .await
                .expect("unscoped mutation joins"),
            (None, None)
        );
        assert!(ctx.wait_for_detached_mutations(std::time::Duration::from_secs(5)).await);
    }

    // A fresh context (before any update) reflects the initial all-false state.
    #[tokio::test]
    async fn fresh_context_is_all_false() {
        let ctx = InstanceContext::new();
        assert!(!ctx.is_erasure().await);
        assert!(!ctx.is_dist_erasure().await);
        assert!(!ctx.is_erasure_sd().await);
    }

    // bootstrap_ctx() is a stable process singleton: repeated calls return the
    // same Arc, so a startup write is visible to a later read through it.
    #[tokio::test]
    async fn bootstrap_ctx_is_stable_singleton() {
        let a = bootstrap_ctx();
        let b = bootstrap_ctx();
        assert!(Arc::ptr_eq(&a, &b), "bootstrap_ctx must return the same Arc");
    }

    // Two independent contexts do not share erasure state — the property that
    // lets two ECStore instances (each carrying its own ctx) stay isolated.
    #[tokio::test]
    async fn distinct_contexts_do_not_share_state() {
        let ctx_a = Arc::new(InstanceContext::new());
        let ctx_b = Arc::new(InstanceContext::new());
        ctx_a.update_erasure_type(SetupType::DistErasure).await;
        ctx_b.update_erasure_type(SetupType::ErasureSD).await;

        assert!(ctx_a.is_dist_erasure().await && ctx_a.is_erasure().await);
        assert!(!ctx_a.is_erasure_sd().await);

        assert!(ctx_b.is_erasure_sd().await);
        assert!(!ctx_b.is_erasure().await && !ctx_b.is_dist_erasure().await);
    }

    // Single-instance zero-change: the bootstrap context's lock manager is the
    // very same Arc the process singleton hands out, so the lock namespace is
    // unchanged from before Slice 3.
    #[tokio::test]
    async fn bootstrap_lock_manager_aliases_process_singleton() {
        assert!(
            Arc::ptr_eq(&bootstrap_ctx().lock_manager(), &get_global_lock_manager()),
            "bootstrap context must alias the process lock-manager singleton"
        );
    }

    // Two independent contexts own distinct lock managers — the property that
    // stops two instances from falsely contending on same-named objects.
    #[tokio::test]
    async fn distinct_contexts_have_distinct_lock_managers() {
        let ctx_a = InstanceContext::new();
        let ctx_b = InstanceContext::new();
        assert!(
            !Arc::ptr_eq(&ctx_a.lock_manager(), &ctx_b.lock_manager()),
            "fresh contexts must not share a lock manager"
        );
    }

    fn region(name: &str) -> Region {
        name.parse().expect("valid test region")
    }

    // A fresh context has no region until set; set-then-get round-trips.
    #[tokio::test]
    async fn region_set_and_get() {
        let ctx = InstanceContext::new();
        assert_eq!(ctx.region(), None);
        ctx.set_region(region("us-east-1"));
        assert_eq!(ctx.region(), Some(region("us-east-1")));
    }

    // Two contexts hold independent regions.
    #[tokio::test]
    async fn distinct_contexts_have_distinct_region() {
        let ctx_a = InstanceContext::new();
        let ctx_b = InstanceContext::new();
        ctx_a.set_region(region("us-east-1"));
        ctx_b.set_region(region("eu-west-1"));
        assert_eq!(ctx_a.region(), Some(region("us-east-1")));
        assert_eq!(ctx_b.region(), Some(region("eu-west-1")));
    }

    // The region is write-once: a second set fails fast, preserving the
    // startup contract of the process global it replaced.
    #[tokio::test]
    #[should_panic(expected = "initialized once")]
    async fn set_region_twice_panics() {
        let ctx = InstanceContext::new();
        ctx.set_region(region("us-east-1"));
        ctx.set_region(region("eu-west-1"));
    }

    // A fresh context has no deployment id until set; set-then-get round-trips.
    #[tokio::test]
    async fn deployment_id_set_and_get() {
        let ctx = InstanceContext::new();
        assert_eq!(ctx.deployment_id(), None);
        let id = Uuid::new_v4();
        ctx.set_deployment_id(id);
        assert_eq!(ctx.deployment_id(), Some(id));
    }

    // Two contexts hold independent deployment ids.
    #[tokio::test]
    async fn distinct_contexts_have_distinct_deployment_id() {
        let ctx_a = InstanceContext::new();
        let ctx_b = InstanceContext::new();
        let id_a = Uuid::new_v4();
        let id_b = Uuid::new_v4();
        ctx_a.set_deployment_id(id_a);
        ctx_b.set_deployment_id(id_b);
        assert_eq!(ctx_a.deployment_id(), Some(id_a));
        assert_eq!(ctx_b.deployment_id(), Some(id_b));
    }

    // The deployment id is write-once: a second set fails fast.
    #[tokio::test]
    #[should_panic(expected = "initialized once")]
    async fn set_deployment_id_twice_panics() {
        let ctx = InstanceContext::new();
        ctx.set_deployment_id(Uuid::new_v4());
        ctx.set_deployment_id(Uuid::new_v4());
    }

    fn endpoints_with_pools(n: usize) -> EndpointServerPools {
        use crate::layout::endpoint::Endpoint;
        use crate::layout::endpoints::{Endpoints, PoolEndpoints};
        let pool = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(vec![Endpoint::try_from("http://127.0.0.1:9000/data0").expect("valid endpoint")]),
            cmd_line: "test".to_string(),
            platform: "test".to_string(),
        };
        EndpointServerPools((0..n).map(|_| pool.clone()).collect())
    }

    // A fresh context has no endpoints until set; set-then-get round-trips.
    #[tokio::test]
    async fn endpoints_set_and_get() {
        let ctx = InstanceContext::new();
        assert!(ctx.endpoints().is_none());
        ctx.set_endpoints(endpoints_with_pools(1));
        assert_eq!(ctx.endpoints().expect("endpoints set").0.len(), 1);
    }

    // Two contexts hold independent endpoint topologies.
    #[tokio::test]
    async fn distinct_contexts_have_distinct_endpoints() {
        let ctx_a = InstanceContext::new();
        let ctx_b = InstanceContext::new();
        ctx_a.set_endpoints(endpoints_with_pools(1));
        ctx_b.set_endpoints(endpoints_with_pools(2));
        assert_eq!(ctx_a.endpoints().expect("a").0.len(), 1);
        assert_eq!(ctx_b.endpoints().expect("b").0.len(), 2);
    }

    // Endpoints are write-once: a second set fails fast.
    #[tokio::test]
    #[should_panic(expected = "initialized once")]
    async fn set_endpoints_twice_panics() {
        let ctx = InstanceContext::new();
        ctx.set_endpoints(endpoints_with_pools(1));
        ctx.set_endpoints(endpoints_with_pools(2));
    }

    // The tier config manager is materialized once and shared for the
    // instance's lifetime — the "create-once, then share" contract of the
    // eager process static it replaced.
    #[tokio::test]
    async fn tier_config_mgr_is_stable_within_an_instance() {
        let ctx = InstanceContext::new();
        assert!(
            Arc::ptr_eq(&ctx.tier_config_mgr(), &ctx.tier_config_mgr()),
            "repeated access must return the same manager"
        );
    }

    // Two contexts own distinct tier config managers.
    #[tokio::test]
    async fn distinct_contexts_have_distinct_tier_config_mgr() {
        let ctx_a = InstanceContext::new();
        let ctx_b = InstanceContext::new();
        assert!(
            !Arc::ptr_eq(&ctx_a.tier_config_mgr(), &ctx_b.tier_config_mgr()),
            "fresh contexts must not share a tier config manager"
        );
    }

    // The event notifier is materialized once and shared within an instance.
    #[tokio::test]
    async fn event_notifier_is_stable_within_an_instance() {
        let ctx = InstanceContext::new();
        assert!(
            Arc::ptr_eq(&ctx.event_notifier(), &ctx.event_notifier()),
            "repeated access must return the same notifier"
        );
    }

    // Two contexts own distinct event notifiers.
    #[tokio::test]
    async fn distinct_contexts_have_distinct_event_notifier() {
        let ctx_a = InstanceContext::new();
        let ctx_b = InstanceContext::new();
        assert!(
            !Arc::ptr_eq(&ctx_a.event_notifier(), &ctx_b.event_notifier()),
            "fresh contexts must not share an event notifier"
        );
    }

    // Lifecycle expiry/transition state are each stable within an instance and
    // distinct across instances.
    #[tokio::test]
    async fn lifecycle_state_is_stable_within_and_distinct_across_instances() {
        let ctx_a = InstanceContext::new();
        let ctx_b = InstanceContext::new();

        assert!(Arc::ptr_eq(&ctx_a.expiry_state(), &ctx_a.expiry_state()));
        assert!(Arc::ptr_eq(&ctx_a.transition_state(), &ctx_a.transition_state()));

        assert!(!Arc::ptr_eq(&ctx_a.expiry_state(), &ctx_b.expiry_state()));
        assert!(!Arc::ptr_eq(&ctx_a.transition_state(), &ctx_b.transition_state()));
    }

    // The bucket monitor is None until initialized, set-once (second init is a
    // no-op returning false), and independent across instances.
    #[tokio::test]
    async fn bucket_monitor_init_once_and_isolated() {
        let ctx_a = InstanceContext::new();
        assert!(ctx_a.bucket_monitor().is_none());
        assert!(ctx_a.init_bucket_monitor(4));
        assert!(ctx_a.bucket_monitor().is_some());
        assert!(!ctx_a.init_bucket_monitor(8), "second init must be ignored");

        let ctx_b = InstanceContext::new();
        assert!(ctx_b.bucket_monitor().is_none(), "a fresh instance has its own uninitialized monitor");
    }

    // Replication state is None until initialized, and independent per instance.
    // (The real async init spawns workers and needs live storage; here we only
    // exercise the read side and cross-instance isolation.)
    #[tokio::test]
    async fn replication_state_is_isolated() {
        let ctx_a = InstanceContext::new();
        assert!(ctx_a.replication_stats().is_none());
        assert!(ctx_a.replication_pool().is_none());
        assert!(!ctx_a.replication_initialized());

        ctx_a.replication_stats_cell().set(Arc::new(ReplicationStats::new())).ok();
        assert!(ctx_a.replication_stats().is_some());
        // The pool is still unset, so replication is not fully initialized.
        assert!(!ctx_a.replication_initialized());

        let ctx_b = InstanceContext::new();
        assert!(
            ctx_b.replication_stats().is_none(),
            "a distinct instance has independent replication state"
        );
    }

    // The background cancel token is set-once, and cancelling one instance's
    // token leaves a distinct instance's uninitialized.
    #[tokio::test]
    async fn background_cancel_token_init_once_and_isolated() {
        let ctx_a = InstanceContext::new();
        assert!(ctx_a.background_cancel_token().is_none());

        let token = CancellationToken::new();
        ctx_a.init_background_cancel_token(token.clone()).expect("init once");
        assert!(ctx_a.background_cancel_token().is_some());
        assert!(
            ctx_a.init_background_cancel_token(CancellationToken::new()).is_err(),
            "second init rejected"
        );

        let ctx_b = InstanceContext::new();
        assert!(ctx_b.background_cancel_token().is_none(), "distinct instance is independent");

        token.cancel();
        assert!(ctx_a.background_cancel_token().unwrap().is_cancelled());
    }

    #[test]
    fn tier_delete_journal_recovery_is_deduplicated_per_store_and_instance() {
        let ctx_a = InstanceContext::new();
        let ctx_b = InstanceContext::new();
        let store_a = Uuid::new_v4();
        let store_b = Uuid::new_v4();

        assert!(ctx_a.mark_tier_delete_journal_recovery_started(store_a));
        assert!(!ctx_a.mark_tier_delete_journal_recovery_started(store_a));
        assert!(ctx_a.mark_tier_delete_journal_recovery_started(store_b));
        assert!(ctx_b.mark_tier_delete_journal_recovery_started(store_a));

        let mut manual_ctx = InstanceContext::new();
        manual_ctx.suppress_tier_delete_journal_recovery_for_test();
        assert!(!manual_ctx.mark_tier_delete_journal_recovery_started(store_a));
        assert!(!manual_ctx.mark_tier_delete_journal_recovery_started(store_b));
        assert!(ctx_b.mark_tier_delete_journal_recovery_started(store_b));
    }

    #[test]
    fn background_cancel_token_binds_the_provided_shutdown_token() {
        let ctx = InstanceContext::new();
        let shutdown = CancellationToken::new();
        let first = ctx.bind_background_cancel_token(shutdown.clone());
        let second = ctx.bind_background_cancel_token(CancellationToken::new());

        shutdown.cancel();
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert!(
            ctx.background_cancel_token()
                .expect("shutdown token should be published")
                .is_cancelled()
        );
    }

    // Phase 5 acceptance (backlog#939): two independent instance contexts share
    // NONE of the runtime state that used to live in process globals. This is
    // the end-to-end proof that the object-graph isolation carrier works — every
    // field migrated across Slices 1-13 is verified independent here.
    #[tokio::test]
    async fn two_instances_isolate_all_migrated_state() {
        let a = InstanceContext::new();
        let b = InstanceContext::new();

        // Erasure setup type (Slice 1).
        a.update_erasure_type(SetupType::DistErasure).await;
        b.update_erasure_type(SetupType::ErasureSD).await;
        assert!(a.is_dist_erasure().await && !b.is_dist_erasure().await);
        assert!(b.is_erasure_sd().await && !a.is_erasure_sd().await);

        // Lock manager (Slice 3).
        assert!(!Arc::ptr_eq(&a.lock_manager(), &b.lock_manager()));

        // Region (Slice 4).
        a.set_region("us-east-1".parse().expect("region"));
        b.set_region("eu-west-1".parse().expect("region"));
        assert_eq!(a.region().expect("a region"), "us-east-1".parse().expect("region"));
        assert_eq!(b.region().expect("b region"), "eu-west-1".parse().expect("region"));

        // Deployment id (Slice 5).
        let (id_a, id_b) = (Uuid::new_v4(), Uuid::new_v4());
        a.set_deployment_id(id_a);
        b.set_deployment_id(id_b);
        assert_eq!(a.deployment_id(), Some(id_a));
        assert_eq!(b.deployment_id(), Some(id_b));

        // Service handles (Slices 7-11): each instance materializes its own.
        assert!(!Arc::ptr_eq(&a.tier_config_mgr(), &b.tier_config_mgr()));
        assert!(!Arc::ptr_eq(&a.event_notifier(), &b.event_notifier()));
        assert!(!Arc::ptr_eq(&a.expiry_state(), &b.expiry_state()));
        assert!(!Arc::ptr_eq(&a.transition_state(), &b.transition_state()));

        // Local disk registry (Slice 12): a write to one is invisible to the other.
        a.local_disk_map().write().await.insert("/disk-a".to_string(), None);
        assert_eq!(a.local_disk_map().read().await.len(), 1);
        assert_eq!(b.local_disk_map().read().await.len(), 0);

        // Bucket monitor (Slice 10): set-once per instance.
        assert!(a.init_bucket_monitor(4));
        assert!(a.bucket_monitor().is_some() && b.bucket_monitor().is_none());

        // Background cancel token (Slice 13): cancelling one doesn't touch the other.
        let token_a = CancellationToken::new();
        a.init_background_cancel_token(token_a.clone()).expect("init a");
        token_a.cancel();
        assert!(a.background_cancel_token().expect("a token").is_cancelled());
        assert!(b.background_cancel_token().is_none());
    }
}

#[cfg(test)]
mod multipart_instance_authority_tests {
    use super::*;
    use crate::layout::endpoint::Endpoint;
    use crate::layout::endpoints::PoolEndpoints;

    fn local_topology(root: &str) -> EndpointServerPools {
        let mut endpoint = Endpoint::try_from(root).expect("local endpoint should parse");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: vec![endpoint].into(),
            cmd_line: String::new(),
            platform: String::new(),
        }])
    }

    #[test]
    fn multipart_instance_authority_is_explicit_stable_across_local_topologies() {
        let ctx = InstanceContext::new();
        assert!(matches!(ctx.multipart_proof_verification_context(), ProofVerificationContext::RpcOnly));
        let topology = local_topology("/tmp/multipart-instance-a");
        let first = ctx
            .bind_multipart_proof_topology(&topology)
            .expect("complete local topology should bind");
        let again = ctx
            .bind_multipart_proof_topology(&topology)
            .expect("reopening the same topology should retain authority");
        assert!(first.same_authority(&again));
        assert!(first.same_authority(&ctx.multipart_proof_verification_context()));
        let replacement = local_topology("/tmp/multipart-instance-b");
        let extended = ctx
            .bind_multipart_proof_topology(&replacement)
            .expect("a complete local reinitialization should retain the native owner");
        assert!(first.same_authority(&extended));
        first.validate_endpoint(&topology.0[0].endpoints.as_ref()[0]).unwrap();
        first.validate_endpoint(&replacement.0[0].endpoints.as_ref()[0]).unwrap();
        assert!(first.same_authority(&ctx.multipart_proof_verification_context()));
        let independent = InstanceContext::new().bind_multipart_proof_topology(&topology).unwrap();
        assert!(!first.same_authority(&independent));
    }

    #[test]
    fn multipart_distributed_instance_never_becomes_local_after_reinitialization() {
        let ctx = InstanceContext::new();
        let mut topology = local_topology("/tmp/multipart-remote-instance");
        topology.0[0].endpoints.as_mut()[0].is_local = false;
        assert!(matches!(
            ctx.bind_multipart_proof_topology(&topology).unwrap(),
            ProofVerificationContext::RpcOnly
        ));
        topology.0[0].endpoints.as_mut()[0].is_local = true;
        assert!(ctx.bind_multipart_proof_topology(&topology).is_err());
        assert!(matches!(ctx.multipart_proof_verification_context(), ProofVerificationContext::RpcOnly));
    }

    #[test]
    fn multipart_parallel_topology_bind_retains_one_instance_authority() {
        let ctx = Arc::new(InstanceContext::new());
        let topologies = (0..4)
            .map(|slot| local_topology(&format!("/tmp/multipart-parallel-instance-{slot}")))
            .collect::<Vec<_>>();
        let authorities = std::thread::scope(|scope| {
            let mut tasks = Vec::with_capacity(topologies.len());
            for topology in &topologies {
                let ctx = ctx.clone();
                tasks.push(scope.spawn(move || ctx.bind_multipart_proof_topology(topology).unwrap()));
            }
            tasks.into_iter().map(|task| task.join().unwrap()).collect::<Vec<_>>()
        });
        assert!(authorities.iter().all(|authority| authority.same_authority(&authorities[0])));
        for topology in topologies {
            authorities[0]
                .validate_endpoint(&topology.0[0].endpoints.as_ref()[0])
                .unwrap();
        }
    }
    #[test]
    fn multipart_topology_registration_rejects_invalid_indices_and_mode_switch_without_granting_endpoints() {
        let ctx = InstanceContext::new();
        let original = local_topology("/tmp/multipart-original-registry");
        let first = ctx.bind_multipart_proof_topology(&original).unwrap();
        let candidate = local_topology("/tmp/multipart-unregistered-candidate");
        let mut outside = candidate.clone();
        outside.0[0].endpoints.as_mut()[0].set_pool_index(1);
        assert!(
            ctx.bind_multipart_proof_topology(&outside).is_err(),
            "one pool cannot address registry slot one"
        );
        assert!(first.validate_endpoint(&outside.0[0].endpoints.as_ref()[0]).is_err());
        let fresh = InstanceContext::new();
        assert!(fresh.bind_multipart_proof_topology(&outside).is_err());
        assert!(
            fresh.multipart_proof_topology.get().is_none(),
            "invalid initial registration must have no authority effect"
        );

        let mut sparse = candidate.clone();
        let mut second = local_topology("/tmp/multipart-unregistered-second").0.remove(0);
        second.endpoints.as_mut()[0].set_pool_index(2);
        sparse.0.push(second);
        assert!(ctx.bind_multipart_proof_topology(&sparse).is_err());
        assert!(
            first.validate_endpoint(&candidate.0[0].endpoints.as_ref()[0]).is_err(),
            "a valid early pool cannot be partially granted"
        );
        let mut mixed = candidate.clone();
        let mut second = local_topology("/tmp/multipart-remote-later-pool").0.remove(0);
        second.endpoints.as_mut()[0].set_pool_index(1);
        second.endpoints.as_mut()[0].is_local = false;
        mixed.0.push(second);
        assert!(
            ctx.bind_multipart_proof_topology(&mixed).is_err(),
            "local authority cannot switch mode for a remote later pool"
        );
        assert!(first.validate_endpoint(&candidate.0[0].endpoints.as_ref()[0]).is_err());
        let mut incomplete = candidate.clone();
        incomplete.0[0].drives_per_set = 2;
        assert!(ctx.bind_multipart_proof_topology(&incomplete).is_err());
        assert!(first.validate_endpoint(&candidate.0[0].endpoints.as_ref()[0]).is_err());
        assert!(first.same_authority(&ctx.multipart_proof_verification_context()));
        first.validate_endpoint(&original.0[0].endpoints.as_ref()[0]).unwrap();
    }
}
