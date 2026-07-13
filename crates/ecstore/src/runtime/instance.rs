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
use crate::layout::endpoints::{EndpointServerPools, SetupType};
use crate::services::event_notification::EventNotifier;
use crate::services::tier::tier::TierConfigMgr;
use rustfs_lock::{GlobalLockManager, get_global_lock_manager};
use s3s::region::Region;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::{OnceCell, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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
        }
    }

    /// This instance's namespace lock manager.
    pub fn lock_manager(&self) -> Arc<GlobalLockManager> {
        self.lock_manager.clone()
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

    /// Set this instance's background-services cancellation token (once).
    pub fn init_background_cancel_token(&self, token: CancellationToken) -> Result<(), CancellationToken> {
        self.background_cancel_token.set(token)
    }

    /// This instance's background-services cancellation token, if initialized.
    pub fn background_cancel_token(&self) -> Option<CancellationToken> {
        self.background_cancel_token.get().cloned()
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
