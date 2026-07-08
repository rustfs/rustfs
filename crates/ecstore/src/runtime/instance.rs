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

use crate::bucket::lifecycle::bucket_lifecycle_ops::{ExpiryState, TransitionState};
use crate::layout::endpoints::{EndpointServerPools, SetupType};
use crate::services::event_notification::EventNotifier;
use crate::services::tier::tier::TierConfigMgr;
use rustfs_lock::{GlobalLockManager, get_global_lock_manager};
use s3s::region::Region;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
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
}
