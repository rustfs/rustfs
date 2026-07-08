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

//! Per-instance runtime identity context (issue #939, Phase 5 — Slice1).
//!
//! Phase 1–4 (#653) unified *access* to ECStore's runtime globals behind
//! accessor APIs but kept the underlying `OnceLock`/`RwLock` statics
//! process-global — deleting the single-instance guard (#3243) would not
//! isolate anything, since two `RustFSServer`s still share one
//! `GLOBAL_OBJECT_API`. This type is the isolation carrier those slices
//! deferred.
//!
//! The isolation vehicle is **the object graph**, not `tokio::task_local`:
//! `task_local` does not propagate across `tokio::spawn`, and ecstore has
//! 155+ internal spawns whose closures already `move` the object `Arc`s.
//! Each [`crate::store::ECStore`] holds an `Arc<InstanceContext>`, so method
//! bodies read `self.ctx` and reach every spawned child without threading a
//! `&ctx` argument through call sites.
//!
//! Slice1 migrates one representative group — the erasure setup type, formerly
//! three independent `RwLock<bool>` statics (`GLOBAL_IS_ERASURE`,
//! `GLOBAL_IS_DIST_ERASURE`, `GLOBAL_IS_ERASURE_SD`) — into a single
//! `RwLock<SetupType>` whose booleans are *derived*. Those three booleans were
//! three separate mutable truth sources kept consistent only by the implicit
//! derivation inside `update_erasure_type`; collapsing them removes the torn
//! intermediate states that design allowed.

use crate::layout::endpoints::SetupType;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

/// Runtime-identity state that distinguishes one ECStore instance from another.
///
/// Slice1 carries only the erasure setup type. Later slices fold in the
/// remaining Tier A runtime-identity globals (topology scalars, disk registry,
/// service handles, background-task lifetime) as separate fields.
#[derive(Debug)]
pub(crate) struct InstanceContext {
    /// Single source of truth for the deployment's setup type. The legacy
    /// `is_erasure`/`is_dist_erasure`/`is_erasure_sd` booleans are derived from
    /// this value rather than stored independently.
    erasure_kind: RwLock<SetupType>,
    /// Per-instance local lock manager (issue #939, Slice3). `SetDisks` reads
    /// this off `self.ctx` so two ECStore instances get *separate* lock
    /// namespaces instead of colliding on one process-global manager. The
    /// bootstrap context deliberately holds the process-global manager (see
    /// [`bootstrap_ctx`]) so single-instance locking is byte-for-byte unchanged.
    local_lock_manager: Arc<rustfs_lock::GlobalLockManager>,
    /// The instance's S3 region (issue #939, Slice4 — topology scalars). A
    /// write-once cell that preserves the former `GLOBAL_REGION` fail-fast
    /// contract *per instance*: a second write is a startup bug and panics.
    region: OnceLock<s3s::region::Region>,
}

impl InstanceContext {
    /// Fresh context in the pre-startup default state — equivalent to the old
    /// three booleans all initialized to `false` (`SetupType::Unknown`).
    ///
    /// Mints its **own** lock manager, so a context built this way is fully
    /// isolated from the process-global one. Production goes through
    /// [`bootstrap_ctx`], which adopts the global manager instead.
    pub(crate) fn new() -> Self {
        Self::with_lock_manager(Arc::new(rustfs_lock::GlobalLockManager::new()))
    }

    /// Build a context around a specific lock manager. Lets [`bootstrap_ctx`]
    /// adopt the process-global manager while [`new`](Self::new) mints an
    /// isolated one.
    fn with_lock_manager(local_lock_manager: Arc<rustfs_lock::GlobalLockManager>) -> Self {
        Self {
            erasure_kind: RwLock::new(SetupType::Unknown),
            local_lock_manager,
            region: OnceLock::new(),
        }
    }

    /// This instance's local lock manager. `SetDisks::new` stores the result so
    /// the set layer locks within this instance's namespace.
    pub(crate) fn local_lock_manager(&self) -> Arc<rustfs_lock::GlobalLockManager> {
        self.local_lock_manager.clone()
    }

    /// Publish this instance's region. Fail-fast, exactly as the old
    /// `set_global_region`: a second write means startup published conflicting
    /// region state, so it panics rather than silently keeping the first value.
    pub(crate) fn set_region(&self, region: s3s::region::Region) {
        self.region
            .set(region)
            .expect("instance region should be initialized once during startup");
    }

    /// This instance's region, if published.
    pub(crate) fn region(&self) -> Option<s3s::region::Region> {
        self.region.get().cloned()
    }

    /// True when the deployment is erasure-coded — single-node multi-drive
    /// (`Erasure`) *or* distributed (`DistErasure`).
    ///
    /// Mirrors the legacy derivation exactly: `update_erasure_type` set
    /// `is_erasure = true` whenever the type was `DistErasure`, so both variants
    /// report `true` here.
    pub(crate) async fn is_erasure(&self) -> bool {
        matches!(*self.erasure_kind.read().await, SetupType::Erasure | SetupType::DistErasure)
    }

    /// True only for the distributed erasure setup type.
    pub(crate) async fn is_dist_erasure(&self) -> bool {
        *self.erasure_kind.read().await == SetupType::DistErasure
    }

    /// True only for the single-drive erasure setup type.
    pub(crate) async fn is_erasure_sd(&self) -> bool {
        *self.erasure_kind.read().await == SetupType::ErasureSD
    }

    /// Replace the setup type. Single write path: the three derived booleans can
    /// never disagree, unlike the former three-lock `update_erasure_type`.
    pub(crate) async fn set_erasure_kind(&self, setup_type: SetupType) {
        *self.erasure_kind.write().await = setup_type;
    }
}

impl Default for InstanceContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-wide bootstrap context.
///
/// During startup, erasure/disk-table state is published *before* any
/// `ECStore` exists. To keep the startup write and the post-construction read
/// hitting the *same* cell, `ECStore::new` adopts this exact `Arc` instead of
/// minting a fresh context. Single-instance semantics are therefore
/// byte-for-byte unchanged: the compatibility facade (`is_erasure()` &c.)
/// resolves to the live store's `ctx` once it exists, and to this bootstrap
/// context before then — the same underlying `InstanceContext`.
static BOOTSTRAP_CTX: OnceLock<Arc<InstanceContext>> = OnceLock::new();

/// Get (initializing on first call) the process bootstrap context.
///
/// Adopts the process-global lock manager (`rustfs_lock::get_global_lock_manager`)
/// rather than minting a fresh one, so `SetDisks` built for the single process
/// instance lock in exactly the same namespace as every other direct caller of
/// `get_global_lock_manager()` — byte-for-byte unchanged locking behavior.
pub(crate) fn bootstrap_ctx() -> Arc<InstanceContext> {
    BOOTSTRAP_CTX
        .get_or_init(|| Arc::new(InstanceContext::with_lock_manager(rustfs_lock::get_global_lock_manager())))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::endpoints::SetupType;

    /// Legacy `update_erasure_type` derivation, replayed field-by-field, so the
    /// context predicates can be checked byte-for-byte against the old three
    /// booleans for every setup type.
    fn legacy_booleans(setup_type: &SetupType) -> (bool, bool, bool) {
        let mut is_erasure = *setup_type == SetupType::Erasure;
        let is_dist_erasure = *setup_type == SetupType::DistErasure;
        if is_dist_erasure {
            is_erasure = true;
        }
        let is_erasure_sd = *setup_type == SetupType::ErasureSD;
        (is_erasure, is_dist_erasure, is_erasure_sd)
    }

    #[tokio::test]
    async fn erasure_predicates_match_legacy_derivation() {
        for setup_type in [
            SetupType::Unknown,
            SetupType::FS,
            SetupType::ErasureSD,
            SetupType::Erasure,
            SetupType::DistErasure,
        ] {
            let ctx = InstanceContext::new();
            ctx.set_erasure_kind(setup_type.clone()).await;

            let (want_erasure, want_dist, want_sd) = legacy_booleans(&setup_type);
            assert_eq!(ctx.is_erasure().await, want_erasure, "is_erasure mismatch for {setup_type:?}");
            assert_eq!(ctx.is_dist_erasure().await, want_dist, "is_dist_erasure mismatch for {setup_type:?}");
            assert_eq!(ctx.is_erasure_sd().await, want_sd, "is_erasure_sd mismatch for {setup_type:?}");
        }
    }

    #[tokio::test]
    async fn fresh_context_is_all_false() {
        let ctx = InstanceContext::new();
        assert!(!ctx.is_erasure().await);
        assert!(!ctx.is_dist_erasure().await);
        assert!(!ctx.is_erasure_sd().await);
    }

    #[tokio::test]
    async fn bootstrap_ctx_is_stable_singleton() {
        let a = bootstrap_ctx();
        let b = bootstrap_ctx();
        assert!(Arc::ptr_eq(&a, &b), "bootstrap_ctx must return the same Arc");
    }

    #[tokio::test]
    async fn distinct_contexts_do_not_share_state() {
        let a = InstanceContext::new();
        let b = InstanceContext::new();
        a.set_erasure_kind(SetupType::DistErasure).await;
        b.set_erasure_kind(SetupType::ErasureSD).await;

        assert!(a.is_dist_erasure().await);
        assert!(!a.is_erasure_sd().await);
        assert!(b.is_erasure_sd().await);
        assert!(!b.is_dist_erasure().await);
    }

    /// Slice3: the bootstrap context must reuse the process-global lock manager,
    /// so single-instance locking is unchanged (other direct callers of
    /// `get_global_lock_manager()` lock in the same namespace as `SetDisks`).
    #[tokio::test]
    async fn bootstrap_ctx_adopts_global_lock_manager() {
        let ctx = bootstrap_ctx();
        assert!(
            Arc::ptr_eq(&ctx.local_lock_manager(), &rustfs_lock::get_global_lock_manager()),
            "bootstrap ctx must reuse the process-global lock manager"
        );
    }

    /// Slice3: a freshly-minted context gets its OWN lock manager — the seam
    /// that gives a second instance an isolated lock namespace.
    #[tokio::test]
    async fn fresh_context_has_isolated_lock_manager() {
        let a = InstanceContext::new();
        let b = InstanceContext::new();
        assert!(
            !Arc::ptr_eq(&a.local_lock_manager(), &b.local_lock_manager()),
            "two fresh contexts must have distinct lock managers"
        );
        assert!(
            !Arc::ptr_eq(&a.local_lock_manager(), &rustfs_lock::get_global_lock_manager()),
            "a fresh context must not share the process-global lock manager"
        );
    }

    fn test_region(name: &str) -> s3s::region::Region {
        s3s::region::Region::new(name.into()).expect("valid test region")
    }

    /// Slice4: region round-trips through the context and starts empty.
    #[tokio::test]
    async fn region_round_trips_through_context() {
        let ctx = InstanceContext::new();
        assert!(ctx.region().is_none(), "region starts unset");
        ctx.set_region(test_region("us-east-1"));
        assert_eq!(ctx.region().map(|r| r.as_str().to_string()), Some("us-east-1".to_string()));
    }

    /// Slice4: two instances hold independent regions — the isolation this slice
    /// delivers for the topology scalar.
    #[tokio::test]
    async fn distinct_contexts_have_independent_regions() {
        let a = InstanceContext::new();
        let b = InstanceContext::new();
        a.set_region(test_region("us-east-1"));
        b.set_region(test_region("eu-west-2"));
        assert_eq!(a.region().unwrap().as_str(), "us-east-1");
        assert_eq!(b.region().unwrap().as_str(), "eu-west-2");
    }

    /// Slice4: the former `GLOBAL_REGION` fail-fast contract is preserved
    /// per-instance — a second write is a startup bug and panics.
    #[tokio::test]
    #[should_panic(expected = "initialized once")]
    async fn set_region_twice_fails_fast() {
        let ctx = InstanceContext::new();
        ctx.set_region(test_region("us-east-1"));
        ctx.set_region(test_region("eu-west-2"));
    }
}
