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

use crate::layout::endpoints::SetupType;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

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
}

impl InstanceContext {
    /// Create a fresh instance context in the initial [`SetupType::Unknown`]
    /// state — byte-for-byte equivalent to the old all-`false` erasure globals.
    pub fn new() -> Self {
        Self {
            erasure_kind: RwLock::new(SetupType::Unknown),
        }
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
    BOOTSTRAP_CTX.get_or_init(|| Arc::new(InstanceContext::new())).clone()
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
}
