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

//! `TlsReloadAdapter<M>` — the single entry-point that connects a target to
//! the TLS reload coordinator.  Each target holds an `Option<TlsReloadAdapter<M>>`
//! and calls `current_material()` on the hot path.  When `None`, the target
//! falls back to its legacy inline fingerprint logic.

use super::config::TlsReloadOptions;
use super::coordinator::TargetTlsReloadCoordinator;
use super::state::{TargetTlsRuntimeState, TargetTlsStatusSnapshot};
use super::r#trait::ReloadableTargetTls;
use std::sync::Arc;
use tracing::warn;

/// Bridges a `ReloadableTargetTls` implementor and the reload coordinator.
///
/// Created via [`TlsReloadAdapter::try_register`].  Holds the coordinator-
/// managed runtime state and exposes a zero-cost `current_material()` accessor
/// for the send hot-path.
pub struct TlsReloadAdapter<M> {
    runtime_state: Arc<TargetTlsRuntimeState<M>>,
    options: TlsReloadOptions,
}

impl<M> std::fmt::Debug for TlsReloadAdapter<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsReloadAdapter")
            .field("target_label", &self.runtime_state.inputs.target_label)
            .finish_non_exhaustive()
    }
}

impl<M> Clone for TlsReloadAdapter<M> {
    fn clone(&self) -> Self {
        Self {
            runtime_state: Arc::clone(&self.runtime_state),
            options: self.options.clone(),
        }
    }
}

impl<M: Send + Sync + 'static> TlsReloadAdapter<M> {
    /// Registers `target` with the coordinator and returns an adapter.
    ///
    /// On success the coordinator has:
    /// - built initial TLS material
    /// - spawned a background poll loop
    ///
    /// On failure returns `None` (the caller should keep its inline fallback
    /// path intact — the target continues to work, just without coordinator
    /// support).
    pub async fn try_register<T: ReloadableTargetTls<Material = M>>(
        target: Arc<T>,
        options: TlsReloadOptions,
        coordinator: &TargetTlsReloadCoordinator,
    ) -> Option<Self> {
        let label = target.tls_input_set().target_label.clone();
        match coordinator.register(target, options.clone()).await {
            Ok(runtime_state) => {
                tracing::info!(target = %label, "TLS reload adapter registered");
                Some(Self { runtime_state, options })
            }
            Err(err) => {
                warn!(target = %label, error = %err, "TLS reload adapter registration failed; target will use inline fallback");
                None
            }
        }
    }

    /// Hot-path accessor: returns the current TLS material managed by the
    /// coordinator.  The returned `Arc<M>` is cheap to clone.
    #[inline]
    pub fn current_material(&self) -> Arc<M> {
        Arc::clone(&self.runtime_state.current.load().material)
    }

    /// Returns the active generation counter.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.runtime_state.current.load().generation.0
    }

    /// Returns a read-only status snapshot for admin/observability.
    pub fn status_snapshot(&self) -> TargetTlsStatusSnapshot {
        TargetTlsReloadCoordinator::build_status_snapshot(&self.runtime_state, &self.options)
    }

    /// Returns the underlying runtime state (for `close()` cleanup etc.).
    pub fn runtime_state(&self) -> &Arc<TargetTlsRuntimeState<M>> {
        &self.runtime_state
    }

    /// Unregisters from the coordinator (stops the poll loop).
    pub async fn unregister(&self, coordinator: &TargetTlsReloadCoordinator) {
        let label = &self.runtime_state.inputs.target_label;
        if let Err(err) = coordinator.unregister(label).await {
            warn!(target = %label, error = %err, "Failed to unregister TLS reload adapter");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TargetError;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct FakeTarget {
        label: String,
        build_calls: AtomicUsize,
        should_fail: AtomicBool,
    }

    impl FakeTarget {
        fn new(label: &str) -> Self {
            Self {
                label: label.to_string(),
                build_calls: AtomicUsize::new(0),
                should_fail: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl ReloadableTargetTls for FakeTarget {
        type Material = String;

        fn tls_input_set(&self) -> super::super::state::TargetTlsInputSet {
            super::super::state::TargetTlsInputSet {
                ca_path: String::new(),
                client_cert_path: String::new(),
                client_key_path: String::new(),
                target_label: self.label.clone(),
            }
        }

        async fn build_tls_material(&self) -> Result<Self::Material, TargetError> {
            self.build_calls.fetch_add(1, Ordering::SeqCst);
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(TargetError::Configuration("fail".to_string()));
            }
            Ok("material".to_string())
        }

        async fn apply_tls_material(
            &self,
            _generation: super::super::fingerprint::TargetTlsGeneration,
            _material: Arc<Self::Material>,
            _mode: super::super::config::ReloadApplyMode,
        ) -> Result<(), TargetError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn try_register_returns_adapter_on_success() {
        let coordinator = TargetTlsReloadCoordinator::new();
        let target = Arc::new(FakeTarget::new("test:fake"));
        let options = TlsReloadOptions::default();

        let adapter = TlsReloadAdapter::try_register(target.clone(), options, &coordinator).await;
        assert!(adapter.is_some());
        assert_eq!(target.build_calls.load(Ordering::SeqCst), 1);

        let a = adapter.unwrap();
        assert_eq!(*a.current_material(), "material");
    }

    #[tokio::test]
    async fn try_register_returns_none_on_failure() {
        let coordinator = TargetTlsReloadCoordinator::new();
        let target = Arc::new(FakeTarget::new("test:fail"));
        target.should_fail.store(true, Ordering::SeqCst);
        let options = TlsReloadOptions::default();

        let adapter = TlsReloadAdapter::try_register(target, options, &coordinator).await;
        assert!(adapter.is_none());
    }

    #[tokio::test]
    async fn adapter_is_clone_and_shares_state() {
        let coordinator = TargetTlsReloadCoordinator::new();
        let target = Arc::new(FakeTarget::new("test:clone"));
        let options = TlsReloadOptions::default();

        let a = TlsReloadAdapter::try_register(target, options, &coordinator).await.unwrap();
        let b = a.clone();

        assert_eq!(*a.current_material(), *b.current_material());
        assert_eq!(a.generation(), b.generation());
    }

    #[tokio::test]
    async fn status_snapshot_contains_label() {
        let coordinator = TargetTlsReloadCoordinator::new();
        let target = Arc::new(FakeTarget::new("test:snap"));
        let options = TlsReloadOptions::default();

        let adapter = TlsReloadAdapter::try_register(target, options, &coordinator).await.unwrap();
        let snap = adapter.status_snapshot();
        assert_eq!(snap.target_label, "test:snap");
        assert!(snap.reload_enabled);
    }
}
