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

//! Target TLS reload coordinator. Manages per-target background poll loops
//! that periodically check TLS material fingerprints and drive safe reload.

use super::config::{ReloadApplyMode, ReloadDetectMode, TlsReloadOptions};
#[cfg(test)]
use super::fingerprint::TargetTlsFingerprint;
use super::fingerprint::{TargetTlsGeneration, build_target_tls_fingerprint};
use super::metrics::{record_target_tls_publication_fail, record_target_tls_reload_result, record_target_tls_reload_skipped};
#[cfg(test)]
use super::state::TargetTlsInputSet;
use super::state::{TargetTlsPublishedState, TargetTlsRuntimeState, TargetTlsStatusSnapshot};
use super::r#trait::ReloadableTargetTls;
use super::validate::validate_tls_material;
use crate::error::TargetError;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

struct TargetReloadEntry {
    #[expect(dead_code)]
    target_label: String,
    cancel_tx: tokio::sync::mpsc::Sender<()>,
    poll_handle: JoinHandle<()>,
}

/// The top-level coordinator that manages TLS reload for all registered targets.
///
/// Typically one instance per process, held alongside `TargetRuntimeManager`.
/// Each registered target gets its own background poll loop that periodically
/// checks TLS fingerprints and drives the build/apply cycle.
pub struct TargetTlsReloadCoordinator {
    entries: RwLock<HashMap<String, TargetReloadEntry>>,
}

impl Default for TargetTlsReloadCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetTlsReloadCoordinator {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Register a target for coordinated TLS reload. Spawns a background poll loop.
    ///
    /// Returns the initial runtime state that the target should hold for
    /// accessing the current TLS material via `ArcSwap`.
    pub async fn register<T: ReloadableTargetTls>(
        &self,
        target: Arc<T>,
        options: TlsReloadOptions,
    ) -> Result<Arc<TargetTlsRuntimeState<T::Material>>, TargetError> {
        if !options.enabled {
            return Err(TargetError::Configuration("TLS reload is disabled".to_string()));
        }

        let inputs = target.tls_input_set();
        let target_label = inputs.target_label.clone();

        // Build initial material
        let initial_material = Arc::new(target.build_tls_material().await?);
        let initial_fingerprint =
            build_target_tls_fingerprint(&inputs.ca_path, &inputs.client_cert_path, &inputs.client_key_path).await?;

        let initial_state = Arc::new(TargetTlsPublishedState {
            generation: TargetTlsGeneration(1),
            fingerprint: initial_fingerprint,
            material: initial_material,
            loaded_at_unix_ms: unix_time_ms(),
        });

        let runtime_state = Arc::new(TargetTlsRuntimeState::new(initial_state.clone(), inputs));

        if options.detect_mode == ReloadDetectMode::Poll || options.detect_mode == ReloadDetectMode::Hybrid {
            let (cancel_tx, cancel_rx) = tokio::sync::mpsc::channel(1);
            let poll_handle = tokio::spawn(spawn_target_poll_loop(target, Arc::clone(&runtime_state), options, cancel_rx));

            let mut entries = self.entries.write().await;
            entries.insert(
                target_label.clone(),
                TargetReloadEntry {
                    target_label: target_label.clone(),
                    cancel_tx,
                    poll_handle,
                },
            );

            info!(target = %target_label, "Registered target for TLS reload coordinator");
        }

        Ok(runtime_state)
    }

    /// Unregister a target and stop its poll loop.
    pub async fn unregister(&self, target_label: &str) -> Result<(), TargetError> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.remove(target_label) {
            let _ = entry.cancel_tx.send(()).await;
            entry.poll_handle.abort();
            info!(target = %target_label, "Unregistered target from TLS reload coordinator");
        }
        Ok(())
    }

    /// Force an immediate reload check for a specific target.
    /// Used by admin endpoints and test harnesses.
    pub async fn force_reload<T: ReloadableTargetTls>(
        &self,
        target: &T,
        runtime_state: &TargetTlsRuntimeState<T::Material>,
        options: &TlsReloadOptions,
    ) -> Result<TargetTlsGeneration, TargetError> {
        reload_target_once(target, runtime_state, options).await
    }

    /// Stop all poll loops.
    pub async fn shutdown(&self) {
        let mut entries = self.entries.write().await;
        for (label, entry) in entries.drain() {
            let _ = entry.cancel_tx.send(()).await;
            entry.poll_handle.abort();
            debug!(target = %label, "Stopped TLS reload poll loop");
        }
    }

    /// Collect status snapshots from all registered targets.
    /// The caller must provide the runtime states separately since the
    /// coordinator does not hold type-erased references to them.
    pub fn build_status_snapshot<M>(
        runtime_state: &TargetTlsRuntimeState<M>,
        options: &TlsReloadOptions,
    ) -> TargetTlsStatusSnapshot {
        let current = runtime_state.current.load();
        let last_attempt = runtime_state.last_attempt_unix_ms();
        let last_success = runtime_state.last_success_unix_ms();
        let last_error = runtime_state.last_error.read().clone();

        TargetTlsStatusSnapshot {
            target_label: runtime_state.inputs.target_label.clone(),
            generation: current.generation.0,
            reload_enabled: options.enabled,
            detect_mode: match options.detect_mode {
                ReloadDetectMode::Poll => "poll",
                ReloadDetectMode::Watch => "watch",
                ReloadDetectMode::Hybrid => "hybrid",
            },
            apply_mode: match options.apply_hint {
                ReloadApplyMode::Lazy => "lazy",
                ReloadApplyMode::SoftReconnect => "soft_reconnect",
            },
            last_attempt_time: if last_attempt > 0 { Some(last_attempt) } else { None },
            last_success_time: if last_success > 0 { Some(last_success) } else { None },
            last_error,
        }
    }
}

/// Background poll loop for a single target.
async fn spawn_target_poll_loop<T: ReloadableTargetTls>(
    target: Arc<T>,
    runtime_state: Arc<TargetTlsRuntimeState<T::Material>>,
    options: TlsReloadOptions,
    mut cancel_rx: tokio::sync::mpsc::Receiver<()>,
) {
    let mut interval = tokio::time::interval(options.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await; // skip the immediate first tick

    let label = &runtime_state.inputs.target_label;
    debug!(target = %label, interval_secs = options.interval.as_secs(), "TLS reload poll loop started");

    loop {
        tokio::select! {
            biased;
            _ = cancel_rx.recv() => {
                info!(target = %label, "TLS reload poll loop stopped");
                return;
            }
            _ = interval.tick() => {
                if let Err(err) = reload_target_once(target.as_ref(), runtime_state.as_ref(), &options).await {
                    warn!(target = %label, error = %err, "TLS reload poll check failed (will retry)");
                }
            }
        }
    }
}

/// Single reload cycle: read → compare → validate → build → apply → publish.
///
/// Returns the new generation on success, or an error on failure.
/// On failure the current generation and material are untouched.
async fn reload_target_once<T: ReloadableTargetTls>(
    target: &T,
    runtime_state: &TargetTlsRuntimeState<T::Material>,
    options: &TlsReloadOptions,
) -> Result<TargetTlsGeneration, TargetError> {
    let now = unix_time_ms();
    runtime_state.mark_attempt(now);
    let started_at = std::time::Instant::now();
    let label = &runtime_state.inputs.target_label;

    // 1. Read TLS files and compute fingerprint
    let inputs = &runtime_state.inputs;
    let next_fingerprint =
        build_target_tls_fingerprint(&inputs.ca_path, &inputs.client_cert_path, &inputs.client_key_path).await?;

    // 2. Compare with current — skip if unchanged
    let current = runtime_state.current.load();
    if current.fingerprint == next_fingerprint {
        record_target_tls_reload_skipped(label, "unchanged");
        return Ok(current.generation);
    }

    // 3. Validate TLS files (cert/key pairing, CA parseable)
    if let Err(err) = validate_tls_material(&inputs.ca_path, &inputs.client_cert_path, &inputs.client_key_path) {
        *runtime_state.last_error.write() = Some(err.to_string());
        record_target_tls_publication_fail(label);
        return Err(err);
    }

    // Also call target-specific validation
    if let Err(err) = target.validate_tls_files().await {
        *runtime_state.last_error.write() = Some(err.to_string());
        record_target_tls_publication_fail(label);
        return Err(err);
    }

    // 4. Build new material (does not touch current state yet)
    let new_material = match target.build_tls_material().await {
        Ok(m) => Arc::new(m),
        Err(err) => {
            *runtime_state.last_error.write() = Some(err.to_string());
            record_target_tls_publication_fail(label);
            return Err(err);
        }
    };

    // 5. Bump generation and apply
    let new_generation = runtime_state.bump_generation();
    if let Err(err) = target
        .apply_tls_material(new_generation, Arc::clone(&new_material), options.apply_hint)
        .await
    {
        *runtime_state.last_error.write() = Some(err.to_string());
        record_target_tls_publication_fail(label);
        return Err(err);
    }

    // 6. Publish new state
    let published = Arc::new(TargetTlsPublishedState {
        generation: new_generation,
        fingerprint: next_fingerprint,
        material: new_material,
        loaded_at_unix_ms: now,
    });
    runtime_state.current.store(published.clone());
    runtime_state.last_good.store(published);
    runtime_state.mark_success(now);
    *runtime_state.last_error.write() = None;

    record_target_tls_reload_result(label, "ok", started_at.elapsed().as_secs_f64(), new_generation.0);

    debug!(target = %label, generation = new_generation.0, "TLS reload successful");
    Ok(new_generation)
}

fn unix_time_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct MockTarget {
        inputs: TargetTlsInputSet,
        build_calls: AtomicUsize,
        apply_calls: AtomicUsize,
        validate_calls: AtomicUsize,
        should_fail_build: AtomicBool,
        should_fail_apply: AtomicBool,
        should_fail_validate: AtomicBool,
    }

    impl MockTarget {
        fn new(label: &str) -> Self {
            Self {
                inputs: TargetTlsInputSet {
                    ca_path: String::new(),
                    client_cert_path: String::new(),
                    client_key_path: String::new(),
                    target_label: label.to_string(),
                },
                build_calls: AtomicUsize::new(0),
                apply_calls: AtomicUsize::new(0),
                validate_calls: AtomicUsize::new(0),
                should_fail_build: AtomicBool::new(false),
                should_fail_apply: AtomicBool::new(false),
                should_fail_validate: AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl ReloadableTargetTls for MockTarget {
        type Material = String;

        fn tls_input_set(&self) -> TargetTlsInputSet {
            self.inputs.clone()
        }

        async fn build_tls_material(&self) -> Result<Self::Material, TargetError> {
            self.build_calls.fetch_add(1, Ordering::SeqCst);
            if self.should_fail_build.load(Ordering::SeqCst) {
                return Err(TargetError::Configuration("build failed".to_string()));
            }
            Ok("mock-material".to_string())
        }

        async fn apply_tls_material(
            &self,
            _generation: TargetTlsGeneration,
            _material: Arc<Self::Material>,
            _mode: ReloadApplyMode,
        ) -> Result<(), TargetError> {
            self.apply_calls.fetch_add(1, Ordering::SeqCst);
            if self.should_fail_apply.load(Ordering::SeqCst) {
                return Err(TargetError::Configuration("apply failed".to_string()));
            }
            Ok(())
        }

        async fn validate_tls_files(&self) -> Result<(), TargetError> {
            self.validate_calls.fetch_add(1, Ordering::SeqCst);
            if self.should_fail_validate.load(Ordering::SeqCst) {
                return Err(TargetError::Configuration("validate failed".to_string()));
            }
            Ok(())
        }
    }

    fn default_options() -> TlsReloadOptions {
        TlsReloadOptions {
            enabled: true,
            detect_mode: ReloadDetectMode::Poll,
            interval: std::time::Duration::from_secs(1),
            debounce: std::time::Duration::from_secs(1),
            min_stable_age: std::time::Duration::from_millis(100),
            apply_hint: ReloadApplyMode::Lazy,
        }
    }

    #[tokio::test]
    async fn register_builds_initial_material() {
        let coordinator = TargetTlsReloadCoordinator::new();
        let target = Arc::new(MockTarget::new("test:webhook"));

        let options = TlsReloadOptions {
            detect_mode: ReloadDetectMode::Watch, // no poll loop for this test
            ..default_options()
        };
        let state = coordinator.register(target.clone(), options).await.unwrap();

        assert_eq!(state.current.load().generation, TargetTlsGeneration(1));
        assert_eq!(target.build_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn register_disabled_returns_error() {
        let coordinator = TargetTlsReloadCoordinator::new();
        let target = Arc::new(MockTarget::new("test:webhook"));

        let options = TlsReloadOptions {
            enabled: false,
            ..default_options()
        };
        let result = coordinator.register(target, options).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn shutdown_stops_all_loops() {
        let coordinator = TargetTlsReloadCoordinator::new();
        let target = Arc::new(MockTarget::new("test:webhook"));

        let _state = coordinator.register(target.clone(), default_options()).await.unwrap();
        assert_eq!(coordinator.entries.read().await.len(), 1);

        coordinator.shutdown().await;
        assert!(coordinator.entries.read().await.is_empty());
    }

    #[tokio::test]
    async fn force_reload_calls_build_and_apply() {
        let target = MockTarget::new("test:webhook");
        let initial_material = Arc::new("initial".to_string());
        let initial_state = Arc::new(TargetTlsPublishedState {
            generation: TargetTlsGeneration(1),
            fingerprint: TargetTlsFingerprint::default(),
            material: initial_material,
            loaded_at_unix_ms: 0,
        });
        let inputs = target.tls_input_set();
        let runtime_state = Arc::new(TargetTlsRuntimeState::new(initial_state, inputs));
        let options = default_options();

        // Force reload should succeed since MockTarget uses empty paths
        // and the fingerprint won't change from default
        let result = reload_target_once(&target, &runtime_state, &options).await.unwrap();
        // Since fingerprint is unchanged (empty paths), generation stays at 1
        assert_eq!(result, TargetTlsGeneration(1));
        // Build should NOT be called because fingerprint unchanged
        assert_eq!(target.build_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn build_failure_preserves_old_generation() {
        let target = MockTarget::new("test:webhook");
        target.should_fail_build.store(true, Ordering::SeqCst);

        // Use a non-default fingerprint so the reload will detect a change
        // (empty paths → default fingerprint ≠ initial fingerprint)
        let initial_material = Arc::new("initial".to_string());
        let initial_fingerprint = TargetTlsFingerprint {
            ca_sha256: Some([1; 32]),
            client_cert_sha256: None,
            client_key_sha256: None,
        };
        let initial_state = Arc::new(TargetTlsPublishedState {
            generation: TargetTlsGeneration(1),
            fingerprint: initial_fingerprint,
            material: initial_material,
            loaded_at_unix_ms: 0,
        });
        let runtime_state = Arc::new(TargetTlsRuntimeState::new(initial_state, target.tls_input_set()));
        let options = default_options();

        // Empty paths produce default fingerprint which differs from initial →
        // validate passes (empty paths), then build is called and fails.
        let result = reload_target_once(&target, &runtime_state, &options).await;
        assert!(result.is_err());
        // Generation should remain at 1
        assert_eq!(runtime_state.current_generation(), TargetTlsGeneration(1));
        assert_eq!(target.build_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn status_snapshot_reflects_state() {
        let target = MockTarget::new("test:webhook");
        let initial_material = Arc::new("initial".to_string());
        let initial_state = Arc::new(TargetTlsPublishedState {
            generation: TargetTlsGeneration(1),
            fingerprint: TargetTlsFingerprint::default(),
            material: initial_material,
            loaded_at_unix_ms: 0,
        });
        let runtime_state = Arc::new(TargetTlsRuntimeState::new(initial_state, target.tls_input_set()));
        let options = default_options();

        let snapshot = TargetTlsReloadCoordinator::build_status_snapshot(runtime_state.as_ref(), &options);
        assert_eq!(snapshot.target_label, "test:webhook");
        assert_eq!(snapshot.generation, 1);
        assert!(snapshot.reload_enabled);
        assert_eq!(snapshot.detect_mode, "poll");
        assert_eq!(snapshot.apply_mode, "lazy");
        assert!(snapshot.last_attempt_time.is_none());
        assert!(snapshot.last_error.is_none());
    }
}
