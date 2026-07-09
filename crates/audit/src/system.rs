//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::{
    AuditEntry, AuditError, AuditRegistry, AuditResult, observability,
    pipeline::{AuditPipeline, AuditRuntimeFacade, AuditRuntimeView},
};
use rustfs_config::server_config::Config;
use rustfs_targets::{ReplayWorkerManager, Target};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_AUDIT: &str = "audit";
const LOG_SUBSYSTEM_SYSTEM: &str = "system";
const EVENT_AUDIT_SYSTEM_STATE: &str = "audit_system_state";
const EVENT_AUDIT_CONFIG_RELOADED: &str = "audit_config_reloaded";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditTargetMetricSnapshot {
    pub failed_messages: u64,
    pub queue_length: u64,
    pub target_id: String,
    pub total_messages: u64,
}

/// State of the audit system
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditSystemState {
    Stopped,
    Starting,
    Running,
    Paused,
    Stopping,
}

/// Main audit system that manages target lifecycle and audit log dispatch
#[derive(Clone)]
pub struct AuditSystem {
    registry: Arc<Mutex<AuditRegistry>>,
    state: Arc<RwLock<AuditSystemState>>,
    config: Arc<RwLock<Option<Config>>>,
    /// Cancellation senders for active audit stream tasks (target_id -> cancel tx)
    stream_cancellers: Arc<RwLock<ReplayWorkerManager>>,
}

impl Default for AuditSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditSystem {
    fn pipeline(&self) -> AuditPipeline {
        AuditPipeline::new(self.registry.clone())
    }

    fn runtime_view(&self) -> AuditRuntimeView {
        AuditRuntimeView::new(self.registry.clone())
    }

    fn runtime_facade(&self) -> AuditRuntimeFacade {
        AuditRuntimeFacade::new(self.registry.clone(), self.stream_cancellers.clone())
    }

    /// Creates a new audit system
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(AuditRegistry::new())),
            state: Arc::new(RwLock::new(AuditSystemState::Stopped)),
            config: Arc::new(RwLock::new(None)),
            stream_cancellers: Arc::new(RwLock::new(ReplayWorkerManager::new())),
        }
    }

    async fn create_targets_from_config(&self, config: &Config) -> AuditResult<Vec<Box<dyn Target<AuditEntry> + Send + Sync>>> {
        let registry = self.registry.lock().await;
        registry.create_audit_targets_from_config(config).await
    }

    /// Stops any active replay workers and closes the currently installed
    /// targets without touching the system state. Lock order is `registry`
    /// then `stream_cancellers` to stay consistent with every other path that
    /// holds both locks (see `runtime_status_snapshot`, backlog#961).
    async fn shutdown_runtime_targets(&self) -> AuditResult<()> {
        let mut registry = self.registry.lock().await;
        let mut replay_workers = self.stream_cancellers.write().await;
        self.runtime_facade()
            .shutdown_runtime(&mut registry, &mut replay_workers)
            .await
    }

    async fn clear_runtime_targets(&self) -> AuditResult<()> {
        self.shutdown_runtime_targets().await?;

        let mut state = self.state.write().await;
        *state = AuditSystemState::Stopped;
        Ok(())
    }

    async fn commit_runtime_targets(
        &self,
        targets: Vec<Box<dyn Target<AuditEntry> + Send + Sync>>,
        final_state: AuditSystemState,
    ) -> AuditResult<()> {
        if targets.is_empty() {
            debug_audit_state("stopped", Some("no_enabled_targets"), None, 0);
            self.clear_runtime_targets().await?;
            return Ok(());
        }

        info!(
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_AUDIT,
            subsystem = LOG_SUBSYSTEM_SYSTEM,
            state = "targets_created",
            target_count = targets.len(),
            "audit system state"
        );

        // Stop-before-start (backlog#970): tear down the existing replay workers
        // and close the currently installed targets *before* activating the new
        // set. Activation spawns fresh replay workers for store-backed targets,
        // so if the old workers were still running they would drain the same
        // persistent queue concurrently with the new ones and re-deliver
        // entries. Shutting the old runtime down first keeps at most one active
        // worker per store across a reload. `replace_targets` performs a second
        // (now no-op) shutdown before installing, which is idempotent.
        self.shutdown_runtime_targets().await?;

        let activation = self.runtime_facade().activate_targets_with_replay(targets).await;
        self.runtime_facade().replace_targets(activation).await?;

        let mut state = self.state.write().await;
        *state = final_state;
        Ok(())
    }

    /// Starts the audit system with the given configuration
    ///
    /// # Arguments
    /// * `config` - The configuration to use for starting the audit system
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn start(&self, config: Config) -> AuditResult<()> {
        // Claim the `Starting` transition atomically while holding the write
        // lock (backlog#978): the previous code released the lock after the
        // check and re-acquired it later to set `Starting`, so two concurrent
        // `start()` calls (or `start()` racing `reload`) could both pass the
        // check and double-activate. Transitioning to `Starting` before
        // dropping the guard makes a concurrent caller observe `Starting` and
        // return early instead.
        {
            let mut state = self.state.write().await;

            match *state {
                AuditSystemState::Running => {
                    return Err(AuditError::AlreadyInitialized);
                }
                AuditSystemState::Starting => {
                    warn_audit_state("starting", Some("already_starting"));
                    return Ok(());
                }
                _ => {}
            }

            *state = AuditSystemState::Starting;
        }

        info!(
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_AUDIT,
            subsystem = LOG_SUBSYSTEM_SYSTEM,
            state = "starting",
            "audit system state"
        );

        // Record system start
        observability::record_system_start();

        // Store configuration
        {
            let mut config_guard = self.config.write().await;
            *config_guard = Some(config.clone());
        }

        match self.create_targets_from_config(&config).await {
            Ok(targets) => {
                // State is already `Starting` (claimed atomically above).
                self.commit_runtime_targets(targets, AuditSystemState::Running).await?;
                info_audit_state("running", None, None);
                Ok(())
            }
            Err(e) => {
                error!(
                    event = EVENT_AUDIT_SYSTEM_STATE,
                    component = LOG_COMPONENT_AUDIT,
                    subsystem = LOG_SUBSYSTEM_SYSTEM,
                    state = "stopped",
                    reason = "target_creation_failed",
                    error = %e,
                    "Failed to create audit targets"
                );
                let mut state = self.state.write().await;
                *state = AuditSystemState::Stopped;
                Err(e)
            }
        }
    }

    /// Pauses the audit system
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn pause(&self) -> AuditResult<()> {
        let mut state = self.state.write().await;

        match *state {
            AuditSystemState::Running => {
                *state = AuditSystemState::Paused;
                info_audit_state("paused", None, None);
                Ok(())
            }
            AuditSystemState::Paused => {
                warn_audit_state("paused", Some("already_paused"));
                Ok(())
            }
            _ => Err(AuditError::Configuration("Cannot pause audit system in current state".to_string(), None)),
        }
    }

    /// Resumes the audit system
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn resume(&self) -> AuditResult<()> {
        let mut state = self.state.write().await;

        match *state {
            AuditSystemState::Paused => {
                *state = AuditSystemState::Running;
                info_audit_state("running", Some("resumed"), None);
                Ok(())
            }
            AuditSystemState::Running => {
                warn_audit_state("running", Some("already_running"));
                Ok(())
            }
            _ => Err(AuditError::Configuration("Cannot resume audit system in current state".to_string(), None)),
        }
    }

    /// Stops the audit system and closes all targets
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn close(&self) -> AuditResult<()> {
        let mut state = self.state.write().await;

        match *state {
            AuditSystemState::Stopped => {
                warn_audit_state("stopped", Some("already_stopped"));
                return Ok(());
            }
            AuditSystemState::Stopping => {
                warn_audit_state("stopping", Some("already_stopping"));
                return Ok(());
            }
            _ => {}
        }

        *state = AuditSystemState::Stopping;
        drop(state);

        info!(
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_AUDIT,
            subsystem = LOG_SUBSYSTEM_SYSTEM,
            state = "stopping",
            "audit system state"
        );

        // Stop all stream tasks first
        if let Err(e) = self.clear_runtime_targets().await {
            error!(
                event = EVENT_AUDIT_SYSTEM_STATE,
                component = LOG_COMPONENT_AUDIT,
                subsystem = LOG_SUBSYSTEM_SYSTEM,
                state = "stopping",
                reason = "target_shutdown_failed",
                error = %e,
                "Failed to close some audit targets"
            );
        }

        // Clear configuration
        let mut config_guard = self.config.write().await;
        *config_guard = None;

        info_audit_state("stopped", None, None);
        Ok(())
    }

    /// Gets the current state of the audit system
    pub async fn get_state(&self) -> AuditSystemState {
        self.state.read().await.clone()
    }

    /// Checks if the audit system is running
    ///
    /// # Returns
    /// * `bool` - True if running, false otherwise
    pub async fn is_running(&self) -> bool {
        matches!(*self.state.read().await, AuditSystemState::Running)
    }

    /// Dispatches an audit log entry to all active targets
    ///
    /// # Arguments
    /// * `entry` - The audit log entry to dispatch
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn dispatch(&self, entry: Arc<AuditEntry>) -> AuditResult<()> {
        let state = self.state.read().await;

        match *state {
            AuditSystemState::Running => {}
            AuditSystemState::Paused => {
                // Do not silently return Ok while paused (backlog#978): the
                // entry is neither delivered nor persisted, so reporting success
                // would corrupt the audit trail. Surface an explicit `Paused`
                // error and let the caller apply its policy (the global helper
                // treats this as a deliberate skip; direct API callers can
                // decide otherwise).
                return Err(AuditError::Paused);
            }
            _ => {
                return Err(AuditError::NotInitialized("Audit system is not running".to_string()));
            }
        }
        drop(state);
        self.pipeline().dispatch(entry).await
    }

    /// Dispatches a batch of audit log entries to all active targets
    ///
    /// # Arguments
    /// * `entries` - A vector of audit log entries to dispatch
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn dispatch_batch(&self, entries: Vec<Arc<AuditEntry>>) -> AuditResult<()> {
        let state = self.state.read().await;
        if *state != AuditSystemState::Running {
            return Err(AuditError::NotInitialized("Audit system is not running".to_string()));
        }
        drop(state);
        self.pipeline().dispatch_batch(entries).await
    }

    /// Enables a specific target
    ///
    /// # Arguments
    /// * `target_id` - The ID of the target to enable, TargetID to string
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn enable_target(&self, target_id: &str) -> AuditResult<()> {
        self.runtime_view().enable_target(target_id).await
    }

    /// Disables a specific target
    ///
    /// # Arguments
    /// * `target_id` - The ID of the target to disable, TargetID to string
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn disable_target(&self, target_id: &str) -> AuditResult<()> {
        self.runtime_view().disable_target(target_id).await
    }

    /// Removes a target from the system
    ///
    /// # Arguments
    /// * `target_id` - The ID of the target to remove, TargetID to string
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn remove_target(&self, target_id: &str) -> AuditResult<()> {
        self.runtime_view().remove_target(target_id).await
    }

    /// Updates or inserts a target
    ///
    /// # Arguments
    /// * `target_id` - The ID of the target to upsert, TargetID to string
    /// * `target` - The target instance to insert or update
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn upsert_target(&self, target_id: String, target: Box<dyn Target<AuditEntry> + Send + Sync>) -> AuditResult<()> {
        self.runtime_view().upsert_target(target_id, target).await
    }

    /// Lists all targets
    ///
    /// # Returns
    /// * `Vec<String>` - List of target IDs
    pub async fn list_targets(&self) -> Vec<String> {
        self.runtime_view().list_targets().await
    }

    /// Returns cloned target values for read-only runtime inspection.
    pub async fn get_target_values(&self) -> Vec<rustfs_targets::SharedTarget<AuditEntry>> {
        self.runtime_view().get_target_values().await
    }

    /// Returns per-target delivery metrics for Prometheus collection.
    pub async fn snapshot_target_metrics(&self) -> Vec<AuditTargetMetricSnapshot> {
        self.pipeline().snapshot_target_metrics().await
    }

    pub async fn snapshot_target_health(&self) -> Vec<rustfs_targets::RuntimeTargetHealthSnapshot> {
        self.pipeline().snapshot_target_health().await
    }

    pub async fn runtime_status_snapshot(&self) -> rustfs_targets::RuntimeStatusSnapshot {
        // Lock order must match every other path that holds both locks
        // (`clear_runtime_targets`, `AuditRuntimeFacade::replace_targets`):
        // acquire `registry` first, then `stream_cancellers`. Reversing the
        // order here would create an ABBA deadlock with those paths.
        let registry = self.registry.lock().await;
        let replay_workers = self.stream_cancellers.read().await;
        registry.runtime_manager().status_snapshot(&replay_workers)
    }

    /// Gets information about a specific target
    ///
    /// # Arguments
    /// * `target_id` - The ID of the target to retrieve, TargetID to string
    ///
    /// # Returns
    /// * `Option<String>` - Target ID if found
    pub async fn get_target(&self, target_id: &str) -> Option<String> {
        self.runtime_view().get_target(target_id).await
    }

    /// Reloads configuration and updates targets
    ///
    /// # Arguments
    /// * `new_config` - The new configuration to load
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn reload_config(&self, new_config: Config) -> AuditResult<()> {
        info!(
            event = EVENT_AUDIT_CONFIG_RELOADED,
            component = LOG_COMPONENT_AUDIT,
            subsystem = LOG_SUBSYSTEM_SYSTEM,
            state = "reloading",
            "audit config reload"
        );

        observability::record_config_reload();

        // Store new configuration
        {
            let mut config_guard = self.config.write().await;
            *config_guard = Some(new_config.clone());
        }

        let final_state = match self.get_state().await {
            AuditSystemState::Paused => AuditSystemState::Paused,
            _ => AuditSystemState::Running,
        };

        match self.create_targets_from_config(&new_config).await {
            Ok(targets) => {
                self.commit_runtime_targets(targets, final_state).await?;
                info!(
                    event = EVENT_AUDIT_CONFIG_RELOADED,
                    component = LOG_COMPONENT_AUDIT,
                    subsystem = LOG_SUBSYSTEM_SYSTEM,
                    state = "reloaded",
                    "audit config reload"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    event = EVENT_AUDIT_CONFIG_RELOADED,
                    component = LOG_COMPONENT_AUDIT,
                    subsystem = LOG_SUBSYSTEM_SYSTEM,
                    state = "reload_failed",
                    error = %e,
                    "Failed to reload audit configuration"
                );
                Err(e)
            }
        }
    }

    /// Gets current audit system metrics
    ///
    /// # Returns
    /// * `AuditMetricsReport` - Current metrics report
    pub async fn get_metrics(&self) -> observability::AuditMetricsReport {
        observability::get_metrics_report().await
    }

    /// Validates system performance against requirements
    ///
    /// # Returns
    /// * `PerformanceValidation` - Performance validation results
    pub async fn validate_performance(&self) -> observability::PerformanceValidation {
        observability::validate_performance().await
    }

    /// Resets all metrics to initial state
    pub async fn reset_metrics(&self) {
        observability::reset_metrics().await;
    }
}

fn info_audit_state(state: &str, reason: Option<&str>, target_count: Option<usize>) {
    info!(
        event = EVENT_AUDIT_SYSTEM_STATE,
        component = LOG_COMPONENT_AUDIT,
        subsystem = LOG_SUBSYSTEM_SYSTEM,
        state,
        reason = reason.unwrap_or_default(),
        target_count = target_count.unwrap_or_default(),
        "audit system state"
    );
}

fn debug_audit_state(state: &str, reason: Option<&str>, error: Option<&str>, target_count: usize) {
    debug!(
        event = EVENT_AUDIT_SYSTEM_STATE,
        component = LOG_COMPONENT_AUDIT,
        subsystem = LOG_SUBSYSTEM_SYSTEM,
        state,
        reason = reason.unwrap_or_default(),
        error = error.unwrap_or_default(),
        target_count,
        "audit system state"
    );
}

fn warn_audit_state(state: &str, reason: Option<&str>) {
    warn!(
        event = EVENT_AUDIT_SYSTEM_STATE,
        component = LOG_COMPONENT_AUDIT,
        subsystem = LOG_SUBSYSTEM_SYSTEM,
        state,
        reason = reason.unwrap_or_default(),
        "audit system state"
    );
}

#[cfg(test)]
mod tests {
    use super::{AuditSystem, AuditSystemState};
    use crate::{AuditEntry, AuditError};
    use async_trait::async_trait;
    use rustfs_targets::ReplayWorkerManager;
    use rustfs_targets::arn::TargetID;
    use rustfs_targets::store::{Key, Store};
    use rustfs_targets::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use rustfs_targets::{StoreError, Target, TargetError};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct TestTarget {
        close_calls: Arc<AtomicUsize>,
        id: TargetID,
    }

    impl TestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                close_calls: Arc::new(AtomicUsize::new(0)),
                id: TargetID::new(id.to_string(), name.to_string()),
            }
        }
    }

    #[async_trait]
    impl<E> Target<E> for TestTarget
    where
        E: rustfs_targets::PluginEvent,
    {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            None
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            Box::new(self.clone())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn reload_with_empty_config_stops_existing_runtime() {
        let system = AuditSystem::new();
        let target = TestTarget::new("primary", "webhook");
        let close_calls = Arc::clone(&target.close_calls);

        {
            let mut registry = system.registry.lock().await;
            registry.add_target("primary:webhook".to_string(), Box::new(target));
        }
        {
            let mut state = system.state.write().await;
            *state = AuditSystemState::Running;
        }
        {
            let mut replay_workers = system.stream_cancellers.write().await;
            let (cancel_tx, _cancel_rx) = mpsc::channel(1);
            replay_workers.insert("primary:webhook".to_string(), cancel_tx);
            assert_eq!(replay_workers.len(), 1);
        }

        system
            .reload_config(rustfs_config::server_config::Config(HashMap::new()))
            .await
            .expect("reload with empty config should succeed");

        assert_eq!(system.get_state().await, AuditSystemState::Stopped);
        assert!(system.list_targets().await.is_empty());
        assert_eq!(system.runtime_status_snapshot().await, ReplayWorkerManager::new().snapshot(0));
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*system.config.read().await, Some(rustfs_config::server_config::Config(HashMap::new())));
    }

    /// Regression guard for backlog#961: `runtime_status_snapshot` and
    /// `clear_runtime_targets` both hold `registry` and `stream_cancellers`.
    /// They previously acquired the two locks in opposite orders (ABBA),
    /// which could deadlock the whole audit control plane under concurrency.
    /// Hammer both paths from multiple worker threads and assert the workload
    /// completes within a timeout instead of hanging.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_status_and_clear_do_not_deadlock() {
        use std::time::Duration;

        const ITERATIONS: usize = 2_000;
        const TASKS_PER_PATH: usize = 4;

        let system = AuditSystem::new();

        // Seed a target + replay worker so both critical sections touch real state.
        {
            let mut registry = system.registry.lock().await;
            registry.add_target("primary:webhook".to_string(), Box::new(TestTarget::new("primary", "webhook")));
        }
        {
            let mut replay_workers = system.stream_cancellers.write().await;
            let (cancel_tx, _cancel_rx) = mpsc::channel(1);
            replay_workers.insert("primary:webhook".to_string(), cancel_tx);
        }

        let mut handles = Vec::new();

        for _ in 0..TASKS_PER_PATH {
            let status_system = system.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ITERATIONS {
                    // registry -> stream_cancellers (read)
                    let _ = status_system.runtime_status_snapshot().await;
                }
            }));

            let clear_system = system.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..ITERATIONS {
                    // registry -> stream_cancellers (write)
                    clear_system
                        .clear_runtime_targets()
                        .await
                        .expect("clear_runtime_targets should succeed");
                }
            }));
        }

        let workload = async {
            for handle in handles {
                handle.await.expect("worker task panicked");
            }
        };

        tokio::time::timeout(Duration::from_secs(30), workload)
            .await
            .expect("audit lock paths deadlocked (backlog#961 regression)");
    }

    /// backlog#978: a paused system must not report success while silently
    /// dropping the entry. `dispatch` should surface an explicit `Paused` error.
    #[tokio::test]
    async fn dispatch_while_paused_returns_error_not_ok() {
        let system = AuditSystem::new();
        {
            let mut state = system.state.write().await;
            *state = AuditSystemState::Paused;
        }

        let result = system.dispatch(Arc::new(AuditEntry::default())).await;
        assert!(
            matches!(result, Err(AuditError::Paused)),
            "paused dispatch must return Err(Paused), got {result:?}"
        );
    }

    /// backlog#978: `start()` now claims the `Starting` transition atomically
    /// under the state lock, so racing `start()` calls cannot both pass the
    /// check and double-activate. Hammer concurrent starts and assert the
    /// workload completes (no deadlock/panic) and converges to a consistent
    /// final state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_start_does_not_hang_or_double_activate() {
        use std::time::Duration;

        let system = AuditSystem::new();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = system.clone();
            handles.push(tokio::spawn(async move {
                // Empty config activates no targets, so a completed start settles
                // the system back to `Stopped`.
                let _ = s.start(rustfs_config::server_config::Config(HashMap::new())).await;
            }));
        }

        let workload = async {
            for handle in handles {
                handle.await.expect("start task panicked");
            }
        };
        tokio::time::timeout(Duration::from_secs(30), workload)
            .await
            .expect("concurrent start deadlocked (backlog#978 regression)");

        assert_eq!(system.get_state().await, AuditSystemState::Stopped);
    }

    /// backlog#970: a reload/commit must tear down the previous replay workers
    /// and close the old targets before activating the replacement set, so the
    /// old and new workers never drain the same store concurrently. Seed an old
    /// target plus a replay worker, commit a new target, and assert the old one
    /// was closed and its worker stopped while the new one is installed.
    #[tokio::test]
    async fn commit_closes_old_targets_before_installing_new() {
        let system = AuditSystem::new();

        let old = TestTarget::new("old", "webhook");
        let old_close = Arc::clone(&old.close_calls);
        {
            let mut registry = system.registry.lock().await;
            registry.add_target("old:webhook".to_string(), Box::new(old));
        }
        {
            let mut replay_workers = system.stream_cancellers.write().await;
            let (cancel_tx, _cancel_rx) = mpsc::channel(1);
            replay_workers.insert("old:webhook".to_string(), cancel_tx);
        }
        {
            let mut state = system.state.write().await;
            *state = AuditSystemState::Running;
        }

        let new = TestTarget::new("new", "webhook");
        let new_close = Arc::clone(&new.close_calls);
        system
            .commit_runtime_targets(vec![Box::new(new)], AuditSystemState::Running)
            .await
            .expect("commit should succeed");

        // Old target closed exactly once during the pre-install shutdown.
        assert_eq!(old_close.load(Ordering::SeqCst), 1);
        // New target installed and left open.
        assert_eq!(new_close.load(Ordering::SeqCst), 0);
        assert_eq!(system.list_targets().await, vec!["new:webhook".to_string()]);
        // Old replay worker stopped; the store-less new target adds none.
        assert_eq!(system.runtime_status_snapshot().await.replay_worker_count, 0);
        assert_eq!(system.get_state().await, AuditSystemState::Running);
    }
}
