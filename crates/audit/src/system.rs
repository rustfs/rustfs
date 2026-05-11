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
use rustfs_ecstore::config::Config;
use rustfs_targets::{ReplayWorkerManager, Target};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

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

    async fn clear_runtime_targets(&self) -> AuditResult<()> {
        self.runtime_facade().stop_replay_workers().await;
        let mut registry = self.registry.lock().await;
        registry.close_all().await?;
        drop(registry);

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
            info!("No enabled audit targets found, keeping audit system stopped");
            self.clear_runtime_targets().await?;
            return Ok(());
        }

        info!(target_count = targets.len(), "Created audit targets successfully");

        let activation = self
            .runtime_facade()
            .activate_targets_with_replay(self.state.clone(), targets)
            .await;
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
        let state = self.state.write().await;

        match *state {
            AuditSystemState::Running => {
                return Err(AuditError::AlreadyInitialized);
            }
            AuditSystemState::Starting => {
                warn!("Audit system is already starting");
                return Ok(());
            }
            _ => {}
        }

        drop(state);

        info!("Starting audit system");

        // Record system start
        observability::record_system_start();

        // Store configuration
        {
            let mut config_guard = self.config.write().await;
            *config_guard = Some(config.clone());
        }

        match self.create_targets_from_config(&config).await {
            Ok(targets) => {
                {
                    let mut state = self.state.write().await;
                    *state = AuditSystemState::Starting;
                }

                self.commit_runtime_targets(targets, AuditSystemState::Running).await?;
                info!("Audit system started successfully");
                Ok(())
            }
            Err(e) => {
                error!(error = %e, "Failed to create audit targets");
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
                info!("Audit system paused");
                Ok(())
            }
            AuditSystemState::Paused => {
                warn!("Audit system is already paused");
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
                info!("Audit system resumed");
                Ok(())
            }
            AuditSystemState::Running => {
                warn!("Audit system is already running");
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
                warn!("Audit system is already stopped");
                return Ok(());
            }
            AuditSystemState::Stopping => {
                warn!("Audit system is already stopping");
                return Ok(());
            }
            _ => {}
        }

        *state = AuditSystemState::Stopping;
        drop(state);

        info!("Stopping audit system");

        // Stop all stream tasks first
        if let Err(e) = self.clear_runtime_targets().await {
            error!(error = %e, "Failed to close some audit targets");
        }

        // Clear configuration
        let mut config_guard = self.config.write().await;
        *config_guard = None;

        info!("Audit system stopped");
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
                return Ok(());
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
        let replay_workers = self.stream_cancellers.read().await;
        let registry = self.registry.lock().await;
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
        info!("Reloading audit system configuration");

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
                info!("Audit configuration reloaded successfully");
                Ok(())
            }
            Err(e) => {
                error!(error = %e, "Failed to reload audit configuration");
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

#[cfg(test)]
mod tests {
    use super::{AuditSystem, AuditSystemState};
    use async_trait::async_trait;
    use rustfs_targets::ReplayWorkerManager;
    use rustfs_targets::arn::TargetID;
    use rustfs_targets::store::{Key, Store};
    use rustfs_targets::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use rustfs_targets::{StoreError, Target, TargetError};
    use serde::{Serialize, de::DeserializeOwned};
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
        E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
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
            .reload_config(rustfs_ecstore::config::Config(HashMap::new()))
            .await
            .expect("reload with empty config should succeed");

        assert_eq!(system.get_state().await, AuditSystemState::Stopped);
        assert!(system.list_targets().await.is_empty());
        assert_eq!(system.runtime_status_snapshot().await, ReplayWorkerManager::new().snapshot(0));
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*system.config.read().await, Some(rustfs_ecstore::config::Config(HashMap::new())));
    }
}
