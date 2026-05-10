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

        // Create targets from configuration
        let mut registry = self.registry.lock().await;
        match registry.create_audit_targets_from_config(&config).await {
            Ok(targets) => {
                if targets.is_empty() {
                    info!("No enabled audit targets found, keeping audit system stopped");
                    drop(registry);
                    return Ok(());
                }

                {
                    let mut state = self.state.write().await;
                    *state = AuditSystemState::Starting;
                }

                info!(target_count = targets.len(), "Created audit targets successfully");

                let activation = self
                    .runtime_facade()
                    .activate_targets_with_replay(self.state.clone(), targets)
                    .await;
                for target in activation.targets {
                    let target_id = target.id().to_string();
                    registry.add_target(target_id, target.clone_dyn());
                }
                *self.stream_cancellers.write().await = activation.replay_workers;

                // Update state to running
                let mut state = self.state.write().await;
                *state = AuditSystemState::Running;
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
        if let Err(e) = async {
            self.runtime_facade().stop_replay_workers().await;
            let mut registry = self.registry.lock().await;
            registry.close_all().await
        }
        .await
        {
            error!(error = %e, "Failed to close some audit targets");
        }

        // Update state to stopped
        let mut state = self.state.write().await;
        *state = AuditSystemState::Stopped;

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

        let targets = {
            let registry = self.registry.lock().await;
            registry.create_audit_targets_from_config(&new_config).await
        };

        match targets {
            Ok(targets) => {
                info!(target_count = targets.len(), "Reloaded audit targets successfully");

                let activation = self
                    .runtime_facade()
                    .activate_targets_with_replay(self.state.clone(), targets)
                    .await;
                if let Err(e) = self.runtime_facade().replace_targets(activation).await {
                    error!(error = %e, "Failed to replace existing targets during reload");
                    return Err(e);
                }

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
