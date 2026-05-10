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

use crate::{AuditEntry, AuditError, AuditRegistry, AuditResult, observability};
use rustfs_ecstore::config::Config;
use rustfs_targets::{
    ReplayWorkerManager, RuntimeActivation, Target, TargetError, activate_targets_with_replay,
    init_target_and_optionally_start_replay, start_replay_worker, target::EntityTarget,
};
use std::sync::Arc;
use std::time::Duration;
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

                let activation = self.activate_targets_with_replay(targets).await;
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
        self.stop_all_streams().await;

        // Close all targets
        let mut registry = self.registry.lock().await;
        if let Err(e) = registry.close_all().await {
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
        let start_time = std::time::Instant::now();

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

        // Collect cloned targets under lock, then dispatch without holding it
        let targets: Vec<(String, Box<dyn Target<AuditEntry> + Send + Sync>)> = {
            let registry = self.registry.lock().await;
            let target_keys = registry.list_targets();

            if target_keys.is_empty() {
                warn!("No audit targets configured for dispatch");
                return Ok(());
            }

            target_keys
                .into_iter()
                .filter_map(|key| registry.get_target(&key).map(|t| (key, t.clone_dyn())))
                .collect()
        };

        // Dispatch to all targets concurrently (no lock held)
        let mut tasks = Vec::new();

        for (target_key, target) in targets {
            let entity_target = EntityTarget {
                object_name: entry.api.name.clone().unwrap_or_default(),
                bucket_name: entry.api.bucket.clone().unwrap_or_default(),
                event_name: entry.event,
                data: (*entry).clone(),
            };

            let task = async move {
                let result = target.save(Arc::new(entity_target)).await;
                (target_key, result)
            };

            tasks.push(task);
        }

        // Execute all dispatch tasks
        let results = futures::future::join_all(tasks).await;

        let mut errors = Vec::new();
        let mut success_count = 0;

        for (target_key, result) in results {
            match result {
                Ok(_) => {
                    success_count += 1;
                    observability::record_target_success();
                }
                Err(e) => {
                    error!(target_id = %target_key, error = %e, "Failed to dispatch audit log to target");
                    errors.push(e);
                    observability::record_target_failure();
                }
            }
        }

        let dispatch_time = start_time.elapsed();

        if errors.is_empty() {
            observability::record_audit_success(dispatch_time);
        } else {
            observability::record_audit_failure(dispatch_time);
            // Log errors but don't fail the entire dispatch
            warn!(
                error_count = errors.len(),
                success_count = success_count,
                "Some audit targets failed to receive log entry"
            );
        }

        Ok(())
    }

    /// Dispatches a batch of audit log entries to all active targets
    ///
    /// # Arguments
    /// * `entries` - A vector of audit log entries to dispatch
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn dispatch_batch(&self, entries: Vec<Arc<AuditEntry>>) -> AuditResult<()> {
        let start_time = std::time::Instant::now();

        let state = self.state.read().await;
        if *state != AuditSystemState::Running {
            return Err(AuditError::NotInitialized("Audit system is not running".to_string()));
        }
        drop(state);

        // Collect targets under lock, then dispatch without holding it
        let targets: Vec<(String, Box<dyn Target<AuditEntry> + Send + Sync>)> = {
            let registry = self.registry.lock().await;
            let target_keys = registry.list_targets();

            if target_keys.is_empty() {
                warn!("No audit targets configured for batch dispatch");
                return Ok(());
            }

            target_keys
                .into_iter()
                .filter_map(|key| registry.get_target(&key).map(|t| (key, t.clone_dyn())))
                .collect()
        };

        let mut tasks = Vec::new();
        for (target_key, target) in targets {
            let entries_clone: Vec<_> = entries.iter().map(Arc::clone).collect();
            let target_key_clone = target_key.clone();

            let task = async move {
                let mut success_count = 0;
                let mut errors = Vec::new();
                for entry in entries_clone {
                    let entity_target = EntityTarget {
                        object_name: entry.api.name.clone().unwrap_or_default(),
                        bucket_name: entry.api.bucket.clone().unwrap_or_default(),
                        event_name: entry.event,
                        data: (*entry).clone(),
                    };
                    match target.save(Arc::new(entity_target)).await {
                        Ok(_) => success_count += 1,
                        Err(e) => errors.push(e),
                    }
                }
                (target_key_clone, success_count, errors)
            };
            tasks.push(task);
        }

        let results = futures::future::join_all(tasks).await;
        let mut total_success = 0;
        let mut total_errors = 0;
        for (_target_id, success_count, errors) in results {
            total_success += success_count;
            total_errors += errors.len();
            for e in errors {
                error!("Batch dispatch error: {:?}", e);
            }
        }

        let dispatch_time = start_time.elapsed();
        info!(
            "Batch dispatched {} entries, success: {}, errors: {}, time: {:?}",
            entries.len(),
            total_success,
            total_errors,
            dispatch_time
        );

        Ok(())
    }

    /// Stops all active audit stream tasks by sending cancellation signals.
    async fn stop_all_streams(&self) {
        let mut cancellers = self.stream_cancellers.write().await;
        cancellers.stop_all("Stopping audit stream").await;
    }

    async fn activate_targets_with_replay(
        &self,
        targets: Vec<Box<dyn Target<AuditEntry> + Send + Sync>>,
    ) -> RuntimeActivation<AuditEntry> {
        activate_targets_with_replay(targets, |target| {
            let state = self.state.clone();
            async move {
                init_target_and_optionally_start_replay(
                    target,
                    |target_id, has_replay| {
                        if has_replay {
                            info!(target_id = %target_id, "Audit stream processing started");
                        } else {
                            info!(target_id = %target_id, "No store configured, skip audit stream processing");
                        }
                    },
                    move |store, target| {
                        start_replay_worker(
                            store,
                            target,
                            Arc::new(move |event| {
                                let state = state.clone();
                                Box::pin(async move {
                                    if !matches!(
                                        *state.read().await,
                                        AuditSystemState::Running | AuditSystemState::Paused | AuditSystemState::Starting
                                    ) {
                                        return;
                                    }
                                    match event {
                                        rustfs_targets::runtime::ReplayEvent::Delivered { key, target } => {
                                            info!(
                                                "Successfully sent audit entry, target: {}, key: {}",
                                                target.id(),
                                                key.to_string()
                                            );
                                            observability::record_target_success();
                                        }
                                        rustfs_targets::runtime::ReplayEvent::RetryableError { error, target, .. } => match error
                                        {
                                            TargetError::NotConnected => {
                                                warn!("Target {} not connected, retrying...", target.id());
                                            }
                                            TargetError::Timeout(_) => {
                                                warn!("Timeout sending to target {}, retrying...", target.id());
                                            }
                                            _ => {}
                                        },
                                        rustfs_targets::runtime::ReplayEvent::Dropped { reason, target, .. } => {
                                            warn!("Dropped queued payload for target {}: {}", target.id(), reason);
                                            observability::record_target_failure();
                                        }
                                        rustfs_targets::runtime::ReplayEvent::PermanentFailure { error, target, .. } => {
                                            error!("Permanent error for target {}: {}", target.id(), error);
                                            target.record_final_failure();
                                            observability::record_target_failure();
                                        }
                                        rustfs_targets::runtime::ReplayEvent::RetryExhausted { key, target } => {
                                            warn!(
                                                "Max retries exceeded for key {}, target: {}, skipping",
                                                key.to_string(),
                                                target.id()
                                            );
                                            target.record_final_failure();
                                            observability::record_target_failure();
                                        }
                                        rustfs_targets::runtime::ReplayEvent::UnreadableEntry { key, error, target } => {
                                            warn!(
                                                "Skipping unreadable audit store entry {} for target {}: {}",
                                                key,
                                                target.id(),
                                                error
                                            );
                                        }
                                    }
                                })
                            }),
                            None,
                            Duration::from_millis(500),
                            Duration::from_millis(500),
                        )
                    },
                )
                .await
            }
        })
        .await
    }

    /// Enables a specific target
    ///
    /// # Arguments
    /// * `target_id` - The ID of the target to enable, TargetID to string
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn enable_target(&self, target_id: &str) -> AuditResult<()> {
        // This would require storing enabled/disabled state per target
        // For now, just check if target exists
        let registry = self.registry.lock().await;
        if registry.get_target(target_id).is_some() {
            info!(target_id = %target_id, "Target enabled");
            Ok(())
        } else {
            Err(AuditError::Configuration(format!("Target not found: {target_id}"), None))
        }
    }

    /// Disables a specific target
    ///
    /// # Arguments
    /// * `target_id` - The ID of the target to disable, TargetID to string
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn disable_target(&self, target_id: &str) -> AuditResult<()> {
        // This would require storing enabled/disabled state per target
        // For now, just check if target exists
        let registry = self.registry.lock().await;
        if registry.get_target(target_id).is_some() {
            info!(target_id = %target_id, "Target disabled");
            Ok(())
        } else {
            Err(AuditError::Configuration(format!("Target not found: {target_id}"), None))
        }
    }

    /// Removes a target from the system
    ///
    /// # Arguments
    /// * `target_id` - The ID of the target to remove, TargetID to string
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure
    pub async fn remove_target(&self, target_id: &str) -> AuditResult<()> {
        let mut registry = self.registry.lock().await;
        if registry.remove_target(target_id).await.is_some() {
            info!(target_id = %target_id, "Target removed");
            Ok(())
        } else {
            Err(AuditError::Configuration(format!("Target not found: {target_id}"), None))
        }
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
        let mut registry = self.registry.lock().await;

        // Initialize the target
        if let Err(e) = target.init().await {
            return Err(AuditError::Target(e));
        }

        // Remove existing target if present
        let _ = registry.remove_target(&target_id).await;

        registry.add_target(target_id.clone(), target);
        info!(target_id = %target_id, "Target upserted");
        Ok(())
    }

    /// Lists all targets
    ///
    /// # Returns
    /// * `Vec<String>` - List of target IDs
    pub async fn list_targets(&self) -> Vec<String> {
        let registry = self.registry.lock().await;
        registry.list_targets()
    }

    /// Returns cloned target values for read-only runtime inspection.
    pub async fn get_target_values(&self) -> Vec<rustfs_targets::SharedTarget<AuditEntry>> {
        let registry = self.registry.lock().await;
        registry.list_target_values()
    }

    /// Returns per-target delivery metrics for Prometheus collection.
    pub async fn snapshot_target_metrics(&self) -> Vec<AuditTargetMetricSnapshot> {
        let registry = self.registry.lock().await;
        registry
            .list_target_values()
            .into_iter()
            .map(|target| {
                let delivery = target.delivery_snapshot();
                AuditTargetMetricSnapshot {
                    failed_messages: delivery.failed_messages,
                    queue_length: delivery.queue_length,
                    target_id: target.id().to_string(),
                    total_messages: delivery.total_messages,
                }
            })
            .collect()
    }

    pub async fn snapshot_target_health(&self) -> Vec<rustfs_targets::RuntimeTargetHealthSnapshot> {
        let registry = self.registry.lock().await;
        let manager = registry.runtime_manager();
        manager.health_snapshots().await
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
        let registry = self.registry.lock().await;
        registry.get_target(target_id).map(|target| target.id().to_string())
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

        // Stop all existing stream tasks first
        self.stop_all_streams().await;

        // Store new configuration
        {
            let mut config_guard = self.config.write().await;
            *config_guard = Some(new_config.clone());
        }

        // Close all existing targets
        let mut registry = self.registry.lock().await;
        if let Err(e) = registry.close_all().await {
            error!(error = %e, "Failed to close existing targets during reload");
        }

        // Create new targets from updated configuration
        match registry.create_audit_targets_from_config(&new_config).await {
            Ok(targets) => {
                info!(target_count = targets.len(), "Reloaded audit targets successfully");

                let activation = self.activate_targets_with_replay(targets).await;
                for target in activation.targets {
                    let target_id = target.id().to_string();
                    info!(target_id = %target_id, "Target initialized (reload)");
                    registry.add_target(target_id, target.clone_dyn());
                }
                *self.stream_cancellers.write().await = activation.replay_workers;

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
