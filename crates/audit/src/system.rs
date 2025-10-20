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
    StoreError, Target, TargetError,
    store::{Key, Store},
    target::EntityTarget,
};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

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
        }
    }

    /// Starts the audit system with the given configuration
    pub async fn start(&self, config: Config) -> AuditResult<()> {
        let mut state = self.state.write().await;

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

        *state = AuditSystemState::Starting;
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
        match registry.create_targets_from_config(&config).await {
            Ok(targets) => {
                info!(target_count = targets.len(), "Created audit targets successfully");

                // Initialize all targets
                for target in targets {
                    let target_id = target.id().to_string();
                    if let Err(e) = target.init().await {
                        error!(target_id = %target_id, error = %e, "Failed to initialize audit target");
                    } else {
                        // After successful initialization, if enabled and there is a store, start the send from storage task
                        if target.is_enabled() {
                            if let Some(store) = target.store() {
                                info!(target_id = %target_id, "Start audit stream processing for target");
                                let store_clone: Box<dyn Store<EntityTarget<AuditEntry>, Error = StoreError, Key = Key> + Send> =
                                    store.boxed_clone();
                                let target_arc: Arc<dyn Target<AuditEntry> + Send + Sync> = Arc::from(target.clone_dyn());
                                self.start_audit_stream_with_batching(store_clone, target_arc);
                                info!(target_id = %target_id, "Audit stream processing started");
                            } else {
                                info!(target_id = %target_id, "No store configured, skip audit stream processing");
                            }
                        } else {
                            info!(target_id = %target_id, "Target disabled, skip audit stream processing");
                        }
                        registry.add_target(target_id, target);
                    }
                }

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
            _ => Err(AuditError::Configuration("Cannot pause audit system in current state".to_string())),
        }
    }

    /// Resumes the audit system
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
            _ => Err(AuditError::Configuration("Cannot resume audit system in current state".to_string())),
        }
    }

    /// Stops the audit system and closes all targets
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
    pub async fn is_running(&self) -> bool {
        matches!(*self.state.read().await, AuditSystemState::Running)
    }

    /// Dispatches an audit log entry to all active targets
    pub async fn dispatch(&self, entry: Arc<AuditEntry>) -> AuditResult<()> {
        let start_time = std::time::Instant::now();

        let state = self.state.read().await;

        match *state {
            AuditSystemState::Running => {
                // Continue with dispatch
                info!("Dispatching audit log entry");
            }
            AuditSystemState::Paused => {
                // Skip dispatch when paused
                return Ok(());
            }
            _ => {
                // Don't dispatch when not running
                return Err(AuditError::NotInitialized("Audit system is not running".to_string()));
            }
        }
        drop(state);

        let registry = self.registry.lock().await;
        let target_ids = registry.list_targets();

        if target_ids.is_empty() {
            warn!("No audit targets configured for dispatch");
            return Ok(());
        }

        // Dispatch to all targets concurrently
        let mut tasks = Vec::new();

        for target_id in target_ids {
            if let Some(target) = registry.get_target(&target_id) {
                let entry_clone = Arc::clone(&entry);
                let target_id_clone = target_id.clone();

                // Create EntityTarget for the audit log entry
                let entity_target = EntityTarget {
                    object_name: entry.api.name.clone().unwrap_or_default(),
                    bucket_name: entry.api.bucket.clone().unwrap_or_default(),
                    event_name: rustfs_targets::EventName::ObjectCreatedPut, // Default, should be derived from entry
                    data: (*entry_clone).clone(),
                };

                let task = async move {
                    let result = target.save(Arc::new(entity_target)).await;
                    (target_id_clone, result)
                };

                tasks.push(task);
            }
        }

        // Execute all dispatch tasks
        let results = futures::future::join_all(tasks).await;

        let mut errors = Vec::new();
        let mut success_count = 0;

        for (target_id, result) in results {
            match result {
                Ok(_) => {
                    success_count += 1;
                    observability::record_target_success();
                }
                Err(e) => {
                    error!(target_id = %target_id, error = %e, "Failed to dispatch audit log to target");
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

    pub async fn dispatch_batch(&self, entries: Vec<Arc<AuditEntry>>) -> AuditResult<()> {
        let start_time = std::time::Instant::now();

        let state = self.state.read().await;
        if *state != AuditSystemState::Running {
            return Err(AuditError::NotInitialized("Audit system is not running".to_string()));
        }
        drop(state);

        let registry = self.registry.lock().await;
        let target_ids = registry.list_targets();

        if target_ids.is_empty() {
            warn!("No audit targets configured for batch dispatch");
            return Ok(());
        }

        let mut tasks = Vec::new();
        for target_id in target_ids {
            if let Some(target) = registry.get_target(&target_id) {
                let entries_clone: Vec<_> = entries.iter().map(Arc::clone).collect();
                let target_id_clone = target_id.clone();

                let task = async move {
                    let mut success_count = 0;
                    let mut errors = Vec::new();
                    for entry in entries_clone {
                        let entity_target = EntityTarget {
                            object_name: entry.api.name.clone().unwrap_or_default(),
                            bucket_name: entry.api.bucket.clone().unwrap_or_default(),
                            event_name: rustfs_targets::EventName::ObjectCreatedPut,
                            data: (*entry).clone(),
                        };
                        match target.save(Arc::new(entity_target)).await {
                            Ok(_) => success_count += 1,
                            Err(e) => errors.push(e),
                        }
                    }
                    (target_id_clone, success_count, errors)
                };
                tasks.push(task);
            }
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

    // New: Audit flow background tasks, based on send_from_store, including retries and exponential backoffs
    fn start_audit_stream_with_batching(
        &self,
        store: Box<dyn Store<EntityTarget<AuditEntry>, Error = StoreError, Key = Key> + Send>,
        target: Arc<dyn Target<AuditEntry> + Send + Sync>,
    ) {
        let state = self.state.clone();

        tokio::spawn(async move {
            use std::time::Duration;
            use tokio::time::sleep;

            info!("Starting audit stream for target: {}", target.id());

            const MAX_RETRIES: usize = 5;
            const BASE_RETRY_DELAY: Duration = Duration::from_secs(2);

            loop {
                match *state.read().await {
                    AuditSystemState::Running | AuditSystemState::Paused | AuditSystemState::Starting => {}
                    _ => {
                        info!("Audit stream stopped for target: {}", target.id());
                        break;
                    }
                }

                let keys: Vec<Key> = store.list();
                if keys.is_empty() {
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }

                for key in keys {
                    let mut retries = 0usize;
                    let mut success = false;

                    while retries < MAX_RETRIES && !success {
                        match target.send_from_store(key.clone()).await {
                            Ok(_) => {
                                info!("Successfully sent audit entry, target: {}, key: {}", target.id(), key.to_string());
                                observability::record_target_success();
                                success = true;
                            }
                            Err(e) => {
                                match &e {
                                    TargetError::NotConnected => {
                                        warn!("Target {} not connected, retrying...", target.id());
                                    }
                                    TargetError::Timeout(_) => {
                                        warn!("Timeout sending to target {}, retrying...", target.id());
                                    }
                                    _ => {
                                        error!("Permanent error for target {}: {}", target.id(), e);
                                        observability::record_target_failure();
                                        break;
                                    }
                                }
                                retries += 1;
                                let backoff = BASE_RETRY_DELAY * (1 << retries);
                                sleep(backoff).await;
                            }
                        }
                    }

                    if retries >= MAX_RETRIES && !success {
                        warn!("Max retries exceeded for key {}, target: {}, skipping", key.to_string(), target.id());
                        observability::record_target_failure();
                    }
                }

                sleep(Duration::from_millis(100)).await;
            }
        });
    }

    /// Enables a specific target
    pub async fn enable_target(&self, target_id: &str) -> AuditResult<()> {
        // This would require storing enabled/disabled state per target
        // For now, just check if target exists
        let registry = self.registry.lock().await;
        if registry.get_target(target_id).is_some() {
            info!(target_id = %target_id, "Target enabled");
            Ok(())
        } else {
            Err(AuditError::Configuration(format!("Target not found: {target_id}")))
        }
    }

    /// Disables a specific target
    pub async fn disable_target(&self, target_id: &str) -> AuditResult<()> {
        // This would require storing enabled/disabled state per target
        // For now, just check if target exists
        let registry = self.registry.lock().await;
        if registry.get_target(target_id).is_some() {
            info!(target_id = %target_id, "Target disabled");
            Ok(())
        } else {
            Err(AuditError::Configuration(format!("Target not found: {target_id}")))
        }
    }

    /// Removes a target from the system
    pub async fn remove_target(&self, target_id: &str) -> AuditResult<()> {
        let mut registry = self.registry.lock().await;
        if let Some(target) = registry.remove_target(target_id) {
            if let Err(e) = target.close().await {
                error!(target_id = %target_id, error = %e, "Failed to close removed target");
            }
            info!(target_id = %target_id, "Target removed");
            Ok(())
        } else {
            Err(AuditError::Configuration(format!("Target not found: {target_id}")))
        }
    }

    /// Updates or inserts a target
    pub async fn upsert_target(&self, target_id: String, target: Box<dyn Target<AuditEntry> + Send + Sync>) -> AuditResult<()> {
        let mut registry = self.registry.lock().await;

        // Initialize the target
        if let Err(e) = target.init().await {
            return Err(AuditError::Target(e));
        }

        // Remove existing target if present
        if let Some(old_target) = registry.remove_target(&target_id) {
            if let Err(e) = old_target.close().await {
                error!(target_id = %target_id, error = %e, "Failed to close old target during upsert");
            }
        }

        registry.add_target(target_id.clone(), target);
        info!(target_id = %target_id, "Target upserted");
        Ok(())
    }

    /// Lists all targets
    pub async fn list_targets(&self) -> Vec<String> {
        let registry = self.registry.lock().await;
        registry.list_targets()
    }

    /// Gets information about a specific target
    pub async fn get_target(&self, target_id: &str) -> Option<String> {
        let registry = self.registry.lock().await;
        registry.get_target(target_id).map(|target| target.id().to_string())
    }

    /// Reloads configuration and updates targets
    pub async fn reload_config(&self, new_config: Config) -> AuditResult<()> {
        info!("Reloading audit system configuration");

        // Record config reload
        observability::record_config_reload();

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
        match registry.create_targets_from_config(&new_config).await {
            Ok(targets) => {
                info!(target_count = targets.len(), "Reloaded audit targets successfully");

                // Initialize all new targets
                for target in targets {
                    let target_id = target.id().to_string();
                    if let Err(e) = target.init().await {
                        error!(target_id = %target_id, error = %e, "Failed to initialize reloaded audit target");
                    } else {
                        // Same starts the storage stream after a heavy load
                        if target.is_enabled() {
                            if let Some(store) = target.store() {
                                info!(target_id = %target_id, "Start audit stream processing for target (reload)");
                                let store_clone: Box<dyn Store<EntityTarget<AuditEntry>, Error = StoreError, Key = Key> + Send> =
                                    store.boxed_clone();
                                let target_arc: Arc<dyn Target<AuditEntry> + Send + Sync> = Arc::from(target.clone_dyn());
                                self.start_audit_stream_with_batching(store_clone, target_arc);
                                info!(target_id = %target_id, "Audit stream processing started (reload)");
                            } else {
                                info!(target_id = %target_id, "No store configured, skip audit stream processing (reload)");
                            }
                        } else {
                            info!(target_id = %target_id, "Target disabled, skip audit stream processing (reload)");
                        }
                        registry.add_target(target.id().to_string(), target);
                    }
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
    pub async fn get_metrics(&self) -> observability::AuditMetricsReport {
        observability::get_metrics_report().await
    }

    /// Validates system performance against requirements
    pub async fn validate_performance(&self) -> observability::PerformanceValidation {
        observability::validate_performance().await
    }

    /// Resets all metrics
    pub async fn reset_metrics(&self) {
        observability::reset_metrics().await;
    }
}
