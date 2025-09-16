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

use super::config::{AuditConfig, AuditTargetConfig, TargetStatus};
use super::factory::{AuditTarget, AuditTargetFactory, DefaultAuditTargetFactory, TargetError};
use crate::entry::audit::AuditLogEntry;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{debug, error, info, warn};

/// Error types for audit system operations
#[derive(Debug, Error)]
pub enum AuditError {
    #[error("Target error: {0}")]
    Target(#[from] TargetError),
    #[error("Target not found: {0}")]
    TargetNotFound(String),
    #[error("System not initialized")]
    NotInitialized,
    #[error("Configuration error: {0}")]
    Configuration(String),
    #[error("Channel error: {0}")]
    Channel(String),
    #[error("Shutdown error: {0}")]
    Shutdown(String),
}

/// Result type for audit operations
pub type AuditResult<T> = Result<T, AuditError>;

/// Target wrapper with statistics and control
struct TargetWrapper {
    target: Box<dyn AuditTarget>,
    enabled: Arc<parking_lot::RwLock<bool>>,
    running: Arc<parking_lot::RwLock<bool>>,
    success_count: AtomicU64,
    error_count: AtomicU64,
    last_error: Arc<parking_lot::RwLock<Option<String>>>,
    last_success: Arc<parking_lot::RwLock<Option<chrono::DateTime<chrono::Utc>>>>,
    last_error_time: Arc<parking_lot::RwLock<Option<chrono::DateTime<chrono::Utc>>>>,
    config: AuditTargetConfig,
}

impl TargetWrapper {
    fn new(target: Box<dyn AuditTarget>, config: AuditTargetConfig) -> Self {
        Self {
            target,
            enabled: Arc::new(parking_lot::RwLock::new(config.enabled)),
            running: Arc::new(parking_lot::RwLock::new(false)),
            success_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_error: Arc::new(parking_lot::RwLock::new(None)),
            last_success: Arc::new(parking_lot::RwLock::new(None)),
            last_error_time: Arc::new(parking_lot::RwLock::new(None)),
            config,
        }
    }

    fn get_status(&self) -> TargetStatus {
        TargetStatus {
            id: self.target.id().to_string(),
            kind: self.target.kind().to_string(),
            enabled: *self.enabled.read(),
            running: *self.running.read(),
            last_error: self.last_error.read().clone(),
            success_count: self.success_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            last_success: *self.last_success.read(),
            last_error_time: *self.last_error_time.read(),
        }
    }

    async fn send_with_retry(&self, entry: Arc<AuditLogEntry>) -> AuditResult<()> {
        if !*self.enabled.read() {
            return Ok(());
        }

        let mut attempts = 0;
        let max_retry = self.config.max_retry;
        let retry_interval = self.config.retry_interval;

        loop {
            match self.target.send(entry.clone()).await {
                Ok(()) => {
                    self.success_count.fetch_add(1, Ordering::Relaxed);
                    *self.last_success.write() = Some(chrono::Utc::now());
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    self.error_count.fetch_add(1, Ordering::Relaxed);
                    *self.last_error.write() = Some(e.to_string());
                    *self.last_error_time.write() = Some(chrono::Utc::now());

                    if attempts >= max_retry {
                        error!("Target {} failed after {} attempts: {}", self.target.id(), attempts, e);
                        return Err(AuditError::Target(e));
                    }

                    warn!("Target {} failed (attempt {}/{}): {}, retrying in {:?}",
                          self.target.id(), attempts, max_retry, e, retry_interval);
                    tokio::time::sleep(retry_interval).await;
                }
            }
        }
    }

    async fn start(&self) -> AuditResult<()> {
        if *self.enabled.read() {
            self.target.start().await?;
            *self.running.write() = true;
            info!("Target {} started", self.target.id());
        }
        Ok(())
    }

    async fn pause(&self) -> AuditResult<()> {
        self.target.pause().await?;
        *self.running.write() = false;
        info!("Target {} paused", self.target.id());
        Ok(())
    }

    async fn resume(&self) -> AuditResult<()> {
        if *self.enabled.read() {
            self.target.resume().await?;
            *self.running.write() = true;
            info!("Target {} resumed", self.target.id());
        }
        Ok(())
    }

    async fn shutdown(&self) -> AuditResult<()> {
        self.target.shutdown().await?;
        *self.running.write() = false;
        info!("Target {} shut down", self.target.id());
        Ok(())
    }

    fn enable(&self) {
        *self.enabled.write() = true;
    }

    fn disable(&self) {
        *self.enabled.write() = false;
        *self.running.write() = false;
    }
}

/// Registry for managing audit targets with thread-safe operations
pub struct TargetRegistry {
    targets: DashMap<String, Arc<TargetWrapper>>,
    factory: Arc<dyn AuditTargetFactory>,
    semaphore: Arc<Semaphore>,
}

impl TargetRegistry {
    /// Create a new target registry with the specified factory
    pub fn new(factory: Arc<dyn AuditTargetFactory>) -> Self {
        Self {
            targets: DashMap::new(),
            factory,
            semaphore: Arc::new(Semaphore::new(16)), // Default max concurrent targets
        }
    }

    /// Add a new target from configuration
    pub async fn add_target(&self, config: AuditTargetConfig) -> AuditResult<()> {
        let target_id = config.id.clone();

        if self.targets.contains_key(&target_id) {
            return Err(AuditError::Configuration(format!("Target {} already exists", target_id)));
        }

        // Validate configuration first
        self.factory.validate_config(&config)?;

        // Create the target
        let target = self.factory.create_target(&config).await?;
        let wrapper = Arc::new(TargetWrapper::new(target, config));

        // Start the target if enabled
        if wrapper.config.enabled {
            wrapper.start().await?;
        }

        self.targets.insert(target_id.clone(), wrapper);
        info!("Target {} added to registry", target_id);
        Ok(())
    }

    /// Remove a target from the registry
    pub async fn remove_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some((_, wrapper)) = self.targets.remove(target_id) {
            wrapper.shutdown().await?;
            info!("Target {} removed from registry", target_id);
            Ok(())
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Enable a target
    pub fn enable_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.enable();
            info!("Target {} enabled", target_id);
            Ok(())
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Disable a target
    pub fn disable_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.disable();
            info!("Target {} disabled", target_id);
            Ok(())
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Start a target
    pub async fn start_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.start().await
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Pause a target
    pub async fn pause_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.pause().await
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Resume a target
    pub async fn resume_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.resume().await
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Get target status
    pub fn get_target_status(&self, target_id: &str) -> Option<TargetStatus> {
        self.targets.get(target_id).map(|wrapper| wrapper.get_status())
    }

    /// Get all target statuses
    pub fn get_all_target_statuses(&self) -> Vec<TargetStatus> {
        self.targets.iter().map(|entry| entry.value().get_status()).collect()
    }

    /// Get all targets (internal use)
    fn get_all_targets(&self) -> Vec<Arc<TargetWrapper>> {
        self.targets.iter().map(|entry| entry.value().clone()).collect()
    }

    /// Dispatch a log entry to all enabled targets
    pub async fn dispatch(&self, entry: Arc<AuditLogEntry>) -> Vec<AuditResult<()>> {
        let targets = self.get_all_targets();
        let mut results = Vec::with_capacity(targets.len());

        for wrapper in targets {
            if *wrapper.enabled.read() {
                let result = wrapper.send_with_retry(entry.clone()).await;
                results.push(result);
            } else {
                results.push(Ok(()));
            }
        }

        results
    }

    /// Shutdown all targets
    pub async fn shutdown_all(&self) -> AuditResult<()> {
        let targets = self.get_all_targets();
        let mut errors = Vec::new();

        for wrapper in targets {
            if let Err(e) = wrapper.shutdown().await {
                errors.push(e);
            }
        }

        if errors.is_empty() {
            info!("All targets shut down successfully");
            Ok(())
        } else {
            Err(AuditError::Shutdown(format!("Some targets failed to shutdown: {:?}", errors)))
        }
    }
}

/// High-performance audit system for collecting and dispatching logs
pub struct AuditSystem {
    registry: Arc<TargetRegistry>,
    tx: mpsc::UnboundedSender<Arc<AuditLogEntry>>,
    shutdown_tx: Arc<RwLock<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl AuditSystem {
    /// Create a new audit system with the specified registry
    pub fn new(registry: Arc<TargetRegistry>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let system_registry = registry.clone();

        // Start the dispatch worker
        tokio::spawn(async move {
            Self::dispatch_worker(system_registry, rx, shutdown_rx).await;
        });

        Self {
            registry,
            tx,
            shutdown_tx: Arc::new(RwLock::new(Some(shutdown_tx))),
        }
    }

    /// Log an audit entry asynchronously
    pub async fn log(&self, entry: Arc<AuditLogEntry>) -> AuditResult<()> {
        self.tx.send(entry)
            .map_err(|_| AuditError::Channel("Failed to send log entry".to_string()))?;
        Ok(())
    }

    /// Get target registry for management operations
    pub fn registry(&self) -> &Arc<TargetRegistry> {
        &self.registry
    }

    /// Shutdown the audit system gracefully
    pub async fn shutdown(&self) -> AuditResult<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.write().await.take() {
            let _ = shutdown_tx.send(());
            self.registry.shutdown_all().await?;
            info!("Audit system shut down");
        }
        Ok(())
    }

    /// Background worker for dispatching log entries
    async fn dispatch_worker(
        registry: Arc<TargetRegistry>,
        mut rx: mpsc::UnboundedReceiver<Arc<AuditLogEntry>>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        info!("Audit dispatch worker started");

        loop {
            tokio::select! {
                entry = rx.recv() => {
                    match entry {
                        Some(entry) => {
                            let results = registry.dispatch(entry).await;
                            let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
                            if !errors.is_empty() {
                                warn!("Some targets failed during dispatch: {:?}", errors);
                            }
                        }
                        None => {
                            debug!("Audit log channel closed");
                            break;
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    info!("Audit dispatch worker shutting down");
                    break;
                }
            }
        }

        info!("Audit dispatch worker stopped");
    }
}

/// Unified audit manager combining registry and system
#[derive(Clone)]
pub struct AuditManager {
    registry: Arc<TargetRegistry>,
    system: Arc<AuditSystem>,
}

impl AuditManager {
    /// Create a new audit manager with default factory
    pub fn new() -> Self {
        let factory = Arc::new(DefaultAuditTargetFactory::new());
        let registry = Arc::new(TargetRegistry::new(factory));
        let system = Arc::new(AuditSystem::new(registry.clone()));

        Self { registry, system }
    }

    /// Create a new audit manager with custom factory
    pub fn with_factory(factory: Arc<dyn AuditTargetFactory>) -> Self {
        let registry = Arc::new(TargetRegistry::new(factory));
        let system = Arc::new(AuditSystem::new(registry.clone()));

        Self { registry, system }
    }

    /// Load configuration and initialize targets
    pub async fn load_config(&self, config: &AuditConfig) -> AuditResult<()> {
        info!("Loading audit configuration with {} targets", config.targets.len());

        for target_config in &config.targets {
            if let Err(e) = self.registry.add_target(target_config.clone()).await {
                error!("Failed to add target {}: {}", target_config.id, e);
                // Continue with other targets instead of failing completely
            }
        }

        info!("Audit configuration loaded successfully");
        Ok(())
    }

    /// Get the target registry
    pub fn registry(&self) -> &Arc<TargetRegistry> {
        &self.registry
    }

    /// Get the audit system
    pub fn system(&self) -> &Arc<AuditSystem> {
        &self.system
    }

    /// Log an audit entry
    pub async fn log(&self, entry: Arc<AuditLogEntry>) -> AuditResult<()> {
        self.system.log(entry).await
    }

    /// Shutdown the audit manager
    pub async fn shutdown(&self) -> AuditResult<()> {
        self.system.shutdown().await
    }
}

impl Default for AuditManager {
    fn default() -> Self {
        Self::new()
    }
}
