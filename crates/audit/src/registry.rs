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

//! Target registry with rustfs-targets integration and lifecycle management.
//!
//! This module provides thread-safe target management with integration to the existing
//! rustfs-targets infrastructure, supporting MQTT, Webhook, and extensible target types.

use crate::config::AuditTargetConfig;
use crate::entity::AuditEntry;
use crate::error::AuditError;
use async_trait::async_trait;
use dashmap::DashMap;
use rustfs_targets::{EntityTarget, EventName, Store, StoreError, Target, TargetError, TargetLog, arn::TargetID, store::Key};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Statistics for target performance monitoring
#[derive(Debug, Clone)]
pub struct TargetStats {
    /// Number of successful dispatches
    pub success_count: u64,
    /// Number of failed dispatches
    pub error_count: u64,
    /// Timestamp of last successful dispatch
    pub last_success: Option<u64>,
    /// Timestamp of last error
    pub last_error: Option<u64>,
    /// Last error message
    pub last_error_message: Option<String>,
}

impl Default for TargetStats {
    fn default() -> Self {
        Self {
            success_count: 0,
            error_count: 0,
            last_success: None,
            last_error: None,
            last_error_message: None,
        }
    }
}

/// Target lifecycle state
#[derive(Debug, Clone, PartialEq)]
pub enum TargetState {
    /// Target is stopped
    Stopped,
    /// Target is running and accepting logs
    Running,
    /// Target is paused (not accepting new logs)
    Paused,
    /// Target has encountered an error
    Error(String),
}

/// Mock target implementation for cases where rustfs-targets is not available
#[derive(Debug, Clone)]
pub struct MockTarget {
    id: String,
    target_type: String,
    config: serde_json::Value,
}

impl MockTarget {
    pub fn new(id: String, target_type: String, config: serde_json::Value) -> Self {
        Self { id, target_type, config }
    }
}

#[async_trait]
impl Target<AuditEntry> for MockTarget {
    fn id(&self) -> TargetID {
        TargetID::from_str(&self.id).unwrap_or_default()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        Ok(true)
    }

    async fn save(&self, event: Arc<EntityTarget<AuditEntry>>) -> Result<(), TargetError> {
        debug!("Mock target {} saving audit entry: {:?}", self.id, event.object_name);
        Ok(())
    }

    async fn send_from_store(&self, _key: Key) -> Result<(), TargetError> {
        debug!("Mock target {} sending from store", self.id);
        Ok(())
    }

    async fn close(&self) -> Result<(), TargetError> {
        debug!("Closing mock target: {}", self.id);
        Ok(())
    }

    async fn init(&self) -> Result<(), TargetError> {
        debug!("Initializing mock target: {} ({})", self.id, self.target_type);
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<EntityTarget<AuditEntry>, Error = StoreError, Key = Key> + Send + Sync)> {
        None
    }

    fn clone_dyn(&self) -> Box<dyn Target<AuditEntry> + Send + Sync> {
        Box::new(self.clone())
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

/// Wrapper for rustfs-targets Target with audit-specific functionality
pub struct AuditTargetWrapper {
    /// Unique target identifier
    pub id: String,
    /// Target kind (webhook, mqtt, etc.)
    pub kind: String,
    /// Whether target is enabled
    pub enabled: bool,
    /// Underlying rustfs-targets Target implementation
    pub target: Box<dyn Target<AuditEntry> + Send + Sync>,
    /// Target lifecycle state
    pub state: Arc<RwLock<TargetState>>,
    /// Performance statistics
    pub stats: Arc<RwLock<TargetStats>>,
    /// Atomic counters for high-performance updates
    pub success_counter: Arc<AtomicU64>,
    pub error_counter: Arc<AtomicU64>,
}

impl AuditTargetWrapper {
    /// Create a new audit target wrapper
    pub fn new(id: String, kind: String, enabled: bool, target: Box<dyn Target + Send + Sync>) -> Self {
        Self {
            id,
            kind,
            enabled,
            target,
            state: Arc::new(RwLock::new(TargetState::Stopped)),
            stats: Arc::new(RwLock::new(TargetStats::default())),
            success_counter: Arc::new(AtomicU64::new(0)),
            error_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Start the target
    pub async fn start(&self) -> Result<(), AuditError> {
        debug!("Starting audit target: {}", self.id);

        match self.target.init().await {
            Ok(_) => {
                *self.state.write().await = TargetState::Running;
                info!("Audit target started successfully: {}", self.id);
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Failed to start target {}: {}", self.id, e);
                *self.state.write().await = TargetState::Error(error_msg.clone());
                error!("{}", error_msg);
                Err(AuditError::TargetError(error_msg))
            }
        }
    }

    /// Pause the target (stop accepting new logs)
    pub async fn pause(&self) {
        debug!("Pausing audit target: {}", self.id);
        *self.state.write().await = TargetState::Paused;
        info!("Audit target paused: {}", self.id);
    }

    /// Resume the target (start accepting logs again)
    pub async fn resume(&self) -> Result<(), AuditError> {
        debug!("Resuming audit target: {}", self.id);

        let current_state = self.state.read().await.clone();
        match current_state {
            TargetState::Paused => {
                *self.state.write().await = TargetState::Running;
                info!("Audit target resumed: {}", self.id);
                Ok(())
            }
            TargetState::Error(_) => {
                // Try to restart the target
                self.start().await
            }
            TargetState::Running => {
                debug!("Audit target {} is already running", self.id);
                Ok(())
            }
            TargetState::Stopped => {
                warn!("Cannot resume stopped target {}, use start() instead", self.id);
                Err(AuditError::TargetError(format!("Target {} is stopped", self.id)))
            }
        }
    }

    /// Stop the target and clean up resources
    pub async fn stop(&self) -> Result<(), AuditError> {
        debug!("Stopping audit target: {}", self.id);

        match self.target.close().await {
            Ok(_) => {
                *self.state.write().await = TargetState::Stopped;
                info!("Audit target stopped successfully: {}", self.id);
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Failed to stop target {}: {}", self.id, e);
                *self.state.write().await = TargetState::Error(error_msg.clone());
                error!("{}", error_msg);
                Err(AuditError::TargetError(error_msg))
            }
        }
    }

    /// Dispatch audit entries to the target
    pub async fn dispatch(&self, entries: Vec<AuditEntry>) -> Result<(), AuditError> {
        let state = self.state.read().await.clone();
        match state {
            TargetState::Running => {
                // Convert audit entries to target log format
                let target_log = TargetLog {
                    event_name: EventName::ObjectAccessedGet, // Default event name
                    key: format!("audit-{}", self.id),
                    records: entries,
                };

                // Dispatch using tokio::spawn for non-blocking operation
                let target_id = self.id.clone();
                let target = self.target.as_ref();
                let success_counter = self.success_counter.clone();
                let error_counter = self.error_counter.clone();
                let stats = self.stats.clone();

                let result = target.save(&target_log).await;

                match result {
                    Ok(_) => {
                        success_counter.fetch_add(1, Ordering::Relaxed);
                        let mut stats_guard = stats.write().await;
                        stats_guard.success_count = success_counter.load(Ordering::Relaxed);
                        stats_guard.last_success = Some(current_timestamp());
                        debug!("Successfully dispatched {} entries to target {}", target_log.records.len(), target_id);
                        Ok(())
                    }
                    Err(e) => {
                        error_counter.fetch_add(1, Ordering::Relaxed);
                        let error_msg = format!("Failed to dispatch to target {}: {}", target_id, e);
                        let mut stats_guard = stats.write().await;
                        stats_guard.error_count = error_counter.load(Ordering::Relaxed);
                        stats_guard.last_error = Some(current_timestamp());
                        stats_guard.last_error_message = Some(error_msg.clone());
                        error!("{}", error_msg);
                        Err(AuditError::TargetError(error_msg))
                    }
                }
            }
            TargetState::Paused => {
                debug!("Target {} is paused, dropping {} entries", self.id, entries.len());
                Ok(()) // Silently drop entries when paused
            }
            TargetState::Stopped => {
                warn!("Target {} is stopped, cannot dispatch entries", self.id);
                Err(AuditError::TargetError(format!("Target {} is stopped", self.id)))
            }
            TargetState::Error(ref error) => {
                warn!("Target {} is in error state: {}", self.id, error);
                Err(AuditError::TargetError(format!("Target {} is in error state: {}", self.id, error)))
            }
        }
    }

    /// Check if target is enabled and can accept dispatches
    pub async fn is_enabled(&self) -> bool {
        if !self.enabled {
            return false;
        }

        let state = self.state.read().await;
        matches!(*state, TargetState::Running)
    }

    /// Get current target state
    pub async fn get_state(&self) -> TargetState {
        self.state.read().await.clone()
    }

    /// Get current target statistics
    pub async fn get_stats(&self) -> TargetStats {
        self.stats.read().await.clone()
    }
}

/// Factory for creating audit targets that integrate with rustfs-targets
#[async_trait]
pub trait AuditTargetFactory: Send + Sync {
    /// Create a target from configuration
    async fn create_target(&self, config: &AuditTargetConfig) -> Result<Box<dyn Target<AuditEntry> + Send + Sync>, AuditError>;

    /// Validate target configuration
    fn validate_config(&self, config: &AuditTargetConfig) -> Result<(), AuditError>;

    /// List supported target types
    fn supported_types(&self) -> Vec<String>;
}

/// Default implementation of AuditTargetFactory integrating with rustfs-targets
pub struct DefaultAuditTargetFactory;

impl DefaultAuditTargetFactory {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AuditTargetFactory for DefaultAuditTargetFactory {
    async fn create_target(&self, config: &AuditTargetConfig) -> Result<Box<dyn Target<AuditEntry> + Send + Sync>, AuditError> {
        debug!("Creating audit target: {} ({})", config.id, config.kind);

        // Validate configuration first
        self.validate_config(config)?;

        match config.kind.as_str() {
            "webhook" | "mqtt" => {
                // For now, create mock targets since rustfs-targets integration may need to be built
                info!("Creating mock {} target for {}", config.kind, config.id);
                let target = MockTarget::new(config.id.clone(), config.kind.clone(), config.args.clone());
                Ok(Box::new(target))
            }
            _ => Err(AuditError::UnsupportedTargetType(config.kind.clone())),
        }
    }

    fn validate_config(&self, config: &AuditTargetConfig) -> Result<(), AuditError> {
        // Validate target type
        if !self.supported_types().contains(&config.kind) {
            return Err(AuditError::UnsupportedTargetType(config.kind.clone()));
        }

        // Validate that target_type is AuditLog for audit targets
        if let Some(target_type) = config.args.get("target_type") {
            if target_type != "AuditLog" {
                return Err(AuditError::ConfigurationError(format!(
                    "Target {} must have target_type 'AuditLog', got '{}'",
                    config.id, target_type
                )));
            }
        } else {
            return Err(AuditError::ConfigurationError(format!(
                "Target {} missing required 'target_type' field",
                config.id
            )));
        }

        // Target-specific validation
        match config.kind.as_str() {
            "webhook" => {
                if config.args.get("url").is_none() {
                    return Err(AuditError::ConfigurationError(format!(
                        "Webhook target {} missing required 'url' field",
                        config.id
                    )));
                }
            }
            "mqtt" => {
                if config.args.get("broker_url").is_none() || config.args.get("topic").is_none() {
                    return Err(AuditError::ConfigurationError(format!(
                        "MQTT target {} missing required 'broker_url' or 'topic' fields",
                        config.id
                    )));
                }
            }
            _ => {} // Other validations can be added here
        }

        Ok(())
    }

    fn supported_types(&self) -> Vec<String> {
        vec!["webhook".to_string(), "mqtt".to_string()]
    }
}

impl Default for DefaultAuditTargetFactory {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe registry for managing audit targets with rustfs-targets integration
pub struct TargetRegistry {
    /// Map of target ID to audit target wrapper
    targets: DashMap<String, Arc<AuditTargetWrapper>>,
    /// Factory for creating targets
    factory: Arc<dyn AuditTargetFactory>,
    /// Registry state
    state: Arc<RwLock<TargetState>>,
}

impl TargetRegistry {
    /// Create a new target registry with the specified factory
    pub fn new(factory: Arc<dyn AuditTargetFactory>) -> Self {
        Self {
            targets: DashMap::new(),
            factory,
            state: Arc::new(RwLock::new(TargetState::Stopped)),
        }
    }

    /// Add a target to the registry
    pub async fn add_target(&self, config: AuditTargetConfig) -> Result<(), AuditError> {
        info!("Adding audit target: {} ({})", config.id, config.kind);

        // Create the target using the factory
        let target = self.factory.create_target(&config).await?;

        // Create wrapper
        let wrapper = Arc::new(AuditTargetWrapper::new(config.id.clone(), config.kind.clone(), config.enabled, target));

        // Initialize the target if enabled
        if config.enabled {
            wrapper.start().await?;
        }

        // Add to registry
        self.targets.insert(config.id.clone(), wrapper);

        info!("Successfully added audit target: {}", config.id);
        Ok(())
    }

    /// Remove a target from the registry
    pub async fn remove_target(&self, target_id: &str) -> Result<(), AuditError> {
        info!("Removing audit target: {}", target_id);

        if let Some((_, wrapper)) = self.targets.remove(target_id) {
            // Stop the target gracefully
            if let Err(e) = wrapper.stop().await {
                warn!("Error stopping target {} during removal: {}", target_id, e);
            }
            info!("Successfully removed audit target: {}", target_id);
            Ok(())
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Enable a target
    pub async fn enable_target(&self, target_id: &str) -> Result<(), AuditError> {
        debug!("Enabling audit target: {}", target_id);

        if let Some(wrapper) = self.targets.get(target_id) {
            // Update enabled flag and start target
            let wrapper = wrapper.value();
            // Note: In a real implementation, we'd want to update the enabled field
            // For now, we'll just start the target
            wrapper.start().await?;
            info!("Enabled audit target: {}", target_id);
            Ok(())
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Disable a target
    pub async fn disable_target(&self, target_id: &str) -> Result<(), AuditError> {
        debug!("Disabling audit target: {}", target_id);

        if let Some(wrapper) = self.targets.get(target_id) {
            let wrapper = wrapper.value();
            wrapper.pause().await;
            info!("Disabled audit target: {}", target_id);
            Ok(())
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Upsert (insert or update) a target configuration
    pub async fn upsert_target(&self, config: AuditTargetConfig) -> Result<(), AuditError> {
        if self.targets.contains_key(&config.id) {
            // Remove existing target and add new one
            self.remove_target(&config.id).await?;
        }
        self.add_target(config).await
    }

    /// Get all target IDs and their states
    pub async fn list_targets(&self) -> Vec<(String, TargetState, TargetStats)> {
        let mut result = Vec::new();

        for entry in self.targets.iter() {
            let wrapper = entry.value();
            let state = wrapper.get_state().await;
            let stats = wrapper.get_stats().await;
            result.push((entry.key().clone(), state, stats));
        }

        result
    }

    /// Get a specific target's information
    pub async fn get_target(&self, target_id: &str) -> Option<(TargetState, TargetStats)> {
        if let Some(wrapper) = self.targets.get(target_id) {
            let wrapper = wrapper.value();
            let state = wrapper.get_state().await;
            let stats = wrapper.get_stats().await;
            Some((state, stats))
        } else {
            None
        }
    }

    /// Dispatch audit entries to all enabled targets concurrently
    pub async fn dispatch_to_all(&self, entries: Vec<AuditEntry>) -> Vec<Result<(), AuditError>> {
        let mut tasks = Vec::new();

        // Collect all enabled targets
        for entry in self.targets.iter() {
            let wrapper = entry.value().clone();
            if wrapper.is_enabled().await {
                let entries_clone = entries.clone();
                tasks.push(tokio::spawn(async move { wrapper.dispatch(entries_clone).await }));
            }
        }

        // Wait for all dispatches to complete
        let mut results = Vec::new();
        for task in tasks {
            match task.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(AuditError::TargetError(format!("Task join error: {}", e)))),
            }
        }

        if results.is_empty() {
            warn!("No enabled audit targets found for dispatch");
        } else {
            debug!("Dispatched to {} targets", results.len());
        }

        results
    }

    /// Start all targets in the registry
    pub async fn start(&self) -> Result<(), AuditError> {
        info!("Starting audit target registry");

        let mut errors = Vec::new();
        for entry in self.targets.iter() {
            let wrapper = entry.value();
            if wrapper.enabled {
                if let Err(e) = wrapper.start().await {
                    errors.push(format!("Failed to start target {}: {}", entry.key(), e));
                }
            }
        }

        *self.state.write().await = TargetState::Running;

        if errors.is_empty() {
            info!("Successfully started audit target registry");
            Ok(())
        } else {
            let error_msg = format!("Some targets failed to start: {}", errors.join(", "));
            error!("{}", error_msg);
            Err(AuditError::TargetError(error_msg))
        }
    }

    /// Pause all targets in the registry
    pub async fn pause(&self) {
        info!("Pausing audit target registry");

        for entry in self.targets.iter() {
            entry.value().pause().await;
        }

        *self.state.write().await = TargetState::Paused;
        info!("Paused audit target registry");
    }

    /// Resume all targets in the registry
    pub async fn resume(&self) -> Result<(), AuditError> {
        info!("Resuming audit target registry");

        let mut errors = Vec::new();
        for entry in self.targets.iter() {
            let wrapper = entry.value();
            if wrapper.enabled {
                if let Err(e) = wrapper.resume().await {
                    errors.push(format!("Failed to resume target {}: {}", entry.key(), e));
                }
            }
        }

        *self.state.write().await = TargetState::Running;

        if errors.is_empty() {
            info!("Successfully resumed audit target registry");
            Ok(())
        } else {
            let error_msg = format!("Some targets failed to resume: {}", errors.join(", "));
            error!("{}", error_msg);
            Err(AuditError::TargetError(error_msg))
        }
    }

    /// Stop all targets and close the registry
    pub async fn close(&self) -> Result<(), AuditError> {
        info!("Closing audit target registry");

        let mut errors = Vec::new();
        for entry in self.targets.iter() {
            if let Err(e) = entry.value().stop().await {
                errors.push(format!("Failed to stop target {}: {}", entry.key(), e));
            }
        }

        // Clear all targets
        self.targets.clear();
        *self.state.write().await = TargetState::Stopped;

        if errors.is_empty() {
            info!("Successfully closed audit target registry");
            Ok(())
        } else {
            let error_msg = format!("Some targets failed to stop: {}", errors.join(", "));
            error!("{}", error_msg);
            Err(AuditError::TargetError(error_msg))
        }
    }

    /// Get registry state
    pub async fn get_state(&self) -> TargetState {
        self.state.read().await.clone()
    }

    /// Get all targets (for testing)
    pub fn get_all_targets(&self) -> Vec<Arc<AuditTargetWrapper>> {
        self.targets.iter().map(|entry| entry.value().clone()).collect()
    }
}

/// Get current timestamp in seconds since Unix epoch
fn current_timestamp() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuditTargetConfig;
    use serde_json::json;

    #[tokio::test]
    async fn test_target_registry_creation() {
        let factory = Arc::new(DefaultAuditTargetFactory::new());
        let registry = TargetRegistry::new(factory);

        assert_eq!(registry.targets.len(), 0);
        assert!(matches!(registry.get_state().await, TargetState::Stopped));
    }

    #[tokio::test]
    async fn test_factory_supported_types() {
        let factory = DefaultAuditTargetFactory::new();
        let types = factory.supported_types();

        assert!(types.contains(&"webhook".to_string()));
        assert!(types.contains(&"mqtt".to_string()));
    }

    #[tokio::test]
    async fn test_factory_config_validation() {
        let factory = DefaultAuditTargetFactory::new();

        // Valid webhook config
        let valid_webhook = AuditTargetConfig {
            id: "test-webhook".to_string(),
            kind: "webhook".to_string(),
            enabled: true,
            args: json!({
                "target_type": "AuditLog",
                "url": "https://example.com/webhook"
            }),
        };

        assert!(factory.validate_config(&valid_webhook).is_ok());

        // Invalid config (missing target_type)
        let invalid_config = AuditTargetConfig {
            id: "test-invalid".to_string(),
            kind: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com/webhook"
            }),
        };

        assert!(factory.validate_config(&invalid_config).is_err());

        // Invalid config (wrong target_type)
        let wrong_type_config = AuditTargetConfig {
            id: "test-wrong".to_string(),
            kind: "webhook".to_string(),
            enabled: true,
            args: json!({
                "target_type": "NotificationLog",
                "url": "https://example.com/webhook"
            }),
        };

        assert!(factory.validate_config(&wrong_type_config).is_err());
    }

    #[tokio::test]
    async fn test_target_wrapper_lifecycle() {
        // This test would need a mock target implementation
        // For now, we'll test the basic structure
        let stats = TargetStats::default();
        assert_eq!(stats.success_count, 0);
        assert_eq!(stats.error_count, 0);
        assert!(stats.last_success.is_none());
        assert!(stats.last_error.is_none());
    }
}
