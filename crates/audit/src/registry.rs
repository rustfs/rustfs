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

//! Target registry for unified lifecycle management and factory pattern

use crate::config::AuditTargetConfig;
use crate::entity::AuditEntry;
use crate::error::{AuditError, AuditResult};
use crate::targets::{MockTarget, AuditWebhookTarget, AuditMqttTarget};
use async_trait::async_trait;
use dashmap::DashMap;
use rustfs_targets::target::{EntityTarget, Target, TargetType};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Factory for creating audit targets
#[async_trait]
pub trait AuditTargetFactory: Send + Sync + 'static {
    /// Create a new target from configuration
    async fn create_target(&self, config: &AuditTargetConfig) -> AuditResult<Box<dyn Target<AuditEntry> + Send + Sync>>;
    
    /// Validate target configuration
    fn validate_config(&self, config: &AuditTargetConfig) -> AuditResult<()>;
    
    /// Get supported target types
    fn supported_types(&self) -> Vec<String>;
}

/// Default implementation of the audit target factory
pub struct DefaultAuditTargetFactory;

impl DefaultAuditTargetFactory {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AuditTargetFactory for DefaultAuditTargetFactory {
    async fn create_target(&self, config: &AuditTargetConfig) -> AuditResult<Box<dyn Target<AuditEntry> + Send + Sync>> {
        // Validate TargetType::AuditLog requirement
        if let Some(target_type) = config.args.get("target_type") {
            if target_type.as_str() != Some(TargetType::AuditLog.as_str()) {
                return Err(AuditError::ConfigurationError(
                    format!("Target type must be '{}' for audit targets", TargetType::AuditLog.as_str())
                ));
            }
        }

        match config.kind.as_str() {
            "webhook" => {
                let target = AuditWebhookTarget::new(config).await?;
                Ok(Box::new(target))
            }
            "mqtt" => {
                let target = AuditMqttTarget::new(config).await?;
                Ok(Box::new(target))
            }
            "mock" => {
                let target = MockTarget::new(&config.id);
                Ok(Box::new(target))
            }
            _ => Err(AuditError::UnsupportedTargetType(config.kind.clone())),
        }
    }

    fn validate_config(&self, config: &AuditTargetConfig) -> AuditResult<()> {
        match config.kind.as_str() {
            "webhook" => {
                if !config.args.get("url").and_then(|v| v.as_str()).map_or(false, |s| !s.is_empty()) {
                    return Err(AuditError::ConfigurationError("Webhook URL is required".to_string()));
                }
            }
            "mqtt" => {
                if !config.args.get("broker_url").and_then(|v| v.as_str()).map_or(false, |s| !s.is_empty()) {
                    return Err(AuditError::ConfigurationError("MQTT broker URL is required".to_string()));
                }
                if !config.args.get("topic").and_then(|v| v.as_str()).map_or(false, |s| !s.is_empty()) {
                    return Err(AuditError::ConfigurationError("MQTT topic is required".to_string()));
                }
            }
            "mock" => {
                // Mock targets have no special requirements
            }
            _ => return Err(AuditError::UnsupportedTargetType(config.kind.clone())),
        }
        Ok(())
    }

    fn supported_types(&self) -> Vec<String> {
        vec!["webhook".to_string(), "mqtt".to_string(), "mock".to_string()]
    }
}

/// Statistics for individual targets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetStats {
    pub target_id: String,
    pub target_kind: String,
    pub enabled: bool,
    pub success_count: u64,
    pub error_count: u64,
    pub last_success_timestamp: Option<u64>,
    pub last_error_timestamp: Option<u64>,
    pub last_error_message: Option<String>,
}

/// Internal target wrapper with statistics
struct AuditTargetWrapper {
    target: Box<dyn Target<AuditEntry> + Send + Sync>,
    enabled: AtomicBool,
    success_count: AtomicU64,
    error_count: AtomicU64,
    last_success_timestamp: AtomicU64,
    last_error_timestamp: AtomicU64,
    last_error_message: RwLock<Option<String>>,
    config: AuditTargetConfig,
}

impl AuditTargetWrapper {
    fn new(target: Box<dyn Target<AuditEntry> + Send + Sync>, config: AuditTargetConfig) -> Self {
        Self {
            target,
            enabled: AtomicBool::new(config.enabled),
            success_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_success_timestamp: AtomicU64::new(0),
            last_error_timestamp: AtomicU64::new(0),
            last_error_message: RwLock::new(None),
            config,
        }
    }

    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    async fn record_success(&self) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_success_timestamp.store(timestamp, Ordering::Relaxed);
    }

    async fn record_error(&self, error: &str) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_error_timestamp.store(timestamp, Ordering::Relaxed);
        *self.last_error_message.write().await = Some(error.to_string());
    }

    async fn get_stats(&self) -> TargetStats {
        let last_error_message = self.last_error_message.read().await.clone();
        let last_success = self.last_success_timestamp.load(Ordering::Relaxed);
        let last_error = self.last_error_timestamp.load(Ordering::Relaxed);

        TargetStats {
            target_id: self.config.id.clone(),
            target_kind: self.config.kind.clone(),
            enabled: self.is_enabled(),
            success_count: self.success_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            last_success_timestamp: if last_success > 0 { Some(last_success) } else { None },
            last_error_timestamp: if last_error > 0 { Some(last_error) } else { None },
            last_error_message,
        }
    }

    async fn save(&self, event: Arc<EntityTarget<AuditEntry>>) -> AuditResult<()> {
        if !self.is_enabled() {
            return Ok(()); // Skip disabled targets
        }

        match self.target.save(event).await {
            Ok(()) => {
                self.record_success().await;
                debug!(target_id = %self.config.id, "Successfully dispatched audit event");
                Ok(())
            }
            Err(e) => {
                let error_msg = e.to_string();
                self.record_error(&error_msg).await;
                error!(target_id = %self.config.id, error = %error_msg, "Failed to dispatch audit event");
                Err(AuditError::TargetError(e))
            }
        }
    }
}

/// Registry statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryStats {
    pub total_targets: usize,
    pub enabled_targets: usize,
    pub total_success: u64,
    pub total_errors: u64,
    pub is_paused: bool,
}

/// Target registry with unified lifecycle management
pub struct TargetRegistry {
    targets: DashMap<String, Arc<AuditTargetWrapper>>,
    factory: Arc<dyn AuditTargetFactory>,
    is_paused: AtomicBool,
}

impl TargetRegistry {
    pub fn new(factory: Arc<dyn AuditTargetFactory>) -> Self {
        Self {
            targets: DashMap::new(),
            factory,
            is_paused: AtomicBool::new(false),
        }
    }

    /// Add a new target to the registry
    pub async fn add_target(&self, config: AuditTargetConfig) -> AuditResult<()> {
        info!(target_id = %config.id, target_kind = %config.kind, "Adding audit target");

        // Validate configuration
        self.factory.validate_config(&config)?;

        // Create target
        let target = self.factory.create_target(&config).await?;

        // Initialize target
        target.init().await.map_err(AuditError::TargetError)?;

        // Wrap and store
        let wrapper = Arc::new(AuditTargetWrapper::new(target, config.clone()));
        self.targets.insert(config.id.clone(), wrapper);

        info!(target_id = %config.id, "Successfully added audit target");
        Ok(())
    }

    /// Remove a target from the registry
    pub async fn remove_target(&self, target_id: &str) -> AuditResult<()> {
        info!(target_id = %target_id, "Removing audit target");

        if let Some((_, wrapper)) = self.targets.remove(target_id) {
            // Close the target
            if let Err(e) = wrapper.target.close().await {
                warn!(target_id = %target_id, error = %e, "Error closing target during removal");
            }
            info!(target_id = %target_id, "Successfully removed audit target");
            Ok(())
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Update target configuration
    pub async fn upsert_target(&self, config: AuditTargetConfig) -> AuditResult<()> {
        if self.targets.contains_key(&config.id) {
            // Remove existing target
            self.remove_target(&config.id).await?;
        }
        
        // Add new target
        self.add_target(config).await
    }

    /// Enable a target
    pub fn enable_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.enable();
            info!(target_id = %target_id, "Enabled audit target");
            Ok(())
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Disable a target
    pub fn disable_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.disable();
            info!(target_id = %target_id, "Disabled audit target");
            Ok(())
        } else {
            Err(AuditError::TargetNotFound(target_id.to_string()))
        }
    }

    /// Get target statistics
    pub async fn get_target(&self, target_id: &str) -> Option<TargetStats> {
        if let Some(wrapper) = self.targets.get(target_id) {
            Some(wrapper.get_stats().await)
        } else {
            None
        }
    }

    /// List all targets with their statistics
    pub async fn list_targets(&self) -> Vec<TargetStats> {
        let mut stats = Vec::new();
        for wrapper in self.targets.iter() {
            stats.push(wrapper.value().get_stats().await);
        }
        stats
    }

    /// Get registry-wide statistics
    pub async fn get_stats(&self) -> RegistryStats {
        let mut total_success = 0;
        let mut total_errors = 0;
        let mut enabled_count = 0;

        for wrapper in self.targets.iter() {
            total_success += wrapper.success_count.load(Ordering::Relaxed);
            total_errors += wrapper.error_count.load(Ordering::Relaxed);
            if wrapper.is_enabled() {
                enabled_count += 1;
            }
        }

        RegistryStats {
            total_targets: self.targets.len(),
            enabled_targets: enabled_count,
            total_success,
            total_errors,
            is_paused: self.is_paused(),
        }
    }

    /// Dispatch event to all enabled targets
    pub async fn dispatch(&self, event: Arc<EntityTarget<AuditEntry>>) -> AuditResult<()> {
        if self.is_paused() {
            return Err(AuditError::SystemPaused);
        }

        let enabled_targets: Vec<_> = self.targets
            .iter()
            .filter(|entry| entry.value().is_enabled())
            .map(|entry| entry.value().clone())
            .collect();

        if enabled_targets.is_empty() {
            warn!("No enabled targets available for audit event dispatch");
            return Ok(());
        }

        // Concurrent dispatch to all targets with error isolation
        let mut tasks = Vec::new();
        for wrapper in enabled_targets {
            let event_clone = event.clone();
            let wrapper_clone = wrapper.clone();
            
            let task = tokio::spawn(async move {
                wrapper_clone.save(event_clone).await
            });
            tasks.push(task);
        }

        // Wait for all dispatches to complete
        let mut dispatch_errors = Vec::new();
        for task in tasks {
            match task.await {
                Ok(Ok(())) => {
                    // Success
                }
                Ok(Err(e)) => {
                    dispatch_errors.push(e);
                }
                Err(e) => {
                    error!(error = %e, "Task join error during audit dispatch");
                    dispatch_errors.push(AuditError::RuntimeError(e.to_string()));
                }
            }
        }

        // Log errors but don't fail the entire operation
        if !dispatch_errors.is_empty() {
            warn!(
                error_count = dispatch_errors.len(),
                "Some audit target dispatches failed"
            );
        }

        Ok(())
    }

    /// Lifecycle management methods
    pub async fn start(&self) -> AuditResult<()> {
        info!("Starting audit target registry");
        self.is_paused.store(false, Ordering::Relaxed);

        // Initialize all targets
        for wrapper in self.targets.iter() {
            if let Err(e) = wrapper.target.init().await {
                error!(
                    target_id = %wrapper.config.id,
                    error = %e,
                    "Failed to initialize audit target"
                );
            }
        }

        info!("Audit target registry started");
        Ok(())
    }

    pub fn pause(&self) -> AuditResult<()> {
        info!("Pausing audit target registry");
        self.is_paused.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn resume(&self) -> AuditResult<()> {
        info!("Resuming audit target registry");
        self.is_paused.store(false, Ordering::Relaxed);
        Ok(())
    }

    pub async fn close(&self) -> AuditResult<()> {
        info!("Closing audit target registry");
        self.is_paused.store(true, Ordering::Relaxed);

        // Close all targets
        for wrapper in self.targets.iter() {
            if let Err(e) = wrapper.target.close().await {
                error!(
                    target_id = %wrapper.config.id,
                    error = %e,
                    "Error closing audit target"
                );
            }
        }

        info!("Audit target registry closed");
        Ok(())
    }

    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_factory_create_mock_target() {
        let factory = DefaultAuditTargetFactory::new();
        let config = AuditTargetConfig {
            id: "test-mock".to_string(),
            kind: "mock".to_string(),
            enabled: true,
            args: json!({
                "target_type": "audit_log"
            }),
        };

        let target = factory.create_target(&config).await.unwrap();
        assert_eq!(target.id().to_string(), "test-mock");
    }

    #[tokio::test]
    async fn test_registry_operations() {
        let factory = Arc::new(DefaultAuditTargetFactory::new());
        let registry = TargetRegistry::new(factory);

        let config = AuditTargetConfig {
            id: "test-target".to_string(),
            kind: "mock".to_string(),
            enabled: true,
            args: json!({
                "target_type": "audit_log"
            }),
        };

        // Add target
        registry.add_target(config).await.unwrap();
        assert_eq!(registry.targets.len(), 1);

        // Get target stats
        let stats = registry.get_target("test-target").await.unwrap();
        assert_eq!(stats.target_id, "test-target");
        assert!(stats.enabled);

        // Disable target
        registry.disable_target("test-target").unwrap();
        let stats = registry.get_target("test-target").await.unwrap();
        assert!(!stats.enabled);

        // Remove target
        registry.remove_target("test-target").await.unwrap();
        assert_eq!(registry.targets.len(), 0);
    }

    #[tokio::test]
    async fn test_registry_lifecycle() {
        let factory = Arc::new(DefaultAuditTargetFactory::new());
        let registry = TargetRegistry::new(factory);

        assert!(!registry.is_paused());

        registry.pause().unwrap();
        assert!(registry.is_paused());

        registry.resume().unwrap();
        assert!(!registry.is_paused());
    }
}