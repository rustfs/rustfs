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

//! Target registry and factory for audit system
use crate::entity::AuditEntry;
use crate::error::{AuditError, AuditResult, TargetError, TargetResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use rustfs_targets::{EventName, Target, TargetLog};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Configuration for an audit target
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditTargetConfig {
    /// Unique target identifier
    pub id: String,
    /// Target type (mqtt, webhook, etc.)
    #[serde(rename = "kind")]
    pub target_type: String,
    /// Whether the target is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Target-specific configuration arguments
    pub args: serde_json::Value,
}

/// Target status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetStatus {
    /// Target ID
    pub id: String,
    /// Target type
    pub target_type: String,
    /// Whether target is enabled
    pub enabled: bool,
    /// Whether target is currently running
    pub running: bool,
    /// Last error message
    pub last_error: Option<String>,
    /// Last error timestamp
    pub last_error_time: Option<DateTime<Utc>>,
    /// Number of successful dispatches
    pub success_count: u64,
    /// Number of failed dispatches
    pub error_count: u64,
    /// Last successful dispatch
    pub last_success_time: Option<DateTime<Utc>>,
    /// Current queue size (if applicable)
    pub queue_size: Option<usize>,
}

/// Trait for audit target factories
#[async_trait]
pub trait AuditTargetFactory: Send + Sync {
    /// Create a new target from configuration
    async fn create_target(&self, config: &AuditTargetConfig) -> TargetResult<Box<dyn AuditTarget>>;

    /// Validate target configuration
    fn validate_config(&self, config: &AuditTargetConfig) -> TargetResult<()>;

    /// Get supported target types
    fn supported_types(&self) -> Vec<&'static str>;
}

/// Trait for audit targets
#[async_trait]
pub trait AuditTarget: Send + Sync {
    /// Send audit entry to target
    async fn send(&self, entry: Arc<AuditEntry>) -> TargetResult<()>;

    /// Start the target
    async fn start(&self) -> TargetResult<()>;

    /// Pause the target (stop accepting new entries)
    async fn pause(&self) -> TargetResult<()>;

    /// Resume the target
    async fn resume(&self) -> TargetResult<()>;

    /// Stop the target gracefully
    async fn close(&self) -> TargetResult<()>;

    /// Get target ID
    fn id(&self) -> &str;

    /// Get target type
    fn target_type(&self) -> &str;

    /// Check if target is enabled
    fn is_enabled(&self) -> bool;

    /// Get current status
    fn status(&self) -> TargetStatus;
}

/// Wrapper for a target with statistics and lifecycle management
struct TargetWrapper {
    target: Box<dyn AuditTarget>,
    enabled: Arc<RwLock<bool>>,
    running: Arc<RwLock<bool>>,
    success_count: AtomicU64,
    error_count: AtomicU64,
    last_error: Arc<RwLock<Option<String>>>,
    last_error_time: Arc<RwLock<Option<DateTime<Utc>>>>,
    last_success_time: Arc<RwLock<Option<DateTime<Utc>>>>,
    config: AuditTargetConfig,
}

impl TargetWrapper {
    fn new(target: Box<dyn AuditTarget>, config: AuditTargetConfig) -> Self {
        Self {
            target,
            enabled: Arc::new(RwLock::new(config.enabled)),
            running: Arc::new(RwLock::new(false)),
            success_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_error: Arc::new(RwLock::new(None)),
            last_error_time: Arc::new(RwLock::new(None)),
            last_success_time: Arc::new(RwLock::new(None)),
            config,
        }
    }

    async fn send_with_retry(&self, entry: Arc<AuditEntry>) -> TargetResult<()> {
        if !*self.enabled.read() {
            return Err(TargetError::Disabled);
        }

        if !*self.running.read() {
            return Err(TargetError::ShuttingDown);
        }

        match self.target.send(entry).await {
            Ok(()) => {
                self.success_count.fetch_add(1, Ordering::Relaxed);
                *self.last_success_time.write() = Some(Utc::now());
                Ok(())
            }
            Err(e) => {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                *self.last_error.write() = Some(e.to_string());
                *self.last_error_time.write() = Some(Utc::now());
                Err(e)
            }
        }
    }

    fn get_status(&self) -> TargetStatus {
        TargetStatus {
            id: self.target.id().to_string(),
            target_type: self.target.target_type().to_string(),
            enabled: *self.enabled.read(),
            running: *self.running.read(),
            last_error: self.last_error.read().clone(),
            last_error_time: *self.last_error_time.read(),
            success_count: self.success_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            last_success_time: *self.last_success_time.read(),
            queue_size: None, // TODO: implement queue size tracking
        }
    }

    async fn start(&self) -> TargetResult<()> {
        if *self.enabled.read() {
            self.target.start().await?;
            *self.running.write() = true;
            info!("Target {} started", self.target.id());
        }
        Ok(())
    }

    async fn pause(&self) -> TargetResult<()> {
        self.target.pause().await?;
        *self.running.write() = false;
        info!("Target {} paused", self.target.id());
        Ok(())
    }

    async fn resume(&self) -> TargetResult<()> {
        if *self.enabled.read() {
            self.target.resume().await?;
            *self.running.write() = true;
            info!("Target {} resumed", self.target.id());
        }
        Ok(())
    }

    async fn close(&self) -> TargetResult<()> {
        self.target.close().await?;
        *self.running.write() = false;
        info!("Target {} closed", self.target.id());
        Ok(())
    }

    fn enable(&self) {
        *self.enabled.write() = true;
    }

    fn disable(&self) {
        *self.enabled.write() = false;
    }
}

/// Registry for managing audit targets
pub struct TargetRegistry {
    targets: DashMap<String, Arc<TargetWrapper>>,
    factory: Arc<dyn AuditTargetFactory>,
}

impl TargetRegistry {
    /// Create a new target registry
    pub fn new(factory: Arc<dyn AuditTargetFactory>) -> Self {
        Self {
            targets: DashMap::new(),
            factory,
        }
    }

    /// Add a new target
    pub async fn add_target(&self, config: AuditTargetConfig) -> AuditResult<()> {
        let target_id = config.id.clone();

        if self.targets.contains_key(&target_id) {
            return Err(AuditError::config(format!("Target '{}' already exists", target_id)));
        }

        // Validate configuration
        self.factory.validate_config(&config).map_err(AuditError::Target)?;

        // Create target
        let target = self.factory.create_target(&config).await.map_err(AuditError::Target)?;

        let wrapper = Arc::new(TargetWrapper::new(target, config));

        // Start target if enabled
        if wrapper.config.enabled {
            wrapper.start().await.map_err(AuditError::Target)?;
        }

        self.targets.insert(target_id.clone(), wrapper);
        info!("Added target '{}'", target_id);
        Ok(())
    }

    /// Remove a target
    pub async fn remove_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some((_, wrapper)) = self.targets.remove(target_id) {
            wrapper.close().await.map_err(AuditError::Target)?;
            info!("Removed target '{}'", target_id);
            Ok(())
        } else {
            Err(AuditError::TargetNotFound {
                id: target_id.to_string(),
            })
        }
    }

    /// Enable a target
    pub fn enable_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.enable();
            info!("Enabled target '{}'", target_id);
            Ok(())
        } else {
            Err(AuditError::TargetNotFound {
                id: target_id.to_string(),
            })
        }
    }

    /// Disable a target
    pub fn disable_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.disable();
            info!("Disabled target '{}'", target_id);
            Ok(())
        } else {
            Err(AuditError::TargetNotFound {
                id: target_id.to_string(),
            })
        }
    }

    /// Start a target
    pub async fn start_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.start().await.map_err(AuditError::Target)
        } else {
            Err(AuditError::TargetNotFound {
                id: target_id.to_string(),
            })
        }
    }

    /// Pause a target
    pub async fn pause_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.pause().await.map_err(AuditError::Target)
        } else {
            Err(AuditError::TargetNotFound {
                id: target_id.to_string(),
            })
        }
    }

    /// Resume a target
    pub async fn resume_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.resume().await.map_err(AuditError::Target)
        } else {
            Err(AuditError::TargetNotFound {
                id: target_id.to_string(),
            })
        }
    }

    /// Close a target
    pub async fn close_target(&self, target_id: &str) -> AuditResult<()> {
        if let Some(wrapper) = self.targets.get(target_id) {
            wrapper.close().await.map_err(AuditError::Target)
        } else {
            Err(AuditError::TargetNotFound {
                id: target_id.to_string(),
            })
        }
    }

    /// Get target status
    pub fn get_target_status(&self, target_id: &str) -> Option<TargetStatus> {
        self.targets.get(target_id).map(|wrapper| wrapper.get_status())
    }

    /// List all targets
    pub fn list_targets(&self) -> Vec<TargetStatus> {
        self.targets.iter().map(|entry| entry.value().get_status()).collect()
    }

    /// Dispatch audit entry to all enabled targets
    pub async fn dispatch(&self, entry: Arc<AuditEntry>) -> Vec<TargetResult<()>> {
        let mut results = Vec::new();

        for target_wrapper in self.targets.iter() {
            let wrapper = target_wrapper.value();
            if *wrapper.enabled.read() && *wrapper.running.read() {
                let result = wrapper.send_with_retry(entry.clone()).await;
                if let Err(ref e) = result {
                    warn!("Target '{}' failed: {}", wrapper.target.id(), e);
                }
                results.push(result);
            }
        }

        if results.is_empty() {
            warn!("No enabled targets available for audit dispatch");
        }

        results
    }

    /// Close all targets
    pub async fn close_all(&self) -> AuditResult<()> {
        let mut errors = Vec::new();

        for target_wrapper in self.targets.iter() {
            if let Err(e) = target_wrapper.value().close().await {
                errors.push(e);
            }
        }

        self.targets.clear();

        if errors.is_empty() {
            info!("All targets closed successfully");
            Ok(())
        } else {
            Err(AuditError::Shutdown {
                message: format!("Some targets failed to close: {:?}", errors),
            })
        }
    }

    /// Get count of enabled targets
    pub fn enabled_target_count(&self) -> usize {
        self.targets.iter().filter(|entry| *entry.value().enabled.read()).count()
    }

    /// Get count of running targets
    pub fn running_target_count(&self) -> usize {
        self.targets.iter().filter(|entry| *entry.value().running.read()).count()
    }
}

/// Default implementations
fn default_enabled() -> bool {
    true
}

impl Default for AuditTargetConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            target_type: String::new(),
            enabled: true,
            args: serde_json::Value::Object(serde_json::Map::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;

    // Mock target for testing
    struct MockTarget {
        id: String,
        target_type: String,
        enabled: bool,
        fail_on_send: bool,
    }

    #[async_trait]
    impl AuditTarget for MockTarget {
        async fn send(&self, _entry: Arc<AuditEntry>) -> TargetResult<()> {
            if self.fail_on_send {
                Err(TargetError::operation_failed("Mock failure"))
            } else {
                Ok(())
            }
        }

        async fn start(&self) -> TargetResult<()> {
            Ok(())
        }
        async fn pause(&self) -> TargetResult<()> {
            Ok(())
        }
        async fn resume(&self) -> TargetResult<()> {
            Ok(())
        }
        async fn close(&self) -> TargetResult<()> {
            Ok(())
        }

        fn id(&self) -> &str {
            &self.id
        }
        fn target_type(&self) -> &str {
            &self.target_type
        }
        fn is_enabled(&self) -> bool {
            self.enabled
        }

        fn status(&self) -> TargetStatus {
            TargetStatus {
                id: self.id.clone(),
                target_type: self.target_type.clone(),
                enabled: self.enabled,
                running: true,
                last_error: None,
                last_error_time: None,
                success_count: 0,
                error_count: 0,
                last_success_time: None,
                queue_size: None,
            }
        }
    }

    struct MockFactory;

    #[async_trait]
    impl AuditTargetFactory for MockFactory {
        async fn create_target(&self, config: &AuditTargetConfig) -> TargetResult<Box<dyn AuditTarget>> {
            Ok(Box::new(MockTarget {
                id: config.id.clone(),
                target_type: config.target_type.clone(),
                enabled: config.enabled,
                fail_on_send: false,
            }))
        }

        fn validate_config(&self, config: &AuditTargetConfig) -> TargetResult<()> {
            if config.id.is_empty() {
                return Err(TargetError::invalid_config("ID cannot be empty"));
            }
            Ok(())
        }

        fn supported_types(&self) -> Vec<&'static str> {
            vec!["mock"]
        }
    }

    #[tokio::test]
    async fn test_target_registry_operations() {
        let factory = Arc::new(MockFactory);
        let registry = TargetRegistry::new(factory);

        // Add target
        let config = AuditTargetConfig {
            id: "test-target".to_string(),
            target_type: "mock".to_string(),
            enabled: true,
            args: serde_json::Value::Object(serde_json::Map::new()),
        };

        registry.add_target(config).await.unwrap();

        // Check target exists
        let status = registry.get_target_status("test-target").unwrap();
        assert_eq!(status.id, "test-target");
        assert_eq!(status.target_type, "mock");
        assert!(status.enabled);

        // List targets
        let targets = registry.list_targets();
        assert_eq!(targets.len(), 1);

        // Disable target
        registry.disable_target("test-target").unwrap();
        let status = registry.get_target_status("test-target").unwrap();
        assert!(!status.enabled);

        // Remove target
        registry.remove_target("test-target").await.unwrap();
        assert!(registry.get_target_status("test-target").is_none());
    }

    #[tokio::test]
    async fn test_target_dispatch() {
        let factory = Arc::new(MockFactory);
        let registry = TargetRegistry::new(factory);

        let config = AuditTargetConfig {
            id: "test-target".to_string(),
            target_type: "mock".to_string(),
            enabled: true,
            args: serde_json::Value::Object(serde_json::Map::new()),
        };

        registry.add_target(config).await.unwrap();

        let entry = Arc::new(AuditEntry::for_s3_operation(
            "s3:GetObject",
            "GetObject",
            Some("test-bucket"),
            Some("test-object"),
        ));

        let results = registry.dispatch(entry).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
    }
}
