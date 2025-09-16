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

//! Global audit logger for easy integration

use crate::entity::AuditEntry;
use crate::error::{AuditError, AuditResult};
use crate::registry::{AuditTargetFactory, TargetRegistry};
use crate::system::{AuditConfig, AuditStats, AuditSystem};
use once_cell::sync::OnceCell;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Global audit logger instance
static AUDIT_LOGGER: OnceCell<Arc<AuditLogger>> = OnceCell::new();

/// Global audit logger for enterprise-grade audit logging
pub struct AuditLogger {
    /// Core audit system
    system: AuditSystem,
    /// Target registry
    registry: Arc<TargetRegistry>,
}

impl AuditLogger {
    /// Create a new audit logger
    fn new(factory: Arc<dyn AuditTargetFactory>, config: AuditConfig) -> Self {
        let registry = Arc::new(TargetRegistry::new(factory));
        let system = AuditSystem::new(registry.clone(), config);

        Self { system, registry }
    }

    /// Initialize targets from configuration
    async fn initialize_targets(&self, config: &AuditConfig) -> AuditResult<()> {
        for target_config in &config.targets {
            if let Err(e) = self.registry.add_target(target_config.clone()).await {
                error!("Failed to add audit target '{}': {}", target_config.id, e);
                // Continue with other targets instead of failing completely
            }
        }
        Ok(())
    }

    /// Log an audit entry
    pub async fn log(&self, entry: Arc<AuditEntry>) -> AuditResult<()> {
        self.system.log(entry).await
    }

    /// Log an audit entry (convenience method)
    pub async fn log_entry(&self, entry: AuditEntry) -> AuditResult<()> {
        self.log(Arc::new(entry)).await
    }

    /// Start the audit system
    pub async fn start(&self) -> AuditResult<()> {
        self.system.start().await
    }

    /// Pause audit logging
    pub async fn pause(&self) -> AuditResult<()> {
        self.system.pause().await
    }

    /// Resume audit logging
    pub async fn resume(&self) -> AuditResult<()> {
        self.system.resume().await
    }

    /// Close the audit system
    pub async fn close(&self) -> AuditResult<()> {
        self.system.close().await
    }

    /// Get system statistics
    pub async fn get_stats(&self) -> AuditResult<AuditStats> {
        self.system.get_stats().await
    }

    /// Update system configuration
    pub async fn update_config(&self, config: AuditConfig) -> AuditResult<()> {
        self.system.update_config(config).await
    }

    /// Get current configuration
    pub async fn get_config(&self) -> AuditConfig {
        self.system.get_config().await
    }

    /// Get target registry
    pub fn registry(&self) -> &Arc<TargetRegistry> {
        self.system.registry()
    }

    /// Add a new target
    pub async fn add_target(&self, config: crate::registry::AuditTargetConfig) -> AuditResult<()> {
        self.registry.add_target(config).await
    }

    /// Remove a target
    pub async fn remove_target(&self, target_id: &str) -> AuditResult<()> {
        self.registry.remove_target(target_id).await
    }

    /// Enable a target
    pub fn enable_target(&self, target_id: &str) -> AuditResult<()> {
        self.registry.enable_target(target_id)
    }

    /// Disable a target
    pub fn disable_target(&self, target_id: &str) -> AuditResult<()> {
        self.registry.disable_target(target_id)
    }

    /// Get target status
    pub fn get_target_status(&self, target_id: &str) -> Option<crate::registry::TargetStatus> {
        self.registry.get_target_status(target_id)
    }

    /// List all targets
    pub fn list_targets(&self) -> Vec<crate::registry::TargetStatus> {
        self.registry.list_targets()
    }
}

/// Initialize the global audit logger
pub async fn initialize_audit_logger<F>(factory: Arc<F>, config: AuditConfig) -> AuditResult<()>
where
    F: AuditTargetFactory + 'static,
{
    let logger = Arc::new(AuditLogger::new(factory, config.clone()));

    // Initialize targets
    logger.initialize_targets(&config).await?;

    AUDIT_LOGGER
        .set(logger)
        .map_err(|_| AuditError::config("Audit logger already initialized"))?;

    info!("Global audit logger initialized with {} targets", config.targets.len());
    Ok(())
}

/// Get the global audit logger instance
pub fn audit_logger() -> Option<Arc<AuditLogger>> {
    AUDIT_LOGGER.get().cloned()
}

/// Log an audit entry using the global logger
pub async fn log_audit_entry(entry: AuditEntry) -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.log_entry(entry).await,
        None => {
            warn!("Audit logger not initialized, dropping entry");
            Ok(())
        }
    }
}

/// Log an audit entry (Arc version)
pub async fn log_audit(entry: Arc<AuditEntry>) -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.log(entry).await,
        None => {
            warn!("Audit logger not initialized, dropping entry");
            Ok(())
        }
    }
}

/// Helper function to create audit entries for S3 operations
pub fn create_s3_audit_entry(event: &str, api_name: &str, bucket: Option<&str>, object: Option<&str>) -> AuditEntry {
    AuditEntry::for_s3_operation(event, api_name, bucket, object)
}

/// Convenience functions for common S3 operations
pub mod s3_events {
    use super::*;

    /// Create audit entry for GetObject
    pub fn get_object(bucket: &str, object: &str) -> AuditEntry {
        create_s3_audit_entry("s3:GetObject", "GetObject", Some(bucket), Some(object))
    }

    /// Create audit entry for PutObject
    pub fn put_object(bucket: &str, object: &str) -> AuditEntry {
        create_s3_audit_entry("s3:PutObject", "PutObject", Some(bucket), Some(object))
    }

    /// Create audit entry for DeleteObject
    pub fn delete_object(bucket: &str, object: &str) -> AuditEntry {
        create_s3_audit_entry("s3:DeleteObject", "DeleteObject", Some(bucket), Some(object))
    }

    /// Create audit entry for ListBucket
    pub fn list_bucket(bucket: &str) -> AuditEntry {
        create_s3_audit_entry("s3:ListBucket", "ListObjects", Some(bucket), None)
    }

    /// Create audit entry for CreateBucket
    pub fn create_bucket(bucket: &str) -> AuditEntry {
        create_s3_audit_entry("s3:CreateBucket", "CreateBucket", Some(bucket), None)
    }

    /// Create audit entry for DeleteBucket
    pub fn delete_bucket(bucket: &str) -> AuditEntry {
        create_s3_audit_entry("s3:DeleteBucket", "DeleteBucket", Some(bucket), None)
    }

    /// Create audit entry for CompleteMultipartUpload
    pub fn complete_multipart_upload(bucket: &str, object: &str) -> AuditEntry {
        create_s3_audit_entry("s3:CompleteMultipartUpload", "CompleteMultipartUpload", Some(bucket), Some(object))
    }

    /// Create audit entry for AbortMultipartUpload
    pub fn abort_multipart_upload(bucket: &str, object: &str) -> AuditEntry {
        create_s3_audit_entry("s3:AbortMultipartUpload", "AbortMultipartUpload", Some(bucket), Some(object))
    }
}

/// Start the global audit system
pub async fn start_audit_system() -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.start().await,
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Pause the global audit system
pub async fn pause_audit_system() -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.pause().await,
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Resume the global audit system
pub async fn resume_audit_system() -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.resume().await,
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Close the global audit system
pub async fn close_audit_system() -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.close().await,
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Get global audit system statistics
pub async fn get_audit_stats() -> AuditResult<AuditStats> {
    match audit_logger() {
        Some(logger) => logger.get_stats().await,
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Add a target to the global system
pub async fn add_audit_target(config: crate::registry::AuditTargetConfig) -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.add_target(config).await,
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Remove a target from the global system
pub async fn remove_audit_target(target_id: &str) -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.remove_target(target_id).await,
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Enable a target in the global system
pub fn enable_audit_target(target_id: &str) -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.enable_target(target_id),
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Disable a target in the global system
pub fn disable_audit_target(target_id: &str) -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.disable_target(target_id),
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// List all targets in the global system
pub fn list_audit_targets() -> AuditResult<Vec<crate::registry::TargetStatus>> {
    match audit_logger() {
        Some(logger) => Ok(logger.list_targets()),
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Get target status from the global system
pub fn get_audit_target_status(target_id: &str) -> AuditResult<Option<crate::registry::TargetStatus>> {
    match audit_logger() {
        Some(logger) => Ok(logger.get_target_status(target_id)),
        None => Err(AuditError::SystemNotInitialized),
    }
}

/// Check if the global audit system is initialized
pub fn is_audit_system_initialized() -> bool {
    AUDIT_LOGGER.get().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{AuditTarget, AuditTargetConfig, AuditTargetFactory, TargetStatus};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Mock target for testing
    struct MockTarget {
        id: String,
        counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AuditTarget for MockTarget {
        async fn send(&self, _entry: Arc<AuditEntry>) -> crate::error::TargetResult<()> {
            self.counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn start(&self) -> crate::error::TargetResult<()> {
            Ok(())
        }
        async fn pause(&self) -> crate::error::TargetResult<()> {
            Ok(())
        }
        async fn resume(&self) -> crate::error::TargetResult<()> {
            Ok(())
        }
        async fn close(&self) -> crate::error::TargetResult<()> {
            Ok(())
        }

        fn id(&self) -> &str {
            &self.id
        }
        fn target_type(&self) -> &str {
            "mock"
        }
        fn is_enabled(&self) -> bool {
            true
        }

        fn status(&self) -> TargetStatus {
            TargetStatus {
                id: self.id.clone(),
                target_type: "mock".to_string(),
                enabled: true,
                running: true,
                last_error: None,
                last_error_time: None,
                success_count: self.counter.load(Ordering::Relaxed) as u64,
                error_count: 0,
                last_success_time: None,
                queue_size: None,
            }
        }
    }

    struct MockFactory {
        counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AuditTargetFactory for MockFactory {
        async fn create_target(&self, config: &AuditTargetConfig) -> crate::error::TargetResult<Box<dyn AuditTarget>> {
            Ok(Box::new(MockTarget {
                id: config.id.clone(),
                counter: self.counter.clone(),
            }))
        }

        fn validate_config(&self, _config: &AuditTargetConfig) -> crate::error::TargetResult<()> {
            Ok(())
        }

        fn supported_types(&self) -> Vec<&'static str> {
            vec!["mock"]
        }
    }

    #[tokio::test]
    async fn test_s3_event_helpers() {
        let entry = s3_events::get_object("test-bucket", "test-object.txt");
        assert_eq!(entry.event, "s3:GetObject");
        assert_eq!(entry.api.name, "GetObject");
        assert_eq!(entry.api.bucket, Some("test-bucket".to_string()));
        assert_eq!(entry.api.object, Some("test-object.txt".to_string()));

        let entry = s3_events::create_bucket("my-bucket");
        assert_eq!(entry.event, "s3:CreateBucket");
        assert_eq!(entry.api.name, "CreateBucket");
        assert_eq!(entry.api.bucket, Some("my-bucket".to_string()));
        assert_eq!(entry.api.object, None);
    }

    #[tokio::test]
    async fn test_global_functions_uninitialized() {
        // Test functions when not initialized
        assert!(!is_audit_system_initialized());

        let result = start_audit_system().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AuditError::SystemNotInitialized));

        let result = get_audit_stats().await;
        assert!(result.is_err());

        let result = list_audit_targets();
        assert!(result.is_err());
    }
}
