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

pub mod config;
pub mod dispatch;
pub mod entry;
pub mod factory;

// Re-export key types and functions
pub use config::{AuditConfig, AuditTargetConfig, TargetStatus};
pub use dispatch::{AuditError, AuditManager, AuditResult, AuditSystem, TargetRegistry};
pub use entry::{
    audit_logger, disable_audit_logging, enable_audit_logging, initialize, 
    is_audit_logging_enabled, log_audit, log_audit_entry, shutdown_audit_logger, AuditLogger
};
pub use factory::{AuditTarget, AuditTargetFactory, DefaultAuditTargetFactory, TargetResult};

use async_trait::async_trait;
use std::error::Error;

/// General Log Target Trait (kept for backward compatibility)
#[async_trait]
pub trait Target: Send + Sync {
    /// Send a single logizable entry
    async fn send(&self, entry: Box<Self>) -> Result<(), Box<dyn Error + Send>>;

    /// Returns the unique name of the target
    fn name(&self) -> &str;

    /// Close target gracefully, ensuring all buffered logs are processed
    async fn shutdown(&self);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::audit::{AuditLogEntry, ApiDetails};
    use std::sync::Arc;
    use tokio;

    #[tokio::test]
    async fn test_audit_config_serialization() {
        let config = AuditConfig {
            targets: vec![
                AuditTargetConfig::new("test-webhook".to_string(), "webhook".to_string())
                    .with_args(serde_json::json!({"endpoint": "http://localhost:8080/webhook"}))
                    .with_enabled(true),
            ],
            enabled: true,
            global_batch_size: 10,
            default_queue_size: 1000,
            max_concurrent_targets: 16,
        };

        let json = serde_json::to_string(&config).expect("Should serialize");
        let deserialized: AuditConfig = serde_json::from_str(&json).expect("Should deserialize");
        
        assert_eq!(config.targets.len(), deserialized.targets.len());
        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.global_batch_size, deserialized.global_batch_size);
    }

    #[tokio::test]
    async fn test_audit_manager_creation() {
        let manager = AuditManager::new();
        
        // Should have empty registry initially
        let statuses = manager.registry().get_all_target_statuses();
        assert!(statuses.is_empty());
    }

    #[tokio::test]
    async fn test_target_config_validation() {
        let factory = DefaultAuditTargetFactory::new();
        
        // Test valid webhook config
        let valid_webhook = AuditTargetConfig::new("webhook-test".to_string(), "webhook".to_string())
            .with_args(serde_json::json!({"endpoint": "https://example.com/webhook"}));
        
        assert!(factory.validate_config(&valid_webhook).is_ok());
        
        // Test invalid webhook config (missing endpoint)
        let invalid_webhook = AuditTargetConfig::new("webhook-test".to_string(), "webhook".to_string())
            .with_args(serde_json::json!({}));
        
        assert!(factory.validate_config(&invalid_webhook).is_err());
        
        // Test unsupported type
        let unsupported = AuditTargetConfig::new("unsupported".to_string(), "file".to_string());
        assert!(factory.validate_config(&unsupported).is_err());
    }

    #[tokio::test]
    async fn test_audit_log_entry_creation() {
        let entry = AuditLogEntry::new()
            .set_version("1.0".to_string())
            .set_event("s3:GetObject".to_string())
            .set_api(
                ApiDetails::new()
                    .set_name(Some("GetObject".to_string()))
                    .set_bucket(Some("test-bucket".to_string()))
                    .set_object(Some("test-object".to_string()))
                    .set_status(Some("OK".to_string()))
                    .set_status_code(Some(200))
            );

        assert_eq!(entry.version, "1.0");
        assert_eq!(entry.event, "s3:GetObject");
        assert_eq!(entry.api.name, Some("GetObject".to_string()));
        assert_eq!(entry.api.bucket, Some("test-bucket".to_string()));
    }

    #[tokio::test]
    async fn test_target_registry_operations() {
        let factory = Arc::new(DefaultAuditTargetFactory::new());
        let registry = TargetRegistry::new(factory);

        // Test adding a target (this will fail due to invalid config, but tests the flow)
        let invalid_config = AuditTargetConfig::new("test".to_string(), "webhook".to_string());
        let result = registry.add_target(invalid_config).await;
        assert!(result.is_err()); // Should fail due to missing endpoint

        // Test with valid config (but httpbin.org might not be accessible)
        let valid_config = AuditTargetConfig::new("test-webhook".to_string(), "webhook".to_string())
            .with_args(serde_json::json!({"endpoint": "https://httpbin.org/post"}))
            .with_enabled(false); // Disabled so it doesn't actually try to send

        let result = registry.add_target(valid_config).await;
        assert!(result.is_ok());

        // Check that target was added
        let statuses = registry.get_all_target_statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].id, "test-webhook");
        assert_eq!(statuses[0].kind, "webhook");
    }
}
