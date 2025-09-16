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
    async fn test_integration_flow_example() {
        // This test demonstrates the typical integration flow as described in the issue
        
        // 1. Load config and register targets on startup
        let config = AuditConfig {
            targets: vec![
                AuditTargetConfig::new("webhook-audit".to_string(), "webhook".to_string())
                    .with_args(serde_json::json!({"endpoint": "http://localhost:8080/audit-webhook"}))
                    .with_enabled(false), // Disabled for testing
            ],
            enabled: true,
            global_batch_size: 10,
            default_queue_size: 1000,
            max_concurrent_targets: 16,
        };

        // 2. Initialize audit system
        let manager = AuditManager::new();
        manager.load_config(&config).await.expect("Should load config");

        // 3. Log collection entry (simulating S3 operation)
        let audit_entry = AuditLogEntry::new()
            .set_version("1.0".to_string())
            .set_deployment_id(Some("rustfs-prod".to_string()))
            .set_event("s3:GetObject".to_string())
            .set_entry_type(Some("S3".to_string()))
            .set_api(
                ApiDetails::new()
                    .set_name(Some("GetObject".to_string()))
                    .set_bucket(Some("user-data".to_string()))
                    .set_object(Some("documents/report.pdf".to_string()))
                    .set_status(Some("OK".to_string()))
                    .set_status_code(Some(200))
                    .set_input_bytes(0)
                    .set_output_bytes(2048576) // 2MB
            )
            .set_remote_host(Some("203.0.113.10".to_string()))
            .set_user_agent(Some("MinIO (linux; amd64) minio-go/v7.0.0".to_string()))
            .set_access_key(Some("AKIAIOSFODNN7EXAMPLE".to_string()));

        let result = manager.log(Arc::new(audit_entry)).await;
        assert!(result.is_ok());

        // Verify target status
        let statuses = manager.registry().get_all_target_statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].id, "webhook-audit");
        assert_eq!(statuses[0].kind, "webhook");
        assert!(!statuses[0].enabled); // Should be disabled as configured

        // Test enabling target
        manager.registry().enable_target("webhook-audit").expect("Should enable");
        let status = manager.registry().get_target_status("webhook-audit").unwrap();
        assert!(status.enabled);

        // Clean shutdown
        manager.shutdown().await.expect("Should shutdown cleanly");
    }
}
