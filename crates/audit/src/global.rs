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

//! Global audit logger singleton for easy integration

use crate::config::{load_config_from_env, validate_config, AuditConfig};
use crate::entity::AuditEntry;
use crate::error::{AuditError, AuditResult};
use crate::registry::{DefaultAuditTargetFactory, TargetRegistry};
use crate::system::AuditSystem;
use once_cell::sync::OnceCell;
use rustfs_targets::target::EntityTarget;
use rustfs_targets::EventName;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// Global audit logger instance
static AUDIT_LOGGER: OnceCell<Arc<AuditLogger>> = OnceCell::new();

/// Global audit logger for easy integration throughout RustFS
pub struct AuditLogger {
    system: RwLock<Option<AuditSystem>>,
}

impl AuditLogger {
    /// Create a new audit logger
    fn new() -> Self {
        Self {
            system: RwLock::new(None),
        }
    }

    /// Initialize the audit system with configuration
    pub async fn initialize(&self, config: Option<AuditConfig>) -> AuditResult<()> {
        let mut system_guard = self.system.write().await;
        
        if system_guard.is_some() {
            warn!("Audit logger is already initialized");
            return Ok(());
        }

        // Load configuration
        let config = match config {
            Some(cfg) => cfg,
            None => load_config_from_env()?,
        };

        // Validate configuration
        validate_config(&config)?;

        info!(
            enabled = config.enabled,
            target_count = config.targets.len(),
            "Initializing audit logger"
        );

        if !config.enabled {
            info!("Audit logging is disabled");
            return Ok(());
        }

        // Create factory and registry
        let factory = Arc::new(DefaultAuditTargetFactory::new());
        let registry = Arc::new(TargetRegistry::new(factory));

        // Add configured targets
        for target_config in config.targets {
            if let Err(e) = registry.add_target(target_config.clone()).await {
                error!(
                    target_id = %target_config.id,
                    error = %e,
                    "Failed to add audit target"
                );
            }
        }

        // Create and start audit system
        let mut system = AuditSystem::new(registry, config.performance);
        system.start().await?;

        *system_guard = Some(system);
        info!("Audit logger initialized successfully");
        Ok(())
    }

    /// Log an audit entry
    pub async fn log(&self, entry: Arc<AuditEntry>) -> AuditResult<()> {
        let system_guard = self.system.read().await;
        
        if let Some(ref system) = *system_guard {
            // Create EntityTarget from AuditEntry
            let entity_target = Arc::new(self.create_entity_target(entry)?);
            system.log(entity_target).await
        } else {
            error!("Audit logger is not initialized - dropping audit entry");
            Err(AuditError::SystemNotInitialized)
        }
    }

    /// Create EntityTarget from AuditEntry
    fn create_entity_target(&self, entry: Arc<AuditEntry>) -> AuditResult<EntityTarget<AuditEntry>> {
        // Extract event name from the audit entry
        let event_name = self.parse_event_name(&entry.event)?;
        
        // Extract bucket and object names
        let bucket_name = entry.api.bucket.clone().unwrap_or_default();
        let object_name = entry.api.object.clone().unwrap_or_default();

        Ok(EntityTarget {
            object_name,
            bucket_name,
            event_name,
            data: (*entry).clone(),
        })
    }

    /// Parse event name from string
    fn parse_event_name(&self, event: &str) -> AuditResult<EventName> {
        match event {
            "s3:GetObject" => Ok(EventName::ObjectAccessedGet),
            "s3:PutObject" => Ok(EventName::ObjectCreatedPut),
            "s3:DeleteObject" => Ok(EventName::ObjectRemovedDelete),
            "s3:ListObjects" => Ok(EventName::BucketAccessedList),
            "s3:CreateBucket" => Ok(EventName::BucketCreated),
            "s3:DeleteBucket" => Ok(EventName::BucketRemoved),
            "s3:CopyObject" => Ok(EventName::ObjectCreatedCopy),
            "s3:CompleteMultipartUpload" => Ok(EventName::ObjectCreatedCompleteMultipartUpload),
            _ => {
                warn!(event = %event, "Unknown S3 event type, using generic event");
                Ok(EventName::ObjectAccessedAll) // Fallback to generic event
            }
        }
    }

    /// Check if the audit logger is initialized
    pub async fn is_initialized(&self) -> bool {
        self.system.read().await.is_some()
    }

    /// Shutdown the audit logger
    pub async fn shutdown(&self) -> AuditResult<()> {
        let mut system_guard = self.system.write().await;
        
        if let Some(mut system) = system_guard.take() {
            info!("Shutting down audit logger");
            system.stop().await?;
            info!("Audit logger shutdown complete");
        }
        
        Ok(())
    }

    /// Get access to the underlying registry for management operations
    pub async fn registry(&self) -> Option<Arc<TargetRegistry>> {
        let system_guard = self.system.read().await;
        system_guard.as_ref().map(|s| s.registry().clone())
    }

    /// Pause audit processing
    pub async fn pause(&self) -> AuditResult<()> {
        let system_guard = self.system.read().await;
        if let Some(ref system) = *system_guard {
            system.pause()
        } else {
            Err(AuditError::SystemNotInitialized)
        }
    }

    /// Resume audit processing
    pub async fn resume(&self) -> AuditResult<()> {
        let system_guard = self.system.read().await;
        if let Some(ref system) = *system_guard {
            system.resume()
        } else {
            Err(AuditError::SystemNotInitialized)
        }
    }
}

/// Initialize the global audit logger
pub async fn initialize_audit_logger(config: Option<AuditConfig>) -> AuditResult<()> {
    let logger = AUDIT_LOGGER.get_or_init(|| Arc::new(AuditLogger::new()));
    logger.initialize(config).await
}

/// Get the global audit logger instance
pub fn audit_logger() -> Option<Arc<AuditLogger>> {
    AUDIT_LOGGER.get().cloned()
}

/// Convenience function to log an audit entry
pub async fn log_audit_entry(entry: Arc<AuditEntry>) -> AuditResult<()> {
    if let Some(logger) = audit_logger() {
        logger.log(entry).await
    } else {
        error!("Audit logger not initialized - dropping audit entry");
        Err(AuditError::SystemNotInitialized)
    }
}

/// Shutdown the global audit logger
pub async fn shutdown_audit_logger() -> AuditResult<()> {
    if let Some(logger) = audit_logger() {
        logger.shutdown().await
    } else {
        Ok(()) // Already shutdown or never initialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::s3_events;

    #[tokio::test]
    async fn test_audit_logger_lifecycle() {
        // Initialize with default config
        initialize_audit_logger(None).await.unwrap();
        
        let logger = audit_logger().expect("Logger should be initialized");
        assert!(logger.is_initialized().await);
        
        // Shutdown
        shutdown_audit_logger().await.unwrap();
    }

    #[tokio::test]
    async fn test_audit_logging() {
        // Initialize audit logger
        initialize_audit_logger(None).await.unwrap();
        
        // Create audit entry
        let entry = s3_events::get_object("test-bucket", "test-key");
        let entry_arc = Arc::new(entry);
        
        // Log entry (should not fail even with no targets configured)
        let result = log_audit_entry(entry_arc).await;
        
        // With no targets configured, this might return an error or succeed
        // depending on the configuration, but it shouldn't panic
        match result {
            Ok(()) => {
                // Success case
            }
            Err(e) => {
                // Expected when no targets are configured
                println!("Expected error with no targets: {}", e);
            }
        }
        
        shutdown_audit_logger().await.unwrap();
    }

    #[tokio::test]
    async fn test_event_name_parsing() {
        let logger = AuditLogger::new();
        
        assert!(matches!(
            logger.parse_event_name("s3:GetObject").unwrap(),
            EventName::ObjectAccessedGet
        ));
        
        assert!(matches!(
            logger.parse_event_name("s3:PutObject").unwrap(),
            EventName::ObjectCreatedPut
        ));
        
        assert!(matches!(
            logger.parse_event_name("s3:DeleteObject").unwrap(),
            EventName::ObjectRemovedDelete
        ));
        
        // Unknown events should fallback to generic
        assert!(matches!(
            logger.parse_event_name("unknown:event").unwrap(),
            EventName::ObjectAccessedAll
        ));
    }
}