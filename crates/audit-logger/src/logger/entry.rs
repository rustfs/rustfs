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

use super::config::AuditConfig;
use super::dispatch::{AuditManager, AuditResult};
use crate::entry::audit::AuditLogEntry;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Global audit logger instance
static AUDIT_LOGGER: OnceCell<Arc<AuditLogger>> = OnceCell::new();

/// Global audit logger for thread-safe, high-performance audit logging
pub struct AuditLogger {
    manager: AuditManager,
    enabled: parking_lot::RwLock<bool>,
}

impl AuditLogger {
    /// Create a new audit logger with the given configuration
    fn new(config: Option<AuditConfig>) -> Self {
        let manager = AuditManager::new();
        let enabled = config.as_ref().map(|c| c.enabled).unwrap_or(true);

        let logger = Self {
            manager,
            enabled: parking_lot::RwLock::new(enabled),
        };

        // Load configuration if provided
        if let Some(config) = config {
            let manager_clone = logger.manager.clone(); // Clone the Arc inside
            tokio::spawn(async move {
                if let Err(e) = manager_clone.load_config(&config).await {
                    error!("Failed to load audit configuration: {}", e);
                }
            });
        }

        logger
    }

    /// Initialize the global audit logger with configuration
    pub fn initialize(config: Option<AuditConfig>) -> AuditResult<()> {
        let logger = Arc::new(Self::new(config));
        
        AUDIT_LOGGER.set(logger)
            .map_err(|_| super::dispatch::AuditError::Configuration("Audit logger already initialized".to_string()))?;
        
        info!("Global audit logger initialized");
        Ok(())
    }

    /// Get the global audit logger instance
    pub fn instance() -> Option<Arc<AuditLogger>> {
        AUDIT_LOGGER.get().cloned()
    }

    /// Log an audit entry asynchronously
    pub async fn log(&self, entry: Arc<AuditLogEntry>) -> AuditResult<()> {
        if !*self.enabled.read() {
            debug!("Audit logging disabled, skipping entry");
            return Ok(());
        }

        self.manager.log(entry).await
    }

    /// Enable audit logging
    pub fn enable(&self) {
        *self.enabled.write() = true;
        info!("Audit logging enabled");
    }

    /// Disable audit logging
    pub fn disable(&self) {
        *self.enabled.write() = false;
        info!("Audit logging disabled");
    }

    /// Check if audit logging is enabled
    pub fn is_enabled(&self) -> bool {
        *self.enabled.read()
    }

    /// Get the audit manager for advanced operations
    pub fn manager(&self) -> &AuditManager {
        &self.manager
    }

    /// Reload configuration
    pub async fn reload_config(&self, config: &AuditConfig) -> AuditResult<()> {
        info!("Reloading audit configuration");
        
        // Update enabled state
        *self.enabled.write() = config.enabled;
        
        // Reload target configurations
        self.manager.load_config(config).await?;
        
        info!("Audit configuration reloaded successfully");
        Ok(())
    }

    /// Shutdown the audit logger gracefully
    pub async fn shutdown(&self) -> AuditResult<()> {
        info!("Shutting down audit logger");
        self.manager.shutdown().await
    }
}

/// Initialize the global audit logger
pub fn initialize(config: Option<AuditConfig>) -> AuditResult<()> {
    AuditLogger::initialize(config)
}

/// Get the global audit logger instance
pub fn audit_logger() -> Option<Arc<AuditLogger>> {
    AuditLogger::instance()
}

/// Convenience function to log an audit entry using the global logger
pub async fn log_audit(entry: Arc<AuditLogEntry>) -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.log(entry).await,
        None => {
            warn!("Audit logger not initialized, dropping log entry");
            Ok(())
        }
    }
}

/// Convenience function to create and log an audit entry
pub async fn log_audit_entry(entry: AuditLogEntry) -> AuditResult<()> {
    log_audit(Arc::new(entry)).await
}

/// Enable global audit logging
pub fn enable_audit_logging() {
    if let Some(logger) = audit_logger() {
        logger.enable();
    } else {
        warn!("Audit logger not initialized, cannot enable");
    }
}

/// Disable global audit logging
pub fn disable_audit_logging() {
    if let Some(logger) = audit_logger() {
        logger.disable();
    } else {
        warn!("Audit logger not initialized, cannot disable");
    }
}

/// Check if global audit logging is enabled
pub fn is_audit_logging_enabled() -> bool {
    audit_logger().map(|logger| logger.is_enabled()).unwrap_or(false)
}

/// Shutdown the global audit logger
pub async fn shutdown_audit_logger() -> AuditResult<()> {
    match audit_logger() {
        Some(logger) => logger.shutdown().await,
        None => {
            warn!("Audit logger not initialized, nothing to shutdown");
            Ok(())
        }
    }
}
