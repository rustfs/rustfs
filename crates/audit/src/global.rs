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

use crate::{AuditEntry, AuditResult, AuditSystem, system::AuditTargetMetricSnapshot};
use rustfs_config::server_config::Config;
use std::sync::{Arc, OnceLock};
use tracing::{debug, error, trace};

const LOG_COMPONENT_AUDIT: &str = "audit";
const LOG_SUBSYSTEM_GLOBAL: &str = "global";
const EVENT_AUDIT_GLOBAL_SKIPPED: &str = "audit_global_skipped";
const EVENT_AUDIT_ENTRY_DROPPED: &str = "audit_entry_dropped";
const EVENT_AUDIT_DISPATCH_FAILED: &str = "audit_dispatch_failed";

/// Global audit system instance
static AUDIT_SYSTEM: OnceLock<Arc<AuditSystem>> = OnceLock::new();

/// Initialize the global audit system
pub fn init_audit_system() -> Arc<AuditSystem> {
    AUDIT_SYSTEM.get_or_init(|| Arc::new(AuditSystem::new())).clone()
}

/// Get the global audit system instance
pub fn audit_system() -> Option<Arc<AuditSystem>> {
    AUDIT_SYSTEM.get().cloned()
}

/// A helper macro for executing closures if the global audit system is initialized.
/// If not initialized, log a warning and return `Ok(())`.
macro_rules! with_audit_system {
    ($async_closure:expr) => {
        if let Some(system) = audit_system() {
            (async move { $async_closure(system).await }).await
        } else {
            debug!(
                event = EVENT_AUDIT_GLOBAL_SKIPPED,
                component = LOG_COMPONENT_AUDIT,
                subsystem = LOG_SUBSYSTEM_GLOBAL,
                reason = "system_not_initialized",
                "Skipped audit system operation"
            );
            Ok(())
        }
    };
}

/// Start the global audit system with configuration
pub async fn start_audit_system(config: Config) -> AuditResult<()> {
    let system = init_audit_system();
    system.start(config).await
}

/// Stop the global audit system
pub async fn stop_audit_system() -> AuditResult<()> {
    with_audit_system!(|system: Arc<AuditSystem>| async move { system.close().await })
}

/// Pause the global audit system
pub async fn pause_audit_system() -> AuditResult<()> {
    with_audit_system!(|system: Arc<AuditSystem>| async move { system.pause().await })
}

/// Resume the global audit system
pub async fn resume_audit_system() -> AuditResult<()> {
    with_audit_system!(|system: Arc<AuditSystem>| async move { system.resume().await })
}

/// Dispatch an audit log entry to all targets
pub async fn dispatch_audit_log(entry: Arc<AuditEntry>) -> AuditResult<()> {
    if let Some(system) = audit_system() {
        if system.is_running().await {
            system.dispatch(entry).await
        } else {
            trace!(
                event = EVENT_AUDIT_ENTRY_DROPPED,
                component = LOG_COMPONENT_AUDIT,
                subsystem = LOG_SUBSYSTEM_GLOBAL,
                reason = "system_not_running",
                "Dropped audit entry"
            );
            Ok(())
        }
    } else {
        debug!(
            event = EVENT_AUDIT_ENTRY_DROPPED,
            component = LOG_COMPONENT_AUDIT,
            subsystem = LOG_SUBSYSTEM_GLOBAL,
            reason = "system_not_initialized",
            "Dropped audit entry"
        );
        Ok(())
    }
}

/// Reload the global audit system configuration
pub async fn reload_audit_config(config: Config) -> AuditResult<()> {
    with_audit_system!(|system: Arc<AuditSystem>| async move { system.reload_config(config).await })
}

/// Returns per-target audit delivery metrics for Prometheus collection.
pub async fn audit_target_metrics() -> Vec<AuditTargetMetricSnapshot> {
    if let Some(system) = audit_system() {
        system.snapshot_target_metrics().await
    } else {
        Vec::new()
    }
}

/// Check if the global audit system is running
pub async fn is_audit_system_running() -> bool {
    if let Some(system) = audit_system() {
        system.is_running().await
    } else {
        false
    }
}

/// AuditLogger singleton for easy access
pub struct AuditLogger;

impl AuditLogger {
    /// Log an audit entry
    pub async fn log(entry: AuditEntry) {
        if let Err(e) = dispatch_audit_log(Arc::new(entry)).await {
            error!(
                event = EVENT_AUDIT_DISPATCH_FAILED,
                component = LOG_COMPONENT_AUDIT,
                subsystem = LOG_SUBSYSTEM_GLOBAL,
                error = %e,
                "Failed to dispatch audit entry"
            );
        }
    }

    /// Check if audit logging is enabled
    pub async fn is_enabled() -> bool {
        is_audit_system_running().await
    }

    /// Get singleton instance
    pub fn instance() -> &'static Self {
        static INSTANCE: AuditLogger = AuditLogger;
        &INSTANCE
    }
}
