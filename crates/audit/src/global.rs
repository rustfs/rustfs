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

use crate::entity::AuditEntry;
use crate::error::AuditError;
use crate::system::AuditSystem;
use once_cell::sync::OnceCell;
use rustfs_targets::target::EntityTarget;
use std::sync::Arc;
use tracing::error;

// Use OnceCell to securely implement global singleton.
static AUDIT_SYSTEM: OnceCell<Arc<AuditSystem>> = OnceCell::new();

/// Initialize the global audit system.
/// This function should be called only once when the app starts.
pub fn initialize(system: AuditSystem) -> Result<(), AuditError> {
    AUDIT_SYSTEM.set(Arc::new(system)).map_err(|_| AuditError::AlreadyInitialized)
}

/// Returns a reference to the global audit system.
/// If the system is not initialized, None is returned.
fn audit_system() -> Option<Arc<AuditSystem>> {
    AUDIT_SYSTEM.get().cloned()
}

/// Global audit logger.
/// This is the recommended way to log audit events throughout the application.
pub struct AuditLogger;

impl AuditLogger {
    /// An audit event is recorded asynchronously.
    /// If the audit system is not initialized, this action will be invalid and an error will be logged.
    pub async fn log(&self, entry: Arc<EntityTarget<AuditEntry>>) {
        if let Some(system) = audit_system() {
            system.log(entry.clone()).await;
        } else {
            // In production, you may not want to print errors every time, consider warning only at startup.
            error!("Audit system not initialized. Audit entry was dropped.");
        }
    }
}

/// Returns an instance of the global AuditLogger.
pub fn audit_logger() -> AuditLogger {
    AuditLogger
}
