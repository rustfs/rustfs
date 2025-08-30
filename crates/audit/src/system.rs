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
use rustfs_targets::target::{EntityTarget, Target};
use std::sync::Arc;
use tracing::{error, warn};

/// 'AuditSystem' is responsible for managing and distributing audit logs to multiple targets.
pub struct AuditSystem {
    targets: Vec<Arc<dyn Target<AuditEntry> + Send + Sync>>,
}

impl AuditSystem {
    /// Create a new instance of 'AuditSystem'.
    ///
    /// # Arguments
    ///
    /// * 'targets' - a vector containing all configured audit targets.
    pub fn new(targets: Vec<Arc<dyn Target<AuditEntry> + Send + Sync>>) -> Self {
        Self { targets }
    }

    /// Asynchronously record an audit entry.。
    ///
    /// This method assigns the given audit entry to all configured targets.
    /// It calls the 'save' method for each target.
    ///
    /// # Arguments
    ///
    /// * 'entry' - The audit entry to be recorded, wrapped in 'Arc<EntityTarget<AuditEntry>>'.
    pub async fn log(&self, entry: Arc<EntityTarget<AuditEntry>>) {
        if self.targets.is_empty() {
            warn!("No audit targets configured, audit entry will be dropped.");
            return;
        }

        let mut futures = Vec::new();
        for target in &self.targets {
            let target_clone = target.clone();
            let entry_clone = entry.clone();
            let future = async move {
                if let Err(e) = target_clone.save(entry_clone).await {
                    error!(target_id = %target_clone.id(), error = %e, "Failed to save audit entry to target");
                }
            };
            futures.push(future);
        }

        // Concurrent execution of the 'save' operation for all targets
        futures::future::join_all(futures).await;
    }
}
