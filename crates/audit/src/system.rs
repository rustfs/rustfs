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

/// `AuditSystem` 负责管理和分派审计日志到多个目标。
pub struct AuditSystem {
    targets: Vec<Arc<dyn Target<AuditEntry> + Send + Sync>>,
}

impl AuditSystem {
    /// 创建一个新的 `AuditSystem` 实例。
    ///
    /// # Arguments
    ///
    /// * `targets` - 一个包含所有已配置审计目标的向量。
    pub fn new(targets: Vec<Arc<dyn Target<AuditEntry> + Send + Sync>>) -> Self {
        Self { targets }
    }

    /// 异步记录一个审计条目。
    ///
    /// 此方法会将给定的审计条目分派给所有已配置的目标。
    /// 它会为每个目标调用 `save` 方法。
    ///
    /// # Arguments
    ///
    /// * `entry` - 要记录的审计条目，包装在 `Arc<EntityTarget<AuditEntry>>` 中。
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

        // 并发执行所有目标的 `save` 操作
        futures::future::join_all(futures).await;
    }
}
