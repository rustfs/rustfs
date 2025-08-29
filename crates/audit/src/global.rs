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

// 使用 OnceCell 来安全地实现全局单例。
static AUDIT_SYSTEM: OnceCell<Arc<AuditSystem>> = OnceCell::new();

/// 初始化全局审计系统。
/// 这个函数应该在应用启动时只调用一次。
pub fn initialize(system: AuditSystem) -> Result<(), AuditError> {
    AUDIT_SYSTEM.set(Arc::new(system)).map_err(|_| AuditError::AlreadyInitialized)
}

/// 返回对全局审计系统的引用。
/// 如果系统未初始化，则返回 None。
fn audit_system() -> Option<Arc<AuditSystem>> {
    AUDIT_SYSTEM.get().cloned()
}

/// 全局审计日志记录器。
/// 这是在整个应用程序中记录审计事件的推荐方式。
pub struct AuditLogger;

impl AuditLogger {
    /// 异步记录一条审计事件。
    /// 如果审计系统未初始化，此操作将无效并记录一条错误。
    pub async fn log(&self, entry: Arc<EntityTarget<AuditEntry>>) {
        if let Some(system) = audit_system() {
            system.log(entry.clone()).await;
        } else {
            // 在生产环境中，你可能不希望每次都打印错误，可以考虑只在启动时警告。
            error!("Audit system not initialized. Audit entry was dropped.");
        }
    }
}

/// 返回全局 AuditLogger 的一个实例。
pub fn audit_logger() -> AuditLogger {
    AuditLogger
}
