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

use thiserror::Error;

/// `AuditError` 枚举定义了审计系统中可能发生的错误。
#[derive(Error, Debug)]
pub enum AuditError {
    /// 表示审计系统已经初始化，无法再次初始化。
    #[error("Audit system has already been initialized")]
    AlreadyInitialized,

    /// 表示审计系统尚未初始化。
    #[error("Audit system is not initialized")]
    NotInitialized,

    /// 包装了来自 `rustfs-targets` crate 的目标错误。
    #[error("Target error: {0}")]
    Target(#[from] rustfs_targets::TargetError),

    /// 表示配置错误。
    #[error("Configuration error: {0}")]
    Config(String),
}
