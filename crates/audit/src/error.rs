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

/// The 'AuditError' enumeration defines the errors that can occur in the audit system.
#[derive(Error, Debug)]
pub enum AuditError {
    /// Indicates that the audit system has been initialized and cannot be initialized again.
    #[error("Audit system has already been initialized")]
    AlreadyInitialized,

    /// Indicates that the audit system has not been initialized.
    #[error("Audit system is not initialized")]
    NotInitialized,

    /// Wrapped the target error from the 'rustfs-targets' crate.
    #[error("Target error: {0}")]
    Target(#[from] rustfs_targets::TargetError),

    /// Indicates a misconfiguration。
    #[error("Configuration error: {0}")]
    Config(String),
}
