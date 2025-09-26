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

/// Result type for audit operations
pub type AuditResult<T> = Result<T, AuditError>;

/// Errors that can occur during audit operations
#[derive(Error, Debug)]
pub enum AuditError {
    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("config not loaded")]
    ConfigNotLoaded,

    #[error("Target error: {0}")]
    Target(#[from] rustfs_targets::TargetError),

    #[error("System not initialized: {0}")]
    NotInitialized(String),

    #[error("System already initialized")]
    AlreadyInitialized,

    #[error("Failed to save configuration: {0}")]
    SaveConfig(String),

    #[error("Failed to load configuration: {0}")]
    LoadConfig(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("Server storage not initialized: {0}")]
    ServerNotInitialized(String),
}
