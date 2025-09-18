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

//! Error types for the audit system

use thiserror::Error;

/// Main error type for audit operations
#[derive(Error, Debug)]
pub enum AuditError {
    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Target error: {0}")]
    TargetError(#[from] rustfs_targets::TargetError),

    #[error("Initialization error: {0}")]
    InitializationError(String),

    #[error("System not initialized")]
    SystemNotInitialized,

    #[error("Target not found: {0}")]
    TargetNotFound(String),

    #[error("System is paused")]
    SystemPaused,

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Environment variable error: {0}")]
    EnvVarError(String),

    #[error("Unsupported target type: {0}")]
    UnsupportedTargetType(String),

    #[error("Runtime error: {0}")]
    RuntimeError(String),
}

impl AuditError {
    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            AuditError::TargetError(e) => e.is_retryable(),
            AuditError::IoError(_) => true,
            AuditError::RuntimeError(_) => true,
            _ => false,
        }
    }

    /// Check if this error should cause system shutdown
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            AuditError::InitializationError(_) | AuditError::ConfigurationError(_)
        )
    }
}

/// Result type for audit operations
pub type AuditResult<T> = Result<T, AuditError>;

/// Extension trait for rustfs_targets::TargetError
trait TargetErrorExt {
    fn is_retryable(&self) -> bool;
}

impl TargetErrorExt for rustfs_targets::TargetError {
    fn is_retryable(&self) -> bool {
        match self {
            rustfs_targets::TargetError::NetworkError(_) => true,
            rustfs_targets::TargetError::Timeout(_) => true,
            rustfs_targets::TargetError::IoError(_) => true,
            _ => false,
        }
    }
}