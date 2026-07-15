// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use thiserror::Error;

use super::heal::{DiskError, EcstoreError};

/// Custom error type for heal operations
/// This enum defines various error variants that can occur during
/// the execution of heal-related tasks, such as I/O errors, storage errors,
/// configuration errors, and specific errors related to healing operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] EcstoreError),

    #[error("Disk error: {0}")]
    Disk(#[from] DiskError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Heal configuration error: {message}")]
    ConfigurationError { message: String },

    #[error("Other error: {0}")]
    Other(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    IO(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid checkpoint: {0}")]
    InvalidCheckpoint(String),

    #[error("Heal task not found: {task_id}")]
    TaskNotFound { task_id: String },

    #[error("Heal task already exists: {task_id}")]
    TaskAlreadyExists { task_id: String },

    #[error("Invalid heal client token")]
    InvalidClientToken,

    #[error("Heal manager is not running")]
    ManagerNotRunning,

    #[error("Heal task execution failed: {message}")]
    TaskExecutionFailed { message: String },

    #[error("Invalid heal type: {heal_type}")]
    InvalidHealType { heal_type: String },

    #[error("Transient heal skip: {message}")]
    TransientSkip { message: String },

    #[error("Heal task cancelled")]
    TaskCancelled,

    #[error("Heal task timeout")]
    TaskTimeout,

    #[error("Heal event processing failed: {message}")]
    EventProcessingFailed { message: String },

    #[error("Heal progress tracking failed: {message}")]
    ProgressTrackingFailed { message: String },
}

/// A specialized Result type for heal operations
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Create an Other error from any error type
    pub fn other(error: impl std::fmt::Display) -> Self {
        Error::Other(error.to_string())
    }

    /// Create a transient skip error for retryable background heal checks.
    pub fn transient_skip(message: impl Into<String>) -> Self {
        Error::TransientSkip { message: message.into() }
    }

    /// Whether a heal operation can be retried without changing its inputs.
    pub(crate) fn is_recoverable_heal(&self) -> bool {
        match self {
            Error::TaskCancelled => false,
            Error::TaskTimeout | Error::TransientSkip { .. } => true,
            Error::Storage(err) => {
                err.is_quorum_error()
                    || matches!(err, EcstoreError::SlowDown | EcstoreError::OperationCanceled | EcstoreError::Lock(_))
                    || is_recoverable_heal_error_message(&err.to_string())
            }
            Error::Disk(err) => {
                matches!(
                    err,
                    DiskError::ErasureReadQuorum
                        | DiskError::ErasureWriteQuorum
                        | DiskError::Timeout
                        | DiskError::SourceStalled
                        | DiskError::FaultyRemoteDisk
                        | DiskError::FaultyDisk
                ) || is_recoverable_heal_error_message(&err.to_string())
            }
            Error::TaskExecutionFailed { message } | Error::IO(message) | Error::Other(message) => {
                is_recoverable_heal_error_message(message)
            }
            Error::Io(err) => is_recoverable_heal_error_message(&err.to_string()),
            _ => false,
        }
    }
}

fn is_recoverable_heal_error_message(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    [
        "failed to acquire read lock",
        "lock acquisition failed",
        "lock acquisition timeout",
        "remote lock rpc timed out",
        "deadline has elapsed",
        "timed out",
        "transport error",
        "network error",
        "connection refused",
        "operation canceled",
        "quorum not reached",
    ]
    .iter()
    .any(|pattern| error.contains(pattern))
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        std::io::Error::other(err)
    }
}
