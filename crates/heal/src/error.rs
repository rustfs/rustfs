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

    #[error("Other error: {0}")]
    Other(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid checkpoint: {0}")]
    InvalidCheckpoint(String),

    #[error("Heal task not found: {task_id}")]
    TaskNotFound { task_id: String },

    #[error("Invalid heal client token")]
    InvalidClientToken,

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
            Error::TaskCancelled | Error::TaskTimeout => false,
            Error::TransientSkip { .. } => true,
            Error::Storage(err) => {
                err.is_quorum_error()
                    || matches!(
                        err,
                        EcstoreError::DiskNotFound
                            | EcstoreError::VolumeNotFound
                            | EcstoreError::SlowDown
                            | EcstoreError::OperationCanceled
                            | EcstoreError::Lock(_)
                    )
                    || is_recoverable_heal_error_message(&err.to_string())
            }
            Error::Disk(err) => {
                matches!(
                    err,
                    DiskError::DiskNotFound
                        | DiskError::ErasureReadQuorum
                        | DiskError::ErasureWriteQuorum
                        | DiskError::Timeout
                        | DiskError::SourceStalled
                        | DiskError::FaultyRemoteDisk
                        | DiskError::FaultyDisk
                ) || is_recoverable_heal_error_message(&err.to_string())
            }
            Error::TaskExecutionFailed { message } | Error::Other(message) => is_recoverable_heal_error_message(message),
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
        "heal rename incomplete",
    ]
    .iter()
    .any(|pattern| error.contains(pattern))
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        std::io::Error::other(err)
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::heal::{DiskError, EcstoreError};

    #[test]
    fn incomplete_target_rename_is_recoverable() {
        let task_error = Error::TaskExecutionFailed {
            message: "heal rename incomplete: 1 of 2 targets committed".to_string(),
        };
        let storage_error = Error::Storage(EcstoreError::Io(std::io::Error::other(
            "heal rename incomplete: 1 of 2 targets committed",
        )));

        assert!(task_error.is_recoverable_heal());
        assert!(storage_error.is_recoverable_heal());
    }

    #[test]
    fn offline_disk_errors_are_recoverable() {
        assert!(Error::Disk(DiskError::DiskNotFound).is_recoverable_heal());
        assert!(Error::Storage(EcstoreError::DiskNotFound).is_recoverable_heal());
        assert!(Error::Storage(EcstoreError::VolumeNotFound).is_recoverable_heal());
    }

    #[test]
    fn task_timeout_is_terminal() {
        assert!(!Error::TaskTimeout.is_recoverable_heal());
    }
}
