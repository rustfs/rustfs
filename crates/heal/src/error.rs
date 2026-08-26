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
    ///
    /// Typed-first (backlog#1845): the primary classification reads error
    /// variants and typed helpers (`is_quorum_error`, `LockError::is_fatal`);
    /// the substring fallback in [`is_recoverable_heal_error_message`] only
    /// catches errors whose typed identity was destroyed upstream.
    pub(crate) fn is_recoverable_heal(&self) -> bool {
        match self {
            Error::TaskCancelled | Error::TaskTimeout => false,
            Error::TransientSkip { .. } => true,
            // Lock failures classify by LockError's own taxonomy: only the
            // fatal variants (ResourceNotFound / PermissionDenied /
            // Configuration) are terminal - retrying cannot fix them - while
            // contention and transport variants (Timeout, Network, Internal,
            // AlreadyLocked, QuorumNotReached, InsufficientNodes, ...) stay
            // recoverable, as the previous blanket `Lock(_) => true` treated
            // them.
            Error::Storage(EcstoreError::Lock(lock_err)) => !lock_err.is_fatal(),
            Error::Storage(err) => {
                err.is_quorum_error()
                    || matches!(
                        err,
                        EcstoreError::DiskNotFound
                            | EcstoreError::VolumeNotFound
                            | EcstoreError::SlowDown
                            | EcstoreError::OperationCanceled
                            | EcstoreError::RemoteClientUnavailable(_)
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
                        | DiskError::RemoteClientUnavailable(_)
                ) || is_recoverable_heal_error_message(&err.to_string())
            }
            Error::TaskExecutionFailed { message } | Error::Other(message) => is_recoverable_heal_error_message(message),
            Error::Io(err) => is_recoverable_heal_error_message(&err.to_string()),
            _ => false,
        }
    }
}

/// Documented substring fallback for errors that reach heal with their typed
/// identity destroyed (stringified through `TaskExecutionFailed`/`Other`, or
/// boxed into `Io`). Every needle is annotated with the producer that emits
/// it; when a producer becomes typed end-to-end, delete its needle here
/// (backlog#1845 - this list only shrinks).
fn is_recoverable_heal_error_message(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    [
        // set_disk/ops/locking.rs ns_loc lock failures rendered into messages.
        "failed to acquire read lock",
        // ecstore cluster/rpc/remote_locker.rs "Lock acquisition failed on
        // remote server" and lock/src local lock responses.
        "lock acquisition failed",
        // lock/src/distributed_lock.rs + client/local.rs LockResponse::failure.
        "lock acquisition timeout",
        // remote_locker.rs RPC deadline wrapper.
        "remote lock rpc timed out",
        // tokio/tonic deadline rendering, reaches heal via stringified RPC errors.
        "deadline has elapsed",
        // generic io/tonic timeout rendering.
        "timed out",
        // tonic transport failure rendering.
        "transport error",
        // LockError::Network display prefix.
        "network error",
        // io::Error ConnectionRefused rendering.
        "connection refused",
        // StorageError::OperationCanceled rendered through task messages.
        "operation canceled",
        // LockError::QuorumNotReached display, when stringified before typing.
        "quorum not reached",
        // set_disk/ops/heal.rs HEAL_RENAME_INCOMPLETE - the one needle with no
        // typed variant yet; the producer formats it into a plain message.
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

    #[test]
    fn lock_contention_and_transport_variants_stay_recoverable() {
        use rustfs_lock::LockError;
        for lock_err in [
            LockError::Timeout {
                resource: "bucket/object".to_string(),
                timeout: std::time::Duration::from_secs(5),
            },
            LockError::Network {
                message: "peer unreachable".to_string(),
                source: Box::new(std::io::Error::other("reset")),
            },
            LockError::Internal {
                message: "channel busy".to_string(),
            },
            LockError::AlreadyLocked {
                resource: "bucket/object".to_string(),
                owner: "node-2".to_string(),
            },
            LockError::QuorumNotReached {
                required: 3,
                achieved: 1,
            },
            LockError::InsufficientNodes {
                required: 3,
                available: 1,
            },
        ] {
            assert!(
                Error::Storage(EcstoreError::Lock(lock_err)).is_recoverable_heal(),
                "contention/transport lock failures must stay retryable"
            );
        }
    }

    #[test]
    fn fatal_lock_variants_are_terminal() {
        use rustfs_lock::LockError;
        for lock_err in [
            LockError::ResourceNotFound {
                resource: "bucket/object".to_string(),
            },
            LockError::PermissionDenied {
                reason: "acl".to_string(),
            },
            LockError::Configuration {
                message: "bad quorum config".to_string(),
            },
        ] {
            assert!(
                !Error::Storage(EcstoreError::Lock(lock_err)).is_recoverable_heal(),
                "fatal lock failures cannot be fixed by retrying"
            );
        }
    }

    #[test]
    fn remote_client_unavailable_is_recoverable_via_typed_variant() {
        // The detail deliberately avoids every substring needle: the typed
        // variant alone must classify these as retryable.
        let detail = "auth interceptor rebuild".to_string();
        assert!(Error::Disk(DiskError::RemoteClientUnavailable(detail.clone())).is_recoverable_heal());
        assert!(Error::Storage(EcstoreError::RemoteClientUnavailable(detail)).is_recoverable_heal());
    }
}
