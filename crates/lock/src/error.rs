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

use crate::LockId;
use std::time::Duration;
use thiserror::Error;

/// Lock operation related error types
#[derive(Error, Debug)]
pub enum LockError {
    /// Lock acquisition timeout
    #[error("Lock acquisition timeout for resource '{resource}' after {timeout:?}")]
    Timeout { resource: String, timeout: Duration },

    /// Resource not found
    #[error("Resource not found: {resource}")]
    ResourceNotFound { resource: String },

    /// Permission denied
    #[error("Permission denied: {reason}")]
    PermissionDenied { reason: String },

    /// Network error
    #[error("Network error: {message}")]
    Network {
        message: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Internal error
    #[error("Internal error: {message}")]
    Internal { message: String },

    /// Resource is already locked
    #[error("Resource '{resource}' is already locked by {owner}")]
    AlreadyLocked { resource: String, owner: String },

    /// Invalid lock handle
    #[error("Invalid lock handle: {handle_id}")]
    InvalidHandle { handle_id: String },

    /// Configuration error
    #[error("Configuration error: {message}")]
    Configuration { message: String },

    /// Serialization error
    #[error("Serialization error: {message}")]
    Serialization {
        message: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Deserialization error
    #[error("Deserialization error: {message}")]
    Deserialization {
        message: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Insufficient nodes for quorum
    #[error("Insufficient nodes for quorum: required {required}, available {available}")]
    InsufficientNodes { required: usize, available: usize },

    /// Quorum not reached
    #[error("Quorum not reached: required {required}, achieved {achieved}")]
    QuorumNotReached { required: usize, achieved: usize },

    /// Queue is full
    #[error("Queue is full: {message}")]
    QueueFull { message: String },

    /// Not the lock owner
    #[error("Not the lock owner: lock_id {lock_id}, owner {owner}")]
    NotOwner { lock_id: LockId, owner: String },
}

impl Clone for LockError {
    fn clone(&self) -> Self {
        match self {
            LockError::Timeout { resource, timeout } => LockError::Timeout {
                resource: resource.clone(),
                timeout: *timeout,
            },
            LockError::ResourceNotFound { resource } => LockError::ResourceNotFound {
                resource: resource.clone(),
            },
            LockError::PermissionDenied { reason } => LockError::PermissionDenied { reason: reason.clone() },
            LockError::Network { message, source: _ } => LockError::Network {
                message: message.clone(),
                source: Box::new(std::io::Error::other(message.clone())),
            },
            LockError::Internal { message } => LockError::Internal {
                message: message.clone(),
            },
            LockError::AlreadyLocked { resource, owner } => LockError::AlreadyLocked {
                resource: resource.clone(),
                owner: owner.clone(),
            },
            LockError::InvalidHandle { handle_id } => LockError::InvalidHandle {
                handle_id: handle_id.clone(),
            },
            LockError::Configuration { message } => LockError::Configuration {
                message: message.clone(),
            },
            LockError::Serialization { message, source: _ } => LockError::Serialization {
                message: message.clone(),
                source: Box::new(std::io::Error::other(message.clone())),
            },
            LockError::Deserialization { message, source: _ } => LockError::Deserialization {
                message: message.clone(),
                source: Box::new(std::io::Error::other(message.clone())),
            },
            LockError::InsufficientNodes { required, available } => LockError::InsufficientNodes {
                required: *required,
                available: *available,
            },
            LockError::QuorumNotReached { required, achieved } => LockError::QuorumNotReached {
                required: *required,
                achieved: *achieved,
            },
            LockError::QueueFull { message } => LockError::QueueFull {
                message: message.clone(),
            },
            LockError::NotOwner { lock_id, owner } => LockError::NotOwner {
                lock_id: lock_id.clone(),
                owner: owner.clone(),
            },
        }
    }
}

impl LockError {
    /// Create timeout error
    pub fn timeout(resource: impl Into<String>, timeout: Duration) -> Self {
        Self::Timeout {
            resource: resource.into(),
            timeout,
        }
    }

    /// Create resource not found error
    pub fn resource_not_found(resource: impl Into<String>) -> Self {
        Self::ResourceNotFound {
            resource: resource.into(),
        }
    }

    /// Create permission denied error
    pub fn permission_denied(reason: impl Into<String>) -> Self {
        Self::PermissionDenied { reason: reason.into() }
    }

    /// Create network error
    pub fn network(message: impl Into<String>, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Network {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// Create internal error
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal { message: message.into() }
    }

    /// Create lock already locked error
    pub fn already_locked(resource: impl Into<String>, owner: impl Into<String>) -> Self {
        Self::AlreadyLocked {
            resource: resource.into(),
            owner: owner.into(),
        }
    }

    /// Create invalid handle error
    pub fn invalid_handle(handle_id: impl Into<String>) -> Self {
        Self::InvalidHandle {
            handle_id: handle_id.into(),
        }
    }

    /// Create configuration error
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration { message: message.into() }
    }

    /// Create serialization error
    pub fn serialization(message: impl Into<String>, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Serialization {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// Create deserialization error
    pub fn deserialization(message: impl Into<String>, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Deserialization {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// Check if it is a retryable error
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Timeout { .. } | Self::Network { .. } | Self::Internal { .. })
    }

    /// Check if it is a fatal error
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::ResourceNotFound { .. } | Self::PermissionDenied { .. } | Self::Configuration { .. }
        )
    }
}

/// Lock operation Result type
pub type Result<T> = std::result::Result<T, LockError>;

/// Convert from std::io::Error
impl From<std::io::Error> for LockError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::TimedOut => Self::Internal {
                message: "IO timeout".to_string(),
            },
            std::io::ErrorKind::NotFound => Self::ResourceNotFound {
                resource: "unknown".to_string(),
            },
            std::io::ErrorKind::PermissionDenied => Self::PermissionDenied { reason: err.to_string() },
            _ => Self::Internal {
                message: err.to_string(),
            },
        }
    }
}

/// Convert from serde_json::Error
impl From<serde_json::Error> for LockError {
    fn from(err: serde_json::Error) -> Self {
        if err.is_io() {
            Self::network("JSON serialization IO error", err)
        } else if err.is_syntax() {
            Self::deserialization("JSON syntax error", err)
        } else if err.is_data() {
            Self::deserialization("JSON data error", err)
        } else {
            Self::serialization("JSON serialization error", err)
        }
    }
}

/// Convert from tonic::Status
impl From<tonic::Status> for LockError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::DeadlineExceeded => Self::Internal {
                message: "gRPC deadline exceeded".to_string(),
            },
            tonic::Code::NotFound => Self::ResourceNotFound {
                resource: "unknown".to_string(),
            },
            tonic::Code::PermissionDenied => Self::PermissionDenied {
                reason: status.message().to_string(),
            },
            tonic::Code::Unavailable => Self::Network {
                message: "gRPC service unavailable".to_string(),
                source: Box::new(status),
            },
            _ => Self::Internal {
                message: status.message().to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let timeout_err = LockError::timeout("test-resource", Duration::from_secs(5));
        assert!(matches!(timeout_err, LockError::Timeout { .. }));

        let not_found_err = LockError::resource_not_found("missing-resource");
        assert!(matches!(not_found_err, LockError::ResourceNotFound { .. }));

        let permission_err = LockError::permission_denied("insufficient privileges");
        assert!(matches!(permission_err, LockError::PermissionDenied { .. }));
    }

    #[test]
    fn test_error_retryable() {
        let timeout_err = LockError::timeout("test", Duration::from_secs(1));
        assert!(timeout_err.is_retryable());

        let network_err = LockError::network("connection failed", std::io::Error::new(std::io::ErrorKind::ConnectionRefused, ""));
        assert!(network_err.is_retryable());

        let not_found_err = LockError::resource_not_found("test");
        assert!(!not_found_err.is_retryable());
    }

    #[test]
    fn test_error_fatal() {
        let not_found_err = LockError::resource_not_found("test");
        assert!(not_found_err.is_fatal());

        let permission_err = LockError::permission_denied("test");
        assert!(permission_err.is_fatal());

        let timeout_err = LockError::timeout("test", Duration::from_secs(1));
        assert!(!timeout_err.is_fatal());
    }
}
