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

//! KMS error types and handling

// use std::fmt; // Unused

/// Result type alias for KMS operations
pub type Result<T> = std::result::Result<T, KmsError>;

/// KMS operation errors
#[derive(thiserror::Error, Debug)]
pub enum KmsError {
    /// Key not found error
    #[error("Key not found: {key_id}")]
    KeyNotFound { key_id: String },

    /// Key already exists error
    #[error("Key already exists: {key_id}")]
    KeyExists { key_id: String },

    /// Permission denied error
    #[error("Permission denied: {operation}")]
    PermissionDenied { operation: String },

    /// Authentication error
    #[error("Authentication failed: {reason}")]
    AuthenticationFailed { reason: String },

    /// Configuration error
    #[error("Configuration error: {message}")]
    ConfigurationError { message: String },

    /// Network/connection error
    #[error("Connection error: {message}")]
    ConnectionError { message: String },

    /// Encryption/decryption error
    #[error("Cryptographic operation failed: {operation}")]
    CryptographicError { operation: String },

    /// Invalid input error
    #[error("Invalid input: {message}")]
    InvalidInput { message: String },

    /// Backend service error
    #[error("Backend service error: {service} - {message}")]
    BackendError { service: String, message: String },

    /// Internal error
    #[error("Internal error: {message}")]
    InternalError { message: String },

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Base64 decode error
    #[error("Base64 decode error: {0}")]
    Base64Error(#[from] base64::DecodeError),

    /// HTTP request error
    #[error("HTTP request error: {0}")]
    HttpError(#[from] reqwest::Error),
}

impl KmsError {
    /// Create a key not found error
    pub fn key_not_found(key_id: impl Into<String>) -> Self {
        Self::KeyNotFound { key_id: key_id.into() }
    }

    /// Create a key exists error
    pub fn key_exists(key_id: impl Into<String>) -> Self {
        Self::KeyExists { key_id: key_id.into() }
    }

    /// Create a permission denied error
    pub fn permission_denied(operation: impl Into<String>) -> Self {
        Self::PermissionDenied {
            operation: operation.into(),
        }
    }

    /// Create an authentication failed error
    pub fn authentication_failed(reason: impl Into<String>) -> Self {
        Self::AuthenticationFailed { reason: reason.into() }
    }

    /// Create a configuration error
    pub fn configuration_error(message: impl Into<String>) -> Self {
        Self::ConfigurationError { message: message.into() }
    }

    /// Create a connection error
    pub fn connection_error(message: impl Into<String>) -> Self {
        Self::ConnectionError { message: message.into() }
    }

    /// Create a cryptographic error
    pub fn cryptographic_error(operation: impl Into<String>) -> Self {
        Self::CryptographicError {
            operation: operation.into(),
        }
    }

    /// Create an invalid input error
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput { message: message.into() }
    }

    /// Create a backend error
    pub fn backend_error(service: impl Into<String>, message: impl Into<String>) -> Self {
        Self::BackendError {
            service: service.into(),
            message: message.into(),
        }
    }

    /// Create an internal error
    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::InternalError { message: message.into() }
    }

    /// Check if the error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(self, KmsError::ConnectionError { .. } | KmsError::BackendError { .. })
    }

    /// Check if the error is permanent
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            KmsError::KeyNotFound { .. }
                | KmsError::KeyExists { .. }
                | KmsError::PermissionDenied { .. }
                | KmsError::AuthenticationFailed { .. }
                | KmsError::ConfigurationError { .. }
                | KmsError::InvalidInput { .. }
        )
    }
}
