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

/// Result type alias for encryption operations
pub type EncryptionResult<T> = std::result::Result<T, EncryptionError>;

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

/// Encryption operation errors
#[derive(thiserror::Error, Debug)]
pub enum EncryptionError {
    /// Encryption algorithm not supported
    #[error("Encryption algorithm not supported: {algorithm}")]
    UnsupportedAlgorithm { algorithm: String },

    /// Invalid key size
    #[error("Invalid key size: expected {expected}, got {actual}")]
    InvalidKeySize { expected: usize, actual: usize },

    /// Invalid IV size
    #[error("Invalid IV size: expected {expected}, got {actual}")]
    InvalidIvSize { expected: usize, actual: usize },

    /// Encryption metadata error
    #[error("Encryption metadata error: {message}")]
    MetadataError { message: String },

    /// Configuration error
    #[error("Encryption configuration error: {message}")]
    ConfigurationError { message: String },

    /// Cipher operation failed
    #[error("Cipher operation failed: {operation} - {reason}")]
    CipherError { operation: String, reason: String },

    /// Authentication tag verification failed
    #[error("Authentication tag verification failed")]
    AuthenticationFailed,

    /// Key derivation failed
    #[error("Key derivation failed: {reason}")]
    KeyDerivationFailed { reason: String },

    /// KMS operation error
    #[error("KMS operation error: {0}")]
    KmsError(#[from] KmsError),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Base64 decode error
    #[error("Base64 decode error: {0}")]
    Base64Error(#[from] base64::DecodeError),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

impl From<EncryptionError> for KmsError {
    fn from(err: EncryptionError) -> Self {
        match err {
            EncryptionError::UnsupportedAlgorithm { algorithm } => KmsError::CryptographicError {
                operation: format!("Unsupported encryption algorithm: {algorithm}"),
            },
            EncryptionError::InvalidKeySize { expected, actual } => KmsError::InvalidInput {
                message: format!("Invalid key size: expected {expected}, got {actual}"),
            },
            EncryptionError::InvalidIvSize { expected, actual } => KmsError::InvalidInput {
                message: format!("Invalid IV size: expected {expected}, got {actual}"),
            },
            EncryptionError::MetadataError { message } => KmsError::InvalidInput { message },
            EncryptionError::ConfigurationError { message } => KmsError::ConfigurationError { message },
            EncryptionError::CipherError { operation, reason } => KmsError::CryptographicError {
                operation: format!("{operation} operation failed: {reason}"),
            },
            EncryptionError::AuthenticationFailed => KmsError::CryptographicError {
                operation: "Authentication tag verification failed".to_string(),
            },
            EncryptionError::KeyDerivationFailed { reason } => KmsError::CryptographicError {
                operation: format!("Key derivation failed: {reason}"),
            },
            EncryptionError::KmsError(kms_err) => kms_err,
            EncryptionError::SerializationError(json_err) => KmsError::SerializationError(json_err),
            EncryptionError::Base64Error(decode_err) => KmsError::Base64Error(decode_err),
            EncryptionError::IoError(io_err) => KmsError::InternalError {
                message: format!("IO error: {io_err}"),
            },
        }
    }
}

impl EncryptionError {
    /// Create an unsupported algorithm error
    pub fn unsupported_algorithm(algorithm: impl Into<String>) -> Self {
        Self::UnsupportedAlgorithm {
            algorithm: algorithm.into(),
        }
    }

    /// Create an invalid key size error
    pub fn invalid_key_size(expected: usize, actual: usize) -> Self {
        Self::InvalidKeySize { expected, actual }
    }

    /// Create an invalid IV size error
    pub fn invalid_iv_size(expected: usize, actual: usize) -> Self {
        Self::InvalidIvSize { expected, actual }
    }

    /// Create a metadata error
    pub fn metadata_error(message: impl Into<String>) -> Self {
        Self::MetadataError { message: message.into() }
    }

    /// Create a configuration error
    pub fn configuration_error(message: impl Into<String>) -> Self {
        Self::ConfigurationError { message: message.into() }
    }

    /// Create a cipher error
    pub fn cipher_error(operation: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::CipherError {
            operation: operation.into(),
            reason: reason.into(),
        }
    }

    /// Create a key derivation error
    pub fn key_derivation_failed(reason: impl Into<String>) -> Self {
        Self::KeyDerivationFailed { reason: reason.into() }
    }

    /// Check if the error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            EncryptionError::KmsError(kms_err) => kms_err.is_retryable(),
            EncryptionError::IoError(_) => true,
            _ => false,
        }
    }

    /// Check if the error is permanent
    pub fn is_permanent(&self) -> bool {
        match self {
            EncryptionError::UnsupportedAlgorithm { .. }
            | EncryptionError::InvalidKeySize { .. }
            | EncryptionError::InvalidIvSize { .. }
            | EncryptionError::ConfigurationError { .. }
            | EncryptionError::AuthenticationFailed => true,
            EncryptionError::KmsError(kms_err) => kms_err.is_permanent(),
            _ => false,
        }
    }
}
