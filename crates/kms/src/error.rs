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

//! KMS error types and result handling

use thiserror::Error;

/// Result type for KMS operations
pub type Result<T> = std::result::Result<T, KmsError>;

/// KMS error types covering all possible failure scenarios
#[derive(Error, Debug, Clone)]
pub enum KmsError {
    /// Configuration errors
    #[error("Configuration error: {message}")]
    ConfigurationError { message: String },

    /// Key not found
    #[error("Key not found: {key_id}")]
    KeyNotFound { key_id: String },

    /// Invalid key format or content
    #[error("Invalid key: {message}")]
    InvalidKey { message: String },

    /// Cryptographic operation failed
    #[error("Cryptographic error in {operation}: {message}")]
    CryptographicError { operation: String, message: String },

    /// Backend communication error
    #[error("Backend error: {message}")]
    BackendError { message: String },

    /// Access denied
    #[error("Access denied: {message}")]
    AccessDenied { message: String },

    /// Key already exists
    #[error("Key already exists: {key_id}")]
    KeyAlreadyExists { key_id: String },

    /// Invalid operation state
    #[error("Invalid operation: {message}")]
    InvalidOperation { message: String },

    /// Internal error
    #[error("Internal error: {message}")]
    InternalError { message: String },

    /// Serialization/deserialization error
    #[error("Serialization error: {message}")]
    SerializationError { message: String },

    /// I/O error
    #[error("I/O error: {message}")]
    IoError { message: String },

    /// Cache error
    #[error("Cache error: {message}")]
    CacheError { message: String },

    /// Validation error
    #[error("Validation error: {message}")]
    ValidationError { message: String },

    /// Unsupported algorithm
    #[error("Unsupported algorithm: {algorithm}")]
    UnsupportedAlgorithm { algorithm: String },

    /// Invalid key size
    #[error("Invalid key size: expected {expected}, got {actual}")]
    InvalidKeySize { expected: usize, actual: usize },

    /// Encryption context mismatch
    #[error("Encryption context mismatch: {message}")]
    ContextMismatch { message: String },
}

impl KmsError {
    /// Create a configuration error
    pub fn configuration_error<S: Into<String>>(message: S) -> Self {
        Self::ConfigurationError { message: message.into() }
    }

    /// Create a key not found error
    pub fn key_not_found<S: Into<String>>(key_id: S) -> Self {
        Self::KeyNotFound { key_id: key_id.into() }
    }

    /// Create an invalid key error
    pub fn invalid_key<S: Into<String>>(message: S) -> Self {
        Self::InvalidKey { message: message.into() }
    }

    /// Create a cryptographic error
    pub fn cryptographic_error<S1: Into<String>, S2: Into<String>>(operation: S1, message: S2) -> Self {
        Self::CryptographicError {
            operation: operation.into(),
            message: message.into(),
        }
    }

    /// Create a backend error
    pub fn backend_error<S: Into<String>>(message: S) -> Self {
        Self::BackendError { message: message.into() }
    }

    /// Create access denied error
    pub fn access_denied<S: Into<String>>(message: S) -> Self {
        Self::AccessDenied { message: message.into() }
    }

    /// Create a key already exists error
    pub fn key_already_exists<S: Into<String>>(key_id: S) -> Self {
        Self::KeyAlreadyExists { key_id: key_id.into() }
    }

    /// Create an invalid operation error
    pub fn invalid_operation<S: Into<String>>(message: S) -> Self {
        Self::InvalidOperation { message: message.into() }
    }

    /// Create an internal error
    pub fn internal_error<S: Into<String>>(message: S) -> Self {
        Self::InternalError { message: message.into() }
    }

    /// Create a serialization error
    pub fn serialization_error<S: Into<String>>(message: S) -> Self {
        Self::SerializationError { message: message.into() }
    }

    /// Create an I/O error
    pub fn io_error<S: Into<String>>(message: S) -> Self {
        Self::IoError { message: message.into() }
    }

    /// Create a cache error
    pub fn cache_error<S: Into<String>>(message: S) -> Self {
        Self::CacheError { message: message.into() }
    }

    /// Create a validation error
    pub fn validation_error<S: Into<String>>(message: S) -> Self {
        Self::ValidationError { message: message.into() }
    }

    /// Create an invalid parameter error
    pub fn invalid_parameter<S: Into<String>>(message: S) -> Self {
        Self::InvalidOperation { message: message.into() }
    }

    /// Create an invalid key state error
    pub fn invalid_key_state<S: Into<String>>(message: S) -> Self {
        Self::InvalidOperation { message: message.into() }
    }

    /// Create an unsupported algorithm error
    pub fn unsupported_algorithm<S: Into<String>>(algorithm: S) -> Self {
        Self::UnsupportedAlgorithm {
            algorithm: algorithm.into(),
        }
    }

    /// Create an invalid key size error
    pub fn invalid_key_size(expected: usize, actual: usize) -> Self {
        Self::InvalidKeySize { expected, actual }
    }

    /// Create an encryption context mismatch error
    pub fn context_mismatch<S: Into<String>>(message: S) -> Self {
        Self::ContextMismatch { message: message.into() }
    }
}

/// Convert from standard library errors
impl From<std::io::Error> for KmsError {
    fn from(error: std::io::Error) -> Self {
        Self::IoError {
            message: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for KmsError {
    fn from(error: serde_json::Error) -> Self {
        Self::SerializationError {
            message: error.to_string(),
        }
    }
}

// Note: We can't implement From for both aes_gcm::Error and chacha20poly1305::Error
// because they might be the same type. Instead, we provide helper functions.

impl KmsError {
    /// Create a KMS error from AES-GCM error
    ///
    /// #Arguments
    /// * `error` - The AES-GCM error to convert
    ///
    /// #Returns
    /// * `KmsError` - The corresponding KMS error
    ///
    pub fn from_aes_gcm_error(error: aes_gcm::Error) -> Self {
        Self::CryptographicError {
            operation: "AES-GCM".to_string(),
            message: error.to_string(),
        }
    }

    /// Create a KMS error from ChaCha20-Poly1305 error
    ///
    /// #Arguments
    /// * `error` - The ChaCha20-Poly1305 error to convert
    ///
    /// #Returns
    /// * `KmsError` - The corresponding KMS error
    ///
    pub fn from_chacha20_error(error: chacha20poly1305::Error) -> Self {
        Self::CryptographicError {
            operation: "ChaCha20-Poly1305".to_string(),
            message: error.to_string(),
        }
    }
}

impl From<url::ParseError> for KmsError {
    fn from(error: url::ParseError) -> Self {
        Self::ConfigurationError {
            message: format!("Invalid URL: {error}"),
        }
    }
}

impl From<reqwest::Error> for KmsError {
    fn from(error: reqwest::Error) -> Self {
        Self::BackendError {
            message: format!("HTTP request failed: {error}"),
        }
    }
}
