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

pub type Result<T> = std::result::Result<T, KeystoneError>;

/// Keystone integration errors
#[derive(Debug, Error)]
pub enum KeystoneError {
    /// Invalid or malformed token
    #[error("Invalid token")]
    InvalidToken,

    /// Token has expired
    #[error("Token expired")]
    TokenExpired,

    /// Invalid EC2 credentials
    #[error("Invalid credentials")]
    InvalidCredentials,

    /// Authentication failed
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    /// HTTP request error
    #[error("HTTP error: {0}")]
    HttpError(String),

    /// Response parsing error
    #[error("Parse error: {0}")]
    ParseError(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Unsupported Keystone version
    #[error("Unsupported Keystone version")]
    UnsupportedVersion,

    /// Project not found
    #[error("Project not found")]
    ProjectNotFound,

    /// User not found
    #[error("User not found")]
    UserNotFound,

    /// Insufficient permissions
    #[error("Insufficient permissions: {0}")]
    InsufficientPermissions(String),

    /// Internal error
    #[error("Internal error: {0}")]
    InternalError(String),

    /// Network timeout
    #[error("Request timeout")]
    Timeout,

    /// Service unavailable
    #[error("Keystone service unavailable")]
    ServiceUnavailable,
}

impl KeystoneError {
    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            KeystoneError::Timeout | KeystoneError::ServiceUnavailable | KeystoneError::HttpError(_)
        )
    }

    /// Check if error is authentication related
    pub fn is_auth_error(&self) -> bool {
        matches!(
            self,
            KeystoneError::InvalidToken
                | KeystoneError::TokenExpired
                | KeystoneError::InvalidCredentials
                | KeystoneError::AuthenticationFailed(_)
        )
    }
}
