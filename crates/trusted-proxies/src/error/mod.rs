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

//! Error types for the trusted proxy system.

mod config;
mod proxy;

pub use config::*;
pub use proxy::*;

/// Unified error type for the application.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Errors related to configuration.
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    /// Errors related to proxy validation.
    #[error("Proxy validation error: {0}")]
    Proxy(#[from] ProxyError),

    /// Errors related to cloud service integration.
    #[error("Cloud service error: {0}")]
    Cloud(String),

    /// General internal errors.
    #[error("Internal error: {0}")]
    Internal(String),

    /// Standard I/O errors.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Errors related to HTTP requests or responses.
    #[error("HTTP error: {0}")]
    Http(String),
}

impl AppError {
    /// Creates a new `Cloud` error.
    pub fn cloud(msg: impl Into<String>) -> Self {
        Self::Cloud(msg.into())
    }

    /// Creates a new `Internal` error.
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Creates a new `Http` error.
    pub fn http(msg: impl Into<String>) -> Self {
        Self::Http(msg.into())
    }

    /// Returns true if the error is considered recoverable.
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::Config(_) => true,
            Self::Proxy(e) => e.is_recoverable(),
            Self::Cloud(_) => true,
            Self::Internal(_) => false,
            Self::Io(_) => true,
            Self::Http(_) => true,
        }
    }
}

/// Type alias for API error responses (Status Code, Error Message).
pub type ApiError = (http::StatusCode, String);

impl From<AppError> for ApiError {
    fn from(err: AppError) -> Self {
        match err {
            AppError::Config(_) => (http::StatusCode::BAD_REQUEST, err.to_string()),
            AppError::Proxy(_) => (http::StatusCode::BAD_REQUEST, err.to_string()),
            AppError::Cloud(_) => (http::StatusCode::SERVICE_UNAVAILABLE, err.to_string()),
            AppError::Internal(_) => (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            AppError::Io(_) => (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            AppError::Http(_) => (http::StatusCode::BAD_GATEWAY, err.to_string()),
        }
    }
}
