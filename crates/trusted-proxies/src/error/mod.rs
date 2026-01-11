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

//! Error types for the trusted proxy system

mod config;
mod proxy;

pub use config::*;
pub use proxy::*;

/// 统一错误类型
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// 配置错误
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    /// 代理验证错误
    #[error("Proxy validation error: {0}")]
    Proxy(#[from] ProxyError),

    /// 云服务错误
    #[error("Cloud service error: {0}")]
    Cloud(String),

    /// 内部错误
    #[error("Internal error: {0}")]
    Internal(String),

    /// IO 错误
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// HTTP 错误
    #[error("HTTP error: {0}")]
    Http(String),
}

impl AppError {
    /// 创建云服务错误
    pub fn cloud(msg: impl Into<String>) -> Self {
        Self::Cloud(msg.into())
    }

    /// 创建内部错误
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// 创建 HTTP 错误
    pub fn http(msg: impl Into<String>) -> Self {
        Self::Http(msg.into())
    }

    /// 判断错误是否可恢复
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::Config(_) => true,
            Self::Proxy(_) => true,
            Self::Cloud(_) => true,
            Self::Internal(_) => false,
            Self::Io(_) => true,
            Self::Http(_) => true,
        }
    }
}

/// HTTP 响应错误类型
pub type ApiError = (axum::http::StatusCode, String);

impl From<AppError> for ApiError {
    fn from(err: AppError) -> Self {
        match err {
            AppError::Config(_) => (axum::http::StatusCode::BAD_REQUEST, err.to_string()),
            AppError::Proxy(_) => (axum::http::StatusCode::BAD_REQUEST, err.to_string()),
            AppError::Cloud(_) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, err.to_string()),
            AppError::Internal(_) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            AppError::Io(_) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            AppError::Http(_) => (axum::http::StatusCode::BAD_GATEWAY, err.to_string()),
        }
    }
}
