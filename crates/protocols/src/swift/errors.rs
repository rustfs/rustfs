// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Swift error types and responses

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::fmt;

/// Swift-specific error type
#[derive(Debug)]
#[allow(dead_code)] // Error variants used by Swift implementation
pub enum SwiftError {
    /// 400 Bad Request
    BadRequest(String),
    /// 401 Unauthorized
    Unauthorized(String),
    /// 403 Forbidden
    Forbidden(String),
    /// 404 Not Found
    NotFound(String),
    /// 409 Conflict
    Conflict(String),
    /// 413 Request Entity Too Large (Payload Too Large)
    RequestEntityTooLarge(String),
    /// 422 Unprocessable Entity
    UnprocessableEntity(String),
    /// 429 Too Many Requests
    TooManyRequests { retry_after: u64, limit: u32, reset: u64 },
    /// 500 Internal Server Error
    InternalServerError(String),
    /// 501 Not Implemented
    NotImplemented(String),
    /// 503 Service Unavailable
    ServiceUnavailable(String),
}

impl fmt::Display for SwiftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SwiftError::BadRequest(msg) => write!(f, "Bad Request: {}", msg),
            SwiftError::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            SwiftError::Forbidden(msg) => write!(f, "Forbidden: {}", msg),
            SwiftError::NotFound(msg) => write!(f, "Not Found: {}", msg),
            SwiftError::Conflict(msg) => write!(f, "Conflict: {}", msg),
            SwiftError::RequestEntityTooLarge(msg) => write!(f, "Request Entity Too Large: {}", msg),
            SwiftError::UnprocessableEntity(msg) => write!(f, "Unprocessable Entity: {}", msg),
            SwiftError::TooManyRequests { retry_after, .. } => {
                write!(f, "Too Many Requests: retry after {} seconds", retry_after)
            }
            SwiftError::InternalServerError(msg) => write!(f, "Internal Server Error: {}", msg),
            SwiftError::NotImplemented(msg) => write!(f, "Not Implemented: {}", msg),
            SwiftError::ServiceUnavailable(msg) => write!(f, "Service Unavailable: {}", msg),
        }
    }
}

impl std::error::Error for SwiftError {}

impl SwiftError {
    fn status_code(&self) -> StatusCode {
        match self {
            SwiftError::BadRequest(_) => StatusCode::BAD_REQUEST,
            SwiftError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            SwiftError::Forbidden(_) => StatusCode::FORBIDDEN,
            SwiftError::NotFound(_) => StatusCode::NOT_FOUND,
            SwiftError::Conflict(_) => StatusCode::CONFLICT,
            SwiftError::RequestEntityTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            SwiftError::UnprocessableEntity(_) => StatusCode::UNPROCESSABLE_ENTITY,
            SwiftError::TooManyRequests { .. } => StatusCode::TOO_MANY_REQUESTS,
            SwiftError::InternalServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            SwiftError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            SwiftError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn generate_trans_id() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_micros();
        format!("tx{:x}", timestamp)
    }
}

impl IntoResponse for SwiftError {
    fn into_response(self) -> Response {
        let trans_id = Self::generate_trans_id();
        let status = self.status_code();

        // Handle TooManyRequests specially to include rate limit headers
        if let SwiftError::TooManyRequests {
            retry_after,
            limit,
            reset,
        } = &self
        {
            return (
                status,
                [
                    ("content-type", "text/plain; charset=utf-8".to_string()),
                    ("x-trans-id", trans_id.clone()),
                    ("x-openstack-request-id", trans_id),
                    ("x-ratelimit-limit", limit.to_string()),
                    ("x-ratelimit-remaining", "0".to_string()),
                    ("x-ratelimit-reset", reset.to_string()),
                    ("retry-after", retry_after.to_string()),
                ],
                self.to_string(),
            )
                .into_response();
        }

        let body = self.to_string();

        (
            status,
            [
                ("content-type", "text/plain; charset=utf-8"),
                ("x-trans-id", trans_id.as_str()),
                ("x-openstack-request-id", trans_id.as_str()),
            ],
            body,
        )
            .into_response()
    }
}

/// Result type for Swift operations
pub type SwiftResult<T> = Result<T, SwiftError>;
