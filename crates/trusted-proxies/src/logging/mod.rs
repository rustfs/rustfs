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

//! Logging module for structured logging and observability.

mod middleware;

pub use middleware::*;

/// Configuration for the logging system.
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    /// Whether to use structured JSON logging.
    pub structured: bool,
    /// The logging level (e.g., "info", "debug").
    pub level: String,
    /// Whether to include a unique request ID in logs.
    pub enable_request_id: bool,
    /// Whether to log the contents of request bodies.
    pub log_request_body: bool,
    /// Whether to log the contents of response bodies.
    pub log_response_body: bool,
    /// List of header names that should be redacted in logs.
    pub sensitive_fields: Vec<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            structured: false,
            level: "info".to_string(),
            enable_request_id: true,
            log_request_body: false,
            log_response_body: false,
            sensitive_fields: vec![
                "password".to_string(),
                "token".to_string(),
                "secret".to_string(),
                "authorization".to_string(),
                "cookie".to_string(),
                "set-cookie".to_string(),
            ],
        }
    }
}

/// Initializes the global tracing subscriber.
pub fn init_logging(config: &LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(config.level.parse().unwrap_or(tracing::Level::INFO.into()))
        .from_env_lossy();

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true);

    if config.structured {
        subscriber.json().init();
    } else {
        subscriber.init();
    }

    tracing::info!("Logging initialized with level: {}", config.level);

    Ok(())
}

/// Helper for logging application events with consistent metadata.
#[derive(Debug, Clone)]
pub struct Logger {
    config: LoggingConfig,
}

impl Logger {
    /// Creates a new `Logger`.
    pub fn new(config: LoggingConfig) -> Self {
        Self { config }
    }

    /// Logs an incoming HTTP request.
    pub fn log_request(&self, req: &http::Request<axum::body::Body>, request_id: &str) {
        let method = req.method();
        let uri = req.uri();
        let version = req.version();

        tracing::info!(
            request.method = %method,
            request.uri = %uri,
            request.version = ?version,
            request_id = %request_id,
            "HTTP request received"
        );

        if self.config.log_request_body {
            self.log_headers(req.headers(), "request");
        }
    }

    /// Logs an outgoing HTTP response.
    pub fn log_response(&self, res: &http::Response<axum::body::Body>, request_id: &str, duration: std::time::Duration) {
        let status = res.status();
        let version = res.version();

        tracing::info!(
            response.status = %status,
            response.version = ?version,
            request_id = %request_id,
            duration_ms = duration.as_millis(),
            "HTTP response sent"
        );

        if self.config.log_response_body {
            self.log_headers(res.headers(), "response");
        }
    }

    /// Logs HTTP headers, redacting sensitive information.
    fn log_headers(&self, headers: &http::HeaderMap, header_type: &str) {
        let mut header_fields = std::collections::HashMap::new();

        for (name, value) in headers {
            let name_str = name.to_string();
            let value_str = match value.to_str() {
                Ok(s) => s.to_string(),
                Err(_) => "[BINARY]".to_string(),
            };

            let is_sensitive = self
                .config
                .sensitive_fields
                .iter()
                .any(|field| name_str.to_lowercase().contains(&field.to_lowercase()));

            if is_sensitive {
                header_fields.insert(name_str, "[REDACTED]".to_string());
            } else {
                header_fields.insert(name_str, value_str);
            }
        }

        tracing::debug!(
            headers = ?header_fields,
            header_type = header_type,
            "HTTP headers"
        );
    }

    /// Logs an error with optional request context.
    pub fn log_error(&self, error: &impl std::error::Error, request_id: Option<&str>) {
        if let Some(id) = request_id {
            tracing::error!(
                error = %error,
                error.type = std::any::type_name_of_val(error),
                request_id = %id,
                "Request error"
            );
        } else {
            tracing::error!(
                error = %error,
                error.type = std::any::type_name_of_val(error),
                "Application error"
            );
        }
    }

    /// Logs a warning message.
    pub fn log_warning(&self, message: &str, context: Option<&str>) {
        if let Some(ctx) = context {
            tracing::warn!(message = %message, context = %ctx, "Warning");
        } else {
            tracing::warn!(message = %message, "Warning");
        }
    }

    /// Logs an informational message.
    pub fn log_info(&self, message: &str, context: Option<&str>) {
        if let Some(ctx) = context {
            tracing::info!(message = %message, context = %ctx, "Info");
        } else {
            tracing::info!(message = %message, "Info");
        }
    }

    /// Logs a debug message.
    pub fn log_debug(&self, message: &str, context: Option<&str>) {
        if let Some(ctx) = context {
            tracing::debug!(message = %message, context = %ctx, "Debug");
        } else {
            tracing::debug!(message = %message, "Debug");
        }
    }
}
