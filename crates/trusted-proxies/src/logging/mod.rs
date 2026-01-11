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

//! Logging module for structured logging and middleware

mod middleware;

pub use middleware::*;

/// 日志配置
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    /// 是否启用结构化日志
    pub structured: bool,
    /// 日志级别
    pub level: String,
    /// 是否启用请求 ID
    pub enable_request_id: bool,
    /// 是否记录请求体
    pub log_request_body: bool,
    /// 是否记录响应体
    pub log_response_body: bool,
    /// 敏感字段列表（将被脱敏）
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
            ],
        }
    }
}

/// 初始化日志系统
pub fn init_logging(config: &LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
    // 创建日志过滤器
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(config.level.parse().unwrap_or(tracing::Level::INFO.into()))
        .from_env_lossy();

    // 根据配置选择日志格式
    if config.structured {
        // 结构化日志（JSON 格式）
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .init();
    } else {
        // 普通文本日志
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .init();
    }

    tracing::info!("Logging initialized with level: {}", config.level);

    Ok(())
}

/// 日志记录器
#[derive(Debug, Clone)]
pub struct Logger {
    config: LoggingConfig,
}

impl Logger {
    /// 创建新的日志记录器
    pub fn new(config: LoggingConfig) -> Self {
        Self { config }
    }

    /// 记录 HTTP 请求
    pub fn log_request(&self, req: &axum::http::Request<axum::body::Body>, request_id: &str) {
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

        // 如果启用了请求体日志记录，记录头部
        if self.config.log_request_body {
            self.log_headers(req.headers(), "request");
        }
    }

    /// 记录 HTTP 响应
    pub fn log_response(&self, res: &axum::http::Response<axum::body::Body>, request_id: &str, duration: std::time::Duration) {
        let status = res.status();
        let version = res.version();

        tracing::info!(
            response.status = %status,
            response.version = ?version,
            request_id = %request_id,
            duration_ms = duration.as_millis(),
            "HTTP response sent"
        );

        // 如果启用了响应体日志记录，记录头部
        if self.config.log_response_body {
            self.log_headers(res.headers(), "response");
        }
    }

    /// 记录头部信息（脱敏敏感字段）
    fn log_headers(&self, headers: &axum::http::HeaderMap, header_type: &str) {
        let mut header_fields = std::collections::HashMap::new();

        for (name, value) in headers {
            let name_str = name.to_string();
            let value_str = match value.to_str() {
                Ok(s) => s.to_string(),
                Err(_) => "[BINARY]".to_string(),
            };

            // 检查是否为敏感字段
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

    /// 记录错误
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

    /// 记录警告
    pub fn log_warning(&self, message: &str, context: Option<&str>) {
        if let Some(ctx) = context {
            tracing::warn!(message = %message, context = %ctx, "Warning");
        } else {
            tracing::warn!(message = %message, "Warning");
        }
    }

    /// 记录信息
    pub fn log_info(&self, message: &str, context: Option<&str>) {
        if let Some(ctx) = context {
            tracing::info!(message = %message, context = %ctx, "Info");
        } else {
            tracing::info!(message = %message, "Info");
        }
    }

    /// 记录调试信息
    pub fn log_debug(&self, message: &str, context: Option<&str>) {
        if let Some(ctx) = context {
            tracing::debug!(message = %message, context = %ctx, "Debug");
        } else {
            tracing::debug!(message = %message, "Debug");
        }
    }
}
