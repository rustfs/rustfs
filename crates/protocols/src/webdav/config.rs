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

use std::fmt::Debug;
use std::net::SocketAddr;
use thiserror::Error;

/// WebDAV server initialization error
#[derive(Debug, Error)]
pub enum WebDavInitError {
    #[error("failed to bind address: {0}")]
    Bind(#[from] std::io::Error),
    #[error("server error: {0}")]
    Server(String),
    #[error("invalid WebDAV configuration: {0}")]
    InvalidConfig(String),
    #[error("TLS error: {0}")]
    Tls(String),
}

/// WebDAV server configuration
#[derive(Debug, Clone)]
pub struct WebDavConfig {
    /// Server bind address
    pub bind_addr: SocketAddr,
    /// Whether TLS is enabled (default: true)
    pub tls_enabled: bool,
    /// Certificate directory path (supports multiple certificates)
    pub cert_dir: Option<String>,
    /// CA certificate file path for client certificate verification
    pub ca_file: Option<String>,
    /// Maximum request body size in bytes (default: 5GB)
    pub max_body_size: u64,
    /// Request timeout in seconds (default: 300)
    pub request_timeout_secs: u64,
}

impl WebDavConfig {
    /// Default maximum body size (5GB)
    pub const DEFAULT_MAX_BODY_SIZE: u64 = 5 * 1024 * 1024 * 1024;
    /// Default request timeout (300 seconds)
    pub const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 300;

    /// Validates the configuration
    pub async fn validate(&self) -> Result<(), WebDavInitError> {
        // Validate TLS configuration
        if self.tls_enabled && self.cert_dir.is_none() {
            return Err(WebDavInitError::InvalidConfig(
                "TLS is enabled but certificate directory is missing".to_string(),
            ));
        }

        if let Some(path) = &self.cert_dir
            && !tokio::fs::try_exists(path).await.unwrap_or(false)
        {
            return Err(WebDavInitError::InvalidConfig(format!("Certificate directory not found: {}", path)));
        }

        // Validate CA file exists if specified
        if let Some(path) = &self.ca_file
            && !tokio::fs::try_exists(path).await.unwrap_or(false)
        {
            return Err(WebDavInitError::InvalidConfig(format!("CA file not found: {}", path)));
        }

        // Validate max body size
        if self.max_body_size == 0 {
            return Err(WebDavInitError::InvalidConfig("max_body_size cannot be zero".to_string()));
        }

        // Validate request timeout
        if self.request_timeout_secs == 0 {
            return Err(WebDavInitError::InvalidConfig("request_timeout_secs cannot be zero".to_string()));
        }

        Ok(())
    }
}

impl Default for WebDavConfig {
    fn default() -> Self {
        Self {
            // Use direct construction instead of parse().unwrap() to avoid panic
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            tls_enabled: true,
            cert_dir: None,
            ca_file: None,
            max_body_size: Self::DEFAULT_MAX_BODY_SIZE,
            request_timeout_secs: Self::DEFAULT_REQUEST_TIMEOUT_SECS,
        }
    }
}
