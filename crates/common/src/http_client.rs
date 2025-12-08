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

//! Centralized HTTP client factory with resilient connection management.
//!
//! This module provides HTTP clients configured with aggressive keepalive settings
//! to detect dead peers quickly (within 3-8 seconds) during abrupt node failures.
//!
//! # Key Features
//! - Fast dead peer detection via TCP and HTTP/2 keepalive
//! - Consistent configuration across all HTTP clients
//! - Tunable timeouts for different operation types
//! - Connection pooling with automatic cleanup

use reqwest::ClientBuilder;
use std::time::Duration;

/// HTTP client configuration for latency-sensitive operations (console, admin, IAM).
/// These operations require fast failure detection to maintain user experience.
pub struct FastHttpClientConfig {
    /// Maximum time to establish TCP connection
    pub connect_timeout_secs: u64,
    /// Overall request timeout
    pub request_timeout_secs: u64,
    /// TCP keepalive probe interval
    pub tcp_keepalive_secs: u64,
    /// HTTP/2 PING interval
    pub http2_keepalive_interval_secs: u64,
    /// HTTP/2 PING ACK timeout
    pub http2_keepalive_timeout_secs: u64,
    /// Connection pool idle timeout
    pub pool_idle_timeout_secs: u64,
    /// Maximum idle connections per host
    pub pool_max_idle_per_host: usize,
}

impl Default for FastHttpClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: 2,
            request_timeout_secs: 10,
            tcp_keepalive_secs: 10,
            http2_keepalive_interval_secs: 3,
            http2_keepalive_timeout_secs: 2,
            pool_idle_timeout_secs: 30,
            pool_max_idle_per_host: 10,
        }
    }
}

/// HTTP client configuration for bulk data operations (file uploads/downloads).
/// These operations can tolerate slightly longer timeouts for stability.
pub struct BulkHttpClientConfig {
    /// Maximum time to establish TCP connection
    pub connect_timeout_secs: u64,
    /// Overall request timeout
    pub request_timeout_secs: u64,
    /// TCP keepalive probe interval
    pub tcp_keepalive_secs: u64,
    /// HTTP/2 PING interval
    pub http2_keepalive_interval_secs: u64,
    /// HTTP/2 PING ACK timeout
    pub http2_keepalive_timeout_secs: u64,
    /// Connection pool idle timeout
    pub pool_idle_timeout_secs: u64,
    /// Maximum idle connections per host
    pub pool_max_idle_per_host: usize,
}

impl Default for BulkHttpClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: 3,
            request_timeout_secs: 30,
            tcp_keepalive_secs: 10,
            http2_keepalive_interval_secs: 5,
            http2_keepalive_timeout_secs: 3,
            pool_idle_timeout_secs: 30,
            pool_max_idle_per_host: 20,
        }
    }
}

/// Create an HTTP client optimized for latency-sensitive operations.
///
/// This client is configured with aggressive keepalive settings to detect
/// dead peers within 3-5 seconds. Use for console, admin, and IAM operations.
///
/// # Examples
/// ```no_run
/// use rustfs_common::http_client::create_fast_http_client;
///
/// let client = create_fast_http_client().unwrap();
/// // Use for console aggregation, admin API calls, etc.
/// ```
pub fn create_fast_http_client() -> Result<reqwest::Client, reqwest::Error> {
    create_fast_http_client_with_config(FastHttpClientConfig::default())
}

/// Create an HTTP client with custom fast configuration.
pub fn create_fast_http_client_with_config(
    config: FastHttpClientConfig,
) -> Result<reqwest::Client, reqwest::Error> {
    ClientBuilder::new()
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .tcp_keepalive(Duration::from_secs(config.tcp_keepalive_secs))
        .http2_keep_alive_interval(Duration::from_secs(config.http2_keepalive_interval_secs))
        .http2_keep_alive_timeout(Duration::from_secs(config.http2_keepalive_timeout_secs))
        .http2_keep_alive_while_idle(true)
        .pool_idle_timeout(Duration::from_secs(config.pool_idle_timeout_secs))
        .pool_max_idle_per_host(config.pool_max_idle_per_host)
        .tcp_nodelay(true)
        .build()
}

/// Create an HTTP client optimized for bulk data operations.
///
/// This client is configured with balanced keepalive settings suitable for
/// large file transfers. Detects dead peers within 5-8 seconds.
///
/// # Examples
/// ```no_run
/// use rustfs_common::http_client::create_bulk_http_client;
///
/// let client = create_bulk_http_client().unwrap();
/// // Use for file uploads, downloads, erasure coding operations
/// ```
pub fn create_bulk_http_client() -> Result<reqwest::Client, reqwest::Error> {
    create_bulk_http_client_with_config(BulkHttpClientConfig::default())
}

/// Create an HTTP client with custom bulk configuration.
pub fn create_bulk_http_client_with_config(
    config: BulkHttpClientConfig,
) -> Result<reqwest::Client, reqwest::Error> {
    ClientBuilder::new()
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .tcp_keepalive(Duration::from_secs(config.tcp_keepalive_secs))
        .http2_keep_alive_interval(Duration::from_secs(config.http2_keepalive_interval_secs))
        .http2_keep_alive_timeout(Duration::from_secs(config.http2_keepalive_timeout_secs))
        .http2_keep_alive_while_idle(true)
        .pool_idle_timeout(Duration::from_secs(config.pool_idle_timeout_secs))
        .pool_max_idle_per_host(config.pool_max_idle_per_host)
        .tcp_nodelay(true)
        .build()
}

/// Read timeout configuration from environment variables.
///
/// Allows operators to tune timeouts for their specific network conditions.
pub fn read_timeout_from_env(var_name: &str, default: u64) -> u64 {
    std::env::var(var_name)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_client_creation() {
        let client = create_fast_http_client();
        assert!(client.is_ok(), "Fast HTTP client creation should succeed");
    }

    #[test]
    fn test_bulk_client_creation() {
        let client = create_bulk_http_client();
        assert!(client.is_ok(), "Bulk HTTP client creation should succeed");
    }

    #[test]
    fn test_custom_config() {
        let config = FastHttpClientConfig {
            connect_timeout_secs: 1,
            request_timeout_secs: 5,
            ..Default::default()
        };
        let client = create_fast_http_client_with_config(config);
        assert!(client.is_ok(), "Custom config client creation should succeed");
    }
}
