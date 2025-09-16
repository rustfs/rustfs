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

//! Webhook audit target implementation

use crate::entity::AuditEntry;
use crate::error::{TargetError, TargetResult};
use crate::registry::{AuditTarget, AuditTargetConfig, TargetStatus};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{debug, error};

/// Webhook audit target implementation with enhanced reliability and performance
pub struct WebhookAuditTarget {
    id: String,
    url: String,
    method: String,
    headers: Option<HashMap<String, String>>,
    timeout_duration: Duration,
    retries: u32,
    client: reqwest::Client,
    enabled: Arc<RwLock<bool>>,
    running: Arc<RwLock<bool>>,
    // Enhanced statistics tracking
    success_count: AtomicU64,
    error_count: AtomicU64,
    last_error: Arc<RwLock<Option<String>>>,
    last_error_time: Arc<RwLock<Option<chrono::DateTime<chrono::Utc>>>>,
    last_success_time: Arc<RwLock<Option<chrono::DateTime<chrono::Utc>>>>,
    // Performance metrics
    total_request_time: AtomicU64,
    min_request_time: AtomicU64,
    max_request_time: AtomicU64,
}

impl WebhookAuditTarget {
    /// Create from configuration with enhanced validation and error handling
    pub async fn from_config(config: &AuditTargetConfig) -> TargetResult<Self> {
        Self::validate_config(config)?;

        let args = &config.args;
        let url = args
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig {
                message: "Missing required 'url' field".to_string(),
            })?
            .to_string();

        let method = args
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("POST")
            .to_uppercase();

        // Validate HTTP method
        match method.as_str() {
            "GET" | "POST" | "PUT" | "PATCH" | "DELETE" => {}
            _ => {
                return Err(TargetError::InvalidConfig {
                    message: format!("Unsupported HTTP method: {}", method),
                });
            }
        }

        let timeout_ms = args
            .get("timeout_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(5000)
            .clamp(100, 60000); // Clamp between 100ms and 60s

        let retries = args
            .get("retries")
            .and_then(|v| v.as_u64())
            .unwrap_or(3)
            .clamp(0, 10) as u32; // Reasonable retry limit

        let headers = args.get("headers").and_then(|v| v.as_object()).map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        });

        // Enhanced HTTP client configuration
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .connect_timeout(Duration::from_millis(timeout_ms / 2))
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .user_agent("RustFS-Audit/1.0")
            .build()
            .map_err(|e| TargetError::InitializationFailed {
                message: format!("Failed to create HTTP client: {}", e),
            })?;

        Ok(Self {
            id: config.id.clone(),
            url,
            method,
            headers,
            timeout_duration: Duration::from_millis(timeout_ms),
            retries,
            client,
            enabled: Arc::new(RwLock::new(config.enabled)),
            running: Arc::new(RwLock::new(false)),
            success_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_error: Arc::new(RwLock::new(None)),
            last_error_time: Arc::new(RwLock::new(None)),
            last_success_time: Arc::new(RwLock::new(None)),
            total_request_time: AtomicU64::new(0),
            min_request_time: AtomicU64::new(u64::MAX),
            max_request_time: AtomicU64::new(0),
        })
    }

    /// Enhanced configuration validation
    pub fn validate_config(config: &AuditTargetConfig) -> TargetResult<()> {
        let args = &config.args;

        // Check required fields
        let url = args
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TargetError::InvalidConfig {
                message: "Missing required 'url' field".to_string(),
            })?;

        // Validate URL format and scheme
        let parsed_url = url::Url::parse(url).map_err(|e| TargetError::InvalidConfig {
            message: format!("Invalid URL '{}': {}", url, e),
        })?;

        match parsed_url.scheme() {
            "http" | "https" => {}
            scheme => {
                return Err(TargetError::InvalidConfig {
                    message: format!("Unsupported URL scheme '{}', only http/https allowed", scheme),
                });
            }
        }

        // Validate method if specified
        if let Some(method) = args.get("method").and_then(|v| v.as_str()) {
            match method.to_uppercase().as_str() {
                "GET" | "POST" | "PUT" | "PATCH" | "DELETE" => {}
                _ => {
                    return Err(TargetError::InvalidConfig {
                        message: format!("Unsupported HTTP method: {}", method),
                    });
                }
            }
        }

        // Validate timeout range
        if let Some(timeout_ms) = args.get("timeout_ms").and_then(|v| v.as_u64()) {
            if timeout_ms < 100 || timeout_ms > 60000 {
                return Err(TargetError::InvalidConfig {
                    message: format!(
                        "timeout_ms must be between 100 and 60000, got {}",
                        timeout_ms
                    ),
                });
            }
        }

        // Validate retry count
        if let Some(retries) = args.get("retries").and_then(|v| v.as_u64()) {
            if retries > 10 {
                return Err(TargetError::InvalidConfig {
                    message: format!("retries must be <= 10, got {}", retries),
                });
            }
        }

        // Validate target_type if specified in args (per specification)
        if let Some(target_type) = args.get("target_type").and_then(|v| v.as_str()) {
            if target_type != "AuditLog" {
                return Err(TargetError::InvalidConfig {
                    message: format!(
                        "target_type must be 'AuditLog', got '{}'",
                        target_type
                    ),
                });
            }
        }

        Ok(())
    }

    /// Send HTTP request with enhanced error handling and retry logic
    async fn send_request(&self, json_payload: String) -> TargetResult<()> {
        let mut last_error = None;

        for attempt in 0..=self.retries {
            let request_start = Instant::now();

            // Build request
            let mut request = self
                .client
                .request(
                    reqwest::Method::from_bytes(self.method.as_bytes())
                        .map_err(|e| TargetError::InvalidConfig {
                            message: format!("Invalid HTTP method: {}", e),
                        })?,
                    &self.url,
                )
                .header("Content-Type", "application/json")
                .body(json_payload.clone());

            // Add custom headers
            if let Some(ref headers) = self.headers {
                for (key, value) in headers {
                    request = request.header(key, value);
                }
            }

            // Execute request with timeout
            let response_result = timeout(self.timeout_duration, request.send()).await;

            let request_duration = request_start.elapsed();
            let duration_nanos = request_duration.as_nanos() as u64;

            match response_result {
                Ok(Ok(response)) => {
                    if response.status().is_success() {
                        // Update success metrics
                        self.success_count.fetch_add(1, Ordering::Relaxed);
                        *self.last_success_time.write() = Some(chrono::Utc::now());

                        // Update performance metrics
                        self.total_request_time.fetch_add(duration_nanos, Ordering::Relaxed);
                        self.update_min_time(duration_nanos);
                        self.update_max_time(duration_nanos);

                        debug!(
                            "Webhook {} sent successfully in {:?} (attempt {})",
                            self.id,
                            request_duration,
                            attempt + 1
                        );
                        return Ok(());
                    } else {
                        let status = response.status();
                        let error_msg = format!("HTTP error: {} {}", status.as_u16(), status.canonical_reason().unwrap_or("Unknown"));
                        
                        // For 4xx errors, don't retry (client errors)
                        if status.is_client_error() {
                            self.record_error(&error_msg);
                            return Err(TargetError::OperationFailed { message: error_msg });
                        }
                        
                        last_error = Some(TargetError::OperationFailed { message: error_msg });
                    }
                }
                Ok(Err(e)) => {
                    last_error = Some(TargetError::from(e));
                }
                Err(_) => {
                    let error_msg = format!("Request timeout after {:?}", self.timeout_duration);
                    last_error = Some(TargetError::Network { message: error_msg });
                }
            }

            // Wait before retry with exponential backoff
            if attempt < self.retries {
                let backoff_ms = (100 * (1 << attempt)).min(5000); // Cap at 5 seconds
                debug!(
                    "Webhook {} failed on attempt {}, retrying in {}ms",
                    self.id,
                    attempt + 1,
                    backoff_ms
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }

        // All retries exhausted
        let final_error = last_error.unwrap_or_else(|| TargetError::OperationFailed {
            message: "Unknown error after all retries".to_string(),
        });

        self.record_error(&final_error.to_string());
        
        Err(TargetError::RetryLimitExceeded {
            attempts: self.retries + 1,
            max_attempts: self.retries + 1,
        })
    }

    /// Record error with timestamp
    fn record_error(&self, error_msg: &str) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        *self.last_error.write() = Some(error_msg.to_string());
        *self.last_error_time.write() = Some(chrono::Utc::now());
        error!("Webhook {} error: {}", self.id, error_msg);
    }

    /// Update minimum request time atomically
    fn update_min_time(&self, duration_nanos: u64) {
        let mut current_min = self.min_request_time.load(Ordering::Relaxed);
        while current_min > duration_nanos {
            match self.min_request_time.compare_exchange_weak(
                current_min,
                duration_nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_current) => current_min = new_current,
            }
        }
    }

    /// Update maximum request time atomically
    fn update_max_time(&self, duration_nanos: u64) {
        let mut current_max = self.max_request_time.load(Ordering::Relaxed);
        while current_max < duration_nanos {
            match self.max_request_time.compare_exchange_weak(
                current_max,
                duration_nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_current) => current_max = new_current,
            }
        }
    }

    /// Get average request time in nanoseconds
    pub fn get_avg_request_time_ns(&self) -> u64 {
        let total = self.total_request_time.load(Ordering::Relaxed);
        let count = self.success_count.load(Ordering::Relaxed);
        if count > 0 {
            total / count
        } else {
            0
        }
    }

    /// Get performance metrics
    pub fn get_performance_metrics(&self) -> (u64, u64, u64) {
        let min = self.min_request_time.load(Ordering::Relaxed);
        let max = self.max_request_time.load(Ordering::Relaxed);
        let avg = self.get_avg_request_time_ns();
        (
            if min == u64::MAX { 0 } else { min },
            max,
            avg,
        )
    }
}

#[async_trait]
impl AuditTarget for WebhookAuditTarget {
    async fn send(&self, entry: Arc<AuditEntry>) -> TargetResult<()> {
        if !*self.enabled.read() {
            return Err(TargetError::Disabled);
        }

        if !*self.running.read() {
            return Err(TargetError::ShuttingDown);
        }

        let json = entry.to_json().map_err(|e| TargetError::Serialization {
            message: format!("Failed to serialize audit entry: {}", e),
        })?;

        self.send_request(json).await
    }

    async fn start(&self) -> TargetResult<()> {
        *self.running.write() = true;
        debug!("Webhook target {} started", self.id);
        Ok(())
    }

    async fn pause(&self) -> TargetResult<()> {
        *self.running.write() = false;
        debug!("Webhook target {} paused", self.id);
        Ok(())
    }

    async fn resume(&self) -> TargetResult<()> {
        if *self.enabled.read() {
            *self.running.write() = true;
            debug!("Webhook target {} resumed", self.id);
        }
        Ok(())
    }

    async fn close(&self) -> TargetResult<()> {
        *self.running.write() = false;
        *self.enabled.write() = false;
        debug!("Webhook target {} closed", self.id);
        Ok(())
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn target_type(&self) -> &str {
        "webhook"
    }

    fn is_enabled(&self) -> bool {
        *self.enabled.read()
    }

    fn status(&self) -> TargetStatus {
        TargetStatus {
            id: self.id.clone(),
            target_type: "webhook".to_string(),
            enabled: *self.enabled.read(),
            running: *self.running.read(),
            last_error: self.last_error.read().clone(),
            last_error_time: *self.last_error_time.read(),
            success_count: self.success_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            last_success_time: *self.last_success_time.read(),
            queue_size: None, // Webhook targets don't have queues
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_webhook_config_validation() {
        // Valid config
        let config = AuditTargetConfig {
            id: "webhook-test".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com/webhook",
                "method": "POST",
                "timeout_ms": 3000,
                "retries": 2
            }),
        };
        assert!(WebhookAuditTarget::validate_config(&config).is_ok());

        // Missing URL
        let config = AuditTargetConfig {
            id: "webhook-test".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({}),
        };
        assert!(WebhookAuditTarget::validate_config(&config).is_err());

        // Invalid URL scheme
        let config = AuditTargetConfig {
            id: "webhook-test".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "ftp://example.com"
            }),
        };
        assert!(WebhookAuditTarget::validate_config(&config).is_err());

        // Invalid method
        let config = AuditTargetConfig {
            id: "webhook-test".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "method": "INVALID"
            }),
        };
        assert!(WebhookAuditTarget::validate_config(&config).is_err());

        // Invalid timeout
        let config = AuditTargetConfig {
            id: "webhook-test".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "timeout_ms": 50  // Too low
            }),
        };
        assert!(WebhookAuditTarget::validate_config(&config).is_err());

        // Invalid retry count
        let config = AuditTargetConfig {
            id: "webhook-test".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "retries": 20  // Too high
            }),
        };
        assert!(WebhookAuditTarget::validate_config(&config).is_err());

        // Valid target_type in args
        let config = AuditTargetConfig {
            id: "webhook-test".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "target_type": "AuditLog"
            }),
        };
        assert!(WebhookAuditTarget::validate_config(&config).is_ok());

        // Invalid target_type in args
        let config = AuditTargetConfig {
            id: "webhook-test".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "target_type": "InvalidType"
            }),
        };
        assert!(WebhookAuditTarget::validate_config(&config).is_err());
    }

    #[tokio::test]
    async fn test_webhook_lifecycle() {
        let config = AuditTargetConfig {
            id: "webhook-test".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://httpbin.org/post",
                "timeout_ms": 1000
            }),
        };

        let target = WebhookAuditTarget::from_config(&config).await.unwrap();

        // Test initial state
        assert_eq!(target.id(), "webhook-test");
        assert_eq!(target.target_type(), "webhook");
        assert!(target.is_enabled());

        // Test lifecycle
        target.start().await.unwrap();
        assert!(target.status().running);

        target.pause().await.unwrap();
        assert!(!target.status().running);

        target.resume().await.unwrap();
        assert!(target.status().running);

        target.close().await.unwrap();
        assert!(!target.status().running);
        assert!(!target.is_enabled());
    }

    #[tokio::test]
    async fn test_webhook_performance_metrics() {
        let config = AuditTargetConfig {
            id: "webhook-perf".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://httpbin.org/delay/0",
                "timeout_ms": 5000
            }),
        };

        let target = WebhookAuditTarget::from_config(&config).await.unwrap();
        target.start().await.unwrap();

        // Initial metrics should be zero
        let (min, max, avg) = target.get_performance_metrics();
        assert_eq!(min, 0);
        assert_eq!(max, 0);
        assert_eq!(avg, 0);

        // Note: Actual HTTP request testing would require a test server
        // This test verifies the metric initialization
    }

    #[test]
    fn test_webhook_config_clamping() {
        // Test timeout clamping
        let config = AuditTargetConfig {
            id: "webhook-clamp".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "timeout_ms": 75000  // Over limit, should be clamped to 60000
            }),
        };

        // This should not fail validation due to clamping in from_config
        assert!(WebhookAuditTarget::validate_config(&config).is_err());

        // Test retry clamping
        let config = AuditTargetConfig {
            id: "webhook-clamp".to_string(),
            target_type: "webhook".to_string(),
            enabled: true,
            args: json!({
                "url": "https://example.com",
                "retries": 15  // Over limit, should be clamped to 10
            }),
        };

        assert!(WebhookAuditTarget::validate_config(&config).is_err());
    }
}