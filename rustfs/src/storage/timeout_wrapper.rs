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

//! Request Timeout Wrapper for GetObject operations.
//!
//! This module provides timeout protection for GetObject requests to prevent
//! indefinite hangs caused by deadlocks, resource exhaustion, or slow I/O.

// Allow dead_code for public API that may be used by external modules or future features
#![allow(dead_code)]
//!
//! - Configurable request-level timeout (default 30 seconds)
//! - Automatic cancellation of sub-tasks on timeout
//! - Resource cleanup on timeout (locks, memory, file handles)
//! - Prometheus metrics for timeout monitoring
//!
//! # Usage
//!
//! ```ignore
//! use crate::storage::timeout_wrapper::{RequestTimeoutWrapper, TimeoutConfig};
//!
//! let config = TimeoutConfig::from_env();
//! let wrapper = RequestTimeoutWrapper::new(config);
//!
//! match wrapper.execute_with_timeout(|cancel_token| async move {
//!     // Your async operation here
//!     Ok(result)
//! }).await {
//!     TimedGetObjectResult::Success(result) => { /* handle success */ }
//!     TimedGetObjectResult::Timeout(info) => { /* handle timeout */ }
//!     TimedGetObjectResult::Error(e) => { /* handle error */ }
//! }
//! ```

use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

#[cfg(feature = "metrics")]
use metrics::{counter, histogram};

/// Timeout configuration for GetObject requests.
#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    /// GetObject request overall timeout (default 30s).
    /// After this duration, the request is cancelled and returns 504.
    pub get_object_timeout: Duration,

    /// Lock acquisition timeout (default 5s).
    /// Time to wait for a lock before giving up.
    pub lock_acquire_timeout: Duration,

    /// Disk read operation timeout (default 10s).
    /// Individual disk read operations that exceed this are cancelled.
    pub disk_read_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            get_object_timeout: Duration::from_secs(rustfs_config::DEFAULT_GET_OBJECT_TIMEOUT),
            lock_acquire_timeout: Duration::from_secs(rustfs_config::DEFAULT_LOCK_ACQUIRE_TIMEOUT),
            disk_read_timeout: Duration::from_secs(rustfs_config::DEFAULT_DISK_READ_TIMEOUT),
        }
    }
}

impl TimeoutConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let get_object_timeout =
            rustfs_utils::get_env_u64(rustfs_config::ENV_GET_OBJECT_TIMEOUT, rustfs_config::DEFAULT_GET_OBJECT_TIMEOUT);
        let lock_acquire_timeout =
            rustfs_utils::get_env_u64(rustfs_config::ENV_LOCK_ACQUIRE_TIMEOUT, rustfs_config::DEFAULT_LOCK_ACQUIRE_TIMEOUT);
        let disk_read_timeout =
            rustfs_utils::get_env_u64(rustfs_config::ENV_DISK_READ_TIMEOUT, rustfs_config::DEFAULT_DISK_READ_TIMEOUT);

        Self {
            get_object_timeout: Duration::from_secs(get_object_timeout),
            lock_acquire_timeout: Duration::from_secs(lock_acquire_timeout),
            disk_read_timeout: Duration::from_secs(disk_read_timeout),
        }
    }

    /// Check if timeout is enabled (timeout > 0).
    pub fn is_timeout_enabled(&self) -> bool {
        self.get_object_timeout > Duration::ZERO
    }
}

/// Information about a timeout event.
#[derive(Debug, Clone)]
pub struct TimeoutInfo {
    /// Request ID for correlation.
    pub request_id: String,
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Configured timeout duration.
    pub timeout_duration: Duration,
    /// Actual elapsed time before timeout.
    pub elapsed: Duration,
    /// Number of bytes transferred before timeout.
    pub bytes_transferred: u64,
    /// Lock hold time before timeout.
    pub lock_hold_time: Option<Duration>,
    /// Number of disk reads completed.
    pub disk_reads_completed: u32,
    /// Number of disk reads pending.
    pub disk_reads_pending: u32,
}

/// Result of a timed GetObject operation.
#[derive(Debug)]
pub enum TimedGetObjectResult<T, E> {
    /// Operation completed successfully within timeout.
    Success(T),
    /// Operation timed out and was cancelled.
    Timeout(TimeoutInfo),
    /// Operation failed with an error (before timeout).
    Error(E),
}

/// Request timeout wrapper for async operations.
#[derive(Debug)]
pub struct RequestTimeoutWrapper {
    /// Configuration.
    config: TimeoutConfig,
    /// Request start time.
    start_time: Instant,
    /// Cancellation token for propagating cancellation to sub-tasks.
    cancel_token: CancellationToken,
    /// Request ID for logging/metrics.
    request_id: String,
}

impl RequestTimeoutWrapper {
    /// Create a new timeout wrapper with the given configuration.
    pub fn new(config: TimeoutConfig) -> Self {
        Self {
            config,
            start_time: Instant::now(),
            cancel_token: CancellationToken::new(),
            request_id: format!("req-{}", &uuid::Uuid::new_v4().to_string()[..8]),
        }
    }

    /// Create a new timeout wrapper with a specific request ID.
    pub fn with_request_id(config: TimeoutConfig, request_id: impl Into<String>) -> Self {
        Self {
            config,
            start_time: Instant::now(),
            cancel_token: CancellationToken::new(),
            request_id: request_id.into(),
        }
    }

    /// Get the request ID.
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Get the cancellation token.
    /// This can be cloned and passed to sub-tasks for cooperative cancellation.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Check if the operation has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    /// Check if the timeout has been exceeded.
    pub fn is_timeout(&self) -> bool {
        self.config.is_timeout_enabled() && self.elapsed() >= self.config.get_object_timeout
    }

    /// Get elapsed time since the request started.
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get remaining time before timeout.
    /// Returns None if timeout is disabled or already exceeded.
    pub fn remaining_time(&self) -> Option<Duration> {
        if !self.config.is_timeout_enabled() {
            return None;
        }
        let remaining = self.config.get_object_timeout.saturating_sub(self.elapsed());
        if remaining == Duration::ZERO { None } else { Some(remaining) }
    }

    /// Execute an async operation with timeout protection.
    ///
    /// The operation receives a `CancellationToken` that it can use to:
    /// - Check for cancellation: `token.is_cancelled()`
    /// - Pass to sub-tasks for propagation
    /// - Bind to futures: `future.until_cancelled(token)`
    ///
    /// # Returns
    ///
    /// - `TimedGetObjectResult::Success(T)` if the operation completed within timeout
    /// - `TimedGetObjectResult::Timeout(TimeoutInfo)` if the operation timed out
    /// - `TimedGetObjectResult::Error(E)` if the operation failed
    pub async fn execute_with_timeout<F, Fut, T, E>(self, operation: F) -> TimedGetObjectResult<T, E>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        if !self.config.is_timeout_enabled() {
            // Timeout disabled, run without timeout
            debug!(
                request_id = %self.request_id,
                "Timeout disabled, executing operation without timeout"
            );
            return match operation(self.cancel_token).await {
                Ok(result) => TimedGetObjectResult::Success(result),
                Err(e) => TimedGetObjectResult::Error(e),
            };
        }

        let timeout_duration = self.config.get_object_timeout;
        let request_id = self.request_id.clone();
        let start_time = self.start_time;

        debug!(
            request_id = %request_id,
            timeout_secs = timeout_duration.as_secs(),
            "Starting timed operation"
        );

        // Record start time for metrics
        #[cfg(feature = "metrics")]
        counter!("rustfs.get.object.requests.started").increment(1);

        // Clone cancel_token for the operation, keep original for potential cancellation
        let cancel_token_for_op = self.cancel_token.clone();

        match tokio::time::timeout(timeout_duration, operation(cancel_token_for_op)).await {
            Ok(Ok(result)) => {
                // Operation completed successfully
                let elapsed = start_time.elapsed();

                #[cfg(feature = "metrics")]
                {
                    counter!("rustfs.get.object.requests.completed").increment(1);
                    histogram!("rustfs.get.object.duration.seconds").record(elapsed.as_secs_f64());
                }

                debug!(
                    request_id = %request_id,
                    elapsed_ms = elapsed.as_millis(),
                    "Operation completed successfully"
                );

                TimedGetObjectResult::Success(result)
            }
            Ok(Err(e)) => {
                // Operation failed before timeout
                let elapsed = start_time.elapsed();

                #[cfg(feature = "metrics")]
                {
                    counter!("rustfs.get.object.requests.failed").increment(1);
                    histogram!("rustfs.get.object.duration.seconds").record(elapsed.as_secs_f64());
                }

                debug!(
                    request_id = %request_id,
                    elapsed_ms = elapsed.as_millis(),
                    "Operation failed with error"
                );

                TimedGetObjectResult::Error(e)
            }
            Err(_) => {
                // Timeout occurred
                let elapsed = start_time.elapsed();

                // Cancel the operation
                self.cancel_token.cancel();

                #[cfg(feature = "metrics")]
                {
                    counter!("rustfs.get.object.timeout.total").increment(1);
                    histogram!("rustfs.get.object.duration.seconds").record(elapsed.as_secs_f64());
                }

                warn!(
                    request_id = %request_id,
                    timeout_secs = timeout_duration.as_secs(),
                    elapsed_ms = elapsed.as_millis(),
                    "Operation timed out, cancellation signal sent"
                );

                TimedGetObjectResult::Timeout(TimeoutInfo {
                    request_id,
                    bucket: String::new(),
                    key: String::new(),
                    timeout_duration,
                    elapsed,
                    bytes_transferred: 0,
                    lock_hold_time: None,
                    disk_reads_completed: 0,
                    disk_reads_pending: 0,
                })
            }
        }
    }

    /// Execute an async operation with timeout and context information.
    ///
    /// This is an extended version of `execute_with_timeout` that includes
    /// bucket and key information for better timeout logging.
    pub async fn execute_with_timeout_and_context<F, Fut, T, E>(
        self,
        bucket: impl Into<String>,
        key: impl Into<String>,
        operation: F,
    ) -> TimedGetObjectResult<T, E>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let bucket = bucket.into();
        let key = key.into();

        if !self.config.is_timeout_enabled() {
            debug!(
                request_id = %self.request_id,
                bucket = %bucket,
                key = %key,
                "Timeout disabled, executing operation without timeout"
            );
            return match operation(self.cancel_token).await {
                Ok(result) => TimedGetObjectResult::Success(result),
                Err(e) => TimedGetObjectResult::Error(e),
            };
        }

        let timeout_duration = self.config.get_object_timeout;
        let request_id = self.request_id.clone();
        let start_time = self.start_time;

        debug!(
            request_id = %request_id,
            bucket = %bucket,
            key = %key,
            timeout_secs = timeout_duration.as_secs(),
            "Starting timed operation"
        );

        #[cfg(feature = "metrics")]
        counter!("rustfs.get.object.requests.started").increment(1);

        // Clone cancel_token for the operation, keep original for potential cancellation
        let cancel_token_for_op = self.cancel_token.clone();

        match tokio::time::timeout(timeout_duration, operation(cancel_token_for_op)).await {
            Ok(Ok(result)) => {
                let elapsed = start_time.elapsed();

                #[cfg(feature = "metrics")]
                {
                    counter!("rustfs.get.object.requests.completed").increment(1);
                    histogram!("rustfs.get.object.duration.seconds").record(elapsed.as_secs_f64());
                }

                debug!(
                    request_id = %request_id,
                    bucket = %bucket,
                    key = %key,
                    elapsed_ms = elapsed.as_millis(),
                    "Operation completed successfully"
                );

                TimedGetObjectResult::Success(result)
            }
            Ok(Err(e)) => {
                let elapsed = start_time.elapsed();

                #[cfg(feature = "metrics")]
                {
                    counter!("rustfs.get.object.requests.failed").increment(1);
                    histogram!("rustfs.get.object.duration.seconds").record(elapsed.as_secs_f64());
                }

                debug!(
                    request_id = %request_id,
                    bucket = %bucket,
                    key = %key,
                    elapsed_ms = elapsed.as_millis(),
                    "Operation failed with error"
                );

                TimedGetObjectResult::Error(e)
            }
            Err(_) => {
                let elapsed = start_time.elapsed();
                self.cancel_token.cancel();

                #[cfg(feature = "metrics")]
                {
                    counter!("rustfs.get.object.timeout.total").increment(1);
                    histogram!("rustfs.get.object.duration.seconds").record(elapsed.as_secs_f64());
                }

                warn!(
                    request_id = %request_id,
                    bucket = %bucket,
                    key = %key,
                    timeout_secs = timeout_duration.as_secs(),
                    elapsed_ms = elapsed.as_millis(),
                    "Operation timed out, cancellation signal sent"
                );

                TimedGetObjectResult::Timeout(TimeoutInfo {
                    request_id,
                    bucket,
                    key,
                    timeout_duration,
                    elapsed,
                    bytes_transferred: 0,
                    lock_hold_time: None,
                    disk_reads_completed: 0,
                    disk_reads_pending: 0,
                })
            }
        }
    }
}

/// Get the duplex buffer size from environment or default.
pub fn get_duplex_buffer_size() -> usize {
    rustfs_utils::get_env_usize(rustfs_config::ENV_DUPLEX_BUFFER_SIZE, rustfs_config::DEFAULT_DUPLEX_BUFFER_SIZE)
}

/// Get the I/O buffer size from environment or default.
pub fn get_io_buffer_size() -> usize {
    rustfs_utils::get_env_usize(rustfs_config::ENV_IO_BUFFER_SIZE, rustfs_config::DEFAULT_IO_BUFFER_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_timeout_config_default() {
        let config = TimeoutConfig::default();
        assert_eq!(config.get_object_timeout, Duration::from_secs(30));
        assert_eq!(config.lock_acquire_timeout, Duration::from_secs(5));
        assert_eq!(config.disk_read_timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_timeout_config_is_enabled() {
        let config = TimeoutConfig::default();
        assert!(config.is_timeout_enabled());

        let disabled_config = TimeoutConfig {
            get_object_timeout: Duration::ZERO,
            ..Default::default()
        };
        assert!(!disabled_config.is_timeout_enabled());
    }

    #[tokio::test]
    async fn test_timeout_wrapper_success() {
        let config = TimeoutConfig {
            get_object_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let wrapper = RequestTimeoutWrapper::new(config);

        let result = wrapper
            .execute_with_timeout(|_token| async move { Ok::<i32, String>(42) })
            .await;

        match result {
            TimedGetObjectResult::Success(value) => assert_eq!(value, 42),
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_timeout_wrapper_timeout() {
        let config = TimeoutConfig {
            get_object_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let wrapper = RequestTimeoutWrapper::new(config);

        let result = wrapper
            .execute_with_timeout(|_token| async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok::<i32, String>(42)
            })
            .await;

        match result {
            TimedGetObjectResult::Timeout(info) => {
                assert!(info.elapsed >= Duration::from_millis(100));
            }
            _ => panic!("Expected Timeout result"),
        }
    }

    #[tokio::test]
    async fn test_timeout_wrapper_error() {
        let config = TimeoutConfig {
            get_object_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let wrapper = RequestTimeoutWrapper::new(config);

        let result = wrapper
            .execute_with_timeout(|_token| async move { Err::<i32, String>("test error".to_string()) })
            .await;

        match result {
            TimedGetObjectResult::Error(e) => assert_eq!(e, "test error"),
            _ => panic!("Expected Error result"),
        }
    }

    #[tokio::test]
    async fn test_timeout_wrapper_disabled() {
        let config = TimeoutConfig {
            get_object_timeout: Duration::ZERO,
            ..Default::default()
        };
        let wrapper = RequestTimeoutWrapper::new(config);

        // This would timeout if timeout was enabled
        let result = wrapper
            .execute_with_timeout(|_token| async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok::<i32, String>(42)
            })
            .await;

        match result {
            TimedGetObjectResult::Success(value) => assert_eq!(value, 42),
            _ => panic!("Expected Success result when timeout is disabled"),
        }
    }

    #[test]
    fn test_get_duplex_buffer_size() {
        // Should return default (4MB) when env var not set
        let size = get_duplex_buffer_size();
        assert_eq!(size, 4 * 1024 * 1024);
    }

    #[test]
    fn test_get_io_buffer_size() {
        // Should return default (128KB) when env var not set
        let size = get_io_buffer_size();
        assert_eq!(size, 128 * 1024);
    }
}
