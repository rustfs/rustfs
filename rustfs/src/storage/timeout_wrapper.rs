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
//!
//! # Migration Note
//!
//! This module extends `rustfs_io_core::RequestTimeoutWrapper` with Tokio
//! cancellation token support. For basic timeout handling without async
//! cancellation, consider using the io-core version:
//!
//! ```ignore
//! // Basic timeout handling
//! use rustfs_io_core::RequestTimeoutWrapper;
//! let wrapper = RequestTimeoutWrapper::new(config);
//! ```
//!
//! # Key Features
//!
//! - Configurable request-level timeout (default 30 seconds)
//! - Automatic cancellation of sub-tasks on timeout
//! - Resource cleanup on timeout (locks, memory, file handles)
//! - Timeout metrics emitted through `rustfs-io-metrics`

// Allow dead_code for public API that may be used by external modules or future features
#![allow(dead_code)]
//!
//! - Configurable request-level timeout (default 30 seconds)
//! - Automatic cancellation of sub-tasks on timeout
//! - Resource cleanup on timeout (locks, memory, file handles)
//! - Timeout metrics emitted through `rustfs-io-metrics`
//!
//! # Usage
//!
//! ```ignore
//! use crate::storage::timeout_wrapper::{GetObjectTimeoutPolicy, RequestTimeoutWrapper};
//!
//! let config = GetObjectTimeoutPolicy::from_env();
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

use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use rustfs_io_core::{RequestTimeoutWrapper as CoreRequestTimeoutWrapper, TimeoutConfig as CoreTimeoutConfig};

/// Request-level timeout policy for GetObject.
#[derive(Debug, Clone)]
pub struct GetObjectTimeoutPolicy {
    /// GetObject request overall timeout (default 30s).
    /// After this duration, the request is cancelled and returns 504.
    pub get_object_timeout: Duration,

    /// Lock acquisition timeout (default 5s).
    /// Time to wait for a lock before giving up.
    pub lock_acquire_timeout: Duration,

    /// Disk read operation timeout (default 10s).
    /// Individual disk read operations that exceed this are cancelled.
    pub disk_read_timeout: Duration,

    /// Enable dynamic timeout calculation based on object size
    pub enable_dynamic_timeout: bool,

    /// Expected transfer speed in bytes per second for timeout estimation
    pub bytes_per_second: u64,

    /// Minimum timeout for dynamic calculation
    pub min_timeout: Duration,

    /// Maximum timeout for dynamic calculation
    pub max_timeout: Duration,
}

impl Default for GetObjectTimeoutPolicy {
    fn default() -> Self {
        Self {
            get_object_timeout: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_GET_TIMEOUT),
            lock_acquire_timeout: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_LOCK_ACQUIRE_TIMEOUT),
            disk_read_timeout: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_DISK_READ_TIMEOUT),
            enable_dynamic_timeout: rustfs_config::DEFAULT_OBJECT_DYNAMIC_TIMEOUT_ENABLE,
            bytes_per_second: rustfs_config::DEFAULT_OBJECT_BYTES_PER_SECOND,
            min_timeout: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_MIN_TIMEOUT),
            max_timeout: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_MAX_TIMEOUT),
        }
    }
}

impl GetObjectTimeoutPolicy {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let get_object_timeout =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_GET_TIMEOUT, rustfs_config::DEFAULT_OBJECT_GET_TIMEOUT);
        let lock_acquire_timeout = rustfs_utils::get_env_u64(
            rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT,
            rustfs_config::DEFAULT_OBJECT_LOCK_ACQUIRE_TIMEOUT,
        );
        let disk_read_timeout = rustfs_utils::get_env_u64(
            rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT,
            rustfs_config::DEFAULT_OBJECT_DISK_READ_TIMEOUT,
        );

        // Dynamic timeout settings
        let enable_dynamic_timeout = rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_DYNAMIC_TIMEOUT_ENABLE,
            rustfs_config::DEFAULT_OBJECT_DYNAMIC_TIMEOUT_ENABLE,
        );
        let bytes_per_second =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_BYTES_PER_SECOND, rustfs_config::DEFAULT_OBJECT_BYTES_PER_SECOND);
        let min_timeout_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_MIN_TIMEOUT, rustfs_config::DEFAULT_OBJECT_MIN_TIMEOUT);
        let max_timeout_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_MAX_TIMEOUT, rustfs_config::DEFAULT_OBJECT_MAX_TIMEOUT);

        Self {
            get_object_timeout: Duration::from_secs(get_object_timeout),
            lock_acquire_timeout: Duration::from_secs(lock_acquire_timeout),
            disk_read_timeout: Duration::from_secs(disk_read_timeout),
            enable_dynamic_timeout,
            bytes_per_second,
            min_timeout: Duration::from_secs(min_timeout_secs),
            max_timeout: Duration::from_secs(max_timeout_secs),
        }
    }

    /// Check if timeout is enabled (timeout > 0).
    pub fn is_timeout_enabled(&self) -> bool {
        self.get_object_timeout > Duration::ZERO
    }

    /// Calculate dynamic timeout based on object size
    pub fn calculate_timeout_for_size(&self, object_size: u64) -> Duration {
        if !self.enable_dynamic_timeout {
            return self.get_object_timeout;
        }

        // Keep storage-layer dynamic-timeout semantics local so request policy
        // bounds remain authoritative. We preserve the historical 1.5x envelope:
        // 80% effective rate * 1.2 safety margin.
        let effective_rate_bps = self.bytes_per_second.saturating_mul(4).saturating_div(5).max(1);
        let estimated_secs = (object_size as f64 / effective_rate_bps as f64) * 1.2;
        let estimated_duration = Duration::from_secs_f64(estimated_secs);

        // Clamp to min/max bounds
        estimated_duration
            .max(self.min_timeout)
            .min(self.max_timeout)
            .min(self.get_object_timeout) // Never exceed configured timeout
    }

    /// Get appropriate timeout for a given operation
    pub fn get_timeout_for_operation(&self, operation_size: Option<u64>) -> Duration {
        match operation_size {
            Some(size) if self.enable_dynamic_timeout && size > 0 => self.calculate_timeout_for_size(size),
            _ => self.get_object_timeout,
        }
    }

    /// Convert the request-level policy into a core timeout configuration.
    ///
    /// This is intentionally lossy: request-specific diagnostics remain in the
    /// storage layer, while the shared timing primitive lives in `io-core`.
    pub fn to_core_config(&self) -> CoreTimeoutConfig {
        CoreTimeoutConfig {
            base_timeout: self.get_object_timeout,
            timeout_per_mb: Duration::ZERO,
            max_timeout: self.max_timeout,
            min_timeout: self.min_timeout,
            get_object_timeout: self.get_object_timeout,
            put_object_timeout: self.get_object_timeout,
            list_objects_timeout: self.get_object_timeout,
            enable_dynamic_timeout: false,
        }
    }
}

/// Backward-compatible alias for the old request timeout name.
#[deprecated(note = "use GetObjectTimeoutPolicy instead")]
pub type TimeoutConfig = GetObjectTimeoutPolicy;

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
    /// Object size (if known)
    pub object_size: Option<u64>,
    /// Progress percentage (0-100)
    pub progress_percent: Option<f32>,
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
pub struct RequestTimeoutWrapper {
    /// Configuration.
    config: GetObjectTimeoutPolicy,
    /// Shared core timing primitive.
    core_wrapper: CoreRequestTimeoutWrapper,
    /// Optional operation size hint for dynamic timeout decisions.
    operation_size: Option<u64>,
    /// Cancellation token for propagating cancellation to sub-tasks.
    cancel_token: CancellationToken,
    /// Request ID for logging/metrics.
    request_id: String,
}

impl std::fmt::Debug for RequestTimeoutWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestTimeoutWrapper")
            .field("config", &self.config)
            .field("elapsed", &self.elapsed())
            .field("request_id", &self.request_id)
            .finish()
    }
}

impl RequestTimeoutWrapper {
    /// Create a new timeout wrapper with the given configuration.
    ///
    /// Note: This uses a sentinel request_id. Prefer `with_request_id()` to pass
    /// the canonical request-id from `RequestContext`.
    pub fn new(config: GetObjectTimeoutPolicy) -> Self {
        Self::new_with_parts(config, None, CancellationToken::new(), "no-request-id".to_string())
    }

    /// Create a new timeout wrapper with a specific request ID.
    pub fn with_request_id(config: GetObjectTimeoutPolicy, request_id: impl Into<String>) -> Self {
        Self::new_with_parts(config, None, CancellationToken::new(), request_id.into())
    }

    /// Create a new timeout wrapper with operation size for dynamic timeout calculation.
    ///
    /// Note: This uses a sentinel request_id. Prefer `with_request_id()` to pass
    /// the canonical request-id from `RequestContext`.
    pub fn with_operation_size(config: GetObjectTimeoutPolicy, operation_size: Option<u64>) -> Self {
        Self::new_with_parts(config, operation_size, CancellationToken::new(), "no-request-id".to_string())
    }

    /// Get the configured timeout for this operation
    pub fn get_timeout(&self, operation_size_hint: Option<u64>) -> Duration {
        self.config
            .get_timeout_for_operation(self.effective_operation_size(operation_size_hint))
    }

    fn new_with_parts(
        config: GetObjectTimeoutPolicy,
        operation_size: Option<u64>,
        cancel_token: CancellationToken,
        request_id: String,
    ) -> Self {
        let core_wrapper = CoreRequestTimeoutWrapper::new(config.to_core_config());
        Self {
            config,
            core_wrapper,
            operation_size,
            cancel_token,
            request_id,
        }
    }

    fn effective_operation_size(&self, operation_size_hint: Option<u64>) -> Option<u64> {
        operation_size_hint.or(self.operation_size)
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
        self.config.is_timeout_enabled() && self.core_wrapper.elapsed() >= self.get_timeout(None)
    }

    /// Get elapsed time since the request started.
    pub fn elapsed(&self) -> Duration {
        self.core_wrapper.elapsed()
    }

    /// Get remaining time before timeout.
    /// Returns None if timeout is disabled or already exceeded.
    pub fn remaining_time(&self) -> Option<Duration> {
        self.remaining_time_for_size(None)
    }

    /// Get remaining time before timeout for a specific operation size.
    pub fn remaining_time_for_size(&self, operation_size: Option<u64>) -> Option<Duration> {
        if !self.config.is_timeout_enabled() {
            return None;
        }
        let timeout = self
            .config
            .get_timeout_for_operation(self.effective_operation_size(operation_size));
        self.core_wrapper.remaining(timeout)
    }

    /// Check if the wrapper should timeout based on elapsed time and optional operation size
    pub fn should_timeout(&self, operation_size: Option<u64>) -> bool {
        if !self.config.is_timeout_enabled() {
            return false;
        }
        let timeout = self
            .config
            .get_timeout_for_operation(self.effective_operation_size(operation_size));
        self.core_wrapper.elapsed() >= timeout
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
        self.execute_with_timeout_internal(None, operation).await
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
        self.execute_with_timeout_internal(Some((bucket, key)), operation).await
    }

    async fn execute_with_timeout_internal<F, Fut, T, E>(
        self,
        context: Option<(String, String)>,
        operation: F,
    ) -> TimedGetObjectResult<T, E>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let timeout_duration = self.get_timeout(None);
        let context_refs = context.as_ref().map(|(bucket, key)| (bucket.as_str(), key.as_str()));

        if !self.config.is_timeout_enabled() {
            if let Some((bucket, key)) = context_refs {
                debug!(
                    request_id = %self.request_id,
                    bucket = %bucket,
                    key = %key,
                    "Timeout disabled, executing operation without timeout"
                );
            } else {
                debug!(
                    request_id = %self.request_id,
                    "Timeout disabled, executing operation without timeout"
                );
            }

            return match operation(self.cancel_token).await {
                Ok(result) => TimedGetObjectResult::Success(result),
                Err(e) => TimedGetObjectResult::Error(e),
            };
        }

        if let Some((bucket, key)) = context_refs {
            debug!(
                request_id = %self.request_id,
                bucket = %bucket,
                key = %key,
                timeout_secs = timeout_duration.as_secs(),
                "Starting timed operation"
            );
        } else {
            debug!(
                request_id = %self.request_id,
                timeout_secs = timeout_duration.as_secs(),
                "Starting timed operation"
            );
        }

        rustfs_io_metrics::record_get_object_request_started();

        let cancel_token_for_op = self.cancel_token.clone();

        match tokio::time::timeout(timeout_duration, operation(cancel_token_for_op)).await {
            Ok(Ok(result)) => {
                let elapsed = self.elapsed();
                rustfs_io_metrics::record_get_object_request_result("success", elapsed.as_secs_f64());

                if let Some((bucket, key)) = context_refs {
                    debug!(
                        request_id = %self.request_id,
                        bucket = %bucket,
                        key = %key,
                        elapsed_ms = elapsed.as_millis(),
                        "Operation completed successfully"
                    );
                } else {
                    debug!(
                        request_id = %self.request_id,
                        elapsed_ms = elapsed.as_millis(),
                        "Operation completed successfully"
                    );
                }

                TimedGetObjectResult::Success(result)
            }
            Ok(Err(e)) => {
                let elapsed = self.elapsed();
                rustfs_io_metrics::record_get_object_request_result("error", elapsed.as_secs_f64());

                if let Some((bucket, key)) = context_refs {
                    debug!(
                        request_id = %self.request_id,
                        bucket = %bucket,
                        key = %key,
                        elapsed_ms = elapsed.as_millis(),
                        "Operation failed with error"
                    );
                } else {
                    debug!(
                        request_id = %self.request_id,
                        elapsed_ms = elapsed.as_millis(),
                        "Operation failed with error"
                    );
                }

                TimedGetObjectResult::Error(e)
            }
            Err(_) => {
                let elapsed = self.elapsed();
                self.cancel_token.cancel();

                rustfs_io_metrics::record_get_object_timeout(None, Some(elapsed.as_secs_f64()));
                rustfs_io_metrics::record_get_object_request_result("timeout", elapsed.as_secs_f64());

                if let Some((bucket, key)) = context_refs {
                    warn!(
                        request_id = %self.request_id,
                        bucket = %bucket,
                        key = %key,
                        timeout_secs = timeout_duration.as_secs(),
                        elapsed_ms = elapsed.as_millis(),
                        "Operation timed out, cancellation signal sent"
                    );
                } else {
                    warn!(
                        request_id = %self.request_id,
                        timeout_secs = timeout_duration.as_secs(),
                        elapsed_ms = elapsed.as_millis(),
                        "Operation timed out, cancellation signal sent"
                    );
                }

                let (bucket, key) = context.unwrap_or_default();
                TimedGetObjectResult::Timeout(TimeoutInfo {
                    request_id: self.request_id,
                    bucket,
                    key,
                    timeout_duration,
                    elapsed,
                    bytes_transferred: 0,
                    lock_hold_time: None,
                    disk_reads_completed: 0,
                    disk_reads_pending: 0,
                    object_size: self.operation_size,
                    progress_percent: None,
                })
            }
        }
    }
}

/// Get the duplex buffer size from environment or default.
pub fn get_duplex_buffer_size() -> usize {
    rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_DUPLEX_BUFFER_SIZE,
        rustfs_config::DEFAULT_OBJECT_DUPLEX_BUFFER_SIZE,
    )
}

/// Get the I/O buffer size from environment or default.
pub fn get_io_buffer_size() -> usize {
    rustfs_utils::get_env_usize(rustfs_config::ENV_OBJECT_IO_BUFFER_SIZE, rustfs_config::DEFAULT_OBJECT_IO_BUFFER_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_timeout_config_default() {
        let config = GetObjectTimeoutPolicy::default();
        assert_eq!(config.get_object_timeout, Duration::from_secs(30));
        assert_eq!(config.lock_acquire_timeout, Duration::from_secs(5));
        assert_eq!(config.disk_read_timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_timeout_config_is_enabled() {
        let config = GetObjectTimeoutPolicy::default();
        assert!(config.is_timeout_enabled());

        let disabled_config = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::ZERO,
            ..Default::default()
        };
        assert!(!disabled_config.is_timeout_enabled());
    }

    #[tokio::test]
    async fn test_timeout_wrapper_success() {
        let config = GetObjectTimeoutPolicy {
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
        let config = GetObjectTimeoutPolicy {
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
        let config = GetObjectTimeoutPolicy {
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
        let config = GetObjectTimeoutPolicy {
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

    #[test]
    fn test_timeout_config_default_with_dynamic() {
        let config = GetObjectTimeoutPolicy::default();
        assert!(config.enable_dynamic_timeout);
        assert_eq!(config.bytes_per_second, rustfs_config::DEFAULT_OBJECT_BYTES_PER_SECOND);
        assert_eq!(config.min_timeout, Duration::from_secs(rustfs_config::DEFAULT_OBJECT_MIN_TIMEOUT));
        assert_eq!(config.max_timeout, Duration::from_secs(rustfs_config::DEFAULT_OBJECT_MAX_TIMEOUT));
    }

    #[test]
    fn test_calculate_timeout_for_size() {
        let config = GetObjectTimeoutPolicy::default();

        // Test with small object (should use min timeout)
        let small_timeout = config.calculate_timeout_for_size(1024); // 1KB
        assert_eq!(small_timeout, Duration::from_secs(rustfs_config::DEFAULT_OBJECT_MIN_TIMEOUT));

        // Test with large object
        let large_timeout = config.calculate_timeout_for_size(10 * 1024 * 1024); // 10MB
        // At 1MB/s with 50% buffer: 10MB / 1MB/s * 1.5 = 15 seconds
        assert!(large_timeout >= Duration::from_secs(14));
        assert!(large_timeout <= Duration::from_secs(16));

        // Test with very large object (should cap at max_timeout)
        let huge_timeout = config.calculate_timeout_for_size(1000 * 1024 * 1024); // 1GB
        assert!(huge_timeout <= Duration::from_secs(rustfs_config::DEFAULT_OBJECT_MAX_TIMEOUT));
    }

    #[test]
    fn test_calculate_timeout_for_size_respects_small_min_timeout() {
        let config = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::from_secs(30),
            enable_dynamic_timeout: true,
            bytes_per_second: 1024 * 1024, // 1MB/s
            min_timeout: Duration::from_secs(1),
            max_timeout: Duration::from_secs(30),
            ..Default::default()
        };

        // Tiny object should still honor policy min_timeout (1s),
        // instead of being raised by any external hard floor.
        let timeout = config.calculate_timeout_for_size(1024); // 1KB
        assert_eq!(timeout, Duration::from_secs(1));
    }

    #[test]
    fn test_timeout_with_dynamic_disabled() {
        let config = GetObjectTimeoutPolicy {
            enable_dynamic_timeout: false,
            ..Default::default()
        };

        // Should use base timeout regardless of size
        let timeout1 = config.get_timeout_for_operation(Some(1024));
        let timeout2 = config.get_timeout_for_operation(Some(100 * 1024 * 1024));

        assert_eq!(timeout1, config.get_object_timeout);
        assert_eq!(timeout2, config.get_object_timeout);
    }
    use rustfs_concurrency::OperationProgress;
    #[test]
    fn test_operation_progress_new() {
        let progress = OperationProgress::new(Some(1000), Duration::from_secs(5));
        assert_eq!(progress.current(), 0);
        progress.update(500);
        assert_eq!(progress.current(), 500);
        assert!(!progress.is_stale());

        // Simulate time passing
        std::thread::sleep(Duration::from_millis(100));
        progress.update(1000);
        assert_eq!(progress.current(), 1000);
    }
    #[test]
    fn test_operation_progress_stale() {
        let progress = OperationProgress::new(Some(1000), Duration::from_millis(100));

        progress.update(500);
        assert!(!progress.is_stale());

        // Wait for stale timeout
        std::thread::sleep(Duration::from_millis(150));
        assert!(progress.is_stale());

        // Update should clear stale status
        progress.update(600);
        assert!(!progress.is_stale());
    }

    #[test]
    fn test_operation_progress_percent() {
        let progress = OperationProgress::new(Some(1000), Duration::from_secs(5));

        assert_eq!(progress.progress_percent(), Some(0.0));

        progress.update(500);
        assert_eq!(progress.progress_percent(), Some(50.0));

        progress.update(1000);
        assert_eq!(progress.progress_percent(), Some(100.0));
    }

    #[test]
    fn test_operation_progress_no_total_size() {
        let progress = OperationProgress::new(None, Duration::from_secs(5));
        assert_eq!(progress.progress_percent(), None);
    }

    #[test]
    fn test_operation_progress_zero_size() {
        let progress = OperationProgress::new(Some(0), Duration::from_secs(5));
        assert_eq!(progress.progress_percent(), Some(100.0));
    }

    #[test]
    fn test_should_timeout() {
        let config = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::from_millis(100),
            ..Default::default()
        };

        let wrapper = RequestTimeoutWrapper::new(config);

        // Should not timeout immediately
        assert!(!wrapper.should_timeout(None));

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(150));
        assert!(wrapper.should_timeout(None));
    }

    #[test]
    fn test_should_timeout_with_size() {
        let config = GetObjectTimeoutPolicy {
            enable_dynamic_timeout: true,
            bytes_per_second: 1024, // 1KB/s
            min_timeout: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_MIN_TIMEOUT),
            max_timeout: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_MAX_TIMEOUT),
            ..Default::default()
        };

        let wrapper = RequestTimeoutWrapper::new(config);

        // Small size should use min timeout
        assert!(!wrapper.should_timeout(Some(1024)));

        // Large size should calculate longer timeout
        assert!(!wrapper.should_timeout(Some(10 * 1024 * 1024)));
    }

    #[test]
    fn test_wrapper_operation_size_hint_is_applied() {
        let config = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::from_secs(300),
            enable_dynamic_timeout: true,
            bytes_per_second: 1024 * 1024,
            min_timeout: Duration::from_secs(1),
            max_timeout: Duration::from_secs(300),
            ..Default::default()
        };
        let size = 100 * 1024 * 1024;
        let wrapper = RequestTimeoutWrapper::with_operation_size(config.clone(), Some(size));

        let expected_dynamic = config.get_timeout_for_operation(Some(size));
        let baseline_no_size = config.get_timeout_for_operation(None);

        assert_ne!(expected_dynamic, baseline_no_size);
        assert_eq!(wrapper.get_timeout(None), expected_dynamic);
    }
}
