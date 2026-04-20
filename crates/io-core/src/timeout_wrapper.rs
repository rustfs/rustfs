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

//! Timeout wrapper for I/O operations.
//!
//! This module provides timeout management for I/O operations with
//! dynamic timeout calculation based on operation size.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Timeout configuration.
#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    /// Base timeout for small operations.
    pub base_timeout: Duration,
    /// Timeout per MB of data.
    pub timeout_per_mb: Duration,
    /// Maximum timeout.
    pub max_timeout: Duration,
    /// Minimum timeout.
    pub min_timeout: Duration,
    /// GetObject operation timeout.
    pub get_object_timeout: Duration,
    /// PutObject operation timeout.
    pub put_object_timeout: Duration,
    /// ListObjects operation timeout.
    pub list_objects_timeout: Duration,
    /// Whether dynamic timeout is enabled.
    pub enable_dynamic_timeout: bool,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            base_timeout: Duration::from_secs(5),
            timeout_per_mb: Duration::from_millis(100),
            max_timeout: Duration::from_secs(300),
            min_timeout: Duration::from_secs(1),
            get_object_timeout: Duration::from_secs(30),
            put_object_timeout: Duration::from_secs(60),
            list_objects_timeout: Duration::from_secs(10),
            enable_dynamic_timeout: true,
        }
    }
}

impl TimeoutConfig {
    /// Create new timeout configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculate dynamic timeout based on size.
    pub fn calculate_timeout(&self, size_bytes: u64) -> Duration {
        if !self.enable_dynamic_timeout {
            return self.base_timeout;
        }

        let mb = size_bytes as f64 / (1024.0 * 1024.0);
        let timeout = self.base_timeout + self.timeout_per_mb.mul_f64(mb);
        timeout.clamp(self.min_timeout, self.max_timeout)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), TimeoutError> {
        if self.min_timeout > self.max_timeout {
            return Err(TimeoutError::InvalidConfig("min_timeout must be <= max_timeout".to_string()));
        }
        if self.base_timeout < self.min_timeout || self.base_timeout > self.max_timeout {
            return Err(TimeoutError::InvalidConfig(
                "base_timeout must be between min_timeout and max_timeout".to_string(),
            ));
        }
        Ok(())
    }
}

/// Timeout error.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TimeoutError {
    /// Operation timed out.
    #[error("Operation timed out after {0:?}")]
    TimedOut(Duration),
    /// Invalid configuration.
    #[error("Invalid timeout config: {0}")]
    InvalidConfig(String),
}

/// Operation progress tracker.
#[derive(Debug)]
pub struct OperationProgress {
    /// Total size (if known).
    pub total_size: Option<u64>,
    /// Bytes processed.
    bytes_processed: AtomicU64,
    /// Last update time.
    last_update: std::sync::Mutex<Instant>,
    /// Stale timeout.
    stale_timeout: Duration,
    /// Start time for transfer rate calculation.
    start_time: Instant,
}

impl OperationProgress {
    /// Create new operation progress.
    pub fn new(total_size: Option<u64>, stale_timeout: Duration) -> Self {
        Self {
            total_size,
            bytes_processed: AtomicU64::new(0),
            last_update: std::sync::Mutex::new(Instant::now()),
            stale_timeout,
            start_time: Instant::now(),
        }
    }

    /// Update progress.
    pub fn update(&self, bytes: u64) {
        self.bytes_processed.store(bytes, Ordering::Relaxed);
        if let Ok(mut last) = self.last_update.lock() {
            *last = Instant::now();
        }
    }

    /// Add to progress.
    pub fn add(&self, bytes: u64) {
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
        if let Ok(mut last) = self.last_update.lock() {
            *last = Instant::now();
        }
    }

    /// Get current progress.
    pub fn current(&self) -> u64 {
        self.bytes_processed.load(Ordering::Relaxed)
    }

    /// Check if progress is stale.
    pub fn is_stale(&self) -> bool {
        if let Ok(last) = self.last_update.lock() {
            last.elapsed() > self.stale_timeout
        } else {
            false
        }
    }

    /// Get progress percentage.
    pub fn progress_percent(&self) -> Option<f64> {
        self.total_size.map(|total| {
            if total == 0 {
                100.0
            } else {
                let processed = self.bytes_processed.load(Ordering::Relaxed);
                (processed as f64 / total as f64 * 100.0).min(100.0)
            }
        })
    }

    /// Get remaining bytes.
    pub fn remaining(&self) -> Option<u64> {
        self.total_size.map(|total| {
            let processed = self.bytes_processed.load(Ordering::Relaxed);
            total.saturating_sub(processed)
        })
    }

    /// Calculate transfer rate in bytes per second.
    ///
    /// Returns 0 if no time has elapsed or no data transferred.
    pub fn transfer_rate(&self) -> u64 {
        let processed = self.bytes_processed.load(Ordering::Relaxed);
        if processed == 0 {
            return 0;
        }

        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            (processed as f64 / elapsed) as u64
        } else {
            0
        }
    }
}

/// Request timeout wrapper.
pub struct RequestTimeoutWrapper {
    /// Configuration.
    config: TimeoutConfig,
    /// Start time.
    start_time: Instant,
    /// Operation progress.
    progress: Option<OperationProgress>,
}

impl RequestTimeoutWrapper {
    /// Create a new timeout wrapper.
    pub fn new(config: TimeoutConfig) -> Self {
        Self {
            config,
            start_time: Instant::now(),
            progress: None,
        }
    }

    /// Create with progress tracking.
    pub fn with_progress(config: TimeoutConfig, total_size: Option<u64>, stale_timeout: Duration) -> Self {
        Self {
            config,
            start_time: Instant::now(),
            progress: Some(OperationProgress::new(total_size, stale_timeout)),
        }
    }

    /// Get the configuration.
    pub fn config(&self) -> &TimeoutConfig {
        &self.config
    }

    /// Get elapsed time.
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get remaining time.
    pub fn remaining(&self, timeout: Duration) -> Option<Duration> {
        let elapsed = self.elapsed();
        if elapsed >= timeout { None } else { Some(timeout - elapsed) }
    }

    /// Check if timed out.
    pub fn is_timed_out(&self, size: Option<u64>) -> bool {
        let timeout = self.get_timeout(size);
        self.elapsed() > timeout
    }

    /// Get the timeout for a given size.
    pub fn get_timeout(&self, size: Option<u64>) -> Duration {
        if self.config.enable_dynamic_timeout {
            if let Some(s) = size {
                self.config.calculate_timeout(s)
            } else {
                self.config.base_timeout
            }
        } else {
            self.config.base_timeout
        }
    }

    /// Check if timed out and return error if so.
    pub fn check_timeout(&self, size: Option<u64>) -> Result<(), TimeoutError> {
        if self.is_timed_out(size) {
            Err(TimeoutError::TimedOut(self.get_timeout(size)))
        } else {
            Ok(())
        }
    }

    /// Get progress.
    pub fn progress(&self) -> Option<&OperationProgress> {
        self.progress.as_ref()
    }

    /// Update progress.
    pub fn update_progress(&self, bytes: u64) {
        if let Some(ref progress) = self.progress {
            progress.update(bytes);
        }
    }

    /// Check if operation is stalled (no progress for a while).
    pub fn is_stalled(&self) -> bool {
        self.progress.as_ref().is_some_and(|p| p.is_stale())
    }

    /// Get progress percentage.
    pub fn progress_percent(&self) -> Option<f64> {
        self.progress.as_ref().and_then(|p| p.progress_percent())
    }
}

/// Timeout statistics.
#[derive(Debug, Default)]
pub struct TimeoutStats {
    /// Total operations.
    pub total_operations: AtomicU64,
    /// Timed out operations.
    pub timed_out: AtomicU64,
    /// Total wait time in nanoseconds.
    pub total_wait_time_ns: AtomicU64,
    /// Maximum wait time in nanoseconds.
    pub max_wait_time_ns: AtomicU64,
}

impl TimeoutStats {
    /// Create new timeout statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an operation.
    pub fn record_operation(&self, wait_time: Duration) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        let ns = wait_time.as_nanos() as u64;
        self.total_wait_time_ns.fetch_add(ns, Ordering::Relaxed);

        let mut current = self.max_wait_time_ns.load(Ordering::Relaxed);
        while ns > current {
            match self
                .max_wait_time_ns
                .compare_exchange_weak(current, ns, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record a timeout.
    pub fn record_timeout(&self) {
        self.timed_out.fetch_add(1, Ordering::Relaxed);
    }

    /// Get timeout rate.
    pub fn timeout_rate(&self) -> f64 {
        let total = self.total_operations.load(Ordering::Relaxed);
        let timed_out = self.timed_out.load(Ordering::Relaxed);
        if total == 0 { 0.0 } else { timed_out as f64 / total as f64 }
    }

    /// Get average wait time.
    pub fn avg_wait_time(&self) -> Duration {
        let total = self.total_wait_time_ns.load(Ordering::Relaxed);
        let count = self.total_operations.load(Ordering::Relaxed);
        total.checked_div(count).map(Duration::from_nanos).unwrap_or(Duration::ZERO)
    }

    /// Reset statistics.
    pub fn reset(&self) {
        self.total_operations.store(0, Ordering::Relaxed);
        self.timed_out.store(0, Ordering::Relaxed);
        self.total_wait_time_ns.store(0, Ordering::Relaxed);
        self.max_wait_time_ns.store(0, Ordering::Relaxed);
    }
}

/// Calculate adaptive timeout based on historical data and current conditions.
///
/// This function adjusts the timeout based on:
/// - Historical transfer rate
/// - Recent timeout count
/// - Object size
pub fn calculate_adaptive_timeout(
    base_timeout: Duration,
    historical_rate_bps: Option<u64>,
    recent_timeout_count: u32,
    object_size: u64,
) -> Duration {
    // If we have recent timeouts, increase timeout
    let timeout_multiplier = if recent_timeout_count > 3 {
        2.0 // Double timeout if many recent timeouts
    } else if recent_timeout_count > 1 {
        1.5 // 50% increase if some timeouts
    } else {
        1.0 // No adjustment
    };

    // If we have historical rate data, use it for estimation
    let estimated_duration = if let Some(rate) = historical_rate_bps {
        if rate > 0 {
            let estimated_secs = (object_size as f64 / rate as f64) * 1.2; // 20% buffer
            Duration::from_secs_f64(estimated_secs)
        } else {
            base_timeout
        }
    } else {
        base_timeout
    };

    // Apply timeout multiplier but clamp to reasonable bounds
    let adaptive_duration = Duration::from_secs_f64(estimated_duration.as_secs_f64() * timeout_multiplier);

    // Clamp to 5 seconds minimum and 10 minutes maximum
    adaptive_duration.clamp(Duration::from_secs(5), Duration::from_secs(600))
}

/// Estimate bytes per second transfer rate.
///
/// This is used for adaptive timeout calculation.
pub fn estimate_bytes_per_second(object_size: u64, expected_duration: Duration) -> u64 {
    let secs = expected_duration.as_secs_f64();
    if secs > 0.0 {
        (object_size as f64 / secs) as u64
    } else {
        // Return a reasonable default (1 MB/s)
        1024 * 1024
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeout_config() {
        let config = TimeoutConfig::default();
        assert!(config.validate().is_ok());

        // Small file
        let timeout = config.calculate_timeout(1024);
        assert!(timeout >= config.min_timeout);

        // Large file
        let timeout = config.calculate_timeout(100 * 1024 * 1024);
        assert!(timeout <= config.max_timeout);
    }

    #[test]
    fn test_timeout_config_validation() {
        let config = TimeoutConfig {
            min_timeout: Duration::from_secs(10),
            max_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_operation_progress() {
        let progress = OperationProgress::new(Some(1000), Duration::from_secs(5));

        assert_eq!(progress.current(), 0);
        assert_eq!(progress.progress_percent(), Some(0.0));

        progress.update(500);
        assert_eq!(progress.current(), 500);
        assert_eq!(progress.progress_percent(), Some(50.0));

        progress.add(300);
        assert_eq!(progress.current(), 800);
        assert_eq!(progress.remaining(), Some(200));
    }

    #[test]
    fn test_request_timeout_wrapper() {
        let config = TimeoutConfig {
            base_timeout: Duration::from_millis(100),
            enable_dynamic_timeout: false,
            ..Default::default()
        };
        let wrapper = RequestTimeoutWrapper::new(config);

        assert!(!wrapper.is_timed_out(None));

        std::thread::sleep(Duration::from_millis(150));

        assert!(wrapper.is_timed_out(None));
        assert!(wrapper.check_timeout(None).is_err());
    }

    #[test]
    fn test_timeout_stats() {
        let stats = TimeoutStats::new();

        stats.record_operation(Duration::from_millis(10));
        stats.record_operation(Duration::from_millis(20));
        stats.record_timeout();

        assert_eq!(stats.total_operations.load(Ordering::Relaxed), 2);
        assert_eq!(stats.timed_out.load(Ordering::Relaxed), 1);
        assert!((stats.timeout_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_progress_tracking() {
        let config = TimeoutConfig::default();
        let wrapper = RequestTimeoutWrapper::with_progress(config, Some(1000), Duration::from_secs(1));

        wrapper.update_progress(500);
        assert_eq!(wrapper.progress_percent(), Some(50.0));
        assert!(!wrapper.is_stalled());
    }
}
