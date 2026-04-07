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

//! Unified configuration interface for I/O operations.
//!
//! This module provides a centralized configuration interface
//! for all I/O-related settings.

use std::time::Duration;

// ============================================================================
// Configuration Constants
// ============================================================================

/// Default cache max capacity.
pub const DEFAULT_CACHE_MAX_CAPACITY: u64 = 10_000;
/// Default cache TTL in seconds.
pub const DEFAULT_CACHE_TTL_SECS: u64 = 300;
/// Default cache max memory in bytes (100 MB).
pub const DEFAULT_CACHE_MAX_MEMORY: u64 = 100 * 1024 * 1024;

/// Default I/O scheduler max concurrent reads.
pub const DEFAULT_MAX_CONCURRENT_READS: usize = 32;
/// Default high priority size threshold (64 KB).
pub const DEFAULT_HIGH_PRIORITY_SIZE_THRESHOLD: usize = 64 * 1024;
/// Default low priority size threshold (4 MB).
pub const DEFAULT_LOW_PRIORITY_SIZE_THRESHOLD: usize = 4 * 1024 * 1024;

/// Default backpressure high watermark.
pub const DEFAULT_BACKPRESSURE_HIGH_WATERMARK: f64 = 0.8;
/// Default backpressure low watermark.
pub const DEFAULT_BACKPRESSURE_LOW_WATERMARK: f64 = 0.5;

/// Default lock acquire timeout in seconds.
pub const DEFAULT_LOCK_ACQUIRE_TIMEOUT_SECS: u64 = 5;
/// Default deadlock detection interval in seconds.
pub const DEFAULT_DEADLOCK_DETECTION_INTERVAL_SECS: u64 = 1;

/// Default base buffer size (128 KB).
pub const DEFAULT_BASE_BUFFER_SIZE: usize = 128 * 1024;
/// Default max buffer size (1 MB).
pub const DEFAULT_MAX_BUFFER_SIZE: usize = 1024 * 1024;
/// Default min buffer size (4 KB).
pub const DEFAULT_MIN_BUFFER_SIZE: usize = 4 * 1024;

// ============================================================================
// Cache Configuration
// ============================================================================

/// Cache configuration settings.
#[derive(Debug, Clone)]
pub struct CacheSettings {
    /// Maximum cache capacity.
    pub max_capacity: u64,
    /// Default TTL.
    pub default_ttl: Duration,
    /// Maximum memory usage.
    pub max_memory: u64,
    /// Whether adaptive TTL is enabled.
    pub adaptive_ttl_enabled: bool,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            max_capacity: DEFAULT_CACHE_MAX_CAPACITY,
            default_ttl: Duration::from_secs(DEFAULT_CACHE_TTL_SECS),
            max_memory: DEFAULT_CACHE_MAX_MEMORY,
            adaptive_ttl_enabled: true,
        }
    }
}

impl CacheSettings {
    /// Create new cache settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder: set max capacity.
    pub fn with_max_capacity(mut self, capacity: u64) -> Self {
        self.max_capacity = capacity;
        self
    }

    /// Builder: set TTL.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Builder: set max memory.
    pub fn with_max_memory(mut self, memory: u64) -> Self {
        self.max_memory = memory;
        self
    }
}

// ============================================================================
// I/O Scheduler Configuration
// ============================================================================

/// I/O scheduler configuration settings.
#[derive(Debug, Clone)]
pub struct IoSchedulerSettings {
    /// Maximum concurrent reads.
    pub max_concurrent_reads: usize,
    /// High priority size threshold.
    pub high_priority_threshold: usize,
    /// Low priority size threshold.
    pub low_priority_threshold: usize,
    /// Base buffer size.
    pub base_buffer_size: usize,
    /// Max buffer size.
    pub max_buffer_size: usize,
    /// Min buffer size.
    pub min_buffer_size: usize,
    /// Whether priority scheduling is enabled.
    pub priority_enabled: bool,
}

impl Default for IoSchedulerSettings {
    fn default() -> Self {
        Self {
            max_concurrent_reads: DEFAULT_MAX_CONCURRENT_READS,
            high_priority_threshold: DEFAULT_HIGH_PRIORITY_SIZE_THRESHOLD,
            low_priority_threshold: DEFAULT_LOW_PRIORITY_SIZE_THRESHOLD,
            base_buffer_size: DEFAULT_BASE_BUFFER_SIZE,
            max_buffer_size: DEFAULT_MAX_BUFFER_SIZE,
            min_buffer_size: DEFAULT_MIN_BUFFER_SIZE,
            priority_enabled: true,
        }
    }
}

impl IoSchedulerSettings {
    /// Create new settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder: set max concurrent reads.
    pub fn with_max_concurrent_reads(mut self, max: usize) -> Self {
        self.max_concurrent_reads = max;
        self
    }

    /// Builder: set buffer sizes.
    pub fn with_buffer_sizes(mut self, base: usize, min: usize, max: usize) -> Self {
        self.base_buffer_size = base;
        self.min_buffer_size = min;
        self.max_buffer_size = max;
        self
    }
}

// ============================================================================
// Backpressure Configuration
// ============================================================================

/// Backpressure configuration settings.
#[derive(Debug, Clone)]
pub struct BackpressureSettings {
    /// Whether backpressure is enabled.
    pub enabled: bool,
    /// High watermark (percentage).
    pub high_watermark: f64,
    /// Low watermark (percentage).
    pub low_watermark: f64,
    /// Cooldown duration.
    pub cooldown: Duration,
}

impl Default for BackpressureSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            high_watermark: DEFAULT_BACKPRESSURE_HIGH_WATERMARK,
            low_watermark: DEFAULT_BACKPRESSURE_LOW_WATERMARK,
            cooldown: Duration::from_millis(100),
        }
    }
}

impl BackpressureSettings {
    /// Create new settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get high watermark threshold for a given max value.
    pub fn high_threshold(&self, max: usize) -> usize {
        (max as f64 * self.high_watermark) as usize
    }

    /// Get low watermark threshold for a given max value.
    pub fn low_threshold(&self, max: usize) -> usize {
        (max as f64 * self.low_watermark) as usize
    }
}

// ============================================================================
// Timeout Configuration
// ============================================================================

/// Timeout configuration settings.
#[derive(Debug, Clone)]
pub struct TimeoutSettings {
    /// Default operation timeout.
    pub default_timeout: Duration,
    /// Maximum retries.
    pub max_retries: usize,
    /// Retry backoff factor.
    pub retry_backoff_factor: f64,
    /// Lock acquire timeout.
    pub lock_acquire_timeout: Duration,
}

impl Default for TimeoutSettings {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_backoff_factor: 2.0,
            lock_acquire_timeout: Duration::from_secs(DEFAULT_LOCK_ACQUIRE_TIMEOUT_SECS),
        }
    }
}

impl TimeoutSettings {
    /// Create new settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculate timeout with backoff for a given retry count.
    pub fn timeout_with_backoff(&self, retry_count: usize) -> Duration {
        let multiplier = self.retry_backoff_factor.powi(retry_count as i32);
        Duration::from_secs_f64(self.default_timeout.as_secs_f64() * multiplier)
    }
}

// ============================================================================
// Deadlock Detection Configuration
// ============================================================================

/// Deadlock detection configuration settings.
#[derive(Debug, Clone)]
pub struct DeadlockDetectionSettings {
    /// Whether detection is enabled.
    pub enabled: bool,
    /// Detection interval.
    pub detection_interval: Duration,
    /// Maximum lock hold time before warning.
    pub max_hold_time: Duration,
}

impl Default for DeadlockDetectionSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            detection_interval: Duration::from_secs(DEFAULT_DEADLOCK_DETECTION_INTERVAL_SECS),
            max_hold_time: Duration::from_secs(30),
        }
    }
}

impl DeadlockDetectionSettings {
    /// Create new settings.
    pub fn new() -> Self {
        Self::default()
    }
}

// ============================================================================
// Unified Configuration
// ============================================================================

/// Unified configuration for all I/O operations.
#[derive(Debug, Clone, Default)]
pub struct IoConfig {
    /// Cache settings.
    pub cache: CacheSettings,
    /// I/O scheduler settings.
    pub scheduler: IoSchedulerSettings,
    /// Backpressure settings.
    pub backpressure: BackpressureSettings,
    /// Timeout settings.
    pub timeout: TimeoutSettings,
    /// Deadlock detection settings.
    pub deadlock_detection: DeadlockDetectionSettings,
}

impl IoConfig {
    /// Create new unified configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder: set cache settings.
    pub fn with_cache(mut self, cache: CacheSettings) -> Self {
        self.cache = cache;
        self
    }

    /// Builder: set scheduler settings.
    pub fn with_scheduler(mut self, scheduler: IoSchedulerSettings) -> Self {
        self.scheduler = scheduler;
        self
    }

    /// Builder: set backpressure settings.
    pub fn with_backpressure(mut self, backpressure: BackpressureSettings) -> Self {
        self.backpressure = backpressure;
        self
    }

    /// Builder: set timeout settings.
    pub fn with_timeout(mut self, timeout: TimeoutSettings) -> Self {
        self.timeout = timeout;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_settings() {
        let settings = CacheSettings::new()
            .with_max_capacity(5000)
            .with_ttl(Duration::from_secs(600));

        assert_eq!(settings.max_capacity, 5000);
        assert_eq!(settings.default_ttl, Duration::from_secs(600));
    }

    #[test]
    fn test_io_scheduler_settings() {
        let settings =
            IoSchedulerSettings::new()
                .with_max_concurrent_reads(64)
                .with_buffer_sizes(256 * 1024, 8 * 1024, 2 * 1024 * 1024);

        assert_eq!(settings.max_concurrent_reads, 64);
        assert_eq!(settings.base_buffer_size, 256 * 1024);
    }

    #[test]
    fn test_backpressure_settings() {
        let settings = BackpressureSettings::new();

        assert_eq!(settings.high_threshold(100), 80);
        assert_eq!(settings.low_threshold(100), 50);
    }

    #[test]
    fn test_timeout_settings() {
        let settings = TimeoutSettings::new();

        // First retry: 30s * 2 = 60s
        let timeout1 = settings.timeout_with_backoff(1);
        assert!(timeout1.as_secs() >= 60);

        // Second retry: 30s * 4 = 120s
        let timeout2 = settings.timeout_with_backoff(2);
        assert!(timeout2.as_secs() >= 120);
    }

    #[test]
    fn test_unified_config() {
        let config = IoConfig::new()
            .with_cache(CacheSettings::new().with_max_capacity(5000))
            .with_scheduler(IoSchedulerSettings::new().with_max_concurrent_reads(64));

        assert_eq!(config.cache.max_capacity, 5000);
        assert_eq!(config.scheduler.max_concurrent_reads, 64);
    }
}
