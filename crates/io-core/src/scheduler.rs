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

//! I/O scheduler for adaptive buffer sizing and load management.
//!
//! This module provides the core I/O scheduling logic that determines
//! optimal buffer sizes, I/O strategies, and load management decisions.

use crate::config::IoSchedulerConfig;
use crate::io_profile::{AccessPattern, StorageMedia, StorageProfile};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// I/O priority levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum IoPriority {
    /// High priority for small, latency-sensitive operations.
    High,
    /// Normal priority for standard operations.
    #[default]
    Normal,
    /// Low priority for large, throughput-oriented operations.
    Low,
}

impl IoPriority {
    /// Determine priority based on request size.
    pub fn from_size(size: i64, high_threshold: usize, low_threshold: usize) -> Self {
        let size = size as usize;
        if size < high_threshold {
            IoPriority::High
        } else if size > low_threshold {
            IoPriority::Low
        } else {
            IoPriority::Normal
        }
    }

    /// Get the priority as a string for metrics labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            IoPriority::High => "high",
            IoPriority::Normal => "normal",
            IoPriority::Low => "low",
        }
    }

    /// Check if this is high priority.
    pub fn is_high(&self) -> bool {
        matches!(self, IoPriority::High)
    }

    /// Check if this is normal priority.
    pub fn is_normal(&self) -> bool {
        matches!(self, IoPriority::Normal)
    }

    /// Check if this is low priority.
    pub fn is_low(&self) -> bool {
        matches!(self, IoPriority::Low)
    }
}

impl std::fmt::Display for IoPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// I/O load level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Default)]
pub enum IoLoadLevel {
    /// Low load - system is underutilized.
    Low,
    /// Medium load - system is moderately utilized.
    #[default]
    Medium,
    /// High load - system is heavily utilized.
    High,
    /// Critical load - system is overloaded.
    Critical,
}

impl IoLoadLevel {
    /// Get the load level as a string for metrics labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            IoLoadLevel::Low => "low",
            IoLoadLevel::Medium => "medium",
            IoLoadLevel::High => "high",
            IoLoadLevel::Critical => "critical",
        }
    }

    /// Determine load level from wait time.
    pub fn from_wait_time(wait_time: Duration, low_threshold: Duration, high_threshold: Duration) -> Self {
        if wait_time <= low_threshold {
            IoLoadLevel::Low
        } else if wait_time <= high_threshold {
            IoLoadLevel::Medium
        } else if wait_time <= high_threshold * 2 {
            IoLoadLevel::High
        } else {
            IoLoadLevel::Critical
        }
    }
}

impl std::fmt::Display for IoLoadLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Bandwidth tier classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum BandwidthTier {
    /// Low bandwidth (< 100 MB/s).
    Low,
    /// Medium bandwidth (100-500 MB/s).
    #[default]
    Medium,
    /// High bandwidth (> 500 MB/s).
    High,
    /// Unknown bandwidth.
    Unknown,
}

impl BandwidthTier {
    /// Determine bandwidth tier from bytes per second.
    pub fn from_bps(bps: u64) -> Self {
        const MB: u64 = 1024 * 1024;
        if bps < 100 * MB {
            BandwidthTier::Low
        } else if bps < 500 * MB {
            BandwidthTier::Medium
        } else {
            BandwidthTier::High
        }
    }

    /// Get the tier as a string for metrics labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            BandwidthTier::Low => "low",
            BandwidthTier::Medium => "medium",
            BandwidthTier::High => "high",
            BandwidthTier::Unknown => "unknown",
        }
    }
}

/// I/O strategy decision.
#[derive(Debug, Clone)]
pub struct IoStrategy {
    /// Buffer size to use for I/O operations.
    pub buffer_size: usize,
    /// Buffer multiplier based on storage media.
    pub buffer_multiplier: f64,
    /// Whether to enable readahead.
    pub enable_readahead: bool,
    /// Whether cache writeback is enabled.
    pub cache_writeback_enabled: bool,
    /// Whether to use buffered I/O.
    pub use_buffered_io: bool,

    // Performance state
    /// Current number of concurrent requests.
    pub concurrent_requests: usize,
    /// Observed bandwidth in bytes per second.
    pub observed_bandwidth_bps: Option<u64>,
    /// Bandwidth tier classification.
    pub bandwidth_tier: BandwidthTier,
    /// Current load level.
    pub load_level: IoLoadLevel,

    // Priority
    /// I/O priority for this operation.
    pub priority: IoPriority,

    // Decision flags
    /// Whether to throttle random I/O.
    pub should_throttle_random_io: bool,
    /// Whether to expand buffer for sequential access.
    pub should_expand_for_sequential: bool,
    /// Whether to reduce buffer due to concurrency.
    pub should_reduce_for_concurrency: bool,
    /// Whether to reduce buffer due to low bandwidth.
    pub should_reduce_for_bandwidth: bool,
}

impl Default for IoStrategy {
    fn default() -> Self {
        Self {
            buffer_size: 128 * 1024,
            buffer_multiplier: 1.0,
            enable_readahead: true,
            cache_writeback_enabled: false,
            use_buffered_io: true,
            concurrent_requests: 0,
            observed_bandwidth_bps: None,
            bandwidth_tier: BandwidthTier::Medium,
            load_level: IoLoadLevel::Low,
            priority: IoPriority::Normal,
            should_throttle_random_io: false,
            should_expand_for_sequential: false,
            should_reduce_for_concurrency: false,
            should_reduce_for_bandwidth: false,
        }
    }
}

impl IoStrategy {
    /// Create a new strategy with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a strategy for sequential access.
    pub fn sequential(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            enable_readahead: true,
            should_expand_for_sequential: true,
            ..Self::default()
        }
    }

    /// Create a strategy for random access.
    pub fn random(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            enable_readahead: false,
            should_throttle_random_io: true,
            ..Self::default()
        }
    }
}

/// I/O load metrics.
#[derive(Debug, Clone, Default)]
pub struct IoLoadMetrics {
    /// Number of samples in the current window.
    pub sample_count: usize,
    /// Total wait time in the window.
    pub total_wait_time: Duration,
    /// Maximum wait time in the window.
    pub max_wait_time: Duration,
    /// Average wait time.
    pub avg_wait_time: Duration,
    /// Current load level.
    pub load_level: IoLoadLevel,
}

impl IoLoadMetrics {
    /// Create new load metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a wait time sample.
    pub fn add_sample(&mut self, wait_time: Duration) {
        self.sample_count += 1;
        self.total_wait_time += wait_time;
        if wait_time > self.max_wait_time {
            self.max_wait_time = wait_time;
        }
        self.avg_wait_time = if self.sample_count > 0 {
            self.total_wait_time / self.sample_count as u32
        } else {
            Duration::ZERO
        };
    }

    /// Update load level based on thresholds.
    pub fn update_load_level(&mut self, low_threshold: Duration, high_threshold: Duration) {
        self.load_level = IoLoadLevel::from_wait_time(self.avg_wait_time, low_threshold, high_threshold);
    }

    /// Reset the metrics.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// I/O scheduler.
pub struct IoScheduler {
    /// Scheduler configuration.
    config: IoSchedulerConfig,
    /// Active request counter.
    active_requests: AtomicUsize,
    /// Load metrics.
    load_metrics: std::sync::Mutex<IoLoadMetrics>,
}

impl IoScheduler {
    /// Create a new I/O scheduler with the given configuration.
    pub fn new(config: IoSchedulerConfig) -> Self {
        Self {
            config,
            active_requests: AtomicUsize::new(0),
            load_metrics: std::sync::Mutex::new(IoLoadMetrics::new()),
        }
    }

    /// Create a new I/O scheduler with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(IoSchedulerConfig::default())
    }

    /// Get the scheduler configuration.
    pub fn config(&self) -> &IoSchedulerConfig {
        &self.config
    }

    /// Get the current number of active requests.
    pub fn active_requests(&self) -> usize {
        self.active_requests.load(Ordering::Relaxed)
    }

    /// Increment the active request count.
    pub fn increment_requests(&self) {
        self.active_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the active request count.
    pub fn decrement_requests(&self) {
        self.active_requests.fetch_sub(1, Ordering::Relaxed);
    }

    /// Calculate I/O strategy for a request.
    pub fn calculate_strategy(&self, file_size: i64, permit_wait_time: Duration, is_sequential: bool) -> IoStrategy {
        let concurrent_requests = self.active_requests.load(Ordering::Relaxed);

        // Determine priority based on file size
        let priority = IoPriority::from_size(
            file_size,
            self.config.high_priority_size_threshold,
            self.config.low_priority_size_threshold,
        );

        // Determine load level
        let load_level =
            IoLoadLevel::from_wait_time(permit_wait_time, self.config.load_low_threshold(), self.config.load_high_threshold());

        // Calculate base buffer size
        let base_buffer = self.config.base_buffer_size;

        // Adjust for concurrency
        let concurrency_factor = match concurrent_requests {
            0..=2 => 1.0,
            3..=4 => 0.75,
            5..=8 => 0.5,
            _ => 0.4,
        };

        // Adjust for load level
        let load_factor = match load_level {
            IoLoadLevel::Low => 1.2,
            IoLoadLevel::Medium => 1.0,
            IoLoadLevel::High => 0.7,
            IoLoadLevel::Critical => 0.5,
        };

        // Adjust for access pattern
        let sequential_factor = if is_sequential { 1.5 } else { 1.0 };

        // Calculate final buffer size
        let buffer_size = (base_buffer as f64 * concurrency_factor * load_factor * sequential_factor) as usize;
        let buffer_size = buffer_size.clamp(self.config.min_buffer_size, self.config.max_buffer_size);

        IoStrategy {
            buffer_size,
            buffer_multiplier: concurrency_factor * load_factor * sequential_factor,
            enable_readahead: is_sequential && load_level != IoLoadLevel::Critical,
            cache_writeback_enabled: load_level == IoLoadLevel::Low,
            use_buffered_io: true,
            concurrent_requests,
            observed_bandwidth_bps: None,
            bandwidth_tier: BandwidthTier::Unknown,
            load_level,
            priority,
            should_throttle_random_io: !is_sequential && load_level >= IoLoadLevel::High,
            should_expand_for_sequential: is_sequential && load_level <= IoLoadLevel::Medium,
            should_reduce_for_concurrency: concurrent_requests > 4,
            should_reduce_for_bandwidth: false,
        }
    }

    /// Calculate multi-factor I/O strategy.
    pub fn calculate_multi_factor_strategy(
        &self,
        file_size: i64,
        permit_wait_time: Duration,
        is_sequential: bool,
        storage_profile: Option<&StorageProfile>,
    ) -> IoStrategy {
        let mut strategy = self.calculate_strategy(file_size, permit_wait_time, is_sequential);

        // Apply storage profile adjustments
        if let Some(profile) = storage_profile {
            // Adjust buffer size based on storage media
            let media_factor = match profile.media {
                StorageMedia::Nvme => 1.5,
                StorageMedia::Ssd => 1.2,
                StorageMedia::Hdd => 0.8,
                StorageMedia::Unknown => 1.0,
            };

            strategy.buffer_size = (strategy.buffer_size as f64 * media_factor).min(self.config.max_buffer_size as f64) as usize;

            // Apply sequential boost if applicable
            if is_sequential {
                strategy.buffer_size = (strategy.buffer_size as f64 * profile.sequential_boost_multiplier)
                    .min(self.config.max_buffer_size as f64) as usize;
            }

            // Apply random penalty if applicable
            if !is_sequential {
                strategy.buffer_size = (strategy.buffer_size as f64 * profile.random_penalty_multiplier)
                    .max(self.config.min_buffer_size as f64) as usize;
            }

            // Update readahead preference
            strategy.enable_readahead = strategy.enable_readahead && profile.prefers_readahead;
        }

        strategy
    }

    /// Record a wait time sample for load tracking.
    pub fn record_wait_time(&self, wait_time: Duration) {
        if let Ok(mut metrics) = self.load_metrics.lock() {
            metrics.add_sample(wait_time);
            metrics.update_load_level(self.config.load_low_threshold(), self.config.load_high_threshold());
        }
    }

    /// Get current load metrics.
    pub fn load_metrics(&self) -> IoLoadMetrics {
        if let Ok(metrics) = self.load_metrics.lock() {
            metrics.clone()
        } else {
            IoLoadMetrics::default()
        }
    }
}

impl Default for IoScheduler {
    fn default() -> Self {
        Self::with_defaults()
    }
}

// ============================================================================
// Buffer Size Calculation Functions
// ============================================================================

/// Constants for buffer size calculations.
pub const KI_B: usize = 1024;
pub const MI_B: usize = 1024 * 1024;

/// Get concurrency-aware buffer size.
///
/// Adjusts buffer size based on the current level of concurrent requests.
/// Higher concurrency leads to smaller buffers to reduce memory pressure.
///
/// # Arguments
///
/// * `file_size` - Size of the file being read (-1 if unknown)
/// * `base_buffer_size` - Base buffer size from workload profile
///
/// # Returns
///
/// Adjusted buffer size in bytes
pub fn get_concurrency_aware_buffer_size(file_size: i64, base_buffer_size: usize) -> usize {
    // Get current concurrency level from global counter
    let concurrent_requests = 1; // Default to 1 if no global counter available

    // Define concurrency thresholds
    let medium_threshold = 4;
    let high_threshold = 8;

    // Calculate adaptive multiplier based on concurrency
    let adaptive_multiplier = if concurrent_requests <= 2 {
        // Low concurrency (1-2): use full buffer size
        1.0
    } else if concurrent_requests <= medium_threshold {
        // Medium concurrency (3-4): slightly reduce buffer size (75% of base)
        0.75
    } else if concurrent_requests <= high_threshold {
        // Higher concurrency (5-8): more aggressive reduction (50% of base)
        0.5
    } else {
        // Very high concurrency (>8): minimize memory per request (40% of base)
        0.4
    };

    // Calculate the adjusted buffer size
    let adjusted_size = (base_buffer_size as f64 * adaptive_multiplier) as usize;

    // Ensure we stay within reasonable bounds
    let min_buffer = if file_size > 0 && file_size < 100 * KI_B as i64 {
        32 * KI_B // For very small files, use minimum buffer
    } else {
        64 * KI_B // Standard minimum buffer size
    };

    let max_buffer = if concurrent_requests > high_threshold {
        256 * KI_B // Cap at 256KB for high concurrency
    } else {
        MI_B // Cap at 1MB for lower concurrency
    };

    adjusted_size.clamp(min_buffer, max_buffer)
}

/// Advanced concurrency-aware buffer sizing with file size optimization.
///
/// This enhanced version considers both concurrency level and file size patterns
/// to provide even better performance characteristics.
///
/// # Arguments
///
/// * `file_size` - Size of the file being read (-1 if unknown)
/// * `base_buffer_size` - Baseline buffer size from workload profile
/// * `is_sequential` - Whether this is a sequential read (hint for optimization)
/// * `concurrent_requests` - Current number of concurrent requests
///
/// # Returns
///
/// Optimized buffer size in bytes
pub fn get_advanced_buffer_size(
    file_size: i64,
    base_buffer_size: usize,
    is_sequential: bool,
    concurrent_requests: usize,
) -> usize {
    // For very small files, use smaller buffers regardless of concurrency
    if file_size > 0 && file_size < 256 * KI_B as i64 {
        return (file_size as usize / 4).clamp(16 * KI_B, 64 * KI_B);
    }

    // Base calculation from standard function
    let standard_size = get_concurrency_aware_buffer_size(file_size, base_buffer_size);

    let medium_threshold = 4;
    let high_threshold = 8;

    // For sequential reads, we can be more aggressive with buffer sizes
    if is_sequential && concurrent_requests <= medium_threshold {
        // Boost buffer size for sequential reads under low concurrency
        let boosted = (standard_size as f64 * 1.5) as usize;
        return boosted.min(MI_B);
    }

    // For random reads under high concurrency, reduce buffer size
    if !is_sequential && concurrent_requests > high_threshold {
        let reduced = (standard_size as f64 * 0.7) as usize;
        return reduced.max(32 * KI_B);
    }

    standard_size
}

/// Get buffer size with storage media optimization.
///
/// Adjusts buffer size based on storage media characteristics.
///
/// # Arguments
///
/// * `base_size` - Base buffer size
/// * `media` - Storage media type
///
/// # Returns
///
/// Optimized buffer size for the storage media
pub fn get_buffer_size_for_media(base_size: usize, media: StorageMedia) -> usize {
    let multiplier = match media {
        StorageMedia::Nvme => 1.5, // NVMe can handle larger buffers
        StorageMedia::Ssd => 1.2,  // SSD benefits from moderate buffers
        StorageMedia::Hdd => 0.8,  // HDD prefers smaller buffers to reduce seek overhead
        StorageMedia::Unknown => 1.0,
    };

    (base_size as f64 * multiplier).min(MI_B as f64) as usize
}

/// Calculate optimal buffer size using multi-factor analysis.
///
/// This is the main entry point for buffer size calculation, considering
/// all factors: concurrency, storage media, access pattern, and load.
///
/// # Arguments
///
/// * `file_size` - Size of the file being read
/// * `base_buffer_size` - Base buffer size
/// * `is_sequential` - Whether access is sequential
/// * `concurrent_requests` - Current concurrency level
/// * `media` - Storage media type
/// * `load_level` - Current I/O load level
///
/// # Returns
///
/// Optimally calculated buffer size
pub fn calculate_optimal_buffer_size(
    file_size: i64,
    base_buffer_size: usize,
    is_sequential: bool,
    concurrent_requests: usize,
    media: StorageMedia,
    load_level: IoLoadLevel,
) -> usize {
    // Start with advanced buffer size calculation
    let mut buffer_size = get_advanced_buffer_size(file_size, base_buffer_size, is_sequential, concurrent_requests);

    // Apply storage media optimization
    buffer_size = get_buffer_size_for_media(buffer_size, media);

    // Apply load-based adjustment
    let load_multiplier = match load_level {
        IoLoadLevel::Low => 1.2,
        IoLoadLevel::Medium => 1.0,
        IoLoadLevel::High => 0.7,
        IoLoadLevel::Critical => 0.5,
    };

    buffer_size = (buffer_size as f64 * load_multiplier) as usize;

    // Final bounds check
    buffer_size.clamp(32 * KI_B, MI_B)
}

/// I/O scheduling context for multi-factor strategy calculation.
#[derive(Debug, Clone)]
pub struct IoSchedulingContext {
    /// File size in bytes (-1 if unknown).
    pub file_size: i64,
    /// Base buffer size from configuration.
    pub base_buffer_size: usize,
    /// Time spent waiting for permit.
    pub permit_wait_duration: Duration,
    /// Whether access is sequential.
    pub is_sequential_hint: bool,
    /// Detected access pattern.
    pub access_pattern: AccessPattern,
    /// Detected storage media.
    pub storage_media: StorageMedia,
    /// Observed bandwidth in bytes per second.
    pub observed_bandwidth_bps: Option<u64>,
    /// Current concurrent request count.
    pub concurrent_requests: usize,
}

impl Default for IoSchedulingContext {
    fn default() -> Self {
        Self {
            file_size: -1,
            base_buffer_size: 128 * KI_B,
            permit_wait_duration: Duration::ZERO,
            is_sequential_hint: true,
            access_pattern: AccessPattern::Unknown,
            storage_media: StorageMedia::Unknown,
            observed_bandwidth_bps: None,
            concurrent_requests: 1,
        }
    }
}

impl IoSchedulingContext {
    /// Create a new scheduling context.
    pub fn new(file_size: i64, base_buffer_size: usize) -> Self {
        Self {
            file_size,
            base_buffer_size,
            ..Self::default()
        }
    }

    /// Builder pattern: set sequential hint.
    pub fn with_sequential(mut self, is_sequential: bool) -> Self {
        self.is_sequential_hint = is_sequential;
        self.access_pattern = if is_sequential {
            AccessPattern::Sequential
        } else {
            AccessPattern::Random
        };
        self
    }

    /// Builder pattern: set storage media.
    pub fn with_media(mut self, media: StorageMedia) -> Self {
        self.storage_media = media;
        self
    }

    /// Builder pattern: set bandwidth.
    pub fn with_bandwidth(mut self, bps: u64) -> Self {
        self.observed_bandwidth_bps = Some(bps);
        self
    }

    /// Builder pattern: set concurrency.
    pub fn with_concurrency(mut self, count: usize) -> Self {
        self.concurrent_requests = count;
        self
    }

    /// Builder pattern: set wait duration.
    pub fn with_wait_duration(mut self, duration: Duration) -> Self {
        self.permit_wait_duration = duration;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_priority() {
        assert_eq!(IoPriority::from_size(1024, 64 * 1024, 4 * 1024 * 1024), IoPriority::High);
        assert_eq!(IoPriority::from_size(1024 * 1024, 64 * 1024, 4 * 1024 * 1024), IoPriority::Normal);
        assert_eq!(IoPriority::from_size(10 * 1024 * 1024, 64 * 1024, 4 * 1024 * 1024), IoPriority::Low);
    }

    #[test]
    fn test_io_load_level() {
        let low = Duration::from_millis(5);
        let high = Duration::from_millis(50);

        assert_eq!(IoLoadLevel::from_wait_time(Duration::from_millis(1), low, high), IoLoadLevel::Low);
        assert_eq!(IoLoadLevel::from_wait_time(Duration::from_millis(20), low, high), IoLoadLevel::Medium);
        assert_eq!(IoLoadLevel::from_wait_time(Duration::from_millis(60), low, high), IoLoadLevel::High);
        assert_eq!(IoLoadLevel::from_wait_time(Duration::from_millis(150), low, high), IoLoadLevel::Critical);
    }

    #[test]
    fn test_bandwidth_tier() {
        assert_eq!(BandwidthTier::from_bps(50 * 1024 * 1024), BandwidthTier::Low);
        assert_eq!(BandwidthTier::from_bps(200 * 1024 * 1024), BandwidthTier::Medium);
        assert_eq!(BandwidthTier::from_bps(600 * 1024 * 1024), BandwidthTier::High);
    }

    #[test]
    fn test_io_strategy_default() {
        let strategy = IoStrategy::default();
        assert!(strategy.buffer_size > 0);
        assert!(strategy.enable_readahead);
    }

    #[test]
    fn test_io_scheduler() {
        let scheduler = IoScheduler::with_defaults();

        let strategy = scheduler.calculate_strategy(1024 * 1024, Duration::from_millis(5), true);
        assert!(strategy.buffer_size > 0);
        assert!(strategy.enable_readahead);
        assert_eq!(strategy.load_level, IoLoadLevel::Low);
    }

    #[test]
    fn test_io_scheduler_with_concurrency() {
        let scheduler = IoScheduler::with_defaults();

        // Simulate concurrent requests
        scheduler.increment_requests();
        scheduler.increment_requests();
        scheduler.increment_requests();

        let strategy = scheduler.calculate_strategy(1024 * 1024, Duration::from_millis(5), true);
        assert_eq!(strategy.concurrent_requests, 3);
    }

    #[test]
    fn test_load_metrics() {
        let mut metrics = IoLoadMetrics::new();

        metrics.add_sample(Duration::from_millis(10));
        metrics.add_sample(Duration::from_millis(20));
        metrics.add_sample(Duration::from_millis(30));

        assert_eq!(metrics.sample_count, 3);
        assert_eq!(metrics.avg_wait_time, Duration::from_millis(20));
        assert_eq!(metrics.max_wait_time, Duration::from_millis(30));
    }

    #[test]
    fn test_get_concurrency_aware_buffer_size() {
        // Test with default concurrency (1)
        let size = get_concurrency_aware_buffer_size(1024 * 1024, 128 * KI_B);
        assert!(size >= 64 * KI_B);
        assert!(size <= MI_B);

        // Test with small file
        let size = get_concurrency_aware_buffer_size(50 * KI_B as i64, 128 * KI_B);
        assert!(size >= 32 * KI_B);
    }

    #[test]
    fn test_get_advanced_buffer_size() {
        // Sequential read with low concurrency
        let size = get_advanced_buffer_size(10 * MI_B as i64, 128 * KI_B, true, 2);
        assert!(size >= 128 * KI_B);

        // Random read with high concurrency
        let size = get_advanced_buffer_size(10 * MI_B as i64, 128 * KI_B, false, 10);
        assert!(size >= 32 * KI_B);

        // Very small file
        let size = get_advanced_buffer_size(100 * KI_B as i64, 128 * KI_B, true, 1);
        assert!(size <= 64 * KI_B);
    }

    #[test]
    fn test_get_buffer_size_for_media() {
        let base = 128 * KI_B;

        // NVMe should get larger buffers
        let nvme_size = get_buffer_size_for_media(base, StorageMedia::Nvme);
        assert!(nvme_size > base);

        // SSD should get slightly larger buffers
        let ssd_size = get_buffer_size_for_media(base, StorageMedia::Ssd);
        assert!(ssd_size > base);

        // HDD should get smaller buffers
        let hdd_size = get_buffer_size_for_media(base, StorageMedia::Hdd);
        assert!(hdd_size < base);
    }

    #[test]
    fn test_calculate_optimal_buffer_size() {
        // Low load, sequential, NVMe
        let size = calculate_optimal_buffer_size(10 * MI_B as i64, 128 * KI_B, true, 2, StorageMedia::Nvme, IoLoadLevel::Low);
        assert!(size >= 32 * KI_B);
        assert!(size <= MI_B);

        // Critical load, random, HDD
        let size =
            calculate_optimal_buffer_size(10 * MI_B as i64, 128 * KI_B, false, 10, StorageMedia::Hdd, IoLoadLevel::Critical);
        assert!(size >= 32 * KI_B);
        assert!(size <= MI_B);
    }

    #[test]
    fn test_io_scheduling_context() {
        let ctx = IoSchedulingContext::new(10 * MI_B as i64, 256 * KI_B)
            .with_sequential(true)
            .with_media(StorageMedia::Nvme)
            .with_bandwidth(500 * MI_B as u64)
            .with_concurrency(4);

        assert_eq!(ctx.file_size, 10 * MI_B as i64);
        assert_eq!(ctx.base_buffer_size, 256 * KI_B);
        assert!(ctx.is_sequential_hint);
        assert_eq!(ctx.storage_media, StorageMedia::Nvme);
        assert_eq!(ctx.observed_bandwidth_bps, Some(500 * MI_B as u64));
        assert_eq!(ctx.concurrent_requests, 4);
    }
}
