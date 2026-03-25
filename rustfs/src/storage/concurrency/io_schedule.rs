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

//! I/O scheduling types for adaptive buffer sizing and load management.

use super::bandwidth_monitor::{BandwidthMonitor, BandwidthSnapshot, BandwidthTier};
use super::io_profile::{AccessPattern, StorageMedia, StorageProfile};
use rustfs_config::{KI_B, MI_B};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// Global concurrent request counter for adaptive buffer sizing.
pub(crate) static ACTIVE_GET_REQUESTS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, PartialEq)]
pub enum IoLoadLevel {
    /// Low load: wait time < 10ms. System has ample I/O capacity.
    Low,
    /// Medium load: wait time 10-50ms. System is moderately loaded.
    Medium,
    /// High load: wait time 50-200ms. System is under significant load.
    High,
    /// Critical load: wait time > 200ms. System is heavily congested.
    Critical,
}

impl IoLoadLevel {
    /// Determine load level from disk permit wait duration.
    ///
    /// Thresholds are based on typical NVMe SSD characteristics:
    /// - Low: < 10ms (normal operation)
    /// - Medium: 10-50ms (moderate contention)
    /// - High: 50-200ms (significant contention)
    /// - Critical: > 200ms (severe congestion)
    pub fn from_wait_duration(wait: Duration) -> Self {
        let wait_ms = wait.as_millis();
        if wait_ms < 10 {
            IoLoadLevel::Low
        } else if wait_ms < 50 {
            IoLoadLevel::Medium
        } else if wait_ms < 200 {
            IoLoadLevel::High
        } else {
            IoLoadLevel::Critical
        }
    }
}

// ============================================
// Priority-Based I/O Scheduling
// ============================================

/// I/O request priority level.
///
/// Requests are classified by size to prevent large requests
/// from starving small requests. This is especially important
/// under high concurrency where many range reads compete for I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IoPriority {
    /// High priority: small requests (< 1MB).
    /// These should complete quickly to maintain responsiveness.
    High,
    /// Normal priority: medium requests (1MB - 10MB).
    /// Standard processing with fair resource allocation.
    Normal,
    /// Low priority: large requests (> 10MB).
    /// These can tolerate longer wait times.
    Low,
}

impl IoPriority {
    /// Determine priority from request size using scheduler config thresholds.
    pub fn from_size(size: i64) -> Self {
        Self::from_size_with_thresholds(size, IoSchedulerConfig::default().high_priority_size_threshold, IoSchedulerConfig::default().low_priority_size_threshold)
    }

    pub fn from_size_with_thresholds(size: i64, high_priority_size_threshold: usize, low_priority_size_threshold: usize) -> Self {
        if size < 0 {
            return IoPriority::Normal;
        }

        let size = size as usize;
        if size < high_priority_size_threshold {
            IoPriority::High
        } else if size > low_priority_size_threshold {
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
}

impl std::fmt::Display for IoPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// I/O scheduler configuration.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct IoSchedulerConfig {
    /// Maximum concurrent disk reads.
    pub max_concurrent_reads: usize,
    /// High priority size threshold in bytes.
    pub high_priority_size_threshold: usize,
    /// Low priority size threshold in bytes.
    pub low_priority_size_threshold: usize,
    /// High priority queue capacity.
    pub queue_high_capacity: usize,
    /// Normal priority queue capacity.
    pub queue_normal_capacity: usize,
    /// Low priority queue capacity.
    pub queue_low_capacity: usize,
    /// Starvation prevention check interval in milliseconds.
    pub starvation_prevention_interval_ms: u64,
    /// Starvation threshold in seconds.
    pub starvation_threshold_secs: u64,
    /// Load sampling window size.
    pub load_sample_window: usize,
    /// High load wait time threshold in milliseconds.
    pub load_high_threshold_ms: u64,
    /// Low load wait time threshold in milliseconds.
    pub load_low_threshold_ms: u64,
    /// Whether priority scheduling is enabled.
    pub enable_priority: bool,
}

impl Default for IoSchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_reads: rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
            enable_priority: rustfs_config::DEFAULT_OBJECT_PRIORITY_SCHEDULING_ENABLE,
            high_priority_size_threshold: rustfs_config::DEFAULT_OBJECT_IO_HIGH_PRIORITY_SIZE_THRESHOLD,
            low_priority_size_threshold: rustfs_config::DEFAULT_OBJECT_IO_LOW_PRIORITY_SIZE_THRESHOLD,
            queue_high_capacity: rustfs_config::DEFAULT_OBJECT_IO_QUEUE_HIGH_CAPACITY,
            queue_normal_capacity: rustfs_config::DEFAULT_OBJECT_IO_QUEUE_NORMAL_CAPACITY,
            queue_low_capacity: rustfs_config::DEFAULT_OBJECT_IO_QUEUE_LOW_CAPACITY,
            starvation_prevention_interval_ms: rustfs_config::DEFAULT_OBJECT_IO_STARVATION_PREVENTION_INTERVAL,
            starvation_threshold_secs: rustfs_config::DEFAULT_OBJECT_IO_STARVATION_THRESHOLD_SECS,
            load_sample_window: rustfs_config::DEFAULT_OBJECT_IO_LOAD_SAMPLE_WINDOW,
            load_high_threshold_ms: rustfs_config::DEFAULT_OBJECT_IO_LOAD_HIGH_THRESHOLD_MS,
            load_low_threshold_ms: rustfs_config::DEFAULT_OBJECT_IO_LOAD_LOW_THRESHOLD_MS,
        }
    }
}

#[allow(dead_code)]
impl IoSchedulerConfig {
    /// Load configuration from environment.
    pub fn from_env() -> Self {
        Self {
            max_concurrent_reads: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_MAX_CONCURRENT_DISK_READS,
                rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
            ),
            enable_priority: rustfs_utils::get_env_bool(
                rustfs_config::ENV_OBJECT_PRIORITY_SCHEDULING_ENABLE,
                rustfs_config::DEFAULT_OBJECT_PRIORITY_SCHEDULING_ENABLE,
            ),
            high_priority_size_threshold: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_HIGH_PRIORITY_SIZE_THRESHOLD,
                rustfs_config::DEFAULT_OBJECT_IO_HIGH_PRIORITY_SIZE_THRESHOLD,
            ),
            low_priority_size_threshold: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_LOW_PRIORITY_SIZE_THRESHOLD,
                rustfs_config::DEFAULT_OBJECT_IO_LOW_PRIORITY_SIZE_THRESHOLD,
            ),
            queue_high_capacity: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_QUEUE_HIGH_CAPACITY,
                rustfs_config::DEFAULT_OBJECT_IO_QUEUE_HIGH_CAPACITY,
            ),
            queue_normal_capacity: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_QUEUE_NORMAL_CAPACITY,
                rustfs_config::DEFAULT_OBJECT_IO_QUEUE_NORMAL_CAPACITY,
            ),
            queue_low_capacity: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_QUEUE_LOW_CAPACITY,
                rustfs_config::DEFAULT_OBJECT_IO_QUEUE_LOW_CAPACITY,
            ),
            starvation_prevention_interval_ms: rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_IO_STARVATION_PREVENTION_INTERVAL,
                rustfs_config::DEFAULT_OBJECT_IO_STARVATION_PREVENTION_INTERVAL,
            ),
            starvation_threshold_secs: rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_IO_STARVATION_THRESHOLD_SECS,
                rustfs_config::DEFAULT_OBJECT_IO_STARVATION_THRESHOLD_SECS,
            ),
            load_sample_window: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_LOAD_SAMPLE_WINDOW,
                rustfs_config::DEFAULT_OBJECT_IO_LOAD_SAMPLE_WINDOW,
            ),
            load_high_threshold_ms: rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_IO_LOAD_HIGH_THRESHOLD_MS,
                rustfs_config::DEFAULT_OBJECT_IO_LOAD_HIGH_THRESHOLD_MS,
            ),
            load_low_threshold_ms: rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_IO_LOAD_LOW_THRESHOLD_MS,
                rustfs_config::DEFAULT_OBJECT_IO_LOAD_LOW_THRESHOLD_MS,
            ),
        }
    }
}

/// I/O queue status for monitoring.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct IoQueueStatus {
    /// Total permits available.
    pub total_permits: usize,
    /// Permits currently in use.
    pub permits_in_use: usize,
    /// Number of high priority requests waiting.
    pub high_priority_waiting: usize,
    /// Number of normal priority requests waiting.
    pub normal_priority_waiting: usize,
    /// Number of low priority requests waiting.
    pub low_priority_waiting: usize,
    /// Number of high priority requests processed.
    pub high_priority_processed: u64,
    /// Number of normal priority requests processed.
    pub normal_priority_processed: u64,
    /// Number of low priority requests processed.
    pub low_priority_processed: u64,
    /// Number of starvation events (low priority requests boosted).
    pub starvation_events: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IoSchedulingContext {
    pub file_size: i64,
    pub base_buffer_size: usize,
    pub permit_wait_duration: Duration,
    pub is_sequential_hint: bool,
    pub access_pattern: AccessPattern,
    pub storage_media: StorageMedia,
    pub observed_bandwidth_bps: Option<u64>,
    pub concurrent_requests: usize,
}

impl IoSchedulingContext {
    pub fn from_wait_duration(permit_wait_duration: Duration, base_buffer_size: usize) -> Self {
        Self {
            file_size: -1,
            base_buffer_size,
            permit_wait_duration,
            is_sequential_hint: false,
            access_pattern: AccessPattern::Unknown,
            storage_media: StorageMedia::Unknown,
            observed_bandwidth_bps: None,
            concurrent_requests: ACTIVE_GET_REQUESTS.load(Ordering::Relaxed),
        }
    }
}

/// Adaptive I/O strategy calculated from current system load.
///
/// This structure provides optimized I/O parameters based on the observed
/// disk permit wait times. It helps balance throughput vs. latency and
/// prevents I/O saturation under high load.
///
/// # Usage Example
///
/// ```ignore
/// let strategy = manager.calculate_io_strategy(permit_wait_duration);
///
/// // Apply strategy to I/O operations
/// let buffer_size = strategy.buffer_size;
/// let enable_readahead = strategy.enable_readahead;
/// let enable_cache_writeback = strategy.cache_writeback_enabled;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct IoStrategy {
    pub storage_media: StorageMedia,
    pub access_pattern: AccessPattern,
    pub observed_bandwidth_bps: Option<u64>,
    pub concurrent_requests: usize,
    pub bandwidth_tier: BandwidthTier,
    pub request_size: i64,
    pub base_buffer_size: usize,
    pub buffer_cap: usize,
    pub sequential_detected: bool,
    pub bandwidth_limited: bool,
    pub readahead_reason: &'static str,
    pub low_bandwidth_threshold_bps: u64,
    pub high_bandwidth_threshold_bps: u64,
    pub random_readahead_disable_concurrency: usize,
    pub low_priority_size_threshold: usize,
    pub high_priority_size_threshold: usize,
    pub priority_enabled: bool,
    pub priority: IoPriority,
    pub queue_capacity_hint: usize,
    pub load_sample_window: usize,
    pub load_high_threshold_ms: u64,
    pub load_low_threshold_ms: u64,
    pub starvation_prevention_interval_ms: u64,
    pub starvation_threshold_secs: u64,
    pub max_concurrent_reads: usize,
    pub priority_queue_high_capacity: usize,
    pub priority_queue_normal_capacity: usize,
    pub priority_queue_low_capacity: usize,
    pub storage_profile: StorageProfile,
    pub bandwidth_snapshot: Option<BandwidthSnapshot>,
    pub scheduling_context: IoSchedulingContext,
    pub high_concurrency_threshold: usize,
    pub medium_concurrency_threshold: usize,
    pub permit_wait_ms: u64,
    pub strategy_version: &'static str,
    pub is_large_request: bool,
    pub is_small_request: bool,
    pub storage_detection_enabled: bool,
    pub storage_media_override_applied: bool,
    pub pattern_history_size: usize,
    pub sequential_step_tolerance_bytes: u64,
    pub bandwidth_ema_beta: f64,
    pub nvme_buffer_cap: usize,
    pub ssd_buffer_cap: usize,
    pub hdd_buffer_cap: usize,
    pub is_range_request: bool,
    pub target_read_size: i64,
    pub source_request_size: i64,
    pub strategy_reason: &'static str,
    pub used_compatibility_path: bool,
    pub queue_depth_hint: usize,
    pub should_throttle_random_io: bool,
    pub should_expand_for_sequential: bool,
    pub should_reduce_for_concurrency: bool,
    pub should_reduce_for_bandwidth: bool,
    pub should_disable_cache_writeback: bool,
    pub should_disable_readahead: bool,
    pub should_use_buffered_io: bool,
    pub effective_multiplier_stage_concurrency: f64,
    pub effective_multiplier_stage_pattern: f64,
    pub effective_multiplier_stage_bandwidth: f64,
    pub final_multiplier: f64,
    pub strategy_source: &'static str,
    pub notes: &'static str,
    pub profile_prefers_readahead: bool,
    pub fallback_to_unknown_media: bool,
    pub request_class: &'static str,
    pub io_path_kind: &'static str,
    pub queue_mode: &'static str,
    pub load_level_label: &'static str,
    pub pattern_label: &'static str,
    pub media_label: &'static str,
    pub bandwidth_label: &'static str,
    pub sequential_hint_applied: bool,
    pub observed_bandwidth_available: bool,
    pub read_size_known: bool,
    pub random_penalty_applied: bool,
    pub sequential_boost_applied: bool,
    pub buffer_cap_applied: bool,
    pub clamp_min_applied: bool,
    pub clamp_max_applied: bool,
    pub readahead_disabled_by_concurrency: bool,
    pub readahead_disabled_by_pattern: bool,
    pub readahead_disabled_by_load: bool,
    pub readahead_disabled_by_bandwidth: bool,
    pub cache_writeback_disabled_by_load: bool,
    pub cache_writeback_disabled_by_pattern: bool,
    pub cache_writeback_disabled_by_request_size: bool,
    pub storage_profile_buffer_cap_source: &'static str,
    pub final_buffer_floor: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IoStrategy {
    /// Recommended buffer size for I/O operations (in bytes).
    ///
    /// Under high load, this is reduced to improve fairness and reduce memory pressure.
    /// Under low load, this is maximized for throughput.
    pub buffer_size: usize,

    /// Buffer size multiplier (0.4 - 1.0) applied to base buffer size.
    ///
    /// - 1.0: Low load - use full buffer
    /// - 0.75: Medium load - slightly reduced
    /// - 0.5: High load - significantly reduced
    /// - 0.4: Critical load - minimal buffer
    pub buffer_multiplier: f64,

    /// Whether to enable aggressive read-ahead for sequential reads.
    ///
    /// Disabled under high load to reduce I/O amplification.
    pub enable_readahead: bool,

    /// Whether to enable cache writeback for this request.
    ///
    /// May be disabled under extreme load to reduce memory pressure.
    pub cache_writeback_enabled: bool,

    /// Whether to use tokio BufReader for improved async I/O.
    ///
    /// Always enabled for better async performance.
    pub use_buffered_io: bool,

    /// The detected I/O load level.
    pub load_level: IoLoadLevel,

    /// The raw permit wait duration that was used to calculate this strategy.
    pub permit_wait_duration: Duration,
}

#[allow(dead_code)]
impl IoStrategy {
    /// Create a new IoStrategy from disk permit wait time and base buffer size.
    ///
    /// This analyzes the wait duration to determine the current I/O load level
    /// and calculates appropriate I/O parameters.
    ///
    /// # Arguments
    ///
    /// * `permit_wait_duration` - Time spent waiting for disk read permit
    /// * `base_buffer_size` - Base buffer size from workload configuration
    ///
    /// # Returns
    ///
    /// An IoStrategy with optimized parameters for the current load level.
    pub fn from_wait_duration(permit_wait_duration: Duration, base_buffer_size: usize) -> Self {
        let load_level = IoLoadLevel::from_wait_duration(permit_wait_duration);

        // Calculate buffer multiplier based on load level
        let buffer_multiplier = match load_level {
            IoLoadLevel::Low => 1.0,
            IoLoadLevel::Medium => 0.75,
            IoLoadLevel::High => 0.5,
            IoLoadLevel::Critical => 0.4,
        };

        // Calculate actual buffer size
        let buffer_size = ((base_buffer_size as f64) * buffer_multiplier) as usize;
        let buffer_size = buffer_size.clamp(32 * KI_B, MI_B);

        // Determine feature toggles based on load
        let enable_readahead = match load_level {
            IoLoadLevel::Low | IoLoadLevel::Medium => true,
            IoLoadLevel::High | IoLoadLevel::Critical => false,
        };

        let cache_writeback_enabled = match load_level {
            IoLoadLevel::Low | IoLoadLevel::Medium | IoLoadLevel::High => true,
            IoLoadLevel::Critical => false, // Disable under extreme load
        };

        Self {
            buffer_size,
            buffer_multiplier,
            enable_readahead,
            cache_writeback_enabled,
            use_buffered_io: true, // Always enabled
            load_level,
            permit_wait_duration,
        }
    }

    /// Get a human-readable description of the current I/O strategy.
    pub fn description(&self) -> String {
        format!(
            "IoStrategy[{:?}]: buffer={}KB, multiplier={:.2}, readahead={}, cache_wb={}, wait={:?}",
            self.load_level,
            self.buffer_size / 1024,
            self.buffer_multiplier,
            self.enable_readahead,
            self.cache_writeback_enabled,
            self.permit_wait_duration
        )
    }
}

/// Rolling window metrics for I/O load tracking.
///
/// This structure maintains a sliding window of recent disk permit wait times
/// to provide smoothed load level estimates. This helps prevent strategy
/// oscillation from transient load spikes.
#[derive(Debug)]
pub(crate) struct IoLoadMetrics {
    /// Recent permit wait durations (sliding window)
    recent_waits: Vec<Duration>,
    /// Maximum samples to keep in the window
    max_samples: usize,
    /// The earliest record index in the recent_waits vector
    earliest_index: usize,
    /// Total wait time observed (for averaging)
    total_wait_ns: AtomicU64,
    /// Total number of observations
    observation_count: AtomicU64,
}

#[allow(dead_code)]
impl IoLoadMetrics {
    pub(crate) fn new(max_samples: usize) -> Self {
        Self {
            recent_waits: Vec::with_capacity(max_samples),
            max_samples,
            earliest_index: 0,
            total_wait_ns: AtomicU64::new(0),
            observation_count: AtomicU64::new(0),
        }
    }

    /// Record a new permit wait observation
    pub(crate) fn record(&mut self, wait: Duration) {
        // Add to recent waits (with eviction if full)
        if self.recent_waits.len() < self.max_samples {
            self.recent_waits.push(wait);
        } else {
            self.recent_waits[self.earliest_index] = wait;
            self.earliest_index = (self.earliest_index + 1) % self.max_samples;
        }

        // Update totals for overall statistics
        self.total_wait_ns.fetch_add(wait.as_nanos() as u64, Ordering::Relaxed);
        self.observation_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the average wait duration over the recent window
    pub(crate) fn average_wait(&self) -> Duration {
        if self.recent_waits.is_empty() {
            return Duration::ZERO;
        }
        let total: Duration = self.recent_waits.iter().sum();
        total / self.recent_waits.len() as u32
    }

    /// Get the maximum wait duration in the recent window
    pub(crate) fn max_wait(&self) -> Duration {
        self.recent_waits.iter().copied().max().unwrap_or(Duration::ZERO)
    }

    /// Get the P95 wait duration from the recent window
    pub(crate) fn p95_wait(&self) -> Duration {
        if self.recent_waits.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.recent_waits.clone();
        sorted.sort();
        let p95_idx = ((sorted.len() as f64) * 0.95) as usize;
        sorted.get(p95_idx.min(sorted.len() - 1)).copied().unwrap_or(Duration::ZERO)
    }

    /// Get the smoothed load level based on recent observations
    pub(crate) fn smoothed_load_level(&self) -> IoLoadLevel {
        IoLoadLevel::from_wait_duration(self.average_wait())
    }

    /// Get the overall average wait since startup
    pub(crate) fn lifetime_average_wait(&self) -> Duration {
        let total = self.total_wait_ns.load(Ordering::Relaxed);
        let count = self.observation_count.load(Ordering::Relaxed);
        if count == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(total / count)
        }
    }

    /// Get the total observation count
    pub(crate) fn observation_count(&self) -> u64 {
        self.observation_count.load(Ordering::Relaxed)
    }
}
pub fn get_concurrency_aware_buffer_size(file_size: i64, base_buffer_size: usize) -> usize {
    let concurrent_requests = ACTIVE_GET_REQUESTS.load(Ordering::Relaxed);

    // Record concurrent request metrics
    #[cfg(all(feature = "metrics", not(test)))]
    {
        use metrics::gauge;
        gauge!("rustfs.concurrent.get.requests").set(concurrent_requests as f64);
    }

    // For low concurrency, use the base buffer size for maximum throughput
    if concurrent_requests <= 1 {
        return base_buffer_size;
    }
    let medium_threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
    );
    let high_threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
    );

    // Calculate adaptive multiplier based on concurrency level
    let adaptive_multiplier = if concurrent_requests <= 2 {
        // Low concurrency (1-2): use full buffer for maximum throughput
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

/// Advanced concurrency-aware buffer sizing with file size optimization
///
/// This enhanced version considers both concurrency level and file size patterns
/// to provide even better performance characteristics.
///
/// # Arguments
///
/// * `file_size` - The size of the file being read, or -1 if unknown
/// * `base_buffer_size` - The baseline buffer size from workload profile
/// * `is_sequential` - Whether this is a sequential read (hint for optimization)
///
/// # Returns
///
/// Optimized buffer size in bytes
///
/// # Examples
///
/// ```ignore
/// let buffer_size = get_advanced_buffer_size(
///     32 * 1024 * 1024,  // 32MB file
///     256 * 1024,        // 256KB base buffer
///     true               // sequential read
/// );
/// ```
pub fn get_advanced_buffer_size(file_size: i64, base_buffer_size: usize, is_sequential: bool) -> usize {
    let concurrent_requests = ACTIVE_GET_REQUESTS.load(Ordering::Relaxed);

    // For very small files, use smaller buffers regardless of concurrency
    // Replace manual max/min chain with clamp
    if file_size > 0 && file_size < 256 * KI_B as i64 {
        return (file_size as usize / 4).clamp(16 * KI_B, 64 * KI_B);
    }

    // Base calculation from standard function
    let standard_size = get_concurrency_aware_buffer_size(file_size, base_buffer_size);
    let medium_threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
    );
    let high_threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
    );
    // For sequential reads, we can be more aggressive with buffer sizes
    if is_sequential && concurrent_requests <= medium_threshold {
        return ((standard_size as f64 * 1.5) as usize).min(2 * MI_B);
    }

    // For high concurrency with large files, optimize for parallel processing
    if concurrent_requests > high_threshold && file_size > 10 * MI_B as i64 {
        // Use smaller, more numerous buffers for better parallelism
        return (standard_size as f64 * 0.8) as usize;
    }

    standard_size
}

// ============================================
// I/O Priority Queue Implementation
// ============================================

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::warn;

/// Queued I/O request with metadata.
#[derive(Debug)]
struct QueuedRequest<T> {
    /// The actual request payload.
    request: T,
    /// Time when the request was enqueued.
    enqueue_time: Instant,
    /// Original priority assigned to the request.
    #[allow(dead_code)]
    original_priority: IoPriority,
    /// Current priority (may be boosted for starvation prevention).
    current_priority: IoPriority,
    /// Whether this request was boosted due to starvation prevention.
    starvation_boosted: bool,
}

/// Queue statistics for monitoring.
#[derive(Debug, Clone, Default)]
struct QueueStats {
    /// Number of high priority requests processed.
    high_processed: u64,
    /// Number of normal priority requests processed.
    normal_processed: u64,
    /// Number of low priority requests processed.
    low_processed: u64,
    /// Number of starvation events (low priority requests boosted).
    starvation_events: u64,
    /// Total wait time in nanoseconds.
    total_wait_time_ns: u64,
}

/// I/O Priority Queue with starvation prevention.
///
/// This structure manages three priority queues (high, normal, low) and implements
/// starvation prevention by promoting low-priority requests that have been waiting
/// too long.
///
/// # Type Parameters
///
/// * `T` - The type of requests being queued.
///
/// # Example
///
/// ```ignore
/// let config = IoSchedulerConfig::default();
/// let queue = IoPriorityQueue::new(config);
///
/// // Enqueue requests
/// queue.enqueue(IoPriority::High, request1).await;
/// queue.enqueue(IoPriority::Low, request2).await;
///
/// // Dequeue (prioritizes high priority)
/// if let Some((request, priority)) = queue.dequeue().await {
///     // Process request
/// }
/// ```
pub struct IoPriorityQueue<T> {
    /// Configuration for the priority queue.
    config: IoPriorityQueueConfig,

    /// High priority queue.
    high_queue: Arc<Mutex<VecDeque<QueuedRequest<T>>>>,
    /// Normal priority queue.
    normal_queue: Arc<Mutex<VecDeque<QueuedRequest<T>>>>,
    /// Low priority queue.
    low_queue: Arc<Mutex<VecDeque<QueuedRequest<T>>>>,

    /// Queue statistics.
    stats: Arc<Mutex<QueueStats>>,

    /// Last time starvation check was performed.
    last_starvation_check: Arc<Mutex<Instant>>,
}

/// Configuration for IoPriorityQueue.
#[derive(Debug, Clone)]
pub struct IoPriorityQueueConfig {
    /// High priority queue capacity.
    pub queue_high_capacity: usize,
    /// Normal priority queue capacity.
    pub queue_normal_capacity: usize,
    /// Low priority queue capacity.
    pub queue_low_capacity: usize,
    /// Starvation prevention check interval in milliseconds.
    pub starvation_prevention_interval_ms: u64,
    /// Starvation threshold in seconds (how long before boosting).
    pub starvation_threshold_secs: u64,
}

impl Default for IoPriorityQueueConfig {
    fn default() -> Self {
        Self {
            queue_high_capacity: rustfs_config::DEFAULT_OBJECT_IO_QUEUE_HIGH_CAPACITY,
            queue_normal_capacity: rustfs_config::DEFAULT_OBJECT_IO_QUEUE_NORMAL_CAPACITY,
            queue_low_capacity: rustfs_config::DEFAULT_OBJECT_IO_QUEUE_LOW_CAPACITY,
            starvation_prevention_interval_ms: rustfs_config::DEFAULT_OBJECT_IO_STARVATION_PREVENTION_INTERVAL,
            starvation_threshold_secs: rustfs_config::DEFAULT_OBJECT_IO_STARVATION_THRESHOLD_SECS,
        }
    }
}

#[allow(dead_code)]
impl IoPriorityQueueConfig {
    /// Load configuration from environment.
    pub fn from_env() -> Self {
        Self {
            queue_high_capacity: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_QUEUE_HIGH_CAPACITY,
                rustfs_config::DEFAULT_OBJECT_IO_QUEUE_HIGH_CAPACITY,
            ),
            queue_normal_capacity: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_QUEUE_NORMAL_CAPACITY,
                rustfs_config::DEFAULT_OBJECT_IO_QUEUE_NORMAL_CAPACITY,
            ),
            queue_low_capacity: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_QUEUE_LOW_CAPACITY,
                rustfs_config::DEFAULT_OBJECT_IO_QUEUE_LOW_CAPACITY,
            ),
            starvation_prevention_interval_ms: rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_IO_STARVATION_PREVENTION_INTERVAL,
                rustfs_config::DEFAULT_OBJECT_IO_STARVATION_PREVENTION_INTERVAL,
            ),
            starvation_threshold_secs: rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_IO_STARVATION_THRESHOLD_SECS,
                rustfs_config::DEFAULT_OBJECT_IO_STARVATION_THRESHOLD_SECS,
            ),
        }
    }
}

#[allow(dead_code)]
impl<T> IoPriorityQueue<T> {
    /// Create a new priority queue with the given configuration.
    pub fn new(config: IoPriorityQueueConfig) -> Self {
        let config_clone = config.clone();
        Self {
            config,
            high_queue: Arc::new(Mutex::new(VecDeque::with_capacity(config_clone.queue_high_capacity))),
            normal_queue: Arc::new(Mutex::new(VecDeque::with_capacity(config_clone.queue_normal_capacity))),
            low_queue: Arc::new(Mutex::new(VecDeque::with_capacity(config_clone.queue_low_capacity))),
            stats: Arc::new(Mutex::new(QueueStats::default())),
            last_starvation_check: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Enqueue a request with the given priority.
    pub async fn enqueue(&self, priority: IoPriority, request: T) {
        let queued = QueuedRequest {
            request,
            enqueue_time: Instant::now(),
            original_priority: priority,
            current_priority: priority,
            starvation_boosted: false,
        };

        match priority {
            IoPriority::High => self.high_queue.lock().await.push_back(queued),
            IoPriority::Normal => self.normal_queue.lock().await.push_back(queued),
            IoPriority::Low => self.low_queue.lock().await.push_back(queued),
        }
    }

    /// Dequeue a request, prioritizing high priority requests.
    ///
    /// This method performs starvation prevention checks before dequeuing.
    /// Returns `None` if all queues are empty.
    pub async fn dequeue(&self) -> Option<(T, IoPriority)> {
        // 1. Check for starvation prevention
        self.check_starvation().await;

        // 2. Dequeue in priority order
        if let Some(queued) = self.high_queue.lock().await.pop_front() {
            self.record_dequeue(&queued).await;
            return Some((queued.request, queued.current_priority));
        }

        if let Some(queued) = self.normal_queue.lock().await.pop_front() {
            self.record_dequeue(&queued).await;
            return Some((queued.request, queued.current_priority));
        }

        if let Some(queued) = self.low_queue.lock().await.pop_front() {
            self.record_dequeue(&queued).await;
            return Some((queued.request, queued.current_priority));
        }

        None
    }

    /// Check for starving low-priority requests and boost them.
    async fn check_starvation(&self) {
        let mut last_check = self.last_starvation_check.lock().await;
        let now = Instant::now();

        // Only check at the configured interval
        if now.duration_since(*last_check) < Duration::from_millis(self.config.starvation_prevention_interval_ms) {
            return;
        }

        *last_check = now;

        // Check if low priority queue has requests waiting too long
        let mut low_queue = self.low_queue.lock().await;
        let mut normal_queue = self.normal_queue.lock().await;

        let starvation_threshold = Duration::from_secs(self.config.starvation_threshold_secs);
        let mut starvation_count = 0;

        while let Some(queued) = low_queue.front() {
            if now.duration_since(queued.enqueue_time) > starvation_threshold {
                let mut queued = low_queue.pop_front().unwrap();
                queued.current_priority = IoPriority::Normal;
                queued.starvation_boosted = true;
                normal_queue.push_back(queued);
                starvation_count += 1;
            } else {
                break;
            }
        }

        if starvation_count > 0 {
            self.stats.lock().await.starvation_events += starvation_count;
            warn!(starvation_count, "Starvation prevention: boosted low priority requests to normal");
        }
    }

    /// Record dequeue statistics.
    async fn record_dequeue(&self, queued: &QueuedRequest<T>) {
        let mut stats = self.stats.lock().await;

        // Record processing count
        match queued.current_priority {
            IoPriority::High => stats.high_processed += 1,
            IoPriority::Normal => stats.normal_processed += 1,
            IoPriority::Low => stats.low_processed += 1,
        }

        // Record wait time
        let wait_time = Instant::now().duration_since(queued.enqueue_time);
        stats.total_wait_time_ns += wait_time.as_nanos() as u64;
    }

    /// Get current queue status for monitoring.
    pub async fn status(&self) -> IoQueueStatus {
        let high_queue = self.high_queue.lock().await;
        let normal_queue = self.normal_queue.lock().await;
        let low_queue = self.low_queue.lock().await;
        let stats = self.stats.lock().await;

        IoQueueStatus {
            total_permits: 0,  // Not applicable for priority queue
            permits_in_use: 0, // Not applicable for priority queue
            high_priority_waiting: high_queue.len(),
            normal_priority_waiting: normal_queue.len(),
            low_priority_waiting: low_queue.len(),
            high_priority_processed: stats.high_processed,
            normal_priority_processed: stats.normal_processed,
            low_priority_processed: stats.low_processed,
            starvation_events: stats.starvation_events,
        }
    }

    /// Get the total number of queued requests.
    pub async fn len(&self) -> usize {
        let high_queue = self.high_queue.lock().await;
        let normal_queue = self.normal_queue.lock().await;
        let low_queue = self.low_queue.lock().await;

        high_queue.len() + normal_queue.len() + low_queue.len()
    }

    /// Check if all queues are empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

// ============================================
// I/O Priority Queue Metrics
// ============================================

/// Global metrics for I/O priority queue monitoring.
///
/// These metrics are exposed for Prometheus scraping and provide
/// visibility into the priority queue behavior.
#[allow(dead_code)]
pub struct IoPriorityMetrics {
    /// High priority queue depth.
    pub high_queue_depth: AtomicU64,
    /// Normal priority queue depth.
    pub normal_queue_depth: AtomicU64,
    /// Low priority queue depth.
    pub low_queue_depth: AtomicU64,
    /// High priority total wait time in nanoseconds.
    pub high_wait_time_ns: AtomicU64,
    /// Normal priority total wait time in nanoseconds.
    pub normal_wait_time_ns: AtomicU64,
    /// Low priority total wait time in nanoseconds.
    pub low_wait_time_ns: AtomicU64,
    /// Total starvation events count.
    pub starvation_events: AtomicU64,
    /// High priority requests processed.
    pub high_processed: AtomicU64,
    /// Normal priority requests processed.
    pub normal_processed: AtomicU64,
    /// Low priority requests processed.
    pub low_processed: AtomicU64,
}

#[allow(dead_code)]
impl IoPriorityMetrics {
    /// Create a new metrics instance.
    pub const fn new() -> Self {
        Self {
            high_queue_depth: AtomicU64::new(0),
            normal_queue_depth: AtomicU64::new(0),
            low_queue_depth: AtomicU64::new(0),
            high_wait_time_ns: AtomicU64::new(0),
            normal_wait_time_ns: AtomicU64::new(0),
            low_wait_time_ns: AtomicU64::new(0),
            starvation_events: AtomicU64::new(0),
            high_processed: AtomicU64::new(0),
            normal_processed: AtomicU64::new(0),
            low_processed: AtomicU64::new(0),
        }
    }

    /// Update queue depths from status.
    #[allow(dead_code)]
    pub fn update_queue_depths(&self, status: &IoQueueStatus) {
        self.high_queue_depth
            .store(status.high_priority_waiting as u64, Ordering::Relaxed);
        self.normal_queue_depth
            .store(status.normal_priority_waiting as u64, Ordering::Relaxed);
        self.low_queue_depth
            .store(status.low_priority_waiting as u64, Ordering::Relaxed);
    }

    /// Record a starvation event.
    #[allow(dead_code)]
    pub fn record_starvation(&self) {
        self.starvation_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a processed request.
    pub fn record_processed(&self, priority: IoPriority) {
        match priority {
            IoPriority::High => self.high_processed.fetch_add(1, Ordering::Relaxed),
            IoPriority::Normal => self.normal_processed.fetch_add(1, Ordering::Relaxed),
            IoPriority::Low => self.low_processed.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Record wait time for a priority level.
    pub fn record_wait_time(&self, priority: IoPriority, wait_ns: u64) {
        match priority {
            IoPriority::High => self.high_wait_time_ns.fetch_add(wait_ns, Ordering::Relaxed),
            IoPriority::Normal => self.normal_wait_time_ns.fetch_add(wait_ns, Ordering::Relaxed),
            IoPriority::Low => self.low_wait_time_ns.fetch_add(wait_ns, Ordering::Relaxed),
        };
    }

    /// Get high priority queue depth.
    pub fn get_high_queue_depth(&self) -> u64 {
        self.high_queue_depth.load(Ordering::Relaxed)
    }

    /// Get normal priority queue depth.
    pub fn get_normal_queue_depth(&self) -> u64 {
        self.normal_queue_depth.load(Ordering::Relaxed)
    }

    /// Get low priority queue depth.
    pub fn get_low_queue_depth(&self) -> u64 {
        self.low_queue_depth.load(Ordering::Relaxed)
    }

    /// Get total starvation events.
    pub fn get_starvation_events(&self) -> u64 {
        self.starvation_events.load(Ordering::Relaxed)
    }

    /// Get metrics summary for logging/debugging.
    #[allow(dead_code)]
    pub fn summary(&self) -> String {
        format!(
            "high_queue={}, normal_queue={}, low_queue={}, starvation={}, high_proc={}, normal_proc={}, low_proc={}",
            self.get_high_queue_depth(),
            self.get_normal_queue_depth(),
            self.get_low_queue_depth(),
            self.get_starvation_events(),
            self.high_processed.load(Ordering::Relaxed),
            self.normal_processed.load(Ordering::Relaxed),
            self.low_processed.load(Ordering::Relaxed)
        )
    }
}

/// Global I/O priority metrics instance.
#[allow(dead_code)]
pub static IO_PRIORITY_METRICS: IoPriorityMetrics = IoPriorityMetrics::new();

// ============================================
// Unit Tests
// ============================================

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tokio::test;

    #[test]
    #[serial]
    async fn test_io_priority_queue_basic() {
        let config = IoPriorityQueueConfig::default();
        let queue = IoPriorityQueue::new(config);

        // Test empty queue
        assert!(queue.is_empty().await);
        assert_eq!(queue.len().await, 0);

        // Test enqueue
        queue.enqueue(IoPriority::High, ()).await;
        queue.enqueue(IoPriority::Normal, ()).await;
        queue.enqueue(IoPriority::Low, ()).await;

        assert!(!queue.is_empty().await);
        assert_eq!(queue.len().await, 3);
    }

    #[test]
    #[serial]
    async fn test_io_priority_queue_dequeue_order() {
        let config = IoPriorityQueueConfig::default();
        let queue = IoPriorityQueue::new(config);

        // Enqueue in reverse priority order
        queue.enqueue(IoPriority::Low, "low").await;
        queue.enqueue(IoPriority::Normal, "normal").await;
        queue.enqueue(IoPriority::High, "high").await;

        // Dequeue should return in priority order
        let (req1, pri1) = queue.dequeue().await.unwrap();
        assert_eq!(req1, "high");
        assert_eq!(pri1, IoPriority::High);

        let (req2, pri2) = queue.dequeue().await.unwrap();
        assert_eq!(req2, "normal");
        assert_eq!(pri2, IoPriority::Normal);

        let (req3, pri3) = queue.dequeue().await.unwrap();
        assert_eq!(req3, "low");
        assert_eq!(pri3, IoPriority::Low);

        // Queue should be empty now
        assert!(queue.is_empty().await);
    }

    #[test]
    #[serial]
    async fn test_io_priority_queue_status() {
        let config = IoPriorityQueueConfig::default();
        let queue = IoPriorityQueue::new(config);

        // Enqueue some requests
        queue.enqueue(IoPriority::High, ()).await;
        queue.enqueue(IoPriority::High, ()).await;
        queue.enqueue(IoPriority::Normal, ()).await;
        queue.enqueue(IoPriority::Low, ()).await;

        let status = queue.status().await;
        assert_eq!(status.high_priority_waiting, 2);
        assert_eq!(status.normal_priority_waiting, 1);
        assert_eq!(status.low_priority_waiting, 1);
    }

    #[test]
    #[serial]
    async fn test_io_priority_queue_starvation_prevention() {
        let config = IoPriorityQueueConfig {
            starvation_threshold_secs: 1,
            starvation_prevention_interval_ms: 100,
            ..Default::default()
        };

        let queue = IoPriorityQueue::new(config);

        // Enqueue a low priority request
        queue.enqueue(IoPriority::Low, "low").await;

        // Wait for starvation threshold
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Dequeue should return the boosted request as Normal priority
        let (req, priority) = queue.dequeue().await.unwrap();
        assert_eq!(req, "low");
        // The request should be boosted to Normal due to starvation prevention
        assert_eq!(priority, IoPriority::Normal);
    }

    #[test]
    #[serial]
    async fn test_io_priority_from_size() {
        // High priority: < 1MB
        assert_eq!(IoPriority::from_size(100 * 1024), IoPriority::High);
        assert_eq!(IoPriority::from_size(512 * 1024), IoPriority::High);

        // Normal priority: 1MB - 10MB
        assert_eq!(IoPriority::from_size(2 * 1024 * 1024), IoPriority::Normal);
        assert_eq!(IoPriority::from_size(5 * 1024 * 1024), IoPriority::Normal);

        // Low priority: > 10MB
        assert_eq!(IoPriority::from_size(20 * 1024 * 1024), IoPriority::Low);
        assert_eq!(IoPriority::from_size(100 * 1024 * 1024), IoPriority::Low);
    }

    #[test]
    #[serial]
    async fn test_io_load_level_from_wait_duration() {
        use std::time::Duration;

        // Low load: < 10ms
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(5)), IoLoadLevel::Low);

        // Medium load: 10-50ms
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(20)), IoLoadLevel::Medium);

        // High load: 50-200ms
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(100)), IoLoadLevel::High);

        // Critical load: > 200ms
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(300)), IoLoadLevel::Critical);
    }

    #[test]
    #[serial]
    async fn test_io_scheduler_config_default() {
        let config = IoSchedulerConfig::default();

        assert!(config.enable_priority);
        assert_eq!(config.high_priority_size_threshold, 1024 * 1024); // 1MB
        assert_eq!(config.low_priority_size_threshold, 100 * 1024 * 1024); // 100MB
        assert_eq!(config.queue_high_capacity, 32);
        assert_eq!(config.queue_normal_capacity, 64);
        assert_eq!(config.queue_low_capacity, 16);
        assert_eq!(config.starvation_threshold_secs, 5);
    }

    #[test]
    #[serial]
    async fn test_io_priority_metrics() {
        let metrics = IoPriorityMetrics::new();

        // Test initial state
        assert_eq!(metrics.get_high_queue_depth(), 0);
        assert_eq!(metrics.get_normal_queue_depth(), 0);
        assert_eq!(metrics.get_low_queue_depth(), 0);
        assert_eq!(metrics.get_starvation_events(), 0);

        // Test recording
        metrics.record_starvation();
        assert_eq!(metrics.get_starvation_events(), 1);

        metrics.record_processed(IoPriority::High);
        metrics.record_processed(IoPriority::High);
        metrics.record_processed(IoPriority::Normal);

        assert_eq!(metrics.high_processed.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.normal_processed.load(Ordering::Relaxed), 1);
    }
}
