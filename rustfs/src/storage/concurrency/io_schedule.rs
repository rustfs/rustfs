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
    /// Determine priority from request size.
    ///
    /// # Arguments
    ///
    /// * `size` - Request size in bytes
    ///
    /// # Returns
    ///
    /// Priority level based on size thresholds:
    /// - High: < 1MB
    /// - Normal: 1MB - 10MB
    /// - Low: > 10MB
    pub fn from_size(size: i64) -> Self {
        const HIGH_THRESHOLD: i64 = MI_B as i64; // 1MB
        const LOW_THRESHOLD: i64 = 10 * MI_B as i64; // 10MB

        if size < HIGH_THRESHOLD {
            IoPriority::High
        } else if size > LOW_THRESHOLD {
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
    /// Whether priority scheduling is enabled.
    pub enable_priority: bool,
}

impl Default for IoSchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_reads: rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
            enable_priority: rustfs_config::DEFAULT_OBJECT_PRIORITY_SCHEDULING_ENABLE,
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
