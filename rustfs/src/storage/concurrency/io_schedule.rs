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

//! I/O scheduling module for adaptive concurrency control.
//!
//! This module provides intelligent I/O scheduling based on system load
//! and request characteristics.

use rustfs_config::{KI_B, MI_B};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

// ============================================
// Adaptive I/O Strategy Types
// ============================================

/// Load level classification based on disk permit wait times.
///
/// This enum represents the current I/O load on the system, determined by
/// analyzing disk permit acquisition wait times. Longer wait times indicate
/// higher contention and system load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone)]
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

impl IoSchedulerConfig {
    /// Load configuration from environment.
    pub fn from_env() -> Self {
        use rustfs_utils::{get_env_bool, get_env_usize};
        Self {
            max_concurrent_reads: get_env_usize(
                rustfs_config::ENV_OBJECT_MAX_CONCURRENT_DISK_READS,
                rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
            ),
            enable_priority: get_env_bool(
                rustfs_config::ENV_OBJECT_PRIORITY_SCHEDULING_ENABLE,
                rustfs_config::DEFAULT_OBJECT_PRIORITY_SCHEDULING_ENABLE,
            ),
        }
    }
}

/// I/O queue status for monitoring.
#[derive(Debug, Clone)]
pub struct IoQueueStatus {
    /// Total permits available.
    pub total_permits: usize,
    /// Permits currently in use.
    pub permits_in_use: usize,
    /// High priority requests waiting.
    pub high_priority_waiting: usize,
    /// Normal priority requests waiting.
    pub normal_priority_waiting: usize,
    /// Low priority requests waiting.
    pub low_priority_waiting: usize,
}

/// Adaptive I/O strategy calculated from current system conditions.
///
/// This structure encapsulates all I/O parameters that should be used
/// for a read operation, calculated based on the current I/O load level.
#[derive(Debug, Clone)]
pub struct IoStrategy {
    /// Buffer size to use for this operation.
    pub buffer_size: usize,
    /// Multiplier applied to base buffer size (0.0-1.0).
    pub buffer_multiplier: f64,
    /// Whether readahead should be enabled.
    pub enable_readahead: bool,
    /// Whether cache writeback is enabled.
    pub cache_writeback_enabled: bool,
    /// Whether to use buffered I/O.
    pub use_buffered_io: bool,
    /// Current I/O load level.
    pub load_level: IoLoadLevel,
    /// The raw permit wait duration that was used to calculate this strategy.
    pub permit_wait_duration: Duration,
}

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

    pub(crate) fn record(&mut self, duration: Duration) {
        // Update totals for lifetime average
        self.total_wait_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.observation_count.fetch_add(1, Ordering::Relaxed);

        // Add to sliding window
        if self.recent_waits.len() < self.max_samples {
            self.recent_waits.push(duration);
        } else {
            // Overwrite oldest entry
            let idx = self.earliest_index % self.max_samples;
            self.recent_waits[idx] = duration;
            self.earliest_index += 1;
        }
    }

    pub(crate) fn average_wait(&self) -> Duration {
        if self.recent_waits.is_empty() {
            return Duration::ZERO;
        }
        let total: Duration = self.recent_waits.iter().sum();
        total / self.recent_waits.len() as u32
    }

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

// ============================================
// Global Request Counter and Buffer Sizing
// ============================================

/// Global concurrent request counter for adaptive buffer sizing.
///
/// This atomic counter tracks the number of active GetObject requests in real-time.
/// It's used by the buffer sizing algorithm to dynamically adjust memory allocation
/// based on current system load, preventing memory contention under high concurrency.
///
/// Access pattern: Lock-free atomic operations (Relaxed ordering for performance).
pub(crate) static ACTIVE_GET_REQUESTS: AtomicUsize = AtomicUsize::new(0);

/// Concurrency-aware buffer size calculator
///
/// This function adapts buffer sizes based on the current concurrent request load
/// to optimize for both throughput and fairness.
///
/// # Strategy
///
/// - **Low concurrency (1-2)**: Use large buffers (512KB-1MB) for maximum throughput
/// - **Medium concurrency (3-8)**: Use moderate buffers (128KB-256KB) for balanced performance
/// - **High concurrency (>8)**: Use smaller buffers (64KB-128KB) for fairness and memory efficiency
///
/// # Arguments
///
/// * `file_size` - The size of the file being read, or -1 if unknown
/// * `base_buffer_size` - The baseline buffer size from workload profile
///
/// # Returns
///
/// Optimized buffer size in bytes for the current concurrency level
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

    // Get thresholds from config
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
        // High concurrency (5-8): reduce buffer size (50% of base)
        0.5
    } else {
        // Very high concurrency (>8): use small buffers (40% of base)
        // This ensures fairness and prevents memory exhaustion
        0.4
    };

    // Calculate the adaptive buffer size
    let adaptive_size = ((base_buffer_size as f64) * adaptive_multiplier) as usize;

    // Ensure minimum buffer size (32KB) for reasonable I/O performance
    let min_buffer = 32 * KI_B;
    let max_buffer = MI_B; // Cap at 1MB to prevent excessive memory use

    let buffer_size = adaptive_size.clamp(min_buffer, max_buffer);

    // If file size is known and smaller than buffer, use file size
    if file_size > 0 && (file_size as usize) < buffer_size {
        return file_size as usize;
    }

    buffer_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_load_level_from_wait() {
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(5)), IoLoadLevel::Low);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(20)), IoLoadLevel::Medium);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(100)), IoLoadLevel::High);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(300)), IoLoadLevel::Critical);
    }

    #[test]
    fn test_io_priority_from_size() {
        assert_eq!(IoPriority::from_size(100 * 1024), IoPriority::High); // 100KB
        assert_eq!(IoPriority::from_size(5 * 1024 * 1024), IoPriority::Normal); // 5MB
        assert_eq!(IoPriority::from_size(20 * 1024 * 1024), IoPriority::Low); // 20MB
    }

    #[test]
    fn test_io_strategy_from_wait() {
        let strategy = IoStrategy::from_wait_duration(Duration::from_millis(5), 256 * 1024);
        assert_eq!(strategy.load_level, IoLoadLevel::Low);
        assert!(strategy.enable_readahead);

        let strategy = IoStrategy::from_wait_duration(Duration::from_millis(300), 256 * 1024);
        assert_eq!(strategy.load_level, IoLoadLevel::Critical);
        assert!(!strategy.enable_readahead);
    }
}
