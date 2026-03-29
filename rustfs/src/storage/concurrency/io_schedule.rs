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
//!
//! # Migration Note
//!
//! This module contains types that are also available in `rustfs_io_core`.
//! For new code, prefer using types from `rustfs_io_core` directly:
//!
//! ```ignore
//! // Recommended: Use io-core types
//! use rustfs_io_core::{
//!     IoLoadLevel, IoPriority, IoSchedulerConfig,
//!     calculate_optimal_buffer_size, get_buffer_size_for_media,
//! };
//! ```
//!
//! This module remains for backward compatibility and provides additional
//! runtime monitoring features (`IoPriorityMetrics`, `IoStrategyDebugInfo`).

use rustfs_config::{KI_B, MI_B};
use rustfs_io_core::io_profile::{AccessPattern, StorageMedia, StorageProfile};
use rustfs_io_metrics::bandwidth::{BandwidthSnapshot, BandwidthTier};
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
    pub fn from_wait_duration(wait: Duration) -> Self {
        Self::from_wait_duration_with_thresholds(
            wait,
            rustfs_config::DEFAULT_OBJECT_IO_LOAD_LOW_THRESHOLD_MS,
            rustfs_config::DEFAULT_OBJECT_IO_LOAD_HIGH_THRESHOLD_MS,
        )
    }

    pub fn from_wait_duration_with_thresholds(wait: Duration, low_threshold_ms: u64, high_threshold_ms: u64) -> Self {
        let wait_ms = wait.as_millis() as u64;
        let low_threshold_ms = low_threshold_ms.max(1);
        let high_threshold_ms = high_threshold_ms.max(low_threshold_ms + 1);
        let critical_threshold_ms = high_threshold_ms.saturating_mul(4);

        if wait_ms < low_threshold_ms {
            IoLoadLevel::Low
        } else if wait_ms < high_threshold_ms {
            IoLoadLevel::Medium
        } else if wait_ms < critical_threshold_ms {
            IoLoadLevel::High
        } else {
            IoLoadLevel::Critical
        }
    }

    /// Get the load level as a string for metrics labels.
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            IoLoadLevel::Low => "low",
            IoLoadLevel::Medium => "medium",
            IoLoadLevel::High => "high",
            IoLoadLevel::Critical => "critical",
        }
    }

    /// Get the load level as a numeric index (0=Low, 1=Medium, 2=High, 3=Critical).
    #[allow(dead_code)]
    pub fn level_index(&self) -> u8 {
        match self {
            IoLoadLevel::Low => 0,
            IoLoadLevel::Medium => 1,
            IoLoadLevel::High => 2,
            IoLoadLevel::Critical => 3,
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
    #[allow(dead_code)]
    pub fn from_size(size: i64) -> Self {
        Self::from_size_with_thresholds(
            size,
            IoSchedulerConfig::default().high_priority_size_threshold,
            IoSchedulerConfig::default().low_priority_size_threshold,
        )
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

    /// Check if this is high priority.
    #[allow(dead_code)]
    pub fn is_high(&self) -> bool {
        matches!(self, IoPriority::High)
    }

    /// Check if this is normal priority.
    #[allow(dead_code)]
    pub fn is_normal(&self) -> bool {
        matches!(self, IoPriority::Normal)
    }

    /// Check if this is low priority.
    #[allow(dead_code)]
    pub fn is_low(&self) -> bool {
        matches!(self, IoPriority::Low)
    }
}

impl std::fmt::Display for IoPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// I/O scheduler configuration.
#[derive(Debug, Clone, PartialEq)]
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

    // Enhanced scheduling configuration fields
    /// Storage media detection enabled.
    pub storage_detection_enabled: bool,
    /// Storage media override string.
    pub storage_media_override: String,
    /// Pattern detection history size.
    pub pattern_history_size: usize,
    /// Sequential step tolerance in bytes.
    pub sequential_step_tolerance_bytes: u64,
    /// Bandwidth EMA beta (smoothing factor).
    pub bandwidth_ema_beta: f64,
    /// Bandwidth low threshold in bytes per second.
    pub bandwidth_low_threshold_bps: u64,
    /// Bandwidth high threshold in bytes per second.
    pub bandwidth_high_threshold_bps: u64,
    /// NVMe buffer capacity in bytes.
    pub nvme_buffer_cap: usize,
    /// SSD buffer capacity in bytes.
    pub ssd_buffer_cap: usize,
    /// HDD buffer capacity in bytes.
    pub hdd_buffer_cap: usize,
    /// Concurrency threshold to disable random readahead.
    pub random_readahead_disable_concurrency: usize,
    /// High concurrency threshold.
    pub high_concurrency_threshold: usize,
    /// Medium concurrency threshold.
    pub medium_concurrency_threshold: usize,
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
            // Enhanced config defaults
            storage_detection_enabled: rustfs_config::DEFAULT_OBJECT_IO_STORAGE_DETECTION_ENABLE,
            storage_media_override: rustfs_config::DEFAULT_OBJECT_IO_STORAGE_MEDIA_OVERRIDE.to_string(),
            pattern_history_size: rustfs_config::DEFAULT_OBJECT_IO_PATTERN_HISTORY_SIZE,
            sequential_step_tolerance_bytes: rustfs_config::DEFAULT_OBJECT_IO_SEQUENTIAL_STEP_TOLERANCE_BYTES,
            bandwidth_ema_beta: rustfs_config::DEFAULT_OBJECT_IO_BANDWIDTH_EMA_BETA,
            bandwidth_low_threshold_bps: rustfs_config::DEFAULT_OBJECT_IO_BANDWIDTH_LOW_THRESHOLD_BPS,
            bandwidth_high_threshold_bps: rustfs_config::DEFAULT_OBJECT_IO_BANDWIDTH_HIGH_THRESHOLD_BPS,
            nvme_buffer_cap: rustfs_config::DEFAULT_OBJECT_IO_NVME_BUFFER_CAP,
            ssd_buffer_cap: rustfs_config::DEFAULT_OBJECT_IO_SSD_BUFFER_CAP,
            hdd_buffer_cap: rustfs_config::DEFAULT_OBJECT_IO_HDD_BUFFER_CAP,
            random_readahead_disable_concurrency: rustfs_config::DEFAULT_OBJECT_IO_RANDOM_READAHEAD_DISABLE_CONCURRENCY,
            high_concurrency_threshold: rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
            medium_concurrency_threshold: rustfs_config::DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
        }
    }
}

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
            // Enhanced config from environment
            storage_detection_enabled: rustfs_utils::get_env_bool(
                rustfs_config::ENV_OBJECT_IO_STORAGE_DETECTION_ENABLE,
                rustfs_config::DEFAULT_OBJECT_IO_STORAGE_DETECTION_ENABLE,
            ),
            storage_media_override: rustfs_utils::get_env_str(
                rustfs_config::ENV_OBJECT_IO_STORAGE_MEDIA_OVERRIDE,
                rustfs_config::DEFAULT_OBJECT_IO_STORAGE_MEDIA_OVERRIDE,
            ),
            pattern_history_size: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_PATTERN_HISTORY_SIZE,
                rustfs_config::DEFAULT_OBJECT_IO_PATTERN_HISTORY_SIZE,
            ),
            sequential_step_tolerance_bytes: rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_IO_SEQUENTIAL_STEP_TOLERANCE_BYTES,
                rustfs_config::DEFAULT_OBJECT_IO_SEQUENTIAL_STEP_TOLERANCE_BYTES,
            ),
            bandwidth_ema_beta: rustfs_utils::get_env_f64(
                rustfs_config::ENV_OBJECT_IO_BANDWIDTH_EMA_BETA,
                rustfs_config::DEFAULT_OBJECT_IO_BANDWIDTH_EMA_BETA,
            ),
            bandwidth_low_threshold_bps: rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_IO_BANDWIDTH_LOW_THRESHOLD_BPS,
                rustfs_config::DEFAULT_OBJECT_IO_BANDWIDTH_LOW_THRESHOLD_BPS,
            ),
            bandwidth_high_threshold_bps: rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_IO_BANDWIDTH_HIGH_THRESHOLD_BPS,
                rustfs_config::DEFAULT_OBJECT_IO_BANDWIDTH_HIGH_THRESHOLD_BPS,
            ),
            nvme_buffer_cap: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_NVME_BUFFER_CAP,
                rustfs_config::DEFAULT_OBJECT_IO_NVME_BUFFER_CAP,
            ),
            ssd_buffer_cap: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_SSD_BUFFER_CAP,
                rustfs_config::DEFAULT_OBJECT_IO_SSD_BUFFER_CAP,
            ),
            hdd_buffer_cap: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_HDD_BUFFER_CAP,
                rustfs_config::DEFAULT_OBJECT_IO_HDD_BUFFER_CAP,
            ),
            random_readahead_disable_concurrency: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_IO_RANDOM_READAHEAD_DISABLE_CONCURRENCY,
                rustfs_config::DEFAULT_OBJECT_IO_RANDOM_READAHEAD_DISABLE_CONCURRENCY,
            ),
            high_concurrency_threshold: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
                rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
            ),
            medium_concurrency_threshold: rustfs_utils::get_env_usize(
                rustfs_config::ENV_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
                rustfs_config::DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
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

/// Performance-critical I/O strategy with minimal footprint.
///
/// This structure contains only the essential runtime fields needed for I/O operations,
/// optimized for cache performance and memory efficiency.
#[derive(Debug, Clone, PartialEq)]
pub struct IoStrategyCore {
    // ===== Basic Configuration =====
    /// Detected storage media type (NVMe/SSD/HDD)
    pub storage_media: StorageMedia,
    /// Detected access pattern (Sequential/Random/Mixed)
    pub access_pattern: AccessPattern,
    /// Request size in bytes (-1 if unknown)
    pub request_size: i64,
    /// Base buffer size before adjustments
    pub base_buffer_size: usize,
    /// Maximum buffer size allowed by storage media
    pub buffer_cap: usize,

    // ===== Runtime Decisions =====
    /// Recommended buffer size for I/O operations (in bytes)
    pub buffer_size: usize,
    /// Buffer size multiplier (0.0 - 1.0) applied to base buffer size
    pub buffer_multiplier: f64,
    /// Whether sequential read-ahead should be enabled
    pub enable_readahead: bool,
    /// Whether cache writeback should be enabled
    pub cache_writeback_enabled: bool,
    /// Whether tokio BufReader should be used
    pub use_buffered_io: bool,

    // ===== Performance State =====
    /// Current concurrent request count
    pub concurrent_requests: usize,
    /// Observed bandwidth (if available)
    pub observed_bandwidth_bps: Option<u64>,
    /// Bandwidth tier (Low/Medium/High/Unknown)
    pub bandwidth_tier: BandwidthTier,
    /// Whether I/O is bandwidth-limited
    pub bandwidth_limited: bool,
    /// Whether sequential access was detected
    pub sequential_detected: bool,

    // ===== Decision Flags =====
    /// Storage profile preferences
    pub storage_profile: StorageProfile,
    /// Scheduling context for this request
    pub scheduling_context: IoSchedulingContext,
    /// Current I/O load level
    pub load_level: IoLoadLevel,
    /// Time spent waiting for disk permit
    pub permit_wait_duration: Duration,

    // ===== Tuning Multipliers =====
    pub final_multiplier: f64,
    pub should_throttle_random_io: bool,
    pub should_expand_for_sequential: bool,
    pub should_reduce_for_concurrency: bool,
    pub should_reduce_for_bandwidth: bool,
    pub should_disable_cache_writeback: bool,
    pub should_disable_readahead: bool,

    // ===== Priority Scheduling =====
    pub priority_enabled: bool,
    pub priority: IoPriority,

    // ===== Bandwidth Snapshot =====
    pub bandwidth_snapshot: Option<BandwidthSnapshot>,
}

impl IoStrategyCore {
    /// Create a minimal IoStrategyCore with essential fields only.
    #[allow(dead_code)]
    pub fn new(storage_media: StorageMedia, access_pattern: AccessPattern, buffer_size: usize) -> Self {
        Self {
            storage_media,
            access_pattern,
            request_size: -1,
            base_buffer_size: buffer_size,
            buffer_cap: buffer_size,
            buffer_size,
            buffer_multiplier: 1.0,
            enable_readahead: false,
            cache_writeback_enabled: true,
            use_buffered_io: true,
            concurrent_requests: 1,
            observed_bandwidth_bps: None,
            bandwidth_tier: BandwidthTier::Unknown,
            bandwidth_limited: false,
            sequential_detected: false,
            storage_profile: StorageProfile::for_media(
                storage_media,
                256 * 1024, // default NVMe cap
                128 * 1024, // default SSD cap
                64 * 1024,  // default HDD cap
            ),
            scheduling_context: IoSchedulingContext::from_wait_duration(Duration::ZERO, buffer_size),
            load_level: IoLoadLevel::Low,
            permit_wait_duration: Duration::ZERO,
            final_multiplier: 1.0,
            should_throttle_random_io: false,
            should_expand_for_sequential: false,
            should_reduce_for_concurrency: false,
            should_reduce_for_bandwidth: false,
            should_disable_cache_writeback: false,
            should_disable_readahead: false,
            priority_enabled: false,
            priority: IoPriority::Normal,
            bandwidth_snapshot: None,
        }
    }
}

/// Debug information for I/O strategy decisions (feature-gated).
///
/// This structure contains detailed debugging, tracing, and observability fields
/// that are only needed during development and troubleshooting.
/// Disabled in production to reduce memory footprint.
#[cfg(feature = "io-scheduler-debug")]
#[derive(Debug, Clone, PartialEq)]
pub struct IoStrategyDebugInfo {
    // ===== Decision Labels =====
    /// Reason for readahead enable/disable decision
    pub readahead_reason: &'static str,
    /// Strategy calculation version
    pub strategy_version: &'static str,
    /// High-level reason for strategy selection
    pub strategy_reason: &'static str,
    /// Source of this strategy (e.g., "from_wait_duration")
    pub strategy_source: &'static str,
    /// Additional notes about this strategy
    pub notes: &'static str,

    // ===== Request Classification =====
    pub request_class: &'static str, // "small" | "medium" | "large"
    pub io_path_kind: &'static str,  // "sequential" | "random"
    pub queue_mode: &'static str,    // "high-priority" | "normal-priority" | "low-priority"

    // ===== State Labels =====
    pub load_level_label: &'static str,
    pub pattern_label: &'static str,
    pub media_label: &'static str,
    pub bandwidth_label: &'static str,
    pub storage_profile_buffer_cap_source: &'static str,

    // ===== Decision Flags =====
    pub is_large_request: bool,
    pub is_small_request: bool,
    pub storage_detection_enabled: bool,
    pub storage_media_override_applied: bool,
    pub used_compatibility_path: bool,
    pub sequential_hint_applied: bool,
    pub observed_bandwidth_available: bool,
    pub read_size_known: bool,

    // ===== Decision Tracking =====
    pub random_penalty_applied: bool,
    pub sequential_boost_applied: bool,
    pub buffer_cap_applied: bool,
    pub clamp_min_applied: bool,
    pub clamp_max_applied: bool,

    // ===== Readahead Decisions =====
    pub readahead_disabled_by_concurrency: bool,
    pub readahead_disabled_by_pattern: bool,
    pub readahead_disabled_by_load: bool,
    pub readahead_disabled_by_bandwidth: bool,

    // ===== Cache Writeback Decisions =====
    pub cache_writeback_disabled_by_load: bool,
    pub cache_writeback_disabled_by_pattern: bool,
    pub cache_writeback_disabled_by_request_size: bool,

    // ===== Threshold Snapshots =====
    pub final_buffer_floor: usize,
    pub queue_depth_hint: usize,
    pub permit_wait_ms: u64,

    // ===== Configuration Thresholds (for debugging) =====
    pub high_concurrency_threshold: usize,
    pub medium_concurrency_threshold: usize,
    pub low_bandwidth_threshold_bps: u64,
    pub high_bandwidth_threshold_bps: u64,
    pub random_readahead_disable_concurrency: usize,
    pub low_priority_size_threshold: usize,
    pub high_priority_size_threshold: usize,

    // ===== Multiplier Breakdown =====
    pub effective_multiplier_stage_concurrency: f64,
    pub effective_multiplier_stage_pattern: f64,
    pub effective_multiplier_stage_bandwidth: f64,

    // ===== Extended Config =====
    pub pattern_history_size: usize,
    pub sequential_step_tolerance_bytes: u64,
    pub bandwidth_ema_beta: f64,
    pub nvme_buffer_cap: usize,
    pub ssd_buffer_cap: usize,
    pub hdd_buffer_cap: usize,
    pub is_range_request: bool,
    pub target_read_size: i64,
    pub source_request_size: i64,
}

/// Adaptive I/O strategy calculated from current system load.
///
/// This structure provides optimized I/O parameters based on the observed
/// disk permit wait times. It helps balance throughput vs. latency and
/// prevents I/O saturation under high load.
///
/// # Architecture
///
/// `IoStrategy` now wraps `IoStrategyCore` for better performance and memory efficiency:
/// - **Core fields**: Only runtime-essential data (~20 fields vs 100+ before)
/// - **Debug info**: Optional feature-gated debugging details (~40 fields)
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
    /// Core strategy with runtime-essential fields
    pub core: IoStrategyCore,

    /// Optional debug information (only available with io-scheduler-debug feature)
    #[cfg(feature = "io-scheduler-debug")]
    pub debug_info: IoStrategyDebugInfo,
}

// Implement Deref for transparent access to core fields
impl std::ops::Deref for IoStrategy {
    type Target = IoStrategyCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl std::ops::DerefMut for IoStrategy {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
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

        // Build minimal scheduling context for compatibility path
        let scheduling_context = IoSchedulingContext::from_wait_duration(permit_wait_duration, base_buffer_size);
        #[cfg(feature = "io-scheduler-debug")]
        let load_level_clone = load_level.clone();
        // Build core strategy
        let core = IoStrategyCore {
            // Basic configuration
            storage_media: StorageMedia::Unknown,
            access_pattern: AccessPattern::Unknown,
            request_size: -1,
            base_buffer_size,
            buffer_cap: buffer_size,

            // Runtime decisions
            buffer_size,
            buffer_multiplier,
            enable_readahead,
            cache_writeback_enabled,
            use_buffered_io: true,

            // Performance state
            concurrent_requests: ACTIVE_GET_REQUESTS.load(Ordering::Relaxed),
            observed_bandwidth_bps: None,
            bandwidth_tier: BandwidthTier::Unknown,
            bandwidth_limited: false,
            sequential_detected: false,

            // Decision flags
            storage_profile: StorageProfile::for_media(
                StorageMedia::Unknown,
                rustfs_config::DEFAULT_OBJECT_IO_NVME_BUFFER_CAP,
                rustfs_config::DEFAULT_OBJECT_IO_SSD_BUFFER_CAP,
                rustfs_config::DEFAULT_OBJECT_IO_HDD_BUFFER_CAP,
            ),
            scheduling_context,
            load_level,
            permit_wait_duration,

            // Tuning multipliers
            final_multiplier: buffer_multiplier,
            should_throttle_random_io: false,
            should_expand_for_sequential: false,
            should_reduce_for_concurrency: false,
            should_reduce_for_bandwidth: false,
            should_disable_cache_writeback: !cache_writeback_enabled,
            should_disable_readahead: !enable_readahead,

            // Priority scheduling
            priority_enabled: false,
            priority: IoPriority::Normal,

            // Bandwidth snapshot
            bandwidth_snapshot: None,
        };

        #[cfg(feature = "io-scheduler-debug")]
        let debug_info = IoStrategyDebugInfo {
            readahead_reason: if enable_readahead { "load-based" } else { "high-load" },
            strategy_version: "1.0-compat",
            strategy_reason: "compatibility-path",
            strategy_source: "from_wait_duration",
            notes: "legacy compatibility mode",
            request_class: "unknown",
            io_path_kind: "compat",
            queue_mode: "standard",
            load_level_label: load_level_clone.as_str(),
            pattern_label: "unknown",
            media_label: "unknown",
            bandwidth_label: "unknown",
            storage_profile_buffer_cap_source: "compat",
            is_large_request: false,
            is_small_request: false,
            storage_detection_enabled: rustfs_config::DEFAULT_OBJECT_IO_STORAGE_DETECTION_ENABLE,
            storage_media_override_applied: false,
            used_compatibility_path: true,
            sequential_hint_applied: false,
            observed_bandwidth_available: false,
            read_size_known: false,
            random_penalty_applied: false,
            sequential_boost_applied: false,
            buffer_cap_applied: false,
            clamp_min_applied: buffer_size <= 32 * KI_B,
            clamp_max_applied: buffer_size >= MI_B,
            readahead_disabled_by_concurrency: false,
            readahead_disabled_by_pattern: false,
            readahead_disabled_by_load: !enable_readahead,
            readahead_disabled_by_bandwidth: false,
            cache_writeback_disabled_by_load: !cache_writeback_enabled,
            cache_writeback_disabled_by_pattern: false,
            cache_writeback_disabled_by_request_size: false,
            final_buffer_floor: 32 * KI_B,
            queue_depth_hint: 0,
            permit_wait_ms: permit_wait_duration.as_millis() as u64,
            high_concurrency_threshold: rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
            medium_concurrency_threshold: rustfs_config::DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
            low_bandwidth_threshold_bps: rustfs_config::DEFAULT_OBJECT_IO_BANDWIDTH_LOW_THRESHOLD_BPS,
            high_bandwidth_threshold_bps: rustfs_config::DEFAULT_OBJECT_IO_BANDWIDTH_HIGH_THRESHOLD_BPS,
            random_readahead_disable_concurrency: rustfs_config::DEFAULT_OBJECT_IO_RANDOM_READAHEAD_DISABLE_CONCURRENCY,
            low_priority_size_threshold: rustfs_config::DEFAULT_OBJECT_IO_LOW_PRIORITY_SIZE_THRESHOLD,
            high_priority_size_threshold: rustfs_config::DEFAULT_OBJECT_IO_HIGH_PRIORITY_SIZE_THRESHOLD,
            // queue_capacity_hint: 0,
            // load_sample_window: rustfs_config::DEFAULT_OBJECT_IO_LOAD_SAMPLE_WINDOW,
            // load_high_threshold_ms: rustfs_config::DEFAULT_OBJECT_IO_LOAD_HIGH_THRESHOLD_MS,
            // load_low_threshold_ms: rustfs_config::DEFAULT_OBJECT_IO_LOAD_LOW_THRESHOLD_MS,
            // starvation_prevention_interval_ms: rustfs_config::DEFAULT_OBJECT_IO_STARVATION_PREVENTION_INTERVAL,
            // starvation_threshold_secs: rustfs_config::DEFAULT_OBJECT_IO_STARVATION_THRESHOLD_SECS,
            // max_concurrent_reads: rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
            // priority_queue_high_capacity: rustfs_config::DEFAULT_OBJECT_IO_QUEUE_HIGH_CAPACITY,
            // priority_queue_normal_capacity: rustfs_config::DEFAULT_OBJECT_IO_QUEUE_NORMAL_CAPACITY,
            // priority_queue_low_capacity: rustfs_config::DEFAULT_OBJECT_IO_QUEUE_LOW_CAPACITY,
            pattern_history_size: rustfs_config::DEFAULT_OBJECT_IO_PATTERN_HISTORY_SIZE,
            sequential_step_tolerance_bytes: rustfs_config::DEFAULT_OBJECT_IO_SEQUENTIAL_STEP_TOLERANCE_BYTES,
            bandwidth_ema_beta: rustfs_config::DEFAULT_OBJECT_IO_BANDWIDTH_EMA_BETA,
            nvme_buffer_cap: rustfs_config::DEFAULT_OBJECT_IO_NVME_BUFFER_CAP,
            ssd_buffer_cap: rustfs_config::DEFAULT_OBJECT_IO_SSD_BUFFER_CAP,
            hdd_buffer_cap: rustfs_config::DEFAULT_OBJECT_IO_HDD_BUFFER_CAP,
            is_range_request: false,
            target_read_size: -1,
            source_request_size: -1,
            // profile_prefers_readahead: false,
            // fallback_to_unknown_media: true,
            effective_multiplier_stage_concurrency: buffer_multiplier,
            effective_multiplier_stage_pattern: 1.0,
            effective_multiplier_stage_bandwidth: 1.0,
        };

        #[cfg(not(feature = "io-scheduler-debug"))]
        {
            Self { core }
        }

        #[cfg(feature = "io-scheduler-debug")]
        {
            Self { core, debug_info }
        }
    }

    /// Create a new IoStrategy from enhanced scheduling context and configuration.
    ///
    /// This is the comprehensive multi-factor strategy calculation that integrates:
    /// - Base buffer size from workload configuration
    /// - Permit wait time and load level
    /// - Concurrent request count
    /// - Storage media profile (NVMe/SSD/HDD)
    /// - Access pattern (sequential/random/mixed)
    /// - Observed bandwidth
    ///
    /// # Arguments
    ///
    /// * `context` - Scheduling context with all runtime factors
    /// * `config` - Scheduler configuration with thresholds and caps
    ///
    /// # Returns
    ///
    /// An IoStrategy with optimized parameters based on all factors.
    pub fn from_context_with_config(context: &IoSchedulingContext, config: &IoSchedulerConfig) -> Self {
        // Stage 1: Start with base buffer size
        let mut buffer_size;
        let mut buffer_multiplier = 1.0;

        // Stage 2: Apply load level reduction based on permit wait
        let load_level = IoLoadLevel::from_wait_duration_with_thresholds(
            context.permit_wait_duration,
            config.load_low_threshold_ms,
            config.load_high_threshold_ms,
        );

        let load_multiplier = match load_level {
            IoLoadLevel::Low => 1.0,
            IoLoadLevel::Medium => 0.75,
            IoLoadLevel::High => 0.5,
            IoLoadLevel::Critical => 0.4,
        };
        buffer_multiplier *= load_multiplier;

        // Stage 3: Apply concurrency-based reduction
        let concurrency_multiplier = if context.concurrent_requests >= config.high_concurrency_threshold {
            0.5
        } else if context.concurrent_requests >= config.medium_concurrency_threshold {
            0.75
        } else {
            1.0
        };
        buffer_multiplier *= concurrency_multiplier;

        // Stage 4: Get storage profile for buffer cap and preferences
        let storage_profile = StorageProfile::for_media(
            context.storage_media,
            config.nvme_buffer_cap,
            config.ssd_buffer_cap,
            config.hdd_buffer_cap,
        );

        // Stage 5: Apply access pattern adjustments
        let pattern_multiplier = match context.access_pattern {
            AccessPattern::Sequential => storage_profile.sequential_boost_multiplier,
            AccessPattern::Random => storage_profile.random_penalty_multiplier,
            AccessPattern::Mixed => 1.0,
            AccessPattern::Unknown => 1.0,
        };
        buffer_multiplier *= pattern_multiplier;

        // Stage 6: Apply bandwidth-based reduction
        let (bandwidth_tier, bandwidth_multiplier, bandwidth_limited) = match context.observed_bandwidth_bps {
            Some(bps) if bps < config.bandwidth_low_threshold_bps => {
                // Low bandwidth: reduce buffer size
                (BandwidthTier::Low, 0.6, true)
            }
            Some(bps) if bps < config.bandwidth_high_threshold_bps => {
                // Medium bandwidth: no change
                (BandwidthTier::Medium, 1.0, false)
            }
            Some(_) => {
                // High bandwidth: can use larger buffers
                (BandwidthTier::High, 1.1, false)
            }
            None => {
                // Unknown bandwidth: conservative
                (BandwidthTier::Unknown, 0.9, false)
            }
        };
        buffer_multiplier *= bandwidth_multiplier;

        // Calculate final buffer size with all multipliers applied
        buffer_size = ((context.base_buffer_size as f64) * buffer_multiplier) as usize;

        // Apply storage media cap
        let buffer_cap = storage_profile.buffer_cap;
        #[cfg(feature = "io-scheduler-debug")]
        let buffer_cap_applied = buffer_size > buffer_cap;
        buffer_size = buffer_size.min(buffer_cap);

        // Apply final clamp (safety bounds)
        let clamp_min = 32 * KI_B;
        let clamp_max = MI_B;
        #[cfg(feature = "io-scheduler-debug")]
        let clamp_min_applied = buffer_size < clamp_min;
        #[cfg(feature = "io-scheduler-debug")]
        let clamp_max_applied = buffer_size > clamp_max;
        buffer_size = buffer_size.clamp(clamp_min, clamp_max);

        // Start with storage profile preference
        let mut should_enable_readahead = storage_profile.prefers_readahead;
        // Determine readahead preference
        #[cfg(feature = "io-scheduler-debug")]
        let mut readahead_reason = if storage_profile.prefers_readahead {
            "media-pref"
        } else {
            "media-no-pref"
        };

        // Apply access pattern override
        let readahead_disabled_by_pattern = matches!(context.access_pattern, AccessPattern::Random);
        if readahead_disabled_by_pattern {
            should_enable_readahead = false;
            #[cfg(feature = "io-scheduler-debug")]
            {
                readahead_reason = "random-pattern";
            }
        }

        // Apply concurrency override
        let readahead_disabled_by_concurrency = context.concurrent_requests >= config.random_readahead_disable_concurrency;
        if readahead_disabled_by_concurrency && matches!(context.access_pattern, AccessPattern::Random) {
            should_enable_readahead = false;
            #[cfg(feature = "io-scheduler-debug")]
            {
                readahead_reason = "high-concurrency-random";
            }
        }

        // Apply load override
        let readahead_disabled_by_load = matches!(load_level, IoLoadLevel::High | IoLoadLevel::Critical);
        if readahead_disabled_by_load {
            should_enable_readahead = false;
            #[cfg(feature = "io-scheduler-debug")]
            {
                readahead_reason = "high-load";
            }
        }

        // Apply bandwidth override
        let readahead_disabled_by_bandwidth = bandwidth_limited;
        if readahead_disabled_by_bandwidth {
            should_enable_readahead = false;
            #[cfg(feature = "io-scheduler-debug")]
            {
                readahead_reason = "low-bandwidth";
            }
        }

        let enable_readahead = should_enable_readahead;

        // Determine cache writeback
        let cache_writeback_enabled = match load_level {
            IoLoadLevel::Critical => false,
            _ => !bandwidth_limited,
        };

        #[cfg(feature = "io-scheduler-debug")]
        let cache_writeback_disabled_by_load = matches!(load_level, IoLoadLevel::Critical);
        #[cfg(feature = "io-scheduler-debug")]
        let cache_writeback_disabled_by_pattern = matches!(context.access_pattern, AccessPattern::Random);

        // Calculate priority based on request size
        let priority = if context.file_size > 0 {
            IoPriority::from_size_with_thresholds(
                context.file_size,
                config.high_priority_size_threshold,
                config.low_priority_size_threshold,
            )
        } else {
            IoPriority::Normal
        };
        #[cfg(feature = "io-scheduler-debug")]
        let load_level_clone = load_level.clone();
        // Build core strategy with essential runtime fields
        let core = IoStrategyCore {
            // ===== Basic Configuration =====
            storage_media: context.storage_media,
            access_pattern: context.access_pattern,
            request_size: context.file_size,
            base_buffer_size: context.base_buffer_size,
            buffer_cap,

            // ===== Runtime Decisions =====
            buffer_size,
            buffer_multiplier,
            enable_readahead,
            cache_writeback_enabled,
            use_buffered_io: true,

            // ===== Performance State =====
            concurrent_requests: context.concurrent_requests,
            observed_bandwidth_bps: context.observed_bandwidth_bps,
            bandwidth_tier,
            bandwidth_limited,
            sequential_detected: matches!(context.access_pattern, AccessPattern::Sequential),

            // ===== Decision Flags =====
            storage_profile,
            scheduling_context: context.clone(),
            load_level,
            permit_wait_duration: context.permit_wait_duration,

            // ===== Tuning Multipliers =====
            final_multiplier: buffer_multiplier,
            should_throttle_random_io: matches!(context.access_pattern, AccessPattern::Random),
            should_expand_for_sequential: matches!(context.access_pattern, AccessPattern::Sequential),
            should_reduce_for_concurrency: concurrency_multiplier < 1.0,
            should_reduce_for_bandwidth: bandwidth_limited,
            should_disable_cache_writeback: !cache_writeback_enabled,
            should_disable_readahead: !enable_readahead,

            // ===== Priority Scheduling =====
            priority_enabled: config.enable_priority,
            priority,

            // ===== Bandwidth Snapshot =====
            bandwidth_snapshot: context.observed_bandwidth_bps.map(|bps| BandwidthSnapshot {
                bytes_per_second: bps,
                tier: bandwidth_tier,
            }),
        };

        #[cfg(feature = "io-scheduler-debug")]
        let debug_info = IoStrategyDebugInfo {
            // ===== Decision Labels =====
            readahead_reason,
            strategy_version: "2.0-multi-factor",
            strategy_reason: "multi-factor",
            strategy_source: "from_context_with_config",
            notes: "Multi-factor strategy with media, pattern, and bandwidth awareness",

            // ===== Request Classification =====
            request_class: if context.file_size > 0 {
                if context.file_size < config.high_priority_size_threshold as i64 {
                    "small"
                } else if context.file_size < config.low_priority_size_threshold as i64 {
                    "medium"
                } else {
                    "large"
                }
            } else {
                "unknown"
            },
            io_path_kind: if context.is_sequential_hint { "sequential" } else { "random" },
            queue_mode: match priority {
                IoPriority::High => "high-priority",
                IoPriority::Normal => "normal-priority",
                IoPriority::Low => "low-priority",
            },

            // ===== State Labels =====
            load_level_label: load_level_clone.as_str(),
            pattern_label: context.access_pattern.as_str(),
            media_label: match context.storage_media {
                StorageMedia::Nvme => "nvme",
                StorageMedia::Ssd => "ssd",
                StorageMedia::Hdd => "hdd",
                StorageMedia::Unknown => "unknown",
            },
            bandwidth_label: match bandwidth_tier {
                BandwidthTier::Low => "low",
                BandwidthTier::Medium => "medium",
                BandwidthTier::High => "high",
                BandwidthTier::Unknown => "unknown",
            },
            storage_profile_buffer_cap_source: match context.storage_media {
                StorageMedia::Nvme => "nvme-cap",
                StorageMedia::Ssd => "ssd-cap",
                StorageMedia::Hdd => "hdd-cap",
                StorageMedia::Unknown => "unknown-cap",
            },

            // ===== Decision Flags =====
            is_large_request: context.file_size > config.low_priority_size_threshold as i64,
            is_small_request: context.file_size > 0 && context.file_size < config.high_priority_size_threshold as i64,
            storage_detection_enabled: config.storage_detection_enabled,
            storage_media_override_applied: !config.storage_media_override.is_empty(),
            used_compatibility_path: false,
            sequential_hint_applied: context.is_sequential_hint,
            observed_bandwidth_available: context.observed_bandwidth_bps.is_some(),
            read_size_known: context.file_size > 0,

            // ===== Decision Tracking =====
            random_penalty_applied: matches!(context.access_pattern, AccessPattern::Random),
            sequential_boost_applied: matches!(context.access_pattern, AccessPattern::Sequential),
            buffer_cap_applied,
            clamp_min_applied,
            clamp_max_applied,

            // ===== Readahead Decisions =====
            readahead_disabled_by_concurrency,
            readahead_disabled_by_pattern,
            readahead_disabled_by_load,
            readahead_disabled_by_bandwidth,

            // ===== Cache Writeback Decisions =====
            cache_writeback_disabled_by_load,
            cache_writeback_disabled_by_pattern,
            cache_writeback_disabled_by_request_size: false,

            // ===== Threshold Snapshots =====
            final_buffer_floor: clamp_min,
            queue_depth_hint: context.concurrent_requests,
            permit_wait_ms: context.permit_wait_duration.as_millis() as u64,

            // ===== Configuration Thresholds =====
            high_concurrency_threshold: config.high_concurrency_threshold,
            medium_concurrency_threshold: config.medium_concurrency_threshold,
            low_bandwidth_threshold_bps: config.bandwidth_low_threshold_bps,
            high_bandwidth_threshold_bps: config.bandwidth_high_threshold_bps,
            random_readahead_disable_concurrency: config.random_readahead_disable_concurrency,
            low_priority_size_threshold: config.low_priority_size_threshold,
            high_priority_size_threshold: config.high_priority_size_threshold,

            // ===== Multiplier Breakdown =====
            effective_multiplier_stage_concurrency: concurrency_multiplier,
            effective_multiplier_stage_pattern: pattern_multiplier,
            effective_multiplier_stage_bandwidth: bandwidth_multiplier,

            // ===== Extended Config =====
            pattern_history_size: config.pattern_history_size,
            sequential_step_tolerance_bytes: config.sequential_step_tolerance_bytes,
            bandwidth_ema_beta: config.bandwidth_ema_beta,
            nvme_buffer_cap: config.nvme_buffer_cap,
            ssd_buffer_cap: config.ssd_buffer_cap,
            hdd_buffer_cap: config.hdd_buffer_cap,
            is_range_request: context.file_size > 0 && !context.is_sequential_hint,
            target_read_size: context.file_size,
            source_request_size: context.file_size,
        };

        #[cfg(not(feature = "io-scheduler-debug"))]
        {
            Self { core }
        }

        #[cfg(feature = "io-scheduler-debug")]
        {
            Self { core, debug_info }
        }
    }

    /// Get a human-readable description of the current I/O strategy.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
#[allow(dead_code)]
struct QueuedRequest<T> {
    /// The actual request payload.
    request: T,
    /// Time when the request was enqueued.
    enqueue_time: Instant,
    /// Original priority assigned to the request.
    original_priority: IoPriority,
    /// Current priority (may be boosted for starvation prevention).
    current_priority: IoPriority,
    /// Whether this request was boosted due to starvation prevention.
    starvation_boosted: bool,
}

/// Queue statistics for monitoring.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
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

impl IoPriorityQueueConfig {
    /// Load configuration from environment.
    #[allow(dead_code)]
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

impl<T> IoPriorityQueue<T> {
    /// Create a new priority queue with the given configuration.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub async fn len(&self) -> usize {
        let high_queue = self.high_queue.lock().await;
        let normal_queue = self.normal_queue.lock().await;
        let low_queue = self.low_queue.lock().await;

        high_queue.len() + normal_queue.len() + low_queue.len()
    }

    /// Check if all queues are empty.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

/// Get optimized buffer size for I/O operations.
///
/// This function provides adaptive buffer sizing based on:
/// - File size (small files get smaller buffers)
/// - Concurrent request count (high concurrency gets smaller buffers)
/// - Base buffer size from configuration
///
/// # Arguments
///
/// * `file_size` - Size of the file being read/written (-1 for unknown)
///
/// # Returns
///
/// Optimal buffer size in bytes
///
/// # Example
///
/// ```ignore
/// let buffer_size = get_buffer_size_opt_in(1024 * 1024); // 1MB file
/// assert!(buffer_size >= 64 * 1024); // At least 64KB
/// ```
#[allow(dead_code)]
pub fn get_buffer_size_opt_in(file_size: i64) -> usize {
    // Get base buffer size from configuration
    let base_buffer_size =
        rustfs_utils::get_env_usize(rustfs_config::ENV_OBJECT_IO_BUFFER_SIZE, rustfs_config::DEFAULT_OBJECT_IO_BUFFER_SIZE);

    // Apply concurrency-aware adjustments
    get_concurrency_aware_buffer_size(file_size, base_buffer_size)
}

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
        assert_eq!(config.low_priority_size_threshold, 10 * 1024 * 1024); // 10MB
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

    // ============================================
    // Multi-Factor Strategy Tests
    // ============================================

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_nvme_sequential_low_load() {
        // NVMe + Sequential + Low load = maximum buffer size
        let context = IoSchedulingContext {
            file_size: 100 * 1024 * 1024,                   // 100MB
            base_buffer_size: 256 * 1024,                   // 256KB
            permit_wait_duration: Duration::from_millis(5), // Low load
            is_sequential_hint: true,
            access_pattern: AccessPattern::Sequential,
            storage_media: StorageMedia::Nvme,
            observed_bandwidth_bps: Some(600 * 1024 * 1024), // 600MB/s (High, > 512MB/s threshold)
            concurrent_requests: 2,                          // Low concurrency
        };

        let config = IoSchedulerConfig::default();
        let strategy = IoStrategy::from_context_with_config(&context, &config);

        // Should get large buffer due to NVMe + Sequential + High bandwidth
        assert!(strategy.buffer_size > 256 * 1024, "NVMe sequential should get larger buffer");
        assert!(strategy.enable_readahead, "Sequential reads should enable readahead");
        assert_eq!(strategy.load_level, IoLoadLevel::Low);
        assert_eq!(strategy.storage_media, StorageMedia::Nvme);
        assert_eq!(strategy.access_pattern, AccessPattern::Sequential);
        assert_eq!(strategy.bandwidth_tier, BandwidthTier::High);
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_hdd_random_high_load() {
        // HDD + Random + High load = conservative buffer size
        let context = IoSchedulingContext {
            file_size: 100 * 1024 * 1024,
            base_buffer_size: 256 * 1024,
            permit_wait_duration: Duration::from_millis(100), // High load
            is_sequential_hint: false,
            access_pattern: AccessPattern::Random,
            storage_media: StorageMedia::Hdd,
            observed_bandwidth_bps: Some(10 * 1024 * 1024), // 10MB/s (Low)
            concurrent_requests: 16,                        // High concurrency
        };

        let config = IoSchedulerConfig::default();
        let strategy = IoStrategy::from_context_with_config(&context, &config);

        // Should get small buffer due to HDD + Random + High load + Low bandwidth
        assert!(strategy.buffer_size < 256 * 1024, "HDD random high load should get smaller buffer");
        assert!(!strategy.enable_readahead, "Random reads should disable readahead");
        assert_eq!(strategy.load_level, IoLoadLevel::High);
        assert_eq!(strategy.storage_media, StorageMedia::Hdd);
        assert_eq!(strategy.access_pattern, AccessPattern::Random);
        assert!(strategy.bandwidth_limited, "Low bandwidth should be marked");
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_ssd_mixed_medium_load() {
        // SSD + Mixed + Medium load = moderate buffer
        let context = IoSchedulingContext {
            file_size: 50 * 1024 * 1024,
            base_buffer_size: 256 * 1024,
            permit_wait_duration: Duration::from_millis(30), // Medium load
            is_sequential_hint: false,
            access_pattern: AccessPattern::Mixed,
            storage_media: StorageMedia::Ssd,
            observed_bandwidth_bps: Some(100 * 1024 * 1024), // 100MB/s (Medium)
            concurrent_requests: 6,                          // Medium concurrency
        };

        let config = IoSchedulerConfig::default();
        let strategy = IoStrategy::from_context_with_config(&context, &config);

        // Should get moderate buffer
        assert!(
            strategy.buffer_size >= 128 * 1024 && strategy.buffer_size <= 256 * 1024,
            "SSD mixed medium load should get moderate buffer"
        );
        assert_eq!(strategy.load_level, IoLoadLevel::Medium);
        assert_eq!(strategy.storage_media, StorageMedia::Ssd);
        assert_eq!(strategy.access_pattern, AccessPattern::Mixed);
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_critical_load_disables_features() {
        // Any media + Critical load = minimal features
        let context = IoSchedulingContext {
            file_size: 10 * 1024 * 1024,
            base_buffer_size: 256 * 1024,
            permit_wait_duration: Duration::from_millis(300), // Critical load
            is_sequential_hint: true,
            access_pattern: AccessPattern::Sequential,
            storage_media: StorageMedia::Nvme,
            observed_bandwidth_bps: Some(200 * 1024 * 1024),
            concurrent_requests: 1,
        };

        let config = IoSchedulerConfig::default();
        let strategy = IoStrategy::from_context_with_config(&context, &config);

        // Critical load should disable readahead and cache writeback
        assert_eq!(strategy.load_level, IoLoadLevel::Critical);
        assert!(!strategy.enable_readahead, "Critical load should disable readahead");
        assert!(!strategy.cache_writeback_enabled, "Critical load should disable cache writeback");
        // Buffer: 256KB * 0.4 (critical) * 1.35 (sequential) ≈ 138KB
        assert!(strategy.buffer_size < 200 * 1024, "Critical load should reduce buffer");
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_buffer_cap_enforcement() {
        // Test that storage media caps are enforced
        let context = IoSchedulingContext {
            file_size: 1000 * 1024 * 1024,                  // 1GB
            base_buffer_size: 16 * 1024 * 1024,             // 16MB (very large)
            permit_wait_duration: Duration::from_millis(1), // Low load
            is_sequential_hint: true,
            access_pattern: AccessPattern::Sequential,
            storage_media: StorageMedia::Nvme,
            observed_bandwidth_bps: Some(1000 * 1024 * 1024), // Very high
            concurrent_requests: 1,
        };

        let config = IoSchedulerConfig::default();
        let strategy = IoStrategy::from_context_with_config(&context, &config);

        // Should be capped at NVMe buffer cap and 1MB max
        assert!(strategy.buffer_size <= MI_B, "Should be capped at 1MB max");

        #[cfg(feature = "io-scheduler-debug")]
        assert!(strategy.debug_info.buffer_cap_applied, "Buffer cap should be applied");
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_bandwidth_low_reduces_buffer() {
        // Low bandwidth should reduce buffer
        let context = IoSchedulingContext {
            file_size: 50 * 1024 * 1024,
            base_buffer_size: 512 * 1024,
            permit_wait_duration: Duration::from_millis(10),
            is_sequential_hint: true,
            access_pattern: AccessPattern::Sequential,
            storage_media: StorageMedia::Ssd,
            observed_bandwidth_bps: Some(5 * 1024 * 1024), // 5MB/s (Low)
            concurrent_requests: 2,
        };

        let config = IoSchedulerConfig::default();
        let strategy = IoStrategy::from_context_with_config(&context, &config);

        assert_eq!(strategy.bandwidth_tier, BandwidthTier::Low);
        assert!(strategy.bandwidth_limited, "Low bandwidth should be flagged");
        assert!(!strategy.enable_readahead, "Low bandwidth should disable readahead");
        assert!(strategy.buffer_size < context.base_buffer_size, "Low bandwidth should reduce buffer");
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_high_concurrency_reduction() {
        // High concurrency should reduce buffer
        let context = IoSchedulingContext {
            file_size: 100 * 1024 * 1024,
            base_buffer_size: 512 * 1024,
            permit_wait_duration: Duration::from_millis(10),
            is_sequential_hint: true,
            access_pattern: AccessPattern::Sequential,
            storage_media: StorageMedia::Nvme,
            observed_bandwidth_bps: Some(200 * 1024 * 1024),
            concurrent_requests: 20, // High concurrency (> 16)
        };

        let config = IoSchedulerConfig::default();
        let strategy = IoStrategy::from_context_with_config(&context, &config);

        assert!(strategy.concurrent_requests >= config.high_concurrency_threshold);
        assert!(strategy.should_reduce_for_concurrency, "Should mark concurrency reduction");
        assert!(strategy.buffer_size < context.base_buffer_size, "High concurrency should reduce buffer");
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_sequential_boost() {
        // Sequential reads should get boost
        let sequential_context = IoSchedulingContext {
            file_size: 50 * 1024 * 1024,
            base_buffer_size: 256 * 1024,
            permit_wait_duration: Duration::from_millis(10),
            is_sequential_hint: true,
            access_pattern: AccessPattern::Sequential,
            storage_media: StorageMedia::Ssd,
            observed_bandwidth_bps: Some(100 * 1024 * 1024),
            concurrent_requests: 2,
        };

        let random_context = IoSchedulingContext {
            file_size: 50 * 1024 * 1024,
            base_buffer_size: 256 * 1024,
            permit_wait_duration: Duration::from_millis(10),
            is_sequential_hint: false,
            access_pattern: AccessPattern::Random,
            storage_media: StorageMedia::Ssd,
            observed_bandwidth_bps: Some(100 * 1024 * 1024),
            concurrent_requests: 2,
        };

        let config = IoSchedulerConfig::default();
        let sequential_strategy = IoStrategy::from_context_with_config(&sequential_context, &config);
        let random_strategy = IoStrategy::from_context_with_config(&random_context, &config);

        assert!(
            sequential_strategy.buffer_size > random_strategy.buffer_size,
            "Sequential should get larger buffer than random"
        );

        #[cfg(feature = "io-scheduler-debug")]
        {
            assert!(sequential_strategy.debug_info.sequential_boost_applied, "Should mark sequential boost");
            assert!(random_strategy.debug_info.random_penalty_applied, "Should mark random penalty");
        }
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_unknown_media_conservative() {
        // Unknown media should be conservative
        let context = IoSchedulingContext {
            file_size: 50 * 1024 * 1024,
            base_buffer_size: 256 * 1024,
            permit_wait_duration: Duration::from_millis(10),
            is_sequential_hint: true,
            access_pattern: AccessPattern::Sequential,
            storage_media: StorageMedia::Unknown,
            observed_bandwidth_bps: None,
            concurrent_requests: 2,
        };

        let config = IoSchedulerConfig::default();
        let strategy = IoStrategy::from_context_with_config(&context, &config);

        assert_eq!(strategy.storage_media, StorageMedia::Unknown);
        assert_eq!(strategy.bandwidth_tier, BandwidthTier::Unknown);
        assert!(
            strategy.buffer_size <= context.base_buffer_size,
            "Unknown media should not exceed base buffer"
        );
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_priority_classification() {
        // Test priority classification based on file size
        let small_context = IoSchedulingContext {
            file_size: 500 * 1024, // 500KB (High priority)
            base_buffer_size: 256 * 1024,
            permit_wait_duration: Duration::from_millis(10),
            is_sequential_hint: false,
            access_pattern: AccessPattern::Unknown,
            storage_media: StorageMedia::Ssd,
            observed_bandwidth_bps: Some(100 * 1024 * 1024),
            concurrent_requests: 2,
        };

        let medium_context = IoSchedulingContext {
            file_size: 5 * 1024 * 1024, // 5MB (Normal priority)
            base_buffer_size: 256 * 1024,
            permit_wait_duration: Duration::from_millis(10),
            is_sequential_hint: false,
            access_pattern: AccessPattern::Unknown,
            storage_media: StorageMedia::Ssd,
            observed_bandwidth_bps: Some(100 * 1024 * 1024),
            concurrent_requests: 2,
        };

        let large_context = IoSchedulingContext {
            file_size: 50 * 1024 * 1024, // 50MB (Low priority)
            base_buffer_size: 256 * 1024,
            permit_wait_duration: Duration::from_millis(10),
            is_sequential_hint: false,
            access_pattern: AccessPattern::Unknown,
            storage_media: StorageMedia::Ssd,
            observed_bandwidth_bps: Some(100 * 1024 * 1024),
            concurrent_requests: 2,
        };

        let config = IoSchedulerConfig::default();
        let small_strategy = IoStrategy::from_context_with_config(&small_context, &config);
        let medium_strategy = IoStrategy::from_context_with_config(&medium_context, &config);
        let large_strategy = IoStrategy::from_context_with_config(&large_context, &config);

        assert_eq!(small_strategy.priority, IoPriority::High);
        assert_eq!(medium_strategy.priority, IoPriority::Normal);
        assert_eq!(large_strategy.priority, IoPriority::Low);
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_readahead_decision_matrix() {
        // Test readahead enable/disable logic
        let configs = vec![
            // (media, pattern, load, bandwidth, concurrency, expected_readahead, reason)
            (
                StorageMedia::Nvme,
                AccessPattern::Sequential,
                IoLoadLevel::Low,
                BandwidthTier::High,
                1,
                true,
                "all-favorable",
            ),
            (
                StorageMedia::Hdd,
                AccessPattern::Random,
                IoLoadLevel::Low,
                BandwidthTier::Medium,
                1,
                false,
                "random-pattern",
            ),
            (
                StorageMedia::Ssd,
                AccessPattern::Sequential,
                IoLoadLevel::High,
                BandwidthTier::Medium,
                1,
                false,
                "high-load",
            ),
            (
                StorageMedia::Nvme,
                AccessPattern::Sequential,
                IoLoadLevel::Low,
                BandwidthTier::Low,
                1,
                false,
                "low-bandwidth",
            ),
            (
                StorageMedia::Ssd,
                AccessPattern::Random,
                IoLoadLevel::Low,
                BandwidthTier::High,
                20,
                false,
                "high-concurrency-random",
            ),
        ];

        for (media, pattern, load, bandwidth, concurrency, expected, reason) in configs {
            let context = IoSchedulingContext {
                file_size: 10 * 1024 * 1024,
                base_buffer_size: 256 * 1024,
                permit_wait_duration: match load {
                    IoLoadLevel::Low => Duration::from_millis(5),
                    IoLoadLevel::Medium => Duration::from_millis(30),
                    IoLoadLevel::High => Duration::from_millis(100),
                    IoLoadLevel::Critical => Duration::from_millis(300),
                },
                is_sequential_hint: matches!(pattern, AccessPattern::Sequential),
                access_pattern: pattern,
                storage_media: media,
                observed_bandwidth_bps: match bandwidth {
                    BandwidthTier::Low => Some(5 * 1024 * 1024),
                    BandwidthTier::Medium => Some(100 * 1024 * 1024),
                    BandwidthTier::High => Some(500 * 1024 * 1024),
                    BandwidthTier::Unknown => None,
                },
                concurrent_requests: concurrency,
            };

            let config = IoSchedulerConfig::default();
            let strategy = IoStrategy::from_context_with_config(&context, &config);

            assert_eq!(
                strategy.enable_readahead, expected,
                "Readahead mismatch for case: {}, expected={}, got={}",
                reason, expected, strategy.enable_readahead
            );
        }
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_buffer_multiplier_stages() {
        // Test that all multiplier stages are applied
        let context = IoSchedulingContext {
            file_size: 100 * 1024 * 1024,
            base_buffer_size: 1024 * 1024,                    // 1MB base
            permit_wait_duration: Duration::from_millis(100), // High load (0.5x)
            is_sequential_hint: false,
            access_pattern: AccessPattern::Random, // Penalty (0.8x)
            storage_media: StorageMedia::Ssd,
            observed_bandwidth_bps: Some(5 * 1024 * 1024), // Low bandwidth (0.6x)
            concurrent_requests: 12,                       // High concurrency (0.75x)
        };

        let config = IoSchedulerConfig::default();
        let strategy = IoStrategy::from_context_with_config(&context, &config);

        // Expected multiplier: 1.0 * 0.5 (load) * 0.5 (concurrency, 12>=8) * 0.8 (random) * 0.6 (bandwidth)
        // = 0.12x
        let expected_min = (1024_f64 * 1024_f64) * 0.10_f64; // ~100KB
        let expected_max = (1024_f64 * 1024_f64) * 0.15_f64; // ~150KB

        assert!(
            strategy.buffer_size >= expected_min as usize && strategy.buffer_size <= expected_max as usize,
            "Buffer size {} should be in range [{}, {}] based on combined multipliers",
            strategy.buffer_size,
            expected_min,
            expected_max
        );

        assert!(strategy.should_reduce_for_concurrency);
        assert!(strategy.should_reduce_for_bandwidth);
    }

    #[test]
    #[serial]
    async fn test_multi_factor_strategy_compatibility_path() {
        // Test that compatibility path (from_wait_duration) still works
        let wait_duration = Duration::from_millis(50);
        let base_buffer = 256 * 1024;

        let compat_strategy = IoStrategy::from_wait_duration(wait_duration, base_buffer);

        // 50ms is >= high_threshold (50ms), so it's High load
        assert_eq!(compat_strategy.load_level, IoLoadLevel::High);
        assert!(compat_strategy.buffer_size > 0);
        assert_eq!(compat_strategy.storage_media, StorageMedia::Unknown);
        assert_eq!(compat_strategy.access_pattern, AccessPattern::Unknown);
    }
}
