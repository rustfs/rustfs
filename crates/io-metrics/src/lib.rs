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

//! RustFS metrics collection and reporting.
//!
//! This crate provides the **single source of truth** for all metrics
//! in RustFS. It uses the `metrics` crate for reporting to OTEL exporters.
//!
//! # Architecture
//!
//! - **Free functions**: Simple `record_*()` functions for quick metric reporting
//! - **PerformanceMetrics**: Shared atomic counter struct for advanced use cases
//! - **MetricsCollector**: I/O operation tracking with percentile calculation
//! - **AutoTuner**: Automatic performance optimization based on metrics
//! - **No HTTP metrics endpoint**: consumers emit metrics through the `metrics` crate;
//!   `rustfs-obs` owns OTEL initialization and export
//!
//! # Usage
//!
//! ```rust,no_run
//! use rustfs_io_metrics::{MetricsCollector, PerformanceMetrics, record_get_object};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! # #[tokio::main]
//! # async fn main() {
//! // Simple recording
//! record_get_object(100.0, 1024);
//!
//! // Advanced usage with collector
//! let metrics = Arc::new(PerformanceMetrics::new());
//! let collector = MetricsCollector::new(metrics, 1000);
//! collector.record_io_operation(1024, Duration::from_millis(10), true).await;
//! # }
//! ```

// Import macros from the metrics crate
#[macro_use]
extern crate metrics;

// Public modules
pub mod adaptive_ttl;
pub mod autotuner;
pub mod backpressure_metrics;
pub mod cache_config;
pub mod capacity_metrics;
pub mod collector;
pub mod config;
pub mod deadlock_metrics;
pub mod io_metrics;
pub mod lock_metrics;
pub mod performance;
pub mod process_lock_metrics;
pub mod sampler;
pub mod timeout_metrics;

pub use autotuner::{AutoTuner, TunerConfig, TuningResult};

// Cache config exports
pub use cache_config::{AdaptiveTTL, CacheConfig, CacheConfigError, CacheHealthStatus, CacheStats};

// Adaptive TTL exports
pub use adaptive_ttl::{
    AccessRecord, AccessTracker, AdaptiveTTLStats, record_access_pattern_change, record_early_eviction, record_ttl_adjustment,
    record_ttl_expiration,
};

// Capacity metrics exports
pub use capacity_metrics::{
    record_capacity_cache_hit, record_capacity_cache_miss, record_capacity_cache_served, record_capacity_current_bytes,
    record_capacity_dirty_disk_count, record_capacity_dynamic_timeout, record_capacity_refresh_inflight,
    record_capacity_refresh_joiner, record_capacity_refresh_request, record_capacity_refresh_result,
    record_capacity_refresh_scope, record_capacity_scan_disk, record_capacity_scan_mode, record_capacity_scan_sampling,
    record_capacity_stall_detected, record_capacity_symlink, record_capacity_timeout_fallback, record_capacity_update_completed,
    record_capacity_update_failed, record_capacity_write_operation,
};

// I/O metrics exports
pub use io_metrics::{
    IoSchedulerStats, record_bandwidth_observation, record_buffer_size_adjustment, record_io_priority_decision,
    record_io_scheduler_decision, record_load_level_change, record_queue_operation, record_starvation_event,
};

// Backpressure metrics exports
pub use backpressure_metrics::{
    record_backpressure_activation, record_backpressure_deactivation, record_backpressure_rejection,
    record_backpressure_state_change, record_concurrent_operations,
};

// Deadlock metrics exports
pub use deadlock_metrics::{
    record_deadlock_detected, record_lock_acquisition, record_lock_contention, record_lock_release, record_long_held_lock,
    record_wait_edge_added, record_wait_edge_removed,
};

// Lock metrics exports
pub use lock_metrics::{
    LockMetricsSummary, record_contention_event, record_early_release, record_lock_hold_time, record_lock_optimization_enabled,
    record_spin_attempt, record_spin_count_change,
};

pub use process_lock_metrics::{
    ProcessLockSnapshot, ProcessPlatformSnapshot, record_read_lock_held_acquire, record_read_lock_held_release,
    record_write_lock_held_acquire, record_write_lock_held_release, snapshot_process_lock_counts,
    snapshot_process_platform_stats,
};
pub use sampler::{
    ProcessResourceSnapshot, ProcessStatusSnapshot, ProcessSystemSnapshot, snapshot_process_platform, snapshot_process_resource,
    snapshot_process_resource_and_system, snapshot_process_system,
};

// Timeout metrics exports
pub use timeout_metrics::{
    TimeoutMetricsSummary, record_dynamic_timeout, record_operation_completion, record_operation_duration,
    record_operation_progress, record_stalled_operation, record_timeout_event,
};

// Config exports
pub use config::{
    BackpressureSettings, CacheSettings, DEFAULT_BASE_BUFFER_SIZE, DEFAULT_CACHE_MAX_CAPACITY, DEFAULT_CACHE_MAX_MEMORY,
    DEFAULT_CACHE_TTL_SECS, DEFAULT_MAX_BUFFER_SIZE, DEFAULT_MAX_CONCURRENT_READS, DEFAULT_MIN_BUFFER_SIZE,
    DeadlockDetectionSettings, IoConfig, IoSchedulerSettings, TimeoutSettings,
};

// Re-exports for convenience
pub use collector::MetricsCollector;
pub use performance::PerformanceMetrics;

/// Record GetObject request start.
#[inline(always)]
pub fn record_get_object_request_start(concurrent_requests: usize) {
    counter!("rustfs_io_get_object_requests_total").increment(1);
    gauge!("rustfs_io_get_object_concurrent_requests").set(concurrent_requests as f64);
}

/// Record GetObject request start without concurrency context.
#[inline(always)]
pub fn record_get_object_request_started() {
    counter!("rustfs_io_get_object_requests_total").increment(1);
}

/// Record GetObject request result.
#[inline(always)]
pub fn record_get_object_request_result(status: &str, duration_secs: f64) {
    counter!("rustfs_io_get_object_request_results_total", "status" => status.to_string()).increment(1);
    histogram!("rustfs_io_get_object_request_duration_seconds", "status" => status.to_string()).record(duration_secs);
}

/// Record GetObject timeout for a specific stage.
#[inline(always)]
pub fn record_get_object_timeout(stage: Option<&str>, elapsed_secs: Option<f64>) {
    match stage {
        Some(stage) => counter!("rustfs_io_get_object_timeout_total", "stage" => stage.to_string()).increment(1),
        None => counter!("rustfs_io_get_object_timeout_total").increment(1),
    }

    if let Some(elapsed_secs) = elapsed_secs {
        histogram!("rustfs_io_get_object_timeout_elapsed_seconds").record(elapsed_secs);
    }
}

/// Record GetObject completion.
#[inline(always)]
pub fn record_get_object_completion(total_duration_secs: f64, response_size_bytes: i64, buffer_size_bytes: usize) {
    counter!("rustfs_io_get_object_completed_total").increment(1);
    histogram!("rustfs_io_get_object_total_duration_seconds").record(total_duration_secs);
    histogram!("rustfs_io_get_object_response_size_bytes").record(response_size_bytes as f64);
    histogram!("rustfs_io_get_object_buffer_size_bytes").record(buffer_size_bytes as f64);
}

/// Record I/O queue congestion observation.
#[inline(always)]
pub fn record_io_queue_congestion() {
    counter!("rustfs_io_queue_congestion_total").increment(1);
}

/// Record I/O priority assignment.
#[inline(always)]
pub fn record_io_priority_assignment(priority: &str) {
    counter!("rustfs_io_priority_assigned_total", "priority" => priority.to_string()).increment(1);
}

/// Record detailed GetObject I/O orchestration metrics.
#[inline(always)]
pub fn record_get_object_io_state(
    permit_wait_secs: f64,
    queue_utilization_percent: f64,
    permits_in_use: usize,
    permits_available: usize,
    load_level: &str,
    buffer_multiplier: f64,
) {
    histogram!("rustfs_io_disk_permit_wait_duration_seconds").record(permit_wait_secs);
    gauge!("rustfs_io_queue_utilization_percent").set(queue_utilization_percent);
    gauge!("rustfs_io_queue_permits_in_use").set(permits_in_use as f64);
    gauge!("rustfs_io_queue_permits_available").set(permits_available as f64);
    gauge!("rustfs_io_buffer_multiplier").set(buffer_multiplier);
    counter!("rustfs_io_strategy_selected_total", "level" => load_level.to_string()).increment(1);
}

/// Record a zero-copy read operation.
///
/// # Arguments
///
/// * `size_bytes` - Size of the data read in bytes
/// * `duration_ms` - Time taken for the read operation in milliseconds
#[inline(always)]
pub fn record_zero_copy_read(size_bytes: usize, duration_ms: f64) {
    counter!("rustfs_zero_copy_reads_total").increment(1);
    histogram!("rustfs_zero_copy_read_size_bytes").record(size_bytes as f64);
    histogram!("rustfs_zero_copy_read_duration_ms").record(duration_ms);
}

/// Record memory copies avoided by using zero-copy.
///
/// # Arguments
///
/// * `bytes_saved` - Number of bytes that would have been copied without zero-copy
#[inline(always)]
pub fn record_memory_copy_saved(bytes_saved: usize) {
    counter!("rustfs_zero_copy_memory_saved_bytes_total").increment(bytes_saved as u64);
}

/// Record a fallback from zero-copy to regular read.
///
/// This happens when zero-copy read fails (e.g., mmap not available,
/// file too large, etc.) and the system falls back to regular I/O.
///
/// # Arguments
///
/// * `reason` - Reason for the fallback (e.g., "mmap_unavailable", "file_too_large")
#[inline(always)]
pub fn record_zero_copy_fallback(reason: &str) {
    counter!("rustfs_zero_copy_fallback_total", "reason" => reason.to_string()).increment(1);
}

// ============================================================================
// BytesPool Metrics
// ============================================================================

/// Record BytesPool buffer acquisition.
///
/// # Arguments
///
/// * `tier` - Pool tier ("small", "medium", "large", "xlarge")
/// * `size` - Buffer size acquired
/// * `from_pool` - Whether buffer was reused from pool
#[inline(always)]
pub fn record_bytes_pool_acquire(tier: &str, size: usize, from_pool: bool) {
    counter!("rustfs_bytes_pool_acquisitions_total", "tier" => tier.to_string()).increment(1);
    gauge!("rustfs_bytes_pool_size_bytes", "tier" => tier.to_string()).set(size as f64);

    if from_pool {
        counter!("rustfs_bytes_pool_hits_total", "tier" => tier.to_string()).increment(1);
    } else {
        counter!("rustfs_bytes_pool_misses_total", "tier" => tier.to_string()).increment(1);
    }
}

/// Record BytesPool buffer return.
///
/// # Arguments
///
/// * `tier` - Pool tier ("small", "medium", "large", "xlarge")
#[inline(always)]
pub fn record_bytes_pool_return(tier: &str) {
    counter!("rustfs_bytes_pool_returns_total", "tier" => tier.to_string()).increment(1);
}

/// Record current BytesPool allocated bytes.
///
/// # Arguments
///
/// * `tier` - Pool tier
/// * `bytes` - Currently allocated bytes
#[inline(always)]
pub fn record_bytes_pool_allocated(tier: &str, bytes: u64) {
    gauge!("rustfs_bytes_pool_allocated_bytes", "tier" => tier.to_string()).set(bytes as f64);
}

/// Get BytesPool hit rate as a gauge metric.
///
/// # Arguments
///
/// * `tier` - Pool tier
/// * `hit_rate` - Hit rate (0.0 - 1.0)
#[inline(always)]
pub fn record_bytes_pool_hit_rate(tier: &str, hit_rate: f64) {
    gauge!("rustfs_bytes_pool_hit_rate", "tier" => tier.to_string()).set(hit_rate * 100.0);
}

/// Record zero-copy write operation.
///
/// # Arguments
///
/// * `size_bytes` - Size of the data written in bytes
/// * `duration_ms` - Time taken for the write operation in milliseconds
#[inline(always)]
pub fn record_zero_copy_write(size_bytes: usize, duration_ms: f64) {
    counter!("rustfs_zero_copy_write_total").increment(1);
    histogram!("rustfs_zero_copy_write_size_bytes").record(size_bytes as f64);
    histogram!("rustfs_zero_copy_write_duration_ms").record(duration_ms);
}

/// Record zero-copy write fallback.
///
/// This happens when zero-copy write fails and the system falls back to regular I/O.
///
/// # Arguments
///
/// * `reason` - Reason for the fallback
#[inline(always)]
pub fn record_zero_copy_write_fallback(reason: &str) {
    counter!("rustfs_zero_copy_write_fallback_total", "reason" => reason.to_string()).increment(1);
}

/// Record bytes saved from zero-copy.
///
/// # Arguments
///
/// * `size_bytes` - Number of bytes saved from zero-copy
#[inline(always)]
pub fn record_bytes_saved(size_bytes: usize) {
    counter!("rustfs_zero_copy_bytes_saved_total").increment(size_bytes as u64);
}

// ============================================================================
// S3 Operation Metrics (GetObject, PutObject, etc.)
// ============================================================================

/// Record GetObject operation metrics.
///
/// # Arguments
///
/// * `duration_ms` - Operation duration in milliseconds
/// * `size_bytes` - Object size in bytes
///
/// Note: this function records aggregate S3 GET metrics only. It must not be
/// interpreted as the definitive source of truth for data-plane copy mode.
#[inline(always)]
pub fn record_get_object(duration_ms: f64, size_bytes: i64) {
    counter!("rustfs_s3_get_object_total").increment(1);
    histogram!("rustfs_s3_get_object_duration_ms").record(duration_ms);

    if size_bytes > 0 {
        histogram!("rustfs_s3_get_object_size_bytes").record(size_bytes as f64);
    }
}

/// Record PutObject operation metrics.
///
/// # Arguments
///
/// * `duration_ms` - Operation duration in milliseconds
/// * `size_bytes` - Object size in bytes
/// * `zero_copy_enabled` - Whether zero-copy was enabled for this operation
#[inline(always)]
pub fn record_put_object(duration_ms: f64, size_bytes: i64, zero_copy_enabled: bool) {
    counter!("rustfs_s3_put_object_total").increment(1);
    histogram!("rustfs_s3_put_object_duration_ms").record(duration_ms);

    if size_bytes > 0 {
        histogram!("rustfs_s3_put_object_size_bytes").record(size_bytes as f64);
    }

    if zero_copy_enabled {
        counter!("rustfs_s3_put_object_zero_copy_enabled_total").increment(1);
    }
}

/// Record ListObjects operation metrics.
///
/// # Arguments
///
/// * `duration_ms` - Operation duration in milliseconds
/// * `objects_count` - Number of objects returned
/// * `is_truncated` - Whether the response was truncated
#[inline(always)]
pub fn record_list_objects(duration_ms: f64, objects_count: u64, is_truncated: bool) {
    counter!("rustfs_s3_list_objects_total").increment(1);
    histogram!("rustfs_s3_list_objects_duration_ms").record(duration_ms);
    histogram!("rustfs_s3_list_objects_count").record(objects_count as f64);

    if is_truncated {
        counter!("rustfs_s3_list_objects_truncated_total").increment(1);
    }
}

/// Record DeleteObject operation metrics.
///
/// # Arguments
///
/// * `duration_ms` - Operation duration in milliseconds
/// * `version_deleted` - Whether a specific version was deleted
#[inline(always)]
pub fn record_delete_object(duration_ms: f64, version_deleted: bool) {
    counter!("rustfs_s3_delete_object_total").increment(1);
    histogram!("rustfs_s3_delete_object_duration_ms").record(duration_ms);

    if version_deleted {
        counter!("rustfs_s3_delete_object_version_total").increment(1);
    }
}

// ============================================================================
// I/O Scheduler Metrics
// ============================================================================

/// Record I/O scheduler strategy selection.
///
/// # Arguments
///
/// * `storage_media` - Detected storage media type ("nvme", "ssd", "hdd", "unknown")
/// * `access_pattern` - Detected access pattern ("sequential", "random", "mixed", "unknown")
/// * `buffer_size` - Selected buffer size in bytes
/// * `concurrent_requests` - Number of concurrent requests
#[inline(always)]
pub fn record_io_strategy(storage_media: &str, access_pattern: &str, buffer_size: usize, concurrent_requests: u64) {
    counter!("rustfs_io_strategy_total",
        "storage_media" => storage_media.to_string(),
        "access_pattern" => access_pattern.to_string(),
    )
    .increment(1);

    gauge!("rustfs_io_buffer_size_bytes",
        "storage_media" => storage_media.to_string(),
    )
    .set(buffer_size as f64);

    gauge!("rustfs_io_concurrent_requests").set(concurrent_requests as f64);
}

/// Record disk permit wait time (load tracking).
///
/// # Arguments
///
/// * `duration_ms` - Time spent waiting for disk permit
#[inline(always)]
pub fn record_permit_wait(duration_ms: f64) {
    histogram!("rustfs_io_permit_wait_duration_ms").record(duration_ms);
}

/// Record I/O load level.
///
/// # Arguments
///
/// * `load_level` - Current load level ("low", "medium", "high", "critical")
/// * `concurrent_requests` - Number of concurrent requests
#[inline(always)]
pub fn record_io_load_level(load_level: &str, concurrent_requests: u64) {
    counter!("rustfs_io_load_level",
        "level" => load_level.to_string(),
    )
    .increment(1);

    gauge!("rustfs_io_concurrent_requests").set(concurrent_requests as f64);
}

/// Record cache size and entry count.
///
/// # Arguments
///
/// * `tier` - Cache tier ("l1", "l2")
/// * `size_bytes` - Total cache size in bytes
/// * `entries` - Number of entries in the cache
#[inline(always)]
pub fn record_cache_size(tier: &str, size_bytes: usize, entries: u64) {
    gauge!("rustfs_cache_size_bytes",
        "tier" => tier.to_string(),
    )
    .set(size_bytes as f64);

    gauge!("rustfs_cache_entries",
        "tier" => tier.to_string(),
    )
    .set(entries as f64);
}

// ============================================================================
// Bandwidth Monitoring Metrics
// ============================================================================

/// Record bandwidth observation.
///
/// # Arguments
///
/// * `bytes_per_second` - Observed bandwidth in bytes per second
/// * `tier` - Bandwidth tier ("low", "medium", "high", "unknown")
#[inline(always)]
pub fn record_bandwidth(bytes_per_second: u64, tier: &str) {
    gauge!("rustfs_bandwidth_current_bps").set(bytes_per_second as f64);
    gauge!("rustfs_bandwidth_current_bps",
        "tier" => tier.to_string(),
    )
    .set(bytes_per_second as f64);

    histogram!("rustfs_bandwidth_observed_bps").record(bytes_per_second as f64);
}

/// Record data transfer for bandwidth calculation.
///
/// # Arguments
///
/// * `bytes` - Number of bytes transferred
/// * `duration_ms` - Duration of the transfer in milliseconds
#[inline(always)]
pub fn record_data_transfer(bytes: u64, duration_ms: f64) {
    counter!("rustfs_io_transfer_bytes_total").increment(bytes);
    histogram!("rustfs_io_transfer_duration_ms").record(duration_ms);

    if duration_ms > 0.0 {
        let bps = (bytes as f64 * 1000.0) / duration_ms;
        histogram!("rustfs_io_transfer_bandwidth_bps").record(bps);
    }
}

// ============================================================================
// System Resource Metrics
// ============================================================================

/// Record memory usage.
///
/// # Arguments
///
/// * `used_bytes` - Used memory in bytes
/// * `total_bytes` - Total memory in bytes
#[inline(always)]
pub fn record_memory_usage(used_bytes: u64, total_bytes: u64) {
    gauge!("rustfs_memory_used_bytes").set(used_bytes as f64);
    gauge!("rustfs_memory_total_bytes").set(total_bytes as f64);

    if total_bytes > 0 {
        let usage_percent = (used_bytes as f64 / total_bytes as f64) * 100.0;
        gauge!("rustfs_memory_usage_percent").set(usage_percent);
    }
}

/// Record CPU usage.
///
/// # Arguments
///
/// * `percent` - CPU usage percentage (0.0 - 100.0)
#[inline(always)]
pub fn record_cpu_usage(percent: f64) {
    gauge!("rustfs_cpu_usage_percent").set(percent);
}

/// Record disk I/O statistics.
///
/// # Arguments
///
/// * `read_bytes` - Bytes read
/// * `write_bytes` - Bytes written
/// * `read_ops` - Number of read operations
/// * `write_ops` - Number of write operations
#[inline(always)]
pub fn record_disk_io(read_bytes: u64, write_bytes: u64, read_ops: u64, write_ops: u64) {
    counter!("rustfs_disk_read_bytes").increment(read_bytes);
    counter!("rustfs_disk_write_bytes").increment(write_bytes);
    counter!("rustfs_disk_read_ops").increment(read_ops);
    counter!("rustfs_disk_write_ops").increment(write_ops);

    gauge!("rustfs_disk_read_bytes_total").set(read_bytes as f64);
    gauge!("rustfs_disk_write_bytes_total").set(write_bytes as f64);
}

// ============================================================================
// Error and Timeout Metrics
// ============================================================================

/// Record operation error.
///
/// # Arguments
///
/// * `operation` - Operation type (e.g., "get_object", "put_object")
/// * `error_type` - Error type (e.g., "timeout", "disk_error", "network")
#[inline(always)]
pub fn record_error(operation: &str, error_type: &str) {
    counter!("rustfs_errors_total",
        "operation" => operation.to_string(),
        "type" => error_type.to_string(),
    )
    .increment(1);
}

/// Record operation timeout.
///
/// # Arguments
///
/// * `operation` - Operation type that timed out
/// * `duration_ms` - Duration before timeout
#[inline(always)]
pub fn record_timeout(operation: &str, duration_ms: f64) {
    counter!("rustfs_timeouts_total",
        "operation" => operation.to_string(),
    )
    .increment(1);

    histogram!("rustfs_timeouts_duration_ms",
        "operation" => operation.to_string(),
    )
    .record(duration_ms);
}

/// Record retry attempt.
///
/// # Arguments
///
/// * `operation` - Operation being retried
/// * `attempt_number` - Attempt number (1-based)
#[inline(always)]
pub fn record_retry(operation: &str, attempt_number: u32) {
    counter!("rustfs_retries_total",
        "operation" => operation.to_string(),
    )
    .increment(1);

    histogram!("rustfs_retries_attempt",
        "operation" => operation.to_string(),
    )
    .record(attempt_number as f64);
}

// ============================================================================
// Helper Metrics (for MetricsCollector)
// ============================================================================

/// Record I/O latency in milliseconds.
///
/// # Arguments
///
/// * `latency_ms` - I/O latency in milliseconds
#[inline(always)]
pub fn record_io_latency(latency_ms: f64) {
    histogram!("rustfs_io_latency_ms").record(latency_ms);
}

/// Record I/O latency P95 in milliseconds.
///
/// # Arguments
///
/// * `latency_ms` - P95 I/O latency in milliseconds
#[inline(always)]
pub fn record_io_latency_p95(latency_ms: f64) {
    gauge!("rustfs_io_latency_p95_ms").set(latency_ms);
}

/// Record I/O latency P99 in milliseconds.
///
/// # Arguments
///
/// * `latency_ms` - P99 I/O latency in milliseconds
#[inline(always)]
pub fn record_io_latency_p99(latency_ms: f64) {
    gauge!("rustfs_io_latency_p99_ms").set(latency_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_zero_copy_read() {
        record_zero_copy_read(1024, 10.5);
        record_memory_copy_saved(1024);
        record_zero_copy_fallback("test");
    }

    #[test]
    fn test_record_bytes_pool_metrics() {
        record_bytes_pool_acquire("small", 4096, true);
        record_bytes_pool_return("small");
        record_bytes_pool_allocated("small", 4096);
        record_bytes_pool_hit_rate("small", 0.85);
    }

    #[test]
    fn test_record_zero_copy_write() {
        record_zero_copy_write(1024, 10.5);
        record_zero_copy_write_fallback("test");
        record_bytes_saved(1024);
    }

    // S3 Operation Metrics Tests
    #[test]
    fn test_record_get_object() {
        record_get_object(100.0, 1024 * 1024);
        record_get_object(50.0, 2048);
    }

    #[test]
    fn test_record_put_object() {
        record_put_object(200.0, 1024 * 1024, true);
        record_put_object(100.0, 512, false);
    }

    #[test]
    fn test_record_list_objects() {
        record_list_objects(50.0, 100, false);
        record_list_objects(75.0, 1000, true);
    }

    #[test]
    fn test_record_delete_object() {
        record_delete_object(25.0, false);
        record_delete_object(30.0, true);
    }

    // I/O Scheduler Metrics Tests
    #[test]
    fn test_record_io_strategy() {
        record_io_strategy("nvme", "sequential", 256 * 1024, 5);
        record_io_strategy("ssd", "random", 64 * 1024, 10);
    }

    #[test]
    fn test_record_permit_wait() {
        record_permit_wait(5.0);
        record_permit_wait(10.5);
    }

    #[test]
    fn test_record_io_load_level() {
        record_io_load_level("low", 2);
        record_io_load_level("medium", 5);
        record_io_load_level("high", 15);
    }

    #[test]
    fn test_record_cache_size() {
        record_cache_size("l1", 50 * 1024 * 1024, 1000);
        record_cache_size("l2", 200 * 1024 * 1024, 5000);
    }

    // Bandwidth Metrics Tests
    #[test]
    fn test_record_bandwidth() {
        record_bandwidth(100 * 1024 * 1024, "high");
        record_bandwidth(50 * 1024 * 1024, "medium");
    }

    #[test]
    fn test_record_data_transfer() {
        record_data_transfer(1024 * 1024, 100.0);
        record_data_transfer(2048, 50.0);
    }

    // System Resource Metrics Tests
    #[test]
    fn test_record_memory_usage() {
        record_memory_usage(1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024);
        record_memory_usage(2 * 1024 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_record_cpu_usage() {
        record_cpu_usage(25.5);
        record_cpu_usage(50.0);
        record_cpu_usage(75.5);
    }

    #[test]
    fn test_record_disk_io() {
        record_disk_io(1024 * 1024, 2048, 100, 50);
        record_disk_io(2048, 4096, 200, 100);
    }

    // Error and Timeout Metrics Tests
    #[test]
    fn test_record_error() {
        record_error("get_object", "timeout");
        record_error("put_object", "disk_error");
    }

    #[test]
    fn test_record_timeout() {
        record_timeout("get_object", 5000.0);
        record_timeout("list_objects", 10000.0);
    }

    #[test]
    fn test_record_retry() {
        record_retry("get_object", 1);
        record_retry("put_object", 2);
    }
}

// ============================================================================
// Zero-Copy Optimization Metrics (Phase 1 Extension)
// ============================================================================

pub mod bandwidth;
pub mod global_metrics;
pub mod metric_names;

pub use metric_names::zero_copy;

/// Record a zero-copy buffer operation.
///
/// This function records metrics for zero-copy buffer operations,
/// including the operation type and size.
#[inline(always)]
pub fn record_zero_copy_buffer_operation(operation: &str, size: usize) {
    counter!(
        zero_copy::BUFFER_OPERATIONS_TOTAL,
        "operation" => operation.to_string()
    )
    .increment(1);

    counter!(
        zero_copy::BUFFER_BYTES_TOTAL,
        "operation" => operation.to_string()
    )
    .increment(size as u64);
}

/// Record memory copy operations.
///
/// This function tracks the number and size of memory copies,
/// which should be minimized in zero-copy paths.
#[inline(always)]
pub fn record_memory_copy(count: u32, size: usize) {
    counter!(zero_copy::MEMORY_COPY_TOTAL).increment(count as u64);

    counter!(zero_copy::MEMORY_COPY_BYTES_TOTAL).increment(size as u64);

    histogram!("rustfs_memory_copy_size_bytes").record(size as f64);
}

/// Record a shared reference operation.
///
/// This function tracks operations that create or use shared references
/// for zero-copy data sharing.
#[inline(always)]
pub fn record_shared_ref_operation(operation: &str) {
    counter!(
        zero_copy::SHARED_REF_OPERATIONS_TOTAL,
        "operation" => operation.to_string()
    )
    .increment(1);
}

/// Record BufReader optimization.
///
/// This function tracks BufReader layer elimination and buffer size
/// adjustments.
#[inline(always)]
pub fn record_bufreader_optimization(layers_eliminated: u32, buffer_size: usize) {
    counter!(zero_copy::BUFREADER_LAYERS_ELIMINATED_TOTAL).increment(layers_eliminated as u64);

    histogram!(zero_copy::BUFREADER_BUFFER_SIZE_BYTES).record(buffer_size as f64);
}

/// Record Direct I/O operation.
///
/// This function tracks Direct I/O operations and their success/fallback
/// status.
#[inline(always)]
pub fn record_direct_io_operation(operation: &str, size: usize, success: bool) {
    let status = if success { "success" } else { "fallback" };

    counter!(
        zero_copy::DIRECT_IO_OPERATIONS_TOTAL,
        "operation" => operation.to_string(),
        "status" => status.to_string()
    )
    .increment(1);

    counter!(
        zero_copy::DIRECT_IO_BYTES_TOTAL,
        "operation" => operation.to_string(),
        "status" => status.to_string()
    )
    .increment(size as u64);
}

/// Update zero-copy performance metrics.
///
/// This function updates gauge metrics for overall zero-copy performance.
#[inline(always)]
pub fn update_zero_copy_performance_metrics(copy_count: u32, throughput_mbps: f64, memory_saved: u64) {
    gauge!(zero_copy::AVG_COPY_COUNT).set(copy_count as f64);

    gauge!(zero_copy::THROUGHPUT_MBPS).set(throughput_mbps);

    gauge!(zero_copy::MEMORY_SAVED_BYTES).set(memory_saved as f64);
}

// ============================================================================
// Zero-Copy Metrics Tests
// ============================================================================

#[cfg(test)]
mod zero_copy_tests {
    use super::*;

    #[test]
    fn test_record_zero_copy_buffer_operation() {
        // This test verifies the function compiles and runs
        // Actual metric verification requires a metrics recorder
        record_zero_copy_buffer_operation("read", 1024);
        record_zero_copy_buffer_operation("write", 2048);
    }

    #[test]
    fn test_record_memory_copy() {
        record_memory_copy(1, 1024);
        record_memory_copy(2, 2048);
    }

    #[test]
    fn test_record_shared_ref_operation() {
        record_shared_ref_operation("create");
        record_shared_ref_operation("share");
    }

    #[test]
    fn test_record_bufreader_optimization() {
        record_bufreader_optimization(1, 8192);
        record_bufreader_optimization(2, 65536);
    }

    #[test]
    fn test_record_direct_io_operation() {
        record_direct_io_operation("read", 4096, true);
        record_direct_io_operation("write", 8192, false);
    }

    #[test]
    fn test_update_zero_copy_performance_metrics() {
        update_zero_copy_performance_metrics(2, 150.5, 1024 * 1024);
    }

    #[test]
    fn test_metric_names() {
        // Verify metric names are defined
        assert!(!zero_copy::BUFFER_OPERATIONS_TOTAL.is_empty());
        assert!(!zero_copy::MEMORY_COPY_TOTAL.is_empty());
        assert!(!zero_copy::THROUGHPUT_MBPS.is_empty());
    }
}
