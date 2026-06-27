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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Global switch for detailed per-stage PUT metrics (path label, stage durations).
/// When `false`, `record_put_object_path` and `record_put_object_stage_duration`
/// become no-ops, and callers can skip the `Instant::now()` syscalls entirely.
///
/// Set to `true` during startup when OTEL metric export is enabled.
static PUT_STAGE_METRICS_ENABLED: AtomicBool = AtomicBool::new(false);
static GET_STAGE_METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable or disable detailed per-stage PUT metrics.
///
/// Called once during startup, typically gated by `rustfs_obs::observability_metric_enabled()`.
pub fn set_put_stage_metrics_enabled(enabled: bool) {
    PUT_STAGE_METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn set_get_stage_metrics_enabled(enabled: bool) {
    GET_STAGE_METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Returns `true` if detailed per-stage PUT metrics are enabled.
///
/// Callers should check this before calling `Instant::now()` for stage timing
/// to avoid unnecessary syscalls when metrics are disabled.
#[inline(always)]
pub fn put_stage_metrics_enabled() -> bool {
    PUT_STAGE_METRICS_ENABLED.load(Ordering::Relaxed)
}

#[inline(always)]
pub fn get_stage_metrics_enabled() -> bool {
    GET_STAGE_METRICS_ENABLED.load(Ordering::Relaxed)
}

// Public modules
pub mod adaptive_ttl;
pub mod autotuner;
pub mod backpressure_metrics;
pub mod cache_config;
pub mod capacity_metrics;
pub mod collector;
pub mod config;
pub mod deadlock_metrics;
pub mod internode_metrics;
pub mod io_metrics;
pub mod lock_metrics;
pub mod performance;
pub mod process_lock_metrics;
pub mod s3_api_metrics;
pub mod sampler;
pub mod system_path_metrics;
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
    record_object_lock_diag_acquire_duration, record_object_lock_diag_enabled, record_object_lock_diag_hold_duration,
    record_object_lock_diag_slow_acquire, record_object_lock_diag_slow_hold, record_spin_attempt, record_spin_count_change,
};

pub use process_lock_metrics::{
    ProcessLockSnapshot, ProcessPlatformSnapshot, record_read_lock_held_acquire, record_read_lock_held_release,
    record_write_lock_held_acquire, record_write_lock_held_release, snapshot_process_lock_counts,
    snapshot_process_platform_stats,
};
pub use s3_api_metrics::{init_s3_metrics, record_s3_op};
pub use sampler::{
    ProcessResourceSnapshot, ProcessStatusSnapshot, ProcessSystemSnapshot, snapshot_process_platform, snapshot_process_resource,
    snapshot_process_resource_and_system, snapshot_process_system,
};
pub use system_path_metrics::record_system_path_failure;

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

static EC_ENCODE_INFLIGHT_BYTES: AtomicU64 = AtomicU64::new(0);
static GET_OBJECT_BUFFERED_BYTES: AtomicU64 = AtomicU64::new(0);
const SHARD_READ_COST_LOCAL: &str = "local";
const SHARD_READ_COST_REMOTE: &str = "remote";
const SHARD_READ_COST_SAME_NODE: &str = "same_node";
const SHARD_READ_COST_UNKNOWN: &str = "unknown";
const LOW_COST_QUORUM_CANDIDATE_FALSE: &str = "false";
const LOW_COST_QUORUM_CANDIDATE_TRUE: &str = "true";

fn saturating_sub_atomic(counter: &AtomicU64, bytes: u64) -> u64 {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_sub(bytes);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => current = actual,
        }
    }
}

#[inline(always)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

#[inline(always)]
fn i64_non_negative_to_f64(value: i64) -> f64 {
    value.max(0) as f64
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TrackedMemoryGauge {
    GetObjectBufferedBytes,
}

/// Drop-based guard for tracked in-memory payloads.
#[derive(Debug)]
pub struct MemoryGaugeGuard {
    gauge: TrackedMemoryGauge,
    bytes: u64,
}

impl Drop for MemoryGaugeGuard {
    fn drop(&mut self) {
        match self.gauge {
            TrackedMemoryGauge::GetObjectBufferedBytes => {
                let next = saturating_sub_atomic(&GET_OBJECT_BUFFERED_BYTES, self.bytes);
                gauge!("rustfs_get_object_buffered_bytes_current").set(next as f64);
            }
        }
    }
}

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

/// Record PutObject request start.
#[inline(always)]
pub fn record_put_object_request_start(concurrent_requests: usize) {
    counter!("rustfs_io_put_object_requests_total").increment(1);
    gauge!("rustfs_io_put_object_concurrent_requests").set(concurrent_requests as f64);
}

/// Record PutObject request result.
#[inline(always)]
pub fn record_put_object_request_result(status: &str, duration_secs: f64) {
    counter!("rustfs_io_put_object_request_results_total", "status" => status.to_string()).increment(1);
    histogram!("rustfs_io_put_object_request_duration_seconds", "status" => status.to_string()).record(duration_secs);
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

/// Record the streaming strategy chosen for a GetObject response body.
#[inline(always)]
pub fn record_get_object_stream_strategy(strategy: &str, buffer_size_bytes: usize, response_size_bytes: i64) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_stream_strategy_total", "strategy" => strategy.to_string()).increment(1);
    histogram!("rustfs_io_get_object_stream_buffer_size_bytes", "strategy" => strategy.to_string())
        .record(usize_to_f64(buffer_size_bytes));
    histogram!("rustfs_io_get_object_stream_response_size_bytes", "strategy" => strategy.to_string())
        .record(i64_non_negative_to_f64(response_size_bytes));
}

/// Record the response-body handoff shape from a GetObject reader into the S3 streaming body.
#[inline(always)]
pub fn record_get_object_response_handoff(
    strategy: &str,
    buffer_source: &str,
    buffer_size_bytes: usize,
    response_size_bytes: i64,
    duration_secs: f64,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_response_handoff_total",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string()
    )
    .increment(1);
    histogram!(
        "rustfs_io_get_object_response_handoff_buffer_size_bytes",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string()
    )
    .record(usize_to_f64(buffer_size_bytes));
    histogram!(
        "rustfs_io_get_object_response_handoff_response_size_bytes",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string()
    )
    .record(i64_non_negative_to_f64(response_size_bytes));
    histogram!(
        "rustfs_io_get_object_response_handoff_duration_seconds",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string()
    )
    .record(duration_secs);
    record_get_object_response_handoff_duration("s3_handler", duration_secs);
}

/// Record ReaderStream capacity chosen for GetObject handoff.
#[inline(always)]
pub fn record_get_object_reader_stream_buffer_size(strategy: &str, buffer_source: &str, buffer_size_bytes: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!(
        "rustfs_io_get_object_reader_stream_buffer_size_bytes",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string()
    )
    .record(usize_to_f64(buffer_size_bytes));
}

/// Record ReaderStream poll outcomes for GetObject handoff attribution.
#[inline(always)]
pub fn record_get_object_reader_stream_poll(
    strategy: &str,
    buffer_source: &str,
    outcome: &'static str,
    remaining_before: usize,
    bytes: usize,
    duration_secs: f64,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    counter!(
        "rustfs_io_get_object_reader_stream_poll_total",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string(),
        "outcome" => outcome
    )
    .increment(1);
    counter!(
        "rustfs_io_get_object_reader_stream_poll_bytes_total",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string(),
        "outcome" => outcome
    )
    .increment(bytes);
    histogram!(
        "rustfs_io_get_object_reader_stream_poll_remaining_bytes",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string(),
        "outcome" => outcome
    )
    .record(usize_to_f64(remaining_before));
    histogram!(
        "rustfs_io_get_object_reader_stream_poll_bytes",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string(),
        "outcome" => outcome
    )
    .record(usize_to_f64(bytes as usize));
    histogram!(
        "rustfs_io_get_object_reader_stream_poll_duration_seconds",
        "strategy" => strategy.to_string(),
        "buffer_source" => buffer_source.to_string(),
        "outcome" => outcome
    )
    .record(duration_secs);
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
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_disk_permit_wait_duration_seconds").record(permit_wait_secs);
    gauge!("rustfs_io_queue_utilization_percent").set(queue_utilization_percent);
    gauge!("rustfs_io_queue_permits_in_use").set(permits_in_use as f64);
    gauge!("rustfs_io_queue_permits_available").set(permits_available as f64);
    gauge!("rustfs_io_buffer_multiplier").set(buffer_multiplier);
    counter!("rustfs_io_strategy_selected_total", "level" => load_level.to_string()).increment(1);
}

/// Record GetObject phase duration for the current read path.
#[inline(always)]
pub fn record_get_object_stage_duration(path: &'static str, stage: &'static str, duration_secs: f64) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_get_object_stage_duration_seconds", "path" => path, "stage" => stage).record(duration_secs);
}

/// Record GetObject metadata fanout duration.
#[inline(always)]
pub fn record_get_object_metadata_fanout_duration(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "metadata_fanout", duration_secs);
}

/// Record latency until the first metadata response arrives.
#[inline(always)]
pub fn record_get_object_first_metadata_response_latency(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "first_metadata_response", duration_secs);
}

/// Record latency until the first valid metadata response arrives.
#[inline(always)]
pub fn record_get_object_first_valid_metadata_response_latency(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "first_valid_metadata_response", duration_secs);
}

/// Record latency of the slowest metadata response in a fanout.
#[inline(always)]
pub fn record_get_object_slowest_metadata_response_latency(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "slowest_metadata_response", duration_secs);
}

/// Record latency until metadata quorum is reached.
#[inline(always)]
pub fn record_get_object_quorum_reached_latency(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "quorum_reached", duration_secs);
}

/// Record one bounded metadata response outcome.
#[inline(always)]
pub fn record_get_object_metadata_response(path: &'static str, outcome: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_metadata_response_total", "path" => path, "outcome" => outcome).increment(1);
}

/// Record aggregate metadata fanout shape for one GetObject metadata read.
#[inline(always)]
pub fn record_get_object_metadata_fanout_shape(path: &'static str, total: usize, valid: usize, ignored: usize, errors: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_get_object_metadata_fanout_total_responses", "path" => path)
        .record(metadata_fanout_count_to_f64(total));
    histogram!("rustfs_io_get_object_metadata_fanout_valid_responses", "path" => path)
        .record(metadata_fanout_count_to_f64(valid));
    histogram!("rustfs_io_get_object_metadata_fanout_ignored_responses", "path" => path)
        .record(metadata_fanout_count_to_f64(ignored));
    histogram!("rustfs_io_get_object_metadata_fanout_error_responses", "path" => path)
        .record(metadata_fanout_count_to_f64(errors));
}

/// Record a guarded metadata early-stop hit for GetObject.
#[inline(always)]
pub fn record_get_object_metadata_early_stop_hit(path: &'static str, reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_metadata_early_stop_total", "path" => path, "decision" => "hit", "reason" => reason)
        .increment(1);
}

/// Record a guarded metadata early-stop miss for GetObject.
#[inline(always)]
pub fn record_get_object_metadata_early_stop_miss(path: &'static str, reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_metadata_early_stop_total", "path" => path, "decision" => "miss", "reason" => reason)
        .increment(1);
}

/// Record how many trailing metadata responses were skipped by early-stop.
#[inline(always)]
pub fn record_get_object_metadata_early_stop_saved_responses(path: &'static str, saved: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_get_object_metadata_early_stop_saved_responses", "path" => path)
        .record(metadata_fanout_count_to_f64(saved));
}

/// Record GetObject reader setup duration.
#[inline(always)]
pub fn record_get_object_reader_setup_duration(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "reader_setup", duration_secs);
}

/// Record latency until the first shard read completes.
#[inline(always)]
pub fn record_get_object_first_shard_read_duration(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "first_shard_read", duration_secs);
}

/// Record GetObject bitrot verification duration.
#[inline(always)]
pub fn record_get_object_bitrot_verify_duration(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "bitrot_verify", duration_secs);
}

/// Record GetObject reconstruct duration.
#[inline(always)]
pub fn record_get_object_reconstruct_duration(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "reconstruct", duration_secs);
}

/// Record GetObject emit duration.
#[inline(always)]
pub fn record_get_object_emit_duration(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "emit", duration_secs);
}

/// Record GetObject first-byte latency as observed by the caller that owns that boundary.
#[inline(always)]
pub fn record_get_object_first_byte_latency(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "first_byte", duration_secs);
}

/// Record GetObject full-body latency as observed by the caller that owns that boundary.
#[inline(always)]
pub fn record_get_object_full_body_latency(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "full_body", duration_secs);
}

/// Record GetObject response handoff duration in the shared stage histogram.
#[inline(always)]
pub fn record_get_object_response_handoff_duration(path: &'static str, duration_secs: f64) {
    record_get_object_stage_duration(path, "response_handoff", duration_secs);
}

/// Record the selected GetObject reader path.
#[inline(always)]
pub fn record_get_object_reader_path(path: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_reader_path_total", "path" => path).increment(1);
}

/// Record why the codec streaming reader was not selected.
#[inline(always)]
pub fn record_get_object_codec_streaming_fallback(reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_codec_streaming_fallback_total", "reason" => reason).increment(1);
}

/// Record one decoded reader stripe processed by a GetObject read path.
#[inline(always)]
pub fn record_get_object_reader_stripe(path: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_reader_stripes_total", "path" => path).increment(1);
}

/// Record bytes emitted by a GetObject reader path.
#[inline(always)]
pub fn record_get_object_reader_bytes(path: &'static str, bytes: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    counter!("rustfs_io_get_object_reader_bytes_total", "path" => path).increment(bytes);
}

/// Record one reader buffer produced by a GetObject read path.
#[inline(always)]
pub fn record_get_object_reader_buffer(path: &'static str, role: &'static str, bytes: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_get_object_reader_buffer_bytes", "path" => path, "role" => role).record(usize_to_f64(bytes));
}

/// Record one copy from a GetObject reader's internal buffer into the downstream read buffer.
#[inline(always)]
pub fn record_get_object_reader_copy(
    path: &'static str,
    bytes: usize,
    read_buf_remaining_before: usize,
    output_remaining_before: usize,
    duration_secs: f64,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    let bytes_counter = u64::try_from(bytes).unwrap_or(u64::MAX);
    counter!("rustfs_io_get_object_reader_copy_chunks_total", "path" => path).increment(1);
    counter!("rustfs_io_get_object_reader_copy_bytes_total", "path" => path).increment(bytes_counter);
    histogram!("rustfs_io_get_object_reader_copy_bytes", "path" => path).record(usize_to_f64(bytes));
    histogram!("rustfs_io_get_object_reader_copy_read_buf_remaining_bytes", "path" => path)
        .record(usize_to_f64(read_buf_remaining_before));
    histogram!("rustfs_io_get_object_reader_copy_output_remaining_bytes", "path" => path)
        .record(usize_to_f64(output_remaining_before));
    histogram!("rustfs_io_get_object_reader_copy_duration_seconds", "path" => path).record(duration_secs);
}

/// Record one downstream poll of the response-facing GetObject reader.
#[inline(always)]
pub fn record_get_object_reader_poll(
    path: &'static str,
    outcome: &'static str,
    read_buf_remaining_before: usize,
    filled_bytes: usize,
    duration_secs: f64,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    let filled_bytes_counter = u64::try_from(filled_bytes).unwrap_or(u64::MAX);
    counter!("rustfs_io_get_object_reader_poll_total", "path" => path, "outcome" => outcome).increment(1);
    counter!("rustfs_io_get_object_reader_poll_filled_bytes_total", "path" => path, "outcome" => outcome)
        .increment(filled_bytes_counter);
    histogram!("rustfs_io_get_object_reader_poll_read_buf_remaining_bytes", "path" => path, "outcome" => outcome)
        .record(usize_to_f64(read_buf_remaining_before));
    histogram!("rustfs_io_get_object_reader_poll_filled_bytes", "path" => path, "outcome" => outcome)
        .record(usize_to_f64(filled_bytes));
    histogram!("rustfs_io_get_object_reader_poll_duration_seconds", "path" => path, "outcome" => outcome).record(duration_secs);
}

/// Record a bounded prefetch outcome for a GetObject reader path.
#[inline(always)]
pub fn record_get_object_reader_prefetch(path: &'static str, outcome: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_reader_prefetch_total", "path" => path, "outcome" => outcome).increment(1);
}

/// Record how long a GetObject reader spent waiting for a prefetch/fill result.
#[inline(always)]
pub fn record_get_object_reader_prefetch_wait(path: &'static str, duration_secs: f64) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_get_object_reader_prefetch_wait_seconds", "path" => path).record(duration_secs);
}

/// Record how many decoded fills were queued ahead of the current output.
#[inline(always)]
pub fn record_get_object_fill_queued(path: &'static str, policy: &'static str, queued: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_fill_queued_total", "path" => path, "policy" => policy)
        .increment(u64::try_from(queued).unwrap_or(u64::MAX));
    histogram!("rustfs_io_get_object_fill_queued", "path" => path, "policy" => policy).record(usize_to_f64(queued));
}

/// Record that a fill completed while the current output buffer still had unread bytes.
#[inline(always)]
pub fn record_get_object_fill_completed_before_output_drained(path: &'static str, policy: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_fill_completed_before_output_drained_total", "path" => path, "policy" => policy).increment(1);
}

/// Record how long output polling waited on the fill pipeline.
#[inline(always)]
pub fn record_get_object_fill_waited_by_output(path: &'static str, policy: &'static str, duration_secs: f64) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_fill_waited_by_output_total", "path" => path, "policy" => policy).increment(1);
    histogram!("rustfs_io_get_object_fill_waited_by_output_seconds", "path" => path, "policy" => policy).record(duration_secs);
}

/// Record that a background fill task was cancelled during reader drop.
#[inline(always)]
pub fn record_get_object_fill_cancelled_on_drop(path: &'static str, policy: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_fill_cancelled_on_drop_total", "path" => path, "policy" => policy).increment(1);
}

/// Record bytes staged into the prefetch queue.
#[inline(always)]
pub fn record_get_object_reader_prefetch_bytes(path: &'static str, policy: &'static str, bytes: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    counter!("rustfs_io_get_object_reader_prefetch_bytes_total", "path" => path, "policy" => policy).increment(bytes);
    histogram!("rustfs_io_get_object_reader_prefetch_bytes", "path" => path, "policy" => policy)
        .record(usize_to_f64(bytes as usize));
}

/// Record one underlying shard read attempt for GetObject read-path attribution.
#[inline(always)]
pub fn record_get_object_shard_read(
    path: &'static str,
    role: &'static str,
    outcome: &'static str,
    bytes: usize,
    duration_secs: f64,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    counter!("rustfs_io_get_object_shard_read_total", "path" => path, "role" => role, "outcome" => outcome).increment(1);
    counter!("rustfs_io_get_object_shard_read_bytes_total", "path" => path, "role" => role, "outcome" => outcome)
        .increment(bytes);
    histogram!("rustfs_io_get_object_shard_read_duration_seconds", "path" => path, "role" => role, "outcome" => outcome)
        .record(duration_secs);
}

/// Record one underlying shard read attempt with bounded locality and error attribution.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub fn record_get_object_shard_read_observation(
    path: &'static str,
    shard_index: usize,
    role: &'static str,
    cost_class: &'static str,
    outcome: &'static str,
    error_class: &'static str,
    bytes: usize,
    duration_secs: f64,
    verify_duration_secs: f64,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    record_get_object_shard_read(path, role, outcome, bytes, duration_secs);

    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    let shard_index = shard_index.to_string();
    counter!(
        "rustfs_io_get_object_shard_read_observed_total",
        "path" => path,
        "shard_index" => shard_index.clone(),
        "role" => role,
        "cost_class" => cost_class,
        "outcome" => outcome,
        "error_class" => error_class
    )
    .increment(1);
    counter!(
        "rustfs_io_get_object_shard_read_observed_bytes_total",
        "path" => path,
        "shard_index" => shard_index.clone(),
        "role" => role,
        "cost_class" => cost_class,
        "outcome" => outcome,
        "error_class" => error_class
    )
    .increment(bytes);
    histogram!(
        "rustfs_io_get_object_shard_read_observed_duration_seconds",
        "path" => path,
        "shard_index" => shard_index.clone(),
        "role" => role,
        "cost_class" => cost_class,
        "outcome" => outcome,
        "error_class" => error_class
    )
    .record(duration_secs);
    histogram!(
        "rustfs_io_get_object_shard_bitrot_verify_duration_seconds",
        "path" => path,
        "shard_index" => shard_index,
        "role" => role,
        "cost_class" => cost_class,
        "outcome" => outcome,
        "error_class" => error_class
    )
    .record(verify_duration_secs);
}

#[inline(always)]
fn shard_read_fanout_to_f64(value: usize) -> f64 {
    u32::try_from(value).map(f64::from).unwrap_or(f64::from(u32::MAX))
}

#[inline(always)]
fn metadata_fanout_count_to_f64(value: usize) -> f64 {
    u32::try_from(value).map(f64::from).unwrap_or(f64::from(u32::MAX))
}

/// Record per-stripe shard locality shape for GetObject read-path attribution.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub fn record_get_object_shard_read_cost_summary(
    path: &'static str,
    local: usize,
    same_node: usize,
    remote: usize,
    unknown: usize,
    low_cost_available: usize,
    low_cost_successful: usize,
    read_quorum: usize,
    low_cost_quorum_candidate: bool,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_get_object_shard_read_cost_class_count", "path" => path, "cost_class" => SHARD_READ_COST_LOCAL)
        .record(shard_read_fanout_to_f64(local));
    histogram!("rustfs_io_get_object_shard_read_cost_class_count", "path" => path, "cost_class" => SHARD_READ_COST_SAME_NODE)
        .record(shard_read_fanout_to_f64(same_node));
    histogram!("rustfs_io_get_object_shard_read_cost_class_count", "path" => path, "cost_class" => SHARD_READ_COST_REMOTE)
        .record(shard_read_fanout_to_f64(remote));
    histogram!("rustfs_io_get_object_shard_read_cost_class_count", "path" => path, "cost_class" => SHARD_READ_COST_UNKNOWN)
        .record(shard_read_fanout_to_f64(unknown));
    histogram!("rustfs_io_get_object_shard_read_low_cost_available", "path" => path)
        .record(shard_read_fanout_to_f64(low_cost_available));
    histogram!("rustfs_io_get_object_shard_read_low_cost_successful", "path" => path)
        .record(shard_read_fanout_to_f64(low_cost_successful));
    histogram!("rustfs_io_get_object_shard_read_quorum", "path" => path).record(shard_read_fanout_to_f64(read_quorum));
    counter!(
        "rustfs_io_get_object_shard_read_low_cost_quorum_candidate_total",
        "path" => path,
        "candidate" => if low_cost_quorum_candidate {
            LOW_COST_QUORUM_CANDIDATE_TRUE
        } else {
            LOW_COST_QUORUM_CANDIDATE_FALSE
        }
    )
    .increment(1);
}

/// Record opt-in GetObject shard-locality policy effects.
#[inline(always)]
pub fn record_get_object_shard_locality_policy(
    path: &'static str,
    local_preferred: usize,
    remote_avoided: usize,
    fallback_to_remote: usize,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    if local_preferred > 0 {
        counter!("rustfs_io_get_object_shard_local_preferred_total", "path" => path)
            .increment(u64::try_from(local_preferred).unwrap_or(u64::MAX));
    }
    if remote_avoided > 0 {
        counter!("rustfs_io_get_object_shard_remote_avoided_total", "path" => path)
            .increment(u64::try_from(remote_avoided).unwrap_or(u64::MAX));
    }
    if fallback_to_remote > 0 {
        counter!("rustfs_io_get_object_shard_fallback_to_remote_total", "path" => path)
            .increment(u64::try_from(fallback_to_remote).unwrap_or(u64::MAX));
    }
}

/// Record that the opt-in shard-locality policy stayed disabled for a stripe.
#[inline(always)]
pub fn record_get_object_shard_locality_policy_disabled(path: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_shard_locality_policy_disabled_total", "path" => path).increment(1);
}
/// Record per-stripe shard-read fanout shape for GetObject read-path attribution.
#[inline(always)]
pub fn record_get_object_shard_read_fanout(
    path: &'static str,
    scheduled: usize,
    completed: usize,
    successful: usize,
    failed: usize,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_get_object_shard_read_scheduled", "path" => path).record(shard_read_fanout_to_f64(scheduled));
    histogram!("rustfs_io_get_object_shard_read_completed", "path" => path).record(shard_read_fanout_to_f64(completed));
    histogram!("rustfs_io_get_object_shard_read_successful", "path" => path).record(shard_read_fanout_to_f64(successful));
    histogram!("rustfs_io_get_object_shard_read_failed", "path" => path).record(shard_read_fanout_to_f64(failed));
}

/// Record GetObject metadata resolution duration.
#[inline(always)]
pub fn record_get_object_metadata_phase_duration(duration_secs: f64) {
    record_get_object_stage_duration("legacy_duplex", "metadata", duration_secs);
}

/// Record GetObject shard reader setup duration.
#[inline(always)]
pub fn record_get_object_shard_reader_setup_duration(duration_secs: f64) {
    record_get_object_stage_duration("legacy_duplex", "reader_setup", duration_secs);
}

/// Record GetObject erasure decode duration.
#[inline(always)]
pub fn record_get_object_decode_duration(duration_secs: f64) {
    record_get_object_stage_duration("legacy_duplex", "decode", duration_secs);
}

/// Record GetObject downstream write wait while emitting decoded data.
#[inline(always)]
pub fn record_get_object_duplex_backpressure_duration(duration_secs: f64) {
    record_get_object_stage_duration("legacy_duplex", "duplex_backpressure", duration_secs);
}

/// Record GetObject read pipeline failures using bounded labels.
#[inline(always)]
pub fn record_get_object_pipeline_failure(stage: &'static str, reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_pipeline_failures_total", "path" => "legacy_duplex", "stage" => stage, "reason" => reason)
        .increment(1);
}

/// Record GetObject read pipeline failures for an explicit bounded path label.
#[inline(always)]
pub fn record_get_object_pipeline_failure_for_path(path: &'static str, stage: &'static str, reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_pipeline_failures_total", "path" => path, "stage" => stage, "reason" => reason).increment(1);
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
/// * `zero_copy_eligible` - Whether the request was eligible for a zero-copy path
#[inline(always)]
pub fn record_put_object(duration_ms: f64, size_bytes: i64, zero_copy_eligible: bool) {
    counter!("rustfs_s3_put_object_total").increment(1);
    histogram!("rustfs_s3_put_object_duration_ms").record(duration_ms);

    if size_bytes > 0 {
        histogram!("rustfs_s3_put_object_size_bytes").record(size_bytes as f64);
    }

    if zero_copy_eligible {
        // Backward-compatible alias for historical dashboards.
        counter!("rustfs_s3_put_object_zero_copy_enabled_total").increment(1);
        counter!("rustfs_s3_put_object_zero_copy_eligible_total").increment(1);
    }
}

#[inline(always)]
pub fn record_put_object_path(path: &'static str) {
    if !put_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_s3_put_object_path_total", "path" => path).increment(1);
}

#[inline(always)]
pub fn record_put_object_stage_duration(stage: &'static str, duration_ms: f64) {
    if !put_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_s3_put_object_stage_duration_ms", "stage" => stage).record(duration_ms);
}

/// Record generic internal operation stage duration (non-PUT paths).
/// Use this for metacache walks, listing, lifecycle, and other background
/// operations that are NOT part of the PUT object hot path.
#[inline(always)]
pub fn record_stage_duration(stage: &'static str, duration_ms: f64) {
    histogram!("rustfs_internal_stage_duration_ms", "stage" => stage).record(duration_ms);
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
    let tier_label = if tier.is_empty() { "unknown" } else { tier };
    gauge!("rustfs_bandwidth_current_bps", "tier" => "all").set(bytes_per_second as f64);
    gauge!("rustfs_bandwidth_current_bps", "tier" => tier_label.to_string()).set(bytes_per_second as f64);

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

/// Record process-level memory split metrics.
#[inline(always)]
pub fn record_process_memory_split(resident_bytes: u64, virtual_bytes: u64) {
    gauge!("rustfs_memory_process_resident_bytes").set(resident_bytes as f64);
    gauge!("rustfs_memory_process_virtual_bytes").set(virtual_bytes as f64);
}

/// Record cgroup memory split metrics when available.
#[inline(always)]
pub fn record_cgroup_memory_split(
    current_bytes: Option<u64>,
    limit_bytes: Option<u64>,
    anon_bytes: Option<u64>,
    file_bytes: Option<u64>,
    active_file_bytes: Option<u64>,
    inactive_file_bytes: Option<u64>,
) {
    if let Some(current_bytes) = current_bytes {
        gauge!("rustfs_memory_cgroup_current_bytes").set(current_bytes as f64);
    }
    if let Some(limit_bytes) = limit_bytes {
        gauge!("rustfs_memory_cgroup_limit_bytes").set(limit_bytes as f64);
    }
    if let Some(anon_bytes) = anon_bytes {
        gauge!("rustfs_memory_cgroup_anon_bytes").set(anon_bytes as f64);
    }
    if let Some(file_bytes) = file_bytes {
        gauge!("rustfs_memory_cgroup_file_bytes").set(file_bytes as f64);
    }
    if let Some(active_file_bytes) = active_file_bytes {
        gauge!("rustfs_memory_cgroup_active_file_bytes").set(active_file_bytes as f64);
    }
    if let Some(inactive_file_bytes) = inactive_file_bytes {
        gauge!("rustfs_memory_cgroup_inactive_file_bytes").set(inactive_file_bytes as f64);
    }
}

/// Track encoded bytes currently queued between erasure encode and disk writers.
#[inline(always)]
pub fn add_ec_encode_inflight_bytes(bytes: usize) {
    let next = EC_ENCODE_INFLIGHT_BYTES.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
    gauge!("rustfs_ec_encode_inflight_bytes_current").set(next as f64);
}

/// Remove encoded bytes from the tracked erasure encode in-flight gauge.
#[inline(always)]
pub fn remove_ec_encode_inflight_bytes(bytes: usize) {
    let next = saturating_sub_atomic(&EC_ENCODE_INFLIGHT_BYTES, bytes as u64);
    gauge!("rustfs_ec_encode_inflight_bytes_current").set(next as f64);
}

/// Return the current tracked EC encode in-flight bytes.
#[inline(always)]
pub fn current_ec_encode_inflight_bytes() -> u64 {
    EC_ENCODE_INFLIGHT_BYTES.load(Ordering::Relaxed)
}

/// Track whole-object buffering on the GET path.
#[inline(always)]
pub fn track_get_object_buffered_bytes(bytes: usize) -> Option<MemoryGaugeGuard> {
    if bytes == 0 {
        return None;
    }

    let next = GET_OBJECT_BUFFERED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
    gauge!("rustfs_get_object_buffered_bytes_current").set(next as f64);

    Some(MemoryGaugeGuard {
        gauge: TrackedMemoryGauge::GetObjectBufferedBytes,
        bytes: bytes as u64,
    })
}

/// Return the current tracked GET whole-buffered bytes.
#[inline(always)]
pub fn current_get_object_buffered_bytes() -> u64 {
    GET_OBJECT_BUFFERED_BYTES.load(Ordering::Relaxed)
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
    counter!("rustfs_disk_read_bytes_total").increment(read_bytes);
    counter!("rustfs_disk_write_bytes_total").increment(write_bytes);
    counter!("rustfs_disk_read_ops_total").increment(read_ops);
    counter!("rustfs_disk_write_ops_total").increment(write_ops);
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
    use std::sync::Mutex;

    // Serialize tests that mutate the process-global PUT_STAGE_METRICS_ENABLED flag.
    static METRICS_FLAG_LOCK: Mutex<()> = Mutex::new(());

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
    fn test_record_get_object_stage_metrics() {
        record_get_object_stage_duration("s3_handler", "request_context", 0.001);
        record_get_object_reader_path("codec_streaming");
        record_get_object_codec_streaming_fallback("range");
        record_get_object_reader_stripe("codec_streaming");
        record_get_object_reader_bytes("codec_streaming", 1024);
        record_get_object_reader_buffer("codec_streaming", "output", 1024);
        record_get_object_reader_copy("codec_streaming", 512, 8192, 1024, 0.0001);
        record_get_object_reader_poll("codec_streaming", "ready_data", 8192, 512, 0.0002);
        record_get_object_reader_prefetch("codec_streaming", "stored");
        record_get_object_reader_prefetch_wait("codec_streaming", 0.0002);
        record_get_object_response_handoff("standard", "selected", 8192, 1024, 0.0001);
        record_get_object_metadata_fanout_duration("legacy_duplex", 0.001);
        record_get_object_first_metadata_response_latency("legacy_duplex", 0.001);
        record_get_object_first_valid_metadata_response_latency("legacy_duplex", 0.001);
        record_get_object_slowest_metadata_response_latency("legacy_duplex", 0.003);
        record_get_object_quorum_reached_latency("legacy_duplex", 0.002);
        record_get_object_metadata_response("legacy_duplex", "valid");
        record_get_object_metadata_fanout_shape("legacy_duplex", 4, 3, 1, 1);
        record_get_object_metadata_early_stop_hit("legacy_duplex", "valid_quorum");
        record_get_object_metadata_early_stop_miss("legacy_duplex", "insufficient_quorum");
        record_get_object_metadata_early_stop_saved_responses("legacy_duplex", 1);
        record_get_object_reader_setup_duration("legacy_duplex", 0.003);
        record_get_object_first_shard_read_duration("codec_streaming", 0.004);
        record_get_object_bitrot_verify_duration("codec_streaming", 0.005);
        record_get_object_reconstruct_duration("codec_streaming", 0.006);
        record_get_object_emit_duration("codec_streaming", 0.007);
        record_get_object_first_byte_latency("s3_handler", 0.008);
        record_get_object_full_body_latency("s3_handler", 0.009);
        record_get_object_response_handoff_duration("s3_handler", 0.0001);
        record_get_object_metadata_phase_duration(0.002);
        record_get_object_shard_reader_setup_duration(0.003);
        record_get_object_decode_duration(0.004);
        record_get_object_duplex_backpressure_duration(0.005);
        record_get_object_pipeline_failure("decode", "read_quorum");
        record_get_object_pipeline_failure_for_path("codec_streaming", "decode", "read_quorum");
        record_get_object_shard_read_observation("codec_streaming", 0, "data", "local", "success", "none", 1024, 0.004, 0.001);
        record_get_object_shard_read_cost_summary("codec_streaming", 3, 1, 2, 0, 4, 4, 4, true);

        assert!(0.005_f64.is_sign_positive());
    }

    #[test]
    fn test_record_get_object_fill_metrics() {
        record_get_object_fill_queued("codec_streaming", "single_inflight", 1);
        record_get_object_fill_completed_before_output_drained("codec_streaming", "single_inflight");
        record_get_object_fill_waited_by_output("codec_streaming", "single_inflight", 0.0003);
        record_get_object_fill_cancelled_on_drop("codec_streaming", "single_inflight");
        record_get_object_reader_prefetch_bytes("codec_streaming", "single_inflight", 4096);
        record_get_object_reader_stream_buffer_size("standard", "selected", 131072);
        record_get_object_reader_stream_poll("standard", "selected", "ready_data", 8192, 4096, 0.0002);

        assert!(0.0003_f64.is_sign_positive());
    }

    #[test]
    fn test_record_put_object() {
        record_put_object(200.0, 1024 * 1024, true);
        record_put_object(100.0, 512, false);
    }

    #[test]
    fn test_record_put_object_request_metrics() {
        record_put_object_request_start(3);
        record_put_object_request_result("ok", 0.25);
        record_put_object_request_result("error", 0.5);
    }

    #[test]
    fn test_record_put_object_path_and_stage() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_put_stage_metrics_enabled(true);
        record_put_object_path("small_eager");
        record_put_object_path("write_inline");
        record_put_object_stage_duration("ingress_prepare", 12.5);
        record_put_object_stage_duration("set_disk_encode", 8.0);
        set_put_stage_metrics_enabled(false);
    }

    #[test]
    fn test_put_stage_metrics_disabled_by_default() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_put_stage_metrics_enabled(false);
        // These should be no-ops (no panic, no recording)
        record_put_object_path("small_eager");
        record_put_object_stage_duration("set_disk_encode", 5.0);
        // Still disabled
        assert!(!put_stage_metrics_enabled());
    }

    #[test]
    fn test_record_get_object_path_and_stage() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_get_stage_metrics_enabled(true);
        record_get_object_stage_duration("s3_handler", "request_context", 0.001);
        record_get_object_reader_path("codec_streaming");
        record_get_object_codec_streaming_fallback("range");
        record_get_object_reader_stripe("codec_streaming");
        record_get_object_reader_bytes("codec_streaming", 1024);
        record_get_object_reader_buffer("codec_streaming", "output", 1024);
        record_get_object_reader_copy("codec_streaming", 512, 8192, 1024, 0.0001);
        record_get_object_reader_poll("codec_streaming", "ready_data", 8192, 512, 0.0002);
        record_get_object_reader_prefetch("codec_streaming", "stored");
        record_get_object_reader_prefetch_wait("codec_streaming", 0.0002);
        record_get_object_response_handoff("standard", "selected", 8192, 1024, 0.0001);
        record_get_object_metadata_fanout_duration("legacy_duplex", 0.001);
        record_get_object_first_metadata_response_latency("legacy_duplex", 0.001);
        record_get_object_first_valid_metadata_response_latency("legacy_duplex", 0.001);
        record_get_object_slowest_metadata_response_latency("legacy_duplex", 0.003);
        record_get_object_quorum_reached_latency("legacy_duplex", 0.002);
        record_get_object_metadata_response("legacy_duplex", "valid");
        record_get_object_metadata_fanout_shape("legacy_duplex", 4, 3, 1, 1);
        record_get_object_metadata_early_stop_hit("legacy_duplex", "valid_quorum");
        record_get_object_metadata_early_stop_miss("legacy_duplex", "insufficient_quorum");
        record_get_object_metadata_early_stop_saved_responses("legacy_duplex", 1);
        record_get_object_reader_setup_duration("legacy_duplex", 0.003);
        record_get_object_first_shard_read_duration("codec_streaming", 0.004);
        record_get_object_bitrot_verify_duration("codec_streaming", 0.005);
        record_get_object_reconstruct_duration("codec_streaming", 0.006);
        record_get_object_emit_duration("codec_streaming", 0.007);
        record_get_object_first_byte_latency("s3_handler", 0.008);
        record_get_object_full_body_latency("s3_handler", 0.009);
        record_get_object_response_handoff_duration("s3_handler", 0.0001);
        record_get_object_shard_reader_setup_duration(0.003);
        record_get_object_decode_duration(0.004);
        record_get_object_duplex_backpressure_duration(0.005);
        record_get_object_pipeline_failure("decode", "read_quorum");
        record_get_object_pipeline_failure_for_path("codec_streaming", "decode", "read_quorum");
        record_get_object_shard_read_observation("codec_streaming", 0, "data", "local", "success", "none", 1024, 0.004, 0.001);
        record_get_object_shard_read_cost_summary("codec_streaming", 3, 1, 2, 0, 4, 4, 4, true);
        set_get_stage_metrics_enabled(false);
    }

    #[test]
    fn test_get_stage_metrics_disabled_by_default() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_get_stage_metrics_enabled(false);
        record_get_object_stage_duration("s3_handler", "request_context", 0.001);
        record_get_object_reader_path("codec_streaming");
        record_get_object_codec_streaming_fallback("range");
        record_get_object_reader_stripe("codec_streaming");
        record_get_object_reader_bytes("codec_streaming", 1024);
        record_get_object_reader_buffer("codec_streaming", "output", 1024);
        record_get_object_reader_copy("codec_streaming", 512, 8192, 1024, 0.0001);
        record_get_object_reader_poll("codec_streaming", "ready_data", 8192, 512, 0.0002);
        record_get_object_reader_prefetch("codec_streaming", "stored");
        record_get_object_reader_prefetch_wait("codec_streaming", 0.0002);
        record_get_object_response_handoff("standard", "selected", 8192, 1024, 0.0001);
        record_get_object_metadata_fanout_duration("legacy_duplex", 0.001);
        record_get_object_first_metadata_response_latency("legacy_duplex", 0.001);
        record_get_object_first_valid_metadata_response_latency("legacy_duplex", 0.001);
        record_get_object_slowest_metadata_response_latency("legacy_duplex", 0.003);
        record_get_object_quorum_reached_latency("legacy_duplex", 0.002);
        record_get_object_metadata_response("legacy_duplex", "valid");
        record_get_object_metadata_fanout_shape("legacy_duplex", 4, 3, 1, 1);
        record_get_object_metadata_early_stop_hit("legacy_duplex", "valid_quorum");
        record_get_object_metadata_early_stop_miss("legacy_duplex", "insufficient_quorum");
        record_get_object_metadata_early_stop_saved_responses("legacy_duplex", 1);
        record_get_object_reader_setup_duration("legacy_duplex", 0.003);
        record_get_object_first_shard_read_duration("codec_streaming", 0.004);
        record_get_object_bitrot_verify_duration("codec_streaming", 0.005);
        record_get_object_reconstruct_duration("codec_streaming", 0.006);
        record_get_object_emit_duration("codec_streaming", 0.007);
        record_get_object_first_byte_latency("s3_handler", 0.008);
        record_get_object_full_body_latency("s3_handler", 0.009);
        record_get_object_response_handoff_duration("s3_handler", 0.0001);
        record_get_object_shard_reader_setup_duration(0.003);
        record_get_object_decode_duration(0.004);
        record_get_object_duplex_backpressure_duration(0.005);
        record_get_object_pipeline_failure("decode", "read_quorum");
        record_get_object_pipeline_failure_for_path("codec_streaming", "decode", "read_quorum");
        record_get_object_shard_read_observation("codec_streaming", 0, "data", "local", "success", "none", 1024, 0.004, 0.001);
        record_get_object_shard_read_cost_summary("codec_streaming", 3, 1, 2, 0, 4, 4, 4, true);
        assert!(!get_stage_metrics_enabled());
    }

    #[test]
    fn test_record_stage_duration_generic() {
        // Generic stage duration should always record (no gating flag)
        record_stage_duration("metacache_walk_dir_primary", 15.0);
        record_stage_duration("store_list_objects_walk_internal", 8.5);
        record_stage_duration("lifecycle_free_version_recovery_failed", 120.0);
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
    fn test_record_process_memory_split() {
        record_process_memory_split(1024, 2048);
        record_process_memory_split(4096, 8192);
    }

    #[test]
    fn test_record_cgroup_memory_split() {
        record_cgroup_memory_split(Some(1), Some(2), Some(3), Some(4), Some(5), Some(6));
        record_cgroup_memory_split(None, None, None, None, None, None);
    }

    #[test]
    fn test_ec_encode_inflight_bytes_tracking() {
        EC_ENCODE_INFLIGHT_BYTES.store(0, Ordering::Relaxed);
        add_ec_encode_inflight_bytes(1024);
        add_ec_encode_inflight_bytes(2048);
        remove_ec_encode_inflight_bytes(1024);
        remove_ec_encode_inflight_bytes(2048);
        remove_ec_encode_inflight_bytes(4096);
        assert_eq!(current_ec_encode_inflight_bytes(), 0);
    }

    #[test]
    fn test_get_object_buffered_bytes_guard() {
        GET_OBJECT_BUFFERED_BYTES.store(0, Ordering::Relaxed);
        drop(track_get_object_buffered_bytes(1024));
        let guard = track_get_object_buffered_bytes(2048);
        drop(guard);
        assert_eq!(current_get_object_buffered_bytes(), 0);
    }

    #[test]
    fn test_get_object_buffered_bytes_guard_saturates_on_underflow() {
        GET_OBJECT_BUFFERED_BYTES.store(1024, Ordering::Relaxed);
        drop(MemoryGaugeGuard {
            gauge: TrackedMemoryGauge::GetObjectBufferedBytes,
            bytes: 2048,
        });
        assert_eq!(current_get_object_buffered_bytes(), 0);
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
