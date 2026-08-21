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

use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

/// Global switch for detailed per-stage PUT metrics (path label, stage durations).
/// When `false`, `record_put_object_path` and `record_put_object_stage_duration`
/// become no-ops, and callers can skip the `Instant::now()` syscalls entirely.
///
/// Enabled only through an explicit runtime opt-in.
static PUT_STAGE_METRICS_ENABLED: AtomicBool = AtomicBool::new(false);
static GET_STAGE_METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Global switch for all remaining (non GET/PUT-stage) metric emission in this
/// crate's free `record_*` functions — I/O scheduler, bytes-pool, zero-copy,
/// bandwidth, system-resource, and error/timeout/retry counters.
///
/// When `false`, those recorders become no-ops so callers skip the label
/// allocations and arithmetic they would otherwise perform before the metric
/// macro. Set to `true` during startup when OTEL metric export is enabled,
/// alongside the GET/PUT stage switches.
///
/// This switch intentionally does NOT gate functions that maintain functional
/// internal state read back by the system (EC encode in-flight accounting,
/// GET whole-object buffered-bytes tracking); those must always run.
static METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable or disable detailed per-stage PUT metrics.
///
/// Called once during startup after applying the detailed PUT attribution opt-in.
pub fn set_put_stage_metrics_enabled(enabled: bool) {
    PUT_STAGE_METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn set_get_stage_metrics_enabled(enabled: bool) {
    GET_STAGE_METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Enable or disable general (non GET/PUT-stage) metric emission.
///
/// Called once during startup, typically gated by `rustfs_obs::observability_metric_enabled()`.
pub fn set_metrics_enabled(enabled: bool) {
    METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Returns `true` if detailed per-stage PUT metrics are enabled.
///
/// Callers should check this before calling `Instant::now()` for stage timing
/// to avoid unnecessary syscalls when metrics are disabled.
#[inline(always)]
pub fn put_stage_metrics_enabled() -> bool {
    PUT_STAGE_METRICS_ENABLED.load(Ordering::Relaxed)
}

/// Start a PUT-stage timer only when detailed PUT attribution is enabled.
#[inline(always)]
pub fn put_stage_timer() -> Option<std::time::Instant> {
    put_stage_metrics_enabled().then(std::time::Instant::now)
}

pub const PUT_STAGE_PUT_OBJECT_COMMIT_NAMESPACE_LOCK_WAIT: &str = "put_object_commit_namespace_lock_wait";
pub const PUT_STAGE_SET_DISK_RENAME_QUORUM_WAIT: &str = "set_disk_rename_quorum_wait";
pub const PUT_STAGE_SET_DISK_RENAME_DISK_WAIT: &str = "set_disk_rename_disk_wait";
pub const PUT_STAGE_SET_DISK_RENAME_FILE_SYNC_PERMIT_WAIT: &str = "set_disk_rename_file_sync_permit_wait";
pub const PUT_STAGE_SET_DISK_RENAME_GLOBAL_FILE_SYNC_PERMIT_WAIT: &str = "set_disk_rename_global_file_sync_permit_wait";
pub const PUT_STAGE_SET_DISK_RENAME_FILE_FDATASYNC: &str = "set_disk_rename_file_fdatasync";
pub const PUT_STAGE_SET_DISK_RENAME_SRC_DIR_FSYNC: &str = "set_disk_rename_src_dir_fsync";
pub const PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC: &str = "set_disk_rename_dst_dir_fsync";
pub const PUT_STAGE_SET_DISK_RENAME_BACKUP_DIR_FSYNC: &str = "set_disk_rename_backup_dir_fsync";
pub const PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC: &str = "set_disk_rename_ancestor_dir_fsync";
pub const PUT_STAGE_SET_DISK_RENAME_RENAME_SYSCALL: &str = "set_disk_rename_rename_syscall";

pub const PUT_COMMIT_LOCK_ADMISSION_BUDGET_DISABLED: &str = "disabled";
pub const PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS: &str = "le_250ms";
pub const PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_500MS: &str = "le_500ms";
pub const PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_1000MS: &str = "le_1000ms";
pub const PUT_COMMIT_LOCK_ADMISSION_BUDGET_GT_1000MS: &str = "gt_1000ms";

pub const PUT_COMMIT_LOCK_ADMISSION_OUTCOME_ACQUIRED: &str = "acquired";
pub const PUT_COMMIT_LOCK_ADMISSION_OUTCOME_TIMEOUT_SLOWDOWN: &str = "timeout_slowdown";
pub const PUT_COMMIT_LOCK_ADMISSION_OUTCOME_LOCK_ERROR: &str = "lock_error";

pub const PUT_RENAME_FDATASYNC_BATCH_MODE_SERIAL: &str = "serial";
pub const PUT_RENAME_FDATASYNC_BATCH_MODE_PARALLEL: &str = "parallel";
pub const PUT_RENAME_FDATASYNC_GROUP_WAIT_ROLE_LEADER: &str = "leader";
pub const PUT_RENAME_FDATASYNC_GROUP_WAIT_ROLE_FOLLOWER: &str = "follower";
pub const PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_ENQUEUE_WAITERS: &str = "enqueue_waiters";
pub const PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_ENQUEUE_FILES: &str = "enqueue_files";
pub const PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_BATCH_WAITERS: &str = "batch_waiters";
pub const PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_BATCH_FILES: &str = "batch_files";
pub const PUT_RENAME_QUORUM_FANOUT_STATE_SCHEDULED: &str = "scheduled";
pub const PUT_RENAME_QUORUM_FANOUT_STATE_WRITE_QUORUM: &str = "write_quorum";
pub const PUT_RENAME_QUORUM_FANOUT_STATE_SUCCESS: &str = "success";
pub const PUT_RENAME_QUORUM_FANOUT_STATE_ERROR: &str = "error";
pub const PUT_RENAME_QUORUM_FANOUT_STATE_PANIC: &str = "panic";
pub const PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_FIRST: &str = "quorum_first";
pub const PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_TAIL: &str = "quorum_tail";
pub const PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_ERROR: &str = "error";

#[inline(always)]
pub fn get_stage_metrics_enabled() -> bool {
    GET_STAGE_METRICS_ENABLED.load(Ordering::Relaxed)
}

/// Returns `true` if general (non GET/PUT-stage) metric emission is enabled.
#[inline(always)]
pub fn metrics_enabled() -> bool {
    METRICS_ENABLED.load(Ordering::Relaxed)
}

// Handle-cached emission for hot, LABEL-LESS metrics. The `metrics` macros
// re-run the recorder's `register_*` (a `RwLock` read + name-key hash + `Arc`
// clone) on every call; for a per-IO label-less metric that lookup is pure
// overhead. In production the handle is resolved once via `LazyLock` and reused.
//
// Under `cfg(test)` the handle is re-resolved on every call instead, because the
// `metrics` crate resolves against a thread-local recorder that
// `with_local_recorder` swaps per test — a process-global cached handle would
// bind to whichever recorder happened to be active first and break test capture.
// These macros must therefore only wrap metrics with a FIXED (label-less) key.
macro_rules! counter_increment_cached {
    ($name:literal, $value:expr) => {{
        #[cfg(not(test))]
        {
            static HANDLE: std::sync::LazyLock<metrics::Counter> = std::sync::LazyLock::new(|| metrics::counter!($name));
            HANDLE.increment($value);
        }
        #[cfg(test)]
        {
            metrics::counter!($name).increment($value);
        }
    }};
}

macro_rules! gauge_set_cached {
    ($name:literal, $value:expr) => {{
        #[cfg(not(test))]
        {
            static HANDLE: std::sync::LazyLock<metrics::Gauge> = std::sync::LazyLock::new(|| metrics::gauge!($name));
            HANDLE.set($value);
        }
        #[cfg(test)]
        {
            metrics::gauge!($name).set($value);
        }
    }};
}

macro_rules! histogram_record_cached {
    ($name:literal, $value:expr) => {{
        #[cfg(not(test))]
        {
            static HANDLE: std::sync::LazyLock<metrics::Histogram> = std::sync::LazyLock::new(|| metrics::histogram!($name));
            HANDLE.record($value);
        }
        #[cfg(test)]
        {
            metrics::histogram!($name).record($value);
        }
    }};
}

// Public modules
pub mod adaptive_ttl;
pub mod autotuner;
pub mod backpressure_metrics;
pub mod cache_config;
pub mod capacity_metrics;
pub mod collector;
pub mod deadlock_metrics;
pub mod internode_metrics;
pub mod io_metrics;
pub mod list_objects_metrics;
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
    record_capacity_timeout_fallback, record_capacity_update_completed, record_capacity_update_failed,
    record_capacity_write_operation, record_old_data_dir_cleanup,
};

// I/O metrics exports
pub use io_metrics::{
    IoSchedulerStats, record_bandwidth_observation, record_buffer_size_adjustment, record_io_priority_decision,
    record_io_scheduler_decision, record_load_level_change, record_queue_operation, record_starvation_event,
};
pub use list_objects_metrics::{
    LIST_OBJECTS_GATHER_OUTCOME_INPUT_CLOSED, LIST_OBJECTS_GATHER_OUTCOME_LIMIT_REACHED,
    LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_ERROR, LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_OK, LIST_OBJECTS_SOURCE_WALKER,
    ListObjectsGatherObservation, ListObjectsIndexPageObservation, ListObjectsLocalReadDirObservation, init_list_objects_metrics,
    record_list_objects_gather, record_list_objects_index_attempt, record_list_objects_index_fallback,
    record_list_objects_index_live_verify_failure, record_list_objects_index_served, record_list_objects_local_read_dir,
    record_list_objects_merge,
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
    ProcessLockEventSnapshot, ProcessLockSnapshot, ProcessPlatformSnapshot, record_lock_reclaimed,
    record_lock_refresh_quorum_lost, record_read_lock_held_acquire, record_read_lock_held_release,
    record_write_lock_held_acquire, record_write_lock_held_release, snapshot_process_lock_counts, snapshot_process_lock_events,
    snapshot_process_platform_stats,
};
pub use s3_api_metrics::{S3OperationMetricSnapshot, init_s3_metrics, record_s3_op, s3_op_metrics_snapshot};
pub use sampler::{
    ProcessResourceSnapshot, ProcessSampler, ProcessStatusSnapshot, ProcessSystemSnapshot, snapshot_process_platform,
    snapshot_process_resource, snapshot_process_resource_and_system, snapshot_process_resource_and_system_with,
    snapshot_process_system,
};
pub use system_path_metrics::record_system_path_failure;

// Timeout metrics exports
pub use timeout_metrics::{
    TimeoutMetricsSummary, record_dynamic_timeout, record_operation_completion, record_operation_duration,
    record_operation_progress, record_stalled_operation, record_timeout_event,
};

// Re-exports for convenience
pub use collector::MetricsCollector;
pub use performance::PerformanceMetrics;

static EC_ENCODE_INFLIGHT_BYTES: AtomicU64 = AtomicU64::new(0);
static EC_ENCODE_PRODUCER_BYTES_CURRENT: AtomicU64 = AtomicU64::new(0);
static EC_ENCODE_PRODUCER_BYTES_PEAK: AtomicU64 = AtomicU64::new(0);
static EC_ENCODE_QUEUE_BYTES_PEAK: AtomicU64 = AtomicU64::new(0);
static EC_ENCODE_WRITER_BYTES_CURRENT: AtomicU64 = AtomicU64::new(0);
static EC_ENCODE_WRITER_BYTES_PEAK: AtomicU64 = AtomicU64::new(0);
static EC_ENCODE_PEAK_PUBLISH_LOCK: Mutex<()> = Mutex::new(());
static GET_OBJECT_BUFFERED_BYTES: AtomicU64 = AtomicU64::new(0);
const SHARD_READ_COST_LOCAL: &str = "local";
const SHARD_READ_COST_REMOTE: &str = "remote";
const SHARD_READ_COST_SAME_NODE: &str = "same_node";
const SHARD_READ_COST_UNKNOWN: &str = "unknown";
const LOW_COST_QUORUM_CANDIDATE_FALSE: &str = "false";
const LOW_COST_QUORUM_CANDIDATE_TRUE: &str = "true";
pub const GET_OBJECT_SIZE_BUCKET_LE_4_KIB: &str = "le_4kib";
pub const GET_OBJECT_SIZE_BUCKET_LE_16_KIB: &str = "le_16kib";
pub const GET_OBJECT_SIZE_BUCKET_LE_64_KIB: &str = "le_64kib";
pub const GET_OBJECT_SIZE_BUCKET_LE_128_KIB: &str = "le_128kib";
pub const GET_OBJECT_SIZE_BUCKET_LE_192_KIB: &str = "le_192kib";
pub const GET_OBJECT_SIZE_BUCKET_LE_256_KIB: &str = "le_256kib";
pub const GET_OBJECT_SIZE_BUCKET_LE_512_KIB: &str = "le_512kib";
pub const GET_OBJECT_SIZE_BUCKET_LE_1_MIB: &str = "le_1mib";
pub const GET_OBJECT_SIZE_BUCKET_GT_1_MIB: &str = "gt_1mib";
pub const GET_OBJECT_SIZE_BUCKET_UNKNOWN: &str = "unknown";

pub struct GetObjectStreamingBodyFailure {
    pub stage: &'static str,
    pub reason: &'static str,
    pub error_class: &'static str,
    pub strategy: &'static str,
    pub buffer_source: &'static str,
    pub size_bucket: &'static str,
    pub emitted_bytes: usize,
    pub remaining_bytes: usize,
}

/// Return the bounded size bucket used by small-object GET diagnostics.
#[inline(always)]
pub const fn get_object_size_bucket(size_bytes: i64) -> &'static str {
    match size_bytes {
        ..=4_096 => GET_OBJECT_SIZE_BUCKET_LE_4_KIB,
        4_097..=16_384 => GET_OBJECT_SIZE_BUCKET_LE_16_KIB,
        16_385..=65_536 => GET_OBJECT_SIZE_BUCKET_LE_64_KIB,
        65_537..=131_072 => GET_OBJECT_SIZE_BUCKET_LE_128_KIB,
        131_073..=196_608 => GET_OBJECT_SIZE_BUCKET_LE_192_KIB,
        196_609..=262_144 => GET_OBJECT_SIZE_BUCKET_LE_256_KIB,
        262_145..=524_288 => GET_OBJECT_SIZE_BUCKET_LE_512_KIB,
        524_289..=1_048_576 => GET_OBJECT_SIZE_BUCKET_LE_1_MIB,
        _ => GET_OBJECT_SIZE_BUCKET_GT_1_MIB,
    }
}

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
fn update_peak_atomic(counter: &AtomicU64, value: u64) -> Option<u64> {
    let mut peak = counter.load(Ordering::Relaxed);
    loop {
        if value <= peak {
            return None;
        }
        match counter.compare_exchange_weak(peak, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Some(value),
            Err(actual) => peak = actual,
        }
    }
}

#[derive(Clone, Copy)]
enum EcEncodePeakMetric {
    Producer,
    Queue,
    Writer,
}

fn publish_ec_encode_peak_with(counter: &AtomicU64, value: u64, before_publish_lock: impl FnOnce(), set_gauge: impl FnOnce(f64)) {
    if update_peak_atomic(counter, value).is_none() {
        return;
    }
    before_publish_lock();
    let _guard = EC_ENCODE_PEAK_PUBLISH_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    set_gauge(counter.load(Ordering::Relaxed) as f64);
}

fn publish_ec_encode_peak(counter: &AtomicU64, metric: EcEncodePeakMetric, value: u64) {
    publish_ec_encode_peak_with(
        counter,
        value,
        || {},
        |peak| match metric {
            EcEncodePeakMetric::Producer => gauge_set_cached!("rustfs_ec_encode_producer_bytes_peak", peak),
            EcEncodePeakMetric::Queue => gauge_set_cached!("rustfs_ec_encode_queue_bytes_peak", peak),
            EcEncodePeakMetric::Writer => gauge_set_cached!("rustfs_ec_encode_writer_bytes_peak", peak),
        },
    );
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
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_requests_total").increment(1);
    gauge!("rustfs_io_get_object_concurrent_requests").set(concurrent_requests as f64);
}

/// Record GetObject request start without concurrency context.
#[inline(always)]
pub fn record_get_object_request_started() {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_requests_total").increment(1);
}

/// Record GetObject request result.
#[inline(always)]
pub fn record_get_object_request_result(status: &str, duration_secs: f64) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_request_results_total", "status" => status.to_string()).increment(1);
    histogram!("rustfs_io_get_object_request_duration_seconds", "status" => status.to_string()).record(duration_secs);
}

/// Record PutObject request start.
#[inline(always)]
pub fn record_put_object_request_start(concurrent_requests: usize) {
    if !metrics_enabled() {
        return;
    }
    counter!("rustfs_io_put_object_requests_total").increment(1);
    gauge!("rustfs_io_put_object_concurrent_requests").set(concurrent_requests as f64);
}

/// Record PutObject request result.
#[inline(always)]
pub fn record_put_object_request_result(status: &str, duration_secs: f64) {
    if !metrics_enabled() {
        return;
    }
    counter!("rustfs_io_put_object_request_results_total", "status" => status.to_string()).increment(1);
    histogram!("rustfs_io_put_object_request_duration_seconds", "status" => status.to_string()).record(duration_secs);
}

/// Record GetObject timeout for a specific stage.
#[inline(always)]
pub fn record_get_object_timeout(stage: Option<&str>, elapsed_secs: Option<f64>) {
    if !get_stage_metrics_enabled() {
        return;
    }
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
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_completed_total").increment(1);
    histogram!("rustfs_io_get_object_total_duration_seconds").record(total_duration_secs);
    histogram!("rustfs_io_get_object_response_size_bytes").record(response_size_bytes as f64);
    histogram!("rustfs_io_get_object_buffer_size_bytes").record(buffer_size_bytes as f64);
}

/// Record the streaming strategy chosen for a GetObject response body.
#[inline(always)]
pub fn record_get_object_stream_strategy(strategy: &'static str, buffer_size_bytes: usize, response_size_bytes: i64) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_stream_strategy_total", "strategy" => strategy).increment(1);
    histogram!("rustfs_io_get_object_stream_buffer_size_bytes", "strategy" => strategy).record(usize_to_f64(buffer_size_bytes));
    histogram!("rustfs_io_get_object_stream_response_size_bytes", "strategy" => strategy)
        .record(i64_non_negative_to_f64(response_size_bytes));
}

/// Record the response-body handoff shape from a GetObject reader into the S3 streaming body.
#[inline(always)]
pub fn record_get_object_response_handoff(
    strategy: &'static str,
    buffer_source: &'static str,
    buffer_size_bytes: usize,
    response_size_bytes: i64,
    duration_secs: f64,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_response_handoff_total",
        "strategy" => strategy,
        "buffer_source" => buffer_source
    )
    .increment(1);
    histogram!(
        "rustfs_io_get_object_response_handoff_buffer_size_bytes",
        "strategy" => strategy,
        "buffer_source" => buffer_source
    )
    .record(usize_to_f64(buffer_size_bytes));
    histogram!(
        "rustfs_io_get_object_response_handoff_response_size_bytes",
        "strategy" => strategy,
        "buffer_source" => buffer_source
    )
    .record(i64_non_negative_to_f64(response_size_bytes));
    histogram!(
        "rustfs_io_get_object_response_handoff_duration_seconds",
        "strategy" => strategy,
        "buffer_source" => buffer_source
    )
    .record(duration_secs);
    record_get_object_response_handoff_duration("s3_handler", duration_secs);
}

/// Record ReaderStream capacity chosen for GetObject handoff.
#[inline(always)]
pub fn record_get_object_reader_stream_buffer_size(
    strategy: &'static str,
    buffer_source: &'static str,
    buffer_size_bytes: usize,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!(
        "rustfs_io_get_object_reader_stream_buffer_size_bytes",
        "strategy" => strategy,
        "buffer_source" => buffer_source
    )
    .record(usize_to_f64(buffer_size_bytes));
}

/// Record ReaderStream poll outcomes for GetObject handoff attribution.
#[inline(always)]
pub fn record_get_object_reader_stream_poll(
    strategy: &'static str,
    buffer_source: &'static str,
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
        "strategy" => strategy,
        "buffer_source" => buffer_source,
        "outcome" => outcome
    )
    .increment(1);
    counter!(
        "rustfs_io_get_object_reader_stream_poll_bytes_total",
        "strategy" => strategy,
        "buffer_source" => buffer_source,
        "outcome" => outcome
    )
    .increment(bytes);
    histogram!(
        "rustfs_io_get_object_reader_stream_poll_remaining_bytes",
        "strategy" => strategy,
        "buffer_source" => buffer_source,
        "outcome" => outcome
    )
    .record(usize_to_f64(remaining_before));
    histogram!(
        "rustfs_io_get_object_reader_stream_poll_bytes",
        "strategy" => strategy,
        "buffer_source" => buffer_source,
        "outcome" => outcome
    )
    .record(usize_to_f64(bytes as usize));
    histogram!(
        "rustfs_io_get_object_reader_stream_poll_duration_seconds",
        "strategy" => strategy,
        "buffer_source" => buffer_source,
        "outcome" => outcome
    )
    .record(duration_secs);
}

/// Record a GET response body failure with bounded attribution labels.
#[inline(always)]
pub fn record_get_object_streaming_body_failure(failure: GetObjectStreamingBodyFailure) {
    if !metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_streaming_body_failure_total",
        "stage" => failure.stage,
        "reason" => failure.reason,
        "error_class" => failure.error_class,
        "strategy" => failure.strategy,
        "buffer_source" => failure.buffer_source,
        "size_bucket" => failure.size_bucket
    )
    .increment(1);
    histogram!(
        "rustfs_io_get_object_streaming_body_failure_emitted_bytes",
        "stage" => failure.stage,
        "reason" => failure.reason,
        "error_class" => failure.error_class,
        "strategy" => failure.strategy,
        "buffer_source" => failure.buffer_source,
        "size_bucket" => failure.size_bucket
    )
    .record(usize_to_f64(failure.emitted_bytes));
    histogram!(
        "rustfs_io_get_object_streaming_body_failure_remaining_bytes",
        "stage" => failure.stage,
        "reason" => failure.reason,
        "error_class" => failure.error_class,
        "strategy" => failure.strategy,
        "buffer_source" => failure.buffer_source,
        "size_bucket" => failure.size_bucket
    )
    .record(usize_to_f64(failure.remaining_bytes));
}

/// Record a poll of the single-chunk in-memory GetObject handoff stream.
#[inline(always)]
pub fn record_get_object_memory_body_stream_poll(source: &'static str, outcome: &'static str, bytes: usize, duration_secs: f64) {
    if !get_stage_metrics_enabled() {
        return;
    }
    let bytes_counter = u64::try_from(bytes).unwrap_or(u64::MAX);
    counter!(
        "rustfs_io_get_object_memory_body_stream_poll_total",
        "source" => source,
        "outcome" => outcome
    )
    .increment(1);
    counter!(
        "rustfs_io_get_object_memory_body_stream_poll_bytes_total",
        "source" => source,
        "outcome" => outcome
    )
    .increment(bytes_counter);
    histogram!(
        "rustfs_io_get_object_memory_body_stream_poll_bytes",
        "source" => source,
        "outcome" => outcome
    )
    .record(usize_to_f64(bytes));
    histogram!(
        "rustfs_io_get_object_memory_body_stream_poll_duration_seconds",
        "source" => source,
        "outcome" => outcome
    )
    .record(duration_secs);
}

/// Record I/O queue congestion observation.
#[inline(always)]
pub fn record_io_queue_congestion() {
    if !metrics_enabled() {
        return;
    }
    counter_increment_cached!("rustfs_io_queue_congestion_total", 1);
}

/// Record I/O priority assignment.
#[inline(always)]
pub fn record_io_priority_assignment(priority: &str) {
    if !metrics_enabled() {
        return;
    }
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

/// Record GetObject stage duration with bounded object class and size labels.
#[inline(always)]
pub fn record_get_object_stage_duration_by_size(
    path: &'static str,
    stage: &'static str,
    object_class: &'static str,
    size_bucket: &'static str,
    duration_secs: f64,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!(
        "rustfs_io_get_object_stage_duration_seconds_by_size",
        "path" => path,
        "stage" => stage,
        "object_class" => object_class,
        "size_bucket" => size_bucket
    )
    .record(duration_secs);
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

/// Record one bounded metadata cache decision.
#[inline(always)]
pub fn record_get_object_metadata_cache_decision(path: &'static str, decision: &'static str, reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_metadata_cache_total", "path" => path, "decision" => decision, "reason" => reason)
        .increment(1);
}

/// Record aggregate metadata fanout shape for one GetObject metadata read.
///
/// The legacy `metadata_fanout_error_responses` series records every non-valid
/// response, including not-found and ignored outcomes. Use
/// `metadata_response_total` outcome labels for failure attribution.
#[inline(always)]
pub fn record_get_object_metadata_fanout_shape(path: &'static str, total: usize, valid: usize, ignored: usize, non_valid: usize) {
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
        .record(metadata_fanout_count_to_f64(non_valid));
}

/// Record task lifecycle shape for one GetObject metadata fanout.
#[inline(always)]
pub fn record_get_object_metadata_fanout_lifecycle(path: &'static str, scheduled: usize, completed: usize, cancelled: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_get_object_metadata_fanout_scheduled", "path" => path).record(metadata_fanout_count_to_f64(scheduled));
    histogram!("rustfs_io_get_object_metadata_fanout_completed", "path" => path).record(metadata_fanout_count_to_f64(completed));
    histogram!("rustfs_io_get_object_metadata_fanout_cancelled", "path" => path).record(metadata_fanout_count_to_f64(cancelled));
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

/// Record the reconstruction outcome for a GetObject reader path.
#[inline(always)]
pub fn record_get_object_reconstruct_outcome(path: &'static str, engine: &'static str, outcome: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_reconstruct_outcome_total",
        "path" => path,
        "engine" => engine,
        "outcome" => outcome
    )
    .increment(1);
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

/// Record the selected GetObject reader path with bounded object class and size labels.
#[inline(always)]
pub fn record_get_object_reader_path_by_size(path: &'static str, object_class: &'static str, size_bucket: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_reader_path_by_size_total",
        "path" => path,
        "object_class" => object_class,
        "size_bucket" => size_bucket
    )
    .increment(1);
}

/// Record the concrete subpath used by the direct-memory GetObject reader.
#[inline(always)]
pub fn record_get_object_direct_memory_subpath(subpath: &'static str, object_class: &'static str, size_bucket: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_direct_memory_subpath_total", "subpath" => subpath).increment(1);
    counter!(
        "rustfs_io_get_object_direct_memory_subpath_by_size_total",
        "subpath" => subpath,
        "object_class" => object_class,
        "size_bucket" => size_bucket
    )
    .increment(1);
}

/// Record the direct-memory GetObject path decision and bounded fallback reason.
#[inline(always)]
pub fn record_get_object_direct_memory_decision(
    outcome: &'static str,
    object_class: &'static str,
    reason: &'static str,
    size_bucket: &'static str,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_direct_memory_decision_total",
        "outcome" => outcome,
        "object_class" => object_class,
        "reason" => reason
    )
    .increment(1);
    counter!(
        "rustfs_io_get_object_direct_memory_decision_by_size_total",
        "outcome" => outcome,
        "object_class" => object_class,
        "reason" => reason,
        "size_bucket" => size_bucket
    )
    .increment(1);
}

/// Record why the codec streaming reader was not selected.
#[inline(always)]
pub fn record_get_object_codec_streaming_fallback(reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_codec_streaming_fallback_total", "reason" => reason).increment(1);
}

/// Record the read path chosen for one encrypted Range GET on the Legacy (rio v1) backend
/// together with its read amplification — physical ciphertext bytes scheduled for the
/// erasure layer divided by the plaintext bytes the client requested. Observed at the
/// ReadPlan decision point (https://github.com/rustfs/backlog/issues/1316 Phase A).
#[inline(always)]
pub fn record_get_encrypted_range_read_amplification(path: &'static str, amplification: f64) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_encrypted_range_read_path_total", "path" => path).increment(1);
    histogram!("rustfs_io_get_encrypted_range_read_amplification", "path" => path).record(amplification);
}

/// Record the final codec-streaming rollout decision for a GET request.
#[inline(always)]
pub fn record_get_object_codec_streaming_decision(outcome: &'static str, object_class: &'static str, reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_codec_streaming_decision_total",
        "outcome" => outcome,
        "object_class" => object_class,
        "reason" => reason
    )
    .increment(1);
}

/// Record the final codec-streaming rollout decision with bounded size attribution.
#[inline(always)]
pub fn record_get_object_codec_streaming_decision_by_size(
    outcome: &'static str,
    object_class: &'static str,
    reason: &'static str,
    size_bucket: &'static str,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_codec_streaming_decision_by_size_total",
        "outcome" => outcome,
        "object_class" => object_class,
        "reason" => reason,
        "size_bucket" => size_bucket
    )
    .increment(1);
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

/// Record that a background fill task was started for a GetObject reader path.
#[inline(always)]
pub fn record_get_object_fill_started(path: &'static str, policy: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_fill_started_total", "path" => path, "policy" => policy).increment(1);
}

/// Record that a persistent fill worker was started for a GetObject reader path.
#[inline(always)]
pub fn record_get_object_fill_worker_started(path: &'static str, policy: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_io_get_object_fill_worker_started_total", "path" => path, "policy" => policy).increment(1);
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

/// Record observe-only shard-locality potential while scheduling remains disabled.
#[inline(always)]
pub fn record_get_object_shard_locality_observe_only(path: &'static str, remote_scheduled: usize, remote_avoid_potential: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_io_get_object_shard_remote_scheduled_observe_only", "path" => path)
        .record(shard_read_fanout_to_f64(remote_scheduled));
    histogram!("rustfs_io_get_object_shard_remote_avoid_potential", "path" => path)
        .record(shard_read_fanout_to_f64(remote_avoid_potential));
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

fn batch_processor_count_to_f64(value: usize) -> f64 {
    u64::try_from(value).unwrap_or(u64::MAX) as f64
}

fn batch_processor_count_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Observe-only batch processor shape and adaptive-concurrency advice.
#[derive(Clone, Copy, Debug)]
pub struct BatchProcessorObservation {
    pub operation: &'static str,
    pub batch_size: usize,
    pub configured_concurrency: usize,
    pub max_queue_wait_secs: f64,
    pub execution_latency_secs: f64,
    pub successes: usize,
    pub errors: usize,
    pub timeouts: usize,
    pub suggested_concurrency: usize,
    pub suggestion_reason: &'static str,
}

/// Record observe-only batch processor shape and adaptive-concurrency advice.
#[inline(always)]
pub fn record_batch_processor_observation(observation: BatchProcessorObservation) {
    if !get_stage_metrics_enabled() {
        return;
    }

    histogram!("rustfs_ecstore_batch_processor_batch_size", "operation" => observation.operation)
        .record(batch_processor_count_to_f64(observation.batch_size));
    histogram!("rustfs_ecstore_batch_processor_configured_concurrency", "operation" => observation.operation)
        .record(batch_processor_count_to_f64(observation.configured_concurrency));
    histogram!("rustfs_ecstore_batch_processor_queue_wait_seconds", "operation" => observation.operation)
        .record(observation.max_queue_wait_secs);
    histogram!("rustfs_ecstore_batch_processor_execution_latency_seconds", "operation" => observation.operation)
        .record(observation.execution_latency_secs);
    counter!(
        "rustfs_ecstore_batch_processor_results_total",
        "operation" => observation.operation,
        "outcome" => "success"
    )
    .increment(batch_processor_count_to_u64(observation.successes));
    counter!(
        "rustfs_ecstore_batch_processor_results_total",
        "operation" => observation.operation,
        "outcome" => "error"
    )
    .increment(batch_processor_count_to_u64(observation.errors));
    counter!(
        "rustfs_ecstore_batch_processor_results_total",
        "operation" => observation.operation,
        "outcome" => "timeout"
    )
    .increment(batch_processor_count_to_u64(observation.timeouts));
    histogram!(
        "rustfs_ecstore_batch_processor_suggested_concurrency",
        "operation" => observation.operation,
        "reason" => observation.suggestion_reason
    )
    .record(batch_processor_count_to_f64(observation.suggested_concurrency));
}

/// Record the bitrot reader setup scheduling strategy selected for a GET read.
#[inline(always)]
pub fn record_get_object_reader_setup_strategy(strategy: &'static str, mode: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_reader_setup_strategy_total",
        "strategy" => strategy,
        "mode" => mode
    )
    .increment(1);
}

/// Record the bitrot reader setup scheduling strategy with bounded GET attribution labels.
#[inline(always)]
pub fn record_get_object_reader_setup_strategy_by_size(
    path: &'static str,
    strategy: &'static str,
    mode: &'static str,
    object_class: &'static str,
    size_bucket: &'static str,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    counter!(
        "rustfs_io_get_object_reader_setup_strategy_by_size_total",
        "path" => path,
        "strategy" => strategy,
        "mode" => mode,
        "object_class" => object_class,
        "size_bucket" => size_bucket
    )
    .increment(1);
}

/// Record the final bitrot reader setup fanout shape for a GET read.
#[inline(always)]
pub fn record_get_object_reader_setup_fanout(
    strategy: &'static str,
    mode: &'static str,
    scheduled: usize,
    attempted: usize,
    ready: usize,
    failed: usize,
    deferred: usize,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!(
        "rustfs_io_get_object_reader_setup_scheduled",
        "strategy" => strategy,
        "mode" => mode
    )
    .record(shard_read_fanout_to_f64(scheduled));
    histogram!(
        "rustfs_io_get_object_reader_setup_attempted",
        "strategy" => strategy,
        "mode" => mode
    )
    .record(shard_read_fanout_to_f64(attempted));
    histogram!(
        "rustfs_io_get_object_reader_setup_ready",
        "strategy" => strategy,
        "mode" => mode
    )
    .record(shard_read_fanout_to_f64(ready));
    histogram!(
        "rustfs_io_get_object_reader_setup_failed",
        "strategy" => strategy,
        "mode" => mode
    )
    .record(shard_read_fanout_to_f64(failed));
    histogram!(
        "rustfs_io_get_object_reader_setup_deferred",
        "strategy" => strategy,
        "mode" => mode
    )
    .record(shard_read_fanout_to_f64(deferred));
}

/// Record the final bitrot reader setup fanout shape with bounded GET attribution labels.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub fn record_get_object_reader_setup_fanout_by_size(
    path: &'static str,
    strategy: &'static str,
    mode: &'static str,
    object_class: &'static str,
    size_bucket: &'static str,
    scheduled: usize,
    attempted: usize,
    ready: usize,
    failed: usize,
    deferred: usize,
) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!(
        "rustfs_io_get_object_reader_setup_scheduled_by_size",
        "path" => path,
        "strategy" => strategy,
        "mode" => mode,
        "object_class" => object_class,
        "size_bucket" => size_bucket
    )
    .record(shard_read_fanout_to_f64(scheduled));
    histogram!(
        "rustfs_io_get_object_reader_setup_attempted_by_size",
        "path" => path,
        "strategy" => strategy,
        "mode" => mode,
        "object_class" => object_class,
        "size_bucket" => size_bucket
    )
    .record(shard_read_fanout_to_f64(attempted));
    histogram!(
        "rustfs_io_get_object_reader_setup_ready_by_size",
        "path" => path,
        "strategy" => strategy,
        "mode" => mode,
        "object_class" => object_class,
        "size_bucket" => size_bucket
    )
    .record(shard_read_fanout_to_f64(ready));
    histogram!(
        "rustfs_io_get_object_reader_setup_failed_by_size",
        "path" => path,
        "strategy" => strategy,
        "mode" => mode,
        "object_class" => object_class,
        "size_bucket" => size_bucket
    )
    .record(shard_read_fanout_to_f64(failed));
    histogram!(
        "rustfs_io_get_object_reader_setup_deferred_by_size",
        "path" => path,
        "strategy" => strategy,
        "mode" => mode,
        "object_class" => object_class,
        "size_bucket" => size_bucket
    )
    .record(shard_read_fanout_to_f64(deferred));
}

/// Record GetObject metadata resolution duration.
#[inline(always)]
pub fn record_get_object_metadata_phase_duration(duration_secs: f64) {
    record_get_object_stage_duration("legacy_duplex", "metadata", duration_secs);
}

/// Record metadata phase duration with early-stop state label.
#[inline(always)]
pub fn record_get_object_metadata_phase_duration_with_early_stop(duration_secs: f64, early_stop_active: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!(
        "rustfs_io_get_object_stage_duration_seconds",
        "path" => "legacy_duplex",
        "stage" => "metadata",
        "early_stop_active" => early_stop_active
    )
    .record(duration_secs);
}

/// Record GET object total duration with reader path label.
#[inline(always)]
pub fn record_get_object_total_duration_with_path(duration_secs: f64, reader_path: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }
    histogram!(
        "rustfs_io_get_object_total_duration_seconds_with_path",
        "reader_path" => reader_path
    )
    .record(duration_secs);
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
    if !metrics_enabled() {
        return;
    }
    counter!("rustfs_zero_copy_reads_total").increment(1);
    histogram!("rustfs_zero_copy_read_size_bytes").record(size_bytes as f64);
    histogram!("rustfs_zero_copy_read_duration_ms").record(duration_ms);

    counter!(mmap_copy::READS_TOTAL).increment(1);
    histogram!(mmap_copy::READ_SIZE_BYTES).record(size_bytes as f64);
    histogram!(mmap_copy::READ_DURATION_MS).record(duration_ms);
    counter!(mmap_copy::BYTES_COPIED_TOTAL).increment(size_bytes as u64);
}

/// Record memory copies avoided by using zero-copy.
///
/// # Arguments
///
/// * `bytes_saved` - Number of bytes that would have been copied without zero-copy
#[inline(always)]
pub fn record_memory_copy_saved(bytes_saved: usize) {
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
    counter!("rustfs_zero_copy_fallback_total", "reason" => reason.to_string()).increment(1);
    counter!(mmap_copy::FALLBACK_TOTAL, "reason" => reason.to_string()).increment(1);
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
    gauge!("rustfs_bytes_pool_hit_rate", "tier" => tier.to_string()).set(hit_rate * 100.0);
}

/// Record a BytesPool buffer acquisition attempt.
///
/// `outcome` = `"hit"` when a buffer is available in the pool, `"miss"` when the
/// pool is empty and a new allocation is required.
///
/// # Arguments
///
/// * `tier` - Pool tier ("small", "medium", "large", "xlarge")
/// * `outcome` - Acquisition outcome ("hit" or "miss")
#[inline(always)]
pub fn record_bytespool_acquisition(tier: &'static str, outcome: &'static str) {
    if !metrics_enabled() {
        return;
    }
    counter!("rustfs_io_bytespool_acquisition_total", "tier" => tier, "outcome" => outcome).increment(1);
}

/// Record a BytesPool buffer return attempt.
///
/// `outcome` = `"recycled"` when the buffer is successfully returned to the
/// pool, `"dropped"` when `try_lock` fails and the buffer is deallocated.
///
/// # Arguments
///
/// * `tier` - Pool tier ("small", "medium", "large", "xlarge")
/// * `outcome` - Return outcome ("recycled" or "dropped")
#[inline(always)]
pub fn record_bytespool_return(tier: &'static str, outcome: &'static str) {
    if !metrics_enabled() {
        return;
    }
    counter!("rustfs_io_bytespool_return_total", "tier" => tier, "outcome" => outcome).increment(1);
}

/// Record zero-copy write operation.
///
/// # Arguments
///
/// * `size_bytes` - Size of the data written in bytes
/// * `duration_ms` - Time taken for the write operation in milliseconds
#[inline(always)]
pub fn record_zero_copy_write(size_bytes: usize, duration_ms: f64) {
    if !metrics_enabled() {
        return;
    }
    counter!("rustfs_zero_copy_write_total").increment(1);
    histogram!("rustfs_zero_copy_write_size_bytes").record(size_bytes as f64);
    histogram!("rustfs_zero_copy_write_duration_ms").record(duration_ms);

    counter!(buffered_write::WRITES_TOTAL).increment(1);
    histogram!(buffered_write::WRITE_SIZE_BYTES).record(size_bytes as f64);
    histogram!(buffered_write::WRITE_DURATION_MS).record(duration_ms);
    counter!(buffered_write::BYTES_COPIED_TOTAL).increment(size_bytes as u64);
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
    if !metrics_enabled() {
        return;
    }
    counter!("rustfs_zero_copy_write_fallback_total", "reason" => reason.to_string()).increment(1);
    counter!(buffered_write::FALLBACK_TOTAL, "reason" => reason.to_string()).increment(1);
}

/// Record bytes saved from zero-copy.
///
/// # Arguments
///
/// * `size_bytes` - Number of bytes saved from zero-copy
#[inline(always)]
pub fn record_bytes_saved(size_bytes: usize) {
    if !metrics_enabled() {
        return;
    }
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
    if !get_stage_metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
fn put_object_size_bucket(size_bytes: i64) -> &'static str {
    const MI_B: i64 = 1024 * 1024;

    match size_bytes {
        i64::MIN..=0 => "unknown",
        1..=MI_B => "le_1mib",
        _ if size_bytes <= 10 * MI_B => "le_10mib",
        _ if size_bytes <= 16 * MI_B => "le_16mib",
        _ if size_bytes <= 32 * MI_B => "le_32mib",
        _ if size_bytes <= 64 * MI_B => "le_64mib",
        _ => "gt_64mib",
    }
}

#[inline(always)]
fn put_object_buffer_bucket(buffer_size: usize) -> &'static str {
    const KI_B: usize = 1024;
    const MI_B: usize = 1024 * 1024;

    match buffer_size {
        0..=65536 => "le_64kib",
        _ if buffer_size <= 128 * KI_B => "le_128kib",
        _ if buffer_size <= 256 * KI_B => "le_256kib",
        _ if buffer_size <= 512 * KI_B => "le_512kib",
        _ if buffer_size <= MI_B => "le_1mib",
        _ => "gt_1mib",
    }
}

#[inline(always)]
fn bool_label(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

#[inline(always)]
pub fn record_put_object_diagnostics(
    path: &'static str,
    eager_status: &'static str,
    size_bytes: i64,
    buffer_size: usize,
    large_concurrency_tuning: bool,
) {
    if !put_stage_metrics_enabled() {
        return;
    }

    let size_bucket = put_object_size_bucket(size_bytes);
    let buffer_bucket = put_object_buffer_bucket(buffer_size);
    counter!(
        "rustfs_s3_put_object_diagnostics_total",
        "path" => path,
        "eager_status" => eager_status,
        "size_bucket" => size_bucket,
        "buffer_bucket" => buffer_bucket,
        "large_concurrency_tuning" => bool_label(large_concurrency_tuning),
    )
    .increment(1);
    histogram!(
        "rustfs_s3_put_object_selected_buffer_size_bytes",
        "path" => path,
        "size_bucket" => size_bucket,
    )
    .record(buffer_size as f64);
}

#[inline(always)]
pub fn record_put_object_stage_duration(stage: &'static str, duration_ms: f64) {
    if !put_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_s3_put_object_stage_duration_ms", "stage" => stage).record(duration_ms);
}

#[inline(always)]
pub fn record_put_object_stage_duration_from(stage: &'static str, started_at: Option<std::time::Instant>) {
    if let Some(started_at) = started_at {
        record_put_object_stage_duration(stage, started_at.elapsed().as_secs_f64() * 1000.0);
    }
}

#[inline(always)]
pub fn record_put_object_commit_lock_admission(budget: &'static str, outcome: &'static str) {
    if !put_stage_metrics_enabled() {
        return;
    }
    counter!("rustfs_s3_put_object_commit_namespace_lock_admission_total", "budget" => budget, "outcome" => outcome).increment(1);
}

#[inline(always)]
fn put_stage_count_value(value: usize) -> f64 {
    match u32::try_from(value) {
        Ok(value) => f64::from(value),
        Err(_) => f64::from(u32::MAX),
    }
}

#[inline(always)]
pub fn record_put_rename_fdatasync_batch(mode: &'static str, files: usize) {
    if !put_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_s3_put_object_rename_fdatasync_batch_files", "mode" => mode).record(put_stage_count_value(files));
}

#[inline(always)]
pub fn record_put_rename_fdatasync_group_wait(role: &'static str, duration_ms: f64) {
    if !put_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_s3_put_object_rename_fdatasync_group_wait_ms", "role" => role).record(duration_ms);
}

#[inline(always)]
pub fn record_put_rename_fdatasync_group_outstanding(state: &'static str, count: usize) {
    if !put_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_s3_put_object_rename_fdatasync_group_outstanding", "state" => state).record(put_stage_count_value(count));
}

#[inline(always)]
pub fn record_put_rename_disk_wait_completion(position: &'static str, duration_ms: f64) {
    if !put_stage_metrics_enabled() {
        return;
    }
    histogram!("rustfs_s3_put_object_rename_disk_wait_completion_ms", "position" => position).record(duration_ms);
}

#[inline(always)]
pub fn record_put_rename_quorum_wait_fanout(
    scheduled: usize,
    write_quorum: usize,
    success: usize,
    error: usize,
    panicked: usize,
) {
    if !put_stage_metrics_enabled() {
        return;
    }
    for (state, count) in [
        (PUT_RENAME_QUORUM_FANOUT_STATE_SCHEDULED, scheduled),
        (PUT_RENAME_QUORUM_FANOUT_STATE_WRITE_QUORUM, write_quorum),
        (PUT_RENAME_QUORUM_FANOUT_STATE_SUCCESS, success),
        (PUT_RENAME_QUORUM_FANOUT_STATE_ERROR, error),
        (PUT_RENAME_QUORUM_FANOUT_STATE_PANIC, panicked),
    ] {
        histogram!("rustfs_s3_put_object_rename_quorum_wait_fanout_disks", "state" => state).record(put_stage_count_value(count));
    }
}

/// Record generic internal operation stage duration (non-PUT paths).
/// Use this for metacache walks, listing, lifecycle, and other background
/// operations that are NOT part of the PUT object hot path.
#[inline(always)]
pub fn record_stage_duration(stage: &'static str, duration_ms: f64) {
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
    counter_increment_cached!("rustfs_io_transfer_bytes_total", bytes);
    histogram_record_cached!("rustfs_io_transfer_duration_ms", duration_ms);

    if duration_ms > 0.0 {
        let bps = (bytes as f64 * 1000.0) / duration_ms;
        histogram_record_cached!("rustfs_io_transfer_bandwidth_bps", bps);
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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

/// Allocator memory stats captured from the active process allocator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AllocatorMemoryObservation {
    pub reserved_bytes: Option<u64>,
    pub committed_bytes: Option<u64>,
    pub page_committed_bytes: Option<u64>,
    pub malloc_requested_bytes: Option<u64>,
    pub malloc_requested_peak_bytes: Option<u64>,
    pub malloc_requested_total_bytes: Option<u64>,
    pub heap_count: Option<u64>,
}

/// Record allocator-level memory attribution when supported by the allocator.
#[inline(always)]
pub fn record_allocator_memory_observation(backend: &'static str, observation: AllocatorMemoryObservation) {
    if !metrics_enabled() {
        return;
    }
    if let Some(reserved_bytes) = observation.reserved_bytes {
        gauge!("rustfs_memory_allocator_reserved_bytes", "backend" => backend).set(reserved_bytes as f64);
    }
    if let Some(committed_bytes) = observation.committed_bytes {
        gauge!("rustfs_memory_allocator_committed_bytes", "backend" => backend).set(committed_bytes as f64);
    }
    if let Some(page_committed_bytes) = observation.page_committed_bytes {
        gauge!("rustfs_memory_allocator_page_committed_bytes", "backend" => backend).set(page_committed_bytes as f64);
    }
    if let Some(malloc_requested_bytes) = observation.malloc_requested_bytes {
        gauge!("rustfs_memory_allocator_malloc_requested_bytes", "backend" => backend).set(malloc_requested_bytes as f64);
    }
    if let Some(malloc_requested_peak_bytes) = observation.malloc_requested_peak_bytes {
        gauge!("rustfs_memory_allocator_malloc_requested_peak_bytes", "backend" => backend)
            .set(malloc_requested_peak_bytes as f64);
    }
    if let Some(malloc_requested_total_bytes) = observation.malloc_requested_total_bytes {
        gauge!("rustfs_memory_allocator_malloc_requested_total_bytes", "backend" => backend)
            .set(malloc_requested_total_bytes as f64);
    }
    if let Some(heap_count) = observation.heap_count {
        gauge!("rustfs_memory_allocator_heap_count", "backend" => backend).set(heap_count as f64);
    }
}

/// Track encoded bytes in the queue hand-off between erasure encode and disk writers.
///
/// This is queue occupancy, not a per-request or process-RSS memory limit. The
/// erasure encoder settles it on failed/cancelled sends, receiver drop, and
/// consumer hand-off before shard writes begin.
#[inline(always)]
pub fn add_ec_encode_inflight_bytes(bytes: usize) {
    let next = EC_ENCODE_INFLIGHT_BYTES.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
    gauge_set_cached!("rustfs_ec_encode_inflight_bytes_current", next as f64);
    if put_stage_metrics_enabled() {
        publish_ec_encode_peak(&EC_ENCODE_QUEUE_BYTES_PEAK, EcEncodePeakMetric::Queue, next);
    }
}

/// Remove encoded bytes from the tracked erasure encode in-flight gauge.
#[inline(always)]
pub fn remove_ec_encode_inflight_bytes(bytes: usize) {
    let next = saturating_sub_atomic(&EC_ENCODE_INFLIGHT_BYTES, bytes as u64);
    gauge_set_cached!("rustfs_ec_encode_inflight_bytes_current", next as f64);
}

/// Return the current tracked EC encode in-flight bytes.
#[inline(always)]
pub fn current_ec_encode_inflight_bytes() -> u64 {
    EC_ENCODE_INFLIGHT_BYTES.load(Ordering::Relaxed)
}

/// Tracks encoded payload bytes held before queue hand-off or during shard writes.
///
/// Each guard contributes to a process-wide stage total until it is dropped. The
/// reported peak therefore includes concurrent PUTs, but excludes reader,
/// allocator, and transport buffers; it is not a per-PUT or process-RSS limit.
pub struct EcEncodePayloadStageGuard {
    counter: &'static AtomicU64,
    bytes: u64,
    enabled: bool,
    current_metric: EcEncodePeakMetric,
}

impl Drop for EcEncodePayloadStageGuard {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }
        let next = saturating_sub_atomic(self.counter, self.bytes);
        match self.current_metric {
            EcEncodePeakMetric::Producer => gauge_set_cached!("rustfs_ec_encode_producer_bytes_current", next as f64),
            EcEncodePeakMetric::Queue => unreachable!("queue bytes use their own ownership guard"),
            EcEncodePeakMetric::Writer => gauge_set_cached!("rustfs_ec_encode_writer_bytes_current", next as f64),
        }
    }
}

fn track_ec_encode_payload_stage(
    bytes: usize,
    counter: &'static AtomicU64,
    peak: &'static AtomicU64,
    metric: EcEncodePeakMetric,
) -> EcEncodePayloadStageGuard {
    let enabled = put_stage_metrics_enabled();
    let bytes = bytes as u64;
    if enabled {
        let next = counter.fetch_add(bytes, Ordering::Relaxed) + bytes;
        match metric {
            EcEncodePeakMetric::Producer => gauge_set_cached!("rustfs_ec_encode_producer_bytes_current", next as f64),
            EcEncodePeakMetric::Queue => unreachable!("queue bytes use their own ownership guard"),
            EcEncodePeakMetric::Writer => gauge_set_cached!("rustfs_ec_encode_writer_bytes_current", next as f64),
        }
        publish_ec_encode_peak(peak, metric, next);
    }
    EcEncodePayloadStageGuard {
        counter,
        bytes,
        enabled,
        current_metric: metric,
    }
}

/// Track encoded producer payload bytes until queue hand-off completes.
#[inline(always)]
pub fn track_ec_encode_producer_bytes(bytes: usize) -> EcEncodePayloadStageGuard {
    track_ec_encode_payload_stage(
        bytes,
        &EC_ENCODE_PRODUCER_BYTES_CURRENT,
        &EC_ENCODE_PRODUCER_BYTES_PEAK,
        EcEncodePeakMetric::Producer,
    )
}

/// Track encoded payload bytes while shard writers own the batch.
#[inline(always)]
pub fn track_ec_encode_writer_bytes(bytes: usize) -> EcEncodePayloadStageGuard {
    track_ec_encode_payload_stage(
        bytes,
        &EC_ENCODE_WRITER_BYTES_CURRENT,
        &EC_ENCODE_WRITER_BYTES_PEAK,
        EcEncodePeakMetric::Writer,
    )
}

/// Return the process-lifetime high-water mark of encoded producer payload bytes.
#[inline(always)]
pub fn current_ec_encode_producer_bytes_peak() -> u64 {
    EC_ENCODE_PRODUCER_BYTES_PEAK.load(Ordering::Relaxed)
}

/// Return the current process-wide encoded producer payload bytes.
#[inline(always)]
pub fn current_ec_encode_producer_bytes() -> u64 {
    EC_ENCODE_PRODUCER_BYTES_CURRENT.load(Ordering::Relaxed)
}

/// Return the process-lifetime high-water mark of encoded queue payload bytes.
#[inline(always)]
pub fn current_ec_encode_queue_bytes_peak() -> u64 {
    EC_ENCODE_QUEUE_BYTES_PEAK.load(Ordering::Relaxed)
}

/// Return the process-lifetime high-water mark of encoded writer payload bytes.
#[inline(always)]
pub fn current_ec_encode_writer_bytes_peak() -> u64 {
    EC_ENCODE_WRITER_BYTES_PEAK.load(Ordering::Relaxed)
}

/// Return the current process-wide encoded writer payload bytes.
#[inline(always)]
pub fn current_ec_encode_writer_bytes() -> u64 {
    EC_ENCODE_WRITER_BYTES_CURRENT.load(Ordering::Relaxed)
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
    histogram_record_cached!("rustfs_io_latency_ms", latency_ms);
}

/// Record I/O latency P95 in milliseconds.
///
/// # Arguments
///
/// * `latency_ms` - P95 I/O latency in milliseconds
#[inline(always)]
pub fn record_io_latency_p95(latency_ms: f64) {
    if !metrics_enabled() {
        return;
    }
    gauge_set_cached!("rustfs_io_latency_p95_ms", latency_ms);
}

/// Record I/O latency P99 in milliseconds.
///
/// # Arguments
///
/// * `latency_ms` - P99 I/O latency in milliseconds
#[inline(always)]
pub fn record_io_latency_p99(latency_ms: f64) {
    if !metrics_enabled() {
        return;
    }
    gauge_set_cached!("rustfs_io_latency_p99_ms", latency_ms);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier, Mutex};

    // Serialize tests that mutate the process-global PUT_STAGE_METRICS_ENABLED flag.
    pub(crate) static METRICS_FLAG_LOCK: Mutex<()> = Mutex::new(());

    /// One row of a `DebuggingRecorder` snapshot.
    pub(crate) type MetricRow = (
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    );

    /// Every metric name present in a snapshot.
    pub(crate) fn emitted_names(rows: &[MetricRow]) -> std::collections::HashSet<&str> {
        rows.iter().map(|(composite, _, _, _)| composite.key().name()).collect()
    }

    /// Counter value for `name`, summed over every label set it was emitted with.
    /// `None` means the counter never reached the recorder.
    pub(crate) fn counter_total(rows: &[MetricRow], name: &str) -> Option<u64> {
        let mut total = None;
        for (composite, _, _, value) in rows {
            if composite.kind() == MetricKind::Counter && composite.key().name() == name {
                match value {
                    DebugValue::Counter(count) => *total.get_or_insert(0) += count,
                    other => panic!("{name} is registered as a counter but holds {other:?}"),
                }
            }
        }
        total
    }

    /// Gauge value for `name`. Panics when several label sets carry it, so a caller
    /// cannot silently assert on an arbitrary one.
    pub(crate) fn gauge_value(rows: &[MetricRow], name: &str) -> Option<f64> {
        let mut matching = rows
            .iter()
            .filter(|(composite, _, _, _)| composite.kind() == MetricKind::Gauge && composite.key().name() == name);
        let value = matching.next().map(|(_, _, _, value)| match value {
            DebugValue::Gauge(value) => value.0,
            other => panic!("{name} is registered as a gauge but holds {other:?}"),
        });
        assert!(
            matching.next().is_none(),
            "{name} carries several label sets; assert on the labelled rows instead"
        );
        value
    }

    /// Histogram samples for `name` across every label set, sorted so the assertion
    /// does not depend on registry iteration order.
    pub(crate) fn histogram_samples(rows: &[MetricRow], name: &str) -> Vec<f64> {
        let mut samples: Vec<f64> = rows
            .iter()
            .filter(|(composite, _, _, _)| composite.kind() == MetricKind::Histogram && composite.key().name() == name)
            .flat_map(|(_, _, _, value)| match value {
                DebugValue::Histogram(samples) => samples.iter().map(|sample| sample.0),
                other => panic!("{name} is registered as a histogram but holds {other:?}"),
            })
            .collect();
        samples.sort_by(f64::total_cmp);
        samples
    }

    /// Replaces four smoke tests that called the zero-copy and bytes-pool recorders
    /// and asserted nothing (rustfs/backlog#1836). The same calls now run against a
    /// local recorder: every metric name these helpers own must be emitted, the
    /// `from_pool` branch must pick the hit/miss counter, and the derived values
    /// (byte totals, the hit rate's percent conversion) must match the inputs.
    #[test]
    fn zero_copy_and_bytes_pool_helpers_emit_their_metrics() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_metrics_enabled(true);
            record_zero_copy_read(1024, 10.5);
            record_memory_copy_saved(1024);
            record_zero_copy_fallback("test");
            record_zero_copy_write(2048, 20.5);
            record_zero_copy_write_fallback("test");
            record_bytes_saved(4096);
            record_bytes_pool_acquire("small", 4096, true);
            record_bytes_pool_acquire("small", 4096, false);
            record_bytes_pool_return("small");
            record_bytes_pool_allocated("small", 4096);
            record_bytes_pool_hit_rate("small", 0.85);
            record_bytespool_acquisition("small", "hit");
            record_bytespool_acquisition("medium", "miss");
            record_bytespool_return("small", "recycled");
            record_bytespool_return("medium", "dropped");
            set_metrics_enabled(false);
        });

        let rows = snapshotter.snapshot().into_vec();
        let names = emitted_names(&rows);
        for expected in [
            "rustfs_zero_copy_reads_total",
            "rustfs_zero_copy_read_size_bytes",
            "rustfs_zero_copy_read_duration_ms",
            mmap_copy::READS_TOTAL,
            mmap_copy::READ_SIZE_BYTES,
            mmap_copy::READ_DURATION_MS,
            mmap_copy::BYTES_COPIED_TOTAL,
            mmap_copy::FALLBACK_TOTAL,
            "rustfs_zero_copy_memory_saved_bytes_total",
            "rustfs_zero_copy_fallback_total",
            "rustfs_zero_copy_write_total",
            "rustfs_zero_copy_write_size_bytes",
            "rustfs_zero_copy_write_duration_ms",
            buffered_write::WRITES_TOTAL,
            buffered_write::WRITE_SIZE_BYTES,
            buffered_write::WRITE_DURATION_MS,
            buffered_write::BYTES_COPIED_TOTAL,
            buffered_write::FALLBACK_TOTAL,
            "rustfs_zero_copy_write_fallback_total",
            "rustfs_zero_copy_bytes_saved_total",
            "rustfs_bytes_pool_acquisitions_total",
            "rustfs_bytes_pool_size_bytes",
            "rustfs_bytes_pool_hits_total",
            "rustfs_bytes_pool_misses_total",
            "rustfs_bytes_pool_returns_total",
            "rustfs_bytes_pool_allocated_bytes",
            "rustfs_bytes_pool_hit_rate",
            "rustfs_io_bytespool_acquisition_total",
            "rustfs_io_bytespool_return_total",
        ] {
            assert!(names.contains(expected), "{expected} must be emitted by its record helper");
        }

        assert_eq!(
            counter_total(&rows, mmap_copy::BYTES_COPIED_TOTAL),
            Some(1024),
            "the read helper must count the read size, not the call"
        );
        assert_eq!(
            counter_total(&rows, buffered_write::BYTES_COPIED_TOTAL),
            Some(2048),
            "the write helper must count the write size, not the call"
        );
        assert_eq!(counter_total(&rows, "rustfs_zero_copy_memory_saved_bytes_total"), Some(1024));
        assert_eq!(counter_total(&rows, "rustfs_zero_copy_bytes_saved_total"), Some(4096));
        assert_eq!(histogram_samples(&rows, "rustfs_zero_copy_read_duration_ms"), vec![10.5]);
        assert_eq!(histogram_samples(&rows, "rustfs_zero_copy_write_duration_ms"), vec![20.5]);
        assert_eq!(
            counter_total(&rows, "rustfs_bytes_pool_hits_total"),
            Some(1),
            "only the from_pool acquisition counts as a hit"
        );
        assert_eq!(
            counter_total(&rows, "rustfs_bytes_pool_misses_total"),
            Some(1),
            "only the non-pool acquisition counts as a miss"
        );
        assert_eq!(
            gauge_value(&rows, "rustfs_bytes_pool_hit_rate"),
            Some(85.0),
            "the hit rate is exported as a percentage"
        );
    }

    #[test]
    fn test_record_batch_processor_observation() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_get_stage_metrics_enabled(true);
        record_batch_processor_observation(BatchProcessorObservation {
            operation: "read",
            batch_size: 16,
            configured_concurrency: 8,
            max_queue_wait_secs: 0.001,
            execution_latency_secs: 0.025,
            successes: 15,
            errors: 1,
            timeouts: 0,
            suggested_concurrency: 10,
            suggestion_reason: "improving",
        });
        assert!(get_stage_metrics_enabled());
        set_get_stage_metrics_enabled(false);
    }

    #[test]
    fn test_record_get_object_stage_metrics() {
        record_get_object_stage_duration("s3_handler", "request_context", 0.001);
        record_get_object_stage_duration_by_size("legacy_duplex", "metadata", "plain_single_part", "le_4kib", 0.001);
        record_get_object_reader_path("codec_streaming");
        record_get_object_reader_path_by_size("codec_streaming", "plain_single_part", "le_1mib");
        record_get_object_codec_streaming_fallback("range");
        record_get_object_codec_streaming_decision("fallback", "range", "range");
        record_get_object_codec_streaming_decision("use", "plain_single_part", "none");
        record_get_object_codec_streaming_decision_by_size("fallback", "plain_single_part", "below_min_size", "le_128kib");
        record_get_object_reader_stripe("codec_streaming");
        record_get_object_reader_bytes("codec_streaming", 1024);
        record_get_object_reader_buffer("codec_streaming", "output", 1024);
        record_get_object_reader_copy("codec_streaming", 512, 8192, 1024, 0.0001);
        record_get_object_reader_poll("codec_streaming", "ready_data", 8192, 512, 0.0002);
        record_get_object_reader_prefetch("codec_streaming", "stored");
        record_get_object_reader_prefetch_wait("codec_streaming", 0.0002);
        record_get_object_response_handoff("standard", "selected", 8192, 1024, 0.0001);
        record_get_object_metadata_fanout_duration("legacy_duplex", 0.001);
        record_get_object_stage_duration("legacy_duplex", "read_version_path_resolve", 0.0001);
        record_get_object_stage_duration("legacy_duplex", "read_version_path_check", 0.0001);
        record_get_object_stage_duration("legacy_duplex", "read_version_xlmeta_read", 0.0005);
        record_get_object_stage_duration("legacy_duplex", "read_version_decode", 0.0002);
        record_get_object_first_metadata_response_latency("legacy_duplex", 0.001);
        record_get_object_first_valid_metadata_response_latency("legacy_duplex", 0.001);
        record_get_object_slowest_metadata_response_latency("legacy_duplex", 0.003);
        record_get_object_quorum_reached_latency("legacy_duplex", 0.002);
        record_get_object_metadata_response("legacy_duplex", "valid");
        record_get_object_metadata_fanout_shape("legacy_duplex", 4, 3, 1, 1);
        record_get_object_metadata_fanout_lifecycle("legacy_duplex", 4, 3, 1);
        record_get_object_metadata_early_stop_hit("legacy_duplex", "valid_quorum");
        record_get_object_metadata_early_stop_miss("legacy_duplex", "insufficient_quorum");
        record_get_object_metadata_early_stop_saved_responses("legacy_duplex", 1);
        record_get_object_reader_setup_duration("legacy_duplex", 0.003);
        record_get_object_first_shard_read_duration("codec_streaming", 0.004);
        record_get_object_bitrot_verify_duration("codec_streaming", 0.005);
        record_get_object_reconstruct_duration("codec_streaming", 0.006);
        record_get_object_reconstruct_outcome("codec_streaming", "legacy", "skip_data_complete");
        record_get_object_emit_duration("codec_streaming", 0.007);
        record_get_object_first_byte_latency("s3_handler", 0.008);
        record_get_object_full_body_latency("s3_handler", 0.009);
        record_get_object_response_handoff_duration("s3_handler", 0.0001);
        record_get_object_metadata_phase_duration(0.002);
        record_get_object_metadata_phase_duration_with_early_stop(0.002, "hit");
        record_get_object_total_duration_with_path(0.050, "legacy_duplex");
        record_get_object_shard_reader_setup_duration(0.003);
        record_get_object_decode_duration(0.004);
        record_get_object_duplex_backpressure_duration(0.005);
        record_get_object_pipeline_failure("decode", "read_quorum");
        record_get_object_pipeline_failure_for_path("codec_streaming", "decode", "read_quorum");
        record_get_object_shard_read_observation("codec_streaming", 0, "data", "local", "success", "none", 1024, 0.004, 0.001);
        record_get_object_shard_read_cost_summary("codec_streaming", 3, 1, 2, 0, 4, 4, 4, true);
        record_get_object_shard_locality_observe_only("codec_streaming", 2, 1);
        record_get_object_reader_setup_strategy("data_blocks_first", "read_quorum");
        record_get_object_reader_setup_strategy_by_size(
            "codec_streaming",
            "data_blocks_first",
            "read_quorum",
            "plain_single_part",
            "le_1mib",
        );
        record_get_object_reader_setup_fanout("data_blocks_first", "read_quorum", 3, 2, 2, 0, 2);
        record_get_object_reader_setup_fanout_by_size(
            "codec_streaming",
            "data_blocks_first",
            "read_quorum",
            "plain_single_part",
            "le_1mib",
            3,
            2,
            2,
            0,
            2,
        );
        record_batch_processor_observation(BatchProcessorObservation {
            operation: "read",
            batch_size: 16,
            configured_concurrency: 8,
            max_queue_wait_secs: 0.001,
            execution_latency_secs: 0.025,
            successes: 15,
            errors: 1,
            timeouts: 0,
            suggested_concurrency: 10,
            suggestion_reason: "improving",
        });

        assert!(0.005_f64.is_sign_positive());
    }

    #[test]
    fn test_get_object_shard_locality_observe_only_metrics_smoke() {
        let remote_scheduled = 2;
        let remote_avoid_potential = 1;

        record_get_object_shard_locality_observe_only("codec_streaming", remote_scheduled, remote_avoid_potential);

        assert!(remote_scheduled >= remote_avoid_potential);
    }

    #[test]
    fn metadata_fanout_lifecycle_records_named_histograms() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_get_stage_metrics_enabled(true);
            record_get_object_metadata_fanout_lifecycle("legacy_duplex", 4, 3, 1);
            set_get_stage_metrics_enabled(false);
        });

        let metrics = snapshotter.snapshot().into_vec();
        for (name, expected) in [
            ("rustfs_io_get_object_metadata_fanout_scheduled", 4.0),
            ("rustfs_io_get_object_metadata_fanout_completed", 3.0),
            ("rustfs_io_get_object_metadata_fanout_cancelled", 1.0),
        ] {
            let value = metrics.iter().find_map(|(composite, _, _, value)| {
                let has_path = composite
                    .key()
                    .labels()
                    .any(|label| label.key() == "path" && label.value() == "legacy_duplex");
                (composite.kind() == MetricKind::Histogram && composite.key().name() == name && has_path).then_some(value)
            });
            assert!(
                matches!(value, Some(DebugValue::Histogram(values)) if values.len() == 1 && values[0].0 == expected),
                "{name} must record the exact fanout lifecycle sample"
            );
        }
    }

    #[test]
    fn test_record_get_object_fill_metrics() {
        record_get_object_fill_queued("codec_streaming", "single_inflight", 1);
        record_get_object_fill_started("codec_streaming", "single_inflight");
        record_get_object_fill_worker_started("codec_streaming", "single_inflight");
        record_get_object_fill_completed_before_output_drained("codec_streaming", "single_inflight");
        record_get_object_fill_waited_by_output("codec_streaming", "single_inflight", 0.0003);
        record_get_object_fill_cancelled_on_drop("codec_streaming", "single_inflight");
        record_get_object_reader_prefetch_bytes("codec_streaming", "single_inflight", 4096);
        record_get_object_reader_stream_buffer_size("standard", "selected", 131072);
        record_get_object_reader_stream_poll("standard", "selected", "ready_data", 8192, 4096, 0.0002);
        record_get_object_memory_body_stream_poll("buffered_body", "ready_data", 4096, 0.0001);

        assert!(0.0003_f64.is_sign_positive());
    }

    /// Replaces five smoke tests (`test_record_get_object`, `test_record_put_object`,
    /// `test_record_put_object_request_metrics`, `test_record_list_objects`,
    /// `test_record_delete_object`) that called the S3 operation recorders and
    /// asserted nothing (rustfs/backlog#1836). Besides pinning the metric names,
    /// this pins the conditional emissions each helper owns: the zero-copy alias
    /// counters fire only for an eligible PUT, the truncated/version counters only
    /// for the truncated listing and the versioned delete.
    #[test]
    fn s3_operation_helpers_emit_their_metrics() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_metrics_enabled(true);
            set_get_stage_metrics_enabled(true);
            record_get_object(100.0, 1024 * 1024);
            record_get_object(50.0, 2048);
            record_put_object(200.0, 1024 * 1024, true);
            record_put_object(100.0, 512, false);
            record_put_object_request_start(3);
            record_put_object_request_result("ok", 0.25);
            record_put_object_request_result("error", 0.5);
            record_list_objects(50.0, 100, false);
            record_list_objects(75.0, 1000, true);
            record_delete_object(25.0, false);
            record_delete_object(30.0, true);
            set_get_stage_metrics_enabled(false);
            set_metrics_enabled(false);
        });

        let rows = snapshotter.snapshot().into_vec();
        let names = emitted_names(&rows);
        for expected in [
            "rustfs_s3_get_object_total",
            "rustfs_s3_get_object_duration_ms",
            "rustfs_s3_get_object_size_bytes",
            "rustfs_s3_put_object_total",
            "rustfs_s3_put_object_duration_ms",
            "rustfs_s3_put_object_size_bytes",
            "rustfs_s3_put_object_zero_copy_enabled_total",
            "rustfs_s3_put_object_zero_copy_eligible_total",
            "rustfs_io_put_object_requests_total",
            "rustfs_io_put_object_concurrent_requests",
            "rustfs_io_put_object_request_results_total",
            "rustfs_io_put_object_request_duration_seconds",
            "rustfs_s3_list_objects_total",
            "rustfs_s3_list_objects_duration_ms",
            "rustfs_s3_list_objects_count",
            "rustfs_s3_list_objects_truncated_total",
            "rustfs_s3_delete_object_total",
            "rustfs_s3_delete_object_duration_ms",
            "rustfs_s3_delete_object_version_total",
        ] {
            assert!(names.contains(expected), "{expected} must be emitted by its record helper");
        }

        assert_eq!(counter_total(&rows, "rustfs_s3_get_object_total"), Some(2));
        assert_eq!(counter_total(&rows, "rustfs_s3_put_object_total"), Some(2));
        assert_eq!(
            counter_total(&rows, "rustfs_s3_put_object_zero_copy_eligible_total"),
            Some(1),
            "only the zero-copy eligible PUT increments the eligibility counter"
        );
        assert_eq!(
            counter_total(&rows, "rustfs_s3_put_object_zero_copy_enabled_total"),
            Some(1),
            "the historical alias must stay in step with the eligibility counter"
        );
        assert_eq!(
            counter_total(&rows, "rustfs_s3_list_objects_truncated_total"),
            Some(1),
            "only the truncated listing increments the truncation counter"
        );
        assert_eq!(
            counter_total(&rows, "rustfs_s3_delete_object_version_total"),
            Some(1),
            "only the versioned delete increments the version counter"
        );
        assert_eq!(
            histogram_samples(&rows, "rustfs_s3_list_objects_count"),
            vec![100.0, 1000.0],
            "the object count, not the duration, belongs in the count histogram"
        );
        assert_eq!(
            gauge_value(&rows, "rustfs_io_put_object_concurrent_requests"),
            Some(3.0),
            "the concurrency gauge must carry the reported in-flight request count"
        );
    }

    #[test]
    fn test_record_put_object_path_and_stage() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_put_stage_metrics_enabled(true);
        record_put_object_path("small_eager");
        record_put_object_path("write_inline");
        record_put_object_stage_duration("ingress_prepare", 12.5);
        record_put_object_stage_duration("set_disk_encode", 8.0);
        record_put_object_diagnostics("zero_copy_eager", "eligible", 32 * 1024 * 1024, 256 * 1024, true);
        assert!(put_stage_metrics_enabled());
        set_put_stage_metrics_enabled(false);
    }

    #[test]
    fn put_stage_sync_tail_labels_are_static_and_gated() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(PUT_STAGE_PUT_OBJECT_COMMIT_NAMESPACE_LOCK_WAIT, "put_object_commit_namespace_lock_wait");
        let stages = [
            PUT_STAGE_PUT_OBJECT_COMMIT_NAMESPACE_LOCK_WAIT,
            PUT_STAGE_SET_DISK_RENAME_QUORUM_WAIT,
            PUT_STAGE_SET_DISK_RENAME_DISK_WAIT,
            PUT_STAGE_SET_DISK_RENAME_FILE_SYNC_PERMIT_WAIT,
            PUT_STAGE_SET_DISK_RENAME_GLOBAL_FILE_SYNC_PERMIT_WAIT,
            PUT_STAGE_SET_DISK_RENAME_FILE_FDATASYNC,
            PUT_STAGE_SET_DISK_RENAME_SRC_DIR_FSYNC,
            PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC,
            PUT_STAGE_SET_DISK_RENAME_BACKUP_DIR_FSYNC,
            PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC,
            PUT_STAGE_SET_DISK_RENAME_RENAME_SYSCALL,
        ];
        let unique = stages.iter().copied().collect::<HashSet<_>>();
        assert_eq!(unique.len(), stages.len());
        assert!(stages.iter().all(|stage| {
            (stage.starts_with("set_disk_rename_") || *stage == PUT_STAGE_PUT_OBJECT_COMMIT_NAMESPACE_LOCK_WAIT)
                && !stage.contains('/')
                && !stage.contains('{')
        }));

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            set_put_stage_metrics_enabled(false);
            for stage in stages {
                record_put_object_stage_duration(stage, 1.0);
            }
            set_put_stage_metrics_enabled(true);
            for stage in stages {
                record_put_object_stage_duration(stage, 1.0);
            }
            set_put_stage_metrics_enabled(false);
        });

        let recorded = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| {
                composite.kind() == MetricKind::Histogram && composite.key().name() == "rustfs_s3_put_object_stage_duration_ms"
            })
            .flat_map(|(composite, _, _, _)| {
                composite
                    .key()
                    .labels()
                    .filter(|label| label.key() == "stage")
                    .map(|label| label.value().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        assert_eq!(recorded.len(), stages.len());
        assert!(stages.iter().all(|stage| recorded.contains(*stage)));
    }

    #[test]
    fn put_commit_lock_admission_labels_are_static_and_gated() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let budgets = [
            PUT_COMMIT_LOCK_ADMISSION_BUDGET_DISABLED,
            PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS,
            PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_500MS,
            PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_1000MS,
            PUT_COMMIT_LOCK_ADMISSION_BUDGET_GT_1000MS,
        ];
        let outcomes = [
            PUT_COMMIT_LOCK_ADMISSION_OUTCOME_ACQUIRED,
            PUT_COMMIT_LOCK_ADMISSION_OUTCOME_TIMEOUT_SLOWDOWN,
            PUT_COMMIT_LOCK_ADMISSION_OUTCOME_LOCK_ERROR,
        ];
        assert_eq!(budgets.iter().copied().collect::<HashSet<_>>().len(), budgets.len());
        assert_eq!(outcomes.iter().copied().collect::<HashSet<_>>().len(), outcomes.len());
        assert!(budgets.iter().chain(outcomes.iter()).all(|label| {
            !label.contains('/')
                && !label.contains('{')
                && !label.contains('}')
                && !label.contains(' ')
                && label
                    .chars()
                    .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        }));

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            set_put_stage_metrics_enabled(false);
            record_put_object_commit_lock_admission(
                PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS,
                PUT_COMMIT_LOCK_ADMISSION_OUTCOME_TIMEOUT_SLOWDOWN,
            );

            set_put_stage_metrics_enabled(true);
            record_put_object_commit_lock_admission(
                PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS,
                PUT_COMMIT_LOCK_ADMISSION_OUTCOME_TIMEOUT_SLOWDOWN,
            );
            record_put_object_commit_lock_admission(
                PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_500MS,
                PUT_COMMIT_LOCK_ADMISSION_OUTCOME_ACQUIRED,
            );
            set_put_stage_metrics_enabled(false);
        });

        let rows = snapshotter.snapshot().into_vec();
        assert_eq!(
            counter_total(&rows, "rustfs_s3_put_object_commit_namespace_lock_admission_total"),
            Some(2)
        );
        let label_sets = rows
            .iter()
            .filter(|(composite, _, _, _)| {
                composite.kind() == MetricKind::Counter
                    && composite.key().name() == "rustfs_s3_put_object_commit_namespace_lock_admission_total"
            })
            .map(|(composite, _, _, _)| {
                composite
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect::<HashSet<_>>()
            })
            .collect::<Vec<_>>();
        assert!(label_sets.contains(&HashSet::from([
            ("budget".to_string(), PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_250MS.to_string()),
            ("outcome".to_string(), PUT_COMMIT_LOCK_ADMISSION_OUTCOME_TIMEOUT_SLOWDOWN.to_string(),),
        ])));
        assert!(label_sets.contains(&HashSet::from([
            ("budget".to_string(), PUT_COMMIT_LOCK_ADMISSION_BUDGET_LE_500MS.to_string()),
            ("outcome".to_string(), PUT_COMMIT_LOCK_ADMISSION_OUTCOME_ACQUIRED.to_string()),
        ])));
    }

    #[test]
    fn put_rename_code_level_metrics_are_static_and_gated() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            set_put_stage_metrics_enabled(false);
            record_put_rename_fdatasync_batch(PUT_RENAME_FDATASYNC_BATCH_MODE_SERIAL, 2);
            record_put_rename_fdatasync_group_wait(PUT_RENAME_FDATASYNC_GROUP_WAIT_ROLE_LEADER, 1.0);
            record_put_rename_fdatasync_group_outstanding(PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_ENQUEUE_WAITERS, 2);
            record_put_rename_disk_wait_completion(PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_FIRST, 3.0);
            record_put_rename_quorum_wait_fanout(4, 3, 3, 1, 0);

            set_put_stage_metrics_enabled(true);
            record_put_rename_fdatasync_batch(PUT_RENAME_FDATASYNC_BATCH_MODE_PARALLEL, 9);
            record_put_rename_fdatasync_group_wait(PUT_RENAME_FDATASYNC_GROUP_WAIT_ROLE_LEADER, 1.0);
            record_put_rename_fdatasync_group_wait(PUT_RENAME_FDATASYNC_GROUP_WAIT_ROLE_FOLLOWER, 2.0);
            record_put_rename_fdatasync_group_outstanding(PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_ENQUEUE_WAITERS, 2);
            record_put_rename_fdatasync_group_outstanding(PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_ENQUEUE_FILES, 4);
            record_put_rename_fdatasync_group_outstanding(PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_BATCH_WAITERS, 3);
            record_put_rename_fdatasync_group_outstanding(PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_BATCH_FILES, 6);
            record_put_rename_disk_wait_completion(PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_FIRST, 3.0);
            record_put_rename_disk_wait_completion(PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_TAIL, 4.0);
            record_put_rename_disk_wait_completion(PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_ERROR, 5.0);
            record_put_rename_quorum_wait_fanout(4, 3, 3, 1, 0);
            set_put_stage_metrics_enabled(false);
        });

        let rows = snapshotter.snapshot().into_vec();
        assert_eq!(histogram_samples(&rows, "rustfs_s3_put_object_rename_fdatasync_batch_files"), vec![9.0]);
        let batch_modes = rows
            .iter()
            .filter(|(composite, _, _, _)| {
                composite.kind() == MetricKind::Histogram
                    && composite.key().name() == "rustfs_s3_put_object_rename_fdatasync_batch_files"
            })
            .flat_map(|(composite, _, _, _)| {
                composite
                    .key()
                    .labels()
                    .filter(|label| label.key() == "mode")
                    .map(|label| label.value().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        assert_eq!(batch_modes, HashSet::from([PUT_RENAME_FDATASYNC_BATCH_MODE_PARALLEL.to_string()]));

        let quorum_samples = histogram_samples(&rows, "rustfs_s3_put_object_rename_quorum_wait_fanout_disks");
        assert_eq!(quorum_samples, vec![0.0, 1.0, 3.0, 3.0, 4.0]);
        assert_eq!(
            histogram_samples(&rows, "rustfs_s3_put_object_rename_fdatasync_group_wait_ms"),
            vec![1.0, 2.0]
        );
        assert_eq!(
            histogram_samples(&rows, "rustfs_s3_put_object_rename_fdatasync_group_outstanding"),
            vec![2.0, 3.0, 4.0, 6.0]
        );
        assert_eq!(
            histogram_samples(&rows, "rustfs_s3_put_object_rename_disk_wait_completion_ms"),
            vec![3.0, 4.0, 5.0]
        );
        let group_wait_roles = rows
            .iter()
            .filter(|(composite, _, _, _)| {
                composite.kind() == MetricKind::Histogram
                    && composite.key().name() == "rustfs_s3_put_object_rename_fdatasync_group_wait_ms"
            })
            .flat_map(|(composite, _, _, _)| {
                composite
                    .key()
                    .labels()
                    .filter(|label| label.key() == "role")
                    .map(|label| label.value().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        assert_eq!(
            group_wait_roles,
            HashSet::from([
                PUT_RENAME_FDATASYNC_GROUP_WAIT_ROLE_LEADER.to_string(),
                PUT_RENAME_FDATASYNC_GROUP_WAIT_ROLE_FOLLOWER.to_string(),
            ])
        );
        let group_outstanding_states = rows
            .iter()
            .filter(|(composite, _, _, _)| {
                composite.kind() == MetricKind::Histogram
                    && composite.key().name() == "rustfs_s3_put_object_rename_fdatasync_group_outstanding"
            })
            .flat_map(|(composite, _, _, _)| {
                composite
                    .key()
                    .labels()
                    .filter(|label| label.key() == "state")
                    .map(|label| label.value().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        assert_eq!(
            group_outstanding_states,
            HashSet::from([
                PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_ENQUEUE_WAITERS.to_string(),
                PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_ENQUEUE_FILES.to_string(),
                PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_BATCH_WAITERS.to_string(),
                PUT_RENAME_FDATASYNC_GROUP_OUTSTANDING_STATE_BATCH_FILES.to_string(),
            ])
        );
        let disk_wait_positions = rows
            .iter()
            .filter(|(composite, _, _, _)| {
                composite.kind() == MetricKind::Histogram
                    && composite.key().name() == "rustfs_s3_put_object_rename_disk_wait_completion_ms"
            })
            .flat_map(|(composite, _, _, _)| {
                composite
                    .key()
                    .labels()
                    .filter(|label| label.key() == "position")
                    .map(|label| label.value().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        assert_eq!(
            disk_wait_positions,
            HashSet::from([
                PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_FIRST.to_string(),
                PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_QUORUM_TAIL.to_string(),
                PUT_RENAME_DISK_WAIT_COMPLETION_POSITION_ERROR.to_string(),
            ])
        );
        let quorum_states = rows
            .iter()
            .filter(|(composite, _, _, _)| {
                composite.kind() == MetricKind::Histogram
                    && composite.key().name() == "rustfs_s3_put_object_rename_quorum_wait_fanout_disks"
            })
            .flat_map(|(composite, _, _, _)| {
                composite
                    .key()
                    .labels()
                    .filter(|label| label.key() == "state")
                    .map(|label| label.value().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        assert_eq!(
            quorum_states,
            HashSet::from([
                PUT_RENAME_QUORUM_FANOUT_STATE_SCHEDULED.to_string(),
                PUT_RENAME_QUORUM_FANOUT_STATE_WRITE_QUORUM.to_string(),
                PUT_RENAME_QUORUM_FANOUT_STATE_SUCCESS.to_string(),
                PUT_RENAME_QUORUM_FANOUT_STATE_ERROR.to_string(),
                PUT_RENAME_QUORUM_FANOUT_STATE_PANIC.to_string(),
            ])
        );
    }

    #[test]
    fn test_put_object_diagnostic_buckets() {
        assert_eq!(put_object_size_bucket(0), "unknown");
        assert_eq!(put_object_size_bucket(10 * 1024 * 1024), "le_10mib");
        assert_eq!(put_object_size_bucket(32 * 1024 * 1024), "le_32mib");
        assert_eq!(put_object_size_bucket(32 * 1024 * 1024 + 1), "le_64mib");
        assert_eq!(put_object_buffer_bucket(64 * 1024), "le_64kib");
        assert_eq!(put_object_buffer_bucket(256 * 1024), "le_256kib");
        assert_eq!(put_object_buffer_bucket(2 * 1024 * 1024), "gt_1mib");
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
    fn put_stage_gate_does_not_disable_basic_put_metrics() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_metrics_enabled(true);
            set_put_stage_metrics_enabled(false);
            record_put_object_request_start(1);
            record_put_object_request_result("ok", 0.001);
            record_put_object(1.0, 1024, false);
            record_put_object_stage_duration("disabled_stage", 0.5);

            set_put_stage_metrics_enabled(true);
            record_put_object_stage_duration("enabled_stage", 0.5);

            set_put_stage_metrics_enabled(false);
            set_metrics_enabled(false);
        });

        let metrics = snapshotter.snapshot().into_vec();
        assert!(metrics.iter().any(|(composite, _, _, _)| {
            composite.kind() == MetricKind::Counter && composite.key().name() == "rustfs_s3_put_object_total"
        }));
        assert!(metrics.iter().any(|(composite, _, _, _)| {
            composite.kind() == MetricKind::Counter && composite.key().name() == "rustfs_io_put_object_requests_total"
        }));

        let stages = metrics
            .iter()
            .filter(|(composite, _, _, _)| {
                composite.kind() == MetricKind::Histogram && composite.key().name() == "rustfs_s3_put_object_stage_duration_ms"
            })
            .flat_map(|(composite, _, _, _)| composite.key().labels().map(|label| label.value().to_string()))
            .collect::<Vec<_>>();
        assert_eq!(stages, ["enabled_stage"]);
    }

    #[test]
    fn test_put_stage_timer_follows_metrics_switch() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_put_stage_metrics_enabled(false);
        assert!(put_stage_timer().is_none());

        set_put_stage_metrics_enabled(true);
        assert!(put_stage_timer().is_some());
        set_put_stage_metrics_enabled(false);
    }

    #[test]
    fn test_record_get_object_path_and_stage() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_get_stage_metrics_enabled(true);
        record_get_object_stage_duration("s3_handler", "request_context", 0.001);
        record_get_object_stage_duration_by_size("legacy_duplex", "metadata", "plain_single_part", "le_4kib", 0.001);
        record_get_object_reader_path("codec_streaming");
        record_get_object_reader_path_by_size("codec_streaming", "plain_single_part", "le_1mib");
        record_get_object_codec_streaming_fallback("range");
        record_get_object_codec_streaming_decision_by_size("fallback", "plain_single_part", "below_min_size", "le_128kib");
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
        record_get_object_reconstruct_outcome("codec_streaming", "legacy", "legacy_called");
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
        record_get_object_stage_duration_by_size("legacy_duplex", "metadata", "plain_single_part", "le_4kib", 0.001);
        record_get_object_reader_path("codec_streaming");
        record_get_object_reader_path_by_size("codec_streaming", "plain_single_part", "le_1mib");
        record_get_object_codec_streaming_fallback("range");
        record_get_object_codec_streaming_decision_by_size("fallback", "plain_single_part", "below_min_size", "le_128kib");
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
        record_get_object_reconstruct_outcome("codec_streaming", "rustfs", "rustfs_called");
        record_get_object_emit_duration("codec_streaming", 0.007);
        record_get_object_first_byte_latency("s3_handler", 0.008);
        record_get_object_full_body_latency("s3_handler", 0.009);
        record_get_object_response_handoff_duration("s3_handler", 0.0001);
        record_get_object_metadata_phase_duration_with_early_stop(0.002, "hit");
        record_get_object_total_duration_with_path(0.050, "legacy_duplex");
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
    fn test_get_object_size_buckets_match_issue714_matrix() {
        assert_eq!(get_object_size_bucket(0), GET_OBJECT_SIZE_BUCKET_LE_4_KIB);
        assert_eq!(get_object_size_bucket(1024), GET_OBJECT_SIZE_BUCKET_LE_4_KIB);
        assert_eq!(get_object_size_bucket(4096), GET_OBJECT_SIZE_BUCKET_LE_4_KIB);
        assert_eq!(get_object_size_bucket(4097), GET_OBJECT_SIZE_BUCKET_LE_16_KIB);
        assert_eq!(get_object_size_bucket(10 * 1024), GET_OBJECT_SIZE_BUCKET_LE_16_KIB);
        assert_eq!(get_object_size_bucket(16 * 1024), GET_OBJECT_SIZE_BUCKET_LE_16_KIB);
        assert_eq!(get_object_size_bucket((16 * 1024) + 1), GET_OBJECT_SIZE_BUCKET_LE_64_KIB);
        assert_eq!(get_object_size_bucket(100 * 1024), GET_OBJECT_SIZE_BUCKET_LE_128_KIB);
        assert_eq!(get_object_size_bucket(128 * 1024), GET_OBJECT_SIZE_BUCKET_LE_128_KIB);
        assert_eq!(get_object_size_bucket((128 * 1024) + 1), GET_OBJECT_SIZE_BUCKET_LE_192_KIB);
        assert_eq!(get_object_size_bucket(192 * 1024), GET_OBJECT_SIZE_BUCKET_LE_192_KIB);
        assert_eq!(get_object_size_bucket((192 * 1024) + 1), GET_OBJECT_SIZE_BUCKET_LE_256_KIB);
        assert_eq!(get_object_size_bucket(256 * 1024), GET_OBJECT_SIZE_BUCKET_LE_256_KIB);
        assert_eq!(get_object_size_bucket((256 * 1024) + 1), GET_OBJECT_SIZE_BUCKET_LE_512_KIB);
        assert_eq!(get_object_size_bucket(512 * 1024), GET_OBJECT_SIZE_BUCKET_LE_512_KIB);
        assert_eq!(get_object_size_bucket((512 * 1024) + 1), GET_OBJECT_SIZE_BUCKET_LE_1_MIB);
        assert_eq!(get_object_size_bucket(1024 * 1024), GET_OBJECT_SIZE_BUCKET_LE_1_MIB);
        assert_eq!(get_object_size_bucket((1024 * 1024) + 1), GET_OBJECT_SIZE_BUCKET_GT_1_MIB);
    }

    #[test]
    fn test_record_stage_duration_generic() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Generic stage duration is gated by the common `metrics_enabled()` switch.
        set_metrics_enabled(true);
        record_stage_duration("metacache_walk_dir_primary", 15.0);
        record_stage_duration("store_list_objects_walk_internal", 8.5);
        record_stage_duration("lifecycle_free_version_recovery_failed", 120.0);
        assert!(metrics_enabled());
        set_metrics_enabled(false);
    }

    #[test]
    fn test_metrics_enabled_toggle_gates_common_recorders() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // Disabled: the common recorders must be no-ops (no panic, no recording).
        set_metrics_enabled(false);
        assert!(!metrics_enabled());
        record_stage_duration("metacache_walk_dir_primary", 15.0);
        record_list_objects(50.0, 100, false);
        record_error("get_object", "timeout");
        record_cpu_usage(25.5);
        record_get_object_streaming_body_failure(GetObjectStreamingBodyFailure {
            stage: "reader_stream",
            reason: "short_eof",
            error_class: "short_eof",
            strategy: "standard",
            buffer_source: "selected",
            size_bucket: GET_OBJECT_SIZE_BUCKET_GT_1_MIB,
            emitted_bytes: 1024,
            remaining_bytes: 512,
        });

        // Enabled: the same recorders run their emission bodies without panicking.
        set_metrics_enabled(true);
        assert!(metrics_enabled());
        record_stage_duration("metacache_walk_dir_primary", 15.0);
        record_list_objects(50.0, 100, false);
        record_error("get_object", "timeout");
        record_cpu_usage(25.5);
        record_get_object_streaming_body_failure(GetObjectStreamingBodyFailure {
            stage: "reader_stream",
            reason: "reader_error",
            error_class: "timeout",
            strategy: "standard",
            buffer_source: "selected",
            size_bucket: GET_OBJECT_SIZE_BUCKET_GT_1_MIB,
            emitted_bytes: 2048,
            remaining_bytes: 256,
        });

        set_metrics_enabled(false);
    }

    /// Replaces six smoke tests (`test_record_io_strategy`, `test_record_permit_wait`,
    /// `test_record_io_load_level`, `test_record_cache_size`, `test_record_bandwidth`,
    /// `test_record_data_transfer`) that called the scheduler, cache and bandwidth
    /// recorders and asserted nothing (rustfs/backlog#1836). The derived bandwidth
    /// value and the `all` tier fan-out are now pinned, not just the names.
    #[test]
    fn io_scheduler_and_bandwidth_helpers_emit_their_metrics() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_metrics_enabled(true);
            record_io_strategy("nvme", "sequential", 256 * 1024, 5);
            record_io_strategy("ssd", "random", 64 * 1024, 10);
            record_permit_wait(5.0);
            record_permit_wait(10.5);
            record_io_load_level("low", 2);
            record_io_load_level("high", 15);
            record_cache_size("l1", 50 * 1024 * 1024, 1000);
            record_bandwidth(100 * 1024 * 1024, "high");
            record_data_transfer(1024 * 1024, 100.0);
            record_data_transfer(2048, 50.0);
            set_metrics_enabled(false);
        });

        let rows = snapshotter.snapshot().into_vec();
        let names = emitted_names(&rows);
        for expected in [
            "rustfs_io_strategy_total",
            "rustfs_io_buffer_size_bytes",
            "rustfs_io_concurrent_requests",
            "rustfs_io_permit_wait_duration_ms",
            "rustfs_io_load_level",
            "rustfs_cache_size_bytes",
            "rustfs_cache_entries",
            "rustfs_bandwidth_current_bps",
            "rustfs_bandwidth_observed_bps",
            "rustfs_io_transfer_bytes_total",
            "rustfs_io_transfer_duration_ms",
            "rustfs_io_transfer_bandwidth_bps",
        ] {
            assert!(names.contains(expected), "{expected} must be emitted by its record helper");
        }

        assert_eq!(counter_total(&rows, "rustfs_io_strategy_total"), Some(2));
        assert_eq!(counter_total(&rows, "rustfs_io_load_level"), Some(2));
        assert_eq!(
            gauge_value(&rows, "rustfs_io_concurrent_requests"),
            Some(15.0),
            "the shared concurrency gauge must hold the last reported value"
        );
        assert_eq!(histogram_samples(&rows, "rustfs_io_permit_wait_duration_ms"), vec![5.0, 10.5]);
        assert_eq!(gauge_value(&rows, "rustfs_cache_entries"), Some(1000.0));
        assert_eq!(gauge_value(&rows, "rustfs_cache_size_bytes"), Some((50 * 1024 * 1024) as f64));
        assert_eq!(
            counter_total(&rows, "rustfs_io_transfer_bytes_total"),
            Some(1024 * 1024 + 2048),
            "transferred bytes must accumulate across calls"
        );
        assert_eq!(
            histogram_samples(&rows, "rustfs_io_transfer_bandwidth_bps"),
            vec![40960.0, 10_485_760.0],
            "bandwidth must be derived as bytes * 1000 / duration_ms"
        );

        let mut bandwidth_by_tier: Vec<(&str, f64)> = rows
            .iter()
            .filter(|(composite, _, _, _)| composite.key().name() == "rustfs_bandwidth_current_bps")
            .map(|(composite, _, _, value)| {
                let tier = composite
                    .key()
                    .labels()
                    .find(|label| label.key() == "tier")
                    .map(|label| label.value())
                    .expect("bandwidth gauges carry a tier label");
                match value {
                    DebugValue::Gauge(value) => (tier, value.0),
                    other => panic!("rustfs_bandwidth_current_bps holds {other:?}"),
                }
            })
            .collect();
        bandwidth_by_tier.sort_by(|left, right| left.0.cmp(right.0));
        assert_eq!(
            bandwidth_by_tier,
            vec![("all", 104_857_600.0), ("high", 104_857_600.0)],
            "record_bandwidth must publish both the aggregate `all` series and the caller tier"
        );
    }

    /// Replaces five smoke tests (`test_record_memory_usage`,
    /// `test_record_process_memory_split`, `test_record_cgroup_memory_split`,
    /// `test_record_cpu_usage`, `test_record_disk_io`) that called the system
    /// resource recorders and asserted nothing (rustfs/backlog#1836). The gauge
    /// values pin the argument order and the usage-percent derivation, which name
    /// checks alone cannot catch.
    #[test]
    fn system_resource_helpers_emit_their_metrics() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_metrics_enabled(true);
            record_memory_usage(1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024);
            record_process_memory_split(1024, 2048);
            record_cgroup_memory_split(Some(1), Some(2), Some(3), Some(4), Some(5), Some(6));
            record_cpu_usage(25.5);
            record_disk_io(1024 * 1024, 2048, 100, 50);
            record_disk_io(2048, 4096, 200, 100);
            set_metrics_enabled(false);
        });

        let rows = snapshotter.snapshot().into_vec();
        assert_eq!(gauge_value(&rows, "rustfs_memory_used_bytes"), Some((1024 * 1024 * 1024) as f64));
        assert_eq!(gauge_value(&rows, "rustfs_memory_total_bytes"), Some((4u64 * 1024 * 1024 * 1024) as f64));
        assert_eq!(
            gauge_value(&rows, "rustfs_memory_usage_percent"),
            Some(25.0),
            "usage percent must be used/total * 100"
        );
        assert_eq!(gauge_value(&rows, "rustfs_memory_process_resident_bytes"), Some(1024.0));
        assert_eq!(
            gauge_value(&rows, "rustfs_memory_process_virtual_bytes"),
            Some(2048.0),
            "resident and virtual bytes must not be swapped"
        );
        for (name, expected) in [
            ("rustfs_memory_cgroup_current_bytes", 1.0),
            ("rustfs_memory_cgroup_limit_bytes", 2.0),
            ("rustfs_memory_cgroup_anon_bytes", 3.0),
            ("rustfs_memory_cgroup_file_bytes", 4.0),
            ("rustfs_memory_cgroup_active_file_bytes", 5.0),
            ("rustfs_memory_cgroup_inactive_file_bytes", 6.0),
        ] {
            assert_eq!(gauge_value(&rows, name), Some(expected), "{name} must receive its own argument");
        }
        assert_eq!(gauge_value(&rows, "rustfs_cpu_usage_percent"), Some(25.5));
        assert_eq!(counter_total(&rows, "rustfs_disk_read_bytes_total"), Some(1024 * 1024 + 2048));
        assert_eq!(counter_total(&rows, "rustfs_disk_write_bytes_total"), Some(2048 + 4096));
        assert_eq!(counter_total(&rows, "rustfs_disk_read_ops_total"), Some(300));
        assert_eq!(
            counter_total(&rows, "rustfs_disk_write_ops_total"),
            Some(150),
            "byte and op counters must not be crossed"
        );
    }

    /// Boundary companion of the `Some(..)` case above: an absent cgroup field must
    /// emit no gauge at all. Publishing `0` for a field the kernel does not expose
    /// would read as a real measurement (rustfs/backlog#1836).
    #[test]
    fn cgroup_memory_split_skips_absent_fields() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_metrics_enabled(true);
            record_cgroup_memory_split(None, None, None, None, None, None);
            set_metrics_enabled(false);
        });

        let rows = snapshotter.snapshot().into_vec();
        assert!(rows.is_empty(), "absent cgroup fields must emit nothing, got {:?}", emitted_names(&rows));
    }

    #[test]
    fn test_ec_encode_inflight_bytes_tracking() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_put_stage_metrics_enabled(true);
        let queue_peak_before = current_ec_encode_queue_bytes_peak();
        EC_ENCODE_INFLIGHT_BYTES.store(0, Ordering::Relaxed);
        add_ec_encode_inflight_bytes(1024);
        add_ec_encode_inflight_bytes(2048);
        remove_ec_encode_inflight_bytes(1024);
        remove_ec_encode_inflight_bytes(2048);
        remove_ec_encode_inflight_bytes(4096);
        assert_eq!(current_ec_encode_inflight_bytes(), 0);
        assert!(
            current_ec_encode_queue_bytes_peak() >= queue_peak_before.max(3072),
            "queue peak must retain the largest observed queue occupancy"
        );
        set_put_stage_metrics_enabled(false);
    }

    #[test]
    fn test_ec_encode_producer_and_writer_stage_guards_aggregate_and_settle() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_put_stage_metrics_enabled(true);

        let producer_bytes = 1024;
        let producer_first = track_ec_encode_producer_bytes(producer_bytes);
        let producer_second = track_ec_encode_producer_bytes(producer_bytes);
        assert_eq!(current_ec_encode_producer_bytes(), 2048);
        assert!(
            current_ec_encode_producer_bytes_peak() >= 2048,
            "producer peak must include simultaneous stage ownership"
        );
        drop((producer_first, producer_second));
        assert_eq!(current_ec_encode_producer_bytes(), 0);

        let writer_bytes = 2048;
        let writer_first = track_ec_encode_writer_bytes(writer_bytes);
        let writer_second = track_ec_encode_writer_bytes(writer_bytes);
        assert_eq!(current_ec_encode_writer_bytes(), 4096);
        assert!(
            current_ec_encode_writer_bytes_peak() >= 4096,
            "writer peak must include simultaneous stage ownership"
        );
        drop((writer_first, writer_second));
        assert_eq!(current_ec_encode_writer_bytes(), 0);

        set_put_stage_metrics_enabled(false);
    }

    #[test]
    fn test_ec_encode_payload_stage_guards_are_noop_when_disabled() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_put_stage_metrics_enabled(false);

        let producer_current_before = current_ec_encode_producer_bytes();
        let producer_peak_before = current_ec_encode_producer_bytes_peak();
        let writer_current_before = current_ec_encode_writer_bytes();
        let writer_peak_before = current_ec_encode_writer_bytes_peak();

        let producer = track_ec_encode_producer_bytes(1024);
        let writer = track_ec_encode_writer_bytes(2048);
        assert_eq!(current_ec_encode_producer_bytes(), producer_current_before);
        assert_eq!(current_ec_encode_producer_bytes_peak(), producer_peak_before);
        assert_eq!(current_ec_encode_writer_bytes(), writer_current_before);
        assert_eq!(current_ec_encode_writer_bytes_peak(), writer_peak_before);

        drop((producer, writer));
        assert_eq!(current_ec_encode_producer_bytes(), producer_current_before);
        assert_eq!(current_ec_encode_producer_bytes_peak(), producer_peak_before);
        assert_eq!(current_ec_encode_writer_bytes(), writer_current_before);
        assert_eq!(current_ec_encode_writer_bytes_peak(), writer_peak_before);
    }

    fn assert_concurrent_ec_encode_stage_guards_aggregate_and_settle(
        track: fn(usize) -> EcEncodePayloadStageGuard,
        current: fn() -> u64,
        peak: fn() -> u64,
    ) {
        const WORKERS: usize = 4;
        const STAGE_BYTES: usize = 1536;

        let current_before = current();
        let peak_before = peak();
        let entered = Arc::new(Barrier::new(WORKERS + 1));
        let release = Arc::new(Barrier::new(WORKERS + 1));
        let mut workers = Vec::with_capacity(WORKERS);

        for _ in 0..WORKERS {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            workers.push(std::thread::spawn(move || {
                let stage = track(STAGE_BYTES);
                entered.wait();
                release.wait();
                drop(stage);
            }));
        }

        entered.wait();
        let expected_delta = u64::try_from(WORKERS).expect("worker count should fit in u64")
            * u64::try_from(STAGE_BYTES).expect("stage bytes should fit in u64");
        assert_eq!(current(), current_before + expected_delta);
        assert!(
            peak() >= peak_before.max(current_before + expected_delta),
            "stage peak must expose process-wide concurrent ownership"
        );

        release.wait();
        for worker in workers {
            worker.join().expect("stage worker should not panic");
        }
        assert_eq!(current(), current_before);
    }

    #[test]
    fn test_ec_encode_payload_stage_guards_track_concurrent_ownership() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_put_stage_metrics_enabled(true);

        assert_concurrent_ec_encode_stage_guards_aggregate_and_settle(
            track_ec_encode_producer_bytes,
            current_ec_encode_producer_bytes,
            current_ec_encode_producer_bytes_peak,
        );
        assert_concurrent_ec_encode_stage_guards_aggregate_and_settle(
            track_ec_encode_writer_bytes,
            current_ec_encode_writer_bytes,
            current_ec_encode_writer_bytes_peak,
        );

        set_put_stage_metrics_enabled(false);
    }

    #[test]
    fn test_ec_encode_producer_peak_exports_the_high_water_mark() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(current_ec_encode_producer_bytes(), 0, "test must start without producer stage ownership");
        let previous_peak = EC_ENCODE_PRODUCER_BYTES_PEAK.swap(0, Ordering::Relaxed);
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_put_stage_metrics_enabled(true);
            let first = track_ec_encode_producer_bytes(1024);
            let second = track_ec_encode_producer_bytes(2048);
            drop((first, second));
            set_put_stage_metrics_enabled(false);
        });

        let exported_peak = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(composite, _, _, value)| {
                (composite.kind() == MetricKind::Gauge && composite.key().name() == "rustfs_ec_encode_producer_bytes_peak")
                    .then_some(value)
            });
        assert!(
            matches!(exported_peak, Some(DebugValue::Gauge(value)) if value.0 == 3072.0),
            "exported producer peak must retain the aggregate high-water mark"
        );
        EC_ENCODE_PRODUCER_BYTES_PEAK.fetch_max(previous_peak, Ordering::Relaxed);
    }

    #[test]
    fn test_ec_encode_peak_publish_does_not_regress_after_out_of_order_cas() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let peak = Arc::new(AtomicU64::new(0));
        let exported = Arc::new(AtomicU64::new(0));
        let first_ready = Arc::new(Barrier::new(2));
        let release_first = Arc::new(Barrier::new(2));
        let first_peak = Arc::clone(&peak);
        let first_exported = Arc::clone(&exported);
        let first_ready_for_thread = Arc::clone(&first_ready);
        let release_first_for_thread = Arc::clone(&release_first);

        let first = std::thread::spawn(move || {
            publish_ec_encode_peak_with(
                &first_peak,
                10,
                || {
                    first_ready_for_thread.wait();
                    release_first_for_thread.wait();
                },
                |value| first_exported.store(value as u64, Ordering::Relaxed),
            );
        });
        first_ready.wait();
        publish_ec_encode_peak_with(&peak, 20, || {}, |value| exported.store(value as u64, Ordering::Relaxed));
        release_first.wait();
        first.join().expect("first publisher should not panic");

        assert_eq!(peak.load(Ordering::Relaxed), 20);
        assert_eq!(exported.load(Ordering::Relaxed), 20, "late publisher must reload the high-water mark");
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

    /// Replaces three smoke tests (`test_record_error`, `test_record_timeout`,
    /// `test_record_retry`) that called the failure recorders and asserted nothing
    /// (rustfs/backlog#1836). The histogram samples pin that the timeout duration
    /// and the retry attempt number reach their histogram rather than being folded
    /// into the counters.
    #[test]
    fn failure_helpers_emit_their_metrics() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_metrics_enabled(true);
            record_error("get_object", "timeout");
            record_error("put_object", "disk_error");
            record_timeout("get_object", 5000.0);
            record_timeout("list_objects", 10000.0);
            record_retry("get_object", 1);
            record_retry("put_object", 2);
            set_metrics_enabled(false);
        });

        let rows = snapshotter.snapshot().into_vec();
        let names = emitted_names(&rows);
        for expected in [
            "rustfs_errors_total",
            "rustfs_timeouts_total",
            "rustfs_timeouts_duration_ms",
            "rustfs_retries_total",
            "rustfs_retries_attempt",
        ] {
            assert!(names.contains(expected), "{expected} must be emitted by its record helper");
        }

        assert_eq!(
            counter_total(&rows, "rustfs_errors_total"),
            Some(2),
            "each error is counted once under its own operation/type labels"
        );
        assert_eq!(counter_total(&rows, "rustfs_timeouts_total"), Some(2));
        assert_eq!(counter_total(&rows, "rustfs_retries_total"), Some(2));
        assert_eq!(histogram_samples(&rows, "rustfs_timeouts_duration_ms"), vec![5000.0, 10000.0]);
        assert_eq!(
            histogram_samples(&rows, "rustfs_retries_attempt"),
            vec![1.0, 2.0],
            "the attempt number belongs in the histogram, not the retry counter"
        );

        let mut error_labels: Vec<(&str, &str)> = rows
            .iter()
            .filter(|(composite, _, _, _)| composite.key().name() == "rustfs_errors_total")
            .map(|(composite, _, _, _)| {
                let label = |key: &str| {
                    composite
                        .key()
                        .labels()
                        .find(|label| label.key() == key)
                        .map(|label| label.value())
                        .expect("error counters carry operation and type labels")
                };
                (label("operation"), label("type"))
            })
            .collect();
        error_labels.sort();
        assert_eq!(
            error_labels,
            vec![("get_object", "timeout"), ("put_object", "disk_error")],
            "operation and error type must not be swapped"
        );
    }
}

// ============================================================================
// Zero-Copy Optimization Metrics (Phase 1 Extension)
// ============================================================================

pub mod bandwidth;
pub mod global_metrics;
pub mod metric_names;

pub use metric_names::{aligned_pread, buffered_write, mmap_copy, zero_copy};

/// Record a zero-copy buffer operation.
///
/// This function records metrics for zero-copy buffer operations,
/// including the operation type and size.
#[inline(always)]
pub fn record_zero_copy_buffer_operation(operation: &str, size: usize) {
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
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
    if !metrics_enabled() {
        return;
    }
    counter!(zero_copy::BUFREADER_LAYERS_ELIMINATED_TOTAL).increment(layers_eliminated as u64);

    histogram!(zero_copy::BUFREADER_BUFFER_SIZE_BYTES).record(buffer_size as f64);
}

/// Record Direct I/O operation.
///
/// This function tracks Direct I/O operations and their success/fallback
/// status.
#[inline(always)]
pub fn record_direct_io_operation(operation: &str, size: usize, success: bool) {
    if !metrics_enabled() {
        return;
    }
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

    counter!(
        aligned_pread::OPERATIONS_TOTAL,
        "operation" => operation.to_string(),
        "status" => status.to_string()
    )
    .increment(1);

    counter!(
        aligned_pread::BYTES_TOTAL,
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
    if !metrics_enabled() {
        return;
    }
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
    use crate::tests::{METRICS_FLAG_LOCK, counter_total, emitted_names, gauge_value, histogram_samples};
    use metrics_util::debugging::DebuggingRecorder;

    /// Replaces six smoke tests (`test_record_zero_copy_buffer_operation`,
    /// `test_record_memory_copy`, `test_record_shared_ref_operation`,
    /// `test_record_bufreader_optimization`, `test_record_direct_io_operation`,
    /// `test_update_zero_copy_performance_metrics`) whose own comment admitted they
    /// only checked that the helpers compile and run (rustfs/backlog#1836). The same
    /// calls now run against a local recorder, and the assertions pin the counter
    /// split (operations vs bytes, copies vs copied bytes), the success/fallback
    /// label mapping, and the three same-typed performance gauges.
    #[test]
    fn zero_copy_helpers_emit_their_metrics() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            set_metrics_enabled(true);
            record_zero_copy_buffer_operation("read", 1024);
            record_zero_copy_buffer_operation("write", 2048);
            record_memory_copy(1, 1024);
            record_memory_copy(2, 2048);
            record_shared_ref_operation("create");
            record_shared_ref_operation("share");
            record_bufreader_optimization(1, 8192);
            record_bufreader_optimization(2, 65536);
            record_direct_io_operation("read", 4096, true);
            record_direct_io_operation("write", 8192, false);
            update_zero_copy_performance_metrics(2, 150.5, 1024 * 1024);
            set_metrics_enabled(false);
        });

        let rows = snapshotter.snapshot().into_vec();
        let names = emitted_names(&rows);
        for expected in [
            zero_copy::BUFFER_OPERATIONS_TOTAL,
            zero_copy::BUFFER_BYTES_TOTAL,
            zero_copy::MEMORY_COPY_TOTAL,
            zero_copy::MEMORY_COPY_BYTES_TOTAL,
            "rustfs_memory_copy_size_bytes",
            zero_copy::SHARED_REF_OPERATIONS_TOTAL,
            zero_copy::BUFREADER_LAYERS_ELIMINATED_TOTAL,
            zero_copy::BUFREADER_BUFFER_SIZE_BYTES,
            zero_copy::DIRECT_IO_OPERATIONS_TOTAL,
            zero_copy::DIRECT_IO_BYTES_TOTAL,
            aligned_pread::OPERATIONS_TOTAL,
            aligned_pread::BYTES_TOTAL,
            zero_copy::AVG_COPY_COUNT,
            zero_copy::THROUGHPUT_MBPS,
            zero_copy::MEMORY_SAVED_BYTES,
        ] {
            assert!(names.contains(expected), "{expected} must be emitted by its record helper");
        }

        assert_eq!(counter_total(&rows, zero_copy::BUFFER_OPERATIONS_TOTAL), Some(2));
        assert_eq!(
            counter_total(&rows, zero_copy::BUFFER_BYTES_TOTAL),
            Some(3072),
            "buffer bytes must accumulate the sizes, not the call count"
        );
        assert_eq!(
            counter_total(&rows, zero_copy::MEMORY_COPY_TOTAL),
            Some(3),
            "the copy counter takes the copy count argument"
        );
        assert_eq!(
            counter_total(&rows, zero_copy::MEMORY_COPY_BYTES_TOTAL),
            Some(3072),
            "the copied-bytes counter takes the size argument"
        );
        assert_eq!(histogram_samples(&rows, "rustfs_memory_copy_size_bytes"), vec![1024.0, 2048.0]);
        assert_eq!(counter_total(&rows, zero_copy::SHARED_REF_OPERATIONS_TOTAL), Some(2));
        assert_eq!(counter_total(&rows, zero_copy::BUFREADER_LAYERS_ELIMINATED_TOTAL), Some(3));
        assert_eq!(histogram_samples(&rows, zero_copy::BUFREADER_BUFFER_SIZE_BYTES), vec![8192.0, 65536.0]);
        assert_eq!(counter_total(&rows, zero_copy::DIRECT_IO_BYTES_TOTAL), Some(12288));
        assert_eq!(
            counter_total(&rows, aligned_pread::BYTES_TOTAL),
            Some(12288),
            "the aligned-pread series must mirror the direct-IO series"
        );
        assert_eq!(gauge_value(&rows, zero_copy::AVG_COPY_COUNT), Some(2.0));
        assert_eq!(gauge_value(&rows, zero_copy::THROUGHPUT_MBPS), Some(150.5));
        assert_eq!(
            gauge_value(&rows, zero_copy::MEMORY_SAVED_BYTES),
            Some((1024 * 1024) as f64),
            "the three performance gauges must not be filled from each other's argument"
        );

        let mut direct_io_labels: Vec<(&str, &str)> = rows
            .iter()
            .filter(|(composite, _, _, _)| composite.key().name() == zero_copy::DIRECT_IO_OPERATIONS_TOTAL)
            .map(|(composite, _, _, _)| {
                let label = |key: &str| {
                    composite
                        .key()
                        .labels()
                        .find(|label| label.key() == key)
                        .map(|label| label.value())
                        .expect("direct-IO counters carry operation and status labels")
                };
                (label("operation"), label("status"))
            })
            .collect();
        direct_io_labels.sort();
        assert_eq!(
            direct_io_labels,
            vec![("read", "success"), ("write", "fallback")],
            "the success flag must map to the success/fallback status label"
        );
    }

    #[test]
    fn test_metric_names() {
        // Verify metric names are defined
        assert!(!zero_copy::BUFFER_OPERATIONS_TOTAL.is_empty());
        assert!(!zero_copy::MEMORY_COPY_TOTAL.is_empty());
        assert!(!zero_copy::THROUGHPUT_MBPS.is_empty());
        assert!(!mmap_copy::READS_TOTAL.is_empty());
        assert!(!buffered_write::WRITES_TOTAL.is_empty());
        assert!(!aligned_pread::OPERATIONS_TOTAL.is_empty());
    }
}
