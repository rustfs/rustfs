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

use crate::config::ObjectDataCacheMode;
use crate::stats::ObjectDataCacheStats;
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::sync::{Arc, OnceLock};

const METRIC_REQUESTS_TOTAL: &str = "rustfs_object_data_cache_requests_total";
const METRIC_FILLS_TOTAL: &str = "rustfs_object_data_cache_fill_total";
const METRIC_FILL_DURATION_SECONDS: &str = "rustfs_object_data_cache_fill_duration_seconds";
const METRIC_FILL_BYTES_TOTAL: &str = "rustfs_object_data_cache_fill_bytes_total";
const METRIC_HIT_BYTES_TOTAL: &str = "rustfs_object_data_cache_hit_bytes_total";
const METRIC_ENTRIES: &str = "rustfs_object_data_cache_entries";
const METRIC_WEIGHTED_BYTES: &str = "rustfs_object_data_cache_weighted_bytes";
const METRIC_INFLIGHT_FILLS: &str = "rustfs_object_data_cache_inflight_fills";
const METRIC_INVALIDATIONS_TOTAL: &str = "rustfs_object_data_cache_invalidations_total";
const METRIC_MEMORY_PRESSURE_TOTAL: &str = "rustfs_object_data_cache_memory_pressure_total";

pub(crate) fn describe_metrics_once() {
    static DESCRIBED: OnceLock<()> = OnceLock::new();
    let _ = DESCRIBED.get_or_init(|| {
        describe_counter!(
            METRIC_REQUESTS_TOTAL,
            "Object data cache request decisions labeled by backend, mode, decision, reason, and size class."
        );
        describe_counter!(
            METRIC_FILLS_TOTAL,
            "Object data cache fill outcomes by backend, mode, result, and size class."
        );
        describe_histogram!(METRIC_FILL_DURATION_SECONDS, "Object data cache fill duration in seconds.");
        describe_counter!(METRIC_FILL_BYTES_TOTAL, "Total bytes submitted to object data cache fill operations.");
        describe_counter!(METRIC_HIT_BYTES_TOTAL, "Total bytes served from object data cache hits.");
        describe_gauge!(METRIC_ENTRIES, "Current object data cache entry count.");
        describe_gauge!(METRIC_WEIGHTED_BYTES, "Approximate weighted bytes held by the object data cache.");
        describe_gauge!(METRIC_INFLIGHT_FILLS, "Current number of in-flight object data cache fills.");
        describe_counter!(
            METRIC_INVALIDATIONS_TOTAL,
            "Object data cache invalidation attempts by backend and reason."
        );
        describe_counter!(METRIC_MEMORY_PRESSURE_TOTAL, "Object data cache fill skips caused by memory pressure.");
    });
}

pub(crate) const fn size_class(size_bytes: u64) -> &'static str {
    if size_bytes <= 4 * 1024 {
        "le_4k"
    } else if size_bytes <= 64 * 1024 {
        "le_64k"
    } else if size_bytes <= 256 * 1024 {
        "le_256k"
    } else if size_bytes <= 1024 * 1024 {
        "le_1m"
    } else if size_bytes <= 4 * 1024 * 1024 {
        "le_4m"
    } else {
        "gt_4m"
    }
}

pub(crate) fn record_request_decision(
    backend: &'static str,
    mode: ObjectDataCacheMode,
    decision: &'static str,
    reason: &'static str,
    size_bytes: u64,
) {
    counter!(
        METRIC_REQUESTS_TOTAL,
        "backend" => backend,
        "mode" => mode.as_metric_label(),
        "decision" => decision,
        "reason" => reason,
        "size_class" => size_class(size_bytes),
    )
    .increment(1);
}

pub(crate) fn record_fill_result(
    backend: &'static str,
    mode: ObjectDataCacheMode,
    result: &'static str,
    size_bytes: u64,
    duration_seconds: f64,
) {
    let size_class = size_class(size_bytes);
    counter!(
        METRIC_FILLS_TOTAL,
        "backend" => backend,
        "mode" => mode.as_metric_label(),
        "result" => result,
        "size_class" => size_class,
    )
    .increment(1);
    histogram!(
        METRIC_FILL_DURATION_SECONDS,
        "backend" => backend,
        "mode" => mode.as_metric_label(),
        "result" => result,
        "size_class" => size_class,
    )
    .record(duration_seconds);
    counter!(
        METRIC_FILL_BYTES_TOTAL,
        "backend" => backend,
        "mode" => mode.as_metric_label(),
        "result" => result,
        "size_class" => size_class,
    )
    .increment(size_bytes);
}

pub(crate) fn record_hit_bytes(backend: &'static str, mode: ObjectDataCacheMode, size_bytes: u64) {
    counter!(
        METRIC_HIT_BYTES_TOTAL,
        "backend" => backend,
        "mode" => mode.as_metric_label(),
        "size_class" => size_class(size_bytes),
    )
    .increment(size_bytes);
}

pub(crate) fn publish_cache_state(backend: &'static str, entries: u64, weighted_bytes: u64) {
    gauge!(METRIC_ENTRIES, "backend" => backend).set(entries as f64);
    gauge!(METRIC_WEIGHTED_BYTES, "backend" => backend).set(weighted_bytes as f64);
}

pub(crate) fn set_inflight_fills(stats: &Arc<ObjectDataCacheStats>, backend: &'static str, count: usize) {
    stats.set_inflight_fills(count);
    let count_u64 = u64::try_from(count).unwrap_or(u64::MAX);
    gauge!(METRIC_INFLIGHT_FILLS, "backend" => backend).set(count_u64 as f64);
}

pub(crate) fn record_singleflight_join(stats: &Arc<ObjectDataCacheStats>) {
    stats.record_singleflight_join();
}

pub(crate) fn record_memory_pressure(stats: &Arc<ObjectDataCacheStats>, backend: &'static str) {
    stats.record_memory_pressure();
    counter!(METRIC_MEMORY_PRESSURE_TOTAL, "backend" => backend).increment(1);
}

pub(crate) fn record_invalidation(backend: &'static str, reason: &'static str) {
    counter!(METRIC_INVALIDATIONS_TOTAL, "backend" => backend, "reason" => reason).increment(1);
}
