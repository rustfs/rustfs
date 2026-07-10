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

const METRIC_PLAN_TOTAL: &str = "rustfs_object_data_cache_plan_total";
const METRIC_LOOKUP_TOTAL: &str = "rustfs_object_data_cache_lookup_total";
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
            METRIC_PLAN_TOTAL,
            "Object data cache GET plan decisions by backend, mode, decision, reason, and size class. \
             Incremented exactly once per GET by the planning layer."
        );
        describe_counter!(
            METRIC_LOOKUP_TOTAL,
            "Object data cache lookup outcomes by backend, mode, result, and size class. \
             Incremented exactly once per GET by the lookup layer."
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
            "Object data cache invalidation attempts by backend, reason, and outcome \
             (removed when keys were dropped, noop when the identity was not cached)."
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

/// Records a GET plan decision. Emitted exactly once per GET by `plan_get`.
pub(crate) fn record_plan_decision(
    backend: &'static str,
    mode: ObjectDataCacheMode,
    decision: &'static str,
    reason: &'static str,
    size_bytes: u64,
) {
    counter!(
        METRIC_PLAN_TOTAL,
        "backend" => backend,
        "mode" => mode.as_metric_label(),
        "decision" => decision,
        "reason" => reason,
        "size_class" => size_class(size_bytes),
    )
    .increment(1);
}

/// Records a cache lookup outcome. Emitted exactly once per GET by `lookup_body`.
pub(crate) fn record_lookup_result(backend: &'static str, mode: ObjectDataCacheMode, result: &'static str, size_bytes: u64) {
    counter!(
        METRIC_LOOKUP_TOTAL,
        "backend" => backend,
        "mode" => mode.as_metric_label(),
        "result" => result,
        "size_class" => size_class(size_bytes),
    )
    .increment(1);
}

/// Records a fill outcome.
///
/// The count is always incremented so every outcome is observable. `size_bytes`
/// is only added to the bytes counter when it is non-zero, and the duration
/// histogram is only recorded when `duration_seconds` is `Some`. Callers pass
/// `0` bytes and `None` duration for outcomes that never submitted a body to the
/// backend (`skipped_by_mode`, `skipped_size_mismatch`) or that merely joined an
/// in-flight leader fill (`joined_inflight`), so those neither inflate
/// `fill_bytes_total` nor pollute the duration histogram with non-fill samples.
pub(crate) fn record_fill_result(
    backend: &'static str,
    mode: ObjectDataCacheMode,
    result: &'static str,
    size_bytes: u64,
    duration_seconds: Option<f64>,
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
    if let Some(duration_seconds) = duration_seconds {
        histogram!(
            METRIC_FILL_DURATION_SECONDS,
            "backend" => backend,
            "mode" => mode.as_metric_label(),
            "result" => result,
            "size_class" => size_class,
        )
        .record(duration_seconds);
    }
    if size_bytes > 0 {
        counter!(
            METRIC_FILL_BYTES_TOTAL,
            "backend" => backend,
            "mode" => mode.as_metric_label(),
            "result" => result,
            "size_class" => size_class,
        )
        .increment(size_bytes);
    }
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

pub(crate) fn record_invalidation(backend: &'static str, reason: &'static str, outcome: &'static str) {
    counter!(METRIC_INVALIDATIONS_TOTAL, "backend" => backend, "reason" => reason, "outcome" => outcome).increment(1);
}

#[cfg(test)]
mod tests {
    use super::{
        METRIC_FILL_BYTES_TOTAL, METRIC_FILL_DURATION_SECONDS, METRIC_FILLS_TOTAL, METRIC_LOOKUP_TOTAL, METRIC_PLAN_TOTAL,
        record_fill_result, record_lookup_result, record_plan_decision,
    };
    use crate::config::ObjectDataCacheMode;
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// Captures every metric emitted by `f` on a thread-local recorder, so the
    /// process-global registry (and its cross-test flakiness) is untouched.
    fn capture(f: impl FnOnce()) -> Vec<(MetricKind, String, DebugValue)> {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, f);
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(composite, _unit, _desc, value)| (composite.kind(), composite.key().name().to_string(), value))
            .collect()
    }

    fn counter_total(metrics: &[(MetricKind, String, DebugValue)], name: &str) -> Option<u64> {
        let mut found = false;
        let mut sum = 0u64;
        for (kind, metric_name, value) in metrics {
            if *kind == MetricKind::Counter && metric_name == name {
                found = true;
                if let DebugValue::Counter(v) = value {
                    sum += *v;
                }
            }
        }
        found.then_some(sum)
    }

    fn has_metric(metrics: &[(MetricKind, String, DebugValue)], kind: MetricKind, name: &str) -> bool {
        metrics.iter().any(|(k, n, _)| *k == kind && n == name)
    }

    #[test]
    fn plan_and_lookup_counters_are_separate_metrics() {
        // ODC-17: one GET produces exactly one plan increment and one lookup
        // increment, on two distinct counter names.
        let metrics = capture(|| {
            record_plan_decision("moka", ObjectDataCacheMode::HitOnly, "cacheable", "eligible", 1024);
            record_lookup_result("moka", ObjectDataCacheMode::HitOnly, "miss", 1024);
        });

        assert_eq!(counter_total(&metrics, METRIC_PLAN_TOTAL), Some(1));
        assert_eq!(counter_total(&metrics, METRIC_LOOKUP_TOTAL), Some(1));
    }

    #[test]
    fn inserted_fill_records_count_bytes_and_duration() {
        let metrics = capture(|| {
            record_fill_result("moka", ObjectDataCacheMode::FillBufferedOnly, "inserted", 100, Some(0.5));
        });

        assert_eq!(counter_total(&metrics, METRIC_FILLS_TOTAL), Some(1));
        assert_eq!(counter_total(&metrics, METRIC_FILL_BYTES_TOTAL), Some(100));
        assert!(has_metric(&metrics, MetricKind::Histogram, METRIC_FILL_DURATION_SECONDS));
    }

    #[test]
    fn joined_inflight_fill_counts_without_bytes_or_duration() {
        // ODC-18: a waiter that joined an in-flight fill records the outcome
        // count but neither fill bytes nor a (wait-time) duration sample.
        let metrics = capture(|| {
            record_fill_result("moka", ObjectDataCacheMode::FillBufferedOnly, "joined_inflight", 0, None);
        });

        assert_eq!(counter_total(&metrics, METRIC_FILLS_TOTAL), Some(1));
        assert_eq!(
            counter_total(&metrics, METRIC_FILL_BYTES_TOTAL),
            None,
            "waiter must not inflate fill bytes"
        );
        assert!(
            !has_metric(&metrics, MetricKind::Histogram, METRIC_FILL_DURATION_SECONDS),
            "waiter must not record a fill duration sample"
        );
    }

    #[test]
    fn skipped_before_backend_records_no_duration() {
        // ODC-18: outcomes that never reached the backend record a count only.
        for result in ["skipped_by_mode", "skipped_size_mismatch"] {
            let metrics = capture(|| {
                record_fill_result("moka", ObjectDataCacheMode::HitOnly, result, 0, None);
            });
            assert_eq!(counter_total(&metrics, METRIC_FILLS_TOTAL), Some(1));
            assert_eq!(counter_total(&metrics, METRIC_FILL_BYTES_TOTAL), None);
            assert!(!has_metric(&metrics, MetricKind::Histogram, METRIC_FILL_DURATION_SECONDS));
        }
    }
}
