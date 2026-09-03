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

//! On-Demand Migration metrics collector (rustfs/backlog#2157).
//!
//! [`OnDemandMigrationBucketStats`] is the obs-side projection of one
//! bucket's runtime snapshot; the storage boundary fills it and this module
//! turns it into Prometheus series. Label values come from the snapshot
//! itself, so the collector never invents a label the runtime did not count.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::on_demand_migration::{
    BREAKER_STATE_CLOSED, BREAKER_STATE_HALF_OPEN, BREAKER_STATE_OPEN, BUCKET_L, LE_L, ODM_BACKFILL_BYTES_MD,
    ODM_BACKFILL_ENQUEUED_MD, ODM_BACKFILL_FAILED_MD, ODM_BACKFILL_JOBS_MD, ODM_BACKFILL_LISTED_MD, ODM_BACKFILL_PULLED_MD,
    ODM_BACKFILL_SKIPPED_EXISTING_MD, ODM_BREAKER_STATE_MD, ODM_INFLIGHT_PULLS_MD, ODM_PULL_FAILURES_TOTAL_MD,
    ODM_PULLED_BYTES_TOTAL_MD, ODM_PULLED_OBJECTS_TOTAL_MD, ODM_QUEUE_DEPTH_MD, ODM_REQUESTS_TOTAL_MD,
    ODM_SOURCE_LATENCY_SECONDS_COUNT_MD, ODM_SOURCE_LATENCY_SECONDS_DISTRIBUTION_MD, ODM_SOURCE_LATENCY_SECONDS_SUM_MD, OP_L,
    OUTCOME_L, PATH_L, REASON_L, SERVER_L, STATE_L,
};
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Bucket-scoped series that do not depend on a label set:
/// `pulled_bytes_total`, `inflight_pulls`, `queue_depth`, `breaker_state`,
/// `source_latency_seconds_sum`, `source_latency_seconds_count`.
const FIXED_METRICS_PER_BUCKET: usize = 6;

/// Breaker state as the runtime reports it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OnDemandMigrationBreakerState {
    #[default]
    Closed,
    HalfOpen,
    Open,
}

impl OnDemandMigrationBreakerState {
    /// The `breaker_state` gauge value.
    pub fn gauge_value(self) -> f64 {
        match self {
            Self::Closed => BREAKER_STATE_CLOSED,
            Self::HalfOpen => BREAKER_STATE_HALF_OPEN,
            Self::Open => BREAKER_STATE_OPEN,
        }
    }
}

/// One bucket of the runtime snapshot. Maps are keyed by the runtime's
/// label values (`op -> outcome -> count`, `path -> count`,
/// `reason -> count`); `BTreeMap` keeps the emitted series order stable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OnDemandMigrationBucketStats {
    pub bucket: String,
    pub requests_total: BTreeMap<String, BTreeMap<String, u64>>,
    pub pulled_bytes_total: u64,
    pub pulled_objects_total: BTreeMap<String, u64>,
    pub pull_failures_total: BTreeMap<String, u64>,
    pub inflight_pulls: u64,
    pub queue_depth: u64,
    /// `(upper bound in milliseconds, cumulative observations at or below it)`,
    /// ascending; observations above the last bound are only in `count`.
    pub source_latency_buckets: Vec<(u64, u64)>,
    pub source_latency_count: u64,
    pub source_latency_sum_ms: u64,
    pub breaker_state: OnDemandMigrationBreakerState,
}

/// Renders a millisecond bound as the `le` label in seconds (`5` -> `0.005`,
/// `1000` -> `1`).
pub fn source_latency_le_label(le_ms: u64) -> String {
    let secs = le_ms as f64 / 1000.0;
    if secs.fract() == 0.0 {
        format!("{}", secs as u64)
    } else {
        format!("{secs}")
    }
}

pub fn collect_on_demand_migration_metrics(stats: &[OnDemandMigrationBucketStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let metric_count = stats
        .iter()
        .map(|stat| {
            FIXED_METRICS_PER_BUCKET
                + stat.requests_total.values().map(BTreeMap::len).sum::<usize>()
                + stat.pulled_objects_total.len()
                + stat.pull_failures_total.len()
                + stat.source_latency_buckets.len()
                + 1
        })
        .sum();
    let mut metrics = Vec::with_capacity(metric_count);
    for stat in stats {
        let bucket_label: Cow<'static, str> = Cow::Owned(stat.bucket.clone());

        for (op, by_outcome) in &stat.requests_total {
            for (outcome, count) in by_outcome {
                metrics.push(
                    PrometheusMetric::from_descriptor(&ODM_REQUESTS_TOTAL_MD, *count as f64)
                        .with_label(BUCKET_L, bucket_label.clone())
                        .with_label_owned(OP_L, op.clone())
                        .with_label_owned(OUTCOME_L, outcome.clone()),
                );
            }
        }
        metrics.push(
            PrometheusMetric::from_descriptor(&ODM_PULLED_BYTES_TOTAL_MD, stat.pulled_bytes_total as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        for (path, count) in &stat.pulled_objects_total {
            metrics.push(
                PrometheusMetric::from_descriptor(&ODM_PULLED_OBJECTS_TOTAL_MD, *count as f64)
                    .with_label(BUCKET_L, bucket_label.clone())
                    .with_label_owned(PATH_L, path.clone()),
            );
        }
        for (reason, count) in &stat.pull_failures_total {
            metrics.push(
                PrometheusMetric::from_descriptor(&ODM_PULL_FAILURES_TOTAL_MD, *count as f64)
                    .with_label(BUCKET_L, bucket_label.clone())
                    .with_label_owned(REASON_L, reason.clone()),
            );
        }
        metrics.push(
            PrometheusMetric::from_descriptor(&ODM_INFLIGHT_PULLS_MD, stat.inflight_pulls as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ODM_QUEUE_DEPTH_MD, stat.queue_depth as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        for (le_ms, cumulative) in &stat.source_latency_buckets {
            metrics.push(
                PrometheusMetric::from_descriptor(&ODM_SOURCE_LATENCY_SECONDS_DISTRIBUTION_MD, *cumulative as f64)
                    .with_label(BUCKET_L, bucket_label.clone())
                    .with_label_owned(LE_L, source_latency_le_label(*le_ms)),
            );
        }
        metrics.push(
            PrometheusMetric::from_descriptor(&ODM_SOURCE_LATENCY_SECONDS_DISTRIBUTION_MD, stat.source_latency_count as f64)
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(LE_L, Cow::Borrowed("+Inf")),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ODM_SOURCE_LATENCY_SECONDS_SUM_MD, stat.source_latency_sum_ms as f64 / 1000.0)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ODM_SOURCE_LATENCY_SECONDS_COUNT_MD, stat.source_latency_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&ODM_BREAKER_STATE_MD, stat.breaker_state.gauge_value())
                .with_label(BUCKET_L, bucket_label),
        );
    }

    metrics
}

/// Counters of one bucket's latest backfill job (ODM-12,
/// rustfs/backlog#2159).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OdmBackfillBucketStats {
    pub bucket: String,
    /// Checkpoint state label (`running`, `completed`, ...).
    pub state: String,
    pub listed: u64,
    pub enqueued: u64,
    pub pulled: u64,
    pub skipped_existing: u64,
    pub failed: u64,
    pub bytes: u64,
}

/// Backfill stats of every bucket with a checkpoint, labelled by node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OdmBackfillRuntimeStats {
    pub server: String,
    pub buckets: Vec<OdmBackfillBucketStats>,
}

/// Seven series per bucket: the state gauge and six counters.
pub fn collect_on_demand_migration_backfill_metrics(stats: &OdmBackfillRuntimeStats) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(stats.buckets.len() * 7);
    for bucket in &stats.buckets {
        let labelled = |descriptor: &'static std::sync::LazyLock<crate::MetricDescriptor>, value: u64| {
            PrometheusMetric::from_descriptor(descriptor, value as f64)
                .with_label_owned(SERVER_L, stats.server.clone())
                .with_label_owned(BUCKET_L, bucket.bucket.clone())
        };
        metrics.push(labelled(&ODM_BACKFILL_JOBS_MD, 1).with_label_owned(STATE_L, bucket.state.clone()));
        metrics.push(labelled(&ODM_BACKFILL_LISTED_MD, bucket.listed));
        metrics.push(labelled(&ODM_BACKFILL_ENQUEUED_MD, bucket.enqueued));
        metrics.push(labelled(&ODM_BACKFILL_PULLED_MD, bucket.pulled));
        metrics.push(labelled(&ODM_BACKFILL_SKIPPED_EXISTING_MD, bucket.skipped_existing));
        metrics.push(labelled(&ODM_BACKFILL_FAILED_MD, bucket.failed));
        metrics.push(labelled(&ODM_BACKFILL_BYTES_MD, bucket.bytes));
    }
    metrics
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::metrics::schema::on_demand_migration::{PULL_FAILURE_REASONS, PULL_PATHS, REQUEST_OPS, REQUEST_OUTCOMES};

    /// The bucket the runtime golden snapshot describes (ecstore
    /// `snapshot_matches_golden_json`): two GET source hits, one negative
    /// cached HEAD, one inline pull of 4096 bytes, one timeout failure, three
    /// source calls of 3 ms, 750 ms and 90 s, one inflight and one queued pull.
    pub(crate) fn golden_stats(bucket: &str) -> OnDemandMigrationBucketStats {
        let mut requests_total = BTreeMap::new();
        for op in REQUEST_OPS {
            let by_outcome: BTreeMap<String, u64> = REQUEST_OUTCOMES
                .iter()
                .map(|outcome| {
                    let count = match (op, *outcome) {
                        ("get", "source_hit") => 2,
                        ("head", "negative_cached") => 1,
                        _ => 0,
                    };
                    (outcome.to_string(), count)
                })
                .collect();
            requests_total.insert(op.to_string(), by_outcome);
        }
        let pulled_objects_total = PULL_PATHS
            .iter()
            .map(|path| (path.to_string(), u64::from(*path == "inline")))
            .collect();
        let pull_failures_total = PULL_FAILURE_REASONS
            .iter()
            .map(|reason| (reason.to_string(), u64::from(*reason == "source_timeout")))
            .collect();
        let bounds_ms = [
            5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 30_000, 60_000,
        ];
        let source_latency_buckets = bounds_ms
            .iter()
            .map(|bound| (*bound, if *bound < 1_000 { 1 } else { 2 }))
            .collect();
        OnDemandMigrationBucketStats {
            bucket: bucket.to_string(),
            requests_total,
            pulled_bytes_total: 4096,
            pulled_objects_total,
            pull_failures_total,
            inflight_pulls: 1,
            queue_depth: 1,
            source_latency_buckets,
            source_latency_count: 3,
            source_latency_sum_ms: 90_753,
            breaker_state: OnDemandMigrationBreakerState::HalfOpen,
        }
    }

    /// `(name, labels, value)` of one emitted series.
    type Rendered = (String, Vec<(String, String)>, f64);

    /// Emitted series in emission order.
    fn rendered(metrics: &[PrometheusMetric]) -> Vec<Rendered> {
        metrics
            .iter()
            .map(|metric| {
                (
                    metric.name.to_string(),
                    metric
                        .labels
                        .iter()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect(),
                    metric.value,
                )
            })
            .collect()
    }

    fn series(name: &str, labels: &[(&str, &str)], value: f64) -> Rendered {
        (
            format!("rustfs_on_demand_migration_{name}"),
            labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            value,
        )
    }

    #[test]
    fn golden_snapshot_renders_every_series_with_fixed_labels() {
        let metrics = collect_on_demand_migration_metrics(&[golden_stats("photos")]);
        let actual = rendered(&metrics);

        let mut expected = Vec::new();
        for op in REQUEST_OPS {
            for outcome in REQUEST_OUTCOMES {
                let value = match (op, outcome) {
                    ("get", "source_hit") => 2.0,
                    ("head", "negative_cached") => 1.0,
                    _ => 0.0,
                };
                expected.push(series("requests_total", &[("bucket", "photos"), ("op", op), ("outcome", outcome)], value));
            }
        }
        // BTreeMap order: the golden fixture's outcomes sort alphabetically.
        expected.sort_by(|a, b| a.1.cmp(&b.1));
        expected.push(series("pulled_bytes_total", &[("bucket", "photos")], 4096.0));
        for path in ["backfill", "background", "inline"] {
            expected.push(series(
                "pulled_objects_total",
                &[("bucket", "photos"), ("path", path)],
                f64::from(u8::from(path == "inline")),
            ));
        }
        let mut reasons = PULL_FAILURE_REASONS;
        reasons.sort_unstable();
        for reason in reasons {
            expected.push(series(
                "pull_failures_total",
                &[("bucket", "photos"), ("reason", reason)],
                f64::from(u8::from(reason == "source_timeout")),
            ));
        }
        expected.push(series("inflight_pulls", &[("bucket", "photos")], 1.0));
        expected.push(series("queue_depth", &[("bucket", "photos")], 1.0));
        for (le, value) in [
            ("0.005", 1.0),
            ("0.01", 1.0),
            ("0.02", 1.0),
            ("0.05", 1.0),
            ("0.1", 1.0),
            ("0.2", 1.0),
            ("0.5", 1.0),
            ("1", 2.0),
            ("2", 2.0),
            ("5", 2.0),
            ("10", 2.0),
            ("20", 2.0),
            ("30", 2.0),
            ("60", 2.0),
            ("+Inf", 3.0),
        ] {
            expected.push(series("source_latency_seconds_distribution", &[("bucket", "photos"), ("le", le)], value));
        }
        expected.push(series("source_latency_seconds_sum", &[("bucket", "photos")], 90.753));
        expected.push(series("source_latency_seconds_count", &[("bucket", "photos")], 3.0));
        expected.push(series("breaker_state", &[("bucket", "photos")], 1.0));

        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(&expected) {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn breaker_state_gauge_encodes_the_three_states() {
        for (state, value) in [
            (OnDemandMigrationBreakerState::Closed, 0.0),
            (OnDemandMigrationBreakerState::HalfOpen, 1.0),
            (OnDemandMigrationBreakerState::Open, 2.0),
        ] {
            let stats = OnDemandMigrationBucketStats {
                bucket: "b".to_string(),
                breaker_state: state,
                ..Default::default()
            };
            let metrics = collect_on_demand_migration_metrics(&[stats]);
            let breaker = metrics
                .iter()
                .find(|metric| metric.name == ODM_BREAKER_STATE_MD.get_full_metric_name())
                .expect("breaker gauge");
            assert_eq!(breaker.value, value);
        }
    }

    #[test]
    fn le_labels_render_bounds_in_seconds() {
        assert_eq!(source_latency_le_label(5), "0.005");
        assert_eq!(source_latency_le_label(20), "0.02");
        assert_eq!(source_latency_le_label(500), "0.5");
        assert_eq!(source_latency_le_label(1_000), "1");
        assert_eq!(source_latency_le_label(60_000), "60");
    }

    #[test]
    fn empty_snapshot_emits_nothing() {
        assert!(collect_on_demand_migration_metrics(&[]).is_empty());
    }

    /// One running and one completed job, as the scheduler would see them
    /// from the local backfill runner.
    pub(crate) fn backfill_golden_stats(server: &str) -> OdmBackfillRuntimeStats {
        OdmBackfillRuntimeStats {
            server: server.to_string(),
            buckets: vec![
                OdmBackfillBucketStats {
                    bucket: "photos".to_string(),
                    state: "running".to_string(),
                    listed: 2000,
                    enqueued: 1500,
                    pulled: 1400,
                    skipped_existing: 500,
                    failed: 3,
                    bytes: 73_400_320,
                },
                OdmBackfillBucketStats {
                    bucket: "docs".to_string(),
                    state: "completed".to_string(),
                    ..Default::default()
                },
            ],
        }
    }

    fn backfill_series<'a>(metrics: &'a [PrometheusMetric], name: &str, bucket: &str) -> Option<&'a PrometheusMetric> {
        metrics.iter().find(|metric| {
            metric.name == name
                && metric
                    .labels
                    .iter()
                    .any(|(label, value)| *label == BUCKET_L && value.as_ref() == bucket)
        })
    }

    #[test]
    fn backfill_collects_seven_series_per_bucket_with_the_odm_subsystem_prefix() {
        let stats = backfill_golden_stats("node1:9000");
        let metrics = collect_on_demand_migration_backfill_metrics(&stats);
        assert_eq!(metrics.len(), 14);
        assert!(
            metrics
                .iter()
                .all(|metric| metric.name.starts_with("rustfs_on_demand_migration_backfill_"))
        );
        assert!(metrics.iter().all(|metric| {
            metric
                .labels
                .iter()
                .any(|(label, value)| *label == SERVER_L && value.as_ref() == "node1:9000")
        }));

        let jobs = backfill_series(&metrics, &ODM_BACKFILL_JOBS_MD.get_full_metric_name(), "photos").expect("jobs gauge");
        assert_eq!(jobs.value, 1.0);
        assert!(
            jobs.labels
                .iter()
                .any(|(label, value)| *label == STATE_L && value.as_ref() == "running")
        );
        let listed = backfill_series(&metrics, &ODM_BACKFILL_LISTED_MD.get_full_metric_name(), "photos").expect("listed");
        assert_eq!(listed.value, 2000.0);
        let bytes = backfill_series(&metrics, &ODM_BACKFILL_BYTES_MD.get_full_metric_name(), "photos").expect("bytes");
        assert_eq!(bytes.value, 73_400_320.0);
        let docs_failed = backfill_series(&metrics, &ODM_BACKFILL_FAILED_MD.get_full_metric_name(), "docs").expect("docs failed");
        assert_eq!(docs_failed.value, 0.0);
        assert_eq!(
            ODM_BACKFILL_LISTED_MD.get_full_metric_name(),
            "rustfs_on_demand_migration_backfill_listed_total"
        );
    }

    #[test]
    fn backfill_no_buckets_means_no_series() {
        let metrics = collect_on_demand_migration_backfill_metrics(&OdmBackfillRuntimeStats::default());
        assert!(metrics.is_empty());
    }
}
