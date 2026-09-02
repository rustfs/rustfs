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

//! On-Demand Migration metric descriptors (rustfs/backlog#2157).
//!
//! Every series is bucket-scoped and mirrors one counter of the per-bucket
//! runtime snapshot (`OdmStatsSnapshot` in ecstore). The label value lists
//! below are the runtime's fixed label sets; they are what series retirement
//! enumerates when a bucket's config disappears, so they must stay in sync
//! with the runtime's golden JSON.

use crate::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// Bucket the series belongs to.
pub const BUCKET_L: &str = "bucket";
/// Request operation that entered ODM (`get` | `head`).
pub const OP_L: &str = "op";
/// How a request that entered ODM ended.
pub const OUTCOME_L: &str = "outcome";
/// Which pipeline stored a pulled object locally.
pub const PATH_L: &str = "path";
/// Why a pull did not produce a local object.
pub const REASON_L: &str = "reason";
/// Upper bound (seconds) of a source latency bucket.
pub const LE_L: &str = "le";

/// Fixed `op` label values.
pub const REQUEST_OPS: [&str; 2] = ["get", "head"];
/// Fixed `outcome` label values; `local_hit` is absent because locally
/// served requests never reach the runtime.
pub const REQUEST_OUTCOMES: [&str; 7] = [
    "source_hit",
    "source_miss",
    "source_error",
    "breaker_open",
    "negative_cached",
    "filtered",
    "unsupported",
];
/// Fixed `path` label values.
pub const PULL_PATHS: [&str; 3] = ["inline", "background", "backfill"];
/// Fixed `reason` label values.
pub const PULL_FAILURE_REASONS: [&str; 12] = [
    "source_not_found",
    "source_access_denied",
    "source_throttled",
    "source_timeout",
    "source_connect",
    "source_server_error",
    "source_unsupported",
    "source_other",
    "etag_mismatch",
    "local_write",
    "canceled",
    "queue_full",
];
/// Fixed `le` label values of the source latency distribution: the runtime's
/// 14 millisecond bounds rendered in seconds, plus the overflow bucket.
pub const SOURCE_LATENCY_LE: [&str; 15] = [
    "0.005", "0.01", "0.02", "0.05", "0.1", "0.2", "0.5", "1", "2", "5", "10", "20", "30", "60", "+Inf",
];

/// `breaker_state` gauge value: the breaker admits every request.
pub const BREAKER_STATE_CLOSED: f64 = 0.0;
/// `breaker_state` gauge value: the breaker admits a single probe.
pub const BREAKER_STATE_HALF_OPEN: f64 = 1.0;
/// `breaker_state` gauge value: the breaker rejects every request.
pub const BREAKER_STATE_OPEN: f64 = 2.0;

const REQUESTS_TOTAL: &str = "requests_total";
const PULLED_BYTES_TOTAL: &str = "pulled_bytes_total";
const PULLED_OBJECTS_TOTAL: &str = "pulled_objects_total";
const PULL_FAILURES_TOTAL: &str = "pull_failures_total";
const INFLIGHT_PULLS: &str = "inflight_pulls";
const QUEUE_DEPTH: &str = "queue_depth";
const SOURCE_LATENCY_SECONDS_DISTRIBUTION: &str = "source_latency_seconds_distribution";
const SOURCE_LATENCY_SECONDS_SUM: &str = "source_latency_seconds_sum";
const SOURCE_LATENCY_SECONDS_COUNT: &str = "source_latency_seconds_count";
const BREAKER_STATE: &str = "breaker_state";

pub static ODM_REQUESTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::from(REQUESTS_TOTAL),
        "Total number of requests that entered on-demand migration for a bucket by operation and outcome",
        &[BUCKET_L, OP_L, OUTCOME_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

pub static ODM_PULLED_BYTES_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::from(PULLED_BYTES_TOTAL),
        "Total number of bytes pulled from the on-demand migration source for a bucket",
        &[BUCKET_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

pub static ODM_PULLED_OBJECTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::from(PULLED_OBJECTS_TOTAL),
        "Total number of objects pulled from the on-demand migration source and stored locally for a bucket by pull path",
        &[BUCKET_L, PATH_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

pub static ODM_PULL_FAILURES_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::from(PULL_FAILURES_TOTAL),
        "Total number of on-demand migration pulls that did not produce a local object for a bucket by reason",
        &[BUCKET_L, REASON_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

pub static ODM_INFLIGHT_PULLS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::from(INFLIGHT_PULLS),
        "Current number of on-demand migration pulls holding a pull slot for a bucket",
        &[BUCKET_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

pub static ODM_QUEUE_DEPTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::from(QUEUE_DEPTH),
        "Current number of on-demand migration pulls waiting for a pull slot for a bucket",
        &[BUCKET_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

/// Source latency is exported in the cumulative `le` counter layout used by
/// the API TTFB distributions: the runtime only exposes pre-aggregated
/// bucket counts, which the recorder cannot replay as histogram samples.
pub static ODM_SOURCE_LATENCY_SECONDS_DISTRIBUTION_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::from(SOURCE_LATENCY_SECONDS_DISTRIBUTION),
        "Cumulative number of on-demand migration source calls for a bucket whose latency was at most le seconds",
        &[BUCKET_L, LE_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

pub static ODM_SOURCE_LATENCY_SECONDS_SUM_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::from(SOURCE_LATENCY_SECONDS_SUM),
        "Total latency in seconds of on-demand migration source calls for a bucket",
        &[BUCKET_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

pub static ODM_SOURCE_LATENCY_SECONDS_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::from(SOURCE_LATENCY_SECONDS_COUNT),
        "Total number of on-demand migration source calls observed for a bucket",
        &[BUCKET_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

pub static ODM_BREAKER_STATE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::from(BREAKER_STATE),
        "State of the on-demand migration source breaker for a bucket: 0 closed, 1 half-open, 2 open",
        &[BUCKET_L],
        subsystems::ON_DEMAND_MIGRATION,
    )
});

// backfill_* descriptors (`listed_total`, `pulled_total`, `skipped_total`,
// `failed_total`, `state`) are added here by ODM-12 (rustfs/backlog#2159).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricType;

    fn labels(descriptor: &MetricDescriptor) -> Vec<&str> {
        descriptor.variable_labels.iter().map(String::as_str).collect()
    }

    #[test]
    fn descriptors_use_the_on_demand_migration_prefix() {
        for (descriptor, suffix) in [
            (&*ODM_REQUESTS_TOTAL_MD, "requests_total"),
            (&*ODM_PULLED_BYTES_TOTAL_MD, "pulled_bytes_total"),
            (&*ODM_PULLED_OBJECTS_TOTAL_MD, "pulled_objects_total"),
            (&*ODM_PULL_FAILURES_TOTAL_MD, "pull_failures_total"),
            (&*ODM_INFLIGHT_PULLS_MD, "inflight_pulls"),
            (&*ODM_QUEUE_DEPTH_MD, "queue_depth"),
            (&*ODM_SOURCE_LATENCY_SECONDS_DISTRIBUTION_MD, "source_latency_seconds_distribution"),
            (&*ODM_SOURCE_LATENCY_SECONDS_SUM_MD, "source_latency_seconds_sum"),
            (&*ODM_SOURCE_LATENCY_SECONDS_COUNT_MD, "source_latency_seconds_count"),
            (&*ODM_BREAKER_STATE_MD, "breaker_state"),
        ] {
            assert_eq!(descriptor.get_full_metric_name(), format!("rustfs_on_demand_migration_{suffix}"));
            assert_eq!(descriptor.subsystem, subsystems::ON_DEMAND_MIGRATION);
            assert!(!descriptor.help.is_empty(), "{suffix} needs help text");
        }
    }

    #[test]
    fn counters_and_gauges_carry_the_documented_label_sets() {
        assert_eq!(ODM_REQUESTS_TOTAL_MD.metric_type, MetricType::Counter);
        assert_eq!(labels(&ODM_REQUESTS_TOTAL_MD), vec!["bucket", "op", "outcome"]);
        assert_eq!(
            ODM_REQUESTS_TOTAL_MD.help,
            "Total number of requests that entered on-demand migration for a bucket by operation and outcome"
        );

        assert_eq!(ODM_PULLED_BYTES_TOTAL_MD.metric_type, MetricType::Counter);
        assert_eq!(labels(&ODM_PULLED_BYTES_TOTAL_MD), vec!["bucket"]);

        assert_eq!(ODM_PULLED_OBJECTS_TOTAL_MD.metric_type, MetricType::Counter);
        assert_eq!(labels(&ODM_PULLED_OBJECTS_TOTAL_MD), vec!["bucket", "path"]);

        assert_eq!(ODM_PULL_FAILURES_TOTAL_MD.metric_type, MetricType::Counter);
        assert_eq!(labels(&ODM_PULL_FAILURES_TOTAL_MD), vec!["bucket", "reason"]);

        assert_eq!(ODM_INFLIGHT_PULLS_MD.metric_type, MetricType::Gauge);
        assert_eq!(labels(&ODM_INFLIGHT_PULLS_MD), vec!["bucket"]);

        assert_eq!(ODM_QUEUE_DEPTH_MD.metric_type, MetricType::Gauge);
        assert_eq!(labels(&ODM_QUEUE_DEPTH_MD), vec!["bucket"]);

        assert_eq!(ODM_BREAKER_STATE_MD.metric_type, MetricType::Gauge);
        assert_eq!(labels(&ODM_BREAKER_STATE_MD), vec!["bucket"]);
        assert_eq!(
            ODM_BREAKER_STATE_MD.help,
            "State of the on-demand migration source breaker for a bucket: 0 closed, 1 half-open, 2 open"
        );
    }

    #[test]
    fn source_latency_uses_the_counter_bucket_contract() {
        assert_eq!(ODM_SOURCE_LATENCY_SECONDS_DISTRIBUTION_MD.metric_type, MetricType::Counter);
        assert_eq!(labels(&ODM_SOURCE_LATENCY_SECONDS_DISTRIBUTION_MD), vec!["bucket", "le"]);
        assert_eq!(ODM_SOURCE_LATENCY_SECONDS_SUM_MD.metric_type, MetricType::Counter);
        assert_eq!(labels(&ODM_SOURCE_LATENCY_SECONDS_SUM_MD), vec!["bucket"]);
        assert_eq!(ODM_SOURCE_LATENCY_SECONDS_COUNT_MD.metric_type, MetricType::Counter);
        assert_eq!(labels(&ODM_SOURCE_LATENCY_SECONDS_COUNT_MD), vec!["bucket"]);
        assert_eq!(SOURCE_LATENCY_LE.len(), 15);
        assert_eq!(SOURCE_LATENCY_LE[14], "+Inf");
    }

    #[test]
    fn fixed_label_values_are_unique() {
        for values in [
            REQUEST_OPS.as_slice(),
            REQUEST_OUTCOMES.as_slice(),
            PULL_PATHS.as_slice(),
            PULL_FAILURE_REASONS.as_slice(),
            SOURCE_LATENCY_LE.as_slice(),
        ] {
            let unique: std::collections::BTreeSet<_> = values.iter().collect();
            assert_eq!(unique.len(), values.len(), "{values:?}");
        }
    }
}
