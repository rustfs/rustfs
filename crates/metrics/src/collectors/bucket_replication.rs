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

//! Bucket replication bandwidth metrics collector.

use crate::MetricType;
use crate::format::PrometheusMetric;
use std::borrow::Cow;

/// Bucket replication bandwidth stats for one replication target.
#[derive(Debug, Clone, Default)]
pub struct BucketReplicationBandwidthStats {
    pub bucket: String,
    pub target_arn: String,
    pub limit_bytes_per_sec: i64,
    pub current_bandwidth_bytes_per_sec: f64,
}

const BUCKET_LABEL: &str = "bucket";
const TARGET_ARN_LABEL: &str = "targetArn";

const METRIC_BANDWIDTH_LIMIT: &str = "rustfs_bucket_replication_bandwidth_limit_bytes_per_second";
const METRIC_BANDWIDTH_CURRENT: &str = "rustfs_bucket_replication_bandwidth_current_bytes_per_second";

const HELP_BANDWIDTH_LIMIT: &str = "Configured bandwidth limit for replication in bytes per second";
const HELP_BANDWIDTH_CURRENT: &str = "Current replication bandwidth in bytes per second (EWMA)";

/// Collect bucket replication bandwidth metrics for Prometheus/OpenTelemetry export.
#[must_use]
#[inline]
pub fn collect_bucket_replication_bandwidth_metrics(stats: &[BucketReplicationBandwidthStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(stats.len() * 2);
    for stat in stats {
        let bucket_label: Cow<'static, str> = Cow::Owned(stat.bucket.clone());
        let target_arn_label: Cow<'static, str> = Cow::Owned(stat.target_arn.clone());

        metrics.push(
            PrometheusMetric::new(
                METRIC_BANDWIDTH_LIMIT,
                MetricType::Gauge,
                HELP_BANDWIDTH_LIMIT,
                stat.limit_bytes_per_sec as f64,
            )
            .with_label(BUCKET_LABEL, bucket_label.clone())
            .with_label(TARGET_ARN_LABEL, target_arn_label.clone()),
        );

        metrics.push(
            PrometheusMetric::new(
                METRIC_BANDWIDTH_CURRENT,
                MetricType::Gauge,
                HELP_BANDWIDTH_CURRENT,
                stat.current_bandwidth_bytes_per_sec,
            )
            .with_label(BUCKET_LABEL, bucket_label)
            .with_label(TARGET_ARN_LABEL, target_arn_label),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_bucket_replication_bandwidth_metrics() {
        let stats = vec![BucketReplicationBandwidthStats {
            bucket: "b1".to_string(),
            target_arn: "arn:rustfs:replication:us-east-1:1:test-2".to_string(),
            limit_bytes_per_sec: 1_048_576,
            current_bandwidth_bytes_per_sec: 204_800.0,
        }];

        let metrics = collect_bucket_replication_bandwidth_metrics(&stats);
        assert_eq!(metrics.len(), 2);

        let limit_metric = metrics.iter().find(|m| m.name == METRIC_BANDWIDTH_LIMIT);
        assert!(limit_metric.is_some());
        assert_eq!(limit_metric.map(|m| m.value), Some(1_048_576.0));
        assert!(
            limit_metric
                .and_then(|m| {
                    m.labels
                        .iter()
                        .find(|(k, _)| *k == TARGET_ARN_LABEL)
                        .map(|(_, v)| v.as_ref() == "arn:rustfs:replication:us-east-1:1:test-2")
                })
                .unwrap_or(false)
        );

        let current_metric = metrics.iter().find(|m| m.name == METRIC_BANDWIDTH_CURRENT);
        assert!(current_metric.is_some());
        assert_eq!(current_metric.map(|m| m.value), Some(204_800.0));
    }

    #[test]
    fn test_collect_bucket_replication_bandwidth_metrics_empty() {
        let stats: Vec<BucketReplicationBandwidthStats> = Vec::new();
        let metrics = collect_bucket_replication_bandwidth_metrics(&stats);
        assert!(metrics.is_empty());
    }
}
