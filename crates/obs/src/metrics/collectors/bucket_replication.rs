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
//!
//! Collects bandwidth metrics for bucket replication targets.
//!
//! This collector reuses the metric descriptors defined in `metrics_type::bucket_replication`
//! to avoid duplication of metric names, types, and help text.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::bucket_replication::{BUCKET_REPL_BANDWIDTH_CURRENT_MD, BUCKET_REPL_BANDWIDTH_LIMIT_MD};
use std::borrow::Cow;

/// Bucket replication bandwidth statistics for metrics collection.
#[derive(Debug, Clone, Default)]
pub struct BucketReplicationBandwidthStats {
    /// Name of the bucket
    pub bucket: String,
    /// Target ARN for replication
    pub target_arn: String,
    /// Configured bandwidth limit in bytes per second
    pub limit_bytes_per_sec: u64,
    /// Current bandwidth in bytes per second (EWMA)
    pub current_bandwidth_bytes_per_sec: f64,
}

/// Collects bucket replication bandwidth metrics from the provided statistics.
///
/// Uses the metric descriptors from `metrics_type::bucket_replication` module.
/// Returns a vector of Prometheus metrics for replication bandwidth.
pub fn collect_bucket_replication_bandwidth_metrics(stats: &[BucketReplicationBandwidthStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(stats.len() * 2);
    for stat in stats {
        let bucket_label: Cow<'static, str> = Cow::Owned(stat.bucket.clone());
        let target_arn_label: Cow<'static, str> = Cow::Owned(stat.target_arn.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_BANDWIDTH_LIMIT_MD, stat.limit_bytes_per_sec as f64)
                .with_label("bucket", bucket_label.clone())
                .with_label("targetArn", target_arn_label.clone()),
        );

        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_BANDWIDTH_CURRENT_MD, stat.current_bandwidth_bytes_per_sec)
                .with_label("bucket", bucket_label)
                .with_label("targetArn", target_arn_label),
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

        let limit_metric_name = BUCKET_REPL_BANDWIDTH_LIMIT_MD.get_full_metric_name();
        let limit_metric = metrics.iter().find(|m| {
            m.name == limit_metric_name && m.value == 1_048_576.0 && m.labels.iter().any(|(k, v)| *k == "bucket" && v == "b1")
        });
        assert!(limit_metric.is_some());
        assert!(
            limit_metric
                .and_then(|m| {
                    m.labels
                        .iter()
                        .find(|(k, _)| *k == "targetArn")
                        .map(|(_, v)| v.as_ref() == "arn:rustfs:replication:us-east-1:1:test-2")
                })
                .unwrap_or(false)
        );

        let current_metric_name = BUCKET_REPL_BANDWIDTH_CURRENT_MD.get_full_metric_name();
        let current_metric = metrics.iter().find(|m| {
            m.name == current_metric_name && m.value == 204_800.0 && m.labels.iter().any(|(k, v)| *k == "bucket" && v == "b1")
        });
        assert!(current_metric.is_some());
    }

    #[test]
    fn test_collect_bucket_replication_bandwidth_metrics_empty() {
        let stats: Vec<BucketReplicationBandwidthStats> = Vec::new();
        let metrics = collect_bucket_replication_bandwidth_metrics(&stats);
        assert!(metrics.is_empty());
    }
}
