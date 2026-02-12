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

//! Per-bucket metrics collector.
//!
//! Collects usage metrics for each bucket in the cluster, including
//! size, object counts, and quota information.

use crate::MetricType;
use crate::format::PrometheusMetric;
use std::borrow::Cow;

/// Usage statistics for a single bucket.
#[derive(Debug, Clone, Default)]
pub struct BucketStats {
    /// Bucket name
    pub name: String,
    /// Total bytes used by the bucket
    pub size_bytes: u64,
    /// Total number of objects in the bucket
    pub objects_count: u64,
    /// Quota limit in bytes (0 means no quota)
    pub quota_bytes: u64,
}

// Static metric definitions
const METRIC_SIZE: &str = "rustfs_bucket_usage_bytes";
const METRIC_OBJECTS: &str = "rustfs_bucket_objects_total";
const METRIC_QUOTA: &str = "rustfs_bucket_quota_bytes";

const HELP_SIZE: &str = "Total bytes used by the bucket";
const HELP_OBJECTS: &str = "Total number of objects in the bucket";
const HELP_QUOTA: &str = "Quota limit in bytes for the bucket";

/// Collects per-bucket usage metrics from the provided bucket statistics.
///
/// # Metrics Produced
///
/// For each bucket, the following metrics are produced with a `bucket` label:
///
/// - `rustfs_bucket_usage_bytes`: Total bytes used by the bucket
/// - `rustfs_bucket_objects_total`: Total number of objects in the bucket
/// - `rustfs_bucket_quota_bytes`: Quota limit in bytes (0 if no quota configured)
///
/// # Arguments
///
/// * `buckets` - Slice of bucket statistics
///
/// # Example
///
/// ```
/// use rustfs_metrics::collectors::{collect_bucket_metrics, BucketStats};
///
/// let buckets = vec![
///     BucketStats {
///         name: "my-bucket".to_string(),
///         size_bytes: 1_000_000,
///         objects_count: 100,
///         quota_bytes: 10_000_000,
///     },
/// ];
/// let metrics = collect_bucket_metrics(&buckets);
/// assert_eq!(metrics.len(), 3); // size, objects, quota
/// ```
#[must_use]
#[inline]
pub fn collect_bucket_metrics(buckets: &[BucketStats]) -> Vec<PrometheusMetric> {
    if buckets.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(buckets.len() * 3);

    for bucket in buckets {
        let bucket_label: Cow<'static, str> = Cow::Owned(bucket.name.clone());

        // Bucket size in bytes
        metrics.push(
            PrometheusMetric::new(METRIC_SIZE, MetricType::Gauge, HELP_SIZE, bucket.size_bytes as f64)
                .with_label("bucket", bucket_label.clone()),
        );

        // Object count
        metrics.push(
            PrometheusMetric::new(METRIC_OBJECTS, MetricType::Gauge, HELP_OBJECTS, bucket.objects_count as f64)
                .with_label("bucket", bucket_label.clone()),
        );

        // Quota (always emit, 0 when no quota configured for consistent PromQL queries)
        metrics.push(
            PrometheusMetric::new(METRIC_QUOTA, MetricType::Gauge, HELP_QUOTA, bucket.quota_bytes as f64)
                .with_label("bucket", bucket_label),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::report_metrics;

    #[test]
    fn test_collect_bucket_metrics() {
        let buckets = vec![
            BucketStats {
                name: "test-bucket".to_string(),
                size_bytes: 1000,
                objects_count: 50,
                quota_bytes: 0,
            },
            BucketStats {
                name: "other-bucket".to_string(),
                size_bytes: 2000,
                objects_count: 100,
                quota_bytes: 0,
            },
        ];

        let metrics = collect_bucket_metrics(&buckets);
        report_metrics(&metrics); // This will compile and run, but we can't easily assert on the global recorder state here.

        // 2 buckets * 3 metrics each (size, objects, quota) = 6 metrics
        assert_eq!(metrics.len(), 6);

        // Verify test-bucket metrics
        let test_bucket_size = metrics
            .iter()
            .find(|m| m.name == METRIC_SIZE && m.labels.iter().any(|(k, v)| *k == "bucket" && v == "test-bucket"));
        assert!(test_bucket_size.is_some());
        assert_eq!(test_bucket_size.map(|m| m.value), Some(1000.0));
    }

    #[test]
    fn test_collect_bucket_metrics_with_quotas() {
        let buckets = vec![BucketStats {
            name: "quota-bucket".to_string(),
            size_bytes: 500,
            objects_count: 10,
            quota_bytes: 10000,
        }];

        let metrics = collect_bucket_metrics(&buckets);
        report_metrics(&metrics);

        // 1 bucket * 3 metrics (size, objects, quota) = 3 metrics
        assert_eq!(metrics.len(), 3);

        // Verify quota metric exists
        let quota_metric = metrics.iter().find(|m| m.name == METRIC_QUOTA);
        assert!(quota_metric.is_some());
        assert_eq!(quota_metric.map(|m| m.value), Some(10000.0));
    }

    #[test]
    fn test_collect_bucket_metrics_empty() {
        let buckets: Vec<BucketStats> = vec![];
        let metrics = collect_bucket_metrics(&buckets);
        assert!(metrics.is_empty());
    }

    #[test]
    fn test_collect_bucket_metrics_zero_quota_always_reported() {
        let buckets = vec![BucketStats {
            name: "no-quota-bucket".to_string(),
            size_bytes: 100,
            objects_count: 5,
            quota_bytes: 0,
        }];

        let metrics = collect_bucket_metrics(&buckets);
        report_metrics(&metrics);

        // Zero quota should still produce a quota metric with value 0 for consistent PromQL queries
        assert_eq!(metrics.len(), 3);
        let quota_metric = metrics.iter().find(|m| m.name == METRIC_QUOTA);
        assert!(quota_metric.is_some());
        assert_eq!(quota_metric.map(|m| m.value), Some(0.0));
    }

    #[test]
    fn test_bucket_stats_default() {
        let stats = BucketStats::default();
        assert!(stats.name.is_empty());
        assert_eq!(stats.size_bytes, 0);
        assert_eq!(stats.objects_count, 0);
        assert_eq!(stats.quota_bytes, 0);
    }
}
