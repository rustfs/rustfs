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
//!
//! This collector reuses the metric descriptors defined in `metrics_type::node_bucket`
//! to avoid duplication of metric names, types, and help text.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::node_bucket::*;
use std::borrow::Cow;

/// Bucket statistics for metrics collection.
///
/// This struct provides a decoupled interface for collecting bucket metrics
/// without depending on specific internal types. HTTP handlers should populate
/// this struct from their available data sources.
#[derive(Debug, Clone, Default)]
pub struct BucketStats {
    /// Name of the bucket
    pub name: String,
    /// Total size of all objects in the bucket (bytes)
    pub size_bytes: u64,
    /// Number of objects in the bucket
    pub objects_count: u64,
    /// Quota limit for the bucket (bytes), 0 if no quota
    pub quota_bytes: u64,
}

/// Collects per-bucket metrics from the provided bucket statistics.
///
/// Uses the metric descriptors from `metrics_type::node_bucket` module.
/// Returns a vector of Prometheus metrics for all buckets.
pub fn collect_bucket_metrics(buckets: &[BucketStats]) -> Vec<PrometheusMetric> {
    if buckets.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(buckets.len() * 3);
    for bucket in buckets {
        let bucket_label: Cow<'static, str> = Cow::Owned(bucket.name.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_USAGE_BYTES_MD, bucket.size_bytes as f64)
                .with_label("bucket", bucket_label.clone()),
        );

        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_OBJECTS_TOTAL_MD, bucket.objects_count as f64)
                .with_label("bucket", bucket_label.clone()),
        );

        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_QUOTA_BYTES_MD, bucket.quota_bytes as f64)
                .with_label("bucket", bucket_label),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_bucket_metrics() {
        let buckets = vec![
            BucketStats {
                name: "test-bucket".to_string(),
                size_bytes: 1000,
                objects_count: 100,
                quota_bytes: 0,
            },
            BucketStats {
                name: "another-bucket".to_string(),
                size_bytes: 2000,
                objects_count: 200,
                quota_bytes: 0,
            },
        ];

        let metrics = collect_bucket_metrics(&buckets);
        report_metrics(&metrics);

        // 2 buckets * 3 metrics each (size, objects, quota) = 6 metrics
        assert_eq!(metrics.len(), 6);

        // Verify test-bucket metrics have correct labels
        let test_bucket_size_name = BUCKET_USAGE_BYTES_MD.get_full_metric_name();
        let test_bucket_size = metrics
            .iter()
            .find(|m| m.name == test_bucket_size_name && m.labels.iter().any(|(k, v)| *k == "bucket" && v == "test-bucket"));
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
        let quota_metric_name = BUCKET_QUOTA_BYTES_MD.get_full_metric_name();
        let quota_metric = metrics.iter().find(|m| {
            m.name == quota_metric_name
                && m.value == 10000.0
                && m.labels.iter().any(|(k, v)| *k == "bucket" && v == "quota-bucket")
        });
        assert!(quota_metric.is_some());
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
        let quota_metric_name = BUCKET_QUOTA_BYTES_MD.get_full_metric_name();
        let quota_metric = metrics.iter().find(|m| {
            m.name == quota_metric_name
                && m.value == 0.0
                && m.labels.iter().any(|(k, v)| *k == "bucket" && v == "no-quota-bucket")
        });
        assert!(quota_metric.is_some());
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
