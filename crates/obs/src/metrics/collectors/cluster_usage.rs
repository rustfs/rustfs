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

#![allow(dead_code)]

//! Cluster usage metrics collector.
//!
//! Collects cluster-wide and per-bucket usage metrics including
//! object counts, sizes, versions, and distributions.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::cluster_usage::*;

/// Cluster-wide usage statistics.
#[derive(Debug, Clone, Default)]
pub struct ClusterUsageStats {
    /// Total bytes used in the cluster
    pub total_bytes: u64,
    /// Total number of objects
    pub objects_count: u64,
    /// Total number of object versions (including delete markers)
    pub versions_count: u64,
    /// Total number of delete markers
    pub delete_markers_count: u64,
    /// Object size distribution by range
    pub object_size_distribution: Vec<(String, u64)>,
    /// Version count distribution by range
    pub versions_distribution: Vec<(String, u64)>,
}

/// Per-bucket usage statistics.
#[derive(Debug, Clone, Default)]
pub struct BucketUsageStats {
    /// Bucket name
    pub bucket: String,
    /// Total bytes used in the bucket
    pub total_bytes: u64,
    /// Total number of objects in the bucket
    pub objects_count: u64,
    /// Total number of object versions (including delete markers)
    pub versions_count: u64,
    /// Total number of delete markers
    pub delete_markers_count: u64,
    /// Bucket quota in bytes (0 if no quota)
    pub quota_bytes: u64,
    /// Object size distribution by range
    pub object_size_distribution: Vec<(String, u64)>,
    /// Version count distribution by range
    pub version_count_distribution: Vec<(String, u64)>,
}

/// Collects cluster-wide usage metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for cluster usage.
pub fn collect_cluster_usage_metrics(stats: &ClusterUsageStats) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(4 + stats.object_size_distribution.len() + stats.versions_distribution.len());

    metrics.push(PrometheusMetric::from_descriptor(&USAGE_TOTAL_BYTES_MD, stats.total_bytes as f64));
    metrics.push(PrometheusMetric::from_descriptor(&USAGE_OBJECTS_COUNT_MD, stats.objects_count as f64));
    metrics.push(PrometheusMetric::from_descriptor(&USAGE_VERSIONS_COUNT_MD, stats.versions_count as f64));
    metrics.push(PrometheusMetric::from_descriptor(
        &USAGE_DELETE_MARKERS_COUNT_MD,
        stats.delete_markers_count as f64,
    ));

    // Object size distribution
    for (range, count) in &stats.object_size_distribution {
        metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_OBJECTS_DISTRIBUTION_MD, *count as f64)
                .with_label_owned(RANGE_LABEL, range.clone()),
        );
    }

    // Version distribution
    for (range, count) in &stats.versions_distribution {
        metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_VERSIONS_DISTRIBUTION_MD, *count as f64)
                .with_label_owned(RANGE_LABEL, range.clone()),
        );
    }

    metrics
}

/// Collects per-bucket usage metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for bucket usage.
pub fn collect_bucket_usage_metrics(stats: &[BucketUsageStats]) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::new();

    for stat in stats {
        let bucket_label = stat.bucket.clone();

        metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_TOTAL_BYTES_MD, stat.total_bytes as f64)
                .with_label_owned(BUCKET_LABEL, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_OBJECTS_TOTAL_MD, stat.objects_count as f64)
                .with_label_owned(BUCKET_LABEL, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_VERSIONS_COUNT_MD, stat.versions_count as f64)
                .with_label_owned(BUCKET_LABEL, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_DELETE_MARKERS_COUNT_MD, stat.delete_markers_count as f64)
                .with_label_owned(BUCKET_LABEL, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_QUOTA_TOTAL_BYTES_MD, stat.quota_bytes as f64)
                .with_label_owned(BUCKET_LABEL, bucket_label.clone()),
        );

        // Object size distribution per bucket
        for (range, count) in &stat.object_size_distribution {
            metrics.push(
                PrometheusMetric::from_descriptor(&USAGE_BUCKET_OBJECT_SIZE_DISTRIBUTION_MD, *count as f64)
                    .with_label_owned(RANGE_LABEL, range.clone())
                    .with_label_owned(BUCKET_LABEL, bucket_label.clone()),
            );
        }

        // Version count distribution per bucket
        for (range, count) in &stat.version_count_distribution {
            metrics.push(
                PrometheusMetric::from_descriptor(&USAGE_BUCKET_OBJECT_VERSION_COUNT_DISTRIBUTION_MD, *count as f64)
                    .with_label_owned(RANGE_LABEL, range.clone())
                    .with_label_owned(BUCKET_LABEL, bucket_label.clone()),
            );
        }
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_cluster_usage_metrics() {
        let stats = ClusterUsageStats {
            total_bytes: 1024 * 1024 * 1024 * 100, // 100 GB
            objects_count: 10000,
            versions_count: 15000,
            delete_markers_count: 500,
            object_size_distribution: vec![
                ("0-1KB".to_string(), 5000),
                ("1KB-1MB".to_string(), 3000),
                ("1MB-100MB".to_string(), 1500),
                ("100MB+".to_string(), 500),
            ],
            versions_distribution: vec![("1".to_string(), 8000), ("2-5".to_string(), 1500), ("6+".to_string(), 500)],
        };

        let metrics = collect_cluster_usage_metrics(&stats);
        report_metrics(&metrics);

        // 4 base metrics + 4 size distribution + 3 version distribution = 11
        assert_eq!(metrics.len(), 11);

        let total_bytes_name = USAGE_TOTAL_BYTES_MD.get_full_metric_name();
        let total_bytes = metrics.iter().find(|m| m.name == total_bytes_name);
        assert!(total_bytes.is_some());
    }

    #[test]
    fn test_collect_bucket_usage_metrics() {
        let stats = vec![BucketUsageStats {
            bucket: "test-bucket".to_string(),
            total_bytes: 1024 * 1024 * 1024 * 10, // 10 GB
            objects_count: 1000,
            versions_count: 1200,
            delete_markers_count: 50,
            quota_bytes: 1024 * 1024 * 1024 * 100, // 100 GB quota
            object_size_distribution: vec![("0-1KB".to_string(), 500)],
            version_count_distribution: vec![("1".to_string(), 800)],
        }];

        let metrics = collect_bucket_usage_metrics(&stats);
        report_metrics(&metrics);

        // 5 base metrics + 1 size distribution + 1 version distribution = 7
        assert_eq!(metrics.len(), 7);
    }
}
