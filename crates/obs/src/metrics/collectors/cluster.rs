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

//! Cluster-wide metrics collector.
//!
//! Collects aggregate metrics across the entire RustFS cluster including
//! total capacity, usage, and object counts.
//!
//! This collector reuses the metric descriptors defined in `metrics_type::cluster`
//! to avoid duplication of metric names, types, and help text.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::cluster::*;

/// Cluster capacity and usage statistics for metrics collection.
///
/// This struct provides a decoupled interface for collecting cluster metrics
/// without depending on specific internal types. HTTP handlers should populate
/// this struct from their available data sources.
#[derive(Debug, Clone, Default)]
pub struct ClusterStats {
    /// Total raw storage capacity across all disks in bytes
    pub raw_capacity_bytes: u64,
    /// Usable capacity after erasure coding overhead in bytes
    pub usable_capacity_bytes: u64,
    /// Currently used storage in bytes
    pub used_bytes: u64,
    /// Available free storage in bytes
    pub free_bytes: u64,
    /// Total number of objects in the cluster
    pub objects_count: u64,
    /// Total number of buckets in the cluster
    pub buckets_count: u64,
}

/// Collects cluster-wide metrics from the provided cluster statistics.
///
/// Uses the metric descriptors from `metrics_type::cluster` module.
/// Returns a vector of Prometheus metrics for cluster statistics.
pub fn collect_cluster_metrics(stats: &ClusterStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&CLUSTER_CAPACITY_RAW_TOTAL_BYTES_MD, stats.raw_capacity_bytes as f64),
        PrometheusMetric::from_descriptor(&CLUSTER_CAPACITY_USABLE_TOTAL_BYTES_MD, stats.usable_capacity_bytes as f64),
        PrometheusMetric::from_descriptor(&CLUSTER_CAPACITY_USED_BYTES_MD, stats.used_bytes as f64),
        PrometheusMetric::from_descriptor(&CLUSTER_CAPACITY_FREE_BYTES_MD, stats.free_bytes as f64),
        PrometheusMetric::from_descriptor(&CLUSTER_OBJECTS_TOTAL_MD, stats.objects_count as f64),
        PrometheusMetric::from_descriptor(&CLUSTER_BUCKETS_TOTAL_MD, stats.buckets_count as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_cluster_metrics() {
        let stats = ClusterStats {
            raw_capacity_bytes: 3000,
            usable_capacity_bytes: 2500,
            used_bytes: 1200,
            free_bytes: 1300,
            objects_count: 100,
            buckets_count: 5,
        };

        let metrics = collect_cluster_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 6);

        // Verify raw capacity
        let raw_capacity_name = CLUSTER_CAPACITY_RAW_TOTAL_BYTES_MD.get_full_metric_name();
        let raw_capacity = metrics.iter().find(|m| m.name == raw_capacity_name && m.value == 3000.0);
        assert!(raw_capacity.is_some());

        // Verify used capacity
        let used_name = CLUSTER_CAPACITY_USED_BYTES_MD.get_full_metric_name();
        let used = metrics.iter().find(|m| m.name == used_name && m.value == 1200.0);
        assert!(used.is_some());

        // Verify object count
        let objects_name = CLUSTER_OBJECTS_TOTAL_MD.get_full_metric_name();
        let objects = metrics.iter().find(|m| m.name == objects_name && m.value == 100.0);
        assert!(objects.is_some());

        // Verify bucket count
        let buckets_name = CLUSTER_BUCKETS_TOTAL_MD.get_full_metric_name();
        let buckets = metrics.iter().find(|m| m.name == buckets_name && m.value == 5.0);
        assert!(buckets.is_some());
    }

    #[test]
    fn test_collect_cluster_metrics_empty() {
        let stats = ClusterStats::default();

        let metrics = collect_cluster_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 6);

        // All values should be zero
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }

    #[test]
    fn test_cluster_stats_default() {
        let stats = ClusterStats::default();
        assert_eq!(stats.raw_capacity_bytes, 0);
        assert_eq!(stats.usable_capacity_bytes, 0);
        assert_eq!(stats.used_bytes, 0);
        assert_eq!(stats.free_bytes, 0);
        assert_eq!(stats.objects_count, 0);
        assert_eq!(stats.buckets_count, 0);
    }
}
