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

use crate::MetricType;
use crate::format::PrometheusMetric;

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

// Static metric definitions to avoid allocations
const METRIC_RAW_CAPACITY: &str = "rustfs_cluster_capacity_raw_total_bytes";
const METRIC_USABLE_CAPACITY: &str = "rustfs_cluster_capacity_usable_total_bytes";
const METRIC_USED: &str = "rustfs_cluster_capacity_used_bytes";
const METRIC_FREE: &str = "rustfs_cluster_capacity_free_bytes";
const METRIC_OBJECTS: &str = "rustfs_cluster_objects_total";
const METRIC_BUCKETS: &str = "rustfs_cluster_buckets_total";

const HELP_RAW_CAPACITY: &str = "Total raw storage capacity in bytes across all disks";
const HELP_USABLE_CAPACITY: &str = "Total usable storage capacity in bytes (accounting for erasure coding)";
const HELP_USED: &str = "Total used storage capacity in bytes";
const HELP_FREE: &str = "Total free storage capacity in bytes";
const HELP_OBJECTS: &str = "Total number of objects in the cluster";
const HELP_BUCKETS: &str = "Total number of buckets in the cluster";

/// Number of metrics produced by this collector.
const METRIC_COUNT: usize = 6;

/// Collects cluster-wide metrics from the provided statistics.
///
/// # Metrics Produced
///
/// - `rustfs_cluster_capacity_raw_total_bytes`: Total raw storage capacity across all disks
/// - `rustfs_cluster_capacity_usable_total_bytes`: Usable capacity after erasure coding overhead
/// - `rustfs_cluster_capacity_used_bytes`: Currently used storage capacity
/// - `rustfs_cluster_capacity_free_bytes`: Available free storage capacity
/// - `rustfs_cluster_objects_total`: Total number of objects in the cluster
/// - `rustfs_cluster_buckets_total`: Total number of buckets in the cluster
///
/// # Arguments
///
/// * `stats` - Cluster statistics containing capacity and usage data
///
/// # Example
///
/// ```
/// use rustfs_metrics::collectors::{collect_cluster_metrics, ClusterStats};
///
/// let stats = ClusterStats {
///     raw_capacity_bytes: 10_000_000_000,
///     usable_capacity_bytes: 8_000_000_000,
///     used_bytes: 2_000_000_000,
///     free_bytes: 6_000_000_000,
///     objects_count: 1000,
///     buckets_count: 10,
/// };
/// let metrics = collect_cluster_metrics(&stats);
/// assert_eq!(metrics.len(), 6);
/// ```
#[must_use]
#[inline]
pub fn collect_cluster_metrics(stats: &ClusterStats) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(METRIC_COUNT);

    metrics.push(PrometheusMetric::new(
        METRIC_RAW_CAPACITY,
        MetricType::Gauge,
        HELP_RAW_CAPACITY,
        stats.raw_capacity_bytes as f64,
    ));
    metrics.push(PrometheusMetric::new(
        METRIC_USABLE_CAPACITY,
        MetricType::Gauge,
        HELP_USABLE_CAPACITY,
        stats.usable_capacity_bytes as f64,
    ));
    metrics.push(PrometheusMetric::new(METRIC_USED, MetricType::Gauge, HELP_USED, stats.used_bytes as f64));
    metrics.push(PrometheusMetric::new(METRIC_FREE, MetricType::Gauge, HELP_FREE, stats.free_bytes as f64));
    metrics.push(PrometheusMetric::new(
        METRIC_OBJECTS,
        MetricType::Gauge,
        HELP_OBJECTS,
        stats.objects_count as f64,
    ));
    metrics.push(PrometheusMetric::new(
        METRIC_BUCKETS,
        MetricType::Gauge,
        HELP_BUCKETS,
        stats.buckets_count as f64,
    ));

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::report_metrics;

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
        let raw_capacity = metrics.iter().find(|m| m.name == METRIC_RAW_CAPACITY);
        assert!(raw_capacity.is_some());
        assert_eq!(raw_capacity.map(|m| m.value), Some(3000.0));

        // Verify used capacity
        let used = metrics.iter().find(|m| m.name == METRIC_USED);
        assert!(used.is_some());
        assert_eq!(used.map(|m| m.value), Some(1200.0));

        // Verify object count
        let objects = metrics.iter().find(|m| m.name == METRIC_OBJECTS);
        assert!(objects.is_some());
        assert_eq!(objects.map(|m| m.value), Some(100.0));

        // Verify bucket count
        let buckets = metrics.iter().find(|m| m.name == METRIC_BUCKETS);
        assert!(buckets.is_some());
        assert_eq!(buckets.map(|m| m.value), Some(5.0));
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
