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

//! Cluster health metrics collector.
//!
//! Collects cluster-wide health metrics including drive counts
//! (offline, online, total).
//!
//! This collector reuses the metric descriptors defined in `metrics_type::cluster_health`
//! to avoid duplication of metric names, types, and help text.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::cluster_health::*;

/// Cluster health statistics.
#[derive(Debug, Clone, Default)]
pub struct ClusterHealthStats {
    /// Number of offline drives in the cluster
    pub drives_offline_count: u64,
    /// Number of online drives in the cluster
    pub drives_online_count: u64,
    /// Total number of drives in the cluster
    pub drives_count: u64,
}

/// Collects cluster health metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::cluster_health` module.
/// Returns a vector of Prometheus metrics for cluster health.
pub fn collect_cluster_health_metrics(stats: &ClusterHealthStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&HEALTH_DRIVES_OFFLINE_COUNT_MD, stats.drives_offline_count as f64),
        PrometheusMetric::from_descriptor(&HEALTH_DRIVES_ONLINE_COUNT_MD, stats.drives_online_count as f64),
        PrometheusMetric::from_descriptor(&HEALTH_DRIVES_COUNT_MD, stats.drives_count as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_cluster_health_metrics() {
        let stats = ClusterHealthStats {
            drives_offline_count: 2,
            drives_online_count: 18,
            drives_count: 20,
        };

        let metrics = collect_cluster_health_metrics(&stats);

        assert_eq!(metrics.len(), 3);

        let offline = metrics.iter().find(|m| m.value == 2.0);
        assert!(offline.is_some());

        let online = metrics.iter().find(|m| m.value == 18.0);
        assert!(online.is_some());
    }

    #[test]
    fn test_collect_cluster_health_metrics_default() {
        let stats = ClusterHealthStats::default();
        let metrics = collect_cluster_health_metrics(&stats);

        assert_eq!(metrics.len(), 3);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
