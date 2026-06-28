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

//! Cluster config metrics collector.
//!
//! Collects cluster configuration metrics including storage class
//! parity settings.
//!
//! This collector reuses the metric descriptors defined in `metrics_type::cluster_config`
//! to avoid duplication of metric names, types, and help text.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::cluster_config::*;

/// Cluster configuration statistics.
#[derive(Debug, Clone, Default)]
pub struct ClusterConfigStats {
    /// Parity for reduced redundancy storage (RRS) class
    pub rrs_parity: u32,
    /// Parity for standard storage class
    pub standard_parity: u32,
}

/// Collects cluster config metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::cluster_config` module.
/// Returns a vector of Prometheus metrics for cluster configuration.
pub fn collect_cluster_config_metrics(stats: &ClusterConfigStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&CONFIG_RRS_PARITY_MD, stats.rrs_parity as f64),
        PrometheusMetric::from_descriptor(&CONFIG_STANDARD_PARITY_MD, stats.standard_parity as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_cluster_config_metrics() {
        let stats = ClusterConfigStats {
            rrs_parity: 2,
            standard_parity: 4,
        };

        let metrics = collect_cluster_config_metrics(&stats);

        assert_eq!(metrics.len(), 2);

        let rrs = metrics.iter().find(|m| m.value == 2.0);
        assert!(rrs.is_some());

        let standard = metrics.iter().find(|m| m.value == 4.0);
        assert!(standard.is_some());
    }

    #[test]
    fn test_collect_cluster_config_metrics_default() {
        let stats = ClusterConfigStats::default();
        let metrics = collect_cluster_config_metrics(&stats);

        assert_eq!(metrics.len(), 2);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
