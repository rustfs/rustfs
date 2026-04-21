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

//! System network metrics collector.
//!
//! Collects internode network metrics including errors, dial times,
//! and bytes sent/received.
//!
//! This module provides system-level internode network metrics.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::system_network::*;

/// Network statistics for internode communication.
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    /// Total number of failed internode calls
    pub internode_errors_total: u64,
    /// Total number of TCP dial timeouts and errors
    pub internode_dial_errors_total: u64,
    /// Average dial time in nanoseconds
    pub internode_dial_avg_time_nanos: u64,
    /// Total bytes sent to other nodes
    pub internode_sent_bytes_total: u64,
    /// Total bytes received from other nodes
    pub internode_recv_bytes_total: u64,
}

/// Collects network metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::system_network` module.
/// Returns a vector of Prometheus metrics for network statistics.
pub fn collect_network_metrics(stats: &NetworkStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&INTERNODE_ERRORS_TOTAL_MD, stats.internode_errors_total as f64),
        PrometheusMetric::from_descriptor(&INTERNODE_DIAL_ERRORS_TOTAL_MD, stats.internode_dial_errors_total as f64),
        PrometheusMetric::from_descriptor(&INTERNODE_DIAL_AVG_TIME_NANOS_MD, stats.internode_dial_avg_time_nanos as f64),
        PrometheusMetric::from_descriptor(&INTERNODE_SENT_BYTES_TOTAL_MD, stats.internode_sent_bytes_total as f64),
        PrometheusMetric::from_descriptor(&INTERNODE_RECV_BYTES_TOTAL_MD, stats.internode_recv_bytes_total as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_network_metrics() {
        let stats = NetworkStats {
            internode_errors_total: 10,
            internode_dial_errors_total: 5,
            internode_dial_avg_time_nanos: 1_500_000,      // 1.5ms
            internode_sent_bytes_total: 1024 * 1024 * 100, // 100 MB
            internode_recv_bytes_total: 1024 * 1024 * 200, // 200 MB
        };

        let metrics = collect_network_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 5);
        assert!(metrics.iter().all(|m| m.name.contains("internode")));
    }

    #[test]
    fn test_collect_network_metrics_default() {
        let stats = NetworkStats::default();
        let metrics = collect_network_metrics(&stats);

        assert_eq!(metrics.len(), 5);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
