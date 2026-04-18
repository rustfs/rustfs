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
//! This module provides both system-level and process-level network metrics,
//! with process-level metrics migrated from `rustfs-obs::system`.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::system_network::*;
use crate::metrics::schema::system_process::{PROCESS_NETWORK_IO_MD, PROCESS_NETWORK_IO_PER_INTERFACE_MD};
use std::borrow::Cow;

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

/// Process network I/O statistics.
///
/// Contains network I/O metrics for a specific process.
#[derive(Debug, Clone, Default)]
pub struct ProcessNetworkStats {
    /// Total bytes received
    pub total_received: u64,
    /// Total bytes transmitted
    pub total_transmitted: u64,
    /// Per-interface statistics: (interface_name, received_bytes, transmitted_bytes)
    pub per_interface: Vec<(String, u64, u64)>,
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

/// Collects process network I/O metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for process network I/O statistics.
/// Each metric includes a `direction` label ("received" or "transmitted").
/// Per-interface metrics also include an `interface` label.
///
/// # Arguments
///
/// * `stats` - Process network I/O statistics
/// * `labels` - Optional additional labels (e.g., process attributes)
pub fn collect_process_network_metrics(
    stats: &ProcessNetworkStats,
    labels: Option<&[(&'static str, Cow<'static, str>)]>,
) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(2 + stats.per_interface.len() * 2);

    // Total network I/O
    let mut received_metric = PrometheusMetric::from_descriptor(&PROCESS_NETWORK_IO_MD, stats.total_received as f64);
    let mut transmitted_metric = PrometheusMetric::from_descriptor(&PROCESS_NETWORK_IO_MD, stats.total_transmitted as f64);

    received_metric.labels.push(("direction", Cow::Borrowed("received")));
    transmitted_metric.labels.push(("direction", Cow::Borrowed("transmitted")));

    if let Some(l) = labels {
        received_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
        transmitted_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
    }

    metrics.push(received_metric);
    metrics.push(transmitted_metric);

    // Per-interface network I/O
    for (interface, received, transmitted) in &stats.per_interface {
        let mut iface_received = PrometheusMetric::from_descriptor(&PROCESS_NETWORK_IO_PER_INTERFACE_MD, *received as f64);
        let mut iface_transmitted = PrometheusMetric::from_descriptor(&PROCESS_NETWORK_IO_PER_INTERFACE_MD, *transmitted as f64);

        iface_received.labels.push(("interface", Cow::Owned(interface.clone())));
        iface_received.labels.push(("direction", Cow::Borrowed("received")));

        iface_transmitted.labels.push(("interface", Cow::Owned(interface.clone())));
        iface_transmitted.labels.push(("direction", Cow::Borrowed("transmitted")));

        if let Some(l) = labels {
            iface_received.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
            iface_transmitted.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
        }

        metrics.push(iface_received);
        metrics.push(iface_transmitted);
    }

    metrics
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
