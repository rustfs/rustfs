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

//! Network I/O metrics collector for host-wide interface counters.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::system_network_host::{HOST_NETWORK_IO_MD, HOST_NETWORK_IO_PER_INTERFACE_MD};
use std::borrow::Cow;

/// Network I/O statistics.
///
/// Contains host-wide network I/O totals and per-interface counters.
#[derive(Debug, Clone, Default)]
pub struct HostNetworkStats {
    /// Total bytes received across observed host interfaces.
    pub total_received: u64,
    /// Total bytes transmitted across observed host interfaces.
    pub total_transmitted: u64,
    /// Per-interface statistics: (interface_name, received_bytes, transmitted_bytes)
    pub per_interface: Vec<(String, u64, u64)>,
}

/// Collects network I/O metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for host-wide network I/O statistics.
/// Each metric includes a `direction` label ("received" or "transmitted").
/// Per-interface metrics also include an `interface` label.
pub fn collect_host_network_metrics(
    stats: &HostNetworkStats,
    labels: Option<&[(&'static str, Cow<'static, str>)]>,
) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(2 + stats.per_interface.len() * 2);

    let mut received_metric = PrometheusMetric::from_descriptor(&HOST_NETWORK_IO_MD, stats.total_received as f64);
    let mut transmitted_metric = PrometheusMetric::from_descriptor(&HOST_NETWORK_IO_MD, stats.total_transmitted as f64);

    received_metric.labels.push(("direction", Cow::Borrowed("received")));
    transmitted_metric.labels.push(("direction", Cow::Borrowed("transmitted")));

    if let Some(l) = labels {
        received_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
        transmitted_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
    }

    metrics.push(received_metric);
    metrics.push(transmitted_metric);

    for (interface, received, transmitted) in &stats.per_interface {
        let mut iface_received = PrometheusMetric::from_descriptor(&HOST_NETWORK_IO_PER_INTERFACE_MD, *received as f64);
        let mut iface_transmitted = PrometheusMetric::from_descriptor(&HOST_NETWORK_IO_PER_INTERFACE_MD, *transmitted as f64);

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

    #[test]
    fn host_network_metrics_use_dedicated_network_host_prefix() {
        let stats = HostNetworkStats {
            total_received: 1024,
            total_transmitted: 2048,
            per_interface: vec![("eth0".to_string(), 512, 256)],
        };

        let metrics = collect_host_network_metrics(&stats, None);

        assert_eq!(metrics.len(), 4);
        assert!(
            metrics
                .iter()
                .all(|metric| metric.name.starts_with("rustfs_system_network_host_"))
        );
    }
}
