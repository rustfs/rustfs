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

//! Per-node and per-disk metrics collector.
//!
//! Collects storage metrics for each disk/drive in the cluster,
//! including capacity, usage, and health status.

use crate::MetricType;
use crate::format::PrometheusMetric;
use std::borrow::Cow;

/// Statistics for a single disk/drive.
#[derive(Debug, Clone, Default)]
pub struct DiskStats {
    /// Server endpoint (e.g., "node1:9000")
    pub server: String,
    /// Drive path (e.g., "/data/disk1")
    pub drive: String,
    /// Total capacity in bytes
    pub total_bytes: u64,
    /// Used space in bytes
    pub used_bytes: u64,
    /// Free space in bytes
    pub free_bytes: u64,
}

// Static metric definitions
const METRIC_TOTAL: &str = "rustfs_node_disk_total_bytes";
const METRIC_USED: &str = "rustfs_node_disk_used_bytes";
const METRIC_FREE: &str = "rustfs_node_disk_free_bytes";

const HELP_TOTAL: &str = "Total disk capacity in bytes";
const HELP_USED: &str = "Used disk space in bytes";
const HELP_FREE: &str = "Free disk space in bytes";

/// Collects per-node disk metrics from the provided disk statistics.
///
/// # Metrics Produced
///
/// For each disk, the following metrics are produced with `server` and `drive` labels:
///
/// - `rustfs_node_disk_total_bytes`: Total capacity of the disk
/// - `rustfs_node_disk_used_bytes`: Used space on the disk
/// - `rustfs_node_disk_free_bytes`: Free space on the disk
///
/// # Arguments
///
/// * `disks` - Slice of disk statistics
///
/// # Example
///
/// ```
/// use rustfs_metrics::collectors::{collect_node_metrics, DiskStats};
///
/// let disks = vec![
///     DiskStats {
///         server: "node1:9000".to_string(),
///         drive: "/data/disk1".to_string(),
///         total_bytes: 1_000_000_000,
///         used_bytes: 400_000_000,
///         free_bytes: 600_000_000,
///     },
/// ];
/// let metrics = collect_node_metrics(&disks);
/// assert_eq!(metrics.len(), 3);
/// ```
#[must_use]
#[inline]
pub fn collect_node_metrics(disks: &[DiskStats]) -> Vec<PrometheusMetric> {
    if disks.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(disks.len() * 3);

    for disk in disks {
        let server_label: Cow<'static, str> = Cow::Owned(disk.server.clone());
        let drive_label: Cow<'static, str> = Cow::Owned(disk.drive.clone());

        metrics.push(
            PrometheusMetric::new(METRIC_TOTAL, MetricType::Gauge, HELP_TOTAL, disk.total_bytes as f64)
                .with_label("server", server_label.clone())
                .with_label("drive", drive_label.clone()),
        );

        metrics.push(
            PrometheusMetric::new(METRIC_USED, MetricType::Gauge, HELP_USED, disk.used_bytes as f64)
                .with_label("server", server_label.clone())
                .with_label("drive", drive_label.clone()),
        );

        metrics.push(
            PrometheusMetric::new(METRIC_FREE, MetricType::Gauge, HELP_FREE, disk.free_bytes as f64)
                .with_label("server", server_label)
                .with_label("drive", drive_label),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_node_metrics() {
        let disks = vec![
            DiskStats {
                server: "node1:9000".to_string(),
                drive: "/data/disk1".to_string(),
                total_bytes: 1000000,
                used_bytes: 400000,
                free_bytes: 600000,
            },
            DiskStats {
                server: "node2:9000".to_string(),
                drive: "/data/disk2".to_string(),
                total_bytes: 2000000,
                used_bytes: 800000,
                free_bytes: 1200000,
            },
        ];

        let metrics = collect_node_metrics(&disks);

        // 2 disks * 3 metrics each = 6 metrics
        assert_eq!(metrics.len(), 6);

        // Verify node1 disk1 total bytes
        let node1_total = metrics.iter().find(|m| {
            m.name == METRIC_TOTAL
                && m.labels.iter().any(|(k, v)| *k == "server" && v == "node1:9000")
                && m.labels.iter().any(|(k, v)| *k == "drive" && v == "/data/disk1")
        });
        assert!(node1_total.is_some());
        assert_eq!(node1_total.map(|m| m.value), Some(1000000.0));

        // Verify node2 disk2 used bytes
        let node2_used = metrics.iter().find(|m| {
            m.name == METRIC_USED
                && m.labels.iter().any(|(k, v)| *k == "server" && v == "node2:9000")
                && m.labels.iter().any(|(k, v)| *k == "drive" && v == "/data/disk2")
        });
        assert!(node2_used.is_some());
        assert_eq!(node2_used.map(|m| m.value), Some(800000.0));
    }

    #[test]
    fn test_collect_node_metrics_empty() {
        let disks: Vec<DiskStats> = vec![];
        let metrics = collect_node_metrics(&disks);
        assert!(metrics.is_empty());
    }

    #[test]
    fn test_collect_node_metrics_labels() {
        let disks = vec![DiskStats {
            server: "localhost:9000".to_string(),
            drive: "/mnt/data".to_string(),
            total_bytes: 500,
            used_bytes: 200,
            free_bytes: 300,
        }];

        let metrics = collect_node_metrics(&disks);

        for metric in &metrics {
            assert_eq!(metric.labels.len(), 2);
            assert!(metric.labels.iter().any(|(k, _)| *k == "server"));
            assert!(metric.labels.iter().any(|(k, _)| *k == "drive"));
        }
    }

    #[test]
    fn test_disk_stats_default() {
        let stats = DiskStats::default();
        assert!(stats.server.is_empty());
        assert!(stats.drive.is_empty());
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.used_bytes, 0);
        assert_eq!(stats.free_bytes, 0);
    }
}
