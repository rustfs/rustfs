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

//! System drive metrics collector.
//!
//! Collects detailed drive/disk metrics including capacity, I/O statistics,
//! error counts, and health status.

use crate::format::PrometheusMetric;
use crate::metrics_type::system_drive::*;

/// Detailed drive statistics for a single drive.
#[derive(Debug, Clone, Default)]
pub struct DriveDetailedStats {
    /// Server identifier (e.g., "node1:9000")
    pub server: String,
    /// Drive path (e.g., "/data/disk1")
    pub drive: String,
    /// Total capacity in bytes
    pub total_bytes: u64,
    /// Used capacity in bytes
    pub used_bytes: u64,
    /// Free capacity in bytes
    pub free_bytes: u64,
    /// Used inodes
    pub used_inodes: u64,
    /// Free inodes
    pub free_inodes: u64,
    /// Total inodes
    pub total_inodes: u64,
    /// Total timeout errors
    pub timeout_errors_total: u64,
    /// Total I/O errors
    pub io_errors_total: u64,
    /// Total availability errors
    pub availability_errors_total: u64,
    /// Number of I/O operations waiting
    pub waiting_io: u64,
    /// API latency in microseconds
    pub api_latency_micros: u64,
    /// Health status (1=healthy, 0=unhealthy)
    pub health: u8,
    /// Reads per second
    pub reads_per_sec: f64,
    /// Kilobytes read per second
    pub reads_kb_per_sec: f64,
    /// Average read await time
    pub reads_await: f64,
    /// Writes per second
    pub writes_per_sec: f64,
    /// Kilobytes written per second
    pub writes_kb_per_sec: f64,
    /// Average write await time
    pub writes_await: f64,
    /// Percentage utilization
    pub perc_util: f64,
}

/// Aggregate drive count statistics.
#[derive(Debug, Clone, Default)]
pub struct DriveCountStats {
    /// Number of offline Drives
    pub offline_count: u64,
    /// Number of online drives
    pub online_count: u64,
    /// Total number of drives
    pub total_count: u64,
}

/// Collects detailed drive metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for each drive.
pub fn collect_drive_detailed_metrics(stats: &[DriveDetailedStats]) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(stats.len() * 19);

    for stat in stats {
        metrics.extend(vec![
            PrometheusMetric::from_descriptor(&DRIVE_TOTAL_BYTES_MD, stat.total_bytes as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_USED_BYTES_MD, stat.used_bytes as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_FREE_BYTES_MD, stat.free_bytes as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_USED_INODES_MD, stat.used_inodes as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_FREE_INODES_MD, stat.free_inodes as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_TOTAL_INODES_MD, stat.total_inodes as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_TIMEOUT_ERRORS_MD, stat.timeout_errors_total as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_IO_ERRORS_MD, stat.io_errors_total as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_AVAILABILITY_ERRORS_MD, stat.availability_errors_total as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_WAITING_IO_MD, stat.waiting_io as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_API_LATENCY_MD, stat.api_latency_micros as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_HEALTH_MD, stat.health as f64)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_READS_PER_SEC_MD, stat.reads_per_sec)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_READS_KB_PER_SEC_MD, stat.reads_kb_per_sec)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_READS_AWAIT_MD, stat.reads_await)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_WRITES_PER_SEC_MD, stat.writes_per_sec)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_WRITES_KB_PER_SEC_MD, stat.writes_kb_per_sec)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_WRITES_AWAIT_MD, stat.writes_await)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
            PrometheusMetric::from_descriptor(&DRIVE_PERC_UTIL_MD, stat.perc_util)
                .with_label_owned(DRIVE_LABEL, stat.drive.clone())
                .with_label_owned(SERVER_LABEL, stat.server.clone()),
        ]);
    }

    metrics
}

/// Collects drive count metrics (offline, online, total).
///
/// Returns a vector of Prometheus metrics for drive counts.
pub fn collect_drive_count_metrics(stats: &DriveCountStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&DRIVE_OFFLINE_COUNT_MD, stats.offline_count as f64),
        PrometheusMetric::from_descriptor(&DRIVE_ONLINE_COUNT_MD, stats.online_count as f64),
        PrometheusMetric::from_descriptor(&DRIVE_COUNT_MD, stats.total_count as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::report_metrics;

    #[test]
    fn test_collect_drive_detailed_metrics() {
        let stats = vec![DriveDetailedStats {
            server: "node1:9000".to_string(),
            drive: "/data/disk1".to_string(),
            total_bytes: 1024 * 1024 * 1024 * 100, // 100 GB
            used_bytes: 1024 * 1024 * 1024 * 50,   // 50 GB
            free_bytes: 1024 * 1024 * 1024 * 50,   // 50 GB
            used_inodes: 100000,
            free_inodes: 900000,
            total_inodes: 1000000,
            timeout_errors_total: 5,
            io_errors_total: 10,
            availability_errors_total: 2,
            waiting_io: 3,
            api_latency_micros: 1500,
            health: 1,
            reads_per_sec: 100.0,
            reads_kb_per_sec: 1024.0,
            reads_await: 5.5,
            writes_per_sec: 50.0,
            writes_kb_per_sec: 512.0,
            writes_await: 10.2,
            perc_util: 75.5,
        }];

        let metrics = collect_drive_detailed_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 19);

        // Verify total bytes metric
        let total_bytes_name = DRIVE_TOTAL_BYTES_MD.get_full_metric_name();
        let total_bytes = metrics.iter().find(|m| m.name == total_bytes_name);
        assert!(total_bytes.is_some());
        assert_eq!(total_bytes.map(|m| m.value), Some(1024.0 * 1024.0 * 1024.0 * 100.0));
    }

    #[test]
    fn test_collect_drive_count_metrics() {
        let stats = DriveCountStats {
            offline_count: 2,
            online_count: 8,
            total_count: 10,
        };

        let metrics = collect_drive_count_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 3);

        // Verify offline count
        let offline_name = DRIVE_OFFLINE_COUNT_MD.get_full_metric_name();
        let offline = metrics.iter().find(|m| m.name == offline_name);
        assert!(offline.is_some());
        assert_eq!(offline.map(|m| m.value), Some(2.0));
    }
}
