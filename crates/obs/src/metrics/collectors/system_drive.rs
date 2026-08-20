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

//! System drive metrics collector.
//!
//! Collects detailed drive/disk metrics including capacity, I/O statistics,
//! error counts, and health status.
//!
//! This module provides both system-level and process-level disk metrics,
//! with process-level metrics migrated from `rustfs-obs::system`.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::system_drive::*;
use crate::metrics::schema::system_process::{DIRECTION_LABEL, PROCESS_DISK_IO_MD};
use std::borrow::Cow;

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
    /// Capacity observation state: live, stale, or missing
    pub capacity_observation_state: &'static str,
    /// Age in seconds of the current capacity observation
    pub capacity_observation_age_seconds: u64,
    /// Used inodes when the platform provides a real inode sample
    pub used_inodes: Option<u64>,
    /// Free inodes when the platform provides a real inode sample
    pub free_inodes: Option<u64>,
    /// Total inodes when the platform provides a real inode sample
    pub total_inodes: Option<u64>,
    /// Total timeout errors when backed by a real error counter
    pub timeout_errors_total: Option<u64>,
    /// Total I/O errors when backed by a real error counter
    pub io_errors_total: Option<u64>,
    /// Total availability errors when backed by a real error counter
    pub availability_errors_total: Option<u64>,
    /// Number of I/O operations waiting when backed by a real queue sample
    pub waiting_io: Option<u64>,
    /// API latency in microseconds when backed by a real latency sample
    pub api_latency_micros: Option<u64>,
    /// Health status (1=healthy, 0=unhealthy)
    pub health: u8,
    /// Total successful write operations when backed by a real disk metric.
    pub writes_total: Option<u64>,
    /// Total successful delete operations when backed by a real disk metric.
    pub deletes_total: Option<u64>,
    /// Reads per second when backed by a real iostat sample
    pub reads_per_sec: Option<f64>,
    /// Kilobytes read per second when backed by a real iostat sample
    pub reads_kb_per_sec: Option<f64>,
    /// Average read await time when backed by a real iostat sample
    pub reads_await: Option<f64>,
    /// Writes per second when backed by a real iostat sample
    pub writes_per_sec: Option<f64>,
    /// Kilobytes written per second when backed by a real iostat sample
    pub writes_kb_per_sec: Option<f64>,
    /// Average write await time when backed by a real iostat sample
    pub writes_await: Option<f64>,
    /// Drive percent utilization when backed by a real iostat sample
    pub perc_util: Option<f64>,
}

/// Detailed drive statistics with runtime topology and per-operation dimensions.
#[derive(Debug, Clone, Default)]
pub(crate) struct DriveRuntimeDetailedStats {
    pub(crate) stats: DriveDetailedStats,
    pub(crate) pool_index: Option<String>,
    pub(crate) set_index: Option<String>,
    pub(crate) drive_index: Option<String>,
    pub(crate) disk_id: Option<String>,
    pub(crate) runtime_state: Option<String>,
    pub(crate) healing: bool,
    pub(crate) scanning: bool,
    pub(crate) offline_duration_seconds: Option<u64>,
    /// Drive API calls by operation
    pub(crate) api_calls: Vec<(String, u64)>,
    /// Last-minute API latency by operation, in microseconds
    pub(crate) api_latency_by_api_micros: Vec<(String, u64)>,
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
    let runtime_stats = stats
        .iter()
        .cloned()
        .map(|stats| DriveRuntimeDetailedStats {
            stats,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    collect_drive_runtime_detailed_metrics(&runtime_stats)
}

pub(crate) fn collect_drive_runtime_detailed_metrics(stats: &[DriveRuntimeDetailedStats]) -> Vec<PrometheusMetric> {
    const DRIVE_RUNTIME_STATES: [&str; 5] = ["online", "offline", "returning", "suspect", "unknown"];

    fn topology_labels(stat: &DriveRuntimeDetailedStats) -> Option<[Cow<'static, str>; 5]> {
        Some([
            Cow::Owned(stat.stats.server.clone()),
            Cow::Owned(stat.stats.drive.clone()),
            Cow::Owned(stat.pool_index.as_ref()?.clone()),
            Cow::Owned(stat.set_index.as_ref()?.clone()),
            Cow::Owned(stat.drive_index.as_ref()?.clone()),
        ])
    }

    fn has_topology_labels(stat: &DriveRuntimeDetailedStats) -> bool {
        stat.pool_index.is_some() && stat.set_index.is_some() && stat.drive_index.is_some()
    }

    fn normalized_runtime_state(runtime_state: &str) -> &str {
        DRIVE_RUNTIME_STATES
            .iter()
            .copied()
            .find(|state| state.eq_ignore_ascii_case(runtime_state))
            .unwrap_or("unknown")
    }

    fn push_topology_metric(
        metrics: &mut Vec<PrometheusMetric>,
        descriptor: &'static crate::metrics::schema::MetricDescriptor,
        value: f64,
        labels: &[Cow<'static, str>; 5],
    ) {
        metrics.push(
            PrometheusMetric::from_descriptor(descriptor, value)
                .with_label(SERVER_LABEL, labels[0].clone())
                .with_label(DRIVE_LABEL, labels[1].clone())
                .with_label(POOL_INDEX_LABEL, labels[2].clone())
                .with_label(SET_INDEX_LABEL, labels[3].clone())
                .with_label(DRIVE_INDEX_LABEL, labels[4].clone()),
        );
    }

    fn push_drive_metric(
        metrics: &mut Vec<PrometheusMetric>,
        descriptor: &'static crate::metrics::schema::MetricDescriptor,
        value: f64,
        server_label: &str,
        drive_label: &str,
    ) {
        metrics.push(
            PrometheusMetric::from_descriptor(descriptor, value)
                .with_label_owned(SERVER_LABEL, server_label.to_string())
                .with_label_owned(DRIVE_LABEL, drive_label.to_string()),
        );
    }

    let metric_capacity = stats
        .iter()
        .map(|stat| {
            let api_metrics = if has_topology_labels(stat) {
                stat.api_calls.len() + stat.api_latency_by_api_micros.len()
            } else {
                0
            };
            31 + api_metrics
        })
        .sum();
    let mut metrics = Vec::with_capacity(metric_capacity);

    for stat in stats {
        let server_label = stat.stats.server.as_str();
        let drive_label = stat.stats.drive.as_str();
        let topology_labels = topology_labels(stat);

        push_drive_metric(
            &mut metrics,
            &DRIVE_TOTAL_BYTES_MD,
            stat.stats.total_bytes as f64,
            server_label,
            drive_label,
        );
        push_drive_metric(
            &mut metrics,
            &DRIVE_USED_BYTES_MD,
            stat.stats.used_bytes as f64,
            server_label,
            drive_label,
        );
        push_drive_metric(
            &mut metrics,
            &DRIVE_FREE_BYTES_MD,
            stat.stats.free_bytes as f64,
            server_label,
            drive_label,
        );
        push_drive_metric(
            &mut metrics,
            &DRIVE_CAPACITY_OBSERVATION_AGE_SECONDS_MD,
            stat.stats.capacity_observation_age_seconds as f64,
            server_label,
            drive_label,
        );
        for state in ["live", "stale", "missing"] {
            metrics.push(
                PrometheusMetric::from_descriptor(
                    &DRIVE_CAPACITY_OBSERVATION_STATE_MD,
                    if state == stat.stats.capacity_observation_state {
                        1.0
                    } else {
                        0.0
                    },
                )
                .with_label_owned(SERVER_LABEL, server_label.to_string())
                .with_label_owned(DRIVE_LABEL, drive_label.to_string())
                .with_label_owned("state", state.to_string()),
            );
        }
        if let Some(value) = stat.stats.used_inodes {
            push_drive_metric(&mut metrics, &DRIVE_USED_INODES_MD, value as f64, server_label, drive_label);
        }
        if let Some(value) = stat.stats.free_inodes {
            push_drive_metric(&mut metrics, &DRIVE_FREE_INODES_MD, value as f64, server_label, drive_label);
        }
        if let Some(value) = stat.stats.total_inodes {
            push_drive_metric(&mut metrics, &DRIVE_TOTAL_INODES_MD, value as f64, server_label, drive_label);
        }
        if let Some(value) = stat.stats.timeout_errors_total {
            push_drive_metric(&mut metrics, &DRIVE_TIMEOUT_ERRORS_MD, value as f64, server_label, drive_label);
        }
        if let Some(value) = stat.stats.io_errors_total {
            push_drive_metric(&mut metrics, &DRIVE_IO_ERRORS_MD, value as f64, server_label, drive_label);
        }
        if let Some(value) = stat.stats.availability_errors_total {
            push_drive_metric(&mut metrics, &DRIVE_AVAILABILITY_ERRORS_MD, value as f64, server_label, drive_label);
        }
        if let Some(value) = stat.stats.waiting_io {
            push_drive_metric(&mut metrics, &DRIVE_WAITING_IO_MD, value as f64, server_label, drive_label);
        }
        if let Some(value) = stat.stats.api_latency_micros {
            push_drive_metric(&mut metrics, &DRIVE_API_LATENCY_MD, value as f64, server_label, drive_label);
        }
        push_drive_metric(&mut metrics, &DRIVE_HEALTH_MD, stat.stats.health as f64, server_label, drive_label);
        if let Some(value) = stat.stats.reads_per_sec {
            push_drive_metric(&mut metrics, &DRIVE_READS_PER_SEC_MD, value, server_label, drive_label);
        }
        if let Some(value) = stat.stats.reads_kb_per_sec {
            push_drive_metric(&mut metrics, &DRIVE_READS_KB_PER_SEC_MD, value, server_label, drive_label);
        }
        if let Some(value) = stat.stats.reads_await {
            push_drive_metric(&mut metrics, &DRIVE_READS_AWAIT_MD, value, server_label, drive_label);
        }
        if let Some(value) = stat.stats.writes_per_sec {
            push_drive_metric(&mut metrics, &DRIVE_WRITES_PER_SEC_MD, value, server_label, drive_label);
        }
        if let Some(value) = stat.stats.writes_kb_per_sec {
            push_drive_metric(&mut metrics, &DRIVE_WRITES_KB_PER_SEC_MD, value, server_label, drive_label);
        }
        if let Some(value) = stat.stats.writes_await {
            push_drive_metric(&mut metrics, &DRIVE_WRITES_AWAIT_MD, value, server_label, drive_label);
        }
        if let Some(value) = stat.stats.perc_util {
            push_drive_metric(&mut metrics, &DRIVE_PERC_UTIL_MD, value, server_label, drive_label);
        }
        if let Some(value) = stat.stats.writes_total {
            push_drive_metric(&mut metrics, &DRIVE_WRITES_TOTAL_MD, value as f64, server_label, drive_label);
        }
        if let Some(value) = stat.stats.deletes_total {
            push_drive_metric(&mut metrics, &DRIVE_DELETES_TOTAL_MD, value as f64, server_label, drive_label);
        }
        if let Some(labels) = &topology_labels {
            if let Some(disk_id) = stat.disk_id.as_ref().filter(|disk_id| !disk_id.is_empty()) {
                metrics.push(
                    PrometheusMetric::from_descriptor(&DRIVE_INFO_MD, 1.0)
                        .with_label(SERVER_LABEL, labels[0].clone())
                        .with_label(DRIVE_LABEL, labels[1].clone())
                        .with_label(POOL_INDEX_LABEL, labels[2].clone())
                        .with_label(SET_INDEX_LABEL, labels[3].clone())
                        .with_label(DRIVE_INDEX_LABEL, labels[4].clone())
                        .with_label_owned(DISK_ID_LABEL, disk_id.clone()),
                );
            }
            if let Some(runtime_state) = stat.runtime_state.as_ref().filter(|state| !state.is_empty()) {
                let runtime_state = normalized_runtime_state(runtime_state);
                for state in DRIVE_RUNTIME_STATES {
                    metrics.push(
                        PrometheusMetric::from_descriptor(
                            &DRIVE_RUNTIME_STATE_MD,
                            if state == runtime_state { 1.0 } else { 0.0 },
                        )
                        .with_label(SERVER_LABEL, labels[0].clone())
                        .with_label(DRIVE_LABEL, labels[1].clone())
                        .with_label(POOL_INDEX_LABEL, labels[2].clone())
                        .with_label(SET_INDEX_LABEL, labels[3].clone())
                        .with_label(DRIVE_INDEX_LABEL, labels[4].clone())
                        .with_label(STATE_LABEL, state),
                    );
                }
            }
            push_topology_metric(&mut metrics, &DRIVE_HEALING_MD, if stat.healing { 1.0 } else { 0.0 }, labels);
            push_topology_metric(&mut metrics, &DRIVE_SCANNING_MD, if stat.scanning { 1.0 } else { 0.0 }, labels);
            push_topology_metric(
                &mut metrics,
                &DRIVE_OFFLINE_DURATION_SECONDS_MD,
                stat.offline_duration_seconds.unwrap_or(0) as f64,
                labels,
            );
            for (api, value) in &stat.api_calls {
                metrics.push(
                    PrometheusMetric::from_descriptor(&DRIVE_API_CALLS_MD, *value as f64)
                        .with_label(SERVER_LABEL, labels[0].clone())
                        .with_label(DRIVE_LABEL, labels[1].clone())
                        .with_label(POOL_INDEX_LABEL, labels[2].clone())
                        .with_label(SET_INDEX_LABEL, labels[3].clone())
                        .with_label(DRIVE_INDEX_LABEL, labels[4].clone())
                        .with_label_owned(API_LABEL, api.clone()),
                );
            }
            for (api, value) in &stat.api_latency_by_api_micros {
                metrics.push(
                    PrometheusMetric::from_descriptor(&DRIVE_API_LATENCY_BY_API_MD, *value as f64)
                        .with_label(SERVER_LABEL, labels[0].clone())
                        .with_label(DRIVE_LABEL, labels[1].clone())
                        .with_label(POOL_INDEX_LABEL, labels[2].clone())
                        .with_label(SET_INDEX_LABEL, labels[3].clone())
                        .with_label(DRIVE_INDEX_LABEL, labels[4].clone())
                        .with_label_owned(API_LABEL, api.clone()),
                );
            }
        }
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

/// Process disk I/O statistics.
///
/// Contains disk I/O metrics for a specific process.
#[derive(Debug, Clone, Default)]
pub struct ProcessDiskStats {
    /// Bytes read from disk
    pub read_bytes: u64,
    /// Bytes written to disk
    pub written_bytes: u64,
}

/// Collects process disk I/O metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for process disk I/O statistics.
/// Each metric includes a `direction` label ("read" or "write").
///
/// # Arguments
///
/// * `stats` - Process disk I/O statistics
/// * `labels` - Optional additional labels (e.g., process attributes)
pub fn collect_process_disk_metrics(
    stats: &ProcessDiskStats,
    labels: Option<&[(&'static str, Cow<'static, str>)]>,
) -> Vec<PrometheusMetric> {
    let mut read_metric = PrometheusMetric::from_descriptor(&PROCESS_DISK_IO_MD, stats.read_bytes as f64);
    let mut write_metric = PrometheusMetric::from_descriptor(&PROCESS_DISK_IO_MD, stats.written_bytes as f64);

    read_metric.labels.push((DIRECTION_LABEL, Cow::Borrowed("read")));
    write_metric.labels.push((DIRECTION_LABEL, Cow::Borrowed("write")));

    if let Some(l) = labels {
        read_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
        write_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
    }

    vec![read_metric, write_metric]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;
    use crate::metrics::schema::system_process::{PROCESS_EXECUTABLE_NAME_LABEL, PROCESS_PID_LABEL};
    use std::collections::BTreeSet;

    fn assert_metric_label_keys(
        metrics: &[PrometheusMetric],
        descriptor: &'static crate::metrics::schema::MetricDescriptor,
        value: f64,
        expected_keys: &[&str],
    ) {
        let metric_name = descriptor.get_full_metric_name();
        let metric = metrics
            .iter()
            .find(|metric| metric.name == metric_name && metric.value == value)
            .expect("expected metric with matching descriptor and value");
        let actual: BTreeSet<&str> = metric.labels.iter().map(|(key, _)| *key).collect();
        let expected: BTreeSet<&str> = expected_keys.iter().copied().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_collect_drive_detailed_metrics() {
        let stats = vec![DriveRuntimeDetailedStats {
            pool_index: Some("0".to_string()),
            set_index: Some("1".to_string()),
            drive_index: Some("2".to_string()),
            disk_id: Some("disk-uuid-1".to_string()),
            runtime_state: Some("online".to_string()),
            healing: true,
            scanning: false,
            offline_duration_seconds: Some(0),
            api_calls: vec![("read".to_string(), 7)],
            api_latency_by_api_micros: vec![("read".to_string(), 2500)],
            stats: DriveDetailedStats {
                server: "node1:9000".to_string(),
                drive: "/data/disk1".to_string(),
                total_bytes: 1024 * 1024 * 1024 * 100, // 100 GB
                used_bytes: 1024 * 1024 * 1024 * 50,   // 50 GB
                free_bytes: 1024 * 1024 * 1024 * 50,   // 50 GB
                capacity_observation_state: "live",
                capacity_observation_age_seconds: 0,
                used_inodes: Some(100000),
                free_inodes: Some(900000),
                total_inodes: Some(1000000),
                timeout_errors_total: Some(5),
                io_errors_total: Some(10),
                availability_errors_total: Some(2),
                waiting_io: Some(3),
                api_latency_micros: Some(1500),
                health: 1,
                writes_total: Some(11),
                deletes_total: Some(4),
                reads_per_sec: Some(100.0),
                reads_kb_per_sec: Some(1024.0),
                reads_await: Some(5.5),
                writes_per_sec: Some(50.0),
                writes_kb_per_sec: Some(512.0),
                writes_await: Some(10.2),
                perc_util: Some(75.5),
            },
        }];

        let metrics = collect_drive_runtime_detailed_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 36);

        // Verify total bytes metric
        let total_bytes_name = DRIVE_TOTAL_BYTES_MD.get_full_metric_name();
        let total_bytes = metrics.iter().find(|m| m.name == total_bytes_name);
        assert!(total_bytes.is_some());
        assert_eq!(total_bytes.map(|m| m.value), Some(1024.0 * 1024.0 * 1024.0 * 100.0));
        assert_metric_label_keys(
            &metrics,
            &DRIVE_TOTAL_BYTES_MD,
            1024.0 * 1024.0 * 1024.0 * 100.0,
            &[SERVER_LABEL, DRIVE_LABEL],
        );
        assert_metric_label_keys(&metrics, &DRIVE_API_LATENCY_MD, 1500.0, &[SERVER_LABEL, DRIVE_LABEL]);
        assert_metric_label_keys(&metrics, &DRIVE_CAPACITY_OBSERVATION_STATE_MD, 1.0, &[SERVER_LABEL, DRIVE_LABEL, "state"]);
        assert_metric_label_keys(
            &metrics,
            &DRIVE_INFO_MD,
            1.0,
            &[
                SERVER_LABEL,
                DRIVE_LABEL,
                POOL_INDEX_LABEL,
                SET_INDEX_LABEL,
                DRIVE_INDEX_LABEL,
                DISK_ID_LABEL,
            ],
        );
        assert_metric_label_keys(
            &metrics,
            &DRIVE_API_CALLS_MD,
            7.0,
            &[
                SERVER_LABEL,
                DRIVE_LABEL,
                POOL_INDEX_LABEL,
                SET_INDEX_LABEL,
                DRIVE_INDEX_LABEL,
                API_LABEL,
            ],
        );
        assert_metric_label_keys(&metrics, &DRIVE_WRITES_TOTAL_MD, 11.0, &[SERVER_LABEL, DRIVE_LABEL]);
        assert_metric_label_keys(&metrics, &DRIVE_DELETES_TOTAL_MD, 4.0, &[SERVER_LABEL, DRIVE_LABEL]);
    }

    #[test]
    fn test_collect_drive_detailed_metrics_skips_unimplemented_placeholders() {
        let stats = vec![DriveDetailedStats {
            server: "node1:9000".to_string(),
            drive: "/data/disk1".to_string(),
            total_bytes: 1024,
            used_bytes: 512,
            free_bytes: 512,
            capacity_observation_state: "live",
            capacity_observation_age_seconds: 0,
            used_inodes: None,
            free_inodes: None,
            total_inodes: None,
            timeout_errors_total: None,
            io_errors_total: None,
            availability_errors_total: None,
            waiting_io: None,
            api_latency_micros: None,
            health: 1,
            writes_total: None,
            deletes_total: None,
            reads_per_sec: None,
            reads_kb_per_sec: None,
            reads_await: None,
            writes_per_sec: None,
            writes_kb_per_sec: None,
            writes_await: None,
            perc_util: None,
        }];

        let metrics = collect_drive_detailed_metrics(&stats);

        assert_eq!(metrics.len(), 8);
        assert!(
            metrics
                .iter()
                .all(|metric| metric.name != DRIVE_PERC_UTIL_MD.get_full_metric_name())
        );
        assert!(
            metrics
                .iter()
                .all(|metric| metric.name != DRIVE_USED_INODES_MD.get_full_metric_name())
        );
        assert!(
            metrics
                .iter()
                .all(|metric| metric.name != DRIVE_IO_ERRORS_MD.get_full_metric_name())
        );
    }

    #[test]
    fn drive_runtime_state_metrics_keep_suspect_state_active() {
        let stats = vec![DriveRuntimeDetailedStats {
            pool_index: Some("0".to_string()),
            set_index: Some("1".to_string()),
            drive_index: Some("2".to_string()),
            runtime_state: Some("suspect".to_string()),
            stats: DriveDetailedStats {
                server: "node1:9000".to_string(),
                drive: "/data/disk1".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }];

        let metrics = collect_drive_runtime_detailed_metrics(&stats);
        let state_metrics = metrics
            .iter()
            .filter(|metric| metric.name == DRIVE_RUNTIME_STATE_MD.get_full_metric_name())
            .collect::<Vec<_>>();

        assert_eq!(state_metrics.len(), 5);
        assert!(state_metrics.iter().any(|metric| {
            metric.value == 1.0
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == STATE_LABEL && value.as_ref() == "suspect")
        }));
        assert!(state_metrics.iter().filter(|metric| metric.value == 1.0).all(|metric| {
            metric
                .labels
                .iter()
                .any(|(name, value)| *name == STATE_LABEL && value.as_ref() == "suspect")
        }));
    }

    #[test]
    fn drive_offline_duration_zeroes_recovered_topology_drive() {
        let stats = vec![DriveRuntimeDetailedStats {
            pool_index: Some("0".to_string()),
            set_index: Some("1".to_string()),
            drive_index: Some("2".to_string()),
            runtime_state: Some("online".to_string()),
            offline_duration_seconds: None,
            stats: DriveDetailedStats {
                server: "node1:9000".to_string(),
                drive: "/data/disk1".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }];

        let metrics = collect_drive_runtime_detailed_metrics(&stats);

        assert!(metrics.iter().any(|metric| {
            metric.name == DRIVE_OFFLINE_DURATION_SECONDS_MD.get_full_metric_name()
                && metric.value == 0.0
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == SERVER_LABEL && value == "node1:9000")
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == DRIVE_INDEX_LABEL && value == "2")
        }));
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

    #[test]
    fn test_collect_process_disk_metrics_with_node_and_process_labels() {
        let stats = ProcessDiskStats {
            read_bytes: 1024,
            written_bytes: 2048,
        };
        let labels = vec![
            (SERVER_LABEL, Cow::Borrowed("node1:9000")),
            (PROCESS_PID_LABEL, Cow::Borrowed("12345")),
            (PROCESS_EXECUTABLE_NAME_LABEL, Cow::Borrowed("rustfs")),
        ];

        let metrics = collect_process_disk_metrics(&stats, Some(&labels));

        assert_eq!(metrics.len(), 2);
        assert_metric_label_keys(
            &metrics,
            &PROCESS_DISK_IO_MD,
            1024.0,
            &[
                DIRECTION_LABEL,
                SERVER_LABEL,
                PROCESS_PID_LABEL,
                PROCESS_EXECUTABLE_NAME_LABEL,
            ],
        );
    }
}
