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

//! Replication metrics collector.
//!
//! Collects cluster-wide replication metrics including queue stats,
//! data transfer rates, and worker information.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::replication::*;

/// Replication statistics.
#[derive(Debug, Clone, Default)]
pub struct ReplicationMetricsSnapshot {
    /// Average number of active replication workers
    pub average_active_workers: f64,
    /// Average queued bytes since server start
    pub average_queued_bytes: i64,
    /// Average queued objects since server start
    pub average_queued_count: i64,
    /// Average data transfer rate in bytes/sec
    pub average_data_transfer_rate: f64,
    /// Number of active replication workers
    pub active_workers: u64,
    /// Current data transfer rate in bytes/sec
    pub current_data_transfer_rate: f64,
    /// Bytes queued in the last full minute
    pub last_minute_queued_bytes: u64,
    /// Objects queued in the last full minute
    pub last_minute_queued_count: u64,
    /// Maximum active workers seen since server start
    pub max_active_workers: u64,
    /// Maximum bytes queued since server start
    pub max_queued_bytes: u64,
    /// Maximum objects queued since server start
    pub max_queued_count: u64,
    /// Maximum data transfer rate seen since server start
    pub max_data_transfer_rate: f64,
    /// Objects currently in replication backlog
    pub recent_backlog_count: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ReplicationRuntimeStats {
    pub(crate) server: String,
    pub(crate) stats: ReplicationMetricsSnapshot,
}

/// Collects replication metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for replication statistics.
pub fn collect_replication_metrics(stats: &ReplicationMetricsSnapshot) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&REPLICATION_AVERAGE_ACTIVE_WORKERS_MD, stats.average_active_workers),
        PrometheusMetric::from_descriptor(&REPLICATION_AVERAGE_QUEUED_BYTES_MD, stats.average_queued_bytes as f64),
        PrometheusMetric::from_descriptor(&REPLICATION_AVERAGE_QUEUED_COUNT_MD, stats.average_queued_count as f64),
        PrometheusMetric::from_descriptor(&REPLICATION_AVERAGE_DATA_TRANSFER_RATE_MD, stats.average_data_transfer_rate),
        PrometheusMetric::from_descriptor(&REPLICATION_CURRENT_ACTIVE_WORKERS_MD, stats.active_workers as f64),
        PrometheusMetric::from_descriptor(&REPLICATION_CURRENT_DATA_TRANSFER_RATE_MD, stats.current_data_transfer_rate),
        PrometheusMetric::from_descriptor(&REPLICATION_LAST_MINUTE_QUEUED_BYTES_MD, stats.last_minute_queued_bytes as f64),
        PrometheusMetric::from_descriptor(&REPLICATION_LAST_MINUTE_QUEUED_COUNT_MD, stats.last_minute_queued_count as f64),
        PrometheusMetric::from_descriptor(&REPLICATION_MAX_ACTIVE_WORKERS_MD, stats.max_active_workers as f64),
        PrometheusMetric::from_descriptor(&REPLICATION_MAX_QUEUED_BYTES_MD, stats.max_queued_bytes as f64),
        PrometheusMetric::from_descriptor(&REPLICATION_MAX_QUEUED_COUNT_MD, stats.max_queued_count as f64),
        PrometheusMetric::from_descriptor(&REPLICATION_MAX_DATA_TRANSFER_RATE_MD, stats.max_data_transfer_rate),
        PrometheusMetric::from_descriptor(&REPLICATION_RECENT_BACKLOG_COUNT_MD, stats.recent_backlog_count as f64),
    ]
}

pub(crate) fn collect_replication_runtime_metrics(runtime: &ReplicationRuntimeStats) -> Vec<PrometheusMetric> {
    let stats = &runtime.stats;
    let mut metrics = collect_replication_metrics(stats);
    metrics.extend([
        PrometheusMetric::from_descriptor(&REPLICATION_AVERAGE_ACTIVE_WORKERS_BY_SERVER_MD, stats.average_active_workers)
            .with_label_owned(SERVER_LABEL, runtime.server.clone()),
        PrometheusMetric::from_descriptor(&REPLICATION_AVERAGE_QUEUED_BYTES_BY_SERVER_MD, stats.average_queued_bytes as f64)
            .with_label_owned(SERVER_LABEL, runtime.server.clone()),
        PrometheusMetric::from_descriptor(&REPLICATION_AVERAGE_QUEUED_COUNT_BY_SERVER_MD, stats.average_queued_count as f64)
            .with_label_owned(SERVER_LABEL, runtime.server.clone()),
        PrometheusMetric::from_descriptor(&REPLICATION_CURRENT_ACTIVE_WORKERS_BY_SERVER_MD, stats.active_workers as f64)
            .with_label_owned(SERVER_LABEL, runtime.server.clone()),
        PrometheusMetric::from_descriptor(&REPLICATION_CURRENT_DATA_TRANSFER_RATE_BY_SERVER_MD, stats.current_data_transfer_rate)
            .with_label_owned(SERVER_LABEL, runtime.server.clone()),
        PrometheusMetric::from_descriptor(
            &REPLICATION_LAST_MINUTE_QUEUED_BYTES_BY_SERVER_MD,
            stats.last_minute_queued_bytes as f64,
        )
        .with_label_owned(SERVER_LABEL, runtime.server.clone()),
        PrometheusMetric::from_descriptor(
            &REPLICATION_LAST_MINUTE_QUEUED_COUNT_BY_SERVER_MD,
            stats.last_minute_queued_count as f64,
        )
        .with_label_owned(SERVER_LABEL, runtime.server.clone()),
        PrometheusMetric::from_descriptor(&REPLICATION_MAX_ACTIVE_WORKERS_BY_SERVER_MD, stats.max_active_workers as f64)
            .with_label_owned(SERVER_LABEL, runtime.server.clone()),
        PrometheusMetric::from_descriptor(&REPLICATION_MAX_QUEUED_BYTES_BY_SERVER_MD, stats.max_queued_bytes as f64)
            .with_label_owned(SERVER_LABEL, runtime.server.clone()),
        PrometheusMetric::from_descriptor(&REPLICATION_MAX_QUEUED_COUNT_BY_SERVER_MD, stats.max_queued_count as f64)
            .with_label_owned(SERVER_LABEL, runtime.server.clone()),
    ]);

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_replication_metrics() {
        let stats = ReplicationMetricsSnapshot {
            average_active_workers: 8.5,
            average_queued_bytes: 1024 * 1024 * 40,
            average_queued_count: 240,
            average_data_transfer_rate: 1024.0 * 1024.0 * 3.0,
            active_workers: 10,
            current_data_transfer_rate: 1024.0 * 1024.0 * 5.0, // 5 MB/s
            last_minute_queued_bytes: 1024 * 1024 * 100,       // 100 MB
            last_minute_queued_count: 500,
            max_active_workers: 20,
            max_queued_bytes: 1024 * 1024 * 500, // 500 MB
            max_queued_count: 2000,
            max_data_transfer_rate: 1024.0 * 1024.0 * 10.0, // 10 MB/s
            recent_backlog_count: 1500,
        };

        let metrics = collect_replication_runtime_metrics(&ReplicationRuntimeStats {
            server: "node-a:9000".to_string(),
            stats,
        });
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 23);

        // Verify active workers
        let active_name = REPLICATION_CURRENT_ACTIVE_WORKERS_MD.get_full_metric_name();
        let active = metrics.iter().find(|m| m.name == active_name);
        assert!(active.is_some());
        assert_eq!(active.map(|m| m.value), Some(10.0));

        let avg_active_name = REPLICATION_AVERAGE_ACTIVE_WORKERS_MD.get_full_metric_name();
        let avg_active = metrics.iter().find(|m| m.name == avg_active_name);
        assert_eq!(avg_active.map(|m| m.value), Some(8.5));

        let active_by_server_name = REPLICATION_CURRENT_ACTIVE_WORKERS_BY_SERVER_MD.get_full_metric_name();
        let active_by_server = metrics.iter().find(|m| m.name == active_by_server_name);
        assert_eq!(active_by_server.map(|m| m.value), Some(10.0));
        assert_eq!(
            active_by_server
                .and_then(|m| m.labels.iter().find(|(name, _)| *name == SERVER_LABEL))
                .map(|(_, value)| value.as_ref()),
            Some("node-a:9000")
        );
        assert!(
            metrics
                .iter()
                .all(|m| m.name != REPLICATION_AVERAGE_DATA_TRANSFER_RATE_BY_SERVER_MD.get_full_metric_name())
        );
        assert!(
            metrics
                .iter()
                .all(|m| m.name != REPLICATION_MAX_DATA_TRANSFER_RATE_BY_SERVER_MD.get_full_metric_name())
        );
        assert!(
            metrics
                .iter()
                .all(|m| m.name != REPLICATION_RECENT_BACKLOG_COUNT_BY_SERVER_MD.get_full_metric_name())
        );
    }

    #[test]
    fn test_collect_replication_metrics_default() {
        let stats = ReplicationMetricsSnapshot::default();
        let metrics = collect_replication_metrics(&stats);

        assert_eq!(metrics.len(), 13);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }

    #[test]
    fn replication_stats_struct_literal_keeps_legacy_fields() {
        let stats = ReplicationMetricsSnapshot {
            average_active_workers: 1.0,
            average_queued_bytes: 2,
            average_queued_count: 3,
            average_data_transfer_rate: 4.0,
            active_workers: 5,
            current_data_transfer_rate: 6.0,
            last_minute_queued_bytes: 7,
            last_minute_queued_count: 8,
            max_active_workers: 9,
            max_queued_bytes: 10,
            max_queued_count: 11,
            max_data_transfer_rate: 12.0,
            recent_backlog_count: 13,
        };

        assert_eq!(collect_replication_metrics(&stats).len(), 13);
    }
}
