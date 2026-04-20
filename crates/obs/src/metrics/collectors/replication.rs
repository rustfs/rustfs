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

//! Replication metrics collector.
//!
//! Collects cluster-wide replication metrics including queue stats,
//! data transfer rates, and worker information.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::replication::*;

/// Replication statistics.
#[derive(Debug, Clone, Default)]
pub struct ReplicationStats {
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
    /// Objects in replication backlog in the last 5 minutes
    pub recent_backlog_count: u64,
}

/// Collects replication metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for replication statistics.
pub fn collect_replication_metrics(stats: &ReplicationStats) -> Vec<PrometheusMetric> {
    vec![
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_replication_metrics() {
        let stats = ReplicationStats {
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

        let metrics = collect_replication_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 9);

        // Verify active workers
        let active_name = REPLICATION_CURRENT_ACTIVE_WORKERS_MD.get_full_metric_name();
        let active = metrics.iter().find(|m| m.name == active_name);
        assert!(active.is_some());
        assert_eq!(active.map(|m| m.value), Some(10.0));
    }

    #[test]
    fn test_collect_replication_metrics_default() {
        let stats = ReplicationStats::default();
        let metrics = collect_replication_metrics(&stats);

        assert_eq!(metrics.len(), 9);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
