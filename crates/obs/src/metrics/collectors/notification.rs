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

//! Notification metrics collector.
//!
//! Collects notification system metrics including events sent,
//! errors, and skipped events.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::cluster_notification::{
    NOTIFICATION_CURRENT_SEND_IN_PROGRESS_BY_SERVER_MD, NOTIFICATION_CURRENT_SEND_IN_PROGRESS_MD,
    NOTIFICATION_EVENTS_ERRORS_TOTAL_BY_SERVER_MD, NOTIFICATION_EVENTS_ERRORS_TOTAL_MD,
    NOTIFICATION_EVENTS_SENT_TOTAL_BY_SERVER_MD, NOTIFICATION_EVENTS_SENT_TOTAL_MD,
    NOTIFICATION_EVENTS_SKIPPED_TOTAL_BY_SERVER_MD, NOTIFICATION_EVENTS_SKIPPED_TOTAL_MD, SERVER,
};
use std::borrow::Cow;

/// Notification statistics.
#[derive(Debug, Clone, Default)]
pub struct NotificationStats {
    /// Number of concurrent send operations in progress
    pub current_send_in_progress: u64,
    /// Total number of events that encountered errors
    pub events_errors_total: u64,
    /// Total number of events successfully sent
    pub events_sent_total: u64,
    /// Total number of events skipped
    pub events_skipped_total: u64,
}

/// Collects notification metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::cluster_notification` module.
/// Returns a vector of Prometheus metrics for notification statistics.
pub fn collect_notification_metrics(stats: &NotificationStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&NOTIFICATION_CURRENT_SEND_IN_PROGRESS_MD, stats.current_send_in_progress as f64),
        PrometheusMetric::from_descriptor(&NOTIFICATION_EVENTS_ERRORS_TOTAL_MD, stats.events_errors_total as f64),
        PrometheusMetric::from_descriptor(&NOTIFICATION_EVENTS_SENT_TOTAL_MD, stats.events_sent_total as f64),
        PrometheusMetric::from_descriptor(&NOTIFICATION_EVENTS_SKIPPED_TOTAL_MD, stats.events_skipped_total as f64),
    ]
}

/// Collects the legacy aggregate metrics and node-local runtime siblings.
pub(crate) fn collect_notification_runtime_metrics(stats: &NotificationStats, server: &str) -> Vec<PrometheusMetric> {
    let mut metrics = collect_notification_metrics(stats);
    if server.is_empty() {
        return metrics;
    }

    let server_label: Cow<'static, str> = Cow::Owned(server.to_string());
    metrics.extend([
        PrometheusMetric::from_descriptor(
            &NOTIFICATION_CURRENT_SEND_IN_PROGRESS_BY_SERVER_MD,
            stats.current_send_in_progress as f64,
        )
        .with_label(SERVER, server_label.clone()),
        PrometheusMetric::from_descriptor(&NOTIFICATION_EVENTS_ERRORS_TOTAL_BY_SERVER_MD, stats.events_errors_total as f64)
            .with_label(SERVER, server_label.clone()),
        PrometheusMetric::from_descriptor(&NOTIFICATION_EVENTS_SENT_TOTAL_BY_SERVER_MD, stats.events_sent_total as f64)
            .with_label(SERVER, server_label.clone()),
        PrometheusMetric::from_descriptor(&NOTIFICATION_EVENTS_SKIPPED_TOTAL_BY_SERVER_MD, stats.events_skipped_total as f64)
            .with_label(SERVER, server_label),
    ]);
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_notification_metrics() {
        let stats = NotificationStats {
            current_send_in_progress: 5,
            events_errors_total: 10,
            events_sent_total: 10000,
            events_skipped_total: 50,
        };

        let metrics = collect_notification_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 4);

        let sent = metrics.iter().find(|m| m.value == 10000.0);
        assert!(sent.is_some());

        let errors = metrics.iter().find(|m| m.value == 10.0);
        assert!(errors.is_some());
    }

    #[test]
    fn test_collect_notification_metrics_default() {
        let stats = NotificationStats::default();
        let metrics = collect_notification_metrics(&stats);

        assert_eq!(metrics.len(), 4);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }

    #[test]
    fn runtime_metrics_keep_aggregate_and_add_server_siblings() {
        let stats = NotificationStats {
            current_send_in_progress: 5,
            events_errors_total: 10,
            events_sent_total: 100,
            events_skipped_total: 2,
        };

        let metrics = collect_notification_runtime_metrics(&stats, "node1:9000");
        assert_eq!(metrics.len(), 8);
        assert_eq!(metrics.iter().filter(|metric| metric.labels.is_empty()).count(), 4);
        assert_eq!(metrics.iter().filter(|metric| metric.labels.len() == 1).count(), 4);
        assert!(metrics.iter().filter(|metric| metric.labels.len() == 1).all(|metric| {
            metric
                .labels
                .iter()
                .any(|(name, value)| *name == SERVER && value == "node1:9000")
        }));
    }

    #[test]
    fn runtime_metrics_do_not_publish_empty_server_series() {
        let metrics = collect_notification_runtime_metrics(&NotificationStats::default(), "");
        assert_eq!(metrics.len(), 4);
        assert!(metrics.iter().all(|metric| metric.labels.is_empty()));
    }
}
