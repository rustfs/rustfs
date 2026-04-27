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

//! Notification metrics collector.
//!
//! Collects notification system metrics including events sent,
//! errors, and skipped events.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::cluster_notification::{
    NOTIFICATION_CURRENT_SEND_IN_PROGRESS_MD, NOTIFICATION_EVENTS_ERRORS_TOTAL_MD, NOTIFICATION_EVENTS_SENT_TOTAL_MD,
    NOTIFICATION_EVENTS_SKIPPED_TOTAL_MD,
};

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
}
