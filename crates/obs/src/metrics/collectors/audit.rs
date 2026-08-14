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

//! Audit metrics collector.
//!
//! Collects audit log metrics including failed messages, queue length,
//! and total messages per target.
//!
//! This collector reuses the metric descriptors defined in `metrics_type::audit`
//! to avoid duplication of metric names, types, and help text.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::audit::*;
use std::borrow::Cow;

/// Audit target statistics for metrics collection.
#[derive(Debug, Clone, Default)]
pub struct AuditTargetStats {
    /// Target identifier
    pub target_id: String,
    /// Number of messages that failed to send
    pub failed_messages: u64,
    /// Number of messages held in the failed-events store
    pub failed_store_length: u64,
    /// Number of unsent messages in queue
    pub queue_length: u64,
    /// Total number of messages sent
    pub total_messages: u64,
}

/// Audit target statistics with runtime-local node identity.
#[derive(Debug, Clone, Default)]
pub(crate) struct AuditTargetRuntimeStats {
    pub(crate) server: String,
    pub(crate) target: AuditTargetStats,
}

/// Collects audit metrics from the provided audit target statistics.
///
/// Uses the metric descriptors from `metrics_type::audit` module.
/// Returns a vector of Prometheus metrics for audit statistics.
pub fn collect_audit_metrics(stats: &[AuditTargetStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(stats.len() * 4);
    for stat in stats {
        let target_id_label: Cow<'static, str> = Cow::Owned(stat.target_id.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_FAILED_MESSAGES_MD, stat.failed_messages as f64)
                .with_label(TARGET_ID, target_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_FAILED_STORE_LENGTH_MD, stat.failed_store_length as f64)
                .with_label(TARGET_ID, target_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TARGET_QUEUE_LENGTH_MD, stat.queue_length as f64)
                .with_label(TARGET_ID, target_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TOTAL_MESSAGES_MD, stat.total_messages as f64)
                .with_label(TARGET_ID, target_id_label),
        );
    }

    metrics
}

pub(crate) fn collect_audit_runtime_metrics(stats: &[AuditTargetRuntimeStats]) -> Vec<PrometheusMetric> {
    let legacy_stats = stats.iter().map(|stat| stat.target.clone()).collect::<Vec<_>>();
    let mut metrics = collect_audit_metrics(&legacy_stats);
    metrics.reserve(stats.len() * 4);

    for stat in stats.iter().filter(|stat| !stat.server.is_empty()) {
        let server_label: Cow<'static, str> = Cow::Owned(stat.server.clone());
        let target_id_label: Cow<'static, str> = Cow::Owned(stat.target.target_id.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_FAILED_MESSAGES_BY_SERVER_MD, stat.target.failed_messages as f64)
                .with_label(SERVER, server_label.clone())
                .with_label(TARGET_ID, target_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_FAILED_STORE_LENGTH_BY_SERVER_MD, stat.target.failed_store_length as f64)
                .with_label(SERVER, server_label.clone())
                .with_label(TARGET_ID, target_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TARGET_QUEUE_LENGTH_BY_SERVER_MD, stat.target.queue_length as f64)
                .with_label(SERVER, server_label.clone())
                .with_label(TARGET_ID, target_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TOTAL_MESSAGES_BY_SERVER_MD, stat.target.total_messages as f64)
                .with_label(SERVER, server_label)
                .with_label(TARGET_ID, target_id_label),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::schema::MetricType;

    #[test]
    fn test_collect_audit_metrics() {
        let stats = vec![
            AuditTargetStats {
                target_id: "target-1".to_string(),
                failed_messages: 5,
                failed_store_length: 3,
                queue_length: 10,
                total_messages: 1000,
            },
            AuditTargetStats {
                target_id: "target-2".to_string(),
                failed_messages: 2,
                failed_store_length: 1,
                queue_length: 5,
                total_messages: 500,
            },
        ];

        let metrics = collect_audit_metrics(&stats);

        assert_eq!(metrics.len(), 8);

        let failed = metrics
            .iter()
            .find(|m| m.value == 5.0 && m.labels.iter().any(|(k, v)| *k == "target_id" && v == "target-1"));
        assert!(failed.is_some());

        let failed_store = metrics.iter().find(|m| {
            m.value == 3.0
                && m.name == AUDIT_FAILED_STORE_LENGTH_MD.get_full_metric_name()
                && m.labels.iter().any(|(k, v)| *k == "target_id" && v == "target-1")
        });
        assert!(failed_store.is_some());

        assert!(
            metrics
                .iter()
                .all(|m| m.name != AUDIT_TARGET_QUEUE_LENGTH_BY_SERVER_MD.get_full_metric_name())
        );
    }

    #[test]
    fn collect_audit_runtime_metrics_adds_server_dimensions() {
        let stats = vec![AuditTargetRuntimeStats {
            server: "node1:9000".to_string(),
            target: AuditTargetStats {
                target_id: "target-1".to_string(),
                failed_messages: 5,
                failed_store_length: 3,
                queue_length: 10,
                total_messages: 1000,
            },
        }];

        let metrics = collect_audit_runtime_metrics(&stats);

        assert_eq!(metrics.len(), 8);
        let server_queue = metrics.iter().find(|m| {
            m.value == 10.0
                && m.name == AUDIT_TARGET_QUEUE_LENGTH_BY_SERVER_MD.get_full_metric_name()
                && m.labels.iter().any(|(k, v)| *k == SERVER && v == "node1:9000")
                && m.labels.iter().any(|(k, v)| *k == TARGET_ID && v == "target-1")
        });
        assert!(server_queue.is_some());
    }

    #[test]
    fn test_collect_audit_metrics_empty() {
        let stats: Vec<AuditTargetStats> = vec![];
        let metrics = collect_audit_metrics(&stats);
        assert!(metrics.is_empty());
    }

    #[test]
    fn audit_target_totals_are_exported_as_gauges() {
        assert_eq!(AUDIT_FAILED_MESSAGES_MD.metric_type, MetricType::Gauge);
        assert_eq!(AUDIT_FAILED_STORE_LENGTH_MD.metric_type, MetricType::Gauge);
        assert_eq!(AUDIT_TARGET_QUEUE_LENGTH_MD.metric_type, MetricType::Gauge);
        assert_eq!(AUDIT_TOTAL_MESSAGES_MD.metric_type, MetricType::Gauge);
    }
}
