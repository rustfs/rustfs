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
    /// Number of unsent messages in queue
    pub queue_length: u64,
    /// Total number of messages sent
    pub total_messages: u64,
}

/// Collects audit metrics from the provided audit target statistics.
///
/// Uses the metric descriptors from `metrics_type::audit` module.
/// Returns a vector of Prometheus metrics for audit statistics.
pub fn collect_audit_metrics(stats: &[AuditTargetStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(stats.len() * 3);
    for stat in stats {
        let target_id_label: Cow<'static, str> = Cow::Owned(stat.target_id.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_FAILED_MESSAGES_MD, stat.failed_messages as f64)
                .with_label("target_id", target_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TARGET_QUEUE_LENGTH_MD, stat.queue_length as f64)
                .with_label("target_id", target_id_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TOTAL_MESSAGES_MD, stat.total_messages as f64)
                .with_label("target_id", target_id_label),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_audit_metrics() {
        let stats = vec![
            AuditTargetStats {
                target_id: "target-1".to_string(),
                failed_messages: 5,
                queue_length: 10,
                total_messages: 1000,
            },
            AuditTargetStats {
                target_id: "target-2".to_string(),
                failed_messages: 2,
                queue_length: 5,
                total_messages: 500,
            },
        ];

        let metrics = collect_audit_metrics(&stats);

        assert_eq!(metrics.len(), 6); // 2 targets * 3 metrics each

        let failed = metrics
            .iter()
            .find(|m| m.value == 5.0 && m.labels.iter().any(|(k, v)| *k == "target_id" && v == "target-1"));
        assert!(failed.is_some());
    }

    #[test]
    fn test_collect_audit_metrics_empty() {
        let stats: Vec<AuditTargetStats> = vec![];
        let metrics = collect_audit_metrics(&stats);
        assert!(metrics.is_empty());
    }
}
