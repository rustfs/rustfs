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

//! ILM (Information Lifecycle Management) metrics collector.
//!
//! Collects ILM metrics including pending tasks, active tasks,
//! and scanned versions.
//!
//! This collector reuses the metric descriptors defined in `metrics_type::ilm`
//! to avoid duplication of metric names, types, and help text.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::ilm::*;

/// ILM statistics for metrics collection.
#[derive(Debug, Clone, Default)]
pub struct IlmStats {
    /// Server identifier
    pub server: String,
    /// Number of pending ILM expiry tasks
    pub expiry_pending_tasks: u64,
    /// Number of active ILM transition tasks
    pub transition_active_tasks: u64,
    /// Number of pending ILM transition tasks
    pub transition_pending_tasks: u64,
    /// Number of missed immediate ILM transition tasks
    pub transition_missed_immediate_tasks: u64,
    /// Number of ILM transition tasks that initially hit full queue backpressure
    pub transition_queue_full_tasks: u64,
    /// Number of ILM transition tasks that timed out waiting for queue capacity
    pub transition_queue_send_timeout_tasks: u64,
    /// Number of bucket-level compensation tasks scheduled after immediate enqueue failure
    pub transition_compensation_scheduled_tasks: u64,
    /// Number of bucket-level compensation tasks currently running
    pub transition_compensation_running_tasks: u64,
    /// Total number of object versions scanned for ILM
    pub versions_scanned: u64,
    /// Additional bounded action/state task details
    pub action_tasks: Vec<IlmActionTaskStats>,
}

/// ILM task metrics by action and state.
#[derive(Debug, Clone, Default)]
pub struct IlmActionTaskStats {
    pub action: String,
    pub state: String,
    pub value: u64,
}

/// Collects ILM metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::ilm` module.
/// Returns a vector of Prometheus metrics for ILM statistics.
pub fn collect_ilm_metrics(stats: &IlmStats) -> Vec<PrometheusMetric> {
    let mut metrics = vec![
        PrometheusMetric::from_descriptor(&ILM_EXPIRY_PENDING_TASKS_MD, stats.expiry_pending_tasks as f64),
        PrometheusMetric::from_descriptor(&ILM_TRANSITION_ACTIVE_TASKS_MD, stats.transition_active_tasks as f64),
        PrometheusMetric::from_descriptor(&ILM_TRANSITION_PENDING_TASKS_MD, stats.transition_pending_tasks as f64),
        PrometheusMetric::from_descriptor(
            &ILM_TRANSITION_MISSED_IMMEDIATE_TASKS_MD,
            stats.transition_missed_immediate_tasks as f64,
        ),
        PrometheusMetric::from_descriptor(&ILM_TRANSITION_QUEUE_FULL_TASKS_MD, stats.transition_queue_full_tasks as f64),
        PrometheusMetric::from_descriptor(
            &ILM_TRANSITION_QUEUE_SEND_TIMEOUT_TASKS_MD,
            stats.transition_queue_send_timeout_tasks as f64,
        ),
        PrometheusMetric::from_descriptor(
            &ILM_TRANSITION_COMPENSATION_SCHEDULED_TASKS_MD,
            stats.transition_compensation_scheduled_tasks as f64,
        ),
        PrometheusMetric::from_descriptor(
            &ILM_TRANSITION_COMPENSATION_RUNNING_TASKS_MD,
            stats.transition_compensation_running_tasks as f64,
        ),
        PrometheusMetric::from_descriptor(&ILM_VERSIONS_SCANNED_MD, stats.versions_scanned as f64),
    ];

    metrics.extend(stats.action_tasks.iter().map(|task| {
        PrometheusMetric::from_descriptor(&ILM_ACTION_TASKS_MD, task.value as f64)
            .with_label_owned(SERVER_LABEL, stats.server.clone())
            .with_label_owned(ACTION_LABEL, task.action.clone())
            .with_label_owned(STATE_LABEL, task.state.clone())
    }));

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_ilm_metrics() {
        let stats = IlmStats {
            server: "node1:9000".to_string(),
            expiry_pending_tasks: 100,
            transition_active_tasks: 5,
            transition_pending_tasks: 50,
            transition_missed_immediate_tasks: 10,
            transition_queue_full_tasks: 2,
            transition_queue_send_timeout_tasks: 3,
            transition_compensation_scheduled_tasks: 4,
            transition_compensation_running_tasks: 1,
            versions_scanned: 1000000,
            action_tasks: vec![
                IlmActionTaskStats {
                    action: "expiry".to_string(),
                    state: "pending".to_string(),
                    value: 100,
                },
                IlmActionTaskStats {
                    action: "transition".to_string(),
                    state: "queue_send_timeout".to_string(),
                    value: 3,
                },
            ],
        };

        let metrics = collect_ilm_metrics(&stats);

        assert_eq!(metrics.len(), 11);

        let pending = metrics.iter().find(|m| m.value == 100.0);
        assert!(pending.is_some());

        let scanned = metrics.iter().find(|m| m.value == 1000000.0);
        assert!(scanned.is_some());

        let transition_timeout = metrics.iter().find(|m| {
            m.name == ILM_ACTION_TASKS_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == SERVER_LABEL && value.as_ref() == "node1:9000")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == ACTION_LABEL && value.as_ref() == "transition")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == STATE_LABEL && value.as_ref() == "queue_send_timeout")
        });
        assert_eq!(transition_timeout.map(|m| m.value), Some(3.0));
    }

    #[test]
    fn test_collect_ilm_metrics_default() {
        let stats = IlmStats::default();
        let metrics = collect_ilm_metrics(&stats);

        assert_eq!(metrics.len(), 9);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
