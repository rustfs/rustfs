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
}

/// ILM task metrics by action and state.
#[derive(Debug, Clone, Default)]
pub(crate) struct IlmActionTaskStats {
    pub(crate) action: String,
    pub(crate) state: String,
    pub(crate) value: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct IlmQueueTaskStats {
    pub(crate) action: String,
    pub(crate) state: String,
    pub(crate) value: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct IlmTaskEventStats {
    pub(crate) action: String,
    pub(crate) result: String,
    pub(crate) value: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct IlmBackpressureStats {
    pub(crate) action: String,
    pub(crate) reason: String,
    pub(crate) value: u64,
}

/// ILM statistics with runtime-local node identity and bounded action/state details.
#[derive(Debug, Clone, Default)]
pub(crate) struct IlmRuntimeStats {
    pub(crate) server: String,
    pub(crate) stats: IlmStats,
    pub(crate) action_tasks: Vec<IlmActionTaskStats>,
    pub(crate) queue_tasks: Vec<IlmQueueTaskStats>,
    pub(crate) task_events: Vec<IlmTaskEventStats>,
    pub(crate) backpressure: Vec<IlmBackpressureStats>,
    pub(crate) versions_scanned: u64,
}

fn is_live_action_task_state(state: &str) -> bool {
    matches!(state, "pending" | "active" | "compensation_running")
}

/// Collects ILM metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::ilm` module.
/// Returns a vector of Prometheus metrics for ILM statistics.
pub fn collect_ilm_metrics(stats: &IlmStats) -> Vec<PrometheusMetric> {
    vec![
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
    ]
}

pub(crate) fn collect_ilm_runtime_metrics(stats: &IlmRuntimeStats) -> Vec<PrometheusMetric> {
    let mut metrics = collect_ilm_metrics(&stats.stats);

    metrics.extend(
        stats
            .action_tasks
            .iter()
            .filter(|task| is_live_action_task_state(&task.state))
            .map(|task| {
                PrometheusMetric::from_descriptor(&ILM_ACTION_TASKS_MD, task.value as f64)
                    .with_label_owned(SERVER_LABEL, stats.server.clone())
                    .with_label_owned(ACTION_LABEL, task.action.clone())
                    .with_label_owned(STATE_LABEL, task.state.clone())
            }),
    );

    metrics.extend(stats.queue_tasks.iter().map(|task| {
        PrometheusMetric::from_descriptor(&ILM_TASKS_MD, task.value as f64)
            .with_label_owned(SERVER_LABEL, stats.server.clone())
            .with_label_owned(ACTION_LABEL, task.action.clone())
            .with_label_owned(QUEUE_STATE_LABEL, task.state.clone())
    }));
    metrics.extend(stats.task_events.iter().map(|event| {
        PrometheusMetric::from_descriptor(&ILM_TASK_EVENTS_MD, event.value as f64)
            .with_label_owned(SERVER_LABEL, stats.server.clone())
            .with_label_owned(ACTION_LABEL, event.action.clone())
            .with_label_owned(RESULT_LABEL, event.result.clone())
    }));
    metrics.extend(stats.backpressure.iter().map(|event| {
        PrometheusMetric::from_descriptor(&ILM_QUEUE_BACKPRESSURE_MD, event.value as f64)
            .with_label_owned(SERVER_LABEL, stats.server.clone())
            .with_label_owned(ACTION_LABEL, event.action.clone())
            .with_label_owned(REASON_LABEL, event.reason.clone())
    }));
    metrics.push(
        PrometheusMetric::from_descriptor(&ILM_VERSIONS_SCANNED_BY_SERVER_MD, stats.versions_scanned as f64)
            .with_label_owned(SERVER_LABEL, stats.server.clone())
            .with_label_owned(SOURCE_LABEL, "lifecycle".to_string()),
    );

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_ilm_metrics() {
        let stats = IlmStats {
            expiry_pending_tasks: 100,
            transition_active_tasks: 5,
            transition_pending_tasks: 50,
            transition_missed_immediate_tasks: 10,
            transition_queue_full_tasks: 2,
            transition_queue_send_timeout_tasks: 3,
            transition_compensation_scheduled_tasks: 4,
            transition_compensation_running_tasks: 1,
            versions_scanned: 1000000,
        };
        let runtime_stats = IlmRuntimeStats {
            server: "node1:9000".to_string(),
            stats,
            queue_tasks: vec![IlmQueueTaskStats {
                action: "transition".to_string(),
                state: "pending".to_string(),
                value: 8,
            }],
            task_events: vec![IlmTaskEventStats {
                action: "transition".to_string(),
                result: "completed".to_string(),
                value: 7,
            }],
            backpressure: vec![IlmBackpressureStats {
                action: "transition".to_string(),
                reason: "queue_full".to_string(),
                value: 2,
            }],
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
                IlmActionTaskStats {
                    action: "transition".to_string(),
                    state: "active".to_string(),
                    value: 5,
                },
            ],
        };

        let metrics = collect_ilm_runtime_metrics(&runtime_stats);

        assert_eq!(metrics.len(), 15);

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
        assert!(transition_timeout.is_none());

        let transition_queue = metrics.iter().find(|m| {
            m.name == ILM_TASKS_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == ACTION_LABEL && value.as_ref() == "transition")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == QUEUE_STATE_LABEL && value.as_ref() == "pending")
        });
        assert_eq!(transition_queue.map(|metric| metric.value), Some(8.0));

        let completed = metrics.iter().find(|m| {
            m.name == ILM_TASK_EVENTS_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == RESULT_LABEL && value.as_ref() == "completed")
        });
        assert_eq!(completed.map(|metric| metric.value), Some(7.0));

        let backpressure = metrics.iter().find(|m| {
            m.name == ILM_QUEUE_BACKPRESSURE_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == REASON_LABEL && value.as_ref() == "queue_full")
        });
        assert_eq!(backpressure.map(|metric| metric.value), Some(2.0));

        let version_detail = metrics.iter().find(|m| {
            m.name == ILM_VERSIONS_SCANNED_BY_SERVER_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == SERVER_LABEL && value.as_ref() == "node1:9000")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == SOURCE_LABEL && value.as_ref() == "lifecycle")
        });
        assert_eq!(version_detail.map(|metric| metric.value), Some(1000000.0));

        let transition_active = metrics.iter().find(|m| {
            m.name == ILM_ACTION_TASKS_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == ACTION_LABEL && value.as_ref() == "transition")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == STATE_LABEL && value.as_ref() == "active")
        });
        assert_eq!(transition_active.map(|m| m.value), Some(5.0));
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
