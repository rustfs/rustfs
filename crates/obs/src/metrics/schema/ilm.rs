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

use crate::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub const SERVER_LABEL: &str = "server";
pub const ACTION_LABEL: &str = "action";
pub const STATE_LABEL: &str = "state";
pub const QUEUE_STATE_LABEL: &str = "queue_state";
pub const RESULT_LABEL: &str = "result";
pub const REASON_LABEL: &str = "reason";
pub const SOURCE_LABEL: &str = "source";

pub static ILM_ACTION_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("action_tasks".to_string()),
        "ILM task counts by server, action, and state",
        &[SERVER_LABEL, ACTION_LABEL, STATE_LABEL],
        subsystems::ILM,
    )
});

pub static ILM_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("tasks".to_string()),
        "Current ILM task counts by server, action, and queue state",
        &[SERVER_LABEL, ACTION_LABEL, QUEUE_STATE_LABEL],
        subsystems::ILM,
    )
});

pub static ILM_TASK_EVENTS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("task_events_total".to_string()),
        "ILM task events by server, action, and result",
        &[SERVER_LABEL, ACTION_LABEL, RESULT_LABEL],
        subsystems::ILM,
    )
});

pub static ILM_QUEUE_BACKPRESSURE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("queue_backpressure_total".to_string()),
        "ILM queue backpressure events by server, action, and reason",
        &[SERVER_LABEL, ACTION_LABEL, REASON_LABEL],
        subsystems::ILM,
    )
});

pub static ILM_EXPIRY_PENDING_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::IlmExpiryPendingTasks,
        "Number of pending ILM expiry tasks in the queue",
        &[],
        subsystems::ILM,
    )
});

pub static ILM_TRANSITION_ACTIVE_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::IlmTransitionActiveTasks,
        "Number of active ILM transition tasks",
        &[],
        subsystems::ILM,
    )
});

pub static ILM_TRANSITION_PENDING_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::IlmTransitionPendingTasks,
        "Number of pending ILM transition tasks in the queue",
        &[],
        subsystems::ILM,
    )
});

pub static ILM_TRANSITION_MISSED_IMMEDIATE_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::IlmTransitionMissedImmediateTasks,
        "Number of missed immediate ILM transition tasks",
        &[],
        subsystems::ILM,
    )
});

pub static ILM_TRANSITION_QUEUE_FULL_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::IlmTransitionQueueFullTasks,
        "Number of ILM transition tasks that initially hit full transition queue backpressure",
        &[],
        subsystems::ILM,
    )
});

pub static ILM_TRANSITION_QUEUE_SEND_TIMEOUT_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::IlmTransitionQueueSendTimeoutTasks,
        "Number of ILM transition tasks that timed out waiting for queue capacity",
        &[],
        subsystems::ILM,
    )
});

pub static ILM_TRANSITION_COMPENSATION_SCHEDULED_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::IlmTransitionCompensationScheduledTasks,
        "Number of bucket-level ILM transition compensation tasks scheduled after enqueue failure",
        &[],
        subsystems::ILM,
    )
});

pub static ILM_TRANSITION_COMPENSATION_RUNNING_TASKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::IlmTransitionCompensationRunningTasks,
        "Number of bucket-level ILM transition compensation tasks currently running",
        &[],
        subsystems::ILM,
    )
});

pub static ILM_VERSIONS_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::IlmVersionsScanned,
        "Total number of object versions checked for ILM actions since server start",
        &[],
        subsystems::ILM,
    )
});

pub static ILM_VERSIONS_SCANNED_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("versions_scanned_by_server".to_string()),
        "ILM lifecycle-checked object versions by server and source",
        &[SERVER_LABEL, SOURCE_LABEL],
        subsystems::ILM,
    )
});
