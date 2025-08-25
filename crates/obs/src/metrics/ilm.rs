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

/// ILM-related metric descriptors
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

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

pub static ILM_VERSIONS_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::IlmVersionsScanned,
        "Total number of object versions checked for ILM actions since server start",
        &[],
        subsystems::ILM,
    )
});
