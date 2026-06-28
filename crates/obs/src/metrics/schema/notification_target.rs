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

use crate::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub const TARGET_ID: &str = "target_id";
pub const TARGET_TYPE: &str = "target_type";

const NOTIFICATION_TARGET_LABELS: [&str; 2] = [TARGET_ID, TARGET_TYPE];

pub static NOTIFICATION_TARGET_FAILED_MESSAGES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NotificationTargetFailedMessages,
        "Total number of notification messages that permanently failed to send",
        &NOTIFICATION_TARGET_LABELS,
        subsystems::NOTIFICATION,
    )
});

pub static NOTIFICATION_TARGET_QUEUE_LENGTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::NotificationTargetQueueLength,
        "Number of queued notification messages pending delivery",
        &NOTIFICATION_TARGET_LABELS,
        subsystems::NOTIFICATION,
    )
});

pub static NOTIFICATION_TARGET_TOTAL_MESSAGES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NotificationTargetTotalMessages,
        "Total number of notification messages successfully delivered",
        &NOTIFICATION_TARGET_LABELS,
        subsystems::NOTIFICATION,
    )
});
