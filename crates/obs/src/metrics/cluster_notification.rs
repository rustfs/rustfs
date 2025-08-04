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

/// Notify the relevant metric descriptor
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, subsystems};
use std::sync::LazyLock;

pub static NOTIFICATION_CURRENT_SEND_IN_PROGRESS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NotificationCurrentSendInProgress,
        "Number of concurrent async Send calls active to all targets",
        &[],
        subsystems::NOTIFICATION,
    )
});

pub static NOTIFICATION_EVENTS_ERRORS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NotificationEventsErrorsTotal,
        "Events that were failed to be sent to the targets",
        &[],
        subsystems::NOTIFICATION,
    )
});

pub static NOTIFICATION_EVENTS_SENT_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NotificationEventsSentTotal,
        "Total number of events sent to the targets",
        &[],
        subsystems::NOTIFICATION,
    )
});

pub static NOTIFICATION_EVENTS_SKIPPED_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NotificationEventsSkippedTotal,
        "Events that were skipped to be sent to the targets due to the in-memory queue being full",
        &[],
        subsystems::NOTIFICATION,
    )
});
