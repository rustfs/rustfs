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

/// A descriptor for metrics related to webhook logs
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// Define label constants for webhook metrics
/// name label
pub const NAME_LABEL: &str = "name";
/// endpoint label
pub const ENDPOINT_LABEL: &str = "endpoint";

// The label used by all webhook metrics
const ALL_WEBHOOK_LABELS: [&str; 2] = [NAME_LABEL, ENDPOINT_LABEL];

pub static WEBHOOK_FAILED_MESSAGES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::WebhookFailedMessages,
        "Number of messages that failed to send",
        &ALL_WEBHOOK_LABELS[..],
        subsystems::LOGGER_WEBHOOK,
    )
});

pub static WEBHOOK_QUEUE_LENGTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::WebhookQueueLength,
        "Webhook queue length",
        &ALL_WEBHOOK_LABELS[..],
        subsystems::LOGGER_WEBHOOK,
    )
});

pub static WEBHOOK_TOTAL_MESSAGES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::WebhookTotalMessages,
        "Total number of messages sent to this target",
        &ALL_WEBHOOK_LABELS[..],
        subsystems::LOGGER_WEBHOOK,
    )
});
