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

use crate::{MetricDescriptor, MetricName, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub const TARGET_ID: &str = "target_id";
pub const SERVER: &str = "server";
pub const RESULT: &str = "result"; // success / failure
pub const STATUS: &str = "status"; // success / failure

pub const SUCCESS: &str = "success";
pub const FAILURE: &str = "failure";

const TARGET_LABELS: [&str; 1] = [TARGET_ID];
const TARGET_SERVER_LABELS: [&str; 2] = [SERVER, TARGET_ID];

pub static AUDIT_FAILED_MESSAGES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::AuditFailedMessages,
        "Total number of messages that failed to send since start",
        &TARGET_LABELS,
        subsystems::AUDIT,
    )
});

pub static AUDIT_FAILED_MESSAGES_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("failed_messages_by_server".to_string()),
        "Total number of messages that failed to send since start by server and target",
        &TARGET_SERVER_LABELS,
        subsystems::AUDIT,
    )
});

pub static AUDIT_FAILED_STORE_LENGTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::AuditFailedStoreLength,
        "Number of audit messages held in the failed-events store for target",
        &TARGET_LABELS,
        subsystems::AUDIT,
    )
});

pub static AUDIT_FAILED_STORE_LENGTH_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("failed_store_length_by_server".to_string()),
        "Number of audit messages held in the failed-events store by server and target",
        &TARGET_SERVER_LABELS,
        subsystems::AUDIT,
    )
});

pub static AUDIT_TARGET_QUEUE_LENGTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::AuditTargetQueueLength,
        "Number of unsent messages in queue for target",
        &TARGET_LABELS,
        subsystems::AUDIT,
    )
});

pub static AUDIT_TARGET_QUEUE_LENGTH_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("target_queue_length_by_server".to_string()),
        "Number of unsent audit messages in queue by server and target",
        &TARGET_SERVER_LABELS,
        subsystems::AUDIT,
    )
});

pub static AUDIT_TOTAL_MESSAGES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::AuditTotalMessages,
        "Total number of messages sent since start",
        &TARGET_LABELS,
        subsystems::AUDIT,
    )
});

pub static AUDIT_TOTAL_MESSAGES_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("total_messages_by_server".to_string()),
        "Total number of messages sent since start by server and target",
        &TARGET_SERVER_LABELS,
        subsystems::AUDIT,
    )
});
