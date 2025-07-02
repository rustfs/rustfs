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

/// audit related metric descriptors
///
/// This module contains the metric descriptors for the audit subsystem.
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};

const TARGET_ID: &str = "target_id";

lazy_static::lazy_static! {
    pub static ref AUDIT_FAILED_MESSAGES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::AuditFailedMessages,
            "Total number of messages that failed to send since start",
            &[TARGET_ID],
            subsystems::AUDIT
        );

    pub static ref AUDIT_TARGET_QUEUE_LENGTH_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::AuditTargetQueueLength,
            "Number of unsent messages in queue for target",
            &[TARGET_ID],
            subsystems::AUDIT
        );

    pub static ref AUDIT_TOTAL_MESSAGES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::AuditTotalMessages,
            "Total number of messages sent since start",
            &[TARGET_ID],
            subsystems::AUDIT
        );
}
