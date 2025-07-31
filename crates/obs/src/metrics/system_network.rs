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

/// Network-related metric descriptors
///
/// These metrics capture internode network communication statistics including:
/// - Error counts for connection and general internode calls
/// - Network dial performance metrics
/// - Data transfer volume in both directions
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// Total number of failed internode calls counter
pub static INTERNODE_ERRORS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::InternodeErrorsTotal,
        "Total number of failed internode calls",
        &[],
        subsystems::SYSTEM_NETWORK_INTERNODE,
    )
});

/// TCP dial timeouts and errors counter
pub static INTERNODE_DIAL_ERRORS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::InternodeDialErrorsTotal,
        "Total number of internode TCP dial timeouts and errors",
        &[],
        subsystems::SYSTEM_NETWORK_INTERNODE,
    )
});

/// Average dial time gauge in nanoseconds
pub static INTERNODE_DIAL_AVG_TIME_NANOS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::InternodeDialAvgTimeNanos,
        "Average dial time of internode TCP calls in nanoseconds",
        &[],
        subsystems::SYSTEM_NETWORK_INTERNODE,
    )
});

/// Outbound network traffic counter in bytes
pub static INTERNODE_SENT_BYTES_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::InternodeSentBytesTotal,
        "Total number of bytes sent to other peer nodes",
        &[],
        subsystems::SYSTEM_NETWORK_INTERNODE,
    )
});

/// Inbound network traffic counter in bytes
pub static INTERNODE_RECV_BYTES_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::InternodeRecvBytesTotal,
        "Total number of bytes received from other peer nodes",
        &[],
        subsystems::SYSTEM_NETWORK_INTERNODE,
    )
});
