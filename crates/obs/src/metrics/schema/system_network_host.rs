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

use crate::{MetricDescriptor, MetricName, new_counter_md, subsystems};
use std::sync::LazyLock;

pub const DIRECTION_LABEL: &str = "direction";
pub const INTERFACE_LABEL: &str = "interface";

/// Host network I/O bytes collected from system network interfaces.
pub static HOST_NETWORK_IO_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::HostNetworkIO,
        "Network bytes transferred across system network interfaces",
        &[DIRECTION_LABEL],
        subsystems::SYSTEM_NETWORK_HOST,
    )
});

/// Host network I/O bytes collected from system network interfaces, grouped per interface.
pub static HOST_NETWORK_IO_PER_INTERFACE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::HostNetworkIOPerInterface,
        "Network bytes transferred across system network interfaces (per interface)",
        &[INTERFACE_LABEL, DIRECTION_LABEL],
        subsystems::SYSTEM_NETWORK_HOST,
    )
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricType;

    #[test]
    fn host_network_descriptors_export_counter_labels() {
        assert_eq!(HOST_NETWORK_IO_MD.metric_type, MetricType::Counter);
        assert_eq!(HOST_NETWORK_IO_MD.variable_labels, vec![DIRECTION_LABEL.to_string()]);

        assert_eq!(HOST_NETWORK_IO_PER_INTERFACE_MD.metric_type, MetricType::Counter);
        assert_eq!(
            HOST_NETWORK_IO_PER_INTERFACE_MD.variable_labels,
            vec![INTERFACE_LABEL.to_string(), DIRECTION_LABEL.to_string()]
        );
    }
}
