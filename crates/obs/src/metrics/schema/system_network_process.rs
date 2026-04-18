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

use crate::{MetricDescriptor, MetricName, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// Process network I/O bytes.
pub static PROCESS_NETWORK_IO_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessNetworkIO,
        "Network bytes transferred by the process",
        &[],
        subsystems::SYSTEM_NETWORK_PROCESS,
    )
});

/// Process network I/O bytes per interface.
pub static PROCESS_NETWORK_IO_PER_INTERFACE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessNetworkIOPerInterface,
        "Network bytes transferred by the process (per interface)",
        &[],
        subsystems::SYSTEM_NETWORK_PROCESS,
    )
});
