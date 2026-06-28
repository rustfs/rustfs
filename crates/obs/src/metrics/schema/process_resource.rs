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

use crate::{MetricDescriptor, MetricName, MetricSubsystem, new_gauge_md};
use std::sync::LazyLock;

/// CPU usage of the RustFS process as a percentage
pub static PROCESS_CPU_PERCENT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("cpu_percent".to_string()),
        "CPU usage of the RustFS process as a percentage",
        &[],
        MetricSubsystem::new("/process"),
    )
});

/// Resident memory usage of the RustFS process in bytes
pub static PROCESS_MEMORY_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("memory_bytes".to_string()),
        "Resident memory usage of the RustFS process in bytes",
        &[],
        MetricSubsystem::new("/process"),
    )
});

/// Uptime of the RustFS process in seconds
pub static PROCESS_UPTIME_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("uptime_seconds".to_string()),
        "Uptime of the RustFS process in seconds",
        &[],
        MetricSubsystem::new("/process"),
    )
});
