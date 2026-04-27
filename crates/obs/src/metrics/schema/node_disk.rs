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

const SERVER_LABEL: &str = "server";
const DRIVE_LABEL: &str = "drive";

/// Total disk capacity in bytes
pub static NODE_DISK_TOTAL_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("disk_total_bytes".to_string()),
        "Total disk capacity in bytes",
        &[SERVER_LABEL, DRIVE_LABEL],
        MetricSubsystem::new("/node"),
    )
});

/// Used disk space in bytes
pub static NODE_DISK_USED_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("disk_used_bytes".to_string()),
        "Used disk space in bytes",
        &[SERVER_LABEL, DRIVE_LABEL],
        MetricSubsystem::new("/node"),
    )
});

/// Free disk space in bytes
pub static NODE_DISK_FREE_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("disk_free_bytes".to_string()),
        "Free disk space in bytes",
        &[SERVER_LABEL, DRIVE_LABEL],
        MetricSubsystem::new("/node"),
    )
});
