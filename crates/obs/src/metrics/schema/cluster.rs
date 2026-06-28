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

/// Total raw storage capacity across all disks in bytes
pub static CLUSTER_CAPACITY_RAW_TOTAL_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("capacity_raw_total_bytes".to_string()),
        "Total raw storage capacity in bytes across all disks",
        &[],
        subsystems::CLUSTER_BASE_PATH,
    )
});

/// Total usable storage capacity in bytes (accounting for erasure coding)
pub static CLUSTER_CAPACITY_USABLE_TOTAL_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("capacity_usable_total_bytes".to_string()),
        "Total usable storage capacity in bytes (accounting for erasure coding)",
        &[],
        subsystems::CLUSTER_BASE_PATH,
    )
});

/// Total used storage capacity in bytes
pub static CLUSTER_CAPACITY_USED_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("capacity_used_bytes".to_string()),
        "Total used storage capacity in bytes",
        &[],
        subsystems::CLUSTER_BASE_PATH,
    )
});

/// Total free storage capacity in bytes
pub static CLUSTER_CAPACITY_FREE_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("capacity_free_bytes".to_string()),
        "Total free storage capacity in bytes",
        &[],
        subsystems::CLUSTER_BASE_PATH,
    )
});

/// Number of drives whose capacity is served from a stale snapshot.
pub static CLUSTER_CAPACITY_STALE_DRIVES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("capacity_stale_drives".to_string()),
        "Count of drives whose capacity metrics are served from stale snapshots",
        &[],
        subsystems::CLUSTER_BASE_PATH,
    )
});

/// Number of drives with no capacity observation available.
pub static CLUSTER_CAPACITY_MISSING_DRIVES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("capacity_missing_drives".to_string()),
        "Count of drives with missing capacity observations",
        &[],
        subsystems::CLUSTER_BASE_PATH,
    )
});

/// Total number of objects in the cluster
pub static CLUSTER_OBJECTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("objects_total".to_string()),
        "Total number of objects in the cluster",
        &[],
        subsystems::CLUSTER_BASE_PATH,
    )
});

/// Total number of buckets in the cluster
pub static CLUSTER_BUCKETS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("buckets_total".to_string()),
        "Total number of buckets in the cluster",
        &[],
        subsystems::CLUSTER_BASE_PATH,
    )
});
