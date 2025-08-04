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

/// Erasure code set related metric descriptors
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// The label for the pool ID
pub const POOL_ID_L: &str = "pool_id";
/// The label for the pool ID
pub const SET_ID_L: &str = "set_id";

pub static ERASURE_SET_OVERALL_WRITE_QUORUM_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetOverallWriteQuorum,
        "Overall write quorum across pools and sets",
        &[],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_OVERALL_HEALTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetOverallHealth,
        "Overall health across pools and sets (1=healthy, 0=unhealthy)",
        &[],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_READ_QUORUM_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetReadQuorum,
        "Read quorum for the erasure set in a pool",
        &[POOL_ID_L, SET_ID_L],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_WRITE_QUORUM_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetWriteQuorum,
        "Write quorum for the erasure set in a pool",
        &[POOL_ID_L, SET_ID_L],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_ONLINE_DRIVES_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetOnlineDrivesCount,
        "Count of online drives in the erasure set in a pool",
        &[POOL_ID_L, SET_ID_L],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_HEALING_DRIVES_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetHealingDrivesCount,
        "Count of healing drives in the erasure set in a pool",
        &[POOL_ID_L, SET_ID_L],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_HEALTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetHealth,
        "Health of the erasure set in a pool (1=healthy, 0=unhealthy)",
        &[POOL_ID_L, SET_ID_L],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_READ_TOLERANCE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetReadTolerance,
        "No of drive failures that can be tolerated without disrupting read operations",
        &[POOL_ID_L, SET_ID_L],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_WRITE_TOLERANCE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetWriteTolerance,
        "No of drive failures that can be tolerated without disrupting write operations",
        &[POOL_ID_L, SET_ID_L],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_READ_HEALTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetReadHealth,
        "Health of the erasure set in a pool for read operations (1=healthy, 0=unhealthy)",
        &[POOL_ID_L, SET_ID_L],
        subsystems::CLUSTER_ERASURE_SET,
    )
});

pub static ERASURE_SET_WRITE_HEALTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ErasureSetWriteHealth,
        "Health of the erasure set in a pool for write operations (1=healthy, 0=unhealthy)",
        &[POOL_ID_L, SET_ID_L],
        subsystems::CLUSTER_ERASURE_SET,
    )
});
