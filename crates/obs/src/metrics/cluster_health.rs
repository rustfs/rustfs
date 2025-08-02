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

/// Cluster health-related metric descriptors
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub static HEALTH_DRIVES_OFFLINE_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthDrivesOfflineCount,
        "Count of offline drives in the cluster",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_DRIVES_ONLINE_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthDrivesOnlineCount,
        "Count of online drives in the cluster",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_DRIVES_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthDrivesCount,
        "Count of all drives in the cluster",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});
