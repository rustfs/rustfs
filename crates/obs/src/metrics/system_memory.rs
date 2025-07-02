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

/// Memory-related metric descriptors
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};

lazy_static::lazy_static! {
    pub static ref MEM_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemTotal,
            "Total memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_USED_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemUsed,
            "Used memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_USED_PERC_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemUsedPerc,
            "Used memory percentage on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_FREE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemFree,
            "Free memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_BUFFERS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemBuffers,
            "Buffers memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_CACHE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemCache,
            "Cache memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_SHARED_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemShared,
            "Shared memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_AVAILABLE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemAvailable,
            "Available memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );
}
