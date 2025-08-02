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

/// Memory-related metric descriptors
///
/// This module provides a set of metric descriptors for system memory statistics.
/// These descriptors are initialized lazily using `std::sync::LazyLock` to ensure
/// they are only created when actually needed, improving performance and reducing
/// startup overhead.
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// Total memory available on the node
pub static MEM_TOTAL_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::MemTotal, "Total memory on the node", &[], subsystems::SYSTEM_MEMORY));

/// Memory currently in use on the node
pub static MEM_USED_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::MemUsed, "Used memory on the node", &[], subsystems::SYSTEM_MEMORY));

/// Percentage of total memory currently in use
pub static MEM_USED_PERC_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::MemUsedPerc,
        "Used memory percentage on the node",
        &[],
        subsystems::SYSTEM_MEMORY,
    )
});

/// Memory not currently in use and available for allocation
pub static MEM_FREE_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::MemFree, "Free memory on the node", &[], subsystems::SYSTEM_MEMORY));

/// Memory used for file buffers by the kernel
pub static MEM_BUFFERS_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::MemBuffers, "Buffers memory on the node", &[], subsystems::SYSTEM_MEMORY));

/// Memory used for caching file data by the kernel
pub static MEM_CACHE_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::MemCache, "Cache memory on the node", &[], subsystems::SYSTEM_MEMORY));

/// Memory shared between multiple processes
pub static MEM_SHARED_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::MemShared, "Shared memory on the node", &[], subsystems::SYSTEM_MEMORY));

/// Estimate of memory available for new applications without swapping
pub static MEM_AVAILABLE_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::MemAvailable, "Available memory on the node", &[], subsystems::SYSTEM_MEMORY));
