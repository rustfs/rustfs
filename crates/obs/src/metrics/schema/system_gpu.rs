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

//! GPU-related metric descriptors.
//!
//! This module defines metric descriptors for GPU monitoring,
//! including GPU memory usage metrics.

use crate::{MetricDescriptor, MetricName, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// Process GPU memory usage metric descriptor.
///
/// Records the amount of physical GPU memory in use by the process.
pub static PROCESS_GPU_MEMORY_USAGE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessGpuMemoryUsage,
        "The amount of physical GPU memory in use",
        &[],
        subsystems::SYSTEM_GPU,
    )
});
