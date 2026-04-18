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

pub mod system_cpu;
pub mod system_drive;
#[cfg(feature = "gpu")]
pub mod system_gpu;
pub mod system_memory;
pub mod system_network;
pub mod system_process;

pub use system_cpu::{CpuStats, ProcessCpuStats, collect_cpu_metrics, collect_process_cpu_metrics};
pub use system_drive::{
    DriveCountStats, DriveDetailedStats, ProcessDiskStats, collect_drive_count_metrics, collect_drive_detailed_metrics,
    collect_process_disk_metrics,
};
#[cfg(feature = "gpu")]
pub use system_gpu::{GpuCollector, GpuError, GpuStats, collect_gpu_metrics};
pub use system_memory::{MemoryStats, ProcessMemoryStats, collect_memory_metrics, collect_process_memory_metrics};
pub use system_network::{NetworkStats, ProcessNetworkStats, collect_network_metrics, collect_process_network_metrics};
pub use system_process::{
    ProcessAttributeError, ProcessAttributes, ProcessStats, ProcessStatusType, collect_process_attributes,
    collect_process_metrics,
};
