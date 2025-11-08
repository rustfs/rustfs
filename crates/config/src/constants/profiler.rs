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

/// Profiler related environment variable names and default values
pub const ENV_ENABLE_PROFILING: &str = "RUSTFS_ENABLE_PROFILING";

// CPU profiling
pub const ENV_CPU_MODE: &str = "RUSTFS_PROF_CPU_MODE"; // off|continuous|periodic
/// Frequency of CPU profiling samples
pub const ENV_CPU_FREQ: &str = "RUSTFS_PROF_CPU_FREQ";
/// Interval between CPU profiling sessions (for periodic mode)
pub const ENV_CPU_INTERVAL_SECS: &str = "RUSTFS_PROF_CPU_INTERVAL_SECS";
/// Duration of each CPU profiling session (for periodic mode)
pub const ENV_CPU_DURATION_SECS: &str = "RUSTFS_PROF_CPU_DURATION_SECS";

/// Memory profiling (jemalloc)
pub const ENV_MEM_PERIODIC: &str = "RUSTFS_PROF_MEM_PERIODIC";
/// Interval between memory profiling snapshots (for periodic mode)
pub const ENV_MEM_INTERVAL_SECS: &str = "RUSTFS_PROF_MEM_INTERVAL_SECS";

/// Output directory
pub const ENV_OUTPUT_DIR: &str = "RUSTFS_PROF_OUTPUT_DIR";

/// Defaults for profiler settings
pub const DEFAULT_ENABLE_PROFILING: bool = false;
/// CPU profiling
pub const DEFAULT_CPU_MODE: &str = "off";
/// Frequency of CPU profiling samples
pub const DEFAULT_CPU_FREQ: usize = 100;
/// Interval between CPU profiling sessions (for periodic mode)
pub const DEFAULT_CPU_INTERVAL_SECS: u64 = 300;
/// Duration of each CPU profiling session (for periodic mode)
pub const DEFAULT_CPU_DURATION_SECS: u64 = 60;
/// Memory profiling (jemalloc)
pub const DEFAULT_MEM_PERIODIC: bool = false;
/// Interval between memory profiling snapshots (for periodic mode)
pub const DEFAULT_MEM_INTERVAL_SECS: u64 = 300;
/// Output directory
pub const DEFAULT_OUTPUT_DIR: &str = ".";
