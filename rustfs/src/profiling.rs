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

use std::path::PathBuf;
use std::time::Duration;
use tracing::{debug, info};

const LOG_COMPONENT_PROFILING: &str = "profiling";
const LOG_SUBSYSTEM_CPU: &str = "cpu";
const LOG_SUBSYSTEM_MEMORY: &str = "memory";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
const LOCAL_CPU_PPROF_UNSUPPORTED_REASON: &str = "local_cpu_pprof_unsupported";
const MEMORY_PPROF_UNSUPPORTED_REASON: &str = "mimalloc_memory_pprof_unsupported";

pub async fn init_from_env() {
    info!(
        component = LOG_COMPONENT_PROFILING,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        event = "profiling_runtime_skipped",
        reason = LOCAL_CPU_PPROF_UNSUPPORTED_REASON,
        target_os = std::env::consts::OS,
        target_env = target_env(),
        target_arch = std::env::consts::ARCH,
        "Local pprof profiling runtime skipped"
    );
}

pub fn shutdown_profiling() {
    debug!(
        component = LOG_COMPONENT_PROFILING,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        event = "profiling_shutdown_skipped",
        reason = LOCAL_CPU_PPROF_UNSUPPORTED_REASON,
        target_os = std::env::consts::OS,
        target_env = target_env(),
        target_arch = std::env::consts::ARCH,
        "Local pprof profiling shutdown skipped"
    );
}

pub async fn dump_cpu_pprof_for(_duration: Duration) -> Result<PathBuf, String> {
    debug!(
        component = LOG_COMPONENT_PROFILING,
        subsystem = LOG_SUBSYSTEM_CPU,
        event = "profiling_dump_skipped",
        profile_type = "cpu",
        reason = LOCAL_CPU_PPROF_UNSUPPORTED_REASON,
        "Local CPU pprof dump skipped"
    );
    Err(local_cpu_pprof_unsupported_message())
}

pub async fn dump_memory_pprof_now() -> Result<PathBuf, String> {
    debug!(
        component = LOG_COMPONENT_PROFILING,
        subsystem = LOG_SUBSYSTEM_MEMORY,
        event = "profiling_dump_skipped",
        profile_type = "memory",
        reason = MEMORY_PPROF_UNSUPPORTED_REASON,
        "Memory pprof dump skipped"
    );
    Err(memory_pprof_unsupported_message())
}

fn local_cpu_pprof_unsupported_message() -> String {
    format!(
        "Local CPU pprof dumps are not supported while RustFS uses Pyroscope 2.x profiling. Use Pyroscope export instead. target_os={}, target_env={}, target_arch={}",
        std::env::consts::OS,
        target_env(),
        std::env::consts::ARCH
    )
}

fn memory_pprof_unsupported_message() -> String {
    format!(
        "Memory pprof dumps are not supported with the mimalloc allocator. target_os={}, target_env={}, target_arch={}",
        std::env::consts::OS,
        target_env(),
        std::env::consts::ARCH
    )
}

fn target_env() -> &'static str {
    option_env!("CARGO_CFG_TARGET_ENV").unwrap_or("unknown")
}
