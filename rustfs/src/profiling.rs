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

use rustfs_config::{
    ENV_CPU_DURATION_SECS, ENV_CPU_FREQ, ENV_CPU_INTERVAL_SECS, ENV_CPU_MODE, ENV_ENABLE_PROFILING, ENV_MEM_INTERVAL_SECS,
    ENV_MEM_PERIODIC, ENV_OUTPUT_DIR,
};
use tracing::{debug, info, warn};

const LOG_COMPONENT_PROFILING: &str = "profiling";
const LOG_SUBSYSTEM_CPU: &str = "cpu";
const LOG_SUBSYSTEM_MEMORY: &str = "memory";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
const LOCAL_CPU_PPROF_UNSUPPORTED_REASON: &str = "local_cpu_pprof_unsupported";
const MEMORY_PPROF_UNSUPPORTED_REASON: &str = "mimalloc_memory_pprof_unsupported";
pub const LOCAL_CPU_PPROF_UNSUPPORTED_SUMMARY: &str = "local CPU pprof dumps are not supported; use Pyroscope export instead";
pub const MEMORY_PPROF_UNSUPPORTED_SUMMARY: &str = "memory pprof dumps are not supported with the mimalloc allocator";

const LEGACY_PROFILING_ENV_KEYS: [&str; 8] = [
    ENV_ENABLE_PROFILING,
    ENV_CPU_MODE,
    ENV_CPU_FREQ,
    ENV_CPU_INTERVAL_SECS,
    ENV_CPU_DURATION_SECS,
    ENV_MEM_PERIODIC,
    ENV_MEM_INTERVAL_SECS,
    ENV_OUTPUT_DIR,
];

pub async fn init_from_env() {
    let legacy_keys = legacy_profiling_env_keys_present();
    if !legacy_keys.is_empty() {
        warn!(
            component = LOG_COMPONENT_PROFILING,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            event = "profiling_legacy_env_ignored",
            ignored_keys = %legacy_keys.join(", "),
            recommended_enable_key = "RUSTFS_OBS_PROFILING_EXPORT_ENABLED",
            recommended_endpoint_key = "RUSTFS_OBS_PROFILING_ENDPOINT",
            output_dir_key = ENV_OUTPUT_DIR,
            "Legacy local profiling environment variables are ignored; use observability profiling export settings instead"
        );
    }

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

pub fn log_cpu_pprof_dump_skipped() {
    debug!(
        component = LOG_COMPONENT_PROFILING,
        subsystem = LOG_SUBSYSTEM_CPU,
        event = "profiling_dump_skipped",
        profile_type = "cpu",
        reason = LOCAL_CPU_PPROF_UNSUPPORTED_REASON,
        "Local CPU pprof dump skipped"
    );
}

pub fn log_memory_pprof_dump_skipped() {
    debug!(
        component = LOG_COMPONENT_PROFILING,
        subsystem = LOG_SUBSYSTEM_MEMORY,
        event = "profiling_dump_skipped",
        profile_type = "memory",
        reason = MEMORY_PPROF_UNSUPPORTED_REASON,
        "Memory pprof dump skipped"
    );
}

pub fn local_cpu_pprof_unsupported_message() -> String {
    unsupported_message(LOCAL_CPU_PPROF_UNSUPPORTED_SUMMARY)
}

pub fn memory_pprof_unsupported_message() -> String {
    unsupported_message(MEMORY_PPROF_UNSUPPORTED_SUMMARY)
}

fn unsupported_message(summary: &str) -> String {
    format!(
        "{summary}. target_os={}, target_env={}, target_arch={}",
        std::env::consts::OS,
        target_env(),
        std::env::consts::ARCH
    )
}

fn legacy_profiling_env_keys_present() -> Vec<&'static str> {
    LEGACY_PROFILING_ENV_KEYS
        .into_iter()
        .filter(|key| std::env::var_os(key).is_some())
        .collect()
}

fn target_env() -> &'static str {
    option_env!("CARGO_CFG_TARGET_ENV").unwrap_or("unknown")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_profiling_env_keys_present_reports_legacy_overrides() {
        temp_env::with_vars(
            [
                (ENV_ENABLE_PROFILING, Some("true")),
                (ENV_OUTPUT_DIR, Some("/tmp/rustfs-profiles")),
            ],
            || {
                let keys = legacy_profiling_env_keys_present();
                assert!(keys.contains(&ENV_ENABLE_PROFILING));
                assert!(keys.contains(&ENV_OUTPUT_DIR));
            },
        );
    }
}
