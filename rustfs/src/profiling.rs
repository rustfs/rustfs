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

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod unsupported_impl {
    use std::path::PathBuf;
    use std::time::Duration;
    use tracing::{debug, info};

    const LOG_COMPONENT_PROFILING: &str = "profiling";
    const LOG_SUBSYSTEM_PLATFORM: &str = "platform";

    pub async fn init_from_env() {
        let target_env = option_env!("CARGO_CFG_TARGET_ENV").unwrap_or("unknown");
        info!(
            component = LOG_COMPONENT_PROFILING,
            subsystem = LOG_SUBSYSTEM_PLATFORM,
            event = "profiling_runtime_skipped",
            reason = "unsupported_platform",
            target_os = std::env::consts::OS,
            target_env,
            target_arch = std::env::consts::ARCH,
            "Profiling runtime skipped"
        );
    }

    /// Stop all background profiling tasks
    pub fn shutdown_profiling() {
        let target_env = option_env!("CARGO_CFG_TARGET_ENV").unwrap_or("unknown");
        debug!(
            component = LOG_COMPONENT_PROFILING,
            subsystem = LOG_SUBSYSTEM_PLATFORM,
            event = "profiling_shutdown_skipped",
            reason = "unsupported_platform",
            target_os = std::env::consts::OS,
            target_env,
            target_arch = std::env::consts::ARCH,
            "Profiling shutdown skipped"
        );
    }

    pub async fn dump_cpu_pprof_for(_duration: Duration) -> Result<PathBuf, String> {
        Err(unsupported_message("CPU profiling"))
    }

    pub async fn dump_memory_pprof_now() -> Result<PathBuf, String> {
        Err(unsupported_message("Memory profiling"))
    }

    fn unsupported_message(feature: &str) -> String {
        let target_env = option_env!("CARGO_CFG_TARGET_ENV").unwrap_or("unknown");
        format!(
            "{feature} is only supported on linux x86_64 gnu. target_os={}, target_env={target_env}, target_arch={}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub use unsupported_impl::{dump_cpu_pprof_for, dump_memory_pprof_now, init_from_env, shutdown_profiling};

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod linux_impl {
    use pprof::protos::Message;
    use rustfs_config::{
        DEFAULT_CPU_DURATION_SECS, DEFAULT_CPU_FREQ, DEFAULT_CPU_INTERVAL_SECS, DEFAULT_CPU_MODE, DEFAULT_ENABLE_PROFILING,
        DEFAULT_MEM_INTERVAL_SECS, DEFAULT_MEM_PERIODIC, DEFAULT_OUTPUT_DIR, ENV_CPU_DURATION_SECS, ENV_CPU_FREQ,
        ENV_CPU_INTERVAL_SECS, ENV_CPU_MODE, ENV_ENABLE_PROFILING, ENV_MEM_INTERVAL_SECS, ENV_MEM_PERIODIC, ENV_OUTPUT_DIR,
    };
    use rustfs_utils::{get_env_bool, get_env_str, get_env_u64, get_env_usize};
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;
    use tracing::{debug, info, warn};

    const LOG_COMPONENT_PROFILING: &str = "profiling";
    const LOG_SUBSYSTEM_CPU: &str = "cpu";
    const LOG_SUBSYSTEM_MEMORY: &str = "memory";
    const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";

    static CPU_CONT_GUARD: OnceLock<Arc<Mutex<Option<pprof::ProfilerGuard<'static>>>>> = OnceLock::new();
    static PROFILING_CANCEL_TOKEN: OnceLock<CancellationToken> = OnceLock::new();

    /// CPU profiling mode
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CpuMode {
        Off,
        Continuous,
        Periodic,
    }

    /// Get or create output directory
    fn output_dir() -> PathBuf {
        let dir = get_env_str(ENV_OUTPUT_DIR, DEFAULT_OUTPUT_DIR);
        let p = PathBuf::from(dir);
        if let Err(e) = create_dir_all(&p) {
            warn!(
                component = LOG_COMPONENT_PROFILING,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                event = "profiling_output_dir_fallback",
                path = %p.display(),
                error = %e,
                fallback = ".",
                "Profiling output directory fallback applied"
            );
            return PathBuf::from(".");
        }
        p
    }

    /// Read CPU profiling mode from env
    fn read_cpu_mode() -> CpuMode {
        match get_env_str(ENV_CPU_MODE, DEFAULT_CPU_MODE).to_lowercase().as_str() {
            "continuous" => CpuMode::Continuous,
            "periodic" => CpuMode::Periodic,
            _ => CpuMode::Off,
        }
    }

    /// Generate timestamp string for filenames
    fn ts() -> String {
        jiff::Zoned::now().strftime("%Y%m%dT%H%M%S").to_string()
    }

    /// Write pprof report to file in protobuf format
    fn write_pprof_report_pb(report: &pprof::Report, path: &Path) -> Result<(), String> {
        let profile = report.pprof().map_err(|e| format!("pprof() failed: {e}"))?;
        let mut buf = Vec::with_capacity(512 * 1024);
        profile.write_to_vec(&mut buf).map_err(|e| format!("encode failed: {e}"))?;
        let mut f = File::create(path).map_err(|e| format!("create file failed: {e}"))?;
        f.write_all(&buf).map_err(|e| format!("write file failed: {e}"))?;
        Ok(())
    }

    /// Internal: dump CPU pprof from existing guard
    async fn dump_cpu_with_guard(guard: &pprof::ProfilerGuard<'_>) -> Result<PathBuf, String> {
        let report = guard.report().build().map_err(|e| format!("build report failed: {e}"))?;
        let out = output_dir().join(format!("cpu_profile_{}.pb", ts()));
        write_pprof_report_pb(&report, &out)?;
        info!(
            component = LOG_COMPONENT_PROFILING,
            subsystem = LOG_SUBSYSTEM_CPU,
            event = "profiling_dump_exported",
            profile_type = "cpu",
            path = %out.display(),
            "Profiling dump exported"
        );
        Ok(out)
    }

    // Public API: dump CPU for a duration; if continuous guard exists, snapshot immediately.
    pub async fn dump_cpu_pprof_for(duration: Duration) -> Result<PathBuf, String> {
        if let Some(cell) = CPU_CONT_GUARD.get() {
            let guard_slot = cell.lock().await;
            if let Some(ref guard) = *guard_slot {
                debug!(
                    component = LOG_COMPONENT_PROFILING,
                    subsystem = LOG_SUBSYSTEM_CPU,
                    event = "profiling_dump_source",
                    profile_type = "cpu",
                    source = "continuous_guard",
                    "Using continuous CPU profiling guard for dump"
                );
                return dump_cpu_with_guard(guard).await;
            }
        }

        let freq = get_env_usize(ENV_CPU_FREQ, DEFAULT_CPU_FREQ) as i32;
        let guard = pprof::ProfilerGuard::new(freq).map_err(|e| format!("create profiler failed: {e}"))?;
        sleep(duration).await;

        dump_cpu_with_guard(&guard).await
    }

    pub async fn dump_memory_pprof_now() -> Result<PathBuf, String> {
        Err(memory_profiling_unsupported_message())
    }

    pub async fn check_memory_profiling() {
        debug!(
            component = LOG_COMPONENT_PROFILING,
            subsystem = LOG_SUBSYSTEM_MEMORY,
            event = "memory_profiling_status",
            result = "skipped",
            reason = "mimalloc_memory_pprof_unsupported",
            "Memory profiling status checked"
        );
    }

    // Internal: start continuous CPU profiling
    async fn start_cpu_continuous(freq_hz: i32) {
        let cell = CPU_CONT_GUARD.get_or_init(|| Arc::new(Mutex::new(None))).clone();
        let mut slot = cell.lock().await;
        if slot.is_some() {
            warn!(
                component = LOG_COMPONENT_PROFILING,
                subsystem = LOG_SUBSYSTEM_CPU,
                event = "profiling_state",
                profile_type = "cpu_continuous",
                state = "already_running",
                "CPU profiling already running"
            );
            return;
        }
        match pprof::ProfilerGuardBuilder::default()
            .frequency(freq_hz)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
        {
            Ok(guard) => {
                *slot = Some(guard);
                info!(
                    component = LOG_COMPONENT_PROFILING,
                    subsystem = LOG_SUBSYSTEM_CPU,
                    event = "profiling_state",
                    profile_type = "cpu_continuous",
                    state = "started",
                    freq_hz,
                    "CPU profiling started"
                );
            }
            Err(e) => warn!(
                component = LOG_COMPONENT_PROFILING,
                subsystem = LOG_SUBSYSTEM_CPU,
                event = "profiling_state",
                profile_type = "cpu_continuous",
                state = "start_failed",
                error = %e,
                "CPU profiling failed to start"
            ),
        }
    }

    // Internal: start periodic CPU sampling loop
    async fn start_cpu_periodic(freq_hz: i32, interval: Duration, duration: Duration, token: CancellationToken) {
        info!(
            component = LOG_COMPONENT_PROFILING,
            subsystem = LOG_SUBSYSTEM_CPU,
            event = "profiling_state",
            profile_type = "cpu_periodic",
            state = "started",
            freq_hz,
            ?interval,
            ?duration,
            "Periodic CPU profiling started"
        );
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        info!(
                            component = LOG_COMPONENT_PROFILING,
                            subsystem = LOG_SUBSYSTEM_CPU,
                            event = "profiling_state",
                            profile_type = "cpu_periodic",
                            state = "cancelled",
                            "Periodic CPU profiling cancelled"
                        );
                        break;
                    }
                    _ = sleep(interval) => {}
                }

                if token.is_cancelled() {
                    break;
                }

                let guard = match pprof::ProfilerGuard::new(freq_hz) {
                    Ok(g) => g,
                    Err(e) => {
                        warn!(
                            component = LOG_COMPONENT_PROFILING,
                            subsystem = LOG_SUBSYSTEM_CPU,
                            event = "profiling_capture_failed",
                            profile_type = "cpu_periodic",
                            stage = "create_guard",
                            error = %e,
                            "Profiling capture failed"
                        );
                        continue;
                    }
                };

                tokio::select! {
                    _ = token.cancelled() => {
                        info!(
                            component = LOG_COMPONENT_PROFILING,
                            subsystem = LOG_SUBSYSTEM_CPU,
                            event = "profiling_state",
                            profile_type = "cpu_periodic",
                            state = "cancelled_during_capture",
                            "Periodic CPU profiling cancelled during capture"
                        );
                        break;
                    }
                    _ = sleep(duration) => {}
                }

                match guard.report().build() {
                    Ok(report) => {
                        let out = output_dir().join(format!("cpu_profile_{}.pb", ts()));
                        if let Err(e) = write_pprof_report_pb(&report, &out) {
                            warn!(
                                component = LOG_COMPONENT_PROFILING,
                                subsystem = LOG_SUBSYSTEM_CPU,
                                event = "profiling_dump_failed",
                                profile_type = "cpu_periodic",
                                stage = "write_dump",
                                path = %out.display(),
                                error = %e,
                                "Periodic CPU dump write failed"
                            );
                        } else {
                            debug!(
                                component = LOG_COMPONENT_PROFILING,
                                subsystem = LOG_SUBSYSTEM_CPU,
                                event = "profiling_dump_exported",
                                profile_type = "cpu_periodic",
                                path = %out.display(),
                                "Profiling dump exported"
                            );
                        }
                    }
                    Err(e) => warn!(
                        component = LOG_COMPONENT_PROFILING,
                        subsystem = LOG_SUBSYSTEM_CPU,
                        event = "profiling_capture_failed",
                        profile_type = "cpu_periodic",
                        stage = "build_report",
                        error = %e,
                        "Profiling capture failed"
                    ),
                }
            }
        });
    }

    async fn start_memory_periodic(_interval: Duration, _token: CancellationToken) {
        debug!(
            component = LOG_COMPONENT_PROFILING,
            subsystem = LOG_SUBSYSTEM_MEMORY,
            event = "profiling_runtime_skipped",
            profile_type = "memory_periodic",
            reason = "mimalloc_memory_pprof_unsupported",
            "Profiling runtime skipped"
        );
    }

    // Public: unified init entry, avoid duplication/conflict
    pub async fn init_from_env() {
        let enabled = get_env_bool(ENV_ENABLE_PROFILING, DEFAULT_ENABLE_PROFILING);
        if !enabled {
            debug!(
                component = LOG_COMPONENT_PROFILING,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                event = "profiling_runtime_disabled",
                reason = "env_flag",
                "Profiling runtime disabled"
            );
            return;
        }

        // Memory pprof dumps are disabled while RustFS uses mimalloc.
        check_memory_profiling().await;

        // Initialize cancellation token
        let token = PROFILING_CANCEL_TOKEN.get_or_init(CancellationToken::new).clone();

        // CPU
        let cpu_mode = read_cpu_mode();
        let cpu_freq = get_env_usize(ENV_CPU_FREQ, DEFAULT_CPU_FREQ) as i32;
        let cpu_interval = Duration::from_secs(get_env_u64(ENV_CPU_INTERVAL_SECS, DEFAULT_CPU_INTERVAL_SECS));
        let cpu_duration = Duration::from_secs(get_env_u64(ENV_CPU_DURATION_SECS, DEFAULT_CPU_DURATION_SECS));

        match cpu_mode {
            CpuMode::Off => debug!(
                component = LOG_COMPONENT_PROFILING,
                subsystem = LOG_SUBSYSTEM_CPU,
                event = "profiling_mode_selected",
                profile_type = "cpu",
                state = "off",
                "Profiling mode selected"
            ),
            CpuMode::Continuous => start_cpu_continuous(cpu_freq).await,
            CpuMode::Periodic => start_cpu_periodic(cpu_freq, cpu_interval, cpu_duration, token.clone()).await,
        }

        // Memory
        let mem_periodic = get_env_bool(ENV_MEM_PERIODIC, DEFAULT_MEM_PERIODIC);
        let mem_interval = Duration::from_secs(get_env_u64(ENV_MEM_INTERVAL_SECS, DEFAULT_MEM_INTERVAL_SECS));
        if mem_periodic {
            start_memory_periodic(mem_interval, token).await;
        }
    }

    fn memory_profiling_unsupported_message() -> String {
        let target_env = option_env!("CARGO_CFG_TARGET_ENV").unwrap_or("unknown");
        format!(
            "Memory pprof dumps are not supported with the mimalloc allocator. target_os={}, target_env={target_env}, target_arch={}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    }

    /// Stop all background profiling tasks
    pub fn shutdown_profiling() {
        if let Some(token) = PROFILING_CANCEL_TOKEN.get() {
            token.cancel();
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub use linux_impl::{dump_cpu_pprof_for, dump_memory_pprof_now, init_from_env, shutdown_profiling};
