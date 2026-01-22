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

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
pub async fn init_from_env() {
    let (target_os, target_env, target_arch) = get_platform_info();
    tracing::info!(
        target: "rustfs::main::run",
        target_os = %target_os,
        target_env = %target_env,
        target_arch = %target_arch,
        "profiling: disabled on this platform. target_os={}, target_env={}, target_arch={}",
        target_os, target_env, target_arch
    );
}

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
fn get_platform_info() -> (String, String, String) {
    (
        std::env::consts::OS.to_string(),
        option_env!("CARGO_CFG_TARGET_ENV").unwrap_or("unknown").to_string(),
        std::env::consts::ARCH.to_string(),
    )
}

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
pub async fn dump_cpu_pprof_for(_duration: std::time::Duration) -> Result<std::path::PathBuf, String> {
    let (target_os, target_env, target_arch) = get_platform_info();
    let msg = format!(
        "CPU profiling is not supported on this platform. target_os={target_os}, target_env={target_env}, target_arch={target_arch}"
    );
    Err(msg)
}

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
pub async fn dump_memory_pprof_now() -> Result<std::path::PathBuf, String> {
    let (target_os, target_env, target_arch) = get_platform_info();
    let msg = format!(
        "Memory profiling is not supported on this platform. target_os={target_os}, target_env={target_env}, target_arch={target_arch}"
    );
    Err(msg)
}

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
mod linux_impl {
    use jemalloc_pprof::PROF_CTL;
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
    use tracing::{debug, error, info, warn};

    static CPU_CONT_GUARD: OnceLock<Arc<Mutex<Option<pprof::ProfilerGuard<'static>>>>> = OnceLock::new();

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
            warn!("profiling: create output dir {} failed: {}, fallback to current dir", p.display(), e);
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
        info!("CPU profile exported: {}", out.display());
        Ok(out)
    }

    // Public API: dump CPU for a duration; if continuous guard exists, snapshot immediately.
    pub async fn dump_cpu_pprof_for(duration: Duration) -> Result<PathBuf, String> {
        if let Some(cell) = CPU_CONT_GUARD.get() {
            let guard_slot = cell.lock().await;
            if let Some(ref guard) = *guard_slot {
                debug!("profiling: using continuous profiler guard for CPU dump");
                return dump_cpu_with_guard(guard).await;
            }
        }

        let freq = get_env_usize(ENV_CPU_FREQ, DEFAULT_CPU_FREQ) as i32;
        let guard = pprof::ProfilerGuard::new(freq).map_err(|e| format!("create profiler failed: {e}"))?;
        sleep(duration).await;

        dump_cpu_with_guard(&guard).await
    }

    // Public API: dump memory pprof now (jemalloc)
    pub async fn dump_memory_pprof_now() -> Result<PathBuf, String> {
        let out = output_dir().join(format!("mem_profile_{}.pb", ts()));
        let mut f = File::create(&out).map_err(|e| format!("create file failed: {e}"))?;

        let prof_ctl_cell = PROF_CTL
            .as_ref()
            .ok_or_else(|| "jemalloc profiling control not available".to_string())?;
        let mut prof_ctl = prof_ctl_cell.lock().await;

        if !prof_ctl.activated() {
            return Err("jemalloc profiling is not active".to_string());
        }

        let bytes = prof_ctl.dump_pprof().map_err(|e| format!("dump pprof failed: {e}"))?;
        f.write_all(&bytes).map_err(|e| format!("write file failed: {e}"))?;
        info!("Memory profile exported: {}", out.display());
        Ok(out)
    }

    // Jemalloc status check (No forced placement, only status observation)
    pub async fn check_jemalloc_profiling() {
        use tikv_jemalloc_ctl::{config, epoch, stats};

        if let Err(e) = epoch::advance() {
            warn!("jemalloc epoch advance failed: {e}");
        }

        match config::malloc_conf::read() {
            Ok(conf) => debug!("jemalloc malloc_conf: {}", conf),
            Err(e) => debug!("jemalloc read malloc_conf failed: {e}"),
        }

        match std::env::var("MALLOC_CONF") {
            Ok(v) => debug!("MALLOC_CONF={}", v),
            Err(_) => debug!("MALLOC_CONF is not set"),
        }

        if let Some(lock) = PROF_CTL.as_ref() {
            let ctl = lock.lock().await;
            info!(activated = ctl.activated(), "jemalloc profiling status");
        } else {
            info!("jemalloc profiling controller is NOT available");
        }

        let _ = epoch::advance();
        macro_rules! show {
            ($name:literal, $reader:expr) => {
                match $reader {
                    Ok(v) => debug!(concat!($name, "={}"), v),
                    Err(e) => debug!(concat!($name, " read failed: {}"), e),
                }
            };
        }
        show!("allocated", stats::allocated::read());
        show!("resident", stats::resident::read());
        show!("mapped", stats::mapped::read());
        show!("metadata", stats::metadata::read());
        show!("active", stats::active::read());
    }

    // Internal: start continuous CPU profiling
    async fn start_cpu_continuous(freq_hz: i32) {
        let cell = CPU_CONT_GUARD.get_or_init(|| Arc::new(Mutex::new(None))).clone();
        let mut slot = cell.lock().await;
        if slot.is_some() {
            warn!("profiling: continuous CPU guard already running");
            return;
        }
        match pprof::ProfilerGuardBuilder::default()
            .frequency(freq_hz)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
        {
            Ok(guard) => {
                *slot = Some(guard);
                info!(freq = freq_hz, "start continuous CPU profiling");
            }
            Err(e) => warn!("start continuous CPU profiling failed: {e}"),
        }
    }

    // Internal: start periodic CPU sampling loop
    async fn start_cpu_periodic(freq_hz: i32, interval: Duration, duration: Duration) {
        info!(freq = freq_hz, ?interval, ?duration, "start periodic CPU profiling");
        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let guard = match pprof::ProfilerGuard::new(freq_hz) {
                    Ok(g) => g,
                    Err(e) => {
                        warn!("periodic CPU profiler create failed: {e}");
                        continue;
                    }
                };
                sleep(duration).await;
                match guard.report().build() {
                    Ok(report) => {
                        let out = output_dir().join(format!("cpu_profile_{}.pb", ts()));
                        if let Err(e) = write_pprof_report_pb(&report, &out) {
                            warn!("write periodic CPU pprof failed: {e}");
                        } else {
                            info!("periodic CPU profile exported: {}", out.display());
                        }
                    }
                    Err(e) => warn!("periodic CPU report build failed: {e}"),
                }
            }
        });
    }

    // Internal: start periodic memory dump when jemalloc profiling is active
    async fn start_memory_periodic(interval: Duration) {
        info!(?interval, "start periodic memory pprof dump");
        tokio::spawn(async move {
            loop {
                sleep(interval).await;

                let Some(lock) = PROF_CTL.as_ref() else {
                    debug!("skip memory dump: PROF_CTL not available");
                    continue;
                };

                let mut ctl = lock.lock().await;
                if !ctl.activated() {
                    debug!("skip memory dump: jemalloc profiling not active");
                    continue;
                }

                let out = output_dir().join(format!("mem_profile_periodic_{}.pb", ts()));
                match File::create(&out) {
                    Err(e) => {
                        error!("periodic mem dump create file failed: {}", e);
                        continue;
                    }
                    Ok(mut f) => match ctl.dump_pprof() {
                        Ok(bytes) => {
                            if let Err(e) = f.write_all(&bytes) {
                                error!("periodic mem dump write failed: {}", e);
                            } else {
                                info!("periodic memory profile dumped to {}", out.display());
                            }
                        }
                        Err(e) => error!("periodic mem dump failed: {}", e),
                    },
                }
            }
        });
    }

    // Public: unified init entry, avoid duplication/conflict
    pub async fn init_from_env() {
        let enabled = get_env_bool(ENV_ENABLE_PROFILING, DEFAULT_ENABLE_PROFILING);
        if !enabled {
            debug!("profiling: disabled by env");
            return;
        }

        // Jemalloc state check once (no dump)
        check_jemalloc_profiling().await;

        // CPU
        let cpu_mode = read_cpu_mode();
        let cpu_freq = get_env_usize(ENV_CPU_FREQ, DEFAULT_CPU_FREQ) as i32;
        let cpu_interval = Duration::from_secs(get_env_u64(ENV_CPU_INTERVAL_SECS, DEFAULT_CPU_INTERVAL_SECS));
        let cpu_duration = Duration::from_secs(get_env_u64(ENV_CPU_DURATION_SECS, DEFAULT_CPU_DURATION_SECS));

        match cpu_mode {
            CpuMode::Off => debug!("profiling: CPU mode off"),
            CpuMode::Continuous => start_cpu_continuous(cpu_freq).await,
            CpuMode::Periodic => start_cpu_periodic(cpu_freq, cpu_interval, cpu_duration).await,
        }

        // Memory
        let mem_periodic = get_env_bool(ENV_MEM_PERIODIC, DEFAULT_MEM_PERIODIC);
        let mem_interval = Duration::from_secs(get_env_u64(ENV_MEM_INTERVAL_SECS, DEFAULT_MEM_INTERVAL_SECS));
        if mem_periodic {
            start_memory_periodic(mem_interval).await;
        }
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
pub use linux_impl::{dump_cpu_pprof_for, dump_memory_pprof_now, init_from_env};
