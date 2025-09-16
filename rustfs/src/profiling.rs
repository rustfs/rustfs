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

use pprof::ProfilerGuard;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::info;

static PROFILER_GUARD: OnceLock<Arc<Mutex<ProfilerGuard<'static>>>> = OnceLock::new();

pub fn init_profiler() -> Result<(), Box<dyn std::error::Error>> {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| format!("Failed to build profiler guard: {e}"))?;

    PROFILER_GUARD
        .set(Arc::new(Mutex::new(guard)))
        .map_err(|_| "Failed to set profiler guard (already initialized)")?;

    info!("Performance profiler initialized");
    Ok(())
}

pub fn is_profiler_enabled() -> bool {
    PROFILER_GUARD.get().is_some()
}

pub fn get_profiler_guard() -> Option<Arc<Mutex<ProfilerGuard<'static>>>> {
    PROFILER_GUARD.get().cloned()
}

pub fn start_profiling_if_enabled() {
    let enable_profiling = std::env::var("RUSTFS_ENABLE_PROFILING")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);

    if enable_profiling {
        match init_profiler() {
            Ok(()) => {
                info!("Performance profiling enabled via RUSTFS_ENABLE_PROFILING environment variable");
            }
            Err(e) => {
                tracing::error!("Failed to initialize profiler: {}", e);
                info!("Performance profiling disabled due to initialization error");
            }
        }
    } else {
        info!("Performance profiling disabled. Set RUSTFS_ENABLE_PROFILING=true to enable");
    }
}
