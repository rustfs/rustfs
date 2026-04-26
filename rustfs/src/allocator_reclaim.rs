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

#![allow(unsafe_code)]

use metrics::{counter, gauge, histogram};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const ENV_ALLOCATOR_RECLAIM_ENABLED: &str = "RUSTFS_ALLOCATOR_RECLAIM_ENABLED";
const ENV_ALLOCATOR_RECLAIM_INTERVAL_SECS: &str = "RUSTFS_ALLOCATOR_RECLAIM_INTERVAL_SECS";
const ENV_ALLOCATOR_RECLAIM_FORCE: &str = "RUSTFS_ALLOCATOR_RECLAIM_FORCE";
const DEFAULT_ALLOCATOR_RECLAIM_INTERVAL_SECS: u64 = 30;

pub fn allocator_backend() -> &'static str {
    #[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
    {
        "jemalloc"
    }

    #[cfg(all(
        not(target_os = "windows"),
        not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
    ))]
    {
        "mimalloc"
    }

    #[cfg(target_os = "windows")]
    {
        "mimalloc-windows"
    }
}

fn active_requests() -> u64 {
    crate::server::active_http_requests()
}

#[cfg(all(
    not(target_os = "windows"),
    not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
))]
fn collect_allocator_memory(force: bool) -> Result<(), String> {
    // SAFETY: `mi_collect` is provided by the active global allocator backend
    // on this target family. It is explicitly intended to reclaim retained
    // pages/segments and does not require additional invariants from the caller.
    unsafe {
        libmimalloc_sys::mi_collect(force);
    }
    Ok(())
}

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
fn collect_allocator_memory(_force: bool) -> Result<(), String> {
    tikv_jemalloc_ctl::epoch::advance().map_err(|err| err.to_string())?;
    Ok(())
}

#[cfg(target_os = "windows")]
fn collect_allocator_memory(_force: bool) -> Result<(), String> {
    Err("allocator reclaim is not supported on Windows".to_string())
}

fn run_allocator_reclaim(force: bool) {
    let backend = allocator_backend();
    let start = std::time::Instant::now();

    match collect_allocator_memory(force) {
        Ok(()) => {
            counter!("rustfs_memory_allocator_reclaim_total", "backend" => backend.to_string(), "result" => "ok".to_string())
                .increment(1);
            histogram!(
                "rustfs_memory_allocator_reclaim_duration_seconds",
                "backend" => backend.to_string(),
                "result" => "ok".to_string()
            )
            .record(start.elapsed().as_secs_f64());
        }
        Err(err) => {
            counter!(
                "rustfs_memory_allocator_reclaim_total",
                "backend" => backend.to_string(),
                "result" => "err".to_string()
            )
            .increment(1);
            warn!(backend, force, error = %err, "allocator reclaim failed");
        }
    }
}

pub fn init_allocator_reclaim(ctx: CancellationToken) {
    let enabled = rustfs_utils::get_env_bool(ENV_ALLOCATOR_RECLAIM_ENABLED, false);
    gauge!("rustfs_memory_allocator_reclaim_enabled").set(if enabled { 1.0 } else { 0.0 });
    counter!("rustfs_memory_allocator_backend_info", "backend" => allocator_backend().to_string()).increment(1);

    if !enabled {
        debug!("allocator reclaim loop disabled");
        return;
    }

    let force = rustfs_utils::get_env_bool(ENV_ALLOCATOR_RECLAIM_FORCE, true);
    let interval = Duration::from_secs(
        rustfs_utils::get_env_u64(ENV_ALLOCATOR_RECLAIM_INTERVAL_SECS, DEFAULT_ALLOCATOR_RECLAIM_INTERVAL_SECS).max(1),
    );

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ctx.cancelled() => {
                    debug!("allocator reclaim loop cancelled");
                    break;
                }
                _ = ticker.tick() => {
                    let active_requests = active_requests();
                    gauge!("rustfs_memory_allocator_reclaim_active_requests").set(active_requests as f64);
                    if active_requests == 0 {
                        run_allocator_reclaim(force);
                    } else {
                        counter!(
                            "rustfs_memory_allocator_reclaim_skipped_total",
                            "reason" => "active_requests".to_string()
                        )
                        .increment(1);
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::allocator_backend;

    #[test]
    fn allocator_backend_name_is_available() {
        assert!(!allocator_backend().is_empty());
    }
}
