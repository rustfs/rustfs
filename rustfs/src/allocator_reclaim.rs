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
const ENV_ALLOCATOR_RECLAIM_IDLE_INTERVALS: &str = "RUSTFS_ALLOCATOR_RECLAIM_IDLE_INTERVALS";
const DEFAULT_ALLOCATOR_RECLAIM_INTERVAL_SECS: u64 = 30;
const DEFAULT_ALLOCATOR_RECLAIM_IDLE_INTERVALS: u64 = 3;

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

fn current_delete_tail_activity() -> u64 {
    crate::delete_tail_activity::current_delete_tail_activity()
}

fn reclaimable_work_inflight() -> u64 {
    active_requests()
        + current_delete_tail_activity()
        + rustfs_io_metrics::current_ec_encode_inflight_bytes()
        + rustfs_io_metrics::current_get_object_buffered_bytes()
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
    let idle_intervals =
        rustfs_utils::get_env_u64(ENV_ALLOCATOR_RECLAIM_IDLE_INTERVALS, DEFAULT_ALLOCATOR_RECLAIM_IDLE_INTERVALS).max(1);
    let interval = Duration::from_secs(
        rustfs_utils::get_env_u64(ENV_ALLOCATOR_RECLAIM_INTERVAL_SECS, DEFAULT_ALLOCATOR_RECLAIM_INTERVAL_SECS).max(1),
    );

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut idle_streak = 0_u64;

        loop {
            tokio::select! {
                _ = ctx.cancelled() => {
                    debug!("allocator reclaim loop cancelled");
                    break;
                }
                _ = ticker.tick() => {
                    let active_requests = active_requests();
                    let delete_tail_activity = current_delete_tail_activity();
                    let ec_inflight_bytes = rustfs_io_metrics::current_ec_encode_inflight_bytes();
                    let get_buffered_bytes = rustfs_io_metrics::current_get_object_buffered_bytes();
                    let reclaimable_inflight = reclaimable_work_inflight();
                    gauge!("rustfs_memory_allocator_reclaim_active_requests").set(active_requests as f64);
                    gauge!("rustfs_memory_allocator_reclaim_delete_tail_activity_current").set(delete_tail_activity as f64);
                    gauge!("rustfs_memory_allocator_reclaim_ec_inflight_bytes_current").set(ec_inflight_bytes as f64);
                    gauge!("rustfs_memory_allocator_reclaim_get_buffered_bytes_current").set(get_buffered_bytes as f64);
                    gauge!("rustfs_memory_allocator_reclaim_reclaimable_work_current").set(reclaimable_inflight as f64);
                    if reclaimable_inflight == 0 {
                        idle_streak = idle_streak.saturating_add(1);
                        gauge!("rustfs_memory_allocator_reclaim_idle_streak").set(idle_streak as f64);
                    } else {
                        idle_streak = 0;
                        gauge!("rustfs_memory_allocator_reclaim_idle_streak").set(0.0);
                    }

                    if idle_streak >= idle_intervals {
                        run_allocator_reclaim(force);
                        idle_streak = 0;
                        gauge!("rustfs_memory_allocator_reclaim_idle_streak").set(0.0);
                    } else {
                        let reason = if reclaimable_inflight > 0 {
                            "work_inflight"
                        } else {
                            "idle_window"
                        };
                        counter!("rustfs_memory_allocator_reclaim_skipped_total", "reason" => reason.to_string()).increment(1);
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{allocator_backend, reclaimable_work_inflight};

    #[test]
    fn allocator_backend_name_is_available() {
        assert!(!allocator_backend().is_empty());
    }

    #[test]
    fn reclaimable_work_inflight_is_collectable() {
        let _ = reclaimable_work_inflight();
    }
}
