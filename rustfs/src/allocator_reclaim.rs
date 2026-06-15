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

use metrics::{counter, gauge, histogram};
use serde::Serialize;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const ALLOCATOR_RECLAIM_SERVICE_NAME: &str = "allocator_reclaim";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimServiceState {
    Disabled,
    Running,
    Stopping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimCancellationSource {
    RuntimeToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimShutdownHandle {
    RuntimeTokenOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AllocatorReclaimStatusSnapshot {
    pub service: &'static str,
    pub state: AllocatorReclaimServiceState,
    pub backend: &'static str,
    pub effective_force: bool,
    pub idle_intervals: u64,
    pub interval_secs: u64,
    pub cancellation_source: AllocatorReclaimCancellationSource,
    pub shutdown_handle: AllocatorReclaimShutdownHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimDesiredState {
    Disabled,
    Enabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AllocatorReclaimDesiredSnapshot {
    pub state: AllocatorReclaimDesiredState,
    pub configured_force: bool,
    pub idle_intervals: u64,
    pub interval_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AllocatorReclaimControllerSnapshot {
    pub desired: AllocatorReclaimDesiredSnapshot,
    pub status: AllocatorReclaimStatusSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimWorkerMutation {
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AllocatorReclaimReconcilePlan {
    pub service: &'static str,
    pub desired: AllocatorReclaimDesiredSnapshot,
    pub current_state: AllocatorReclaimServiceState,
    pub worker_mutation: AllocatorReclaimWorkerMutation,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AllocatorReclaimController;

impl AllocatorReclaimController {
    pub fn snapshot(&self, ctx: &CancellationToken) -> AllocatorReclaimControllerSnapshot {
        allocator_reclaim_controller_snapshot(ctx)
    }

    pub fn reconcile(&self, ctx: &CancellationToken) -> AllocatorReclaimReconcilePlan {
        let snapshot = self.snapshot(ctx);
        self.reconcile_snapshot(snapshot)
    }

    pub fn reconcile_snapshot(&self, snapshot: AllocatorReclaimControllerSnapshot) -> AllocatorReclaimReconcilePlan {
        AllocatorReclaimReconcilePlan {
            service: ALLOCATOR_RECLAIM_SERVICE_NAME,
            desired: snapshot.desired,
            current_state: snapshot.status.state,
            worker_mutation: AllocatorReclaimWorkerMutation::None,
        }
    }
}

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

fn current_scanner_activity() -> u64 {
    rustfs_scanner::current_scanner_activity()
}

fn current_heal_activity() -> u64 {
    rustfs_heal::current_heal_active_tasks() + rustfs_heal::current_heal_queue_length()
}

#[derive(Clone, Copy, Debug, Default)]
struct ReclaimableWorkSnapshot {
    active_requests: u64,
    delete_tail_activity: u64,
    scanner_activity: u64,
    heal_activity: u64,
    ec_inflight_bytes: u64,
    get_buffered_bytes: u64,
}

impl ReclaimableWorkSnapshot {
    fn active_signal_count(self) -> u64 {
        u64::from(self.active_requests > 0)
            + u64::from(self.delete_tail_activity > 0)
            + u64::from(self.scanner_activity > 0)
            + u64::from(self.heal_activity > 0)
            + u64::from(self.ec_inflight_bytes > 0)
            + u64::from(self.get_buffered_bytes > 0)
    }
}

fn reclaimable_work_snapshot() -> ReclaimableWorkSnapshot {
    ReclaimableWorkSnapshot {
        active_requests: active_requests(),
        delete_tail_activity: current_delete_tail_activity(),
        scanner_activity: current_scanner_activity(),
        heal_activity: current_heal_activity(),
        ec_inflight_bytes: rustfs_io_metrics::current_ec_encode_inflight_bytes(),
        get_buffered_bytes: rustfs_io_metrics::current_get_object_buffered_bytes(),
    }
}

fn configured_allocator_reclaim_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_ALLOCATOR_RECLAIM_ENABLED,
        rustfs_config::DEFAULT_ALLOCATOR_RECLAIM_ENABLED,
    )
}

fn configured_allocator_reclaim_force() -> bool {
    rustfs_utils::get_env_bool(rustfs_config::ENV_ALLOCATOR_RECLAIM_FORCE, rustfs_config::DEFAULT_ALLOCATOR_RECLAIM_FORCE)
}

fn configured_allocator_reclaim_idle_intervals() -> u64 {
    rustfs_utils::get_env_u64(
        rustfs_config::ENV_ALLOCATOR_RECLAIM_IDLE_INTERVALS,
        rustfs_config::DEFAULT_ALLOCATOR_RECLAIM_IDLE_INTERVALS,
    )
    .max(1)
}

fn configured_allocator_reclaim_interval_secs() -> u64 {
    rustfs_utils::get_env_u64(
        rustfs_config::ENV_ALLOCATOR_RECLAIM_INTERVAL_SECS,
        rustfs_config::DEFAULT_ALLOCATOR_RECLAIM_INTERVAL_SECS,
    )
    .max(1)
}

fn effective_allocator_reclaim_force(backend: &str, configured_force: bool) -> bool {
    configured_force && backend != "jemalloc"
}

fn build_allocator_reclaim_desired_snapshot(
    enabled: bool,
    configured_force: bool,
    idle_intervals: u64,
    interval_secs: u64,
) -> AllocatorReclaimDesiredSnapshot {
    let state = if enabled {
        AllocatorReclaimDesiredState::Enabled
    } else {
        AllocatorReclaimDesiredState::Disabled
    };

    AllocatorReclaimDesiredSnapshot {
        state,
        configured_force,
        idle_intervals: idle_intervals.max(1),
        interval_secs: interval_secs.max(1),
    }
}

fn build_allocator_reclaim_status_snapshot(
    enabled: bool,
    backend: &'static str,
    effective_force: bool,
    idle_intervals: u64,
    interval_secs: u64,
    cancellation_requested: bool,
) -> AllocatorReclaimStatusSnapshot {
    let state = if !enabled {
        AllocatorReclaimServiceState::Disabled
    } else if cancellation_requested {
        AllocatorReclaimServiceState::Stopping
    } else {
        AllocatorReclaimServiceState::Running
    };

    AllocatorReclaimStatusSnapshot {
        service: ALLOCATOR_RECLAIM_SERVICE_NAME,
        state,
        backend,
        effective_force,
        idle_intervals: idle_intervals.max(1),
        interval_secs: interval_secs.max(1),
        cancellation_source: AllocatorReclaimCancellationSource::RuntimeToken,
        shutdown_handle: AllocatorReclaimShutdownHandle::RuntimeTokenOnly,
    }
}

pub fn allocator_reclaim_status_snapshot(ctx: &CancellationToken) -> AllocatorReclaimStatusSnapshot {
    let backend = allocator_backend();
    let configured_force = configured_allocator_reclaim_force();
    build_allocator_reclaim_status_snapshot(
        configured_allocator_reclaim_enabled(),
        backend,
        effective_allocator_reclaim_force(backend, configured_force),
        configured_allocator_reclaim_idle_intervals(),
        configured_allocator_reclaim_interval_secs(),
        ctx.is_cancelled(),
    )
}

fn build_allocator_reclaim_controller_snapshot(
    enabled: bool,
    backend: &'static str,
    configured_force: bool,
    idle_intervals: u64,
    interval_secs: u64,
    cancellation_requested: bool,
) -> AllocatorReclaimControllerSnapshot {
    AllocatorReclaimControllerSnapshot {
        desired: build_allocator_reclaim_desired_snapshot(enabled, configured_force, idle_intervals, interval_secs),
        status: build_allocator_reclaim_status_snapshot(
            enabled,
            backend,
            effective_allocator_reclaim_force(backend, configured_force),
            idle_intervals,
            interval_secs,
            cancellation_requested,
        ),
    }
}

pub fn allocator_reclaim_controller_snapshot(ctx: &CancellationToken) -> AllocatorReclaimControllerSnapshot {
    build_allocator_reclaim_controller_snapshot(
        configured_allocator_reclaim_enabled(),
        allocator_backend(),
        configured_allocator_reclaim_force(),
        configured_allocator_reclaim_idle_intervals(),
        configured_allocator_reclaim_interval_secs(),
        ctx.is_cancelled(),
    )
}

#[cfg(all(
    not(target_os = "windows"),
    not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))
))]
#[allow(unsafe_code)]
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
    let _ = tikv_jemalloc_ctl::background_thread::write(true);
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
    let backend = allocator_backend();
    let enabled = configured_allocator_reclaim_enabled();
    gauge!("rustfs_memory_allocator_reclaim_enabled").set(if enabled { 1.0 } else { 0.0 });
    counter!("rustfs_memory_allocator_backend_info", "backend" => backend.to_string()).increment(1);

    if !enabled {
        debug!("allocator reclaim loop disabled");
        return;
    }

    let configured_force = configured_allocator_reclaim_force();
    if backend == "jemalloc" && configured_force {
        warn!(
            backend,
            env = rustfs_config::ENV_ALLOCATOR_RECLAIM_FORCE,
            "allocator reclaim force mode is not supported on jemalloc backend; ignoring configured force flag"
        );
    }
    let force = effective_allocator_reclaim_force(backend, configured_force);
    let idle_intervals = configured_allocator_reclaim_idle_intervals();
    let interval = Duration::from_secs(configured_allocator_reclaim_interval_secs());

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
                    let snapshot = reclaimable_work_snapshot();
                    let active_signal_count = snapshot.active_signal_count();
                    gauge!("rustfs_memory_allocator_reclaim_active_requests").set(snapshot.active_requests as f64);
                    gauge!("rustfs_memory_allocator_reclaim_delete_tail_activity_current").set(snapshot.delete_tail_activity as f64);
                    gauge!("rustfs_memory_allocator_reclaim_scanner_activity_current").set(snapshot.scanner_activity as f64);
                    gauge!("rustfs_memory_allocator_reclaim_heal_activity_current").set(snapshot.heal_activity as f64);
                    gauge!("rustfs_memory_allocator_reclaim_ec_inflight_bytes_current").set(snapshot.ec_inflight_bytes as f64);
                    gauge!("rustfs_memory_allocator_reclaim_get_buffered_bytes_current").set(snapshot.get_buffered_bytes as f64);
                    gauge!("rustfs_memory_allocator_reclaim_reclaimable_work_current").set(active_signal_count as f64);
                    if active_signal_count == 0 {
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
                        let reason = if active_signal_count > 0 {
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
    use super::{
        ALLOCATOR_RECLAIM_SERVICE_NAME, AllocatorReclaimCancellationSource, AllocatorReclaimController,
        AllocatorReclaimDesiredState, AllocatorReclaimServiceState, AllocatorReclaimShutdownHandle,
        AllocatorReclaimWorkerMutation, allocator_backend, build_allocator_reclaim_controller_snapshot,
        build_allocator_reclaim_status_snapshot, effective_allocator_reclaim_force, reclaimable_work_snapshot,
    };

    #[test]
    fn allocator_backend_name_is_available() {
        assert!(!allocator_backend().is_empty());
    }

    #[test]
    fn reclaimable_work_snapshot_is_collectable() {
        let _ = reclaimable_work_snapshot();
    }

    #[test]
    fn allocator_reclaim_status_reports_disabled_state() {
        let snapshot = build_allocator_reclaim_status_snapshot(false, "mimalloc", true, 3, 30, false);

        assert_eq!(snapshot.service, ALLOCATOR_RECLAIM_SERVICE_NAME);
        assert_eq!(snapshot.state, AllocatorReclaimServiceState::Disabled);
        assert_eq!(snapshot.backend, "mimalloc");
        assert!(snapshot.effective_force);
        assert_eq!(snapshot.idle_intervals, 3);
        assert_eq!(snapshot.interval_secs, 30);
        assert_eq!(snapshot.cancellation_source, AllocatorReclaimCancellationSource::RuntimeToken);
        assert_eq!(snapshot.shutdown_handle, AllocatorReclaimShutdownHandle::RuntimeTokenOnly);
    }

    #[test]
    fn allocator_reclaim_status_reports_running_and_stopping_states() {
        let running = build_allocator_reclaim_status_snapshot(true, "mimalloc", true, 0, 0, false);
        let stopping = build_allocator_reclaim_status_snapshot(true, "mimalloc", false, 4, 60, true);

        assert_eq!(running.state, AllocatorReclaimServiceState::Running);
        assert_eq!(running.idle_intervals, 1);
        assert_eq!(running.interval_secs, 1);
        assert_eq!(stopping.state, AllocatorReclaimServiceState::Stopping);
        assert_eq!(stopping.idle_intervals, 4);
        assert_eq!(stopping.interval_secs, 60);
    }

    #[test]
    fn allocator_reclaim_force_preserves_jemalloc_override() {
        assert!(!effective_allocator_reclaim_force("jemalloc", true));
        assert!(effective_allocator_reclaim_force("mimalloc", true));
        assert!(!effective_allocator_reclaim_force("mimalloc", false));
    }

    #[test]
    fn allocator_reclaim_controller_reconcile_is_idempotent() {
        let controller = AllocatorReclaimController;
        let snapshot = build_allocator_reclaim_controller_snapshot(true, "mimalloc", true, 3, 30, false);

        let first = controller.reconcile_snapshot(snapshot);
        let second = controller.reconcile_snapshot(snapshot);

        assert_eq!(first, second);
        assert_eq!(first.desired.state, AllocatorReclaimDesiredState::Enabled);
        assert_eq!(first.current_state, AllocatorReclaimServiceState::Running);
        assert_eq!(first.worker_mutation, AllocatorReclaimWorkerMutation::None);
    }

    #[test]
    fn allocator_reclaim_controller_preserves_cancellation_state_without_worker_mutation() {
        let controller = AllocatorReclaimController;
        let snapshot = build_allocator_reclaim_controller_snapshot(true, "mimalloc", false, 3, 30, true);
        let plan = controller.reconcile_snapshot(snapshot);

        assert_eq!(snapshot.status.state, AllocatorReclaimServiceState::Stopping);
        assert_eq!(plan.current_state, AllocatorReclaimServiceState::Stopping);
        assert_eq!(plan.worker_mutation, AllocatorReclaimWorkerMutation::None);
    }

    #[test]
    fn allocator_reclaim_controller_reports_disabled_desired_state_without_starting_worker() {
        let controller = AllocatorReclaimController;
        let snapshot = build_allocator_reclaim_controller_snapshot(false, "mimalloc", true, 0, 0, false);
        let plan = controller.reconcile_snapshot(snapshot);

        assert_eq!(snapshot.desired.state, AllocatorReclaimDesiredState::Disabled);
        assert_eq!(snapshot.desired.idle_intervals, 1);
        assert_eq!(snapshot.desired.interval_secs, 1);
        assert_eq!(plan.current_state, AllocatorReclaimServiceState::Disabled);
        assert_eq!(plan.worker_mutation, AllocatorReclaimWorkerMutation::None);
    }

    #[tokio::test(start_paused = true)]
    async fn allocator_reclaim_controller_harness_is_stable_across_paused_time() {
        let controller = AllocatorReclaimController;
        let snapshot = build_allocator_reclaim_controller_snapshot(true, "mimalloc", true, 3, 30, false);
        let before = controller.reconcile_snapshot(snapshot);

        tokio::time::advance(std::time::Duration::from_secs(30)).await;
        let after = controller.reconcile_snapshot(snapshot);

        assert_eq!(before, after);
        assert_eq!(after.worker_mutation, AllocatorReclaimWorkerMutation::None);
    }
}
