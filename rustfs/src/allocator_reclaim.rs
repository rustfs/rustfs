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

//! Allocator memory reclaim runtime.
//!
//! RustFS uses mimalloc on the supported production targets. Under bursty PUT,
//! GET, scanner, and heal workloads, mimalloc can retain freed pages in process
//! heaps for later reuse instead of immediately returning them to the OS. That
//! behavior is usually good for latency, but it can make process RSS look high
//! after a workload has gone idle. This module provides an opt-in background
//! loop that waits for a configurable idle window and then asks the allocator to
//! collect retained memory.
//!
//! The loop is intentionally conservative:
//!
//! - enablement and intervals are read from startup environment configuration;
//! - the periodic tick only samples cheap process-wide counters;
//! - reclaim is skipped while request, delete-tail, scanner, heal, EC encode,
//!   or whole-object GET buffering activity is still visible;
//! - cancellation is driven by the shared runtime `CancellationToken`;
//! - the controller surface is read-only and reports intent/status without
//!   mutating the worker lifecycle.
//!
//! `init_observability_runtime` passes a cloned cancellation token into this
//! module. Cloning a `CancellationToken` only creates another handle to the
//! same cancellation source; it does not duplicate the runtime state or spawn
//! work by itself.

use metrics::{counter, gauge, histogram};
use serde::Serialize;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const ALLOCATOR_RECLAIM_SERVICE_NAME: &str = "allocator_reclaim";

/// Externally visible lifecycle state for the allocator reclaim service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimServiceState {
    /// Reclaim is disabled by configuration, so no background loop is spawned.
    Disabled,
    /// Reclaim is enabled and the background loop is expected to be alive.
    Running,
    /// Runtime cancellation has been requested and the loop is exiting.
    Stopping,
}

/// Source that stops the reclaim loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimCancellationSource {
    /// The loop is tied to the process runtime cancellation token.
    RuntimeToken,
}

/// Shutdown ownership model for the reclaim worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimShutdownHandle {
    /// There is no dedicated join handle in the controller surface today.
    RuntimeTokenOnly,
}

/// Read-only status returned to background-controller/status callers.
///
/// This snapshot is intentionally derived from current configuration and the
/// runtime cancellation token. It does not prove that a spawned Tokio task is
/// currently scheduled; the controller pilot for this service does not own
/// worker mutation or task supervision yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AllocatorReclaimStatusSnapshot {
    pub service: &'static str,
    pub state: AllocatorReclaimServiceState,
    /// Allocator backend label used in metrics and status responses.
    pub backend: &'static str,
    /// Effective force flag after backend-specific support is applied.
    pub effective_force: bool,
    /// Number of consecutive idle ticks required before reclaim runs.
    pub idle_intervals: u64,
    /// Tick interval in seconds.
    pub interval_secs: u64,
    pub cancellation_source: AllocatorReclaimCancellationSource,
    pub shutdown_handle: AllocatorReclaimShutdownHandle,
}

/// Desired enablement from environment configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimDesiredState {
    Disabled,
    Enabled,
}

/// Desired allocator reclaim configuration.
///
/// Values are clamped to a minimum of one interval/tick to keep the background
/// loop from becoming a zero-duration busy loop if an operator supplies `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AllocatorReclaimDesiredSnapshot {
    pub state: AllocatorReclaimDesiredState,
    /// Raw force preference before backend-specific support is applied.
    pub configured_force: bool,
    pub idle_intervals: u64,
    pub interval_secs: u64,
}

/// Combined desired/status view for controller reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AllocatorReclaimControllerSnapshot {
    pub desired: AllocatorReclaimDesiredSnapshot,
    pub status: AllocatorReclaimStatusSnapshot,
}

/// Worker mutation requested by reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocatorReclaimWorkerMutation {
    /// The current controller surface reports only; it must not start, stop,
    /// resize, or wake the reclaim worker.
    None,
}

/// Idempotent reconcile output for the allocator reclaim controller surface.
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
    /// Build a fresh controller snapshot from current process state.
    pub fn snapshot(&self, ctx: &CancellationToken) -> AllocatorReclaimControllerSnapshot {
        allocator_reclaim_controller_snapshot(ctx)
    }

    /// Reconcile current desired/status state without mutating the worker.
    pub fn reconcile(&self, ctx: &CancellationToken) -> AllocatorReclaimReconcilePlan {
        let snapshot = self.snapshot(ctx);
        self.reconcile_snapshot(snapshot)
    }

    /// Convert a prebuilt snapshot into an idempotent reconcile plan.
    pub fn reconcile_snapshot(&self, snapshot: AllocatorReclaimControllerSnapshot) -> AllocatorReclaimReconcilePlan {
        AllocatorReclaimReconcilePlan {
            service: ALLOCATOR_RECLAIM_SERVICE_NAME,
            desired: snapshot.desired,
            current_state: snapshot.status.state,
            worker_mutation: AllocatorReclaimWorkerMutation::None,
        }
    }
}

/// Return the allocator backend name used by reclaim and memory metrics.
pub fn allocator_backend() -> &'static str {
    #[cfg(not(target_os = "windows"))]
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

/// Snapshot of activity classes that make allocator reclaim undesirable.
///
/// A non-zero field does not mean memory is definitely unreclaimable. It means
/// a workload that commonly allocates or owns large buffers is still in flight,
/// so reclaim should wait for a quieter interval to avoid fighting the
/// allocator while hot paths are active.
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
    /// Count how many independent activity classes are currently non-idle.
    fn active_signal_count(self) -> u64 {
        u64::from(self.active_requests > 0)
            + u64::from(self.delete_tail_activity > 0)
            + u64::from(self.scanner_activity > 0)
            + u64::from(self.heal_activity > 0)
            + u64::from(self.ec_inflight_bytes > 0)
            + u64::from(self.get_buffered_bytes > 0)
    }
}

/// Collect the current cheap activity gauges used to gate reclaim.
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

/// Read the startup enablement switch.
///
/// The code default is disabled. Local developer scripts may choose to export
/// the variable as enabled for their own launch profile.
fn configured_allocator_reclaim_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_ALLOCATOR_RECLAIM_ENABLED,
        rustfs_config::DEFAULT_ALLOCATOR_RECLAIM_ENABLED,
    )
}

/// Read whether reclaim should ask the allocator for a forceful collection.
fn configured_allocator_reclaim_force() -> bool {
    rustfs_utils::get_env_bool(rustfs_config::ENV_ALLOCATOR_RECLAIM_FORCE, rustfs_config::DEFAULT_ALLOCATOR_RECLAIM_FORCE)
}

/// Read the number of consecutive idle ticks required before reclaim.
fn configured_allocator_reclaim_idle_intervals() -> u64 {
    rustfs_utils::get_env_u64(
        rustfs_config::ENV_ALLOCATOR_RECLAIM_IDLE_INTERVALS,
        rustfs_config::DEFAULT_ALLOCATOR_RECLAIM_IDLE_INTERVALS,
    )
    .max(1)
}

/// Read the reclaim-loop tick interval in seconds.
fn configured_allocator_reclaim_interval_secs() -> u64 {
    rustfs_utils::get_env_u64(
        rustfs_config::ENV_ALLOCATOR_RECLAIM_INTERVAL_SECS,
        rustfs_config::DEFAULT_ALLOCATOR_RECLAIM_INTERVAL_SECS,
    )
    .max(1)
}

/// Apply backend support constraints to the configured force flag.
fn effective_allocator_reclaim_force(backend: &str, configured_force: bool) -> bool {
    configured_force && backend != "mimalloc-windows"
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

#[cfg(not(target_os = "windows"))]
fn collect_allocator_memory(force: bool) -> Result<(), String> {
    rustfs_mimalloc::MiMalloc::collect(force);
    Ok(())
}

#[cfg(target_os = "windows")]
fn collect_allocator_memory(_force: bool) -> Result<(), String> {
    Err("allocator reclaim is not supported on Windows".to_string())
}

/// Execute one allocator collection and publish the outcome metrics.
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

/// Start the allocator reclaim loop when `RUSTFS_ALLOCATOR_RECLAIM_ENABLED` is true.
///
/// The loop samples activity once per configured interval. Reclaim runs only
/// after `RUSTFS_ALLOCATOR_RECLAIM_IDLE_INTERVALS` consecutive samples show no
/// tracked work. With the current defaults, an enabled loop waits for roughly
/// 90 seconds of observed quiet time before calling mimalloc collection
/// (`30s * 3`). Configuration is sampled once at startup; changing the
/// environment later does not start, stop, or retune the existing worker.
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
    let force = effective_allocator_reclaim_force(backend, configured_force);
    let idle_intervals = configured_allocator_reclaim_idle_intervals();
    let interval = Duration::from_secs(configured_allocator_reclaim_interval_secs());

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Avoid catch-up bursts after the runtime has been busy or suspended.
        // Reclaim decisions are based on current idleness, not on the number
        // of missed historical ticks.
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
                        // This can return memory to the OS, but it may also
                        // reduce allocator locality for the next burst. Keep
                        // it outside active workload windows.
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
    fn allocator_reclaim_force_is_disabled_only_on_windows_backend() {
        assert!(!effective_allocator_reclaim_force("mimalloc-windows", true));
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
