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

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, RwLock};

use self::heal_info::{
    BackgroundHealInfoReadStatus, ScannerBitrotStart, read_background_heal_info_with_epoch, record_scanner_bitrot_status,
    resolve_background_heal_info_for_cycle, save_background_heal_info_for_epoch, scanner_bitrot_start_for_cycle,
};
use crate::data_usage_define::{
    BACKGROUND_HEAL_INFO_PATH, DATA_USAGE_BLOOM_NAME_PATH, DATA_USAGE_OBJ_NAME_PATH, DATA_USAGE_OBSERVED_OBJ_NAME_PATH,
    DATA_USAGE_RECOVERY_PATH, DataUsageCache, DataUsageCacheRevision, LEGACY_DATA_USAGE_OBJ_NAME_PATH, read_config_revision,
    read_config_with_revision,
};
use crate::runtime_config::{
    ScannerRuntimeConfig, ScannerRuntimeConfigSource, refresh_scanner_runtime_config_from_global, scanner_bitrot_cycle,
    scanner_cycle_interval, scanner_runtime_config_changed, scanner_runtime_config_generation, set_scanner_default_cycle_secs,
};
use crate::scanner_budget::{ScannerCycleBudget, ScannerCycleBudgetConfig, ScannerCycleBudgetReason};
use crate::scanner_folder::{data_usage_update_dir_cycles, heal_object_select_prob};
use crate::scanner_io::{
    ScannerCycleDeferReason, ScannerCycleResult, ScannerCycleStatus, dirty_usage_bucket_notified, dirty_usage_buckets_pending,
    dirty_usage_generation, scanner_dirty_usage_state, scanner_maintenance_changed, scanner_maintenance_generation,
};
use crate::sleeper::{SCANNER_SLEEPER, set_scanner_default_speed};
use crate::{DataUsageInfo, ScannerActivityGuard, ScannerError, ScannerRuntimeGuard};
use crate::{ScannerConfigObjectDelete, ScannerObjectIO, ScannerObjectOptions};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use rustfs_config::ScannerSpeed;
#[cfg(test)]
use rustfs_config::{
    ENV_SCANNER_BITROT_CYCLE_SECS, ENV_SCANNER_CYCLE_MAX_DIRECTORIES, ENV_SCANNER_CYCLE_MAX_DURATION_SECS,
    ENV_SCANNER_CYCLE_MAX_OBJECTS,
};
use rustfs_config::{ENV_SCANNER_CYCLE, ENV_SCANNER_SPEED, ENV_SCANNER_START_DELAY_SECS};
use rustfs_data_usage::observed_data_usage_is_newer;
use rustfs_heal_contracts::heal_channel::HealScanMode;
use rustfs_lock::{NamespaceLockGuard, error::LockError};
use rustfs_scanner_metrics::metrics::{
    CurrentCycle, Metric, Metrics, RemoteScanProgress, ScanCyclePartialReason, ScanCycleWorkSnapshot,
    ScannerUsagePublicationObservation, ScannerUsageSaveResult, ScannerWorkSource, emit_scan_cycle_complete,
    emit_scan_cycle_deferred, emit_scan_cycle_partial_with_source, emit_scan_cycle_superseded, global_metrics,
};
use rustfs_scanner_metrics::startup::ScannerStartupToken;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Notify, mpsc};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::storage_api::ScannerStorage;
use crate::storage_api::scan::{
    BucketOptions, NamespaceLocking as _, SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION, SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION,
    SCANNER_ACTIVITY_PROTOCOL_VERSION,
};
use crate::{
    DiskError, ECStore, EcstoreError, ListPathRawOptions, RUSTFS_META_BUCKET, SCANNER_PUBLICATION_EPOCH_CHANGED,
    ScannerLifecycleConfigExt as _, ScannerReplicationConfigExt as _, delete_config_with_publication_admission_for_epoch,
    get_lifecycle_config, get_replication_config, invalidate_admin_data_usage_snapshot_cache,
    invalidate_data_usage_snapshot_cache, list_path_raw, read_config, replace_bucket_usage_memory_from_info, save_config,
    save_config_shared_with_preconditions_and_lease_fence_and_scope, save_config_with_preconditions,
    save_config_with_publication_admission_for_epoch, scanner_publication_admission_for_epoch, scanner_publication_epoch,
    scanner_publication_epoch_changed,
};

const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
const LOG_SUBSYSTEM_BACKGROUND_HEAL: &str = "background_heal";
const EVENT_SCANNER_CYCLE_STATE: &str = "scanner_cycle_state";
const EVENT_SCANNER_LOCK_STATE: &str = "scanner_lock_state";
const EVENT_SCANNER_PERSIST_STATE: &str = "scanner_persist_state";
const EVENT_SCANNER_RUNTIME_CONFIG: &str = "scanner_runtime_config";
const EVENT_SCANNER_BACKGROUND_HEAL_STATE: &str = "scanner_background_heal_state";
const METRIC_SCANNER_LEADER_LOCK_TOTAL: &str = "rustfs_scanner_leader_lock_total";
const CLEAN_IDLE_MAX_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_SCANNER_SCHEDULE_DELAY: Duration = Duration::from_secs(365 * 24 * 60 * 60);

#[cfg(test)]
static SCANNER_STARTUP_OBSERVED_PROBE: LazyLock<StdMutex<Option<Arc<ScannerStartupObservedProbeState>>>> =
    LazyLock::new(|| StdMutex::new(None));

#[cfg(test)]
struct ScannerStartupObservedProbeState {
    observed: Notify,
    resume: Notify,
}

#[cfg(test)]
struct ScannerObservedProbeState {
    store_key: usize,
    paused: bool,
    notify: Notify,
}

#[cfg(test)]
pub(super) struct ScannerStartupObservedProbe {
    state: Arc<ScannerStartupObservedProbeState>,
}

#[cfg(test)]
static SCANNER_RUNTIME_OBSERVED_PROBE: LazyLock<StdMutex<Option<Arc<ScannerObservedProbeState>>>> =
    LazyLock::new(|| StdMutex::new(None));

#[cfg(test)]
pub(super) struct ScannerRuntimeObservedProbe {
    state: Arc<ScannerObservedProbeState>,
}

#[cfg(test)]
impl ScannerStartupObservedProbe {
    pub(super) fn install() -> Self {
        let state = Arc::new(ScannerStartupObservedProbeState {
            observed: Notify::new(),
            resume: Notify::new(),
        });
        let mut probe = SCANNER_STARTUP_OBSERVED_PROBE
            .lock()
            .expect("scanner startup observed probe should not be poisoned");
        assert!(probe.is_none(), "scanner startup observed probe must be unique");
        *probe = Some(state.clone());
        Self { state }
    }

    pub(super) async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.state.observed.notified())
            .await
            .expect("scanner should complete startup pause-backlog observation");
    }

    pub(super) fn resume(&self) {
        self.state.resume.notify_one();
    }
}

#[cfg(test)]
impl ScannerRuntimeObservedProbe {
    pub(super) fn install(storeapi: &Arc<ECStore>, paused: bool) -> Self {
        let state = Arc::new(ScannerObservedProbeState {
            store_key: scanner_observed_probe_store_key(storeapi),
            paused,
            notify: Notify::new(),
        });
        let mut probe = SCANNER_RUNTIME_OBSERVED_PROBE
            .lock()
            .expect("scanner runtime observed probe should not be poisoned");
        assert!(probe.is_none(), "scanner runtime observed probe must be unique");
        *probe = Some(state.clone());
        Self { state }
    }

    pub(super) async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.state.notify.notified())
            .await
            .expect("scanner should complete runtime pause-backlog observation");
    }
}

#[cfg(test)]
impl Drop for ScannerStartupObservedProbe {
    fn drop(&mut self) {
        let mut probe = SCANNER_STARTUP_OBSERVED_PROBE
            .lock()
            .expect("scanner startup observed probe should not be poisoned");
        if probe.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *probe = None;
        }
        self.state.resume.notify_one();
    }
}

#[cfg(test)]
impl Drop for ScannerRuntimeObservedProbe {
    fn drop(&mut self) {
        let mut probe = SCANNER_RUNTIME_OBSERVED_PROBE
            .lock()
            .expect("scanner runtime observed probe should not be poisoned");
        if probe.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *probe = None;
        }
    }
}

#[cfg(test)]
async fn notify_scanner_startup_observed_for_test() {
    let probe = {
        SCANNER_STARTUP_OBSERVED_PROBE
            .lock()
            .expect("scanner startup observed probe should not be poisoned")
            .clone()
    };
    if let Some(probe) = probe {
        probe.observed.notify_one();
        probe.resume.notified().await;
    }
}

#[cfg(test)]
fn scanner_observed_probe_store_key<S>(storeapi: &Arc<S>) -> usize
where
    S: ScannerStorage,
{
    storeapi.scanner_observed_probe_store_key()
}

#[cfg(test)]
fn notify_scanner_runtime_observed_for_test<S>(storeapi: &Arc<S>, observation: ScannerPauseBacklogObservation)
where
    S: ScannerStorage,
{
    if let Some(probe) = SCANNER_RUNTIME_OBSERVED_PROBE
        .lock()
        .expect("scanner runtime observed probe should not be poisoned")
        .clone()
        && probe.store_key == scanner_observed_probe_store_key(storeapi)
        && probe.paused == observation.paused
    {
        probe.notify.notify_one();
    }
}

const CLEAN_IDLE_BACKOFF_FACTOR: u32 = 2;
/// First-retry delay after a scanner cycle cannot publish authoritative usage.
///
/// A superseded cycle is the *expected* outcome of the dirty-usage fast path:
/// a write burst marks buckets dirty, the scanner wakes within milliseconds,
/// and the still-landing writes then supersede the snapshot it took. Charging
/// that first race a full cycle interval means a burst of writes surfaces in
/// usage/quota accounting roughly two cycles late (measured: ~120 s on an
/// otherwise idle instance whose clean-idle backoff had doubled a 60 s
/// interval), which defeats the fast path it is meant to protect.
///
/// The exponential growth in [`ScannerRetryBackoff::retry_interval`] is
/// what protects against a persistently hot bucket driving an unbroken
/// full-scan loop, so it can start small: 5 s, 10 s, 20 s … capped by
/// [`SCANNER_RETRY_MAX_INTERVAL`]. A one-off race recovers in seconds; a
/// genuinely hot bucket still reaches minute-scale backoff within a handful of
/// cycles. Preflight deferrals use the same bounded schedule so a temporarily
/// unavailable peer cannot drive a tight retry loop.
const SCANNER_RETRY_BASE_INTERVAL: Duration = Duration::from_secs(5);
const SCANNER_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// Probe cadence while data movement pauses the scanner. Recovery retries use
/// the bounded backoff in `cycle_state` instead of a flat pause.
const SCANNER_CYCLE_RECOVERY_PAUSED_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Permanent recovery states still get a sparse status probe so a reset that
/// races the wait registration cannot leave the scanner asleep forever.
const SCANNER_CYCLE_RECOVERY_BLOCKED_PROBE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const SCANNER_LEADER_LOCK_POLL_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(not(test))]
const SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(50);
const MAINTENANCE_FEATURE_INSPECTION_TIMEOUT: Duration = Duration::from_secs(30);
const SCANNER_STARTUP_USAGE_INSPECTION_TIMEOUT: Duration = Duration::from_secs(30);
const MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MAINTENANCE_FEATURE_INSPECTION_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);
const MAX_MAINTENANCE_FEATURE_INSPECTION_ATTEMPTS: usize = 2;
const SCANNER_PERSIST_CAS_RETRIES: usize = 2;
const DATA_USAGE_BACKUP_INTERVAL_CYCLES: u64 = 10;
const SCANNER_PUBLICATION_LEASE_FENCE_MAX_ENTRIES: usize = 256;
const SCANNER_PUBLICATION_LEASE_FENCE_MAX_BYTES: usize = 64 * 1024;
const SCANNER_CYCLE_STATE_MAGIC: &[u8; 8] = b"RSCYC001";
const SCANNER_CYCLE_STATE_HEADER_LEN: usize = 24;
#[cfg(test)]
const ENV_SCANNER_START_DELAY_SECS_DEPRECATED: &str = "RUSTFS_DATA_SCANNER_START_DELAY_SECS";
#[cfg(test)]
type ScannerCycleStatePersistTestHook = (u64, Arc<Notify>);
#[cfg(test)]
static SCANNER_CYCLE_STATE_PERSIST_TEST_HOOK: LazyLock<StdMutex<Option<ScannerCycleStatePersistTestHook>>> =
    LazyLock::new(|| StdMutex::new(None));

static SCANNER_CYCLE_RECOVERY_WAKE: LazyLock<Notify> = LazyLock::new(Notify::new);

fn remote_publication_lease_fence_targets_are_required(target_count: usize, grants_present: bool, fence_present: bool) -> bool {
    target_count > 0 && (!grants_present || !fence_present)
}

pub(super) fn notify_scanner_cycle_recovery_wake() {
    SCANNER_CYCLE_RECOVERY_WAKE.notify_one();
}

#[cfg(test)]
struct ScannerCycleStatePersistTestHookGuard;

#[cfg(test)]
impl Drop for ScannerCycleStatePersistTestHookGuard {
    fn drop(&mut self) {
        *SCANNER_CYCLE_STATE_PERSIST_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[cfg(test)]
fn set_scanner_cycle_state_persist_test_hook(leader_epoch: u64, reached: Arc<Notify>) -> ScannerCycleStatePersistTestHookGuard {
    *SCANNER_CYCLE_STATE_PERSIST_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((leader_epoch, reached));
    ScannerCycleStatePersistTestHookGuard
}

#[cfg(test)]
fn notify_scanner_cycle_state_persist_test_hook(leader_epoch: u64) {
    let reached = SCANNER_CYCLE_STATE_PERSIST_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .filter(|(expected_epoch, _)| *expected_epoch == leader_epoch)
        .map(|(_, reached)| reached.clone());
    if let Some(reached) = reached {
        reached.notify_one();
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[non_exhaustive]
pub struct ScannerCycleScheduleStatus {
    execution_role: &'static str,
    effective_interval_available: bool,
    effective_interval_seconds: u64,
    usage_bootstrap_rebuild_pending: bool,
    clean_idle_backoff_enabled: bool,
    clean_idle_backoff_multiplier: u64,
    superseded_retry_backoff_enabled: bool,
    superseded_cycles: u32,
}

impl Default for ScannerCycleScheduleStatus {
    fn default() -> Self {
        Self {
            execution_role: "unknown",
            effective_interval_available: false,
            effective_interval_seconds: 0,
            usage_bootstrap_rebuild_pending: false,
            clean_idle_backoff_enabled: false,
            clean_idle_backoff_multiplier: 1,
            superseded_retry_backoff_enabled: false,
            superseded_cycles: 0,
        }
    }
}

impl ScannerCycleScheduleStatus {
    pub fn effective_interval_seconds(self) -> u64 {
        self.effective_interval_seconds
    }
}

static SCANNER_CYCLE_SCHEDULE: LazyLock<RwLock<ScannerCycleScheduleStatus>> =
    LazyLock::new(|| RwLock::new(ScannerCycleScheduleStatus::default()));

pub fn scanner_cycle_schedule_status() -> ScannerCycleScheduleStatus {
    *SCANNER_CYCLE_SCHEDULE.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn record_scanner_cycle_schedule(
    effective_interval: Duration,
    usage_bootstrap_rebuild_pending: bool,
    clean_idle_backoff_enabled: bool,
    clean_idle_backoff_multiplier: u64,
    superseded_retry_backoff_enabled: bool,
    superseded_cycles: u32,
) {
    let effective_interval_seconds = effective_interval
        .as_secs()
        .saturating_add(u64::from(effective_interval.subsec_nanos() != 0));
    let mut schedule = SCANNER_CYCLE_SCHEDULE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *schedule = ScannerCycleScheduleStatus {
        execution_role: "leader",
        effective_interval_available: true,
        effective_interval_seconds,
        usage_bootstrap_rebuild_pending,
        clean_idle_backoff_enabled,
        clean_idle_backoff_multiplier: clean_idle_backoff_multiplier.max(1),
        superseded_retry_backoff_enabled,
        superseded_cycles,
    };
}

fn record_scanner_cycle_schedule_role(execution_role: &'static str) {
    let mut schedule = SCANNER_CYCLE_SCHEDULE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *schedule = ScannerCycleScheduleStatus {
        execution_role,
        ..ScannerCycleScheduleStatus::default()
    };
}

fn reset_scanner_cycle_schedule() {
    record_scanner_cycle_schedule_role("unknown");
}

enum ScannerLeaderLockFailure<'a> {
    Contended,
    Failed(&'a LockError),
}

fn classify_scanner_leader_lock_failure(error: &LockError) -> ScannerLeaderLockFailure<'_> {
    match error {
        LockError::Timeout { .. } => ScannerLeaderLockFailure::Contended,
        error => ScannerLeaderLockFailure::Failed(error),
    }
}

/// Returns the base cycle interval.
/// Priority order:
/// 1. RUSTFS_SCANNER_CYCLE (if set, overrides everything)
/// 2. RUSTFS_SCANNER_START_DELAY_SECS (for backward compatibility)
/// 3. Deployment-specific default cycle override
/// 4. RUSTFS_SCANNER_SPEED preset
#[cfg(test)]
fn cycle_interval() -> Duration {
    resolve_scanner_runtime_config().cycle_interval
}

fn scanner_cycle_budget_config() -> ScannerCycleBudgetConfig {
    resolve_scanner_runtime_config().cycle_budget
}

fn record_scanner_leader_lock_state(state: &'static str) {
    metrics::counter!(
        METRIC_SCANNER_LEADER_LOCK_TOTAL,
        "state" => state
    )
    .increment(1);
}

async fn finish_scanner_leader_iteration(lock_lost: bool, state: &'static str, error: String) {
    reset_scanner_cycle_schedule();
    let liveness_already_recorded = lock_lost && !global_metrics().report().await.leader_lock_held_by_this_process;
    if !liveness_already_recorded {
        global_metrics().record_scanner_leader_liveness(state, false, error).await;
    }
}

#[cfg(test)]
fn scanner_cycle_max_duration() -> Option<Duration> {
    resolve_scanner_runtime_config().cycle_budget.max_duration
}

fn resolve_scanner_runtime_config() -> crate::runtime_config::ScannerRuntimeConfig {
    #[cfg(test)]
    {
        crate::runtime_config::resolve_scanner_runtime_config_from_global()
    }
    #[cfg(not(test))]
    {
        crate::runtime_config::current_scanner_runtime_config()
    }
}

fn scan_cycle_partial_reason(reason: Option<ScannerCycleBudgetReason>) -> ScanCyclePartialReason {
    match reason {
        Some(ScannerCycleBudgetReason::Runtime) => ScanCyclePartialReason::Runtime,
        Some(ScannerCycleBudgetReason::Objects) => ScanCyclePartialReason::Objects,
        Some(ScannerCycleBudgetReason::Directories) => ScanCyclePartialReason::Directories,
        None => ScanCyclePartialReason::Unknown,
    }
}

fn scan_cycle_partial_source(reason: Option<ScannerCycleBudgetReason>) -> Option<ScannerWorkSource> {
    match reason {
        Some(ScannerCycleBudgetReason::Objects | ScannerCycleBudgetReason::Directories) => Some(ScannerWorkSource::Usage),
        Some(ScannerCycleBudgetReason::Runtime) | None => None,
    }
}

/// Compute a randomized inter-cycle sleep.
// Delay is scan interval +- 10%, with a floor of 1 second.
fn randomized_cycle_delay() -> Duration {
    randomized_cycle_delay_for(scanner_cycle_interval())
}

fn randomized_cycle_delay_for(interval: Duration) -> Duration {
    let interval = interval.max(Duration::from_secs(1)).min(MAX_SCANNER_SCHEDULE_DELAY);
    // Uniform in [-0.1, 0.1), keeping actual delay within 10% of interval.
    let jitter_factor = (rand::random::<f64>() * 0.2) - 0.1;
    let delay = interval.mul_f64(1.0 + jitter_factor);
    delay.max(Duration::from_secs(1)).min(MAX_SCANNER_SCHEDULE_DELAY)
}

fn cap_clean_idle_cycle_delay(delay: Duration, max_interval: Duration, enabled: bool) -> Duration {
    if !enabled {
        return delay;
    }

    let max_interval = max_interval.max(Duration::from_secs(1));
    if delay <= max_interval {
        return delay;
    }

    // Reflect positive jitter below the cap instead of collapsing every
    // positive sample onto the same instant once backoff reaches its ceiling.
    max_interval
        .saturating_sub(delay.saturating_sub(max_interval))
        .max(Duration::from_secs(1))
}

// None starts promptly; Some is the normalized interval to jitter once.
fn scanner_startup_delay_policy(
    config: &ScannerRuntimeConfig,
    usage_cache_is_cold: bool,
    has_buckets: bool,
    has_active_replication: bool,
) -> Option<Duration> {
    if usage_cache_is_cold || (has_active_replication && has_buckets) {
        return None;
    }
    Some(
        config
            .start_delay
            .unwrap_or_else(|| config.cycle_interval.min(Duration::from_secs(60)))
            .max(Duration::from_secs(1))
            .min(MAX_SCANNER_SCHEDULE_DELAY),
    )
}

async fn wait_for_scanner_runtime_wake(
    ctx: &CancellationToken,
    deadline: Instant,
    runtime_generation: u64,
    maintenance_generation: u64,
) -> ScannerCycleWakeReason {
    let timer = tokio::time::sleep_until(deadline);
    tokio::pin!(timer);
    loop {
        if ctx.is_cancelled() {
            return ScannerCycleWakeReason::Cancelled;
        }
        if scanner_runtime_config_generation() != runtime_generation {
            return ScannerCycleWakeReason::RuntimeConfig;
        }
        if scanner_maintenance_generation() != maintenance_generation {
            return ScannerCycleWakeReason::MaintenanceConfig;
        }
        tokio::select! {
            biased;
            _ = ctx.cancelled() => return ScannerCycleWakeReason::Cancelled,
            _ = &mut timer => return ScannerCycleWakeReason::Timer,
            _ = SCANNER_CYCLE_RECOVERY_WAKE.notified() => return ScannerCycleWakeReason::Recovery,
            _ = scanner_runtime_config_changed() => {},
            _ = scanner_maintenance_changed() => {},
        }
    }
}

async fn wait_for_scanner_startup<F, J>(
    startup: ScannerStartupToken,
    ctx: &CancellationToken,
    inspection: F,
    replication_active: bool,
    maintenance_generation: u64,
    mut sample_delay: J,
) -> bool
where
    F: Future<Output = (bool, bool)>,
    J: FnMut(Duration) -> Duration,
{
    let started = Instant::now();
    if ctx.is_cancelled() {
        return false;
    }
    if scanner_maintenance_generation() != maintenance_generation {
        return true;
    }
    global_metrics().startup().record_usage_inspection(startup);
    let (usage_cache_is_cold, has_buckets) = tokio::select! {
        biased;
        _ = ctx.cancelled() => return false,
        result = tokio::time::timeout(SCANNER_STARTUP_USAGE_INSPECTION_TIMEOUT, inspection) => match result {
            Ok(state) => state,
            Err(_) => {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_RUNTIME_CONFIG,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    state = "startup_usage_inspection_timed_out",
                    "Scanner startup usage inspection timed out; proceeding to the fenced scan attempt"
                );
                return true;
            }
        },
    };
    let mut runtime_generation = scanner_runtime_config_generation();
    let mut policy = scanner_startup_delay_policy(
        &crate::runtime_config::current_scanner_runtime_config(),
        usage_cache_is_cold,
        has_buckets,
        replication_active,
    );
    let delay = policy.map(&mut sample_delay).unwrap_or_default();
    if policy.is_none() {
        info!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "startup_delay_skipped",
            reason = if usage_cache_is_cold { "usage_cache_cold" } else { "replication_active" },
            "Scanner startup delay skipped"
        );
    }
    let mut deadline = started + delay;
    if policy.is_some() {
        global_metrics().startup().record_wait(startup, deadline);
    }
    loop {
        let wake = wait_for_scanner_runtime_wake(ctx, deadline, runtime_generation, maintenance_generation).await;
        if wake != ScannerCycleWakeReason::RuntimeConfig {
            return wake != ScannerCycleWakeReason::Cancelled;
        }
        runtime_generation = scanner_runtime_config_generation();
        let updated = scanner_startup_delay_policy(
            &crate::runtime_config::current_scanner_runtime_config(),
            usage_cache_is_cold,
            has_buckets,
            replication_active,
        );
        if updated != policy {
            // Equivalent policies retain their deadline and jitter sample.
            policy = updated;
            deadline = started + policy.map(&mut sample_delay).unwrap_or_default();
            global_metrics().startup().record_wait(startup, deadline);
        }
    }
}

fn data_usage_info_is_cold(info: &DataUsageInfo) -> bool {
    !info.is_complete_bucket_usage_snapshot()
}

pub(super) fn data_usage_info_has_persisted_baseline_identity(info: &DataUsageInfo) -> bool {
    if info.is_complete_bucket_usage_snapshot() {
        return true;
    }

    // Pre-marker snapshots remain readable only when their legacy identity is
    // complete: a timestamp, a scanner cycle, and an exact bucket cardinality.
    // A current snapshot with only scanner_epoch/scanner_cycle (or an explicit
    // incomplete marker) is not evidence of a durable usage baseline.
    !info.usage_snapshot_bootstrap_pending
        && !info.usage_snapshot_complete
        && info.scanner_epoch.is_none()
        && info.usage_snapshot_converged != Some(false)
        && info.last_update.is_some()
        && info.scanner_cycle.is_some()
        && u64::try_from(info.buckets_usage.len()).ok() == Some(info.buckets_count)
}

pub(super) fn data_usage_info_is_bootstrap_pending(info: &DataUsageInfo) -> bool {
    let Some(last_update) = info.last_update else {
        return false;
    };

    info == &scanner_usage_bootstrap_marker(last_update, info.scanner_epoch)
}

pub(super) fn scanner_usage_bootstrap_marker(last_update: std::time::SystemTime, scanner_epoch: Option<u64>) -> DataUsageInfo {
    DataUsageInfo {
        last_update: Some(last_update),
        scanner_epoch,
        usage_snapshot_converged: Some(false),
        usage_snapshot_bootstrap_pending: true,
        ..Default::default()
    }
}

fn usage_cache_needs_prompt_scan(authoritative: &DataUsageInfo, observed: Option<&DataUsageInfo>) -> bool {
    data_usage_info_is_cold(authoritative)
        || observed.is_some_and(|observed| observed_data_usage_is_newer(observed, authoritative))
}

async fn read_data_usage_config_for_startup(storeapi: &Arc<impl ScannerObjectIO>) -> Result<Option<Vec<u8>>, EcstoreError> {
    async fn read_pair(storeapi: &Arc<impl ScannerObjectIO>, primary_path: &str) -> Result<Option<Vec<u8>>, EcstoreError> {
        match read_config(storeapi.clone(), primary_path).await {
            Ok(data) => Ok(Some(data)),
            Err(EcstoreError::ConfigNotFound) => {
                let backup_path = format!("{primary_path}.bkp");
                match read_config(storeapi.clone(), backup_path.as_str()).await {
                    Ok(data) => Ok(Some(data)),
                    Err(EcstoreError::ConfigNotFound) => Ok(None),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        }
    }

    for path in [DATA_USAGE_OBJ_NAME_PATH.as_str(), LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()] {
        if let Some(data) = read_pair(storeapi, path).await? {
            return Ok(Some(data));
        }
    }
    Ok(None)
}

fn data_usage_backup_due(data_usage_info: &DataUsageInfo) -> bool {
    data_usage_info
        .scanner_cycle
        .is_some_and(|cycle| cycle % DATA_USAGE_BACKUP_INTERVAL_CYCLES == 0)
}

#[cfg(test)]
#[allow(dead_code)]
async fn sync_data_usage_backup_from_primary(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
) -> Result<(), EcstoreError> {
    sync_data_usage_backup_from_primary_for_epoch_and_lease_and_fence(ctx, storeapi, None, None, None).await
}

#[allow(dead_code)]
async fn sync_data_usage_backup_from_primary_for_epoch_and_lease_and_fence(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    expected_publication_epoch: Option<u64>,
    remote_lease_deadline: Option<std::time::Instant>,
    scanner_publication_lease_fence: Option<&str>,
) -> Result<(), EcstoreError> {
    sync_data_usage_backup_from_primary_for_epoch_and_lease_and_fence_and_scope(
        ctx,
        storeapi,
        expected_publication_epoch,
        remote_lease_deadline,
        scanner_publication_lease_fence,
        Vec::new(),
        Arc::new(AtomicBool::new(true)),
    )
    .await
}

async fn sync_data_usage_backup_from_primary_for_epoch_and_lease_and_fence_and_scope(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    expected_publication_epoch: Option<u64>,
    remote_lease_deadline: Option<std::time::Instant>,
    scanner_publication_lease_fence: Option<&str>,
    remote_lease_tokens: Vec<Uuid>,
    lease_release_safe: Arc<AtomicBool>,
) -> Result<(), EcstoreError> {
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    for retry in 0..=SCANNER_PERSIST_CAS_RETRIES {
        if ctx.is_cancelled() {
            return Ok(());
        }
        if remote_lease_deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
        }

        let read_epoch = match expected_publication_epoch {
            Some(expected_epoch) => {
                if scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                    .await
                    .is_none()
                {
                    return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
                }
                expected_epoch
            }
            None => scanner_publication_epoch(storeapi.clone())
                .await
                .ok_or_else(|| EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED))?,
        };
        let (primary, _) = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await?;
        let primary = primary.ok_or(EcstoreError::ConfigNotFound)?;
        let primary_info = serde_json::from_slice::<DataUsageInfo>(&primary)
            .map_err(|err| EcstoreError::other(format!("authoritative data usage snapshot is invalid: {err}")))?;
        if !data_usage_info_has_persisted_baseline_identity(&primary_info) {
            return Err(EcstoreError::other(
                "authoritative data usage snapshot has no persisted baseline identity",
            ));
        }
        let primary = Bytes::from(primary);

        if remote_lease_deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
        }

        let (backup, revision) = read_config_with_revision(storeapi.clone(), &backup_path).await?;
        if backup.as_deref() == Some(primary.as_ref()) {
            if scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch)
                .await
                .is_some()
            {
                return Ok(());
            }
            if retry < SCANNER_PERSIST_CAS_RETRIES {
                continue;
            }
            return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
        }

        let sha256hex = Some(hex_simd::encode_to_string(Sha256::digest(&primary), hex_simd::AsciiCase::Lower));
        let save_result = {
            if remote_lease_deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
            }
            let Some(_publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch).await else {
                if retry < SCANNER_PERSIST_CAS_RETRIES {
                    continue;
                }
                return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
            };
            let publication_scope = match expected_publication_epoch {
                Some(expected_epoch) => {
                    storeapi
                        .scanner_data_usage_publication_commit_scope_with_release_flag(
                            expected_epoch,
                            usage_store::scanner_publication_scope_deadline(data_usage_persist_timeout(), remote_lease_deadline),
                            remote_lease_tokens.clone(),
                            Arc::clone(&lease_release_safe),
                        )
                        .await
                }
                None => None,
            };
            if expected_publication_epoch.is_some() && publication_scope.is_none() {
                if retry < SCANNER_PERSIST_CAS_RETRIES {
                    continue;
                }
                return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
            }
            let save_result = save_config_shared_with_preconditions_and_lease_fence_and_scope(
                storeapi.clone(),
                &backup_path,
                primary.clone(),
                sha256hex,
                revision.preconditions(),
                scanner_publication_lease_fence,
                publication_scope.clone(),
            )
            .await;
            if let Some(scope) = publication_scope {
                match scope.wait_for_completion().await {
                    crate::storage_api::owner::ScannerPublicationCommitState::Committed
                    | crate::storage_api::owner::ScannerPublicationCommitState::AbortedBeforeCommit => save_result,
                    crate::storage_api::owner::ScannerPublicationCommitState::Indeterminate
                    | crate::storage_api::owner::ScannerPublicationCommitState::Admitted
                    | crate::storage_api::owner::ScannerPublicationCommitState::InFlight => Err(EcstoreError::other(
                        "scanner backup publication commit scope did not reach a safe terminal state",
                    )),
                }
            } else {
                save_result
            }
        };

        match save_result {
            Ok(_) => {}
            Err(err) => {
                let (observed, _) = read_config_with_revision(storeapi.clone(), &backup_path).await?;
                if observed.as_deref() == Some(primary.as_ref()) {
                    // The write committed even though the response was lost.
                } else if err == EcstoreError::PreconditionFailed && retry < SCANNER_PERSIST_CAS_RETRIES {
                    continue;
                } else {
                    return Err(err);
                }
            }
        }

        let (current_primary, _) = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await?;
        if current_primary.as_deref() == Some(primary.as_ref())
            && scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch)
                .await
                .is_some()
        {
            return Ok(());
        }
        if expected_publication_epoch.is_some()
            && scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch)
                .await
                .is_none()
        {
            return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
        }
        if retry < SCANNER_PERSIST_CAS_RETRIES {
            continue;
        }
    }

    Err(EcstoreError::other(
        "authoritative data usage snapshot changed while synchronizing its backup",
    ))
}

async fn persisted_usage_cache_is_cold_for_startup<S>(storeapi: &Arc<S>) -> bool
where
    S: ScannerObjectIO + ScannerConfigObjectDelete,
{
    let Some(data) = (match read_data_usage_config_for_startup(storeapi).await {
        Ok(data) => data,
        Err(err) => {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                state = "startup_inspect_failed",
                error = %err,
                "Scanner startup cache inspection failed"
            );
            return false;
        }
    }) else {
        return true;
    };

    match serde_json::from_slice::<DataUsageInfo>(&data) {
        Ok(info) => {
            if data_usage_info_is_cold(&info) {
                return true;
            }
            match read_config(storeapi.clone(), DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()).await {
                Ok(observed) => match serde_json::from_slice::<DataUsageInfo>(&observed) {
                    Ok(observed) => usage_cache_needs_prompt_scan(&info, Some(&observed)),
                    Err(err) => {
                        warn!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                            state = "startup_observed_decode_failed",
                            error = %err,
                            "Scanner startup found an invalid observational snapshot and will refresh it promptly"
                        );
                        true
                    }
                },
                Err(EcstoreError::ConfigNotFound) => false,
                Err(err) => {
                    warn!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                        state = "startup_observed_inspect_failed",
                        error = %err,
                        "Scanner startup could not inspect the observational snapshot"
                    );
                    false
                }
            }
        }
        Err(err) => {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                state = "startup_decode_failed",
                error = %err,
                "Scanner startup cache decode failed"
            );
            true
        }
    }
}

async fn initial_scanner_startup_usage_state<S>(storeapi: &Arc<S>, replication_active: bool) -> (bool, bool)
where
    S: ScannerObjectIO + ScannerConfigObjectDelete + crate::storage_api::scan::BucketOperations<Error = EcstoreError>,
{
    let cold = persisted_usage_cache_is_cold_for_startup(storeapi).await;
    if cold || !replication_active {
        return (cold, false);
    }
    let has_buckets = match storeapi
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
    {
        Ok(buckets) => !buckets.is_empty(),
        Err(err) => {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_RUNTIME_CONFIG,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "startup_bucket_inspect_failed",
                error = %err,
                "Scanner startup bucket inspection failed"
            );
            false
        }
    };
    (cold, has_buckets)
}

fn prepare_cycle_for_usage_floor_bootstrap(
    cycle_info: &mut CurrentCycle,
    usage_floor: PersistedUsageFloor,
    startup: PersistedUsageFloorStartup,
) -> (bool, ScannerCycleResetPolicy) {
    match startup {
        PersistedUsageFloorStartup::Authoritative => (false, ScannerCycleResetPolicy::None),
        PersistedUsageFloorStartup::Missing => {
            // Cycle progress without its corresponding usage floor cannot
            // prove namespace coverage. Restart from cycle zero while keeping
            // the separately fenced leader epoch monotonic.
            *cycle_info = CurrentCycle::default();
            (true, ScannerCycleResetPolicy::ResetAll)
        }
        PersistedUsageFloorStartup::BootstrapPending => {
            // An unfenced marker may have been written before an upgrade's old
            // cycle state was replaced. A fenced marker belongs to the current
            // scanner generation and may retain partial-cycle progress.
            if usage_floor.leader_epoch == 0 {
                *cycle_info = CurrentCycle::default();
            }
            (
                true,
                if usage_floor.leader_epoch == 0 {
                    ScannerCycleResetPolicy::ResetAll
                } else {
                    ScannerCycleResetPolicy::None
                },
            )
        }
        PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence => {
            // The legacy incomplete fence proves only its leader epoch, not
            // namespace coverage. Clear coverage while retaining the durable
            // cycle number so surviving caches cannot force a regression.
            let next = cycle_info.next;
            *cycle_info = CurrentCycle {
                next,
                ..Default::default()
            };
            (true, ScannerCycleResetPolicy::ResetCoveragePreservingNext)
        }
    }
}

pub async fn init_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) {
    init_data_scanner_with_storage(ctx, storeapi, ScannerRuntimeGuard::new()).await;
}

async fn run_scanner_usage_recovery_intents_for_startup(
    ctx: CancellationToken,
    storeapi: Arc<ECStore>,
) -> Result<usize, ScannerError> {
    let intent_ids = scanner_usage_recovery_intents_for_startup(&ctx, storeapi.clone()).await?;
    let mut attempted = 0usize;
    for intent_id in intent_ids {
        if ctx.is_cancelled() {
            break;
        }
        run_scanner_usage_recovery_intent(ctx.child_token(), storeapi.clone(), intent_id).await?;
        attempted = attempted.saturating_add(1);
    }
    Ok(attempted)
}

/// Start normal scanning when enabled, or one bounded recovery attempt.
/// The disabled branch returns a finite task for the startup owner to join;
/// it never enables ordinary namespace scanning while it replays durable reset
/// intents and cleanup markers.
pub async fn init_scanner_with_recovery(
    ctx: CancellationToken,
    storeapi: Arc<ECStore>,
    enabled: bool,
) -> Option<tokio::task::JoinHandle<()>> {
    if enabled {
        let runtime_guard = ScannerRuntimeGuard::new();
        if let Err(error) = run_scanner_usage_recovery_intents_for_startup(ctx.clone(), storeapi.clone()).await {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "recovery_intent_startup_discovery_failed",
                error = %error,
                "Scanner recovery intent startup discovery failed"
            );
        }
        init_data_scanner_with_storage(ctx, storeapi, runtime_guard).await;
        return None;
    }
    Some(tokio::spawn(async move {
        if let Err(error) = run_scanner_usage_recovery_intents_for_startup(ctx.clone(), storeapi.clone()).await {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "recovery_intent_startup_discovery_failed",
                error = %error,
                "Scanner recovery intent startup discovery failed"
            );
        }
        if let Err(error) = resume_scanner_cycle_cleanup(ctx, storeapi).await {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "disabled_cleanup_deferred",
                error = %error,
                "Disabled scanner cleanup remains pending for an operator retry"
            );
        }
    }))
}

async fn init_data_scanner_with_storage<S>(ctx: CancellationToken, storeapi: Arc<S>, mut runtime_guard: ScannerRuntimeGuard)
where
    S: ScannerStorage,
{
    let startup = runtime_guard.startup_token();
    let (startup_features, startup_maintenance_generation) = configure_scanner_defaults(&ctx, &storeapi).await;
    // Force init global sleeper so config is read once at startup.
    let _ = &*SCANNER_SLEEPER;
    if let Err(err) = refresh_scanner_runtime_config_from_global() {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_RUNTIME_CONFIG,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "startup_apply_failed",
            error = %err,
            "Scanner runtime config apply failed at startup"
        );
    }

    runtime_guard.mark_initialized();
    let replication_active = startup_features.replication;
    let ctx_clone = ctx;
    let storeapi_clone = storeapi;
    tokio::spawn(async move {
        let _runtime_guard = runtime_guard;
        if !wait_for_scanner_startup(
            startup,
            &ctx_clone,
            initial_scanner_startup_usage_state(&storeapi_clone, replication_active),
            replication_active,
            startup_maintenance_generation.unwrap_or_else(scanner_maintenance_generation),
            randomized_cycle_delay_for,
        )
        .await
        {
            return;
        }

        let mut transient_backoff = ScannerRetryBackoff::default();
        let mut recovery_retry_count = 0_u32;
        loop {
            if ctx_clone.is_cancelled() {
                break;
            }

            let runtime_generation = scanner_runtime_config_generation();
            let maintenance_generation = scanner_maintenance_generation();
            let run_result = run_data_scanner_with_maintenance_state(
                ctx_clone.clone(),
                storeapi_clone.clone(),
                startup_features,
                startup_maintenance_generation,
                startup,
            )
            .await;
            if let Err(e) = &run_result {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_CYCLE_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    state = "run_failed",
                    error = %e,
                    "Scanner runtime iteration failed"
                );
            }
            // A retryable recovery observation never becomes terminal: the
            // retry is recorded with a bounded backoff and the observation
            // itself names the next attempt (rustfs/rustfs#8297, #8670).
            let recovery_retry = if scanner_cycle_recovery_status().retryable {
                let retry = record_scanner_cycle_recovery_retry(recovery_retry_count.saturating_add(1), scanner_cycle_interval());
                recovery_retry_count = retry.attempt;
                Some(retry.delay)
            } else {
                recovery_retry_count = 0;
                None
            };

            let recovery_status = scanner_cycle_recovery_status();
            if !recovery_status.retryable
                && matches!(recovery_status.state.as_str(), "blocked" | "recovery-required" | "cleanup-pending")
            {
                transient_backoff.record_retryable_cycle(false);
                tokio::select! {
                    _ = ctx_clone.cancelled() => break,
                    _ = SCANNER_CYCLE_RECOVERY_WAKE.notified() => {},
                    _ = tokio::time::sleep(SCANNER_CYCLE_RECOVERY_BLOCKED_PROBE_INTERVAL) => {},
                }
                continue;
            }

            let retry_delay = if let Some(delay) = recovery_retry {
                transient_backoff.record_retryable_cycle(true);
                delay
            } else if run_result.is_err() {
                transient_backoff.record_retryable_cycle(true);
                transient_backoff
                    .retry_interval(scanner_cycle_interval())
                    .unwrap_or(SCANNER_RETRY_BASE_INTERVAL)
            } else {
                transient_backoff.record_retryable_cycle(false);
                randomized_cycle_delay()
            };
            if wait_for_scanner_runtime_wake(&ctx_clone, Instant::now() + retry_delay, runtime_generation, maintenance_generation)
                .await
                == ScannerCycleWakeReason::Cancelled
            {
                break;
            }
        }
    });
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ScannerMaintenanceFeatures {
    lifecycle: bool,
    replication: bool,
    inspection_failed: bool,
}

impl ScannerMaintenanceFeatures {
    fn needs_regular_cycle(self) -> bool {
        self.lifecycle || self.replication || self.inspection_failed
    }

    fn requires_full_scan(
        self,
        observed_generation: Option<u64>,
        current_generation: u64,
        wake: ScannerCycleWakeReason,
        superseded_retry: bool,
    ) -> bool {
        self.needs_regular_cycle()
            || observed_generation != Some(current_generation)
            || !(matches!(wake, ScannerCycleWakeReason::DirtyUsage | ScannerCycleWakeReason::ClusterActivity)
                || (wake == ScannerCycleWakeReason::Timer && superseded_retry))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScannerUsageBootstrapRebuild {
    pending: bool,
}

impl ScannerUsageBootstrapRebuild {
    fn from_startup(startup: PersistedUsageFloorStartup) -> Self {
        Self {
            pending: startup != PersistedUsageFloorStartup::Authoritative,
        }
    }

    fn pending(self) -> bool {
        self.pending
    }

    fn wait_plan(self, mut plan: ScannerCycleWaitPlan, convergence_retry_interval: Option<Duration>) -> ScannerCycleWaitPlan {
        if self.pending && convergence_retry_interval.is_none() {
            plan.delay = Duration::ZERO;
        }
        plan
    }

    fn clean_idle_backoff_enabled(self, enabled: bool) -> bool {
        enabled && !self.pending
    }

    fn record_cycle(&mut self, outcome: ScannerCycleOutcome) -> bool {
        if matches!(
            outcome,
            ScannerCycleOutcome::Completed | ScannerCycleOutcome::CompletedWithPendingMaintenance
        ) {
            let was_pending = self.pending;
            self.pending = false;
            return was_pending;
        }
        false
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaintenanceInspectionDecision {
    Accept,
    Retry,
    PreserveBaseCycle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaintenanceInspectionAttempt {
    Completed(ScannerMaintenanceFeatures),
    TimedOut,
    Cancelled,
}

async fn wait_for_maintenance_feature_inspection<F>(
    ctx: &CancellationToken,
    inspection: F,
    timeout: Duration,
) -> MaintenanceInspectionAttempt
where
    F: Future<Output = ScannerMaintenanceFeatures>,
{
    tokio::select! {
        _ = ctx.cancelled() => MaintenanceInspectionAttempt::Cancelled,
        result = tokio::time::timeout(timeout, inspection) => match result {
            Ok(features) => MaintenanceInspectionAttempt::Completed(features),
            Err(_) => MaintenanceInspectionAttempt::TimedOut,
        },
    }
}

fn maintenance_inspection_decision(generation: u64, current_generation: u64, attempts: usize) -> MaintenanceInspectionDecision {
    if generation == current_generation {
        MaintenanceInspectionDecision::Accept
    } else if attempts < MAX_MAINTENANCE_FEATURE_INSPECTION_ATTEMPTS {
        MaintenanceInspectionDecision::Retry
    } else {
        MaintenanceInspectionDecision::PreserveBaseCycle
    }
}

fn single_disk_default_speed() -> ScannerSpeed {
    ScannerSpeed::Default
}

async fn detect_scanner_maintenance_features<S>(storeapi: &Arc<S>) -> ScannerMaintenanceFeatures
where
    S: ScannerStorage,
{
    let mut features = ScannerMaintenanceFeatures::default();
    let buckets = match storeapi
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
    {
        Ok(buckets) => buckets,
        Err(err) => {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_RUNTIME_CONFIG,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "maintenance_feature_inspect_failed",
                error = %err,
                "Scanner maintenance feature inspection failed; preserving speed-based cycle"
            );
            features.inspection_failed = true;
            return features;
        }
    };

    for bucket in buckets {
        if !features.lifecycle {
            match get_lifecycle_config(&bucket.name).await {
                Ok((lifecycle, _)) => {
                    features.lifecycle = lifecycle.has_active_rules("");
                }
                Err(EcstoreError::ConfigNotFound) => {}
                Err(err) => {
                    warn!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_RUNTIME_CONFIG,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        bucket = %bucket.name,
                        state = "lifecycle_inspect_failed",
                        error = %err,
                        "Scanner lifecycle inspection failed; preserving speed-based cycle"
                    );
                    features.inspection_failed = true;
                }
            }
        }

        if !features.replication {
            match get_replication_config(&bucket.name).await {
                Ok((replication, _)) => {
                    features.replication = replication.has_active_rules("", true);
                }
                Err(EcstoreError::ConfigNotFound) => {}
                Err(err) => {
                    warn!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_RUNTIME_CONFIG,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        bucket = %bucket.name,
                        state = "replication_inspect_failed",
                        error = %err,
                        "Scanner replication inspection failed; preserving speed-based cycle"
                    );
                    features.inspection_failed = true;
                }
            }
        }

        // Replication settles both regular-cycle and prompt-start decisions.
        if features.replication {
            break;
        }
    }

    features
}

async fn detect_stable_scanner_maintenance_features(
    ctx: &CancellationToken,
    storeapi: &Arc<impl ScannerStorage>,
) -> Option<(ScannerMaintenanceFeatures, u64)> {
    detect_stable_scanner_maintenance_features_with(
        ctx,
        || detect_scanner_maintenance_features(storeapi),
        MAINTENANCE_FEATURE_INSPECTION_TIMEOUT,
    )
    .await
}

async fn detect_stable_scanner_maintenance_features_with<F, Fut>(
    ctx: &CancellationToken,
    mut inspect: F,
    timeout: Duration,
) -> Option<(ScannerMaintenanceFeatures, u64)>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ScannerMaintenanceFeatures>,
{
    let mut attempts = 0usize;
    loop {
        attempts += 1;
        let generation = scanner_maintenance_generation();
        let mut features = match wait_for_maintenance_feature_inspection(ctx, inspect(), timeout).await {
            MaintenanceInspectionAttempt::Completed(features) => features,
            MaintenanceInspectionAttempt::Cancelled => return None,
            MaintenanceInspectionAttempt::TimedOut => {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_RUNTIME_CONFIG,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    timeout = ?timeout,
                    state = "maintenance_feature_inspection_timed_out",
                    "Scanner maintenance feature inspection timed out; preserving the base cycle"
                );
                ScannerMaintenanceFeatures {
                    inspection_failed: true,
                    ..Default::default()
                }
            }
        };
        let current_generation = scanner_maintenance_generation();
        match maintenance_inspection_decision(generation, current_generation, attempts) {
            MaintenanceInspectionDecision::Accept => return Some((features, current_generation)),
            MaintenanceInspectionDecision::Retry => {}
            MaintenanceInspectionDecision::PreserveBaseCycle => {
                features.inspection_failed = true;
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_RUNTIME_CONFIG,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    attempts = MAX_MAINTENANCE_FEATURE_INSPECTION_ATTEMPTS,
                    state = "maintenance_feature_inspection_unstable",
                    "Scanner maintenance configuration changed repeatedly during inspection; preserving the base cycle"
                );
                return Some((features, current_generation));
            }
        }
    }
}

async fn configure_scanner_defaults(
    ctx: &CancellationToken,
    storeapi: &Arc<impl ScannerStorage>,
) -> (ScannerMaintenanceFeatures, Option<u64>) {
    let (features, maintenance_generation) = detect_stable_scanner_maintenance_features(ctx, storeapi)
        .await
        .unwrap_or_else(|| {
            (
                ScannerMaintenanceFeatures {
                    inspection_failed: true,
                    ..Default::default()
                },
                scanner_maintenance_generation(),
            )
        });
    if storeapi.setup_is_erasure_sd().await {
        // Single-disk keeps the speed-preset-derived default cycle (60s at the
        // `default` preset) instead of a special shorter cycle: no measured
        // cold-start ILM latency basis for an override, and clean-idle backoff
        // already stretches idle cadence. Decision record: backlog#1878 (HS-16).
        set_scanner_default_speed(single_disk_default_speed());
        set_scanner_default_cycle_secs(None);
        info!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_RUNTIME_CONFIG,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            env_speed = ENV_SCANNER_SPEED,
            env_cycle = ENV_SCANNER_CYCLE,
            env_start_delay = ENV_SCANNER_START_DELAY_SECS,
            lifecycle_active = features.lifecycle,
            replication_active = features.replication,
            feature_inspection_failed = features.inspection_failed,
            state = "single_disk_defaults_applied",
            "Scanner defaults applied"
        );
        (features, Some(maintenance_generation))
    } else {
        set_scanner_default_speed(ScannerSpeed::Default);
        set_scanner_default_cycle_secs(None);
        (features, Some(maintenance_generation))
    }
}

#[cfg(test)]
fn bitrot_scan_cycle() -> Option<Duration> {
    resolve_scanner_runtime_config().bitrot_cycle
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScannerBitrotPolicy {
    cycle: Option<Duration>,
    deep_window_cycles: Option<u64>,
}

impl ScannerBitrotPolicy {
    fn new(cycle: Option<Duration>, deep_scan_supported: bool, deep_window_cycles: u64) -> Self {
        Self {
            cycle,
            deep_window_cycles: deep_scan_supported.then_some(deep_window_cycles),
        }
    }
}

fn get_cycle_scan_mode(current_cycle: u64, start: ScannerBitrotStart, policy: ScannerBitrotPolicy) -> HealScanMode {
    let Some(bitrot_cycle) = policy.cycle else {
        return HealScanMode::Normal;
    };

    let Some(deep_window_cycles) = policy.deep_window_cycles else {
        return HealScanMode::Normal;
    };

    if bitrot_cycle.is_zero() {
        return HealScanMode::Deep;
    }

    // Only a recorded Deep window keeps the selection window open. A missing
    // record used to be read as a window that started at cycle 0, which ran
    // Deep for `deep_window_cycles` cycles whenever the start could not be
    // read back (rustfs/rustfs#8015); an in-memory anchor only counts the
    // cadence.
    let started = match start {
        ScannerBitrotStart::None => return HealScanMode::Normal,
        ScannerBitrotStart::Window { cycle, started } => {
            if current_cycle.saturating_sub(cycle) < deep_window_cycles {
                return HealScanMode::Deep;
            }
            started
        }
        ScannerBitrotStart::Anchor(anchored) => anchored,
    };

    let elapsed = Utc::now().signed_duration_since(started).to_std().unwrap_or(Duration::ZERO);
    if elapsed >= bitrot_cycle {
        HealScanMode::Deep
    } else {
        HealScanMode::Normal
    }
}

fn background_heal_info_for_scan_start(
    mut info: BackgroundHealInfo,
    current_cycle: u64,
    scan_mode: HealScanMode,
    now: DateTime<Utc>,
    policy: ScannerBitrotPolicy,
) -> Option<BackgroundHealInfo> {
    let reset_bitrot_start = scan_mode == HealScanMode::Deep && should_reset_bitrot_start(&info, current_cycle, now, policy);
    if info.current_scan_mode == scan_mode && !reset_bitrot_start {
        return None;
    }

    info.current_scan_mode = scan_mode;
    if reset_bitrot_start {
        info.bitrot_start_cycle = current_cycle;
        info.bitrot_start_time = Some(now);
    }

    Some(info)
}

fn should_reset_bitrot_start(
    info: &BackgroundHealInfo,
    current_cycle: u64,
    now: DateTime<Utc>,
    policy: ScannerBitrotPolicy,
) -> bool {
    let Some(bitrot_start_time) = info.bitrot_start_time else {
        return true;
    };

    let Some(bitrot_cycle) = policy.cycle else {
        return false;
    };

    let Some(deep_window_cycles) = policy.deep_window_cycles else {
        return false;
    };

    if bitrot_cycle.is_zero() {
        return true;
    }

    if current_cycle.saturating_sub(info.bitrot_start_cycle) < deep_window_cycles {
        return false;
    }

    let elapsed = now
        .signed_duration_since(bitrot_start_time)
        .to_std()
        .unwrap_or(Duration::ZERO);
    elapsed >= bitrot_cycle
}

fn background_heal_info_for_scan_complete(mut info: BackgroundHealInfo, scan_mode: HealScanMode) -> Option<BackgroundHealInfo> {
    if scan_mode != HealScanMode::Deep || info.current_scan_mode != HealScanMode::Deep {
        return None;
    }

    info.current_scan_mode = HealScanMode::Normal;
    Some(info)
}

fn background_heal_info_for_scan_result(
    info: BackgroundHealInfo,
    scan_mode: HealScanMode,
    success: bool,
) -> Option<BackgroundHealInfo> {
    if !success {
        return None;
    }

    background_heal_info_for_scan_complete(info, scan_mode)
}

fn retain_recent_cycle_completions(cycle_completed: &mut Vec<DateTime<Utc>>) {
    let keep = data_usage_update_dir_cycles() as usize;
    if cycle_completed.len() > keep {
        let drop_count = cycle_completed.len() - keep;
        cycle_completed.drain(..drop_count);
    }
}

/// Get lock acquire timeout from environment variable RUSTFS_LOCK_ACQUIRE_TIMEOUT (in seconds)
/// Defaults to 5 seconds if not set or invalid
/// For distributed environments with multiple nodes, a longer timeout may be needed
fn get_lock_acquire_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64("RUSTFS_LOCK_ACQUIRE_TIMEOUT", 5))
}

fn data_usage_persist_timeout() -> Duration {
    DataUsageCache::publication_timeout()
}

#[cfg(not(test))]
const SCANNER_CYCLE_EPOCH_FENCE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const SCANNER_CYCLE_EPOCH_FENCE_TIMEOUT: Duration = Duration::from_millis(50);

async fn fence_scanner_epoch_after_cycle_timeout<Store, LockLost>(
    ctx: &CancellationToken,
    storeapi: Arc<Store>,
    cycle_info: &mut CurrentCycle,
    cycle_revision: &mut DataUsageCacheRevision,
    leader_epoch: &mut u64,
    allow_bootstrap_pending: bool,
    lock_lost: LockLost,
) -> bool
where
    Store: ScannerObjectIO + ScannerConfigObjectDelete,
    LockLost: Future<Output = ()>,
{
    let fence_ctx = ctx.child_token();
    let claim = claim_scanner_leadership(
        &fence_ctx,
        storeapi,
        cycle_info,
        cycle_revision,
        leader_epoch,
        allow_bootstrap_pending,
        ScannerCycleResetPolicy::None,
    );
    tokio::pin!(claim);
    tokio::pin!(lock_lost);
    tokio::select! {
        biased;
        _ = &mut lock_lost => {
            fence_ctx.cancel();
            false
        }
        result = tokio::time::timeout(SCANNER_CYCLE_EPOCH_FENCE_TIMEOUT, &mut claim) => {
            result.unwrap_or(false) && !fence_ctx.is_cancelled()
        }
    }
}

struct ScannerCycleDeadlineState<'a> {
    cycle_info: &'a mut CurrentCycle,
    cycle_revision: &'a mut DataUsageCacheRevision,
    leader_epoch: &'a mut u64,
    cycle_budget: &'a ScannerCycleBudget,
    allow_bootstrap_pending: bool,
}

fn cycle_timeout_requires_recovery(worker_stopped: bool, cycle_state_persisted: bool, generation_fenced: bool) -> bool {
    !worker_stopped || !cycle_state_persisted || !generation_fenced
}

async fn handle_scanner_cycle_deadline<Store>(
    ctx: &CancellationToken,
    storeapi: Arc<Store>,
    state: ScannerCycleDeadlineState<'_>,
    worker_stopped: bool,
    guard: &mut NamespaceLockGuard,
) where
    Store: ScannerObjectIO + ScannerConfigObjectDelete,
{
    let fenced = fence_scanner_epoch_after_cycle_timeout(
        ctx,
        storeapi,
        state.cycle_info,
        state.cycle_revision,
        state.leader_epoch,
        state.allow_bootstrap_pending,
        guard.lock_lost_notified(),
    )
    .await;
    let cycle_state_persisted = state.cycle_budget.cycle_state_persisted();
    let recovery_required = cycle_timeout_requires_recovery(worker_stopped, cycle_state_persisted, fenced);
    warn!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_CYCLE_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        state = "cycle_timeout",
        worker_stopped,
        cycle_state_persisted,
        generation_fenced = fenced,
        recovery_required,
        "Scanner cycle deadline expired; durable cursor/generation fencing completed when possible"
    );
    global_metrics().record_scanner_cycle_timeout(recovery_required, state.cycle_budget.progress_age());
    // Stop renewing before releasing the lease. A new leader can then claim the
    // higher persisted generation instead of inheriting the expired worker.
    guard.release();
    global_metrics().set_cycle(None).await;
}

async fn mark_scan_cycle_idle(cycle_info: &mut CurrentCycle, cycle_metrics_guard: &mut ScannerCycleMetricsGuard) {
    cycle_info.current = 0;
    global_metrics().clear_current_scan_mode();
    cycle_metrics_guard.finish(cycle_info.clone()).await;
}

struct ScannerCycleScheduling {
    requires_full_scan: bool,
    service_cohort: Option<Arc<StdMutex<crate::scanner_io::ScannerServiceCohort>>>,
    /// Receives the reason when a `Complete` cycle walked every bucket in
    /// place of an eligible dirty-bucket scope.
    scope_fallback: Option<Arc<ScannerScopeFallbackSlot>>,
}

/// Hand-off from a cycle to the leader loop: while dirty buckets remain after
/// a full-namespace fallback, the next cycle is paced by a bounded retry
/// schedule, not by the immediate dirty-usage wake that scoped cycles use.
#[derive(Debug, Default)]
struct ScannerScopeFallbackSlot(StdMutex<Option<crate::scanner_io::ScannerScopeFallbackReason>>);

impl ScannerScopeFallbackSlot {
    fn set(&self, reason: Option<crate::scanner_io::ScannerScopeFallbackReason>) {
        *self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = reason;
    }

    fn take(&self) -> Option<crate::scanner_io::ScannerScopeFallbackReason> {
        self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take()
    }
}

#[cfg(test)]
async fn run_data_scanner_cycle<S>(
    ctx: &CancellationToken,
    storeapi: &Arc<S>,
    cycle_info: &mut CurrentCycle,
    cycle_revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
) -> ScannerCycleOutcome
where
    S: ScannerStorage,
{
    let cycle_budget = ScannerCycleBudget::new(ctx, scanner_cycle_budget_config());
    run_data_scanner_cycle_with_budget(
        ctx,
        storeapi,
        cycle_info,
        cycle_revision,
        leader_epoch,
        cycle_budget,
        ScannerCycleScheduling {
            requires_full_scan: true,
            service_cohort: None,
            scope_fallback: None,
        },
    )
    .await
}

#[instrument(skip_all)]
#[hotpath::measure]
async fn run_data_scanner_cycle_with_budget<S>(
    ctx: &CancellationToken,
    storeapi: &Arc<S>,
    cycle_info: &mut CurrentCycle,
    cycle_revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    cycle_budget: Arc<ScannerCycleBudget>,
    scheduling: ScannerCycleScheduling,
) -> ScannerCycleOutcome
where
    S: ScannerStorage,
{
    let _activity_guard = ScannerActivityGuard::new();
    if let Err(err) = refresh_scanner_runtime_config_from_global() {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_RUNTIME_CONFIG,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "refresh_failed",
            error = %err,
            "Scanner runtime config refresh failed"
        );
    }
    let configured_bitrot_cycle = scanner_bitrot_cycle();
    let usage_persist_timeout = data_usage_persist_timeout();
    cycle_info.current = cycle_info.next;
    let now = Instant::now();
    cycle_info.started = Utc::now();

    let mut cycle_metrics_guard =
        ScannerCycleMetricsGuard::new(cycle_info.clone(), leader_epoch, cycle_budget.remote_scan_progress()).await;

    // Refresh the storage-owned movement snapshot before reading background
    // heal state. A missing heal object yields an in-memory default; do not
    // let that default influence a cycle while publication is blocked.
    if storeapi.scanner_data_movement_pause_status().await.paused {
        mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
        return ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
    }
    let background_heal_read = read_background_heal_info_with_epoch(storeapi.clone()).await;
    match background_heal_read.status {
        BackgroundHealInfoReadStatus::Blocked => {
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
        }
        BackgroundHealInfoReadStatus::Transient => {
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable);
        }
        BackgroundHealInfoReadStatus::Failed => {
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Failed;
        }
        BackgroundHealInfoReadStatus::Loaded | BackgroundHealInfoReadStatus::Missing => {}
    }
    let background_heal_epoch = background_heal_read.expected_epoch;
    let deep_scan_supported = !storeapi.setup_is_erasure_sd().await;
    let bitrot_policy = ScannerBitrotPolicy::new(configured_bitrot_cycle, deep_scan_supported, heal_object_select_prob() as u64);
    // The persisted record wins when it carries a start; the in-memory record
    // covers a save that was skipped or lost since this process recorded it.
    let (mut background_heal_info, mut bitrot_start_source) = resolve_background_heal_info_for_cycle(&background_heal_read);
    let bitrot_start = scanner_bitrot_start_for_cycle(&background_heal_info, Utc::now());
    if matches!(bitrot_start, ScannerBitrotStart::Anchor(_)) {
        bitrot_start_source = ScannerBitrotStartSource::Memory;
    }

    let scan_mode = get_cycle_scan_mode(cycle_info.current, bitrot_start, bitrot_policy);
    info!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_CYCLE_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        cycle = cycle_info.current,
        scan_mode = ?scan_mode,
        bitrot_start_source = bitrot_start_source.as_str(),
        state = "started",
        "Scanner cycle started"
    );
    let _scan_mode_guard = ScannerScanModeGuard::new(scan_mode);
    if let Some(new_heal_info) = background_heal_info_for_scan_start(
        background_heal_info.clone(),
        cycle_info.current,
        scan_mode,
        Utc::now(),
        bitrot_policy,
    ) {
        background_heal_info = new_heal_info.clone();
        if new_heal_info.bitrot_start_time != background_heal_read.info.bitrot_start_time {
            // A start recorded by this cycle is only in memory until a later
            // read returns it from storage.
            bitrot_start_source = ScannerBitrotStartSource::Memory;
        }
        save_background_heal_info_for_epoch(storeapi.clone(), new_heal_info, background_heal_epoch).await;
    }
    record_scanner_bitrot_status(
        scanner_bitrot_start_for_cycle(&background_heal_info, Utc::now()),
        bitrot_start_source,
        deep_scan_supported,
        scan_mode,
    );

    let cycle_start = std::time::Instant::now();
    // Baseline reads are part of the same publication proof as the eventual
    // scanner aggregate. Hold only the short storage-owned admission guard
    // across this metadata read; the full bucket scan runs after it is
    // released and carries the captured epoch forward.
    let Some(baseline_publication_guard) = storeapi.scanner_data_usage_publication_admission().await else {
        mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
        return ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
    };
    let baseline_publication_epoch = baseline_publication_guard.epoch();
    let usage_persist_baseline_result = read_data_usage_persist_baseline(storeapi.clone()).await;
    let observed_usage_candidate_result = read_config(storeapi.clone(), DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str()).await;
    drop(baseline_publication_guard);
    let usage_persist_baseline = match usage_persist_baseline_result {
        Ok(baseline) => baseline,
        Err(err) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                cycle = cycle_info.current,
                path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                state = "usage_baseline_load_failed",
                error = %err,
                "Scanner cycle could not capture the data usage persistence baseline"
            );
            emit_scan_cycle_complete(false, cycle_start.elapsed());
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Failed;
        }
    };
    let observed_usage_candidate = match observed_usage_candidate_result {
        Ok(candidate) => Some(Bytes::from(candidate)),
        Err(EcstoreError::ConfigNotFound) => None,
        Err(err) => {
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                cycle = cycle_info.current,
                path = %DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str(),
                state = "observed_candidate_load_failed",
                error = %err,
                "Scanner skipped an unavailable observed usage candidate for scoped refresh"
            );
            None
        }
    };
    let (sender, receiver) = mpsc::channel::<DataUsageInfo>(1);

    let done_cycle = Metrics::time(Metric::ScanCycle);
    let scan_result = crate::scanner_io::nsscanner_with_storage_status_scoped(
        storeapi.as_ref(),
        crate::scanner_io::ScannerCycleRequest {
            ctx: cycle_budget.token(),
            budget: cycle_budget.clone(),
            updates: sender,
            want_cycle: cycle_info.current,
            leader_epoch,
            scan_mode,
            scan_scope: crate::scanner_io::ScannerBucketScanScope::default(),
            persisted_usage_baseline: usage_persist_baseline.data.clone(),
            observed_usage_candidate,
            requires_full_scan: scheduling.requires_full_scan,
            service_cohort: scheduling.service_cohort,
            #[cfg(test)]
            resolved_scope_observer: None,
            #[cfg(test)]
            post_walk_gate: None,
        },
    )
    .await;
    if let Some(slot) = &scheduling.scope_fallback {
        slot.set(
            scan_result
                .as_ref()
                .ok()
                .and_then(ScannerCycleResult::completed_scope_fallback),
        );
    }
    let publication_plan = scan_result
        .as_ref()
        .map(ScannerCyclePublicationPlan::for_result)
        .unwrap_or(ScannerCyclePublicationPlan::ProgressOnly);
    let publication_defer_reason = match &scan_result {
        Ok(result)
            if result
                .publication_epoch()
                .is_some_and(|publication_epoch| publication_epoch != baseline_publication_epoch) =>
        {
            Some(ScannerCycleDeferReason::DataMovement)
        }
        // A complete walk can still be retained as an observational snapshot
        // when only the final activity proof was unavailable.  It must not
        // block the observation receiver: the authoritative publication
        // fence remains enforced by the usage store and the cycle is advanced
        // as partial without acknowledging dirty usage.
        Ok(result)
            if publication_plan.is_observational()
                && matches!(
                    result.status,
                    ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
                ) =>
        {
            None
        }
        Ok(result) => {
            final_data_usage_publication_defer_reason(storeapi.as_ref(), result.status, publication_plan.is_observational()).await
        }
        Err(_) => Some(ScannerCycleDeferReason::ActivityBaselineUnavailable),
    };
    let observational_only = publication_plan.is_observational();
    let publication_epoch = scan_result.as_ref().ok().and_then(ScannerCycleResult::publication_epoch);
    let remote_publication_lease_targets = if publication_defer_reason.is_none() {
        scan_result
            .as_ref()
            .ok()
            .map(|result| result.remote_publication_lease_targets().to_vec())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let mut remote_publication_leases = None;
    let remote_lease_defer_reason = if remote_publication_lease_targets.is_empty() {
        None
    } else if let Some(notification_system) = storeapi.scanner_notification_system() {
        let lease_result = publication_plan
            .await_leases(ctx, &cycle_budget, cycle_info.current, leader_epoch, |observational_lease| {
                let targets = remote_publication_lease_targets.clone();
                let notification_system = Arc::clone(&notification_system);
                async move {
                    if observational_lease {
                        notification_system
                            .acquire_observational_scanner_publication_leases(targets)
                            .await
                    } else {
                        notification_system.acquire_scanner_publication_leases(targets).await
                    }
                }
            })
            .await;
        match lease_result {
            ScannerPublicationProofWait::Ready(grants) => {
                remote_publication_leases = Some((notification_system, grants));
                None
            }
            ScannerPublicationProofWait::Rejected(_) | ScannerPublicationProofWait::Cancelled => {
                Some(ScannerCycleDeferReason::ActivityBaselineUnavailable)
            }
        }
    } else {
        Some(ScannerCycleDeferReason::ActivityBaselineUnavailable)
    };
    let remote_lease_deadline = remote_publication_leases
        .as_ref()
        .and_then(|(_, grants)| grants.iter().map(|grant| grant.lease.expires_at).min());
    // The transient fence is carried only to the SetDisks rename boundary;
    // it is never inserted into FileInfo metadata.  Keep the representation
    // bounded and require one authenticated token per remote target so a
    // partial grant can never silently fall back to an unfenced rename.
    let remote_lease_fence = remote_publication_leases
        .as_ref()
        .filter(|_| publication_plan.has_usage_update())
        .and_then(|(_, grants)| {
            if grants.len() != remote_publication_lease_targets.len()
                || grants.len() > SCANNER_PUBLICATION_LEASE_FENCE_MAX_ENTRIES
            {
                return None;
            }
            let mut fence = BTreeMap::new();
            for grant in grants {
                if grant.host.is_empty() || grant.host.len() > 1024 {
                    return None;
                }
                if fence.insert(grant.host.clone(), grant.lease.token.to_string()).is_some() {
                    return None;
                }
            }
            if remote_publication_lease_targets
                .iter()
                .any(|(host, _, _)| !fence.contains_key(host))
            {
                return None;
            }
            serde_json::to_string(&fence)
                .ok()
                .filter(|encoded| encoded.len() <= SCANNER_PUBLICATION_LEASE_FENCE_MAX_BYTES)
        });
    let remote_lease_fence_defer_reason = (publication_plan.has_usage_update()
        && remote_publication_lease_fence_targets_are_required(
            remote_publication_lease_targets.len(),
            remote_publication_leases.is_some(),
            remote_lease_fence.is_some(),
        ))
    .then_some(ScannerCycleDeferReason::ActivityBaselineUnavailable);
    let publication_defer_reason = publication_defer_reason
        .or(remote_lease_defer_reason)
        .or(remote_lease_fence_defer_reason);
    // A PUT tail can finish between the walk and lease acquisition without
    // changing the movement epoch accepted by those leases. Re-prove the
    // namespace baseline only after every peer has granted publication.
    let post_lease_activity_defer_reason = if publication_defer_reason.is_none()
        && remote_publication_leases.is_some()
        && let Ok(result) = &scan_result
        && result.status == ScannerCycleStatus::Complete
    {
        scanner_post_lease_activity_defer_reason(
            result.activity_baseline(),
            probe_scanner_activity(storeapi.as_ref(), true).await,
        )
    } else {
        None
    };
    let publication_defer_reason = publication_defer_reason.or(post_lease_activity_defer_reason);
    // Include reasons discovered while acquiring or validating remote leases.
    let publication_deferred = publication_defer_reason.is_some();
    let budget_elapsed = cycle_budget.budget_elapsed() && !ctx.is_cancelled();
    let remote_lease_probe = remote_publication_leases
        .as_ref()
        .filter(|_| publication_plan.has_usage_update())
        .map(|(notification_system, grants)| (Arc::clone(notification_system), grants.clone()));
    let remote_lease_release_safe = Arc::new(AtomicBool::new(true));
    let mut usage_publication_result = match publication_defer_reason {
        Some(reason) => {
            drop(receiver);
            DataUsagePublicationResult::from(DataUsagePersistOutcome::Deferred(reason))
        }
        None if !publication_plan.has_usage_update() => {
            drop(receiver);
            DataUsagePublicationResult::from(DataUsagePersistOutcome::NoUpdate)
        }
        None => {
            // ScannerIO emits its complete or observational update only after
            // all set workers finish. Persist after the final activity fence;
            // this also avoids blocking the scanner on a denied publication.
            let storeapi_clone = storeapi.clone();
            let ctx_clone = ctx.clone();
            let route_probe_store = storeapi.clone();
            let route_probe_observational_only = observational_only;
            let remote_lease_release_safe_for_task = Arc::clone(&remote_lease_release_safe);
            let remote_lease_tokens = remote_publication_leases
                .as_ref()
                .map(|(_, grants)| grants.iter().map(|grant| grant.lease.token).collect())
                .unwrap_or_default();
            let ack_expectation = scan_result
                .as_ref()
                .ok()
                .filter(|result| result.has_dirty_usage_to_acknowledge())
                .and_then(ScannerCycleResult::publication_expectation);
            let mut usage_persist_task = AbortOnDropHandle::new(tokio::spawn(async move {
                store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch_and_lease_fence(
                    ctx_clone,
                    storeapi_clone,
                    receiver,
                    Some(leader_epoch),
                    Some(usage_persist_baseline),
                    ScannerPublicationFence::new(
                        publication_epoch,
                        remote_lease_deadline,
                        remote_lease_fence,
                    )
                    .with_ack_expectation(ack_expectation)
                    .with_remote_lease_tokens(remote_lease_tokens)
                    .with_lease_release_flag(remote_lease_release_safe_for_task),
                    move || {
                        let storeapi = route_probe_store.clone();
                        let remote_lease_probe = remote_lease_probe.clone();
                        let observational_only = route_probe_observational_only;
                        async move {
                            if let Some((notification_system, grants)) = remote_lease_probe.as_ref()
                                && notification_system.validate_scanner_publication_leases(grants).await.is_err()
                            {
                                let remote_lease_targets = grants
                                    .iter()
                                    .map(|grant| {
                                        (
                                            grant.host.clone(),
                                            grant.lease.session_id.clone(),
                                            grant.lease.movement_generation,
                                        )
                                    })
                                    .collect::<Vec<_>>();
                                let remote_leases_valid = grants.iter().all(|grant| grant.lease.is_valid());
                                return Some(scanner_remote_publication_lease_failure_defer_reason(
                                    &remote_lease_targets,
                                    remote_leases_valid,
                                    probe_scanner_activity(storeapi.as_ref(), true).await,
                                ));
                            }
                            scanner_local_publication_defer_reason(storeapi.as_ref(), observational_only).await
                        }
                    },
                )
                .await
            }));
            match wait_for_data_usage_persist_task(ctx, &mut usage_persist_task, usage_persist_timeout).await {
                DataUsagePersistTaskResult::Completed(outcome) => outcome,
                DataUsagePersistTaskResult::JoinFailed(err) => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        cycle = cycle_info.current,
                        state = "usage_persist_task_failed",
                        error = %err,
                        "Scanner data usage persistence task failed"
                    );
                    DataUsagePublicationResult::from(DataUsagePersistOutcome::Failed)
                }
                DataUsagePersistTaskResult::Cancelled => {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        cycle = cycle_info.current,
                        state = "usage_persist_task_cancelled",
                        "Scanner data usage persistence task cancelled"
                    );
                    DataUsagePublicationResult::from(DataUsagePersistOutcome::Failed)
                }
                DataUsagePersistTaskResult::TimedOut => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        cycle = cycle_info.current,
                        timeout = ?usage_persist_timeout,
                        state = "usage_persist_task_timed_out",
                        "Scanner data usage persistence task timed out"
                    );
                    DataUsagePublicationResult::from(DataUsagePersistOutcome::Failed)
                }
            }
        }
    };
    let mut usage_persist_outcome = usage_publication_result.outcome();
    let lease_expired = remote_publication_leases
        .as_ref()
        .is_some_and(|(_, grants)| grants.iter().any(|grant| !grant.lease.is_valid()));
    if !remote_lease_release_safe.load(Ordering::Acquire) {
        // A cancelled or detached storage mutation did not report a safe
        // terminal state. Keep remote grants until their own expiry rather
        // than releasing movement admission while a commit may be unknown.
        usage_persist_outcome = if usage_persist_outcome == DataUsagePersistOutcome::Failed {
            DataUsagePersistOutcome::Failed
        } else {
            DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::PublicationLeaseDeadlineExceeded)
        };
    } else if let Some((notification_system, grants)) = remote_publication_leases.take() {
        let release_result = notification_system.release_scanner_publication_leases(grants).await;
        let lease_release_failed = release_result.is_err();
        if lease_expired || lease_release_failed {
            // A lease that expired or could not be released is never treated
            // as a successful authoritative publication. The peer may have
            // admitted movement immediately after the lease ended.
            usage_persist_outcome = if usage_persist_outcome == DataUsagePersistOutcome::Failed {
                DataUsagePersistOutcome::Failed
            } else if lease_release_failed {
                DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::PublicationLeaseReleaseFailed)
            } else {
                DataUsagePersistOutcome::Deferred(ScannerCycleDeferReason::PublicationLeaseDeadlineExceeded)
            };
        }
    }
    let unresolved_heal_work = global_metrics().current_scan_cycle_has_unresolved_heal_work();

    let scan_cycle_result = match scan_result {
        Ok(result) => result,
        Err(e) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_CYCLE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                cycle = cycle_info.current,
                scan_mode = ?scan_mode,
                state = "failed",
                duration = ?now.elapsed(),
                error = %e,
                "Scanner cycle failed"
            );
            emit_scan_cycle_complete(false, cycle_start.elapsed());
            if !ctx.is_cancelled()
                && let Some(new_heal_info) = background_heal_info_for_scan_result(background_heal_info.clone(), scan_mode, false)
            {
                save_background_heal_info_for_epoch(storeapi.clone(), new_heal_info, background_heal_epoch).await;
            }
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Failed;
        }
    };
    if ctx.is_cancelled() {
        debug!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            cycle = cycle_info.current,
            state = "cancelled_before_commit",
            "Scanner cycle stopped before committing cycle state"
        );
        emit_scan_cycle_complete(false, cycle_start.elapsed());
        mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
        return ScannerCycleOutcome::Failed;
    }
    cycle_metrics_guard
        .publication
        .record(usage_publication_result.observation_for_attempt(
            leader_epoch,
            cycle_info.current,
            !scan_cycle_result.has_usage_update(),
            usage_persist_outcome,
        ));
    let required_cycle_floor = scan_cycle_result.required_cycle_floor();
    let pre_commit_outcome = scanner_cycle_pre_commit_outcome(required_cycle_floor, &usage_persist_outcome);
    update_scanner_cache_cycle_recovery_status(
        cycle_info.current,
        leader_epoch,
        required_cycle_floor,
        pre_commit_outcome,
        scan_cycle_result.status == ScannerCycleStatus::Complete,
    );
    match pre_commit_outcome {
        Some(ScannerCyclePreCommitOutcome::RecoverCacheCycle(required_cycle)) => {
            record_scanner_cache_cycle_recovery_attempt();
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_CYCLE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                cycle = cycle_info.current,
                required_cycle,
                state = "cache_cycle_ahead",
                partial_cause = "cache_cycle_ahead",
                "Scanner cycle is recovering to a newer durable cache generation"
            );
            emit_scan_cycle_partial_with_source(cycle_start.elapsed(), ScanCyclePartialReason::Unknown, None);
            let persisted = persist_required_scanner_cycle_floor_for_epoch(
                ctx,
                storeapi.clone(),
                cycle_info,
                cycle_revision,
                leader_epoch,
                &mut cycle_metrics_guard,
                ScannerCycleFloorOptions {
                    required_cycle,
                    expected_publication_epoch: publication_epoch,
                },
            )
            .await;
            return if persisted {
                cycle_budget.mark_cycle_state_persisted();
                ScannerCycleOutcome::Partial
            } else if let Some(expected_epoch) = publication_epoch
                && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                    .await
                    .is_none()
            {
                emit_scan_cycle_deferred(cycle_start.elapsed());
                ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement)
            } else {
                ScannerCycleOutcome::StatePersistenceFailed
            };
        }
        Some(ScannerCyclePreCommitOutcome::Deferred(reason)) => {
            info!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_CYCLE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                cycle = cycle_info.current,
                reason = reason.as_str(),
                state = "deferred",
                "Scanner cycle deferred before data usage publication"
            );
            if publication_deferred {
                global_metrics().record_scanner_usage_deferred(reason.as_str());
            }
            emit_scan_cycle_deferred(cycle_start.elapsed());
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Deferred(reason);
        }
        None => {}
    }
    if usage_persist_outcome == DataUsagePersistOutcome::Failed {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            cycle = cycle_info.current,
            state = "usage_not_durable",
            "Scanner cycle completed without a durable data usage snapshot"
        );
        emit_scan_cycle_complete(false, cycle_start.elapsed());
        mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
        return ScannerCycleOutcome::Failed;
    }
    if budget_elapsed {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            cycle = cycle_info.current,
            duration = ?now.elapsed(),
            reason = ?cycle_budget.reason(),
            max_duration = ?cycle_budget.max_duration(),
            max_objects = ?cycle_budget.max_objects(),
            max_directories = ?cycle_budget.max_directories(),
            state = "budget_reached",
            "Scanner cycle budget reached"
        );
        let budget_reason = cycle_budget.reason();
        emit_scan_cycle_partial_with_source(
            cycle_start.elapsed(),
            scan_cycle_partial_reason(budget_reason),
            scan_cycle_partial_source(budget_reason),
        );
        let persisted = finalize_partial_scan_cycle_for_epoch(
            ctx,
            storeapi.clone(),
            cycle_info,
            cycle_revision,
            leader_epoch,
            &mut cycle_metrics_guard,
            publication_epoch,
        )
        .await;
        return if persisted {
            cycle_budget.mark_cycle_state_persisted();
            ScannerCycleOutcome::Partial
        } else if let Some(expected_epoch) = publication_epoch
            && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                .await
                .is_none()
        {
            emit_scan_cycle_deferred(cycle_start.elapsed());
            ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement)
        } else {
            ScannerCycleOutcome::StatePersistenceFailed
        };
    }

    usage_publication_result.restrict_outcome(usage_persist_outcome);
    let partial_cause = if scan_cycle_result.has_partial_bucket_progress() && !scan_cycle_result.has_usage_update() {
        "saved_bucket_progress"
    } else if scan_cycle_result.has_usage_update()
        && matches!(
            scan_cycle_result.status,
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
        )
    {
        "activity_unverified_observation"
    } else {
        "incomplete_coverage"
    };
    let pending_dirty_usage_buckets = scan_cycle_result.pending_dirty_usage_buckets();
    let (completion_outcome, scanner_pending_maintenance_work, remote_dirty_usage_acknowledgements) =
        finalize_scanner_cycle_result(scan_cycle_result, usage_publication_result);
    let remote_dirty_usage_pending = if remote_dirty_usage_acknowledgements.is_empty() {
        false
    } else if let Some(notification_system) = storeapi.scanner_notification_system() {
        let acknowledgement_count = remote_dirty_usage_acknowledgements.len();
        let acknowledgement_proof = remote_dirty_usage_acknowledgements.clone();
        let acknowledgements = remote_dirty_usage_acknowledgements.into_iter().map(Into::into).collect();
        remote_dirty_usage_acknowledgement_pending(
            cycle_info.current,
            acknowledgement_count,
            &acknowledgement_proof,
            notification_system.acknowledge_scanner_dirty_usage(acknowledgements),
            || probe_scanner_activity(storeapi.as_ref(), true),
        )
        .await
    } else {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            cycle = cycle_info.current,
            state = "remote_dirty_usage_acknowledgement_unavailable",
            "Scanner cycle cannot acknowledge remote dirty usage without a notification system"
        );
        true
    };
    let pending_maintenance_work = scanner_pending_maintenance_work || unresolved_heal_work || remote_dirty_usage_pending;
    match completion_outcome {
        ScannerCycleOutcome::Failed | ScannerCycleOutcome::StatePersistenceFailed => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                cycle = cycle_info.current,
                outcome = ?usage_persist_outcome,
                state = "usage_not_durable",
                "Scanner cycle completed without a durable data usage snapshot"
            );
            emit_scan_cycle_complete(false, cycle_start.elapsed());
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Failed;
        }
        ScannerCycleOutcome::Partial => {
            if ctx.is_cancelled() {
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_CYCLE_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    cycle = cycle_info.current,
                    state = "incomplete_cancelled",
                    "Scanner cycle stopped before a complete usage snapshot was produced"
                );
            } else {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_CYCLE_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    cycle = cycle_info.current,
                    state = "incomplete",
                    partial_cause,
                    "Scanner cycle ended without a complete usage snapshot"
                );
            }
            emit_scan_cycle_partial_with_source(cycle_start.elapsed(), ScanCyclePartialReason::Unknown, None);
            let persisted = finalize_partial_scan_cycle_for_epoch(
                ctx,
                storeapi.clone(),
                cycle_info,
                cycle_revision,
                leader_epoch,
                &mut cycle_metrics_guard,
                publication_epoch,
            )
            .await;
            return if persisted {
                cycle_budget.mark_cycle_state_persisted();
                ScannerCycleOutcome::Partial
            } else if let Some(expected_epoch) = publication_epoch
                && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                    .await
                    .is_none()
            {
                emit_scan_cycle_deferred(cycle_start.elapsed());
                ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement)
            } else {
                ScannerCycleOutcome::StatePersistenceFailed
            };
        }
        ScannerCycleOutcome::Deferred(reason) => {
            info!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_CYCLE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                cycle = cycle_info.current,
                reason = reason.as_str(),
                state = "deferred",
                "Scanner cycle deferred before usage scanning began"
            );
            global_metrics().record_scanner_usage_deferred(reason.as_str());
            emit_scan_cycle_deferred(cycle_start.elapsed());
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Deferred(reason);
        }
        ScannerCycleOutcome::Superseded => {
            info!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_CYCLE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                cycle = cycle_info.current,
                state = "superseded",
                "Scanner cycle usage snapshot was superseded by concurrent namespace activity"
            );
            if finalize_partial_scan_cycle_for_epoch(
                ctx,
                storeapi.clone(),
                cycle_info,
                cycle_revision,
                leader_epoch,
                &mut cycle_metrics_guard,
                publication_epoch,
            )
            .await
            {
                cycle_budget.mark_cycle_state_persisted();
                emit_scan_cycle_superseded(cycle_start.elapsed());
                return ScannerCycleOutcome::Superseded;
            }
            if let Some(expected_epoch) = publication_epoch
                && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                    .await
                    .is_none()
            {
                emit_scan_cycle_deferred(cycle_start.elapsed());
                return ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
            }
            emit_scan_cycle_complete(false, cycle_start.elapsed());
            return ScannerCycleOutcome::StatePersistenceFailed;
        }
        ScannerCycleOutcome::Completed | ScannerCycleOutcome::CompletedWithPendingMaintenance => {}
    }
    if let Some(expected_epoch) = publication_epoch
        && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
            .await
            .is_none()
    {
        emit_scan_cycle_deferred(cycle_start.elapsed());
        mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
        return ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
    }
    let previous_cycle_info = cycle_info.clone();
    if let Err(err) = advance_scanner_cycle(cycle_info) {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "cycle_counter_exhausted",
            error = %err,
            "Scanner completed cycle could not advance"
        );
        mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
        emit_scan_cycle_complete(false, cycle_start.elapsed());
        return ScannerCycleOutcome::Failed;
    }
    cycle_info.current = 0;
    cycle_info.cycle_completed.push(Utc::now());
    global_metrics().clear_current_scan_mode();

    retain_recent_cycle_completions(&mut cycle_info.cycle_completed);
    if persist_scanner_cycle_state_for_epoch(ctx, storeapi.clone(), cycle_info, cycle_revision, leader_epoch, publication_epoch)
        .await
        != ScannerCycleStatePersistOutcome::Saved
    {
        if let Some(expected_epoch) = publication_epoch
            && scanner_publication_admission_for_epoch(storeapi.clone(), expected_epoch)
                .await
                .is_none()
        {
            *cycle_info = previous_cycle_info;
            emit_scan_cycle_deferred(cycle_start.elapsed());
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
        }
        cycle_metrics_guard.finish(cycle_info.clone()).await;
        emit_scan_cycle_complete(false, cycle_start.elapsed());
        return ScannerCycleOutcome::StatePersistenceFailed;
    }
    cycle_budget.mark_cycle_state_persisted();

    done_cycle();
    emit_scan_cycle_complete(true, cycle_start.elapsed());
    if let Some(new_heal_info) = background_heal_info_for_scan_result(background_heal_info.clone(), scan_mode, true) {
        save_background_heal_info_for_epoch(storeapi.clone(), new_heal_info, background_heal_epoch).await;
    }

    info!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_CYCLE_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        cycle = cycle_info.current,
        scan_mode = ?scan_mode,
        state = "completed",
        duration = ?now.elapsed(),
        cycles_total = cycle_info.cycle_completed.len(),
        pending_dirty_buckets = pending_dirty_usage_buckets,
        "Scanner cycle completed"
    );

    cycle_metrics_guard.finish(cycle_info.clone()).await;
    scanner_cycle_outcome_with_pending_maintenance(ScannerCycleOutcome::Completed, pending_maintenance_work)
}

struct ScannerCycleMetricsGuard {
    start: Option<ScanCycleWorkSnapshot>,
    publication: Arc<ScannerUsagePublicationObservation>,
}

impl ScannerCycleMetricsGuard {
    async fn new(cycle: CurrentCycle, leader_epoch: u64, remote_progress: Arc<RemoteScanProgress>) -> Self {
        let publication = Arc::new(ScannerUsagePublicationObservation::new(leader_epoch, cycle.current));
        Self {
            start: Some(
                global_metrics()
                    .start_scan_cycle_work_with_publication(cycle, remote_progress, publication.clone())
                    .await,
            ),
            publication,
        }
    }

    async fn finish(&mut self, cycle: CurrentCycle) {
        if let Some(start) = self.start {
            self.publication.finish();
            global_metrics().finish_scan_cycle_work_with_cycle(start, cycle).await;
            self.start = None;
        }
    }
}

impl Drop for ScannerCycleMetricsGuard {
    fn drop(&mut self) {
        if let Some(start) = self.start.take() {
            self.publication.interrupt();
            global_metrics().finish_scan_cycle_work(start);
        }
    }
}

pub async fn run_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) -> Result<(), ScannerError> {
    run_data_scanner_with_storage(ctx, storeapi).await
}

async fn run_data_scanner_with_storage<S>(ctx: CancellationToken, storeapi: Arc<S>) -> Result<(), ScannerError>
where
    S: ScannerStorage,
{
    let runtime_guard = ScannerRuntimeGuard::new();
    let (maintenance_features, maintenance_generation) = configure_scanner_defaults(&ctx, &storeapi).await;
    run_data_scanner_with_maintenance_state(
        ctx,
        storeapi,
        maintenance_features,
        maintenance_generation,
        runtime_guard.startup_token(),
    )
    .await
}

async fn current_scanner_pause_backlog_observation<S>(storeapi: &Arc<S>) -> ScannerPauseBacklogObservation
where
    S: ScannerStorage,
{
    let now_unix_secs = scanner_pause_backlog_now();
    let pause = storeapi.scanner_data_movement_pause_status().await;
    let metrics = global_metrics().report().await;
    ScannerPauseBacklogObservation {
        now_unix_secs,
        paused: pause.paused,
        movement_generation: pause.movement_generation,
        movement_work_items: pause.movement_backlog_work_items,
        pause_started_at_unix_secs: pause.started_at_unix_secs,
        dirty_usage_buckets: metrics.usage_freshness.dirty_pending_buckets,
        discovered_expiry_items: metrics
            .lifecycle_expiry
            .current_queued
            .saturating_add(metrics.lifecycle_expiry.current_active),
        discovered_transition_items: metrics
            .lifecycle_transition
            .current_queued
            .saturating_add(metrics.lifecycle_transition.current_active)
            .saturating_add(metrics.lifecycle_transition.compensation_pending)
            .saturating_add(metrics.lifecycle_transition.compensation_running),
    }
}

async fn wait_for_scanner_data_movement_resume<S>(
    ctx: &CancellationToken,
    storeapi: &Arc<S>,
    guard: &NamespaceLockGuard,
    pause_backlog: &mut ScannerPauseBacklogController<S>,
) -> bool
where
    S: ScannerStorage,
{
    loop {
        let observation = current_scanner_pause_backlog_observation(storeapi).await;
        pause_backlog.observe(observation).await;
        #[cfg(test)]
        notify_scanner_runtime_observed_for_test(storeapi, observation);
        if !observation.paused {
            return !ctx.is_cancelled() && !guard.is_lock_lost();
        }

        let movement_changed = storeapi.scanner_data_movement_changed();
        if storeapi.scanner_data_movement_generation() != observation.movement_generation {
            continue;
        }
        tokio::select! {
            _ = ctx.cancelled() => return false,
            _ = guard.lock_lost_notified() => return false,
            _ = movement_changed.notified() => {},
            _ = tokio::time::sleep(SCANNER_CYCLE_RECOVERY_PAUSED_INTERVAL) => {},
        }
    }
}

async fn finish_scanner_pause_backlog_cycle<S>(
    pause_backlog: &mut ScannerPauseBacklogController<S>,
    storeapi: &Arc<S>,
    attempt: ScannerPauseBacklogAttemptDecision,
    outcome: ScannerCycleOutcome,
) where
    S: ScannerStorage,
{
    let observation = current_scanner_pause_backlog_observation(storeapi).await;
    if let ScannerPauseBacklogAttemptDecision::Tracked(serial) = attempt {
        pause_backlog.finish_attempt(serial, outcome, observation).await;
    } else {
        pause_backlog.observe_cycle_outcome(outcome, observation).await;
    }
    #[cfg(test)]
    notify_scanner_runtime_observed_for_test(storeapi, observation);
}

async fn run_data_scanner_with_maintenance_state<S>(
    ctx: CancellationToken,
    storeapi: Arc<S>,
    mut maintenance_features: ScannerMaintenanceFeatures,
    mut maintenance_generation_seen: Option<u64>,
    startup: ScannerStartupToken,
) -> Result<(), ScannerError>
where
    S: ScannerStorage,
{
    reset_scanner_cycle_schedule();
    // Normal scanner entry is an attempt, not evidence of leadership, coverage, or publication.
    global_metrics().startup().record_attempt(startup);
    // Acquire leader lock (write lock) to ensure only one scanner runs
    let mut guard = match storeapi.new_ns_lock(RUSTFS_META_BUCKET, "leader.lock").await {
        Ok(ns_lock) => match ns_lock.get_write_lock_quiet(get_lock_acquire_timeout()).await {
            Ok(guard) => {
                record_scanner_cycle_schedule_role("leader");
                record_scanner_leader_lock_state("acquired");
                global_metrics().record_scanner_leader_liveness("acquired", true, "").await;
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_LOCK_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    lock_name = "leader.lock",
                    state = "acquired",
                    "Scanner leader lock acquired"
                );
                guard
            }
            Err(e) => {
                match classify_scanner_leader_lock_failure(&e) {
                    ScannerLeaderLockFailure::Contended => {
                        clear_scanner_transient_cycle_recovery_status();
                        record_scanner_cycle_schedule_role("follower");
                        record_scanner_leader_lock_state("contended");
                        global_metrics().record_scanner_leader_liveness("contended", false, "").await;
                        debug!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_LOCK_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            lock_name = "leader.lock",
                            state = "contended",
                            "Scanner leader lock contended"
                        );
                    }
                    ScannerLeaderLockFailure::Failed(error) => {
                        record_scanner_leader_lock_state("acquire_failed");
                        global_metrics()
                            .record_scanner_leader_liveness("acquire_failed", false, error.to_string())
                            .await;
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_LOCK_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            lock_name = "leader.lock",
                            state = "acquire_failed",
                            error = %error,
                            "Scanner leader lock acquisition failed"
                        );
                    }
                }
                return Ok(());
            }
        },
        Err(e) => {
            record_scanner_leader_lock_state("create_failed");
            global_metrics()
                .record_scanner_leader_liveness("create_failed", false, e.to_string())
                .await;
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_LOCK_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                lock_name = "leader.lock",
                state = "create_failed",
                error = %e,
                "Scanner leader lock creation failed"
            );
            return Ok(());
        }
    };
    let pause_backlog_now = scanner_pause_backlog_now();
    let mut pause_backlog = match ScannerPauseBacklogController::claim(storeapi.clone(), pause_backlog_now).await {
        Ok(controller) => controller,
        Err(err) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "pause_backlog_claim_failed",
                error = %err,
                "Scanner pause backlog persistence is unavailable"
            );
            ScannerPauseBacklogController::unavailable(storeapi.clone(), err, pause_backlog_now)
        }
    };
    if !wait_for_scanner_data_movement_resume(&ctx, &storeapi, &guard, &mut pause_backlog).await {
        global_metrics().set_cycle(None).await;
        return Ok(());
    }
    #[cfg(test)]
    notify_scanner_startup_observed_for_test().await;
    let single_disk = storeapi.setup_is_erasure_sd().await;
    let erasure = storeapi.setup_is_erasure().await;
    let distributed = storeapi.setup_is_dist_erasure().await;
    let clean_idle_topology_supported = single_disk || erasure;
    let mut dirty_usage_generation_seen = dirty_usage_generation();
    let mut runtime_config_generation_seen = scanner_runtime_config_generation();
    let mut clean_idle_backoff = ScannerCleanIdleBackoff::default();
    let mut superseded_backoff = ScannerRetryBackoff::default();
    let mut deferred_backoff = ScannerRetryBackoff::default();
    let mut scope_fallback_backoff = ScannerRetryBackoff::default();
    let scope_fallback_slot = Arc::new(ScannerScopeFallbackSlot::default());
    let service_cohort = Arc::new(StdMutex::new(crate::scanner_io::ScannerServiceCohort::default()));
    let initial_runtime_config = resolve_scanner_runtime_config();
    if clean_idle_topology_supported && maintenance_generation_seen.is_none() {
        let Some((features, generation)) = detect_stable_scanner_maintenance_features(&ctx, &storeapi).await else {
            global_metrics().set_cycle(None).await;
            finish_scanner_leader_iteration(false, "stopped", String::new()).await;
            return Ok(());
        };
        maintenance_features = features;
        maintenance_generation_seen = Some(generation);
    }
    let mut maintenance_inspection_retry = ScannerMaintenanceInspectionRetry::from_features(maintenance_features, Instant::now());
    let mut scanner_activity_seen = None;
    let mut scanner_activity_backoff_blocked = false;
    if scanner_activity_probe_required(
        clean_idle_topology_supported,
        scanner_activity_backoff_blocked,
        maintenance_features,
        &initial_runtime_config,
    ) {
        observe_scanner_activity(&storeapi, distributed, &mut scanner_activity_seen).await;
    }

    let mut rebuild_from_quarantine = false;
    let (mut cycle_info, mut leader_epoch, mut cycle_revision) =
        match load_scanner_cycle_state_for_startup(storeapi.clone()).await {
            ScannerCycleStateStartup::Ready {
                cycle,
                leader_epoch,
                revision,
            } => (cycle, leader_epoch, revision),
            ScannerCycleStateStartup::RebuildFromQuarantine { revision } => {
                // The usage floor raises the cycle below and the leadership
                // claim raises the epoch; the quarantined revision is the CAS
                // precondition so a concurrent writer cannot be overwritten.
                rebuild_from_quarantine = true;
                (CurrentCycle::default(), 0, revision)
            }
            ScannerCycleStateStartup::Blocked => {
                global_metrics().set_cycle(None).await;
                finish_scanner_leader_iteration(false, "stopped", String::new()).await;
                return Ok(());
            }
            ScannerCycleStateStartup::Transient(err) => {
                global_metrics().set_cycle(None).await;
                finish_scanner_leader_iteration(false, "stopped", String::new()).await;
                return Err(err);
            }
        };
    let mut usage_floor_result = persisted_usage_floor_for_startup(storeapi.clone(), true).await;
    if usage_floor_result.is_err() {
        match recover_dual_corrupt_scanner_state(storeapi.clone(), &cycle_revision, || {
            !ctx.is_cancelled() && !guard.is_lock_lost()
        })
        .await
        {
            Ok(Some((cycle, epoch, revision))) => {
                cycle_info = cycle;
                leader_epoch = epoch;
                cycle_revision = revision;
                rebuild_from_quarantine = true;
                usage_floor_result = persisted_usage_floor_for_startup(storeapi.clone(), true).await;
            }
            Ok(None) => {}
            Err(err) => usage_floor_result = Err(err),
        }
    }
    let (usage_floor, usage_floor_startup) = match usage_floor_result {
        Ok(result) => result,
        Err(err) => {
            let error = err.to_string();
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                state = "usage_floor_load_failed",
                error = %err,
                "Scanner stopped because the persisted usage floor could not be loaded"
            );
            record_scanner_usage_floor_failure(error.clone());
            global_metrics().set_cycle(None).await;
            finish_scanner_leader_iteration(false, "usage_floor_load_failed", error).await;
            return Ok(());
        }
    };
    let (allow_usage_floor_bootstrap_pending, usage_floor_cycle_reset_policy) =
        prepare_cycle_for_usage_floor_bootstrap(&mut cycle_info, usage_floor, usage_floor_startup);
    let mut usage_bootstrap_rebuild = ScannerUsageBootstrapRebuild::from_startup(usage_floor_startup);
    apply_persisted_usage_floor(&mut cycle_info, &mut leader_epoch, usage_floor);
    match usage_floor_startup {
        PersistedUsageFloorStartup::Authoritative
        | PersistedUsageFloorStartup::BootstrapPending
        | PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence => {}
        PersistedUsageFloorStartup::Missing => {
            if ctx.is_cancelled() || guard.is_lock_lost() {
                global_metrics().set_cycle(None).await;
                finish_scanner_leader_iteration(guard.is_lock_lost(), "stopped", String::new()).await;
                return Ok(());
            }

            let bootstrap_ctx = ctx.child_token();
            match await_scanner_cycle_with_lock_fence(
                &bootstrap_ctx,
                initialize_usage_baseline_bootstrap(storeapi.clone()),
                guard.lock_lost_notified(),
            )
            .await
            {
                Some(Ok(())) => {}
                Some(Err(err)) => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                        state = "usage_floor_bootstrap_failed",
                        error = %err,
                        "Scanner stopped because the usage baseline bootstrap could not be initialized"
                    );
                    global_metrics().set_cycle(None).await;
                    finish_scanner_leader_iteration(false, "usage_floor_bootstrap_failed", err.to_string()).await;
                    return Ok(());
                }
                None => {
                    global_metrics().set_cycle(None).await;
                    finish_scanner_leader_iteration(guard.is_lock_lost(), "stopped", String::new()).await;
                    return Ok(());
                }
            }
        }
    }

    if ctx.is_cancelled() || guard.is_lock_lost() {
        global_metrics().set_cycle(None).await;
        finish_scanner_leader_iteration(guard.is_lock_lost(), "stopped", String::new()).await;
        return Ok(());
    }
    let claim_ctx = ctx.child_token();
    let leadership_claimed = await_scanner_cycle_with_lock_fence(
        &claim_ctx,
        claim_scanner_leadership(
            &claim_ctx,
            storeapi.clone(),
            &mut cycle_info,
            &mut cycle_revision,
            &mut leader_epoch,
            allow_usage_floor_bootstrap_pending,
            usage_floor_cycle_reset_policy,
        ),
        guard.lock_lost_notified(),
    )
    .await
    .unwrap_or(false);
    if guard.is_lock_lost() {
        record_scanner_leader_lock_lost("Scanner leader lock lost while claiming the leadership epoch").await;
        global_metrics().set_cycle(None).await;
        finish_scanner_leader_iteration(true, "lost", String::new()).await;
        return Ok(());
    }
    if !leadership_claimed {
        let observation = current_scanner_pause_backlog_observation(&storeapi).await;
        pause_backlog.observe(observation).await;
        #[cfg(test)]
        notify_scanner_runtime_observed_for_test(&storeapi, observation);
        if observation.paused {
            if wait_for_scanner_data_movement_resume(&ctx, &storeapi, &guard, &mut pause_backlog).await {
                return Err(ScannerError::Other(
                    "scanner startup was fenced by data movement; retrying from durable state".to_string(),
                ));
            }
            global_metrics().set_cycle(None).await;
            return Ok(());
        }
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_LOCK_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            lock_name = "leader.lock",
            state = "epoch_claim_failed",
            "Scanner stopped because the leadership epoch could not be claimed"
        );
        global_metrics().set_cycle(None).await;
        finish_scanner_leader_iteration(false, "epoch_claim_failed", "leadership epoch claim failed".to_string()).await;
        return Ok(());
    }
    if rebuild_from_quarantine {
        record_scanner_cycle_state_rebuilt_from_quarantine(cycle_info.next, leader_epoch);
    }
    if usage_floor_startup == PersistedUsageFloorStartup::RecoveredLegacyIncompleteFence
        && let Err(err) = complete_legacy_incomplete_usage_floor_recovery(storeapi.clone(), leader_epoch).await
    {
        let error = err.to_string();
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "usage_floor_recovery_cleanup_deferred",
            path = %DATA_USAGE_RECOVERY_PATH.as_str(),
            error = %err,
            "Scanner usage floor recovery marker cleanup was deferred"
        );
        global_metrics().set_cycle(None).await;
        finish_scanner_leader_iteration(false, "usage_floor_recovery_pending", error).await;
        return Ok(());
    }

    let initial_pause_backlog_attempt = if usage_bootstrap_rebuild.pending() {
        pause_backlog
            .begin_usage_bootstrap_rebuild_attempt(scanner_pause_backlog_now())
            .await
    } else {
        pause_backlog.begin_attempt(scanner_pause_backlog_now()).await
    };
    if !ctx.is_cancelled()
        && matches!(
            initial_pause_backlog_attempt,
            ScannerPauseBacklogAttemptDecision::Untracked
                | ScannerPauseBacklogAttemptDecision::Tracked(_)
                | ScannerPauseBacklogAttemptDecision::Degraded
        )
    {
        // Preserve previous behavior: run one cycle immediately after lock acquisition.
        let usage_bootstrap_pending_before_cycle = usage_bootstrap_rebuild.pending();
        let dirty_generation_before_cycle = dirty_usage_generation();
        let dirty_usage_pending_before_cycle = dirty_usage_buckets_pending();
        let maintenance_generation_before_cycle = scanner_maintenance_generation();
        if guard.is_lock_lost() {
            record_scanner_leader_lock_lost("Scanner leader lock lost before the initial cycle").await;
            global_metrics().set_cycle(None).await;
            finish_scanner_leader_iteration(true, "lost", String::new()).await;
            return Ok(());
        }
        let cycle_ctx = ctx.child_token();
        let cycle_budget = ScannerCycleBudget::new_with_runtime_progress_tracking(&cycle_ctx, scanner_cycle_budget_config());
        record_scanner_cycle_attempt(None);
        let initial_outcome = match await_scanner_cycle_with_budget_fence(
            &cycle_ctx,
            &cycle_budget,
            run_data_scanner_cycle_with_budget(
                &cycle_ctx,
                &storeapi,
                &mut cycle_info,
                &mut cycle_revision,
                leader_epoch,
                cycle_budget.clone(),
                ScannerCycleScheduling {
                    requires_full_scan: true,
                    service_cohort: Some(service_cohort.clone()),
                    scope_fallback: None,
                },
            ),
            guard.lock_lost_notified(),
        )
        .await
        {
            ScannerCycleWaitOutcome::Completed(outcome) => outcome,
            ScannerCycleWaitOutcome::LockLost => {
                record_scanner_leader_lock_lost("Scanner leader lock lost during the initial cycle").await;
                global_metrics().set_cycle(None).await;
                finish_scanner_leader_iteration(true, "lost", String::new()).await;
                return Ok(());
            }
            ScannerCycleWaitOutcome::Cancelled => {
                global_metrics().set_cycle(None).await;
                finish_scanner_leader_iteration(guard.is_lock_lost(), "stopped", String::new()).await;
                return Ok(());
            }
            ScannerCycleWaitOutcome::Deadline { worker_stopped } => {
                handle_scanner_cycle_deadline(
                    &ctx,
                    storeapi.clone(),
                    ScannerCycleDeadlineState {
                        cycle_info: &mut cycle_info,
                        cycle_revision: &mut cycle_revision,
                        leader_epoch: &mut leader_epoch,
                        cycle_budget: &cycle_budget,
                        allow_bootstrap_pending: allow_usage_floor_bootstrap_pending,
                    },
                    worker_stopped,
                    &mut guard,
                )
                .await;
                finish_scanner_leader_iteration(guard.is_lock_lost(), "stopped", String::new()).await;
                return Ok(());
            }
        };
        finish_scanner_pause_backlog_cycle(&mut pause_backlog, &storeapi, initial_pause_backlog_attempt, initial_outcome).await;
        if initial_outcome == ScannerCycleOutcome::StatePersistenceFailed
            && let Err(reason) =
                reload_cycle_state_under_same_epoch(&storeapi, &mut cycle_info, &mut cycle_revision, leader_epoch).await
        {
            global_metrics().set_cycle(None).await;
            let error = format!("scanner cycle state persistence failed; {reason}; retrying from durable state");
            finish_scanner_leader_iteration(false, "state_persist_failed", error.clone()).await;
            return Err(ScannerError::Other(error));
        }
        if usage_bootstrap_rebuild.record_cycle(initial_outcome) {
            clean_idle_backoff.reset();
        }
        superseded_backoff.record_retryable_cycle(initial_outcome == ScannerCycleOutcome::Superseded);
        deferred_backoff.record_retryable_cycle(matches!(
            initial_outcome,
            ScannerCycleOutcome::Deferred(_) | ScannerCycleOutcome::StatePersistenceFailed
        ));
        dirty_usage_generation_seen = dirty_generation_before_cycle;
        if guard.is_lock_lost() {
            record_scanner_leader_lock_lost("Scanner leader lock lost during the initial cycle").await;
            global_metrics().set_cycle(None).await;
            finish_scanner_leader_iteration(true, "lost", String::new()).await;
            return Ok(());
        }
        let runtime_config = resolve_scanner_runtime_config();
        let scanner_activity_observation = if scanner_activity_probe_required(
            clean_idle_topology_supported,
            scanner_activity_backoff_blocked,
            maintenance_features,
            &runtime_config,
        ) {
            observe_scanner_activity(&storeapi, distributed, &mut scanner_activity_seen).await
        } else {
            scanner_activity_seen = None;
            ScannerActivityObservation::NotRequired
        };
        if scanner_activity_observation == ScannerActivityObservation::MaintenanceChanged {
            scanner_activity_backoff_blocked = true;
        }
        let scanner_activity_ready = !scanner_activity_backoff_blocked && scanner_activity_seen.is_some();
        let backoff_enabled = usage_bootstrap_rebuild.clean_idle_backoff_enabled(scanner_clean_idle_backoff_enabled(
            clean_idle_topology_supported,
            scanner_activity_ready,
            maintenance_features,
            &runtime_config,
        ));
        record_scanner_cycle_result(
            &mut clean_idle_backoff,
            &runtime_config,
            backoff_enabled,
            ScannerCycleWakeReason::Timer,
            initial_outcome,
            scanner_cycle_observed_dirty_work(
                dirty_usage_pending_before_cycle,
                dirty_generation_before_cycle,
                dirty_usage_generation(),
            ) || usage_bootstrap_pending_before_cycle
                || maintenance_generation_before_cycle != scanner_maintenance_generation()
                || scanner_activity_observed_work(scanner_activity_observation),
        );
        runtime_config_generation_seen = scanner_runtime_config_generation();
    }

    loop {
        if ctx.is_cancelled() {
            break;
        }

        let pause_backlog_observation = current_scanner_pause_backlog_observation(&storeapi).await;
        pause_backlog.observe(pause_backlog_observation).await;
        #[cfg(test)]
        notify_scanner_runtime_observed_for_test(&storeapi, pause_backlog_observation);
        let runtime_config = resolve_scanner_runtime_config();
        if clean_idle_topology_supported {
            let current_generation = scanner_maintenance_generation();
            if maintenance_generation_seen != Some(current_generation) {
                scanner_activity_seen = None;
                scanner_activity_backoff_blocked = scanner_activity_backoff_blocked_after_wake(
                    scanner_activity_backoff_blocked,
                    ScannerCycleWakeReason::MaintenanceConfig,
                );
                let Some((features, generation)) = detect_stable_scanner_maintenance_features(&ctx, &storeapi).await else {
                    break;
                };
                maintenance_features = features;
                maintenance_generation_seen = Some(generation);
                maintenance_inspection_retry.record_inspection(features, Instant::now());
            }
        }
        if !scanner_activity_probe_required(
            clean_idle_topology_supported,
            scanner_activity_backoff_blocked,
            maintenance_features,
            &runtime_config,
        ) {
            scanner_activity_seen = None;
        }
        let scanner_activity_ready = !scanner_activity_backoff_blocked && scanner_activity_seen.is_some();
        let backoff_enabled = usage_bootstrap_rebuild.clean_idle_backoff_enabled(scanner_clean_idle_backoff_enabled(
            clean_idle_topology_supported,
            scanner_activity_ready,
            maintenance_features,
            &runtime_config,
        ));
        let mut wait_plan =
            scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, backoff_enabled, randomized_cycle_delay_for);
        let superseded_retry_interval =
            scanner_superseded_retry_interval(superseded_backoff, &runtime_config, maintenance_features);
        let deferred_retry_interval = deferred_backoff.retry_interval(runtime_config.cycle_interval);
        let scope_fallback_retry_interval = scanner_scope_fallback_retry_interval(scope_fallback_backoff, &runtime_config);
        let mut convergence_retry_interval = superseded_retry_interval
            .or(scope_fallback_retry_interval)
            .or(deferred_retry_interval);
        if let Some(retry_interval) = convergence_retry_interval {
            wait_plan.effective_interval = retry_interval;
            wait_plan.delay = randomized_cycle_delay_for(retry_interval).min(retry_interval);
        }
        if let Some(pause_backlog_delay) =
            pause_backlog.scheduling_delay(scanner_pause_backlog_now(), usage_bootstrap_rebuild.pending())
        {
            wait_plan.effective_interval = pause_backlog_delay.max(Duration::from_secs(1));
            wait_plan.delay = pause_backlog_delay;
            convergence_retry_interval = Some(pause_backlog_delay.max(Duration::from_secs(1)));
        }
        wait_plan = usage_bootstrap_rebuild.wait_plan(wait_plan, convergence_retry_interval);
        let dirty_generation_before_wait = dirty_usage_generation();
        let dirty_usage_pending_before_wait = dirty_usage_buckets_pending();
        let maintenance_generation_before_wait = scanner_maintenance_generation();
        record_scanner_cycle_schedule(
            wait_plan.effective_interval,
            usage_bootstrap_rebuild.pending(),
            backoff_enabled,
            u64::from(clean_idle_backoff.interval_multiplier),
            superseded_retry_interval.is_some(),
            superseded_backoff.consecutive_cycles,
        );
        debug!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            configured_interval = ?runtime_config.cycle_interval,
            effective_interval = ?wait_plan.effective_interval,
            clean_idle_max_interval = ?wait_plan.clean_idle_max_interval,
            scheduled_delay = ?wait_plan.delay,
            usage_bootstrap_rebuild_pending = usage_bootstrap_rebuild.pending(),
            interval_multiplier = clean_idle_backoff.interval_multiplier,
            clean_idle_backoff_enabled = backoff_enabled,
            superseded_retry_backoff_enabled = superseded_retry_interval.is_some(),
            superseded_cycles = superseded_backoff.consecutive_cycles,
            deferred_retry_backoff_enabled = deferred_retry_interval.is_some(),
            deferred_cycles = deferred_backoff.consecutive_cycles,
            scope_fallback_retry_backoff_enabled = scope_fallback_retry_interval.is_some(),
            scope_fallback_cycles = scope_fallback_backoff.consecutive_cycles,
            lifecycle_active = maintenance_features.lifecycle,
            replication_active = maintenance_features.replication,
            feature_inspection_failed = maintenance_features.inspection_failed,
            state = "wait_scheduled",
            "Scanner cycle wait scheduled"
        );

        let activity_poll_interval = backoff_enabled.then_some(runtime_config.cycle_interval.max(Duration::from_secs(1)));
        let movement_generation_before_wait = storeapi.scanner_data_movement_generation();
        let movement_changed = storeapi.scanner_data_movement_changed();
        let movement_store = storeapi.clone();
        let movement = ScannerMovementWaitContext {
            movement_generation_seen: Some(movement_generation_before_wait),
            movement_changed,
            current_movement_generation: move || movement_store.scanner_data_movement_generation(),
            is_lock_lost: || guard.is_lock_lost(),
            recovery_wake: Some(&SCANNER_CYCLE_RECOVERY_WAKE),
        };
        let wake_reason = wait_for_next_scanner_cycle_with_activity_and_movement(
            &ctx,
            wait_plan.delay,
            activity_poll_interval,
            &mut scanner_activity_seen,
            ScannerCycleObservedGenerations::for_wait(
                &runtime_config,
                convergence_retry_interval,
                dirty_usage_generation_seen,
                runtime_config_generation_seen,
                maintenance_generation_before_wait,
            ),
            movement,
            || probe_scanner_activity(storeapi.as_ref(), distributed),
        )
        .await;
        record_scanner_cycle_wakeup(wake_reason);
        scanner_activity_backoff_blocked =
            scanner_activity_backoff_blocked_after_wake(scanner_activity_backoff_blocked, wake_reason);
        match wake_reason {
            ScannerCycleWakeReason::Cancelled => break,
            ScannerCycleWakeReason::LeaderLockLost => {
                record_scanner_leader_lock_lost("Scanner leader lock lost while waiting for the next cycle").await;
                break;
            }
            ScannerCycleWakeReason::RuntimeConfig => {
                runtime_config_generation_seen = scanner_runtime_config_generation();
                maintenance_generation_seen = None;
                scanner_activity_seen = None;
                clean_idle_backoff.reset();
                continue;
            }
            ScannerCycleWakeReason::MaintenanceConfig => {
                maintenance_generation_seen = None;
                scanner_activity_seen = None;
                clean_idle_backoff.reset();
                continue;
            }
            ScannerCycleWakeReason::ClusterMaintenance => {
                clean_idle_backoff.reset();
            }
            ScannerCycleWakeReason::MovementGeneration => {
                scanner_activity_seen = None;
                clean_idle_backoff.reset();
            }
            ScannerCycleWakeReason::Timer
            | ScannerCycleWakeReason::DirtyUsage
            | ScannerCycleWakeReason::ClusterActivity
            | ScannerCycleWakeReason::ClusterActivityUnavailable
            | ScannerCycleWakeReason::Recovery => {}
        }

        if wake_reason == ScannerCycleWakeReason::DirtyUsage {
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_CYCLE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "dirty_usage_wakeup",
                "Scanner cycle woke for dirty usage work"
            );
        }
        if matches!(
            wake_reason,
            ScannerCycleWakeReason::ClusterActivity
                | ScannerCycleWakeReason::ClusterMaintenance
                | ScannerCycleWakeReason::ClusterActivityUnavailable
        ) {
            let cluster_activity_verified = wake_reason == ScannerCycleWakeReason::ClusterActivity;
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_CYCLE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "cluster_activity_wakeup",
                cluster_activity_verified,
                "Scanner cycle woke for cluster activity"
            );
        }

        if guard.is_lock_lost() {
            record_scanner_leader_lock_lost("Scanner leader lock lost before starting the next cycle").await;
            break;
        }
        let pause_backlog_observation = current_scanner_pause_backlog_observation(&storeapi).await;
        pause_backlog.observe(pause_backlog_observation).await;
        #[cfg(test)]
        notify_scanner_runtime_observed_for_test(&storeapi, pause_backlog_observation);
        if pause_backlog_observation.paused {
            continue;
        }
        let pause_backlog_attempt = if usage_bootstrap_rebuild.pending() {
            pause_backlog
                .begin_usage_bootstrap_rebuild_attempt(scanner_pause_backlog_now())
                .await
        } else {
            pause_backlog.begin_attempt(scanner_pause_backlog_now()).await
        };
        if pause_backlog_attempt == ScannerPauseBacklogAttemptDecision::RateLimited {
            continue;
        }
        let usage_bootstrap_pending_before_cycle = usage_bootstrap_rebuild.pending();
        let dirty_generation_before_cycle = dirty_usage_generation();
        let cycle_ctx = ctx.child_token();
        let cycle_budget = ScannerCycleBudget::new_with_runtime_progress_tracking(&cycle_ctx, scanner_cycle_budget_config());
        record_scanner_cycle_attempt(Some(wake_reason));
        let outcome = match await_scanner_cycle_with_budget_fence(
            &cycle_ctx,
            &cycle_budget,
            run_data_scanner_cycle_with_budget(
                &cycle_ctx,
                &storeapi,
                &mut cycle_info,
                &mut cycle_revision,
                leader_epoch,
                cycle_budget.clone(),
                ScannerCycleScheduling {
                    // Cache reuse is admitted by the persisted coverage proof in ScannerIO.
                    // Bootstrap remains pending until authoritative publication succeeds.
                    requires_full_scan: maintenance_features.requires_full_scan(
                        maintenance_generation_seen,
                        scanner_maintenance_generation(),
                        wake_reason,
                        superseded_retry_interval.is_some() || scope_fallback_retry_interval.is_some(),
                    ),
                    service_cohort: Some(service_cohort.clone()),
                    scope_fallback: Some(scope_fallback_slot.clone()),
                },
            ),
            guard.lock_lost_notified(),
        )
        .await
        {
            ScannerCycleWaitOutcome::Completed(outcome) => outcome,
            ScannerCycleWaitOutcome::LockLost => {
                record_scanner_leader_lock_lost("Scanner leader lock lost during a scanner cycle").await;
                global_metrics().set_cycle(None).await;
                finish_scanner_leader_iteration(true, "lost", String::new()).await;
                return Ok(());
            }
            ScannerCycleWaitOutcome::Cancelled => {
                global_metrics().set_cycle(None).await;
                finish_scanner_leader_iteration(guard.is_lock_lost(), "stopped", String::new()).await;
                return Ok(());
            }
            ScannerCycleWaitOutcome::Deadline { worker_stopped } => {
                handle_scanner_cycle_deadline(
                    &ctx,
                    storeapi.clone(),
                    ScannerCycleDeadlineState {
                        cycle_info: &mut cycle_info,
                        cycle_revision: &mut cycle_revision,
                        leader_epoch: &mut leader_epoch,
                        cycle_budget: &cycle_budget,
                        allow_bootstrap_pending: allow_usage_floor_bootstrap_pending,
                    },
                    worker_stopped,
                    &mut guard,
                )
                .await;
                finish_scanner_leader_iteration(guard.is_lock_lost(), "stopped", String::new()).await;
                return Ok(());
            }
        };
        finish_scanner_pause_backlog_cycle(&mut pause_backlog, &storeapi, pause_backlog_attempt, outcome).await;
        if outcome == ScannerCycleOutcome::StatePersistenceFailed
            && let Err(reason) =
                reload_cycle_state_under_same_epoch(&storeapi, &mut cycle_info, &mut cycle_revision, leader_epoch).await
        {
            global_metrics().set_cycle(None).await;
            let error = format!("scanner cycle state persistence failed; {reason}; retrying from durable state");
            finish_scanner_leader_iteration(false, "state_persist_failed", error.clone()).await;
            return Err(ScannerError::Other(error));
        }
        if usage_bootstrap_rebuild.record_cycle(outcome) {
            clean_idle_backoff.reset();
        }
        superseded_backoff.record_retryable_cycle(outcome == ScannerCycleOutcome::Superseded);
        deferred_backoff.record_retryable_cycle(matches!(
            outcome,
            ScannerCycleOutcome::Deferred(_) | ScannerCycleOutcome::StatePersistenceFailed
        ));
        // Writes that land after the walk would otherwise wake the next full
        // walk immediately; with no dirty bucket left there is nothing to pace.
        let scope_fallback = scope_fallback_slot.take().filter(|_| {
            matches!(
                outcome,
                ScannerCycleOutcome::Completed | ScannerCycleOutcome::CompletedWithPendingMaintenance
            ) && dirty_usage_buckets_pending()
        });
        scope_fallback_backoff.record_retryable_cycle(scope_fallback.is_some());
        dirty_usage_generation_seen = dirty_generation_before_cycle;
        if guard.is_lock_lost() {
            record_scanner_leader_lock_lost("Scanner leader lock lost during a scanner cycle").await;
            break;
        }
        let current_runtime_generation = scanner_runtime_config_generation();
        let runtime_config_changed = current_runtime_generation != runtime_config_generation_seen;
        runtime_config_generation_seen = current_runtime_generation;
        if runtime_config_changed {
            maintenance_generation_seen = None;
            clean_idle_backoff.reset();
        }

        let runtime_config = resolve_scanner_runtime_config();
        let current_maintenance_generation = scanner_maintenance_generation();
        let maintenance_config_changed =
            maintenance_generation_seen.is_some_and(|generation| generation != current_maintenance_generation);
        let retry_failed_inspection = maintenance_inspection_retry.retry_due(maintenance_features, wake_reason, Instant::now());
        if clean_idle_topology_supported && (maintenance_config_changed || retry_failed_inspection) {
            let Some((features, generation)) = detect_stable_scanner_maintenance_features(&ctx, &storeapi).await else {
                break;
            };
            maintenance_features = features;
            maintenance_generation_seen = Some(generation);
            maintenance_inspection_retry.record_inspection(features, Instant::now());
        }

        if runtime_config_changed {
            clean_idle_backoff.reset();
            continue;
        }
        if maintenance_config_changed {
            scanner_activity_seen = None;
            scanner_activity_backoff_blocked = scanner_activity_backoff_blocked_after_wake(
                scanner_activity_backoff_blocked,
                ScannerCycleWakeReason::MaintenanceConfig,
            );
            clean_idle_backoff.reset();
            continue;
        }

        let scanner_activity_observation = if scanner_activity_probe_required(
            clean_idle_topology_supported,
            scanner_activity_backoff_blocked,
            maintenance_features,
            &runtime_config,
        ) {
            observe_scanner_activity(&storeapi, distributed, &mut scanner_activity_seen).await
        } else {
            scanner_activity_seen = None;
            ScannerActivityObservation::NotRequired
        };
        if scanner_activity_observation == ScannerActivityObservation::MaintenanceChanged {
            scanner_activity_backoff_blocked = true;
        }
        let scanner_activity_ready = !scanner_activity_backoff_blocked && scanner_activity_seen.is_some();
        let backoff_enabled = usage_bootstrap_rebuild.clean_idle_backoff_enabled(scanner_clean_idle_backoff_enabled(
            clean_idle_topology_supported,
            scanner_activity_ready,
            maintenance_features,
            &runtime_config,
        ));
        record_scanner_cycle_result(
            &mut clean_idle_backoff,
            &runtime_config,
            backoff_enabled,
            wake_reason,
            outcome,
            scanner_cycle_observed_dirty_work(
                dirty_usage_pending_before_wait,
                dirty_generation_before_wait,
                dirty_usage_generation(),
            ) || usage_bootstrap_pending_before_cycle
                || scanner_activity_observed_work(scanner_activity_observation),
        );
    }

    global_metrics().set_cycle(None).await;
    finish_scanner_leader_iteration(guard.is_lock_lost(), "stopped", String::new()).await;

    debug!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_CYCLE_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        state = "stopped",
        "Scanner runtime stopped"
    );

    Ok(())
}

/// Re-read durable cycle state after a save that could not be confirmed.
/// The leader keeps its epoch only while storage still names that epoch; a
/// newer owner or an unreadable primary ends the iteration so the next claim
/// re-fences (rustfs/rustfs#8141: a non-fence failure must not change the
/// `(cycle, epoch)` fence peers validate).
async fn reload_cycle_state_under_same_epoch<S>(
    storeapi: &Arc<S>,
    cycle_info: &mut CurrentCycle,
    cycle_revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
) -> Result<(), String>
where
    S: ScannerObjectIO,
{
    let (persisted, revision) = read_config_with_revision(storeapi.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .map_err(|err| format!("durable cycle state could not be read: {err}"))?;
    let persisted = persisted.ok_or_else(|| "durable cycle state is missing".to_string())?;
    let (persisted_cycle, persisted_epoch) =
        decode_scanner_cycle_state(&persisted).map_err(|err| format!("durable cycle state is invalid: {err}"))?;
    if persisted_epoch != leader_epoch {
        return Err(format!(
            "durable cycle state is owned by leadership epoch {persisted_epoch}, this leader holds {leader_epoch}"
        ));
    }
    warn!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_PERSIST_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
        state = "state_persist_reloaded",
        leader_epoch,
        durable_next_cycle = persisted_cycle.next,
        in_memory_next_cycle = cycle_info.next,
        "Scanner cycle state reloaded under the same leadership epoch after an unconfirmed save"
    );
    *cycle_info = persisted_cycle;
    *cycle_revision = revision;
    Ok(())
}

struct ScannerScanModeGuard;

impl ScannerScanModeGuard {
    fn new(scan_mode: HealScanMode) -> Self {
        global_metrics().set_current_scan_mode(scan_mode);
        Self
    }
}

impl Drop for ScannerScanModeGuard {
    fn drop(&mut self) {
        global_metrics().clear_current_scan_mode();
    }
}

async fn final_data_usage_publication_defer_reason<S>(
    storeapi: &S,
    status: ScannerCycleStatus,
    observational_only: bool,
) -> Option<ScannerCycleDeferReason>
where
    S: ScannerStorage,
{
    match status {
        ScannerCycleStatus::Complete | ScannerCycleStatus::Superseded => {
            if let Some(reason) = scanner_local_publication_defer_reason(storeapi, observational_only).await {
                return Some(reason);
            }
            if status == ScannerCycleStatus::Complete {
                let distributed = storeapi.setup_is_dist_erasure().await;
                match probe_scanner_activity(storeapi, distributed).await {
                    Ok(snapshot) if scanner_activity_allows_usage_publication(&snapshot) => None,
                    Ok(_) => Some(ScannerCycleDeferReason::DataMovement),
                    Err(_) => Some(ScannerCycleDeferReason::ActivityBaselineUnavailable),
                }
            } else {
                // A superseded cycle is explicitly observational and cannot
                // replace the authoritative snapshot. It may still be
                // persisted as a convergence baseline for the next cycle.
                None
            }
        }
        ScannerCycleStatus::Deferred(reason) => Some(reason),
        // Incomplete cycles may publish a non-authoritative observational
        // snapshot when at least one set has a usable current/LKG view.
        ScannerCycleStatus::Incomplete => None,
    }
}

/// The local publication fence. Only storage data movement defers a
/// publication: an active or terminal decommission, a rebalance, or an
/// exhausted movement sequence. An ordinary PUT whose rename fanout is still
/// pending is not a fence; its bucket is shaded through the dirty-usage
/// generation and certified per bucket, so a sustained write load cannot
/// hold the authoritative snapshot back (rustfs/rustfs#8494,
/// rustfs/rustfs#8305).
async fn scanner_local_publication_defer_reason<S>(storeapi: &S, observational_only: bool) -> Option<ScannerCycleDeferReason>
where
    S: ScannerStorage,
{
    let blocked = if observational_only {
        storeapi.scanner_data_movement_publication_blocked().await
    } else {
        storeapi.scanner_data_usage_publication_blocked().await
    };
    if !blocked {
        return None;
    }
    // Both barriers describe storage movement. The authoritative barrier also
    // holds terminal decommission entries an operator has not cleared, which
    // is the same durable, rate-limited catch-up debt.
    debug!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_PERSIST_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        stage = "local_barrier",
        blocker = "data_movement",
        "Scanner usage publication deferred"
    );
    Some(ScannerCycleDeferReason::DataMovement)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerCyclePublicationPlan {
    ProgressOnly,
    Observational,
    Authoritative,
}

impl ScannerCyclePublicationPlan {
    fn for_result(result: &ScannerCycleResult) -> Self {
        match (result.has_usage_update(), result.status) {
            (false, _) => Self::ProgressOnly,
            (
                true,
                ScannerCycleStatus::Superseded
                | ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
                | ScannerCycleStatus::Incomplete,
            ) => Self::Observational,
            (true, _) => Self::Authoritative,
        }
    }

    fn has_usage_update(self) -> bool {
        self != Self::ProgressOnly
    }

    fn is_observational(self) -> bool {
        self == Self::Observational
    }

    fn uses_observational_lease(self) -> bool {
        self != Self::Authoritative
    }

    async fn await_leases<T, F, Fut>(
        self,
        parent: &CancellationToken,
        budget: &ScannerCycleBudget,
        cycle: u64,
        leader_epoch: u64,
        mut acquire: F,
    ) -> ScannerPublicationProofWait<T, EcstoreError>
    where
        F: FnMut(bool) -> Fut,
        Fut: Future<Output = Result<T, EcstoreError>>,
    {
        // Exhausting the walk budget must not cancel the movement proof needed
        // to save partial progress. No usage update means namespace stability
        // is unnecessary, but peer session/generation and movement checks stay.
        let proof_ctx = if self == Self::ProgressOnly {
            parent.clone()
        } else {
            budget.token()
        };
        let started_at = Instant::now();
        let publication_kind = match self {
            Self::ProgressOnly => "progress_only",
            Self::Observational => "observational",
            Self::Authoritative => "authoritative",
        };
        let lease_purpose = if self.uses_observational_lease() {
            "observational"
        } else {
            "authoritative"
        };
        debug!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "publication_proof_started",
            cycle,
            leader_epoch,
            stage = "lease_acquire",
            publication_kind,
            lease_purpose,
            "Scanner is acquiring peer publication proof"
        );
        let proof = await_scanner_publication_proof(
            &proof_ctx,
            ScannerPublicationProofContext {
                cycle,
                stage: "lease_acquire",
                leader_epoch: Some(leader_epoch),
                lease_purpose: Some(lease_purpose),
            },
            || acquire(self.uses_observational_lease()),
            |err| {
                // The structured reason comes from local acquisition, not a peer report.
                crate::storage_api::scanner_publication_lease_failure_info(err)
                    .is_some_and(|info| info.reason == Some("channel_acquisition_timed_out"))
                    || scanner_publication_lease_error_is_retryable(&err.to_string())
            },
            crate::storage_api::scanner_publication_lease_failure_info,
        );
        let (proof, timed_out) = if self == Self::ProgressOnly {
            // Object/directory budgets can expire with no runtime deadline.
            // Bound the control-plane drain independently of the walk budget.
            match tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, proof).await {
                Ok(proof) => (proof, false),
                Err(_) => (ScannerPublicationProofWait::Cancelled, true),
            }
        } else {
            (proof.await, false)
        };
        let outcome = if timed_out {
            "timed_out"
        } else {
            match &proof {
                ScannerPublicationProofWait::Ready(_) => "acquired",
                ScannerPublicationProofWait::Rejected(_) => "rejected",
                ScannerPublicationProofWait::Cancelled => "cancelled",
            }
        };
        debug!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "publication_lease_wait_finished",
            cycle,
            leader_epoch,
            stage = "lease_acquire",
            publication_kind,
            lease_purpose,
            outcome,
            parent_cancelled_at_return = parent.is_cancelled(),
            proof_token_cancelled_at_return = proof_ctx.is_cancelled(),
            elapsed = ?started_at.elapsed(),
            "Scanner peer publication lease wait finished"
        );
        proof
    }
}

/// Re-prove the guardrail after every peer granted a publication lease. The
/// comparison uses the guardrail fields (`scanner_activity_guardrail_veto`),
/// not the full digest: local generation movement in this window is the next
/// cycle's work and its buckets stay in the dirty map. A peer whose dirty
/// usage moved keeps deferring in this phase, as it does during the walk.
fn scanner_post_lease_activity_defer_reason(
    baseline: Option<&ScannerActivitySnapshot>,
    activity: Result<ScannerActivitySnapshot, String>,
) -> Option<ScannerCycleDeferReason> {
    match activity {
        Ok(snapshot)
            if scanner_activity_allows_usage_publication(&snapshot)
                && baseline.is_some_and(|baseline| {
                    scanner_activity_guardrail_veto(baseline, &snapshot).is_none()
                        && scanner_peer_dirty_usage_moved(baseline, &snapshot).is_none()
                }) =>
        {
            None
        }
        observed => {
            let blocker = match &observed {
                Err(_) => "probe_failed",
                Ok(snapshot) if !scanner_activity_allows_usage_publication(snapshot) => "publication_blocked",
                Ok(_) if baseline.is_none() => "baseline_missing",
                Ok(snapshot) => baseline
                    .and_then(|baseline| {
                        scanner_activity_guardrail_veto(baseline, snapshot)
                            .or_else(|| scanner_peer_dirty_usage_moved(baseline, snapshot))
                    })
                    .map_or("activity_changed", |veto| veto.field),
            };
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                stage = "post_lease_activity",
                blocker,
                "Scanner usage publication deferred"
            );
            Some(ScannerCycleDeferReason::ActivityBaselineUnavailable)
        }
    }
}

fn scanner_remote_publication_lease_failure_defer_reason(
    remote_lease_targets: &[(String, String, u64)],
    remote_leases_valid: bool,
    activity_after_failure: Result<ScannerActivitySnapshot, String>,
) -> ScannerCycleDeferReason {
    if !remote_leases_valid {
        return ScannerCycleDeferReason::DataMovement;
    }
    let Ok(snapshot) = activity_after_failure else {
        return ScannerCycleDeferReason::ActivityBaselineUnavailable;
    };
    if !scanner_activity_allows_usage_publication(&snapshot) {
        return ScannerCycleDeferReason::DataMovement;
    }
    if scanner_publication_lease_targets_match_activity(remote_lease_targets, &snapshot) {
        ScannerCycleDeferReason::ActivityBaselineUnavailable
    } else {
        ScannerCycleDeferReason::DataMovement
    }
}

fn scanner_publication_lease_targets_match_activity(
    remote_lease_targets: &[(String, String, u64)],
    activity: &ScannerActivitySnapshot,
) -> bool {
    let mut expected = BTreeMap::new();
    for (host, instance_id, movement_generation) in remote_lease_targets {
        if host.is_empty()
            || expected
                .insert(host.as_str(), (instance_id.as_str(), *movement_generation))
                .is_some()
        {
            return false;
        }
    }

    let mut observed_remote_targets = 0usize;
    for (host, node_activity) in activity {
        if host == LOCAL_SCANNER_ACTIVITY_NODE {
            continue;
        }
        observed_remote_targets = observed_remote_targets.saturating_add(1);
        let Some((expected_instance_id, expected_movement_generation)) = expected.get(host.as_str()) else {
            return false;
        };
        if node_activity.instance_id != *expected_instance_id
            || node_activity.movement_generation != *expected_movement_generation
        {
            return false;
        }
    }

    observed_remote_targets == remote_lease_targets.len()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerCyclePreCommitOutcome {
    RecoverCacheCycle(u64),
    Deferred(ScannerCycleDeferReason),
}

fn scanner_cycle_pre_commit_outcome(
    required_cycle_floor: Option<u64>,
    usage_persist_outcome: &DataUsagePersistOutcome,
) -> Option<ScannerCyclePreCommitOutcome> {
    // Keep the publication barrier fail-closed: `.bloomcycle.bin` uses the
    // same routed writer and its floor must remain pending while data movement
    // hides the source pool.
    match usage_persist_outcome {
        DataUsagePersistOutcome::Deferred(reason) => Some(ScannerCyclePreCommitOutcome::Deferred(*reason)),
        _ => required_cycle_floor.map(ScannerCyclePreCommitOutcome::RecoverCacheCycle),
    }
}

fn update_scanner_cache_cycle_recovery_status(
    requested_cycle: u64,
    leader_epoch: u64,
    required_cycle_floor: Option<u64>,
    pre_commit_outcome: Option<ScannerCyclePreCommitOutcome>,
    cache_scope_complete: bool,
) {
    match (required_cycle_floor, pre_commit_outcome) {
        (Some(required_cycle), _) => {
            record_scanner_cache_cycle_ahead(requested_cycle, required_cycle, leader_epoch);
        }
        (None, Some(ScannerCyclePreCommitOutcome::Deferred(_))) => {
            // A deferred scan may not have covered the cache that established
            // the existing floor, so it cannot prove recovery is complete.
        }
        (None, _) if cache_scope_complete => clear_scanner_cache_cycle_ahead(),
        (None, _) => {}
    }
}

fn scanner_cycle_completion_outcome(
    scan_status: ScannerCycleStatus,
    usage_persist_outcome: DataUsagePersistOutcome,
    has_dirty_usage: bool,
    has_failed_dirty_usage: bool,
) -> ScannerCycleOutcome {
    match (scan_status, usage_persist_outcome) {
        (_, DataUsagePersistOutcome::Deferred(reason)) => ScannerCycleOutcome::Deferred(reason),
        (_, DataUsagePersistOutcome::Failed) => ScannerCycleOutcome::Failed,
        (ScannerCycleStatus::Deferred(reason), DataUsagePersistOutcome::NoUpdate)
            if !has_dirty_usage && !has_failed_dirty_usage =>
        {
            ScannerCycleOutcome::Deferred(reason)
        }
        (ScannerCycleStatus::Deferred(_), _) => ScannerCycleOutcome::Failed,
        (ScannerCycleStatus::Superseded, _) if !has_failed_dirty_usage => ScannerCycleOutcome::Superseded,
        (ScannerCycleStatus::Superseded, _) => ScannerCycleOutcome::Failed,
        (
            ScannerCycleStatus::Incomplete,
            DataUsagePersistOutcome::Saved | DataUsagePersistOutcome::AlreadyDurable | DataUsagePersistOutcome::PriorCycleDurable,
        ) if !has_failed_dirty_usage => ScannerCycleOutcome::Partial,
        (ScannerCycleStatus::Incomplete, _) => ScannerCycleOutcome::Failed,
        (
            ScannerCycleStatus::Complete,
            DataUsagePersistOutcome::Saved | DataUsagePersistOutcome::AlreadyDurable | DataUsagePersistOutcome::PriorCycleDurable,
        ) => ScannerCycleOutcome::Completed,
        (ScannerCycleStatus::Complete, DataUsagePersistOutcome::Current) if !has_dirty_usage => ScannerCycleOutcome::Completed,
        (ScannerCycleStatus::Complete, _) => ScannerCycleOutcome::Failed,
    }
}

fn finalize_scanner_cycle_result(
    scan_cycle_result: crate::scanner_io::ScannerCycleResult,
    publication: DataUsagePublicationResult,
) -> (ScannerCycleOutcome, bool, Vec<ScannerDirtyUsageAcknowledgement>) {
    let (usage_persist_outcome, proof) = publication.into_parts();
    let completion_outcome = scanner_cycle_completion_outcome_for_result(&scan_cycle_result, usage_persist_outcome);
    let durable_complete_snapshot = scan_cycle_result.status == ScannerCycleStatus::Complete
        && matches!(
            usage_persist_outcome,
            DataUsagePersistOutcome::Saved | DataUsagePersistOutcome::AlreadyDurable
        )
        && scan_cycle_result.publication_expectation().as_ref().is_some_and(|expected| {
            proof
                .as_ref()
                .is_some_and(|proof| proof.verified_version_for(expected).is_some())
        });
    let pending_maintenance_work = scan_cycle_result.has_pending_maintenance_work()
        || (scan_cycle_result.has_dirty_usage_to_acknowledge() && !durable_complete_snapshot);
    let remote_dirty_usage_acknowledgements = if durable_complete_snapshot {
        match proof {
            Some(proof) => scan_cycle_result.acknowledge_durable_usage(&proof),
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    (
        completion_outcome,
        pending_maintenance_work || crate::scanner_io::dirty_usage_buckets_pending(),
        remote_dirty_usage_acknowledgements,
    )
}

fn scanner_cycle_completion_outcome_for_result(
    scan_cycle_result: &crate::scanner_io::ScannerCycleResult,
    usage_persist_outcome: DataUsagePersistOutcome,
) -> ScannerCycleOutcome {
    let has_dirty_usage = scan_cycle_result.has_dirty_usage_to_acknowledge();
    let has_failed_dirty_usage = scan_cycle_result.has_failed_dirty_usage();
    if scan_cycle_result.status == ScannerCycleStatus::Incomplete
        && scan_cycle_result.has_partial_bucket_progress()
        && !scan_cycle_result.has_usage_update()
        && !has_failed_dirty_usage
        && usage_persist_outcome == DataUsagePersistOutcome::NoUpdate
    {
        // No root was submitted: retain dirty usage and advance only the
        // fenced partial-cycle state. A fresh sweep still has to certify usage.
        return ScannerCycleOutcome::Partial;
    }
    if scan_cycle_result.has_usage_update()
        && matches!(
            scan_cycle_result.status,
            ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable)
        )
    {
        return match usage_persist_outcome {
            DataUsagePersistOutcome::Saved
            | DataUsagePersistOutcome::AlreadyDurable
            | DataUsagePersistOutcome::PriorCycleDurable
            | DataUsagePersistOutcome::Current
                if !has_failed_dirty_usage =>
            {
                ScannerCycleOutcome::Partial
            }
            DataUsagePersistOutcome::Deferred(reason) => ScannerCycleOutcome::Deferred(reason),
            _ => ScannerCycleOutcome::Failed,
        };
    }
    scanner_cycle_completion_outcome(scan_cycle_result.status, usage_persist_outcome, has_dirty_usage, has_failed_dirty_usage)
}

/// Decide whether an incoming usage snapshot must be skipped as stale, given the local
/// wall clock `now`. Mirrors `stale_data_usage_persist_reason` in
/// `crates/ecstore/src/data_usage/mod.rs` — keep the two consistent.
///
/// If the persisted `existing.last_update` is future-dated beyond
/// [`rustfs_data_usage::USAGE_LAST_UPDATE_FUTURE_TOLERANCE`] (clock step-back or a
/// slower-clock scanner leader), it is untrustworthy: the save is allowed so usage
/// stats cannot freeze forever.
fn stale_data_usage_update_reason(
    incoming: &DataUsageInfo,
    existing: &DataUsageInfo,
    now: std::time::SystemTime,
) -> Option<&'static str> {
    match (incoming.scanner_epoch, existing.scanner_epoch) {
        (Some(incoming_epoch), Some(existing_epoch)) if incoming_epoch < existing_epoch => {
            return Some("older_scanner_epoch");
        }
        (Some(incoming_epoch), Some(existing_epoch)) if incoming_epoch > existing_epoch => return None,
        (Some(_), None) => return None,
        (None, Some(_)) => return Some("missing_incoming_scanner_epoch"),
        (Some(_), Some(_)) | (None, None) => {}
    }

    match (incoming.scanner_cycle, existing.scanner_cycle) {
        (Some(incoming_cycle), Some(existing_cycle)) if incoming_cycle < existing_cycle => {
            return Some("older_scanner_cycle");
        }
        (Some(incoming_cycle), Some(existing_cycle)) if incoming_cycle == existing_cycle => {
            return Some("conflicting_same_scanner_cycle");
        }
        (Some(_), Some(_)) | (Some(_), None) => return None,
        (None, Some(_)) => return Some("missing_incoming_scanner_cycle"),
        (None, None) => {}
    }

    match (incoming.last_update, existing.last_update) {
        (Some(new_ts), Some(existing_ts))
            if new_ts <= existing_ts && !rustfs_data_usage::usage_last_update_is_untrusted_future(existing_ts, now) =>
        {
            Some("older_or_equal_last_update")
        }
        (None, Some(_)) => Some("missing_incoming_last_update"),
        _ => None,
    }
}

fn data_usage_reintroduces_missing_bucket(incoming: &DataUsageInfo, existing: Option<&DataUsageInfo>) -> bool {
    let Some(existing) = existing else {
        return !incoming.buckets_usage.is_empty() || !incoming.bucket_sizes.is_empty();
    };
    incoming
        .buckets_usage
        .keys()
        .chain(incoming.bucket_sizes.keys())
        .any(|bucket| !existing.buckets_usage.contains_key(bucket) && !existing.bucket_sizes.contains_key(bucket))
}

/// Store data usage info in backend. Will store all objects sent on the receiver until closed.
mod activity;
mod backlog;
mod cycle_state;
mod heal_info;
mod leadership;
mod usage_store;

use activity::*;
use backlog::*;
pub use backlog::{ScannerPauseBacklogReplicaId, ScannerPauseBacklogReplicaStatus, ScannerPauseBacklogReplicaStatusState};
use cycle_state::*;
use leadership::*;
pub(crate) use usage_store::RootPublicationProof;
use usage_store::*;

#[cfg(test)]
pub(crate) use activity::scanner_node_activity_for_tests;
pub use activity::scanner_topology_digest;
pub(crate) use activity::{
    LOCAL_SCANNER_ACTIVITY_NODE, ScannerActivityGuardrailVeto, ScannerActivitySnapshot, ScannerDirtyUsageAcknowledgement,
    ScannerDirtyUsageAcknowledgementKind, probe_scanner_activity, scanner_activity_allows_usage_publication,
    scanner_activity_coverage_digest, scanner_activity_dirty_usage_state_for_host, scanner_activity_guardrail_veto,
    scanner_activity_publication_lease_targets, scanner_activity_snapshot_digest, scanner_activity_structural_digest,
    scanner_dirty_usage_acknowledgements, scanner_peer_dirty_usage_moved,
};
pub(crate) use activity::{ScannerCycleOutcome, scanner_cycle_outcome_with_pending_maintenance};
pub use backlog::{
    ScannerPauseBacklogAlertReason, ScannerPauseBacklogPhase, ScannerPauseBacklogStatus, ScannerPauseBacklogThresholds,
    register_scanner_pause_backlog_retirement, scanner_pause_backlog_status,
};
#[cfg(test)]
pub(crate) use cycle_state::encode_scanner_cycle_fence_for_test;
pub use cycle_state::{
    SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD, ScannerCycleRecoveryMarker, ScannerCycleRecoveryStatus,
    ScannerRecoveryIntentAcceptResult, ScannerRecoveryIntentConflict, ScannerRecoveryIntentRecord, ScannerRecoveryIntentRequest,
    ScannerUsageStateResetResult, accept_scanner_usage_recovery_intent, get_scanner_usage_recovery_intent,
    reset_scanner_cycle_recovery, reset_scanner_usage_state_for_full_rebuild, run_scanner_usage_recovery_intent,
    scanner_cycle_recovery_status, scanner_recovery_actor_sha256,
};
pub(crate) use cycle_state::{
    current_scanner_leader_epoch, decode_persisted_scanner_cycle_fence, load_scanner_cycle_state_for_startup,
};
pub use heal_info::{
    BackgroundHealInfo, ScannerBitrotStartSource, ScannerBitrotStatus, read_background_heal_info, save_background_heal_info,
    scanner_bitrot_status,
};
pub use usage_store::store_data_usage_in_backend;

#[cfg(test)]
mod tests;
