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
use std::sync::{Arc, LazyLock, RwLock};

use crate::ScannerObjectIO;
use crate::data_usage_define::{
    BACKGROUND_HEAL_INFO_PATH, DATA_USAGE_BLOOM_NAME_PATH, DATA_USAGE_OBJ_NAME_PATH, DataUsageCache, DataUsageCacheRevision,
    read_config_with_revision,
};
use crate::runtime_config::{
    ScannerRuntimeConfig, ScannerRuntimeConfigSource, refresh_scanner_runtime_config_from_global, scanner_bitrot_cycle,
    scanner_cycle_interval, scanner_runtime_config_changed, scanner_runtime_config_generation, scanner_start_delay,
    set_scanner_default_cycle_secs,
};
use crate::scanner_budget::{ScannerCycleBudget, ScannerCycleBudgetConfig, ScannerCycleBudgetReason};
use crate::scanner_folder::{data_usage_update_dir_cycles, heal_object_select_prob};
use crate::scanner_io::{
    ScannerCycleStatus, ScannerIOCycle, dirty_usage_bucket_notified, dirty_usage_buckets_pending, dirty_usage_generation,
    scanner_maintenance_changed, scanner_maintenance_generation,
};
use crate::sleeper::{SCANNER_SLEEPER, set_scanner_default_speed};
use crate::{DataUsageInfo, ScannerActivityGuard, ScannerError};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::metrics::{
    CurrentCycle, Metric, Metrics, ScanCyclePartialReason, ScannerUsageSaveResult, ScannerWorkSource, emit_scan_cycle_complete,
    emit_scan_cycle_partial_with_source, emit_scan_cycle_superseded, global_metrics,
};
use rustfs_config::ScannerSpeed;
#[cfg(test)]
use rustfs_config::{
    ENV_SCANNER_BITROT_CYCLE_SECS, ENV_SCANNER_CYCLE_MAX_DIRECTORIES, ENV_SCANNER_CYCLE_MAX_DURATION_SECS,
    ENV_SCANNER_CYCLE_MAX_OBJECTS,
};
use rustfs_config::{ENV_SCANNER_CYCLE, ENV_SCANNER_SPEED, ENV_SCANNER_START_DELAY_SECS};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::storage_api::scan::{
    BucketOperations, BucketOptions, NamespaceLocking as _, SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION,
    SCANNER_ACTIVITY_PROTOCOL_VERSION,
};
use crate::{
    ECStore, EcstoreError, RUSTFS_META_BUCKET, ScannerLifecycleConfigExt as _, ScannerReplicationConfigExt as _,
    get_lifecycle_config, get_replication_config, read_config, replace_bucket_usage_memory_from_info, save_config,
    save_config_shared_with_preconditions, save_config_with_preconditions, scanner_is_erasure_sd,
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
const CLEAN_IDLE_BACKOFF_FACTOR: u32 = 2;
const SCANNER_LEADER_LOCK_POLL_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(not(test))]
const SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(50);
const MAINTENANCE_FEATURE_INSPECTION_TIMEOUT: Duration = Duration::from_secs(30);
const MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MAINTENANCE_FEATURE_INSPECTION_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);
const MAX_MAINTENANCE_FEATURE_INSPECTION_ATTEMPTS: usize = 2;
const SCANNER_PERSIST_CAS_RETRIES: usize = 2;
const SCANNER_CYCLE_STATE_MAGIC: &[u8; 8] = b"RSCYC001";
const SCANNER_CYCLE_STATE_HEADER_LEN: usize = 24;
#[cfg(test)]
const ENV_SCANNER_START_DELAY_SECS_DEPRECATED: &str = "RUSTFS_DATA_SCANNER_START_DELAY_SECS";

#[derive(Debug, thiserror::Error)]
enum ScannerCycleStateError {
    #[error("failed to encode scanner cycle state: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("failed to decode scanner cycle state: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("{0}")]
    InvalidData(&'static str),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PersistedUsageFloor {
    next_cycle: u64,
    leader_epoch: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[non_exhaustive]
pub struct ScannerCycleScheduleStatus {
    effective_interval_seconds: u64,
    clean_idle_backoff_enabled: bool,
    clean_idle_backoff_multiplier: u64,
}

impl Default for ScannerCycleScheduleStatus {
    fn default() -> Self {
        Self {
            effective_interval_seconds: 0,
            clean_idle_backoff_enabled: false,
            clean_idle_backoff_multiplier: 1,
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
    clean_idle_backoff_enabled: bool,
    clean_idle_backoff_multiplier: u64,
) {
    let effective_interval_seconds = effective_interval
        .as_secs()
        .saturating_add(u64::from(effective_interval.subsec_nanos() != 0));
    let mut schedule = SCANNER_CYCLE_SCHEDULE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *schedule = ScannerCycleScheduleStatus {
        effective_interval_seconds,
        clean_idle_backoff_enabled,
        clean_idle_backoff_multiplier: clean_idle_backoff_multiplier.max(1),
    };
}

fn reset_scanner_cycle_schedule() {
    record_scanner_cycle_schedule(Duration::ZERO, false, 1);
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
    let interval = interval.max(Duration::from_secs(1));
    // Uniform in [-0.1, 0.1), keeping actual delay within 10% of interval.
    let jitter_factor = (rand::random::<f64>() * 0.2) - 0.1;
    let delay = interval.mul_f64(1.0 + jitter_factor);
    delay.max(Duration::from_secs(1))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerCycleWakeReason {
    Timer,
    DirtyUsage,
    ClusterActivity,
    ClusterMaintenance,
    ClusterActivityUnavailable,
    RuntimeConfig,
    MaintenanceConfig,
    LeaderLockLost,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerCycleOutcome {
    Completed,
    CompletedWithPendingMaintenance,
    Partial,
    Superseded,
    Failed,
}

pub(crate) fn scanner_cycle_outcome_with_pending_maintenance(
    outcome: ScannerCycleOutcome,
    pending_maintenance_work: bool,
) -> ScannerCycleOutcome {
    if outcome == ScannerCycleOutcome::Completed && pending_maintenance_work {
        ScannerCycleOutcome::CompletedWithPendingMaintenance
    } else {
        outcome
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScannerCleanIdleBackoff {
    interval_multiplier: u32,
}

impl Default for ScannerCleanIdleBackoff {
    fn default() -> Self {
        Self { interval_multiplier: 1 }
    }
}

impl ScannerCleanIdleBackoff {
    fn reset(&mut self) {
        self.interval_multiplier = 1;
    }

    fn effective_interval(self, base_interval: Duration, max_interval: Duration, enabled: bool) -> Duration {
        let base_interval = base_interval.max(Duration::from_secs(1));
        if !enabled {
            return base_interval;
        }

        let max_interval = max_interval.max(base_interval);
        base_interval.saturating_mul(self.interval_multiplier).min(max_interval)
    }

    fn record_cycle(
        &mut self,
        base_interval: Duration,
        max_interval: Duration,
        enabled: bool,
        wake_reason: ScannerCycleWakeReason,
        outcome: ScannerCycleOutcome,
        dirty_work_observed: bool,
    ) {
        if !enabled
            || wake_reason != ScannerCycleWakeReason::Timer
            || outcome != ScannerCycleOutcome::Completed
            || dirty_work_observed
        {
            self.reset();
            return;
        }

        let max_interval = max_interval.max(base_interval.max(Duration::from_secs(1)));
        if self.effective_interval(base_interval, max_interval, true) < max_interval {
            self.interval_multiplier = self.interval_multiplier.saturating_mul(CLEAN_IDLE_BACKOFF_FACTOR);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ScannerMaintenanceInspectionRetry {
    consecutive_failures: u32,
    retry_at: Option<Instant>,
}

impl ScannerMaintenanceInspectionRetry {
    fn from_features(features: ScannerMaintenanceFeatures, now: Instant) -> Self {
        let mut retry = Self::default();
        retry.record_inspection(features, now);
        retry
    }

    fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.retry_at = None;
    }

    fn retry_interval(self) -> Option<Duration> {
        if self.consecutive_failures == 0 {
            return None;
        }

        let exponent = self.consecutive_failures.saturating_sub(1).min(31);
        let multiplier = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
        Some(
            MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL
                .saturating_mul(multiplier)
                .min(MAINTENANCE_FEATURE_INSPECTION_RETRY_MAX_INTERVAL),
        )
    }

    fn record_inspection(&mut self, features: ScannerMaintenanceFeatures, now: Instant) {
        if !features.inspection_failed {
            self.reset();
            return;
        }

        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.retry_at = self.retry_interval().map(|interval| now + interval);
    }

    fn retry_due(self, features: ScannerMaintenanceFeatures, wake_reason: ScannerCycleWakeReason, now: Instant) -> bool {
        features.inspection_failed
            && wake_reason == ScannerCycleWakeReason::Timer
            && self.retry_at.is_some_and(|retry_at| now >= retry_at)
    }
}

fn scanner_cycle_observed_dirty_work(
    pending_before_wait: bool,
    generation_before_wait: u64,
    generation_after_cycle: u64,
) -> bool {
    pending_before_wait || generation_before_wait != generation_after_cycle
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScannerCycleWaitPlan {
    effective_interval: Duration,
    clean_idle_max_interval: Duration,
    delay: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScannerCycleObservedGenerations {
    dirty_usage: u64,
    runtime_config: u64,
    maintenance: u64,
}

const LOCAL_SCANNER_ACTIVITY_NODE: &str = "<local>";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScannerNodeActivity {
    instance_id: String,
    namespace_generation: u64,
    maintenance_generation: u64,
    protocol_version: u32,
    topology_digest: [u8; 32],
    data_movement_active: bool,
}

pub(crate) type ScannerActivitySnapshot = BTreeMap<String, ScannerNodeActivity>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerActivityObservation {
    NotRequired,
    Unchanged,
    Changed,
    MaintenanceChanged,
    Unverified,
}

fn scanner_cycle_wait_plan(
    runtime_config: &ScannerRuntimeConfig,
    clean_idle_backoff: ScannerCleanIdleBackoff,
    clean_idle_backoff_enabled: bool,
    jitter: impl FnOnce(Duration) -> Duration,
) -> ScannerCycleWaitPlan {
    let clean_idle_max_interval = scanner_clean_idle_max_interval(runtime_config.cycle_interval, runtime_config);
    let effective_interval =
        clean_idle_backoff.effective_interval(runtime_config.cycle_interval, clean_idle_max_interval, clean_idle_backoff_enabled);
    let delay = cap_clean_idle_cycle_delay(jitter(effective_interval), clean_idle_max_interval, clean_idle_backoff_enabled);

    ScannerCycleWaitPlan {
        effective_interval,
        clean_idle_max_interval,
        delay,
    }
}

fn record_scanner_cycle_result(
    clean_idle_backoff: &mut ScannerCleanIdleBackoff,
    runtime_config: &ScannerRuntimeConfig,
    clean_idle_backoff_enabled: bool,
    wake_reason: ScannerCycleWakeReason,
    outcome: ScannerCycleOutcome,
    dirty_work_observed: bool,
) {
    clean_idle_backoff.record_cycle(
        runtime_config.cycle_interval,
        scanner_clean_idle_max_interval(runtime_config.cycle_interval, runtime_config),
        clean_idle_backoff_enabled,
        wake_reason,
        outcome,
        dirty_work_observed,
    );
}

fn scanner_clean_idle_backoff_configured(runtime_config: &ScannerRuntimeConfig) -> bool {
    let bitrot_cycle_allows_backoff =
        runtime_config.bitrot_cycle.is_none() || runtime_config.bitrot_cycle_source == ScannerRuntimeConfigSource::Default;
    runtime_config.cycle_interval_source == ScannerRuntimeConfigSource::Default && bitrot_cycle_allows_backoff
}

fn scanner_clean_idle_max_interval(base_interval: Duration, runtime_config: &ScannerRuntimeConfig) -> Duration {
    let policy_max = CLEAN_IDLE_MAX_INTERVAL.max(base_interval);
    let Some(bitrot_cycle) = runtime_config.bitrot_cycle else {
        return policy_max;
    };
    if runtime_config.bitrot_cycle_source != ScannerRuntimeConfigSource::Default {
        return policy_max;
    }

    let selection_window = heal_object_select_prob();
    if selection_window == 0 {
        return policy_max;
    }

    bitrot_cycle
        .checked_div(selection_window)
        .unwrap_or(base_interval)
        .max(base_interval)
        .min(policy_max)
}

fn scanner_clean_idle_backoff_enabled(
    topology_supported: bool,
    cluster_activity_ready: bool,
    features: ScannerMaintenanceFeatures,
    runtime_config: &ScannerRuntimeConfig,
) -> bool {
    topology_supported
        && cluster_activity_ready
        && !features.needs_regular_cycle()
        && scanner_clean_idle_backoff_configured(runtime_config)
}

fn scanner_activity_probe_required(
    topology_supported: bool,
    backoff_blocked: bool,
    features: ScannerMaintenanceFeatures,
    runtime_config: &ScannerRuntimeConfig,
) -> bool {
    topology_supported
        && !backoff_blocked
        && !features.needs_regular_cycle()
        && scanner_clean_idle_backoff_configured(runtime_config)
}

fn scanner_activity_observed_work(observation: ScannerActivityObservation) -> bool {
    matches!(
        observation,
        ScannerActivityObservation::Changed
            | ScannerActivityObservation::MaintenanceChanged
            | ScannerActivityObservation::Unverified
    )
}

fn scanner_activity_backoff_blocked_after_wake(currently_blocked: bool, wake_reason: ScannerCycleWakeReason) -> bool {
    match wake_reason {
        ScannerCycleWakeReason::ClusterMaintenance => true,
        ScannerCycleWakeReason::MaintenanceConfig => false,
        _ => currently_blocked,
    }
}

async fn wait_for_next_scanner_cycle<F>(
    ctx: &CancellationToken,
    delay: Duration,
    dirty_usage_generation_seen: u64,
    runtime_config_generation: u64,
    maintenance_generation: u64,
    is_lock_lost: F,
) -> ScannerCycleWakeReason
where
    F: Fn() -> bool,
{
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    let lock_poll = tokio::time::sleep(SCANNER_LEADER_LOCK_POLL_INTERVAL);
    tokio::pin!(lock_poll);

    loop {
        if is_lock_lost() {
            return ScannerCycleWakeReason::LeaderLockLost;
        }
        if scanner_runtime_config_generation() != runtime_config_generation {
            return ScannerCycleWakeReason::RuntimeConfig;
        }
        if scanner_maintenance_generation() != maintenance_generation {
            return ScannerCycleWakeReason::MaintenanceConfig;
        }
        if dirty_usage_buckets_pending() && dirty_usage_generation() != dirty_usage_generation_seen {
            return ScannerCycleWakeReason::DirtyUsage;
        }

        tokio::select! {
            _ = ctx.cancelled() => return ScannerCycleWakeReason::Cancelled,
            _ = &mut sleep => return ScannerCycleWakeReason::Timer,
            _ = &mut lock_poll => {
                if is_lock_lost() {
                    return ScannerCycleWakeReason::LeaderLockLost;
                }
                lock_poll.as_mut().reset(Instant::now() + SCANNER_LEADER_LOCK_POLL_INTERVAL);
            }
            _ = dirty_usage_bucket_notified() => {
                if scanner_runtime_config_generation() != runtime_config_generation {
                    return ScannerCycleWakeReason::RuntimeConfig;
                }
                if scanner_maintenance_generation() != maintenance_generation {
                    return ScannerCycleWakeReason::MaintenanceConfig;
                }
                if dirty_usage_buckets_pending() && dirty_usage_generation() != dirty_usage_generation_seen {
                    return ScannerCycleWakeReason::DirtyUsage;
                }
            }
            _ = scanner_runtime_config_changed() => {
                if scanner_runtime_config_generation() != runtime_config_generation {
                    return ScannerCycleWakeReason::RuntimeConfig;
                }
            }
            _ = scanner_maintenance_changed() => {
                if scanner_maintenance_generation() != maintenance_generation {
                    return ScannerCycleWakeReason::MaintenanceConfig;
                }
            }
        }
    }
}

async fn wait_for_next_scanner_cycle_with_activity<F, Probe, ProbeFuture>(
    ctx: &CancellationToken,
    delay: Duration,
    activity_poll_interval: Option<Duration>,
    activity_seen: &mut Option<ScannerActivitySnapshot>,
    generations: ScannerCycleObservedGenerations,
    is_lock_lost: F,
    mut probe_activity: Probe,
) -> ScannerCycleWakeReason
where
    F: Fn() -> bool,
    Probe: FnMut() -> ProbeFuture,
    ProbeFuture: Future<Output = Result<ScannerActivitySnapshot, String>>,
{
    let deadline = Instant::now() + delay;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return ScannerCycleWakeReason::Timer;
        }
        let wait_slice = activity_poll_interval
            .map(|interval| interval.max(Duration::from_secs(1)).min(remaining))
            .unwrap_or(remaining);
        let wake_reason = wait_for_next_scanner_cycle(
            ctx,
            wait_slice,
            generations.dirty_usage,
            generations.runtime_config,
            generations.maintenance,
            &is_lock_lost,
        )
        .await;
        if wake_reason != ScannerCycleWakeReason::Timer || Instant::now() >= deadline {
            return wake_reason;
        }

        let Some(_) = activity_poll_interval else {
            return ScannerCycleWakeReason::Timer;
        };
        if is_lock_lost() {
            return ScannerCycleWakeReason::LeaderLockLost;
        }

        let probe = probe_activity();
        tokio::pin!(probe);
        let lock_lost = async {
            loop {
                tokio::time::sleep(SCANNER_LEADER_LOCK_POLL_INTERVAL).await;
                if is_lock_lost() {
                    break;
                }
            }
        };
        tokio::pin!(lock_lost);
        let probe_result = tokio::select! {
            result = &mut probe => result,
            _ = ctx.cancelled() => return ScannerCycleWakeReason::Cancelled,
            _ = &mut lock_lost => return ScannerCycleWakeReason::LeaderLockLost,
        };

        let had_baseline = activity_seen.is_some();
        let (observation, probe_error) = apply_scanner_activity_probe_result(activity_seen, probe_result);
        if let Some(err) = probe_error {
            log_scanner_activity_probe_error(had_baseline, &err);
        }
        match observation {
            ScannerActivityObservation::Unchanged | ScannerActivityObservation::NotRequired => {}
            ScannerActivityObservation::Changed => return ScannerCycleWakeReason::ClusterActivity,
            ScannerActivityObservation::MaintenanceChanged => return ScannerCycleWakeReason::ClusterMaintenance,
            ScannerActivityObservation::Unverified => return ScannerCycleWakeReason::ClusterActivityUnavailable,
        }
    }
}

fn log_scanner_activity_probe_error(had_baseline: bool, err: &str) {
    if had_baseline {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "cluster_activity_probe_failed",
            error = %err,
            "Scanner cluster activity probe failed; preserving the base cycle"
        );
    } else {
        debug!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "cluster_activity_probe_unavailable",
            error = %err,
            "Scanner cluster activity probe remains unavailable"
        );
    }
}

fn compare_scanner_activity(previous: &ScannerActivitySnapshot, current: &ScannerActivitySnapshot) -> ScannerActivityObservation {
    if previous == current {
        return ScannerActivityObservation::Unchanged;
    }

    for (host, current_activity) in current {
        let Some(previous_activity) = previous.get(host) else {
            continue;
        };
        if host != LOCAL_SCANNER_ACTIVITY_NODE
            && previous_activity.instance_id == current_activity.instance_id
            && previous_activity.maintenance_generation != current_activity.maintenance_generation
        {
            return ScannerActivityObservation::MaintenanceChanged;
        }
    }

    ScannerActivityObservation::Changed
}

fn apply_scanner_activity_probe_result(
    activity_seen: &mut Option<ScannerActivitySnapshot>,
    result: Result<ScannerActivitySnapshot, String>,
) -> (ScannerActivityObservation, Option<String>) {
    match result {
        Ok(current) => {
            let observation = match activity_seen.as_ref() {
                Some(previous) => compare_scanner_activity(previous, &current),
                None => ScannerActivityObservation::Unverified,
            };
            *activity_seen = Some(current);
            (observation, None)
        }
        Err(err) => {
            *activity_seen = None;
            (ScannerActivityObservation::Unverified, Some(err))
        }
    }
}

async fn observe_scanner_activity(
    storeapi: &Arc<ECStore>,
    distributed: bool,
    activity_seen: &mut Option<ScannerActivitySnapshot>,
) -> ScannerActivityObservation {
    let had_baseline = activity_seen.is_some();
    let (observation, probe_error) =
        apply_scanner_activity_probe_result(activity_seen, probe_scanner_activity(storeapi, distributed).await);
    if let Some(err) = probe_error {
        log_scanner_activity_probe_error(had_baseline, &err);
    }
    observation
}

pub(crate) fn scanner_activity_snapshot_digest(snapshot: &ScannerActivitySnapshot) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(u64::try_from(snapshot.len()).unwrap_or(u64::MAX).to_be_bytes());
    for (host, activity) in snapshot {
        let host = host.as_bytes();
        let instance_id = activity.instance_id.as_bytes();
        hasher.update(u64::try_from(host.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(host);
        hasher.update(u64::try_from(instance_id.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(instance_id);
        hasher.update(activity.namespace_generation.to_be_bytes());
        hasher.update(activity.maintenance_generation.to_be_bytes());
        hasher.update(activity.protocol_version.to_be_bytes());
        hasher.update(activity.topology_digest);
        hasher.update([u8::from(activity.data_movement_active)]);
    }
    hasher.finalize().into()
}

pub(crate) fn scanner_activity_allows_usage_publication(snapshot: &ScannerActivitySnapshot) -> bool {
    snapshot.values().all(|activity| !activity.data_movement_active)
}

pub fn scanner_topology_digest(storeapi: &ECStore) -> [u8; 32] {
    let endpoint_pools = storeapi.endpoints();
    let mut hasher = Sha256::new();
    hasher.update(u64::try_from(endpoint_pools.0.len()).unwrap_or(u64::MAX).to_be_bytes());
    for (pool_index, pool) in endpoint_pools.0.iter().enumerate() {
        hasher.update(u64::try_from(pool_index).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(u64::try_from(pool.set_count).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(u64::try_from(pool.drives_per_set).unwrap_or(u64::MAX).to_be_bytes());
        let mut endpoints = pool.endpoints.as_ref().iter().collect::<Vec<_>>();
        endpoints.sort_unstable_by(|left, right| {
            (left.pool_idx, left.set_idx, left.disk_idx, left.url.as_str()).cmp(&(
                right.pool_idx,
                right.set_idx,
                right.disk_idx,
                right.url.as_str(),
            ))
        });
        hasher.update(u64::try_from(endpoints.len()).unwrap_or(u64::MAX).to_be_bytes());
        for endpoint in endpoints {
            hasher.update(endpoint.pool_idx.to_be_bytes());
            hasher.update(endpoint.set_idx.to_be_bytes());
            hasher.update(endpoint.disk_idx.to_be_bytes());
            let url = endpoint.url.as_str().as_bytes();
            hasher.update(u64::try_from(url.len()).unwrap_or(u64::MAX).to_be_bytes());
            hasher.update(url);
        }
    }
    hasher.finalize().into()
}

pub(crate) async fn probe_scanner_activity(storeapi: &ECStore, distributed: bool) -> Result<ScannerActivitySnapshot, String> {
    let topology_digest = scanner_topology_digest(storeapi);
    let data_movement_active = storeapi.scanner_data_movement_active().await;
    let namespace_generation = storeapi.scanner_namespace_mutation_generation();
    let maintenance_generation = scanner_maintenance_generation();
    if namespace_generation == u64::MAX || maintenance_generation == u64::MAX {
        return Err("local scanner activity generation is exhausted".to_string());
    }
    let mut snapshot = ScannerActivitySnapshot::from([(
        LOCAL_SCANNER_ACTIVITY_NODE.to_string(),
        ScannerNodeActivity {
            instance_id: crate::scanner_io::scanner_activity_epoch().to_string(),
            namespace_generation,
            maintenance_generation,
            protocol_version: SCANNER_ACTIVITY_PROTOCOL_VERSION,
            topology_digest,
            data_movement_active,
        },
    )]);
    if !distributed {
        return Ok(snapshot);
    }

    let notification_system = storeapi
        .notification_system()
        .ok_or_else(|| "notification system is not initialized".to_string())?;
    let peers = notification_system
        .scanner_activity_snapshots()
        .await
        .map_err(|err| err.to_string())?;
    for (host, activity) in peers {
        if activity.namespace_generation == u64::MAX || activity.maintenance_generation == u64::MAX {
            return Err(format!("scanner activity peer {host} exhausted its activity generation"));
        }
        let (peer_topology_digest, peer_data_movement_active) = match activity.protocol_version {
            SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION => {
                return Err(format!("scanner activity peer {host} cannot verify data movement publication fencing"));
            }
            SCANNER_ACTIVITY_PROTOCOL_VERSION => (
                activity
                    .topology_digest
                    .ok_or_else(|| format!("scanner activity peer {host} omitted its storage topology"))?,
                activity
                    .data_movement_active
                    .ok_or_else(|| format!("scanner activity peer {host} omitted its data movement state"))?,
            ),
            version => {
                return Err(format!(
                    "scanner activity peer {host} uses protocol {version}, expected {}",
                    SCANNER_ACTIVITY_PROTOCOL_VERSION
                ));
            }
        };
        if peer_topology_digest != topology_digest {
            return Err(format!("scanner activity peer {host} has a different storage topology"));
        }
        if snapshot
            .insert(
                host.clone(),
                ScannerNodeActivity {
                    instance_id: activity.instance_id,
                    namespace_generation: activity.namespace_generation,
                    maintenance_generation: activity.maintenance_generation,
                    protocol_version: activity.protocol_version,
                    topology_digest: peer_topology_digest,
                    data_movement_active: peer_data_movement_active,
                },
            )
            .is_some()
        {
            return Err(format!("duplicate scanner activity peer: {host}"));
        }
    }
    Ok(snapshot)
}

fn initial_scanner_delay_for(start_delay_secs: Option<u64>) -> Duration {
    start_delay_secs
        .map(|secs| randomized_cycle_delay_for(Duration::from_secs(secs)))
        .unwrap_or_else(randomized_cycle_delay)
}

fn initial_scanner_delay_for_startup(
    start_delay_secs: Option<u64>,
    usage_cache_is_cold: bool,
    has_buckets: bool,
    has_active_replication: bool,
) -> Duration {
    // Skip the startup delay when the cache is cold (first ever scan) OR when active replication
    // rules exist. A cold usage cache also covers startup-before-bucket-creation: running the
    // first cycle promptly keeps later bucket metrics bounded by the normal scanner cycle instead
    // of an extra startup delay. Replication config is live-read at startup by
    // configure_scanner_defaults, so this signal is always current regardless of when the persisted
    // DataUsageInfo was last written.
    if usage_cache_is_cold || (has_active_replication && has_buckets) {
        Duration::ZERO
    } else {
        initial_scanner_delay_for(start_delay_secs)
    }
}

fn data_usage_info_is_cold(info: &DataUsageInfo) -> bool {
    info.last_update.is_none() || (info.buckets_usage.is_empty() && info.bucket_sizes.is_empty())
}

async fn read_data_usage_config_for_startup(storeapi: &Arc<ECStore>) -> Result<Option<Vec<u8>>, EcstoreError> {
    match read_config(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await {
        Ok(data) => Ok(Some(data)),
        Err(EcstoreError::ConfigNotFound) => {
            let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
            match read_config(storeapi.clone(), backup_path.as_str()).await {
                Ok(data) => Ok(Some(data)),
                Err(EcstoreError::ConfigNotFound) => Ok(None),
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    }
}

async fn persisted_usage_cache_is_cold_for_startup(storeapi: &Arc<ECStore>) -> bool {
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
        Ok(info) => data_usage_info_is_cold(&info),
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

async fn initial_scanner_startup_usage_state(storeapi: &Arc<ECStore>) -> (bool, bool) {
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

    (persisted_usage_cache_is_cold_for_startup(storeapi).await, has_buckets)
}

pub async fn init_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) {
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

    let replication_active = startup_features.replication;
    let ctx_clone = ctx;
    let storeapi_clone = storeapi;
    tokio::spawn(async move {
        let (usage_cache_is_cold, has_buckets) = initial_scanner_startup_usage_state(&storeapi_clone).await;
        let sleep_time = initial_scanner_delay_for_startup(
            scanner_start_delay().map(|duration| duration.as_secs()),
            usage_cache_is_cold,
            has_buckets,
            replication_active,
        );
        if sleep_time.is_zero() {
            let skip_reason = if usage_cache_is_cold {
                "usage_cache_cold"
            } else {
                "replication_active"
            };
            info!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_CYCLE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "startup_delay_skipped",
                reason = skip_reason,
                "Scanner startup delay skipped"
            );
        } else {
            tokio::time::sleep(sleep_time).await;
        }

        loop {
            if ctx_clone.is_cancelled() {
                break;
            }

            if let Err(e) = run_data_scanner_with_maintenance_state(
                ctx_clone.clone(),
                storeapi_clone.clone(),
                startup_features,
                startup_maintenance_generation,
            )
            .await
            {
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
            // Backoff before retrying after lock contention or scanner-level failures.
            // Keep this cancellation-aware so shutdown is not delayed by backoff sleep.
            tokio::select! {
                _ = ctx_clone.cancelled() => break,
                _ = tokio::time::sleep(randomized_cycle_delay()) => {}
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

fn single_disk_default_cycle_secs(_features: ScannerMaintenanceFeatures) -> Option<u64> {
    None
}

fn single_disk_default_speed() -> ScannerSpeed {
    ScannerSpeed::Default
}

async fn detect_scanner_maintenance_features(storeapi: &Arc<ECStore>) -> ScannerMaintenanceFeatures {
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

        if features.needs_regular_cycle() {
            break;
        }
    }

    features
}

async fn detect_stable_scanner_maintenance_features(
    ctx: &CancellationToken,
    storeapi: &Arc<ECStore>,
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
    storeapi: &Arc<ECStore>,
) -> (ScannerMaintenanceFeatures, Option<u64>) {
    if storeapi.setup_is_erasure_sd().await {
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
        let default_cycle_secs = single_disk_default_cycle_secs(features);
        set_scanner_default_speed(single_disk_default_speed());
        set_scanner_default_cycle_secs(default_cycle_secs);
        info!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_RUNTIME_CONFIG,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            env_speed = ENV_SCANNER_SPEED,
            env_cycle = ENV_SCANNER_CYCLE,
            env_start_delay = ENV_SCANNER_START_DELAY_SECS,
            ?default_cycle_secs,
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
        (ScannerMaintenanceFeatures::default(), None)
    }
}

#[cfg(test)]
fn bitrot_scan_cycle() -> Option<Duration> {
    resolve_scanner_runtime_config().bitrot_cycle
}

fn get_cycle_scan_mode(
    current_cycle: u64,
    bitrot_start_cycle: u64,
    bitrot_start_time: Option<DateTime<Utc>>,
    bitrot_cycle: Option<Duration>,
) -> HealScanMode {
    let Some(bitrot_cycle) = bitrot_cycle else {
        return HealScanMode::Normal;
    };

    if bitrot_cycle.is_zero() {
        return HealScanMode::Deep;
    }

    if current_cycle.saturating_sub(bitrot_start_cycle) < heal_object_select_prob() as u64 {
        return HealScanMode::Deep;
    }

    let Some(bitrot_start_time) = bitrot_start_time else {
        return HealScanMode::Deep;
    };

    let elapsed = Utc::now()
        .signed_duration_since(bitrot_start_time)
        .to_std()
        .unwrap_or(Duration::ZERO);
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
    bitrot_cycle: Option<Duration>,
) -> Option<BackgroundHealInfo> {
    let reset_bitrot_start =
        scan_mode == HealScanMode::Deep && should_reset_bitrot_start(&info, current_cycle, now, bitrot_cycle);
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
    bitrot_cycle: Option<Duration>,
) -> bool {
    let Some(bitrot_start_time) = info.bitrot_start_time else {
        return true;
    };

    let Some(bitrot_cycle) = bitrot_cycle else {
        return false;
    };

    if bitrot_cycle.is_zero() {
        return true;
    }

    if current_cycle.saturating_sub(info.bitrot_start_cycle) < heal_object_select_prob() as u64 {
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

/// Background healing information
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundHealInfo {
    /// Bitrot scan start time
    pub bitrot_start_time: Option<DateTime<Utc>>,
    /// Bitrot scan start cycle
    pub bitrot_start_cycle: u64,
    /// Current scan mode
    pub current_scan_mode: HealScanMode,
}

/// Read background healing information from storage
pub async fn read_background_heal_info(storeapi: Arc<ECStore>) -> BackgroundHealInfo {
    // Skip for ErasureSD setup
    if scanner_is_erasure_sd().await {
        return BackgroundHealInfo::default();
    }

    // Get last healing information
    match read_config(storeapi, &BACKGROUND_HEAL_INFO_PATH).await {
        Ok(buf) => serde_json::from_slice::<BackgroundHealInfo>(&buf).unwrap_or_else(|e| {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_BACKGROUND_HEAL_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_BACKGROUND_HEAL,
                path = %&*BACKGROUND_HEAL_INFO_PATH,
                state = "decode_failed",
                error = %e,
                "Scanner background heal decode failed"
            );
            BackgroundHealInfo::default()
        }),
        Err(e) => {
            // Only log if it's not a ConfigNotFound error
            if e != EcstoreError::ConfigNotFound {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_BACKGROUND_HEAL_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_BACKGROUND_HEAL,
                    path = %&*BACKGROUND_HEAL_INFO_PATH,
                    state = "read_failed",
                    error = %e,
                    "Scanner background heal read failed"
                );
            }
            BackgroundHealInfo::default()
        }
    }
}

/// Save background healing information to storage
#[instrument(skip(storeapi))]
pub async fn save_background_heal_info(storeapi: Arc<ECStore>, info: BackgroundHealInfo) {
    // Skip for ErasureSD setup
    if scanner_is_erasure_sd().await {
        return;
    }

    // Serialize to JSON
    let data = match serde_json::to_vec(&info) {
        Ok(data) => data,
        Err(e) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_BACKGROUND_HEAL_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_BACKGROUND_HEAL,
                path = %&*BACKGROUND_HEAL_INFO_PATH,
                state = "encode_failed",
                error = %e,
                "Scanner background heal encode failed"
            );
            return;
        }
    };

    // Save configuration
    if let Err(e) = save_config(storeapi, &BACKGROUND_HEAL_INFO_PATH, data).await {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_BACKGROUND_HEAL_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_BACKGROUND_HEAL,
            path = %&*BACKGROUND_HEAL_INFO_PATH,
            state = "save_failed",
            error = %e,
            "Scanner background heal save failed"
        );
    }
}

/// Get lock acquire timeout from environment variable RUSTFS_LOCK_ACQUIRE_TIMEOUT (in seconds)
/// Defaults to 5 seconds if not set or invalid
/// For distributed environments with multiple nodes, a longer timeout may be needed
fn get_lock_acquire_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64("RUSTFS_LOCK_ACQUIRE_TIMEOUT", 5))
}

fn data_usage_persist_timeout() -> Duration {
    DataUsageCache::persistence_timeout()
}

async fn mark_scan_cycle_idle(cycle_info: &mut CurrentCycle) {
    cycle_info.current = 0;
    global_metrics().clear_current_scan_mode();
    global_metrics().set_cycle(Some(cycle_info.clone())).await;
}

fn encode_scanner_cycle_state(cycle_info: &CurrentCycle, leader_epoch: u64) -> Result<Vec<u8>, ScannerCycleStateError> {
    if cycle_info.next == u64::MAX {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle counter is exhausted"));
    }
    let cycle_info_buf = rmp_serde::to_vec(cycle_info)?;
    let mut buf = Vec::with_capacity(cycle_info_buf.len() + SCANNER_CYCLE_STATE_HEADER_LEN);
    buf.extend_from_slice(&cycle_info.next.to_le_bytes());
    buf.extend_from_slice(SCANNER_CYCLE_STATE_MAGIC);
    buf.extend_from_slice(&leader_epoch.to_le_bytes());
    buf.extend_from_slice(&cycle_info_buf);
    Ok(buf)
}

fn decode_scanner_cycle_state(buf: &[u8]) -> Result<(CurrentCycle, u64), ScannerCycleStateError> {
    if buf.len() < 8 {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle state is truncated"));
    }

    let persisted_next = u64::from_le_bytes(
        buf[0..8]
            .try_into()
            .map_err(|_| ScannerCycleStateError::InvalidData("scanner cycle counter is truncated"))?,
    );
    if persisted_next == u64::MAX {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle counter is exhausted"));
    }
    if buf.len() == 8 {
        return Ok((
            CurrentCycle {
                next: persisted_next,
                ..Default::default()
            },
            0,
        ));
    }

    let (leader_epoch, payload) = if buf.len() >= 16 && &buf[8..16] == SCANNER_CYCLE_STATE_MAGIC {
        if buf.len() < SCANNER_CYCLE_STATE_HEADER_LEN {
            return Err(ScannerCycleStateError::InvalidData("scanner cycle fencing header is truncated"));
        }
        let epoch = u64::from_le_bytes(
            buf[16..24]
                .try_into()
                .map_err(|_| ScannerCycleStateError::InvalidData("scanner leader epoch is truncated"))?,
        );
        if epoch == 0 {
            return Err(ScannerCycleStateError::InvalidData("scanner leader epoch is zero"));
        }
        (epoch, &buf[SCANNER_CYCLE_STATE_HEADER_LEN..])
    } else {
        (0, &buf[8..])
    };

    let cycle_info = rmp_serde::from_slice::<CurrentCycle>(payload)?;
    if cycle_info.next != persisted_next {
        return Err(ScannerCycleStateError::InvalidData("scanner cycle counter disagrees with encoded state"));
    }
    Ok((cycle_info, leader_epoch))
}

pub(crate) fn decode_persisted_scanner_cycle_fence(buf: &[u8]) -> Result<(u64, u64), ScannerError> {
    decode_scanner_cycle_state(buf)
        .map(|(cycle, leader_epoch)| (cycle.next, leader_epoch))
        .map_err(|err| ScannerError::Other(format!("persisted scanner cycle state is invalid: {err}")))
}

#[cfg(test)]
pub(crate) fn encode_scanner_cycle_fence_for_test(next_cycle: u64, leader_epoch: u64) -> Vec<u8> {
    encode_scanner_cycle_state(
        &CurrentCycle {
            next: next_cycle,
            ..Default::default()
        },
        leader_epoch,
    )
    .expect("test scanner cycle fence should encode")
}

pub(crate) async fn current_scanner_leader_epoch() -> Result<u64, ScannerError> {
    let store = crate::resolve_scanner_object_store_handle()
        .ok_or_else(|| ScannerError::Other("scanner object layer is unavailable".to_string()))?;
    match read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await {
        Ok(buf) => {
            let (_, leader_epoch) = decode_persisted_scanner_cycle_fence(&buf)?;
            if leader_epoch == 0 {
                return Err(ScannerError::Other("persisted scanner cycle state has no leader epoch".to_string()));
            }
            Ok(leader_epoch)
        }
        Err(err) => Err(ScannerError::Other(format!("failed to read persisted scanner leader epoch: {err}"))),
    }
}

fn decode_scanner_cycle_state_for_startup(buf: &[u8]) -> Result<(CurrentCycle, u64), ScannerCycleStateError> {
    if buf.is_empty() {
        Ok((CurrentCycle::default(), 0))
    } else {
        decode_scanner_cycle_state(buf)
    }
}

fn advance_scanner_cycle(cycle_info: &mut CurrentCycle) -> Result<(), ScannerCycleStateError> {
    let next = cycle_info
        .next
        .checked_add(1)
        .filter(|next| *next < u64::MAX)
        .ok_or(ScannerCycleStateError::InvalidData("scanner cycle counter is exhausted"))?;
    cycle_info.next = next;
    Ok(())
}

async fn persisted_usage_floor(storeapi: Arc<impl ScannerObjectIO>) -> Result<PersistedUsageFloor, ScannerError> {
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let mut floor = PersistedUsageFloor::default();
    for path in [DATA_USAGE_OBJ_NAME_PATH.as_str(), backup_path.as_str()] {
        let data = match read_config(storeapi.clone(), path).await {
            Ok(data) => data,
            Err(EcstoreError::ConfigNotFound) => continue,
            Err(err) => {
                return Err(ScannerError::Other(format!(
                    "failed to read scanner usage epoch floor from {path}: {err}"
                )));
            }
        };
        let usage = serde_json::from_slice::<DataUsageInfo>(&data)
            .map_err(|err| ScannerError::Other(format!("failed to decode scanner usage floor from {path}: {err}")))?;
        floor.leader_epoch = floor.leader_epoch.max(usage.scanner_epoch.unwrap_or_default());
        if let Some(completed_cycle) = usage.scanner_cycle {
            let next_cycle = completed_cycle
                .checked_add(1)
                .filter(|next| *next < u64::MAX)
                .ok_or_else(|| ScannerError::Other(format!("persisted scanner usage cycle is exhausted in {path}")))?;
            floor.next_cycle = floor.next_cycle.max(next_cycle);
        }
    }
    Ok(floor)
}

fn apply_persisted_usage_floor(cycle_info: &mut CurrentCycle, leader_epoch: &mut u64, floor: PersistedUsageFloor) {
    cycle_info.next = cycle_info.next.max(floor.next_cycle);
    *leader_epoch = (*leader_epoch).max(floor.leader_epoch);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerLeadershipClaimReconcile {
    Durable,
    Changed,
    Unchanged,
}

async fn reconcile_scanner_leadership_claim(
    storeapi: Arc<impl ScannerObjectIO>,
    attempted: &[u8],
    previous_revision: &DataUsageCacheRevision,
    claimed_epoch: u64,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    persisted_epoch: &mut u64,
) -> Result<ScannerLeadershipClaimReconcile, ScannerError> {
    let (persisted, persisted_revision) = read_config_with_revision(storeapi, DATA_USAGE_BLOOM_NAME_PATH.as_str())
        .await
        .map_err(|err| ScannerError::Other(format!("failed to reconcile scanner leadership claim: {err}")))?;
    let revision_changed = &persisted_revision != previous_revision;
    *revision = persisted_revision;

    let Some(persisted) = persisted else {
        *cycle_info = CurrentCycle::default();
        return Ok(if revision_changed {
            ScannerLeadershipClaimReconcile::Changed
        } else {
            ScannerLeadershipClaimReconcile::Unchanged
        });
    };
    if persisted == attempted {
        *persisted_epoch = claimed_epoch;
        return Ok(ScannerLeadershipClaimReconcile::Durable);
    }

    let (current, epoch) = decode_scanner_cycle_state(&persisted)
        .map_err(|err| ScannerError::Other(format!("scanner leadership conflict winner is invalid: {err}")))?;
    *cycle_info = current;
    *persisted_epoch = (*persisted_epoch).max(epoch);
    Ok(if revision_changed {
        ScannerLeadershipClaimReconcile::Changed
    } else {
        ScannerLeadershipClaimReconcile::Unchanged
    })
}

fn decode_usage_snapshot_for_epoch_fence(data: &[u8], path: &str) -> Result<DataUsageInfo, ScannerError> {
    serde_json::from_slice(data)
        .map_err(|err| ScannerError::Other(format!("failed to decode scanner usage epoch fence from {path}: {err}")))
}

async fn usage_snapshot_for_epoch_fence(
    storeapi: Arc<impl ScannerObjectIO>,
    primary: Option<&[u8]>,
) -> Result<DataUsageInfo, ScannerError> {
    if let Some(primary) = primary {
        return decode_usage_snapshot_for_epoch_fence(primary, DATA_USAGE_OBJ_NAME_PATH.as_str());
    }

    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    let (backup, _) = read_config_with_revision(storeapi, &backup_path)
        .await
        .map_err(|err| ScannerError::Other(format!("failed to read scanner usage epoch fence backup: {err}")))?;
    backup
        .as_deref()
        .map(|data| decode_usage_snapshot_for_epoch_fence(data, &backup_path))
        .transpose()
        .map(Option::unwrap_or_default)
}

async fn fence_scanner_usage_epoch(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    claimed_epoch: u64,
) -> Result<(), ScannerError> {
    for retry in 0..=SCANNER_PERSIST_CAS_RETRIES {
        if ctx.is_cancelled() {
            return Err(ScannerError::Other("scanner leadership was cancelled before usage fencing".to_string()));
        }

        let (primary, revision) = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .map_err(|err| ScannerError::Other(format!("failed to read scanner usage epoch fence: {err}")))?;
        let mut usage = usage_snapshot_for_epoch_fence(storeapi.clone(), primary.as_deref()).await?;
        match usage.scanner_epoch {
            Some(epoch) if epoch > claimed_epoch => {
                return Err(ScannerError::Other(format!(
                    "scanner usage epoch fence lost to newer leader: claimed={claimed_epoch}, persisted={epoch}"
                )));
            }
            Some(epoch) if epoch == claimed_epoch => return Ok(()),
            Some(_) | None => {}
        }
        usage.scanner_epoch = Some(claimed_epoch);
        let data = serde_json::to_vec(&usage)
            .map_err(|err| ScannerError::Other(format!("failed to encode scanner usage epoch fence: {err}")))?;

        let save_result =
            save_config_with_preconditions(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), data, revision.preconditions())
                .await;
        if save_result
            .as_ref()
            .ok()
            .and_then(|object_info| object_info.etag.as_deref())
            .is_some_and(|etag| !etag.is_empty())
        {
            return Ok(());
        }

        let (persisted, persisted_revision) = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .map_err(|err| ScannerError::Other(format!("failed to reconcile scanner usage epoch fence: {err}")))?;
        if let Some(persisted) = persisted {
            let persisted = decode_usage_snapshot_for_epoch_fence(&persisted, DATA_USAGE_OBJ_NAME_PATH.as_str())?;
            match persisted.scanner_epoch {
                Some(epoch) if epoch == claimed_epoch => return Ok(()),
                Some(epoch) if epoch > claimed_epoch => {
                    return Err(ScannerError::Other(format!(
                        "scanner usage epoch fence lost to newer leader: claimed={claimed_epoch}, persisted={epoch}"
                    )));
                }
                Some(_) | None => {}
            }
        }

        let precondition_failed = matches!(save_result, Err(EcstoreError::PreconditionFailed));
        if retry < SCANNER_PERSIST_CAS_RETRIES && (precondition_failed || persisted_revision != revision) {
            continue;
        }
        return Err(ScannerError::Other(match save_result {
            Ok(_) => "scanner usage epoch fence returned no ETag and could not be confirmed".to_string(),
            Err(err) => format!("scanner usage epoch fence save failed: {err}"),
        }));
    }

    Err(ScannerError::Other("scanner usage epoch fence retries exhausted".to_string()))
}

async fn complete_scanner_leadership_claim(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    claimed_epoch: u64,
) -> bool {
    if let Err(err) = fence_scanner_usage_epoch(ctx, storeapi, claimed_epoch).await {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
            state = "usage_epoch_fence_failed",
            claimed_epoch,
            error = %err,
            "Scanner leadership usage epoch fencing failed"
        );
        return false;
    }
    !ctx.is_cancelled()
}

async fn claim_scanner_leadership(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    persisted_epoch: &mut u64,
) -> bool {
    for retry in 0..=SCANNER_PERSIST_CAS_RETRIES {
        if ctx.is_cancelled() {
            return false;
        }
        let Some(claimed_epoch) = persisted_epoch.checked_add(1) else {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "leader_epoch_exhausted",
                "Scanner leadership epoch is exhausted"
            );
            return false;
        };
        let data = match encode_scanner_cycle_state(cycle_info, claimed_epoch) {
            Ok(data) => data,
            Err(err) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "leader_claim_encode_failed",
                    error = %err,
                    "Scanner leadership claim encoding failed"
                );
                return false;
            }
        };
        let previous_revision = revision.clone();

        let save_result =
            save_config_with_preconditions(storeapi.clone(), &DATA_USAGE_BLOOM_NAME_PATH, data.clone(), revision.preconditions())
                .await;
        match save_result {
            Ok(object_info) => {
                if let Some(etag) = object_info.etag.filter(|etag| !etag.is_empty()) {
                    *revision = DataUsageCacheRevision::Etag(etag);
                    *persisted_epoch = claimed_epoch;
                    return complete_scanner_leadership_claim(ctx, storeapi, claimed_epoch).await;
                }

                match reconcile_scanner_leadership_claim(
                    storeapi.clone(),
                    &data,
                    &previous_revision,
                    claimed_epoch,
                    cycle_info,
                    revision,
                    persisted_epoch,
                )
                .await
                {
                    Ok(ScannerLeadershipClaimReconcile::Durable) => {
                        return complete_scanner_leadership_claim(ctx, storeapi, claimed_epoch).await;
                    }
                    Ok(ScannerLeadershipClaimReconcile::Changed) if retry < SCANNER_PERSIST_CAS_RETRIES => continue,
                    Ok(ScannerLeadershipClaimReconcile::Changed | ScannerLeadershipClaimReconcile::Unchanged) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "leader_claim_missing_revision",
                            "Scanner leadership claim returned no ETag and could not be confirmed"
                        );
                        return false;
                    }
                    Err(err) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "leader_claim_reconcile_failed",
                            error = %err,
                            "Scanner leadership claim read-back failed"
                        );
                        return false;
                    }
                }
            }
            Err(err) => {
                let precondition_failed = matches!(err, EcstoreError::PreconditionFailed);
                match reconcile_scanner_leadership_claim(
                    storeapi.clone(),
                    &data,
                    &previous_revision,
                    claimed_epoch,
                    cycle_info,
                    revision,
                    persisted_epoch,
                )
                .await
                {
                    Ok(ScannerLeadershipClaimReconcile::Durable) => {
                        return complete_scanner_leadership_claim(ctx, storeapi, claimed_epoch).await;
                    }
                    Ok(ScannerLeadershipClaimReconcile::Changed)
                        if retry < SCANNER_PERSIST_CAS_RETRIES && !ctx.is_cancelled() =>
                    {
                        continue;
                    }
                    Ok(ScannerLeadershipClaimReconcile::Unchanged)
                        if precondition_failed && retry < SCANNER_PERSIST_CAS_RETRIES && !ctx.is_cancelled() =>
                    {
                        continue;
                    }
                    Ok(ScannerLeadershipClaimReconcile::Changed | ScannerLeadershipClaimReconcile::Unchanged) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = if precondition_failed {
                                "leader_claim_conflicts_exhausted"
                            } else {
                                "leader_claim_failed"
                            },
                            error = %err,
                            "Scanner leadership claim failed"
                        );
                        return false;
                    }
                    Err(reconcile_err) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "leader_claim_reload_failed",
                            error = %reconcile_err,
                            save_error = %err,
                            "Scanner leadership claim reconciliation failed"
                        );
                        return false;
                    }
                }
            }
        }
    }

    false
}

async fn persist_scanner_cycle_state(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
) -> bool {
    let buf = match encode_scanner_cycle_state(cycle_info, leader_epoch) {
        Ok(buf) => buf,
        Err(e) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "encode_failed",
                error = %e,
                "Scanner state encoding failed"
            );
            return false;
        }
    };

    for retry in 0..=SCANNER_PERSIST_CAS_RETRIES {
        if ctx.is_cancelled() {
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "cancelled_before_save",
                retry,
                "Scanner state persistence cancelled by the leader fence"
            );
            return false;
        }

        match save_config_with_preconditions(storeapi.clone(), &DATA_USAGE_BLOOM_NAME_PATH, buf.clone(), revision.preconditions())
            .await
        {
            Ok(object_info) => {
                let Some(etag) = object_info.etag.filter(|etag| !etag.is_empty()) else {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "missing_revision",
                        "Scanner state save returned no ETag"
                    );
                    return false;
                };
                *revision = DataUsageCacheRevision::Etag(etag);
                if ctx.is_cancelled() {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "cancelled_after_save",
                        retry,
                        "Scanner state save completed after the leader fence was cancelled"
                    );
                    return false;
                }
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "saved",
                    "Scanner state saved"
                );
                return true;
            }
            Err(EcstoreError::PreconditionFailed) => {
                let (persisted, persisted_revision) =
                    match read_config_with_revision(storeapi.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str()).await {
                        Ok(result) => result,
                        Err(e) => {
                            error!(
                                target: "rustfs::scanner",
                                event = EVENT_SCANNER_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_RUNTIME,
                                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                                state = "conflict_reload_failed",
                                error = %e,
                                "Scanner state conflict reconciliation failed"
                            );
                            return false;
                        }
                    };
                *revision = persisted_revision;
                if ctx.is_cancelled() {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "cancelled_after_conflict",
                        retry,
                        "Scanner state conflict reconciliation cancelled by the leader fence"
                    );
                    return false;
                }

                if let Some(persisted) = persisted {
                    if persisted.len() < 8 {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "conflict_state_invalid",
                            length = persisted.len(),
                            "Scanner state conflict winner is truncated"
                        );
                        return false;
                    }

                    let (persisted_cycle, persisted_epoch) = match decode_scanner_cycle_state(&persisted) {
                        Ok(state) => state,
                        Err(e) => {
                            error!(
                                target: "rustfs::scanner",
                                event = EVENT_SCANNER_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_RUNTIME,
                                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                                state = "conflict_state_decode_failed",
                                error = %e,
                                "Scanner state conflict winner could not be decoded"
                            );
                            return false;
                        }
                    };
                    if persisted_epoch != leader_epoch {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "leader_epoch_fenced",
                            expected_epoch = leader_epoch,
                            persisted_epoch,
                            "Scanner state save rejected by a newer leadership epoch"
                        );
                        return false;
                    }

                    if persisted_cycle.next >= cycle_info.next {
                        *cycle_info = persisted_cycle;
                        global_metrics().set_cycle(Some(cycle_info.clone())).await;
                        debug!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                            state = "conflict_reconciled",
                            retry,
                            "Scanner state adopted the current persisted cycle"
                        );
                        return true;
                    }
                }

                if retry < SCANNER_PERSIST_CAS_RETRIES {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                        state = "conflict_retry",
                        retry = retry + 1,
                        "Scanner state CAS conflict will be retried"
                    );
                    continue;
                }

                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "conflict_retries_exhausted",
                    retries = SCANNER_PERSIST_CAS_RETRIES,
                    "Scanner state CAS conflict retries exhausted"
                );
                return false;
            }
            Err(e) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                    state = "failed",
                    error = %e,
                    "Scanner state persistence failed"
                );
                return false;
            }
        }
    }

    false
}

async fn finalize_partial_scan_cycle(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
) -> bool {
    // A budget-limited cycle is deliberate pacing, not a failure. The cycle counter
    // must still advance (and persist) because per-bucket next_cycle is stamped from
    // it and compacted folders are only rescanned when their hash matches
    // next_cycle % DATA_USAGE_UPDATE_DIR_CYCLES; a pinned counter starves lifecycle
    // expiry and usage refresh on every folder outside the stuck window.
    if let Err(err) = advance_scanner_cycle(cycle_info) {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "cycle_counter_exhausted",
            error = %err,
            "Scanner partial cycle could not advance"
        );
        mark_scan_cycle_idle(cycle_info).await;
        return false;
    }
    mark_scan_cycle_idle(cycle_info).await;
    persist_scanner_cycle_state(ctx, storeapi, cycle_info, revision, leader_epoch).await
}

async fn persist_required_scanner_cycle_floor(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    cycle_info: &mut CurrentCycle,
    revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    required_cycle: u64,
) -> bool {
    if required_cycle <= cycle_info.current || required_cycle == u64::MAX {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            current_cycle = cycle_info.current,
            required_cycle,
            state = "invalid_cache_cycle_floor",
            "Scanner cache cycle floor is invalid"
        );
        mark_scan_cycle_idle(cycle_info).await;
        return false;
    }

    cycle_info.next = cycle_info.next.max(required_cycle);
    mark_scan_cycle_idle(cycle_info).await;
    persist_scanner_cycle_state(ctx, storeapi, cycle_info, revision, leader_epoch).await
}

async fn await_scanner_cycle_with_lock_fence<Cycle, LockLost>(
    cycle_ctx: &CancellationToken,
    cycle: Cycle,
    lock_lost: LockLost,
) -> Option<Cycle::Output>
where
    Cycle: Future,
    LockLost: Future<Output = ()>,
{
    tokio::pin!(cycle);
    tokio::pin!(lock_lost);
    tokio::select! {
        biased;
        _ = &mut lock_lost => {
            cycle_ctx.cancel();
            tokio::time::timeout(SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, &mut cycle).await.ok()
        }
        output = &mut cycle => Some(output),
    }
}

#[instrument(skip_all)]
async fn run_data_scanner_cycle(
    ctx: &CancellationToken,
    storeapi: &Arc<ECStore>,
    cycle_info: &mut CurrentCycle,
    cycle_revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
) -> ScannerCycleOutcome {
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
    let configured_cycle_interval = scanner_cycle_interval();
    let configured_bitrot_cycle = scanner_bitrot_cycle();
    let cycle_budget_config = scanner_cycle_budget_config();
    let usage_persist_timeout = data_usage_persist_timeout();
    global_metrics().record_scanner_cycle_config(
        configured_cycle_interval,
        configured_bitrot_cycle,
        cycle_budget_config.max_duration,
        cycle_budget_config.max_objects,
        cycle_budget_config.max_directories,
    );
    cycle_info.current = cycle_info.next;
    let now = Instant::now();
    cycle_info.started = Utc::now();

    global_metrics().set_cycle(Some(cycle_info.clone())).await;

    let mut background_heal_info = read_background_heal_info(storeapi.clone()).await;

    let scan_mode = get_cycle_scan_mode(
        cycle_info.current,
        background_heal_info.bitrot_start_cycle,
        background_heal_info.bitrot_start_time,
        configured_bitrot_cycle,
    );
    info!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_CYCLE_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        cycle = cycle_info.current,
        scan_mode = ?scan_mode,
        state = "started",
        "Scanner cycle started"
    );
    let _scan_mode_guard = ScannerScanModeGuard::new(scan_mode);
    if let Some(new_heal_info) = background_heal_info_for_scan_start(
        background_heal_info.clone(),
        cycle_info.current,
        scan_mode,
        Utc::now(),
        configured_bitrot_cycle,
    ) {
        background_heal_info = new_heal_info.clone();
        save_background_heal_info(storeapi.clone(), new_heal_info).await;
    }

    let cycle_start = std::time::Instant::now();
    let usage_persist_baseline = match read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await {
        Ok((data, revision)) => DataUsagePersistBaseline {
            data: data.map(Bytes::from),
            revision,
        },
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
            mark_scan_cycle_idle(cycle_info).await;
            return ScannerCycleOutcome::Failed;
        }
    };
    let (sender, receiver) = mpsc::channel::<DataUsageInfo>(1);
    let storeapi_clone = storeapi.clone();
    let ctx_clone = ctx.clone();
    let mut usage_persist_task = tokio::spawn(async move {
        store_data_usage_in_backend_with_outcome_for_epoch_and_baseline(
            ctx_clone,
            storeapi_clone,
            receiver,
            Some(leader_epoch),
            Some(usage_persist_baseline),
        )
        .await
    });

    let done_cycle = Metrics::time(Metric::ScanCycle);
    let cycle_work_start = global_metrics().start_scan_cycle_work();
    let cycle_budget = ScannerCycleBudget::new(ctx, cycle_budget_config);
    let scan_result = storeapi
        .clone()
        .nsscanner_with_status(
            cycle_budget.token(),
            cycle_budget.clone(),
            sender,
            cycle_info.current,
            leader_epoch,
            scan_mode,
        )
        .await;
    let budget_elapsed = cycle_budget.budget_elapsed() && !ctx.is_cancelled();
    let usage_persist_outcome = match wait_for_data_usage_persist_task(ctx, &mut usage_persist_task, usage_persist_timeout).await
    {
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
            DataUsagePersistOutcome::Failed
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
            DataUsagePersistOutcome::Failed
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
            DataUsagePersistOutcome::Failed
        }
    };
    let unresolved_heal_work = global_metrics().current_scan_cycle_has_unresolved_heal_work();
    global_metrics().finish_scan_cycle_work(cycle_work_start);

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
                save_background_heal_info(storeapi.clone(), new_heal_info).await;
            }
            mark_scan_cycle_idle(cycle_info).await;
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
        mark_scan_cycle_idle(cycle_info).await;
        return ScannerCycleOutcome::Failed;
    }
    if let Some(required_cycle) = scan_cycle_result.required_cycle_floor() {
        warn!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_CYCLE_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            cycle = cycle_info.current,
            required_cycle,
            state = "cache_cycle_ahead",
            "Scanner cycle is recovering to a newer durable cache generation"
        );
        emit_scan_cycle_partial_with_source(cycle_start.elapsed(), ScanCyclePartialReason::Unknown, None);
        return if persist_required_scanner_cycle_floor(
            ctx,
            storeapi.clone(),
            cycle_info,
            cycle_revision,
            leader_epoch,
            required_cycle,
        )
        .await
        {
            ScannerCycleOutcome::Partial
        } else {
            ScannerCycleOutcome::Failed
        };
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
        mark_scan_cycle_idle(cycle_info).await;
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
        return if finalize_partial_scan_cycle(ctx, storeapi.clone(), cycle_info, cycle_revision, leader_epoch).await {
            ScannerCycleOutcome::Partial
        } else {
            ScannerCycleOutcome::Failed
        };
    }

    let (completion_outcome, scanner_pending_maintenance_work) =
        finalize_scanner_cycle_result(scan_cycle_result, usage_persist_outcome);
    let pending_maintenance_work = scanner_pending_maintenance_work || unresolved_heal_work;
    match completion_outcome {
        ScannerCycleOutcome::Failed => {
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
            mark_scan_cycle_idle(cycle_info).await;
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
                    "Scanner cycle ended without a complete usage snapshot"
                );
            }
            emit_scan_cycle_partial_with_source(cycle_start.elapsed(), ScanCyclePartialReason::Unknown, None);
            return if finalize_partial_scan_cycle(ctx, storeapi.clone(), cycle_info, cycle_revision, leader_epoch).await {
                ScannerCycleOutcome::Partial
            } else {
                ScannerCycleOutcome::Failed
            };
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
            if finalize_partial_scan_cycle(ctx, storeapi.clone(), cycle_info, cycle_revision, leader_epoch).await {
                emit_scan_cycle_superseded(cycle_start.elapsed());
                return ScannerCycleOutcome::Superseded;
            }
            emit_scan_cycle_complete(false, cycle_start.elapsed());
            return ScannerCycleOutcome::Failed;
        }
        ScannerCycleOutcome::Completed | ScannerCycleOutcome::CompletedWithPendingMaintenance => {}
    }
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
        mark_scan_cycle_idle(cycle_info).await;
        emit_scan_cycle_complete(false, cycle_start.elapsed());
        return ScannerCycleOutcome::Failed;
    }
    cycle_info.current = 0;
    cycle_info.cycle_completed.push(Utc::now());
    global_metrics().clear_current_scan_mode();

    retain_recent_cycle_completions(&mut cycle_info.cycle_completed);
    global_metrics().set_cycle(Some(cycle_info.clone())).await;
    if !persist_scanner_cycle_state(ctx, storeapi.clone(), cycle_info, cycle_revision, leader_epoch).await {
        mark_scan_cycle_idle(cycle_info).await;
        emit_scan_cycle_complete(false, cycle_start.elapsed());
        return ScannerCycleOutcome::Failed;
    }

    done_cycle();
    emit_scan_cycle_complete(true, cycle_start.elapsed());
    if let Some(new_heal_info) = background_heal_info_for_scan_result(background_heal_info.clone(), scan_mode, true) {
        save_background_heal_info(storeapi.clone(), new_heal_info).await;
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
        "Scanner cycle completed"
    );

    scanner_cycle_outcome_with_pending_maintenance(ScannerCycleOutcome::Completed, pending_maintenance_work)
}

async fn record_scanner_leader_lock_lost(message: &'static str) {
    reset_scanner_cycle_schedule();
    record_scanner_leader_lock_state("lost");
    global_metrics()
        .record_scanner_leader_liveness("lost", false, "leader lock refresh quorum lost")
        .await;
    warn!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_LOCK_STATE,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        lock_name = "leader.lock",
        state = "lost",
        reason = message,
        "Scanner leader lock lost"
    );
}

pub async fn run_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) -> Result<(), ScannerError> {
    let (maintenance_features, maintenance_generation) = configure_scanner_defaults(&ctx, &storeapi).await;
    run_data_scanner_with_maintenance_state(ctx, storeapi, maintenance_features, maintenance_generation).await
}

async fn run_data_scanner_with_maintenance_state(
    ctx: CancellationToken,
    storeapi: Arc<ECStore>,
    mut maintenance_features: ScannerMaintenanceFeatures,
    mut maintenance_generation_seen: Option<u64>,
) -> Result<(), ScannerError> {
    reset_scanner_cycle_schedule();
    // Acquire leader lock (write lock) to ensure only one scanner runs
    let guard = match storeapi.new_ns_lock(RUSTFS_META_BUCKET, "leader.lock").await {
        Ok(ns_lock) => match ns_lock.get_write_lock_quiet(get_lock_acquire_timeout()).await {
            Ok(guard) => {
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
                record_scanner_leader_lock_state("contended");
                global_metrics()
                    .record_scanner_leader_liveness("contended", false, e.to_string())
                    .await;
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_LOCK_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    lock_name = "leader.lock",
                    state = "contended",
                    error = ?e,
                    "Scanner leader lock contended"
                );
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
    let single_disk = storeapi.setup_is_erasure_sd().await;
    let erasure = storeapi.setup_is_erasure().await;
    let distributed = storeapi.setup_is_dist_erasure().await;
    let clean_idle_topology_supported = single_disk || erasure;
    let mut dirty_usage_generation_seen = dirty_usage_generation();
    let mut runtime_config_generation_seen = scanner_runtime_config_generation();
    let mut clean_idle_backoff = ScannerCleanIdleBackoff::default();
    let initial_runtime_config = resolve_scanner_runtime_config();
    if clean_idle_topology_supported
        && scanner_clean_idle_backoff_configured(&initial_runtime_config)
        && maintenance_generation_seen.is_none()
    {
        let Some((features, generation)) = detect_stable_scanner_maintenance_features(&ctx, &storeapi).await else {
            global_metrics().set_cycle(None).await;
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

    let (buf, mut cycle_revision) = match read_config_with_revision(storeapi.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str()).await {
        Ok((buf, revision)) => (buf.unwrap_or_default(), revision),
        Err(err) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "revision_load_failed",
                error = %err,
                "Scanner cycle state revision load failed"
            );
            global_metrics().set_cycle(None).await;
            return Ok(());
        }
    };
    let (mut cycle_info, mut leader_epoch) = match decode_scanner_cycle_state_for_startup(&buf) {
        Ok(state) => state,
        Err(err) => {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "cycle_decode_failed",
                error = %err,
                "Scanner stopped because persisted cycle state is invalid"
            );
            global_metrics().set_cycle(None).await;
            return Ok(());
        }
    };
    let usage_floor = match persisted_usage_floor(storeapi.clone()).await {
        Ok(floor) => floor,
        Err(err) => {
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
            global_metrics().set_cycle(None).await;
            return Ok(());
        }
    };
    apply_persisted_usage_floor(&mut cycle_info, &mut leader_epoch, usage_floor);

    if ctx.is_cancelled() || guard.is_lock_lost() {
        global_metrics().set_cycle(None).await;
        return Ok(());
    }
    let claim_ctx = ctx.child_token();
    let leadership_claimed = await_scanner_cycle_with_lock_fence(
        &claim_ctx,
        claim_scanner_leadership(&claim_ctx, storeapi.clone(), &mut cycle_info, &mut cycle_revision, &mut leader_epoch),
        guard.lock_lost_notified(),
    )
    .await
    .unwrap_or(false);
    if guard.is_lock_lost() {
        record_scanner_leader_lock_lost("Scanner leader lock lost while claiming the leadership epoch").await;
        global_metrics().set_cycle(None).await;
        return Ok(());
    }
    if !leadership_claimed {
        error!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_LOCK_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            lock_name = "leader.lock",
            state = "epoch_claim_failed",
            "Scanner stopped because the leadership epoch could not be claimed"
        );
        global_metrics()
            .record_scanner_leader_liveness("epoch_claim_failed", false, "leadership epoch claim failed")
            .await;
        global_metrics().set_cycle(None).await;
        return Ok(());
    }

    if !ctx.is_cancelled() {
        // Preserve previous behavior: run one cycle immediately after lock acquisition.
        let dirty_generation_before_cycle = dirty_usage_generation();
        let dirty_usage_pending_before_cycle = dirty_usage_buckets_pending();
        let maintenance_generation_before_cycle = scanner_maintenance_generation();
        if guard.is_lock_lost() {
            record_scanner_leader_lock_lost("Scanner leader lock lost before the initial cycle").await;
            global_metrics().set_cycle(None).await;
            return Ok(());
        }
        let cycle_ctx = ctx.child_token();
        let initial_outcome = await_scanner_cycle_with_lock_fence(
            &cycle_ctx,
            run_data_scanner_cycle(&cycle_ctx, &storeapi, &mut cycle_info, &mut cycle_revision, leader_epoch),
            guard.lock_lost_notified(),
        )
        .await
        .unwrap_or(ScannerCycleOutcome::Failed);
        dirty_usage_generation_seen = dirty_generation_before_cycle;
        if guard.is_lock_lost() {
            record_scanner_leader_lock_lost("Scanner leader lock lost during the initial cycle").await;
            global_metrics().set_cycle(None).await;
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
        let backoff_enabled = scanner_clean_idle_backoff_enabled(
            clean_idle_topology_supported,
            scanner_activity_ready,
            maintenance_features,
            &runtime_config,
        );
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
            ) || maintenance_generation_before_cycle != scanner_maintenance_generation()
                || scanner_activity_observed_work(scanner_activity_observation),
        );
        runtime_config_generation_seen = scanner_runtime_config_generation();
    }

    loop {
        if ctx.is_cancelled() {
            break;
        }

        let runtime_config = resolve_scanner_runtime_config();
        if clean_idle_topology_supported && scanner_clean_idle_backoff_configured(&runtime_config) {
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
        let backoff_enabled = scanner_clean_idle_backoff_enabled(
            clean_idle_topology_supported,
            scanner_activity_ready,
            maintenance_features,
            &runtime_config,
        );
        let wait_plan = scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, backoff_enabled, randomized_cycle_delay_for);
        let dirty_generation_before_wait = dirty_usage_generation();
        let dirty_usage_pending_before_wait = dirty_usage_buckets_pending();
        let maintenance_generation_before_wait = scanner_maintenance_generation();
        record_scanner_cycle_schedule(
            wait_plan.effective_interval,
            backoff_enabled,
            u64::from(clean_idle_backoff.interval_multiplier),
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
            interval_multiplier = clean_idle_backoff.interval_multiplier,
            clean_idle_backoff_enabled = backoff_enabled,
            lifecycle_active = maintenance_features.lifecycle,
            replication_active = maintenance_features.replication,
            feature_inspection_failed = maintenance_features.inspection_failed,
            state = "wait_scheduled",
            "Scanner cycle wait scheduled"
        );

        let activity_poll_interval = backoff_enabled.then_some(runtime_config.cycle_interval.max(Duration::from_secs(1)));
        let wake_reason = wait_for_next_scanner_cycle_with_activity(
            &ctx,
            wait_plan.delay,
            activity_poll_interval,
            &mut scanner_activity_seen,
            ScannerCycleObservedGenerations {
                dirty_usage: dirty_usage_generation_seen,
                runtime_config: runtime_config_generation_seen,
                maintenance: maintenance_generation_before_wait,
            },
            || guard.is_lock_lost(),
            || probe_scanner_activity(storeapi.as_ref(), distributed),
        )
        .await;
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
            ScannerCycleWakeReason::Timer
            | ScannerCycleWakeReason::DirtyUsage
            | ScannerCycleWakeReason::ClusterActivity
            | ScannerCycleWakeReason::ClusterActivityUnavailable => {}
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
        let dirty_generation_before_cycle = dirty_usage_generation();
        let cycle_ctx = ctx.child_token();
        let outcome = await_scanner_cycle_with_lock_fence(
            &cycle_ctx,
            run_data_scanner_cycle(&cycle_ctx, &storeapi, &mut cycle_info, &mut cycle_revision, leader_epoch),
            guard.lock_lost_notified(),
        )
        .await
        .unwrap_or(ScannerCycleOutcome::Failed);
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
        if clean_idle_topology_supported
            && scanner_clean_idle_backoff_configured(&runtime_config)
            && (maintenance_config_changed || retry_failed_inspection)
        {
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
        let backoff_enabled = scanner_clean_idle_backoff_enabled(
            clean_idle_topology_supported,
            scanner_activity_ready,
            maintenance_features,
            &runtime_config,
        );
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
            ) || scanner_activity_observed_work(scanner_activity_observation),
        );
    }

    global_metrics().set_cycle(None).await;
    reset_scanner_cycle_schedule();
    if !guard.is_lock_lost() {
        global_metrics().record_scanner_leader_liveness("stopped", false, "").await;
    }

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum DataUsagePersistOutcome {
    #[default]
    NoUpdate,
    Current,
    AlreadyDurable,
    PriorCycleDurable,
    Saved,
    Failed,
}

#[derive(Clone, Debug)]
struct DataUsagePersistBaseline {
    data: Option<Bytes>,
    revision: DataUsageCacheRevision,
}

#[derive(Debug)]
enum DataUsagePersistTaskResult {
    Completed(DataUsagePersistOutcome),
    Cancelled,
    TimedOut,
    JoinFailed(tokio::task::JoinError),
}

async fn wait_for_data_usage_persist_task(
    ctx: &CancellationToken,
    task: &mut tokio::task::JoinHandle<DataUsagePersistOutcome>,
    timeout: Duration,
) -> DataUsagePersistTaskResult {
    tokio::select! {
        biased;
        result = &mut *task => match result {
            Ok(outcome) => DataUsagePersistTaskResult::Completed(outcome),
            Err(err) => DataUsagePersistTaskResult::JoinFailed(err),
        },
        _ = ctx.cancelled() => {
            task.abort();
            let _ = (&mut *task).await;
            DataUsagePersistTaskResult::Cancelled
        },
        _ = tokio::time::sleep(timeout) => {
            task.abort();
            let _ = (&mut *task).await;
            DataUsagePersistTaskResult::TimedOut
        }
    }
}

fn scanner_cycle_completion_outcome(
    scan_status: ScannerCycleStatus,
    usage_persist_outcome: DataUsagePersistOutcome,
    has_dirty_usage: bool,
    has_failed_dirty_usage: bool,
) -> ScannerCycleOutcome {
    match (scan_status, usage_persist_outcome) {
        (_, DataUsagePersistOutcome::Failed) => ScannerCycleOutcome::Failed,
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
    usage_persist_outcome: DataUsagePersistOutcome,
) -> (ScannerCycleOutcome, bool) {
    let completion_outcome = scanner_cycle_completion_outcome(
        scan_cycle_result.status,
        usage_persist_outcome,
        scan_cycle_result.has_dirty_usage_to_acknowledge(),
        scan_cycle_result.has_failed_dirty_usage(),
    );
    let pending_maintenance_work = scan_cycle_result.has_pending_maintenance_work();
    if matches!(
        usage_persist_outcome,
        DataUsagePersistOutcome::Saved | DataUsagePersistOutcome::AlreadyDurable
    ) {
        scan_cycle_result.acknowledge_durable_usage();
    }
    (completion_outcome, pending_maintenance_work)
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
#[instrument(skip(ctx, storeapi))]
pub async fn store_data_usage_in_backend(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    receiver: mpsc::Receiver<DataUsageInfo>,
) {
    let _ = store_data_usage_in_backend_with_outcome(ctx, storeapi, receiver).await;
}

async fn store_data_usage_in_backend_with_outcome(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    receiver: mpsc::Receiver<DataUsageInfo>,
) -> DataUsagePersistOutcome {
    store_data_usage_in_backend_with_outcome_for_epoch(ctx, storeapi, receiver, None).await
}

async fn store_data_usage_in_backend_with_outcome_for_epoch(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
) -> DataUsagePersistOutcome {
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline(ctx, storeapi, receiver, leader_epoch, None).await
}

async fn store_data_usage_in_backend_with_outcome_for_epoch_and_baseline(
    ctx: CancellationToken,
    storeapi: Arc<impl ScannerObjectIO>,
    mut receiver: mpsc::Receiver<DataUsageInfo>,
    leader_epoch: Option<u64>,
    initial_baseline: Option<DataUsagePersistBaseline>,
) -> DataUsagePersistOutcome {
    let mut attempts = 1u32;
    let mut outcome = DataUsagePersistOutcome::NoUpdate;
    let mut next_baseline = initial_baseline;

    'updates: while let Some(mut data_usage_info) = receiver.recv().await {
        let _activity_guard = ScannerActivityGuard::new();
        if ctx.is_cancelled() {
            break;
        }
        if let Some(leader_epoch) = leader_epoch {
            data_usage_info.scanner_epoch = Some(leader_epoch);
        }

        let data = match serde_json::to_vec(&data_usage_info) {
            Ok(data) => data,
            Err(e) => {
                error!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                    state = "encode_failed",
                    error = %e,
                    "Scanner data usage encode failed"
                );
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::EncodeFailed);
                outcome = DataUsagePersistOutcome::Failed;
                continue;
            }
        };
        let sha256hex = (!data.is_empty()).then(|| hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower));
        let data = Bytes::from(data);
        let backup_data = (attempts > 10).then(|| data.clone());
        let mut cas_retry = 0usize;
        let save_outcome = loop {
            if ctx.is_cancelled() {
                break 'updates;
            }

            let baseline = if cas_retry == 0 { next_baseline.take() } else { None };
            let (existing_data, revision) = match baseline {
                Some(baseline) => (baseline.data, baseline.revision),
                None => match read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await {
                    Ok((data, revision)) => (data.map(Bytes::from), revision),
                    Err(e) => {
                        error!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                            state = "revision_load_failed",
                            error = %e,
                            "Scanner data usage revision load failed"
                        );
                        break DataUsagePersistOutcome::Failed;
                    }
                },
            };
            let existing = existing_data
                .as_deref()
                .and_then(|buf| serde_json::from_slice::<DataUsageInfo>(buf).ok());
            if cas_retry > 0 && data_usage_reintroduces_missing_bucket(&data_usage_info, existing.as_ref()) {
                debug!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                    incoming_scanner_epoch = ?data_usage_info.scanner_epoch,
                    incoming_scanner_cycle = ?data_usage_info.scanner_cycle,
                    state = "skip_deleted_bucket_reintroduction",
                    "Scanner usage update skipped after a concurrent bucket removal"
                );
                break DataUsagePersistOutcome::Current;
            }
            if let Some(existing) = existing.as_ref() {
                if existing == &data_usage_info {
                    break DataUsagePersistOutcome::AlreadyDurable;
                }
                if existing.scanner_epoch.is_some()
                    && existing.scanner_epoch == data_usage_info.scanner_epoch
                    && existing.scanner_cycle.is_some()
                    && existing.scanner_cycle == data_usage_info.scanner_cycle
                {
                    break DataUsagePersistOutcome::PriorCycleDurable;
                }
                if let Some(reason) = stale_data_usage_update_reason(&data_usage_info, existing, std::time::SystemTime::now()) {
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                        incoming_scanner_epoch = ?data_usage_info.scanner_epoch,
                        existing_scanner_epoch = ?existing.scanner_epoch,
                        incoming_scanner_cycle = ?data_usage_info.scanner_cycle,
                        existing_scanner_cycle = ?existing.scanner_cycle,
                        incoming_last_update = ?data_usage_info.last_update,
                        existing_last_update = ?existing.last_update,
                        reason = reason,
                        state = "skip_stale_update",
                        "Scanner stale data usage update skipped"
                    );
                    break DataUsagePersistOutcome::Current;
                }
            }
            if ctx.is_cancelled() {
                break 'updates;
            }

            let done_save = Metrics::time(Metric::SaveUsage);
            let save_result = save_config_shared_with_preconditions(
                storeapi.clone(),
                DATA_USAGE_OBJ_NAME_PATH.as_str(),
                data.clone(),
                sha256hex.clone(),
                revision.preconditions(),
            )
            .await;
            done_save();

            match save_result {
                Ok(object_info) => {
                    next_baseline = object_info
                        .etag
                        .filter(|etag| !etag.is_empty())
                        .map(|etag| DataUsagePersistBaseline {
                            data: Some(data.clone()),
                            revision: DataUsageCacheRevision::Etag(etag),
                        });
                    break DataUsagePersistOutcome::Saved;
                }
                Err(EcstoreError::PreconditionFailed) if cas_retry < SCANNER_PERSIST_CAS_RETRIES => {
                    cas_retry += 1;
                    debug!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                        state = "conflict_retry",
                        retry = cas_retry,
                        "Scanner data usage CAS conflict will be reconciled"
                    );
                }
                Err(e) => {
                    error!(
                        target: "rustfs::scanner",
                        event = EVENT_SCANNER_PERSIST_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                        state = if matches!(e, EcstoreError::PreconditionFailed) {
                            "conflict_retries_exhausted"
                        } else {
                            "save_failed"
                        },
                        error = %e,
                        "Scanner data usage save failed"
                    );
                    break DataUsagePersistOutcome::Failed;
                }
            }
        };

        match save_outcome {
            DataUsagePersistOutcome::Current => {
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::SkippedStale);
                outcome = DataUsagePersistOutcome::Current;
                continue;
            }
            DataUsagePersistOutcome::AlreadyDurable => {
                replace_bucket_usage_memory_from_info(&data_usage_info).await;
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
                outcome = DataUsagePersistOutcome::AlreadyDurable;
                continue;
            }
            DataUsagePersistOutcome::PriorCycleDurable => {
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
                outcome = DataUsagePersistOutcome::PriorCycleDurable;
                continue;
            }
            DataUsagePersistOutcome::Failed | DataUsagePersistOutcome::NoUpdate => {
                global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Failed);
                outcome = DataUsagePersistOutcome::Failed;
                attempts += 1;
                continue;
            }
            DataUsagePersistOutcome::Saved => {}
        }

        replace_bucket_usage_memory_from_info(&data_usage_info).await;
        global_metrics().record_scanner_usage_save_result(ScannerUsageSaveResult::Success);
        outcome = DataUsagePersistOutcome::Saved;

        if let Some(data) = backup_data {
            let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
            let done_save = Metrics::time(Metric::SaveUsage);
            let backup_result = match read_config_with_revision(storeapi.clone(), &backup_path).await {
                Ok((existing, revision)) => {
                    let existing = existing
                        .as_deref()
                        .and_then(|buf| serde_json::from_slice::<DataUsageInfo>(buf).ok());
                    let stale = existing
                        .as_ref()
                        .and_then(|existing| {
                            stale_data_usage_update_reason(&data_usage_info, existing, std::time::SystemTime::now())
                        })
                        .is_some();
                    let reintroduces_deleted_bucket = existing
                        .as_ref()
                        .is_some_and(|existing| data_usage_reintroduces_missing_bucket(&data_usage_info, Some(existing)));
                    if reintroduces_deleted_bucket {
                        debug!(
                            target: "rustfs::scanner",
                            event = EVENT_SCANNER_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_RUNTIME,
                            path = %backup_path,
                            incoming_scanner_epoch = ?data_usage_info.scanner_epoch,
                            incoming_scanner_cycle = ?data_usage_info.scanner_cycle,
                            state = "skip_deleted_bucket_reintroduction",
                            "Scanner usage backup skipped after a concurrent bucket removal"
                        );
                        Ok(None)
                    } else if stale || ctx.is_cancelled() {
                        Ok(None)
                    } else {
                        save_config_shared_with_preconditions(
                            storeapi.clone(),
                            &backup_path,
                            data,
                            sha256hex.clone(),
                            revision.preconditions(),
                        )
                        .await
                        .map(Some)
                    }
                }
                Err(err) => Err(err),
            };
            if let Err(e) = backup_result {
                warn!(
                    target: "rustfs::scanner",
                    event = EVENT_SCANNER_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    path = %backup_path,
                    state = "backup_save_failed",
                    error = %e,
                    "Scanner data usage backup save failed"
                );
            }
            done_save();
            attempts = 1;
        }

        attempts += 1;
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EcstoreResult;
    use crate::{
        ScannerGetObjectReader as GetObjectReader, ScannerObjectInfo as ObjectInfo, ScannerObjectOptions as ObjectOptions,
        ScannerPutObjReader as PutObjReader,
    };
    use serial_test::serial;
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::task::Poll;
    use temp_env::{with_var, with_var_unset};
    use tokio::io::AsyncReadExt;
    use tokio::sync::Mutex;

    const TEST_DEFAULT_SCANNER_CYCLE_SECS: u64 = 24 * 60 * 60;

    fn assert_run_data_scanner_signature<F, Fut>(_run: F)
    where
        F: Fn(CancellationToken, Arc<ECStore>) -> Fut,
        Fut: Future<Output = Result<(), ScannerError>>,
    {
    }

    #[test]
    fn run_data_scanner_keeps_its_two_argument_api() {
        assert_run_data_scanner_signature(run_data_scanner);
    }

    #[tokio::test]
    async fn scanner_cycle_lock_fence_cancels_cycle_context() {
        let cycle_ctx = CancellationToken::new();
        let observed_ctx = cycle_ctx.clone();
        let output = await_scanner_cycle_with_lock_fence(
            &cycle_ctx,
            async move {
                observed_ctx.cancelled().await;
                observed_ctx.is_cancelled()
            },
            std::future::ready(()),
        )
        .await;

        assert_eq!(output, Some(true));
        assert!(cycle_ctx.is_cancelled());
    }

    #[tokio::test]
    async fn scanner_cycle_lock_fence_preserves_completed_cycle() {
        let cycle_ctx = CancellationToken::new();
        let output = await_scanner_cycle_with_lock_fence(&cycle_ctx, std::future::ready(7_u8), std::future::pending()).await;

        assert_eq!(output, Some(7));
        assert!(!cycle_ctx.is_cancelled());
    }

    #[tokio::test]
    async fn scanner_cycle_lock_fence_bounds_uncooperative_shutdown() {
        let cycle_ctx = CancellationToken::new();
        let output = await_scanner_cycle_with_lock_fence(&cycle_ctx, std::future::pending::<()>(), std::future::ready(())).await;

        assert_eq!(output, None);
        assert!(cycle_ctx.is_cancelled());
    }

    struct ScannerDefaultSpeedGuard;

    impl ScannerDefaultSpeedGuard {
        fn set(speed: ScannerSpeed) -> Self {
            set_scanner_default_speed(speed);
            Self
        }
    }

    impl Drop for ScannerDefaultSpeedGuard {
        fn drop(&mut self) {
            set_scanner_default_speed(ScannerSpeed::Default);
        }
    }

    struct ScannerDefaultCycleGuard;

    impl ScannerDefaultCycleGuard {
        fn set(secs: u64) -> Self {
            set_scanner_default_cycle_secs(Some(secs));
            Self
        }
    }

    impl Drop for ScannerDefaultCycleGuard {
        fn drop(&mut self) {
            set_scanner_default_cycle_secs(None);
        }
    }

    #[derive(Debug, Default)]
    struct MemoryConfigStore {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        revisions: Mutex<HashMap<String, u64>>,
        fail_put_number: Mutex<HashMap<String, usize>>,
        error_after_commit_put_number: Mutex<HashMap<String, usize>>,
        interleaving_puts: Mutex<HashMap<String, (usize, Vec<u8>)>>,
        cancel_after_interleaving_puts: Mutex<HashMap<String, CancellationToken>>,
        cancel_after_successful_puts: Mutex<HashMap<String, (usize, CancellationToken)>>,
        put_counts: Mutex<HashMap<String, usize>>,
    }

    fn memory_config_key(bucket: &str, object: &str) -> String {
        format!("{bucket}/{object}")
    }

    #[async_trait::async_trait]
    impl crate::storage_api::scanner_io::ObjectIO for MemoryConfigStore {
        type Error = EcstoreError;
        type RangeSpec = crate::storage_api::scanner_io::HTTPRangeSpec;
        type HeaderMap = http::HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = GetObjectReader;
        type PutObjectReader = PutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<crate::storage_api::scanner_io::HTTPRangeSpec>,
            _h: http::HeaderMap,
            _opts: &ObjectOptions,
        ) -> EcstoreResult<GetObjectReader> {
            let key = memory_config_key(bucket, object);
            let data = self
                .objects
                .lock()
                .await
                .get(&key)
                .cloned()
                .ok_or(EcstoreError::FileNotFound)?;
            let revision = *self.revisions.lock().await.entry(key).or_insert(1);

            Ok(GetObjectReader {
                stream: Box::new(Cursor::new(data)),
                object_info: ObjectInfo {
                    etag: Some(format!("memory-{revision}")),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            data: &mut PutObjReader,
            opts: &ObjectOptions,
        ) -> EcstoreResult<ObjectInfo> {
            let mut buf = Vec::new();
            data.stream.read_to_end(&mut buf).await?;
            let key = memory_config_key(bucket, object);
            let put_count = {
                let mut put_counts = self.put_counts.lock().await;
                let put_count = put_counts.entry(key.clone()).or_insert(0);
                *put_count += 1;
                *put_count
            };

            if self.fail_put_number.lock().await.get(&key) == Some(&put_count) {
                return Err(EcstoreError::other("injected put failure"));
            }

            let interleaving_data = {
                let mut interleaving_puts = self.interleaving_puts.lock().await;
                if interleaving_puts
                    .get(&key)
                    .is_some_and(|(expected_put, _)| *expected_put == put_count)
                {
                    interleaving_puts.remove(&key).map(|(_, data)| data)
                } else {
                    None
                }
            };
            let cancel_after_interleaving = if interleaving_data.is_some() {
                self.cancel_after_interleaving_puts.lock().await.remove(&key)
            } else {
                None
            };
            let mut objects = self.objects.lock().await;
            let mut revisions = self.revisions.lock().await;
            if let Some(interleaving_data) = interleaving_data {
                let revision = revisions.get(&key).copied().unwrap_or(0) + 1;
                objects.insert(key.clone(), interleaving_data);
                revisions.insert(key.clone(), revision);
                if let Some(cancel) = cancel_after_interleaving {
                    cancel.cancel();
                }
            }
            let current_revision = objects.contains_key(&key).then(|| revisions.get(&key).copied().unwrap_or(1));
            if let Some(preconditions) = &opts.http_preconditions {
                if preconditions
                    .if_none_match
                    .as_deref()
                    .is_some_and(|condition| !condition.trim().is_empty())
                    && current_revision.is_some()
                {
                    return Err(EcstoreError::PreconditionFailed);
                }
                if let Some(expected) = preconditions
                    .if_match
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    let actual = current_revision.map(|revision| format!("memory-{revision}"));
                    if actual.as_deref() != Some(expected.trim_matches('"')) {
                        return Err(EcstoreError::PreconditionFailed);
                    }
                }
            }

            let revision = current_revision.unwrap_or(0) + 1;
            objects.insert(key.clone(), buf);
            revisions.insert(key.clone(), revision);
            drop(revisions);
            drop(objects);
            let cancel_after_success = {
                let mut cancellations = self.cancel_after_successful_puts.lock().await;
                if cancellations
                    .get(&key)
                    .is_some_and(|(expected_put, _)| *expected_put == put_count)
                {
                    cancellations.remove(&key).map(|(_, cancel)| cancel)
                } else {
                    None
                }
            };
            if let Some(cancel) = cancel_after_success {
                cancel.cancel();
            }
            if self.error_after_commit_put_number.lock().await.get(&key) == Some(&put_count) {
                return Err(EcstoreError::other("injected post-commit put failure"));
            }
            Ok(ObjectInfo {
                etag: Some(format!("memory-{revision}")),
                ..Default::default()
            })
        }
    }

    fn with_unset_scanner_timing_env(f: impl FnOnce()) {
        with_var_unset(ENV_SCANNER_SPEED, || {
            with_var_unset("MINIO_SCANNER_SPEED", || {
                with_var_unset(ENV_SCANNER_CYCLE, || {
                    with_var_unset("MINIO_SCANNER_CYCLE", || {
                        with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                            with_var_unset(ENV_SCANNER_START_DELAY_SECS_DEPRECATED, f);
                        });
                    });
                });
            });
        });
    }

    #[test]
    #[serial]
    fn test_randomized_cycle_delay_keeps_configured_start_delay() {
        // 120s with ±10% jitter should stay clearly above the historic 30s cap.
        let delay = randomized_cycle_delay_for(Duration::from_secs(120));
        assert!(delay > Duration::from_secs(30), "expected delay > 30s, got {delay:?}");
        // Jitter window should stay within configured bounds.
        assert!(delay >= Duration::from_secs(108));
        assert!(delay <= Duration::from_secs(132));
    }

    #[test]
    #[serial]
    fn test_initial_scanner_delay_uses_configured_start_delay() {
        let delay = initial_scanner_delay_for(Some(120));
        assert!(delay >= Duration::from_secs(108));
        assert!(delay <= Duration::from_secs(132));
    }

    #[test]
    #[serial]
    fn test_initial_scanner_delay_uses_cycle_without_explicit_start_delay() {
        with_var(ENV_SCANNER_CYCLE, Some("120"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            let delay = initial_scanner_delay_for(None);
            assert!(delay >= Duration::from_secs(108));
            assert!(delay <= Duration::from_secs(132));
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[test]
    #[serial]
    fn test_initial_scanner_delay_skips_for_cold_usage_cache_with_buckets() {
        let delay = initial_scanner_delay_for_startup(Some(120), true, true, false);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    #[serial]
    fn test_initial_scanner_delay_keeps_configured_delay_for_warm_usage_cache_no_replication() {
        let delay = initial_scanner_delay_for_startup(Some(120), false, true, false);
        assert!(delay >= Duration::from_secs(108));
        assert!(delay <= Duration::from_secs(132));
    }

    #[test]
    #[serial]
    fn test_initial_scanner_delay_skips_for_cold_usage_cache_without_buckets() {
        let delay = initial_scanner_delay_for_startup(Some(120), true, false, false);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    #[serial]
    fn test_initial_scanner_delay_skips_for_active_replication_warm_cache() {
        // Warm cache + active replication rules → skip startup delay so that FAILED-status objects
        // from a crash are healed on the first cycle, not after a 27-33 min sleep.
        let delay = initial_scanner_delay_for_startup(Some(120), false, true, true);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    #[serial]
    fn test_initial_scanner_delay_keeps_delay_for_replication_without_buckets() {
        // Active replication but no buckets → no objects to scan, keep normal delay.
        let delay = initial_scanner_delay_for_startup(Some(120), false, false, true);
        assert!(delay >= Duration::from_secs(108));
        assert!(delay <= Duration::from_secs(132));
    }

    #[test]
    #[serial]
    fn test_scanner_cycle_max_duration_uses_env() {
        with_var(ENV_SCANNER_CYCLE_MAX_DURATION_SECS, Some("42"), || {
            assert_eq!(scanner_cycle_max_duration(), Some(Duration::from_secs(42)));
        });
    }

    #[test]
    #[serial]
    fn test_scanner_cycle_max_duration_default_is_disabled() {
        with_var_unset(ENV_SCANNER_CYCLE_MAX_DURATION_SECS, || {
            assert_eq!(scanner_cycle_max_duration(), None);
        });
    }

    #[tokio::test]
    async fn test_scanner_cycle_budget_cancels_after_duration() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_duration: Some(Duration::from_millis(1)),
                ..Default::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(5), budget.token().cancelled())
            .await
            .expect("scanner cycle budget should cancel after max duration");

        assert!(budget.budget_elapsed());
        assert!(budget.token().is_cancelled());
    }

    #[tokio::test]
    async fn test_scanner_cycle_budget_drop_cancels_child_without_elapsed() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_duration: Some(Duration::from_secs(60)),
                ..Default::default()
            },
        );
        let token = budget.token();

        drop(budget);

        assert!(token.is_cancelled());
    }

    #[test]
    #[serial]
    fn test_scanner_cycle_budget_config_uses_work_budget_env() {
        with_var(ENV_SCANNER_CYCLE_MAX_OBJECTS, Some("100"), || {
            with_var(ENV_SCANNER_CYCLE_MAX_DIRECTORIES, Some("25"), || {
                let config = scanner_cycle_budget_config();
                assert_eq!(config.max_objects, Some(100));
                assert_eq!(config.max_directories, Some(25));
            });
        });
    }

    #[test]
    #[serial]
    fn test_scanner_cycle_budget_config_disables_zero_work_budgets() {
        with_var(ENV_SCANNER_CYCLE_MAX_OBJECTS, Some("0"), || {
            with_var(ENV_SCANNER_CYCLE_MAX_DIRECTORIES, Some("0"), || {
                let config = scanner_cycle_budget_config();
                assert_eq!(config.max_objects, None);
                assert_eq!(config.max_directories, None);
            });
        });
    }

    #[test]
    fn test_scan_cycle_partial_reason_maps_budget_reason() {
        assert_eq!(
            scan_cycle_partial_reason(Some(ScannerCycleBudgetReason::Runtime)),
            ScanCyclePartialReason::Runtime
        );
        assert_eq!(
            scan_cycle_partial_reason(Some(ScannerCycleBudgetReason::Objects)),
            ScanCyclePartialReason::Objects
        );
        assert_eq!(
            scan_cycle_partial_reason(Some(ScannerCycleBudgetReason::Directories)),
            ScanCyclePartialReason::Directories
        );
        assert_eq!(scan_cycle_partial_reason(None), ScanCyclePartialReason::Unknown);
    }

    #[test]
    fn test_scan_cycle_partial_source_maps_budget_reason() {
        assert_eq!(scan_cycle_partial_source(Some(ScannerCycleBudgetReason::Runtime)), None);
        assert_eq!(
            scan_cycle_partial_source(Some(ScannerCycleBudgetReason::Objects)),
            Some(ScannerWorkSource::Usage)
        );
        assert_eq!(
            scan_cycle_partial_source(Some(ScannerCycleBudgetReason::Directories)),
            Some(ScannerWorkSource::Usage)
        );
        assert_eq!(scan_cycle_partial_source(None), None);
    }

    #[tokio::test]
    #[serial]
    async fn test_mark_scan_cycle_idle_clears_published_cycle_state() {
        let mut cycle_info = CurrentCycle {
            current: 12,
            next: 13,
            cycle_completed: vec![Utc::now()],
            started: Utc::now(),
        };

        global_metrics().set_current_scan_mode(HealScanMode::Deep);
        global_metrics().set_cycle(Some(cycle_info.clone())).await;

        mark_scan_cycle_idle(&mut cycle_info).await;

        let published = global_metrics()
            .get_cycle()
            .await
            .expect("scanner cycle state should remain published");

        assert_eq!(cycle_info.current, 0);
        assert_eq!(cycle_info.next, 13);
        assert_eq!(published.current, 0);
        assert_eq!(published.next, 13);
        assert_eq!(global_metrics().current_scan_mode(), HealScanMode::Unknown);

        global_metrics().set_cycle(None).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_finalize_partial_scan_cycle_advances_and_persists_counter() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let mut revision = DataUsageCacheRevision::Missing;
        let mut cycle_info = CurrentCycle {
            current: 12,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };

        assert!(finalize_partial_scan_cycle(&ctx, store.clone(), &mut cycle_info, &mut revision, 1).await);

        assert_eq!(cycle_info.next, 13);
        assert_eq!(cycle_info.current, 0);
        assert!(cycle_info.cycle_completed.is_empty());
        assert!(matches!(revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-1"));

        let buf = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("cycle state should be persisted after a partial cycle");
        assert_eq!(
            u64::from_le_bytes(buf[0..8].try_into().expect("persisted state should start with the counter")),
            13
        );
        let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("persisted cycle info should decode");
        assert_eq!(decoded.next, 13);
        assert_eq!(decoded.current, 0);
        assert_eq!(epoch, 1);

        global_metrics().set_cycle(None).await;
    }

    #[tokio::test]
    #[serial]
    async fn scanner_cycle_recovers_to_newer_durable_cache_floor() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let mut revision = DataUsageCacheRevision::Missing;
        let mut cycle_info = CurrentCycle {
            current: 12,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };

        assert!(persist_required_scanner_cycle_floor(&ctx, store.clone(), &mut cycle_info, &mut revision, 7, 19).await);
        assert_eq!(cycle_info.current, 0);
        assert_eq!(cycle_info.next, 19);

        let buf = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("recovered cycle floor should be persisted");
        let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("recovered cycle state should decode");
        assert_eq!(decoded.current, 0);
        assert_eq!(decoded.next, 19);
        assert_eq!(epoch, 7);

        global_metrics().set_cycle(None).await;
    }

    #[tokio::test]
    #[serial]
    async fn scanner_cycle_rejects_invalid_cache_floor() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let mut revision = DataUsageCacheRevision::Missing;
        let mut cycle_info = CurrentCycle {
            current: 12,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };

        assert!(!persist_required_scanner_cycle_floor(&ctx, store.clone(), &mut cycle_info, &mut revision, 7, 12).await);
        assert_eq!(cycle_info.next, 12);
        assert_eq!(revision, DataUsageCacheRevision::Missing);
        assert!(
            !persist_required_scanner_cycle_floor(
                &ctx,
                store.clone(),
                &mut CurrentCycle {
                    current: 12,
                    next: 12,
                    ..Default::default()
                },
                &mut revision,
                7,
                u64::MAX,
            )
            .await
        );
        assert!(read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await.is_err());

        global_metrics().set_cycle(None).await;
    }

    #[test]
    fn scanner_cycle_state_decodes_legacy_and_fenced_formats() {
        let cycle = CurrentCycle {
            current: 12,
            next: 13,
            cycle_completed: vec![],
            started: Utc::now(),
        };
        let mut legacy = cycle.next.to_le_bytes().to_vec();
        legacy.extend(cycle.marshal().expect("legacy cycle state should encode"));

        let (legacy_cycle, legacy_epoch) =
            decode_scanner_cycle_state(&legacy).expect("legacy cycle state should remain readable");
        assert_eq!(legacy_cycle.next, 13);
        assert_eq!(legacy_epoch, 0);

        let fenced = encode_scanner_cycle_state(&cycle, 7).expect("fenced cycle state should encode");
        let (fenced_cycle, fenced_epoch) = decode_scanner_cycle_state(&fenced).expect("fenced cycle state should decode");
        assert_eq!(fenced_cycle.next, 13);
        assert_eq!(fenced_epoch, 7);
    }

    #[test]
    fn scanner_startup_fails_closed_on_nonempty_corrupt_cycle_state() {
        assert_eq!(
            decode_scanner_cycle_state_for_startup(&[])
                .expect("missing cycle state should use defaults")
                .1,
            0
        );
        assert!(decode_scanner_cycle_state_for_startup(&[1]).is_err());

        let mut corrupt_fenced = 13_u64.to_le_bytes().to_vec();
        corrupt_fenced.extend_from_slice(SCANNER_CYCLE_STATE_MAGIC);
        corrupt_fenced.extend_from_slice(&7_u64.to_le_bytes());
        corrupt_fenced.extend_from_slice(b"not-msgpack");
        assert!(decode_scanner_cycle_state_for_startup(&corrupt_fenced).is_err());
        assert!(decode_scanner_cycle_state_for_startup(&u64::MAX.to_le_bytes()).is_err());

        let exhausted = CurrentCycle {
            next: u64::MAX,
            ..Default::default()
        };
        assert!(encode_scanner_cycle_state(&exhausted, 7).is_err());
    }

    #[tokio::test]
    async fn scanner_startup_uses_primary_and_backup_usage_floor() {
        let store = Arc::new(MemoryConfigStore::default());
        let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
        for (path, epoch, cycle) in [(DATA_USAGE_OBJ_NAME_PATH.as_str(), 8, 100), (backup_path.as_str(), 11, 103)] {
            store.objects.lock().await.insert(
                memory_config_key(RUSTFS_META_BUCKET, path),
                serde_json::to_vec(&DataUsageInfo {
                    scanner_epoch: Some(epoch),
                    scanner_cycle: Some(cycle),
                    ..Default::default()
                })
                .expect("usage snapshot should encode"),
            );
        }

        let floor = persisted_usage_floor(store).await.expect("usage floor should load");
        assert_eq!(
            floor,
            PersistedUsageFloor {
                next_cycle: 104,
                leader_epoch: 11,
            }
        );

        let mut cycle = CurrentCycle::default();
        let mut epoch = 0;
        apply_persisted_usage_floor(&mut cycle, &mut epoch, floor);
        assert_eq!(cycle.next, 104);
        assert_eq!(epoch, 11);
    }

    #[tokio::test]
    async fn scanner_usage_floor_fails_closed_on_corrupt_or_exhausted_usage_state() {
        let store = Arc::new(MemoryConfigStore::default());
        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
            b"not-json".to_vec(),
        );

        assert!(persisted_usage_floor(store.clone()).await.is_err());

        store.objects.lock().await.insert(
            memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()),
            serde_json::to_vec(&DataUsageInfo {
                scanner_cycle: Some(u64::MAX - 1),
                ..Default::default()
            })
            .expect("usage snapshot should encode"),
        );
        assert!(persisted_usage_floor(store).await.is_err());
    }

    #[test]
    fn scanner_cycle_advance_fails_before_reserved_exhausted_value() {
        let mut cycle = CurrentCycle {
            next: u64::MAX - 2,
            ..Default::default()
        };
        advance_scanner_cycle(&mut cycle).expect("last persistable scanner cycle should remain valid");
        assert_eq!(cycle.next, u64::MAX - 1);
        assert!(advance_scanner_cycle(&mut cycle).is_err());
        assert_eq!(cycle.next, u64::MAX - 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_finalize_partial_scan_cycle_reports_persist_failure() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
        store.fail_put_number.lock().await.insert(key, 1);
        let mut revision = DataUsageCacheRevision::Missing;
        let mut cycle_info = CurrentCycle {
            current: 12,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };

        assert!(!finalize_partial_scan_cycle(&ctx, store, &mut cycle_info, &mut revision, 1).await);
        assert_eq!(cycle_info.next, 13);
        assert_eq!(cycle_info.current, 0);
        assert_eq!(revision, DataUsageCacheRevision::Missing);

        global_metrics().set_cycle(None).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_persist_scanner_cycle_state_reconciles_newer_winner() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let mut initial_revision = DataUsageCacheRevision::Missing;
        let mut initial = CurrentCycle {
            current: 0,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };
        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut initial, &mut initial_revision, 1).await);

        let mut current_revision = initial_revision.clone();
        let mut stale_revision = initial_revision;
        let mut current = CurrentCycle {
            next: 14,
            ..initial.clone()
        };
        let mut stale = CurrentCycle { next: 13, ..initial };

        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut current, &mut current_revision, 1).await);
        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut stale, &mut stale_revision, 1).await);

        let buf = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("new leader cycle state should remain persisted");
        let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("persisted cycle state should decode");
        assert_eq!(decoded.next, 14);
        assert_eq!(epoch, 1);
        assert_eq!(stale.next, 14);
        assert!(matches!(current_revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-2"));
        assert!(matches!(stale_revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-2"));

        global_metrics().set_cycle(None).await;
    }

    #[tokio::test]
    async fn test_persist_scanner_cycle_state_retries_after_stale_winner() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let mut initial_revision = DataUsageCacheRevision::Missing;
        let mut initial = CurrentCycle {
            current: 0,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };
        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut initial, &mut initial_revision, 1).await);

        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
        let stale = CurrentCycle {
            next: 13,
            ..initial.clone()
        };
        let stale_buf = encode_scanner_cycle_state(&stale, 1).expect("stale cycle state should encode");
        store.interleaving_puts.lock().await.insert(key, (2, stale_buf));

        let mut current = CurrentCycle { next: 14, ..initial };
        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut current, &mut initial_revision, 1).await);

        let buf = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("newer cycle state should replace the stale conflict winner");
        let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("persisted cycle state should decode");
        assert_eq!(decoded.next, 14);
        assert_eq!(epoch, 1);
        assert_eq!(current.next, 14);
        assert!(matches!(initial_revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-3"));
    }

    #[tokio::test]
    async fn test_persist_scanner_cycle_state_stops_retry_after_leader_fence() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let mut revision = DataUsageCacheRevision::Missing;
        let mut initial = CurrentCycle {
            current: 0,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };
        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut initial, &mut revision, 1).await);

        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
        let replacement = CurrentCycle {
            next: 13,
            ..initial.clone()
        };
        let replacement_buf = encode_scanner_cycle_state(&replacement, 2).expect("replacement cycle state should encode");
        store.interleaving_puts.lock().await.insert(key.clone(), (2, replacement_buf));
        store
            .cancel_after_interleaving_puts
            .lock()
            .await
            .insert(key.clone(), ctx.clone());

        let mut stale_leader = CurrentCycle { next: 14, ..initial };
        assert!(!persist_scanner_cycle_state(&ctx, store.clone(), &mut stale_leader, &mut revision, 1).await);

        let buf = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("replacement leader cycle state should remain persisted");
        let (decoded, epoch) = decode_scanner_cycle_state(&buf).expect("persisted cycle state should decode");
        assert_eq!(decoded.next, 13);
        assert_eq!(epoch, 2);
        assert_eq!(stale_leader.next, 14);
        assert!(matches!(revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-2"));
        assert_eq!(store.put_counts.lock().await.get(&key), Some(&2));
    }

    #[tokio::test]
    async fn test_leadership_claim_preserves_usage_epoch_floor_across_old_epoch_conflict() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let mut revision = DataUsageCacheRevision::Missing;
        let mut cycle = CurrentCycle {
            current: 0,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };
        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 1).await);

        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
        let old_epoch_commit = CurrentCycle {
            next: 14,
            ..cycle.clone()
        };
        store.interleaving_puts.lock().await.insert(
            key.clone(),
            (
                2,
                encode_scanner_cycle_state(&old_epoch_commit, 1).expect("old-epoch cycle state should encode"),
            ),
        );

        let mut persisted_epoch = 8;
        assert!(claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch,).await);

        let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("new leadership claim should remain persisted");
        let (claimed_cycle, claimed_epoch) = decode_scanner_cycle_state(&state).expect("claimed cycle state should decode");
        assert_eq!(claimed_cycle.next, 14);
        assert_eq!(claimed_epoch, 9);
        assert_eq!(persisted_epoch, 9);
        assert_eq!(store.put_counts.lock().await.get(&key), Some(&3));
    }

    #[tokio::test]
    async fn test_leadership_claim_confirms_commit_after_returned_error() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
        let usage_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        store.error_after_commit_put_number.lock().await.insert(key.clone(), 1);
        store.error_after_commit_put_number.lock().await.insert(usage_key.clone(), 1);
        let mut revision = DataUsageCacheRevision::Missing;
        let mut cycle = CurrentCycle {
            current: 0,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };
        let mut persisted_epoch = 0;

        assert!(claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch).await);

        let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("ambiguous leadership claim should be durable");
        let (claimed_cycle, claimed_epoch) = decode_scanner_cycle_state(&state).expect("claimed cycle state should decode");
        assert_eq!(claimed_cycle.next, 12);
        assert_eq!(claimed_epoch, 1);
        assert_eq!(persisted_epoch, 1);
        assert!(matches!(revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-1"));
        assert_eq!(store.put_counts.lock().await.get(&key), Some(&1));
        let usage = read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("ambiguous usage epoch fence should be durable");
        assert_eq!(
            serde_json::from_slice::<DataUsageInfo>(&usage)
                .expect("usage epoch fence should decode")
                .scanner_epoch,
            Some(1)
        );
        assert_eq!(store.put_counts.lock().await.get(&usage_key), Some(&1));
    }

    #[tokio::test]
    async fn test_leadership_claim_usage_fence_rejects_old_inflight_writer() {
        let store = Arc::new(MemoryConfigStore::default());
        let usage_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let mut old_usage = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            scanner_epoch: Some(4),
            scanner_cycle: Some(11),
            ..Default::default()
        };
        old_usage.buckets_usage.insert(
            "bucket-a".to_string(),
            rustfs_data_usage::BucketUsageInfo {
                objects_count: 2,
                size: 84,
                ..Default::default()
            },
        );
        old_usage.buckets_count = 1;
        old_usage.calculate_totals();
        let old_data = serde_json::to_vec(&old_usage).expect("old usage snapshot should encode");
        store.objects.lock().await.insert(usage_key.clone(), old_data.clone());
        store.revisions.lock().await.insert(usage_key, 1);

        let ctx = CancellationToken::new();
        let mut revision = DataUsageCacheRevision::Missing;
        let mut cycle = CurrentCycle {
            next: 12,
            started: Utc::now(),
            ..Default::default()
        };
        let mut persisted_epoch = 4;
        assert!(claim_scanner_leadership(&ctx, store.clone(), &mut cycle, &mut revision, &mut persisted_epoch).await);

        let (fenced_data, fenced_revision) = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("fenced usage snapshot should load");
        let fenced = serde_json::from_slice::<DataUsageInfo>(fenced_data.as_deref().expect("fenced usage snapshot should exist"))
            .expect("fenced usage snapshot should decode");
        assert_eq!(fenced.scanner_epoch, Some(5));
        assert_eq!(fenced.objects_total_count, 2);
        assert_eq!(fenced.buckets_usage.get("bucket-a").map(|usage| usage.size), Some(84));
        assert!(matches!(fenced_revision, DataUsageCacheRevision::Etag(ref etag) if etag == "memory-2"));

        let stale_save = save_config_with_preconditions(
            store,
            DATA_USAGE_OBJ_NAME_PATH.as_str(),
            old_data,
            DataUsageCacheRevision::Etag("memory-1".to_string()).preconditions(),
        )
        .await;
        assert!(matches!(stale_save, Err(EcstoreError::PreconditionFailed)));
    }

    #[tokio::test]
    async fn test_successful_old_epoch_commit_is_fenced_after_cancellation() {
        let store = Arc::new(MemoryConfigStore::default());
        let ctx = CancellationToken::new();
        let mut revision = DataUsageCacheRevision::Missing;
        let mut cycle = CurrentCycle {
            current: 0,
            next: 12,
            cycle_completed: vec![],
            started: Utc::now(),
        };
        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 1).await);

        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_BLOOM_NAME_PATH.as_str());
        store
            .cancel_after_successful_puts
            .lock()
            .await
            .insert(key.clone(), (2, ctx.clone()));
        cycle.next = 14;
        assert!(!persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 1).await);

        let (persisted, persisted_revision) = read_config_with_revision(store.clone(), DATA_USAGE_BLOOM_NAME_PATH.as_str())
            .await
            .expect("committed old-epoch state should load");
        let mut replacement_cycle = decode_scanner_cycle_state(
            persisted
                .as_deref()
                .expect("old-epoch state should have committed before cancellation"),
        )
        .expect("old-epoch state should decode")
        .0;
        let mut replacement_revision = persisted_revision;
        let mut replacement_epoch = 1;
        let replacement_ctx = CancellationToken::new();
        assert!(
            claim_scanner_leadership(
                &replacement_ctx,
                store.clone(),
                &mut replacement_cycle,
                &mut replacement_revision,
                &mut replacement_epoch,
            )
            .await
        );

        let state = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
            .await
            .expect("replacement leadership claim should persist");
        let (claimed_cycle, claimed_epoch) = decode_scanner_cycle_state(&state).expect("replacement cycle state should decode");
        assert_eq!(claimed_cycle.next, 14);
        assert_eq!(claimed_epoch, 2);
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_preserves_newer_snapshot() {
        let store = Arc::new(MemoryConfigStore::default());
        let (sender, receiver) = mpsc::channel(2);
        let ctx = CancellationToken::new();

        let newer = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            buckets_count: 2,
            ..Default::default()
        };
        let older = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
            buckets_count: 1,
            ..Default::default()
        };

        sender.send(newer).await.expect("newer usage snapshot should enqueue");
        sender.send(older).await.expect("older usage snapshot should enqueue");
        drop(sender);

        let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

        let objects = store.objects.lock().await;
        let saved = objects
            .get(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()))
            .expect("data usage config should be saved");
        let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");

        assert_eq!(saved.buckets_count, 2);
        assert_eq!(saved.last_update, Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)));
        assert_eq!(outcome, DataUsagePersistOutcome::Current);
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_fences_interleaving_newer_writer() {
        let store = Arc::new(MemoryConfigStore::default());
        let (sender, receiver) = mpsc::channel(1);
        let ctx = CancellationToken::new();
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let newer = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            buckets_count: 2,
            ..Default::default()
        };
        let stale = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
            buckets_count: 1,
            ..Default::default()
        };
        store
            .interleaving_puts
            .lock()
            .await
            .insert(key.clone(), (1, serde_json::to_vec(&newer).expect("newer usage snapshot should encode")));

        sender.send(stale).await.expect("stale usage snapshot should enqueue");
        drop(sender);

        let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

        let objects = store.objects.lock().await;
        let saved = objects
            .get(&key)
            .expect("interleaving newer usage snapshot should remain saved");
        let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");
        assert_eq!(saved.buckets_count, 2);
        assert_eq!(saved.last_update, newer.last_update);
        assert_eq!(outcome, DataUsagePersistOutcome::Current);
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_does_not_resurrect_deleted_bucket_after_conflict() {
        let store = Arc::new(MemoryConfigStore::default());
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let mut initial = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            scanner_epoch: Some(8),
            scanner_cycle: Some(12),
            ..Default::default()
        };
        initial.buckets_usage.insert(
            "bucket-a".to_string(),
            rustfs_data_usage::BucketUsageInfo {
                objects_count: 2,
                size: 84,
                ..Default::default()
            },
        );
        initial.bucket_sizes.insert("bucket-a".to_string(), 84);
        initial.buckets_count = 1;
        initial.calculate_totals();
        let initial_data = serde_json::to_vec(&initial).expect("initial usage snapshot should encode");
        store.objects.lock().await.insert(key.clone(), initial_data.clone());
        store.revisions.lock().await.insert(key.clone(), 1);

        let mut deleted = initial.clone();
        deleted.buckets_usage.clear();
        deleted.bucket_sizes.clear();
        deleted.buckets_count = 0;
        deleted.calculate_totals();
        store
            .interleaving_puts
            .lock()
            .await
            .insert(key.clone(), (1, serde_json::to_vec(&deleted).expect("deleted snapshot should encode")));

        let mut incoming = initial;
        incoming.last_update = Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(30));
        incoming.scanner_cycle = Some(13);
        let (sender, receiver) = mpsc::channel(1);
        sender.send(incoming).await.expect("stale scanner snapshot should enqueue");
        drop(sender);

        let outcome = store_data_usage_in_backend_with_outcome_for_epoch_and_baseline(
            CancellationToken::new(),
            store.clone(),
            receiver,
            Some(8),
            Some(DataUsagePersistBaseline {
                data: Some(Bytes::from(initial_data)),
                revision: DataUsageCacheRevision::Etag("memory-1".to_string()),
            }),
        )
        .await;

        assert_eq!(outcome, DataUsagePersistOutcome::Current);
        let saved = store
            .objects
            .lock()
            .await
            .get(&key)
            .cloned()
            .expect("deleted usage snapshot should remain");
        let saved = serde_json::from_slice::<DataUsageInfo>(&saved).expect("deleted usage snapshot should decode");
        assert!(!saved.buckets_usage.contains_key("bucket-a"));
        assert!(!saved.bucket_sizes.contains_key("bucket-a"));
        assert_eq!(store.put_counts.lock().await.get(&key), Some(&1));
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_does_not_resurrect_deleted_bucket_in_backup() {
        let store = Arc::new(MemoryConfigStore::default());
        let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
        let backup_key = memory_config_key(RUSTFS_META_BUCKET, &backup_path);
        let deleted = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            scanner_epoch: Some(8),
            scanner_cycle: Some(1),
            ..Default::default()
        };
        store.objects.lock().await.insert(
            backup_key.clone(),
            serde_json::to_vec(&deleted).expect("deleted backup snapshot should encode"),
        );
        store.revisions.lock().await.insert(backup_key.clone(), 1);

        let (sender, receiver) = mpsc::channel(11);
        for cycle in 2_u64..=12 {
            let mut incoming = DataUsageInfo {
                last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20 + cycle)),
                scanner_epoch: Some(8),
                scanner_cycle: Some(cycle),
                ..Default::default()
            };
            incoming.buckets_usage.insert(
                "bucket-a".to_string(),
                rustfs_data_usage::BucketUsageInfo {
                    objects_count: 2,
                    size: 84,
                    ..Default::default()
                },
            );
            incoming.bucket_sizes.insert("bucket-a".to_string(), 84);
            incoming.buckets_count = 1;
            incoming.calculate_totals();
            sender.send(incoming).await.expect("usage snapshot should enqueue");
        }
        drop(sender);

        assert_eq!(
            store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await,
            DataUsagePersistOutcome::Saved
        );

        let saved = store
            .objects
            .lock()
            .await
            .get(&backup_key)
            .cloned()
            .expect("deleted backup snapshot should remain");
        let saved = serde_json::from_slice::<DataUsageInfo>(&saved).expect("backup snapshot should decode");
        assert!(!saved.buckets_usage.contains_key("bucket-a"));
        assert!(!saved.bucket_sizes.contains_key("bucket-a"));
        assert_eq!(store.put_counts.lock().await.get(&backup_key), None);
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_retries_after_stale_interleaving_writer() {
        let store = Arc::new(MemoryConfigStore::default());
        let (sender, receiver) = mpsc::channel(1);
        let ctx = CancellationToken::new();
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let initial = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
            buckets_count: 1,
            ..Default::default()
        };
        let stale_winner = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            buckets_count: 2,
            ..Default::default()
        };
        let current = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(30)),
            buckets_count: 3,
            ..Default::default()
        };
        store
            .objects
            .lock()
            .await
            .insert(key.clone(), serde_json::to_vec(&initial).expect("initial usage snapshot should encode"));
        store.revisions.lock().await.insert(key.clone(), 1);
        store.interleaving_puts.lock().await.insert(
            key.clone(),
            (1, serde_json::to_vec(&stale_winner).expect("stale usage snapshot should encode")),
        );

        sender
            .send(current.clone())
            .await
            .expect("current usage snapshot should enqueue");
        drop(sender);

        let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

        let objects = store.objects.lock().await;
        let saved = objects
            .get(&key)
            .expect("current usage snapshot should replace the stale conflict winner");
        let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");
        assert_eq!(saved.buckets_count, 3);
        assert_eq!(saved.last_update, current.last_update);
        assert_eq!(outcome, DataUsagePersistOutcome::Saved);
        drop(objects);
        assert_eq!(store.put_counts.lock().await.get(&key), Some(&2));
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_rejects_untimestamped_stale_snapshot() {
        let store = Arc::new(MemoryConfigStore::default());
        let (sender, receiver) = mpsc::channel(2);
        let ctx = CancellationToken::new();

        let timestamped = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            buckets_count: 2,
            ..Default::default()
        };
        let untimestamped = DataUsageInfo {
            last_update: None,
            buckets_count: 1,
            ..Default::default()
        };

        sender
            .send(timestamped)
            .await
            .expect("timestamped usage snapshot should enqueue");
        sender
            .send(untimestamped)
            .await
            .expect("untimestamped usage snapshot should enqueue");
        drop(sender);

        let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

        let objects = store.objects.lock().await;
        let saved = objects
            .get(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()))
            .expect("data usage config should be saved");
        let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");

        assert_eq!(saved.buckets_count, 2);
        assert_eq!(saved.last_update, Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)));
        assert_eq!(outcome, DataUsagePersistOutcome::Current);
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_recognizes_already_durable_snapshot() {
        let store = Arc::new(MemoryConfigStore::default());
        let (sender, receiver) = mpsc::channel(1);
        let ctx = CancellationToken::new();
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let snapshot = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            scanner_cycle: Some(12),
            buckets_count: 2,
            ..Default::default()
        };
        store
            .objects
            .lock()
            .await
            .insert(key.clone(), serde_json::to_vec(&snapshot).expect("durable usage snapshot should encode"));
        store.revisions.lock().await.insert(key.clone(), 1);

        sender
            .send(snapshot)
            .await
            .expect("ambiguous committed snapshot should enqueue");
        drop(sender);

        let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

        assert_eq!(outcome, DataUsagePersistOutcome::AlreadyDurable);
        assert_eq!(store.put_counts.lock().await.get(&key), None);
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_advances_past_changed_same_epoch_cycle() {
        let store = Arc::new(MemoryConfigStore::default());
        let (sender, receiver) = mpsc::channel(1);
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let durable = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
            scanner_epoch: Some(8),
            scanner_cycle: Some(12),
            buckets_count: 2,
            ..Default::default()
        };
        store
            .objects
            .lock()
            .await
            .insert(key.clone(), serde_json::to_vec(&durable).expect("durable usage snapshot should encode"));
        store.revisions.lock().await.insert(key.clone(), 1);

        sender
            .send(DataUsageInfo {
                last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(30)),
                scanner_epoch: Some(8),
                scanner_cycle: Some(12),
                buckets_count: 3,
                ..Default::default()
            })
            .await
            .expect("changed retry snapshot should enqueue");
        drop(sender);

        let outcome =
            store_data_usage_in_backend_with_outcome_for_epoch(CancellationToken::new(), store.clone(), receiver, Some(8)).await;

        assert_eq!(outcome, DataUsagePersistOutcome::PriorCycleDurable);
        assert_eq!(store.put_counts.lock().await.get(&key), None);
        let saved = store
            .objects
            .lock()
            .await
            .get(&key)
            .cloned()
            .expect("first snapshot should remain durable");
        assert_eq!(
            serde_json::from_slice::<DataUsageInfo>(&saved)
                .expect("durable usage snapshot should decode")
                .buckets_count,
            2
        );
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_orders_scanner_cycles_before_wall_clock() {
        let store = Arc::new(MemoryConfigStore::default());
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let existing = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(200)),
            scanner_cycle: Some(12),
            buckets_count: 2,
            ..Default::default()
        };
        store
            .objects
            .lock()
            .await
            .insert(key.clone(), serde_json::to_vec(&existing).expect("existing usage snapshot should encode"));
        store.revisions.lock().await.insert(key.clone(), 1);

        let (older_sender, older_receiver) = mpsc::channel(1);
        let older = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(300)),
            scanner_cycle: Some(11),
            buckets_count: 1,
            ..Default::default()
        };
        older_sender.send(older).await.expect("older-cycle snapshot should enqueue");
        drop(older_sender);
        assert_eq!(
            store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), older_receiver).await,
            DataUsagePersistOutcome::Current
        );

        let (newer_sender, newer_receiver) = mpsc::channel(1);
        let newer = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(100)),
            scanner_cycle: Some(13),
            buckets_count: 3,
            ..Default::default()
        };
        newer_sender
            .send(newer.clone())
            .await
            .expect("newer-cycle snapshot should enqueue");
        drop(newer_sender);
        assert_eq!(
            store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), newer_receiver).await,
            DataUsagePersistOutcome::Saved
        );

        let saved = store
            .objects
            .lock()
            .await
            .get(&key)
            .cloned()
            .expect("newer scanner cycle should be persisted");
        assert_eq!(
            serde_json::from_slice::<DataUsageInfo>(&saved)
                .expect("persisted usage snapshot should decode")
                .scanner_cycle,
            Some(13)
        );
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_orders_leader_epochs_before_cycles() {
        let store = Arc::new(MemoryConfigStore::default());
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let existing = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(200)),
            scanner_epoch: Some(8),
            scanner_cycle: Some(12),
            buckets_count: 2,
            ..Default::default()
        };
        store
            .objects
            .lock()
            .await
            .insert(key.clone(), serde_json::to_vec(&existing).expect("existing usage snapshot should encode"));
        store.revisions.lock().await.insert(key.clone(), 1);

        let (older_sender, older_receiver) = mpsc::channel(1);
        older_sender
            .send(DataUsageInfo {
                last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(300)),
                scanner_epoch: Some(7),
                scanner_cycle: Some(99),
                buckets_count: 1,
                ..Default::default()
            })
            .await
            .expect("old-epoch snapshot should enqueue");
        drop(older_sender);
        assert_eq!(
            store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), older_receiver).await,
            DataUsagePersistOutcome::Current
        );

        let (newer_sender, newer_receiver) = mpsc::channel(1);
        newer_sender
            .send(DataUsageInfo {
                last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(100)),
                scanner_epoch: None,
                scanner_cycle: Some(1),
                buckets_count: 3,
                ..Default::default()
            })
            .await
            .expect("replacement-epoch snapshot should enqueue");
        drop(newer_sender);
        assert_eq!(
            store_data_usage_in_backend_with_outcome_for_epoch(CancellationToken::new(), store.clone(), newer_receiver, Some(9),)
                .await,
            DataUsagePersistOutcome::Saved
        );

        let saved = store
            .objects
            .lock()
            .await
            .get(&key)
            .cloned()
            .expect("replacement leader snapshot should persist");
        let saved = serde_json::from_slice::<DataUsageInfo>(&saved).expect("persisted usage snapshot should decode");
        assert_eq!(saved.scanner_epoch, Some(9));
        assert_eq!(saved.scanner_cycle, Some(1));
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_keeps_first_same_cycle_snapshot() {
        let store = Arc::new(MemoryConfigStore::default());
        let key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let existing = DataUsageInfo {
            last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(100)),
            scanner_cycle: Some(12),
            buckets_count: 2,
            ..Default::default()
        };
        store
            .objects
            .lock()
            .await
            .insert(key.clone(), serde_json::to_vec(&existing).expect("existing usage snapshot should encode"));
        store.revisions.lock().await.insert(key.clone(), 1);

        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(DataUsageInfo {
                last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(300)),
                scanner_cycle: Some(12),
                buckets_count: 3,
                ..Default::default()
            })
            .await
            .expect("conflicting same-cycle snapshot should enqueue");
        drop(sender);

        assert_eq!(
            store_data_usage_in_backend_with_outcome(CancellationToken::new(), store.clone(), receiver).await,
            DataUsagePersistOutcome::Current
        );
        let saved = store
            .objects
            .lock()
            .await
            .get(&key)
            .cloned()
            .expect("first same-cycle snapshot should remain persisted");
        assert_eq!(
            serde_json::from_slice::<DataUsageInfo>(&saved)
                .expect("persisted usage snapshot should decode")
                .buckets_count,
            2
        );
    }

    fn usage_with_last_update(last_update: Option<std::time::SystemTime>) -> DataUsageInfo {
        DataUsageInfo {
            last_update,
            ..Default::default()
        }
    }

    #[test]
    fn test_stale_data_usage_update_reason_allows_newer_incoming() {
        let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let incoming = usage_with_last_update(Some(now));
        let existing = usage_with_last_update(Some(now - Duration::from_secs(60)));
        assert_eq!(stale_data_usage_update_reason(&incoming, &existing, now), None);
    }

    #[test]
    fn test_stale_data_usage_update_reason_skips_older_or_equal_incoming() {
        let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let existing = usage_with_last_update(Some(now - Duration::from_secs(60)));

        let older = usage_with_last_update(Some(now - Duration::from_secs(120)));
        assert_eq!(stale_data_usage_update_reason(&older, &existing, now), Some("older_or_equal_last_update"));

        let equal = usage_with_last_update(existing.last_update);
        assert_eq!(stale_data_usage_update_reason(&equal, &existing, now), Some("older_or_equal_last_update"));
    }

    #[test]
    fn test_stale_data_usage_update_reason_allows_save_when_existing_is_future_dated() {
        // Existing snapshot timestamp beyond the clock tolerance is untrustworthy
        // (clock step-back / slower-clock leader): the save must be allowed even
        // though incoming <= existing, otherwise usage stats freeze forever.
        let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let existing =
            usage_with_last_update(Some(now + rustfs_data_usage::USAGE_LAST_UPDATE_FUTURE_TOLERANCE + Duration::from_secs(1)));
        let incoming = usage_with_last_update(Some(now));
        assert_eq!(stale_data_usage_update_reason(&incoming, &existing, now), None);
    }

    #[test]
    fn test_stale_data_usage_update_reason_skips_at_exact_tolerance_boundary() {
        // Exactly at now + tolerance is still within the trusted window.
        let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let existing = usage_with_last_update(Some(now + rustfs_data_usage::USAGE_LAST_UPDATE_FUTURE_TOLERANCE));
        let incoming = usage_with_last_update(Some(now));
        assert_eq!(
            stale_data_usage_update_reason(&incoming, &existing, now),
            Some("older_or_equal_last_update")
        );
    }

    #[test]
    fn test_stale_data_usage_update_reason_preserves_none_handling() {
        let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

        let incoming_none = usage_with_last_update(None);
        let existing_some = usage_with_last_update(Some(now - Duration::from_secs(60)));
        assert_eq!(
            stale_data_usage_update_reason(&incoming_none, &existing_some, now),
            Some("missing_incoming_last_update")
        );

        let incoming_some = usage_with_last_update(Some(now));
        let existing_none = usage_with_last_update(None);
        assert_eq!(stale_data_usage_update_reason(&incoming_some, &existing_none, now), None);

        let both_none = usage_with_last_update(None);
        assert_eq!(stale_data_usage_update_reason(&both_none, &usage_with_last_update(None), now), None);
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_keeps_backup_when_primary_save_fails() {
        let store = Arc::new(MemoryConfigStore::default());
        let (sender, receiver) = mpsc::channel(11);
        let ctx = CancellationToken::new();

        let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
        let main_key = memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str());
        let backup_key = memory_config_key(RUSTFS_META_BUCKET, &backup_path);
        let old_backup = b"old-backup".to_vec();

        store.objects.lock().await.insert(backup_key.clone(), old_backup.clone());
        store.fail_put_number.lock().await.insert(main_key.clone(), 11);

        for idx in 1_u64..=11 {
            sender
                .send(DataUsageInfo {
                    last_update: Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(idx)),
                    buckets_count: idx,
                    ..Default::default()
                })
                .await
                .expect("usage snapshot should enqueue");
        }
        drop(sender);

        let outcome = store_data_usage_in_backend_with_outcome(ctx, store.clone(), receiver).await;

        let objects = store.objects.lock().await;
        assert_eq!(
            objects.get(&backup_key),
            Some(&old_backup),
            "primary save failure must not overwrite the previous backup"
        );
        let saved = objects
            .get(&main_key)
            .expect("last successful primary usage snapshot should remain saved");
        let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");
        assert_eq!(saved.buckets_count, 10);
        assert_eq!(saved.last_update, Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10)));
        assert_eq!(outcome, DataUsagePersistOutcome::Failed);
    }

    #[tokio::test]
    async fn test_store_data_usage_in_backend_reports_missing_snapshot() {
        let store = Arc::new(MemoryConfigStore::default());
        let (sender, receiver) = mpsc::channel(1);
        let ctx = CancellationToken::new();
        drop(sender);

        let outcome = store_data_usage_in_backend_with_outcome(ctx, store, receiver).await;

        assert_eq!(outcome, DataUsagePersistOutcome::NoUpdate);
    }

    #[test]
    fn test_scanner_cycle_completion_prioritizes_persist_failure() {
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Incomplete, DataUsagePersistOutcome::Failed, true, true),
            ScannerCycleOutcome::Failed
        );
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Incomplete, DataUsagePersistOutcome::NoUpdate, true, true),
            ScannerCycleOutcome::Failed
        );
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Incomplete, DataUsagePersistOutcome::Saved, true, false),
            ScannerCycleOutcome::Partial
        );
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Incomplete, DataUsagePersistOutcome::Saved, true, true),
            ScannerCycleOutcome::Failed
        );
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::Saved, true, false),
            ScannerCycleOutcome::Completed
        );
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::AlreadyDurable, true, false,),
            ScannerCycleOutcome::Completed
        );
        assert_eq!(
            scanner_cycle_completion_outcome(
                ScannerCycleStatus::Complete,
                DataUsagePersistOutcome::PriorCycleDurable,
                true,
                false,
            ),
            ScannerCycleOutcome::Completed
        );
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::Current, false, false),
            ScannerCycleOutcome::Completed
        );
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::Current, true, false),
            ScannerCycleOutcome::Failed
        );
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Complete, DataUsagePersistOutcome::NoUpdate, false, false),
            ScannerCycleOutcome::Failed
        );
        for persist_outcome in [
            DataUsagePersistOutcome::NoUpdate,
            DataUsagePersistOutcome::Current,
            DataUsagePersistOutcome::Saved,
        ] {
            assert_eq!(
                scanner_cycle_completion_outcome(ScannerCycleStatus::Superseded, persist_outcome, true, false),
                ScannerCycleOutcome::Superseded
            );
        }
        assert_eq!(
            scanner_cycle_completion_outcome(ScannerCycleStatus::Superseded, DataUsagePersistOutcome::Saved, true, true),
            ScannerCycleOutcome::Failed
        );
    }

    #[test]
    #[serial]
    fn finalizing_a_saved_cycle_acknowledges_its_exact_dirty_snapshot() {
        crate::scanner_io::clear_dirty_usage_bucket("photos");
        crate::scanner_io::record_dirty_usage_bucket("photos");
        let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

        let unsaved = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot.clone()));
        let (outcome, _) = finalize_scanner_cycle_result(unsaved, DataUsagePersistOutcome::NoUpdate);
        assert_eq!(outcome, ScannerCycleOutcome::Failed);
        assert!(crate::scanner_io::dirty_usage_buckets_pending());

        let saved = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot));
        let (outcome, _) = finalize_scanner_cycle_result(saved, DataUsagePersistOutcome::Saved);
        assert_eq!(outcome, ScannerCycleOutcome::Completed);
        assert!(!crate::scanner_io::dirty_usage_buckets_pending());
    }

    #[test]
    #[serial]
    fn finalizing_an_already_durable_cycle_acknowledges_its_exact_dirty_snapshot() {
        crate::scanner_io::clear_dirty_usage_bucket("photos");
        crate::scanner_io::record_dirty_usage_bucket("photos");
        let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

        let durable = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot));
        let (outcome, _) = finalize_scanner_cycle_result(durable, DataUsagePersistOutcome::AlreadyDurable);

        assert_eq!(outcome, ScannerCycleOutcome::Completed);
        assert!(!crate::scanner_io::dirty_usage_buckets_pending());
    }

    #[test]
    #[serial]
    fn finalizing_a_prior_same_cycle_snapshot_keeps_new_dirty_work_pending() {
        crate::scanner_io::clear_dirty_usage_bucket("photos");
        crate::scanner_io::record_dirty_usage_bucket("photos");
        let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

        let durable = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(dirty_snapshot));
        let (outcome, _) = finalize_scanner_cycle_result(durable, DataUsagePersistOutcome::PriorCycleDurable);

        assert_eq!(outcome, ScannerCycleOutcome::Completed);
        assert!(crate::scanner_io::dirty_usage_buckets_pending());
        crate::scanner_io::clear_dirty_usage_bucket("photos");
    }

    #[test]
    #[serial]
    fn finalizing_a_superseded_cycle_keeps_dirty_work_pending() {
        crate::scanner_io::clear_dirty_usage_bucket("photos");
        crate::scanner_io::record_dirty_usage_bucket("photos");
        let dirty_snapshot = crate::scanner_io::dirty_usage_buckets_for_tests();

        let superseded = crate::scanner_io::ScannerCycleResult::new(ScannerCycleStatus::Superseded, Some(dirty_snapshot));
        let (outcome, _) = finalize_scanner_cycle_result(superseded, DataUsagePersistOutcome::NoUpdate);

        assert_eq!(outcome, ScannerCycleOutcome::Superseded);
        assert!(crate::scanner_io::dirty_usage_buckets_pending());
        crate::scanner_io::clear_dirty_usage_bucket("photos");
    }

    #[test]
    #[serial]
    fn data_usage_persist_wait_covers_cache_retries_and_backup() {
        with_var(rustfs_config::ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("7"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(data_usage_persist_timeout(), Duration::from_millis(31_350));
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[tokio::test]
    async fn data_usage_persist_wait_aborts_when_scanner_is_cancelled() {
        let ctx = CancellationToken::new();
        let mut task = tokio::spawn(async {
            std::future::pending::<()>().await;
            DataUsagePersistOutcome::Saved
        });
        ctx.cancel();

        let result = wait_for_data_usage_persist_task(&ctx, &mut task, Duration::from_secs(60)).await;

        assert!(matches!(result, DataUsagePersistTaskResult::Cancelled));
        assert!(task.is_finished());
    }

    #[tokio::test(start_paused = true)]
    async fn data_usage_persist_wait_aborts_after_timeout() {
        let ctx = CancellationToken::new();
        let mut task = tokio::spawn(async {
            std::future::pending::<()>().await;
            DataUsagePersistOutcome::Saved
        });

        let result = wait_for_data_usage_persist_task(&ctx, &mut task, Duration::from_secs(30)).await;

        assert!(matches!(result, DataUsagePersistTaskResult::TimedOut));
        assert!(task.is_finished());
    }

    #[tokio::test(start_paused = true)]
    async fn maintenance_feature_inspection_preserves_base_cycle_after_timeout() {
        let ctx = CancellationToken::new();

        let result = wait_for_maintenance_feature_inspection(
            &ctx,
            std::future::pending::<ScannerMaintenanceFeatures>(),
            Duration::from_secs(30),
        )
        .await;

        assert_eq!(result, MaintenanceInspectionAttempt::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn stable_maintenance_detection_preserves_base_cycle_after_timeout() {
        let ctx = CancellationToken::new();

        let (features, generation) = detect_stable_scanner_maintenance_features_with(
            &ctx,
            std::future::pending::<ScannerMaintenanceFeatures>,
            Duration::from_secs(30),
        )
        .await
        .expect("timeout should preserve the scanner rather than stop it");

        assert!(features.inspection_failed);
        assert_eq!(generation, scanner_maintenance_generation());
        assert!(!scanner_clean_idle_backoff_enabled(
            true,
            true,
            features,
            &ScannerRuntimeConfig::default()
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn failed_maintenance_inspection_uses_bounded_retry_backoff() {
        let failed = ScannerMaintenanceFeatures {
            inspection_failed: true,
            ..Default::default()
        };
        let mut retry = ScannerMaintenanceInspectionRetry::from_features(failed, Instant::now());

        assert_eq!(retry.retry_interval(), Some(MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL));
        assert!(!retry.retry_due(failed, ScannerCycleWakeReason::Timer, Instant::now()));
        tokio::time::advance(MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL).await;
        assert!(retry.retry_due(failed, ScannerCycleWakeReason::Timer, Instant::now()));
        assert!(!retry.retry_due(failed, ScannerCycleWakeReason::DirtyUsage, Instant::now()));

        retry.record_inspection(failed, Instant::now());
        assert_eq!(
            retry.retry_interval(),
            Some(MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL.saturating_mul(2))
        );
        for _ in 0..8 {
            retry.record_inspection(failed, Instant::now());
        }
        assert_eq!(retry.retry_interval(), Some(MAINTENANCE_FEATURE_INSPECTION_RETRY_MAX_INTERVAL));

        retry.record_inspection(ScannerMaintenanceFeatures::default(), Instant::now());
        assert_eq!(retry, ScannerMaintenanceInspectionRetry::default());
    }

    #[tokio::test]
    async fn maintenance_feature_inspection_stops_on_cancellation() {
        let ctx = CancellationToken::new();
        ctx.cancel();

        let result = wait_for_maintenance_feature_inspection(
            &ctx,
            std::future::pending::<ScannerMaintenanceFeatures>(),
            Duration::from_secs(30),
        )
        .await;

        assert_eq!(result, MaintenanceInspectionAttempt::Cancelled);
    }

    #[test]
    #[serial]
    fn test_cycle_interval_prefers_explicit_cycle_override() {
        with_var(ENV_SCANNER_SPEED, Some("slowest"), || {
            with_var(ENV_SCANNER_CYCLE, Some("42"), || {
                assert_eq!(cycle_interval(), Duration::from_secs(42));
            });
        });
    }

    #[test]
    #[serial]
    fn test_cycle_interval_prefers_explicit_cycle_over_default_cycle() {
        let _guard = ScannerDefaultCycleGuard::set(TEST_DEFAULT_SCANNER_CYCLE_SECS);

        with_var(ENV_SCANNER_CYCLE, Some("42"), || {
            assert_eq!(cycle_interval(), Duration::from_secs(42));
        });
    }

    #[test]
    #[serial]
    fn test_cycle_interval_uses_scanner_default_speed_override_when_unconfigured() {
        let _guard = ScannerDefaultSpeedGuard::set(ScannerSpeed::Slowest);

        with_unset_scanner_timing_env(|| {
            assert_eq!(cycle_interval(), Duration::from_secs(30 * 60));
        });
    }

    #[test]
    #[serial]
    fn test_cycle_interval_prefers_explicit_speed_over_default_speed_override() {
        let _guard = ScannerDefaultSpeedGuard::set(ScannerSpeed::Slowest);

        with_var_unset(ENV_SCANNER_CYCLE, || {
            with_var_unset("MINIO_SCANNER_CYCLE", || {
                with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                    with_var_unset(ENV_SCANNER_START_DELAY_SECS_DEPRECATED, || {
                        with_var(ENV_SCANNER_SPEED, Some("fastest"), || {
                            assert_eq!(cycle_interval(), Duration::from_secs(1));
                        });
                    });
                });
            });
        });
    }

    #[test]
    #[serial]
    fn test_cycle_interval_uses_default_cycle_override_when_unconfigured() {
        let _guard = ScannerDefaultCycleGuard::set(TEST_DEFAULT_SCANNER_CYCLE_SECS);

        with_unset_scanner_timing_env(|| {
            assert_eq!(cycle_interval(), Duration::from_secs(TEST_DEFAULT_SCANNER_CYCLE_SECS));
        });
    }

    #[test]
    fn test_single_disk_default_cycle_uses_speed_based_interval_without_maintenance_features() {
        assert_eq!(single_disk_default_cycle_secs(ScannerMaintenanceFeatures::default()), None);
    }

    #[test]
    fn test_single_disk_default_speed_uses_regular_scanner_default() {
        assert_eq!(single_disk_default_speed(), ScannerSpeed::Default);
    }

    #[test]
    fn test_maintenance_feature_inspection_is_bounded_and_conservative() {
        assert_eq!(maintenance_inspection_decision(1, 1, 1), MaintenanceInspectionDecision::Accept);
        assert_eq!(maintenance_inspection_decision(1, 2, 1), MaintenanceInspectionDecision::Retry);
        assert_eq!(
            maintenance_inspection_decision(1, 2, MAX_MAINTENANCE_FEATURE_INSPECTION_ATTEMPTS),
            MaintenanceInspectionDecision::PreserveBaseCycle
        );
    }

    #[test]
    fn clean_idle_backoff_grows_to_cap() {
        let base_interval = Duration::from_secs(60);
        let max_interval = CLEAN_IDLE_MAX_INTERVAL;
        let mut backoff = ScannerCleanIdleBackoff::default();

        assert_eq!(backoff.effective_interval(base_interval, max_interval, true), Duration::from_secs(60));
        for expected_secs in [
            120, 240, 480, 960, 1_920, 3_840, 7_680, 15_360, 30_720, 61_440, 86_400, 86_400,
        ] {
            backoff.record_cycle(
                base_interval,
                max_interval,
                true,
                ScannerCycleWakeReason::Timer,
                ScannerCycleOutcome::Completed,
                false,
            );
            assert_eq!(
                backoff.effective_interval(base_interval, max_interval, true),
                Duration::from_secs(expected_secs)
            );
        }
    }

    #[test]
    fn scanner_cycle_wait_plan_drives_growth_resets_and_bitrot_cap() {
        let runtime_config = ScannerRuntimeConfig {
            cycle_interval: Duration::from_secs(60),
            bitrot_cycle: None,
            ..Default::default()
        };
        let mut clean_idle_backoff = ScannerCleanIdleBackoff::default();

        let plan = scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, true, std::convert::identity);
        assert_eq!(plan.delay, Duration::from_secs(60));

        for expected in [120, 240] {
            record_scanner_cycle_result(
                &mut clean_idle_backoff,
                &runtime_config,
                true,
                ScannerCycleWakeReason::Timer,
                ScannerCycleOutcome::Completed,
                false,
            );
            let plan = scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, true, std::convert::identity);
            assert_eq!(plan.delay, Duration::from_secs(expected));
        }

        for (wake_reason, outcome, dirty_work_observed) in [
            (ScannerCycleWakeReason::Timer, ScannerCycleOutcome::Completed, true),
            (ScannerCycleWakeReason::Timer, ScannerCycleOutcome::Partial, false),
            (ScannerCycleWakeReason::Timer, ScannerCycleOutcome::Failed, false),
            (ScannerCycleWakeReason::Timer, ScannerCycleOutcome::CompletedWithPendingMaintenance, false),
            (ScannerCycleWakeReason::DirtyUsage, ScannerCycleOutcome::Completed, false),
        ] {
            record_scanner_cycle_result(
                &mut clean_idle_backoff,
                &runtime_config,
                true,
                wake_reason,
                outcome,
                dirty_work_observed,
            );
            let plan = scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, true, std::convert::identity);
            assert_eq!(plan.effective_interval, Duration::from_secs(60));
            assert_eq!(plan.delay, Duration::from_secs(60));

            record_scanner_cycle_result(
                &mut clean_idle_backoff,
                &runtime_config,
                true,
                ScannerCycleWakeReason::Timer,
                ScannerCycleOutcome::Completed,
                false,
            );
        }

        clean_idle_backoff.reset();
        for _ in 0..32 {
            record_scanner_cycle_result(
                &mut clean_idle_backoff,
                &runtime_config,
                true,
                ScannerCycleWakeReason::Timer,
                ScannerCycleOutcome::Completed,
                false,
            );
        }
        let plan = scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, true, |interval| interval.mul_f64(1.1));
        assert_eq!(plan.effective_interval, CLEAN_IDLE_MAX_INTERVAL);
        assert!(plan.delay < CLEAN_IDLE_MAX_INTERVAL);
        assert_eq!(
            plan.delay,
            CLEAN_IDLE_MAX_INTERVAL.saturating_sub(CLEAN_IDLE_MAX_INTERVAL.mul_f64(1.1) - CLEAN_IDLE_MAX_INTERVAL)
        );
    }

    #[test]
    #[serial]
    fn scanner_cycle_schedule_status_reports_effective_backoff() {
        record_scanner_cycle_schedule(Duration::from_millis(86_400_001), true, 2_048);

        let status = scanner_cycle_schedule_status();

        assert_eq!(status.effective_interval_seconds, 86_401);
        assert!(status.clean_idle_backoff_enabled);
        assert_eq!(status.clean_idle_backoff_multiplier, 2_048);

        reset_scanner_cycle_schedule();
        let status = scanner_cycle_schedule_status();
        assert_eq!(status.effective_interval_seconds, 0);
        assert!(!status.clean_idle_backoff_enabled);
        assert_eq!(status.clean_idle_backoff_multiplier, 1);
    }

    #[test]
    fn clean_idle_backoff_resets_for_non_idle_work() {
        let base_interval = Duration::from_secs(60);
        let max_interval = CLEAN_IDLE_MAX_INTERVAL;
        let mut backoff = ScannerCleanIdleBackoff::default();

        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        assert_eq!(backoff.effective_interval(base_interval, max_interval, true), Duration::from_secs(240));

        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::DirtyUsage,
            ScannerCycleOutcome::Completed,
            false,
        );
        assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);

        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Partial,
            false,
        );
        assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);

        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Failed,
            false,
        );
        assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);

        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            true,
        );
        assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);

        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::CompletedWithPendingMaintenance,
            false,
        );
        assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);
    }

    #[test]
    fn test_dirty_work_is_observed_across_cycle_waits() {
        assert!(scanner_cycle_observed_dirty_work(true, 7, 7));
        assert!(scanner_cycle_observed_dirty_work(false, 7, 8));
        assert!(!scanner_cycle_observed_dirty_work(false, 7, 7));
    }

    #[test]
    fn clean_idle_backoff_never_shortens_base_interval() {
        let base_interval = Duration::from_secs(48 * 60 * 60);
        let mut backoff = ScannerCleanIdleBackoff::default();

        backoff.record_cycle(
            base_interval,
            CLEAN_IDLE_MAX_INTERVAL,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );

        assert_eq!(backoff.effective_interval(base_interval, CLEAN_IDLE_MAX_INTERVAL, true), base_interval);
    }

    #[test]
    fn clean_idle_backoff_resets_while_disabled() {
        let base_interval = Duration::from_secs(60);
        let max_interval = CLEAN_IDLE_MAX_INTERVAL;
        let mut backoff = ScannerCleanIdleBackoff::default();

        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        backoff.record_cycle(
            base_interval,
            max_interval,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );
        assert_eq!(backoff.effective_interval(base_interval, max_interval, true), Duration::from_secs(240));

        backoff.record_cycle(
            base_interval,
            max_interval,
            false,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            false,
        );

        assert_eq!(backoff.effective_interval(base_interval, max_interval, false), base_interval);
        assert_eq!(backoff.effective_interval(base_interval, max_interval, true), base_interval);
    }

    #[test]
    fn clean_idle_backoff_policy_preserves_explicit_and_maintenance_cycles() {
        let no_features = ScannerMaintenanceFeatures::default();
        let default_config = ScannerRuntimeConfig::default();
        assert!(scanner_clean_idle_backoff_enabled(true, true, no_features, &default_config));
        assert!(!scanner_clean_idle_backoff_enabled(false, true, no_features, &default_config));
        assert!(!scanner_clean_idle_backoff_enabled(true, false, no_features, &default_config));

        for source in [ScannerRuntimeConfigSource::Env, ScannerRuntimeConfigSource::Config] {
            let mut config = default_config.clone();
            config.cycle_interval_source = source;
            assert!(!scanner_clean_idle_backoff_enabled(true, true, no_features, &config));
        }

        for source in [
            ScannerRuntimeConfigSource::Env,
            ScannerRuntimeConfigSource::Config,
            ScannerRuntimeConfigSource::ScannerCompatConfig,
        ] {
            let mut explicit_bitrot_config = default_config.clone();
            explicit_bitrot_config.bitrot_cycle = Some(Duration::from_secs(60 * 60));
            explicit_bitrot_config.bitrot_cycle_source = source;
            assert!(!scanner_clean_idle_backoff_enabled(true, true, no_features, &explicit_bitrot_config));

            explicit_bitrot_config.bitrot_cycle = None;
            assert!(scanner_clean_idle_backoff_enabled(true, true, no_features, &explicit_bitrot_config));
        }

        for features in [
            ScannerMaintenanceFeatures {
                lifecycle: true,
                ..Default::default()
            },
            ScannerMaintenanceFeatures {
                replication: true,
                ..Default::default()
            },
            ScannerMaintenanceFeatures {
                inspection_failed: true,
                ..Default::default()
            },
        ] {
            assert!(!scanner_clean_idle_backoff_enabled(true, true, features, &default_config));
        }
    }

    #[test]
    fn clean_idle_backoff_requires_activity_probes() {
        let default_config = ScannerRuntimeConfig::default();
        let no_features = ScannerMaintenanceFeatures::default();
        assert!(scanner_activity_probe_required(true, false, no_features, &default_config));
        assert!(!scanner_activity_probe_required(false, false, no_features, &default_config));
        assert!(!scanner_activity_probe_required(true, true, no_features, &default_config));

        let mut explicit_cycle = default_config.clone();
        explicit_cycle.cycle_interval_source = ScannerRuntimeConfigSource::Env;
        assert!(!scanner_activity_probe_required(true, false, no_features, &explicit_cycle));

        let lifecycle = ScannerMaintenanceFeatures {
            lifecycle: true,
            ..Default::default()
        };
        assert!(!scanner_activity_probe_required(true, false, lifecycle, &default_config));
    }

    #[test]
    #[serial]
    fn clean_idle_cap_preserves_default_bitrot_coverage_window() {
        let config = ScannerRuntimeConfig {
            bitrot_cycle: Some(Duration::from_secs(30 * 24 * 60 * 60)),
            bitrot_cycle_source: ScannerRuntimeConfigSource::Default,
            ..Default::default()
        };

        with_var("RUSTFS_HEAL_OBJECT_SELECT_PROB", Some("1024"), || {
            let max_interval = scanner_clean_idle_max_interval(Duration::from_secs(60), &config);
            assert_eq!(max_interval, Duration::from_millis(2_531_250));
            let positive_jitter = max_interval.mul_f64(1.1);
            let actual_delay = cap_clean_idle_cycle_delay(positive_jitter, max_interval, true);
            assert!(actual_delay < max_interval);
            assert_eq!(actual_delay, max_interval.saturating_sub(positive_jitter - max_interval));
            assert!(actual_delay.saturating_mul(1024) <= config.bitrot_cycle.expect("bitrot cycle should be configured"));
        });
    }

    #[test]
    #[serial]
    fn clean_idle_cap_allows_policy_max_when_bitrot_is_disabled() {
        let config = ScannerRuntimeConfig {
            bitrot_cycle: None,
            ..Default::default()
        };

        assert_eq!(scanner_clean_idle_max_interval(Duration::from_secs(60), &config), CLEAN_IDLE_MAX_INTERVAL);
    }

    #[test]
    #[serial]
    fn clean_idle_cap_never_shortens_the_base_cycle() {
        let config = ScannerRuntimeConfig {
            bitrot_cycle: Some(Duration::from_secs(60)),
            bitrot_cycle_source: ScannerRuntimeConfigSource::Default,
            ..Default::default()
        };

        with_var("RUSTFS_HEAL_OBJECT_SELECT_PROB", Some("1024"), || {
            assert_eq!(scanner_clean_idle_max_interval(Duration::from_secs(60), &config), Duration::from_secs(60));
        });
    }

    #[test]
    fn test_single_disk_default_cycle_preserves_regular_cycle_for_lifecycle() {
        assert_eq!(
            single_disk_default_cycle_secs(ScannerMaintenanceFeatures {
                lifecycle: true,
                ..Default::default()
            }),
            None
        );
    }

    #[test]
    fn test_single_disk_default_cycle_preserves_regular_cycle_for_replication() {
        assert_eq!(
            single_disk_default_cycle_secs(ScannerMaintenanceFeatures {
                replication: true,
                ..Default::default()
            }),
            None
        );
    }

    #[test]
    fn test_single_disk_default_cycle_preserves_regular_cycle_on_inspection_failure() {
        assert_eq!(
            single_disk_default_cycle_secs(ScannerMaintenanceFeatures {
                inspection_failed: true,
                ..Default::default()
            }),
            None
        );
    }

    #[test]
    #[serial]
    fn test_cycle_interval_keeps_default_cycle_with_explicit_speed() {
        let _guard = ScannerDefaultCycleGuard::set(TEST_DEFAULT_SCANNER_CYCLE_SECS);

        with_var_unset(ENV_SCANNER_CYCLE, || {
            with_var_unset("MINIO_SCANNER_CYCLE", || {
                with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                    with_var_unset(ENV_SCANNER_START_DELAY_SECS_DEPRECATED, || {
                        with_var(ENV_SCANNER_SPEED, Some("slowest"), || {
                            assert_eq!(cycle_interval(), Duration::from_secs(TEST_DEFAULT_SCANNER_CYCLE_SECS));
                        });
                    });
                });
            });
        });
    }

    #[test]
    #[serial]
    fn test_cycle_interval_prefers_explicit_start_delay_over_default_cycle() {
        let _guard = ScannerDefaultCycleGuard::set(TEST_DEFAULT_SCANNER_CYCLE_SECS);

        with_var_unset(ENV_SCANNER_CYCLE, || {
            with_var_unset("MINIO_SCANNER_CYCLE", || {
                with_var(ENV_SCANNER_START_DELAY_SECS, Some("120"), || {
                    assert_eq!(cycle_interval(), Duration::from_secs(120));
                });
            });
        });
    }

    #[test]
    #[serial]
    fn test_cycle_interval_supports_minio_speed_alias() {
        with_var_unset(ENV_SCANNER_SPEED, || {
            with_var_unset(ENV_SCANNER_CYCLE, || {
                with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                    with_var("MINIO_SCANNER_SPEED", Some("slowest"), || {
                        assert_eq!(cycle_interval(), Duration::from_secs(30 * 60));
                    });
                });
            });
        });
    }

    #[test]
    #[serial]
    fn test_cycle_interval_supports_minio_cycle_alias() {
        with_var_unset(ENV_SCANNER_CYCLE, || {
            with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                with_var("MINIO_SCANNER_CYCLE", Some("90"), || {
                    assert_eq!(cycle_interval(), Duration::from_secs(90));
                });
            });
        });
    }

    #[test]
    #[serial]
    fn test_randomized_cycle_delay_handles_small_start_delay() {
        // 0 is treated as minimum 1 second before jitter, with lower bound preserved.
        let delay = randomized_cycle_delay_for(Duration::from_secs(0));
        assert!(delay >= Duration::from_secs(1), "expected delay >= 1s");
        assert!(delay < Duration::from_secs(2), "expected delay < 2s");
    }

    #[tokio::test]
    #[serial]
    async fn test_wait_for_next_scanner_cycle_wakes_for_dirty_usage() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();

        let ctx = CancellationToken::new();
        let dirty_generation = crate::scanner_io::dirty_usage_generation();
        let mut wait = Box::pin(wait_for_next_scanner_cycle(
            &ctx,
            Duration::from_secs(60),
            dirty_generation,
            crate::runtime_config::scanner_runtime_config_generation(),
            crate::scanner_io::scanner_maintenance_generation(),
            || false,
        ));
        assert!(matches!(futures::poll!(&mut wait), Poll::Pending));

        crate::scanner_io::record_dirty_usage_bucket("photos");
        let reason = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("dirty usage should wake scanner before timer");

        assert_eq!(reason, ScannerCycleWakeReason::DirtyUsage);
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    }

    #[tokio::test]
    #[serial]
    async fn test_wait_for_next_scanner_cycle_sees_unattempted_dirty_usage() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let dirty_generation = crate::scanner_io::dirty_usage_generation();
        crate::scanner_io::record_dirty_usage_bucket("photos");

        let ctx = CancellationToken::new();
        let reason = wait_for_next_scanner_cycle(
            &ctx,
            Duration::from_secs(60),
            dirty_generation,
            crate::runtime_config::scanner_runtime_config_generation(),
            crate::scanner_io::scanner_maintenance_generation(),
            || false,
        )
        .await;

        assert_eq!(reason, ScannerCycleWakeReason::DirtyUsage);
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn test_wait_for_next_scanner_cycle_retries_stable_dirty_usage_on_timer() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        crate::scanner_io::record_dirty_usage_bucket("photos");
        let dirty_generation = crate::scanner_io::dirty_usage_generation();
        let ctx = CancellationToken::new();
        let wait = wait_for_next_scanner_cycle(
            &ctx,
            Duration::from_secs(60),
            dirty_generation,
            crate::runtime_config::scanner_runtime_config_generation(),
            crate::scanner_io::scanner_maintenance_generation(),
            || false,
        );

        let reason = wait.await;

        assert_eq!(reason, ScannerCycleWakeReason::Timer);
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    }

    #[tokio::test]
    #[serial]
    async fn test_wait_for_next_scanner_cycle_wakes_for_repeated_dirty_bucket() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        crate::scanner_io::record_dirty_usage_bucket("photos");
        let dirty_generation = crate::scanner_io::dirty_usage_generation();
        let ctx = CancellationToken::new();
        let mut wait = Box::pin(wait_for_next_scanner_cycle(
            &ctx,
            Duration::from_secs(60),
            dirty_generation,
            crate::runtime_config::scanner_runtime_config_generation(),
            crate::scanner_io::scanner_maintenance_generation(),
            || false,
        ));
        assert!(matches!(futures::poll!(&mut wait), Poll::Pending));

        crate::scanner_io::record_dirty_usage_bucket("photos");
        let reason = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("a newer mutation of an already-dirty bucket should wake scanner");

        assert_eq!(reason, ScannerCycleWakeReason::DirtyUsage);
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    }

    #[tokio::test]
    #[serial]
    async fn test_wait_for_next_scanner_cycle_reschedules_for_runtime_config() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let observed_generation = crate::runtime_config::scanner_runtime_config_generation();
        let ctx = CancellationToken::new();
        let mut wait = Box::pin(wait_for_next_scanner_cycle(
            &ctx,
            Duration::from_secs(60),
            crate::scanner_io::dirty_usage_generation(),
            observed_generation,
            crate::scanner_io::scanner_maintenance_generation(),
            || false,
        ));
        assert!(matches!(futures::poll!(&mut wait), Poll::Pending));

        let mut config = rustfs_config::server_config::Config::new();
        config.set_defaults();
        crate::runtime_config::apply_scanner_runtime_config(&config).expect("default scanner config should apply");
        let reason = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("runtime config should wake scanner before timer");

        assert_eq!(reason, ScannerCycleWakeReason::RuntimeConfig);
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    }

    #[tokio::test]
    #[serial]
    async fn test_wait_for_next_scanner_cycle_reschedules_for_maintenance_change() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let observed_generation = crate::scanner_io::scanner_maintenance_generation();
        let ctx = CancellationToken::new();
        let mut wait = Box::pin(wait_for_next_scanner_cycle(
            &ctx,
            Duration::from_secs(60),
            crate::scanner_io::dirty_usage_generation(),
            crate::runtime_config::scanner_runtime_config_generation(),
            observed_generation,
            || false,
        ));
        assert!(matches!(futures::poll!(&mut wait), Poll::Pending));

        crate::scanner_io::record_scanner_maintenance_change("photos");
        let reason = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("maintenance change should wake scanner before timer");

        assert_eq!(reason, ScannerCycleWakeReason::MaintenanceConfig);
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    }

    #[tokio::test]
    async fn test_wait_for_next_scanner_cycle_stops_after_leader_lock_loss() {
        let ctx = CancellationToken::new();
        let reason = wait_for_next_scanner_cycle(
            &ctx,
            Duration::from_secs(60),
            crate::scanner_io::dirty_usage_generation(),
            crate::runtime_config::scanner_runtime_config_generation(),
            crate::scanner_io::scanner_maintenance_generation(),
            || true,
        )
        .await;

        assert_eq!(reason, ScannerCycleWakeReason::LeaderLockLost);
    }

    fn scanner_node_activity(epoch: &str, namespace_generation: u64, maintenance_generation: u64) -> ScannerNodeActivity {
        ScannerNodeActivity {
            instance_id: epoch.to_string(),
            namespace_generation,
            maintenance_generation,
            protocol_version: SCANNER_ACTIVITY_PROTOCOL_VERSION,
            topology_digest: [3; 32],
            data_movement_active: false,
        }
    }

    #[test]
    fn scanner_activity_snapshot_digest_fences_storage_topology() {
        let first = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
        let mut changed = first.clone();
        changed.get_mut("node-2").expect("node should exist").topology_digest = [4; 32];

        assert_ne!(scanner_activity_snapshot_digest(&first), scanner_activity_snapshot_digest(&changed));
    }

    #[test]
    fn scanner_activity_snapshot_digest_fences_peer_protocol_upgrades() {
        let legacy = BTreeMap::from([(
            "node-2".to_string(),
            ScannerNodeActivity {
                protocol_version: SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION,
                ..scanner_node_activity("epoch-a", 7, 3)
            },
        )]);
        let current = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);

        assert_ne!(scanner_activity_snapshot_digest(&legacy), scanner_activity_snapshot_digest(&current));
    }

    #[test]
    fn scanner_activity_snapshot_fences_data_movement() {
        let idle = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
        let mut moving = idle.clone();
        moving.get_mut("node-2").expect("node should exist").data_movement_active = true;

        assert!(scanner_activity_allows_usage_publication(&idle));
        assert!(!scanner_activity_allows_usage_publication(&moving));
        assert_ne!(scanner_activity_snapshot_digest(&idle), scanner_activity_snapshot_digest(&moving));
    }

    #[test]
    fn scanner_activity_observation_requires_a_complete_baseline() {
        let mut seen = None;
        let first = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);

        let (observation, error) = apply_scanner_activity_probe_result(&mut seen, Ok(first.clone()));
        assert_eq!(observation, ScannerActivityObservation::Unverified);
        assert!(error.is_none());

        let (observation, error) = apply_scanner_activity_probe_result(&mut seen, Ok(first));
        assert_eq!(observation, ScannerActivityObservation::Unchanged);
        assert!(error.is_none());

        let changed = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 8, 3))]);
        let (observation, error) = apply_scanner_activity_probe_result(&mut seen, Ok(changed));
        assert_eq!(observation, ScannerActivityObservation::Changed);
        assert!(error.is_none());

        let restarted = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-b", 8, 0))]);
        let (observation, error) = apply_scanner_activity_probe_result(&mut seen, Ok(restarted));
        assert_eq!(observation, ScannerActivityObservation::Changed);
        assert!(error.is_none());

        let (observation, error) =
            apply_scanner_activity_probe_result(&mut seen, Err("peer does not support activity probes".to_string()));
        assert_eq!(observation, ScannerActivityObservation::Unverified);
        assert_eq!(error.as_deref(), Some("peer does not support activity probes"));
        assert!(seen.is_none());
    }

    #[test]
    fn remote_maintenance_change_is_distinct_from_namespace_activity() {
        let previous = BTreeMap::from([
            (LOCAL_SCANNER_ACTIVITY_NODE.to_string(), scanner_node_activity("local", 5, 2)),
            ("node-2".to_string(), scanner_node_activity("remote", 7, 3)),
        ]);
        let remote_maintenance_changed = BTreeMap::from([
            (LOCAL_SCANNER_ACTIVITY_NODE.to_string(), scanner_node_activity("local", 5, 2)),
            ("node-2".to_string(), scanner_node_activity("remote", 7, 4)),
        ]);
        assert_eq!(
            compare_scanner_activity(&previous, &remote_maintenance_changed),
            ScannerActivityObservation::MaintenanceChanged
        );

        let local_maintenance_changed = BTreeMap::from([
            (LOCAL_SCANNER_ACTIVITY_NODE.to_string(), scanner_node_activity("local", 5, 3)),
            ("node-2".to_string(), scanner_node_activity("remote", 7, 3)),
        ]);
        assert_eq!(
            compare_scanner_activity(&previous, &local_maintenance_changed),
            ScannerActivityObservation::Changed
        );
    }

    #[test]
    fn local_maintenance_wakeup_releases_a_remote_maintenance_block() {
        let blocked = scanner_activity_backoff_blocked_after_wake(false, ScannerCycleWakeReason::ClusterMaintenance);
        assert!(blocked);

        let unblocked = scanner_activity_backoff_blocked_after_wake(blocked, ScannerCycleWakeReason::MaintenanceConfig);
        assert!(!unblocked);
        assert!(scanner_activity_backoff_blocked_after_wake(
            blocked,
            ScannerCycleWakeReason::ClusterActivity
        ));
    }

    #[test]
    fn scanner_activity_after_a_cycle_restores_the_base_interval() {
        let runtime_config = ScannerRuntimeConfig {
            cycle_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let mut backoff = ScannerCleanIdleBackoff { interval_multiplier: 8 };

        record_scanner_cycle_result(
            &mut backoff,
            &runtime_config,
            true,
            ScannerCycleWakeReason::Timer,
            ScannerCycleOutcome::Completed,
            scanner_activity_observed_work(ScannerActivityObservation::Changed),
        );

        let plan = scanner_cycle_wait_plan(&runtime_config, backoff, true, std::convert::identity);
        assert_eq!(plan.effective_interval, Duration::from_secs(60));
        assert_eq!(plan.delay, Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn distributed_clean_idle_wait_wakes_at_base_interval_for_remote_activity() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let ctx = CancellationToken::new();
        let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));
        let changed = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 8, 3))]);

        let reason = wait_for_next_scanner_cycle_with_activity(
            &ctx,
            Duration::from_secs(120),
            Some(Duration::from_secs(60)),
            &mut seen,
            ScannerCycleObservedGenerations {
                dirty_usage: crate::scanner_io::dirty_usage_generation(),
                runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
                maintenance: crate::scanner_io::scanner_maintenance_generation(),
            },
            || false,
            || std::future::ready(Ok(changed.clone())),
        )
        .await;

        assert_eq!(reason, ScannerCycleWakeReason::ClusterActivity);
        assert_eq!(seen, Some(changed));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn distributed_clean_idle_wait_blocks_backoff_for_unpropagated_maintenance() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let ctx = CancellationToken::new();
        let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));
        let changed = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 4))]);

        let reason = wait_for_next_scanner_cycle_with_activity(
            &ctx,
            Duration::from_secs(120),
            Some(Duration::from_secs(60)),
            &mut seen,
            ScannerCycleObservedGenerations {
                dirty_usage: crate::scanner_io::dirty_usage_generation(),
                runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
                maintenance: crate::scanner_io::scanner_maintenance_generation(),
            },
            || false,
            || std::future::ready(Ok(changed.clone())),
        )
        .await;

        assert_eq!(reason, ScannerCycleWakeReason::ClusterMaintenance);
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn distributed_clean_idle_wait_fails_closed_when_a_peer_is_unverifiable() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let ctx = CancellationToken::new();
        let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));

        let reason = wait_for_next_scanner_cycle_with_activity(
            &ctx,
            Duration::from_secs(120),
            Some(Duration::from_secs(60)),
            &mut seen,
            ScannerCycleObservedGenerations {
                dirty_usage: crate::scanner_io::dirty_usage_generation(),
                runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
                maintenance: crate::scanner_io::scanner_maintenance_generation(),
            },
            || false,
            || std::future::ready(Err("node-2 is unreachable".to_string())),
        )
        .await;

        assert_eq!(reason, ScannerCycleWakeReason::ClusterActivityUnavailable);
        assert!(seen.is_none());
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn distributed_clean_idle_wait_keeps_the_extended_deadline_when_peers_are_clean() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let ctx = CancellationToken::new();
        let expected = BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]);
        let mut seen = Some(expected.clone());

        let reason = wait_for_next_scanner_cycle_with_activity(
            &ctx,
            Duration::from_secs(120),
            Some(Duration::from_secs(60)),
            &mut seen,
            ScannerCycleObservedGenerations {
                dirty_usage: crate::scanner_io::dirty_usage_generation(),
                runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
                maintenance: crate::scanner_io::scanner_maintenance_generation(),
            },
            || false,
            || std::future::ready(Ok(expected.clone())),
        )
        .await;

        assert_eq!(reason, ScannerCycleWakeReason::Timer);
        assert_eq!(seen, Some(expected));
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn scanner_activity_probe_wait_is_cancellation_aware() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let ctx = CancellationToken::new();
        let cancel = ctx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(61)).await;
            cancel.cancel();
        });
        let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));

        let reason = wait_for_next_scanner_cycle_with_activity(
            &ctx,
            Duration::from_secs(120),
            Some(Duration::from_secs(60)),
            &mut seen,
            ScannerCycleObservedGenerations {
                dirty_usage: crate::scanner_io::dirty_usage_generation(),
                runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
                maintenance: crate::scanner_io::scanner_maintenance_generation(),
            },
            || false,
            std::future::pending::<Result<ScannerActivitySnapshot, String>>,
        )
        .await;

        assert_eq!(reason, ScannerCycleWakeReason::Cancelled);
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn scanner_activity_probe_wait_stops_after_leader_lock_loss() {
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let ctx = CancellationToken::new();
        let lock_lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lose_lock = Arc::clone(&lock_lost);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(61)).await;
            lose_lock.store(true, std::sync::atomic::Ordering::Release);
        });
        let mut seen = Some(BTreeMap::from([("node-2".to_string(), scanner_node_activity("epoch-a", 7, 3))]));

        let reason = wait_for_next_scanner_cycle_with_activity(
            &ctx,
            Duration::from_secs(120),
            Some(Duration::from_secs(60)),
            &mut seen,
            ScannerCycleObservedGenerations {
                dirty_usage: crate::scanner_io::dirty_usage_generation(),
                runtime_config: crate::runtime_config::scanner_runtime_config_generation(),
                maintenance: crate::scanner_io::scanner_maintenance_generation(),
            },
            || lock_lost.load(std::sync::atomic::Ordering::Acquire),
            std::future::pending::<Result<ScannerActivitySnapshot, String>>,
        )
        .await;

        assert_eq!(reason, ScannerCycleWakeReason::LeaderLockLost);
    }

    #[test]
    #[serial]
    fn test_get_cycle_scan_mode_runs_deep_until_selection_window_completes() {
        with_var(ENV_SCANNER_BITROT_CYCLE_SECS, Some("3600"), || {
            let mode = get_cycle_scan_mode(10, 0, Some(Utc::now()), bitrot_scan_cycle());
            assert_eq!(mode, HealScanMode::Deep);
        });
    }

    #[test]
    #[serial]
    fn test_get_cycle_scan_mode_respects_elapsed_bitrot_cycle() {
        with_var(ENV_SCANNER_BITROT_CYCLE_SECS, Some("3600"), || {
            let recent = Utc::now() - chrono::Duration::minutes(30);
            let old = Utc::now() - chrono::Duration::hours(2);

            assert_eq!(get_cycle_scan_mode(2048, 0, Some(recent), bitrot_scan_cycle()), HealScanMode::Normal);
            assert_eq!(get_cycle_scan_mode(2048, 0, Some(old), bitrot_scan_cycle()), HealScanMode::Deep);
        });
    }

    #[test]
    #[serial]
    fn test_get_cycle_scan_mode_can_disable_periodic_deep_scan() {
        with_var(ENV_SCANNER_BITROT_CYCLE_SECS, Some("off"), || {
            assert_eq!(get_cycle_scan_mode(1, 0, None, bitrot_scan_cycle()), HealScanMode::Normal);
        });
    }

    #[test]
    #[serial]
    fn test_background_heal_info_for_scan_start_marks_deep_active() {
        let now = Utc::now();
        let info =
            background_heal_info_for_scan_start(BackgroundHealInfo::default(), 7, HealScanMode::Deep, now, bitrot_scan_cycle())
                .expect("deep scan should update background heal info");

        assert_eq!(info.current_scan_mode, HealScanMode::Deep);
        assert_eq!(info.bitrot_start_cycle, 7);
        assert_eq!(info.bitrot_start_time, Some(now));
    }

    #[test]
    #[serial]
    fn test_background_heal_info_for_scan_start_keeps_deep_window_start() {
        with_var_unset(ENV_SCANNER_BITROT_CYCLE_SECS, || {
            let started_at = Utc::now();
            let info = BackgroundHealInfo {
                bitrot_start_time: Some(started_at),
                bitrot_start_cycle: 7,
                current_scan_mode: HealScanMode::Normal,
            };

            let info = background_heal_info_for_scan_start(info, 8, HealScanMode::Deep, Utc::now(), bitrot_scan_cycle())
                .expect("deep scan should mark active status");

            assert_eq!(info.current_scan_mode, HealScanMode::Deep);
            assert_eq!(info.bitrot_start_cycle, 7);
            assert_eq!(info.bitrot_start_time, Some(started_at));
        });
    }

    #[test]
    #[serial]
    fn test_background_heal_info_for_scan_complete_marks_deep_idle() {
        let started_at = Utc::now();
        let info = BackgroundHealInfo {
            bitrot_start_time: Some(started_at),
            bitrot_start_cycle: 7,
            current_scan_mode: HealScanMode::Deep,
        };

        let info = background_heal_info_for_scan_complete(info, HealScanMode::Deep)
            .expect("completed deep scan should update background heal info");

        assert_eq!(info.current_scan_mode, HealScanMode::Normal);
        assert_eq!(info.bitrot_start_cycle, 7);
        assert_eq!(info.bitrot_start_time, Some(started_at));
    }

    #[test]
    #[serial]
    fn test_background_heal_info_for_scan_complete_leaves_normal_scan_unchanged() {
        let info = BackgroundHealInfo {
            bitrot_start_time: Some(Utc::now()),
            bitrot_start_cycle: 7,
            current_scan_mode: HealScanMode::Normal,
        };

        assert!(background_heal_info_for_scan_complete(info, HealScanMode::Normal).is_none());
    }

    #[test]
    #[serial]
    fn test_background_heal_info_for_failed_scan_preserves_deep_mode() {
        let info = BackgroundHealInfo {
            bitrot_start_time: Some(Utc::now()),
            bitrot_start_cycle: 7,
            current_scan_mode: HealScanMode::Deep,
        };

        assert!(background_heal_info_for_scan_result(info, HealScanMode::Deep, false).is_none());
    }

    #[test]
    fn test_retain_recent_cycle_completions_keeps_last_entries() {
        let base = Utc::now();
        let keep = data_usage_update_dir_cycles() as usize;
        let mut completed: Vec<_> = (0..keep + 2).map(|i| base + chrono::Duration::seconds(i as i64)).collect();

        retain_recent_cycle_completions(&mut completed);

        assert_eq!(completed.len(), keep);
        assert_eq!(completed.first().copied(), Some(base + chrono::Duration::seconds(2)));
        assert_eq!(completed.last().copied(), Some(base + chrono::Duration::seconds((keep + 1) as i64)));
    }
}
