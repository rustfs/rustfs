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
#[cfg(test)]
use std::sync::Mutex as StdMutex;
use std::sync::{Arc, LazyLock, RwLock};

use self::heal_info::{BackgroundHealInfoReadStatus, read_background_heal_info_with_epoch, save_background_heal_info_for_epoch};
use crate::data_usage_define::{
    BACKGROUND_HEAL_INFO_PATH, DATA_USAGE_BLOOM_NAME_PATH, DATA_USAGE_OBJ_NAME_PATH, DATA_USAGE_OBSERVED_OBJ_NAME_PATH,
    DataUsageCache, DataUsageCacheRevision, LEGACY_DATA_USAGE_OBJ_NAME_PATH, read_config_revision, read_config_with_revision,
};
use crate::runtime_config::{
    ScannerRuntimeConfig, ScannerRuntimeConfigSource, refresh_scanner_runtime_config_from_global, scanner_bitrot_cycle,
    scanner_cycle_interval, scanner_runtime_config_changed, scanner_runtime_config_generation, scanner_start_delay,
    set_scanner_default_cycle_secs,
};
use crate::scanner_budget::{ScannerCycleBudget, ScannerCycleBudgetConfig, ScannerCycleBudgetReason};
use crate::scanner_folder::{data_usage_update_dir_cycles, heal_object_select_prob};
use crate::scanner_io::{
    ScannerCycleDeferReason, ScannerCycleResult, ScannerCycleStatus, ScannerIOCycle, dirty_usage_bucket_notified,
    dirty_usage_buckets_pending, dirty_usage_generation, scanner_dirty_usage_state, scanner_maintenance_changed,
    scanner_maintenance_generation,
};
use crate::sleeper::{SCANNER_SLEEPER, set_scanner_default_speed};
use crate::{DataUsageInfo, ScannerActivityGuard, ScannerError, ScannerRuntimeGuard};
use crate::{ScannerConfigObjectDelete, ScannerObjectIO, ScannerObjectOptions};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::metrics::{
    CurrentCycle, Metric, Metrics, ScanCyclePartialReason, ScanCycleWorkSnapshot, ScannerUsageSaveResult, ScannerWorkSource,
    emit_scan_cycle_complete, emit_scan_cycle_deferred, emit_scan_cycle_partial_with_source, emit_scan_cycle_superseded,
    global_metrics,
};
use rustfs_config::ScannerSpeed;
#[cfg(test)]
use rustfs_config::{
    ENV_SCANNER_BITROT_CYCLE_SECS, ENV_SCANNER_CYCLE_MAX_DIRECTORIES, ENV_SCANNER_CYCLE_MAX_DURATION_SECS,
    ENV_SCANNER_CYCLE_MAX_OBJECTS,
};
use rustfs_config::{ENV_SCANNER_CYCLE, ENV_SCANNER_SPEED, ENV_SCANNER_START_DELAY_SECS};
use rustfs_data_usage::observed_data_usage_is_newer;
use rustfs_lock::NamespaceLockGuard;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Notify, mpsc};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, info, instrument, warn};

use crate::storage_api::scan::{
    BucketOperations, BucketOptions, NamespaceLocking as _, SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION,
    SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION, SCANNER_ACTIVITY_PROTOCOL_VERSION,
};
use crate::{
    ECStore, EcstoreError, RUSTFS_META_BUCKET, SCANNER_PUBLICATION_EPOCH_CHANGED, ScannerLifecycleConfigExt as _,
    ScannerReplicationConfigExt as _, delete_config_with_publication_admission_for_epoch, get_lifecycle_config,
    get_replication_config, invalidate_admin_data_usage_snapshot_cache, invalidate_data_usage_snapshot_cache, read_config,
    replace_bucket_usage_memory_from_info, save_config, save_config_shared_with_preconditions, save_config_with_preconditions,
    save_config_with_publication_admission_for_epoch, scanner_is_erasure_sd, scanner_publication_admission_for_epoch,
    scanner_publication_epoch, scanner_publication_epoch_changed,
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
/// A transient backend outage remains self-healing after the short retry
/// budget is exhausted, but the probe is intentionally sparse until storage
/// recovers or an operator reset wakes the scanner.
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
const MAINTENANCE_FEATURE_INSPECTION_RETRY_BASE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MAINTENANCE_FEATURE_INSPECTION_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);
const MAX_MAINTENANCE_FEATURE_INSPECTION_ATTEMPTS: usize = 2;
const SCANNER_PERSIST_CAS_RETRIES: usize = 2;
const DATA_USAGE_BACKUP_INTERVAL_CYCLES: u64 = 10;
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
    effective_interval_seconds: u64,
    clean_idle_backoff_enabled: bool,
    clean_idle_backoff_multiplier: u64,
    superseded_retry_backoff_enabled: bool,
    superseded_cycles: u32,
}

impl Default for ScannerCycleScheduleStatus {
    fn default() -> Self {
        Self {
            effective_interval_seconds: 0,
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
        effective_interval_seconds,
        clean_idle_backoff_enabled,
        clean_idle_backoff_multiplier: clean_idle_backoff_multiplier.max(1),
        superseded_retry_backoff_enabled,
        superseded_cycles,
    };
}

fn reset_scanner_cycle_schedule() {
    record_scanner_cycle_schedule(Duration::ZERO, false, 1, false, 0);
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
    !info.usage_snapshot_complete
        && info.scanner_epoch.is_none()
        && info.usage_snapshot_converged != Some(false)
        && info.last_update.is_some()
        && info.scanner_cycle.is_some()
        && u64::try_from(info.buckets_usage.len()).ok() == Some(info.buckets_count)
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
async fn sync_data_usage_backup_from_primary(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
) -> Result<(), EcstoreError> {
    sync_data_usage_backup_from_primary_for_epoch(ctx, storeapi, None).await
}

async fn sync_data_usage_backup_from_primary_for_epoch(
    ctx: &CancellationToken,
    storeapi: Arc<impl ScannerObjectIO + ScannerConfigObjectDelete>,
    expected_publication_epoch: Option<u64>,
) -> Result<(), EcstoreError> {
    let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
    for retry in 0..=SCANNER_PERSIST_CAS_RETRIES {
        if ctx.is_cancelled() {
            return Ok(());
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
            let Some(_publication_admission) = scanner_publication_admission_for_epoch(storeapi.clone(), read_epoch).await else {
                if retry < SCANNER_PERSIST_CAS_RETRIES {
                    continue;
                }
                return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
            };
            save_config_shared_with_preconditions(
                storeapi.clone(),
                &backup_path,
                primary.clone(),
                sha256hex,
                revision.preconditions(),
            )
            .await
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
    let runtime_guard = ScannerRuntimeGuard::new();
    tokio::spawn(async move {
        let _runtime_guard = runtime_guard;
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

        let mut transient_backoff = ScannerRetryBackoff::default();
        let mut recovery_retry_count = 0_u32;
        loop {
            if ctx_clone.is_cancelled() {
                break;
            }

            let run_result = run_data_scanner_with_maintenance_state(
                ctx_clone.clone(),
                storeapi_clone.clone(),
                startup_features,
                startup_maintenance_generation,
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
            let recovery_status = scanner_cycle_recovery_status();
            if recovery_status.retryable {
                recovery_retry_count = recovery_retry_count.saturating_add(1);
                let _ = record_scanner_cycle_recovery_retry(recovery_retry_count);
            } else {
                recovery_retry_count = 0;
            }

            let recovery_status = scanner_cycle_recovery_status();
            if recovery_status.state == "paused" {
                transient_backoff.record_retryable_cycle(false);
                tokio::select! {
                    _ = ctx_clone.cancelled() => break,
                    _ = SCANNER_CYCLE_RECOVERY_WAKE.notified() => {},
                    _ = tokio::time::sleep(SCANNER_CYCLE_RECOVERY_PAUSED_INTERVAL) => {},
                }
                recovery_retry_count = 0;
                continue;
            }
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

            let retry_delay = if recovery_status.retryable || run_result.is_err() {
                transient_backoff.record_retryable_cycle(true);
                transient_backoff
                    .retry_interval(scanner_cycle_interval())
                    .unwrap_or(SCANNER_RETRY_BASE_INTERVAL)
            } else {
                transient_backoff.record_retryable_cycle(false);
                randomized_cycle_delay()
            };
            // Backoff before retrying after lock contention or scanner-level failures.
            // Keep this cancellation-aware so shutdown is not delayed by backoff sleep.
            tokio::select! {
                _ = ctx_clone.cancelled() => break,
                _ = SCANNER_CYCLE_RECOVERY_WAKE.notified() => {},
                _ = tokio::time::sleep(retry_delay) => {}
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

/// Get lock acquire timeout from environment variable RUSTFS_LOCK_ACQUIRE_TIMEOUT (in seconds)
/// Defaults to 5 seconds if not set or invalid
/// For distributed environments with multiple nodes, a longer timeout may be needed
fn get_lock_acquire_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64("RUSTFS_LOCK_ACQUIRE_TIMEOUT", 5))
}

fn data_usage_persist_timeout() -> Duration {
    DataUsageCache::persistence_timeout()
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
    lock_lost: LockLost,
) -> bool
where
    Store: ScannerObjectIO + ScannerConfigObjectDelete,
    LockLost: Future<Output = ()>,
{
    let fence_ctx = ctx.child_token();
    let claim = claim_scanner_leadership(&fence_ctx, storeapi, cycle_info, cycle_revision, leader_epoch);
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

#[cfg(test)]
async fn run_data_scanner_cycle(
    ctx: &CancellationToken,
    storeapi: &Arc<ECStore>,
    cycle_info: &mut CurrentCycle,
    cycle_revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
) -> ScannerCycleOutcome {
    let cycle_budget = ScannerCycleBudget::new(ctx, scanner_cycle_budget_config());
    run_data_scanner_cycle_with_budget(ctx, storeapi, cycle_info, cycle_revision, leader_epoch, cycle_budget).await
}

#[instrument(skip_all)]
#[hotpath::measure]
async fn run_data_scanner_cycle_with_budget(
    ctx: &CancellationToken,
    storeapi: &Arc<ECStore>,
    cycle_info: &mut CurrentCycle,
    cycle_revision: &mut DataUsageCacheRevision,
    leader_epoch: u64,
    cycle_budget: Arc<ScannerCycleBudget>,
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
    let cycle_budget_config = ScannerCycleBudgetConfig {
        max_duration: cycle_budget.max_duration(),
        max_objects: cycle_budget.max_objects(),
        max_directories: cycle_budget.max_directories(),
    };
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

    let mut cycle_metrics_guard = ScannerCycleMetricsGuard::new(cycle_info.clone()).await;

    // Refresh the storage-owned movement snapshot before reading background
    // heal state. A missing heal object yields an in-memory default; do not
    // let that default influence a cycle while publication is blocked.
    if storeapi.scanner_data_usage_publication_blocked().await {
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
        BackgroundHealInfoReadStatus::ErasureSd
        | BackgroundHealInfoReadStatus::Loaded
        | BackgroundHealInfoReadStatus::Missing => {}
    }
    let mut background_heal_info = background_heal_read.info;
    let background_heal_epoch = background_heal_read.expected_epoch;

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
        save_background_heal_info_for_epoch(storeapi.clone(), new_heal_info, background_heal_epoch).await;
    }

    let cycle_start = std::time::Instant::now();
    // Baseline reads are part of the same publication proof as the eventual
    // scanner aggregate. Hold only the short storage-owned admission guard
    // across this metadata read; the full bucket scan runs after it is
    // released and carries the captured epoch forward.
    let Some((baseline_publication_guard, baseline_publication_epoch)) =
        storeapi.scanner_data_usage_publication_admission_guard().await
    else {
        mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
        return ScannerCycleOutcome::Deferred(ScannerCycleDeferReason::DataMovement);
    };
    let usage_persist_baseline_result = read_config_with_revision(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await;
    drop(baseline_publication_guard);
    let usage_persist_baseline = match usage_persist_baseline_result {
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
            mark_scan_cycle_idle(cycle_info, &mut cycle_metrics_guard).await;
            return ScannerCycleOutcome::Failed;
        }
    };
    let (sender, receiver) = mpsc::channel::<DataUsageInfo>(1);

    let done_cycle = Metrics::time(Metric::ScanCycle);
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
    let publication_defer_reason = match &scan_result {
        Ok(result)
            if result
                .publication_epoch()
                .is_some_and(|publication_epoch| publication_epoch != baseline_publication_epoch) =>
        {
            Some(ScannerCycleDeferReason::DataMovement)
        }
        Ok(result) => final_data_usage_publication_defer_reason(storeapi.as_ref(), result.status).await,
        Err(_) => Some(ScannerCycleDeferReason::ActivityBaselineUnavailable),
    };
    let publication_epoch = scan_result.as_ref().ok().and_then(ScannerCycleResult::publication_epoch);
    let budget_elapsed = cycle_budget.budget_elapsed() && !ctx.is_cancelled();
    let usage_persist_outcome = match publication_defer_reason {
        Some(reason) => {
            drop(receiver);
            DataUsagePersistOutcome::Deferred(reason)
        }
        None => {
            // ScannerIO emits its complete or observational update only after
            // all set workers finish. Persist after the final activity fence;
            // this also avoids blocking the scanner on a denied publication.
            let storeapi_clone = storeapi.clone();
            let ctx_clone = ctx.clone();
            let route_probe_store = storeapi.clone();
            let mut usage_persist_task = AbortOnDropHandle::new(tokio::spawn(async move {
                store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch(
                    ctx_clone,
                    storeapi_clone,
                    receiver,
                    Some(leader_epoch),
                    Some(usage_persist_baseline),
                    publication_epoch,
                    move || {
                        let storeapi = route_probe_store.clone();
                        async move { storeapi.scanner_data_usage_publication_blocked().await }
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
            }
        }
    };
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
    match scanner_cycle_pre_commit_outcome(scan_cycle_result.required_cycle_floor(), &usage_persist_outcome) {
        Some(ScannerCyclePreCommitOutcome::RecoverCacheCycle(required_cycle)) => {
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
                ScannerCycleOutcome::Failed
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
            ScannerCycleOutcome::Failed
        };
    }

    let (completion_outcome, scanner_pending_maintenance_work, remote_dirty_usage_acknowledgements) =
        finalize_scanner_cycle_result(scan_cycle_result, usage_persist_outcome);
    let remote_dirty_usage_pending = if remote_dirty_usage_acknowledgements.is_empty() {
        false
    } else if let Some(notification_system) = storeapi.notification_system() {
        let acknowledgement_count = remote_dirty_usage_acknowledgements.len();
        let acknowledgements = remote_dirty_usage_acknowledgements
            .into_iter()
            .map(|acknowledgement| (acknowledgement.host, acknowledgement.instance_id, acknowledgement.generation))
            .collect();
        remote_dirty_usage_acknowledgement_pending(
            cycle_info.current,
            acknowledgement_count,
            notification_system.acknowledge_scanner_dirty_usage(acknowledgements),
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
                ScannerCycleOutcome::Failed
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
            return ScannerCycleOutcome::Failed;
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
    if !persist_scanner_cycle_state_for_epoch(ctx, storeapi.clone(), cycle_info, cycle_revision, leader_epoch, publication_epoch)
        .await
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
        return ScannerCycleOutcome::Failed;
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
        "Scanner cycle completed"
    );

    cycle_metrics_guard.finish(cycle_info.clone()).await;
    scanner_cycle_outcome_with_pending_maintenance(ScannerCycleOutcome::Completed, pending_maintenance_work)
}

struct ScannerCycleMetricsGuard {
    start: Option<ScanCycleWorkSnapshot>,
}

impl ScannerCycleMetricsGuard {
    async fn new(cycle: CurrentCycle) -> Self {
        Self {
            start: Some(global_metrics().start_scan_cycle_work_with_cycle(cycle).await),
        }
    }

    async fn finish(&mut self, cycle: CurrentCycle) {
        if let Some(start) = self.start {
            global_metrics().finish_scan_cycle_work_with_cycle(start, cycle).await;
            self.start = None;
        }
    }
}

impl Drop for ScannerCycleMetricsGuard {
    fn drop(&mut self) {
        if let Some(start) = self.start.take() {
            global_metrics().finish_scan_cycle_work(start);
        }
    }
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
    let mut guard = match storeapi.new_ns_lock(RUSTFS_META_BUCKET, "leader.lock").await {
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
    let mut superseded_backoff = ScannerRetryBackoff::default();
    let mut deferred_backoff = ScannerRetryBackoff::default();
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

    let (mut cycle_info, mut leader_epoch, mut cycle_revision) =
        match load_scanner_cycle_state_for_startup(storeapi.clone()).await {
            ScannerCycleStateStartup::Ready {
                cycle,
                leader_epoch,
                revision,
            } => (cycle, leader_epoch, revision),
            ScannerCycleStateStartup::Blocked => {
                global_metrics().set_cycle(None).await;
                return Ok(());
            }
            ScannerCycleStateStartup::Transient(err) => {
                global_metrics().set_cycle(None).await;
                return Err(err);
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
        let cycle_budget = ScannerCycleBudget::new_with_runtime_progress_tracking(&cycle_ctx, scanner_cycle_budget_config());
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
            ),
            guard.lock_lost_notified(),
        )
        .await
        {
            ScannerCycleWaitOutcome::Completed(outcome) => outcome,
            ScannerCycleWaitOutcome::LockLost => {
                record_scanner_leader_lock_lost("Scanner leader lock lost during the initial cycle").await;
                global_metrics().set_cycle(None).await;
                return Ok(());
            }
            ScannerCycleWaitOutcome::Cancelled => {
                global_metrics().set_cycle(None).await;
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
                    },
                    worker_stopped,
                    &mut guard,
                )
                .await;
                return Ok(());
            }
        };
        superseded_backoff.record_retryable_cycle(initial_outcome == ScannerCycleOutcome::Superseded);
        deferred_backoff.record_retryable_cycle(matches!(initial_outcome, ScannerCycleOutcome::Deferred(_)));
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
        let mut wait_plan =
            scanner_cycle_wait_plan(&runtime_config, clean_idle_backoff, backoff_enabled, randomized_cycle_delay_for);
        let superseded_retry_interval = superseded_backoff.retry_interval(runtime_config.cycle_interval);
        let deferred_retry_interval = deferred_backoff.retry_interval(runtime_config.cycle_interval);
        let convergence_retry_interval = superseded_retry_interval.or(deferred_retry_interval);
        if let Some(retry_interval) = convergence_retry_interval {
            wait_plan.effective_interval = retry_interval;
            wait_plan.delay = randomized_cycle_delay_for(retry_interval).min(retry_interval);
        }
        let dirty_generation_before_wait = dirty_usage_generation();
        let dirty_usage_pending_before_wait = dirty_usage_buckets_pending();
        let maintenance_generation_before_wait = scanner_maintenance_generation();
        record_scanner_cycle_schedule(
            wait_plan.effective_interval,
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
            interval_multiplier = clean_idle_backoff.interval_multiplier,
            clean_idle_backoff_enabled = backoff_enabled,
            superseded_retry_backoff_enabled = superseded_retry_interval.is_some(),
            superseded_cycles = superseded_backoff.consecutive_cycles,
            deferred_retry_backoff_enabled = deferred_retry_interval.is_some(),
            deferred_cycles = deferred_backoff.consecutive_cycles,
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
            ScannerCycleObservedGenerations::for_wait(
                &runtime_config,
                convergence_retry_interval,
                dirty_usage_generation_seen,
                runtime_config_generation_seen,
                maintenance_generation_before_wait,
            ),
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
        let cycle_budget = ScannerCycleBudget::new_with_runtime_progress_tracking(&cycle_ctx, scanner_cycle_budget_config());
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
            ),
            guard.lock_lost_notified(),
        )
        .await
        {
            ScannerCycleWaitOutcome::Completed(outcome) => outcome,
            ScannerCycleWaitOutcome::LockLost => {
                record_scanner_leader_lock_lost("Scanner leader lock lost during a scanner cycle").await;
                global_metrics().set_cycle(None).await;
                return Ok(());
            }
            ScannerCycleWaitOutcome::Cancelled => {
                global_metrics().set_cycle(None).await;
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
                    },
                    worker_stopped,
                    &mut guard,
                )
                .await;
                return Ok(());
            }
        };
        superseded_backoff.record_retryable_cycle(outcome == ScannerCycleOutcome::Superseded);
        deferred_backoff.record_retryable_cycle(matches!(outcome, ScannerCycleOutcome::Deferred(_)));
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

async fn final_data_usage_publication_defer_reason(
    storeapi: &ECStore,
    status: ScannerCycleStatus,
) -> Option<ScannerCycleDeferReason> {
    match status {
        ScannerCycleStatus::Complete | ScannerCycleStatus::Superseded => {
            if storeapi.scanner_data_usage_publication_blocked().await {
                return Some(ScannerCycleDeferReason::DataMovement);
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
    usage_persist_outcome: DataUsagePersistOutcome,
) -> (ScannerCycleOutcome, bool, Vec<ScannerDirtyUsageAcknowledgement>) {
    let completion_outcome = scanner_cycle_completion_outcome(
        scan_cycle_result.status,
        usage_persist_outcome,
        scan_cycle_result.has_dirty_usage_to_acknowledge(),
        scan_cycle_result.has_failed_dirty_usage(),
    );
    let pending_maintenance_work = scan_cycle_result.has_pending_maintenance_work();
    let durable_complete_snapshot = scan_cycle_result.status == ScannerCycleStatus::Complete
        && matches!(
            usage_persist_outcome,
            DataUsagePersistOutcome::Saved | DataUsagePersistOutcome::AlreadyDurable
        );
    let remote_dirty_usage_acknowledgements = if durable_complete_snapshot {
        scan_cycle_result.acknowledge_durable_usage()
    } else {
        Vec::new()
    };
    (completion_outcome, pending_maintenance_work, remote_dirty_usage_acknowledgements)
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
mod cycle_state;
mod heal_info;
mod leadership;
mod usage_store;

use activity::*;
use cycle_state::*;
use leadership::*;
use usage_store::*;

pub use activity::scanner_topology_digest;
pub(crate) use activity::{
    ScannerActivitySnapshot, ScannerDirtyUsageAcknowledgement, probe_scanner_activity, scanner_activity_allows_usage_publication,
    scanner_activity_snapshot_digest, scanner_dirty_usage_acknowledgements,
};
pub(crate) use activity::{ScannerCycleOutcome, scanner_cycle_outcome_with_pending_maintenance};
#[cfg(test)]
pub(crate) use cycle_state::encode_scanner_cycle_fence_for_test;
pub use cycle_state::{
    ScannerCycleRecoveryMarker, ScannerCycleRecoveryStatus, reset_scanner_cycle_recovery, scanner_cycle_recovery_status,
};
pub(crate) use cycle_state::{
    current_scanner_leader_epoch, decode_persisted_scanner_cycle_fence, load_scanner_cycle_state_for_startup,
};
pub use heal_info::{BackgroundHealInfo, read_background_heal_info, save_background_heal_info};
pub use usage_store::store_data_usage_in_backend;

#[cfg(test)]
mod tests;
