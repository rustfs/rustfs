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

use std::sync::Arc;

use crate::data_usage_define::{BACKGROUND_HEAL_INFO_PATH, DATA_USAGE_BLOOM_NAME_PATH, DATA_USAGE_OBJ_NAME_PATH};
use crate::runtime_config::{
    current_scanner_runtime_config, lookup_scanner_runtime_config, refresh_scanner_runtime_config_from_global,
    scanner_bitrot_cycle, scanner_cycle_interval, scanner_start_delay, set_scanner_default_cycle_secs,
};
use crate::scanner_budget::{ScannerCycleBudget, ScannerCycleBudgetConfig, ScannerCycleBudgetReason};
use crate::scanner_folder::{data_usage_update_dir_cycles, heal_object_select_prob};
use crate::scanner_io::ScannerIO;
use crate::sleeper::{SCANNER_SLEEPER, set_scanner_default_speed};
use crate::{DataUsageInfo, ScannerActivityGuard, ScannerError};
use chrono::{DateTime, Utc};
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::metrics::{
    CurrentCycle, Metric, Metrics, ScanCyclePartialReason, ScannerWorkSource, emit_scan_cycle_complete,
    emit_scan_cycle_partial_with_source, global_metrics,
};
use rustfs_config::ScannerSpeed;
#[cfg(test)]
use rustfs_config::{
    ENV_SCANNER_BITROT_CYCLE_SECS, ENV_SCANNER_CYCLE_MAX_DIRECTORIES, ENV_SCANNER_CYCLE_MAX_DURATION_SECS,
    ENV_SCANNER_CYCLE_MAX_OBJECTS,
};
use rustfs_config::{ENV_SCANNER_CYCLE, ENV_SCANNER_SPEED, ENV_SCANNER_START_DELAY_SECS};
use rustfs_ecstore::bucket::lifecycle::lifecycle::Lifecycle as _;
use rustfs_ecstore::bucket::metadata_sys::{get_lifecycle_config, get_replication_config};
use rustfs_ecstore::bucket::replication::ReplicationConfigurationExt as _;
use rustfs_ecstore::config::com::{read_config, save_config};
use rustfs_ecstore::disk::RUSTFS_META_BUCKET;
use rustfs_ecstore::error::Error as EcstoreError;
use rustfs_ecstore::global::is_erasure_sd;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::store_api::{BucketOperations, NamespaceLocking as _, ObjectIO};
use rustfs_storage_api::BucketOptions;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

const SINGLE_DISK_SCANNER_CYCLE_SECS: u64 = 24 * 60 * 60;
const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
const LOG_SUBSYSTEM_BACKGROUND_HEAL: &str = "background_heal";
const EVENT_SCANNER_CYCLE_STATE: &str = "scanner_cycle_state";
const EVENT_SCANNER_LOCK_STATE: &str = "scanner_lock_state";
const EVENT_SCANNER_PERSIST_STATE: &str = "scanner_persist_state";
const EVENT_SCANNER_RUNTIME_CONFIG: &str = "scanner_runtime_config";
const EVENT_SCANNER_BACKGROUND_HEAL_STATE: &str = "scanner_background_heal_state";
#[cfg(test)]
const ENV_SCANNER_START_DELAY_SECS_DEPRECATED: &str = "RUSTFS_DATA_SCANNER_START_DELAY_SECS";

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

#[cfg(test)]
fn scanner_cycle_max_duration() -> Option<Duration> {
    resolve_scanner_runtime_config().cycle_budget.max_duration
}

fn resolve_scanner_runtime_config() -> crate::runtime_config::ScannerRuntimeConfig {
    let config = rustfs_config::server_config::get_global_server_config();
    match lookup_scanner_runtime_config(config.as_ref()) {
        Ok(config) => config,
        Err(err) => {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_RUNTIME_CONFIG,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                state = "resolve_failed",
                error = %err,
                "Scanner runtime config fallback applied"
            );
            current_scanner_runtime_config()
        }
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
    // rules exist. In single-disk/Slowest mode the normal inter-cycle delay is 27-33 minutes; if
    // the node was SIGKILL'd with FAILED-status objects queued, waiting that long leaves them
    // permanently unhealed until the next full cycle. Replication config is live-read at startup
    // by configure_scanner_defaults, so this signal is always current regardless of when the
    // persisted DataUsageInfo was last written.
    if (usage_cache_is_cold || has_active_replication) && has_buckets {
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

    if !has_buckets {
        return (false, false);
    }

    (persisted_usage_cache_is_cold_for_startup(storeapi).await, true)
}

pub async fn init_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) {
    let startup_features = configure_scanner_defaults(&storeapi).await;
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

            if let Err(e) = run_data_scanner(ctx_clone.clone(), storeapi_clone.clone()).await {
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

fn single_disk_default_cycle_secs(features: ScannerMaintenanceFeatures) -> Option<u64> {
    if features.needs_regular_cycle() {
        None
    } else {
        Some(SINGLE_DISK_SCANNER_CYCLE_SECS)
    }
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

async fn configure_scanner_defaults(storeapi: &Arc<ECStore>) -> ScannerMaintenanceFeatures {
    if is_erasure_sd().await {
        let features = detect_scanner_maintenance_features(storeapi).await;
        let default_cycle_secs = single_disk_default_cycle_secs(features);
        set_scanner_default_speed(ScannerSpeed::Slowest);
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
        features
    } else {
        set_scanner_default_speed(ScannerSpeed::Default);
        set_scanner_default_cycle_secs(None);
        ScannerMaintenanceFeatures::default()
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
    if is_erasure_sd().await {
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
    if is_erasure_sd().await {
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

async fn mark_scan_cycle_idle(cycle_info: &mut CurrentCycle) {
    cycle_info.current = 0;
    global_metrics().clear_current_scan_mode();
    global_metrics().set_cycle(Some(cycle_info.clone())).await;
}

#[instrument(skip_all)]
async fn run_data_scanner_cycle(ctx: &CancellationToken, storeapi: &Arc<ECStore>, cycle_info: &mut CurrentCycle) {
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

    let (sender, receiver) = mpsc::channel::<DataUsageInfo>(1);
    let storeapi_clone = storeapi.clone();
    let ctx_clone = ctx.clone();
    tokio::spawn(async move {
        store_data_usage_in_backend(ctx_clone, storeapi_clone, receiver).await;
    });

    let done_cycle = Metrics::time(Metric::ScanCycle);
    let cycle_start = std::time::Instant::now();
    let cycle_work_start = global_metrics().start_scan_cycle_work();
    let cycle_budget = ScannerCycleBudget::new(ctx, cycle_budget_config);
    if let Err(e) = storeapi
        .clone()
        .nsscanner(cycle_budget.token(), cycle_budget.clone(), sender, cycle_info.current, scan_mode)
        .await
    {
        let budget_elapsed = cycle_budget.budget_elapsed() && !ctx.is_cancelled();
        global_metrics().finish_scan_cycle_work(cycle_work_start);
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
            mark_scan_cycle_idle(cycle_info).await;
            return;
        }
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
        if let Some(new_heal_info) = background_heal_info_for_scan_complete(background_heal_info.clone(), scan_mode) {
            save_background_heal_info(storeapi.clone(), new_heal_info).await;
        }
        return;
    }
    if cycle_budget.budget_elapsed() && !ctx.is_cancelled() {
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
        global_metrics().finish_scan_cycle_work(cycle_work_start);
        let budget_reason = cycle_budget.reason();
        emit_scan_cycle_partial_with_source(
            cycle_start.elapsed(),
            scan_cycle_partial_reason(budget_reason),
            scan_cycle_partial_source(budget_reason),
        );
        mark_scan_cycle_idle(cycle_info).await;
        return;
    }
    done_cycle();
    global_metrics().finish_scan_cycle_work(cycle_work_start);
    emit_scan_cycle_complete(true, cycle_start.elapsed());
    if let Some(new_heal_info) = background_heal_info_for_scan_complete(background_heal_info.clone(), scan_mode) {
        save_background_heal_info(storeapi.clone(), new_heal_info).await;
    }

    cycle_info.next += 1;
    cycle_info.current = 0;
    cycle_info.cycle_completed.push(Utc::now());
    global_metrics().clear_current_scan_mode();

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

    retain_recent_cycle_completions(&mut cycle_info.cycle_completed);
    global_metrics().set_cycle(Some(cycle_info.clone())).await;

    let cycle_info_buf = cycle_info.marshal().unwrap_or_default();

    let mut buf = Vec::with_capacity(cycle_info_buf.len() + 8);
    buf.extend_from_slice(&cycle_info.next.to_le_bytes());
    buf.extend_from_slice(&cycle_info_buf);

    if let Err(e) = save_config(storeapi.clone(), &DATA_USAGE_BLOOM_NAME_PATH, buf).await {
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
    } else {
        debug!(
            target: "rustfs::scanner",
            event = EVENT_SCANNER_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            path = %&*DATA_USAGE_BLOOM_NAME_PATH,
            state = "saved",
            "Scanner state saved"
        );
    }
}

pub async fn run_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) -> Result<(), ScannerError> {
    // Acquire leader lock (write lock) to ensure only one scanner runs
    let _guard = match storeapi.new_ns_lock(RUSTFS_META_BUCKET, "leader.lock").await {
        Ok(ns_lock) => match ns_lock.get_write_lock_quiet(get_lock_acquire_timeout()).await {
            Ok(guard) => {
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

    let mut cycle_info = CurrentCycle::default();
    let buf = read_config(storeapi.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .unwrap_or_default();
    if buf.len() == 8 {
        cycle_info.next = u64::from_le_bytes(buf.try_into().unwrap_or_default());
    } else if buf.len() > 8 {
        cycle_info.next = u64::from_le_bytes(buf[0..8].try_into().unwrap_or_default());
        if let Err(e) = cycle_info.unmarshal(&buf[8..]) {
            warn!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %&*DATA_USAGE_BLOOM_NAME_PATH,
                state = "cycle_decode_failed",
                error = %e,
                "Scanner cycle state decode failed"
            );
        }
    }

    if !ctx.is_cancelled() {
        // Preserve previous behavior: run one cycle immediately after lock acquisition.
        run_data_scanner_cycle(&ctx, &storeapi, &mut cycle_info).await;
    }

    loop {
        if ctx.is_cancelled() {
            break;
        }

        // Randomized inter-cycle delay
        tokio::select! {
            _ = ctx.cancelled() => break,
            _ = tokio::time::sleep(randomized_cycle_delay()) => {
                run_data_scanner_cycle(&ctx, &storeapi, &mut cycle_info).await;
            },
        }
    }

    global_metrics().set_cycle(None).await;

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

/// Store data usage info in backend. Will store all objects sent on the receiver until closed.
#[instrument(skip(ctx, storeapi))]
pub async fn store_data_usage_in_backend(
    ctx: CancellationToken,
    storeapi: Arc<impl ObjectIO>,
    mut receiver: mpsc::Receiver<DataUsageInfo>,
) {
    let mut attempts = 1u32;

    while let Some(data_usage_info) = receiver.recv().await {
        let _activity_guard = ScannerActivityGuard::new();
        if ctx.is_cancelled() {
            break;
        }

        if let Ok(buf) = read_config(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str()).await
            && let Ok(existing) = serde_json::from_slice::<DataUsageInfo>(&buf)
            && let (Some(new_ts), Some(existing_ts)) = (data_usage_info.last_update, existing.last_update)
            && new_ts <= existing_ts
        {
            debug!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                incoming_last_update = ?new_ts,
                existing_last_update = ?existing_ts,
                state = "skip_stale_update",
                "Scanner stale data usage update skipped"
            );
            continue;
        }

        // Serialize to JSON
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
                continue;
            }
        };

        // Save a backup every 10th update
        if attempts > 10 {
            let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
            let done_save = Metrics::time(Metric::SaveUsage);
            if let Err(e) = save_config(storeapi.clone(), &backup_path, data.clone()).await {
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

        // Save main configuration
        let done_save = Metrics::time(Metric::SaveUsage);
        if let Err(e) = save_config(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), data).await {
            error!(
                target: "rustfs::scanner",
                event = EVENT_SCANNER_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                path = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                state = "save_failed",
                error = %e,
                "Scanner data usage save failed"
            );
        } else {
            rustfs_ecstore::data_usage::replace_bucket_usage_memory_from_info(&data_usage_info).await;
        }
        done_save();

        attempts += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_ecstore::store_api::{GetObjectReader, ObjectIO, ObjectInfo, ObjectOptions, PutObjReader};
    use serial_test::serial;
    use std::collections::HashMap;
    use std::io::Cursor;
    use temp_env::{with_var, with_var_unset};
    use tokio::io::AsyncReadExt;
    use tokio::sync::Mutex;

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
    }

    fn memory_config_key(bucket: &str, object: &str) -> String {
        format!("{bucket}/{object}")
    }

    #[async_trait::async_trait]
    impl ObjectIO for MemoryConfigStore {
        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<rustfs_ecstore::store_api::HTTPRangeSpec>,
            _h: http::HeaderMap,
            _opts: &ObjectOptions,
        ) -> rustfs_ecstore::error::Result<GetObjectReader> {
            let objects = self.objects.lock().await;
            let data = objects
                .get(&memory_config_key(bucket, object))
                .cloned()
                .ok_or(rustfs_ecstore::error::Error::FileNotFound)?;

            Ok(GetObjectReader {
                stream: Box::new(Cursor::new(data)),
                object_info: ObjectInfo::default(),
            })
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            data: &mut PutObjReader,
            _opts: &ObjectOptions,
        ) -> rustfs_ecstore::error::Result<ObjectInfo> {
            let mut buf = Vec::new();
            data.stream.read_to_end(&mut buf).await?;
            self.objects.lock().await.insert(memory_config_key(bucket, object), buf);
            Ok(ObjectInfo::default())
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
    fn test_initial_scanner_delay_keeps_configured_delay_without_buckets() {
        let delay = initial_scanner_delay_for_startup(Some(120), true, false, false);
        assert!(delay >= Duration::from_secs(108));
        assert!(delay <= Duration::from_secs(132));
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

        store_data_usage_in_backend(ctx, store.clone(), receiver).await;

        let objects = store.objects.lock().await;
        let saved = objects
            .get(&memory_config_key(RUSTFS_META_BUCKET, DATA_USAGE_OBJ_NAME_PATH.as_str()))
            .expect("data usage config should be saved");
        let saved = serde_json::from_slice::<DataUsageInfo>(saved).expect("saved usage snapshot should decode");

        assert_eq!(saved.buckets_count, 2);
        assert_eq!(saved.last_update, Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(20)));
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
        let _guard = ScannerDefaultCycleGuard::set(SINGLE_DISK_SCANNER_CYCLE_SECS);

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
        let _guard = ScannerDefaultCycleGuard::set(SINGLE_DISK_SCANNER_CYCLE_SECS);

        with_unset_scanner_timing_env(|| {
            assert_eq!(cycle_interval(), Duration::from_secs(SINGLE_DISK_SCANNER_CYCLE_SECS));
        });
    }

    #[test]
    fn test_single_disk_default_cycle_uses_long_interval_without_maintenance_features() {
        assert_eq!(
            single_disk_default_cycle_secs(ScannerMaintenanceFeatures::default()),
            Some(SINGLE_DISK_SCANNER_CYCLE_SECS)
        );
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
    fn test_cycle_interval_keeps_single_disk_cycle_with_explicit_speed() {
        let _guard = ScannerDefaultCycleGuard::set(SINGLE_DISK_SCANNER_CYCLE_SECS);

        with_var_unset(ENV_SCANNER_CYCLE, || {
            with_var_unset("MINIO_SCANNER_CYCLE", || {
                with_var_unset(ENV_SCANNER_START_DELAY_SECS, || {
                    with_var_unset(ENV_SCANNER_START_DELAY_SECS_DEPRECATED, || {
                        with_var(ENV_SCANNER_SPEED, Some("slowest"), || {
                            assert_eq!(cycle_interval(), Duration::from_secs(SINGLE_DISK_SCANNER_CYCLE_SECS));
                        });
                    });
                });
            });
        });
    }

    #[test]
    #[serial]
    fn test_cycle_interval_prefers_explicit_start_delay_over_default_cycle() {
        let _guard = ScannerDefaultCycleGuard::set(SINGLE_DISK_SCANNER_CYCLE_SECS);

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
