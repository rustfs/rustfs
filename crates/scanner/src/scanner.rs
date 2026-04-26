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
use crate::scanner_folder::data_usage_update_dir_cycles;
use crate::scanner_io::ScannerIO;
use crate::sleeper::SCANNER_SLEEPER;
use crate::{DataUsageInfo, ScannerActivityGuard, ScannerError};
use chrono::{DateTime, Utc};
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::metrics::{CurrentCycle, Metric, Metrics, emit_scan_cycle_complete, global_metrics};
use rustfs_config::ScannerSpeed;
use rustfs_config::{DEFAULT_SCANNER_SPEED, ENV_SCANNER_CYCLE, ENV_SCANNER_SPEED, ENV_SCANNER_START_DELAY_SECS};
use rustfs_ecstore::StorageAPI as _;
use rustfs_ecstore::config::com::{read_config, save_config};
use rustfs_ecstore::disk::RUSTFS_META_BUCKET;
use rustfs_ecstore::error::Error as EcstoreError;
use rustfs_ecstore::global::is_erasure_sd;
use rustfs_ecstore::store::ECStore;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

const ENV_SCANNER_START_DELAY_SECS_DEPRECATED: &str = "RUSTFS_DATA_SCANNER_START_DELAY_SECS";

/// Returns the base cycle interval.
/// Priority order:
/// 1. RUSTFS_SCANNER_CYCLE (if set, overrides everything)
/// 2. RUSTFS_SCANNER_START_DELAY_SECS (for backward compatibility)
/// 3. RUSTFS_SCANNER_SPEED preset
fn cycle_interval() -> Duration {
    if let Some(secs) = rustfs_utils::get_env_opt_u64(ENV_SCANNER_CYCLE) {
        return Duration::from_secs(secs);
    }
    if let Some(secs) = scanner_start_delay_secs() {
        return Duration::from_secs(secs);
    }
    let speed_str = rustfs_utils::get_env_str(ENV_SCANNER_SPEED, DEFAULT_SCANNER_SPEED);
    ScannerSpeed::from_env_str(&speed_str).cycle_interval()
}

fn scanner_start_delay_secs() -> Option<u64> {
    let deprecated = [ENV_SCANNER_START_DELAY_SECS_DEPRECATED];
    rustfs_utils::get_env_opt_u64_with_aliases(ENV_SCANNER_START_DELAY_SECS, &deprecated)
}

/// Compute a randomized inter-cycle sleep.
// Delay is scan interval +- 10%, with a floor of 1 second.
fn randomized_cycle_delay() -> Duration {
    randomized_cycle_delay_for(cycle_interval())
}

fn randomized_cycle_delay_for(interval: Duration) -> Duration {
    let interval = interval.max(Duration::from_secs(1));
    // Uniform in [-0.1, 0.1), keeping actual delay within 10% of interval.
    let jitter_factor = (rand::random::<f64>() * 0.2) - 0.1;
    let delay = interval.mul_f64(1.0 + jitter_factor);
    delay.max(Duration::from_secs(1))
}

fn initial_scanner_delay() -> Duration {
    initial_scanner_delay_for(scanner_start_delay_secs())
}

fn initial_scanner_delay_for(start_delay_secs: Option<u64>) -> Duration {
    start_delay_secs
        .map(|secs| randomized_cycle_delay_for(Duration::from_secs(secs)))
        .unwrap_or_else(|| Duration::from_secs(rand::random::<u64>() % 5))
}

pub async fn init_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) {
    // Force init global sleeper so config is read once at startup.
    let _ = &*SCANNER_SLEEPER;

    let ctx_clone = ctx;
    let storeapi_clone = storeapi;
    tokio::spawn(async move {
        let sleep_time = initial_scanner_delay();
        tokio::time::sleep(sleep_time).await;

        loop {
            if ctx_clone.is_cancelled() {
                break;
            }

            if let Err(e) = run_data_scanner(ctx_clone.clone(), storeapi_clone.clone()).await {
                error!("Failed to run data scanner: {e}");
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

fn get_cycle_scan_mode(_current_cycle: u64, _bitrot_start_cycle: u64, _bitrot_start_time: Option<DateTime<Utc>>) -> HealScanMode {
    // TODO: from config
    HealScanMode::Normal
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
            error!("Failed to unmarshal background heal info from {}: {}", &*BACKGROUND_HEAL_INFO_PATH, e);
            BackgroundHealInfo::default()
        }),
        Err(e) => {
            // Only log if it's not a ConfigNotFound error
            if e != EcstoreError::ConfigNotFound {
                warn!("Failed to read background heal info from {}: {}", &*BACKGROUND_HEAL_INFO_PATH, e);
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
            error!("Failed to marshal background heal info: {}", e);
            return;
        }
    };

    // Save configuration
    if let Err(e) = save_config(storeapi, &BACKGROUND_HEAL_INFO_PATH, data).await {
        warn!("Failed to save background heal info to {}: {}", &*BACKGROUND_HEAL_INFO_PATH, e);
    }
}

/// Get lock acquire timeout from environment variable RUSTFS_LOCK_ACQUIRE_TIMEOUT (in seconds)
/// Defaults to 5 seconds if not set or invalid
/// For distributed environments with multiple nodes, a longer timeout may be needed
fn get_lock_acquire_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64("RUSTFS_LOCK_ACQUIRE_TIMEOUT", 5))
}

#[instrument(skip_all)]
async fn run_data_scanner_cycle(ctx: &CancellationToken, storeapi: &Arc<ECStore>, cycle_info: &mut CurrentCycle) {
    let _activity_guard = ScannerActivityGuard::new();
    SCANNER_SLEEPER.refresh_from_env();
    info!("Start run data scanner cycle");
    cycle_info.current = cycle_info.next;
    let now = Instant::now();
    cycle_info.started = Utc::now();

    global_metrics().set_cycle(Some(cycle_info.clone())).await;

    let background_heal_info = read_background_heal_info(storeapi.clone()).await;

    let scan_mode = get_cycle_scan_mode(
        cycle_info.current,
        background_heal_info.bitrot_start_cycle,
        background_heal_info.bitrot_start_time,
    );
    if background_heal_info.current_scan_mode != scan_mode {
        let mut new_heal_info = background_heal_info.clone();
        new_heal_info.current_scan_mode = scan_mode;

        if scan_mode == HealScanMode::Deep {
            new_heal_info.bitrot_start_cycle = cycle_info.current;
            new_heal_info.bitrot_start_time = Some(Utc::now());
        }

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
    if let Err(e) = storeapi
        .clone()
        .nsscanner(ctx.clone(), sender, cycle_info.current, scan_mode)
        .await
    {
        error!(duration = ?now.elapsed(), "Fail run data scanner cycle: {e}");
        emit_scan_cycle_complete(false, cycle_start.elapsed());
        return;
    }
    done_cycle();
    emit_scan_cycle_complete(true, cycle_start.elapsed());

    cycle_info.next += 1;
    cycle_info.current = 0;
    cycle_info.cycle_completed.push(Utc::now());

    info!(duration = ?now.elapsed(), cycles_total=cycle_info.cycle_completed.len(), "Success run data scanner cycle");

    if cycle_info.cycle_completed.len() >= data_usage_update_dir_cycles() as usize {
        cycle_info.cycle_completed = cycle_info.cycle_completed.split_off(data_usage_update_dir_cycles() as usize);
    }

    global_metrics().set_cycle(Some(cycle_info.clone())).await;

    let cycle_info_buf = cycle_info.marshal().unwrap_or_default();

    let mut buf = Vec::with_capacity(cycle_info_buf.len() + 8);
    buf.extend_from_slice(&cycle_info.next.to_le_bytes());
    buf.extend_from_slice(&cycle_info_buf);

    if let Err(e) = save_config(storeapi.clone(), &DATA_USAGE_BLOOM_NAME_PATH, buf).await {
        error!("Failed to save data usage bloom name to {}: {}", &*DATA_USAGE_BLOOM_NAME_PATH, e);
    } else {
        info!("Data usage bloom name saved successfully");
    }
}

pub async fn run_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) -> Result<(), ScannerError> {
    // Acquire leader lock (write lock) to ensure only one scanner runs
    let _guard = match storeapi.new_ns_lock(RUSTFS_META_BUCKET, "leader.lock").await {
        Ok(ns_lock) => match ns_lock.get_write_lock_quiet(get_lock_acquire_timeout()).await {
            Ok(guard) => {
                debug!("run_data_scanner: acquired leader write lock");
                guard
            }
            Err(e) => {
                debug!("run_data_scanner: other node is running, failed to acquire leader write lock: {:?}", e);
                return Ok(());
            }
        },
        Err(e) => {
            error!("run_data_scanner: failed to create namespace lock: {e}");
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
            warn!("Failed to unmarshal cycle info: {e}");
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

    debug!("Data scanner done");

    Ok(())
}

/// Store data usage info in backend. Will store all objects sent on the receiver until closed.
#[instrument(skip(ctx, storeapi))]
pub async fn store_data_usage_in_backend(
    ctx: CancellationToken,
    storeapi: Arc<ECStore>,
    mut receiver: mpsc::Receiver<DataUsageInfo>,
) {
    let mut attempts = 1u32;

    while let Some(data_usage_info) = receiver.recv().await {
        let _activity_guard = ScannerActivityGuard::new();
        if ctx.is_cancelled() {
            break;
        }

        // Serialize to JSON
        let data = match serde_json::to_vec(&data_usage_info) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to marshal data usage info: {}", e);
                continue;
            }
        };

        // Save a backup every 10th update
        if attempts > 10 {
            let backup_path = format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str());
            if let Err(e) = save_config(storeapi.clone(), &backup_path, data.clone()).await {
                warn!("Failed to save data usage backup to {}: {}", backup_path, e);
            }
            attempts = 1;
        }

        // Save main configuration
        if let Err(e) = save_config(storeapi.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), data).await {
            error!("Failed to save data usage info to {}: {e}", DATA_USAGE_OBJ_NAME_PATH.as_str());
        }

        attempts += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use temp_env::{with_var, with_var_unset};

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
    fn test_cycle_interval_prefers_explicit_cycle_override() {
        with_var(ENV_SCANNER_SPEED, Some("slowest"), || {
            with_var(ENV_SCANNER_CYCLE, Some("42"), || {
                assert_eq!(cycle_interval(), Duration::from_secs(42));
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
}
