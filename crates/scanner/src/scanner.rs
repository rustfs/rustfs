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
use crate::metrics::CurrentCycle;
use crate::metrics::global_metrics;
use crate::scanner_folder::data_usage_update_dir_cycles;
use crate::scanner_io::ScannerIO;
use crate::{DataUsageInfo, ScannerError};
use chrono::{DateTime, Utc};
use rustfs_common::heal_channel::HealScanMode;
use rustfs_config::{DEFAULT_DATA_SCANNER_START_DELAY_SECS, ENV_DATA_SCANNER_START_DELAY_SECS};
use rustfs_ecstore::StorageAPI as _;
use rustfs_ecstore::config::com::{read_config, save_config};
use rustfs_ecstore::disk::RUSTFS_META_BUCKET;
use rustfs_ecstore::error::Error as EcstoreError;
use rustfs_ecstore::global::is_erasure_sd;
use rustfs_ecstore::store::ECStore;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

fn data_scanner_start_delay() -> Duration {
    let secs = rustfs_utils::get_env_u64(ENV_DATA_SCANNER_START_DELAY_SECS, DEFAULT_DATA_SCANNER_START_DELAY_SECS);
    Duration::from_secs(secs)
}

pub async fn init_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) {
    let ctx_clone = ctx.clone();
    let storeapi_clone = storeapi.clone();
    tokio::spawn(async move {
        let sleep_time = Duration::from_secs(rand::random::<u64>() % 5);
        tokio::time::sleep(sleep_time).await;

        loop {
            if ctx_clone.is_cancelled() {
                break;
            }

            if let Err(e) = run_data_scanner(ctx_clone.clone(), storeapi_clone.clone()).await {
                error!("Failed to run data scanner: {e}");
            }
            tokio::time::sleep(data_scanner_start_delay()).await;
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
        Ok(buf) => match serde_json::from_slice::<BackgroundHealInfo>(&buf) {
            Ok(info) => info,
            Err(e) => {
                error!("Failed to unmarshal background heal info from {}: {}", &*BACKGROUND_HEAL_INFO_PATH, e);
                BackgroundHealInfo::default()
            }
        },
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

pub async fn run_data_scanner(ctx: CancellationToken, storeapi: Arc<ECStore>) -> Result<(), ScannerError> {
    // Acquire leader lock (write lock) to ensure only one scanner runs
    let _guard = match storeapi.new_ns_lock(RUSTFS_META_BUCKET, "leader.lock").await {
        Ok(ns_lock) => match ns_lock.get_write_lock(get_lock_acquire_timeout()).await {
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

    let mut ticker = tokio::time::interval(data_scanner_start_delay());
    loop {
        tokio::select! {
            _ = ctx.cancelled() => {
                break;
            }
            _ = ticker.tick() => {

                cycle_info.current = cycle_info.next;
                cycle_info.started = Utc::now();

                global_metrics().set_cycle(Some(cycle_info.clone())).await;

                let background_heal_info = read_background_heal_info(storeapi.clone()).await;

                let scan_mode = get_cycle_scan_mode(cycle_info.current, background_heal_info.bitrot_start_cycle, background_heal_info.bitrot_start_time);
                if background_heal_info.current_scan_mode != scan_mode {
                    let mut new_heal_info = background_heal_info.clone();
                    new_heal_info.current_scan_mode = scan_mode;

                    if scan_mode == HealScanMode::Deep {
                        new_heal_info.bitrot_start_cycle = cycle_info.current;
                        new_heal_info.bitrot_start_time = Some(Utc::now());
                    }

                    save_background_heal_info(storeapi.clone(), new_heal_info).await;
                }



                let (sender, receiver) = tokio::sync::mpsc::channel::<DataUsageInfo>(1);
                let storeapi_clone = storeapi.clone();
                let ctx_clone = ctx.clone();
                tokio::spawn(async move {
                    store_data_usage_in_backend(ctx_clone, storeapi_clone, receiver).await;
                });


               if let Err(e) = storeapi.clone().nsscanner(ctx.clone(), sender, cycle_info.current, scan_mode).await {
                error!("Failed to scan namespace: {e}");
               } else {
                info!("Namespace scanned successfully");

                cycle_info.next +=1;
                cycle_info.current = 0;
                cycle_info.cycle_completed.push(Utc::now());

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

                ticker.reset();
            }
        }
    }

    global_metrics().set_cycle(None).await;

    debug!("Data scanner done");

    Ok(())
}

/// Store data usage info in backend. Will store all objects sent on the receiver until closed.
pub async fn store_data_usage_in_backend(
    ctx: CancellationToken,
    storeapi: Arc<ECStore>,
    mut receiver: mpsc::Receiver<DataUsageInfo>,
) {
    let mut attempts = 1u32;

    while let Some(data_usage_info) = receiver.recv().await {
        if ctx.is_cancelled() {
            break;
        }

        debug!("store_data_usage_in_backend: received data usage info: {:?}", &data_usage_info);

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
            let backup_path = format!("{:?}.bkp", &DATA_USAGE_OBJ_NAME_PATH);
            if let Err(e) = save_config(storeapi.clone(), &backup_path, data.clone()).await {
                warn!("Failed to save data usage backup to {}: {}", backup_path, e);
            }
            attempts = 1;
        }

        // Save main configuration
        if let Err(e) = save_config(storeapi.clone(), &DATA_USAGE_OBJ_NAME_PATH, data).await {
            error!("Failed to save data usage info to {:?}: {e}", &DATA_USAGE_OBJ_NAME_PATH);
        }

        attempts += 1;
    }
}
