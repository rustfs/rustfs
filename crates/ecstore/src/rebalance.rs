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

use crate::StorageAPI;
use crate::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use crate::config::com::{read_config_with_metadata, save_config_with_opts};
use crate::disk::error::DiskError;
use crate::error::{Error, Result};
use crate::error::{is_err_data_movement_overwrite, is_err_object_not_found, is_err_version_not_found};
use crate::global::get_global_endpoints;
use crate::pools::ListCallback;
use crate::set_disk::SetDisks;
use crate::store::ECStore;
use crate::store_api::{CompletePart, GetObjectReader, ObjectIO, ObjectOptions, PutObjReader};
use http::HeaderMap;
use rustfs_common::defer;
use rustfs_filemeta::{FileInfo, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use rustfs_rio::{HashReader, WarpReader};
use rustfs_utils::path::encode_dir_object;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

const REBAL_META_FMT: u16 = 1; // Replace with actual format value
const REBAL_META_VER: u16 = 1; // Replace with actual version value
const REBAL_META_NAME: &str = "rebalance.bin";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RebalanceStats {
    #[serde(rename = "ifs")]
    pub init_free_space: u64, // Pool free space at the start of rebalance
    #[serde(rename = "ic")]
    pub init_capacity: u64, // Pool capacity at the start of rebalance
    #[serde(rename = "bus")]
    pub buckets: Vec<String>, // Buckets being rebalanced or to be rebalanced
    #[serde(rename = "rbs")]
    pub rebalanced_buckets: Vec<String>, // Buckets rebalanced
    #[serde(rename = "bu")]
    pub bucket: String, // Last rebalanced bucket
    #[serde(rename = "ob")]
    pub object: String, // Last rebalanced object
    #[serde(rename = "no")]
    pub num_objects: u64, // Number of objects rebalanced
    #[serde(rename = "nv")]
    pub num_versions: u64, // Number of versions rebalanced
    #[serde(rename = "bs")]
    pub bytes: u64, // Number of bytes rebalanced
    #[serde(rename = "par")]
    pub participating: bool, // Whether the pool is participating in rebalance
    #[serde(rename = "inf")]
    pub info: RebalanceInfo, // Rebalance operation info
}

impl RebalanceStats {
    pub fn update(&mut self, bucket: String, fi: &FileInfo) {
        if fi.is_latest {
            self.num_objects += 1;
        }

        self.num_versions += 1;
        let on_disk_size = if !fi.deleted {
            fi.size * (fi.erasure.data_blocks + fi.erasure.parity_blocks) as i64 / fi.erasure.data_blocks as i64
        } else {
            0
        };
        self.bytes += on_disk_size as u64;
        self.bucket = bucket;
        self.object = fi.name.clone();
    }
}

pub type RStats = Vec<Arc<RebalanceStats>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RebalStatus {
    #[default]
    None,
    Started,
    Completed,
    Stopped,
    Failed,
}

impl fmt::Display for RebalStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match self {
            RebalStatus::None => "None",
            RebalStatus::Started => "Started",
            RebalStatus::Completed => "Completed",
            RebalStatus::Stopped => "Stopped",
            RebalStatus::Failed => "Failed",
        };
        write!(f, "{status}")
    }
}

impl From<u8> for RebalStatus {
    fn from(value: u8) -> Self {
        match value {
            1 => RebalStatus::Started,
            2 => RebalStatus::Completed,
            3 => RebalStatus::Stopped,
            4 => RebalStatus::Failed,
            _ => RebalStatus::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RebalSaveOpt {
    #[default]
    Stats,
    StoppedAt,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RebalanceInfo {
    #[serde(rename = "startTs")]
    pub start_time: Option<OffsetDateTime>, // Time at which rebalance-start was issued
    #[serde(rename = "stopTs")]
    pub end_time: Option<OffsetDateTime>, // Time at which rebalance operation completed or rebalance-stop was called
    #[serde(rename = "status")]
    pub status: RebalStatus, // Current state of rebalance operation
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct DiskStat {
    pub total_space: u64,
    pub available_space: u64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct RebalanceMeta {
    #[serde(skip)]
    pub cancel: Option<CancellationToken>, // To be invoked on rebalance-stop
    #[serde(skip)]
    pub last_refreshed_at: Option<OffsetDateTime>,
    #[serde(rename = "stopTs")]
    pub stopped_at: Option<OffsetDateTime>, // Time when rebalance-stop was issued
    #[serde(rename = "id")]
    pub id: String, // ID of the ongoing rebalance operation
    #[serde(rename = "pf")]
    pub percent_free_goal: f64, // Computed from total free space and capacity at the start of rebalance
    #[serde(rename = "rss")]
    pub pool_stats: Vec<RebalanceStats>, // Per-pool rebalance stats keyed by pool index
}

impl RebalanceMeta {
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn load<S: StorageAPI>(&mut self, store: Arc<S>) -> Result<()> {
        self.load_with_opts(store, ObjectOptions::default()).await
    }

    pub async fn load_with_opts<S: StorageAPI>(&mut self, store: Arc<S>, opts: ObjectOptions) -> Result<()> {
        let (data, _) = read_config_with_metadata(store, REBAL_META_NAME, &opts).await?;
        if data.is_empty() {
            info!("rebalanceMeta load_with_opts: no data");
            return Ok(());
        }
        if data.len() <= 4 {
            return Err(Error::other("rebalanceMeta load_with_opts: no data"));
        }

        // Read header
        match u16::from_le_bytes([data[0], data[1]]) {
            REBAL_META_FMT => {}
            fmt => return Err(Error::other(format!("rebalanceMeta load_with_opts: unknown format: {fmt}"))),
        }
        match u16::from_le_bytes([data[2], data[3]]) {
            REBAL_META_VER => {}
            ver => return Err(Error::other(format!("rebalanceMeta load_with_opts: unknown version: {ver}"))),
        }

        let meta: Self = rmp_serde::from_read(Cursor::new(&data[4..]))?;
        *self = meta;

        self.last_refreshed_at = Some(OffsetDateTime::now_utc());

        info!("rebalanceMeta load_with_opts: loaded meta done");
        Ok(())
    }

    pub async fn save<S: StorageAPI>(&self, store: Arc<S>) -> Result<()> {
        self.save_with_opts(store, ObjectOptions::default()).await
    }

    pub async fn save_with_opts<S: StorageAPI>(&self, store: Arc<S>, opts: ObjectOptions) -> Result<()> {
        if self.pool_stats.is_empty() {
            info!("rebalanceMeta save_with_opts: no pool stats");
            return Ok(());
        }

        let mut data = Vec::new();

        // Initialize the header
        data.extend(&REBAL_META_FMT.to_le_bytes());
        data.extend(&REBAL_META_VER.to_le_bytes());

        let msg = rmp_serde::to_vec(self)?;
        data.extend(msg);

        save_config_with_opts(store, REBAL_META_NAME, data, &opts).await?;

        Ok(())
    }
}

impl ECStore {
    #[tracing::instrument(skip_all)]
    pub async fn load_rebalance_meta(&self) -> Result<()> {
        let mut meta = RebalanceMeta::new();
        info!("rebalanceMeta: store load rebalance meta");
        match meta.load(self.pools[0].clone()).await {
            Ok(_) => {
                info!("rebalanceMeta: rebalance meta loaded0");
                {
                    let mut rebalance_meta = self.rebalance_meta.write().await;

                    *rebalance_meta = Some(meta);

                    drop(rebalance_meta);
                }

                info!("rebalanceMeta: rebalance meta loaded1");

                if let Err(err) = self.update_rebalance_stats().await {
                    error!("Failed to update rebalance stats: {}", err);
                } else {
                    info!("rebalanceMeta: rebalance meta loaded2");
                }
            }
            Err(err) => {
                if err != Error::ConfigNotFound {
                    error!("rebalanceMeta: load rebalance meta err {:?}", &err);
                    return Err(err);
                }

                info!("rebalanceMeta: not found, rebalance not started");
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn update_rebalance_stats(&self) -> Result<()> {
        let mut ok = false;

        let pool_stats = {
            let rebalance_meta = self.rebalance_meta.read().await;
            rebalance_meta.as_ref().map(|v| v.pool_stats.clone()).unwrap_or_default()
        };

        info!("update_rebalance_stats: pool_stats: {:?}", &pool_stats);

        for i in 0..self.pools.len() {
            if pool_stats.get(i).is_none() {
                info!("update_rebalance_stats: pool {} not found", i);
                let mut rebalance_meta = self.rebalance_meta.write().await;
                info!("update_rebalance_stats: pool {} not found, add", i);
                if let Some(meta) = rebalance_meta.as_mut() {
                    meta.pool_stats.push(RebalanceStats::default());
                }
                ok = true;
                drop(rebalance_meta);
            }
        }

        if ok {
            info!("update_rebalance_stats: save rebalance meta");

            let rebalance_meta = self.rebalance_meta.read().await;
            if let Some(meta) = rebalance_meta.as_ref() {
                meta.save(self.pools[0].clone()).await?;
            }
        }

        Ok(())
    }

    // async fn find_index(&self, index: usize) -> Option<usize> {
    //     if let Some(meta) = self.rebalance_meta.read().await.as_ref() {
    //         return meta.pool_stats.get(index).map(|_v| index);
    //     }

    //     None
    // }

    #[tracing::instrument(skip(self))]
    pub async fn init_rebalance_meta(&self, bucktes: Vec<String>) -> Result<String> {
        info!("init_rebalance_meta: start rebalance");
        let si = self.storage_info().await;

        let mut disk_stats = vec![DiskStat::default(); self.pools.len()];

        let mut total_cap = 0;
        let mut total_free = 0;
        for disk in si.disks.iter() {
            if disk.pool_index < 0 || disk_stats.len() <= disk.pool_index as usize {
                continue;
            }

            total_cap += disk.total_space;
            total_free += disk.available_space;

            disk_stats[disk.pool_index as usize].total_space += disk.total_space;
            disk_stats[disk.pool_index as usize].available_space += disk.available_space;
        }

        let percent_free_goal = total_free as f64 / total_cap as f64;

        let mut pool_stats = Vec::with_capacity(self.pools.len());

        let now = OffsetDateTime::now_utc();

        for disk_stat in disk_stats.iter() {
            let mut pool_stat = RebalanceStats {
                init_free_space: disk_stat.available_space,
                init_capacity: disk_stat.total_space,
                buckets: bucktes.clone(),
                rebalanced_buckets: Vec::with_capacity(bucktes.len()),
                ..Default::default()
            };

            if (disk_stat.available_space as f64 / disk_stat.total_space as f64) < percent_free_goal {
                pool_stat.participating = true;
                pool_stat.info = RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                };
            }

            pool_stats.push(pool_stat);
        }

        let meta = RebalanceMeta {
            id: Uuid::new_v4().to_string(),
            percent_free_goal,
            pool_stats,
            ..Default::default()
        };

        meta.save(self.pools[0].clone()).await?;

        info!("init_rebalance_meta: rebalance meta saved");

        let id = meta.id.clone();

        {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            *rebalance_meta = Some(meta);
            drop(rebalance_meta);
        }

        Ok(id)
    }

    #[tracing::instrument(skip(self, fi))]
    pub async fn update_pool_stats(&self, pool_index: usize, bucket: String, fi: &FileInfo) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        if let Some(meta) = rebalance_meta.as_mut() {
            if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                pool_stat.update(bucket, fi);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn next_rebal_bucket(&self, pool_index: usize) -> Result<Option<String>> {
        info!("next_rebal_bucket: pool_index: {}", pool_index);
        let rebalance_meta = self.rebalance_meta.read().await;
        info!("next_rebal_bucket: rebalance_meta: {:?}", rebalance_meta);
        if let Some(meta) = rebalance_meta.as_ref() {
            if let Some(pool_stat) = meta.pool_stats.get(pool_index) {
                if pool_stat.info.status == RebalStatus::Completed || !pool_stat.participating {
                    info!("next_rebal_bucket: pool_index: {} completed or not participating", pool_index);
                    return Ok(None);
                }

                if pool_stat.buckets.is_empty() {
                    info!("next_rebal_bucket: pool_index: {} buckets is empty", pool_index);
                    return Ok(None);
                }
                info!("next_rebal_bucket: pool_index: {} bucket: {}", pool_index, pool_stat.buckets[0]);
                return Ok(Some(pool_stat.buckets[0].clone()));
            }
        }

        info!("next_rebal_bucket: pool_index: {} None", pool_index);
        Ok(None)
    }

    #[tracing::instrument(skip(self))]
    pub async fn bucket_rebalance_done(&self, pool_index: usize, bucket: String) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        if let Some(meta) = rebalance_meta.as_mut() {
            if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                info!("bucket_rebalance_done: buckets {:?}", &pool_stat.buckets);

                // 使用 retain 来过滤掉要删除的 bucket
                let mut found = false;
                pool_stat.buckets.retain(|b| {
                    if b.as_str() == bucket.as_str() {
                        found = true;
                        pool_stat.rebalanced_buckets.push(b.clone());
                        false // 删除这个元素
                    } else {
                        true // 保留这个元素
                    }
                });

                if found {
                    info!("bucket_rebalance_done: bucket {} rebalanced", &bucket);
                    return Ok(());
                } else {
                    info!("bucket_rebalance_done: bucket {} not found", bucket);
                }
            }
        }
        info!("bucket_rebalance_done: bucket {} not found", bucket);
        Ok(())
    }

    pub async fn is_rebalance_started(&self) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(ref meta) = *rebalance_meta {
            if meta.stopped_at.is_some() {
                info!("is_rebalance_started: rebalance stopped");
                return false;
            }

            meta.pool_stats.iter().enumerate().for_each(|(i, v)| {
                info!(
                    "is_rebalance_started: pool_index: {}, participating: {:?}, status: {:?}",
                    i, v.participating, v.info.status
                );
            });

            if meta
                .pool_stats
                .iter()
                .any(|v| v.participating && v.info.status != RebalStatus::Completed)
            {
                info!("is_rebalance_started: rebalance started");
                return true;
            }
        }

        info!("is_rebalance_started: rebalance not started");
        false
    }

    pub async fn is_pool_rebalancing(&self, pool_index: usize) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(ref meta) = *rebalance_meta {
            if meta.stopped_at.is_some() {
                return false;
            }

            if let Some(pool_stat) = meta.pool_stats.get(pool_index) {
                return pool_stat.participating && pool_stat.info.status == RebalStatus::Started;
            }
        }

        false
    }

    #[tracing::instrument(skip(self))]
    pub async fn stop_rebalance(self: &Arc<Self>) -> Result<()> {
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(meta) = rebalance_meta.as_ref() {
            if let Some(cancel_tx) = meta.cancel.as_ref() {
                cancel_tx.cancel();
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn start_rebalance(self: &Arc<Self>) {
        info!("start_rebalance: start rebalance");
        // let rebalance_meta = self.rebalance_meta.read().await;

        let cancel_tx = CancellationToken::new();
        let rx = cancel_tx.clone();

        {
            let mut rebalance_meta = self.rebalance_meta.write().await;

            if let Some(meta) = rebalance_meta.as_mut() {
                meta.cancel = Some(cancel_tx)
            } else {
                info!("start_rebalance: rebalance_meta is None exit");
                return;
            }

            drop(rebalance_meta);
        }

        let participants = {
            if let Some(ref meta) = *self.rebalance_meta.read().await {
                // if meta.stopped_at.is_some() {
                //     warn!("start_rebalance: rebalance already stopped exit");
                //     return;
                // }

                let mut participants = vec![false; meta.pool_stats.len()];
                for (i, pool_stat) in meta.pool_stats.iter().enumerate() {
                    info!("start_rebalance: pool {} status: {:?}", i, pool_stat.info.status);
                    if pool_stat.info.status != RebalStatus::Started {
                        info!("start_rebalance: pool {} not started, skipping", i);
                        continue;
                    }

                    info!("start_rebalance: pool {} participating: {:?}", i, pool_stat.participating);
                    participants[i] = pool_stat.participating;
                }
                participants
            } else {
                info!("start_rebalance:2  rebalance_meta is None exit");
                Vec::new()
            }
        };

        for (idx, participating) in participants.iter().enumerate() {
            if !*participating {
                info!("start_rebalance: pool {} is not participating, skipping", idx);
                continue;
            }

            if !get_global_endpoints().as_ref().get(idx).is_some_and(|v| {
                info!("start_rebalance: pool {} endpoints: {:?}", idx, v.endpoints);
                v.endpoints.as_ref().first().is_some_and(|e| {
                    info!("start_rebalance: pool {} endpoint: {:?}, is_local: {}", idx, e, e.is_local);
                    e.is_local
                })
            }) {
                info!("start_rebalance: pool {} is not local, skipping", idx);
                continue;
            }

            let pool_idx = idx;
            let store = self.clone();
            let rx_clone = rx.clone();
            tokio::spawn(async move {
                if let Err(err) = store.rebalance_buckets(rx_clone, pool_idx).await {
                    error!("Rebalance failed for pool {}: {}", pool_idx, err);
                } else {
                    info!("Rebalance completed for pool {}", pool_idx);
                }
            });
        }

        info!("start_rebalance: rebalance started done");
    }

    #[tracing::instrument(skip(self, rx))]
    async fn rebalance_buckets(self: &Arc<Self>, rx: CancellationToken, pool_index: usize) -> Result<()> {
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<Result<()>>(1);

        // Save rebalance metadata periodically
        let store = self.clone();
        let save_task = tokio::spawn(async move {
            let mut timer = tokio::time::interval_at(Instant::now() + Duration::from_secs(30), Duration::from_secs(10));
            let mut msg: String;
            let mut quit = false;

            loop {
                tokio::select! {
                    //  TODO: cancel rebalance
                    Some(result) = done_rx.recv() => {
                        quit = true;
                        let now = OffsetDateTime::now_utc();

                        let state = match result {
                            Ok(_) => {
                                info!("rebalance_buckets: completed");
                                msg = format!("Rebalance completed at {now:?}");
                                RebalStatus::Completed},
                            Err(err) => {
                                info!("rebalance_buckets: error: {:?}", err);
                                // TODO: check stop
                                if err.to_string().contains("canceled") {
                                    msg = format!("Rebalance stopped at {now:?}");
                                    RebalStatus::Stopped
                                } else {
                                    msg = format!("Rebalance stopped at {now:?} with err {err:?}");
                                    RebalStatus::Failed
                                }
                            }
                        };

                        {
                            info!("rebalance_buckets: save rebalance meta, pool_index: {}, state: {:?}", pool_index, state);
                            let mut rebalance_meta = store.rebalance_meta.write().await;

                            if let Some(rbm) = rebalance_meta.as_mut() {
                                info!("rebalance_buckets: save rebalance meta2, pool_index: {}, state: {:?}", pool_index, state);
                                rbm.pool_stats[pool_index].info.status = state;
                                rbm.pool_stats[pool_index].info.end_time = Some(now);
                            }
                        }


                    }
                    _ = timer.tick() => {
                        let now = OffsetDateTime::now_utc();
                        msg = format!("Saving rebalance metadata at {now:?}");
                    }
                }

                if let Err(err) = store.save_rebalance_stats(pool_index, RebalSaveOpt::Stats).await {
                    error!("{} err: {:?}", msg, err);
                } else {
                    info!(msg);
                }

                if quit {
                    info!("{}: exiting save_task", msg);
                    return;
                }

                timer.reset();
            }
        });

        info!("Pool {} rebalancing is started", pool_index);

        loop {
            if rx.is_cancelled() {
                info!("Pool {} rebalancing is stopped", pool_index);
                done_tx.send(Err(Error::other("rebalance stopped canceled"))).await.ok();
                break;
            }

            if let Some(bucket) = self.next_rebal_bucket(pool_index).await? {
                info!("Rebalancing bucket: start {}", bucket);

                if let Err(err) = self.rebalance_bucket(rx.clone(), bucket.clone(), pool_index).await {
                    if err.to_string().contains("not initialized") {
                        info!("rebalance_bucket: rebalance not initialized, continue");
                        continue;
                    }
                    error!("Error rebalancing bucket {}: {:?}", bucket, err);
                    done_tx.send(Err(err)).await.ok();
                    break;
                }

                info!("Rebalance bucket: done {} ", bucket);
                self.bucket_rebalance_done(pool_index, bucket).await?;
            } else {
                info!("Rebalance bucket: no bucket to rebalance");
                break;
            }
        }

        info!("Pool {} rebalancing is done", pool_index);

        done_tx.send(Ok(())).await.ok();
        save_task.await.ok();
        info!("Pool {} rebalancing is done2", pool_index);
        Ok(())
    }

    async fn check_if_rebalance_done(&self, pool_index: usize) -> bool {
        let mut rebalance_meta = self.rebalance_meta.write().await;

        if let Some(meta) = rebalance_meta.as_mut() {
            if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                // Check if the pool's rebalance status is already completed
                if pool_stat.info.status == RebalStatus::Completed {
                    info!("check_if_rebalance_done: pool {} is already completed", pool_index);
                    return true;
                }

                // Calculate the percentage of free space improvement
                let pfi = (pool_stat.init_free_space + pool_stat.bytes) as f64 / pool_stat.init_capacity as f64;

                // Mark pool rebalance as done if within 5% of the PercentFreeGoal
                if (pfi - meta.percent_free_goal).abs() <= 0.05 {
                    pool_stat.info.status = RebalStatus::Completed;
                    pool_stat.info.end_time = Some(OffsetDateTime::now_utc());
                    info!("check_if_rebalance_done: pool {} is completed, pfi: {}", pool_index, pfi);
                    return true;
                }
            }
        }

        false
    }

    #[allow(unused_assignments)]
    #[tracing::instrument(skip(self, set))]
    async fn rebalance_entry(
        self: Arc<Self>,
        bucket: String,
        pool_index: usize,
        entry: MetaCacheEntry,
        set: Arc<SetDisks>,
        // wk: Arc<Workers>,
    ) {
        info!("rebalance_entry: start rebalance_entry");

        // defer!(|| async {
        //     warn!("rebalance_entry: defer give worker start");
        //     wk.give().await;
        //     warn!("rebalance_entry: defer give worker done");
        // });

        if entry.is_dir() {
            info!("rebalance_entry: entry is dir, skipping");
            return;
        }

        if self.check_if_rebalance_done(pool_index).await {
            info!("rebalance_entry: rebalance done, skipping pool {}", pool_index);
            return;
        }

        let mut fivs = match entry.file_info_versions(&bucket) {
            Ok(fivs) => fivs,
            Err(err) => {
                error!("rebalance_entry Error getting file info versions: {}", err);
                info!("rebalance_entry: Error getting file info versions, skipping");
                return;
            }
        };

        fivs.versions.sort_by(|a, b| b.mod_time.cmp(&a.mod_time));

        let mut rebalanced: usize = 0;
        let expired: usize = 0;
        for version in fivs.versions.iter() {
            if version.is_remote() {
                info!("rebalance_entry Entry {} is remote, skipping", version.name);
                continue;
            }
            // TODO: filterLifecycle

            let remaining_versions = fivs.versions.len() - expired;
            if version.deleted && remaining_versions == 1 {
                rebalanced += 1;
                info!("rebalance_entry Entry {} is deleted and last version, skipping", version.name);
                continue;
            }
            let version_id = version.version_id.map(|v| v.to_string());

            let mut ignore = false;
            let mut failure = false;
            let mut error = None;
            if version.deleted {
                if let Err(err) = set
                    .delete_object(
                        &bucket,
                        &version.name,
                        ObjectOptions {
                            versioned: true,
                            version_id: version_id.clone(),
                            mod_time: version.mod_time,
                            src_pool_idx: pool_index,
                            data_movement: true,
                            delete_marker: true,
                            skip_decommissioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err) {
                        ignore = true;
                        info!("rebalance_entry {} Entry {} is already deleted, skipping", &bucket, version.name);
                        continue;
                    }
                    error = Some(err);
                    failure = true;
                }

                if !failure {
                    error!("rebalance_entry {} Entry {} deleted successfully", &bucket, &version.name);
                    let _ = self.update_pool_stats(pool_index, bucket.clone(), version).await;

                    rebalanced += 1;
                } else {
                    error!(
                        "rebalance_entry {} Error deleting entry {}/{:?}: {:?}",
                        &bucket, &version.name, &version.version_id, error
                    );
                }

                continue;
            }

            for _i in 0..3 {
                info!("rebalance_entry: get_object_reader, bucket: {}, version: {}", &bucket, &version.name);
                let rd = match set
                    .get_object_reader(
                        bucket.as_str(),
                        &encode_dir_object(&version.name),
                        None,
                        HeaderMap::new(),
                        &ObjectOptions {
                            version_id: version_id.clone(),
                            no_lock: true, // NoDecryption
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(rd) => rd,
                    Err(err) => {
                        if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                            ignore = true;
                            info!(
                                "rebalance_entry: get_object_reader, bucket: {}, version: {}, ignore",
                                &bucket, &version.name
                            );
                            break;
                        }

                        failure = true;
                        error!("rebalance_entry: get_object_reader err {:?}", &err);
                        continue;
                    }
                };

                if let Err(err) = self.clone().rebalance_object(pool_index, bucket.clone(), rd).await {
                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err) {
                        ignore = true;
                        info!("rebalance_entry {} Entry {} is already deleted, skipping", &bucket, version.name);
                        break;
                    }

                    failure = true;
                    error!("rebalance_entry: rebalance_object err {:?}", &err);
                    continue;
                }

                failure = false;
                info!("rebalance_entry {} Entry {} rebalanced successfully", &bucket, &version.name);
                break;
            }

            if ignore {
                info!("rebalance_entry {} Entry {} is already deleted, skipping", &bucket, version.name);
                continue;
            }

            if failure {
                error!(
                    "rebalance_entry {} Error rebalancing entry {}/{:?}: {:?}",
                    &bucket, &version.name, &version.version_id, error
                );
                break;
            }

            let _ = self.update_pool_stats(pool_index, bucket.clone(), version).await;
            rebalanced += 1;
        }

        if rebalanced == fivs.versions.len() {
            if let Err(err) = set
                .delete_object(
                    bucket.as_str(),
                    &encode_dir_object(&entry.name),
                    ObjectOptions {
                        delete_prefix: true,
                        delete_prefix_object: true,

                        ..Default::default()
                    },
                )
                .await
            {
                error!("rebalance_entry: delete_object err {:?}", &err);
            } else {
                info!("rebalance_entry {} Entry {} deleted successfully", &bucket, &entry.name);
            }
        }
    }

    #[tracing::instrument(skip(self, rd))]
    async fn rebalance_object(self: Arc<Self>, pool_idx: usize, bucket: String, rd: GetObjectReader) -> Result<()> {
        let object_info = rd.object_info.clone();

        // TODO: check : use size or actual_size ?
        let _actual_size = object_info.get_actual_size()?;

        if object_info.is_multipart() {
            let res = match self
                .new_multipart_upload(
                    &bucket,
                    &object_info.name,
                    &ObjectOptions {
                        version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
                        user_defined: object_info.user_defined.clone(),
                        src_pool_idx: pool_idx,
                        data_movement: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(res) => res,
                Err(err) => {
                    error!("rebalance_object: new_multipart_upload err {:?}", &err);
                    return Err(err);
                }
            };

            defer!(|| async {
                if let Err(err) = self
                    .abort_multipart_upload(&bucket, &object_info.name, &res.upload_id, &ObjectOptions::default())
                    .await
                {
                    error!("rebalance_object: abort_multipart_upload err {:?}", &err);
                }
            });

            let mut parts = vec![CompletePart::default(); object_info.parts.len()];

            let mut reader = rd.stream;

            for (i, part) in object_info.parts.iter().enumerate() {
                // 每次从 reader 中读取一个 part 上传

                let mut chunk = vec![0u8; part.size];

                reader.read_exact(&mut chunk).await?;

                // 每次从 reader 中读取一个 part 上传
                let mut data = PutObjReader::from_vec(chunk);

                let pi = match self
                    .put_object_part(
                        &bucket,
                        &object_info.name,
                        &res.upload_id,
                        part.number,
                        &mut data,
                        &ObjectOptions {
                            preserve_etag: Some(part.etag.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(pi) => pi,
                    Err(err) => {
                        error!("rebalance_object: put_object_part err {:?}", &err);
                        return Err(err);
                    }
                };

                parts[i] = CompletePart {
                    part_num: pi.part_num,
                    etag: pi.etag,
                    ..Default::default()
                };
            }

            if let Err(err) = self
                .clone()
                .complete_multipart_upload(
                    &bucket,
                    &object_info.name,
                    &res.upload_id,
                    parts,
                    &ObjectOptions {
                        data_movement: true,
                        mod_time: object_info.mod_time,
                        ..Default::default()
                    },
                )
                .await
            {
                error!("rebalance_object: complete_multipart_upload err {:?}", &err);
                return Err(err);
            }

            return Ok(());
        }

        let reader = BufReader::new(rd.stream);
        let hrd = HashReader::new(Box::new(WarpReader::new(reader)), object_info.size, object_info.size, None, None, false)?;
        let mut data = PutObjReader::new(hrd);

        if let Err(err) = self
            .put_object(
                &bucket,
                &object_info.name,
                &mut data,
                &ObjectOptions {
                    src_pool_idx: pool_idx,
                    data_movement: true,
                    version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
                    mod_time: object_info.mod_time,
                    user_defined: object_info.user_defined.clone(),
                    preserve_etag: object_info.etag.clone(),

                    ..Default::default()
                },
            )
            .await
        {
            error!("rebalance_object: put_object err {:?}", &err);
            return Err(err);
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    async fn rebalance_bucket(self: &Arc<Self>, rx: CancellationToken, bucket: String, pool_index: usize) -> Result<()> {
        // Placeholder for actual bucket rebalance logic
        info!("Rebalancing bucket {} in pool {}", bucket, pool_index);

        // TODO: other config
        // if bucket != RUSTFS_META_BUCKET{

        // }

        let pool = self.pools[pool_index].clone();

        let mut jobs = Vec::new();

        // let wk = Workers::new(pool.disk_set.len() * 2).map_err(Error::other)?;
        // wk.clone().take().await;
        for (set_idx, set) in pool.disk_set.iter().enumerate() {
            let rebalance_entry: ListCallback = Arc::new({
                let this = Arc::clone(self);
                let bucket = bucket.clone();
                // let wk = wk.clone();
                let set = set.clone();
                move |entry: MetaCacheEntry| {
                    let this = this.clone();
                    let bucket = bucket.clone();
                    // let wk = wk.clone();
                    let set = set.clone();
                    Box::pin(async move {
                        info!("rebalance_entry: rebalance_entry spawn start");
                        // wk.take().await;
                        // tokio::spawn(async move {
                        info!("rebalance_entry: rebalance_entry spawn start2");
                        this.rebalance_entry(bucket, pool_index, entry, set).await;
                        info!("rebalance_entry: rebalance_entry spawn done");
                        // });
                    })
                }
            });

            let set = set.clone();
            let rx = rx.clone();
            let bucket = bucket.clone();
            // let wk = wk.clone();

            let job = tokio::spawn(async move {
                if let Err(err) = set.list_objects_to_rebalance(rx, bucket, rebalance_entry).await {
                    error!("Rebalance worker {} error: {}", set_idx, err);
                } else {
                    info!("Rebalance worker {} done", set_idx);
                }
                // wk.clone().give().await;
            });

            jobs.push(job);
        }

        // wk.wait().await;
        for job in jobs {
            job.await.unwrap();
        }
        info!("rebalance_bucket: rebalance_bucket done");
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn save_rebalance_stats(&self, pool_idx: usize, opt: RebalSaveOpt) -> Result<()> {
        // TODO: lock
        let mut meta = RebalanceMeta::new();
        if let Err(err) = meta.load(self.pools[0].clone()).await {
            if err != Error::ConfigNotFound {
                info!("save_rebalance_stats: load err: {:?}", err);
                return Err(err);
            }
        }

        match opt {
            RebalSaveOpt::Stats => {
                {
                    let mut rebalance_meta = self.rebalance_meta.write().await;
                    if let Some(rbm) = rebalance_meta.as_mut() {
                        meta.pool_stats[pool_idx] = rbm.pool_stats[pool_idx].clone();
                    }
                }

                if let Some(pool_stat) = meta.pool_stats.get_mut(pool_idx) {
                    pool_stat.info.end_time = Some(OffsetDateTime::now_utc());
                }
            }
            RebalSaveOpt::StoppedAt => {
                meta.stopped_at = Some(OffsetDateTime::now_utc());
            }
        }

        {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            *rebalance_meta = Some(meta.clone());
        }

        info!(
            "save_rebalance_stats: save rebalance meta, pool_idx: {}, opt: {:?}, meta: {:?}",
            pool_idx, opt, meta
        );
        meta.save(self.pools[0].clone()).await?;

        Ok(())
    }
}

impl SetDisks {
    #[tracing::instrument(skip(self, rx, cb))]
    pub async fn list_objects_to_rebalance(
        self: &Arc<Self>,
        rx: CancellationToken,
        bucket: String,
        cb: ListCallback,
    ) -> Result<()> {
        info!("list_objects_to_rebalance: start list_objects_to_rebalance");
        // Placeholder for actual object listing logic
        let (disks, _) = self.get_online_disks_with_healing(false).await;
        if disks.is_empty() {
            info!("list_objects_to_rebalance: no disk available");
            return Err(Error::other("errNoDiskAvailable"));
        }

        info!("list_objects_to_rebalance: get online disks with healing");
        let listing_quorum = self.set_drive_count.div_ceil(2);

        let resolver = MetadataResolutionParams {
            dir_quorum: listing_quorum,
            obj_quorum: listing_quorum,
            bucket: bucket.clone(),
            ..Default::default()
        };

        let cb1 = cb.clone();
        list_path_raw(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                bucket: bucket.clone(),
                recursive: true,
                min_disks: listing_quorum,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    info!("list_objects_to_rebalance: agreed: {:?}", &entry.name);
                    Box::pin(cb1(entry))
                })),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                    // let cb = cb.clone();
                    let resolver = resolver.clone();
                    let cb = cb.clone();

                    match entries.resolve(resolver) {
                        Some(entry) => {
                            info!("list_objects_to_rebalance: list_objects_to_decommission get {}", &entry.name);
                            Box::pin(async move { cb(entry).await })
                        }
                        None => {
                            info!("list_objects_to_rebalance: list_objects_to_decommission get none");
                            Box::pin(async {})
                        }
                    }
                })),
                ..Default::default()
            },
        )
        .await?;

        info!("list_objects_to_rebalance: list_objects_to_rebalance done");
        Ok(())
    }
}
