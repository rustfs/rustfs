use std::io::Cursor;
use std::sync::Arc;
use std::time::SystemTime;

use crate::cache_value::metacache_set::{list_path_raw, ListPathRawOptions};
use crate::config::com::{read_config_with_metadata, save_config_with_opts};
use crate::config::error::is_err_config_not_found;
use crate::disk::{MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use crate::global::get_global_endpoints;
use crate::pools::ListCallback;
use crate::set_disk::SetDisks;
use crate::store::ECStore;
use crate::store_api::{CompletePart, FileInfo, GetObjectReader, ObjectIO, ObjectOptions, PutObjReader};
use crate::store_err::{is_err_data_movement_overwrite, is_err_object_not_found, is_err_version_not_found};
use crate::utils::path::encode_dir_object;
use crate::StorageAPI;
use common::defer;
use common::error::{Error, Result};
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast::{self, Receiver as B_Receiver};
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};
use uuid::Uuid;
use workers::workers::Workers;

const REBAL_META_FMT: u16 = 1; // Replace with actual format value
const REBAL_META_VER: u16 = 1; // Replace with actual version value
const REBAL_META_NAME: &str = "rebalance_meta";

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
            fi.size as i64 * (fi.erasure.data_blocks + fi.erasure.parity_blocks) as i64 / fi.erasure.data_blocks as i64
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

use std::fmt;

impl fmt::Display for RebalStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match self {
            RebalStatus::None => "None",
            RebalStatus::Started => "Started",
            RebalStatus::Completed => "Completed",
            RebalStatus::Stopped => "Stopped",
            RebalStatus::Failed => "Failed",
        };
        write!(f, "{}", status)
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
    pub start_time: Option<SystemTime>, // Time at which rebalance-start was issued
    #[serde(rename = "stopTs")]
    pub end_time: Option<SystemTime>, // Time at which rebalance operation completed or rebalance-stop was called
    #[serde(rename = "status")]
    pub status: RebalStatus, // Current state of rebalance operation
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct DiskStat {
    pub total_space: u64,
    pub available_space: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RebalanceMeta {
    #[serde(skip)]
    pub cancel: Option<tokio::sync::broadcast::Sender<bool>>, // To be invoked on rebalance-stop
    #[serde(skip)]
    pub last_refreshed_at: Option<SystemTime>,
    #[serde(rename = "stopTs")]
    pub stopped_at: Option<SystemTime>, // Time when rebalance-stop was issued
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
            warn!("rebalanceMeta: no data");
            return Ok(());
        }
        if data.len() <= 4 {
            return Err(Error::msg("rebalanceMeta: no data"));
        }

        // Read header
        match u16::from_le_bytes([data[0], data[1]]) {
            REBAL_META_FMT => {}
            fmt => return Err(Error::msg(format!("rebalanceMeta: unknown format: {}", fmt))),
        }
        match u16::from_le_bytes([data[2], data[3]]) {
            REBAL_META_VER => {}
            ver => return Err(Error::msg(format!("rebalanceMeta: unknown version: {}", ver))),
        }

        let meta: Self = rmp_serde::from_read(Cursor::new(&data[4..]))?;
        *self = meta;

        self.last_refreshed_at = Some(SystemTime::now());

        warn!("rebalanceMeta: loaded meta done");
        Ok(())
    }

    pub async fn save<S: StorageAPI>(&self, store: Arc<S>) -> Result<()> {
        self.save_with_opts(store, ObjectOptions::default()).await
    }

    pub async fn save_with_opts<S: StorageAPI>(&self, store: Arc<S>, opts: ObjectOptions) -> Result<()> {
        if self.pool_stats.is_empty() {
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
        warn!("rebalanceMeta: load rebalance meta");
        match meta.load(self.pools[0].clone()).await {
            Ok(_) => {
                warn!("rebalanceMeta: rebalance meta loaded0");
                {
                    let mut rebalance_meta = self.rebalance_meta.write().await;

                    *rebalance_meta = Some(meta);

                    drop(rebalance_meta);
                }

                warn!("rebalanceMeta: rebalance meta loaded1");

                if let Err(err) = self.update_rebalance_stats().await {
                    error!("Failed to update rebalance stats: {}", err);
                } else {
                    warn!("rebalanceMeta: rebalance meta loaded2");
                }
            }
            Err(err) => {
                if !is_err_config_not_found(&err) {
                    error!("rebalanceMeta: load rebalance meta err {:?}", &err);
                    return Err(err);
                }

                error!("rebalanceMeta: not found, rebalance not started");
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn update_rebalance_stats(&self) -> Result<()> {
        let mut ok = false;

        for i in 0..self.pools.len() {
            if self.find_index(i).await.is_none() {
                let mut rebalance_meta = self.rebalance_meta.write().await;
                if let Some(meta) = rebalance_meta.as_mut() {
                    meta.pool_stats.push(RebalanceStats::default());
                }
                ok = true;
                drop(rebalance_meta);
            }
        }

        if ok {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            if let Some(meta) = rebalance_meta.as_mut() {
                meta.save(self.pools[0].clone()).await?;
            }
            drop(rebalance_meta);
        }

        Ok(())
    }

    async fn find_index(&self, index: usize) -> Option<usize> {
        if let Some(meta) = self.rebalance_meta.read().await.as_ref() {
            return meta.pool_stats.get(index).map(|_v| index);
        }

        None
    }

    #[tracing::instrument(skip(self))]
    pub async fn init_rebalance_meta(&self, bucktes: Vec<String>) -> Result<String> {
        warn!("init_rebalance_meta: start rebalance");
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

        let now = SystemTime::now();

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

        warn!("init_rebalance_meta: rebalance meta saved");

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
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(meta) = rebalance_meta.as_ref() {
            if let Some(pool_stat) = meta.pool_stats.get(pool_index) {
                if pool_stat.info.status == RebalStatus::Completed || !pool_stat.participating {
                    return Ok(None);
                }

                if pool_stat.buckets.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(pool_stat.buckets[0].clone()));
            }
        }

        Ok(None)
    }

    #[tracing::instrument(skip(self))]
    pub async fn bucket_rebalance_done(&self, pool_index: usize, bucket: String) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        if let Some(meta) = rebalance_meta.as_mut() {
            if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                warn!("bucket_rebalance_done: buckets {:?}", &pool_stat.buckets);
                if let Some(idx) = pool_stat.buckets.iter().position(|b| b.as_str() == bucket.as_str()) {
                    warn!("bucket_rebalance_done: bucket {} rebalanced", &bucket);
                    pool_stat.buckets.remove(idx);
                    pool_stat.rebalanced_buckets.push(bucket);

                    return Ok(());
                } else {
                    warn!("bucket_rebalance_done: bucket {} not found", bucket);
                }
            }
        }

        Ok(())
    }

    pub async fn is_rebalance_started(&self) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(ref meta) = *rebalance_meta {
            if meta.stopped_at.is_some() {
                return false;
            }

            if meta
                .pool_stats
                .iter()
                .any(|v| v.participating && v.info.status != RebalStatus::Completed)
            {
                return true;
            }
        }

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
            if let Some(tx) = meta.cancel.as_ref() {
                let _ = tx.send(true);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn start_rebalance(self: &Arc<Self>) {
        warn!("start_rebalance: start rebalance");
        // let rebalance_meta = self.rebalance_meta.read().await;

        let (tx, rx) = broadcast::channel::<bool>(1);

        {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            if let Some(meta) = rebalance_meta.as_mut() {
                meta.cancel = Some(tx)
            } else {
                error!("start_rebalance: rebalance_meta is None exit");
                return;
            }

            drop(rebalance_meta);
        }

        let participants = {
            if let Some(ref meta) = *self.rebalance_meta.read().await {
                if meta.stopped_at.is_some() {
                    warn!("start_rebalance: rebalance already stopped exit");
                    return;
                }

                let mut participants = vec![false; meta.pool_stats.len()];
                for (i, pool_stat) in meta.pool_stats.iter().enumerate() {
                    if pool_stat.info.status == RebalStatus::Started {
                        participants[i] = pool_stat.participating;
                    }
                }
                participants
            } else {
                Vec::new()
            }
        };

        for (idx, participating) in participants.iter().enumerate() {
            if !*participating {
                warn!("start_rebalance: pool {} is not participating, skipping", idx);
                continue;
            }

            if get_global_endpoints()
                .as_ref()
                .get(idx)
                .map_or(true, |v| v.endpoints.as_ref().first().map_or(true, |e| e.is_local))
            {
                warn!("start_rebalance: pool {} is not local, skipping", idx);
                continue;
            }

            let pool_idx = idx;
            let store = self.clone();
            let rx = rx.resubscribe();
            tokio::spawn(async move {
                if let Err(err) = store.rebalance_buckets(rx, pool_idx).await {
                    error!("Rebalance failed for pool {}: {}", pool_idx, err);
                } else {
                    info!("Rebalance completed for pool {}", pool_idx);
                }
            });
        }

        warn!("start_rebalance: rebalance started done");
    }

    #[tracing::instrument(skip(self, rx))]
    async fn rebalance_buckets(self: &Arc<Self>, rx: B_Receiver<bool>, pool_index: usize) -> Result<()> {
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<Result<()>>(1);

        // Save rebalance metadata periodically
        let store = self.clone();
        let save_task = tokio::spawn(async move {
            let mut timer = tokio::time::interval_at(Instant::now() + Duration::from_secs(10), Duration::from_secs(10));
            let mut msg: String;
            let mut quit = false;

            loop {
                tokio::select! {
                    //  TODO: cancel rebalance
                    Some(result) = done_rx.recv() => {
                        quit = true;
                        let now = SystemTime::now();


                        let state = match result {
                            Ok(_) => {
                                msg = format!("Rebalance completed at {:?}", now);
                                RebalStatus::Completed},
                            Err(err) => {
                                // TODO: check stop
                                if err.to_string().contains("canceled") {
                                    msg = format!("Rebalance stopped at {:?}", now);
                                    RebalStatus::Stopped
                                } else {
                                    msg = format!("Rebalance stopped at {:?} with err {:?}", now, err);
                                    RebalStatus::Failed
                                }
                            }
                        };

                        {
                            let mut rebalance_meta = store.rebalance_meta.write().await;

                            if let Some(rbm) = rebalance_meta.as_mut() {
                                rbm.pool_stats[pool_index].info.status = state;
                                rbm.pool_stats[pool_index].info.end_time = Some(now);
                            }
                        }


                    }
                    _ = timer.tick() => {
                        let now = SystemTime::now();
                        msg = format!("Saving rebalance metadata at {:?}", now);
                    }
                }

                if let Err(err) = store.save_rebalance_stats(pool_index, RebalSaveOpt::Stats).await {
                    error!("{} err: {:?}", msg, err);
                } else {
                    info!(msg);
                }

                if quit {
                    warn!("{}: exiting save_task", msg);
                    return;
                }

                timer.reset();
            }
        });

        warn!("Pool {} rebalancing is started", pool_index + 1);

        while let Some(bucket) = self.next_rebal_bucket(pool_index).await? {
            warn!("Rebalancing bucket: start {}", bucket);

            if let Err(err) = self.rebalance_bucket(rx.resubscribe(), bucket.clone(), pool_index).await {
                if err.to_string().contains("not initialized") {
                    warn!("rebalance_bucket: rebalance not initialized, continue");
                    continue;
                }
                error!("Error rebalancing bucket {}: {:?}", bucket, err);
                done_tx.send(Err(err)).await.ok();
                break;
            }

            warn!("Rebalance bucket: done {} ", bucket);
            self.bucket_rebalance_done(pool_index, bucket).await?;
        }

        warn!("Pool {} rebalancing is done", pool_index + 1);

        done_tx.send(Ok(())).await.ok();
        save_task.await.ok();

        Ok(())
    }

    async fn check_if_rebalance_done(&self, pool_index: usize) -> bool {
        let mut rebalance_meta = self.rebalance_meta.write().await;

        if let Some(meta) = rebalance_meta.as_mut() {
            if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                // Check if the pool's rebalance status is already completed
                if pool_stat.info.status == RebalStatus::Completed {
                    return true;
                }

                // Calculate the percentage of free space improvement
                let pfi = (pool_stat.init_free_space + pool_stat.bytes) as f64 / pool_stat.init_capacity as f64;

                // Mark pool rebalance as done if within 5% of the PercentFreeGoal
                if (pfi - meta.percent_free_goal).abs() <= 0.05 {
                    pool_stat.info.status = RebalStatus::Completed;
                    pool_stat.info.end_time = Some(SystemTime::now());
                    return true;
                }
            }
        }

        false
    }

    #[tracing::instrument(skip(self, wk, set))]
    async fn rebalance_entry(
        &self,
        bucket: String,
        pool_index: usize,
        entry: MetaCacheEntry,
        set: Arc<SetDisks>,
        wk: Arc<Workers>,
    ) {
        defer!(|| async {
            wk.give().await;
        });

        if entry.is_dir() {
            return;
        }

        if self.check_if_rebalance_done(pool_index).await {
            return;
        }

        let mut fivs = match entry.file_info_versions(&bucket) {
            Ok(fivs) => fivs,
            Err(err) => {
                error!("rebalance_entry Error getting file info versions: {}", err);
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
                            break;
                        }

                        failure = true;
                        error!("rebalance_entry: get_object_reader err {:?}", &err);
                        continue;
                    }
                };

                if let Err(err) = self.rebalance_object(pool_index, bucket.clone(), rd).await {
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
    async fn rebalance_object(&self, pool_idx: usize, bucket: String, rd: GetObjectReader) -> Result<()> {
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
                // 每次从reader中读取一个part上传

                let mut chunk = vec![0u8; part.size];

                reader.read_exact(&mut chunk).await?;

                // 每次从reader中读取一个part上传
                let rd = Box::new(Cursor::new(chunk));
                let mut data = PutObjReader::new(rd, part.size);

                let pi = match self
                    .put_object_part(
                        &bucket,
                        &object_info.name,
                        &res.upload_id,
                        part.number,
                        &mut data,
                        &ObjectOptions {
                            preserve_etag: part.e_tag.clone(),
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
                    e_tag: pi.etag,
                };
            }

            if let Err(err) = self
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

        let mut data = PutObjReader::new(rd.stream, object_info.size);

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
    async fn rebalance_bucket(self: &Arc<Self>, rx: B_Receiver<bool>, bucket: String, pool_index: usize) -> Result<()> {
        // Placeholder for actual bucket rebalance logic
        warn!("Rebalancing bucket {} in pool {}", bucket, pool_index);

        // TODO: other config
        // if bucket != RUSTFS_META_BUCKET{

        // }

        let pool = self.pools[pool_index].clone();

        let wk = Workers::new(pool.disk_set.len() * 2).map_err(|v| Error::from_string(v))?;

        for (set_idx, set) in pool.disk_set.iter().enumerate() {
            wk.clone().take().await;

            let rebalance_entry: ListCallback = Arc::new({
                let this = Arc::clone(self);
                let bucket = bucket.clone();
                let wk = wk.clone();
                let set = set.clone();
                move |entry: MetaCacheEntry| {
                    let this = this.clone();
                    let bucket = bucket.clone();
                    let wk = wk.clone();
                    let set = set.clone();
                    Box::pin(async move {
                        wk.take().await;
                        tokio::spawn(async move {
                            this.rebalance_entry(bucket, pool_index, entry, set, wk).await;
                        });
                    })
                }
            });

            let set = set.clone();
            let rx = rx.resubscribe();
            let bucket = bucket.clone();
            let wk = wk.clone();
            tokio::spawn(async move {
                if let Err(err) = set.list_objects_to_rebalance(rx, bucket, rebalance_entry).await {
                    error!("Rebalance worker {} error: {}", set_idx, err);
                } else {
                    info!("Rebalance worker {} done", set_idx);
                }
                wk.clone().give().await;
            });
        }

        wk.wait().await;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn save_rebalance_stats(&self, pool_idx: usize, opt: RebalSaveOpt) -> Result<()> {
        // TODO: NSLOOK

        let mut meta = RebalanceMeta::new();
        meta.load_with_opts(
            self.pools[0].clone(),
            ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await?;

        if opt == RebalSaveOpt::StoppedAt {
            meta.stopped_at = Some(SystemTime::now());
        }

        let mut rebalance_meta = self.rebalance_meta.write().await;

        if let Some(rb) = rebalance_meta.as_mut() {
            if opt == RebalSaveOpt::Stats {
                meta.pool_stats[pool_idx] = rb.pool_stats[pool_idx].clone();
            }

            *rb = meta;
        } else {
            *rebalance_meta = Some(meta);
        }

        if let Some(meta) = rebalance_meta.as_mut() {
            meta.save_with_opts(
                self.pools[0].clone(),
                ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await?;
        }

        Ok(())
    }
}

impl SetDisks {
    #[tracing::instrument(skip(self, rx, cb))]
    pub async fn list_objects_to_rebalance(
        self: &Arc<Self>,
        rx: B_Receiver<bool>,
        bucket: String,
        cb: ListCallback,
    ) -> Result<()> {
        // Placeholder for actual object listing logic
        let (disks, _) = self.get_online_disks_with_healing(false).await;
        if disks.is_empty() {
            return Err(Error::msg("errNoDiskAvailable"));
        }

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
                recursice: true,
                min_disks: listing_quorum,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| Box::pin(cb1(entry)))),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
                    // let cb = cb.clone();
                    let resolver = resolver.clone();
                    let cb = cb.clone();

                    match entries.resolve(resolver) {
                        Some(entry) => {
                            warn!("rebalance: list_objects_to_decommission get {}", &entry.name);
                            Box::pin(async move { cb(entry).await })
                        }
                        None => {
                            warn!("rebalance: list_objects_to_decommission get none");
                            Box::pin(async {})
                        }
                    }
                })),
                ..Default::default()
            },
        )
        .await?;

        Ok(())
    }
}
