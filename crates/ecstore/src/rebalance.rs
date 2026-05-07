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
use crate::data_movement;
use crate::data_usage::DATA_USAGE_CACHE_NAME;
use crate::disk::error::DiskError;
use crate::error::{Error, Result};
use crate::error::{
    is_err_data_movement_overwrite, is_err_object_not_found, is_err_operation_canceled, is_err_version_not_found,
};
use crate::global::get_global_endpoints;
use crate::pools::ListCallback;
use crate::set_disk::SetDisks;
use crate::store::ECStore;
use crate::store_api::{GetObjectReader, HTTPRangeSpec, ObjectIO, ObjectInfo, ObjectOperations, ObjectOptions};
use http::HeaderMap;
use rustfs_filemeta::{FileInfo, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use rustfs_utils::path::encode_dir_object;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use time::OffsetDateTime;
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
        let on_disk_size = if fi.deleted || fi.erasure.data_blocks == 0 || fi.size <= 0 {
            0
        } else {
            let data_blocks = fi.erasure.data_blocks as i64;
            let total_blocks = fi.erasure.data_blocks.saturating_add(fi.erasure.parity_blocks) as i64;
            fi.size
                .saturating_mul(total_blocks)
                .checked_div(data_blocks)
                .unwrap_or(0)
                .max(0) as u64
        };
        self.bytes = self.bytes.saturating_add(on_disk_size);
        self.bucket = bucket;
        self.object = fi.name.clone();
    }
}

pub type RStats = Vec<Arc<RebalanceStats>>;

#[derive(Debug, Default)]
struct RebalanceBucketConfigs {
    lifecycle_config: Option<s3s::dto::BucketLifecycleConfiguration>,
    lock_retention: Option<s3s::dto::DefaultRetention>,
    replication_config: Option<(s3s::dto::ReplicationConfiguration, OffsetDateTime)>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct MigrationVersionResult {
    pub moved: bool,
    pub ignored: bool,
    pub cleanup_ignored: bool,
    pub failed: bool,
    pub error: Option<Error>,
}

fn rebalance_delete_marker_opts(version: &FileInfo, version_id: Option<String>, src_pool_idx: usize) -> ObjectOptions {
    ObjectOptions {
        versioned: true,
        version_id,
        mod_time: version.mod_time,
        src_pool_idx,
        data_movement: true,
        delete_marker: true,
        skip_decommissioned: true,
        delete_replication: version.replication_state_internal.clone(),
        ..Default::default()
    }
}

fn rebalance_remote_tiered_opts(version: &FileInfo, version_id: Option<String>, src_pool_idx: usize) -> ObjectOptions {
    ObjectOptions {
        versioned: version_id.is_some(),
        version_id,
        mod_time: version.mod_time,
        user_defined: version.metadata.clone(),
        src_pool_idx,
        data_movement: true,
        ..Default::default()
    }
}

#[async_trait::async_trait]
pub(crate) trait MigrationBackend: Send + Sync {
    async fn get_object_reader_for_migration(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader>;

    async fn delete_object_for_migration(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo>;

    async fn move_remote_version_for_migration(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl MigrationBackend for SetDisks {
    async fn get_object_reader_for_migration(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        self.get_object_reader(bucket, object, range, h, opts).await
    }

    async fn delete_object_for_migration(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        self.delete_object(bucket, object, opts).await
    }

    async fn move_remote_version_for_migration(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        self.decommission_tiered_object(bucket, object, fi, opts).await
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn migrate_entry_version<Backend, F, Fut>(
    set: &Backend,
    bucket: String,
    pool_index: usize,
    version: &FileInfo,
    version_id: Option<String>,
    max_attempts: usize,
    ignore_data_usage_cache: bool,
    mut transfer: F,
) -> MigrationVersionResult
where
    Backend: MigrationBackend + ?Sized,
    F: FnMut(usize, String, GetObjectReader) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
{
    let max_attempts = max_attempts.max(1);

    if ignore_data_usage_cache && bucket == crate::disk::RUSTFS_META_BUCKET && version.name.contains(DATA_USAGE_CACHE_NAME) {
        return MigrationVersionResult {
            moved: false,
            ignored: true,
            cleanup_ignored: false,
            failed: false,
            error: None,
        };
    }

    if version.is_remote() {
        if let Err(err) = set
            .move_remote_version_for_migration(
                &bucket,
                &version.name,
                version,
                &rebalance_remote_tiered_opts(version, version_id, pool_index),
            )
            .await
        {
            if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err) {
                return MigrationVersionResult {
                    moved: false,
                    ignored: true,
                    cleanup_ignored: true,
                    failed: false,
                    error: None,
                };
            }

            return MigrationVersionResult {
                moved: false,
                ignored: false,
                cleanup_ignored: false,
                failed: true,
                error: Some(err),
            };
        }

        return MigrationVersionResult {
            moved: true,
            ignored: false,
            cleanup_ignored: false,
            failed: false,
            error: None,
        };
    }

    if version.deleted {
        if let Err(err) = set
            .delete_object_for_migration(&bucket, &version.name, rebalance_delete_marker_opts(version, version_id, pool_index))
            .await
        {
            if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err) {
                return MigrationVersionResult {
                    moved: false,
                    ignored: true,
                    cleanup_ignored: true,
                    failed: false,
                    error: None,
                };
            }

            return MigrationVersionResult {
                moved: false,
                ignored: false,
                cleanup_ignored: false,
                failed: true,
                error: Some(err),
            };
        }

        return MigrationVersionResult {
            moved: true,
            ignored: false,
            cleanup_ignored: false,
            failed: false,
            error: None,
        };
    }

    let mut last_error: Option<Error> = None;
    for attempt in 0..max_attempts {
        let rd = match set
            .get_object_reader_for_migration(
                &bucket,
                &encode_dir_object(&version.name),
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    version_id: version_id.clone(),
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(rd) => rd,
            Err(err) => {
                if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                    return MigrationVersionResult {
                        moved: false,
                        ignored: true,
                        cleanup_ignored: true,
                        failed: false,
                        error: None,
                    };
                }

                last_error = Some(err);
                if attempt + 1 >= max_attempts {
                    return MigrationVersionResult {
                        moved: false,
                        ignored: false,
                        cleanup_ignored: false,
                        failed: true,
                        error: last_error,
                    };
                }

                continue;
            }
        };

        if let Err(err) = transfer(pool_index, bucket.clone(), rd).await {
            if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err) {
                return MigrationVersionResult {
                    moved: false,
                    ignored: true,
                    cleanup_ignored: true,
                    failed: false,
                    error: None,
                };
            }

            last_error = Some(err);
            if attempt + 1 >= max_attempts {
                return MigrationVersionResult {
                    moved: false,
                    ignored: false,
                    cleanup_ignored: false,
                    failed: true,
                    error: last_error,
                };
            }

            continue;
        }

        return MigrationVersionResult {
            moved: true,
            ignored: false,
            cleanup_ignored: false,
            failed: false,
            error: None,
        };
    }

    MigrationVersionResult {
        moved: false,
        ignored: false,
        cleanup_ignored: false,
        failed: true,
        error: last_error,
    }
}

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
    #[serde(rename = "err")]
    pub last_error: Option<String>, // Last rebalance error message
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

fn is_rebalance_pool_started(pool_stat: &RebalanceStats) -> bool {
    pool_stat.participating && pool_stat.info.status == RebalStatus::Started
}

fn is_rebalance_in_progress(meta: &RebalanceMeta) -> bool {
    if meta.stopped_at.is_some() {
        return false;
    }

    meta.pool_stats.iter().any(is_rebalance_pool_started)
}

fn is_rebalance_conflicting_with_decommission(meta: &RebalanceMeta) -> bool {
    is_rebalance_in_progress(meta)
}

fn first_rebalance_bucket(pool_stat: &RebalanceStats) -> Option<String> {
    pool_stat.buckets.first().cloned()
}

fn rebalance_meta_load_no_data_error() -> Error {
    Error::other("rebalance metadata load failed: metadata payload is too short")
}

fn rebalance_meta_load_unknown_format_error(fmt: u16) -> Error {
    Error::other(format!("rebalance metadata load failed: unknown format {fmt}"))
}

fn rebalance_meta_load_unknown_version_error(ver: u16) -> Error {
    Error::other(format!("rebalance metadata load failed: unknown version {ver}"))
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
            return Err(rebalance_meta_load_no_data_error());
        }

        // Read header
        match u16::from_le_bytes([data[0], data[1]]) {
            REBAL_META_FMT => {}
            fmt => return Err(rebalance_meta_load_unknown_format_error(fmt)),
        }
        match u16::from_le_bytes([data[2], data[3]]) {
            REBAL_META_VER => {}
            ver => return Err(rebalance_meta_load_unknown_version_error(ver)),
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
        let pool = clone_first_arc(&self.pools, "rebalanceMeta: no pools available")?;
        if resolve_rebalance_meta_load_result(meta.load(pool).await)? {
            info!("rebalanceMeta: rebalance meta loaded0");
            {
                let mut rebalance_meta = self.rebalance_meta.write().await;

                *rebalance_meta = Some(meta);

                drop(rebalance_meta);
            }

            info!("rebalanceMeta: rebalance meta loaded1");

            resolve_load_rebalance_stats_update_result(self.update_rebalance_stats().await)?;
            info!("rebalanceMeta: rebalance meta loaded2");
        } else {
            info!("rebalanceMeta: not found, rebalance not started");
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn update_rebalance_stats(&self) -> Result<()> {
        let mut ok = false;

        let pool_stats = {
            let rebalance_meta = self.rebalance_meta.read().await;
            clone_rebalance_pool_stats(rebalance_meta.as_ref())?
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
                let pool = clone_first_arc(&self.pools, "update_rebalance_stats: no pools available")?;
                resolve_rebalance_meta_save_result(meta.save(pool).await, "update_rebalance_stats")?;
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

        let percent_free_goal = percent_free_ratio(total_free, total_cap);

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

            if should_pool_participate(disk_stat.available_space, disk_stat.total_space, percent_free_goal) {
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

        let pool = clone_first_arc(&self.pools, "init_rebalance_meta: no pools available")?;
        resolve_rebalance_meta_save_result(meta.save(pool).await, "init_rebalance_meta")?;

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
        if let Some(meta) = rebalance_meta.as_mut()
            && let Some(pool_stat) = meta.pool_stats.get_mut(pool_index)
        {
            pool_stat.update(bucket, fi);
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn next_rebal_bucket(&self, pool_index: usize) -> Result<Option<String>> {
        info!("next_rebal_bucket: pool_index: {}", pool_index);
        let rebalance_meta = self.rebalance_meta.read().await;
        info!("next_rebal_bucket: rebalance_meta: {:?}", rebalance_meta);
        resolve_next_rebalance_bucket(rebalance_meta.as_ref(), pool_index)
    }

    #[tracing::instrument(skip(self))]
    pub async fn bucket_rebalance_done(&self, pool_index: usize, bucket: String) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        mark_rebalance_bucket_done(rebalance_meta.as_mut(), pool_index, &bucket)
    }

    pub async fn is_rebalance_started(&self) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(meta) = rebalance_meta.as_ref() {
            meta.pool_stats.iter().enumerate().for_each(|(i, v)| {
                info!(
                    "is_rebalance_started: pool_index: {}, participating: {:?}, status: {:?}",
                    i, v.participating, v.info.status
                );
            });

            let started = is_rebalance_conflicting_with_decommission(meta);
            if started {
                info!("is_rebalance_started: rebalance started");
                return true;
            }
        }

        info!("is_rebalance_started: rebalance not started");
        false
    }

    pub async fn is_rebalance_conflicting_with_decommission(&self) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        rebalance_meta
            .as_ref()
            .is_some_and(is_rebalance_conflicting_with_decommission)
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
        let meta_to_save = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            stop_rebalance_meta_snapshot(rebalance_meta.as_mut(), OffsetDateTime::now_utc())
        };

        if let Some(meta_to_save) = meta_to_save {
            let pool = clone_first_arc(self.pools.as_slice(), "stop_rebalance: no pools available")?;
            resolve_rebalance_meta_save_result(meta_to_save.save(pool).await, "stop_rebalance")?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn start_rebalance(self: &Arc<Self>) -> Result<()> {
        info!("start_rebalance: start rebalance");
        let decommission_running = self.is_decommission_running().await;
        // let rebalance_meta = self.rebalance_meta.read().await;

        let cancel_tx = CancellationToken::new();
        let rx = cancel_tx.clone();
        let mut meta_to_save = None;

        {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            validate_start_rebalance_state(decommission_running, rebalance_meta.is_some())?;

            let Some(meta) = rebalance_meta.as_mut() else {
                return Err(Error::ConfigNotFound);
            };
            if should_skip_start_rebalance(meta.cancel.is_some(), is_rebalance_in_progress(meta)) {
                info!("start_rebalance: already in progress, skip duplicate start");
                return Ok(());
            }
            if complete_rebalance_pools_at_goal(meta, OffsetDateTime::now_utc()) {
                meta_to_save = Some(meta.clone());
            }
            meta.cancel = Some(cancel_tx);

            drop(rebalance_meta);
        }

        if let Some(meta) = meta_to_save {
            let pool = clone_first_arc(self.pools.as_slice(), "start_rebalance: no pools available")?;
            resolve_rebalance_meta_save_result(meta.save(pool).await, "start_rebalance complete pools at goal")?;
        }

        let participants = if let Some(ref meta) = *self.rebalance_meta.read().await {
            resolve_rebalance_participants(meta.pool_stats.as_slice(), self.pools.len())
        } else {
            info!("start_rebalance:2  rebalance_meta is None exit");
            Vec::new()
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
        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    async fn rebalance_buckets(self: &Arc<Self>, rx: CancellationToken, pool_index: usize) -> Result<()> {
        ensure_valid_rebalance_pool_index(self.pools.len(), pool_index)?;

        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<Result<()>>(1);

        // Save rebalance metadata periodically
        let store = self.clone();
        let save_task = tokio::spawn(async move {
            let mut timer = tokio::time::interval_at(Instant::now() + Duration::from_secs(30), Duration::from_secs(10));
            let mut msg: String;
            let mut quit = false;

            loop {
                tokio::select! {
                    result = done_rx.recv() => {
                        quit = true;
                        let now = OffsetDateTime::now_utc();
                        let terminal_event = classify_rebalance_terminal_event(result, now);
                        msg = terminal_event.message().to_string();
                        let mut rebalance_meta = store.rebalance_meta.write().await;
                        if let Some(meta) = rebalance_meta.as_mut() {
                            let meta_stopped = meta.stopped_at.is_some();
                            if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                                if should_preserve_rebalance_stopped_state(
                                    meta_stopped,
                                    pool_stat.info.status,
                                    &terminal_event,
                                ) {
                                    info!(
                                        "rebalance_buckets: preserving stopped status for pool {}",
                                        pool_index
                                    );
                                } else {
                                    apply_rebalance_terminal_event(
                                        &mut pool_stat.info.status,
                                        &mut pool_stat.info.end_time,
                                        &mut pool_stat.info.last_error,
                                        terminal_event,
                                        now,
                                    );
                                }
                            }
                        }
                    }
                    _ = timer.tick() => {
                        let now = OffsetDateTime::now_utc();
                        msg = format!("Saving rebalance metadata at {now:?}");
                    }
                }

                if let Err(err) = store.save_rebalance_stats(pool_index, RebalSaveOpt::Stats).await {
                    let wrapped = Error::other(format!("rebalance save_task stats save failed for pool {pool_index}: {err}"));
                    error!("{} err: {:?}", msg, wrapped);
                    if quit {
                        return Err(wrapped);
                    }
                } else {
                    info!(msg);
                }

                if quit {
                    info!("{}: exiting save_task", msg);
                    return Ok(());
                }

                timer.reset();
            }
        });

        info!("Pool {} rebalancing is started", pool_index);
        let mut final_result: Result<()> = Ok(());

        loop {
            if rx.is_cancelled() {
                info!("Pool {} rebalancing is stopped", pool_index);
                let err = Error::OperationCanceled;
                final_result = Err(resolve_rebalance_terminal_error(
                    err.clone(),
                    send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                ));
                break;
            }

            let next_bucket = match self.next_rebal_bucket(pool_index).await {
                Ok(bucket) => bucket,
                Err(err) => {
                    error!("next_rebal_bucket failed for pool {}: {:?}", pool_index, err);
                    final_result = Err(resolve_rebalance_terminal_error(
                        err.clone(),
                        send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                    ));
                    break;
                }
            };

            if let Some(bucket) = next_bucket {
                info!("Rebalancing bucket: start {}", bucket);

                if let Err(err) = resolve_rebalance_bucket_result(
                    self.rebalance_bucket(rx.clone(), bucket.clone(), pool_index).await,
                    pool_index,
                    &bucket,
                ) {
                    error!("Error rebalancing bucket {}: {:?}", bucket, err);
                    final_result = Err(resolve_rebalance_terminal_error(
                        err.clone(),
                        send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                    ));
                    break;
                }

                info!("Rebalance bucket: done {} ", bucket);
                if let Err(err) = self.bucket_rebalance_done(pool_index, bucket).await {
                    error!("bucket_rebalance_done failed for pool {}: {:?}", pool_index, err);
                    final_result = Err(resolve_rebalance_terminal_error(
                        err.clone(),
                        send_rebalance_done_signal(&done_tx, Err(err.clone()), pool_index).await,
                    ));
                    break;
                }
            } else {
                info!("Rebalance bucket: no bucket to rebalance");
                break;
            }
        }

        info!("Pool {} rebalancing is done", pool_index);

        if final_result.is_ok()
            && let Err(err) = send_rebalance_done_signal(&done_tx, Ok(()), pool_index).await
        {
            final_result = Err(err);
        }
        drop(done_tx);
        if let Err(err) = resolve_rebalance_save_task_result(pool_index, save_task.await)
            && final_result.is_ok()
        {
            final_result = Err(err);
        }
        info!("Pool {} rebalancing is done2", pool_index);
        final_result
    }

    async fn check_if_rebalance_done(&self, pool_index: usize) -> bool {
        let mut rebalance_meta = self.rebalance_meta.write().await;

        if let Some(meta) = rebalance_meta.as_mut()
            && let Some(pool_stat) = meta.pool_stats.get_mut(pool_index)
        {
            // Check if the pool's rebalance status is already completed
            if pool_stat.info.status == RebalStatus::Completed {
                info!("check_if_rebalance_done: pool {} is already completed", pool_index);
                return true;
            }

            // Mark pool rebalance as done if within 5% of the PercentFreeGoal
            let pfi = if pool_stat.init_capacity == 0 {
                0.0
            } else {
                (pool_stat.init_free_space + pool_stat.bytes) as f64 / pool_stat.init_capacity as f64
            };

            if rebalance_goal_reached(
                pool_stat.init_free_space,
                pool_stat.init_capacity,
                pool_stat.bytes,
                meta.percent_free_goal,
            ) {
                pool_stat.info.status = RebalStatus::Completed;
                pool_stat.info.end_time = Some(OffsetDateTime::now_utc());
                info!("check_if_rebalance_done: pool {} is completed, pfi: {}", pool_index, pfi);
                return true;
            }
        }

        false
    }
}

fn rebalance_goal_reached(init_free_space: u64, init_capacity: u64, bytes: u64, percent_free_goal: f64) -> bool {
    if init_capacity == 0 {
        return false;
    }

    let pfi = (init_free_space + bytes) as f64 / init_capacity as f64;
    (pfi - percent_free_goal).abs() <= 0.05 + f64::EPSILON
}

fn percent_free_ratio(total_free: u64, total_cap: u64) -> f64 {
    if total_cap == 0 {
        return 0.0;
    }
    total_free as f64 / total_cap as f64
}

fn next_rebal_bucket_from_stat(pool_stat: &RebalanceStats) -> Option<String> {
    if pool_stat.buckets.is_empty() {
        return None;
    }

    first_rebalance_bucket(pool_stat)
}

fn rebalance_metadata_not_initialized_error(operation: &str) -> Error {
    Error::other(format!("failed to {operation}: rebalance metadata not initialized"))
}

fn invalid_rebalance_pool_index_error(pool_index: usize, pool_count: usize) -> Error {
    Error::other(format!("invalid rebalance pool index {pool_index} for {pool_count} pools"))
}

fn clone_rebalance_pool_stats(meta: Option<&RebalanceMeta>) -> Result<Vec<RebalanceStats>> {
    let Some(meta) = meta else {
        return Err(rebalance_metadata_not_initialized_error("clone rebalance pool stats"));
    };
    Ok(meta.pool_stats.clone())
}

fn resolve_next_rebalance_bucket(meta: Option<&RebalanceMeta>, pool_index: usize) -> Result<Option<String>> {
    let Some(meta) = meta else {
        return Err(rebalance_metadata_not_initialized_error("resolve next rebalance bucket"));
    };

    ensure_valid_rebalance_pool_index(meta.pool_stats.len(), pool_index)?;
    let Some(pool_stat) = meta.pool_stats.get(pool_index) else {
        return Err(invalid_rebalance_pool_index_error(pool_index, meta.pool_stats.len()));
    };

    if pool_stat.info.status == RebalStatus::Completed || !pool_stat.participating {
        info!("next_rebal_bucket: pool_index: {} completed or not participating", pool_index);
        return Ok(None);
    }

    if pool_stat.buckets.is_empty() {
        info!("next_rebal_bucket: pool_index: {} buckets is empty", pool_index);
        return Ok(None);
    }

    if let Some(bucket) = next_rebal_bucket_from_stat(pool_stat) {
        info!("next_rebal_bucket: pool_index: {} bucket: {}", pool_index, bucket);
        return Ok(Some(bucket));
    }

    info!("next_rebal_bucket: pool_index: {} None", pool_index);
    Ok(None)
}

fn mark_rebalance_bucket_done(meta: Option<&mut RebalanceMeta>, pool_index: usize, bucket: &str) -> Result<()> {
    let Some(meta) = meta else {
        return Err(rebalance_metadata_not_initialized_error("mark rebalance bucket done"));
    };

    ensure_valid_rebalance_pool_index(meta.pool_stats.len(), pool_index)?;
    let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) else {
        return Err(invalid_rebalance_pool_index_error(pool_index, meta.pool_stats.len()));
    };

    info!("bucket_rebalance_done: buckets {:?}", &pool_stat.buckets);

    if take_bucket_from_rebalance_queue(pool_stat, bucket) {
        info!("bucket_rebalance_done: bucket {} rebalanced", bucket);
        Ok(())
    } else {
        Err(Error::other(format!(
            "failed to mark rebalance bucket done: bucket {bucket} was not queued for pool {pool_index}"
        )))
    }
}

fn take_bucket_from_rebalance_queue(pool_stat: &mut RebalanceStats, bucket: &str) -> bool {
    let mut found = false;
    pool_stat.buckets.retain(|name| {
        if name == bucket {
            found = true;
            pool_stat.rebalanced_buckets.push(name.clone());
            false
        } else {
            true
        }
    });

    found
}

fn should_pool_participate(init_free_space: u64, init_capacity: u64, percent_free_goal: f64) -> bool {
    init_capacity > 0 && percent_free_ratio(init_free_space, init_capacity) < percent_free_goal
}

fn complete_rebalance_pools_at_goal(meta: &mut RebalanceMeta, now: OffsetDateTime) -> bool {
    let mut changed = false;

    for pool_stat in meta.pool_stats.iter_mut() {
        if !is_rebalance_pool_started(pool_stat) {
            continue;
        }

        if rebalance_goal_reached(
            pool_stat.init_free_space,
            pool_stat.init_capacity,
            pool_stat.bytes,
            meta.percent_free_goal,
        ) {
            pool_stat.info.status = RebalStatus::Completed;
            pool_stat.info.end_time = Some(now);
            pool_stat.info.last_error = None;
            changed = true;
        }
    }

    changed
}

fn resolve_rebalance_worker_result(
    set_idx: usize,
    worker_result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match worker_result {
        Ok(result) => result,
        Err(err) => Err(Error::other(format!("rebalance worker {set_idx} task join error: {err}"))),
    }
}

fn resolve_rebalance_save_task_result(
    pool_idx: usize,
    save_task_result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match save_task_result {
        Ok(result) => result.map_err(|err| Error::other(format!("rebalance save_task failed for pool {pool_idx}: {err}"))),
        Err(err) => Err(Error::other(format!("rebalance save_task for pool {pool_idx} join error: {err}"))),
    }
}

fn resolve_rebalance_meta_save_result(result: Result<()>, stage: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("rebalance meta save failed during {stage}: {err}")))
}

fn resolve_rebalance_meta_load_result(result: Result<()>) -> Result<bool> {
    match result {
        Ok(()) => Ok(true),
        Err(Error::ConfigNotFound) => Ok(false),
        Err(err) => {
            error!("rebalanceMeta: load rebalance meta err {:?}", &err);
            Err(Error::other(format!("rebalance metadata load failed during load_rebalance_meta: {err}")))
        }
    }
}

fn resolve_rebalance_stats_update_result(result: Result<()>, pool_idx: usize, bucket: &str, object_name: &str) -> Result<()> {
    result.map_err(|err| {
        Error::other(format!(
            "rebalance stats update failed for pool {pool_idx} bucket {bucket} object {object_name}: {err}"
        ))
    })
}

fn resolve_rebalance_file_info_versions_result<T, E>(
    result: std::result::Result<T, E>,
    bucket: &str,
    object_name: &str,
) -> Result<T>
where
    E: std::fmt::Display,
{
    result.map_err(|err| Error::other(format!("rebalance file_info_versions failed for {bucket}/{object_name}: {err}")))
}

fn resolve_rebalance_entry_cleanup_delete_result(result: Result<ObjectInfo>, bucket: &str, object_name: &str) -> Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok(()),
        Err(err) => Err(Error::other(format!("rebalance cleanup delete failed for {bucket}/{object_name}: {err}"))),
    }
}

fn resolve_rebalance_migrate_result_error(
    err: Option<Error>,
    pool_idx: usize,
    bucket: &str,
    object_name: &str,
    version_id: Option<&str>,
) -> Error {
    err.unwrap_or_else(|| {
        Error::other(format!(
            "rebalance migration reported failure without error for pool {pool_idx} entry {bucket}/{object_name} version {}",
            version_id.unwrap_or("none")
        ))
    })
}

fn resolve_load_rebalance_stats_update_result(result: Result<()>) -> Result<()> {
    result.map_err(|err| Error::other(format!("rebalance metadata stats refresh failed after load: {err}")))
}

async fn send_rebalance_done_signal(
    done_tx: &tokio::sync::mpsc::Sender<Result<()>>,
    signal: Result<()>,
    pool_idx: usize,
) -> Result<()> {
    done_tx
        .send(signal)
        .await
        .map_err(|err| Error::other(format!("rebalance done signal send failed for pool {pool_idx}: {err}")))
}

fn resolve_rebalance_terminal_error(primary_err: Error, signal_result: Result<()>) -> Error {
    match signal_result {
        Ok(()) => primary_err,
        Err(signal_err) => Error::other(format!("rebalance terminal signal failed after error {primary_err}: {signal_err}")),
    }
}

fn resolve_rebalance_bucket_error(entry_error: Option<Error>, worker_error: Option<Error>) -> Result<()> {
    if let Some(err) = entry_error {
        return Err(err);
    }

    if let Some(err) = worker_error {
        return Err(err);
    }

    Ok(())
}

fn resolve_rebalance_bucket_result(result: Result<()>, pool_idx: usize, bucket: &str) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) if is_err_operation_canceled(&err) => Err(err),
        Err(err) => Err(Error::other(format!("rebalance bucket {bucket} failed for pool {pool_idx}: {err}"))),
    }
}

fn ensure_rebalance_listing_disks_available(has_disks: bool, bucket: &str) -> Result<()> {
    if !has_disks {
        return Err(Error::other(format!(
            "failed to list objects to rebalance for bucket {bucket}: no disks available"
        )));
    }

    Ok(())
}

fn with_rebalance_entry_context(stage: &str, bucket: &str, object_name: &str, err: Error) -> Error {
    Error::other(format!("rebalance entry {stage} failed for {bucket}/{object_name}: {err}"))
}

fn should_count_rebalance_version_complete(result: &MigrationVersionResult) -> bool {
    result.cleanup_ignored || (result.moved && !result.failed)
}

fn should_cleanup_rebalance_source_entry(rebalanced: usize, total_versions: usize) -> bool {
    rebalanced == total_versions
}

fn should_skip_rebalance_delete_marker(version: &FileInfo, remaining_versions: usize, replication_configured: bool) -> bool {
    version.deleted && remaining_versions == 1 && !replication_configured
}

fn resolve_rebalance_optional_bucket_config_result<T>(bucket: &str, stage: &str, result: Result<T>) -> Result<Option<T>> {
    match result {
        Ok(config) => Ok(Some(config)),
        Err(Error::ConfigNotFound) => Ok(None),
        Err(err) => Err(Error::other(format!("rebalance {stage} config load failed for bucket {bucket}: {err}"))),
    }
}

async fn load_rebalance_bucket_configs(bucket: &str) -> Result<RebalanceBucketConfigs> {
    if bucket == crate::disk::RUSTFS_META_BUCKET {
        return Ok(RebalanceBucketConfigs::default());
    }

    let _ = resolve_rebalance_optional_bucket_config_result(
        bucket,
        "versioning",
        crate::bucket::versioning_sys::BucketVersioningSys::get(bucket).await,
    )?;

    Ok(RebalanceBucketConfigs {
        lifecycle_config: crate::global::GLOBAL_LifecycleSys.get(bucket).await,
        lock_retention: crate::bucket::object_lock::objectlock_sys::BucketObjectLockSys::get(bucket).await,
        replication_config: resolve_rebalance_optional_bucket_config_result(
            bucket,
            "replication",
            crate::bucket::metadata_sys::get_replication_config(bucket).await,
        )?,
    })
}

fn clone_first_arc<T>(values: &[Arc<T>], err_msg: &str) -> Result<Arc<T>> {
    values.first().cloned().ok_or_else(|| Error::other(err_msg))
}

fn clone_arc_by_index<T>(values: &[Arc<T>], idx: usize, err_prefix: &str) -> Result<Arc<T>> {
    values
        .get(idx)
        .cloned()
        .ok_or_else(|| Error::other(format!("{err_prefix}: {idx}")))
}

fn ensure_valid_rebalance_pool_index(pool_count: usize, idx: usize) -> Result<()> {
    if idx >= pool_count {
        return Err(invalid_rebalance_pool_index_error(idx, pool_count));
    }

    Ok(())
}

enum RebalanceTerminalEvent {
    Completed { msg: String },
    Stopped { msg: String },
    Failed { msg: String, last_error: String },
    ChannelClosed { msg: String, last_error: String },
}

impl RebalanceTerminalEvent {
    fn message(&self) -> &str {
        match self {
            RebalanceTerminalEvent::Completed { msg }
            | RebalanceTerminalEvent::Stopped { msg }
            | RebalanceTerminalEvent::Failed { msg, .. }
            | RebalanceTerminalEvent::ChannelClosed { msg, .. } => msg,
        }
    }
}

fn apply_rebalance_terminal_event(
    status: &mut RebalStatus,
    end_time: &mut Option<OffsetDateTime>,
    last_error: &mut Option<String>,
    terminal_event: RebalanceTerminalEvent,
    now: OffsetDateTime,
) {
    match terminal_event {
        RebalanceTerminalEvent::Completed { .. } => {
            *status = RebalStatus::Completed;
            *end_time = Some(now);
            *last_error = None;
        }
        RebalanceTerminalEvent::Stopped { .. } => {
            *status = RebalStatus::Stopped;
            *end_time = Some(now);
            *last_error = None;
        }
        RebalanceTerminalEvent::Failed { last_error: err, .. }
        | RebalanceTerminalEvent::ChannelClosed { last_error: err, .. } => {
            *status = RebalStatus::Failed;
            *end_time = Some(now);
            *last_error = Some(err);
        }
    }
}

fn classify_rebalance_terminal_event(signal: Option<Result<()>>, now: OffsetDateTime) -> RebalanceTerminalEvent {
    match signal {
        Some(Ok(())) => RebalanceTerminalEvent::Completed {
            msg: format!("Rebalance completed at {now:?}"),
        },
        Some(Err(err)) => {
            if is_err_operation_canceled(&err) {
                RebalanceTerminalEvent::Stopped {
                    msg: format!("Rebalance stopped at {now:?}"),
                }
            } else {
                RebalanceTerminalEvent::Failed {
                    msg: format!("Rebalance failed at {now:?} with err {err:?}"),
                    last_error: err.to_string(),
                }
            }
        }
        None => RebalanceTerminalEvent::ChannelClosed {
            msg: format!("Rebalance save task channel closed unexpectedly at {now:?}"),
            last_error: format!("rebalance save channel closed before terminal event at {now:?}"),
        },
    }
}

fn ensure_rebalance_not_decommissioning(decommission_running: bool) -> bool {
    !decommission_running
}

fn validate_start_rebalance_state(decommission_running: bool, meta_loaded: bool) -> Result<()> {
    if !ensure_rebalance_not_decommissioning(decommission_running) {
        return Err(Error::DecommissionAlreadyRunning);
    }
    if !meta_loaded {
        return Err(Error::ConfigNotFound);
    }

    Ok(())
}

fn should_skip_start_rebalance(cancel_attached: bool, in_progress: bool) -> bool {
    cancel_attached && in_progress
}

fn is_rebalance_stopped_terminal_event(terminal_event: &RebalanceTerminalEvent) -> bool {
    matches!(terminal_event, RebalanceTerminalEvent::Stopped { .. })
}

fn should_preserve_rebalance_stopped_state(
    meta_stopped: bool,
    status: RebalStatus,
    terminal_event: &RebalanceTerminalEvent,
) -> bool {
    (meta_stopped || status == RebalStatus::Stopped) && !is_rebalance_stopped_terminal_event(terminal_event)
}

fn resolve_rebalance_participants(pool_stats: &[RebalanceStats], pool_count: usize) -> Vec<bool> {
    let mut participants = vec![false; pool_count];

    for (idx, pool_stat) in pool_stats.iter().enumerate() {
        if idx >= participants.len() {
            break;
        }

        if pool_stat.info.status == RebalStatus::Started {
            participants[idx] = pool_stat.participating;
        }
    }

    participants
}

fn is_rebalance_actively_running(meta: &RebalanceMeta) -> bool {
    meta.cancel.is_some() && is_rebalance_in_progress(meta)
}

fn should_ignore_rebalance_data_usage_cache(bucket: &str) -> bool {
    bucket == crate::disk::RUSTFS_META_BUCKET
}

fn apply_rebalance_save_option(meta: &mut RebalanceMeta, pool_idx: usize, opt: RebalSaveOpt, now: OffsetDateTime) {
    match opt {
        RebalSaveOpt::Stats => {
            if pool_idx >= meta.pool_stats.len() {
                info!("save_rebalance_stats: pool_idx {pool_idx} out of range for pool_stats");
            }
        }
        RebalSaveOpt::StoppedAt => {
            apply_stopped_at(meta, now);
        }
    }

    meta.last_refreshed_at = Some(now);
}

fn mark_started_rebalance_pools_stopped(meta: &mut RebalanceMeta, stop_time: OffsetDateTime) {
    for pool_stat in meta.pool_stats.iter_mut() {
        if pool_stat.info.status == RebalStatus::Started {
            pool_stat.info.status = RebalStatus::Stopped;
            pool_stat.info.end_time.get_or_insert(stop_time);
            pool_stat.info.last_error = None;
        }
    }
}

fn apply_stopped_at(meta: &mut RebalanceMeta, now: OffsetDateTime) {
    meta.stopped_at = Some(now);
    mark_started_rebalance_pools_stopped(meta, now);
}

fn stop_rebalance_state(meta: &mut RebalanceMeta, now: OffsetDateTime) {
    if let Some(cancel_tx) = meta.cancel.take() {
        cancel_tx.cancel();
    }

    let stop_time = meta.stopped_at.unwrap_or(now);
    if meta.stopped_at.is_none() && is_rebalance_in_progress(meta) {
        meta.stopped_at = Some(stop_time);
    }

    if meta.stopped_at.is_some() {
        mark_started_rebalance_pools_stopped(meta, stop_time);
    }
}

fn stop_rebalance_meta_snapshot(meta: Option<&mut RebalanceMeta>, now: OffsetDateTime) -> Option<RebalanceMeta> {
    meta.map(|meta| {
        stop_rebalance_state(meta, now);
        meta.last_refreshed_at = Some(now);
        meta.clone()
    })
}

impl ECStore {
    #[allow(unused_assignments)]
    #[tracing::instrument(skip(self, set))]
    async fn rebalance_entry(
        self: Arc<Self>,
        bucket: String,
        pool_index: usize,
        entry: MetaCacheEntry,
        set: Arc<SetDisks>,
        bucket_configs: Arc<RebalanceBucketConfigs>,
        // wk: Arc<Workers>,
    ) -> Result<()> {
        info!("rebalance_entry: start rebalance_entry");

        // defer!(|| async {
        //     warn!("rebalance_entry: defer give worker start");
        //     wk.give().await;
        //     warn!("rebalance_entry: defer give worker done");
        // });

        if entry.is_dir() {
            info!("rebalance_entry: entry is dir, skipping");
            return Ok(());
        }

        if self.check_if_rebalance_done(pool_index).await {
            info!("rebalance_entry: rebalance done, skipping pool {}", pool_index);
            return Ok(());
        }

        let mut fivs =
            resolve_rebalance_file_info_versions_result(entry.file_info_versions(&bucket), bucket.as_str(), entry.name.as_str())?;

        fivs.versions
            .sort_by_key(|v| (v.mod_time.is_none(), std::cmp::Reverse(v.mod_time)));

        let mut rebalanced: usize = 0;
        let mut expired: usize = 0;
        for version in fivs.versions.iter() {
            if crate::pools::should_skip_lifecycle_for_data_movement(
                self.clone(),
                &bucket,
                version,
                bucket_configs.lifecycle_config.as_ref(),
                bucket_configs.lock_retention.clone(),
                bucket_configs.replication_config.clone(),
                true,
                &crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc::Rebal,
            )
            .await
            {
                expired += 1;
                info!("rebalance_entry {} Entry {} expired by lifecycle, skipping", &bucket, version.name);
                continue;
            }

            let remaining_versions = fivs.versions.len() - expired;
            if should_skip_rebalance_delete_marker(version, remaining_versions, bucket_configs.replication_config.is_some()) {
                rebalanced += 1;
                info!(
                    "rebalance_entry Entry {} is deleted and last version without replication, skipping",
                    version.name
                );
                continue;
            }

            let version_id = version.version_id.map(|v| v.to_string());
            let mut transfer = |src_pool_idx: usize, bucket: String, rd: GetObjectReader| {
                let store = self.clone();
                async move { store.rebalance_object(src_pool_idx, bucket, rd).await }
            };
            let result = migrate_entry_version(
                set.as_ref(),
                bucket.clone(),
                pool_index,
                version,
                version_id.clone(),
                3,
                should_ignore_rebalance_data_usage_cache(bucket.as_str()),
                &mut transfer,
            )
            .await;

            if result.ignored {
                if should_count_rebalance_version_complete(&result) {
                    rebalanced += 1;
                }
                info!("rebalance_entry {} Entry {} is already deleted, skipping", &bucket, version.name);
                continue;
            }

            if result.failed {
                let err = resolve_rebalance_migrate_result_error(
                    result.error,
                    pool_index,
                    bucket.as_str(),
                    version.name.as_str(),
                    version_id.as_deref(),
                );
                error!(
                    "rebalance_entry {} Error rebalancing entry {}/{:?}: {:?}",
                    &bucket, &version.name, &version.version_id, err
                );
                return Err(with_rebalance_entry_context("migrate", bucket.as_str(), version.name.as_str(), err));
            }

            resolve_rebalance_stats_update_result(
                self.update_pool_stats(pool_index, bucket.clone(), version).await,
                pool_index,
                bucket.as_str(),
                version.name.as_str(),
            )?;
            if should_count_rebalance_version_complete(&result) {
                rebalanced += 1;
            }
        }

        if should_cleanup_rebalance_source_entry(rebalanced, fivs.versions.len()) {
            resolve_rebalance_entry_cleanup_delete_result(
                set.delete_object(
                    bucket.as_str(),
                    &encode_dir_object(&entry.name),
                    ObjectOptions {
                        delete_prefix: true,
                        delete_prefix_object: true,

                        ..Default::default()
                    },
                )
                .await,
                bucket.as_str(),
                entry.name.as_str(),
            )?;
            info!("rebalance_entry {} Entry {} deleted successfully", &bucket, &entry.name);
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, rd))]
    async fn rebalance_object(self: Arc<Self>, pool_idx: usize, bucket: String, rd: GetObjectReader) -> Result<()> {
        data_movement::migrate_object(self, pool_idx, bucket, rd, "rebalance_object").await
    }

    #[tracing::instrument(skip(self, rx))]
    async fn rebalance_bucket(self: &Arc<Self>, rx: CancellationToken, bucket: String, pool_index: usize) -> Result<()> {
        ensure_valid_rebalance_pool_index(self.pools.len(), pool_index)?;

        // Placeholder for actual bucket rebalance logic
        info!("Rebalancing bucket {} in pool {}", bucket, pool_index);

        // TODO: other config
        // if bucket != RUSTFS_META_BUCKET{

        // }

        let pool = clone_arc_by_index(self.pools.as_slice(), pool_index, "invalid rebalance pool index")?;
        let bucket_configs = Arc::new(load_rebalance_bucket_configs(&bucket).await?);

        let mut jobs = Vec::new();
        let entry_error = Arc::new(tokio::sync::Mutex::new(None::<Error>));

        // let wk = Workers::new(pool.disk_set.len() * 2).map_err(Error::other)?;
        // wk.clone().take().await;
        for (set_idx, set) in pool.disk_set.iter().enumerate() {
            let rebalance_entry: ListCallback = Arc::new({
                let this = Arc::clone(self);
                let bucket = bucket.clone();
                let entry_error = entry_error.clone();
                let callback_rx = rx.clone();
                // let wk = wk.clone();
                let set = set.clone();
                let bucket_configs = bucket_configs.clone();
                move |entry: MetaCacheEntry| {
                    let this = this.clone();
                    let bucket = bucket.clone();
                    let entry_error = entry_error.clone();
                    let callback_rx = callback_rx.clone();
                    // let wk = wk.clone();
                    let set = set.clone();
                    let bucket_configs = bucket_configs.clone();
                    Box::pin(async move {
                        if callback_rx.is_cancelled() {
                            return;
                        }
                        if entry_error.lock().await.is_some() {
                            return;
                        }

                        info!("rebalance_entry: rebalance_entry spawn start");
                        // wk.take().await;
                        // tokio::spawn(async move {
                        info!("rebalance_entry: rebalance_entry spawn start2");
                        if let Err(err) = this.rebalance_entry(bucket, pool_index, entry, set, bucket_configs).await {
                            error!("rebalance_entry: rebalance entry failed: {err}");
                            let mut first_err = entry_error.lock().await;
                            if first_err.is_none() {
                                *first_err = Some(err);
                                callback_rx.cancel();
                            }
                        }
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
                let result = set.list_objects_to_rebalance(rx, bucket, rebalance_entry).await;
                if let Err(err) = &result {
                    error!("Rebalance worker {} error: {}", set_idx, err);
                } else {
                    info!("Rebalance worker {} done", set_idx);
                }
                // wk.clone().give().await;
                result
            });

            jobs.push((set_idx, job));
        }

        // wk.wait().await;
        let mut worker_error: Option<Error> = None;
        for (set_idx, job) in jobs {
            if let Err(err) = resolve_rebalance_worker_result(set_idx, job.await)
                && worker_error.is_none()
            {
                worker_error = Some(err);
            }
        }
        let entry_error = entry_error.lock().await.clone();
        resolve_rebalance_bucket_error(entry_error, worker_error)?;

        info!("rebalance_bucket: rebalance_bucket done");
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn save_rebalance_stats(&self, pool_idx: usize, opt: RebalSaveOpt) -> Result<()> {
        let meta_to_save = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            let Some(meta) = rebalance_meta.as_mut() else {
                return Ok(());
            };

            let now = OffsetDateTime::now_utc();
            apply_rebalance_save_option(meta, pool_idx, opt, now);
            meta.clone()
        };

        let pool = clone_first_arc(&self.pools, "save_rebalance_stats: no pools available")?;

        info!(
            "save_rebalance_stats: save rebalance meta, pool_idx: {}, opt: {:?}, meta: {:?}",
            pool_idx, opt, meta_to_save
        );
        let stage = format!("save_rebalance_stats for pool {pool_idx} opt {opt:?}");
        resolve_rebalance_meta_save_result(meta_to_save.save(pool).await, stage.as_str())?;

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
        ensure_rebalance_listing_disks_available(!disks.is_empty(), &bucket)?;

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

#[cfg(test)]
mod rebalance_unit_tests {
    use super::first_rebalance_bucket;
    use super::is_rebalance_actively_running;
    use super::is_rebalance_conflicting_with_decommission;
    use super::is_rebalance_in_progress;
    use super::percent_free_ratio;
    use super::rebalance_goal_reached;
    use super::{
        GetObjectReader, HTTPRangeSpec, MigrationBackend, MigrationVersionResult, ObjectInfo, ObjectOptions, RebalSaveOpt,
        RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats, RebalanceTerminalEvent, apply_rebalance_save_option,
        apply_rebalance_terminal_event, apply_stopped_at, classify_rebalance_terminal_event, clone_arc_by_index, clone_first_arc,
        clone_rebalance_pool_stats, complete_rebalance_pools_at_goal, ensure_rebalance_listing_disks_available,
        ensure_rebalance_not_decommissioning, ensure_valid_rebalance_pool_index, is_rebalance_stopped_terminal_event,
        load_rebalance_bucket_configs, mark_rebalance_bucket_done, migrate_entry_version, next_rebal_bucket_from_stat,
        rebalance_delete_marker_opts, rebalance_meta_load_no_data_error, rebalance_meta_load_unknown_format_error,
        rebalance_meta_load_unknown_version_error, resolve_load_rebalance_stats_update_result, resolve_next_rebalance_bucket,
        resolve_rebalance_bucket_error, resolve_rebalance_bucket_result, resolve_rebalance_entry_cleanup_delete_result,
        resolve_rebalance_file_info_versions_result, resolve_rebalance_meta_load_result, resolve_rebalance_meta_save_result,
        resolve_rebalance_migrate_result_error, resolve_rebalance_optional_bucket_config_result, resolve_rebalance_participants,
        resolve_rebalance_save_task_result, resolve_rebalance_stats_update_result, resolve_rebalance_terminal_error,
        resolve_rebalance_worker_result, send_rebalance_done_signal, should_cleanup_rebalance_source_entry,
        should_count_rebalance_version_complete, should_ignore_rebalance_data_usage_cache, should_pool_participate,
        should_preserve_rebalance_stopped_state, should_skip_rebalance_delete_marker, should_skip_start_rebalance,
        stop_rebalance_meta_snapshot, stop_rebalance_state, take_bucket_from_rebalance_queue, validate_start_rebalance_state,
        with_rebalance_entry_context,
    };
    use crate::data_movement;
    use crate::data_usage::DATA_USAGE_CACHE_NAME;
    use crate::disk::RUSTFS_META_BUCKET;
    use crate::error::{Error, Result};
    use rustfs_filemeta::FileInfo;
    use rustfs_filemeta::TRANSITION_COMPLETE;
    use rustfs_rio::Index;
    use s3s::dto::ReplicationConfiguration;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use time::OffsetDateTime;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    struct MigrationBackendSpy {
        get_object_reader: Mutex<Option<core::result::Result<GetObjectReader, Error>>>,
        delete_object: Mutex<Option<core::result::Result<ObjectInfo, Error>>>,
        move_remote: Mutex<Option<core::result::Result<(), Error>>>,
        get_calls: AtomicUsize,
        delete_calls: AtomicUsize,
        move_remote_calls: AtomicUsize,
    }

    impl MigrationBackendSpy {
        fn new(
            get_object_reader: Option<core::result::Result<GetObjectReader, Error>>,
            delete_object: Option<core::result::Result<ObjectInfo, Error>>,
            move_remote: Option<core::result::Result<(), Error>>,
        ) -> Self {
            Self {
                get_object_reader: Mutex::new(get_object_reader),
                delete_object: Mutex::new(delete_object),
                move_remote: Mutex::new(move_remote),
                get_calls: AtomicUsize::new(0),
                delete_calls: AtomicUsize::new(0),
                move_remote_calls: AtomicUsize::new(0),
            }
        }

        fn get_calls(&self) -> usize {
            self.get_calls.load(Ordering::SeqCst)
        }

        fn delete_calls(&self) -> usize {
            self.delete_calls.load(Ordering::SeqCst)
        }

        fn move_remote_calls(&self) -> usize {
            self.move_remote_calls.load(Ordering::SeqCst)
        }

        fn make_reader() -> GetObjectReader {
            GetObjectReader {
                stream: Box::new(Cursor::new(vec![0_u8; 3])),
                object_info: ObjectInfo::default(),
            }
        }
    }

    #[async_trait::async_trait]
    impl MigrationBackend for MigrationBackendSpy {
        async fn get_object_reader_for_migration(
            &self,
            _bucket: &str,
            _object: &str,
            _range: Option<HTTPRangeSpec>,
            _h: http::HeaderMap,
            _opts: &ObjectOptions,
        ) -> Result<GetObjectReader> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(result) = self.get_object_reader.lock().unwrap().take() {
                return result;
            }

            Ok(Self::make_reader())
        }

        async fn delete_object_for_migration(&self, _bucket: &str, _object: &str, _opts: ObjectOptions) -> Result<ObjectInfo> {
            self.delete_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(result) = self.delete_object.lock().unwrap().take() {
                return result;
            }

            Ok(ObjectInfo::default())
        }

        async fn move_remote_version_for_migration(
            &self,
            _bucket: &str,
            _object: &str,
            _fi: &FileInfo,
            _opts: &ObjectOptions,
        ) -> Result<()> {
            self.move_remote_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(result) = self.move_remote.lock().unwrap().take() {
                return result;
            }

            Ok(())
        }
    }

    fn version_deleted() -> FileInfo {
        let mut version = FileInfo::new("object.bin", 4, 2);
        version.name = "object.bin".to_string();
        version.deleted = true;
        version
    }

    fn version_normal() -> FileInfo {
        let mut version = FileInfo::new("object.bin", 4, 2);
        version.name = "object.bin".to_string();
        version.size = 64;
        version
    }

    fn version_remote() -> FileInfo {
        let mut version = FileInfo::new("object.bin", 4, 2);
        version.name = "object.bin".to_string();
        version.transition_status = TRANSITION_COMPLETE.to_string();
        version
    }

    #[test]
    fn test_rebalance_delete_marker_opts_preserves_replication_state() {
        let mod_time = OffsetDateTime::now_utc();
        let version = FileInfo {
            mod_time: Some(mod_time),
            replication_state_internal: Some(rustfs_filemeta::ReplicationState {
                replica_status: rustfs_filemeta::ReplicationStatusType::Replica,
                delete_marker: true,
                replicate_decision_str: "existing".to_string(),
                ..Default::default()
            }),
            ..version_deleted()
        };

        let opts = rebalance_delete_marker_opts(&version, Some("version-id".to_string()), 7);
        let replication = opts.delete_replication.expect("replication state should be preserved");

        assert!(opts.versioned);
        assert!(opts.data_movement);
        assert!(opts.delete_marker);
        assert!(opts.skip_decommissioned);
        assert_eq!(opts.src_pool_idx, 7);
        assert_eq!(opts.version_id.as_deref(), Some("version-id"));
        assert_eq!(opts.mod_time, Some(mod_time));
        assert_eq!(replication.replica_status, rustfs_filemeta::ReplicationStatusType::Replica);
        assert!(replication.delete_marker);
        assert_eq!(replication.replicate_decision_str, "existing");
    }

    #[tokio::test]
    async fn test_migrate_entry_version_remote_version_is_moved_without_transfer() {
        let backend = MigrationBackendSpy::new(None, Some(Ok(ObjectInfo::default())), Some(Ok(())));
        let version = version_remote();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            0,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
        assert_eq!(backend.move_remote_calls(), 1);
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_remote_not_found_is_cleanup_ignored() {
        let backend = MigrationBackendSpy::new(
            None,
            Some(Ok(ObjectInfo::default())),
            Some(Err(Error::ObjectNotFound("bucket".to_string(), "object.bin".to_string()))),
        );
        let version = version_remote();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            0,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
        assert_eq!(backend.move_remote_calls(), 1);
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_remote_failure_is_reported() {
        let backend = MigrationBackendSpy::new(None, Some(Ok(ObjectInfo::default())), Some(Err(Error::SlowDown)));
        let version = version_remote();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            0,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(result.failed);
        assert!(matches!(result.error, Some(Error::SlowDown)));
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
        assert_eq!(backend.move_remote_calls(), 1);
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_deleted_version_calls_delete_and_moved() {
        let backend = MigrationBackendSpy::new(None, Some(Ok(ObjectInfo::default())), None);
        let version = version_deleted();
        let mut transfer = |_, _, _| async move { Ok(()) };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_deleted_version_not_found_is_ignored() {
        let backend = MigrationBackendSpy::new(
            None,
            Some(Err(Error::ObjectNotFound("bucket".to_string(), "object.bin".to_string()))),
            None,
        );
        let version = version_deleted();
        let mut transfer = |_, _, _| async move { Ok(()) };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(backend.delete_calls(), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_reader_not_found_is_ignored() {
        let backend = MigrationBackendSpy::new(
            Some(Err(Error::ObjectNotFound("bucket".to_string(), "object.bin".to_string()))),
            None,
            None,
        );
        let version = version_normal();
        let mut transfer = |_, _, _| async move { Ok(()) };

        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(backend.get_calls(), 1);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_reader_retries_before_success() {
        let backend = MigrationBackendSpy::new(Some(Err(Error::SlowDown)), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(backend.get_calls(), 2);
        assert_eq!(backend.delete_calls(), 0);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 1);
    }

    struct AlwaysFailGetBackend {
        get_calls: AtomicUsize,
    }

    impl AlwaysFailGetBackend {
        fn new() -> Self {
            Self {
                get_calls: AtomicUsize::new(0),
            }
        }

        fn get_calls(&self) -> usize {
            self.get_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl MigrationBackend for AlwaysFailGetBackend {
        async fn get_object_reader_for_migration(
            &self,
            _bucket: &str,
            _object: &str,
            _range: Option<HTTPRangeSpec>,
            _h: http::HeaderMap,
            _opts: &ObjectOptions,
        ) -> Result<GetObjectReader> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            Err(Error::SlowDown)
        }

        async fn delete_object_for_migration(&self, _bucket: &str, _object: &str, _opts: ObjectOptions) -> Result<ObjectInfo> {
            Ok(ObjectInfo::default())
        }

        async fn move_remote_version_for_migration(
            &self,
            _bucket: &str,
            _object: &str,
            _fi: &FileInfo,
            _opts: &ObjectOptions,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_migrate_entry_version_reader_fails_after_retries() {
        let backend = AlwaysFailGetBackend::new();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(result.failed);
        assert!(matches!(result.error, Some(Error::SlowDown)));
        assert_eq!(backend.get_calls(), 3);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_zero_max_attempts_still_attempts_once() {
        let backend = AlwaysFailGetBackend::new();
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            0,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(result.failed);
        assert!(matches!(result.error, Some(Error::SlowDown)));
        assert_eq!(backend.get_calls(), 1);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_transfer_retries_before_success() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    let attempt = transfer_count.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        return Err(Error::SlowDown);
                    }
                    Ok(())
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert_eq!(backend.get_calls(), 2);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_transfer_fails_after_retries() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Err(Error::NotModified)
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            2,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.failed);
        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(result.error.is_some());
        assert_eq!(backend.get_calls(), 2);
        assert_eq!(transfer_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_transfer_not_found_is_ignored() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Err(Error::ObjectNotFound("bucket".to_string(), "object.bin".to_string()))
                }
            }
        };

        let version = version_normal();
        let result = migrate_entry_version(
            &backend,
            "bucket".to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            3,
            false,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_ignores_data_usage_cache_when_enabled() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let version = {
            let mut version = version_normal();
            version.name = format!("{}.{}", DATA_USAGE_CACHE_NAME, version.name);
            version
        };
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            RUSTFS_META_BUCKET.to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            2,
            true,
            &mut transfer,
        )
        .await;

        assert!(result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(!result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 0);
        assert_eq!(backend.get_calls(), 0);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[tokio::test]
    async fn test_migrate_entry_version_data_usage_cache_moves_when_ignore_disabled() {
        let backend = MigrationBackendSpy::new(Some(Ok(MigrationBackendSpy::make_reader())), None, None);
        let version = {
            let mut version = version_normal();
            version.name = format!("{}.{}", DATA_USAGE_CACHE_NAME, version.name);
            version
        };
        let transfer_count = Arc::new(AtomicUsize::new(0));
        let mut transfer = {
            let transfer_count = transfer_count.clone();
            move |_, _, _| {
                let transfer_count = transfer_count.clone();
                async move {
                    transfer_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let result = migrate_entry_version(
            &backend,
            RUSTFS_META_BUCKET.to_string(),
            1,
            &version,
            version.version_id.map(|v| v.to_string()),
            2,
            false,
            &mut transfer,
        )
        .await;

        assert!(!result.ignored);
        assert!(!result.cleanup_ignored);
        assert!(result.moved);
        assert!(!result.failed);
        assert!(result.error.is_none());
        assert_eq!(transfer_count.load(Ordering::SeqCst), 1);
        assert_eq!(backend.get_calls(), 1);
        assert_eq!(backend.delete_calls(), 0);
    }

    #[test]
    fn test_should_ignore_rebalance_data_usage_cache_true_for_meta_bucket() {
        assert!(should_ignore_rebalance_data_usage_cache(RUSTFS_META_BUCKET));
    }

    #[test]
    fn test_should_ignore_rebalance_data_usage_cache_false_for_regular_bucket() {
        assert!(!should_ignore_rebalance_data_usage_cache("bucket-a"));
    }

    #[test]
    fn test_rebalance_goal_reached_exact_tolerance_bound() {
        let init_free_space = 200_u64;
        let init_capacity = 1_000_u64;
        let goal = 0.45_f64;

        // goal - 0.05 => 200 + 200 = 400 free / 1000 => 0.4 exactly
        assert!(rebalance_goal_reached(init_free_space, init_capacity, 200, goal));

        // one byte above the tolerance boundary should be false
        assert!(!rebalance_goal_reached(init_free_space, init_capacity, 199, goal));
    }

    #[test]
    fn test_rebalance_goal_reached_within_tolerance() {
        let init_free_space = 200_u64;
        let init_capacity = 1_000_u64;
        let bytes = 250_u64;
        let goal = 0.45_f64;

        assert!(rebalance_goal_reached(init_free_space, init_capacity, bytes, goal));
    }

    #[test]
    fn test_rebalance_goal_not_reached_outside_tolerance() {
        let init_free_space = 100_u64;
        let init_capacity = 1_000_u64;
        let bytes = 80_u64;
        let goal = 0.5_f64;

        assert!(!rebalance_goal_reached(init_free_space, init_capacity, bytes, goal));
    }

    #[test]
    fn test_rebalance_goal_zero_capacity_is_false() {
        assert!(!rebalance_goal_reached(100, 0, 50, 0.5));
    }

    #[test]
    fn test_rebalance_goal_above_one_is_true_when_within_tolerance() {
        assert!(rebalance_goal_reached(950, 1_000, 100, 1.0));
    }

    #[test]
    fn test_rebalance_goal_below_zero_is_true_when_within_tolerance() {
        assert!(rebalance_goal_reached(10, 1_000, 0, -0.01));
    }

    #[test]
    fn test_resolve_rebalance_worker_result_passthrough() {
        assert!(resolve_rebalance_worker_result(0, Ok(Ok(()))).is_ok());

        let err = resolve_rebalance_worker_result(0, Ok(Err(Error::OperationCanceled))).unwrap_err();
        assert!(matches!(err, Error::OperationCanceled));
    }

    #[tokio::test]
    async fn test_resolve_rebalance_worker_result_join_error_keeps_context() {
        let join_error = tokio::spawn(async {
            panic!("rebalance worker panic");
        })
        .await
        .expect_err("panic task should return JoinError");

        let err = resolve_rebalance_worker_result(7, Err(join_error)).unwrap_err();
        assert!(err.to_string().contains("rebalance worker 7 task join error"));
    }

    #[test]
    fn test_resolve_rebalance_save_task_result_passthrough() {
        assert!(resolve_rebalance_save_task_result(0, Ok(Ok(()))).is_ok());
    }

    #[test]
    fn test_resolve_rebalance_save_task_result_wraps_inner_error_context() {
        let err = resolve_rebalance_save_task_result(1, Ok(Err(Error::SlowDown)))
            .expect_err("inner save-task error should include pool context");
        assert!(err.to_string().contains("rebalance save_task failed for pool 1"));
    }

    #[test]
    fn test_resolve_rebalance_meta_save_result_passthrough() {
        assert!(resolve_rebalance_meta_save_result(Ok(()), "stop_rebalance").is_ok());
    }

    #[test]
    fn test_resolve_rebalance_meta_save_result_wraps_error_context() {
        let err = resolve_rebalance_meta_save_result(Err(Error::SlowDown), "init_rebalance_meta")
            .expect_err("meta save failure should include stage context");
        let message = err.to_string();
        assert!(message.contains("rebalance meta save failed during init_rebalance_meta"));
        assert!(message.contains(Error::SlowDown.to_string().as_str()));
    }

    #[test]
    fn test_rebalance_meta_load_no_data_error_formats_context() {
        let err = rebalance_meta_load_no_data_error();
        let rendered = err.to_string();

        assert!(rendered.contains("rebalance metadata load failed"), "{rendered}");
        assert!(rendered.contains("payload is too short"), "{rendered}");
    }

    #[test]
    fn test_rebalance_meta_load_unknown_format_error_formats_context() {
        let err = rebalance_meta_load_unknown_format_error(9);
        let rendered = err.to_string();

        assert!(rendered.contains("rebalance metadata load failed"), "{rendered}");
        assert!(rendered.contains("unknown format 9"), "{rendered}");
    }

    #[test]
    fn test_rebalance_meta_load_unknown_version_error_formats_context() {
        let err = rebalance_meta_load_unknown_version_error(3);
        let rendered = err.to_string();

        assert!(rendered.contains("rebalance metadata load failed"), "{rendered}");
        assert!(rendered.contains("unknown version 3"), "{rendered}");
    }

    #[test]
    fn test_resolve_rebalance_stats_update_result_passthrough() {
        assert!(resolve_rebalance_stats_update_result(Ok(()), 0, "bucket", "object").is_ok());
    }

    #[test]
    fn test_resolve_rebalance_stats_update_result_wraps_error_context() {
        let err = resolve_rebalance_stats_update_result(Err(Error::SlowDown), 2, "bucket-a", "obj.txt")
            .expect_err("stats update error should include context");
        assert!(
            err.to_string()
                .contains("rebalance stats update failed for pool 2 bucket bucket-a object obj.txt")
        );
    }

    #[test]
    fn test_resolve_load_rebalance_stats_update_result_passthrough() {
        assert!(resolve_load_rebalance_stats_update_result(Ok(())).is_ok());
    }

    #[test]
    fn test_resolve_load_rebalance_stats_update_result_wraps_error_context() {
        let err = resolve_load_rebalance_stats_update_result(Err(Error::SlowDown))
            .expect_err("load-time stats refresh failure should include context");
        assert!(err.to_string().contains("rebalance metadata stats refresh failed after load"));
    }

    #[test]
    fn test_resolve_rebalance_file_info_versions_result_passthrough() {
        let value = resolve_rebalance_file_info_versions_result::<usize, Error>(Ok(7), "bucket-a", "obj.txt")
            .expect("ok results should pass through");
        assert_eq!(value, 7);
    }

    #[test]
    fn test_resolve_rebalance_file_info_versions_result_wraps_error_context() {
        let err = resolve_rebalance_file_info_versions_result::<usize, Error>(Err(Error::SlowDown), "bucket-a", "obj.txt")
            .expect_err("errors should be wrapped");
        let message = err.to_string();
        assert!(message.contains("rebalance file_info_versions failed for bucket-a/obj.txt"));
    }

    #[test]
    fn test_resolve_rebalance_meta_load_result_returns_true_for_loaded_meta() {
        assert!(resolve_rebalance_meta_load_result(Ok(())).expect("loaded rebalance metadata should pass through"));
    }

    #[test]
    fn test_resolve_rebalance_meta_load_result_returns_false_for_missing_meta() {
        assert!(
            !resolve_rebalance_meta_load_result(Err(Error::ConfigNotFound))
                .expect("missing rebalance metadata should be treated as not started")
        );
    }

    #[test]
    fn test_resolve_rebalance_meta_load_result_wraps_error_context() {
        let err =
            resolve_rebalance_meta_load_result(Err(Error::SlowDown)).expect_err("unexpected load failures should be wrapped");
        let message = err.to_string();
        assert!(message.contains("rebalance metadata load failed during load_rebalance_meta"));
    }

    #[test]
    fn test_resolve_rebalance_entry_cleanup_delete_result_passthrough() {
        let result = resolve_rebalance_entry_cleanup_delete_result(Ok(ObjectInfo::default()), "bucket-a", "obj.txt");
        assert!(result.is_ok());
    }

    #[test]
    fn test_resolve_rebalance_entry_cleanup_delete_result_ignores_not_found() {
        let result = resolve_rebalance_entry_cleanup_delete_result(
            Err(Error::ObjectNotFound("bucket-a".to_string(), "obj.txt".to_string())),
            "bucket-a",
            "obj.txt",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_resolve_rebalance_entry_cleanup_delete_result_wraps_error_context() {
        let err = resolve_rebalance_entry_cleanup_delete_result(Err(Error::SlowDown), "bucket-a", "obj.txt")
            .expect_err("unexpected cleanup errors should be wrapped");
        let message = err.to_string();
        assert!(message.contains("rebalance cleanup delete failed for bucket-a/obj.txt"));
    }

    #[test]
    fn test_resolve_rebalance_migrate_result_error_preserves_inner_error() {
        let err = resolve_rebalance_migrate_result_error(Some(Error::SlowDown), 2, "bucket-a", "obj.txt", Some("vid-1"));
        assert!(matches!(err, Error::SlowDown));
    }

    #[test]
    fn test_resolve_rebalance_migrate_result_error_wraps_missing_error_context() {
        let err = resolve_rebalance_migrate_result_error(None, 2, "bucket-a", "obj.txt", Some("vid-1"));
        let message = err.to_string();
        assert!(
            message
                .contains("rebalance migration reported failure without error for pool 2 entry bucket-a/obj.txt version vid-1"),
            "{message}"
        );
    }

    #[test]
    fn test_resolve_rebalance_bucket_result_passthrough() {
        assert!(resolve_rebalance_bucket_result(Ok(()), 2, "bucket-a").is_ok());
    }

    #[test]
    fn test_resolve_rebalance_bucket_result_preserves_operation_canceled() {
        let err = resolve_rebalance_bucket_result(Err(Error::OperationCanceled), 2, "bucket-a")
            .expect_err("operation canceled should be preserved");
        assert!(matches!(err, Error::OperationCanceled));
    }

    #[test]
    fn test_resolve_rebalance_bucket_result_wraps_not_initialized_with_context() {
        let err = resolve_rebalance_bucket_result(Err(Error::other("errServerNotInitialized")), 2, "bucket-a")
            .expect_err("not initialized should be surfaced with context");
        let message = err.to_string();
        assert!(message.contains("rebalance bucket bucket-a failed for pool 2"));
        assert!(message.contains("errServerNotInitialized"));
    }

    #[test]
    fn test_rebalance_listing_disks_available_rejects_empty_set() {
        let err = ensure_rebalance_listing_disks_available(false, "bucket-a")
            .expect_err("missing online disks should be reported with bucket context");
        assert!(
            err.to_string()
                .contains("failed to list objects to rebalance for bucket bucket-a: no disks available")
        );
    }

    #[test]
    fn test_rebalance_listing_disks_available_allows_online_disks() {
        assert!(ensure_rebalance_listing_disks_available(true, "bucket-a").is_ok());
    }

    #[test]
    fn test_with_rebalance_entry_context_formats_stage_bucket_and_object() {
        let err = with_rebalance_entry_context("migrate", "bucket-a", "obj.txt", Error::SlowDown);
        let message = err.to_string();
        assert!(message.contains("rebalance entry migrate failed for bucket-a/obj.txt"));
        assert!(message.contains("Please reduce your request rate"));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_for_cleanup_safe_ignored_result() {
        let result = MigrationVersionResult {
            ignored: true,
            cleanup_ignored: true,
            ..Default::default()
        };
        assert!(should_count_rebalance_version_complete(&result));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_rejects_skip_only_ignored_result() {
        let result = MigrationVersionResult {
            ignored: true,
            cleanup_ignored: false,
            ..Default::default()
        };
        assert!(!should_count_rebalance_version_complete(&result));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_for_moved_result() {
        let result = MigrationVersionResult {
            moved: true,
            ..Default::default()
        };
        assert!(should_count_rebalance_version_complete(&result));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_rejects_failed_result() {
        let result = MigrationVersionResult {
            moved: true,
            failed: true,
            ..Default::default()
        };
        assert!(!should_count_rebalance_version_complete(&result));
    }

    #[test]
    fn test_should_count_rebalance_version_complete_rejects_incomplete_result() {
        assert!(!should_count_rebalance_version_complete(&MigrationVersionResult::default()));
    }

    #[test]
    fn test_should_skip_rebalance_delete_marker_when_last_remaining_without_replication() {
        assert!(should_skip_rebalance_delete_marker(&version_deleted(), 1, false));
    }

    #[test]
    fn test_should_skip_rebalance_delete_marker_rejects_configured_replication() {
        assert!(!should_skip_rebalance_delete_marker(&version_deleted(), 1, true));
    }

    #[test]
    fn test_should_skip_rebalance_delete_marker_rejects_non_deleted_versions() {
        assert!(!should_skip_rebalance_delete_marker(&version_normal(), 1, false));
    }

    #[test]
    fn test_should_skip_rebalance_delete_marker_rejects_multiple_remaining_versions() {
        assert!(!should_skip_rebalance_delete_marker(&version_deleted(), 2, false));
    }

    #[test]
    fn test_should_cleanup_rebalance_source_entry_accepts_all_versions_completed() {
        assert!(should_cleanup_rebalance_source_entry(3, 3));
    }

    #[test]
    fn test_should_cleanup_rebalance_source_entry_rejects_versions_only_expired_by_lifecycle() {
        assert!(!should_cleanup_rebalance_source_entry(2, 3));
    }

    #[test]
    fn test_resolve_rebalance_optional_bucket_config_result_passthrough() {
        let result = resolve_rebalance_optional_bucket_config_result(
            "bucket-a",
            "replication",
            Ok((ReplicationConfiguration::default(), OffsetDateTime::UNIX_EPOCH)),
        )
        .expect("bucket config should pass through");
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_rebalance_optional_bucket_config_result_returns_none_for_missing_config() {
        let result = resolve_rebalance_optional_bucket_config_result::<()>("bucket-a", "versioning", Err(Error::ConfigNotFound))
            .expect("missing bucket config should map to None");
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_rebalance_optional_bucket_config_result_wraps_other_errors() {
        let err = resolve_rebalance_optional_bucket_config_result::<()>("bucket-a", "replication", Err(Error::SlowDown))
            .expect_err("unexpected bucket config errors should be wrapped with context");
        assert!(
            err.to_string()
                .contains("rebalance replication config load failed for bucket bucket-a")
        );
    }

    #[tokio::test]
    async fn test_load_rebalance_bucket_configs_skips_meta_bucket_lookup() {
        let configs = load_rebalance_bucket_configs(RUSTFS_META_BUCKET)
            .await
            .expect("meta bucket config loading should short-circuit");
        assert!(configs.lifecycle_config.is_none());
        assert!(configs.lock_retention.is_none());
        assert!(configs.replication_config.is_none());
    }

    #[tokio::test]
    async fn test_resolve_rebalance_save_task_result_join_error_keeps_context() {
        let join_error = tokio::spawn(async {
            panic!("rebalance save task panic");
        })
        .await
        .expect_err("panic task should return JoinError");

        let err = resolve_rebalance_save_task_result(3, Err(join_error)).unwrap_err();
        assert!(err.to_string().contains("rebalance save_task for pool 3 join error"));
    }

    #[tokio::test]
    async fn test_send_rebalance_done_signal_sends_message() {
        let (tx, mut rx) = mpsc::channel(1);

        send_rebalance_done_signal(&tx, Ok(()), 2)
            .await
            .expect("send should succeed when receiver is active");

        let received = rx.recv().await.expect("receiver should get signal");
        assert!(received.is_ok());
    }

    #[tokio::test]
    async fn test_send_rebalance_done_signal_reports_closed_channel() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);

        let err = send_rebalance_done_signal(&tx, Ok(()), 5)
            .await
            .expect_err("send should fail when receiver is closed");
        assert!(err.to_string().contains("rebalance done signal send failed for pool 5"));
    }

    #[test]
    fn test_resolve_rebalance_terminal_error_keeps_primary_when_signal_ok() {
        let err = resolve_rebalance_terminal_error(Error::SlowDown, Ok(()));
        assert!(matches!(err, Error::SlowDown));
    }

    #[test]
    fn test_resolve_rebalance_terminal_error_wraps_signal_failure_context() {
        let err = resolve_rebalance_terminal_error(Error::SlowDown, Err(Error::OperationCanceled));
        assert!(err.to_string().contains("rebalance terminal signal failed after error"));
    }

    #[test]
    fn test_resolve_rebalance_bucket_error_prefers_entry_error() {
        let err = resolve_rebalance_bucket_error(Some(Error::OperationCanceled), Some(Error::SlowDown)).unwrap_err();
        assert!(matches!(err, Error::OperationCanceled));
    }

    #[test]
    fn test_resolve_rebalance_bucket_error_uses_worker_error_when_entry_ok() {
        let err = resolve_rebalance_bucket_error(None, Some(Error::SlowDown)).unwrap_err();
        assert!(matches!(err, Error::SlowDown));
    }

    #[test]
    fn test_resolve_rebalance_bucket_error_is_ok_when_no_errors() {
        assert!(resolve_rebalance_bucket_error(None, None).is_ok());
    }

    #[test]
    fn test_ensure_valid_rebalance_pool_index_allows_in_range() {
        assert!(ensure_valid_rebalance_pool_index(3, 2).is_ok());
    }

    #[test]
    fn test_ensure_valid_rebalance_pool_index_rejects_out_of_range() {
        let err = ensure_valid_rebalance_pool_index(2, 2).expect_err("out of range index should fail");
        assert!(err.to_string().contains("invalid rebalance pool index"));
    }

    #[test]
    fn test_clone_first_arc_returns_first_value() {
        let values = vec![Arc::new(7_u8), Arc::new(9_u8)];
        let first = clone_first_arc(values.as_slice(), "empty values").expect("first value should be returned");
        assert_eq!(*first, 7_u8);
    }

    #[test]
    fn test_clone_first_arc_rejects_empty_values() {
        let values: Vec<Arc<u8>> = Vec::new();
        let err = clone_first_arc(values.as_slice(), "empty values").expect_err("empty values should fail");
        assert!(err.to_string().contains("empty values"));
    }

    #[test]
    fn test_clone_arc_by_index_returns_value() {
        let values = vec![Arc::new(7_u8), Arc::new(9_u8)];
        let value =
            clone_arc_by_index(values.as_slice(), 1, "invalid rebalance pool index").expect("index within bounds should work");
        assert_eq!(*value, 9_u8);
    }

    #[test]
    fn test_clone_arc_by_index_rejects_out_of_range() {
        let values = vec![Arc::new(7_u8)];
        let err =
            clone_arc_by_index(values.as_slice(), 2, "invalid rebalance pool index").expect_err("out of range index should fail");
        assert!(err.to_string().contains("invalid rebalance pool index: 2"));
    }

    #[test]
    fn test_classify_rebalance_terminal_event_completed() {
        let now = OffsetDateTime::now_utc();
        match classify_rebalance_terminal_event(Some(Ok(())), now) {
            RebalanceTerminalEvent::Completed { msg } => assert!(msg.contains("Rebalance completed")),
            _ => panic!("expected completed terminal event"),
        }
    }

    #[test]
    fn test_classify_rebalance_terminal_event_stopped() {
        let now = OffsetDateTime::now_utc();
        match classify_rebalance_terminal_event(Some(Err(Error::OperationCanceled)), now) {
            RebalanceTerminalEvent::Stopped { msg } => assert!(msg.contains("Rebalance stopped")),
            _ => panic!("expected stopped terminal event"),
        }
    }

    #[test]
    fn test_classify_rebalance_terminal_event_failed() {
        let now = OffsetDateTime::now_utc();
        match classify_rebalance_terminal_event(Some(Err(Error::SlowDown)), now) {
            RebalanceTerminalEvent::Failed { msg, last_error } => {
                assert!(msg.contains("Rebalance failed"));
                assert!(msg.contains("with err"));
                assert_eq!(last_error, Error::SlowDown.to_string());
            }
            _ => panic!("expected failed terminal event"),
        }
    }

    #[test]
    fn test_classify_rebalance_terminal_event_channel_closed() {
        let now = OffsetDateTime::now_utc();
        match classify_rebalance_terminal_event(None, now) {
            RebalanceTerminalEvent::ChannelClosed { msg, last_error } => {
                assert!(msg.contains("channel closed"));
                assert!(last_error.contains("before terminal event"));
                assert!(msg.contains("at"));
                assert!(last_error.contains("at"));
            }
            _ => panic!("expected channel closed terminal event"),
        }
    }

    #[test]
    fn test_apply_rebalance_terminal_event_channel_closed_marks_failed() {
        let now = OffsetDateTime::now_utc();
        let mut status = RebalStatus::Started;
        let mut end_time = None;
        let mut last_error = None;

        apply_rebalance_terminal_event(
            &mut status,
            &mut end_time,
            &mut last_error,
            RebalanceTerminalEvent::ChannelClosed {
                msg: "channel closed".to_string(),
                last_error: "rebalance save channel closed before terminal event".to_string(),
            },
            now,
        );

        assert_eq!(status, RebalStatus::Failed);
        assert_eq!(end_time, Some(now));
        assert_eq!(last_error.as_deref(), Some("rebalance save channel closed before terminal event"));
    }

    #[test]
    fn test_apply_rebalance_terminal_event_stopped_clears_error() {
        let now = OffsetDateTime::now_utc();
        let mut status = RebalStatus::Started;
        let mut end_time = None;
        let mut last_error = Some("old-error".to_string());

        apply_rebalance_terminal_event(
            &mut status,
            &mut end_time,
            &mut last_error,
            RebalanceTerminalEvent::Stopped {
                msg: "rebalance stopped".to_string(),
            },
            now,
        );

        assert_eq!(status, RebalStatus::Stopped);
        assert_eq!(end_time, Some(now));
        assert_eq!(last_error, None);
    }

    #[test]
    fn test_is_rebalance_stopped_terminal_event_only_matches_stopped_variant() {
        let stopped = RebalanceTerminalEvent::Stopped {
            msg: "stopped".to_string(),
        };
        let completed = RebalanceTerminalEvent::Completed {
            msg: "completed".to_string(),
        };

        assert!(is_rebalance_stopped_terminal_event(&stopped));
        assert!(!is_rebalance_stopped_terminal_event(&completed));
    }

    #[test]
    fn test_should_preserve_rebalance_stopped_state_when_meta_marked_stopped() {
        let event = RebalanceTerminalEvent::Completed {
            msg: "completed".to_string(),
        };

        assert!(should_preserve_rebalance_stopped_state(true, RebalStatus::Started, &event));
    }

    #[test]
    fn test_should_preserve_rebalance_stopped_state_when_pool_already_stopped() {
        let event = RebalanceTerminalEvent::Failed {
            msg: "failed".to_string(),
            last_error: "boom".to_string(),
        };

        assert!(should_preserve_rebalance_stopped_state(false, RebalStatus::Stopped, &event));
    }

    #[test]
    fn test_should_preserve_rebalance_stopped_state_allows_stopped_terminal_update() {
        let event = RebalanceTerminalEvent::Stopped {
            msg: "stopped".to_string(),
        };

        assert!(!should_preserve_rebalance_stopped_state(true, RebalStatus::Started, &event));
    }

    #[test]
    fn test_ensure_rebalance_not_decommissioning_rejects_running_decommission() {
        assert!(!ensure_rebalance_not_decommissioning(true));
    }

    #[test]
    fn test_ensure_rebalance_not_decommissioning_allows_idle_decommission() {
        assert!(ensure_rebalance_not_decommissioning(false));
    }

    #[test]
    fn test_validate_start_rebalance_state_rejects_running_decommission() {
        let err = validate_start_rebalance_state(true, true).expect_err("running decommission should block rebalance start");
        assert!(matches!(err, Error::DecommissionAlreadyRunning));
    }

    #[test]
    fn test_validate_start_rebalance_state_rejects_missing_meta() {
        let err = validate_start_rebalance_state(false, false).expect_err("missing rebalance meta should fail");
        assert!(matches!(err, Error::ConfigNotFound));
    }

    #[test]
    fn test_validate_start_rebalance_state_allows_loaded_meta() {
        validate_start_rebalance_state(false, true).expect("loaded rebalance meta should allow start");
    }

    #[test]
    fn test_percent_free_ratio_zero_capacity_is_zero() {
        assert_eq!(percent_free_ratio(100, 0), 0.0);
    }

    #[test]
    fn test_percent_free_ratio_normal_case() {
        assert_eq!(percent_free_ratio(250, 1_000), 0.25);
    }

    #[test]
    fn test_should_pool_participate_false_when_capacity_zero() {
        assert!(!should_pool_participate(0, 0, 0.2));
    }

    #[test]
    fn test_should_pool_participate_true_when_ratio_below_goal() {
        assert!(should_pool_participate(200, 1_000, 0.3));
    }

    #[test]
    fn test_should_pool_participate_false_when_ratio_meets_goal() {
        assert!(!should_pool_participate(300, 1_000, 0.3));
    }

    #[test]
    fn test_complete_rebalance_pools_at_goal_marks_started_participants_completed() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let mut meta = RebalanceMeta {
            percent_free_goal: 0.5,
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    init_free_space: 400,
                    init_capacity: 1_000,
                    bytes: 50,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: true,
                    init_free_space: 100,
                    init_capacity: 1_000,
                    bytes: 0,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(complete_rebalance_pools_at_goal(&mut meta, now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Completed);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(now));
        assert_eq!(meta.pool_stats[1].info.status, RebalStatus::Started);
    }

    #[test]
    fn test_should_skip_start_rebalance_only_when_running_and_cancel_attached() {
        assert!(should_skip_start_rebalance(true, true));
        assert!(!should_skip_start_rebalance(true, false));
        assert!(!should_skip_start_rebalance(false, true));
        assert!(!should_skip_start_rebalance(false, false));
    }

    #[test]
    fn test_new_multipart_abort_flag_defaults_to_abort_enabled() {
        let flag = data_movement::new_multipart_abort_flag();
        assert!(data_movement::should_abort_multipart_upload(&flag));
    }

    #[test]
    fn test_mark_multipart_upload_completed_disables_abort_cleanup() {
        let flag = data_movement::new_multipart_abort_flag();
        data_movement::mark_multipart_upload_completed(&flag);
        assert!(!data_movement::should_abort_multipart_upload(&flag));
    }

    #[test]
    fn test_decode_part_index_returns_none_when_absent() {
        assert!(data_movement::decode_part_index(None).is_none());
    }

    #[test]
    fn test_decode_part_index_returns_none_for_invalid_payload() {
        let invalid = bytes::Bytes::from_static(b"not-a-valid-index");
        assert!(data_movement::decode_part_index(Some(&invalid)).is_none());
    }

    #[test]
    fn test_decode_part_index_returns_some_for_valid_payload() {
        let mut index = Index::new();
        index.add(0, 0).expect("first index entry should be accepted");
        index
            .add(2_097_152, 2_097_152)
            .expect("second index entry should advance totals");

        let encoded = index.into_vec();
        let decoded = data_movement::decode_part_index(Some(&encoded)).expect("valid index payload should decode");

        assert_eq!(decoded.total_uncompressed, 2_097_152);
        assert_eq!(decoded.total_compressed, 2_097_152);
    }

    #[test]
    fn test_resolve_rebalance_participants_respects_runtime_pool_count() {
        let now = OffsetDateTime::now_utc();
        let stats = vec![
            RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
            RebalanceStats {
                participating: false,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
            RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let participants = resolve_rebalance_participants(stats.as_slice(), 2);
        assert_eq!(participants, vec![true, false]);
    }

    #[test]
    fn test_resolve_rebalance_participants_requires_started_status() {
        let now = OffsetDateTime::now_utc();
        let stats = vec![
            RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
            RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let participants = resolve_rebalance_participants(stats.as_slice(), 2);
        assert_eq!(participants, vec![false, true]);
    }

    #[test]
    fn test_is_rebalance_actively_running_requires_cancel_and_started_state() {
        let now = OffsetDateTime::now_utc();
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!is_rebalance_actively_running(&meta));
        meta.cancel = Some(CancellationToken::new());
        assert!(is_rebalance_actively_running(&meta));
    }

    #[test]
    fn test_is_rebalance_in_progress_only_started_participants() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            stopped_at: None,
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    info: RebalanceInfo {
                        start_time: Some(now),
                        status: RebalStatus::Completed,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: true,
                    info: RebalanceInfo {
                        start_time: Some(now),
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(is_rebalance_in_progress(&meta));
    }

    #[test]
    fn test_is_rebalance_conflicting_with_decommission_true_when_in_progress() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            stopped_at: None,
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(is_rebalance_conflicting_with_decommission(&meta));
    }

    #[test]
    fn test_is_rebalance_conflicting_with_decommission_false_when_stopped() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            stopped_at: Some(now),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!is_rebalance_conflicting_with_decommission(&meta));
    }

    #[test]
    fn test_is_rebalance_in_progress_stopped_takes_precedence() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            stopped_at: Some(now),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!is_rebalance_in_progress(&meta));
    }

    #[test]
    fn test_first_rebalance_bucket_returns_first_name() {
        let pool_stat = RebalanceStats {
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
            ..Default::default()
        };

        assert_eq!(first_rebalance_bucket(&pool_stat), Some("bucket-a".to_string()));
    }

    #[test]
    fn test_first_rebalance_bucket_returns_none_when_empty() {
        let pool_stat = RebalanceStats::default();

        assert_eq!(first_rebalance_bucket(&pool_stat), None);
    }

    #[test]
    fn test_next_rebal_bucket_from_stat_respects_empty_queue() {
        let pool_stat = RebalanceStats {
            buckets: vec![],
            ..Default::default()
        };

        assert_eq!(next_rebal_bucket_from_stat(&pool_stat), None);
    }

    #[test]
    fn test_next_rebal_bucket_from_stat_returns_first_bucket() {
        let now = OffsetDateTime::now_utc();
        let pool_stat = RebalanceStats {
            participating: true,
            info: RebalanceInfo {
                status: RebalStatus::Started,
                start_time: Some(now),
                ..Default::default()
            },
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
            ..Default::default()
        };

        assert_eq!(next_rebal_bucket_from_stat(&pool_stat), Some("bucket-a".to_string()));
    }

    #[test]
    fn test_clone_rebalance_pool_stats_rejects_missing_meta() {
        let err = clone_rebalance_pool_stats(None).expect_err("missing rebalance meta should fail");
        assert!(
            err.to_string()
                .contains("failed to clone rebalance pool stats: rebalance metadata not initialized")
        );
    }

    #[test]
    fn test_clone_rebalance_pool_stats_clones_entries() {
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats::default()],
            ..Default::default()
        };

        let stats = clone_rebalance_pool_stats(Some(&meta)).expect("metadata should clone pool stats");
        assert_eq!(stats.len(), 1);
    }

    #[test]
    fn test_resolve_next_rebalance_bucket_rejects_missing_meta() {
        let err = resolve_next_rebalance_bucket(None, 0).expect_err("missing meta should fail");
        assert!(
            err.to_string()
                .contains("failed to resolve next rebalance bucket: rebalance metadata not initialized")
        );
    }

    #[test]
    fn test_resolve_next_rebalance_bucket_rejects_invalid_pool_index() {
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats::default()],
            ..Default::default()
        };

        let err = resolve_next_rebalance_bucket(Some(&meta), 3).expect_err("invalid pool index should fail");
        assert!(err.to_string().contains("invalid rebalance pool index 3 for 1 pools"));
    }

    #[test]
    fn test_resolve_next_rebalance_bucket_returns_none_for_completed_pool() {
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    ..Default::default()
                },
                buckets: vec!["bucket-a".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let next = resolve_next_rebalance_bucket(Some(&meta), 0).expect("completed pool should return none");
        assert!(next.is_none());
    }

    #[test]
    fn test_resolve_next_rebalance_bucket_returns_first_bucket_for_active_pool() {
        let now = OffsetDateTime::now_utc();
        let meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let next = resolve_next_rebalance_bucket(Some(&meta), 0).expect("active pool should return first bucket");
        assert_eq!(next.as_deref(), Some("bucket-a"));
    }

    #[test]
    fn test_take_bucket_from_rebalance_queue_moves_bucket_and_keeps_remaining() {
        let mut pool_stat = RebalanceStats {
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string(), "bucket-a".to_string()],
            rebalanced_buckets: Vec::new(),
            ..Default::default()
        };

        assert!(take_bucket_from_rebalance_queue(&mut pool_stat, "bucket-a"));
        assert_eq!(pool_stat.buckets, vec!["bucket-b".to_string()]);
        assert_eq!(pool_stat.rebalanced_buckets, vec!["bucket-a".to_string(), "bucket-a".to_string()]);
    }

    #[test]
    fn test_take_bucket_from_rebalance_queue_no_match_keeps_queue() {
        let mut pool_stat = RebalanceStats {
            buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
            rebalanced_buckets: Vec::new(),
            ..Default::default()
        };

        assert!(!take_bucket_from_rebalance_queue(&mut pool_stat, "bucket-c"));
        assert_eq!(pool_stat.buckets, vec!["bucket-a".to_string(), "bucket-b".to_string()]);
        assert!(pool_stat.rebalanced_buckets.is_empty());
    }

    #[test]
    fn test_mark_rebalance_bucket_done_rejects_missing_meta() {
        let err = mark_rebalance_bucket_done(None, 0, "bucket-a").expect_err("missing meta should fail");
        assert!(
            err.to_string()
                .contains("failed to mark rebalance bucket done: rebalance metadata not initialized")
        );
    }

    #[test]
    fn test_mark_rebalance_bucket_done_rejects_invalid_pool_index() {
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats::default()],
            ..Default::default()
        };

        let err = mark_rebalance_bucket_done(Some(&mut meta), 3, "bucket-a").expect_err("invalid pool index should fail");
        assert!(err.to_string().contains("invalid rebalance pool index 3 for 1 pools"));
    }

    #[test]
    fn test_mark_rebalance_bucket_done_rejects_missing_bucket() {
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                buckets: vec!["bucket-a".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = mark_rebalance_bucket_done(Some(&mut meta), 0, "bucket-x").expect_err("missing bucket should fail");
        assert!(
            err.to_string()
                .contains("failed to mark rebalance bucket done: bucket bucket-x was not queued for pool 0")
        );
    }

    #[test]
    fn test_mark_rebalance_bucket_done_marks_bucket_as_rebalanced() {
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                buckets: vec!["bucket-a".to_string(), "bucket-b".to_string()],
                rebalanced_buckets: Vec::new(),
                ..Default::default()
            }],
            ..Default::default()
        };

        mark_rebalance_bucket_done(Some(&mut meta), 0, "bucket-a").expect("bucket in queue should be marked done");
        assert_eq!(meta.pool_stats[0].buckets, vec!["bucket-b".to_string()]);
        assert_eq!(meta.pool_stats[0].rebalanced_buckets, vec!["bucket-a".to_string()]);
    }

    #[test]
    fn test_apply_stopped_at_transitions_started_pools_only() {
        let now = OffsetDateTime::now_utc();
        let mut meta = RebalanceMeta {
            pool_stats: vec![
                RebalanceStats {
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        end_time: None,
                        last_error: Some("old".to_string()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    info: RebalanceInfo {
                        status: RebalStatus::Failed,
                        end_time: Some(now),
                        last_error: Some("failed".to_string()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        apply_stopped_at(&mut meta, now);

        assert_eq!(meta.stopped_at, Some(now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(now));
        assert_eq!(meta.pool_stats[0].info.last_error, None);

        assert_eq!(meta.pool_stats[1].info.status, RebalStatus::Failed);
        assert_eq!(meta.pool_stats[1].info.end_time, Some(now));
        assert_eq!(meta.pool_stats[1].info.last_error.as_deref(), Some("failed"));
    }

    #[test]
    fn test_stop_rebalance_state_cancels_token_and_marks_stopped_when_in_progress() {
        let now = OffsetDateTime::from_unix_timestamp(10_000).unwrap();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut meta = RebalanceMeta {
            cancel: Some(cancel),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        stop_rebalance_state(&mut meta, now);

        assert!(cancel_clone.is_cancelled());
        assert!(meta.cancel.is_none());
        assert_eq!(meta.stopped_at, Some(now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(now));
    }

    #[test]
    fn test_stop_rebalance_state_clears_token_without_forcing_stopped_when_not_in_progress() {
        let now = OffsetDateTime::from_unix_timestamp(20_000).unwrap();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut meta = RebalanceMeta {
            cancel: Some(cancel),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    end_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        stop_rebalance_state(&mut meta, now);

        assert!(cancel_clone.is_cancelled());
        assert!(meta.cancel.is_none());
        assert_eq!(meta.stopped_at, None);
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Completed);
    }

    #[test]
    fn test_stop_rebalance_state_normalizes_started_pool_when_stopped_at_already_set() {
        let stopped_at = OffsetDateTime::from_unix_timestamp(30_000).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(40_000).unwrap();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut meta = RebalanceMeta {
            cancel: Some(cancel),
            stopped_at: Some(stopped_at),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    last_error: Some("stale".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        stop_rebalance_state(&mut meta, now);

        assert!(cancel_clone.is_cancelled());
        assert!(meta.cancel.is_none());
        assert_eq!(meta.stopped_at, Some(stopped_at));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(stopped_at));
        assert_eq!(meta.pool_stats[0].info.last_error, None);
    }

    #[test]
    fn test_stop_rebalance_meta_snapshot_returns_none_when_meta_missing() {
        let now = OffsetDateTime::from_unix_timestamp(50_000).unwrap();
        assert!(stop_rebalance_meta_snapshot(None, now).is_none());
    }

    #[test]
    fn test_stop_rebalance_meta_snapshot_stops_meta_and_returns_snapshot() {
        let now = OffsetDateTime::from_unix_timestamp(60_000).unwrap();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut meta = RebalanceMeta {
            cancel: Some(cancel),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let snapshot = stop_rebalance_meta_snapshot(Some(&mut meta), now).expect("snapshot should be returned for present meta");

        assert!(cancel_clone.is_cancelled());
        assert!(meta.cancel.is_none());
        assert_eq!(meta.stopped_at, Some(now));
        assert_eq!(meta.last_refreshed_at, Some(now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);

        assert!(snapshot.cancel.is_none());
        assert_eq!(snapshot.stopped_at, Some(now));
        assert_eq!(snapshot.last_refreshed_at, Some(now));
        assert_eq!(snapshot.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(snapshot.pool_stats[0].info.end_time, Some(now));
    }

    #[test]
    fn test_apply_rebalance_save_option_stats_keeps_pool_status_and_updates_refresh() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let later = OffsetDateTime::from_unix_timestamp(2_000).unwrap();
        let mut meta = RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                buckets: vec!["bucket-a".to_string()],
                ..Default::default()
            }],
            last_refreshed_at: Some(now),
            stopped_at: None,
            ..Default::default()
        };

        apply_rebalance_save_option(&mut meta, 0, RebalSaveOpt::Stats, later);

        assert_eq!(meta.last_refreshed_at, Some(later));
        assert_eq!(meta.stopped_at, None);
        assert_eq!(meta.pool_stats.len(), 1);
        assert!(meta.pool_stats[0].participating);
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Started);
        assert_eq!(meta.pool_stats[0].info.start_time, Some(now));
        assert_eq!(meta.pool_stats[0].buckets, vec!["bucket-a".to_string()]);
    }

    #[test]
    fn test_apply_rebalance_save_option_stopped_at_updates_refresh_and_statuses() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let mut meta = RebalanceMeta {
            pool_stats: vec![
                RebalanceStats {
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    info: RebalanceInfo {
                        status: RebalStatus::Failed,
                        last_error: Some("previous failure".to_string()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        apply_rebalance_save_option(&mut meta, 9_000, RebalSaveOpt::StoppedAt, now);

        assert_eq!(meta.stopped_at, Some(now));
        assert_eq!(meta.last_refreshed_at, Some(now));
        assert_eq!(meta.pool_stats[0].info.status, RebalStatus::Stopped);
        assert_eq!(meta.pool_stats[0].info.end_time, Some(now));
        assert!(meta.pool_stats[0].info.last_error.is_none());
        assert_eq!(meta.pool_stats[1].info.status, RebalStatus::Failed);
        assert_eq!(meta.pool_stats[1].info.last_error.as_deref(), Some("previous failure"));
    }

    #[test]
    fn test_rebalance_stats_update_counts_and_bytes_growth() {
        let mut stat = RebalanceStats {
            bucket: "bucket-a".to_string(),
            object: "obj-previous".to_string(),
            num_objects: 3,
            num_versions: 5,
            bytes: 120,
            ..Default::default()
        };

        let mut latest = FileInfo::new("object-1", 4, 2);
        latest.name = "object-1".to_string();
        latest.is_latest = true;
        latest.size = 300;
        latest.deleted = false;
        latest.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        latest.version_id = None;

        let mut historical = FileInfo::new("object-1", 4, 2);
        historical.name = "object-1".to_string();
        historical.is_latest = false;
        historical.size = 128;
        historical.deleted = false;
        historical.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        historical.version_id = None;

        let mut tombstone = FileInfo::new("object-1", 4, 2);
        tombstone.name = "object-1".to_string();
        tombstone.is_latest = false;
        tombstone.size = 64;
        tombstone.deleted = true;
        tombstone.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        tombstone.version_id = None;

        stat.update("bucket-b".to_string(), &latest);
        stat.update("bucket-b".to_string(), &historical);
        stat.update("bucket-b".to_string(), &tombstone);

        assert_eq!(stat.bucket, "bucket-b");
        assert_eq!(stat.object, "object-1");
        assert_eq!(stat.num_objects, 4);
        assert_eq!(stat.num_versions, 8);
        let expected_bytes = 120_u64
            + (latest.size * (latest.erasure.data_blocks + latest.erasure.parity_blocks) as i64
                / latest.erasure.data_blocks as i64) as u64
            + (historical.size * (historical.erasure.data_blocks + historical.erasure.parity_blocks) as i64
                / historical.erasure.data_blocks as i64) as u64;
        assert_eq!(stat.bytes, expected_bytes);
    }

    #[test]
    fn test_rebalance_stats_update_ignores_invalid_data_blocks() {
        let mut stat = RebalanceStats {
            bucket: "bucket-a".to_string(),
            object: "obj-previous".to_string(),
            num_objects: 1,
            num_versions: 2,
            bytes: 77,
            ..Default::default()
        };

        let mut invalid = FileInfo::new("object-invalid", 0, 2);
        invalid.name = "object-invalid".to_string();
        invalid.is_latest = true;
        invalid.size = 256;
        invalid.deleted = false;
        invalid.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        invalid.version_id = None;

        stat.update("bucket-z".to_string(), &invalid);

        assert_eq!(stat.bucket, "bucket-z");
        assert_eq!(stat.object, "object-invalid");
        assert_eq!(stat.num_objects, 2);
        assert_eq!(stat.num_versions, 3);
        assert_eq!(stat.bytes, 77);
    }

    #[test]
    fn test_rebalance_goal_reached_tolerance_and_regression() {
        let init_free_space = 150_u64;
        let init_capacity = 800_u64;
        let goal = 0.35_f64;

        assert!(!rebalance_goal_reached(init_free_space, init_capacity, 0, goal));
        assert!(rebalance_goal_reached(init_free_space, init_capacity, 90, goal));
        assert!(!rebalance_goal_reached(init_free_space, init_capacity, 89, goal));
    }
}
