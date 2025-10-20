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

use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use crate::config::com::{CONFIG_PREFIX, read_config, save_config};
use crate::data_usage::DATA_USAGE_CACHE_NAME;
use crate::disk::error::DiskError;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::error::{
    StorageError, is_err_bucket_exists, is_err_bucket_not_found, is_err_data_movement_overwrite, is_err_object_not_found,
    is_err_version_not_found,
};
use crate::new_object_layer_fn;
use crate::notification_sys::get_global_notification_sys;
use crate::set_disk::SetDisks;
use crate::store_api::{
    BucketOptions, CompletePart, GetObjectReader, MakeBucketOptions, ObjectIO, ObjectOptions, PutObjReader, StorageAPI,
};
use crate::{sets::Sets, store::ECStore};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use futures::future::BoxFuture;
use http::HeaderMap;
use rmp_serde::{Deserializer, Serializer};
use rustfs_common::defer;
use rustfs_common::heal_channel::HealOpts;
use rustfs_filemeta::{MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use rustfs_rio::{HashReader, WarpReader};
use rustfs_utils::path::{SLASH_SEPARATOR, encode_dir_object, path_join};
use rustfs_workers::workers::Workers;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub const POOL_META_NAME: &str = "pool.bin";
pub const POOL_META_FORMAT: u16 = 1;
pub const POOL_META_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatus {
    #[serde(rename = "id")]
    pub id: usize,
    #[serde(rename = "cmdline")]
    pub cmd_line: String,
    #[serde(rename = "lastUpdate", with = "time::serde::rfc3339")]
    pub last_update: OffsetDateTime,
    #[serde(rename = "decommissionInfo")]
    pub decommission: Option<PoolDecommissionInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolMeta {
    pub version: u16,
    pub pools: Vec<PoolStatus>,
    pub dont_save: bool,
}

impl PoolMeta {
    pub fn new(pools: &[Arc<Sets>], prev_meta: &PoolMeta) -> Self {
        let mut new_meta = Self {
            version: POOL_META_VERSION,
            pools: Vec::new(),
            ..Default::default()
        };

        for (idx, pool) in pools.iter().enumerate() {
            let mut skip = false;

            for current_pool in prev_meta.pools.iter() {
                if current_pool.cmd_line == pool.endpoints.cmd_line {
                    new_meta.pools.push(current_pool.clone());
                    skip = true;
                    break;
                }
            }

            if skip {
                continue;
            }

            new_meta.pools.push(PoolStatus {
                cmd_line: pool.endpoints.cmd_line.clone(),
                id: idx,
                last_update: OffsetDateTime::now_utc(),
                decommission: None,
            });
        }

        new_meta
    }

    pub fn is_suspended(&self, idx: usize) -> bool {
        if idx >= self.pools.len() {
            return false;
        }

        self.pools[idx].decommission.is_some()
    }

    pub async fn load(&mut self, pool: Arc<Sets>, _pools: Vec<Arc<Sets>>) -> Result<()> {
        let data = match read_config(pool, POOL_META_NAME).await {
            Ok(data) => {
                if data.is_empty() {
                    return Ok(());
                } else if data.len() <= 4 {
                    return Err(Error::other("poolMeta: no data"));
                }
                data
            }
            Err(err) => {
                if err == Error::ConfigNotFound {
                    return Ok(());
                }
                return Err(err);
            }
        };
        let format = LittleEndian::read_u16(&data[0..2]);
        if format != POOL_META_FORMAT {
            return Err(Error::other(format!("PoolMeta: unknown format: {format}")));
        }
        let version = LittleEndian::read_u16(&data[2..4]);
        if version != POOL_META_VERSION {
            return Err(Error::other(format!("PoolMeta: unknown version: {version}")));
        }

        let mut buf = Deserializer::new(Cursor::new(&data[4..]));
        let meta: PoolMeta = Deserialize::deserialize(&mut buf)?;
        *self = meta;

        if self.version != POOL_META_VERSION {
            return Err(Error::other(format!("unexpected PoolMeta version: {}", self.version)));
        }
        Ok(())
    }

    pub async fn save(&self, pools: Vec<Arc<Sets>>) -> Result<()> {
        if self.dont_save {
            return Ok(());
        }
        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(POOL_META_FORMAT)?;
        data.write_u16::<LittleEndian>(POOL_META_VERSION)?;
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf))?;
        data.write_all(&buf)?;

        for pool in pools {
            save_config(pool, POOL_META_NAME, data.clone()).await?;
        }

        Ok(())
    }

    pub fn decommission_cancel(&mut self, idx: usize) -> bool {
        if let Some(stats) = self.pools.get_mut(idx) {
            if let Some(d) = &stats.decommission {
                if !d.canceled {
                    stats.last_update = OffsetDateTime::now_utc();

                    let mut pd = d.clone();
                    pd.start_time = None;
                    pd.canceled = true;
                    pd.failed = false;
                    pd.complete = false;

                    stats.decommission = Some(pd);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }
    pub fn decommission_failed(&mut self, idx: usize) -> bool {
        if let Some(stats) = self.pools.get_mut(idx) {
            if let Some(d) = &stats.decommission {
                if !d.failed {
                    stats.last_update = OffsetDateTime::now_utc();

                    let mut pd = d.clone();
                    pd.start_time = None;
                    pd.canceled = false;
                    pd.failed = true;
                    pd.complete = false;

                    stats.decommission = Some(pd);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }
    pub fn decommission_complete(&mut self, idx: usize) -> bool {
        if let Some(stats) = self.pools.get_mut(idx) {
            if let Some(d) = &stats.decommission {
                if !d.complete {
                    stats.last_update = OffsetDateTime::now_utc();

                    let mut pd = d.clone();
                    pd.start_time = None;
                    pd.canceled = false;
                    pd.failed = false;
                    pd.complete = true;

                    stats.decommission = Some(pd);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }
    pub fn decommission(&mut self, idx: usize, pi: PoolSpaceInfo) -> Result<()> {
        if let Some(pool) = self.pools.get_mut(idx) {
            if let Some(ref info) = pool.decommission {
                if !info.complete && !info.failed && !info.canceled {
                    return Err(StorageError::DecommissionAlreadyRunning);
                }
            }

            let now = OffsetDateTime::now_utc();
            pool.last_update = now;
            pool.decommission = Some(PoolDecommissionInfo {
                start_time: Some(now),
                start_size: pi.free,
                total_size: pi.total,
                current_size: pi.free,
                ..Default::default()
            });
        }

        Ok(())
    }
    pub fn queue_buckets(&mut self, idx: usize, bks: Vec<DecomBucketInfo>) {
        for bk in bks.iter() {
            if let Some(dec) = self.pools[idx].decommission.as_mut() {
                dec.bucket_push(bk);
            }
        }
    }
    pub fn pending_buckets(&self, idx: usize) -> Vec<DecomBucketInfo> {
        let mut list = Vec::new();

        if let Some(pool) = self.pools.get(idx) {
            if let Some(ref info) = pool.decommission {
                for bk in info.queued_buckets.iter() {
                    let (name, prefix) = path2_bucket_object(bk);
                    list.push(DecomBucketInfo { name, prefix });
                }
            }
        }

        list
    }

    pub fn is_bucket_decommissioned(&self, idx: usize, bucket: String) -> bool {
        if let Some(ref info) = self.pools[idx].decommission {
            info.is_bucket_decommissioned(&bucket)
        } else {
            false
        }
    }

    pub fn bucket_done(&mut self, idx: usize, bucket: String) -> bool {
        if let Some(pool) = self.pools.get_mut(idx) {
            if let Some(info) = pool.decommission.as_mut() {
                info.bucket_pop(&bucket)
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn count_item(&mut self, idx: usize, size: usize, failed: bool) {
        if let Some(pool) = self.pools.get_mut(idx) {
            if let Some(info) = pool.decommission.as_mut() {
                if failed {
                    info.items_decommission_failed += 1;
                    info.bytes_failed += size;
                } else {
                    info.items_decommissioned += 1;
                    info.bytes_done += size;
                }
            }
        }
    }

    pub fn track_current_bucket_object(&mut self, idx: usize, bucket: String, object: String) {
        if self.pools.get(idx).is_none_or(|v| v.decommission.is_none()) {
            return;
        }

        if let Some(pool) = self.pools.get_mut(idx) {
            if let Some(info) = pool.decommission.as_mut() {
                info.object = object;
                info.bucket = bucket;
            }
        }
    }

    pub async fn update_after(&mut self, idx: usize, pools: Vec<Arc<Sets>>, duration: Duration) -> Result<bool> {
        if self.pools.get(idx).is_none_or(|v| v.decommission.is_none()) {
            return Err(Error::other("InvalidArgument"));
        }

        let now = OffsetDateTime::now_utc();

        if now.unix_timestamp() - self.pools[idx].last_update.unix_timestamp() > duration.whole_seconds() {
            self.pools[idx].last_update = now;
            self.save(pools).await?;

            return Ok(true);
        }

        Ok(false)
    }

    #[allow(dead_code)]
    pub fn validate(&self, pools: Vec<Arc<Sets>>) -> Result<bool> {
        struct PoolInfo {
            position: usize,
            completed: bool,
            decom_started: bool,
        }

        let mut remembered_pools = HashMap::new();
        for (idx, pool) in self.pools.iter().enumerate() {
            let mut complete = false;
            let mut decom_started = false;
            if let Some(decommission) = &pool.decommission {
                if decommission.complete {
                    complete = true;
                }
                decom_started = true;
            }
            remembered_pools.insert(
                pool.cmd_line.clone(),
                PoolInfo {
                    position: idx,
                    completed: complete,
                    decom_started,
                },
            );
        }

        let mut specified_pools = HashMap::new();
        for (idx, pool) in pools.iter().enumerate() {
            specified_pools.insert(pool.endpoints.cmd_line.clone(), idx);
        }

        let mut update = false;

        // 检查指定的池是否需要从已退役的池中移除。
        for k in specified_pools.keys() {
            if let Some(pi) = remembered_pools.get(k) {
                if pi.completed {
                    error!(
                        "pool({}) = {} is decommissioned, please remove from server command line",
                        pi.position + 1,
                        k
                    );
                    // return Err(Error::other(format!(
                    //     "pool({}) = {} is decommissioned, please remove from server command line",
                    //     pi.position + 1,
                    //     k
                    // )));
                }
            } else {
                // 如果之前记住的池不再存在，允许更新，因为可能是添加了一个新池。
                update = true;
            }
        }

        if specified_pools.len() == remembered_pools.len() {
            for (k, pi) in remembered_pools.iter() {
                if let Some(pos) = specified_pools.get(k) {
                    if *pos != pi.position {
                        update = true; // 池的顺序发生了变化，允许更新。
                    }
                }
            }
        }

        if !update {
            update = specified_pools.len() != remembered_pools.len();
        }

        Ok(update)
    }

    pub fn return_resumable_pools(&self) -> Vec<PoolStatus> {
        let mut new_pools = Vec::new();
        for pool in &self.pools {
            if let Some(decommission) = &pool.decommission {
                if decommission.complete || decommission.canceled {
                    // 不需要恢复的情况：
                    // - 退役已完成
                    // - 退役已取消
                    continue;
                }
                // 其他情况需要恢复
                new_pools.push(pool.clone());
            }
        }
        new_pools
    }
}

fn path2_bucket_object(name: &str) -> (String, String) {
    path2_bucket_object_with_base_path("", name)
}

fn path2_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    // Trim the base path and leading slash
    let trimmed_path = path
        .strip_prefix(base_path)
        .unwrap_or(path)
        .strip_prefix(SLASH_SEPARATOR)
        .unwrap_or(path);
    // Find the position of the first '/'
    let pos = trimmed_path.find(SLASH_SEPARATOR).unwrap_or(trimmed_path.len());
    // Split into bucket and prefix
    let bucket = &trimmed_path[0..pos];
    let prefix = &trimmed_path[pos + 1..]; // +1 to skip the '/' character if it exists

    (bucket.to_string(), prefix.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PoolDecommissionInfo {
    #[serde(rename = "startTime", with = "time::serde::rfc3339::option")]
    pub start_time: Option<OffsetDateTime>,
    #[serde(rename = "startSize")]
    pub start_size: usize,
    #[serde(rename = "totalSize")]
    pub total_size: usize,
    #[serde(rename = "currentSize")]
    pub current_size: usize,
    #[serde(rename = "complete")]
    pub complete: bool,
    #[serde(rename = "failed")]
    pub failed: bool,
    #[serde(rename = "canceled")]
    pub canceled: bool,

    #[serde(skip)]
    pub queued_buckets: Vec<String>,
    #[serde(skip)]
    pub decommissioned_buckets: Vec<String>,
    #[serde(skip)]
    pub bucket: String,
    #[serde(skip)]
    pub prefix: String,
    #[serde(skip)]
    pub object: String,

    #[serde(rename = "objectsDecommissioned")]
    pub items_decommissioned: usize,
    #[serde(rename = "objectsDecommissionedFailed")]
    pub items_decommission_failed: usize,
    #[serde(rename = "bytesDecommissioned")]
    pub bytes_done: usize,
    #[serde(rename = "bytesDecommissionedFailed")]
    pub bytes_failed: usize,
}

impl PoolDecommissionInfo {
    pub fn bucket_push(&mut self, bucket: &DecomBucketInfo) {
        for b in self.queued_buckets.iter() {
            if self.is_bucket_decommissioned(b) {
                return;
            }

            if b == &bucket.to_string() {
                return;
            }
        }

        self.queued_buckets.push(bucket.to_string());

        self.bucket = bucket.name.clone();
        self.prefix = bucket.prefix.clone();
    }
    pub fn is_bucket_decommissioned(&self, bucket: &String) -> bool {
        for b in self.decommissioned_buckets.iter() {
            if b == bucket {
                return true;
            }
        }
        false
    }
    pub fn bucket_pop(&mut self, bucket: &String) -> bool {
        self.decommissioned_buckets.push(bucket.clone());

        let mut found = None;
        for (i, b) in self.queued_buckets.iter().enumerate() {
            if b == bucket {
                found = Some(i);
                break;
            }
        }

        if let Some(i) = found {
            self.queued_buckets.remove(i);
            if &self.bucket == bucket {
                self.bucket = "".to_owned();
                self.prefix = "".to_owned();
                self.object = "".to_owned();
            }

            return true;
        }
        false
    }
}

#[derive(Debug)]
pub struct PoolSpaceInfo {
    pub free: usize,
    pub total: usize,
    pub used: usize,
}

#[derive(Debug, Default, Clone)]
pub struct DecomBucketInfo {
    pub name: String,
    pub prefix: String,
}

impl Display for DecomBucketInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            path_join(&[PathBuf::from(self.name.clone()), PathBuf::from(self.prefix.clone())]).to_string_lossy()
        )
    }
}

impl ECStore {
    pub async fn status(&self, idx: usize) -> Result<PoolStatus> {
        let space_info = self.get_decommission_pool_space_info(idx).await?;

        let pool_meta = self.pool_meta.read().await;

        let mut pool_info = pool_meta.pools[idx].clone();
        if let Some(d) = pool_info.decommission.as_mut() {
            d.total_size = space_info.total;
            d.current_size = space_info.free;
        } else {
            pool_info.decommission = Some(PoolDecommissionInfo {
                total_size: space_info.total,
                current_size: space_info.free,
                ..Default::default()
            });
        }

        Ok(pool_info)
    }

    async fn get_decommission_pool_space_info(&self, idx: usize) -> Result<PoolSpaceInfo> {
        if let Some(sets) = self.pools.get(idx) {
            let mut info = sets.storage_info().await;
            info.backend = self.backend_info().await;

            let total = get_total_usable_capacity(&info.disks, &info);
            let free = get_total_usable_capacity_free(&info.disks, &info);

            Ok(PoolSpaceInfo {
                free,
                total,
                used: total - free,
            })
        } else {
            Err(Error::other("InvalidArgument"))
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn decommission_cancel(&self, idx: usize) -> Result<()> {
        if self.single_pool() {
            return Err(Error::other("InvalidArgument"));
        }

        let Some(has_canceler) = self.decommission_cancelers.get(idx) else {
            return Err(Error::other("InvalidArgument"));
        };

        if has_canceler.is_none() {
            return Err(StorageError::DecommissionNotStarted);
        }

        let mut lock = self.pool_meta.write().await;
        if lock.decommission_cancel(idx) {
            lock.save(self.pools.clone()).await?;

            drop(lock);

            if let Some(notification_sys) = get_global_notification_sys() {
                notification_sys.reload_pool_meta().await;
            }
        }

        Ok(())
    }
    pub async fn is_decommission_running(&self) -> bool {
        let pool_meta = self.pool_meta.read().await;
        for pool in pool_meta.pools.iter() {
            if let Some(ref info) = pool.decommission {
                if !info.complete && !info.failed && !info.canceled {
                    return true;
                }
            }
        }

        false
    }

    #[tracing::instrument(skip(self, rx))]
    pub async fn decommission(&self, rx: CancellationToken, indices: Vec<usize>) -> Result<()> {
        warn!("decommission: {:?}", indices);
        if indices.is_empty() {
            return Err(Error::other("InvalidArgument"));
        }

        if self.single_pool() {
            return Err(Error::other("InvalidArgument"));
        }

        self.start_decommission(indices.clone()).await?;

        let rx_clone = rx.clone();
        tokio::spawn(async move {
            let Some(store) = new_object_layer_fn() else {
                error!("store not init");
                return;
            };
            for idx in indices.iter() {
                store.do_decommission_in_routine(rx_clone.clone(), *idx).await;
            }
        });

        Ok(())
    }

    #[allow(unused_assignments)]
    #[tracing::instrument(skip(self, set, wk, rcfg))]
    async fn decommission_entry(
        self: &Arc<Self>,
        idx: usize,
        entry: MetaCacheEntry,
        bucket: String,
        set: Arc<SetDisks>,
        wk: Arc<Workers>,
        rcfg: Option<String>,
    ) {
        warn!("decommission_entry: {} {}", &bucket, &entry.name);
        wk.give().await;
        if entry.is_dir() {
            warn!("decommission_entry: skip dir {}", &entry.name);
            return;
        }

        let mut fivs = match entry.file_info_versions(&bucket) {
            Ok(f) => f,
            Err(err) => {
                error!("decommission_pool: file_info_versions err {:?}", &err);
                return;
            }
        };

        fivs.versions.sort_by(|a, b| b.mod_time.cmp(&a.mod_time));

        let mut decommissioned: usize = 0;
        let expired: usize = 0;

        for version in fivs.versions.iter() {
            // TODO: filterLifecycle
            let remaining_versions = fivs.versions.len() - expired;
            if version.deleted && remaining_versions == 1 && rcfg.is_none() {
                //
                decommissioned += 1;
                info!("decommission_pool: DELETE marked object with no other non-current versions will be skipped");
                continue;
            }

            let version_id = version.version_id.map(|v| v.to_string());

            let mut ignore = false;
            let mut failure = false;
            let mut error = None;
            if version.deleted {
                // TODO: other params
                if let Err(err) = self
                    .delete_object(
                        bucket.as_str(),
                        &version.name,
                        ObjectOptions {
                            versioned: true,
                            version_id: version_id.clone(),
                            mod_time: version.mod_time,
                            src_pool_idx: idx,
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
                        continue;
                    }

                    failure = true;

                    error = Some(err)
                }

                {
                    self.pool_meta.write().await.count_item(idx, 0, failure);
                }

                if !failure {
                    decommissioned += 1;
                }

                info!(
                    "decommission_pool: DecomCopyDeleteMarker  {} {} {:?} {:?}",
                    &bucket, &version.name, &version_id, error
                );
                continue;
            }

            for _i in 0..3 {
                if version.is_remote() {
                    // TODO: DecomTieredObject
                }

                let bucket = bucket.clone();

                let rd = match set
                    .get_object_reader(
                        bucket.as_str(),
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
                            ignore = true;
                            break;
                        }

                        if !ignore {
                            //
                            if bucket == RUSTFS_META_BUCKET && version.name.contains(DATA_USAGE_CACHE_NAME) {
                                ignore = true;
                                error!("decommission_pool: ignore data usage cache {}", &version.name);
                                break;
                            }
                        }

                        failure = true;
                        error!("decommission_pool: get_object_reader err {:?}", &err);
                        continue;
                    }
                };

                let bucket_name = bucket.clone();
                let object_name = rd.object_info.name.clone();

                if let Err(err) = self.clone().decommission_object(idx, bucket, rd).await {
                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err) {
                        ignore = true;
                        break;
                    }

                    failure = true;

                    error!("decommission_pool: decommission_object err {:?}", &err);
                    continue;
                }

                warn!(
                    "decommission_pool: decommission_object done {}/{} {}",
                    &bucket_name, &object_name, &version.name
                );

                failure = false;
                break;
            }

            if ignore {
                info!("decommission_pool: ignore {}", &version.name);
                continue;
            }

            {
                self.pool_meta.write().await.count_item(idx, decommissioned, failure);
            }

            if failure {
                break;
            }

            decommissioned += 1;
        }

        if decommissioned == fivs.versions.len() {
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
                error!("decommission_pool: delete_object err {:?}", &err);
            }
        }

        {
            let mut pool_meta = self.pool_meta.write().await;

            pool_meta.track_current_bucket_object(idx, bucket.clone(), entry.name.clone());

            let ok = pool_meta
                .update_after(idx, self.pools.clone(), Duration::seconds(30))
                .await
                .unwrap_or_default();

            drop(pool_meta);
            if ok {
                if let Some(notification_sys) = get_global_notification_sys() {
                    notification_sys.reload_pool_meta().await;
                }
            }
        }

        warn!("decommission_pool: decommission_entry done {} {}", &bucket, &entry.name);
    }

    #[tracing::instrument(skip(self, rx))]
    async fn decommission_pool(
        self: &Arc<Self>,
        rx: CancellationToken,
        idx: usize,
        pool: Arc<Sets>,
        bi: DecomBucketInfo,
    ) -> Result<()> {
        let wk = Workers::new(pool.disk_set.len() * 2).map_err(Error::other)?;

        // let mut vc = None;
        // replication
        let rcfg: Option<String> = None;

        if bi.name != RUSTFS_META_BUCKET {
            let _versioning = BucketVersioningSys::get(&bi.name).await?;
            // vc = Some(versioning);
            // TODO: LifecycleSys
            // TODO: BucketObjectLockSys
            // TODO: ReplicationConfig
        }

        for (set_idx, set) in pool.disk_set.iter().enumerate() {
            wk.clone().take().await;

            warn!("decommission_pool: decommission_pool {} {}", set_idx, &bi.name);

            let decommission_entry: ListCallback = Arc::new({
                let this = Arc::clone(self);
                let bucket = bi.name.clone();
                let wk = wk.clone();
                let set = set.clone();
                let rcfg = rcfg.clone();
                move |entry: MetaCacheEntry| {
                    let this = this.clone();
                    let bucket = bucket.clone();
                    let wk = wk.clone();
                    let set = set.clone();
                    let rcfg = rcfg.clone();

                    Box::pin(async move {
                        wk.take().await;
                        this.decommission_entry(idx, entry, bucket, set, wk, rcfg).await
                    })
                }
            });

            let set = set.clone();
            let rx_clone = rx.clone();
            let bi = bi.clone();
            let set_id = set_idx;
            let wk_clone = wk.clone();
            tokio::spawn(async move {
                loop {
                    if rx_clone.is_cancelled() {
                        warn!("decommission_pool: cancel {}", set_id);
                        break;
                    }
                    warn!("decommission_pool: list_objects_to_decommission {} {}", set_id, &bi.name);

                    match set
                        .list_objects_to_decommission(rx_clone.clone(), bi.clone(), decommission_entry.clone())
                        .await
                    {
                        Ok(_) => {
                            warn!("decommission_pool: list_objects_to_decommission {} done", set_id);
                            break;
                        }
                        Err(err) => {
                            error!("decommission_pool: list_objects_to_decommission {} err {:?}", set_id, &err);
                            if is_err_bucket_not_found(&err) {
                                warn!("decommission_pool: list_objects_to_decommission {} volume not found", set_id);
                                break;
                            }

                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        }
                    }
                }

                wk_clone.give().await;
            });
        }

        warn!("decommission_pool: decommission_pool wait {} {}", idx, &bi.name);

        wk.wait().await;

        warn!("decommission_pool: decommission_pool done {} {}", idx, &bi.name);

        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    pub async fn do_decommission_in_routine(self: &Arc<Self>, rx: CancellationToken, idx: usize) {
        if let Err(err) = self.decommission_in_background(rx, idx).await {
            error!("decom err {:?}", &err);
            if let Err(er) = self.decommission_failed(idx).await {
                error!("decom failed err {:?}", &er);
            } else {
                warn!("decommission: decommission_failed  {}", idx);
            }

            return;
        }

        warn!("decommission: decommission_in_background complete {}", idx);

        let (failed, cmd_line) = {
            let pool_meta = self.pool_meta.read().await;
            let failed = {
                if let Some(info) = &pool_meta.pools[idx].decommission {
                    info.items_decommission_failed > 0
                } else {
                    false
                }
            };
            let cmd_line = pool_meta.pools[idx].cmd_line.clone();
            (failed, cmd_line)
        };

        if !failed {
            warn!("Decommissioning complete for pool {}, verifying for any pending objects", cmd_line);
            if let Err(er) = self.decommission_failed(idx).await {
                error!("decom failed err {:?}", &er);
            }
        } else if let Err(er) = self.complete_decommission(idx).await {
            error!("decom complete err {:?}", &er);
        }

        warn!("Decommissioning complete for pool {}", cmd_line);
    }

    #[tracing::instrument(skip(self))]
    pub async fn decommission_failed(&self, idx: usize) -> Result<()> {
        if self.single_pool() {
            return Err(Error::other("errInvalidArgument"));
        }

        let mut pool_meta = self.pool_meta.write().await;
        if pool_meta.decommission_failed(idx) {
            pool_meta.save(self.pools.clone()).await?;

            drop(pool_meta);

            if let Some(notification_sys) = get_global_notification_sys() {
                notification_sys.reload_pool_meta().await;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn complete_decommission(&self, idx: usize) -> Result<()> {
        if self.single_pool() {
            return Err(Error::other("errInvalidArgument"));
        }

        let mut pool_meta = self.pool_meta.write().await;
        if pool_meta.decommission_complete(idx) {
            pool_meta.save(self.pools.clone()).await?;
            drop(pool_meta);
            if let Some(notification_sys) = get_global_notification_sys() {
                notification_sys.reload_pool_meta().await;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    async fn decommission_in_background(self: &Arc<Self>, rx: CancellationToken, idx: usize) -> Result<()> {
        let pool = self.pools[idx].clone();

        let pending = {
            let pool_meta = self.pool_meta.read().await;
            pool_meta.pending_buckets(idx)
        };

        for bucket in pending.iter() {
            let is_decommissioned = {
                let pool_meta = self.pool_meta.read().await;
                pool_meta.is_bucket_decommissioned(idx, bucket.to_string())
            };

            if is_decommissioned {
                warn!("decommission: already done, moving on {}", bucket.to_string());

                {
                    let mut pool_meta = self.pool_meta.write().await;
                    if pool_meta.bucket_done(idx, bucket.to_string()) {
                        if let Err(err) = pool_meta.save(self.pools.clone()).await {
                            error!("decom pool_meta.save err {:?}", err);
                        }
                    }
                }
                continue;
            }

            warn!("decommission: currently on bucket {}", &bucket.name);

            if let Err(err) = self.decommission_pool(rx.clone(), idx, pool.clone(), bucket.clone()).await {
                error!("decommission: decommission_pool err {:?}", &err);
                return Err(err);
            } else {
                warn!("decommission: decommission_pool done {}", &bucket.name);
            }

            {
                let mut pool_meta = self.pool_meta.write().await;
                if pool_meta.bucket_done(idx, bucket.to_string()) {
                    if let Err(err) = pool_meta.save(self.pools.clone()).await {
                        error!("decom pool_meta.save err {:?}", err);
                    }
                }

                warn!("decommission: decommission_pool bucket_done {}", &bucket.name);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn start_decommission(&self, indices: Vec<usize>) -> Result<()> {
        if indices.is_empty() {
            return Err(Error::other("errInvalidArgument"));
        }

        if self.single_pool() {
            return Err(Error::other("errInvalidArgument"));
        }

        let decom_buckets = self.get_buckets_to_decommission().await?;

        for bk in decom_buckets.iter() {
            let _ = self.heal_bucket(&bk.name, &HealOpts::default()).await;
        }

        let meta_buckets = [
            path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(CONFIG_PREFIX)]),
            path_join(&[PathBuf::from(RUSTFS_META_BUCKET), PathBuf::from(BUCKET_META_PREFIX)]),
        ];

        for bk in meta_buckets.iter() {
            if let Err(err) = self
                .make_bucket(bk.to_string_lossy().to_string().as_str(), &MakeBucketOptions::default())
                .await
            {
                if !is_err_bucket_exists(&err) {
                    return Err(err);
                }
            }
        }

        let mut pool_meta = self.pool_meta.write().await;
        for idx in indices.iter() {
            let pi = self.get_decommission_pool_space_info(*idx).await?;

            pool_meta.decommission(*idx, pi)?;

            pool_meta.queue_buckets(*idx, decom_buckets.clone());
        }

        pool_meta.save(self.pools.clone()).await?;

        if let Some(notification_sys) = get_global_notification_sys() {
            notification_sys.reload_pool_meta().await;
        }

        Ok(())
    }

    async fn get_buckets_to_decommission(&self) -> Result<Vec<DecomBucketInfo>> {
        let buckets = self.list_bucket(&BucketOptions::default()).await?;

        let mut ret: Vec<DecomBucketInfo> = buckets
            .iter()
            .map(|v| DecomBucketInfo {
                name: v.name.clone(),
                ..Default::default()
            })
            .collect();

        ret.push(DecomBucketInfo {
            name: RUSTFS_META_BUCKET.to_owned(),
            prefix: CONFIG_PREFIX.to_owned(),
        });
        ret.push(DecomBucketInfo {
            name: RUSTFS_META_BUCKET.to_owned(),
            prefix: BUCKET_META_PREFIX.to_owned(),
        });

        Ok(ret)
    }

    #[tracing::instrument(skip(self, rd))]
    async fn decommission_object(self: Arc<Self>, pool_idx: usize, bucket: String, rd: GetObjectReader) -> Result<()> {
        warn!("decommission_object: start {} {}", &bucket, &rd.object_info.name);
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
                    error!("decommission_object: new_multipart_upload err {:?}", &err);
                    return Err(err);
                }
            };

            defer!(|| async {
                if let Err(err) = self
                    .abort_multipart_upload(&bucket, &object_info.name, &res.upload_id, &ObjectOptions::default())
                    .await
                {
                    error!("decommission_object: abort_multipart_upload err {:?}", &err);
                }
            });

            let mut parts = vec![CompletePart::default(); object_info.parts.len()];

            let mut reader = rd.stream;

            for (i, part) in object_info.parts.iter().enumerate() {
                let mut chunk = vec![0u8; part.size];

                reader.read_exact(&mut chunk).await?;

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
                        error!("decommission_object: put_object_part {} err {:?}", i, &err);
                        return Err(err);
                    }
                };

                warn!("decommission_object: put_object_part {} done {} {}", i, &bucket, &object_info.name);

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
                error!("decommission_object: complete_multipart_upload err {:?}", &err);
                return Err(err);
            }

            warn!("decommission_object: complete_multipart_upload done {} {}", &bucket, &object_info.name);
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
            error!("decommission_object: put_object err {:?}", &err);
            return Err(err);
        }

        warn!("decommission_object: put_object done {} {}", &bucket, &object_info.name);
        Ok(())
    }
}

// impl Fn(MetaCacheEntry) -> impl Future<Output = Result<(), Error>>

pub type ListCallback = Arc<dyn Fn(MetaCacheEntry) -> BoxFuture<'static, ()> + Send + Sync + 'static>;

impl SetDisks {
    #[tracing::instrument(skip(self, rx, cb_func))]
    async fn list_objects_to_decommission(
        self: &Arc<Self>,
        rx: CancellationToken,
        bucket_info: DecomBucketInfo,
        cb_func: ListCallback,
    ) -> Result<()> {
        let (disks, _) = self.get_online_disks_with_healing(false).await;
        if disks.is_empty() {
            return Err(Error::other("errNoDiskAvailable"));
        }

        let listing_quorum = self.set_drive_count.div_ceil(2);

        let resolver = MetadataResolutionParams {
            dir_quorum: listing_quorum,
            obj_quorum: listing_quorum,
            bucket: bucket_info.name.clone(),
            ..Default::default()
        };

        let cb1 = cb_func.clone();

        list_path_raw(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                bucket: bucket_info.name.clone(),
                path: bucket_info.prefix.clone(),
                recursive: true,
                min_disks: listing_quorum,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| Box::pin(cb1(entry)))),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                    let resolver = resolver.clone();
                    let cb_func = cb_func.clone();
                    match entries.resolve(resolver) {
                        Some(entry) => {
                            warn!("decommission_pool: list_objects_to_decommission get {}", &entry.name);
                            Box::pin(async move {
                                cb_func(entry).await;
                            })
                        }
                        None => {
                            warn!("decommission_pool: list_objects_to_decommission get none");
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

pub fn get_total_usable_capacity(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    let mut capacity = 0;
    for disk in disks.iter() {
        if disk.pool_index < 0 || info.backend.standard_sc_data.len() <= disk.pool_index as usize {
            continue;
        }
        if (disk.disk_index as usize) < info.backend.standard_sc_data[disk.pool_index as usize] {
            capacity += disk.total_space as usize;
        }
    }
    capacity
}

pub fn get_total_usable_capacity_free(disks: &[rustfs_madmin::Disk], info: &rustfs_madmin::StorageInfo) -> usize {
    let mut capacity = 0;
    for disk in disks.iter() {
        if disk.pool_index < 0 || info.backend.standard_sc_data.len() <= disk.pool_index as usize {
            continue;
        }
        if (disk.disk_index as usize) < info.backend.standard_sc_data[disk.pool_index as usize] {
            capacity += disk.available_space as usize;
        }
    }
    capacity
}
