use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::cache_value::metacache_set::{list_path_raw, ListPathRawOptions};
use crate::config::com::{read_config, save_config, CONFIG_PREFIX};
use crate::config::error::ConfigError;
use crate::disk::{MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams, BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::heal::data_usage::DATA_USAGE_CACHE_NAME;
use crate::heal::heal_commands::HealOpts;
use crate::new_object_layer_fn;
use crate::notification_sys::get_global_notification_sys;
use crate::set_disk::SetDisks;
use crate::store_api::{
    BucketOptions, CompletePart, GetObjectReader, MakeBucketOptions, ObjectIO, ObjectOptions, PutObjReader, StorageAPI,
};
use crate::store_err::{
    is_err_bucket_exists, is_err_data_movement_overwrite, is_err_object_not_found, is_err_version_not_found, StorageError,
};
use crate::utils::path::{encode_dir_object, path_join, SLASH_SEPARATOR};
use crate::{sets::Sets, store::ECStore};
use ::workers::workers::Workers;
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use common::defer;
use common::error::{Error, Result};
use futures::future::BoxFuture;
use http::HeaderMap;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::sync::broadcast::Receiver as B_Receiver;
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
                    return Err(Error::from_string("poolMeta: no data"));
                }
                data
            }
            Err(err) => {
                if let Some(ConfigError::NotFound) = err.downcast_ref::<ConfigError>() {
                    return Ok(());
                }
                return Err(err);
            }
        };
        let format = LittleEndian::read_u16(&data[0..2]);
        if format != POOL_META_FORMAT {
            return Err(Error::msg(format!("PoolMeta: unknown format: {}", format)));
        }
        let version = LittleEndian::read_u16(&data[2..4]);
        if version != POOL_META_VERSION {
            return Err(Error::msg(format!("PoolMeta: unknown version: {}", version)));
        }

        let mut buf = Deserializer::new(Cursor::new(&data[4..]));
        let meta: PoolMeta = Deserialize::deserialize(&mut buf).unwrap();
        *self = meta;

        if self.version != POOL_META_VERSION {
            return Err(Error::msg(format!("unexpected PoolMeta version: {}", self.version)));
        }
        Ok(())
    }

    pub async fn save(&self, pools: Vec<Arc<Sets>>) -> Result<()> {
        if self.dont_save {
            return Ok(());
        }
        let mut data = Vec::new();
        data.write_u16::<LittleEndian>(POOL_META_FORMAT).unwrap();
        data.write_u16::<LittleEndian>(POOL_META_VERSION).unwrap();
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
                    return Err(Error::new(StorageError::DecommissionAlreadyRunning));
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
        if !self.pools.get(idx).is_some_and(|v| v.decommission.is_some()) {
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
        if !self.pools.get(idx).is_some_and(|v| v.decommission.is_some()) {
            return Err(Error::msg("InvalidArgument"));
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
                    // return Err(Error::msg(format!(
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
            Err(Error::msg("InvalidArgument"))
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn decommission_cancel(&self, idx: usize) -> Result<()> {
        if self.single_pool() {
            return Err(Error::msg("InvalidArgument"));
        }

        let Some(has_canceler) = self.decommission_cancelers.get(idx) else {
            return Err(Error::msg("InvalidArgument"));
        };

        if has_canceler.is_none() {
            return Err(Error::new(StorageError::DecommissionNotStarted));
        }

        let mut lock = self.pool_meta.write().await;
        if lock.decommission_cancel(idx) {
            lock.save(self.pools.clone()).await?;
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
    pub async fn decommission(&self, rx: B_Receiver<bool>, indices: Vec<usize>) -> Result<()> {
        if indices.is_empty() {
            return Err(Error::msg("errInvalidArgument"));
        }

        if self.single_pool() {
            return Err(Error::msg("errInvalidArgument"));
        }

        self.start_decommission(indices.clone()).await?;

        tokio::spawn(async move {
            let Some(store) = new_object_layer_fn() else {
                error!("store not init");
                return;
            };
            for idx in indices.iter() {
                store.do_decommission_in_routine(rx.resubscribe(), *idx).await;
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
        wk.give().await;
        if entry.is_dir() {
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

                if let Err(err) = self.clone().decommission_object(idx, bucket, rd).await {
                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) || is_err_data_movement_overwrite(&err) {
                        ignore = true;
                        break;
                    }

                    failure = true;

                    error!("decommission_pool: decommission_object err {:?}", &err);
                    continue;
                }

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
            if ok {
                // TODO: ReloadPoolMeta
            }
        }
    }

    #[tracing::instrument(skip(self, rx))]
    async fn decommission_pool(
        self: &Arc<Self>,
        rx: B_Receiver<bool>,
        idx: usize,
        pool: Arc<Sets>,
        bi: DecomBucketInfo,
    ) -> Result<()> {
        let wk = Workers::new(pool.disk_set.len() * 2).map_err(|v| Error::from_string(v))?;

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
                    Box::pin(async move { this.decommission_entry(idx, entry, bucket, set, wk, rcfg).await })
                }
            });

            let set = set.clone();
            let mut rx = rx.resubscribe();
            let bi = bi.clone();
            let set_id = set_idx;
            tokio::spawn(async move {
                loop {
                    if rx.try_recv().is_ok() {
                        break;
                    }
                    if let Err(err) = set
                        .list_objects_to_decommission(rx.resubscribe(), bi.clone(), decommission_entry.clone())
                        .await
                    {
                        error!("decommission_pool: list_objects_to_decommission {} err {:?}", set_id, &err);
                    }
                }
            });
        }

        wk.wait().await;

        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    pub async fn do_decommission_in_routine(self: &Arc<Self>, rx: B_Receiver<bool>, idx: usize) {
        if let Err(err) = self.decommission_in_background(rx, idx).await {
            error!("decom err {:?}", &err);
            if let Err(er) = self.decommission_failed(idx).await {
                error!("decom failed err {:?}", &er);
            }

            return;
        }

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
    }

    #[tracing::instrument(skip(self))]
    pub async fn decommission_failed(&self, idx: usize) -> Result<()> {
        if self.single_pool() {
            return Err(Error::msg("errInvalidArgument"));
        }

        let mut pool_meta = self.pool_meta.write().await;
        if pool_meta.decommission_failed(idx) {
            pool_meta.save(self.pools.clone()).await?;
            if let Some(notification_sys) = get_global_notification_sys() {
                notification_sys.reload_pool_meta().await;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn complete_decommission(&self, idx: usize) -> Result<()> {
        if self.single_pool() {
            return Err(Error::msg("errInvalidArgument"));
        }

        let mut pool_meta = self.pool_meta.write().await;
        if pool_meta.decommission_complete(idx) {
            pool_meta.save(self.pools.clone()).await?;
            if let Some(notification_sys) = get_global_notification_sys() {
                notification_sys.reload_pool_meta().await;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, rx))]
    async fn decommission_in_background(self: &Arc<Self>, rx: B_Receiver<bool>, idx: usize) -> Result<()> {
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
                info!("decommission: already done, moving on {}", bucket.to_string());

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

            info!("decommission: currently on bucket {}", &bucket.name);

            if let Err(err) = self
                .decommission_pool(rx.resubscribe(), idx, pool.clone(), bucket.clone())
                .await
            {
                error!("decommission: decommission_pool err {:?}", &err);
                return Err(err);
            }

            {
                let mut pool_meta = self.pool_meta.write().await;
                if pool_meta.bucket_done(idx, bucket.to_string()) {
                    if let Err(err) = pool_meta.save(self.pools.clone()).await {
                        error!("decom pool_meta.save err {:?}", err);
                    }
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn start_decommission(&self, indices: Vec<usize>) -> Result<()> {
        if indices.is_empty() {
            return Err(Error::msg("errInvalidArgument"));
        }

        if self.single_pool() {
            return Err(Error::msg("errInvalidArgument"));
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

            // TODO: defer abort_multipart_upload

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
                // 每次从reader中读取一个part上传

                let mut data = PutObjReader::new(reader, part.size);

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
                        error!("decommission_object: put_object_part err {:?}", &err);
                        return Err(err);
                    }
                };

                parts[i] = CompletePart {
                    part_num: pi.part_num,
                    e_tag: pi.etag,
                };

                // 把reader所有权拿回来?
                reader = data.stream;
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
                error!("decommission_object: complete_multipart_upload err {:?}", &err);
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
            error!("decommission_object: put_object err {:?}", &err);
            return Err(err);
        }

        Ok(())
    }
}

// impl Fn(MetaCacheEntry) -> impl Future<Output = Result<(), Error>>

pub type ListCallback = Arc<dyn Fn(MetaCacheEntry) -> BoxFuture<'static, ()> + Send + Sync + 'static>;

impl SetDisks {
    #[tracing::instrument(skip(self, rx, cb_func))]
    async fn list_objects_to_decommission(
        self: &Arc<Self>,
        rx: B_Receiver<bool>,
        bucket_info: DecomBucketInfo,
        cb_func: ListCallback,
    ) -> Result<()> {
        let (disks, _) = self.get_online_disks_with_healing(false).await;
        if disks.is_empty() {
            return Err(Error::msg("errNoDiskAvailable"));
        }

        let listing_quorum = self.set_drive_count.div_ceil(2);

        let resolver = MetadataResolutionParams {
            dir_quorum: listing_quorum,
            obj_quorum: listing_quorum,
            bucket: bucket_info.name.clone(),
            ..Default::default()
        };

        list_path_raw(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                bucket: bucket_info.name.clone(),
                path: bucket_info.prefix.clone(),
                recursice: true,
                min_disks: listing_quorum,
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
                    let resolver = resolver.clone();
                    if let Ok(Some(entry)) = entries.resolve(resolver) {
                        cb_func(entry)
                    } else {
                        Box::pin(async {})
                    }
                })),
                ..Default::default()
            },
        )
        .await?;

        Ok(())
    }
}

fn get_total_usable_capacity(disks: &[madmin::Disk], info: &madmin::StorageInfo) -> usize {
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

fn get_total_usable_capacity_free(disks: &[madmin::Disk], info: &madmin::StorageInfo) -> usize {
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
