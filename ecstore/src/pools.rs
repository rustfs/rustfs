use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::config::common::{read_config, save_config, CONFIG_PREFIX};
use crate::config::error::ConfigError;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::heal::heal_commands::HealOpts;
use crate::new_object_layer_fn;
use crate::store_api::{BucketOptions, MakeBucketOptions, StorageAPI, StorageDisk, StorageInfo};
use crate::store_err::{is_err_bucket_exists, StorageError};
use crate::utils::path::{path_join, SLASH_SEPARATOR};
use crate::{sets::Sets, store::ECStore};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{error, info, warn};

pub const POOL_META_NAME: &str = "pool.bin";
pub const POOL_META_FORMAT: u16 = 1;
pub const POOL_META_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatus {
    pub id: usize,
    pub cmd_line: String,
    pub last_update: OffsetDateTime,
    pub decommission: Option<PoolDecommissionInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolMeta {
    pub version: u16,
    pub pools: Vec<PoolStatus>,
    pub dont_save: bool,
}

impl PoolMeta {
    pub fn new(pools: Vec<Arc<Sets>>) -> Self {
        let mut status = Vec::with_capacity(pools.len());
        for (idx, pool) in pools.iter().enumerate() {
            status.push(PoolStatus {
                id: idx,
                cmd_line: pool.endpoints.cmd_line.clone(),
                last_update: OffsetDateTime::now_utc(),
                decommission: None,
            });
        }

        Self {
            version: POOL_META_VERSION,
            pools: status,
            dont_save: false,
        }
    }

    pub fn is_suspended(&self, idx: usize) -> bool {
        if idx >= self.pools.len() {
            return false;
        }

        self.pools[idx].decommission.is_some()
    }

    pub async fn load(&mut self) -> Result<()> {
        let Some(store) = new_object_layer_fn() else { return Err(Error::msg("errServerNotInitialized")) };

        let data = match read_config(store.clone(), POOL_META_NAME).await {
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
            save_config(pool, POOL_META_NAME, &data).await?;
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
                    return Err(Error::msg("DecommissionAlreadyRunning"));
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
}

fn path2_bucket_object(name: &String) -> (String, String) {
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
    pub start_time: Option<OffsetDateTime>,
    pub start_size: usize,
    pub total_size: usize,
    pub current_size: usize,
    pub complete: bool,
    pub failed: bool,
    pub canceled: bool,
    pub queued_buckets: Vec<String>,
    pub decommissioned_buckets: Vec<String>,
    pub bucket: String,
    pub prefix: String,
    pub object: String,

    pub items_decommissioned: usize,
    pub items_decommission_failed: usize,
    pub bytes_done: usize,
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

impl ToString for DecomBucketInfo {
    fn to_string(&self) -> String {
        path_join(&[PathBuf::from(self.name.clone()), PathBuf::from(self.prefix.clone())])
            .to_string_lossy()
            .to_string()
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

    pub async fn decommission(&self, indices: Vec<usize>) -> Result<()> {
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
                store.do_decommission_in_routine(*idx).await;
            }
        });

        Ok(())
    }

    async fn decommission_pool<S: StorageAPI>(&self, _idx: usize, _pool: Arc<S>, bi: DecomBucketInfo) -> Result<()> {
        let mut _vc = None;

        if bi.name == RUSTFS_META_BUCKET {
            let versioning = BucketVersioningSys::get(&bi.name).await?;
            _vc = Some(versioning);
        }

        // FIXME:
        unimplemented!()
    }

    async fn do_decommission_in_routine(&self, idx: usize) {
        if let Err(err) = self.decommission_in_background(idx).await {
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

    pub async fn decommission_failed(&self, idx: usize) -> Result<()> {
        if self.single_pool() {
            return Err(Error::msg("errInvalidArgument"));
        }

        let mut pool_meta = self.pool_meta.write().await;
        if pool_meta.decommission_failed(idx) {
            pool_meta.save(self.pools.clone()).await?;
            // FIXME: globalNotificationSys.ReloadPoolMeta(ctx)
        }

        Ok(())
    }

    pub async fn complete_decommission(&self, idx: usize) -> Result<()> {
        if self.single_pool() {
            return Err(Error::msg("errInvalidArgument"));
        }

        let mut pool_meta = self.pool_meta.write().await;
        if pool_meta.decommission_complete(idx) {
            pool_meta.save(self.pools.clone()).await?;
            // FIXME: globalNotificationSys.ReloadPoolMeta(ctx)
        }

        Ok(())
    }

    async fn decommission_in_background(&self, idx: usize) -> Result<()> {
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

            if let Err(err) = self.decommission_pool(idx, pool.clone(), bucket.clone()).await {
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

        // FIXME: globalNotificationSys.ReloadPoolMeta(ctx)

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
}

fn get_total_usable_capacity(disks: &[StorageDisk], info: &StorageInfo) -> usize {
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

fn get_total_usable_capacity_free(disks: &[StorageDisk], info: &StorageInfo) -> usize {
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

#[test]
fn test_pool_meta() -> Result<()> {
    let meta = PoolMeta::new(vec![]);
    let mut data = Vec::new();
    data.write_u16::<LittleEndian>(POOL_META_FORMAT).unwrap();
    data.write_u16::<LittleEndian>(POOL_META_VERSION).unwrap();
    let mut buf = Vec::new();
    meta.serialize(&mut Serializer::new(&mut buf))?;
    data.write_all(&buf)?;

    let format = LittleEndian::read_u16(&data[0..2]);
    if format != POOL_META_FORMAT {
        return Err(Error::msg(format!("PoolMeta: unknown format: {}", format)));
    }
    let version = LittleEndian::read_u16(&data[2..4]);
    if version != POOL_META_VERSION {
        return Err(Error::msg(format!("PoolMeta: unknown version: {}", version)));
    }

    let mut buf = Deserializer::new(Cursor::new(&data[4..]));
    let de_meta: PoolMeta = Deserialize::deserialize(&mut buf).unwrap();

    if de_meta.version != POOL_META_VERSION {
        return Err(Error::msg(format!("unexpected PoolMeta version: {}", de_meta.version)));
    }

    println!("meta: {:?}", de_meta);
    Ok(())
}
