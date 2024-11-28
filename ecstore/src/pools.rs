use crate::config::common::{read_config, save_config, CONFIG_PREFIX};
use crate::config::error::ConfigError;
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::heal::heal_commands::HealOpts;
use crate::new_object_layer_fn;
use crate::store_api::{BucketOptions, MakeBucketOptions, StorageAPI, StorageDisk, StorageInfo};
use crate::store_err::{is_err_bucket_exists, StorageError};
use crate::utils::path::path_join;
use crate::{sets::Sets, store::ECStore};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::error;

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
            save_config(pool, &POOL_META_NAME, &data).await?;
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
    pub fn decommission(&mut self, idx: usize, pi: PoolSpaceInfo) -> Result<()> {
        if let Some(pool) = self.pools.get_mut(idx) {
            if let Some(ref info) = pool.decommission {
                if !info.complete && !info.failed && !info.canceled {
                    return Err(Error::msg("DecommissionAlreadyRunning"));
                }
            }

            let now = OffsetDateTime::now_utc();
            pool.last_update = now.clone();
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
                store.do_decommission_in_routine(idx.clone()).await;
            }
        });

        Ok(())
    }

    async fn do_decommission_in_routine(&self, _idx: usize) {
        // FIXME:
        unimplemented!()
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

        let meta_buckets = vec![
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
            let pi = self.get_decommission_pool_space_info(idx.clone()).await?;

            pool_meta.decommission(idx.clone(), pi)?;

            pool_meta.queue_buckets(idx.clone(), decom_buckets.clone());
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

fn get_total_usable_capacity(disks: &Vec<StorageDisk>, info: &StorageInfo) -> usize {
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

fn get_total_usable_capacity_free(disks: &Vec<StorageDisk>, info: &StorageInfo) -> usize {
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
