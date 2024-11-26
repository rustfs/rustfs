use crate::error::{Error, Result};
use crate::store_api::{StorageAPI, StorageDisk, StorageInfo};
use crate::store_err::StorageError;
use crate::{sets::Sets, store::ECStore};
use serde::Serialize;
use std::sync::Arc;
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize)]
pub struct PoolStatus {
    pub id: usize,
    pub cmd_line: String,
    pub last_update: OffsetDateTime,
    pub decommission: Option<PoolDecommissionInfo>,
}

#[derive(Debug, Clone)]
pub struct PoolMeta {
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
}

#[derive(Debug, Clone, Serialize, Default)]
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

#[derive(Debug)]
pub struct PoolSpaceInfo {
    pub free: usize,
    pub total: usize,
    pub used: usize,
}

impl ECStore {
    pub async fn status(&self, idx: usize) -> Result<PoolStatus> {
        let space_info = self.get_decommission_pool_space_info(idx).await?;
        let mut pool_info = self.pool_meta.pools[idx].clone();
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

    pub async fn decommission_cancel(&mut self, idx: usize) -> Result<()> {
        if self.single_pool() {
            return Err(Error::msg("InvalidArgument"));
        }

        let Some(has_canceler) = self.decommission_cancelers.get(idx) else {
            return Err(Error::msg("InvalidArgument"));
        };

        if has_canceler.is_none() {
            return Err(Error::new(StorageError::DecommissionNotStarted));
        }

        if self.pool_meta.decommission_cancel(idx) {
            // FIXME:
        }

        unimplemented!()
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
