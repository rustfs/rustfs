use crate::error::{Error, Result};
use crate::store_api::{StorageAPI, StorageDisk, StorageInfo};
use crate::{sets::Sets, store::ECStore};
use std::sync::Arc;
use time::OffsetDateTime;

#[derive(Debug, Clone)]
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
}

#[derive(Debug, Clone)]
pub struct PoolDecommissionInfo {
    pub start_time: OffsetDateTime,
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

struct PoolSpaceInfo {
    pub free: usize,
    pub total: usize,
    pub used: usize,
}

impl ECStore {
    pub fn status(&self, idx: usize) -> Result<PoolStatus> {
        unimplemented!()
    }

    async fn get_decommission_pool_space_info(&self, idx: usize) -> Result<PoolSpaceInfo> {
        if let Some(sets) = self.pools.get(idx) {
            let mut info = sets.storage_info().await;
            info.backend = self.backend_info().await;

            unimplemented!()
        } else {
            Err(Error::msg("InvalidArgument"))
        }
    }
}

fn get_total_usable_capacity(disks: &Vec<StorageDisk>, info: &StorageInfo) -> usize {
    for disk in disks.iter() {
        // if disk.pool_index < 0 || info.backend.standard_scdata.len() <= disk.pool_index {
        //     continue;
        // }
    }
    unimplemented!()
}
