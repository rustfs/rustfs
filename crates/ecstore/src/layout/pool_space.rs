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

use std::slice::Iter;

use crate::bucket::utils::is_meta_bucketname;
use crate::disk::DiskInfo;
use crate::error::{Error, Result};
use crate::global::{DISK_ASSUME_UNKNOWN_SIZE, DISK_FILL_FRACTION, DISK_MIN_INODES, is_erasure_sd};

#[derive(Debug, Default, Clone)]
pub struct PoolAvailableSpace {
    pub index: usize,
    pub available: u64,    // in bytes
    pub max_used_pct: u64, // Used disk percentage of most filled disk, rounded down.
}

#[derive(Debug, Default, Clone)]
pub struct ServerPoolsAvailableSpace(pub(crate) Vec<PoolAvailableSpace>);

impl ServerPoolsAvailableSpace {
    pub fn iter(&self) -> Iter<'_, PoolAvailableSpace> {
        self.0.iter()
    }

    // TotalAvailable - total available space
    pub fn total_available(&self) -> u64 {
        let mut total = 0;
        for pool in &self.0 {
            total += pool.available;
        }
        total
    }

    // FilterMaxUsed will filter out any pools that has used percent bigger than max,
    // unless all have that, in which case all are preserved.
    pub fn filter_max_used(&mut self, max: u64) {
        if self.0.len() <= 1 {
            // Nothing to do.
            return;
        }
        let mut ok = false;
        for pool in &self.0 {
            if pool.available > 0 && pool.max_used_pct < max {
                ok = true;
                break;
            }
        }
        if !ok {
            // All above limit.
            // Do not modify
            return;
        }

        // Remove entries that are above.
        for pool in self.0.iter_mut() {
            if pool.available > 0 && pool.max_used_pct < max {
                continue;
            }
            pool.available = 0
        }
    }
}

pub async fn has_space_for(dis: &[Option<DiskInfo>], size: i64) -> Result<bool> {
    let size = { if size < 0 { DISK_ASSUME_UNKNOWN_SIZE } else { size as u64 * 2 } };

    let mut available = 0;
    let mut total = 0;
    let mut disks_num = 0;

    for disk in dis.iter().flatten() {
        disks_num += 1;
        total += disk.total;
        available += disk.total - disk.used;
    }

    if disks_num < dis.len() / 2 || disks_num == 0 {
        return Err(Error::other(format!(
            "not enough online disks to calculate the available space,need {}, found {}",
            (dis.len() / 2) + 1,
            disks_num,
        )));
    }

    let per_disk = size / disks_num as u64;

    for disk in dis.iter().flatten() {
        if !is_erasure_sd().await && disk.free_inodes < DISK_MIN_INODES && disk.used_inodes > 0 {
            return Ok(false);
        }

        if disk.free <= per_disk {
            return Ok(false);
        }
    }

    if available < size {
        return Ok(false);
    }
    available -= size;

    let want = total as f64 * (1.0 - DISK_FILL_FRACTION);

    Ok(available > want as u64)
}

pub(crate) async fn build_server_pools_available_space(
    bucket: &str,
    size: i64,
    n_sets: &[usize],
    infos: &[Vec<Option<DiskInfo>>],
) -> ServerPoolsAvailableSpace {
    let mut server_pools = vec![PoolAvailableSpace::default(); infos.len()];

    for (i, zinfo) in infos.iter().enumerate() {
        if zinfo.is_empty() {
            server_pools[i] = PoolAvailableSpace {
                index: i,
                ..Default::default()
            };

            continue;
        }

        if !is_meta_bucketname(bucket) && !has_space_for(zinfo, size).await.unwrap_or_default() {
            server_pools[i] = PoolAvailableSpace {
                index: i,
                ..Default::default()
            };

            continue;
        }

        let mut available = 0;
        let mut max_used_pct = 0;
        for disk in zinfo.iter().flatten() {
            if disk.total == 0 {
                continue;
            }

            available += disk.total - disk.used;

            let pct_used = disk.used * 100 / disk.total;

            if pct_used > max_used_pct {
                max_used_pct = pct_used;
            }
        }

        available *= n_sets[i] as u64;

        server_pools[i] = PoolAvailableSpace {
            index: i,
            available,
            max_used_pct,
        }
    }

    ServerPoolsAvailableSpace(server_pools)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk_info(total: u64, used: u64, free: u64) -> DiskInfo {
        DiskInfo {
            total,
            used,
            free,
            free_inodes: 1_024,
            ..Default::default()
        }
    }

    #[test]
    fn pool_available_space_creation_preserves_fields() {
        let space = PoolAvailableSpace {
            index: 0,
            available: 1000,
            max_used_pct: 50,
        };

        assert_eq!(space.index, 0);
        assert_eq!(space.available, 1000);
        assert_eq!(space.max_used_pct, 50);
    }

    #[test]
    fn server_pools_available_space_filter_keeps_shape_and_updates_availability() {
        let mut spaces = ServerPoolsAvailableSpace(vec![
            PoolAvailableSpace {
                index: 0,
                available: 1000,
                max_used_pct: 50,
            },
            PoolAvailableSpace {
                index: 1,
                available: 2000,
                max_used_pct: 80,
            },
        ]);

        assert_eq!(spaces.total_available(), 3000);

        spaces.filter_max_used(60);

        assert_eq!(spaces.0.len(), 2);
        assert_eq!(spaces.0[0].index, 0);
        assert_eq!(spaces.0[0].available, 1000);
        assert_eq!(spaces.0[1].available, 0);
        assert_eq!(spaces.total_available(), 1000);
    }

    #[test]
    fn server_pools_available_space_iter_preserves_pool_order() {
        let spaces = ServerPoolsAvailableSpace(vec![PoolAvailableSpace {
            index: 0,
            available: 1000,
            max_used_pct: 50,
        }]);

        let indexes = spaces.iter().map(|space| space.index).collect::<Vec<_>>();

        assert_eq!(indexes, vec![0]);
    }

    #[tokio::test]
    async fn build_server_pools_available_space_returns_zero_for_empty_pool_info() {
        let spaces = build_server_pools_available_space("bucket-a", 64, &[1], &[Vec::new()]).await;

        assert_eq!(spaces.0.len(), 1);
        assert_eq!(spaces.0[0].index, 0);
        assert_eq!(spaces.0[0].available, 0);
        assert_eq!(spaces.0[0].max_used_pct, 0);
    }

    #[tokio::test]
    async fn build_server_pools_available_space_computes_available_capacity_and_max_used_pct() {
        let infos = vec![vec![Some(disk_info(1_000, 100, 900)), Some(disk_info(1_000, 200, 800))]];

        let spaces = build_server_pools_available_space("bucket-a", 64, &[2], &infos).await;

        assert_eq!(spaces.0.len(), 1);
        assert_eq!(spaces.0[0].index, 0);
        assert_eq!(spaces.0[0].available, 3_400);
        assert_eq!(spaces.0[0].max_used_pct, 20);
    }

    #[tokio::test]
    async fn build_server_pools_available_space_skips_capacity_guard_for_meta_bucket() {
        let infos = vec![vec![Some(disk_info(10, 9, 1)), Some(disk_info(10, 9, 1))]];

        let spaces = build_server_pools_available_space(crate::disk::RUSTFS_META_BUCKET, 1_024, &[1], &infos).await;

        assert_eq!(spaces.0.len(), 1);
        assert_eq!(spaces.0[0].available, 2);
        assert_eq!(spaces.0[0].max_used_pct, 90);
    }
}
