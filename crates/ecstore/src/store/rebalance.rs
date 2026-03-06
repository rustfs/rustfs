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

use super::*;

impl ECStore {
    #[instrument(level = "debug", skip(self))]
    pub(super) async fn delete_all(&self, bucket: &str, prefix: &str) -> Result<()> {
        let mut futures = Vec::new();
        for sets in self.pools.iter() {
            for set in sets.disk_set.iter() {
                futures.push(set.delete_all(bucket, prefix));
                // let disks = set.disks.read().await;
                // let dd = disks.clone();
                // for disk in dd {
                //     if disk.is_none() {
                //         continue;
                //     }
                //     // let disk = disk.as_ref().unwrap().clone();
                //     // futures.push(disk.delete(
                //     //     bucket,
                //     //     prefix,
                //     //     DeleteOptions {
                //     //         recursive: true,
                //     //         immediate: false,
                //     //     },
                //     // ));
                // }
            }
        }
        let results = join_all(futures).await;

        let mut errs = Vec::new();

        for res in results {
            match res {
                Ok(_) => errs.push(None),
                Err(e) => errs.push(Some(e)),
            }
        }

        debug!("store delete_all errs {:?}", errs);

        Ok(())
    }

    pub(super) async fn delete_prefix(&self, bucket: &str, object: &str) -> Result<()> {
        for pool in self.pools.iter() {
            pool.delete_object(
                bucket,
                object,
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await?;
        }

        Ok(())
    }

    async fn get_available_pool_idx(&self, bucket: &str, object: &str, size: i64) -> Option<usize> {
        // // Return a random one first

        let mut server_pools = self.get_server_pools_available_space(bucket, object, size).await;
        server_pools.filter_max_used(100 - (100_f64 * DISK_RESERVE_FRACTION) as u64);
        let total = server_pools.total_available();

        if total == 0 {
            return None;
        }

        let mut rng = rand::rng();
        let random_u64: u64 = rng.random_range(0..total);

        let choose = random_u64 % total;
        let mut at_total = 0;

        for pool in server_pools.iter() {
            at_total += pool.available;
            if at_total > choose && pool.available > 0 {
                return Some(pool.index);
            }
        }

        None
    }

    async fn get_server_pools_available_space(&self, bucket: &str, object: &str, size: i64) -> ServerPoolsAvailableSpace {
        let mut n_sets = vec![0; self.pools.len()];
        let mut infos = vec![Vec::new(); self.pools.len()];

        // TODO: add concurrency
        for (idx, pool) in self.pools.iter().enumerate() {
            if self.is_suspended(idx).await || self.is_pool_rebalancing(idx).await {
                continue;
            }

            n_sets[idx] = pool.set_count;

            if let Ok(disks) = pool.get_disks_by_key(object).get_disks(0, 0).await {
                let disk_infos = get_disk_infos(&disks).await;
                infos[idx] = disk_infos;
            }
        }

        let mut server_pools = vec![PoolAvailableSpace::default(); self.pools.len()];
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

    pub(super) async fn is_suspended(&self, idx: usize) -> bool {
        // TODO: LOCK

        let pool_meta = self.pool_meta.read().await;

        pool_meta.is_suspended(idx)
    }

    pub(super) async fn get_pool_idx(&self, bucket: &str, object: &str, size: i64) -> Result<usize> {
        let idx = match self
            .get_pool_idx_existing_with_opts(
                bucket,
                object,
                &ObjectOptions {
                    skip_decommissioned: true,
                    skip_rebalancing: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(res) => res,
            Err(err) => {
                if !is_err_object_not_found(&err) {
                    return Err(err);
                }

                if let Some(hit_idx) = self.get_available_pool_idx(bucket, object, size).await {
                    hit_idx
                } else {
                    return Err(Error::DiskFull);
                }
            }
        };

        Ok(idx)
    }

    pub(super) async fn get_pool_idx_no_lock(&self, bucket: &str, object: &str, size: i64) -> Result<usize> {
        let idx = match self.get_pool_idx_existing_no_lock(bucket, object).await {
            Ok(res) => res,
            Err(err) => {
                if !is_err_object_not_found(&err) {
                    return Err(err);
                }

                if let Some(idx) = self.get_available_pool_idx(bucket, object, size).await {
                    idx
                } else {
                    warn!("get_pool_idx_no_lock: disk full {}/{}", bucket, object);
                    return Err(Error::DiskFull);
                }
            }
        };

        Ok(idx)
    }

    async fn get_pool_idx_existing_no_lock(&self, bucket: &str, object: &str) -> Result<usize> {
        self.get_pool_idx_existing_with_opts(
            bucket,
            object,
            &ObjectOptions {
                no_lock: true,
                skip_decommissioned: true,
                skip_rebalancing: true,
                ..Default::default()
            },
        )
        .await
    }

    pub(super) async fn get_pool_idx_existing_with_opts(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<usize> {
        let (pinfo, _) = self.get_pool_info_existing_with_opts(bucket, object, opts).await?;
        Ok(pinfo.index)
    }

    pub(super) async fn get_pool_info_existing_with_opts(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<(PoolObjInfo, Vec<PoolErr>)> {
        self.internal_get_pool_info_existing_with_opts(bucket, object, opts).await
    }

    async fn internal_get_pool_info_existing_with_opts(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<(PoolObjInfo, Vec<PoolErr>)> {
        let mut futures = Vec::new();
        for pool in self.pools.iter() {
            let mut pool_opts = opts.clone();
            if !pool_opts.metadata_chg {
                pool_opts.version_id = None;
            }

            futures.push(async move { pool.get_object_info(bucket, object, &pool_opts).await });
        }

        let results = join_all(futures).await;

        let mut ress = Vec::new();

        // join_all preserves the input order
        for (i, res) in results.into_iter().enumerate() {
            let index = i;

            match res {
                Ok(r) => {
                    ress.push(PoolObjInfo {
                        index,
                        object_info: r,
                        err: None,
                    });
                }
                Err(e) => {
                    ress.push(PoolObjInfo {
                        index,
                        err: Some(e),
                        ..Default::default()
                    });
                }
            }
        }

        ress.sort_by(|a, b| {
            let at = a.object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH);
            let bt = b.object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH);

            bt.cmp(&at)
        });

        let mut def_pool = PoolObjInfo::default();
        let mut has_def_pool = false;

        for pinfo in ress.iter() {
            if opts.skip_decommissioned && self.is_suspended(pinfo.index).await {
                continue;
            }

            if opts.skip_rebalancing && self.is_pool_rebalancing(pinfo.index).await {
                continue;
            }

            if pinfo.err.is_none() {
                return Ok((pinfo.clone(), self.pools_with_object(&ress, opts).await));
            }

            let err = pinfo.err.as_ref().unwrap();

            if err == &Error::ErasureReadQuorum && !opts.metadata_chg {
                return Ok((pinfo.clone(), self.pools_with_object(&ress, opts).await));
            }

            def_pool = pinfo.clone();
            has_def_pool = true;
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html
            if is_err_object_not_found(err)
                && let Err(err) = opts.precondition_check(&pinfo.object_info)
            {
                return Err(err.clone());
            }

            if !is_err_object_not_found(err) && !is_err_version_not_found(err) {
                return Err(err.clone());
            }

            if pinfo.object_info.delete_marker && !pinfo.object_info.name.is_empty() {
                return Ok((pinfo.clone(), Vec::new()));
            }
        }

        if opts.replication_request && opts.delete_marker && has_def_pool {
            return Ok((def_pool, Vec::new()));
        }

        Err(Error::ObjectNotFound(bucket.to_owned(), object.to_owned()))
    }

    async fn pools_with_object(&self, pools: &[PoolObjInfo], opts: &ObjectOptions) -> Vec<PoolErr> {
        let mut errs = Vec::new();

        for pool in pools.iter() {
            if opts.skip_decommissioned && self.is_suspended(pool.index).await {
                continue;
            }

            if opts.skip_rebalancing && self.is_pool_rebalancing(pool.index).await {
                continue;
            }

            if let Some(err) = &pool.err {
                if err == &Error::ErasureReadQuorum {
                    errs.push(PoolErr {
                        index: Some(pool.index),
                        err: Some(Error::ErasureReadQuorum),
                    });
                }
            } else {
                errs.push(PoolErr {
                    index: Some(pool.index),
                    err: None,
                });
            }
        }
        errs
    }

    pub(super) async fn get_latest_object_info_with_idx(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<(ObjectInfo, usize)> {
        let mut futures = Vec::with_capacity(self.pools.len());
        for pool in self.pools.iter() {
            futures.push(pool.get_object_info(bucket, object, opts));
        }

        let results = join_all(futures).await;

        struct IndexRes {
            res: Option<ObjectInfo>,
            idx: usize,
            err: Option<Error>,
        }

        let mut idx_res = Vec::with_capacity(self.pools.len());

        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(res) => {
                    idx_res.push(IndexRes {
                        res: Some(res),
                        idx,
                        err: None,
                    });
                }
                Err(e) => {
                    idx_res.push(IndexRes {
                        res: None,
                        idx,
                        err: Some(e),
                    });
                }
            }
        }

        // TODO: test order
        idx_res.sort_by(|a, b| {
            let a_mod = if let Some(o1) = &a.res {
                o1.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
            } else {
                OffsetDateTime::UNIX_EPOCH
            };

            let b_mod = if let Some(o2) = &b.res {
                o2.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
            } else {
                OffsetDateTime::UNIX_EPOCH
            };

            if a_mod == b_mod {
                return if a.idx < b.idx { Ordering::Greater } else { Ordering::Less };
            }

            b_mod.cmp(&a_mod)
        });

        for res in idx_res.into_iter() {
            if let Some(obj) = res.res {
                return Ok((obj, res.idx));
            }

            if let Some(err) = res.err
                && !is_err_object_not_found(&err)
                && !is_err_version_not_found(&err)
            {
                return Err(err);
            }

            // TODO: delete marker
        }

        let object = decode_dir_object(object);

        if opts.version_id.is_none() {
            Err(StorageError::ObjectNotFound(bucket.to_owned(), object.to_owned()))
        } else {
            Err(StorageError::VersionNotFound(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            ))
        }
    }

    pub(super) async fn delete_object_from_all_pools(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
        errs: Vec<PoolErr>,
    ) -> Result<ObjectInfo> {
        let mut objs = Vec::new();
        let mut derrs = Vec::new();

        for pe in errs.iter() {
            if let Some(err) = &pe.err
                && err == &StorageError::ErasureWriteQuorum
            {
                objs.push(None);
                derrs.push(Some(StorageError::ErasureWriteQuorum));
                continue;
            }

            if let Some(idx) = pe.index {
                match self.pools[idx].delete_object(bucket, object, opts.clone()).await {
                    Ok(res) => {
                        objs.push(Some(res));

                        derrs.push(None);
                    }
                    Err(err) => {
                        objs.push(None);
                        derrs.push(Some(err));
                    }
                }
            }
        }

        if let Some(e) = &derrs[0] {
            return Err(e.clone());
        }

        Ok(objs[0].as_ref().unwrap().clone())
    }

    pub async fn reload_pool_meta(&self) -> Result<()> {
        let mut meta = PoolMeta::default();
        meta.load(self.pools[0].clone(), self.pools.clone()).await?;

        let mut pool_meta = self.pool_meta.write().await;
        *pool_meta = meta;
        // *self.pool_meta.write().unwrap() = meta;
        Ok(())
    }

    /// Disk information deduplication function
    ///
    /// Use multiple field combinations to ensure uniqueness:
    /// - endpoint (node address)
    /// - drive_path (mount path)
    /// - pool_index (pool index)
    /// - set_index (Collection Index)
    /// - disk_index (disk index)
    pub(crate) fn deduplicate_disks(disks: Vec<rustfs_madmin::Disk>) -> Vec<rustfs_madmin::Disk> {
        use std::collections::HashMap;
        use std::collections::hash_map::Entry;

        let mut unique_disks: HashMap<String, rustfs_madmin::Disk> = HashMap::new();
        let mut duplicate_count = 0;

        for disk in disks {
            let key = format!(
                "{}|{}|p{}s{}d{}",
                disk.endpoint, disk.drive_path, disk.pool_index, disk.set_index, disk.disk_index
            );

            match unique_disks.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(disk);
                }
                Entry::Occupied(_) => {
                    duplicate_count += 1;
                }
            }
        }

        if duplicate_count > 0 {
            debug!("Deduplicated {} duplicate disk entries", duplicate_count);
        }

        unique_disks.into_values().collect()
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper> {
        self.pools[0].new_ns_lock(bucket, object).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_backend_info(&self) -> rustfs_madmin::BackendInfo {
        let (standard_sc_parity, rr_sc_parity) = {
            if let Some(sc) = GLOBAL_STORAGE_CLASS.get() {
                let sc_parity = sc
                    .get_parity_for_sc(storageclass::CLASS_STANDARD)
                    .or(Some(self.pools[0].default_parity_count));

                let rrs_sc_parity = sc.get_parity_for_sc(storageclass::RRS);

                (sc_parity, rrs_sc_parity)
            } else {
                (Some(self.pools[0].default_parity_count), None)
            }
        };

        let mut standard_sc_data = Vec::new();
        let mut rr_sc_data = Vec::new();
        let mut drives_per_set = Vec::new();
        let mut total_sets = Vec::new();

        for (idx, set_count) in self.set_drive_counts().iter().enumerate() {
            if let Some(sc_parity) = standard_sc_parity {
                standard_sc_data.push(set_count - sc_parity);
            }
            if let Some(sc_parity) = rr_sc_parity {
                rr_sc_data.push(set_count - sc_parity);
            }
            total_sets.push(self.pools[idx].set_count);
            drives_per_set.push(*set_count);
        }

        rustfs_madmin::BackendInfo {
            backend_type: rustfs_madmin::BackendByte::Erasure,
            online_disks: rustfs_madmin::BackendDisks::new(),
            offline_disks: rustfs_madmin::BackendDisks::new(),
            standard_sc_data,
            standard_sc_parity,
            rr_sc_data,
            rr_sc_parity,
            total_sets,
            drives_per_set,
            ..Default::default()
        }
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_storage_info(&self) -> rustfs_madmin::StorageInfo {
        let Some(notification_sy) = get_global_notification_sys() else {
            return rustfs_madmin::StorageInfo::default();
        };

        let mut info = notification_sy.storage_info(self).await;

        // 🔧 Defensive deduplication: This protection mechanism is retained even if the upstream is fixed
        let original_count = info.disks.len();
        info.disks = Self::deduplicate_disks(info.disks);
        let final_count = info.disks.len();

        if original_count != final_count {
            warn!(
                "Storage info deduplication: removed {} duplicate disk entries ({} -> {})",
                original_count - final_count,
                original_count,
                final_count
            );
        }

        info
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_local_storage_info(&self) -> rustfs_madmin::StorageInfo {
        let mut futures = Vec::with_capacity(self.pools.len());

        for pool in self.pools.iter() {
            futures.push(pool.local_storage_info())
        }

        let results = join_all(futures).await;

        let mut disks = Vec::new();

        for res in results.into_iter() {
            disks.extend_from_slice(&res.disks);
        }

        // 🔧 Defensive deduplication: when aggregating disks from all pools, drop duplicate
        //  entries that may be reported multiple times by backends; this extra layer is kept
        //  even if the upstream reporting is later fixed.
        let original_count = disks.len();
        disks = Self::deduplicate_disks(disks);

        if original_count != disks.len() {
            warn!("Local storage info deduplication: {} -> {}", original_count, disks.len());
        }

        let backend = self.backend_info().await;
        rustfs_madmin::StorageInfo { backend, disks }
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_disks(&self, pool_idx: usize, set_idx: usize) -> Result<Vec<Option<DiskStore>>> {
        if pool_idx < self.pools.len() && set_idx < self.pools[pool_idx].disk_set.len() {
            self.pools[pool_idx].disk_set[set_idx].get_disks(0, 0).await
        } else {
            Err(Error::other(format!("pool idx {pool_idx}, set idx {set_idx}, not found")))
        }
    }

    #[instrument(skip(self))]
    pub(super) fn handle_set_drive_counts(&self) -> Vec<usize> {
        let mut counts = vec![0; self.pools.len()];

        for (i, pool) in self.pools.iter().enumerate() {
            counts[i] = pool.set_drive_count();
        }
        counts
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
        for (pool_idx, pool) in self.pools.iter().enumerate() {
            for (set_idx, set) in pool.format.erasure.sets.iter().enumerate() {
                for (disk_idx, disk_id) in set.iter().enumerate() {
                    if disk_id.to_string() == id {
                        return Ok((Some(pool_idx), Some(set_idx), Some(disk_idx)));
                    }
                }
            }
        }

        Err(Error::DiskNotFound)
    }
}
