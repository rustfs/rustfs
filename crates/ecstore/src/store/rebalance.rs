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
use crate::layout::pool_space::{ServerPoolsAvailableSpace, build_server_pools_available_space};
use crate::runtime_sources;
use rustfs_storage_api::{NamespaceLocking as _, ObjectOperations as _, StorageAdminApi};
pub(in crate::store) mod support;
use support::{
    LatestObjectInfoCandidate, PoolErr, PoolObjInfo, RebalanceDeletePoolResult, pool_lookup_not_found_error,
    rebalance_disk_set_lookup_error, resolve_latest_object_info_candidates, resolve_rebalance_delete_from_all_pools_result,
    resolve_rebalance_delete_from_all_pools_results, resolve_store_rebalance_pool_meta_reload_result,
};

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

    pub(super) async fn delete_prefix(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        for pool in self.pools.iter() {
            let mut opts = opts.clone();
            opts.delete_prefix = true;
            pool.delete_object(bucket, object, opts).await?;
        }

        Ok(())
    }

    pub(super) async fn get_available_pool_idx(&self, bucket: &str, object: &str, size: i64) -> Option<usize> {
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

    pub(super) async fn get_available_pool_idx_excluding(
        &self,
        bucket: &str,
        object: &str,
        size: i64,
        excluded_pool_idx: usize,
    ) -> Option<usize> {
        let mut server_pools = self.get_server_pools_available_space(bucket, object, size).await;
        server_pools.filter_max_used(100 - (100_f64 * DISK_RESERVE_FRACTION) as u64);

        if let Some(pool) = server_pools.0.get_mut(excluded_pool_idx) {
            pool.available = 0;
        }

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
        let pool_inputs = join_all(self.pools.iter().enumerate().map(|(idx, pool)| async move {
            if self.is_suspended(idx).await || self.is_pool_rebalancing(idx).await {
                return (idx, 0, Vec::new());
            }

            let disks = pool.get_disks_by_key(object).disk_inventory().await;
            let disk_infos = get_disk_infos(&disks).await;

            (idx, pool.set_count, disk_infos)
        }))
        .await;

        for (idx, set_count, disk_infos) in pool_inputs {
            n_sets[idx] = set_count;
            infos[idx] = disk_infos;
        }

        build_server_pools_available_space(bucket, size, &n_sets, &infos).await
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
                return Err(err);
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

        Err(pool_lookup_not_found_error(bucket, object, opts))
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
        let mut candidates = Vec::with_capacity(self.pools.len());

        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(res) => {
                    candidates.push(LatestObjectInfoCandidate {
                        info: Some(res),
                        idx,
                        err: None,
                    });
                }
                Err(e) => {
                    candidates.push(LatestObjectInfoCandidate {
                        info: None,
                        idx,
                        err: Some(e),
                    });
                }
            }
        }

        // Delete markers are returned as latest object infos here. Higher-level
        // access paths are responsible for translating them into read/write
        // semantics such as object-not-found or method-not-allowed.
        resolve_latest_object_info_candidates(candidates, bucket, object, opts)
    }

    pub(super) async fn delete_object_from_all_pools(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
        errs: Vec<PoolErr>,
    ) -> Result<ObjectInfo> {
        let mut results = Vec::with_capacity(errs.len());

        for pe in errs.iter() {
            if let Some(err) = &pe.err
                && err == &StorageError::ErasureWriteQuorum
            {
                if let Some(idx) = pe.index {
                    results.push(RebalanceDeletePoolResult {
                        pool_idx: idx,
                        result: Err(StorageError::ErasureWriteQuorum),
                    });
                }
                continue;
            }

            if let Some(idx) = pe.index {
                results.push(RebalanceDeletePoolResult {
                    pool_idx: idx,
                    result: self.pools[idx].delete_object(bucket, object, opts.clone()).await,
                });
            }
        }

        resolve_rebalance_delete_from_all_pools_result(
            resolve_rebalance_delete_from_all_pools_results(results, bucket, object),
            bucket,
            object,
        )
    }

    pub async fn reload_pool_meta(&self) -> Result<()> {
        let mut meta = PoolMeta::default();
        resolve_store_rebalance_pool_meta_reload_result(
            meta.load(self.pools[0].clone(), self.pools.clone()).await,
            "reload_pool_meta",
        )?;

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
        let (standard_sc_parity, rr_sc_parity) =
            runtime_sources::backend_storage_class_parities(self.pools[0].default_parity_count);

        let mut standard_sc_data = Vec::new();
        let mut rr_sc_data = Vec::new();
        let mut drives_per_set = Vec::new();
        let mut total_sets = Vec::new();

        for (idx, set_count) in StorageAdminApi::set_drive_counts(self).iter().enumerate() {
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
        let Some(notification_sy) = runtime_sources::notification_sys() else {
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
            futures.push(pool.local_storage_info_snapshot())
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

        let backend = StorageAdminApi::backend_info(self).await;
        rustfs_madmin::StorageInfo { backend, disks }
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_disks(&self, pool_idx: usize, set_idx: usize) -> Result<Vec<Option<DiskStore>>> {
        if pool_idx < self.pools.len() && set_idx < self.pools[pool_idx].disk_set.len() {
            Ok(self.pools[pool_idx].disk_set[set_idx].disk_inventory().await)
        } else {
            Err(rebalance_disk_set_lookup_error(pool_idx, set_idx, self.pools.len()))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn object_info_with_mod_time(unix_ts: i64, delete_marker: bool) -> ObjectInfo {
        ObjectInfo {
            mod_time: Some(OffsetDateTime::from_unix_timestamp(unix_ts).unwrap()),
            delete_marker,
            ..Default::default()
        }
    }

    #[test]
    fn resolve_latest_object_info_candidates_returns_latest_delete_marker() {
        let candidates = vec![
            LatestObjectInfoCandidate {
                info: Some(object_info_with_mod_time(10, false)),
                idx: 0,
                err: None,
            },
            LatestObjectInfoCandidate {
                info: Some(object_info_with_mod_time(20, true)),
                idx: 1,
                err: None,
            },
        ];

        let (info, idx) =
            resolve_latest_object_info_candidates(candidates, "bucket", "object", &ObjectOptions::default()).unwrap();

        assert_eq!(idx, 1);
        assert!(info.delete_marker);
    }

    #[test]
    fn resolve_latest_object_info_candidates_prefers_higher_pool_idx_on_equal_mod_time() {
        let candidates = vec![
            LatestObjectInfoCandidate {
                info: Some(object_info_with_mod_time(10, false)),
                idx: 0,
                err: None,
            },
            LatestObjectInfoCandidate {
                info: Some(object_info_with_mod_time(10, false)),
                idx: 1,
                err: None,
            },
        ];

        let (_, idx) = resolve_latest_object_info_candidates(candidates, "bucket", "object", &ObjectOptions::default()).unwrap();

        assert_eq!(idx, 1);
    }

    #[test]
    fn resolve_latest_object_info_candidates_returns_non_not_found_error() {
        let err = resolve_latest_object_info_candidates(
            vec![LatestObjectInfoCandidate {
                info: None,
                idx: 0,
                err: Some(Error::ErasureReadQuorum),
            }],
            "bucket",
            "object",
            &ObjectOptions::default(),
        )
        .unwrap_err();

        assert_eq!(err, Error::ErasureReadQuorum);
    }

    #[test]
    fn resolve_latest_object_info_candidates_returns_version_not_found_for_versioned_lookups() {
        let err = resolve_latest_object_info_candidates(
            vec![LatestObjectInfoCandidate {
                info: None,
                idx: 0,
                err: Some(Error::ObjectNotFound("bucket".to_string(), "object".to_string())),
            }],
            "bucket",
            "object",
            &ObjectOptions {
                version_id: Some("vid-1".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert_eq!(
            err,
            Error::VersionNotFound("bucket".to_string(), "object".to_string(), "vid-1".to_string())
        );
    }

    #[test]
    fn pool_lookup_not_found_error_returns_object_not_found_for_latest_lookup() {
        let err = pool_lookup_not_found_error("bucket", "object", &ObjectOptions::default());

        assert_eq!(err, Error::ObjectNotFound("bucket".to_string(), "object".to_string()));
    }

    #[test]
    fn pool_lookup_not_found_error_returns_version_not_found_for_versioned_lookup() {
        let err = pool_lookup_not_found_error(
            "bucket",
            "object",
            &ObjectOptions {
                version_id: Some("vid-1".to_string()),
                ..Default::default()
            },
        );

        assert_eq!(
            err,
            Error::VersionNotFound("bucket".to_string(), "object".to_string(), "vid-1".to_string())
        );
    }

    #[test]
    fn resolve_store_rebalance_pool_meta_reload_result_passthrough_ok() {
        resolve_store_rebalance_pool_meta_reload_result(Ok(()), "reload_pool_meta")
            .expect("successful pool meta reload should pass through");
    }

    #[test]
    fn resolve_store_rebalance_pool_meta_reload_result_wraps_error_context() {
        let err = resolve_store_rebalance_pool_meta_reload_result(Err(Error::SlowDown), "reload_pool_meta")
            .expect_err("failed pool meta reload should be wrapped");
        let err_message = err.to_string();
        assert!(err_message.contains("store rebalance pool meta reload failed during reload_pool_meta"));
        assert!(err_message.contains(&Error::SlowDown.to_string()));
    }

    #[test]
    fn resolve_rebalance_delete_from_all_pools_result_passthrough_ok() {
        let info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };

        let resolved = resolve_rebalance_delete_from_all_pools_result(Ok(info.clone()), "bucket", "object")
            .expect("successful rebalance delete should pass through");

        assert_eq!(resolved.bucket, info.bucket);
        assert_eq!(resolved.name, info.name);
    }

    #[test]
    fn resolve_rebalance_delete_from_all_pools_result_wraps_object_context() {
        let err = resolve_rebalance_delete_from_all_pools_result(Err(Error::SlowDown), "bucket", "object")
            .expect_err("failed rebalance delete should be wrapped");
        let rendered = err.to_string();

        assert!(rendered.contains("failed to delete rebalance source object bucket/object"), "{rendered}");
        assert!(rendered.contains(&Error::SlowDown.to_string()), "{rendered}");
    }

    #[test]
    fn resolve_rebalance_delete_from_all_pools_results_fails_on_later_pool_error() {
        let err = resolve_rebalance_delete_from_all_pools_results(
            vec![
                RebalanceDeletePoolResult {
                    pool_idx: 0,
                    result: Ok(ObjectInfo {
                        bucket: "bucket".to_string(),
                        name: "object".to_string(),
                        ..Default::default()
                    }),
                },
                RebalanceDeletePoolResult {
                    pool_idx: 1,
                    result: Err(Error::SlowDown),
                },
            ],
            "bucket",
            "object",
        )
        .expect_err("non-ignorable errors from later pools must not be hidden");
        let rendered = err.to_string();

        assert!(rendered.contains("pool 1 delete failed for bucket/object"), "{rendered}");
        assert!(rendered.contains(&Error::SlowDown.to_string()), "{rendered}");
    }

    #[test]
    fn resolve_rebalance_delete_from_all_pools_results_ignores_later_not_found_after_success() {
        let info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };

        let resolved = resolve_rebalance_delete_from_all_pools_results(
            vec![
                RebalanceDeletePoolResult {
                    pool_idx: 0,
                    result: Ok(info.clone()),
                },
                RebalanceDeletePoolResult {
                    pool_idx: 1,
                    result: Err(Error::ObjectNotFound("bucket".to_string(), "object".to_string())),
                },
            ],
            "bucket",
            "object",
        )
        .expect("not-found errors from other pools should be ignored when a delete succeeds");

        assert_eq!(resolved.bucket, info.bucket);
        assert_eq!(resolved.name, info.name);
    }

    #[test]
    fn resolve_rebalance_delete_from_all_pools_results_accepts_success_after_not_found() {
        let info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };

        let resolved = resolve_rebalance_delete_from_all_pools_results(
            vec![
                RebalanceDeletePoolResult {
                    pool_idx: 0,
                    result: Err(Error::ObjectNotFound("bucket".to_string(), "object".to_string())),
                },
                RebalanceDeletePoolResult {
                    pool_idx: 1,
                    result: Ok(info.clone()),
                },
            ],
            "bucket",
            "object",
        )
        .expect("a successful delete should pass even when an earlier pool reports not-found");

        assert_eq!(resolved.bucket, info.bucket);
        assert_eq!(resolved.name, info.name);
    }

    #[test]
    fn resolve_rebalance_delete_from_all_pools_results_fails_when_all_results_are_ignored_errors() {
        let err = resolve_rebalance_delete_from_all_pools_results(
            vec![
                RebalanceDeletePoolResult {
                    pool_idx: 0,
                    result: Err(Error::ObjectNotFound("bucket".to_string(), "object".to_string())),
                },
                RebalanceDeletePoolResult {
                    pool_idx: 1,
                    result: Err(Error::VersionNotFound("bucket".to_string(), "object".to_string(), "vid-1".to_string())),
                },
            ],
            "bucket",
            "object",
        )
        .expect_err("all ignored errors without any successful delete should still fail");
        let rendered = err.to_string();

        assert!(rendered.contains("pool 1 delete failed for bucket/object"), "{rendered}");
        assert!(rendered.contains("Version not found"), "{rendered}");
    }

    #[test]
    fn resolve_rebalance_delete_from_all_pools_results_fails_on_write_quorum_even_with_success() {
        let err = resolve_rebalance_delete_from_all_pools_results(
            vec![
                RebalanceDeletePoolResult {
                    pool_idx: 0,
                    result: Ok(ObjectInfo {
                        bucket: "bucket".to_string(),
                        name: "object".to_string(),
                        ..Default::default()
                    }),
                },
                RebalanceDeletePoolResult {
                    pool_idx: 1,
                    result: Err(Error::ErasureWriteQuorum),
                },
            ],
            "bucket",
            "object",
        )
        .expect_err("write quorum failures must fail the aggregate delete");
        let rendered = err.to_string();

        assert!(rendered.contains("pool 1 delete failed for bucket/object"), "{rendered}");
        assert!(rendered.contains(&Error::ErasureWriteQuorum.to_string()), "{rendered}");
    }

    #[test]
    fn rebalance_disk_set_lookup_error_formats_pool_and_set_context() {
        let err = rebalance_disk_set_lookup_error(2, 7, 3);

        assert!(
            err.to_string()
                .contains("failed to resolve rebalance disk set: pool index 2, set index 7, pool count 3")
        );
    }
}
