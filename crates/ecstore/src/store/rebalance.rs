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
use crate::config::storageclass;
use crate::core::pools::merge_pool_status_refresh;
use crate::layout::pool_space::{ServerPoolsAvailableSpace, build_server_pools_available_space};
use crate::runtime::sources as runtime_sources;
use crate::storage_api_contracts::{admin::StorageAdminApi, namespace::NamespaceLocking as _, object::ObjectOperations as _};
pub(in crate::store) mod support;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_POOLS: &str = "pools";
const EVENT_POOL_META_RELOAD: &str = "pool_meta_reload";
use support::{
    LatestObjectInfoCandidate, PoolErr, PoolObjInfo, RebalanceDeletePoolResult, pool_lookup_not_found_error,
    rebalance_disk_set_lookup_error, resolve_latest_object_info_candidates, resolve_rebalance_delete_from_all_pools_result,
    resolve_rebalance_delete_from_all_pools_results, resolve_store_rebalance_pool_meta_reload_result,
};

#[derive(Debug, Default, Eq, PartialEq)]
struct BackendStorageClassInfo {
    standard_sc_data: Vec<usize>,
    standard_sc_parities: Vec<usize>,
    standard_sc_parity: Option<usize>,
    rr_sc_data: Vec<usize>,
    rr_sc_parities: Vec<usize>,
    rr_sc_parity: Option<usize>,
}

fn resolve_pool_layout(
    drives_per_set: &[usize],
    mut parity_for_pool: impl FnMut(usize, usize) -> Option<usize>,
) -> Option<(Vec<usize>, Vec<usize>)> {
    let mut parities = Vec::with_capacity(drives_per_set.len());
    let mut data = Vec::with_capacity(drives_per_set.len());

    for (pool_index, &drives) in drives_per_set.iter().enumerate() {
        if drives == 0 {
            return None;
        }

        let parity = parity_for_pool(pool_index, drives)?;
        let data_drives = drives.checked_sub(parity)?;
        if data_drives == 0 || parity > data_drives {
            return None;
        }

        data.push(data_drives);
        parities.push(parity);
    }

    Some((parities, data))
}

fn homogeneous_parity(parities: &[usize]) -> Option<usize> {
    let first = *parities.first()?;
    parities.iter().all(|&parity| parity == first).then_some(first)
}

fn resolve_complete_pool_layouts(
    drives_per_set: &[usize],
    standard_parity_for_pool: impl FnMut(usize, usize) -> Option<usize>,
    rr_parity_for_pool: impl FnMut(usize, usize) -> Option<usize>,
) -> Option<BackendStorageClassInfo> {
    let (standard_sc_parities, standard_sc_data) = resolve_pool_layout(drives_per_set, standard_parity_for_pool)?;
    let (rr_sc_parities, rr_sc_data) = resolve_pool_layout(drives_per_set, rr_parity_for_pool)?;

    for ((&standard_parity, &rr_parity), &drives) in standard_sc_parities.iter().zip(&rr_sc_parities).zip(drives_per_set) {
        storageclass::validate_parity_inner(standard_parity, rr_parity, drives).ok()?;
    }

    Some(BackendStorageClassInfo {
        standard_sc_parity: homogeneous_parity(&standard_sc_parities),
        standard_sc_data,
        standard_sc_parities,
        rr_sc_parity: homogeneous_parity(&rr_sc_parities),
        rr_sc_data,
        rr_sc_parities,
    })
}

fn resolve_backend_storage_class_info(
    config: &storageclass::Config,
    drives_per_set: &[usize],
    default_standard_parities: &[usize],
) -> BackendStorageClassInfo {
    if drives_per_set.len() != default_standard_parities.len() {
        return BackendStorageClassInfo::default();
    }

    if !config.is_initialized() {
        let Some((standard_sc_parities, standard_sc_data)) =
            resolve_pool_layout(drives_per_set, |pool_index, _| default_standard_parities.get(pool_index).copied())
        else {
            return BackendStorageClassInfo::default();
        };

        return BackendStorageClassInfo {
            standard_sc_parity: homogeneous_parity(&standard_sc_parities),
            standard_sc_data,
            standard_sc_parities,
            ..Default::default()
        };
    }

    match (config.parities_for_sc(storageclass::STANDARD), config.parities_for_sc(storageclass::RRS)) {
        (Some(standard), Some(rr)) if standard.len() == drives_per_set.len() && rr.len() == drives_per_set.len() => {
            resolve_complete_pool_layouts(
                drives_per_set,
                |pool_index, drives| config.parity_for_pool(storageclass::STANDARD, pool_index, drives),
                |pool_index, drives| config.parity_for_pool(storageclass::RRS, pool_index, drives),
            )
            .unwrap_or_default()
        }
        (None, None) => {
            let Some(standard) = config.get_parity_for_sc(storageclass::STANDARD) else {
                return BackendStorageClassInfo::default();
            };
            let Some(rr) = config.get_parity_for_sc(storageclass::RRS) else {
                return BackendStorageClassInfo::default();
            };

            resolve_complete_pool_layouts(drives_per_set, |_, _| Some(standard), |_, _| Some(rr)).unwrap_or_default()
        }
        _ => BackendStorageClassInfo::default(),
    }
}

fn build_backend_info(
    config: &storageclass::Config,
    drives_per_set: &[usize],
    default_standard_parities: &[usize],
    total_sets: &[usize],
) -> rustfs_madmin::BackendInfo {
    let storage_class_info = if total_sets.len() == drives_per_set.len() {
        resolve_backend_storage_class_info(config, drives_per_set, default_standard_parities)
    } else {
        BackendStorageClassInfo::default()
    };

    rustfs_madmin::BackendInfo {
        backend_type: rustfs_madmin::BackendByte::Erasure,
        online_disks: rustfs_madmin::BackendDisks::new(),
        offline_disks: rustfs_madmin::BackendDisks::new(),
        standard_sc_data: storage_class_info.standard_sc_data,
        standard_sc_parities: storage_class_info.standard_sc_parities,
        standard_sc_parity: storage_class_info.standard_sc_parity,
        rr_sc_data: storage_class_info.rr_sc_data,
        rr_sc_parities: storage_class_info.rr_sc_parities,
        rr_sc_parity: storage_class_info.rr_sc_parity,
        total_sets: total_sets.to_vec(),
        drives_per_set: drives_per_set.to_vec(),
    }
}

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
                //     // let disk = disk.as_ref().expect("operation should succeed").clone();
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
        if opts.lifecycle_delete_all.is_some() {
            let mut preflight_opts = opts.clone();
            preflight_opts
                .lifecycle_delete_all
                .as_mut()
                .ok_or(StorageError::PreconditionFailed)?
                .phase = crate::object_api::LifecycleDeleteAllPhase::Preflight;
            for pool in &self.pools {
                #[cfg(test)]
                lifecycle_delete_all_test_failure(crate::object_api::LifecycleDeleteAllPhase::Preflight, pool.pool_idx)?;
                pool.delete_object(bucket, object, preflight_opts.clone()).await?;
            }

            opts.lifecycle_delete_all_journal()
                .ok_or(StorageError::PreconditionFailed)?
                .lock()
                .mark_mutation_started();
            let mut non_trigger_opts = opts.clone();
            non_trigger_opts
                .lifecycle_delete_all
                .as_mut()
                .ok_or(StorageError::PreconditionFailed)?
                .phase = crate::object_api::LifecycleDeleteAllPhase::History;
            for pool in &self.pools {
                #[cfg(test)]
                lifecycle_delete_all_test_failure(crate::object_api::LifecycleDeleteAllPhase::History, pool.pool_idx)?;
                let mut pool_opts = non_trigger_opts.clone();
                pool_opts.delete_prefix = true;
                pool.delete_object(bucket, object, pool_opts).await?;
            }

            let mut final_preflight_opts = opts.clone();
            final_preflight_opts
                .lifecycle_delete_all
                .as_mut()
                .ok_or(StorageError::PreconditionFailed)?
                .phase = crate::object_api::LifecycleDeleteAllPhase::FinalPreflight;
            let mut trigger_pools = Vec::new();
            for (pool_index, pool) in self.pools.iter().enumerate() {
                #[cfg(test)]
                lifecycle_delete_all_test_failure(crate::object_api::LifecycleDeleteAllPhase::FinalPreflight, pool.pool_idx)?;
                let result = pool.delete_object(bucket, object, final_preflight_opts.clone()).await?;
                if !result.name.is_empty() {
                    trigger_pools.push(pool_index);
                }
            }
            if trigger_pools.is_empty() {
                return Err(StorageError::PreconditionFailed);
            }

            let mut trigger_opts = opts.clone();
            trigger_opts
                .lifecycle_delete_all
                .as_mut()
                .ok_or(StorageError::PreconditionFailed)?
                .phase = crate::object_api::LifecycleDeleteAllPhase::Trigger;
            for pool_index in trigger_pools {
                #[cfg(test)]
                lifecycle_delete_all_test_failure(crate::object_api::LifecycleDeleteAllPhase::Trigger, pool_index)?;
                let mut pool_opts = trigger_opts.clone();
                pool_opts.delete_prefix = true;
                self.pools[pool_index].delete_object(bucket, object, pool_opts).await?;
            }
            return Ok(());
        }

        let mut first_error = None;
        let mut first_volume_error = None;
        let mut has_success = false;
        for pool in &self.pools {
            let mut opts = opts.clone();
            opts.delete_prefix = true;
            match pool.delete_object(bucket, object, opts).await {
                Ok(_) => has_success = true,
                Err(err) if is_err_strict_volume_not_found(&err) => {
                    if first_volume_error.is_none() {
                        first_volume_error = Some(err);
                    }
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        match first_error {
            Some(err) => Err(err),
            None if has_success => Ok(()),
            None => match first_volume_error {
                Some(err) => Err(err),
                None => Ok(()),
            },
        }
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
        // TODO(backlog): acquire pool metadata lock for consistent suspension check

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

            let err = pinfo.err.as_ref().expect("operation should succeed");

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
        for (idx, pool) in self.pools.iter().enumerate() {
            if opts.skip_decommissioned && self.is_suspended(idx).await {
                continue;
            }

            if opts.skip_rebalancing && self.is_pool_rebalancing(idx).await {
                continue;
            }

            futures.push(async move { (idx, pool.get_object_info(bucket, object, opts).await) });
        }

        let results = join_all(futures).await;
        let mut candidates = Vec::with_capacity(self.pools.len());

        for (idx, result) in results {
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

    /// Peer reload entry: refreshes in-memory pool metadata from the shared
    /// persisted snapshot. Returns whether newer state was actually merged so
    /// callers only trigger missing-worker recovery after a real state change;
    /// delayed snapshots are merged monotonically and never blind-assigned.
    pub async fn reload_pool_meta(&self) -> Result<bool> {
        // Serialize the durable reload with local movement transitions. Loading
        // before acquiring this gate would allow a stale disk snapshot to
        // overwrite a newer local transition after the writer commits.
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        let mut reloaded = PoolMeta::default();
        resolve_store_rebalance_pool_meta_reload_result(
            reloaded.load(self.pools[0].clone(), self.pools.clone()).await,
            "reload_pool_meta",
        )?;

        // Lock order: release the decommission_cancelers guard before taking
        // the pool_meta write guard; neither is held without the movement gate.
        let active_workers = {
            let cancelers = self.decommission_cancelers.read().await;
            cancelers
                .iter()
                .map(|canceler| canceler.as_ref().is_some_and(DecommissionCanceler::is_active))
                .collect::<Vec<_>>()
        };

        let incoming_has_pools = !reloaded.pools.is_empty();
        let mut pool_meta = self.pool_meta.write().await;
        let movement_before = pool_meta.clone();
        let merged_newer = merge_pool_status_refresh(&mut pool_meta, reloaded, &active_workers);
        if crate::core::pools::pool_meta_movement_snapshot_changed(&movement_before, &pool_meta) {
            self.ctx.advance_data_movement_operation_epoch();
        }

        if !merged_newer && !incoming_has_pools {
            warn!(
                event = EVENT_POOL_META_RELOAD,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                result = "ignored",
                reason = "missing_metadata",
                "Peer pool meta reload ignored because persisted metadata is missing"
            );
        } else if !merged_newer {
            debug!(
                event = EVENT_POOL_META_RELOAD,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_POOLS,
                result = "ignored",
                reason = "stale_snapshot",
                "Peer pool meta reload ignored as a stale snapshot"
            );
        }

        Ok(merged_newer)
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

    #[instrument(level = "trace", skip(self))]
    pub(super) async fn handle_new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper> {
        self.pools[0].new_ns_lock(bucket, object).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_backend_info(&self) -> rustfs_madmin::BackendInfo {
        let drives_per_set = StorageAdminApi::set_drive_counts(self);
        let default_standard_parities = self.pools.iter().map(|pool| pool.default_parity_count).collect::<Vec<_>>();
        let storage_class = runtime_sources::storage_class_config_snapshot();
        let total_sets = self.pools.iter().map(|pool| pool.set_count).collect::<Vec<_>>();

        build_backend_info(&storage_class, &drives_per_set, &default_standard_parities, &total_sets)
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
static LIFECYCLE_DELETE_ALL_TEST_FAILURE: std::sync::Mutex<Option<(crate::object_api::LifecycleDeleteAllPhase, usize)>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
fn lifecycle_delete_all_test_failure(phase: crate::object_api::LifecycleDeleteAllPhase, pool_index: usize) -> Result<()> {
    if LIFECYCLE_DELETE_ALL_TEST_FAILURE
        .lock()
        .expect("lifecycle delete-all failure hook should not poison")
        .is_some_and(|failure| failure == (phase, pool_index))
    {
        return Err(StorageError::PreconditionFailed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::replication::{ReplicationStatusType, VersionPurgeStatusType};
    use crate::config::storageclass::{CLASS_RRS, CLASS_STANDARD, lookup_config_for_pools_without_env};
    use crate::core::pools::{POOL_META_VERSION, PoolDecommissionInfo, PoolStatus};
    use crate::disk::error::DiskError;
    use crate::layout::endpoint::Endpoint;
    use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    use crate::object_api::ObjectLockConfigSnapshot;
    use crate::storage_api_contracts::bucket::MakeBucketOptions;
    use crate::storage_api_contracts::object::ObjectIO as _;
    use arc_swap::ArcSwap;
    use rustfs_config::server_config::KVS;
    use rustfs_filemeta::FileInfo;
    use std::sync::Arc;
    use time::{Duration as TimeDuration, OffsetDateTime};
    use tokio_util::sync::CancellationToken;

    async fn setup_multi_pool_test_store(
        name: &str,
        drives_per_pool: &[usize],
    ) -> (tempfile::TempDir, Arc<ECStore>, CancellationToken) {
        let temp_dir = tempfile::tempdir().expect("multi-pool test directory should be created");
        let mut pools = Vec::with_capacity(drives_per_pool.len());
        for (pool_index, drives_per_set) in drives_per_pool.iter().copied().enumerate() {
            let mut endpoints = Vec::with_capacity(drives_per_set);
            for disk_index in 0..drives_per_set {
                let disk_path = temp_dir.path().join(format!("pool{pool_index}-disk{disk_index}"));
                tokio::fs::create_dir_all(&disk_path)
                    .await
                    .expect("multi-pool test disk should be created");
                let mut endpoint =
                    Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
                endpoint.set_pool_index(pool_index);
                endpoint.set_set_index(0);
                endpoint.set_disk_index(disk_index);
                endpoints.push(endpoint);
            }
            pools.push(PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set,
                endpoints: Endpoints::from(endpoints),
                cmd_line: format!("{name}-pool-{pool_index}"),
                platform: "test".to_string(),
            });
        }

        let endpoint_pools = EndpointServerPools(pools);
        let instance_ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        crate::store::init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
            .await
            .expect("multi-pool local disks should initialize");
        let shutdown = CancellationToken::new();
        let store = ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("test address should parse"),
            endpoint_pools,
            shutdown.clone(),
            instance_ctx,
        )
        .await
        .expect("multi-pool store should initialize");
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        (temp_dir, store, shutdown)
    }

    struct LifecycleDeleteAllFailureGuard;

    impl Drop for LifecycleDeleteAllFailureGuard {
        fn drop(&mut self) {
            *LIFECYCLE_DELETE_ALL_TEST_FAILURE
                .lock()
                .expect("lifecycle delete-all failure hook should not poison") = None;
        }
    }

    async fn seed_multi_pool_delete_all(store: &Arc<ECStore>, bucket: &str, object: &str) -> ObjectOptions {
        let trigger_id = Uuid::new_v4();
        for (pool_index, pool) in store.pools.iter().enumerate() {
            let mut history_reader = PutObjReader::from_vec(format!("{object}-history-{pool_index}").into_bytes());
            pool.put_object(
                bucket,
                object,
                &mut history_reader,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(Uuid::new_v4().to_string()),
                    mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
                    ..Default::default()
                },
            )
            .await
            .expect("history should be stored");
            let mut trigger_reader = PutObjReader::from_vec(format!("{object}-trigger-{pool_index}").into_bytes());
            pool.put_object(
                bucket,
                object,
                &mut trigger_reader,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(trigger_id.to_string()),
                    mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2)),
                    ..Default::default()
                },
            )
            .await
            .expect("shared trigger should be stored");
        }
        let mut opts = ObjectOptions {
            delete_prefix: true,
            delete_prefix_object: true,
            versioned: true,
            lifecycle_delete_all: Some(crate::object_api::LifecycleDeleteAllRequest {
                version_id: Some(trigger_id),
                delete_marker: false,
                action: rustfs_common::metrics::IlmAction::DeleteAllVersionsAction,
                rule_id: "rule".to_string(),
                phase: crate::object_api::LifecycleDeleteAllPhase::Preflight,
            }),
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
            ))),
            delete_replication_config_snapshot: Some(Arc::new(
                crate::bucket::replication::DeleteReplicationConfigSnapshot::default(),
            )),
            ..Default::default()
        };
        opts.ensure_lifecycle_delete_all_journal();
        opts
    }

    async fn ordinary_version_count(store: &ECStore, pool_index: usize, bucket: &str, object: &str) -> usize {
        store.pools[pool_index].disk_set[0]
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("pool metadata should load")
            .map(|versions| {
                versions
                    .versions
                    .iter()
                    .filter(|version| !version.tier_free_version())
                    .count()
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn delete_prefix_attempts_later_pools_after_an_earlier_pool_error() {
        let (_temp_dir, store, shutdown) = setup_multi_pool_test_store("delete-prefix", &[2, 4]).await;
        let bucket = format!("delete-prefix-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created in both pools");

        let first_pool_disks = store.pools[0].disk_set[0].disks.read().await.clone();
        for disk in first_pool_disks.iter().flatten() {
            disk.write_all(&bucket, "blocked", bytes::Bytes::from_static(b"not-a-directory"))
                .await
                .expect("first pool should contain a blocking parent file");
        }
        let later_pool_disks = store.pools[1].disk_set[0].disks.read().await.clone();
        let later_data_disk = later_pool_disks[0].clone().expect("later pool should have its first disk");
        later_data_disk
            .write_all(&bucket, "blocked/prefix/object", bytes::Bytes::from_static(b"data"))
            .await
            .expect("later pool should contain the prefix on its available disk");
        *store.pools[1].disk_set[0].disks.write().await = vec![Some(later_data_disk.clone()), None, None, None];

        let err = store
            .delete_prefix(&bucket, "blocked/prefix", &ObjectOptions::default())
            .await
            .expect_err("the first pool's hard error must be returned");

        assert!(
            matches!(err, StorageError::PrefixAccessDenied(ref error_bucket, ref error_prefix)
                if error_bucket == &bucket && error_prefix == "blocked/prefix"),
            "unexpected multi-pool delete error: {err:?}"
        );
        assert!(matches!(
            later_data_disk.read_all(&bucket, "blocked/prefix/object").await,
            Err(DiskError::FileNotFound)
        ));

        *store.pools[1].disk_set[0].disks.write().await = later_pool_disks.clone();
        for disk in first_pool_disks.iter().flatten() {
            disk.write_all(&bucket, "second-blocked", bytes::Bytes::from_static(b"not-a-directory"))
                .await
                .expect("first pool should contain a second blocking parent file");
        }
        for disk in later_pool_disks.iter().flatten() {
            disk.write_all(&bucket, "second-blocked/prefix/object", bytes::Bytes::from_static(b"data"))
                .await
                .expect("later pool should contain the second prefix");
        }
        let err = store
            .delete_prefix(&bucket, "second-blocked/prefix", &ObjectOptions::default())
            .await
            .expect_err("a successful later pool must not override the first pool's hard error");
        assert!(
            matches!(err, StorageError::PrefixAccessDenied(ref error_bucket, ref error_prefix)
                if error_bucket == &bucket && error_prefix == "second-blocked/prefix"),
            "unexpected hard-error plus success result: {err:?}"
        );
        for disk in later_pool_disks.iter().flatten() {
            assert!(matches!(
                disk.read_all(&bucket, "second-blocked/prefix/object").await,
                Err(DiskError::FileNotFound)
            ));
        }

        for disk in later_pool_disks.iter().flatten() {
            disk.delete_volume(&bucket, true)
                .await
                .expect("the bucket should be absent from the later pool");
        }
        let healthy_object = "healthy/prefix/object";
        for disk in first_pool_disks.iter().flatten() {
            disk.write_all(&bucket, healthy_object, bytes::Bytes::from_static(b"data"))
                .await
                .expect("the first pool should contain the healthy prefix");
        }
        store
            .delete_prefix(&bucket, "healthy/prefix", &ObjectOptions::default())
            .await
            .expect("one successful pool should make a partially missing bucket idempotent");
        for disk in first_pool_disks.iter().flatten() {
            assert!(matches!(disk.read_all(&bucket, healthy_object).await, Err(DiskError::FileNotFound)));
        }

        let missing_bucket = format!("delete-prefix-missing-{}", Uuid::new_v4().simple());
        let err = store
            .delete_prefix(&missing_bucket, "missing/prefix", &ObjectOptions::default())
            .await
            .expect_err("a bucket missing from every pool must remain an error");
        assert_eq!(err, StorageError::BucketNotFound(missing_bucket));

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn lifecycle_delete_all_history_failure_preserves_trigger_and_retry_converges() {
        let (_temp_dir, store, shutdown) = setup_multi_pool_test_store("lifecycle-delete-all", &[4, 4]).await;
        let bucket = format!("lifecycle-delete-all-{}", Uuid::new_v4().simple());
        let object = "object";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created in both pools");

        for pool_index in 0..2 {
            let mut reader = PutObjReader::from_vec(format!("pool-{pool_index}-history").into_bytes());
            store.pools[pool_index]
                .put_object(
                    &bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("historical version should be stored");
        }
        let marker = store.pools[0]
            .delete_object(
                &bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("trigger marker should be stored in the first pool");
        let marker_id = marker.version_id.expect("trigger marker should have a version id");
        let mut opts = ObjectOptions {
            delete_prefix: true,
            delete_prefix_object: true,
            versioned: true,
            lifecycle_delete_all: Some(crate::object_api::LifecycleDeleteAllRequest {
                version_id: Some(marker_id),
                delete_marker: true,
                action: rustfs_common::metrics::IlmAction::DelMarkerDeleteAllVersionsAction,
                rule_id: "rule".to_string(),
                phase: crate::object_api::LifecycleDeleteAllPhase::Preflight,
            }),
            object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
            ))),
            delete_replication_config_snapshot: Some(Arc::new(
                crate::bucket::replication::DeleteReplicationConfigSnapshot::default(),
            )),
            ..Default::default()
        };
        opts.ensure_lifecycle_delete_all_journal();

        let _failure_guard = LifecycleDeleteAllFailureGuard;
        *LIFECYCLE_DELETE_ALL_TEST_FAILURE
            .lock()
            .expect("lifecycle delete-all failure hook should not poison") =
            Some((crate::object_api::LifecycleDeleteAllPhase::History, 1));
        let err = store
            .delete_prefix(&bucket, object, &opts)
            .await
            .expect_err("a later pool history failure must stop before trigger deletion");
        assert_eq!(err, StorageError::PreconditionFailed);
        assert!(
            opts.lifecycle_delete_all_journal()
                .expect("delete-all journal should be initialized")
                .lock()
                .mutation_started()
        );

        let first_pool = store.pools[0].disk_set[0]
            .load_file_info_versions_exact(&bucket, object)
            .await
            .expect("first pool metadata should load")
            .expect("the trigger should remain");
        let first_pool_ordinary: Vec<&FileInfo> = first_pool
            .versions
            .iter()
            .filter(|version| !version.tier_free_version())
            .collect();
        assert_eq!(first_pool_ordinary.len(), 1);
        assert_eq!(first_pool_ordinary[0].version_id, Some(marker_id));
        assert!(first_pool_ordinary[0].deleted);

        *LIFECYCLE_DELETE_ALL_TEST_FAILURE
            .lock()
            .expect("lifecycle delete-all failure hook should not poison") = None;
        store
            .delete_prefix(&bucket, object, &opts)
            .await
            .expect("retry should delete remaining history and its trigger owner");
        for pool in &store.pools {
            assert!(
                pool.disk_set[0]
                    .load_file_info_versions_exact(&bucket, object)
                    .await
                    .expect("pool metadata should load after retry")
                    .is_none(),
                "all ordinary versions should be removed after retry"
            );
        }

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn lifecycle_delete_all_phase_failures_preserve_barriers_and_retry() {
        let (_temp_dir, store, shutdown) = setup_multi_pool_test_store("lifecycle-delete-all-phases", &[4, 4]).await;
        let bucket = format!("lifecycle-delete-all-phases-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created in both pools");
        let _failure_guard = LifecycleDeleteAllFailureGuard;

        for (object, phase, expected_counts, mutation_started) in [
            ("preflight-failure", crate::object_api::LifecycleDeleteAllPhase::Preflight, [2, 2], false),
            (
                "final-preflight-failure",
                crate::object_api::LifecycleDeleteAllPhase::FinalPreflight,
                [1, 1],
                true,
            ),
            ("trigger-failure", crate::object_api::LifecycleDeleteAllPhase::Trigger, [0, 1], true),
        ] {
            let opts = seed_multi_pool_delete_all(&store, &bucket, object).await;
            *LIFECYCLE_DELETE_ALL_TEST_FAILURE
                .lock()
                .expect("lifecycle delete-all failure hook should not poison") = Some((phase, 1));
            let err = store
                .delete_prefix(&bucket, object, &opts)
                .await
                .expect_err("injected phase failure should stop the transaction");
            assert_eq!(err, StorageError::PreconditionFailed);
            assert_eq!(
                opts.lifecycle_delete_all_journal()
                    .expect("delete-all journal should be initialized")
                    .lock()
                    .mutation_started(),
                mutation_started
            );
            assert_eq!(ordinary_version_count(&store, 0, &bucket, object).await, expected_counts[0]);
            assert_eq!(ordinary_version_count(&store, 1, &bucket, object).await, expected_counts[1]);

            *LIFECYCLE_DELETE_ALL_TEST_FAILURE
                .lock()
                .expect("lifecycle delete-all failure hook should not poison") = None;
            store
                .delete_prefix(&bucket, object, &opts)
                .await
                .expect("retry should converge after the injected failure is removed");
            assert_eq!(ordinary_version_count(&store, 0, &bucket, object).await, 0);
            assert_eq!(ordinary_version_count(&store, 1, &bucket, object).await, 0);
        }

        shutdown.cancel();
    }

    fn assert_backend_layout_empty(info: &rustfs_madmin::BackendInfo) {
        assert!(info.standard_sc_parities.is_empty());
        assert!(info.standard_sc_data.is_empty());
        assert_eq!(info.standard_sc_parity, None);
        assert!(info.rr_sc_parities.is_empty());
        assert!(info.rr_sc_data.is_empty());
        assert_eq!(info.rr_sc_parity, None);
    }

    #[test]
    fn build_backend_info_reports_heterogeneous_automatic_config_in_pool_order() {
        let config = lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("automatic storage class should resolve");

        let info = build_backend_info(&config, &[4, 2], &[2, 1], &[7, 3]);

        assert!(matches!(info.backend_type, rustfs_madmin::BackendByte::Erasure));
        assert_eq!(info.standard_sc_parities, vec![2, 1]);
        assert_eq!(info.standard_sc_data, vec![2, 1]);
        assert_eq!(info.standard_sc_parity, None);
        assert_eq!(info.rr_sc_parities, vec![1, 1]);
        assert_eq!(info.rr_sc_data, vec![3, 1]);
        assert_eq!(info.rr_sc_parity, Some(1));
        assert_eq!(info.drives_per_set, vec![4, 2]);
        assert_eq!(info.total_sets, vec![7, 3]);
    }

    #[test]
    fn build_backend_info_keeps_truthful_homogeneous_scalar() {
        let mut kvs = KVS::new();
        kvs.insert(CLASS_STANDARD.to_string(), "EC:2".to_string());
        let config =
            lookup_config_for_pools_without_env(&kvs, &[4, 6]).expect("explicit storage class should resolve for every pool");

        let info = build_backend_info(&config, &[4, 6], &[2, 3], &[1, 1]);

        assert_eq!(info.standard_sc_parities, vec![2, 2]);
        assert_eq!(info.standard_sc_data, vec![2, 4]);
        assert_eq!(info.standard_sc_parity, Some(2));
        assert_eq!(info.rr_sc_parities, vec![1, 1]);
        assert_eq!(info.rr_sc_data, vec![3, 5]);
        assert_eq!(info.rr_sc_parity, Some(1));
    }

    #[test]
    fn build_backend_info_reports_single_disk_pool_without_inventing_parity() {
        let config = lookup_config_for_pools_without_env(&KVS::new(), &[4, 1]).expect("single disk pool should resolve");

        let info = build_backend_info(&config, &[4, 1], &[2, 0], &[1, 1]);

        assert_eq!(info.standard_sc_parities, vec![2, 0]);
        assert_eq!(info.standard_sc_data, vec![2, 1]);
        assert_eq!(info.standard_sc_parity, None);
        assert_eq!(info.rr_sc_parities, vec![1, 0]);
        assert_eq!(info.rr_sc_data, vec![3, 1]);
        assert_eq!(info.rr_sc_parity, None);
    }

    #[test]
    fn build_backend_info_uses_pool_defaults_only_when_truly_uninitialized() {
        let info = build_backend_info(&Default::default(), &[4, 2], &[1, 0], &[1, 1]);

        assert_eq!(info.standard_sc_parities, vec![1, 0]);
        assert_eq!(info.standard_sc_data, vec![3, 2]);
        assert_eq!(info.standard_sc_parity, None);
        assert!(info.rr_sc_parities.is_empty());
        assert!(info.rr_sc_data.is_empty());
        assert_eq!(info.rr_sc_parity, None);
    }

    #[test]
    fn build_backend_info_fails_closed_on_initialized_snapshot_mismatch() {
        let config = lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("automatic storage class should resolve");

        let info = build_backend_info(&config, &[4, 6], &[1, 2], &[1, 1]);

        assert_backend_layout_empty(&info);
        assert_eq!(info.drives_per_set, vec![4, 6]);
        assert_eq!(info.total_sets, vec![1, 1]);
    }

    #[test]
    fn build_backend_info_expands_valid_legacy_scalar_snapshot() {
        let mut kvs = KVS::new();
        kvs.insert(CLASS_STANDARD.to_string(), "EC:2".to_string());
        kvs.insert(CLASS_RRS.to_string(), "EC:1".to_string());
        let current =
            lookup_config_for_pools_without_env(&kvs, &[4, 6]).expect("distinct legacy scalars should resolve for every pool");
        let encoded = serde_json::to_string(&current).expect("config should serialize");
        let legacy: storageclass::Config = serde_json::from_str(&encoded).expect("legacy scalar config should deserialize");

        let info = build_backend_info(&legacy, &[4, 6], &[2, 3], &[1, 1]);

        assert_eq!(info.standard_sc_parities, vec![2, 2]);
        assert_eq!(info.standard_sc_data, vec![2, 4]);
        assert_eq!(info.standard_sc_parity, Some(2));
        assert_eq!(info.rr_sc_parities, vec![1, 1]);
        assert_eq!(info.rr_sc_data, vec![3, 5]);
        assert_eq!(info.rr_sc_parity, Some(1));
    }

    #[test]
    fn build_backend_info_rejects_legacy_scalar_invalid_for_later_pool() {
        let current = lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("automatic config should resolve");
        let encoded = serde_json::to_string(&current).expect("config should serialize");
        let legacy: storageclass::Config = serde_json::from_str(&encoded).expect("legacy scalar config should deserialize");

        let info = build_backend_info(&legacy, &[4, 2], &[2, 1], &[1, 1]);

        assert_backend_layout_empty(&info);
    }

    #[test]
    fn build_backend_info_never_reports_invalid_geometry() {
        let config = storageclass::Config::default();

        assert_backend_layout_empty(&build_backend_info(&config, &[4, 2], &[5, 1], &[1, 1]));
        assert_backend_layout_empty(&build_backend_info(&config, &[0], &[0], &[1]));
        assert_backend_layout_empty(&build_backend_info(&config, &[4], &[3], &[1]));
    }

    #[test]
    fn build_backend_info_rejects_mismatched_topology_lengths() {
        let config = lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("automatic storage class should resolve");

        assert_backend_layout_empty(&build_backend_info(&config, &[4, 2], &[2], &[1, 1]));
        assert_backend_layout_empty(&build_backend_info(&config, &[4, 2], &[2, 1], &[1]));

        let empty = build_backend_info(&Default::default(), &[], &[], &[]);
        assert_backend_layout_empty(&empty);
        assert!(empty.drives_per_set.is_empty());
        assert!(empty.total_sets.is_empty());
    }

    #[test]
    fn build_backend_info_uses_one_complete_arc_swap_snapshot() {
        let old = lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("old config should resolve");
        let mut new_kvs = KVS::new();
        new_kvs.insert(CLASS_STANDARD.to_string(), "EC:1".to_string());
        let new = lookup_config_for_pools_without_env(&new_kvs, &[4, 2]).expect("new config should resolve");
        let snapshots = ArcSwap::from_pointee(old);
        let held_old = snapshots.load_full();
        snapshots.store(Arc::new(new));

        let old_info = build_backend_info(&held_old, &[4, 2], &[2, 1], &[1, 1]);
        let new_info = build_backend_info(&snapshots.load_full(), &[4, 2], &[2, 1], &[1, 1]);

        assert_eq!(old_info.standard_sc_parities, vec![2, 1]);
        assert_eq!(old_info.standard_sc_data, vec![2, 1]);
        assert_eq!(old_info.standard_sc_parity, None);
        assert_eq!(old_info.rr_sc_parities, vec![1, 1]);
        assert_eq!(new_info.standard_sc_parities, vec![1, 1]);
        assert_eq!(new_info.standard_sc_data, vec![3, 1]);
        assert_eq!(new_info.standard_sc_parity, Some(1));
        assert_eq!(new_info.rr_sc_parities, vec![1, 1]);
    }

    fn object_info_with_mod_time(unix_ts: i64, delete_marker: bool) -> ObjectInfo {
        ObjectInfo {
            mod_time: Some(OffsetDateTime::from_unix_timestamp(unix_ts).expect("operation should succeed")),
            delete_marker,
            ..Default::default()
        }
    }

    fn object_info_with_identity(unix_ts: i64, delete_marker: bool, version_id: Uuid, etag: Option<String>) -> ObjectInfo {
        ObjectInfo {
            version_id: Some(version_id),
            etag,
            ..object_info_with_mod_time(unix_ts, delete_marker)
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

        let (info, idx) = resolve_latest_object_info_candidates(candidates, "bucket", "object", &ObjectOptions::default())
            .expect("operation should succeed");

        assert_eq!(idx, 1);
        assert!(info.delete_marker);
    }

    #[test]
    fn resolve_latest_object_info_candidates_prefers_higher_pool_idx_on_equal_mod_time_for_equivalent_candidates() {
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

        let (_, idx) = resolve_latest_object_info_candidates(candidates, "bucket", "object", &ObjectOptions::default())
            .expect("operation should succeed");

        assert_eq!(idx, 1);
    }

    #[test]
    fn resolve_latest_object_info_candidates_keeps_index_fallback_for_fully_equivalent_identities() {
        let candidates = vec![
            LatestObjectInfoCandidate {
                info: Some(object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()))),
                idx: 2,
                err: None,
            },
            LatestObjectInfoCandidate {
                info: Some(object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()))),
                idx: 7,
                err: None,
            },
        ];

        let (info, idx) = resolve_latest_object_info_candidates(candidates, "bucket", "object", &ObjectOptions::default())
            .expect("equivalent replicas must resolve deterministically");

        assert_eq!(idx, 7);
        assert_eq!(info.version_id, Some(Uuid::from_u128(1)));
    }

    #[test]
    fn resolve_latest_object_info_candidates_rejects_equal_time_version_id_conflict() {
        let candidates = vec![
            LatestObjectInfoCandidate {
                info: Some(object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()))),
                idx: 0,
                err: None,
            },
            LatestObjectInfoCandidate {
                info: Some(object_info_with_identity(10, false, Uuid::from_u128(2), Some("etag-a".to_string()))),
                idx: 1,
                err: None,
            },
        ];

        let err = resolve_latest_object_info_candidates(candidates, "bucket", "object", &ObjectOptions::default())
            .expect_err("divergent version ids must not silently resolve to the higher pool index");

        assert_eq!(err, Error::ErasureReadQuorum);
    }

    #[test]
    fn resolve_latest_object_info_candidates_rejects_equal_time_etag_conflict() {
        let candidates = vec![
            LatestObjectInfoCandidate {
                info: Some(object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-old".to_string()))),
                idx: 0,
                err: None,
            },
            LatestObjectInfoCandidate {
                info: Some(object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-new".to_string()))),
                idx: 1,
                err: None,
            },
        ];

        let err = resolve_latest_object_info_candidates(candidates, "bucket", "object", &ObjectOptions::default())
            .expect_err("divergent etags must not silently resolve to the higher pool index");

        assert_eq!(err, Error::ErasureReadQuorum);
    }

    #[test]
    fn resolve_latest_object_info_candidates_rejects_equal_time_delete_marker_conflict() {
        let candidates = vec![
            LatestObjectInfoCandidate {
                info: Some(object_info_with_identity(10, false, Uuid::from_u128(1), None)),
                idx: 0,
                err: None,
            },
            LatestObjectInfoCandidate {
                info: Some(object_info_with_identity(10, true, Uuid::from_u128(1), Some("etag-a".to_string()))),
                idx: 1,
                err: None,
            },
        ];

        let err = resolve_latest_object_info_candidates(candidates, "bucket", "object", &ObjectOptions::default())
            .expect_err("a delete marker tied with a live version must not be masked by the pool index");

        assert_eq!(err, Error::ErasureReadQuorum);
    }

    fn assert_equal_time_identity_conflict(left: ObjectInfo, right: ObjectInfo) {
        let err = resolve_latest_object_info_candidates(
            vec![
                LatestObjectInfoCandidate {
                    info: Some(left),
                    idx: 0,
                    err: None,
                },
                LatestObjectInfoCandidate {
                    info: Some(right),
                    idx: 1,
                    err: None,
                },
            ],
            "bucket",
            "object",
            &ObjectOptions::default(),
        )
        .expect_err("equal-time identity divergence must fail closed");

        assert_eq!(err, Error::ErasureReadQuorum);
    }

    #[test]
    fn resolve_latest_object_info_candidates_rejects_equal_time_payload_identity_conflicts() {
        let base = object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()));

        let mut data_dir = base.clone();
        data_dir.data_dir = Some(Uuid::from_u128(2));
        assert_equal_time_identity_conflict(base.clone(), data_dir);

        let mut size = base.clone();
        size.size = 1;
        assert_equal_time_identity_conflict(base.clone(), size);

        let mut actual_size = base.clone();
        actual_size.actual_size = 1;
        assert_equal_time_identity_conflict(base.clone(), actual_size);

        let mut checksum = base.clone();
        checksum.checksum = Some(bytes::Bytes::from_static(b"checksum"));
        assert_equal_time_identity_conflict(base.clone(), checksum);

        let mut parts = base.clone();
        parts.parts = std::sync::Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
            etag: "part-etag".to_string(),
            number: 1,
            size: 1,
            ..Default::default()
        }]);
        assert_equal_time_identity_conflict(base.clone(), parts);

        let mut transition = base;
        transition.transitioned_object.tier = "tier-a".to_string();
        assert_equal_time_identity_conflict(
            object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string())),
            transition,
        );
    }

    #[test]
    fn resolve_latest_object_info_candidates_accepts_internal_metadata_aliases() {
        let base = object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()));
        let mut rustfs_alias = base.clone();
        rustfs_alias.user_defined = std::sync::Arc::new(std::collections::HashMap::from([(
            "x-rustfs-internal-compression".to_string(),
            "zstd".to_string(),
        )]));
        let mut minio_alias = base.clone();
        minio_alias.user_defined = std::sync::Arc::new(std::collections::HashMap::from([(
            "X-MINIO-INTERNAL-COMPRESSION".to_string(),
            "zstd".to_string(),
        )]));

        let (_, idx) = resolve_latest_object_info_candidates(
            vec![
                LatestObjectInfoCandidate {
                    info: Some(rustfs_alias),
                    idx: 0,
                    err: None,
                },
                LatestObjectInfoCandidate {
                    info: Some(minio_alias),
                    idx: 1,
                    err: None,
                },
            ],
            "bucket",
            "object",
            &ObjectOptions::default(),
        )
        .expect("same-value internal aliases should resolve");
        assert_eq!(idx, 1);

        let mut dual_alias = base.clone();
        dual_alias.user_defined = std::sync::Arc::new(std::collections::HashMap::from([
            ("x-rustfs-internal-compression".to_string(), "zstd".to_string()),
            ("x-minio-internal-compression".to_string(), "zstd".to_string()),
        ]));
        let mut single_alias = base;
        single_alias.user_defined = std::sync::Arc::new(std::collections::HashMap::from([(
            "x-rustfs-internal-compression".to_string(),
            "zstd".to_string(),
        )]));

        let (_, idx) = resolve_latest_object_info_candidates(
            vec![
                LatestObjectInfoCandidate {
                    info: Some(dual_alias),
                    idx: 0,
                    err: None,
                },
                LatestObjectInfoCandidate {
                    info: Some(single_alias),
                    idx: 1,
                    err: None,
                },
            ],
            "bucket",
            "object",
            &ObjectOptions::default(),
        )
        .expect("dual-key and single-key internal metadata should resolve");
        assert_eq!(idx, 1);
    }

    #[test]
    fn resolve_latest_object_info_candidates_rejects_different_internal_metadata_alias_values() {
        let base = object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()));
        let mut rustfs_alias = base.clone();
        rustfs_alias.user_defined = std::sync::Arc::new(std::collections::HashMap::from([(
            "x-rustfs-internal-compression".to_string(),
            "zstd".to_string(),
        )]));
        let mut minio_alias = base;
        minio_alias.user_defined = std::sync::Arc::new(std::collections::HashMap::from([(
            "x-minio-internal-compression".to_string(),
            "snappy".to_string(),
        )]));

        assert_equal_time_identity_conflict(rustfs_alias, minio_alias);
    }

    #[test]
    fn resolve_latest_object_info_candidates_preserves_dynamic_internal_metadata_identity_case() {
        for suffix_prefix in ["replication-reset-", "replication-delete-marker-version-"] {
            let base = object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()));
            let mut rustfs_alias = base.clone();
            rustfs_alias.user_defined = std::sync::Arc::new(std::collections::HashMap::from([(
                format!(
                    "X-RUSTFS-INTERNAL-{}{suffix}",
                    suffix_prefix.to_uppercase(),
                    suffix = "arn:aws:s3:::Bucket"
                ),
                "value".to_string(),
            )]));
            let mut minio_alias = base.clone();
            minio_alias.user_defined = std::sync::Arc::new(std::collections::HashMap::from([(
                format!("x-minio-internal-{suffix_prefix}arn:aws:s3:::Bucket"),
                "value".to_string(),
            )]));

            let (_, idx) = resolve_latest_object_info_candidates(
                vec![
                    LatestObjectInfoCandidate {
                        info: Some(rustfs_alias.clone()),
                        idx: 0,
                        err: None,
                    },
                    LatestObjectInfoCandidate {
                        info: Some(minio_alias),
                        idx: 1,
                        err: None,
                    },
                ],
                "bucket",
                "object",
                &ObjectOptions::default(),
            )
            .expect("dynamic internal aliases with the same target should resolve");
            assert_eq!(idx, 1);

            let mut different_target_case = base;
            different_target_case.user_defined = std::sync::Arc::new(std::collections::HashMap::from([(
                format!("x-minio-internal-{suffix_prefix}arn:aws:s3:::bucket"),
                "value".to_string(),
            )]));

            assert_equal_time_identity_conflict(rustfs_alias, different_target_case);
        }
    }

    #[test]
    fn resolve_latest_object_info_candidates_rejects_conflicting_internal_metadata_aliases_in_one_candidate() {
        let base = object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()));
        let mut first = base.clone();
        first.user_defined = std::sync::Arc::new(std::collections::HashMap::from([
            ("x-rustfs-internal-compression".to_string(), "zstd".to_string()),
            ("x-minio-internal-compression".to_string(), "snappy".to_string()),
        ]));
        let mut second = base;
        second.user_defined = first.user_defined.clone();

        assert_equal_time_identity_conflict(first, second);
    }

    #[test]
    fn resolve_latest_object_info_candidates_rejects_replication_identity_conflict() {
        let base = object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()));

        let mut replication = base.clone();
        replication.replication_status_internal = Some("PENDING".to_string());
        replication.replication_status = ReplicationStatusType::Pending;
        assert_equal_time_identity_conflict(base.clone(), replication);

        let mut purge = base.clone();
        purge.version_purge_status_internal = Some("PENDING".to_string());
        purge.version_purge_status = VersionPurgeStatusType::Pending;
        assert_equal_time_identity_conflict(base.clone(), purge);

        let mut decision = base;
        decision.replication_decision = "replicate".to_string();
        assert_equal_time_identity_conflict(
            object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string())),
            decision,
        );
    }

    #[test]
    fn resolve_latest_object_info_candidates_rejects_none_vs_unix_epoch_mod_time() {
        let mut without_mod_time = object_info_with_identity(0, false, Uuid::from_u128(1), Some("etag-a".to_string()));
        without_mod_time.mod_time = None;
        let with_unix_epoch = object_info_with_identity(0, false, Uuid::from_u128(1), Some("etag-a".to_string()));

        assert_equal_time_identity_conflict(without_mod_time, with_unix_epoch);
    }

    #[test]
    fn resolve_latest_object_info_candidates_ignores_older_identity_conflicts() {
        let latest = object_info_with_identity(20, false, Uuid::from_u128(1), Some("etag-latest".to_string()));
        let mut older = object_info_with_identity(10, true, Uuid::from_u128(2), Some("etag-old".to_string()));
        older.data_dir = Some(Uuid::from_u128(2));

        let (info, idx) = resolve_latest_object_info_candidates(
            vec![
                LatestObjectInfoCandidate {
                    info: Some(latest),
                    idx: 0,
                    err: None,
                },
                LatestObjectInfoCandidate {
                    info: Some(older),
                    idx: 9,
                    err: None,
                },
            ],
            "bucket",
            "object",
            &ObjectOptions::default(),
        )
        .expect("older identity divergence must not affect the latest candidate");

        assert_eq!(idx, 0);
        assert_eq!(
            info.mod_time,
            Some(OffsetDateTime::from_unix_timestamp(20).expect("operation should succeed"))
        );
    }

    #[test]
    fn resolve_latest_object_info_candidates_ignores_not_found_pools_when_resolving() {
        let candidates = vec![
            LatestObjectInfoCandidate {
                info: Some(object_info_with_identity(10, false, Uuid::from_u128(1), Some("etag-a".to_string()))),
                idx: 0,
                err: None,
            },
            LatestObjectInfoCandidate {
                info: None,
                idx: 1,
                err: Some(Error::ObjectNotFound("bucket".to_string(), "object".to_string())),
            },
        ];

        let (info, idx) = resolve_latest_object_info_candidates(candidates, "bucket", "object", &ObjectOptions::default())
            .expect("not-found pools must not block resolution of found candidates");

        assert_eq!(idx, 0);
        assert_eq!(info.version_id, Some(Uuid::from_u128(1)));
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

    fn reload_test_pool_status(decommission: Option<PoolDecommissionInfo>, last_update: time::OffsetDateTime) -> PoolStatus {
        PoolStatus {
            id: 0,
            cmd_line: "pool-0".to_string(),
            last_update,
            decommission,
        }
    }

    fn reload_test_pool_meta(pool: PoolStatus) -> PoolMeta {
        PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![pool],
            dont_save: false,
        }
    }

    async fn persist_reload_snapshot(store: &ECStore, snapshot: &PoolMeta) {
        snapshot
            .save(store.pools.clone())
            .await
            .expect("pool meta snapshot should persist to every pool");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn peer_pool_meta_reload_does_not_rollback_newer_local_states() {
        let (_temp_dir, store, shutdown) = setup_multi_pool_test_store("pool-meta-reload-stale", &[2]).await;

        let stale_time = OffsetDateTime::now_utc();
        let newer_time = stale_time + TimeDuration::seconds(30);
        let progressed_states: [(&str, PoolDecommissionInfo); 4] = [
            (
                "queued",
                PoolDecommissionInfo {
                    queued: true,
                    start_time: Some(stale_time),
                    ..Default::default()
                },
            ),
            (
                "canceled",
                PoolDecommissionInfo {
                    canceled: true,
                    start_time: Some(stale_time),
                    ..Default::default()
                },
            ),
            (
                "failed",
                PoolDecommissionInfo {
                    failed: true,
                    start_time: Some(stale_time),
                    ..Default::default()
                },
            ),
            (
                "complete",
                PoolDecommissionInfo {
                    complete: true,
                    start_time: Some(stale_time),
                    ..Default::default()
                },
            ),
        ];

        for (state_label, local_state) in progressed_states {
            {
                let mut pool_meta = store.pool_meta.write().await;
                *pool_meta = reload_test_pool_meta(reload_test_pool_status(Some(local_state.clone()), newer_time));
            }

            // A delayed peer message carries a snapshot that predates the local progression.
            let stale_snapshot = reload_test_pool_meta(reload_test_pool_status(
                Some(PoolDecommissionInfo {
                    start_time: Some(stale_time),
                    ..Default::default()
                }),
                stale_time,
            ));
            persist_reload_snapshot(&store, &stale_snapshot).await;

            let merged_newer = store.reload_pool_meta().await.expect("stale reload should succeed");
            assert!(
                !merged_newer,
                "a delayed reload must not report merged newer state for the {state_label} progression"
            );

            let pool_meta = store.pool_meta.read().await;
            let info = pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("local decommission state should survive a stale reload");
            assert_eq!(info.queued, local_state.queued, "{state_label} queued flag must not roll back");
            assert_eq!(info.canceled, local_state.canceled, "{state_label} canceled flag must not roll back");
            assert_eq!(info.failed, local_state.failed, "{state_label} failed flag must not roll back");
            assert_eq!(info.complete, local_state.complete, "{state_label} complete flag must not roll back");
            assert_eq!(
                pool_meta.pools[0].last_update, newer_time,
                "{state_label} progress timestamp must be kept"
            );
        }

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn peer_pool_meta_reload_merges_newer_state_and_is_idempotent_on_duplicate_delivery() {
        let (_temp_dir, store, shutdown) = setup_multi_pool_test_store("pool-meta-reload-duplicate", &[2]).await;

        let older_time = OffsetDateTime::now_utc();
        let newer_time = older_time + TimeDuration::seconds(30);

        {
            let mut pool_meta = store.pool_meta.write().await;
            *pool_meta = reload_test_pool_meta(reload_test_pool_status(
                Some(PoolDecommissionInfo {
                    items_decommissioned: 1,
                    ..Default::default()
                }),
                older_time,
            ));
        }
        let newer_snapshot = reload_test_pool_meta(reload_test_pool_status(
            Some(PoolDecommissionInfo {
                complete: true,
                items_decommissioned: 10,
                ..Default::default()
            }),
            newer_time,
        ));
        persist_reload_snapshot(&store, &newer_snapshot).await;

        let merged_newer = store.reload_pool_meta().await.expect("first reload should succeed");
        assert!(merged_newer, "a strictly newer persisted snapshot must merge");

        {
            let pool_meta = store.pool_meta.read().await;
            let info = pool_meta.pools[0].decommission.as_ref().expect("merged decommission state");
            assert!(info.complete);
            assert_eq!(info.items_decommissioned, 10);
            assert_eq!(pool_meta.pools[0].last_update, newer_time);
        }

        // Redelivering the same generation must be a no-op.
        let duplicate_merged = store.reload_pool_meta().await.expect("duplicate reload should succeed");
        assert!(!duplicate_merged, "a duplicate delivery must not re-apply merged state");

        {
            let pool_meta = store.pool_meta.read().await;
            let info = pool_meta.pools[0].decommission.as_ref().expect("merged decommission state");
            assert!(info.complete);
            assert_eq!(info.items_decommissioned, 10);
            assert_eq!(pool_meta.pools[0].last_update, newer_time);
        }

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn peer_pool_meta_reload_keeps_active_worker_progress_over_newer_snapshot() {
        let (_temp_dir, store, shutdown) = setup_multi_pool_test_store("pool-meta-reload-worker", &[2]).await;
        *store.decommission_cancelers.write().await = vec![Some(crate::core::pools::DecommissionCanceler::new_for_test(
            CancellationToken::new(),
        ))];

        let worker_time = OffsetDateTime::now_utc();
        let newer_time = worker_time + TimeDuration::seconds(30);

        {
            let mut pool_meta = store.pool_meta.write().await;
            *pool_meta = reload_test_pool_meta(reload_test_pool_status(
                Some(PoolDecommissionInfo {
                    start_time: Some(worker_time),
                    items_decommissioned: 10,
                    bytes_done: 1_024,
                    ..Default::default()
                }),
                worker_time,
            ));
        }
        // Even a strictly newer terminal snapshot must not override a live worker.
        let newer_terminal_snapshot = reload_test_pool_meta(reload_test_pool_status(
            Some(PoolDecommissionInfo {
                complete: true,
                ..Default::default()
            }),
            newer_time,
        ));
        persist_reload_snapshot(&store, &newer_terminal_snapshot).await;

        let merged_newer = store
            .reload_pool_meta()
            .await
            .expect("reload under an active worker should succeed");
        assert!(!merged_newer, "an active local worker must block snapshot replacement");

        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("worker progress should remain");
        assert!(!info.complete);
        assert_eq!(info.items_decommissioned, 10);
        assert_eq!(info.bytes_done, 1_024);
        assert_eq!(pool_meta.pools[0].last_update, worker_time);

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn peer_pool_meta_reload_fails_closed_when_persisted_metadata_is_missing() {
        let (temp_dir, store, shutdown) = setup_multi_pool_test_store("pool-meta-reload-missing", &[2]).await;

        let kept_time = OffsetDateTime::now_utc();
        {
            let mut pool_meta = store.pool_meta.write().await;
            *pool_meta = reload_test_pool_meta(reload_test_pool_status(
                Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
                kept_time,
            ));
        }
        // Persist first so the test controls exactly what exists on disk.
        persist_reload_snapshot(
            &store,
            &reload_test_pool_meta(reload_test_pool_status(
                Some(PoolDecommissionInfo {
                    complete: true,
                    ..Default::default()
                }),
                kept_time,
            )),
        )
        .await;

        let mut deleted_any = false;
        for disk_index in 0..2 {
            let pool_bin_dir = temp_dir
                .path()
                .join(format!("pool0-disk{disk_index}"))
                .join(crate::disk::RUSTFS_META_BUCKET)
                .join(crate::core::pools::POOL_META_NAME);
            if pool_bin_dir.exists() {
                tokio::fs::remove_dir_all(&pool_bin_dir)
                    .await
                    .expect("persisted pool metadata object dir should be removable");
                deleted_any = true;
            }
        }
        // The meta-bucket layout may nest objects per pool; fall back to removing
        // every pool.bin object directory below the temp root.
        if !deleted_any {
            panic!("no pool.bin found under {:?}", temp_dir.path());
        }

        let merged_newer = store
            .reload_pool_meta()
            .await
            .expect("reload with missing metadata should fail closed, not error");
        assert!(!merged_newer, "missing persisted metadata must not count as merged state");

        let pool_meta = store.pool_meta.read().await;
        let info = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("missing persisted metadata must not default local state away");
        assert!(info.complete);
        assert_eq!(pool_meta.pools[0].last_update, kept_time);

        shutdown.cancel();
    }
}
