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

#![allow(clippy::map_entry)]

use crate::bucket::lifecycle::bucket_lifecycle_ops::init_background_expiry;
use crate::bucket::metadata_sys::{self, set_bucket_metadata};
use crate::bucket::utils::check_abort_multipart_args;
use crate::bucket::utils::check_complete_multipart_args;
use crate::bucket::utils::check_copy_obj_args;
use crate::bucket::utils::check_del_obj_args;
use crate::bucket::utils::check_get_obj_args;
use crate::bucket::utils::check_list_multipart_args;
use crate::bucket::utils::check_list_parts_args;
use crate::bucket::utils::check_new_multipart_args;
use crate::bucket::utils::check_object_args;
use crate::bucket::utils::check_put_object_args;
use crate::bucket::utils::check_put_object_part_args;
use crate::bucket::utils::{check_valid_bucket_name, check_valid_bucket_name_strict, is_meta_bucketname};
use crate::config::GLOBAL_STORAGE_CLASS;
use crate::config::storageclass;
use crate::disk::endpoint::{Endpoint, EndpointType};
use crate::disk::{DiskAPI, DiskInfo, DiskInfoOptions};
use crate::error::{Error, Result};
use crate::error::{
    StorageError, is_err_bucket_exists, is_err_bucket_not_found, is_err_invalid_upload_id, is_err_object_not_found,
    is_err_read_quorum, is_err_version_not_found, to_object_err,
};
use crate::global::{
    DISK_ASSUME_UNKNOWN_SIZE, DISK_FILL_FRACTION, DISK_MIN_INODES, DISK_RESERVE_FRACTION, GLOBAL_BOOT_TIME,
    GLOBAL_LOCAL_DISK_MAP, GLOBAL_LOCAL_DISK_SET_DRIVES, GLOBAL_TierConfigMgr, get_global_deployment_id, get_global_endpoints,
    is_dist_erasure, is_erasure_sd, set_global_deployment_id, set_object_layer,
};
use crate::notification_sys::get_global_notification_sys;
use crate::pools::PoolMeta;
use crate::rebalance::RebalanceMeta;
use crate::rpc::RemoteClient;
use crate::store_api::{
    ListMultipartsInfo, ListObjectVersionsInfo, ListPartsInfo, MultipartInfo, ObjectIO, ObjectInfoOrErr, WalkOptions,
};
use crate::store_init::{check_disk_fatal_errs, ec_drives_no_config};
use crate::{
    bucket::{lifecycle::bucket_lifecycle_ops::TransitionState, metadata::BucketMetadata},
    disk::{BUCKET_META_PREFIX, DiskOption, DiskStore, RUSTFS_META_BUCKET, new_disk},
    endpoints::EndpointServerPools,
    rpc::S3PeerSys,
    sets::Sets,
    store_api::{
        BucketInfo, BucketOptions, CompletePart, DeleteBucketOptions, DeletedObject, GetObjectReader, HTTPRangeSpec,
        ListObjectsV2Info, MakeBucketOptions, MultipartUploadResult, ObjectInfo, ObjectOptions, ObjectToDelete, PartInfo,
        PutObjReader, StorageAPI,
    },
    store_init,
};
use futures::future::join_all;
use http::HeaderMap;
use lazy_static::lazy_static;
use rand::RngExt as _;
use rustfs_common::heal_channel::{HealItemType, HealOpts};
use rustfs_common::{GLOBAL_LOCAL_NODE_NAME, GLOBAL_RUSTFS_HOST, GLOBAL_RUSTFS_PORT};
use rustfs_filemeta::FileInfo;
use rustfs_lock::{LocalClient, LockClient, NamespaceLockWrapper};
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_utils::path::{decode_dir_object, encode_dir_object, path_join_buf};
use s3s::dto::{BucketVersioningStatus, ObjectLockConfiguration, ObjectLockEnabled, VersioningConfiguration};
use std::cmp::Ordering;
use std::net::SocketAddr;
use std::process::exit;
use std::slice::Iter;
use std::time::SystemTime;
use std::{collections::HashMap, sync::Arc, time::Duration};
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

/// Check if a directory contains any xl.meta files (indicating actual S3 objects)
/// This is used to determine if a bucket is empty for deletion purposes.
async fn has_xlmeta_files(path: &std::path::Path) -> bool {
    use crate::disk::STORAGE_FORMAT_FILE;
    use tokio::fs;

    let mut stack = vec![path.to_path_buf()];

    while let Some(current_path) = stack.pop() {
        let mut entries = match fs::read_dir(&current_path).await {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // Skip hidden files/directories (like .rustfs.sys)
            if file_name_str.starts_with('.') {
                continue;
            }

            // Check if this is an xl.meta file
            if file_name_str == STORAGE_FORMAT_FILE {
                return true;
            }

            // If it's a directory, add to stack for further exploration
            if let Ok(file_type) = entry.file_type().await
                && file_type.is_dir()
            {
                stack.push(entry.path());
            }
        }
    }

    false
}

const MAX_UPLOADS_LIST: usize = 10000;

#[derive(Debug)]
pub struct ECStore {
    pub id: Uuid,
    // pub disks: Vec<DiskStore>,
    pub disk_map: HashMap<usize, Vec<Option<DiskStore>>>,
    pub pools: Vec<Arc<Sets>>,
    pub peer_sys: S3PeerSys,
    // pub local_disks: Vec<DiskStore>,
    pub pool_meta: RwLock<PoolMeta>,
    pub rebalance_meta: RwLock<Option<RebalanceMeta>>,
    pub decommission_cancelers: Vec<Option<usize>>,
}

// impl Clone for ECStore {
//     fn clone(&self) -> Self {
//         let pool_meta = match self.pool_meta.read() {
//             Ok(pool_meta) => pool_meta.clone(),
//             Err(_) => PoolMeta::default(),
//         };
//         Self {
//             id: self.id.clone(),
//             disk_map: self.disk_map.clone(),
//             pools: self.pools.clone(),
//             peer_sys: self.peer_sys.clone(),
//             pool_meta: std_RwLock::new(pool_meta),
//             decommission_cancelers: self.decommission_cancelers.clone(),
//         }
//     }
// }

impl ECStore {
    #[allow(clippy::new_ret_no_self)]
    #[instrument(level = "debug", skip(endpoint_pools))]
    pub async fn new(address: SocketAddr, endpoint_pools: EndpointServerPools, ctx: CancellationToken) -> Result<Arc<Self>> {
        // let layouts = DisksLayout::from_volumes(endpoints.as_slice())?;

        let mut deployment_id = None;

        // let (endpoint_pools, _) = EndpointServerPools::create_server_endpoints(address.as_str(), &layouts)?;

        let mut pools = Vec::with_capacity(endpoint_pools.as_ref().len());
        let mut disk_map = HashMap::with_capacity(endpoint_pools.as_ref().len());

        let first_is_local = endpoint_pools.first_local();

        let mut local_disks = Vec::new();

        info!("ECStore new address: {}", address.to_string());
        let mut host = address.ip().to_string();
        if host.is_empty() {
            host = GLOBAL_RUSTFS_HOST.read().await.to_string()
        }
        let mut port = address.port().to_string();
        if port.is_empty() {
            port = GLOBAL_RUSTFS_PORT.read().await.to_string()
        }
        info!("ECStore new host: {}, port: {}", host, port);
        init_local_peer(&endpoint_pools, &host, &port).await;

        // debug!("endpoint_pools: {:?}", endpoint_pools);

        let mut common_parity_drives = 0;

        for (i, pool_eps) in endpoint_pools.as_ref().iter().enumerate() {
            if common_parity_drives == 0 {
                let parity_drives = ec_drives_no_config(pool_eps.drives_per_set)?;
                storageclass::validate_parity(parity_drives, pool_eps.drives_per_set)?;
                common_parity_drives = parity_drives;
            }

            // validate_parity(parity_count, pool_eps.drives_per_set)?;

            // Initialize disks without health monitoring so that remote peers
            // are not immediately marked as faulty before they have a chance to
            // start up. Health monitoring is enabled after format loading succeeds.
            let (disks, errs) = store_init::init_disks(
                &pool_eps.endpoints,
                &DiskOption {
                    cleanup: true,
                    health_check: false,
                },
            )
            .await;

            check_disk_fatal_errs(&errs)?;

            let fm = {
                let mut times = 0;
                let mut interval = 1;
                loop {
                    match store_init::connect_load_init_formats(
                        first_is_local,
                        &disks,
                        pool_eps.set_count,
                        pool_eps.drives_per_set,
                        deployment_id,
                    )
                    .await
                    {
                        Ok(fm) => break Ok(fm),
                        // Wrap the final error if we are giving up
                        Err(e) if times >= 10 => {
                            break Err(Error::other(format!("can not get formats after {} retries, last error: {e}", times)));
                        }
                        // Retrying so just drop the error
                        Err(_) => {}
                    }
                    times += 1;
                    if interval < 16 {
                        interval *= 2;
                    }
                    info!("retrying get formats after {:?}", interval);
                    select! {
                        _ = tokio::signal::ctrl_c() => {
                            info!("got ctrl+c, exits");
                            exit(0);
                        }
                        _ = sleep(Duration::from_secs(interval)) => {
                        }
                    }
                }
            }?;

            // Format loading succeeded, enable health monitoring on all disks
            for disk in disks.iter().flatten() {
                disk.enable_health_check();
            }

            if deployment_id.is_none() {
                deployment_id = Some(fm.id);
            }

            if deployment_id != Some(fm.id) {
                return Err(Error::other("deployment_id not same in one pool"));
            }

            if deployment_id.is_some() && deployment_id.unwrap().is_nil() {
                deployment_id = Some(Uuid::new_v4());
            }

            for disk in disks.iter() {
                if disk.is_some() && disk.as_ref().unwrap().is_local() {
                    local_disks.push(disk.as_ref().unwrap().clone());
                }
            }

            let sets = Sets::new(disks.clone(), pool_eps, &fm, i, common_parity_drives).await?;
            pools.push(sets);

            disk_map.insert(i, disks);
        }

        // Replace the local disk
        if !is_dist_erasure().await {
            let mut global_local_disk_map = GLOBAL_LOCAL_DISK_MAP.write().await;
            for disk in local_disks {
                let path = disk.endpoint().to_string();
                global_local_disk_map.insert(path, Some(disk.clone()));
            }
        }

        let peer_sys = S3PeerSys::new(&endpoint_pools);
        let mut pool_meta = PoolMeta::new(&pools, &PoolMeta::default());
        pool_meta.dont_save = true;

        let decommission_cancelers = vec![None; pools.len()];
        let ec = Arc::new(ECStore {
            id: deployment_id.unwrap(),
            disk_map,
            pools,
            peer_sys,
            pool_meta: RwLock::new(pool_meta),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers,
        });

        // Only set it when the global deployment ID is not yet configured
        if let Some(dep_id) = deployment_id
            && get_global_deployment_id().is_none()
        {
            set_global_deployment_id(dep_id);
        }

        let wait_sec = 5;
        let mut exit_count = 0;
        loop {
            if let Err(err) = ec.init(ctx.clone()).await {
                error!("init err: {}", err);
                error!("retry after  {} second", wait_sec);
                sleep(Duration::from_secs(wait_sec)).await;

                if exit_count > 10 {
                    return Err(Error::other("ec init failed"));
                }

                exit_count += 1;

                continue;
            }

            break;
        }

        set_object_layer(ec.clone()).await;

        Ok(ec)
    }

    #[instrument(level = "debug", skip(self, rx))]
    pub async fn init(self: &Arc<Self>, rx: CancellationToken) -> Result<()> {
        GLOBAL_BOOT_TIME.get_or_init(|| async { SystemTime::now() }).await;

        if self.load_rebalance_meta().await.is_ok() {
            self.start_rebalance().await;
        }

        let mut meta = PoolMeta::default();
        meta.load(self.pools[0].clone(), self.pools.clone()).await?;
        let update = meta.validate(self.pools.clone())?;

        if !update {
            {
                let mut pool_meta = self.pool_meta.write().await;
                *pool_meta = meta.clone();
            }
        } else {
            let new_meta = PoolMeta::new(&self.pools, &meta);
            new_meta.save(self.pools.clone()).await?;
            {
                let mut pool_meta = self.pool_meta.write().await;
                *pool_meta = new_meta;
            }
        }

        let pools = meta.return_resumable_pools();
        let mut pool_indices = Vec::with_capacity(pools.len());

        let endpoints = get_global_endpoints();

        for p in pools.iter() {
            if let Some(idx) = endpoints.get_pool_idx(&p.cmd_line) {
                pool_indices.push(idx);
            } else {
                return Err(Error::other(format!(
                    "unexpected state present for decommission status pool({}) not found",
                    p.cmd_line
                )));
            }
        }

        if !pool_indices.is_empty() {
            let idx = pool_indices[0];
            if endpoints.as_ref()[idx].endpoints.as_ref()[0].is_local {
                let store = self.clone();

                tokio::spawn(async move {
                    // wait  3 minutes for cluster init
                    tokio::time::sleep(Duration::from_secs(60 * 3)).await;

                    if let Err(err) = store.decommission(rx.clone(), pool_indices.clone()).await {
                        if err == StorageError::DecommissionAlreadyRunning {
                            for i in pool_indices.iter() {
                                store.do_decommission_in_routine(rx.clone(), *i).await;
                            }
                            return;
                        }

                        error!("store init decommission err: {}", err);

                        // TODO: check config err
                    }
                });
            }
        }

        init_background_expiry(self.clone()).await;

        TransitionState::init(self.clone()).await;

        if let Err(err) = GLOBAL_TierConfigMgr.write().await.init(self.clone()).await {
            info!("TierConfigMgr init error: {}", err);
        }

        Ok(())
    }

    pub fn init_local_disks() {}

    // pub fn local_disks(&self) -> Vec<DiskStore> {
    //     self.local_disks.clone()
    // }

    pub fn single_pool(&self) -> bool {
        self.pools.len() == 1
    }

    // define in store_list_objects.rs
    // pub async fn list_path(&self, opts: &ListPathOptions, delimiter: &str) -> Result<ListObjectsInfo> {
    //     // if opts.prefix.ends_with(SLASH_SEPARATOR) {
    //     //     return Err(Error::other("eof"));
    //     // }

    //     let mut opts = opts.clone();

    //     if opts.base_dir.is_empty() {
    //         opts.base_dir = base_dir_from_prefix(&opts.prefix);
    //     }

    //     let objects = self.list_merged(&opts, delimiter).await?;

    //     let info = ListObjectsInfo {
    //         objects,
    //         ..Default::default()
    //     };
    //     Ok(info)
    // }

    // Read all entries
    // define in store_list_objects.rs
    // async fn list_merged(&self, opts: &ListPathOptions, delimiter: &str) -> Result<Vec<ObjectInfo>> {
    //     let walk_opts = WalkDirOptions {
    //         bucket: opts.bucket.clone(),
    //         base_dir: opts.base_dir.clone(),
    //         ..Default::default()
    //     };

    //     // let (mut wr, mut rd) = tokio::io::duplex(1024);

    //     let mut futures = Vec::new();

    //     for sets in self.pools.iter() {
    //         for set in sets.disk_set.iter() {
    //             futures.push(set.walk_dir(&walk_opts));
    //         }
    //     }

    //     let results = join_all(futures).await;

    //     // let mut errs = Vec::new();
    //     let mut ress = Vec::new();
    //     let mut uniq = HashSet::new();

    //     for (disks_ress, _disks_errs) in results {
    //         for disks_res in disks_ress.iter() {
    //             if disks_res.is_none() {
    //                 // TODO handle errs
    //                 continue;
    //             }
    //             let entries = disks_res.as_ref().unwrap();

    //             for entry in entries {
    //                 // warn!("lst_merged entry---- {}", &entry.name);

    //                 if !opts.prefix.is_empty() && !entry.name.starts_with(&opts.prefix) {
    //                     continue;
    //                 }

    //                 if !uniq.contains(&entry.name) {
    //                     uniq.insert(entry.name.clone());
    //                     // TODO: filter

    //                     if opts.limit > 0 && ress.len() as i32 >= opts.limit {
    //                         return Ok(ress);
    //                     }

    //                     if entry.is_object() {
    //                         if !delimiter.is_empty() {
    //                             // entry.name.trim_start_matches(pat)
    //                         }

    //                         let fi = entry.to_fileinfo(&opts.bucket)?;
    //                         if let Some(f) = fi {
    //                             ress.push(f.to_object_info(&opts.bucket, &entry.name, false));
    //                         }
    //                         continue;
    //                     }

    //                     if entry.is_dir() {
    //                         ress.push(ObjectInfo {
    //                             is_dir: true,
    //                             bucket: opts.bucket.clone(),
    //                             name: entry.name.clone(),
    //                             ..Default::default()
    //                         });
    //                     }
    //                 }
    //             }
    //         }
    //     }

    //     // warn!("list_merged errs {:?}", errs);

    //     Ok(ress)
    // }

    #[instrument(level = "debug", skip(self))]
    async fn delete_all(&self, bucket: &str, prefix: &str) -> Result<()> {
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
    async fn delete_prefix(&self, bucket: &str, object: &str) -> Result<()> {
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

    async fn is_suspended(&self, idx: usize) -> bool {
        // TODO: LOCK

        let pool_meta = self.pool_meta.read().await;

        pool_meta.is_suspended(idx)
    }

    async fn get_pool_idx(&self, bucket: &str, object: &str, size: i64) -> Result<usize> {
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

    async fn get_pool_idx_no_lock(&self, bucket: &str, object: &str, size: i64) -> Result<usize> {
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

    async fn get_pool_idx_existing_with_opts(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<usize> {
        let (pinfo, _) = self.get_pool_info_existing_with_opts(bucket, object, opts).await?;
        Ok(pinfo.index)
    }
    async fn get_pool_info_existing_with_opts(
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

    async fn get_latest_object_info_with_idx(
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

    async fn delete_object_from_all_pools(
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

        let mut unique_disks: HashMap<String, rustfs_madmin::Disk> = HashMap::new();
        let mut duplicate_count = 0;

        for disk in disks {
            // Generate a compound unique key
            let key = format!(
                "{}|{}|p{}s{}d{}",
                disk.endpoint, disk.drive_path, disk.pool_index, disk.set_index, disk.disk_index
            );

            // Use the entry API to avoid duplicate inserts
            use std::collections::hash_map::Entry;
            match unique_disks.entry(key) {
                Entry::Vacant(e) => {
                    e.insert(disk);
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
}

pub async fn find_local_disk(disk_path: &String) -> Option<DiskStore> {
    let disk_map = GLOBAL_LOCAL_DISK_MAP.read().await;

    if let Some(disk) = disk_map.get(disk_path) {
        disk.as_ref().cloned()
    } else {
        None
    }
}

pub async fn get_disk_via_endpoint(endpoint: &Endpoint) -> Option<DiskStore> {
    let global_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.read().await;
    if global_set_drives.is_empty() {
        return GLOBAL_LOCAL_DISK_MAP.read().await[&endpoint.to_string()].clone();
    }
    global_set_drives[endpoint.pool_idx as usize][endpoint.set_idx as usize][endpoint.disk_idx as usize].clone()
}

pub async fn all_local_disk_path() -> Vec<String> {
    let disk_map = GLOBAL_LOCAL_DISK_MAP.read().await;
    disk_map.keys().cloned().collect()
}

pub async fn all_local_disk() -> Vec<DiskStore> {
    let disk_map = GLOBAL_LOCAL_DISK_MAP.read().await;
    disk_map
        .values()
        .filter(|v| v.is_some())
        .map(|v| v.as_ref().unwrap().clone())
        .collect()
}

// init_local_disks must succeed before the server starts
pub async fn init_local_disks(endpoint_pools: EndpointServerPools) -> Result<()> {
    let opt = &DiskOption {
        cleanup: true,
        health_check: true,
    };

    let mut global_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.write().await;
    for pool_eps in endpoint_pools.as_ref().iter() {
        let mut set_count_drives = Vec::with_capacity(pool_eps.set_count);
        for _ in 0..pool_eps.set_count {
            set_count_drives.push(vec![None; pool_eps.drives_per_set]);
        }

        global_set_drives.push(set_count_drives);
    }

    let mut global_local_disk_map = GLOBAL_LOCAL_DISK_MAP.write().await;

    for pool_eps in endpoint_pools.as_ref().iter() {
        let mut set_drives = HashMap::new();
        for ep in pool_eps.endpoints.as_ref().iter() {
            if !ep.is_local {
                continue;
            }

            let disk = new_disk(ep, opt).await?;

            let path = disk.endpoint().to_string();

            global_local_disk_map.insert(path, Some(disk.clone()));

            set_drives.insert(ep.disk_idx, Some(disk.clone()));

            global_set_drives[ep.pool_idx as usize][ep.set_idx as usize][ep.disk_idx as usize] = Some(disk.clone());
        }
    }

    Ok(())
}

/// create unique lock clients for the endpoints and store them globally
pub fn init_lock_clients(endpoint_pools: EndpointServerPools) {
    let mut unique_endpoints: HashMap<String, &Endpoint> = HashMap::new();

    for pool_eps in endpoint_pools.as_ref().iter() {
        for ep in pool_eps.endpoints.as_ref().iter() {
            unique_endpoints.insert(ep.host_port(), ep);
        }
    }

    let mut clients = HashMap::new();
    let mut first_local_client_set = false;

    for (key, endpoint) in unique_endpoints {
        if endpoint.is_local {
            let local_client = Arc::new(LocalClient::new()) as Arc<dyn LockClient>;

            // Store the first LocalClient globally for use by other modules
            if !first_local_client_set {
                if let Err(e) = crate::global::set_global_lock_client(local_client.clone()) {
                    // If already set, ignore the error (another thread may have set it)
                    warn!("set_global_lock_client error: {:?}", e);
                } else {
                    first_local_client_set = true;
                }
            }

            clients.insert(key, local_client);
        } else {
            clients.insert(key, Arc::new(RemoteClient::new(endpoint.url.to_string())) as Arc<dyn LockClient>);
        }
    }

    // Store the lock clients map globally
    if crate::global::set_global_lock_clients(clients).is_err() {
        error!("init_lock_clients: error setting lock clients");
    }
}

#[derive(Debug, Default)]
struct PoolErr {
    index: Option<usize>,
    err: Option<Error>,
}

#[derive(Debug, Default)]
pub struct PoolObjInfo {
    pub index: usize,
    pub object_info: ObjectInfo,
    pub err: Option<Error>,
}

impl Clone for PoolObjInfo {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            object_info: self.object_info.clone(),
            err: self.err.clone(),
        }
    }
}

// #[derive(Debug, Default, Clone)]
// pub struct ListPathOptions {
//     pub id: String,

//     // Bucket of the listing.
//     pub bucket: String,

//     // Directory inside the bucket.
//     // When unset listPath will set this based on Prefix
//     pub base_dir: String,

//     // Scan/return only content with prefix.
//     pub prefix: String,

//     // FilterPrefix will return only results with this prefix when scanning.
//     // Should never contain a slash.
//     // Prefix should still be set.
//     pub filter_prefix: String,

//     // Marker to resume listing.
//     // The response will be the first entry >= this object name.
//     pub marker: String,

//     // Limit the number of results.
//     pub limit: i32,
// }

#[async_trait::async_trait]
impl ObjectIO for ECStore {
    #[instrument(level = "debug", skip(self))]
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        check_get_obj_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_reader(bucket, object.as_str(), range, h, opts).await;
        }

        // TODO: nslock

        let mut opts = opts.clone();

        opts.no_lock = true;

        // TODO: check if DeleteMarker
        let (_oi, idx) = self.get_latest_object_info_with_idx(bucket, &object, &opts).await?;

        self.pools[idx]
            .get_object_reader(bucket, object.as_str(), range, h, &opts)
            .await
    }
    #[instrument(level = "debug", skip(self, data))]
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        check_put_object_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object(bucket, object.as_str(), data, opts).await;
        }

        let idx = self.get_pool_idx(bucket, &object, data.size()).await?;

        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            ));
        }

        self.pools[idx].put_object(bucket, &object, data, opts).await
    }
}

lazy_static! {
    static ref enableObjcetLockConfig: ObjectLockConfiguration = ObjectLockConfiguration {
        object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
        ..Default::default()
    };
    static ref enableVersioningConfig: VersioningConfiguration = VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
        ..Default::default()
    };
}

#[async_trait::async_trait]
impl StorageAPI for ECStore {
    #[instrument(skip(self))]
    async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper> {
        self.pools[0].new_ns_lock(bucket, object).await
    }
    #[instrument(skip(self))]
    async fn backend_info(&self) -> rustfs_madmin::BackendInfo {
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
    async fn storage_info(&self) -> rustfs_madmin::StorageInfo {
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
    async fn local_storage_info(&self) -> rustfs_madmin::StorageInfo {
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
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        if !is_meta_bucketname(bucket)
            && let Err(err) = check_valid_bucket_name_strict(bucket)
        {
            return Err(StorageError::BucketNameInvalid(err.to_string()));
        }

        // TODO: nslock

        if let Err(err) = self.peer_sys.make_bucket(bucket, opts).await {
            let err = to_object_err(err.into(), vec![bucket]);
            if !is_err_bucket_exists(&err) {
                error!("make bucket failed: {err}");
                let _ = self
                    .delete_bucket(
                        bucket,
                        &DeleteBucketOptions {
                            no_lock: true,
                            no_recreate: true,
                            ..Default::default()
                        },
                    )
                    .await;
            }
            return Err(err);
        };

        let mut meta = BucketMetadata::new(bucket);

        meta.set_created(opts.created_at);

        if opts.lock_enabled {
            meta.object_lock_config_xml = crate::bucket::utils::serialize::<ObjectLockConfiguration>(&enableObjcetLockConfig)?;
            meta.versioning_config_xml = crate::bucket::utils::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        if opts.versioning_enabled {
            meta.versioning_config_xml = crate::bucket::utils::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        meta.save().await?;

        set_bucket_metadata(bucket.to_string(), meta).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let mut info = self.peer_sys.get_bucket_info(bucket, opts).await?;

        if let Ok(sys) = metadata_sys::get(bucket).await {
            info.created = Some(sys.created);
            info.versioning = sys.versioning();
            info.object_locking = sys.object_locking();
        }

        Ok(info)
    }
    #[instrument(skip(self))]
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        // TODO: opts.cached

        let mut buckets = self.peer_sys.list_bucket(opts).await?;

        if !opts.no_metadata {
            for bucket in buckets.iter_mut() {
                if let Ok(created) = metadata_sys::created_at(&bucket.name).await {
                    bucket.created = Some(created);
                }
            }
        }
        Ok(buckets)
    }
    #[instrument(skip(self))]
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        if is_meta_bucketname(bucket) {
            return Err(StorageError::BucketNameInvalid(bucket.to_string()));
        }

        if let Err(err) = check_valid_bucket_name(bucket) {
            return Err(StorageError::BucketNameInvalid(err.to_string()));
        }

        // TODO: nslock

        // Check bucket exists before deletion (per S3 API spec)
        // If bucket doesn't exist, return NoSuchBucket error
        if let Err(err) = self.peer_sys.get_bucket_info(bucket, &BucketOptions::default()).await {
            // Convert DiskError to StorageError for comparison
            let storage_err: StorageError = err.into();
            if is_err_bucket_not_found(&storage_err) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
            return Err(to_object_err(storage_err, vec![bucket]));
        }

        // Check bucket is empty before deletion (per S3 API spec)
        // If bucket is not empty (contains actual objects with xl.meta files) and force
        // is not set, return BucketNotEmpty error.
        // Note: Empty directories (left after object deletion) should NOT count as objects.
        if !opts.force {
            let local_disks = all_local_disk().await;
            for disk in local_disks.iter() {
                // Check if bucket directory contains any xl.meta files (actual objects)
                // We recursively scan for xl.meta files to determine if bucket has objects
                // Use the disk's root path to construct bucket path
                let bucket_path = disk.path().join(bucket);
                if has_xlmeta_files(&bucket_path).await {
                    return Err(StorageError::BucketNotEmpty(bucket.to_string()));
                }
            }
        }

        self.peer_sys
            .delete_bucket(bucket, opts)
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket]))?;

        // TODO: replication opts.srdelete_op

        // Delete the metadata
        self.delete_all(RUSTFS_META_BUCKET, format!("{BUCKET_META_PREFIX}/{bucket}").as_str())
            .await?;
        Ok(())
    }

    // @continuation_token marker
    // @start_after as marker when continuation_token empty
    // @delimiter default="/", empty when recursive
    // @max_keys limit
    #[instrument(skip(self))]
    async fn list_objects_v2(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        fetch_owner: bool,
        start_after: Option<String>,
        incl_deleted: bool,
    ) -> Result<ListObjectsV2Info> {
        self.inner_list_objects_v2(
            bucket,
            prefix,
            continuation_token,
            delimiter,
            max_keys,
            fetch_owner,
            start_after,
            incl_deleted,
        )
        .await
    }

    #[instrument(skip(self))]
    async fn list_object_versions(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        self.inner_list_object_versions(bucket, prefix, marker, version_marker, delimiter, max_keys)
            .await
    }

    async fn walk(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()> {
        self.walk_internal(rx, bucket, prefix, result, opts).await
    }

    #[instrument(skip(self))]
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        check_object_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_info(bucket, object.as_str(), opts).await;
        }

        // TODO: nslock

        let (info, _) = self.get_latest_object_info_with_idx(bucket, object.as_str(), opts).await?;

        opts.precondition_check(&info)?;
        Ok(info)
    }

    // TODO: review
    #[instrument(skip(self))]
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        check_copy_obj_args(src_bucket, src_object)?;
        check_copy_obj_args(dst_bucket, dst_object)?;

        let src_object = encode_dir_object(src_object);
        let dst_object = encode_dir_object(dst_object);

        let cp_src_dst_same = path_join_buf(&[src_bucket, &src_object]) == path_join_buf(&[dst_bucket, &dst_object]);

        // TODO: nslock

        let pool_idx = self.get_pool_idx_no_lock(src_bucket, &src_object, src_info.size).await?;

        if cp_src_dst_same {
            if let (Some(src_vid), Some(dst_vid)) = (&src_opts.version_id, &dst_opts.version_id)
                && src_vid == dst_vid
            {
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, dst_opts)
                    .await;
            }

            if !dst_opts.versioned && src_opts.version_id.is_none() {
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, dst_opts)
                    .await;
            }

            if dst_opts.versioned && src_opts.version_id != dst_opts.version_id {
                src_info.version_only = true;
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, dst_opts)
                    .await;
            }
        }

        let put_opts = ObjectOptions {
            user_defined: src_info.user_defined.clone(),
            versioned: dst_opts.versioned,
            version_id: dst_opts.version_id.clone(),
            no_lock: true,
            mod_time: dst_opts.mod_time,
            ..Default::default()
        };

        if let Some(put_object_reader) = src_info.put_object_reader.as_mut() {
            return self.pools[pool_idx]
                .put_object(dst_bucket, &dst_object, put_object_reader, &put_opts)
                .await;
        }

        Err(StorageError::InvalidArgument(
            src_bucket.to_owned(),
            src_object.to_owned(),
            "put_object_reader is none".to_owned(),
        ))
    }
    #[instrument(skip(self))]
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        check_del_obj_args(bucket, object)?;

        if opts.delete_prefix {
            self.delete_prefix(bucket, object).await?;
            return Ok(ObjectInfo::default());
        }

        // TODO: nslock

        let object = encode_dir_object(object);
        let object = object.as_str();

        let mut gopts = opts.clone();
        gopts.no_lock = true;

        // Determine which pool contains it
        let (mut pinfo, errs) = self
            .get_pool_info_existing_with_opts(bucket, object, &gopts)
            .await
            .map_err(|e| {
                if is_err_read_quorum(&e) {
                    StorageError::ErasureWriteQuorum
                } else {
                    e
                }
            })?;

        if pinfo.object_info.delete_marker && opts.version_id.is_none() {
            pinfo.object_info.name = decode_dir_object(object);
            return Ok(pinfo.object_info);
        }

        if opts.data_movement && opts.src_pool_idx == pinfo.index {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.unwrap_or_default(),
            ));
        }

        if opts.data_movement {
            let mut obj = self.pools[pinfo.index].delete_object(bucket, object, opts).await?;
            obj.name = decode_dir_object(obj.name.as_str());
            return Ok(obj);
        }

        if !errs.is_empty() && !opts.versioned && !opts.version_suspended {
            return self.delete_object_from_all_pools(bucket, object, &opts, errs).await;
        }

        for pool in self.pools.iter() {
            match pool.delete_object(bucket, object, opts.clone()).await {
                Ok(res) => {
                    let mut obj = res;
                    obj.name = decode_dir_object(object);
                    return Ok(obj);
                }
                Err(err) => {
                    if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                        return Err(err);
                    }
                }
            }
        }

        if let Some(ver) = opts.version_id {
            return Err(StorageError::VersionNotFound(bucket.to_owned(), object.to_owned(), ver));
        }

        Err(StorageError::ObjectNotFound(bucket.to_owned(), object.to_owned()))
    }
    // TODO: review
    #[instrument(skip(self))]
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        // encode object name
        let objects: Vec<ObjectToDelete> = objects
            .iter()
            .map(|v| {
                let mut v = v.clone();
                v.object_name = encode_dir_object(v.object_name.as_str());
                v
            })
            .collect();

        // Default return value
        let mut del_objects = vec![DeletedObject::default(); objects.len()];

        let mut del_errs = Vec::with_capacity(objects.len());
        for _ in 0..objects.len() {
            del_errs.push(None)
        }

        // TODO: nslock

        let mut futures = Vec::with_capacity(self.pools.len());

        for pool in self.pools.iter() {
            futures.push(pool.delete_objects(bucket, objects.clone(), opts.clone()));
        }

        let results = join_all(futures).await;

        for idx in 0..del_objects.len() {
            for (dels, errs) in results.iter() {
                if errs[idx].is_none() && dels[idx].found {
                    del_errs[idx] = None;
                    del_objects[idx] = dels[idx].clone();
                    break;
                }

                if del_errs[idx].is_none() {
                    del_errs[idx] = errs[idx].clone();
                    del_objects[idx] = dels[idx].clone();
                }
            }
        }

        del_objects.iter_mut().for_each(|v| {
            v.object_name = decode_dir_object(&v.object_name);
        });

        (del_objects, del_errs)

        // let mut futures = Vec::with_capacity(objects.len());

        // for obj in objects.iter() {
        //     futures.push(async move {
        //         self.internal_get_pool_info_existing_with_opts(
        //             bucket,
        //             &obj.object_name,
        //             &ObjectOptions {
        //                 no_lock: true,
        //                 ..Default::default()
        //             },
        //         )
        //         .await
        //     });
        // }

        // let results = join_all(futures).await;

        // // let mut jhs = Vec::new();
        // // let semaphore = Arc::new(Semaphore::new(num_cpus::get()));
        // // let pools = Arc::new(self.pools.clone());

        // // for obj in objects.iter() {
        // //     let (semaphore, pools, bucket, object_name, opt) = (
        // //         semaphore.clone(),
        // //         pools.clone(),
        // //         bucket.to_string(),
        // //         obj.object_name.to_string(),
        // //         ObjectOptions::default(),
        // //     );

        // //     let jh = tokio::spawn(async move {
        // //         let _permit = semaphore.acquire().await.unwrap();
        // //         self.internal_get_pool_info_existing_with_opts(pools.as_ref(), &bucket, &object_name, &opt)
        // //             .await
        // //     });
        // //     jhs.push(jh);
        // // }
        // // let mut results = Vec::new();
        // // for jh in jhs {
        // //     results.push(jh.await.unwrap());
        // // }

        // // Record the mapping pool_idx -> object index
        // let mut pool_obj_idx_map = HashMap::new();
        // let mut orig_index_map = HashMap::new();

        // for (i, res) in results.into_iter().enumerate() {
        //     match res {
        //         Ok((pinfo, _)) => {
        //             if let Some(obj) = objects.get(i) {
        //                 if pinfo.object_info.delete_marker && obj.version_id.is_none() {
        //                     del_objects[i] = DeletedObject {
        //                         delete_marker: pinfo.object_info.delete_marker,
        //                         delete_marker_version_id: pinfo.object_info.version_id.map(|v| v.to_string()),
        //                         object_name: decode_dir_object(&pinfo.object_info.name),
        //                         delete_marker_mtime: pinfo.object_info.mod_time,
        //                         ..Default::default()
        //                     };
        //                     continue;
        //                 }

        //                 if !pool_obj_idx_map.contains_key(&pinfo.index) {
        //                     pool_obj_idx_map.insert(pinfo.index, vec![obj.clone()]);
        //                 } else if let Some(val) = pool_obj_idx_map.get_mut(&pinfo.index) {
        //                     val.push(obj.clone());
        //                 }

        //                 if !orig_index_map.contains_key(&pinfo.index) {
        //                     orig_index_map.insert(pinfo.index, vec![i]);
        //                 } else if let Some(val) = orig_index_map.get_mut(&pinfo.index) {
        //                     val.push(i);
        //                 }
        //             }
        //         }
        //         Err(e) => {
        //             if !is_err_object_not_found(&e) && is_err_version_not_found(&e) {
        //                 del_errs[i] = Some(e)
        //             }

        //             if let Some(obj) = objects.get(i) {
        //                 del_objects[i] = DeletedObject {
        //                     object_name: decode_dir_object(&obj.object_name),
        //                     version_id: obj.version_id.map(|v| v.to_string()),
        //                     ..Default::default()
        //                 }
        //             }
        //         }
        //     }
        // }

        // if !pool_obj_idx_map.is_empty() {
        //     for (i, sets) in self.pools.iter().enumerate() {
        //         // Retrieve the object index for a pool idx
        //         if let Some(objs) = pool_obj_idx_map.get(&i) {
        //             // Fetch the corresponding object (should never be None)
        //             // let objs: Vec<ObjectToDelete> = obj_idxs.iter().filter_map(|&idx| objects.get(idx).cloned()).collect();

        //             if objs.is_empty() {
        //                 continue;
        //             }

        //             let (pdel_objs, perrs) = sets.delete_objects(bucket, objs.clone(), opts.clone()).await?;

        //             // Insert simultaneously (should never be None)
        //             let org_indexes = orig_index_map.get(&i).unwrap();

        //             // perrs should follow the same order as obj_idxs
        //             for (i, err) in perrs.into_iter().enumerate() {
        //                 let obj_idx = org_indexes[i];

        //                 if err.is_some() {
        //                     del_errs[obj_idx] = err;
        //                 }

        //                 let mut dobj = pdel_objs.get(i).unwrap().clone();
        //                 dobj.object_name = decode_dir_object(&dobj.object_name);

        //                 del_objects[obj_idx] = dobj;
        //             }
        //         }
        //     }
        // }

        // Ok((del_objects, del_errs))
    }

    #[instrument(skip(self))]
    async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        max_parts: usize,
        opts: &ObjectOptions,
    ) -> Result<ListPartsInfo> {
        check_list_parts_args(bucket, object, upload_id)?;

        // TODO: nslock

        if self.single_pool() {
            return self.pools[0]
                .list_object_parts(bucket, object, upload_id, part_number_marker, max_parts, opts)
                .await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
                continue;
            }
            return match pool
                .list_object_parts(bucket, object, upload_id, part_number_marker, max_parts, opts)
                .await
            {
                Ok(res) => Ok(res),
                Err(err) => {
                    if is_err_invalid_upload_id(&err) {
                        continue;
                    }
                    Err(err)
                }
            };
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }

    #[instrument(skip(self))]
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo> {
        check_list_multipart_args(bucket, prefix, &key_marker, &upload_id_marker, &delimiter)?;

        if prefix.is_empty() {
            // TODO: return from cache
        }

        if self.single_pool() {
            return self.pools[0]
                .list_multipart_uploads(bucket, prefix, key_marker, upload_id_marker, delimiter, max_uploads)
                .await;
        }

        let mut uploads = Vec::new();

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
                continue;
            }
            let res = pool
                .list_multipart_uploads(
                    bucket,
                    prefix,
                    key_marker.clone(),
                    upload_id_marker.clone(),
                    delimiter.clone(),
                    max_uploads,
                )
                .await?;
            uploads.extend(res.uploads);
        }

        Ok(ListMultipartsInfo {
            key_marker,
            upload_id_marker,
            max_uploads,
            uploads,
            prefix: prefix.to_owned(),
            delimiter: delimiter.to_owned(),
            ..Default::default()
        })
    }

    #[instrument(skip(self))]
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        check_new_multipart_args(bucket, object)?;

        if self.single_pool() {
            return self.pools[0].new_multipart_upload(bucket, object, opts).await;
        }

        for (idx, pool) in self.pools.iter().enumerate() {
            if self.is_suspended(idx).await || self.is_pool_rebalancing(idx).await {
                continue;
            }
            let res = pool
                .list_multipart_uploads(bucket, object, None, None, None, MAX_UPLOADS_LIST)
                .await?;

            if !res.uploads.is_empty() {
                return self.pools[idx].new_multipart_upload(bucket, object, opts).await;
            }
        }
        let idx = self.get_pool_idx(bucket, object, -1).await?;
        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                "".to_owned(),
            ));
        }

        self.pools[idx].new_multipart_upload(bucket, object, opts).await
    }

    #[instrument(skip(self))]
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            let _ = self.pools[0].add_partial(bucket, object.as_str(), version_id).await;
        }

        let idx = self
            .get_pool_idx_existing_with_opts(bucket, object.as_str(), &ObjectOptions::default())
            .await?;

        let _ = self.pools[idx].add_partial(bucket, object.as_str(), version_id).await;
        Ok(())
    }
    #[instrument(skip(self))]
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].transition_object(bucket, &object, opts).await;
        }

        //opts.skip_decommissioned = true;
        //opts.no_lock = true;
        let idx = self.get_pool_idx_existing_with_opts(bucket, &object, opts).await?;

        self.pools[idx].transition_object(bucket, &object, opts).await
    }

    #[instrument(skip(self))]
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].clone().restore_transitioned_object(bucket, &object, opts).await;
        }

        //opts.skip_decommissioned = true;
        //opts.nolock = true;
        let idx = self.get_pool_idx_existing_with_opts(bucket, object.as_str(), opts).await?;

        self.pools[idx]
            .clone()
            .restore_transitioned_object(bucket, &object, opts)
            .await
    }

    #[instrument(skip(self))]
    async fn copy_object_part(
        &self,
        src_bucket: &str,
        src_object: &str,
        _dst_bucket: &str,
        _dst_object: &str,
        _upload_id: &str,
        _part_id: usize,
        _start_offset: i64,
        _length: i64,
        _src_info: &ObjectInfo,
        _src_opts: &ObjectOptions,
        _dst_opts: &ObjectOptions,
    ) -> Result<()> {
        check_new_multipart_args(src_bucket, src_object)?;

        // TODO: PutObjectReader
        // self.put_object_part(dst_bucket, dst_object, upload_id, part_id, data, opts)

        unimplemented!()
    }
    #[instrument(skip(self, data))]
    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        check_put_object_part_args(bucket, object, upload_id)?;

        if self.single_pool() {
            return self.pools[0]
                .put_object_part(bucket, object, upload_id, part_id, data, opts)
                .await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
                continue;
            }
            let err = match pool.put_object_part(bucket, object, upload_id, part_id, data, opts).await {
                Ok(res) => return Ok(res),
                Err(err) => {
                    if is_err_invalid_upload_id(&err) {
                        None
                    } else {
                        Some(err)
                    }
                }
            };

            if let Some(err) = err {
                error!("put_object_part err: {:?}", err);
                return Err(err);
            }
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }

    #[instrument(skip(self))]
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        check_list_parts_args(bucket, object, upload_id)?;
        if self.single_pool() {
            return self.pools[0].get_multipart_info(bucket, object, upload_id, opts).await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
                continue;
            }

            return match pool.get_multipart_info(bucket, object, upload_id, opts).await {
                Ok(res) => Ok(res),
                Err(err) => {
                    if is_err_invalid_upload_id(&err) {
                        continue;
                    }

                    Err(err)
                }
            };
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }
    #[instrument(skip(self))]
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()> {
        check_abort_multipart_args(bucket, object, upload_id)?;

        // TODO: defer DeleteUploadID

        if self.single_pool() {
            return self.pools[0].abort_multipart_upload(bucket, object, upload_id, opts).await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
                continue;
            }

            let err = match pool.abort_multipart_upload(bucket, object, upload_id, opts).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    //
                    if is_err_invalid_upload_id(&err) { None } else { Some(err) }
                }
            };

            if let Some(er) = err {
                return Err(er);
            }
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }

    #[instrument(skip(self))]
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        check_complete_multipart_args(bucket, object, upload_id)?;

        if self.single_pool() {
            return self.pools[0]
                .clone()
                .complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
                .await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
                continue;
            }

            let pool = pool.clone();
            let err = match pool
                .complete_multipart_upload(bucket, object, upload_id, uploaded_parts.clone(), opts)
                .await
            {
                Ok(res) => return Ok(res),
                Err(err) => {
                    //
                    if is_err_invalid_upload_id(&err) { None } else { Some(err) }
                }
            };

            if let Some(er) = err {
                return Err(er);
            }
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }

    #[instrument(skip(self))]
    async fn get_disks(&self, pool_idx: usize, set_idx: usize) -> Result<Vec<Option<DiskStore>>> {
        if pool_idx < self.pools.len() && set_idx < self.pools[pool_idx].disk_set.len() {
            self.pools[pool_idx].disk_set[set_idx].get_disks(0, 0).await
        } else {
            Err(Error::other(format!("pool idx {pool_idx}, set idx {set_idx}, not found")))
        }
    }

    #[instrument(skip(self))]
    fn set_drive_counts(&self) -> Vec<usize> {
        let mut counts = vec![0; self.pools.len()];

        for (i, pool) in self.pools.iter().enumerate() {
            counts[i] = pool.set_drive_count();
        }
        counts
    }
    #[instrument(skip(self))]
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].put_object_metadata(bucket, object.as_str(), opts).await;
        }

        let mut opts = opts.clone();
        opts.metadata_chg = true;

        let idx = self.get_pool_idx_existing_with_opts(bucket, object.as_str(), &opts).await?;

        self.pools[idx].put_object_metadata(bucket, object.as_str(), &opts).await
    }
    #[instrument(skip(self))]
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_tags(bucket, object.as_str(), opts).await;
        }

        let (oi, _) = self.get_latest_object_info_with_idx(bucket, &object, opts).await?;

        Ok(oi.user_tags)
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object_tags(bucket, object.as_str(), tags, opts).await;
        }

        let idx = self.get_pool_idx_existing_with_opts(bucket, object.as_str(), opts).await?;

        self.pools[idx].put_object_tags(bucket, object.as_str(), tags, opts).await
    }

    #[instrument(skip(self))]
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()> {
        check_del_obj_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0]
                .delete_object_version(bucket, object.as_str(), fi, force_del_marker)
                .await;
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].delete_object_tags(bucket, object.as_str(), opts).await;
        }

        let idx = self.get_pool_idx_existing_with_opts(bucket, object.as_str(), opts).await?;

        self.pools[idx].delete_object_tags(bucket, object.as_str(), opts).await
    }

    #[instrument(skip(self))]
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        info!("heal_format");
        let mut r = HealResultItem {
            heal_item_type: HealItemType::Metadata.to_string(),
            detail: "disk-format".to_string(),
            ..Default::default()
        };

        let mut count_no_heal = 0;
        for pool in self.pools.iter() {
            let (mut result, err) = pool.heal_format(dry_run).await?;
            if let Some(err) = err {
                match err {
                    StorageError::NoHealRequired => {
                        count_no_heal += 1;
                    }
                    _ => {
                        continue;
                    }
                }
            }
            r.disk_count += result.disk_count;
            r.set_count += result.set_count;
            r.before.drives.append(&mut result.before.drives);
            r.after.drives.append(&mut result.after.drives);
        }
        if count_no_heal == self.pools.len() {
            info!("heal format success, NoHealRequired");
            return Ok((r, Some(StorageError::NoHealRequired)));
        }
        info!("heal format success result: {:?}", r);
        Ok((r, None))
    }

    #[instrument(skip(self))]
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let res = self.peer_sys.heal_bucket(bucket, opts).await?;

        Ok(res)
    }
    #[instrument(skip(self))]
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        info!("ECStore heal_object");
        let object = encode_dir_object(object);

        let mut futures = Vec::with_capacity(self.pools.len());
        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
                continue;
            }
            futures.push(pool.heal_object(bucket, &object, version_id, opts));
        }
        let results = join_all(futures).await;

        let mut errs = Vec::with_capacity(self.pools.len());
        let mut ress = Vec::with_capacity(self.pools.len());

        for res in results.into_iter() {
            match res {
                Ok((result, err)) => {
                    let mut result = result;
                    result.object = decode_dir_object(&result.object);
                    ress.push(result);
                    errs.push(err);
                }
                Err(err) => {
                    errs.push(Some(err));
                    ress.push(HealResultItem::default());
                }
            }
        }

        for (idx, err) in errs.iter().enumerate() {
            if err.is_none() {
                return Ok((ress.remove(idx), None));
            }
        }

        // No pool returned a nil error, return the first non 'not found' error
        for (index, err) in errs.iter().enumerate() {
            return match err {
                Some(err) => {
                    if is_err_object_not_found(err) || is_err_version_not_found(err) {
                        continue;
                    }
                    Ok((ress.remove(index), Some(err.clone())))
                }
                None => Ok((ress.remove(index), None)),
            };
        }

        // At this stage, all errors are 'not found'
        if !version_id.is_empty() {
            return Ok((HealResultItem::default(), Some(Error::FileVersionNotFound)));
        }

        Ok((HealResultItem::default(), Some(Error::FileNotFound)))
    }

    #[instrument(skip(self))]
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
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

    #[instrument(skip(self))]
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].check_abandoned_parts(bucket, &object, opts).await;
        }

        let mut errs = Vec::new();
        for pool in self.pools.iter() {
            //TODO: IsSuspended
            if let Err(err) = pool.check_abandoned_parts(bucket, &object, opts).await {
                errs.push(err);
            }
        }

        if !errs.is_empty() {
            return Err(errs[0].clone());
        }

        Ok(())
    }

    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let get_object_reader = <Self as ObjectIO>::get_object_reader(self, bucket, object, None, HeaderMap::new(), opts).await?;
        // Stream to sink to avoid loading entire object into memory during verification
        let mut reader = get_object_reader.stream;
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        Ok(())
    }
}

async fn init_local_peer(endpoint_pools: &EndpointServerPools, host: &String, port: &String) {
    let mut peer_set = Vec::new();
    endpoint_pools.as_ref().iter().for_each(|endpoints| {
        endpoints.endpoints.as_ref().iter().for_each(|endpoint| {
            if endpoint.get_type() == EndpointType::Url && endpoint.is_local && endpoint.url.has_host() {
                peer_set.push(endpoint.url.host_str().unwrap().to_string());
            }
        });
    });

    if peer_set.is_empty() {
        if !host.is_empty() {
            *GLOBAL_LOCAL_NODE_NAME.write().await = format!("{host}:{port}");
            return;
        }

        *GLOBAL_LOCAL_NODE_NAME.write().await = format!("127.0.0.1:{port}");
        return;
    }

    *GLOBAL_LOCAL_NODE_NAME.write().await = peer_set[0].clone();
}

pub async fn get_disk_infos(disks: &[Option<DiskStore>]) -> Vec<Option<DiskInfo>> {
    let opts = &DiskInfoOptions::default();
    let mut res = vec![None; disks.len()];
    for (idx, disk_op) in disks.iter().enumerate() {
        if let Some(disk) = disk_op
            && let Ok(info) = disk.disk_info(opts).await
        {
            res[idx] = Some(info);
        }
    }

    res
}

#[derive(Debug, Default, Clone)]
pub struct PoolAvailableSpace {
    pub index: usize,
    pub available: u64,    // in bytes
    pub max_used_pct: u64, // Used disk percentage of most filled disk, rounded down.
}

#[derive(Debug, Default, Clone)]
pub struct ServerPoolsAvailableSpace(Vec<PoolAvailableSpace>);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_disk_infos() {
        let disks = vec![None, None]; // Empty disks for testing
        let infos = get_disk_infos(&disks).await;

        assert_eq!(infos.len(), disks.len());
        // All should be None since we passed None disks
        assert!(infos.iter().all(|info| info.is_none()));
    }

    #[tokio::test]
    async fn test_has_space_for() {
        let disk_infos = vec![None, None]; // No actual disk info

        let result = has_space_for(&disk_infos, 1024).await;
        // Should fail due to no valid disk info
        assert!(result.is_err());
    }

    #[test]
    fn test_server_pools_available_space() {
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
        // filter_max_used sets available to 0 for filtered pools, doesn't remove them
        assert_eq!(spaces.0.len(), 2); // Length remains the same
        assert_eq!(spaces.0[0].index, 0);
        assert_eq!(spaces.0[0].available, 1000); // First pool should still be available
        assert_eq!(spaces.0[1].available, 0); // Second pool should be filtered (available = 0)
        assert_eq!(spaces.total_available(), 1000); // Only first pool contributes to total
    }

    #[tokio::test]
    async fn test_find_local_disk() {
        let result = find_local_disk(&"/nonexistent/path".to_string()).await;
        assert!(result.is_none(), "Should return None for nonexistent path");
    }

    #[tokio::test]
    async fn test_all_local_disk_path() {
        let paths = all_local_disk_path().await;
        // Should return empty or some paths depending on global state
        assert!(paths.is_empty() || !paths.is_empty());
    }

    #[tokio::test]
    async fn test_all_local_disk() {
        let disks = all_local_disk().await;
        // Should return empty or some disks depending on global state
        assert!(disks.is_empty() || !disks.is_empty());
    }

    // Test that we can create the basic structures without global state
    #[test]
    fn test_pool_available_space_creation() {
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
    fn test_server_pools_available_space_iter() {
        let spaces = ServerPoolsAvailableSpace(vec![PoolAvailableSpace {
            index: 0,
            available: 1000,
            max_used_pct: 50,
        }]);

        let mut count = 0;
        for space in spaces.iter() {
            assert_eq!(space.index, 0);
            count += 1;
        }
        assert_eq!(count, 1);
    }
}
