#![allow(clippy::map_entry)]

use crate::bucket::metadata_sys::{self, init_bucket_metadata_sys, set_bucket_metadata};
use crate::bucket::utils::{check_valid_bucket_name, check_valid_bucket_name_strict, is_meta_bucketname};
use crate::config::GLOBAL_StorageClass;
use crate::config::{self, storageclass, GLOBAL_ConfigSys};
use crate::disk::endpoint::{Endpoint, EndpointType};
use crate::disk::{DiskAPI, DiskInfo, DiskInfoOptions, MetaCacheEntry};
use crate::global::{
    is_dist_erasure, is_erasure_sd, set_global_deployment_id, set_object_layer, DISK_ASSUME_UNKNOWN_SIZE, DISK_FILL_FRACTION,
    DISK_MIN_INODES, DISK_RESERVE_FRACTION, GLOBAL_LOCAL_DISK_MAP, GLOBAL_LOCAL_DISK_SET_DRIVES,
};
use crate::heal::data_usage::{DataUsageInfo, DATA_USAGE_ROOT};
use crate::heal::data_usage_cache::{DataUsageCache, DataUsageCacheInfo};
use crate::heal::heal_commands::{HealOpts, HealResultItem, HealScanMode, HEAL_ITEM_METADATA};
use crate::heal::heal_ops::{HealEntryFn, HealSequence};
use crate::new_object_layer_fn;
use crate::pools::PoolMeta;
use crate::store_api::{
    BackendByte, BackendDisks, BackendInfo, ListMultipartsInfo, ListObjectVersionsInfo, MultipartInfo, ObjectIO, StorageInfo,
};
use crate::store_err::{
    is_err_bucket_exists, is_err_invalid_upload_id, is_err_object_not_found, is_err_read_quorum, is_err_version_not_found,
    to_object_err, StorageError,
};
use crate::store_init::ec_drives_no_config;
use crate::utils::crypto::base64_decode;
use crate::utils::path::{base_dir_from_prefix, decode_dir_object, encode_dir_object, SLASH_SEPARATOR};
use crate::utils::xml;
use crate::{
    bucket::metadata::BucketMetadata,
    disk::{error::DiskError, new_disk, DiskOption, DiskStore, WalkDirOptions, BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    endpoints::EndpointServerPools,
    error::{Error, Result},
    peer::S3PeerSys,
    sets::Sets,
    store_api::{
        BucketInfo, BucketOptions, CompletePart, DeleteBucketOptions, DeletedObject, GetObjectReader, HTTPRangeSpec,
        ListObjectsInfo, ListObjectsV2Info, MakeBucketOptions, MultipartUploadResult, ObjectInfo, ObjectOptions, ObjectToDelete,
        PartInfo, PutObjReader, StorageAPI,
    },
    store_init, utils,
};
use common::globals::{GLOBAL_Local_Node_Name, GLOBAL_Rustfs_Host, GLOBAL_Rustfs_Port};
use futures::future::join_all;
use glob::Pattern;
use http::HeaderMap;
use lazy_static::lazy_static;
use rand::Rng;
use s3s::dto::{BucketVersioningStatus, ObjectLockConfiguration, ObjectLockEnabled, VersioningConfiguration};
use std::cmp::Ordering;
use std::process::exit;
use std::slice::Iter;
use std::time::SystemTime;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::{interval, sleep};
use tokio::{fs, select};
use tracing::{debug, info};
use uuid::Uuid;

const MAX_UPLOADS_LIST: usize = 10000;

#[derive(Debug)]
pub struct ECStore {
    pub id: uuid::Uuid,
    // pub disks: Vec<DiskStore>,
    pub disk_map: HashMap<usize, Vec<Option<DiskStore>>>,
    pub pools: Vec<Arc<Sets>>,
    pub peer_sys: S3PeerSys,
    // pub local_disks: Vec<DiskStore>,
    pub pool_meta: RwLock<PoolMeta>,
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
    pub async fn new(_address: String, endpoint_pools: EndpointServerPools) -> Result<Arc<Self>> {
        // let layouts = DisksLayout::from_volumes(endpoints.as_slice())?;

        let mut deployment_id = None;

        // let (endpoint_pools, _) = EndpointServerPools::create_server_endpoints(address.as_str(), &layouts)?;

        let mut pools = Vec::with_capacity(endpoint_pools.as_ref().len());
        let mut disk_map = HashMap::with_capacity(endpoint_pools.as_ref().len());

        let first_is_local = endpoint_pools.first_local();

        let mut local_disks = Vec::new();

        init_local_peer(
            &endpoint_pools,
            &GLOBAL_Rustfs_Host.read().await.to_string(),
            &GLOBAL_Rustfs_Port.read().await.to_string(),
        )
        .await;

        debug!("endpoint_pools: {:?}", endpoint_pools);

        let mut common_parity_drives = 0;

        for (i, pool_eps) in endpoint_pools.as_ref().iter().enumerate() {
            if common_parity_drives == 0 {
                let parity_drives = ec_drives_no_config(pool_eps.drives_per_set)?;
                storageclass::validate_parity(parity_drives, pool_eps.drives_per_set)?;
                common_parity_drives = parity_drives;
            }

            // validate_parity(partiy_count, pool_eps.drives_per_set)?;

            let (disks, errs) = crate::store_init::init_disks(
                &pool_eps.endpoints,
                &DiskOption {
                    cleanup: true,
                    health_check: true,
                },
            )
            .await;

            DiskError::check_disk_fatal_errs(&errs)?;

            let fm = {
                let mut times = 0;
                let mut interval = 1;
                loop {
                    if let Ok(fm) = store_init::connect_load_init_formats(
                        first_is_local,
                        &disks,
                        pool_eps.set_count,
                        pool_eps.drives_per_set,
                        deployment_id,
                    )
                    .await
                    {
                        break fm;
                    }
                    times += 1;
                    if interval < 16 {
                        interval *= 2;
                    }
                    if times > 10 {
                        return Err(Error::from_string("can not get formats"));
                    }
                    info!("retrying get formats after {:?}", interval);
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {
                            info!("got ctrl+c, exits");
                            exit(0);
                        }
                        _ = sleep(Duration::from_secs(interval)) => {
                        }
                    }
                }
            };

            if deployment_id.is_none() {
                deployment_id = Some(fm.id);
            }

            if deployment_id != Some(fm.id) {
                return Err(Error::msg("deployment_id not same in one pool"));
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

        // 替换本地磁盘
        if !is_dist_erasure().await {
            let mut global_local_disk_map = GLOBAL_LOCAL_DISK_MAP.write().await;
            for disk in local_disks {
                let path = disk.endpoint().to_string();
                global_local_disk_map.insert(path, Some(disk.clone()));
            }
        }

        let peer_sys = S3PeerSys::new(&endpoint_pools);
        let mut pool_meta = PoolMeta::new(pools.clone());
        pool_meta.dont_save = true;

        let decommission_cancelers = vec![None; pools.len()];
        let ec = Arc::new(ECStore {
            id: deployment_id.unwrap(),
            disk_map,
            pools,
            peer_sys,
            pool_meta: pool_meta.into(),
            decommission_cancelers,
        });

        set_object_layer(ec.clone()).await;

        if let Some(dep_id) = deployment_id {
            set_global_deployment_id(dep_id).await;
        }

        Ok(ec)
    }

    pub async fn init(api: Arc<ECStore>) -> Result<()> {
        config::init();
        GLOBAL_ConfigSys.init(api.clone()).await?;

        let buckets_list = api
            .list_bucket(&BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .map_err(|err| Error::from_string(err.to_string()))?;

        let buckets = buckets_list.iter().map(|v| v.name.clone()).collect();

        // FIXME:

        init_bucket_metadata_sys(api.clone(), buckets).await;

        Ok(())
    }

    pub fn init_local_disks() {}

    // pub fn local_disks(&self) -> Vec<DiskStore> {
    //     self.local_disks.clone()
    // }

    pub fn single_pool(&self) -> bool {
        self.pools.len() == 1
    }

    async fn list_path(&self, opts: &ListPathOptions, delimiter: &str) -> Result<ListObjectsInfo> {
        // if opts.prefix.ends_with(SLASH_SEPARATOR) {
        //     return Err(Error::msg("eof"));
        // }

        let mut opts = opts.clone();

        if opts.base_dir.is_empty() {
            opts.base_dir = base_dir_from_prefix(&opts.prefix);
        }

        let objects = self.list_merged(&opts, delimiter).await?;

        let info = ListObjectsInfo {
            objects,
            ..Default::default()
        };
        Ok(info)
    }

    // 读所有
    async fn list_merged(&self, opts: &ListPathOptions, delimiter: &str) -> Result<Vec<ObjectInfo>> {
        let walk_opts = WalkDirOptions {
            bucket: opts.bucket.clone(),
            base_dir: opts.base_dir.clone(),
            ..Default::default()
        };

        // let (mut wr, mut rd) = tokio::io::duplex(1024);

        let mut futures = Vec::new();

        for sets in self.pools.iter() {
            for set in sets.disk_set.iter() {
                futures.push(set.walk_dir(&walk_opts));
            }
        }

        let results = join_all(futures).await;

        // let mut errs = Vec::new();
        let mut ress = Vec::new();
        let mut uniq = HashSet::new();

        for (disks_ress, _disks_errs) in results {
            for disks_res in disks_ress.iter() {
                if disks_res.is_none() {
                    // TODO handle errs
                    continue;
                }
                let entrys = disks_res.as_ref().unwrap();

                for entry in entrys {
                    // warn!("lst_merged entry---- {}", &entry.name);

                    if !opts.prefix.is_empty() && !entry.name.starts_with(&opts.prefix) {
                        continue;
                    }

                    if !uniq.contains(&entry.name) {
                        uniq.insert(entry.name.clone());
                        // TODO: 过滤

                        if opts.limit > 0 && ress.len() as i32 >= opts.limit {
                            return Ok(ress);
                        }

                        if entry.is_object() {
                            if !delimiter.is_empty() {
                                // entry.name.trim_start_matches(pat)
                            }

                            let fi = entry.to_fileinfo(&opts.bucket)?;
                            if let Some(f) = fi {
                                ress.push(f.to_object_info(&opts.bucket, &entry.name, false));
                            }
                            continue;
                        }

                        if entry.is_dir() {
                            ress.push(ObjectInfo {
                                is_dir: true,
                                bucket: opts.bucket.clone(),
                                name: entry.name.clone(),
                                ..Default::default()
                            });
                        }
                    }
                }
            }
        }

        // warn!("list_merged errs {:?}", errs);

        Ok(ress)
    }

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
    async fn delete_prefix(&self, _bucket: &str, _object: &str) -> Result<()> {
        unimplemented!()
    }

    async fn get_available_pool_idx(&self, bucket: &str, object: &str, size: i64) -> Option<usize> {
        let mut server_pools = self.get_server_pools_available_space(bucket, object, size).await;
        server_pools.filter_max_used(100 - (100_f64 * DISK_RESERVE_FRACTION) as u64);
        let total = server_pools.total_available();

        if total == 0 {
            return None;
        }

        let mut rng = rand::thread_rng();
        let random_u64: u64 = rng.gen();

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

        // TODO: 并发
        for (idx, pool) in self.pools.iter().enumerate() {
            // TODO: IsSuspended

            n_sets[idx] = pool.set_count;

            if let Ok(disks) = pool.get_disks_by_key(object).get_disks(0, 0).await {
                let disk_infos = get_disk_infos(&disks).await;
                infos[idx] = disk_infos;
            }
        }

        let mut server_pools = Vec::new();
        for (i, zinfo) in infos.iter().enumerate() {
            if zinfo.is_empty() {
                server_pools.push(PoolAvailableSpace {
                    index: i,
                    ..Default::default()
                });

                continue;
            }

            if !is_meta_bucketname(bucket) {
                let avail = has_space_for(zinfo, size).await.unwrap_or_default();

                if !avail {
                    server_pools.push(PoolAvailableSpace {
                        index: i,
                        ..Default::default()
                    });

                    continue;
                }
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
                    return Err(Error::new(DiskError::DiskFull));
                }
            }
        };

        Ok(idx)
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
            futures.push(pool.get_object_info(bucket, object, opts));
        }

        let results = join_all(futures).await;

        let mut ress = Vec::new();

        // join_all结果跟输入顺序一致
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

            at.cmp(&bt)
        });

        let mut def_pool = PoolObjInfo::default();
        let mut has_def_pool = false;

        let pool_meta = self.pool_meta.read().await;
        for pinfo in ress.iter() {
            if opts.skip_decommissioned && pool_meta.is_suspended(pinfo.index) {
                continue;
            }

            // TODO:SkipRebalancing
            // if opts.SkipRebalancing && z.IsPoolRebalancing(pinfo.Index) {
            //     continue
            // }

            if pinfo.err.is_none() {
                return Ok((pinfo.clone(), self.pools_with_object(&ress, opts).await));
            }

            let err = pinfo.err.as_ref().unwrap();

            if is_err_read_quorum(err) && !opts.metadata_chg {
                return Ok((pinfo.clone(), self.pools_with_object(&ress, opts).await));
            }

            def_pool = pinfo.clone();
            has_def_pool = true;

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

        Err(to_object_err(Error::new(DiskError::FileNotFound), vec![bucket, object]))
    }

    async fn pools_with_object(&self, pools: &[PoolObjInfo], opts: &ObjectOptions) -> Vec<PoolErr> {
        let mut errs = Vec::new();
        let pool_meta = self.pool_meta.read().await;
        for pool in pools.iter() {
            if opts.skip_decommissioned && pool_meta.is_suspended(pool.index) {
                continue;
            }
            // TODO:SkipRebalancing
            // if opts.SkipRebalancing && z.IsPoolRebalancing(pinfo.Index) {
            //     continue
            // }

            if let Some(err) = &pool.err {
                if is_err_read_quorum(err) {
                    errs.push(PoolErr {
                        index: Some(pool.index),
                        err: Some(Error::new(StorageError::InsufficientReadQuorum)),
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

    pub async fn ns_scanner(
        &self,
        updates: Sender<DataUsageInfo>,
        want_cycle: usize,
        heal_scan_mode: HealScanMode,
    ) -> Result<()> {
        let all_buckets = self.list_bucket(&BucketOptions::default()).await?;
        if all_buckets.is_empty() {
            let _ = updates.send(DataUsageInfo::default()).await;
            return Ok(());
        }

        let mut total_results = 0;
        let mut result_index = 0;
        self.pools.iter().for_each(|pool| {
            total_results += pool.disk_set.len();
        });
        let results = Arc::new(RwLock::new(vec![DataUsageCache::default(); total_results]));
        let (cancel, _) = broadcast::channel(100);
        let first_err = Arc::new(RwLock::new(None));
        let mut futures = Vec::new();
        for pool in self.pools.iter() {
            for set in pool.disk_set.iter() {
                let index = result_index;
                let results_clone = results.clone();
                let first_err_clone = first_err.clone();
                let cancel_clone = cancel.clone();
                let all_buckets_clone = all_buckets.clone();
                futures.push(async move {
                    let (tx, mut rx) = mpsc::channel(100);
                    let task = tokio::spawn(async move {
                        loop {
                            match rx.recv().await {
                                Some(info) => {
                                    results_clone.write().await[index] = info;
                                }
                                None => {
                                    return;
                                }
                            }
                        }
                    });
                    if let Err(err) = set
                        .ns_scanner(&all_buckets_clone, want_cycle as u32, tx, heal_scan_mode)
                        .await
                    {
                        let mut f_w = first_err_clone.write().await;
                        if f_w.is_none() {
                            *f_w = Some(err);
                        }
                        let _ = cancel_clone.send(true);
                        return;
                    }
                    let _ = task.await;
                });
                result_index += 1;
            }
        }
        let (update_closer_tx, mut update_close_rx) = mpsc::channel(10);
        let mut ctx_clone = cancel.subscribe();
        let all_buckets_clone = all_buckets.clone();
        let task = tokio::spawn(async move {
            let mut last_update: Option<SystemTime> = None;
            let mut interval = interval(Duration::from_secs(30));
            let all_merged = Arc::new(RwLock::new(DataUsageCache::default()));
            loop {
                select! {
                    _ = ctx_clone.recv() => {
                        return;
                    }
                    _ = update_close_rx.recv() => {
                        update_scan(all_merged.clone(), results.clone(), &mut last_update, all_buckets_clone.clone(), updates.clone()).await;
                        return;
                    }
                    _ = interval.tick() => {
                        update_scan(all_merged.clone(), results.clone(), &mut last_update, all_buckets_clone.clone(), updates.clone()).await;
                    }
                }
            }
        });
        let _ = join_all(futures).await;
        let mut ctx_closer = cancel.subscribe();
        select! {
            _ = update_closer_tx.send(true) => {

            }
            _ = ctx_closer.recv() => {

            }
        }
        let _ = task.await;
        if let Some(err) = first_err.read().await.as_ref() {
            return Err(err.clone());
        }
        Ok(())
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
            if let Some(obj1) = &a.res {
                if let Some(obj2) = &b.res {
                    let cmp = obj1.mod_time.cmp(&obj2.mod_time);
                    match cmp {
                        // eq use lowest
                        Ordering::Equal => {
                            if a.idx < b.idx {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        _ => cmp,
                    }
                } else {
                    Ordering::Greater
                }
            } else {
                Ordering::Less
            }
        });

        for res in idx_res {
            if let Some(obj) = res.res {
                return Ok((obj, res.idx));
            }

            if let Some(err) = res.err {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(err);
                }

                // TODO: delete marker
            }
        }

        let object = decode_dir_object(object);

        if opts.version_id.is_none() {
            Err(Error::new(StorageError::ObjectNotFound(bucket.to_owned(), object.to_owned())))
        } else {
            Err(Error::new(StorageError::VersionNotFound(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            )))
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
            if let Some(err) = &pe.err {
                if is_err_read_quorum(err) {
                    objs.push(None);
                    derrs.push(Some(Error::new(StorageError::InsufficientWriteQuorum)));
                    continue;
                }
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

        if derrs[0].is_some() {
            return Err(derrs[0].as_ref().unwrap().clone());
        }

        Ok(objs[0].as_ref().unwrap().clone())
    }

    pub async fn reload_pool_meta(&self) -> Result<()> {
        let mut meta = PoolMeta::default();
        meta.load().await?;

        let mut pool_meta = self.pool_meta.write().await;
        *pool_meta = meta;
        // *self.pool_meta.write().unwrap() = meta;
        Ok(())
    }
}

async fn update_scan(
    all_merged: Arc<RwLock<DataUsageCache>>,
    results: Arc<RwLock<Vec<DataUsageCache>>>,
    last_update: &mut Option<SystemTime>,
    all_buckets: Vec<BucketInfo>,
    updates: Sender<DataUsageInfo>,
) {
    let mut w = all_merged.write().await;
    *w = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    for info in results.read().await.iter() {
        if info.info.last_update.is_none() {
            return;
        }
        w.merge(info);
    }
    if w.info.last_update > *last_update && w.root().is_none() {
        let _ = updates.send(w.dui(&w.info.name, &all_buckets)).await;
        *last_update = w.info.last_update;
    }
}

pub async fn find_local_disk(disk_path: &String) -> Option<DiskStore> {
    let disk_path = match fs::canonicalize(disk_path).await {
        Ok(disk_path) => disk_path,
        Err(_) => return None,
    };

    let disk_map = GLOBAL_LOCAL_DISK_MAP.read().await;

    let path = disk_path.to_string_lossy().to_string();
    if disk_map.contains_key(&path) {
        let a = disk_map[&path].as_ref().cloned();

        return a;
    }
    None
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

// init_local_disks 初始化本地磁盘，server启动前必须初始化成功
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

#[derive(Debug, Default)]
struct PoolErr {
    index: Option<usize>,
    err: Option<Error>,
}

#[derive(Debug, Default, Clone)]
pub struct PoolObjInfo {
    pub index: usize,
    pub object_info: ObjectInfo,
    pub err: Option<Error>,
}

#[derive(Debug, Default, Clone)]
pub struct ListPathOptions {
    pub id: String,

    // Bucket of the listing.
    pub bucket: String,

    // Directory inside the bucket.
    // When unset listPath will set this based on Prefix
    pub base_dir: String,

    // Scan/return only content with prefix.
    pub prefix: String,

    // FilterPrefix will return only results with this prefix when scanning.
    // Should never contain a slash.
    // Prefix should still be set.
    pub filter_prefix: String,

    // Marker to resume listing.
    // The response will be the first entry >= this object name.
    pub marker: String,

    // Limit the number of results.
    pub limit: i32,
}

#[async_trait::async_trait]
impl ObjectIO for ECStore {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: HTTPRangeSpec,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        check_get_obj_args(bucket, object)?;

        let object = utils::path::encode_dir_object(object);

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
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        check_put_object_args(bucket, object)?;

        let object = utils::path::encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object(bucket, object.as_str(), data, opts).await;
        }

        let idx = self.get_pool_idx(bucket, &object, data.content_length as i64).await?;

        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(Error::new(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            )));
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
    async fn backend_info(&self) -> BackendInfo {
        let (standard_sc_parity, rr_sc_parity) = {
            if let Some(sc) = GLOBAL_StorageClass.get() {
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

        BackendInfo {
            backend_type: BackendByte::Erasure,
            online_disks: BackendDisks::new(),
            offline_disks: BackendDisks::new(),
            standard_sc_data,
            standard_sc_parity,
            rr_sc_data,
            rr_sc_parity,
            total_sets,
            drives_per_set,
            ..Default::default()
        }
    }
    async fn storage_info(&self) -> StorageInfo {
        // FIXME: globalNotificationSys.StorageInfo
        unimplemented!()
    }
    async fn local_storage_info(&self) -> StorageInfo {
        let mut futures = Vec::with_capacity(self.pools.len());

        for pool in self.pools.iter() {
            futures.push(pool.local_storage_info())
        }

        let results = join_all(futures).await;

        let mut disks = Vec::new();

        for res in results.into_iter() {
            disks.extend_from_slice(&res.disks);
        }

        let backend = self.backend_info().await;
        StorageInfo { backend, disks }
    }

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

    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        if is_meta_bucketname(bucket) {
            return Err(StorageError::BucketNameInvalid(bucket.to_string()).into());
        }

        if let Err(err) = check_valid_bucket_name(bucket) {
            return Err(StorageError::BucketNameInvalid(err.to_string()).into());
        }

        // TODO: nslock

        let mut opts = opts.clone();
        if !opts.force {
            // FIXME: check bucket exists
            opts.force = true
        }

        self.peer_sys
            .delete_bucket(bucket, &opts)
            .await
            .map_err(|e| to_object_err(e, vec![bucket]))?;

        // TODO: replication opts.srdelete_op

        // 删除meta
        self.delete_all(RUSTFS_META_BUCKET, format!("{}/{}", BUCKET_META_PREFIX, bucket).as_str())
            .await?;
        Ok(())
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        if !is_meta_bucketname(bucket) {
            if let Err(err) = check_valid_bucket_name_strict(bucket) {
                return Err(StorageError::BucketNameInvalid(err.to_string()).into());
            }

            // TODO: nslock
        }

        if let Err(err) = self.peer_sys.make_bucket(bucket, opts).await {
            if !is_err_bucket_exists(&err) {
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
        };

        let mut meta = BucketMetadata::new(bucket);

        if let Some(crd) = opts.created_at {
            meta.set_created(crd);
        }

        if opts.lock_enabled {
            meta.object_lock_config_xml = xml::serialize::<ObjectLockConfiguration>(&enableObjcetLockConfig)?;
            meta.versioning_config_xml = xml::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        if opts.versioning_enabled {
            meta.versioning_config_xml = xml::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        meta.save().await.map_err(|e| to_object_err(e, vec![bucket]))?;

        set_bucket_metadata(bucket.to_string(), meta).await;

        Ok(())
    }
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let mut info = self
            .peer_sys
            .get_bucket_info(bucket, opts)
            .await
            .map_err(|e| to_object_err(e, vec![bucket]))?;

        if let Ok(sys) = metadata_sys::get(bucket).await {
            info.created = Some(sys.created);
            info.versionning = sys.versioning();
            info.object_locking = sys.object_locking();
        }

        Ok(info)
    }

    // TODO: review
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> Result<(Vec<DeletedObject>, Vec<Option<Error>>)> {
        // encode object name
        let objects: Vec<ObjectToDelete> = objects
            .iter()
            .map(|v| {
                let mut v = v.clone();
                v.object_name = utils::path::encode_dir_object(v.object_name.as_str());
                v
            })
            .collect();

        // 默认返回值
        let mut del_objects = vec![DeletedObject::default(); objects.len()];

        let mut del_errs = Vec::with_capacity(objects.len());
        for _ in 0..objects.len() {
            del_errs.push(None)
        }

        // TODO: nslock

        let mut futures = Vec::with_capacity(objects.len());

        for obj in objects.iter() {
            futures.push(async move {
                self.internal_get_pool_info_existing_with_opts(
                    bucket,
                    &obj.object_name,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
            });
        }

        let results = join_all(futures).await;

        // let mut jhs = Vec::new();
        // let semaphore = Arc::new(Semaphore::new(num_cpus::get()));
        // let pools = Arc::new(self.pools.clone());

        // for obj in objects.iter() {
        //     let (semaphore, pools, bucket, object_name, opt) = (
        //         semaphore.clone(),
        //         pools.clone(),
        //         bucket.to_string(),
        //         obj.object_name.to_string(),
        //         ObjectOptions::default(),
        //     );

        //     let jh = tokio::spawn(async move {
        //         let _permit = semaphore.acquire().await.unwrap();
        //         self.internal_get_pool_info_existing_with_opts(pools.as_ref(), &bucket, &object_name, &opt)
        //             .await
        //     });
        //     jhs.push(jh);
        // }
        // let mut results = Vec::new();
        // for jh in jhs {
        //     results.push(jh.await.unwrap());
        // }

        // 记录pool Index 对应的objects pool_idx -> objects idx
        let mut pool_obj_idx_map = HashMap::new();
        let mut orig_index_map = HashMap::new();

        for (i, res) in results.into_iter().enumerate() {
            match res {
                Ok((pinfo, _)) => {
                    if pinfo.object_info.delete_marker && opts.version_id.is_none() {
                        del_objects[i] = DeletedObject {
                            delete_marker: pinfo.object_info.delete_marker,
                            delete_marker_version_id: pinfo.object_info.version_id.map(|v| v.to_string()),
                            object_name: utils::path::decode_dir_object(&pinfo.object_info.name),
                            delete_marker_mtime: pinfo.object_info.mod_time,
                            ..Default::default()
                        };
                    }

                    if let Some(obj) = objects.get(i) {
                        if !pool_obj_idx_map.contains_key(&pinfo.index) {
                            pool_obj_idx_map.insert(pinfo.index, vec![obj.clone()]);
                        } else if let Some(val) = pool_obj_idx_map.get_mut(&pinfo.index) {
                            val.push(obj.clone());
                        }

                        if !orig_index_map.contains_key(&pinfo.index) {
                            orig_index_map.insert(pinfo.index, vec![i]);
                        } else if let Some(val) = orig_index_map.get_mut(&pinfo.index) {
                            val.push(i);
                        }
                    }
                }
                Err(e) => {
                    if !is_err_object_not_found(&e) && is_err_version_not_found(&e) {
                        del_errs[i] = Some(e)
                    }

                    if let Some(obj) = objects.get(i) {
                        del_objects[i] = DeletedObject {
                            object_name: utils::path::decode_dir_object(&obj.object_name),
                            version_id: obj.version_id.map(|v| v.to_string()),
                            ..Default::default()
                        }
                    }
                }
            }
        }

        if !pool_obj_idx_map.is_empty() {
            for (i, sets) in self.pools.iter().enumerate() {
                //  取pool idx 对应的 objects index
                if let Some(objs) = pool_obj_idx_map.get(&i) {
                    //  取对应obj,理论上不会none
                    // let objs: Vec<ObjectToDelete> = obj_idxs.iter().filter_map(|&idx| objects.get(idx).cloned()).collect();

                    if objs.is_empty() {
                        continue;
                    }

                    let (pdel_objs, perrs) = sets.delete_objects(bucket, objs.clone(), opts.clone()).await?;

                    // 同时存入不可能为none
                    let org_indexes = orig_index_map.get(&i).unwrap();

                    // perrs的顺序理论上跟obj_idxs顺序一致
                    for (i, err) in perrs.into_iter().enumerate() {
                        let obj_idx = org_indexes[i];

                        if err.is_some() {
                            del_errs[obj_idx] = err;
                        }

                        let mut dobj = pdel_objs.get(i).unwrap().clone();
                        dobj.object_name = utils::path::decode_dir_object(&dobj.object_name);

                        del_objects[obj_idx] = dobj;
                    }
                }
            }
        }

        Ok((del_objects, del_errs))
    }

    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        check_del_obj_args(bucket, object)?;

        if opts.delete_prefix {
            self.delete_prefix(bucket, object).await?;
            return Ok(ObjectInfo::default());
        }

        // TODO: nslock

        let object = utils::path::encode_dir_object(object);
        let object = object.as_str();

        // 查询在哪个pool
        let (mut pinfo, errs) = self
            .get_pool_info_existing_with_opts(bucket, object, &opts)
            .await
            .map_err(|e| {
                if is_err_read_quorum(&e) {
                    Error::new(StorageError::InsufficientWriteQuorum)
                } else {
                    e
                }
            })?;

        if pinfo.object_info.delete_marker && opts.version_id.is_none() {
            pinfo.object_info.name = utils::path::decode_dir_object(object);
            return Ok(pinfo.object_info);
        }

        if opts.data_movement && opts.src_pool_idx == pinfo.index {
            return Err(Error::new(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.unwrap_or_default(),
            )));
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
                    obj.name = utils::path::decode_dir_object(object);
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
            return Err(Error::new(StorageError::VersionNotFound(bucket.to_owned(), object.to_owned(), ver)));
        }

        Err(Error::new(StorageError::ObjectNotFound(bucket.to_owned(), object.to_owned())))
    }

    // TODO: review
    async fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        delimiter: &str,
        max_keys: i32,
        _fetch_owner: bool,
        _start_after: &str,
    ) -> Result<ListObjectsV2Info> {
        let opts = ListPathOptions {
            bucket: bucket.to_string(),
            limit: max_keys,
            prefix: prefix.to_owned(),
            ..Default::default()
        };

        let info = self.list_path(&opts, delimiter).await?;

        // warn!("list_objects_v2 info {:?}", info);

        let v2 = ListObjectsV2Info {
            is_truncated: info.is_truncated,
            continuation_token: continuation_token.to_owned(),
            next_continuation_token: info.next_marker,
            objects: info.objects,
            prefixes: info.prefixes,
        };

        Ok(v2)
    }
    async fn list_object_versions(
        &self,
        _bucket: &str,
        _prefix: &str,
        marker: &str,
        version_marker: &str,
        _delimiter: &str,
        _max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        if marker.is_empty() && !version_marker.is_empty() {
            return Err(Error::new(StorageError::NotImplemented));
        }

        // let opts = ListPathOptions {
        //     bucket: bucket.to_owned(),
        //     marker: marker.to_owned(),
        //     prefix: prefix.to_owned(),
        //     limit: max_keys,
        //     ..Default::default()
        // };

        // let list = self
        //     .list_path(&opts, delimiter)
        //     .await
        //     .map_err(|e| to_object_err(e, vec![bucket]))?;

        // for info in list.objects.iter() {
        //     //
        // }

        // FIXME:
        unimplemented!()
    }
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        check_object_args(bucket, object)?;

        let object = utils::path::encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_info(bucket, object.as_str(), opts).await;
        }

        // TODO: nslock

        let (info, _) = self.get_latest_object_info_with_idx(bucket, object.as_str(), opts).await?;

        Ok(info)
    }

    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_tags(bucket, object.as_str(), opts).await;
        }

        let (oi, _) = self.get_latest_object_info_with_idx(bucket, &object, opts).await?;

        Ok(oi.user_tags)
    }

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

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object_tags(bucket, object.as_str(), tags, opts).await;
        }

        let idx = self.get_pool_idx_existing_with_opts(bucket, object.as_str(), opts).await?;

        self.pools[idx].put_object_tags(bucket, object.as_str(), tags, opts).await
    }
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].delete_object_tags(bucket, object.as_str(), opts).await;
        }

        let idx = self.get_pool_idx_existing_with_opts(bucket, object.as_str(), opts).await?;

        self.pools[idx].delete_object_tags(bucket, object.as_str(), opts).await
    }
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
            // TODO: IsSuspended
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
                return Err(err);
            }
        }

        Err(Error::new(StorageError::InvalidUploadID(
            bucket.to_owned(),
            object.to_owned(),
            upload_id.to_owned(),
        )))
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: &str,
        delimiter: &str,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo> {
        check_list_multipart_args(bucket, prefix, key_marker, upload_id_marker, delimiter)?;

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
            let res = pool
                .list_multipart_uploads(bucket, prefix, key_marker, upload_id_marker, delimiter, max_uploads)
                .await?;
            uploads.extend(res.uploads);
        }

        Ok(ListMultipartsInfo {
            key_marker: key_marker.to_owned(),
            upload_id_marker: upload_id_marker.to_owned(),
            max_uploads,
            uploads,
            prefix: prefix.to_owned(),
            delimiter: delimiter.to_owned(),
            ..Default::default()
        })
    }
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        check_new_multipart_args(bucket, object)?;

        if self.single_pool() {
            return self.pools[0].new_multipart_upload(bucket, object, opts).await;
        }

        for (idx, pool) in self.pools.iter().enumerate() {
            // // TODO: IsSuspended
            let res = pool
                .list_multipart_uploads(bucket, object, "", "", "", MAX_UPLOADS_LIST)
                .await?;

            if !res.uploads.is_empty() {
                return self.pools[idx].new_multipart_upload(bucket, object, opts).await;
            }
        }

        let idx = self.get_pool_idx(bucket, object, -1).await?;
        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(Error::new(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                "".to_owned(),
            )));
        }

        self.pools[idx].new_multipart_upload(bucket, object, opts).await
    }
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

        for (idx, pool) in self.pools.iter().enumerate() {
            if self.is_suspended(idx).await {
                continue;
            }

            match pool.get_multipart_info(bucket, object, upload_id, opts).await {
                Ok(res) => return Ok(res),
                Err(err) => {
                    if is_err_invalid_upload_id(&err) {
                        continue;
                    }

                    return Err(err);
                }
            }
        }

        Err(Error::new(StorageError::InvalidUploadID(
            bucket.to_owned(),
            object.to_owned(),
            upload_id.to_owned(),
        )))
    }
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()> {
        check_abort_multipart_args(bucket, object, upload_id)?;

        // TODO: defer

        if self.single_pool() {
            return self.pools[0].abort_multipart_upload(bucket, object, upload_id, opts).await;
        }

        for pool in self.pools.iter() {
            // TODO: IsSuspended

            let err = match pool.abort_multipart_upload(bucket, object, upload_id, opts).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    //
                    if is_err_invalid_upload_id(&err) {
                        None
                    } else {
                        Some(err)
                    }
                }
            };

            if let Some(er) = err {
                return Err(er);
            }
        }

        Err(Error::new(StorageError::InvalidUploadID(
            bucket.to_owned(),
            object.to_owned(),
            upload_id.to_owned(),
        )))
    }
    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        check_complete_multipart_args(bucket, object, upload_id)?;

        if self.single_pool() {
            return self.pools[0]
                .complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
                .await;
        }

        for pool in self.pools.iter() {
            // TODO: IsSuspended

            let err = match pool
                .complete_multipart_upload(bucket, object, upload_id, uploaded_parts.clone(), opts)
                .await
            {
                Ok(res) => return Ok(res),
                Err(err) => {
                    //
                    if is_err_invalid_upload_id(&err) {
                        None
                    } else {
                        Some(err)
                    }
                }
            };

            if let Some(er) = err {
                return Err(er);
            }
        }

        Err(Error::new(StorageError::InvalidUploadID(
            bucket.to_owned(),
            object.to_owned(),
            upload_id.to_owned(),
        )))
    }

    async fn get_disks(&self, pool_idx: usize, set_idx: usize) -> Result<Vec<Option<DiskStore>>> {
        if pool_idx < self.pools.len() && set_idx < self.pools[pool_idx].disk_set.len() {
            self.pools[pool_idx].disk_set[set_idx].get_disks(0, 0).await
        } else {
            Err(Error::msg(format!("pool idx {}, set idx {}, not found", pool_idx, set_idx)))
        }
    }

    fn set_drive_counts(&self) -> Vec<usize> {
        let mut counts = vec![0; self.pools.len()];

        for (i, pool) in self.pools.iter().enumerate() {
            counts[i] = pool.set_drive_count();
        }
        counts
    }
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        let mut r = HealResultItem {
            heal_item_type: HEAL_ITEM_METADATA.to_string(),
            detail: "disk-format".to_string(),
            ..Default::default()
        };

        let mut count_no_heal = 0;
        for pool in self.pools.iter() {
            let (mut result, err) = pool.heal_format(dry_run).await?;
            if let Some(err) = err {
                match err.downcast_ref::<DiskError>() {
                    Some(DiskError::NoHealRequired) => {
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
            return Ok((r, Some(Error::new(DiskError::NoHealRequired))));
        }
        Ok((r, None))
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        self.peer_sys.heal_bucket(bucket, opts).await
    }
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        info!("ECStore heal_object");
        let object = utils::path::encode_dir_object(object);
        let errs = Arc::new(RwLock::new(vec![None; self.pools.len()]));
        let results = Arc::new(RwLock::new(vec![HealResultItem::default(); self.pools.len()]));
        let mut futures = Vec::with_capacity(self.pools.len());
        for (idx, pool) in self.pools.iter().enumerate() {
            //TODO: IsSuspended
            let object = object.clone();
            let results = results.clone();
            let errs = errs.clone();
            futures.push(async move {
                match pool.heal_object(bucket, &object, version_id, opts).await {
                    Ok((mut result, err)) => {
                        result.object = utils::path::decode_dir_object(&result.object);
                        results.write().await.insert(idx, result);
                        errs.write().await[idx] = err;
                    }
                    Err(err) => {
                        errs.write().await[idx] = Some(err);
                    }
                }
            });
        }
        let _ = join_all(futures).await;

        // Return the first nil error
        for (index, err) in errs.read().await.iter().enumerate() {
            if err.is_none() {
                return Ok((results.write().await.remove(index), None));
            }
        }

        // No pool returned a nil error, return the first non 'not found' error
        for (index, err) in errs.read().await.iter().enumerate() {
            match err {
                Some(err) => match err.downcast_ref::<DiskError>() {
                    Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => {}
                    _ => return Ok((results.write().await.remove(index), Some(err.clone()))),
                },
                None => {
                    return Ok((results.write().await.remove(index), None));
                }
            }
        }

        // At this stage, all errors are 'not found'
        if !version_id.is_empty() {
            return Ok((HealResultItem::default(), Some(Error::new(DiskError::FileVersionNotFound))));
        }

        Ok((HealResultItem::default(), Some(Error::new(DiskError::FileNotFound))))
    }
    async fn heal_objects(
        &self,
        bucket: &str,
        prefix: &str,
        opts: &HealOpts,
        hs: Arc<HealSequence>,
        is_meta: bool,
    ) -> Result<()> {
        info!("heal objects");
        let opts_clone = *opts;
        let heal_entry: HealEntryFn = Arc::new(move |bucket: String, entry: MetaCacheEntry, scan_mode: HealScanMode| {
            let opts_clone = opts_clone;
            let hs_clone = hs.clone();
            Box::pin(async move {
                if entry.is_dir() {
                    return Ok(());
                }

                if bucket == RUSTFS_META_BUCKET
                    && Pattern::new("buckets/*/.metacache/*")
                        .map(|p| p.matches(&entry.name))
                        .unwrap_or(false)
                    || Pattern::new("tmp/*").map(|p| p.matches(&entry.name)).unwrap_or(false)
                    || Pattern::new("multipart/*").map(|p| p.matches(&entry.name)).unwrap_or(false)
                    || Pattern::new("tmp-old/*").map(|p| p.matches(&entry.name)).unwrap_or(false)
                {
                    return Ok(());
                }
                let fivs = match entry.file_info_versions(&bucket) {
                    Ok(fivs) => fivs,
                    Err(_) => {
                        if is_meta {
                            return HealSequence::heal_meta_object(hs_clone.clone(), &bucket, &entry.name, "", scan_mode).await;
                        } else {
                            return HealSequence::heal_object(hs_clone.clone(), &bucket, &entry.name, "", scan_mode).await;
                        }
                    }
                };

                if opts_clone.remove && !opts_clone.dry_run {
                    let Some(store) = new_object_layer_fn() else { return Err(Error::msg("errServerNotInitialized")) };

                    if let Err(err) = store.check_abandoned_parts(&bucket, &entry.name, &opts_clone).await {
                        info!("unable to check object {}/{} for abandoned data: {}", bucket, entry.name, err.to_string());
                    }
                }
                for version in fivs.versions.iter() {
                    if is_meta {
                        if let Err(err) = HealSequence::heal_meta_object(
                            hs_clone.clone(),
                            &bucket,
                            &version.name,
                            &version.version_id.map(|v| v.to_string()).unwrap_or("".to_string()),
                            scan_mode,
                        )
                        .await
                        {
                            match err.downcast_ref() {
                                Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => {}
                                _ => {
                                    return Err(err);
                                }
                            }
                        }
                    } else if let Err(err) = HealSequence::heal_object(
                        hs_clone.clone(),
                        &bucket,
                        &version.name,
                        &version.version_id.map(|v| v.to_string()).unwrap_or("".to_string()),
                        scan_mode,
                    )
                    .await
                    {
                        match err.downcast_ref() {
                            Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => {}
                            _ => {
                                return Err(err);
                            }
                        }
                    }
                }
                Ok(())
            })
        });
        let mut first_err = None;
        for (idx, pool) in self.pools.iter().enumerate() {
            if opts.pool.is_some() && opts.pool.unwrap() != idx {
                continue;
            }
            //TODO: IsSuspended

            for (idx, set) in pool.disk_set.iter().enumerate() {
                if opts.set.is_some() && opts.set.unwrap() != idx {
                    continue;
                }

                if let Err(err) = set.list_and_heal(bucket, prefix, opts, heal_entry.clone()).await {
                    if first_err.is_none() {
                        first_err = Some(err)
                    }
                }
            }
        }

        if first_err.is_some() {
            return Err(first_err.unwrap());
        }

        Ok(())
    }

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

        Err(Error::new(DiskError::DiskNotFound))
    }

    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()> {
        let object = utils::path::encode_dir_object(object);
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
            *GLOBAL_Local_Node_Name.write().await = format!("{}:{}", host, port);
            return;
        }

        *GLOBAL_Local_Node_Name.write().await = format!("127.0.0.1:{}", port);
        return;
    }

    *GLOBAL_Local_Node_Name.write().await = peer_set[0].clone();
}

pub fn is_valid_object_prefix(object: &str) -> bool {
    // Implement object prefix validation
    !object.is_empty() // Placeholder
}

fn is_valid_object_name(object: &str) -> bool {
    // Implement object name validation
    !object.is_empty() // Placeholder
}

fn check_object_name_for_length_and_slash(bucket: &str, object: &str) -> Result<()> {
    if object.len() > 1024 {
        return Err(Error::new(StorageError::ObjectNameTooLong(bucket.to_owned(), object.to_owned())));
    }

    if object.starts_with(SLASH_SEPARATOR) {
        return Err(Error::new(StorageError::ObjectNamePrefixAsSlash(bucket.to_owned(), object.to_owned())));
    }

    #[cfg(target_os = "windows")]
    {
        if object.contains('\\')
            || object.contains(':')
            || object.contains('*')
            || object.contains('?')
            || object.contains('"')
            || object.contains('|')
            || object.contains('<')
            || object.contains('>')
        {
            return Err(Error::new(StorageError::ObjectNameInvalid(bucket.to_owned(), object.to_owned())));
        }
    }

    Ok(())
}

fn _check_copy_obj_args(bucket: &str, object: &str) -> Result<()> {
    check_bucket_and_object_names(bucket, object)
}

fn check_get_obj_args(bucket: &str, object: &str) -> Result<()> {
    check_bucket_and_object_names(bucket, object)
}

fn check_del_obj_args(bucket: &str, object: &str) -> Result<()> {
    check_bucket_and_object_names(bucket, object)
}

fn check_bucket_and_object_names(bucket: &str, object: &str) -> Result<()> {
    if !is_meta_bucketname(bucket) && check_valid_bucket_name_strict(bucket).is_err() {
        return Err(Error::new(StorageError::BucketNameInvalid(bucket.to_string())));
    }

    if object.is_empty() {
        return Err(Error::new(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string())));
    }

    if !is_valid_object_prefix(object) {
        return Err(Error::new(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string())));
    }

    if cfg!(target_os = "windows") && object.contains('\\') {
        return Err(Error::new(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string())));
    }

    Ok(())
}

fn check_list_objs_args(bucket: &str, prefix: &str, _marker: &str) -> Result<()> {
    if !is_meta_bucketname(bucket) && check_valid_bucket_name_strict(bucket).is_err() {
        return Err(Error::new(StorageError::BucketNameInvalid(bucket.to_string())));
    }

    if !is_valid_object_prefix(prefix) {
        return Err(Error::new(StorageError::ObjectNameInvalid(bucket.to_string(), prefix.to_string())));
    }

    Ok(())
}

fn check_list_multipart_args(
    bucket: &str,
    prefix: &str,
    key_marker: &str,
    upload_id_marker: &str,
    _delimiter: &str,
) -> Result<()> {
    check_list_objs_args(bucket, prefix, key_marker)?;

    if !upload_id_marker.is_empty() {
        if key_marker.ends_with('/') {
            return Err(Error::new(StorageError::InvalidUploadIDKeyCombination(
                upload_id_marker.to_string(),
                key_marker.to_string(),
            )));
        }

        if let Err(_e) = base64_decode(upload_id_marker.as_bytes()) {
            return Err(Error::new(StorageError::MalformedUploadID(upload_id_marker.to_owned())));
        }
    }

    Ok(())
}

fn check_object_args(bucket: &str, object: &str) -> Result<()> {
    if !is_meta_bucketname(bucket) && check_valid_bucket_name_strict(bucket).is_err() {
        return Err(Error::new(StorageError::BucketNameInvalid(bucket.to_string())));
    }

    check_object_name_for_length_and_slash(bucket, object)?;

    if !is_valid_object_name(object) {
        return Err(Error::new(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string())));
    }

    Ok(())
}

fn check_new_multipart_args(bucket: &str, object: &str) -> Result<()> {
    check_object_args(bucket, object)
}

fn check_multipart_object_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    if let Err(e) = base64_decode(upload_id.as_bytes()) {
        return Err(Error::new(StorageError::MalformedUploadID(format!(
            "{}/{}-{},err:{}",
            bucket, object, upload_id, e
        ))));
    };
    check_object_args(bucket, object)
}

fn check_put_object_part_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    check_multipart_object_args(bucket, object, upload_id)
}

fn check_list_parts_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    check_multipart_object_args(bucket, object, upload_id)
}

fn check_complete_multipart_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    check_multipart_object_args(bucket, object, upload_id)
}

fn check_abort_multipart_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
    check_multipart_object_args(bucket, object, upload_id)
}

fn check_put_object_args(bucket: &str, object: &str) -> Result<()> {
    if !is_meta_bucketname(bucket) && check_valid_bucket_name_strict(bucket).is_err() {
        return Err(Error::new(StorageError::BucketNameInvalid(bucket.to_string())));
    }

    check_object_name_for_length_and_slash(bucket, object)?;

    if object.is_empty() || !is_valid_object_prefix(object) {
        return Err(Error::new(StorageError::ObjectNameInvalid(bucket.to_string(), object.to_string())));
    }

    Ok(())
}

async fn get_disk_infos(disks: &[Option<DiskStore>]) -> Vec<Option<DiskInfo>> {
    let opts = &DiskInfoOptions::default();
    let mut res = vec![None; disks.len()];
    for (idx, disk_op) in disks.iter().enumerate() {
        if let Some(disk) = disk_op {
            if let Ok(info) = disk.disk_info(opts).await {
                res[idx] = Some(info);
            }
        }
    }

    res
}

#[derive(Debug, Default)]
pub struct PoolAvailableSpace {
    pub index: usize,
    pub available: u64,    // in bytes
    pub max_used_pct: u64, // Used disk percentage of most filled disk, rounded down.
}

pub struct ServerPoolsAvailableSpace(Vec<PoolAvailableSpace>);

impl ServerPoolsAvailableSpace {
    fn iter(&self) -> Iter<'_, PoolAvailableSpace> {
        self.0.iter()
    }
    // TotalAvailable - total available space
    fn total_available(&self) -> u64 {
        let mut total = 0;
        for pool in &self.0 {
            total += pool.available;
        }
        total
    }

    // FilterMaxUsed will filter out any pools that has used percent bigger than max,
    // unless all have that, in which case all are preserved.
    fn filter_max_used(&mut self, max: u64) {
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
                pool.available = 0
            }
        }
    }
}

async fn has_space_for(dis: &[Option<DiskInfo>], size: i64) -> Result<bool> {
    let size = {
        if size < 0 {
            DISK_ASSUME_UNKNOWN_SIZE
        } else {
            size as u64 * 2
        }
    };

    let mut available = 0;
    let mut total = 0;
    let mut disks_num = 0;

    for disk in dis.iter().flatten() {
        disks_num += 1;
        total += disk.total;
        available += disk.total - disk.used;
    }

    if disks_num < dis.len() / 2 || disks_num == 0 {
        return Err(Error::msg(format!(
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
