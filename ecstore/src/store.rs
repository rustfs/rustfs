#![allow(clippy::map_entry)]

use crate::bucket::metadata;
use crate::bucket::metadata_sys::{self, init_bucket_metadata_sys, set_bucket_metadata};
use crate::bucket::utils::{check_valid_bucket_name, check_valid_bucket_name_strict, is_meta_bucketname};
use crate::config::GLOBAL_StorageClass;
use crate::config::{self, storageclass, GLOBAL_ConfigSys};
use crate::disk::endpoint::EndpointType;
use crate::disk::{DiskAPI, DiskInfo, DiskInfoOptions, MetaCacheEntry};
use crate::global::{
    is_dist_erasure, is_erasure_sd, set_global_deployment_id, set_object_layer, DISK_ASSUME_UNKNOWN_SIZE, DISK_FILL_FRACTION,
    DISK_MIN_INODES, DISK_RESERVE_FRACTION, GLOBAL_LOCAL_DISK_MAP, GLOBAL_LOCAL_DISK_SET_DRIVES,
};
use crate::heal::heal_commands::{HealOpts, HealResultItem, HealScanMode};
use crate::heal::heal_ops::HealObjectFn;
use crate::new_object_layer_fn;
use crate::store_api::{BackendByte, BackendDisks, BackendInfo, ListMultipartsInfo, ObjectIO, StorageInfo};
use crate::store_err::{
    is_err_bucket_exists, is_err_invalid_upload_id, is_err_object_not_found, is_err_version_not_found, StorageError,
};
use crate::store_init::ec_drives_no_config;
use crate::utils::crypto::base64_decode;
use crate::utils::path::{decode_dir_object, encode_dir_object, SLASH_SEPARATOR};
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
use backon::{ExponentialBuilder, Retryable};
use common::globals::{GLOBAL_Local_Node_Name, GLOBAL_Rustfs_Host, GLOBAL_Rustfs_Port};
use futures::future::join_all;
use glob::Pattern;
use http::HeaderMap;
use lazy_static::lazy_static;
use rand::Rng;
use s3s::dto::{BucketVersioningStatus, ObjectLockConfiguration, ObjectLockEnabled, VersioningConfiguration};
use std::cmp::Ordering;
use std::slice::Iter;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::fs;
use tokio::sync::Semaphore;

use tracing::{debug, info};
use uuid::Uuid;

const MAX_UPLOADS_LIST: usize = 10000;

#[derive(Debug, Clone)]
pub struct ECStore {
    pub id: uuid::Uuid,
    // pub disks: Vec<DiskStore>,
    pub disk_map: HashMap<usize, Vec<Option<DiskStore>>>,
    pub pools: Vec<Arc<Sets>>,
    pub peer_sys: S3PeerSys,
    // pub local_disks: Vec<DiskStore>,
}

impl ECStore {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(_address: String, endpoint_pools: EndpointServerPools) -> Result<Self> {
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

            let fm = (|| async {
                store_init::connect_load_init_formats(
                    first_is_local,
                    &disks,
                    pool_eps.set_count,
                    pool_eps.drives_per_set,
                    deployment_id,
                )
                .await
            })
            .retry(ExponentialBuilder::default().with_max_times(usize::MAX))
            .sleep(tokio::time::sleep)
            .notify(|err, dur: Duration| {
                info!("retrying get formats {:?} after {:?}", err, dur);
            })
            .await?;

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
                let path = disk.path().to_string_lossy().to_string();
                global_local_disk_map.insert(path, Some(disk.clone()));
            }
        }

        let peer_sys = S3PeerSys::new(&endpoint_pools);

        let ec = ECStore {
            id: deployment_id.unwrap(),
            disk_map,
            pools,
            peer_sys,
        };

        set_object_layer(ec.clone()).await;

        if let Some(dep_id) = deployment_id {
            set_global_deployment_id(dep_id).await;
        }

        Ok(ec)
    }

    pub async fn init(&self) -> Result<()> {
        config::init();
        GLOBAL_ConfigSys.init(self).await?;

        let buckets_list = self
            .list_bucket(&BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .map_err(|err| Error::from_string(err.to_string()))?;

        let buckets = buckets_list.iter().map(|v| v.name.clone()).collect();

        init_bucket_metadata_sys(self.clone(), buckets).await;

        Ok(())
    }

    pub fn init_local_disks() {}

    // pub fn local_disks(&self) -> Vec<DiskStore> {
    //     self.local_disks.clone()
    // }

    fn single_pool(&self) -> bool {
        self.pools.len() == 1
    }

    async fn list_path(&self, opts: &ListPathOptions) -> Result<ListObjectsInfo> {
        let objects = self.list_merged(opts).await?;

        let info = ListObjectsInfo {
            objects,
            ..Default::default()
        };
        Ok(info)
    }

    // 读所有
    async fn list_merged(&self, opts: &ListPathOptions) -> Result<Vec<ObjectInfo>> {
        let opts = WalkDirOptions {
            bucket: opts.bucket.clone(),
            ..Default::default()
        };

        // let (mut wr, mut rd) = tokio::io::duplex(1024);

        let mut futures = Vec::new();

        for sets in self.pools.iter() {
            for set in sets.disk_set.iter() {
                futures.push(set.walk_dir(&opts));
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
                    if !uniq.contains(&entry.name) {
                        uniq.insert(entry.name.clone());
                        // TODO: 过滤
                        if opts.limit > 0 && ress.len() as i32 >= opts.limit {
                            return Ok(ress);
                        }

                        if entry.is_object() {
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
    ) -> Result<(PoolObjInfo, Vec<Error>)> {
        internal_get_pool_info_existing_with_opts(&self.pools, bucket, object, opts).await
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

        let mut idx = 0;
        for result in results {
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

            idx += 1;
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

            let path = disk.path().to_string_lossy().to_string();

            global_local_disk_map.insert(path, Some(disk.clone()));

            set_drives.insert(ep.disk_idx, Some(disk.clone()));

            if ep.pool_idx.is_some() && ep.set_idx.is_some() && ep.disk_idx.is_some() {
                global_set_drives[ep.pool_idx.unwrap()][ep.set_idx.unwrap()][ep.disk_idx.unwrap()] = Some(disk.clone());
            }
        }
    }

    Ok(())
}

async fn internal_get_pool_info_existing_with_opts(
    pools: &[Arc<Sets>],
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
) -> Result<(PoolObjInfo, Vec<Error>)> {
    let mut futures = Vec::new();

    for pool in pools.iter() {
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

    for res in ress {
        // check
        if res.err.is_none() {
            // TODO: let errs = self.poolsWithObject()
            return Ok((res, Vec::new()));
        }
    }

    let ret = PoolObjInfo::default();

    Ok((ret, Vec::new()))
}

#[derive(Debug, Default)]
pub struct PoolObjInfo {
    pub index: usize,
    pub object_info: ObjectInfo,
    pub err: Option<Error>,
}

#[derive(Debug, Default)]
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
        let object = utils::path::encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_reader(bucket, object.as_str(), range, h, opts).await;
        }

        // TODO: nslock

        let mut opts = opts.clone();

        opts.no_lock = true;

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
        let (standard_scparities, rrscparities) = {
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

        let mut standard_scdata = Vec::new();
        let mut rrscdata = Vec::new();
        let mut drives_per_set = Vec::new();
        let mut total_sets = Vec::new();

        for (idx, set_count) in self.set_drive_counts().iter().enumerate() {
            if let Some(sc_parity) = standard_scparities {
                standard_scdata.push(set_count - sc_parity);
            }
            if let Some(rr_sc_parity) = rrscparities {
                rrscdata.push(set_count - rr_sc_parity);
            }
            total_sets.push(self.pools[idx].set_count);
            drives_per_set.push(*set_count);
        }

        BackendInfo {
            backend_type: BackendByte::Erasure,
            online_disks: BackendDisks::new(),
            offline_disks: BackendDisks::new(),
            standard_scdata,
            standard_scparities,
            rrscdata,
            rrscparities,
            total_sets,
            drives_per_set,
        }
    }
    async fn storage_info(&self) -> StorageInfo {
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
        StorageInfo {
            backend: Some(backend),
            disks,
        }
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
            // TODO: check bucket exists
            opts.force = true
        }

        self.peer_sys.delete_bucket(bucket, &opts).await?;

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
            meta.object_lock_config_xml = metadata::serialize::<ObjectLockConfiguration>(&enableObjcetLockConfig)?;
            meta.versioning_config_xml = metadata::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        if opts.versioning_enabled {
            meta.versioning_config_xml = metadata::serialize::<VersioningConfiguration>(&enableVersioningConfig)?;
        }

        meta.save(self).await?;

        set_bucket_metadata(bucket.to_string(), meta).await;

        Ok(())
    }
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let mut info = self.peer_sys.get_bucket_info(bucket, opts).await?;

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

        let mut jhs = Vec::new();
        let semaphore = Arc::new(Semaphore::new(num_cpus::get()));
        let pools = Arc::new(self.pools.clone());

        for obj in objects.iter() {
            let (semaphore, pools, bucket, object_name, opt) = (
                semaphore.clone(),
                pools.clone(),
                bucket.to_string(),
                obj.object_name.to_string(),
                ObjectOptions::default(),
            );

            let jh = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                internal_get_pool_info_existing_with_opts(pools.as_ref(), &bucket, &object_name, &opt).await
            });
            jhs.push(jh);
        }
        let mut results = Vec::new();
        for jh in jhs {
            results.push(jh.await.unwrap());
        }

        // 记录pool Index 对应的objects pool_idx -> objects idx
        let mut pool_index_objects = HashMap::new();

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

                    if !pool_index_objects.contains_key(&pinfo.index) {
                        pool_index_objects.insert(pinfo.index, vec![i]);
                    } else {
                        // let mut vals = pool_index_objects.
                        if let Some(val) = pool_index_objects.get_mut(&pinfo.index) {
                            val.push(i);
                        }
                    }
                }
                Err(e) => {
                    //TODO: check not found

                    del_errs[i] = Some(e)
                }
            }
        }

        if !pool_index_objects.is_empty() {
            for sets in self.pools.iter() {
                //  取pool idx 对应的 objects index
                let vals = pool_index_objects.get(&sets.pool_idx);
                if vals.is_none() {
                    continue;
                }

                let obj_idxs = vals.unwrap();
                //  取对应obj,理论上不会none
                let objs: Vec<ObjectToDelete> = obj_idxs.iter().filter_map(|&idx| objects.get(idx).cloned()).collect();

                if objs.is_empty() {
                    continue;
                }

                let (pdel_objs, perrs) = sets.delete_objects(bucket, objs, opts.clone()).await?;

                // perrs的顺序理论上跟obj_idxs顺序一致
                for (i, err) in perrs.into_iter().enumerate() {
                    let obj_idx = obj_idxs[i];

                    if err.is_some() {
                        del_errs[obj_idx] = err;
                    }

                    let mut dobj = pdel_objs.get(i).unwrap().clone();
                    dobj.object_name = utils::path::decode_dir_object(&dobj.object_name);

                    del_objects[obj_idx] = dobj;
                }
            }
        }

        Ok((del_objects, del_errs))
    }
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        if opts.delete_prefix {
            self.delete_prefix(bucket, object).await?;
            return Ok(ObjectInfo::default());
        }

        let object = utils::path::encode_dir_object(object);
        let object = object.as_str();

        // 查询在哪个pool
        let (mut pinfo, errs) = self.get_pool_info_existing_with_opts(bucket, object, &opts).await?;
        if pinfo.object_info.delete_marker && opts.version_id.is_none() {
            pinfo.object_info.name = utils::path::decode_dir_object(object);
            return Ok(pinfo.object_info);
        }

        if !errs.is_empty() {
            // TODO: deleteObjectFromAllPools
        }

        let mut obj = self.pools[pinfo.index].delete_object(bucket, object, opts.clone()).await?;
        obj.name = utils::path::decode_dir_object(object);

        Ok(obj)
    }

    // TODO: review
    async fn list_objects_v2(
        &self,
        bucket: &str,
        _prefix: &str,
        continuation_token: &str,
        _delimiter: &str,
        max_keys: i32,
        _fetch_owner: bool,
        _start_after: &str,
    ) -> Result<ListObjectsV2Info> {
        let opts = ListPathOptions {
            bucket: bucket.to_string(),
            limit: max_keys,
            ..Default::default()
        };

        let info = self.list_path(&opts).await?;

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
            max_uploads: max_uploads,
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
    async fn heal_format(&self, dry_run: bool) -> Result<HealResultItem> {
        unimplemented!()
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        unimplemented!()
    }
    async fn heal_object(&self, bucket: &str, object: &str, version_id: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let object = utils::path::encode_dir_object(object);
        let mut errs = HashMap::new();
        let mut results = HashMap::new();
        for (idx, pool) in self.pools.iter().enumerate() {
            //TODO: IsSuspended
            match pool.heal_object(bucket, &object, version_id, opts).await {
                Ok(mut result) => {
                    result.object = utils::path::decode_dir_object(&result.object);
                    results.insert(idx, result);
                }
                Err(err) => {
                    errs.insert(idx, err);
                }
            }
        }

        // Return the first nil error
        for i in 0..self.pools.len() {
            if !errs.contains_key(&i) {
                return Ok(results.remove(&i).unwrap());
            }
        }

        // No pool returned a nil error, return the first non 'not found' error
        for (k, err) in errs.iter() {
            match err.downcast_ref::<DiskError>() {
                Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => {}
                _ => return Ok(results.remove(k).unwrap()),
            }
        }

        // At this stage, all errors are 'not found'
        if !version_id.is_empty() {
            return Err(Error::new(DiskError::FileVersionNotFound));
        }

        Err(Error::new(DiskError::FileNotFound))
    }
    async fn heal_objects(&self, bucket: &str, prefix: &str, opts: &HealOpts, func: HealObjectFn) -> Result<()> {
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

                if let Err(err) = set.list_and_heal(bucket, prefix, opts, func.clone()).await {
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

fn is_valid_object_prefix(object: &str) -> bool {
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
    _check_bucket_and_object_names(bucket, object)
}

fn _check_get_obj_args(bucket: &str, object: &str) -> Result<()> {
    _check_bucket_and_object_names(bucket, object)
}

fn _check_del_obj_args(bucket: &str, object: &str) -> Result<()> {
    _check_bucket_and_object_names(bucket, object)
}

fn _check_bucket_and_object_names(bucket: &str, object: &str) -> Result<()> {
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

fn _check_list_parts_args(bucket: &str, object: &str, upload_id: &str) -> Result<()> {
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

pub async fn heal_entry(
    bucket: String,
    entry: MetaCacheEntry,
    scan_mode: HealScanMode,
    opts: HealOpts,
    func: HealObjectFn,
) -> Result<()> {
    if entry.is_dir() {
        return Ok(());
    }

    // We might land at .metacache, .trash, .multipart
    // no need to heal them skip, only when bucket
    // is '.minio.sys'
    if bucket == RUSTFS_META_BUCKET {
        if Pattern::new("buckets/*/.metacache/*")
            .map(|p| p.matches(&entry.name))
            .unwrap_or(false)
            || Pattern::new("tmp/*").map(|p| p.matches(&entry.name)).unwrap_or(false)
            || Pattern::new("multipart/*").map(|p| p.matches(&entry.name)).unwrap_or(false)
            || Pattern::new("tmp-old/*").map(|p| p.matches(&entry.name)).unwrap_or(false)
        {
            return Ok(());
        }
    }

    let layer = new_object_layer_fn();
    let lock = layer.read().await;
    let store = match lock.as_ref() {
        Some(s) => s,
        None => return Err(Error::msg("errServerNotInitialized")),
    };

    match entry.file_info_versions(&bucket) {
        Ok(fivs) => {
            if opts.remove && !opts.dry_run {
                if let Err(err) = store.check_abandoned_parts(&bucket, &entry.name, &opts).await {
                    return Err(Error::from_string(format!(
                        "unable to check object {}/{} for abandoned data: {}",
                        bucket, entry.name, err
                    )));
                }
            }

            for version in fivs.versions.iter() {
                let version_id = version.version_id.map_or("".to_string(), |version_id| version_id.to_string());
                if let Err(err) = func(&bucket, &entry.name, &version_id, scan_mode) {
                    match err.downcast_ref::<DiskError>() {
                        Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => {}
                        _ => return Err(err),
                    }
                }
            }
        }
        Err(_) => {
            return func(&bucket, &entry.name, "", scan_mode);
        }
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
