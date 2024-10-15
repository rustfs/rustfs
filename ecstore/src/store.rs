#![allow(clippy::map_entry)]

use crate::bucket::metadata;
use crate::bucket::metadata_sys::set_bucket_metadata;
use crate::disk::endpoint::EndpointType;
use crate::disk::MetaCacheEntry;
use crate::global::{is_dist_erasure, set_object_layer, GLOBAL_LOCAL_DISK_MAP, GLOBAL_LOCAL_DISK_SET_DRIVES};
use crate::heal::heal_commands::{HealOpts, HealResultItem, HealScanMode};
use crate::heal::heal_ops::HealObjectFn;
use crate::store_api::ObjectIO;
use crate::{
    bucket::metadata::BucketMetadata,
    disk::{error::DiskError, new_disk, DiskOption, DiskStore, WalkDirOptions, BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    endpoints::EndpointServerPools,
    error::{Error, Result},
    peer::S3PeerSys,
    sets::Sets,
    storage_class::default_partiy_count,
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
use s3s::dto::{ObjectLockConfiguration, ObjectLockEnabled};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::fs;
use tokio::sync::Semaphore;

use tracing::{debug, info, warn};
use uuid::Uuid;

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

        for (i, pool_eps) in endpoint_pools.as_ref().iter().enumerate() {
            // TODO: read from config parseStorageClass
            let partiy_count = default_partiy_count(pool_eps.drives_per_set);

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

            let sets = Sets::new(disks.clone(), pool_eps, &fm, i, partiy_count).await?;
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

        Ok(ec)
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

    async fn get_pool_info_existing_with_opts(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<(PoolObjInfo, Vec<Error>)> {
        internal_get_pool_info_existing_with_opts(&self.pools, bucket, object, opts).await
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

        unimplemented!()
    }
    async fn put_object(&self, bucket: &str, object: &str, data: PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        // checkPutObjectArgs

        let object = utils::path::encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object(bucket, object.as_str(), data, opts).await;
        }

        unimplemented!()
    }
}

#[async_trait::async_trait]
impl StorageAPI for ECStore {
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let buckets = self.peer_sys.list_bucket(opts).await?;

        Ok(buckets)
    }

    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        self.peer_sys.delete_bucket(bucket, opts).await?;

        // 删除meta
        self.delete_all(RUSTFS_META_BUCKET, format!("{}/{}", BUCKET_META_PREFIX, bucket).as_str())
            .await?;
        Ok(())
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        // TODO:  check valid bucket name

        // TODO: delete created bucket when error
        self.peer_sys.make_bucket(bucket, opts).await?;

        let mut meta = BucketMetadata::new(bucket);

        warn!("make bucket opsts {:?}", &opts);

        if opts.lock_enabled {
            let cfg = ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                ..Default::default()
            };

            meta.object_lock_config_xml = metadata::serialize::<ObjectLockConfiguration>(&cfg)?;

            warn!("make bucket add object_lock_config_xml {:?}", &meta.object_lock_config_xml);
            // FIXME: version config
        }

        meta.save(self).await?;

        set_bucket_metadata(bucket.to_string(), meta).await;

        // TODO: toObjectErr

        Ok(())
    }
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let info = self.peer_sys.get_bucket_info(bucket, opts).await?;

        Ok(info)
    }
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
                    if pinfo.object_info.delete_marker && opts.version_id.is_empty() {
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
        if pinfo.object_info.delete_marker && opts.version_id.is_empty() {
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
        let object = utils::path::encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_info(bucket, object.as_str(), opts).await;
        }

        unimplemented!()
    }

    async fn put_object_info(&self, bucket: &str, object: &str, info: ObjectInfo, opts: &ObjectOptions) -> Result<()> {
        let object = utils::path::encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object_info(bucket, object.as_str(), info, opts).await;
        }

        unimplemented!()
    }

    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        if self.single_pool() {
            return self.pools[0]
                .put_object_part(bucket, object, upload_id, part_id, data, opts)
                .await;
        }
        unimplemented!()
    }

    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        if self.single_pool() {
            return self.pools[0].new_multipart_upload(bucket, object, opts).await;
        }
        unimplemented!()
    }
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()> {
        if self.single_pool() {
            return self.pools[0].abort_multipart_upload(bucket, object, upload_id, opts).await;
        }

        unimplemented!()
    }
    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        if self.single_pool() {
            return self.pools[0]
                .complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
                .await;
        }
        unimplemented!()
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
            if errs.get(&i).is_none() {
                return Ok(results.remove(&i).unwrap());
            }
        }

        // No pool returned a nil error, return the first non 'not found' error
        for (k, err) in errs.iter() {
            match err.downcast_ref::<DiskError>() {
                Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => {}
                _ => return Ok(results.remove(&k).unwrap()),
            }
        }

        // At this stage, all errors are 'not found'
        if !version_id.is_empty() {
            return Err(Error::new(DiskError::FileVersionNotFound));
        }

        Err(Error::new(DiskError::FileNotFound))
    }
    async fn heal_objects(&self, bucket: &str, prefix: &str, opts: &HealOpts, func: HealObjectFn) -> Result<()> {
        let heal_entry = |bucket: String, entry: MetaCacheEntry, scan_mode: HealScanMode| async move {
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

            match entry.file_info_versions(&bucket) {
                Ok(fivs) => {
                    if opts.remove && !opts.dry_run {
                        if let Err(err) = self.check_abandoned_parts(&bucket, &entry.name, opts).await {
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
        };

        for (idx, pool) in self.pools.iter().enumerate() {
            if opts.pool.is_some() && opts.pool.unwrap() != idx {
                continue;
            }
            //TODO: IsSuspended

            for (idx, set) in pool.disk_set.iter().enumerate() {
                if opts.set.is_some() && opts.set.unwrap() != idx {
                    continue;
                }

                set.list
            }
        }
        todo!()
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
            return Err(errs[0]);
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
