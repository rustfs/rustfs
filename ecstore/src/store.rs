use crate::{
    bucket_meta::BucketMetadata,
    disk::{error::DiskError, DeleteOptions, DiskOption, DiskStore, WalkDirOptions, BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    endpoints::EndpointServerPools,
    error::{Error, Result},
    peer::S3PeerSys,
    sets::Sets,
    store_api::{
        BucketInfo, BucketOptions, CompletePart, DeletedObject, GetObjectReader, HTTPRangeSpec, ListObjectsInfo,
        ListObjectsV2Info, MakeBucketOptions, MultipartUploadResult, ObjectInfo, ObjectOptions, ObjectToDelete, PartInfo,
        PutObjReader, StorageAPI,
    },
    store_init, utils,
};
use futures::future::join_all;
use http::HeaderMap;
use s3s::{dto::StreamingBlob, Body};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tracing::{debug, warn};
use uuid::Uuid;

use lazy_static::lazy_static;

lazy_static! {
    pub static ref GLOBAL_OBJECT_API: Arc<Mutex<Option<ECStore>>> = Arc::new(Mutex::new(None));
}

pub fn new_object_layer_fn() -> Arc<Mutex<Option<ECStore>>> {
    // 这里不需要显式地锁定和解锁，因为 Arc 提供了必要的线程安全性
    GLOBAL_OBJECT_API.clone()
}

async fn set_object_layer(o: ECStore) {
    let mut global_object_api = GLOBAL_OBJECT_API.lock().await;
    *global_object_api = Some(o);
}

#[derive(Debug)]
pub struct ECStore {
    pub id: uuid::Uuid,
    // pub disks: Vec<DiskStore>,
    pub disk_map: HashMap<usize, Vec<Option<DiskStore>>>,
    pub pools: Vec<Sets>,
    pub peer_sys: S3PeerSys,
    pub local_disks: Vec<DiskStore>,
}

impl ECStore {
    pub async fn new(_address: String, endpoint_pools: EndpointServerPools) -> Result<()> {
        // let layouts = DisksLayout::try_from(endpoints.as_slice())?;

        let mut deployment_id = None;

        // let (endpoint_pools, _) = EndpointServerPools::create_server_endpoints(address.as_str(), &layouts)?;

        let mut pools = Vec::with_capacity(endpoint_pools.as_ref().len());
        let mut disk_map = HashMap::with_capacity(endpoint_pools.as_ref().len());

        let first_is_local = endpoint_pools.first_local();

        let mut local_disks = Vec::new();

        for (i, pool_eps) in endpoint_pools.as_ref().iter().enumerate() {
            // TODO: read from config parseStorageClass
            let partiy_count = store_init::default_partiy_count(pool_eps.drives_per_set);

            let (disks, errs) = crate::store_init::init_disks(
                &pool_eps.endpoints,
                &DiskOption {
                    cleanup: true,
                    health_check: true,
                },
            )
            .await;

            DiskError::check_disk_fatal_errs(&errs)?;

            let fm = store_init::do_init_format_file(
                first_is_local,
                &disks,
                pool_eps.set_count,
                pool_eps.drives_per_set,
                deployment_id,
            )
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

            let sets = Sets::new(disks.clone(), pool_eps, &fm, i, partiy_count)?;

            pools.push(sets);

            disk_map.insert(i, disks);
        }

        let peer_sys = S3PeerSys::new(&endpoint_pools, local_disks.clone());

        let ec = ECStore {
            id: deployment_id.unwrap(),
            disk_map,
            pools,
            local_disks,
            peer_sys,
        };

        set_object_layer(ec).await;

        Ok(())
    }

    pub fn local_disks(&self) -> Vec<DiskStore> {
        self.local_disks.clone()
    }

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
                for disk in set.disks.iter() {
                    if disk.is_none() {
                        continue;
                    }

                    let disk = disk.as_ref().unwrap();
                    let opts = opts.clone();
                    // let mut wr = &mut wr;
                    futures.push(disk.walk_dir(opts));
                    // tokio::spawn(async move { disk.walk_dir(opts, wr).await });
                }
            }
        }

        let results = join_all(futures).await;

        let mut errs = Vec::new();
        let mut ress = Vec::new();
        let mut uniq = HashSet::new();

        for res in results {
            match res {
                Ok(entrys) => {
                    for entry in entrys {
                        if !uniq.contains(&entry.name) {
                            uniq.insert(entry.name.clone());
                            // TODO: 过滤
                            if opts.limit > 0 && ress.len() as i32 >= opts.limit {
                                return Ok(ress);
                            }

                            if entry.is_object() {
                                let fi = entry.to_fileinfo(&opts.bucket)?;
                                if fi.is_some() {
                                    ress.push(fi.unwrap().into_object_info(&opts.bucket, &entry.name, false));
                                }
                                continue;
                            }

                            if entry.is_dir() {
                                ress.push(ObjectInfo {
                                    is_dir: true,
                                    bucket: opts.bucket.clone(),
                                    name: entry.name,
                                    ..Default::default()
                                });
                            }
                        }
                    }
                    errs.push(None);
                }
                Err(e) => errs.push(Some(e)),
            }
        }

        warn!("list_merged errs {:?}", errs);

        Ok(ress)
    }

    async fn delete_all(&self, bucket: &str, prefix: &str) -> Result<()> {
        let mut futures = Vec::new();
        for sets in self.pools.iter() {
            for set in sets.disk_set.iter() {
                for disk in set.disks.iter() {
                    if disk.is_none() {
                        continue;
                    }

                    let disk = disk.as_ref().unwrap();
                    futures.push(disk.delete(
                        bucket,
                        prefix,
                        DeleteOptions {
                            recursive: true,
                            immediate: false,
                        },
                    ));
                }
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
        let mut futures = Vec::new();

        for pool in self.pools.iter() {
            futures.push(pool.get_object_info(bucket, object, opts));
        }

        let results = join_all(futures).await;

        let mut ress = Vec::new();

        let mut i = 0;

        // join_all结果跟输入顺序一致
        for res in results {
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
            i += 1;
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
impl StorageAPI for ECStore {
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let buckets = self.peer_sys.list_bucket(opts).await?;

        Ok(buckets)
    }
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        // TODO:  check valid bucket name

        // TODO: delete created bucket when error
        self.peer_sys.make_bucket(bucket, opts).await?;

        let meta = BucketMetadata::new(bucket);
        let data = meta.marshal_msg()?;
        let file_path = meta.save_file_path();

        // TODO: wrap hash reader

        let content_len = data.len();

        let body = Body::from(data);

        let reader = PutObjReader::new(StreamingBlob::from(body), content_len);

        self.put_object(
            RUSTFS_META_BUCKET,
            &file_path,
            reader,
            &ObjectOptions {
                max_parity: true,
                ..Default::default()
            },
        )
        .await?;

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

        // TODO: limte 限制并发数量
        let opt = ObjectOptions::default();
        // 取所有poolObjInfo
        let mut futures = Vec::new();
        for obj in objects.iter() {
            futures.push(self.get_pool_info_existing_with_opts(bucket, &obj.object_name, &opt));
        }

        let results = join_all(futures).await;

        // 记录pool Index 对应的objects pool_idx -> objects idx
        let mut pool_index_objects = HashMap::new();

        let mut i = 0;
        for res in results {
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

            i += 1;
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
                let objs: Vec<ObjectToDelete> = obj_idxs
                    .iter()
                    .filter_map(|&idx| {
                        if let Some(obj) = objects.get(idx) {
                            Some(obj.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                if objs.is_empty() {
                    continue;
                }

                let (pdel_objs, perrs) = sets.delete_objects(bucket, objs, opts.clone()).await?;

                // perrs的顺序理论上跟obj_idxs顺序一致
                let mut i = 0;
                for err in perrs {
                    let obj_idx = obj_idxs[i];

                    if err.is_some() {
                        del_errs[obj_idx] = err;
                    }

                    let mut dobj = pdel_objs.get(i).unwrap().clone();
                    dobj.object_name = utils::path::decode_dir_object(&dobj.object_name);

                    del_objects[obj_idx] = dobj;

                    i += 1;
                }
            }
        }

        Ok((del_objects, del_errs))
    }
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        if opts.delete_prefix {
            self.delete_prefix(bucket, &object).await?;
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
    async fn put_object(&self, bucket: &str, object: &str, data: PutObjReader, opts: &ObjectOptions) -> Result<()> {
        // checkPutObjectArgs

        let object = utils::path::encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object(bucket, object.as_str(), data, opts).await;
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

    async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        self.peer_sys.delete_bucket(bucket).await?;

        // 删除meta
        self.delete_all(RUSTFS_META_BUCKET, format!("{}/{}", BUCKET_META_PREFIX, bucket).as_str())
            .await?;
        Ok(())
    }
}
