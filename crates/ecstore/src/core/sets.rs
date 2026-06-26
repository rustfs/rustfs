#![allow(clippy::map_entry)]
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

use crate::disk::error_reduce::count_errs;
use crate::error::{Error, Result};
use crate::layout::set_heal::{formats_to_drives_info, new_heal_format_sets};
use crate::storage_api_contracts::{
    bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
    list::{StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr, StorageWalkOptions},
    multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo},
    object::{DeletedObject, ObjectIO as _, ObjectOperations as _, ObjectToDelete},
    range::HTTPRangeSpec,
};
use crate::{
    disk::{
        DiskAPI, DiskOption, DiskStore,
        error::DiskError,
        format::{DistributionAlgoVersion, FormatV3},
        new_disk,
    },
    endpoints::{Endpoints, PoolEndpoints},
    error::StorageError,
    object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader},
    runtime::sources as runtime_sources,
    set_disk::SetDisks,
    store_init::{check_format_erasure_values, get_format_erasure_in_quorum, load_format_erasure_all, save_format_file},
};
use futures::{
    future::join_all,
    stream::{FuturesUnordered, StreamExt},
};
use http::HeaderMap;
use rustfs_common::heal_channel::HealOpts;
use rustfs_common::heal_channel::{DriveState, HealItemType};
use rustfs_filemeta::FileInfo;
use rustfs_lock::NamespaceLockWrapper;
use rustfs_lock::client::LockClient;
use rustfs_madmin::heal_commands::{HealDriveInfo, HealResultItem};
use rustfs_utils::{crc_hash, path::path_join_buf, sip_hash};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use tracing::{error, info};
use uuid::Uuid;

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

#[derive(Debug, Clone)]
pub struct Sets {
    pub id: Uuid,
    // pub sets: Vec<Objects>,
    // pub disk_set: Vec<Vec<Option<DiskStore>>>, // [set_count_idx][set_drive_count_idx] = disk_idx
    pub disk_set: Vec<Arc<SetDisks>>, // [set_count_idx][set_drive_count_idx] = disk_idx
    pub pool_idx: usize,
    pub endpoints: PoolEndpoints,
    pub format: FormatV3,
    pub parity_count: usize,
    pub set_count: usize,
    pub set_drive_count: usize,
    pub default_parity_count: usize,
    pub distribution_algo: DistributionAlgoVersion,
    exit_signal: Option<Sender<()>>,
}

impl Drop for Sets {
    fn drop(&mut self) {
        if let Some(exit_signal) = self.exit_signal.take() {
            let _ = exit_signal.send(());
        }
    }
}

impl Sets {
    #[tracing::instrument(level = "debug", skip(disks, endpoints, fm, pool_idx, parity_count))]
    pub async fn new(
        disks: Vec<Option<DiskStore>>,
        endpoints: &PoolEndpoints,
        fm: &FormatV3,
        pool_idx: usize,
        parity_count: usize,
    ) -> Result<Arc<Self>> {
        let set_count = fm.erasure.sets.len();
        let set_drive_count = fm.erasure.sets[0].len();

        let mut disk_set = Vec::with_capacity(set_count);

        // Get lock clients from global storage
        let lock_clients = runtime_sources::global_lock_clients();

        for i in 0..set_count {
            let mut set_drive = Vec::with_capacity(set_drive_count);
            let mut set_endpoints = Vec::with_capacity(set_drive_count);
            let mut set_lock_clients: HashMap<String, Arc<dyn LockClient>> = HashMap::new();
            for j in 0..set_drive_count {
                let idx = i * set_drive_count + j;
                let mut disk = disks[idx].clone();

                let endpoint = endpoints.endpoints.as_ref()[idx].clone();

                if let Some(lock_clients_map) = lock_clients {
                    let host_port = endpoint.host_port();
                    if let Some(lock_client) = lock_clients_map.get(&host_port)
                        && !set_lock_clients.contains_key(&host_port)
                    {
                        set_lock_clients.insert(host_port, lock_client.clone());
                    }
                }

                set_endpoints.push(endpoint);

                if disk.is_none() {
                    warn!("sets new set_drive {}-{} is none", i, j);
                    set_drive.push(None);
                    continue;
                }

                if disk.as_ref().unwrap().is_local() && runtime_sources::setup_is_dist_erasure().await {
                    let local_disk = runtime_sources::local_disk_set_drive(pool_idx, i, j).await;

                    if local_disk.is_none() {
                        warn!("sets new set_drive {}-{} local_disk is none", i, j);
                        set_drive.push(None);
                        continue;
                    }

                    let _ = disk.as_ref().unwrap().close().await;

                    disk = local_disk;
                }

                let has_disk_id = disk.as_ref().unwrap().get_disk_id().await.unwrap_or_else(|err| {
                    if err == DiskError::UnformattedDisk {
                        error!("get_disk_id err {:?}", err);
                    } else {
                        warn!("get_disk_id err {:?}", err);
                    }

                    None
                });

                if let Some(_disk_id) = has_disk_id {
                    set_drive.push(disk);
                } else {
                    error!("sets new set_drive {}-{} get_disk_id is none", i, j);
                    set_drive.push(None);
                }
            }

            let lockers = set_lock_clients.values().cloned().collect::<Vec<Arc<dyn LockClient>>>();
            let set_disks = SetDisks::new(
                runtime_sources::local_node_name().await,
                Arc::new(RwLock::new(set_drive)),
                set_drive_count,
                parity_count,
                i,
                pool_idx,
                set_endpoints,
                fm.clone(),
                lockers,
            )
            .await;

            disk_set.push(set_disks);
        }

        let (tx, rx) = tokio::sync::broadcast::channel(1);

        let sets = Arc::new(Self {
            id: fm.id,
            // sets: todo!(),
            disk_set,
            pool_idx,
            endpoints: endpoints.clone(),
            format: fm.clone(),
            parity_count,
            set_count,
            set_drive_count,
            default_parity_count: parity_count,
            distribution_algo: fm.erasure.distribution_algo.clone(),
            exit_signal: Some(tx),
        });

        let asets = sets.clone();

        let rx1 = rx.resubscribe();
        tokio::spawn(async move { asets.monitor_and_connect_endpoints(rx1).await });

        // let sets2 = sets.clone();
        // let rx2 = rx.resubscribe();
        // tokio::spawn(async move { sets2.cleanup_deleted_objects_loop(rx2).await });

        Ok(sets)
    }

    pub fn set_drive_count(&self) -> usize {
        self.set_drive_count
    }

    // pub async fn cleanup_deleted_objects_loop(self: Arc<Self>, mut rx: Receiver<()>) {
    //     tokio::time::sleep(Duration::from_secs(5)).await;

    //     info!("start cleanup_deleted_objects_loop");

    //     // TODO: config interval
    //     let mut interval = tokio::time::interval(Duration::from_secs(15 * 3));
    //     loop {
    //         tokio::select! {
    //            _= interval.tick()=>{

    //             info!("cleanup_deleted_objects_loop tick");

    //             for set in self.disk_set.iter() {
    //                 set.clone().cleanup_deleted_objects().await;
    //             }

    //             interval.reset();
    //            },

    //            _ = rx.recv() => {
    //             warn!("cleanup_deleted_objects_loop ctx cancelled");
    //             break;
    //            }
    //         }
    //     }

    //     warn!("cleanup_deleted_objects_loop exit");
    // }

    pub async fn monitor_and_connect_endpoints(&self, mut rx: Receiver<()>) {
        tokio::time::sleep(Duration::from_secs(5)).await;

        info!("start monitor_and_connect_endpoints");

        self.connect_disks().await;

        // TODO: config interval
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        loop {
            tokio::select! {
               _= interval.tick()=>{
                // debug!("tick...");
                self.connect_disks().await;

                interval.reset();
               },

               _ = rx.recv() => {
                warn!("monitor_and_connect_endpoints ctx cancelled");
                break;
               }
            }
        }

        warn!("monitor_and_connect_endpoints exit");
    }

    async fn connect_disks(&self) {
        // debug!("start connect_disks ...");
        for set in self.disk_set.iter() {
            set.connect_disks().await;
        }
        // debug!("done connect_disks ...");
    }

    pub fn get_disks(&self, set_idx: usize) -> Arc<SetDisks> {
        self.disk_set[set_idx].clone()
    }

    pub fn get_disks_by_key(&self, key: &str) -> Arc<SetDisks> {
        self.get_disks(self.get_hashed_set_index(key))
    }

    pub(crate) async fn storage_info_snapshot(&self) -> rustfs_madmin::StorageInfo {
        let mut futures = Vec::with_capacity(self.disk_set.len());

        for set in self.disk_set.iter() {
            futures.push(set.storage_info_snapshot())
        }

        let results = join_all(futures).await;
        let mut disks = Vec::new();

        for res in results.into_iter() {
            disks.extend_from_slice(&res.disks);
        }

        rustfs_madmin::StorageInfo {
            disks,
            ..Default::default()
        }
    }

    pub(crate) async fn local_storage_info_snapshot(&self) -> rustfs_madmin::StorageInfo {
        let mut futures = Vec::with_capacity(self.disk_set.len());

        for set in self.disk_set.iter() {
            futures.push(set.local_storage_info_snapshot())
        }

        let results = join_all(futures).await;
        let mut disks = Vec::new();

        for res in results.into_iter() {
            disks.extend_from_slice(&res.disks);
        }

        rustfs_madmin::StorageInfo {
            disks,
            ..Default::default()
        }
    }

    fn get_hashed_set_index(&self, input: &str) -> usize {
        match self.distribution_algo {
            DistributionAlgoVersion::V1 => crc_hash(input, self.disk_set.len()),

            DistributionAlgoVersion::V2 | DistributionAlgoVersion::V3 => sip_hash(input, self.disk_set.len(), self.id.as_bytes()),
        }
    }

    // async fn commit_rename_data_dir(
    //     &self,
    //     disks: &Vec<Option<DiskStore>>,
    //     bucket: &str,
    //     object: &str,
    //     data_dir: &str,
    //     // write_quorum: usize,
    // ) -> Vec<Option<Error>> {
    //     unimplemented!()
    // }

    async fn delete_prefix(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let mut futures = Vec::new();
        let mut opt = opts.clone();
        opt.delete_prefix = true;

        for set in self.disk_set.iter() {
            futures.push(set.delete_object(bucket, object, opt.clone()));
        }

        let _results = join_all(futures).await;

        Ok(())
    }
}

// #[derive(Debug)]
// pub struct Objects {
//     pub endpoints: Vec<Endpoint>,
//     pub disks: Vec<usize>,
//     pub set_index: usize,
//     pub pool_index: usize,
//     pub set_drive_count: usize,
//     pub default_parity_count: usize,
// }

struct DelObj {
    // set_idx: usize,
    orig_idx: usize,
    obj: ObjectToDelete,
}

fn apply_delete_objects_results(
    del_objects: &mut [DeletedObject],
    del_errs: &mut [Option<Error>],
    set_objects: &[DelObj],
    dobjects: &[DeletedObject],
    errs: Vec<Option<Error>>,
) {
    for (i, err) in errs.into_iter().enumerate() {
        let obj = set_objects
            .get(i)
            .expect("delete_objects should return errors aligned with input objects");

        del_errs[obj.orig_idx] = err;
        del_objects[obj.orig_idx] = dobjects
            .get(i)
            .expect("delete_objects should return objects aligned with input objects")
            .clone();
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::object::ObjectIO for Sets {
    type Error = Error;
    type RangeSpec = HTTPRangeSpec;
    type HeaderMap = HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = GetObjectReader;
    type PutObjectReader = PutObjReader;

    #[tracing::instrument(level = "debug", skip(self, object, h, opts))]
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        self.get_disks_by_key(object)
            .get_object_reader(bucket, object, range, h, opts)
            .await
    }
    #[tracing::instrument(level = "debug", skip(self, data))]
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.get_disks_by_key(object).put_object(bucket, object, data, opts).await
    }
}

#[async_trait::async_trait]
impl BucketOperations for Sets {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        for set in &self.disk_set {
            set.make_bucket(bucket, opts).await?;
        }

        Ok(())
    }
    #[tracing::instrument(skip(self))]
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        let mut first_err = None;
        for set in &self.disk_set {
            match set.get_bucket_info(bucket, opts).await {
                Ok(info) => return Ok(info),
                Err(err) if first_err.is_none() => first_err = Some(err),
                Err(_) => {}
            }
        }

        Err(first_err.unwrap_or_else(|| StorageError::BucketNotFound(bucket.to_string())))
    }

    #[tracing::instrument(skip(self))]
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let mut buckets = HashMap::new();
        let mut first_err = None;

        for set in &self.disk_set {
            match set.list_bucket(opts).await {
                Ok(set_buckets) => {
                    for bucket in set_buckets {
                        buckets.entry(bucket.name.clone()).or_insert(bucket);
                    }
                }
                Err(err) if first_err.is_none() => first_err = Some(err),
                Err(_) => {}
            }
        }

        if buckets.is_empty()
            && let Some(err) = first_err
        {
            return Err(err);
        }

        let mut buckets = buckets.into_values().collect::<Vec<_>>();
        buckets.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(buckets)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        for set in &self.disk_set {
            set.delete_bucket(bucket, opts).await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::object::ObjectOperations for Sets {
    type Error = Error;
    type ObjectInfo = ObjectInfo;
    type ObjectOptions = ObjectOptions;
    type FileInfo = FileInfo;
    type ObjectToDelete = ObjectToDelete;
    type DeletedObject = DeletedObject;

    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.get_disks_by_key(object).get_object_info(bucket, object, opts).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let gor = self.get_object_reader(bucket, object, None, HeaderMap::new(), opts).await?;
        let mut reader = gor.stream;

        // Stream data to sink instead of reading all into memory to prevent OOM
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
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
        let src_set = self.get_disks_by_key(src_object);
        let dst_set = self.get_disks_by_key(dst_object);

        let cp_src_dst_same = path_join_buf(&[src_bucket, src_object]) == path_join_buf(&[dst_bucket, dst_object]);

        if cp_src_dst_same {
            if let (Some(src_vid), Some(dst_vid)) = (&src_opts.version_id, &dst_opts.version_id)
                && src_vid == dst_vid
            {
                return src_set
                    .copy_object(src_bucket, src_object, dst_bucket, dst_object, src_info, src_opts, dst_opts)
                    .await;
            }

            if !dst_opts.versioned && src_opts.version_id.is_none() {
                return src_set
                    .copy_object(src_bucket, src_object, dst_bucket, dst_object, src_info, src_opts, dst_opts)
                    .await;
            }

            if dst_opts.versioned && src_opts.version_id != dst_opts.version_id {
                src_info.version_only = true;
                return src_set
                    .copy_object(src_bucket, src_object, dst_bucket, dst_object, src_info, src_opts, dst_opts)
                    .await;
            }
        }

        let put_opts = ObjectOptions {
            user_defined: dst_opts.user_defined.clone(),
            versioned: dst_opts.versioned,
            version_id: dst_opts.version_id.clone(),
            mod_time: dst_opts.mod_time,
            http_preconditions: dst_opts.http_preconditions.clone(),
            ..Default::default()
        };

        if let Some(put_object_reader) = src_info.put_object_reader.as_mut() {
            return dst_set.put_object(dst_bucket, dst_object, put_object_reader, &put_opts).await;
        }

        Err(StorageError::InvalidArgument(
            src_bucket.to_owned(),
            src_object.to_owned(),
            "put_object_reader2 is none".to_owned(),
        ))
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()> {
        self.get_disks_by_key(object)
            .delete_object_version(bucket, object, fi, force_del_marker)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        if opts.delete_prefix && !opts.delete_prefix_object {
            self.delete_prefix(bucket, object, &opts).await?;
            return Ok(ObjectInfo::default());
        }

        self.get_disks_by_key(object).delete_object(bucket, object, opts).await
    }

    #[tracing::instrument(skip(self))]
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        // Default return value
        let mut del_objects = vec![DeletedObject::default(); objects.len()];

        let mut del_errs = Vec::with_capacity(objects.len());
        for _ in 0..objects.len() {
            del_errs.push(None)
        }

        let mut set_obj_map = HashMap::new();

        // hash key
        for (i, obj) in objects.iter().enumerate() {
            let idx = self.get_hashed_set_index(obj.object_name.as_str());

            if !set_obj_map.contains_key(&idx) {
                set_obj_map.insert(
                    idx,
                    vec![DelObj {
                        // set_idx: idx,
                        orig_idx: i,
                        obj: obj.clone(),
                    }],
                );
            } else if let Some(val) = set_obj_map.get_mut(&idx) {
                val.push(DelObj {
                    // set_idx: idx,
                    orig_idx: i,
                    obj: obj.clone(),
                });
            }
        }

        let max_concurrent = set_obj_map.len().min(num_cpus::get()).max(1);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        let mut futures = FuturesUnordered::new();
        let bucket = bucket.to_string();

        for (k, v) in set_obj_map {
            let disks = self.get_disks(k);
            let objs: Vec<ObjectToDelete> = v.iter().map(|v| v.obj.clone()).collect();
            let bucket = bucket.clone();
            let opts = opts.clone();
            let semaphore = semaphore.clone();

            futures.push(async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .expect("delete_objects semaphore should remain open");
                let (dobjects, errs) = disks.delete_objects(&bucket, objs, opts).await;
                (v, dobjects, errs)
            });
        }

        while let Some((v, dobjects, errs)) = futures.next().await {
            apply_delete_objects_results(&mut del_objects, &mut del_errs, &v, &dobjects, errs);
        }

        (del_objects, del_errs)
    }

    #[tracing::instrument(skip(self))]
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.get_disks_by_key(object).put_object_metadata(bucket, object, opts).await
    }

    #[tracing::instrument(skip(self))]
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        self.get_disks_by_key(object).get_object_tags(bucket, object, opts).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.get_disks_by_key(object)
            .put_object_tags(bucket, object, tags, opts)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.get_disks_by_key(object).delete_object_tags(bucket, object, opts).await
    }

    #[tracing::instrument(skip(self))]
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        self.get_disks_by_key(object).add_partial(bucket, object, version_id).await
    }

    #[tracing::instrument(skip(self))]
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.get_disks_by_key(object).transition_object(bucket, object, opts).await
    }

    #[tracing::instrument(skip(self))]
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.get_disks_by_key(object)
            .restore_transitioned_object(bucket, object, opts)
            .await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::list::ListOperations for Sets {
    type Error = Error;
    type ListObjectsV2Info = ListObjectsV2Info;
    type ListObjectVersionsInfo = ListObjectVersionsInfo;
    type ObjectInfoOrErr = ObjectInfoOrErr;
    type WalkOptions = WalkOptions;
    type WalkCancellation = CancellationToken;
    type WalkResultSender = tokio::sync::mpsc::Sender<ObjectInfoOrErr>;

    #[tracing::instrument(skip(self))]
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

    #[tracing::instrument(skip(self))]
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
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::multipart::MultipartOperations for Sets {
    type Error = Error;
    type ObjectInfo = ObjectInfo;
    type ObjectOptions = ObjectOptions;
    type PutObjectReader = PutObjReader;
    type CompletePart = CompletePart;
    type ListMultipartsInfo = ListMultipartsInfo;
    type MultipartUploadResult = MultipartUploadResult;
    type PartInfo = PartInfo;
    type MultipartInfo = MultipartInfo;
    type ListPartsInfo = ListPartsInfo;

    #[tracing::instrument(skip(self))]
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo> {
        self.get_disks_by_key(prefix)
            .list_multipart_uploads(bucket, prefix, key_marker, upload_id_marker, delimiter, max_uploads)
            .await
    }
    #[tracing::instrument(skip(self))]
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        self.get_disks_by_key(object).new_multipart_upload(bucket, object, opts).await
    }

    #[tracing::instrument(skip(self))]
    async fn copy_object_part(
        &self,
        _src_bucket: &str,
        _src_object: &str,
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
        Err(StorageError::NotImplemented)
    }

    #[tracing::instrument(skip(self))]
    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        self.get_disks_by_key(object)
            .put_object_part(bucket, object, upload_id, part_id, data, opts)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        self.get_disks_by_key(object)
            .get_multipart_info(bucket, object, upload_id, opts)
            .await
    }

    async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        max_parts: usize,
        opts: &ObjectOptions,
    ) -> Result<ListPartsInfo> {
        self.get_disks_by_key(object)
            .list_object_parts(bucket, object, upload_id, part_number_marker, max_parts, opts)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()> {
        self.get_disks_by_key(object)
            .abort_multipart_upload(bucket, object, upload_id, opts)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.get_disks_by_key(object)
            .complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
            .await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::heal::HealOperations for Sets {
    type Error = Error;
    type HealResultItem = HealResultItem;
    type HealOptions = HealOpts;

    #[tracing::instrument(skip(self))]
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        let (disks, _) = init_storage_disks_with_errors(
            &self.endpoints.endpoints,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await;
        let (formats, errs) = load_format_erasure_all(&disks, true).await;
        if let Err(err) = check_format_erasure_values(&formats, self.set_drive_count) {
            info!("failed to check formats erasure values: {}", err);
            return Ok((HealResultItem::default(), Some(err)));
        }
        let ref_format = match get_format_erasure_in_quorum(&formats) {
            Ok(format) => format,
            Err(err) => return Ok((HealResultItem::default(), Some(err))),
        };
        let mut res = HealResultItem {
            heal_item_type: HealItemType::Metadata.to_string(),
            detail: "disk-format".to_string(),
            disk_count: self.set_count * self.set_drive_count,
            set_count: self.set_count,
            ..Default::default()
        };
        let before_derives = formats_to_drives_info(&self.endpoints.endpoints, &formats, &errs);
        res.before.drives = vec![HealDriveInfo::default(); before_derives.len()];
        res.after.drives = vec![HealDriveInfo::default(); before_derives.len()];

        for v in before_derives.iter() {
            res.before.drives.push(v.clone());
            res.after.drives.push(v.clone());
        }
        if count_errs(&errs, &DiskError::UnformattedDisk) == 0 {
            info!("disk formats success, NoHealRequired, errs: {:?}", errs);
            return Ok((res, Some(StorageError::NoHealRequired)));
        }

        // if !self.format.eq(&ref_format) {
        //     info!("format ({:?}) not eq ref_format ({:?})", self.format, ref_format);
        //     return Ok((res, Some(Error::new(DiskError::CorruptedFormat))));
        // }

        let (new_format_sets, _) = new_heal_format_sets(&ref_format, self.set_count, self.set_drive_count, &formats, &errs);
        if !dry_run {
            let mut tmp_new_formats = vec![None; self.set_count * self.set_drive_count];
            for (i, set) in new_format_sets.iter().enumerate() {
                for (j, fm) in set.iter().enumerate() {
                    if let Some(fm) = fm {
                        res.after.drives[i * self.set_drive_count + j].uuid = fm.erasure.this.to_string();
                        res.after.drives[i * self.set_drive_count + j].state = DriveState::Ok.to_string();
                        tmp_new_formats[i * self.set_drive_count + j] = Some(fm.clone());
                    }
                }
            }
            // Save new formats `format.json` on unformatted disks.
            for (fm, disk) in tmp_new_formats.iter_mut().zip(disks.iter()) {
                if fm.is_some() && disk.is_some() && save_format_file(disk, fm).await.is_err() {
                    let _ = disk.as_ref().unwrap().close().await;
                    *fm = None;
                }
            }

            for (index, fm) in tmp_new_formats.iter().enumerate() {
                if let Some(fm) = fm {
                    let (m, n) = match ref_format.find_disk_index_by_disk_id(fm.erasure.this) {
                        Ok((m, n)) => (m, n),
                        Err(_) => continue,
                    };
                    if let Some(set) = self.disk_set.get(m)
                        && let Some(Some(disk)) = set.disks.read().await.get(n)
                    {
                        let _ = disk.close().await;
                    }

                    if let Some(Some(disk)) = disks.get(index) {
                        self.disk_set[m].renew_disk(&disk.endpoint()).await;
                    }
                }
            }
        }
        Ok((res, None))
    }
    #[tracing::instrument(skip(self))]
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let mut result = HealResultItem {
            heal_item_type: HealItemType::Bucket.to_string(),
            bucket: bucket.to_string(),
            set_count: self.set_count,
            ..Default::default()
        };

        for set in &self.disk_set {
            let mut set_result = set.heal_bucket(bucket, opts).await?;
            result.disk_count += set_result.disk_count;
            result.before.drives.append(&mut set_result.before.drives);
            result.after.drives.append(&mut set_result.after.drives);
        }

        Ok(result)
    }
    #[tracing::instrument(skip(self))]
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.get_disks_by_key(object)
            .heal_object(bucket, object, version_id, opts)
            .await
    }
    #[tracing::instrument(skip(self))]
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
        for (set_idx, set) in self.format.erasure.sets.iter().enumerate() {
            for (disk_idx, disk_id) in set.iter().enumerate() {
                if disk_id.to_string() == id {
                    return Ok((Some(self.pool_idx), Some(set_idx), Some(disk_idx)));
                }
            }
        }

        Err(Error::DiskNotFound)
    }
    #[tracing::instrument(skip(self))]
    async fn check_abandoned_parts(&self, _bucket: &str, _object: &str, _opts: &HealOpts) -> Result<()> {
        // Multipart orphan reconciliation is intentionally retained above the pool/set layers
        // until there is a concrete caller and a stable lower-level contract to implement.
        Err(StorageError::NotImplemented)
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::namespace::NamespaceLocking for Sets {
    type Error = Error;
    type NamespaceLock = NamespaceLockWrapper;

    async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper> {
        self.disk_set[0].new_ns_lock(bucket, object).await
    }
}

async fn _close_storage_disks(disks: &[Option<DiskStore>]) {
    let mut futures = Vec::with_capacity(disks.len());
    for disk in disks.iter().flatten() {
        let disk = disk.clone();
        futures.push(tokio::spawn(async move {
            let _ = disk.close().await;
        }));
    }
    let _ = join_all(futures).await;
}

async fn init_storage_disks_with_errors(
    endpoints: &Endpoints,
    opts: &DiskOption,
) -> (Vec<Option<DiskStore>>, Vec<Option<DiskError>>) {
    // Bootstrap disks.
    // let disks = Arc::new(RwLock::new(vec![None; endpoints.as_ref().len()]));
    // let errs = Arc::new(RwLock::new(vec![None; endpoints.as_ref().len()]));
    let mut futures = Vec::with_capacity(endpoints.as_ref().len());
    for endpoint in endpoints.as_ref().iter() {
        futures.push(new_disk(endpoint, opts));

        // let ep = endpoint.clone();
        // let opt = opts.clone();
        // let disks_clone = disks.clone();
        // let errs_clone = errs.clone();
        // futures.push(tokio::spawn(async move {
        //     match new_disk(&ep, &opt).await {
        //         Ok(disk) => {
        //             disks_clone.write().await[index] = Some(disk);
        //             errs_clone.write().await[index] = None;
        //         }
        //         Err(err) => {
        //             disks_clone.write().await[index] = None;
        //             errs_clone.write().await[index] = Some(err);
        //         }
        //     }
        // }));
    }
    // let _ = join_all(futures).await;
    // let disks = disks.read().await.clone();
    // let errs = errs.read().await.clone();

    let mut disks = Vec::with_capacity(endpoints.as_ref().len());
    let mut errs = Vec::with_capacity(endpoints.as_ref().len());

    let results = join_all(futures).await;
    for result in results {
        match result {
            Ok(disk) => {
                disks.push(Some(disk));
                errs.push(None);
            }
            Err(err) => {
                disks.push(None);
                errs.push(Some(err));
            }
        }
    }

    (disks, errs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::SetupType;
    use crate::layout::endpoint::Endpoint;
    use crate::storage_api_contracts::heal::HealOperations as _;
    use crate::storage_api_contracts::list::ListOperations as _;
    use rustfs_lock::client::local::LocalClient;
    use serial_test::serial;

    struct SetupTypeGuard {
        previous: SetupType,
    }

    impl SetupTypeGuard {
        async fn switch_to(next: SetupType) -> Self {
            let previous = runtime_sources::current_setup_type().await;
            runtime_sources::set_setup_type(next).await;
            Self { previous }
        }
    }

    impl Drop for SetupTypeGuard {
        fn drop(&mut self) {
            let previous = self.previous.clone();
            let handle = tokio::runtime::Handle::current();
            tokio::task::block_in_place(|| {
                handle.block_on(async move {
                    runtime_sources::set_setup_type(previous).await;
                });
            });
        }
    }

    #[test]
    fn test_apply_delete_objects_results_preserves_original_order_for_out_of_order_batches() {
        let mut del_objects = vec![DeletedObject::default(); 3];
        let mut del_errs = vec![None, None, None];

        let early_batch = vec![DelObj {
            orig_idx: 1,
            obj: ObjectToDelete {
                object_name: "second".to_string(),
                ..Default::default()
            },
        }];
        let early_objects = vec![DeletedObject {
            object_name: "second".to_string(),
            found: true,
            ..Default::default()
        }];

        let late_batch = vec![
            DelObj {
                orig_idx: 2,
                obj: ObjectToDelete {
                    object_name: "third".to_string(),
                    ..Default::default()
                },
            },
            DelObj {
                orig_idx: 0,
                obj: ObjectToDelete {
                    object_name: "first".to_string(),
                    ..Default::default()
                },
            },
        ];
        let late_objects = vec![
            DeletedObject {
                object_name: "third".to_string(),
                found: true,
                ..Default::default()
            },
            DeletedObject {
                object_name: "first".to_string(),
                found: true,
                ..Default::default()
            },
        ];

        apply_delete_objects_results(&mut del_objects, &mut del_errs, &early_batch, &early_objects, vec![None]);
        apply_delete_objects_results(
            &mut del_objects,
            &mut del_errs,
            &late_batch,
            &late_objects,
            vec![Some(Error::other("third failed")), None],
        );

        assert_eq!(del_objects[0].object_name, "first");
        assert_eq!(del_objects[1].object_name, "second");
        assert_eq!(del_objects[2].object_name, "third");

        assert!(del_errs[0].is_none());
        assert!(del_errs[1].is_none());
        assert_eq!(
            del_errs[2].as_ref().map(ToString::to_string),
            Some(Error::other("third failed").to_string())
        );
    }

    #[tokio::test]
    async fn sets_get_pool_and_set_returns_matching_coordinates() {
        let format = FormatV3::new(2, 2);
        let target = format.erasure.sets[1][0].to_string();

        let endpoints = vec![
            Endpoint::try_from("http://127.0.0.1:9000/data0").expect("first endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9001/data1").expect("second endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9002/data2").expect("third endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9003/data3").expect("fourth endpoint should parse"),
        ];

        let sets = Sets {
            id: format.id,
            disk_set: Vec::new(),
            pool_idx: 3,
            endpoints: PoolEndpoints {
                legacy: false,
                set_count: 2,
                drives_per_set: 2,
                endpoints: Endpoints::from(endpoints),
                cmd_line: String::new(),
                platform: String::new(),
            },
            format,
            parity_count: 1,
            set_count: 2,
            set_drive_count: 2,
            default_parity_count: 1,
            distribution_algo: DistributionAlgoVersion::V1,
            exit_signal: None,
        };

        let result = sets
            .get_pool_and_set(&target)
            .await
            .expect("disk id should resolve within the pool");

        assert_eq!(result, (Some(3), Some(1), Some(0)));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn sets_list_objects_v2_lists_objects_within_the_pool() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let format = FormatV3::new(1, 2);
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();

        for disk_idx in 0..2 {
            let dir = tempfile::tempdir().expect("tempdir should be created");
            let mut endpoint =
                Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_idx);

            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("disk should be created");

            let mut disk_format = format.clone();
            disk_format.erasure.this = format.erasure.sets[0][disk_idx];
            save_format_file(&Some(disk.clone()), &Some(disk_format))
                .await
                .expect("format should be saved");

            std::mem::forget(dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        let set_disks = SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            2,
            1,
            0,
            0,
            endpoints.clone(),
            format.clone(),
            vec![Arc::new(LocalClient::new()), Arc::new(LocalClient::new())],
        )
        .await;

        let sets = Arc::new(Sets {
            id: format.id,
            disk_set: vec![set_disks],
            pool_idx: 0,
            endpoints: PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 2,
                endpoints: Endpoints::from(endpoints),
                cmd_line: String::new(),
                platform: String::new(),
            },
            format,
            parity_count: 1,
            set_count: 1,
            set_drive_count: 2,
            default_parity_count: 1,
            distribution_algo: DistributionAlgoVersion::V1,
            exit_signal: None,
        });

        let bucket = format!("bucket-{}", Uuid::new_v4().simple());
        let object = format!("object-{}", Uuid::new_v4().simple());

        sets.make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let mut reader = PutObjReader::from_vec(b"hello".to_vec());
        sets.put_object(&bucket, &object, &mut reader, &ObjectOptions::default())
            .await
            .expect("object should be written");

        let result = sets
            .clone()
            .list_objects_v2(&bucket, "", None, None, 1000, false, None, false)
            .await
            .expect("pool-level listing should succeed");

        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].name, object);
    }

    #[tokio::test]
    async fn sets_check_abandoned_parts_returns_typed_not_implemented_error() {
        let format = FormatV3::new(1, 1);
        let sets = Sets {
            id: format.id,
            disk_set: Vec::new(),
            pool_idx: 0,
            endpoints: PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 1,
                endpoints: Endpoints::from(Vec::new()),
                cmd_line: String::new(),
                platform: String::new(),
            },
            format,
            parity_count: 0,
            set_count: 1,
            set_drive_count: 1,
            default_parity_count: 0,
            distribution_algo: DistributionAlgoVersion::V1,
            exit_signal: None,
        };

        let err = sets
            .check_abandoned_parts("bucket", "object", &HealOpts::default())
            .await
            .expect_err("abandoned-parts ownership should stay above the pool/set storage layers");
        assert!(matches!(err, StorageError::NotImplemented));
    }
}
