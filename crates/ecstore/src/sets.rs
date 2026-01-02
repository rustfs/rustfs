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

use std::{collections::HashMap, sync::Arc};

use crate::disk::error_reduce::count_errs;
use crate::error::{Error, Result};
use crate::store_api::{ListPartsInfo, ObjectInfoOrErr, WalkOptions};
use crate::{
    disk::{
        DiskAPI, DiskInfo, DiskOption, DiskStore,
        error::DiskError,
        format::{DistributionAlgoVersion, FormatV3},
        new_disk,
    },
    endpoints::{Endpoints, PoolEndpoints},
    error::StorageError,
    global::{GLOBAL_LOCAL_DISK_SET_DRIVES, is_dist_erasure},
    set_disk::SetDisks,
    store_api::{
        BucketInfo, BucketOptions, CompletePart, DeleteBucketOptions, DeletedObject, GetObjectReader, HTTPRangeSpec,
        ListMultipartsInfo, ListObjectVersionsInfo, ListObjectsV2Info, MakeBucketOptions, MultipartInfo, MultipartUploadResult,
        ObjectIO, ObjectInfo, ObjectOptions, ObjectToDelete, PartInfo, PutObjReader, StorageAPI,
    },
    store_init::{check_format_erasure_values, get_format_erasure_in_quorum, load_format_erasure_all, save_format_file},
};
use futures::future::join_all;
use http::HeaderMap;
use rustfs_common::heal_channel::HealOpts;
use rustfs_common::{
    GLOBAL_LOCAL_NODE_NAME,
    heal_channel::{DriveState, HealItemType},
};
use rustfs_filemeta::FileInfo;

use rustfs_madmin::heal_commands::{HealDriveInfo, HealResultItem};
use rustfs_utils::{crc_hash, path::path_join_buf, sip_hash};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time::Duration;
use tracing::warn;
use tracing::{error, info};

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

        let mut unique: Vec<Vec<String>> = (0..set_count).map(|_| vec![]).collect();

        for (idx, endpoint) in endpoints.endpoints.as_ref().iter().enumerate() {
            let set_idx = idx / set_drive_count;
            if endpoint.is_local && !unique[set_idx].contains(&"local".to_string()) {
                unique[set_idx].push("local".to_string());
            }

            if !endpoint.is_local {
                let host_port = format!("{}:{}", endpoint.url.host_str().unwrap(), endpoint.url.port().unwrap());
                if !unique[set_idx].contains(&host_port) {
                    unique[set_idx].push(host_port);
                }
            }
        }

        let mut disk_set = Vec::with_capacity(set_count);

        // Create fast lock manager for high performance
        let fast_lock_manager = Arc::new(rustfs_lock::FastObjectLockManager::new());

        for i in 0..set_count {
            let mut set_drive = Vec::with_capacity(set_drive_count);
            let mut set_endpoints = Vec::with_capacity(set_drive_count);
            for j in 0..set_drive_count {
                let idx = i * set_drive_count + j;
                let mut disk = disks[idx].clone();

                let endpoint = endpoints.endpoints.as_ref()[idx].clone();
                set_endpoints.push(endpoint);

                if disk.is_none() {
                    warn!("sets new set_drive {}-{} is none", i, j);
                    set_drive.push(None);
                    continue;
                }

                if disk.as_ref().unwrap().is_local() && is_dist_erasure().await {
                    let local_disk = {
                        let local_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.read().await;
                        local_set_drives[pool_idx][i][j].clone()
                    };

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

            // Note: write_quorum was used for the old lock system, no longer needed with FastLock
            let _write_quorum = set_drive_count - parity_count;

            let set_disks = SetDisks::new(
                fast_lock_manager.clone(),
                GLOBAL_LOCAL_NODE_NAME.read().await.to_string(),
                Arc::new(RwLock::new(set_drive)),
                set_drive_count,
                parity_count,
                i,
                pool_idx,
                set_endpoints,
                fm.clone(),
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

    async fn delete_prefix(&self, bucket: &str, object: &str) -> Result<()> {
        let mut futures = Vec::new();
        let opt = ObjectOptions {
            delete_prefix: true,
            ..Default::default()
        };

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

#[async_trait::async_trait]
impl ObjectIO for Sets {
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
impl StorageAPI for Sets {
    #[tracing::instrument(skip(self))]
    async fn backend_info(&self) -> rustfs_madmin::BackendInfo {
        unimplemented!()
    }
    #[tracing::instrument(skip(self))]
    async fn storage_info(&self) -> rustfs_madmin::StorageInfo {
        let mut futures = Vec::with_capacity(self.disk_set.len());

        for set in self.disk_set.iter() {
            futures.push(set.storage_info())
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
    #[tracing::instrument(skip(self))]
    async fn local_storage_info(&self) -> rustfs_madmin::StorageInfo {
        let mut futures = Vec::with_capacity(self.disk_set.len());

        for set in self.disk_set.iter() {
            futures.push(set.local_storage_info())
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
    #[tracing::instrument(skip(self))]
    async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> Result<()> {
        unimplemented!()
    }
    #[tracing::instrument(skip(self))]
    async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn delete_bucket(&self, _bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn list_objects_v2(
        self: Arc<Self>,
        _bucket: &str,
        _prefix: &str,
        _continuation_token: Option<String>,
        _delimiter: Option<String>,
        _max_keys: i32,
        _fetch_owner: bool,
        _start_after: Option<String>,
        _incl_deleted: bool,
    ) -> Result<ListObjectsV2Info> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn list_object_versions(
        self: Arc<Self>,
        _bucket: &str,
        _prefix: &str,
        _marker: Option<String>,
        _version_marker: Option<String>,
        _delimiter: Option<String>,
        _max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        unimplemented!()
    }

    async fn walk(
        self: Arc<Self>,
        _rx: CancellationToken,
        _bucket: &str,
        _prefix: &str,
        _result: tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
        _opts: WalkOptions,
    ) -> Result<()> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.get_disks_by_key(object).get_object_info(bucket, object, opts).await
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
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, _force_del_marker: bool) -> Result<()> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        if opts.delete_prefix && !opts.delete_prefix_object {
            self.delete_prefix(bucket, object).await?;
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

        // TODO: concurrency
        for (k, v) in set_obj_map {
            let disks = self.get_disks(k);
            let objs: Vec<ObjectToDelete> = v.iter().map(|v| v.obj.clone()).collect();
            let (dobjects, errs) = disks.delete_objects(bucket, objs, opts.clone()).await;

            for (i, err) in errs.into_iter().enumerate() {
                let obj = v.get(i).unwrap();

                del_errs[obj.orig_idx] = err;

                del_objects[obj.orig_idx] = dobjects.get(i).unwrap().clone();
            }
        }

        (del_objects, del_errs)
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
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.get_disks_by_key(object).transition_object(bucket, object, opts).await
    }

    #[tracing::instrument(skip(self))]
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        self.get_disks_by_key(object).add_partial(bucket, object, version_id).await
    }

    #[tracing::instrument(skip(self))]
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.get_disks_by_key(object)
            .restore_transitioned_object(bucket, object, opts)
            .await
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
        unimplemented!()
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

    #[tracing::instrument(skip(self))]
    async fn get_disks(&self, _pool_idx: usize, _set_idx: usize) -> Result<Vec<Option<DiskStore>>> {
        unimplemented!()
    }

    #[tracing::instrument(skip(self))]
    fn set_drive_counts(&self) -> Vec<usize> {
        unimplemented!()
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
    async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> Result<HealResultItem> {
        unimplemented!()
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
    async fn get_pool_and_set(&self, _id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
        unimplemented!()
    }
    #[tracing::instrument(skip(self))]
    async fn check_abandoned_parts(&self, _bucket: &str, _object: &str, _opts: &HealOpts) -> Result<()> {
        unimplemented!()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let gor = self.get_object_reader(bucket, object, None, HeaderMap::new(), opts).await?;
        let mut reader = gor.stream;

        // Stream data to sink instead of reading all into memory to prevent OOM
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;

        Ok(())
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

fn formats_to_drives_info(endpoints: &Endpoints, formats: &[Option<FormatV3>], errs: &[Option<DiskError>]) -> Vec<HealDriveInfo> {
    let mut before_drives = Vec::with_capacity(endpoints.as_ref().len());
    for (index, format) in formats.iter().enumerate() {
        let drive = endpoints.get_string(index);
        let state = if format.is_some() {
            DriveState::Ok.to_string()
        } else if let Some(Some(err)) = errs.get(index) {
            if *err == DiskError::UnformattedDisk {
                DriveState::Missing.to_string()
            } else if *err == DiskError::DiskNotFound {
                DriveState::Offline.to_string()
            } else {
                DriveState::Corrupt.to_string()
            }
        } else {
            DriveState::Corrupt.to_string()
        };

        let uuid = if let Some(format) = format {
            format.erasure.this.to_string()
        } else {
            "".to_string()
        };
        before_drives.push(HealDriveInfo {
            uuid,
            endpoint: drive,
            state: state.to_string(),
        });
    }
    before_drives
}

fn new_heal_format_sets(
    ref_format: &FormatV3,
    set_count: usize,
    set_drive_count: usize,
    formats: &[Option<FormatV3>],
    errs: &[Option<DiskError>],
) -> (Vec<Vec<Option<FormatV3>>>, Vec<Vec<DiskInfo>>) {
    let mut new_formats = vec![vec![None; set_drive_count]; set_count];
    let mut current_disks_info = vec![vec![DiskInfo::default(); set_drive_count]; set_count];
    for (i, set) in ref_format.erasure.sets.iter().enumerate() {
        for j in 0..set.len() {
            if let Some(Some(err)) = errs.get(i * set_drive_count + j)
                && *err == DiskError::UnformattedDisk
            {
                let mut fm = FormatV3::new(set_count, set_drive_count);
                fm.id = ref_format.id;
                fm.format = ref_format.format.clone();
                fm.version = ref_format.version.clone();
                fm.erasure.this = ref_format.erasure.sets[i][j];
                fm.erasure.sets = ref_format.erasure.sets.clone();
                fm.erasure.version = ref_format.erasure.version.clone();
                fm.erasure.distribution_algo = ref_format.erasure.distribution_algo.clone();
                new_formats[i][j] = Some(fm);
            }
            if let (Some(format), None) = (&formats[i * set_drive_count + j], &errs[i * set_drive_count + j])
                && let Some(info) = &format.disk_info
                && !info.endpoint.is_empty()
            {
                current_disks_info[i][j] = info.clone();
            }
        }
    }

    (new_formats, current_disks_info)
}
