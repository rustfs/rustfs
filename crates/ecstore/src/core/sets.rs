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
use crate::error::{Error, Result, is_all_volume_not_found, is_err_object_not_found, is_err_strict_volume_not_found};
use crate::layout::set_heal::{formats_to_drives_info, new_heal_format_sets};
use crate::multipart_listing::paginate_multipart_listing;
use crate::storage_api_contracts::{
    bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
    list::{StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr, StorageWalkOptions},
    multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo},
    object::{DeleteAccounting, DeletedObject, ObjectIO as _, ObjectOperations as _, ObjectToDelete},
    range::HTTPRangeSpec,
};
use crate::{
    disk::{
        DiskAPI, DiskOption, DiskStore,
        error::DiskError,
        format::{DistributionAlgoVersion, FormatV3},
        new_disk,
    },
    error::StorageError,
    layout::endpoints::{Endpoints, PoolEndpoints},
    object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader},
    runtime::instance::{InstanceContext, bootstrap_ctx},
    runtime::sources as runtime_sources,
    set_disk::{PreparedGetObjectMetadata, SetDisks},
    store::init_format::{
        check_format_erasure_values, load_format_erasure_all, save_format_file, select_format_erasure_in_quorum,
    },
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
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_utils::{crc_hash, path::path_join_buf, sip_hash};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
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

const LIST_MULTIPART_SETS_CONCURRENCY: usize = 4;

fn is_idempotent_delete_prefix_error(err: &Error) -> bool {
    is_err_object_not_found(err) || is_err_strict_volume_not_found(err)
}

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
    /// Per-instance runtime context (Phase 5, backlog#939).
    ///
    /// Carried down the object graph (ECStore → Sets → SetDisks) so that
    /// instance-scoped state resolves through the owning instance rather than a
    /// process global. Consumed starting Slice 3 (lock-namespace isolation).
    ctx: Arc<InstanceContext>,
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
        Self::new_with_instance_ctx(disks, endpoints, fm, pool_idx, parity_count, bootstrap_ctx()).await
    }

    /// Build the pool's sets bound to an explicit instance context (Phase 5
    /// follow-up, backlog#1052). The legacy [`Sets::new`] entry adopts the
    /// process bootstrap context; a store constructed around its own context
    /// passes it here so the whole object graph shares one cell.
    pub async fn new_with_instance_ctx(
        disks: Vec<Option<DiskStore>>,
        endpoints: &PoolEndpoints,
        fm: &FormatV3,
        pool_idx: usize,
        parity_count: usize,
        instance_ctx: Arc<InstanceContext>,
    ) -> Result<Arc<Self>> {
        let set_count = fm.erasure.sets.len();
        let set_drive_count = fm.erasure.sets[0].len();

        let mut disk_set = Vec::with_capacity(set_count);

        let lock_registry = runtime_sources::lock_registry();

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

                if disk.as_ref().unwrap().is_local() && instance_ctx.is_dist_erasure().await {
                    let local_disk = runtime_sources::local_disk_set_drive(&instance_ctx, pool_idx, i, j).await;

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

            let lockers = lock_registry
                .as_ref()
                .map(|registry| registry.clients_for_endpoints(&set_endpoints))
                .unwrap_or_default();
            let set_disks = SetDisks::new_with_instance_ctx(
                runtime_sources::local_node_name().await,
                Arc::new(RwLock::new(set_drive)),
                set_drive_count,
                parity_count,
                i,
                pool_idx,
                set_endpoints,
                fm.clone(),
                lockers,
                instance_ctx.clone(),
            )
            .await;

            disk_set.push(set_disks);
        }

        let (tx, rx) = tokio::sync::broadcast::channel(1);

        let sets = Arc::new(Self {
            id: fm.id,
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
            ctx: instance_ctx,
        });

        let asets = sets.clone();

        let rx1 = rx.resubscribe();
        tokio::spawn(async move { asets.monitor_and_connect_endpoints(rx1).await });

        Ok(sets)
    }

    pub fn set_drive_count(&self) -> usize {
        self.set_drive_count
    }

    /// This pool's per-instance runtime context (Phase 5, backlog#939).
    #[allow(dead_code)] // Consumed starting Slice 3 (lock-namespace isolation).
    pub(crate) fn instance_ctx(&self) -> &Arc<InstanceContext> {
        &self.ctx
    }

    pub async fn monitor_and_connect_endpoints(&self, mut rx: Receiver<()>) {
        tokio::time::sleep(Duration::from_secs(5)).await;

        info!("start monitor_and_connect_endpoints");

        self.connect_disks().await;

        // TODO(backlog): make monitor_and_connect interval configurable instead of hardcoded 15s
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

    pub(crate) fn get_disks_for_heal_object(&self, key: &str, opts: &HealOpts) -> Result<Arc<SetDisks>> {
        match opts.set {
            Some(set_idx) => self.disk_set.get(set_idx).cloned().ok_or_else(|| {
                StorageError::InvalidArgument(
                    "heal".to_string(),
                    "set".to_string(),
                    format!(
                        "invalid heal set index {set_idx} for pool {} with {} sets",
                        self.pool_idx,
                        self.disk_set.len()
                    ),
                )
            }),
            None => Ok(self.get_disks_by_key(key)),
        }
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

    async fn delete_prefix(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let mut futures = Vec::new();
        let mut opt = opts.clone();
        opt.delete_prefix = true;

        for set in self.disk_set.iter() {
            futures.push(set.delete_object(bucket, object, opt.clone()));
        }

        let errs = join_all(futures)
            .await
            .into_iter()
            .map(|result| result.err())
            .collect::<Vec<_>>();
        if is_all_volume_not_found(&errs) {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        for err in errs.into_iter().flatten() {
            if !is_idempotent_delete_prefix_error(&err) {
                return Err(err);
            }
        }

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

fn apply_delete_accounting_results(
    accounting: &mut [Option<DeleteAccounting>],
    set_objects: &[DelObj],
    set_accounting: &[Option<DeleteAccounting>],
) {
    for (obj, value) in set_objects.iter().zip(set_accounting.iter()) {
        accounting[obj.orig_idx] = value.clone();
    }
}

impl Sets {
    pub(crate) async fn delete_objects_with_accounting(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>, Vec<Option<DeleteAccounting>>) {
        let mut del_objects = vec![DeletedObject::default(); objects.len()];
        let mut del_errs = vec![None; objects.len()];
        let mut accounting = vec![None; objects.len()];
        let mut set_obj_map = HashMap::new();

        for (i, obj) in objects.iter().enumerate() {
            let idx = self.get_hashed_set_index(obj.object_name.as_str());
            set_obj_map.entry(idx).or_insert_with(Vec::new).push(DelObj {
                orig_idx: i,
                obj: obj.clone(),
            });
        }

        let max_concurrent = set_obj_map.len().min(num_cpus::get()).max(1);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        let mut futures = FuturesUnordered::new();
        let bucket = bucket.to_owned();

        for (set_index, set_objects) in set_obj_map {
            let disks = self.get_disks(set_index);
            let objects = set_objects.iter().map(|entry| entry.obj.clone()).collect::<Vec<_>>();
            let bucket = bucket.clone();
            let opts = opts.clone();
            let semaphore = semaphore.clone();
            futures.push(async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .expect("delete_objects semaphore should remain open");
                let (deleted, errors, accounting) = disks.delete_objects_with_accounting(&bucket, objects, opts).await;
                (set_objects, deleted, errors, accounting)
            });
        }

        while let Some((set_objects, deleted, errors, set_accounting)) = futures.next().await {
            apply_delete_objects_results(&mut del_objects, &mut del_errs, &set_objects, &deleted, errors);
            apply_delete_accounting_results(&mut accounting, &set_objects, &set_accounting);
        }

        (del_objects, del_errs, accounting)
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

impl Sets {
    pub(crate) async fn prepare_get_object_reader_metadata(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<PreparedGetObjectMetadata> {
        self.get_disks_by_key(object)
            .prepare_get_object_metadata(bucket, object, opts)
            .await
    }

    pub(crate) async fn get_object_reader_with_prepared_metadata(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        headers: HeaderMap,
        opts: &ObjectOptions,
        metadata: PreparedGetObjectMetadata,
    ) -> Result<GetObjectReader> {
        self.get_disks_by_key(object)
            .get_object_reader_with_prepared_metadata(bucket, object, range, headers, opts, metadata)
            .await
    }

    /// `put_object` plus the rename_data old-size backfill
    /// (rustfs/backlog#1009); see `SetDisks::put_object_with_old_current_size`.
    pub async fn put_object_with_old_current_size(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<(ObjectInfo, Option<crate::disk::OldCurrentSize>)> {
        self.get_disks_by_key(object)
            .put_object_with_old_current_size(bucket, object, data, opts)
            .await
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

    #[tracing::instrument(skip(self, objects, opts))]
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        let (deleted, errors, _) = self.delete_objects_with_accounting(bucket, objects, opts).await;
        (deleted, errors)
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

    #[tracing::instrument(level = "trace", skip(self))]
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
        let per_set_limit = max_uploads.saturating_add(1);
        let results = futures::stream::iter(self.disk_set.iter().cloned())
            .map(|set| {
                let key_marker = key_marker.clone();
                let upload_id_marker = upload_id_marker.clone();
                let delimiter = delimiter.clone();
                async move {
                    // ECStore owns the bucket lifecycle fence and calls the
                    // incarnation-aware pool helper. This lower-level trait
                    // surface has no ECStore guard to propagate.
                    set.list_multipart_uploads_for_incarnation(
                        bucket,
                        prefix,
                        key_marker,
                        upload_id_marker,
                        delimiter,
                        per_set_limit,
                        None,
                    )
                    .await
                }
            })
            .buffer_unordered(LIST_MULTIPART_SETS_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut uploads = Vec::new();
        let mut common_prefixes = HashSet::new();
        let mut source_truncated = false;
        for result in results {
            let page = result?;
            uploads.extend(page.uploads);
            common_prefixes.extend(page.common_prefixes);
            source_truncated |= page.is_truncated;
        }

        let page = paginate_multipart_listing(
            uploads,
            common_prefixes.into_iter().collect(),
            key_marker.as_deref(),
            key_marker.as_ref().and(upload_id_marker.as_deref()),
            max_uploads,
            source_truncated,
        );

        Ok(ListMultipartsInfo {
            key_marker,
            upload_id_marker,
            next_key_marker: page.next_key_marker,
            next_upload_id_marker: page.next_upload_id_marker,
            max_uploads,
            is_truncated: page.is_truncated,
            uploads: page.uploads,
            common_prefixes: page.common_prefixes,
            prefix: prefix.to_owned(),
            delimiter,
        })
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

impl Sets {
    pub(crate) async fn heal_format_with_fence<F>(&self, dry_run: bool, fence_lost: F) -> Result<(HealResultItem, Option<Error>)>
    where
        F: Fn() -> bool + Send + Sync,
    {
        let (disks, init_errs) = init_storage_disks_with_errors(
            &self.endpoints.endpoints,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await;
        let (formats, mut errs) = load_format_erasure_all(&disks, true).await;
        for (err, init_err) in errs.iter_mut().zip(init_errs) {
            if init_err.is_some() {
                *err = init_err;
            }
        }
        if errs.iter().any(|err| {
            matches!(
                err,
                Some(DiskError::InconsistentDisk | DiskError::CorruptedFormat | DiskError::CorruptedBackend)
            )
        }) {
            return Ok((HealResultItem::default(), Some(StorageError::CorruptedFormat)));
        }
        if let Err(err) = check_format_erasure_values(&formats, self.set_drive_count) {
            info!("failed to check formats erasure values: {}", err);
            return Ok((HealResultItem::default(), Some(err)));
        }
        let (ref_format, quorum_members) = match select_format_erasure_in_quorum(&formats, 0) {
            Ok((format, members)) if format.shared_identity() == self.format.shared_identity() => (format, members),
            Ok(_) => return Ok((HealResultItem::default(), Some(StorageError::CorruptedFormat))),
            Err(err) => return Ok((HealResultItem::default(), Some(err))),
        };
        if formats
            .iter()
            .zip(quorum_members)
            .any(|(format, member)| format.is_some() && !member)
        {
            return Ok((HealResultItem::default(), Some(StorageError::CorruptedFormat)));
        }
        let mut res = HealResultItem {
            heal_item_type: HealItemType::Metadata.to_string(),
            detail: "disk-format".to_string(),
            disk_count: self.set_count * self.set_drive_count,
            set_count: self.set_count,
            ..Default::default()
        };
        // One drive record per endpoint (`formats_to_drives_info` returns exactly
        // N entries). Assign directly instead of pre-filling N defaults and then
        // pushing N real entries: the old form produced a 2N-long list whose empty
        // placeholder half received the healed uuid/Ok updates below (indexed by
        // `i * set_drive_count + j`, i.e. 0..N), leaving the real drive entries
        // never marked healed. Mirrors the set-level `heal_format`.
        let before_derives = formats_to_drives_info(&self.endpoints.endpoints, &formats, &errs);
        res.before.drives = before_derives.clone();
        res.after.drives = before_derives;
        if count_errs(&errs, &DiskError::UnformattedDisk) == 0 {
            info!("disk formats success, NoHealRequired, errs: {:?}", errs);
            return Ok((res, Some(StorageError::NoHealRequired)));
        }

        let (new_format_sets, _) = new_heal_format_sets(&ref_format, self.set_count, self.set_drive_count, &formats, &errs);
        if !dry_run {
            let mut tmp_new_formats = vec![None; self.set_count * self.set_drive_count];
            for (i, set) in new_format_sets.iter().enumerate() {
                for (j, fm) in set.iter().enumerate() {
                    if let Some(fm) = fm {
                        tmp_new_formats[i * self.set_drive_count + j] = Some(fm.clone());
                    }
                }
            }
            // Save new formats `format.json` on unformatted disks.
            for (index, (fm, disk)) in tmp_new_formats.iter_mut().zip(disks.iter()).enumerate() {
                if fm.is_some() && disk.is_some() {
                    if fence_lost() {
                        return Ok((res, Some(StorageError::SlowDown)));
                    }
                    if let Err(err) = save_format_file(disk, fm).await {
                        if let Some(disk) = disk.as_ref() {
                            let _ = disk.close().await;
                        }
                        return Ok((res, Some(err.into())));
                    }
                    if let Some(saved_format) = fm.as_ref() {
                        res.after.drives[index].uuid = saved_format.erasure.this.to_string();
                        res.after.drives[index].state = DriveState::Ok.to_string();
                    }
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
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::heal::HealOperations for Sets {
    type Error = Error;
    type HealResultItem = HealResultItem;
    type HealOptions = HealOpts;

    #[tracing::instrument(skip(self))]
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        self.heal_format_with_fence(dry_run, || false).await
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
    #[tracing::instrument(level = "trace", skip(self, opts), fields(bucket = %bucket, object = %object, version_id = %version_id))]
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.get_disks_for_heal_object(object, opts)?
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
    #[tracing::instrument(level = "debug", skip(self, opts), fields(bucket = %bucket, object = %object, dry_run = opts.dry_run))]
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()> {
        self.get_disks_for_heal_object(object, opts)?
            .check_abandoned_parts(bucket, object, opts)
            .await
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
pub(crate) async fn make_local_two_set_sets() -> (Vec<tempfile::TempDir>, Arc<Sets>) {
    make_local_two_set_sets_with_ctx(bootstrap_ctx()).await
}

#[cfg(test)]
pub(crate) async fn make_local_two_set_sets_with_ctx(ctx: Arc<InstanceContext>) -> (Vec<tempfile::TempDir>, Arc<Sets>) {
    use crate::layout::endpoint::Endpoint;
    use rustfs_lock::client::local::LocalClient;

    let format = FormatV3::new(2, 2);
    let mut temp_dirs = Vec::new();
    let mut all_endpoints = Vec::new();
    let mut disk_sets = Vec::new();

    for set_index in 0..2 {
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();
        for disk_index in 0..2 {
            let temp_dir = tempfile::tempdir().expect("tempdir should be created");
            let mut endpoint = Endpoint::try_from(temp_dir.path().to_str().expect("tempdir path should be utf8"))
                .expect("endpoint should parse");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(set_index);
            endpoint.set_disk_index(disk_index);
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
            disk_format.erasure.this = format.erasure.sets[set_index][disk_index];
            save_format_file(&Some(disk.clone()), &Some(disk_format))
                .await
                .expect("format should be saved");
            temp_dirs.push(temp_dir);
            all_endpoints.push(endpoint.clone());
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }
        let lockers = (0..2)
            .map(|_| {
                Arc::new(LocalClient::with_manager(Arc::new(rustfs_lock::GlobalLockManager::Enabled(Arc::new(
                    rustfs_lock::FastObjectLockManager::new(),
                ))))) as Arc<dyn rustfs_lock::LockClient>
            })
            .collect();
        disk_sets.push(
            SetDisks::new_with_instance_ctx(
                "test-owner".to_string(),
                Arc::new(RwLock::new(disks)),
                2,
                1,
                set_index,
                0,
                endpoints,
                format.clone(),
                lockers,
                Arc::clone(&ctx),
            )
            .await,
        );
    }

    let sets = Arc::new(Sets {
        id: format.id,
        disk_set: disk_sets,
        pool_idx: 0,
        endpoints: PoolEndpoints {
            legacy: false,
            set_count: 2,
            drives_per_set: 2,
            endpoints: Endpoints::from(all_endpoints),
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
        ctx,
    });
    (temp_dirs, sets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::endpoint::Endpoint;
    use crate::layout::endpoints::SetupType;
    use crate::storage_api_contracts::heal::HealOperations as _;
    use crate::storage_api_contracts::list::ListOperations as _;
    use crate::storage_api_contracts::multipart::MultipartOperations as _;
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

    #[test]
    fn delete_prefix_error_classification_only_ignores_absence() {
        assert!(is_idempotent_delete_prefix_error(&StorageError::FileNotFound));
        assert!(is_idempotent_delete_prefix_error(&StorageError::ObjectNotFound(
            "bucket".to_string(),
            "prefix".to_string()
        )));
        assert!(is_idempotent_delete_prefix_error(&StorageError::VolumeNotFound));
        assert!(is_idempotent_delete_prefix_error(&StorageError::BucketNotFound("bucket".to_string())));
        assert!(!is_idempotent_delete_prefix_error(&StorageError::DiskNotFound));
        assert!(!is_idempotent_delete_prefix_error(&StorageError::ErasureWriteQuorum));
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
            ctx: bootstrap_ctx(),
        };

        let result = sets
            .get_pool_and_set(&target)
            .await
            .expect("disk id should resolve within the pool");

        assert_eq!(result, (Some(3), Some(1), Some(0)));
    }

    #[tokio::test]
    async fn heal_object_uses_explicit_set_scope() {
        let (_temp_dirs, sets) = make_local_two_set_sets().await;
        let selected = sets
            .get_disks_for_heal_object(
                "object",
                &HealOpts {
                    set: Some(1),
                    ..Default::default()
                },
            )
            .expect("requested set should be selected");

        assert!(Arc::ptr_eq(&selected, &sets.disk_set[1]));
    }

    #[tokio::test]
    async fn heal_object_without_set_scope_keeps_hash_routing() {
        let (_temp_dirs, sets) = make_local_two_set_sets().await;
        let object = "object";
        let selected = sets
            .get_disks_for_heal_object(object, &HealOpts::default())
            .expect("hash-routed set should be selected");

        assert!(Arc::ptr_eq(&selected, &sets.get_disks_by_key(object)));
    }

    #[tokio::test]
    async fn heal_object_rejects_invalid_set_scope() {
        let (_temp_dirs, sets) = make_local_two_set_sets().await;
        let err = sets
            .get_disks_for_heal_object(
                "object",
                &HealOpts {
                    set: Some(2),
                    ..Default::default()
                },
            )
            .expect_err("out-of-range set scope must fail closed");

        assert!(
            matches!(err, StorageError::InvalidArgument(_, ref field, ref reason)
                if field == "set" && reason.contains("invalid heal set index 2 for pool 0 with 2 sets")),
            "unexpected invalid set error: {err:?}"
        );
    }

    #[tokio::test]
    async fn delete_prefix_surfaces_a_hard_error_from_any_set() {
        let (_temp_dirs, sets) = make_local_two_set_sets().await;
        let bucket = format!("delete-prefix-{}", Uuid::new_v4().simple());
        sets.make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created across both sets");

        let healthy_disks = sets.disk_set[0].disks.read().await.clone();
        for disk in healthy_disks.iter().flatten() {
            disk.write_all(&bucket, "blocked/prefix/object", bytes::Bytes::from_static(b"data"))
                .await
                .expect("healthy set should contain the prefix");
        }

        let failing_disks = sets.disk_set[1].disks.read().await.clone();
        for disk in failing_disks.iter().flatten() {
            disk.write_all(&bucket, "blocked", bytes::Bytes::from_static(b"not-a-directory"))
                .await
                .expect("failing set should contain a parent file");
        }

        let err = sets
            .delete_object(
                &bucket,
                "blocked/prefix",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("a hard failure from one set must not be reported as success");

        match err {
            StorageError::PrefixAccessDenied(error_bucket, error_prefix) => {
                assert_eq!(error_bucket, bucket);
                assert_eq!(error_prefix, "blocked/prefix");
            }
            other => panic!("unexpected recursive delete error: {other:?}"),
        }
        for disk in healthy_disks.iter().flatten() {
            assert!(
                matches!(disk.read_all(&bucket, "blocked/prefix/object").await, Err(DiskError::FileNotFound)),
                "the healthy set should still complete its prefix deletion"
            );
        }
    }

    #[tokio::test]
    async fn delete_prefix_keeps_a_missing_bucket_idempotent_across_sets() {
        let (_temp_dirs, sets) = make_local_two_set_sets().await;
        let bucket = format!("delete-prefix-{}", Uuid::new_v4().simple());
        sets.make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created across both sets");

        let healthy_disks = sets.disk_set[0].disks.read().await.clone();
        for disk in healthy_disks.iter().flatten() {
            disk.write_all(&bucket, "existing/prefix/object", bytes::Bytes::from_static(b"data"))
                .await
                .expect("healthy set should contain the prefix");
        }
        let missing_bucket_disks = sets.disk_set[1].disks.read().await.clone();
        for disk in missing_bucket_disks.iter().flatten() {
            disk.delete_volume(&bucket, true)
                .await
                .expect("the bucket should be removed from one set");
        }

        sets.delete_object(
            &bucket,
            "existing/prefix",
            ObjectOptions {
                delete_prefix: true,
                ..Default::default()
            },
        )
        .await
        .expect("a missing bucket on one set should remain an idempotent success");
        for disk in healthy_disks.iter().flatten() {
            assert!(
                matches!(disk.read_all(&bucket, "existing/prefix/object").await, Err(DiskError::FileNotFound)),
                "the healthy set should still complete its prefix deletion"
            );
        }
    }

    #[tokio::test]
    async fn delete_prefix_preserves_a_completely_missing_bucket_error() {
        let (_temp_dirs, sets) = make_local_two_set_sets().await;
        let bucket = format!("delete-prefix-missing-{}", Uuid::new_v4().simple());

        let err = sets
            .delete_object(
                &bucket,
                "missing/prefix",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("a completely missing bucket must not be reported as a successful object deletion");

        assert_eq!(err, StorageError::BucketNotFound(bucket));
    }

    #[tokio::test]
    async fn delete_prefix_fails_when_one_set_is_entirely_offline() {
        let (_temp_dirs, sets) = make_local_two_set_sets().await;
        let bucket = format!("delete-prefix-{}", Uuid::new_v4().simple());
        sets.make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created across both sets");

        let online_disks = sets.disk_set[0].disks.read().await.clone();
        let offline_disks = sets.disk_set[1].disks.read().await.clone();
        for disk in online_disks.iter().chain(offline_disks.iter()).flatten() {
            disk.write_all(&bucket, "offline/prefix/object", bytes::Bytes::from_static(b"data"))
                .await
                .expect("each set should contain the prefix before the outage");
        }
        *sets.disk_set[1].disks.write().await = vec![None, None];

        let err = sets
            .delete_object(
                &bucket,
                "offline/prefix",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("an entirely offline set must make the recursive delete fail");

        assert!(
            matches!(err, StorageError::InsufficientWriteQuorum(ref error_bucket, ref error_prefix)
                if error_bucket == &bucket && error_prefix == "offline/prefix"),
            "unexpected offline-set error: {err:?}"
        );
        for disk in online_disks.iter().flatten() {
            assert!(matches!(
                disk.read_all(&bucket, "offline/prefix/object").await,
                Err(DiskError::FileNotFound)
            ));
        }
        for disk in offline_disks.iter().flatten() {
            disk.read_all(&bucket, "offline/prefix/object")
                .await
                .expect("the offline set's untouched prefix must still be present");
        }
    }

    #[tokio::test]
    async fn set_format_heal_accepts_quorum_from_a_nonzero_set() {
        let (_temp_dirs, sets) = make_local_two_set_sets().await;

        let (result, err) = sets.disk_set[1]
            .heal_format(false)
            .await
            .expect("the second erasure set should load its own format quorum");

        assert!(matches!(err, Some(StorageError::NoHealRequired)), "unexpected heal result: {err:?}");
        assert_eq!(result.disk_count, 2);
        assert_eq!(result.set_count, 1);
    }

    #[tokio::test]
    async fn format_heal_rejects_foreign_majorities_at_set_and_pool_scopes() {
        let (_temp_dirs, _canonical_format, sets) = setup_heal_format_sets(2, true).await;
        let set_disks = set_level_heal_view(&sets).await;

        let (_, set_err) = set_disks
            .heal_format(false)
            .await
            .expect("set format heal should report a typed mismatch");
        assert!(
            matches!(set_err, Some(StorageError::CorruptedFormat)),
            "foreign set majority must not replace the cached format: {set_err:?}"
        );

        let (_, pool_err) = sets
            .heal_format(false)
            .await
            .expect("pool format heal should report a typed mismatch");
        assert!(
            matches!(pool_err, Some(StorageError::CorruptedFormat)),
            "foreign pool majority must not replace the cached format: {pool_err:?}"
        );
    }

    #[tokio::test]
    async fn pool_format_heal_rejects_a_wrong_slot_minority() {
        let (_temp_dirs, canonical_format, sets) = setup_heal_format_sets(3, false).await;
        let mut poisoned_format = canonical_format.clone();
        poisoned_format.erasure.this = canonical_format.erasure.sets[0][0];
        replace_heal_test_format(&sets, 2, &poisoned_format).await;
        let probe_err = new_disk(
            &sets.endpoints.endpoints.as_ref()[2],
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect_err("a wrong-slot local format must fail disk initialization");
        assert_eq!(probe_err, DiskError::InconsistentDisk);

        let (_, pool_err) = sets
            .heal_format(false)
            .await
            .expect("pool format heal should report a typed slot mismatch");
        assert!(
            matches!(pool_err, Some(StorageError::CorruptedFormat)),
            "a wrong-slot minority must not be reported as no-heal-required: {pool_err:?}"
        );
        assert_eq!(
            read_heal_test_format(&sets, 2).await,
            poisoned_format,
            "format heal must not overwrite a wrong-slot disk"
        );
    }

    #[tokio::test]
    async fn format_heal_rejects_a_foreign_minority_at_set_and_pool_scopes() {
        let (_temp_dirs, canonical_format, sets) = setup_heal_format_sets(3, false).await;
        let mut poisoned_format = canonical_format.clone();
        poisoned_format.id = Uuid::new_v4();
        poisoned_format.erasure.this = poisoned_format.erasure.sets[0][2];
        replace_heal_test_format(&sets, 2, &poisoned_format).await;
        let set_disks = set_level_heal_view(&sets).await;

        let (_, set_err) = set_disks
            .heal_format(false)
            .await
            .expect("set format heal should report a typed identity mismatch");
        assert!(
            matches!(set_err, Some(StorageError::CorruptedFormat)),
            "a foreign minority must not be reported as no-heal-required: {set_err:?}"
        );

        let (_, pool_err) = sets
            .heal_format(false)
            .await
            .expect("pool format heal should report a typed identity mismatch");
        assert!(
            matches!(pool_err, Some(StorageError::CorruptedFormat)),
            "a foreign minority must not be reported as no-heal-required: {pool_err:?}"
        );
        assert_eq!(
            read_heal_test_format(&sets, 2).await,
            poisoned_format,
            "format heal must not overwrite a foreign disk"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn list_multipart_uploads_merges_all_sets_without_pagination_loss() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::Erasure).await;
        let (_temp_dirs, sets) = make_local_two_set_sets().await;
        let bucket = format!("multipart-list-{}", Uuid::new_v4().simple());
        sets.make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let mut keys_by_set = [Vec::new(), Vec::new()];
        for index in 0..100 {
            let key = format!("logs/{index:03}.bin");
            let set_index = sets.get_hashed_set_index(&key);
            if keys_by_set[set_index].len() < 2 {
                keys_by_set[set_index].push(key);
            }
            if keys_by_set.iter().all(|keys| keys.len() == 2) {
                break;
            }
        }
        assert!(keys_by_set.iter().all(|keys| keys.len() == 2), "test keys must span both sets");

        let repeated_key = keys_by_set[0][0].clone();
        let mut expected = Vec::new();
        for key in keys_by_set.iter().flatten() {
            let upload = sets
                .new_multipart_upload(&bucket, key, &ObjectOptions::default())
                .await
                .expect("multipart upload should be created");
            expected.push((key.clone(), upload.upload_id));
        }
        let second = sets
            .new_multipart_upload(&bucket, &repeated_key, &ObjectOptions::default())
            .await
            .expect("second upload for the same key should be created");
        expected.push((repeated_key, second.upload_id));
        expected.sort();

        let mut actual = Vec::new();
        let mut key_marker = None;
        let mut upload_id_marker = None;
        for _ in 0..expected.len() + 1 {
            let page = sets
                .list_multipart_uploads(&bucket, "logs/", key_marker.clone(), upload_id_marker.clone(), None, 2)
                .await
                .expect("multipart page should list across every set");
            assert!(page.uploads.len() <= 2);
            actual.extend(
                page.uploads
                    .iter()
                    .map(|upload| (upload.object.clone(), upload.upload_id.clone())),
            );
            if !page.is_truncated {
                break;
            }
            key_marker = page.next_key_marker;
            upload_id_marker = page.next_upload_id_marker;
        }

        // Compare only the decoded `<uuid>x<timestamp>` suffixes: the full
        // upload id embeds the process-global deployment id, which a
        // concurrently running test can swap between create and list time.
        let normalize = |uploads: &[(String, String)]| {
            let mut normalized = uploads
                .iter()
                .map(|(key, upload_id)| (key.clone(), runtime_sources::upload_uuid_suffix(upload_id)))
                .collect::<Vec<_>>();
            normalized.sort();
            normalized
        };
        let actual = normalize(&actual);
        assert_eq!(actual, normalize(&expected), "set-level merge must return every upload exactly once");
        let mut deduped = actual.clone();
        deduped.dedup();
        assert_eq!(deduped.len(), actual.len(), "set-level pagination must not duplicate uploads");

        let mut nested_by_set = [None, None];
        for index in 0..100 {
            let key = format!("nested/group-{index:03}/file.bin");
            let set_index = sets.get_hashed_set_index(&key);
            nested_by_set[set_index].get_or_insert(key);
            if nested_by_set.iter().all(Option::is_some) {
                break;
            }
        }
        for key in nested_by_set.iter().flatten() {
            sets.new_multipart_upload(&bucket, key, &ObjectOptions::default())
                .await
                .expect("nested multipart upload should be created");
        }
        let mut expected_prefixes = nested_by_set
            .iter()
            .flatten()
            .map(|key| {
                key.rsplit_once('/')
                    .expect("nested key should contain a delimiter")
                    .0
                    .to_string()
                    + "/"
            })
            .collect::<Vec<_>>();
        expected_prefixes.sort();

        let first = sets
            .list_multipart_uploads(&bucket, "nested/", None, None, Some("/".to_string()), 1)
            .await
            .expect("first delimiter page should list across every set");
        assert!(first.is_truncated);
        assert_eq!(first.common_prefixes, expected_prefixes[..1]);
        let second = sets
            .list_multipart_uploads(
                &bucket,
                "nested/",
                first.next_key_marker,
                first.next_upload_id_marker,
                Some("/".to_string()),
                1,
            )
            .await
            .expect("second delimiter page should list across every set");
        assert!(!second.is_truncated);
        assert_eq!(second.common_prefixes, expected_prefixes[1..]);
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
            ctx: bootstrap_ctx(),
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
    async fn sets_check_abandoned_parts_rejects_invalid_set_scope() {
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
            ctx: bootstrap_ctx(),
        };

        let err = sets
            .check_abandoned_parts(
                "bucket",
                "object",
                &HealOpts {
                    set: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect_err("out-of-range abandoned-parts set scope must fail closed");
        assert!(
            matches!(err, StorageError::InvalidArgument(_, ref field, ref reason)
                if field == "set" && reason.contains("invalid heal set index 1")),
            "unexpected invalid set error: {err:?}"
        );
    }

    // Builds a single-set `Sets` over `SET_DRIVE_COUNT` local temp-dir disks,
    // formatting the first `num_formatted` of them against a shared reference
    // format and leaving the rest unformatted. Returns the live TempDir handles
    // (must be kept alive), the reference format, and the assembled `Sets`.
    // `disk_set` is intentionally empty: these tests only exercise paths that
    // return before pool-level healing delegates into a set.
    async fn setup_heal_format_sets(num_formatted: usize, foreign_identity: bool) -> (Vec<tempfile::TempDir>, FormatV3, Sets) {
        const SET_DRIVE_COUNT: usize = 3;
        let ref_format = FormatV3::new(1, SET_DRIVE_COUNT);
        let mut stored_format = ref_format.clone();
        if foreign_identity {
            stored_format.id = Uuid::new_v4();
        }

        let mut dirs = Vec::with_capacity(SET_DRIVE_COUNT);
        let mut endpoints = Vec::with_capacity(SET_DRIVE_COUNT);
        for i in 0..SET_DRIVE_COUNT {
            let dir = tempfile::tempdir().expect("tempdir should be created");
            let mut endpoint =
                Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(i);
            dirs.push(dir);
            endpoints.push(endpoint);
        }

        for (i, endpoint) in endpoints.iter().enumerate().take(num_formatted) {
            let disk = new_disk(
                endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("disk should be created");
            let mut disk_format = stored_format.clone();
            disk_format.erasure.this = stored_format.erasure.sets[0][i];
            save_format_file(&Some(disk), &Some(disk_format))
                .await
                .expect("format should be saved");
        }

        let sets = Sets {
            id: ref_format.id,
            disk_set: Vec::new(),
            pool_idx: 0,
            endpoints: PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: SET_DRIVE_COUNT,
                endpoints: Endpoints::from(endpoints),
                cmd_line: String::new(),
                platform: String::new(),
            },
            format: ref_format.clone(),
            parity_count: 1,
            set_count: 1,
            set_drive_count: SET_DRIVE_COUNT,
            default_parity_count: 1,
            distribution_algo: DistributionAlgoVersion::V1,
            exit_signal: None,
            ctx: bootstrap_ctx(),
        };

        (dirs, ref_format, sets)
    }

    async fn set_level_heal_view(sets: &Sets) -> Arc<SetDisks> {
        let endpoints = sets.endpoints.endpoints.as_ref().clone();
        let mut disks = Vec::with_capacity(endpoints.len());
        for endpoint in &endpoints {
            disks.push(Some(
                new_disk(
                    endpoint,
                    &DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await
                .expect("fresh set-level disk handle should open"),
            ));
        }

        SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            endpoints.len(),
            1,
            0,
            0,
            endpoints,
            sets.format.clone(),
            Vec::new(),
        )
        .await
    }

    async fn replace_heal_test_format(sets: &Sets, disk_index: usize, format: &FormatV3) {
        let disk = new_disk(
            &sets.endpoints.endpoints.as_ref()[disk_index],
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("heal test disk should open");
        save_format_file(&Some(disk.clone()), &Some(format.clone()))
            .await
            .expect("poisoned test format should be written");
    }

    async fn read_heal_test_format(sets: &Sets, disk_index: usize) -> FormatV3 {
        let path = std::path::Path::new(&sets.endpoints.endpoints.as_ref()[disk_index].get_file_path())
            .join(crate::disk::RUSTFS_META_BUCKET)
            .join(crate::disk::FORMAT_CONFIG_FILE);
        let data = tokio::fs::read(path).await.expect("test format should be readable");
        FormatV3::try_from(data.as_slice()).expect("test format should parse")
    }

    // Regression for #956 (NoHealRequired path): with every disk already
    // formatted, `heal_format` reports exactly one drive record per disk
    // (N = set_count * set_drive_count), each carrying a real endpoint. Before
    // the fix the list was pre-filled with N empty placeholders and then N real
    // entries were pushed, yielding a 2N list whose first half was blank.
    #[tokio::test]
    #[serial]
    async fn heal_format_no_heal_required_reports_one_record_per_disk() {
        let (_dirs, _ref_format, sets) = setup_heal_format_sets(3, false).await;

        let (res, err) = sets.heal_format(true).await.expect("heal_format should succeed");
        // All disks formatted -> NoHealRequired early return, still returns `res`.
        assert!(matches!(err, Some(StorageError::NoHealRequired)), "expected NoHealRequired, got {err:?}");

        assert_eq!(res.before.drives.len(), 3, "before drives must be N, not 2N");
        assert_eq!(res.after.drives.len(), 3, "after drives must be N, not 2N");
        for (i, d) in res.before.drives.iter().enumerate() {
            assert!(
                !d.endpoint.is_empty(),
                "before drive {i} endpoint must not be empty (no placeholder rows)"
            );
            assert_eq!(d.state, DriveState::Ok.to_string(), "formatted disk {i} must be Ok");
        }
        for (i, d) in res.after.drives.iter().enumerate() {
            assert!(!d.endpoint.is_empty(), "after drive {i} endpoint must not be empty (no placeholder rows)");
        }
    }

    // Regression for #956 (heal path): with one unformatted disk the heal path is
    // taken (past the NoHealRequired check). Even here the reported drive list is
    // length N (never 2N) and index-aligned with the endpoints, so the healed
    // status updates (indexed `i * set_drive_count + j`, 0..N) address the real
    // drive entries rather than an empty placeholder half. `dry_run == true` keeps
    // the assertion on the reported shape without mutating disks or global state.
    #[tokio::test]
    #[serial]
    async fn heal_format_heal_path_reports_one_record_per_disk_aligned() {
        // Disks 0 and 1 formatted (quorum), disk 2 unformatted.
        let (_dirs, _ref_format, sets) = setup_heal_format_sets(2, false).await;

        let (res, err) = sets.heal_format(true).await.expect("heal_format should succeed");
        // Unformatted disk present -> heal path, not NoHealRequired.
        assert!(err.is_none(), "expected heal path (no NoHealRequired), got {err:?}");

        assert_eq!(res.before.drives.len(), 3, "before drives must be N, not 2N");
        assert_eq!(res.after.drives.len(), 3, "after drives must be N, not 2N");

        // Every record maps to a real endpoint; the drive list is index-aligned
        // with `formats_to_drives_info`, so no empty placeholder half remains.
        for (i, d) in res.before.drives.iter().enumerate() {
            assert!(!d.endpoint.is_empty(), "before drive {i} endpoint must not be empty");
        }
        assert_eq!(res.before.drives[0].state, DriveState::Ok.to_string());
        assert_eq!(res.before.drives[1].state, DriveState::Ok.to_string());
        // The unformatted disk is reported Missing on its own (real) entry.
        assert_eq!(
            res.before.drives[2].state,
            DriveState::Missing.to_string(),
            "unformatted disk must be Missing on its real index, not on a placeholder"
        );
    }

    #[tokio::test]
    #[serial]
    async fn replacement_format_only_writes_the_requested_slot() {
        let (_dirs, _ref_format, sets) = setup_heal_format_sets(1, false).await;
        let target = sets.endpoints.endpoints.as_ref()[1].to_string();
        let untouched = sets.endpoints.endpoints.as_ref()[2].to_string();
        let set = set_level_heal_view(&sets).await;

        let (result, error) = set
            .heal_replacement_format(false, std::slice::from_ref(&target))
            .await
            .expect("target-scoped replacement format should run");

        assert!(error.is_none(), "target format must not report an error: {error:?}");
        assert!(
            result
                .after
                .drives
                .iter()
                .any(|drive| drive.endpoint == target && drive.state == DriveState::Ok.to_string()),
            "requested replacement slot must be formatted"
        );
        let untouched_format = std::path::Path::new(&sets.endpoints.endpoints.as_ref()[2].get_file_path())
            .join(crate::disk::RUSTFS_META_BUCKET)
            .join(crate::disk::FORMAT_CONFIG_FILE);
        assert!(
            !tokio::fs::try_exists(untouched_format)
                .await
                .expect("untouched replacement format path should be inspectable"),
            "unrequested slot {untouched} must remain unformatted"
        );
    }

    fn instance_ctx_test_pool_endpoints() -> (FormatV3, PoolEndpoints) {
        let format = FormatV3::new(1, 2);
        let endpoints = vec![
            Endpoint::try_from("http://127.0.0.1:9000/data0").expect("first endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9001/data1").expect("second endpoint should parse"),
        ];
        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "instance-ctx-adoption-test".to_string(),
            platform: "test".to_string(),
        };
        (format, pool_endpoints)
    }

    // Phase 5 follow-up (backlog#1052): a pool built through the ctx-explicit
    // constructor carries the caller's context through Sets AND every SetDisks,
    // so nothing in the object graph silently binds to the process bootstrap.
    #[tokio::test]
    async fn sets_new_with_instance_ctx_threads_context_through_graph() {
        let (format, pool_endpoints) = instance_ctx_test_pool_endpoints();
        let instance_ctx = Arc::new(InstanceContext::new());

        let sets = Sets::new_with_instance_ctx(vec![None, None], &pool_endpoints, &format, 0, 1, instance_ctx.clone())
            .await
            .expect("sets should build with empty disks");

        assert!(
            Arc::ptr_eq(sets.instance_ctx(), &instance_ctx),
            "Sets must adopt the explicitly passed instance context"
        );
        for set_disks in &sets.disk_set {
            assert!(
                Arc::ptr_eq(set_disks.instance_ctx(), &instance_ctx),
                "every SetDisks must adopt the explicitly passed instance context"
            );
        }
        assert!(
            !Arc::ptr_eq(sets.instance_ctx(), &bootstrap_ctx()),
            "a fresh context must not alias the process bootstrap context"
        );
    }

    // The legacy constructor keeps single-instance behavior byte-for-byte: it
    // still adopts the process bootstrap context.
    #[tokio::test]
    async fn sets_new_legacy_adopts_bootstrap_context() {
        let (format, pool_endpoints) = instance_ctx_test_pool_endpoints();

        let sets = Sets::new(vec![None, None], &pool_endpoints, &format, 0, 1)
            .await
            .expect("sets should build with empty disks");

        assert!(
            Arc::ptr_eq(sets.instance_ctx(), &bootstrap_ctx()),
            "legacy Sets::new must keep adopting the process bootstrap context"
        );
    }
}
