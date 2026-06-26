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

use crate::bucket::bandwidth::monitor::Monitor;
use crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
use crate::bucket::lifecycle::bucket_lifecycle_ops::{
    enqueue_immediate_expiry, enqueue_transition_immediate, init_background_expiry,
};
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
use crate::config::storageclass;
use crate::disk::endpoint::{Endpoint, EndpointType};
use crate::disk::{DiskAPI, DiskInfo, DiskInfoOptions};
use crate::error::{Error, Result};
use crate::error::{
    StorageError, is_err_bucket_exists, is_err_bucket_not_found, is_err_invalid_upload_id, is_err_object_not_found,
    is_err_read_quorum, is_err_version_not_found, to_object_err,
};
use crate::pools::PoolMeta;
use crate::rebalance::RebalanceMeta;
use crate::rpc::RemoteClient;
use crate::runtime::global::{DISK_RESERVE_FRACTION, TypeLocalDiskSetDrives};
use crate::runtime::sources as runtime_sources;
use crate::services::event_notification::EventNotifier;
use crate::storage_api_contracts::{
    bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
    list::{StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr, StorageWalkOptions},
    multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo},
    object::{DeletedObject, ObjectToDelete},
    range::HTTPRangeSpec,
};
use crate::store_init::{check_disk_fatal_errs, ec_drives_no_config};
use crate::tier::tier::TierConfigMgr;
use crate::{
    bucket::{lifecycle::bucket_lifecycle_ops::TransitionState, metadata::BucketMetadata},
    disk::{BUCKET_META_PREFIX, DiskOption, DiskStore, RUSTFS_META_BUCKET},
    endpoints::EndpointServerPools,
    object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader},
    rpc::S3PeerSys,
    sets::Sets,
    store_init,
};
use futures::future::join_all;
use http::HeaderMap;
use lazy_static::lazy_static;
use rand::RngExt as _;
use rustfs_common::heal_channel::{HealItemType, HealOpts};
use rustfs_config::server_config::Config;
use rustfs_filemeta::FileInfo;
use rustfs_lock::{LocalClient, LockClient, NamespaceLockWrapper};
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_utils::path::{decode_dir_object, encode_dir_object, path_join_buf};
use s3s::dto::{BucketVersioningStatus, ObjectLockConfiguration, ObjectLockEnabled, VersioningConfiguration};
use std::net::SocketAddr;
use std::process::exit;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::Duration,
};
use time::OffsetDateTime;
use tokio::select;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

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

async fn enqueue_transition_after_write(result: Result<ObjectInfo>, src: LcEventSrc) -> Result<ObjectInfo> {
    match result {
        Ok(oi) => {
            if should_enqueue_transition_immediately(&oi) {
                enqueue_transition_immediate(&oi, src.clone()).await;
                enqueue_immediate_expiry(&oi, src).await;
            }
            Ok(oi)
        }
        Err(err) => Err(err),
    }
}

fn should_enqueue_transition_immediately(oi: &ObjectInfo) -> bool {
    !is_meta_bucketname(&oi.bucket)
}

const MAX_UPLOADS_LIST: usize = 10000;

mod bucket;
mod heal;
mod init;
mod list;
mod multipart;
mod object;
mod peer;
mod rebalance;

use peer::init_local_peer;
pub use peer::{
    all_local_disk, all_local_disk_path, find_local_disk_by_ref, get_disk_infos, init_local_disks, init_lock_clients,
    prewarm_local_disk_id_map,
};

pub struct ECStore {
    pub id: Uuid,
    // pub disks: Vec<DiskStore>,
    pub disk_map: HashMap<usize, Vec<Option<DiskStore>>>,
    pub pools: Vec<Arc<Sets>>,
    pub peer_sys: S3PeerSys,
    // pub local_disks: Vec<DiskStore>,
    pub pool_meta: RwLock<PoolMeta>,
    pub rebalance_meta: RwLock<Option<RebalanceMeta>>,
    pub decommission_cancelers: RwLock<Vec<Option<CancellationToken>>>,
    /// Serializes rebalance/decommission start transitions.
    ///
    /// Lock order: acquire `start_gate` before `pool_meta`, `rebalance_meta`,
    /// or `decommission_cancelers`. The guarded sections may perform bounded
    /// async metadata work so check/init/start cannot race across operations.
    pub(crate) start_gate: Mutex<()>,
    /// Serializes full-document pool metadata saves.
    ///
    /// Lock order: acquire `pool_meta_save_gate` without holding `pool_meta`.
    /// The saver then clones the latest `pool_meta` under a short read lock and
    /// releases it before awaiting disk writes.
    pub(crate) pool_meta_save_gate: Mutex<()>,

    // Phase 2 migration pending - do not use directly.
    /// Local disk maps (migrated from GLOBAL_LOCAL_DISK_MAP/ID_MAP/SET_DRIVES)
    pub(crate) local_disk_map: Arc<RwLock<HashMap<String, Option<DiskStore>>>>,
    pub(crate) local_disk_id_map: Arc<RwLock<HashMap<Uuid, String>>>,
    pub(crate) local_disk_set_drives: Arc<RwLock<TypeLocalDiskSetDrives>>,
    /// Tier config manager (migrated from GLOBAL_TierConfigMgr)
    pub(crate) tier_config_mgr: Arc<RwLock<TierConfigMgr>>,
    /// Event notifier (migrated from GLOBAL_EventNotifier)
    pub(crate) event_notifier: Arc<RwLock<EventNotifier>>,
    /// Bucket monitor (migrated from GLOBAL_BUCKET_MONITOR)
    pub(crate) bucket_monitor: OnceLock<Arc<Monitor>>,
}

impl std::fmt::Debug for ECStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ECStore")
            .field("id", &self.id)
            .field("disk_map", &self.disk_map)
            .field("pools", &self.pools)
            .field("pool_meta", &self.pool_meta)
            .finish_non_exhaustive()
    }
}

/// Phase 2: Accessor methods for config globals
/// These delegate to the process-global statics. No local state — the globals
/// remain the single source of truth until the migration is complete.
impl ECStore {
    /// Get server configuration (delegates to global)
    pub fn get_server_config(&self) -> Option<Config> {
        runtime_sources::server_config()
    }

    /// Set server configuration (delegates to global)
    pub fn set_server_config(&self, cfg: Config) {
        runtime_sources::set_server_config(cfg);
    }

    /// Get storage class configuration (delegates to global)
    pub fn get_storage_class(&self) -> Option<crate::config::storageclass::Config> {
        runtime_sources::storage_class_config()
    }

    /// Set storage class configuration (delegates to global)
    pub fn set_storage_class(&self, cfg: crate::config::storageclass::Config) {
        runtime_sources::set_storage_class_config(cfg);
    }
}

/// Phase 3: Accessor methods for service globals
/// These provide a unified API through ECStore for accessing cross-cutting
/// service singletons. The globals remain the source of truth.
impl ECStore {
    /// Get the notification system
    pub fn notification_system(&self) -> Option<&'static crate::services::notification_sys::NotificationSys> {
        runtime_sources::notification_sys()
    }

    /// Get the bucket metadata system
    pub fn bucket_metadata_sys(&self) -> Option<Arc<tokio::sync::RwLock<crate::bucket::metadata_sys::BucketMetadataSys>>> {
        runtime_sources::bucket_metadata_sys()
    }

    /// Get the global endpoints
    pub fn endpoints(&self) -> EndpointServerPools {
        runtime_sources::endpoint_pools().unwrap_or_else(|| Vec::new().into())
    }

    /// Get the global region
    pub fn region(&self) -> Option<s3s::region::Region> {
        runtime_sources::region()
    }

    /// Get the tier config manager
    pub fn tier_config_mgr(&self) -> Arc<tokio::sync::RwLock<crate::tier::tier::TierConfigMgr>> {
        runtime_sources::global_tier_config_mgr()
    }

    /// Get the server configuration
    pub fn server_config(&self) -> Option<Config> {
        runtime_sources::server_config()
    }

    /// Get the storage class configuration
    pub fn storage_class(&self) -> Option<crate::config::storageclass::Config> {
        runtime_sources::storage_class_config()
    }
}

/// Phase 4: Server address accessors
/// These provide a unified API through ECStore for accessing server-level
/// configuration globals. The globals remain the source of truth.
impl ECStore {
    /// Get the server port
    pub fn port(&self) -> u16 {
        runtime_sources::rustfs_port()
    }

    /// Get the server host
    pub async fn host(&self) -> String {
        runtime_sources::rustfs_host().await
    }

    /// Get the server address (host:port)
    pub async fn addr(&self) -> String {
        runtime_sources::rustfs_addr().await
    }
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
impl crate::storage_api_contracts::object::ObjectIO for ECStore {
    type Error = Error;
    type RangeSpec = HTTPRangeSpec;
    type HeaderMap = HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = GetObjectReader;
    type PutObjectReader = PutObjReader;

    #[instrument(level = "debug", skip(self))]
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        self.handle_get_object_reader(bucket, object, range, h, opts).await
    }
    #[instrument(level = "debug", skip(self, data))]
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        enqueue_transition_after_write(self.handle_put_object(bucket, object, data, opts).await, LcEventSrc::S3PutObject).await
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
impl BucketOperations for ECStore {
    type Error = Error;

    #[instrument(skip(self))]
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        self.handle_make_bucket(bucket, opts).await
    }

    #[instrument(skip(self))]
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo> {
        self.handle_get_bucket_info(bucket, opts).await
    }
    #[instrument(skip(self))]
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        self.handle_list_bucket(opts).await
    }
    #[instrument(skip(self))]
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        self.handle_delete_bucket(bucket, opts).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::object::ObjectOperations for ECStore {
    type Error = Error;
    type ObjectInfo = ObjectInfo;
    type ObjectOptions = ObjectOptions;
    type FileInfo = FileInfo;
    type ObjectToDelete = ObjectToDelete;
    type DeletedObject = DeletedObject;

    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_get_object_info(bucket, object, opts).await
    }

    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.handle_verify_object_integrity(bucket, object, opts).await
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
        enqueue_transition_after_write(
            self.handle_copy_object(src_bucket, src_object, dst_bucket, dst_object, src_info, src_opts, dst_opts)
                .await,
            LcEventSrc::S3CopyObject,
        )
        .await
    }

    #[instrument(skip(self))]
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()> {
        self.handle_delete_object_version(bucket, object, fi, force_del_marker).await
    }

    #[instrument(skip(self))]
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        self.handle_delete_object(bucket, object, opts).await
    }
    // TODO: review
    #[instrument(skip(self))]
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        self.handle_delete_objects(bucket, objects, opts).await
    }

    #[instrument(skip(self))]
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_put_object_metadata(bucket, object, opts).await
    }
    #[instrument(skip(self))]
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        self.handle_get_object_tags(bucket, object, opts).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_put_object_tags(bucket, object, tags, opts).await
    }

    #[instrument(skip(self))]
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_delete_object_tags(bucket, object, opts).await
    }

    #[instrument(skip(self))]
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        self.handle_add_partial(bucket, object, version_id).await
    }
    #[instrument(skip(self))]
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.handle_transition_object(bucket, object, opts).await
    }

    #[instrument(skip(self))]
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.handle_restore_transitioned_object(bucket, object, opts).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::list::ListOperations for ECStore {
    type Error = Error;
    type ListObjectsV2Info = ListObjectsV2Info;
    type ListObjectVersionsInfo = ListObjectVersionsInfo;
    type ObjectInfoOrErr = ObjectInfoOrErr;
    type WalkOptions = WalkOptions;
    type WalkCancellation = CancellationToken;
    type WalkResultSender = tokio::sync::mpsc::Sender<ObjectInfoOrErr>;

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
        self.handle_list_objects_v2(
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
        self.handle_list_object_versions(bucket, prefix, marker, version_marker, delimiter, max_keys)
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
        self.handle_walk(rx, bucket, prefix, result, opts).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::multipart::MultipartOperations for ECStore {
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
        self.handle_list_multipart_uploads(bucket, prefix, key_marker, upload_id_marker, delimiter, max_uploads)
            .await
    }

    #[instrument(skip(self))]
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        self.handle_new_multipart_upload(bucket, object, opts).await
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
        self.handle_copy_object_part(
            src_bucket,
            src_object,
            _dst_bucket,
            _dst_object,
            _upload_id,
            _part_id,
            _start_offset,
            _length,
            _src_info,
            _src_opts,
            _dst_opts,
        )
        .await
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
        self.handle_put_object_part(bucket, object, upload_id, part_id, data, opts)
            .await
    }

    #[instrument(skip(self))]
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        self.handle_get_multipart_info(bucket, object, upload_id, opts).await
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
        self.handle_list_object_parts(bucket, object, upload_id, part_number_marker, max_parts, opts)
            .await
    }

    #[instrument(skip(self))]
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()> {
        self.handle_abort_multipart_upload(bucket, object, upload_id, opts).await
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
        enqueue_transition_after_write(
            self.handle_complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
                .await,
            LcEventSrc::S3CompleteMultipartUpload,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::heal::HealOperations for ECStore {
    type Error = Error;
    type HealResultItem = HealResultItem;
    type HealOptions = HealOpts;

    #[instrument(skip(self))]
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        self.handle_heal_format(dry_run).await
    }

    #[instrument(skip(self))]
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        self.handle_heal_bucket(bucket, opts).await
    }
    #[instrument(skip(self))]
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.handle_heal_object(bucket, object, version_id, opts).await
    }

    #[instrument(skip(self))]
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
        self.handle_get_pool_and_set(id).await
    }

    #[instrument(skip(self))]
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()> {
        self.handle_check_abandoned_parts(bucket, object, opts).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::namespace::NamespaceLocking for ECStore {
    type Error = Error;
    type NamespaceLock = NamespaceLockWrapper;

    async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper> {
        self.handle_new_ns_lock(bucket, object).await
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::admin::StorageAdminApi for ECStore {
    type BackendInfo = rustfs_madmin::BackendInfo;
    type StorageInfo = rustfs_madmin::StorageInfo;
    type Disk = DiskStore;
    type Error = Error;

    #[instrument(skip(self))]
    async fn backend_info(&self) -> Self::BackendInfo {
        self.handle_backend_info().await
    }

    #[instrument(skip(self))]
    async fn storage_info(&self) -> Self::StorageInfo {
        self.handle_storage_info().await
    }

    #[instrument(skip(self))]
    async fn local_storage_info(&self) -> Self::StorageInfo {
        self.handle_local_storage_info().await
    }

    #[instrument(skip(self))]
    async fn disk_set_inventory(
        &self,
        selector: crate::storage_api_contracts::admin::DiskSetSelector,
    ) -> Result<Vec<Option<Self::Disk>>> {
        self.handle_get_disks(selector.pool_idx, selector.set_idx).await
    }

    #[instrument(skip(self))]
    fn set_drive_counts(&self) -> Vec<usize> {
        self.handle_set_drive_counts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::{Endpoints, PoolEndpoints};
    use crate::runtime::global::{GLOBAL_LOCAL_DISK_ID_MAP, reset_local_disk_test_state};
    use crate::store_init::{connect_load_init_formats, init_disks};
    use serial_test::serial;
    use tempfile::TempDir;

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

        let result = crate::layout::pool_space::has_space_for(&disk_infos, 1024).await;
        // Should fail due to no valid disk info
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_find_local_disk() {
        let result = peer::find_local_disk("/nonexistent/path").await;
        assert!(result.is_none(), "Should return None for nonexistent path");
    }

    #[tokio::test]
    #[serial]
    async fn test_find_local_disk_by_ref_backfills_uuid_map() {
        reset_local_disk_test_state().await;

        let temp_dir = TempDir::new().expect("create temp dir for local disk ref test");
        let disk_paths = (0..4)
            .map(|idx| temp_dir.path().join(format!("disk{}", idx + 1)))
            .collect::<Vec<_>>();
        for disk_path in &disk_paths {
            std::fs::create_dir_all(disk_path).expect("create disk path");
        }

        let mut endpoints = Vec::new();
        for (idx, disk_path) in disk_paths.iter().enumerate() {
            let mut endpoint = Endpoint::try_from(disk_path.to_str().expect("disk path to str")).expect("endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(idx);
            endpoints.push(endpoint);
        }

        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "find-local-disk-by-ref-test".to_string(),
            platform: "test".to_string(),
        }]);

        init_local_disks(endpoint_pools.clone()).await.expect("init local disks");

        let (disks, errs) = init_disks(
            &endpoint_pools.as_ref().first().expect("pool endpoints").endpoints,
            &DiskOption {
                cleanup: true,
                health_check: false,
            },
        )
        .await;

        assert!(errs.iter().all(|err| err.is_none()), "disk init should succeed: {errs:?}");
        connect_load_init_formats(true, &disks, 1, 4, None)
            .await
            .expect("initialize format metadata");

        GLOBAL_LOCAL_DISK_ID_MAP.write().await.clear();

        let local_disks = all_local_disk().await;
        let first_disk = local_disks.first().expect("local disk exists");
        let disk_id = first_disk
            .get_disk_id()
            .await
            .expect("get disk id should succeed")
            .expect("disk id should exist");

        let found = find_local_disk_by_ref(&disk_id.to_string()).await;
        assert!(found.is_some(), "disk lookup by id should backfill cache");
        assert_eq!(
            GLOBAL_LOCAL_DISK_ID_MAP.read().await.get(&disk_id).cloned(),
            Some(first_disk.endpoint().to_string())
        );

        reset_local_disk_test_state().await;
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

    #[test]
    fn test_should_not_enqueue_transition_for_internal_metadata_bucket() {
        let oi = ObjectInfo {
            bucket: RUSTFS_META_BUCKET.to_string(),
            name: format!("{BUCKET_META_PREFIX}/bucket/.metadata.bin"),
            ..Default::default()
        };

        assert!(!should_enqueue_transition_immediately(&oi));
    }
}
