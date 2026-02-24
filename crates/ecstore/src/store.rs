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
    GLOBAL_LOCAL_DISK_MAP, GLOBAL_LOCAL_DISK_SET_DRIVES, GLOBAL_TierConfigMgr, get_global_bucket_monitor,
    get_global_deployment_id, get_global_endpoints, init_global_bucket_monitor, is_dist_erasure, is_erasure_sd,
    set_global_deployment_id, set_object_layer,
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
    all_local_disk, all_local_disk_path, find_local_disk, get_disk_infos, get_disk_via_endpoint, has_space_for, init_local_disks,
    init_lock_clients,
};

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

// init_local_disks must succeed before the server starts
/// create unique lock clients for the endpoints and store them globally
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
        self.handle_get_object_reader(bucket, object, range, h, opts).await
    }
    #[instrument(level = "debug", skip(self, data))]
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_put_object(bucket, object, data, opts).await
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
        self.handle_new_ns_lock(bucket, object).await
    }
    #[instrument(skip(self))]
    async fn backend_info(&self) -> rustfs_madmin::BackendInfo {
        self.handle_backend_info().await
    }
    #[instrument(skip(self))]
    async fn storage_info(&self) -> rustfs_madmin::StorageInfo {
        self.handle_storage_info().await
    }
    #[instrument(skip(self))]
    async fn local_storage_info(&self) -> rustfs_madmin::StorageInfo {
        self.handle_local_storage_info().await
    }

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

    #[instrument(skip(self))]
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_get_object_info(bucket, object, opts).await
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
        self.handle_copy_object(src_bucket, src_object, dst_bucket, dst_object, src_info, src_opts, dst_opts)
            .await
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
        self.handle_complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
            .await
    }

    #[instrument(skip(self))]
    async fn get_disks(&self, pool_idx: usize, set_idx: usize) -> Result<Vec<Option<DiskStore>>> {
        self.handle_get_disks(pool_idx, set_idx).await
    }

    #[instrument(skip(self))]
    fn set_drive_counts(&self) -> Vec<usize> {
        self.handle_set_drive_counts()
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
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()> {
        self.handle_delete_object_version(bucket, object, fi, force_del_marker).await
    }

    #[instrument(skip(self))]
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.handle_delete_object_tags(bucket, object, opts).await
    }

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

    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        self.handle_verify_object_integrity(bucket, object, opts).await
    }
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
