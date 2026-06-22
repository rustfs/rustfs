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

use http::HeaderMap;
use rustfs_ecstore::api::bucket as ecstore_bucket;
use rustfs_ecstore::api::cache as ecstore_cache;
use rustfs_ecstore::api::capacity as ecstore_capacity;
use rustfs_ecstore::api::config as ecstore_config;
use rustfs_ecstore::api::data_usage as ecstore_data_usage;
use rustfs_ecstore::api::disk as ecstore_disk;
use rustfs_ecstore::api::error as ecstore_error;
use rustfs_ecstore::api::global as ecstore_global;
use rustfs_ecstore::api::set_disk as ecstore_set_disk;
use rustfs_ecstore::api::storage as ecstore_storage;
use rustfs_ecstore::api::tier as ecstore_tier;
use rustfs_storage_api::{HTTPRangeSpec, ObjectIO, ObjectToDelete};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) const BUCKET_META_PREFIX: &str = ecstore_disk::BUCKET_META_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = ecstore_disk::RUSTFS_META_BUCKET;
pub(crate) const STORAGE_FORMAT_FILE: &str = ecstore_disk::STORAGE_FORMAT_FILE;
pub(crate) const TRANSITION_COMPLETE: &str = ecstore_bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;

pub(crate) type Disk = ecstore_disk::Disk;
#[cfg(test)]
pub(crate) type DiskStore = ecstore_disk::DiskStore;
pub(crate) type DiskLocation = ecstore_disk::DiskLocation;
pub(crate) type DiskError = ecstore_disk::error::DiskError;
pub(crate) type DiskResult<T> = ecstore_disk::error::Result<T>;
pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type EcstoreError = ecstore_error::Error;
pub(crate) type EcstoreResult<T> = ecstore_error::Result<T>;
pub(crate) type ListPathRawOptions = ecstore_cache::ListPathRawOptions;
pub(crate) type BucketTargetSys = ecstore_bucket::bucket_target_sys::BucketTargetSys;
pub(crate) type BucketVersioningSys = ecstore_bucket::versioning_sys::BucketVersioningSys;
pub(crate) type DiskInfoOptions = ecstore_disk::DiskInfoOptions;
pub(crate) type Evaluator = ecstore_bucket::lifecycle::evaluator::Evaluator;
pub(crate) type Event = ecstore_bucket::lifecycle::lifecycle::Event;
pub(crate) type LcEventSrc = ecstore_bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
pub(crate) type ObjectOpts = ecstore_bucket::lifecycle::lifecycle::ObjectOpts;
pub(crate) type ReplicationConfig = ecstore_bucket::replication::ReplicationConfig;
pub(crate) type ReplicationHealQueueResult = ecstore_bucket::replication::ReplicationHealQueueResult;
pub(crate) type ReplicationQueueAdmission = ecstore_bucket::replication::ReplicationQueueAdmission;
pub(crate) type ScanGuard = ecstore_disk::ScanGuard;
pub(crate) type SetDisks = ecstore_set_disk::SetDisks;
pub(crate) type StorageError = ecstore_error::StorageError;

pub type ScannerGetObjectReader = <ECStore as ObjectIO>::GetObjectReader;
pub type ScannerObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub type ScannerObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub type ScannerObjectToDelete = ObjectToDelete;
pub type ScannerPutObjReader = <ECStore as ObjectIO>::PutObjectReader;

pub(crate) mod storageclass {
    use super::ecstore_config;

    pub(crate) const RRS: &str = ecstore_config::storageclass::RRS;
    pub(crate) const STANDARD: &str = ecstore_config::storageclass::STANDARD;
}

#[cfg(test)]
pub(crate) fn init_ecstore_config_for_scanner_tests() {
    ecstore_config::init();
}

#[cfg(test)]
pub(crate) type DiskOption = ecstore_disk::DiskOption;
#[cfg(test)]
pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;

#[cfg(test)]
pub(crate) async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> ecstore_disk::error::Result<DiskStore> {
    ecstore_disk::new_disk(ep, opt).await
}

pub(crate) async fn get_lifecycle_config(
    bucket: &str,
) -> EcstoreResult<(s3s::dto::BucketLifecycleConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_lifecycle_config(bucket).await
}

pub(crate) async fn get_object_lock_config(
    bucket: &str,
) -> EcstoreResult<(s3s::dto::ObjectLockConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_object_lock_config(bucket).await
}

pub(crate) async fn get_replication_config(
    bucket: &str,
) -> EcstoreResult<(s3s::dto::ReplicationConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_replication_config(bucket).await
}

pub(crate) trait ScannerLifecycleConfigExt {
    fn has_active_rules(&self, prefix: &str) -> bool;
}

impl ScannerLifecycleConfigExt for s3s::dto::BucketLifecycleConfiguration {
    fn has_active_rules(&self, prefix: &str) -> bool {
        <s3s::dto::BucketLifecycleConfiguration as ecstore_bucket::lifecycle::lifecycle::Lifecycle>::has_active_rules(
            self, prefix,
        )
    }
}

pub(crate) trait ScannerReplicationConfigExt {
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool;
}

impl ScannerReplicationConfigExt for s3s::dto::ReplicationConfiguration {
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool {
        <s3s::dto::ReplicationConfiguration as ecstore_bucket::replication::ReplicationConfigurationExt>::has_active_rules(
            self, prefix, recursive,
        )
    }
}

pub(crate) trait ScannerVersioningConfigExt {
    fn prefix_enabled(&self, prefix: &str) -> bool;
    fn versioned(&self, prefix: &str) -> bool;
}

impl ScannerVersioningConfigExt for s3s::dto::VersioningConfiguration {
    fn prefix_enabled(&self, prefix: &str) -> bool {
        <s3s::dto::VersioningConfiguration as ecstore_bucket::versioning::VersioningApi>::prefix_enabled(self, prefix)
    }

    fn versioned(&self, prefix: &str) -> bool {
        <s3s::dto::VersioningConfiguration as ecstore_bucket::versioning::VersioningApi>::versioned(self, prefix)
    }
}

pub(crate) trait ScannerDiskExt {
    async fn disk_info(&self, opts: &DiskInfoOptions) -> DiskResult<ecstore_disk::DiskInfo>;
    async fn read_metadata(&self, volume: &str, path: &str) -> DiskResult<ecstore_disk::Bytes>;
    fn path(&self) -> PathBuf;
    fn get_disk_location(&self) -> DiskLocation;
    fn start_scan(&self) -> ScanGuard;
}

impl<T> ScannerDiskExt for T
where
    T: ecstore_disk::DiskAPI,
{
    async fn disk_info(&self, opts: &DiskInfoOptions) -> DiskResult<ecstore_disk::DiskInfo> {
        ecstore_disk::DiskAPI::disk_info(self, opts).await
    }

    async fn read_metadata(&self, volume: &str, path: &str) -> DiskResult<ecstore_disk::Bytes> {
        ecstore_disk::DiskAPI::read_metadata(self, volume, path).await
    }

    fn path(&self) -> PathBuf {
        ecstore_disk::DiskAPI::path(self)
    }

    fn get_disk_location(&self) -> DiskLocation {
        ecstore_disk::DiskAPI::get_disk_location(self)
    }

    fn start_scan(&self) -> ScanGuard {
        ecstore_disk::DiskAPI::start_scan(self)
    }
}

pub(crate) async fn apply_transition_rule(event: &Event, src: &LcEventSrc, oi: &ScannerObjectInfo) -> bool {
    ecstore_bucket::lifecycle::bucket_lifecycle_ops::apply_transition_rule(event, src, oi).await
}

pub(crate) async fn apply_expiry_rule(event: &Event, src: &LcEventSrc, oi: &ScannerObjectInfo) -> bool {
    ecstore_bucket::lifecycle::bucket_lifecycle_ops::apply_expiry_rule(event, src, oi).await
}

pub(crate) async fn list_global_tiers() -> Vec<ecstore_tier::tier_config::TierConfig> {
    ecstore_global::GLOBAL_TierConfigMgr.read().await.list_tiers()
}

pub(crate) async fn enqueue_global_free_version(oi: ScannerObjectInfo) {
    ecstore_bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
        .write()
        .await
        .enqueue_free_version(oi)
        .await;
}

pub(crate) async fn enqueue_global_newer_noncurrent(
    bucket: &str,
    to_delete_objs: Vec<ObjectToDelete>,
    event: Event,
    src: &LcEventSrc,
) -> bool {
    ecstore_bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
        .write()
        .await
        .enqueue_by_newer_noncurrent(bucket, to_delete_objs, event, src)
        .await
}

pub(crate) async fn queue_replication_heal_internal(
    bucket: &str,
    oi: ScannerObjectInfo,
    rcfg: ReplicationConfig,
    retry_count: u32,
) -> ReplicationHealQueueResult {
    ecstore_bucket::replication::queue_replication_heal_internal(bucket, oi, rcfg, retry_count).await
}

pub(crate) fn resolve_scanner_object_store_handle() -> Option<Arc<ECStore>> {
    ecstore_global::resolve_object_store_handle()
}

pub(crate) fn is_reserved_or_invalid_bucket(bucket: &str, strict: bool) -> bool {
    ecstore_capacity::is_reserved_or_invalid_bucket(bucket, strict)
}

pub(crate) fn path2_bucket_object(name: &str) -> (String, String) {
    ecstore_capacity::path2_bucket_object(name)
}

pub(crate) fn path2_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    ecstore_capacity::path2_bucket_object_with_base_path(base_path, path)
}

pub(crate) async fn is_erasure() -> bool {
    ecstore_global::is_erasure().await
}

pub(crate) async fn is_erasure_sd() -> bool {
    ecstore_global::is_erasure_sd().await
}

pub(crate) async fn read_config<S>(api: Arc<S>, file: &str) -> EcstoreResult<Vec<u8>>
where
    S: ScannerObjectIO,
{
    ecstore_config::com::read_config(api, file).await
}

pub(crate) async fn save_config<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> EcstoreResult<()>
where
    S: ScannerObjectIO,
{
    ecstore_config::com::save_config(api, file, data).await
}

pub(crate) async fn list_path_raw(rx: CancellationToken, opts: ListPathRawOptions) -> std::result::Result<(), DiskError> {
    ecstore_cache::list_path_raw(rx, opts).await
}

pub(crate) async fn replace_bucket_usage_memory_from_info(data_usage_info: &rustfs_data_usage::DataUsageInfo) {
    ecstore_data_usage::replace_bucket_usage_memory_from_info(data_usage_info).await;
}

pub trait ScannerObjectIO:
    ObjectIO<
        Error = EcstoreError,
        RangeSpec = HTTPRangeSpec,
        HeaderMap = HeaderMap,
        ObjectOptions = ScannerObjectOptions,
        ObjectInfo = ScannerObjectInfo,
        GetObjectReader = ScannerGetObjectReader,
        PutObjectReader = ScannerPutObjReader,
    >
{
}

impl<T> ScannerObjectIO for T where
    T: ObjectIO<
            Error = EcstoreError,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ScannerObjectOptions,
            ObjectInfo = ScannerObjectInfo,
            GetObjectReader = ScannerGetObjectReader,
            PutObjectReader = ScannerPutObjReader,
        >
{
}
