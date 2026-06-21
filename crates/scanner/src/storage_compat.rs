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
use rustfs_storage_api::{HTTPRangeSpec, ObjectIO, ObjectToDelete};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) const BUCKET_META_PREFIX: &str = rustfs_ecstore::api::disk::BUCKET_META_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = rustfs_ecstore::api::disk::RUSTFS_META_BUCKET;
pub(crate) const STORAGE_FORMAT_FILE: &str = rustfs_ecstore::api::disk::STORAGE_FORMAT_FILE;
pub(crate) const TRANSITION_COMPLETE: &str = rustfs_ecstore::api::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;

pub(crate) type Disk = rustfs_ecstore::api::disk::Disk;
#[cfg(test)]
pub(crate) type DiskStore = rustfs_ecstore::api::disk::DiskStore;
pub(crate) type DiskError = rustfs_ecstore::api::disk::error::DiskError;
pub(crate) type ECStore = rustfs_ecstore::api::storage::ECStore;
pub(crate) type EcstoreError = rustfs_ecstore::api::error::Error;
pub(crate) type EcstoreResult<T> = rustfs_ecstore::api::error::Result<T>;
pub(crate) type ListPathRawOptions = rustfs_ecstore::api::cache::ListPathRawOptions;
pub(crate) type BucketTargetSys = rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys;
pub(crate) type BucketVersioningSys = rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys;
pub(crate) type DiskInfoOptions = rustfs_ecstore::api::disk::DiskInfoOptions;
pub(crate) type Evaluator = rustfs_ecstore::api::bucket::lifecycle::evaluator::Evaluator;
pub(crate) type Event = rustfs_ecstore::api::bucket::lifecycle::lifecycle::Event;
pub(crate) type LcEventSrc = rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
pub(crate) type ObjectOpts = rustfs_ecstore::api::bucket::lifecycle::lifecycle::ObjectOpts;
pub(crate) type ReplicationConfig = rustfs_ecstore::api::bucket::replication::ReplicationConfig;
pub(crate) type ReplicationHealQueueResult = rustfs_ecstore::api::bucket::replication::ReplicationHealQueueResult;
pub(crate) type ReplicationQueueAdmission = rustfs_ecstore::api::bucket::replication::ReplicationQueueAdmission;
pub(crate) type SetDisks = rustfs_ecstore::api::set_disk::SetDisks;
pub(crate) type StorageError = rustfs_ecstore::api::error::StorageError;

pub type ScannerGetObjectReader = <ECStore as ObjectIO>::GetObjectReader;
pub type ScannerObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub type ScannerObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub type ScannerObjectToDelete = ObjectToDelete;
pub type ScannerPutObjReader = <ECStore as ObjectIO>::PutObjectReader;

pub(crate) mod storageclass {
    pub(crate) const RRS: &str = rustfs_ecstore::api::config::storageclass::RRS;
    pub(crate) const STANDARD: &str = rustfs_ecstore::api::config::storageclass::STANDARD;
}

#[cfg(test)]
pub(crate) fn init_ecstore_config_for_scanner_tests() {
    rustfs_ecstore::api::config::init();
}

#[cfg(test)]
pub(crate) type DiskOption = rustfs_ecstore::api::disk::DiskOption;
#[cfg(test)]
pub(crate) type Endpoint = rustfs_ecstore::api::disk::endpoint::Endpoint;

#[cfg(test)]
pub(crate) async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> rustfs_ecstore::api::disk::error::Result<DiskStore> {
    rustfs_ecstore::api::disk::new_disk(ep, opt).await
}

pub(crate) async fn get_lifecycle_config(
    bucket: &str,
) -> EcstoreResult<(s3s::dto::BucketLifecycleConfiguration, time::OffsetDateTime)> {
    rustfs_ecstore::api::bucket::metadata_sys::get_lifecycle_config(bucket).await
}

pub(crate) async fn get_object_lock_config(
    bucket: &str,
) -> EcstoreResult<(s3s::dto::ObjectLockConfiguration, time::OffsetDateTime)> {
    rustfs_ecstore::api::bucket::metadata_sys::get_object_lock_config(bucket).await
}

pub(crate) async fn get_replication_config(
    bucket: &str,
) -> EcstoreResult<(s3s::dto::ReplicationConfiguration, time::OffsetDateTime)> {
    rustfs_ecstore::api::bucket::metadata_sys::get_replication_config(bucket).await
}

pub(crate) async fn apply_transition_rule(event: &Event, src: &LcEventSrc, oi: &ScannerObjectInfo) -> bool {
    rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::apply_transition_rule(event, src, oi).await
}

pub(crate) async fn apply_expiry_rule(event: &Event, src: &LcEventSrc, oi: &ScannerObjectInfo) -> bool {
    rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::apply_expiry_rule(event, src, oi).await
}

pub(crate) async fn list_global_tiers() -> Vec<rustfs_ecstore::api::tier::tier_config::TierConfig> {
    rustfs_ecstore::api::global::GLOBAL_TierConfigMgr.read().await.list_tiers()
}

pub(crate) async fn enqueue_global_free_version(oi: ScannerObjectInfo) {
    rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
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
    rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
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
    rustfs_ecstore::api::bucket::replication::queue_replication_heal_internal(bucket, oi, rcfg, retry_count).await
}

pub(crate) fn resolve_scanner_object_store_handle() -> Option<Arc<ECStore>> {
    rustfs_ecstore::api::global::resolve_object_store_handle()
}

pub(crate) fn is_reserved_or_invalid_bucket(bucket: &str, strict: bool) -> bool {
    rustfs_ecstore::api::capacity::is_reserved_or_invalid_bucket(bucket, strict)
}

pub(crate) fn path2_bucket_object(name: &str) -> (String, String) {
    rustfs_ecstore::api::capacity::path2_bucket_object(name)
}

pub(crate) fn path2_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    rustfs_ecstore::api::capacity::path2_bucket_object_with_base_path(base_path, path)
}

pub(crate) async fn is_erasure() -> bool {
    rustfs_ecstore::api::global::is_erasure().await
}

pub(crate) async fn is_erasure_sd() -> bool {
    rustfs_ecstore::api::global::is_erasure_sd().await
}

pub(crate) async fn read_config<S>(api: Arc<S>, file: &str) -> EcstoreResult<Vec<u8>>
where
    S: ScannerObjectIO,
{
    rustfs_ecstore::api::config::com::read_config(api, file).await
}

pub(crate) async fn save_config<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> EcstoreResult<()>
where
    S: ScannerObjectIO,
{
    rustfs_ecstore::api::config::com::save_config(api, file, data).await
}

pub(crate) async fn list_path_raw(rx: CancellationToken, opts: ListPathRawOptions) -> std::result::Result<(), DiskError> {
    rustfs_ecstore::api::cache::list_path_raw(rx, opts).await
}

pub(crate) async fn replace_bucket_usage_memory_from_info(data_usage_info: &rustfs_data_usage::DataUsageInfo) {
    rustfs_ecstore::api::data_usage::replace_bucket_usage_memory_from_info(data_usage_info).await;
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
