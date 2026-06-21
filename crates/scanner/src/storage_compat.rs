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
pub(crate) type DiskError = rustfs_ecstore::api::disk::error::DiskError;
pub(crate) type ECStore = rustfs_ecstore::api::storage::ECStore;
pub(crate) type EcstoreError = rustfs_ecstore::api::error::Error;
pub(crate) type EcstoreResult<T> = rustfs_ecstore::api::error::Result<T>;
pub(crate) type ListPathRawOptions = rustfs_ecstore::api::cache::ListPathRawOptions;
pub(crate) type SetDisks = rustfs_ecstore::api::set_disk::SetDisks;
pub(crate) type StorageError = rustfs_ecstore::api::error::StorageError;

pub(crate) use rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys;
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::{
    GLOBAL_ExpiryState, apply_expiry_rule, apply_transition_rule,
};
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::evaluator::Evaluator;
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::lifecycle::{Event, Lifecycle, ObjectOpts};
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::{
    get_lifecycle_config, get_object_lock_config, get_replication_config,
};
pub(crate) use rustfs_ecstore::api::bucket::replication::{
    ReplicationConfig, ReplicationConfigurationExt, ReplicationQueueAdmission, queue_replication_heal_internal,
};
pub(crate) use rustfs_ecstore::api::bucket::versioning::VersioningApi;
pub(crate) use rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys;
pub(crate) use rustfs_ecstore::api::disk::{DiskAPI, DiskInfoOptions};
pub(crate) use rustfs_ecstore::api::global::GLOBAL_TierConfigMgr;

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
pub(crate) use rustfs_ecstore::api::config::init as init_ecstore_config_for_scanner_tests;

#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::{DiskOption, endpoint::Endpoint, new_disk};

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
