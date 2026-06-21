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

use std::sync::Arc;

#[cfg(test)]
use rustfs_ecstore::api::config as ecstore_config;
use rustfs_ecstore::api::{
    admin as ecstore_admin, bucket as ecstore_bucket, client as ecstore_client, disk as ecstore_disk, error as ecstore_error,
    global as ecstore_global, metrics as ecstore_metrics, rio as ecstore_rio, rpc as ecstore_rpc, set_disk as ecstore_set_disk,
    storage as ecstore_storage,
};

pub(crate) const BUCKET_ACCELERATE_CONFIG: &str = ecstore_bucket::metadata::BUCKET_ACCELERATE_CONFIG;
pub(crate) const BUCKET_LOGGING_CONFIG: &str = ecstore_bucket::metadata::BUCKET_LOGGING_CONFIG;
pub(crate) const BUCKET_REQUEST_PAYMENT_CONFIG: &str = ecstore_bucket::metadata::BUCKET_REQUEST_PAYMENT_CONFIG;
pub(crate) const BUCKET_VERSIONING_CONFIG: &str = ecstore_bucket::metadata::BUCKET_VERSIONING_CONFIG;
pub(crate) const BUCKET_WEBSITE_CONFIG: &str = ecstore_bucket::metadata::BUCKET_WEBSITE_CONFIG;
pub(crate) const DEFAULT_READ_BUFFER_SIZE: usize = ecstore_set_disk::DEFAULT_READ_BUFFER_SIZE;
pub(crate) const OBJECT_LOCK_CONFIG: &str = ecstore_bucket::metadata::OBJECT_LOCK_CONFIG;
pub(crate) const PEER_RESTSIGNAL: &str = ecstore_rpc::PEER_RESTSIGNAL;
pub(crate) const PEER_RESTSUB_SYS: &str = ecstore_rpc::PEER_RESTSUB_SYS;
pub(crate) const SERVICE_SIGNAL_REFRESH_CONFIG: u64 = ecstore_rpc::SERVICE_SIGNAL_REFRESH_CONFIG;
pub(crate) const SERVICE_SIGNAL_RELOAD_DYNAMIC: u64 = ecstore_rpc::SERVICE_SIGNAL_RELOAD_DYNAMIC;
#[cfg(test)]
pub(crate) const STORAGE_CLASS_SUB_SYS: &str = ecstore_config::com::STORAGE_CLASS_SUB_SYS;

pub(crate) type BucketMetadata = ecstore_bucket::metadata::BucketMetadata;
#[cfg(test)]
pub(crate) type BucketMetadataSys = ecstore_bucket::metadata_sys::BucketMetadataSys;
pub(crate) type BucketVersioningSys = ecstore_bucket::versioning_sys::BucketVersioningSys;
pub(crate) type CollectMetricsOpts = ecstore_metrics::CollectMetricsOpts;
pub(crate) type DeleteOptions = ecstore_disk::DeleteOptions;
pub(crate) type DiskError = ecstore_disk::error::DiskError;
pub(crate) type DiskInfoOptions = ecstore_disk::DiskInfoOptions;
pub(crate) type DiskStore = ecstore_disk::DiskStore;
pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type FileInfoVersions = ecstore_disk::FileInfoVersions;
pub(crate) type LocalPeerS3Client = ecstore_rpc::LocalPeerS3Client;
pub(crate) type MetricType = ecstore_metrics::MetricType;
pub(crate) type ObjectLockBlockReason = ecstore_bucket::object_lock::objectlock_sys::ObjectLockBlockReason;
pub(crate) type PolicySys = ecstore_bucket::policy_sys::PolicySys;
pub(crate) type ReadMultipleReq = ecstore_disk::ReadMultipleReq;
pub(crate) type ReadMultipleResp = ecstore_disk::ReadMultipleResp;
pub(crate) type ReadOptions = ecstore_disk::ReadOptions;
pub(crate) type StorageError = ecstore_error::StorageError;
pub(crate) type Error = StorageError;
pub(crate) type Result<T> = core::result::Result<T, Error>;
pub(crate) type UpdateMetadataOpts = ecstore_disk::UpdateMetadataOpts;
pub(crate) type WalkDirOptions = ecstore_disk::WalkDirOptions;
pub(crate) type WriteEncryption = ecstore_rio::WriteEncryption;

pub(crate) async fn get_local_server_property() -> rustfs_madmin::ServerProperties {
    ecstore_admin::get_local_server_property().await
}

pub(crate) async fn load_bucket_metadata(api: Arc<ECStore>, bucket: &str) -> Result<BucketMetadata> {
    ecstore_bucket::metadata::load_bucket_metadata(api, bucket).await
}

#[cfg(test)]
pub(crate) fn bucket_metadata_sys_initialized() -> bool {
    ecstore_bucket::metadata_sys::GLOBAL_BucketMetadataSys.get().is_some()
}

#[cfg(test)]
pub(crate) fn get_global_bucket_metadata_sys() -> Option<Arc<tokio::sync::RwLock<BucketMetadataSys>>> {
    ecstore_bucket::metadata_sys::get_global_bucket_metadata_sys()
}

pub(crate) async fn delete_bucket_metadata_config(bucket: &str, config_file: &str) -> Result<time::OffsetDateTime> {
    ecstore_bucket::metadata_sys::delete(bucket, config_file).await
}

pub(crate) async fn get_bucket_metadata(bucket: &str) -> Result<Arc<BucketMetadata>> {
    ecstore_bucket::metadata_sys::get(bucket).await
}

pub(crate) async fn get_bucket_accelerate_config(
    bucket: &str,
) -> Result<(s3s::dto::AccelerateConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_accelerate_config(bucket).await
}

pub(crate) async fn get_bucket_policy_raw(bucket: &str) -> Result<(String, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_bucket_policy_raw(bucket).await
}

pub(crate) async fn get_bucket_cors_config(bucket: &str) -> Result<(s3s::dto::CORSConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_cors_config(bucket).await
}

pub(crate) async fn get_bucket_logging_config(bucket: &str) -> Result<(s3s::dto::BucketLoggingStatus, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_logging_config(bucket).await
}

pub(crate) async fn get_bucket_object_lock_config(
    bucket: &str,
) -> Result<(s3s::dto::ObjectLockConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_object_lock_config(bucket).await
}

pub(crate) async fn get_public_access_block_config(
    bucket: &str,
) -> Result<(s3s::dto::PublicAccessBlockConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_public_access_block_config(bucket).await
}

pub(crate) async fn get_bucket_replication_config(
    bucket: &str,
) -> Result<(s3s::dto::ReplicationConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_replication_config(bucket).await
}

pub(crate) async fn get_bucket_request_payment_config(
    bucket: &str,
) -> Result<(s3s::dto::RequestPaymentConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_request_payment_config(bucket).await
}

pub(crate) async fn get_bucket_sse_config(
    bucket: &str,
) -> Result<(s3s::dto::ServerSideEncryptionConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_sse_config(bucket).await
}

pub(crate) async fn get_bucket_website_config(bucket: &str) -> Result<(s3s::dto::WebsiteConfiguration, time::OffsetDateTime)> {
    ecstore_bucket::metadata_sys::get_website_config(bucket).await
}

pub(crate) async fn set_bucket_metadata(bucket: String, bm: BucketMetadata) -> Result<()> {
    ecstore_bucket::metadata_sys::set_bucket_metadata(bucket, bm).await
}

pub(crate) async fn update_bucket_metadata_config(
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
) -> Result<time::OffsetDateTime> {
    ecstore_bucket::metadata_sys::update(bucket, config_file, data).await
}

pub(crate) fn add_object_lock_years(dt: time::OffsetDateTime, years: i32) -> time::OffsetDateTime {
    ecstore_bucket::object_lock::objectlock_sys::add_years(dt, years)
}

pub(crate) fn check_retention_for_modification(
    user_defined: &std::collections::HashMap<String, String>,
    new_mode: Option<&str>,
    new_retain_until: Option<time::OffsetDateTime>,
    bypass_governance: bool,
) -> Option<ObjectLockBlockReason> {
    ecstore_bucket::object_lock::objectlock_sys::check_retention_for_modification(
        user_defined,
        new_mode,
        new_retain_until,
        bypass_governance,
    )
}

pub(crate) async fn record_replication_proxy(bucket: &str, api: &str, is_err: bool) {
    if let Some(stats) = ecstore_bucket::replication::GLOBAL_REPLICATION_STATS.get() {
        stats.inc_proxy(bucket, api, is_err).await;
    }
}

pub(crate) fn decode_tags(tags: &str) -> Vec<s3s::dto::Tag> {
    ecstore_bucket::tagging::decode_tags(tags)
}

pub(crate) fn decode_tags_to_map(tags: &str) -> std::collections::HashMap<String, String> {
    ecstore_bucket::tagging::decode_tags_to_map(tags)
}

pub(crate) fn encode_tags(tags: Vec<s3s::dto::Tag>) -> String {
    ecstore_bucket::tagging::encode_tags(tags)
}

pub(crate) fn serialize<T: s3s::xml::Serialize>(val: &T) -> s3s::xml::SerResult<Vec<u8>> {
    ecstore_bucket::utils::serialize(val)
}

pub(crate) fn to_s3s_etag(etag: &str) -> s3s::dto::ETag {
    ecstore_client::object_api_utils::to_s3s_etag(etag)
}

pub(crate) fn is_err_bucket_not_found(err: &Error) -> bool {
    ecstore_error::is_err_bucket_not_found(err)
}

pub(crate) fn is_err_object_not_found(err: &Error) -> bool {
    ecstore_error::is_err_object_not_found(err)
}

pub(crate) fn is_err_version_not_found(err: &Error) -> bool {
    ecstore_error::is_err_version_not_found(err)
}

pub(crate) fn get_global_lock_client() -> Option<Arc<dyn rustfs_lock::client::LockClient>> {
    ecstore_global::get_global_lock_client()
}

pub(crate) fn get_global_region() -> Option<s3s::region::Region> {
    ecstore_global::get_global_region()
}

pub(crate) fn resolve_object_store_handle() -> Option<Arc<ECStore>> {
    ecstore_global::resolve_object_store_handle()
}

pub(crate) async fn collect_local_metrics(
    types: MetricType,
    opts: &CollectMetricsOpts,
) -> rustfs_madmin::metrics::RealtimeMetrics {
    ecstore_metrics::collect_local_metrics(types, opts).await
}

pub(crate) fn verify_rpc_signature(url: &str, method: &http::Method, headers: &http::HeaderMap) -> std::io::Result<()> {
    ecstore_rpc::verify_rpc_signature(url, method, headers)
}

pub(crate) async fn reload_transition_tier_config(api: Arc<ECStore>) -> std::io::Result<()> {
    ecstore_global::GLOBAL_TierConfigMgr.write().await.reload(api).await
}

pub(crate) async fn all_local_disk_path() -> Vec<String> {
    ecstore_storage::all_local_disk_path().await
}

pub(crate) async fn find_local_disk_by_ref(disk_ref: &str) -> Option<DiskStore> {
    ecstore_storage::find_local_disk_by_ref(disk_ref).await
}

pub(crate) type GetObjectReader = <ECStore as rustfs_storage_api::ObjectIO>::GetObjectReader;
pub(crate) type ObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub(crate) type ObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub(crate) type PutObjReader = <ECStore as rustfs_storage_api::ObjectIO>::PutObjectReader;
