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

pub(crate) use rustfs_ecstore::api::admin::get_local_server_property;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::bucket::metadata::BucketMetadata;
pub(crate) use rustfs_ecstore::api::bucket::metadata::{
    BUCKET_ACCELERATE_CONFIG, BUCKET_LOGGING_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_VERSIONING_CONFIG,
    BUCKET_WEBSITE_CONFIG, OBJECT_LOCK_CONFIG, load_bucket_metadata,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::{
    GLOBAL_BucketMetadataSys as GLOBAL_BUCKET_METADATA_SYS, get_global_bucket_metadata_sys,
};
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::{
    delete as delete_bucket_metadata_config, get as get_bucket_metadata, get_accelerate_config as get_bucket_accelerate_config,
    get_bucket_policy_raw, get_cors_config as get_bucket_cors_config, get_logging_config as get_bucket_logging_config,
    get_object_lock_config as get_bucket_object_lock_config, get_public_access_block_config,
    get_replication_config as get_bucket_replication_config, get_request_payment_config as get_bucket_request_payment_config,
    get_sse_config as get_bucket_sse_config, get_website_config as get_bucket_website_config, set_bucket_metadata,
    update as update_bucket_metadata_config,
};
pub(crate) use rustfs_ecstore::api::bucket::object_lock::objectlock_sys::{
    add_years as add_object_lock_years, check_retention_for_modification,
};
pub(crate) use rustfs_ecstore::api::bucket::policy_sys::PolicySys;
pub(crate) use rustfs_ecstore::api::bucket::replication::{GLOBAL_REPLICATION_STATS, ReplicationConfigurationExt};
pub(crate) use rustfs_ecstore::api::bucket::tagging::{decode_tags, decode_tags_to_map, encode_tags};
pub(crate) use rustfs_ecstore::api::bucket::utils::serialize;
pub(crate) use rustfs_ecstore::api::bucket::versioning::VersioningApi;
pub(crate) use rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys;
pub(crate) use rustfs_ecstore::api::client::object_api_utils::to_s3s_etag;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::config::com::STORAGE_CLASS_SUB_SYS;
pub(crate) use rustfs_ecstore::api::disk::error::DiskError;
pub(crate) use rustfs_ecstore::api::disk::{
    DeleteOptions, DiskAPI, DiskInfoOptions, DiskStore, FileInfoVersions, ReadMultipleReq, ReadMultipleResp, ReadOptions,
    UpdateMetadataOpts, WalkDirOptions,
};
pub(crate) use rustfs_ecstore::api::error::{
    Error, Result, StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found,
};
pub(crate) use rustfs_ecstore::api::global::{
    GLOBAL_TierConfigMgr, get_global_lock_client, get_global_region, resolve_object_store_handle,
};
pub(crate) use rustfs_ecstore::api::metrics::{CollectMetricsOpts, MetricType, collect_local_metrics};
pub(crate) use rustfs_ecstore::api::rio::WriteEncryption;
pub(crate) use rustfs_ecstore::api::rpc::{
    LocalPeerS3Client, PEER_RESTSIGNAL, PEER_RESTSUB_SYS, PeerS3Client, SERVICE_SIGNAL_REFRESH_CONFIG,
    SERVICE_SIGNAL_RELOAD_DYNAMIC, verify_rpc_signature,
};
pub(crate) use rustfs_ecstore::api::set_disk::DEFAULT_READ_BUFFER_SIZE;
pub(crate) use rustfs_ecstore::api::storage::{ECStore, all_local_disk_path, find_local_disk_by_ref};

pub(crate) type GetObjectReader = <ECStore as rustfs_storage_api::ObjectIO>::GetObjectReader;
pub(crate) type ObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub(crate) type ObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub(crate) type PutObjReader = <ECStore as rustfs_storage_api::ObjectIO>::PutObjectReader;
