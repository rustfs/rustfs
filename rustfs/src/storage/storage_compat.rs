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
pub(crate) use rustfs_ecstore::api::bucket::{
    metadata, metadata_sys, object_lock, policy_sys, replication, tagging, utils, versioning, versioning_sys,
};
pub(crate) use rustfs_ecstore::api::client::object_api_utils;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::config::com;
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
