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

pub(crate) mod ecstore {
    pub(crate) use rustfs_ecstore::global::{get_global_lock_client, resolve_object_store_handle};

    pub(crate) mod admin_server_info {
        pub(crate) use rustfs_ecstore::admin_server_info::get_local_server_property;
    }

    pub(crate) mod bucket {
        pub(crate) use rustfs_ecstore::bucket::{
            metadata, metadata_sys, object_lock, policy_sys, replication, tagging, utils, versioning, versioning_sys,
        };
    }

    pub(crate) mod client {
        pub(crate) use rustfs_ecstore::client::object_api_utils;
    }

    #[cfg(test)]
    pub(crate) mod config {
        pub(crate) use rustfs_ecstore::config::com;
    }

    pub(crate) mod disk {
        pub(crate) use rustfs_ecstore::disk::{
            DeleteOptions, DiskAPI, DiskInfoOptions, DiskStore, FileInfoVersions, ReadMultipleReq, ReadMultipleResp, ReadOptions,
            UpdateMetadataOpts, WalkDirOptions, error,
        };
    }

    pub(crate) mod error {
        pub(crate) use rustfs_ecstore::error::{
            Error, Result, StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found,
        };
    }

    pub(crate) mod global {
        pub(crate) use rustfs_ecstore::global::{GLOBAL_TierConfigMgr, get_global_region};
    }

    pub(crate) mod metrics_realtime {
        pub(crate) use rustfs_ecstore::metrics_realtime::{CollectMetricsOpts, MetricType, collect_local_metrics};
    }

    pub(crate) mod rio {
        pub(crate) use rustfs_ecstore::rio::WriteEncryption;
    }

    pub(crate) mod rpc {
        pub(crate) use rustfs_ecstore::rpc::{
            LocalPeerS3Client, PEER_RESTSIGNAL, PEER_RESTSUB_SYS, PeerS3Client, SERVICE_SIGNAL_REFRESH_CONFIG,
            SERVICE_SIGNAL_RELOAD_DYNAMIC, verify_rpc_signature,
        };
    }

    pub(crate) mod set_disk {
        pub(crate) use rustfs_ecstore::set_disk::DEFAULT_READ_BUFFER_SIZE;
    }

    pub(crate) mod store {
        pub(crate) use rustfs_ecstore::store::{ECStore, all_local_disk_path, find_local_disk_by_ref};
    }

    pub(crate) type GetObjectReader = rustfs_ecstore::store_api::GetObjectReader;
    pub(crate) type ObjectInfo = rustfs_ecstore::store_api::ObjectInfo;
    pub(crate) type ObjectOptions = rustfs_ecstore::store_api::ObjectOptions;
    pub(crate) type PutObjReader = rustfs_ecstore::store_api::PutObjReader;
}
