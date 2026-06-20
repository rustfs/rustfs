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

pub(crate) use rustfs_ecstore::api::admin::get_server_info;
pub(crate) use rustfs_ecstore::api::capacity::{
    PoolDecommissionInfo, PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free,
};
pub(crate) use rustfs_ecstore::api::layout::EndpointServerPools;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::layout::{Endpoints, PoolEndpoints};
pub(crate) use rustfs_ecstore::api::notification::get_global_notification_sys;
pub(crate) use rustfs_ecstore::api::storage::ECStore;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::storage::init_local_disks;
pub(crate) use rustfs_ecstore::bucket::{
    bucket_target_sys, lifecycle, metadata, metadata_sys, object_lock, policy_sys, quota, replication, tagging, target, utils,
    versioning, versioning_sys,
};
pub(crate) use rustfs_ecstore::client::object_api_utils;
#[cfg(test)]
pub(crate) use rustfs_ecstore::client::transition_api;
pub(crate) use rustfs_ecstore::compress::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
pub(crate) use rustfs_ecstore::config::storageclass;
pub(crate) use rustfs_ecstore::data_usage::{
    apply_bucket_usage_memory_overlay, load_data_usage_from_backend, record_bucket_object_delete_memory,
    record_bucket_object_write_memory,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::disk::endpoint::Endpoint;
pub(crate) use rustfs_ecstore::disk::error::DiskError;
pub(crate) use rustfs_ecstore::disk::error_reduce::is_all_buckets_not_found;
pub(crate) use rustfs_ecstore::error::{
    Error, StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::global::GLOBAL_TierConfigMgr;
pub(crate) use rustfs_ecstore::global::{
    get_global_endpoints_opt, get_global_region, get_global_tier_config_mgr, new_object_layer_fn, set_object_store_resolver,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::rio::{DecryptReader, EncryptReader, HardLimitReader, boxed_reader};
pub(crate) use rustfs_ecstore::rio::{
    DynReader, HashReader, WriteEncryption, WritePlan, compression_metadata_value, wrap_reader,
};
pub(crate) use rustfs_ecstore::set_disk::{get_lock_acquire_timeout, is_valid_storage_class};
pub(crate) use rustfs_ecstore::tier::tier::TierConfigMgr;
#[cfg(test)]
pub(crate) use rustfs_ecstore::tier::tier_config::{TierConfig, TierType};
#[cfg(test)]
pub(crate) use rustfs_ecstore::tier::warm_backend::{WarmBackend, WarmBackendGetOpts};
