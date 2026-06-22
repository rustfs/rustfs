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

pub(crate) use rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys as EcstoreBucketTargetSys;
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc as EcstoreLcEventSrc;
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::{
    GLOBAL_ExpiryState as ECSTORE_GLOBAL_EXPIRY_STATE, apply_expiry_rule as ecstore_apply_expiry_rule,
    apply_transition_rule as ecstore_apply_transition_rule,
};
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::evaluator::Evaluator as EcstoreEvaluator;
pub(crate) use rustfs_ecstore::api::bucket::lifecycle::lifecycle::{
    Event as EcstoreEvent, Lifecycle as EcstoreLifecycle, ObjectOpts as EcstoreObjectOpts,
    TRANSITION_COMPLETE as ECSTORE_TRANSITION_COMPLETE,
};
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::{
    get_lifecycle_config as ecstore_get_lifecycle_config, get_object_lock_config as ecstore_get_object_lock_config,
    get_replication_config as ecstore_get_replication_config,
};
pub(crate) use rustfs_ecstore::api::bucket::replication::{
    ReplicationConfig as EcstoreReplicationConfig, ReplicationConfigurationExt as EcstoreReplicationConfigurationExt,
    ReplicationHealQueueResult as EcstoreReplicationHealQueueResult,
    ReplicationQueueAdmission as EcstoreReplicationQueueAdmission,
    queue_replication_heal_internal as ecstore_queue_replication_heal_internal,
};
pub(crate) use rustfs_ecstore::api::bucket::versioning::VersioningApi as EcstoreVersioningApi;
pub(crate) use rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys as EcstoreBucketVersioningSys;
pub(crate) use rustfs_ecstore::api::cache::{
    ListPathRawOptions as EcstoreListPathRawOptions, list_path_raw as ecstore_list_path_raw,
};
pub(crate) use rustfs_ecstore::api::capacity::{
    is_reserved_or_invalid_bucket as ecstore_is_reserved_or_invalid_bucket, path2_bucket_object as ecstore_path2_bucket_object,
    path2_bucket_object_with_base_path as ecstore_path2_bucket_object_with_base_path,
};
pub(crate) use rustfs_ecstore::api::config::com::{read_config as ecstore_read_config, save_config as ecstore_save_config};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::config::init as ecstore_config_init;
pub(crate) use rustfs_ecstore::api::config::storageclass::{
    RRS as ECSTORE_STORAGECLASS_RRS, STANDARD as ECSTORE_STORAGECLASS_STANDARD,
};
pub(crate) use rustfs_ecstore::api::data_usage::replace_bucket_usage_memory_from_info as ecstore_replace_bucket_usage_memory_from_info;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint as EcstoreEndpoint;
pub(crate) use rustfs_ecstore::api::disk::error::{DiskError as EcstoreDiskError, Result as EcstoreDiskResult};
pub(crate) use rustfs_ecstore::api::disk::{
    BUCKET_META_PREFIX as ECSTORE_BUCKET_META_PREFIX, Bytes as EcstoreDiskBytes, Disk as EcstoreDisk, DiskAPI as EcstoreDiskAPI,
    DiskInfo as EcstoreDiskInfo, DiskInfoOptions as EcstoreDiskInfoOptions, DiskLocation as EcstoreDiskLocation,
    RUSTFS_META_BUCKET as ECSTORE_RUSTFS_META_BUCKET, STORAGE_FORMAT_FILE as ECSTORE_STORAGE_FORMAT_FILE,
    ScanGuard as EcstoreScanGuard,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::{
    DiskOption as EcstoreDiskOption, DiskStore as EcstoreDiskStore, new_disk as ecstore_new_disk,
};
pub(crate) use rustfs_ecstore::api::error::{
    Error as EcstoreErrorType, Result as EcstoreResultType, StorageError as EcstoreStorageError,
};
pub(crate) use rustfs_ecstore::api::global::{
    GLOBAL_TierConfigMgr as ECSTORE_GLOBAL_TIER_CONFIG_MGR, is_erasure as ecstore_is_erasure,
    is_erasure_sd as ecstore_is_erasure_sd, resolve_object_store_handle as ecstore_resolve_object_store_handle,
};
pub(crate) use rustfs_ecstore::api::set_disk::SetDisks as EcstoreSetDisks;
pub(crate) use rustfs_ecstore::api::storage::ECStore as EcstoreStore;
pub(crate) use rustfs_ecstore::api::tier::tier_config::TierConfig as EcstoreTierConfig;
