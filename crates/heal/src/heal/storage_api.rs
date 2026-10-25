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

pub(crate) use rustfs_ecstore::api::capacity::is_reserved_or_invalid_bucket as ecstore_is_reserved_or_invalid_bucket;
pub(crate) use rustfs_ecstore::api::data_usage::{
    DATA_USAGE_CACHE_NAME as ECSTORE_DATA_USAGE_CACHE_NAME,
    load_admin_data_usage_from_backend_cached as ecstore_load_admin_data_usage_from_backend_cached,
};
pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint as EcstoreEndpoint;
pub(crate) use rustfs_ecstore::api::disk::error::{DiskError as EcstoreDiskError, Result as EcstoreDiskResult};
pub(crate) use rustfs_ecstore::api::disk::{
    BUCKET_META_PREFIX as ECSTORE_BUCKET_META_PREFIX, Bytes as EcstoreDiskBytes,
    ConditionalFileUpdate as EcstoreConditionalFileUpdate, DeleteOptions as EcstoreDeleteOptions, DiskAPI as EcstoreDiskAPI,
    DiskStore as EcstoreDiskStore, HEALING_MARKER_PATH as ECSTORE_HEALING_MARKER_PATH,
    RUSTFS_META_BUCKET as ECSTORE_RUSTFS_META_BUCKET, ReplacementExecutionLease as EcstoreReplacementExecutionLease,
};
pub(crate) use rustfs_ecstore::api::disk::{DiskOption as EcstoreDiskOption, new_disk as ecstore_new_disk};
pub(crate) use rustfs_ecstore::api::error::{Error as EcstoreErrorType, StorageError as EcstoreStorageError};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::layout::{
    EndpointServerPools as EcstoreEndpointServerPools, Endpoints as EcstoreEndpoints, PoolEndpoints as EcstorePoolEndpoints,
};
pub(crate) use rustfs_ecstore::api::runtime::local_disk_map_read as ecstore_local_disk_map_read;
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::storage::init_local_disks as ecstore_init_local_disks;
pub(crate) use rustfs_ecstore::api::storage::{
    ECStore as EcstoreStore, HealLifecycleExpiryContext as EcstoreHealLifecycleExpiryContext,
    HealObjectStorageResult as EcstoreHealObjectStorageResult, POOL_META_NAME as ECSTORE_POOL_META_NAME,
};
use rustfs_storage_api as storage_contracts;

pub(crate) mod owner {
    pub(crate) use rustfs_ecstore::api::disk::PhysicalResourcePlan;
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::api::disk::{PhysicalDiskBinding, PhysicalDiskSample, PhysicalDiskSampler};
    #[cfg(test)]
    pub(crate) use rustfs_ecstore::spawn_blocking_with_resource_ownership as ecstore_spawn_blocking_with_resource_ownership;
    pub(crate) use rustfs_ecstore::with_background_resource_set as ecstore_with_background_resource_set;

    pub(crate) use super::storage_contracts::{ObjectIO, ObjectOperations};

    pub(crate) use super::{
        ECSTORE_BUCKET_META_PREFIX, ECSTORE_DATA_USAGE_CACHE_NAME, ECSTORE_HEALING_MARKER_PATH, ECSTORE_POOL_META_NAME,
        ECSTORE_RUSTFS_META_BUCKET, EcstoreConditionalFileUpdate, EcstoreDeleteOptions, EcstoreDiskAPI, EcstoreDiskBytes,
        EcstoreDiskError, EcstoreDiskResult, EcstoreDiskStore, EcstoreEndpoint, EcstoreErrorType,
        EcstoreHealLifecycleExpiryContext, EcstoreStorageError, EcstoreStore, ecstore_is_reserved_or_invalid_bucket,
        ecstore_load_admin_data_usage_from_backend_cached, ecstore_local_disk_map_read,
    };

    pub(crate) use super::{EcstoreDiskOption, ecstore_new_disk};
}

pub(crate) mod storage {
    pub(crate) use super::EcstoreHealObjectStorageResult;
    pub(crate) use super::storage_contracts::{
        BucketInfo, BucketOperations, DiskSetSelector, HealOperations, ListOperations, ObjectIO, ObjectOperations,
        StorageAdminApi,
    };

    #[cfg(test)]
    pub(crate) use super::storage_contracts::DeleteBucketOptions;
}

#[cfg(test)]
pub(crate) mod status {
    pub(crate) use super::storage_contracts::BucketInfo;
}
