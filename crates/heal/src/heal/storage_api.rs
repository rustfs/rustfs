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

pub(crate) use rustfs_ecstore::api::data_usage::DATA_USAGE_CACHE_NAME as ECSTORE_DATA_USAGE_CACHE_NAME;
pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint as EcstoreEndpoint;
pub(crate) use rustfs_ecstore::api::disk::error::{DiskError as EcstoreDiskError, Result as EcstoreDiskResult};
pub(crate) use rustfs_ecstore::api::disk::{
    BUCKET_META_PREFIX as ECSTORE_BUCKET_META_PREFIX, Bytes as EcstoreDiskBytes, DeleteOptions as EcstoreDeleteOptions,
    DiskAPI as EcstoreDiskAPI, DiskStore as EcstoreDiskStore, RUSTFS_META_BUCKET as ECSTORE_RUSTFS_META_BUCKET,
};
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::disk::{DiskOption as EcstoreDiskOption, new_disk as ecstore_new_disk};
pub(crate) use rustfs_ecstore::api::error::{Error as EcstoreErrorType, StorageError as EcstoreStorageError};
pub(crate) use rustfs_ecstore::api::global::GLOBAL_LOCAL_DISK_MAP as ECSTORE_GLOBAL_LOCAL_DISK_MAP;
pub(crate) use rustfs_ecstore::api::storage::ECStore as EcstoreStore;
use rustfs_storage_api as storage_contracts;

pub(crate) mod owner {
    pub(crate) use super::storage_contracts::{ObjectIO, ObjectOperations};

    pub(crate) use super::{
        ECSTORE_BUCKET_META_PREFIX, ECSTORE_DATA_USAGE_CACHE_NAME, ECSTORE_GLOBAL_LOCAL_DISK_MAP, ECSTORE_RUSTFS_META_BUCKET,
        EcstoreDeleteOptions, EcstoreDiskAPI, EcstoreDiskBytes, EcstoreDiskError, EcstoreDiskResult, EcstoreDiskStore,
        EcstoreEndpoint, EcstoreErrorType, EcstoreStorageError, EcstoreStore,
    };

    #[cfg(test)]
    pub(crate) use super::{EcstoreDiskOption, ecstore_new_disk};
}

pub(crate) mod storage {
    pub(crate) use super::storage_contracts::{
        BucketInfo, BucketOperations, DiskSetSelector, HealOperations, ListOperations, ObjectIO, ObjectOperations,
        StorageAdminApi,
    };
}

#[cfg(test)]
pub(crate) mod status {
    pub(crate) use super::storage_contracts::BucketInfo;
}
