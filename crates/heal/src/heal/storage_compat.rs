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

pub(crate) const DATA_USAGE_CACHE_NAME: &str = rustfs_ecstore::data_usage::DATA_USAGE_CACHE_NAME;
pub(crate) const BUCKET_META_PREFIX: &str = rustfs_ecstore::disk::BUCKET_META_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = rustfs_ecstore::disk::RUSTFS_META_BUCKET;

pub(crate) type DiskError = rustfs_ecstore::disk::error::DiskError;
pub(crate) type DiskStore = rustfs_ecstore::disk::DiskStore;
pub(crate) type ECStore = rustfs_ecstore::api::storage::ECStore;
pub(crate) type EcstoreError = rustfs_ecstore::error::Error;
pub(crate) type Endpoint = rustfs_ecstore::disk::endpoint::Endpoint;
pub(crate) type StorageError = rustfs_ecstore::error::StorageError;

pub(crate) use rustfs_ecstore::disk::DiskAPI;
pub(crate) use rustfs_ecstore::global::GLOBAL_LOCAL_DISK_MAP;

#[cfg(test)]
pub(crate) use rustfs_ecstore::disk::{DiskOption, new_disk};

pub type HealObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub type HealObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub type HealPutObjReader = <ECStore as rustfs_storage_api::ObjectIO>::PutObjectReader;
