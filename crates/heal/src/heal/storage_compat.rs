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

pub(crate) const DATA_USAGE_CACHE_NAME: &str = rustfs_ecstore::api::data_usage::DATA_USAGE_CACHE_NAME;
pub(crate) const BUCKET_META_PREFIX: &str = rustfs_ecstore::api::disk::BUCKET_META_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = rustfs_ecstore::api::disk::RUSTFS_META_BUCKET;

pub(crate) type DiskError = rustfs_ecstore::api::disk::error::DiskError;
pub(crate) type DiskStore = rustfs_ecstore::api::disk::DiskStore;
pub(crate) type ECStore = rustfs_ecstore::api::storage::ECStore;
pub(crate) type EcstoreError = rustfs_ecstore::api::error::Error;
pub(crate) type Endpoint = rustfs_ecstore::api::disk::endpoint::Endpoint;
pub(crate) type StorageError = rustfs_ecstore::api::error::StorageError;
pub(crate) type LocalDiskMap = std::collections::HashMap<String, Option<DiskStore>>;

pub(crate) struct GlobalLocalDiskMap;

pub(crate) static GLOBAL_LOCAL_DISK_MAP: GlobalLocalDiskMap = GlobalLocalDiskMap;

impl GlobalLocalDiskMap {
    pub(crate) async fn read(&self) -> tokio::sync::RwLockReadGuard<'static, LocalDiskMap> {
        rustfs_ecstore::api::global::GLOBAL_LOCAL_DISK_MAP.read().await
    }
}

#[cfg(test)]
pub(crate) type DiskOption = rustfs_ecstore::api::disk::DiskOption;

#[cfg(test)]
pub(crate) async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> rustfs_ecstore::api::disk::error::Result<DiskStore> {
    rustfs_ecstore::api::disk::new_disk(ep, opt).await
}

pub type HealObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub type HealObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub type HealPutObjReader = <ECStore as rustfs_storage_api::ObjectIO>::PutObjectReader;
