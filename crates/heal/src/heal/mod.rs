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

pub mod channel;
pub mod erasure_healer;
pub mod event;
pub mod manager;
pub mod progress;
pub mod resume;
pub mod storage;
pub mod task;
pub mod utils;

use rustfs_ecstore::api::data_usage as ecstore_data_usage;
use rustfs_ecstore::api::disk as ecstore_disk;
use rustfs_ecstore::api::error as ecstore_error;
use rustfs_ecstore::api::global as ecstore_global;
use rustfs_ecstore::api::storage as ecstore_storage;

pub use erasure_healer::ErasureSetHealer;
pub use manager::{HealManager, HealOperationsSnapshot, HealPriorityCounts, HealSourceCounts};
pub use resume::{CheckpointManager, ResumeCheckpoint, ResumeManager, ResumeState, ResumeUtils};
pub use task::{HealOptions, HealPriority, HealRequest, HealTask, HealType};

pub(crate) const DATA_USAGE_CACHE_NAME: &str = ecstore_data_usage::DATA_USAGE_CACHE_NAME;
pub(crate) const BUCKET_META_PREFIX: &str = ecstore_disk::BUCKET_META_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = ecstore_disk::RUSTFS_META_BUCKET;

pub(crate) type DiskError = ecstore_disk::error::DiskError;
pub(crate) type DiskResult<T> = ecstore_disk::error::Result<T>;
pub(crate) type DiskStore = ecstore_disk::DiskStore;
pub(crate) type ECStore = ecstore_storage::ECStore;
pub(crate) type EcstoreError = ecstore_error::Error;
pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;
pub(crate) type StorageError = ecstore_error::StorageError;
pub(crate) type LocalDiskMap = std::collections::HashMap<String, Option<DiskStore>>;

pub(crate) struct GlobalLocalDiskMap;

pub(crate) static GLOBAL_LOCAL_DISK_MAP: GlobalLocalDiskMap = GlobalLocalDiskMap;

impl GlobalLocalDiskMap {
    pub(crate) async fn read(&self) -> tokio::sync::RwLockReadGuard<'static, LocalDiskMap> {
        ecstore_global::GLOBAL_LOCAL_DISK_MAP.read().await
    }
}

#[cfg(test)]
pub(crate) type DiskOption = ecstore_disk::DiskOption;

#[cfg(test)]
pub(crate) async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> ecstore_disk::error::Result<DiskStore> {
    ecstore_disk::new_disk(ep, opt).await
}

pub(crate) trait HealDiskExt {
    fn endpoint(&self) -> Endpoint;
    async fn get_disk_id(&self) -> DiskResult<Option<uuid::Uuid>>;
    async fn read_all(&self, volume: &str, path: &str) -> DiskResult<ecstore_disk::Bytes>;
    async fn write_all(&self, volume: &str, path: &str, data: ecstore_disk::Bytes) -> DiskResult<()>;
    async fn delete(&self, volume: &str, path: &str, options: ecstore_disk::DeleteOptions) -> DiskResult<()>;
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> DiskResult<Vec<String>>;
    #[cfg(test)]
    async fn make_volume(&self, volume: &str) -> DiskResult<()>;
}

impl<T> HealDiskExt for T
where
    T: ecstore_disk::DiskAPI,
{
    fn endpoint(&self) -> Endpoint {
        ecstore_disk::DiskAPI::endpoint(self)
    }

    async fn get_disk_id(&self) -> DiskResult<Option<uuid::Uuid>> {
        ecstore_disk::DiskAPI::get_disk_id(self).await
    }

    async fn read_all(&self, volume: &str, path: &str) -> DiskResult<ecstore_disk::Bytes> {
        ecstore_disk::DiskAPI::read_all(self, volume, path).await
    }

    async fn write_all(&self, volume: &str, path: &str, data: ecstore_disk::Bytes) -> DiskResult<()> {
        ecstore_disk::DiskAPI::write_all(self, volume, path, data).await
    }

    async fn delete(&self, volume: &str, path: &str, options: ecstore_disk::DeleteOptions) -> DiskResult<()> {
        ecstore_disk::DiskAPI::delete(self, volume, path, options).await
    }

    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> DiskResult<Vec<String>> {
        ecstore_disk::DiskAPI::list_dir(self, origvolume, volume, dir_path, count).await
    }

    #[cfg(test)]
    async fn make_volume(&self, volume: &str) -> DiskResult<()> {
        ecstore_disk::DiskAPI::make_volume(self, volume).await
    }
}

pub type HealObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub type HealObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub type HealPutObjReader = <ECStore as rustfs_storage_api::ObjectIO>::PutObjectReader;
