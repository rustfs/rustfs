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
pub(crate) mod storage_api;
pub mod task;
pub mod utils;

use storage_api::owner::{
    ECSTORE_BUCKET_META_PREFIX, ECSTORE_DATA_USAGE_CACHE_NAME, ECSTORE_GLOBAL_LOCAL_DISK_MAP, ECSTORE_RUSTFS_META_BUCKET,
    EcstoreDeleteOptions, EcstoreDiskAPI, EcstoreDiskBytes, EcstoreDiskError, EcstoreDiskResult, EcstoreDiskStore,
    EcstoreEndpoint, EcstoreErrorType, EcstoreStorageError, EcstoreStore, ObjectIO, ObjectOperations,
};
#[cfg(test)]
use storage_api::owner::{EcstoreDiskOption, ecstore_new_disk};

pub use erasure_healer::ErasureSetHealer;
pub use manager::{HealManager, HealOperationsSnapshot, HealPriorityCounts, HealSourceCounts};
pub use resume::{CheckpointManager, ResumeCheckpoint, ResumeManager, ResumeState, ResumeUtils};
pub use task::{HealOptions, HealPriority, HealRequest, HealTask, HealType};

pub(crate) const DATA_USAGE_CACHE_NAME: &str = ECSTORE_DATA_USAGE_CACHE_NAME;
pub(crate) const BUCKET_META_PREFIX: &str = ECSTORE_BUCKET_META_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = ECSTORE_RUSTFS_META_BUCKET;

pub(crate) type DiskError = EcstoreDiskError;
pub(crate) type DiskResult<T> = EcstoreDiskResult<T>;
pub(crate) type DiskStore = EcstoreDiskStore;
pub(crate) type ECStore = EcstoreStore;
pub(crate) type EcstoreError = EcstoreErrorType;
pub(crate) type Endpoint = EcstoreEndpoint;
pub(crate) type StorageError = EcstoreStorageError;
pub(crate) type LocalDiskMap = std::collections::HashMap<String, Option<DiskStore>>;

pub(crate) struct GlobalLocalDiskMap;

pub(crate) static GLOBAL_LOCAL_DISK_MAP: GlobalLocalDiskMap = GlobalLocalDiskMap;

impl GlobalLocalDiskMap {
    pub(crate) async fn read(&self) -> tokio::sync::RwLockReadGuard<'static, LocalDiskMap> {
        ECSTORE_GLOBAL_LOCAL_DISK_MAP.read().await
    }
}

#[cfg(test)]
pub(crate) type DiskOption = EcstoreDiskOption;

#[cfg(test)]
pub(crate) async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> DiskResult<DiskStore> {
    ecstore_new_disk(ep, opt).await
}

pub(crate) trait HealDiskExt {
    fn endpoint(&self) -> Endpoint;
    async fn get_disk_id(&self) -> DiskResult<Option<uuid::Uuid>>;
    async fn read_all(&self, volume: &str, path: &str) -> DiskResult<EcstoreDiskBytes>;
    async fn write_all(&self, volume: &str, path: &str, data: EcstoreDiskBytes) -> DiskResult<()>;
    async fn delete(&self, volume: &str, path: &str, options: EcstoreDeleteOptions) -> DiskResult<()>;
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> DiskResult<Vec<String>>;
    #[cfg(test)]
    async fn make_volume(&self, volume: &str) -> DiskResult<()>;
}

impl<T> HealDiskExt for T
where
    T: EcstoreDiskAPI,
{
    fn endpoint(&self) -> Endpoint {
        EcstoreDiskAPI::endpoint(self)
    }

    async fn get_disk_id(&self) -> DiskResult<Option<uuid::Uuid>> {
        EcstoreDiskAPI::get_disk_id(self).await
    }

    async fn read_all(&self, volume: &str, path: &str) -> DiskResult<EcstoreDiskBytes> {
        EcstoreDiskAPI::read_all(self, volume, path).await
    }

    async fn write_all(&self, volume: &str, path: &str, data: EcstoreDiskBytes) -> DiskResult<()> {
        EcstoreDiskAPI::write_all(self, volume, path, data).await
    }

    async fn delete(&self, volume: &str, path: &str, options: EcstoreDeleteOptions) -> DiskResult<()> {
        EcstoreDiskAPI::delete(self, volume, path, options).await
    }

    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> DiskResult<Vec<String>> {
        EcstoreDiskAPI::list_dir(self, origvolume, volume, dir_path, count).await
    }

    #[cfg(test)]
    async fn make_volume(&self, volume: &str) -> DiskResult<()> {
        EcstoreDiskAPI::make_volume(self, volume).await
    }
}

pub type HealObjectInfo = <ECStore as ObjectOperations>::ObjectInfo;
pub type HealObjectOptions = <ECStore as ObjectOperations>::ObjectOptions;
pub type HealPutObjReader = <ECStore as ObjectIO>::PutObjectReader;
