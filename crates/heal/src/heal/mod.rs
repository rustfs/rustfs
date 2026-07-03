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
    ECSTORE_BUCKET_META_PREFIX, ECSTORE_DATA_USAGE_CACHE_NAME, ECSTORE_HEALING_MARKER_PATH, ECSTORE_RUSTFS_META_BUCKET,
    EcstoreDeleteOptions, EcstoreDiskAPI, EcstoreDiskBytes, EcstoreDiskError, EcstoreDiskResult, EcstoreDiskStore,
    EcstoreEndpoint, EcstoreErrorType, EcstoreStorageError, EcstoreStore, ObjectIO, ObjectOperations,
    ecstore_local_disk_map_read,
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

/// Marker written to every local disk while the process runs; removed by
/// [`clear_unclean_shutdown_markers`] on graceful shutdown. Finding it at
/// startup means the previous run crashed or lost power, so the heal manager
/// proactively re-verifies all local erasure sets.
pub(crate) const UNCLEAN_SHUTDOWN_MARKER_PATH: &str = "unclean-shutdown";

/// Remove the unclean-shutdown markers from all local disks. Call at the end of
/// a graceful shutdown, after the data plane has stopped accepting writes.
pub async fn clear_unclean_shutdown_markers() {
    let local_disk_map = local_disk_map_read().await;
    for disk in local_disk_map.values().flatten() {
        if let Err(err) = EcstoreDiskAPI::delete(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            UNCLEAN_SHUTDOWN_MARKER_PATH,
            EcstoreDeleteOptions::default(),
        )
        .await
            && err != DiskError::FileNotFound
        {
            tracing::warn!(
                endpoint = %EcstoreDiskAPI::endpoint(disk.as_ref()),
                error = ?err,
                "failed to clear unclean-shutdown marker"
            );
        }
    }
}

/// Per-disk healing marker path (inside `RUSTFS_META_BUCKET`), mirrored from
/// ecstore so both sides agree on where `DiskInfo.healing` is derived from.
pub(crate) const HEALING_MARKER_PATH: &str = ECSTORE_HEALING_MARKER_PATH;

/// Write the healing marker on the local disks matching `endpoints` so their
/// `DiskInfo.healing` reports true while the erasure-set heal rebuilds them.
pub(crate) async fn set_healing_markers(endpoints: &[String], set_disk_id: &str) {
    apply_healing_markers(endpoints, Some(set_disk_id)).await;
}

/// Remove the healing markers written by [`set_healing_markers`].
pub(crate) async fn clear_healing_markers(endpoints: &[String]) {
    apply_healing_markers(endpoints, None).await;
}

async fn apply_healing_markers(endpoints: &[String], set_disk_id: Option<&str>) {
    if endpoints.is_empty() {
        return;
    }
    let local_disk_map = local_disk_map_read().await;
    for disk in local_disk_map.values().flatten() {
        let endpoint = EcstoreDiskAPI::endpoint(disk.as_ref()).to_string();
        if !endpoints.iter().any(|candidate| candidate == &endpoint) {
            continue;
        }
        let result = match set_disk_id {
            Some(set_disk_id) => {
                EcstoreDiskAPI::write_all(
                    disk.as_ref(),
                    RUSTFS_META_BUCKET,
                    HEALING_MARKER_PATH,
                    EcstoreDiskBytes::copy_from_slice(set_disk_id.as_bytes()),
                )
                .await
            }
            None => match EcstoreDiskAPI::delete(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                HEALING_MARKER_PATH,
                EcstoreDeleteOptions::default(),
            )
            .await
            {
                Err(DiskError::FileNotFound) => Ok(()),
                other => other,
            },
        };
        if let Err(err) = result {
            tracing::warn!(
                endpoint = %endpoint,
                action = if set_disk_id.is_some() { "set" } else { "clear" },
                error = ?err,
                "failed to update healing marker"
            );
        }
    }
}

pub(crate) type DiskError = EcstoreDiskError;
pub(crate) type DiskResult<T> = EcstoreDiskResult<T>;
pub(crate) type DiskStore = EcstoreDiskStore;
pub(crate) type ECStore = EcstoreStore;
pub(crate) type EcstoreError = EcstoreErrorType;
pub(crate) type Endpoint = EcstoreEndpoint;
pub(crate) type StorageError = EcstoreStorageError;
pub(crate) type LocalDiskMap = std::collections::HashMap<String, Option<DiskStore>>;

pub(crate) async fn local_disk_map_read() -> tokio::sync::RwLockReadGuard<'static, LocalDiskMap> {
    ecstore_local_disk_map_read().await
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
