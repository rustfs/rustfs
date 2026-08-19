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
pub mod manager;
pub mod mrf_queue;
pub mod progress;
pub(crate) mod replacement_readiness;
pub mod resume;
pub mod storage;
pub(crate) mod storage_api;
pub mod task;
pub mod utils;

use storage_api::owner::{
    ECSTORE_BUCKET_META_PREFIX, ECSTORE_DATA_USAGE_CACHE_NAME, ECSTORE_HEALING_MARKER_PATH, ECSTORE_RUSTFS_META_BUCKET,
    EcstoreConditionalFileUpdate, EcstoreDeleteOptions, EcstoreDiskAPI, EcstoreDiskBytes, EcstoreDiskError, EcstoreDiskResult,
    EcstoreDiskStore, EcstoreEndpoint, EcstoreErrorType, EcstoreStorageError, EcstoreStore, ObjectIO, ObjectOperations,
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
pub(crate) async fn set_healing_markers(endpoints: &[String], marker: &str) -> crate::Result<()> {
    apply_healing_markers(endpoints, Some(marker), None, false).await
}

/// Remove an owner marker after the replacement scan's verified state is
/// durable. A missing marker is idempotent here because a crash may have
/// happened after the previous terminal clear and before resume cleanup.
pub(crate) async fn clear_healing_markers_after_verified(endpoints: &[String], marker: &str) -> crate::Result<()> {
    apply_healing_markers(endpoints, None, Some(marker), true).await
}

#[cfg(test)]
fn marker_matches(current: &[u8], expected_marker: Option<&str>) -> bool {
    expected_marker.is_some_and(|expected| current == expected.as_bytes())
}

async fn apply_healing_markers(
    endpoints: &[String],
    marker: Option<&str>,
    expected_marker: Option<&str>,
    allow_missing: bool,
) -> crate::Result<()> {
    if endpoints.is_empty() {
        return Ok(());
    }
    let mut local_disks = std::collections::HashMap::new();
    {
        let local_disk_map = local_disk_map_read().await;
        for disk in local_disk_map.values().flatten() {
            local_disks.insert(EcstoreDiskAPI::endpoint(disk.as_ref()).to_string(), disk.clone());
        }
    }

    let mut matched_endpoints = std::collections::HashSet::new();
    let mut targets = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        if !matched_endpoints.insert(endpoint.clone()) {
            return Err(DiskError::other("healing marker endpoint is duplicated").into());
        }
        let Some(disk) = local_disks.remove(endpoint) else {
            return Err(DiskError::other("healing marker target is unavailable").into());
        };
        targets.push(disk);
    }

    apply_healing_markers_to_targets(targets, marker, expected_marker, allow_missing).await
}

async fn apply_healing_markers_to_targets(
    targets: Vec<DiskStore>,
    marker: Option<&str>,
    expected_marker: Option<&str>,
    allow_missing: bool,
) -> crate::Result<()> {
    apply_healing_markers_to_targets_with_after_acquire(targets, marker, expected_marker, allow_missing, |_| {}).await
}

async fn apply_healing_markers_to_targets_with_after_acquire<F>(
    targets: Vec<DiskStore>,
    marker: Option<&str>,
    expected_marker: Option<&str>,
    allow_missing: bool,
    mut after_acquire: F,
) -> crate::Result<()>
where
    F: FnMut(&DiskStore),
{
    let marker_bytes = marker.map(|marker| EcstoreDiskBytes::copy_from_slice(marker.as_bytes()));
    let expected_bytes = expected_marker.map(|marker| EcstoreDiskBytes::copy_from_slice(marker.as_bytes()));
    let mut newly_acquired = Vec::new();
    for disk in targets {
        let result = match marker_bytes.as_ref() {
            Some(marker) => {
                match EcstoreDiskAPI::compare_and_update_file(
                    disk.as_ref(),
                    RUSTFS_META_BUCKET,
                    HEALING_MARKER_PATH,
                    None,
                    Some(marker.clone()),
                )
                .await
                {
                    Ok(EcstoreConditionalFileUpdate::Updated) => {
                        newly_acquired.push(disk.clone());
                        after_acquire(&disk);
                        Ok(())
                    }
                    Ok(EcstoreConditionalFileUpdate::Mismatch) => match EcstoreDiskAPI::compare_and_update_file(
                        disk.as_ref(),
                        RUSTFS_META_BUCKET,
                        HEALING_MARKER_PATH,
                        Some(marker.clone()),
                        Some(marker.clone()),
                    )
                    .await
                    {
                        Ok(EcstoreConditionalFileUpdate::Updated) => Ok(()),
                        Ok(_) => Err(DiskError::other("healing marker ownership changed")),
                        Err(err) => Err(err),
                    },
                    Ok(EcstoreConditionalFileUpdate::Missing) => Err(DiskError::other("healing marker disappeared")),
                    Err(err) => Err(err),
                }
            }
            None => {
                match EcstoreDiskAPI::compare_and_update_file(
                    disk.as_ref(),
                    RUSTFS_META_BUCKET,
                    HEALING_MARKER_PATH,
                    expected_bytes.clone(),
                    None,
                )
                .await
                {
                    Ok(EcstoreConditionalFileUpdate::Updated) => Ok(()),
                    Ok(EcstoreConditionalFileUpdate::Missing) if allow_missing => Ok(()),
                    Ok(EcstoreConditionalFileUpdate::Missing) => Err(DiskError::other("healing marker is missing")),
                    Ok(EcstoreConditionalFileUpdate::Mismatch) => Err(DiskError::other("healing marker ownership changed")),
                    Err(err) => Err(err),
                }
            }
        };
        if let Err(err) = result {
            if let Some(marker) = marker_bytes.as_ref() {
                let mut rollback_error = None;
                for acquired in newly_acquired.iter().rev() {
                    if let Err(rollback) = EcstoreDiskAPI::compare_and_update_file(
                        acquired.as_ref(),
                        RUSTFS_META_BUCKET,
                        HEALING_MARKER_PATH,
                        Some(marker.clone()),
                        None,
                    )
                    .await
                    {
                        rollback_error.get_or_insert(rollback);
                    }
                }
                if let Some(rollback) = rollback_error {
                    return Err(DiskError::other(format!(
                        "healing marker acquisition failed ({err}) and owner-safe rollback failed ({rollback})"
                    ))
                    .into());
                }
            }
            return Err(err.into());
        }
    }
    Ok(())
}

pub(crate) type DiskError = EcstoreDiskError;
pub(crate) type DiskResult<T> = EcstoreDiskResult<T>;
pub(crate) type DiskStore = EcstoreDiskStore;
pub(crate) type ECStore = EcstoreStore;
pub(crate) type EcstoreError = EcstoreErrorType;
pub(crate) type Endpoint = EcstoreEndpoint;
pub(crate) type StorageError = EcstoreStorageError;
pub(crate) type LocalDiskMap = std::collections::HashMap<String, Option<DiskStore>>;

/// Read the local disk map as an owned guard.
///
/// Returns an owned guard (see the ecstore boundary), so the heal manager can
/// hold it across `.await` without depending on a `'static` process global —
/// the prerequisite for moving the disk map into the per-instance
/// `InstanceContext` (backlog#939). Usage is otherwise unchanged.
pub(crate) async fn local_disk_map_read() -> tokio::sync::OwnedRwLockReadGuard<LocalDiskMap> {
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

#[cfg(test)]
mod tests {
    use super::{
        DiskError, DiskOption, Endpoint, HEALING_MARKER_PATH, RUSTFS_META_BUCKET, apply_healing_markers_to_targets,
        apply_healing_markers_to_targets_with_after_acquire, marker_matches, new_disk,
    };
    use crate::{
        Error,
        heal::storage_api::owner::{EcstoreConditionalFileUpdate, EcstoreDiskAPI, EcstoreDiskBytes},
    };
    use tempfile::TempDir;

    async fn make_marker_disk(temp: &TempDir, name: &str) -> super::DiskStore {
        let path = temp.path().join(name);
        std::fs::create_dir_all(&path).expect("marker disk directory should be created");
        let endpoint = Endpoint::try_from(path.to_string_lossy().as_ref()).expect("marker disk endpoint should be valid");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("marker disk should initialize");
        let metadata_volume = disk.make_volume(RUSTFS_META_BUCKET).await;
        assert!(
            matches!(metadata_volume, Ok(()) | Err(DiskError::VolumeExists)),
            "marker metadata volume should exist: {metadata_volume:?}"
        );
        disk
    }

    #[test]
    fn marker_clear_requires_the_current_owner_token() {
        assert!(marker_matches(b"set:task-a", Some("set:task-a")));
        assert!(!marker_matches(b"set:task-b", Some("set:task-a")));
        assert!(!marker_matches(b"set:task-a", None));
    }

    #[tokio::test]
    async fn marker_acquisition_rolls_back_after_second_disk_ownership_conflict() {
        let temp = TempDir::new().expect("marker test directory should be created");
        let first = make_marker_disk(&temp, "first").await;
        let second = make_marker_disk(&temp, "second").await;
        let owner_b = EcstoreDiskBytes::from_static(b"owner-b");

        assert_eq!(
            EcstoreDiskAPI::compare_and_update_file(
                second.as_ref(),
                RUSTFS_META_BUCKET,
                HEALING_MARKER_PATH,
                None,
                Some(owner_b.clone()),
            )
            .await
            .expect("second disk owner should acquire marker"),
            EcstoreConditionalFileUpdate::Updated
        );

        let err = apply_healing_markers_to_targets(vec![first.clone(), second.clone()], Some("owner-a"), None, false)
            .await
            .expect_err("second disk ownership must reject the partial acquisition");
        assert!(matches!(err, Error::Disk(DiskError::Io(ref io)) if io.to_string() == "healing marker ownership changed"));
        assert!(matches!(
            EcstoreDiskAPI::read_all(first.as_ref(), RUSTFS_META_BUCKET, HEALING_MARKER_PATH).await,
            Err(DiskError::FileNotFound)
        ));
        assert_eq!(
            EcstoreDiskAPI::read_all(second.as_ref(), RUSTFS_META_BUCKET, HEALING_MARKER_PATH)
                .await
                .expect("conflicting owner marker must remain"),
            owner_b
        );
    }

    #[tokio::test]
    async fn marker_acquisition_rolls_back_after_second_disk_io_error() {
        let temp = TempDir::new().expect("marker test directory should be created");
        let first = make_marker_disk(&temp, "first").await;
        let second_path = temp.path().join("second");
        std::fs::create_dir_all(&second_path).expect("second marker disk directory should be created");
        let second_endpoint =
            Endpoint::try_from(second_path.to_string_lossy().as_ref()).expect("second marker endpoint should be valid");
        let second = new_disk(
            &second_endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("second marker disk should initialize");
        std::fs::remove_dir_all(second_path.join(RUSTFS_META_BUCKET))
            .expect("second marker metadata directory should be removed for the I/O failure fixture");
        std::fs::write(second_path.join(RUSTFS_META_BUCKET), b"not a directory")
            .expect("second marker volume should become an I/O failure fixture");

        let err = apply_healing_markers_to_targets(vec![first.clone(), second], Some("owner-a"), None, false)
            .await
            .expect_err("second disk I/O failure must reject the partial acquisition");
        assert!(
            matches!(err, Error::Disk(DiskError::FileAccessDenied)),
            "second marker operation must report its mapped filesystem failure: {err:?}"
        );
        assert!(matches!(
            EcstoreDiskAPI::read_all(first.as_ref(), RUSTFS_META_BUCKET, HEALING_MARKER_PATH).await,
            Err(DiskError::FileNotFound)
        ));
    }

    #[tokio::test]
    async fn marker_acquisition_reports_an_owner_safe_rollback_io_failure() {
        let temp = TempDir::new().expect("marker test directory should be created");
        let first = make_marker_disk(&temp, "first").await;
        let second = make_marker_disk(&temp, "second").await;
        let owner_b = EcstoreDiskBytes::from_static(b"owner-b");
        let first_path = EcstoreDiskAPI::path(first.as_ref());
        let moved_metadata_path = first_path.join("metadata-before-rollback");

        assert_eq!(
            EcstoreDiskAPI::compare_and_update_file(
                second.as_ref(),
                RUSTFS_META_BUCKET,
                HEALING_MARKER_PATH,
                None,
                Some(owner_b),
            )
            .await
            .expect("second disk owner should acquire marker"),
            EcstoreConditionalFileUpdate::Updated
        );

        let err =
            apply_healing_markers_to_targets_with_after_acquire(vec![first, second], Some("owner-a"), None, false, |disk| {
                let metadata_path = EcstoreDiskAPI::path(disk.as_ref()).join(RUSTFS_META_BUCKET);
                std::fs::rename(&metadata_path, &moved_metadata_path)
                    .expect("first marker metadata should move after acquisition");
                std::fs::write(&metadata_path, b"not a directory")
                    .expect("first marker metadata should become a rollback I/O failure fixture");
            })
            .await
            .expect_err("rollback I/O failure must remain visible to the caller");
        let message = err.to_string();
        assert!(message.contains("healing marker acquisition failed"));
        assert!(message.contains("owner-safe rollback failed"));
        assert!(moved_metadata_path.join(HEALING_MARKER_PATH).exists());
    }

    #[tokio::test]
    async fn concurrent_marker_acquisition_has_one_owner_on_every_disk() {
        let temp = TempDir::new().expect("marker test directory should be created");
        let first = make_marker_disk(&temp, "first").await;
        let second = make_marker_disk(&temp, "second").await;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));

        let owner_a_barrier = barrier.clone();
        let owner_a_first = first.clone();
        let owner_a_second = second.clone();
        let owner_a = tokio::spawn(async move {
            owner_a_barrier.wait().await;
            apply_healing_markers_to_targets(vec![owner_a_first, owner_a_second], Some("owner-a"), None, false).await
        });
        let owner_b_barrier = barrier.clone();
        let owner_b_first = first.clone();
        let owner_b_second = second.clone();
        let owner_b = tokio::spawn(async move {
            owner_b_barrier.wait().await;
            apply_healing_markers_to_targets(vec![owner_b_first, owner_b_second], Some("owner-b"), None, false).await
        });

        barrier.wait().await;
        let owner_a_result = owner_a.await.expect("owner a task should join");
        let owner_b_result = owner_b.await.expect("owner b task should join");
        assert_ne!(
            owner_a_result.is_ok(),
            owner_b_result.is_ok(),
            "exactly one owner must acquire both markers"
        );

        let winning_marker = if owner_a_result.is_ok() { b"owner-a" } else { b"owner-b" };
        for disk in [&first, &second] {
            assert_eq!(
                EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, HEALING_MARKER_PATH)
                    .await
                    .expect("every disk must retain the winning owner marker"),
                EcstoreDiskBytes::from_static(winning_marker)
            );
        }
    }
}
