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

use super::*;
use crate::bucket::lifecycle::lifecycle;
use rustfs_filemeta::RestoreStatusOps;
use rustfs_utils::http::headers::{AMZ_RESTORE_EXPIRY_DAYS, AMZ_RESTORE_REQUEST_DATE};
use s3s::dto::{RestoreStatus, Timestamp};

#[cfg(all(test, feature = "test-util"))]
struct RestoreFinalizeBarrierState {
    bucket: String,
    object: String,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(all(test, feature = "test-util"))]
static RESTORE_FINALIZE_BARRIER: std::sync::OnceLock<std::sync::Mutex<Option<Arc<RestoreFinalizeBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(all(test, feature = "test-util"))]
pub(in crate::set_disk) struct RestoreFinalizeBarrier {
    state: Arc<RestoreFinalizeBarrierState>,
}

#[cfg(all(test, feature = "test-util"))]
impl RestoreFinalizeBarrier {
    pub(in crate::set_disk) fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(RestoreFinalizeBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = RESTORE_FINALIZE_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("restore finalize barrier mutex should not poison");
        assert!(slot.is_none(), "restore finalize barrier must be installed by one test at a time");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(in crate::set_disk) async fn wait_until_paused(&self) {
        self.state.arrived.notified().await;
    }

    pub(in crate::set_disk) fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(all(test, feature = "test-util"))]
impl Drop for RestoreFinalizeBarrier {
    fn drop(&mut self) {
        let mut slot = RESTORE_FINALIZE_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("restore finalize barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(all(test, feature = "test-util"))]
async fn maybe_pause_restore_finalize(bucket: &str, object: &str) {
    let barrier = RESTORE_FINALIZE_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("restore finalize barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket && barrier.object == object)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RestoreCleanupIdentity {
    version_id: Option<Uuid>,
    data_dir: Option<Uuid>,
    mod_time: Option<OffsetDateTime>,
    size: i64,
}

impl RestoreCleanupIdentity {
    fn from_object_info(obj_info: &ObjectInfo) -> Self {
        Self {
            version_id: obj_info.version_id,
            data_dir: obj_info.data_dir,
            mod_time: obj_info.mod_time,
            size: obj_info.size,
        }
    }

    fn matches_file_info(&self, fi: &FileInfo, expected_etag: &str) -> bool {
        // Normalize the nil version on both sides: a versioning-suspended object
        // is `Some(Uuid::nil())` on one and `None` on the other, so a raw compare
        // reports every suspended restore as "changed before finalization".
        self.version_id.filter(|version_id| !version_id.is_nil()) == fi.version_id.filter(|version_id| !version_id.is_nil())
            && self.data_dir == fi.data_dir
            && self.mod_time == fi.mod_time
            && self.size == fi.size
            && expected_etag == get_raw_etag(&fi.metadata)
    }
}

fn ensure_restore_metadata_lock_held(bucket: &str, object: &str, opts: &ObjectOptions, mode: &'static str) -> Result<()> {
    if opts
        .namespace_lock_fence
        .as_ref()
        .is_some_and(NamespaceLockFence::is_lock_lost)
    {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode,
            bucket: bucket.to_string(),
            object: object.to_string(),
            required: 1,
            achieved: 0,
        });
    }
    Ok(())
}

impl SetDisks {
    pub(super) async fn finalize_restore_metadata(
        &self,
        bucket: &str,
        object: &str,
        obj_info: &ObjectInfo,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let expected = RestoreCleanupIdentity::from_object_info(obj_info);
        let expected_operation_id = restore_operation_id_from_metadata(&opts.user_defined)?;
        let expected_etag = obj_info
            .etag
            .clone()
            .unwrap_or_else(|| get_raw_etag(obj_info.user_defined.as_ref()));
        let version_id = expected.version_id.map(|v| v.to_string());
        let lock_guard = if !opts.no_lock {
            Some(
                self.acquire_write_lock_diag("restore_finalize_metadata", bucket, object)
                    .await?,
            )
        } else {
            None
        };
        let read_opts = ObjectOptions {
            version_id,
            versioned: opts.versioned,
            version_suspended: opts.version_suspended,
            include_part_checksums: true,
            ..Default::default()
        };
        let (mut fi, _, disks) = self
            .get_object_fileinfo_gated(bucket, object, &read_opts, false, false)
            .await?
            .into_owned();
        if let Some(expected_operation_id) = expected_operation_id
            && restore_operation_id_from_metadata(&fi.metadata)?.is_some_and(|actual| actual != expected_operation_id)
        {
            return Err(Error::other("restore operation id changed before metadata finalization"));
        }
        if !expected.matches_file_info(&fi, &expected_etag) {
            return Err(Error::other("restored object changed before restore metadata finalization"));
        }
        #[cfg(all(test, feature = "test-util"))]
        maybe_pause_restore_finalize(bucket, object).await;
        let restore_expiry =
            lifecycle::expected_expiry_time(OffsetDateTime::now_utc(), opts.transition.restore_request.days.unwrap_or(1));
        fi.metadata.insert(
            X_AMZ_RESTORE.as_str().to_string(),
            RestoreStatus {
                is_restore_in_progress: Some(false),
                restore_expiry_date: Some(Timestamp::from(restore_expiry)),
            }
            .to_string(),
        );
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        ensure_restore_metadata_lock_held(bucket, object, opts, "restore_finalize_metadata")?;
        if lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(Error::other("restore finalization lock lost before metadata update"));
        }
        self.update_object_meta_with_opts(
            bucket,
            object,
            fi.clone(),
            &disks,
            &UpdateMetadataOpts {
                replace_user_metadata: true,
                ..Default::default()
            },
        )
        .await?;
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }

    pub async fn update_restore_metadata(
        &self,
        bucket: &str,
        object: &str,
        obj_info: &ObjectInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        if obj_info.bucket.is_empty() || obj_info.name.is_empty() {
            return Ok(());
        }
        let expected = RestoreCleanupIdentity::from_object_info(obj_info);
        let expected_operation_id = restore_operation_id_from_metadata(&opts.user_defined)?;
        let expected_etag = obj_info
            .etag
            .clone()
            .unwrap_or_else(|| get_raw_etag(obj_info.user_defined.as_ref()));
        let version_id = expected.version_id.map(|v| v.to_string());
        let lock_guard = if !opts.no_lock {
            Some(
                self.acquire_write_lock_diag("restore_cleanup_metadata", bucket, object)
                    .await?,
            )
        } else {
            None
        };
        let read_opts = ObjectOptions {
            version_id,
            versioned: opts.versioned,
            version_suspended: opts.version_suspended,
            include_part_checksums: true,
            ..Default::default()
        };
        let (mut fi, _, disks) = self
            .get_object_fileinfo_gated(bucket, object, &read_opts, false, false)
            .await?
            .into_owned();
        if let Some(expected_operation_id) = expected_operation_id {
            match restore_operation_id_from_metadata(&fi.metadata)? {
                Some(actual_operation_id) if actual_operation_id == expected_operation_id => {}
                _ => return Ok(()),
            }
        }
        if !expected.matches_file_info(&fi, &expected_etag) {
            return Ok(());
        }
        ensure_restore_metadata_lock_held(bucket, object, opts, "restore_cleanup_metadata")?;
        fi.metadata.remove(X_AMZ_RESTORE.as_str());
        fi.metadata.remove(AMZ_RESTORE_EXPIRY_DAYS);
        fi.metadata.remove(AMZ_RESTORE_REQUEST_DATE);
        rustfs_utils::http::metadata_compat::remove_str(
            &mut fi.metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
        );
        if lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(Error::other("restore cleanup lock lost before metadata update".to_string()));
        }
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        self.update_object_meta_with_opts(
            bucket,
            object,
            fi,
            &disks,
            &UpdateMetadataOpts {
                replace_user_metadata: true,
                ..Default::default()
            },
        )
        .await?;
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        Ok(())
    }
}
