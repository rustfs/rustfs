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

//! Single-disk object rename publication and rollback. The shared execution core
//! retains instrumentation, mutation leases, and commit guards through the syscall.

#[cfg(all(test, windows))]
use super::run_destination_commit_directory_preparation;
#[cfg(any(not(windows), test))]
use super::should_fail_local_inline_rollback_hardlink;
use super::{
    EVENT_DISK_LOCAL_ACCESS_FAILED, EVENT_DISK_LOCAL_HEAL_PURGE_FAILED, EVENT_DISK_LOCAL_RENAME_REJECTED, LOG_COMPONENT_ECSTORE,
    LOG_SUBSYSTEM_DISK_LOCAL, LocalDisk, SyncMode, effective_durability, has_inline_commit_test_failpoints,
    inline_metadata_rollback_dir, observe_old_current, remove_dir_all_if_exists, remove_dst_base_before_commit,
    remove_file_if_exists, rename_data_versions_signature, run_inline_preparation_before_backup,
    should_fail_after_metadata_commit, should_fail_before_old_metadata_backup, should_fail_commit_rename,
    should_remove_staged_meta_before_commit, skip_access_checks, tmp_staging_mutation_path,
};
#[cfg(test)]
use super::{run_inline_before_file_sync_admission, run_owned_file_write_before_open, run_rename_data_after_first_publication};
use crate::crash_inject::{self, CrashPoint};
use crate::disk::{
    QUOTA_MUTATION_FENCE_METADATA_SUFFIX, RenameDataResp, STORAGE_FORMAT_FILE, STORAGE_FORMAT_FILE_BACKUP, SnapshotLeaseToken,
    error::{DiskError, Result},
    error_conv::{to_access_error, to_file_error},
    os,
    os::check_path_length,
};
use bytes::Bytes;
use rustfs_filemeta::{FileInfo, FileMeta};
use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;
use tracing::{info, warn};
use uuid::Uuid;

/// Hold later repair publications after admitting one baseline object. The
/// fixture arms this on one replacement disk before rejoining the cluster.
#[cfg(feature = "e2e-test-hooks")]
async fn wait_for_heal_commit_test_barrier(root: &Path, bucket: &str, object: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let barrier = root.join(".rustfs.sys/e2e-heal-commit-barrier");
    let prefix = match fs::read_to_string(&barrier).await {
        Ok(prefix) => prefix,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let key = format!("{bucket}/{object}");
    if prefix.is_empty() || !key.starts_with(&prefix) {
        return Ok(());
    }
    let admitted = barrier.with_extension("admitted");
    match fs::OpenOptions::new().write(true).create_new(true).open(&admitted).await {
        Ok(mut file) => {
            file.write_all(key.as_bytes()).await?;
            return Ok(());
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        if !fs::try_exists(&barrier).await? || fs::read_to_string(&admitted).await? == key {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::new(ErrorKind::TimedOut, "heal commit test barrier was not released").into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

struct RenameCommitContext<'a> {
    // Drop the directory identity guard before releasing mutation ownership.
    directory_guard: os::RenameCommitGuard,
    publication_root: &'a os::PublicationRoot,
    mutation_lease: Arc<os::NamespaceMutationLease>,
    source_metadata: &'a Path,
    destination_metadata: &'a Path,
    source_volume_dir: &'a Path,
    data_paths: Option<&'a (PathBuf, PathBuf)>,
}

struct StagedRollbackBackup(PathBuf);

impl StagedRollbackBackup {
    fn path(&self) -> &Path {
        &self.0
    }
}

impl RenameCommitContext<'_> {
    fn rollback_metadata_std(&self, rollback_data_dir: Option<Uuid>) -> std::io::Result<()> {
        rollback_metadata_std(self.destination_metadata, rollback_data_dir)
    }

    fn rollback_non_inline_std(&self, rollback_data_dir: Option<Uuid>) -> std::io::Result<()> {
        self.rollback_metadata_std(rollback_data_dir)?;
        if let Some((_, new_data_path)) = self.data_paths {
            remove_dir_all_if_exists(new_data_path)?;
        }
        Ok(())
    }

    fn rollback_inline_std(
        &self,
        rollback_data_dir: Option<Uuid>,
        local_backup: Option<&StagedRollbackBackup>,
    ) -> std::io::Result<()> {
        rollback_inline_metadata_std(self.destination_metadata, rollback_data_dir, local_backup)
    }

    async fn restore_published_data_source(&self) -> Result<()> {
        let Some((src_data_path, dst_data_path)) = self.data_paths else {
            return Ok(());
        };
        let src_volume_dir = self.source_volume_dir;
        let publication_root = self.publication_root;
        let mutation_lease = self.mutation_lease.clone();
        if fs::symlink_metadata(src_data_path).await.is_ok() {
            return Ok(());
        }
        let result =
            match os::rename_all_with_lease(dst_data_path, src_data_path, src_volume_dir, publication_root, mutation_lease).await
            {
                Ok(()) => Ok(()),
                Err(DiskError::FileNotFound) => {
                    let source_exists = fs::symlink_metadata(src_data_path).await.is_ok();
                    let destination_missing = matches!(
                        fs::symlink_metadata(dst_data_path).await,
                        Err(err) if err.kind() == ErrorKind::NotFound
                    );
                    if source_exists && destination_missing {
                        Ok(())
                    } else {
                        Err(DiskError::FileNotFound)
                    }
                }
                Err(err) => Err(err),
            };
        if let Err(err) = &result {
            warn!(
                target: "rustfs_ecstore::disk::local",
                event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                reason = "restore_staged_data_source_failed",
                src_path = ?src_data_path,
                dst_path = ?dst_data_path,
                error = ?err,
                "Failed to restore staged data after a metadata commit was rejected"
            );
        }
        result
    }
}

#[cfg(any(not(windows), test))]
pub(super) fn create_local_inline_rollback_backup(
    dst_file_path: &Path,
    staging_file_path: &Path,
    old_metadata: &[u8],
) -> std::io::Result<PathBuf> {
    let Some(staging_parent) = staging_file_path.parent() else {
        return Err(std::io::Error::new(ErrorKind::InvalidInput, "missing staging metadata parent"));
    };
    let backup_path = staging_parent.join(STORAGE_FORMAT_FILE_BACKUP);
    remove_file_if_exists(&backup_path)?;
    if (should_fail_local_inline_rollback_hardlink(dst_file_path) || std::fs::hard_link(dst_file_path, &backup_path).is_err())
        && let Err(err) = std::fs::write(&backup_path, old_metadata)
    {
        let _ = remove_file_if_exists(&backup_path);
        return Err(err);
    }
    Ok(backup_path)
}

fn rollback_metadata_std(destination_metadata: &Path, rollback_data_dir: Option<Uuid>) -> std::io::Result<()> {
    if let Some(old_data_dir) = rollback_data_dir {
        let Some(dst_parent) = destination_metadata.parent() else {
            return Err(std::io::Error::new(ErrorKind::InvalidInput, "missing object metadata parent"));
        };
        let backup_path = dst_parent.join(old_data_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
        std::fs::rename(backup_path, destination_metadata)?;
    } else {
        remove_file_if_exists(destination_metadata)?;
    }
    Ok(())
}

fn rollback_inline_metadata_std(
    destination_metadata: &Path,
    rollback_data_dir: Option<Uuid>,
    local_backup: Option<&StagedRollbackBackup>,
) -> std::io::Result<()> {
    #[cfg(all(test, not(windows)))]
    os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Rollback, destination_metadata);
    if let Some(backup) = local_backup {
        // The preceding commit used this staged directory on the same
        // filesystem. Never unlink xl.meta before its atomic replacement.
        std::fs::rename(backup.path(), destination_metadata)?;
        Ok(())
    } else {
        rollback_metadata_std(destination_metadata, rollback_data_dir)
    }
}

fn fsync_inline_commit_std(
    dst: &Path,
    dst_volume_dir: &Path,
    destination_was_absent: bool,
    local_backup: Option<&StagedRollbackBackup>,
    commit_guard: &os::RenameCommitGuard,
) -> std::io::Result<()> {
    if let Some(dst_parent) = dst.parent() {
        let fsync_started = rustfs_io_metrics::put_stage_timer();
        if let Err(err) = os::fsync_commit_directory_limited_std(dst_parent, commit_guard) {
            rustfs_io_metrics::record_put_object_stage_duration_from(
                rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC,
                fsync_started,
            );
            rollback_inline_metadata_std(dst, None, local_backup)?;
            return Err(err);
        }
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC,
            fsync_started,
        );
    }

    if destination_was_absent {
        let mut ancestor = dst.parent().and_then(Path::parent);
        while let Some(ancestor_dir) = ancestor {
            if !ancestor_dir.starts_with(dst_volume_dir) {
                break;
            }
            let fsync_started = rustfs_io_metrics::put_stage_timer();
            if let Err(err) = os::fsync_dir_limited_std(ancestor_dir) {
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC,
                    fsync_started,
                );
                rollback_inline_metadata_std(dst, None, local_backup)?;
                return Err(err);
            }
            rustfs_io_metrics::record_put_object_stage_duration_from(
                rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC,
                fsync_started,
            );
            if ancestor_dir == dst_volume_dir {
                break;
            }
            ancestor = ancestor_dir.parent();
        }
    }
    Ok(())
}

type PreparedInlineMetadata = (
    Option<Uuid>,
    Option<Uuid>,
    Option<Vec<u8>>,
    Option<crate::disk::OldCurrentSize>,
    bool,
    Option<FileInfo>,
    Option<StagedRollbackBackup>,
    bool,
    os::PreparedRenameSource,
    Option<Arc<os::PreparedPoolMetaBackup>>,
    Option<super::pool_meta_native::DurableHeadReceipt>,
);

enum InlinePreparationOutcome {
    Committed(RenameDataResp, Option<super::pool_meta_native::DurableHeadReceipt>),
    CommitErr(DiskError),
    Prepared(PreparedInlineMetadata),
}

#[cfg(test)]
pub(super) async fn lock_rename_commit_directories(
    source_parent: &Path,
    destination_parent: &Path,
    base_dir: &Path,
    publication_root: &os::PublicationRoot,
    mutation_lease: Arc<os::NamespaceMutationLease>,
) -> Result<os::RenameCommitGuard> {
    #[cfg(any(windows, target_os = "linux"))]
    let result = {
        let source_parent = source_parent.to_path_buf();
        let destination_parent = destination_parent.to_path_buf();
        let base_dir = base_dir.to_path_buf();
        let publication_root = publication_root.clone();
        os::run_blocking_namespace_operation(mutation_lease, move || {
            let result = os::prepare_rename_commit_guard(&source_parent, &destination_parent, &base_dir, &publication_root);
            #[cfg(all(test, windows))]
            if result.is_ok() {
                run_destination_commit_directory_preparation(&destination_parent);
            }
            result
        })
        .await
    };
    #[cfg(not(any(windows, target_os = "linux")))]
    let result = {
        let _ = mutation_lease;
        os::prepare_rename_commit_guard(source_parent, destination_parent, base_dir, publication_root)
    };

    let result = result.map_err(|err| match std::fs::symlink_metadata(base_dir) {
        Err(base_err) if base_err.kind() == ErrorKind::NotFound => base_err,
        _ => err,
    });

    result.map_err(to_file_error).map_err(DiskError::from)
}

async fn prepare_rename_commit_and_destination_metadata(
    create_source_parent: bool,
    source_parent: &Path,
    destination_parent: &Path,
    destination_metadata: &Path,
    base_dir: &Path,
    publication_root: &os::PublicationRoot,
    mutation_lease: Arc<os::NamespaceMutationLease>,
) -> Result<(os::RenameCommitGuard, Option<Bytes>)> {
    #[cfg(any(windows, target_os = "linux"))]
    let result = {
        let source_parent = source_parent.to_path_buf();
        let destination_parent = destination_parent.to_path_buf();
        let destination_metadata = destination_metadata.to_path_buf();
        let base_dir = base_dir.to_path_buf();
        let publication_root = publication_root.clone();
        os::run_blocking_namespace_operation(mutation_lease, move || {
            if create_source_parent {
                std::fs::create_dir_all(&source_parent)?;
            }
            let guard = match os::prepare_rename_commit_guard(&source_parent, &destination_parent, &base_dir, &publication_root) {
                Ok(guard) => guard,
                Err(err) => {
                    return Err(match std::fs::symlink_metadata(&base_dir) {
                        Err(base_err) if base_err.kind() == ErrorKind::NotFound => base_err,
                        _ => err,
                    });
                }
            };
            #[cfg(all(test, windows))]
            run_destination_commit_directory_preparation(&destination_parent);
            let data = os::read_destination_file_with_commit_guard(&destination_metadata, &guard)?;
            Ok((guard, data.map(Bytes::from)))
        })
        .await
    };
    #[cfg(not(any(windows, target_os = "linux")))]
    let result = {
        let _ = mutation_lease;
        if create_source_parent {
            fs::create_dir_all(source_parent).await.map_err(to_file_error)?;
        }
        let guard =
            os::prepare_rename_commit_guard(source_parent, destination_parent, base_dir, publication_root).map_err(|err| {
                match std::fs::symlink_metadata(base_dir) {
                    Err(base_err) if base_err.kind() == ErrorKind::NotFound => base_err,
                    _ => err,
                }
            });
        match guard {
            Ok(guard) => match fs::read(destination_metadata).await {
                Ok(data) => Ok((guard, Some(Bytes::from(data)))),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok((guard, None)),
                Err(err) => Err(err),
            },
            Err(err) => Err(err),
        }
    };

    result.map_err(to_file_error).map_err(DiskError::from)
}

/// Proof produced only when the local rename returns at an existing access
/// preflight, before metadata, backups, or object data can be published.
#[derive(Debug)]
pub(in crate::disk) struct LocalRenamePreflightRejection(());

#[derive(Default)]
pub(super) struct RenameDataState {
    namespace_owner: Option<Arc<dyn Send + Sync>>,
    preflight_rejection: Option<LocalRenamePreflightRejection>,
}

// Keep optional native MPU state machines out of ordinary rename frames.
// Construct the future here as well as pinning it, so its temporary stays off
// the caller's poll stack. Call only from the existing native MPU branches.
#[inline(never)]
fn pin_multipart_future<F: std::future::Future>(build: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
    Box::pin(build())
}

impl LocalDisk {
    #[tracing::instrument(name = "rename_data", target = "rustfs_ecstore::disk::local", level = "trace", skip_all)]
    pub(super) async fn rename_data_inner(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
        state: &mut RenameDataState,
    ) -> Result<RenameDataResp> {
        super::multipart_staging::reject_noncanonical_scope(&self.root, self.io_root(), src_volume, src_path).inspect_err(
            |_| {
                state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
            },
        )?;
        super::multipart_staging::reject_noncanonical_scope(&self.root, self.io_root(), dst_volume, dst_path).inspect_err(
            |_| {
                state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
            },
        )?;
        super::pool_meta_native::reject_raw_path(&self.root, src_volume, src_path).inspect_err(|_| {
            state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
        })?;
        super::pool_meta_native::reject_noncanonical_object(&self.root, dst_volume, dst_path).inspect_err(|_| {
            state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
        })?;
        crate::hp_guard!("LocalDisk::rename_data");
        #[cfg(feature = "e2e-test-hooks")]
        if fi.is_healing() {
            wait_for_heal_commit_test_barrier(&self.root, dst_volume, dst_path).await?;
        }
        let mut fi = fi;
        let pool_meta_condition = if super::pool_meta_native::is_object(&self.root, dst_volume, dst_path) {
            Some(super::pool_meta_native::PoolMetaNativeCasCondition::take(&mut fi).inspect_err(|_| {
                state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
            })?)
        } else {
            None
        };
        let native_timing = pool_meta_condition.is_some();
        if pool_meta_condition
            .as_ref()
            .is_some_and(|condition| fi.data_dir == Some(condition.attempt_id))
        {
            state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
            return Err(DiskError::FileCorrupt);
        }
        if let Some(condition) = &pool_meta_condition {
            let new_identity = super::pool_meta_native::PoolMetaNativeFileIdentity::from_file_info(&fi)?;
            if condition
                .parent
                .as_ref()
                .is_some_and(|parent| new_identity.mod_time_unix_nanos <= parent.mod_time_unix_nanos)
            {
                state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
                return Err(DiskError::ConditionalDeleteConflict);
            }
        }
        // A non-force DeleteBucket must not remove a directory while a local
        // object commit is publishing into it. The peer's empty scan remains
        // optimistic; this lease establishes the local commit/delete order and
        // remains owned by any blocking syscall that outlives async cancellation.
        let (dst_volume_dir, destination_object_path) = self.io_get_bucket_and_object_path(dst_volume, dst_path)?;
        let quota_fence_token =
            match rustfs_utils::http::metadata_compat::get_consistent_str(&fi.metadata, QUOTA_MUTATION_FENCE_METADATA_SUFFIX) {
                Some(value) => {
                    let token = Uuid::parse_str(value).map_err(|_| DiskError::FileCorrupt)?;
                    Some(SnapshotLeaseToken::from_slice(token.as_bytes())?)
                }
                None if rustfs_utils::http::metadata_compat::contains_key_str(
                    &fi.metadata,
                    QUOTA_MUTATION_FENCE_METADATA_SUFFIX,
                ) =>
                {
                    return Err(DiskError::FileCorrupt);
                }
                None => None,
            };
        rustfs_utils::http::metadata_compat::remove_str(&mut fi.metadata, QUOTA_MUTATION_FENCE_METADATA_SUFFIX);
        let quota_fence_claim = match quota_fence_token {
            Some(token) => Some(self.claim_quota_mutation_fence(dst_volume, dst_path, token).await?),
            None => None,
        };
        // A failed fanout can undo the destination after this disk succeeded.
        // Keep the original upload authoritative until coordinator quorum: only
        // this private attempt's links and generated metadata may be consumed.
        let multipart_staging = if src_volume == super::super::RUSTFS_META_MULTIPART_BUCKET {
            Some(
                pin_multipart_future(|| {
                    self.prepare_multipart_commit_staging(src_path, &fi, dst_volume, state.namespace_owner.clone())
                })
                .await?,
            )
        } else {
            None
        };
        let multipart_publication = if let Some(staging) = multipart_staging.as_ref() {
            Some(pin_multipart_future(|| self.prepare_native_multipart_publication(src_path, &mut fi, staging)).await?)
        } else {
            None
        };
        let (src_volume, src_path) = match multipart_staging.as_ref() {
            Some(staging) => (super::super::RUSTFS_META_TMP_BUCKET, staging.path.as_str()),
            None => (src_volume, src_path),
        };
        // Quota admission -> metadata RMW -> namespace/volume publication.
        // MPU completion remains parts-backed even for a zero-byte part; its
        // version-owned retirement anchor is published with that same DD.
        let no_inline = fi.data.is_none() && (fi.size > 0 || multipart_publication.is_some());
        let fi_healing = fi.is_healing();
        let staging_prefix = if fi_healing && !no_inline && src_volume == super::super::RUSTFS_META_TMP_BUCKET {
            Some(tmp_staging_mutation_path(&self.root, src_path)?)
        } else {
            None
        };
        let root_destination_object_path = self.root.join(
            destination_object_path
                .strip_prefix(self.io_root())
                .map_err(|_| DiskError::InvalidPath)?,
        );
        let metadata_wait = os::StageTimer::new(native_timing, "pool_meta_native_metadata_lease_wait");
        let metadata_owner: Option<Arc<dyn Send + Sync>> = match multipart_staging.as_ref() {
            Some(staging) => Some(Arc::new((state.namespace_owner.take(), staging.lease.clone()))),
            None => state.namespace_owner.take(),
        };
        let metadata_lease = os::acquire_metadata_mutation_lease(&root_destination_object_path, metadata_owner).await;
        drop(metadata_wait);
        #[cfg(unix)]
        let parent_receipt = if native_timing {
            self.pool_meta_durable_head.lock().take().filter(|receipt| {
                pool_meta_condition.as_ref().is_some_and(|condition| {
                    condition.parent_raw_sha256.is_none() && condition.parent.as_ref() == Some(&receipt.identity)
                })
            })
        } else {
            None
        };
        #[cfg(unix)]
        let mut receipt_mutation = native_timing
            .then(|| super::pool_meta_native::ReceiptMutationGuard::new(&self.pool_meta_durable_head, metadata_lease.clone()));
        let namespace_wait = os::StageTimer::new(native_timing, "pool_meta_native_namespace_lease_wait");
        let mutation_lease = os::acquire_rename_data_mutation_lease_with_owner(
            &self.root,
            dst_volume,
            &root_destination_object_path,
            staging_prefix.as_deref(),
            Some(metadata_lease),
        )
        .await;
        drop(namespace_wait);
        if let Some(claim) = quota_fence_claim {
            mutation_lease.attach_external_guard(claim);
        }
        if let Some(condition) = &pool_meta_condition {
            let _parent_check = os::StageTimer::new(native_timing, "pool_meta_native_parent_check");
            if let Err(err) = self.check_pool_meta_parent(condition, &destination_object_path).await {
                state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
                return Err(err);
            }
        }
        if fi.is_legacy_indexed_delete_marker() {
            fi.erasure.index = 0;
        }
        fi.validate_for_metadata_read()?;
        // Snapshot the destination part paths before `fi` is consumed below. These
        // are the descriptors a reader may hold for the version this call is about
        // to replace (backlog#1145); readers build the identical string in
        // `io_primitives`. An inline-data version has no parts and yields none.
        let invalidate_part_paths: Vec<String> = {
            let data_dir = fi.data_dir.unwrap_or_default();
            fi.parts
                .iter()
                .flat_map(|part| {
                    std::iter::once(format!("{dst_path}/{data_dir}/part.{}", part.number)).chain(
                        part.integrity
                            .as_ref()
                            .map(|integrity| format!("{dst_path}/{data_dir}/{}", integrity.file_name())),
                    )
                })
                .collect()
        };
        let src_volume_dir = self.io_get_bucket_path(src_volume)?;
        if !skip_access_checks(src_volume)
            && let Err(e) = super::super::fs::access_std(&src_volume_dir)
        {
            info!(
                target: "rustfs_ecstore::disk::local",
                event = EVENT_DISK_LOCAL_ACCESS_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                path = ?src_volume_dir,
                operation = "rename_data_src_access",
                error = %e,
                "Disk local access check failed"
            );
            state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        if !skip_access_checks(dst_volume)
            && let Err(e) = super::super::fs::access_std(&dst_volume_dir)
        {
            info!(
                target: "rustfs_ecstore::disk::local",
                event = EVENT_DISK_LOCAL_ACCESS_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                path = ?dst_volume_dir,
                operation = "rename_data_dst_access",
                error = %e,
                "Disk local access check failed"
            );
            state.preflight_rejection = Some(LocalRenamePreflightRejection(()));
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        // Native publication consumes its canonical head. The legacy path
        // keeps main's independent recovery authority and staging filename.
        let preserve_upload_metadata =
            multipart_publication.is_none() && src_volume == super::super::RUSTFS_META_MULTIPART_BUCKET;
        let source_metadata_name = if preserve_upload_metadata {
            super::super::MULTIPART_COMMIT_METADATA_FILE
        } else {
            STORAGE_FORMAT_FILE
        };
        let src_file_path = match multipart_publication.as_ref() {
            Some(publication) => publication.canonical_head(),
            None => self.io_get_object_path(src_volume, format!("{src_path}/{source_metadata_name}").as_str())?,
        };
        let dst_file_path = self.io_get_object_open_path(dst_volume, format!("{}/{}", dst_path, STORAGE_FORMAT_FILE).as_str())?;

        // data_dir path (only used by the non-inline commit branch)
        let has_data_dir_path = if no_inline && !fi.is_remote() {
            if let Some(data_dir) = fi
                .data_dir
                .map(|dir| rustfs_utils::path::retain_slash(dir.to_string().as_str()))
            {
                let src_data_path = self.io_get_object_path(
                    src_volume,
                    rustfs_utils::path::retain_slash(format!("{}/{}", src_path, data_dir).as_str()).as_str(),
                )?;
                let dst_data_path = self.io_get_object_path(
                    dst_volume,
                    rustfs_utils::path::retain_slash(format!("{}/{}", dst_path, data_dir).as_str()).as_str(),
                )?;

                Some((src_data_path, dst_data_path))
            } else {
                None
            }
        } else {
            None
        };

        if multipart_publication.is_none()
            && let Some((source_directory, target_directory)) = has_data_dir_path.as_ref()
        {
            // Reject imported provenance before any DD purge or publication.
            // Same-disk heal copies only an already verified local anchor.
            self.retain_owned_multipart_anchor(
                &fi,
                dst_volume,
                dst_path,
                source_directory,
                target_directory,
                mutation_lease.clone(),
            )
            .await?;
        }

        check_path_length(src_file_path.to_string_lossy().to_string().as_str())?;
        check_path_length(dst_file_path.to_string_lossy().to_string().as_str())?;

        // Resolved once for the whole commit so a concurrent configuration
        // change can never leave a single rename_data half-synced. The tier is
        // keyed on the destination volume: user data staged in scratch
        // namespaces follows the configured tier, while commits into
        // system-critical namespaces (IAM, config, bucket metadata) stay
        // pinned to strict.
        let durability = effective_durability(dst_volume);

        let src_file_parent = src_file_path
            .parent()
            .ok_or_else(|| DiskError::other("missing staged metadata parent"))?;
        let dst_file_parent = dst_file_path
            .parent()
            .ok_or_else(|| DiskError::other("missing object metadata parent"))?;
        // Acquire the common trees before reading destination metadata. On
        // Windows this pins the object directory identity across publication.
        // Multipart authority already created its source tree. Recreating it
        // here would resurrect staging retired before a delayed request arrived.
        let (rename_commit_guard, has_dst_buf) = prepare_rename_commit_and_destination_metadata(
            !no_inline && !preserve_upload_metadata,
            src_file_parent,
            dst_file_parent,
            &dst_file_path,
            &dst_volume_dir,
            &self.publication_root,
            mutation_lease.clone(),
        )
        .await?;
        let mut commit = RenameCommitContext {
            directory_guard: rename_commit_guard,
            publication_root: &self.publication_root,
            mutation_lease,
            source_metadata: &src_file_path,
            destination_metadata: &dst_file_path,
            source_volume_dir: &src_volume_dir,
            data_paths: has_data_dir_path.as_ref(),
        };

        if preserve_upload_metadata {
            let source_object_path = self.get_object_path(src_volume, src_path)?;
            let authority_owner: Arc<dyn Send + Sync> = if source_object_path == root_destination_object_path {
                commit.mutation_lease.clone()
            } else {
                os::acquire_metadata_mutation_lease(&source_object_path, Some(commit.mutation_lease.clone())).await
            };
            let authority = self.io_get_object_path(src_volume, &format!("{src_path}/{STORAGE_FORMAT_FILE}"))?;
            let recovery = self.io_get_object_path(
                src_volume,
                &format!("{src_path}/{}/{STORAGE_FORMAT_FILE}", super::super::MULTIPART_RECOVERY_DIR),
            )?;
            check_path_length(recovery.to_string_lossy().as_ref())?;
            os::rename_all_with_owner(
                &authority,
                &recovery,
                src_file_parent,
                commit.publication_root,
                Some(authority_owner.clone()),
            )
            .await?;
            // This is system metadata authority, so use its existing strict
            // durability policy even when the object payload is relaxed.
            if effective_durability(src_volume).syncs_commit_metadata() {
                os::fsync_dir_with_owner(recovery.parent().ok_or(DiskError::InvalidPath)?, Some(authority_owner.clone()))
                    .await
                    .map_err(to_file_error)?;
                os::fsync_dir_with_owner(src_file_parent, Some(authority_owner.clone()))
                    .await
                    .map_err(to_file_error)?;
            }
        }

        if no_inline {
            // Non-inline: read xl.meta, parse, write, rename data dir, rename xl.meta
            let mut xlmeta = FileMeta::new();
            // An existing dst xl.meta that fails to parse leaves `xlmeta` empty
            // and gets overwritten by the commit below (pre-existing behavior);
            // track that so the old-size observation reports unknown instead of
            // a false `Absent` (rustfs/backlog#1009).
            let mut dst_meta_unparsable = false;
            if let Some(dst_buf) = has_dst_buf.as_ref() {
                if FileMeta::is_xl2_v1_format(dst_buf)
                    && let Ok(nmeta) = FileMeta::load(dst_buf)
                {
                    xlmeta = nmeta
                } else {
                    dst_meta_unparsable = true;
                }
            }

            let mut skip_parent = dst_volume_dir.clone();
            if has_dst_buf.as_ref().is_some()
                && let Some(parent) = dst_file_path.parent()
            {
                skip_parent = parent.to_path_buf();
            }

            let version_id = fi.version_id.unwrap_or_default();
            let (old_current_size, old_current_source_checked, old_current_source) =
                observe_old_current(has_dst_buf.is_some(), dst_meta_unparsable, &xlmeta, dst_volume, dst_path, version_id);
            let has_old_data_dir = xlmeta.find_unshared_data_dir_for_version(Some(version_id));
            let rollback_data_dir = if let Some(condition) = &pool_meta_condition {
                condition.has_parent().then_some(condition.attempt_id)
            } else {
                has_old_data_dir.or_else(|| {
                    if has_dst_buf.is_some() && xlmeta.find_version(Some(version_id)).is_ok() {
                        Some(inline_metadata_rollback_dir(version_id, &xlmeta))
                    } else {
                        None
                    }
                })
            };
            if let Some(old_data_dir) = has_old_data_dir.as_ref() {
                let _ = xlmeta.data.remove_two(version_id, *old_data_dir);
            }
            xlmeta.add_version(fi)?;
            let version_signature = rename_data_versions_signature(&xlmeta);
            let new_dst_buf = xlmeta.marshal_msg()?;

            // This tmp xl.meta is renamed onto dst_file_path at the commit
            // point below, so only its contents must be durable before the
            // rename (SyncMode::FileOnly); the dst parent directory is fsynced
            // after the commit rename, and a crash before the rename means the
            // PUT was never acknowledged. A metadata commit: relaxed tiers
            // leave it to the page cache.
            if let Some(publication) = multipart_publication.as_ref() {
                pin_multipart_future(|| publication.prepare_metadata(&new_dst_buf, has_dst_buf.as_deref())).await?;
            }
            let tmp_meta_sync = if durability.syncs_commit_metadata() || multipart_publication.is_some() {
                SyncMode::FileOnly
            } else {
                SyncMode::None
            };
            // The tmp xl.meta write and the shard-file fdatasync are independent
            // (disjoint paths) and both only need to be durable before the commit
            // renames below, so run them concurrently to drop a blocking
            // round-trip from the PUT commit critical path (rustfs/backlog#922
            // step 2). The "contents durable -> rename -> dst dir fsync" ordering
            // is unchanged — both futures complete before any rename — which the
            // rename_data crash-consistency harness (backlog#935) exercises.
            //
            // Shard durability: once rename_data succeeds the write is
            // acknowledged, so data must not live only in the page cache.
            // Multipart parts were already synced during rename_part, so their
            // fdatasync here is a cheap no-op. A missing source dir is left for the
            // rename below to report through the existing rollback path. Payload
            // durability is kept by both strict and relaxed.
            let tmp_meta_write = {
                let src_file_path = multipart_publication
                    .as_ref()
                    .map_or_else(|| commit.source_metadata.to_path_buf(), |publication| publication.prepared_path.clone());
                let dst_file_path = dst_file_path.clone();
                let owned_directory_guard = commit.directory_guard.clone();
                let owned_mutation_lease = commit.mutation_lease.clone();
                async move {
                    os::run_blocking_namespace_operation(owned_mutation_lease, move || {
                        #[cfg(test)]
                        run_owned_file_write_before_open(&src_file_path);
                        let mut prepared_metadata_source = os::create_prepared_rename_source_with_commit_guard(
                            &src_file_path,
                            &dst_file_path,
                            &owned_directory_guard,
                        )?;
                        prepared_metadata_source.write_all(&new_dst_buf, tmp_meta_sync != SyncMode::None)?;
                        Ok(prepared_metadata_source)
                    })
                    .await
                    .map_err(to_file_error)
                    .map_err(DiskError::from)
                }
            };
            let shard_sync = async {
                if durability.syncs_data_shards()
                    && let Some((src_data_path, _)) = has_data_dir_path.as_ref()
                    && let Err(err) = os::sync_dir_files_with_limiter(src_data_path, self.file_sync_permits.clone()).await
                    && err.kind() != ErrorKind::NotFound
                {
                    return Err::<(), DiskError>(to_file_error(err).into());
                }
                Ok(())
            };
            let (tmp_meta_res, shard_sync_res) = tokio::join!(tmp_meta_write, shard_sync);
            // Surface a tmp-meta failure first (its prior serial position), then a
            // shard-sync failure; either aborts before any rename, exactly as the
            // sequential version did.
            let prepared_metadata_source = tmp_meta_res?;
            shard_sync_res?;
            let prepared_metadata_source = match multipart_publication.as_ref() {
                Some(publication) => pin_multipart_future(|| publication.install_head(prepared_metadata_source)).await?,
                None => prepared_metadata_source,
            };
            commit.directory_guard = remove_dst_base_before_commit(
                dst_path,
                commit.directory_guard,
                src_file_parent,
                dst_file_parent,
                &dst_volume_dir,
                commit.publication_root,
                commit.mutation_lease.clone(),
            )
            .await?;
            if should_remove_staged_meta_before_commit(dst_path) {
                drop(prepared_metadata_source);
                std::fs::remove_file(&src_file_path).map_err(to_file_error)?;
                return Err(DiskError::FileNotFound);
            }

            // Heal reuses the version's data_dir, so for in-place corruption
            // the destination dir still exists — and rename(2) cannot replace
            // a non-empty directory (EEXIST on XFS, ENOTEMPTY on ext4). Purge
            // it first, healing commits only; fresh PUTs mint a new data_dir
            // and never collide. Best effort: a real failure surfaces in the
            // rename below.
            if fi_healing
                && let Some((_, dst_data_path)) = has_data_dir_path.as_ref()
                && let Err(err) = self
                    .move_to_trash_with_namespace_owner(dst_data_path, true, false, Some(commit.mutation_lease.clone()))
                    .await
            {
                warn!(
                    target: "rustfs_ecstore::disk::local",
                    event = EVENT_DISK_LOCAL_HEAL_PURGE_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    dst_path = ?dst_data_path,
                    error = ?err,
                    "Healing commit could not purge the stale destination data dir"
                );
            }
            if let Some((src_data_path, dst_data_path)) = has_data_dir_path.as_ref() {
                #[cfg(target_os = "linux")]
                let payload_guard = if multipart_publication.is_some() {
                    Some(
                        os::multipart_payload_commit_guard(
                            &commit.directory_guard,
                            src_data_path.parent().ok_or(DiskError::InvalidPath)?,
                            commit.mutation_lease.clone(),
                        )
                        .await
                        .map_err(to_file_error)?,
                    )
                } else {
                    None
                };
                #[cfg(target_os = "linux")]
                let directory_guard = payload_guard.as_ref().unwrap_or(&commit.directory_guard);
                #[cfg(not(target_os = "linux"))]
                let directory_guard = &commit.directory_guard;
                if let Err(err) = os::rename_all_with_commit_guard(
                    src_data_path,
                    dst_data_path,
                    &skip_parent,
                    commit.publication_root,
                    directory_guard,
                    commit.mutation_lease.clone(),
                )
                .await
                {
                    info!(
                        target: "rustfs_ecstore::disk::local",
                        event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        reason = "rename_all_data_path_failed",
                        src_path = ?src_data_path,
                        dst_path = ?dst_data_path,
                        error = ?err,
                        "Disk local rename flow failed"
                    );
                    commit.restore_published_data_source().await?;
                    return Err(err);
                }
            }
            #[cfg(test)]
            if has_data_dir_path.is_some() {
                run_rename_data_after_first_publication(&self.root, dst_volume, dst_path);
            }

            // Crash-consistency injection: hard power loss after the data dir
            // is in place but before xl.meta commits. No cleanup — the harness
            // reopens the disk and asserts the object still reads as the old
            // version (the staged data dir is a harmless orphan for GC).
            if crash_inject::should_crash_at(CrashPoint::RenameAfterDataRename, dst_path) {
                return Err(DiskError::Unexpected);
            }

            if should_fail_before_old_metadata_backup(dst_path) {
                info!(
                    target: "rustfs_ecstore::disk::local",
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "test_fail_before_old_metadata_backup",
                    "Disk local rename flow failed before metadata commit"
                );
                commit.restore_published_data_source().await?;
                return Err(DiskError::Unexpected);
            }

            // The rollback backup stays where it is written (no rename) and is
            // the sole restore source for a later undo_write, so under strict
            // it keeps SyncMode::FileAndDir: contents and directory entry both
            // durable. It is part of the metadata commit machinery, so relaxed
            // tiers leave it to the page cache like the xl.meta it mirrors.
            let backup_sync = if durability.syncs_commit_metadata() {
                SyncMode::FileAndDir
            } else {
                SyncMode::None
            };
            if let (Some(old_data_dir), Some(dst_buf)) = (rollback_data_dir, has_dst_buf.as_ref())
                && multipart_publication.as_ref().is_none_or(|publication| publication.legacy)
            {
                let backup_parent = dst_file_parent.join(old_data_dir.to_string());
                #[cfg(not(windows))]
                if let Err(err) = os::make_dir_all(&backup_parent, &skip_parent).await {
                    commit.restore_published_data_source().await?;
                    return Err(err);
                }
                let backup_path_guard = match commit
                    .directory_guard
                    .create_destination_directory_for_path_access(&backup_parent)
                {
                    Ok(guard) => guard,
                    Err(err) => {
                        commit.restore_published_data_source().await?;
                        return Err(DiskError::from(to_file_error(err)));
                    }
                };
                let backup_path = backup_parent.join(STORAGE_FORMAT_FILE_BACKUP);
                if let Err(err) = check_path_length(backup_path.to_string_lossy().as_ref()) {
                    #[cfg(windows)]
                    drop(backup_path_guard);
                    commit.restore_published_data_source().await?;
                    return Err(err);
                }
                let backup_bytes = dst_buf.clone();
                // Keep the volume, commit-tree, and exact destination-path
                // guards in this task until the backup write and durability
                // sync finish. A detached spawn_blocking writer could survive
                // cancellation and later truncate a newer transaction's
                // deterministic rollback backup.
                let write_result = os::run_blocking_namespace_operation(commit.mutation_lease.clone(), move || {
                    #[cfg(test)]
                    run_owned_file_write_before_open(&backup_path);
                    backup_path_guard.write_file_for_path_access(
                        &backup_path,
                        backup_bytes.as_ref(),
                        backup_sync != SyncMode::None,
                        backup_sync == SyncMode::FileAndDir,
                    )
                })
                .await
                .map_err(to_file_error)
                .map_err(DiskError::from);
                if let Err(err) = write_result {
                    info!(
                        target: "rustfs_ecstore::disk::local",
                        event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        reason = "write_old_metadata_backup_failed",
                        error = ?err,
                        "Disk local rename flow failed"
                    );
                    commit.restore_published_data_source().await?;
                    return Err(err);
                }
            }

            // Crash-consistency injection: hard power loss after the rollback
            // backup is durable but before the xl.meta commit rename. No
            // cleanup — the harness asserts the object still reads as the old
            // version, since the destination xl.meta is untouched here.
            if crash_inject::should_crash_at(CrashPoint::RenameAfterBackupBeforeMetaCommit, dst_path) {
                return Err(DiskError::Unexpected);
            }

            if let Err(err) = os::rename_all_with_prepared_source(
                prepared_metadata_source,
                &src_file_path,
                &dst_file_path,
                &skip_parent,
                commit.publication_root,
                &commit.directory_guard,
                commit.mutation_lease.clone(),
            )
            .await
            {
                info!(
                    target: "rustfs_ecstore::disk::local",
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "rename_all_metadata_failed",
                    src_path = ?src_file_path,
                    dst_path = ?dst_file_path,
                    error = ?err,
                    "Disk local rename flow failed"
                );
                commit.restore_published_data_source().await?;
                return Err(err);
            }
            if should_fail_after_metadata_commit(dst_path) {
                if let Some(publication) = multipart_publication.as_ref() {
                    pin_multipart_future(|| publication.restore_target_after_error(commit.mutation_lease.clone())).await?;
                } else {
                    commit.rollback_non_inline_std(rollback_data_dir).map_err(to_file_error)?;
                }
                return Err(DiskError::Unexpected);
            }

            // Crash-consistency injection: hard power loss immediately after the
            // xl.meta commit rename but before the durability fsync. Unlike the
            // graceful failpoint above, no rollback runs — the commit rename is
            // already on disk, so the harness asserts the object reads back as
            // the new version.
            if crash_inject::should_crash_at(CrashPoint::RenameAfterMetaCommit, dst_path) {
                return Err(DiskError::Unexpected);
            }

            // Persist the directory entries for both the data dir and xl.meta renames;
            // without this the commit itself can vanish on power loss. Relaxed tiers
            // accept that window (documented in docs/operations/durability-modes.md).
            if durability.syncs_commit_metadata()
                && let Some(parent) = dst_file_path.parent()
            {
                let fsync_started = rustfs_io_metrics::put_stage_timer();
                if let Err(err) =
                    os::fsync_commit_directory(parent, &commit.directory_guard, commit.mutation_lease.clone(), None).await
                {
                    rustfs_io_metrics::record_put_object_stage_duration_from(
                        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC,
                        fsync_started,
                    );
                    if let Some(publication) = multipart_publication.as_ref() {
                        pin_multipart_future(|| publication.restore_target_after_error(commit.mutation_lease.clone())).await?;
                    } else {
                        commit.rollback_non_inline_std(rollback_data_dir).map_err(to_file_error)?;
                    }
                    // The commit rename changed the dst part inodes before this fsync
                    // failed and rolled them back; drop any fd cached during that
                    // window so readers re-open the restored inode (rustfs/backlog#1177).
                    for part_path in &invalidate_part_paths {
                        self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
                    }
                    return Err(to_file_error(err).into());
                }
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC,
                    fsync_started,
                );
            }

            // First PUT of an object creates its directory (and any missing prefix
            // dirs) via reliable_mkdir_all, which never fsyncs the parent chain. The
            // commit fsync above persists the object dir's *contents*, not its own
            // entry in the bucket/prefix dir, so on power loss after ack the whole
            // object dir could vanish (rustfs/backlog#922 step 4). For a new object
            // (no prior xl.meta) fsync the ancestor chain from the object dir's
            // parent up to and including the bucket so those new directory entries
            // are durable. Overwrites already have a durable object dir. The
            // starts_with guard bounds the walk to the bucket subtree. Relaxed/none
            // accept the wider window, like the commit fsync above.
            if has_dst_buf.is_none() && durability.syncs_commit_metadata() {
                let mut ancestor = dst_file_path.parent().and_then(|object_dir| object_dir.parent());
                while let Some(dir) = ancestor {
                    if !dir.starts_with(&dst_volume_dir) {
                        break;
                    }
                    let fsync_started = rustfs_io_metrics::put_stage_timer();
                    if let Err(err) = os::fsync_dir_with_owner(dir, Some(commit.mutation_lease.clone())).await {
                        rustfs_io_metrics::record_put_object_stage_duration_from(
                            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC,
                            fsync_started,
                        );
                        if let Some(publication) = multipart_publication.as_ref() {
                            pin_multipart_future(|| publication.restore_target_after_error(commit.mutation_lease.clone()))
                                .await?;
                        } else {
                            commit.rollback_non_inline_std(rollback_data_dir).map_err(to_file_error)?;
                        }
                        // Same post-commit rollback window as above — drop cached
                        // dst part fds so readers re-open the restored inode
                        // (rustfs/backlog#1177).
                        for part_path in &invalidate_part_paths {
                            self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
                        }
                        return Err(to_file_error(err).into());
                    }
                    rustfs_io_metrics::record_put_object_stage_duration_from(
                        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC,
                        fsync_started,
                    );
                    if dir == dst_volume_dir.as_path() {
                        break;
                    }
                    ancestor = dir.parent();
                }
            }

            // Publication and every rollback-capable durability step are now
            // complete. Do not retain the Windows object identity guard while
            // cleaning staging paths or invalidating cached descriptors.
            #[cfg(windows)]
            drop(commit.directory_guard);

            if multipart_publication.is_none()
                && !preserve_upload_metadata
                && let Some(parent) = src_file_path.parent()
            {
                let _ = std::fs::remove_dir(parent);
            }

            // Heal reuses a version's `data_dir` and lands the rebuilt shard on
            // the SAME `<object>/<data_dir>/part.N` path. Without this, a cached
            // descriptor would keep serving the pre-heal inode, defeating the heal
            // and eroding read quorum (backlog#1145).
            //
            // The exact keys are derivable here, and this runs on every write, so
            // use them rather than registering a predicate the read path would then
            // have to evaluate. Readers build the same string
            // (`{object}/{data_dir}/part.{n}`), and `fi.parts` enumerates every
            // part of the version now at `dst_path` — any part path absent from it
            // no longer exists for readers to ask for.
            for part_path in &invalidate_part_paths {
                self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
            }

            let multipart_source = match multipart_publication.as_ref() {
                Some(publication) => match pin_multipart_future(|| publication.finish(commit.mutation_lease.clone())).await {
                    Ok(result) => Some(Box::new(result)),
                    Err(error) => {
                        pin_multipart_future(|| publication.restore_target_after_error(commit.mutation_lease.clone())).await?;
                        return Err(error);
                    }
                },
                None => None,
            };
            Ok(RenameDataResp {
                multipart_source,
                old_data_dir: has_old_data_dir,
                rollback_data_dir: if multipart_publication.as_ref().is_some_and(|publication| !publication.legacy) {
                    None
                } else {
                    rollback_data_dir
                },
                cleanup_data_dir: has_old_data_dir,
                sign: version_signature,
                old_current_size,
                old_current_source_checked,
                old_current_source,
            })
        } else {
            // Inline metadata preparation is blocking. The transaction lease is
            // moved into that work so a timeout can release the async waiter without
            // allowing a retry to reuse the deterministic staging path too early.
            let src = src_file_path.clone();
            let dst = dst_file_path.clone();
            let dst_path_for_failpoint = dst_path.to_string();
            let dst_volume_for_source = dst_volume.to_string();
            let dst_path_for_source = dst_path.to_string();
            let dst_volume_dir_for_commit = dst_volume_dir.clone();
            let publication_root_for_commit = self.publication_root.clone();
            let cleanup_src_parent = multipart_publication.is_none() && !preserve_upload_metadata;
            #[cfg(windows)]
            let source_parent = src_file_parent.to_path_buf();
            let rename_commit_guard_for_preparation = commit.directory_guard.clone();
            let sync = durability.syncs_commit_metadata();
            #[cfg(unix)]
            let receipt_identity = if sync
                && pool_meta_condition
                    .as_ref()
                    .is_some_and(|condition| condition.parent_raw_sha256.is_none())
            {
                Some(super::pool_meta_native::PoolMetaNativeFileIdentity::from_file_info(&fi)?)
            } else {
                None
            };
            #[cfg(unix)]
            let owned_backup_path = pool_meta_condition
                .as_ref()
                .filter(|condition| condition.has_parent())
                .map(|condition| {
                    destination_object_path
                        .join(condition.attempt_id.to_string())
                        .join(STORAGE_FORMAT_FILE_BACKUP)
                });
            #[cfg(unix)]
            if let Some(backup) = &owned_backup_path {
                let parent = backup
                    .parent()
                    .ok_or_else(|| DiskError::other("missing owned backup parent"))?;
                commit.mutation_lease =
                    os::acquire_namespace_mutation_lease_with_owner(parent, Some(commit.mutation_lease.clone())).await;
            }
            #[cfg(test)]
            run_inline_before_file_sync_admission(dst_path);
            let mut file_sync_admission = if sync {
                let _admission = os::StageTimer::new(native_timing, "pool_meta_native_file_sync_admission");
                Some(
                    os::acquire_file_sync_admission(self.file_sync_permits.clone())
                        .await
                        .map_err(to_file_error)
                        .map_err(DiskError::from)?,
                )
            } else {
                None
            };
            let preparation_dispatch = os::StageTimer::new(native_timing, "pool_meta_native_prepare_dispatch_wait");
            let prepare_inline_metadata = move |is_cancelled: &dyn Fn() -> bool| {
                drop(preparation_dispatch);
                let _preparation = os::StageTimer::new(native_timing, "pool_meta_native_prepare_execute");
                let mut prepared_metadata_source =
                    os::create_prepared_rename_source_with_commit_guard(&src, &dst, &rename_commit_guard_for_preparation)?;
                #[cfg(windows)]
                let source_metadata_guard =
                    rename_commit_guard_for_preparation.lock_source_directory_for_path_access(&source_parent)?;
                let mut xlmeta = FileMeta::new();
                // Same as the non-inline branch: an unparsable existing dst
                // xl.meta must surface as unknown, not `Absent`
                // (rustfs/backlog#1009).
                let mut dst_meta_unparsable = false;
                if let Some(ref buf) = has_dst_buf {
                    if FileMeta::is_xl2_v1_format(buf)
                        && let Ok(nmeta) = FileMeta::load(buf)
                    {
                        xlmeta = nmeta
                    } else {
                        dst_meta_unparsable = true;
                    }
                }

                let version_id = fi.version_id.unwrap_or_default();
                let (old_current_size, old_current_source_checked, old_current_source) = observe_old_current(
                    has_dst_buf.is_some(),
                    dst_meta_unparsable,
                    &xlmeta,
                    &dst_volume_for_source,
                    &dst_path_for_source,
                    version_id,
                );
                let old_data_dir = xlmeta.find_unshared_data_dir_for_version(Some(version_id));
                // Healing commits never run set-level post-return rollback; keep
                // their intra-commit rollback backup in the staging directory so
                // `<object>/<rollback_data_dir>/xl.meta.bkp` is never stranded.
                let rollback_data_dir = if let Some(condition) = &pool_meta_condition {
                    condition.has_parent().then_some(condition.attempt_id)
                } else if fi_healing {
                    None
                } else {
                    old_data_dir.or_else(|| {
                        if has_dst_buf.is_some() && xlmeta.find_version(Some(version_id)).is_ok() {
                            Some(inline_metadata_rollback_dir(version_id, &xlmeta))
                        } else {
                            None
                        }
                    })
                };
                let mut staged_rollback_path = None;
                if let Some(d) = old_data_dir.as_ref() {
                    let _ = xlmeta.data.remove_two(version_id, *d);
                }
                xlmeta.add_version(fi)?;
                let version_signature = rename_data_versions_signature(&xlmeta);
                let new_buf = xlmeta.marshal_msg()?;
                // Write the staged xl.meta. Inline objects carry their data inside
                // xl.meta, so this is the durable preparation for the metadata commit:
                // relaxed tiers do no per-object fsync here at all (aligned
                // with MinIO's default), trading a documented power-loss
                // window for latency.
                {
                    let _new_file = os::StageTimer::new(native_timing, "pool_meta_native_staged_write_sync");
                    prepared_metadata_source.write_all(&new_buf, sync)?;
                }
                run_inline_preparation_before_backup(&dst_path_for_failpoint);
                #[cfg(unix)]
                let mut receipt_candidate = receipt_identity.and_then(|identity| {
                    os::DurableFileReceipt::capture(&publication_root_for_commit, &src, &new_buf)
                        .ok()
                        .map(|physical| super::pool_meta_native::DurableHeadReceipt { physical, identity })
                });
                #[cfg(not(unix))]
                let receipt_candidate = None;
                #[cfg(unix)]
                let owned_backup = match (owned_backup_path.as_ref(), has_dst_buf.as_ref()) {
                    (Some(backup_path), Some(old_metadata)) => {
                        let backup = {
                            let _create = os::StageTimer::new(native_timing, "pool_meta_native_backup_create");
                            Arc::new(os::prepare_pool_meta_owned_backup(
                                &dst,
                                backup_path,
                                &dst_volume_dir_for_commit,
                                &publication_root_for_commit,
                                old_metadata,
                                should_fail_local_inline_rollback_hardlink(&dst),
                            )?)
                        };
                        let reuses_durability = parent_receipt.as_ref().is_some_and(|receipt| {
                            backup.can_reuse_durability(&receipt.physical, &publication_root_for_commit, old_metadata)
                        });
                        if !backup.is_linked() {
                            receipt_candidate = None;
                        }
                        if sync && !reuses_durability {
                            let _backup_sync = os::StageTimer::new(native_timing, "pool_meta_native_backup_file_sync");
                            backup.sync_data()?;
                        } else if sync {
                            rustfs_io_metrics::record_put_object_stage_count("pool_meta_native_backup_sync_reused", 1);
                        }
                        Some(backup)
                    }
                    (Some(_), None) => {
                        return Err(std::io::Error::new(ErrorKind::InvalidData, "pool metadata predecessor disappeared"));
                    }
                    (None, _) => None,
                };
                #[cfg(not(unix))]
                let owned_backup = None;
                if let Some(ref old_metadata) = has_dst_buf
                    && owned_backup.is_none()
                    && (rollback_data_dir.is_some() || fi_healing || sync || cfg!(test))
                {
                    #[cfg(windows)]
                    let backup_path = {
                        let _backup = os::StageTimer::new(native_timing, "pool_meta_native_backup_create_sync");
                        let backup_path = src
                            .parent()
                            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "missing staging metadata parent"))?
                            .join(STORAGE_FORMAT_FILE_BACKUP);
                        source_metadata_guard.write_file_for_path_access(&backup_path, old_metadata, sync, false)?;
                        backup_path
                    };
                    #[cfg(not(windows))]
                    let backup_path = {
                        let _backup = os::StageTimer::new(native_timing, "pool_meta_native_backup_create");
                        create_local_inline_rollback_backup(&dst, &src, old_metadata)?
                    };
                    #[cfg(not(windows))]
                    if sync {
                        let _backup_sync = os::StageTimer::new(native_timing, "pool_meta_native_backup_file_sync");
                        std::fs::File::open(&backup_path)?.sync_data()?;
                    }
                    staged_rollback_path = Some(StagedRollbackBackup(backup_path));
                }

                let destination_was_absent = has_dst_buf.is_none();
                if !is_cancelled()
                    && rollback_data_dir.is_none()
                    && !os::dst_dir_fsync_group_commit_enabled()
                    && !has_inline_commit_test_failpoints(&dst_path_for_failpoint)
                {
                    let rename_res = if should_fail_commit_rename(&dst_path_for_failpoint) {
                        drop(prepared_metadata_source);
                        Err(DiskError::other("test fail during metadata commit rename"))
                    } else {
                        os::rename_prepared_source_on_thread(
                            prepared_metadata_source,
                            &src,
                            &dst,
                            &dst_volume_dir_for_commit,
                            &publication_root_for_commit,
                            &rename_commit_guard_for_preparation,
                        )
                    };
                    if let Err(err) = rename_res {
                        if let Some(backup_path) = staged_rollback_path.as_ref().map(StagedRollbackBackup::path) {
                            let _ = remove_file_if_exists(backup_path);
                        }
                        return Ok(InlinePreparationOutcome::CommitErr(err));
                    }

                    if should_fail_after_metadata_commit(&dst_path_for_failpoint) {
                        let err = rollback_inline_metadata_std(&dst, None, staged_rollback_path.as_ref())
                            .err()
                            .unwrap_or_else(|| std::io::Error::other("test fail after metadata commit"));
                        return Ok(InlinePreparationOutcome::CommitErr(DiskError::from(err)));
                    }

                    if sync
                        && let Err(err) = fsync_inline_commit_std(
                            &dst,
                            &dst_volume_dir_for_commit,
                            destination_was_absent,
                            staged_rollback_path.as_ref(),
                            &rename_commit_guard_for_preparation,
                        )
                    {
                        return Ok(InlinePreparationOutcome::CommitErr(DiskError::from(err)));
                    }

                    if let Some(backup_path) = staged_rollback_path.as_ref().map(StagedRollbackBackup::path) {
                        let _ = remove_file_if_exists(backup_path);
                    }
                    #[cfg(not(windows))]
                    if cleanup_src_parent && let Some(parent) = src.parent() {
                        let _ = std::fs::remove_dir(parent);
                    }

                    return Ok::<_, std::io::Error>(InlinePreparationOutcome::Committed(
                        RenameDataResp {
                            multipart_source: None,
                            old_data_dir,
                            rollback_data_dir,
                            cleanup_data_dir: old_data_dir,
                            sign: version_signature,
                            old_current_size,
                            old_current_source_checked,
                            old_current_source,
                        },
                        receipt_candidate,
                    ));
                }

                Ok::<_, std::io::Error>(InlinePreparationOutcome::Prepared((
                    rollback_data_dir,
                    old_data_dir,
                    version_signature,
                    old_current_size,
                    old_current_source_checked,
                    old_current_source,
                    staged_rollback_path,
                    destination_was_absent,
                    prepared_metadata_source,
                    owned_backup,
                    receipt_candidate,
                )))
            };
            let preparation_total = os::StageTimer::new(native_timing, "pool_meta_native_prepare_total");
            let inline_preparation = if let Some(admission) = file_sync_admission.as_ref() {
                os::run_cancellable_blocking_namespace_file_sync_operation(
                    commit.mutation_lease.clone(),
                    admission,
                    prepare_inline_metadata,
                )
                .await
            } else {
                os::run_cancellable_blocking_namespace_operation(commit.mutation_lease.clone(), prepare_inline_metadata).await
            }
            .map_err(to_file_error)
            .map_err(DiskError::from);
            drop(preparation_total);
            let (
                rollback_data_dir,
                cleanup_data_dir,
                version_signature,
                old_current_size,
                old_current_source_checked,
                old_current_source,
                mut local_rollback_path,
                destination_was_absent,
                prepared_metadata_source,
                owned_backup,
                receipt_candidate,
            ) = match inline_preparation {
                Ok(InlinePreparationOutcome::Committed(resp, receipt_candidate)) => {
                    drop(file_sync_admission.take());
                    #[cfg(unix)]
                    self.publish_pool_meta_durable_head(receipt_candidate, dst_file_path.clone(), commit.mutation_lease.clone())
                        .await;
                    #[cfg(not(unix))]
                    let _ = receipt_candidate;
                    #[cfg(windows)]
                    drop(commit.directory_guard);
                    #[cfg(windows)]
                    if cleanup_src_parent && let Some(parent) = src_file_path.parent() {
                        let _ = std::fs::remove_dir(parent);
                    }
                    for part_path in &invalidate_part_paths {
                        self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
                    }
                    #[cfg(unix)]
                    if let Some(receipt_mutation) = receipt_mutation.as_mut() {
                        receipt_mutation.complete();
                    }
                    return Ok(resp);
                }
                Ok(InlinePreparationOutcome::CommitErr(err)) | Err(err) => {
                    drop(file_sync_admission.take());
                    for part_path in &invalidate_part_paths {
                        self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
                    }
                    return Err(err);
                }
                Ok(InlinePreparationOutcome::Prepared(prepared)) => prepared,
            };

            commit.directory_guard = remove_dst_base_before_commit(
                dst_path,
                commit.directory_guard,
                src_file_parent,
                dst_file_parent,
                &dst_volume_dir,
                commit.publication_root,
                commit.mutation_lease.clone(),
            )
            .await?;

            if should_remove_staged_meta_before_commit(dst_path) {
                drop(prepared_metadata_source);
                let remove_result = std::fs::remove_file(&src_file_path);
                if let Some(backup_path) = local_rollback_path.as_ref().map(StagedRollbackBackup::path) {
                    let _ = remove_file_if_exists(backup_path);
                }
                remove_result.map_err(to_file_error)?;
                return Err(DiskError::FileNotFound);
            }

            #[cfg(unix)]
            if let Some(backup) = &owned_backup {
                // The backup namespace lease owns the head transaction; retain
                // its directory and inode guards in every later detached job.
                commit.mutation_lease.attach_external_guard(backup.clone());
                #[cfg(test)]
                run_rename_data_after_first_publication(&self.root, dst_volume, dst_path);
                let backup_base = dst_volume_dir.clone();
                let backup_root = commit.publication_root.clone();
                if let Some(admission) = file_sync_admission.as_ref() {
                    let _backup_dir = os::StageTimer::new(native_timing, "pool_meta_native_backup_dir_sync");
                    let fsync_started = rustfs_io_metrics::put_stage_timer();
                    let backup = backup.clone();
                    let result =
                        os::run_blocking_namespace_file_sync_operation(commit.mutation_lease.clone(), admission, move || {
                            backup.verify_paths(&backup_base, &backup_root)?;
                            backup.sync_directory()
                        })
                        .await;
                    rustfs_io_metrics::record_put_object_stage_duration_from(
                        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_BACKUP_DIR_FSYNC,
                        fsync_started,
                    );
                    result.map_err(to_file_error)?;
                } else {
                    let backup = backup.clone();
                    os::run_blocking_namespace_operation(commit.mutation_lease.clone(), move || {
                        backup.verify_paths(&backup_base, &backup_root)
                    })
                    .await
                    .map_err(to_file_error)?;
                }
            }
            #[cfg(not(unix))]
            let _ = owned_backup;

            if let (Some(rollback_data_dir), Some(staged_backup)) =
                (rollback_data_dir, local_rollback_path.as_ref().map(StagedRollbackBackup::path))
            {
                let Some(dst_parent) = dst_file_path.parent() else {
                    return Err(DiskError::other("missing object metadata parent"));
                };
                let backup_path = dst_parent
                    .join(rollback_data_dir.to_string())
                    .join(STORAGE_FORMAT_FILE_BACKUP);
                // rename_all acquires the backup path's namespace lease. Do not
                // hold a disk admission while acquiring another namespace lock.
                drop(file_sync_admission.take());
                let backup_publish = os::StageTimer::new(native_timing, "pool_meta_native_backup_publish");
                let backup_result = os::rename_all_with_owner(
                    staged_backup,
                    &backup_path,
                    &dst_volume_dir,
                    commit.publication_root,
                    Some(commit.mutation_lease.clone()),
                )
                .await;
                drop(backup_publish);
                if let Err(err) = backup_result {
                    let _ = remove_file_if_exists(staged_backup);
                    return Err(err);
                }
                #[cfg(test)]
                run_rename_data_after_first_publication(&self.root, dst_volume, dst_path);
                if sync {
                    let _admission = os::StageTimer::new(native_timing, "pool_meta_native_file_sync_readmission");
                    file_sync_admission = Some(
                        os::acquire_file_sync_admission(self.file_sync_permits.clone())
                            .await
                            .map_err(to_file_error)
                            .map_err(DiskError::from)?,
                    );
                }
                if let Some(admission) = file_sync_admission.as_ref()
                    && let Some(backup_parent) = backup_path.parent()
                {
                    let _backup_dir = os::StageTimer::new(native_timing, "pool_meta_native_backup_dir_sync");
                    let fsync_started = rustfs_io_metrics::put_stage_timer();
                    if let Err(err) =
                        os::fsync_dir_with_namespace_file_sync_limit(backup_parent, commit.mutation_lease.clone(), admission)
                            .await
                    {
                        rustfs_io_metrics::record_put_object_stage_duration_from(
                            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_BACKUP_DIR_FSYNC,
                            fsync_started,
                        );
                        return Err(DiskError::from(to_file_error(err)));
                    }
                    rustfs_io_metrics::record_put_object_stage_duration_from(
                        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_BACKUP_DIR_FSYNC,
                        fsync_started,
                    );
                }
                local_rollback_path = None;
            }

            let head_publish = os::StageTimer::new(native_timing, "pool_meta_native_head_publish");
            let commit_result = if should_fail_commit_rename(dst_path) {
                Err(DiskError::other("test fail during metadata commit rename"))
            } else {
                os::rename_all_with_prepared_source(
                    prepared_metadata_source,
                    &src_file_path,
                    &dst_file_path,
                    &dst_volume_dir,
                    commit.publication_root,
                    &commit.directory_guard,
                    commit.mutation_lease.clone(),
                )
                .await
            };
            drop(head_publish);
            if let Err(err) = commit_result {
                if let Some(backup_path) = local_rollback_path.as_ref().map(StagedRollbackBackup::path) {
                    let _ = remove_file_if_exists(backup_path);
                }
                for part_path in &invalidate_part_paths {
                    self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
                }
                return Err(err);
            }

            let post_commit = async {
                if should_fail_after_metadata_commit(dst_path) {
                    commit.rollback_inline_std(rollback_data_dir, local_rollback_path.as_ref())?;
                    return Err(std::io::Error::other("test fail after metadata commit"));
                }

                // Persist the commit rename's directory entry across power loss.
                if let Some(admission) = file_sync_admission.as_ref()
                    && let Some(dst_parent) = dst_file_path.parent()
                {
                    let _head_dir = os::StageTimer::new(native_timing, "pool_meta_native_head_dir_sync");
                    let fsync_started = rustfs_io_metrics::put_stage_timer();
                    if let Err(err) = os::fsync_commit_directory(
                        dst_parent,
                        &commit.directory_guard,
                        commit.mutation_lease.clone(),
                        Some(admission),
                    )
                    .await
                    {
                        rustfs_io_metrics::record_put_object_stage_duration_from(
                            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC,
                            fsync_started,
                        );
                        commit.rollback_inline_std(rollback_data_dir, local_rollback_path.as_ref())?;
                        return Err(err);
                    }
                    rustfs_io_metrics::record_put_object_stage_duration_from(
                        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC,
                        fsync_started,
                    );
                }

                // Same power-loss gap as the non-inline path (rustfs/backlog#922
                // step 4): a first PUT creates the object dir (and any missing
                // prefix dirs) whose entry in the bucket/prefix dir reliable_mkdir_all
                // never fsynced. The fsync above persists the object dir's contents,
                // not its own entry, so for a new inline object fsync the ancestor
                // chain up to and including the bucket. Overwrites already have a
                // durable object dir; the starts_with guard bounds the walk.
                if let Some(admission) = file_sync_admission.as_ref()
                    && destination_was_absent
                {
                    let mut ancestor = dst_file_path.parent().and_then(|object_dir| object_dir.parent());
                    while let Some(ancestor_dir) = ancestor {
                        if !ancestor_dir.starts_with(&dst_volume_dir) {
                            break;
                        }
                        let fsync_started = rustfs_io_metrics::put_stage_timer();
                        if let Err(err) =
                            os::fsync_dir_with_namespace_file_sync_limit(ancestor_dir, commit.mutation_lease.clone(), admission)
                                .await
                        {
                            rustfs_io_metrics::record_put_object_stage_duration_from(
                                rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC,
                                fsync_started,
                            );
                            commit.rollback_inline_std(rollback_data_dir, local_rollback_path.as_ref())?;
                            return Err(err);
                        }
                        rustfs_io_metrics::record_put_object_stage_duration_from(
                            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC,
                            fsync_started,
                        );
                        if ancestor_dir == dst_volume_dir.as_path() {
                            break;
                        }
                        ancestor = ancestor_dir.parent();
                    }
                }

                Ok::<(), std::io::Error>(())
            }
            .await;

            // The disk admission protects the durability chain, not staging
            // cleanup or cache invalidation after that chain has completed.
            drop(file_sync_admission.take());

            // A post-commit rollback (for example, a commit-metadata fsync
            // failure under strict durability) restores the old metadata; drop any
            // descriptors cached during the committed window before propagating the
            // error (rustfs/backlog#1177). Inline objects carry data in xl.meta, so
            // this is mostly defensive and keeps both commit branches consistent.
            if let Err(err) = post_commit {
                for part_path in &invalidate_part_paths {
                    self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
                }
                return Err(DiskError::from(err));
            }

            #[cfg(unix)]
            self.publish_pool_meta_durable_head(receipt_candidate, dst_file_path.clone(), commit.mutation_lease.clone())
                .await;
            #[cfg(not(unix))]
            let _ = receipt_candidate;

            // The commit no longer has a rollback path. Release the Windows
            // object identity guard before best-effort staging cleanup.
            #[cfg(windows)]
            drop(commit.directory_guard);

            if let Some(backup_path) = local_rollback_path.as_ref().map(StagedRollbackBackup::path) {
                let _ = remove_file_if_exists(backup_path);
            }

            // Cleanup
            if cleanup_src_parent && let Some(parent) = src_file_path.parent() {
                let _ = std::fs::remove_dir(parent);
            }

            // Heal reuses a version's `data_dir` and lands the rebuilt shard on
            // the SAME `<object>/<data_dir>/part.N` path. Without this, a cached
            // descriptor would keep serving the pre-heal inode, defeating the heal
            // and eroding read quorum (backlog#1145).
            //
            // The exact keys are derivable here, and this runs on every write, so
            // use them rather than registering a predicate the read path would then
            // have to evaluate. Readers build the same string
            // (`{object}/{data_dir}/part.{n}`), and `fi.parts` enumerates every
            // part of the version now at `dst_path` — any part path absent from it
            // no longer exists for readers to ask for.
            for part_path in &invalidate_part_paths {
                self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
            }

            #[cfg(unix)]
            if let Some(receipt_mutation) = receipt_mutation.as_mut() {
                receipt_mutation.complete();
            }
            Ok(RenameDataResp {
                multipart_source: None,
                old_data_dir: cleanup_data_dir,
                rollback_data_dir,
                cleanup_data_dir,
                sign: version_signature,
                old_current_size,
                old_current_source_checked,
                old_current_source,
            })
        }
    }

    pub(in crate::disk) async fn rename_data_observed(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: &FileInfo,
        dst_volume: &str,
        dst_path: &str,
        namespace_owner: Option<Arc<dyn Send + Sync>>,
    ) -> super::super::RenameDataObservation {
        let mut state = RenameDataState {
            namespace_owner,
            ..Default::default()
        };
        let result = self
            .rename_data_inner(src_volume, src_path, fi.clone(), dst_volume, dst_path, &mut state)
            .await;
        super::super::RenameDataObservation {
            result,
            preflight_rejection: state.preflight_rejection,
        }
    }
}
