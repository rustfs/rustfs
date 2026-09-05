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
use super::{
    EVENT_DISK_LOCAL_ACCESS_FAILED, EVENT_DISK_LOCAL_HEAL_PURGE_FAILED, EVENT_DISK_LOCAL_RENAME_REJECTED, LOG_COMPONENT_ECSTORE,
    LOG_SUBSYSTEM_DISK_LOCAL, LocalDisk, SyncMode, effective_durability, inline_metadata_rollback_dir, observe_old_current_size,
    remove_dir_all_if_exists, remove_dst_base_before_commit, remove_file_if_exists, rename_data_versions_signature,
    run_inline_preparation_before_backup, should_fail_after_metadata_commit, should_fail_before_old_metadata_backup,
    should_fail_commit_rename, should_fail_local_inline_rollback_hardlink, should_remove_staged_meta_before_commit,
    skip_access_checks,
};
#[cfg(test)]
use super::{run_inline_before_file_sync_admission, run_owned_file_write_before_open, run_rename_data_after_first_publication};
use crate::crash_inject::{self, CrashPoint};
use crate::disk::{
    QUOTA_MUTATION_FENCE_METADATA_SUFFIX, RenameDataResp, STORAGE_FORMAT_FILE, STORAGE_FORMAT_FILE_BACKUP, SnapshotLeaseToken,
    error::{DiskError, Result},
    error_conv::{to_access_error, to_file_error},
    os,
    os::{check_path_length, rename_all},
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

fn rollback_committed_rename_std(
    dst_file_path: &Path,
    new_data_path: Option<&Path>,
    rollback_data_dir: Option<Uuid>,
) -> std::io::Result<()> {
    if let Some(old_data_dir) = rollback_data_dir {
        let Some(dst_parent) = dst_file_path.parent() else {
            return Err(std::io::Error::new(ErrorKind::InvalidInput, "missing object metadata parent"));
        };
        let backup_path = dst_parent.join(old_data_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
        std::fs::rename(backup_path, dst_file_path)?;
    } else {
        remove_file_if_exists(dst_file_path)?;
    }

    if let Some(new_data_path) = new_data_path {
        remove_dir_all_if_exists(new_data_path)?;
    }

    Ok(())
}

fn rollback_inline_metadata_commit_std(
    dst_file_path: &Path,
    rollback_data_dir: Option<Uuid>,
    local_rollback_path: Option<&Path>,
) -> std::io::Result<()> {
    if let Some(backup_path) = local_rollback_path {
        // The commit immediately before this rollback renamed the staged
        // xl.meta from the same directory as `backup_path` onto
        // `dst_file_path`, proving both paths are on the same filesystem.
        // Unix rename atomically replaces the committed destination; never
        // unlink it first or an interrupted rollback could lose xl.meta.
        std::fs::rename(backup_path, dst_file_path)?;
    } else {
        rollback_committed_rename_std(dst_file_path, None, rollback_data_dir)?;
    }
    Ok(())
}

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

pub(super) async fn lock_rename_commit_directories(
    source_parent: &Path,
    destination_parent: &Path,
    base_dir: &Path,
    publication_root: &os::PublicationRoot,
    mutation_lease: Arc<os::NamespaceMutationLease>,
) -> Result<os::RenameCommitGuard> {
    #[cfg(windows)]
    let result = {
        let source_parent = source_parent.to_path_buf();
        let destination_parent = destination_parent.to_path_buf();
        let base_dir = base_dir.to_path_buf();
        let publication_root = publication_root.clone();
        os::run_blocking_namespace_operation(mutation_lease, move || {
            let result = os::prepare_rename_commit_guard(&source_parent, &destination_parent, &base_dir, &publication_root);
            #[cfg(test)]
            if result.is_ok() {
                run_destination_commit_directory_preparation(&destination_parent);
            }
            result
        })
        .await
    };
    #[cfg(not(windows))]
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

async fn read_rename_destination_metadata(
    file_path: &Path,
    rename_commit_guard: &os::RenameCommitGuard,
    mutation_lease: Arc<os::NamespaceMutationLease>,
) -> Result<Option<Bytes>> {
    #[cfg(windows)]
    let result = {
        let file_path = file_path.to_path_buf();
        let rename_commit_guard = rename_commit_guard.clone();
        os::run_blocking_namespace_operation(mutation_lease, move || {
            os::read_destination_file_with_commit_guard(&file_path, &rename_commit_guard)
        })
        .await
    };
    #[cfg(not(windows))]
    let _ = (rename_commit_guard, mutation_lease);
    #[cfg(not(windows))]
    let result = match super::super::fs::read_file(file_path).await {
        Ok(data) => Ok(Some(data)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    };

    result
        .map(|data| data.map(Bytes::from))
        .map_err(to_file_error)
        .map_err(DiskError::from)
}

async fn restore_renamed_data_source(
    src_volume_dir: &Path,
    src_data_path: &Path,
    dst_data_path: &Path,
    publication_root: &os::PublicationRoot,
    mutation_lease: Arc<os::NamespaceMutationLease>,
) -> Result<()> {
    if fs::symlink_metadata(src_data_path).await.is_ok() {
        return Ok(());
    }
    let result =
        match os::rename_all_with_lease(dst_data_path, src_data_path, src_volume_dir, publication_root, mutation_lease).await {
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

async fn restore_published_data_source(
    data_paths: Option<&(PathBuf, PathBuf)>,
    src_volume_dir: &Path,
    publication_root: &os::PublicationRoot,
    mutation_lease: Arc<os::NamespaceMutationLease>,
) -> Result<()> {
    let Some((src_data_path, dst_data_path)) = data_paths else {
        return Ok(());
    };
    restore_renamed_data_source(src_volume_dir, src_data_path, dst_data_path, publication_root, mutation_lease).await
}

/// Proof produced only when the local rename returns at an existing access
/// preflight, before metadata, backups, or object data can be published.
#[derive(Debug)]
pub(in crate::disk) struct LocalRenamePreflightRejection(());

impl LocalDisk {
    #[tracing::instrument(name = "rename_data", target = "rustfs_ecstore::disk::local", level = "trace", skip_all)]
    pub(super) async fn rename_data_inner(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
        preflight_rejection: &mut Option<LocalRenamePreflightRejection>,
    ) -> Result<RenameDataResp> {
        crate::hp_guard!("LocalDisk::rename_data");
        let mut fi = fi;
        // A non-force DeleteBucket must not remove a directory while a local
        // object commit is publishing into it. The peer's empty scan remains
        // optimistic; this lease establishes the local commit/delete order and
        // remains owned by any blocking syscall that outlives async cancellation.
        let destination_object_path = self.io_get_object_path(dst_volume, dst_path)?;
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
        let mutation_lease = os::acquire_rename_data_mutation_lease(&self.root, dst_volume, &destination_object_path).await;
        if let Some(claim) = quota_fence_claim {
            mutation_lease.attach_external_guard(claim);
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
                .map(|part| format!("{dst_path}/{data_dir}/part.{}", part.number))
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
            *preflight_rejection = Some(LocalRenamePreflightRejection(()));
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        let dst_volume_dir = self.io_get_bucket_path(dst_volume)?;
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
            *preflight_rejection = Some(LocalRenamePreflightRejection(()));
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        // xl.meta path
        let src_file_path = self.io_get_object_path(src_volume, format!("{}/{}", src_path, STORAGE_FORMAT_FILE).as_str())?;
        let dst_file_path = self.io_get_object_path(dst_volume, format!("{}/{}", dst_path, STORAGE_FORMAT_FILE).as_str())?;

        // data_dir path
        let has_data_dir_path = {
            let has_data_dir = {
                if !fi.is_remote() {
                    fi.data_dir
                        .map(|dir| rustfs_utils::path::retain_slash(dir.to_string().as_str()))
                } else {
                    None
                }
            };

            if let Some(data_dir) = has_data_dir {
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
        };

        check_path_length(src_file_path.to_string_lossy().to_string().as_str())?;
        check_path_length(dst_file_path.to_string_lossy().to_string().as_str())?;

        let no_inline = fi.data.is_none() && fi.size > 0;
        // Captured before `fi` is consumed by add_version; gates the stale
        // destination purge below.
        let fi_healing = fi.is_healing();

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
        if !no_inline {
            fs::create_dir_all(src_file_parent).await.map_err(to_file_error)?;
        }
        // Acquire the common trees before reading destination metadata. On
        // Windows this pins the object directory identity across metadata
        // preparation, data publication, rollback backup, and final commit.
        let rename_commit_guard = lock_rename_commit_directories(
            src_file_parent,
            dst_file_parent,
            &dst_volume_dir,
            &self.publication_root,
            mutation_lease.clone(),
        )
        .await?;
        let has_dst_buf = read_rename_destination_metadata(&dst_file_path, &rename_commit_guard, mutation_lease.clone()).await?;

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

            let old_current_size = if dst_meta_unparsable {
                None
            } else {
                observe_old_current_size(has_dst_buf.is_some(), &xlmeta)
            };

            let mut skip_parent = dst_volume_dir.clone();
            if has_dst_buf.as_ref().is_some()
                && let Some(parent) = dst_file_path.parent()
            {
                skip_parent = parent.to_path_buf();
            }

            let version_id = fi.version_id.unwrap_or_default();
            let has_old_data_dir = xlmeta.find_unshared_data_dir_for_version(Some(version_id));
            let old_version_exists = xlmeta.find_version(Some(version_id)).is_ok();
            let rollback_data_dir = has_old_data_dir.or_else(|| {
                if old_version_exists && has_dst_buf.is_some() {
                    Some(inline_metadata_rollback_dir(version_id, &xlmeta))
                } else {
                    None
                }
            });
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
            let tmp_meta_sync = if durability.syncs_commit_metadata() {
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
                let src_file_path = src_file_path.clone();
                let dst_file_path = dst_file_path.clone();
                let rename_commit_guard = rename_commit_guard.clone();
                let mutation_lease = mutation_lease.clone();
                async move {
                    os::run_blocking_namespace_operation(mutation_lease, move || {
                        #[cfg(test)]
                        run_owned_file_write_before_open(&src_file_path);
                        let mut prepared_metadata_source = os::create_prepared_rename_source_with_commit_guard(
                            &src_file_path,
                            &dst_file_path,
                            &rename_commit_guard,
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
            let rename_commit_guard = remove_dst_base_before_commit(
                dst_path,
                rename_commit_guard,
                src_file_parent,
                dst_file_parent,
                &dst_volume_dir,
                &self.publication_root,
                mutation_lease.clone(),
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
                && let Err(err) = self.move_to_trash(dst_data_path, true, false).await
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
            if let Some((src_data_path, dst_data_path)) = has_data_dir_path.as_ref()
                && let Err(err) = os::rename_all_with_commit_guard(
                    src_data_path,
                    dst_data_path,
                    &skip_parent,
                    &self.publication_root,
                    &rename_commit_guard,
                    mutation_lease.clone(),
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
                restore_published_data_source(
                    has_data_dir_path.as_ref(),
                    &src_volume_dir,
                    &self.publication_root,
                    mutation_lease.clone(),
                )
                .await?;
                return Err(err);
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
                restore_published_data_source(
                    has_data_dir_path.as_ref(),
                    &src_volume_dir,
                    &self.publication_root,
                    mutation_lease.clone(),
                )
                .await?;
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
            if let (Some(old_data_dir), Some(dst_buf)) = (rollback_data_dir, has_dst_buf.as_ref()) {
                let backup_parent = dst_file_parent.join(old_data_dir.to_string());
                #[cfg(not(windows))]
                if let Err(err) = os::make_dir_all(&backup_parent, &skip_parent).await {
                    restore_published_data_source(
                        has_data_dir_path.as_ref(),
                        &src_volume_dir,
                        &self.publication_root,
                        mutation_lease.clone(),
                    )
                    .await?;
                    return Err(err);
                }
                let backup_path_guard = match rename_commit_guard.create_destination_directory_for_path_access(&backup_parent) {
                    Ok(guard) => guard,
                    Err(err) => {
                        restore_published_data_source(
                            has_data_dir_path.as_ref(),
                            &src_volume_dir,
                            &self.publication_root,
                            mutation_lease.clone(),
                        )
                        .await?;
                        return Err(DiskError::from(to_file_error(err)));
                    }
                };
                let backup_path = backup_parent.join(STORAGE_FORMAT_FILE_BACKUP);
                if let Err(err) = check_path_length(backup_path.to_string_lossy().as_ref()) {
                    #[cfg(windows)]
                    drop(backup_path_guard);
                    restore_published_data_source(
                        has_data_dir_path.as_ref(),
                        &src_volume_dir,
                        &self.publication_root,
                        mutation_lease.clone(),
                    )
                    .await?;
                    return Err(err);
                }
                let backup_bytes = dst_buf.clone();
                // Keep the volume, commit-tree, and exact destination-path
                // guards in this task until the backup write and durability
                // sync finish. A detached spawn_blocking writer could survive
                // cancellation and later truncate a newer transaction's
                // deterministic rollback backup.
                let write_result = os::run_blocking_namespace_operation(mutation_lease.clone(), move || {
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
                    restore_published_data_source(
                        has_data_dir_path.as_ref(),
                        &src_volume_dir,
                        &self.publication_root,
                        mutation_lease.clone(),
                    )
                    .await?;
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
                &self.publication_root,
                &rename_commit_guard,
                mutation_lease.clone(),
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
                restore_published_data_source(
                    has_data_dir_path.as_ref(),
                    &src_volume_dir,
                    &self.publication_root,
                    mutation_lease.clone(),
                )
                .await?;
                return Err(err);
            }

            let committed_new_data_path = has_data_dir_path.as_ref().map(|(_, dst_data_path)| dst_data_path.as_path());
            if should_fail_after_metadata_commit(dst_path) {
                rollback_committed_rename_std(&dst_file_path, committed_new_data_path, rollback_data_dir)
                    .map_err(to_file_error)?;
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
                if let Err(err) = os::fsync_dst_dir_group_commit(parent).await {
                    rustfs_io_metrics::record_put_object_stage_duration_from(
                        rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC,
                        fsync_started,
                    );
                    rollback_committed_rename_std(&dst_file_path, committed_new_data_path, rollback_data_dir)
                        .map_err(to_file_error)?;
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
                    if let Err(err) = os::fsync_dir(dir).await {
                        rustfs_io_metrics::record_put_object_stage_duration_from(
                            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC,
                            fsync_started,
                        );
                        rollback_committed_rename_std(&dst_file_path, committed_new_data_path, rollback_data_dir)
                            .map_err(to_file_error)?;
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
            drop(rename_commit_guard);

            if let Some(src_file_path_parent) = src_file_path.parent() {
                if src_volume != super::super::RUSTFS_META_MULTIPART_BUCKET {
                    let _ = std::fs::remove_dir(src_file_path_parent);
                } else {
                    let _ = self
                        .delete_file(&dst_volume_dir, &src_file_path_parent.to_path_buf(), true, false)
                        .await;
                }
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

            Ok(RenameDataResp {
                old_data_dir: has_old_data_dir,
                rollback_data_dir,
                cleanup_data_dir: has_old_data_dir,
                sign: version_signature,
                old_current_size,
            })
        } else {
            // Inline metadata preparation is blocking. The transaction lease is
            // moved into that work so a timeout can release the async waiter without
            // allowing a retry to reuse the deterministic staging path too early.
            let src = src_file_path.clone();
            let dst = dst_file_path.clone();
            let cleanup_path = if src_volume == super::super::RUSTFS_META_MULTIPART_BUCKET {
                src_file_path.parent().map(|p| p.to_path_buf())
            } else {
                None
            };
            let dst_path_for_failpoint = dst_path.to_string();
            #[cfg(windows)]
            let source_parent = src_file_parent.to_path_buf();
            let rename_commit_guard_for_preparation = rename_commit_guard.clone();
            let sync = durability.syncs_commit_metadata();
            #[cfg(test)]
            run_inline_before_file_sync_admission(dst_path);
            let mut file_sync_admission = if sync {
                Some(
                    os::acquire_file_sync_admission(self.file_sync_permits.clone())
                        .await
                        .map_err(to_file_error)
                        .map_err(DiskError::from)?,
                )
            } else {
                None
            };
            let prepare_inline_metadata = move || {
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

                let old_current_size = if dst_meta_unparsable {
                    None
                } else {
                    observe_old_current_size(has_dst_buf.is_some(), &xlmeta)
                };

                let version_id = fi.version_id.unwrap_or_default();
                let old_data_dir = xlmeta.find_unshared_data_dir_for_version(Some(version_id));
                let old_version_exists = xlmeta.find_version(Some(version_id)).is_ok();
                let rollback_data_dir = old_data_dir.or_else(|| {
                    if old_version_exists && has_dst_buf.is_some() {
                        Some(inline_metadata_rollback_dir(version_id, &xlmeta))
                    } else {
                        None
                    }
                });
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
                prepared_metadata_source.write_all(&new_buf, sync)?;
                run_inline_preparation_before_backup(&dst_path_for_failpoint);
                if let Some(ref old_metadata) = has_dst_buf
                    && (rollback_data_dir.is_some() || sync || cfg!(test))
                {
                    #[cfg(windows)]
                    let backup_path = {
                        let backup_path = src
                            .parent()
                            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "missing staging metadata parent"))?
                            .join(STORAGE_FORMAT_FILE_BACKUP);
                        source_metadata_guard.write_file_for_path_access(&backup_path, old_metadata, sync, false)?;
                        backup_path
                    };
                    #[cfg(not(windows))]
                    let backup_path = create_local_inline_rollback_backup(&dst, &src, old_metadata)?;
                    #[cfg(not(windows))]
                    if sync {
                        std::fs::File::open(&backup_path)?.sync_data()?;
                    }
                    staged_rollback_path = Some(backup_path);
                }

                Ok::<_, std::io::Error>((
                    rollback_data_dir,
                    old_data_dir,
                    version_signature,
                    old_current_size,
                    staged_rollback_path,
                    has_dst_buf.is_none(),
                    prepared_metadata_source,
                ))
            };
            let inline_preparation = if let Some(admission) = file_sync_admission.as_ref() {
                os::run_blocking_namespace_file_sync_operation(mutation_lease.clone(), admission, prepare_inline_metadata).await
            } else {
                os::run_blocking_namespace_operation(mutation_lease.clone(), prepare_inline_metadata).await
            }
            .map_err(to_file_error)
            .map_err(DiskError::from);

            let (
                rollback_data_dir,
                cleanup_data_dir,
                version_signature,
                old_current_size,
                mut local_rollback_path,
                destination_was_absent,
                prepared_metadata_source,
            ) = match inline_preparation {
                Ok(prepared) => prepared,
                Err(err) => {
                    for part_path in &invalidate_part_paths {
                        self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
                    }
                    return Err(err);
                }
            };

            let rename_commit_guard = remove_dst_base_before_commit(
                dst_path,
                rename_commit_guard,
                src_file_parent,
                dst_file_parent,
                &dst_volume_dir,
                &self.publication_root,
                mutation_lease.clone(),
            )
            .await?;

            if should_remove_staged_meta_before_commit(dst_path) {
                drop(prepared_metadata_source);
                let remove_result = std::fs::remove_file(&src_file_path);
                if let Some(backup_path) = local_rollback_path.as_deref() {
                    let _ = remove_file_if_exists(backup_path);
                }
                remove_result.map_err(to_file_error)?;
                return Err(DiskError::FileNotFound);
            }

            if let (Some(rollback_data_dir), Some(staged_backup)) = (rollback_data_dir, local_rollback_path.as_deref()) {
                let Some(dst_parent) = dst_file_path.parent() else {
                    return Err(DiskError::other("missing object metadata parent"));
                };
                let backup_path = dst_parent
                    .join(rollback_data_dir.to_string())
                    .join(STORAGE_FORMAT_FILE_BACKUP);
                // rename_all acquires the backup path's namespace lease. Do not
                // hold a disk admission while acquiring another namespace lock.
                drop(file_sync_admission.take());
                if let Err(err) = rename_all(staged_backup, &backup_path, &dst_volume_dir, &self.publication_root).await {
                    let _ = remove_file_if_exists(staged_backup);
                    return Err(err);
                }
                #[cfg(test)]
                run_rename_data_after_first_publication(&self.root, dst_volume, dst_path);
                if sync {
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
                    let fsync_started = rustfs_io_metrics::put_stage_timer();
                    if let Err(err) =
                        os::fsync_dir_with_namespace_file_sync_limit(backup_parent, mutation_lease.clone(), admission).await
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

            let commit_result = if should_fail_commit_rename(dst_path) {
                Err(DiskError::other("test fail during metadata commit rename"))
            } else {
                os::rename_all_with_prepared_source(
                    prepared_metadata_source,
                    &src_file_path,
                    &dst_file_path,
                    &dst_volume_dir,
                    &self.publication_root,
                    &rename_commit_guard,
                    mutation_lease.clone(),
                )
                .await
            };
            if let Err(err) = commit_result {
                if let Some(backup_path) = local_rollback_path.as_deref() {
                    let _ = remove_file_if_exists(backup_path);
                }
                for part_path in &invalidate_part_paths {
                    self.io_backend.invalidate_cached_fd(dst_volume, part_path).await;
                }
                return Err(err);
            }

            let post_commit = async {
                if should_fail_after_metadata_commit(dst_path) {
                    rollback_inline_metadata_commit_std(&dst_file_path, rollback_data_dir, local_rollback_path.as_deref())?;
                    return Err(std::io::Error::other("test fail after metadata commit"));
                }

                // Persist the commit rename's directory entry across power loss.
                if let Some(admission) = file_sync_admission.as_ref()
                    && let Some(dst_parent) = dst_file_path.parent()
                {
                    let fsync_started = rustfs_io_metrics::put_stage_timer();
                    if let Err(err) =
                        os::fsync_dst_dir_group_commit_or_namespace_file_sync_limit(dst_parent, mutation_lease.clone(), admission)
                            .await
                    {
                        rustfs_io_metrics::record_put_object_stage_duration_from(
                            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_DST_DIR_FSYNC,
                            fsync_started,
                        );
                        rollback_inline_metadata_commit_std(&dst_file_path, rollback_data_dir, local_rollback_path.as_deref())?;
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
                            os::fsync_dir_with_namespace_file_sync_limit(ancestor_dir, mutation_lease.clone(), admission).await
                        {
                            rustfs_io_metrics::record_put_object_stage_duration_from(
                                rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_ANCESTOR_DIR_FSYNC,
                                fsync_started,
                            );
                            rollback_inline_metadata_commit_std(
                                &dst_file_path,
                                rollback_data_dir,
                                local_rollback_path.as_deref(),
                            )?;
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

            // The commit no longer has a rollback path. Release the Windows
            // object identity guard before best-effort staging cleanup.
            #[cfg(windows)]
            drop(rename_commit_guard);

            if let Some(backup_path) = local_rollback_path.as_deref() {
                let _ = remove_file_if_exists(backup_path);
            }

            // Cleanup
            if let Some(ref cleanup) = cleanup_path {
                let _ = self.delete_file(&dst_volume_dir, cleanup, true, false).await;
            } else if let Some(parent) = src_file_path.parent() {
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

            Ok(RenameDataResp {
                old_data_dir: cleanup_data_dir,
                rollback_data_dir,
                cleanup_data_dir,
                sign: version_signature,
                old_current_size,
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
    ) -> super::super::RenameDataObservation {
        let mut preflight_rejection = None;
        let result = self
            .rename_data_inner(src_volume, src_path, fi.clone(), dst_volume, dst_path, &mut preflight_rejection)
            .await;
        super::super::RenameDataObservation {
            result,
            preflight_rejection,
        }
    }
}
