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

use crate::config::storageclass::DEFAULT_INLINE_BLOCK;
use crate::crash_inject::{self, CrashPoint};
use crate::data_usage::local_snapshot::ensure_data_usage_layout;
use crate::diagnostics::get::{
    GET_OBJECT_PATH_INTERNAL_META, GET_OBJECT_PATH_LEGACY_DUPLEX, GET_STAGE_READ_VERSION_DECODE,
    GET_STAGE_READ_VERSION_PATH_CHECK, GET_STAGE_READ_VERSION_PATH_RESOLVE, GET_STAGE_READ_VERSION_XLMETA_READ,
    get_stage_timer_if_enabled, record_get_stage_duration_if_enabled,
};
#[cfg(test)]
use crate::disk::HEALING_MARKER_PATH;
use crate::disk::disk_store::{get_drive_walkdir_stall_timeout, get_object_disk_read_timeout};
use crate::disk::{
    BUCKET_META_PREFIX, CHECK_PART_FILE_CORRUPT, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN,
    CHECK_PART_VOLUME_NOT_FOUND, CheckPartsResp, ConditionalFileUpdate, DataDirDeleteStatus, DeleteOptions, DiskAPI, DiskInfo,
    DiskInfoOptions, DiskLocation, DiskMetrics, FileInfoVersions, FileReader, FileWriter, MmapCopyStageMetrics, OldCurrentSize,
    PART_TRANSACTION_NEW_META, PART_TRANSACTION_OLD_META, PART_TRANSACTION_ROLLBACK, PartTransactionAction,
    QUOTA_MUTATION_FENCE_METADATA_SUFFIX, RUSTFS_META_BUCKET, RUSTFS_META_TMP_BUCKET, RUSTFS_META_TMP_DELETED_BUCKET,
    ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp, STORAGE_FORMAT_FILE, STORAGE_FORMAT_FILE_BACKUP,
    SnapshotLeaseToken, UpdateMetadataOpts, VolumeInfo, WalkDirOptions, conv_part_err_to_int,
    endpoint::Endpoint,
    error::{DiskError, Error, FileAccessDeniedWithContext, Result},
    error_conv::{to_access_error, to_file_error, to_unformatted_disk_error, to_volume_error},
    format::FormatV3,
    fs::{O_APPEND, O_CREATE, O_RDONLY, O_TRUNC, O_WRONLY, access, lstat, lstat_std, remove, remove_all_std, remove_std, rename},
    is_quota_mutation_fence_path, os,
    os::{check_path_length, is_dir_not_empty_error, is_empty_dir, is_root_disk, rename_all, rename_all_ignore_missing_source},
    quota_mutation_fence_path,
};
use crate::erasure::coding::{self, bitrot_verify};
use crate::runtime::sources as runtime_sources;
use bytes::Bytes;
use metrics::counter;
#[cfg(target_os = "linux")]
use metrics::gauge;
use parking_lot::RwLock as ParkingLotRwLock;
use rustfs_filemeta::{
    Cache, FileInfo, FileInfoOpts, FileMeta, MetaCacheEntry, MetacacheWriter, ObjectPartInfo, Opts, RawFileInfo, UpdateFn,
    ValidationMode, get_file_info, read_xl_meta_no_data_sync,
};
use rustfs_utils::HashAlgorithm;
use rustfs_utils::os::get_info;
use rustfs_utils::path::{
    GLOBAL_DIR_SUFFIX, GLOBAL_DIR_SUFFIX_WITH_SLASH, SLASH_SEPARATOR, clean, decode_dir_object, encode_dir_object, has_suffix,
    path_join, path_join_buf,
};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::io::{Error as IoError, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use std::{
    fs::Metadata,
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use tokio::fs::{self, File};
#[cfg(not(unix))]
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ErrorKind, ReadBuf};
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};
use tokio::time::{Instant, Sleep, interval_at, timeout};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const DELETED_OBJECTS_CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 5);
const STALE_TMP_OBJECT_EXPIRY: Duration = Duration::from_secs(24 * 60 * 60);
const RUSTFS_META_TMP_OLD_BUCKET: &str = ".rustfs.sys/tmp-old";
const INLINE_METADATA_ROLLBACK_DIR_XOR: u128 = 0x7275737466735f696e6c696e655f7262;
const DELETE_MARKER_ROLLBACK_FILE: &str = "xl.meta.delete-marker.rollback";
pub(crate) const DELETE_DATA_DIR_MARKER_PREFIX: &str = "delete-data.";
pub(crate) const RESERVED_DELETE_DATA_DIR_MARKER_PREFIX: &str = "reserve-delete-data.";
const STARTUP_CLEANUP_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const ENV_BITROT_SIZE_MISMATCH_RETRY_COUNT: &str = "RUSTFS_BITROT_SIZE_MISMATCH_RETRY_COUNT";
const ENV_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS: &str = "RUSTFS_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS";
const DEFAULT_BITROT_SIZE_MISMATCH_RETRY_COUNT: u64 = 2;
const DEFAULT_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS: u64 = 100;
const PART_TRANSACTION_OLD_DATA: &str = "old.data";
const PART_TRANSACTION_OLD_DATA_ABSENT: &str = "old.data.absent";
const PART_TRANSACTION_OLD_META_ABSENT: &str = "old.meta.absent";
const PART_TRANSACTION_PUBLISH_META: &str = "publish.meta";
enum ReadAllError {
    Open(std::io::Error),
    Disk(DiskError),
}

struct ListingMetadataRead {
    bytes: Vec<u8>,
    file_meta: Option<FileMeta>,
    data_dirs: HashSet<String>,
    has_namespace_child_candidate: bool,
}

fn read_all_data_std(path: &Path) -> core::result::Result<(Vec<u8>, Option<OffsetDateTime>), ReadAllError> {
    let mut file = std::fs::File::open(path).map_err(ReadAllError::Open)?;
    let metadata = file.metadata().map_err(|err| ReadAllError::Disk(to_file_error(err).into()))?;

    if metadata.is_dir() {
        return Err(ReadAllError::Disk(DiskError::FileNotFound));
    }

    let size = usize::try_from(metadata.len()).map_err(|err| ReadAllError::Disk(DiskError::other(err)))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|err| ReadAllError::Disk(Error::other(err)))?;
    std::io::Read::read_to_end(&mut file, &mut bytes).map_err(|err| ReadAllError::Disk(to_file_error(err).into()))?;

    let modtime = metadata.modified().ok().map(OffsetDateTime::from);
    Ok((bytes, modtime))
}

pub(crate) fn inline_metadata_rollback_dir(version_id: Uuid, meta: &FileMeta) -> Uuid {
    let used_data_dirs: HashSet<Uuid> = meta.get_data_dirs().unwrap_or_default().into_iter().flatten().collect();
    let base = version_id.as_u128() ^ INLINE_METADATA_ROLLBACK_DIR_XOR;
    let mut salt = 0u128;

    loop {
        let candidate = Uuid::from_u128(base ^ salt);
        if !candidate.is_nil() && !used_data_dirs.contains(&candidate) {
            return candidate;
        }
        salt = salt.wrapping_add(1);
    }
}

fn remove_file_if_exists(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn remove_dir_all_if_exists(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn snapshot_part_transaction_file(src: &Path, backup: &Path, absent: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(src) {
        Ok(metadata) if metadata.is_file() => std::fs::hard_link(src, backup),
        Ok(_) => Err(std::io::Error::new(ErrorKind::InvalidData, "multipart transaction source is not a file")),
        Err(err) if err.kind() == ErrorKind::NotFound => std::fs::write(absent, []),
        Err(err) => Err(err),
    }
}

fn restore_part_transaction_file(current: &Path, backup: &Path, absent: &Path, restore: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(backup) {
        Ok(metadata) if metadata.is_file() => {
            remove_file_if_exists(restore)?;
            std::fs::hard_link(backup, restore)?;
            std::fs::rename(restore, current)
        }
        Ok(_) => Err(std::io::Error::new(ErrorKind::InvalidData, "multipart transaction backup is not a file")),
        Err(err) if err.kind() == ErrorKind::NotFound && absent.is_file() => remove_file_if_exists(current),
        Err(err) => Err(err),
    }
}

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

fn create_local_inline_rollback_backup(
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

async fn write_metadata_rollback_backup(object_dir: &Path, rollback_dir: Uuid, data: &[u8]) -> Result<()> {
    let backup_dir = object_dir.join(rollback_dir.to_string());
    fs::create_dir_all(&backup_dir).await.map_err(to_file_error)?;
    fs::write(backup_dir.join(STORAGE_FORMAT_FILE_BACKUP), data)
        .await
        .map_err(to_file_error)?;
    Ok(())
}

async fn restore_metadata_backup(
    object_dir: &Path,
    xl_path: &Path,
    rollback_dir: Uuid,
    publication_root: &os::PublicationRoot,
) -> Result<()> {
    let rollback_path = object_dir.join(rollback_dir.to_string());
    let backup_path = rollback_path.join(STORAGE_FORMAT_FILE_BACKUP);
    rename_all(&backup_path, xl_path, object_dir, publication_root).await?;
    // A synthetic inline rollback dir held only the backup the rename above
    // just consumed; reclaim it so the object dir can empty out. A real data
    // dir still holds its parts, so the non-recursive remove is a benign
    // no-op there (mirrors restore_delete_rollback).
    let _ = fs::remove_dir(&rollback_path).await;
    Ok(())
}

async fn lock_rename_commit_directories(
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
    let result = match super::fs::read_file(file_path).await {
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

async fn restore_delete_rollback(
    object_dir: &Path,
    xl_path: &Path,
    rollback_dir: Uuid,
    publication_root: &os::PublicationRoot,
) -> Result<()> {
    remove_version_delete_markers(object_dir, rollback_dir).await?;
    let rollback_path = object_dir.join(rollback_dir.to_string());
    let mut staged_paths = Vec::new();
    let mut remove_new_metadata = false;
    match fs::read_dir(&rollback_path).await {
        Ok(mut entries) => {
            while let Some(entry) = entries.next_entry().await.map_err(to_file_error)? {
                let name = entry.file_name();
                if name == DELETE_MARKER_ROLLBACK_FILE {
                    remove_new_metadata = true;
                } else if name != STORAGE_FORMAT_FILE_BACKUP {
                    staged_paths.push((entry.path(), object_dir.join(name)));
                }
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(to_file_error(err).into()),
    }

    let had_staged_paths = !staged_paths.is_empty();
    for (src, dst) in staged_paths {
        rename_all(&src, &dst, object_dir, publication_root).await?;
    }

    let backup_path = rollback_path.join(STORAGE_FORMAT_FILE_BACKUP);
    match rename_all(&backup_path, xl_path, object_dir, publication_root).await {
        Ok(()) => {
            let _ = fs::remove_dir(&rollback_path).await;
            Ok(())
        }
        // A missing backup only means "remove the newly-created delete marker"
        // when the marker proves there was no old metadata to restore.
        Err(DiskError::FileNotFound) if remove_new_metadata => match fs::remove_file(xl_path).await {
            Ok(()) => {
                let _ = fs::remove_file(rollback_path.join(DELETE_MARKER_ROLLBACK_FILE)).await;
                let _ = fs::remove_dir(&rollback_path).await;
                Ok(())
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let _ = fs::remove_file(rollback_path.join(DELETE_MARKER_ROLLBACK_FILE)).await;
                let _ = fs::remove_dir(&rollback_path).await;
                Ok(())
            }
            Err(err) => Err(to_file_error(err).into()),
        },
        Err(DiskError::FileNotFound) if had_staged_paths => Err(DiskError::FileNotFound),
        Err(DiskError::FileNotFound) => match fs::metadata(xl_path).await {
            Ok(_) => {
                let _ = fs::remove_dir(&rollback_path).await;
                Ok(())
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Err(DiskError::FileNotFound),
            Err(err) => Err(to_file_error(err).into()),
        },
        Err(err) => Err(err),
    }
}

async fn remove_version_delete_markers(object_dir: &Path, rollback_dir: Uuid) -> Result<()> {
    let reserved_name = format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{rollback_dir}");
    let committed_name = format!("{DELETE_DATA_DIR_MARKER_PREFIX}{rollback_dir}");
    let mut entries = match fs::read_dir(object_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(to_file_error(err).into()),
    };
    while let Some(entry) = entries.next_entry().await.map_err(to_file_error)? {
        if !entry.file_type().await.map_err(to_file_error)?.is_dir()
            || !entry.file_name().to_str().is_some_and(|name| Uuid::parse_str(name).is_ok())
        {
            continue;
        }
        for marker_name in [&reserved_name, &committed_name] {
            match fs::remove_file(entry.path().join(marker_name)).await {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(to_file_error(err).into()),
            }
        }
    }
    Ok(())
}

struct DeleteRollbackFailure {
    stage: &'static str,
    error: DiskError,
}

async fn restore_delete_rollback_after_error(
    object_dir: &Path,
    xl_path: &Path,
    rollback_dir: Option<Uuid>,
    volume: &str,
    path: &str,
    failure: DeleteRollbackFailure,
    publication_root: &os::PublicationRoot,
) -> DiskError {
    let DeleteRollbackFailure { stage, error } = failure;
    let Some(rollback_dir) = rollback_dir else {
        return error;
    };

    if let Err(restore_err) = restore_delete_rollback(object_dir, xl_path, rollback_dir, publication_root).await {
        warn!(
            volume,
            path,
            rollback_dir = %rollback_dir,
            stage,
            cause = ?error,
            error = ?restore_err,
            "failed to restore delete rollback after local delete error"
        );
    }

    error
}

/// Whether a failed `remove_dir` while cleaning up an object path is benign.
///
/// The directory is either already gone (`NotFound`) or still holds sibling
/// entries owned by another step — e.g. the delete rollback-staging dir, which
/// the caller removes only after write quorum is confirmed — in which case it is
/// left intact. illumos/Solaris report the still-populated case as EEXIST rather
/// than ENOTEMPTY (see [`is_dir_not_empty_error`]), so matching `ErrorKind`
/// alone misclassifies it as a hard failure and turns a benign delete into a
/// spurious `FileAccessDenied` (rustfs/rustfs#4978).
fn is_benign_object_rmdir_error(err: &std::io::Error) -> bool {
    err.kind() == ErrorKind::NotFound || is_dir_not_empty_error(err)
}

/// Classify a `delete_volume` removal error. A non-force `remove_dir` on a
/// populated bucket must map to `VolumeNotEmpty`; illumos/Solaris report that as
/// EEXIST rather than ENOTEMPTY, which `to_volume_error` would otherwise pass
/// through as a raw OS error (rustfs/rustfs#4978).
fn classify_delete_volume_error(err: std::io::Error) -> DiskError {
    if is_dir_not_empty_error(&err) {
        DiskError::VolumeNotEmpty
    } else {
        to_volume_error(err).into()
    }
}

#[cfg(unix)]
struct EmptyDirectoryFrame {
    path: PathBuf,
    name_in_parent: std::ffi::CString,
    entries: rustix::fs::Dir,
}

#[cfg(unix)]
fn empty_tree_io_error(err: rustix::io::Errno) -> std::io::Error {
    match err {
        rustix::io::Errno::NOTDIR | rustix::io::Errno::LOOP => std::io::Error::from(ErrorKind::DirectoryNotEmpty),
        _ => err.into(),
    }
}

#[cfg(unix)]
fn remove_empty_directory_tree_unix_at(
    root_parent: impl std::os::fd::AsFd,
    root_name: &std::ffi::CStr,
    root: &Path,
    mut before_descend: impl FnMut(&Path) -> std::io::Result<()>,
    mut before_remove: impl FnMut(&Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use rustix::{
        fs::{AtFlags, Dir, Mode, OFlags, fstat, openat, statat, unlinkat},
        io::Errno,
    };
    use std::os::unix::ffi::OsStrExt;

    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let root_fd = openat(&root_parent, root_name, flags, Mode::empty()).map_err(empty_tree_io_error)?;
    // Each frame owns one directory iterator/FD, so memory and descriptors are
    // bounded by path depth rather than by the number of empty remnants.
    let mut stack = vec![EmptyDirectoryFrame {
        path: root.to_path_buf(),
        name_in_parent: root_name.to_owned(),
        entries: Dir::new(root_fd).map_err(empty_tree_io_error)?,
    }];

    while let Some(mut frame) = stack.pop() {
        let next_child = loop {
            let Some(entry) = frame.entries.next() else {
                break None;
            };
            let entry = entry.map_err(std::io::Error::from)?;
            let name = entry.file_name();
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                continue;
            }

            let name = name.to_owned();
            let child_path = frame.path.join(std::ffi::OsStr::from_bytes(name.as_bytes()));
            before_descend(&child_path)?;
            break Some((child_path, name));
        };

        if let Some((child_path, name)) = next_child {
            let parent = frame.entries.fd().map_err(std::io::Error::from)?;
            let child = match openat(parent, name.as_c_str(), flags, Mode::empty()) {
                Ok(child) => child,
                // A concurrent cleanup may remove an empty child after readdir
                // returns it. Resume the parent instead of treating the whole
                // bucket as missing and leaving its root behind.
                Err(Errno::NOENT) => {
                    stack.push(frame);
                    continue;
                }
                Err(err) => return Err(empty_tree_io_error(err)),
            };
            stack.push(frame);
            stack.push(EmptyDirectoryFrame {
                path: child_path,
                name_in_parent: name,
                entries: Dir::new(child).map_err(empty_tree_io_error)?,
            });
            continue;
        }
        before_remove(&frame.path)?;
        let parent = if let Some(parent) = stack.last() {
            parent.entries.fd().map_err(std::io::Error::from)?
        } else {
            root_parent.as_fd()
        };
        let expected = fstat(frame.entries.fd().map_err(std::io::Error::from)?).map_err(empty_tree_io_error)?;
        let current = statat(parent, frame.name_in_parent.as_c_str(), AtFlags::SYMLINK_NOFOLLOW).map_err(empty_tree_io_error)?;
        if current.st_dev != expected.st_dev || current.st_ino != expected.st_ino {
            return Err(std::io::Error::from(ErrorKind::DirectoryNotEmpty));
        }
        match unlinkat(parent, frame.name_in_parent.as_c_str(), AtFlags::REMOVEDIR).map_err(empty_tree_io_error) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

#[cfg(unix)]
fn remove_empty_directory_tree_unix_with(
    root: &Path,
    before_descend: impl FnMut(&Path) -> std::io::Result<()>,
    before_remove: impl FnMut(&Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use rustix::fs::{Mode, OFlags, open};
    use std::os::unix::ffi::OsStrExt;

    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let root_parent_path = root
        .parent()
        .ok_or_else(|| std::io::Error::from(ErrorKind::DirectoryNotEmpty))?;
    let root_name = root
        .file_name()
        .ok_or_else(|| std::io::Error::from(ErrorKind::DirectoryNotEmpty))?
        .as_bytes();
    let root_name = std::ffi::CString::new(root_name).map_err(|_| std::io::Error::from(ErrorKind::DirectoryNotEmpty))?;
    let root_parent = open(root_parent_path, flags, Mode::empty()).map_err(empty_tree_io_error)?;
    remove_empty_directory_tree_unix_at(&root_parent, root_name.as_c_str(), root, before_descend, before_remove)
}

#[cfg(target_os = "linux")]
async fn remove_empty_directory_tree_under_mount_lease(
    mount_lease: &std::fs::File,
    volume: &str,
    root: PathBuf,
) -> std::io::Result<()> {
    let root_parent = mount_lease.try_clone()?;
    let root_name = std::ffi::CString::new(volume.as_bytes()).map_err(|_| std::io::Error::from(ErrorKind::DirectoryNotEmpty))?;
    tokio::task::spawn_blocking(move || {
        remove_empty_directory_tree_unix_at(&root_parent, root_name.as_c_str(), &root, |_| Ok(()), |_| Ok(()))
    })
    .await?
}

#[cfg(unix)]
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
async fn remove_empty_directory_tree_with(
    root: &Path,
    before_descend: impl FnMut(&Path) -> std::io::Result<()>,
    before_remove: impl FnMut(&Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    remove_empty_directory_tree_unix_with(root, before_descend, before_remove)
}

#[cfg(all(unix, not(target_os = "linux")))]
async fn remove_empty_directory_tree(root: &Path) -> std::io::Result<()> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || remove_empty_directory_tree_unix_with(&root, |_| Ok(()), |_| Ok(()))).await?
}

#[cfg(windows)]
#[derive(Debug)]
struct LockedEmptyDirectory {
    handle: winapi_util::Handle,
}

#[cfg(windows)]
fn validate_windows_empty_directory(file_attributes: u64) -> std::io::Result<()> {
    const FILE_ATTRIBUTE_DIRECTORY: u64 = 0x10;
    const FILE_ATTRIBUTE_REPARSE_POINT: u64 = 0x400;

    if file_attributes & FILE_ATTRIBUTE_DIRECTORY == 0 || file_attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(std::io::Error::from(ErrorKind::DirectoryNotEmpty));
    }
    Ok(())
}

#[cfg(windows)]
async fn lock_windows_empty_directory(path: &Path, canonical_root: Option<&Path>) -> std::io::Result<LockedEmptyDirectory> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::{
        Foundation::GENERIC_READ,
        Storage::FileSystem::{DELETE, FILE_SHARE_READ},
    };

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;

    let path = path.to_path_buf();
    let canonical_root = canonical_root.map(Path::to_path_buf);
    tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .access_mode(GENERIC_READ | DELETE)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&path)?;
        let handle = winapi_util::Handle::from_file(file);
        let info = winapi_util::file::information(&handle)?;
        validate_windows_empty_directory(info.file_attributes())?;
        if let Some(canonical_root) = canonical_root {
            let canonical_path = std::fs::canonicalize(path)?;
            if !canonical_path.starts_with(canonical_root) {
                return Err(std::io::Error::from(ErrorKind::DirectoryNotEmpty));
            }
        }
        Ok::<_, std::io::Error>(LockedEmptyDirectory { handle })
    })
    .await?
}

#[cfg(windows)]
// SAFETY: This helper only passes an owned live handle and one initialized
// FILE_DISPOSITION_INFO to the synchronous Windows deletion API.
#[allow(unsafe_code)]
async fn remove_windows_empty_directory(directory: LockedEmptyDirectory) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{FILE_DISPOSITION_INFO, FileDispositionInfo, SetFileInformationByHandle};

        let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
        let disposition_size = u32::try_from(std::mem::size_of_val(&disposition))
            .map_err(|_| std::io::Error::other("FILE_DISPOSITION_INFO size exceeds the Win32 API limit"))?;
        let handle = directory.handle.as_raw_handle();
        // SAFETY: `handle` is owned by `directory` and stays live for this synchronous
        // call. `disposition` is initialized with the exact structure and byte size
        // required by `FileDispositionInfo`; Windows does not retain the pointer.
        let deleted = unsafe {
            SetFileInformationByHandle(handle, FileDispositionInfo, std::ptr::from_ref(&disposition).cast(), disposition_size)
        };
        if deleted == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    })
    .await?
}

#[cfg(windows)]
struct WindowsEmptyDirectoryFrame {
    path: PathBuf,
    directory: LockedEmptyDirectory,
    entries: fs::ReadDir,
}

#[cfg(windows)]
async fn remove_empty_directory_tree_with(
    root: &Path,
    mut before_descend: impl FnMut(&Path) -> std::io::Result<()>,
    mut before_remove: impl FnMut(&Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let root_directory = lock_windows_empty_directory(root, None).await?;
    let canonical_root = fs::canonicalize(root).await?;
    let root_entries = fs::read_dir(root).await?;

    // Holding each validated directory without delete sharing keeps its path
    // generation stable until handle-relative deletion. State is O(depth).
    let mut stack = vec![WindowsEmptyDirectoryFrame {
        path: root.to_path_buf(),
        directory: root_directory,
        entries: root_entries,
    }];

    while let Some(mut frame) = stack.pop() {
        match frame.entries.next_entry().await {
            Ok(Some(entry)) => {
                let child = entry.path();
                before_descend(&child)?;
                let child_directory = match lock_windows_empty_directory(&child, Some(&canonical_root)).await {
                    Ok(directory) => directory,
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        stack.push(frame);
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                let child_entries = match fs::read_dir(&child).await {
                    Ok(entries) => entries,
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        stack.push(frame);
                        continue;
                    }
                    Err(err) if err.kind() == ErrorKind::NotADirectory => {
                        return Err(std::io::Error::from(ErrorKind::DirectoryNotEmpty));
                    }
                    Err(err) => return Err(err),
                };
                stack.push(frame);
                stack.push(WindowsEmptyDirectoryFrame {
                    path: child,
                    directory: child_directory,
                    entries: child_entries,
                });
            }
            Ok(None) => {
                before_remove(&frame.path)?;
                drop(frame.entries);
                match remove_windows_empty_directory(frame.directory).await {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) if err.kind() == ErrorKind::NotADirectory => {
                        return Err(std::io::Error::from(ErrorKind::DirectoryNotEmpty));
                    }
                    Err(err) => return Err(err),
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

#[cfg(windows)]
async fn remove_empty_directory_tree(root: &Path) -> std::io::Result<()> {
    remove_empty_directory_tree_with(root, |_| Ok(()), |_| Ok(())).await
}

#[cfg(all(not(unix), not(windows)))]
async fn remove_empty_directory_tree(root: &Path) -> std::io::Result<()> {
    fs::remove_dir(root).await
}

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_DISK_LOCAL: &str = "disk_local";
const EVENT_DISK_LOCAL_STARTUP_CLEANUP: &str = "disk_local_startup_cleanup";
const EVENT_DISK_LOCAL_BACKGROUND_CLEANUP: &str = "disk_local_background_cleanup";
const EVENT_DISK_LOCAL_SCAN_FAILED: &str = "disk_local_scan_failed";
const EVENT_DISK_LOCAL_RENAME_REJECTED: &str = "disk_local_rename_rejected";
const EVENT_DISK_LOCAL_READ_VERSION_FALLBACK: &str = "disk_local_read_version_fallback";
#[cfg(target_os = "linux")]
const EVENT_DISK_LOCAL_DIRECT_IO_FALLBACK: &str = "disk_local_direct_io_fallback";
/// A disk latched io_uring off at runtime and now reads via StdBackend
/// (rustfs/backlog#1172). The gray-release signal operators watch for.
#[cfg(target_os = "linux")]
const EVENT_DISK_LOCAL_URING_LATCH_OFF: &str = "disk_local_uring_latch_off";
const EVENT_DISK_LOCAL_DELETE_FAILED: &str = "disk_local_delete_failed";
const EVENT_DISK_LOCAL_DELETE_ROLLBACK_FAILED: &str = "disk_local_delete_rollback_failed";
const EVENT_DISK_LOCAL_CHECK_PARTS: &str = "disk_local_check_parts";
const EVENT_DISK_LOCAL_ACCESS_FAILED: &str = "disk_local_access_failed";
const EVENT_DISK_LOCAL_VOLUME_SETUP_FAILED: &str = "disk_local_volume_setup_failed";
const EVENT_DISK_LOCAL_FORMAT_DECODE_FAILED: &str = "disk_local_format_decode_failed";
/// A healing commit could not trash the stale destination data dir it is about
/// to replace. Best effort — the rename that follows fails closed — but a
/// recurring signal means heal is stuck on that drive.
const EVENT_DISK_LOCAL_HEAL_PURGE_FAILED: &str = "disk_local_heal_purge_failed";
const METRIC_GET_OBJECT_MMAP_PAGE_FAULTS_TOTAL: &str = "rustfs_io_get_object_mmap_page_faults_total";
const METRIC_GET_OBJECT_DIRECT_READ_PAGE_FAULTS_TOTAL: &str = "rustfs_io_get_object_direct_read_page_faults_total";
// io_uring read-backend gray-release observability (rustfs/backlog#1172).
#[cfg(target_os = "linux")]
const METRIC_URING_LATCH_TOTAL: &str = "rustfs_io_uring_latch_off_total";
#[cfg(target_os = "linux")]
const METRIC_URING_FALLBACK_TOTAL: &str = "rustfs_io_uring_read_fallback_total";
#[cfg(target_os = "linux")]
const METRIC_URING_IN_FLIGHT: &str = "rustfs_io_uring_in_flight";
#[cfg(target_os = "linux")]
const METRIC_URING_CQ_OVERFLOW: &str = "rustfs_io_uring_cq_overflow";
#[cfg(target_os = "linux")]
const METRIC_URING_CANCEL_ALREADY: &str = "rustfs_io_uring_cancel_already";
/// Read-side EINVAL/EOPNOTSUPP from a native O_DIRECT read that happened AFTER a
/// successful O_DIRECT open (rustfs/backlog#1214). Unlike an open-time refusal
/// (unsupported filesystem), this most likely means an alignment bug in the
/// aligned read path, so it is surfaced with a counter + warn instead of a
/// once-per-disk debug trace.
#[cfg(target_os = "linux")]
const METRIC_URING_DIRECT_READ_EINVAL_TOTAL: &str = "rustfs_io_uring_direct_read_einval_total";
/// How often the per-disk driver StatsSnapshot is exported to metrics
/// (rustfs/backlog#1172).
#[cfg(target_os = "linux")]
const URING_STATS_EXPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

#[inline(always)]
fn record_mmap_copy_stage(metrics: MmapCopyStageMetrics, stage: &'static str, started_at: Option<std::time::Instant>) {
    if let Some(started_at) = started_at {
        rustfs_io_metrics::record_get_object_stage_duration(metrics.path, stage, started_at.elapsed().as_secs_f64());
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MmapPageFaultCounts {
    minor: libc::c_long,
    major: libc::c_long,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MmapPageFaultDelta {
    minor: u64,
    major: u64,
}

#[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
fn mmap_rusage_who() -> libc::c_int {
    libc::RUSAGE_THREAD
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn mmap_rusage_who() -> libc::c_int {
    libc::RUSAGE_SELF
}

#[cfg(unix)]
// SAFETY: this allowance is limited to reading kernel-provided rusage data via
// libc; each unsafe operation below documents pointer validity and initialization.
#[allow(unsafe_code)]
fn read_mmap_page_fault_counts(enabled: bool) -> Option<MmapPageFaultCounts> {
    if !enabled {
        return None;
    }

    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` writes to the provided `rusage` pointer when it
    // returns 0. The pointer is valid for writes and initialized only on success.
    let rc = unsafe { libc::getrusage(mmap_rusage_who(), usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }

    // SAFETY: `getrusage` returned success, so `usage` has been initialized.
    let usage = unsafe { usage.assume_init() };
    Some(MmapPageFaultCounts {
        minor: usage.ru_minflt,
        major: usage.ru_majflt,
    })
}

#[cfg(unix)]
fn non_negative_fault_delta(before: libc::c_long, after: libc::c_long) -> u64 {
    if after <= before {
        return 0;
    }

    u64::try_from(after - before).unwrap_or(u64::MAX)
}

#[cfg(unix)]
fn mmap_page_fault_delta(before: Option<MmapPageFaultCounts>, after: Option<MmapPageFaultCounts>) -> MmapPageFaultDelta {
    match (before, after) {
        (Some(before), Some(after)) => MmapPageFaultDelta {
            minor: non_negative_fault_delta(before.minor, after.minor),
            major: non_negative_fault_delta(before.major, after.major),
        },
        _ => MmapPageFaultDelta::default(),
    }
}

#[cfg(unix)]
fn record_mmap_page_fault_delta(path: &'static str, stage: &'static str, delta: MmapPageFaultDelta) {
    if delta.minor > 0 {
        counter!(
            METRIC_GET_OBJECT_MMAP_PAGE_FAULTS_TOTAL,
            "path" => path,
            "stage" => stage,
            "kind" => "minor",
        )
        .increment(delta.minor);
    }

    if delta.major > 0 {
        counter!(
            METRIC_GET_OBJECT_MMAP_PAGE_FAULTS_TOTAL,
            "path" => path,
            "stage" => stage,
            "kind" => "major",
        )
        .increment(delta.major);
    }
}

#[cfg(unix)]
fn record_direct_read_page_fault_delta(path: &'static str, stage: &'static str, delta: MmapPageFaultDelta) {
    if delta.minor > 0 {
        counter!(
            METRIC_GET_OBJECT_DIRECT_READ_PAGE_FAULTS_TOTAL,
            "path" => path,
            "stage" => stage,
            "kind" => "minor",
        )
        .increment(delta.minor);
    }

    if delta.major > 0 {
        counter!(
            METRIC_GET_OBJECT_DIRECT_READ_PAGE_FAULTS_TOTAL,
            "path" => path,
            "stage" => stage,
            "kind" => "major",
        )
        .increment(delta.major);
    }
}

/// Enable O_DIRECT for large sequential reads.
/// When enabled, shard reads bypass the page cache using O_DIRECT flag.
/// Requires aligned buffers (typically 512 bytes or 4096 bytes).
/// Default: false (uses page cache via mmap/pread).
#[allow(
    dead_code,
    reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)"
)]
const ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE: &str = "RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE";
#[allow(
    dead_code,
    reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)"
)]
const DEFAULT_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE: bool = false;

/// Minimum shard size threshold for O_DIRECT reads.
/// Only shards larger than this threshold will use O_DIRECT.
/// Default: 4MB.
#[allow(
    dead_code,
    reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)"
)]
const ENV_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD: &str = "RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD";
#[allow(
    dead_code,
    reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)"
)]
const DEFAULT_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD: usize = 4 * 1024 * 1024;

/// Enable O_DIRECT for erasure shard / multipart part data writes (Linux only).
/// When enabled, `create_file` streams shard bytes straight to the device with
/// O_DIRECT, so the commit-point `sync_dir_files` fdatasync no longer flushes
/// ~2 MiB of dirty pages inside the `rename_data` critical section (it degrades
/// to a cheap metadata/device FLUSH). Aligned whole blocks are written direct;
/// the trailing sub-alignment remainder falls back to a buffered write after
/// clearing O_DIRECT (MinIO's recipe). Durability is unchanged: the file is
/// still fdatasynced at the commit point by the unchanged `sync_dir_files`.
/// EINVAL/EOPNOTSUPP (tmpfs, overlayfs, 9p, ...) latch the path off and fall
/// back to buffered writes for the whole disk. Non-Linux always falls back.
/// Default: false (buffered writes via the page cache, as before).
#[allow(
    dead_code,
    reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)"
)]
const ENV_RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE: &str = "RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE";
#[allow(
    dead_code,
    reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)"
)]
const DEFAULT_RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE: bool = false;
const ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE: &str = "RUSTFS_OBJECT_MMAP_POPULATE_ENABLE";
const DEFAULT_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE: bool = false;
const ENV_RUSTFS_OBJECT_MMAP_READ_METHOD: &str = "RUSTFS_OBJECT_MMAP_READ_METHOD";
const RUSTFS_OBJECT_MMAP_READ_METHOD_MMAP_COPY: &str = "mmap_copy";
const RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY: &str = "direct_read_copy";

/// Legacy binary switch for commit-point durability (fsync writes and renames).
/// Kept for compatibility: `true` maps to the `strict` durability mode (the
/// default), `false` keeps its historical semantics of disabling every fsync
/// on this disk, system-critical metadata included. Superseded by
/// `RUSTFS_DURABILITY_MODE`, which takes precedence when both are set.
/// Default: true.
const ENV_RUSTFS_DRIVE_SYNC_ENABLE: &str = "RUSTFS_DRIVE_SYNC_ENABLE";
const DEFAULT_RUSTFS_DRIVE_SYNC_ENABLE: bool = true;

/// Durability tier for object data-path writes: `strict` (default) | `relaxed` | `none`.
/// See docs/operations/durability-modes.md for the power-loss guarantee matrix.
const ENV_RUSTFS_DURABILITY_MODE: &str = "RUSTFS_DURABILITY_MODE";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalReadCopyMethod {
    MmapCopy,
    DirectReadCopy,
}

/// Snapshot an env-sourced read-path switch once, then serve it from memory.
///
/// The value comes from the process environment, which is fixed at launch, so a
/// per-read `std::env::var` (a global lock plus a `String` allocation, and for
/// bools a `to_ascii_lowercase`) is pure hot-path tax at the IOPS these reads run
/// at. In production the closure runs once via `LazyLock`; under `cfg(test)` it
/// runs every call so `temp_env` overrides stay observable and tests keep their
/// isolation. Invalid-value fallback (backlog#1130) lives in the closure, so it
/// is identical on both paths.
macro_rules! cached_read_env {
    ($(#[$meta:meta])* fn $name:ident() -> $ty:ty = $read:expr;) => {
        $(#[$meta])*
        #[inline]
        fn $name() -> $ty {
            fn read_env() -> $ty {
                $read
            }
            #[cfg(test)]
            {
                read_env()
            }
            #[cfg(not(test))]
            {
                static VALUE: std::sync::LazyLock<$ty> = std::sync::LazyLock::new(read_env as fn() -> $ty);
                *VALUE
            }
        }
    };
}

cached_read_env! {
    /// Check if O_DIRECT reads are enabled.
    #[allow(dead_code, reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)")]
    fn is_direct_io_read_enabled() -> bool =
        rustfs_utils::get_env_bool(ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE, DEFAULT_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE);
}

cached_read_env! {
    /// Check if O_DIRECT shard/part data writes are enabled.
    #[allow(dead_code, reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)")]
    fn is_direct_io_write_enabled() -> bool =
        rustfs_utils::get_env_bool(ENV_RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE, DEFAULT_RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE);
}

/// Enable the runtime-probed io_uring read backend (backlog#1104). Default:
/// false (gray-off). The backend is used only when this is set AND the per-disk
/// probe succeeds; otherwise, and on any per-read driver error, reads fall back
/// to `StdBackend` byte-for-byte.
#[cfg(target_os = "linux")]
const ENV_RUSTFS_IO_URING_READ_ENABLE: &str = "RUSTFS_IO_URING_READ_ENABLE";
#[cfg(target_os = "linux")]
const DEFAULT_RUSTFS_IO_URING_READ_ENABLE: bool = false;

/// io_uring submission-queue depth used when probing a disk (backlog#1104).
/// Backpressure caps in-flight at this value **per shard**, below that ring's CQ
/// capacity (2×), so CQ overflow is structurally unreachable.
#[cfg(target_os = "linux")]
const URING_QUEUE_DEPTH: u32 = 128;

/// Maximum bytes handed to the driver in a single op on the buffered read path
/// (rustfs/backlog#1174). Backpressure permits count OPS, not bytes, and the
/// driver zero-fills a full-size buffer per op, so an unbounded single read could
/// pin ~length bytes per permit. Reads at or below this cap take the fast
/// single-op, zero-copy path; larger reads are split into sequential chunks so
/// worst-case in-flight memory is bounded by `permits x this` per shard. Set high
/// so ordinary shard reads are never chunked.
#[cfg(target_os = "linux")]
const URING_MAX_OP_LEN: usize = 128 << 20;

/// Number of independent io_uring rings (each with its own driver thread) to run
/// per disk (backlog#1145).
///
/// A buffered read that hits the page cache completes inline inside
/// `io_uring_enter`, so the thread driving a ring performs that read's memcpy;
/// one ring per disk therefore caps cache-hit reads at a single core's memory
/// bandwidth. Sharding lifts that ceiling roughly linearly. Measured on a
/// 16-core host: 1 MiB reads went from 4911 MB/s (1 shard) to 47361 MB/s (8),
/// and 64 KiB reads at concurrency 32 from 124k to 345k IOPS — while keeping
/// io_uring's tail-latency advantage.
///
/// Cost is `disks × shards` driver threads, each normally blocked in `poll(2)`.
/// The default stays modest for that reason; raise it on cache-heavy workloads.
#[cfg(target_os = "linux")]
const ENV_RUSTFS_IO_URING_SHARDS: &str = "RUSTFS_IO_URING_SHARDS";
#[cfg(target_os = "linux")]
const MAX_URING_SHARDS: usize = 16;

/// Shards per disk: `RUSTFS_IO_URING_SHARDS` when set, else a quarter of the
/// available parallelism clamped to `1..=4`. Clamped to `1..=MAX_URING_SHARDS`
/// so a mistyped env var cannot spawn an unbounded number of driver threads per
/// disk.
#[cfg(target_os = "linux")]
fn get_io_uring_shards() -> usize {
    let default = std::thread::available_parallelism()
        .map(|n| (n.get() / 4).clamp(1, 4))
        .unwrap_or(1);
    rustfs_utils::get_env_usize(ENV_RUSTFS_IO_URING_SHARDS, default).clamp(1, MAX_URING_SHARDS)
}

/// Check if the runtime-probed io_uring read backend is enabled.
#[cfg(target_os = "linux")]
fn is_io_uring_read_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_RUSTFS_IO_URING_READ_ENABLE, DEFAULT_RUSTFS_IO_URING_READ_ENABLE)
}

const EVENT_DISK_LOCAL_DURABILITY_MODE: &str = "disk_local_durability_mode";

/// Process-wide durability tier for commit-point fsync work on the local disk.
///
/// `Strict` is the default and preserves the historical (fully synced) write
/// path bit for bit. The other tiers are opt-in and only relax the object
/// data path; writes committing into system-critical namespaces stay pinned
/// to `Strict` (see [`effective_durability`]), except under `LegacyOff`,
/// which keeps the exact historical semantics of
/// `RUSTFS_DRIVE_SYNC_ENABLE=false` (no fsync anywhere, system metadata
/// included) so existing deployments keep their behavior unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DurabilityMode {
    /// Every commit point is fsynced: shard/part contents, xl.meta contents,
    /// rollback backups, and the directory entries of commit renames.
    Strict,
    /// Object payload bytes (erasure shard files, multipart part files) are
    /// still fdatasynced before the commit rename, but metadata commits
    /// (xl.meta contents, rollback backups, directory entries) are left to
    /// the page cache. Aligned with MinIO's default durability posture.
    Relaxed,
    /// No fsync on the object data path at all. System-critical namespaces
    /// are still pinned to `Strict`.
    None,
    /// Historical semantics of `RUSTFS_DRIVE_SYNC_ENABLE=false`: no fsync
    /// anywhere, without the system-critical pinning. Only reachable through
    /// the legacy switch; not exposed by `RUSTFS_DURABILITY_MODE`.
    LegacyOff,
}

impl DurabilityMode {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "strict" => Some(Self::Strict),
            "relaxed" => Some(Self::Relaxed),
            "none" => Some(Self::None),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Relaxed => "relaxed",
            Self::None => "none",
            Self::LegacyOff => "legacy-off",
        }
    }

    /// Whether object payload bytes (erasure shard files, multipart part
    /// files) must be fdatasynced at commit points.
    fn syncs_data_shards(self) -> bool {
        matches!(self, Self::Strict | Self::Relaxed)
    }

    /// Whether metadata commits must be fsynced: xl.meta contents, rollback
    /// backups, and the directory entries created by commit renames.
    fn syncs_commit_metadata(self) -> bool {
        matches!(self, Self::Strict)
    }
}

/// Pure resolution of the durability mode from configuration values.
///
/// `RUSTFS_DURABILITY_MODE` wins when set to a valid value; otherwise the
/// legacy `RUSTFS_DRIVE_SYNC_ENABLE` switch keeps its historical mapping
/// (`true` -> strict, `false` -> the old full-off semantics). The default is
/// strict.
fn resolve_durability_mode(mode_env: Option<String>, legacy_drive_sync_enabled: bool) -> DurabilityMode {
    if let Some(raw) = mode_env {
        if let Some(mode) = DurabilityMode::parse(&raw) {
            return mode;
        }
        warn!(
            event = EVENT_DISK_LOCAL_DURABILITY_MODE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
            value = %raw,
            "Invalid RUSTFS_DURABILITY_MODE value; expected strict|relaxed|none, falling back to the legacy drive-sync switch"
        );
    }
    if legacy_drive_sync_enabled {
        DurabilityMode::Strict
    } else {
        DurabilityMode::LegacyOff
    }
}

/// The configured durability mode, resolved from the environment once per
/// process and cached (the previous binary switch re-read the environment on
/// every call, i.e. a dozen times per PUT, and could even flip mid-operation).
pub(crate) fn durability_mode() -> DurabilityMode {
    #[cfg(test)]
    if let Some(mode) = durability_mode_override::get() {
        return mode;
    }
    static MODE: OnceLock<DurabilityMode> = OnceLock::new();
    *MODE.get_or_init(|| {
        let mode = resolve_durability_mode(
            rustfs_utils::get_env_opt_str(ENV_RUSTFS_DURABILITY_MODE),
            rustfs_utils::get_env_bool(ENV_RUSTFS_DRIVE_SYNC_ENABLE, DEFAULT_RUSTFS_DRIVE_SYNC_ENABLE),
        );
        info!(
            event = EVENT_DISK_LOCAL_DURABILITY_MODE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
            mode = mode.as_str(),
            "Storage durability mode resolved"
        );
        mode
    })
}

/// Test-only override for [`durability_mode`].
///
/// The production value is resolved from the environment once per process, so
/// tests exercising non-default tiers need a process-level override hook.
/// Setting an override serializes callers on a mutex so a relaxed-tier test
/// can never leak its mode into a parallel strict-tier test.
#[cfg(test)]
pub(crate) mod durability_mode_override {
    use super::DurabilityMode;
    use std::sync::{Mutex, MutexGuard, PoisonError, RwLock};

    static OVERRIDE: RwLock<Option<DurabilityMode>> = RwLock::new(None);
    static SERIAL: Mutex<()> = Mutex::new(());

    pub(crate) fn get() -> Option<DurabilityMode> {
        *OVERRIDE.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Holds the override (and the serialization lock) until dropped.
    pub(crate) struct OverrideGuard {
        _serial: MutexGuard<'static, ()>,
    }

    impl Drop for OverrideGuard {
        fn drop(&mut self) {
            *OVERRIDE.write().unwrap_or_else(PoisonError::into_inner) = None;
        }
    }

    pub(crate) fn set(mode: DurabilityMode) -> OverrideGuard {
        let serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        *OVERRIDE.write().unwrap_or_else(PoisonError::into_inner) = Some(mode);
        OverrideGuard { _serial: serial }
    }
}

/// Per-bucket durability overrides (HP-5 phase 2, rustfs/backlog#938).
///
/// The disk layer never loads bucket metadata itself: the bucket metadata
/// subsystem publishes the parsed override here whenever a bucket's cached
/// metadata is set, refreshed, or removed, so this registry follows exactly
/// the existing bucket-metadata cache invalidation semantics (immediate on
/// the node applying a config change, peer reload notification plus the
/// periodic refresh loop elsewhere). Lookups sit on the commit hot path, so
/// the empty-registry case (no bucket overrides configured anywhere — the
/// default) is a single relaxed atomic load and the phase 1 behavior is
/// preserved bit for bit.
pub(crate) mod bucket_durability {
    use super::{
        DurabilityMode, EVENT_DISK_LOCAL_DURABILITY_MODE, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_DISK_LOCAL, is_scratch_volume,
        is_system_critical_volume,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{OnceLock, PoisonError, RwLock};
    use tracing::{info, warn};

    static OVERRIDES: OnceLock<RwLock<HashMap<String, DurabilityMode>>> = OnceLock::new();
    /// Fast-path gate: false means "no override registered anywhere", which
    /// keeps default deployments off the map lookup entirely.
    static NON_EMPTY: AtomicBool = AtomicBool::new(false);

    fn overrides() -> &'static RwLock<HashMap<String, DurabilityMode>> {
        OVERRIDES.get_or_init(|| RwLock::new(HashMap::new()))
    }

    /// Publish (or clear, with `None`) the durability override for `bucket`.
    ///
    /// System namespaces can never carry an override: they are pinned to
    /// `strict` by [`super::effective_durability`], and any attempt to
    /// register one is rejected here as defense in depth.
    pub(crate) fn set(bucket: &str, mode: Option<DurabilityMode>) {
        if bucket.is_empty() {
            return;
        }
        if is_system_critical_volume(bucket) || is_scratch_volume(bucket) {
            if mode.is_some() {
                warn!(
                    event = EVENT_DISK_LOCAL_DURABILITY_MODE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    bucket = %bucket,
                    "Rejected per-bucket durability override for a system namespace; it stays pinned to strict"
                );
            }
            return;
        }
        // The legacy full-off switch is process-wide only and deliberately
        // unreachable per bucket (`DurabilityMode::parse` never returns it).
        let mode = mode.filter(|m| *m != DurabilityMode::LegacyOff);

        let mut map = overrides().write().unwrap_or_else(PoisonError::into_inner);
        let changed = match mode {
            Some(mode) => map.insert(bucket.to_string(), mode) != Some(mode),
            None => map.remove(bucket).is_some(),
        };
        NON_EMPTY.store(!map.is_empty(), Ordering::Release);
        drop(map);

        if changed {
            info!(
                event = EVENT_DISK_LOCAL_DURABILITY_MODE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                bucket = %bucket,
                mode = mode.map_or("inherit", |m| m.as_str()),
                "Per-bucket durability override updated"
            );
        }
    }

    /// The override registered for `volume`, if any. `volume` is the commit
    /// destination, so user buckets resolve by name while scratch and system
    /// namespaces never match (they are refused by [`set`]).
    pub(crate) fn lookup(volume: &str) -> Option<DurabilityMode> {
        if !NON_EMPTY.load(Ordering::Acquire) {
            return None;
        }
        overrides()
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(volume)
            .copied()
    }
}

/// Whether `volume` stages in-flight user object data (`.rustfs.sys/tmp`,
/// `.rustfs.sys/multipart`, and their subtrees). These namespaces follow the
/// configured durability mode: their contents commit into user buckets and
/// are exactly the writes the relaxed tiers exist for.
fn is_scratch_volume(volume: &str) -> bool {
    for scratch in [RUSTFS_META_TMP_BUCKET, super::RUSTFS_META_MULTIPART_BUCKET] {
        if volume == scratch || volume.strip_prefix(scratch).is_some_and(|rest| rest.starts_with('/')) {
            return true;
        }
    }
    false
}

/// Whether writes committing into `volume` carry system-critical state:
/// format.json, IAM and cluster config, bucket metadata, and everything else
/// under `.rustfs.sys` (or `.minio.sys` during migration) outside the scratch
/// namespaces. Losing these can take out the whole deployment and they are
/// far off the object hot path, so they never follow a relaxed tier.
fn is_system_critical_volume(volume: &str) -> bool {
    if is_scratch_volume(volume) {
        return false;
    }
    for meta in [RUSTFS_META_BUCKET, super::MIGRATING_META_BUCKET] {
        if volume == meta || volume.strip_prefix(meta).is_some_and(|rest| rest.starts_with('/')) {
            return true;
        }
    }
    false
}

/// Effective durability for writes that commit into `volume`.
///
/// Resolution order: system-critical volumes are pinned to `Strict`
/// regardless of any configuration; otherwise a per-bucket override
/// (published by the bucket metadata subsystem, see [`bucket_durability`])
/// wins over the process-wide mode; otherwise the process-wide mode applies.
/// The legacy full-off switch keeps its historical semantics: it is never
/// pinned and per-bucket overrides do not apply under it.
pub(crate) fn effective_durability(volume: &str) -> DurabilityMode {
    let global = durability_mode();
    if global == DurabilityMode::LegacyOff {
        return global;
    }
    if is_system_critical_volume(volume) {
        return DurabilityMode::Strict;
    }
    bucket_durability::lookup(volume).unwrap_or(global)
}

cached_read_env! {
    /// Get the O_DIRECT read threshold size.
    #[allow(dead_code, reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)")]
    fn get_direct_io_read_threshold() -> usize =
        rustfs_utils::get_env_usize(ENV_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD, DEFAULT_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD);
}

cached_read_env! {
    /// Whether mmap reads should fault the mapping in with `MAP_POPULATE`.
    fn mmap_populate_enabled() -> bool =
        rustfs_utils::get_env_bool(ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE, DEFAULT_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE);
}

fn should_populate_mmap_read(length: usize) -> bool {
    length > 0 && mmap_populate_enabled()
}

cached_read_env! {
    fn local_read_copy_method() -> LocalReadCopyMethod = {
        let method = rustfs_utils::get_env_str(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, RUSTFS_OBJECT_MMAP_READ_METHOD_MMAP_COPY);
        match method.as_str() {
            RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY => LocalReadCopyMethod::DirectReadCopy,
            _ => LocalReadCopyMethod::MmapCopy,
        }
    };
}

/// Runtime state for the true O_DIRECT read path (Linux only).
///
/// `supported` starts true and latches false on the first EINVAL/EOPNOTSUPP
/// from an O_DIRECT open/read (tmpfs, overlayfs, and 9p commonly reject the
/// flag); the path then permanently falls back to the buffered read methods
/// for this disk. `align` caches the DIO alignment probed from the backing
/// filesystem. O_DIRECT errors must never surface to callers: EINVAL maps to
/// `FileNotFound` in `to_file_error`, which would masquerade as a missing
/// shard and trigger spurious EC rebuilds.
#[cfg(target_os = "linux")]
#[derive(Debug)]
struct DirectIoReadState {
    supported: AtomicBool,
    align: OnceLock<usize>,
    fallback_logged: AtomicBool,
}

#[cfg(target_os = "linux")]
impl DirectIoReadState {
    fn new() -> Self {
        Self {
            supported: AtomicBool::new(true),
            align: OnceLock::new(),
            fallback_logged: AtomicBool::new(false),
        }
    }
}

#[cfg(target_os = "linux")]
const DEFAULT_DIRECT_IO_ALIGN: usize = 4096;

/// Probe the DIO alignment requirement for the file's filesystem via
/// statx STATX_DIOALIGN (kernel >= 6.1). Falls back to 4096, a safe upper
/// bound for 512e/4Kn devices, when the kernel or filesystem does not
/// report it.
#[cfg(target_os = "linux")]
fn probe_direct_io_align(file: &std::fs::File) -> usize {
    use rustix::fs::{AtFlags, StatxFlags};

    match rustix::fs::statx(file, "", AtFlags::EMPTY_PATH, StatxFlags::DIOALIGN) {
        Ok(stx) => {
            if StatxFlags::from_bits_retain(stx.stx_mask).contains(StatxFlags::DIOALIGN) {
                let align = stx.stx_dio_mem_align.max(stx.stx_dio_offset_align) as usize;
                if align.is_power_of_two() && align >= 512 {
                    return align;
                }
            }
            DEFAULT_DIRECT_IO_ALIGN
        }
        Err(_) => DEFAULT_DIRECT_IO_ALIGN,
    }
}

/// Heap buffer with explicit alignment for O_DIRECT reads.
#[cfg(target_os = "linux")]
struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    len: usize,
    layout: std::alloc::Layout,
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
impl AlignedBuf {
    fn new(len: usize, align: usize) -> std::io::Result<Self> {
        debug_assert!(len > 0, "AlignedBuf must not be zero-sized");
        let layout =
            std::alloc::Layout::from_size_align(len, align).map_err(|e| std::io::Error::new(ErrorKind::InvalidInput, e))?;
        // SAFETY: `layout` has non-zero size (callers guarantee len > 0) and a
        // valid power-of-two alignment enforced by Layout::from_size_align.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = std::ptr::NonNull::new(ptr).ok_or(ErrorKind::OutOfMemory)?;
        Ok(Self { ptr, len, layout })
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is a live allocation of exactly `len` bytes owned by
        // self, initialized to zero at allocation and only written via
        // `as_mut_slice`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as in `as_slice`, plus `&mut self` guarantees exclusivity.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`layout` come from the successful alloc_zeroed in new().
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

#[cfg(target_os = "linux")]
fn is_direct_io_unsupported(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::EINVAL) | Some(libc::EOPNOTSUPP))
}

/// True O_DIRECT positioned read: open with O_DIRECT, read the aligned
/// superset range into an aligned bounce buffer, then slice out the exact
/// logical range. Alignment padding never leaks to callers — BitrotReader
/// reads exact shard_size and would flag padded output as corruption.
///
/// Short reads are legal for O_DIRECT; the loop stops at EOF (res == 0).
/// A read that ends before covering the logical range is an error (the
/// caller has already validated `offset + length <= file size`, so this
/// only happens on concurrent truncation) and makes the caller fall back
/// to the buffered path.
#[cfg(target_os = "linux")]
fn pread_direct_aligned(file_path: &Path, offset: u64, length: usize, state: &DirectIoReadState) -> std::io::Result<Bytes> {
    use std::os::unix::fs::{FileExt, OpenOptionsExt};

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::DIRECT.bits() as i32)
        .open(file_path)?;

    let align = *state.align.get_or_init(|| probe_direct_io_align(&file));
    let align_u64 = align as u64;

    let aligned_offset = offset - (offset % align_u64);
    let logical_start = usize::try_from(offset - aligned_offset).map_err(|_| std::io::Error::from(ErrorKind::InvalidInput))?;
    let logical_end = logical_start.checked_add(length).ok_or(ErrorKind::InvalidInput)?;
    let aligned_len = logical_end.checked_add(align - 1).ok_or(ErrorKind::InvalidInput)? / align * align;

    let mut buf = AlignedBuf::new(aligned_len, align)?;

    let mut filled = 0usize;
    while filled < aligned_len {
        // `filled` stays a multiple of `align` except possibly at EOF, so
        // both the buffer address and the file offset remain aligned.
        let n = file.read_at(&mut buf.as_mut_slice()[filled..], aligned_offset + filled as u64)?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    if filled < logical_end {
        return Err(std::io::Error::new(ErrorKind::UnexpectedEof, "short O_DIRECT read"));
    }

    Ok(Bytes::copy_from_slice(&buf.as_slice()[logical_start..logical_end]))
}

// `AlignedBuf` uniquely owns a single heap allocation reached only through
// `&self`/`&mut self`; there is no interior mutability and no aliasing, so it
// is sound to move it across threads (into a `spawn_blocking` flush closure)
// and to share `&AlignedBuf` between threads.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
// SAFETY: exclusive heap ownership, no aliasing (see the note above).
unsafe impl Send for AlignedBuf {}
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
// SAFETY: `&AlignedBuf` only exposes read-only access to an immutable buffer.
unsafe impl Sync for AlignedBuf {}

/// Runtime state for the true O_DIRECT write path (Linux only), mirroring
/// [`DirectIoReadState`].
///
/// `supported` starts true and latches false on the first EINVAL/EOPNOTSUPP
/// from an O_DIRECT open (tmpfs, overlayfs, and 9p commonly reject the flag);
/// `create_file` then permanently opens shard files buffered for this disk.
/// `align` caches the DIO alignment probed from the backing filesystem. As on
/// the read path, an O_DIRECT open error must never surface to callers: EINVAL
/// maps to `FileNotFound` in `to_file_error`, which would masquerade as a
/// missing shard and trigger spurious EC rebuilds.
#[cfg(target_os = "linux")]
#[derive(Debug)]
struct DirectIoWriteState {
    supported: AtomicBool,
    align: OnceLock<usize>,
    fallback_logged: AtomicBool,
}

#[cfg(target_os = "linux")]
impl DirectIoWriteState {
    fn new() -> Self {
        Self {
            supported: AtomicBool::new(true),
            align: OnceLock::new(),
            fallback_logged: AtomicBool::new(false),
        }
    }
}

/// Target staging size for O_DIRECT writes, rounded up to the DIO alignment.
/// Bounds the per-writer aligned bounce buffer and batches many shard blocks
/// into one positioned write to keep the syscall count low.
#[allow(
    dead_code,
    reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)"
)]
const DIRECT_WRITE_STAGING_BYTES: usize = 1024 * 1024;

/// Aligned bounce-buffer capacity for a given DIO alignment: the target staging
/// size rounded up to a whole multiple of `align` so the buffer address, every
/// flushed batch length, and every write offset stay alignment-correct.
/// Platform-independent (no O_DIRECT), so it is unit-tested on any host.
#[allow(
    dead_code,
    reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)"
)]
fn direct_write_staging_capacity(align: usize) -> usize {
    debug_assert!(align.is_power_of_two() && align >= 512);
    DIRECT_WRITE_STAGING_BYTES.div_ceil(align) * align
}

/// Split `filled` staged bytes into the alignment-sized prefix written with
/// O_DIRECT and the sub-alignment tail written buffered. Platform-independent,
/// so the tail-boundary math is unit-tested on any host.
#[allow(
    dead_code,
    reason = "platform-conditional: production callers are inside #[cfg(target_os = \"linux\")] blocks, so this reads as dead on non-Linux hosts (backlog#1823)"
)]
fn direct_write_tail_split(filled: usize, align: usize) -> (usize, usize) {
    let aligned = filled - (filled % align);
    (aligned, filled - aligned)
}

/// Positioned write-all helper: retries short writes at increasing offsets.
///
/// Under O_DIRECT the buffer address, `offset`, and length must all be aligned;
/// callers guarantee that. After O_DIRECT has been cleared (tail path) there is
/// no alignment requirement.
#[cfg(target_os = "linux")]
fn pwrite_all(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let n = file.write_at(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(ErrorKind::WriteZero, "O_DIRECT positioned write wrote 0 bytes"));
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

/// Never let an O_DIRECT write error reach `to_file_error` as `InvalidInput`:
/// that maps to `FileNotFound` and would masquerade as a missing shard,
/// triggering a spurious EC rebuild (backlog#897 / issue correction #2). Any
/// EINVAL/EOPNOTSUPP surfacing from a flush is remapped to a generic error so
/// the write-quorum machinery treats it as the real write failure it is.
#[cfg(target_os = "linux")]
fn sanitize_direct_write_error(err: std::io::Error) -> std::io::Error {
    if is_direct_io_unsupported(&err) {
        std::io::Error::other(format!("O_DIRECT shard write failed: {err}"))
    } else {
        err
    }
}

/// Owned O_DIRECT write state moved in and out of the `spawn_blocking` flush
/// closures so the reactor is never blocked on synchronous device I/O.
#[cfg(target_os = "linux")]
struct DirectWriteInner {
    file: std::fs::File,
    /// Aligned bounce buffer; its capacity (`buf.len`) is a whole multiple of
    /// `align`.
    buf: AlignedBuf,
    /// Bytes currently staged in `buf` and not yet written to the device.
    filled: usize,
    /// Next file offset for an O_DIRECT positioned write; always a multiple of
    /// `align` because every batch flushed before the tail is a whole multiple.
    write_offset: u64,
    align: usize,
    direct_cleared: bool,
}

#[cfg(target_os = "linux")]
impl DirectWriteInner {
    /// Flush a full staging batch (`filled == buf capacity`, a multiple of
    /// `align`) straight to the device with O_DIRECT.
    fn flush_batch(&mut self) -> std::io::Result<()> {
        if self.filled == 0 {
            return Ok(());
        }
        debug_assert_eq!(self.filled % self.align, 0, "batch flush must be alignment-sized");
        pwrite_all(&self.file, &self.buf.as_slice()[..self.filled], self.write_offset).map_err(sanitize_direct_write_error)?;
        self.write_offset += self.filled as u64;
        self.filled = 0;
        Ok(())
    }

    /// Final flush at shutdown: write the aligned prefix with O_DIRECT, then the
    /// sub-alignment tail buffered after clearing O_DIRECT (MinIO's recipe; the
    /// tail is not separately fsynced — the commit-point `sync_dir_files`
    /// fdatasync covers the whole file, issue correction #5).
    fn finish(&mut self) -> std::io::Result<()> {
        let (aligned, remainder) = direct_write_tail_split(self.filled, self.align);
        if aligned > 0 {
            pwrite_all(&self.file, &self.buf.as_slice()[..aligned], self.write_offset).map_err(sanitize_direct_write_error)?;
            self.write_offset += aligned as u64;
        }

        if remainder > 0 {
            self.clear_direct()?;
            // Snapshot the slice bounds first to avoid borrowing `self.buf`
            // while `self.file` is borrowed immutably below.
            let start = aligned;
            let end = self.filled;
            pwrite_all(&self.file, &self.buf.as_slice()[start..end], self.write_offset)?;
            self.write_offset += remainder as u64;
        }
        self.filled = 0;
        Ok(())
    }

    /// Drop the O_DIRECT flag from the open file so the unaligned tail can be
    /// written through the page cache without an alignment fault.
    fn clear_direct(&mut self) -> std::io::Result<()> {
        if self.direct_cleared {
            return Ok(());
        }
        let flags = rustix::fs::fcntl_getfl(&self.file).map_err(std::io::Error::from)?;
        rustix::fs::fcntl_setfl(&self.file, flags - rustix::fs::OFlags::DIRECT).map_err(std::io::Error::from)?;
        self.direct_cleared = true;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
type DirectFlushHandle = tokio::task::JoinHandle<(DirectWriteInner, std::io::Result<()>)>;

#[cfg(target_os = "linux")]
enum DirectWriteState {
    Idle(Option<DirectWriteInner>),
    Busy(DirectFlushHandle),
}

/// Streaming O_DIRECT writer returned by `create_file` on Linux when the path
/// is enabled and supported.
///
/// Incoming bytes are memcpy'd into an aligned bounce buffer (cheap, on the
/// reactor); each full aligned batch and the shutdown tail are flushed on the
/// blocking pool so the reactor never stalls on synchronous device I/O — the
/// same offloading posture as the buffered `tokio::fs::File` writer it
/// replaces. Durability is unchanged: no fsync happens here; the commit-point
/// `sync_dir_files` fdatasync persists the file.
#[cfg(target_os = "linux")]
struct DirectWriter {
    state: DirectWriteState,
    shutdown_started: bool,
    shutdown_done: bool,
}

#[cfg(target_os = "linux")]
impl DirectWriter {
    fn new(inner: DirectWriteInner) -> Self {
        Self {
            state: DirectWriteState::Idle(Some(inner)),
            shutdown_started: false,
            shutdown_done: false,
        }
    }

    /// Build a writer over an already-open plain file with a caller-chosen
    /// alignment and staging capacity. Exercises the streaming/tail state
    /// machine deterministically on CI filesystems that reject O_DIRECT
    /// (tmpfs/overlayfs), where the production `open_direct_writer` would latch
    /// off; `write_at`/`fcntl` behave identically on a buffered file.
    #[cfg(test)]
    fn from_std_file_for_test(file: std::fs::File, align: usize, capacity: usize) -> Self {
        assert_eq!(capacity % align, 0, "test capacity must be an alignment multiple");
        let buf = AlignedBuf::new(capacity, align).expect("aligned buffer allocation");
        Self::new(DirectWriteInner {
            file,
            buf,
            filled: 0,
            write_offset: 0,
            align,
            direct_cleared: false,
        })
    }

    /// Drive an in-flight flush to completion, returning the recovered inner
    /// state. Returns `Pending`/errors verbatim; on success the state is left
    /// `Idle`.
    fn poll_drive_busy(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        if let DirectWriteState::Busy(handle) = &mut self.state {
            let (inner, res) = match std::task::ready!(std::pin::Pin::new(handle).poll(cx)) {
                Ok(pair) => pair,
                Err(join_err) => {
                    return std::task::Poll::Ready(Err(std::io::Error::other(format!("O_DIRECT flush task failed: {join_err}"))));
                }
            };
            self.state = DirectWriteState::Idle(Some(inner));
            if self.shutdown_started {
                self.shutdown_done = true;
            }
            res?;
        }
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(target_os = "linux")]
impl AsyncWrite for DirectWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                DirectWriteState::Busy(_) => {
                    std::task::ready!(this.poll_drive_busy(cx))?;
                }
                DirectWriteState::Idle(inner_opt) => {
                    if buf.is_empty() {
                        return std::task::Poll::Ready(Ok(0));
                    }
                    let inner = inner_opt.as_mut().expect("idle direct writer must hold inner state");
                    let capacity = inner.buf.len;
                    let space = capacity - inner.filled;
                    let n = space.min(buf.len());
                    let start = inner.filled;
                    inner.buf.as_mut_slice()[start..start + n].copy_from_slice(&buf[..n]);
                    inner.filled += n;

                    if inner.filled == capacity {
                        let mut inner = inner_opt.take().expect("idle direct writer must hold inner state");
                        let handle = tokio::task::spawn_blocking(move || {
                            let res = inner.flush_batch();
                            (inner, res)
                        });
                        this.state = DirectWriteState::Busy(handle);
                    }
                    return std::task::Poll::Ready(Ok(n));
                }
            }
        }
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        // Only drive an in-flight batch to completion. Sub-alignment staged
        // bytes cannot be flushed mid-stream (they would misalign the next
        // O_DIRECT offset); they are written by `poll_shutdown`.
        self.get_mut().poll_drive_busy(cx)
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                DirectWriteState::Busy(_) => {
                    std::task::ready!(this.poll_drive_busy(cx))?;
                }
                DirectWriteState::Idle(inner_opt) => {
                    if this.shutdown_done {
                        return std::task::Poll::Ready(Ok(()));
                    }
                    let mut inner = inner_opt.take().expect("idle direct writer must hold inner state");
                    this.shutdown_started = true;
                    let handle = tokio::task::spawn_blocking(move || {
                        let res = inner.finish();
                        (inner, res)
                    });
                    this.state = DirectWriteState::Busy(handle);
                }
            }
        }
    }
}

/// Open a shard file for an O_DIRECT streaming write, probing DIO alignment on
/// the freshly created file. Returns `Ok(None)` (with the state latched off and
/// a one-time warning) when the filesystem rejects O_DIRECT, so the caller can
/// fall back to the buffered writer without ever surfacing EINVAL.
#[cfg(target_os = "linux")]
fn open_direct_writer(file_path: &Path, state: &DirectIoWriteState) -> Result<Option<DirectWriter>> {
    use std::os::unix::fs::OpenOptionsExt;

    let open_result = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(file_path);

    let file = match open_result {
        Ok(file) => file,
        Err(err) => {
            if is_direct_io_unsupported(&err) {
                state.supported.store(false, Ordering::Relaxed);
                if !state.fallback_logged.swap(true, Ordering::Relaxed) {
                    warn!(
                        event = EVENT_DISK_LOCAL_DIRECT_IO_FALLBACK,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = %file_path.display(),
                        error = ?err,
                        "O_DIRECT write unavailable; falling back to buffered writes"
                    );
                }
                return Ok(None);
            }
            // A genuine open failure (permissions, disk full, ...): map it as a
            // normal file error so callers see the real cause.
            return Err(to_file_error(err).into());
        }
    };

    let align = *state.align.get_or_init(|| probe_direct_io_align(&file));
    let capacity = direct_write_staging_capacity(align);
    let buf = match AlignedBuf::new(capacity, align) {
        Ok(buf) => buf,
        Err(err) => return Err(to_file_error(err).into()),
    };

    Ok(Some(DirectWriter::new(DirectWriteInner {
        file,
        buf,
        filled: 0,
        write_offset: 0,
        align,
        direct_cleared: false,
    })))
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn mmap_page_size() -> Result<u64> {
    static PAGE_SIZE: OnceLock<Option<u64>> = OnceLock::new();

    PAGE_SIZE
        .get_or_init(|| {
            // SAFETY: `sysconf(_SC_PAGESIZE)` has no pointer arguments and only
            // queries process-global OS configuration.
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            if page_size <= 0 {
                return None;
            }
            u64::try_from(page_size).ok()
        })
        .ok_or_else(|| DiskError::other("failed to determine system page size"))
}

#[cfg(test)]
static RENAME_DATA_FAIL_BEFORE_OLD_METADATA_BACKUP: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
#[cfg(test)]
static RENAME_DATA_FAIL_AFTER_METADATA_COMMIT: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
#[cfg(test)]
static RENAME_DATA_FAIL_COMMIT_RENAME: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
#[cfg(test)]
static RENAME_DATA_REMOVE_STAGED_META_BEFORE_COMMIT: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
#[cfg(test)]
static LOCAL_INLINE_ROLLBACK_HARDLINK_FAILURE: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);
#[cfg(test)]
static RENAME_DATA_REMOVE_DST_BASE_BEFORE_COMMIT: std::sync::Mutex<Option<(String, PathBuf)>> = std::sync::Mutex::new(None);
#[cfg(test)]
type InlinePreparationHook = Box<dyn FnOnce() + Send>;
#[cfg(test)]
type RenameDataPublicationHookKey = (PathBuf, String, String);
#[cfg(test)]
static INLINE_PREPARATION_BEFORE_BACKUP: std::sync::LazyLock<std::sync::Mutex<HashMap<String, InlinePreparationHook>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
#[cfg(test)]
static INLINE_BEFORE_FILE_SYNC_ADMISSION: std::sync::LazyLock<std::sync::Mutex<HashMap<String, InlinePreparationHook>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
#[cfg(test)]
static RENAME_DATA_AFTER_FIRST_PUBLICATION: std::sync::LazyLock<
    std::sync::Mutex<HashMap<RenameDataPublicationHookKey, InlinePreparationHook>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
#[cfg(test)]
static OWNED_FILE_WRITE_BEFORE_OPEN: std::sync::LazyLock<std::sync::Mutex<HashMap<PathBuf, InlinePreparationHook>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
#[cfg(all(test, windows))]
static DESTINATION_COMMIT_DIRECTORY_PREPARATION: std::sync::LazyLock<std::sync::Mutex<HashMap<PathBuf, InlinePreparationHook>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
#[cfg(test)]
static DELETE_VERSION_FAIL_AFTER_DATA_STAGED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
#[cfg(test)]
static DELETE_VERSION_FAIL_AFTER_COMMIT: std::sync::Mutex<Vec<(PathBuf, String)>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn set_rename_data_fail_before_old_metadata_backup(dst_path: &str) {
    *RENAME_DATA_FAIL_BEFORE_OLD_METADATA_BACKUP
        .lock()
        .expect("test failpoint lock should not be poisoned") = Some(dst_path.to_string());
}

#[cfg(test)]
fn set_rename_data_fail_after_metadata_commit(dst_path: &str) {
    RENAME_DATA_FAIL_AFTER_METADATA_COMMIT
        .lock()
        .expect("test failpoint lock should not be poisoned")
        .push(dst_path.to_string());
}

#[cfg(test)]
fn set_rename_data_fail_commit_rename(dst_path: &str) {
    *RENAME_DATA_FAIL_COMMIT_RENAME
        .lock()
        .expect("test failpoint lock should not be poisoned") = Some(dst_path.to_string());
}

#[cfg(test)]
fn set_rename_data_remove_staged_meta_before_commit(dst_path: &str) {
    *RENAME_DATA_REMOVE_STAGED_META_BEFORE_COMMIT
        .lock()
        .expect("test failpoint lock should not be poisoned") = Some(dst_path.to_string());
}

#[cfg(test)]
fn set_local_inline_rollback_hardlink_failure(dst_path: &Path) {
    *LOCAL_INLINE_ROLLBACK_HARDLINK_FAILURE
        .lock()
        .expect("test failpoint lock should not be poisoned") = Some(dst_path.to_path_buf());
}

#[cfg(test)]
fn set_rename_data_remove_dst_base_before_commit(dst_path: &str, dst_base: &Path) {
    *RENAME_DATA_REMOVE_DST_BASE_BEFORE_COMMIT
        .lock()
        .expect("test failpoint lock should not be poisoned") = Some((dst_path.to_string(), dst_base.to_path_buf()));
}

#[cfg(test)]
fn set_inline_preparation_before_backup(dst_path: &str, hook: impl FnOnce() + Send + 'static) {
    INLINE_PREPARATION_BEFORE_BACKUP
        .lock()
        .expect("test preparation hook lock should not be poisoned")
        .insert(dst_path.to_string(), Box::new(hook));
}

#[cfg(test)]
fn set_inline_before_file_sync_admission(dst_path: &str, hook: impl FnOnce() + Send + 'static) {
    INLINE_BEFORE_FILE_SYNC_ADMISSION
        .lock()
        .expect("test admission hook lock should not be poisoned")
        .insert(dst_path.to_string(), Box::new(hook));
}

#[cfg(test)]
fn set_rename_data_after_first_publication(root: &Path, dst_volume: &str, dst_path: &str, hook: impl FnOnce() + Send + 'static) {
    RENAME_DATA_AFTER_FIRST_PUBLICATION
        .lock()
        .expect("test publication hook lock should not be poisoned")
        .insert((root.to_path_buf(), dst_volume.to_string(), dst_path.to_string()), Box::new(hook));
}

#[cfg(test)]
fn set_owned_file_write_before_open(path: &Path, hook: impl FnOnce() + Send + 'static) {
    OWNED_FILE_WRITE_BEFORE_OPEN
        .lock()
        .expect("test file write hook lock should not be poisoned")
        .insert(path.to_path_buf(), Box::new(hook));
}

#[cfg(all(test, windows))]
fn set_destination_commit_directory_preparation(path: &Path, hook: impl FnOnce() + Send + 'static) {
    DESTINATION_COMMIT_DIRECTORY_PREPARATION
        .lock()
        .expect("test destination preparation hook lock should not be poisoned")
        .insert(path.to_path_buf(), Box::new(hook));
}

#[cfg(test)]
fn set_delete_version_fail_after_data_staged(path: &str) {
    DELETE_VERSION_FAIL_AFTER_DATA_STAGED
        .lock()
        .expect("test failpoint lock should not be poisoned")
        .push(path.to_string());
}

#[cfg(test)]
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub(crate) fn set_delete_version_fail_after_commit(root: &Path, path: &str) {
    DELETE_VERSION_FAIL_AFTER_COMMIT
        .lock()
        .expect("test failpoint lock should not be poisoned")
        .push((root.to_path_buf(), path.to_string()));
}

#[cfg(test)]
fn should_fail_before_old_metadata_backup(dst_path: &str) -> bool {
    let mut target = RENAME_DATA_FAIL_BEFORE_OLD_METADATA_BACKUP
        .lock()
        .expect("test failpoint lock should not be poisoned");
    if target.as_deref() == Some(dst_path) {
        target.take();
        true
    } else {
        false
    }
}

#[cfg(test)]
fn should_fail_after_metadata_commit(dst_path: &str) -> bool {
    let mut targets = RENAME_DATA_FAIL_AFTER_METADATA_COMMIT
        .lock()
        .expect("test failpoint lock should not be poisoned");
    if let Some(index) = targets.iter().position(|target| target == dst_path) {
        targets.remove(index);
        true
    } else {
        false
    }
}

#[cfg(test)]
fn should_fail_commit_rename(dst_path: &str) -> bool {
    let mut target = RENAME_DATA_FAIL_COMMIT_RENAME
        .lock()
        .expect("test failpoint lock should not be poisoned");
    if target.as_deref() == Some(dst_path) {
        target.take();
        true
    } else {
        false
    }
}

#[cfg(test)]
fn should_remove_staged_meta_before_commit(dst_path: &str) -> bool {
    let mut target = RENAME_DATA_REMOVE_STAGED_META_BEFORE_COMMIT
        .lock()
        .expect("test failpoint lock should not be poisoned");
    if target.as_deref() != Some(dst_path) {
        return false;
    }
    target.take();
    true
}

#[cfg(test)]
fn should_fail_local_inline_rollback_hardlink(dst_path: &Path) -> bool {
    let mut target = LOCAL_INLINE_ROLLBACK_HARDLINK_FAILURE
        .lock()
        .expect("test failpoint lock should not be poisoned");
    if target.as_deref() == Some(dst_path) {
        target.take();
        true
    } else {
        false
    }
}

#[cfg(test)]
async fn remove_dst_base_before_commit(
    dst_path: &str,
    guard: os::RenameCommitGuard,
    source_parent: &Path,
    destination_parent: &Path,
    destination_base: &Path,
    publication_root: &os::PublicationRoot,
    mutation_lease: Arc<os::NamespaceMutationLease>,
) -> Result<os::RenameCommitGuard> {
    let base = {
        let mut target = RENAME_DATA_REMOVE_DST_BASE_BEFORE_COMMIT
            .lock()
            .expect("test failpoint lock should not be poisoned");
        if target.as_ref().is_some_and(|(target_path, _)| target_path == dst_path) {
            target.take().map(|(_, base)| base)
        } else {
            None
        }
    };
    let Some(base) = base else {
        return Ok(guard);
    };

    #[cfg(windows)]
    drop(guard);
    std::fs::remove_dir_all(base).map_err(to_file_error)?;
    lock_rename_commit_directories(source_parent, destination_parent, destination_base, publication_root, mutation_lease).await
}

#[cfg(test)]
fn run_inline_preparation_before_backup(dst_path: &str) {
    let hook = INLINE_PREPARATION_BEFORE_BACKUP
        .lock()
        .expect("test preparation hook lock should not be poisoned")
        .remove(dst_path);
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn run_inline_before_file_sync_admission(dst_path: &str) {
    let hook = INLINE_BEFORE_FILE_SYNC_ADMISSION
        .lock()
        .expect("test admission hook lock should not be poisoned")
        .remove(dst_path);
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn run_rename_data_after_first_publication(root: &Path, dst_volume: &str, dst_path: &str) {
    let hook = RENAME_DATA_AFTER_FIRST_PUBLICATION
        .lock()
        .expect("test publication hook lock should not be poisoned")
        .remove(&(root.to_path_buf(), dst_volume.to_string(), dst_path.to_string()));
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn run_owned_file_write_before_open(path: &Path) {
    let hook = OWNED_FILE_WRITE_BEFORE_OPEN
        .lock()
        .expect("test file write hook lock should not be poisoned")
        .remove(path);
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(all(test, windows))]
fn run_destination_commit_directory_preparation(path: &Path) {
    let hook = DESTINATION_COMMIT_DIRECTORY_PREPARATION
        .lock()
        .expect("test destination preparation hook lock should not be poisoned")
        .remove(path);
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn should_fail_after_delete_data_staged(path: &str) -> bool {
    let mut targets = DELETE_VERSION_FAIL_AFTER_DATA_STAGED
        .lock()
        .expect("test failpoint lock should not be poisoned");
    if let Some(index) = targets.iter().position(|target| target == path) {
        targets.remove(index);
        true
    } else {
        false
    }
}

#[cfg(test)]
fn should_fail_after_delete_commit(root: &Path, path: &str) -> bool {
    let mut targets = DELETE_VERSION_FAIL_AFTER_COMMIT
        .lock()
        .expect("test failpoint lock should not be poisoned");
    if let Some(index) = targets
        .iter()
        .position(|(target_root, target_path)| target_root == root && target_path == path)
    {
        targets.remove(index);
        true
    } else {
        false
    }
}

#[cfg(not(test))]
fn should_fail_before_old_metadata_backup(_dst_path: &str) -> bool {
    false
}

#[cfg(not(test))]
fn should_fail_after_metadata_commit(_dst_path: &str) -> bool {
    false
}

#[cfg(not(test))]
fn should_fail_commit_rename(_dst_path: &str) -> bool {
    false
}

#[cfg(not(test))]
fn should_remove_staged_meta_before_commit(_dst_path: &str) -> bool {
    false
}

#[cfg(not(test))]
fn should_fail_local_inline_rollback_hardlink(_dst_path: &Path) -> bool {
    false
}

#[cfg(not(test))]
async fn remove_dst_base_before_commit(
    _dst_path: &str,
    guard: os::RenameCommitGuard,
    _source_parent: &Path,
    _destination_parent: &Path,
    _destination_base: &Path,
    _publication_root: &os::PublicationRoot,
    _mutation_lease: Arc<os::NamespaceMutationLease>,
) -> Result<os::RenameCommitGuard> {
    Ok(guard)
}

#[cfg(not(test))]
fn run_inline_preparation_before_backup(_dst_path: &str) {}

#[cfg(not(test))]
fn should_fail_after_delete_data_staged(_path: &str) -> bool {
    false
}

#[cfg(not(test))]
fn should_fail_after_delete_commit(_root: &Path, _path: &str) -> bool {
    false
}

fn log_startup_disk_io_error(stage: &str, path: &Path, err: &IoError) {
    warn!(
        event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
        stage,
        path = %path.display(),
        error_kind = ?err.kind(),
        raw_os_error = ?err.raw_os_error(),
        error = ?err,
        state = "io_failed",
        "Disk local startup filesystem operation failed"
    );
}

fn log_startup_disk_error(stage: &str, path: &Path, err: &DiskError) {
    if let DiskError::Io(io_err) = err {
        log_startup_disk_io_error(stage, path, io_err);
        return;
    }

    warn!(
        event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
        stage,
        path = %path.display(),
        error = ?err,
        state = "failed",
        "Disk local startup operation failed"
    );
}

#[derive(Debug, Clone)]
pub struct FormatInfo {
    pub id: Option<Uuid>,
    pub data: Bytes,
    pub file_info: Option<Metadata>,
    pub last_check: Option<OffsetDateTime>,
}

/// A helper enum to handle internal buffer types for writing data.
pub enum InternalBuf<'a> {
    Ref(&'a [u8]),
    Owned(Bytes),
}

/// Durability mode for `write_all_internal`.
///
/// `FileOnly` is reserved for tmp files the caller immediately renames away.
/// The safe-rename recipe (file content fdatasync -> rename -> fsync of the
/// destination parent directory) never needs the tmp directory entry to be
/// durable: the rename removes it, and a crash before the rename means the
/// operation was never acknowledged, so there is nothing to recover. Files
/// that stay where they are written (format.json via `write_all_public`, the
/// old-metadata rollback backup in `rename_data`, ...) must use `FileAndDir`
/// so both the contents and the new directory entry survive power loss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncMode {
    /// No fsync; durability is not required (or drive sync is disabled).
    None,
    /// fdatasync the file contents, then fsync its parent directory.
    FileAndDir,
    /// fdatasync only the file contents. Only valid when the caller renames
    /// the file away right after the write and fsyncs the rename
    /// destination's parent directory before acknowledging.
    FileOnly,
}

#[allow(
    dead_code,
    reason = "reclaim bookkeeping fields written by Drop but never read back (backlog#1823)"
)]
struct FileCacheReclaimWriter {
    inner: File,
    reclaim_len: usize,
    reclaim_on_shutdown: bool,
    reclaimed: bool,
}

#[allow(
    dead_code,
    reason = "reclaim bookkeeping fields written by Drop but never read back (backlog#1823)"
)]
struct FileCacheReclaimReader {
    inner: File,
    reclaim_offset: u64,
    reclaim_len: usize,
    reclaim_on_drop: bool,
    reclaimed: bool,
}

struct StallTimeoutReader<R> {
    inner: R,
    timeout: Duration,
    timer: Option<std::pin::Pin<Box<Sleep>>>,
}

impl<R> StallTimeoutReader<R> {
    fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            timer: None,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for StallTimeoutReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();
        match std::pin::Pin::new(&mut self.inner).poll_read(cx, buf) {
            std::task::Poll::Ready(result) => {
                self.timer = None;
                std::task::Poll::Ready(result)
            }
            std::task::Poll::Pending => {
                if self.timeout.is_zero() {
                    return std::task::Poll::Pending;
                }

                if self.timer.is_none() {
                    self.timer = Some(Box::pin(tokio::time::sleep(self.timeout)));
                }

                if let Some(timer) = self.timer.as_mut()
                    && std::future::Future::poll(timer.as_mut(), cx).is_ready()
                {
                    self.timer = None;
                    return std::task::Poll::Ready(Err(std::io::Error::new(
                        ErrorKind::TimedOut,
                        "local disk read stall timeout",
                    )));
                }

                if buf.filled().len() > filled_before {
                    self.timer = None;
                }

                std::task::Poll::Pending
            }
        }
    }
}

#[allow(
    dead_code,
    reason = "reclaim metrics emitter reached only from the Linux-gated reclaim paths (backlog#1823)"
)]
fn record_file_cache_reclaim_success(kind: &'static str, reclaim_len: usize, started: std::time::Instant) {
    // Runs per read-stream page-cache reclaim window; skip the whole emission
    // (three metric-key constructions) when general metrics are disabled.
    if !rustfs_io_metrics::metrics_enabled() {
        return;
    }
    // `kind`, "ok" and "err" are all `&'static str`; the `metrics` macros take
    // static label values directly, so pass them as-is instead of allocating a
    // `String` per reclaim.
    counter!("rustfs_page_cache_reclaim_requests_total", "kind" => kind, "result" => "ok").increment(1);
    counter!("rustfs_page_cache_reclaim_bytes_total", "kind" => kind).increment(reclaim_len as u64);
    metrics::histogram!("rustfs_page_cache_reclaim_duration_seconds", "kind" => kind).record(started.elapsed().as_secs_f64());
}

fn record_file_cache_reclaim_error(kind: &'static str) {
    if !rustfs_io_metrics::metrics_enabled() {
        return;
    }
    counter!("rustfs_page_cache_reclaim_requests_total", "kind" => kind, "result" => "err").increment(1);
}

cached_read_env! {
    fn bitrot_size_mismatch_retry_count() -> usize =
        rustfs_utils::get_env_u64(ENV_BITROT_SIZE_MISMATCH_RETRY_COUNT, DEFAULT_BITROT_SIZE_MISMATCH_RETRY_COUNT) as usize;
}

cached_read_env! {
    fn bitrot_size_mismatch_retry_delay() -> Duration = Duration::from_millis(rustfs_utils::get_env_u64(
        ENV_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS,
        DEFAULT_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS,
    ));
}

fn is_bitrot_size_mismatch_error(err: &std::io::Error) -> bool {
    err.to_string().contains("bitrot shard file size mismatch")
}

fn is_bitrot_verification_error(err: &std::io::Error) -> bool {
    is_bitrot_size_mismatch_error(err) || err.to_string().contains("bitrot hash mismatch")
}

fn metacache_write_error(err: rustfs_filemeta::Error) -> DiskError {
    let err = DiskError::from(err);
    if err.contains_io_error_kind(ErrorKind::BrokenPipe) {
        DiskError::metacache_output_stream_closed()
    } else {
        err
    }
}

async fn write_metacache_obj<W>(out: &mut MetacacheWriter<W>, obj: &MetaCacheEntry) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    out.write_obj(obj).await.map_err(metacache_write_error)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Bound one drive read issued by a walk with the stall timeout.
///
/// A walk is not failed for taking a long time — a large directory tree is not a
/// fault. It is failed only when the drive stops answering, so the timeout wraps
/// each individual read rather than the walk as a whole. Time spent blocked
/// writing to a slow consumer is deliberately outside this budget.
///
/// Every filesystem call a walk makes must go through this, because the listing
/// path skips the wrapper-level total timeout: an unbounded read would leave the
/// walk with no liveness bound of its own.
async fn with_walk_stall_deadline<T, F>(stall: Option<Duration>, fut: F) -> Result<T>
where
    F: std::future::Future<Output = T>,
{
    match stall {
        Some(stall) if !stall.is_zero() => timeout(stall, fut).await.map_err(|_| DiskError::Timeout),
        _ => Ok(fut.await),
    }
}

/// [`with_walk_stall_deadline`] for reads that already yield a [`Result`].
async fn with_walk_stall_timeout<T, F>(stall: Option<Duration>, fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    with_walk_stall_deadline(stall, fut).await?
}

async fn read_dir_entries_with_walk_stall(path: &Path, count: i32, stall: Option<Duration>) -> Result<Vec<String>> {
    let mut entries = with_walk_stall_deadline(stall, fs::read_dir(path))
        .await?
        .map_err(to_file_error)?;
    let mut names = Vec::new();
    let mut remaining = count;

    loop {
        let Some(entry) = with_walk_stall_deadline(stall, entries.next_entry())
            .await?
            .map_err(to_file_error)?
        else {
            break;
        };
        let name = entry.file_name().to_string_lossy().to_string();

        if name.is_empty() || name == "." || name == ".." {
            continue;
        }

        let file_type = with_walk_stall_deadline(stall, entry.file_type())
            .await?
            .map_err(to_file_error)?;
        if file_type.is_file() {
            names.push(name);
        } else if file_type.is_dir() {
            names.push(format!("{name}{SLASH_SEPARATOR}"));
        } else {
            continue;
        }

        remaining -= 1;
        if remaining == 0 {
            break;
        }
    }

    Ok(names)
}

impl FileCacheReclaimReader {
    fn new(inner: File, reclaim_offset: u64, reclaim_len: usize, reclaim_on_drop: bool) -> Self {
        #[cfg(target_os = "macos")]
        if reclaim_on_drop {
            let _ = set_fd_nocache(&inner);
        }

        Self {
            inner,
            reclaim_offset,
            reclaim_len,
            reclaim_on_drop,
            reclaimed: false,
        }
    }

    #[cfg(target_os = "linux")]
    fn reclaim_file_cache(&mut self) -> std::io::Result<()> {
        use core::num::NonZeroU64;
        use rustix::fs::{Advice, fadvise};

        if !self.reclaim_on_drop || self.reclaimed || self.reclaim_len == 0 {
            return Ok(());
        }

        let started = std::time::Instant::now();
        let reclaim_len =
            NonZeroU64::new(self.reclaim_len as u64).expect("reclaim_len is guaranteed non-zero by the early return");
        fadvise(&self.inner, self.reclaim_offset, Some(reclaim_len), Advice::DontNeed).map_err(std::io::Error::from)?;

        self.reclaimed = true;
        record_file_cache_reclaim_success("read", self.reclaim_len, started);
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn reclaim_file_cache(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn set_fd_nocache(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `fcntl` is called on a valid file descriptor owned by `file`.
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if ret == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn set_std_fd_nocache(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `fcntl` is called on a valid file descriptor owned by `file`.
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if ret == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

impl Drop for FileCacheReclaimReader {
    fn drop(&mut self) {
        if let Err(err) = self.reclaim_file_cache() {
            record_file_cache_reclaim_error("read");
            debug!(error = ?err, reclaim_offset = self.reclaim_offset, reclaim_len = self.reclaim_len, "failed to reclaim file cache after read");
        }
    }
}

impl AsyncRead for FileCacheReclaimReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl FileCacheReclaimWriter {
    fn new(inner: File, reclaim_len: usize, reclaim_on_shutdown: bool) -> Self {
        #[cfg(target_os = "macos")]
        if reclaim_on_shutdown {
            let _ = set_fd_nocache(&inner);
        }

        Self {
            inner,
            reclaim_len,
            reclaim_on_shutdown,
            reclaimed: false,
        }
    }

    #[cfg(target_os = "linux")]
    fn reclaim_file_cache(&mut self) -> std::io::Result<()> {
        use core::num::NonZeroU64;
        use rustix::fs::{Advice, fadvise};

        if !self.reclaim_on_shutdown || self.reclaimed || self.reclaim_len == 0 {
            return Ok(());
        }

        let started = std::time::Instant::now();
        let reclaim_len =
            NonZeroU64::new(self.reclaim_len as u64).expect("reclaim_len is guaranteed non-zero by the early return");
        fadvise(&self.inner, 0, Some(reclaim_len), Advice::DontNeed).map_err(std::io::Error::from)?;

        self.reclaimed = true;
        record_file_cache_reclaim_success("write", self.reclaim_len, started);
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn reclaim_file_cache(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl AsyncWrite for FileCacheReclaimWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match std::pin::Pin::new(&mut self.inner).poll_shutdown(cx) {
            std::task::Poll::Ready(Ok(())) => {
                if let Err(err) = self.reclaim_file_cache() {
                    record_file_cache_reclaim_error("write");
                    debug!(error = ?err, reclaim_len = self.reclaim_len, "failed to reclaim file cache after write");
                }
                std::task::Poll::Ready(Ok(()))
            }
            other => other,
        }
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

cached_read_env! {
    fn file_cache_reclaim_write_enabled() -> bool = rustfs_utils::get_env_bool(
        rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE,
        rustfs_config::DEFAULT_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE,
    );
}

fn should_reclaim_file_cache_after_write(file_size: i64) -> bool {
    if file_size <= 0 {
        return false;
    }

    if !file_cache_reclaim_write_enabled() {
        return false;
    }

    // Same threshold as the read-side reclaim (backlog#1182): reuse its snapshot.
    file_size as usize >= file_cache_reclaim_threshold()
}

cached_read_env! {
    fn file_cache_reclaim_read_enabled() -> bool = rustfs_utils::get_env_bool(
        rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE,
        rustfs_config::DEFAULT_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE,
    );
}

cached_read_env! {
    fn file_cache_reclaim_threshold() -> usize = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD,
    );
}

fn should_reclaim_file_cache_after_read(length: usize) -> bool {
    if length == 0 {
        return false;
    }

    if !file_cache_reclaim_read_enabled() {
        return false;
    }

    length >= file_cache_reclaim_threshold()
}

/// Write-open semantics for [`LocalIoBackend::open_write`].
///
/// `Truncate` mirrors `DiskAPI::create_file` (O_CREATE|O_WRONLY|O_TRUNC, no
/// volume access check, cache-reclaim writer); `Append` mirrors
/// `DiskAPI::append_file` (O_CREATE|O_APPEND|O_WRONLY, volume access check).
/// The access-check asymmetry is preserved historical behavior.
#[derive(Clone, Copy, Debug)]
pub(crate) enum WriteMode {
    Truncate { size_hint: i64 },
    Append,
}

/// Local-disk file I/O backend behind [`LocalDisk`].
///
/// Models the real per-file operations of the `DiskAPI` hot path so an
/// alternative backend (e.g. a runtime-probed io_uring implementation) can be
/// swapped in without touching callers. The default [`StdBackend`] preserves
/// the pre-trait behavior byte-for-byte. Commit-point durability
/// (fdatasync -> rename -> fsync-dir in `rename_data`) is deliberately NOT
/// part of this trait.
#[async_trait::async_trait]
pub(crate) trait LocalIoBackend: Send + Sync + Debug + 'static {
    /// Positioned whole-range read returning owned bytes
    /// (mirrors `read_file_mmap_copy_with_metrics`).
    async fn pread_bytes(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
        metrics: Option<MmapCopyStageMetrics>,
    ) -> Result<Bytes>;

    /// Open a bounded streaming reader over `offset..offset+length`
    /// (mirrors `read_file_stream`).
    async fn open_read_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader>;

    /// Open a whole-file streaming reader (mirrors `read_file`).
    async fn open_full_read(&self, volume: &str, path: &str) -> Result<FileReader>;

    /// Open a writer (mirrors `create_file`/`append_file` per [`WriteMode`]).
    async fn open_write(&self, volume: &str, path: &str, mode: WriteMode) -> Result<FileWriter>;

    /// Drop any descriptor this backend caches for exactly `volume`'s `path`
    /// (backlog#1145). Preferred wherever the caller knows the affected paths.
    ///
    /// MUST be called by every path that replaces a file a reader may have opened
    /// — `rename_data` (heal reuses a version's `data_dir` and lands a rebuilt
    /// shard on the same part path) and `rename_file`.
    async fn invalidate_cached_fd(&self, _volume: &str, _path: &str) {}

    /// Drop every descriptor for `volume`'s `path` and anything beneath it.
    /// For callers that cannot enumerate the affected paths (`delete`).
    ///
    /// Backends that hold no descriptors ignore both methods.
    fn invalidate_cached_fds_under(&self, _volume: &str, _path: &str) {}

    /// Drop every descriptor for `volume` — used when the whole bucket tree is
    /// removed (`delete_volume`), where the exact object paths are unknown
    /// (rustfs/backlog#1177).
    fn invalidate_cached_fds_for_volume(&self, _volume: &str) {}

    /// Drop ALL cached descriptors. Called when this disk instance is retired
    /// (`close`) so a replacement instance's invalidations are never defeated by
    /// this one continuing to serve stale fds (rustfs/backlog#1177).
    async fn clear_cached_fds(&self) {}
}

/// Default [`LocalIoBackend`]: tokio blocking-pool file I/O plus the
/// mmap-copy / direct-read-copy positioned read, moved verbatim from the
/// former `DiskAPI` method bodies on `LocalDisk`.
pub(crate) struct StdBackend {
    root: PathBuf,
    #[cfg(target_os = "linux")]
    direct_io: Arc<DirectIoReadState>,
    #[cfg(target_os = "linux")]
    direct_io_write: Arc<DirectIoWriteState>,
    /// Per-disk descriptor cache for buffered reads (rustfs/backlog#1801).
    /// `None` when disabled by env, blocked by a low `RLIMIT_NOFILE`, or on
    /// non-Linux (where the cache type is unavailable). Like the io_uring
    /// cache, only the buffered read path populates it; O_DIRECT reads keep
    /// opening their own aligned descriptors.
    #[cfg(target_os = "linux")]
    fd_cache: Option<FdCache>,
}

// Manual `Debug` mirrors `UringBackend`: the fd cache (and the Linux-only
// direct-IO state) hold types that do not implement `Debug`, so a derive would
// force `FdCache: Debug`. `finish_non_exhaustive` skips them.
impl std::fmt::Debug for StdBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdBackend").field("root", &self.root).finish_non_exhaustive()
    }
}

impl StdBackend {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self::build(root, true)
    }

    /// Construct without the descriptor cache.
    ///
    /// `UringBackend` wraps a `StdBackend` and runs its own `FdCache` over the
    /// same positioned reads. If the inner `StdBackend` also built a cache, a
    /// fallback read (`UringBackend::pread_bytes` delegates to the inner backend
    /// on latch-off / O_DIRECT / buffered errors) would populate a *second*
    /// cache that `UringBackend`'s invalidation never touches — re-opening the
    /// stale-inode hazard `FdCache` exists to close (rustfs/backlog#1176/#1801).
    /// The wrapper therefore owns the only cache for the disk; the inner backend
    /// opens per read. This also avoids double-counting `FD_CACHE_CAPACITY`
    /// against `RLIMIT_NOFILE` (rustfs/backlog#1178).
    #[cfg(target_os = "linux")]
    pub(crate) fn new_without_fd_cache(root: PathBuf) -> Self {
        Self::build(root, false)
    }

    fn build(root: PathBuf, build_fd_cache: bool) -> Self {
        // Gate the fd cache on RLIMIT_NOFILE headroom (rustfs/backlog#1178):
        // 512 fds/disk with a low soft limit and several disks would hit EMFILE.
        // Fall back to open-per-read when the limit is too small.
        #[cfg(target_os = "linux")]
        let fd_cache = if build_fd_cache && is_local_fd_cache_enabled() {
            if rlimit_allows_fd_cache() {
                Some(FdCache::new())
            } else {
                warn!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    "std fd cache disabled: RLIMIT_NOFILE soft limit too low for 512 fds/disk; using open-per-read"
                );
                None
            }
        } else {
            None
        };
        // `build_fd_cache` is only consulted on Linux (for the fd cache); on
        // other platforms it has no effect and would trip the unused-variable lint.
        #[cfg(not(target_os = "linux"))]
        let _ = build_fd_cache;
        Self {
            root,
            #[cfg(target_os = "linux")]
            direct_io: Arc::new(DirectIoReadState::new()),
            #[cfg(target_os = "linux")]
            direct_io_write: Arc::new(DirectIoWriteState::new()),
            #[cfg(target_os = "linux")]
            fd_cache,
        }
    }

    fn io_root(&self) -> &Path {
        &self.root
    }

    async fn open_file(&self, path: impl AsRef<Path>, mode: usize, skip_parent: impl AsRef<Path>) -> Result<File> {
        let mut skip_parent = skip_parent.as_ref();
        if skip_parent.as_os_str().is_empty() {
            skip_parent = self.io_root();
        }

        if let Some(parent) = path.as_ref().parent()
            && parent != skip_parent
        {
            os::make_dir_all(parent, skip_parent).await?;
        }

        let f = super::fs::open_file(path.as_ref(), mode).await.map_err(to_file_error)?;

        Ok(f)
    }

    async fn open_file_read_only(&self, path: impl AsRef<Path>) -> Result<File> {
        let f = super::fs::open_file(path.as_ref(), O_RDONLY).await.map_err(to_file_error)?;
        Ok(f)
    }
}

#[async_trait::async_trait]
impl LocalIoBackend for StdBackend {
    /// File read using mmap-then-copy on Unix or efficient read on non-Unix.
    // SAFETY: Unix unsafe calls in this function only query page size and mmap
    // a read-only file region after bounds and alignment are validated.
    #[allow(unsafe_code)]
    async fn pread_bytes(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
        metrics: Option<MmapCopyStageMetrics>,
    ) -> Result<Bytes> {
        let metrics = metrics.filter(|_| rustfs_io_metrics::get_stage_metrics_enabled());
        let metrics_enabled = metrics.is_some();

        let metadata_validate_start = metrics_enabled.then(std::time::Instant::now);
        let Some(end_offset) = offset.checked_add(length) else {
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.metadata_validate_stage, metadata_validate_start);
            }
            return Err(DiskError::FileCorrupt);
        };

        // Unix: use mmap to read the data (copies into Bytes for safe ownership)
        // Non-Unix: fall back to efficient read
        #[cfg(unix)]
        {
            use memmap2::MmapOptions;
            use std::time::{Duration as StdDuration, Instant as StdInstant};

            #[allow(dead_code, reason = "mmap copy result slot kept beside the mapping it owns (backlog#1823)")]
            struct MmapCopyReadResult {
                bytes: Bytes,
                access_check_duration: StdDuration,
                path_resolve_duration: StdDuration,
                metadata_lookup_duration: StdDuration,
                metadata_validate_duration: StdDuration,
                file_open_duration: StdDuration,
                mmap_map_duration: StdDuration,
                mmap_copy_duration: StdDuration,
                direct_read_copy_duration: StdDuration,
                mmap_map_fault_delta: MmapPageFaultDelta,
                mmap_copy_fault_delta: MmapPageFaultDelta,
                direct_read_copy_fault_delta: MmapPageFaultDelta,
                blocking_task_duration: StdDuration,
                used_direct_io: bool,
                /// The descriptor opened by THIS call (None on a cache hit), handed
                /// back so the async caller can index it in the fd cache.
                opened_fd: Option<Arc<std::fs::File>>,
            }

            enum MmapCopyReadError {
                Disk(DiskError),
                OutOfBounds { actual_size: u64 },
            }

            impl From<DiskError> for MmapCopyReadError {
                fn from(err: DiskError) -> Self {
                    Self::Disk(err)
                }
            }

            let start = StdInstant::now();
            let root = self.root.clone();
            let volume_owned = volume.to_owned();
            let path_owned = path.to_owned();

            let should_reclaim_after_read = should_reclaim_file_cache_after_read(length);
            let should_populate_mmap_read = should_populate_mmap_read(length);
            let read_copy_method = local_read_copy_method();
            #[cfg(target_os = "linux")]
            let direct_io_eligible = is_direct_io_read_enabled() && length > 0 && length >= get_direct_io_read_threshold();
            #[cfg(target_os = "linux")]
            let direct_io_state = self.direct_io.clone();
            let offset_u64 = u64::try_from(offset).map_err(|_| DiskError::FileCorrupt)?;
            let end_offset_u64 = u64::try_from(end_offset).map_err(|_| DiskError::FileCorrupt)?;

            // Descriptor cache (rustfs/backlog#1801): on a hit the read reuses an
            // already-open descriptor (via dup below) and skips `access` +
            // `File::open`. Linux-only — on other Unix `cached_fd` is None and the
            // read opens per call exactly as before. `fd_lookup` snapshots the
            // invalidation generation BEFORE the open so a heal/delete that lands
            // while the blocking open is in flight prevents the now-stale descriptor
            // from being inserted (rustfs/backlog#1176).
            #[cfg(target_os = "linux")]
            let fd_lookup = self.fd_cache.as_ref().map(|cache| {
                let key = FdKey {
                    volume: volume.to_owned(),
                    path: path.to_owned(),
                    direct: false,
                };
                let gen_at_open = cache.generation();
                (cache, key, gen_at_open)
            });
            #[cfg(target_os = "linux")]
            let cached_fd: Option<Arc<std::fs::File>> = match &fd_lookup {
                Some((cache, key, _)) => cache.get(key).await,
                None => None,
            };
            #[cfg(not(target_os = "linux"))]
            let cached_fd: Option<Arc<std::fs::File>> = None;

            let blocking_wait_start = metrics_enabled.then(std::time::Instant::now);
            let read_result = tokio::task::spawn_blocking(move || {
                let blocking_task_start = metrics_enabled.then(StdInstant::now);

                // Resolve the part path unconditionally: the O_DIRECT branch (large
                // reads) opens its own aligned descriptor by path even on a cache hit.
                let path_resolve_start = metrics_enabled.then(StdInstant::now);
                let file_path = local_disk_object_path(&root, &volume_owned, &path_owned)?;
                check_path_length(file_path.to_string_lossy().as_ref())?;
                let path_resolve_duration = path_resolve_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                let file_open_start = metrics_enabled.then(StdInstant::now);
                // Acquire the read handle (rustfs/backlog#1801). On a descriptor-cache
                // hit this reuses the cached descriptor via `dup` (one syscall, no path
                // resolution or permission re-check) and skips the volume access probe;
                // on a miss it resolves the volume, access-checks, and opens the file.
                // `File::try_clone` shares the cached descriptor's open-file offset, so
                // the read below is positioned (mmap offset argument / `read_exact_at`)
                // and never depends on the descriptor's current offset. `cached_fd` being
                // None also marks this call as a miss for the cache-insert side-channel.
                let (file, access_check_duration) = if let Some(cached) = cached_fd.as_ref() {
                    (cached.as_ref().try_clone().map_err(DiskError::from)?, StdDuration::ZERO)
                } else {
                    // Measure the volume access probe only — the part-path resolution
                    // above is accounted in `path_resolve_duration` (rustfs/backlog#1801).
                    let access_check_start = metrics_enabled.then(StdInstant::now);
                    let volume_dir = local_disk_bucket_path(&root, &volume_owned)?;
                    if !skip_access_checks(&volume_owned) {
                        crate::disk::fs::access_std(&volume_dir)
                            .map_err(|e| DiskError::from(to_access_error(e, DiskError::VolumeAccessDenied)))?;
                    }
                    let access_check_duration = access_check_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());
                    (std::fs::File::open(&file_path).map_err(DiskError::from)?, access_check_duration)
                };
                let file_open_duration = file_open_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                let metadata_lookup_start = metrics_enabled.then(StdInstant::now);
                // On a cache hit this fstats the cached descriptor — the inode it was
                // opened against, which invalidation keeps current for live entries. EC
                // shards are fixed-length, so a still-cached pre-heal length is benign.
                let meta = file.metadata().map_err(DiskError::from)?;
                let metadata_lookup_duration = metadata_lookup_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                let metadata_validate_start = metrics_enabled.then(StdInstant::now);
                if meta.len() < end_offset_u64 {
                    return Err(MmapCopyReadError::OutOfBounds { actual_size: meta.len() });
                }
                let metadata_validate_duration =
                    metadata_validate_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                #[cfg(target_os = "macos")]
                if should_reclaim_after_read {
                    let _ = set_std_fd_nocache(&file);
                }

                let mut mmap_map_duration = StdDuration::ZERO;
                let mut mmap_copy_duration = StdDuration::ZERO;
                let mut direct_read_copy_duration = StdDuration::ZERO;
                let mut mmap_map_fault_delta = MmapPageFaultDelta::default();
                let mut mmap_copy_fault_delta = MmapPageFaultDelta::default();
                let mut direct_read_copy_fault_delta = MmapPageFaultDelta::default();
                let mut _reclaim_offset = offset_u64;
                let mut _reclaim_len = length;

                #[cfg(target_os = "linux")]
                let mut direct_io_bytes: Option<Bytes> = None;
                #[cfg(not(target_os = "linux"))]
                let direct_io_bytes: Option<Bytes> = None;
                #[cfg(target_os = "linux")]
                if direct_io_eligible && direct_io_state.supported.load(Ordering::Relaxed) {
                    let direct_start = metrics_enabled.then(StdInstant::now);
                    let direct_faults_before = read_mmap_page_fault_counts(metrics_enabled);
                    match pread_direct_aligned(&file_path, offset_u64, length, &direct_io_state) {
                        Ok(bytes) => {
                            let direct_faults_after = read_mmap_page_fault_counts(metrics_enabled);
                            direct_read_copy_duration = direct_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());
                            direct_read_copy_fault_delta = mmap_page_fault_delta(direct_faults_before, direct_faults_after);
                            direct_io_bytes = Some(bytes);
                        }
                        Err(err) => {
                            // Never surface O_DIRECT errors: EINVAL maps to
                            // FileNotFound in to_file_error and would trigger a
                            // spurious EC rebuild. Latch off on unsupported
                            // filesystems; otherwise retry buffered this once.
                            if is_direct_io_unsupported(&err) {
                                direct_io_state.supported.store(false, Ordering::Relaxed);
                            }
                            if !direct_io_state.fallback_logged.swap(true, Ordering::Relaxed) {
                                warn!(
                                    event = EVENT_DISK_LOCAL_DIRECT_IO_FALLBACK,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                                    path = %file_path.display(),
                                    error = ?err,
                                    "O_DIRECT read unavailable; falling back to buffered reads"
                                );
                            }
                        }
                    }
                }

                let used_direct_io = direct_io_bytes.is_some();
                let bytes = if let Some(bytes) = direct_io_bytes {
                    bytes
                } else {
                    match read_copy_method {
                        LocalReadCopyMethod::MmapCopy => {
                            // mmap offsets on Unix must be page-size aligned. Align the
                            // mapping down to the nearest page boundary, then slice out the
                            // originally requested logical range.
                            let page_size = mmap_page_size()?;
                            let aligned_offset = offset_u64 - (offset_u64 % page_size);
                            let logical_offset = usize::try_from(offset_u64 - aligned_offset)
                                .map_err(|_| DiskError::other("mmap offset overflow"))?;
                            let map_len = logical_offset
                                .checked_add(length)
                                .ok_or_else(|| DiskError::other("mmap length overflow"))?;
                            _reclaim_offset = aligned_offset;
                            _reclaim_len = map_len;

                            // SAFETY: The file is opened as read-only, and we're mapping a region
                            // that we've already verified exists and is within file bounds. The
                            // file offset passed to mmap is page-size aligned as required on Unix.
                            let mmap_map_start = metrics_enabled.then(StdInstant::now);
                            let mmap_map_faults_before = read_mmap_page_fault_counts(metrics_enabled);
                            let mut mmap_options = MmapOptions::new();
                            mmap_options.offset(aligned_offset).len(map_len);
                            if should_populate_mmap_read {
                                mmap_options.populate();
                            }
                            let mmap = unsafe { mmap_options.map(&file) }.map_err(DiskError::other)?;
                            let mmap_map_faults_after = read_mmap_page_fault_counts(metrics_enabled);
                            mmap_map_duration = mmap_map_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());
                            mmap_map_fault_delta = mmap_page_fault_delta(mmap_map_faults_before, mmap_map_faults_after);

                            // Copy only the requested logical range into a Bytes buffer. This
                            // avoids undefined behavior from treating OS-managed mmap memory as
                            // allocator-managed Vec storage, at the cost of an extra copy.
                            let end = logical_offset
                                .checked_add(length)
                                .ok_or_else(|| DiskError::other("mmap slice length overflow"))?;
                            let mmap_copy_start = metrics_enabled.then(StdInstant::now);
                            let mmap_copy_faults_before = read_mmap_page_fault_counts(metrics_enabled);
                            let bytes = Bytes::copy_from_slice(&mmap[logical_offset..end]);
                            let mmap_copy_faults_after = read_mmap_page_fault_counts(metrics_enabled);
                            mmap_copy_duration = mmap_copy_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());
                            mmap_copy_fault_delta = mmap_page_fault_delta(mmap_copy_faults_before, mmap_copy_faults_after);
                            bytes
                        }
                        LocalReadCopyMethod::DirectReadCopy => {
                            use std::os::unix::fs::FileExt;

                            let direct_read_copy_start = metrics_enabled.then(StdInstant::now);
                            let direct_read_copy_faults_before = read_mmap_page_fault_counts(metrics_enabled);
                            let mut buffer = vec![0; length];
                            // Positioned read: a cache hit reads through a `dup`'d handle
                            // that shares the cached descriptor's offset, so this must not
                            // touch the descriptor offset (rustfs/backlog#1801).
                            file.read_exact_at(&mut buffer, offset_u64).map_err(DiskError::from)?;
                            let direct_read_copy_faults_after = read_mmap_page_fault_counts(metrics_enabled);
                            direct_read_copy_duration =
                                direct_read_copy_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());
                            direct_read_copy_fault_delta =
                                mmap_page_fault_delta(direct_read_copy_faults_before, direct_read_copy_faults_after);
                            Bytes::from(buffer)
                        }
                    }
                };

                #[cfg(target_os = "linux")]
                if should_reclaim_after_read && _reclaim_len > 0 {
                    use core::num::NonZeroU64;
                    use rustix::fs::{Advice, fadvise};

                    let reclaim_len = NonZeroU64::new(
                        u64::try_from(_reclaim_len).map_err(|_| DiskError::other("read reclaim length overflow"))?,
                    )
                    .ok_or_else(|| DiskError::other("read reclaim length overflow"))?;
                    fadvise(&file, _reclaim_offset, Some(reclaim_len), Advice::DontNeed)
                        .map_err(std::io::Error::from)
                        .map_err(DiskError::from)?;
                }

                let blocking_task_duration = blocking_task_start.map_or(StdDuration::ZERO, |started_at| started_at.elapsed());

                // Hand the freshly opened descriptor back so the async caller can index
                // the cache — None on a hit (the cache already holds it). mmap/reclaim
                // above only borrowed `file`, so it is still owned here and moves into the
                // Arc; `cached_fd.is_none()` is true exactly when this call did the open.
                // Non-Linux has no fd cache, so skip the Arc allocation there.
                #[cfg(target_os = "linux")]
                let opened_fd: Option<Arc<std::fs::File>> = cached_fd.is_none().then(|| Arc::new(file));
                #[cfg(not(target_os = "linux"))]
                let opened_fd: Option<Arc<std::fs::File>> = None;

                Ok::<MmapCopyReadResult, MmapCopyReadError>(MmapCopyReadResult {
                    bytes,
                    access_check_duration,
                    path_resolve_duration,
                    metadata_lookup_duration,
                    metadata_validate_duration,
                    file_open_duration,
                    mmap_map_duration,
                    mmap_copy_duration,
                    direct_read_copy_duration,
                    mmap_map_fault_delta,
                    mmap_copy_fault_delta,
                    direct_read_copy_fault_delta,
                    blocking_task_duration,
                    used_direct_io,
                    opened_fd,
                })
            })
            .await
            .map_err(DiskError::from)
            .map_err(MmapCopyReadError::Disk)
            .and_then(|result| result);
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.blocking_wait_stage, blocking_wait_start);
            }
            let read_result = match read_result {
                Ok(read_result) => read_result,
                Err(MmapCopyReadError::Disk(err)) => return Err(err),
                Err(MmapCopyReadError::OutOfBounds { actual_size }) => {
                    error!(
                        event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        volume,
                        path,
                        offset,
                        length,
                        actual_size,
                        reason = "read_file_mmap_copy_out_of_bounds",
                        "Disk local read fallback failed"
                    );
                    return Err(DiskError::FileCorrupt);
                }
            };
            if metrics_enabled && let Some(metrics) = metrics {
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.blocking_task_stage,
                    read_result.blocking_task_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.access_check_stage,
                    read_result.access_check_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.path_resolve_stage,
                    read_result.path_resolve_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.metadata_lookup_stage,
                    read_result.metadata_lookup_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.metadata_validate_stage,
                    read_result.metadata_validate_duration.as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    metrics.path,
                    metrics.file_open_stage,
                    read_result.file_open_duration.as_secs_f64(),
                );
                if read_result.used_direct_io {
                    rustfs_io_metrics::record_get_object_stage_duration(
                        metrics.path,
                        metrics.direct_read_copy_stage,
                        read_result.direct_read_copy_duration.as_secs_f64(),
                    );
                    record_direct_read_page_fault_delta(
                        metrics.path,
                        metrics.direct_read_copy_stage,
                        read_result.direct_read_copy_fault_delta,
                    );
                } else {
                    match read_copy_method {
                        LocalReadCopyMethod::MmapCopy => {
                            rustfs_io_metrics::record_get_object_stage_duration(
                                metrics.path,
                                metrics.mmap_map_stage,
                                read_result.mmap_map_duration.as_secs_f64(),
                            );
                            rustfs_io_metrics::record_get_object_stage_duration(
                                metrics.path,
                                metrics.mmap_copy_stage,
                                read_result.mmap_copy_duration.as_secs_f64(),
                            );
                            record_mmap_page_fault_delta(metrics.path, metrics.mmap_map_stage, read_result.mmap_map_fault_delta);
                            record_mmap_page_fault_delta(
                                metrics.path,
                                metrics.mmap_copy_stage,
                                read_result.mmap_copy_fault_delta,
                            );
                        }
                        LocalReadCopyMethod::DirectReadCopy => {
                            rustfs_io_metrics::record_get_object_stage_duration(
                                metrics.path,
                                metrics.direct_read_copy_stage,
                                read_result.direct_read_copy_duration.as_secs_f64(),
                            );
                            record_direct_read_page_fault_delta(
                                metrics.path,
                                metrics.direct_read_copy_stage,
                                read_result.direct_read_copy_fault_delta,
                            );
                        }
                    }
                }
            }
            // Index the freshly opened descriptor for future cache hits
            // (rustfs/backlog#1801). `insert_if_fresh` refuses to cache if an
            // invalidation (heal/delete/rename) bumped the generation between the
            // open snapshot and now, so a stale pre-mutation inode is never served
            // (rustfs/backlog#1176). On a cache hit `opened_fd` is None; on non-Linux
            // there is no fd cache, so this is gated out entirely.
            #[cfg(target_os = "linux")]
            if let (Some((cache, key, gen_at_open)), Some(opened)) = (fd_lookup, read_result.opened_fd) {
                cache.insert_if_fresh(key, opened, gen_at_open).await;
            }
            let bytes = read_result.bytes;

            // Log successful mmap read metrics
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

            // Record mmap read metrics
            rustfs_io_metrics::record_zero_copy_read(length, duration_ms);

            debug!(
                size = length,
                duration_ms = duration_ms,
                mmap_populate = should_populate_mmap_read,
                read_copy_method = ?read_copy_method,
                "mmap_read_success"
            );

            return Ok(bytes);
        }

        // Non-Unix fallback: efficient read into Bytes
        #[cfg(not(unix))]
        {
            // Record zero-copy fallback
            rustfs_io_metrics::record_zero_copy_fallback("non_unix_platform");

            debug!(reason = "non_unix_platform", "zero_copy_fallback");

            let access_check_start = metrics_enabled.then(std::time::Instant::now);
            let volume_dir = local_disk_bucket_path(self.io_root(), volume)?;
            if !skip_access_checks(volume) {
                access(&volume_dir)
                    .await
                    .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
            }
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.access_check_stage, access_check_start);
            }

            let path_resolve_start = metrics_enabled.then(std::time::Instant::now);
            let file_path = local_disk_object_path(self.io_root(), volume, path)?;
            check_path_length(file_path.to_string_lossy().as_ref())?;
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.path_resolve_stage, path_resolve_start);
            }

            let file_path_clone = file_path.clone();
            let metadata_lookup_start = metrics_enabled.then(std::time::Instant::now);
            let meta_result = tokio::task::spawn_blocking(move || std::fs::metadata(&file_path_clone).map_err(DiskError::from))
                .await
                .map_err(DiskError::from)
                .and_then(|result| result);
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.metadata_lookup_stage, metadata_lookup_start);
            }
            let meta = meta_result?;

            let metadata_validate_start = metrics_enabled.then(std::time::Instant::now);
            if meta.len() < end_offset as u64 {
                if let Some(metrics) = metrics {
                    record_mmap_copy_stage(metrics, metrics.metadata_validate_stage, metadata_validate_start);
                }
                error!(
                    event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    volume,
                    path,
                    offset,
                    length,
                    actual_size = meta.len(),
                    reason = "read_file_mmap_copy_out_of_bounds",
                    "Disk local read fallback failed"
                );
                return Err(DiskError::FileCorrupt);
            }
            if let Some(metrics) = metrics {
                record_mmap_copy_stage(metrics, metrics.metadata_validate_stage, metadata_validate_start);
            }

            let mut f = self.open_file(file_path, O_RDONLY, volume_dir).await?;

            if offset > 0 {
                f.seek(SeekFrom::Start(offset as u64)).await?;
            }

            let mut buffer = vec![0; length];
            f.read_exact(&mut buffer).await?;

            Ok(Bytes::from(buffer))
        }
    }

    async fn open_read_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        let volume_dir = local_disk_bucket_path(self.io_root(), volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let file_path = local_disk_object_path(self.io_root(), volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;

        let mut f = self.open_file_read_only(file_path).await?;

        let meta = f.metadata().await?;
        let end_offset = offset.checked_add(length).ok_or(DiskError::FileCorrupt)?;
        if meta.len() < end_offset as u64 {
            error!(
                event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                volume,
                path,
                offset,
                length,
                actual_size = meta.len(),
                reason = "read_file_stream_out_of_bounds",
                "Disk local read fallback failed"
            );
            return Err(DiskError::FileCorrupt);
        }

        if offset > 0 {
            f.seek(SeekFrom::Start(offset as u64)).await?;
        }

        let reclaim_on_drop = should_reclaim_file_cache_after_read(length);
        let reader = FileCacheReclaimReader::new(f, offset as u64, length, reclaim_on_drop);
        Ok(Box::new(StallTimeoutReader::new(reader, get_object_disk_read_timeout())))
    }

    async fn open_full_read(&self, volume: &str, path: &str) -> Result<FileReader> {
        let volume_dir = local_disk_bucket_path(self.io_root(), volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let file_path = local_disk_object_path(self.io_root(), volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;

        let f = self.open_file_read_only(file_path).await?;

        Ok(Box::new(f))
    }

    async fn open_write(&self, volume: &str, path: &str, mode: WriteMode) -> Result<FileWriter> {
        match mode {
            WriteMode::Truncate { size_hint } => {
                let volume_dir = local_disk_bucket_path(self.io_root(), volume)?;
                let file_path = local_disk_object_path(self.io_root(), volume, path)?;
                check_path_length(file_path.to_string_lossy().as_ref())?;

                if let Some(parent) = file_path.parent() {
                    os::make_dir_all(parent, &volume_dir).await?;
                }

                // O_DIRECT streaming write (Linux, opt-in): shard bytes stream
                // straight to the device so the commit-point fdatasync no longer
                // flushes the whole shard's dirty pages inside the rename_data
                // critical section. Latches off and falls back to the buffered
                // writer on filesystems that reject O_DIRECT; never surfaces the
                // EINVAL (which would masquerade as a missing shard).
                #[cfg(target_os = "linux")]
                if is_direct_io_write_enabled() && self.direct_io_write.supported.load(Ordering::Relaxed) {
                    let write_state = self.direct_io_write.clone();
                    let direct_path = file_path.clone();
                    let direct = tokio::task::spawn_blocking(move || open_direct_writer(&direct_path, &write_state))
                        .await
                        .map_err(|err| DiskError::other(format!("O_DIRECT open task failed: {err}")))??;
                    if let Some(writer) = direct {
                        return Ok(Box::new(writer));
                    }
                }

                // O_TRUNC: if a file already exists at this path, stale trailing bytes past
                // the new content would otherwise survive and mismatch the metadata size.
                let f = super::fs::open_file(&file_path, O_CREATE | O_WRONLY | O_TRUNC)
                    .await
                    .map_err(to_file_error)?;
                let reclaim_on_shutdown = should_reclaim_file_cache_after_write(size_hint);

                Ok(Box::new(FileCacheReclaimWriter::new(f, size_hint.max(0) as usize, reclaim_on_shutdown)))
            }
            WriteMode::Append => {
                let volume_dir = local_disk_bucket_path(self.io_root(), volume)?;
                if !skip_access_checks(volume) {
                    access(&volume_dir)
                        .await
                        .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
                }

                let file_path = local_disk_object_path(self.io_root(), volume, path)?;
                check_path_length(file_path.to_string_lossy().as_ref())?;

                let f = self.open_file(file_path, O_CREATE | O_APPEND | O_WRONLY, volume_dir).await?;

                Ok(Box::new(f))
            }
        }
    }

    // Descriptor-cache invalidation for StdBackend (rustfs/backlog#1801). On
    // non-Linux `fd_cache` does not exist, so these overrides are absent and the
    // trait's default no-op impls apply. On Linux they mirror UringBackend so
    // the existing LocalDisk mutation hooks (rename_data/rename_file/delete/
    // delete_volume/close) drop stale descriptors on every inode swap.
    #[cfg(target_os = "linux")]
    async fn invalidate_cached_fd(&self, volume: &str, path: &str) {
        if let Some(cache) = self.fd_cache.as_ref() {
            cache.invalidate_exact(volume, path).await;
        }
    }

    #[cfg(target_os = "linux")]
    fn invalidate_cached_fds_under(&self, volume: &str, path: &str) {
        if let Some(cache) = self.fd_cache.as_ref() {
            cache.invalidate_under(volume, path);
        }
    }

    #[cfg(target_os = "linux")]
    fn invalidate_cached_fds_for_volume(&self, volume: &str) {
        if let Some(cache) = self.fd_cache.as_ref() {
            cache.invalidate_volume(volume);
        }
    }

    #[cfg(target_os = "linux")]
    async fn clear_cached_fds(&self) {
        if let Some(cache) = self.fd_cache.as_ref() {
            cache.clear();
        }
    }
}

/// Enable the per-disk descriptor cache for io_uring reads (backlog#1145).
#[cfg(target_os = "linux")]
const ENV_RUSTFS_IO_URING_FD_CACHE: &str = "RUSTFS_IO_URING_FD_CACHE";
#[cfg(target_os = "linux")]
const DEFAULT_RUSTFS_IO_URING_FD_CACHE: bool = true;

/// Open descriptors kept per disk. Each entry holds an fd, so this bounds the
/// cache's share of `RLIMIT_NOFILE`. moka evicts asynchronously, so the count may
/// briefly exceed this.
#[cfg(target_os = "linux")]
const FD_CACHE_CAPACITY: u64 = 512;

/// Backstop on how long a cached descriptor may serve reads. Explicit
/// invalidation (below) is the correctness mechanism; this only bounds the
/// blast radius if a future mutation path forgets to call it.
#[cfg(target_os = "linux")]
const FD_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(5);

#[cfg(target_os = "linux")]
fn is_io_uring_fd_cache_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_RUSTFS_IO_URING_FD_CACHE, DEFAULT_RUSTFS_IO_URING_FD_CACHE)
}

/// Enable the per-disk descriptor cache for the default `StdBackend` reads
/// (rustfs/backlog#1801). Independent of the io_uring switch so each backend is
/// separately controllable; both share the same `rlimit_allows_fd_cache` guard
/// because each may hold up to `FD_CACHE_CAPACITY` (512) descriptors per disk.
#[cfg(target_os = "linux")]
const ENV_RUSTFS_LOCAL_FD_CACHE: &str = "RUSTFS_LOCAL_FD_CACHE";
#[cfg(target_os = "linux")]
const DEFAULT_RUSTFS_LOCAL_FD_CACHE: bool = true;

#[cfg(target_os = "linux")]
fn is_local_fd_cache_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_RUSTFS_LOCAL_FD_CACHE, DEFAULT_RUSTFS_LOCAL_FD_CACHE)
}

/// Whether the soft `RLIMIT_NOFILE` has enough headroom to run the fd cache
/// safely (rustfs/backlog#1178). The cache holds up to `FD_CACHE_CAPACITY` (512)
/// descriptors PER DISK and `try_new` cannot know the disk count, so a low limit
/// (the common 1024 default on a bare-metal / non-systemd run) would exhaust fds
/// with just a couple of disks. Require ample headroom before enabling it;
/// otherwise fall back to open-per-read. The packaged systemd unit sets
/// 1,048,576, so tuned deployments are unaffected.
#[cfg(target_os = "linux")]
fn rlimit_allows_fd_cache() -> bool {
    const MIN_SOFT_NOFILE: u64 = 16 << 10;
    match rustix::process::getrlimit(rustix::process::Resource::Nofile).current {
        Some(soft) => soft >= MIN_SOFT_NOFILE,
        None => true, // no soft limit (unlimited)
    }
}

/// Drop `offset..offset+length` from the page cache after an io_uring read,
/// mirroring what `StdBackend::pread_bytes` does for the same range
/// (backlog#1145).
///
/// The reclaim is a deliberate policy — large object reads are usually cold, and
/// keeping them resident evicts everything else — gated by
/// `RUSTFS_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE` (on by default) above
/// `RUSTFS_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD` (4 MiB). It is not an artifact of
/// how StdBackend happens to read, so enabling io_uring must not silently turn it
/// off. Errors surface exactly as they do on the StdBackend path.
#[cfg(target_os = "linux")]
fn reclaim_read_range(file: &std::fs::File, offset: u64, length: usize) -> Result<()> {
    use core::num::NonZeroU64;
    use rustix::fs::{Advice, fadvise};

    let length = u64::try_from(length).map_err(|_| DiskError::other("read reclaim length overflow"))?;
    if length == 0 {
        return Ok(());
    }
    // Page-align the reclaim window down to the containing page, matching
    // StdBackend's mmap reclaim `[aligned_offset, offset + length)`
    // (rustfs/backlog#1173). fadvise(DONTNEED) only drops fully-covered pages, so
    // reclaiming the raw unaligned range would leave the head partial page
    // resident that the mmap path drops — divergent residency for the same read
    // (bitrot shards' 32-byte block headers keep offsets off page boundaries, so
    // this is the common case, not a corner case).
    let page = mmap_page_size()?;
    let aligned_offset = offset - (offset % page);
    let end = offset
        .checked_add(length)
        .ok_or_else(|| DiskError::other("read reclaim range overflow"))?;
    let Some(aligned_len) = NonZeroU64::new(end - aligned_offset) else {
        return Ok(());
    };
    fadvise(file, aligned_offset, Some(aligned_len), Advice::DontNeed)
        .map_err(std::io::Error::from)
        .map_err(DiskError::from)
}

/// A cached descriptor is keyed by the open flags too: the O_DIRECT and buffered
/// read paths must never hand each other a descriptor opened the other way.
/// Only the buffered path caches today, so `direct` is always `false`; keeping it
/// in the key stops a future O_DIRECT cache from colliding with this one.
#[cfg(target_os = "linux")]
#[derive(PartialEq, Eq, Hash, Clone)]
struct FdKey {
    volume: String,
    path: String,
    direct: bool,
}

/// Per-disk cache of open descriptors for io_uring reads (backlog#1145).
///
/// Why this exists: `pread_uring` opened the file on the blocking pool for every
/// read, so each read paid a `spawn_blocking` round trip — the very thread hop
/// io_uring exists to avoid. Measured on a 16-core host with a 4-shard driver,
/// removing it is worth +36% to +180% IOPS and 3-5x better p999.
///
/// Why it is safe to cache a *part file* descriptor:
/// - only `<object>/<data_dir>/part.N` reaches this backend's `pread_bytes`;
///   `xl.meta` (the one path replaced in place) is read through `read_all` /
///   `read_metadata` and never gets here;
/// - part files are never rewritten in place — a replacement is always
///   write-new-tmp then `rename`, which swaps the inode, so a cached descriptor
///   can never observe a torn shard.
///
/// Why invalidation is nevertheless REQUIRED: heal reuses the existing version's
/// `data_dir` and renames a reconstructed shard onto the *same* part path. A
/// cached descriptor would keep serving the pre-heal (corrupt) inode, defeating
/// the heal and eroding read quorum. `delete` likewise unlinks the part while a
/// cached descriptor would keep the inode readable.
///
/// Backed by `moka`, already a dependency: its `get` is sharded rather than
/// behind one mutex (this cache is touched on every read, at >300k IOPS), and it
/// evicts by TinyLFU instead of arbitrarily. `time_to_live` supplies the backstop
/// TTL should a future mutation path forget to invalidate; `max_capacity` bounds
/// this cache's share of `RLIMIT_NOFILE`. Eviction drops the `Arc<File>`, closing
/// the descriptor once no in-flight read still holds it.
#[cfg(target_os = "linux")]
struct FdCache {
    cache: moka::future::Cache<FdKey, Arc<std::fs::File>>,
    /// Bumped by every invalidation. A miss-path open snapshots this before it
    /// opens and refuses to insert if it moved, so an fd opened before a
    /// heal/delete commit can never be resurrected into the cache after the
    /// commit's invalidation ran (open-then-insert race, rustfs/backlog#1176).
    generation: std::sync::atomic::AtomicU64,
}

#[cfg(target_os = "linux")]
impl FdCache {
    fn new() -> Self {
        Self::with_ttl(FD_CACHE_TTL)
    }

    /// Build a cache with an explicit TTL backstop. Production goes through
    /// `new` (`FD_CACHE_TTL`); tests inject a short TTL to exercise the backstop
    /// eviction (rustfs/backlog#1180) without a multi-second wait.
    fn with_ttl(ttl: std::time::Duration) -> Self {
        Self {
            cache: moka::future::Cache::builder()
                .max_capacity(FD_CACHE_CAPACITY)
                .time_to_live(ttl)
                // Required for `invalidate_entries_if`; without it that call
                // fails at runtime instead of dropping stale descriptors.
                .support_invalidation_closures()
                .build(),
            generation: std::sync::atomic::AtomicU64::new(0),
        }
    }

    async fn get(&self, key: &FdKey) -> Option<Arc<std::fs::File>> {
        self.cache.get(key).await
    }

    /// Current invalidation generation, snapshotted by the miss path before it
    /// opens a descriptor (rustfs/backlog#1176).
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Insert a freshly-opened descriptor only if no invalidation happened since
    /// it was opened (rustfs/backlog#1176). An invalidate that ran during the
    /// open bumped the generation, so a stale pre-heal/pre-delete inode is never
    /// cached. The post-insert re-check closes the tiny window where an
    /// invalidate races the insert itself, by removing the entry we just added.
    async fn insert_if_fresh(&self, key: FdKey, file: Arc<std::fs::File>, gen_at_open: u64) {
        if self.generation.load(Ordering::Acquire) != gen_at_open {
            return;
        }
        self.cache.insert(key.clone(), file).await;
        if self.generation.load(Ordering::Acquire) != gen_at_open {
            self.cache.invalidate(&key).await;
        }
    }

    /// Drop the descriptor for exactly this path. Preferred wherever the caller
    /// knows the keys: unlike a predicate it costs nothing on later reads.
    async fn invalidate_exact(&self, volume: &str, path: &str) {
        // Bump BEFORE the moka invalidation so a concurrent miss-path insert
        // that snapshotted the old generation is refused (rustfs/backlog#1176).
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.cache
            .invalidate(&FdKey {
                volume: volume.to_owned(),
                path: path.to_owned(),
                direct: false,
            })
            .await;
    }

    /// Drop every descriptor for `volume` whose path is `prefix` or lies under it.
    /// A component-boundary check keeps `a/b` from invalidating `a/bc`.
    ///
    /// moka applies the predicate to entries inserted at or before this call and
    /// guarantees a later `get` never returns one of them, so the invalidation is
    /// effective immediately even though the removal itself is deferred. Reserved
    /// for paths whose exact keys are unknown (`delete`); every predicate is
    /// re-evaluated by subsequent `get`s, so registering one per write would tax
    /// the read path.
    fn invalidate_under(&self, volume: &str, prefix: &str) {
        // Bump before registering the predicate so a concurrent miss-path insert
        // that snapshotted the old generation is refused (rustfs/backlog#1176).
        self.generation.fetch_add(1, Ordering::AcqRel);
        let volume = volume.to_owned();
        let prefix = prefix.trim_end_matches('/').to_owned();
        let matches = move |k: &FdKey, _: &Arc<std::fs::File>| {
            k.volume == volume && (k.path == prefix || k.path.strip_prefix(&prefix).is_some_and(|r| r.starts_with('/')))
        };
        if self.cache.invalidate_entries_if(matches).is_err() {
            // Closure support is enabled at construction, so this is unreachable.
            // If it ever changes, over-invalidate rather than serve a stale
            // descriptor: correctness first, the cache refills on the next read.
            self.cache.invalidate_all();
        }
    }

    /// Drop every descriptor for `volume` — the whole bucket is gone
    /// (rustfs/backlog#1177).
    fn invalidate_volume(&self, volume: &str) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        let volume = volume.to_owned();
        let matches = move |k: &FdKey, _: &Arc<std::fs::File>| k.volume == volume;
        if self.cache.invalidate_entries_if(matches).is_err() {
            self.cache.invalidate_all();
        }
    }

    /// Drop ALL descriptors — this disk instance is being retired
    /// (rustfs/backlog#1177).
    fn clear(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.cache.invalidate_all();
    }

    /// Unconditional insert. Production reads go through `insert_if_fresh` (the
    /// generation guard, rustfs/backlog#1176); this bare primitive is only for
    /// tests that drive the cache directly.
    #[cfg(test)]
    async fn insert(&self, key: FdKey, file: Arc<std::fs::File>) {
        self.cache.insert(key, file).await;
    }

    #[cfg(test)]
    async fn entry_count(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count()
    }
}

/// Runtime-probed io_uring read backend (backlog#1104).
///
/// Wraps a [`StdBackend`] for everything except positioned reads, which go
/// through rustfs-uring's cancel-safe `UringDriver`. Constructed only when
/// `RUSTFS_IO_URING_READ_ENABLE` is set AND the per-disk probe succeeds; on any
/// per-read driver error a read falls back to the inner `StdBackend`, so
/// behavior never regresses. The read preamble (path resolution, access checks,
/// bounds) mirrors `StdBackend::pread_bytes` exactly — only the raw byte read
/// differs.
#[cfg(target_os = "linux")]
pub(crate) struct UringBackend {
    root: PathBuf,
    /// Caches `root.display().to_string()` for the metric `"root"` label. `root`
    /// never changes after construction, so formatting the `Path` on every
    /// fallback emission is pure waste (rustfs/backlog#1185).
    root_label: String,
    inner: StdBackend,
    /// Wrapped in `ManuallyDrop` so `Drop` can move the (last) `Arc` onto a
    /// blocking thread: `UringDriver`'s own `Drop` joins its driver threads and
    /// can block up to the bounded-drain timeout on a hung disk, which must never
    /// run on a tokio worker during disk reconnect/shutdown (backlog#1170).
    /// `ManuallyDrop` derefs transparently, so read call sites are unchanged.
    driver: std::mem::ManuallyDrop<Arc<rustfs_uring::UringDriver>>,
    /// Runtime degradation latch (backlog#1101). Starts `true`; once a read
    /// returns a restriction-class errno (io_uring became unusable on this
    /// disk), it is set `false` and all further reads go straight to
    /// `StdBackend` — no more per-read io_uring attempts. Mirrors
    /// [`DirectIoReadState::supported`].
    active: std::sync::atomic::AtomicBool,
    fallback_logged: std::sync::atomic::AtomicBool,
    /// Per-disk O_DIRECT+io_uring state (backlog#1102). `supported` starts
    /// `true`; it latches `false` the first time the filesystem refuses
    /// O_DIRECT, after which O_DIRECT-eligible reads use `StdBackend`'s aligned
    /// path instead of retrying. `align` caches the probed device alignment so
    /// the `statx` probe runs at most once per disk. Independent of `active`:
    /// `active` gates io_uring as a whole, this gates only the native O_DIRECT
    /// read shape.
    direct_uring: DirectIoReadState,
    /// Count of reads that completed through the native io_uring + O_DIRECT path
    /// (`pread_uring_direct`) on this disk (rustfs/backlog#1213). Incremented only
    /// on success, so a value `> 0` is proof the native path actually executed
    /// rather than silently degrading to the StdBackend fallback. Tests assert on
    /// it to avoid a vacuous pass on filesystems that reject O_DIRECT; it also
    /// gives a gray release a positive signal that the O_DIRECT tier is serving
    /// reads instead of only ever counting fallbacks.
    native_direct_reads: std::sync::atomic::AtomicU64,
    /// Per-disk descriptor cache (backlog#1145). `None` when
    /// `RUSTFS_IO_URING_FD_CACHE` is off, which restores the open-per-read path.
    fd_cache: Option<FdCache>,
}

/// Disks whose io_uring probe failed, so `UringBackend::try_new` can skip
/// re-probing them on reconnect (per-disk probe cache, backlog#1101). A probe
/// creates a ring and spawns a driver thread; caching the negative result
/// avoids repeating that on every `LocalDisk` reconstruction of a disk that
/// does not support io_uring.
#[cfg(target_os = "linux")]
static URING_UNSUPPORTED_DISKS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<PathBuf>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

/// True when a runtime io_uring read error means the io_uring SUBSYSTEM is
/// unusable on this disk (→ latch off), as opposed to a per-read/file error
/// `StdBackend` would hit too (backlog#1101, narrowed in #1171). Data errors
/// (EIO), missing files, and parameter errors do NOT latch.
///
/// Deliberately narrower than the probe-time restriction class (C7 in the driver
/// warns the two must not be conflated): EACCES/EPERM at read time on an
/// already-open fd are usually per-file (an LSM hooks security_file_permission on
/// every read), and StdBackend would hit the same denial, so EACCES does NOT
/// latch the whole disk. EPERM is kept because a seccomp/LSM policy applied after
/// startup blocks io_uring_enter subsystem-wide. EOPNOTSUPP is classified
/// per-path by the caller (O_DIRECT shape vs subsystem), so it is not latched
/// here.
#[cfg(target_os = "linux")]
fn is_io_uring_unsupported(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::ENOSYS) | Some(libc::EPERM))
}

/// Resolve `volume`/`path` to the on-disk object path, running the same volume
/// access and path-length checks `StdBackend::pread_bytes` does before opening
/// (backlog#1102/#1145).
///
/// Blocking (`access_std` stats the volume dir), so it is called inside the
/// `spawn_blocking` closures of both io_uring read paths. It is the one piece
/// they genuinely share; each caller opens the returned path its own way
/// (buffered vs `O_DIRECT`) and maps the error into its own type.
#[cfg(target_os = "linux")]
fn resolve_uring_object_path(root: &Path, volume: &str, path: &str) -> Result<PathBuf> {
    let volume_dir = local_disk_bucket_path(root, volume)?;
    if !skip_access_checks(volume) {
        crate::disk::fs::access_std(&volume_dir)
            .map_err(|e| DiskError::from(to_access_error(e, DiskError::VolumeAccessDenied)))?;
    }
    let file_path = local_disk_object_path(root, volume, path)?;
    check_path_length(file_path.to_string_lossy().as_ref())?;
    Ok(file_path)
}

#[cfg(target_os = "linux")]
impl std::fmt::Debug for UringBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringBackend")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

#[cfg(target_os = "linux")]
impl Drop for UringBackend {
    fn drop(&mut self) {
        // Take the driver Arc out and, if we are on a tokio runtime, drop it on a
        // blocking thread. `UringDriver::Drop` sends Shutdown and joins each
        // driver thread; on a hung disk that join can block up to the bounded
        // drain timeout, which must not stall a runtime worker during disk
        // reconnect/shutdown (backlog#1170). Off-runtime (tests, plain threads) a
        // synchronous join is fine.
        // SAFETY: `ManuallyDrop::take` runs exactly once, here in `Drop`, and the
        // field is never used again.
        #[allow(unsafe_code)]
        let driver = unsafe { std::mem::ManuallyDrop::take(&mut self.driver) };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(move || drop(driver));
            }
            Err(_) => drop(driver),
        }
    }
}

#[cfg(target_os = "linux")]
impl UringBackend {
    /// Probe io_uring on `root`; `Some(backend)` if usable, `None` to fall back
    /// to `StdBackend`. A restricted-environment errno degrades quietly; an
    /// unexpected errno is surfaced as a warning (both still fall back).
    pub(crate) fn try_new(root: PathBuf) -> Option<Self> {
        // Per-disk probe cache: skip a disk already known not to support
        // io_uring (backlog#1101).
        if URING_UNSUPPORTED_DISKS
            .lock()
            .expect("uring probe cache mutex poisoned")
            .contains(&root)
        {
            return None;
        }
        let shards = get_io_uring_shards();
        match rustfs_uring::UringDriver::probe_and_start_sharded(URING_QUEUE_DEPTH, shards) {
            Ok(driver) => {
                info!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    root = %root.display(),
                    shards,
                    "io_uring read backend enabled"
                );
                // Gate the fd cache on RLIMIT_NOFILE headroom (rustfs/backlog#1178):
                // 512 fds/disk with a low soft limit and several disks would hit
                // EMFILE. Fall back to open-per-read when the limit is too small.
                let fd_cache = if is_io_uring_fd_cache_enabled() {
                    if rlimit_allows_fd_cache() {
                        Some(FdCache::new())
                    } else {
                        warn!(
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                            root = %root.display(),
                            "io_uring fd cache disabled: RLIMIT_NOFILE soft limit too low for 512 fds/disk; using open-per-read"
                        );
                        None
                    }
                } else {
                    None
                };
                let driver = Arc::new(driver);
                // Periodically export the driver StatsSnapshot to metrics so a
                // gray release is not flying blind (rustfs/backlog#1172).
                Self::spawn_stats_exporter(&driver, root.clone());
                // Compute the metric label once, before `root` moves into the
                // struct (rustfs/backlog#1185).
                let root_label = root.display().to_string();
                Some(Self {
                    inner: StdBackend::new_without_fd_cache(root.clone()),
                    root,
                    root_label,
                    driver: std::mem::ManuallyDrop::new(driver),
                    active: std::sync::atomic::AtomicBool::new(true),
                    fallback_logged: std::sync::atomic::AtomicBool::new(false),
                    direct_uring: DirectIoReadState::new(),
                    native_direct_reads: std::sync::atomic::AtomicU64::new(0),
                    fd_cache,
                })
            }
            Err(err) => {
                if err.is_expected_restriction() {
                    debug!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        root = %root.display(),
                        error = ?err,
                        "io_uring unavailable (restricted environment); using StdBackend"
                    );
                    // Only a genuine environment restriction is permanently
                    // negative-cached: it will not change without a restart.
                    URING_UNSUPPORTED_DISKS
                        .lock()
                        .expect("uring probe cache mutex poisoned")
                        .insert(root);
                } else {
                    // An unexpected error may be transient (ENOMEM/EMFILE under
                    // startup fd/memory pressure, ring/eventfd setup, a partial
                    // shard start). Do NOT latch the disk off io_uring forever;
                    // fall back for now and let the next LocalDisk reconstruction
                    // (disk reconnect) re-probe (rustfs/backlog#1171).
                    warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        root = %root.display(),
                        error = ?err,
                        "io_uring probe failed unexpectedly; using StdBackend (will re-probe on reconnect)"
                    );
                }
                None
            }
        }
    }

    /// Latch io_uring off for this whole disk, logging the transition once at
    /// warn (rustfs/backlog#1172). Without this the only signal operators ever
    /// see is the startup "backend enabled" line, which stays true on dashboards
    /// even after the first read latched the disk back to StdBackend forever.
    /// `swap` makes the log fire exactly once, on the true -> false edge.
    fn latch_active_off(&self, io_err: &std::io::Error) {
        if self.active.swap(false, Ordering::Relaxed) {
            counter!(METRIC_URING_LATCH_TOTAL, "root" => self.root.display().to_string()).increment(1);
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                event = EVENT_DISK_LOCAL_URING_LATCH_OFF,
                root = %self.root.display(),
                error = ?io_err,
                "io_uring latched off for this disk; all reads now use StdBackend"
            );
        }
    }

    /// Count an io_uring -> StdBackend read fallback, so a gray release can see
    /// how much traffic is actually on io_uring vs falling back
    /// (rustfs/backlog#1172).
    fn record_uring_fallback(&self) {
        // Clone the cached label (one alloc of a short string) instead of
        // re-formatting the `Path` per read (rustfs/backlog#1185).
        counter!(METRIC_URING_FALLBACK_TOTAL, "root" => self.root_label.clone()).increment(1);
    }

    /// Spawn a low-frequency task that exports the per-disk driver StatsSnapshot
    /// (in-flight, cq_overflow, submit_errors, cancel_already) to metrics so the
    /// gray release has runtime signal (rustfs/backlog#1172). The task holds only
    /// a `Weak` reference, so it never keeps the driver alive; when the last
    /// strong reference is gone it stops on the next tick. Any temporary strong
    /// reference it takes to read stats is dropped on the blocking pool so that,
    /// if it turns out to be the last one, `UringDriver::Drop`'s thread join never
    /// runs on an async worker (rustfs/backlog#1170).
    fn spawn_stats_exporter(driver: &Arc<rustfs_uring::UringDriver>, root: PathBuf) {
        // try_new may be constructed outside a tokio runtime (some unit tests
        // build the backend directly); only run the exporter when a runtime is
        // present. Production always constructs it from async LocalDisk::new.
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let weak = Arc::downgrade(driver);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(URING_STATS_EXPORT_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let Some(driver) = weak.upgrade() else {
                    break;
                };
                let s = driver.stats();
                tokio::task::spawn_blocking(move || drop(driver));
                // submit_errors is exported once the pinned rustfs-uring is bumped
                // to a release that carries it (rustfs/backlog#1172, #1181).
                let root = root.display().to_string();
                gauge!(METRIC_URING_IN_FLIGHT, "root" => root.clone()).set(s.in_flight as f64);
                gauge!(METRIC_URING_CQ_OVERFLOW, "root" => root.clone()).set(s.cq_overflow as f64);
                gauge!(METRIC_URING_CANCEL_ALREADY, "root" => root).set(s.cancel_already as f64);
            }
        });
    }

    /// Classify an O_DIRECT read-side error and latch the right path off, matching
    /// StdBackend (rustfs/backlog#1171). An O_DIRECT-shape error (EINVAL/EOPNOTSUPP)
    /// latches only the native direct path; a genuine subsystem error latches
    /// io_uring as a whole. Shared by the single-op and chunked read paths so the
    /// classification lives in one place (rustfs/backlog#1174).
    fn classify_direct_read_error(&self, io_err: &std::io::Error) {
        if is_direct_io_unsupported(io_err) {
            // This helper is only ever reached from the READ side: the O_DIRECT
            // `open` in `pread_uring_direct` already succeeded, and an open-time
            // refusal is handled separately as `DirectOpenError::ODirectRefused`
            // before any read is issued. So an EINVAL/EOPNOTSUPP arriving here is
            // a *read-time* error on an fd the kernel accepted for O_DIRECT. That
            // is far more likely an alignment bug in the aligned read path than a
            // filesystem that does not support O_DIRECT -- yet the old code
            // latched the whole disk's native path off with only a once-per-disk
            // debug trace, making a real correctness bug effectively invisible
            // (rustfs/backlog#1214).
            //
            // Diagnostics only: the fallback behaviour is unchanged. The native
            // O_DIRECT path is still latched off and the caller still falls back
            // to StdBackend for this and every future eligible read. We only make
            // the event observable -- a counter plus a once-per-disk `warn!`
            // instead of a silent `debug!` -- so an operator can see an alignment
            // regression rather than a mystery latency/CPU shift from buffered
            // reads.
            counter!(METRIC_URING_DIRECT_READ_EINVAL_TOTAL, "root" => self.root_label.clone()).increment(1);
            if !self.direct_uring.fallback_logged.swap(true, Ordering::Relaxed) {
                warn!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    root = %self.root.display(),
                    error = ?io_err,
                    "io_uring O_DIRECT read returned EINVAL/EOPNOTSUPP AFTER a successful O_DIRECT open; \
                     this is more likely an alignment bug than an unsupported filesystem. Latching the \
                     native O_DIRECT path off and reading via StdBackend (logged once per disk)"
                );
            }
            self.direct_uring.supported.store(false, Ordering::Relaxed);
        } else if is_io_uring_unsupported(io_err) {
            self.latch_active_off(io_err);
        }
    }

    /// Positioned read via io_uring. Mirrors `StdBackend::pread_bytes`'s
    /// resolution/access/bounds preamble, then reads the range with the driver
    /// (whole-range: the driver resubmits short reads for positioned reads).
    async fn pread_uring(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<Bytes> {
        let Some(end_offset) = offset.checked_add(length) else {
            return Err(DiskError::FileCorrupt);
        };
        let offset_u64 = u64::try_from(offset).map_err(|_| DiskError::FileCorrupt)?;

        // Descriptor-cache hit: no `open`, no `spawn_blocking`, so the read path
        // never leaves the runtime worker — which is the whole point of io_uring
        // (backlog#1145). The two preamble checks the miss path runs are not
        // silently lost:
        //   * bounds — a file shorter than `end_offset` short-reads, and the
        //     driver only short-reads at EOF (it resubmits otherwise), so the
        //     `bytes.len() != length` check below is exactly the old
        //     `meta.len() < end_offset` check and yields the same `FileCorrupt`;
        //   * volume access — skipped while an entry is live. A disk that became
        //     unreachable keeps serving its already-open descriptors for at most
        //     `FD_CACHE_TTL`, after which the re-open re-runs the check. Disk
        //     health is tracked independently of this per-read probe.
        // The cache handle and its lookup key travel together: `Some` exactly when
        // the fd cache is enabled, so neither use site below re-checks presence.
        let cache_entry = self.fd_cache.as_ref().map(|cache| {
            let key = FdKey {
                volume: volume.to_owned(),
                path: path.to_owned(),
                direct: false,
            };
            (cache, key)
        });
        let cached = match &cache_entry {
            Some((cache, key)) => cache.get(key).await,
            None => None,
        };

        let file = match cached {
            Some(file) => file,
            None => {
                // Snapshot the cache generation BEFORE opening (rustfs/backlog#1176):
                // if a heal/delete invalidation runs while this open is in flight,
                // the generation moves and insert_if_fresh refuses to cache the
                // now-stale descriptor.
                let gen_at_open = cache_entry.as_ref().map(|(cache, _)| cache.generation());
                let root = self.root.clone();
                let volume_owned = volume.to_owned();
                let path_owned = path.to_owned();
                let file = tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
                    let file_path = resolve_uring_object_path(&root, &volume_owned, &path_owned)?;
                    let file = std::fs::File::open(&file_path).map_err(DiskError::from)?;
                    let meta = file.metadata().map_err(DiskError::from)?;
                    let end_offset_u64 = u64::try_from(end_offset).map_err(|_| DiskError::FileCorrupt)?;
                    if meta.len() < end_offset_u64 {
                        return Err(DiskError::FileCorrupt);
                    }
                    Ok(file)
                })
                .await
                .map_err(|e| DiskError::other(format!("uring pread join error: {e}")))??;
                let file = Arc::new(file);
                if let (Some((cache, key)), Some(gen_at_open)) = (cache_entry, gen_at_open) {
                    cache.insert_if_fresh(key, Arc::clone(&file), gen_at_open).await;
                }
                file
            }
        };

        if length == 0 {
            // Parity with StdBackend and the miss path (rustfs/backlog#1173): a
            // zero-length read still rejects an offset past EOF. The miss path
            // validated `meta.len() < end_offset` (end_offset == offset here), but
            // a cache hit skipped it — so fstat the descriptor and match. This is
            // a rare path (callers do not issue zero-length reads), so the one
            // extra fstat is negligible.
            match file.metadata() {
                Ok(meta) if offset_u64 > meta.len() => return Err(DiskError::FileCorrupt),
                Ok(_) => {}
                Err(e) => return Err(DiskError::from(e)),
            }
            return Ok(Bytes::new());
        }

        // The driver consumes the handle; keep one for the post-read reclaim.
        let file_for_reclaim = Arc::clone(&file);
        let bytes = if length <= URING_MAX_OP_LEN {
            // Fast path: one op. The driver's Vec becomes the result with no copy.
            match self.driver.read_at(file, offset_u64, length).await {
                Ok(bytes) => bytes,
                Err(io_err) => {
                    // Latch io_uring off for this disk if the errno says the
                    // subsystem is unusable (backlog#1101); the caller falls back
                    // to StdBackend for this and every future read.
                    if is_io_uring_unsupported(&io_err) {
                        self.latch_active_off(&io_err);
                    }
                    return Err(DiskError::from(io_err));
                }
            }
        } else {
            // Very large read: split into sequential chunks so a single op cannot
            // pin ~length bytes of driver buffer, bounding worst-case in-flight
            // memory (rustfs/backlog#1174). Chunks are awaited one at a time, so
            // only one is in flight per read.
            let mut assembled = Vec::with_capacity(length);
            let mut done = 0usize;
            while done < length {
                let chunk = (length - done).min(URING_MAX_OP_LEN);
                let chunk_off = offset_u64 + done as u64;
                let part = match self.driver.read_at(Arc::clone(&file), chunk_off, chunk).await {
                    Ok(part) => part,
                    Err(io_err) => {
                        if is_io_uring_unsupported(&io_err) {
                            self.latch_active_off(&io_err);
                        }
                        return Err(DiskError::from(io_err));
                    }
                };
                if part.len() != chunk {
                    // A short chunk before the end means EOF: the file is shorter
                    // than offset + length, same as the miss path's meta check.
                    return Err(DiskError::FileCorrupt);
                }
                assembled.extend_from_slice(&part);
                done += chunk;
            }
            assembled
        };
        if bytes.len() != length {
            // The driver resubmits short reads, so a short result means EOF: the
            // file is shorter than `offset + length`. That is precisely what the
            // miss path's `meta.len() < end_offset` check rejects, so report the
            // same error whether or not the descriptor came from the cache.
            return Err(DiskError::FileCorrupt);
        }
        // Same page-cache policy as StdBackend: an io_uring read must not leave a
        // large shard resident just because it took a different code path.
        if should_reclaim_file_cache_after_read(length) {
            reclaim_read_range(&file_for_reclaim, offset_u64, length)?;
        }
        Ok(Bytes::from(bytes))
    }

    /// Native O_DIRECT positioned read through io_uring (backlog#1102): open the
    /// file with `O_DIRECT`, then let the driver read the block-aligned superset
    /// range into a block-aligned buffer and hand back exactly the requested
    /// logical range. This keeps BOTH io_uring's async submission AND O_DIRECT's
    /// page-cache bypass, instead of trading one for the other.
    ///
    /// Latching (so a failure is never re-attempted per-read):
    /// - the filesystem refusing O_DIRECT (`EINVAL`/`EOPNOTSUPP` on open) latches
    ///   [`Self::direct_uring`]`.supported` off — the caller then uses
    ///   `StdBackend`'s aligned path;
    /// - a restriction-class errno from the read latches [`Self::active`] off,
    ///   the whole-io_uring degradation from backlog#1101.
    ///
    /// Any other error is returned so the caller can fall back for this read
    /// without masking a genuine data problem as a permanent downgrade.
    async fn pread_uring_direct(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<Bytes> {
        let Some(end_offset) = offset.checked_add(length) else {
            return Err(DiskError::FileCorrupt);
        };
        let root = self.root.clone();
        let volume_owned = volume.to_owned();
        let path_owned = path.to_owned();
        // Probe the device alignment at most once per disk: pass the cached
        // value in so the blocking closure can skip `statx` when it is known.
        let cached_align = self.direct_uring.align.get().copied();

        let opened = tokio::task::spawn_blocking(move || -> std::result::Result<(std::fs::File, u64, usize), DirectOpenError> {
            use std::os::unix::fs::OpenOptionsExt;
            let file_path = resolve_uring_object_path(&root, &volume_owned, &path_owned).map_err(DirectOpenError::Disk)?;
            let file = match std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(rustix::fs::OFlags::DIRECT.bits() as i32)
                .open(&file_path)
            {
                Ok(file) => file,
                // Filesystem refuses O_DIRECT: signal a latch, not a hard error.
                Err(e) if is_direct_io_unsupported(&e) => return Err(DirectOpenError::ODirectRefused),
                Err(e) => return Err(DirectOpenError::Disk(DiskError::from(e))),
            };
            let meta = file.metadata().map_err(|e| DirectOpenError::Disk(DiskError::from(e)))?;
            let end_offset_u64 = u64::try_from(end_offset).map_err(|_| DirectOpenError::Disk(DiskError::FileCorrupt))?;
            if meta.len() < end_offset_u64 {
                return Err(DirectOpenError::Disk(DiskError::FileCorrupt));
            }
            let offset_u64 = u64::try_from(offset).map_err(|_| DirectOpenError::Disk(DiskError::FileCorrupt))?;
            let align = cached_align.unwrap_or_else(|| probe_direct_io_align(&file));
            Ok((file, offset_u64, align))
        })
        .await
        .map_err(|e| DiskError::other(format!("uring O_DIRECT pread join error: {e}")))?;

        let (file, offset_u64, probed_align) = match opened {
            Ok(t) => t,
            Err(DirectOpenError::ODirectRefused) => {
                // Latch the native O_DIRECT path off for this disk; the caller
                // falls back to StdBackend's aligned path for this and every
                // future eligible read.
                self.direct_uring.supported.store(false, Ordering::Relaxed);
                if !self.direct_uring.fallback_logged.swap(true, Ordering::Relaxed) {
                    debug!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        "filesystem refused O_DIRECT under io_uring; using StdBackend aligned path (logged once per disk)"
                    );
                }
                return Err(DiskError::other("filesystem refused O_DIRECT"));
            }
            Err(DirectOpenError::Disk(e)) => return Err(e),
        };
        // Cache the alignment so the next read skips the statx probe.
        let align = *self.direct_uring.align.get_or_init(|| probed_align);

        if length == 0 {
            return Ok(Bytes::new());
        }

        let file = Arc::new(file);
        let file_for_reclaim = Arc::clone(&file);
        let bytes = if length <= URING_MAX_OP_LEN {
            // Fast path: one op. The driver's Vec becomes the result with no copy.
            match self.driver.read_at_direct(Arc::clone(&file), offset_u64, length, align).await {
                Ok(bytes) => bytes,
                Err(io_err) => {
                    self.classify_direct_read_error(&io_err);
                    return Err(DiskError::from(io_err));
                }
            }
        } else {
            // Split a very large O_DIRECT read into sequential chunks so a single
            // op cannot pin ~length bytes of driver buffer, bounding worst-case
            // in-flight memory (rustfs/backlog#1174). read_at_direct aligns each
            // chunk's sub-range internally; chunk sizes are a multiple of
            // URING_MAX_OP_LEN, so boundary re-reads are at most one block.
            let mut assembled = Vec::with_capacity(length);
            let mut done = 0usize;
            while done < length {
                let chunk = (length - done).min(URING_MAX_OP_LEN);
                let chunk_off = offset_u64 + done as u64;
                let part = match self.driver.read_at_direct(Arc::clone(&file), chunk_off, chunk, align).await {
                    Ok(part) => part,
                    Err(io_err) => {
                        self.classify_direct_read_error(&io_err);
                        return Err(DiskError::from(io_err));
                    }
                };
                if part.len() != chunk {
                    return Err(DiskError::other("io_uring O_DIRECT returned a short read"));
                }
                assembled.extend_from_slice(&part);
                done += chunk;
            }
            assembled
        };
        if bytes.len() != length {
            return Err(DiskError::other("io_uring O_DIRECT returned a short read"));
        }
        // O_DIRECT should leave nothing resident, but a filesystem that quietly
        // buffered the read still has to honour the reclaim policy.
        if should_reclaim_file_cache_after_read(length) {
            reclaim_read_range(&file_for_reclaim, offset_u64, length)?;
        }
        // The native io_uring + O_DIRECT read completed (rustfs/backlog#1213):
        // record it so callers/tests can distinguish this path from the
        // StdBackend fallback, which never reaches here.
        self.native_direct_reads.fetch_add(1, Ordering::Relaxed);
        Ok(Bytes::from(bytes))
    }
}

/// Outcome of opening a file with `O_DIRECT` on the blocking pool for
/// [`UringBackend::pread_uring_direct`]. `ODirectRefused` is split out from a
/// generic disk error so the async side can latch the native direct path off
/// (rather than treating an unsupported filesystem as a hard read failure).
#[cfg(target_os = "linux")]
enum DirectOpenError {
    ODirectRefused,
    Disk(DiskError),
}

#[cfg(target_os = "linux")]
#[async_trait::async_trait]
impl LocalIoBackend for UringBackend {
    async fn pread_bytes(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
        metrics: Option<MmapCopyStageMetrics>,
    ) -> Result<Bytes> {
        // Latched off (backlog#1101): io_uring proved unusable on this disk, so
        // skip it entirely and read via StdBackend.
        if !self.active.load(Ordering::Relaxed) {
            self.record_uring_fallback();
            return self.inner.pread_bytes(volume, path, offset, length, metrics).await;
        }

        // O_DIRECT interop (backlog#1102): pick the read shape by eligibility
        // and per-disk capability, preferring the path that keeps io_uring's
        // async submission — never a blanket downgrade.
        let direct_eligible = is_direct_io_read_enabled() && length > 0 && length >= get_direct_io_read_threshold();
        if direct_eligible {
            if self.direct_uring.supported.load(Ordering::Relaxed) {
                // Best path: io_uring + native O_DIRECT (async submission AND no
                // page-cache pollution). On any error fall back to StdBackend
                // for this read; the latching errnos already flipped the
                // relevant per-disk latch inside `pread_uring_direct`.
                match self.pread_uring_direct(volume, path, offset, length).await {
                    Ok(bytes) => return Ok(bytes),
                    Err(err) => {
                        if !self.fallback_logged.swap(true, Ordering::Relaxed) {
                            debug!(
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                                error = ?err,
                                "io_uring O_DIRECT read fell back to StdBackend (logged once per disk)"
                            );
                        }
                        self.record_uring_fallback();
                        return self.inner.pread_bytes(volume, path, offset, length, metrics).await;
                    }
                }
            }
            // O_DIRECT proved unusable on this disk earlier: use StdBackend's
            // aligned path, which itself degrades to buffered if the filesystem
            // rejects O_DIRECT. Not an io_uring downgrade — io_uring cannot
            // serve an O_DIRECT read here without polluting the page cache.
            return self.inner.pread_bytes(volume, path, offset, length, metrics).await;
        }

        // Non-O_DIRECT read: buffered io_uring, falling back to StdBackend on any
        // per-read error.
        match self.pread_uring(volume, path, offset, length).await {
            Ok(bytes) => Ok(bytes),
            Err(err) => {
                if !self.fallback_logged.swap(true, Ordering::Relaxed) {
                    let latched = !self.active.load(Ordering::Relaxed);
                    debug!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        error = ?err,
                        latched_off = latched,
                        "io_uring read fell back to StdBackend (logged once per disk)"
                    );
                }
                self.record_uring_fallback();
                self.inner.pread_bytes(volume, path, offset, length, metrics).await
            }
        }
    }

    async fn open_read_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        self.inner.open_read_stream(volume, path, offset, length).await
    }

    async fn open_full_read(&self, volume: &str, path: &str) -> Result<FileReader> {
        self.inner.open_full_read(volume, path).await
    }

    async fn open_write(&self, volume: &str, path: &str, mode: WriteMode) -> Result<FileWriter> {
        self.inner.open_write(volume, path, mode).await
    }

    async fn invalidate_cached_fd(&self, volume: &str, path: &str) {
        if let Some(cache) = self.fd_cache.as_ref() {
            cache.invalidate_exact(volume, path).await;
        }
    }

    fn invalidate_cached_fds_under(&self, volume: &str, path: &str) {
        if let Some(cache) = self.fd_cache.as_ref() {
            cache.invalidate_under(volume, path);
        }
    }

    fn invalidate_cached_fds_for_volume(&self, volume: &str) {
        if let Some(cache) = self.fd_cache.as_ref() {
            cache.invalidate_volume(volume);
        }
    }

    async fn clear_cached_fds(&self) {
        if let Some(cache) = self.fd_cache.as_ref() {
            cache.clear();
        }
    }
}

/// Select the local read backend: the runtime-probed io_uring backend when
/// enabled and the per-disk probe succeeds, otherwise the default
/// [`StdBackend`] (backlog#1104). Enabling io_uring is opt-in and falls back
/// byte-for-byte, so the default build is unchanged.
fn build_local_io_backend(root: PathBuf) -> Arc<dyn LocalIoBackend> {
    #[cfg(target_os = "linux")]
    if is_io_uring_read_enabled()
        && let Some(backend) = UringBackend::try_new(root.clone())
    {
        return Arc::new(backend);
    }
    Arc::new(StdBackend::new(root))
}

#[allow(
    dead_code,
    reason = "path cache and cwd slots retained beside the disk root they derive from (backlog#1823)"
)]
pub struct LocalDisk {
    pub root: PathBuf,
    publication_root: os::PublicationRoot,
    /// I/O root pinned to the mount instance that was opened while the disk
    /// was initialized. On Linux this is `/proc/self/fd/<dirfd>/.`; resolving
    /// paths beneath it keeps repair I/O on that mount even if the configured
    /// pathname is later covered by another mount.
    io_root: PathBuf,
    #[cfg(target_os = "linux")]
    mount_lease: std::fs::File,
    #[cfg(target_os = "linux")]
    mount_lease_mount_id: Option<u64>,
    /// Public path for callers that need the configured disk layout. Internal
    /// disk I/O uses `io_format_path`, which is rooted at `mount_lease`.
    pub format_path: PathBuf,
    io_format_path: PathBuf,
    pub format_info: RwLock<FormatInfo>,
    pub endpoint: Endpoint,
    pub disk_info_cache: Arc<Cache<DiskInfo>>,
    pub scanning: Arc<AtomicU32>,
    pub rotational: bool,
    pub fstype: String,
    pub major: u64,
    pub minor: u64,
    pub nrrequests: u64,
    // Performance optimization fields
    path_cache: Arc<ParkingLotRwLock<HashMap<String, PathBuf>>>,
    current_dir: Arc<OnceLock<PathBuf>>,
    // pub id: Mutex<Option<Uuid>>,
    // pub format_data: Mutex<Vec<u8>>,
    // pub format_file_info: Mutex<Option<Metadata>>,
    // pub format_last_check: Mutex<Option<OffsetDateTime>>,
    startup_cleanup_ready: Arc<AtomicU32>,
    startup_cleanup_notify: Arc<Notify>,
    exit_signal: Option<tokio::sync::broadcast::Sender<()>>,
    io_backend: Arc<dyn LocalIoBackend>,
    file_sync_permits: Arc<Semaphore>,
    snapshot_leases: Arc<Mutex<SnapshotLeaseRegistry>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SnapshotLeaseKey {
    volume: String,
    path: String,
}

#[derive(Default)]
struct SnapshotLeaseEntry {
    tokens: HashSet<SnapshotLeaseToken>,
    pending_delete: Option<DeleteOptions>,
    deleting: bool,
    mutation_fence: Option<Arc<QuotaMutationFenceState>>,
}

#[derive(Default)]
struct QuotaMutationFenceState {
    revoked: AtomicBool,
    running: AtomicUsize,
    notify: Notify,
}

struct QuotaMutationFenceClaim {
    state: Arc<QuotaMutationFenceState>,
}

impl Drop for QuotaMutationFenceClaim {
    fn drop(&mut self) {
        self.state.running.fetch_sub(1, Ordering::AcqRel);
        self.state.notify.notify_waiters();
    }
}

#[derive(Default)]
struct SnapshotLeaseRegistry {
    entries: HashMap<SnapshotLeaseKey, SnapshotLeaseEntry>,
}

impl Drop for LocalDisk {
    fn drop(&mut self) {
        if let Some(exit_signal) = self.exit_signal.take() {
            let _ = exit_signal.send(());
        }
    }
}

impl Debug for LocalDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalDisk")
            .field("root", &self.root)
            .field("format_path", &self.format_path)
            .field("format_info", &self.format_info)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// Resolve the local disk root path from an endpoint path.
///
/// Tries `canonicalize` first (fast path). On Windows, if canonicalization reports
/// `NotFound` for paths that may still be valid mount roots, falls back to
/// `absolutize` + metadata check to accept valid local directory roots that
/// don't support full canonicalization.
fn resolve_local_disk_root(ep_path: &str) -> Result<PathBuf> {
    match rustfs_utils::canonicalize(ep_path) {
        Ok(path) => Ok(path),
        Err(err) => {
            if err.kind() != ErrorKind::NotFound {
                return Err(to_file_error(err).into());
            }

            #[cfg(windows)]
            {
                // On Windows, canonicalize can fail for ZFS volumes, junction points,
                // subst drives, and other non-standard filesystem mounts. Try a fallback
                // path resolution using absolutize + metadata check.
                let absolute = match crate::disk::endpoint::windows_fallback_local_path(ep_path, &err, "local disk root") {
                    Ok(path) => path,
                    Err(_) => {
                        return Err(DiskError::VolumeNotFound);
                    }
                };

                match std::fs::metadata(&absolute) {
                    Ok(metadata) => {
                        if !metadata.is_dir() {
                            return Err(DiskError::DiskNotDir);
                        }
                        return Ok(absolute);
                    }
                    Err(meta_err) => {
                        if meta_err.kind() == ErrorKind::NotFound {
                            return Err(DiskError::VolumeNotFound);
                        }
                        return Err(to_file_error(meta_err).into());
                    }
                }
            }

            #[cfg(not(windows))]
            {
                Err(DiskError::VolumeNotFound)
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReplacementMountIdentity {
    device: u64,
    inode: u64,
    mount_id: u64,
}

#[cfg(target_os = "linux")]
fn replacement_mount_identity(metadata: &std::fs::Metadata, mount_id: u64) -> ReplacementMountIdentity {
    use std::os::unix::fs::MetadataExt as _;

    ReplacementMountIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        mount_id,
    }
}

#[cfg(target_os = "linux")]
fn mount_id_for_fd(fd: &std::fs::File) -> Option<u64> {
    use rustix::fs::{AtFlags, StatxFlags};
    use std::os::fd::AsRawFd as _;

    rustix::fs::statx(fd, "", AtFlags::EMPTY_PATH, StatxFlags::MNT_ID)
        .ok()
        .filter(|statx| StatxFlags::from_bits_retain(statx.stx_mask).contains(StatxFlags::MNT_ID))
        .map(|statx| statx.stx_mnt_id)
        .or_else(|| mount_id_from_fdinfo(fd.as_raw_fd()))
}

#[cfg(target_os = "linux")]
fn mount_id_for_path(path: &Path) -> Option<u64> {
    use rustix::fs::{AtFlags, CWD, StatxFlags};

    rustix::fs::statx(CWD, path, AtFlags::empty(), StatxFlags::MNT_ID)
        .ok()
        .filter(|statx| StatxFlags::from_bits_retain(statx.stx_mask).contains(StatxFlags::MNT_ID))
        .map(|statx| statx.stx_mnt_id)
        .or_else(|| {
            std::fs::canonicalize(path)
                .ok()
                .and_then(|path| mount_id_from_mountinfo(&path))
        })
}

#[cfg(target_os = "linux")]
fn mount_id_from_fdinfo(fd: std::os::fd::RawFd) -> Option<u64> {
    std::fs::read_to_string(format!("/proc/self/fdinfo/{fd}"))
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:")?.trim().parse().ok())
}

#[cfg(target_os = "linux")]
fn mount_id_from_mountinfo(path: &Path) -> Option<u64> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    mount_id_from_mountinfo_contents(&mountinfo, path)
}

#[cfg(target_os = "linux")]
fn mount_id_from_mountinfo_contents(mountinfo: &str, path: &Path) -> Option<u64> {
    let mountpoint = path
        .to_string_lossy()
        .replace('\\', "\\134")
        .replace('\t', "\\011")
        .replace('\n', "\\012")
        .replace(' ', "\\040");
    mountinfo.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let mount_id = fields.next()?.parse().ok()?;
        fields.next()?;
        fields.next()?;
        fields.next()?;
        (fields.next()? == mountpoint).then_some(mount_id)
    })
}

impl LocalDisk {
    #[cfg(target_os = "linux")]
    fn open_mount_lease(root: &Path) -> Result<(std::fs::File, PathBuf, Option<u64>)> {
        use rustix::fs::{Mode, OFlags, open};
        use std::os::fd::AsRawFd as _;

        let fd = open(
            root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(std::io::Error::from)
        .map_err(DiskError::from)?;
        let lease = std::fs::File::from(fd);
        let io_root = PathBuf::from(format!("/proc/self/fd/{}/.", lease.as_raw_fd()));
        let mount_id = mount_id_for_fd(&lease);
        Ok((lease, io_root, mount_id))
    }

    #[cfg(not(target_os = "linux"))]
    fn open_mount_lease(root: &Path) -> Result<PathBuf> {
        Ok(root.to_path_buf())
    }

    fn io_root(&self) -> &Path {
        &self.io_root
    }

    /// Auto-replacement is destructive, so it is admitted only when the
    /// configured endpoint still names the directory held by `mount_lease`.
    pub fn has_replacement_mount_lease(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            let Ok(configured) = std::fs::metadata(self.endpoint.get_file_path()) else {
                return false;
            };
            let Ok(pinned) = self.mount_lease.metadata() else {
                return false;
            };
            let Some(configured_mount_id) = mount_id_for_path(Path::new(&self.endpoint.get_file_path())) else {
                return false;
            };
            let Some(pinned_mount_id) = self.mount_lease_mount_id else {
                return false;
            };
            replacement_mount_identity(&configured, configured_mount_id) == replacement_mount_identity(&pinned, pinned_mount_id)
        }

        #[cfg(not(target_os = "linux"))]
        false
    }

    /// Return the descriptor-rooted path for the mount instance admitted for
    /// automatic replacement. Callers must not derive destructive identity
    /// from the mutable endpoint pathname.
    pub fn replacement_mount_lease_root(&self) -> Option<PathBuf> {
        self.has_replacement_mount_lease().then(|| self.io_root.clone())
    }

    pub async fn new(ep: &Endpoint, cleanup: bool) -> Result<Self> {
        debug!(
            event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
            endpoint = %ep,
            state = "create_started",
            cleanup,
            "Local disk creation started"
        );
        let endpoint_path = ep.get_file_path();
        let root = resolve_local_disk_root(&endpoint_path).inspect_err(|err| {
            log_startup_disk_error("resolve_local_disk_root", Path::new(&endpoint_path), err);
        })?;
        #[cfg(windows)]
        let publication_root_path = {
            // `resolve_local_disk_root` validates fallback mount roots. The
            // publication root must still retain the configured alias so paths
            // created before final-path normalization remain relative to it.
            drop(root);
            Path::new(&endpoint_path)
        };
        #[cfg(not(windows))]
        let publication_root_path = root.as_path();
        let publication_root = os::PublicationRoot::new(publication_root_path)
            .map_err(DiskError::from)
            .inspect_err(|err| {
                log_startup_disk_error("open_publication_root", publication_root_path, err);
            })?;
        // On Windows the configured endpoint may be a junction, subst drive, or
        // mapped path. Use the final path from the pinned root handle for every
        // subsequent path-based operation so retargeting the configured alias
        // cannot split ordinary IO from handle-relative publication.
        let root = publication_root.path().to_path_buf();

        #[cfg(target_os = "linux")]
        let (mount_lease, io_root, mount_lease_mount_id) = Self::open_mount_lease(&root)?;
        #[cfg(not(target_os = "linux"))]
        let io_root = Self::open_mount_lease(&root)?;

        ensure_data_usage_layout(&io_root)
            .await
            .map_err(DiskError::from)
            .inspect_err(|err| {
                log_startup_disk_error("ensure_data_usage_layout", &root, err);
            })?;

        let startup_cleanup_ready = Arc::new(AtomicU32::new(u32::from(!cleanup)));
        let startup_cleanup_notify = Arc::new(Notify::new());

        if cleanup
            && let Err(err) = Self::cleanup_tmp_on_startup(
                &io_root,
                &publication_root,
                startup_cleanup_ready.clone(),
                startup_cleanup_notify.clone(),
            )
            .await
        {
            startup_cleanup_ready.store(1, Ordering::Release);
            startup_cleanup_notify.notify_waiters();
            warn!(
                event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                root = ?root,
                state = "failed",
                error = ?err,
                "Local disk startup cleanup failed"
            );
        }

        // Use optimized path resolution instead of absolutize_virtually
        let format_path = root.join(RUSTFS_META_BUCKET).join(super::FORMAT_CONFIG_FILE);
        let io_format_path = io_root.join(RUSTFS_META_BUCKET).join(super::FORMAT_CONFIG_FILE);
        debug!(
            event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
            root = ?root,
            format_path = ?format_path,
            state = "format_path_resolved",
            "Local disk format path resolved"
        );
        let (format_data, format_meta) = read_file_exists(&io_format_path).await.inspect_err(|err| {
            log_startup_disk_error("read_format_json", &io_format_path, err);
        })?;

        let mut id = None;
        // let mut format_legacy = false;
        let mut format_last_check = None;

        if !format_data.is_empty() {
            let s = format_data.as_ref();
            let fm = FormatV3::try_from(s).map_err(Error::other)?;
            let (set_idx, disk_idx) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

            if set_idx as i32 != ep.set_idx || disk_idx as i32 != ep.disk_idx {
                return Err(DiskError::InconsistentDisk);
            }

            id = Some(fm.erasure.this);
            // format_legacy = fm.erasure.distribution_algo == DistributionAlgoVersion::V1;
            format_last_check = Some(OffsetDateTime::now_utc());
        }

        let format_info = FormatInfo {
            id,
            data: format_data,
            file_info: format_meta,
            last_check: format_last_check,
        };
        let root_clone = root.clone();
        let update_fn: UpdateFn<DiskInfo> = Box::new(move || {
            let disk_id = id;
            let root = root_clone.clone();
            Box::pin(async move {
                match get_disk_info(root.clone()).await {
                    Ok((info, is_root_disk)) => {
                        let physical_device_ids = match rustfs_utils::os::get_physical_device_ids(root.to_string_lossy().as_ref())
                        {
                            Ok(ids) => ids,
                            Err(err) => {
                                warn!(
                                    event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                                    root = ?root,
                                    state = "physical_device_id_lookup_failed",
                                    error = ?err,
                                    "Disk local startup metadata lookup failed"
                                );
                                Vec::new()
                            }
                        };
                        // An erasure-set heal drops a marker on the disks it is
                        // rebuilding (see rustfs-heal); surface it so scanner
                        // coordination, lock selection and admin/metrics see
                        // the rebuild. Refreshed with this cache (~1s).
                        let healing = tokio::fs::try_exists(root.join(RUSTFS_META_BUCKET).join(super::HEALING_MARKER_PATH))
                            .await
                            .unwrap_or(false);
                        let disk_info = DiskInfo {
                            total: info.total,
                            free: info.free,
                            used: info.used,
                            used_inodes: info.files.saturating_sub(info.ffree),
                            free_inodes: info.ffree,
                            major: info.major,
                            minor: info.minor,
                            fs_type: info.fstype,
                            root_disk: is_root_disk,
                            physical_device_ids,
                            id: disk_id,
                            healing,
                            ..Default::default()
                        };
                        // if root {
                        //     return Err(Error::new(DiskError::DriveIsRoot));
                        // }

                        Ok(disk_info)
                    }
                    Err(err) => Err(err.into()),
                }
            })
        });

        let cache = Cache::new(update_fn, Duration::from_secs(1), Opts::default());

        // TODO(backlog): add O_DIRECT I/O support for performance-critical paths
        // TODO(backlog): populate DiskInfo in constructor
        let mut disk = Self {
            root: root.clone(),
            publication_root,
            io_root: io_root.clone(),
            #[cfg(target_os = "linux")]
            mount_lease,
            #[cfg(target_os = "linux")]
            mount_lease_mount_id,
            endpoint: ep.clone(),
            format_path,
            io_format_path,
            format_info: RwLock::new(format_info),
            disk_info_cache: Arc::new(cache),
            scanning: Arc::new(AtomicU32::new(0)),
            rotational: Default::default(),
            fstype: Default::default(),
            minor: Default::default(),
            major: Default::default(),
            nrrequests: Default::default(),
            // // format_legacy,
            // format_file_info: Mutex::new(format_meta),
            // format_data: Mutex::new(format_data),
            // format_last_check: Mutex::new(format_last_check),
            path_cache: Arc::new(ParkingLotRwLock::new(HashMap::with_capacity(2048))),
            current_dir: Arc::new(OnceLock::new()),
            startup_cleanup_ready,
            startup_cleanup_notify,
            exit_signal: None,
            io_backend: build_local_io_backend(io_root.clone()),
            file_sync_permits: os::disk_file_sync_limiter(&root),
            snapshot_leases: Arc::new(Mutex::new(SnapshotLeaseRegistry::default())),
        };
        let (info, _root) = get_disk_info(root.clone()).await.inspect_err(|err| {
            log_startup_disk_error("get_disk_info", &root, err);
        })?;
        disk.major = info.major;
        disk.minor = info.minor;
        disk.fstype = info.fstype;

        // if root {
        //     return Err(Error::new(DiskError::DriveIsRoot));
        // }

        if info.nrrequests > 0 {
            disk.nrrequests = info.nrrequests;
        }

        if info.rotational {
            disk.rotational = true;
        }

        disk.make_meta_volumes().await.inspect_err(|err| {
            log_startup_disk_error("make_meta_volumes", &disk.root, err);
        })?;

        let (exit_tx, exit_rx) = tokio::sync::broadcast::channel(1);
        disk.exit_signal = Some(exit_tx);

        let io_root = disk.io_root.clone();
        let publication_root = disk.publication_root.clone();
        tokio::spawn(Self::cleanup_deleted_objects_loop(io_root, publication_root, exit_rx));
        debug!(
            event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
            endpoint = %disk.endpoint,
            root = ?disk.root,
            state = "created",
            "Local disk created"
        );
        Ok(disk)
    }

    async fn cleanup_deleted_objects_loop(
        root: PathBuf,
        publication_root: os::PublicationRoot,
        mut exit_rx: tokio::sync::broadcast::Receiver<()>,
    ) {
        let start_at = Instant::now() + DELETED_OBJECTS_CLEANUP_INTERVAL;
        let mut interval = interval_at(start_at, DELETED_OBJECTS_CLEANUP_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = Self::cleanup_deleted_objects(root.clone()).await {
                        error!(
                            event = EVENT_DISK_LOCAL_BACKGROUND_CLEANUP,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                            task = "deleted_objects",
                            state = "failed",
                            error = ?err,
                            "Disk local background cleanup failed"
                        );
                    }
                    if let Err(err) = Self::cleanup_stale_tmp_objects(root.clone(), &publication_root).await {
                        error!(
                            event = EVENT_DISK_LOCAL_BACKGROUND_CLEANUP,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                            task = "stale_tmp_objects",
                            state = "failed",
                            error = ?err,
                            "Disk local background cleanup failed"
                        );
                    }
                }
                _ = exit_rx.recv() => {
                    info!(
                        event = EVENT_DISK_LOCAL_BACKGROUND_CLEANUP,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        task = "deleted_objects_loop",
                        state = "stopped",
                        "Disk local background cleanup loop stopped"
                    );
                    break;
                }
            }
        }
    }

    fn meta_path(root: &Path, meta_path: &str) -> PathBuf {
        #[cfg(windows)]
        let meta_path = meta_path.replace('/', "\\");
        #[cfg(not(windows))]
        let meta_path = meta_path.to_string();

        root.join(meta_path)
    }

    async fn cleanup_tmp_on_startup(
        root: &Path,
        publication_root: &os::PublicationRoot,
        startup_cleanup_ready: Arc<AtomicU32>,
        startup_cleanup_notify: Arc<Notify>,
    ) -> Result<()> {
        let tmp_path = Self::meta_path(root, RUSTFS_META_TMP_BUCKET);
        let tmp_old_path = Self::meta_path(root, RUSTFS_META_TMP_OLD_BUCKET).join(Uuid::new_v4().to_string());

        rename_all_ignore_missing_source(&tmp_path, &tmp_old_path, root, publication_root)
            .await
            .inspect_err(|err| {
                log_startup_disk_error("cleanup_tmp_rename_all", &tmp_path, err);
            })?;

        let tmp_deleted_path = Self::meta_path(root, RUSTFS_META_TMP_DELETED_BUCKET);
        tokio::fs::create_dir_all(&tmp_deleted_path).await.inspect_err(|err| {
            log_startup_disk_io_error("cleanup_tmp_create_deleted_dir", &tmp_deleted_path, err);
        })?;

        let tmp_old_root = Self::meta_path(root, RUSTFS_META_TMP_OLD_BUCKET);
        tokio::spawn(async move {
            if let Err(err) = tokio::fs::remove_dir_all(&tmp_old_root).await
                && err.kind() != ErrorKind::NotFound
            {
                log_startup_disk_io_error("cleanup_tmp_remove_old_dir", &tmp_old_root, &err);
            }
            startup_cleanup_ready.store(1, Ordering::Release);
            startup_cleanup_notify.notify_waiters();
        });

        Ok(())
    }

    async fn wait_for_startup_cleanup(&self) {
        if self.startup_cleanup_ready.load(Ordering::Acquire) != 0 {
            return;
        }

        if wait_for_startup_cleanup_signal(
            self.startup_cleanup_ready.as_ref(),
            self.startup_cleanup_notify.as_ref(),
            STARTUP_CLEANUP_WAIT_TIMEOUT,
        )
        .await
        {
            debug!(disk = %self.endpoint, "startup cleanup barrier released before walk_dir");
        } else {
            warn!(
                event = EVENT_DISK_LOCAL_STARTUP_CLEANUP,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                disk = %self.endpoint,
                timeout_ms = STARTUP_CLEANUP_WAIT_TIMEOUT.as_millis(),
                state = "timed_out",
                "Disk local startup cleanup barrier timed out"
            );
        }
    }

    async fn cleanup_stale_tmp_objects(root: PathBuf, publication_root: &os::PublicationRoot) -> Result<()> {
        Self::cleanup_stale_tmp_objects_with_expiry(root, publication_root, STALE_TMP_OBJECT_EXPIRY).await
    }

    async fn cleanup_stale_tmp_objects_with_expiry(
        root: PathBuf,
        publication_root: &os::PublicationRoot,
        expiry: Duration,
    ) -> Result<()> {
        let tmp_path = Self::meta_path(&root, RUSTFS_META_TMP_BUCKET);
        let mut entries = match fs::read_dir(&tmp_path).await {
            Ok(entries) => entries,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(());
                }
                return Err(e.into());
            }
        };

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.is_empty() || name == "." || name == ".." || name == ".trash" {
                continue;
            }

            let file_type = entry.file_type().await?;
            if !file_type.is_dir() {
                continue;
            }

            let Some(age) = entry
                .metadata()
                .await?
                .modified()
                .ok()
                .and_then(|modified| modified.elapsed().ok())
            else {
                continue;
            };
            if age <= expiry {
                continue;
            }

            let target_path = Self::meta_path(&root, RUSTFS_META_TMP_DELETED_BUCKET).join(Uuid::new_v4().to_string());
            rename_all(entry.path(), target_path, Self::meta_path(&root, RUSTFS_META_BUCKET), publication_root).await?;
        }

        Ok(())
    }

    async fn cleanup_deleted_objects(root: PathBuf) -> Result<()> {
        let trash = Self::meta_path(&root, RUSTFS_META_TMP_DELETED_BUCKET);
        let mut entries = match fs::read_dir(&trash).await {
            Ok(entries) => entries,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(());
                }
                return Err(e.into());
            }
        };

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.is_empty() || name == "." || name == ".." {
                continue;
            }

            let file_type = entry.file_type().await?;

            let path = trash.join(name);

            if file_type.is_dir() {
                if let Err(e) = tokio::fs::remove_dir_all(path).await
                    && e.kind() != ErrorKind::NotFound
                {
                    return Err(e.into());
                }
            } else if let Err(e) = tokio::fs::remove_file(path).await
                && e.kind() != ErrorKind::NotFound
            {
                return Err(e.into());
            }
        }

        Ok(())
    }

    fn is_valid_volname(volname: &str) -> bool {
        if volname.len() < 3 {
            return false;
        }

        #[cfg(target_os = "windows")]
        {
            // Windows volume names must not include reserved characters.
            // This regular expression matches disallowed characters.
            if volname.contains('|')
                || volname.contains('<')
                || volname.contains('>')
                || volname.contains('?')
                || volname.contains('*')
                || volname.contains(':')
                || volname.contains('"')
                || volname.contains('\\')
            {
                return false;
            }
        }

        true
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn check_format_json(&self) -> Result<Metadata> {
        let md = fs::metadata(&self.io_format_path).await.map_err(to_unformatted_disk_error)?;
        Ok(md)
    }
    async fn make_meta_volumes(&self) -> Result<()> {
        let buckets = format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}");
        let multipart = format!("{}/{}", RUSTFS_META_BUCKET, "multipart");
        let config = format!("{}/{}", RUSTFS_META_BUCKET, "config");
        let tmp = format!("{}/{}", RUSTFS_META_BUCKET, "tmp");

        let defaults = vec![
            buckets.as_str(),
            multipart.as_str(),
            config.as_str(),
            tmp.as_str(),
            RUSTFS_META_TMP_DELETED_BUCKET,
        ];

        self.make_volumes(defaults).await
    }

    /// Resolve a caller-visible path beneath the configured disk root.
    pub fn resolve_abs_path(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        Ok(Self::resolve_abs_path_from(&self.root, path.as_ref()))
    }

    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    fn io_resolve_abs_path(&self, path: impl AsRef<Path>) -> PathBuf {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy();

        // Fast cache read
        {
            let cache = self.path_cache.read();
            if let Some(cached_path) = cache.get(path_str.as_ref()) {
                return cached_path.clone();
            }
        }

        let normalized = Self::resolve_abs_path_from(self.io_root(), path_ref);

        // Cache the result
        {
            let mut cache = self.path_cache.write();

            // Simple cache size control
            if cache.len() >= 4096 {
                // Clear half the cache - simple eviction strategy
                let keys_to_remove: Vec<_> = cache.keys().take(cache.len() / 2).cloned().collect();
                for key in keys_to_remove {
                    cache.remove(&key);
                }
            }

            cache.insert(path_str.into_owned(), normalized.clone());
        }

        normalized
    }

    fn resolve_abs_path_from(root: &Path, path: &Path) -> PathBuf {
        let abs_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            #[cfg(windows)]
            {
                root.join(path.to_string_lossy().replace('/', "\\"))
            }
            #[cfg(not(windows))]
            {
                root.join(path)
            }
        };

        normalize_path_components(abs_path)
    }

    // Get the absolute path of an object
    pub fn get_object_path(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        local_disk_object_path(&self.root, bucket, key)
    }

    // Get the absolute path of a bucket
    pub fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        local_disk_bucket_path(&self.root, bucket)
    }

    pub(crate) fn get_object_path_for_io(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        self.io_get_object_path(bucket, key)
    }

    pub(crate) fn get_bucket_path_for_io(&self, bucket: &str) -> Result<PathBuf> {
        self.io_get_bucket_path(bucket)
    }

    fn io_get_object_path(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        self.local_disk_object_path(self.io_root(), bucket, key)
    }

    fn io_get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        self.local_disk_bucket_path(self.io_root(), bucket)
    }

    fn local_disk_object_path(&self, root: &Path, bucket: &str, key: &str) -> Result<PathBuf> {
        let (bucket_path, path) = build_local_disk_object_path(root, bucket, key);
        #[cfg(target_os = "linux")]
        {
            check_local_disk_valid_object_path_at(root, &self.mount_lease, &bucket_path, &path)?;
        }
        #[cfg(not(target_os = "linux"))]
        {
            check_local_disk_valid_object_path(root, &bucket_path, &path)?;
        }
        Ok(path)
    }

    fn local_disk_bucket_path(&self, root: &Path, bucket: &str) -> Result<PathBuf> {
        let bucket_path = build_local_disk_bucket_path(root, bucket);
        #[cfg(target_os = "linux")]
        {
            check_local_disk_valid_path_at(root, &self.mount_lease, &bucket_path)?;
        }
        #[cfg(not(target_os = "linux"))]
        {
            check_local_disk_valid_path(root, &bucket_path)?;
        }
        Ok(bucket_path)
    }

    // Check if a path is valid
    #[allow(
        dead_code,
        reason = "method wrapper over the live free function check_local_disk_valid_path; no caller in this port (backlog#1823)"
    )]
    fn check_valid_path<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            check_local_disk_valid_path_at(self.io_root(), &self.mount_lease, path)
        }
        #[cfg(not(target_os = "linux"))]
        {
            check_local_disk_valid_path(self.io_root(), path)
        }
    }

    #[allow(
        dead_code,
        reason = "method wrapper over the live free function reject_local_disk_symlink_components; no caller in this port (backlog#1823)"
    )]
    fn reject_symlink_components(&self, path: &Path) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            reject_local_disk_symlink_components_at(self.io_root(), &self.mount_lease, path)
        }
        #[cfg(not(target_os = "linux"))]
        {
            reject_local_disk_symlink_components(self.io_root(), path)
        }
    }

    // Batch path generation with single lock acquisition
    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    fn get_object_paths_batch(&self, requests: &[(String, String)]) -> Result<Vec<PathBuf>> {
        let mut results = Vec::with_capacity(requests.len());
        let mut cache_misses = Vec::new();

        // First attempt to get all paths from cache
        {
            let cache = self.path_cache.read();
            for (i, (bucket, key)) in requests.iter().enumerate() {
                let cache_key = path_join_buf(&[bucket, key]);
                if let Some(cached_path) = cache.get(&cache_key) {
                    results.push((i, cached_path.clone()));
                } else {
                    cache_misses.push((i, bucket, key, cache_key));
                }
            }
        }

        // Handle cache misses
        if !cache_misses.is_empty() {
            let mut new_entries = Vec::new();
            for (i, _bucket, _key, cache_key) in cache_misses {
                let path = self.io_resolve_abs_path(&cache_key);

                results.push((i, path.clone()));
                new_entries.push((cache_key, path));
            }

            // Batch update cache
            {
                let mut cache = self.path_cache.write();
                for (key, path) in new_entries {
                    cache.insert(key, path);
                }
            }
        }

        // Sort results back to original order
        results.sort_by_key(|(i, _)| *i);
        Ok(results.into_iter().map(|(_, path)| path).collect())
    }

    // /// Write to the filesystem atomically.
    // /// This is done by first writing to a temporary location and then moving the file.
    // pub(crate) async fn prepare_file_write<'a>(&self, path: &'a PathBuf) -> Result<FileWriter<'a>> {
    //     let tmp_path = self.io_get_object_path(RUSTFS_META_TMP_BUCKET, Uuid::new_v4().to_string().as_str())?;

    //     debug!("prepare_file_write tmp_path:{:?}, path:{:?}", &tmp_path, &path);

    //     let file = File::create(&tmp_path).await?;
    //     let writer = BufWriter::new(file);
    //     Ok(FileWriter {
    //         tmp_path,
    //         dest_path: path,
    //         writer,
    //         clean_tmp: true,
    //     })
    // }

    async fn move_to_trash(&self, delete_path: &PathBuf, recursive: bool, immediate_purge: bool) -> Result<()> {
        // if recursive {
        //     remove_all_std(delete_path).map_err(to_volume_error)?;
        // } else {
        //     remove_std(delete_path).map_err(to_file_error)?;
        // }

        // return Ok(());

        // TODO(backlog): make disk space checks and trash cleanup event-driven instead of poll-based

        let trash_path = self.io_get_object_path(RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
        // if let Some(parent) = trash_path.parent() {
        //     if !parent.exists() {
        //         fs::create_dir_all(parent).await?;
        //     }
        // }

        let err = if recursive {
            rename_all_ignore_missing_source(
                delete_path,
                trash_path,
                self.io_get_bucket_path(RUSTFS_META_TMP_DELETED_BUCKET)?,
                &self.publication_root,
            )
            .await
            .err()
        } else {
            match rename(&delete_path, &trash_path).await {
                Ok(()) => None,
                Err(err)
                    if err.kind() == ErrorKind::NotFound && os::rename_source_is_missing(delete_path, &self.publication_root) =>
                {
                    None
                }
                Err(err) => Some(to_file_error(err).into()),
            }
        };

        if immediate_purge || delete_path.to_string_lossy().ends_with(SLASH_SEPARATOR) {
            let trash_path2 = self.io_get_object_path(RUSTFS_META_TMP_DELETED_BUCKET, Uuid::new_v4().to_string().as_str())?;
            let _ = rename_all_ignore_missing_source(
                encode_dir_object(delete_path.to_string_lossy().as_ref()),
                trash_path2,
                self.io_get_bucket_path(RUSTFS_META_TMP_DELETED_BUCKET)?,
                &self.publication_root,
            )
            .await;
        }

        if let Some(err) = err {
            if err == Error::DiskFull {
                // Out of space to stage the trash rename: fall back to an in-place
                // remove and propagate any failure from that remove.
                if recursive {
                    remove_all_std(delete_path).map_err(to_volume_error)?;
                } else {
                    remove_std(delete_path).map_err(to_file_error)?;
                }

                return Ok(());
            }

            // Missing sources are folded into `None` above. Any remaining error is
            // a real failure, including a missing destination base.
            return Err(err);
        }

        Ok(())
    }

    async fn delete_unleased(&self, volume: &str, path: &str, opt: &DeleteOptions) -> Result<()> {
        let volume_dir = self.io_get_bucket_path(volume)?;
        if !skip_access_checks(volume)
            && let Err(e) = access(&volume_dir).await
        {
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        let file_path = self.io_get_object_path(volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;
        self.delete_file(&volume_dir, &file_path, opt.recursive, opt.immediate)
            .await?;
        // A deleted shard must not remain readable through the io_uring fd cache.
        self.io_backend.invalidate_cached_fds_under(volume, path);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    #[async_recursion::async_recursion]
    async fn delete_file(
        &self,
        base_path: &PathBuf,
        delete_path: &PathBuf,
        recursive: bool,
        immediate_purge: bool,
    ) -> Result<()> {
        // debug!("delete_file {:?}\n base_path:{:?}", &delete_path, &base_path);

        if is_root_path(base_path) || is_root_path(delete_path) {
            // debug!("delete_file skip {:?}", &delete_path);
            return Ok(());
        }

        if !delete_path.starts_with(base_path) || base_path == delete_path {
            // debug!("delete_file skip {:?}", &delete_path);
            return Ok(());
        }

        if recursive {
            self.move_to_trash(delete_path, recursive, immediate_purge).await?;
        } else if delete_path.is_dir() {
            // debug!("delete_file remove_dir {:?}", &delete_path);
            if let Err(err) = fs::remove_dir(&delete_path).await {
                // debug!("remove_dir err {:?} when {:?}", &err, &delete_path);
                // A missing or still-populated directory is benign here; see
                // is_benign_object_rmdir_error (handles the illumos/Solaris EEXIST
                // convention, rustfs/rustfs#4978).
                if !is_benign_object_rmdir_error(&err) {
                    warn!(
                        event = EVENT_DISK_LOCAL_DELETE_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = ?delete_path,
                        operation = "remove_dir",
                        error_kind = %err.kind(),
                        "Disk local delete failed"
                    );
                    return Err(Error::other(FileAccessDeniedWithContext {
                        path: delete_path.clone(),
                        source: err,
                    }));
                }
            }
            // debug!("delete_file remove_dir done {:?}", &delete_path);
        } else if let Err(err) = fs::remove_file(&delete_path).await {
            // debug!("remove_file err {:?} when {:?}", &err, &delete_path);
            match err.kind() {
                ErrorKind::NotFound => (),
                _ => {
                    warn!(
                        event = EVENT_DISK_LOCAL_DELETE_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = ?delete_path,
                        operation = "remove_file",
                        error = ?err,
                        "Disk local delete failed"
                    );
                    return Err(Error::other(FileAccessDeniedWithContext {
                        path: delete_path.clone(),
                        source: err,
                    }));
                }
            }
        }

        if let Some(dir_path) = delete_path.parent() {
            Box::pin(self.delete_file(base_path, &PathBuf::from(dir_path), false, false)).await?;
        }

        // debug!("delete_file done {:?}", &delete_path);
        Ok(())
    }

    /// read xl.meta raw data
    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_raw(
        &self,
        bucket: &str,
        volume_dir: impl AsRef<Path>,
        file_path: impl AsRef<Path>,
        read_data: bool,
    ) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        if file_path.as_ref().as_os_str().is_empty() {
            return Err(DiskError::FileNotFound);
        }

        let meta_path = path_join(&[file_path.as_ref(), Path::new(STORAGE_FORMAT_FILE)]);

        let res = {
            if read_data {
                self.read_all_data_with_dmtime(bucket, volume_dir, meta_path).await
            } else {
                match self.read_metadata_with_dmtime(meta_path).await {
                    Ok(res) => Ok(res),
                    Err(err) => {
                        if err == Error::FileNotFound
                            && !skip_access_checks(volume_dir.as_ref().to_string_lossy().to_string().as_str())
                            && let Err(e) = access(volume_dir.as_ref()).await
                            && e.kind() == ErrorKind::NotFound
                        {
                            // warn!("read_metadata_with_dmtime os err {:?}", &aerr);
                            return Err(DiskError::VolumeNotFound);
                        }

                        Err(err)
                    }
                }
            }
        };

        let (buf, mtime) = res?;
        if buf.is_empty() {
            return Err(DiskError::FileNotFound);
        }

        Ok((buf, mtime))
    }

    #[hotpath::measure(impl_type = "LocalDisk")]
    async fn read_metadata_with_dmtime(&self, file_path: impl AsRef<Path>) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        check_path_length(file_path.as_ref().to_string_lossy().as_ref())?;

        // HP-12 item 1 (sub-change A): fold the open + fstat + bounded xl.meta
        // read into a single spawn_blocking dispatch instead of three separate
        // async fs hops. The closure mirrors the previous async body one-to-one,
        // including every error mapping, so the returned Result stays
        // byte-for-byte equivalent (see read_xl_meta_no_data_sync equivalence
        // tests in rustfs-filemeta):
        //  - open failure     -> to_file_error (was fs::open_file(..).map_err(to_file_error))
        //  - is_dir           -> Error::FileNotFound (NOT to_file_error(EISDIR))
        //  - metadata failure -> to_file_error
        //  - parse failure    -> propagated verbatim from read_xl_meta_no_data_sync (`?`)
        let path = file_path.as_ref().to_path_buf();
        let (data, modtime) = tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
            // Read-only open, equivalent to O_RDONLY (get_readonly_options only sets read(true)).
            let mut f = std::fs::File::open(&path).map_err(to_file_error)?;

            let meta = f.metadata().map_err(to_file_error)?;

            if meta.is_dir() {
                // fix use io::Error
                return Err(Error::FileNotFound);
            }

            let size = meta.len() as usize;

            let data = read_xl_meta_no_data_sync(&mut f, size)?;

            let modtime = match meta.modified() {
                Ok(md) => Some(OffsetDateTime::from(md)),
                Err(_) => None,
            };

            Ok((data, modtime))
        })
        .await
        .map_err(DiskError::from)??;

        Ok((data, modtime))
    }

    #[hotpath::measure(impl_type = "LocalDisk")]
    async fn read_all_data(&self, volume: &str, volume_dir: impl AsRef<Path>, file_path: impl AsRef<Path>) -> Result<Vec<u8>> {
        // TODO(backlog): add configurable timeout for read_all_data operations
        let (data, _) = self.read_all_data_with_dmtime(volume, volume_dir, file_path).await?;
        Ok(data)
    }

    async fn read_listing_metadata(&self, volume: &str, object_name: &str) -> Result<ListingMetadataRead> {
        let object_dir = self.io_get_object_path(volume, object_name)?;
        let metadata_path = object_dir.join(STORAGE_FORMAT_FILE);
        let volume_dir = self.io_get_bucket_path(volume)?;
        let result = tokio::task::spawn_blocking(move || {
            let (bytes, _) = read_all_data_std(&metadata_path)?;
            let file_meta = FileMeta::load(&bytes).ok();
            let data_dirs: HashSet<String> = file_meta
                .as_ref()
                .and_then(|meta| meta.get_data_dirs().ok())
                .into_iter()
                .flatten()
                .flatten()
                .map(|data_dir| data_dir.to_string())
                .collect();
            let probe = match os::read_dir_probe(&object_dir, data_dirs.len().saturating_add(2)) {
                Ok(probe) => probe,
                Err(err) if err.kind() == ErrorKind::NotFound => os::ReadDirProbe {
                    entries: Vec::new(),
                    complete: true,
                },
                Err(err) => return Err(ReadAllError::Disk(to_file_error(err).into())),
            };
            let found_child = probe.entries.into_iter().any(|entry| {
                entry
                    .strip_suffix(SLASH_SEPARATOR)
                    .is_some_and(|child| !child.is_empty() && !data_dirs.contains(child))
            });
            let has_namespace_child_candidate = found_child || !probe.complete;

            Ok(ListingMetadataRead {
                bytes,
                file_meta,
                data_dirs,
                has_namespace_child_candidate,
            })
        })
        .await
        .map_err(DiskError::from)?;

        self.resolve_read_all_result(volume, &volume_dir, result).await
    }

    async fn resolve_read_all_result<T>(
        &self,
        volume: &str,
        volume_dir: &Path,
        result: core::result::Result<T, ReadAllError>,
    ) -> Result<T> {
        match result {
            Ok(value) => Ok(value),
            Err(ReadAllError::Disk(err)) => Err(err),
            Err(ReadAllError::Open(err)) => {
                if err.kind() == ErrorKind::NotFound
                    && !skip_access_checks(volume)
                    && let Err(access_err) = access(volume_dir).await
                    && access_err.kind() == ErrorKind::NotFound
                {
                    warn!(
                        event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        reason = "read_all_data_with_dmtime_volume_not_found",
                        error = ?access_err,
                        "Disk local read fallback failed"
                    );
                    return Err(DiskError::VolumeNotFound);
                }

                Err(to_file_error(err).into())
            }
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_all_data_with_dmtime(
        &self,
        volume: &str,
        volume_dir: impl AsRef<Path>,
        file_path: impl AsRef<Path>,
    ) -> Result<(Vec<u8>, Option<OffsetDateTime>)> {
        // HP-12 item 1 (sub-change A): fold open + fstat + is_dir + try_reserve +
        // read_to_end into a single spawn_blocking dispatch. The closure mirrors
        // the previous async body one-to-one so the returned Result stays
        // byte-for-byte equivalent. Post-open errors are mapped to their final
        // DiskError inside the closure (metadata/read -> to_file_error; is_dir ->
        // FileNotFound; try_reserve -> Error::other) exactly as before. The raw
        // open error is carried out unmapped so the async side can preserve the
        // original NotFound -> access(volume_dir) -> VolumeNotFound fallback.
        //
        // Only the open() call can yield ErrorKind::NotFound here: once open
        // succeeds the fd is valid, so fstat/read never return ENOENT. Hence
        // gating the volume fallback on the open error alone is equivalent to
        // the original code, where the fallback lived solely in the open match arm.
        let path = file_path.as_ref().to_path_buf();
        let res = tokio::task::spawn_blocking(move || read_all_data_std(&path))
            .await
            .map_err(DiskError::from)?;

        let (bytes, modtime) = self.resolve_read_all_result(volume, volume_dir.as_ref(), res).await?;

        Ok((bytes, modtime))
    }

    async fn write_missing_delete_marker(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        object_dir: &Path,
        xl_path: &Path,
        rollback_dir: Option<Uuid>,
    ) -> Result<()> {
        if let Some(rollback_dir) = rollback_dir {
            let rollback_path = object_dir.join(rollback_dir.to_string());
            fs::create_dir_all(&rollback_path).await.map_err(to_file_error)?;
            fs::write(rollback_path.join(DELETE_MARKER_ROLLBACK_FILE), [])
                .await
                .map_err(to_file_error)?;
        }
        if let Err(err) = self.write_metadata("", volume, path, fi).await {
            if let Some(rollback_dir) = rollback_dir
                && let Err(restore_err) = restore_delete_rollback(object_dir, xl_path, rollback_dir, &self.publication_root).await
            {
                warn!(
                    event = EVENT_DISK_LOCAL_DELETE_ROLLBACK_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    result = "failed",
                    volume,
                    path,
                    rollback_dir = %rollback_dir,
                    error = ?restore_err,
                    "Disk local delete rollback failed"
                );
            }
            return Err(err);
        }
        Ok(())
    }

    async fn delete_versions_internal(&self, volume: &str, path: &str, fis: &[FileInfo], opts: &DeleteOptions) -> Result<()> {
        let volume_dir = self.io_get_bucket_path(volume)?;
        let xlpath = self.io_get_object_path(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str())?;
        let object_dir = xlpath
            .parent()
            .ok_or_else(|| DiskError::other("missing object metadata parent"))?;

        if let Some(rollback_dir) = opts.old_data_dir
            && opts.undo_write
        {
            if opts.undo_delete {
                return restore_delete_rollback(object_dir, &xlpath, rollback_dir, &self.publication_root).await;
            }

            return restore_metadata_backup(object_dir, &xlpath, rollback_dir, &self.publication_root).await;
        }

        let (data, _) = match self.read_all_data_with_dmtime(volume, volume_dir.as_path(), &xlpath).await {
            Ok(data) => data,
            Err(DiskError::FileNotFound) => {
                // `deleted` alone can be an explicit marker purge; only
                // `mark_deleted` may create metadata that was not present.
                let Some(delete_marker) = fis.iter().find(|fi| fi.deleted && fi.mark_deleted).cloned() else {
                    return Err(DiskError::FileNotFound);
                };
                return self
                    .write_missing_delete_marker(volume, path, delete_marker, object_dir, &xlpath, opts.old_data_dir)
                    .await;
            }
            Err(err) => return Err(err),
        };

        if data.is_empty() {
            return Err(DiskError::FileNotFound);
        }

        let mut fm = FileMeta::default();

        fm.unmarshal_msg(&data)?;
        let rollback_dir = opts.old_data_dir;
        let mut reserved_version_delete = false;
        if let Some(rollback_dir) = rollback_dir {
            write_metadata_rollback_backup(object_dir, rollback_dir, &data).await?;
        }

        for fi in fis.iter() {
            let data_dir = match fm.delete_version(fi) {
                Ok(res) => res,
                Err(err) => {
                    let err: DiskError = err.into();
                    if !fi.deleted && (err == DiskError::FileNotFound || err == DiskError::FileVersionNotFound) {
                        continue;
                    }

                    if reserved_version_delete && let Some(rollback_dir) = rollback_dir {
                        return Err(self
                            .abort_reserved_version_delete(
                                object_dir,
                                rollback_dir,
                                volume,
                                path,
                                "delete_versions_metadata_update",
                                err,
                            )
                            .await);
                    }
                    return Err(restore_delete_rollback_after_error(
                        object_dir,
                        &xlpath,
                        rollback_dir,
                        volume,
                        path,
                        DeleteRollbackFailure {
                            stage: "delete_versions_metadata_update",
                            error: err,
                        },
                        &self.publication_root,
                    )
                    .await);
                }
            };

            if let Some(dir) = data_dir {
                let vid = fi.version_id.unwrap_or_default();
                let _ = fm.data.remove(vec![vid, dir]);

                let dir_path = match self.io_get_object_path(volume, format!("{path}/{dir}").as_str()) {
                    Ok(dir_path) => dir_path,
                    Err(err) => {
                        if reserved_version_delete && let Some(rollback_dir) = rollback_dir {
                            return Err(self
                                .abort_reserved_version_delete(
                                    object_dir,
                                    rollback_dir,
                                    volume,
                                    path,
                                    "delete_versions_data_path",
                                    err,
                                )
                                .await);
                        }
                        return Err(restore_delete_rollback_after_error(
                            object_dir,
                            &xlpath,
                            rollback_dir,
                            volume,
                            path,
                            DeleteRollbackFailure {
                                stage: "delete_versions_data_path",
                                error: err,
                            },
                            &self.publication_root,
                        )
                        .await);
                    }
                };
                if let Some(rollback_dir) = rollback_dir {
                    let rollback_path = object_dir.join(rollback_dir.to_string());
                    if let Err(err) = fs::create_dir_all(&rollback_path).await {
                        let err: DiskError = to_file_error(err).into();
                        if reserved_version_delete {
                            return Err(self
                                .abort_reserved_version_delete(
                                    object_dir,
                                    rollback_dir,
                                    volume,
                                    path,
                                    "delete_versions_rollback_dir",
                                    err,
                                )
                                .await);
                        }
                        return Err(restore_delete_rollback_after_error(
                            object_dir,
                            &xlpath,
                            Some(rollback_dir),
                            volume,
                            path,
                            DeleteRollbackFailure {
                                stage: "delete_versions_rollback_dir",
                                error: err,
                            },
                            &self.publication_root,
                        )
                        .await);
                    }
                    let reserved = match self.reserve_version_delete(volume, path, dir, rollback_dir).await {
                        Ok(reserved) => reserved,
                        Err(err) => {
                            return Err(self
                                .abort_reserved_version_delete(
                                    object_dir,
                                    rollback_dir,
                                    volume,
                                    path,
                                    "delete_versions_reserve_data",
                                    err,
                                )
                                .await);
                        }
                    };
                    reserved_version_delete |= reserved;
                    let rollback_data_path = rollback_path.join(dir.to_string());
                    if !reserved
                        && let Err(err) = rename_all_ignore_missing_source(
                            &dir_path,
                            &rollback_data_path,
                            &rollback_path,
                            &self.publication_root,
                        )
                        .await
                    {
                        return Err(restore_delete_rollback_after_error(
                            object_dir,
                            &xlpath,
                            Some(rollback_dir),
                            volume,
                            path,
                            DeleteRollbackFailure {
                                stage: "delete_versions_stage_data",
                                error: err,
                            },
                            &self.publication_root,
                        )
                        .await);
                    }
                    if should_fail_after_delete_data_staged(path) {
                        if reserved_version_delete {
                            return Err(self
                                .abort_reserved_version_delete(
                                    object_dir,
                                    rollback_dir,
                                    volume,
                                    path,
                                    "delete_versions_test_after_stage",
                                    DiskError::Unexpected,
                                )
                                .await);
                        }
                        return Err(restore_delete_rollback_after_error(
                            object_dir,
                            &xlpath,
                            Some(rollback_dir),
                            volume,
                            path,
                            DeleteRollbackFailure {
                                stage: "delete_versions_test_after_stage",
                                error: DiskError::Unexpected,
                            },
                            &self.publication_root,
                        )
                        .await);
                    }
                } else if let Err(err) = self.move_to_trash(&dir_path, true, false).await
                    && !(err == DiskError::FileNotFound || err == DiskError::VolumeNotFound)
                {
                    return Err(err);
                };

                // The version's data dir was staged or trashed; drop any cached
                // io_uring descriptors under it so a deleted `part.N` cannot keep
                // answering reads (rustfs/backlog#1175).
                self.io_backend.invalidate_cached_fds_under(volume, &format!("{path}/{dir}"));
            }
        }

        // Remove xl.meta when no versions remain
        if fm.versions.is_empty() {
            if let Err(err) = self.delete_file(&volume_dir, &xlpath, true, false).await {
                if reserved_version_delete && let Some(rollback_dir) = rollback_dir {
                    return Err(self
                        .abort_reserved_version_delete(
                            object_dir,
                            rollback_dir,
                            volume,
                            path,
                            "delete_versions_commit_delete",
                            err,
                        )
                        .await);
                }
                return Err(restore_delete_rollback_after_error(
                    object_dir,
                    &xlpath,
                    rollback_dir,
                    volume,
                    path,
                    DeleteRollbackFailure {
                        stage: "delete_versions_commit_delete",
                        error: err,
                    },
                    &self.publication_root,
                )
                .await);
            }
            if reserved_version_delete
                && let Some(rollback_dir) = rollback_dir
                && let Err(err) = self.commit_reserved_version_delete(volume, path, rollback_dir).await
            {
                return Err(self
                    .abort_reserved_version_delete(object_dir, rollback_dir, volume, path, "delete_versions_commit_intent", err)
                    .await);
            }
            return Ok(());
        }

        // Update xl.meta atomically: a concurrent reader or crash mid-write must
        // never observe a truncated xl.meta for versions that were not deleted.
        let buf = match fm.marshal_msg() {
            Ok(buf) => buf,
            Err(err) => {
                let err: DiskError = err.into();
                if reserved_version_delete && let Some(rollback_dir) = rollback_dir {
                    return Err(self
                        .abort_reserved_version_delete(
                            object_dir,
                            rollback_dir,
                            volume,
                            path,
                            "delete_versions_metadata_encode",
                            err,
                        )
                        .await);
                }
                return Err(restore_delete_rollback_after_error(
                    object_dir,
                    &xlpath,
                    rollback_dir,
                    volume,
                    path,
                    DeleteRollbackFailure {
                        stage: "delete_versions_metadata_encode",
                        error: err,
                    },
                    &self.publication_root,
                )
                .await);
            }
        };

        if let Err(err) = self
            .write_all_meta(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str(), &buf, true)
            .await
        {
            if reserved_version_delete && let Some(rollback_dir) = rollback_dir {
                return Err(self
                    .abort_reserved_version_delete(object_dir, rollback_dir, volume, path, "delete_versions_commit_write", err)
                    .await);
            }
            return Err(restore_delete_rollback_after_error(
                object_dir,
                &xlpath,
                rollback_dir,
                volume,
                path,
                DeleteRollbackFailure {
                    stage: "delete_versions_commit_write",
                    error: err,
                },
                &self.publication_root,
            )
            .await);
        }

        if reserved_version_delete
            && let Some(rollback_dir) = rollback_dir
            && let Err(err) = self.commit_reserved_version_delete(volume, path, rollback_dir).await
        {
            return Err(self
                .abort_reserved_version_delete(object_dir, rollback_dir, volume, path, "delete_versions_commit_intent", err)
                .await);
        }

        Ok(())
    }

    async fn write_all_meta(&self, volume: &str, path: &str, buf: &[u8], sync: bool) -> Result<()> {
        let volume_dir = self.io_get_bucket_path(volume)?;
        let file_path = self.io_get_object_path(volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;

        let tmp_volume_dir = self.io_get_bucket_path(RUSTFS_META_TMP_BUCKET)?;
        let tmp_file_path = self.io_get_object_path(RUSTFS_META_TMP_BUCKET, Uuid::new_v4().to_string().as_str())?;

        let durability = effective_durability(volume);

        // The tmp file is renamed to its final location right below, so only
        // its contents must be durable here (SyncMode::FileOnly): the rename
        // drops the tmp directory entry, and the destination parent directory
        // is fsynced after the rename. Both are metadata commits, so relaxed
        // tiers skip them.
        let tmp_sync = if sync && durability.syncs_commit_metadata() {
            SyncMode::FileOnly
        } else {
            SyncMode::None
        };
        self.write_all_internal(&tmp_file_path, InternalBuf::Ref(buf), tmp_sync, &tmp_volume_dir)
            .await?;

        // Crash-consistency injection: hard power loss after the replacement
        // xl.meta is staged in the tmp bucket but before the atomic rename that
        // publishes it. The destination xl.meta is untouched, so a crash here
        // must leave the object's metadata byte-for-byte the old version
        // (rustfs/backlog#864); the staged tmp file is a harmless orphan swept by
        // tmp-bucket GC. Compiles to a no-op outside `#[cfg(test)]`.
        if crash_inject::should_crash_at(CrashPoint::MetaWriteAfterTmpBeforeRename, path) {
            return Err(DiskError::Unexpected);
        }

        rename_all(tmp_file_path, &file_path, volume_dir, &self.publication_root).await?;

        if sync
            && durability.syncs_commit_metadata()
            && let Some(parent) = file_path.parent()
        {
            os::fsync_dir(parent).await.map_err(to_file_error)?;
        }

        Ok(())
    }

    // write_all_public for trail
    async fn write_all_public(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        if volume == RUSTFS_META_BUCKET && path == super::FORMAT_CONFIG_FILE {
            let mut format_info = self.format_info.write().await;
            format_info.data.clone_from(&data);
        }

        let volume_dir = self.io_get_bucket_path(volume)?;

        // Files written here (format.json, ...) stay where they land — no
        // rename follows — so the new directory entry must be fsynced too.
        // System-critical volumes are pinned to strict by effective_durability;
        // only the legacy full-off switch (historical semantics) skips this.
        let sync = if effective_durability(volume).syncs_commit_metadata() {
            SyncMode::FileAndDir
        } else {
            SyncMode::None
        };
        self.write_all_private(volume, path, data, sync, &volume_dir).await?;

        Ok(())
    }

    // write_all_private with check_path_length
    #[tracing::instrument(level = "trace", skip_all)]
    async fn write_all_private(&self, volume: &str, path: &str, buf: Bytes, sync: SyncMode, skip_parent: &Path) -> Result<()> {
        let file_path = self.io_get_object_path(volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;

        self.write_all_internal(&file_path, InternalBuf::Owned(buf), sync, skip_parent)
            .await?;

        Ok(())
    }
    // write_all_internal do write file.
    // Executes the given SyncMode verbatim: durability policy (tier gating,
    // system-critical pinning) is resolved by callers via effective_durability.
    async fn write_all_internal(
        &self,
        file_path: &Path,
        data: InternalBuf<'_>,
        sync: SyncMode,
        skip_parent: &Path,
    ) -> Result<()> {
        let skip_parent = if skip_parent.as_os_str().is_empty() {
            self.io_root()
        } else {
            skip_parent
        };

        match data {
            InternalBuf::Ref(buf) => {
                let mut f = self.open_file(file_path, O_CREATE | O_WRONLY | O_TRUNC, skip_parent).await?;
                f.write_all(buf).await.map_err(to_file_error)?;
                if sync != SyncMode::None {
                    f.sync_data().await.map_err(to_file_error)?;
                    // Persist the directory entry too, so a freshly created file
                    // survives power loss along with its contents. Skipped for
                    // FileOnly: the caller renames the file away immediately.
                    if sync == SyncMode::FileAndDir
                        && let Some(parent) = file_path.parent()
                    {
                        os::fsync_dir(parent).await.map_err(to_file_error)?;
                    }
                }
            }
            InternalBuf::Owned(buf) => {
                let path = file_path.to_path_buf();
                if let Some(parent) = path.parent()
                    && parent != skip_parent
                {
                    os::make_dir_all(parent, skip_parent).await?;
                }

                tokio::task::spawn_blocking(move || {
                    #[cfg(test)]
                    run_owned_file_write_before_open(&path);

                    let mut file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&path)
                        .map_err(to_file_error)?;
                    std::io::Write::write_all(&mut file, buf.as_ref()).map_err(to_file_error)?;
                    if sync != SyncMode::None {
                        file.sync_data().map_err(to_file_error)?;
                        // FileOnly callers rename the file away, so the tmp
                        // directory entry never needs to become durable.
                        if sync == SyncMode::FileAndDir
                            && let Some(parent) = path.parent()
                        {
                            os::fsync_dir_std(parent).map_err(to_file_error)?;
                        }
                    }
                    Ok::<_, std::io::Error>(())
                })
                .await
                .map_err(DiskError::from)??;
            }
        }

        Ok(())
    }

    async fn open_file(&self, path: impl AsRef<Path>, mode: usize, skip_parent: impl AsRef<Path>) -> Result<File> {
        let mut skip_parent = skip_parent.as_ref();
        if skip_parent.as_os_str().is_empty() {
            skip_parent = self.io_root();
        }

        if let Some(parent) = path.as_ref().parent()
            && parent != skip_parent
        {
            os::make_dir_all(parent, skip_parent).await?;
        }

        let f = super::fs::open_file(path.as_ref(), mode).await.map_err(to_file_error)?;

        Ok(f)
    }

    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    async fn open_file_read_only(&self, path: impl AsRef<Path>) -> Result<File> {
        let f = super::fs::open_file(path.as_ref(), O_RDONLY).await.map_err(to_file_error)?;
        Ok(f)
    }

    #[allow(dead_code, reason = "MinIO-parity surface with no caller in this port (backlog#1823)")]
    fn get_metrics(&self) -> DiskMetrics {
        DiskMetrics::default()
    }

    async fn bitrot_verify(&self, part_path: &PathBuf, part_size: usize, algo: HashAlgorithm, shard_size: usize) -> Result<()> {
        let retry_count = bitrot_size_mismatch_retry_count();
        let retry_delay = bitrot_size_mismatch_retry_delay();

        for attempt in 0..=retry_count {
            let file = super::fs::open_file(part_path, O_RDONLY).await.map_err(to_file_error)?;
            let meta = file.metadata().await.map_err(to_file_error)?;
            let file_size = meta.len() as usize;

            match bitrot_verify(Box::new(file), file_size, part_size, algo.clone(), shard_size).await {
                Ok(()) => return Ok(()),
                Err(err) if attempt < retry_count && is_bitrot_size_mismatch_error(&err) => {
                    info!(
                        event = EVENT_DISK_LOCAL_CHECK_PARTS,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = %part_path.display(),
                        expected_size = part_size,
                        actual_size = file_size,
                        retry_attempt = attempt + 1,
                        retry_count,
                        retry_delay_ms = retry_delay.as_millis(),
                        state = "bitrot_retry",
                        "Disk local check_parts state changed"
                    );
                    tokio::time::sleep(retry_delay).await;
                }
                Err(err) if is_bitrot_verification_error(&err) => return Err(DiskError::FileCorrupt),
                Err(err) => return Err(to_file_error(err).into()),
            }
        }

        Err(DiskError::FileCorrupt)
    }

    #[async_recursion::async_recursion]
    #[allow(clippy::too_many_arguments)]
    async fn scan_dir<W>(
        &self,
        mut current: String,
        mut prefix: String,
        opts: &WalkDirOptions,
        out: &mut MetacacheWriter<W>,
        objs_returned: &mut i32,
        skip_current_dir_object: bool,
        multipart_dir_to_skip: Option<HashSet<String>>,
    ) -> Result<()>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let forward = {
            opts.forward_to
                .as_ref()
                .and_then(|v| v.strip_prefix(&current))
                .map(|forward| {
                    if let Some(idx) = forward.find('/') {
                        forward[..idx].to_owned()
                    } else {
                        forward.to_owned()
                    }
                })
        };

        if opts.limit > 0 && *objs_returned >= opts.limit {
            return Ok(());
        }

        // TODO(backlog): add directory listing lock to prevent concurrent enumeration

        let stall = opts.stall_timeout_duration();

        // Keep the existing in-memory sort contract, but bound each directory-entry
        // read rather than treating the whole enumeration as one stalled disk
        // operation. Object listing keeps using per-entry stall deadlines through
        // `read_dir_entries_with_walk_stall` so wide prefixes can still be handled
        // as a single logical read in API semantics.
        let read_dir_started = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let dir_path_abs = self.io_get_object_path(&opts.bucket, current.trim_start_matches(SLASH_SEPARATOR))?;
        let read_dir_result = match read_dir_entries_with_walk_stall(&dir_path_abs, -1, stall).await {
            Err(err) if err == Error::FileNotFound && !skip_access_checks(&opts.bucket) => {
                let volume_dir = self.io_get_bucket_path(&opts.bucket)?;
                if let Err(access_err) = access(&volume_dir).await {
                    Err(to_access_error(access_err, DiskError::VolumeAccessDenied).into())
                } else {
                    Err(err)
                }
            }
            result => result,
        };
        if let Some(started) = read_dir_started {
            rustfs_io_metrics::record_list_objects_local_read_dir(rustfs_io_metrics::ListObjectsLocalReadDirObservation {
                outcome: if read_dir_result.is_ok() {
                    rustfs_io_metrics::LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_OK
                } else {
                    rustfs_io_metrics::LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_ERROR
                },
                requested_count: -1,
                returned_entries: read_dir_result.as_ref().map_or(0, Vec::len),
                duration_ms: started.elapsed().as_secs_f64() * 1000.0,
                is_root: current.trim_matches('/').is_empty(),
                has_filter_prefix: !prefix.is_empty(),
                has_forward: forward.is_some(),
            });
        }

        let mut entries = match read_dir_result {
            Ok(res) => res,
            Err(e) => {
                if e != DiskError::VolumeNotFound && e != Error::FileNotFound {
                    error!(
                        event = EVENT_DISK_LOCAL_SCAN_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = %current,
                        operation = "list_dir",
                        error = ?e,
                        "Disk local scan failed"
                    );
                    return Err(e);
                }

                if opts.report_notfound && e == Error::FileNotFound && current == opts.base_dir {
                    return Err(DiskError::FileNotFound);
                }

                return Ok(());
            }
        };

        if entries.is_empty() {
            return Ok(());
        }

        current = current.trim_matches('/').to_owned();

        let bucket = opts.bucket.as_str();

        let mut dir_objes = HashSet::new();

        // First-level filtering
        for item in entries.iter_mut() {
            let entry = item.clone();
            // check limit
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }
            // check multipart dir
            if skip_current_dir_object
                && let Some(ref dir_to_skip) = multipart_dir_to_skip
                && dir_to_skip.contains(entry.trim_end_matches(SLASH_SEPARATOR))
            {
                *item = "".to_owned();
                continue;
            }
            // check prefix
            if !prefix.is_empty() && !entry.starts_with(prefix.as_str()) {
                *item = "".to_owned();
                continue;
            }

            if let Some(forward) = &forward
                && &entry < forward
            {
                *item = "".to_owned();
                continue;
            }

            if entry.ends_with(SLASH_SEPARATOR) {
                if entry.ends_with(GLOBAL_DIR_SUFFIX_WITH_SLASH) {
                    let entry = format!("{}{}", entry.as_str().trim_end_matches(GLOBAL_DIR_SUFFIX_WITH_SLASH), SLASH_SEPARATOR);
                    dir_objes.insert(entry.clone());
                    *item = entry;
                    continue;
                }

                *item = entry.trim_end_matches(SLASH_SEPARATOR).to_owned();
                continue;
            }

            *item = "".to_owned();

            if entry.ends_with(STORAGE_FORMAT_FILE) {
                if skip_current_dir_object {
                    continue;
                }

                let metadata =
                    with_walk_stall_timeout(stall, self.read_metadata(bucket, format!("{}/{}", current, entry).as_str())).await?;

                let entry = entry.strip_suffix(STORAGE_FORMAT_FILE).unwrap_or_default().to_owned();
                let name = entry.trim_end_matches(SLASH_SEPARATOR);
                let name = decode_dir_object(format!("{}/{}", current, name).as_str());

                if opts.limit <= 0 || metadata_counts_toward_limit(&metadata) {
                    *objs_returned += 1;
                }

                write_metacache_obj(
                    out,
                    &MetaCacheEntry {
                        name: name.clone(),
                        metadata: metadata.to_vec(),
                        ..Default::default()
                    },
                )
                .await?;

                continue;
            }
        }

        entries.sort();

        if let Some(forward) = &forward {
            for (i, entry) in entries.iter().enumerate() {
                if entry >= forward || forward.starts_with(entry.as_str()) {
                    entries.drain(..i);
                    break;
                }
            }
        }

        let mut dir_stack: Vec<(String, bool, Option<HashSet<String>>, bool)> = Vec::with_capacity(5);
        // Explicit directory markers and real directories can resolve to the same logical path.
        let schedule_dir = |dir_stack: &mut Vec<(String, bool, Option<HashSet<String>>, bool)>,
                            dir_name: String,
                            skip_object: bool,
                            dir_to_skip: Option<HashSet<String>>,
                            scan_required: bool| {
            if let Some((last_dir_name, existing_skip_object, existing_dir_to_skip, existing_scan_required)) =
                dir_stack.last_mut()
                && *last_dir_name == dir_name
            {
                *existing_skip_object |= skip_object;
                *existing_scan_required |= scan_required;
                if let Some(existing_dir_to_skip) = existing_dir_to_skip {
                    if let Some(new_dir_to_skip) = &dir_to_skip {
                        existing_dir_to_skip.extend(new_dir_to_skip.iter().cloned());
                    }
                } else {
                    *existing_dir_to_skip = dir_to_skip;
                }
            } else {
                dir_stack.push((dir_name, skip_object, dir_to_skip, scan_required));
            }
        };
        prefix = "".to_owned();

        for entry in entries.iter() {
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }

            if entry.is_empty() {
                continue;
            }

            let name = path_join_buf(&[current.as_str(), entry.as_str()]);

            while let Some((last_name, _, _, _)) = dir_stack.last()
                && *last_name < name
            {
                let (pop, skip_object, dir_to_skip, scan_required) = dir_stack.pop().expect("operation should succeed");
                write_metacache_obj(
                    out,
                    &MetaCacheEntry {
                        name: pop.clone(),
                        ..Default::default()
                    },
                )
                .await?;

                let scan_path = pop.clone();
                if opts.recursive
                    && scan_required
                    && let Err(er) =
                        Box::pin(self.scan_dir(pop, prefix.clone(), opts, out, objs_returned, skip_object, dir_to_skip)).await
                {
                    if !er.is_metacache_output_stream_closed() {
                        error!(
                            event = EVENT_DISK_LOCAL_SCAN_FAILED,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                            path = %scan_path,
                            operation = "scan_dir",
                            error = ?er,
                            "Disk local scan failed"
                        );
                    }
                    return Err(er);
                }
            }

            let mut meta = MetaCacheEntry {
                name,
                ..Default::default()
            };

            let mut is_dir_obj = false;

            if let Some(_dir) = dir_objes.get(entry) {
                is_dir_obj = true;
                meta.name
                    .truncate(meta.name.len() - meta.name.chars().last().expect("operation should succeed").len_utf8());
                meta.name.push_str(GLOBAL_DIR_SUFFIX_WITH_SLASH);
            }

            let fname = format!("{}/{}", meta.name, STORAGE_FORMAT_FILE);
            let metadata_read = if opts.recursive && !is_dir_obj {
                with_walk_stall_timeout(stall, self.read_listing_metadata(&opts.bucket, &meta.name))
                    .await
                    .map(|read| {
                        (
                            Bytes::from(read.bytes),
                            read.file_meta,
                            Some(read.data_dirs),
                            read.has_namespace_child_candidate,
                        )
                    })
            } else {
                with_walk_stall_timeout(stall, self.read_metadata(&opts.bucket, fname.as_str()))
                    .await
                    .map(|metadata| (metadata, None, None, true))
            };

            match metadata_read {
                Ok((res, prefetched_file_meta, prefetched_data_dirs, has_namespace_child_candidate)) => {
                    if is_dir_obj {
                        meta.name = meta.name.trim_end_matches(GLOBAL_DIR_SUFFIX_WITH_SLASH).to_owned();
                        meta.name.push_str(SLASH_SEPARATOR);
                    }

                    meta.metadata = res.to_vec();

                    write_metacache_obj(out, &meta).await?;

                    let file_meta = match prefetched_file_meta {
                        Some(file_meta) => Some(file_meta),
                        None if opts.limit > 0 || opts.recursive || !is_dir_obj => FileMeta::load(&res).ok(),
                        None => None,
                    };

                    if opts.limit <= 0 || file_meta.as_ref().is_none_or(file_meta_counts_toward_limit) {
                        *objs_returned += 1;
                    }

                    let dir_to_skip = if let Some(data_dirs) = prefetched_data_dirs {
                        data_dirs
                    } else {
                        let mut data_dirs_to_skip = HashSet::new();
                        if let Some(file_meta) = file_meta.as_ref()
                            && let Ok(data_dirs) = file_meta.get_data_dirs()
                        {
                            for data_dir in data_dirs.iter().flatten() {
                                data_dirs_to_skip.insert(data_dir.to_string());
                            }
                        }
                        data_dirs_to_skip
                    };

                    if opts.recursive {
                        let mut dir_name = meta.name.clone();
                        if !dir_name.ends_with(SLASH_SEPARATOR) {
                            dir_name.push_str(SLASH_SEPARATOR);
                        }
                        schedule_dir(
                            &mut dir_stack,
                            dir_name,
                            true,
                            if dir_to_skip.is_empty() { None } else { Some(dir_to_skip) },
                            has_namespace_child_candidate,
                        );
                    } else if !is_dir_obj
                        && self
                            .object_dir_has_listable_child(&opts.bucket, &meta.name, &dir_to_skip, opts.incl_deleted, stall)
                            .await?
                    {
                        // A plain object `a` shares its backing directory with any
                        // children `a/...`, and non-recursive walks never descend into
                        // it — so the prefix `a/` must be produced here or delimiter
                        // listings lose the CommonPrefix (backlog#1042). Dir-marker
                        // objects are excluded: their logical children live in a
                        // separate real directory entry handled above.
                        let mut dir_name = meta.name.clone();
                        dir_name.push_str(SLASH_SEPARATOR);
                        schedule_dir(&mut dir_stack, dir_name, true, None, true);
                    }
                }
                Err(err) => {
                    if err == Error::FileNotFound || err == Error::IsNotRegular {
                        // NOT an object, append to stack (with slash)
                        // If dirObject, but no metadata (which is unexpected) we skip it.
                        if !is_dir_obj
                            && !with_walk_stall_deadline(stall, is_empty_dir(self.io_get_object_path(&opts.bucket, &meta.name)?))
                                .await?
                        {
                            meta.name.push_str(SLASH_SEPARATOR);
                            if opts.recursive
                                || opts.incl_deleted
                                || self
                                    .directory_has_listing_entry(&opts.bucket, &meta.name, opts.incl_deleted, stall)
                                    .await?
                            {
                                schedule_dir(&mut dir_stack, meta.name, false, None, true);
                            }
                        }

                        continue;
                    }

                    error!(
                        event = EVENT_DISK_LOCAL_SCAN_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = %fname,
                        operation = "read_metadata",
                        error = ?err,
                        "Disk local scan failed"
                    );
                    return Err(err);
                }
            };
        }

        while let Some((dir, skip_object, dir_to_skip, scan_required)) = dir_stack.pop() {
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }

            write_metacache_obj(
                out,
                &MetaCacheEntry {
                    name: dir.clone(),
                    ..Default::default()
                },
            )
            .await?;

            let scan_path = dir.clone();
            if opts.recursive
                && scan_required
                && let Err(er) =
                    Box::pin(self.scan_dir(dir, prefix.clone(), opts, out, objs_returned, skip_object, dir_to_skip)).await
            {
                if !er.is_metacache_output_stream_closed() {
                    error!(
                        event = EVENT_DISK_LOCAL_SCAN_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = %scan_path,
                        operation = "scan_dir",
                        error = ?er,
                        "Disk local recursive scan failed"
                    );
                }
                return Err(er);
            }
        }

        Ok(())
    }

    /// Whether the backing directory of plain object `object_name` also holds
    /// listable children (`object_name/...`), beyond the object's own storage
    /// internals: its `xl.meta` and the version data dirs in `data_dirs`.
    ///
    /// With `incl_deleted`, a child subtree counts as soon as it holds any
    /// object metadata (versioned listings surface delete markers too);
    /// otherwise the child must have a visible listing entry.
    async fn object_dir_has_listable_child(
        &self,
        bucket: &str,
        object_name: &str,
        data_dirs: &HashSet<String>,
        incl_deleted: bool,
        stall: Option<Duration>,
    ) -> Result<bool> {
        // The backing dir usually holds only the xl.meta plus the version data
        // dirs, so a read bounded just past that count decides the common case
        // without materializing large child sets: an under-filled batch proves
        // the listing is complete. Only an inconclusive full batch (candidates
        // present but none listable yet) falls back to the unbounded read.
        let bounded = i32::try_from(data_dirs.len() + 2).unwrap_or(-1);
        let mut probed = HashSet::new();

        for count in [bounded, -1] {
            let entries = match with_walk_stall_timeout(stall, self.list_dir("", bucket, object_name, count)).await {
                Ok(entries) => entries,
                Err(err) => {
                    if err == DiskError::VolumeNotFound || err == Error::FileNotFound {
                        return Ok(false);
                    }

                    return Err(err);
                }
            };

            let complete = count < 0 || entries.len() < count as usize;

            for entry in entries {
                let Some(child) = entry.strip_suffix(SLASH_SEPARATOR) else {
                    // Plain files (the object's own xl.meta) can never hold children.
                    continue;
                };

                if child.is_empty() || data_dirs.contains(child) || !probed.insert(child.to_owned()) {
                    continue;
                }

                let child_path = path_join_buf(&[object_name, child]);
                if self
                    .directory_has_listing_entry(bucket, &child_path, incl_deleted, stall)
                    .await?
                {
                    return Ok(true);
                }
            }

            if complete {
                break;
            }
        }

        Ok(false)
    }

    /// Whether anything under `dir_name` would appear in a listing. With
    /// `incl_deleted`, any `xl.meta` counts (versioned listings surface
    /// delete-marker-only objects too); otherwise the metadata must hold a
    /// visible version.
    async fn directory_has_listing_entry(
        &self,
        bucket: &str,
        dir_name: &str,
        incl_deleted: bool,
        stall: Option<Duration>,
    ) -> Result<bool> {
        let mut stack = vec![dir_name.trim_matches('/').to_owned()];

        while let Some(current) = stack.pop() {
            if current.is_empty() {
                continue;
            }

            let entries = match with_walk_stall_timeout(stall, self.list_dir("", bucket, &current, -1)).await {
                Ok(entries) => entries,
                Err(err) => {
                    if err == DiskError::VolumeNotFound || err == Error::FileNotFound {
                        continue;
                    }

                    return Err(err);
                }
            };

            let mut data_dirs_to_skip = HashSet::new();
            let mut child_dirs = Vec::new();

            for entry in entries {
                if entry == STORAGE_FORMAT_FILE {
                    if incl_deleted {
                        return Ok(true);
                    }

                    let metadata_path = path_join_buf(&[current.as_str(), STORAGE_FORMAT_FILE]);
                    match with_walk_stall_timeout(stall, self.read_metadata(bucket, metadata_path.as_str())).await {
                        Ok(metadata) => {
                            let file_meta = match FileMeta::load(&metadata) {
                                Ok(file_meta) => file_meta,
                                Err(_) => return Ok(true),
                            };

                            if file_meta_counts_toward_limit(&file_meta) {
                                return Ok(true);
                            }

                            if let Ok(data_dirs) = file_meta.get_data_dirs() {
                                for data_dir in data_dirs.iter().flatten() {
                                    data_dirs_to_skip.insert(data_dir.to_string());
                                }
                            }
                        }
                        Err(err) => {
                            if err != Error::FileNotFound && err != Error::IsNotRegular {
                                return Err(err);
                            }
                        }
                    }

                    continue;
                }

                if entry.ends_with(SLASH_SEPARATOR) {
                    let child = entry.trim_end_matches(SLASH_SEPARATOR);
                    if !child.is_empty() {
                        child_dirs.push(child.to_owned());
                    }
                }
            }

            for child in child_dirs {
                if !data_dirs_to_skip.contains(&child) {
                    stack.push(path_join_buf(&[current.as_str(), child.as_str()]));
                }
            }
        }

        Ok(false)
    }
}

pub struct ScanGuard(pub Arc<AtomicU32>);

impl Drop for ScanGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

fn rename_data_versions_signature(meta: &FileMeta) -> Option<Vec<u8>> {
    if meta.versions.len() > 10 {
        return None;
    }

    let mut signature = Vec::with_capacity(meta.versions.len() * 16);
    for version in meta.versions.iter() {
        signature.extend_from_slice(version.header.version_id.unwrap_or_default().as_bytes());
    }
    Some(signature)
}

/// rustfs/backlog#1009: observe the destination key's *current* (latest)
/// version in the dst `xl.meta` that `rename_data` already loaded, before
/// `add_version` commits the incoming one. Replicates the pre-PUT
/// `get_object_info` outcome bit for bit, as the per-disk `read_version`
/// pipeline (`get_file_info`) would report it:
///
/// - `dst_meta_existed == false` (no dst xl.meta on this disk): the lookup
///   errors FileNotFound → app-level `None` → `Absent`.
/// - Existing meta whose versions are all hidden free versions: the lookup
///   errors FileNotFound the same way → `Absent`.
/// - Existing meta with zero versions: `get_file_info` synthesizes a deleted
///   `FileInfo` with size 0 and the lookup returns `Ok` → `Present(0)`.
/// - A resolvable latest version — live object, delete marker, or a
///   purge-pending version flagged `deleted` — returns `Ok` with
///   `ObjectInfo.size == fi.size` (0 for markers) → `Present(fi.size)`.
///   Delete markers deliberately do NOT map to `Absent`: today's lookup
///   returns `Ok(size 0)` for them, and delete-marker creation never
///   decrements `objects_count`, so `Some(0)` is what keeps versioned
///   accounting bit-identical.
/// - A latest version that fails to decode — including the part-array length
///   guard that `all_parts=true` enables, the same flag the per-disk lookup
///   uses — yields `None` (unknown): the old lookup surfaced a per-disk error
///   there, so this disk must not vote in the set-level quorum reduction.
///
/// One deliberate divergence: `read_data` stays `false` (the lookup used
/// `true`), so a corrupt inline-data map that would have errored the old
/// lookup votes the version's own `size` here instead of abstaining. The
/// size field decodes independently of the inline data, so the vote carries
/// the same value healthy disks report, and skipping the lookup's inline
/// bytes clone keeps the observation allocation-light.
fn observe_old_current_size(dst_meta_existed: bool, xlmeta: &FileMeta) -> Option<OldCurrentSize> {
    if !dst_meta_existed {
        return Some(OldCurrentSize::Absent);
    }
    if xlmeta.versions.is_empty() {
        return Some(OldCurrentSize::Present(0));
    }
    match xlmeta.into_fileinfo("", "", "", false, false, true) {
        Ok(fi) => Some(OldCurrentSize::Present(fi.size)),
        Err(rustfs_filemeta::Error::FileNotFound) => Some(OldCurrentSize::Absent),
        Err(_) => None,
    }
}

fn is_root_path(path: impl AsRef<Path>) -> bool {
    path.as_ref().components().count() == 1 && path.as_ref().has_root()
}

fn metadata_counts_toward_limit(metadata: &[u8]) -> bool {
    FileMeta::load(metadata).map_or(true, |meta| file_meta_counts_toward_limit(&meta))
}

fn file_meta_counts_toward_limit(meta: &FileMeta) -> bool {
    meta.into_fileinfo("", "", "", false, true, false)
        .map_or_else(|_| !meta.all_hidden(true), |latest| !latest.deleted && !latest.tier_free_version())
}

// Filter std::io::ErrorKind::NotFound
async fn read_file_exists(path: impl AsRef<Path>) -> Result<(Bytes, Option<Metadata>)> {
    let p = path.as_ref();
    let (data, meta) = match read_file_all(&p).await {
        Ok((data, meta)) => (data, Some(meta)),
        Err(e) => {
            if e == Error::FileNotFound {
                (Bytes::new(), None)
            } else {
                return Err(e);
            }
        }
    };

    // let mut data = Vec::new();
    // if meta.is_some() {
    //     data = fs::read(&p).await?;
    // }

    Ok((data, meta))
}

async fn read_file_all(path: impl AsRef<Path>) -> Result<(Bytes, Metadata)> {
    let p = path.as_ref();
    let meta = read_file_metadata(&path).await?;

    let data = fs::read(&p)
        .await
        .inspect_err(|err| {
            log_startup_disk_io_error("read_file_all", p, err);
        })
        .map_err(to_file_error)?;

    Ok((data.into(), meta))
}

async fn read_file_metadata(p: impl AsRef<Path>) -> Result<Metadata> {
    let path = p.as_ref();
    let meta = fs::metadata(path)
        .await
        .inspect_err(|err| {
            if err.kind() != ErrorKind::NotFound {
                log_startup_disk_io_error("read_file_metadata", path, err);
            }
        })
        .map_err(to_file_error)?;

    Ok(meta)
}

fn skip_access_checks(p: impl AsRef<str>) -> bool {
    let vols = [
        RUSTFS_META_TMP_DELETED_BUCKET,
        RUSTFS_META_TMP_BUCKET,
        super::RUSTFS_META_MULTIPART_BUCKET,
        RUSTFS_META_BUCKET,
    ];

    for v in vols.iter() {
        if p.as_ref().starts_with(v) {
            return true;
        }
    }

    false
}

fn local_disk_object_path(root: &Path, bucket: &str, key: &str) -> Result<PathBuf> {
    let (bucket_path, path) = build_local_disk_object_path(root, bucket, key);
    check_local_disk_valid_object_path(root, &bucket_path, &path)?;
    Ok(path)
}

fn build_local_disk_object_path(root: &Path, bucket: &str, key: &str) -> (PathBuf, PathBuf) {
    let cache_key = if key.is_empty() {
        bucket.to_string()
    } else {
        path_join_buf(&[bucket, key])
    };

    #[cfg(windows)]
    let bucket_path = root.join(bucket.replace('/', "\\"));
    #[cfg(not(windows))]
    let bucket_path = root.join(bucket);

    #[cfg(windows)]
    let path = root.join(cache_key.replace('/', "\\"));
    #[cfg(not(windows))]
    let path = root.join(cache_key);

    (bucket_path, path)
}

fn local_disk_bucket_path(root: &Path, bucket: &str) -> Result<PathBuf> {
    let bucket_path = build_local_disk_bucket_path(root, bucket);
    check_local_disk_valid_path(root, &bucket_path)?;
    Ok(bucket_path)
}

fn build_local_disk_bucket_path(root: &Path, bucket: &str) -> PathBuf {
    #[cfg(windows)]
    let bucket_path = root.join(bucket.replace('/', "\\"));
    #[cfg(not(windows))]
    let bucket_path = root.join(bucket);

    bucket_path
}

fn check_local_disk_valid_object_path(root: &Path, bucket_path: &Path, path: &Path) -> Result<()> {
    let bucket_path = normalize_path_components(bucket_path);
    let path = normalize_path_components(path);
    if !bucket_path.starts_with(root) || !path.starts_with(&bucket_path) {
        return Err(DiskError::InvalidPath);
    }

    reject_local_disk_symlink_components(root, &path)
}

fn check_local_disk_valid_path(root: &Path, path: impl AsRef<Path>) -> Result<()> {
    let path = normalize_path_components(path);
    if !path.starts_with(root) {
        return Err(DiskError::InvalidPath);
    }

    reject_local_disk_symlink_components(root, &path)
}

#[cfg(target_os = "linux")]
fn check_local_disk_valid_object_path_at(root: &Path, root_fd: &std::fs::File, bucket_path: &Path, path: &Path) -> Result<()> {
    let bucket_path = normalize_path_components(bucket_path);
    let path = normalize_path_components(path);
    if !bucket_path.starts_with(root) || !path.starts_with(&bucket_path) {
        return Err(DiskError::InvalidPath);
    }

    reject_local_disk_symlink_components_at(root, root_fd, &path)
}

#[cfg(target_os = "linux")]
fn check_local_disk_valid_path_at(root: &Path, root_fd: &std::fs::File, path: impl AsRef<Path>) -> Result<()> {
    let path = normalize_path_components(path);
    if !path.starts_with(root) {
        return Err(DiskError::InvalidPath);
    }

    reject_local_disk_symlink_components_at(root, root_fd, &path)
}

#[cfg(target_os = "linux")]
fn reject_local_disk_symlink_components_at(root: &Path, root_fd: &std::fs::File, path: &Path) -> Result<()> {
    let relative = path.strip_prefix(root).map_err(|_| DiskError::InvalidPath)?;
    match validate_existing_local_disk_prefix_at(root_fd, relative) {
        Ok(()) => Ok(()),
        Err(LocalDiskPathValidationAtError::Unsupported) => reject_local_disk_symlink_components(root, path),
        Err(LocalDiskPathValidationAtError::InvalidPath) => Err(DiskError::InvalidPath),
        Err(LocalDiskPathValidationAtError::Io(err)) => Err(to_file_error(err).into()),
    }
}

#[cfg(target_os = "linux")]
enum LocalDiskPathValidationAtError {
    Unsupported,
    InvalidPath,
    Io(std::io::Error),
}

#[cfg(target_os = "linux")]
fn validate_existing_local_disk_prefix_at(
    root_fd: &std::fs::File,
    relative: &Path,
) -> core::result::Result<(), LocalDiskPathValidationAtError> {
    use rustix::fs::{Mode, OFlags, ResolveFlags, openat2};
    use rustix::io::Errno;

    if relative.as_os_str().is_empty() {
        return Ok(());
    }

    let mut candidate = relative.to_path_buf();
    loop {
        match openat2(
            root_fd,
            &candidate,
            OFlags::PATH | OFlags::CLOEXEC,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS,
        ) {
            Ok(_) => return Ok(()),
            Err(Errno::NOSYS) => return Err(LocalDiskPathValidationAtError::Unsupported),
            Err(Errno::LOOP | Errno::XDEV) => return Err(LocalDiskPathValidationAtError::InvalidPath),
            Err(Errno::NOENT) => {
                let Some(parent) = candidate.parent().filter(|parent| !parent.as_os_str().is_empty()) else {
                    return Ok(());
                };
                candidate = parent.to_path_buf();
            }
            Err(err) => return Err(LocalDiskPathValidationAtError::Io(err.into())),
        }
    }
}

fn reject_local_disk_symlink_components(root: &Path, path: &Path) -> Result<()> {
    let relative = path.strip_prefix(root).map_err(|_| DiskError::InvalidPath)?;
    let mut current = root.to_path_buf();

    for component in relative.components() {
        current.push(component.as_os_str());

        match lstat_std(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(DiskError::InvalidPath);
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => break,
            Err(err) => return Err(to_file_error(err).into()),
        }
    }

    Ok(())
}

// Lightweight path normalization without filesystem calls
fn normalize_path_components(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    let mut result = PathBuf::new();

    for component in path.components() {
        match component {
            std::path::Component::Normal(name) => {
                result.push(name);
            }
            std::path::Component::ParentDir => {
                result.pop();
            }
            std::path::Component::CurDir => {
                // Ignore current directory components
            }
            std::path::Component::RootDir => {
                result.push(component);
            }
            std::path::Component::Prefix(_prefix) => {
                result.push(component);
            }
        }
    }

    result
}

impl LocalDisk {
    async fn claim_quota_mutation_fence(
        &self,
        volume: &str,
        path: &str,
        token: SnapshotLeaseToken,
    ) -> Result<Arc<QuotaMutationFenceClaim>> {
        let key = SnapshotLeaseKey {
            volume: RUSTFS_META_BUCKET.to_string(),
            path: quota_mutation_fence_path(volume, path),
        };
        let state = {
            let registry = self.snapshot_leases.lock().await;
            let entry = registry.entries.get(&key).ok_or(DiskError::FileNotFound)?;
            let state = entry.mutation_fence.as_ref().ok_or(DiskError::FileNotFound)?;
            if !entry.tokens.contains(&token) || state.revoked.load(Ordering::Acquire) {
                return Err(DiskError::FileNotFound);
            }
            state.running.fetch_add(1, Ordering::AcqRel);
            Arc::clone(state)
        };
        if state.revoked.load(Ordering::Acquire) {
            state.running.fetch_sub(1, Ordering::AcqRel);
            state.notify.notify_waiters();
            return Err(DiskError::FileNotFound);
        }
        Ok(Arc::new(QuotaMutationFenceClaim { state }))
    }

    async fn reserve_version_delete(&self, volume: &str, object: &str, data_dir: Uuid, rollback_dir: Uuid) -> Result<bool> {
        let path = format!("{object}/{data_dir}");
        let data_path = self.io_get_object_path(volume, &path)?;
        match fs::metadata(&data_path).await {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Ok(false),
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(to_file_error(err).into()),
        }
        let marker_path = data_path.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{rollback_dir}"));
        let marker = File::create(marker_path).await.map_err(to_file_error)?;
        if effective_durability(volume).syncs_commit_metadata() {
            marker.sync_all().await.map_err(to_file_error)?;
            os::fsync_dir(&data_path).await.map_err(to_file_error)?;
        }
        Ok(true)
    }

    async fn commit_reserved_version_delete(&self, volume: &str, object: &str, rollback_dir: Uuid) -> Result<()> {
        let object_path = self.io_get_object_path(volume, object)?;
        let mut entries = match fs::read_dir(object_path).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(to_file_error(err).into()),
        };
        let reserved_name = format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{rollback_dir}");
        let committed_name = format!("{DELETE_DATA_DIR_MARKER_PREFIX}{rollback_dir}");
        while let Some(entry) = entries.next_entry().await.map_err(to_file_error)? {
            if !entry.file_type().await.map_err(to_file_error)?.is_dir()
                || !entry.file_name().to_str().is_some_and(|name| Uuid::parse_str(name).is_ok())
            {
                continue;
            }
            let reserved_path = entry.path().join(&reserved_name);
            match fs::rename(&reserved_path, entry.path().join(&committed_name)).await {
                Ok(()) => {
                    if effective_durability(volume).syncs_commit_metadata() {
                        os::fsync_dir(&entry.path()).await.map_err(to_file_error)?;
                    }
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(to_file_error(err).into()),
            }
        }
        Ok(())
    }

    async fn finish_version_delete(&self, volume: &str, object: &str, rollback_dir: Uuid) -> Result<bool> {
        let object_path = self.io_get_object_path(volume, object)?;
        let mut entries = match fs::read_dir(object_path).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(to_file_error(err).into()),
        };
        let marker_name = format!("{DELETE_DATA_DIR_MARKER_PREFIX}{rollback_dir}");
        let mut first_err = None;
        let mut found = false;
        while let Some(entry) = entries.next_entry().await.map_err(to_file_error)? {
            let Some(data_dir) = entry.file_name().to_str().and_then(|data_dir| Uuid::parse_str(data_dir).ok()) else {
                continue;
            };
            match fs::metadata(entry.path().join(&marker_name)).await {
                Ok(metadata) if metadata.is_file() => found = true,
                Ok(_) => continue,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(to_file_error(err).into()),
            }
            if let Err(err) = self
                .delete_data_dir(
                    volume,
                    &format!("{object}/{data_dir}"),
                    DeleteOptions {
                        recursive: true,
                        ..Default::default()
                    },
                )
                .await
                && first_err.is_none()
                && err != DiskError::FileNotFound
                && err != DiskError::VolumeNotFound
            {
                first_err = Some(err);
            }
        }
        first_err.map_or(Ok(found), Err)
    }

    async fn abort_reserved_version_delete(
        &self,
        object_dir: &Path,
        rollback_dir: Uuid,
        volume: &str,
        object: &str,
        stage: &'static str,
        err: DiskError,
    ) -> DiskError {
        let xl_path = object_dir.join(STORAGE_FORMAT_FILE);
        restore_delete_rollback_after_error(
            object_dir,
            &xl_path,
            Some(rollback_dir),
            volume,
            object,
            DeleteRollbackFailure { stage, error: err },
            &self.publication_root,
        )
        .await
    }

    /// Execute every deferred data-dir deletion pending on `volume` right now,
    /// even while snapshot leases are still held. Bucket deletion requires it:
    /// a streaming reader defers the physical cleanup of an already-deleted
    /// version, and a non-force `delete_volume` would otherwise fail closed
    /// with `VolumeNotEmpty` on those remnants even though the bucket is
    /// logically empty. The still-active readers keep their open descriptors;
    /// only path-based reopens observe the removal.
    async fn settle_pending_snapshot_deletes(&self, volume: &str) {
        let pending: Vec<(SnapshotLeaseKey, DeleteOptions)> = {
            let mut registry = self.snapshot_leases.lock().await;
            registry
                .entries
                .iter_mut()
                .filter(|(key, entry)| key.volume == volume && !entry.deleting && entry.pending_delete.is_some())
                .map(|(key, entry)| {
                    entry.deleting = true;
                    (key.clone(), entry.pending_delete.clone().expect("filtered on Some"))
                })
                .collect()
        };

        for (key, opts) in pending {
            let result = self.delete_unleased(&key.volume, &key.path, &opts).await;
            let mut registry = self.snapshot_leases.lock().await;
            match result {
                Ok(()) => {
                    registry.entries.remove(&key);
                }
                Err(err) => {
                    if let Some(entry) = registry.entries.get_mut(&key) {
                        entry.deleting = false;
                    }
                    warn!(
                        volume = %key.volume,
                        path = %key.path,
                        error = %err,
                        "failed to settle deferred data-dir deletion before volume removal"
                    );
                }
            }
        }
    }
}

/// Batch positioned reads for local EC shard files in a single `spawn_blocking`.
///
/// Collapses per-shard blocking-pool round-trips that dominate warm GET
/// fan-out on single-node multi-disk topologies.
#[cfg(unix)]
pub(crate) async fn batch_shard_pread(requests: Vec<(std::path::PathBuf, usize, usize)>) -> Vec<Result<Bytes>> {
    let n = requests.len();
    tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;

        let mut results = Vec::with_capacity(n);
        for (file_path, offset, length) in requests {
            let r = (|| -> Result<Bytes> {
                let meta = std::fs::metadata(&file_path).map_err(DiskError::from)?;
                let end = offset.checked_add(length).ok_or(DiskError::FileCorrupt)?;
                if meta.len() < u64::try_from(end).unwrap_or(u64::MAX) {
                    return Err(DiskError::FileCorrupt);
                }

                let file = std::fs::File::open(&file_path).map_err(DiskError::from)?;
                let mut buf = vec![0u8; length];
                let mut total = 0usize;
                while total < length {
                    let nbytes = file
                        .read_at(&mut buf[total..], u64::try_from(offset + total).unwrap_or(u64::MAX))
                        .map_err(DiskError::from)?;
                    if nbytes == 0 {
                        return Err(DiskError::FileCorrupt);
                    }
                    total += nbytes;
                }
                Ok(Bytes::from(buf))
            })();
            results.push(r);
        }
        results
    })
    .await
    .unwrap_or_else(|e| {
        let msg = format!("spawn_blocking join: {e}");
        (0..n).map(|_| Err(DiskError::other(msg.clone()))).collect()
    })
}

#[async_trait::async_trait]
impl DiskAPI for LocalDisk {
    fn to_string(&self) -> String {
        self.root.to_string_lossy().to_string()
    }

    fn is_local(&self) -> bool {
        true
    }

    fn host_name(&self) -> String {
        self.endpoint.host_port()
    }

    async fn is_online(&self) -> bool {
        true
    }

    fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    async fn close(&self) -> Result<()> {
        // This disk instance is being retired (e.g. replaced by renew_disk on
        // reconnect). Drop its cached descriptors so a replacement instance's
        // invalidations are never defeated by this one continuing to serve stale
        // fds through operations still holding a snapshot of it
        // (rustfs/backlog#1177).
        self.io_backend.clear_cached_fds().await;
        Ok(())
    }

    fn path(&self) -> PathBuf {
        self.root.clone()
    }

    fn get_disk_location(&self) -> DiskLocation {
        DiskLocation {
            pool_idx: {
                if self.endpoint.pool_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.pool_idx as usize)
                }
            },
            set_idx: {
                if self.endpoint.set_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.set_idx as usize)
                }
            },
            disk_idx: {
                if self.endpoint.disk_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.disk_idx as usize)
                }
            },
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        let format_info = {
            let format_info = self.format_info.read().await;
            format_info.clone()
        };

        let id = format_info.id;

        if format_info.file_info.is_some() && id.is_some() {
            // Reuse the cached disk id only when the cached format check is fresh.
            if let Some(last_check) = format_info.last_check
                && last_check.unix_timestamp() + 1 >= OffsetDateTime::now_utc().unix_timestamp()
            {
                return Ok(id);
            }
        }

        let file_meta = match self.check_format_json().await {
            Ok(meta) => meta,
            Err(err) => {
                if matches!(err, DiskError::UnformattedDisk | DiskError::DiskNotFound) {
                    let mut format_info = self.format_info.write().await;
                    format_info.id = None;
                    format_info.data = Bytes::new();
                    format_info.file_info = None;
                    format_info.last_check = None;
                }
                return Err(err);
            }
        };

        if let Some(file_info) = &format_info.file_info
            && super::fs::same_file(&file_meta, file_info)
        {
            let mut format_info = self.format_info.write().await;
            format_info.last_check = Some(OffsetDateTime::now_utc());
            drop(format_info);

            return Ok(id);
        }

        debug!("get_disk_id: read format.json");

        let b = fs::read(&self.io_format_path).await.map_err(to_unformatted_disk_error)?;

        let fm = FormatV3::try_from(b.as_slice()).map_err(|e| {
            warn!(
                event = EVENT_DISK_LOCAL_FORMAT_DECODE_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                error = ?e,
                "Disk local format decode failed"
            );
            DiskError::CorruptedBackend
        })?;

        let (m, n) = fm.find_disk_index_by_disk_id(fm.erasure.this)?;

        let disk_id = fm.erasure.this;

        if m as i32 != self.endpoint.set_idx || n as i32 != self.endpoint.disk_idx {
            return Err(DiskError::InconsistentDisk);
        }

        let mut format_info = self.format_info.write().await;
        format_info.id = Some(disk_id);
        format_info.file_info = Some(file_meta);
        format_info.data = b.into();
        format_info.last_check = Some(OffsetDateTime::now_utc());
        drop(format_info);

        Ok(Some(disk_id))
    }

    async fn set_disk_id(&self, _id: Option<Uuid>) -> Result<()> {
        // No setup is required locally
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        crate::hp_guard!("LocalDisk::read_all");
        if volume == RUSTFS_META_BUCKET && path == super::FORMAT_CONFIG_FILE {
            let format_info = self.format_info.read().await;
            if !format_info.data.is_empty() {
                return Ok(format_info.data.clone());
            }
        }

        let p = self.io_get_object_path(volume, path)?;

        let (data, _) = read_file_all(&p).await?;

        Ok(data)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        crate::hp_guard!("LocalDisk::write_all");
        self.write_all_public(volume, path, data).await
    }

    async fn compare_and_update_file(
        &self,
        volume: &str,
        path: &str,
        expected: Option<Bytes>,
        replacement: Option<Bytes>,
    ) -> Result<ConditionalFileUpdate> {
        #[cfg(unix)]
        {
            use rustix::fs::{FlockOperation, flock};
            use std::io::Write as _;

            let file_path = self.io_get_object_path(volume, path)?;
            let path = path.to_string();
            let sync_metadata = effective_durability(volume).syncs_commit_metadata();
            return Ok(tokio::task::spawn_blocking(move || {
                // A persistent directory lock bounds metadata growth. Removing
                // per-target lock files can split flock ownership across inodes.
                let lock_path = file_path
                    .parent()
                    .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "conditional file has no parent"))?
                    .join(".rustfs-cas.lock");
                let lock = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&lock_path)?;
                flock(&lock, FlockOperation::NonBlockingLockExclusive).map_err(std::io::Error::from)?;
                let result = (|| {
                    let current = match std::fs::read(&file_path) {
                        Ok(current) => Some(current),
                        Err(err) if err.kind() == ErrorKind::NotFound => None,
                        Err(err) => return Err(err),
                    };
                    let matches = match (&current, &expected) {
                        (None, None) => true,
                        (Some(current), Some(expected)) => current.as_slice() == expected.as_ref(),
                        _ => false,
                    };
                    if !matches {
                        return Ok(match current {
                            None => ConditionalFileUpdate::Missing,
                            Some(_) => ConditionalFileUpdate::Mismatch,
                        });
                    }

                    match replacement {
                        Some(replacement) => {
                            let parent = file_path
                                .parent()
                                .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "conditional file has no parent"))?;
                            let temporary = parent.join(format!(".{}.{}.tmp", path.replace('/', "_"), Uuid::new_v4()));
                            let write_result = (|| -> std::io::Result<()> {
                                let mut staged = std::fs::OpenOptions::new().create_new(true).write(true).open(&temporary)?;
                                staged.write_all(&replacement)?;
                                if sync_metadata {
                                    staged.sync_all()?;
                                }
                                std::fs::rename(&temporary, &file_path)?;
                                Ok(())
                            })();
                            if let Err(err) = write_result {
                                let _ = std::fs::remove_file(&temporary);
                                return Err(err);
                            }
                            if sync_metadata {
                                os::fsync_dir_std(parent)?;
                            }
                        }
                        None => {
                            std::fs::remove_file(&file_path)?;
                            if sync_metadata && let Some(parent) = file_path.parent() {
                                os::fsync_dir_std(parent)?;
                            }
                        }
                    }
                    Ok(ConditionalFileUpdate::Updated)
                })();
                let _ = flock(&lock, FlockOperation::Unlock);
                result
            })
            .await
            .map_err(DiskError::from)??);
        }

        #[cfg(windows)]
        {
            let file_path = self.io_get_object_path(volume, path)?;
            let sync_metadata = effective_durability(volume).syncs_commit_metadata();
            let publication_root = self.publication_root.clone();
            return Ok(tokio::task::spawn_blocking(move || {
                os::compare_and_update_control_file(
                    &file_path,
                    expected.as_deref(),
                    replacement.as_deref(),
                    sync_metadata,
                    &publication_root,
                )
            })
            .await
            .map_err(DiskError::from)??);
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = (volume, path, expected, replacement);
            Err(DiskError::MethodNotAllowed)
        }
    }

    fn has_replacement_mount_lease(&self) -> bool {
        LocalDisk::has_replacement_mount_lease(self)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        crate::hp_guard!("LocalDisk::delete");
        let handled_version_delete = if opt.recursive
            && opt.immediate
            && let Some((object, transaction_id)) = path.rsplit_once('/')
            && let Ok(transaction_id) = Uuid::parse_str(transaction_id)
        {
            self.finish_version_delete(volume, object, transaction_id).await?
        } else {
            false
        };
        match self.delete_unleased(volume, path, &opt).await {
            Err(DiskError::FileNotFound) if handled_version_delete => Ok(()),
            result => result,
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        let volume_dir = self.io_get_bucket_path(volume)?;
        if !skip_access_checks(volume)
            && let Err(e) = access(&volume_dir).await
        {
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        let mut resp = CheckPartsResp {
            results: vec![0; fi.parts.len()],
        };

        let erasure = &fi.erasure;
        let codec_erasure = coding::Erasure::try_new_with_options(
            erasure.data_blocks,
            erasure.parity_blocks,
            erasure.block_size,
            fi.uses_legacy_checksum,
        )
        .map_err(DiskError::from)?;
        fi.validate(ValidationMode::RequireErasure)?;
        for (i, part) in fi.parts.iter().enumerate() {
            let checksum_info = erasure.get_checksum_info(part.number);
            let checksum_algo = if fi.uses_legacy_checksum && checksum_info.algorithm == HashAlgorithm::HighwayHash256S {
                HashAlgorithm::HighwayHash256SLegacy
            } else {
                checksum_info.algorithm
            };
            let part_path = self.io_get_object_path(
                volume,
                path_join_buf(&[
                    path,
                    &fi.data_dir.map_or_else(|| "".to_string(), |dir| dir.to_string()),
                    &format!("part.{}", part.number),
                ])
                .as_str(),
            )?;
            let err = self
                .bitrot_verify(
                    &part_path,
                    codec_erasure.shard_file_size(part.size as i64) as usize,
                    checksum_algo,
                    codec_erasure.shard_size(),
                )
                .await
                .err();
            resp.results[i] = conv_part_err_to_int(&err);
            if resp.results[i] == CHECK_PART_UNKNOWN
                && let Some(err) = err
            {
                error!(
                    event = EVENT_DISK_LOCAL_CHECK_PARTS,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    path = ?part_path,
                    part_number = part.number,
                    state = "bitrot_verify_failed",
                    error = ?err,
                    "Disk local check_parts state changed"
                );
                if err == DiskError::FileAccessDenied {
                    continue;
                }
                info!(
                    event = EVENT_DISK_LOCAL_CHECK_PARTS,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    endpoint = %self.endpoint,
                    path = ?part_path,
                    part_number = part.number,
                    state = "unknown",
                    "Disk local check_parts state changed"
                );
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_parts(&self, bucket: &str, paths: &[String]) -> Result<Vec<ObjectPartInfo>> {
        let volume_dir = self.io_get_bucket_path(bucket)?;

        let mut ret = vec![ObjectPartInfo::default(); paths.len()];

        for (i, path_str) in paths.iter().enumerate() {
            let path = Path::new(path_str);
            let file_name = path.file_name().and_then(|v| v.to_str()).unwrap_or_default();
            let num = file_name
                .strip_prefix("part.")
                .and_then(|v| v.strip_suffix(".meta"))
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or_default();

            if let Err(err) = access(
                self.io_get_object_path(
                    bucket,
                    path_join_buf(&[
                        path.parent().unwrap_or_else(|| Path::new("")).to_string_lossy().as_ref(),
                        &format!("part.{num}"),
                    ])
                    .as_str(),
                )?,
            )
            .await
            {
                ret[i] = ObjectPartInfo {
                    number: num,
                    error: Some(err.to_string()),
                    ..Default::default()
                };
                continue;
            }

            let data = match self
                .read_all_data(
                    bucket,
                    volume_dir.clone(),
                    self.io_get_object_path(bucket, path.to_string_lossy().as_ref())?,
                )
                .await
            {
                Ok(data) => data,
                Err(err) => {
                    ret[i] = ObjectPartInfo {
                        number: num,
                        error: Some(err.to_string()),
                        ..Default::default()
                    };
                    continue;
                }
            };

            match ObjectPartInfo::unmarshal(&data) {
                Ok(meta) => {
                    ret[i] = meta;
                }
                Err(err) => {
                    ret[i] = ObjectPartInfo {
                        number: num,
                        error: Some(err.to_string()),
                        ..Default::default()
                    };
                }
            };
        }

        Ok(ret)
    }
    #[tracing::instrument(level = "trace", skip_all)]
    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        let layout = fi.validate(ValidationMode::RequireErasure)?.ok_or(DiskError::FileCorrupt)?;
        let volume_dir = self.io_get_bucket_path(volume)?;
        let file_path = self.io_get_object_path(volume, path)?;
        check_path_length(file_path.to_string_lossy().as_ref())?;
        let mut resp = CheckPartsResp {
            results: vec![0; fi.parts.len()],
        };

        for (i, part) in fi.parts.iter().enumerate() {
            let part_path = self.io_get_object_path(
                volume,
                path_join_buf(&[
                    path,
                    &fi.data_dir.map_or_else(|| "".to_string(), |dir| dir.to_string()),
                    &format!("part.{}", part.number),
                ])
                .as_str(),
            )?;

            debug!(
                event = EVENT_DISK_LOCAL_CHECK_PARTS,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                path = ?part_path,
                part_number = part.number,
                state = "checking",
                "Disk local check_parts state changed"
            );

            match lstat(&part_path).await {
                Ok(st) => {
                    if st.is_dir() {
                        resp.results[i] = CHECK_PART_FILE_NOT_FOUND;
                        continue;
                    }
                    let expected_size = layout.shard_file_size(part.size).ok_or(DiskError::FileCorrupt)?;
                    let expected_size = u64::try_from(expected_size).map_err(|_| DiskError::FileCorrupt)?;
                    if st.len() < expected_size {
                        resp.results[i] = CHECK_PART_FILE_CORRUPT;
                        continue;
                    }

                    resp.results[i] = CHECK_PART_SUCCESS;
                }
                Err(err) => {
                    debug!(
                        event = EVENT_DISK_LOCAL_CHECK_PARTS,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = ?part_path,
                        part_number = part.number,
                        state = "part_stat_failed",
                        error = ?err,
                        "Disk local check_parts state changed"
                    );

                    let e: DiskError = to_file_error(err).into();

                    if e == DiskError::FileNotFound {
                        if !skip_access_checks(volume)
                            && let Err(err) = access(&volume_dir).await
                            && err.kind() == ErrorKind::NotFound
                        {
                            resp.results[i] = CHECK_PART_VOLUME_NOT_FOUND;
                            continue;
                        }
                        resp.results[i] = CHECK_PART_FILE_NOT_FOUND;
                    } else {
                        error!(
                            event = EVENT_DISK_LOCAL_CHECK_PARTS,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                            path = ?file_path,
                            part_number = part.number,
                            state = "file_stat_failed",
                            error = ?e,
                            "Disk local check_parts state changed"
                        );
                    }
                    continue;
                }
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn prepare_part_transaction(
        &self,
        src_volume: &str,
        src_path: &str,
        dst_volume: &str,
        dst_path: &str,
        meta: Bytes,
    ) -> Result<()> {
        let src_volume_dir = self.io_get_bucket_path(src_volume)?;
        let dst_volume_dir = self.io_get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            super::fs::access_std(&src_volume_dir).map_err(|err| to_access_error(err, DiskError::VolumeAccessDenied))?;
        }
        if !skip_access_checks(dst_volume) {
            super::fs::access_std(&dst_volume_dir).map_err(|err| to_access_error(err, DiskError::VolumeAccessDenied))?;
        }

        let src_file_path = self.io_get_object_path(src_volume, src_path)?;
        let dst_file_path = self.io_get_object_path(dst_volume, dst_path)?;
        let dst_meta_path = self.io_get_object_path(dst_volume, &format!("{dst_path}.meta"))?;
        let transaction_path = self.io_get_object_path(dst_volume, &crate::disk::part_transaction_path(dst_path))?;
        for path in [&src_file_path, &dst_file_path, &dst_meta_path, &transaction_path] {
            check_path_length(path.to_string_lossy().as_ref())?;
        }

        let durability = effective_durability(dst_volume);
        tokio::task::spawn_blocking(move || {
            let source = std::fs::symlink_metadata(&src_file_path).map_err(to_file_error)?;
            if !source.is_file() {
                return Err(DiskError::FileAccessDenied);
            }
            if transaction_path.exists() {
                return Err(DiskError::FileAccessDenied);
            }

            let Some(transaction_parent) = transaction_path.parent() else {
                return Err(DiskError::InvalidPath);
            };
            std::fs::create_dir_all(transaction_parent).map_err(to_file_error)?;
            let staging_path = transaction_parent.join(format!(".part-txn-{}", Uuid::new_v4()));
            std::fs::create_dir(&staging_path).map_err(to_file_error)?;

            let prepare_result = (|| -> std::io::Result<()> {
                snapshot_part_transaction_file(
                    &dst_file_path,
                    &staging_path.join(PART_TRANSACTION_OLD_DATA),
                    &staging_path.join(PART_TRANSACTION_OLD_DATA_ABSENT),
                )?;
                snapshot_part_transaction_file(
                    &dst_meta_path,
                    &staging_path.join(PART_TRANSACTION_OLD_META),
                    &staging_path.join(PART_TRANSACTION_OLD_META_ABSENT),
                )?;

                let mut new_meta = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(staging_path.join(PART_TRANSACTION_NEW_META))?;
                std::io::Write::write_all(&mut new_meta, &meta)?;
                if durability.syncs_commit_metadata() {
                    new_meta.sync_data()?;
                }
                // Windows rejects renaming a directory while one of its children is
                // still open, even when the child handle shares delete access.
                drop(new_meta);
                if durability.syncs_commit_metadata() {
                    os::fsync_dir_std(&staging_path)?;
                }
                std::fs::rename(&staging_path, &transaction_path)?;
                if durability.syncs_commit_metadata() {
                    os::fsync_dir_std(transaction_parent)?;
                }
                Ok(())
            })();

            if let Err(err) = prepare_result {
                let _ = remove_dir_all_if_exists(&staging_path);
                return Err(to_file_error(err).into());
            }

            Ok(())
        })
        .await
        .map_err(DiskError::from)?
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn settle_part_transaction(&self, volume: &str, path: &str, action: PartTransactionAction) -> Result<()> {
        self.io_get_bucket_path(volume)?;
        let current_data_path = self.io_get_object_path(volume, path)?;
        let current_meta_path = self.io_get_object_path(volume, &format!("{path}.meta"))?;
        let transaction_path = self.io_get_object_path(volume, &crate::disk::part_transaction_path(path))?;
        for candidate in [&current_data_path, &current_meta_path, &transaction_path] {
            check_path_length(candidate.to_string_lossy().as_ref())?;
        }
        let durability = effective_durability(volume);

        tokio::task::spawn_blocking(move || {
            match std::fs::symlink_metadata(&transaction_path) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => return Err(DiskError::FileCorrupt),
                Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
                Err(err) => return Err(to_file_error(err).into()),
            }

            if action == PartTransactionAction::Rollback {
                std::fs::write(transaction_path.join(PART_TRANSACTION_ROLLBACK), []).map_err(to_file_error)?;
                if durability.syncs_commit_metadata() {
                    os::fsync_dir_std(&transaction_path).map_err(to_file_error)?;
                }
                restore_part_transaction_file(
                    &current_data_path,
                    &transaction_path.join(PART_TRANSACTION_OLD_DATA),
                    &transaction_path.join(PART_TRANSACTION_OLD_DATA_ABSENT),
                    &transaction_path.join("restore.data"),
                )
                .map_err(to_file_error)?;
                restore_part_transaction_file(
                    &current_meta_path,
                    &transaction_path.join(PART_TRANSACTION_OLD_META),
                    &transaction_path.join(PART_TRANSACTION_OLD_META_ABSENT),
                    &transaction_path.join("restore.meta"),
                )
                .map_err(to_file_error)?;
                if durability.syncs_commit_metadata()
                    && let Some(parent) = current_data_path.parent()
                {
                    os::fsync_dir_std(parent).map_err(to_file_error)?;
                }
            }

            let Some(parent) = transaction_path.parent() else {
                return Err(DiskError::InvalidPath);
            };
            let cleanup_path = parent.join(format!(".part-txn-settled-{}", Uuid::new_v4()));
            std::fs::rename(&transaction_path, &cleanup_path).map_err(to_file_error)?;
            if durability.syncs_commit_metadata() {
                os::fsync_dir_std(parent).map_err(to_file_error)?;
            }
            remove_dir_all_if_exists(&cleanup_path).map_err(to_file_error)?;
            Ok(())
        })
        .await
        .map_err(DiskError::from)??;

        self.io_backend.invalidate_cached_fd(volume, path).await;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()> {
        let src_volume_dir = self.io_get_bucket_path(src_volume)?;
        let dst_volume_dir = self.io_get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            super::fs::access_std(&src_volume_dir).map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?
        }
        if !skip_access_checks(dst_volume) {
            super::fs::access_std(&dst_volume_dir).map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?
        }

        let src_is_dir = has_suffix(src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR);

        if !src_is_dir && dst_is_dir || src_is_dir && !dst_is_dir {
            warn!(
                event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                reason = "src_dst_type_mismatch",
                src_is_dir,
                dst_is_dir,
                "Disk local rename rejected"
            );
            return Err(DiskError::FileAccessDenied);
        }

        let src_file_path = self.io_get_object_path(src_volume, src_path)?;
        let dst_file_path = self.io_get_object_path(dst_volume, dst_path)?;

        // warn!("rename_part src_file_path:{:?}, dst_file_path:{:?}", &src_file_path, &dst_file_path);

        check_path_length(src_file_path.to_string_lossy().as_ref())?;
        check_path_length(dst_file_path.to_string_lossy().as_ref())?;

        if src_is_dir {
            let meta_op = match lstat_std(&src_file_path).map_err(|e| to_file_error(e).into()) {
                Ok(meta) => Some(meta),
                Err(e) => {
                    return Err(e);
                }
            };

            if let Some(meta) = meta_op
                && !meta.is_dir()
            {
                warn!(
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "src_expected_dir_missing",
                    path = ?src_file_path,
                    "Disk local rename rejected"
                );
                return Err(DiskError::FileAccessDenied);
            }

            // Clear any stale destination before the directory rename. An absent
            // destination is the normal case when renaming a directory to a new
            // location, so tolerate NotFound instead of aborting the whole rename
            // (MinIO's RenameFile ignores osIsNotExist here).
            if let Err(e) = remove_std(&dst_file_path)
                && e.kind() != ErrorKind::NotFound
            {
                return Err(to_file_error(e).into());
            }
        } else {
            let meta = lstat_std(&src_file_path).map_err(|e| -> DiskError { to_file_error(e).into() })?;
            if meta.is_dir() {
                warn!(
                    event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    reason = "src_unexpected_dir",
                    path = ?src_file_path,
                    "Disk local rename rejected"
                );
                return Err(DiskError::FileAccessDenied);
            }
        }

        let transaction_publish_meta = if src_is_dir {
            None
        } else {
            let transaction_path = self.io_get_object_path(dst_volume, &crate::disk::part_transaction_path(dst_path))?;
            let transaction_meta_path = transaction_path.join(PART_TRANSACTION_NEW_META);
            match fs::read(&transaction_meta_path).await {
                Ok(expected_meta) => {
                    if expected_meta.as_slice() != meta.as_ref() {
                        return Err(DiskError::FileCorrupt);
                    }

                    let publish_meta_path = transaction_path.join(PART_TRANSACTION_PUBLISH_META);
                    let source_meta_path = transaction_meta_path.clone();
                    let publish_path = publish_meta_path.clone();
                    tokio::task::spawn_blocking(move || {
                        remove_file_if_exists(&publish_path)?;
                        std::fs::hard_link(source_meta_path, &publish_path)
                    })
                    .await
                    .map_err(DiskError::from)?
                    .map_err(to_file_error)?;
                    Some(publish_meta_path)
                }
                // Old peers know only RenamePart. The new coordinator never
                // reaches rename unless prepare succeeded, so an absent
                // transaction directory identifies the rolling-upgrade legacy
                // path. A present directory without new.meta is corruption.
                Err(err) if err.kind() == ErrorKind::NotFound => match fs::metadata(&transaction_path).await {
                    Ok(_) => return Err(DiskError::FileCorrupt),
                    Err(meta_err) if meta_err.kind() == ErrorKind::NotFound => None,
                    Err(meta_err) => return Err(to_file_error(meta_err).into()),
                },
                Err(err) => return Err(to_file_error(err).into()),
            }
        };

        // UploadPart is acknowledged once this rename lands, so the part data and
        // its directory entry must be durable before we return. Relaxed keeps the
        // part payload fdatasync but leaves the directory entry to the page cache.
        let durability = effective_durability(dst_volume);
        if durability.syncs_data_shards() && !src_is_dir {
            let src = src_file_path.clone();
            tokio::task::spawn_blocking(move || os::sync_file(&src))
                .await
                .map_err(DiskError::from)?
                .map_err(to_file_error)?;
        }

        rename_all(&src_file_path, &dst_file_path, &dst_volume_dir, &self.publication_root).await?;

        if durability.syncs_commit_metadata()
            && let Some(parent) = dst_file_path.parent()
        {
            os::fsync_dir(parent).await.map_err(to_file_error)?;
        }

        let dst_meta = lstat_std(&dst_file_path).map_err(|e| -> DiskError { to_file_error(e).into() })?;
        if src_is_dir != dst_meta.is_dir() {
            warn!(
                event = EVENT_DISK_LOCAL_RENAME_REJECTED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                reason = "dst_type_changed_after_rename",
                path = ?dst_file_path,
                "Disk local rename rejected"
            );
            return Err(DiskError::FileAccessDenied);
        }

        if let Some(transaction_publish_meta) = transaction_publish_meta {
            let dst_meta_path = self.io_get_object_path(dst_volume, &format!("{dst_path}.meta"))?;
            rename_all(&transaction_publish_meta, &dst_meta_path, &dst_volume_dir, &self.publication_root).await?;
            if durability.syncs_commit_metadata()
                && let Some(parent) = dst_meta_path.parent()
            {
                os::fsync_dir(parent).await.map_err(to_file_error)?;
            }
        } else {
            self.write_all(dst_volume, format!("{dst_path}.meta").as_str(), meta).await?;
        }

        if let Some(parent) = src_file_path.parent() {
            self.delete_file(&src_volume_dir, &parent.to_path_buf(), false, false).await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        crate::hp_guard!("LocalDisk::rename_file");
        let src_volume_dir = self.io_get_bucket_path(src_volume)?;
        let dst_volume_dir = self.io_get_bucket_path(dst_volume)?;
        if !skip_access_checks(src_volume) {
            access(&src_volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }
        if !skip_access_checks(dst_volume) {
            access(&dst_volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        let src_is_dir = has_suffix(src_path, SLASH_SEPARATOR);
        let dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR);
        if (dst_is_dir || src_is_dir) && (!dst_is_dir || !src_is_dir) {
            return Err(Error::from(DiskError::FileAccessDenied));
        }

        let src_file_path = self.io_get_object_path(src_volume, src_path)?;
        check_path_length(src_file_path.to_string_lossy().as_ref())?;

        let dst_file_path = self.io_get_object_path(dst_volume, dst_path)?;
        check_path_length(dst_file_path.to_string_lossy().as_ref())?;

        if src_is_dir {
            let meta_op = match lstat(&src_file_path).await {
                Ok(meta) => Some(meta),
                Err(e) => {
                    let e: DiskError = to_file_error(e).into();
                    if e != DiskError::FileNotFound {
                        return Err(e);
                    } else {
                        None
                    }
                }
            };

            if let Some(meta) = meta_op
                && !meta.is_dir()
            {
                return Err(DiskError::FileAccessDenied);
            }

            // Clear any stale destination before the directory rename. An absent
            // destination is the normal case when renaming a directory to a new
            // location, so tolerate NotFound instead of aborting the whole rename
            // (MinIO's RenameFile ignores osIsNotExist here).
            if let Err(e) = remove(&dst_file_path).await
                && e.kind() != ErrorKind::NotFound
            {
                return Err(to_file_error(e).into());
            }
        }

        rename_all(&src_file_path, &dst_file_path, &dst_volume_dir, &self.publication_root).await?;

        // Both ends changed identity: the source path no longer exists and the
        // destination now resolves to a different inode (backlog#1145).
        self.io_backend.invalidate_cached_fd(src_volume, src_path).await;
        self.io_backend.invalidate_cached_fd(dst_volume, dst_path).await;

        if let Some(parent) = src_file_path.parent() {
            let _ = self.delete_file(&src_volume_dir, &parent.to_path_buf(), false, false).await;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn create_file(&self, origvolume: &str, volume: &str, path: &str, _file_size: i64) -> Result<FileWriter> {
        crate::hp_guard!("LocalDisk::create_file");
        if !origvolume.is_empty() {
            let origvolume_dir = self.io_get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume) {
                access(origvolume_dir)
                    .await
                    .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
            }
        }

        self.io_backend
            .open_write(volume, path, WriteMode::Truncate { size_hint: _file_size })
            .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    // async fn append_file(&self, volume: &str, path: &str, mut r: DuplexStream) -> Result<File> {
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        self.io_backend.open_write(volume, path, WriteMode::Append).await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        crate::hp_guard!("LocalDisk::read_file");
        self.io_backend.open_full_read(volume, path).await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        crate::hp_guard!("LocalDisk::read_file_stream");
        self.io_backend.open_read_stream(volume, path, offset, length).await
    }

    /// File read using mmap-then-copy on Unix or efficient read on non-Unix.
    // SAFETY: Unix unsafe calls in this function only query page size and mmap
    // a read-only file region after bounds and alignment are validated.
    #[allow(unsafe_code)]
    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_file_mmap_copy(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<Bytes> {
        self.read_file_mmap_copy_with_metrics(volume, path, offset, length, None)
            .await
    }

    /// File read using mmap-then-copy on Unix or efficient read on non-Unix.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_file_mmap_copy_with_metrics(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
        metrics: Option<MmapCopyStageMetrics>,
    ) -> Result<Bytes> {
        self.io_backend.pread_bytes(volume, path, offset, length, metrics).await
    }

    /// List a single directory. `count < 0` enumerates the *whole* directory in
    /// one `os::read_dir` call.
    ///
    /// Wide-directory stall hazard (rustfs/backlog#1216, a #2999 sub-class):
    /// the walk caller wraps this whole call in the per-read stall budget
    /// (`with_walk_stall_timeout`, default 5s via
    /// `RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS`) as if the entire directory
    /// enumeration were a single read. For a *wide, flat* directory -- one
    /// bucket prefix holding millions of immediate children -- a single
    /// `readdir` of the whole directory can itself exceed the stall budget on a
    /// healthy disk. That trips `DiskError::Timeout`, which the listing path can
    /// escalate to a quorum failure and surface to the client as a ListObjects
    /// 500, even though nothing is actually wrong with the drive.
    ///
    /// This path keeps the legacy one-shot call contract and does not attempt
    /// per-entry timeout segmentation for compatibility reasons. Wide prefix
    /// listing workarounds therefore remain operational: raise
    /// `RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS` or use the
    /// high-latency drive-timeout profile from
    /// `get_drive_walkdir_stall_timeout`.
    ///
    /// This is deliberately NOT fixed here by rewriting this one-shot
    /// `os::read_dir` into a streaming/batched readdir with per-chunk timeout
    /// refresh: that is an architecture-level change with high regression
    /// surface (ordering, the `count` contract, quorum merge semantics) and is
    /// tracked as a separate follow-up. Object listing now uses
    /// `scan_dir` + `read_dir_entries_with_walk_stall` to keep wide-prefix
    /// listing resilient while preserving existing full-enumeration behavior.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn list_dir(&self, origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        if !origvolume.is_empty() {
            let origvolume_dir = self.io_get_bucket_path(origvolume)?;
            if !skip_access_checks(origvolume)
                && let Err(e) = access(origvolume_dir).await
            {
                return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
            }
        }

        let volume_dir = self.io_get_bucket_path(volume)?;
        let dir_path_abs = self.io_get_object_path(volume, dir_path.trim_start_matches(SLASH_SEPARATOR))?;

        // Whole-directory enumeration in one syscall path (see the wide-directory
        // stall hazard on this fn): with `count < 0` this reads every entry, and
        // the caller's stall budget bounds the entire call as a unit.
        let entries = match os::read_dir(&dir_path_abs, count).await {
            Ok(res) => res,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound
                    && !skip_access_checks(volume)
                    && let Err(e) = access(&volume_dir).await
                {
                    return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
                }

                return Err(to_file_error(e).into());
            }
        };

        Ok(entries)
    }

    // TODO(backlog): support io.writer cancellation and early termination in walk_dir
    #[tracing::instrument(level = "trace", skip_all)]
    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        self.wait_for_startup_cleanup().await;

        // Callers that do not pin a stall budget inherit the configured default, so
        // every local walk keeps a liveness bound even when the wrapper-level total
        // timeout is skipped.
        let mut opts = opts;
        if opts.stall_timeout_ms.is_none() {
            opts.stall_timeout_ms = Some(duration_millis(get_drive_walkdir_stall_timeout()));
        }
        let stall = opts.stall_timeout_duration();

        let volume_dir = self.io_get_bucket_path(&opts.bucket)?;

        if !skip_access_checks(&opts.bucket)
            && let Err(e) = with_walk_stall_deadline(stall, access(&volume_dir)).await?
        {
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        let mut wr = wr;

        let mut out = MetacacheWriter::new(&mut wr);

        let mut objs_returned = 0;

        let mut skip_current_dir_object = false;
        let mut multipart_dir_to_skip: HashSet<String> = HashSet::new();
        if opts.base_dir.ends_with(SLASH_SEPARATOR) {
            if let Ok(data) = with_walk_stall_timeout(
                stall,
                self.read_metadata(
                    &opts.bucket,
                    path_join_buf(&[
                        format!("{}{}", opts.base_dir.trim_end_matches(SLASH_SEPARATOR), GLOBAL_DIR_SUFFIX).as_str(),
                        STORAGE_FORMAT_FILE,
                    ])
                    .as_str(),
                ),
            )
            .await
            {
                let meta = MetaCacheEntry {
                    name: opts.base_dir.clone(),
                    metadata: data.to_vec(),
                    ..Default::default()
                };
                write_metacache_obj(&mut out, &meta).await?;
                objs_returned += 1;
            } else {
                let fpath = self
                    .io_get_object_path(&opts.bucket, path_join_buf(&[opts.base_dir.as_str(), STORAGE_FORMAT_FILE]).as_str())?;

                if let Ok(meta) = with_walk_stall_deadline(stall, tokio::fs::metadata(&fpath)).await?
                    && meta.is_file()
                {
                    skip_current_dir_object = true;
                    if let Ok(meta_bytes) = with_walk_stall_deadline(
                        stall,
                        self.read_metadata(
                            opts.bucket.as_str(),
                            path_join_buf(&[opts.base_dir.as_str(), STORAGE_FORMAT_FILE]).as_str(),
                        ),
                    )
                    .await?
                        && let Ok(file_meta) = FileMeta::load(&meta_bytes)
                        && let Ok(data_dirs) = file_meta.get_data_dirs()
                    {
                        for data_dir in data_dirs.iter().flatten() {
                            multipart_dir_to_skip.insert(data_dir.to_string());
                        }
                    }
                }
            }
        }

        self.scan_dir(
            opts.base_dir.clone(),
            opts.filter_prefix.clone().unwrap_or_default(),
            &opts,
            &mut out,
            &mut objs_returned,
            skip_current_dir_object,
            if multipart_dir_to_skip.is_empty() {
                None
            } else {
                Some(multipart_dir_to_skip)
            },
        )
        .await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
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
            && let Err(e) = super::fs::access_std(&src_volume_dir)
        {
            info!(
                event = EVENT_DISK_LOCAL_ACCESS_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                path = ?src_volume_dir,
                operation = "rename_data_src_access",
                error = %e,
                "Disk local access check failed"
            );
            return Err(to_access_error(e, DiskError::VolumeAccessDenied).into());
        }

        let dst_volume_dir = self.io_get_bucket_path(dst_volume)?;
        if !skip_access_checks(dst_volume)
            && let Err(e) = super::fs::access_std(&dst_volume_dir)
        {
            info!(
                event = EVENT_DISK_LOCAL_ACCESS_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                path = ?dst_volume_dir,
                operation = "rename_data_dst_access",
                error = %e,
                "Disk local access check failed"
            );
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
                if src_volume != super::RUSTFS_META_MULTIPART_BUCKET {
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
            let cleanup_path = if src_volume == super::RUSTFS_META_MULTIPART_BUCKET {
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

    #[tracing::instrument(level = "trace", skip_all)]
    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        for vol in volumes {
            if let Err(e) = self.make_volume(vol).await
                && e != DiskError::VolumeExists
            {
                error!(
                    event = EVENT_DISK_LOCAL_VOLUME_SETUP_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                    volume = vol,
                    operation = "make_volumes",
                    error = %e,
                    "Disk local volume setup failed"
                );
                return Err(e);
            }
            // TODO(backlog): add post-setup disk health verification
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn make_volume(&self, volume: &str) -> Result<()> {
        if !Self::is_valid_volname(volume) {
            return Err(Error::other("Invalid arguments specified"));
        }

        let volume_dir = self.io_get_bucket_path(volume)?;

        if let Err(e) = access(&volume_dir).await {
            if e.kind() == ErrorKind::NotFound {
                os::make_dir_all(&volume_dir, self.io_root()).await?;
                return Ok(());
            }
            error!(
                event = EVENT_DISK_LOCAL_VOLUME_SETUP_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                volume,
                operation = "make_volume",
                error = %e,
                "Disk local volume setup failed"
            );
            return Err(to_volume_error(e).into());
        }

        Err(DiskError::VolumeExists)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let mut volumes = Vec::new();

        let entries = os::read_dir(self.io_root(), -1).await.map_err(to_volume_error)?;

        for entry in entries {
            if !has_suffix(&entry, SLASH_SEPARATOR) || !Self::is_valid_volname(clean(&entry).as_str()) {
                continue;
            }

            volumes.push(VolumeInfo {
                name: clean(&entry),
                created: None,
            });
        }

        Ok(volumes)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        let volume_dir = self.io_get_bucket_path(volume)?;
        let meta = lstat(&volume_dir).await.map_err(to_volume_error)?;

        let modtime = match meta.modified() {
            Ok(md) => Some(OffsetDateTime::from(md)),
            Err(_) => None,
        };

        Ok(VolumeInfo {
            name: volume.to_string(),
            created: modtime,
        })
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        let volume_dir = self.io_get_bucket_path(volume)?;
        if !skip_access_checks(volume) {
            access(&volume_dir)
                .await
                .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
        }

        for path in paths.iter() {
            let file_path = self.io_get_object_path(volume, path)?;

            check_path_length(file_path.to_string_lossy().as_ref())?;

            self.move_to_trash(&file_path, false, false).await?;

            // A cached io_uring descriptor under a just-removed path would keep
            // its inode readable; drop it (rustfs/backlog#1175).
            self.io_backend.invalidate_cached_fds_under(volume, path);
        }

        Ok(())
    }

    async fn acquire_snapshot_lease(&self, volume: &str, path: &str) -> Result<SnapshotLeaseToken> {
        let key = SnapshotLeaseKey {
            volume: volume.to_string(),
            path: path.to_string(),
        };
        if volume == RUSTFS_META_BUCKET && is_quota_mutation_fence_path(path) {
            let mut registry = self.snapshot_leases.lock().await;
            let entry = registry.entries.entry(key).or_default();
            let state = entry
                .mutation_fence
                .get_or_insert_with(|| Arc::new(QuotaMutationFenceState::default()));
            if state.revoked.load(Ordering::Acquire) {
                return Err(DiskError::FileNotFound);
            }
            let token = SnapshotLeaseToken::new();
            entry.tokens.insert(token);
            return Ok(token);
        }

        let file_path = self.io_get_object_path(volume, path)?;
        let _mutation_lease = os::acquire_rename_data_mutation_lease(&self.root, volume, &file_path).await;
        let token = {
            let mut registry = self.snapshot_leases.lock().await;
            if registry.entries.get(&key).is_some_and(|entry| entry.deleting) {
                return Err(DiskError::FileNotFound);
            }
            let token = SnapshotLeaseToken::new();
            registry.entries.entry(key).or_default().tokens.insert(token);
            token
        };
        match fs::metadata(file_path).await {
            Ok(metadata) if metadata.is_dir() => Ok(token),
            Ok(_) => {
                self.release_snapshot_lease(volume, path, token).await?;
                Err(DiskError::FileNotFound)
            }
            Err(err) => {
                self.release_snapshot_lease(volume, path, token).await?;
                Err(to_file_error(err).into())
            }
        }
    }

    async fn release_snapshot_lease(&self, volume: &str, path: &str, token: SnapshotLeaseToken) -> Result<()> {
        let key = SnapshotLeaseKey {
            volume: volume.to_string(),
            path: path.to_string(),
        };
        if volume == RUSTFS_META_BUCKET && is_quota_mutation_fence_path(path) {
            if !token.is_revoke_all() {
                let mut registry = self.snapshot_leases.lock().await;
                let Some(entry) = registry.entries.get_mut(&key) else {
                    return Ok(());
                };
                entry.tokens.remove(&token);
                let removable = entry.tokens.is_empty()
                    && entry
                        .mutation_fence
                        .as_ref()
                        .is_none_or(|state| state.running.load(Ordering::Acquire) == 0);
                if removable {
                    registry.entries.remove(&key);
                }
                return Ok(());
            }
            let state = {
                let mut registry = self.snapshot_leases.lock().await;
                let Some(entry) = registry.entries.get_mut(&key) else {
                    return Ok(());
                };
                let Some(state) = entry.mutation_fence.as_ref().cloned() else {
                    registry.entries.remove(&key);
                    return Ok(());
                };
                state.revoked.store(true, Ordering::Release);
                entry.tokens.clear();
                state
            };
            loop {
                let notified = state.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if state.running.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
            self.snapshot_leases.lock().await.entries.remove(&key);
            return Ok(());
        }
        let opts = {
            let mut registry = self.snapshot_leases.lock().await;
            let Some(entry) = registry.entries.get_mut(&key) else {
                return Ok(());
            };
            entry.tokens.remove(&token);
            if !entry.tokens.is_empty() || entry.deleting {
                return Ok(());
            }

            let Some(opts) = entry.pending_delete.clone() else {
                registry.entries.remove(&key);
                return Ok(());
            };
            entry.deleting = true;
            opts
        };
        let result = self.delete_unleased(volume, path, &opts).await;
        let mut registry = self.snapshot_leases.lock().await;
        match result {
            Ok(()) => {
                registry.entries.remove(&key);
                Ok(())
            }
            Err(err) => {
                if let Some(entry) = registry.entries.get_mut(&key) {
                    entry.deleting = false;
                }
                Err(err)
            }
        }
    }

    async fn renew_snapshot_lease(&self, volume: &str, path: &str, token: SnapshotLeaseToken) -> Result<SnapshotLeaseToken> {
        let key = SnapshotLeaseKey {
            volume: volume.to_string(),
            path: path.to_string(),
        };
        let mut registry = self.snapshot_leases.lock().await;
        let Some(entry) = registry.entries.get_mut(&key) else {
            return Err(DiskError::FileNotFound);
        };
        if entry.deleting || !entry.tokens.remove(&token) {
            return Err(DiskError::FileNotFound);
        }
        let renewed = SnapshotLeaseToken::new();
        entry.tokens.insert(renewed);
        Ok(renewed)
    }

    async fn delete_data_dir(&self, volume: &str, path: &str, opts: DeleteOptions) -> Result<DataDirDeleteStatus> {
        let key = SnapshotLeaseKey {
            volume: volume.to_string(),
            path: path.to_string(),
        };
        {
            let mut registry = self.snapshot_leases.lock().await;
            if let Some(entry) = registry.entries.get_mut(&key) {
                if !entry.tokens.is_empty() {
                    entry.pending_delete.get_or_insert_with(|| opts.clone());
                    return Ok(DataDirDeleteStatus::Deferred);
                }
                if entry.deleting {
                    entry.pending_delete.get_or_insert_with(|| opts.clone());
                    return Ok(DataDirDeleteStatus::Deferred);
                }
                entry.deleting = true;
                entry.pending_delete.get_or_insert_with(|| opts.clone());
            } else {
                registry.entries.insert(
                    key.clone(),
                    SnapshotLeaseEntry {
                        pending_delete: Some(opts.clone()),
                        deleting: true,
                        ..Default::default()
                    },
                );
            }
        }

        let result = self.delete_unleased(volume, path, &opts).await;
        let mut registry = self.snapshot_leases.lock().await;
        match result {
            Ok(()) => {
                registry.entries.remove(&key);
                Ok(DataDirDeleteStatus::Deleted)
            }
            Err(err) => {
                if let Some(entry) = registry.entries.get_mut(&key) {
                    entry.deleting = false;
                }
                Err(err)
            }
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        if !fi.metadata.is_empty() {
            let file_path = self.io_get_object_path(volume, path)?;

            check_path_length(file_path.to_string_lossy().as_ref())?;

            let buf = self
                .read_all(volume, format!("{}/{}", path, STORAGE_FORMAT_FILE).as_str())
                .await
                .map_err(|e| {
                    if e == DiskError::FileNotFound && fi.version_id.is_some() {
                        DiskError::FileVersionNotFound
                    } else {
                        e
                    }
                })?;

            if !FileMeta::is_xl2_v1_format(buf.as_ref()) {
                return Err(DiskError::FileVersionNotFound);
            }

            let mut xl_meta = FileMeta::load(buf.as_ref())?;

            xl_meta.update_object_version_with_opts(fi, opts.replace_user_metadata)?;

            let wbuf = xl_meta.marshal_msg()?;

            return self
                .write_all_meta(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str(), &wbuf, !opts.no_persistence)
                .await;
        }

        Err(Error::other("Invalid Argument"))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        crate::hp_guard!("LocalDisk::write_metadata");
        fi.validate_for_metadata_read()?;
        let p = self.io_get_object_path(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str())?;

        let mut meta = FileMeta::new();
        if !fi.fresh {
            let (buf, _) = read_file_exists(&p).await?;
            if !buf.is_empty() {
                let _ = meta.unmarshal_msg(&buf).map_err(|_| {
                    meta = FileMeta::new();
                });
            }
        }

        meta.add_version(fi)?;

        let fm_data = meta.marshal_msg()?;

        // Atomic temp+rename: this path also rewrites live xl.meta (delete markers,
        // decommission), where an in-place truncate would expose torn metadata.
        self.write_all_meta(volume, format!("{path}/{STORAGE_FORMAT_FILE}").as_str(), &fm_data, true)
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_version(
        &self,
        org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo> {
        crate::hp_guard!("LocalDisk::read_version");
        let stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        let metrics_path = if stage_metrics_enabled && crate::bucket::utils::is_meta_bucketname(volume) {
            GET_OBJECT_PATH_INTERNAL_META
        } else {
            GET_OBJECT_PATH_LEGACY_DUPLEX
        };
        if !org_volume.is_empty() {
            let org_volume_path = self.io_get_bucket_path(org_volume)?;
            if !skip_access_checks(org_volume) {
                access(&org_volume_path)
                    .await
                    .map_err(|e| to_access_error(e, DiskError::VolumeAccessDenied))?;
            }
        }

        let path_resolve_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let file_path = self.io_get_object_path(volume, path)?;
        let volume_dir = self.io_get_bucket_path(volume)?;
        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_READ_VERSION_PATH_RESOLVE, path_resolve_start);

        let path_check_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        check_path_length(file_path.to_string_lossy().as_ref())?;
        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_READ_VERSION_PATH_CHECK, path_check_start);

        let read_data = opts.read_data;

        let xlmeta_read_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let raw_read_result = self.read_raw(volume, volume_dir.clone(), file_path, read_data).await;
        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_READ_VERSION_XLMETA_READ, xlmeta_read_start);
        let (data, _) = raw_read_result.map_err(|e| {
            if e == DiskError::FileNotFound && !version_id.is_empty() {
                DiskError::FileVersionNotFound
            } else {
                e
            }
        })?;

        let decode_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let file_info_result: Result<FileInfo> = (|| {
            let fi = get_file_info(
                &data,
                volume,
                path,
                version_id,
                FileInfoOpts {
                    data: read_data,
                    include_free_versions: opts.incl_free_versions,
                    include_part_checksums: false,
                },
            )?;
            fi.validate_for_metadata_read()?;
            Ok(fi)
        })();
        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_READ_VERSION_DECODE, decode_start);
        let mut fi = file_info_result?;
        if fi.is_canonical_delete_marker() {
            return Ok(fi);
        }

        if opts.read_data {
            if fi.data.as_ref().is_some_and(|d| !d.is_empty()) || fi.size == 0 {
                if fi.inline_data() {
                    return Ok(fi);
                }

                if fi.size == 0 || fi.version_id.is_none_or(|v| v.is_nil()) {
                    fi.set_inline_data();
                    return Ok(fi);
                };
                if let Some(part) = fi.parts.first() {
                    let part_path = format!("part.{}", part.number);
                    let part_path = path_join_buf(&[
                        path,
                        fi.data_dir.map_or_else(|| "".to_string(), |dir| dir.to_string()).as_str(),
                        part_path.as_str(),
                    ]);
                    let part_path = self.io_get_object_path(volume, part_path.as_str())?;
                    if lstat(&part_path).await.is_err() {
                        fi.set_inline_data();
                        return Ok(fi);
                    }
                }

                fi.data = None;
            }

            let inline = fi.transition_status.is_empty() && fi.data_dir.is_some() && fi.parts.len() == 1;
            if inline && fi.shard_file_size(fi.parts[0].actual_size) < DEFAULT_INLINE_BLOCK as i64 {
                let part_path = path_join_buf(&[
                    path,
                    fi.data_dir.map_or_else(|| "".to_string(), |dir| dir.to_string()).as_str(),
                    format!("part.{}", fi.parts[0].number).as_str(),
                ]);
                let part_path = self.io_get_object_path(volume, part_path.as_str())?;

                let data = self.read_all_data(volume, volume_dir, part_path.clone()).await.map_err(|e| {
                    warn!(
                        event = EVENT_DISK_LOCAL_READ_VERSION_FALLBACK,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                        path = ?part_path,
                        reason = "inline_data_read_failed",
                        error = %e,
                        "Disk local read_version fallback failed"
                    );
                    e
                })?;
                fi.data = Some(Bytes::from(data));
            }
        }

        Ok(fi)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        crate::hp_guard!("LocalDisk::read_xl");
        let file_path = self.io_get_object_path(volume, path)?;
        let file_dir = self.io_get_bucket_path(volume)?;

        let (buf, _) = self.read_raw(volume, file_dir, file_path, read_data).await?;

        Ok(RawFileInfo { buf })
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        if path.starts_with(SLASH_SEPARATOR) {
            return self
                .delete(
                    volume,
                    path,
                    DeleteOptions {
                        recursive: false,
                        immediate: false,
                        ..Default::default()
                    },
                )
                .await;
        }

        let volume_dir = self.io_get_bucket_path(volume)?;

        let file_path = self.io_get_object_path(volume, path)?;

        check_path_length(file_path.to_string_lossy().as_ref())?;

        let xl_path = path_join(&[file_path.as_path(), Path::new(STORAGE_FORMAT_FILE)]);
        if let Some(old_data_dir) = opts.old_data_dir
            && opts.undo_write
        {
            if opts.undo_delete {
                restore_delete_rollback(file_path.as_path(), &xl_path, old_data_dir, &self.publication_root).await?;
            } else {
                restore_metadata_backup(file_path.as_path(), &xl_path, old_data_dir, &self.publication_root).await?;
            }

            if !opts.undo_delete
                && let Some(new_data_dir) = fi.data_dir
            {
                let new_data_path = path_join(&[file_path.as_path(), Path::new(new_data_dir.to_string().as_str())]);
                check_path_length(new_data_path.to_string_lossy().as_ref())?;
                if let Err(err) = self.move_to_trash(&new_data_path, true, false).await
                    && err != DiskError::FileNotFound
                    && err != DiskError::VolumeNotFound
                {
                    return Err(err);
                }
            }

            return Ok(());
        }

        let rollback_dir = opts.old_data_dir;
        let buf = match self.read_all_data(volume, &volume_dir, &xl_path).await {
            Ok(res) => res,
            Err(err) => {
                if err != DiskError::FileNotFound {
                    return Err(err);
                }

                if fi.deleted && force_del_marker {
                    return self
                        .write_missing_delete_marker(volume, path, fi, file_path.as_path(), &xl_path, rollback_dir)
                        .await;
                }

                return if fi.version_id.is_some() {
                    Err(DiskError::FileVersionNotFound)
                } else {
                    Err(DiskError::FileNotFound)
                };
            }
        };

        let mut meta = FileMeta::load(&buf)?;
        let old_dir = meta.delete_version(&fi)?;
        let mut reserved_version_delete = false;
        if let Some(rollback_dir) = rollback_dir {
            write_metadata_rollback_backup(file_path.as_path(), rollback_dir, &buf).await?;
        }

        if let Some(uuid) = old_dir {
            let vid = fi.version_id.unwrap_or_default();
            if let Err(err) = meta.data.remove(vec![vid, uuid]) {
                let err: DiskError = err.into();
                return Err(restore_delete_rollback_after_error(
                    file_path.as_path(),
                    &xl_path,
                    rollback_dir,
                    volume,
                    path,
                    DeleteRollbackFailure {
                        stage: "delete_version_metadata_update",
                        error: err,
                    },
                    &self.publication_root,
                )
                .await);
            }

            let old_path = path_join(&[file_path.as_path(), Path::new(uuid.to_string().as_str())]);
            if let Err(err) = check_path_length(old_path.to_string_lossy().as_ref()) {
                return Err(restore_delete_rollback_after_error(
                    file_path.as_path(),
                    &xl_path,
                    rollback_dir,
                    volume,
                    path,
                    DeleteRollbackFailure {
                        stage: "delete_version_data_path",
                        error: err,
                    },
                    &self.publication_root,
                )
                .await);
            }

            if let Some(rollback_dir) = rollback_dir {
                let rollback_path = file_path.join(rollback_dir.to_string());
                if let Err(err) = fs::create_dir_all(&rollback_path).await {
                    let err: DiskError = to_file_error(err).into();
                    return Err(restore_delete_rollback_after_error(
                        file_path.as_path(),
                        &xl_path,
                        Some(rollback_dir),
                        volume,
                        path,
                        DeleteRollbackFailure {
                            stage: "delete_version_rollback_dir",
                            error: err,
                        },
                        &self.publication_root,
                    )
                    .await);
                }
                reserved_version_delete = match self.reserve_version_delete(volume, path, uuid, rollback_dir).await {
                    Ok(reserved) => reserved,
                    Err(err) => {
                        return Err(restore_delete_rollback_after_error(
                            file_path.as_path(),
                            &xl_path,
                            Some(rollback_dir),
                            volume,
                            path,
                            DeleteRollbackFailure {
                                stage: "delete_version_reserve_data",
                                error: err,
                            },
                            &self.publication_root,
                        )
                        .await);
                    }
                };
                let rollback_data_path = rollback_path.join(uuid.to_string());
                if !reserved_version_delete
                    && let Err(err) =
                        rename_all_ignore_missing_source(&old_path, &rollback_data_path, &rollback_path, &self.publication_root)
                            .await
                {
                    return Err(restore_delete_rollback_after_error(
                        file_path.as_path(),
                        &xl_path,
                        Some(rollback_dir),
                        volume,
                        path,
                        DeleteRollbackFailure {
                            stage: "delete_version_stage_data",
                            error: err,
                        },
                        &self.publication_root,
                    )
                    .await);
                }
                if should_fail_after_delete_data_staged(path) {
                    if reserved_version_delete {
                        return Err(self
                            .abort_reserved_version_delete(
                                file_path.as_path(),
                                rollback_dir,
                                volume,
                                path,
                                "delete_version_test_after_stage",
                                DiskError::Unexpected,
                            )
                            .await);
                    }
                    return Err(restore_delete_rollback_after_error(
                        file_path.as_path(),
                        &xl_path,
                        Some(rollback_dir),
                        volume,
                        path,
                        DeleteRollbackFailure {
                            stage: "delete_version_test_after_stage",
                            error: DiskError::Unexpected,
                        },
                        &self.publication_root,
                    )
                    .await);
                }
            } else if let Err(err) = self.move_to_trash(&old_path, true, false).await
                && err != DiskError::FileNotFound
                && err != DiskError::VolumeNotFound
            {
                return Err(err);
            }

            // The version's data dir was staged for rollback or trashed, so its
            // `part.N` inodes no longer exist for readers. A cached io_uring
            // descriptor would keep serving them, so drop every cached fd under
            // this data dir (rustfs/backlog#1175). If a later rollback restores
            // the dir, the next read simply re-opens it.
            self.io_backend.invalidate_cached_fds_under(volume, &format!("{path}/{uuid}"));
        }

        let commit_result = if !meta.versions.is_empty() {
            let buf = match meta.marshal_msg() {
                Ok(buf) => buf,
                Err(err) => {
                    let err: DiskError = err.into();
                    if reserved_version_delete && let Some(rollback_dir) = rollback_dir {
                        return Err(self
                            .abort_reserved_version_delete(
                                file_path.as_path(),
                                rollback_dir,
                                volume,
                                path,
                                "delete_version_metadata_encode",
                                err,
                            )
                            .await);
                    }
                    return Err(restore_delete_rollback_after_error(
                        file_path.as_path(),
                        &xl_path,
                        rollback_dir,
                        volume,
                        path,
                        DeleteRollbackFailure {
                            stage: "delete_version_metadata_encode",
                            error: err,
                        },
                        &self.publication_root,
                    )
                    .await);
                }
            };
            self.write_all_meta(volume, format!("{path}{SLASH_SEPARATOR}{STORAGE_FORMAT_FILE}").as_str(), &buf, true)
                .await
        } else {
            self.delete_file(&volume_dir, &xl_path, true, false).await
        };

        if let Err(err) = commit_result {
            if reserved_version_delete && let Some(rollback_dir) = rollback_dir {
                return Err(self
                    .abort_reserved_version_delete(file_path.as_path(), rollback_dir, volume, path, "delete_version_commit", err)
                    .await);
            }
            return Err(restore_delete_rollback_after_error(
                file_path.as_path(),
                &xl_path,
                rollback_dir,
                volume,
                path,
                DeleteRollbackFailure {
                    stage: "delete_version_commit",
                    error: err,
                },
                &self.publication_root,
            )
            .await);
        }

        if reserved_version_delete
            && let Some(rollback_dir) = rollback_dir
            && let Err(err) = self.commit_reserved_version_delete(volume, path, rollback_dir).await
        {
            return Err(self
                .abort_reserved_version_delete(
                    file_path.as_path(),
                    rollback_dir,
                    volume,
                    path,
                    "delete_version_commit_intent",
                    err,
                )
                .await);
        }

        if should_fail_after_delete_commit(self.root.as_path(), path) {
            return Err(DiskError::Unexpected);
        }

        Ok(())
    }
    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete_versions(&self, volume: &str, versions: Vec<FileInfoVersions>, opts: DeleteOptions) -> Vec<Option<Error>> {
        let mut errs = Vec::with_capacity(versions.len());
        for _ in 0..versions.len() {
            errs.push(None);
        }

        for (i, ver) in versions.iter().enumerate() {
            if let Err(e) = self
                .delete_versions_internal(volume, ver.name.as_str(), &ver.versions, &opts)
                .await
            {
                errs[i] = Some(e);
            } else {
                errs[i] = None;
            }
        }

        errs
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        let mut results = Vec::new();
        let mut found = 0;

        for v in req.files.iter() {
            let fpath = self.io_get_object_path(&req.bucket, format!("{}/{}", req.prefix, v).as_str())?;
            let mut res = ReadMultipleResp {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                file: v.clone(),
                ..Default::default()
            };

            // if req.metadata_only {}
            match read_file_all(&fpath).await {
                Ok((data, meta)) => {
                    found += 1;

                    if req.max_size > 0 && data.len() > req.max_size {
                        res.exists = true;
                        res.error = format!("max size ({}) exceeded: {}", req.max_size, data.len());
                        results.push(res);
                        break;
                    }

                    res.exists = true;
                    res.data = data.into();
                    res.mod_time = match meta.modified() {
                        Ok(md) => Some(OffsetDateTime::from(md)),
                        Err(_) => {
                            warn!(
                                event = EVENT_DISK_LOCAL_FORMAT_DECODE_FAILED,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_DISK_LOCAL,
                                reason = "modified_time_unsupported",
                                "Disk local modified time is unsupported on this platform"
                            );
                            None
                        }
                    };
                    results.push(res);

                    if req.max_results > 0 && found >= req.max_results {
                        break;
                    }
                }
                Err(e) => {
                    if e != DiskError::FileNotFound && e != DiskError::VolumeNotFound {
                        res.exists = true;
                        res.error = e.to_string();
                    }

                    if req.abort404 && !res.exists {
                        results.push(res);
                        break;
                    }

                    results.push(res);
                }
            }
        }

        Ok(results)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete_volume(&self, volume: &str, force_delete: bool) -> Result<()> {
        let p = self.io_get_bucket_path(volume)?;
        let _volume_mutation_guard = os::disk_volume_mutation_lock(&self.root, volume).write_owned().await;

        // A streaming reader's snapshot lease defers the physical cleanup of
        // data dirs whose version delete already committed. Those remnants are
        // logically deleted, so run the parked cleanups now instead of letting
        // the non-force removal below fail closed on them (the s3-tests SSE-C
        // teardown races exactly this way: DeleteObjects, then DeleteBucket
        // while an abandoned GET body still pins the lease).
        self.settle_pending_snapshot_deletes(volume).await;

        // Non-force removes empty directory remnants children-first with
        // non-recursive rmdir calls. A file that exists during the scan, or
        // appears before its parent is removed, fails closed with
        // VolumeNotEmpty. Only an explicit force delete removes recursively.
        let res = if force_delete {
            fs::remove_dir_all(&p).await
        } else {
            #[cfg(target_os = "linux")]
            {
                remove_empty_directory_tree_under_mount_lease(&self.mount_lease, volume, p.clone()).await
            }
            #[cfg(not(target_os = "linux"))]
            {
                remove_empty_directory_tree(&p).await
            }
        };

        if let Err(err) = res {
            let e: DiskError = classify_delete_volume_error(err);
            if e != DiskError::VolumeNotFound {
                return Err(e);
            }
        }

        // The whole bucket tree is gone; drop every cached io_uring descriptor
        // for it so a cache hit cannot keep serving a removed object (the read
        // hit path skips the volume-access check, so nothing else would notice)
        // (rustfs/backlog#1177).
        self.io_backend.invalidate_cached_fds_for_volume(volume);

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn disk_info(&self, _: &DiskInfoOptions) -> Result<DiskInfo> {
        let mut info = Cache::get(self.disk_info_cache.clone()).await?;
        info.nr_requests = self.nrrequests;
        info.rotational = self.rotational;
        info.mount_path = self.path().to_str().expect("operation should succeed").to_string();
        info.endpoint = self.endpoint.to_string();
        info.scanning = self.scanning.load(Ordering::Acquire) == 1;

        if info.id.is_none() {
            info.id = self.get_disk_id().await.unwrap_or(None);
        }

        Ok(info)
    }
    #[tracing::instrument(level = "trace", skip_all)]
    fn start_scan(&self) -> ScanGuard {
        self.scanning.fetch_add(1, Ordering::Release);
        ScanGuard(Arc::clone(&self.scanning))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_metadata(&self, volume: &str, path: &str) -> Result<Bytes> {
        crate::hp_guard!("LocalDisk::read_metadata");
        let file_path = self.io_get_object_path(volume, path)?;
        let volume_dir = self.io_get_bucket_path(volume)?;
        let (data, _) = self.read_all_data_with_dmtime(volume, volume_dir, file_path).await?;
        Ok(data.into())
    }
}

impl LocalDisk {
    pub(crate) async fn rename_data_borrowed(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: &FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        <Self as DiskAPI>::rename_data(self, src_volume, src_path, fi.clone(), dst_volume, dst_path).await
    }
}

async fn wait_for_startup_cleanup_signal(
    startup_cleanup_ready: &AtomicU32,
    startup_cleanup_notify: &Notify,
    wait_timeout: Duration,
) -> bool {
    if startup_cleanup_ready.load(Ordering::Acquire) != 0 {
        return true;
    }

    timeout(wait_timeout, async {
        loop {
            if startup_cleanup_ready.load(Ordering::Acquire) != 0 {
                return;
            }
            let notified = startup_cleanup_notify.notified();
            if startup_cleanup_ready.load(Ordering::Acquire) != 0 {
                return;
            }
            notified.await;
        }
    })
    .await
    .is_ok()
}

#[tracing::instrument(level = "trace", skip_all)]
async fn get_disk_info(drive_path: PathBuf) -> Result<(rustfs_utils::os::DiskInfo, bool)> {
    let drive_path = drive_path.to_string_lossy().to_string();
    check_path_length(&drive_path)?;

    let disk_info = get_info(&drive_path).inspect_err(|err| {
        log_startup_disk_io_error("get_disk_info_stat", Path::new(&drive_path), err);
    })?;
    let root_drive = if let Some(root_disk_threshold) = runtime_sources::root_disk_threshold_for_erasure_disk().await {
        if root_disk_threshold > 0 {
            disk_info.total <= root_disk_threshold
        } else {
            is_root_disk(&drive_path, SLASH_SEPARATOR).unwrap_or_default()
        }
    } else {
        false
    };

    Ok((disk_info, root_drive))
}

#[cfg(test)]
mod test {
    use super::*;
    use rustfs_filemeta::ErasureInfo;
    use std::io::{self, Write};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct CapturedLogs {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            let buffer = self
                .buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .clone();
            String::from_utf8(buffer).expect("captured logs should be valid UTF-8")
        }
    }

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    fn test_file_info(name: &str, version_id: Uuid, data_dir: Option<Uuid>, data: Option<Bytes>) -> FileInfo {
        let size = data
            .as_ref()
            .map(|data| i64::try_from(data.len()).expect("test data length should fit i64"))
            .unwrap_or(1);
        let mut file_info = FileInfo::new(name, 1, 0);
        file_info.erasure.index = 1;
        file_info.name = name.to_string();
        file_info.version_id = Some(version_id);
        file_info.data_dir = data_dir;
        file_info.data = data;
        file_info.size = size;
        file_info.parts = vec![ObjectPartInfo {
            number: 1,
            size: usize::try_from(size).expect("test object size should fit usize"),
            actual_size: size,
            ..Default::default()
        }];
        file_info.mod_time = Some(OffsetDateTime::now_utc());
        file_info
    }

    fn test_meta(fi: FileInfo) -> Vec<u8> {
        let mut meta = FileMeta::default();
        meta.add_version(fi).expect("test metadata should accept file info");
        meta.marshal_msg().expect("test metadata should encode")
    }

    #[test]
    #[serial_test::serial]
    fn read_version_records_local_metadata_stage_breakdown() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should be created");
        let recorder = crate::test_metrics::CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let dir = tempfile::tempdir().expect("temp dir should be created");
                let endpoint =
                    Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
                let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
                let bucket = "bucket";
                let object = "stage-breakdown";
                ensure_test_volume(&disk, bucket).await;

                let object_dir = dir.path().join(bucket).join(object);
                fs::create_dir_all(&object_dir)
                    .await
                    .expect("object directory should be created");
                fs::write(
                    object_dir.join(STORAGE_FORMAT_FILE),
                    test_meta(test_file_info(object, Uuid::new_v4(), None, Some(Bytes::from_static(b"inline")))),
                )
                .await
                .expect("object metadata should be written");

                disk.read_version(
                    "",
                    bucket,
                    object,
                    "",
                    &ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("read_version should succeed");

                let meta_object = "stage-breakdown-meta";
                let meta_object_dir = dir.path().join(RUSTFS_META_BUCKET).join(meta_object);
                fs::create_dir_all(&meta_object_dir)
                    .await
                    .expect("internal metadata object directory should be created");
                fs::write(
                    meta_object_dir.join(STORAGE_FORMAT_FILE),
                    test_meta(test_file_info(meta_object, Uuid::new_v4(), None, Some(Bytes::from_static(b"meta")))),
                )
                .await
                .expect("internal metadata should be written");

                disk.read_version(
                    "",
                    RUSTFS_META_BUCKET,
                    meta_object,
                    "",
                    &ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("internal metadata read_version should succeed");
            });
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

        for stage in [
            GET_STAGE_READ_VERSION_PATH_RESOLVE,
            GET_STAGE_READ_VERSION_PATH_CHECK,
            GET_STAGE_READ_VERSION_XLMETA_READ,
            GET_STAGE_READ_VERSION_DECODE,
        ] {
            assert_eq!(
                recorder
                    .histogram_values(
                        "rustfs_io_get_object_stage_duration_seconds",
                        &[("path", GET_OBJECT_PATH_LEGACY_DUPLEX), ("stage", stage)]
                    )
                    .len(),
                1,
                "{stage} should be recorded once for user-bucket LocalDisk::read_version"
            );
            assert_eq!(
                recorder
                    .histogram_values(
                        "rustfs_io_get_object_stage_duration_seconds",
                        &[("path", GET_OBJECT_PATH_INTERNAL_META), ("stage", stage)]
                    )
                    .len(),
                1,
                "{stage} should be recorded once for internal-meta LocalDisk::read_version"
            );
        }
    }

    #[test]
    fn inline_metadata_rollback_dir_avoids_real_data_dir_collision() {
        let target_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("version id should parse");
        let colliding_dir = Uuid::from_u128(target_version.as_u128() ^ INLINE_METADATA_ROLLBACK_DIR_XOR);
        let other_version = Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").expect("version id should parse");

        let mut meta = FileMeta::new();
        meta.add_version(test_file_info("object", other_version, Some(colliding_dir), None))
            .expect("test metadata should accept file info");

        let rollback_dir = inline_metadata_rollback_dir(target_version, &meta);
        assert_ne!(rollback_dir, colliding_dir);
        assert!(!rollback_dir.is_nil());
    }

    #[tokio::test]
    async fn inline_overwrite_does_not_report_rollback_dir_for_cleanup() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "bucket";
        let object = "parent";
        let tmp_object = "tmp-write";
        let version_id = Uuid::nil();

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        fs::create_dir_all(dir.path().join(bucket).join(object))
            .await
            .expect("destination object directory should be created");
        fs::write(
            dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE),
            test_meta(test_file_info(object, version_id, None, Some(Bytes::from_static(b"old")))),
        )
        .await
        .expect("old inline metadata should be written");
        fs::create_dir_all(dir.path().join(RUSTFS_META_TMP_BUCKET).join(tmp_object))
            .await
            .expect("staging object directory should be created");

        let response = disk
            .rename_data(
                RUSTFS_META_TMP_BUCKET,
                tmp_object,
                test_file_info(object, version_id, None, Some(Bytes::from_static(b"new"))),
                bucket,
                object,
            )
            .await
            .expect("inline overwrite should commit");

        assert_eq!(response.old_data_dir, None);
        assert_eq!(
            response.rollback_data_dir,
            Some(inline_metadata_rollback_dir(version_id, &FileMeta::new()))
        );
        assert_eq!(
            response.cleanup_data_dir, None,
            "synthetic rollback state must not be recursively reclaimed"
        );
    }

    #[test]
    fn local_inline_rollback_backup_falls_back_when_hardlink_fails() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let object_dir = dir.path().join("object");
        std::fs::create_dir(&object_dir).expect("object dir should be created");
        let xl_path = object_dir.join(STORAGE_FORMAT_FILE);
        let staging_dir = dir.path().join("staging");
        std::fs::create_dir(&staging_dir).expect("staging dir should be created");
        let staging_path = staging_dir.join(STORAGE_FORMAT_FILE);
        let old_metadata = b"old metadata";
        std::fs::write(&xl_path, old_metadata).expect("old metadata should be written");
        set_local_inline_rollback_hardlink_failure(&xl_path);

        let rollback_path = create_local_inline_rollback_backup(&xl_path, &staging_path, old_metadata)
            .expect("copy fallback should create rollback backup");
        let backup = std::fs::read(&rollback_path).expect("fallback backup should be readable");

        assert_eq!(backup, old_metadata);
        assert_eq!(rollback_path.parent(), Some(staging_dir.as_path()));
    }

    // Call-site guards for rustfs/rustfs#4978. On Linux/macOS CI a real
    // non-empty rmdir yields ENOTEMPTY (already tolerated by the pre-fix paths),
    // so an end-to-end delete test cannot detect a call-site regression. These
    // drive the decision functions the call sites use with a synthetic Solaris
    // EEXIST, so reverting the fix at either site turns a test red on any host.
    #[test]
    fn is_benign_object_rmdir_error_tolerates_missing_and_non_empty() {
        assert!(is_benign_object_rmdir_error(&std::io::Error::from(std::io::ErrorKind::NotFound)));
        assert!(is_benign_object_rmdir_error(&std::io::Error::from(std::io::ErrorKind::DirectoryNotEmpty)));
        // A genuine failure must still propagate.
        assert!(!is_benign_object_rmdir_error(&std::io::Error::from(std::io::ErrorKind::PermissionDenied)));
        #[cfg(unix)]
        {
            assert!(is_benign_object_rmdir_error(&std::io::Error::from_raw_os_error(libc::ENOTEMPTY)));
            // illumos/Solaris report a non-empty rmdir as EEXIST, not ENOTEMPTY.
            assert!(is_benign_object_rmdir_error(&std::io::Error::from_raw_os_error(libc::EEXIST)));
            assert!(!is_benign_object_rmdir_error(&std::io::Error::from_raw_os_error(libc::EACCES)));
        }
    }

    #[test]
    fn classify_delete_volume_error_maps_not_empty_and_missing() {
        assert!(matches!(
            classify_delete_volume_error(std::io::Error::from(std::io::ErrorKind::DirectoryNotEmpty)),
            DiskError::VolumeNotEmpty
        ));
        assert!(matches!(
            classify_delete_volume_error(std::io::Error::from(std::io::ErrorKind::NotFound)),
            DiskError::VolumeNotFound
        ));
        #[cfg(unix)]
        {
            assert!(matches!(
                classify_delete_volume_error(std::io::Error::from_raw_os_error(libc::ENOTEMPTY)),
                DiskError::VolumeNotEmpty
            ));
            // illumos/Solaris non-empty rmdir -> EEXIST must still refuse the bucket.
            assert!(matches!(
                classify_delete_volume_error(std::io::Error::from_raw_os_error(libc::EEXIST)),
                DiskError::VolumeNotEmpty
            ));
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_empty_tree_requires_non_reparse_directory() {
        validate_windows_empty_directory(0x10).expect("ordinary directories should be accepted");
        assert!(validate_windows_empty_directory(0).is_err());
        assert!(validate_windows_empty_directory(0x10 | 0x400).is_err());
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_empty_tree_blocks_replacement_at_final_delete_boundary() {
        let root = tempfile::tempdir().expect("temporary root should be created");
        let bucket_path = root.path().join("bucket");
        let child_path = bucket_path.join("child");
        fs::create_dir_all(&child_path).await.expect("bucket child should be created");
        let canonical_root = fs::canonicalize(&bucket_path).await.expect("bucket path should canonicalize");
        let directory = lock_windows_empty_directory(&child_path, Some(&canonical_root))
            .await
            .expect("child directory should be locked");

        std::fs::rename(&child_path, bucket_path.join("replacement"))
            .expect_err("the locked directory must not be replaceable at the final deletion boundary");
        remove_windows_empty_directory(directory)
            .await
            .expect("handle-relative deletion should remove the locked directory");

        assert!(!child_path.exists(), "the exact locked directory should be removed");
    }

    async fn ensure_test_volume(disk: &LocalDisk, volume: &str) {
        match disk.make_volume(volume).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("test volume should be available: {err:?}"),
        }
    }

    #[tokio::test]
    async fn read_version_rejects_zero_data_geometry_before_inline_shard_math() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "bucket";
        let object = "invalid-erasure";
        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&object_dir).await.expect("object dir should be created");
        let mut file_info = test_file_info(object, Uuid::new_v4(), Some(Uuid::new_v4()), None);
        file_info.parts = vec![ObjectPartInfo {
            number: 1,
            size: 1,
            actual_size: 1,
            ..Default::default()
        }];
        file_info.erasure.data_blocks = 0;
        file_info.erasure.parity_blocks = 2;
        file_info.erasure.block_size = 1;
        file_info.erasure.index = 1;
        file_info.erasure.distribution = vec![1, 2];
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), test_meta(file_info))
            .await
            .expect("invalid metadata should be written for the read regression");

        let err = disk
            .read_version(
                "",
                bucket,
                object,
                "",
                &ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("invalid erasure geometry must fail before shard size calculation");

        assert_eq!(err, DiskError::FileCorrupt);
    }

    #[tokio::test]
    async fn read_version_delete_marker_never_enters_inline_shard_math() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "bucket";
        let object = "delete-marker";
        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&object_dir).await.expect("object dir should be created");
        let file_info = FileInfo {
            name: object.to_string(),
            version_id: Some(Uuid::new_v4()),
            deleted: true,
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), test_meta(file_info))
            .await
            .expect("delete marker metadata should be written");

        let file_info = disk
            .read_version(
                "",
                bucket,
                object,
                "",
                &ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("delete marker must return before payload shard math");

        assert!(file_info.deleted);
        assert_eq!(file_info.erasure.data_blocks, 0);
    }

    #[tokio::test]
    async fn read_version_purge_pending_payload_still_loads_inline_candidate_data() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "bucket";
        let object = "purge-pending-object";
        let version_id = Uuid::new_v4();
        let data_dir = Uuid::new_v4();
        let payload = b"purge-pending payload";
        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join(object);
        let part_dir = object_dir.join(data_dir.to_string());
        fs::create_dir_all(&part_dir)
            .await
            .expect("object data dir should be created");
        fs::write(part_dir.join("part.1"), payload)
            .await
            .expect("payload part should be written");

        let mut file_info = test_file_info(object, version_id, Some(data_dir), None);
        file_info.size = payload.len() as i64;
        file_info.add_object_part(
            1,
            "part-etag".to_string(),
            payload.len(),
            file_info.mod_time,
            payload.len() as i64,
            None,
            None,
        );
        rustfs_utils::http::insert_str(
            &mut file_info.metadata,
            rustfs_utils::http::SUFFIX_PURGESTATUS,
            "target=PENDING;".to_string(),
        );
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), test_meta(file_info))
            .await
            .expect("purge-pending object metadata should be written");

        let file_info = disk
            .read_version(
                "",
                bucket,
                object,
                "",
                &ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("purge-pending object remains an erasure payload at the disk boundary");

        assert!(file_info.deleted, "version purge state should retain its logical deleted flag");
        assert!(!file_info.is_canonical_delete_marker());
        assert_eq!(file_info.data.as_deref(), Some(payload.as_slice()));
    }

    /// Regression coverage for the disk-layer delete/rename fixes:
    /// - move_to_trash must propagate real rename failures instead of silently
    ///   reporting success (rustfs/backlog#948, ECA-07).
    /// - the directory (trailing-slash) branch of rename_file/rename_part must
    ///   tolerate a missing destination instead of aborting on NotFound
    ///   (rustfs/backlog#960, ECA-19).
    mod delete_and_rename_regressions {
        use super::*;
        use tempfile::tempdir;

        async fn new_disk() -> (LocalDisk, tempfile::TempDir) {
            let dir = tempdir().expect("temp dir should be created");
            let endpoint =
                Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
            let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
            (disk, dir)
        }

        // #948: a genuinely missing source is benign and must still return Ok.
        #[tokio::test]
        async fn windows_and_unix_move_to_trash_missing_source_is_ok() {
            let (disk, dir) = new_disk().await;
            let missing = dir.path().join("bucket").join("does-not-exist");

            disk.move_to_trash(&missing, true, false)
                .await
                .expect("missing source must be treated as benign");
            disk.move_to_trash(&missing, false, false)
                .await
                .expect("missing source must be treated as benign (non-recursive)");
        }

        #[tokio::test]
        async fn windows_and_unix_move_to_trash_missing_destination_base_preserves_the_source() {
            let (disk, dir) = new_disk().await;
            let source = dir.path().join("bucket/object");
            let source_file = dir.path().join("bucket/object-file");
            fs::create_dir_all(&source).await.expect("source directory should be created");
            fs::write(source.join("part.1"), b"payload")
                .await
                .expect("source payload should be written");
            fs::write(&source_file, b"file-payload")
                .await
                .expect("source file should be written");
            let trash = disk
                .get_bucket_path(RUSTFS_META_TMP_DELETED_BUCKET)
                .expect("trash path should resolve");
            if let Err(err) = fs::remove_dir_all(&trash).await
                && err.kind() != ErrorKind::NotFound
            {
                panic!("trash directory should be removable: {err}");
            }

            let err = disk
                .move_to_trash(&source, true, false)
                .await
                .expect_err("a missing trash base must not be reported as a successful delete");

            assert_eq!(err, DiskError::FileNotFound);
            let err = disk
                .move_to_trash(&source_file, false, false)
                .await
                .expect_err("a missing trash base must not be reported as a successful non-recursive delete");
            assert_eq!(err, DiskError::FileNotFound);
            assert_eq!(
                fs::read(source.join("part.1"))
                    .await
                    .expect("source payload must remain readable"),
                b"payload"
            );
            assert_eq!(fs::read(&source_file).await.expect("source file must remain readable"), b"file-payload");
        }

        // #948: a real rename failure (here ENOTDIR, because a path component is a
        // regular file) must propagate instead of being swallowed as Ok(()). Before
        // the fix every non-DiskFull error fell through to `return Ok(())`.
        #[tokio::test]
        async fn windows_and_unix_move_to_trash_propagates_real_rename_error() {
            let (disk, dir) = new_disk().await;
            let bucket_dir = dir.path().join("bucket");
            fs::create_dir_all(&bucket_dir).await.expect("bucket dir should be created");
            let regular_file = bucket_dir.join("afile");
            fs::write(&regular_file, b"x").await.expect("regular file should be written");

            // Traversing through the regular file yields ENOTDIR at rename time.
            let bad_path = regular_file.join("child");

            let err = disk
                .move_to_trash(&bad_path, true, false)
                .await
                .expect_err("a real rename failure must propagate, not be reported as success");
            assert_eq!(err, DiskError::FileAccessDenied, "ENOTDIR must map to FileAccessDenied via to_file_error");
        }

        // #948: the happy path is unchanged — an existing object is moved out of its
        // original location and the call succeeds.
        #[tokio::test]
        async fn windows_and_unix_move_to_trash_moves_existing_object() {
            let (disk, dir) = new_disk().await;
            let object_dir = dir.path().join("bucket").join("obj-dir");
            fs::create_dir_all(&object_dir).await.expect("object dir should be created");
            fs::write(object_dir.join("part.1"), b"data")
                .await
                .expect("part should be written");

            disk.move_to_trash(&object_dir, true, false)
                .await
                .expect("existing object should move to trash");
            assert!(!object_dir.exists(), "object must be gone from its original location");
        }

        // #960: renaming a directory to a brand-new (non-existent) location must
        // succeed. Before the fix the unconditional pre-rename remove returned
        // FileNotFound and aborted the whole rename.
        #[tokio::test]
        async fn windows_and_unix_rename_file_directory_to_missing_destination_succeeds() {
            let (disk, dir) = new_disk().await;
            ensure_test_volume(&disk, "vol").await;

            let src_dir = dir.path().join("vol").join("a").join("dir");
            fs::create_dir_all(&src_dir).await.expect("src dir should be created");
            fs::write(src_dir.join("file"), b"payload")
                .await
                .expect("src file should be written");

            assert!(has_suffix("a/dir/", SLASH_SEPARATOR), "src path must carry directory semantics");
            disk.rename_file("vol", "a/dir/", "vol", "b/newdir/")
                .await
                .expect("directory rename to a missing destination must succeed");

            let moved = dir.path().join("vol").join("b").join("newdir").join("file");
            assert_eq!(fs::read(&moved).await.expect("moved file should be readable"), b"payload");
            assert!(!src_dir.exists(), "source directory must be gone after rename");
        }

        // #960: the same NotFound-tolerance fix applied to rename_part.
        #[tokio::test]
        async fn windows_and_unix_rename_part_directory_to_missing_destination_succeeds() {
            let (disk, dir) = new_disk().await;
            ensure_test_volume(&disk, "vol").await;

            let src_dir = dir.path().join("vol").join("a").join("dir");
            fs::create_dir_all(&src_dir).await.expect("src dir should be created");
            fs::write(src_dir.join("file"), b"payload")
                .await
                .expect("src file should be written");

            disk.rename_part("vol", "a/dir/", "vol", "b/newdir/", Bytes::from_static(b"meta-bytes"))
                .await
                .expect("directory rename_part to a missing destination must succeed");

            let moved = dir.path().join("vol").join("b").join("newdir").join("file");
            assert_eq!(fs::read(&moved).await.expect("moved file should be readable"), b"payload");
            let meta = dir.path().join("vol").join("b").join("newdir").join(".meta");
            assert_eq!(fs::read(&meta).await.expect("meta file should be readable"), b"meta-bytes");
            assert!(!src_dir.exists(), "source directory must be gone after rename");
        }
    }

    /// Crash-consistency harness for the rename_data commit sequence
    /// (rustfs/backlog#935 HP-14, test plan rustfs/backlog#896; hard rule from
    /// rustfs/backlog#878: "After partial commit, the object can only be an old or new version; it cannot be mixed").
    ///
    /// For every pre-commit crash point × durability tier, it seeds a committed
    /// object, stages a replacement, injects a hard power loss (no in-process
    /// rollback runs), reopens the disk to model a restart, and asserts the
    /// object still reads back as exactly the old version — or, when there was
    /// no old version, does not exist. The un-injected run asserts the commit
    /// makes the new version visible. Relaxed is exercised alongside Strict so
    /// the durability relaxations landing in HP-1/HP-4/HP-5 are held to the same
    /// old-or-new invariant, only with a wider (documented) power-loss window.
    mod crash_consistency {
        use super::*;
        use crate::crash_inject::{self, CrashPoint};
        use tempfile::tempdir;

        const VERSION_ID: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        const OLD_DATA_DIR: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        const NEW_DATA_DIR: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";

        async fn run_scenario(mode: DurabilityMode, crash: Option<CrashPoint>, with_old_version: bool) {
            // Serializes with every other durability-sensitive test and pins the
            // resolved tier for the whole scenario (held until dropped).
            let _mode = durability_mode_override::set(mode);

            let dir = tempdir().expect("temp dir should be created");
            let endpoint =
                Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
            let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

            let bucket = "bucket";
            let object = "crash-object";
            let tmp_object = "tmp-crash-object";
            let version_id = Uuid::parse_str(VERSION_ID).expect("version id should parse");
            let old_data_dir = Uuid::parse_str(OLD_DATA_DIR).expect("old data dir should parse");
            let new_data_dir = Uuid::parse_str(NEW_DATA_DIR).expect("new data dir should parse");

            ensure_test_volume(&disk, bucket).await;
            ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

            let object_dir = dir.path().join(bucket).join(object);
            let meta_path = object_dir.join(STORAGE_FORMAT_FILE);

            let old_meta = if with_old_version {
                let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
                let old_meta = test_meta(old_fi);
                fs::create_dir_all(object_dir.join(old_data_dir.to_string()))
                    .await
                    .expect("old data dir should be created");
                fs::write(&meta_path, &old_meta)
                    .await
                    .expect("old metadata should be written");
                Some(old_meta)
            } else {
                None
            };

            // Stage the replacement version's shard data under the tmp bucket.
            let tmp_data_dir = dir
                .path()
                .join(RUSTFS_META_TMP_BUCKET)
                .join(tmp_object)
                .join(new_data_dir.to_string());
            fs::create_dir_all(&tmp_data_dir)
                .await
                .expect("new tmp data dir should be created");
            fs::write(tmp_data_dir.join("part.1"), b"new-data")
                .await
                .expect("new tmp data should be written");

            if let Some(point) = crash {
                crash_inject::arm(point, object);
            }
            let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
            let result = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
                .await;

            // Reopen the disk to model a process restart after the crash.
            drop(disk);
            let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should reopen");
            let read = disk
                .read_version("", bucket, object, &version_id.to_string(), &ReadOptions::default())
                .await;

            match crash {
                Some(CrashPoint::RenameAfterMetaCommit) => {
                    // Hard power loss right after the xl.meta commit rename: the
                    // commit landed and no rollback ran, so the object must read
                    // back as the new version — whether or not an old version
                    // preceded it. This is the new-version half of the "old or
                    // new, never mixed" invariant (rustfs/backlog#878).
                    assert!(result.is_err(), "{mode:?}/AfterMetaCommit: an armed crash must surface as an error");
                    let fi = read.expect("new version must be readable after a post-commit crash");
                    assert_eq!(
                        fi.data_dir,
                        Some(new_data_dir),
                        "{mode:?}/AfterMetaCommit: read must resolve to the committed new data dir, never roll back to the old one"
                    );
                    assert!(
                        object_dir.join(new_data_dir.to_string()).exists(),
                        "{mode:?}/AfterMetaCommit: new data dir must remain on disk after the commit rename"
                    );
                }
                Some(point) => {
                    assert!(result.is_err(), "{mode:?}/{point:?}: an armed crash must surface as an error");
                    match &old_meta {
                        Some(old) => {
                            // The commit rename never ran: xl.meta is byte-for-byte
                            // the old version and its data dir is intact.
                            let after = fs::read(&meta_path).await.expect("old metadata must survive the crash");
                            assert_eq!(&after, old, "{mode:?}/{point:?}: xl.meta must remain the old version");
                            assert!(
                                object_dir.join(old_data_dir.to_string()).exists(),
                                "{mode:?}/{point:?}: old data dir must remain on disk"
                            );
                            let fi = read.expect("old version must be readable after the crash");
                            assert_eq!(
                                fi.data_dir,
                                Some(old_data_dir),
                                "{mode:?}/{point:?}: read must resolve to the old data dir, never the half-committed new one"
                            );
                        }
                        None => {
                            // No prior version: a pre-commit crash must leave no
                            // object behind at all.
                            assert!(
                                !meta_path.exists(),
                                "{mode:?}/{point:?}: no old version means no xl.meta after a pre-commit crash"
                            );
                            let err = read.expect_err("absent object must not be readable");
                            assert!(
                                matches!(err, DiskError::FileNotFound | DiskError::FileVersionNotFound),
                                "{mode:?}/{point:?}: unexpected error for absent object: {err:?}"
                            );
                        }
                    }
                }
                None => {
                    result.expect("un-injected rename_data must commit");
                    let fi = read.expect("new version must be readable after commit");
                    assert_eq!(
                        fi.data_dir,
                        Some(new_data_dir),
                        "{mode:?}: read must resolve to the newly committed data dir"
                    );
                    assert!(
                        object_dir.join(new_data_dir.to_string()).exists(),
                        "{mode:?}: new data dir must be in place after commit"
                    );
                }
            }
        }

        const CRASH_POINTS: [CrashPoint; 2] = [
            CrashPoint::RenameAfterDataRename,
            CrashPoint::RenameAfterBackupBeforeMetaCommit,
        ];

        #[tokio::test]
        async fn overwrite_pre_commit_crash_keeps_old_version_strict() {
            for point in CRASH_POINTS {
                run_scenario(DurabilityMode::Strict, Some(point), true).await;
            }
        }

        #[tokio::test]
        async fn overwrite_pre_commit_crash_keeps_old_version_relaxed() {
            for point in CRASH_POINTS {
                run_scenario(DurabilityMode::Relaxed, Some(point), true).await;
            }
        }

        #[tokio::test]
        async fn fresh_pre_commit_crash_leaves_no_object_strict() {
            for point in CRASH_POINTS {
                run_scenario(DurabilityMode::Strict, Some(point), false).await;
            }
        }

        #[tokio::test]
        async fn fresh_pre_commit_crash_leaves_no_object_relaxed() {
            for point in CRASH_POINTS {
                run_scenario(DurabilityMode::Relaxed, Some(point), false).await;
            }
        }

        #[tokio::test]
        async fn commit_without_crash_makes_new_version_visible() {
            run_scenario(DurabilityMode::Strict, None, true).await;
            run_scenario(DurabilityMode::Relaxed, None, true).await;
            run_scenario(DurabilityMode::Strict, None, false).await;
        }

        // Post-commit hard-crash points: the counterpart to the pre-commit
        // cases above. A crash right after the xl.meta commit rename (no
        // rollback) must leave the *new* version readable, closing the "old or
        // new, never mixed" invariant from the new-version side. Relaxed is
        // exercised alongside Strict so the durability relaxations are held to
        // the same invariant.
        #[tokio::test]
        async fn overwrite_post_commit_crash_keeps_new_version_strict() {
            run_scenario(DurabilityMode::Strict, Some(CrashPoint::RenameAfterMetaCommit), true).await;
        }

        #[tokio::test]
        async fn overwrite_post_commit_crash_keeps_new_version_relaxed() {
            run_scenario(DurabilityMode::Relaxed, Some(CrashPoint::RenameAfterMetaCommit), true).await;
        }

        #[tokio::test]
        async fn fresh_post_commit_crash_keeps_new_version_strict() {
            run_scenario(DurabilityMode::Strict, Some(CrashPoint::RenameAfterMetaCommit), false).await;
        }

        #[tokio::test]
        async fn fresh_post_commit_crash_keeps_new_version_relaxed() {
            run_scenario(DurabilityMode::Relaxed, Some(CrashPoint::RenameAfterMetaCommit), false).await;
        }
    }

    /// Crash-consistency for the in-place xl.meta update path — the atomic
    /// temp+rename inside [`LocalDisk::write_all_meta`] shared by `update_metadata`
    /// and `write_metadata` (delete markers, tag/metadata rewrites, decommission).
    ///
    /// rustfs/backlog#864: a fault that interrupts an in-place metadata rewrite
    /// must not mutate the committed object. [`CrashPoint::MetaWriteAfterTmpBeforeRename`]
    /// models a hard power loss after the replacement xl.meta is staged in the tmp
    /// bucket but before the publishing rename. Both durability tiers are held to
    /// the same rule: the destination xl.meta survives byte-for-byte, the object
    /// still reads as the old version, the staged tmp file is a reclaimable orphan
    /// confined to the tmp bucket, and a later un-injected rewrite publishes
    /// cleanly (retryable). This path previously had only parser-level unit tests.
    mod meta_write_crash_consistency {
        use super::*;
        use crate::crash_inject::{self, CrashPoint};
        use tempfile::tempdir;

        async fn run(mode: DurabilityMode) {
            // Serialize with every other durability-sensitive test and pin the
            // resolved tier for the whole scenario.
            let _mode = durability_mode_override::set(mode);

            let dir = tempdir().expect("temp dir should be created");
            let endpoint =
                Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
            let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

            let bucket = "bucket";
            let object = "meta-write-crash-object";
            ensure_test_volume(&disk, bucket).await;
            ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

            // Seed a committed object (version 1) by writing its xl.meta directly:
            // read_data=false reads never touch the data dir, so no shard staging
            // is needed to exercise the metadata commit window.
            let object_dir = dir.path().join(bucket).join(object);
            fs::create_dir_all(&object_dir).await.expect("object dir should be created");
            let meta_path = object_dir.join(STORAGE_FORMAT_FILE);
            let v1 = Uuid::new_v4();
            let old_meta = test_meta(test_file_info(object, v1, Some(Uuid::new_v4()), None));
            fs::write(&meta_path, &old_meta)
                .await
                .expect("committed xl.meta should be written");

            // Arm the crash, then attempt an in-place rewrite that adds version 2.
            let meta_key = format!("{object}/{STORAGE_FORMAT_FILE}");
            crash_inject::arm(CrashPoint::MetaWriteAfterTmpBeforeRename, &meta_key);
            let v2 = Uuid::new_v4();
            let result = disk
                .write_metadata("", bucket, object, test_file_info(object, v2, Some(Uuid::new_v4()), None))
                .await;
            assert!(
                matches!(result, Err(DiskError::Unexpected)),
                "{mode:?}: the armed crash point must be the failure that surfaced, got {result:?}"
            );

            // Reopen the disk to model a process restart after the crash.
            drop(disk);
            let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should reopen");

            // rustfs/backlog#864: the committed xl.meta is byte-for-byte the old
            // version — the interrupted rewrite never published.
            let after = fs::read(&meta_path).await.expect("xl.meta must survive the crash");
            assert_eq!(&after, &old_meta, "{mode:?}: xl.meta must remain the old version byte-for-byte");

            // The old version still reads; the un-published new version is absent.
            disk.read_version("", bucket, object, &v1.to_string(), &ReadOptions::default())
                .await
                .expect("the old version must remain readable after the crash");
            let new_read = disk
                .read_version("", bucket, object, &v2.to_string(), &ReadOptions::default())
                .await;
            assert!(
                matches!(new_read, Err(DiskError::FileVersionNotFound)),
                "{mode:?}: the interrupted update must not publish the new version, got {new_read:?}"
            );

            // The crash leaves no staging debris in the object directory; the
            // staged replacement is a reclaimable orphan under the tmp bucket.
            let mut object_entries = fs::read_dir(&object_dir).await.expect("object dir should list");
            let mut object_files = Vec::new();
            while let Some(entry) = object_entries
                .next_entry()
                .await
                .expect("object dir entry should be readable")
            {
                object_files.push(entry.file_name().to_string_lossy().to_string());
            }
            assert_eq!(
                object_files,
                vec![STORAGE_FORMAT_FILE.to_string()],
                "{mode:?}: the object directory must hold only its committed xl.meta"
            );

            // A retried rewrite (un-injected) publishes cleanly: the path is safely
            // retryable after the crash.
            crash_inject::disarm(CrashPoint::MetaWriteAfterTmpBeforeRename, &meta_key);
            disk.write_metadata("", bucket, object, test_file_info(object, v2, Some(Uuid::new_v4()), None))
                .await
                .expect("a retried metadata rewrite must succeed after the crash");
            disk.read_version("", bucket, object, &v2.to_string(), &ReadOptions::default())
                .await
                .expect("the retried version must be readable");
        }

        #[tokio::test]
        async fn meta_write_crash_before_rename_keeps_old_version_strict() {
            run(DurabilityMode::Strict).await;
        }

        #[tokio::test]
        async fn meta_write_crash_before_rename_keeps_old_version_relaxed() {
            run(DurabilityMode::Relaxed).await;
        }
    }

    /// Backdate a path's mtime so zero-expiry cleanup tests classify it as
    /// stale deterministically, instead of sleeping and hoping the filesystem
    /// timestamp granularity (or a backward wall-clock step) cooperates.
    fn backdate_mtime(path: &Path, age: Duration) {
        use std::fs::{File, FileTimes};
        let mtime = std::time::SystemTime::now() - age;
        File::open(path)
            .expect("path should open to backdate its mtime")
            .set_times(FileTimes::new().set_modified(mtime))
            .expect("mtime should rewind into the past");
    }

    #[tokio::test]
    async fn startup_cleanup_barrier_and_tmp_trash_cleanup_cover_noop_and_delete_paths() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let publication_root = os::PublicationRoot::new(dir.path()).expect("publication root should open");

        disk.startup_cleanup_ready.store(0, Ordering::Release);
        let ready = Arc::clone(&disk.startup_cleanup_ready);
        let notify = Arc::clone(&disk.startup_cleanup_notify);
        tokio::spawn(async move {
            ready.store(1, Ordering::Release);
            notify.notify_waiters();
        });
        disk.wait_for_startup_cleanup().await;
        assert_eq!(disk.startup_cleanup_ready.load(Ordering::Acquire), 1);

        LocalDisk::cleanup_stale_tmp_objects_with_expiry(dir.path().join("missing-root"), &publication_root, Duration::ZERO)
            .await
            .expect("missing tmp path should be a cleanup no-op");
        LocalDisk::cleanup_deleted_objects(dir.path().join("missing-root"))
            .await
            .expect("missing trash path should be a cleanup no-op");

        let tmp_root = dir.path().join(RUSTFS_META_TMP_BUCKET);
        let stale_dir = tmp_root.join("stale-upload");
        let live_file = tmp_root.join("part-file");
        let trash_root = dir.path().join(RUSTFS_META_TMP_DELETED_BUCKET);
        fs::create_dir_all(&stale_dir).await.expect("stale dir should be created");
        fs::write(&live_file, b"not-a-dir").await.expect("tmp file should be created");
        fs::create_dir_all(&trash_root).await.expect("trash dir should be created");
        backdate_mtime(&stale_dir, Duration::from_secs(10));

        LocalDisk::cleanup_stale_tmp_objects_with_expiry(dir.path().to_path_buf(), &publication_root, Duration::ZERO)
            .await
            .expect("stale tmp directory should move to trash");
        assert!(!stale_dir.exists(), "stale tmp directory should be moved away");
        assert!(live_file.exists(), "plain tmp files should be ignored by stale dir cleanup");

        fs::write(trash_root.join("trash-file"), b"delete me")
            .await
            .expect("trash file should be created");
        fs::create_dir_all(trash_root.join("trash-dir"))
            .await
            .expect("trash dir should be created");
        LocalDisk::cleanup_deleted_objects(dir.path().to_path_buf())
            .await
            .expect("trash cleanup should remove files and directories");
        assert!(
            fs::read_dir(&trash_root)
                .await
                .expect("trash root should exist")
                .next_entry()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn path_cache_covers_absolute_relative_batch_hit_miss_and_eviction() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let absolute = disk
            .resolve_abs_path(dir.path().join("absolute-object"))
            .expect("absolute path should resolve");
        assert!(absolute.ends_with("absolute-object"));

        {
            let mut cache = disk.path_cache.write();
            for index in 0..4096 {
                cache.insert(format!("cached-{index}"), dir.path().join(format!("cached-{index}")));
            }
        }
        let relative = disk.resolve_abs_path("bucket/object").expect("relative path should resolve");
        assert!(relative.ends_with("bucket/object"));
        assert!(
            disk.path_cache.read().len() < 4097,
            "cache eviction should run before inserting a new path"
        );

        let requests = vec![
            ("bucket".to_string(), "a".to_string()),
            ("bucket".to_string(), "b".to_string()),
        ];
        let first = disk
            .get_object_paths_batch(&requests)
            .expect("batch path resolution should handle cache misses");
        assert_eq!(first.len(), 2);
        assert!(first[0].ends_with("bucket/a"));
        assert!(first[1].ends_with("bucket/b"));

        let second = disk
            .get_object_paths_batch(&requests)
            .expect("batch path resolution should reuse cache hits");
        assert_eq!(second, first);
    }

    #[tokio::test]
    async fn open_file_read_only_returns_existing_payload_without_parent_creation() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let path = dir.path().join("bucket/object/part.1");
        fs::create_dir_all(path.parent().expect("test file should have a parent"))
            .await
            .expect("parent directory should be created");
        fs::write(&path, b"read-only-payload")
            .await
            .expect("test file should be written");

        let mut file = disk.open_file_read_only(&path).await.expect("read-only file should open");
        let mut payload = Vec::new();
        file.read_to_end(&mut payload).await.expect("read-only file should read");

        assert_eq!(payload, b"read-only-payload");
    }

    #[tokio::test]
    async fn write_metadata_rejects_default_like_delete_marker() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let forged = FileInfo {
            deleted: true,
            ..Default::default()
        };

        let err = disk
            .write_metadata("bucket", "bucket", "object", forged)
            .await
            .expect_err("default-like delete marker must be rejected before persistence");
        assert_eq!(err, DiskError::FileCorrupt);
    }

    #[tokio::test]
    async fn write_metadata_replaces_corrupt_existing_xl_meta_without_losing_new_version() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "metadata-rewrite-bucket";
        let object = "nested/object";
        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&object_dir).await.expect("object dir should be created");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), b"not-valid-xl-meta")
            .await
            .expect("corrupt metadata should be installed");

        let version_id = Uuid::new_v4();
        let mut fi = test_file_info(object, version_id, Some(Uuid::new_v4()), Some(Bytes::from_static(b"restored")));
        fi.fresh = false;
        disk.write_metadata(bucket, bucket, object, fi)
            .await
            .expect("new metadata write should replace corrupt old metadata");

        let raw = disk
            .read_all(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"))
            .await
            .expect("rewritten metadata should be readable");
        let restored = FileMeta::load(&raw)
            .expect("rewritten metadata should decode")
            .into_fileinfo(bucket, object, "", true, false, true)
            .expect("rewritten metadata should expose the new version");

        assert_eq!(restored.version_id, Some(version_id));
        assert_eq!(restored.name, object);
    }

    fn test_check_parts_file_info(data_dir: Uuid) -> FileInfo {
        FileInfo {
            name: "dir/object".to_string(),
            data_dir: Some(data_dir),
            parts: (1..=4)
                .map(|number| ObjectPartInfo {
                    number,
                    size: 5,
                    actual_size: 5,
                    ..Default::default()
                })
                .collect(),
            erasure: ErasureInfo {
                data_blocks: 2,
                parity_blocks: 2,
                block_size: 4,
                index: 1,
                distribution: vec![1, 2, 3, 4],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_check_parts_classifies_part_and_volume_failures() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let data_dir = Uuid::parse_str("01010101-0101-0101-0101-010101010101").expect("data dir should parse");
        let fi = test_check_parts_file_info(data_dir);

        ensure_test_volume(&disk, bucket).await;
        let part_dir = dir.path().join(bucket).join(object).join(data_dir.to_string());
        fs::create_dir_all(&part_dir).await.expect("part dir should be created");
        fs::write(part_dir.join("part.1"), vec![1; 4])
            .await
            .expect("valid part should be written");
        fs::write(part_dir.join("part.2"), vec![2; 3])
            .await
            .expect("short part should be written");
        fs::create_dir_all(part_dir.join("part.3"))
            .await
            .expect("directory part marker should be created");

        let resp = disk
            .check_parts(bucket, object, &fi)
            .await
            .expect("check_parts should return per-part status");
        assert_eq!(
            resp.results,
            vec![
                CHECK_PART_SUCCESS,
                CHECK_PART_FILE_CORRUPT,
                CHECK_PART_FILE_NOT_FOUND,
                CHECK_PART_FILE_NOT_FOUND,
            ],
            "valid, short, directory, and missing parts must be classified distinctly"
        );

        let missing_volume_resp = disk
            .check_parts("missing-bucket", object, &fi)
            .await
            .expect("missing volume should be reported per part");
        assert_eq!(
            missing_volume_resp.results,
            vec![CHECK_PART_VOLUME_NOT_FOUND; fi.parts.len()],
            "missing volume must not be reported as recoverable missing shards"
        );
    }

    #[tokio::test]
    async fn test_read_parts_reports_bad_metadata_and_missing_data_part() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        ensure_test_volume(&disk, bucket).await;

        let valid_part = ObjectPartInfo {
            etag: "etag-1".to_string(),
            number: 1,
            size: 5,
            actual_size: 5,
            ..Default::default()
        };
        disk.write_all(bucket, "upload/part.1", Bytes::from_static(b"data-1"))
            .await
            .expect("part data should be written");
        disk.write_all(
            bucket,
            "upload/part.1.meta",
            Bytes::from(valid_part.marshal_msg().expect("part metadata should encode")),
        )
        .await
        .expect("part metadata should be written");
        disk.write_all(bucket, "upload/part.2", Bytes::from_static(b"data-2"))
            .await
            .expect("second part data should be written");
        disk.write_all(bucket, "upload/part.2.meta", Bytes::from_static(b"not-msgpack"))
            .await
            .expect("bad part metadata should be written");
        disk.write_all(bucket, "upload/part.3.meta", Bytes::from_static(b"orphan-meta"))
            .await
            .expect("orphan metadata should be written");

        let parts = disk
            .read_parts(
                bucket,
                &[
                    "upload/part.1.meta".to_string(),
                    "upload/part.2.meta".to_string(),
                    "upload/part.3.meta".to_string(),
                ],
            )
            .await
            .expect("read_parts should return per-part status");

        assert_eq!(parts[0], valid_part);
        assert_eq!(parts[1].number, 2);
        assert!(
            parts[1].error.is_some(),
            "bad metadata must be surfaced as a per-part error instead of a decoded part"
        );
        assert_eq!(parts[2].number, 3);
        assert!(parts[2].error.is_some(), "missing data part must be surfaced as a per-part error");
    }

    #[tokio::test]
    async fn test_rename_part_rejects_type_mismatch_without_touching_source() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let tmp_volume = "tmp";
        let bucket = "bucket";
        ensure_test_volume(&disk, tmp_volume).await;
        ensure_test_volume(&disk, bucket).await;

        let payload = Bytes::from_static(b"part payload");
        disk.write_all(tmp_volume, "upload/part.1", payload.clone())
            .await
            .expect("source part should be written");

        let result = disk
            .rename_part(tmp_volume, "upload/part.1", bucket, "object/part.1/", Bytes::from_static(b"metadata"))
            .await;
        assert!(
            matches!(result, Err(DiskError::FileAccessDenied)),
            "file-to-directory rename_part mismatch must be rejected, got {result:?}"
        );
        assert_eq!(
            disk.read_all(tmp_volume, "upload/part.1")
                .await
                .expect("source part must remain after rejected rename"),
            payload
        );
        assert!(
            matches!(disk.read_all(bucket, "object/part.1.meta").await, Err(DiskError::FileNotFound)),
            "rejected rename_part must not write destination metadata"
        );
    }

    #[tokio::test]
    async fn test_rename_part_commits_data_and_metadata_then_removes_source() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let tmp_volume = "tmp";
        let bucket = "bucket";
        ensure_test_volume(&disk, tmp_volume).await;
        ensure_test_volume(&disk, bucket).await;

        let payload = Bytes::from_static(b"part payload");
        let meta = Bytes::from_static(b"part metadata");
        disk.write_all(tmp_volume, "upload/part.1", payload.clone())
            .await
            .expect("source part should be written");

        disk.prepare_part_transaction(tmp_volume, "upload/part.1", bucket, "object/part.1", meta.clone())
            .await
            .expect("part transaction should be prepared");
        disk.rename_part(tmp_volume, "upload/part.1", bucket, "object/part.1", meta.clone())
            .await
            .expect("rename_part should commit part");
        disk.settle_part_transaction(bucket, "object/part.1", PartTransactionAction::Commit)
            .await
            .expect("part transaction should be committed");

        assert_eq!(
            disk.read_all(bucket, "object/part.1")
                .await
                .expect("destination part should be readable"),
            payload
        );
        assert_eq!(
            disk.read_all(bucket, "object/part.1.meta")
                .await
                .expect("destination metadata should be readable"),
            meta
        );
        assert!(
            matches!(disk.read_all(tmp_volume, "upload/part.1").await, Err(DiskError::FileNotFound)),
            "source part must be removed after a successful commit"
        );

        let legacy_payload = Bytes::from_static(b"legacy peer payload");
        let legacy_meta = Bytes::from_static(b"legacy peer metadata");
        disk.write_all(tmp_volume, "legacy/part.1", legacy_payload.clone())
            .await
            .expect("legacy source part should be written");
        disk.rename_part(tmp_volume, "legacy/part.1", bucket, "object/part.1", legacy_meta.clone())
            .await
            .expect("pre-transaction peer RenamePart should remain supported");
        assert_eq!(
            disk.read_all(bucket, "object/part.1")
                .await
                .expect("legacy destination part should be readable"),
            legacy_payload
        );
        assert_eq!(
            disk.read_all(bucket, "object/part.1.meta")
                .await
                .expect("legacy destination metadata should be readable"),
            legacy_meta
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_rename_part_commits_realistic_windows_multipart_path() {
        use crate::disk::RUSTFS_META_MULTIPART_BUCKET;
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        assert_eq!(effective_durability(RUSTFS_META_MULTIPART_BUCKET), DurabilityMode::Strict);
        let dir = tempdir().expect("temp dir should be created");
        let root = dir.path().join("realistic-windows-multipart-root");
        fs::create_dir_all(&root).await.expect("disk root should be created");
        let endpoint = Endpoint::try_from(root.to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        ensure_test_volume(&disk, RUSTFS_META_MULTIPART_BUCKET).await;

        let src_path = "upload/part.1";
        let dst_path = concat!(
            "6f897928dfe04a87a269ccd9f5a5897d9cbbdf6b55e4d903ef3cbc1125c0cb8f/",
            "8f897819-2604-4f3d-b843-c32a45d198b2x1786372838834745500/",
            "58ba822c-06e4-4332-81cc-be2c9d921900/part.1"
        );
        let transaction_path = disk
            .io_get_object_path(RUSTFS_META_MULTIPART_BUCKET, &crate::disk::part_transaction_path(dst_path))
            .expect("transaction path should resolve");
        let deepest_marker = transaction_path
            .parent()
            .expect("transaction path should have a parent")
            .join(".part-txn-00000000-0000-0000-0000-000000000000")
            .join(PART_TRANSACTION_OLD_DATA_ABSENT);
        assert!(
            deepest_marker.as_os_str().len() > 260,
            "regression path must cross the traditional Windows MAX_PATH boundary: {deepest_marker:?}"
        );
        let payload = Bytes::from_static(b"part payload");
        let meta = Bytes::from_static(b"part metadata");
        disk.write_all(RUSTFS_META_TMP_BUCKET, src_path, payload.clone())
            .await
            .expect("source part should be written");

        disk.prepare_part_transaction(RUSTFS_META_TMP_BUCKET, src_path, RUSTFS_META_MULTIPART_BUCKET, dst_path, meta.clone())
            .await
            .expect("realistic Windows part transaction should be prepared");
        disk.rename_part(RUSTFS_META_TMP_BUCKET, src_path, RUSTFS_META_MULTIPART_BUCKET, dst_path, meta.clone())
            .await
            .expect("realistic Windows part should be committed");
        disk.settle_part_transaction(RUSTFS_META_MULTIPART_BUCKET, dst_path, PartTransactionAction::Commit)
            .await
            .expect("realistic Windows part transaction should be settled");

        assert_eq!(
            disk.read_all(RUSTFS_META_MULTIPART_BUCKET, dst_path)
                .await
                .expect("destination part should be readable"),
            payload
        );
        assert_eq!(
            disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &format!("{dst_path}.meta"))
                .await
                .expect("destination metadata should be readable"),
            meta
        );

        let replacement_payload = Bytes::from_static(b"replacement part payload");
        let replacement_meta = Bytes::from_static(b"replacement part metadata");
        disk.write_all(RUSTFS_META_TMP_BUCKET, src_path, replacement_payload.clone())
            .await
            .expect("replacement source part should be written");
        disk.prepare_part_transaction(
            RUSTFS_META_TMP_BUCKET,
            src_path,
            RUSTFS_META_MULTIPART_BUCKET,
            dst_path,
            replacement_meta.clone(),
        )
        .await
        .expect("replacement Windows part transaction should be prepared");
        disk.rename_part(
            RUSTFS_META_TMP_BUCKET,
            src_path,
            RUSTFS_META_MULTIPART_BUCKET,
            dst_path,
            replacement_meta.clone(),
        )
        .await
        .expect("replacement Windows part should be committed");
        disk.settle_part_transaction(RUSTFS_META_MULTIPART_BUCKET, dst_path, PartTransactionAction::Commit)
            .await
            .expect("replacement Windows part transaction should be settled");

        assert_eq!(
            disk.read_all(RUSTFS_META_MULTIPART_BUCKET, dst_path)
                .await
                .expect("replacement destination part should be readable"),
            replacement_payload
        );
        assert_eq!(
            disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &format!("{dst_path}.meta"))
                .await
                .expect("replacement destination metadata should be readable"),
            replacement_meta
        );
        assert!(
            matches!(disk.read_all(RUSTFS_META_TMP_BUCKET, src_path).await, Err(DiskError::FileNotFound)),
            "successful replacement must remove its source part"
        );
        assert!(!transaction_path.exists(), "settled replacement must remove its transaction directory");
    }

    #[tokio::test]
    async fn test_part_transaction_rolls_back_data_published_before_metadata() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        ensure_test_volume(&disk, "tmp").await;
        ensure_test_volume(&disk, "bucket").await;

        disk.write_all("tmp", "upload/part.1", Bytes::from_static(b"new data"))
            .await
            .expect("new part should be staged");
        disk.write_all("bucket", "object/part.1", Bytes::from_static(b"old data"))
            .await
            .expect("old part data should be staged");
        disk.write_all("bucket", "object/part.1.meta", Bytes::from_static(b"old metadata"))
            .await
            .expect("old part metadata should be staged");

        disk.prepare_part_transaction("tmp", "upload/part.1", "bucket", "object/part.1", Bytes::from_static(b"new metadata"))
            .await
            .expect("part transaction should be prepared");
        disk.rename_file("tmp", "upload/part.1", "bucket", "object/part.1")
            .await
            .expect("data publication should succeed");
        disk.settle_part_transaction("bucket", "object/part.1", PartTransactionAction::Rollback)
            .await
            .expect("part transaction should roll back");

        assert_eq!(
            disk.read_all("bucket", "object/part.1")
                .await
                .expect("old part data should be restored"),
            Bytes::from_static(b"old data")
        );
        assert_eq!(
            disk.read_all("bucket", "object/part.1.meta")
                .await
                .expect("old part metadata should be restored"),
            Bytes::from_static(b"old metadata")
        );
    }

    struct BlockingScanWriter {
        entered_tx: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl AsyncWrite for BlockingScanWriter {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<io::Result<usize>> {
            if let Some(tx) = self.entered_tx.take() {
                let _ = tx.send(());
            }
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn blocking_scan_writer_keeps_flush_and_shutdown_pending() {
        let mut flush_writer = BlockingScanWriter { entered_tx: None };
        assert!(
            timeout(Duration::from_millis(10), flush_writer.flush()).await.is_err(),
            "blocking scan writer flush should stay pending"
        );

        let mut shutdown_writer = BlockingScanWriter { entered_tx: None };
        assert!(
            timeout(Duration::from_millis(10), shutdown_writer.shutdown()).await.is_err(),
            "blocking scan writer shutdown should stay pending"
        );
    }

    /// A writer that stalls on every write, standing in for a slow listing
    /// consumer (quorum merge, a lagging peer drive).
    struct SlowWriter {
        delay: Duration,
        sleep: Option<Pin<Box<Sleep>>>,
    }

    impl AsyncWrite for SlowWriter {
        fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            if self.sleep.is_none() {
                let delay = self.delay;
                self.sleep = Some(Box::pin(tokio::time::sleep(delay)));
            }

            let sleep = self.sleep.as_mut().expect("sleep was just installed");
            match sleep.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.sleep = None;
                    Poll::Ready(Ok(buf.len()))
                }
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    // #4644: the stall budget bounds a single drive read, never the walk as a
    // whole. A walk that keeps making progress must survive even when the total
    // time spent blocked on a slow consumer dwarfs the stall timeout.
    #[tokio::test]
    async fn walk_dir_does_not_charge_consumer_backpressure_to_the_stall_budget() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let bucket = "test-bucket";
        for idx in 0..4 {
            let object_dir = dir.path().join(bucket).join(format!("prefix/object-{idx}"));
            fs::create_dir_all(&object_dir).await.expect("object dir should be created");
            fs::write(object_dir.join(STORAGE_FORMAT_FILE), b"meta")
                .await
                .expect("object metadata should be written");
        }

        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let stall = Duration::from_millis(300);
        let write_delay = Duration::from_millis(150);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "prefix/".to_string(),
            recursive: true,
            stall_timeout_ms: Some(duration_millis(stall)),
            ..Default::default()
        };

        let mut writer = SlowWriter {
            delay: write_delay,
            sleep: None,
        };

        let started = std::time::Instant::now();
        let result = disk.walk_dir(opts, &mut writer).await;
        let elapsed = started.elapsed();

        assert!(result.is_ok(), "a walk making steady progress must not time out, got {result:?}");
        assert!(
            elapsed > stall,
            "test is only meaningful if the walk outlives the stall budget, elapsed {elapsed:?} vs stall {stall:?}"
        );
    }

    // #4644: the stall timeout is what catches a drive that stops answering.
    //
    // This exercises the bound directly rather than through `walk_dir`. A drive
    // read only outlives the budget when the drive itself hangs, and a local disk
    // offers no seam to make a real read hang: `access`/`read_dir`/`read_metadata`
    // dispatch to tokio's blocking pool, so on a healthy filesystem they always
    // complete first. Racing them against a paused clock would only buy a flaky
    // test.
    #[tokio::test(start_paused = true)]
    async fn with_walk_stall_timeout_fails_only_when_a_read_stops_answering() {
        let stall = Duration::from_millis(100);

        let hung = with_walk_stall_timeout(Some(stall), async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        })
        .await;
        assert!(
            matches!(hung, Err(DiskError::Timeout)),
            "a read that stops answering must trip the stall budget, got {hung:?}"
        );

        let prompt = with_walk_stall_timeout(Some(stall), async { Ok(7_u32) })
            .await
            .expect("a prompt read must pass through");
        assert_eq!(prompt, 7);

        // No budget configured: the read is unbounded on purpose.
        let unbounded = with_walk_stall_timeout(None, async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        })
        .await;
        assert!(unbounded.is_ok(), "an unset stall budget must not bound the read");

        let disabled = with_walk_stall_timeout(Some(Duration::ZERO), async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        })
        .await;
        assert!(disabled.is_ok(), "a zero stall budget must disable the bound");
    }

    // #4644: reads that do not yield a Result (`access`, `is_empty_dir`,
    // `fs::metadata`) are bounded by the same budget — the listing path has no
    // total timeout left to fall back on.
    #[tokio::test(start_paused = true)]
    async fn with_walk_stall_deadline_bounds_reads_that_do_not_yield_a_result() {
        let stall = Duration::from_millis(100);

        let hung = with_walk_stall_deadline(Some(stall), async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            true
        })
        .await;
        assert!(
            matches!(hung, Err(DiskError::Timeout)),
            "a hung infallible read must trip the stall budget, got {hung:?}"
        );

        let prompt = with_walk_stall_deadline(Some(stall), async { true })
            .await
            .expect("a prompt read must pass through");
        assert!(prompt);
    }

    #[tokio::test]
    async fn concurrent_local_disk_scans_of_same_prefix_succeed() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let bucket = "test-bucket";
        let object_dir = dir.path().join(bucket).join("prefix/object");
        fs::create_dir_all(&object_dir).await.expect("object dir should be created");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), b"meta")
            .await
            .expect("object metadata should be written");

        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "prefix/".to_string(),
            recursive: true,
            ..Default::default()
        };

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let first_disk = Arc::clone(&disk);
        let first_opts = opts.clone();
        let mut blocking_writer = BlockingScanWriter {
            entered_tx: Some(entered_tx),
        };
        let first_scan = tokio::spawn(async move { first_disk.walk_dir(first_opts, &mut blocking_writer).await });

        entered_rx.await.expect("first scan should enter write path");
        let mut second_writer = tokio::io::sink();
        let second_scan = disk.walk_dir(opts.clone(), &mut second_writer).await;
        assert!(
            second_scan.is_ok(),
            "concurrent scan of same bucket and prefix must succeed, got {second_scan:?}"
        );
        first_scan.abort();
        assert!(
            first_scan
                .await
                .expect_err("first scan task should be cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn test_rename_data_writes_old_metadata_backup_before_non_inline_undo() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let tmp_object = "tmp-write";
        let version_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("22222222-2222-2222-2222-222222222222").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("33333333-3333-3333-3333-333333333333").expect("new data dir should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let dst_object_dir = dir.path().join(bucket).join("dir/object");
        fs::create_dir_all(dst_object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), test_meta(old_fi))
            .await
            .expect("old metadata should be written");

        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new-data")
            .await
            .expect("new tmp data should be written");

        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        let resp = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("rename_data should commit");

        assert_eq!(resp.old_data_dir, Some(old_data_dir));
        assert_eq!(resp.sign, Some(version_id.as_bytes().to_vec()));
        assert!(
            dst_object_dir
                .join(old_data_dir.to_string())
                .join(STORAGE_FORMAT_FILE_BACKUP)
                .exists()
        );
        assert!(
            !dst_object_dir
                .join(old_data_dir.to_string())
                .join(STORAGE_FORMAT_FILE)
                .exists()
        );
    }

    #[tokio::test]
    #[serial_test::serial(file_sync_probe)]
    #[allow(clippy::await_holding_lock)]
    async fn test_rename_data_shares_file_sync_limit_across_one_disk() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let bucket = "shared-sync-limit-bucket";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let first_data_dir = Uuid::from_u128(1);
        let second_data_dir = Uuid::from_u128(2);
        for (tmp_object, data_dir) in [("tmp-first", first_data_dir), ("tmp-second", second_data_dir)] {
            let tmp_data_dir = dir
                .path()
                .join(RUSTFS_META_TMP_BUCKET)
                .join(tmp_object)
                .join(data_dir.to_string());
            fs::create_dir_all(&tmp_data_dir)
                .await
                .expect("staged data dir should be created");
            for part in 1..=os::MAX_PARALLEL_FILE_SYNCS {
                fs::write(tmp_data_dir.join(format!("part.{part}")), b"shard")
                    .await
                    .expect("staged shard should be written");
            }
        }

        // Use the disk's canonicalized root path — on macOS, tempfile returns
        // /var/folders/... while LocalDisk resolves to /private/var/folders/...
        // via dunce::canonicalize. The probe's starts_with check would fail
        // with the non-canonical path, causing wait_for_active to hang.
        let _probe = os::file_sync_probe::set_blocking(&disk.root);
        let first = {
            let disk = disk.clone();
            tokio::spawn(async move {
                let fi = test_file_info("first-object", Uuid::from_u128(11), Some(first_data_dir), None);
                disk.rename_data(RUSTFS_META_TMP_BUCKET, "tmp-first", fi, bucket, "first-object")
                    .await
            })
        };
        let second = {
            let disk = disk.clone();
            tokio::spawn(async move {
                let fi = test_file_info("second-object", Uuid::from_u128(12), Some(second_data_dir), None);
                disk.rename_data(RUSTFS_META_TMP_BUCKET, "tmp-second", fi, bucket, "second-object")
                    .await
            })
        };
        os::file_sync_probe::wait_for_active(os::MAX_PARALLEL_FILE_SYNCS).await;

        assert_eq!(
            disk.file_sync_permits.available_permits(),
            0,
            "concurrent rename_data calls must share the LocalDisk sync limit"
        );
        assert_eq!(
            os::file_sync_probe::peak(),
            os::MAX_PARALLEL_FILE_SYNCS,
            "one disk must not exceed its shared file-sync capacity"
        );
        let reconnected = LocalDisk::new(&endpoint, false).await.expect("local disk should reconnect");
        assert!(
            Arc::ptr_eq(&disk.file_sync_permits, &reconnected.file_sync_permits),
            "reconnecting the same disk must preserve its file-sync limiter"
        );
        assert_eq!(
            reconnected.file_sync_permits.available_permits(),
            0,
            "reconnected disk must inherit the outstanding sync budget"
        );

        os::file_sync_probe::release();
        first
            .await
            .expect("first rename_data task should join")
            .expect("first rename_data should commit");
        second
            .await
            .expect("second rename_data task should join")
            .expect("second rename_data should commit");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_write_all_meta_skips_tmp_parent_dir_fsync_but_fsyncs_dst_parent() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "sync-meta-bucket";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let meta_path = format!("dir/object/{STORAGE_FORMAT_FILE}");
        disk.write_all_meta(bucket, &meta_path, b"payload", true)
            .await
            .expect("write_all_meta should succeed");

        let dst_file_path = disk.get_object_path(bucket, &meta_path).expect("dst path should resolve");
        assert_eq!(
            tokio::fs::read(&dst_file_path).await.expect("xl.meta should exist"),
            b"payload",
            "renamed xl.meta must carry the written contents"
        );

        let tmp_parent = disk
            .get_bucket_path(RUSTFS_META_TMP_BUCKET)
            .expect("tmp bucket path should resolve");
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&tmp_parent),
            "tmp parent dir must not be fsynced for a write-then-rename tmp file"
        );

        let dst_parent = dst_file_path.parent().expect("dst file should have a parent").to_path_buf();
        assert!(
            os::fsync_dir_recorder::was_fsynced(&dst_parent),
            "destination parent dir must be fsynced after the commit rename"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_write_all_public_still_fsyncs_parent_dir() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "sync-public-bucket";
        ensure_test_volume(&disk, bucket).await;

        disk.write_all(bucket, "config/settings.json", Bytes::from_static(b"payload"))
            .await
            .expect("write_all should succeed");

        let file_path = disk
            .get_object_path(bucket, "config/settings.json")
            .expect("file path should resolve");
        let parent = file_path.parent().expect("file should have a parent").to_path_buf();
        assert!(
            os::fsync_dir_recorder::was_fsynced(&parent),
            "direct (non-renamed) writes must keep fsyncing their parent dir"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_rename_data_non_inline_skips_tmp_parent_dir_fsync() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "sync-rename-bucket";
        let object = "dir/object";
        let tmp_object = "tmp-sync-write";
        let version_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("55555555-5555-5555-5555-555555555555").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("66666666-6666-6666-6666-666666666666").expect("new data dir should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let dst_object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(dst_object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), test_meta(old_fi))
            .await
            .expect("old metadata should be written");

        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        let tmp_parent = tmp_data_dir
            .parent()
            .expect("tmp data dir should have a parent")
            .to_path_buf();
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new-data")
            .await
            .expect("new tmp data should be written");

        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("rename_data should commit");
        assert!(
            !tmp_parent.exists(),
            "successful non-inline commit should remove the empty staging parent"
        );

        // The tmp xl.meta write point uses SyncMode::FileOnly: its parent dir
        // ({tmp}/{tmp_object}) must not be fsynced.
        let tmp_meta_parent = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{STORAGE_FORMAT_FILE}"))
            .expect("tmp meta path should resolve")
            .parent()
            .expect("tmp meta should have a parent")
            .to_path_buf();
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&tmp_meta_parent),
            "tmp xl.meta parent dir must not be fsynced for the write-then-rename tmp file"
        );

        // The commit sequence itself is untouched: the destination parent dir
        // is fsynced after the commit rename, ...
        let dst_meta_parent = disk
            .get_object_path(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"))
            .expect("dst meta path should resolve")
            .parent()
            .expect("dst meta should have a parent")
            .to_path_buf();
        assert!(
            os::fsync_dir_recorder::was_fsynced(&dst_meta_parent),
            "destination parent dir must be fsynced after the commit rename"
        );

        // ... and the rollback backup (which stays in place, no rename) still
        // fsyncs its parent dir (SyncMode::FileAndDir).
        let backup_parent = disk
            .get_object_path(bucket, &format!("{object}/{old_data_dir}/{STORAGE_FORMAT_FILE_BACKUP}"))
            .expect("backup path should resolve")
            .parent()
            .expect("backup should have a parent")
            .to_path_buf();
        assert!(
            os::fsync_dir_recorder::was_fsynced(&backup_parent),
            "old-metadata rollback backup must keep fsyncing its parent dir"
        );
    }

    #[tokio::test]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn rename_data_non_inline_uses_dst_dir_fsync_group_commit_when_enabled() {
        let _group_commit = os::set_dst_dir_fsync_group_commit_for_test(true);
        let bucket = "grouped-dst-fsync-bucket";
        let object = "dir/object";
        let (disk, _dir) = commit_new_object(DurabilityMode::Strict, bucket, object).await;
        let dst_meta_parent = disk
            .get_object_path(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"))
            .expect("dst meta path should resolve")
            .parent()
            .expect("dst meta should have a parent")
            .to_path_buf();

        assert_eq!(
            os::fsync_dir_recorder::grouped_batch_sizes(&dst_meta_parent),
            vec![1],
            "enabled non-inline rename_data must route the dst parent fsync through the group commit coordinator"
        );
    }

    #[tokio::test]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn rename_data_non_inline_dst_dir_fsync_group_commit_failure_rolls_back_fresh_put() {
        use tempfile::tempdir;

        let _group_commit = os::set_dst_dir_fsync_group_commit_for_test(true);
        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "grouped-dst-fsync-failure-bucket";
        let object = "dir/object";
        let tmp_object = "tmp-grouped-dst-fsync-failure";
        let version_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").expect("version id should parse");
        let new_data_dir = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("data dir should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new-data")
            .await
            .expect("new tmp data should be written");
        let dst_meta_parent = dir.path().join(bucket).join(object);
        os::fsync_dir_recorder::set_grouped_failure(&dst_meta_parent, io::ErrorKind::PermissionDenied);

        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        let err = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect_err("grouped dst dir fsync failure must fail the fresh PUT");

        assert_eq!(err, DiskError::FileAccessDenied);
        assert!(
            !dst_meta_parent.join(STORAGE_FORMAT_FILE).exists(),
            "fresh PUT rollback must remove the committed xl.meta after grouped dst dir fsync failure"
        );
        assert!(
            !dst_meta_parent.join(new_data_dir.to_string()).exists(),
            "fresh PUT rollback must remove the committed data dir after grouped dst dir fsync failure"
        );
    }

    #[tokio::test]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn rename_data_inline_uses_dst_dir_fsync_group_commit_when_enabled() {
        use tempfile::tempdir;

        let _group_commit = os::set_dst_dir_fsync_group_commit_for_test(true);
        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "grouped-inline-dst-fsync-bucket";
        let object = "dir/inline-object";
        let tmp_object = "tmp-grouped-inline-dst-fsync";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let version_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("version id should parse");
        let new_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-payload")));
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("inline rename_data should commit");

        let dst_meta_parent = disk
            .get_object_path(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"))
            .expect("dst meta path should resolve")
            .parent()
            .expect("dst meta should have a parent")
            .to_path_buf();
        assert_eq!(
            os::fsync_dir_recorder::grouped_batch_sizes(&dst_meta_parent),
            vec![1],
            "enabled inline rename_data must route the dst parent fsync through the group commit coordinator"
        );
        assert!(
            !os::fsync_dir_recorder::was_limited(&dst_meta_parent),
            "enabled grouped dst parent fsync must not also run the direct file-sync limited path"
        );
    }

    #[tokio::test]
    #[serial_test::serial(dst_dir_fsync_group_commit)]
    async fn rename_data_inline_dst_dir_fsync_group_commit_failure_rolls_back_fresh_put() {
        use tempfile::tempdir;

        let _group_commit = os::set_dst_dir_fsync_group_commit_for_test(true);
        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "grouped-inline-dst-fsync-failure-bucket";
        let object = "dir/inline-object";
        let tmp_object = "tmp-grouped-inline-dst-fsync-failure";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let dst_meta_parent = dir.path().join(bucket).join(object);
        os::fsync_dir_recorder::set_grouped_failure(&dst_meta_parent, io::ErrorKind::PermissionDenied);
        let version_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").expect("version id should parse");
        let new_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-payload")));
        let err = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect_err("grouped dst dir fsync failure must fail the fresh inline PUT");

        assert!(
            matches!(err, DiskError::Io(ref io_err) if io_err.kind() == io::ErrorKind::PermissionDenied),
            "grouped dst dir fsync failure must propagate the injected permission error"
        );
        assert!(
            !dst_meta_parent.join(STORAGE_FORMAT_FILE).exists(),
            "fresh inline PUT rollback must remove the committed xl.meta after grouped dst dir fsync failure"
        );
    }

    // Seed a first PUT of `object` (no prior version) through the non-inline
    // rename_data path and return (disk, tempdir). The object dir and any prefix
    // dirs are created during the commit.
    async fn commit_new_object(mode: DurabilityMode, bucket: &str, object: &str) -> (LocalDisk, tempfile::TempDir) {
        use tempfile::tempdir;
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let tmp_object = "tmp-new-object";
        let version_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").expect("version id should parse");
        let new_data_dir = Uuid::parse_str("88888888-8888-8888-8888-888888888888").expect("new data dir should parse");
        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new-data")
            .await
            .expect("new tmp data should be written");

        let _mode = durability_mode_override::set(mode);
        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("rename_data should commit the new object");
        (disk, dir)
    }

    // Stage the bitrot-heal collision: a committed version whose data_dir is
    // present and non-empty, plus a replacement shard staged in tmp for the
    // SAME data_dir (heal repairs in place, it does not mint a new data_dir).
    async fn stage_healing_collision(
        bucket: &str,
        object: &str,
        tmp_object: &str,
    ) -> (LocalDisk, tempfile::TempDir, std::path::PathBuf, FileInfo) {
        use tempfile::tempdir;
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let version_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").expect("version id should parse");
        let data_dir = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").expect("data dir should parse");

        let object_dir = dir.path().join(bucket).join(object);
        let dst_data_dir = object_dir.join(data_dir.to_string());
        fs::create_dir_all(&dst_data_dir)
            .await
            .expect("dst data dir should be created");
        fs::write(dst_data_dir.join("part.1"), b"stale-corrupt-shard")
            .await
            .expect("stale shard should be written");
        let old_fi = test_file_info(object, version_id, Some(data_dir), None);
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), test_meta(old_fi))
            .await
            .expect("old metadata should be written");

        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"healed-shard")
            .await
            .expect("healed shard should be written");

        let new_fi = test_file_info(object, version_id, Some(data_dir), None);
        (disk, dir, dst_data_dir.join("part.1"), new_fi)
    }

    // A healing commit must replace a still-existing destination data dir;
    // without the purge it failed on every attempt and bitrot was never
    // repaired.
    #[tokio::test]
    async fn rename_data_healing_commit_replaces_stale_destination_data_dir() {
        let (disk, _dir, dst_part, mut new_fi) = stage_healing_collision("bucket", "bitrot-object", "tmp-heal-object").await;
        new_fi.set_healing();

        disk.rename_data(RUSTFS_META_TMP_BUCKET, "tmp-heal-object", new_fi, "bucket", "bitrot-object")
            .await
            .expect("a healing rename_data must replace the stale destination data dir");

        let content = fs::read(&dst_part).await.expect("healed shard should be readable");
        assert_eq!(content, b"healed-shard", "the healed shard must replace the stale corrupt content");
    }

    // The purge is healing-gated: an ordinary commit colliding with a
    // non-empty data dir must keep failing loudly.
    #[tokio::test]
    async fn rename_data_non_healing_destination_collision_still_fails() {
        let (disk, _dir, dst_part, new_fi) = stage_healing_collision("bucket", "collision-object", "tmp-collision-object").await;

        disk.rename_data(RUSTFS_META_TMP_BUCKET, "tmp-collision-object", new_fi, "bucket", "collision-object")
            .await
            .expect_err("a non-healing rename_data onto a non-empty destination data dir must fail");

        let content = fs::read(&dst_part).await.expect("stale shard should still be readable");
        assert_eq!(
            content, b"stale-corrupt-shard",
            "a failed non-healing commit must leave the existing content untouched"
        );
    }

    #[tokio::test]
    async fn test_rename_data_new_object_fsyncs_new_ancestor_dirs() {
        // A first PUT under a new prefix must fsync every newly created ancestor
        // directory (prefix dir and bucket dir) so the object dir's own entry
        // survives power loss after ack (rustfs/backlog#922 step 4).
        let bucket = "new-object-bucket";
        let (disk, _dir) = commit_new_object(DurabilityMode::Strict, bucket, "prefix/new-object").await;

        let bucket_dir = disk.get_bucket_path(bucket).expect("bucket path should resolve");
        let prefix_dir = disk.get_object_path(bucket, "prefix").expect("prefix path should resolve");
        assert!(
            os::fsync_dir_recorder::was_fsynced(&prefix_dir),
            "the newly created prefix dir must be fsynced"
        );
        assert!(
            os::fsync_dir_recorder::was_fsynced(&bucket_dir),
            "the bucket dir must be fsynced so the new prefix entry survives power loss"
        );
    }

    #[tokio::test]
    async fn test_rename_data_relaxed_new_object_skips_ancestor_fsync() {
        // Relaxed persists shard payload but leaves metadata/directory commits to
        // the page cache, so a new object must not fsync the ancestor chain.
        let bucket = "new-object-bucket-relaxed";
        let (disk, _dir) = commit_new_object(DurabilityMode::Relaxed, bucket, "prefix/new-object").await;

        let bucket_dir = disk.get_bucket_path(bucket).expect("bucket path should resolve");
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&bucket_dir),
            "relaxed durability must not fsync the bucket dir"
        );
    }

    #[tokio::test]
    async fn windows_and_unix_rename_data_new_inline_object_fsyncs_new_ancestor_dirs() {
        // The inline commit path (fi.data present) has the same mkdir gap as the
        // non-inline path: a first PUT under a new prefix must fsync the newly
        // created prefix and bucket dirs.
        use tempfile::tempdir;
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "new-inline-bucket";
        let object = "prefix/new-inline-object";
        let tmp_object = "tmp-new-inline";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        let tmp_parent = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, tmp_object)
            .expect("tmp parent should resolve");

        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let version_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").expect("version id should parse");
        // fi.data present -> no_inline is false -> the inline commit branch runs.
        let new_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-payload")));
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("inline rename_data should commit the new object");
        assert!(!tmp_parent.exists(), "successful inline commit should remove the empty staging parent");

        let bucket_dir = disk.get_bucket_path(bucket).expect("bucket path should resolve");
        let prefix_dir = disk.get_object_path(bucket, "prefix").expect("prefix path should resolve");
        assert!(
            os::fsync_dir_recorder::was_fsynced(&prefix_dir),
            "the newly created prefix dir must be fsynced on an inline first PUT"
        );
        assert_eq!(
            os::fsync_dir_recorder::was_limited(&prefix_dir),
            cfg!(unix),
            "only Unix inline prefix fsyncs should use the disk file-sync limit"
        );
        assert!(
            os::fsync_dir_recorder::was_fsynced(&bucket_dir),
            "the bucket dir must be fsynced on an inline first PUT"
        );
        assert_eq!(
            os::fsync_dir_recorder::was_limited(&bucket_dir),
            cfg!(unix),
            "only Unix inline bucket fsyncs should use the disk file-sync limit"
        );
    }

    #[tokio::test]
    async fn rename_data_inline_preserves_non_empty_staging_parent() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "inline-staging-sentinel-bucket";
        let object = "inline-object";
        let tmp_object = "inline-stage-with-sentinel";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let tmp_parent = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, tmp_object)
            .expect("tmp parent should resolve");
        fs::create_dir_all(&tmp_parent).await.expect("tmp parent should be created");
        let sentinel = tmp_parent.join("sentinel");
        fs::write(&sentinel, b"keep").await.expect("sentinel should be written");

        let fi = test_file_info(object, Uuid::new_v4(), None, Some(Bytes::from_static(b"inline-payload")));
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, fi, bucket, object)
            .await
            .expect("non-empty staging cleanup must not negate the committed object");

        assert_eq!(fs::read(&sentinel).await.expect("sentinel should remain"), b"keep");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn strict_inline_rename_retains_admission_until_commit_fsync() {
        use std::sync::atomic::{AtomicU8, Ordering};
        use std::sync::mpsc;
        use tempfile::tempdir;
        use tokio::sync::oneshot;

        const FIRST_BARRIER: u8 = 1;
        const SECOND_PREPARATION: u8 = 2;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let mut disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        disk.file_sync_permits = Arc::new(Semaphore::new(1));
        let disk = Arc::new(disk);
        let bucket = "inline-admission-order";
        let first_object = "first-object";
        let second_object = "second-object";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let (first_prepared_tx, first_prepared_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        set_inline_preparation_before_backup(first_object, move || {
            first_prepared_tx.send(()).expect("signal first preparation");
            release_first_rx.recv().expect("wait for queued rename");
        });
        let first_disk = disk.clone();
        let first = tokio::spawn(async move {
            first_disk
                .rename_data(
                    RUSTFS_META_TMP_BUCKET,
                    "first-stage",
                    test_file_info(first_object, Uuid::new_v4(), None, Some(Bytes::from_static(b"first"))),
                    bucket,
                    first_object,
                )
                .await
        });
        tokio::task::spawn_blocking(move || first_prepared_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("first preparation waiter should run")
            .expect("first rename should hold the only disk admission");

        let first_event = Arc::new(AtomicU8::new(0));
        let first_barrier_event = first_event.clone();
        let first_object_dir = disk
            .get_object_path_for_io(bucket, first_object)
            .expect("first object path should resolve");
        os::fsync_dir_recorder::set_before_limited(&first_object_dir, move || {
            let _ = first_barrier_event.compare_exchange(0, FIRST_BARRIER, Ordering::SeqCst, Ordering::SeqCst);
        });

        let second_preparation_event = first_event.clone();
        set_inline_preparation_before_backup(second_object, move || {
            second_preparation_event.fetch_or(SECOND_PREPARATION, Ordering::SeqCst);
        });
        let (second_admission_tx, second_admission_rx) = oneshot::channel();
        set_inline_before_file_sync_admission(second_object, move || {
            second_admission_tx.send(()).expect("signal second admission attempt");
        });
        let second_disk = disk.clone();
        let mut second = Box::pin(async move {
            second_disk
                .rename_data(
                    RUSTFS_META_TMP_BUCKET,
                    "second-stage",
                    test_file_info(second_object, Uuid::new_v4(), None, Some(Bytes::from_static(b"second"))),
                    bucket,
                    second_object,
                )
                .await
        });
        let mut second_admission_rx = Box::pin(second_admission_rx);
        tokio::time::timeout(Duration::from_secs(30), async {
            tokio::select! {
                _ = &mut second => panic!("second rename must wait for disk admission"),
                signal = &mut second_admission_rx => signal.expect("second admission hook should run"),
            }
        })
        .await
        .expect("second rename should reach the admission queue");

        release_first_tx.send(()).expect("release first preparation");
        let (first_result, second_result) = tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(first, second) })
            .await
            .expect("both inline renames should complete");
        first_result
            .expect("first rename task should join")
            .expect("first inline rename should commit");
        second_result.expect("second inline rename should commit");

        assert_eq!(
            first_event.load(Ordering::SeqCst),
            FIRST_BARRIER | SECOND_PREPARATION,
            "the queued rename must not overtake the admitted rename before its commit fsync"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_data_non_inline_retains_destination_identity_across_publications() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "windows-non-inline-identity-bucket";
        let object = "prefix/non-inline-object";
        let tmp_object = "windows-non-inline-identity-stage";
        let version_id = Uuid::parse_str("99999999-1111-2222-3333-aaaaaaaaaaaa").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("99999999-7777-8888-9999-cccccccccccc").expect("old data dir should parse");
        let data_dir = Uuid::parse_str("99999999-4444-5555-6666-bbbbbbbbbbbb").expect("data dir should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let object_dir = disk
            .get_object_path(bucket, object)
            .expect("destination object path should resolve");
        let old_data_path = object_dir.join(old_data_dir.to_string());
        fs::create_dir_all(&old_data_path)
            .await
            .expect("old data directory should be created");
        fs::write(old_data_path.join("part.1"), b"old-payload")
            .await
            .expect("old part should be written");
        let old_meta = test_meta(test_file_info(object, version_id, Some(old_data_dir), None));
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), &old_meta)
            .await
            .expect("old metadata should be written");

        let staged_part = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{data_dir}/part.1"))
            .expect("staged part path should resolve");
        fs::create_dir_all(staged_part.parent().expect("staged part should have a parent"))
            .await
            .expect("staged data directory should be created");
        fs::write(&staged_part, b"payload")
            .await
            .expect("staged part should be written");

        let replacement_dir = disk
            .get_object_path(bucket, "prefix/replacement-object")
            .expect("replacement object path should resolve");
        let staging_parent = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, tmp_object)
            .expect("staging parent should resolve");
        let replacement_staging_parent = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, "replacement-non-inline-stage")
            .expect("replacement staging parent should resolve");
        let staged_metadata = staging_parent.join(STORAGE_FORMAT_FILE);
        let replacement_staged_metadata = staging_parent.join("replacement-xl.meta");
        let object_dir_for_hook = object_dir.clone();
        let replacement_dir_for_hook = replacement_dir.clone();
        let staging_parent_for_hook = staging_parent.clone();
        let replacement_staging_parent_for_hook = replacement_staging_parent.clone();
        let staged_metadata_for_hook = staged_metadata.clone();
        let replacement_staged_metadata_for_hook = replacement_staged_metadata.clone();
        set_rename_data_after_first_publication(&disk.root, bucket, object, move || {
            std::fs::rename(&object_dir_for_hook, &replacement_dir_for_hook)
                .expect_err("the destination object identity must remain pinned until xl.meta commits");
            std::fs::rename(&staging_parent_for_hook, &replacement_staging_parent_for_hook)
                .expect_err("the staging identity must remain pinned across data and xl.meta publication");
            std::fs::rename(&staged_metadata_for_hook, &replacement_staged_metadata_for_hook)
                .expect_err("the prepared xl.meta entry must not be replaceable after data publication");
        });

        let fi = test_file_info(object, version_id, Some(data_dir), None);
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, fi, bucket, object)
            .await
            .expect("non-inline rename_data should commit");

        assert!(!replacement_dir.exists(), "the destination object directory must not be replaced");
        assert!(
            !staging_parent.exists(),
            "successful commit should remove the empty staging parent after releasing its guard"
        );
        assert!(
            !replacement_staging_parent.exists(),
            "the staging parent must not be replaced between data and metadata publication"
        );
        assert!(
            !replacement_staged_metadata.exists(),
            "the prepared metadata source must remain the committed entry"
        );
        assert_eq!(
            fs::read(object_dir.join(data_dir.to_string()).join("part.1"))
                .await
                .expect("published part should be readable"),
            b"payload"
        );
        assert!(
            object_dir.join(STORAGE_FORMAT_FILE).exists(),
            "metadata must publish into the pinned object directory"
        );
        assert_eq!(
            fs::read(old_data_path.join(STORAGE_FORMAT_FILE_BACKUP))
                .await
                .expect("rollback metadata should be written while the destination guard is held"),
            old_meta
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_data_replaces_hard_linked_legacy_destination_metadata() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "windows-hard-linked-destination-bucket";
        let object = "prefix/inline-object";
        let tmp_object = "windows-hard-linked-destination-stage";
        let version_id = Uuid::parse_str("aaaaaaaa-7777-8888-9999-bbbbbbbbbbbb").expect("version id should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let object_dir = disk
            .get_object_path(bucket, object)
            .expect("destination object path should resolve");
        fs::create_dir_all(&object_dir)
            .await
            .expect("destination object directory should be created");
        let destination = object_dir.join(STORAGE_FORMAT_FILE);
        let old_meta = test_meta(test_file_info(object, version_id, None, Some(Bytes::from_static(b"old-inline-payload"))));
        fs::write(&destination, &old_meta)
            .await
            .expect("destination metadata should be written");
        let legacy_backup = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("legacy-inline-rollback/{STORAGE_FORMAT_FILE_BACKUP}"))
            .expect("legacy rollback backup path should resolve");
        fs::create_dir_all(legacy_backup.parent().expect("legacy rollback backup should have a parent"))
            .await
            .expect("legacy rollback directory should be created");
        std::fs::hard_link(&destination, &legacy_backup).expect("legacy rollback backup hard link should be created");

        let new_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"new-inline-payload")));
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("a legacy hard-linked destination must be safely replaced");

        assert_eq!(
            fs::read(&legacy_backup)
                .await
                .expect("legacy rollback backup should remain readable"),
            old_meta,
            "replacing the destination must not mutate its legacy rollback backup"
        );
        let raw = fs::read(&destination).await.expect("published metadata should be readable");
        assert_ne!(raw, old_meta, "the new metadata must replace the legacy hard-linked destination");
        FileMeta::load(&raw)
            .expect("published metadata should parse")
            .find_version(Some(version_id))
            .expect("published metadata must contain the committed version");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_data_supersedes_a_hard_linked_rollback_backup() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "windows-hard-linked-backup-bucket";
        let object = "prefix/non-inline-object";
        let tmp_object = "windows-hard-linked-backup-stage";
        let version_id = Uuid::parse_str("cccccccc-7777-8888-9999-dddddddddddd").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("eeeeeeee-1111-2222-3333-aaaaaaaaaaaa").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("ffffffff-4444-5555-6666-bbbbbbbbbbbb").expect("new data dir should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let object_dir = disk
            .get_object_path(bucket, object)
            .expect("destination object path should resolve");
        let old_data_path = object_dir.join(old_data_dir.to_string());
        fs::create_dir_all(&old_data_path)
            .await
            .expect("old data directory should be created");
        fs::write(old_data_path.join("part.1"), b"old-payload")
            .await
            .expect("old part should be written");
        let old_meta = test_meta(test_file_info(object, version_id, Some(old_data_dir), None));
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), &old_meta)
            .await
            .expect("old metadata should be written");

        let victim = dir.path().join("hard-link-victim-backup");
        let victim_bytes = b"must-not-be-truncated";
        fs::write(&victim, victim_bytes)
            .await
            .expect("backup victim should be written");
        let backup_path = old_data_path.join(STORAGE_FORMAT_FILE_BACKUP);
        std::fs::hard_link(&victim, &backup_path).expect("rollback backup hard link should be created");

        let staged_part = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{new_data_dir}/part.1"))
            .expect("staged part path should resolve");
        fs::create_dir_all(staged_part.parent().expect("staged part should have a parent"))
            .await
            .expect("staged data directory should be created");
        fs::write(&staged_part, b"new-payload")
            .await
            .expect("staged part should be written");

        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("the rollback backup entry should be safely superseded");

        assert_eq!(
            fs::read(&victim).await.expect("backup victim should remain readable"),
            victim_bytes,
            "publishing the rollback backup must not truncate another hard link"
        );
        assert_eq!(
            fs::read(&backup_path).await.expect("rollback backup should be readable"),
            old_meta,
            "the superseded backup entry must contain the exact previous metadata"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_data_inline_publishes_via_guarded_rename() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "windows-inline-bucket";
        let object = "prefix/inline-object";
        let tmp_object = "windows-inline-stage";
        let version_id = Uuid::parse_str("aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb").expect("version id should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        let source = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{STORAGE_FORMAT_FILE}"))
            .expect("staged metadata path should resolve");
        let destination = disk
            .get_object_path(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"))
            .expect("destination metadata path should resolve");
        let replacement_source = source.with_file_name("replacement-xl.meta");
        let fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-payload")));
        let source_pinned_before_write = Arc::new(AtomicBool::new(false));
        let source_pinned_before_write_in_hook = Arc::clone(&source_pinned_before_write);
        let source_for_hook = source.clone();
        let replacement_source_for_hook = replacement_source.clone();
        os::windows_rename_test_hooks::install_before_source_write(&source, move || {
            source_pinned_before_write_in_hook.store(true, Ordering::Release);
            std::fs::rename(&source_for_hook, &replacement_source_for_hook)
                .expect_err("the staged metadata entry must be pinned before its first write");
            std::fs::OpenOptions::new()
                .write(true)
                .open(&source_for_hook)
                .expect_err("the staged metadata entry must reject a second writer before its first write");
        });
        let guarded_commit_seen = Arc::new(AtomicBool::new(false));
        let guarded_commit_seen_in_hook = Arc::clone(&guarded_commit_seen);
        os::windows_rename_test_hooks::install_before_publication(&destination, move || {
            guarded_commit_seen_in_hook.store(true, Ordering::Release);
        });

        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, fi, bucket, object)
            .await
            .expect("inline metadata must publish after its writer is closed");

        assert!(
            source_pinned_before_write.load(Ordering::Acquire),
            "the production inline writer must pin the staged metadata entry before writing"
        );
        assert!(
            guarded_commit_seen.load(Ordering::Acquire),
            "the production inline commit must use guarded handle-relative publication"
        );
        assert!(!source.exists(), "successful publication must remove the staged metadata path");
        assert!(
            !replacement_source.exists(),
            "the pinned staged metadata entry must not be replaceable before its first write"
        );
        let raw = std::fs::read(&destination).expect("read published metadata");
        let metadata = FileMeta::load(&raw).expect("published metadata must parse");
        metadata
            .find_version(Some(version_id))
            .expect("published metadata must contain the committed version");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_rename_data_inline_retains_destination_identity_across_backup_and_commit() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "windows-inline-identity-bucket";
        let object = "prefix/inline-object";
        let tmp_object = "windows-inline-identity-stage";
        let version_id = Uuid::parse_str("aaaaaaaa-4444-5555-6666-bbbbbbbbbbbb").expect("version id should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let object_dir = disk
            .get_object_path(bucket, object)
            .expect("destination object path should resolve");
        fs::create_dir_all(&object_dir)
            .await
            .expect("destination object directory should be created");
        let old_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"old-inline-payload")));
        let old_meta = test_meta(old_fi);
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), &old_meta)
            .await
            .expect("old metadata should be written");

        let replacement_dir = disk
            .get_object_path(bucket, "prefix/replacement-inline-object")
            .expect("replacement object path should resolve");
        let staging_parent = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, tmp_object)
            .expect("staging parent should resolve");
        let staged_metadata = staging_parent.join(STORAGE_FORMAT_FILE);
        let replacement_staged_metadata = staging_parent.join("replacement-xl.meta");
        let object_dir_for_hook = object_dir.clone();
        let replacement_dir_for_hook = replacement_dir.clone();
        let staged_metadata_for_hook = staged_metadata.clone();
        let replacement_staged_metadata_for_hook = replacement_staged_metadata.clone();
        set_rename_data_after_first_publication(&disk.root, bucket, object, move || {
            std::fs::rename(&object_dir_for_hook, &replacement_dir_for_hook)
                .expect_err("the destination object identity must remain pinned after publishing its rollback backup");
            std::fs::rename(&staged_metadata_for_hook, &replacement_staged_metadata_for_hook)
                .expect_err("the prepared inline xl.meta must not be replaceable after backup publication");
        });

        let new_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"new-inline-payload")));
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("inline rename_data should commit");

        assert!(!replacement_dir.exists(), "the destination object directory must not be replaced");
        assert!(
            !replacement_staged_metadata.exists(),
            "the prepared inline metadata source must remain the committed entry"
        );
        let raw = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("published metadata should be readable");
        assert_ne!(raw, old_meta, "the new metadata must replace the old inline version");
        let metadata = FileMeta::load(&raw).expect("published metadata should parse");
        metadata
            .find_version(Some(version_id))
            .expect("published metadata must contain the committed version");
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_cancelled_destination_preparation_releases_waiter_but_retains_volume_guard() {
        use std::sync::{Arc, mpsc};

        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let bucket = "windows-cancelled-preparation-bucket";
        let object = "prefix/inline-object";
        let tmp_object = "windows-cancelled-preparation-stage";
        let version_id = Uuid::parse_str("cccccccc-1111-2222-3333-dddddddddddd").expect("version id should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        let object_dir = disk
            .get_object_path(bucket, object)
            .expect("destination object path should resolve");
        let destination = object_dir.join(STORAGE_FORMAT_FILE);
        let fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-payload")));
        let _mode = durability_mode_override::set(DurabilityMode::Relaxed);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        set_destination_commit_directory_preparation(&object_dir, move || {
            entered_tx.send(()).expect("signal destination preparation entry");
            release_rx.recv().expect("wait until cancellation has been observed");
        });

        let operation_disk = Arc::clone(&disk);
        let rename = tokio::spawn(async move {
            operation_disk
                .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, fi, bucket, object)
                .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("destination preparation waiter should run")
            .expect("rename_data must reach destination preparation");

        rename.abort();
        let cancellation = tokio::time::timeout(Duration::from_secs(1), rename)
            .await
            .expect("the async waiter should observe cancellation without waiting for destination preparation")
            .expect_err("the aborted rename task should be cancelled");
        assert!(cancellation.is_cancelled(), "the rename waiter should report cancellation");
        let volume_lock = os::disk_volume_mutation_lock(&disk.root, bucket);
        let volume_guard_released_early = Arc::clone(&volume_lock).try_write_owned().is_ok();
        release_tx
            .send(())
            .expect("release destination preparation after cancellation");

        assert!(
            !volume_guard_released_early,
            "cancellation must not release the volume guard while destination preparation can still mutate the namespace"
        );
        let _exclusive = tokio::time::timeout(Duration::from_secs(5), volume_lock.write_owned())
            .await
            .expect("volume guard must be released after cancelled destination preparation finishes");
        assert!(
            !destination.exists(),
            "cancellation during preparation must prevent the outer transaction from publishing metadata"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_and_unix_cancelled_staged_metadata_write_serializes_same_object_retry() {
        use std::sync::{Arc, mpsc};

        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let bucket = "cancelled-staged-metadata-bucket";
        let object = "prefix/non-inline-object";
        let tmp_object = "cancelled-staged-metadata-stage";
        let version_id = Uuid::parse_str("dddddddd-1111-2222-3333-eeeeeeeeeeee").expect("version id should parse");
        let data_dir = Uuid::parse_str("ffffffff-1111-2222-3333-aaaaaaaaaaaa").expect("data dir should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let staged_part = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{data_dir}/part.1"))
            .expect("staged data path should resolve");
        fs::create_dir_all(staged_part.parent().expect("staged part should have a parent"))
            .await
            .expect("staged data directory should be created");
        fs::write(&staged_part, b"new-payload")
            .await
            .expect("staged part should be written");
        let staged_metadata = disk
            .get_object_path_for_io(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{STORAGE_FORMAT_FILE}"))
            .expect("staged metadata path should resolve");

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        set_owned_file_write_before_open(&staged_metadata, move || {
            entered_tx.send(()).expect("signal staged metadata write entry");
            release_rx.recv().expect("wait until cancellation has been observed");
        });

        let mut cancelled_fi = test_file_info(object, version_id, Some(data_dir), None);
        cancelled_fi
            .metadata
            .insert("test-generation".to_string(), "cancelled".to_string());
        let cancelled_disk = Arc::clone(&disk);
        let cancelled = tokio::spawn(async move {
            cancelled_disk
                .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, cancelled_fi, bucket, object)
                .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("staged metadata waiter should run")
            .expect("rename_data must reach the staged metadata write");
        cancelled.abort();
        let cancellation = tokio::time::timeout(Duration::from_secs(1), cancelled)
            .await
            .expect("the async waiter should observe cancellation without waiting for the staged writer")
            .expect_err("the aborted rename task should be cancelled");
        assert!(cancellation.is_cancelled(), "the rename waiter should report cancellation");

        let mut retry_fi = test_file_info(object, version_id, Some(data_dir), None);
        retry_fi.metadata.insert("test-generation".to_string(), "retry".to_string());
        let retry_disk = Arc::clone(&disk);
        let mut retry = tokio::spawn(async move {
            retry_disk
                .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, retry_fi, bucket, object)
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut retry).await.is_err(),
            "the retry must wait while the cancelled staged writer still owns the object namespace"
        );
        release_tx.send(()).expect("release cancelled staged metadata writer");
        tokio::time::timeout(Duration::from_secs(10), retry)
            .await
            .expect("retry should finish after the cancelled writer releases the namespace")
            .expect("retry task should not panic")
            .expect("same-object retry should commit successfully");

        let destination_metadata = disk
            .get_object_path(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"))
            .expect("destination metadata path should resolve");
        let raw = fs::read(destination_metadata)
            .await
            .expect("retried metadata should be readable");
        let metadata = FileMeta::load(&raw).expect("retried metadata should parse");
        let (_, version) = metadata
            .find_version(Some(version_id))
            .expect("retried metadata should contain the requested version");
        assert_eq!(
            version
                .object
                .expect("non-inline version should contain object metadata")
                .meta_user
                .get("test-generation")
                .map(String::as_str),
            Some("retry"),
            "the cancelled writer must not overwrite metadata committed by the retry"
        );
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_cancelled_inline_publication_releases_waiter_but_retains_volume_guard() {
        use std::sync::{Arc, mpsc};

        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let bucket = "windows-cancelled-inline-bucket";
        let object = "prefix/inline-object";
        let tmp_object = "windows-cancelled-inline-stage";
        let version_id = Uuid::parse_str("bbbbbbbb-1111-2222-3333-cccccccccccc").expect("version id should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        let destination = disk
            .get_object_path(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"))
            .expect("destination metadata path should resolve");
        let fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-payload")));
        let _mode = durability_mode_override::set(DurabilityMode::Relaxed);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        os::windows_rename_test_hooks::install_before_publication(&destination, move || {
            entered_tx.send(()).expect("signal publication hook entry");
            release_rx.recv().expect("wait until cancellation has been observed");
        });

        let operation_disk = Arc::clone(&disk);
        let rename = tokio::spawn(async move {
            operation_disk
                .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, fi, bucket, object)
                .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("publication entry waiter should run")
            .expect("publication must reach its guarded commit");

        rename.abort();
        let cancellation = tokio::time::timeout(Duration::from_secs(1), rename)
            .await
            .expect("the async waiter should observe cancellation without waiting for publication")
            .expect_err("the aborted rename task should be cancelled");
        assert!(cancellation.is_cancelled(), "the rename waiter should report cancellation");
        let volume_lock = os::disk_volume_mutation_lock(&disk.root, bucket);
        let volume_guard_released_early = Arc::clone(&volume_lock).try_write_owned().is_ok();
        release_tx.send(()).expect("release publication after cancellation");

        assert!(
            !volume_guard_released_early,
            "cancellation must not release the volume guard while an inline publication can still commit"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while !destination.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("publication should complete before the cancelled task releases its guard");
        let _exclusive = tokio::time::timeout(Duration::from_secs(5), volume_lock.write_owned())
            .await
            .expect("volume guard must be released after publication finishes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_and_unix_cancelled_non_inline_backup_write_retains_its_volume_guard() {
        use std::sync::{Arc, mpsc};

        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let bucket = "cancelled-backup-write-bucket";
        let object = "prefix/non-inline-object";
        let tmp_object = "cancelled-backup-write-stage";
        let version_id = Uuid::parse_str("aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("cccccccc-1111-2222-3333-dddddddddddd").expect("data dir should parse");
        let new_data_dir = Uuid::parse_str("eeeeeeee-1111-2222-3333-ffffffffffff").expect("data dir should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let object_dir = disk
            .get_object_path(bucket, object)
            .expect("destination object path should resolve");
        let old_meta = test_meta(test_file_info(object, version_id, Some(old_data_dir), None));
        fs::create_dir_all(object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data directory should be created");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        let staged_part = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{new_data_dir}/part.1"))
            .expect("staged data path should resolve");
        fs::create_dir_all(staged_part.parent().expect("staged part should have a parent"))
            .await
            .expect("staged data directory should be created");
        fs::write(&staged_part, b"new-payload")
            .await
            .expect("staged part should be written");

        let backup_path = object_dir.join(old_data_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
        let backup_hook_path = disk
            .get_object_path_for_io(bucket, &path_join_buf(&[object, &old_data_dir.to_string(), STORAGE_FORMAT_FILE_BACKUP]))
            .expect("rollback backup io path should resolve");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        set_owned_file_write_before_open(&backup_hook_path, move || {
            entered_tx.send(()).expect("signal backup write entry");
            release_rx.recv().expect("wait until cancellation has been observed");
        });

        let operation_disk = Arc::clone(&disk);
        let rename = tokio::spawn(async move {
            operation_disk
                .rename_data(
                    RUSTFS_META_TMP_BUCKET,
                    tmp_object,
                    test_file_info(object, version_id, Some(new_data_dir), None),
                    bucket,
                    object,
                )
                .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("backup write waiter should run")
            .expect("rename_data must reach the rollback backup write");

        rename.abort();
        let cancellation = tokio::time::timeout(Duration::from_secs(1), rename)
            .await
            .expect("the async waiter should observe cancellation without waiting for the backup write")
            .expect_err("the aborted rename task should be cancelled");
        assert!(cancellation.is_cancelled(), "the rename waiter should report cancellation");
        let volume_lock = os::disk_volume_mutation_lock(&disk.root, bucket);
        let volume_guard_released_early = Arc::clone(&volume_lock).try_write_owned().is_ok();
        release_tx.send(()).expect("release rollback backup write after cancellation");

        assert!(
            !volume_guard_released_early,
            "cancellation must not release the volume guard while a rollback backup can still be written"
        );
        let _exclusive = tokio::time::timeout(Duration::from_secs(5), volume_lock.write_owned())
            .await
            .expect("volume guard must be released after the rollback backup write finishes");
        assert_eq!(
            fs::read(&backup_path).await.expect("rollback backup should be readable"),
            old_meta,
            "the guarded write must preserve the exact old metadata"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_and_unix_delete_volume_waits_for_in_flight_rename_data_commit() {
        use std::sync::{Arc, mpsc};

        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let bucket = "delete-volume-commit-order-bucket";
        let object = "prefix/non-inline-object";
        let tmp_object = "delete-volume-commit-order-stage";
        let version_id = Uuid::parse_str("aaaaaaaa-2222-3333-4444-bbbbbbbbbbbb").expect("version id should parse");
        let data_dir = Uuid::parse_str("cccccccc-2222-3333-4444-dddddddddddd").expect("data dir should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let staged_part = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{data_dir}/part.1"))
            .expect("staged data path should resolve");
        fs::create_dir_all(staged_part.parent().expect("staged part should have a parent"))
            .await
            .expect("staged data directory should be created");
        fs::write(&staged_part, b"new-payload")
            .await
            .expect("staged part should be written");

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        set_rename_data_after_first_publication(&disk.root, bucket, object, move || {
            entered_tx.send(()).expect("signal first publication");
            release_rx.recv().expect("wait while delete_volume is blocked");
        });

        let rename_disk = Arc::clone(&disk);
        let rename = tokio::spawn(async move {
            rename_disk
                .rename_data(
                    RUSTFS_META_TMP_BUCKET,
                    tmp_object,
                    test_file_info(object, version_id, Some(data_dir), None),
                    bucket,
                    object,
                )
                .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("publication entry waiter should run")
            .expect("rename_data must publish its data before metadata commit");

        let delete_disk = Arc::clone(&disk);
        let mut delete = tokio::spawn(async move { delete_disk.delete_volume(bucket, false).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut delete).await.is_err(),
            "delete_volume must wait for the in-flight metadata commit"
        );

        release_tx.send(()).expect("release metadata commit");
        rename
            .await
            .expect("rename_data task should finish")
            .expect("rename_data should commit successfully");
        let delete_err = tokio::time::timeout(Duration::from_secs(5), delete)
            .await
            .expect("delete_volume should finish after the commit")
            .expect("delete_volume task should not panic")
            .expect_err("the committed object must keep the bucket non-empty");
        assert_eq!(delete_err, DiskError::VolumeNotEmpty);
    }

    #[tokio::test]
    #[serial_test::serial(rename_data_deleted_bucket)]
    async fn windows_and_unix_rename_data_non_inline_does_not_recreate_bucket_deleted_before_commit() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "deleted-before-non-inline-commit";
        let object = "prefix/object";
        let tmp_object = "tmp-non-inline-delete-race";
        let data_dir = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").expect("data dir should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let staged_data = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{data_dir}/part.1"))
            .expect("staged data path should resolve");
        fs::create_dir_all(staged_data.parent().expect("staged data should have a parent"))
            .await
            .expect("staged data parent should be created");
        fs::write(&staged_data, b"payload")
            .await
            .expect("staged shard should be written");

        let bucket_path = disk.get_bucket_path(bucket).expect("bucket path should resolve");
        set_rename_data_remove_dst_base_before_commit(object, &bucket_path);
        let fi = test_file_info(object, Uuid::new_v4(), Some(data_dir), None);
        let err = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, fi, bucket, object)
            .await
            .expect_err("commit must fail after the destination bucket is deleted");

        assert_eq!(err, DiskError::FileNotFound);
        assert!(!bucket_path.exists(), "rename_data must not recreate the deleted bucket");
        assert!(staged_data.exists(), "failed commit must preserve the staged shard");
    }

    #[tokio::test]
    #[serial_test::serial(rename_data_deleted_bucket)]
    async fn windows_and_unix_rename_data_inline_does_not_recreate_bucket_deleted_before_commit() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "deleted-before-inline-commit";
        let object = "prefix/object";
        let tmp_object = "tmp-inline-delete-race";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let bucket_path = disk.get_bucket_path(bucket).expect("bucket path should resolve");
        set_rename_data_remove_dst_base_before_commit(object, &bucket_path);
        let fi = test_file_info(object, Uuid::new_v4(), None, Some(Bytes::from_static(b"inline payload")));
        let err = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, fi, bucket, object)
            .await
            .expect_err("inline commit must fail after the destination bucket is deleted");

        assert_eq!(err, DiskError::FileNotFound);
        assert!(!bucket_path.exists(), "inline rename_data must not recreate the deleted bucket");
        assert!(
            disk.get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{STORAGE_FORMAT_FILE}"))
                .expect("staged metadata path should resolve")
                .exists(),
            "failed inline commit must preserve staged metadata"
        );
    }

    #[test]
    fn test_resolve_durability_mode_mapping() {
        // Default: nothing set -> strict (current main behavior).
        assert_eq!(resolve_durability_mode(None, DEFAULT_RUSTFS_DRIVE_SYNC_ENABLE), DurabilityMode::Strict);
        // Legacy switch compatibility mapping.
        assert_eq!(resolve_durability_mode(None, true), DurabilityMode::Strict);
        assert_eq!(resolve_durability_mode(None, false), DurabilityMode::LegacyOff);
        // Explicit mode wins over the legacy switch.
        assert_eq!(resolve_durability_mode(Some("strict".into()), false), DurabilityMode::Strict);
        assert_eq!(resolve_durability_mode(Some("relaxed".into()), true), DurabilityMode::Relaxed);
        assert_eq!(resolve_durability_mode(Some("none".into()), true), DurabilityMode::None);
        // Case- and whitespace-tolerant.
        assert_eq!(resolve_durability_mode(Some(" RELAXED ".into()), true), DurabilityMode::Relaxed);
        // Invalid values fall back to the legacy switch, then the default.
        assert_eq!(resolve_durability_mode(Some("bogus".into()), true), DurabilityMode::Strict);
        assert_eq!(resolve_durability_mode(Some("bogus".into()), false), DurabilityMode::LegacyOff);
        assert_eq!(resolve_durability_mode(Some(String::new()), true), DurabilityMode::Strict);
    }

    #[test]
    fn test_durability_mode_sync_gates() {
        // Strict = current main behavior: every commit point synced.
        assert!(DurabilityMode::Strict.syncs_data_shards());
        assert!(DurabilityMode::Strict.syncs_commit_metadata());
        // Relaxed keeps payload durability, drops metadata-commit fsyncs.
        assert!(DurabilityMode::Relaxed.syncs_data_shards());
        assert!(!DurabilityMode::Relaxed.syncs_commit_metadata());
        // None and the legacy full-off switch sync nothing on the data path.
        assert!(!DurabilityMode::None.syncs_data_shards());
        assert!(!DurabilityMode::None.syncs_commit_metadata());
        assert!(!DurabilityMode::LegacyOff.syncs_data_shards());
        assert!(!DurabilityMode::LegacyOff.syncs_commit_metadata());
    }

    #[test]
    fn test_system_critical_volume_classification() {
        // System namespaces are pinned.
        assert!(is_system_critical_volume(RUSTFS_META_BUCKET));
        assert!(is_system_critical_volume(&format!("{RUSTFS_META_BUCKET}/buckets")));
        assert!(is_system_critical_volume(&format!("{RUSTFS_META_BUCKET}/config")));
        assert!(is_system_critical_volume(super::super::MIGRATING_META_BUCKET));
        // Scratch namespaces stage user object data and follow the tier.
        assert!(!is_system_critical_volume(RUSTFS_META_TMP_BUCKET));
        assert!(!is_system_critical_volume(RUSTFS_META_TMP_DELETED_BUCKET));
        assert!(!is_system_critical_volume(super::super::RUSTFS_META_MULTIPART_BUCKET));
        // User buckets follow the tier; similarly-prefixed names are not meta.
        assert!(!is_system_critical_volume("my-bucket"));
        assert!(!is_system_critical_volume(".rustfs.sys-lookalike"));
    }

    #[test]
    fn test_effective_durability_pins_system_volumes() {
        {
            let _mode = durability_mode_override::set(DurabilityMode::Relaxed);
            assert_eq!(effective_durability("user-bucket"), DurabilityMode::Relaxed);
            assert_eq!(effective_durability(super::super::RUSTFS_META_MULTIPART_BUCKET), DurabilityMode::Relaxed);
            assert_eq!(effective_durability(RUSTFS_META_TMP_BUCKET), DurabilityMode::Relaxed);
            assert_eq!(effective_durability(RUSTFS_META_BUCKET), DurabilityMode::Strict);
        }
        {
            let _mode = durability_mode_override::set(DurabilityMode::None);
            assert_eq!(effective_durability("user-bucket"), DurabilityMode::None);
            assert_eq!(effective_durability(RUSTFS_META_BUCKET), DurabilityMode::Strict);
        }
        {
            // The legacy full-off switch keeps its historical semantics:
            // nothing is pinned, not even system-critical namespaces.
            let _mode = durability_mode_override::set(DurabilityMode::LegacyOff);
            assert_eq!(effective_durability("user-bucket"), DurabilityMode::LegacyOff);
            assert_eq!(effective_durability(RUSTFS_META_BUCKET), DurabilityMode::LegacyOff);
        }
    }

    /// Removes the bucket's durability override when dropped so a test can
    /// never leak its override into another test's lookup.
    struct BucketOverrideGuard(&'static str);

    impl BucketOverrideGuard {
        fn set(bucket: &'static str, mode: DurabilityMode) -> Self {
            bucket_durability::set(bucket, Some(mode));
            Self(bucket)
        }
    }

    impl Drop for BucketOverrideGuard {
        fn drop(&mut self) {
            bucket_durability::set(self.0, None);
        }
    }

    #[test]
    fn test_effective_durability_bucket_override() {
        // Global strict + per-bucket relaxed: only the named bucket drops.
        {
            let _mode = durability_mode_override::set(DurabilityMode::Strict);
            let _guard = BucketOverrideGuard::set("hp5b-override-relaxed", DurabilityMode::Relaxed);
            assert_eq!(effective_durability("hp5b-override-relaxed"), DurabilityMode::Relaxed);
            assert_eq!(effective_durability("hp5b-other-bucket"), DurabilityMode::Strict);
            assert_eq!(effective_durability(RUSTFS_META_BUCKET), DurabilityMode::Strict);
        }
        // Override cleared: the bucket follows the global mode again (a new
        // PUT after a config change resolves the new tier).
        {
            let _mode = durability_mode_override::set(DurabilityMode::Strict);
            assert_eq!(effective_durability("hp5b-override-relaxed"), DurabilityMode::Strict);
        }
        // Global relaxed + per-bucket strict: overrides can raise durability.
        {
            let _mode = durability_mode_override::set(DurabilityMode::Relaxed);
            let _guard = BucketOverrideGuard::set("hp5b-override-strict", DurabilityMode::Strict);
            assert_eq!(effective_durability("hp5b-override-strict"), DurabilityMode::Strict);
            assert_eq!(effective_durability("hp5b-other-bucket"), DurabilityMode::Relaxed);
        }
    }

    #[test]
    fn test_bucket_durability_refuses_system_namespaces() {
        let _mode = durability_mode_override::set(DurabilityMode::Strict);

        // System-critical and scratch namespaces can never carry an override.
        bucket_durability::set(RUSTFS_META_BUCKET, Some(DurabilityMode::Relaxed));
        bucket_durability::set(&format!("{RUSTFS_META_BUCKET}/buckets"), Some(DurabilityMode::None));
        bucket_durability::set(RUSTFS_META_TMP_BUCKET, Some(DurabilityMode::Relaxed));
        bucket_durability::set(super::super::RUSTFS_META_MULTIPART_BUCKET, Some(DurabilityMode::Relaxed));
        bucket_durability::set("", Some(DurabilityMode::Relaxed));

        assert_eq!(bucket_durability::lookup(RUSTFS_META_BUCKET), None);
        assert_eq!(bucket_durability::lookup(RUSTFS_META_TMP_BUCKET), None);
        assert_eq!(effective_durability(RUSTFS_META_BUCKET), DurabilityMode::Strict);

        // The legacy full-off tier is process-wide only: registering it per
        // bucket is dropped, not stored.
        bucket_durability::set("hp5b-legacy-refused", Some(DurabilityMode::LegacyOff));
        assert_eq!(bucket_durability::lookup("hp5b-legacy-refused"), None);
    }

    #[test]
    fn test_effective_durability_legacy_off_ignores_bucket_overrides() {
        let _mode = durability_mode_override::set(DurabilityMode::LegacyOff);
        let _guard = BucketOverrideGuard::set("hp5b-legacy-bucket", DurabilityMode::Strict);
        // The legacy switch keeps its historical semantics bit for bit.
        assert_eq!(effective_durability("hp5b-legacy-bucket"), DurabilityMode::LegacyOff);
    }

    /// HP-5b behavior regression: with the global mode at strict (the
    /// default), a bucket override to relaxed must skip the metadata-commit
    /// dir fsync for that bucket only, and clearing the override must restore
    /// the strict behavior for the next write.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_write_all_meta_bucket_override_relaxed_then_cleared() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let overridden = "hp5b-relaxed-write-bucket";
        let untouched = "hp5b-strict-write-bucket";
        ensure_test_volume(&disk, overridden).await;
        ensure_test_volume(&disk, untouched).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let meta_path = format!("dir/object/{STORAGE_FORMAT_FILE}");

        {
            let _guard = BucketOverrideGuard::set("hp5b-relaxed-write-bucket", DurabilityMode::Relaxed);

            disk.write_all_meta(overridden, &meta_path, b"payload", true)
                .await
                .expect("write_all_meta should succeed");
            let overridden_parent = disk
                .get_object_path(overridden, &meta_path)
                .expect("dst path should resolve")
                .parent()
                .expect("dst file should have a parent")
                .to_path_buf();
            assert!(
                !os::fsync_dir_recorder::was_fsynced(&overridden_parent),
                "bucket override to relaxed must skip the metadata-commit dir fsync"
            );

            // A bucket without an override keeps the strict default.
            disk.write_all_meta(untouched, &meta_path, b"payload", true)
                .await
                .expect("write_all_meta should succeed");
            let untouched_parent = disk
                .get_object_path(untouched, &meta_path)
                .expect("dst path should resolve")
                .parent()
                .expect("dst file should have a parent")
                .to_path_buf();
            assert!(
                os::fsync_dir_recorder::was_fsynced(&untouched_parent),
                "buckets without an override must keep the strict commit fsyncs"
            );
        }

        // Override cleared (guard dropped): the next write is strict again.
        let second_meta_path = format!("dir/object-after-clear/{STORAGE_FORMAT_FILE}");
        disk.write_all_meta(overridden, &second_meta_path, b"payload", true)
            .await
            .expect("write_all_meta should succeed");
        let after_clear_parent = disk
            .get_object_path(overridden, &second_meta_path)
            .expect("dst path should resolve")
            .parent()
            .expect("dst file should have a parent")
            .to_path_buf();
        assert!(
            os::fsync_dir_recorder::was_fsynced(&after_clear_parent),
            "clearing the override must restore strict fsyncs for new writes"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_write_all_meta_relaxed_skips_dst_parent_dir_fsync() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Relaxed);

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "relaxed-meta-bucket";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let meta_path = format!("dir/object/{STORAGE_FORMAT_FILE}");
        disk.write_all_meta(bucket, &meta_path, b"payload", true)
            .await
            .expect("write_all_meta should succeed");

        let dst_file_path = disk.get_object_path(bucket, &meta_path).expect("dst path should resolve");
        assert_eq!(
            tokio::fs::read(&dst_file_path).await.expect("xl.meta should exist"),
            b"payload",
            "relaxed mode must not change what gets written, only what gets fsynced"
        );

        let dst_parent = dst_file_path.parent().expect("dst file should have a parent").to_path_buf();
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&dst_parent),
            "relaxed mode must skip the metadata-commit dir fsync on user volumes"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_write_all_public_relaxed_pins_system_volume() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Relaxed);

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let user_bucket = "relaxed-public-bucket";
        ensure_test_volume(&disk, user_bucket).await;

        // User volume: the direct write follows the relaxed tier.
        disk.write_all(user_bucket, "config/settings.json", Bytes::from_static(b"payload"))
            .await
            .expect("write_all should succeed");
        let user_parent = disk
            .get_object_path(user_bucket, "config/settings.json")
            .expect("file path should resolve")
            .parent()
            .expect("file should have a parent")
            .to_path_buf();
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&user_parent),
            "relaxed mode must skip the dir fsync for direct writes into user volumes"
        );

        // System-critical volume: pinned to strict regardless of the tier.
        disk.write_all(RUSTFS_META_BUCKET, "buckets/test/bucket-metadata", Bytes::from_static(b"payload"))
            .await
            .expect("write_all should succeed");
        let meta_parent = disk
            .get_object_path(RUSTFS_META_BUCKET, "buckets/test/bucket-metadata")
            .expect("meta path should resolve")
            .parent()
            .expect("meta file should have a parent")
            .to_path_buf();
        assert!(
            os::fsync_dir_recorder::was_fsynced(&meta_parent),
            "system-critical writes must stay fully synced under relaxed mode"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_rename_data_relaxed_keeps_shard_sync_skips_commit_fsyncs() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Relaxed);

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "relaxed-rename-bucket";
        let object = "dir/object";
        let tmp_object = "tmp-relaxed-write";
        let version_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("88888888-8888-8888-8888-888888888888").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("99999999-9999-9999-9999-999999999999").expect("new data dir should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let dst_object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(dst_object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), test_meta(old_fi))
            .await
            .expect("old metadata should be written");

        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new-data")
            .await
            .expect("new tmp data should be written");

        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        let resp = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("rename_data should commit");
        assert_eq!(resp.old_data_dir, Some(old_data_dir), "relaxed mode must not change commit semantics");

        // Compare against root-resolved paths: the recorder stores the paths
        // the disk actually fsyncs, which go through the canonicalized root.
        let resolved_tmp_data_dir = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{new_data_dir}"))
            .expect("tmp data dir path should resolve");
        let resolved_dst_object_dir = disk.get_object_path(bucket, object).expect("dst object dir should resolve");

        // Payload durability is kept: sync_dir_files fdatasyncs the shard
        // files and fsyncs the staged data dir before the commit rename.
        assert!(
            os::fsync_dir_recorder::was_fsynced(&resolved_tmp_data_dir),
            "relaxed mode must keep the shard-data sync before the commit rename"
        );

        // Metadata-commit fsyncs are skipped: neither the destination parent
        // dir nor the rollback backup parent dir is fsynced.
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&resolved_dst_object_dir),
            "relaxed mode must skip the commit-rename dir fsync"
        );
        let resolved_backup_parent = resolved_dst_object_dir.join(old_data_dir.to_string());
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&resolved_backup_parent),
            "relaxed mode must skip the rollback-backup dir fsync"
        );
        assert!(
            dst_object_dir
                .join(old_data_dir.to_string())
                .join(STORAGE_FORMAT_FILE_BACKUP)
                .exists(),
            "the rollback backup itself must still be written"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_rename_data_legacy_off_skips_all_fsyncs() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::LegacyOff);

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "legacy-off-bucket";
        let object = "dir/object";
        let tmp_object = "tmp-legacy-write";
        let version_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("version id should parse");
        let new_data_dir = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("new data dir should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new-data")
            .await
            .expect("new tmp data should be written");

        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("rename_data should commit");

        // Historical RUSTFS_DRIVE_SYNC_ENABLE=false semantics: no fsync at
        // all, not even the shard-data sync relaxed keeps. Assertions use the
        // root-resolved paths the disk actually passes to fsync.
        let resolved_tmp_data_dir = disk
            .get_object_path(RUSTFS_META_TMP_BUCKET, &format!("{tmp_object}/{new_data_dir}"))
            .expect("tmp data dir path should resolve");
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&resolved_tmp_data_dir),
            "legacy-off must not sync staged shard data"
        );
        let resolved_dst_object_dir = disk.get_object_path(bucket, object).expect("dst object dir should resolve");
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&resolved_dst_object_dir),
            "legacy-off must not fsync the commit-rename dir"
        );

        // And system-critical volumes are NOT pinned: the old full-off
        // behavior is preserved bit for bit for existing deployments.
        disk.write_all(RUSTFS_META_BUCKET, "buckets/legacy/bucket-metadata", Bytes::from_static(b"payload"))
            .await
            .expect("write_all should succeed");
        let meta_parent = disk
            .get_object_path(RUSTFS_META_BUCKET, "buckets/legacy/bucket-metadata")
            .expect("meta path should resolve")
            .parent()
            .expect("meta file should have a parent")
            .to_path_buf();
        assert!(
            !os::fsync_dir_recorder::was_fsynced(&meta_parent),
            "legacy-off keeps the historical semantics: system metadata is not synced either"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn test_rename_data_writes_old_metadata_backup_for_inline_overwrite() {
        use std::sync::mpsc;
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));

        let bucket = "bucket";
        let object = "inline-object";
        let tmp_object = "tmp-inline-write";
        let version_id = Uuid::parse_str("12121212-1212-1212-1212-121212121212").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("34343434-3434-3434-3434-343434343434").expect("old data dir should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let old_meta = test_meta(old_fi);
        let dst_object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(dst_object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), &old_meta)
            .await
            .expect("old metadata should be written");

        let tmp_object_dir = dir.path().join(RUSTFS_META_TMP_BUCKET).join(tmp_object);
        fs::create_dir_all(&tmp_object_dir)
            .await
            .expect("tmp object dir should be created");

        let (published_tx, published_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        set_rename_data_after_first_publication(&disk.root, bucket, object, move || {
            published_tx.send(()).expect("signal backup publication");
            release_rx.recv().expect("wait for lock-order assertion");
        });
        let new_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-new")));
        let rename_disk = disk.clone();
        let rename = tokio::spawn(async move {
            rename_disk
                .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
                .await
        });
        tokio::task::spawn_blocking(move || published_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("publication waiter should run")
            .expect("rollback backup must be published");
        assert_eq!(
            disk.file_sync_permits.available_permits(),
            os::MAX_PARALLEL_FILE_SYNCS,
            "backup publication must not acquire namespace while holding disk admission"
        );
        release_tx.send(()).expect("release backup publication");
        let resp = rename
            .await
            .expect("inline rename task should join")
            .expect("inline rename_data should commit");

        assert_eq!(resp.old_data_dir, Some(old_data_dir));
        assert_eq!(resp.sign, Some(version_id.as_bytes().to_vec()));
        let backup_path = dst_object_dir.join(old_data_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
        assert!(backup_path.exists());
        assert!(
            os::fsync_dir_recorder::was_fsynced(backup_path.parent().expect("backup must have a parent")),
            "strict inline overwrite must persist the rollback backup directory entry"
        );
        assert_eq!(
            os::fsync_dir_recorder::was_limited(backup_path.parent().expect("backup must have a parent")),
            cfg!(unix),
            "only Unix rollback backup fsyncs should use the disk file-sync limit"
        );
        assert!(
            os::fsync_dir_recorder::was_fsynced(&dst_object_dir),
            "strict inline overwrite must persist the committed xl.meta directory entry"
        );
        assert_eq!(
            os::fsync_dir_recorder::was_limited(&dst_object_dir),
            cfg!(unix),
            "only Unix inline commit fsyncs should use the disk file-sync limit"
        );
        // The rollback backup must contain the previous metadata bytes verbatim so
        // that undo_write can restore the prior committed object; guards the inline
        // backup write against truncation/corruption regressions.
        assert_eq!(
            fs::read(&backup_path).await.expect("backup should be readable"),
            old_meta,
            "inline rollback backup must contain the previous metadata bytes verbatim"
        );
        assert!(
            !dst_object_dir
                .join(old_data_dir.to_string())
                .join(STORAGE_FORMAT_FILE)
                .exists()
        );
        // rustfs/backlog#1009: the overwritten live current version (size 1
        // from `test_file_info`) must be surfaced through the backfill.
        assert_eq!(resp.old_current_size, Some(OldCurrentSize::Present(1)));
    }

    /// rustfs/backlog#1009: `observe_old_current_size` must mirror the pre-PUT
    /// `get_object_info` semantics bit for bit — latest version's
    /// `ObjectInfo.size` (0 for a delete-marker latest, which that lookup
    /// returns as `Ok`, not as not-found), missing key → `Absent` — and
    /// `rename_data` must report it for both the inline and non-inline commit
    /// branches.
    mod old_current_size_backfill {
        use super::*;
        use tempfile::tempdir;

        fn live_file_info(name: &str, version_id: Uuid, size: i64, mod_time: OffsetDateTime) -> FileInfo {
            FileInfo {
                name: name.to_string(),
                version_id: Some(version_id),
                size,
                mod_time: Some(mod_time),
                ..Default::default()
            }
        }

        fn delete_marker_file_info(name: &str, version_id: Uuid, mod_time: OffsetDateTime) -> FileInfo {
            FileInfo {
                name: name.to_string(),
                version_id: Some(version_id),
                deleted: true,
                mod_time: Some(mod_time),
                ..Default::default()
            }
        }

        #[test]
        fn observe_reports_absent_for_missing_key() {
            assert_eq!(observe_old_current_size(false, &FileMeta::default()), Some(OldCurrentSize::Absent));
        }

        /// An existing xl.meta with zero versions reads back through
        /// `get_file_info` as a synthetic deleted FileInfo of size 0, so the
        /// pre-PUT lookup reported `Some(0)`, not not-found.
        #[test]
        fn observe_reports_present_zero_for_existing_versionless_meta() {
            assert_eq!(observe_old_current_size(true, &FileMeta::default()), Some(OldCurrentSize::Present(0)));
        }

        #[test]
        fn observe_reports_latest_live_version_size() {
            let now = OffsetDateTime::now_utc();
            let mut meta = FileMeta::new();
            meta.add_version(live_file_info("object", Uuid::new_v4(), 10, now - time::Duration::seconds(10)))
                .expect("older live version should be added");
            meta.add_version(live_file_info("object", Uuid::new_v4(), 42, now))
                .expect("newer live version should be added");

            assert_eq!(observe_old_current_size(true, &meta), Some(OldCurrentSize::Present(42)));
        }

        /// The pre-PUT lookup returns `Ok(size 0)` for a delete-marker latest
        /// (RustFS's `SetDisks::get_object_info` does not convert markers to
        /// not-found), and delete-marker creation never decrements
        /// objects_count — so the backfill must report `Present(0)` here, not
        /// `Absent`, to keep versioned accounting bit-identical.
        #[test]
        fn observe_reports_present_zero_for_delete_marker_latest() {
            let now = OffsetDateTime::now_utc();
            let mut meta = FileMeta::new();
            meta.add_version(live_file_info("object", Uuid::new_v4(), 42, now - time::Duration::seconds(10)))
                .expect("live version should be added");
            meta.add_version(delete_marker_file_info("object", Uuid::new_v4(), now))
                .expect("delete marker should be added");

            assert_eq!(observe_old_current_size(true, &meta), Some(OldCurrentSize::Present(0)));
        }

        /// A latest version whose part arrays are corrupt (lengths disagree)
        /// made the old per-disk lookup error out — this disk must abstain
        /// (`None`), never vote. Pins the `all_parts=true` flag that enables
        /// the part-array length guard.
        #[test]
        fn observe_abstains_for_corrupt_part_arrays() {
            let now = OffsetDateTime::now_utc();
            let mut fi = live_file_info("object", Uuid::new_v4(), 42, now);
            fi.add_object_part(1, "etag".to_string(), 42, Some(now), 42, None, None);
            let mut version = rustfs_filemeta::FileMetaVersion::from(fi);
            version
                .object
                .as_mut()
                .expect("object version should carry a MetaObject")
                .part_sizes
                .clear();

            let mut meta = FileMeta::new();
            meta.add_version_filemata(version)
                .expect("corrupt-part version should still insert");

            assert_eq!(observe_old_current_size(true, &meta), None);
        }

        async fn test_disk(dir: &tempfile::TempDir) -> LocalDisk {
            let endpoint =
                Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
            let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
            ensure_test_volume(&disk, "bucket").await;
            ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
            disk
        }

        #[tokio::test]
        async fn inline_rename_data_reports_absent_for_fresh_key() {
            let dir = tempdir().expect("temp dir should be created");
            let disk = test_disk(&dir).await;

            let new_fi = test_file_info("object", Uuid::new_v4(), None, Some(Bytes::from_static(b"inline-new")));
            let resp = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, "tmp-fresh", new_fi, "bucket", "object")
                .await
                .expect("inline rename_data should commit");

            assert_eq!(resp.old_current_size, Some(OldCurrentSize::Absent));
        }

        #[tokio::test]
        async fn inline_rename_data_reports_present_zero_for_delete_marker_latest() {
            let dir = tempdir().expect("temp dir should be created");
            let disk = test_disk(&dir).await;

            let now = OffsetDateTime::now_utc();
            let mut old_meta = FileMeta::new();
            old_meta
                .add_version(live_file_info("object", Uuid::new_v4(), 42, now - time::Duration::seconds(10)))
                .expect("live version should be added");
            old_meta
                .add_version(delete_marker_file_info("object", Uuid::new_v4(), now))
                .expect("delete marker should be added");
            let dst_object_dir = dir.path().join("bucket").join("object");
            fs::create_dir_all(&dst_object_dir).await.expect("dst dir should be created");
            fs::write(
                dst_object_dir.join(STORAGE_FORMAT_FILE),
                old_meta.marshal_msg().expect("old metadata should encode"),
            )
            .await
            .expect("old metadata should be written");

            let new_fi = test_file_info("object", Uuid::new_v4(), None, Some(Bytes::from_static(b"inline-new")));
            let resp = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, "tmp-marker", new_fi, "bucket", "object")
                .await
                .expect("inline rename_data should commit");

            // The pre-PUT lookup returns Ok(size 0) for a marker latest, so
            // the backfill must match it (see observe_old_current_size docs).
            assert_eq!(resp.old_current_size, Some(OldCurrentSize::Present(0)));
        }

        #[tokio::test]
        async fn inline_rename_data_reports_unknown_for_unparsable_dst_meta() {
            let dir = tempdir().expect("temp dir should be created");
            let disk = test_disk(&dir).await;

            let dst_object_dir = dir.path().join("bucket").join("object");
            fs::create_dir_all(&dst_object_dir).await.expect("dst dir should be created");
            fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), b"not-an-xl-meta")
                .await
                .expect("garbage metadata should be written");

            let new_fi = test_file_info("object", Uuid::new_v4(), None, Some(Bytes::from_static(b"inline-new")));
            let resp = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, "tmp-garbage", new_fi, "bucket", "object")
                .await
                .expect("inline rename_data should commit");

            assert_eq!(resp.old_current_size, None);
        }

        #[tokio::test]
        async fn non_inline_rename_data_reports_absent_then_previous_size() {
            let dir = tempdir().expect("temp dir should be created");
            let disk = test_disk(&dir).await;

            // First non-inline commit: fresh key must report Absent.
            let first_data_dir = Uuid::new_v4();
            let tmp_data_dir = dir
                .path()
                .join(RUSTFS_META_TMP_BUCKET)
                .join("tmp-first")
                .join(first_data_dir.to_string());
            fs::create_dir_all(&tmp_data_dir)
                .await
                .expect("tmp data dir should be created");
            fs::write(tmp_data_dir.join("part.1"), b"first")
                .await
                .expect("part should be written");
            let mut first_fi = test_file_info("object", Uuid::new_v4(), Some(first_data_dir), None);
            first_fi.size = 5;
            let resp = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, "tmp-first", first_fi, "bucket", "object")
                .await
                .expect("first non-inline rename_data should commit");
            assert_eq!(resp.old_current_size, Some(OldCurrentSize::Absent));

            // Overwrite: the committed live version (size 5) must be reported.
            let second_data_dir = Uuid::new_v4();
            let tmp_data_dir = dir
                .path()
                .join(RUSTFS_META_TMP_BUCKET)
                .join("tmp-second")
                .join(second_data_dir.to_string());
            fs::create_dir_all(&tmp_data_dir)
                .await
                .expect("tmp data dir should be created");
            fs::write(tmp_data_dir.join("part.1"), b"second-longer")
                .await
                .expect("part should be written");
            let mut second_fi = test_file_info("object", Uuid::new_v4(), Some(second_data_dir), None);
            second_fi.size = 13;
            let resp = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, "tmp-second", second_fi, "bucket", "object")
                .await
                .expect("second non-inline rename_data should commit");
            assert_eq!(resp.old_current_size, Some(OldCurrentSize::Present(5)));
        }

        /// Twin of the inline unparsable-dst test: the `dst_meta_unparsable`
        /// tracking is duplicated per branch, so the non-inline copy needs its
        /// own regression coverage.
        #[tokio::test]
        async fn non_inline_rename_data_reports_unknown_for_unparsable_dst_meta() {
            let dir = tempdir().expect("temp dir should be created");
            let disk = test_disk(&dir).await;

            let dst_object_dir = dir.path().join("bucket").join("object");
            fs::create_dir_all(&dst_object_dir).await.expect("dst dir should be created");
            fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), b"not-an-xl-meta")
                .await
                .expect("garbage metadata should be written");

            let data_dir = Uuid::new_v4();
            let tmp_data_dir = dir
                .path()
                .join(RUSTFS_META_TMP_BUCKET)
                .join("tmp-garbage-noninline")
                .join(data_dir.to_string());
            fs::create_dir_all(&tmp_data_dir)
                .await
                .expect("tmp data dir should be created");
            fs::write(tmp_data_dir.join("part.1"), b"payload")
                .await
                .expect("part should be written");
            let mut new_fi = test_file_info("object", Uuid::new_v4(), Some(data_dir), None);
            new_fi.size = 7;
            let resp = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, "tmp-garbage-noninline", new_fi, "bucket", "object")
                .await
                .expect("non-inline rename_data should commit over garbage metadata");

            assert_eq!(resp.old_current_size, None);
        }
    }

    #[tokio::test]
    async fn windows_and_unix_rename_data_inline_post_commit_error_restores_old_metadata() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "inline-post-commit-object";
        let tmp_object = "tmp-inline-post-commit-write";
        let version_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").expect("version id should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-old")));
        let old_meta = test_meta(old_fi);
        let dst_object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&dst_object_dir)
            .await
            .expect("object dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        let tmp_object_dir = dir.path().join(RUSTFS_META_TMP_BUCKET).join(tmp_object);
        fs::create_dir_all(&tmp_object_dir)
            .await
            .expect("tmp object dir should be created");

        set_rename_data_fail_after_metadata_commit(object);
        let new_fi = test_file_info(object, version_id, None, Some(Bytes::from_static(b"inline-new")));
        let err = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect_err("post-commit failure must be returned");

        assert!(matches!(err, DiskError::Io(ref io_err) if io_err.kind() == ErrorKind::Other));
        let restored_meta = fs::read(dst_object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("old metadata should still be readable");
        assert_eq!(restored_meta, old_meta);
    }

    #[tokio::test]
    async fn windows_and_unix_rename_data_inline_post_commit_error_removes_fresh_metadata() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "fresh-inline-post-commit-bucket";
        let object = "fresh-inline-post-commit-object";
        let tmp_object = "tmp-fresh-inline-post-commit";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        set_rename_data_fail_after_metadata_commit(object);
        let fi = test_file_info(object, Uuid::new_v4(), None, Some(Bytes::from_static(b"inline")));
        let err = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, fi, bucket, object)
            .await
            .expect_err("post-commit failure must reject a fresh inline object");

        assert!(matches!(err, DiskError::Io(ref io_err) if io_err.kind() == ErrorKind::Other));
        assert!(
            !dir.path().join(bucket).join(object).join(STORAGE_FORMAT_FILE).exists(),
            "failed fresh inline commit must remove published metadata"
        );
    }

    #[tokio::test]
    async fn windows_and_unix_rename_data_non_inline_post_commit_error_removes_fresh_object() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "fresh-non-inline-post-commit-bucket";
        let object = "fresh-non-inline-post-commit-object";
        let tmp_object = "tmp-fresh-non-inline-post-commit";
        let data_dir = Uuid::new_v4();
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;
        let staged_part = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(data_dir.to_string())
            .join("part.1");
        fs::create_dir_all(staged_part.parent().expect("staged part should have a parent"))
            .await
            .expect("staged data directory should be created");
        fs::write(&staged_part, b"payload")
            .await
            .expect("staged part should be written");

        set_rename_data_fail_after_metadata_commit(object);
        let fi = test_file_info(object, Uuid::new_v4(), Some(data_dir), None);
        let err = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, fi, bucket, object)
            .await
            .expect_err("post-commit failure must reject a fresh non-inline object");

        assert!(matches!(err, DiskError::Unexpected));
        let destination = dir.path().join(bucket).join(object);
        assert!(
            !destination.join(STORAGE_FORMAT_FILE).exists(),
            "failed fresh non-inline commit must remove published metadata"
        );
        assert!(
            !destination.join(data_dir.to_string()).exists(),
            "failed fresh non-inline commit must remove published data"
        );
    }

    #[tokio::test]
    async fn rename_delete_marker_post_commit_error_restores_other_version_metadata() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "delete-marker-post-commit-object";
        let old_version_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").expect("version id should parse");
        let marker_version_id = Uuid::parse_str("88888888-8888-8888-8888-888888888888").expect("version id should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_meta = test_meta(test_file_info(object, old_version_id, None, Some(Bytes::from_static(b"inline-old"))));
        let dst_object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&dst_object_dir)
            .await
            .expect("object dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        let marker = FileInfo {
            volume: bucket.to_string(),
            name: object.to_string(),
            version_id: Some(marker_version_id),
            deleted: true,
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        let xl_path = dst_object_dir.join(STORAGE_FORMAT_FILE);
        set_local_inline_rollback_hardlink_failure(&xl_path);
        set_rename_data_fail_after_metadata_commit(object);
        let result = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, "tmp-delete-marker", marker, bucket, object)
            .await;

        assert!(result.is_err());
        let restored_meta = fs::read(dst_object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("old metadata should still be readable");
        assert_eq!(restored_meta, old_meta);
        let mut entries = fs::read_dir(&dst_object_dir)
            .await
            .expect("object directory should remain readable");
        while let Some(entry) = entries.next_entry().await.expect("object directory entry should be readable") {
            assert!(!entry.path().is_dir(), "local rollback directory should be removed");
        }
        assert!(
            !dir.path()
                .join(RUSTFS_META_TMP_BUCKET)
                .join("tmp-delete-marker")
                .join(STORAGE_FORMAT_FILE_BACKUP)
                .exists(),
            "copy fallback backup should be consumed by atomic rollback"
        );
    }

    // The undo_write restore consumes `<rollback>/xl.meta.bkp` by rename; a
    // synthetic rollback dir is then empty and must be reclaimed so the object
    // dir can empty out (BucketNotEmpty leak). A real data dir still holds its
    // parts and must survive the non-recursive remove.
    #[tokio::test]
    async fn restore_metadata_backup_reclaims_empty_rollback_dir_only() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let publication_root = os::PublicationRoot::new(dir.path()).expect("publication root should open");
        let object_dir = dir.path().join("bucket").join("obj");
        let xl_path = object_dir.join(STORAGE_FORMAT_FILE);
        let rollback_dir = Uuid::new_v4();
        let rollback_path = object_dir.join(rollback_dir.to_string());
        fs::create_dir_all(&rollback_path)
            .await
            .expect("rollback dir should be created");
        fs::write(rollback_path.join(STORAGE_FORMAT_FILE_BACKUP), b"old-meta")
            .await
            .expect("backup should be written");

        restore_metadata_backup(&object_dir, &xl_path, rollback_dir, &publication_root)
            .await
            .expect("restore should succeed");
        assert_eq!(
            fs::read(&xl_path).await.expect("xl.meta should be restored"),
            b"old-meta",
            "restore must move the backup back onto xl.meta"
        );
        assert!(!rollback_path.exists(), "an emptied synthetic rollback dir must be reclaimed");

        // Real data dir: parts remain, the dir must survive.
        let real_dir = Uuid::new_v4();
        let real_path = object_dir.join(real_dir.to_string());
        fs::create_dir_all(&real_path).await.expect("real data dir should be created");
        fs::write(real_path.join(STORAGE_FORMAT_FILE_BACKUP), b"older-meta")
            .await
            .expect("backup should be written");
        fs::write(real_path.join("part.1"), b"data")
            .await
            .expect("part should be written");

        restore_metadata_backup(&object_dir, &xl_path, real_dir, &publication_root)
            .await
            .expect("restore should succeed");
        assert!(real_path.join("part.1").exists(), "a real data dir must keep its parts");
        assert!(real_path.exists(), "a non-empty data dir must not be removed");
    }

    #[tokio::test]
    async fn rename_commit_failure_cleans_local_rollback_backup() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "bucket";
        let object = "commit-rename-failure-object";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_meta = test_meta(test_file_info(object, Uuid::new_v4(), None, Some(Bytes::from_static(b"old"))));
        let dst_object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&dst_object_dir)
            .await
            .expect("object dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        set_rename_data_fail_commit_rename(object);
        let result = disk
            .rename_data(
                RUSTFS_META_TMP_BUCKET,
                "tmp-commit-failure",
                FileInfo {
                    volume: bucket.to_string(),
                    name: object.to_string(),
                    version_id: Some(Uuid::new_v4()),
                    deleted: true,
                    mod_time: Some(OffsetDateTime::now_utc()),
                    ..Default::default()
                },
                bucket,
                object,
            )
            .await;

        assert!(result.is_err());
        assert_eq!(
            fs::read(dst_object_dir.join(STORAGE_FORMAT_FILE))
                .await
                .expect("old metadata should remain readable"),
            old_meta
        );
        let mut entries = fs::read_dir(&dst_object_dir)
            .await
            .expect("object directory should remain readable");
        while let Some(entry) = entries.next_entry().await.expect("object directory entry should be readable") {
            assert!(!entry.path().is_dir(), "failed commit must clean local rollback directory");
        }
    }

    #[tokio::test]
    async fn windows_and_unix_inline_rename_missing_staged_metadata_fails_without_replacing_destination() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "bucket";
        let object = "missing-staged-inline-object";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_meta = test_meta(test_file_info(object, Uuid::new_v4(), None, Some(Bytes::from_static(b"old"))));
        let dst_object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&dst_object_dir)
            .await
            .expect("object dir should be created");
        fs::write(dst_object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        set_rename_data_remove_staged_meta_before_commit(object);
        let err = disk
            .rename_data(
                RUSTFS_META_TMP_BUCKET,
                "tmp-missing-staged-inline",
                test_file_info(object, Uuid::new_v4(), None, Some(Bytes::from_static(b"new"))),
                bucket,
                object,
            )
            .await
            .expect_err("a missing staged xl.meta must fail publication");

        assert_eq!(err, DiskError::FileNotFound);
        assert_eq!(
            fs::read(dst_object_dir.join(STORAGE_FORMAT_FILE))
                .await
                .expect("old metadata should remain readable"),
            old_meta
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn windows_and_unix_cancelled_inline_preparation_serializes_newer_commit() {
        use std::sync::mpsc;
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let bucket = "bucket";
        let object = "cancelled-inline-preparation";
        let version_id = Uuid::parse_str("7c5d2fa4-84aa-47aa-8a8d-a8d121ef3579").expect("version id should parse");
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&object_dir).await.expect("object dir should be created");
        let initial_meta = test_meta(test_file_info(object, version_id, None, Some(Bytes::from_static(b"v0"))));
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), &initial_meta)
            .await
            .expect("initial metadata should be written");

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        set_inline_preparation_before_backup(object, move || {
            entered_tx.send(()).expect("signal blocked preparation");
            release_rx.recv().expect("wait for newer commits");
        });
        let cancelled_disk = Arc::clone(&disk);
        let cancelled = tokio::spawn(async move {
            cancelled_disk
                .rename_data(
                    RUSTFS_META_TMP_BUCKET,
                    "cancelled-inline-stage",
                    test_file_info(object, version_id, None, Some(Bytes::from_static(b"cancelled"))),
                    bucket,
                    object,
                )
                .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("preparation waiter should run")
            .expect("preparation must reach the backup hook");
        assert_eq!(
            disk.file_sync_permits.available_permits(),
            os::MAX_PARALLEL_FILE_SYNCS - 1,
            "strict inline preparation must hold one disk file-sync permit"
        );
        cancelled.abort();
        assert!(cancelled.await.expect_err("operation should be cancelled").is_cancelled());

        let newer_disk = Arc::clone(&disk);
        let mut newer = tokio::spawn(async move {
            newer_disk
                .rename_data(
                    RUSTFS_META_TMP_BUCKET,
                    "newer-inline-stage",
                    test_file_info(object, version_id, None, Some(Bytes::from_static(b"v1"))),
                    bucket,
                    object,
                )
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut newer).await.is_err(),
            "a newer commit must wait while cancelled preparation owns the object namespace"
        );
        release_tx.send(()).expect("release cancelled preparation");
        tokio::time::timeout(Duration::from_secs(10), newer)
            .await
            .expect("newer commit should finish after cancelled preparation releases the namespace")
            .expect("newer commit task should not panic")
            .expect("newer inline metadata should commit");

        let current_v1 = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("newer metadata should be readable");
        let current_meta = FileMeta::load(&current_v1).expect("newer metadata should parse");
        let rollback_dir = inline_metadata_rollback_dir(version_id, &current_meta);
        let shared_backup = object_dir.join(rollback_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);

        set_rename_data_fail_commit_rename(object);
        disk.rename_data(
            RUSTFS_META_TMP_BUCKET,
            "latest-inline-stage",
            test_file_info(object, version_id, None, Some(Bytes::from_static(b"v2"))),
            bucket,
            object,
        )
        .await
        .expect_err("the latest commit should stop after publishing its rollback backup");
        assert_eq!(fs::read(&shared_backup).await.expect("latest rollback backup should exist"), current_v1);

        let cancelled_backup = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join("cancelled-inline-stage")
            .join(STORAGE_FORMAT_FILE_BACKUP);
        assert_eq!(
            fs::read(&cancelled_backup)
                .await
                .expect("cancelled preparation should finish its private backup"),
            initial_meta,
            "the cancelled preparation must retain the metadata snapshot it observed"
        );

        assert_eq!(
            fs::read(&shared_backup)
                .await
                .expect("shared rollback backup should remain readable"),
            current_v1,
            "cancelled preparation must not overwrite the newer shared rollback backup"
        );
        assert_eq!(
            fs::read(object_dir.join(STORAGE_FORMAT_FILE))
                .await
                .expect("current metadata should remain readable"),
            current_v1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn relaxed_inline_preparation_does_not_use_file_sync_limit() {
        use std::sync::mpsc;
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Relaxed);
        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let bucket = "relaxed-inline-preparation";
        let object = "inline-object";
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        set_inline_preparation_before_backup(object, move || {
            entered_tx.send(()).expect("signal blocked preparation");
            release_rx.recv().expect("wait for permit assertion");
        });
        let rename_disk = Arc::clone(&disk);
        let rename = tokio::spawn(async move {
            rename_disk
                .rename_data(
                    RUSTFS_META_TMP_BUCKET,
                    "relaxed-inline-stage",
                    test_file_info(object, Uuid::new_v4(), None, Some(Bytes::from_static(b"payload"))),
                    bucket,
                    object,
                )
                .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("preparation waiter should run")
            .expect("preparation must reach the hook");
        assert_eq!(
            disk.file_sync_permits.available_permits(),
            os::MAX_PARALLEL_FILE_SYNCS,
            "relaxed inline preparation must not consume strict sync capacity"
        );

        release_tx.send(()).expect("release inline preparation");
        rename
            .await
            .expect("rename task should join")
            .expect("relaxed inline rename should commit");
    }

    #[tokio::test]
    async fn rename_purge_pending_payload_stays_object_and_cleans_local_backup() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "purge-pending-rename-object";
        let old_version_id = Uuid::new_v4();
        let purge_version_id = Uuid::new_v4();
        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let dst_object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&dst_object_dir)
            .await
            .expect("object dir should be created");
        fs::write(
            dst_object_dir.join(STORAGE_FORMAT_FILE),
            test_meta(test_file_info(object, old_version_id, None, Some(Bytes::from_static(b"old")))),
        )
        .await
        .expect("old metadata should be written");

        let mut purge_pending = test_file_info(object, purge_version_id, None, Some(Bytes::from_static(b"purge-pending")));
        rustfs_utils::http::insert_str(
            &mut purge_pending.metadata,
            rustfs_utils::http::SUFFIX_PURGESTATUS,
            "target=PENDING;".to_string(),
        );
        purge_pending.deleted = true;
        disk.rename_data(RUSTFS_META_TMP_BUCKET, "tmp-purge-pending", purge_pending, bucket, object)
            .await
            .expect("purge-pending erasure payload should commit");

        let stored = disk
            .read_version(
                "",
                bucket,
                object,
                &purge_version_id.to_string(),
                &ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("purge-pending payload should remain readable");
        assert!(stored.deleted);
        assert!(!stored.is_canonical_delete_marker());
        assert_eq!(stored.erasure.data_blocks, 1);
        assert_eq!(stored.size, 13);

        let mut entries = fs::read_dir(&dst_object_dir)
            .await
            .expect("object directory should remain readable");
        while let Some(entry) = entries.next_entry().await.expect("object directory entry should be readable") {
            assert!(!entry.path().is_dir(), "successful local rollback directory should be removed");
        }
    }

    #[tokio::test]
    async fn test_delete_version_undo_restores_backup_to_object_root() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let version_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("55555555-5555-5555-5555-555555555555").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("66666666-6666-6666-6666-666666666666").expect("new data dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join("dir/object");
        fs::create_dir_all(object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old backup dir should be created");
        fs::create_dir_all(object_dir.join(new_data_dir.to_string()))
            .await
            .expect("new data dir should be created");

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let old_meta = test_meta(old_fi);
        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        fs::write(
            object_dir.join(old_data_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP),
            old_meta.clone(),
        )
        .await
        .expect("old metadata backup should be written");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), test_meta(new_fi.clone()))
            .await
            .expect("new metadata should be written");

        disk.delete_version(
            bucket,
            object,
            new_fi,
            false,
            DeleteOptions {
                undo_write: true,
                old_data_dir: Some(old_data_dir),
                ..Default::default()
            },
        )
        .await
        .expect("undo should restore old metadata");

        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("restored metadata should be readable");
        assert_eq!(restored_meta, old_meta);
        assert!(!object_dir.join("dir/object").join(STORAGE_FORMAT_FILE).exists());
    }

    #[tokio::test]
    async fn test_delete_version_undo_restores_backup_when_other_versions_remain() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let version_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").expect("version id should parse");
        let other_version_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("55555555-5555-5555-5555-555555555555").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("66666666-6666-6666-6666-666666666666").expect("new data dir should parse");
        let other_data_dir = Uuid::parse_str("88888888-8888-8888-8888-888888888888").expect("other data dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join("dir/object");
        fs::create_dir_all(object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old backup dir should be created");
        fs::create_dir_all(object_dir.join(new_data_dir.to_string()))
            .await
            .expect("new data dir should be created");

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let other_fi = test_file_info(object, other_version_id, Some(other_data_dir), None);
        let mut old_meta = FileMeta::default();
        old_meta
            .add_version(old_fi)
            .expect("old metadata should accept old file info");
        old_meta
            .add_version(other_fi.clone())
            .expect("old metadata should accept other file info");
        let old_meta = old_meta.marshal_msg().expect("old metadata should encode");

        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        let mut new_meta = FileMeta::default();
        new_meta
            .add_version(new_fi.clone())
            .expect("new metadata should accept new file info");
        new_meta
            .add_version(other_fi)
            .expect("new metadata should accept other file info");

        fs::write(
            object_dir.join(old_data_dir.to_string()).join(STORAGE_FORMAT_FILE_BACKUP),
            old_meta.clone(),
        )
        .await
        .expect("old metadata backup should be written");
        fs::write(
            object_dir.join(STORAGE_FORMAT_FILE),
            new_meta.marshal_msg().expect("new metadata should encode"),
        )
        .await
        .expect("new metadata should be written");

        disk.delete_version(
            bucket,
            object,
            new_fi,
            false,
            DeleteOptions {
                undo_write: true,
                old_data_dir: Some(old_data_dir),
                ..Default::default()
            },
        )
        .await
        .expect("undo should restore old metadata");

        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("restored metadata should be readable");
        assert_eq!(restored_meta, old_meta);
    }

    #[tokio::test]
    async fn test_delete_versions_ignores_missing_non_deleted_version_and_deletes_existing() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let existing_version = Uuid::parse_str("10101010-1010-1010-1010-101010101010").expect("version id should parse");
        let missing_version = Uuid::parse_str("20202020-2020-2020-2020-202020202020").expect("version id should parse");
        let existing_data_dir = Uuid::parse_str("30303030-3030-3030-3030-303030303030").expect("data dir should parse");
        let missing_data_dir = Uuid::parse_str("40404040-4040-4040-4040-404040404040").expect("data dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(object_dir.join(existing_data_dir.to_string()))
            .await
            .expect("existing data dir should be created");

        let existing_fi = test_file_info(object, existing_version, Some(existing_data_dir), None);
        let missing_fi = test_file_info(object, missing_version, Some(missing_data_dir), None);
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), test_meta(existing_fi.clone()))
            .await
            .expect("existing metadata should be written");

        disk.delete_versions_internal(bucket, object, &[missing_fi, existing_fi], &DeleteOptions::default())
            .await
            .expect("missing non-deleted version should not abort deletion");

        assert!(
            matches!(
                disk.read_all(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}")).await,
                Err(DiskError::FileNotFound)
            ),
            "metadata should be removed once the remaining real version is deleted"
        );
        assert!(
            !object_dir.join(existing_data_dir.to_string()).exists(),
            "deleted version data directory should leave the object path"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_delete_version_missing_inline_data_dir_does_not_warn() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/inline-object";
        let version_id = Uuid::parse_str("31313131-1111-2222-3333-444444444444").expect("version id should parse");
        let data_dir = Uuid::parse_str("32323232-1111-2222-3333-444444444444").expect("data dir should parse");
        let rollback_dir = Uuid::parse_str("33333333-1111-2222-3333-444444444444").expect("rollback dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&object_dir).await.expect("object dir should be created");
        let fi = test_file_info(object, version_id, Some(data_dir), Some(Bytes::from_static(b"inline")));
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), test_meta(fi.clone()))
            .await
            .expect("inline metadata should be written");
        assert!(!object_dir.join(data_dir.to_string()).exists());

        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        disk.delete_version(
            bucket,
            object,
            fi,
            false,
            DeleteOptions {
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("missing inline data dir should not fail deletion");

        assert!(!object_dir.join(STORAGE_FORMAT_FILE).exists());
        assert!(
            object_dir
                .join(rollback_dir.to_string())
                .join(STORAGE_FORMAT_FILE_BACKUP)
                .exists()
        );
        assert!(!logs.contents().contains("reliable_rename failed"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_delete_versions_missing_inline_data_dir_does_not_warn() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/inline-object-batch";
        let version_id = Uuid::parse_str("34343434-1111-2222-3333-444444444444").expect("version id should parse");
        let data_dir = Uuid::parse_str("35353535-1111-2222-3333-444444444444").expect("data dir should parse");
        let rollback_dir = Uuid::parse_str("36363636-1111-2222-3333-444444444444").expect("rollback dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(&object_dir).await.expect("object dir should be created");
        let fi = test_file_info(object, version_id, Some(data_dir), Some(Bytes::from_static(b"inline")));
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), test_meta(fi.clone()))
            .await
            .expect("inline metadata should be written");
        assert!(!object_dir.join(data_dir.to_string()).exists());

        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        disk.delete_versions_internal(
            bucket,
            object,
            &[fi],
            &DeleteOptions {
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("missing inline data dir should not fail batch deletion");

        assert!(!object_dir.join(STORAGE_FORMAT_FILE).exists());
        assert!(
            object_dir
                .join(rollback_dir.to_string())
                .join(STORAGE_FORMAT_FILE_BACKUP)
                .exists()
        );
        assert!(!logs.contents().contains("reliable_rename failed"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_ignore_missing_source_helper_still_warns_on_real_error() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        let publication_root = os::PublicationRoot::new(dir.path()).expect("publication root should open");
        fs::create_dir_all(&src).await.expect("source dir should be created");
        fs::write(src.join("part.1"), b"live-data")
            .await
            .expect("source data should be written");
        fs::create_dir_all(&dst).await.expect("destination dir should be created");
        fs::write(dst.join("sentinel"), b"conflict")
            .await
            .expect("destination conflict should be written");

        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let err = rename_all_ignore_missing_source(&src, &dst, dir.path(), &publication_root)
            .await
            .expect_err("a non-empty rollback destination must reject rename");

        assert_ne!(err, DiskError::FileNotFound);
        assert!(src.exists());
        assert!(dst.join("sentinel").exists());
        assert!(logs.contents().contains("reliable_rename failed"));
    }

    #[tokio::test]
    async fn test_delete_version_rollback_releases_reserved_data_dir() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let version_id = Uuid::parse_str("99999999-1111-2222-3333-444444444444").expect("version id should parse");
        let data_dir = Uuid::parse_str("88888888-1111-2222-3333-444444444444").expect("data dir should parse");
        let rollback_dir = Uuid::parse_str("77777777-1111-2222-3333-444444444444").expect("rollback dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join("dir/object");
        let data_path = object_dir.join(data_dir.to_string());
        fs::create_dir_all(&data_path).await.expect("data dir should be created");
        fs::write(data_path.join("part.1"), b"old-data")
            .await
            .expect("part data should be written");

        let old_fi = test_file_info(object, version_id, Some(data_dir), None);
        let old_meta = test_meta(old_fi.clone());
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        disk.delete_version(
            bucket,
            object,
            old_fi.clone(),
            false,
            DeleteOptions {
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("delete should stage rollback state");

        assert!(!object_dir.join(STORAGE_FORMAT_FILE).exists());
        assert!(
            data_path.exists(),
            "the delete transaction must reserve the original data dir instead of moving it"
        );
        assert!(
            object_dir
                .join(rollback_dir.to_string())
                .join(STORAGE_FORMAT_FILE_BACKUP)
                .exists()
        );
        assert!(!object_dir.join(rollback_dir.to_string()).join(data_dir.to_string()).exists());

        disk.delete_version(
            bucket,
            object,
            old_fi.clone(),
            false,
            DeleteOptions {
                undo_write: true,
                undo_delete: true,
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("undo should restore metadata and data");

        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("metadata should be restored");
        assert_eq!(restored_meta, old_meta);
        assert_eq!(
            fs::read(data_path.join("part.1"))
                .await
                .expect("part data should be restored"),
            b"old-data"
        );
        assert!(!object_dir.join(rollback_dir.to_string()).exists());

        disk.delete_version(
            bucket,
            object,
            old_fi.clone(),
            false,
            DeleteOptions {
                undo_write: true,
                undo_delete: true,
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("repeated undo should be a no-op after rollback state is consumed");

        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("metadata should remain restored after repeated undo");
        assert_eq!(restored_meta, old_meta);
        assert_eq!(
            fs::read(data_path.join("part.1"))
                .await
                .expect("part data should remain restored after repeated undo"),
            b"old-data"
        );

        fs::create_dir_all(object_dir.join(rollback_dir.to_string()))
            .await
            .expect("stale empty rollback dir should be created");
        disk.delete_version(
            bucket,
            object,
            old_fi,
            false,
            DeleteOptions {
                undo_write: true,
                undo_delete: true,
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("undo with consumed backup and no delete-marker marker should be a no-op");

        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("metadata should remain restored after stale-dir undo");
        assert_eq!(restored_meta, old_meta);
        assert_eq!(
            fs::read(data_path.join("part.1"))
                .await
                .expect("part data should remain restored after stale-dir undo"),
            b"old-data"
        );
        assert!(!object_dir.join(rollback_dir.to_string()).exists());
    }

    #[tokio::test]
    async fn test_delete_version_error_after_staging_restores_data_dir() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/delete-version-error";
        let version_id = Uuid::parse_str("21212121-1111-2222-3333-444444444444").expect("version id should parse");
        let data_dir = Uuid::parse_str("22222222-1111-2222-3333-444444444444").expect("data dir should parse");
        let rollback_dir = Uuid::parse_str("23232323-1111-2222-3333-444444444444").expect("rollback dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join(object);
        let data_path = object_dir.join(data_dir.to_string());
        fs::create_dir_all(&data_path).await.expect("data dir should be created");
        fs::write(data_path.join("part.1"), b"old-data")
            .await
            .expect("part data should be written");

        let old_fi = test_file_info(object, version_id, Some(data_dir), None);
        let old_meta = test_meta(old_fi.clone());
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        set_delete_version_fail_after_data_staged(object);
        let err = disk
            .delete_version(
                bucket,
                object,
                old_fi,
                false,
                DeleteOptions {
                    old_data_dir: Some(rollback_dir),
                    ..Default::default()
                },
            )
            .await
            .expect_err("delete should fail after staging data");
        assert_eq!(err, DiskError::Unexpected);

        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("metadata should be restored");
        assert_eq!(restored_meta, old_meta);
        assert_eq!(
            fs::read(data_path.join("part.1"))
                .await
                .expect("part data should be restored"),
            b"old-data"
        );
        assert!(!object_dir.join(rollback_dir.to_string()).exists());
    }

    #[tokio::test]
    async fn test_delete_versions_error_after_staging_restores_data_dir() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/delete-versions-error";
        let version_id = Uuid::parse_str("24242424-1111-2222-3333-444444444444").expect("version id should parse");
        let data_dir = Uuid::parse_str("25252525-1111-2222-3333-444444444444").expect("data dir should parse");
        let rollback_dir = Uuid::parse_str("26262626-1111-2222-3333-444444444444").expect("rollback dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join(object);
        let data_path = object_dir.join(data_dir.to_string());
        fs::create_dir_all(&data_path).await.expect("data dir should be created");
        fs::write(data_path.join("part.1"), b"old-data")
            .await
            .expect("part data should be written");

        let old_fi = test_file_info(object, version_id, Some(data_dir), None);
        let old_meta = test_meta(old_fi.clone());
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        set_delete_version_fail_after_data_staged(object);
        let errs = disk
            .delete_versions(
                bucket,
                vec![FileInfoVersions {
                    name: object.to_string(),
                    versions: vec![old_fi],
                    ..Default::default()
                }],
                DeleteOptions {
                    old_data_dir: Some(rollback_dir),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(errs, vec![Some(DiskError::Unexpected)]);

        let restored_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("metadata should be restored");
        assert_eq!(restored_meta, old_meta);
        assert_eq!(
            fs::read(data_path.join("part.1"))
                .await
                .expect("part data should be restored"),
            b"old-data"
        );
        assert!(!object_dir.join(rollback_dir.to_string()).exists());
    }

    // backlog#1158 safety premise: the set layer fans the undo out to every online
    // disk on a quorum-failed delete, including disks that never staged a rollback
    // (e.g. a disk that errored before staging). A delete-undo on such a disk must be
    // a safe no-op that leaves the committed object untouched.
    #[tokio::test]
    async fn test_delete_version_undo_is_noop_when_nothing_staged() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "dir/object";
        let version_id = Uuid::parse_str("aaaaaaaa-1111-2222-3333-444444444444").expect("version id should parse");
        let data_dir = Uuid::parse_str("bbbbbbbb-1111-2222-3333-444444444444").expect("data dir should parse");
        let rollback_dir = Uuid::parse_str("cccccccc-1111-2222-3333-444444444444").expect("rollback dir should parse");

        ensure_test_volume(&disk, bucket).await;

        // A committed object exists on this disk, but no rollback state was staged.
        let object_dir = dir.path().join(bucket).join("dir/object");
        let data_path = object_dir.join(data_dir.to_string());
        fs::create_dir_all(&data_path).await.expect("data dir should be created");
        fs::write(data_path.join("part.1"), b"live-data")
            .await
            .expect("part data should be written");
        let fi = test_file_info(object, version_id, Some(data_dir), None);
        let meta = test_meta(fi.clone());
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), meta.clone())
            .await
            .expect("metadata should be written");

        // Undo targeting a rollback dir that was never created must be an Ok no-op.
        disk.delete_version(
            bucket,
            object,
            fi.clone(),
            false,
            DeleteOptions {
                undo_write: true,
                undo_delete: true,
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("undo with no staged rollback state must be a no-op");

        // The committed object is untouched.
        assert_eq!(
            fs::read(object_dir.join(STORAGE_FORMAT_FILE))
                .await
                .expect("metadata should remain"),
            meta
        );
        assert_eq!(fs::read(data_path.join("part.1")).await.expect("data should remain"), b"live-data");
        assert!(!object_dir.join(rollback_dir.to_string()).exists());
    }

    #[tokio::test]
    async fn test_delete_marker_rollback_removes_new_metadata_without_backup() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "missing/object";
        let rollback_dir = Uuid::parse_str("66666666-1111-2222-3333-444444444444").expect("rollback dir should parse");

        ensure_test_volume(&disk, bucket).await;

        let object_dir = dir.path().join(bucket).join("missing/object");
        let delete_marker = FileInfo {
            name: object.to_string(),
            version_id: Some(Uuid::parse_str("55555555-1111-2222-3333-444444444444").expect("version id should parse")),
            deleted: true,
            mark_deleted: true,
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        disk.delete_version(
            bucket,
            object,
            delete_marker.clone(),
            true,
            DeleteOptions {
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("delete marker should be written");
        assert!(object_dir.join(STORAGE_FORMAT_FILE).exists());
        let rollback_path = object_dir.join(rollback_dir.to_string());
        assert!(
            rollback_path.join(DELETE_MARKER_ROLLBACK_FILE).exists(),
            "delete-marker rollback should carry an explicit no-backup marker"
        );

        disk.delete_version(
            bucket,
            object,
            delete_marker.clone(),
            true,
            DeleteOptions {
                undo_write: true,
                undo_delete: true,
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("undo should remove new delete marker metadata");
        assert!(!object_dir.join(STORAGE_FORMAT_FILE).exists());
        assert!(!rollback_path.exists());

        disk.delete_version(
            bucket,
            object,
            delete_marker,
            true,
            DeleteOptions {
                undo_write: true,
                undo_delete: true,
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("repeated delete-marker undo should be a no-op");
        assert!(!object_dir.join(STORAGE_FORMAT_FILE).exists());
    }

    #[tokio::test]
    async fn test_rename_data_failure_before_metadata_commit_preserves_old_metadata() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let bucket = "bucket";
        let object = "failpoint-object";
        let tmp_object = "tmp-object";
        let version_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("version id should parse");
        let old_data_dir = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("old data dir should parse");
        let new_data_dir = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").expect("version id should parse");

        ensure_test_volume(&disk, bucket).await;
        ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

        let old_fi = test_file_info(object, version_id, Some(old_data_dir), None);
        let old_meta = test_meta(old_fi);
        let object_dir = dir.path().join(bucket).join(object);
        fs::create_dir_all(object_dir.join(old_data_dir.to_string()))
            .await
            .expect("old data dir should be created");
        fs::write(object_dir.join(STORAGE_FORMAT_FILE), old_meta.clone())
            .await
            .expect("old metadata should be written");

        let tmp_data_dir = dir
            .path()
            .join(RUSTFS_META_TMP_BUCKET)
            .join(tmp_object)
            .join(new_data_dir.to_string());
        fs::create_dir_all(&tmp_data_dir)
            .await
            .expect("new tmp data dir should be created");
        fs::write(tmp_data_dir.join("part.1"), b"new-data")
            .await
            .expect("new tmp data should be written");

        set_rename_data_fail_before_old_metadata_backup(object);
        let new_fi = test_file_info(object, version_id, Some(new_data_dir), None);
        let result = disk
            .rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi.clone(), bucket, object)
            .await;

        assert!(result.is_err());
        let current_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("old metadata should still be readable");
        assert_eq!(current_meta, old_meta);
        assert_eq!(
            fs::read(tmp_data_dir.join("part.1"))
                .await
                .expect("failed commit must restore the staged data directory"),
            b"new-data",
            "recursive rollback must preserve the staged shard payload"
        );
        assert!(
            !object_dir.join(new_data_dir.to_string()).exists(),
            "failed commit must not strand the new data directory at the destination"
        );

        disk.rename_data(RUSTFS_META_TMP_BUCKET, tmp_object, new_fi, bucket, object)
            .await
            .expect("the same staged request should succeed on its first retry");
        assert_eq!(
            fs::read(object_dir.join(new_data_dir.to_string()).join("part.1"))
                .await
                .expect("retried commit should publish the staged shard"),
            b"new-data"
        );
        assert!(!tmp_data_dir.exists(), "successful retry must consume the restored staging directory");
        let committed_meta = fs::read(object_dir.join(STORAGE_FORMAT_FILE))
            .await
            .expect("retried metadata should be readable");
        let committed_meta = FileMeta::load(&committed_meta).expect("retried metadata should parse");
        let (_, committed_version) = committed_meta
            .find_version(Some(version_id))
            .expect("retried metadata should contain the requested version");
        assert_eq!(
            committed_version
                .object
                .expect("retried non-inline version should contain object metadata")
                .data_dir,
            Some(new_data_dir)
        );
    }

    #[tokio::test]
    async fn test_skip_access_checks() {
        // let arr = Vec::new();

        let vols = [
            RUSTFS_META_TMP_DELETED_BUCKET,
            RUSTFS_META_TMP_BUCKET,
            super::super::RUSTFS_META_MULTIPART_BUCKET,
            RUSTFS_META_BUCKET,
        ];

        let paths: Vec<_> = vols.iter().map(|v| path_join(&[Path::new(v), Path::new("test")])).collect();

        for p in paths.iter() {
            assert!(skip_access_checks(p.to_str().expect("operation should succeed")));
        }
    }

    #[derive(Debug, Default)]
    struct PendingTestReader;

    impl AsyncRead for PendingTestReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    #[tokio::test(start_paused = true)]
    async fn local_read_timeout_reader_times_out_when_inner_stalls() {
        let mut reader = StallTimeoutReader::new(PendingTestReader, Duration::from_secs(10));
        let mut buf = [0; 1];

        let err = reader
            .read(&mut buf)
            .await
            .expect_err("stalled local reader should return a timeout error");

        assert_eq!(err.kind(), ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn local_read_timeout_reader_with_zero_timeout_stays_pending() {
        let mut reader = StallTimeoutReader::new(PendingTestReader, Duration::ZERO);
        let mut buf = [0; 1];

        let result = timeout(Duration::from_millis(10), reader.read(&mut buf)).await;

        assert!(result.is_err(), "zero timeout must leave stalled reads pending instead of failing");
    }

    #[tokio::test]
    async fn test_get_disk_id_invalidates_cache_after_format_removal() {
        use crate::disk::FORMAT_CONFIG_FILE;
        use crate::disk::format::FormatV3;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let meta_dir = dir.path().join(RUSTFS_META_BUCKET);
        fs::create_dir_all(&meta_dir).await.expect("meta dir should be creatable");
        let mut format = FormatV3::new(1, 1);
        format.erasure.this = format.erasure.sets[0][0];
        let format_json = format.to_json().expect("format should serialize");
        fs::write(meta_dir.join(FORMAT_CONFIG_FILE), format_json)
            .await
            .expect("format.json should be writable");

        let disk = LocalDisk::new(&endpoint, false)
            .await
            .expect("local disk should open after seeding format");

        let initial_id = disk.get_disk_id().await.expect("disk id lookup should succeed");
        assert!(initial_id.is_some(), "new disk should expose a disk id");

        fs::remove_file(&disk.format_path)
            .await
            .expect("format.json should be removable");

        tokio::time::sleep(Duration::from_secs(2)).await;

        let err = disk
            .get_disk_id()
            .await
            .expect_err("removed format.json should invalidate the cached disk id");
        assert!(matches!(err, DiskError::UnformattedDisk));

        let format_info = disk.format_info.read().await.clone();
        assert!(format_info.id.is_none(), "cached disk id should be cleared");
        assert!(format_info.data.is_empty(), "cached format bytes should be cleared");
        assert!(format_info.file_info.is_none(), "cached file metadata should be cleared");
        assert!(format_info.last_check.is_none(), "cached format timestamp should be cleared");
    }

    #[tokio::test]
    async fn cleanup_tmp_on_startup_allows_missing_tmp_directory() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let publication_root = os::PublicationRoot::new(dir.path()).expect("publication root should open");

        LocalDisk::cleanup_tmp_on_startup(dir.path(), &publication_root, Arc::new(AtomicU32::new(0)), Arc::new(Notify::new()))
            .await
            .expect("missing temporary directory should already be clean");

        assert!(LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_DELETED_BUCKET).exists());
    }

    #[tokio::test]
    async fn cleanup_tmp_on_startup_moves_existing_tmp_and_recreates_trash() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let publication_root = os::PublicationRoot::new(dir.path()).expect("publication root should open");
        let tmp = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_BUCKET);
        let leftover = tmp.join("leftover").join("data");
        fs::create_dir_all(leftover.parent().expect("operation should succeed"))
            .await
            .expect("operation should succeed");
        fs::write(&leftover, b"temporary").await.expect("operation should succeed");

        LocalDisk::cleanup_tmp_on_startup(dir.path(), &publication_root, Arc::new(AtomicU32::new(0)), Arc::new(Notify::new()))
            .await
            .expect("operation should succeed");

        assert!(!tmp.join("leftover").exists());
        assert!(LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_DELETED_BUCKET).exists());
    }

    #[tokio::test]
    async fn cleanup_stale_tmp_objects_moves_expired_tmp_dirs_to_trash() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let publication_root = os::PublicationRoot::new(dir.path()).expect("publication root should open");
        let tmp = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_BUCKET);
        let stale = tmp.join("stale").join("data");
        let trash = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_DELETED_BUCKET);
        fs::create_dir_all(stale.parent().expect("operation should succeed"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(&trash).await.expect("operation should succeed");
        fs::write(&stale, b"temporary").await.expect("operation should succeed");

        // Backdate after the write above: creating stale/data refreshes the
        // scanned tmp/stale directory's mtime.
        backdate_mtime(&tmp.join("stale"), Duration::from_secs(10));
        LocalDisk::cleanup_stale_tmp_objects_with_expiry(dir.path().to_path_buf(), &publication_root, Duration::ZERO)
            .await
            .expect("operation should succeed");

        assert!(!tmp.join("stale").exists());
        assert!(trash.exists());

        let mut entries = fs::read_dir(&trash).await.expect("operation should succeed");
        assert!(entries.next_entry().await.expect("operation should succeed").is_some());
    }

    #[tokio::test]
    async fn cleanup_stale_tmp_objects_keeps_fresh_dirs_and_regular_files() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let publication_root = os::PublicationRoot::new(dir.path()).expect("publication root should open");
        let tmp = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_BUCKET);
        let fresh_dir = tmp.join("fresh").join("data");
        let regular_file = tmp.join("note.txt");
        let trash = LocalDisk::meta_path(dir.path(), RUSTFS_META_TMP_DELETED_BUCKET);

        fs::create_dir_all(fresh_dir.parent().expect("operation should succeed"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(&trash).await.expect("operation should succeed");
        fs::write(&fresh_dir, b"temporary").await.expect("operation should succeed");
        fs::write(&regular_file, b"keep").await.expect("operation should succeed");

        LocalDisk::cleanup_stale_tmp_objects_with_expiry(dir.path().to_path_buf(), &publication_root, Duration::from_secs(60))
            .await
            .expect("operation should succeed");

        assert!(tmp.join("fresh").exists());
        assert!(regular_file.exists());

        let mut entries = fs::read_dir(&trash).await.expect("operation should succeed");
        assert!(entries.next_entry().await.expect("operation should succeed").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_loop_interval_does_not_tick_immediately() {
        let start_at = Instant::now() + DELETED_OBJECTS_CLEANUP_INTERVAL;
        let mut interval = interval_at(start_at, DELETED_OBJECTS_CLEANUP_INTERVAL);

        assert!(timeout(Duration::from_secs(1), interval.tick()).await.is_err());

        tokio::time::advance(DELETED_OBJECTS_CLEANUP_INTERVAL).await;
        interval.tick().await;
    }

    #[tokio::test(start_paused = true)]
    async fn startup_cleanup_barrier_waits_for_notification() {
        let ready = Arc::new(AtomicU32::new(0));
        let notify = Arc::new(Notify::new());

        let wait = tokio::spawn({
            let ready = ready.clone();
            let notify = notify.clone();
            async move { wait_for_startup_cleanup_signal(ready.as_ref(), notify.as_ref(), Duration::from_secs(2)).await }
        });

        tokio::task::yield_now().await;
        assert!(!wait.is_finished());

        ready.store(1, Ordering::Release);
        notify.notify_waiters();

        assert!(wait.await.expect("operation should succeed"));
    }

    #[tokio::test(start_paused = true)]
    async fn startup_cleanup_barrier_times_out() {
        let ready = Arc::new(AtomicU32::new(0));
        let notify = Arc::new(Notify::new());

        let wait = tokio::spawn({
            let ready = ready.clone();
            let notify = notify.clone();
            async move { wait_for_startup_cleanup_signal(ready.as_ref(), notify.as_ref(), Duration::from_secs(2)).await }
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;

        assert!(!wait.await.expect("operation should succeed"));
    }

    #[test]
    fn metacache_write_obj_classifies_closed_output_stream() {
        struct BrokenPipeWriter;

        impl AsyncWrite for BrokenPipeWriter {
            fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<io::Result<usize>> {
                Poll::Ready(Err(io::Error::new(ErrorKind::BrokenPipe, "closed")))
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        let mut writer = BrokenPipeWriter;
        let mut out = MetacacheWriter::new(&mut writer);

        let err = futures::executor::block_on(write_metacache_obj(
            &mut out,
            &MetaCacheEntry {
                name: "object".to_string(),
                ..Default::default()
            },
        ))
        .expect_err("closed metacache output stream should fail");

        assert!(err.is_metacache_output_stream_closed());
    }

    #[tokio::test]
    async fn listing_metadata_probe_distinguishes_data_dirs_from_namespace_children() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let bucket = "test-bucket";
        let object_dir = dir.path().join(bucket).join("object");
        let data_dir = Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("parse data dir");
        let version_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").expect("parse version id");
        let mut file_meta = FileMeta::default();
        let mut file_info = FileInfo::new("object", 1, 1);
        file_info.data_dir = Some(data_dir);
        file_info.version_id = Some(version_id);
        file_info.mod_time = Some(OffsetDateTime::now_utc());
        file_meta.add_version(file_info).expect("add object version");

        fs::create_dir_all(object_dir.join(data_dir.to_string()))
            .await
            .expect("create version data dir");
        fs::write(
            object_dir.join(STORAGE_FORMAT_FILE),
            file_meta.marshal_msg().expect("encode object metadata"),
        )
        .await
        .expect("write object metadata");

        let endpoint = Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("parse endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("create local disk");

        let leaf = disk
            .read_listing_metadata(bucket, "object")
            .await
            .expect("read leaf metadata");
        assert!(!leaf.has_namespace_child_candidate);
        assert!(leaf.data_dirs.contains(&data_dir.to_string()));

        fs::create_dir(object_dir.join("child"))
            .await
            .expect("create namespace child");
        let parent = disk
            .read_listing_metadata(bucket, "object")
            .await
            .expect("read parent metadata");
        assert!(parent.has_namespace_child_candidate);
    }

    #[tokio::test]
    async fn test_scan_dir_includes_nested_object_dirs() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        fs::create_dir_all(bucket_dir.join("foo/bar/xyzzy"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(bucket_dir.join("quux/thud"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(bucket_dir.join("asdf"))
            .await
            .expect("operation should succeed");

        fs::write(bucket_dir.join("foo/bar/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join("foo/bar/xyzzy/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join("quux/thud/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join("asdf/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (reader, mut writer) = tokio::io::duplex(4096);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "".to_string(),
            recursive: true,
            ..Default::default()
        };
        let mut objs_returned = 0;

        disk.scan_dir("".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await
            .expect("operation should succeed");
        out.close().await.expect("operation should succeed");

        let mut reader = MetacacheReader::new(reader);
        let entries = reader.read_all().await.expect("operation should succeed");
        assert!(
            entries.iter().any(|entry| entry.name == "asdf/" && entry.metadata.is_empty()),
            "leaf object traversal markers must remain in the metacache stream"
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.name == "foo/bar/" && entry.metadata.is_empty()),
            "objects with namespace children must still emit a traversal prefix"
        );
        let names: Vec<String> = entries
            .into_iter()
            .filter(|entry| !entry.metadata.is_empty())
            .map(|entry| entry.name)
            .collect();

        assert!(names.contains(&"asdf".to_string()));
        assert!(names.contains(&"foo/bar".to_string()));
        assert!(names.contains(&"foo/bar/xyzzy".to_string()));
        assert!(names.contains(&"quux/thud".to_string()));
    }

    #[test]
    #[serial_test::serial]
    fn scan_dir_records_whole_parent_read_dir_before_page_limit() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should be created");
        let recorder = crate::test_metrics::CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                use rustfs_filemeta::MetacacheReader;
                use tempfile::tempdir;

                let dir = tempdir().expect("tempdir should be created");
                let bucket = "test-bucket";
                let bucket_dir = dir.path().join(bucket);
                const OBJECTS: usize = 12;

                for index in 0..OBJECTS {
                    let object_dir = bucket_dir.join(format!("object-{index:04}"));
                    fs::create_dir_all(&object_dir)
                        .await
                        .expect("object directory should be created");
                    fs::write(object_dir.join(STORAGE_FORMAT_FILE), b"meta")
                        .await
                        .expect("object metadata should be written");
                }

                let endpoint =
                    Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
                let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

                let (reader, mut writer) = tokio::io::duplex(4096);
                let mut out = MetacacheWriter::new(&mut writer);
                let opts = WalkDirOptions {
                    bucket: bucket.to_string(),
                    base_dir: String::new(),
                    recursive: true,
                    limit: 1,
                    ..Default::default()
                };
                let mut objs_returned = 0;

                disk.scan_dir(String::new(), String::new(), &opts, &mut out, &mut objs_returned, false, None)
                    .await
                    .expect("scan_dir should succeed");
                out.close().await.expect("metacache writer should close");
                drop(out);
                drop(writer);

                let mut reader = MetacacheReader::new(reader);
                let visible_objects = reader
                    .read_all()
                    .await
                    .expect("scan output should decode")
                    .into_iter()
                    .filter(|entry| !entry.metadata.is_empty())
                    .count();

                assert_eq!(visible_objects, 1);
                assert_eq!(objs_returned, 1);
            });
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

        assert_eq!(
            recorder.counter_value(
                "rustfs_s3_list_objects_local_read_dir_total",
                &[("outcome", "ok"), ("count_mode", "whole"), ("is_root", "true")]
            ),
            1
        );
        assert_eq!(
            recorder.histogram_values(
                "rustfs_s3_list_objects_local_read_dir_entries",
                &[("outcome", "ok"), ("count_mode", "whole")]
            ),
            vec![12.0]
        );
    }

    #[test]
    #[serial_test::serial]
    fn scan_dir_leaf_object_avoids_recursive_scan() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should be created");
        let recorder = crate::test_metrics::CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                use tempfile::tempdir;

                let dir = tempdir().expect("tempdir should be created");
                let bucket = "test-bucket";
                let object_dir = dir.path().join(bucket).join("object");
                fs::create_dir_all(&object_dir)
                    .await
                    .expect("object directory should be created");
                fs::write(object_dir.join(STORAGE_FORMAT_FILE), b"meta")
                    .await
                    .expect("object metadata should be written");

                let endpoint =
                    Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
                let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
                let mut sink = tokio::io::sink();
                let mut out = MetacacheWriter::new(&mut sink);
                let opts = WalkDirOptions {
                    bucket: bucket.to_owned(),
                    recursive: true,
                    ..Default::default()
                };
                let mut objects_returned = 0;

                disk.scan_dir(String::new(), String::new(), &opts, &mut out, &mut objects_returned, false, None)
                    .await
                    .expect("scan_dir should succeed");
                out.close().await.expect("metacache writer should close");

                assert_eq!(objects_returned, 1);
            });
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);

        assert_eq!(
            recorder.counter_value(
                "rustfs_s3_list_objects_local_read_dir_total",
                &[("outcome", "ok"), ("count_mode", "whole"), ("is_root", "true")]
            ),
            1
        );
        assert_eq!(
            recorder.counter_value(
                "rustfs_s3_list_objects_local_read_dir_total",
                &[("outcome", "ok"), ("count_mode", "whole"), ("is_root", "false")]
            ),
            0
        );
    }

    #[tokio::test]
    async fn test_scan_dir_reports_base_dir_object_metadata() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let base_dir = "base-object";
        let bucket_dir = dir.path().join(bucket);
        fs::create_dir_all(bucket_dir.join(base_dir))
            .await
            .expect("base object dir should be created");
        fs::write(bucket_dir.join(base_dir).join(STORAGE_FORMAT_FILE), b"meta")
            .await
            .expect("base object metadata should be written");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (reader, mut writer) = tokio::io::duplex(4096);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: base_dir.to_string(),
            recursive: false,
            ..Default::default()
        };
        let mut objs_returned = 0;

        disk.scan_dir(base_dir.to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await
            .expect("operation should succeed");
        out.close().await.expect("operation should succeed");

        let mut reader = MetacacheReader::new(reader);
        let entries = reader.read_all().await.expect("operation should succeed");
        let names: Vec<String> = entries
            .into_iter()
            .filter(|entry| !entry.metadata.is_empty())
            .map(|entry| entry.name)
            .collect();

        assert_eq!(names, vec![format!("{base_dir}/")]);
        assert_eq!(objs_returned, 1);
    }

    #[tokio::test]
    async fn test_scan_dir_deduplicates_explicit_dir_marker_recursion() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        fs::create_dir_all(bucket_dir.join("marker/file.txt"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(bucket_dir.join("marker/subdir/file.txt"))
            .await
            .expect("operation should succeed");
        fs::create_dir_all(bucket_dir.join(format!("marker/subdir{GLOBAL_DIR_SUFFIX}")))
            .await
            .expect("operation should succeed");

        fs::write(bucket_dir.join("marker/file.txt/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join("marker/subdir/file.txt/xl.meta"), b"meta")
            .await
            .expect("operation should succeed");
        fs::write(bucket_dir.join(format!("marker/subdir{GLOBAL_DIR_SUFFIX}/xl.meta")), b"meta")
            .await
            .expect("operation should succeed");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (reader, mut writer) = tokio::io::duplex(4096);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "marker/".to_string(),
            recursive: true,
            ..Default::default()
        };
        let mut objs_returned = 0;

        disk.scan_dir("marker/".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await
            .expect("operation should succeed");
        out.close().await.expect("operation should succeed");

        let mut reader = MetacacheReader::new(reader);
        let entries = reader.read_all().await.expect("operation should succeed");
        let names: Vec<String> = entries
            .into_iter()
            .filter(|entry| !entry.metadata.is_empty())
            .map(|entry| entry.name)
            .collect();

        assert_eq!(names.iter().filter(|name| *name == "marker/subdir/file.txt").count(), 1);
        assert_eq!(names.iter().filter(|name| *name == "marker/subdir/").count(), 1);
        assert_eq!(names.iter().filter(|name| *name == "marker/file.txt").count(), 1);
    }

    // The per-disk walk stream must be sorted by name so the k-way merge across
    // sets (which assumes each channel is non-decreasing, see
    // `merge_entry_channels`, backlog#1046) never reorders keys. An explicit
    // directory-marker object (`folder/` on disk as `folder__XLDIR__/xl.meta`)
    // coexisting with real children under the same prefix is the case most
    // likely to break that invariant: the marker and the real directory resolve
    // to the same stacked name, and the marker must still be emitted at that
    // sorted position rather than after the recursion into its children.
    // Empirically the stream stays sorted; this pins it (backlog#1068
    // investigation — the suspected out-of-order emission does not occur).
    #[tokio::test]
    async fn test_scan_dir_marker_with_children_emits_sorted_stream() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        async fn scan_sequence(layout: &[(&str, &[u8])], base: &str, recursive: bool) -> Vec<(String, bool)> {
            let dir = tempdir().expect("tempdir should be created");
            let bucket = "test-bucket";
            let bucket_dir = dir.path().join(bucket);
            for (path, body) in layout {
                let full = bucket_dir.join(path);
                fs::create_dir_all(full.parent().expect("child path has a parent"))
                    .await
                    .expect("layout dir should be created");
                fs::write(full, body).await.expect("layout metadata should be written");
            }
            let endpoint =
                Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
            let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
            let (reader, mut writer) = tokio::io::duplex(16384);
            let mut out = MetacacheWriter::new(&mut writer);
            let opts = WalkDirOptions {
                bucket: bucket.to_string(),
                base_dir: base.to_string(),
                recursive,
                ..Default::default()
            };
            let mut objs_returned = 0;
            disk.scan_dir(base.to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
                .await
                .expect("scan_dir should succeed");
            out.close().await.expect("metacache writer should close");
            drop(out);
            drop(writer);
            let mut reader = MetacacheReader::new(reader);
            reader
                .read_all()
                .await
                .expect("scan output should decode")
                .into_iter()
                .map(|entry| (entry.name, !entry.metadata.is_empty()))
                .collect()
        }

        fn assert_marker_stream_sorted(label: &str, seq: &[(String, bool)]) {
            let names: Vec<&str> = seq.iter().map(|(name, _)| name.as_str()).collect();
            let mut sorted = names.clone();
            sorted.sort();
            assert_eq!(names, sorted, "[{label}] scan stream must be sorted, got {seq:?}");
            // A marker object (metadata-bearing, trailing-slash name) must not
            // trail its own children.
            for (i, (name, has_meta)) in seq.iter().enumerate() {
                if *has_meta && name.ends_with(SLASH_SEPARATOR) {
                    let later_child = seq[..i]
                        .iter()
                        .any(|(other, _)| other.starts_with(name.as_str()) && other != name);
                    assert!(!later_child, "[{label}] marker {name} emitted after its own child, seq={seq:?}");
                }
            }
        }

        let suffix = GLOBAL_DIR_SUFFIX;

        assert_marker_stream_sorted(
            "marker + direct children",
            &scan_sequence(
                &[
                    (&format!("folder{suffix}/xl.meta"), b"m"),
                    ("folder/a/xl.meta", b"c"),
                    ("folder/b/xl.meta", b"c"),
                ],
                "",
                true,
            )
            .await,
        );
        assert_marker_stream_sorted(
            "nested marker",
            &scan_sequence(
                &[
                    (&format!("folder/sub{suffix}/xl.meta"), b"m"),
                    ("folder/sub/a/xl.meta", b"c"),
                    ("folder/sub/z/xl.meta", b"c"),
                ],
                "",
                true,
            )
            .await,
        );
        assert_marker_stream_sorted(
            "marker with a deep child chain",
            &scan_sequence(&[(&format!("folder{suffix}/xl.meta"), b"m"), ("folder/a/b/c/xl.meta", b"c")], "", true).await,
        );
        assert_marker_stream_sorted(
            "two sibling markers",
            &scan_sequence(
                &[
                    (&format!("a{suffix}/xl.meta"), b"m"),
                    ("a/x/xl.meta", b"c"),
                    (&format!("b{suffix}/xl.meta"), b"m"),
                    ("b/y/xl.meta", b"c"),
                ],
                "",
                true,
            )
            .await,
        );
    }

    #[tokio::test]
    async fn test_scan_dir_forward_to_repeated_prefix_component() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        for name in [
            "different/prefix/prefix/repo-0000",
            "different/prefix/prefix/repo-0001",
            "different/prefix/prefix/repo-0002",
            "engineering/alpha-0000",
            "engineering/engineering/engineering/repo-0000",
            "engineering/engineering/engineering/repo-0001",
            "engineering/engineering/repo-0000",
            "engineering/engineering/repo-0001",
            "engineering/engineering/repo-0002",
            "engineering/zulu-0000",
            "unrelated/engineering/repo-0000",
        ] {
            let object_dir = bucket_dir.join(name);
            fs::create_dir_all(&object_dir).await.expect("operation should succeed");
            fs::write(object_dir.join(STORAGE_FORMAT_FILE), b"meta")
                .await
                .expect("operation should succeed");
        }

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        async fn scan_names(disk: &LocalDisk, bucket: &str, base_dir: &str, forward_to: &str) -> (Vec<String>, i32) {
            let (reader, mut writer) = tokio::io::duplex(4096);
            let mut out = MetacacheWriter::new(&mut writer);
            let opts = WalkDirOptions {
                bucket: bucket.to_string(),
                base_dir: base_dir.to_string(),
                recursive: true,
                forward_to: Some(forward_to.to_string()),
                ..Default::default()
            };
            let mut objs_returned = 0;

            disk.scan_dir(base_dir.to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
                .await
                .expect("operation should succeed");
            out.close().await.expect("operation should succeed");
            drop(out);
            drop(writer);

            let mut reader = MetacacheReader::new(reader);
            let entries = reader.read_all().await.expect("operation should succeed");
            let names: Vec<String> = entries
                .into_iter()
                .filter(|entry| !entry.metadata.is_empty())
                .map(|entry| entry.name)
                .collect();

            (names, objs_returned)
        }

        let (engineering_names, engineering_count) =
            scan_names(&disk, bucket, "engineering/", "engineering/engineering/engineering/repo-0001").await;

        assert_eq!(
            engineering_names,
            vec![
                "engineering/engineering/engineering/repo-0001".to_string(),
                "engineering/engineering/repo-0000".to_string(),
                "engineering/engineering/repo-0001".to_string(),
                "engineering/engineering/repo-0002".to_string(),
                "engineering/zulu-0000".to_string(),
            ],
            "forward_to must resume at the requested triply repeated prefix and preserve lexicographic order"
        );
        assert_eq!(engineering_count as usize, engineering_names.len());

        let (different_names, different_count) =
            scan_names(&disk, bucket, "different/", "different/prefix/prefix/repo-0001").await;

        assert_eq!(
            different_names,
            vec![
                "different/prefix/prefix/repo-0001".to_string(),
                "different/prefix/prefix/repo-0002".to_string(),
            ],
            "forward_to must also work for repeated components unrelated to the engineering prefix"
        );
        assert_eq!(different_count as usize, different_names.len());

        let (double_names, double_count) = scan_names(&disk, bucket, "engineering/", "engineering/engineering/repo-0001").await;

        assert_eq!(
            double_names,
            vec![
                "engineering/engineering/repo-0001".to_string(),
                "engineering/engineering/repo-0002".to_string(),
                "engineering/zulu-0000".to_string(),
            ],
            "forward_to must not skip a child directory whose name repeats the base prefix"
        );
        assert_eq!(double_count as usize, double_names.len());
    }

    #[tokio::test]
    async fn test_scan_dir_hidden_delete_markers_do_not_exhaust_limit() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        fn delete_marker_metadata(version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            fm.add_version(FileInfo {
                deleted: true,
                version_id: Some(Uuid::parse_str(version_id).expect("test version id should parse")),
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            })
            .expect("delete marker metadata should be valid");
            fm.marshal_msg().expect("delete marker metadata should encode")
        }

        fn delete_marker_with_old_object_metadata(delete_version_id: &str, object_version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            fm.add_version({
                let mut fi = FileInfo::new("hidden", 1, 1);
                fi.version_id = Some(Uuid::parse_str(object_version_id).expect("test version id should parse"));
                fi.mod_time = Some(OffsetDateTime::now_utc() - time::Duration::seconds(1));
                fi
            })
            .expect("object metadata should be valid");
            fm.add_version(FileInfo {
                deleted: true,
                version_id: Some(Uuid::parse_str(delete_version_id).expect("test version id should parse")),
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            })
            .expect("delete marker metadata should be valid");
            fm.marshal_msg().expect("delete marker metadata should encode")
        }

        fn object_metadata(version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            let mut fi = FileInfo::new("visible", 1, 1);
            fi.version_id = Some(Uuid::parse_str(version_id).expect("test version id should parse"));
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).expect("object metadata should be valid");
            fm.marshal_msg().expect("object metadata should encode")
        }

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        for (name, version_id) in [
            ("shard/aaa-trash-0000", "11111111-1111-1111-1111-111111111111"),
            ("shard/aaa-trash-0001", "22222222-2222-2222-2222-222222222222"),
            ("shard/aaa-trash-0002", "33333333-3333-3333-3333-333333333333"),
        ] {
            let object_dir = bucket_dir.join(name);
            fs::create_dir_all(&object_dir).await.expect("operation should succeed");
            fs::write(object_dir.join(STORAGE_FORMAT_FILE), delete_marker_metadata(version_id))
                .await
                .expect("operation should succeed");
        }

        let hidden_versioned_dir = bucket_dir.join("shard/aaa-trash-0003");
        fs::create_dir_all(&hidden_versioned_dir)
            .await
            .expect("operation should succeed");
        fs::write(
            hidden_versioned_dir.join(STORAGE_FORMAT_FILE),
            delete_marker_with_old_object_metadata(
                "44444444-4444-4444-4444-444444444444",
                "55555555-5555-5555-5555-555555555555",
            ),
        )
        .await
        .expect("operation should succeed");

        let visible_dir = bucket_dir.join("shard/bbb-visible-0000");
        fs::create_dir_all(&visible_dir).await.expect("operation should succeed");
        fs::write(
            visible_dir.join(STORAGE_FORMAT_FILE),
            object_metadata("66666666-6666-6666-6666-666666666666"),
        )
        .await
        .expect("operation should succeed");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (reader, mut writer) = tokio::io::duplex(4096);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "".to_string(),
            recursive: true,
            limit: 1,
            ..Default::default()
        };
        let mut objs_returned = 0;

        disk.scan_dir("".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await
            .expect("operation should succeed");
        out.close().await.expect("operation should succeed");
        drop(out);
        drop(writer);

        let mut reader = MetacacheReader::new(reader);
        let has_visible_object = reader
            .read_all()
            .await
            .expect("operation should succeed")
            .into_iter()
            .any(|entry| !entry.metadata.is_empty() && entry.name == "shard/bbb-visible-0000");

        assert!(has_visible_object);
        assert_eq!(objs_returned, 1);
    }

    #[tokio::test]
    async fn test_scan_dir_nonrecursive_skips_dirs_with_only_hidden_delete_markers() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        fn hidden_versioned_object_metadata(name: &str, delete_version_id: &str, object_version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            fm.add_version({
                let mut fi = FileInfo::new(name, 1, 1);
                fi.version_id = Some(Uuid::parse_str(object_version_id).expect("test version id should parse"));
                fi.mod_time = Some(OffsetDateTime::now_utc() - time::Duration::seconds(1));
                fi
            })
            .expect("object metadata should be valid");
            fm.add_version(FileInfo {
                name: name.to_owned(),
                deleted: true,
                version_id: Some(Uuid::parse_str(delete_version_id).expect("test version id should parse")),
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            })
            .expect("delete marker metadata should be valid");
            fm.marshal_msg().expect("hidden metadata should encode")
        }

        fn visible_object_metadata(name: &str, version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            let mut fi = FileInfo::new(name, 1, 1);
            fi.version_id = Some(Uuid::parse_str(version_id).expect("test version id should parse"));
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).expect("object metadata should be valid");
            fm.marshal_msg().expect("visible metadata should encode")
        }

        async fn scan_names(disk: &LocalDisk, bucket: &str, base_dir: &str, incl_deleted: bool) -> Vec<String> {
            let (reader, mut writer) = tokio::io::duplex(4096);
            let mut out = MetacacheWriter::new(&mut writer);
            let opts = WalkDirOptions {
                bucket: bucket.to_string(),
                base_dir: base_dir.to_string(),
                recursive: false,
                incl_deleted,
                ..Default::default()
            };
            let mut objs_returned = 0;

            disk.scan_dir(base_dir.to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
                .await
                .expect("scan_dir should succeed");
            out.close().await.expect("metacache writer should close");
            drop(out);
            drop(writer);

            let mut reader = MetacacheReader::new(reader);
            reader
                .read_all()
                .await
                .expect("scan output should decode")
                .into_iter()
                .map(|entry| entry.name)
                .collect()
        }

        let dir = tempdir().expect("tempdir should be created");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        let hidden_object = bucket_dir.join("hidden/deleted.txt");
        fs::create_dir_all(&hidden_object)
            .await
            .expect("hidden object dir should be created");
        fs::write(
            hidden_object.join(STORAGE_FORMAT_FILE),
            hidden_versioned_object_metadata(
                "hidden/deleted.txt",
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222",
            ),
        )
        .await
        .expect("hidden object metadata should be written");

        let nested_hidden_object = bucket_dir.join("hidden/nested/deleted.txt");
        fs::create_dir_all(&nested_hidden_object)
            .await
            .expect("nested hidden object dir should be created");
        fs::write(
            nested_hidden_object.join(STORAGE_FORMAT_FILE),
            hidden_versioned_object_metadata(
                "hidden/nested/deleted.txt",
                "33333333-3333-3333-3333-333333333333",
                "44444444-4444-4444-4444-444444444444",
            ),
        )
        .await
        .expect("nested hidden object metadata should be written");

        let visible_object = bucket_dir.join("visible/nested/object.txt");
        fs::create_dir_all(&visible_object)
            .await
            .expect("visible object dir should be created");
        fs::write(
            visible_object.join(STORAGE_FORMAT_FILE),
            visible_object_metadata("visible/nested/object.txt", "55555555-5555-5555-5555-555555555555"),
        )
        .await
        .expect("visible object metadata should be written");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let root_names = scan_names(&disk, bucket, "", false).await;
        assert!(!root_names.contains(&"hidden/".to_string()));
        assert!(root_names.contains(&"visible/".to_string()));

        let hidden_names = scan_names(&disk, bucket, "hidden/", false).await;
        assert!(!hidden_names.contains(&"hidden/nested/".to_string()));

        let visible_names = scan_names(&disk, bucket, "visible/", false).await;
        assert!(visible_names.contains(&"visible/nested/".to_string()));

        let versioned_root_names = scan_names(&disk, bucket, "", true).await;
        assert!(versioned_root_names.contains(&"hidden/".to_string()));

        let versioned_hidden_names = scan_names(&disk, bucket, "hidden/", true).await;
        assert!(versioned_hidden_names.contains(&"hidden/nested/".to_string()));
    }

    /// backlog#1042: in a non-recursive (delimiter) listing a plain object `a`
    /// and a sibling object `a/b` under the same-named prefix must BOTH appear —
    /// `a` as an object (Contents) and `a/` as a prefix dir (CommonPrefix). A leaf
    /// object with no sibling must NOT gain a spurious prefix.
    #[tokio::test]
    async fn test_scan_dir_nonrecursive_object_and_sibling_prefix_coexist() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        fn visible_object_metadata(name: &str, version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            let mut fi = FileInfo::new(name, 1, 1);
            fi.version_id = Some(Uuid::parse_str(version_id).expect("test version id should parse"));
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).expect("object metadata should be valid");
            fm.marshal_msg().expect("visible metadata should encode")
        }

        async fn scan_entries(disk: &LocalDisk, bucket: &str, base_dir: &str) -> Vec<(String, bool)> {
            let (reader, mut writer) = tokio::io::duplex(4096);
            let mut out = MetacacheWriter::new(&mut writer);
            let opts = WalkDirOptions {
                bucket: bucket.to_string(),
                base_dir: base_dir.to_string(),
                recursive: false,
                ..Default::default()
            };
            let mut objs_returned = 0;
            disk.scan_dir(base_dir.to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
                .await
                .expect("scan_dir should succeed");
            out.close().await.expect("metacache writer should close");
            drop(out);
            drop(writer);

            let mut reader = MetacacheReader::new(reader);
            reader
                .read_all()
                .await
                .expect("scan output should decode")
                .into_iter()
                .map(|entry| (entry.name.clone(), entry.is_object()))
                .collect()
        }

        let dir = tempdir().expect("tempdir should be created");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        // Object `a` at bucket/a/xl.meta.
        let obj_a = bucket_dir.join("a");
        fs::create_dir_all(&obj_a).await.expect("object a dir should be created");
        fs::write(
            obj_a.join(STORAGE_FORMAT_FILE),
            visible_object_metadata("a", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        )
        .await
        .expect("object a metadata should be written");

        // Sibling object `a/b` at bucket/a/b/xl.meta — makes `a/` a real prefix.
        let obj_ab = bucket_dir.join("a/b");
        fs::create_dir_all(&obj_ab).await.expect("object a/b dir should be created");
        fs::write(
            obj_ab.join(STORAGE_FORMAT_FILE),
            visible_object_metadata("a/b", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        )
        .await
        .expect("object a/b metadata should be written");

        // Leaf object `c` at bucket/c/xl.meta — must NOT produce a prefix `c/`.
        let obj_c = bucket_dir.join("c");
        fs::create_dir_all(&obj_c).await.expect("object c dir should be created");
        fs::write(
            obj_c.join(STORAGE_FORMAT_FILE),
            visible_object_metadata("c", "cccccccc-cccc-cccc-cccc-cccccccccccc"),
        )
        .await
        .expect("object c metadata should be written");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let entries = scan_entries(&disk, bucket, "").await;

        assert!(
            entries.iter().any(|(name, is_object)| name == "a" && *is_object),
            "object `a` must be emitted as an object, got {entries:?}"
        );
        assert!(
            entries.iter().any(|(name, is_object)| name == "a/" && !*is_object),
            "prefix `a/` must be emitted as a dir because sibling `a/b` exists, got {entries:?}"
        );
        assert!(
            entries.iter().any(|(name, is_object)| name == "c" && *is_object),
            "leaf object `c` must be emitted as an object, got {entries:?}"
        );
        assert!(
            !entries.iter().any(|(name, _)| name == "c/"),
            "leaf object `c` must not produce a spurious prefix `c/`, got {entries:?}"
        );
    }

    // Regression for backlog#1042: on a single disk, a plain object `a` and its
    // children `a/...` share one backing directory, so the non-recursive scan
    // must produce both the object entry `a` and the prefix entry `a/` — while
    // an object whose directory only holds its own xl.meta and data dirs must
    // not grow a phantom prefix.
    #[tokio::test]
    async fn test_scan_dir_nonrecursive_object_with_children_emits_prefix() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        fn visible_object_metadata(name: &str, version_id: &str, data_dir: Option<&str>) -> Vec<u8> {
            let mut fm = FileMeta::default();
            let mut fi = FileInfo::new(name, 1, 1);
            fi.version_id = Some(Uuid::parse_str(version_id).expect("test version id should parse"));
            fi.data_dir = data_dir.map(|dir| Uuid::parse_str(dir).expect("test data dir should parse"));
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).expect("object metadata should be valid");
            fm.marshal_msg().expect("visible metadata should encode")
        }

        fn hidden_object_metadata(name: &str, version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            fm.add_version(FileInfo {
                name: name.to_owned(),
                deleted: true,
                version_id: Some(Uuid::parse_str(version_id).expect("test version id should parse")),
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            })
            .expect("delete marker metadata should be valid");
            fm.marshal_msg().expect("hidden metadata should encode")
        }

        async fn scan_entries(disk: &LocalDisk, bucket: &str, incl_deleted: bool) -> Vec<(String, bool)> {
            let (reader, mut writer) = tokio::io::duplex(65536);
            let mut out = MetacacheWriter::new(&mut writer);
            let opts = WalkDirOptions {
                bucket: bucket.to_string(),
                base_dir: "".to_string(),
                recursive: false,
                incl_deleted,
                limit: 1000,
                ..Default::default()
            };
            let mut objs_returned = 0;

            disk.scan_dir("".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
                .await
                .expect("scan_dir should succeed");
            out.close().await.expect("metacache writer should close");
            drop(out);
            drop(writer);

            let mut reader = MetacacheReader::new(reader);
            reader
                .read_all()
                .await
                .expect("scan output should decode")
                .into_iter()
                .map(|entry| (entry.name, !entry.metadata.is_empty()))
                .collect()
        }

        let dir = tempdir().expect("tempdir should be created");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        // `alpha` object with a data dir, plus a real child `alpha/beta`.
        let alpha_data_dir = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        fs::create_dir_all(bucket_dir.join("alpha").join(alpha_data_dir))
            .await
            .expect("alpha data dir should be created");
        fs::write(bucket_dir.join("alpha").join(alpha_data_dir).join("part.1"), b"data")
            .await
            .expect("alpha part should be written");
        fs::write(
            bucket_dir.join("alpha").join(STORAGE_FORMAT_FILE),
            visible_object_metadata("alpha", "11111111-1111-1111-1111-111111111111", Some(alpha_data_dir)),
        )
        .await
        .expect("alpha metadata should be written");
        fs::create_dir_all(bucket_dir.join("alpha/beta"))
            .await
            .expect("alpha child dir should be created");
        fs::write(
            bucket_dir.join("alpha/beta").join(STORAGE_FORMAT_FILE),
            visible_object_metadata("alpha/beta", "22222222-2222-2222-2222-222222222222", None),
        )
        .await
        .expect("alpha child metadata should be written");

        // `gamma` object whose only extra child is hidden by a delete marker.
        fs::create_dir_all(bucket_dir.join("gamma/hidden"))
            .await
            .expect("gamma child dir should be created");
        fs::write(
            bucket_dir.join("gamma").join(STORAGE_FORMAT_FILE),
            visible_object_metadata("gamma", "33333333-3333-3333-3333-333333333333", None),
        )
        .await
        .expect("gamma metadata should be written");
        fs::write(
            bucket_dir.join("gamma/hidden").join(STORAGE_FORMAT_FILE),
            hidden_object_metadata("gamma/hidden", "44444444-4444-4444-4444-444444444444"),
        )
        .await
        .expect("gamma child metadata should be written");

        // `plain` object with only its own storage internals: no prefix expected.
        let plain_data_dir = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        fs::create_dir_all(bucket_dir.join("plain").join(plain_data_dir))
            .await
            .expect("plain data dir should be created");
        fs::write(bucket_dir.join("plain").join(plain_data_dir).join("part.1"), b"data")
            .await
            .expect("plain part should be written");
        // Data dirs can hold xl.meta-bearing subdirectories (multipart layout);
        // the probe must skip the whole data dir, not just non-metadata files.
        fs::create_dir_all(bucket_dir.join("plain").join(plain_data_dir).join("seg"))
            .await
            .expect("plain data subdir should be created");
        fs::write(
            bucket_dir
                .join("plain")
                .join(plain_data_dir)
                .join("seg")
                .join(STORAGE_FORMAT_FILE),
            visible_object_metadata("plain-part", "88888888-8888-8888-8888-888888888888", None),
        )
        .await
        .expect("plain data subdir metadata should be written");
        fs::write(
            bucket_dir.join("plain").join(STORAGE_FORMAT_FILE),
            visible_object_metadata("plain", "55555555-5555-5555-5555-555555555555", Some(plain_data_dir)),
        )
        .await
        .expect("plain metadata should be written");

        // `zeta` sorts last so its prefix is flushed by the final drain, not the
        // in-loop flush that `alpha/` exercises.
        fs::create_dir_all(bucket_dir.join("zeta/child"))
            .await
            .expect("zeta child dir should be created");
        fs::write(
            bucket_dir.join("zeta").join(STORAGE_FORMAT_FILE),
            visible_object_metadata("zeta", "66666666-6666-6666-6666-666666666666", None),
        )
        .await
        .expect("zeta metadata should be written");
        fs::write(
            bucket_dir.join("zeta/child").join(STORAGE_FORMAT_FILE),
            visible_object_metadata("zeta/child", "77777777-7777-7777-7777-777777777777", None),
        )
        .await
        .expect("zeta child metadata should be written");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let entries = scan_entries(&disk, bucket, false).await;
        let names: Vec<&str> = entries.iter().map(|(name, _)| name.as_str()).collect();

        assert_eq!(names, vec!["alpha", "alpha/", "gamma", "plain", "zeta", "zeta/"]);
        assert!(entries.iter().any(|(name, has_meta)| name == "alpha" && *has_meta));
        assert!(entries.iter().any(|(name, has_meta)| name == "alpha/" && !*has_meta));
        assert!(entries.iter().any(|(name, has_meta)| name == "zeta/" && !*has_meta));

        // Versioned listings surface delete-marker-only children, so `gamma/`
        // appears; data dirs still never masquerade as prefixes.
        let versioned_entries = scan_entries(&disk, bucket, true).await;
        let versioned_names: Vec<&str> = versioned_entries.iter().map(|(name, _)| name.as_str()).collect();

        assert_eq!(versioned_names, vec!["alpha", "alpha/", "gamma", "gamma/", "plain", "zeta", "zeta/"]);
    }

    // Preserve the explicit dir-marker semantics: a marker object `folder/` and
    // the real directory `folder/` still collapse to a single prefix entry; the
    // marker itself must not trigger the object-dir child probe.
    #[tokio::test]
    async fn test_scan_dir_nonrecursive_dir_marker_prefix_not_duplicated() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        fn visible_object_metadata(name: &str, version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            let mut fi = FileInfo::new(name, 1, 1);
            fi.version_id = Some(Uuid::parse_str(version_id).expect("test version id should parse"));
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).expect("object metadata should be valid");
            fm.marshal_msg().expect("visible metadata should encode")
        }

        let dir = tempdir().expect("tempdir should be created");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        fs::create_dir_all(bucket_dir.join(format!("folder{GLOBAL_DIR_SUFFIX}")))
            .await
            .expect("marker dir should be created");
        fs::write(
            bucket_dir
                .join(format!("folder{GLOBAL_DIR_SUFFIX}"))
                .join(STORAGE_FORMAT_FILE),
            visible_object_metadata("folder/", "11111111-1111-1111-1111-111111111111"),
        )
        .await
        .expect("marker metadata should be written");
        fs::create_dir_all(bucket_dir.join("folder/nested"))
            .await
            .expect("real child dir should be created");
        fs::write(
            bucket_dir.join("folder/nested").join(STORAGE_FORMAT_FILE),
            visible_object_metadata("folder/nested", "22222222-2222-2222-2222-222222222222"),
        )
        .await
        .expect("real child metadata should be written");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let (reader, mut writer) = tokio::io::duplex(65536);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "".to_string(),
            recursive: false,
            limit: 1000,
            ..Default::default()
        };
        let mut objs_returned = 0;

        disk.scan_dir("".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await
            .expect("scan_dir should succeed");
        out.close().await.expect("metacache writer should close");
        drop(out);
        drop(writer);

        let mut reader = MetacacheReader::new(reader);
        let entries = reader.read_all().await.expect("scan output should decode");

        let marker_objects = entries
            .iter()
            .filter(|entry| entry.name == "folder/" && !entry.metadata.is_empty())
            .count();
        let prefix_dirs = entries
            .iter()
            .filter(|entry| entry.name == "folder/" && entry.metadata.is_empty())
            .count();

        assert_eq!(marker_objects, 1, "dir marker object should be reported exactly once");
        assert_eq!(prefix_dirs, 1, "prefix dir should be reported exactly once");
        // No other entries may leak from the marker (e.g. a malformed `folder//`
        // from probing the marker's encoded directory).
        assert_eq!(entries.len(), 2, "scan must emit exactly the marker object and the prefix dir");
    }

    // The prefix synthesized for an object with children is dropped when the
    // page limit is hit on the object itself, and must be re-derived on the
    // forward_to resume of the next page.
    #[tokio::test]
    async fn test_scan_dir_limit_boundary_resumes_synthesized_prefix() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        fn visible_object_metadata(name: &str, version_id: &str) -> Vec<u8> {
            let mut fm = FileMeta::default();
            let mut fi = FileInfo::new(name, 1, 1);
            fi.version_id = Some(Uuid::parse_str(version_id).expect("test version id should parse"));
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).expect("object metadata should be valid");
            fm.marshal_msg().expect("visible metadata should encode")
        }

        async fn scan_names(disk: &LocalDisk, bucket: &str, limit: i32, forward_to: Option<String>) -> Vec<String> {
            let (reader, mut writer) = tokio::io::duplex(65536);
            let mut out = MetacacheWriter::new(&mut writer);
            let opts = WalkDirOptions {
                bucket: bucket.to_string(),
                base_dir: "".to_string(),
                recursive: false,
                limit,
                forward_to,
                ..Default::default()
            };
            let mut objs_returned = 0;

            disk.scan_dir("".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
                .await
                .expect("scan_dir should succeed");
            out.close().await.expect("metacache writer should close");
            drop(out);
            drop(writer);

            let mut reader = MetacacheReader::new(reader);
            reader
                .read_all()
                .await
                .expect("scan output should decode")
                .into_iter()
                .map(|entry| entry.name)
                .collect()
        }

        let dir = tempdir().expect("tempdir should be created");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);

        fs::create_dir_all(bucket_dir.join("a/b"))
            .await
            .expect("object dirs should be created");
        fs::write(
            bucket_dir.join("a").join(STORAGE_FORMAT_FILE),
            visible_object_metadata("a", "11111111-1111-1111-1111-111111111111"),
        )
        .await
        .expect("object metadata should be written");
        fs::write(
            bucket_dir.join("a/b").join(STORAGE_FORMAT_FILE),
            visible_object_metadata("a/b", "22222222-2222-2222-2222-222222222222"),
        )
        .await
        .expect("child metadata should be written");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        // Page 1: `a` consumes the whole limit, the pending `a/` is dropped.
        let first_page = scan_names(&disk, bucket, 1, None).await;
        assert_eq!(first_page, vec!["a".to_string()]);

        // Page 2: resuming at `a/` re-derives the prefix from the object entry.
        let resumed = scan_names(&disk, bucket, 1000, Some("a/".to_string())).await;
        assert!(
            resumed.contains(&"a/".to_string()),
            "resumed scan must re-derive the synthesized prefix, got {resumed:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_scan_dir_propagates_metadata_read_errors() {
        use std::fs::Permissions;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let bucket = "test-bucket";
        let bucket_dir = dir.path().join(bucket);
        let object_dir = bucket_dir.join("broken");
        let meta_path = object_dir.join(STORAGE_FORMAT_FILE);

        fs::create_dir_all(&object_dir).await.expect("operation should succeed");
        fs::write(&meta_path, b"meta").await.expect("operation should succeed");

        let original_permissions = fs::metadata(&meta_path)
            .await
            .expect("operation should succeed")
            .permissions();
        fs::set_permissions(&meta_path, Permissions::from_mode(0o000))
            .await
            .expect("operation should succeed");
        if File::open(&meta_path).await.is_ok() {
            fs::set_permissions(&meta_path, original_permissions)
                .await
                .expect("operation should succeed");
            return;
        }

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (_reader, mut writer) = tokio::io::duplex(4096);
        let mut out = MetacacheWriter::new(&mut writer);
        let opts = WalkDirOptions {
            bucket: bucket.to_string(),
            base_dir: "".to_string(),
            recursive: true,
            ..Default::default()
        };
        let mut objs_returned = 0;

        let result = disk
            .scan_dir("".to_string(), "".to_string(), &opts, &mut out, &mut objs_returned, false, None)
            .await;

        fs::set_permissions(&meta_path, original_permissions)
            .await
            .expect("operation should succeed");

        assert!(matches!(result, Err(DiskError::FileAccessDenied)));
    }

    #[tokio::test]
    async fn test_walk_dir_ignore_multipart_dirs() {
        use rustfs_filemeta::MetacacheReader;
        use tempfile::tempdir;

        const UUID_MULTIPART_1: &str = "8b262d24-fcf9-473d-a4cd-f9b27f24f60e";
        const UUID_MULTIPART_2: &str = "fbf3183c-63be-45cc-b3bf-424ddb7f95f8";
        const UUID_OBJ: &str = "db8b9b74-9016-4f9e-83e9-82a772947d28";
        const VER_ID_1: &str = "c683f9f8-c0a1-4bc5-8a67-0faafa839a1a";
        const VER_ID_2: &str = "a4b84f6e-c8ba-461b-8f9d-43feb0893efb";
        const VER_ID_3: &str = "892c9ae7-2bb3-44ee-9a71-bc7ddf08d765";
        const BASE_DIR: &str = "dir1/obj/";
        const MULTIPART_DIR: &str = "multipart-file";
        const DIR_IN_MULTIPART_DIR: &str = "dir-in-multipart";
        const EMPTY_STR: &str = "";

        let parse_uuid = |s: &str| Uuid::parse_str(s).expect("operation should succeed");
        let create_file_info = |version_id: &str, data_dir: &str| FileInfo {
            version_id: Some(parse_uuid(version_id)),
            data_dir: Some(parse_uuid(data_dir)),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        let dir = tempdir().expect("operation should succeed");
        let obj_base = dir.path().join("test-bucket").join(BASE_DIR);
        let multipart_base = obj_base.join(MULTIPART_DIR);
        let dir_in_multipart_base = multipart_base.join(DIR_IN_MULTIPART_DIR);

        fs::create_dir_all(&multipart_base).await.expect("operation should succeed");
        for uuid in &[UUID_MULTIPART_1, UUID_MULTIPART_2] {
            fs::create_dir_all(multipart_base.join(uuid))
                .await
                .expect("operation should succeed");
            fs::write(multipart_base.join(uuid).join("part.1"), b"part")
                .await
                .expect("operation should succeed");
        }
        fs::create_dir_all(obj_base.join(UUID_OBJ))
            .await
            .expect("operation should succeed");
        fs::write(obj_base.join(UUID_OBJ).join("part.1"), b"part")
            .await
            .expect("operation should succeed");

        fs::create_dir_all(&dir_in_multipart_base)
            .await
            .expect("operation should succeed");
        fs::write(dir_in_multipart_base.join(STORAGE_FORMAT_FILE), b"meta")
            .await
            .expect("operation should succeed");

        let mut fm = FileMeta::default();
        fm.add_version(create_file_info(VER_ID_1, UUID_MULTIPART_1))
            .expect("operation should succeed");
        fm.add_version(create_file_info(VER_ID_2, UUID_MULTIPART_2))
            .expect("operation should succeed");
        fs::write(
            multipart_base.join(STORAGE_FORMAT_FILE),
            fm.marshal_msg().expect("operation should succeed"),
        )
        .await
        .expect("operation should succeed");

        let mut fm = FileMeta::default();
        fm.add_version(create_file_info(VER_ID_3, UUID_OBJ))
            .expect("operation should succeed");
        fs::write(obj_base.join(STORAGE_FORMAT_FILE), fm.marshal_msg().expect("operation should succeed"))
            .await
            .expect("operation should succeed");

        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let (reader, mut writer) = tokio::io::duplex(4096);
        disk.walk_dir(
            WalkDirOptions {
                bucket: "test-bucket".to_string(),
                base_dir: BASE_DIR.to_string(),
                recursive: true,
                filter_prefix: Some(EMPTY_STR.to_string()),
                ..Default::default()
            },
            &mut writer,
        )
        .await
        .expect("operation should succeed");
        MetacacheWriter::new(&mut writer)
            .close()
            .await
            .expect("operation should succeed");

        let mut reader = MetacacheReader::new(reader);
        let entries = reader.read_all().await.expect("operation should succeed");
        let names: Vec<String> = entries.into_iter().map(|entry| entry.name).collect();

        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}", BASE_DIR, MULTIPART_DIR))
                .count(),
            1
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/", BASE_DIR, MULTIPART_DIR))
                .count(),
            1
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}", BASE_DIR, MULTIPART_DIR, DIR_IN_MULTIPART_DIR))
                .count(),
            1
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}/", BASE_DIR, MULTIPART_DIR, DIR_IN_MULTIPART_DIR))
                .count(),
            1
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}", BASE_DIR, MULTIPART_DIR, UUID_MULTIPART_1))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}", BASE_DIR, MULTIPART_DIR, UUID_MULTIPART_2))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}", BASE_DIR, UUID_OBJ))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}/", BASE_DIR, MULTIPART_DIR, UUID_MULTIPART_1))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/{}/", BASE_DIR, MULTIPART_DIR, UUID_MULTIPART_2))
                .count(),
            0
        );
        assert_eq!(
            names
                .iter()
                .filter(|name| *name == &format!("{}{}/", BASE_DIR, UUID_OBJ))
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn test_make_volume() {
        let p = "./testv0";
        fs::create_dir_all(&p).await.expect("operation should succeed");

        let ep = Endpoint::try_from(p).expect("endpoint should parse");

        let disk = LocalDisk::new(&ep, false).await.expect("operation should succeed");

        let tmpp = disk
            .resolve_abs_path(Path::new(RUSTFS_META_TMP_DELETED_BUCKET))
            .expect("operation should succeed");

        println!("ppp :{:?}", tmpp);

        let volumes = vec!["a123", "b123", "c123"];

        disk.make_volumes(volumes.clone()).await.expect("operation should succeed");

        disk.make_volumes(volumes.clone()).await.expect("operation should succeed");

        let _ = fs::remove_dir_all(&p).await;
    }

    #[tokio::test]
    async fn test_delete_volume() {
        let p = "./testv1";
        fs::create_dir_all(&p).await.expect("operation should succeed");

        let ep = Endpoint::try_from(p).expect("endpoint should parse");

        let disk = LocalDisk::new(&ep, false).await.expect("operation should succeed");

        let tmpp = disk
            .resolve_abs_path(Path::new(RUSTFS_META_TMP_DELETED_BUCKET))
            .expect("operation should succeed");

        println!("ppp :{:?}", tmpp);

        let volumes = vec!["a123", "b123", "c123"];

        disk.make_volumes(volumes.clone()).await.expect("operation should succeed");

        disk.delete_volume("a", true).await.expect("operation should succeed");

        let _ = fs::remove_dir_all(&p).await;
    }

    #[tokio::test]
    async fn test_local_disk_basic_operations() {
        let test_dir = "./test_local_disk_basic";
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");

        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        // Test basic properties
        assert!(disk.is_local());
        // Note: host_name() for local disks might be empty or contain localhost/hostname
        // assert!(!disk.host_name().is_empty());
        assert!(!disk.to_string().is_empty());

        // Test path resolution
        let abs_path = disk.resolve_abs_path("test/path").expect("operation should succeed");
        assert!(abs_path.is_absolute());

        // Test bucket path
        let bucket_path = disk.get_bucket_path("test-bucket").expect("operation should succeed");
        assert!(bucket_path.to_string_lossy().contains("test-bucket"));

        // Test object path
        let object_path = disk
            .get_object_path("test-bucket", "test-object")
            .expect("operation should succeed");
        assert!(object_path.to_string_lossy().contains("test-bucket"));
        assert!(object_path.to_string_lossy().contains("test-object"));

        // Clean up the test directory
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_get_bucket_path_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        let outside_dir = tempdir().expect("operation should succeed");
        let link_path = root_dir.path().join("escape-bucket");
        symlink(outside_dir.path(), &link_path).expect("operation should succeed");

        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        assert!(matches!(disk.get_bucket_path("escape-bucket"), Err(DiskError::InvalidPath)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn get_bucket_path_for_io_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let outside_dir = tempfile::tempdir().expect("outside temp dir should be created");
        let link_path = root_dir.path().join("escape-bucket");
        symlink(outside_dir.path(), &link_path).expect("bucket symlink should be created");

        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        assert!(matches!(disk.get_bucket_path_for_io("escape-bucket"), Err(DiskError::InvalidPath)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_get_object_path_rejects_symlink_component_escape() {
        use std::os::unix::fs::symlink;
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        let outside_dir = tempdir().expect("operation should succeed");
        let bucket_dir = root_dir.path().join("bucket");
        fs::create_dir_all(&bucket_dir).await.expect("operation should succeed");
        let link_path = bucket_dir.join("escape");
        symlink(outside_dir.path(), &link_path).expect("operation should succeed");

        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        assert!(matches!(disk.get_object_path("bucket", "escape/object.txt"), Err(DiskError::InvalidPath)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn get_object_path_for_io_rejects_symlink_leaf() {
        use std::os::unix::fs::symlink;

        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let outside_file = root_dir.path().join("outside-file");
        fs::write(&outside_file, b"outside")
            .await
            .expect("outside file should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        disk.make_volume("bucket").await.expect("bucket should be created");
        symlink(&outside_file, root_dir.path().join("bucket/object")).expect("object symlink should be created");

        assert!(matches!(disk.get_object_path_for_io("bucket", "object"), Err(DiskError::InvalidPath)));
    }

    #[tokio::test]
    async fn get_object_path_rejects_key_traversal_out_of_bucket() {
        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        assert!(matches!(disk.get_object_path("bucket", "../outside"), Err(DiskError::InvalidPath)));
        assert!(matches!(
            disk.get_object_path("bucket", "prefix/../../outside"),
            Err(DiskError::InvalidPath)
        ));
    }

    #[tokio::test]
    async fn get_object_path_accepts_missing_leaf_under_existing_bucket() {
        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        disk.make_volume("bucket").await.expect("bucket should be created");

        let object_path = disk
            .get_object_path("bucket", "missing-object")
            .expect("missing leaf under a valid bucket should resolve");

        assert_eq!(object_path, disk.root.join("bucket/missing-object"));
    }

    #[tokio::test]
    async fn get_object_path_for_io_rejects_key_traversal_out_of_bucket() {
        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        assert!(matches!(disk.get_object_path_for_io("bucket", "../outside"), Err(DiskError::InvalidPath)));
        assert!(matches!(
            disk.get_object_path_for_io("bucket", "prefix/../../outside"),
            Err(DiskError::InvalidPath)
        ));
    }

    #[tokio::test]
    async fn get_object_path_for_io_accepts_missing_leaf_under_existing_bucket() {
        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        disk.make_volume("bucket").await.expect("bucket should be created");

        let object_path = disk
            .get_object_path_for_io("bucket", "missing-object")
            .expect("missing leaf under a valid I/O bucket should resolve");

        assert!(object_path.ends_with("bucket/missing-object"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn get_object_path_rejects_symlink_component_after_prior_valid_lookup() {
        use std::os::unix::fs::symlink;

        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let outside_dir = tempfile::tempdir().expect("outside temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let prefix = root_dir.path().join("bucket/prefix");
        fs::create_dir_all(&prefix).await.expect("prefix should be created");

        disk.get_object_path("bucket", "prefix/object")
            .expect("initial lookup should validate the real prefix");
        fs::remove_dir(&prefix).await.expect("prefix should be removable");
        symlink(outside_dir.path(), &prefix).expect("prefix should be replaced by a symlink");

        assert!(matches!(disk.get_object_path("bucket", "prefix/object"), Err(DiskError::InvalidPath)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn get_object_path_for_io_rejects_symlink_component_after_prior_valid_lookup() {
        use std::os::unix::fs::symlink;

        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let outside_dir = tempfile::tempdir().expect("outside temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let prefix = root_dir.path().join("bucket/prefix");
        fs::create_dir_all(&prefix).await.expect("prefix should be created");

        disk.get_object_path_for_io("bucket", "prefix/object")
            .expect("initial I/O lookup should validate the real prefix");
        fs::remove_dir(&prefix).await.expect("prefix should be removable");
        symlink(outside_dir.path(), &prefix).expect("prefix should be replaced by a symlink");

        assert!(matches!(
            disk.get_object_path_for_io("bucket", "prefix/object"),
            Err(DiskError::InvalidPath)
        ));
    }

    #[tokio::test]
    async fn get_object_path_accepts_parent_recreated_after_prior_valid_lookup() {
        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let prefix = root_dir.path().join("bucket/prefix");
        fs::create_dir_all(&prefix).await.expect("prefix should be created");

        disk.get_object_path("bucket", "prefix/object")
            .expect("initial lookup should validate the real prefix");
        fs::remove_dir(&prefix).await.expect("prefix should be removable");
        fs::create_dir(&prefix).await.expect("prefix should be recreated");

        let object_path = disk
            .get_object_path("bucket", "prefix/object")
            .expect("recreated non-symlink parent should validate");
        assert_eq!(object_path, disk.root.join("bucket/prefix/object"));
    }

    #[tokio::test]
    async fn get_object_path_for_io_accepts_parent_recreated_after_prior_valid_lookup() {
        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let prefix = root_dir.path().join("bucket/prefix");
        fs::create_dir_all(&prefix).await.expect("prefix should be created");

        disk.get_object_path_for_io("bucket", "prefix/object")
            .expect("initial I/O lookup should validate the real prefix");
        fs::remove_dir(&prefix).await.expect("prefix should be removable");
        fs::create_dir(&prefix).await.expect("prefix should be recreated");

        let object_path = disk
            .get_object_path_for_io("bucket", "prefix/object")
            .expect("recreated non-symlink parent should validate for I/O");
        assert!(object_path.ends_with("bucket/prefix/object"));
    }

    #[tokio::test]
    async fn get_object_path_handles_many_unique_missing_prefixes_without_state_growth() {
        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        disk.make_volume("bucket").await.expect("bucket should be created");

        for index in 0..5000 {
            let object_path = disk
                .get_object_path("bucket", &format!("prefix-{index}/object"))
                .expect("unique missing prefix should validate without shared state");
            assert!(object_path.ends_with(format!("bucket/prefix-{index}/object")));
        }
    }

    #[tokio::test]
    async fn get_object_path_concurrent_validation_keeps_paths_under_bucket() {
        let root_dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        disk.make_volume("bucket").await.expect("bucket should be created");
        let barrier = Arc::new(tokio::sync::Barrier::new(32));
        let mut tasks = Vec::with_capacity(32);

        for index in 0..32 {
            let disk = disk.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                disk.get_object_path("bucket", &format!("object-{index}"))
                    .expect("concurrent validation should resolve object path")
            }));
        }

        for task in tasks {
            let object_path = task.await.expect("validation task should complete");
            assert!(object_path.starts_with(disk.root.join("bucket")));
        }
    }

    #[tokio::test]
    async fn test_local_disk_file_operations() {
        let test_dir = "./test_local_disk_file_ops";
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");

        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        // Create test volume
        disk.make_volume("test-volume").await.expect("operation should succeed");

        // Test write and read operations
        let test_data: Vec<u8> = vec![1, 2, 3, 4, 5];
        disk.write_all("test-volume", "test-file.txt", test_data.clone().into())
            .await
            .expect("operation should succeed");

        let read_data = disk
            .read_all("test-volume", "test-file.txt")
            .await
            .expect("operation should succeed");
        assert_eq!(read_data, test_data);

        // Test file deletion
        let delete_opts = DeleteOptions {
            recursive: false,
            immediate: true,
            undo_write: false,
            undo_delete: false,
            old_data_dir: None,
        };
        disk.delete("test-volume", "test-file.txt", delete_opts)
            .await
            .expect("operation should succeed");

        // Clean up
        disk.delete_volume("test-volume", true)
            .await
            .expect("operation should succeed");
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn delete_volume_non_force_refuses_non_empty_bucket() {
        // backlog#799 B1: a non-force delete_volume must refuse a bucket that
        // still holds object data (VolumeNotEmpty) and leave it intact, so a
        // misclassified "dangling" bucket cannot be recursively wiped. Only an
        // explicit force delete removes it recursively.
        let test_dir = "./test_b1_delete_volume_guard";
        let _ = fs::remove_dir_all(&test_dir).await;
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");
        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        disk.make_volume("b1-bucket").await.expect("operation should succeed");
        let data: Vec<u8> = vec![1, 2, 3];
        disk.write_all("b1-bucket", "obj.dat", data.clone().into())
            .await
            .expect("operation should succeed");

        // Non-force must refuse and preserve the data.
        let err = disk
            .delete_volume("b1-bucket", false)
            .await
            .expect_err("non-empty bucket must be refused");
        assert!(matches!(err, DiskError::VolumeNotEmpty), "expected VolumeNotEmpty, got {err:?}");
        assert!(
            disk.stat_volume("b1-bucket").await.is_ok(),
            "bucket must still exist after a refused non-force delete"
        );
        assert_eq!(disk.read_all("b1-bucket", "obj.dat").await.expect("data preserved"), data);

        // Force removes it recursively.
        disk.delete_volume("b1-bucket", true)
            .await
            .expect("force delete removes non-empty");
        assert!(disk.stat_volume("b1-bucket").await.is_err(), "bucket must be gone after force delete");

        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn delete_volume_non_force_removes_nested_empty_directories() {
        let root = tempfile::tempdir().expect("temporary disk root should be created");
        let endpoint = Endpoint::try_from(root.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "nested-empty-bucket";

        disk.make_volume(bucket).await.expect("bucket should be created");
        fs::create_dir_all(disk.path().join(bucket).join("a/b/c"))
            .await
            .expect("nested empty directories should be created");

        disk.delete_volume(bucket, false)
            .await
            .expect("non-force delete should remove an empty directory tree");

        assert!(matches!(disk.stat_volume(bucket).await, Err(DiskError::VolumeNotFound)));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn delete_volume_non_force_removes_empty_bucket_under_mount_lease_root() {
        let root = tempfile::tempdir().expect("temporary disk root should be created");
        let endpoint = Endpoint::try_from(root.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let bucket = "mount-lease-empty-delete";

        disk.make_volume(bucket).await.expect("bucket should be created");
        fs::create_dir_all(root.path().join(bucket).join("deleted/object/path"))
            .await
            .expect("empty object remnant should be created");

        disk.delete_volume(bucket, false)
            .await
            .expect("non-force delete should remove empty remnants through the held mount lease");

        assert!(!root.path().join(bucket).exists(), "empty bucket should be removed");
    }

    #[tokio::test]
    async fn empty_tree_delete_preserves_xlmeta_published_after_scan() {
        let root = tempfile::tempdir().expect("temporary disk root should be created");
        let bucket_path = root.path().join("bucket");
        let object_path = bucket_path.join("object").join(STORAGE_FORMAT_FILE);
        fs::create_dir_all(object_path.parent().expect("object path should have a parent"))
            .await
            .expect("empty object directory should be created");

        let data = b"committed object metadata";
        let mut published = false;
        let err = remove_empty_directory_tree_with(
            &bucket_path,
            |_| Ok(()),
            |directory| {
                if !published && directory == object_path.parent().expect("object path should have a parent") {
                    std::fs::write(&object_path, data)?;
                    published = true;
                }
                Ok(())
            },
        )
        .await
        .expect_err("rmdir should refuse metadata published after the directory scan");

        assert!(published, "test barrier should publish metadata before rmdir");
        assert!(matches!(classify_delete_volume_error(err), DiskError::VolumeNotEmpty));
        assert_eq!(std::fs::read(&object_path).expect("object metadata should be preserved"), data);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn empty_tree_delete_rejects_child_replaced_with_external_symlink() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("temporary root should be created");
        let bucket_path = root.path().join("bucket");
        let child_path = bucket_path.join("child");
        let outside_path = root.path().join("outside");
        let outside_empty = outside_path.join("must-remain");
        fs::create_dir_all(&child_path).await.expect("bucket child should be created");
        fs::create_dir_all(&outside_empty)
            .await
            .expect("outside directory should be created");

        let mut replaced = false;
        let err = remove_empty_directory_tree_with(
            &bucket_path,
            |child| {
                if !replaced && child == child_path {
                    std::fs::remove_dir(&child_path)?;
                    symlink(&outside_path, &child_path)?;
                    replaced = true;
                }
                Ok(())
            },
            |_| Ok(()),
        )
        .await
        .expect_err("a replaced child must fail closed");

        assert!(replaced, "test barrier should replace the child before it is opened");
        assert!(matches!(classify_delete_volume_error(err), DiskError::VolumeNotEmpty));
        assert!(outside_empty.exists(), "bucket deletion must not remove directories outside the bucket");
        assert!(
            std::fs::symlink_metadata(&child_path)
                .expect("replacement symlink should remain")
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn empty_tree_delete_rechecks_parent_after_child_disappears() {
        let root = tempfile::tempdir().expect("temporary root should be created");
        let bucket_path = root.path().join("bucket");
        let child_path = bucket_path.join("child");
        fs::create_dir_all(&child_path).await.expect("bucket child should be created");

        let mut removed = false;
        remove_empty_directory_tree_with(
            &bucket_path,
            |child| {
                if !removed && child == child_path {
                    std::fs::remove_dir(&child_path)?;
                    removed = true;
                }
                Ok(())
            },
            |_| Ok(()),
        )
        .await
        .expect("a vanished empty child should not leave the bucket root behind");

        assert!(removed, "test hook should remove the child before openat");
        assert!(!bucket_path.exists(), "parent should be rechecked and removed after the child disappears");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn empty_tree_delete_rejects_root_generation_replacement() {
        let root = tempfile::tempdir().expect("temporary root should be created");
        let bucket_path = root.path().join("bucket");
        let moved_path = root.path().join("bucket-before-replacement");
        fs::create_dir(&bucket_path).await.expect("bucket should be created");

        let mut replaced = false;
        let err = remove_empty_directory_tree_with(
            &bucket_path,
            |_| Ok(()),
            |directory| {
                if !replaced && directory == bucket_path {
                    std::fs::rename(&bucket_path, &moved_path)?;
                    std::fs::create_dir(&bucket_path)?;
                    replaced = true;
                }
                Ok(())
            },
        )
        .await
        .expect_err("a replacement root directory must fail closed");

        assert!(replaced, "test hook should replace the root generation before rmdir");
        assert!(matches!(classify_delete_volume_error(err), DiskError::VolumeNotEmpty));
        assert!(moved_path.exists(), "the originally scanned root should not be removed by its old name");
        assert!(bucket_path.exists(), "the replacement root should remain after identity validation fails");
    }

    #[tokio::test]
    async fn test_local_disk_volume_operations() {
        let test_dir = "./test_local_disk_volumes";
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");

        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        // Test creating multiple volumes
        let volumes = vec!["vol1", "vol2", "vol3"];
        disk.make_volumes(volumes.clone()).await.expect("operation should succeed");

        // Test listing volumes
        let volume_list = disk.list_volumes().await.expect("operation should succeed");
        assert!(!volume_list.is_empty());

        // Test volume stats
        for vol in &volumes {
            let vol_info = disk.stat_volume(vol).await.expect("operation should succeed");
            assert_eq!(vol_info.name, *vol);
        }

        // Test deleting volumes
        for vol in &volumes {
            disk.delete_volume(vol, true).await.expect("operation should succeed");
        }

        // Clean up the test directory
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn test_local_disk_disk_info() {
        let test_dir = "./test_local_disk_info";
        fs::create_dir_all(&test_dir).await.expect("operation should succeed");

        let endpoint = Endpoint::try_from(test_dir).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        let disk_info_opts = DiskInfoOptions {
            disk_id: "test-disk".to_string(),
            metrics: true,
            noop: false,
        };

        let disk_info = disk.disk_info(&disk_info_opts).await.expect("operation should succeed");

        // Basic checks on disk info
        // Note: On macOS, Windows, and some other systems, fs_type may be empty
        // because statvfs does not provide filesystem type information.
        // This is a platform limitation, not a bug.
        #[cfg(not(any(target_os = "macos", windows)))]
        assert!(!disk_info.fs_type.is_empty(), "fs_type should not be empty on this platform");
        assert!(disk_info.total > 0);
        assert!(disk_info.free <= disk_info.total);
        assert_eq!(disk_info.nr_requests, disk.nrrequests);
        assert_eq!(disk_info.rotational, disk.rotational);
        assert!(!disk_info.mount_path.is_empty());
        assert!(!disk_info.endpoint.is_empty());

        // Clean up the test directory
        let _ = fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn test_read_file_stream_rejects_offset_length_overflow() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "test-file.txt", Bytes::from_static(b"test"))
            .await
            .expect("operation should succeed");

        let result = disk.read_file_stream("test-volume", "test-file.txt", usize::MAX, 1).await;
        assert!(matches!(result, Err(DiskError::FileCorrupt)));
    }

    #[tokio::test]
    async fn test_read_file_mmap_copy_rejects_offset_length_overflow() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "test-file.txt", Bytes::from_static(b"test"))
            .await
            .expect("operation should succeed");

        let result = disk.read_file_mmap_copy("test-volume", "test-file.txt", usize::MAX, 1).await;
        assert!(matches!(result, Err(DiskError::FileCorrupt)));
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_read_file_zero_copy_legacy_alias_rejects_offset_length_overflow() {
        use tempfile::tempdir;

        let dir = tempdir().expect("operation should succeed");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("operation should succeed")).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");

        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "test-file.txt", Bytes::from_static(b"test"))
            .await
            .expect("operation should succeed");

        let result = disk.read_file_zero_copy("test-volume", "test-file.txt", usize::MAX, 1).await;
        assert!(matches!(result, Err(DiskError::FileCorrupt)));
    }

    #[test]
    fn test_is_valid_volname() {
        // Valid volume names (length >= 3)
        assert!(LocalDisk::is_valid_volname("valid-name"));
        assert!(LocalDisk::is_valid_volname("test123"));
        assert!(LocalDisk::is_valid_volname("my-bucket"));

        // Test minimum length requirement
        assert!(!LocalDisk::is_valid_volname(""));
        assert!(!LocalDisk::is_valid_volname("a"));
        assert!(!LocalDisk::is_valid_volname("ab"));
        assert!(LocalDisk::is_valid_volname("abc"));

        // Note: The current implementation doesn't check for system volume names
        // It only checks length and platform-specific special characters
        // System volume names are valid according to the current implementation
        assert!(LocalDisk::is_valid_volname(RUSTFS_META_BUCKET));
        assert!(LocalDisk::is_valid_volname(RUSTFS_META_TMP_BUCKET));

        // Testing platform-specific behavior for special characters
        #[cfg(windows)]
        {
            // On Windows systems, these should be invalid
            assert!(!LocalDisk::is_valid_volname("invalid\\name"));
            assert!(!LocalDisk::is_valid_volname("invalid:name"));
            assert!(!LocalDisk::is_valid_volname("invalid|name"));
            assert!(!LocalDisk::is_valid_volname("invalid<name"));
            assert!(!LocalDisk::is_valid_volname("invalid>name"));
            assert!(!LocalDisk::is_valid_volname("invalid?name"));
            assert!(!LocalDisk::is_valid_volname("invalid*name"));
            assert!(!LocalDisk::is_valid_volname("invalid\"name"));
        }

        #[cfg(not(windows))]
        {
            // On non-Windows systems, the current implementation doesn't check special characters
            // So these would be considered valid
            assert!(LocalDisk::is_valid_volname("valid/name"));
            assert!(LocalDisk::is_valid_volname("valid:name"));
        }
    }

    #[tokio::test]
    async fn test_read_file_exists() {
        let test_file = "./test_read_exists.txt";

        // Test non-existent file
        let (data, metadata) = read_file_exists(test_file).await.expect("operation should succeed");
        assert!(data.is_empty());
        assert!(metadata.is_none());

        // Create test file
        fs::write(test_file, b"test content").await.expect("operation should succeed");

        // Test existing file
        let (data, metadata) = read_file_exists(test_file).await.expect("operation should succeed");
        assert_eq!(data.as_ref(), b"test content");
        assert!(metadata.is_some());

        // Clean up
        let _ = fs::remove_file(test_file).await;
    }

    #[tokio::test]
    async fn test_read_file_all() {
        let test_file = "./test_read_all.txt";
        let test_content = b"test content for read_all";

        // Create test file
        fs::write(test_file, test_content).await.expect("operation should succeed");

        // Test reading file
        let (data, metadata) = read_file_all(test_file).await.expect("operation should succeed");
        assert_eq!(data.as_ref(), test_content);
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), test_content.len() as u64);

        // Clean up
        let _ = fs::remove_file(test_file).await;
    }

    #[tokio::test]
    async fn test_read_file_metadata() {
        let test_file = "./test_metadata.txt";

        // Create test file
        fs::write(test_file, b"test").await.expect("operation should succeed");

        // Test reading metadata
        let metadata = read_file_metadata(test_file).await.expect("operation should succeed");
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 4); // "test" is 4 bytes

        // Clean up
        let _ = fs::remove_file(test_file).await;
    }

    #[test]
    fn test_is_root_path() {
        // Unix root path
        assert!(is_root_path("/"));

        // Windows root path (only on Windows)
        #[cfg(windows)]
        assert!(is_root_path("\\"));

        // Non-root paths
        assert!(!is_root_path("/home"));
        assert!(!is_root_path("/tmp"));
        assert!(!is_root_path("relative/path"));

        // On non-Windows systems, backslash is not a root path
        #[cfg(not(windows))]
        assert!(!is_root_path("\\"));
    }

    #[test]
    fn test_normalize_path_components() {
        // Test basic relative path
        assert_eq!(normalize_path_components("a/b/c"), PathBuf::from("a/b/c"));

        // Test path with current directory components (should be ignored)
        assert_eq!(normalize_path_components("a/./b/./c"), PathBuf::from("a/b/c"));

        // Test path with parent directory components
        assert_eq!(normalize_path_components("a/b/../c"), PathBuf::from("a/c"));

        // Test path with multiple parent directory components
        assert_eq!(normalize_path_components("a/b/c/../../d"), PathBuf::from("a/d"));

        // Test path that goes beyond root
        assert_eq!(normalize_path_components("a/../../../b"), PathBuf::from("b"));

        // Test absolute path
        assert_eq!(normalize_path_components("/a/b/c"), PathBuf::from("/a/b/c"));

        // Test absolute path with parent components
        assert_eq!(normalize_path_components("/a/b/../c"), PathBuf::from("/a/c"));

        // Test complex path with mixed components
        assert_eq!(normalize_path_components("a/./b/../c/./d/../e"), PathBuf::from("a/c/e"));

        // Test path with only current directory
        assert_eq!(normalize_path_components("."), PathBuf::from(""));

        // Test path with only parent directory
        assert_eq!(normalize_path_components(".."), PathBuf::from(""));

        // Test path with multiple current directories
        assert_eq!(normalize_path_components("./././a"), PathBuf::from("a"));

        // Test path with multiple parent directories
        assert_eq!(normalize_path_components("../../a"), PathBuf::from("a"));

        // Test empty path
        assert_eq!(normalize_path_components(""), PathBuf::from(""));

        // Test path starting with current directory
        assert_eq!(normalize_path_components("./a/b"), PathBuf::from("a/b"));

        // Test path starting with parent directory
        assert_eq!(normalize_path_components("../a/b"), PathBuf::from("a/b"));

        // Test complex case with multiple levels of parent navigation
        assert_eq!(normalize_path_components("a/b/c/../../../d/e/f/../../g"), PathBuf::from("d/g"));

        // Test path that completely cancels out
        assert_eq!(normalize_path_components("a/b/../../../c/d/../../.."), PathBuf::from(""));

        // Test Windows-style paths (if applicable)
        #[cfg(windows)]
        {
            assert_eq!(normalize_path_components("C:\\a\\b\\c"), PathBuf::from("C:\\a\\b\\c"));

            assert_eq!(normalize_path_components("C:\\a\\..\\b"), PathBuf::from("C:\\b"));
        }
    }

    #[test]
    fn should_reclaim_file_cache_after_write_respects_env_and_threshold() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE, || {
            assert!(!should_reclaim_file_cache_after_write(8 * 1024 * 1024));
        });

        temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE, Some("true"), || {
            temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD, Some("4194304"), || {
                assert!(should_reclaim_file_cache_after_write(8 * 1024 * 1024));
                assert!(!should_reclaim_file_cache_after_write(1024));
            });
        });
    }

    #[test]
    fn should_reclaim_file_cache_after_read_respects_env_and_threshold() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE, || {
            temp_env::with_var_unset(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD, || {
                assert!(should_reclaim_file_cache_after_read(8 * 1024 * 1024));
                assert!(!should_reclaim_file_cache_after_read(1024));
            });
        });

        temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE, Some("false"), || {
            temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD, Some("4194304"), || {
                assert!(!should_reclaim_file_cache_after_read(8 * 1024 * 1024));
            });
        });

        temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE, Some("true"), || {
            temp_env::with_var(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD, Some("4194304"), || {
                assert!(should_reclaim_file_cache_after_read(8 * 1024 * 1024));
                assert!(!should_reclaim_file_cache_after_read(1024));
            });
        });
    }

    #[test]
    fn should_populate_mmap_read_respects_env() {
        temp_env::with_var_unset(ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE, || {
            assert!(!should_populate_mmap_read(512 * 1024));
        });

        temp_env::with_var(ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE, Some("true"), || {
            assert!(should_populate_mmap_read(512 * 1024));
            assert!(!should_populate_mmap_read(0));
        });

        temp_env::with_var(ENV_RUSTFS_OBJECT_MMAP_POPULATE_ENABLE, Some("false"), || {
            assert!(!should_populate_mmap_read(512 * 1024));
        });
    }

    #[test]
    fn local_read_copy_method_respects_env() {
        temp_env::with_var_unset(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, || {
            assert_eq!(local_read_copy_method(), LocalReadCopyMethod::MmapCopy);
        });

        temp_env::with_var(
            ENV_RUSTFS_OBJECT_MMAP_READ_METHOD,
            Some(RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY),
            || {
                assert_eq!(local_read_copy_method(), LocalReadCopyMethod::DirectReadCopy);
            },
        );

        temp_env::with_var(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, Some("unknown"), || {
            assert_eq!(local_read_copy_method(), LocalReadCopyMethod::MmapCopy);
        });
    }

    #[test]
    fn direct_io_drive_sync_and_bitrot_retry_envs_respect_overrides() {
        temp_env::with_var_unset(ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE, || {
            assert!(!is_direct_io_read_enabled());
        });
        temp_env::with_var(ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE, Some("true"), || {
            assert!(is_direct_io_read_enabled());
        });

        temp_env::with_var_unset(ENV_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD, || {
            assert_eq!(get_direct_io_read_threshold(), DEFAULT_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD);
        });
        temp_env::with_var(ENV_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD, Some("12345"), || {
            assert_eq!(get_direct_io_read_threshold(), 12_345);
        });

        temp_env::with_var_unset(ENV_RUSTFS_DRIVE_SYNC_ENABLE, || {
            assert_eq!(
                resolve_durability_mode(
                    None,
                    rustfs_utils::get_env_bool(ENV_RUSTFS_DRIVE_SYNC_ENABLE, DEFAULT_RUSTFS_DRIVE_SYNC_ENABLE),
                ),
                DurabilityMode::Strict
            );
        });
        temp_env::with_var(ENV_RUSTFS_DRIVE_SYNC_ENABLE, Some("false"), || {
            assert_eq!(
                resolve_durability_mode(
                    None,
                    rustfs_utils::get_env_bool(ENV_RUSTFS_DRIVE_SYNC_ENABLE, DEFAULT_RUSTFS_DRIVE_SYNC_ENABLE),
                ),
                DurabilityMode::LegacyOff
            );
        });

        temp_env::with_var_unset(ENV_BITROT_SIZE_MISMATCH_RETRY_COUNT, || {
            assert_eq!(bitrot_size_mismatch_retry_count(), DEFAULT_BITROT_SIZE_MISMATCH_RETRY_COUNT as usize);
        });
        temp_env::with_var(ENV_BITROT_SIZE_MISMATCH_RETRY_COUNT, Some("7"), || {
            assert_eq!(bitrot_size_mismatch_retry_count(), 7);
        });
        temp_env::with_var_unset(ENV_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS, || {
            assert_eq!(
                bitrot_size_mismatch_retry_delay(),
                Duration::from_millis(DEFAULT_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS)
            );
        });
        temp_env::with_var(ENV_BITROT_SIZE_MISMATCH_RETRY_DELAY_MS, Some("42"), || {
            assert_eq!(bitrot_size_mismatch_retry_delay(), Duration::from_millis(42));
        });
    }

    #[test]
    // Serialized because it flips the process-global GET stage-metrics gate,
    // which the decode.rs shard-locality tests also toggle under the same key.
    #[serial_test::serial]
    fn mmap_and_reclaim_metric_helpers_record_expected_counters_and_samples() {
        let metrics = || MmapCopyStageMetrics {
            path: "local_test",
            access_check_stage: "access",
            path_resolve_stage: "path",
            metadata_lookup_stage: "metadata_lookup",
            metadata_validate_stage: "metadata_validate",
            blocking_wait_stage: "blocking_wait",
            blocking_task_stage: "blocking_task",
            file_open_stage: "file_open",
            mmap_map_stage: "mmap_map",
            mmap_copy_stage: "mmap_copy",
            direct_read_copy_stage: "direct_read_copy",
        };

        let recorder = crate::test_metrics::CapturingRecorder::default();
        let previous_gate = rustfs_io_metrics::get_stage_metrics_enabled();
        let previous_metrics_gate = rustfs_io_metrics::metrics_enabled();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);
        rustfs_io_metrics::set_metrics_enabled(true);
        metrics::with_local_recorder(&recorder, || {
            record_mmap_copy_stage(metrics(), "mmap_copy", None);
            record_mmap_copy_stage(metrics(), "mmap_copy", Some(std::time::Instant::now()));
            record_file_cache_reclaim_success("read", 128, std::time::Instant::now());
            record_file_cache_reclaim_error("write");

            #[cfg(unix)]
            {
                record_mmap_page_fault_delta("local_test", "mmap_map", MmapPageFaultDelta::default());
                record_mmap_page_fault_delta("local_test", "mmap_map", MmapPageFaultDelta { minor: 1, major: 2 });
                record_direct_read_page_fault_delta("local_test", "direct_read_copy", MmapPageFaultDelta::default());
                record_direct_read_page_fault_delta("local_test", "direct_read_copy", MmapPageFaultDelta { minor: 3, major: 4 });
            }
        });
        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_gate);
        rustfs_io_metrics::set_metrics_enabled(previous_metrics_gate);

        assert_eq!(
            recorder.histogram_sample_count("rustfs_io_get_object_stage_duration_seconds"),
            1,
            "only the Some-timer stage call must record a duration sample"
        );
        assert_eq!(
            recorder.counter_value("rustfs_page_cache_reclaim_requests_total", &[("kind", "read"), ("result", "ok")]),
            1
        );
        assert_eq!(recorder.counter_value("rustfs_page_cache_reclaim_bytes_total", &[("kind", "read")]), 128);
        assert_eq!(recorder.histogram_sample_count("rustfs_page_cache_reclaim_duration_seconds"), 1);
        assert_eq!(
            recorder.counter_value("rustfs_page_cache_reclaim_requests_total", &[("kind", "write"), ("result", "err")]),
            1
        );

        #[cfg(unix)]
        {
            for (kind, expected) in [("minor", 1), ("major", 2)].iter().copied() {
                let labels = [("path", "local_test"), ("stage", "mmap_map"), ("kind", kind)];
                assert_eq!(
                    recorder.counter_value(METRIC_GET_OBJECT_MMAP_PAGE_FAULTS_TOTAL, &labels),
                    expected,
                    "zero deltas must not emit and positive mmap page fault deltas must accumulate exactly"
                );
            }
            for (kind, expected) in [("minor", 3), ("major", 4)].iter().copied() {
                let labels = [("path", "local_test"), ("stage", "direct_read_copy"), ("kind", kind)];
                assert_eq!(
                    recorder.counter_value(METRIC_GET_OBJECT_DIRECT_READ_PAGE_FAULTS_TOTAL, &labels),
                    expected,
                    "zero deltas must not emit and positive direct-read page fault deltas must accumulate exactly"
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn mmap_page_fault_counts_respect_disabled_and_enabled_modes() {
        assert_eq!(read_mmap_page_fault_counts(false), None);

        let counts = read_mmap_page_fault_counts(true).expect("getrusage should return page fault counters");
        assert!(counts.minor >= 0);
        assert!(counts.major >= 0);
    }

    #[cfg(unix)]
    #[test]
    fn mmap_page_size_is_cached_positive() {
        let first = mmap_page_size().expect("page size should be available");
        let second = mmap_page_size().expect("cached page size should be available");

        assert!(first > 0);
        assert_eq!(first, second);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_file_mmap_copy_supports_direct_read_copy_method() {
        use tempfile::tempdir;

        temp_env::async_with_vars(
            [(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, Some(RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY))],
            async {
                let root_dir = tempdir().expect("operation should succeed");
                let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
                let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
                disk.make_volume("test-volume").await.expect("operation should succeed");
                disk.write_all("test-volume", "test-file.txt", Bytes::from_static(b"0123456789abcdef"))
                    .await
                    .expect("operation should succeed");

                let data = disk
                    .read_file_mmap_copy("test-volume", "test-file.txt", 4, 6)
                    .await
                    .expect("operation should succeed");

                assert_eq!(data, Bytes::from_static(b"456789"));
            },
        )
        .await;
    }

    #[test]
    fn resolve_local_disk_root_reports_missing_path_as_volume_not_found() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let missing = dir.path().join("missing");

        let err = resolve_local_disk_root(missing.to_str().expect("temp path should be utf8"))
            .expect_err("missing disk root must be rejected");

        assert!(matches!(err, DiskError::VolumeNotFound));
    }

    #[tokio::test]
    async fn local_disk_debug_includes_stable_identity_fields() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        let debug = format!("{disk:?}");

        assert!(debug.contains("LocalDisk"));
        assert!(debug.contains("root"));
        assert!(debug.contains("format_path"));
        assert!(debug.contains("endpoint"));
    }

    #[tokio::test]
    async fn std_backend_truncate_append_stream_and_full_read_restore_bytes() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let volume = "test-volume";
        fs::create_dir_all(dir.path().join(volume))
            .await
            .expect("volume should be created");
        let backend = StdBackend::new(dir.path().to_path_buf());

        let mut writer = backend
            .open_write(volume, "nested/blob.bin", WriteMode::Truncate { size_hint: 6 })
            .await
            .expect("truncate writer should open");
        writer.write_all(b"abcdef").await.expect("initial bytes should write");
        writer.shutdown().await.expect("truncate writer should shutdown");

        let window = backend
            .pread_bytes(volume, "nested/blob.bin", 1, 3, None)
            .await
            .expect("pread should restore requested window");
        assert_eq!(window, Bytes::from_static(b"bcd"));

        let mut writer = backend
            .open_write(volume, "nested/blob.bin", WriteMode::Truncate { size_hint: 2 })
            .await
            .expect("second truncate writer should open");
        writer.write_all(b"xy").await.expect("truncated bytes should write");
        writer.shutdown().await.expect("second truncate writer should shutdown");

        let mut writer = backend
            .open_write(volume, "nested/blob.bin", WriteMode::Append)
            .await
            .expect("append writer should open");
        writer.write_all(b"z").await.expect("append byte should write");
        writer.shutdown().await.expect("append writer should shutdown");

        let mut stream = backend
            .open_read_stream(volume, "nested/blob.bin", 0, 3)
            .await
            .expect("bounded stream should open");
        let mut streamed = Vec::new();
        stream.read_to_end(&mut streamed).await.expect("bounded stream should read");
        assert_eq!(streamed, b"xyz");

        let mut full = backend
            .open_full_read(volume, "nested/blob.bin")
            .await
            .expect("full stream should open");
        let mut body = Vec::new();
        full.read_to_end(&mut body).await.expect("full stream should read");
        assert_eq!(body, b"xyz");
    }

    #[tokio::test]
    async fn std_backend_rejects_overflow_and_out_of_bounds_reads() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let volume = "test-volume";
        fs::create_dir_all(dir.path().join(volume))
            .await
            .expect("volume should be created");
        fs::write(dir.path().join(volume).join("blob.bin"), b"abc")
            .await
            .expect("test object should be written");
        let backend = StdBackend::new(dir.path().to_path_buf());

        let overflow = backend.pread_bytes(volume, "blob.bin", usize::MAX, 1, None).await;
        assert!(matches!(overflow, Err(DiskError::FileCorrupt)));

        let out_of_bounds = backend.pread_bytes(volume, "blob.bin", 2, 2, None).await;
        assert!(matches!(out_of_bounds, Err(DiskError::FileCorrupt)));

        let stream_out_of_bounds = backend.open_read_stream(volume, "blob.bin", 2, 2).await;
        assert!(matches!(stream_out_of_bounds, Err(DiskError::FileCorrupt)));
    }

    #[tokio::test]
    async fn file_cache_reclaim_wrappers_forward_read_write_flush_and_shutdown() {
        use futures_util::task::noop_waker_ref;
        use std::io::IoSlice;
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let path = dir.path().join("reclaim.bin");
        let file = File::create(&path).await.expect("writer file should be created");
        let mut writer = FileCacheReclaimWriter::new(file, 6, true);

        let mut cx = Context::from_waker(noop_waker_ref());
        let bufs = [IoSlice::new(b"ab"), IoSlice::new(b"cd")];
        let vectored = Pin::new(&mut writer).poll_write_vectored(&mut cx, &bufs);
        assert!(matches!(vectored, Poll::Ready(Ok(_)) | Poll::Pending));
        let _ = AsyncWrite::is_write_vectored(&writer);

        writer.write_all(b"abcdef").await.expect("writer should forward writes");
        writer.flush().await.expect("writer should forward flush");
        writer.shutdown().await.expect("writer should reclaim on shutdown");

        let file = File::open(&path).await.expect("reader file should open");
        let mut reader = FileCacheReclaimReader::new(file, 0, 6, true);
        let mut body = Vec::new();
        reader.read_to_end(&mut body).await.expect("reader should forward reads");
        assert!(body.ends_with(b"abcdef"));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_nocache_helpers_accept_tokio_and_std_files() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let path = dir.path().join("nocache.bin");
        fs::write(&path, b"nocache").await.expect("test file should be written");

        let tokio_file = File::open(&path).await.expect("tokio file should open");
        set_fd_nocache(&tokio_file).expect("tokio fd should accept F_NOCACHE");

        let std_file = std::fs::File::open(&path).expect("std file should open");
        set_std_fd_nocache(&std_file).expect("std fd should accept F_NOCACHE");
    }

    #[cfg(unix)]
    #[test]
    fn mmap_page_fault_delta_clamps_non_monotonic_counts() {
        let before = Some(MmapPageFaultCounts { minor: 10, major: 4 });
        let after = Some(MmapPageFaultCounts { minor: 7, major: 6 });

        assert_eq!(mmap_page_fault_delta(before, after), MmapPageFaultDelta { minor: 0, major: 2 });
        assert_eq!(mmap_page_fault_delta(before, None), MmapPageFaultDelta::default());
    }

    #[test]
    fn test_is_bitrot_size_mismatch_error_only_matches_target_message() {
        assert!(is_bitrot_size_mismatch_error(&io::Error::other("bitrot shard file size mismatch")));
        assert!(!is_bitrot_size_mismatch_error(&io::Error::other("bitrot hash mismatch")));
    }

    #[test]
    fn test_is_bitrot_verification_error_matches_hash_and_size_mismatch() {
        assert!(is_bitrot_verification_error(&io::Error::other("bitrot shard file size mismatch")));
        assert!(is_bitrot_verification_error(&io::Error::other("bitrot hash mismatch")));
        assert!(!is_bitrot_verification_error(&io::Error::other("unrelated io failure")));
    }

    #[tokio::test]
    async fn local_disk_verify_file_preserves_erasure_construction_error() {
        use crate::erasure::coding::ErasureConstructionError;
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "verify-volume";
        ensure_test_volume(&disk, volume).await;

        let mut file_info = FileInfo::new("invalid.bin", 2, 2);
        file_info.erasure.block_size = 0;
        let error = match disk.verify_file(volume, "invalid.bin", &file_info).await {
            Ok(_) => panic!("invalid local-disk erasure metadata must be rejected"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("block_size must be greater than zero"));
        let io_source = std::error::Error::source(&error).expect("DiskError::Io must expose its io::Error source");
        let construction_source = io_source
            .source()
            .expect("io::Error must expose the erasure construction error");
        assert!(construction_source.is::<ErasureConstructionError>());
    }

    #[tokio::test]
    async fn snapshot_leases_defer_data_dir_cleanup_until_last_release() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "snapshot-lease-volume";
        let data_dir = path_join_buf(&["object", &Uuid::new_v4().to_string()]);
        let first_part = path_join_buf(&[&data_dir, "part.1"]);
        let later_part = path_join_buf(&[&data_dir, "part.2"]);
        ensure_test_volume(&disk, volume).await;
        disk.write_all(volume, &first_part, Bytes::from_static(b"first"))
            .await
            .expect("first shard should be written");
        disk.write_all(volume, &later_part, Bytes::from_static(b"later"))
            .await
            .expect("later shard should be written");

        let first = disk
            .acquire_snapshot_lease(volume, &data_dir)
            .await
            .expect("first lease should be acquired");
        let second = disk
            .acquire_snapshot_lease(volume, &data_dir)
            .await
            .expect("second lease should be acquired");
        let renewed = disk
            .renew_snapshot_lease(volume, &data_dir, first)
            .await
            .expect("first lease should renew atomically");
        disk.release_snapshot_lease(volume, &data_dir, first)
            .await
            .expect("the superseded token should be idempotent");
        let status = disk
            .delete_data_dir(
                volume,
                &data_dir,
                DeleteOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await
            .expect("cleanup should be deferred");
        assert_eq!(status, DataDirDeleteStatus::Deferred);
        assert_eq!(
            disk.read_all(volume, &later_part)
                .await
                .expect("a later multipart shard must remain openable while leased"),
            Bytes::from_static(b"later")
        );

        disk.release_snapshot_lease(volume, &data_dir, renewed)
            .await
            .expect("renewed lease release should succeed");
        assert!(
            disk.read_all(volume, &first_part).await.is_ok(),
            "one remaining lease must keep the data directory"
        );
        disk.release_snapshot_lease(volume, &data_dir, second)
            .await
            .expect("last lease release should run deferred cleanup");
        disk.release_snapshot_lease(volume, &data_dir, second)
            .await
            .expect("releasing an already released token should be idempotent");
        assert!(matches!(disk.read_all(volume, &first_part).await, Err(DiskError::FileNotFound)));
    }

    #[tokio::test]
    async fn quota_mutation_fence_revoke_waits_for_active_claim_and_rejects_late_claims() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let bucket = "quota-fence-volume";
        let object = "object";
        let fence_path = quota_mutation_fence_path(bucket, object);
        let token = disk
            .acquire_snapshot_lease(RUSTFS_META_BUCKET, &fence_path)
            .await
            .expect("quota mutation token should be prepared");
        let claim = disk
            .claim_quota_mutation_fence(bucket, object, token)
            .await
            .expect("prepared token should be claimable");

        let release_disk = Arc::clone(&disk);
        let mut release = tokio::spawn(async move {
            release_disk
                .release_snapshot_lease(RUSTFS_META_BUCKET, &fence_path, SnapshotLeaseToken::revoke_all())
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut release).await.is_err(),
            "revoke must wait until an already claimed mutation has finished"
        );

        drop(claim);
        tokio::time::timeout(Duration::from_secs(1), release)
            .await
            .expect("revoke should wake after the final claim drops")
            .expect("revoke task should not panic")
            .expect("revoke should succeed");
        assert!(matches!(
            disk.claim_quota_mutation_fence(bucket, object, token).await,
            Err(DiskError::FileNotFound)
        ));
    }

    #[tokio::test]
    async fn delete_version_keeps_later_part_until_snapshot_release() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "snapshot-version-delete";
        let object = "object";
        let version_id = Uuid::new_v4();
        let data_dir = Uuid::new_v4();
        let rollback_dir = Uuid::new_v4();
        let data_path = path_join_buf(&[object, &data_dir.to_string()]);
        let first_part = path_join_buf(&[&data_path, "part.1"]);
        let later_part = path_join_buf(&[&data_path, "part.2"]);
        ensure_test_volume(&disk, volume).await;
        disk.write_all(volume, &first_part, Bytes::from_static(b"first"))
            .await
            .expect("first shard should be written");
        disk.write_all(volume, &later_part, Bytes::from_static(b"later"))
            .await
            .expect("later shard should be written");
        let fi = test_file_info(object, version_id, Some(data_dir), None);
        disk.write_all(volume, &path_join_buf(&[object, STORAGE_FORMAT_FILE]), test_meta(fi.clone()).into())
            .await
            .expect("metadata should be written");

        let snapshot = disk
            .acquire_snapshot_lease(volume, &data_path)
            .await
            .expect("snapshot lease should be acquired");
        disk.delete_version(
            volume,
            object,
            fi.clone(),
            false,
            DeleteOptions {
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("version delete should commit metadata");
        disk.delete(
            volume,
            &format!("{object}/{rollback_dir}"),
            DeleteOptions {
                recursive: true,
                immediate: true,
                ..Default::default()
            },
        )
        .await
        .expect("version delete should schedule physical cleanup");

        assert_eq!(
            disk.read_all(volume, &later_part)
                .await
                .expect("a later multipart shard must remain openable while leased"),
            Bytes::from_static(b"later")
        );
        disk.release_snapshot_lease(volume, &data_path, snapshot)
            .await
            .expect("snapshot release should run deferred cleanup");
        assert!(matches!(disk.read_all(volume, &first_part).await, Err(DiskError::FileNotFound)));
    }

    #[tokio::test]
    async fn delete_volume_settles_lease_deferred_cleanup() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "snapshot-lease-bucket-delete";
        let object = "multipart_enc";
        let version_id = Uuid::new_v4();
        let data_dir = Uuid::new_v4();
        let rollback_dir = Uuid::new_v4();
        let data_path = path_join_buf(&[object, &data_dir.to_string()]);
        let part = path_join_buf(&[&data_path, "part.1"]);
        ensure_test_volume(&disk, volume).await;
        disk.write_all(volume, &part, Bytes::from_static(b"payload"))
            .await
            .expect("shard should be written");
        let fi = test_file_info(object, version_id, Some(data_dir), None);
        disk.write_all(volume, &path_join_buf(&[object, STORAGE_FORMAT_FILE]), test_meta(fi.clone()).into())
            .await
            .expect("metadata should be written");

        // An abandoned streaming GET pins the data dir with a snapshot lease.
        let snapshot = disk
            .acquire_snapshot_lease(volume, &data_path)
            .await
            .expect("snapshot lease should be acquired");
        disk.delete_version(
            volume,
            object,
            fi.clone(),
            false,
            DeleteOptions {
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("version delete should commit metadata");
        disk.delete(
            volume,
            &format!("{object}/{rollback_dir}"),
            DeleteOptions {
                recursive: true,
                immediate: true,
                ..Default::default()
            },
        )
        .await
        .expect("version delete should schedule physical cleanup");

        // The bucket is logically empty; a non-force volume delete must settle
        // the deferred data-dir cleanup instead of failing with VolumeNotEmpty.
        disk.delete_volume(volume, false)
            .await
            .expect("bucket delete must not observe lease-deferred remnants");
        assert!(matches!(
            disk.read_all(volume, &part).await,
            Err(DiskError::FileNotFound | DiskError::VolumeNotFound)
        ));

        // The late lease release finds nothing pending and stays idempotent.
        disk.release_snapshot_lease(volume, &data_path, snapshot)
            .await
            .expect("releasing the lease after bucket deletion should be a no-op");
    }

    #[tokio::test]
    async fn version_delete_cleanup_intent_survives_local_disk_restart() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let volume = "snapshot-version-delete-restart";
        let object = "object";
        let data_dir = Uuid::new_v4();
        let rollback_dir = Uuid::new_v4();
        let data_path = path_join_buf(&[object, &data_dir.to_string()]);
        let part = path_join_buf(&[&data_path, "part.1"]);
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        ensure_test_volume(&disk, volume).await;
        disk.write_all(volume, &part, Bytes::from_static(b"part"))
            .await
            .expect("shard should be written");
        fs::create_dir_all(root_dir.path().join(volume).join(object).join(rollback_dir.to_string()))
            .await
            .expect("rollback directory should be created");
        assert!(
            disk.reserve_version_delete(volume, object, data_dir, rollback_dir)
                .await
                .expect("cleanup intent should be persisted")
        );
        disk.commit_reserved_version_delete(volume, object, rollback_dir)
            .await
            .expect("cleanup intent should be committed");
        drop(disk);

        let restarted = LocalDisk::new(&endpoint, false).await.expect("local disk should restart");
        restarted
            .delete(
                volume,
                &format!("{object}/{rollback_dir}"),
                DeleteOptions {
                    recursive: true,
                    immediate: true,
                    ..Default::default()
                },
            )
            .await
            .expect("rollback cleanup should recover persisted intent");
        assert!(matches!(restarted.read_all(volume, &part).await, Err(DiskError::FileNotFound)));
    }

    #[tokio::test]
    async fn uuid_suffix_delete_does_not_run_version_cleanup_without_bound_marker() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "snapshot-non-transaction-delete";
        let object = "object";
        let requested_dir = Uuid::new_v4();
        let victim_dir = Uuid::new_v4();
        ensure_test_volume(&disk, volume).await;
        disk.write_all(
            volume,
            &format!("{object}/{requested_dir}/{DELETE_DATA_DIR_MARKER_PREFIX}{victim_dir}"),
            Bytes::new(),
        )
        .await
        .expect("legacy-shaped marker should be written");
        disk.write_all(volume, &format!("{object}/{victim_dir}/part.1"), Bytes::from_static(b"live"))
            .await
            .expect("victim shard should be written");

        disk.delete(
            volume,
            &format!("{object}/{requested_dir}"),
            DeleteOptions {
                recursive: true,
                immediate: true,
                ..Default::default()
            },
        )
        .await
        .expect("ordinary UUID directory delete should succeed");

        assert_eq!(
            disk.read_all(volume, &format!("{object}/{victim_dir}/part.1"))
                .await
                .expect("unbound sibling must not be deleted"),
            Bytes::from_static(b"live")
        );
    }

    #[tokio::test]
    async fn version_delete_marker_is_durable_and_marker_errors_propagate() {
        use tempfile::tempdir;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "snapshot-marker-durability";
        let object = "object";
        let data_dir = Uuid::new_v4();
        let rollback_dir = Uuid::new_v4();
        ensure_test_volume(&disk, volume).await;
        let data_path = disk
            .get_object_path(volume, &format!("{object}/{data_dir}"))
            .expect("data path should resolve");
        fs::create_dir_all(&data_path).await.expect("data dir should be created");

        assert!(
            disk.reserve_version_delete(volume, object, data_dir, rollback_dir)
                .await
                .expect("reserved marker should be written")
        );
        assert!(
            os::fsync_dir_recorder::was_fsynced(&data_path),
            "strict durability must fsync the data directory after marker creation"
        );
        let committed_path = data_path.join(format!("{DELETE_DATA_DIR_MARKER_PREFIX}{rollback_dir}"));
        fs::create_dir_all(&committed_path)
            .await
            .expect("conflicting committed marker directory should be created");
        fs::write(committed_path.join("entry"), b"conflict")
            .await
            .expect("conflicting marker directory should be non-empty");
        disk.commit_reserved_version_delete(volume, object, rollback_dir)
            .await
            .expect_err("marker rename failure must propagate");

        let second_data_dir = Uuid::new_v4();
        let second_rollback = Uuid::new_v4();
        let second_path = disk
            .get_object_path(volume, &format!("{object}/{second_data_dir}"))
            .expect("second data path should resolve");
        fs::create_dir_all(second_path.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{second_rollback}")))
            .await
            .expect("reserved marker conflict directory should be created");
        assert!(
            disk.reserve_version_delete(volume, object, second_data_dir, second_rollback)
                .await
                .is_err(),
            "marker creation failure must propagate"
        );
    }

    #[tokio::test]
    async fn deferred_version_delete_replays_after_restart_without_rollback_dir() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let volume = "snapshot-deferred-delete-restart";
        let object = "object";
        let version_id = Uuid::new_v4();
        let data_dir = Uuid::new_v4();
        let rollback_dir = Uuid::new_v4();
        let data_path = path_join_buf(&[object, &data_dir.to_string()]);
        let part = path_join_buf(&[&data_path, "part.1"]);
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        ensure_test_volume(&disk, volume).await;
        disk.write_all(volume, &part, Bytes::from_static(b"part"))
            .await
            .expect("shard should be written");
        let fi = test_file_info(object, version_id, Some(data_dir), None);
        disk.write_all(volume, &path_join_buf(&[object, STORAGE_FORMAT_FILE]), test_meta(fi.clone()).into())
            .await
            .expect("metadata should be written");
        let _lease = disk
            .acquire_snapshot_lease(volume, &data_path)
            .await
            .expect("snapshot lease should be acquired");
        disk.delete_version(
            volume,
            object,
            fi,
            false,
            DeleteOptions {
                old_data_dir: Some(rollback_dir),
                ..Default::default()
            },
        )
        .await
        .expect("version delete should commit");
        disk.delete(
            volume,
            &format!("{object}/{rollback_dir}"),
            DeleteOptions {
                recursive: true,
                immediate: true,
                ..Default::default()
            },
        )
        .await
        .expect("physical cleanup should be deferred");
        assert!(disk.read_all(volume, &part).await.is_ok(), "leased data must remain");
        drop(disk);

        let restarted = LocalDisk::new(&endpoint, false).await.expect("local disk should restart");
        restarted
            .delete(
                volume,
                &format!("{object}/{rollback_dir}"),
                DeleteOptions {
                    recursive: true,
                    immediate: true,
                    ..Default::default()
                },
            )
            .await
            .expect("committed marker should replay without rollback directory");
        assert!(matches!(restarted.read_all(volume, &part).await, Err(DiskError::FileNotFound)));
    }

    #[tokio::test]
    async fn data_dir_cleanup_without_a_lease_keeps_existing_behavior() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "snapshot-no-lease-volume";
        let data_dir = path_join_buf(&["object", &Uuid::new_v4().to_string()]);
        let part = path_join_buf(&[&data_dir, "part.1"]);
        ensure_test_volume(&disk, volume).await;
        disk.write_all(volume, &part, Bytes::from_static(b"payload"))
            .await
            .expect("test shard should be written");

        let status = disk
            .delete_data_dir(
                volume,
                &data_dir,
                DeleteOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await
            .expect("unleased cleanup should retain the existing delete behavior");
        assert_eq!(status, DataDirDeleteStatus::Deleted);
        assert!(matches!(disk.read_all(volume, &part).await, Err(DiskError::FileNotFound)));
    }

    #[tokio::test]
    async fn snapshot_lease_acquire_and_cleanup_are_atomic() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("local disk should be created"));
        let volume = "snapshot-race-volume";
        ensure_test_volume(&disk, volume).await;

        for iteration in 0..32 {
            let data_dir = path_join_buf(&["object", &format!("{iteration:032x}")]);
            let part = path_join_buf(&[&data_dir, "part.1"]);
            disk.write_all(volume, &part, Bytes::from_static(b"payload"))
                .await
                .expect("test shard should be written");
            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let acquire_disk = Arc::clone(&disk);
            let acquire_barrier = Arc::clone(&barrier);
            let acquire_path = data_dir.clone();
            let acquire = tokio::spawn(async move {
                acquire_barrier.wait().await;
                acquire_disk.acquire_snapshot_lease(volume, &acquire_path).await
            });
            let delete_disk = Arc::clone(&disk);
            let delete_barrier = Arc::clone(&barrier);
            let delete_path = data_dir.clone();
            let delete = tokio::spawn(async move {
                delete_barrier.wait().await;
                delete_disk
                    .delete_data_dir(
                        volume,
                        &delete_path,
                        DeleteOptions {
                            recursive: true,
                            ..Default::default()
                        },
                    )
                    .await
            });
            barrier.wait().await;

            let acquired = acquire.await.expect("acquire task should join");
            let deleted = delete
                .await
                .expect("delete task should join")
                .expect("delete should either run or defer");
            match acquired {
                Ok(token) => {
                    assert_eq!(deleted, DataDirDeleteStatus::Deferred);
                    assert!(disk.read_all(volume, &part).await.is_ok());
                    disk.release_snapshot_lease(volume, &data_dir, token)
                        .await
                        .expect("release should finish deferred cleanup");
                }
                Err(DiskError::FileNotFound) => {
                    assert_eq!(deleted, DataDirDeleteStatus::Deleted);
                }
                Err(err) => panic!("unexpected lease acquisition error: {err}"),
            }
            assert!(matches!(disk.read_all(volume, &part).await, Err(DiskError::FileNotFound)));
        }
    }

    #[tokio::test]
    async fn local_disk_check_parts_rejects_zero_data_geometry_before_shard_math() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "check-parts-volume";
        let object = "object.bin";
        let data_dir = Uuid::new_v4();
        ensure_test_volume(&disk, volume).await;

        let part_path = path_join_buf(&[object, &data_dir.to_string(), "part.1"]);
        disk.write_all(volume, &part_path, Bytes::from_static(b"shard"))
            .await
            .expect("test shard should be written");
        let file_info = FileInfo {
            data_dir: Some(data_dir),
            parts: vec![ObjectPartInfo {
                number: 1,
                size: 1,
                actual_size: 1,
                ..Default::default()
            }],
            erasure: ErasureInfo {
                data_blocks: 0,
                parity_blocks: 2,
                block_size: 1,
                index: 1,
                distribution: vec![1, 2],
                ..Default::default()
            },
            ..Default::default()
        };

        let err = disk
            .check_parts(volume, object, &file_info)
            .await
            .expect_err("invalid erasure metadata must fail before shard size calculation");

        assert_eq!(err, DiskError::FileCorrupt);
    }

    #[tokio::test]
    async fn local_disk_read_file_verifier_reports_bitrot_mismatch() {
        use crate::erasure::coding::BitrotWriter;
        use rustfs_filemeta::ChecksumInfo;
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "verify-volume";
        ensure_test_volume(&disk, volume).await;

        let payload = Bytes::from_static(b"bitrot-payload!!");
        let object = "object.bin";
        let data_dir = Uuid::new_v4();
        let part_number = 1;
        let checksum_algo = HashAlgorithm::HighwayHash256S;
        let mut file_info = FileInfo::new(object, 1, 0);
        file_info.volume = volume.to_string();
        file_info.name = object.to_string();
        file_info.size = i64::try_from(payload.len()).expect("test payload length should fit i64");
        file_info.data_dir = Some(data_dir);
        file_info.erasure.block_size = payload.len();
        file_info.erasure.index = 1;
        file_info.erasure.checksums = vec![ChecksumInfo {
            part_number,
            algorithm: checksum_algo.clone(),
            hash: Bytes::new(),
        }];
        file_info.parts = vec![ObjectPartInfo {
            number: part_number,
            size: payload.len(),
            actual_size: i64::try_from(payload.len()).expect("test payload length should fit i64"),
            ..Default::default()
        }];

        let mut writer = BitrotWriter::new(io::Cursor::new(Vec::new()), file_info.erasure.shard_size(), checksum_algo);
        writer
            .write(&payload)
            .await
            .expect("bitrot writer should encode test payload");
        writer.shutdown().await.expect("bitrot writer should flush test payload");
        let mut encoded = writer.into_inner().into_inner();
        let last = encoded.last_mut().expect("encoded part should not be empty");
        *last ^= 0xff;

        let part_path = path_join_buf(&[object, &data_dir.to_string(), &format!("part.{part_number}")]);
        disk.write_all(volume, &part_path, Bytes::from(encoded))
            .await
            .expect("corrupted encoded part should be written");

        let result = disk
            .verify_file(volume, object, &file_info)
            .await
            .expect("verify_file should return per-part status");

        assert_eq!(result.results, vec![CHECK_PART_FILE_CORRUPT]);
    }

    #[tokio::test]
    async fn local_disk_verify_file_matches_legacy_and_nonlegacy_shard_geometry() {
        use crate::erasure::coding::{BitrotWriter, Erasure};
        use rustfs_filemeta::ChecksumInfo;
        use tempfile::tempdir;

        let root_dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let volume = "verify-volume";
        ensure_test_volume(&disk, volume).await;

        for (object, payload, uses_legacy_checksum) in [
            ("nonlegacy.bin", Bytes::from_static(b"sharddata"), false),
            ("legacy.bin", Bytes::from_static(b"legacydata"), true),
        ] {
            let data_dir = Uuid::new_v4();
            let part_number = 1;
            let codec_erasure = Erasure::new_with_options(2, 2, 16, uses_legacy_checksum);
            let mut file_info = FileInfo::new(object, 2, 2);
            file_info.volume = volume.to_string();
            file_info.name = object.to_string();
            file_info.size = 17;
            file_info.data_dir = Some(data_dir);
            file_info.uses_legacy_checksum = uses_legacy_checksum;
            file_info.erasure.block_size = 16;
            file_info.erasure.index = 1;
            file_info.erasure.checksums = vec![ChecksumInfo {
                part_number,
                algorithm: HashAlgorithm::HighwayHash256S,
                hash: Bytes::new(),
            }];
            file_info.parts = vec![ObjectPartInfo {
                number: part_number,
                size: 17,
                actual_size: 17,
                ..Default::default()
            }];

            let checksum_algo = if uses_legacy_checksum {
                HashAlgorithm::HighwayHash256SLegacy
            } else {
                HashAlgorithm::HighwayHash256S
            };
            let mut writer = BitrotWriter::new(io::Cursor::new(Vec::new()), codec_erasure.shard_size(), checksum_algo);
            writer.write(&payload[..8]).await.expect("first shard block should encode");
            writer.write(&payload[8..]).await.expect("final shard block should encode");
            writer.shutdown().await.expect("bitrot writer should flush test payload");

            let part_path = path_join_buf(&[object, &data_dir.to_string(), &format!("part.{part_number}")]);
            disk.write_all(volume, &part_path, Bytes::from(writer.into_inner().into_inner()))
                .await
                .expect("encoded part should be written");

            let result = disk
                .verify_file(volume, object, &file_info)
                .await
                .expect("verify_file should return per-part status");

            assert_eq!(result.results, vec![CHECK_PART_SUCCESS]);
        }
    }

    // ----- HP-6: O_DIRECT shard write -----

    #[test]
    fn direct_io_write_env_gate_defaults_off() {
        temp_env::with_var_unset(ENV_RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE, || {
            assert!(!is_direct_io_write_enabled(), "O_DIRECT write must default to off");
        });
        temp_env::with_var(ENV_RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE, Some("true"), || {
            assert!(is_direct_io_write_enabled());
        });
        temp_env::with_var(ENV_RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE, Some("false"), || {
            assert!(!is_direct_io_write_enabled());
        });
    }

    #[test]
    fn direct_write_staging_capacity_is_smallest_alignment_multiple_covering_target() {
        for align in [512usize, 1024, 4096, 8192, 65536] {
            let cap = direct_write_staging_capacity(align);
            assert_eq!(cap % align, 0, "capacity must be an alignment multiple (align={align})");
            assert!(
                cap >= DIRECT_WRITE_STAGING_BYTES,
                "capacity must cover the target staging size (align={align})"
            );
            assert!(
                cap - DIRECT_WRITE_STAGING_BYTES < align,
                "capacity must be the smallest covering multiple (align={align})"
            );
        }
    }

    #[test]
    fn direct_write_tail_split_separates_aligned_prefix_from_remainder() {
        let align = 4096usize;
        assert_eq!(direct_write_tail_split(0, align), (0, 0));
        assert_eq!(direct_write_tail_split(align, align), (align, 0));
        assert_eq!(direct_write_tail_split(3 * align, align), (3 * align, 0));
        assert_eq!(direct_write_tail_split(100, align), (0, 100));
        let (prefix, remainder) = direct_write_tail_split(2 * align + 123, align);
        assert_eq!((prefix, remainder), (2 * align, 123));
        assert_eq!(prefix + remainder, 2 * align + 123, "split must reconstruct the input length");
        assert_eq!(prefix % align, 0, "prefix must be alignment-sized");
        assert!(remainder < align, "remainder must be sub-alignment");
    }

    /// End-to-end round trip through `create_file`'s writer with O_DIRECT writes
    /// enabled. On a block-backed Linux filesystem this drives the true
    /// O_DIRECT path; on macOS and on CI filesystems that reject O_DIRECT it
    /// exercises the buffered fallback. Either way, enabling the gate must not
    /// change the bytes read back, across sizes crossing the alignment and
    /// multi-batch staging boundaries (zero-breakage contract).
    #[cfg(unix)]
    #[tokio::test]
    async fn create_file_direct_write_round_trips_all_sizes() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("tempdir");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("disk");
        disk.make_volume("test-volume").await.expect("make_volume");

        let sizes = [
            1usize,
            511,
            512,
            4095,
            4096,
            4097,
            8192,
            12345,
            DIRECT_WRITE_STAGING_BYTES - 1,
            DIRECT_WRITE_STAGING_BYTES,
            DIRECT_WRITE_STAGING_BYTES + 4096,
            2 * DIRECT_WRITE_STAGING_BYTES + 777,
        ];

        for (i, size) in sizes.into_iter().enumerate() {
            let mut state = 0x1234_5678u64.wrapping_add(i as u64);
            let content: Vec<u8> = (0..size)
                .map(|_| {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    (state >> 33) as u8
                })
                .collect();
            let path = format!("obj-{i}.bin");

            temp_env::async_with_vars([(ENV_RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE, Some("true"))], async {
                let mut writer = disk
                    .create_file("", "test-volume", &path, size as i64)
                    .await
                    .expect("create_file");
                // Odd-sized chunks exercise partial staging fills and flushes.
                let mut off = 0;
                while off < content.len() {
                    let end = (off + 777).min(content.len());
                    writer.write_all(&content[off..end]).await.expect("write_all");
                    off = end;
                }
                writer.shutdown().await.expect("shutdown");
            })
            .await;

            let got = disk.read_file_mmap_copy("test-volume", &path, 0, size).await.expect("read");
            assert_eq!(got.as_ref(), content.as_slice(), "round-trip mismatch at size={size}");
        }
    }

    /// Deterministic coverage of the O_DIRECT writer's aligned-batch + tail
    /// state machine on Linux, independent of whether the CI filesystem
    /// supports O_DIRECT: the writer is driven over a plain file with a small
    /// alignment and staging capacity so both a full-batch flush and the tail
    /// split are exercised for every size class.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn direct_writer_state_machine_round_trips_over_plain_file() {
        use std::io::Read;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let align = 512usize;
        let capacity = 4 * align; // forces multi-batch flushes for larger sizes
        for &size in &[0usize, 1, 511, 512, 513, 1024, 2048, 2049, 5000] {
            let path = dir.path().join(format!("s{size}"));
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .read(true)
                .open(&path)
                .expect("open plain file");
            let content: Vec<u8> = (0..size).map(|i| (i.wrapping_mul(7).wrapping_add(3)) as u8).collect();

            let mut writer = DirectWriter::from_std_file_for_test(file, align, capacity);
            let mut off = 0;
            while off < content.len() {
                let end = (off + 300).min(content.len());
                writer.write_all(&content[off..end]).await.expect("write_all");
                off = end;
            }
            writer.shutdown().await.expect("shutdown");

            let mut got = Vec::new();
            std::fs::File::open(&path)
                .expect("reopen")
                .read_to_end(&mut got)
                .expect("read_to_end");
            assert_eq!(got, content, "state-machine round-trip mismatch at size={size}");
        }
    }

    /// Differential test for the LocalIoBackend refactor: every read shape
    /// (pread via mmap_copy, pread via direct_read_copy, bounded stream, full
    /// read) must return byte-identical data for the same (offset, length),
    /// including ranges straddling page boundaries and the file tail.
    #[cfg(unix)]
    #[tokio::test]
    async fn io_backend_read_shapes_return_identical_bytes() {
        use tempfile::tempdir;
        use tokio::io::AsyncReadExt;

        const FILE_LEN: usize = 64 * 1024;

        // Deterministic pseudo-random content (LCG) so failures are reproducible.
        let mut state = 0x9e3779b97f4a7c15u64;
        let content: Vec<u8> = (0..FILE_LEN)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (state >> 33) as u8
            })
            .collect();
        let content = Bytes::from(content);

        let root_dir = tempdir().expect("operation should succeed");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "blob.bin", content.clone())
            .await
            .expect("operation should succeed");

        let page = mmap_page_size().expect("page size should be available") as usize;
        let ranges = [
            (0usize, FILE_LEN),
            (1, 17),
            (page - 1, 2),
            (page, page),
            (2 * page - 1, page + 2),
            (FILE_LEN - 7, 7),
            (0, 0),
        ];

        for (offset, length) in ranges {
            let expected = content.slice(offset..offset + length);

            for method in [
                RUSTFS_OBJECT_MMAP_READ_METHOD_MMAP_COPY,
                RUSTFS_OBJECT_MMAP_READ_METHOD_DIRECT_READ_COPY,
            ] {
                let got = temp_env::async_with_vars([(ENV_RUSTFS_OBJECT_MMAP_READ_METHOD, Some(method))], async {
                    disk.read_file_mmap_copy("test-volume", "blob.bin", offset, length)
                        .await
                        .expect("operation should succeed")
                })
                .await;
                assert_eq!(got, expected, "pread_bytes({method}) mismatch at offset={offset} length={length}");
            }

            let mut stream = disk
                .read_file_stream("test-volume", "blob.bin", offset, length)
                .await
                .expect("operation should succeed");
            let mut streamed = vec![0u8; length];
            stream.read_exact(&mut streamed).await.expect("operation should succeed");
            assert_eq!(
                Bytes::from(streamed),
                expected,
                "open_read_stream mismatch at offset={offset} length={length}"
            );
        }

        let mut full = disk
            .read_file("test-volume", "blob.bin")
            .await
            .expect("operation should succeed");
        let mut all = Vec::new();
        full.read_to_end(&mut all).await.expect("operation should succeed");
        assert_eq!(Bytes::from(all), content, "open_full_read mismatch");
    }

    /// io_uring read backend (backlog#1104): with `RUSTFS_IO_URING_READ_ENABLE`
    /// set, every positioned read returns byte-identical data (the driver serves
    /// it when io_uring is available; on a restricted host the backend degrades
    /// to `StdBackend` and the bytes still match).
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn uring_backend_reads_match_std() {
        use tempfile::tempdir;

        const FILE_LEN: usize = 64 * 1024;
        let mut state = 0x9e3779b97f4a7c15u64;
        let content: Vec<u8> = (0..FILE_LEN)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (state >> 33) as u8
            })
            .collect();
        let content = Bytes::from(content);

        let page = mmap_page_size().expect("page size should be available") as usize;
        let ranges = [
            (0usize, FILE_LEN),
            (1, 17),
            (page - 1, 2),
            (page, page),
            (2 * page - 1, page + 2),
            (FILE_LEN - 7, 7),
            (0, 0),
        ];

        // The env var must be set before LocalDisk::new (the backend is chosen
        // at construction).
        let got_ranges = temp_env::async_with_vars([(ENV_RUSTFS_IO_URING_READ_ENABLE, Some("true"))], async {
            let root_dir = tempdir().expect("operation should succeed");
            let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
            let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
            disk.make_volume("test-volume").await.expect("operation should succeed");
            disk.write_all("test-volume", "blob.bin", content.clone())
                .await
                .expect("operation should succeed");
            let mut out = Vec::new();
            for (offset, length) in ranges {
                out.push(
                    disk.read_file_mmap_copy("test-volume", "blob.bin", offset, length)
                        .await
                        .expect("io_uring-enabled read must succeed (driver or fallback)"),
                );
            }
            out
        })
        .await;

        for ((offset, length), got) in ranges.into_iter().zip(got_ranges) {
            let expected = content.slice(offset..offset + length);
            assert_eq!(got, expected, "uring read mismatch at offset={offset} length={length}");
        }
    }

    /// The runtime degradation latch classifier (backlog#1101, narrowed in
    /// #1171): only subsystem-level errnos latch io_uring off for the whole disk.
    /// EACCES/EOPNOTSUPP are per-file or per-path (handled by the caller) and
    /// data/parameter/missing-file errors never latch.
    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_unsupported_classifies_restriction_errnos_only() {
        use std::io::Error;
        for errno in [libc::EPERM, libc::ENOSYS] {
            assert!(is_io_uring_unsupported(&Error::from_raw_os_error(errno)), "errno {errno} should latch");
        }
        for errno in [
            libc::EACCES,
            libc::EOPNOTSUPP,
            libc::EIO,
            libc::EINVAL,
            libc::ENOENT,
            libc::EAGAIN,
        ] {
            assert!(!is_io_uring_unsupported(&Error::from_raw_os_error(errno)), "errno {errno} must not latch");
        }
        assert!(!is_io_uring_unsupported(&Error::other("driver gone")));
    }

    /// Non-vacuity gate for the io_uring tests (backlog#1179). Emits a grep-able
    /// `SKIP <name>` line and, when `RUSTFS_URING_TESTS_MUST_RUN` is set — a CI
    /// leg that guarantees io_uring is available (e.g. `seccomp=unconfined`) —
    /// panics instead of skipping, so a suite that silently degraded to
    /// StdBackend cannot merge green.
    #[cfg(target_os = "linux")]
    fn uring_test_skip(name: &str) {
        if std::env::var_os("RUSTFS_URING_TESTS_MUST_RUN").is_some() {
            panic!("SKIP {name}: io_uring unavailable but RUSTFS_URING_TESTS_MUST_RUN is set — this leg must exercise io_uring");
        }
        eprintln!("SKIP {name}: io_uring unavailable (restricted environment)");
    }

    /// Per-disk probe cache (backlog#1101): a disk already recorded as
    /// unsupported is skipped by `try_new` without a fresh probe.
    #[cfg(target_os = "linux")]
    #[test]
    fn uring_probe_cache_skips_known_unsupported_disk() {
        use tempfile::tempdir;

        // Precondition: io_uring must be usable on this host. Otherwise a `None`
        // result below is ambiguous — it could be a probe failure rather than
        // the cache fast-path — so the test would pass vacuously in a restricted
        // CI environment. Probe an uncached root; skip if io_uring is
        // unavailable. (The returned backend, if any, is dropped immediately,
        // shutting its driver down.)
        let probe_dir = tempdir().expect("tempdir");
        if UringBackend::try_new(probe_dir.path().to_path_buf()).is_none() {
            uring_test_skip("uring_probe_cache_skips_known_unsupported_disk");
            return;
        }

        // Now a `None` for a cached-unsupported root can ONLY be the cache
        // fast-path (io_uring works here, so a fresh probe would have succeeded).
        let cached = std::path::PathBuf::from("/nonexistent/uring-probe-cache-test-root");
        URING_UNSUPPORTED_DISKS
            .lock()
            .expect("uring probe cache mutex poisoned")
            .insert(cached.clone());
        let skipped = UringBackend::try_new(cached.clone()).is_none();
        // Clean up the process-wide cache entry so no shared state leaks to
        // other tests.
        URING_UNSUPPORTED_DISKS
            .lock()
            .expect("uring probe cache mutex poisoned")
            .remove(&cached);
        assert!(skipped, "a cached-unsupported disk must skip the probe and return None");
    }

    /// Shard count (backlog#1145): `disks × shards` driver threads is the cost, so
    /// a mistyped env var must not spawn an unbounded number per disk. The default
    /// scales with cores but stays inside `1..=4`; any override is clamped to
    /// `1..=MAX_URING_SHARDS`, and an unparseable value falls back to the default.
    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_shard_count_defaults_by_cores_and_clamps_overrides() {
        temp_env::with_var_unset(ENV_RUSTFS_IO_URING_SHARDS, || {
            let default = get_io_uring_shards();
            assert!((1..=4).contains(&default), "default shards must stay in 1..=4, got {default}");
        });
        temp_env::with_var(ENV_RUSTFS_IO_URING_SHARDS, Some("8"), || {
            assert_eq!(get_io_uring_shards(), 8, "an in-range override must be honored");
        });
        temp_env::with_var(ENV_RUSTFS_IO_URING_SHARDS, Some("0"), || {
            assert_eq!(get_io_uring_shards(), 1, "zero shards would start no driver at all");
        });
        temp_env::with_var(ENV_RUSTFS_IO_URING_SHARDS, Some("100000"), || {
            assert_eq!(
                get_io_uring_shards(),
                MAX_URING_SHARDS,
                "a huge override must be capped, not spawn a thread per unit"
            );
        });
        temp_env::with_var(ENV_RUSTFS_IO_URING_SHARDS, Some("not-a-number"), || {
            let got = get_io_uring_shards();
            assert!((1..=4).contains(&got), "an unparseable override must fall back to the default, got {got}");
        });
    }

    /// Prefix invalidation must respect component boundaries: dropping the
    /// descriptors for object `a/b` must not also drop those for `a/bc`.
    /// moka applies the predicate lazily, so this also pins the guarantee we rely
    /// on — a `get` after `invalidate_under` never returns a matched entry.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn fd_cache_prefix_invalidation_respects_component_boundary() {
        use std::fs::File;
        let dir = tempfile::tempdir().expect("operation should succeed");
        let make = || Arc::new(File::create(dir.path().join("f")).expect("operation should succeed"));
        let cache = FdCache::new();
        let key = |path: &str| FdKey {
            volume: "v".into(),
            path: path.into(),
            direct: false,
        };
        for path in ["a/b", "a/b/dir/part.1", "a/bc", "a/bc/dir/part.1"] {
            cache.insert(key(path), make()).await;
        }
        assert_eq!(cache.entry_count().await, 4);

        cache.invalidate_under("v", "a/b");
        // `a/b` itself and everything under it go; the sibling `a/bc` stays.
        assert!(cache.get(&key("a/b")).await.is_none(), "the prefix itself must be dropped");
        assert!(cache.get(&key("a/b/dir/part.1")).await.is_none(), "children must be dropped");
        assert!(cache.get(&key("a/bc")).await.is_some(), "a/bc is not under a/b");
        assert!(cache.get(&key("a/bc/dir/part.1")).await.is_some(), "a/bc/... is not under a/b");

        // A different volume with the same path must be untouched.
        cache.insert(key("a/b"), make()).await;
        cache.invalidate_under("other", "a/b");
        assert!(cache.get(&key("a/b")).await.is_some(), "invalidation must be scoped to its volume");

        // Exact invalidation drops only its own key.
        cache.invalidate_exact("v", "a/bc").await;
        assert!(cache.get(&key("a/bc")).await.is_none(), "exact key must be dropped");
        assert!(
            cache.get(&key("a/bc/dir/part.1")).await.is_some(),
            "exact invalidation must not touch children"
        );
    }

    /// The descriptor cache (backlog#1145) must not let a healed shard be masked
    /// by a stale descriptor.
    ///
    /// Heal reuses the version's `data_dir` and renames a rebuilt shard onto the
    /// SAME `<object>/<data_dir>/part.N` path. This test reproduces exactly that:
    /// read the part (which caches its descriptor), swap the file for new content
    /// by rename, and assert that a read still returns the OLD bytes — proving the
    /// hazard is real and the cache is actually in play — then invalidate and
    /// assert the healed bytes become visible.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn uring_fd_cache_hides_a_healed_shard_until_invalidated() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        let root = root_dir.path().to_path_buf();
        let Some(backend) = temp_env::with_vars(
            [
                (ENV_RUSTFS_IO_URING_READ_ENABLE, Some("true")),
                (ENV_RUSTFS_IO_URING_FD_CACHE, Some("true")),
            ],
            || UringBackend::try_new(root.clone()),
        ) else {
            // Restricted environment (CI seccomp): io_uring is unavailable, so
            // there is no descriptor cache to exercise. Do not vacuously pass.
            uring_test_skip("uring_fd_cache_hides_a_healed_shard_until_invalidated");
            return;
        };
        assert!(backend.fd_cache.is_some(), "the cache must be on for this test to mean anything");

        let volume = "bucket";
        let object = "obj/0d1e2f/part.1";
        let dir = root.join(volume).join("obj/0d1e2f");
        std::fs::create_dir_all(&dir).expect("operation should succeed");
        let part = root.join(volume).join(object);
        std::fs::write(&part, b"corrupt-shard").expect("operation should succeed");

        let before = backend
            .pread_bytes(volume, object, 0, b"corrupt-shard".len(), None)
            .await
            .expect("operation should succeed");
        assert_eq!(before, Bytes::from_static(b"corrupt-shard"));

        // Heal: write the rebuilt shard beside the old one and rename it into
        // place, exactly as rename_data does. The path is unchanged; the inode is not.
        let rebuilt = dir.join("part.1.rebuilt");
        std::fs::write(&rebuilt, b"healed--shard").expect("operation should succeed");
        std::fs::rename(&rebuilt, &part).expect("operation should succeed");

        let stale = backend
            .pread_bytes(volume, object, 0, b"healed--shard".len(), None)
            .await
            .expect("operation should succeed");
        assert_eq!(
            stale,
            Bytes::from_static(b"corrupt-shard"),
            "a cached descriptor is expected to still see the pre-heal inode — this is the hazard \
             invalidate_cached_fds exists to close, and the assertion proves the cache is live"
        );

        backend.invalidate_cached_fds_under(volume, "obj/0d1e2f");
        let healed = backend
            .pread_bytes(volume, object, 0, b"healed--shard".len(), None)
            .await
            .expect("operation should succeed");
        assert_eq!(
            healed,
            Bytes::from_static(b"healed--shard"),
            "after invalidation the healed shard must be visible"
        );
    }

    /// Same heal hazard as the io_uring test, but exercised through the default
    /// `StdBackend` read path (rustfs/backlog#1801): a cached descriptor keeps
    /// serving the pre-heal inode until `invalidate_cached_fds_under` drops it.
    /// `StdBackend` reads via mmap/`try_clone`, so this proves the dup-based hit
    /// path also defers to invalidation rather than masking a healed shard.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn std_fd_cache_hides_a_healed_shard_until_invalidated() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        let root = root_dir.path().to_path_buf();
        let backend = temp_env::with_vars([(ENV_RUSTFS_LOCAL_FD_CACHE, Some("true"))], || StdBackend::new(root.clone()));
        if backend.fd_cache.is_none() {
            // RLIMIT_NOFILE too low for 512 fds/disk (rustfs/backlog#1178): the
            // cache is off, so there is nothing to exercise. Do not vacuously pass.
            eprintln!(
                "std_fd_cache_hides_a_healed_shard_until_invalidated: skipped \
                 (RLIMIT_NOFILE too low for the std fd cache)"
            );
            return;
        }

        let volume = "bucket";
        let object = "obj/0d1e2f/part.1";
        let dir = root.join(volume).join("obj/0d1e2f");
        std::fs::create_dir_all(&dir).expect("operation should succeed");
        let part = root.join(volume).join(object);
        std::fs::write(&part, b"corrupt-shard").expect("operation should succeed");

        let before = backend
            .pread_bytes(volume, object, 0, b"corrupt-shard".len(), None)
            .await
            .expect("operation should succeed");
        assert_eq!(before, Bytes::from_static(b"corrupt-shard"));

        // Heal: rename rebuilt content onto the same part path — inode swap, path
        // unchanged. A cached descriptor would keep reading the old inode.
        let rebuilt = dir.join("part.1.rebuilt");
        std::fs::write(&rebuilt, b"healed--shard").expect("operation should succeed");
        std::fs::rename(&rebuilt, &part).expect("operation should succeed");

        let stale = backend
            .pread_bytes(volume, object, 0, b"healed--shard".len(), None)
            .await
            .expect("operation should succeed");
        assert_eq!(
            stale,
            Bytes::from_static(b"corrupt-shard"),
            "a cached descriptor is expected to still see the pre-heal inode — this is the \
             hazard invalidate_cached_fds exists to close, and the assertion proves the cache is live"
        );

        backend.invalidate_cached_fds_under(volume, "obj/0d1e2f");
        let healed = backend
            .pread_bytes(volume, object, 0, b"healed--shard".len(), None)
            .await
            .expect("operation should succeed");
        assert_eq!(healed, Bytes::from_static(b"healed--shard"));
    }

    /// A repeated read of the same shard must (a) return correct bytes both times
    /// and (b) actually populate the descriptor cache, so the second read can skip
    /// `File::open` (rustfs/backlog#1801).
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn std_fd_cache_serves_repeated_reads_and_caches_descriptor() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        let root = root_dir.path().to_path_buf();
        let backend = temp_env::with_vars([(ENV_RUSTFS_LOCAL_FD_CACHE, Some("true"))], || StdBackend::new(root.clone()));
        let cache = match backend.fd_cache.as_ref() {
            Some(c) => c,
            None => {
                eprintln!(
                    "std_fd_cache_serves_repeated_reads_and_caches_descriptor: skipped \
                     (RLIMIT_NOFILE too low for the std fd cache)"
                );
                return;
            }
        };

        let volume = "bucket";
        let object = "obj/abc/part.1";
        std::fs::create_dir_all(root.join(volume).join("obj/abc")).expect("operation should succeed");
        let payload = b"hello-small-shard-payload";
        std::fs::write(root.join(volume).join(object), payload).expect("operation should succeed");

        let first = backend
            .pread_bytes(volume, object, 0, payload.len(), None)
            .await
            .expect("operation should succeed");
        assert_eq!(first, Bytes::from_static(payload));

        // After the first miss the freshly opened descriptor is indexed; a second
        // read of the same path is a cache hit.
        assert_eq!(cache.entry_count().await, 1, "the first read should have cached exactly one descriptor");

        let second = backend
            .pread_bytes(volume, object, 0, payload.len(), None)
            .await
            .expect("operation should succeed");
        assert_eq!(second, Bytes::from_static(payload));

        // Invalidating by the object prefix drops the cached descriptor.
        backend.invalidate_cached_fds_under(volume, "obj/abc");
        assert_eq!(cache.entry_count().await, 0, "prefix invalidation must drop the cached descriptor");
    }

    /// `StdBackend::new_without_fd_cache` must not build a descriptor cache.
    /// `UringBackend` wraps a `StdBackend` and owns the only cache for the disk,
    /// so an inner cache would be populated by fallback reads
    /// (`UringBackend::pread_bytes` delegates inward) yet never invalidated —
    /// the stale-inode hazard `FdCache` exists to close (backlog#1176/#1801).
    /// This pins the contract so a future constructor change cannot regress it.
    #[cfg(target_os = "linux")]
    #[test]
    fn new_without_fd_cache_builds_no_descriptor_cache() {
        let root_dir = tempfile::tempdir().expect("operation should succeed");
        let backend = StdBackend::new_without_fd_cache(root_dir.path().to_path_buf());
        assert!(
            backend.fd_cache.is_none(),
            "new_without_fd_cache must not build a descriptor cache — UringBackend owns the only cache for the disk"
        );
    }

    /// The mutation paths on `LocalDisk` must actually call
    /// `invalidate_cached_fds`, not merely have it available (backlog#1145).
    /// `rename_file` replaces the inode at a path a reader has already cached;
    /// `delete` unlinks it while a cached descriptor would keep it readable.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn uring_fd_cache_is_invalidated_by_rename_file_and_delete() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_IO_URING_READ_ENABLE, Some("true")),
                (ENV_RUSTFS_IO_URING_FD_CACHE, Some("true")),
            ],
            async {
                let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
                let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
                disk.make_volume("bucket").await.expect("operation should succeed");

                disk.write_all("bucket", "obj/part.1", Bytes::from_static(b"v1"))
                    .await
                    .expect("operation should succeed");
                let got = disk
                    .read_file_mmap_copy("bucket", "obj/part.1", 0, 2)
                    .await
                    .expect("operation should succeed");
                assert_eq!(got, Bytes::from_static(b"v1"), "first read seeds the descriptor cache");

                // rename_file over the same destination path: the inode changes.
                disk.write_all("bucket", "staging/part.1", Bytes::from_static(b"v2"))
                    .await
                    .expect("operation should succeed");
                disk.rename_file("bucket", "staging/part.1", "bucket", "obj/part.1")
                    .await
                    .expect("operation should succeed");
                let got = disk
                    .read_file_mmap_copy("bucket", "obj/part.1", 0, 2)
                    .await
                    .expect("operation should succeed");
                assert_eq!(got, Bytes::from_static(b"v2"), "rename_file must invalidate the cached descriptor");

                // delete must stop the path from answering reads at all.
                disk.delete("bucket", "obj/part.1", DeleteOptions::default())
                    .await
                    .expect("operation should succeed");
                let err = disk.read_file_mmap_copy("bucket", "obj/part.1", 0, 2).await;
                assert!(
                    err.is_err(),
                    "a deleted part must not keep answering reads from a cached descriptor, got {err:?}"
                );
            },
        )
        .await;
    }

    /// `delete_paths` is one of the primary object-delete entry points that
    /// removes data without going through `LocalDisk::delete`; it must invalidate
    /// the fd cache too, or a cached descriptor keeps a removed part readable
    /// (backlog#1175/#1180).
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn uring_fd_cache_is_invalidated_by_delete_paths() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_IO_URING_READ_ENABLE, Some("true")),
                (ENV_RUSTFS_IO_URING_FD_CACHE, Some("true")),
            ],
            async {
                let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
                let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
                disk.make_volume("bucket").await.expect("operation should succeed");

                disk.write_all("bucket", "obj/part.1", Bytes::from_static(b"v1"))
                    .await
                    .expect("operation should succeed");
                let got = disk
                    .read_file_mmap_copy("bucket", "obj/part.1", 0, 2)
                    .await
                    .expect("operation should succeed");
                assert_eq!(got, Bytes::from_static(b"v1"), "first read seeds the descriptor cache");

                disk.delete_paths("bucket", &["obj/part.1".to_string()])
                    .await
                    .expect("operation should succeed");
                let err = disk.read_file_mmap_copy("bucket", "obj/part.1", 0, 2).await;
                assert!(err.is_err(), "delete_paths must invalidate the cached descriptor, got {err:?}");
            },
        )
        .await;
    }

    /// The real `rename_data` commit path — heal's write-then-commit, and the
    /// two production copies at `LocalDisk::rename_data` — must invalidate the
    /// destination part descriptors, not merely `rename_file`/`delete`
    /// (backlog#1180 item 1). Heal reuses a version's `data_dir` and lands the
    /// rebuilt shard on the SAME `<object>/<data_dir>/part.N` path, so a
    /// descriptor cached before the commit would keep serving the pre-heal inode.
    ///
    /// Faithfulness: it drives `disk.rename_data(...)` for real (non-inline part,
    /// so `invalidate_part_paths` is non-empty), not a raw `fs::rename` + manual
    /// invalidate. Non-vacuity: after seeding the cache it removes the on-disk
    /// data dir out of band — the cached fd keeps the old inode alive — and
    /// asserts a read still returns the OLD bytes, which fails outright if the
    /// cache is off, so the test cannot pass without a live cache. Removing the
    /// dir also clears the path for the directory rename `rename_data` performs.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn uring_fd_cache_is_invalidated_by_rename_data() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_IO_URING_READ_ENABLE, Some("true")),
                (ENV_RUSTFS_IO_URING_FD_CACHE, Some("true")),
            ],
            async {
                let root = root_dir.path().to_path_buf();
                // The invalidation only means anything when a descriptor is
                // actually cached: io_uring must be available AND the fd cache on
                // (RLIMIT_NOFILE headroom, backlog#1178). Probe the (existing)
                // root to decide; otherwise there is nothing to exercise.
                let cache_on = UringBackend::try_new(root.clone())
                    .map(|b| b.fd_cache.is_some())
                    .unwrap_or(false);
                if !cache_on {
                    uring_test_skip("uring_fd_cache_is_invalidated_by_rename_data");
                    return;
                }

                let data_dir = Uuid::new_v4();
                let version_id = Uuid::new_v4();
                let part_rel = format!("object/{data_dir}/part.1");

                let endpoint = Endpoint::try_from(root.to_string_lossy().as_ref()).expect("operation should succeed");
                let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
                // `LocalDisk::new` already creates the system buckets, so the tmp
                // bucket exists; `ensure_test_volume` tolerates that.
                ensure_test_volume(&disk, "bucket").await;
                ensure_test_volume(&disk, RUSTFS_META_TMP_BUCKET).await;

                // 1. Seed the descriptor cache: write the destination part and read
                //    it, caching an open fd to that (pre-heal) inode.
                disk.write_all("bucket", &part_rel, Bytes::from_static(b"oldshard"))
                    .await
                    .expect("operation should succeed");
                let got = disk.read_file_mmap_copy("bucket", &part_rel, 0, 8).await.expect("seed read");
                assert_eq!(got, Bytes::from_static(b"oldshard"), "first read seeds the descriptor cache");

                // 2. Remove the destination data dir out of band. The cached fd
                //    keeps the old inode alive and readable, and clears the path so
                //    the real rename_data below can move a fresh data dir onto it (a
                //    directory rename cannot land on a non-empty dir). No LocalDisk
                //    mutation runs here, so nothing invalidates the cache.
                std::fs::remove_dir_all(root.join("bucket").join(format!("object/{data_dir}")))
                    .expect("remove destination data dir out of band");

                // Liveness gate (non-vacuity): the cache must still serve the
                // removed inode. With the cache off this read would fail, so this
                // assertion is what proves the descriptor is genuinely cached.
                let stale = disk
                    .read_file_mmap_copy("bucket", &part_rel, 0, 8)
                    .await
                    .expect("a cached descriptor must still read the removed inode");
                assert_eq!(
                    stale,
                    Bytes::from_static(b"oldshard"),
                    "a cached descriptor is expected to still see the pre-commit inode — proves the cache is live"
                );

                // 3. Stage the rebuilt shard in the tmp bucket under the SAME
                //    data_dir (exactly as heal does) and commit it with the real
                //    rename_data (non-inline: data=None, size>0, one part).
                let src_object = "heal-src";
                disk.write_all(
                    RUSTFS_META_TMP_BUCKET,
                    &format!("{src_object}/{data_dir}/part.1"),
                    Bytes::from_static(b"newshard"),
                )
                .await
                .expect("stage rebuilt shard");

                let mut fi = test_file_info("object", version_id, Some(data_dir), None);
                fi.size = 8;
                fi.add_object_part(1, "etag".to_string(), 8, Some(OffsetDateTime::now_utc()), 8, None, None);
                disk.rename_data(RUSTFS_META_TMP_BUCKET, src_object, fi, "bucket", "object")
                    .await
                    .expect("real rename_data must commit the rebuilt shard");

                // 4. The read must now see the committed shard: rename_data
                //    invalidated the cached descriptor for `{dst}/{data_dir}/part.N`.
                let healed = disk
                    .read_file_mmap_copy("bucket", &part_rel, 0, 8)
                    .await
                    .expect("post-commit read");
                assert_eq!(
                    healed,
                    Bytes::from_static(b"newshard"),
                    "rename_data must invalidate the cached descriptor so the committed shard is visible"
                );
            },
        )
        .await;
    }

    /// The TTL backstop (backlog#1178/#1180): `FdCache` must stop serving a
    /// descriptor once `time_to_live` elapses even when no mutation path ever
    /// calls an explicit `invalidate_*`. This is the safety net for a future
    /// write path that forgets to invalidate — the stale descriptor self-evicts
    /// rather than masking a replaced inode indefinitely. An injected short TTL
    /// exercises the backstop without the production 5s wait; a static check
    /// pins the production value so a change to it is a conscious edit.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn fd_cache_ttl_evicts_without_explicit_invalidation() {
        use std::fs::File;

        assert_eq!(
            FD_CACHE_TTL,
            std::time::Duration::from_secs(5),
            "the production TTL backstop value is pinned; update this assertion deliberately if it changes"
        );

        let dir = tempfile::tempdir().expect("operation should succeed");
        let ttl = std::time::Duration::from_millis(200);
        let cache = FdCache::with_ttl(ttl);
        let key = FdKey {
            volume: "v".into(),
            path: "object/dd/part.1".into(),
            direct: false,
        };
        let file = Arc::new(File::create(dir.path().join("f")).expect("operation should succeed"));
        cache.insert(key.clone(), file).await;
        assert!(cache.get(&key).await.is_some(), "a freshly inserted descriptor must be served");

        // Well past the TTL (10x margin against scheduler jitter): the backstop
        // must drop the descriptor with no explicit invalidation in between.
        tokio::time::sleep(ttl * 10).await;
        assert!(
            cache.get(&key).await.is_none(),
            "the TTL backstop must stop serving a descriptor once time_to_live elapses"
        );
        assert_eq!(cache.entry_count().await, 0, "the expired entry must be evicted, not merely hidden");
    }

    /// Zero-length read bounds parity on the cache-HIT path (backlog#1173/#1180).
    /// A `length == 0` read past EOF must be rejected identically whether the
    /// descriptor is freshly opened (miss path) or served from the cache: the
    /// cache-hit branch fstats the descriptor to reproduce the miss path's
    /// `offset > len` check instead of returning empty unconditionally. Seeds
    /// the cache with a normal read so the zero-length reads are hits, then pins
    /// that UringBackend and StdBackend agree on every case.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn uring_zero_length_read_bounds_match_std_on_cache_hit() {
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        let root = root_dir.path().to_path_buf();
        let Some(backend) = temp_env::with_vars(
            [
                (ENV_RUSTFS_IO_URING_READ_ENABLE, Some("true")),
                (ENV_RUSTFS_IO_URING_FD_CACHE, Some("true")),
            ],
            || UringBackend::try_new(root.clone()),
        ) else {
            uring_test_skip("uring_zero_length_read_bounds_match_std_on_cache_hit");
            return;
        };
        assert!(
            backend.fd_cache.is_some(),
            "the cache-hit branch is the point of this test; the cache must be on"
        );

        let volume = "bucket";
        let object = "obj/dd/part.1";
        std::fs::create_dir_all(root.join(volume).join("obj/dd")).expect("operation should succeed");
        let content: &[u8] = b"exactly-fifteen"; // 15 bytes
        std::fs::write(root.join(volume).join(object), content).expect("operation should succeed");
        let len = content.len();

        // A normal read seeds the descriptor cache, so the zero-length reads
        // below take the cache-hit branch (3206) rather than the miss path.
        let seed = backend.pread_bytes(volume, object, 0, len, None).await.expect("seed read");
        assert_eq!(seed, Bytes::copy_from_slice(content));

        // StdBackend over the same file is the parity oracle.
        let std_backend = StdBackend::new(root.clone());
        for offset in [0usize, len, len + 1] {
            let uring = backend.pread_bytes(volume, object, offset, 0, None).await;
            let std = std_backend.pread_bytes(volume, object, offset, 0, None).await;
            if offset > len {
                assert!(
                    matches!(uring, Err(DiskError::FileCorrupt)),
                    "zero-length read past EOF must be rejected on the cache-hit path (offset={offset}), got {uring:?}"
                );
                assert!(
                    matches!(std, Err(DiskError::FileCorrupt)),
                    "StdBackend oracle must reject the same zero-length read past EOF (offset={offset}), got {std:?}"
                );
            } else {
                let u = uring.unwrap_or_else(|e| panic!("zero-length read at/inside EOF must succeed (offset={offset}): {e:?}"));
                let s = std.unwrap_or_else(|e| {
                    panic!("StdBackend zero-length read at/inside EOF must succeed (offset={offset}): {e:?}")
                });
                assert!(
                    u.is_empty() && s.is_empty(),
                    "zero-length read at/inside EOF must be empty (offset={offset})"
                );
            }
        }
    }

    /// Pages of `path` still resident in the page cache, via `mincore(2)`.
    /// `mincore` reports residency without faulting anything in, so measuring
    /// cannot perturb what it measures.
    #[cfg(target_os = "linux")]
    #[allow(unsafe_code)]
    fn resident_pages(path: &std::path::Path) -> usize {
        use memmap2::MmapOptions;
        let file = std::fs::File::open(path).expect("operation should succeed");
        let len = file.metadata().expect("operation should succeed").len() as usize;
        // SAFETY: read-only map of a regular file we just opened; the map is only
        // used as a page-aligned address range for mincore, never dereferenced.
        let map = unsafe { MmapOptions::new().len(len).map(&file).expect("operation should succeed") };
        let page = mmap_page_size().expect("page size should be available") as usize;
        let mut vec = vec![0u8; len.div_ceil(page)];
        // SAFETY: `map.as_ptr()` is page-aligned and `len` bytes long; `vec` has
        // one byte per page of that range, which is what mincore writes.
        let rc = unsafe { libc::mincore(map.as_ptr() as *mut libc::c_void, len, vec.as_mut_ptr()) };
        assert_eq!(rc, 0, "mincore failed: {}", io::Error::last_os_error());
        vec.iter().filter(|b| *b & 1 == 1).count()
    }

    /// Enabling io_uring must not silently disable the page-cache reclaim policy
    /// (backlog#1145).
    ///
    /// `StdBackend::pread_bytes` calls `fadvise(DONTNEED)` after a read at or
    /// above `RUSTFS_OBJECT_FILE_CACHE_RECLAIM_THRESHOLD` (4 MiB, on by default):
    /// large object reads are usually cold, and leaving them resident evicts
    /// everything else. That is a deliberate policy, not a side effect of how
    /// StdBackend reads — so the io_uring path owes the same behavior.
    ///
    /// The test measures the policy rather than the call: it reads an 8 MiB file
    /// through each backend and asks `mincore` how much of it stayed resident.
    /// The reclaim-disabled case is the non-vacuity gate — if residency were not
    /// observable there, a backend that never reclaimed would pass silently.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn io_uring_reclaims_page_cache_exactly_like_std() {
        use tempfile::tempdir;

        // Above the 4 MiB default threshold, so the policy applies.
        const LEN: usize = 8 << 20;
        let root_dir = tempdir().expect("operation should succeed");
        let root = root_dir.path().to_path_buf();
        let (volume, object) = ("bucket", "obj/dd/part.1");
        std::fs::create_dir_all(root.join(volume).join("obj/dd")).expect("operation should succeed");
        let file_path = root.join(volume).join(object);
        std::fs::write(&file_path, vec![7u8; LEN]).expect("operation should succeed");
        // DONTNEED skips dirty pages; make them clean so residency reflects the
        // reclaim and not writeback timing.
        std::fs::File::open(&file_path)
            .expect("operation should succeed")
            .sync_all()
            .expect("operation should succeed");

        let total_pages = LEN.div_ceil(mmap_page_size().expect("page size should be available") as usize);

        let std_backend: Arc<dyn LocalIoBackend> = Arc::new(StdBackend::new(root.clone()));
        let uring_backend: Option<Arc<dyn LocalIoBackend>> =
            UringBackend::try_new(root.clone()).map(|b| Arc::new(b) as Arc<dyn LocalIoBackend>);
        if uring_backend.is_none() {
            uring_test_skip("io_uring_reclaims_page_cache_exactly_like_std (io_uring half)");
        }

        // `residency_after` reads the whole file first so the pages are certainly
        // resident going in; whatever remains afterwards is the backend's doing.
        async fn residency_after(backend: &Arc<dyn LocalIoBackend>, path: &std::path::Path, volume: &str, object: &str) -> usize {
            let _ = std::fs::read(path).expect("operation should succeed");
            let got = backend
                .pread_bytes(volume, object, 0, LEN, None)
                .await
                .expect("operation should succeed");
            assert_eq!(got.len(), LEN, "the read itself must still return every byte");
            resident_pages(path)
        }

        for (name, backend) in [Some(("std", &std_backend)), uring_backend.as_ref().map(|b| ("uring", b))]
            .into_iter()
            .flatten()
        {
            // Policy ON (the default): the range must not stay resident.
            let resident = temp_env::async_with_vars(
                [(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE, Some("true"))],
                residency_after(backend, &file_path, volume, object),
            )
            .await;
            assert!(
                resident <= total_pages / 10,
                "{name}: reclaim is on, so the read range must not stay resident; {resident}/{total_pages} pages remain"
            );

            // Policy OFF: the pages must stay. This proves the assertion above can
            // actually fail — without it, a backend that never reclaims would pass.
            let resident = temp_env::async_with_vars(
                [(rustfs_config::ENV_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE, Some("false"))],
                residency_after(backend, &file_path, volume, object),
            )
            .await;
            assert!(
                resident >= total_pages / 2,
                "{name}: reclaim is off, so the range must stay resident — otherwise this test \
                 cannot observe reclaim at all; only {resident}/{total_pages} pages remain"
            );
        }
    }

    /// O_DIRECT interop (backlog#1102): with BOTH io_uring and O_DIRECT enabled,
    /// an O_DIRECT-eligible read keeps O_DIRECT semantics via the native
    /// `read_at_direct` path (or, if that disk can't do io_uring+O_DIRECT, the
    /// StdBackend aligned fallback) and still returns exactly the requested
    /// bytes for unaligned ranges.
    ///
    /// The old shape of this test only checked byte-equivalence through
    /// `LocalDisk::read_file_mmap_copy`. On a filesystem that rejects O_DIRECT
    /// the read silently degrades to the buffered fallback and the byte check
    /// still passes, so the test could go green without the native O_DIRECT path
    /// ever running — a vacuous pass (rustfs/backlog#1213). It now builds a real
    /// `UringBackend`, drives `pread_bytes` (which routes eligible reads into
    /// `pread_uring_direct`), and asserts the native path actually executed via
    /// the `native_direct_reads` counter. When the backing filesystem cannot do
    /// io_uring or O_DIRECT (restricted CI runners, tmpfs/overlayfs), the test
    /// skips loudly with `eprintln!` instead of asserting a tautology — but it
    /// still checks byte-correctness on whatever tier served the read.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn uring_preserves_o_direct_for_eligible_reads() {
        use std::sync::atomic::Ordering;
        use tempfile::tempdir;

        // Unaligned on purpose: 3 blocks + 7 bytes.
        const FILE_LEN: usize = 4096 * 3 + 7;
        let mut state = 0x2545f4914f6cdd1du64;
        let content: Vec<u8> = (0..FILE_LEN)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (state >> 33) as u8
            })
            .collect();
        let content = Bytes::from(content);

        let ranges = [
            (0usize, FILE_LEN),
            (0, 4096),
            (4095, 4098),
            (4096 * 2, 4096 + 7),
            (FILE_LEN - 7, 7),
        ];

        let root_dir = tempdir().expect("operation should succeed");
        let root = root_dir.path().to_path_buf();

        // Lay out the shard with LocalDisk, then read it back through a real
        // UringBackend so the test can inspect the O_DIRECT latch and the native
        // read counter directly.
        {
            let endpoint = Endpoint::try_from(root.to_string_lossy().as_ref()).expect("operation should succeed");
            let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
            disk.make_volume("test-volume").await.expect("operation should succeed");
            disk.write_all("test-volume", "shard.bin", content.clone())
                .await
                .expect("operation should succeed");
        }

        // Skip if io_uring is unavailable on this host (restricted env, e.g. the
        // Kubernetes CI runners): there is no native O_DIRECT path to exercise.
        let Some(backend) = UringBackend::try_new(root) else {
            uring_test_skip("uring_preserves_o_direct_for_eligible_reads");
            return;
        };

        // Threshold 1 makes every non-empty read O_DIRECT-eligible, so each read
        // below drives `pread_bytes` into the native `pread_uring_direct` path.
        // These knobs are read per-read, so setting them around the reads is
        // enough (the backend was already constructed above).
        let got_ranges = temp_env::async_with_vars(
            [
                (ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE, Some("true")),
                (ENV_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD, Some("1")),
            ],
            async {
                let mut out = Vec::new();
                for (offset, length) in ranges {
                    out.push(
                        backend
                            .pread_bytes("test-volume", "shard.bin", offset, length, None)
                            .await
                            .expect("O_DIRECT-eligible read must succeed under io_uring (direct or fallback)"),
                    );
                }
                out
            },
        )
        .await;

        // Byte-correctness holds regardless of which tier served the read.
        for ((offset, length), got) in ranges.into_iter().zip(got_ranges) {
            assert_eq!(
                got,
                content.slice(offset..offset + length),
                "O_DIRECT read mismatch at offset={offset} length={length}"
            );
        }

        // The point of backlog#1213: prove the NATIVE O_DIRECT path executed
        // rather than silently passing on the StdBackend fallback. If the
        // filesystem refuses O_DIRECT, `direct_uring.supported` latches off and
        // no native read is counted — skip loudly instead of asserting nothing.
        let native_hits = backend.native_direct_reads.load(Ordering::Relaxed);
        let still_supported = backend.direct_uring.supported.load(Ordering::Relaxed);
        if still_supported && native_hits > 0 {
            assert_eq!(
                native_hits,
                ranges.len() as u64,
                "every eligible read should have gone through the native io_uring O_DIRECT path"
            );
        } else {
            eprintln!(
                "SKIP uring_preserves_o_direct_for_eligible_reads: native O_DIRECT path not \
                 exercised on this filesystem (direct_uring.supported={still_supported}, \
                 native_direct_reads={native_hits}); byte-correctness was still asserted"
            );
        }
    }

    /// Once io_uring is latched off (backlog#1101), reads still return correct
    /// bytes via StdBackend. Lay out the disk with LocalDisk, then read the same
    /// root through a UringBackend with the latch tripped.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn uring_backend_latched_off_reads_via_std() {
        use std::sync::atomic::Ordering;
        use tempfile::tempdir;

        let root_dir = tempdir().expect("operation should succeed");
        let root = root_dir.path().to_path_buf();
        let content: Vec<u8> = (0..4096u32).map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8).collect();
        let content = Bytes::from(content);

        {
            let endpoint = Endpoint::try_from(root.to_string_lossy().as_ref()).expect("operation should succeed");
            let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
            disk.make_volume("test-volume").await.expect("operation should succeed");
            disk.write_all("test-volume", "blob.bin", content.clone())
                .await
                .expect("operation should succeed");
        }

        // Skip if io_uring is unavailable on this host (restricted env).
        let Some(backend) = UringBackend::try_new(root) else {
            uring_test_skip("uring_backend_latched_off_reads_via_std");
            return;
        };
        backend.active.store(false, Ordering::Relaxed);

        let got = backend
            .pread_bytes("test-volume", "blob.bin", 100, 512, None)
            .await
            .expect("latched-off read must succeed via StdBackend");
        assert_eq!(got, content.slice(100..612), "latched-off read returned wrong bytes");
    }

    /// The O_DIRECT read path must return the same bytes as the buffered
    /// path for unaligned shard sizes and ranges, and must silently fall
    /// back (never error) on filesystems that reject O_DIRECT (e.g. tmpfs).
    /// Both legs are covered regardless of which filesystem backs tempdir.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn direct_io_read_matches_buffered_path_or_falls_back() {
        use tempfile::tempdir;

        // Unaligned on purpose: 3 blocks + 7 bytes.
        const FILE_LEN: usize = 4096 * 3 + 7;

        let mut state = 0x2545f4914f6cdd1du64;
        let content: Vec<u8> = (0..FILE_LEN)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (state >> 33) as u8
            })
            .collect();
        let content = Bytes::from(content);

        let root_dir = tempdir().expect("operation should succeed");
        let endpoint = Endpoint::try_from(root_dir.path().to_string_lossy().as_ref()).expect("operation should succeed");
        let disk = LocalDisk::new(&endpoint, false).await.expect("operation should succeed");
        disk.make_volume("test-volume").await.expect("operation should succeed");
        disk.write_all("test-volume", "shard.bin", content.clone())
            .await
            .expect("operation should succeed");

        let ranges = [
            (0usize, FILE_LEN),
            (0, 4096),
            (4095, 4098),
            (4096 * 2, 4096 + 7),
            (FILE_LEN - 7, 7),
        ];

        for (offset, length) in ranges {
            let expected = content.slice(offset..offset + length);
            // Threshold 1 forces every non-empty read through the O_DIRECT attempt.
            let got = temp_env::async_with_vars(
                [
                    (ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE, Some("true")),
                    (ENV_RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD, Some("1")),
                ],
                async {
                    disk.read_file_mmap_copy("test-volume", "shard.bin", offset, length)
                        .await
                        .expect("O_DIRECT-eligible read must succeed (direct or fallback)")
                },
            )
            .await;
            assert_eq!(got, expected, "direct-io read mismatch at offset={offset} length={length}");
        }
    }

    /// pread_direct_aligned must never leak alignment padding: the returned
    /// buffer is exactly the requested logical range.
    #[cfg(target_os = "linux")]
    #[test]
    fn pread_direct_aligned_exact_range_or_unsupported() {
        use std::io::Write;

        let dir = tempfile::tempdir().expect("operation should succeed");
        let file_path = dir.path().join("blob.bin");
        let content: Vec<u8> = (0..4096 * 2 + 13).map(|i| (i % 251) as u8).collect();
        std::fs::File::create(&file_path)
            .and_then(|mut f| f.write_all(&content))
            .expect("operation should succeed");

        let state = DirectIoReadState::new();
        match pread_direct_aligned(&file_path, 4090, 100, &state) {
            Ok(bytes) => {
                assert_eq!(&bytes[..], &content[4090..4190], "padding must not leak");
            }
            Err(err) => {
                assert!(
                    is_direct_io_unsupported(&err),
                    "only unsupported-filesystem errors are acceptable here: {err}"
                );
            }
        }
    }

    /// P1.5 benchmark gate harness (backlog#893): O_DIRECT vs mmap-copy.
    ///
    /// Ignored by default; run explicitly in release mode on a Linux box:
    ///
    /// ```text
    /// RUSTFS_BENCH_DIR=/data/rustfs/bench \
    ///   RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE=true RUSTFS_OBJECT_DIRECT_IO_READ_THRESHOLD=1 \
    ///   cargo test --release -p rustfs-ecstore --lib direct_read_bench_gate -- --ignored --nocapture
    /// ```
    ///
    /// Baseline run: same command without the two DIRECT_IO vars. Knobs:
    /// RUSTFS_BENCH_SHARD_MIB (8), RUSTFS_BENCH_FILE_COUNT (64),
    /// RUSTFS_BENCH_READS (256). Cold cache is enforced with
    /// fadvise(DONTNEED) over the dataset between rounds. Every read is
    /// verified for length; the first four shards are verified byte-for-byte
    /// before timing starts (correctness before performance).
    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "benchmark harness, run explicitly in release mode"]
    async fn direct_read_bench_gate() {
        use std::time::Instant;

        fn env_usize(name: &str, default: usize) -> usize {
            std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
        }

        fn cpu_time_secs() -> f64 {
            // SAFETY: getrusage with RUSAGE_SELF and a zeroed out-param.
            #[allow(unsafe_code)]
            unsafe {
                let mut ru: libc::rusage = std::mem::zeroed();
                if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
                    return f64::NAN;
                }
                let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
                tv(ru.ru_utime) + tv(ru.ru_stime)
            }
        }

        fn drop_dataset_cache(paths: &[PathBuf]) {
            use rustix::fs::{Advice, fadvise};
            for p in paths {
                if let Ok(f) = std::fs::File::open(p) {
                    let _ = fadvise(&f, 0, None, Advice::DontNeed);
                }
            }
        }

        fn percentile(sorted: &[f64], p: f64) -> f64 {
            let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
            sorted[idx]
        }

        fn gen_content(len: usize, seed: u64) -> Bytes {
            let mut state = seed;
            let v: Vec<u8> = (0..len)
                .map(|_| {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    (state >> 33) as u8
                })
                .collect();
            Bytes::from(v)
        }

        let bench_dir = std::env::var("RUSTFS_BENCH_DIR").expect("set RUSTFS_BENCH_DIR to a directory on the target disk");
        let shard_mib = env_usize("RUSTFS_BENCH_SHARD_MIB", 8);
        let file_count = env_usize("RUSTFS_BENCH_FILE_COUNT", 64);
        let reads = env_usize("RUSTFS_BENCH_READS", 256);
        let shard_len = shard_mib * 1024 * 1024;
        const VOLUME: &str = "bench-volume";

        std::fs::create_dir_all(&bench_dir).expect("create bench dir");
        let endpoint = Endpoint::try_from(bench_dir.as_str()).expect("endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk");
        let _ = disk.make_volume(VOLUME).await;

        // Populate, skipping shards that already exist with the right size.
        let mut paths = Vec::with_capacity(file_count);
        for i in 0..file_count {
            let name = format!("shard-{i:04}.bin");
            let abs = disk.get_object_path(VOLUME, &name).expect("path");
            let need_write = std::fs::metadata(&abs).map(|m| m.len() as usize != shard_len).unwrap_or(true);
            if need_write {
                disk.write_all(VOLUME, &name, gen_content(shard_len, 0x9e3779b9 + i as u64))
                    .await
                    .expect("populate shard");
            }
            paths.push(abs);
        }

        // Correctness gate before any timing.
        for i in 0..4.min(file_count) {
            let name = format!("shard-{i:04}.bin");
            let got = disk
                .read_file_mmap_copy(VOLUME, &name, 0, shard_len)
                .await
                .expect("verify read");
            assert_eq!(got, gen_content(shard_len, 0x9e3779b9 + i as u64), "shard {i} content mismatch");
        }

        let mut idx_state = 0xdeadbeefu64;
        let mut pick = |n: usize| {
            idx_state = idx_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((idx_state >> 33) as usize) % n
        };

        for _ in 0..8 {
            let name = format!("shard-{:04}.bin", pick(file_count));
            let _ = disk
                .read_file_mmap_copy(VOLUME, &name, 0, shard_len)
                .await
                .expect("warmup read");
        }
        drop_dataset_cache(&paths);

        // Concurrency models the EC GET shape: FuturesUnordered over shard
        // reads. concurrency=1 keeps the original sequential behavior.
        let concurrency = env_usize("RUSTFS_BENCH_CONCURRENCY", 1).max(1);
        let disk = std::sync::Arc::new(disk);

        let mut latencies_us = Vec::with_capacity(reads);
        let cpu_before = cpu_time_secs();
        let wall_start = Instant::now();
        let mut done = 0usize;
        while done < reads {
            use futures::StreamExt;

            let batch = concurrency.min(reads - done);
            let mut tasks = futures::stream::FuturesUnordered::new();
            for _ in 0..batch {
                let name = format!("shard-{:04}.bin", pick(file_count));
                let disk = disk.clone();
                tasks.push(async move {
                    let t = Instant::now();
                    let bytes = disk
                        .read_file_mmap_copy(VOLUME, &name, 0, shard_len)
                        .await
                        .expect("bench read");
                    assert_eq!(bytes.len(), shard_len);
                    t.elapsed().as_secs_f64() * 1e6
                });
            }
            while let Some(lat) = tasks.next().await {
                latencies_us.push(lat);
            }
            done += batch;
            if done.is_multiple_of(file_count) {
                drop_dataset_cache(&paths);
            }
        }
        let wall = wall_start.elapsed().as_secs_f64();
        let cpu = cpu_time_secs() - cpu_before;

        latencies_us.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        let mean = latencies_us.iter().sum::<f64>() / latencies_us.len() as f64;
        let direct_enabled = std::env::var(ENV_RUSTFS_OBJECT_DIRECT_IO_READ_ENABLE).unwrap_or_default();

        println!(
            "BENCH_RESULT {{\"direct_io_enabled\":\"{direct_enabled}\",\"concurrency\":{concurrency},\"shard_mib\":{shard_mib},\"file_count\":{file_count},\
\"reads\":{reads},\"p50_us\":{:.1},\"p95_us\":{:.1},\"p99_us\":{:.1},\"mean_us\":{:.1},\"wall_s\":{wall:.3},\
\"cpu_s\":{cpu:.3},\"throughput_mib_s\":{:.1}}}",
            percentile(&latencies_us, 0.50),
            percentile(&latencies_us, 0.95),
            percentile(&latencies_us, 0.99),
            mean,
            (reads * shard_mib) as f64 / wall,
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_batch_shard_pread_basic() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let payloads: &[&[u8]] = &[b"aaaaaa", b"bbbbbb", b"cccccc"];
        let mut requests = Vec::new();
        for (i, payload) in payloads.iter().enumerate() {
            let p = dir.path().join(format!("shard-{i}.bin"));
            std::fs::write(&p, payload).unwrap();
            requests.push((p, 0usize, payload.len()));
        }

        let results = batch_shard_pread(requests).await;
        assert_eq!(results.len(), payloads.len());
        for (result, expected) in results.iter().zip(payloads.iter()) {
            assert_eq!(result.as_ref().unwrap().as_ref(), *expected);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_batch_shard_pread_partial_errors() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let good_path = dir.path().join("good.bin");
        std::fs::write(&good_path, b"good data").unwrap();
        let missing_path = dir.path().join("does-not-exist.bin");

        let requests = vec![(good_path, 0usize, 9usize), (missing_path, 0usize, 4usize)];

        let results = batch_shard_pread(requests).await;
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert_eq!(results[0].as_ref().unwrap().as_ref(), b"good data");
        assert!(results[1].is_err());
        assert!(matches!(results[1].as_ref().unwrap_err(), DiskError::Io(_)));
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn windows_and_unix_conditional_file_update_never_deletes_a_new_owner() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        let owner_a = Bytes::from_static(b"owner-a");
        let owner_b = Bytes::from_static(b"owner-b");

        assert_eq!(
            disk.compare_and_update_file(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, None, Some(owner_a.clone()))
                .await
                .expect("owner a should acquire marker"),
            ConditionalFileUpdate::Updated
        );
        assert_eq!(
            disk.compare_and_update_file(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, Some(owner_a.clone()), Some(owner_b.clone()),)
                .await
                .expect("owner b should replace marker"),
            ConditionalFileUpdate::Updated
        );
        assert_eq!(
            disk.compare_and_update_file(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, Some(owner_a), None)
                .await
                .expect("stale owner check should complete"),
            ConditionalFileUpdate::Mismatch
        );
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH)
                .await
                .expect("new owner marker should remain"),
            owner_b.clone()
        );
        assert_eq!(
            disk.compare_and_update_file(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, Some(owner_b), None)
                .await
                .expect("current owner should remove marker"),
            ConditionalFileUpdate::Updated
        );
        assert!(matches!(
            disk.read_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH).await,
            Err(DiskError::FileNotFound)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn conditional_file_update_returns_would_block_when_marker_lock_is_contended() {
        use rustix::fs::{FlockOperation, flock};

        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        ensure_test_volume(&disk, RUSTFS_META_BUCKET).await;
        let marker_path = disk
            .get_object_path(RUSTFS_META_BUCKET, HEALING_MARKER_PATH)
            .expect("marker path should resolve");
        let lock_path = marker_path
            .parent()
            .expect("marker path should have a parent")
            .join(".rustfs-cas.lock");
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .expect("marker lock should open");
        flock(&lock, FlockOperation::LockExclusive).expect("marker lock should be held");

        let err = tokio::time::timeout(
            Duration::from_secs(1),
            disk.compare_and_update_file(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, None, Some(Bytes::from_static(b"owner"))),
        )
        .await
        .expect("contended conditional update must not block")
        .expect_err("contended conditional update must retry");

        assert!(matches!(err, DiskError::Io(ref err) if err.kind() == ErrorKind::WouldBlock));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_conditional_file_update_returns_would_block_when_marker_lock_is_contended() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");
        ensure_test_volume(&disk, RUSTFS_META_BUCKET).await;
        let marker_path = disk
            .get_object_path(RUSTFS_META_BUCKET, HEALING_MARKER_PATH)
            .expect("marker path should resolve");
        let lock_path = marker_path
            .parent()
            .expect("marker path should have a parent")
            .join(".rustfs-cas.lock");
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .expect("marker lock should open");
        lock.try_lock().expect("marker lock should be held");

        let err = tokio::time::timeout(
            Duration::from_secs(1),
            disk.compare_and_update_file(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, None, Some(Bytes::from_static(b"owner"))),
        )
        .await
        .expect("contended conditional update must not block")
        .expect_err("contended conditional update must retry");

        assert!(matches!(err, DiskError::Io(ref err) if err.kind() == ErrorKind::WouldBlock));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn replacement_io_paths_stay_under_the_mount_lease() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        assert!(disk.has_replacement_mount_lease());
        let root_path = disk
            .get_object_path("bucket", "object")
            .expect("public object path should resolve");
        assert_eq!(root_path, disk.root.join("bucket/object"));
        assert!(!root_path.starts_with("/proc/self/fd/"));
        assert_eq!(
            disk.get_bucket_path("bucket").expect("public bucket path should resolve"),
            disk.root.join("bucket")
        );
        assert_eq!(
            disk.resolve_abs_path("bucket/object")
                .expect("public absolute path should resolve"),
            disk.root.join("bucket/object")
        );

        let object_path = disk
            .io_get_object_path("bucket", "object")
            .expect("lease I/O object path should resolve");
        assert!(
            object_path.starts_with("/proc/self/fd/"),
            "replacement I/O must resolve beneath the held directory descriptor"
        );
        assert!(
            std::fs::metadata(disk.io_root())
                .expect("lease I/O root must be stat-able")
                .is_dir(),
            "lease I/O root must remain usable as a directory path"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn replacement_mount_lease_rejects_a_replaced_endpoint_path() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let root = dir.path().to_path_buf();
        let old_root = root.with_extension("leased");
        let endpoint = Endpoint::try_from(root.to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        std::fs::rename(&root, &old_root).expect("move leased mount root aside");
        std::fs::create_dir(&root).expect("replace configured endpoint directory");

        assert!(
            !disk.has_replacement_mount_lease(),
            "replacement admission must reject a path that no longer names the lease"
        );
        assert_eq!(
            disk.format_path,
            root.join(RUSTFS_META_BUCKET).join(crate::disk::FORMAT_CONFIG_FILE),
            "the public format path must keep configured-root semantics after replacement"
        );
        disk.write_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, Bytes::from_static(b"owner"))
            .await
            .expect("lease-root marker write should succeed");
        assert!(old_root.join(RUSTFS_META_BUCKET).join(HEALING_MARKER_PATH).exists());
        assert!(!root.join(RUSTFS_META_BUCKET).join(HEALING_MARKER_PATH).exists());

        drop(disk);
        std::fs::remove_dir_all(&old_root).expect("remove old leased root");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn startup_cleanup_uses_mount_lease_root_as_a_directory() {
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let tmp_leftover = dir.path().join(RUSTFS_META_TMP_BUCKET).join("leftover").join("data");
        fs::create_dir_all(tmp_leftover.parent().expect("tmp leftover should have a parent"))
            .await
            .expect("tmp leftover parent should be created");
        fs::write(&tmp_leftover, b"temporary")
            .await
            .expect("tmp leftover should be written");

        let endpoint = Endpoint::try_from(dir.path().to_str().expect("temp dir should be utf8")).expect("endpoint should parse");
        let disk = LocalDisk::new(&endpoint, true).await.expect("local disk should be created");

        assert!(disk.has_replacement_mount_lease());
        assert!(
            !tmp_leftover.exists(),
            "startup tmp cleanup must be able to rename beneath the procfd I/O root"
        );
        assert!(LocalDisk::meta_path(disk.io_root(), RUSTFS_META_TMP_DELETED_BUCKET).exists());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn replacement_mount_lease_rejects_a_retargeted_endpoint_symlink() {
        use std::os::unix::fs::symlink;
        use tempfile::tempdir;

        let dir = tempdir().expect("temp dir should be created");
        let first_root = dir.path().join("first");
        let second_root = dir.path().join("second");
        let endpoint_path = dir.path().join("replacement");
        std::fs::create_dir(&first_root).expect("first replacement root should be created");
        std::fs::create_dir(&second_root).expect("second replacement root should be created");
        symlink(&first_root, &endpoint_path).expect("replacement endpoint symlink should be created");
        let endpoint = Endpoint::try_from(endpoint_path.to_str().expect("replacement endpoint should be utf8"))
            .expect("replacement endpoint should parse");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk should be created");

        assert!(disk.has_replacement_mount_lease());
        assert!(disk.replacement_mount_lease_root().is_some());

        std::fs::remove_file(&endpoint_path).expect("original endpoint symlink should be removed");
        symlink(&second_root, &endpoint_path).expect("replacement endpoint should be retargeted");

        assert!(
            !disk.has_replacement_mount_lease(),
            "replacement admission must reject an endpoint symlink retargeted away from the held mount"
        );
        assert!(
            disk.replacement_mount_lease_root().is_none(),
            "readiness must not expose an identity root after endpoint retargeting"
        );

        disk.write_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, Bytes::from_static(b"owner"))
            .await
            .expect("lease-root marker write should succeed");
        assert!(first_root.join(RUSTFS_META_BUCKET).join(HEALING_MARKER_PATH).exists());
        assert!(!second_root.join(RUSTFS_META_BUCKET).join(HEALING_MARKER_PATH).exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn replacement_mount_identity_rejects_a_bind_remount_with_the_same_inode() {
        let held = ReplacementMountIdentity {
            device: 8,
            inode: 42,
            mount_id: 101,
        };
        let rebound = ReplacementMountIdentity { mount_id: 102, ..held };

        assert_ne!(
            held, rebound,
            "a bind remount can retain device and inode while changing the mount incarnation"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mountinfo_fallback_uses_the_exact_escaped_mountpoint() {
        let mountinfo = "101 1 0:30 / / rw - rootfs rootfs rw\n202 101 0:42 / /mnt/replacement\\040disk rw - tmpfs tmpfs rw\n";
        assert_eq!(mount_id_from_mountinfo_contents(mountinfo, Path::new("/mnt/replacement disk")), Some(202));
        assert_eq!(mount_id_from_mountinfo_contents(mountinfo, Path::new("/mnt/replacement")), None);
    }
}
