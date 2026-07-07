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

use crate::disk::error::DiskError;
use crate::disk::error::Result;
use crate::disk::error_conv::to_file_error;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::{
    io,
    path::{Component, Path},
};
use tokio::fs;
use tracing::warn;

/// Check path length according to OS limits.
pub fn check_path_length(path_name: &str) -> Result<()> {
    // Apple OS X path length is limited to 1016
    if cfg!(target_os = "macos") && path_name.len() > 1016 {
        return Err(DiskError::FileNameTooLong);
    }

    // Disallow more than 1024 characters on windows, there
    // are no known name_max limits on Windows.
    if cfg!(target_os = "windows") && path_name.len() > 1024 {
        return Err(DiskError::FileNameTooLong);
    }

    // On Unix we reject paths if they are just '.', '..' or '/'
    let invalid_paths = [".", "..", "/"];
    if invalid_paths.contains(&path_name) {
        return Err(DiskError::FileAccessDenied);
    }

    // Check each path segment length is > 255 on all Unix
    // platforms, look for this value as NAME_MAX in
    // /usr/include/linux/limits.h
    let mut count = 0usize;
    for c in path_name.chars() {
        match c {
            '/' => count = 0,
            '\\' if cfg!(target_os = "windows") => count = 0, // Reset
            _ => {
                count += 1;
                if count > 255 {
                    return Err(DiskError::FileNameTooLong);
                }
            }
        }
    }

    // Success.
    Ok(())
}

/// Fsync a directory so recently created or renamed entries survive power loss.
/// No-op on non-Unix platforms where directories cannot be opened for syncing.
pub fn fsync_dir_std(dir: impl AsRef<Path>) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir.as_ref())?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Async wrapper around [`fsync_dir_std`]; runs the blocking fsync off the runtime.
pub async fn fsync_dir(dir: impl AsRef<Path>) -> io::Result<()> {
    let dir = dir.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || fsync_dir_std(dir)).await?
}

/// Fdatasync every regular file directly inside `dir`, then fsync the directory
/// itself. Used at commit points so erasure shard files written through the page
/// cache are durable before their directory is renamed into its final location.
pub fn sync_dir_files_std(dir: impl AsRef<Path>) -> io::Result<()> {
    for entry in std::fs::read_dir(dir.as_ref())? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            std::fs::File::open(entry.path())?.sync_data()?;
        }
    }
    fsync_dir_std(dir)
}

/// Async wrapper around [`sync_dir_files_std`]; runs the blocking syncs off the runtime.
pub async fn sync_dir_files(dir: impl AsRef<Path>) -> io::Result<()> {
    let dir = dir.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || sync_dir_files_std(dir)).await?
}

/// Check if the given disk path is the root disk.
/// On Windows, always return false.
/// On Unix, compare the disk paths.
#[tracing::instrument(level = "debug", skip_all)]
pub fn is_root_disk(disk_path: &str, root_disk: &str) -> Result<bool> {
    if cfg!(target_os = "windows") {
        return Ok(false);
    }

    rustfs_utils::os::same_disk(disk_path, root_disk).map_err(|e| to_file_error(e).into())
}

/// Create a directory and all its parent components if they are missing.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn make_dir_all(path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> Result<()> {
    check_path_length(path.as_ref().to_string_lossy().to_string().as_str())?;

    reliable_mkdir_all(path.as_ref(), base_dir.as_ref())
        .await
        .map_err(to_file_error)?;

    Ok(())
}

/// Check if a directory is empty.
/// Only reads one entry to determine if the directory is empty.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn is_empty_dir(path: impl AsRef<Path>) -> bool {
    read_dir(path.as_ref(), 1).await.is_ok_and(|v| v.is_empty())
}

// read_dir  count read limit. when count == 0 unlimit.
/// Return file names in the directory.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn read_dir(path: impl AsRef<Path>, count: i32) -> std::io::Result<Vec<String>> {
    let mut entries = fs::read_dir(path.as_ref()).await?;

    let mut volumes = Vec::new();

    let mut count = count;

    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();

        if name.is_empty() || name == "." || name == ".." {
            continue;
        }

        let file_type = entry.file_type().await?;

        if file_type.is_file() {
            volumes.push(name);
        } else if file_type.is_dir() {
            volumes.push(format!("{name}{SLASH_SEPARATOR}"));
        } else {
            // Entries we don't return (symlinks, sockets, fifos) must not consume
            // the limit: is_empty_dir/list_dir(count=1) would misreport otherwise.
            continue;
        }
        count -= 1;
        if count == 0 {
            break;
        }
    }

    Ok(volumes)
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn rename_all(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
) -> Result<()> {
    reliable_rename(src_file_path, dst_file_path.as_ref(), base_dir)
        .await
        .map_err(to_file_error)?;

    Ok(())
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn rename_all_ignore_missing_source(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
) -> Result<()> {
    match reliable_rename_inner(src_file_path, dst_file_path.as_ref(), base_dir, false).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(to_file_error(err).into()),
    }
}

async fn reliable_rename(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
) -> io::Result<()> {
    reliable_rename_inner(src_file_path, dst_file_path, base_dir, true).await
}

async fn reliable_rename_inner(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
    warn_on_failure: bool,
) -> io::Result<()> {
    if let Some(parent) = dst_file_path.as_ref().parent()
        && !file_exists(parent)
    {
        reliable_mkdir_all(parent, base_dir.as_ref()).await?;
    }

    let mut i = 0;
    loop {
        if let Err(e) = super::fs::rename_std(src_file_path.as_ref(), dst_file_path.as_ref()) {
            if should_retry_rename(&e, i) {
                i += 1;
                continue;
            }
            if warn_on_failure {
                warn!(
                    "reliable_rename failed. src_file_path: {:?}, dst_file_path: {:?}, base_dir: {:?}, err: {:?}",
                    src_file_path.as_ref(),
                    dst_file_path.as_ref(),
                    base_dir.as_ref(),
                    e
                );
            }
            return Err(e);
        }

        break;
    }

    Ok(())
}

/// Whether a failed `rename` in [`reliable_rename_inner`] should be retried.
///
/// Only the first failure is retried, and `NotFound` is never retried: the
/// retry does not recreate the missing source or parent directory, so a second
/// attempt is guaranteed to fail identically. Skipping it spares speculative
/// cleanup renames (e.g. `move_to_trash` on an already-removed tmp path) a
/// pointless second syscall. This predicate is shared by the `rename_data`
/// commit path via `rename_all`, so any relaxation here must keep genuine
/// transient errors retryable.
fn should_retry_rename(err: &io::Error, attempt: usize) -> bool {
    attempt == 0 && err.kind() != io::ErrorKind::NotFound
}

pub async fn reliable_mkdir_all(path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> io::Result<()> {
    let mut i = 0;

    let mut base_dir = base_dir.as_ref();
    loop {
        if let Err(e) = os_mkdir_all(path.as_ref(), base_dir).await {
            if e.kind() == io::ErrorKind::NotFound && i == 0 {
                i += 1;

                if let Some(base_parent) = base_dir.parent()
                    && let Some(c) = base_parent.components().next()
                    && c != Component::RootDir
                {
                    base_dir = base_parent
                }
                continue;
            }

            return Err(e);
        }

        break;
    }

    Ok(())
}

/// Create a directory and all its parent components if they are missing.
/// Without recursion support, fall back to create_dir_all
/// This function will not create directories under base_dir.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn os_mkdir_all(dir_path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> io::Result<()> {
    if !base_dir.as_ref().to_string_lossy().is_empty() && base_dir.as_ref().starts_with(dir_path.as_ref()) {
        return Ok(());
    }

    if let Err(e) = super::fs::mkdir(dir_path.as_ref()).await {
        if e.kind() == io::ErrorKind::AlreadyExists {
            return Ok(());
        }

        if e.kind() != io::ErrorKind::NotFound {
            return Err(e);
        }

        if let Some(parent) = dir_path.as_ref().parent() {
            // Fall back to creating the missing parent chain only when the direct mkdir proves it is required.
            if let Err(parent_err) = super::fs::make_dir_all(parent).await
                && parent_err.kind() != io::ErrorKind::AlreadyExists
            {
                return Err(parent_err);
            }
        }

        if let Err(retry_err) = super::fs::mkdir(dir_path.as_ref()).await
            && retry_err.kind() != io::ErrorKind::AlreadyExists
        {
            return Err(retry_err);
        }
    }

    Ok(())
}

/// Check if a file exists.
/// Returns true if the file exists, false otherwise.
#[tracing::instrument(level = "debug", skip_all)]
pub fn file_exists(path: impl AsRef<Path>) -> bool {
    std::fs::metadata(path.as_ref()).map(|_| true).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn rename_all_missing_source_returns_file_not_found() {
        let temp_dir = tempdir().expect("create temp dir");
        let src = temp_dir.path().join("missing");
        let dst = temp_dir.path().join("dst");

        let err = rename_all(&src, &dst, temp_dir.path())
            .await
            .expect_err("missing source must fail");

        assert!(matches!(err, DiskError::FileNotFound));
        assert!(!dst.exists());
    }

    #[tokio::test]
    async fn rename_all_ignore_missing_source_returns_ok() {
        let temp_dir = tempdir().expect("create temp dir");
        let src = temp_dir.path().join("missing");
        let dst = temp_dir.path().join("dst");

        rename_all_ignore_missing_source(&src, &dst, temp_dir.path())
            .await
            .expect("missing cleanup source must be ignored");

        assert!(!dst.exists());
    }

    #[test]
    fn rename_retry_never_retries_not_found() {
        // NotFound is terminal for the retry loop: the retry does not recreate
        // the missing source/parent, so a second rename would fail identically.
        let not_found = io::Error::new(io::ErrorKind::NotFound, "missing");
        assert!(!should_retry_rename(&not_found, 0));
        assert!(!should_retry_rename(&not_found, 1));
    }

    #[test]
    fn rename_retry_allows_single_retry_for_other_errors() {
        let denied = io::Error::new(io::ErrorKind::PermissionDenied, "denied");
        assert!(should_retry_rename(&denied, 0));
        assert!(!should_retry_rename(&denied, 1));
    }

    #[tokio::test]
    async fn rename_all_moves_existing_directory_tree() {
        // Guards the rename_data commit path, which funnels through
        // reliable_rename_inner via rename_all.
        let temp_dir = tempdir().expect("create temp dir");
        let src = temp_dir.path().join("src-dir");
        std::fs::create_dir_all(src.join("nested")).expect("create src tree");
        std::fs::write(src.join("nested").join("part.1"), b"payload").expect("write part");
        let dst = temp_dir.path().join("dst-parent").join("dst-dir");

        rename_all(&src, &dst, temp_dir.path()).await.expect("rename must succeed");

        assert!(!src.exists());
        assert_eq!(std::fs::read(dst.join("nested").join("part.1")).expect("read moved part"), b"payload");
    }

    #[tokio::test]
    async fn fsync_dir_succeeds_on_directory() {
        let temp_dir = tempdir().expect("create temp dir");

        fsync_dir(temp_dir.path()).await.expect("fsync dir must succeed");
    }

    #[tokio::test]
    async fn sync_dir_files_syncs_regular_files_and_dir() {
        let temp_dir = tempdir().expect("create temp dir");
        std::fs::write(temp_dir.path().join("part.1"), b"shard-one").expect("write part.1");
        std::fs::write(temp_dir.path().join("part.2"), b"shard-two").expect("write part.2");
        std::fs::create_dir(temp_dir.path().join("subdir")).expect("create subdir");

        sync_dir_files(temp_dir.path()).await.expect("sync dir files must succeed");
    }

    #[tokio::test]
    async fn sync_dir_files_missing_dir_returns_not_found() {
        let temp_dir = tempdir().expect("create temp dir");
        let missing = temp_dir.path().join("missing");

        let err = sync_dir_files(&missing).await.expect_err("missing dir must fail");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
