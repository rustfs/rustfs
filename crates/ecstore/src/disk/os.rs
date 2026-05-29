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

async fn reliable_rename(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
) -> io::Result<()> {
    // Try rename first; only mkdir on ENOENT (avoids extra stat on happy path)
    match super::fs::rename_std(src_file_path.as_ref(), dst_file_path.as_ref()) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {
            // Retry once on transient errors
            match super::fs::rename_std(src_file_path.as_ref(), dst_file_path.as_ref()) {
                Ok(()) => return Ok(()),
                Err(retry_err) => {
                    if retry_err.kind() == io::ErrorKind::NotFound {
                        // Parent dir likely missing, create it
                        if let Some(parent) = dst_file_path.as_ref().parent() {
                            reliable_mkdir_all(parent, base_dir.as_ref()).await?;
                        }
                        return super::fs::rename_std(src_file_path.as_ref(), dst_file_path.as_ref())
                            .map_err(|final_err| {
                                warn!(
                                    "reliable_rename failed after mkdir. src: {:?}, dst: {:?}, err: {:?}",
                                    src_file_path.as_ref(), dst_file_path.as_ref(), final_err
                                );
                                final_err
                            });
                    }
                    warn!(
                        "reliable_rename failed. src: {:?}, dst: {:?}, base_dir: {:?}, err: {:?}",
                        src_file_path.as_ref(), dst_file_path.as_ref(), base_dir.as_ref(), retry_err
                    );
                    return Err(retry_err);
                }
            }
        }
    }

    // First attempt returned NotFound — parent dir missing
    if let Some(parent) = dst_file_path.as_ref().parent() {
        reliable_mkdir_all(parent, base_dir.as_ref()).await?;
    }

    super::fs::rename_std(src_file_path.as_ref(), dst_file_path.as_ref()).map_err(|e| {
        warn!(
            "reliable_rename failed after mkdir. src: {:?}, dst: {:?}, err: {:?}",
            src_file_path.as_ref(), dst_file_path.as_ref(), e
        );
        e
    })
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
