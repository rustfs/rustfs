use std::{
    io,
    path::{Component, Path},
};

use crate::{
    disk::error::{is_sys_err_not_dir, is_sys_err_path_not_found, os_is_not_exist},
    utils::{self, os::same_disk},
};
use common::error::{Error, Result};
use tokio::fs;
use tracing::info;

use super::error::{os_err_to_file_err, os_is_exist, DiskError};

pub fn check_path_length(path_name: &str) -> Result<()> {
    // Apple OS X path length is limited to 1016
    if cfg!(target_os = "macos") && path_name.len() > 1016 {
        return Err(Error::new(DiskError::FileNameTooLong));
    }

    // Disallow more than 1024 characters on windows, there
    // are no known name_max limits on Windows.
    if cfg!(target_os = "windows") && path_name.len() > 1024 {
        return Err(Error::new(DiskError::FileNameTooLong));
    }

    // On Unix we reject paths if they are just '.', '..' or '/'
    let invalid_paths = [".", "..", "/"];
    if invalid_paths.contains(&path_name) {
        return Err(Error::new(DiskError::FileAccessDenied));
    }

    // Check each path segment length is > 255 on all Unix
    // platforms, look for this value as NAME_MAX in
    // /usr/include/linux/limits.h
    let mut count = 0usize;
    for c in path_name.chars() {
        match c {
            '/' | '\\' if cfg!(target_os = "windows") => count = 0, // Reset
            _ => {
                count += 1;
                if count > 255 {
                    return Err(Error::new(DiskError::FileNameTooLong));
                }
            }
        }
    }

    // Success.
    Ok(())
}

pub fn is_root_disk(disk_path: &str, root_disk: &str) -> Result<bool> {
    if cfg!(target_os = "windows") {
        return Ok(false);
    }

    same_disk(disk_path, root_disk)
}

pub async fn make_dir_all(path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> Result<()> {
    check_path_length(path.as_ref().to_string_lossy().to_string().as_str())?;

    if let Err(e) = reliable_mkdir_all(path.as_ref(), base_dir.as_ref()).await {
        if is_sys_err_not_dir(&e) || is_sys_err_path_not_found(&e) {
            return Err(Error::new(DiskError::FileAccessDenied));
        }

        return Err(os_err_to_file_err(e));
    }

    Ok(())
}

pub async fn is_empty_dir(path: impl AsRef<Path>) -> bool {
    read_dir(path.as_ref(), 1).await.is_ok_and(|v| v.is_empty())
}

// read_dir  count read limit. when count == 0 unlimit.
pub async fn read_dir(path: impl AsRef<Path>, count: i32) -> Result<Vec<String>> {
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
            volumes.push(format!("{}{}", name, utils::path::SLASH_SEPARATOR));
        }
        count -= 1;
        if count == 0 {
            break;
        }
    }

    Ok(volumes)
}

pub async fn rename_all(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
) -> Result<()> {
    reliable_rename(src_file_path, dst_file_path.as_ref(), base_dir)
        .await
        .map_err(|e| {
            if is_sys_err_not_dir(&e) || !os_is_not_exist(&e) || is_sys_err_path_not_found(&e) {
                Error::new(DiskError::FileAccessDenied)
            } else if os_is_not_exist(&e) {
                Error::new(DiskError::FileNotFound)
            } else if os_is_exist(&e) {
                Error::new(DiskError::IsNotRegular)
            } else {
                Error::new(e)
            }
        })?;

    Ok(())
}

pub async fn reliable_rename(
    src_file_path: impl AsRef<Path>,
    dst_file_path: impl AsRef<Path>,
    base_dir: impl AsRef<Path>,
) -> io::Result<()> {
    if let Some(parent) = dst_file_path.as_ref().parent() {
        if !file_exists(parent).await {
            info!("reliable_rename reliable_mkdir_all parent: {:?}", parent);
            reliable_mkdir_all(parent, base_dir.as_ref()).await?;
        }
    }

    let mut i = 0;
    loop {
        if let Err(e) = utils::fs::rename(src_file_path.as_ref(), dst_file_path.as_ref()).await {
            if os_is_not_exist(&e) && i == 0 {
                i += 1;
                continue;
            }
            // info!(
            //     "reliable_rename failed. src_file_path: {:?}, dst_file_path: {:?}, base_dir: {:?}, err: {:?}",
            //     src_file_path.as_ref(),
            //     dst_file_path.as_ref(),
            //     base_dir.as_ref(),
            //     e
            // );
            return Err(e);
        }

        break;
    }

    Ok(())
}

pub async fn reliable_mkdir_all(path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> io::Result<()> {
    let mut i = 0;

    let mut base_dir = base_dir.as_ref();
    loop {
        if let Err(e) = os_mkdir_all(path.as_ref(), base_dir).await {
            if os_is_not_exist(&e) && i == 0 {
                i += 1;

                if let Some(base_parent) = base_dir.parent() {
                    if let Some(c) = base_parent.components().next() {
                        if c != Component::RootDir {
                            base_dir = base_parent
                        }
                    }
                }
                continue;
            }

            return Err(e);
        }

        break;
    }

    Ok(())
}

pub async fn os_mkdir_all(dir_path: impl AsRef<Path>, base_dir: impl AsRef<Path>) -> io::Result<()> {
    if !base_dir.as_ref().to_string_lossy().is_empty() && base_dir.as_ref().starts_with(dir_path.as_ref()) {
        return Ok(());
    }

    if let Some(parent) = dir_path.as_ref().parent() {
        // 不支持递归，直接create_dir_all了
        if let Err(e) = utils::fs::make_dir_all(&parent).await {
            if os_is_exist(&e) {
                return Ok(());
            }

            return Err(e);
        }
        // Box::pin(os_mkdir_all(&parent, &base_dir)).await?;
    }

    if let Err(e) = utils::fs::mkdir(dir_path.as_ref()).await {
        if os_is_exist(&e) {
            return Ok(());
        }

        return Err(e);
    }

    Ok(())
}

pub async fn file_exists(path: impl AsRef<Path>) -> bool {
    fs::metadata(path.as_ref()).await.map(|_| true).unwrap_or(false)
}
