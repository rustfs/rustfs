use std::path::Path;

use futures::TryFutureExt;
use tokio::fs;

use crate::{
    error::{Error, Result},
    utils,
};

use super::error::{ioerr_to_diskerr, DiskError};

fn check_path_length(path_name: &str) -> Result<()> {
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

pub async fn make_dir_all(path: impl AsRef<Path>) -> Result<()> {
    check_path_length(path.as_ref().to_string_lossy().to_string().as_str())?;

    utils::fs::make_dir_all(path.as_ref()).map_err(ioerr_to_diskerr).await?;

    Ok(())
}

// read_dir  count read limit. when count == 0 unlimit.
pub async fn read_dir(path: impl AsRef<Path>, count: usize) -> Result<Vec<String>> {
    let mut entries = fs::read_dir(path.as_ref()).await?;

    let mut volumes = Vec::new();

    let mut count: i32 = {
        if count == 0 {
            -1
        } else {
            count as i32
        }
    };

    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();

        if name == "" || name == "." || name == ".." {
            continue;
        }

        let file_type = entry.file_type().await?;

        if file_type.is_dir() {
            count -= 1;
            volumes.push(format!("{}{}", name, utils::path::SLASH_SEPARATOR));

            if count == 0 {
                break;
            }
        }
    }

    Ok(volumes)
}
