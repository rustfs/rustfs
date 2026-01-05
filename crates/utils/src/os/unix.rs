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

use super::{DiskInfo, IOStats};
use nix::sys::statfs::Statfs;
use nix::sys::{stat::stat, statfs::statfs};
use std::io::Error;
use std::path::Path;

// FreeBSD and OpenBSD return a signed integer for blocks_available.
// Cast to an unsigned integer to use with DiskInfo.
#[cfg(any(target_os = "freebsd", target_os = "openbsd"))]
fn blocks_available(stat: &Statfs) -> u64 {
    match stat.blocks_available().try_into() {
        Ok(bavail) => bavail,
        Err(e) => {
            tracing::warn!("blocks_available returned a negative value: Using 0 as fallback. {}", e);
            0
        }
    }
}

// FreeBSD returns a signed integer for files_free. Cast to an unsigned integer
// to use with DiskInfo
#[cfg(target_os = "freebsd")]
fn files_free(stat: &Statfs) -> u64 {
    match stat.files_free().try_into() {
        Ok(files_free) => files_free,
        Err(e) => {
            tracing::warn!("files_free returned a negative value: Using 0 as fallback. {}", e);
            0
        }
    }
}

#[cfg(not(target_os = "freebsd"))]
fn files_free(stat: &Statfs) -> u64 {
    stat.files_free()
}

#[cfg(not(any(target_os = "freebsd", target_os = "openbsd")))]
fn blocks_available(stat: &Statfs) -> u64 {
    stat.blocks_available()
}

/// Returns total and free bytes available in a directory, e.g. `/`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
    let path_display = p.as_ref().display();
    let stat = statfs(p.as_ref())?;

    let bsize = stat.block_size() as u64;
    let bfree = stat.blocks_free();
    let bavail = blocks_available(&stat);
    let blocks = stat.blocks();

    let reserved = match bfree.checked_sub(bavail) {
        Some(reserved) => reserved,
        None => {
            return Err(Error::other(format!(
                "detected f_bavail space ({bavail}) > f_bfree space ({bfree}), fs corruption at ({path_display}). please run 'fsck'",
            )));
        }
    };

    let total = match blocks.checked_sub(reserved) {
        Some(total) => total * bsize,
        None => {
            return Err(Error::other(format!(
                "detected reserved space ({reserved}) > blocks space ({blocks}), fs corruption at ({path_display}). please run 'fsck'",
            )));
        }
    };

    let free = bavail * bsize;
    let used = match total.checked_sub(free) {
        Some(used) => used,
        None => {
            return Err(Error::other(format!(
                "detected free space ({free}) > total drive space ({total}), fs corruption at ({path_display}). please run 'fsck'"
            )));
        }
    };

    Ok(DiskInfo {
        total,
        free,
        used,
        files: stat.files(),
        ffree: files_free(&stat),
        fstype: stat.filesystem_type_name().to_string(),
        ..Default::default()
    })
}

pub fn same_disk(disk1: &str, disk2: &str) -> std::io::Result<bool> {
    let stat1 = stat(disk1)?;
    let stat2 = stat(disk2)?;

    Ok(stat1.st_dev == stat2.st_dev)
}

pub fn get_drive_stats(_major: u32, _minor: u32) -> std::io::Result<IOStats> {
    Ok(IOStats::default())
}
