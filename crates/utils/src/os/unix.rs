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
use nix::sys::{stat::stat, statvfs::statvfs};
use std::io::Error;
use std::path::Path;

/// Returns total and free bytes available in a directory, e.g. `/`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
    let path_display = p.as_ref().display();
    let stat = statvfs(p.as_ref())?;

    let bsize = stat.block_size();
    let bfree = stat.blocks_free() as u64;
    let bavail = stat.blocks_available() as u64;
    let blocks = stat.blocks() as u64;

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
        files: stat.files() as u64,
        ffree: stat.files_free() as u64,
        // Statvfs does not provide a way to return the filesystem as name.
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
