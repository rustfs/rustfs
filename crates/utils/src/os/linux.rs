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

use nix::sys::stat::{self, stat};
use nix::sys::statfs::{self, FsType, statfs};
use std::fs::File;
use std::io::{self, BufRead, Error, ErrorKind};
use std::path::Path;

use super::{DiskInfo, IOStats};

/// Returns total and free bytes available in a directory, e.g. `/`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
    let path_display = p.as_ref().display();
    let stat_fs = statfs(p.as_ref())?;

    let bsize = stat_fs.block_size() as u64;
    let bfree = stat_fs.blocks_free() as u64;
    let bavail = stat_fs.blocks_available() as u64;
    let blocks = stat_fs.blocks() as u64;

    let reserved = match bfree.checked_sub(bavail) {
        Some(reserved) => reserved,
        None => {
            return Err(Error::other(format!(
                "detected f_bavail space ({bavail}) > f_bfree space ({bfree}), fs corruption at ({path_display}). please run 'fsck'"
            )));
        }
    };

    let total = match blocks.checked_sub(reserved) {
        Some(total) => total * bsize,
        None => {
            return Err(Error::other(format!(
                "detected reserved space ({reserved}) > blocks space ({blocks}), fs corruption at ({path_display}). please run 'fsck'"
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

    let st = stat(p.as_ref())?;

    Ok(DiskInfo {
        total,
        free,
        used,
        files: stat_fs.files(),
        ffree: stat_fs.files_free(),
        fstype: get_fs_type(stat_fs.filesystem_type()).to_string(),
        major: stat::major(st.st_dev),
        minor: stat::minor(st.st_dev),
        ..Default::default()
    })
}

/// Returns the filesystem type of the underlying mounted filesystem
///
/// TODO The following mapping could not find the corresponding constant in `nix`:
///
/// "137d" => "EXT",
/// "4244" => "HFS",
/// "5346544e" => "NTFS",
/// "61756673" => "AUFS",
/// "ef51" => "EXT2OLD",
/// "2fc12fc1" => "zfs",
/// "ff534d42" => "cifs",
/// "53464846" => "wslfs",
fn get_fs_type(fs_type: FsType) -> &'static str {
    match fs_type {
        statfs::TMPFS_MAGIC => "TMPFS",
        statfs::MSDOS_SUPER_MAGIC => "MSDOS",
        // statfs::XFS_SUPER_MAGIC => "XFS",
        statfs::NFS_SUPER_MAGIC => "NFS",
        statfs::EXT4_SUPER_MAGIC => "EXT4",
        statfs::ECRYPTFS_SUPER_MAGIC => "ecryptfs",
        statfs::OVERLAYFS_SUPER_MAGIC => "overlayfs",
        statfs::REISERFS_SUPER_MAGIC => "REISERFS",
        _ => "UNKNOWN",
    }
}

pub fn same_disk(disk1: &str, disk2: &str) -> std::io::Result<bool> {
    let stat1 = stat(disk1)?;
    let stat2 = stat(disk2)?;

    Ok(stat1.st_dev == stat2.st_dev)
}

pub fn get_drive_stats(major: u32, minor: u32) -> std::io::Result<IOStats> {
    read_drive_stats(&format!("/sys/dev/block/{major}:{minor}/stat"))
}

fn read_drive_stats(stats_file: &str) -> std::io::Result<IOStats> {
    let stats = read_stat(stats_file)?;
    if stats.len() < 11 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("found invalid format while reading {stats_file}"),
        ));
    }
    let mut io_stats = IOStats {
        read_ios: stats[0],
        read_merges: stats[1],
        read_sectors: stats[2],
        read_ticks: stats[3],
        write_ios: stats[4],
        write_merges: stats[5],
        write_sectors: stats[6],
        write_ticks: stats[7],
        current_ios: stats[8],
        total_ticks: stats[9],
        req_ticks: stats[10],
        ..Default::default()
    };

    if stats.len() > 14 {
        io_stats.discard_ios = stats[11];
        io_stats.discard_merges = stats[12];
        io_stats.discard_sectors = stats[13];
        io_stats.discard_ticks = stats[14];
    }
    Ok(io_stats)
}

fn read_stat(file_name: &str) -> std::io::Result<Vec<u64>> {
    // Open file
    let path = Path::new(file_name);
    let file = File::open(path)?;

    // Create a BufReader
    let reader = io::BufReader::new(file);

    // Read first line
    let mut stats = Vec::new();
    if let Some(line) = reader.lines().next() {
        let line = line?;
        // Split line and parse as u64
        // https://rust-lang.github.io/rust-clippy/master/index.html#trim_split_whitespace
        for token in line.split_whitespace() {
            let ui64: u64 = token
                .parse()
                .map_err(|e| Error::new(ErrorKind::InvalidData, format!("failed to parse '{token}' as u64: {e}")))?;
            stats.push(ui64);
        }
    }

    Ok(stats)
}

#[cfg(test)]
mod test {
    use super::get_drive_stats;
    use tracing::debug;

    #[ignore] // FIXME: failed in github actions
    #[test]
    fn test_stats() {
        let major = 7;
        let minor = 11;
        let s = get_drive_stats(major, minor).unwrap();
        debug!("Drive stats for major: {}, minor: {} - {:?}", major, minor, s);
    }
}
