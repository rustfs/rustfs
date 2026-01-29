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
use rustix::fs::statfs;
use std::fs::File;
use std::io::{self, BufRead, Error, ErrorKind};
use std::path::Path;

/// Returns total and free bytes available in a directory, e.g. `/`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
    let path_display = p.as_ref().display();
    // Use statfs on Linux to get access to f_type (filesystem magic number)
    let stat = statfs(p.as_ref())?;

    // Linux statfs:
    // f_bsize: Optimal transfer block size
    // f_blocks: Total data blocks in file system
    // f_frsize: Fragment size (since Linux 2.6) - unit for blocks
    //
    // If f_frsize is > 0, it is the unit for f_blocks, f_bfree, f_bavail.
    // Otherwise f_bsize is used.
    let bsize = if stat.f_frsize > 0 {
        stat.f_frsize as u64
    } else {
        stat.f_bsize as u64
    };

    let bfree = stat.f_bfree as u64;
    let bavail = stat.f_bavail as u64;
    let blocks = stat.f_blocks as u64;

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

    let st = rustix::fs::stat(p.as_ref())?;

    Ok(DiskInfo {
        total,
        free,
        used,
        files: stat.f_files as u64,
        ffree: stat.f_ffree as u64,
        fstype: get_fs_type(stat.f_type as u64).to_string(),
        major: rustix::fs::major(st.st_dev) as u64,
        minor: rustix::fs::minor(st.st_dev) as u64,
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
fn get_fs_type(fs_type: u64) -> &'static str {
    // Magic numbers for various filesystems
    match fs_type {
        0x01021994 => "TMPFS",
        0x4d44 => "MSDOS",
        0x6969 => "NFS",
        0xEF53 => "EXT4",
        0xf15f => "ecryptfs",
        0x794c7630 => "overlayfs",
        0x52654973 => "REISERFS",
        // Additional common ones can be added here:
        // 0x58465342 => "XFS",
        // 0x9123683E => "BTRFS",
        _ => "UNKNOWN",
    }
}

pub fn same_disk(disk1: &str, disk2: &str) -> std::io::Result<bool> {
    let stat1 = rustix::fs::stat(disk1)?;
    let stat2 = rustix::fs::stat(disk2)?;

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
