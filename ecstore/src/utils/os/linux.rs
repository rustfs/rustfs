use nix::sys::stat::{self, stat};
use nix::sys::statfs::{self, statfs, FsType};
use std::fs::File;
use std::io::{self, BufRead, Error, ErrorKind};
use std::path::Path;

use crate::disk::Info;
use common::error::{Error as e_Error, Result};

use super::IOStats;

/// returns total and free bytes available in a directory, e.g. `/`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<Info> {
    let stat_fs = statfs(p.as_ref())?;

    let bsize = stat_fs.block_size() as u64;
    let bfree = stat_fs.blocks_free() as u64;
    let bavail = stat_fs.blocks_available() as u64;
    let blocks = stat_fs.blocks() as u64;

    let reserved = match bfree.checked_sub(bavail) {
        Some(reserved) => reserved,
        None => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "detected f_bavail space ({}) > f_bfree space ({}), fs corruption at ({}). please run 'fsck'",
                    bavail,
                    bfree,
                    p.as_ref().display()
                ),
            ))
        }
    };

    let total = match blocks.checked_sub(reserved) {
        Some(total) => total * bsize,
        None => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "detected reserved space ({}) > blocks space ({}), fs corruption at ({}). please run 'fsck'",
                    reserved,
                    blocks,
                    p.as_ref().display()
                ),
            ))
        }
    };

    let free = bavail * bsize;
    let used = match total.checked_sub(free) {
        Some(used) => used,
        None => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "detected free space ({}) > total drive space ({}), fs corruption at ({}). please run 'fsck'",
                    free,
                    total,
                    p.as_ref().display()
                ),
            ))
        }
    };

    let st = stat(p.as_ref())?;

    Ok(Info {
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

/// returns the filesystem type of the underlying mounted filesystem
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

pub fn same_disk(disk1: &str, disk2: &str) -> Result<bool> {
    let stat1 = stat(disk1)?;
    let stat2 = stat(disk2)?;

    Ok(stat1.st_dev == stat2.st_dev)
}

pub fn get_drive_stats(major: u32, minor: u32) -> Result<IOStats> {
    read_drive_stats(&format!("/sys/dev/block/{}:{}/stat", major, minor))
}

fn read_drive_stats(stats_file: &str) -> Result<IOStats> {
    let stats = read_stat(stats_file)?;
    if stats.len() < 11 {
        return Err(e_Error::from_string(format!("found invalid format while reading {}", stats_file)));
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

fn read_stat(file_name: &str) -> Result<Vec<u64>> {
    // 打开文件
    let path = Path::new(file_name);
    let file = File::open(path)?;

    // 创建一个 BufReader
    let reader = io::BufReader::new(file);

    // 读取第一行
    let mut stats = Vec::new();
    if let Some(line) = reader.lines().next() {
        let line = line?;
        // 分割行并解析为 u64
        // https://rust-lang.github.io/rust-clippy/master/index.html#trim_split_whitespace
        for token in line.split_whitespace() {
            let ui64: u64 = token.parse()?;
            stats.push(ui64);
        }
    }

    Ok(stats)
}

#[cfg(test)]
mod test {
    use super::get_drive_stats;

    #[ignore] // FIXME: failed in github actions
    #[test]
    fn test_stats() {
        let major = 7;
        let minor = 11;
        let s = get_drive_stats(major, minor).unwrap();
        println!("{:?}", s);
    }
}
