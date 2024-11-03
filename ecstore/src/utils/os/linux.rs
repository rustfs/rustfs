use nix::sys::stat::{self, stat};
use nix::sys::statfs::{self, statfs, FsType};
use std::io::{Error, ErrorKind};
use std::path::Path;

use crate::{disk::Info, error::Result};

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
        statfs::XFS_SUPER_MAGIC => "XFS",
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
