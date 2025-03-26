use super::IOStats;
use crate::disk::Info;
use common::error::Result;
use nix::sys::{stat::stat, statfs::statfs};
use std::io::{Error, ErrorKind};
use std::path::Path;

/// returns total and free bytes available in a directory, e.g. `/`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<Info> {
    let stat = statfs(p.as_ref())?;

    let bsize = stat.block_size() as u64;
    let bfree = stat.blocks_free() as u64;
    let bavail = stat.blocks_available() as u64;
    let blocks = stat.blocks() as u64;

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

    Ok(Info {
        total,
        free,
        used,
        files: stat.files(),
        ffree: stat.files_free(),
        fstype: stat.filesystem_type_name().to_string(),
        ..Default::default()
    })
}

pub fn same_disk(disk1: &str, disk2: &str) -> Result<bool> {
    let stat1 = stat(disk1)?;
    let stat2 = stat(disk2)?;

    Ok(stat1.st_dev == stat2.st_dev)
}

pub fn get_drive_stats(_major: u32, _minor: u32) -> Result<IOStats> {
    Ok(IOStats::default())
}
