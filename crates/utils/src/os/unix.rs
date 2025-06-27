use super::{DiskInfo, IOStats};
use nix::sys::{stat::stat, statfs::statfs};
use std::io::Error;
use std::path::Path;

/// Returns total and free bytes available in a directory, e.g. `/`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
    let path_display = p.as_ref().display();
    let stat = statfs(p.as_ref())?;

    let bsize = stat.block_size() as u64;
    let bfree = stat.blocks_free();
    let bavail = stat.blocks_available();
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
        ffree: stat.files_free(),
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
