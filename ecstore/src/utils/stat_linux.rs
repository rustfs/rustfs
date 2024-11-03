use nix::sys::{
    stat::{major, minor, stat},
    statfs::{statfs, FsType},
};

use crate::{
    disk::Info,
    error::{Error, Result},
};

use lazy_static::lazy_static;
use std::collections::HashMap;

lazy_static! {
    static ref FS_TYPE_TO_STRING_MAP: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        m.insert("1021994", "TMPFS");
        m.insert("137d", "EXT");
        m.insert("4244", "HFS");
        m.insert("4d44", "MSDOS");
        m.insert("52654973", "REISERFS");
        m.insert("5346544e", "NTFS");
        m.insert("58465342", "XFS");
        m.insert("61756673", "AUFS");
        m.insert("6969", "NFS");
        m.insert("ef51", "EXT2OLD");
        m.insert("ef53", "EXT4");
        m.insert("f15f", "ecryptfs");
        m.insert("794c7630", "overlayfs");
        m.insert("2fc12fc1", "zfs");
        m.insert("ff534d42", "cifs");
        m.insert("53464846", "wslfs");
        m
    };
}

fn get_fs_type(ftype: FsType) -> String {
    let binding = format!("{:?}", ftype);
    let fs_type_hex = binding.as_str();
    match FS_TYPE_TO_STRING_MAP.get(fs_type_hex) {
        Some(fs_type_string) => fs_type_string.to_string(),
        None => "UNKNOWN".to_string(),
    }
}

pub fn get_info(path: &str) -> Result<Info> {
    let statfs = statfs(path)?;
    let reserved_blocks = statfs.blocks_free() - statfs.blocks_available();
    let mut info = Info {
        total: statfs.block_size() as u64 * (statfs.blocks() - reserved_blocks),
        free: statfs.blocks() as u64 * statfs.blocks_available(),
        files: statfs.files(),
        ffree: statfs.files_free(),
        fstype: get_fs_type(statfs.filesystem_type()),
        ..Default::default()
    };

    let stat = stat(path)?;
    let dev_id = stat.st_dev as u64;
    info.major = major(dev_id);
    info.minor = minor(dev_id);

    if info.free > info.total {
        return Err(Error::from_string(format!(
            "detected free space {} > total drive space {}, fs corruption at {}. please run 'fsck'",
            info.free, info.total, path
        )));
    }

    info.used = info.total - info.free;

    Ok(info)
}

pub fn same_disk(disk1: &str, disk2: &str) -> Result<bool> {
    let stat1 = stat(disk1)?;
    let stat2 = stat(disk2)?;

    Ok(stat1.st_dev == stat2.st_dev)
}
