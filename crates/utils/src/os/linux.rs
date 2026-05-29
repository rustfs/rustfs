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

use super::{DiskInfo, IOStats, fs_type::get_fs_type};
use rustix::fs::statfs;
use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::io::{self, BufRead, Error, ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use tracing::warn;

static BAVAIL_GT_BFREE_WARNING_PATHS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();

/// Returns total and free bytes available in a directory, e.g. `/`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
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
    let (total, free, used) = calculate_space_usage(blocks, bfree, bavail, bsize, p.as_ref().display())?;

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

fn calculate_space_usage(
    blocks: u64,
    bfree: u64,
    bavail: u64,
    bsize: u64,
    path: impl std::fmt::Display,
) -> std::io::Result<(u64, u64, u64)> {
    let available = if bfree < bavail {
        let path_display = path.to_string();
        if should_warn_bavail_greater_than_bfree(&path_display) {
            warn!(
                path = %path_display,
                f_bfree = bfree,
                f_bavail = bavail,
                "detected f_bavail greater than f_bfree, capping available blocks to f_bfree for compatibility"
            );
        }
        bfree
    } else {
        bavail
    };
    let reserved = bfree - available;

    let total = match blocks.checked_sub(reserved) {
        Some(total) => total * bsize,
        None => {
            return Err(Error::other(format!(
                "detected reserved space ({reserved}) > blocks space ({blocks}), fs corruption at ({path}). please run 'fsck'",
            )));
        }
    };

    let free = available * bsize;
    let used = match total.checked_sub(free) {
        Some(used) => used,
        None => {
            return Err(Error::other(format!(
                "detected free space ({free}) > total drive space ({total}), fs corruption at ({path}). please run 'fsck'"
            )));
        }
    };

    Ok((total, free, used))
}

fn should_warn_bavail_greater_than_bfree(path_display: &str) -> bool {
    let warned_paths = BAVAIL_GT_BFREE_WARNING_PATHS.get_or_init(|| Mutex::new(BTreeSet::new()));
    match warned_paths.lock() {
        Ok(mut warned_paths) => warned_paths.insert(path_display.to_owned()),
        Err(_) => true,
    }
}

pub fn same_disk(disk1: &str, disk2: &str) -> std::io::Result<bool> {
    let stat1 = rustix::fs::stat(disk1)?;
    let stat2 = rustix::fs::stat(disk2)?;

    Ok(stat1.st_dev == stat2.st_dev)
}

/// Resolve the leaf physical device identities backing a local filesystem path.
///
/// Linux block stacks such as partitions, `dm-*`, or software RAID can all
/// expose a filesystem through an intermediate device node. This helper walks
/// sysfs until it reaches the leaf backing devices so the caller can compare
/// physical failure domains instead of only filesystem device numbers.
pub fn get_physical_device_ids(disk: &str) -> std::io::Result<Vec<String>> {
    let stat = rustix::fs::stat(disk)?;
    let major = rustix::fs::major(stat.st_dev) as u64;
    let minor = rustix::fs::minor(stat.st_dev) as u64;
    let devices = resolve_block_device_ids(major, minor)?;

    Ok(devices.into_iter().collect())
}

fn resolve_block_device_ids(major: u64, minor: u64) -> std::io::Result<BTreeSet<String>> {
    let sysfs_path = PathBuf::from(format!("/sys/dev/block/{major}:{minor}"));
    let resolved = match fs::canonicalize(&sysfs_path) {
        Ok(path) => path,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return Ok(BTreeSet::from([format!("{major}:{minor}")]));
        }
        Err(err) => return Err(err),
    };
    let devices = collect_block_device_ids(&resolved)?;

    if devices.is_empty() {
        Ok(BTreeSet::from([format!("{major}:{minor}")]))
    } else {
        Ok(devices)
    }
}

fn collect_block_device_ids(device_path: &Path) -> std::io::Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    let slaves_dir = device_path.join("slaves");

    match fs::read_dir(&slaves_dir) {
        Ok(entries) => {
            let mut found_slave = false;
            for entry in entries {
                let entry = entry?;
                found_slave = true;
                let resolved = fs::canonicalize(entry.path())?;
                ids.extend(collect_block_device_ids(&resolved)?);
            }

            if found_slave {
                return Ok(ids);
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    ids.insert(normalize_block_device_name(device_path));

    Ok(ids)
}

fn normalize_block_device_name(device_path: &Path) -> String {
    if device_path.join("partition").exists()
        && let Some(parent_name) = device_path.parent().and_then(|parent| parent.file_name())
    {
        return parent_name.to_string_lossy().into_owned();
    }

    device_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| device_path.display().to_string())
}

/// Check whether any configured export path contains nested mount points.
///
/// This mirrors the intent of MinIO's cross-device mount guardrail: once an
/// export path is selected, RustFS should not silently traverse into child
/// mount points hosted by other devices.
pub fn check_cross_device_mounts(paths: &[String]) -> std::io::Result<()> {
    check_cross_device_mounts_with_reader(paths, File::open("/proc/mounts")?)
}

/// Parse `/proc/mounts`-style content and validate each export path against it.
fn check_cross_device_mounts_with_reader(paths: &[String], mut reader: impl Read) -> std::io::Result<()> {
    let mut content = String::new();
    reader.read_to_string(&mut content)?;
    let mount_paths = parse_mount_paths(&content);

    for path in paths {
        ensure_no_sub_mounts(path, &mount_paths)?;
    }

    Ok(())
}

/// Extract mount paths from `/proc/mounts` content, decoding escaped spaces.
fn parse_mount_paths(content: &str) -> Vec<String> {
    content
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() != 6 {
                return None;
            }

            Some(fields[1].replace("\\040", " "))
        })
        .collect()
}

/// Validate that `path` does not contain nested child mount points.
fn ensure_no_sub_mounts(path: &str, mount_paths: &[String]) -> std::io::Result<()> {
    if !Path::new(path).is_absolute() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("Invalid argument, path ({path}) is expected to be absolute"),
        ));
    }
    if path == "/" {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Invalid argument, path (/) cannot be the filesystem root for export validation",
        ));
    }

    let base = normalize_mount_path(path);
    let mut cross_mounts = Vec::new();

    for mount_path in mount_paths {
        let mount_base = normalize_mount_path(mount_path);
        if mount_base.starts_with(&base) && mount_base != base {
            cross_mounts.push(mount_path.clone());
        }
    }

    if cross_mounts.is_empty() {
        return Ok(());
    }

    cross_mounts.sort();
    cross_mounts.dedup();

    Err(Error::other(format!(
        "Nested mount points detected under path ({path}) at the following locations: {}. Export path should not have any sub-mounts, refusing to start.",
        cross_mounts.join(", ")
    )))
}

/// Normalize mount paths so prefix checks treat `/a/b` and `/a/b/` identically.
fn normalize_mount_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("{trimmed}/")
    }
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
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn normalize_partition_device_to_parent_disk() {
        let dir = tempdir().unwrap();
        let block = dir.path().join("block");
        let disk = block.join("nvme0n1");
        let partition = disk.join("nvme0n1p1");
        let slaves = partition.join("slaves");

        fs::create_dir_all(&slaves).unwrap();
        fs::write(partition.join("partition"), "1").unwrap();

        let ids = collect_block_device_ids(&partition).unwrap();
        assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec!["nvme0n1".to_string()]);
    }

    #[test]
    fn flatten_device_mapper_slaves_to_leaf_devices() {
        let dir = tempdir().unwrap();
        let block = dir.path().join("block");
        let dm = block.join("dm-0");
        let dm_slaves = dm.join("slaves");
        let nvme0 = block.join("nvme0n1");
        let nvme1 = block.join("nvme1n1");

        fs::create_dir_all(&dm_slaves).unwrap();
        fs::create_dir_all(&nvme0).unwrap();
        fs::create_dir_all(&nvme1).unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&nvme0, dm_slaves.join("nvme0n1")).unwrap();
            std::os::unix::fs::symlink(&nvme1, dm_slaves.join("nvme1n1")).unwrap();
        }

        let ids = collect_block_device_ids(&dm).unwrap();
        assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec!["nvme0n1".to_string(), "nvme1n1".to_string()]);
    }

    #[test]
    fn detect_cross_device_sub_mounts() {
        let mounts = "\
/dev/root / ext4 rw 0 0
/dev/sdb1 /data ext4 rw 0 0
/dev/sdc1 /data/disk1/sub ext4 rw 0 0
";

        let err = check_cross_device_mounts_with_reader(&["/data/disk1".to_string()], mounts.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("Nested mount points detected under path"));
        assert!(err.to_string().contains("/data/disk1/sub"));
    }

    #[test]
    fn allow_mount_path_without_sub_mounts() {
        let mounts = "\
/dev/root / ext4 rw 0 0
/dev/sdb1 /data/disk1 ext4 rw 0 0
";

        check_cross_device_mounts_with_reader(&["/data/disk1".to_string()], mounts.as_bytes()).unwrap();
    }

    #[test]
    fn parse_mount_paths_decodes_escaped_spaces() {
        let mounts = "/dev/sdb1 /data/my\\040disk ext4 rw 0 0\n";

        let paths = parse_mount_paths(mounts);
        assert_eq!(paths, vec!["/data/my disk".to_string()]);
    }

    #[test]
    fn reject_relative_path_for_cross_device_validation() {
        let err = ensure_no_sub_mounts("relative/path", &[]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(err.to_string().contains("expected to be absolute"));
    }

    #[test]
    fn reject_root_path_for_cross_device_validation() {
        let err = ensure_no_sub_mounts("/", &[]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(err.to_string().contains("cannot be the filesystem root"));
    }

    #[test]
    fn fallback_to_major_minor_when_sysfs_link_missing() {
        let major = u64::MAX;
        let minor = u64::MAX;
        let ids = resolve_block_device_ids(major, minor).unwrap();
        assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![format!("{major}:{minor}")]);
    }

    #[test]
    fn calculate_space_usage_normal_bfree_greater_than_bavail() {
        // Typical ext4/xfs scenario: bavail < bfree due to reserved blocks
        let blocks = 1_000_u64;
        let bfree = 900_u64;
        let bavail = 850_u64;
        let bsize = 4_096_u64;

        let (total, free, used) = calculate_space_usage(blocks, bfree, bavail, bsize, "/data").unwrap();
        assert_eq!(total, blocks * bsize);
        assert_eq!(free, bavail * bsize);
        assert_eq!(used, (blocks - bavail) * bsize);
    }

    #[test]
    fn calculate_space_usage_bfree_equals_bavail() {
        // No reserved blocks: bfree == bavail (e.g. FAT/exFAT or root user)
        let blocks = 500_u64;
        let bfree = 400_u64;
        let bavail = 400_u64;
        let bsize = 4_096_u64;

        let (total, free, used) = calculate_space_usage(blocks, bfree, bavail, bsize, "/data").unwrap();
        assert_eq!(total, blocks * bsize);
        assert_eq!(free, bfree * bsize);
        assert_eq!(used, (blocks - bfree) * bsize);
    }

    #[test]
    fn calculate_space_usage_all_zero_blocks() {
        let (total, free, used) = calculate_space_usage(0, 0, 0, 4_096, "/data").unwrap();
        assert_eq!(total, 0);
        assert_eq!(free, 0);
        assert_eq!(used, 0);
    }

    #[test]
    fn calculate_space_usage_all_blocks_free() {
        let blocks = 1_000_u64;
        let bfree = 1_000_u64;
        let bavail = 1_000_u64;
        let bsize = 4_096_u64;

        let (total, free, used) = calculate_space_usage(blocks, bfree, bavail, bsize, "/data").unwrap();
        assert_eq!(total, blocks * bsize);
        assert_eq!(free, blocks * bsize);
        assert_eq!(used, 0);
    }

    #[test]
    fn calculate_space_usage_allows_bavail_greater_than_bfree() {
        let blocks = 1_000_u64;
        let bfree = 900_u64;
        let bavail = 920_u64;
        let bsize = 4_096_u64;

        let (total, free, used) = calculate_space_usage(blocks, bfree, bavail, bsize, "/data").unwrap();
        assert_eq!(total, blocks * bsize);
        assert_eq!(free, bfree * bsize);
        assert_eq!(used, total - free);
    }

    #[test]
    fn calculate_space_usage_rejects_free_greater_than_total() {
        let err = calculate_space_usage(100, 120, 120, 4_096, "/data").unwrap_err();
        assert!(err.to_string().contains("detected free space"));
    }

    #[test]
    fn bavail_greater_than_bfree_warning_is_once_per_path() {
        let path = "/data/rustfs-bavail-warning-once";

        assert!(should_warn_bavail_greater_than_bfree(path));
        assert!(!should_warn_bavail_greater_than_bfree(path));
        assert!(should_warn_bavail_greater_than_bfree("/data/rustfs-bavail-warning-other"));
    }
}
