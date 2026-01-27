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

#![allow(unsafe_code)] // TODO: audit unsafe code

use crate::os::{DiskInfo, IOStats};
use std::io::Error;
use std::path::Path;
use windows::Win32::Foundation::MAX_PATH;
use windows::Win32::Storage::FileSystem::{GetDiskFreeSpaceExW, GetDiskFreeSpaceW, GetVolumeInformationW, GetVolumePathNameW};

/// Returns total and free bytes available in a directory, e.g. `C:\`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
    let path_wide = p
        .as_ref()
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<u16>>();

    let mut free_bytes_available = 0u64;
    let mut total_number_of_bytes = 0u64;
    let mut total_number_of_free_bytes = 0u64;

    unsafe {
        GetDiskFreeSpaceExW(
            windows::core::PCWSTR::from_raw(path_wide.as_ptr()),
            Some(&mut free_bytes_available),
            Some(&mut total_number_of_bytes),
            Some(&mut total_number_of_free_bytes),
        )
        .map_err(|e| Error::from_raw_os_error(e.code().0 as i32))?;
    }

    let total = total_number_of_bytes;
    let free = free_bytes_available;

    if free > total {
        return Err(Error::other(format!(
            "detected free space ({free}) > total drive space ({total}), fs corruption at ({}). please run 'fsck'",
            p.as_ref().display()
        )));
    }

    let mut sectors_per_cluster = 0u32;
    let mut bytes_per_sector = 0u32;
    let mut number_of_free_clusters = 0u32;
    let mut total_number_of_clusters = 0u32;

    unsafe {
        GetDiskFreeSpaceW(
            windows::core::PCWSTR::from_raw(path_wide.as_ptr()),
            Some(&mut sectors_per_cluster),
            Some(&mut bytes_per_sector),
            Some(&mut number_of_free_clusters),
            Some(&mut total_number_of_clusters),
        )
        .map_err(|e| Error::from_raw_os_error(e.code().0 as i32))?;
    }

    Ok(DiskInfo {
        total,
        free,
        used: total - free,
        files: total_number_of_clusters as u64,
        ffree: number_of_free_clusters as u64,
        fstype: get_fs_type(&path_wide).unwrap_or_default(),
        ..Default::default()
    })
}

/// Returns leading volume name.
///
/// # Arguments
/// * `v` - A slice of u16 representing the path in UTF-16 encoding
///
/// # Returns
/// * `Ok(Vec<u16>)` containing the volume name in UTF-16 encoding.
/// * `Err` if an error occurs during the operation.
#[allow(dead_code)]
fn get_volume_name(v: &[u16]) -> std::io::Result<Vec<u16>> {
    let mut volume_name_buffer = [0u16; MAX_PATH as usize];

    unsafe {
        GetVolumePathNameW(windows::core::PCWSTR::from_raw(v.as_ptr()), &mut volume_name_buffer)
            .map_err(|e| Error::from_raw_os_error(e.code().0 as i32))?;
    }

    let len = volume_name_buffer
        .iter()
        .position(|&x| x == 0)
        .unwrap_or(volume_name_buffer.len());
    Ok(volume_name_buffer[..len].to_vec())
}

#[allow(dead_code)]
fn utf16_to_string(v: &[u16]) -> String {
    let len = v.iter().position(|&x| x == 0).unwrap_or(v.len());
    String::from_utf16_lossy(&v[..len])
}

/// Returns the filesystem type of the underlying mounted filesystem
///
/// # Arguments
/// * `p` - A slice of u16 representing the path in UTF-16 encoding
///
/// # Returns
/// * `Ok(String)` containing the filesystem type (e.g., "NTFS", "FAT32").
/// * `Err` if an error occurs during the operation.
#[allow(dead_code)]
fn get_fs_type(p: &[u16]) -> std::io::Result<String> {
    let path = get_volume_name(p)?;

    let mut volume_serial_number = 0u32;
    let mut maximum_component_length = 0u32;
    let mut file_system_flags = 0u32;
    let mut volume_name_buffer = [0u16; MAX_PATH as usize];
    let mut file_system_name_buffer = [0u16; MAX_PATH as usize];

    unsafe {
        GetVolumeInformationW(
            windows::core::PCWSTR::from_raw(path.as_ptr()),
            Some(&mut volume_name_buffer),
            Some(&mut volume_serial_number),
            Some(&mut maximum_component_length),
            Some(&mut file_system_flags),
            Some(&mut file_system_name_buffer),
        )
        .map_err(|e| Error::from_raw_os_error(e.code().0 as i32))?;
    }

    Ok(utf16_to_string(&file_system_name_buffer))
}

/// Determines if two paths are on the same disk.
///
/// # Arguments
/// * `disk1` - The first disk path as a string slice.
/// * `disk2` - The second disk path as a string slice.
///
/// # Returns
/// * `Ok(true)` if both paths are on the same disk.
/// * `Ok(false)` if both paths are on different disks.
/// * `Err` if an error occurs during the operation.
pub fn same_disk(disk1: &str, disk2: &str) -> std::io::Result<bool> {
    let path1_wide: Vec<u16> = disk1.encode_utf16().chain(std::iter::once(0)).collect();
    let path2_wide: Vec<u16> = disk2.encode_utf16().chain(std::iter::once(0)).collect();

    let volume1 = get_volume_name(&path1_wide)?;
    let volume2 = get_volume_name(&path2_wide)?;

    Ok(volume1 == volume2)
}

/// Retrieves I/O statistics for a drive identified by its major and minor numbers.
///
/// # Arguments
/// * `major` - The major number of the drive.
/// * `minor` - The minor number of the drive.
///
/// # Returns
/// * `Ok(IOStats)` containing the I/O statistics.
/// * `Err` if an error occurs during the operation.
pub fn get_drive_stats(_major: u32, _minor: u32) -> std::io::Result<IOStats> {
    // Windows does not provide direct IO stats via simple API; this is a stub
    // For full implementation, consider using PDH or WMI, but that adds complexity
    Ok(IOStats::default())
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "windows")]
    #[test]
    fn test_get_info_valid_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let info = get_info(temp_dir.path()).unwrap();

        // Verify disk info is valid
        assert!(info.total > 0);
        assert!(info.free > 0);
        assert!(info.used > 0);
        assert!(info.files > 0);
        assert!(info.ffree > 0);
        assert!(!info.fstype.is_empty());
    }
    #[cfg(target_os = "windows")]
    #[test]
    fn test_get_info_invalid_path() {
        use std::path::PathBuf;
        let invalid_path = PathBuf::from("Z:\\invalid\\path");
        let result = get_info(&invalid_path);

        assert!(result.is_err());
    }
    #[cfg(target_os = "windows")]
    #[test]
    fn test_same_disk_same_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let result = same_disk(path, path).unwrap();
        assert!(result);
    }
    #[cfg(target_os = "windows")]
    #[test]
    fn test_same_disk_different_paths() {
        let temp_dir1 = tempfile::tempdir().unwrap();
        let temp_dir2 = tempfile::tempdir().unwrap();

        let path1 = temp_dir1.path().to_str().unwrap();
        let path2 = temp_dir2.path().to_str().unwrap();

        let _result = same_disk(path1, path2).unwrap();
        // Since both temporary directories are created in the same file system,
        // they should be on the same disk in most cases
        // Test passes if the function doesn't panic - the actual result depends on test environment
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn get_info_with_root_drive() {
        let info = get_info("C:\\").unwrap();
        assert!(info.total > 0);
        assert!(info.free > 0);
        assert!(info.used > 0);
        assert!(info.files > 0);
        assert!(info.ffree > 0);
        assert!(!info.fstype.is_empty());
    }
}
