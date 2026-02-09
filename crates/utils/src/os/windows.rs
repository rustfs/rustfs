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
    let path_wide = to_wide_path(p.as_ref());

    let mut free_bytes_available = 0u64;
    let mut total_number_of_bytes = 0u64;
    let mut total_number_of_free_bytes = 0u64;

    // SAFETY:
    // 1. `path_wide` is a valid null-terminated UTF-16 string.
    // 2. Pointers to `u64` variables are valid and point to initialized stack memory.
    unsafe {
        GetDiskFreeSpaceExW(
            windows::core::PCWSTR::from_raw(path_wide.as_ptr()),
            Some(&mut free_bytes_available),
            Some(&mut total_number_of_bytes),
            Some(&mut total_number_of_free_bytes),
        )
        .map_err(|e| Error::from_raw_os_error(e.code().0))?;
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

    // SAFETY:
    // 1. `path_wide` is a valid null-terminated UTF-16 string.
    // 2. Pointers to `u32` variables are valid and point to initialized stack memory.
    unsafe {
        GetDiskFreeSpaceW(
            windows::core::PCWSTR::from_raw(path_wide.as_ptr()),
            Some(&mut sectors_per_cluster),
            Some(&mut bytes_per_sector),
            Some(&mut number_of_free_clusters),
            Some(&mut total_number_of_clusters),
        )
        .map_err(|e| Error::from_raw_os_error(e.code().0))?;
    }

    Ok(DiskInfo {
        total,
        free,
        used: total - free,
        files: total_number_of_clusters as u64,
        ffree: number_of_free_clusters as u64,
        fstype: get_windows_fs_type(&path_wide).unwrap_or_default(),
        ..Default::default()
    })
}

fn get_windows_fs_type(p: &[u16]) -> std::io::Result<String> {
    let path = get_volume_name(p)?;

    let mut volume_serial_number = 0u32;
    let mut maximum_component_length = 0u32;
    let mut file_system_flags = 0u32;
    let mut volume_name_buffer = [0u16; MAX_PATH as usize];
    let mut file_system_name_buffer = [0u16; MAX_PATH as usize];

    // SAFETY:
    // 1. `path` is a valid null-terminated UTF-16 string (volume root path).
    // 2. Buffers are allocated with `MAX_PATH` size, which is sufficient for standard Windows paths.
    // 3. Pointers to output variables are valid.
    unsafe {
        GetVolumeInformationW(
            windows::core::PCWSTR::from_raw(path.as_ptr()),
            Some(&mut volume_name_buffer),
            Some(&mut volume_serial_number),
            Some(&mut maximum_component_length),
            Some(&mut file_system_flags),
            Some(&mut file_system_name_buffer),
        )
        .map_err(|e| Error::from_raw_os_error(e.code().0))?;
    }

    Ok(utf16_to_string(&file_system_name_buffer))
}

fn get_volume_name(v: &[u16]) -> std::io::Result<Vec<u16>> {
    let mut volume_name_buffer = [0u16; MAX_PATH as usize];

    // SAFETY:
    // 1. `v` is a valid null-terminated UTF-16 string.
    // 2. `volume_name_buffer` is allocated with `MAX_PATH` size.
    // 3. `GetVolumePathNameW` writes to the buffer and respects the buffer size (implicitly MAX_PATH for this API context usually, though explicit length param isn't present, it expects a buffer large enough).
    // Note: GetVolumePathNameW documentation says "The buffer should be large enough to hold the path". MAX_PATH is generally safe for volume roots.
    unsafe {
        GetVolumePathNameW(windows::core::PCWSTR::from_raw(v.as_ptr()), &mut volume_name_buffer)
            .map_err(|e| Error::from_raw_os_error(e.code().0))?;
    }

    let len = volume_name_buffer
        .iter()
        .position(|&x| x == 0)
        .unwrap_or(volume_name_buffer.len());
    Ok(volume_name_buffer[..len].to_vec())
}

fn utf16_to_string(v: &[u16]) -> String {
    let len = v.iter().position(|&x| x == 0).unwrap_or(v.len());
    String::from_utf16_lossy(&v[..len])
}

fn to_wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(std::iter::once(0)).collect()
}

// Helper trait to access encode_wide which is only available on Windows
use std::os::windows::ffi::OsStrExt;

pub fn same_disk(disk1: &str, disk2: &str) -> std::io::Result<bool> {
    let path1_wide = to_wide_path(Path::new(disk1));
    let path2_wide = to_wide_path(Path::new(disk2));

    let volume1 = get_volume_name(&path1_wide)?;
    let volume2 = get_volume_name(&path2_wide)?;

    Ok(volume1 == volume2)
}

pub fn get_drive_stats(_major: u32, _minor: u32) -> std::io::Result<IOStats> {
    Ok(IOStats::default())
}
