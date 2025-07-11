#![allow(unsafe_code)] // TODO: audit unsafe code
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
use std::io::Error;
use std::mem;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use winapi::shared::minwindef::{DWORD, MAX_PATH};
use winapi::shared::ntdef::ULARGE_INTEGER;
use winapi::um::fileapi::{GetDiskFreeSpaceExW, GetDiskFreeSpaceW, GetVolumeInformationW, GetVolumePathNameW};
use winapi::um::winnt::{LPCWSTR, WCHAR};

/// Returns total and free bytes available in a directory, e.g. `C:\`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
    let path_display = p.as_ref().display();
    let path_wide: Vec<WCHAR> = p
        .as_ref()
        .to_path_buf()
        .into_os_string()
        .encode_wide()
        .chain(std::iter::once(0)) // Null-terminate the string
        .collect();

    let mut lp_free_bytes_available: ULARGE_INTEGER = unsafe { mem::zeroed() };
    let mut lp_total_number_of_bytes: ULARGE_INTEGER = unsafe { mem::zeroed() };
    let mut lp_total_number_of_free_bytes: ULARGE_INTEGER = unsafe { mem::zeroed() };

    let success = unsafe {
        GetDiskFreeSpaceExW(
            path_wide.as_ptr(),
            &mut lp_free_bytes_available,
            &mut lp_total_number_of_bytes,
            &mut lp_total_number_of_free_bytes,
        )
    };
    if success == 0 {
        return Err(Error::last_os_error());
    }

    let total = unsafe { *lp_total_number_of_bytes.QuadPart() };
    let free = unsafe { *lp_total_number_of_free_bytes.QuadPart() };

    if free > total {
        return Err(Error::other(format!(
            "detected free space ({free}) > total drive space ({total}), fs corruption at ({path_display}). please run 'fsck'"
        )));
    }

    let mut lp_sectors_per_cluster: DWORD = 0;
    let mut lp_bytes_per_sector: DWORD = 0;
    let mut lp_number_of_free_clusters: DWORD = 0;
    let mut lp_total_number_of_clusters: DWORD = 0;

    let success = unsafe {
        GetDiskFreeSpaceW(
            path_wide.as_ptr(),
            &mut lp_sectors_per_cluster,
            &mut lp_bytes_per_sector,
            &mut lp_number_of_free_clusters,
            &mut lp_total_number_of_clusters,
        )
    };
    if success == 0 {
        return Err(Error::last_os_error());
    }

    Ok(DiskInfo {
        total,
        free,
        used: total - free,
        files: lp_total_number_of_clusters as u64,
        ffree: lp_number_of_free_clusters as u64,

        // TODO This field is currently unused, and since this logic causes a
        // NotFound error during startup on Windows systems, it has been commented out here
        //
        // The error occurs in GetVolumeInformationW where the path parameter
        // is of type [WCHAR; MAX_PATH]. For a drive letter, there are excessive
        // trailing zeros, which causes the failure here.
        //
        // fstype: get_fs_type(&path_wide)?,
        ..Default::default()
    })
}

/// Returns leading volume name.
#[allow(dead_code)]
fn get_volume_name(v: &[WCHAR]) -> std::io::Result<LPCWSTR> {
    let volume_name_size: DWORD = MAX_PATH as _;
    let mut lp_volume_name_buffer: [WCHAR; MAX_PATH] = [0; MAX_PATH];

    let success = unsafe { GetVolumePathNameW(v.as_ptr(), lp_volume_name_buffer.as_mut_ptr(), volume_name_size) };

    if success == 0 {
        return Err(Error::last_os_error());
    }

    Ok(lp_volume_name_buffer.as_ptr())
}

#[allow(dead_code)]
fn utf16_to_string(v: &[WCHAR]) -> String {
    let len = v.iter().position(|&x| x == 0).unwrap_or(v.len());
    String::from_utf16_lossy(&v[..len])
}

/// Returns the filesystem type of the underlying mounted filesystem
#[allow(dead_code)]
fn get_fs_type(p: &[WCHAR]) -> std::io::Result<String> {
    let path = get_volume_name(p)?;

    let volume_name_size: DWORD = MAX_PATH as _;
    let n_file_system_name_size: DWORD = MAX_PATH as _;

    let mut lp_volume_serial_number: DWORD = 0;
    let mut lp_maximum_component_length: DWORD = 0;
    let mut lp_file_system_flags: DWORD = 0;

    let mut lp_volume_name_buffer: [WCHAR; MAX_PATH] = [0; MAX_PATH];
    let mut lp_file_system_name_buffer: [WCHAR; MAX_PATH] = [0; MAX_PATH];

    let success = unsafe {
        GetVolumeInformationW(
            path,
            lp_volume_name_buffer.as_mut_ptr(),
            volume_name_size,
            &mut lp_volume_serial_number,
            &mut lp_maximum_component_length,
            &mut lp_file_system_flags,
            lp_file_system_name_buffer.as_mut_ptr(),
            n_file_system_name_size,
        )
    };

    if success == 0 {
        return Err(Error::last_os_error());
    }

    Ok(utf16_to_string(&lp_file_system_name_buffer))
}

pub fn same_disk(_disk1: &str, _disk2: &str) -> std::io::Result<bool> {
    Ok(false)
}

pub fn get_drive_stats(_major: u32, _minor: u32) -> std::io::Result<IOStats> {
    Ok(IOStats::default())
}
