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

use crate::os::{DiskInfo, IOStats};
use std::io::{Error, ErrorKind};
use std::os::windows::ffi::OsStrExt;
use std::path::{Component, Path, Prefix};
use windows::Win32::Foundation::MAX_PATH;
use windows::Win32::Storage::FileSystem::{GetDiskFreeSpaceExW, GetDiskFreeSpaceW, GetVolumeInformationW, GetVolumePathNameW};
use windows::core::PCWSTR;

/// Documented maximum for the `GetVolumeInformationW` name buffers.
const VOLUME_INFORMATION_BUFFER_LEN: usize = MAX_PATH as usize + 1;

const BACKSLASH: u16 = b'\\' as u16;
const SLASH: u16 = b'/' as u16;

/// A NUL-terminated UTF-16 path that is safe to hand to Win32 as a `PCWSTR`.
#[derive(Debug, PartialEq, Eq)]
struct WidePath(Vec<u16>);

impl WidePath {
    /// Terminates `units` with NUL, rejecting interior NULs that would silently
    /// truncate the path seen by Win32.
    fn new(mut units: Vec<u16>) -> std::io::Result<Self> {
        if units.contains(&0) {
            return Err(Error::new(ErrorKind::InvalidInput, "path contains an interior NUL character"));
        }
        units.push(0);
        Ok(Self(units))
    }

    /// Returns `path` in the extended-length (`\\?\` or `\\?\UNC\`) form, so
    /// the volume APIs keep working beyond `MAX_PATH`.
    ///
    /// The path is first made absolute with `GetFullPathNameW` (via
    /// [`std::path::absolute`]) so separators, `.`/`..` and trailing dots or
    /// spaces are normalized as a non-verbatim Win32 call would see them.
    /// Verbatim and `\\.\` device paths are kept unchanged.
    fn extended(path: &Path) -> std::io::Result<Self> {
        let absolute = std::path::absolute(path)?;
        let wide = absolute.as_os_str().encode_wide().collect::<Vec<_>>();
        let (prefix, rest) = match absolute.components().next() {
            Some(Component::Prefix(prefix)) => match prefix.kind() {
                Prefix::Disk(_) => (r"\\?\", &wide[..]),
                // `\\server\share\x` becomes `\\?\UNC\server\share\x`.
                Prefix::UNC(..) => (r"\\?\UNC", &wide[1..]),
                _ => return Self::new(wide),
            },
            _ => return Self::new(wide),
        };
        let mut extended = prefix.encode_utf16().collect::<Vec<_>>();
        extended.extend_from_slice(rest);
        Self::new(extended)
    }

    /// Like [`WidePath::extended`], with the trailing backslash that
    /// directory APIs such as `GetDiskFreeSpaceExW` require for UNC paths.
    fn directory(path: &Path) -> std::io::Result<Self> {
        let mut path = Self::extended(path)?;
        path.0.pop();
        if !matches!(path.0.last(), Some(&BACKSLASH | &SLASH)) {
            path.0.push(BACKSLASH);
        }
        path.0.push(0);
        Ok(path)
    }

    fn units(&self) -> &[u16] {
        &self.0[..self.0.len() - 1]
    }

    fn as_pcwstr(&self) -> PCWSTR {
        PCWSTR::from_raw(self.0.as_ptr())
    }
}

/// Returns total and free bytes available in a directory, e.g. `C:\`.
// SAFETY: Windows API calls receive NUL-terminated UTF-16 paths and valid
// pointers to initialized stack output variables.
#[allow(unsafe_code)]
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
    let directory = WidePath::directory(p.as_ref())?;

    let mut free_bytes_available = 0u64;
    let mut total_number_of_bytes = 0u64;
    let mut total_number_of_free_bytes = 0u64;

    // The directory itself, not its volume root, is queried so per-directory
    // quotas (e.g. FSRM) are reflected in the reported capacity.
    // SAFETY:
    // 1. `directory` is a NUL-terminated UTF-16 string owned for the whole call.
    // 2. Pointers to `u64` variables are valid and point to initialized stack memory.
    unsafe {
        GetDiskFreeSpaceExW(
            directory.as_pcwstr(),
            Some(&mut free_bytes_available),
            Some(&mut total_number_of_bytes),
            Some(&mut total_number_of_free_bytes),
        )
        .map_err(win32_error)?;
    }

    let total = total_number_of_bytes;
    let free = free_bytes_available;

    if free > total {
        return Err(Error::other(format!(
            "detected free space ({free}) > total drive space ({total}), fs corruption at ({}). please run 'fsck'",
            p.as_ref().display()
        )));
    }

    // `GetDiskFreeSpaceW` and `GetVolumeInformationW` take a volume root, not
    // an arbitrary directory.
    let volume = get_volume_path(&directory)?;

    let mut sectors_per_cluster = 0u32;
    let mut bytes_per_sector = 0u32;
    let mut number_of_free_clusters = 0u32;
    let mut total_number_of_clusters = 0u32;

    // SAFETY:
    // 1. `volume` is a NUL-terminated UTF-16 string owned for the whole call.
    // 2. Pointers to `u32` variables are valid and point to initialized stack memory.
    unsafe {
        GetDiskFreeSpaceW(
            volume.as_pcwstr(),
            Some(&mut sectors_per_cluster),
            Some(&mut bytes_per_sector),
            Some(&mut number_of_free_clusters),
            Some(&mut total_number_of_clusters),
        )
        .map_err(win32_error)?;
    }

    Ok(DiskInfo {
        total,
        free,
        used: total - free,
        files: total_number_of_clusters as u64,
        ffree: number_of_free_clusters as u64,
        fstype: get_volume_information(&volume)
            .map(|info| info.file_system_name)
            .unwrap_or_default(),
        ..Default::default()
    })
}

struct VolumeInformation {
    serial_number: u32,
    file_system_name: String,
}

// SAFETY: `GetVolumeInformationW` receives a NUL-terminated volume root and a
// slice whose length the bindings pass as the buffer size.
#[allow(unsafe_code)]
fn get_volume_information(volume: &WidePath) -> std::io::Result<VolumeInformation> {
    let mut serial_number = 0u32;
    let mut file_system_name = [0u16; VOLUME_INFORMATION_BUFFER_LEN];

    // SAFETY:
    // 1. `volume` is a NUL-terminated UTF-16 volume root with a trailing backslash,
    //    as returned by `GetVolumePathNameW`.
    // 2. `file_system_name` holds the documented maximum of `MAX_PATH + 1` units and
    //    its length is passed alongside the pointer.
    // 3. The serial number pointer targets an initialized stack variable.
    unsafe {
        GetVolumeInformationW(
            volume.as_pcwstr(),
            None,
            Some(&mut serial_number),
            None,
            None,
            Some(&mut file_system_name),
        )
        .map_err(win32_error)?;
    }

    Ok(VolumeInformation {
        serial_number,
        file_system_name: utf16_to_string(&file_system_name),
    })
}

/// Returns the NUL-terminated root of the volume that contains `path`, e.g.
/// `\\?\C:\` or a mounted folder such as `\\?\C:\mnt\disk1\`.
// SAFETY: `GetVolumePathNameW` receives a NUL-terminated path and a writable
// slice whose length the bindings pass as the buffer size.
#[allow(unsafe_code)]
fn get_volume_path(path: &WidePath) -> std::io::Result<WidePath> {
    // The volume path is a prefix of the full input path plus a trailing
    // backslash, so the input length (including its NUL) plus one always fits.
    let mut buffer = vec![0u16; (path.0.len() + 1).max(MAX_PATH as usize + 1)];

    // SAFETY:
    // 1. `path` is a NUL-terminated UTF-16 string owned for the whole call.
    // 2. `buffer` is writable and its length is passed as `cchBufferLength`.
    unsafe {
        GetVolumePathNameW(path.as_pcwstr(), &mut buffer).map_err(win32_error)?;
    }

    let len = buffer.iter().position(|&x| x == 0).unwrap_or(buffer.len());
    buffer.truncate(len);
    WidePath::new(buffer)
}

/// Returns the identity of the volume containing `path` in its conventional
/// form (`C:\`, `\\server\share\`), independent of whether the caller used
/// an extended-length path.
fn volume_id(path: &str) -> std::io::Result<String> {
    let volume = get_volume_path(&WidePath::extended(Path::new(path))?)?;
    Ok(String::from_utf16_lossy(&simplify_extended(volume.units())))
}

/// Strips the `\\?\` prefix from a drive path and maps `\\?\UNC\` back to
/// `\\`; other verbatim paths (e.g. `\\?\Volume{GUID}\`) are returned as is.
fn simplify_extended(units: &[u16]) -> std::borrow::Cow<'_, [u16]> {
    const VERBATIM: [u16; 4] = [BACKSLASH, BACKSLASH, b'?' as u16, BACKSLASH];
    let Some(rest) = units.strip_prefix(&VERBATIM[..]) else {
        return units.into();
    };
    if let [drive, colon, ..] = rest
        && *colon == b':' as u16
        && char::from_u32(u32::from(*drive)).is_some_and(|c| c.is_ascii_alphabetic())
    {
        return rest.into();
    }
    if let [u, n, c, BACKSLASH, share @ ..] = rest
        && String::from_utf16_lossy(&[*u, *n, *c]).eq_ignore_ascii_case("UNC")
    {
        let mut simplified = vec![BACKSLASH, BACKSLASH];
        simplified.extend_from_slice(share);
        return simplified.into();
    }
    units.into()
}

/// Converts a failed `windows` call into an `io::Error` carrying the Win32
/// error code, so `raw_os_error()` and `kind()` match `std::fs` errors.
fn win32_error(err: windows::core::Error) -> Error {
    const FACILITY_WIN32_MASK: u32 = 0xFFFF_0000;
    const FACILITY_WIN32_FAILURE: u32 = 0x8007_0000;

    let hresult = err.code().0 as u32;
    if hresult & FACILITY_WIN32_MASK == FACILITY_WIN32_FAILURE {
        Error::from_raw_os_error((hresult & !FACILITY_WIN32_MASK) as i32)
    } else {
        Error::other(err)
    }
}

fn utf16_to_string(v: &[u16]) -> String {
    let len = v.iter().position(|&x| x == 0).unwrap_or(v.len());
    String::from_utf16_lossy(&v[..len])
}

pub fn same_disk(disk1: &str, disk2: &str) -> std::io::Result<bool> {
    Ok(volume_id(disk1)? == volume_id(disk2)?)
}

/// Automatic replacement formatting is disabled until Windows has an
/// equivalent mount identity probe.
pub fn is_mount_point(_path: &std::path::Path) -> std::io::Result<bool> {
    Ok(false)
}

pub fn get_physical_device_ids(disk: &str) -> std::io::Result<Vec<String>> {
    Ok(vec![volume_id(disk)?])
}

/// Returns the volume serial number for the filesystem backing `path`.
///
/// This is the Windows equivalent of Linux's `st_dev` major:minor pair and is
/// useful for diagnostic purposes when validating physical disk independence.
pub fn get_volume_serial_number(path: &str) -> std::io::Result<u32> {
    let volume = get_volume_path(&WidePath::extended(Path::new(path))?)?;
    Ok(get_volume_information(&volume)?.serial_number)
}

pub fn check_cross_device_mounts(_paths: &[String]) -> std::io::Result<()> {
    Ok(())
}

pub fn get_drive_stats(_major: u32, _minor: u32) -> std::io::Result<IOStats> {
    Ok(IOStats::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().collect()
    }

    /// Creates a directory whose absolute path is well beyond `MAX_PATH`.
    fn long_directory(root: &Path) -> PathBuf {
        let mut dir = root.to_path_buf();
        for i in 0..6 {
            dir.push(format!("{i}-{}", "a".repeat(60)));
        }
        std::fs::create_dir_all(&dir).expect("std::fs creates long directories itself");
        assert!(dir.as_os_str().len() > MAX_PATH as usize);
        dir
    }

    #[test]
    fn windows_wide_path_is_nul_terminated_extended_length() {
        let disk = WidePath::extended(Path::new(r"C:/data/./bucket/../bucket")).unwrap();
        assert_eq!(disk.units(), wide(r"\\?\C:\data\bucket"));
        assert_eq!(disk.0.last(), Some(&0));

        let unc = WidePath::extended(Path::new(r"\\server\share\data")).unwrap();
        assert_eq!(unc.units(), wide(r"\\?\UNC\server\share\data"));

        for unchanged in [r"\\?\C:\data", r"\\?\UNC\server\share\data", r"\\.\C:\data"] {
            assert_eq!(WidePath::extended(Path::new(unchanged)).unwrap().units(), wide(unchanged));
        }

        let relative = WidePath::extended(Path::new("relative")).unwrap();
        assert!(relative.units().starts_with(&wide(r"\\?\")), "relative paths are made absolute first");
    }

    #[test]
    fn windows_directory_path_has_single_trailing_backslash() {
        // A UNC share root without the trailing backslash is rejected by
        // `GetDiskFreeSpaceExW`.
        let share = WidePath::directory(Path::new(r"\\server\share")).unwrap();
        assert_eq!(share.units(), wide(r"\\?\UNC\server\share\"));
        let root = WidePath::directory(Path::new(r"C:\")).unwrap();
        assert_eq!(root.units(), wide(r"\\?\C:\"));
        assert_eq!(root.0.last(), Some(&0));
    }

    #[test]
    fn windows_wide_path_rejects_interior_nul() {
        let err = WidePath::new(wide("C:\\a\0b")).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn windows_simplify_extended_restores_conventional_volume_ids() {
        for (input, expected) in [
            (r"\\?\C:\", r"C:\"),
            (r"\\?\UNC\server\share\", r"\\server\share\"),
            (r"\\?\unc\server\share\", r"\\server\share\"),
            (
                r"\\?\Volume{0b6e1f0a-0000-0000-0000-100000000000}\",
                r"\\?\Volume{0b6e1f0a-0000-0000-0000-100000000000}\",
            ),
            (r"C:\", r"C:\"),
        ] {
            assert_eq!(&*simplify_extended(&wide(input)), wide(expected).as_slice(), "{input}");
        }
    }

    #[test]
    fn windows_win32_error_keeps_os_error_code() {
        // HRESULT_FROM_WIN32(ERROR_PATH_NOT_FOUND)
        let err = win32_error(windows::core::Error::from_hresult(windows::core::HRESULT(0x8007_0003_u32 as i32)));
        assert_eq!(err.raw_os_error(), Some(3));
        assert_eq!(err.kind(), ErrorKind::NotFound);

        let missing = get_info(r"Z:\rustfs-missing\path").unwrap_err();
        assert!(missing.raw_os_error().is_some_and(|code| code < 0x1_0000), "{missing:?}");
    }

    #[test]
    fn windows_get_info_reports_fstype() {
        let temp_dir = tempfile::tempdir().unwrap();
        let info = get_info(temp_dir.path()).unwrap();
        assert!(info.total > 0);
        assert!(!info.fstype.is_empty(), "GetVolumeInformationW must name the filesystem");

        let ids = get_physical_device_ids(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(ids.len(), 1);
        assert!(!ids[0].starts_with(r"\\?\"), "volume ids keep their conventional form: {ids:?}");
        assert!(ids[0].ends_with('\\'), "{ids:?}");
    }

    #[test]
    fn windows_volume_apis_accept_paths_beyond_max_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let short = temp_dir.path();
        let long = long_directory(short);
        let (short_str, long_str) = (short.to_str().unwrap(), long.to_str().unwrap());

        let short_info = get_info(short).unwrap();
        let long_info = get_info(&long).unwrap();
        assert!(long_info.total > 0);
        assert!(!long_info.fstype.is_empty());
        assert_eq!(long_info.fstype, short_info.fstype);
        assert_eq!(long_info.files, short_info.files);

        assert!(same_disk(short_str, long_str).unwrap());
        assert_eq!(get_physical_device_ids(long_str).unwrap(), get_physical_device_ids(short_str).unwrap());
        assert_eq!(get_volume_serial_number(long_str).unwrap(), get_volume_serial_number(short_str).unwrap());

        let verbatim = format!(r"\\?\{long_str}");
        assert_eq!(get_physical_device_ids(&verbatim).unwrap(), get_physical_device_ids(short_str).unwrap());
    }

    #[test]
    fn windows_get_info_accepts_unc_share_root_without_trailing_backslash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let absolute = std::path::absolute(temp_dir.path()).unwrap();
        let local = absolute.to_str().unwrap();
        let Some((drive, rest)) = local.split_once(":\\") else {
            eprintln!("skipping: temp dir {local} is not on a drive letter");
            return;
        };
        let share = format!(r"\\localhost\{drive}$");
        if !Path::new(&share).is_dir() {
            eprintln!("skipping: administrative share {share} is not reachable");
            return;
        }

        let share_info = get_info(&share).unwrap();
        assert!(share_info.total > 0);
        assert!(!share_info.fstype.is_empty());

        let long = long_directory(&PathBuf::from(format!(r"{share}\{rest}")));
        let long_info = get_info(&long).unwrap();
        assert!(!long_info.fstype.is_empty());
        assert!(same_disk(&share, long.to_str().unwrap()).unwrap());
    }
}
