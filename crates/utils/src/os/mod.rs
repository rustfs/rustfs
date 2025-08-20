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

#[cfg(target_os = "linux")]
mod linux;
#[cfg(all(unix, not(target_os = "linux")))]
mod unix;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
pub use linux::{get_drive_stats, get_info, same_disk};
// pub use linux::same_disk;

#[cfg(all(unix, not(target_os = "linux")))]
pub use unix::{get_drive_stats, get_info, same_disk};
#[cfg(target_os = "windows")]
pub use windows::{get_drive_stats, get_info, same_disk};

#[derive(Debug, Default, PartialEq)]
pub struct IOStats {
    pub read_ios: u64,
    pub read_merges: u64,
    pub read_sectors: u64,
    pub read_ticks: u64,
    pub write_ios: u64,
    pub write_merges: u64,
    pub write_sectors: u64,
    pub write_ticks: u64,
    pub current_ios: u64,
    pub total_ticks: u64,
    pub req_ticks: u64,
    pub discard_ios: u64,
    pub discard_merges: u64,
    pub discard_sectors: u64,
    pub discard_ticks: u64,
    pub flush_ios: u64,
    pub flush_ticks: u64,
}

#[derive(Debug, Default, PartialEq)]
pub struct DiskInfo {
    pub total: u64,
    pub free: u64,
    pub used: u64,
    pub files: u64,
    pub ffree: u64,
    pub fstype: String,
    pub major: u64,
    pub minor: u64,
    pub name: String,
    pub rotational: bool,
    pub nrrequests: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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

    #[test]
    fn test_get_info_invalid_path() {
        let invalid_path = PathBuf::from("/invalid/path");
        let result = get_info(&invalid_path);

        assert!(result.is_err());
    }

    #[test]
    fn test_same_disk_same_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let result = same_disk(path, path).unwrap();
        assert!(result);
    }

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

    #[ignore] // FIXME: failed in github actions
    #[test]
    fn test_get_drive_stats_default() {
        let stats = get_drive_stats(0, 0).unwrap();
        assert_eq!(stats, IOStats::default());
    }
}
