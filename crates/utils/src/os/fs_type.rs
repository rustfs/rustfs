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

/// Returns the filesystem type of the underlying mounted filesystem.
///
/// TODO: Verify these less common magic values against stable Linux headers or
/// filesystem documentation before adding them:
///
/// "137d" => "EXT",
/// "4244" => "HFS",
/// "5346544e" => "NTFS",
/// "61756673" => "AUFS",
/// "ef51" => "EXT2OLD",
/// "2fc12fc1" => "zfs",
/// "53464846" => "wslfs",
pub(crate) fn get_fs_type(fs_type: u64) -> &'static str {
    // Magic numbers for various filesystems.
    match fs_type {
        0x01021994 => "TMPFS",
        0x4d44 => "MSDOS",
        0x6969 => "NFS",
        0xEF53 => "EXT4",
        0xf15f => "ecryptfs",
        0x794c7630 => "overlayfs",
        0x52654973 => "REISERFS",
        0x58465342 => "XFS",
        0x9123683E => "BTRFS",
        0x00c36400 => "CEPH",
        0x2011BAB0 => "EXFAT",
        0xE0F5E1E2 => "EROFS",
        0xF2F52010 => "F2FS",
        0x9660 => "ISOFS",
        0x65735546 => "FUSE",
        0x73717368 => "SQUASHFS",
        0xFF534D42 => "CIFS",
        0xFE534D42 => "SMB2",
        0xca451a4e => "BCACHEFS",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::get_fs_type;

    #[test]
    fn map_common_linux_filesystem_magic_numbers() {
        assert_eq!(get_fs_type(0x58465342), "XFS");
        assert_eq!(get_fs_type(0x9123683E), "BTRFS");
    }

    #[test]
    fn map_verified_linux_uapi_filesystem_magic_numbers() {
        assert_eq!(get_fs_type(0x00c36400), "CEPH");
        assert_eq!(get_fs_type(0x2011BAB0), "EXFAT");
        assert_eq!(get_fs_type(0xE0F5E1E2), "EROFS");
        assert_eq!(get_fs_type(0xF2F52010), "F2FS");
        assert_eq!(get_fs_type(0x9660), "ISOFS");
        assert_eq!(get_fs_type(0x65735546), "FUSE");
        assert_eq!(get_fs_type(0x73717368), "SQUASHFS");
        assert_eq!(get_fs_type(0xFF534D42), "CIFS");
        assert_eq!(get_fs_type(0xFE534D42), "SMB2");
        assert_eq!(get_fs_type(0xca451a4e), "BCACHEFS");
    }
}
