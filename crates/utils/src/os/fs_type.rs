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
/// "ff534d42" => "cifs",
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
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_common_linux_filesystem_magic_numbers() {
        assert_eq!(get_fs_type(0x58465342), "XFS");
        assert_eq!(get_fs_type(0x9123683E), "BTRFS");
    }
}
