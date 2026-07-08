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
#[allow(dead_code)]
pub(crate) fn get_fs_type(fs_type: u64) -> &'static str {
    // Magic numbers for various filesystems.
    match fs_type {
        0xadf5 => "ADFS",
        0xadff => "AFFS",
        0x5346414F | 0x6B414653 => "AFS",
        0x0187 => "AUTOFS",
        0x01021994 => "TMPFS",
        0x4d44 => "MSDOS",
        0x6969 => "NFS",
        0xEF53 => "EXT4",
        0xf15f => "ecryptfs",
        0x794c7630 => "overlayfs",
        0x52654973 => "REISERFS",
        0x73757245 => "CODA",
        0x28cd3d45 | 0x453dcd28 => "CRAMFS",
        0x64626720 => "DEBUGFS",
        0x73636673 => "SECURITYFS",
        0xf97cff8c => "SELINUXFS",
        0x43415d53 => "SMACKFS",
        0x858458f6 => "RAMFS",
        0x958458f6 => "HUGETLBFS",
        0x58465342 => "XFS",
        0x9123683E => "BTRFS",
        0x00c36400 => "CEPH",
        0x2011BAB0 => "EXFAT",
        0x414A53 => "EFS",
        0xE0F5E1E2 => "EROFS",
        0xF2F52010 => "F2FS",
        0x3434 => "NILFS2",
        0xf995e849 => "HPFS",
        0x9660 => "ISOFS",
        0x72b6 => "JFFS2",
        0x65735546 => "FUSE",
        0x73717368 => "SQUASHFS",
        0x6165676C => "PSTOREFS",
        0xde5e81e4 => "EFIVARFS",
        0x00c0ffee => "HOSTFS",
        0x137F | 0x138F => "MINIX",
        0x2468 | 0x2478 => "MINIX2",
        0x4d5a => "MINIX3",
        0x564c => "NCPFS",
        0x7461636f => "OCFS2",
        0x9fa1 => "OPENPROMFS",
        0x002f => "QNX4",
        0x68191122 => "QNX6",
        0x517B => "SMB",
        0xFF534D42 => "CIFS",
        0xFE534D42 => "SMB2",
        0x27e0eb => "CGROUP",
        0x63677270 => "CGROUP2",
        0x74726163 => "TRACEFS",
        0x01021997 => "V9FS",
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
        assert_eq!(get_fs_type(0xadf5), "ADFS");
        assert_eq!(get_fs_type(0xadff), "AFFS");
        assert_eq!(get_fs_type(0x5346414F), "AFS");
        assert_eq!(get_fs_type(0x0187), "AUTOFS");
        assert_eq!(get_fs_type(0x00c36400), "CEPH");
        assert_eq!(get_fs_type(0x73757245), "CODA");
        assert_eq!(get_fs_type(0x28cd3d45), "CRAMFS");
        assert_eq!(get_fs_type(0x453dcd28), "CRAMFS");
        assert_eq!(get_fs_type(0x64626720), "DEBUGFS");
        assert_eq!(get_fs_type(0x73636673), "SECURITYFS");
        assert_eq!(get_fs_type(0xf97cff8c), "SELINUXFS");
        assert_eq!(get_fs_type(0x43415d53), "SMACKFS");
        assert_eq!(get_fs_type(0x858458f6), "RAMFS");
        assert_eq!(get_fs_type(0x958458f6), "HUGETLBFS");
        assert_eq!(get_fs_type(0x2011BAB0), "EXFAT");
        assert_eq!(get_fs_type(0x414A53), "EFS");
        assert_eq!(get_fs_type(0xE0F5E1E2), "EROFS");
        assert_eq!(get_fs_type(0xF2F52010), "F2FS");
        assert_eq!(get_fs_type(0x3434), "NILFS2");
        assert_eq!(get_fs_type(0xf995e849), "HPFS");
        assert_eq!(get_fs_type(0x9660), "ISOFS");
        assert_eq!(get_fs_type(0x72b6), "JFFS2");
        assert_eq!(get_fs_type(0x65735546), "FUSE");
        assert_eq!(get_fs_type(0x73717368), "SQUASHFS");
        assert_eq!(get_fs_type(0x6165676C), "PSTOREFS");
        assert_eq!(get_fs_type(0xde5e81e4), "EFIVARFS");
        assert_eq!(get_fs_type(0x00c0ffee), "HOSTFS");
        assert_eq!(get_fs_type(0x137F), "MINIX");
        assert_eq!(get_fs_type(0x138F), "MINIX");
        assert_eq!(get_fs_type(0x2468), "MINIX2");
        assert_eq!(get_fs_type(0x2478), "MINIX2");
        assert_eq!(get_fs_type(0x4d5a), "MINIX3");
        assert_eq!(get_fs_type(0x564c), "NCPFS");
        assert_eq!(get_fs_type(0x7461636f), "OCFS2");
        assert_eq!(get_fs_type(0x9fa1), "OPENPROMFS");
        assert_eq!(get_fs_type(0x002f), "QNX4");
        assert_eq!(get_fs_type(0x68191122), "QNX6");
        assert_eq!(get_fs_type(0x6B414653), "AFS");
        assert_eq!(get_fs_type(0x517B), "SMB");
        assert_eq!(get_fs_type(0xFF534D42), "CIFS");
        assert_eq!(get_fs_type(0xFE534D42), "SMB2");
        assert_eq!(get_fs_type(0x27e0eb), "CGROUP");
        assert_eq!(get_fs_type(0x63677270), "CGROUP2");
        assert_eq!(get_fs_type(0x74726163), "TRACEFS");
        assert_eq!(get_fs_type(0x01021997), "V9FS");
        assert_eq!(get_fs_type(0xca451a4e), "BCACHEFS");
    }
}
