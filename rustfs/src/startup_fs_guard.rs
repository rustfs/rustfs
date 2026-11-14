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

use crate::storage_api::startup::fs_guard::EndpointServerPools;
use rustfs_config::{
    DEFAULT_RUSTFS_UNSUPPORTED_FS_POLICY, ENV_RUSTFS_UNSUPPORTED_FS_POLICY, RUSTFS_UNSUPPORTED_FS_POLICY_FAIL,
    RUSTFS_UNSUPPORTED_FS_POLICY_WARN,
};
use rustfs_utils::os::DiskInfo;
use std::collections::BTreeSet;
use std::io::{Error, Result};
use tracing::{info, warn};

const LOG_COMPONENT_STORAGE: &str = "storage";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_STARTUP_FS_BOUNDARY: &str = "startup_fs_boundary";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnsupportedFsPolicy {
    Warn,
    Fail,
}

impl UnsupportedFsPolicy {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            RUSTFS_UNSUPPORTED_FS_POLICY_WARN => Some(Self::Warn),
            RUSTFS_UNSUPPORTED_FS_POLICY_FAIL => Some(Self::Fail),
            _ => None,
        }
    }
}

fn get_unsupported_fs_policy() -> UnsupportedFsPolicy {
    let raw = rustfs_utils::get_env_str(ENV_RUSTFS_UNSUPPORTED_FS_POLICY, DEFAULT_RUSTFS_UNSUPPORTED_FS_POLICY);
    if let Some(policy) = UnsupportedFsPolicy::parse(&raw) {
        return policy;
    }

    warn!(
        env = ENV_RUSTFS_UNSUPPORTED_FS_POLICY,
        value = %raw,
        default = DEFAULT_RUSTFS_UNSUPPORTED_FS_POLICY,
        "Invalid unsupported filesystem policy; falling back to default"
    );
    UnsupportedFsPolicy::parse(DEFAULT_RUSTFS_UNSUPPORTED_FS_POLICY).unwrap_or(UnsupportedFsPolicy::Warn)
}

fn is_unsupported_fs_type(fs_type: &str) -> bool {
    let normalized = fs_type.trim().to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "nfs" | "cifs" | "smb2" | "fuse" | "fuseblk" | "overlayfs" | "9p" | "v9fs" | "ceph" | "glusterfs" | "gfs" | "gfs2"
    ) || normalized.starts_with("fuse.")
}

fn is_ninep_fs_type(fs_type: &str) -> bool {
    let normalized = fs_type.trim().to_ascii_lowercase();
    matches!(normalized.as_str(), "9p" | "v9fs")
}

/// One `/proc/self/mountinfo` record.
///
/// Fields are `(mount id) (parent) (major:minor) (root) (mount point) (options)
/// (optional fields) - (fs type) (source) (super options)`.
struct MountInfoRecord {
    mount_point: String,
    fs_type: String,
    options: String,
}

/// gVisor (runsc) directfs bind mounts are host POSIX files opened through
/// donated file descriptors. The sandbox still reports them as 9p, and the
/// mountinfo options include both `trans=fd` and `directfs`. Real 9p and
/// virtio shares do not carry that pair, so the host type stays hidden only
/// for this shape.
fn is_gvisor_directfs_bind_mount(fs_type: &str, mountinfo: &str, path: &str) -> bool {
    if !is_ninep_fs_type(fs_type) {
        return false;
    }
    let records = parse_mountinfo(mountinfo);
    let Some(record) = covering_mount(&records, path) else {
        return false;
    };
    is_ninep_fs_type(&record.fs_type) && options_include_gvisor_directfs(&record.options)
}

fn options_include_gvisor_directfs(options: &str) -> bool {
    let mut has_trans_fd = false;
    let mut has_directfs = false;
    for option in options.split(',') {
        match option.trim() {
            "trans=fd" => has_trans_fd = true,
            "directfs" => has_directfs = true,
            _ => {}
        }
        if has_trans_fd && has_directfs {
            return true;
        }
    }
    false
}

fn parse_mountinfo(mountinfo: &str) -> Vec<MountInfoRecord> {
    mountinfo.lines().filter_map(parse_mountinfo_line).collect()
}

fn parse_mountinfo_line(line: &str) -> Option<MountInfoRecord> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }

    let mut fields = line.split_whitespace();
    // mount id, parent id, major:minor, root
    for _ in 0..4 {
        fields.next()?;
    }
    let mount_point = normalize_mount_point(unescape_mountinfo_path(fields.next()?));
    let mount_options = fields.next()?;

    let mut seen_separator = false;
    let mut after_separator = 0usize;
    let mut fs_type = None;
    let mut super_options = None;
    for field in fields {
        if !seen_separator {
            if field == "-" {
                seen_separator = true;
            }
            continue;
        }
        after_separator += 1;
        match after_separator {
            1 => fs_type = Some(field),
            2 => {}
            3 => {
                super_options = Some(field);
                break;
            }
            _ => break,
        }
    }

    let fs_type = fs_type?;
    let super_options = super_options?;
    Some(MountInfoRecord {
        mount_point,
        fs_type: fs_type.to_string(),
        // Mount options and super options are both "mountinfo options".
        // gVisor puts `trans=fd` and `directfs` in the super options.
        options: format!("{mount_options},{super_options}"),
    })
}

fn unescape_mountinfo_path(raw: &str) -> String {
    if !raw.contains('\\') {
        return raw.to_string();
    }

    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(offset) = rest.find('\\') {
        out.push_str(&rest[..offset]);
        let after = &rest[offset + 1..];
        let octal_digits = after.len() >= 3 && after.as_bytes()[..3].iter().all(|digit| (b'0'..=b'7').contains(digit));
        if octal_digits {
            match u8::from_str_radix(&after[..3], 8) {
                Ok(value) => {
                    out.push(char::from(value));
                    rest = &after[3..];
                }
                Err(_) => {
                    out.push('\\');
                    rest = after;
                }
            }
        } else {
            out.push('\\');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

fn normalize_mount_point(mut mount_point: String) -> String {
    if mount_point.len() > 1 {
        while mount_point.ends_with('/') {
            mount_point.pop();
        }
    }
    mount_point
}

fn trim_trailing_slash(path: &str) -> &str {
    if path.len() > 1 && path.ends_with('/') {
        path.trim_end_matches('/')
    } else {
        path
    }
}

fn mount_covers(mount_point: &str, path: &str) -> bool {
    if mount_point == "/" {
        return path.starts_with('/');
    }
    path == mount_point || path.starts_with(&format!("{mount_point}/"))
}

/// The visible mount is the longest mount point that contains `path`.
/// A later record of the same length wins, matching mount order in mountinfo.
fn covering_mount<'a>(records: &'a [MountInfoRecord], path: &str) -> Option<&'a MountInfoRecord> {
    let path = trim_trailing_slash(path);
    let mut best: Option<&MountInfoRecord> = None;
    for record in records {
        if !mount_covers(&record.mount_point, path) {
            continue;
        }
        let replaces_best = best.is_none_or(|current| record.mount_point.len() >= current.mount_point.len());
        if replaces_best {
            best = Some(record);
        }
    }
    best
}

#[derive(Debug, Eq, PartialEq)]
enum EndpointFsClass {
    Supported,
    GvisorDirectfs,
    Unsupported(String),
}

fn classify_endpoint_fs(info: &DiskInfo, mountinfo: &str, path: &str) -> EndpointFsClass {
    // A Windows network redirector stays unsupported even when the reported
    // filesystem name would otherwise pass. gVisor directfs is not that case.
    if !info.remote && is_gvisor_directfs_bind_mount(&info.fstype, mountinfo, path) {
        return EndpointFsClass::GvisorDirectfs;
    }
    match unsupported_fs_label(info) {
        Some(label) => EndpointFsClass::Unsupported(label),
        None => EndpointFsClass::Supported,
    }
}

fn read_self_mountinfo() -> String {
    #[cfg(target_os = "linux")]
    {
        // An unreadable mount table cannot prove directfs. Leaving it empty
        // keeps a 9p/V9FS endpoint on the unsupported path.
        std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default()
    }
    #[cfg(not(target_os = "linux"))]
    {
        String::new()
    }
}

/// Describes why the filesystem behind `info` is outside the supported
/// boundary, or returns `None` when it is supported.
fn unsupported_fs_label(info: &DiskInfo) -> Option<String> {
    if info.remote {
        // Windows names the server's filesystem for SMB shares, which would
        // otherwise pass as a local NTFS volume.
        return Some(format!("{} on network share", info.fstype));
    }
    is_unsupported_fs_type(&info.fstype).then(|| info.fstype.clone())
}

fn collect_local_paths(endpoint_pools: &EndpointServerPools) -> Vec<String> {
    let mut local_paths = BTreeSet::new();
    for pool in endpoint_pools.as_ref() {
        for endpoint in pool.endpoints.as_ref() {
            if endpoint.is_local {
                local_paths.insert(endpoint.get_file_path());
            }
        }
    }
    local_paths.into_iter().collect()
}

pub(crate) fn enforce_unsupported_fs_policy(endpoint_pools: &EndpointServerPools) -> Result<()> {
    let local_paths = collect_local_paths(endpoint_pools);
    if local_paths.is_empty() {
        return Ok(());
    }

    let mountinfo = read_self_mountinfo();
    let mut unsupported = Vec::new();
    let mut gvisor_directfs = Vec::new();
    for path in local_paths {
        let info = match rustfs_utils::os::get_info(&path) {
            Ok(info) => info,
            Err(err) => {
                warn!(
                    path = %path,
                    error = %err,
                    "Failed to inspect filesystem type for startup boundary guard; skipping this path"
                );
                continue;
            }
        };

        match classify_endpoint_fs(&info, &mountinfo, &path) {
            EndpointFsClass::Supported => {}
            EndpointFsClass::GvisorDirectfs => gvisor_directfs.push(path),
            EndpointFsClass::Unsupported(label) => unsupported.push((path, label)),
        }
    }

    if !gvisor_directfs.is_empty() {
        let path = gvisor_directfs.join(", ");
        info!(
            event = EVENT_STARTUP_FS_BOUNDARY,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "gvisor_directfs",
            path = %path,
            "Host filesystem type is not visible through the sandbox (gVisor directfs bind mount)"
        );
    }

    if unsupported.is_empty() {
        return Ok(());
    }

    let detail = unsupported
        .iter()
        .map(|(path, label)| format!("{path} ({label})"))
        .collect::<Vec<_>>()
        .join(", ");

    let message = format!(
        "Unsupported filesystem type detected for RustFS local endpoints: {detail}. \
RustFS only supports direct-attached local POSIX filesystems for production workloads."
    );

    match get_unsupported_fs_policy() {
        UnsupportedFsPolicy::Warn => {
            warn!(
                env = ENV_RUSTFS_UNSUPPORTED_FS_POLICY,
                expected = format!("{RUSTFS_UNSUPPORTED_FS_POLICY_WARN}|{RUSTFS_UNSUPPORTED_FS_POLICY_FAIL}"),
                policy = RUSTFS_UNSUPPORTED_FS_POLICY_WARN,
                "{message}"
            );
            Ok(())
        }
        UnsupportedFsPolicy::Fail => Err(Error::other(format!(
            "{message} Startup aborted by policy: {ENV_RUSTFS_UNSUPPORTED_FS_POLICY}={RUSTFS_UNSUPPORTED_FS_POLICY_FAIL}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_fs_type_matcher_covers_network_like_types() {
        assert!(is_unsupported_fs_type("NFS"));
        assert!(is_unsupported_fs_type("cifs"));
        assert!(is_unsupported_fs_type("smb2"));
        assert!(is_unsupported_fs_type("fuse"));
        assert!(is_unsupported_fs_type("fuse.sshfs"));
        assert!(is_unsupported_fs_type("v9fs"));
        assert!(is_unsupported_fs_type("overlayfs"));
    }

    #[test]
    fn unsupported_fs_type_matcher_keeps_local_posix_types_supported() {
        assert!(!is_unsupported_fs_type("XFS"));
        assert!(!is_unsupported_fs_type("EXT4"));
        assert!(!is_unsupported_fs_type("BTRFS"));
    }

    #[test]
    fn remote_volume_is_unsupported_whatever_its_fstype() {
        // An SMB share on Windows reports the server's filesystem name.
        let share = DiskInfo {
            fstype: "NTFS".to_string(),
            remote: true,
            ..Default::default()
        };
        assert_eq!(unsupported_fs_label(&share).as_deref(), Some("NTFS on network share"));

        let local = DiskInfo {
            fstype: "NTFS".to_string(),
            ..Default::default()
        };
        assert_eq!(unsupported_fs_label(&local), None);

        let cifs = DiskInfo {
            fstype: "CIFS".to_string(),
            ..Default::default()
        };
        assert_eq!(unsupported_fs_label(&cifs).as_deref(), Some("CIFS"));
    }

    #[test]
    fn policy_parser_handles_valid_and_invalid_values() {
        assert_eq!(UnsupportedFsPolicy::parse("warn"), Some(UnsupportedFsPolicy::Warn));
        assert_eq!(UnsupportedFsPolicy::parse("FAIL"), Some(UnsupportedFsPolicy::Fail));
        assert_eq!(UnsupportedFsPolicy::parse("unknown"), None);
    }

    #[test]
    #[serial_test::serial]
    fn invalid_policy_falls_back_to_configured_default() {
        temp_env::with_var(ENV_RUSTFS_UNSUPPORTED_FS_POLICY, Some("invalid"), || {
            assert_eq!(
                get_unsupported_fs_policy(),
                UnsupportedFsPolicy::parse(DEFAULT_RUSTFS_UNSUPPORTED_FS_POLICY).unwrap_or(UnsupportedFsPolicy::Warn)
            );
        });
    }

    /// Reporter mountinfo from a gVisor directfs bind of a host directory on `/data`.
    const GVISOR_DIRECTFS_MOUNTINFO: &str = "\
23 18 0:26 / /data rw - 9p none rw,trans=fd,rfdno=6,wfdno=6,aname=/,dfltuid=4294967294,dfltgid=4294967294,dcache=1000,cache=remote_revalidating,disable_fifo_open,directfs
";

    const VIRTIO_9P_MOUNTINFO: &str = "\
36 22 0:40 / /mnt/virtio rw,relatime shared:1 - 9p hostshare rw,trans=virtio,version=9p2000.L,posixacl,msize=104857600
";

    const TCP_9P_MOUNTINFO: &str = "\
41 22 0:41 / /mnt/tcp rw,relatime - 9p 10.0.0.8 rw,trans=tcp,version=9p2000.L
";

    fn v9fs_disk() -> DiskInfo {
        DiskInfo {
            fstype: "V9FS".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn gvisor_directfs_bind_mount_is_not_unsupported_v9fs() {
        let info = v9fs_disk();
        assert_eq!(
            classify_endpoint_fs(&info, GVISOR_DIRECTFS_MOUNTINFO, "/data"),
            EndpointFsClass::GvisorDirectfs
        );
        assert!(is_gvisor_directfs_bind_mount("v9fs", GVISOR_DIRECTFS_MOUNTINFO, "/data"));
        assert!(is_gvisor_directfs_bind_mount("9p", GVISOR_DIRECTFS_MOUNTINFO, "/data/"));
        // The endpoint may be a directory inside the bind mount.
        assert_eq!(
            classify_endpoint_fs(&info, GVISOR_DIRECTFS_MOUNTINFO, "/data/rustfs"),
            EndpointFsClass::GvisorDirectfs
        );
    }

    #[test]
    fn plain_9p_and_virtio_mounts_stay_unsupported() {
        let info = v9fs_disk();
        assert_eq!(
            classify_endpoint_fs(&info, VIRTIO_9P_MOUNTINFO, "/mnt/virtio"),
            EndpointFsClass::Unsupported("V9FS".to_string())
        );
        assert_eq!(
            classify_endpoint_fs(&info, TCP_9P_MOUNTINFO, "/mnt/tcp"),
            EndpointFsClass::Unsupported("V9FS".to_string())
        );
        assert_eq!(classify_endpoint_fs(&info, "", "/data"), EndpointFsClass::Unsupported("V9FS".to_string()));
        assert!(is_unsupported_fs_type("v9fs"));
        assert!(is_unsupported_fs_type("9p"));
    }

    #[test]
    fn gvisor_exception_requires_both_trans_fd_and_directfs() {
        let info = v9fs_disk();
        let trans_fd_only = "23 18 0:26 / /data rw - 9p none rw,trans=fd,rfdno=6,wfdno=6,aname=/";
        let directfs_only = "23 18 0:26 / /data rw - 9p none rw,directfs,aname=/";
        let trans_fd_prefix = "23 18 0:26 / /data rw - 9p none rw,trans=fdx,directfs";
        let directfs_substring = "23 18 0:26 / /data rw - 9p none rw,trans=virtio,cache=directfs";

        for mountinfo in [trans_fd_only, directfs_only, trans_fd_prefix, directfs_substring] {
            assert_eq!(
                classify_endpoint_fs(&info, mountinfo, "/data"),
                EndpointFsClass::Unsupported("V9FS".to_string()),
                "mountinfo should stay unsupported: {mountinfo}"
            );
        }
    }

    #[test]
    fn gvisor_exception_does_not_apply_to_other_filesystem_types() {
        for fstype in ["NFS", "CIFS", "FUSE", "fuse.sshfs", "overlayfs"] {
            let info = DiskInfo {
                fstype: fstype.to_string(),
                ..Default::default()
            };
            assert_eq!(
                classify_endpoint_fs(&info, GVISOR_DIRECTFS_MOUNTINFO, "/data"),
                EndpointFsClass::Unsupported(fstype.to_string())
            );
        }

        // statfs says V9FS, but the covering mount is not 9p. Do not exempt it.
        let nfs_with_same_options = "50 1 0:50 / /data rw - nfs 10.0.0.1:/export rw,trans=fd,directfs";
        assert_eq!(
            classify_endpoint_fs(&v9fs_disk(), nfs_with_same_options, "/data"),
            EndpointFsClass::Unsupported("V9FS".to_string())
        );

        let xfs = DiskInfo {
            fstype: "XFS".to_string(),
            ..Default::default()
        };
        assert_eq!(classify_endpoint_fs(&xfs, GVISOR_DIRECTFS_MOUNTINFO, "/data"), EndpointFsClass::Supported);
    }

    #[test]
    fn gvisor_exception_uses_the_covering_mount() {
        let mixed = "\
18 1 8:1 / / rw,relatime - ext4 /dev/sda1 rw
36 18 0:40 / /mnt/virtio rw,relatime shared:1 - 9p hostshare rw,trans=virtio,version=9p2000.L
23 18 0:26 / /data rw - 9p none rw,trans=fd,rfdno=6,wfdno=6,aname=/,dfltuid=4294967294,dfltgid=4294967294,dcache=1000,cache=remote_revalidating,disable_fifo_open,directfs
41 18 0:41 / /mnt/tcp rw - 9p 10.0.0.8 rw,trans=tcp,version=9p2000.L
";
        let info = v9fs_disk();
        assert_eq!(classify_endpoint_fs(&info, mixed, "/data"), EndpointFsClass::GvisorDirectfs);
        assert_eq!(
            classify_endpoint_fs(&info, mixed, "/mnt/virtio"),
            EndpointFsClass::Unsupported("V9FS".to_string())
        );
        assert_eq!(
            classify_endpoint_fs(&info, mixed, "/mnt/tcp"),
            EndpointFsClass::Unsupported("V9FS".to_string())
        );
        // `/data` must not exempt the sibling `/database`, which sits on the root mount.
        assert_eq!(
            classify_endpoint_fs(&info, mixed, "/database"),
            EndpointFsClass::Unsupported("V9FS".to_string())
        );
    }

    #[test]
    fn escaped_mount_point_matches_the_endpoint_path() {
        let mountinfo = "23 18 0:26 / /data\\040disk rw - 9p none rw,trans=fd,directfs";
        assert_eq!(
            classify_endpoint_fs(&v9fs_disk(), mountinfo, "/data disk"),
            EndpointFsClass::GvisorDirectfs
        );
        assert_eq!(
            classify_endpoint_fs(&v9fs_disk(), mountinfo, "/data disk2"),
            EndpointFsClass::Unsupported("V9FS".to_string())
        );
    }

    #[test]
    fn remote_v9fs_stays_a_network_share() {
        let share = DiskInfo {
            fstype: "V9FS".to_string(),
            remote: true,
            ..Default::default()
        };
        assert_eq!(
            classify_endpoint_fs(&share, GVISOR_DIRECTFS_MOUNTINFO, "/data"),
            EndpointFsClass::Unsupported("V9FS on network share".to_string())
        );
    }
}
