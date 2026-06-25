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

use crate::storage_api::EndpointServerPools;
use rustfs_config::{
    DEFAULT_RUSTFS_UNSUPPORTED_FS_POLICY, ENV_RUSTFS_UNSUPPORTED_FS_POLICY, RUSTFS_UNSUPPORTED_FS_POLICY_FAIL,
    RUSTFS_UNSUPPORTED_FS_POLICY_WARN,
};
use std::collections::BTreeSet;
use std::io::{Error, Result};
use tracing::warn;

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

    let mut unsupported = Vec::new();
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

        if is_unsupported_fs_type(&info.fstype) {
            unsupported.push((path, info.fstype));
        }
    }

    if unsupported.is_empty() {
        return Ok(());
    }

    let detail = unsupported
        .iter()
        .map(|(path, fs_type)| format!("{path} ({fs_type})"))
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
}
