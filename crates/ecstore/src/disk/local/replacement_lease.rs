// Copyright 2026 RustFS Team
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

use crate::disk::error::{DiskError, Result};
use std::{path::PathBuf, sync::Arc};

/// An exclusive replacement executor on a descriptor-pinned local disk.
/// Mutation workers retain an Arc until their outstanding I/O has finished.
#[derive(Debug)]
pub struct ReplacementExecutionLease {
    _lock: std::fs::File,
}

// This inode must survive format repair and metadata cleanup. Removing a lock
// file while it is held would allow a second owner to lock a different inode.
const EXECUTION_LOCK_FILE: &str = ".rustfs-replacement.lock";

pub(super) async fn acquire(root: PathBuf) -> Result<Arc<ReplacementExecutionLease>> {
    #[cfg(unix)]
    {
        tokio::task::spawn_blocking(move || {
            use rustix::fs::{FlockOperation, Mode, OFlags, flock, open};
            let lock = std::fs::File::from(
                open(
                    root.join(EXECUTION_LOCK_FILE),
                    OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                    Mode::RUSR | Mode::WUSR,
                )
                .map_err(std::io::Error::from)?,
            );
            flock(&lock, FlockOperation::NonBlockingLockExclusive).map_err(std::io::Error::from)?;
            Ok(Arc::new(ReplacementExecutionLease { _lock: lock }))
        })
        .await
        .map_err(DiskError::from)?
    }
    #[cfg(not(unix))]
    {
        let _ = root;
        Err(DiskError::other("replacement execution leases are unsupported on this platform"))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replacement_execution_lease_excludes_independent_openers() {
        let temp = tempfile::TempDir::new().expect("lease root");
        let lease = acquire(temp.path().to_path_buf()).await.expect("first executor");
        assert!(matches!(
            acquire(temp.path().to_path_buf()).await,
            Err(DiskError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
        let worker = lease.clone();
        drop(lease);
        assert!(
            acquire(temp.path().to_path_buf()).await.is_err(),
            "outstanding worker must retain ownership"
        );
        drop(worker);
        acquire(temp.path().to_path_buf())
            .await
            .expect("ownership is released after the final worker");
    }

    #[tokio::test]
    async fn replacement_execution_lease_fences_another_process() {
        const CHILD_ROOT: &str = "RUSTFS_TEST_REPLACEMENT_LEASE_ROOT";
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let available = std::env::var_os("RUSTFS_TEST_REPLACEMENT_LEASE_AVAILABLE").is_some();
            assert_eq!(acquire(PathBuf::from(root)).await.is_ok(), available);
            return;
        }
        let temp = tempfile::TempDir::new().expect("lease root");
        let owner = acquire(temp.path().to_path_buf()).await.expect("parent owns lease");
        let run_child = |available| {
            let mut child = std::process::Command::new(std::env::current_exe().expect("test executable"));
            child.args([
                "--exact",
                "disk::local::replacement_lease::tests::replacement_execution_lease_fences_another_process",
            ]);
            child.env(CHILD_ROOT, temp.path());
            child.env_remove("RUSTFS_TEST_REPLACEMENT_LEASE_AVAILABLE");
            if available {
                child.env("RUSTFS_TEST_REPLACEMENT_LEASE_AVAILABLE", "1");
            }
            assert!(child.status().expect("child lease probe").success());
        };
        run_child(false);
        drop(owner);
        run_child(true);
    }

    #[tokio::test]
    async fn replacement_execution_lease_rejects_a_symlink_lock() {
        let temp = tempfile::TempDir::new().expect("lease root");
        let destination = temp.path().join("other");
        std::fs::write(&destination, b"untouched").expect("sentinel");
        std::os::unix::fs::symlink(&destination, temp.path().join(EXECUTION_LOCK_FILE)).expect("symlink fixture");
        assert!(acquire(temp.path().to_path_buf()).await.is_err());
        assert_eq!(std::fs::read(destination).expect("sentinel remains"), b"untouched");
    }
}
