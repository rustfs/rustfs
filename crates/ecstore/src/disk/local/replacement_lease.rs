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

/// A compatibility retry may only admit a disk that has not received data.
/// The caller retains the mount descriptor and the replacement executor lease.
pub(crate) async fn is_blank_target(root: PathBuf) -> Result<bool> {
    tokio::task::spawn_blocking(move || {
        let mut directories = vec![(root, 0usize)];
        let mut entries_left = 64usize;
        while let Some((directory, depth)) = directories.pop() {
            for entry in std::fs::read_dir(directory)? {
                let entry = entry?;
                let Some(remaining) = entries_left.checked_sub(1) else {
                    return Ok(false);
                };
                entries_left = remaining;
                let kind = entry.file_type()?;
                let name = entry.file_name();
                if depth == 0 && name == EXECUTION_LOCK_FILE && kind.is_file() {
                    if entry.metadata()?.len() != 0 {
                        return Ok(false);
                    }
                } else if depth == 1 && name == crate::disk::HEALING_MARKER_PATH && kind.is_file() {
                    if entry.metadata()?.len() > 256 {
                        return Ok(false);
                    }
                } else if kind.is_dir() && depth < 4 && (depth > 0 || name == crate::disk::RUSTFS_META_BUCKET) {
                    directories.push((entry.path(), depth + 1));
                } else {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    })
    .await
    .map_err(DiskError::from)?
}

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
    async fn blank_target_probe_rejects_data_symlinks_and_excessive_inventory() {
        let temp = tempfile::TempDir::new().expect("blank target root");
        let root = temp.path();
        let _lease = acquire(root.to_path_buf()).await.expect("replacement execution lease");
        let metadata = root.join(crate::disk::RUSTFS_META_BUCKET);
        std::fs::create_dir_all(metadata.join("tmp")).expect("empty system directories");
        std::fs::write(metadata.join(crate::disk::HEALING_MARKER_PATH), b"owner").expect("healing marker");
        assert!(
            is_blank_target(root.to_path_buf())
                .await
                .expect("blank target with control files")
        );

        for path in [
            root.join("user-bucket"),
            metadata.join("tmp").join("payload"),
            metadata.join("format.json"),
        ] {
            std::fs::write(&path, b"data").expect("residual content");
            assert!(!is_blank_target(root.to_path_buf()).await.expect("inspect residual content"));
            std::fs::remove_file(path).expect("remove residual fixture");
        }
        std::fs::create_dir(root.join("user-bucket")).expect("even an empty user bucket is not a fresh target");
        assert!(!is_blank_target(root.to_path_buf()).await.expect("inspect user bucket"));
        std::fs::remove_dir(root.join("user-bucket")).expect("remove bucket fixture");
        std::os::unix::fs::symlink(root, metadata.join("cycle")).expect("symlink fixture");
        assert!(
            !is_blank_target(root.to_path_buf())
                .await
                .expect("reject symlink without following it")
        );
        std::fs::remove_file(metadata.join("cycle")).expect("remove symlink fixture");
        std::fs::create_dir_all(metadata.join("a/b/c/d")).expect("excessive depth");
        assert!(!is_blank_target(root.to_path_buf()).await.expect("bounded directory depth"));
        std::fs::remove_dir_all(metadata.join("a")).expect("remove deep fixture");
        std::fs::remove_file(metadata.join(crate::disk::HEALING_MARKER_PATH)).expect("remove marker fixture");
        std::fs::remove_dir(metadata.join("tmp")).expect("remove empty fixture directory");
        for index in 0..62 {
            std::fs::create_dir(metadata.join(index.to_string())).expect("bounded empty directories");
        }
        assert!(is_blank_target(root.to_path_buf()).await.expect("exact inventory limit"));
        std::fs::create_dir(metadata.join("extra")).expect("one over inventory limit");
        assert!(!is_blank_target(root.to_path_buf()).await.expect("bounded inventory"));
        assert!(is_blank_target(root.join("missing")).await.is_err(), "missing root is not a blank disk");
    }

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
