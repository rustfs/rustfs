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

//! Bounded emergency headroom for durable deletion. Healthy operations access
//! only the shared owner's atomic state. Only allocation failures consume the
//! reserve, without holding its mutex across transaction publication or quorum.

use std::{io, path::Path, sync::Arc};

#[cfg(unix)]
pub(super) const RESERVE_BYTES: u64 = 8 * 1024 * 1024;

pub(super) struct DeleteReserve {
    #[cfg(unix)]
    inner: unix::State,
}

impl DeleteReserve {
    pub(super) fn acquire(root: &Path) -> io::Result<Arc<Self>> {
        #[cfg(unix)]
        {
            unix::acquire(root)
        }
        #[cfg(not(unix))]
        {
            let _ = root;
            Ok(Arc::new(Self {}))
        }
    }

    pub(super) fn is_armed(&self) -> bool {
        #[cfg(unix)]
        {
            self.inner.is_armed()
        }
        #[cfg(not(unix))]
        {
            true
        }
    }

    /// Populate the reserve on the disk's dedicated fsync lane.
    pub(super) fn initialize(&self) -> io::Result<()> {
        self.replenish()
    }

    /// Refill only when sufficient free space remains for recovery. An armed
    /// owner returns immediately without opening files or probing capacity.
    pub(super) fn replenish(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.inner.replenish()
        }
        #[cfg(not(unix))]
        {
            Ok(())
        }
    }

    /// Probe capacity during startup so a persisted reserve can make the disk
    /// writable before its normal writeability check.
    pub(super) fn release_if_low(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.inner.release_if_low()
        }
        #[cfg(not(unix))]
        {
            Ok(())
        }
    }

    /// Retry an allocation-safe scratch operation once after ENOSPC. The first
    /// waiter releases the reserve; concurrent waiters serialize behind it and
    /// may use the same released capacity. The caller must ensure that a failed
    /// attempt leaves only disposable scratch or an idempotent primitive under
    /// the existing namespace owner. Never replay a transaction or a
    /// publication-plus-sync sequence with this helper.
    ///
    /// The owner mutex covers release and the retry, excluding replenishment
    /// during this bounded operation. Later transaction phases and unrelated
    /// allocators can still compete for the released space.
    pub(super) fn retry_after_enospc<T>(&self, mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
        match operation() {
            #[cfg(unix)]
            Err(error) if error.kind() == io::ErrorKind::StorageFull => self.inner.retry_after_enospc(operation, error),
            result => result,
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use parking_lot::Mutex;
    use rustix::fs::{Mode, OFlags, mkdirat, open, openat};
    use std::{
        collections::HashMap,
        fs::File,
        io::{Seek, SeekFrom, Write},
        os::unix::fs::MetadataExt,
        sync::{
            OnceLock, Weak,
            atomic::{AtomicBool, Ordering},
        },
    };

    const METADATA_DIR: &str = ".rustfs.sys";
    const RESERVE_NAME: &str = "delete-reserve";
    type RootIdentity = (u64, u64);
    type OwnerRegistry = Mutex<HashMap<RootIdentity, Weak<DeleteReserve>>>;
    static OWNERS: OnceLock<OwnerRegistry> = OnceLock::new();

    pub(super) struct State {
        root: File,
        mutation: Mutex<()>,
        armed: AtomicBool,
    }

    pub(super) fn acquire(path: &Path) -> io::Result<Arc<DeleteReserve>> {
        let root = File::from(open(
            path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let metadata = root.metadata()?;
        let identity = (metadata.dev(), metadata.ino());
        let mut registry = OWNERS.get_or_init(Mutex::default).lock();
        if let Some(owner) = registry.get(&identity).and_then(Weak::upgrade) {
            return Ok(owner);
        }
        registry.retain(|_, owner| owner.strong_count() != 0);
        let owner = Arc::new(DeleteReserve {
            inner: State {
                root,
                mutation: Mutex::new(()),
                armed: AtomicBool::new(false),
            },
        });
        registry.insert(identity, Arc::downgrade(&owner));
        Ok(owner)
    }

    fn metadata_directory(root: &File, create: bool) -> io::Result<File> {
        if create {
            match mkdirat(root, METADATA_DIR, Mode::from_raw_mode(0o700)) {
                Ok(()) => root.sync_all()?,
                Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(File::from(openat(
            root,
            METADATA_DIR,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?))
    }

    fn reserve_file(parent: &File, create: bool) -> io::Result<File> {
        let flags = OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        let file = File::from(openat(
            parent,
            RESERVE_NAME,
            if create { flags | OFlags::CREATE } else { flags },
            Mode::from_raw_mode(0o600),
        )?);
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > RESERVE_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid delete reserve file"));
        }
        Ok(file)
    }

    fn absent_to_none<T>(result: io::Result<T>) -> io::Result<Option<T>> {
        match result {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    impl State {
        pub(super) fn is_armed(&self) -> bool {
            self.armed.load(Ordering::Acquire)
        }

        pub(super) fn replenish(&self) -> io::Result<()> {
            if self.is_armed() {
                return Ok(());
            }
            let _guard = self.mutation.lock();
            if self.is_armed() {
                return Ok(());
            }
            let existing = match absent_to_none(metadata_directory(&self.root, false))? {
                Some(parent) => absent_to_none(reserve_file(&parent, false))?.map(|file| (parent, file)),
                None => None,
            };
            if let Some((_, file)) = &existing {
                let metadata = file.metadata()?;
                if metadata.len() == RESERVE_BYTES && metadata.blocks().saturating_mul(512) >= RESERVE_BYTES {
                    self.armed.store(true, Ordering::Release);
                    return Ok(());
                }
            }
            // A depleted reserve must not take newly released recovery space
            // away from a disk that is still nearly full.
            if available_bytes(&self.root)? < RESERVE_BYTES * 2 {
                return Ok(());
            }
            let (parent, mut file) = match existing {
                Some(existing) => existing,
                None => {
                    let parent = metadata_directory(&self.root, true)?;
                    let file = reserve_file(&parent, true)?;
                    (parent, file)
                }
            };
            let metadata = file.metadata()?;
            // A length alone does not reserve blocks. Repair sparse files left
            // by interrupted allocation or older installations.
            if metadata.blocks().saturating_mul(512) < metadata.len() {
                file.set_len(0)?;
            }
            let length = file.seek(SeekFrom::End(0))?;
            let mut remaining = RESERVE_BYTES - length;
            let mut bytes = [0_u8; 64 * 1024];
            while remaining != 0 {
                let count = usize::try_from(remaining.min(64 * 1024)).map_err(io::Error::other)?;
                // Zero blocks can become holes on transparent-compression filesystems.
                rand::fill(&mut bytes[..count]);
                file.write_all(&bytes[..count])?;
                remaining -= u64::try_from(count).map_err(io::Error::other)?;
            }
            file.sync_all()?;
            if file.metadata()?.blocks().saturating_mul(512) < RESERVE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "filesystem did not allocate delete reserve blocks",
                ));
            }
            parent.sync_all()?;
            self.armed.store(true, Ordering::Release);
            Ok(())
        }

        fn release_locked(&self) -> io::Result<bool> {
            let Some(parent) = absent_to_none(metadata_directory(&self.root, false))? else {
                self.armed.store(false, Ordering::Release);
                return Ok(false);
            };
            let Some(file) = absent_to_none(reserve_file(&parent, false))? else {
                self.armed.store(false, Ordering::Release);
                return Ok(false);
            };
            let metadata = file.metadata()?;
            if metadata.len() == 0 {
                self.armed.store(false, Ordering::Release);
                return Ok(false);
            }
            file.set_len(0)?;
            self.armed.store(false, Ordering::Release);
            // The reserve contains no recovery authority. Truncation releases
            // blocks immediately and needs no durability barrier before retry.
            Ok(metadata.blocks() != 0)
        }

        pub(super) fn release_if_low(&self) -> io::Result<()> {
            if available_bytes(&self.root)? < RESERVE_BYTES / 2 {
                let _guard = self.mutation.lock();
                self.release_locked()?;
            }
            Ok(())
        }

        pub(super) fn retry_after_enospc<T>(
            &self,
            mut operation: impl FnMut() -> io::Result<T>,
            original: io::Error,
        ) -> io::Result<T> {
            let _guard = self.mutation.lock();
            match self.release_locked() {
                // A peer may have consumed the shared reserve while this
                // operation was failing. Waiting for the mutation lock and
                // replaying once lets it use the capacity that peer released.
                // The public contract only accepts disposable scratch or an
                // idempotent primitive, so the retry remains safe even when no
                // reserve was present. Invalid reserve state still fails closed.
                Ok(_) => operation(),
                Err(_) => Err(original),
            }
        }
    }

    fn available_bytes(root: &File) -> io::Result<u64> {
        let stat = rustix::fs::fstatvfs(root)?;
        let block_size = if stat.f_frsize == 0 { stat.f_bsize } else { stat.f_frsize };
        let available = u128::from(stat.f_bavail) * u128::from(block_size);
        Ok(u64::try_from(available).unwrap_or(u64::MAX))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn reserve_path(root: &Path) -> std::path::PathBuf {
            root.join(METADATA_DIR).join(RESERVE_NAME)
        }

        #[test]
        fn delete_reserve_allocates_retries_once_and_replenishes_real_blocks() {
            let root = tempfile::tempdir().expect("create disk root");
            let owner = DeleteReserve::acquire(root.path()).expect("pin disk root");
            owner.initialize().expect("allocate reserve");
            let path = reserve_path(root.path());
            let initial = std::fs::metadata(&path).expect("reserve metadata");
            assert_eq!(initial.len(), RESERVE_BYTES);
            assert!(initial.blocks() * 512 >= RESERVE_BYTES);
            assert!(owner.is_armed());
            let mut calls = 0;
            let result = owner
                .retry_after_enospc(|| {
                    calls += 1;
                    if calls == 1 {
                        Err(io::Error::new(io::ErrorKind::StorageFull, "scratch allocation"))
                    } else {
                        Ok(42)
                    }
                })
                .expect("retry scratch allocation");
            assert_eq!(result, 42);
            assert_eq!(calls, 2);
            assert!(!owner.is_armed());
            assert_eq!(std::fs::metadata(&path).expect("released reserve").len(), 0);
            owner.replenish().expect("replenish reserve");
            assert!(owner.is_armed());
            assert_eq!(std::fs::metadata(&path).expect("replenished reserve").len(), RESERVE_BYTES);
        }

        #[test]
        fn delete_reserve_retries_once_without_local_reserve_capacity() {
            let root = tempfile::tempdir().expect("create disk root");
            let owner = DeleteReserve::acquire(root.path()).expect("pin disk root");
            let mut calls = 0;
            let error = owner
                .retry_after_enospc::<()>(|| {
                    calls += 1;
                    Err(io::Error::new(io::ErrorKind::StorageFull, format!("allocation {calls}")))
                })
                .expect_err("retry without a reserve still has no capacity");
            assert_eq!(calls, 2);
            assert_eq!(error.to_string(), "allocation 2");
            owner.initialize().expect("allocate reserve");
            let parent = metadata_directory(&owner.inner.root, false).expect("metadata directory");
            let reserve = reserve_file(&parent, false).expect("reserve file");
            reserve.set_len(0).expect("deplete reserve");
            calls = 0;
            assert!(
                owner
                    .retry_after_enospc::<()>(|| {
                        calls += 1;
                        Err(io::Error::new(io::ErrorKind::StorageFull, "empty reserve"))
                    })
                    .is_err()
            );
            assert_eq!(calls, 2);
        }

        #[test]
        fn delete_reserve_retry_ignores_other_errors_and_stays_bounded() {
            let root = tempfile::tempdir().expect("create disk root");
            let owner = DeleteReserve::acquire(root.path()).expect("pin disk root");
            owner.initialize().expect("allocate reserve");
            let mut calls = 0;
            let error = owner
                .retry_after_enospc::<()>(|| {
                    calls += 1;
                    Err(io::Error::new(io::ErrorKind::PermissionDenied, "permission failure"))
                })
                .expect_err("permission failures must propagate");
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            assert_eq!(calls, 1);
            assert!(owner.is_armed());
            calls = 0;
            let error = owner
                .retry_after_enospc::<()>(|| {
                    calls += 1;
                    Err(io::Error::new(io::ErrorKind::StorageFull, format!("allocation {calls}")))
                })
                .expect_err("second allocation failure propagates");
            assert_eq!(calls, 2);
            assert_eq!(error.to_string(), "allocation 2");
        }

        #[test]
        fn delete_reserve_rejects_symlinks_and_hardlinks_without_hiding_operation_errors() {
            use std::os::unix::fs::symlink;
            let root = tempfile::tempdir().expect("create disk root");
            let owner = DeleteReserve::acquire(root.path()).expect("pin disk root");
            let external = tempfile::tempdir().expect("create external directory");
            let victim = external.path().join("victim");
            std::fs::write(&victim, b"preserve").expect("write victim");
            let metadata = root.path().join(METADATA_DIR);
            symlink(external.path(), &metadata).expect("symlink metadata directory");
            assert!(owner.replenish().is_err());
            std::fs::remove_file(&metadata).expect("remove directory link");
            std::fs::create_dir(&metadata).expect("create metadata directory");
            let reserve = reserve_path(root.path());
            symlink(&victim, &reserve).expect("symlink reserve");
            assert!(owner.replenish().is_err());
            let mut calls = 0;
            let error = owner
                .retry_after_enospc::<()>(|| {
                    calls += 1;
                    Err(io::Error::new(io::ErrorKind::StorageFull, "original error"))
                })
                .expect_err("invalid reserve cannot mask allocation failure");
            assert_eq!(calls, 1);
            assert_eq!(error.to_string(), "original error");
            std::fs::remove_file(&reserve).expect("remove reserve link");
            std::fs::hard_link(&victim, &reserve).expect("hardlink reserve");
            assert!(owner.replenish().is_err());
            assert!(owner.inner.release_locked().is_err());
            assert_eq!(std::fs::read(&victim).expect("read victim"), b"preserve");
        }

        #[test]
        fn delete_reserve_repairs_partial_and_sparse_files() {
            let root = tempfile::tempdir().expect("create disk root");
            let owner = DeleteReserve::acquire(root.path()).expect("pin disk root");
            let parent = metadata_directory(&owner.inner.root, true).expect("create metadata directory");
            let mut file = reserve_file(&parent, true).expect("create reserve file");
            file.write_all(b"partial allocation").expect("write partial reserve");
            owner.replenish().expect("finish partial allocation");
            let metadata = file.metadata().expect("partial reserve metadata");
            assert_eq!(metadata.len(), RESERVE_BYTES);
            assert!(metadata.blocks() * 512 >= RESERVE_BYTES);
            file.set_len(0).expect("truncate reserve");
            file.set_len(RESERVE_BYTES).expect("create sparse reserve");
            owner.inner.armed.store(false, Ordering::Release);
            assert!(file.metadata().expect("sparse reserve metadata").blocks() * 512 < RESERVE_BYTES);
            owner.replenish().expect("repair sparse allocation");
            let metadata = file.metadata().expect("repaired reserve metadata");
            assert_eq!(metadata.len(), RESERVE_BYTES);
            assert!(metadata.blocks() * 512 >= RESERVE_BYTES);
        }

        #[test]
        fn delete_reserve_shares_reopened_owner_and_serializes_recovery_with_replenishment() {
            let root = tempfile::tempdir().expect("create disk root");
            let first = DeleteReserve::acquire(root.path()).expect("open first root");
            let second = DeleteReserve::acquire(root.path()).expect("reopen root");
            assert!(Arc::ptr_eq(&first, &second));
            first.initialize().expect("allocate reserve");
            let (retry_entered_tx, retry_entered_rx) = std::sync::mpsc::channel();
            let (allow_retry_tx, allow_retry_rx) = std::sync::mpsc::channel();
            let (replenished_tx, replenished_rx) = std::sync::mpsc::channel();
            let recovery_owner = first.clone();
            let root_path = root.path().to_path_buf();
            std::thread::scope(|scope| {
                let recovery = scope.spawn(move || {
                    let mut calls = 0;
                    recovery_owner
                        .retry_after_enospc(|| {
                            calls += 1;
                            if calls == 1 {
                                return Err(io::Error::new(io::ErrorKind::StorageFull, "scratch allocation"));
                            }
                            assert!(recovery_owner.inner.mutation.try_lock().is_none(), "retry must retain the reserve mutex");
                            retry_entered_tx.send(()).expect("notify retry");
                            allow_retry_rx
                                .recv_timeout(std::time::Duration::from_secs(10))
                                .expect("allow retry completion");
                            assert_eq!(std::fs::metadata(reserve_path(&root_path)).expect("depleted reserve").len(), 0);
                            Ok(())
                        })
                        .expect("retry allocation");
                });
                retry_entered_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("retry entered");
                let replenish = scope.spawn(move || {
                    second.replenish().expect("concurrent replenishment");
                    replenished_tx.send(()).expect("notify replenishment");
                });
                assert!(matches!(replenished_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)));
                allow_retry_tx.send(()).expect("finish retry");
                recovery.join().expect("recovery worker");
                replenish.join().expect("replenishment worker");
            });
            assert!(first.is_armed());
            assert_eq!(std::fs::metadata(reserve_path(root.path())).expect("final reserve").len(), RESERVE_BYTES);
        }

        #[test]
        fn delete_reserve_gives_concurrent_enospc_waiters_one_retry() {
            let root = tempfile::tempdir().expect("create disk root");
            let first = DeleteReserve::acquire(root.path()).expect("open first owner");
            let second = DeleteReserve::acquire(root.path()).expect("open second owner");
            first.initialize().expect("allocate reserve");

            let (first_retry_tx, first_retry_rx) = std::sync::mpsc::channel();
            let (allow_first_tx, allow_first_rx) = std::sync::mpsc::channel();
            let (second_failed_tx, second_failed_rx) = std::sync::mpsc::channel();
            let (second_done_tx, second_done_rx) = std::sync::mpsc::channel();

            std::thread::scope(|scope| {
                let first_waiter = scope.spawn(move || {
                    let mut calls = 0;
                    first
                        .retry_after_enospc(|| {
                            calls += 1;
                            if calls == 1 {
                                return Err(io::Error::new(io::ErrorKind::StorageFull, "first ENOSPC"));
                            }
                            first_retry_tx.send(()).expect("notify first retry");
                            allow_first_rx
                                .recv_timeout(std::time::Duration::from_secs(10))
                                .expect("allow first retry to finish");
                            Ok(())
                        })
                        .expect("first waiter retries");
                    assert_eq!(calls, 2);
                });

                first_retry_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("first waiter holds recovery lock");
                let second_waiter = scope.spawn(move || {
                    let mut calls = 0;
                    second
                        .retry_after_enospc(|| {
                            calls += 1;
                            if calls == 1 {
                                second_failed_tx.send(()).expect("notify second ENOSPC");
                                return Err(io::Error::new(io::ErrorKind::StorageFull, "second ENOSPC"));
                            }
                            Ok(())
                        })
                        .expect("second waiter retries after shared release");
                    assert_eq!(calls, 2);
                    second_done_tx.send(()).expect("notify second completion");
                });

                second_failed_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("second waiter observed ENOSPC");
                assert!(matches!(second_done_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)));
                allow_first_tx.send(()).expect("finish first retry");
                first_waiter.join().expect("first waiter");
                second_done_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("second waiter completed its bounded retry");
                second_waiter.join().expect("second waiter");
            });

            assert_eq!(std::fs::metadata(reserve_path(root.path())).expect("depleted reserve").len(), 0);
        }

        #[test]
        fn delete_reserve_rejects_oversize_files_and_fifos() {
            let root = tempfile::tempdir().expect("create disk root");
            let owner = DeleteReserve::acquire(root.path()).expect("pin disk root");
            let parent = metadata_directory(&owner.inner.root, true).expect("create metadata directory");
            let file = reserve_file(&parent, true).expect("create reserve file");
            file.set_len(RESERVE_BYTES + 1).expect("create oversized reserve");
            assert!(owner.replenish().is_err());
            assert!(owner.inner.release_locked().is_err());
            assert_eq!(file.metadata().expect("oversize metadata").len(), RESERVE_BYTES + 1);
            let path = reserve_path(root.path());
            std::fs::remove_file(&path).expect("remove oversized file");
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(&path)
                    .status()
                    .expect("create FIFO fixture")
                    .success()
            );
            assert!(owner.replenish().is_err());
            assert!(owner.inner.release_locked().is_err());
            use std::os::unix::fs::FileTypeExt;
            assert!(std::fs::symlink_metadata(path).expect("FIFO metadata").file_type().is_fifo());
        }
    }
}
