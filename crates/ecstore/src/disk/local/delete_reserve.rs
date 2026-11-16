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

//! Bounded emergency headroom for the existing durable delete protocol.
//!
//! Call these synchronous operations on the disk's blocking/fsync lane. Releasing
//! the reserve gives deletion a chance to persist its journal on an exhausted
//! filesystem. Concurrent allocators can still consume the released blocks.

use std::{io, path::Path};

#[cfg(unix)]
pub(super) const RESERVE_BYTES: u64 = 8 * 1024 * 1024;

pub(super) fn initialize(root: &Path) -> io::Result<()> {
    replenish(root)
}

pub(super) fn replenish(root: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        unix::replenish(root)
    }
    #[cfg(not(unix))]
    {
        let _ = root;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "delete reserve requires handle-relative filesystem access",
        ))
    }
}

pub(super) fn release_if_low(root: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        unix::release_if_low(root)
    }
    #[cfg(not(unix))]
    {
        let _ = root;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "delete reserve requires handle-relative filesystem access",
        ))
    }
}

#[cfg(all(test, unix))]
pub(super) fn force_low_space(root: &Path) -> io::Result<impl Drop> {
    unix::force_low_space(root)
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
        sync::{Arc, OnceLock, Weak},
    };

    const METADATA_DIR: &str = ".rustfs.sys";
    const RESERVE_NAME: &str = "delete-reserve";
    type RootIdentity = (u64, u64);
    type LockRegistry = Mutex<HashMap<RootIdentity, Weak<Mutex<()>>>>;
    static LOCKS: OnceLock<LockRegistry> = OnceLock::new();

    fn root_directory(root: &Path) -> io::Result<File> {
        Ok(File::from(open(
            root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?))
    }

    fn root_lock(root: &File) -> io::Result<Arc<Mutex<()>>> {
        let metadata = root.metadata()?;
        let identity = (metadata.dev(), metadata.ino());
        let mut registry = LOCKS.get_or_init(Mutex::default).lock();
        if let Some(lock) = registry.get(&identity).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        registry.retain(|_, lock| lock.strong_count() != 0);
        let lock = Arc::new(Mutex::new(()));
        registry.insert(identity, Arc::downgrade(&lock));
        Ok(lock)
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

    pub(super) fn replenish(root: &Path) -> io::Result<()> {
        let root = root_directory(root)?;
        let lock = root_lock(&root)?;
        let _guard = lock.lock();
        // Keep newly released space available for recovery on nearly full disks.
        if available_bytes(&root)? < RESERVE_BYTES * 2 {
            return Ok(());
        }
        let parent = metadata_directory(&root, true)?;
        let mut file = reserve_file(&parent, true)?;
        let metadata = file.metadata()?;
        // A length alone does not reserve blocks. Repair an incomplete/sparse
        // file left by an interrupted allocation or an older installation.
        if metadata.blocks().saturating_mul(512) < metadata.len() {
            file.set_len(0)?;
        }
        let length = file.seek(SeekFrom::End(0))?;
        if length == RESERVE_BYTES {
            return Ok(());
        }
        let mut remaining = RESERVE_BYTES - length;
        let bytes = [0_u8; 64 * 1024];
        while remaining != 0 {
            let count = usize::try_from(remaining.min(64 * 1024)).map_err(io::Error::other)?;
            file.write_all(&bytes[..count])?;
            remaining -= u64::try_from(count).map_err(io::Error::other)?;
        }
        file.sync_all()?;
        parent.sync_all()
    }

    fn release_with_available(root: &File, available: u64) -> io::Result<()> {
        if available >= RESERVE_BYTES / 2 {
            return Ok(());
        }
        let lock = root_lock(root)?;
        let _guard = lock.lock();
        let parent = match metadata_directory(root, false) {
            Ok(parent) => parent,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        let file = match reserve_file(&parent, false) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if file.metadata()?.len() != 0 {
            file.set_len(0)?;
            file.sync_all()?;
        }
        Ok(())
    }

    pub(super) fn release_if_low(root: &Path) -> io::Result<()> {
        let root = root_directory(root)?;
        #[cfg(test)]
        {
            let metadata = root.metadata()?;
            if FORCED_LOW_SPACE
                .get_or_init(Mutex::default)
                .lock()
                .contains_key(&(metadata.dev(), metadata.ino()))
            {
                return release_with_available(&root, 0);
            }
        }
        release_with_available(&root, available_bytes(&root)?)
    }

    fn available_bytes(root: &File) -> io::Result<u64> {
        let stat = rustix::fs::fstatvfs(root)?;
        let block_size = if stat.f_frsize == 0 { stat.f_bsize } else { stat.f_frsize };
        let available = u128::from(stat.f_bavail) * u128::from(block_size);
        Ok(u64::try_from(available).unwrap_or(u64::MAX))
    }

    #[cfg(test)]
    static FORCED_LOW_SPACE: OnceLock<Mutex<HashMap<RootIdentity, usize>>> = OnceLock::new();

    #[cfg(test)]
    pub(super) struct LowSpaceGuard(RootIdentity);

    #[cfg(test)]
    impl Drop for LowSpaceGuard {
        fn drop(&mut self) {
            let mut roots = FORCED_LOW_SPACE.get_or_init(Mutex::default).lock();
            if let Some(count) = roots.get_mut(&self.0) {
                *count -= 1;
                if *count == 0 {
                    roots.remove(&self.0);
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn force_low_space(root: &Path) -> io::Result<LowSpaceGuard> {
        let root = root_directory(root)?;
        let metadata = root.metadata()?;
        let identity = (metadata.dev(), metadata.ino());
        *FORCED_LOW_SPACE
            .get_or_init(Mutex::default)
            .lock()
            .entry(identity)
            .or_default() += 1;
        Ok(LowSpaceGuard(identity))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn delete_reserve_allocates_releases_and_replenishes_real_blocks() {
            let root = tempfile::tempdir().expect("create disk root");
            replenish(root.path()).expect("allocate reserve");
            let path = root.path().join(METADATA_DIR).join(RESERVE_NAME);
            let initial = std::fs::metadata(&path).expect("reserve metadata");
            assert_eq!(initial.len(), RESERVE_BYTES);
            assert!(initial.blocks() * 512 >= RESERVE_BYTES);
            replenish(root.path()).expect("idempotent allocation");
            let directory = root_directory(root.path()).expect("pin root");
            release_with_available(&directory, RESERVE_BYTES).expect("retain reserve with free space");
            assert_eq!(std::fs::metadata(&path).expect("retained reserve").len(), RESERVE_BYTES);
            release_with_available(&directory, 0).expect("release reserve");
            assert_eq!(std::fs::metadata(&path).expect("released reserve").len(), 0);
            release_with_available(&directory, 0).expect("idempotent release");
            replenish(root.path()).expect("replenish reserve");
            assert_eq!(std::fs::metadata(&path).expect("replenished reserve").len(), RESERVE_BYTES);
        }

        #[test]
        fn delete_reserve_rejects_symlinks_and_hardlinks() {
            use std::os::unix::fs::symlink;
            let root = tempfile::tempdir().expect("create disk root");
            let external = tempfile::tempdir().expect("create external directory");
            let victim = external.path().join("victim");
            std::fs::write(&victim, b"preserve").expect("write victim");
            let metadata = root.path().join(METADATA_DIR);
            symlink(external.path(), &metadata).expect("symlink metadata directory");
            assert!(replenish(root.path()).is_err());
            std::fs::remove_file(&metadata).expect("remove directory link");
            std::fs::create_dir(&metadata).expect("create metadata directory");
            let reserve = metadata.join(RESERVE_NAME);
            symlink(&victim, &reserve).expect("symlink reserve");
            assert!(replenish(root.path()).is_err());
            let directory = root_directory(root.path()).expect("pin disk root");
            assert!(release_with_available(&directory, 0).is_err());
            std::fs::remove_file(&reserve).expect("remove reserve link");
            std::fs::hard_link(&victim, &reserve).expect("hardlink reserve");
            assert!(replenish(root.path()).is_err());
            assert!(release_with_available(&directory, 0).is_err());
            assert_eq!(std::fs::read(&victim).expect("read victim"), b"preserve");
        }

        #[test]
        fn delete_reserve_repairs_partial_and_sparse_files() {
            let root = tempfile::tempdir().expect("create disk root");
            let directory = root_directory(root.path()).expect("pin disk root");
            let parent = metadata_directory(&directory, true).expect("create metadata directory");
            let mut file = reserve_file(&parent, true).expect("create reserve file");
            file.write_all(b"partial allocation").expect("write partial reserve");
            replenish(root.path()).expect("finish partial allocation");
            let metadata = file.metadata().expect("partial reserve metadata");
            assert_eq!(metadata.len(), RESERVE_BYTES);
            assert!(metadata.blocks() * 512 >= RESERVE_BYTES);
            file.set_len(0).expect("truncate reserve");
            file.set_len(RESERVE_BYTES).expect("create sparse reserve");
            assert!(file.metadata().expect("sparse reserve metadata").blocks() * 512 < RESERVE_BYTES);
            replenish(root.path()).expect("repair sparse allocation");
            let metadata = file.metadata().expect("repaired reserve metadata");
            assert_eq!(metadata.len(), RESERVE_BYTES);
            assert!(metadata.blocks() * 512 >= RESERVE_BYTES);
        }

        #[test]
        fn delete_reserve_serializes_reopened_roots_during_release_and_replenishment() {
            let root = tempfile::tempdir().expect("create disk root");
            replenish(root.path()).expect("allocate reserve");
            let first = root_directory(root.path()).expect("open first root");
            let second = root_directory(root.path()).expect("reopen root");
            let first_lock = root_lock(&first).expect("first physical root lock");
            let second_lock = root_lock(&second).expect("reopened physical root lock");
            assert!(Arc::ptr_eq(&first_lock, &second_lock));
            let barrier = Arc::new(std::sync::Barrier::new(2));
            std::thread::scope(|scope| {
                let release_barrier = barrier.clone();
                let release = scope.spawn(move || {
                    release_barrier.wait();
                    for _ in 0..8 {
                        release_with_available(&first, 0).expect("concurrent release");
                    }
                });
                let replenish = scope.spawn(|| {
                    barrier.wait();
                    for _ in 0..8 {
                        super::replenish(root.path()).expect("concurrent replenishment");
                    }
                });
                release.join().expect("release worker");
                replenish.join().expect("replenish worker");
            });
            let path = root.path().join(METADATA_DIR).join(RESERVE_NAME);
            let metadata = std::fs::metadata(path).expect("final reserve metadata");
            assert!(metadata.len() == 0 || metadata.len() == RESERVE_BYTES);
            assert_eq!(metadata.nlink(), 1);
            super::replenish(root.path()).expect("replenish after concurrent operations");
            let parent = metadata_directory(&second, false).expect("reopened metadata directory");
            assert_eq!(
                reserve_file(&parent, false)
                    .expect("valid reserve")
                    .metadata()
                    .expect("metadata")
                    .len(),
                RESERVE_BYTES
            );
        }

        #[test]
        fn delete_reserve_rejects_oversize_files_and_fifos() {
            let root = tempfile::tempdir().expect("create disk root");
            let directory = root_directory(root.path()).expect("pin disk root");
            let parent = metadata_directory(&directory, true).expect("create metadata directory");
            let file = reserve_file(&parent, true).expect("create reserve file");
            file.set_len(RESERVE_BYTES + 1).expect("create oversized reserve");
            assert!(replenish(root.path()).is_err());
            assert!(release_with_available(&directory, 0).is_err());
            assert_eq!(file.metadata().expect("oversize metadata").len(), RESERVE_BYTES + 1);
            let path = root.path().join(METADATA_DIR).join(RESERVE_NAME);
            std::fs::remove_file(&path).expect("remove oversized file");
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(&path)
                    .status()
                    .expect("run mkfifo for private fixture")
                    .success()
            );
            assert!(replenish(root.path()).is_err());
            assert!(release_with_available(&directory, 0).is_err());
            use std::os::unix::fs::FileTypeExt;
            assert!(std::fs::symlink_metadata(path).expect("FIFO metadata").file_type().is_fifo());
        }

        #[test]
        fn delete_reserve_missing_is_safe_to_release() {
            let root = tempfile::tempdir().expect("create disk root");
            let directory = root_directory(root.path()).expect("pin disk root");
            release_with_available(&directory, 0).expect("release absent reserve");
            assert!(!root.path().join(METADATA_DIR).exists());
        }
    }
}
