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
use std::{fs::File, sync::Arc};

/// A target disk's admitted artifact publication. Physical mutation workers
/// retain this Arc until their final syscall and rollback have finished.
pub struct ArtifactLease {
    _active: File,
    _key_directory: File,
    _seal_directory: File,
    _root: File,
}

pub type ArtifactPutLease = ArtifactLease;
pub type ArtifactRepairLease = ArtifactLease;

#[derive(Clone, Copy)]
pub(crate) enum Domain {
    Put,
    Repair,
}

impl Domain {
    fn directory(self) -> &'static str {
        match self {
            Self::Put => ".rustfs-artifact-put-seals",
            Self::Repair => ".rustfs-artifact-repair-seals",
        }
    }

    fn closed(self) -> &'static [u8] {
        match self {
            Self::Put => b"rustfs-artifact-put-sealed-v1\n",
            Self::Repair => b"rustfs-artifact-repair-sealed-v1\n",
        }
    }
}

pub(super) async fn pinned_root(disk: &super::LocalDisk) -> Result<File> {
    #[cfg(target_os = "linux")]
    let mount = disk.mount_lease.clone();
    #[cfg(all(unix, not(target_os = "linux")))]
    let path = disk.io_root().to_owned();
    #[cfg(not(unix))]
    let _ = disk;
    tokio::task::spawn_blocking(move || {
        #[cfg(target_os = "linux")]
        {
            mount.try_clone().map_err(DiskError::from)
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            use rustix::fs::{Mode, OFlags, open};
            open(
                path,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(std::io::Error::from)
            .map_err(DiskError::from)
        }
        #[cfg(not(unix))]
        {
            Err(DiskError::other("artifact disk leases require Unix"))
        }
    })
    .await
    .map_err(DiskError::from)?
}

pub(super) async fn acquire(root: File, key: [u8; 32]) -> Result<Arc<ArtifactPutLease>> {
    acquire_in(root, key, Domain::Put).await
}

pub(super) async fn acquire_in(root: File, key: [u8; 32], domain: Domain) -> Result<Arc<ArtifactLease>> {
    #[cfg(unix)]
    {
        unix::acquire(root, key, domain).await
    }
    #[cfg(not(unix))]
    {
        let _ = (root, key, domain);
        Err(DiskError::other("artifact disk leases require Unix"))
    }
}

pub(super) async fn seal(root: File, key: [u8; 32]) -> Result<()> {
    seal_in(root, key, Domain::Put).await
}

pub(super) async fn seal_in(root: File, key: [u8; 32], domain: Domain) -> Result<()> {
    #[cfg(unix)]
    {
        unix::seal(root, key, domain).await
    }
    #[cfg(not(unix))]
    {
        let _ = (root, key, domain);
        Err(DiskError::other("artifact disk leases require Unix"))
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use rustix::fs::{FlockOperation, Mode, OFlags, flock, mkdirat, openat};
    use rustix::io::Errno;
    use std::{io, io::Read, io::Write, os::unix::fs::MetadataExt, time::Duration};

    const POLL_INTERVAL: Duration = Duration::from_millis(10);
    const LOCK_TIMEOUT: Duration = Duration::from_secs(30);

    struct Opened {
        root: File,
        seals: File,
        key: File,
        admission: File,
        active: File,
        domain: Domain,
    }

    fn invalid() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, "invalid artifact seal filesystem state")
    }

    fn directory(parent: &File, name: &str, device: u64) -> io::Result<File> {
        match mkdirat(parent, name, Mode::RWXU) {
            Ok(()) | Err(Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        let file = File::from(openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let metadata = file.metadata()?;
        if !metadata.is_dir() || metadata.dev() != device || metadata.mode() & 0o077 != 0 {
            return Err(invalid());
        }
        Ok(file)
    }

    fn regular(file: &File, device: u64) -> io::Result<std::fs::Metadata> {
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.dev() != device || metadata.nlink() != 1 || metadata.mode() & 0o077 != 0 {
            return Err(invalid());
        }
        Ok(metadata)
    }

    fn lock_file(parent: &File, name: &str, device: u64) -> io::Result<File> {
        let file = File::from(openat(
            parent,
            name,
            OFlags::CREATE | OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::RUSR | Mode::WUSR,
        )?);
        if regular(&file, device)?.len() != 0 {
            return Err(invalid());
        }
        Ok(file)
    }

    impl Opened {
        fn open(root: File, key: [u8; 32], domain: Domain) -> io::Result<Self> {
            let metadata = root.metadata()?;
            if !metadata.is_dir() {
                return Err(invalid());
            }
            let seals = directory(&root, domain.directory(), metadata.dev())?;
            let name = hex_simd::encode_to_string(key, hex_simd::AsciiCase::Lower);
            let key = directory(&seals, &name, metadata.dev())?;
            // Neither lock inode is ever removed or replaced, including after
            // closing. Independent processes must always lock the same inode.
            let admission = lock_file(&key, "admission.lock", metadata.dev())?;
            let active = lock_file(&key, "active.lock", metadata.dev())?;
            Ok(Self {
                root,
                seals,
                key,
                admission,
                active,
                domain,
            })
        }

        fn closed(&self) -> io::Result<Option<File>> {
            let mut file = match openat(
                &self.key,
                "closed",
                OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            ) {
                Ok(file) => File::from(file),
                Err(Errno::NOENT) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let length = u64::try_from(self.domain.closed().len()).map_err(|_| invalid())?;
            if regular(&file, self.root.metadata()?.dev())?.len() != length {
                return Err(invalid());
            }
            let mut bytes = vec![0; self.domain.closed().len()];
            file.read_exact(&mut bytes)?;
            if bytes != self.domain.closed() {
                return Err(invalid());
            }
            Ok(Some(file))
        }

        fn admit(self) -> io::Result<Arc<ArtifactLease>> {
            if self.closed()?.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "artifact attempt mutation domain is closed",
                ));
            }
            // Admission serializes the closed check with taking this shared
            // physical owner. A sealer cannot pass between those two steps.
            flock(&self.active, FlockOperation::NonBlockingLockShared)?;
            flock(&self.admission, FlockOperation::Unlock)?;
            Ok(Arc::new(ArtifactLease {
                _active: self.active,
                _key_directory: self.key,
                _seal_directory: self.seals,
                _root: self.root,
            }))
        }

        fn close(self) -> io::Result<Self> {
            let marker = match self.closed()? {
                Some(marker) => marker,
                None => {
                    let mut marker = File::from(openat(
                        &self.key,
                        "closed",
                        OFlags::CREATE | OFlags::EXCL | OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                        Mode::RUSR | Mode::WUSR,
                    )?);
                    regular(&marker, self.root.metadata()?.dev())?;
                    marker.write_all(self.domain.closed())?;
                    marker
                }
            };
            // No admission is released until the permanent denial and every
            // ancestor entry are durable. A failed/cancelled close never removes it.
            marker.sync_all()?;
            self.admission.sync_all()?;
            self.active.sync_all()?;
            self.key.sync_all()?;
            self.seals.sync_all()?;
            self.root.sync_all()?;
            flock(&self.admission, FlockOperation::Unlock)?;
            Ok(self)
        }
    }

    async fn lock(file: &File, operation: FlockOperation) -> io::Result<()> {
        tokio::time::timeout(LOCK_TIMEOUT, async {
            loop {
                match flock(file, operation) {
                    Ok(()) => return Ok(()),
                    Err(Errno::WOULDBLOCK | Errno::INTR) => tokio::time::sleep(POLL_INTERVAL).await,
                    Err(error) => return Err(error.into()),
                }
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "artifact disk lease lock deadline"))?
    }

    async fn open(root: File, key: [u8; 32], domain: Domain) -> Result<Opened> {
        tokio::task::spawn_blocking(move || Opened::open(root, key, domain))
            .await
            .map_err(DiskError::from)?
            .map_err(DiskError::from)
    }

    pub(super) async fn acquire(root: File, key: [u8; 32], domain: Domain) -> Result<Arc<ArtifactLease>> {
        let opened = open(root, key, domain).await?;
        // Lock order: admission exclusive -> active shared. Contention sleeps
        // asynchronously; no blocking executor thread waits for another owner.
        lock(&opened.admission, FlockOperation::NonBlockingLockExclusive).await?;
        tokio::task::spawn_blocking(move || opened.admit())
            .await
            .map_err(DiskError::from)?
            .map_err(DiskError::from)
    }

    pub(super) async fn seal(root: File, key: [u8; 32], domain: Domain) -> Result<()> {
        let opened = open(root, key, domain).await?;
        lock(&opened.admission, FlockOperation::NonBlockingLockExclusive).await?;
        let opened = tokio::task::spawn_blocking(move || opened.close())
            .await
            .map_err(DiskError::from)??;
        // Closing is already durable. Cancellation or timeout here only loses
        // the confirmation; admission stays permanently closed across reopen.
        lock(&opened.active, FlockOperation::NonBlockingLockExclusive).await?;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{path::Path, time::Duration};

    const KEY: [u8; 32] = [7; 32];

    fn root(path: &Path) -> File {
        use rustix::fs::{Mode, OFlags, open};
        File::from(
            open(
                path,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .expect("open pinned test root"),
        )
    }

    fn directory(path: &Path) -> std::path::PathBuf {
        path.join(Domain::Put.directory())
            .join(hex_simd::encode_to_string(KEY, hex_simd::AsciiCase::Lower))
    }

    async fn closed(path: &Path) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !directory(path).join("closed").exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("sealer reaches its monotonic close point");
    }

    #[tokio::test]
    async fn artifact_repair_closing_domain_is_independent_and_survives_reopen() {
        let temp = tempfile::tempdir().expect("disk root");
        seal(root(temp.path()), KEY).await.expect("PUT closes first");
        let repair = acquire_in(root(temp.path()), KEY, Domain::Repair)
            .await
            .expect("PUT closure permits existing-version repair");
        assert!(acquire(root(temp.path()), KEY).await.is_err());
        drop(repair);
        seal_in(root(temp.path()), KEY, Domain::Repair)
            .await
            .expect("repair closes independently");
        assert!(acquire_in(root(temp.path()), KEY, Domain::Repair).await.is_err());
        seal_in(root(temp.path()), KEY, Domain::Repair)
            .await
            .expect("reopened repair close is durable");

        let other = [8; 32];
        seal_in(root(temp.path()), other, Domain::Repair)
            .await
            .expect("close only repair admission");
        drop(
            acquire(root(temp.path()), other)
                .await
                .expect("repair closure permits PUT domain"),
        );
    }

    #[tokio::test]
    async fn artifact_repair_cancelled_waiter_keeps_physical_owner_and_durable_close() {
        let temp = tempfile::tempdir().expect("disk root");
        let writer = acquire_in(root(temp.path()), KEY, Domain::Repair)
            .await
            .expect("repair admitted");
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, waiting) = std::sync::mpsc::channel();
        let owner = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let _owner = writer;
                let _ = entered.send(());
                let _ = waiting.recv();
            })
            .await
            .expect("repair worker");
        });
        started.await.expect("physical repair entered");
        owner.abort();
        assert!(owner.await.expect_err("cancel repair waiter").is_cancelled());
        let sealer = tokio::spawn(seal_in(root(temp.path()), KEY, Domain::Repair));
        let marker = temp
            .path()
            .join(Domain::Repair.directory())
            .join(hex_simd::encode_to_string(KEY, hex_simd::AsciiCase::Lower))
            .join("closed");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !marker.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("repair admission durably closes");
        assert!(!sealer.is_finished(), "ARTIFACT_REPAIR_CANCELLED_PHYSICAL_TAIL");
        sealer.abort();
        assert!(sealer.await.expect_err("cancel repair close waiter").is_cancelled());
        assert!(acquire_in(root(temp.path()), KEY, Domain::Repair).await.is_err());
        let pending = seal_in(root(temp.path()), KEY, Domain::Repair);
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending).await.is_err(),
            "ARTIFACT_REPAIR_REOPEN_MUST_DRAIN_PHYSICAL_TAIL"
        );
        release.send(()).expect("release physical repair");
        tokio::time::timeout(Duration::from_secs(5), &mut pending)
            .await
            .expect("physical drain bounded")
            .expect("reopened close confirms repair drain");
        assert!(acquire_in(root(temp.path()), KEY, Domain::Repair).await.is_err());
    }

    #[tokio::test]
    async fn artifact_put_seal_waits_for_independent_openers_and_last_clone() {
        let temp = tempfile::tempdir().expect("disk root");
        let first = acquire(root(temp.path()), KEY).await.expect("first admitted writer");
        let tail = first.clone();
        let second = acquire(root(temp.path()), KEY).await.expect("independent shared writer");
        let pending = seal(root(temp.path()), KEY);
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending).await.is_err(),
            "ARTIFACT_PUT_ACTIVE_WRITERS_DRAIN"
        );
        drop(first);
        drop(second);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending).await.is_err(),
            "ARTIFACT_PUT_PHYSICAL_CLONE_RETAINED"
        );
        assert!(acquire(root(temp.path()), KEY).await.is_err(), "ARTIFACT_PUT_CLOSE_REJECTS_ADMISSION");
        drop(tail);
        tokio::time::timeout(Duration::from_secs(5), &mut pending)
            .await
            .expect("physical tail drain bounded")
            .expect("seal complete");
        seal(root(temp.path()), KEY)
            .await
            .expect("independent reopen preserves closed state");
        assert!(acquire(root(temp.path()), KEY).await.is_err(), "ARTIFACT_PUT_CLOSED_AFTER_REOPEN");
    }

    #[tokio::test]
    async fn artifact_put_close_cancellation_leaves_durable_denial() {
        let temp = tempfile::tempdir().expect("disk root");
        let writer = acquire(root(temp.path()), KEY).await.expect("admitted writer");
        let sealer = tokio::spawn(seal(root(temp.path()), KEY));
        closed(temp.path()).await;
        sealer.abort();
        assert!(sealer.await.expect_err("cancel close waiter").is_cancelled());
        assert!(
            acquire(root(temp.path()), KEY).await.is_err(),
            "ARTIFACT_PUT_CANCELLED_CLOSE_STAYS_CLOSED"
        );
        drop(writer);
        seal(root(temp.path()), KEY).await.expect("reopen finishes physical drain");
        assert!(acquire(root(temp.path()), KEY).await.is_err(), "ARTIFACT_PUT_CANCELLED_CLOSE_REOPEN");
    }

    #[tokio::test]
    async fn artifact_put_physical_worker_retains_lease_after_waiter_cancellation() {
        let temp = tempfile::tempdir().expect("disk root");
        let writer = acquire(root(temp.path()), KEY).await.expect("physical writer admitted");
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, waiting) = std::sync::mpsc::channel();
        let owner = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let _physical_owner = writer;
                let _ = entered.send(());
                let _ = waiting.recv();
            })
            .await
            .expect("physical worker");
        });
        started.await.expect("physical syscall entered");
        owner.abort();
        assert!(owner.await.expect_err("cancel async writer").is_cancelled());
        let pending = seal(root(temp.path()), KEY);
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending).await.is_err(),
            "ARTIFACT_PUT_CANCELLED_PHYSICAL_TAIL"
        );
        release.send(()).expect("release physical worker");
        tokio::time::timeout(Duration::from_secs(5), &mut pending)
            .await
            .expect("physical worker drains")
            .expect("seal confirms real completion");
    }

    #[tokio::test]
    async fn artifact_put_seal_waits_for_another_process() {
        const CHILD_ROOT: &str = "RUSTFS_TEST_ARTIFACT_PUT_SEAL_ROOT";
        if let Some(path) = std::env::var_os(CHILD_ROOT) {
            seal(root(Path::new(&path)), KEY)
                .await
                .expect("child closes and drains parent writer");
            assert!(acquire(root(Path::new(&path)), KEY).await.is_err(), "ARTIFACT_PUT_CHILD_CLOSED");
            return;
        }
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let temp = tempfile::tempdir().expect("disk root");
        let writer = acquire(root(temp.path()), KEY).await.expect("parent owns physical write");
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().expect("test executable"))
                .args([
                    "--exact",
                    "disk::local::artifact_lease::tests::artifact_put_seal_waits_for_another_process",
                ])
                .env(CHILD_ROOT, temp.path())
                .spawn()
                .expect("start independent sealer"),
        );
        closed(temp.path()).await;
        assert!(child.0.try_wait().expect("probe child").is_none(), "ARTIFACT_PUT_CROSS_PROCESS_DRAIN");
        assert!(
            acquire(root(temp.path()), KEY).await.is_err(),
            "ARTIFACT_PUT_CROSS_PROCESS_ADMISSION_CLOSED"
        );
        drop(writer);
        let status = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = child.0.try_wait().expect("child wait") {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("child physically drains");
        assert!(status.success(), "ARTIFACT_PUT_CROSS_PROCESS_SEAL");
        assert!(acquire(root(temp.path()), KEY).await.is_err(), "ARTIFACT_PUT_PROCESS_REOPEN_CLOSED");
    }

    #[tokio::test]
    async fn artifact_put_lease_rejects_symlinks_at_every_seal_component() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        for component in ["directory", "key", "admission.lock", "active.lock", "closed"] {
            let temp = tempfile::tempdir().expect("disk root");
            let outside = tempfile::tempdir().expect("symlink destination");
            let sentinel = outside.path().join("untouched");
            std::fs::write(&sentinel, b"unchanged").expect("sentinel");
            let target = match component {
                "directory" => temp.path().join(Domain::Put.directory()),
                "key" => {
                    std::fs::create_dir(temp.path().join(Domain::Put.directory())).expect("seal directory");
                    std::fs::set_permissions(temp.path().join(Domain::Put.directory()), std::fs::Permissions::from_mode(0o700))
                        .expect("private seal directory");
                    directory(temp.path())
                }
                file => {
                    drop(acquire(root(temp.path()), KEY).await.expect("initialize seal files"));
                    let target = directory(temp.path()).join(file);
                    if file != "closed" {
                        std::fs::remove_file(&target).expect("replace fixture lock");
                    }
                    target
                }
            };
            symlink(
                if matches!(component, "directory" | "key") {
                    outside.path()
                } else {
                    &sentinel
                },
                target,
            )
            .expect("symlink fixture");
            assert!(
                acquire(root(temp.path()), KEY).await.is_err(),
                "ARTIFACT_PUT_SYMLINK_REJECTED: {component}"
            );
            assert!(
                seal(root(temp.path()), KEY).await.is_err(),
                "ARTIFACT_PUT_SYMLINK_SEAL_REJECTED: {component}"
            );
            assert_eq!(std::fs::read(&sentinel).expect("sentinel intact"), b"unchanged");
        }
    }

    #[tokio::test]
    async fn artifact_put_lease_rejects_corrupt_closed_marker() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().expect("disk root");
        drop(acquire(root(temp.path()), KEY).await.expect("initialize seal files"));
        let marker = directory(temp.path()).join("closed");
        std::fs::write(&marker, b"partial").expect("corrupt marker fixture");
        std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o600)).expect("marker permissions");
        assert!(acquire(root(temp.path()), KEY).await.is_err(), "ARTIFACT_PUT_CORRUPT_MARKER_FAILS_CLOSED");
        assert!(seal(root(temp.path()), KEY).await.is_err(), "ARTIFACT_PUT_CORRUPT_MARKER_NOT_CONFIRMED");
        assert_eq!(std::fs::read(marker).expect("corruption retained"), b"partial");
    }

    #[tokio::test]
    async fn artifact_put_lease_stays_on_pinned_root_after_path_replacement() {
        let temp = tempfile::tempdir().expect("disk root parent");
        let current = temp.path().join("current");
        let original = temp.path().join("original");
        std::fs::create_dir(&current).expect("initial disk root");
        let pin = root(&current);
        drop(
            acquire(pin.try_clone().expect("root pin clone"), KEY)
                .await
                .expect("initialize pinned seal"),
        );
        std::fs::rename(&current, &original).expect("replace configured pathname");
        std::fs::create_dir(&current).expect("replacement disk root");
        seal(pin, KEY).await.expect("close original disk through its descriptor");
        assert!(directory(&original).join("closed").exists(), "ARTIFACT_PUT_ROOT_PINNED");
        assert!(!current.join(Domain::Put.directory()).exists(), "ARTIFACT_PUT_REPLACEMENT_UNTOUCHED");
        assert!(acquire(root(&original), KEY).await.is_err());
    }
}
