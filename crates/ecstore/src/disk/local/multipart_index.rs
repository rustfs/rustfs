// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Bucket-to-staging-root candidates: one marker per staging root, recorded
//! before the root is published and pruned once the root is gone. Source
//! metadata stays authoritative.
//!
//! A marker without a staging root is either a leftover or an upload whose
//! root is still being published. The publisher proves the second case by
//! holding a [`PublicationPin`] from before it records the marker until its
//! filesystem work has returned; no elapsed time is taken as that proof.

use super::{LocalDisk, read_metadata_only_std};
use crate::disk::{
    DiskAPI, RUSTFS_META_MULTIPART_BUCKET, STORAGE_FORMAT_FILE,
    error::{DiskError, Result},
    multipart_index::{MULTIPART_INDEX_PAGE_SIZE, MultipartIndexPage},
    os,
};
use rustfs_filemeta::{FileInfo, FileInfoOpts, get_file_info};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::{
    collections::{BTreeSet, HashMap},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, LazyLock, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};
use tokio::sync::Notify;
use uuid::Uuid;

const INDEX: &str = ".rustfs.sys/multipart-index-v1";
const REBUILD_DIRENTS_PER_JOB: usize = 64;
type DirectoryEntries = Box<dyn Iterator<Item = io::Result<String>> + Send>;
type PhysicalRoot = (u64, [u8; 16]);
static OWNERS: LazyLock<parking_lot::Mutex<HashMap<PhysicalRoot, Weak<IndexOwner>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

pub(super) struct IndexOwner {
    // Revalidate the configured path against this owner's physical identity.
    root_path: Arc<PathBuf>,
    physical: PhysicalRoot,
    #[cfg(unix)]
    _identity_pin: std::fs::File,
    // Linux names _identity_pin so retiring a caller's root cannot retarget it.
    base: PathBuf,
    epoch: Uuid,
    // Retain this physical owner's queue across bootstrap and disk-handle gaps.
    blocking_queue: Arc<os::DiskBlockingQueue>,
    // No persisted readiness is trusted: an old binary may have written while
    // this process was stopped. Same-process handles share the continuous owner.
    initialized: OnceLock<Arc<IndexBootstrap>>,
    // Set while some task drives the bootstrap scan; cleared by that driver's
    // drop so a cancelled scan can be resumed by the next caller.
    driving: AtomicBool,
    disabled: AtomicBool,
    // Markers whose staging root some publisher is creating right now, with
    // the number of publishers. Boot-local like the index itself: every writer
    // of this physical drive in this process shares this owner.
    publishing: parking_lot::Mutex<HashMap<PathBuf, usize>>,
    #[cfg(test)]
    rebuild_head_pause: parking_lot::Mutex<Option<RebuildHeadPause>>,
    #[cfg(test)]
    publication_pause: parking_lot::Mutex<Option<RebuildHeadPause>>,
    #[cfg(test)]
    observed_rebuild_queue: parking_lot::Mutex<Option<Arc<os::DiskBlockingQueue>>>,
    #[cfg(test)]
    rebuild_single_job: AtomicBool,
}

struct IndexBootstrap {
    result: AtomicU8,
    completed: Notify,
}

fn sha(value: &str) -> String {
    rustfs_utils::crypto::hex(Sha256::digest(value.as_bytes()))
}
fn is_sha(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
fn canonical_upload(path: &str) -> Option<&str> {
    let (root, upload) = path.split_once('/')?;
    (is_sha(root) && !upload.is_empty() && !upload.contains('/') && upload != "." && upload != "..").then_some(root)
}

struct IndexDirectory {
    #[cfg(unix)]
    directory: std::fs::File,
    #[cfg(not(unix))]
    path: PathBuf,
    #[cfg(not(unix))]
    _guard: os::ExistingBaseDirectoryGuard,
}
impl IndexDirectory {
    fn entries(&self) -> io::Result<DirectoryEntries> {
        #[cfg(unix)]
        {
            Ok(Box::new(rustix::fs::Dir::read_from(&self.directory)?.map(|entry| {
                entry
                    .map(|entry| entry.file_name().to_string_lossy().into_owned())
                    .map_err(io::Error::from)
            })))
        }
        #[cfg(not(unix))]
        {
            Ok(Box::new(
                std::fs::read_dir(&self.path)?.map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned())),
            ))
        }
    }
    fn open_file(&self, name: &str) -> io::Result<std::fs::File> {
        #[cfg(unix)]
        {
            Ok(std::fs::File::from(rustix::fs::openat(
                &self.directory,
                name,
                rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )?))
        }
        #[cfg(not(unix))]
        {
            std::fs::File::open(self.path.join(name))
        }
    }
    fn remove_file(&self, name: &str) -> io::Result<()> {
        #[cfg(unix)]
        {
            Ok(rustix::fs::unlinkat(&self.directory, name, rustix::fs::AtFlags::empty())?)
        }
        #[cfg(not(unix))]
        {
            std::fs::remove_file(self.path.join(name))
        }
    }
}

/// Resume the same pinned iterators, rather than reopening and rescanning a
/// growing directory for each page. Only successful EOF can assert coverage.
#[derive(Default)]
struct RebuildCursor {
    roots: Option<DirectoryEntries>,
    uploads: Option<(PathBuf, DirectoryEntries)>,
    started: bool,
}

impl RebuildCursor {
    fn step(&mut self, owner: &IndexOwner) -> io::Result<bool> {
        owner.ensure_current_root()?;
        if !self.started {
            self.started = true;
            match owner.directory(Path::new(RUSTFS_META_MULTIPART_BUCKET)) {
                Ok(directory) => self.roots = Some(directory.entries()?),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    owner.ensure_current_root()?;
                    return Ok(true);
                }
                Err(error) => return Err(error),
            }
        }
        for _ in 0..REBUILD_DIRENTS_PER_JOB {
            if let Some((relative, entries)) = self.uploads.as_mut() {
                let Some(upload) = entries.next() else {
                    self.uploads = None;
                    continue;
                };
                let upload = upload?;
                if matches!(upload.as_str(), "." | "..") {
                    continue;
                }
                let source = match owner.directory(&relative.join(&upload)) {
                    Ok(source) => source,
                    Err(error) if matches!(error.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => continue,
                    Err(error) => return Err(error),
                };
                let metadata = match source.open_file(STORAGE_FORMAT_FILE) {
                    Ok(file) => read_metadata_only_std(Ok(file)).map_err(io::Error::other)?.0,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                // The head is decoded; release the source pin before indexing. On
                // Windows the pin denies delete sharing, so holding it would make a
                // concurrent abort of this upload fail with a sharing violation.
                drop(source);
                let source_root = relative
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| io::Error::other("invalid multipart source root"))?;
                let path = format!("{source_root}/{upload}");
                let info = get_file_info(
                    &metadata,
                    RUSTFS_META_MULTIPART_BUCKET,
                    &path,
                    "",
                    FileInfoOpts {
                        data: false,
                        include_free_versions: false,
                        include_part_checksums: false,
                    },
                )
                .map_err(io::Error::other)?;
                #[cfg(test)]
                owner.pause_rebuild_head_for_test();
                owner.record(&path, &info)?;
                owner.ensure_current_root()?;
                return Ok(false);
            }
            let Some(name) = self.roots.as_mut().and_then(Iterator::next) else {
                owner.ensure_current_root()?;
                return Ok(true);
            };
            let name = name?;
            if !is_sha(&name) {
                continue;
            }
            let relative = Path::new(RUSTFS_META_MULTIPART_BUCKET).join(name);
            match owner.directory(&relative) {
                Ok(directory) => self.uploads = Some((relative, directory.entries()?)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            }
        }
        owner.ensure_current_root()?;
        Ok(false)
    }
}

#[cfg(test)]
struct RebuildHeadPause {
    started: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

impl IndexOwner {
    #[cfg(test)]
    fn pause_rebuild_head_for_test(&self) {
        let pause = self.rebuild_head_pause.lock().take();
        if let Some(pause) = pause {
            let _ = pause.started.send(());
            let _ = pause.release.recv_timeout(std::time::Duration::from_secs(10));
        }
    }

    pub(super) fn acquire(root: &os::PublicationRoot, logical_root: &Path, base: &Path) -> Result<Arc<Self>> {
        let (device, inode) = root.physical_identity()?;
        let key = (device, inode);
        let mut owners = OWNERS.lock();
        owners.retain(|_, owner| owner.strong_count() != 0);
        if let Some(owner) = owners.get(&key).and_then(Weak::upgrade) {
            return Ok(owner);
        }
        #[cfg(unix)]
        let identity_pin = {
            use std::os::unix::fs::MetadataExt;
            let pin = root.open_readonly(base)?;
            let metadata = pin.metadata()?;
            if metadata.dev() != device || metadata.ino().to_le_bytes() != inode[..8] {
                return Err(DiskError::DiskNotFound);
            }
            pin
        };
        #[cfg(target_os = "linux")]
        let base = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/proc/self/fd/{}/.", identity_pin.as_raw_fd()))
        };
        #[cfg(not(target_os = "linux"))]
        let base = base.to_owned();
        let blocking_queue = os::register_disk_blocking_queue(logical_root, &base);
        let owner = Arc::new(Self {
            root_path: Arc::new(root.path().to_path_buf()),
            physical: key,
            #[cfg(unix)]
            _identity_pin: identity_pin,
            base,
            epoch: Uuid::new_v4(),
            blocking_queue,
            initialized: OnceLock::new(),
            driving: AtomicBool::new(false),
            disabled: AtomicBool::new(false),
            publishing: parking_lot::Mutex::new(HashMap::new()),
            #[cfg(test)]
            rebuild_head_pause: parking_lot::Mutex::new(None),
            #[cfg(test)]
            publication_pause: parking_lot::Mutex::new(None),
            #[cfg(test)]
            observed_rebuild_queue: parking_lot::Mutex::new(None),
            #[cfg(test)]
            rebuild_single_job: AtomicBool::new(false),
        });
        owners.insert(key, Arc::downgrade(&owner));
        Ok(owner)
    }

    /// Attach a reconnect's logical root and new pinned descriptor to the queue
    /// already retained by this physical owner. Existing jobs keep their old alias.
    pub(super) fn attach_blocking_queue(&self, root: &Path, io_root: &Path) -> Arc<os::DiskBlockingQueue> {
        let _ = os::register_disk_blocking_queue(&self.base, root);
        let _ = os::register_disk_blocking_queue(root, io_root);
        self.blocking_queue.clone()
    }

    /// Hand out the one owned bootstrap scan. The caller drives the returned
    /// future on a task it already owns (the disk's cleanup loop), so no task is
    /// spawned per disk and dropping the future releases this owner; the next
    /// `start` resumes an unfinished scan. Until EOF, callers retain complete
    /// legacy discovery; a failed scan disables only this optimization.
    pub(super) fn start(self: &Arc<Self>) -> Option<impl Future<Output = ()> + Send + 'static> {
        let bootstrap = self
            .initialized
            .get_or_init(|| {
                Arc::new(IndexBootstrap {
                    result: AtomicU8::new(0),
                    completed: Notify::new(),
                })
            })
            .clone();
        if bootstrap.result.load(Ordering::Acquire) != 0 || self.driving.swap(true, Ordering::AcqRel) {
            return None;
        }
        struct Driver(Arc<IndexOwner>);
        impl Drop for Driver {
            fn drop(&mut self) {
                self.0.driving.store(false, Ordering::Release);
            }
        }
        let driver = Driver(self.clone());
        Some(async move {
            let success = driver.0.rebuild().await.is_ok();
            // A completion waiter may drop the final disk handle and reopen it.
            // Release this boot owner before publishing completion to that waiter.
            drop(driver);
            bootstrap.result.store(if success { 1 } else { 2 }, Ordering::Release);
            bootstrap.completed.notify_waiters();
        })
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(super) async fn initialize(self: &Arc<Self>) {
        if let Some(scan) = self.start() {
            scan.await;
        }
        let Some(bootstrap) = self.initialized.get() else {
            return;
        };
        loop {
            let notified = bootstrap.completed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if bootstrap.result.load(Ordering::Acquire) != 0 {
                return;
            }
            notified.await;
        }
    }

    fn record(&self, path: &str, info: &FileInfo) -> io::Result<()> {
        let Some(source_root) = canonical_upload(path) else {
            return Ok(());
        };
        let bucket = info.metadata.get(crate::disk::multipart_index::RUSTFS_MULTIPART_BUCKET_KEY);
        let object = info.metadata.get(crate::disk::multipart_index::RUSTFS_MULTIPART_OBJECT_KEY);
        let (Some(bucket), Some(object)) = (bucket, object) else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "multipart index owner is missing"));
        };
        if bucket.is_empty() || object.is_empty() || sha(&format!("{bucket}/{object}")) != source_root {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "multipart index owner does not match staging root",
            ));
        }
        self.ensure_current_root()?;
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags, mkdirat, openat};
            let open_dir = |parent: rustix::fd::BorrowedFd<'_>, component: &str| -> io::Result<rustix::fd::OwnedFd> {
                let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
                match openat(parent, component, flags, Mode::empty()) {
                    Ok(dir) => Ok(dir),
                    Err(rustix::io::Errno::NOENT) => {
                        match mkdirat(parent, component, Mode::RWXU | Mode::RWXG | Mode::RWXO) {
                            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                            Err(error) => return Err(error.into()),
                        }
                        openat(parent, component, flags, Mode::empty()).map_err(Into::into)
                    }
                    Err(error) => Err(error.into()),
                }
            };
            use std::os::fd::AsFd;
            let bucket_sha = sha(bucket);
            let sys_dir = open_dir(self._identity_pin.as_fd(), ".rustfs.sys")?;
            let index_dir = open_dir(sys_dir.as_fd(), "multipart-index-v1")?;
            let parent = open_dir(index_dir.as_fd(), &bucket_sha)?;
            // The index is boot-local: every start rescans the staging area
            // before serving it, so a marker lost to a crash is recreated before
            // it can be consulted. Creation is enough; no fsync of the marker or
            // its ancestors. An existing marker needs nothing: the caller's
            // publication pin, not the marker's age, keeps it from being pruned.
            match openat(
                &parent,
                source_root,
                OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            ) {
                Ok(marker) => {
                    if !std::fs::File::from(marker).metadata()?.is_file() {
                        return Err(io::Error::other("index marker is not a file"));
                    }
                }
                Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(error.into()),
            }
        }
        #[cfg(not(unix))]
        {
            let root = os::PublicationRoot::new(&self.root_path)?;
            let directory = self.base.join(INDEX).join(sha(bucket));
            let marker = directory.join(source_root);
            let _parents = os::mkdir_all_below_existing_base_std(&directory, &self.base, &root)?;
            os::replace_multipart_source_metadata(&marker, b"1", &self.base, &root)?;
            os::sync_multipart_owned_record_parents(&self.base.join(".rustfs.sys"), &directory, true, &self.base, &root)?;
        }
        Ok(())
    }

    /// Declare that the caller is about to publish the staging root of
    /// `marker`. Must precede the marker lease the caller records under, so a
    /// prune that found no pin has released that lease before the marker is
    /// (re)created.
    fn pin_publication(self: &Arc<Self>, marker: PathBuf) -> PublicationPin {
        *self.publishing.lock().entry(marker.clone()).or_default() += 1;
        PublicationPin {
            owner: Arc::clone(self),
            marker,
        }
    }

    fn is_publishing(&self, marker: &Path) -> bool {
        self.publishing.lock().contains_key(marker)
    }

    fn ensure_current_root(&self) -> io::Result<()> {
        #[cfg(unix)]
        let current = {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::symlink_metadata(&*self.root_path)?;
            if !metadata.is_dir() {
                return Err(io::Error::other("multipart index physical root is not a directory"));
            }
            let mut inode = [0; 16];
            inode[..8].copy_from_slice(&metadata.ino().to_le_bytes());
            (metadata.dev(), inode)
        };
        #[cfg(not(unix))]
        let current = os::PublicationRoot::new(&self.root_path)?.physical_identity()?;
        if current != self.physical {
            self.disabled.store(true, Ordering::Release);
            return Err(io::Error::other("multipart index physical root changed"));
        }
        Ok(())
    }

    fn directory(&self, relative: &Path) -> io::Result<IndexDirectory> {
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags, openat};
            let mut directory = self._identity_pin.try_clone()?;
            for component in relative.components() {
                let std::path::Component::Normal(component) = component else {
                    return Err(io::Error::other("invalid index directory component"));
                };
                directory = std::fs::File::from(openat(
                    &directory,
                    component,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )?);
            }
            Ok(IndexDirectory { directory })
        }
        #[cfg(not(unix))]
        {
            let path = self.base.join(relative);
            let root = os::PublicationRoot::new(&self.root_path)?;
            let guard = os::pin_multipart_source_directory(&path, &self.base, &root)?;
            Ok(IndexDirectory { path, _guard: guard })
        }
    }

    async fn rebuild(self: &Arc<Self>) -> io::Result<()> {
        let mut cursor = RebuildCursor::default();
        loop {
            let owner = self.clone();
            // Each lease/job owns at most one head and a bounded number of dirents.
            // No namespace lock or fsync lane is retained while the next job queues.
            let lease = os::acquire_namespace_mutation_lease_with_owner(&self.base.join(INDEX), None).await;
            #[cfg(test)]
            {
                *self.observed_rebuild_queue.lock() = Some(lease.blocking_queue_for_test());
            }
            let (next, done) = os::run_blocking_namespace_operation_with_sync(lease, true, move || {
                let done = cursor.step(&owner)?;
                #[cfg(test)]
                if owner.rebuild_single_job.load(Ordering::Acquire) {
                    // Negative control: the former whole-scan closure never
                    // returns the worker between heads. Use the real cursor/I/O.
                    let mut done = done;
                    while !done {
                        done = cursor.step(&owner)?;
                    }
                    return Ok((cursor, done));
                }
                Ok((cursor, done))
            })
            .await?;
            cursor = next;
            if done {
                return Ok(());
            }
        }
    }

    /// Record the marker of `path`'s staging root. The returned pin must stay
    /// alive until the filesystem work that publishes the root has returned,
    /// including work that outlives a cancelled caller.
    pub(super) async fn record_info(self: &Arc<Self>, path: &str, info: &FileInfo) -> Result<Option<PublicationPin>> {
        if canonical_upload(path).is_none() {
            return Ok(None);
        }
        let owner = self.clone();
        let path = path.to_owned();
        let info = info.clone();
        let marker = info
            .metadata
            .get(crate::disk::multipart_index::RUSTFS_MULTIPART_BUCKET_KEY)
            .map(|bucket| {
                owner
                    .base
                    .join(INDEX)
                    .join(sha(bucket))
                    .join(path.split('/').next().unwrap_or_default())
            })
            .unwrap_or_else(|| owner.base.join(INDEX));
        let pin = self.pin_publication(marker.clone());
        let lease = os::acquire_namespace_mutation_lease_with_owner(&marker, None).await;
        os::run_blocking_namespace_operation_with_sync(lease, true, move || {
            match owner.record(&path, &info) {
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    // Preserve legacy malformed-metadata write behavior, but
                    // never claim index completeness after an unindexable write.
                    owner.disabled.store(true, Ordering::Release);
                    Ok(())
                }
                result => result,
            }
        })
        .await?;
        Ok(Some(pin))
    }

    pub(super) async fn prepare_record_head(self: &Arc<Self>, path: &str, bytes: &[u8]) -> Option<PreparedHeadRecord> {
        let upload = path
            .strip_suffix("/xl.meta")
            .filter(|upload| canonical_upload(upload).is_some())?;
        let info = match get_file_info(
            bytes,
            RUSTFS_META_MULTIPART_BUCKET,
            upload,
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: false,
            },
        ) {
            Ok(info) => info,
            Err(_) => {
                self.disabled.store(true, Ordering::Release);
                return None;
            }
        };
        let marker = info
            .metadata
            .get(crate::disk::multipart_index::RUSTFS_MULTIPART_BUCKET_KEY)
            .map(|bucket| {
                self.base
                    .join(INDEX)
                    .join(sha(bucket))
                    .join(upload.split('/').next().unwrap_or_default())
            })
            .unwrap_or_else(|| self.base.join(INDEX));
        let pin = self.pin_publication(marker.clone());
        let lease = os::acquire_namespace_mutation_lease_with_owner(&marker, None).await;
        Some(PreparedHeadRecord {
            upload: upload.to_owned(),
            info,
            pin,
            _lease: lease,
        })
    }

    /// Record the marker of the staging root that `path` belongs to. See
    /// [`Self::record_info`] for how long the returned pin must live.
    pub(super) async fn record_head(self: &Arc<Self>, path: &str, bytes: &[u8]) -> Result<Option<PublicationPin>> {
        let Some(prepared) = self.prepare_record_head(path, bytes).await else {
            return Ok(None);
        };
        prepared.run().await.map(Some)
    }

    async fn page(self: &Arc<Self>, bucket: &str, marker: Option<&str>, disk_id: Uuid) -> Result<MultipartIndexPage> {
        if self.disabled.load(Ordering::Acquire)
            || self
                .initialized
                .get()
                .is_none_or(|bootstrap| bootstrap.result.load(Ordering::Acquire) != 1)
            || marker.is_some_and(|value| !is_sha(value))
        {
            return Err(DiskError::MethodNotAllowed);
        }
        let owner = self.clone();
        let bucket = bucket.to_owned();
        let marker = marker.map(str::to_owned);
        let (page, stale) = crate::disk::io_admission::spawn_blocking_with_disk_io_permit(None, move || {
            owner.ensure_current_root()?;
            let directory = match owner.directory(&Path::new(INDEX).join(sha(&bucket))) {
                Ok(directory) => Some(directory),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(DiskError::from(error)),
            };
            let entries = directory.as_ref().map(IndexDirectory::entries).transpose()?;
            let mut roots = BTreeSet::new();
            for entry in entries.into_iter().flatten() {
                let name = entry?;
                if is_sha(&name) && marker.as_ref().is_none_or(|marker| name > *marker) {
                    roots.insert(name);
                    if roots.len() > MULTIPART_INDEX_PAGE_SIZE + 1 {
                        roots.pop_last();
                    }
                }
            }
            owner.ensure_current_root()?;
            if owner.disabled.load(Ordering::Acquire) {
                return Err(DiskError::MethodNotAllowed);
            }
            let more = roots.len() > MULTIPART_INDEX_PAGE_SIZE;
            if more {
                roots.pop_last();
            }
            // The marker advances past every examined name, live or stale, so a
            // pruned name never stalls pagination.
            let next_marker = more.then(|| roots.last().cloned()).flatten();
            // Completion and abort remove the staging root, not the marker, so
            // markers outlive their uploads. Drop the ones whose staging root is
            // gone, keeping the index proportional to live uploads rather than
            // to every key ever multipart-initiated on this drive.
            let mut live = Vec::with_capacity(roots.len());
            let mut stale = Vec::new();
            let markers = owner.base.join(INDEX).join(sha(&bucket));
            for root in roots {
                // The marker precedes its staging root: a pinned one is an
                // upload in flight and stays a candidate. The pin is read
                // before the root, so a publisher that finished in between has
                // already made its root visible.
                if owner.is_publishing(&markers.join(&root)) {
                    live.push(root);
                    continue;
                }
                match owner.directory(&Path::new(RUSTFS_META_MULTIPART_BUCKET).join(&root)) {
                    Ok(_) => live.push(root),
                    Err(error) if matches!(error.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => {
                        stale.push(root);
                    }
                    Err(error) => return Err(DiskError::from(error)),
                }
            }
            Ok((
                MultipartIndexPage {
                    bucket,
                    disk_id,
                    epoch: owner.epoch,
                    roots: live,
                    next_marker,
                },
                stale,
            ))
        })
        .await
        .map_err(DiskError::from)??;
        for root in stale {
            self.prune_marker(&page.bucket, &root).await?;
        }
        Ok(page)
    }

    /// Remove one marker whose staging root was gone when a page examined it.
    /// The marker's own namespace lease serializes this with recording. Under
    /// that lease the publication pin is read first and the staging root
    /// second: a publisher pinned before the read keeps the marker, one that
    /// already finished has made its root visible, and one that pins later
    /// records its marker only after this lease is released.
    async fn prune_marker(self: &Arc<Self>, bucket: &str, root: &str) -> Result<()> {
        let owner = self.clone();
        let bucket_directory = Path::new(INDEX).join(sha(bucket));
        let marker = owner.base.join(&bucket_directory).join(root);
        let root = root.to_owned();
        let lease = os::acquire_namespace_mutation_lease_with_owner(&marker, None).await;
        // Durable-mutation lane: a page may prune up to 256 markers, which must
        // not queue behind every other mutation on the one-worker foreground lane.
        os::run_blocking_namespace_operation_with_sync(lease, true, move || {
            owner.ensure_current_root()?;
            if owner.is_publishing(&marker) {
                return Ok(());
            }
            match owner.directory(&Path::new(RUSTFS_META_MULTIPART_BUCKET).join(&root)) {
                Ok(_) => return Ok(()),
                Err(error) if matches!(error.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => {}
                Err(error) => return Err(error),
            }
            let directory = match owner.directory(&bucket_directory) {
                Ok(directory) => directory,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };
            match directory.remove_file(&root) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
                _ => Ok(()),
            }
        })
        .await?;
        Ok(())
    }
}

/// Proof that the staging root of one marker is being published. While any pin
/// for a marker is alive, pruning leaves that marker alone.
#[must_use = "dropping the pin lets a concurrent listing prune the marker before its staging root exists"]
pub(super) struct PublicationPin {
    owner: Arc<IndexOwner>,
    marker: PathBuf,
}

impl PublicationPin {
    /// Test seam between recording the marker and publishing the staging root.
    #[cfg(test)]
    pub(super) fn pause_for_test(&self) {
        let pause = self.owner.publication_pause.lock().take();
        if let Some(pause) = pause {
            let _pause = os::enter_blocking_test_hook();
            let _ = pause.started.send(());
            let _ = pause.release.recv_timeout(std::time::Duration::from_secs(10));
        }
    }
}

impl Drop for PublicationPin {
    fn drop(&mut self) {
        let mut publishing = self.owner.publishing.lock();
        if let Some(count) = publishing.get_mut(&self.marker) {
            *count -= 1;
            if *count == 0 {
                publishing.remove(&self.marker);
            }
        }
    }
}

pub(super) struct PreparedHeadRecord {
    upload: String,
    info: FileInfo,
    pin: PublicationPin,
    _lease: Arc<os::NamespaceMutationLease>,
}

impl PreparedHeadRecord {
    /// Record the marker and release its lease. The caller publishes the
    /// staging root while it holds the returned pin.
    pub(super) fn run_std(self) -> io::Result<PublicationPin> {
        let Self {
            upload,
            info,
            pin,
            _lease,
        } = self;
        match pin.owner.record(&upload, &info) {
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                pin.owner.disabled.store(true, Ordering::Release);
                Ok(pin)
            }
            Err(error) => Err(error),
            Ok(()) => Ok(pin),
        }
    }

    async fn run(self) -> Result<PublicationPin> {
        let lease = Arc::clone(&self._lease);
        Ok(os::run_blocking_namespace_operation_with_sync(lease, true, move || self.run_std()).await?)
    }
}

impl LocalDisk {
    #[cfg(any(test, feature = "test-util"))]
    pub(in crate::disk) async fn wait_for_multipart_index_bootstrap(&self) {
        self.multipart_index.initialize().await;
    }

    pub(super) async fn multipart_index_page(&self, bucket: &str, marker: Option<&str>) -> Result<MultipartIndexPage> {
        let disk_id = self
            .get_disk_id()
            .await?
            .filter(|id| !id.is_nil())
            .ok_or(DiskError::DiskNotFound)?;
        self.multipart_index.page(bucket, marker, disk_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::{
        endpoint::Endpoint,
        multipart_index::{RUSTFS_MULTIPART_BUCKET_KEY, RUSTFS_MULTIPART_OBJECT_KEY},
    };
    use rustfs_filemeta::FileMeta;

    fn upload(bucket: &str, key: &str) -> (String, FileInfo) {
        let path = format!("{}/{}x1", sha(&format!("{bucket}/{key}")), Uuid::new_v4());
        let mut info = FileInfo::new(&path, 2, 2);
        info.erasure.index = 1;
        info.data_dir = Some(Uuid::new_v4());
        info.mod_time = Some(time::OffsetDateTime::now_utc());
        info.fresh = true;
        info.metadata.insert(RUSTFS_MULTIPART_BUCKET_KEY.into(), bucket.into());
        info.metadata.insert(RUSTFS_MULTIPART_OBJECT_KEY.into(), key.into());
        (path, info)
    }

    fn seed_legacy(root: &Path, path: &str, info: FileInfo) {
        let directory = root.join(RUSTFS_META_MULTIPART_BUCKET).join(path);
        std::fs::create_dir_all(&directory).expect("legacy source directory");
        let mut metadata = FileMeta::new();
        metadata.add_version(info).expect("legacy metadata");
        std::fs::write(directory.join(STORAGE_FORMAT_FILE), metadata.marshal_msg().expect("encode legacy head"))
            .expect("old binary source publication without index");
    }

    async fn open(root: &Path) -> LocalDisk {
        let endpoint = Endpoint::try_from(root.to_str().expect("UTF-8 test path")).expect("disk endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("disk must remain usable");
        tokio::time::timeout(std::time::Duration::from_secs(5), disk.multipart_index.initialize())
            .await
            .expect("bootstrap completes (success or fallback)");
        disk
    }

    #[tokio::test]
    async fn multipart_index_backfills_legacy_and_indexes_old_write_metadata_after_readiness() {
        let root = tempfile::tempdir().expect("disk root");
        let (before, info) = upload("bucket", "before-upgrade");
        seed_legacy(root.path(), &before, info);
        let disk = open(root.path()).await;
        let identity = Uuid::new_v4();
        let page = disk
            .multipart_index
            .page("bucket", None, identity)
            .await
            .expect("covered startup page");
        assert_eq!(page.roots, vec![before.split('/').next().expect("source sha")]);

        let (after, info) = upload("bucket", "old-coordinator-after-upgrade");
        disk.write_metadata("bucket", RUSTFS_META_MULTIPART_BUCKET, &after, info)
            .await
            .expect("unchanged old coordinator metadata RPC");
        let page = disk.multipart_index.page("bucket", None, identity).await.expect("live page");
        assert_eq!(page.roots.len(), 2);
        assert!(page.roots.iter().any(|root| after.starts_with(root)));
        assert!(
            disk.multipart_index
                .page("other-bucket", None, identity)
                .await
                .expect("other tenant page")
                .roots
                .is_empty()
        );
    }

    #[tokio::test]
    async fn multipart_index_reconnect_shares_owner_but_reboot_rescans_old_writes() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = open(root.path()).await;
        let reconnect = open(root.path()).await;
        assert!(Arc::ptr_eq(&disk.multipart_index, &reconnect.multipart_index));
        let epoch = disk.multipart_index.epoch;
        drop(disk);
        drop(reconnect);
        let (path, info) = upload("bucket", "written-by-rolled-back-node");
        seed_legacy(root.path(), &path, info);
        let restarted = open(root.path()).await;
        let page = restarted
            .multipart_index
            .page("bucket", None, Uuid::new_v4())
            .await
            .expect("new boot backfill");
        assert_ne!(page.epoch, epoch);
        assert_eq!(page.roots, vec![path.split('/').next().expect("source sha")]);
    }

    #[tokio::test]
    async fn multipart_index_marker_failure_precedes_authoritative_source_publication() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = open(root.path()).await;
        let (path, info) = upload("bucket", "must-not-commit");
        std::fs::write(root.path().join(INDEX), b"blocks marker directory").expect("inject marker persistence failure");
        disk.write_metadata("bucket", RUSTFS_META_MULTIPART_BUCKET, &path, info)
            .await
            .expect_err("marker must be durable before successful source publication");
        assert!(
            !root
                .path()
                .join(RUSTFS_META_MULTIPART_BUCKET)
                .join(path)
                .join(STORAGE_FORMAT_FILE)
                .exists()
        );
    }

    #[tokio::test]
    async fn multipart_index_corrupt_backfill_never_claims_an_empty_complete_page() {
        let root = tempfile::tempdir().expect("disk root");
        let (path, info) = upload("bucket", "damaged");
        seed_legacy(root.path(), &path, info);
        std::fs::write(
            root.path()
                .join(RUSTFS_META_MULTIPART_BUCKET)
                .join(&path)
                .join(STORAGE_FORMAT_FILE),
            b"bad metadata",
        )
        .expect("seed damaged legacy metadata");
        let disk = open(root.path()).await;
        assert!(matches!(
            disk.multipart_index.page("bucket", None, Uuid::new_v4()).await,
            Err(DiskError::MethodNotAllowed)
        ));
        let (healthy, info) = upload("bucket", "still-writable");
        disk.write_metadata("bucket", RUSTFS_META_MULTIPART_BUCKET, &healthy, info)
            .await
            .expect("legacy fallback keeps writes available");
        assert!(matches!(
            disk.multipart_index.page("bucket", None, Uuid::new_v4()).await,
            Err(DiskError::MethodNotAllowed)
        ));
    }

    #[tokio::test]
    async fn multipart_index_unindexable_raw_head_revokes_readiness_before_legacy_write() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = open(root.path()).await;
        let (path, info) = upload("bucket", "raw-rewrite");
        disk.write_metadata("bucket", RUSTFS_META_MULTIPART_BUCKET, &path, info)
            .await
            .expect("source birth");
        disk.write_all(
            RUSTFS_META_MULTIPART_BUCKET,
            &format!("{path}/xl.meta"),
            bytes::Bytes::from_static(b"legacy corruption"),
        )
        .await
        .expect("retain existing raw write semantics");
        assert!(matches!(
            disk.multipart_index.page("bucket", None, Uuid::new_v4()).await,
            Err(DiskError::MethodNotAllowed)
        ));
    }

    #[tokio::test]
    async fn multipart_index_pages_are_bounded_and_never_walk_foreign_bucket_roots() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = open(root.path()).await;
        let directory = root.path().join(INDEX).join(sha("bucket"));
        std::fs::create_dir_all(&directory).expect("seed ready candidates");
        for i in 0..(MULTIPART_INDEX_PAGE_SIZE + 2) {
            std::fs::write(directory.join(format!("{i:064x}")), b"1").expect("candidate marker");
            std::fs::create_dir_all(root.path().join(RUSTFS_META_MULTIPART_BUCKET).join(format!("{i:064x}")))
                .expect("live staging root");
        }
        // A foreign bucket's unreadable layout must not enter this page's walk.
        std::fs::write(root.path().join(INDEX).join(sha("foreign")), b"not a directory").expect("foreign layout");
        let disk_id = Uuid::new_v4();
        let first = disk
            .multipart_index
            .page("bucket", None, disk_id)
            .await
            .expect("first bounded page");
        assert_eq!(first.roots.len(), MULTIPART_INDEX_PAGE_SIZE);
        let second = disk
            .multipart_index
            .page("bucket", first.next_marker.as_deref(), disk_id)
            .await
            .expect("second page");
        assert_eq!(second.roots.len(), 2);
        assert_eq!(first.epoch, second.epoch);
        assert!(second.next_marker.is_none());
        assert!(first.roots.last() < second.roots.first());
    }
    #[tokio::test]
    async fn multipart_index_pages_prune_markers_whose_staging_root_is_gone() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = open(root.path()).await;
        let directory = root.path().join(INDEX).join(sha("bucket"));
        std::fs::create_dir_all(&directory).expect("marker directory");
        let live = format!("{:064x}", 1);
        let finished = format!("{:064x}", 2);
        let in_flight = format!("{:064x}", 3);
        for name in [&live, &finished, &in_flight] {
            std::fs::write(directory.join(name), b"1").expect("marker");
        }
        // Completion and abort remove the staging root and leave the marker.
        std::fs::create_dir_all(root.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&live)).expect("live staging root");
        let owner = &disk.multipart_index;
        let publishing = owner.pin_publication(owner.base.join(INDEX).join(sha("bucket")).join(&in_flight));
        let second_publisher = owner.pin_publication(owner.base.join(INDEX).join(sha("bucket")).join(&in_flight));
        let page = owner.page("bucket", None, Uuid::new_v4()).await.expect("page");
        // The in-flight marker has no staging root yet but is pinned: a candidate, not a leftover.
        assert_eq!(page.roots, vec![live.clone(), in_flight.clone()]);
        assert!(page.next_marker.is_none());
        assert!(directory.join(&live).exists(), "a live root keeps its marker");
        assert!(directory.join(&in_flight).exists(), "a pinned marker is kept");
        assert!(!directory.join(&finished).exists(), "a finished root's marker is pruned at once");
        // One of two publishers of the same root finishing does not unpin it.
        drop(publishing);
        let page = owner
            .page("bucket", None, Uuid::new_v4())
            .await
            .expect("page with one publisher left");
        assert_eq!(page.roots, vec![live.clone(), in_flight.clone()]);
        assert!(directory.join(&in_flight).exists(), "the remaining publisher keeps the marker");
        // A publication that ended without a staging root leaves a leftover.
        drop(second_publisher);
        let page = owner
            .page("bucket", None, Uuid::new_v4())
            .await
            .expect("page after the failed publication");
        assert_eq!(page.roots, vec![live]);
        assert!(!directory.join(&in_flight).exists(), "an unpinned marker without a root is pruned");
        assert!(owner.publishing.lock().is_empty(), "finished publishers leave no pin behind");
    }

    fn pause_next_publication(owner: &IndexOwner) -> (tokio::sync::oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (started, waiting) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        *owner.publication_pause.lock() = Some(RebuildHeadPause {
            started,
            release: released,
        });
        (waiting, release)
    }

    /// The window the pin closes: the marker is recorded and its lease is
    /// released, the staging root is not published yet, and a listing examines
    /// the bucket. The marker is dated a day back so that no grace period could
    /// be what keeps it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_index_listing_during_source_publication_keeps_the_new_upload_listed() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = Arc::new(open(root.path()).await);
        let (path, info) = upload("bucket", "published-while-listed");
        let source_root = path.split('/').next().expect("source sha").to_owned();
        let marker = root.path().join(INDEX).join(sha("bucket")).join(&source_root);
        let source_head = root
            .path()
            .join(RUSTFS_META_MULTIPART_BUCKET)
            .join(&path)
            .join(STORAGE_FORMAT_FILE);
        let (paused, release) = pause_next_publication(&disk.multipart_index);
        let writer = tokio::spawn({
            let disk = Arc::clone(&disk);
            let path = path.clone();
            async move { disk.write_metadata("bucket", RUSTFS_META_MULTIPART_BUCKET, &path, info).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), paused)
            .await
            .expect("the publication reaches the pause")
            .expect("pause sender");
        assert!(marker.exists(), "the marker precedes the staging root");
        assert!(!source_head.exists(), "the staging root is not published yet");
        std::fs::File::options()
            .write(true)
            .open(&marker)
            .expect("marker")
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(86_400))
            .expect("age the marker");

        let identity = Uuid::new_v4();
        let page = disk
            .multipart_index
            .page("bucket", None, identity)
            .await
            .expect("page during publication");
        assert_eq!(page.roots, vec![source_root.clone()], "an upload in flight stays a candidate");
        assert!(marker.exists(), "a listing must not prune the marker of an upload in flight");

        release.send(()).expect("resume the publication");
        writer.await.expect("writer task").expect("the upload metadata is published");
        assert!(source_head.exists());
        let page = disk
            .multipart_index
            .page("bucket", None, identity)
            .await
            .expect("page after publication");
        assert_eq!(page.roots, vec![source_root], "the published upload is listed");
        assert!(marker.exists());
        assert!(disk.multipart_index.publishing.lock().is_empty());
    }

    /// A timed-out caller drops its future while the filesystem work goes on.
    /// The pin belongs to that work, so the marker survives until the staging
    /// root the detached work publishes is visible.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_index_cancelled_publisher_keeps_its_marker_until_the_detached_work_returns() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = Arc::new(open(root.path()).await);
        let (path, info) = upload("bucket", "caller-timed-out");
        let source_root = path.split('/').next().expect("source sha").to_owned();
        let marker = root.path().join(INDEX).join(sha("bucket")).join(&source_root);
        let source_head = root
            .path()
            .join(RUSTFS_META_MULTIPART_BUCKET)
            .join(&path)
            .join(STORAGE_FORMAT_FILE);
        let (paused, release) = pause_next_publication(&disk.multipart_index);
        let writer = tokio::spawn({
            let disk = Arc::clone(&disk);
            let path = path.clone();
            async move { disk.write_metadata("bucket", RUSTFS_META_MULTIPART_BUCKET, &path, info).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), paused)
            .await
            .expect("the publication reaches the pause")
            .expect("pause sender");
        writer.abort();
        assert!(writer.await.expect_err("the caller is gone").is_cancelled());

        let identity = Uuid::new_v4();
        let page = disk
            .multipart_index
            .page("bucket", None, identity)
            .await
            .expect("page after the caller left");
        assert_eq!(page.roots, vec![source_root.clone()]);
        assert!(marker.exists(), "the detached publication still owns the marker");

        release.send(()).expect("resume the detached publication");
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !source_head.exists() || !disk.multipart_index.publishing.lock().is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the detached publication completes");
        let page = disk
            .multipart_index
            .page("bucket", None, identity)
            .await
            .expect("page after the detached publication");
        assert_eq!(page.roots, vec![source_root], "the upload the detached work published is listed");
        assert!(marker.exists());
    }

    /// A page classifies a marker as stale before the publisher pins it; the
    /// prune that follows must read the pin again under the marker lease.
    #[tokio::test]
    async fn multipart_index_prune_rechecks_the_pin_under_the_marker_lease() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = open(root.path()).await;
        let owner = &disk.multipart_index;
        let directory = root.path().join(INDEX).join(sha("bucket"));
        std::fs::create_dir_all(&directory).expect("marker directory");
        let name = format!("{:064x}", 9);
        std::fs::write(directory.join(&name), b"1").expect("marker");

        let pin = owner.pin_publication(owner.base.join(INDEX).join(sha("bucket")).join(&name));
        owner.prune_marker("bucket", &name).await.expect("prune of a pinned marker");
        assert!(directory.join(&name).exists(), "a publisher that pinned after the page keeps its marker");

        drop(pin);
        owner.prune_marker("bucket", &name).await.expect("prune of a leftover");
        assert!(!directory.join(&name).exists(), "the same marker is a leftover once nobody publishes it");
    }

    /// The publisher pins before it queues on the marker lease. A prune that
    /// holds the lease and found no pin has therefore unlinked the marker
    /// before the publisher records it, and the record recreates it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_index_publisher_pins_before_it_waits_for_the_marker_lease() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = Arc::new(open(root.path()).await);
        let owner = Arc::clone(&disk.multipart_index);
        let (path, info) = upload("bucket", "queued-behind-a-prune");
        let source_root = path.split('/').next().expect("source sha").to_owned();
        let marker = owner.base.join(INDEX).join(sha("bucket")).join(&source_root);
        let visible_marker = root.path().join(INDEX).join(sha("bucket")).join(&source_root);

        // Stand in for a prune that owns the marker lease and already read "no pin".
        let prune_lease = os::acquire_namespace_mutation_lease_with_owner(&marker, None).await;
        assert!(!owner.is_publishing(&marker));
        let writer = tokio::spawn({
            let disk = Arc::clone(&disk);
            let path = path.clone();
            async move { disk.write_metadata("bucket", RUSTFS_META_MULTIPART_BUCKET, &path, info).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !owner.is_publishing(&marker) {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("the publisher pins while the marker lease is still held by the prune");
        assert!(!visible_marker.exists(), "the marker is recorded only under the lease");

        drop(prune_lease);
        writer.await.expect("writer task").expect("the upload metadata is published");
        assert!(visible_marker.exists(), "the record after the prune recreates the marker");
        let page = owner.page("bucket", None, Uuid::new_v4()).await.expect("page");
        assert_eq!(page.roots, vec![source_root]);
    }

    /// Stale markers at the start of a page, at its cursor, and filling a whole
    /// page: every page still satisfies the receiver's contract and advances.
    #[tokio::test]
    async fn multipart_index_pages_of_stale_markers_keep_valid_cursors() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = open(root.path()).await;
        let directory = root.path().join(INDEX).join(sha("bucket"));
        std::fs::create_dir_all(&directory).expect("marker directory");
        // Page one is entirely stale. Page two is live except its first name
        // and the name its cursor lands on. Two live names follow.
        let total = MULTIPART_INDEX_PAGE_SIZE * 2 + 2;
        let stale = |i: usize| i <= MULTIPART_INDEX_PAGE_SIZE || i == MULTIPART_INDEX_PAGE_SIZE * 2 - 1;
        for i in 0..total {
            let name = format!("{i:064x}");
            std::fs::write(directory.join(&name), b"1").expect("marker");
            if !stale(i) {
                std::fs::create_dir_all(root.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&name)).expect("staging root");
            }
        }
        let disk_id = Uuid::new_v4();
        let first = disk.multipart_index.page("bucket", None, disk_id).await.expect("first page");
        first
            .validate("bucket", None, disk_id)
            .expect("an empty page with a cursor validates");
        assert!(first.roots.is_empty(), "a fully stale page has no roots");
        let cursor = first.next_marker.clone().expect("more markers follow a fully stale page");
        assert_eq!(cursor, format!("{:064x}", MULTIPART_INDEX_PAGE_SIZE - 1));

        let second = disk
            .multipart_index
            .page("bucket", Some(&cursor), disk_id)
            .await
            .expect("second page");
        second
            .validate("bucket", Some(&cursor), disk_id)
            .expect("stale first and cursor names validate");
        assert_eq!(second.roots.len(), MULTIPART_INDEX_PAGE_SIZE - 2);
        assert_eq!(second.roots.first(), Some(&format!("{:064x}", MULTIPART_INDEX_PAGE_SIZE + 1)));
        let cursor = second
            .next_marker
            .clone()
            .expect("the cursor is the stale last examined name");
        assert_eq!(cursor, format!("{:064x}", MULTIPART_INDEX_PAGE_SIZE * 2 - 1));
        assert!(second.roots.last() < Some(&cursor));

        let third = disk
            .multipart_index
            .page("bucket", Some(&cursor), disk_id)
            .await
            .expect("third page");
        third.validate("bucket", Some(&cursor), disk_id).expect("last page validates");
        assert_eq!(third.roots.len(), 2);
        assert!(third.next_marker.is_none());
        let remaining = std::fs::read_dir(&directory).expect("marker directory").count();
        assert_eq!(remaining, total - MULTIPART_INDEX_PAGE_SIZE - 2, "every stale marker is pruned");
    }

    #[tokio::test]
    async fn multipart_index_pruned_page_keeps_a_valid_cursor() {
        let root = tempfile::tempdir().expect("disk root");
        let disk = open(root.path()).await;
        let directory = root.path().join(INDEX).join(sha("bucket"));
        std::fs::create_dir_all(&directory).expect("marker directory");
        let stale = format!("{:064x}", 7);
        for i in 0..(MULTIPART_INDEX_PAGE_SIZE + 2) {
            let name = format!("{i:064x}");
            std::fs::write(directory.join(&name), b"1").expect("marker");
            if name != stale {
                std::fs::create_dir_all(root.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&name)).expect("staging root");
            }
        }
        let disk_id = Uuid::new_v4();
        let first = disk.multipart_index.page("bucket", None, disk_id).await.expect("first page");
        first
            .validate("bucket", None, disk_id)
            .expect("a pruned page still validates");
        assert_eq!(first.roots.len(), MULTIPART_INDEX_PAGE_SIZE - 1, "the stale marker is not a root");
        assert!(!first.roots.contains(&stale));
        let cursor = first.next_marker.clone().expect("more markers follow");
        assert_eq!(
            cursor,
            format!("{:064x}", MULTIPART_INDEX_PAGE_SIZE - 1),
            "the cursor is the last examined name"
        );
        assert!(!directory.join(&stale).exists(), "the stale marker is pruned");
        let second = disk
            .multipart_index
            .page("bucket", Some(&cursor), disk_id)
            .await
            .expect("second page");
        second
            .validate("bucket", Some(&cursor), disk_id)
            .expect("continuation validates");
        assert_eq!(second.roots.len(), 2);
        assert!(second.next_marker.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn multipart_index_aliases_share_readiness_and_root_replacement_cannot_return_empty() {
        let temporary = tempfile::tempdir().expect("alias fixture");
        let actual_parent = temporary.path().join("actual");
        let actual = actual_parent.join("disk");
        std::fs::create_dir_all(&actual).expect("physical root");
        let alias_parent = temporary.path().join("alias");
        std::os::unix::fs::symlink(&actual_parent, &alias_parent).expect("ancestor alias");
        let first = open(&actual).await;
        let alias = open(&alias_parent.join("disk")).await;
        assert!(
            Arc::ptr_eq(&first.multipart_index, &alias.multipart_index),
            "aliases must share physical readiness"
        );
        let (path, info) = upload("bucket", "shared");
        first
            .write_metadata("bucket", RUSTFS_META_MULTIPART_BUCKET, &path, info)
            .await
            .expect("shared upload");
        alias
            .write_all(
                RUSTFS_META_MULTIPART_BUCKET,
                &format!("{path}/xl.meta"),
                bytes::Bytes::from_static(b"bad head"),
            )
            .await
            .expect("legacy alias rewrite");
        assert!(matches!(
            first.multipart_index.page("bucket", None, Uuid::new_v4()).await,
            Err(DiskError::MethodNotAllowed)
        ));
        drop(first);
        drop(alias);
        // Use a separate healthy disk so this assertion is about a root switch,
        // not the explicit readiness invalidation above.
        let root = temporary.path().join("healthy");
        std::fs::create_dir(&root).expect("healthy root");
        let (path, info) = upload("bucket", "before-root-replacement");
        seed_legacy(&root, &path, info);
        let disk = open(&root).await;
        assert_eq!(
            disk.multipart_index
                .page("bucket", None, Uuid::new_v4())
                .await
                .expect("original ready page")
                .roots
                .len(),
            1
        );
        std::fs::rename(&root, temporary.path().join("retired")).expect("retire physical root");
        std::fs::create_dir(&root).expect("new empty physical root");
        assert!(
            disk.multipart_index.page("bucket", None, Uuid::new_v4()).await.is_err(),
            "replacement root must not fabricate a ready empty result"
        );
    }
    #[tokio::test]
    async fn multipart_index_cancelled_bootstrap_waiter_does_not_restart_or_publish_early() {
        let root = tempfile::tempdir().expect("bootstrap root");
        let (path, info) = upload("bucket", "legacy");
        seed_legacy(root.path(), &path, info);
        let publication = os::PublicationRoot::new(root.path()).expect("pinned root");
        #[cfg(target_os = "linux")]
        let base = publication.io_path();
        #[cfg(not(target_os = "linux"))]
        let base = root.path();
        let owner = IndexOwner::acquire(&publication, root.path(), base).expect("bootstrap owner");
        let lock = os::acquire_namespace_mutation_lease_with_owner(&owner.base.join(INDEX), None).await;
        let first = tokio::spawn({
            let owner = owner.clone();
            async move { owner.initialize().await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while owner.initialized.get().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned bootstrap started");
        let bootstrap = owner.initialized.get().expect("shared worker state").clone();
        first.abort();
        let _ = first.await;
        assert_eq!(bootstrap.result.load(Ordering::Acquire), 0, "native admission is still blocked");
        assert!(
            owner.page("bucket", None, Uuid::new_v4()).await.is_err(),
            "incomplete coverage cannot be published"
        );
        let second = tokio::spawn({
            let owner = owner.clone();
            async move { owner.initialize().await }
        });
        tokio::task::yield_now().await;
        assert!(Arc::ptr_eq(&bootstrap, owner.initialized.get().expect("same bootstrap")));
        assert!(!second.is_finished(), "replacement waiter must join the same worker");
        drop(lock);
        tokio::time::timeout(std::time::Duration::from_secs(5), second)
            .await
            .expect("worker completes")
            .expect("waiter");
        assert_eq!(bootstrap.result.load(Ordering::Acquire), 1);
        assert_eq!(
            owner
                .page("bucket", None, Uuid::new_v4())
                .await
                .expect("covered result")
                .roots
                .len(),
            1
        );
    }
    fn owner_for_root(root: &Path) -> Arc<IndexOwner> {
        let publication = os::PublicationRoot::new(root).expect("pinned root");
        #[cfg(target_os = "linux")]
        let base = publication.io_path();
        #[cfg(not(target_os = "linux"))]
        let base = root;
        IndexOwner::acquire(&publication, root, base).expect("bootstrap owner")
    }

    fn pause_next_head(owner: &IndexOwner) -> (tokio::sync::oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (started, waiting) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        *owner.rebuild_head_pause.lock() = Some(RebuildHeadPause {
            started,
            release: released,
        });
        (waiting, release)
    }

    #[tokio::test]
    async fn multipart_index_background_bootstrap_does_not_gate_disk_admission() {
        let root = tempfile::tempdir().expect("disk root");
        let (path, info) = upload("bucket", "legacy");
        seed_legacy(root.path(), &path, info);
        let owner = owner_for_root(root.path());
        let lock = os::acquire_namespace_mutation_lease_with_owner(&owner.base.join(INDEX), None).await;
        let endpoint = Endpoint::try_from(root.path().to_str().expect("root path")).expect("endpoint");
        let disk = tokio::time::timeout(std::time::Duration::from_secs(5), LocalDisk::new(&endpoint, false))
            .await
            .expect("disk must come online while bootstrap is blocked")
            .expect("local disk");
        assert!(Arc::ptr_eq(&owner, &disk.multipart_index));
        assert!(matches!(
            owner.page("bucket", None, Uuid::new_v4()).await,
            Err(DiskError::MethodNotAllowed)
        ));
        disk.write_all(crate::disk::RUSTFS_META_BUCKET, "foreground", bytes::Bytes::from_static(b"online"))
            .await
            .expect("foreground storage remains available before coverage");
        drop(lock);
        tokio::time::timeout(std::time::Duration::from_secs(5), owner.initialize())
            .await
            .expect("bootstrap joins");
        assert_eq!(
            owner
                .page("bucket", None, Uuid::new_v4())
                .await
                .expect("ready page")
                .roots
                .len(),
            1
        );
    }

    async fn check_rebuild_fsync_progress(single_job: bool) {
        use futures::FutureExt;
        let root = tempfile::tempdir().expect("disk root");
        for key in ["first", "second"] {
            let (path, info) = upload("bucket", key);
            seed_legacy(root.path(), &path, info);
        }
        let owner = owner_for_root(root.path());
        owner.rebuild_single_job.store(single_job, Ordering::Release);
        let lock = os::acquire_namespace_mutation_lease_with_owner(&owner.base.join(INDEX), None).await;
        let endpoint = Endpoint::try_from(root.path().to_str().expect("root path")).expect("endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("online disk during bootstrap");
        assert!(Arc::ptr_eq(&owner.blocking_queue, &disk.blocking_queue));
        assert!(Arc::ptr_eq(&lock.blocking_queue_for_test(), &disk.blocking_queue));
        let mut blockers = Vec::new();
        let mut releases = Vec::new();
        // Plain channel barriers deliberately do not call enter_blocking_test_hook:
        // that hook expands worker capacity and would hide starvation here.
        for _ in 1..os::MAX_FSYNC_WORKERS_PER_DISK {
            let queue = disk.blocking_queue.clone();
            let (started, waiting) = tokio::sync::oneshot::channel();
            let (release, released) = std::sync::mpsc::channel();
            blockers.push(tokio::spawn(async move {
                queue
                    .run_fsync_cancellable(move |_| {
                        let _ = started.send(());
                        released
                            .recv_timeout(std::time::Duration::from_secs(10))
                            .map_err(io::Error::other)?;
                        Ok(())
                    })
                    .await
            }));
            tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
                .await
                .expect("worker starts")
                .expect("worker signal");
            releases.push(release);
        }
        let (first_started, release_first) = pause_next_head(&owner);
        drop(lock);
        tokio::time::timeout(std::time::Duration::from_secs(5), first_started)
            .await
            .expect("first head starts")
            .expect("head signal");
        assert!(
            Arc::ptr_eq(
                owner
                    .observed_rebuild_queue
                    .lock()
                    .as_ref()
                    .expect("actual bootstrap lease queue"),
                &disk.blocking_queue,
            ),
            "head job, all blockers and foreground writer must share one queue"
        );
        // This is the real durable PUT file writer, not a no-op queue job. Its
        // first poll enqueues the write behind the eight occupied workers.
        let path = root.path().join("foreground-put");
        let mut write = std::pin::pin!(disk.write_all_internal(
            &path,
            bytes::Bytes::from_static(b"foreground payload"),
            super::super::SyncMode::FileAndDir,
            root.path(),
        ));
        assert!(write.as_mut().now_or_never().is_none(), "write must initially queue");
        let (second_started, release_second) = pause_next_head(&owner);
        release_first.send(()).expect("release first head");
        tokio::time::timeout(std::time::Duration::from_secs(5), second_started)
            .await
            .expect("second head starts")
            .expect("head signal");
        assert_eq!(owner.initialized.get().expect("worker").result.load(Ordering::Acquire), 0);
        if single_job {
            assert!(
                write.as_mut().now_or_never().is_none(),
                "old whole-scan closure still occupies the last worker"
            );
            assert!(!path.exists(), "old scheduling cannot publish while the second head is blocked");
        } else {
            tokio::time::timeout(std::time::Duration::from_secs(5), &mut write)
                .await
                .expect("foreground PUT advances before EOF")
                .expect("durable PUT");
            assert_eq!(std::fs::read(&path).expect("committed foreground bytes"), b"foreground payload");
        }
        release_second.send(()).expect("release second head");
        if single_job {
            tokio::time::timeout(std::time::Duration::from_secs(5), &mut write)
                .await
                .expect("old write progresses only after scan releases worker")
                .expect("durable PUT");
        }
        for release in releases {
            release.send(()).expect("release other worker");
        }
        for blocker in blockers {
            blocker.await.expect("worker join").expect("worker completed");
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), owner.initialize())
            .await
            .expect("bootstrap EOF");
        assert_eq!(
            owner
                .page("bucket", None, Uuid::new_v4())
                .await
                .expect("ready page")
                .roots
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn multipart_index_rebuild_releases_fsync_lane_before_eof_for_foreground_write() {
        check_rebuild_fsync_progress(false).await;
    }

    #[tokio::test]
    async fn multipart_index_whole_scan_negative_control_blocks_foreground_write_until_eof() {
        check_rebuild_fsync_progress(true).await;
    }

    #[tokio::test]
    async fn multipart_index_reconnect_retains_queue_for_old_and_new_pinned_aliases() {
        let root = tempfile::tempdir().expect("disk root");
        let (path, info) = upload("bucket", "legacy");
        seed_legacy(root.path(), &path, info);
        let owner = owner_for_root(root.path());
        let disk = open(root.path()).await;
        assert!(Arc::ptr_eq(&owner, &disk.multipart_index));
        let weak_queue = Arc::downgrade(&disk.blocking_queue);
        owner.observed_rebuild_queue.lock().take();
        drop(disk);
        // No job/namespace lease remains here. The physical owner itself must
        // keep its original queue registration alive across a reconnect gap.
        assert!(weak_queue.upgrade().is_some(), "index owner retains queue between jobs and disk handles");
        let reconnected = open(root.path()).await;
        assert!(Arc::ptr_eq(&owner, &reconnected.multipart_index));
        assert!(Arc::ptr_eq(&weak_queue.upgrade().expect("same queue"), &reconnected.blocking_queue));
        #[cfg(target_os = "linux")]
        assert_ne!(owner.base, reconnected.io_root, "reconnect uses a distinct live pinned descriptor");
        let old = os::acquire_namespace_mutation_lease_with_owner(&owner.base.join(INDEX), None).await;
        assert!(Arc::ptr_eq(&old.blocking_queue_for_test(), &reconnected.blocking_queue));
        drop(old);
        let new = os::acquire_namespace_mutation_lease_with_owner(&reconnected.io_root.join(INDEX), None).await;
        assert!(Arc::ptr_eq(&new.blocking_queue_for_test(), &reconnected.blocking_queue));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn multipart_index_owner_queue_survives_reassigned_caller_alias() {
        const CHILD: &str = "RUSTFS_TEST_INDEX_OWNER_QUEUE_ALIAS_CHILD";
        if std::env::var_os(CHILD).is_none() {
            // Alias reassignment mutates a process-wide registry. Isolate it
            // from concurrent tests that may reuse the retired fd number.
            let mut child = std::process::Command::new(std::env::current_exe().expect("current test binary"))
                .arg("--exact")
                .arg("disk::local::multipart_index::tests::multipart_index_owner_queue_survives_reassigned_caller_alias")
                .arg("--nocapture")
                .env(CHILD, "1")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("start isolated queue-alias regression");
            let completed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
                loop {
                    poll.tick().await;
                    if child.try_wait().expect("poll queue-alias regression").is_some() {
                        break;
                    }
                }
            })
            .await;
            if completed.is_err() {
                child.kill().expect("stop blocked queue-alias regression");
            }
            let output = child.wait_with_output().expect("collect queue-alias regression");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(completed.is_ok(), "queue-alias regression must finish within its deadline");
            assert!(output.status.success(), "queue-alias regression failed: {stdout} {stderr}");
            assert!(stdout.contains("1 passed"), "the child must execute the queue-alias regression: {stdout}");
            return;
        }

        let root = tempfile::tempdir().expect("original disk root");
        let (path, info) = upload("bucket", "legacy");
        seed_legacy(root.path(), &path, info);
        let publication = os::PublicationRoot::new(root.path()).expect("original caller root");
        let caller_alias = publication.io_path().to_path_buf();
        let owner = IndexOwner::acquire(&publication, root.path(), &caller_alias).expect("original physical owner");
        let original_queue = owner.blocking_queue.clone();
        let other_root = tempfile::tempdir().expect("other disk root");
        let other_publication = os::PublicationRoot::new(other_root.path()).expect("other pinned root");
        drop(publication);

        // A retired caller's fd number can be reused by another disk. Assign
        // its alias explicitly so the test does not depend on fd allocation.
        let other_queue = os::register_disk_blocking_queue(other_root.path(), &caller_alias);
        let other_attached = os::register_disk_blocking_queue(other_root.path(), other_publication.io_path());
        assert!(Arc::ptr_eq(&other_queue, &other_attached));
        assert!(!Arc::ptr_eq(&original_queue, &other_queue));

        tokio::time::timeout(std::time::Duration::from_secs(5), owner.initialize())
            .await
            .expect("bootstrap completes after the caller alias is reassigned");
        assert!(Arc::ptr_eq(
            owner.observed_rebuild_queue.lock().as_ref().expect("actual rebuild queue"),
            &original_queue,
        ));
        assert_eq!(
            owner
                .page("bucket", None, Uuid::new_v4())
                .await
                .expect("original disk index coverage")
                .roots
                .len(),
            1,
        );

        let reconnect = os::PublicationRoot::new(root.path()).expect("reconnected caller root");
        let attached = owner.attach_blocking_queue(root.path(), reconnect.io_path());
        assert!(Arc::ptr_eq(&attached, &original_queue));
        for (path, expected_queue) in [
            (owner.base.join(INDEX), &original_queue),
            (reconnect.io_path().join(INDEX), &original_queue),
            (root.path().join(INDEX), &original_queue),
            (other_publication.io_path().join(INDEX), &other_queue),
            (other_root.path().join(INDEX), &other_queue),
        ] {
            let lease = os::acquire_namespace_mutation_lease_with_owner(&path, None).await;
            assert!(
                Arc::ptr_eq(&lease.blocking_queue_for_test(), expected_queue),
                "queue routing for {path:?}"
            );
        }
    }

    #[test]
    fn multipart_index_rebuild_counts_empty_and_invalid_entries_toward_job_budget() {
        let root = tempfile::tempdir().expect("disk root");
        let staging = root.path().join(RUSTFS_META_MULTIPART_BUCKET);
        std::fs::create_dir_all(&staging).expect("staging root");
        for number in 0..REBUILD_DIRENTS_PER_JOB * 3 {
            std::fs::create_dir(staging.join(format!("invalid-{number}"))).expect("invalid directory");
            std::fs::create_dir(staging.join(format!("{number:064x}"))).expect("empty valid root");
        }
        let owner = owner_for_root(root.path());
        let observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = observed.clone();
        let entries = owner
            .directory(Path::new(RUSTFS_META_MULTIPART_BUCKET))
            .expect("pinned staging")
            .entries()
            .expect("streamed entries");
        let mut cursor = RebuildCursor {
            started: true,
            roots: Some(Box::new(entries.inspect(move |_| {
                counter.fetch_add(1, Ordering::Relaxed);
            }))),
            uploads: None,
        };
        assert!(!cursor.step(&owner).expect("first bounded step"));
        assert!(observed.load(Ordering::Relaxed) <= REBUILD_DIRENTS_PER_JOB);
        let mut complete = false;
        for _ in 0..64 {
            let before = observed.load(Ordering::Relaxed);
            complete = cursor.step(&owner).expect("next bounded step");
            assert!(observed.load(Ordering::Relaxed) - before <= REBUILD_DIRENTS_PER_JOB);
            if complete {
                break;
            }
        }
        assert!(complete, "retained cursors eventually reach EOF without rescanning");
        assert!(observed.load(Ordering::Relaxed) >= REBUILD_DIRENTS_PER_JOB * 6);
    }

    #[test]
    fn multipart_index_rebuild_tolerates_removed_roots_and_upload_directories() {
        let root = tempfile::tempdir().expect("disk root");
        let (path, info) = upload("bucket", "removed");
        seed_legacy(root.path(), &path, info);
        let owner = owner_for_root(root.path());
        let (sha_dir, upload_dir) = path.split_once('/').expect("canonical upload");
        let relative = Path::new(RUSTFS_META_MULTIPART_BUCKET).join(sha_dir);
        // Deterministically model a name yielded by readdir just before deletion.
        let mut uploads = RebuildCursor {
            started: true,
            roots: Some(Box::new(std::iter::empty())),
            uploads: Some((relative.clone(), Box::new(std::iter::once(Ok(upload_dir.to_owned()))))),
        };
        std::fs::remove_dir_all(root.path().join(&relative).join(upload_dir)).expect("delete yielded upload");
        assert!(uploads.step(&owner).expect("deleted upload is not corruption"));
        let mut roots = RebuildCursor {
            started: true,
            roots: Some(Box::new(std::iter::once(Ok(sha_dir.to_owned())))),
            uploads: None,
        };
        std::fs::remove_dir(root.path().join(relative)).expect("delete yielded root");
        assert!(roots.step(&owner).expect("deleted root is not corruption"));
    }

    #[tokio::test]
    async fn multipart_index_online_rebuild_covers_concurrent_create_restore_and_delete() {
        let root = tempfile::tempdir().expect("disk root");
        let mut format = crate::disk::format::FormatV3::new(1, 4);
        format.erasure.this = format.erasure.sets[0][0];
        std::fs::create_dir_all(root.path().join(crate::disk::RUSTFS_META_BUCKET)).expect("format directory");
        std::fs::write(
            root.path()
                .join(crate::disk::RUSTFS_META_BUCKET)
                .join(crate::disk::FORMAT_CONFIG_FILE),
            format.to_json().expect("format JSON"),
        )
        .expect("format publication");
        let (deleted, old) = upload("bucket", "deleted-during-scan");
        seed_legacy(root.path(), &deleted, old);
        let (restored, info) = upload("bucket", "restored-during-scan");
        seed_legacy(root.path(), &restored, info);
        let head = root
            .path()
            .join(RUSTFS_META_MULTIPART_BUCKET)
            .join(&restored)
            .join(STORAGE_FORMAT_FILE);
        let restore_bytes = std::fs::read(&head).expect("original source head");
        std::fs::remove_file(&head).expect("simulate recoverable missing head");
        let owner = owner_for_root(root.path());
        let (started, release) = pause_next_head(&owner);
        let mut endpoint = Endpoint::try_from(root.path().to_str().expect("root path")).expect("endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let disk = LocalDisk::new(&endpoint, false).await.expect("online disk");
        tokio::time::timeout(std::time::Duration::from_secs(5), started)
            .await
            .expect("legacy head loaded")
            .expect("head signal");
        let (created, info) = upload("bucket", "created-during-scan");
        disk.write_metadata("bucket", RUSTFS_META_MULTIPART_BUCKET, &created, info)
            .await
            .expect("old coordinator create during scan");
        let store = disk
            .open_multipart_source_store(&restored, None)
            .await
            .expect("native source store");
        store
            .replace_head(&restore_bytes)
            .await
            .expect("native recovery restores head during scan");
        drop(store);
        std::fs::remove_dir_all(root.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&deleted))
            .expect("remove already read source");
        release.send(()).expect("resume scan after concurrent mutations");
        tokio::time::timeout(std::time::Duration::from_secs(5), owner.initialize())
            .await
            .expect("bootstrap joins");
        let page = owner.page("bucket", None, Uuid::new_v4()).await.expect("complete coverage");
        for source in [&created, &restored] {
            assert!(
                page.roots.iter().any(|candidate| source.starts_with(candidate)),
                "live upload cannot be omitted"
            );
            assert!(
                root.path()
                    .join(RUSTFS_META_MULTIPART_BUCKET)
                    .join(source)
                    .join(STORAGE_FORMAT_FILE)
                    .exists()
            );
        }
        assert!(
            !root.path().join(RUSTFS_META_MULTIPART_BUCKET).join(deleted).exists(),
            "deleted source need not survive"
        );
    }
}
