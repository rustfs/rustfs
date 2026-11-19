// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Scanner filesystem jobs retain resource ownership inside the native worker.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs::{FileType, Metadata};
use std::future::{Future, poll_fn};
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use crate::storage_api::ecstore_spawn_blocking_with_resource_ownership as spawn_native;

// Keep Tokio's existing bounded prefetch and one native job per refill.
const DIRECTORY_CHUNK_SIZE: usize = 32;

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod positioned;

/// Identifies a verified prefix of a physical directory buffer. Only the
/// matching directory generation can use its platform-specific kernel cookie.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectoryAnchor {
    directory: [u8; 32],
    cookie: u64,
    entries: u32,
    prefix: [u8; 32],
}

#[derive(Clone)]
pub(super) struct DirEntry {
    source: EntrySource,
    file_type: Option<FileType>,
}

#[derive(Clone)]
enum EntrySource {
    Native(Arc<std::fs::DirEntry>),
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Positioned(Arc<PositionedEntry>),
    Recorded {
        parent: Arc<std::path::Path>,
        name: OsString,
    },
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone)]
struct PositionedEntry {
    parent: Arc<Path>,
    name: OsString,
    anchor: Option<DirectoryAnchor>,
}

impl DirEntry {
    pub(super) fn recorded(parent: Arc<std::path::Path>, name: OsString, file_type: Option<FileType>) -> Self {
        Self {
            source: EntrySource::Recorded { parent, name },
            file_type,
        }
    }

    pub(super) fn anchor(&self) -> Option<&DirectoryAnchor> {
        match &self.source {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            EntrySource::Positioned(entry) => entry.anchor.as_ref(),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn without_anchor(self) -> Self {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let mut entry = self;
            if let EntrySource::Positioned(position) = &mut entry.source {
                Arc::make_mut(position).anchor = None;
            }
            entry
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        self
    }

    pub(super) fn is_recorded(&self) -> bool {
        matches!(&self.source, EntrySource::Recorded { .. })
    }

    pub(super) fn cached_type(&self) -> Option<FileType> {
        self.file_type
    }

    pub(super) fn path(&self) -> PathBuf {
        match &self.source {
            EntrySource::Native(entry) => entry.path(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            EntrySource::Positioned(entry) => entry.parent.join(&entry.name),
            EntrySource::Recorded { parent, name } => parent.join(name),
        }
    }

    pub(super) fn file_name(&self) -> OsString {
        match &self.source {
            EntrySource::Native(entry) => entry.file_name(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            EntrySource::Positioned(entry) => entry.name.clone(),
            EntrySource::Recorded { name, .. } => name.clone(),
        }
    }

    pub(super) async fn file_type(&self) -> io::Result<FileType> {
        if let Some(file_type) = self.file_type {
            return Ok(file_type);
        }
        match &self.source {
            EntrySource::Native(entry) => {
                let entry = entry.clone();
                spawn_native(move || entry.file_type()).await.map_err(io::Error::other)?
            }
            _ => {
                let path = self.path();
                spawn_native(move || std::fs::symlink_metadata(path).map(|meta| meta.file_type()))
                    .await
                    .map_err(io::Error::other)?
            }
        }
    }
}

enum PhysicalReader {
    Standard(std::fs::ReadDir),
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Positioned(Box<positioned::Reader>),
}

impl PhysicalReader {
    fn open(path: &Path, indexed: bool, anchor: Option<&DirectoryAnchor>) -> io::Result<Self> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if indexed {
            return positioned::Reader::open(path, anchor).map(Box::new).map(Self::Positioned);
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let _ = indexed;
        if anchor.is_some() {
            return Err(io::ErrorKind::Unsupported.into());
        }
        std::fs::read_dir(path).map(Self::Standard)
    }

    fn next(&mut self) -> Option<io::Result<DirEntry>> {
        match self {
            Self::Standard(reader) => reader.next().map(|entry| {
                entry.map(|entry| DirEntry {
                    file_type: entry.file_type().ok(),
                    source: EntrySource::Native(Arc::new(entry)),
                })
            }),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::Positioned(reader) => reader.next(),
        }
    }
}

struct DirectoryBuffer {
    reader: PhysicalReader,
    entries: VecDeque<io::Result<DirEntry>>,
    more: bool,
}

impl DirectoryBuffer {
    fn refill(&mut self) {
        for _ in 0..DIRECTORY_CHUNK_SIZE {
            let Some(entry) = self.reader.next() else {
                self.more = false;
                break;
            };
            let success = entry.is_ok();
            self.entries.push_back(entry);
            if !success {
                break;
            }
        }
    }
}

enum DirectoryState {
    Idle(Option<DirectoryBuffer>),
    Pending(tokio::task::JoinHandle<DirectoryBuffer>),
    Failed,
}

pub(super) struct ReadDir {
    state: DirectoryState,
    #[cfg(test)]
    slow_directory: bool,
}

impl ReadDir {
    pub(super) async fn open(path: &str, indexed: bool) -> io::Result<Self> {
        Self::open_directory(path, indexed, None).await
    }

    pub(super) async fn open_at(path: &str, anchor: &DirectoryAnchor) -> io::Result<Self> {
        Self::open_directory(path, true, Some(anchor.clone())).await
    }

    async fn open_directory(path: &str, indexed: bool, anchor: Option<DirectoryAnchor>) -> io::Result<Self> {
        #[cfg(test)]
        let slow_directory = super::tests::fair_prefix_quantum::is_slow_directory(path);
        let path = PathBuf::from(path);
        let buffer = spawn_native(move || {
            let mut buffer = DirectoryBuffer {
                reader: PhysicalReader::open(&path, indexed, anchor.as_ref())?,
                entries: VecDeque::with_capacity(DIRECTORY_CHUNK_SIZE),
                more: true,
            };
            buffer.refill();
            Ok::<_, io::Error>(buffer)
        })
        .await
        .map_err(io::Error::other)??;
        Ok(Self {
            state: DirectoryState::Idle(Some(buffer)),
            #[cfg(test)]
            slow_directory,
        })
    }

    pub(super) async fn next_entry(&mut self) -> io::Result<Option<DirEntry>> {
        // Keep a pending refill on the reader, not in the borrowed next-entry
        // future. Cancelling that future must not skip its prefetched entries.
        #[cfg(test)]
        if self.slow_directory {
            super::tests::fair_prefix_quantum::charge_physical_read().await;
        }
        poll_fn(|cx| self.poll_next_entry(cx)).await
    }

    fn poll_next_entry(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<DirEntry>>> {
        loop {
            match &mut self.state {
                DirectoryState::Idle(buffer) => {
                    let Some(ready) = buffer.as_mut() else {
                        return Poll::Ready(Err(io::Error::other("scanner directory buffer is unavailable")));
                    };
                    if let Some(entry) = ready.entries.pop_front() {
                        return Poll::Ready(entry.map(Some));
                    }
                    if !ready.more {
                        return Poll::Ready(Ok(None));
                    }
                    let Some(mut buffer) = buffer.take() else {
                        return Poll::Ready(Err(io::Error::other("scanner directory buffer is unavailable")));
                    };
                    self.state = DirectoryState::Pending(spawn_native(move || {
                        buffer.refill();
                        buffer
                    }));
                }
                DirectoryState::Pending(worker) => match ready!(Pin::new(worker).poll(cx)) {
                    Ok(buffer) => self.state = DirectoryState::Idle(Some(buffer)),
                    Err(error) => {
                        self.state = DirectoryState::Failed;
                        return Poll::Ready(Err(io::Error::other(error)));
                    }
                },
                DirectoryState::Failed => {
                    return Poll::Ready(Err(io::Error::other("scanner directory worker failed")));
                }
            }
        }
    }
}

pub(super) async fn metadata(path: &str) -> io::Result<Metadata> {
    let path = PathBuf::from(path);
    spawn_native(move || std::fs::metadata(path))
        .await
        .map_err(io::Error::other)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_concurrency::adaptive::{
        AdaptiveAdmission, AdaptivePermit,
        permit_set::{AdaptivePermitSet, AdaptivePermitSetOwner},
    };
    use std::num::NonZeroU16;

    enum Resources {
        Single(AdaptivePermit),
        Layered(AdaptivePermitSet),
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .expect("single native worker runtime")
    }

    fn directory(count: usize) -> tempfile::TempDir {
        let root = tempfile::tempdir().expect("owned directory");
        for index in 0..count {
            std::fs::write(root.path().join(format!("object-{index}")), b"data").expect("test file");
        }
        root
    }

    async fn occupy_native_worker() -> (std::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>) {
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let worker = tokio::task::spawn_blocking(move || {
            entered.send(()).expect("worker-start observer");
            let _ = resume.recv();
        });
        started.await.expect("native worker occupied");
        (release, worker)
    }

    #[test]
    fn scanner_native_jobs_keep_single_and_layered_units_after_waiter_cancellation() {
        runtime().block_on(async {
            let root = directory(DIRECTORY_CHUNK_SIZE + 3);
            let path = root.path().to_str().expect("test path");
            for count in [1, 2] {
                for operation in ["open", "refill", "metadata", "file_type"] {
                    let gates: Vec<_> = (0..count).map(|_| AdaptiveAdmission::new(NonZeroU16::MIN, 1)).collect();
                    let mut units: Vec<_> = gates.iter().map(|gate| gate.try_acquire().expect("physical unit")).collect();
                    let owner = AdaptivePermitSetOwner::new(NonZeroU16::MIN, gates.clone()).expect("physical subset");
                    let resources = if count == 1 {
                        Resources::Single(units.pop().expect("single physical unit"))
                    } else {
                        Resources::Layered(owner.capture(&units.iter().collect::<Vec<_>>()).expect("native ownership"))
                    };
                    drop(units);
                    let mut reader = ReadDir::open(path, false).await.expect("prepare reader");
                    let mut entry = reader.next_entry().await.expect("first entry").expect("file");
                    entry.file_type = None;
                    for _ in 1..DIRECTORY_CHUNK_SIZE {
                        reader.next_entry().await.expect("consume prefetched entry").expect("file");
                    }
                    let (release, blocker) = occupy_native_worker().await;
                    let work = async {
                        match operation {
                            "open" => ReadDir::open(path, false).await.map(|_| ()),
                            "refill" => reader.next_entry().await.map(|_| ()),
                            "metadata" => metadata(entry.path().to_str().expect("entry path")).await.map(|_| ()),
                            "file_type" => entry.file_type().await.map(|_| ()),
                            _ => unreachable!("fixed test operations"),
                        }
                    };
                    let mut waiter = Box::pin(async {
                        match resources {
                            Resources::Single(unit) => {
                                crate::storage_api::ecstore_with_background_resource_unit(unit, work).await
                            }
                            Resources::Layered(units) => {
                                crate::storage_api::ecstore_with_background_resource_set(units, work).await
                            }
                        }
                    });
                    assert!(futures::poll!(&mut waiter).is_pending(), "native job must be queued: {operation}");
                    drop(waiter);
                    drop(reader);
                    let held_after_cancel = gates.iter().all(|gate| gate.snapshot().active == 1);
                    gates[0].set_limit(0);
                    release.send(()).expect("release native worker");
                    blocker.await.expect("blocker finished");
                    // The sentinel runs after the queued filesystem job on the
                    // sole blocking worker, even after its waiter was dropped.
                    tokio::task::spawn_blocking(|| ()).await.expect("native queue drained");
                    assert!(
                        held_after_cancel,
                        "cancelled {operation} refunded {count} physical units before native completion"
                    );
                    assert!(gates.iter().all(|gate| gate.snapshot().active == 0), "native {operation} leaked capacity");
                    assert!(gates[0].try_acquire().is_none(), "native completion cannot undo a capacity reduction");
                }
            }
        });
    }

    #[test]
    fn cancelled_refill_resumes_without_skipping_or_duplicating_entries() {
        runtime().block_on(async {
            let count = DIRECTORY_CHUNK_SIZE * 3 + 7;
            let root = directory(count);
            let mut reader = ReadDir::open(root.path().to_str().expect("root path"), false)
                .await
                .expect("open");
            let mut names = std::collections::BTreeSet::new();
            for _ in 0..DIRECTORY_CHUNK_SIZE {
                let entry = reader.next_entry().await.expect("entry").expect("file");
                assert!(names.insert(entry.file_name()));
            }
            let (release, blocker) = occupy_native_worker().await;
            let mut next = Box::pin(reader.next_entry());
            assert!(futures::poll!(&mut next).is_pending());
            drop(next);
            release.send(()).expect("release refill");
            blocker.await.expect("blocker ended");
            while let Some(entry) = reader.next_entry().await.expect("resume refill") {
                assert!(names.insert(entry.file_name()), "duplicate entry after cancelled refill");
            }
            assert_eq!(names.len(), count, "cancelling the borrowed future must retain the pending chunk");
            assert!(reader.next_entry().await.expect("repeat EOF").is_none());
        });
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn reopened_directory_matches_original_suffix_and_rejects_stale_or_corrupt_positions() {
        runtime().block_on(async {
            #[cfg(target_os = "linux")]
            use std::os::unix::ffi::OsStrExt;
            let root = directory(900);
            let path = root.path().to_str().expect("root path");
            #[cfg(target_os = "linux")]
            let raw_name = std::ffi::OsStr::from_bytes(b"object-\xff");
            // APFS requires filenames that are valid Unicode.
            #[cfg(target_os = "macos")]
            let raw_name = std::ffi::OsStr::new("object-目录");
            std::fs::write(root.path().join(raw_name), []).expect("platform-native directory entry");
            let mut reader = ReadDir::open(path, true).await.expect("open indexed directory");
            let mut names = Vec::new();
            let mut anchors = Vec::new();
            while let Some(entry) = reader.next_entry().await.expect("read indexed entry") {
                assert!(!entry.is_recorded());
                names.push(entry.file_name());
                anchors.push(entry.anchor().expect("native directory position").clone());
            }
            drop(reader);
            assert!(names.iter().any(|name| name == raw_name));
            assert!(anchors.iter().any(|anchor| anchor.cookie != 0), "exercise a later kernel buffer");
            for i in [0, 32, 145, 419, names.len() - 1] {
                let mut resumed = ReadDir::open_at(path, &anchors[i])
                    .await
                    .expect("reopen directory at verified entry");
                for expected in &names[i + 1..] {
                    assert_eq!(
                        &resumed
                            .next_entry()
                            .await
                            .expect("suffix read")
                            .expect("suffix entry")
                            .file_name(),
                        expected
                    );
                }
                assert!(resumed.next_entry().await.expect("resumed EOF").is_none());
            }

            let mut corrupt = anchors[0].clone();
            corrupt.prefix[0] ^= 1;
            assert!(matches!(ReadDir::open_at(path, &corrupt).await, Err(err) if err.kind() == io::ErrorKind::InvalidData));
            corrupt.entries = u32::MAX;
            assert!(matches!(ReadDir::open_at(path, &corrupt).await, Err(err) if err.kind() == io::ErrorKind::InvalidData));
            let other = directory(1);
            let another = other.path().to_str().expect("other directory");
            assert!(matches!(ReadDir::open_at(another, &anchors[0]).await, Err(err) if err.kind() == io::ErrorKind::InvalidData));
            // Removing the bound entry also invalidates its directory generation.
            std::fs::remove_file(root.path().join(&names[0])).expect("remove anchor entry");
            assert!(matches!(ReadDir::open_at(path, &anchors[0]).await, Err(err) if err.kind() == io::ErrorKind::InvalidData));
        });
    }

    #[test]
    fn directory_prefetch_preserves_empty_and_exact_chunk_boundaries() {
        runtime().block_on(async {
            for count in [0, 1, DIRECTORY_CHUNK_SIZE, DIRECTORY_CHUNK_SIZE + 1] {
                let root = directory(count);
                let mut reader = ReadDir::open(root.path().to_str().expect("root path"), false)
                    .await
                    .expect("open");
                let mut names = std::collections::BTreeSet::new();
                while let Some(entry) = reader.next_entry().await.expect("read entry") {
                    assert!(names.insert(entry.file_name()), "prefetch duplicated an entry");
                }
                assert_eq!(names.len(), count);
                assert!(reader.next_entry().await.expect("repeat EOF").is_none());
            }
        });
    }

    #[test]
    fn failed_refill_is_an_error_after_partial_enumeration() {
        runtime().block_on(async {
            let root = directory(DIRECTORY_CHUNK_SIZE + 1);
            let mut reader = ReadDir::open(root.path().to_str().expect("root path"), false)
                .await
                .expect("open");
            for _ in 0..DIRECTORY_CHUNK_SIZE {
                assert!(reader.next_entry().await.expect("prefetched entry").is_some());
            }
            let (release, blocker) = occupy_native_worker().await;
            let mut next = Box::pin(reader.next_entry());
            assert!(futures::poll!(&mut next).is_pending());
            drop(next);
            let DirectoryState::Pending(worker) = &reader.state else {
                panic!("refill must be queued");
            };
            worker.abort();
            release.send(()).expect("release native worker");
            blocker.await.expect("blocker finished");
            for _ in 0..2 {
                assert!(
                    matches!(reader.next_entry().await, Err(error) if error.kind() == io::ErrorKind::Other),
                    "a failed refill cannot become successful EOF"
                );
            }
        });
    }

    #[test]
    fn scanner_native_errors_and_symlink_metadata_keep_filesystem_semantics() {
        runtime().block_on(async {
            let root = directory(1);
            let path = root.path().join("missing");
            let missing = path.to_str().expect("missing path");
            assert!(matches!(ReadDir::open(missing, false).await, Err(error) if error.kind() == io::ErrorKind::NotFound));
            assert!(matches!(metadata(missing).await, Err(error) if error.kind() == io::ErrorKind::NotFound));
            let file = root.path().join("object-0");
            assert!(ReadDir::open(file.to_str().expect("file path"), false).await.is_err());
            #[cfg(unix)]
            {
                let link = root.path().join("link");
                std::os::unix::fs::symlink(&file, &link).expect("file symlink");
                let info = metadata(link.to_str().expect("link path")).await.expect("follow symlink");
                assert!(info.is_file());
                assert_eq!(info.len(), 4);
            }
        });
    }
}
