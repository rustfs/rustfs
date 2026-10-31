// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Scanner filesystem jobs retain resource ownership inside the native worker.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs::{FileType, Metadata};
use std::future::{Future, poll_fn};
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use crate::storage_api::ecstore_spawn_blocking_with_resource_ownership as spawn_native;

// Keep Tokio's existing bounded prefetch and one native job per refill.
const DIRECTORY_CHUNK_SIZE: usize = 32;

pub(super) struct DirEntry {
    entry: Arc<std::fs::DirEntry>,
    file_type: Option<FileType>,
}

impl DirEntry {
    pub(super) fn path(&self) -> PathBuf {
        self.entry.path()
    }

    pub(super) fn file_name(&self) -> OsString {
        self.entry.file_name()
    }

    pub(super) async fn file_type(&self) -> io::Result<FileType> {
        if let Some(file_type) = self.file_type {
            return Ok(file_type);
        }
        let entry = self.entry.clone();
        spawn_native(move || entry.file_type()).await.map_err(io::Error::other)?
    }
}

struct DirectoryBuffer {
    reader: std::fs::ReadDir,
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
            self.entries.push_back(entry.map(|entry| DirEntry {
                file_type: entry.file_type().ok(),
                entry: Arc::new(entry),
            }));
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
}

impl ReadDir {
    pub(super) async fn open(path: &str) -> io::Result<Self> {
        let path = PathBuf::from(path);
        let buffer = spawn_native(move || {
            let mut buffer = DirectoryBuffer {
                reader: std::fs::read_dir(path)?,
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
        })
    }

    pub(super) async fn next_entry(&mut self) -> io::Result<Option<DirEntry>> {
        // Keep a pending refill on the reader, not in the borrowed next-entry
        // future. Cancelling that future must not skip its prefetched entries.
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
                    let mut reader = ReadDir::open(path).await.expect("prepare reader");
                    let mut entry = reader.next_entry().await.expect("first entry").expect("file");
                    entry.file_type = None;
                    for _ in 1..DIRECTORY_CHUNK_SIZE {
                        reader.next_entry().await.expect("consume prefetched entry").expect("file");
                    }
                    let (release, blocker) = occupy_native_worker().await;
                    let work = async {
                        match operation {
                            "open" => ReadDir::open(path).await.map(|_| ()),
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
            let mut reader = ReadDir::open(root.path().to_str().expect("root path")).await.expect("open");
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

    #[test]
    fn directory_prefetch_preserves_empty_and_exact_chunk_boundaries() {
        runtime().block_on(async {
            for count in [0, 1, DIRECTORY_CHUNK_SIZE, DIRECTORY_CHUNK_SIZE + 1] {
                let root = directory(count);
                let mut reader = ReadDir::open(root.path().to_str().expect("root path")).await.expect("open");
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
            let mut reader = ReadDir::open(root.path().to_str().expect("root path")).await.expect("open");
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
            assert!(matches!(ReadDir::open(missing).await, Err(error) if error.kind() == io::ErrorKind::NotFound));
            assert!(matches!(metadata(missing).await, Err(error) if error.kind() == io::ErrorKind::NotFound));
            let file = root.path().join("object-0");
            assert!(ReadDir::open(file.to_str().expect("file path")).await.is_err());
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
