// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::super::scan_load::park_restore;
use super::*;
use std::any::Any;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::sync::{Mutex, Weak};
use tokio::time::Instant as WallInstant;

const MEMORY_LIMIT: u64 = 1024 * 1024;
const PARKED_LIMIT: usize = 32;

enum Buffer {
    Memory(Vec<u8>),
    File(Arc<Mutex<File>>),
}

struct Position {
    pending: Vec<(Digest, u8)>,
    length: u64,
    leaves: u64,
    nodes: u64,
    buffer: Buffer,
    writer: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    decoder: Option<tokio::task::JoinHandle<StorageResult<DataUsageCache>>>,
}

struct Suspended {
    owner: Weak<dyn Any + Send + Sync>,
    name: String,
    manifest: Manifest,
    last_polled: WallInstant,
    position: Position,
}

struct Recovery(Option<Suspended>);

static PARKED: LazyLock<Mutex<Vec<Suspended>>> = LazyLock::new(|| Mutex::new(Vec::new()));

impl Recovery {
    async fn open<S: ScannerObjectIO>(store: &Arc<S>, name: &str, manifest: &Manifest) -> StorageResult<Self> {
        let erased: Arc<dyn Any + Send + Sync> = store.clone();
        let owner = Arc::downgrade(&erased);
        let resumed = {
            let mut parked = PARKED.lock().unwrap_or_else(|error| error.into_inner());
            parked.retain(|job| {
                job.owner.strong_count() != 0 && (!job.owner.ptr_eq(&owner) || job.name != name || job.manifest == *manifest)
            });
            parked
                .iter()
                .position(|job| job.owner.ptr_eq(&owner) && job.name == name && job.manifest == *manifest)
                .map(|index| parked.swap_remove(index))
        };
        if let Some(mut job) = resumed {
            job.last_polled = WallInstant::now();
            return Ok(Self(Some(job)));
        }
        let buffer = if manifest.length <= MEMORY_LIMIT {
            Buffer::Memory(Vec::new())
        } else {
            let file = tokio::task::spawn_blocking(tempfile::tempfile)
                .await
                .map_err(Error::other)??;
            Buffer::File(Arc::new(Mutex::new(file)))
        };
        Ok(Self(Some(Suspended {
            owner,
            name: name.to_owned(),
            manifest: manifest.clone(),
            last_polled: WallInstant::now(),
            position: Position {
                pending: vec![(manifest.root, manifest.level)],
                length: 0,
                leaves: 0,
                nodes: 0,
                buffer,
                writer: None,
                decoder: None,
            },
        })))
    }
}

// Park only verified bytes and traversal state. Outstanding metadata reads
// and their admissions are dropped with the waiter. A local spool write or
// decode may finish after cancellation and is joined before advancing again.
impl Drop for Recovery {
    fn drop(&mut self) {
        let Some(mut job) = self.0.take() else { return };
        if job.position.nodes == 0 && job.position.leaves == 0 {
            return;
        }
        let mut parked = PARKED.lock().unwrap_or_else(|error| error.into_inner());
        job.last_polled = WallInstant::now();
        if let Some(previous) = parked
            .iter_mut()
            .find(|previous| previous.owner.ptr_eq(&job.owner) && previous.name == job.name && previous.manifest == job.manifest)
        {
            // Readers of an immutable root traverse the same ordered prefix.
            // Keep the furthest admission regardless of cancellation order.
            if job.position.verified_progress() > previous.position.verified_progress() {
                std::mem::swap(previous, &mut job);
            }
        } else {
            let displaced = park_restore(&mut parked, PARKED_LIMIT, job, |job| job.last_polled);
            drop(parked);
            drop(displaced);
        }
    }
}

impl Position {
    fn verified_progress(&self) -> (u64, bool) {
        (self.nodes + self.leaves, self.decoder.is_some())
    }

    async fn restore<S: ScannerObjectIO>(
        &mut self,
        store: Arc<S>,
        name: &str,
        manifest: &Manifest,
    ) -> StorageResult<DataUsageCache> {
        let max_leaves = manifest.length.div_ceil(PAGE_MIN as u64).saturating_add(3);
        loop {
            // Retain the local write across cancellation. The verified page already
            // owns its offset and must not consume another storage read next round.
            if let Some(writer) = &mut self.writer {
                writer.await.map_err(Error::other)??;
                self.writer = None;
            }
            let Some(&(key, level)) = self.pending.last() else { break };
            if level != 0 {
                let node = read_block(store.clone(), name, manifest.bundle, key).await?;
                if node.is_empty()
                    || node.len() > FANOUT * 32
                    || node.len() % 32 != 0
                    || self.nodes >= max_leaves.saturating_mul(u64::from(manifest.level))
                {
                    return Err(invalid("invalid scanner checkpoint restore table"));
                }
                self.pending.pop();
                for &child in node.as_chunks::<32>().0.iter().rev() {
                    self.pending.push((child, level - 1));
                }
                self.nodes += 1;
                continue;
            }
            let block = read_block(store.clone(), name, manifest.bundle, key).await?;
            let next = self
                .length
                .checked_add(u64::try_from(block.len()).map_err(|_| invalid("invalid restore length"))?)
                .filter(|len| *len <= manifest.length)
                .ok_or_else(|| invalid("scanner checkpoint exceeds its committed length"))?;
            if self.leaves >= max_leaves {
                return Err(invalid("too many scanner checkpoint leaves"));
            }
            match &mut self.buffer {
                Buffer::Memory(bytes) => bytes.extend_from_slice(&block),
                Buffer::File(file) => {
                    let file = file.clone();
                    let offset = self.length;
                    self.writer = Some(tokio::task::spawn_blocking(move || {
                        let mut file = file.lock().unwrap_or_else(|error| error.into_inner());
                        file.seek(SeekFrom::Start(offset))?;
                        file.write_all(&block)
                    }));
                }
            }
            self.pending.pop();
            self.length = next;
            self.leaves += 1;
        }
        if self.length != manifest.length {
            return Err(invalid("incomplete scanner checkpoint"));
        }
        let job = self.decoder.get_or_insert_with(|| {
            let buffer = std::mem::replace(&mut self.buffer, Buffer::Memory(Vec::new()));
            tokio::task::spawn_blocking(move || {
                let result = match buffer {
                    Buffer::Memory(bytes) => rmp_serde::from_slice(&bytes),
                    Buffer::File(file) => {
                        let mut file = file.lock().unwrap_or_else(|error| error.into_inner());
                        file.seek(SeekFrom::Start(0))?;
                        rmp_serde::from_read(std::io::BufReader::new(&mut *file))
                    }
                };
                result.map_err(|_| invalid("invalid scanner checkpoint payload"))
            })
        });
        job.await.map_err(Error::other)?
    }
}

pub(super) async fn restore<S: ScannerObjectIO>(store: Arc<S>, name: &str, manifest: &Manifest) -> StorageResult<DataUsageCache> {
    let mut job = Recovery::open(&store, name, manifest).await?;
    let result = job
        .0
        .as_mut()
        .ok_or_else(|| invalid("missing checkpoint restore state"))?
        .position
        .restore(store, name, manifest)
        .await;
    job.0.take();
    result
}
