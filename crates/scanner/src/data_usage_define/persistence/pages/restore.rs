// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::any::Any;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::sync::{Mutex, Weak};
use std::time::Instant as WallInstant;

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
                job.owner.strong_count() != 0
                    && job.last_polled.elapsed() < Duration::from_secs(3600)
                    && (!job.owner.ptr_eq(&owner) || job.name != name || job.manifest == *manifest)
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
                decoder: None,
            },
        })))
    }
}

// Park only verified bytes and traversal state. Outstanding metadata reads
// and their admissions are dropped with the waiter. A CPU-only decode may
// finish after cancellation and is joined by the next reader of this root.
impl Drop for Recovery {
    fn drop(&mut self) {
        let Some(mut job) = self.0.take() else { return };
        if job.position.nodes == 0 && job.position.leaves == 0 {
            return;
        }
        let mut parked = PARKED.lock().unwrap_or_else(|error| error.into_inner());
        if parked.len() < PARKED_LIMIT && !parked.iter().any(|old| old.owner.ptr_eq(&job.owner) && old.name == job.name) {
            job.last_polled = WallInstant::now();
            parked.push(job);
        }
    }
}

impl Position {
    async fn restore<S: ScannerObjectIO>(
        &mut self,
        store: Arc<S>,
        name: &str,
        manifest: &Manifest,
    ) -> StorageResult<DataUsageCache> {
        let max_leaves = manifest.length.div_ceil(PAGE_MIN as u64).saturating_add(3);
        while let Some(&(key, level)) = self.pending.last() {
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
            let batch: Vec<_> = self
                .pending
                .iter()
                .rev()
                .take_while(|(_, level)| *level == 0)
                .take(IO_CONCURRENCY)
                .map(|(key, _)| *key)
                .collect();
            let mut reads = Vec::with_capacity(batch.len());
            for &key in &batch {
                reads.push(read_block(store.clone(), name, manifest.bundle, key));
            }
            let blocks = try_join_all(reads).await?;
            let chunk_bytes = blocks.iter().map(|block| block.len()).sum::<usize>();
            let next = self
                .length
                .checked_add(u64::try_from(chunk_bytes).map_err(|_| invalid("invalid restore length"))?)
                .filter(|len| *len <= manifest.length)
                .ok_or_else(|| invalid("scanner checkpoint exceeds its committed length"))?;
            let leaves = self.leaves + u64::try_from(batch.len()).map_err(|_| invalid("too many checkpoint pages"))?;
            if leaves > max_leaves {
                return Err(invalid("too many scanner checkpoint leaves"));
            }
            match &mut self.buffer {
                Buffer::Memory(bytes) => {
                    for block in &blocks {
                        bytes.extend_from_slice(block);
                    }
                }
                Buffer::File(file) => {
                    let file = file.clone();
                    let offset = self.length;
                    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                        let mut file = file.lock().unwrap_or_else(|error| error.into_inner());
                        file.seek(SeekFrom::Start(offset))?;
                        for block in blocks {
                            file.write_all(&block)?;
                        }
                        Ok(())
                    })
                    .await
                    .map_err(Error::other)??;
                }
            }
            self.pending.truncate(self.pending.len() - batch.len());
            self.length = next;
            self.leaves = leaves;
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
