// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use crate::storage_api::ecstore_spawn_blocking_with_resource_ownership as spawn_native;
use sha2::{Digest, Sha256};

const PAGE_NAMES: usize = 256;
const PAGE_NAME_BYTES: usize = 32 * 1024;
const MERGE_WINDOW: usize = 8;

struct Chunk {
    offset: u64,
    bytes: usize,
    digest: [u8; 32],
}

struct Page {
    count: usize,
    first: String,
    chunks: Vec<Chunk>,
    window: Option<(usize, Vec<String>)>,
}

/// Runs are sorted and sealed as the native directory is read. Their bounded
/// merge keeps one small lookahead window per run. Interleaved runs still
/// read each scratch chunk at most once between rewinds.
#[derive(Default)]
pub(super) struct NamePages {
    file: Option<Arc<Mutex<File>>>,
    pages: Vec<Page>,
    building: Vec<String>,
    name_bytes: usize,
    count: usize,
    heads: BinaryHeap<Reverse<(String, usize, usize)>>,
    file_end: u64,
    ready: bool,
    pending: Option<String>,
    through: Option<String>,
}

impl NamePages {
    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn ready(&self) -> bool {
        self.ready
    }

    pub(super) fn add(&mut self, name: String) {
        self.name_bytes += name.len();
        self.count += 1;
        self.building.push(name);
    }

    pub(super) async fn flush_full(&mut self) -> io::Result<()> {
        if self.building.len() < PAGE_NAMES && self.name_bytes < PAGE_NAME_BYTES {
            return Ok(());
        }
        self.building.sort_unstable();
        let Some(first) = self.building.first().cloned() else { return Ok(()) };
        let mut bytes = Vec::new();
        let mut chunks = Vec::new();
        let mut end = self.file_end;
        for names in self.building.chunks(MERGE_WINDOW) {
            let start = bytes.len();
            rmp_serde::encode::write(&mut bytes, &names).map_err(io::Error::other)?;
            let data = &bytes[start..];
            chunks.push(Chunk {
                offset: end,
                bytes: data.len(),
                digest: Sha256::digest(data).into(),
            });
            end = end
                .checked_add(u64::try_from(data.len()).map_err(io::Error::other)?)
                .ok_or_else(|| io::Error::other("scanner name pages exceed scratch file limits"))?;
        }
        let file = match &self.file {
            Some(file) => file.clone(),
            None => {
                let file = spawn_native(tempfile::tempfile).await.map_err(io::Error::other)??;
                self.file.insert(Arc::new(Mutex::new(file))).clone()
            }
        };
        let offset = self.file_end;
        // A native write can outlive cancellation. The next attempt writes
        // the same sorted bytes at the same offset before extending the file.
        spawn_native(move || {
            let mut file = file.lock().unwrap_or_else(|p| p.into_inner());
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&bytes)
        })
        .await
        .map_err(io::Error::other)??;
        self.heads.push(Reverse((first.clone(), self.pages.len(), 0)));
        self.pages.push(Page {
            count: self.building.len(),
            first,
            chunks,
            window: None,
        });
        self.file_end = end;
        self.building.clear();
        self.name_bytes = 0;
        Ok(())
    }

    pub(super) fn finish(&mut self) {
        if self.ready {
            return;
        }
        self.building.sort_unstable();
        if let Some(name) = self.building.first() {
            self.heads.push(Reverse((name.clone(), self.pages.len(), 0)));
        }
        self.ready = true;
    }

    pub(super) fn rewind(&mut self) {
        self.pending = None;
        self.through = None;
        self.heads.clear();
        self.heads.extend(
            self.pages
                .iter()
                .enumerate()
                .map(|(index, page)| Reverse((page.first.clone(), index, 0))),
        );
        if let Some(name) = self.building.first() {
            self.heads.push(Reverse((name.clone(), self.pages.len(), 0)));
        }
    }

    async fn successor(&mut self, page: usize, position: usize) -> io::Result<Option<String>> {
        let Some(info) = self.pages.get_mut(page) else { return Ok(self.building.get(position).cloned()) };
        if position >= info.count {
            return Ok(None);
        }
        let chunk_index = position / MERGE_WINDOW;
        if info.window.as_ref().is_none_or(|(index, _)| *index != chunk_index) {
            let file = self
                .file
                .clone()
                .ok_or_else(|| io::Error::other("scanner name page file missing"))?;
            let chunk = &info.chunks[chunk_index];
            let (offset, len, digest) = (chunk.offset, chunk.bytes, chunk.digest);
            let names = spawn_native(move || {
                let mut bytes = vec![0; len];
                {
                    let mut file = file.lock().unwrap_or_else(|p| p.into_inner());
                    file.seek(SeekFrom::Start(offset))?;
                    file.read_exact(&mut bytes)?;
                }
                if <[u8; 32]>::from(Sha256::digest(&bytes)) != digest {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "scanner name page checksum differs"));
                }
                rmp_serde::from_slice::<Vec<String>>(&bytes).map_err(io::Error::other)
            })
            .await
            .map_err(io::Error::other)??;
            info.window = Some((chunk_index, names));
        }
        info.window
            .as_ref()
            .and_then(|(_, names)| names.get(position % MERGE_WINDOW))
            .cloned()
            .map(Some)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "scanner name page truncated"))
    }

    pub(super) async fn next(&mut self) -> io::Result<Option<String>> {
        if self.pending.is_some() {
            return Ok(self.pending.clone());
        }
        while let Some(Reverse((name, page, index))) = self.heads.peek().cloned() {
            let next = self.successor(page, index + 1).await?;
            self.heads.pop();
            if let Some(next) = next {
                self.heads.push(Reverse((next, page, index + 1)));
            }
            if self.through.as_ref() == Some(&name) {
                continue;
            }
            self.pending = Some(name.clone());
            return Ok(Some(name));
        }
        Ok(None)
    }

    pub(super) fn acknowledge(&mut self) -> bool {
        if let Some(name) = self.pending.take() {
            self.through = Some(name);
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn interleaved_pages_resume_at_each_unacknowledged_name() {
        let mut pages = NamePages::default();
        let mut expected = std::collections::BTreeSet::new();
        // Each consecutive sorted run interleaves with every other run. One
        // duplicate straddles two runs and a partial run remains in memory.
        for run in 0..5 {
            for index in (0..PAGE_NAMES).rev() {
                let name = format!("item-{index:04}-{run:02}");
                expected.insert(name.clone());
                pages.add(name);
                pages.flush_full().await.expect("append sorted run");
            }
        }
        pages.add("item-0000-02".to_owned());
        pages.finish();
        for name in &expected {
            assert_eq!(pages.next().await.expect("scheduled child").as_ref(), Some(name));
            assert_eq!(pages.next().await.expect("retry after cancellation").as_ref(), Some(name));
            assert!(pages.acknowledge());
        }
        assert_eq!(pages.next().await.expect("sealed EOF"), None);
        pages.rewind();
        assert_eq!(pages.next().await.expect("fresh coverage").as_ref(), expected.first());
    }
}
