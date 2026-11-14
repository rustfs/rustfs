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

use crate::disk::{DiskAPI, DiskStore};
use crate::erasure::codec::shard_buf::ShardBytes;
use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use rustfs_filemeta::FileInfo;
use rustfs_filemeta::shard_integrity::{
    IntegrityLayout, MAX_INLINE_PROOF_BYTES, PartIntegrity, SUFFIX_INLINE_INTEGRITY, node_digest,
};
use rustfs_utils::http::{contains_key_str, get_consistent_str, insert_str};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

const MEMORY_LIMIT: usize = 1024 * 1024;
const COPY_CHUNK_SIZE: usize = 64 * 1024;

fn corrupt() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "shard integrity verification failed")
}

/// Only encoded digest rows are retained. Payload bytes never enter the spool.
/// Large parts spill to an anonymous temporary file; cancellation drops it.
pub(crate) struct IntegrityBuilder {
    part: PartIntegrity,
    rows: Vec<u8>,
    spill: Option<tokio::fs::File>,
    prepared: Option<Box<PreparedIntegrity>>,
}

impl IntegrityBuilder {
    pub(crate) fn new(layout: IntegrityLayout, number: usize) -> io::Result<Self> {
        Ok(Self {
            part: PartIntegrity::new(layout, number).map_err(|_| corrupt())?,
            rows: Vec::new(),
            spill: None,
            prepared: None,
        })
    }

    pub(crate) fn part(&self) -> &PartIntegrity {
        &self.part
    }

    pub(crate) async fn push<'a>(&mut self, shards: impl ExactSizeIterator<Item = &'a [u8]>) -> io::Result<()> {
        self.push_with_precomputed(shards, &[]).await
    }

    pub(crate) async fn push_with_precomputed<'a>(
        &mut self,
        shards: impl ExactSizeIterator<Item = &'a [u8]>,
        precomputed: &[Option<[u8; 32]>],
    ) -> io::Result<()> {
        if shards.len() != self.part.layout.shards() {
            return Err(corrupt());
        }
        let mut row = Vec::with_capacity(shards.len() * 32);
        for (index, payload) in shards.enumerate() {
            let digest = match precomputed.get(index).copied().flatten() {
                Some(digest) => digest,
                None => self
                    .part
                    .shard_digest(self.part.stripes, index, payload)
                    .map_err(|_| corrupt())?,
            };
            row.extend_from_slice(&digest);
        }
        self.append(&row).await?;
        self.part.stripes = self.part.stripes.checked_add(1).ok_or_else(corrupt)?;
        Ok(())
    }

    async fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.rows.extend_from_slice(bytes);
        if self.rows.len() >= MEMORY_LIMIT {
            if self.spill.is_none() {
                let file = tokio::task::spawn_blocking(tempfile::tempfile)
                    .await
                    .map_err(io::Error::other)??;
                self.spill = Some(tokio::fs::File::from_std(file));
            }
            if let Some(file) = &mut self.spill {
                file.write_all(&self.rows).await?;
                self.rows.clear();
            }
        }
        Ok(())
    }

    async fn take_spool(&mut self) -> io::Result<Spool> {
        let rows = std::mem::take(&mut self.rows);
        if let Some(mut file) = self.spill.take() {
            file.write_all(&rows).await?;
            file.flush().await?;
            Ok(Spool::File(file.into_std().await))
        } else {
            Ok(Spool::Memory(rows))
        }
    }

    pub(crate) async fn prepare(&mut self, size: usize) -> io::Result<&mut PreparedIntegrity> {
        if self.prepared.is_none() {
            self.part.size = size.try_into().map_err(|_| corrupt())?;
            self.part.validate().map_err(|_| corrupt())?;
            let part = self.part.clone();
            let rows = self.take_spool().await?;
            let prepared = if part.stripes < 16 {
                build_index(part, rows)?
            } else {
                tokio::task::spawn_blocking(move || build_index(part, rows))
                    .await
                    .map_err(io::Error::other)??
            };
            self.prepared = Some(Box::new(prepared));
        }
        self.prepared.as_deref_mut().ok_or_else(corrupt)
    }

    pub(crate) fn set_prewritten_disks(&mut self, prewritten_disks: Vec<bool>) {
        if let Some(prepared) = self.prepared.as_deref_mut() {
            prepared.set_prewritten_disks(prewritten_disks);
        }
    }

    pub(crate) async fn finish(mut self, size: usize) -> io::Result<PreparedIntegrity> {
        if let Some(prepared) = self.prepared {
            return Ok(*prepared);
        }
        self.prepare(size).await?;
        self.prepared.map(|p| *p).ok_or_else(corrupt)
    }
}

enum Spool {
    Memory(Vec<u8>),
    File(std::fs::File),
}

impl Spool {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        if let Self::Memory(buffer) = self
            && buffer.len() + bytes.len() > MEMORY_LIMIT
        {
            let mut file = tempfile::tempfile()?;
            file.write_all(buffer)?;
            *self = Self::File(file);
        }
        match self {
            Self::Memory(buffer) => buffer.extend_from_slice(bytes),
            Self::File(file) => {
                file.seek(SeekFrom::End(0))?;
                file.write_all(bytes)?;
            }
        }
        Ok(())
    }

    fn read_at(&mut self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        match self {
            Self::Memory(buffer) => {
                let offset = usize::try_from(offset).map_err(|_| corrupt())?;
                let end = offset.checked_add(out.len()).ok_or_else(corrupt)?;
                out.copy_from_slice(buffer.get(offset..end).ok_or_else(corrupt)?);
                Ok(())
            }
            Self::File(file) => {
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(out)
            }
        }
    }
}

fn build_index(mut part: PartIntegrity, mut rows: Spool) -> io::Result<PreparedIntegrity> {
    let leaves = u64::from(part.stripes.max(1)).next_power_of_two();
    let mut tree = Spool::Memory(Vec::new());
    let row_size = part.layout.shards() * 32;
    let mut row = vec![0; row_size];
    for stripe in 0..leaves {
        let stripe_number = u32::try_from(stripe).map_err(|_| corrupt())?;
        let digest = if stripe < u64::from(part.stripes) {
            rows.read_at(stripe * u64::try_from(row_size).map_err(|_| corrupt())?, &mut row)?;
            part.leaf_digest(stripe_number, &row).map_err(|_| corrupt())?
        } else {
            part.padding_digest(stripe_number)
        };
        tree.append(&digest)?;
    }
    let mut levels = vec![0u64];
    let mut count = leaves;
    let mut level_offset = 0;
    let mut next_offset = leaves * 32;
    let mut pair = [0; 64];
    while count > 1 {
        for pair_number in 0..count / 2 {
            tree.read_at(level_offset + pair_number * 64, &mut pair)?;
            let left = pair[..32].try_into().map_err(|_| corrupt())?;
            let right = pair[32..].try_into().map_err(|_| corrupt())?;
            tree.append(&node_digest(left, right))?;
        }
        levels.push(next_offset);
        level_offset = next_offset;
        count /= 2;
        next_offset += count * 32;
    }
    let mut root = [0; 32];
    tree.read_at(level_offset, &mut root)?;
    part.root = part.root_digest(&root);
    let mut index = Spool::Memory(Vec::new());
    index.append(&part.index_header())?;
    let mut sibling = [0; 32];
    for stripe in 0..part.stripes {
        rows.read_at(u64::from(stripe) * u64::try_from(row_size).map_err(|_| corrupt())?, &mut row)?;
        index.append(&row)?;
        let mut position = u64::from(stripe);
        for &offset in levels.iter().take(levels.len() - 1) {
            tree.read_at(offset + (position ^ 1) * 32, &mut sibling)?;
            index.append(&sibling)?;
            position >>= 1;
        }
    }
    Ok(PreparedIntegrity {
        part,
        index,
        prewritten_disks: Vec::new(),
    })
}

pub(crate) struct PreparedIntegrity {
    pub(crate) part: PartIntegrity,
    index: Spool,
    prewritten_disks: Vec<bool>,
}

impl PreparedIntegrity {
    pub(crate) async fn copy_from(proof: &PartProofReader) -> io::Result<Self> {
        let mut builder = IntegrityBuilder {
            part: proof.part.clone(),
            rows: proof.part.index_header().to_vec(),
            spill: None,
            prepared: None,
        };
        for stripe in 0..proof.part.stripes {
            builder.append(&proof.record(stripe).await?).await?;
        }
        let index = builder.take_spool().await?;
        Ok(Self {
            part: proof.part.clone(),
            index,
            prewritten_disks: Vec::new(),
        })
    }

    pub(crate) fn memory_bytes(&self) -> Option<Bytes> {
        match &self.index {
            Spool::Memory(bytes) => Some(Bytes::copy_from_slice(bytes)),
            Spool::File(_) => None,
        }
    }

    pub(crate) fn set_prewritten_disks(&mut self, prewritten_disks: Vec<bool>) {
        self.prewritten_disks = prewritten_disks;
    }

    pub(crate) fn inline_bytes(&self) -> io::Result<Bytes> {
        match &self.index {
            Spool::Memory(bytes) if bytes.len() <= MAX_INLINE_PROOF_BYTES => Ok(Bytes::copy_from_slice(bytes)),
            _ => Err(corrupt()),
        }
    }

    pub(crate) fn set_inline_metadata(&self, fi: &mut FileInfo) -> io::Result<()> {
        insert_str(
            &mut fi.metadata,
            SUFFIX_INLINE_INTEGRITY,
            base64_simd::STANDARD.encode_to_string(self.inline_bytes()?),
        );
        Ok(())
    }

    /// A disk survives only if BOTH payload and index writes succeed. The caller
    /// supplies the surviving payload disks and rechecks the commit quorum.
    pub(crate) async fn write(
        self,
        disks: &mut [Option<DiskStore>],
        original_volume: &str,
        volume: &str,
        directory: &str,
    ) -> io::Result<PartIntegrity> {
        let length = self.part.index_size().map_err(|_| corrupt())?;
        let length_i64 = i64::try_from(length).map_err(|_| corrupt())?;
        let path = format!("{directory}/{}", self.part.file_name());
        let mut file = match self.index {
            Spool::Memory(bytes) => {
                if disks
                    .iter()
                    .enumerate()
                    .all(|(idx, disk)| disk.is_none() || self.prewritten_disks.get(idx).copied().unwrap_or(false))
                {
                    return Ok(self.part);
                }
                let payload = Bytes::from(bytes);
                let prewritten_disks = &self.prewritten_disks;
                futures::future::join_all(disks.iter_mut().enumerate().map(|(idx, disk_opt)| {
                    let path = &path;
                    let payload = payload.clone();
                    let already_written = prewritten_disks.get(idx).copied().unwrap_or(false);
                    async move {
                        if already_written {
                            return;
                        }
                        let Some(disk) = disk_opt.as_ref() else {
                            return;
                        };
                        let res = tokio::time::timeout(std::time::Duration::from_secs(60), async {
                            let mut writer = disk
                                .create_file(original_volume, volume, path, length_i64)
                                .await
                                .map_err(io::Error::other)?;
                            if let Some(local) = crate::disk::as_lazy_local_file_writer_mut(&mut *writer) {
                                local.write_all_bytes(payload).await?;
                            } else {
                                writer.write_all(&payload).await?;
                            }
                            writer.shutdown().await
                        })
                        .await;
                        if !matches!(res, Ok(Ok(()))) {
                            *disk_opt = None;
                        }
                    }
                }))
                .await;
                return Ok(self.part);
            }
            Spool::File(file) => file,
        };
        let mut writers = futures::future::join_all(disks.iter().map(|disk| async {
            match disk {
                Some(disk) => disk
                    .create_file(original_volume, volume, &path, length_i64)
                    .await
                    .map(Some)
                    .map_err(io::Error::other),
                None => Ok(None),
            }
        }))
        .await
        .into_iter()
        .map(Result::ok)
        .map(Option::flatten)
        .collect::<Vec<_>>();
        // The file has a single owner; duplicated descriptors share offsets.
        file = tokio::task::spawn_blocking(move || {
            file.seek(SeekFrom::Start(0))?;
            io::Result::Ok(file)
        })
        .await
        .map_err(io::Error::other)??;
        let mut source = tokio::fs::File::from_std(file);
        let mut buffer = vec![0; COPY_CHUNK_SIZE.min(length)];
        loop {
            let n = source.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            futures::future::join_all(writers.iter_mut().map(|writer| {
                let bytes = &buffer[..n];
                async move {
                    if let Some(inner) = writer
                        && !matches!(
                            tokio::time::timeout(std::time::Duration::from_secs(60), inner.write_all(bytes)).await,
                            Ok(Ok(()))
                        )
                    {
                        *writer = None;
                    }
                }
            }))
            .await;
        }
        futures::future::join_all(writers.iter_mut().zip(disks.iter_mut()).map(|(writer, disk)| async move {
            let success = if let Some(writer) = writer {
                matches!(
                    tokio::time::timeout(std::time::Duration::from_secs(60), writer.shutdown()).await,
                    Ok(Ok(()))
                )
            } else {
                false
            };
            if !success {
                *disk = None;
            }
        }))
        .await;
        Ok(self.part)
    }
}

/// A shared, two-stripe cache coalesces the N readers' proof fetches. The only
/// async lock is held across a bounded read from at most N replicas; it never
/// nests another lock. A missing local index may use any authenticated replica.
pub(crate) struct PartProofReader {
    pub(crate) part: PartIntegrity,
    sources: Vec<(DiskStore, String)>,
    volume: String,
    inline: Option<Bytes>,
    cached: Mutex<ProofCache>,
}

#[derive(Default)]
struct ProofCache {
    records: std::collections::VecDeque<(u32, Bytes)>,
    preferred: usize,
}

type VerifiedStripeKey = (uuid::Uuid, [u8; 32], u32);

static VERIFIED_STRIPE_RECORDS: std::sync::LazyLock<parking_lot::Mutex<std::collections::HashMap<VerifiedStripeKey, Bytes>>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

fn get_verified_stripe_record(part: &PartIntegrity, stripe: u32) -> Option<Bytes> {
    VERIFIED_STRIPE_RECORDS
        .lock()
        .get(&(part.generation, part.root, stripe))
        .cloned()
}

fn put_verified_stripe_record(part: &PartIntegrity, stripe: u32, record: Bytes) {
    let mut map = VERIFIED_STRIPE_RECORDS.lock();
    if map.len() >= 4096
        && let Some(evict_key) = map.keys().next().copied()
    {
        map.remove(&evict_key);
    }
    map.insert((part.generation, part.root, stripe), record);
}

pub(crate) fn invalidate_verified_stripe_records(generation: uuid::Uuid) {
    VERIFIED_STRIPE_RECORDS
        .lock()
        .retain(|(stored_gen, _, _), _| *stored_gen != generation);
}

impl PartProofReader {
    pub(crate) async fn reconstruction_proof<S: ShardBytes>(
        &self,
        stripe: usize,
        shards: &[Option<S>],
    ) -> io::Result<Option<ReconstructionProof>> {
        let mut missing = 0u16;
        for (index, shard) in shards.iter().take(usize::from(self.part.layout.data)).enumerate() {
            if shard.is_none() {
                missing |= 1 << index;
            }
        }
        if missing == 0 {
            return Ok(None);
        }
        let stripe = u32::try_from(stripe).map_err(|_| corrupt())?;
        Ok(Some(ReconstructionProof {
            part: self.part.clone(),
            stripe,
            record: self.record(stripe).await?,
            missing,
        }))
    }

    pub(crate) fn new(
        part: PartIntegrity,
        files: &[FileInfo],
        disks: &[Option<DiskStore>],
        volume: &str,
        object: &str,
    ) -> io::Result<Arc<Self>> {
        part.validate().map_err(|_| corrupt())?;
        let mut sources = Vec::with_capacity(disks.len());
        let mut inline = None;
        for (file, disk) in files.iter().zip(disks) {
            let part_index = usize::try_from(part.number)
                .ok()
                .and_then(|number| file.parts.binary_search_by_key(&number, |p| p.number).ok());
            if part_index.and_then(|index| file.parts[index].integrity.as_ref()) != Some(&part) {
                continue;
            }
            if inline.is_none() && contains_key_str(&file.metadata, SUFFIX_INLINE_INTEGRITY) {
                let candidate = (|| {
                    let value = get_consistent_str(&file.metadata, SUFFIX_INLINE_INTEGRITY)?;
                    if value.len() > MAX_INLINE_PROOF_BYTES.div_ceil(3) * 4 {
                        return None;
                    }
                    let decoded = base64_simd::STANDARD.decode_to_vec(value).ok()?;
                    if decoded.len() != part.index_size().ok()? || !decoded.starts_with(&part.index_header()) {
                        return None;
                    }
                    for stripe in 0..part.stripes {
                        let offset = part.record_offset(stripe).ok()?;
                        part.verify_record(stripe, decoded.get(offset..offset + part.record_size())?)
                            .ok()?;
                    }
                    Some(Bytes::from(decoded))
                })();
                if let Some(candidate) = candidate {
                    inline = Some(candidate);
                }
            }
            if let (Some(disk), Some(directory)) = (disk, file.data_dir.filter(|id| !id.is_nil())) {
                sources.push((disk.clone(), format!("{object}/{directory}/{}", part.file_name())));
            }
        }
        Ok(Arc::new(Self {
            part,
            sources,
            volume: volume.to_owned(),
            inline,
            cached: Mutex::new(ProofCache::default()),
        }))
    }

    pub(crate) async fn record(&self, stripe: u32) -> io::Result<Bytes> {
        let offset = self.part.record_offset(stripe).map_err(|_| corrupt())?;
        if let Some(bytes) = get_verified_stripe_record(&self.part, stripe) {
            return Ok(bytes);
        }
        let mut cached = self.cached.lock().await;
        if let Some((_, bytes)) = cached.records.iter().find(|(number, _)| *number == stripe) {
            return Ok(bytes.clone());
        }
        let size = self.part.record_size();
        let record = if let Some(inline) = &self.inline {
            inline.slice(offset..offset + size)
        } else {
            let read = |index: usize| async move {
                let (disk, path) = &self.sources[index];
                let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
                    let mut reader = disk
                        .read_file_stream(&self.volume, path, offset, size)
                        .await
                        .map_err(io::Error::other)?;
                    let mut bytes = vec![0; size];
                    reader.read_exact(&mut bytes).await?;
                    self.part.verify_record(stripe, &bytes).map_err(|_| corrupt())?;
                    io::Result::Ok(Bytes::from(bytes))
                })
                .await;
                (index, result)
            };
            if self.sources.is_empty() {
                return Err(corrupt());
            }
            let preferred = cached.preferred % self.sources.len();
            let mut pending = FuturesUnordered::new();
            pending.push(read(preferred));
            let first = tokio::select! {
                result = pending.next() => result,
                () = tokio::time::sleep(std::time::Duration::from_millis(50)) => None,
            };
            let mut found = match first {
                Some((index, Ok(Ok(bytes)))) => Some((index, bytes)),
                _ => None,
            };
            if found.is_none() {
                for index in 0..self.sources.len() {
                    if index != preferred {
                        pending.push(read(index));
                    }
                }
                while let Some((index, result)) = pending.next().await {
                    if let Ok(Ok(bytes)) = result {
                        found = Some((index, bytes));
                        break;
                    }
                }
            }
            let (index, bytes) = found.ok_or_else(corrupt)?;
            cached.preferred = index;
            bytes
        };
        self.part.verify_record(stripe, &record).map_err(|_| corrupt())?;
        if self.part.stripes <= 4 {
            put_verified_stripe_record(&self.part, stripe, record.clone());
        }
        if cached.records.len() == 2 {
            cached.records.pop_front();
        }
        cached.records.push_back((stripe, record.clone()));
        Ok(record)
    }

    pub(crate) async fn verify(&self, stripe: u32, index: usize, payload: &[u8]) -> io::Result<()> {
        let record = self.record(stripe).await?;
        // Authentication of the shared record is cached; payload digests are
        // always recomputed, including skip_verify and repaired-shard paths.
        let expected = record.get(index * 32..(index + 1) * 32).ok_or_else(corrupt)?;
        if self
            .part
            .shard_digest(stripe, index, payload)
            .map_err(|_| corrupt())?
            .as_slice()
            != expected
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct ShardVerifier {
    proof: Arc<PartProofReader>,
    coding_index: usize,
    first_stripe: u32,
    consumed: u32,
    advanced: Option<Arc<AtomicUsize>>,
}

impl ShardVerifier {
    pub(crate) fn proof(&self) -> Arc<PartProofReader> {
        Arc::clone(&self.proof)
    }
    pub(crate) fn new(
        proof: Arc<PartProofReader>,
        coding_index: usize,
        first_stripe: usize,
        advanced: Option<Arc<AtomicUsize>>,
    ) -> io::Result<Self> {
        if coding_index >= proof.part.layout.shards() {
            return Err(corrupt());
        }
        Ok(Self {
            proof,
            coding_index,
            first_stripe: first_stripe.try_into().map_err(|_| corrupt())?,
            consumed: 0,
            advanced,
        })
    }

    pub(crate) async fn verify(&mut self, payload: &[u8]) -> io::Result<()> {
        let advanced = self.advanced.as_ref().map_or(0, |value| value.load(Ordering::Acquire));
        let stripe = self
            .first_stripe
            .checked_add(u32::try_from(advanced).map_err(|_| corrupt())?)
            .and_then(|value| value.checked_add(self.consumed))
            .ok_or_else(corrupt)?;
        self.proof.verify(stripe, self.coding_index, payload).await?;
        self.consumed = self.consumed.checked_add(1).ok_or_else(corrupt)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReconstructionProof {
    part: PartIntegrity,
    stripe: u32,
    record: Bytes,
    missing: u16,
}

impl ReconstructionProof {
    pub(crate) fn verify<S: ShardBytes>(&self, shards: &[Option<S>]) -> io::Result<()> {
        for index in 0..usize::from(self.part.layout.data) {
            if self.missing & (1 << index) == 0 {
                continue;
            }
            let payload = shards
                .get(index)
                .and_then(|shard| shard.as_ref().map(ShardBytes::as_slice))
                .ok_or_else(corrupt)?;
            let actual = self.part.shard_digest(self.stripe, index, payload).map_err(|_| corrupt())?;
            if self.record.get(index * 32..(index + 1) * 32) != Some(actual.as_slice()) {
                return Err(corrupt());
            }
        }
        Ok(())
    }
}

/// Deep scans verify stripes across all disks in lockstep so a single bounded
/// proof fetch serves all N payload checks. VerifyFile from an older peer is
/// deliberately not treated as evidence of this independent commitment.
pub(crate) async fn verify_deep_parts(
    files: &[FileInfo],
    disks: &[Option<DiskStore>],
    expected: &FileInfo,
    volume: &str,
    object: &str,
    statuses: &mut std::collections::HashMap<usize, Vec<usize>>,
) -> crate::disk::error::Result<()> {
    use crate::disk::error::DiskError;
    use crate::disk::{CHECK_PART_FILE_CORRUPT, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN, conv_part_err_to_int};
    use crate::io_support::bitrot::create_bitrot_reader_from_bytes;
    let erasure = crate::erasure::coding::Erasure::try_new_with_options(
        expected.erasure.data_blocks,
        expected.erasure.parity_blocks,
        expected.erasure.block_size,
        expected.uses_legacy_checksum,
    )
    .map_err(DiskError::from)?;
    let read_timeout = crate::disk::disk_store::get_object_disk_read_timeout();
    for (part_index, part) in expected.parts.iter().enumerate() {
        let commitment = part.integrity.as_ref().ok_or(DiskError::FileCorrupt)?;
        let proof = PartProofReader::new(commitment.clone(), files, disks, volume, object).map_err(DiskError::from)?;
        if commitment.stripes == 0 && commitment.root != commitment.root_digest(&commitment.padding_digest(0)) {
            return Err(DiskError::FileCorrupt);
        }
        let part_status = statuses.get_mut(&part_index).ok_or(DiskError::FileCorrupt)?;
        // Deep verification must consume every encoded stripe, including a
        // partial final stripe, before certifying the shard's integrity.
        let length = usize::try_from(erasure.shard_file_size(part.size as i64)).map_err(|_| DiskError::FileCorrupt)?;
        let mut readers = Vec::with_capacity(disks.len());
        for (index, disk) in disks.iter().enumerate() {
            if part_status[index] != CHECK_PART_UNKNOWN {
                readers.push(None);
                continue;
            }
            let file = &files[index];
            if file.erasure.get_checksum_info(part.number).algorithm != rustfs_utils::HashAlgorithm::HighwayHash256S {
                return Err(DiskError::BitrotHashAlgoInvalid);
            }
            let path = format!("{object}/{}/part.{}", file.data_dir.unwrap_or_default(), part.number);
            let result = create_bitrot_reader_from_bytes(
                file.data.clone(),
                disk.as_ref(),
                volume,
                &path,
                0,
                length,
                erasure.shard_size(),
                rustfs_utils::HashAlgorithm::HighwayHash256S,
                false,
                false,
            )
            .await;
            match result {
                Ok(Some(mut reader)) => {
                    let coding_index = expected
                        .erasure
                        .distribution
                        .get(index)
                        .and_then(|index| index.checked_sub(1))
                        .ok_or(DiskError::FileCorrupt)?;
                    reader
                        .set_integrity(ShardVerifier::new(Arc::clone(&proof), coding_index, 0, None).map_err(DiskError::from)?)
                        .map_err(DiskError::from)?;
                    readers.push(Some(reader));
                    part_status[index] = CHECK_PART_SUCCESS;
                }
                Ok(None) => {
                    readers.push(None);
                    part_status[index] = CHECK_PART_FILE_CORRUPT;
                }
                Err(error) => {
                    if !matches!(error, DiskError::FileNotFound | DiskError::FileVersionNotFound | DiskError::FileCorrupt) {
                        return Err(error);
                    }
                    readers.push(None);
                    part_status[index] = conv_part_err_to_int(&Some(error));
                }
            }
        }
        let mut remaining = length;
        let mut buffers = vec![vec![0; erasure.shard_size().min(length)]; disks.len()];
        while remaining > 0 {
            let want = remaining.min(erasure.shard_size());
            let results = futures::future::join_all(readers.iter_mut().zip(&mut buffers).map(|(reader, buffer)| async move {
                let Some(reader) = reader else { return Ok(()) };
                let read = reader.read(&mut buffer[..want]);
                if read_timeout.is_zero() {
                    read.await.map(|_| ())
                } else {
                    tokio::time::timeout(read_timeout, read)
                        .await
                        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "integrity scan timed out"))?
                        .map(|_| ())
                }
            }))
            .await;
            for (index, result) in results.into_iter().enumerate() {
                if result.is_err() {
                    part_status[index] = CHECK_PART_FILE_CORRUPT;
                    readers[index] = None;
                }
            }
            remaining -= want;
        }
    }
    Ok(())
}

async fn verify_index_file(disk: &DiskStore, volume: &str, path: &str, part: &PartIntegrity) -> io::Result<()> {
    let size = part.index_size().map_err(|_| corrupt())?;
    let source = disk.read_file_stream(volume, path, 0, size).await.map_err(io::Error::other)?;
    let mut source = tokio::io::BufReader::with_capacity(COPY_CHUNK_SIZE, source);
    let mut header = [0; rustfs_filemeta::shard_integrity::INDEX_HEADER_SIZE];
    source.read_exact(&mut header).await?;
    if header != part.index_header() {
        return Err(corrupt());
    }
    let mut record = vec![0; part.record_size()];
    for stripe in 0..part.stripes {
        source.read_exact(&mut record).await?;
        part.verify_record(stripe, &record).map_err(|_| corrupt())?;
    }
    Ok(())
}

/// Restore only untrusted/missing proof replicas, under the existing root and
/// the caller's exclusive object lock. This does not rewrite payload or xl.meta.
pub(crate) async fn restore_proof_replicas(
    expected: &FileInfo,
    disks: &[Option<DiskStore>],
    volume: &str,
    object: &str,
) -> crate::disk::error::Result<usize> {
    use crate::disk::DeleteOptions;
    use crate::disk::RUSTFS_META_TMP_BUCKET;
    use crate::disk::error::DiskError;
    if contains_key_str(&expected.metadata, SUFFIX_INLINE_INTEGRITY) || expected.deleted || expected.is_remote() {
        return Ok(0);
    }
    let directory = expected.data_dir.filter(|id| !id.is_nil()).ok_or(DiskError::FileCorrupt)?;
    let mut repaired = 0;
    for part in &expected.parts {
        let part = part.integrity.as_ref().ok_or(DiskError::FileCorrupt)?;
        let path = format!("{object}/{directory}/{}", part.file_name());
        let checks = futures::future::join_all(disks.iter().map(|disk| async {
            let Some(disk) = disk else { return true };
            matches!(
                tokio::time::timeout(std::time::Duration::from_secs(60), verify_index_file(disk, volume, &path, part)).await,
                Ok(Ok(()))
            )
        }))
        .await;
        if checks.iter().all(|valid| *valid) {
            continue;
        }
        let proof = PartProofReader {
            part: part.clone(),
            sources: disks.iter().flatten().map(|disk| (disk.clone(), path.clone())).collect(),
            volume: volume.to_owned(),
            inline: None,
            cached: Mutex::new(ProofCache::default()),
        };
        let prepared = PreparedIntegrity::copy_from(&proof).await.map_err(DiskError::from)?;
        let temporary = format!("integrity-{}", uuid::Uuid::new_v4());
        let mut targets: Vec<_> = disks
            .iter()
            .zip(&checks)
            .map(|(disk, valid)| if *valid { None } else { disk.clone() })
            .collect();
        let result: crate::disk::error::Result<()> = async {
            prepared
                .write(&mut targets, volume, RUSTFS_META_TMP_BUCKET, &temporary)
                .await
                .map_err(DiskError::from)?;
            let source_path = format!("{temporary}/{}", part.file_name());
            for (index, valid) in checks.iter().enumerate() {
                if *valid {
                    continue;
                }
                let disk = targets[index].as_ref().ok_or(DiskError::ErasureWriteQuorum)?;
                disk.rename_file_durable(RUSTFS_META_TMP_BUCKET, &source_path, volume, &path)
                    .await?;
                repaired += 1;
            }
            Ok(())
        }
        .await;
        for disk in disks.iter().flatten() {
            let _ = disk
                .delete(
                    RUSTFS_META_TMP_BUCKET,
                    &temporary,
                    DeleteOptions {
                        recursive: true,
                        immediate: true,
                        ..Default::default()
                    },
                )
                .await;
        }
        result?;
    }
    Ok(repaired)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn integrity_index_rejects_donor_payload_record_position_and_generation() {
        let layout = IntegrityLayout::new(2, 2, 8, false).expect("layout");
        let mut builder = IntegrityBuilder::new(layout, 1).expect("builder");
        builder.part.generation = uuid::Uuid::parse_str("00112233-4455-4677-8899-aabbccddeeff").expect("fixed generation");
        let shards = [b"ABCD".as_slice(), b"EFGH", b"IJKL", b"MNOP"];
        builder.push(shards.into_iter()).await.expect("first stripe");
        builder.push(shards.into_iter()).await.expect("second stripe");
        let prepared = builder.finish(16).await.expect("index");
        let bytes = prepared.inline_bytes().expect("inline index");
        let part = prepared.part;
        // Frozen independently with Python hashlib from the v1 byte layout.
        let expected_root = [
            33, 254, 159, 226, 214, 235, 239, 175, 182, 132, 172, 81, 6, 234, 125, 187, 25, 98, 190, 6, 251, 241, 171, 33, 150,
            188, 179, 250, 26, 60, 47, 198,
        ];
        assert_eq!(part.root, expected_root);
        let offset = part.record_offset(0).expect("offset");
        let record = &bytes[offset..offset + part.record_size()];
        part.verify_shard(0, 0, shards[0], record).expect("target payload");
        assert!(part.verify_shard(0, 0, shards[1], record).is_err());
        assert!(part.verify_record(1, record).is_err());
        let mut other = part.clone();
        other.generation = uuid::Uuid::new_v4();
        assert!(other.verify_record(0, record).is_err());
        for position in [0, 32, record.len() - 1] {
            let mut corrupt_record = record.to_vec();
            corrupt_record[position] ^= 1;
            assert!(part.verify_record(0, &corrupt_record).is_err());
        }
        let proof = Arc::new(PartProofReader {
            part,
            sources: vec![],
            volume: "bucket".to_owned(),
            inline: Some(bytes),
            cached: Mutex::new(ProofCache::default()),
        });
        let mut verifier = ShardVerifier::new(proof, 0, 0, None).expect("verifier");
        verifier.verify(shards[0]).await.expect("stripe 0");
        verifier.verify(shards[0]).await.expect("stripe 1");
        assert!(verifier.verify(shards[0]).await.is_err(), "no clean success past committed stripes");
    }

    #[tokio::test]
    async fn integrity_reconstruction_rejects_wrong_decoder_output() {
        let mut builder = IntegrityBuilder::new(IntegrityLayout::new(2, 2, 8, false).expect("layout"), 1).expect("builder");
        builder
            .push([b"abcd".as_slice(), b"efgh", b"ijkl", b"mnop"].into_iter())
            .await
            .expect("stripe");
        let prepared = builder.finish(8).await.expect("index");
        let index = prepared.inline_bytes().expect("bytes");
        let proof = PartProofReader {
            part: prepared.part,
            inline: Some(index),
            sources: Vec::new(),
            volume: String::new(),
            cached: Mutex::new(ProofCache::default()),
        };
        let mut shards = vec![None, Some(b"efgh".to_vec()), Some(b"ijkl".to_vec()), Some(b"mnop".to_vec())];
        let verification = proof
            .reconstruction_proof(0, &shards)
            .await
            .expect("proof")
            .expect("missing data");
        shards[0] = Some(b"donr".to_vec());
        assert!(verification.verify(&shards).is_err());
        shards[0] = Some(b"abcd".to_vec());
        verification.verify(&shards).expect("verified reconstruction");
    }

    #[tokio::test]
    async fn integrity_builder_spills_without_changing_proofs() {
        let layout = IntegrityLayout::new(12, 4, 12, false).expect("layout");
        let mut builder = IntegrityBuilder::new(layout, 7).expect("builder");
        let shards = [b"x".as_slice(); 16];
        for _ in 0..2050 {
            builder.push(shards.into_iter()).await.expect("stripe");
        }
        assert!(builder.spill.is_some(), "digest rows exceed memory bound");
        let mut prepared = builder.finish(2050 * 12).await.expect("spilled tree");
        assert!(matches!(prepared.index, Spool::File(_)), "materialized index is also bounded");
        let mut record = vec![0; prepared.part.record_size()];
        for stripe in [0, 1025, 2049] {
            let offset = prepared.part.record_offset(stripe).expect("offset");
            prepared
                .index
                .read_at(u64::try_from(offset).expect("offset fits"), &mut record)
                .expect("proof");
            prepared.part.verify_shard(stripe, 15, b"x", &record).expect("spilled proof");
        }
    }

    #[tokio::test]
    async fn verified_stripe_record_cache_serves_verified_record_and_still_verifies_payload() {
        let layout = IntegrityLayout::new(2, 2, 8, false).expect("layout");
        let mut builder = IntegrityBuilder::new(layout, 1).expect("builder");
        let shards = [b"ABCD".as_slice(), b"EFGH", b"IJKL", b"MNOP"];
        builder.push(shards.into_iter()).await.expect("stripe");
        let prepared = builder.finish(8).await.expect("index");
        let inline_bytes = prepared.inline_bytes().expect("inline bytes");
        let part = prepared.part;

        let warm_reader = PartProofReader {
            part: part.clone(),
            sources: Vec::new(),
            volume: "bucket".to_owned(),
            inline: Some(inline_bytes),
            cached: Mutex::new(ProofCache::default()),
        };
        warm_reader.verify(0, 0, shards[0]).await.expect("warm verify");
        assert!(
            get_verified_stripe_record(&part, 0).is_some(),
            "small verified stripe record should populate the verified stripe cache"
        );

        let cached_reader = Arc::new(PartProofReader {
            part,
            sources: Vec::new(),
            volume: "bucket".to_owned(),
            inline: None,
            cached: Mutex::new(ProofCache::default()),
        });
        cached_reader.verify(0, 0, shards[0]).await.expect("cached verify");
        assert!(
            cached_reader.verify(0, 0, shards[1]).await.is_err(),
            "cached record must still reject corrupt or mismatched shard payloads"
        );
    }

    #[tokio::test]
    async fn shard_verifier_rejects_out_of_bounds_index_and_empty_proof_sources_fail_closed() {
        let mut builder = IntegrityBuilder::new(IntegrityLayout::new(2, 2, 8, false).expect("layout"), 1).expect("builder");
        builder
            .push([b"abcd".as_slice(), b"efgh", b"ijkl", b"mnop"].into_iter())
            .await
            .expect("stripe");
        let prepared = builder.finish(8).await.expect("index");
        let proof = Arc::new(PartProofReader {
            part: prepared.part,
            sources: Vec::new(),
            volume: "bucket".to_owned(),
            inline: None,
            cached: Mutex::new(ProofCache::default()),
        });

        // coding_index >= layout.shards() is rejected at construction time.
        let err = ShardVerifier::new(proof.clone(), 4, 0, None)
            .err()
            .expect("coding_index out of bounds must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("shard integrity verification failed"));

        // Valid coding_index with neither inline bytes nor disk sources fails closed when reading stripe record.
        let mut verifier = ShardVerifier::new(proof, 0, 0, None).expect("verifier");
        let err = verifier
            .verify(b"abcd")
            .await
            .expect_err("empty proof sources with no inline index must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("shard integrity verification failed"));
    }

    #[tokio::test]
    async fn spilled_index_write_verify_and_restore_proof_replicas_round_trip() {
        let (_dirs, store) = crate::services::rebalance::test_store_with_persisted_rebalance_meta(Default::default()).await;
        let mut disk_slots = store.pools[0].disk_set[0].get_local_disks().await;
        let disks: Vec<_> = disk_slots.iter().flatten().cloned().collect();
        assert_eq!(disks.len(), 2);
        let volume = "spilled-integrity-bucket";
        for disk in &disks {
            disk.make_volume(volume).await.expect("make volume");
        }

        // 2 shards (1 data + 1 parity) matches the 2-disk fixture; 2350 stripes * 448B > 1 MiB spills to Spool::File(_).
        let layout = IntegrityLayout::new(1, 1, 8, false).expect("layout");
        let mut builder = IntegrityBuilder::new(layout, 1).expect("builder");
        let shards = [b"abcdefgh".as_slice(); 2];
        for _ in 0..2350 {
            builder.push(shards.into_iter()).await.expect("stripe");
        }
        let part_size = 2350 * 8;
        let prepared = builder.finish(part_size).await.expect("spilled index");
        assert!(matches!(prepared.index, Spool::File(_)));

        let data_dir = uuid::Uuid::new_v4();
        let object = "object";
        let directory = format!("{object}/{data_dir}");
        let part = prepared
            .write(&mut disk_slots, volume, volume, &directory)
            .await
            .expect("write spilled index");

        let rel_path = format!("{directory}/{}", part.file_name());
        for disk in &disks {
            verify_index_file(disk, volume, &rel_path, &part)
                .await
                .expect("verify spilled index on disk");
        }

        // Corrupt replica 0; restore_proof_replicas should repair it from healthy replica 1.
        disks[0]
            .write_all(volume, &rel_path, bytes::Bytes::from_static(b"corrupted-index"))
            .await
            .expect("corrupt disk 0");
        assert!(verify_index_file(&disks[0], volume, &rel_path, &part).await.is_err());

        let mut fi = rustfs_filemeta::FileInfo {
            volume: volume.to_owned(),
            name: object.to_owned(),
            data_dir: Some(data_dir),
            erasure: rustfs_filemeta::ErasureInfo {
                data_blocks: 1,
                parity_blocks: 1,
                block_size: 8,
                ..Default::default()
            },
            parts: vec![rustfs_filemeta::ObjectPartInfo {
                number: 1,
                size: part_size,
                actual_size: part_size as i64,
                integrity: Some(part.clone()),
                ..Default::default()
            }],
            ..Default::default()
        };
        fi.persist_shard_integrity().expect("persist shard integrity");
        let repaired = restore_proof_replicas(&fi, &disk_slots, volume, object)
            .await
            .expect("restore proof replica 0");
        assert_eq!(repaired, 1);
        verify_index_file(&disks[0], volume, &rel_path, &part)
            .await
            .expect("repaired replica 0 must verify");

        // Negative durability boundary: when all replicas are corrupted, restore_proof_replicas must fail closed.
        for disk in &disks {
            disk.write_all(volume, &rel_path, bytes::Bytes::from_static(b"all-corrupted"))
                .await
                .expect("corrupt all replicas");
        }
        let err = restore_proof_replicas(&fi, &disk_slots, volume, object)
            .await
            .expect_err("all corrupted proof replicas must fail closed");
        assert!(
            err.to_string().contains("shard integrity verification failed"),
            "unexpected error when all proof replicas are corrupt: {err}"
        );
    }
}
