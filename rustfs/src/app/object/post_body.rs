// Copyright 2024 RustFS Team
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

//! Bounded staging for unknown-length browser uploads.
//! Storage receives a file stream only after the gateway file body reaches verified EOF.

use super::native_body::{BodyChunks, from_io_stream};
use super::request_body::{BodyReadControl, UploadChunks, demand_native_upload, guard_native_upload_progress};
use super::{HashReader, put_object_body_read_timeout, s3s_body_error_to_io};
use crate::app::RequestEnvelope;
use crate::error::ApiError;
use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{AeadInOut, KeyInit},
};
use bytes::Bytes;
use futures::{StreamExt as _, stream};
use http::HeaderMap;
use parking_lot::Mutex;
use rand::{TryRng as _, rngs::SysRng};
use rustfs_gateway_stream::Body;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::AsyncReadExt as _;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::io::StreamReader;
use zeroize::Zeroizing;

mod archive_snapshot;
use archive_snapshot::{ArchiveSnapshot, ArchiveSnapshotBudget, PreflightedArchiveSnapshot};
mod archive_binding;
pub(crate) use archive_binding::{
    PreflightedPostArchive, VerifiedPostArchivePolicy, post_archive_admission_closed, stage_post_archive,
};
#[cfg(test)]
pub(crate) use archive_binding::{stage_post_archive_for_test, with_archive_resources_for_test};

type PostArchiveKeyPolicy = dyn Fn(&str) -> bool + Send + Sync;

const REPLAY_CHUNK_SIZE: usize = 64 * 1024;
const TAG_SIZE: usize = 16;
const FRAME_OVERHEAD: u64 = 4 + TAG_SIZE as u64;
const POST_STAGING_SLOTS: usize = 2;
const POST_STAGING_BUDGET: u64 = 6 * 1024 * 1024 * 1024;
const POST_ARCHIVE_SNAPSHOT_LIMIT: u64 = 0;
const POST_STAGING_MAX_FREE_RESERVE: u64 = 1024 * 1024 * 1024;
const POST_STAGING_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const POST_STAGING_SPACE_ATTEMPTS: usize = 4;
static POST_STAGING: OnceLock<io::Result<StagingResources>> = OnceLock::new();

struct StagingResources {
    slots: Arc<Semaphore>,
    budget: Arc<StagingBudget>,
    archive_snapshot_budget: Arc<ArchiveSnapshotBudget>,
    closer: SyncSender<StagingOwned>,
}

impl StagingResources {
    fn new() -> io::Result<Self> {
        Ok(Self {
            slots: Arc::new(Semaphore::new(POST_STAGING_SLOTS)),
            budget: Arc::new(StagingBudget::new(POST_STAGING_BUDGET, staging_space)),
            // Production archive admission remains closed until its resource and identity gates pass.
            archive_snapshot_budget: ArchiveSnapshotBudget::new(POST_ARCHIVE_SNAPSHOT_LIMIT),
            closer: staging_closer(POST_STAGING_SLOTS)?,
        })
    }
}

#[derive(Clone, Copy)]
struct StagingSpace {
    available: u64,
    total: u64,
    block_size: u64,
}

impl StagingSpace {
    fn rounded(&self, bytes: u64) -> Result<u64, StagingWriteError> {
        bytes
            .checked_add(self.block_size.checked_sub(1).ok_or(StagingWriteError::Io)?)
            .map(|value| value / self.block_size * self.block_size)
            .ok_or(StagingWriteError::Io)
    }

    fn required(&self, pending: u64, rounded: u64) -> Result<u64, StagingWriteError> {
        if self.block_size == 0 || self.total == 0 || self.available > self.total {
            return Err(StagingWriteError::Io);
        }
        POST_STAGING_MAX_FREE_RESERVE
            .min((self.total / 10).max(1))
            .checked_add(pending)
            .and_then(|required| required.checked_add(rounded))
            .ok_or(StagingWriteError::Capacity)
    }
}

#[cfg(unix)]
fn staging_space(file: &std::fs::File) -> io::Result<StagingSpace> {
    let stat = rustix::fs::fstatvfs(file)?;
    let block_size = if stat.f_frsize == 0 { stat.f_bsize } else { stat.f_frsize };
    staging_space_from_blocks(block_size, stat.f_blocks, stat.f_bfree, stat.f_bavail)
}

#[cfg(any(unix, test))]
fn staging_space_from_blocks(block_size: u64, blocks: u64, free: u64, available: u64) -> io::Result<StagingSpace> {
    if block_size == 0 || blocks == 0 || free > blocks || available > free {
        return Err(io::Error::other("invalid POST staging filesystem statistics"));
    }
    let available = available
        .checked_mul(block_size)
        .ok_or_else(|| io::Error::other("invalid POST staging available space"))?;
    let total = blocks
        .checked_mul(block_size)
        .ok_or_else(|| io::Error::other("invalid POST staging filesystem size"))?;
    Ok(StagingSpace {
        available,
        total,
        block_size,
    })
}

#[cfg(not(unix))]
fn staging_space(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Err(io::Error::other("POST staging filesystem space check is unavailable"))
}

#[derive(Default)]
struct StagingBudgetState {
    reserved: u64,
    pending: u64,
    completed_writes: u64,
}

type StagingSpaceProbe = dyn Fn(&std::fs::File) -> io::Result<StagingSpace> + Send + Sync;

struct StagingBudget {
    limit: u64,
    state: Mutex<StagingBudgetState>,
    space: Box<StagingSpaceProbe>,
}

#[derive(Debug)]
enum StagingWriteError {
    Capacity,
    Io,
}

impl StagingBudget {
    fn new(limit: u64, space: impl Fn(&std::fs::File) -> io::Result<StagingSpace> + Send + Sync + 'static) -> Self {
        Self {
            limit,
            state: Mutex::new(StagingBudgetState::default()),
            space: Box::new(space),
        }
    }

    fn preflight(&self, file: &std::fs::File) -> Result<(), StagingWriteError> {
        for _ in 0..POST_STAGING_SPACE_ATTEMPTS {
            let generation = {
                let state = self.state.lock();
                state
                    .reserved
                    .checked_add(FRAME_OVERHEAD)
                    .filter(|&total| total <= self.limit)
                    .ok_or(StagingWriteError::Capacity)?;
                if state.completed_writes == u64::MAX {
                    return Err(StagingWriteError::Capacity);
                }
                state.completed_writes
            };
            let space = (self.space)(file).map_err(|_| StagingWriteError::Io)?;
            let rounded = space.rounded(FRAME_OVERHEAD)?;
            let state = self.state.lock();
            if state.completed_writes != generation {
                continue;
            }
            state
                .reserved
                .checked_add(FRAME_OVERHEAD)
                .filter(|&total| total <= self.limit)
                .ok_or(StagingWriteError::Capacity)?;
            if space.available < space.required(state.pending, rounded)? {
                return Err(StagingWriteError::Capacity);
            }
            return Ok(());
        }
        Err(StagingWriteError::Capacity)
    }
}

struct StagingAllocation {
    budget: Arc<StagingBudget>,
    reserved: AtomicU64,
}

impl StagingAllocation {
    fn new(budget: Arc<StagingBudget>) -> Self {
        Self {
            budget,
            reserved: AtomicU64::new(0),
        }
    }

    // The caller holds the file lock first. No path takes these locks in reverse
    // order, and the budget lock is never held across an async suspension.
    fn reserve(&self, file: &std::fs::File, bytes: u64) -> Result<PendingStagingWrite<'_>, StagingWriteError> {
        self.reserve_with(file, bytes, |file| (self.budget.space)(file))
    }

    fn reserve_with(
        &self,
        file: &std::fs::File,
        bytes: u64,
        mut probe: impl FnMut(&std::fs::File) -> io::Result<StagingSpace>,
    ) -> Result<PendingStagingWrite<'_>, StagingWriteError> {
        for _ in 0..POST_STAGING_SPACE_ATTEMPTS {
            let generation = {
                let state = self.budget.state.lock();
                state
                    .reserved
                    .checked_add(bytes)
                    .filter(|&total| total <= self.budget.limit)
                    .ok_or(StagingWriteError::Capacity)?;
                if state.completed_writes == u64::MAX {
                    return Err(StagingWriteError::Capacity);
                }
                state.completed_writes
            };
            // A filesystem query can block. Never hold the budget lock across it;
            // if another write finishes during the query, the snapshot may be old.
            let space = probe(file).map_err(|_| StagingWriteError::Io)?;
            let rounded = space.rounded(bytes)?;
            let mut state = self.budget.state.lock();
            if state.completed_writes != generation {
                continue;
            }
            let reserved = state
                .reserved
                .checked_add(bytes)
                .filter(|&total| total <= self.budget.limit)
                .ok_or(StagingWriteError::Capacity)?;
            let own_reserved = self
                .reserved
                .load(Ordering::Relaxed)
                .checked_add(bytes)
                .ok_or(StagingWriteError::Capacity)?;
            let pending = state.pending.checked_add(rounded).ok_or(StagingWriteError::Capacity)?;
            let required = space.required(state.pending, rounded)?;
            if space.available < required {
                return Err(StagingWriteError::Capacity);
            }
            state.reserved = reserved;
            state.pending = pending;
            self.reserved.store(own_reserved, Ordering::Relaxed);
            return Ok(PendingStagingWrite {
                budget: &self.budget,
                bytes: rounded,
            });
        }
        Err(StagingWriteError::Capacity)
    }
}

impl Drop for StagingAllocation {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();
        state.reserved -= self.reserved.load(Ordering::Relaxed);
    }
}

struct PendingStagingWrite<'a> {
    budget: &'a StagingBudget,
    bytes: u64,
}

impl Drop for PendingStagingWrite<'_> {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();
        state.pending -= self.bytes;
        // Saturation permanently fails closed rather than permitting a stale
        // snapshot to become indistinguishable from a current one.
        state.completed_writes = state.completed_writes.saturating_add(1);
    }
}

// Fields drop in declaration order: the file handle drops before the allocation and permit.
struct StagingOwned {
    file: Mutex<std::fs::File>,
    allocation: StagingAllocation,
    _permit: OwnedSemaphorePermit,
    #[cfg(test)]
    before_close: Option<Box<dyn FnOnce() + Send + Sync>>,
}

fn staging_closer(capacity: usize) -> io::Result<SyncSender<StagingOwned>> {
    let (sender, receiver) = mpsc::sync_channel::<StagingOwned>(capacity);
    let worker = std::thread::Builder::new()
        .name("rustfs-post-close".to_owned())
        .spawn(move || {
            while let Ok(owned) = receiver.recv() {
                #[cfg(test)]
                let mut owned = owned;
                #[cfg(test)]
                if let Some(hook) = owned.before_close.take() {
                    hook();
                }
                drop(owned);
            }
        })?;
    // The worker exits after its last sender is dropped; staging must not join
    // it from an async executor while a filesystem close may be blocked.
    drop(worker);
    Ok(sender)
}

// Blocking I/O holds an Arc. Its last drop hands the file, budget and permit to
// the closer together, including on cancellation and after runtime shutdown.
struct StagedFile {
    owned: Option<StagingOwned>,
    cipher: Aes256Gcm,
    closer: SyncSender<StagingOwned>,
}

impl StagedFile {
    fn new(
        file: std::fs::File,
        cipher: Aes256Gcm,
        budget: Arc<StagingBudget>,
        permit: OwnedSemaphorePermit,
        closer: SyncSender<StagingOwned>,
    ) -> Self {
        Self {
            owned: Some(StagingOwned {
                file: Mutex::new(file),
                allocation: StagingAllocation::new(budget),
                _permit: permit,
                #[cfg(test)]
                before_close: None,
            }),
            cipher,
            closer,
        }
    }
}

impl Deref for StagedFile {
    type Target = StagingOwned;

    fn deref(&self) -> &Self::Target {
        // The owned resources are taken only by Drop, after the last reference.
        self.owned.as_ref().expect("live staged file owns its resources")
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        let Some(owned) = self.owned.take() else {
            return;
        };
        match self.closer.try_send(owned) {
            Ok(()) => {}
            // The process-wide queue has one entry per permit, so Full cannot
            // occur under normal admission. If the worker has failed, closing
            // synchronously is safer than releasing the budget before the file.
            Err(TrySendError::Full(owned) | TrySendError::Disconnected(owned)) => drop(owned),
        }
    }
}

fn staging_cipher() -> io::Result<Aes256Gcm> {
    let mut key = Zeroizing::new([0u8; 32]);
    SysRng.try_fill_bytes(key.as_mut()).map_err(io::Error::other)?;
    Aes256Gcm::new_from_slice(key.as_ref()).map_err(|_| io::Error::other("invalid POST staging key"))
}

#[cfg(unix)]
fn ensure_unlinked(file: &std::fs::File) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    if file.metadata()?.nlink() != 0 {
        return Err(io::Error::other("POST staging file is still linked"));
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_unlinked(_file: &std::fs::File) -> io::Result<()> {
    Ok(())
}

async fn file_io<T: Send + 'static>(
    file: Arc<StagedFile>,
    operation: impl FnOnce(&mut std::fs::File) -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(move || operation(&mut file.file.lock()))
        .await
        .map_err(|_| io::Error::other("POST staging task failed"))?
}

// The per-upload key is random and never persisted. A counter supplies distinct
// nonces under that key, including for the authenticated zero-length end frame.
fn frame_parameters(sequence: u64, length: u32) -> ([u8; 12], [u8; 12]) {
    let mut nonce = [0; 12];
    nonce[4..].copy_from_slice(&sequence.to_be_bytes());
    let mut aad = [0; 12];
    aad[..8].copy_from_slice(&sequence.to_be_bytes());
    aad[8..].copy_from_slice(&length.to_be_bytes());
    (nonce, aad)
}

fn write_frame(file: &mut std::fs::File, cipher: &Aes256Gcm, sequence: u64, mut plaintext: Zeroizing<Vec<u8>>) -> io::Result<()> {
    let length = u32::try_from(plaintext.len()).map_err(|_| io::Error::other("POST staging frame too large"))?;
    let (nonce, aad) = frame_parameters(sequence, length);
    cipher
        .encrypt_in_place(&Nonce::from(nonce), &aad, &mut *plaintext)
        .map_err(|_| io::Error::other("POST staging encryption failed"))?;
    file.write_all(&length.to_be_bytes())?;
    file.write_all(&plaintext)
}

fn read_frame(file: &mut impl Read, cipher: &Aes256Gcm, sequence: u64) -> io::Result<Zeroizing<Vec<u8>>> {
    let mut header = [0; 4];
    file.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header);
    let length_usize = usize::try_from(length).map_err(|_| io::Error::other("invalid POST staging frame length"))?;
    if length_usize > REPLAY_CHUNK_SIZE {
        return Err(io::Error::other("invalid POST staging frame length"));
    }
    let mut ciphertext = Zeroizing::new(vec![0; length_usize + TAG_SIZE]);
    file.read_exact(&mut ciphertext)?;
    let (nonce, aad) = frame_parameters(sequence, length);
    cipher
        .decrypt_in_place(&Nonce::from(nonce), &aad, &mut *ciphertext)
        .map_err(|_| io::Error::other("POST staging authentication failed"))?;
    Ok(ciphertext)
}

fn read_end_frame(file: &mut impl Read, cipher: &Aes256Gcm, sequence: u64) -> io::Result<()> {
    if !read_frame(file, cipher, sequence)?.is_empty() || read_chunk(file, &mut [0; 1])? != 0 {
        return Err(io::Error::other("invalid POST staging end"));
    }
    Ok(())
}

async fn write_post_frame(file: &Arc<StagedFile>, sequence: &mut u64, plaintext: Zeroizing<Vec<u8>>) -> S3Result<()> {
    let next = sequence.checked_add(1).ok_or_else(staging_io_error)?;
    let bytes = u64::try_from(plaintext.len())
        .ok()
        .and_then(|length| length.checked_add(FRAME_OVERHEAD))
        .ok_or_else(staging_io_error)?;
    let staged = Arc::clone(file);
    let current = *sequence;
    let result = tokio::task::spawn_blocking(move || {
        let mut file = staged.file.lock();
        let _pending = staged.allocation.reserve(&file, bytes)?;
        write_frame(&mut file, &staged.cipher, current, plaintext).map_err(|_| StagingWriteError::Io)
    })
    .await
    .map_err(|_| staging_io_error())?;
    match result {
        Ok(()) => {}
        Err(StagingWriteError::Capacity) => return Err(staging_capacity_error()),
        Err(StagingWriteError::Io) => return Err(staging_io_error()),
    }
    *sequence = next;
    Ok(())
}

fn read_chunk(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        match reader.read(buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

fn acquire_staging_slot(slots: &Arc<Semaphore>) -> S3Result<OwnedSemaphorePermit> {
    Arc::clone(slots).try_acquire_owned().map_err(|_| staging_capacity_error())
}

fn staging_capacity_error() -> S3Error {
    s3_error!(SlowDown, "POST staging capacity is exhausted")
}

fn staging_write_error(error: StagingWriteError) -> S3Error {
    match error {
        StagingWriteError::Capacity => staging_capacity_error(),
        StagingWriteError::Io => staging_io_error(),
    }
}

async fn with_staging_deadline<T>(duration: Duration, operation: impl Future<Output = S3Result<T>>) -> S3Result<T> {
    let deadline = tokio::time::Instant::now() + duration;
    tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => Err(s3_error!(RequestTimeout, "POST staging time limit exceeded")),
        result = operation => {
            if tokio::time::Instant::now() >= deadline {
                Err(s3_error!(RequestTimeout, "POST staging time limit exceeded"))
            } else {
                result
            }
        }
    }
}

struct FinishBodyRead(BodyReadControl);

impl Drop for FinishBodyRead {
    fn drop(&mut self) {
        self.0.finish();
    }
}

/// Optional file checksums to finish before a staged body can be handed to a writer.
pub(crate) struct PostBodyIntegrity {
    checksum_headers: HeaderMap,
    content_md5: Option<String>,
    content_sha256: Option<String>,
}

impl PostBodyIntegrity {
    pub(crate) fn for_archive(checksum_headers: HeaderMap, content_md5: Option<String>, content_sha256: Option<String>) -> Self {
        Self {
            checksum_headers,
            content_md5,
            content_sha256,
        }
    }
}

fn checked_post_chunks(chunks: UploadChunks, integrity: Option<PostBodyIntegrity>) -> S3Result<UploadChunks> {
    let Some(integrity) = integrity else {
        return Ok(chunks);
    };
    let md5 = integrity
        .content_md5
        .as_deref()
        .map(|value| {
            let decoded = base64_simd::STANDARD
                .decode_to_vec(value.as_bytes())
                .map_err(|_| s3_error!(InvalidDigest))?;
            if decoded.len() != 16 {
                return Err(s3_error!(InvalidDigest));
            }
            Ok(hex_simd::encode_to_string(decoded, hex_simd::AsciiCase::Lower))
        })
        .transpose()?;
    let source = StreamReader::new(chunks.map(|chunk| chunk.map_err(s3s_body_error_to_io)));
    // Zero avoids a hard length limit while retaining HashReader's Content-MD5 check.
    // The file stream and staging limit still have to reach their own verified EOF.
    let mut reader = HashReader::from_stream(source, 0, 0, md5, integrity.content_sha256, false).map_err(ApiError::from)?;
    reader
        .add_checksum(&integrity.checksum_headers, None, false)
        .map_err(ApiError::from)?;
    let checked = stream::try_unfold(reader, |mut reader| async move {
        let mut buffer = Zeroizing::new(vec![0u8; REPLAY_CHUNK_SIZE]);
        let size = reader.read(&mut buffer[..]).await?;
        if size == 0 {
            return Ok(None);
        }
        buffer.truncate(size);
        Ok(Some((Bytes::from_owner(buffer), reader)))
    });
    let (checked, _) = BodyChunks::with_trailers(from_io_stream(checked), None);
    Ok(Box::new(checked))
}

pub(crate) async fn stage_post_body(
    envelope: &RequestEnvelope,
    bucket: &str,
    key: &str,
    body: Body,
    integrity: Option<PostBodyIntegrity>,
) -> S3Result<(Body, i64)> {
    let resources = POST_STAGING
        .get_or_init(StagingResources::new)
        .as_ref()
        .map_err(|_| staging_io_error())?;
    let permit = acquire_staging_slot(&resources.slots)?;
    with_staging_deadline(
        POST_STAGING_TIMEOUT,
        stage_admitted_post_body(
            envelope,
            bucket,
            key,
            (body, integrity),
            permit,
            Arc::clone(&resources.budget),
            resources.closer.clone(),
        ),
    )
    .await
}

async fn stage_admitted_post_body(
    envelope: &RequestEnvelope,
    bucket: &str,
    key: &str,
    input: (Body, Option<PostBodyIntegrity>),
    permit: OwnedSemaphorePermit,
    budget: Arc<StagingBudget>,
    closer: SyncSender<StagingOwned>,
) -> S3Result<(Body, i64)> {
    let (file, length) = ingest_admitted_post_body(envelope, bucket, key, input, permit, budget, closer).await?;
    let replay_length = u64::try_from(length).map_err(|_| staging_io_error())?;
    Ok((replay_post_file(file, replay_length), length))
}

async fn ingest_admitted_post_body(
    envelope: &RequestEnvelope,
    bucket: &str,
    key: &str,
    input: (Body, Option<PostBodyIntegrity>),
    permit: OwnedSemaphorePermit,
    budget: Arc<StagingBudget>,
    closer: SyncSender<StagingOwned>,
) -> S3Result<(Arc<StagedFile>, i64)> {
    let (body, integrity) = input;
    let file = tokio::task::spawn_blocking(move || {
        let cipher = staging_cipher().map_err(|_| StagingWriteError::Io)?;
        let file = tempfile::tempfile().map_err(|_| StagingWriteError::Io)?;
        ensure_unlinked(&file).map_err(|_| StagingWriteError::Io)?;
        budget.preflight(&file)?;
        Ok::<_, StagingWriteError>(Arc::new(StagedFile::new(file, cipher, budget, permit, closer)))
    })
    .await
    .map_err(|_| staging_io_error())?
    .map_err(staging_write_error)?;
    let request_id = envelope.request_context().map_or("", |context| context.request_id.as_str());
    let timeout = put_object_body_read_timeout();
    let (chunks, _) = BodyChunks::with_trailers(body, None);
    let chunks: UploadChunks = Box::new(chunks);
    // The gateway owns the transport monitor. Expiring its raw body as an I/O
    // error would be rendered as IncompleteBody before the handler can answer
    // RequestTimeout, so guard the decoded file stream independently.
    let (chunks, control) = guard_native_upload_progress(chunks, timeout, bucket, key, request_id, 0);
    let chunks = checked_post_chunks(demand_native_upload(chunks, control.clone()), integrity)?;
    let finish = FinishBodyRead(control);
    let result = copy_post_file(&file, chunks, rustfs_config::MAX_SINGLE_PUT_OBJECT_SIZE).await;
    drop(finish);
    let length = result?;
    file_io(Arc::clone(&file), |file| {
        file.flush()?;
        file.seek(SeekFrom::Start(0))?;
        Ok(())
    })
    .await
    .map_err(|_| staging_io_error())?;
    // A zero-length consumer might never poll its body, so verify its end here.
    if length == 0 {
        verify_empty_staging(&file).await?;
    }
    Ok((file, length))
}

async fn verify_empty_staging(file: &Arc<StagedFile>) -> S3Result<()> {
    let staged = Arc::clone(file);
    file_io(Arc::clone(file), move |file| {
        read_end_frame(file, &staged.cipher, 0)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(())
    })
    .await
    .map_err(|_| staging_io_error())
}

fn replay_post_file(file: Arc<StagedFile>, length: u64) -> Body {
    let replay = stream::try_unfold((file, 0u64, Some(length)), |(file, sequence, remaining)| async move {
        let Some(remaining) = remaining else {
            return Ok(None);
        };
        let staged = Arc::clone(&file);
        let (bytes, next_sequence, next_remaining) = file_io(Arc::clone(&file), move |file| {
            if remaining == 0 {
                read_end_frame(file, &staged.cipher, sequence)?;
                return Ok((Zeroizing::new(Vec::new()), sequence, 0));
            }
            let bytes = read_frame(file, &staged.cipher, sequence)?;
            let frame_length = u64::try_from(bytes.len()).map_err(io::Error::other)?;
            if frame_length == 0 || frame_length > remaining {
                return Err(io::Error::other("invalid POST staging plaintext length"));
            }
            let next_sequence = sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("POST staging frame counter exhausted"))?;
            let next_remaining = remaining - frame_length;
            // Consumers can stop after the known length. Authenticate the end and
            // reject trailing bytes before releasing the last plaintext frame.
            if next_remaining == 0 {
                read_end_frame(file, &staged.cipher, next_sequence)?;
            }
            Ok((bytes, next_sequence, next_remaining))
        })
        .await?;
        // Bytes owns the zeroizing frame until its last clone or slice is dropped.
        Ok(if bytes.is_empty() {
            None
        } else if next_remaining == 0 {
            Some((Bytes::from_owner(bytes), (file, next_sequence, None)))
        } else {
            Some((Bytes::from_owner(bytes), (file, next_sequence, Some(next_remaining))))
        })
    });
    from_io_stream(replay)
}

async fn copy_post_file(file: &Arc<StagedFile>, mut chunks: UploadChunks, limit: u64) -> S3Result<i64> {
    let mut length = 0u64;
    let mut sequence = 0u64;
    // Reserve tag space before copying plaintext so encryption cannot reallocate it.
    let mut pending = Zeroizing::new(Vec::with_capacity(REPLAY_CHUNK_SIZE + TAG_SIZE));
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(post_body_error)?;
        let next_length = u64::try_from(chunk.len())
            .ok()
            .and_then(|chunk_length| length.checked_add(chunk_length))
            .filter(|&length| length <= limit);
        let Some(next_length) = next_length else {
            return Err(s3_error!(EntityTooLarge, "The object is too large"));
        };
        length = next_length;
        let mut input = chunk.as_ref();
        while !input.is_empty() {
            let count = input.len().min(REPLAY_CHUNK_SIZE - pending.len());
            pending.extend_from_slice(&input[..count]);
            input = &input[count..];
            if pending.len() == REPLAY_CHUNK_SIZE {
                write_post_frame(
                    file,
                    &mut sequence,
                    std::mem::replace(&mut pending, Zeroizing::new(Vec::with_capacity(REPLAY_CHUNK_SIZE + TAG_SIZE))),
                )
                .await?;
            }
        }
    }
    if !pending.is_empty() {
        write_post_frame(file, &mut sequence, pending).await?;
    }
    write_post_frame(file, &mut sequence, Zeroizing::new(Vec::new())).await?;
    i64::try_from(length).map_err(|_| s3_error!(EntityTooLarge, "The object is too large"))
}

fn post_body_error(error: Box<dyn std::error::Error + Send + Sync>) -> S3Error {
    let classified: S3Error = ApiError::from(s3s_body_error_to_io(error)).into();
    if classified.code() == &S3ErrorCode::InternalError {
        s3_error!(MalformedPOSTRequest, "The POST file did not reach a valid end")
    } else {
        classified
    }
}

fn staging_io_error() -> S3Error {
    s3_error!(InternalError, "Unable to stage the POST file")
}

#[cfg(test)]
mod tests;
