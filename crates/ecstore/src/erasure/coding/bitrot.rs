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

use pin_project_lite::pin_project;
use rustfs_utils::HashAlgorithm;
use std::future::poll_fn;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::error;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_ERASURE: &str = "erasure";
const EVENT_BITROT_SHORT_SHARD_READ: &str = "bitrot_short_shard_read";
const EVENT_BITROT_HASH_MISMATCH: &str = "bitrot_hash_mismatch";
const MAX_RETAINED_CHUNKS_PER_BLOCK: usize = 64;
const MAX_CHUNK_POLLS_PER_YIELD: usize = MAX_RETAINED_CHUNKS_PER_BLOCK + 1;

/// Result of polling an optional owned-chunk handoff.
pub enum ShardChunkRead {
    /// The source does not support owned-chunk handoff and remains untouched.
    Unsupported,
    /// The source reached EOF.
    Eof,
    /// A non-empty chunk containing at most the requested number of bytes.
    Chunk(bytes::Bytes),
}

/// A shard source that may already hold its bytes in memory.
///
/// The GET path reads a shard out of the page cache into a `Bytes` and then, in
/// the old code, copied it twice more: once out of the `Cursor` wrapping it into
/// the reader's scratch buffer, and once from there into the caller's buffer.
/// `try_take_block` lets an in-memory source hand over the `[hash][data]` block
/// as a slice instead, collapsing that to a single copy (rustfs/backlog#1159:
/// `Cursor::poll_read` was 8.23% of GET CPU).
///
/// The default says "not in memory", so a streaming source keeps the old path and
/// its short-read/EOF semantics unchanged.
pub trait ShardSource: AsyncRead + Send + Sync + Unpin {
    /// The next `n` bytes, consumed from the source, or `None` when the source is
    /// not in memory or holds fewer than `n` bytes left. Advancing must match what
    /// an `AsyncRead` of `n` bytes would have done, so the two can be mixed.
    fn try_take_block(&mut self, _n: usize) -> Option<bytes::Bytes> {
        None
    }

    /// Polls one owned chunk when the source supports chunk handoff.
    /// `Unsupported` must leave the source untouched.
    fn poll_read_chunk(self: Pin<&mut Self>, _cx: &mut Context<'_>, _max: usize) -> Poll<std::io::Result<ShardChunkRead>> {
        Poll::Ready(Ok(ShardChunkRead::Unsupported))
    }
}

/// Borrowed and owned byte slices are ordinary streaming sources: they carry no
/// `Bytes` to hand out, so they take the default and keep the old copy path.
impl ShardSource for std::io::Cursor<Vec<u8>> {}

impl ShardSource for std::io::Cursor<&[u8]> {}

impl ShardSource for Box<dyn AsyncRead + Send + Sync + Unpin> {}

impl ShardSource for std::io::Cursor<bytes::Bytes> {
    fn try_take_block(&mut self, n: usize) -> Option<bytes::Bytes> {
        let pos = usize::try_from(self.position()).ok()?;
        let end = pos.checked_add(n)?;
        if end > self.get_ref().len() {
            return None;
        }
        self.set_position(end as u64);
        Some(self.get_ref().slice(pos..end))
    }
}

pin_project! {
    /// BitrotReader reads (hash+data) blocks from an async reader and verifies hash integrity.
    pub struct BitrotReader<R> {
        #[pin]
        inner: R,
        hash_algo: HashAlgorithm,
        shard_size: usize,
        // Scratch buffer reused across reads. On the hashed path it holds the
        // contiguous on-disk `[hash][data]` block so both are pulled in a single
        // pass; grown lazily and never shrunk.
        buf: Vec<u8>,
        // Reused owned chunk vector for the remote HTTP fast path. Keeping the
        // allocation with the reader avoids allocating once per bitrot block.
        chunks: Vec<bytes::Bytes>,
        skip_verify: bool,
        last_verify_duration: Duration,
    }
}

impl<R> BitrotReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    /// Create a new BitrotReader.
    pub fn new(inner: R, shard_size: usize, algo: HashAlgorithm, skip_verify: bool) -> Self {
        Self {
            inner,
            hash_algo: algo,
            shard_size,
            buf: Vec::new(),
            chunks: Vec::new(),
            skip_verify,
            last_verify_duration: Duration::ZERO,
        }
    }

    pub(crate) fn last_verify_duration(&self) -> Duration {
        self.last_verify_duration
    }

    #[cfg(test)]
    pub(crate) fn inner_ref(&self) -> &R {
        &self.inner
    }

    /// Read a single (hash+data) block, verify hash, and copy `out.len()` bytes
    /// into `out`. Returns an error if the shard is short, the hash mismatches,
    /// or `out` is larger than one shard. On error `out`'s contents are
    /// unspecified but never contain bytes that failed the hash check — the copy
    /// happens only after verification.
    #[hotpath::measure(impl_type = "BitrotReader")]
    pub async fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let want = out.len();
        self.begin_read(want)?;

        // No-hash path: pull the shard straight into the caller's buffer with no
        // intermediate copy. There is no leading hash to co-locate, so a combined
        // buffer would only add a memcpy.
        if self.hash_algo.size() == 0 {
            let data_len = fill(&mut self.inner, out).await?;
            return self.finish_len(data_len, want);
        }

        let need = self.hash_algo.size() + want;
        self.read_scratch_block(need, want).await?;
        let (data, verify) = split_and_verify(&self.hash_algo, self.skip_verify, &self.buf[..need])?;
        out.copy_from_slice(data);
        self.last_verify_duration = verify;
        Ok(want)
    }

    /// Shared preamble for [`Self::read`]/[`Self::read_appending`]: reset the
    /// verify timer and reject a request larger than one shard.
    fn begin_read(&mut self, want: usize) -> std::io::Result<()> {
        self.last_verify_duration = Duration::ZERO;
        if want > self.shard_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("data size {want} exceeds shard size {}", self.shard_size),
            ));
        }
        Ok(())
    }

    /// Read a full `[hash][data]` block of `need` bytes into the scratch buffer
    /// in a single pass, returning it as a slice. On the streaming disk reader
    /// (a raw tokio File whose every `read` is a spawn_blocking round-trip) the
    /// single pass halves the per-block dispatch count; on an in-memory source
    /// it is a plain fill.
    ///
    /// A short read (EOF before the whole block is in hand) is a truncated shard,
    /// returned as `UnexpectedEof` so the caller drops this reader and parity
    /// reconstruction engages, rather than silently shifting every downstream
    /// byte (backlog#799 B2). This fires independent of the hash check, so it
    /// also catches truncation under `skip_verify`.
    ///
    /// On `Ok` the block is `self.buf[..need]`; the caller re-borrows it so the
    /// verification can also borrow `self` immutably.
    async fn read_scratch_block(&mut self, need: usize, want: usize) -> std::io::Result<()> {
        if self.buf.len() < need {
            self.buf.resize(need, 0);
        }
        let filled = fill(&mut self.inner, &mut self.buf[..need]).await?;
        if filled < need {
            return Err(short_shard_read(filled.saturating_sub(self.hash_algo.size()), want));
        }
        Ok(())
    }

    /// Map a completed no-hash read to the shared short-shard contract: a full
    /// buffer returns its length, a short read is UnexpectedEof (backlog#799 B2).
    fn finish_len(&self, data_len: usize, want: usize) -> std::io::Result<usize> {
        if data_len < want {
            return Err(short_shard_read(data_len, want));
        }
        Ok(data_len)
    }
}

/// A truncated shard is `UnexpectedEof`, not a short success (backlog#799 B2).
fn short_shard_read(got: usize, want: usize) -> std::io::Error {
    error!(
        event = EVENT_BITROT_SHORT_SHARD_READ,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_ERASURE,
        state = "failed",
        got,
        want,
        "short shard read: got {got} of {want} bytes"
    );
    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, format!("short shard read: got {got} of {want} bytes"))
}

/// Split a `[hash][data]` block, verify the hash (unless `skip_verify`), and
/// return the data slice plus the time spent hashing. The caller writes `data`
/// into its sink **only after** this returns `Ok`, so a shard that fails the
/// hash never reaches the caller's buffer. The verify duration is returned
/// rather than stored so this stays a free function usable while `self` is
/// borrowed for the block.
fn split_and_verify<'a>(hash_algo: &HashAlgorithm, skip_verify: bool, block: &'a [u8]) -> std::io::Result<(&'a [u8], Duration)> {
    let (hash, data) = block.split_at(hash_algo.size());
    if skip_verify {
        return Ok((data, Duration::ZERO));
    }
    let verify_start = std::time::Instant::now();
    let actual_hash = hash_algo.hash_encode(data);
    let verify = verify_start.elapsed();
    if actual_hash.as_ref() != hash {
        error!(
            event = EVENT_BITROT_HASH_MISMATCH,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_ERASURE,
            state = "failed",
            data_len = data.len(),
            "bitrot hash mismatch"
        );
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bitrot hash mismatch"));
    }
    Ok((data, verify))
}

impl<R> BitrotReader<R>
where
    R: ShardSource,
{
    /// Same contract as [`Self::read`], but **appends** `want` bytes into `out`'s
    /// spare capacity instead of demanding an initialized `&mut [u8]`
    /// (rustfs/backlog#1159).
    ///
    /// `read` forced its caller to hand over a zeroed buffer purely to satisfy
    /// `&mut [u8]`, and every one of those bytes was then overwritten. Because a
    /// short read is an error here (never a partially filled buffer), `out` ends
    /// up holding exactly the bytes this reader produced, so nothing the reader
    /// did not write is ever observable — the zeroing bought nothing.
    ///
    /// On return `out.len()` has grown by exactly the returned count.
    pub async fn read_appending(&mut self, out: &mut Vec<u8>, want: usize) -> std::io::Result<usize> {
        use bytes::BufMut as _;
        use tokio::io::AsyncReadExt as _;

        self.begin_read(want)?;
        out.reserve(want);
        let hash_size = self.hash_algo.size();

        // No-hash path: read straight into `out`'s spare capacity. `read_buf`
        // advances the length only over bytes the reader actually wrote, so an
        // uninitialized tail can never be exposed.
        if hash_size == 0 {
            let start = out.len();
            while out.len() - start < want {
                let remaining = want - (out.len() - start);
                let n = self
                    .inner
                    .read_buf(&mut (&mut *out).limit(remaining))
                    .await
                    .inspect_err(|e| error!("bitrot reader read error: {e}"))?;
                if n == 0 {
                    break;
                }
            }
            return self.finish_len(out.len() - start, want);
        }

        let need = hash_size + want;

        if let Some(block) = self.inner.try_take_block(need) {
            let (data, verify) = split_and_verify(&self.hash_algo, self.skip_verify, &block)?;
            out.extend_from_slice(data);
            self.last_verify_duration = verify;
            return Ok(want);
        }

        self.chunks.clear();
        let handed_off = {
            let inner = &mut self.inner;
            let chunks = &mut self.chunks;
            let tail_buf = &mut self.buf;
            let mut received = 0usize;
            poll_fn(|cx| {
                for _ in 0..MAX_CHUNK_POLLS_PER_YIELD {
                    let next = match Pin::new(&mut *inner).poll_read_chunk(cx, need - received) {
                        Poll::Ready(Ok(next)) => next,
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => return Poll::Pending,
                    };
                    let chunk = match next {
                        ShardChunkRead::Unsupported if received == 0 => return Poll::Ready(Ok(false)),
                        ShardChunkRead::Unsupported => {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "chunk handoff became unavailable after transferring data",
                            )));
                        }
                        ShardChunkRead::Eof => {
                            return Poll::Ready(Err(short_shard_read(received.saturating_sub(hash_size), want)));
                        }
                        ShardChunkRead::Chunk(chunk) => chunk,
                    };

                    if received == 0 {
                        tail_buf.clear();
                    }
                    if chunk.is_empty() {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "chunk handoff returned an empty chunk",
                        )));
                    }
                    let remaining = need - received;
                    if chunk.len() > remaining {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "chunk handoff exceeded its requested boundary",
                        )));
                    }
                    received += chunk.len();

                    if chunks.len() == MAX_RETAINED_CHUNKS_PER_BLOCK {
                        if tail_buf.is_empty() {
                            tail_buf.reserve_exact(need - (received - chunk.len()));
                        }
                        tail_buf.extend_from_slice(&chunk);
                    } else {
                        chunks.push(chunk);
                    }

                    if received == need {
                        return Poll::Ready(Ok(true));
                    }
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            })
            .await?
        };
        if handed_off {
            if self.chunks.len() == 1 && self.buf.is_empty() {
                let block = &self.chunks[0];
                let (data, verify) = split_and_verify(&self.hash_algo, self.skip_verify, block)?;
                out.extend_from_slice(data);
                self.last_verify_duration = verify;
                return Ok(want);
            }

            let block_chunks = || {
                self.chunks
                    .iter()
                    .map(|chunk| chunk.as_ref())
                    .chain((!self.buf.is_empty()).then_some(self.buf.as_slice()))
            };
            if !self.skip_verify {
                let verify_start = std::time::Instant::now();
                let actual_hash = self
                    .hash_algo
                    .hash_encode_slices(block_chunks().scan(hash_size, |skip, chunk| {
                        let start = (*skip).min(chunk.len());
                        *skip -= start;
                        Some(&chunk[start..])
                    }));
                let verify = verify_start.elapsed();
                let mut hash_offset = 0;
                let mut remaining = hash_size;
                for chunk in block_chunks() {
                    let take = remaining.min(chunk.len());
                    if actual_hash.as_ref()[hash_offset..hash_offset + take] != chunk[..take] {
                        error!(
                            event = EVENT_BITROT_HASH_MISMATCH,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_ERASURE,
                            state = "failed",
                            data_len = want,
                            "bitrot hash mismatch"
                        );
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bitrot hash mismatch"));
                    }
                    hash_offset += take;
                    remaining -= take;
                    if remaining == 0 {
                        break;
                    }
                }
                self.last_verify_duration = verify;
            }
            let mut skip = hash_size;
            for chunk in block_chunks() {
                let start = skip.min(chunk.len());
                skip -= start;
                out.extend_from_slice(&chunk[start..]);
            }
            return Ok(want);
        }

        // Streaming path: same single pass and same verification as `read`; only
        // the sink differs (`extend_from_slice` into `out` instead of
        // `copy_from_slice` into a pre-zeroed buffer).
        self.read_scratch_block(need, want).await?;
        let (data, verify) = split_and_verify(&self.hash_algo, self.skip_verify, &self.buf[..need])?;
        out.extend_from_slice(data);
        self.last_verify_duration = verify;
        Ok(want)
    }
}

pin_project! {
    /// BitrotWriter writes (hash+data) blocks to an async writer.
    pub struct BitrotWriter<W> {
        #[pin]
        inner: W,
        hash_algo: HashAlgorithm,
        shard_size: usize,
        finished: bool,
    }
}

impl<W> BitrotWriter<W>
where
    W: AsyncWrite + Unpin + Send + Sync,
{
    /// Create a new BitrotWriter.
    pub fn new(inner: W, shard_size: usize, algo: HashAlgorithm) -> Self {
        let hash_algo = algo;
        Self {
            inner,
            hash_algo,
            shard_size,
            finished: false,
        }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Write a (hash+data) block. Returns the number of data bytes written.
    /// Returns an error if called after a short write or if data exceeds shard_size.
    #[hotpath::measure(label = "BitrotWriter::write", impl_type = "BitrotWriter")]
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.finished {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "bitrot writer already finished"));
        }

        if buf.len() > self.shard_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("data size {} exceeds shard size {}", buf.len(), self.shard_size),
            ));
        }

        if buf.len() < self.shard_size {
            self.finished = true;
        }

        let hash_algo = &self.hash_algo;

        // Interleaved per-block bitrot: prepend the block's hash so the on-disk
        // block is `[hash][data]`. This `size() > 0` condition is broader than
        // the streaming-only condition in `bitrot_shard_file_size`, so it is
        // only self-consistent for the two streaming Highway variants
        // (`HighwayHash256S` / `HighwayHash256SLegacy`) — the only algorithms
        // production ever uses here (backlog#959 / ECA-18). For non-streaming
        // algorithms MinIO uses whole-file bitrot with no interleaved hash, so
        // driving one through this writer would produce a file whose length
        // disagrees with `bitrot_shard_file_size` and fail `bitrot_verify`; do
        // not feed a non-streaming algorithm here without a separate path.
        if hash_algo.size() > 0 {
            let hash = hash_algo.hash_encode(buf);
            if hash.as_ref().is_empty() {
                error!("bitrot writer write hash error: hash is empty");
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "hash is empty"));
            }
            write_all_vectored(&mut self.inner, hash.as_ref(), buf).await?;
        } else {
            self.inner.write_all(buf).await?;
        }

        let n = buf.len();

        Ok(n)
    }

    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        self.inner.flush().await?;
        self.inner.shutdown().await
    }
}

/// Read into `buf` until it is full or the reader hits EOF, returning the number
/// of bytes actually read. A raw tokio File typically satisfies this in one
/// `read` (one spawn_blocking round-trip); the loop only re-enters on a genuine
/// partial read. Callers treat `filled < buf.len()` as a truncated shard.
async fn fill<R>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..]).await.map_err(|e| {
            error!("bitrot reader read error: {}", e);
            e
        })?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

async fn write_all_vectored<W>(writer: &mut W, hash: &[u8], data: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut hash_offset = 0;
    let mut data_offset = 0;

    while hash_offset < hash.len() || data_offset < data.len() {
        let slices = [IoSlice::new(&hash[hash_offset..]), IoSlice::new(&data[data_offset..])];
        let written = writer.write_vectored(&slices).await?;
        if written == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "failed to write hash and data"));
        }

        let hash_remaining = hash.len() - hash_offset;
        if written < hash_remaining {
            hash_offset += written;
            continue;
        }

        hash_offset = hash.len();
        data_offset += written - hash_remaining;
    }

    Ok(())
}

/// On-disk size of a shard file for a part of `size` data bytes.
///
/// This is a byte-for-byte port of MinIO's `bitrotShardFileSize` and encodes
/// MinIO's per-algorithm bitrot layout, NOT a uniform "one hash per block" rule:
///
/// - `HighwayHash256S` / `HighwayHash256SLegacy` are the *streaming* variants.
///   They use interleaved per-block bitrot: every `shard_size` block on disk is
///   `[hash][data]`, so the file carries `ceil(size / shard_size)` extra hashes.
/// - Every other algorithm (`SHA256`, `HighwayHash256`, `BLAKE2b512`, `Md5`,
///   `None`) maps to MinIO's *whole-file* bitrot for legacy V1 objects: the hash
///   lives in xl.meta, not interleaved on disk, so the on-disk file is exactly
///   `size` bytes. Returning the bare `size` here is therefore correct, not a
///   bug — adding per-block hash bytes would make this guard reject genuine
///   legacy whole-file-bitrot parts and break MinIO interop.
///
/// INVARIANT (backlog#959 / ECA-18): this crate only ever writes and verifies
/// the *streaming* per-block layout. `BitrotWriter::write` interleaves a hash on
/// any `hash_algo.size() > 0`, and `bitrot_verify`'s read loop assumes an
/// interleaved hash per block; both are only consistent with THIS function for
/// the two streaming Highway variants. That is safe because every production
/// write path hardcodes `HighwayHash256S` and `ErasureInfo::get_checksum_info`
/// defaults to `HighwayHash256S` (see the regression tests below and in
/// rustfs-filemeta). The non-streaming branches of this function exist purely to
/// preserve the MinIO formula's whole-file semantics; feeding a non-streaming
/// algorithm through `BitrotWriter` + `bitrot_verify` is unsupported and would
/// mismatch this size — do not wire one in without a dedicated whole-file path.
pub fn bitrot_shard_file_size(size: usize, shard_size: usize, algo: HashAlgorithm) -> usize {
    if algo != HashAlgorithm::HighwayHash256S && algo != HashAlgorithm::HighwayHash256SLegacy {
        // Non-streaming (whole-file bitrot) algorithms carry no interleaved
        // per-block hashes on disk; the on-disk file is exactly `size` bytes.
        return size;
    }
    // Streaming Highway variants: one hash is interleaved before every block.
    size.div_ceil(shard_size) * algo.size() + size
}

/// Verify an interleaved per-block bitrot shard file and consume the reader
/// through EOF. Bytes beyond the encoded length are corruption, even when every
/// expected block has a valid hash.
///
/// The read loop below assumes every block on disk is `[hash][data]` (streaming
/// bitrot). It is therefore only valid for the streaming Highway variants, whose
/// on-disk length matches `bitrot_shard_file_size` — production always uses
/// `HighwayHash256S` (backlog#959 / ECA-18). Passing a non-streaming algorithm
/// (`SHA256` / `HighwayHash256` / `BLAKE2b512` / `Md5`) is unsupported: MinIO
/// stores those as whole-file bitrot with no interleaved hash, so the size guard
/// on the next line would reject a genuinely healthy part. Reading legacy V1
/// whole-file-bitrot objects would need a separate verification path.
#[hotpath::measure]
pub async fn bitrot_verify<R: AsyncRead + Unpin + Send>(
    mut r: R,
    want_size: usize,
    part_size: usize,
    algo: HashAlgorithm,
    mut shard_size: usize,
) -> std::io::Result<()> {
    let mut hash_buf = vec![0; algo.size()];
    let mut left = want_size;

    if left != bitrot_shard_file_size(part_size, shard_size, algo.clone()) {
        return Err(std::io::Error::other("bitrot shard file size mismatch"));
    }

    while left > 0 {
        let n = r.read_exact(&mut hash_buf).await?;
        left -= n;

        if left < shard_size {
            shard_size = left;
        }

        let mut buf = vec![0; shard_size];
        let read = r.read_exact(&mut buf).await?;

        let actual_hash = algo.hash_encode(&buf);
        if actual_hash.as_ref() != &hash_buf[0..n] {
            return Err(std::io::Error::other("bitrot hash mismatch"));
        }

        left -= read;
    }

    let mut trailing = [0u8; 1];
    if r.read(&mut trailing).await? != 0 {
        return Err(std::io::Error::other("bitrot shard file has trailing data"));
    }

    Ok(())
}

/// Custom writer enum that supports inline buffer storage
pub enum CustomWriter {
    /// Inline buffer writer - stores data in memory
    InlineBuffer(Vec<u8>),
    /// Disk-based writer using tokio file
    Other(Box<dyn AsyncWrite + Unpin + Send + Sync>),
}

impl CustomWriter {
    /// Create a new inline buffer writer
    pub fn new_inline_buffer() -> Self {
        Self::InlineBuffer(Vec::new())
    }

    /// Create a new disk writer from any AsyncWrite implementation
    pub fn new_tokio_writer<W>(writer: W) -> Self
    where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
    {
        Self::Other(Box::new(writer))
    }

    /// Get the inline buffer data if this is an inline buffer writer
    pub fn get_inline_data(&self) -> Option<&[u8]> {
        match self {
            Self::InlineBuffer(data) => Some(data),
            Self::Other(_) => None,
        }
    }

    /// Extract the inline buffer data, consuming the writer
    pub fn into_inline_data(self) -> Option<Vec<u8>> {
        match self {
            Self::InlineBuffer(data) => Some(data),
            Self::Other(_) => None,
        }
    }
}

impl AsyncWrite for CustomWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::InlineBuffer(data) => {
                data.extend_from_slice(buf);
                std::task::Poll::Ready(Ok(buf.len()))
            }
            Self::Other(writer) => {
                let pinned_writer = std::pin::Pin::new(writer.as_mut());
                pinned_writer.poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::InlineBuffer(_) => std::task::Poll::Ready(Ok(())),
            Self::Other(writer) => {
                let pinned_writer = std::pin::Pin::new(writer.as_mut());
                pinned_writer.poll_flush(cx)
            }
        }
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::InlineBuffer(_) => std::task::Poll::Ready(Ok(())),
            Self::Other(writer) => {
                let pinned_writer = std::pin::Pin::new(writer.as_mut());
                pinned_writer.poll_shutdown(cx)
            }
        }
    }

    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::InlineBuffer(data) => {
                let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
                for buf in bufs {
                    data.extend_from_slice(buf);
                }
                std::task::Poll::Ready(Ok(total))
            }
            Self::Other(writer) => {
                let pinned_writer = std::pin::Pin::new(writer.as_mut());
                pinned_writer.poll_write_vectored(cx, bufs)
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::InlineBuffer(_) => true,
            Self::Other(writer) => writer.is_write_vectored(),
        }
    }
}

/// Wrapper around BitrotWriter that uses our custom writer
pub struct BitrotWriterWrapper {
    bitrot_writer: BitrotWriter<CustomWriter>,
    writer_type: WriterType,
}

/// Enum to track the type of writer we're using
enum WriterType {
    InlineBuffer,
    Other,
}

impl std::fmt::Debug for BitrotWriterWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitrotWriterWrapper")
            .field(
                "writer_type",
                &match self.writer_type {
                    WriterType::InlineBuffer => "InlineBuffer",
                    WriterType::Other => "Other",
                },
            )
            .finish()
    }
}

impl BitrotWriterWrapper {
    /// Create a new BitrotWriterWrapper with custom writer
    pub fn new(writer: CustomWriter, shard_size: usize, checksum_algo: HashAlgorithm) -> Self {
        let writer_type = match &writer {
            CustomWriter::InlineBuffer(_) => WriterType::InlineBuffer,
            CustomWriter::Other(_) => WriterType::Other,
        };

        Self {
            bitrot_writer: BitrotWriter::new(writer, shard_size, checksum_algo),
            writer_type,
        }
    }

    /// Write data to the bitrot writer
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bitrot_writer.write(buf).await
    }

    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        self.bitrot_writer.shutdown().await
    }

    /// Extract the inline buffer data, consuming the wrapper
    pub fn into_inline_data(self) -> Option<Vec<u8>> {
        match self.writer_type {
            WriterType::InlineBuffer => {
                let writer = self.bitrot_writer.into_inner();
                writer.into_inline_data()
            }
            WriterType::Other => None,
        }
    }
}

// --- startup bitrot self-test (rustfs/backlog#1873, MinIO bitrotSelfTest parity) ---
//
// A broken hash implementation (bad SIMD feature combination, platform drift, a
// key-handling regression) fails silently: every shard reads back "corrupt",
// heal rewrites data that was fine, and cross-platform clusters disagree about
// which copy is healthy. The self-test below pins the algorithms the moment a
// process starts, so a drifted build announces itself instead of quietly
// rewriting objects. See docs/rustfs-heal-scanner-vs-minio-comprehensive-
// analysis-2026-08-16.md §6 HS-11.

/// Length of the deterministic self-test payload.
pub const BITROT_SELF_TEST_PAYLOAD_LEN: usize = 4096;

/// Known-answer digest of [`bitrot_self_test_payload`] under `HighwayHash256S`
/// (the production default). Pinned so any platform or build where the
/// implementation drifts fails startup instead of miss-hashing shards.
const BITROT_SELF_TEST_KAT_HIGHWAY_HASH256S: [u8; 32] = [
    0xb9, 0x32, 0xa2, 0xaa, 0x4a, 0xb7, 0x33, 0x6a, 0xa3, 0xca, 0x7e, 0x61, 0x9d, 0x86, 0x52, 0x14, 0x6e, 0x7f, 0xd8, 0x9e, 0xea,
    0x08, 0xd9, 0x8c, 0x33, 0x85, 0x87, 0x19, 0x30, 0xd6, 0xed, 0x06,
];

/// Known-answer digest of the same payload under `HighwayHash256SLegacy`.
const BITROT_SELF_TEST_KAT_HIGHWAY_HASH256S_LEGACY: [u8; 32] = [
    0x98, 0x24, 0x71, 0x4f, 0x16, 0xbb, 0x48, 0x39, 0xed, 0x68, 0xfa, 0x63, 0x5e, 0xd9, 0x07, 0x61, 0xdf, 0x0a, 0xff, 0xcf, 0x7d,
    0x8c, 0xa8, 0xc7, 0xc0, 0xb6, 0x6f, 0x05, 0xdb, 0xda, 0x5a, 0x22,
];

/// FIPS 180-2 test vector: SHA-256 of the ASCII string "abc". Unlike the
/// Highway digests above this one is externally verifiable, so it guards the
/// whole `HashAlgorithm` plumbing even for readers who distrust pinned
/// self-computed constants.
const BITROT_SELF_TEST_KAT_SHA256_ABC: [u8; 32] = [
    0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96,
    0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
];

/// Deterministic self-test payload: xorshift64* from a fixed seed, so every
/// platform and every run hashes the same 4096 bytes.
fn bitrot_self_test_payload() -> [u8; BITROT_SELF_TEST_PAYLOAD_LEN] {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut payload = [0u8; BITROT_SELF_TEST_PAYLOAD_LEN];
    for byte in payload.iter_mut() {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        *byte = state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8;
    }
    payload
}

/// Why a bitrot self-test failed.
#[derive(Debug)]
pub enum BitrotSelfTestError {
    /// A known-answer digest mismatched the pinned constant.
    KnownAnswerMismatch {
        algorithm: &'static str,
        got: String,
        want: String,
    },
    /// A freshly encoded shard failed `bitrot_verify`.
    RoundtripVerify { algorithm: &'static str, detail: String },
    /// A verified roundtrip read back different bytes than were written.
    RoundtripReadback { algorithm: &'static str },
    /// A deliberately tampered shard was not rejected by `bitrot_verify`.
    TamperNotRejected {
        algorithm: &'static str,
        tampered: &'static str,
    },
}

impl std::fmt::Display for BitrotSelfTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KnownAnswerMismatch { algorithm, got, want } => {
                write!(f, "known-answer mismatch for {algorithm}: got {got}, want {want}")
            }
            Self::RoundtripVerify { algorithm, detail } => write!(f, "{algorithm} roundtrip shard failed verification: {detail}"),
            Self::RoundtripReadback { algorithm } => write!(f, "{algorithm} roundtrip read back different bytes"),
            Self::TamperNotRejected { algorithm, tampered } => {
                write!(f, "{algorithm} tampered shard ({tampered}) was not rejected")
            }
        }
    }
}

impl std::error::Error for BitrotSelfTestError {}

fn self_test_hex(bytes: &[u8]) -> String {
    rustfs_utils::hex(bytes)
}

// (kept as a named one-liner so every KAT failure site reads the same; the
// underlying formatter is the shared `rustfs_utils::hex`)

/// Compare a digest against its pinned constant. Split out so a test can drive
/// it with a wrong constant and prove the mismatch path fires.
fn bitrot_kat_check(
    algorithm: &'static str,
    algo: &HashAlgorithm,
    payload: &[u8],
    expected: &[u8; 32],
) -> Result<(), BitrotSelfTestError> {
    let digest = algo.hash_encode(payload);
    let digest = digest.as_ref();
    if digest.len() != expected.len() || digest != expected.as_slice() {
        return Err(BitrotSelfTestError::KnownAnswerMismatch {
            algorithm,
            got: self_test_hex(digest),
            want: self_test_hex(expected),
        });
    }
    Ok(())
}

/// Encode `payload` with `shard_size` blocks, verify it end to end, and read
/// every block back through `BitrotReader` comparing bytes.
async fn bitrot_roundtrip_check(
    algorithm: &'static str,
    algo: HashAlgorithm,
    payload: &[u8],
    shard_size: usize,
) -> Result<(), BitrotSelfTestError> {
    let mut writer = BitrotWriter::new(std::io::Cursor::new(Vec::<u8>::new()), shard_size, algo.clone());
    for chunk in payload.chunks(shard_size) {
        writer
            .write(chunk)
            .await
            .map_err(|err| BitrotSelfTestError::RoundtripVerify {
                algorithm,
                detail: format!("encode failed: {err}"),
            })?;
    }
    let encoded = writer.into_inner().into_inner();

    let on_disk = bitrot_shard_file_size(payload.len(), shard_size, algo.clone());
    if encoded.len() != on_disk {
        return Err(BitrotSelfTestError::RoundtripVerify {
            algorithm,
            detail: format!("encoded {} bytes, size formula says {on_disk}", encoded.len()),
        });
    }
    bitrot_verify(std::io::Cursor::new(encoded.clone()), on_disk, payload.len(), algo.clone(), shard_size)
        .await
        .map_err(|err| BitrotSelfTestError::RoundtripVerify {
            algorithm,
            detail: err.to_string(),
        })?;

    let mut reader = BitrotReader::new(std::io::Cursor::new(encoded), shard_size, algo, false);
    let mut offset = 0usize;
    while offset < payload.len() {
        let want = shard_size.min(payload.len() - offset);
        let mut buf = vec![0u8; want];
        let read = reader
            .read(&mut buf)
            .await
            .map_err(|err| BitrotSelfTestError::RoundtripVerify {
                algorithm,
                detail: format!("read back failed at offset {offset}: {err}"),
            })?;
        if read != want || buf[..read] != payload[offset..offset + read] {
            return Err(BitrotSelfTestError::RoundtripReadback { algorithm });
        }
        offset += read;
    }
    Ok(())
}

/// Flip one byte and require `bitrot_verify` to reject the result.
async fn bitrot_tamper_check(
    algorithm: &'static str,
    algo: HashAlgorithm,
    payload: &[u8],
    shard_size: usize,
    tampered: &'static str,
    flip_at: usize,
) -> Result<(), BitrotSelfTestError> {
    let mut writer = BitrotWriter::new(std::io::Cursor::new(Vec::<u8>::new()), shard_size, algo.clone());
    for chunk in payload.chunks(shard_size) {
        writer.write(chunk).await.expect("self-test encode should not fail");
    }
    let mut corrupt = writer.into_inner().into_inner();
    let flip_index = flip_at % corrupt.len();
    corrupt[flip_index] ^= 0x80;

    let on_disk = bitrot_shard_file_size(payload.len(), shard_size, algo.clone());
    match bitrot_verify(std::io::Cursor::new(corrupt), on_disk, payload.len(), algo, shard_size).await {
        // The flipped byte must be rejected as a hash mismatch specifically, not
        // by any incidental read error: an in-memory cursor cannot fail reads,
        // so accepting any other failure here would mask a verify path that
        // errors out before it ever compares hashes.
        Err(err) if err.to_string().contains("hash mismatch") => Ok(()),
        Ok(()) => Err(BitrotSelfTestError::TamperNotRejected { algorithm, tampered }),
        Err(err) => Err(BitrotSelfTestError::RoundtripVerify {
            algorithm,
            detail: format!("tampered shard rejected with an unexpected error: {err}"),
        }),
    }
}

/// Verify every bitrot algorithm this crate can write or verify in production:
/// both streaming Highway variants roundtrip end to end (encode → size formula
/// → `bitrot_verify` → read back) and reject a flipped byte in both the data
/// and the leading hash, while all three hashed algorithms reproduce their
/// pinned known-answer digests.
///
/// Runs in well under a millisecond on 4 KiB of data; callers may run it inline
/// at startup. Pure CPU, no allocation beyond a few KiB of scratch.
pub async fn bitrot_self_test() -> Result<(), BitrotSelfTestError> {
    let payload = bitrot_self_test_payload();

    // Externally verifiable vector first: it guards the HashAlgorithm plumbing
    // itself, before any self-pinned constants are consulted.
    let abc = HashAlgorithm::SHA256.hash_encode(b"abc");
    if abc.as_ref() != BITROT_SELF_TEST_KAT_SHA256_ABC.as_slice() {
        return Err(BitrotSelfTestError::KnownAnswerMismatch {
            algorithm: "SHA256",
            got: self_test_hex(abc.as_ref()),
            want: self_test_hex(&BITROT_SELF_TEST_KAT_SHA256_ABC),
        });
    }

    bitrot_kat_check(
        "HighwayHash256S",
        &HashAlgorithm::HighwayHash256S,
        &payload,
        &BITROT_SELF_TEST_KAT_HIGHWAY_HASH256S,
    )?;
    bitrot_kat_check(
        "HighwayHash256SLegacy",
        &HashAlgorithm::HighwayHash256SLegacy,
        &payload,
        &BITROT_SELF_TEST_KAT_HIGHWAY_HASH256S_LEGACY,
    )?;

    for (algorithm, algo) in [
        ("HighwayHash256S", HashAlgorithm::HighwayHash256S),
        ("HighwayHash256SLegacy", HashAlgorithm::HighwayHash256SLegacy),
    ] {
        // Full blocks plus a partial tail, exactly like a real part stripe.
        let tail_len = 2 * 1024 + 333;
        bitrot_roundtrip_check(algorithm, algo.clone(), &payload, 1024).await?;
        bitrot_roundtrip_check(algorithm, algo.clone(), &payload[..tail_len], 1024).await?;
        // One flipped byte in the final data block, one in the first leading
        // hash: both must fail verification.
        bitrot_tamper_check(algorithm, algo.clone(), &payload, 1024, "final data byte", payload.len() - 1).await?;
        bitrot_tamper_check(algorithm, algo, &payload, 1024, "leading hash byte", 0).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BitrotReader, BitrotWriter, BitrotWriterWrapper, CustomWriter, bitrot_kat_check, bitrot_self_test,
        bitrot_self_test_payload, bitrot_shard_file_size, bitrot_verify, write_all_vectored,
    };
    use super::{MAX_RETAINED_CHUNKS_PER_BLOCK, ShardChunkRead, ShardSource};
    use bytes::Bytes;
    use rustfs_utils::HashAlgorithm;
    use std::collections::VecDeque;
    use std::io::{self, Cursor, IoSlice};
    use std::pin::Pin;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

    struct FragmentedSource {
        chunks: VecDeque<Bytes>,
    }

    impl FragmentedSource {
        fn new(bytes: Vec<u8>, fragment_sizes: &[usize]) -> Self {
            let mut chunks = VecDeque::new();
            let mut offset = 0;
            for &size in fragment_sizes {
                let end = (offset + size).min(bytes.len());
                if offset < end {
                    chunks.push_back(Bytes::copy_from_slice(&bytes[offset..end]));
                }
                offset = end;
            }
            if offset < bytes.len() {
                chunks.push_back(Bytes::copy_from_slice(&bytes[offset..]));
            }
            Self { chunks }
        }
    }

    impl AsyncRead for FragmentedSource {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("fragmented source must use chunk handoff")))
        }
    }

    impl ShardSource for FragmentedSource {
        fn poll_read_chunk(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, max: usize) -> Poll<io::Result<ShardChunkRead>> {
            let Some(mut chunk) = self.chunks.pop_front() else {
                return Poll::Ready(Ok(ShardChunkRead::Eof));
            };
            if chunk.len() > max {
                self.chunks.push_front(chunk.split_off(max));
                chunk.truncate(max);
            }
            Poll::Ready(Ok(ShardChunkRead::Chunk(chunk)))
        }
    }

    struct GeneratedChunkSource {
        bytes: Bytes,
        offset: usize,
        fragment_size: usize,
        fail_at: Option<usize>,
    }

    impl GeneratedChunkSource {
        fn new(bytes: Vec<u8>, fragment_size: usize) -> Self {
            assert!(fragment_size > 0);
            Self {
                bytes: Bytes::from(bytes),
                offset: 0,
                fragment_size,
                fail_at: None,
            }
        }

        fn failing(bytes: Vec<u8>, fragment_size: usize, fail_at: usize) -> Self {
            Self {
                fail_at: Some(fail_at),
                ..Self::new(bytes, fragment_size)
            }
        }
    }

    impl AsyncRead for GeneratedChunkSource {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("generated source must use chunk handoff")))
        }
    }

    impl ShardSource for GeneratedChunkSource {
        fn poll_read_chunk(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, max: usize) -> Poll<io::Result<ShardChunkRead>> {
            if self.fail_at == Some(self.offset) {
                return Poll::Ready(Err(rustfs_rio::new_test_internode_http_io_error(
                    rustfs_rio::InternodeHttpErrorKind::BodyStreamAborted,
                )));
            }
            if self.offset == self.bytes.len() {
                return Poll::Ready(Ok(ShardChunkRead::Eof));
            }
            let error_limit = self.fail_at.unwrap_or(self.bytes.len());
            let take = self
                .fragment_size
                .min(max)
                .min(error_limit - self.offset)
                .min(self.bytes.len() - self.offset);
            let start = self.offset;
            self.offset += take;
            Poll::Ready(Ok(ShardChunkRead::Chunk(self.bytes.slice(start..start + take))))
        }
    }

    struct InvalidChunkSource {
        mode: InvalidChunkMode,
    }

    #[derive(Clone, Copy)]
    enum InvalidChunkMode {
        Empty,
        Oversized,
        UnsupportedAfterChunk,
        Unsupported,
    }

    impl AsyncRead for InvalidChunkSource {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("invalid source must use chunk handoff")))
        }
    }

    impl ShardSource for InvalidChunkSource {
        fn poll_read_chunk(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, max: usize) -> Poll<io::Result<ShardChunkRead>> {
            match self.mode {
                InvalidChunkMode::Empty => Poll::Ready(Ok(ShardChunkRead::Chunk(Bytes::new()))),
                InvalidChunkMode::Oversized => Poll::Ready(Ok(ShardChunkRead::Chunk(Bytes::from(vec![0; max + 1])))),
                InvalidChunkMode::UnsupportedAfterChunk => {
                    self.mode = InvalidChunkMode::Unsupported;
                    Poll::Ready(Ok(ShardChunkRead::Chunk(Bytes::from_static(b"x"))))
                }
                InvalidChunkMode::Unsupported => Poll::Ready(Ok(ShardChunkRead::Unsupported)),
            }
        }
    }

    struct ScratchReuseSource {
        block: Option<Bytes>,
        saw_reused_scratch: bool,
    }

    impl AsyncRead for ScratchReuseSource {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let Some(block) = self.block.take() else {
                return Poll::Ready(Ok(()));
            };
            self.saw_reused_scratch = buf.initialize_unfilled()[..block.len()].iter().all(|byte| *byte == 0xa5);
            buf.put_slice(&block);
            Poll::Ready(Ok(()))
        }
    }

    impl ShardSource for ScratchReuseSource {}

    #[derive(Default)]
    struct VectoredCountingWriter {
        vectored_writes: Arc<AtomicUsize>,
        writes: Vec<u8>,
    }

    impl AsyncWrite for VectoredCountingWriter {
        fn poll_write(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::other("poll_write should not be used")))
        }

        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_write_vectored(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            self.vectored_writes.fetch_add(1, Ordering::SeqCst);
            let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
            for buf in bufs {
                self.writes.extend_from_slice(buf);
            }
            Poll::Ready(Ok(total))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }
    }

    #[derive(Default)]
    struct CountingWriter {
        flushes: Arc<AtomicUsize>,
        shutdowns: Arc<AtomicUsize>,
        writes: Vec<u8>,
    }

    impl AsyncWrite for CountingWriter {
        fn poll_write(mut self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            self.writes.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Default)]
    struct LimitedVectoredWriter {
        max_write: usize,
        writes: Vec<u8>,
    }

    impl AsyncWrite for LimitedVectoredWriter {
        fn poll_write(mut self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            let len = buf.len().min(self.max_write);
            self.writes.extend_from_slice(&buf[..len]);
            Poll::Ready(Ok(len))
        }

        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_write_vectored(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let mut remaining = self.max_write;
            let mut written = 0;
            for buf in bufs {
                if remaining == 0 {
                    break;
                }
                let len = buf.len().min(remaining);
                self.writes.extend_from_slice(&buf[..len]);
                remaining -= len;
                written += len;
            }
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }
    }

    #[test]
    fn bitrot_self_test_payload_is_deterministic() {
        // Two independent builds of the payload must agree byte for byte, or
        // the pinned known-answer digests below would be meaningless.
        assert_eq!(bitrot_self_test_payload(), bitrot_self_test_payload());
    }

    #[test]
    fn bitrot_self_test_rejects_a_wrong_known_answer_digest() {
        let payload = bitrot_self_test_payload();
        let wrong = [0u8; 32];
        let err = bitrot_kat_check("HighwayHash256S", &HashAlgorithm::HighwayHash256S, &payload, &wrong)
            .expect_err("a zeroed digest must never match");
        match err {
            super::BitrotSelfTestError::KnownAnswerMismatch { algorithm, .. } => assert_eq!(algorithm, "HighwayHash256S"),
            other => panic!("expected KnownAnswerMismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bitrot_self_test_passes() {
        bitrot_self_test()
            .await
            .expect("the pinned digests and roundtrip checks must all pass on this platform");
    }

    #[tokio::test]
    async fn vectored_test_writers_cover_fallback_flush_and_shutdown_paths() {
        let mut counting = VectoredCountingWriter::default();
        assert!(counting.is_write_vectored());
        let err = counting
            .write(b"plain write")
            .await
            .expect_err("plain writes should be rejected by vectored-only test writer");
        assert_eq!(err.to_string(), "poll_write should not be used");
        counting.flush().await.expect("flush should succeed");
        counting.shutdown().await.expect("shutdown should succeed");

        let mut limited = LimitedVectoredWriter {
            max_write: 2,
            writes: Vec::new(),
        };
        assert!(limited.is_write_vectored());
        let written = limited
            .write(b"plain")
            .await
            .expect("limited writer should accept partial plain write");
        assert_eq!(written, 2);
        assert_eq!(limited.writes, b"pl");
        limited.flush().await.expect("flush should succeed");
        limited.shutdown().await.expect("shutdown should succeed");
    }

    #[tokio::test]
    async fn test_bitrot_read_write_ok() {
        let data = b"hello world! this is a test shard.";
        let data_size = data.len();
        let shard_size = 8;

        let buf: Vec<u8> = Vec::new();
        let writer = Cursor::new(buf);
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, HashAlgorithm::HighwayHash256);

        let mut n = 0;
        for chunk in data.chunks(shard_size) {
            n += bitrot_writer.write(chunk).await.unwrap();
        }
        assert_eq!(n, data.len());

        // Read
        let reader = bitrot_writer.into_inner();
        let reader = Cursor::new(reader.into_inner());
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256, false);
        let mut out = Vec::new();
        let mut n = 0;
        while n < data_size {
            // Size the buffer to the expected shard length for this stripe (the
            // last stripe is legitimately shorter); BitrotReader now requires the
            // buffer to be filled exactly, matching how the decode/heal paths size
            // per-stripe shard buffers (backlog#799 B2).
            let this_size = shard_size.min(data_size - n);
            let mut buf = vec![0u8; this_size];
            let m = bitrot_reader.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..m], &data[n..n + m]);

            out.extend_from_slice(&buf[..m]);
            n += m;
        }

        assert_eq!(n, data_size);
        assert_eq!(data, &out[..]);
    }

    #[tokio::test]
    async fn bitrot_verify_accepts_valid_shard_file_and_rejects_size_or_hash_mismatch() {
        let data = b"bitrot verify covers every shard";
        let shard_size = 8;
        let algo = HashAlgorithm::HighwayHash256S;
        let writer = Cursor::new(Vec::new());
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, algo.clone());
        for chunk in data.chunks(shard_size) {
            bitrot_writer.write(chunk).await.unwrap();
        }
        let written = bitrot_writer.into_inner().into_inner();

        bitrot_verify(Cursor::new(written.clone()), written.len(), data.len(), algo.clone(), shard_size)
            .await
            .expect("valid bitrot shard file should verify");

        let mut truncated = written.clone();
        truncated.pop();
        let err = bitrot_verify(Cursor::new(truncated), written.len(), data.len(), algo.clone(), shard_size)
            .await
            .expect_err("one-byte-short shard file must be rejected while reading");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);

        let err = bitrot_verify(Cursor::new(written.clone()), written.len() - 1, data.len(), algo.clone(), shard_size)
            .await
            .expect_err("wrong file size must be rejected before reading data");
        assert!(err.to_string().contains("size mismatch"));

        let mut corrupt = written.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x80;
        let err = bitrot_verify(
            std::io::Cursor::new(corrupt),
            super::bitrot_shard_file_size(data.len(), shard_size, algo.clone()),
            data.len(),
            algo,
            shard_size,
        )
        .await
        .expect_err("hash mismatch must reject corrupted data");
        assert!(err.to_string().contains("hash mismatch"));

        for trailing in [vec![0xa5], vec![0xa5; 17]] {
            let mut oversized = written.clone();
            oversized.extend_from_slice(&trailing);
            let err = bitrot_verify(
                Cursor::new(oversized),
                written.len(),
                data.len(),
                HashAlgorithm::HighwayHash256S,
                shard_size,
            )
            .await
            .expect_err("trailing bytes after a valid encoded shard must be rejected");
            assert!(err.to_string().contains("trailing data"));
        }
    }

    #[tokio::test]
    async fn bitrot_verify_rejects_issue_5173_final_block_hash_mismatch() {
        let logical_size = 8_250_370usize;
        let shard_size = 1_048_576usize;
        let algo = HashAlgorithm::HighwayHash256S;
        let mut bitrot_writer = BitrotWriter::new(Cursor::new(Vec::new()), shard_size, algo.clone());
        let data = vec![0x5a; logical_size];
        for chunk in data.chunks(shard_size) {
            bitrot_writer.write(chunk).await.expect("issue 5173 shard should encode");
        }
        let mut written = bitrot_writer.into_inner().into_inner();

        assert_eq!(written.len(), 8_250_626);
        assert_eq!(written.len() - logical_size, 8 * algo.size());

        let last = written.len() - 1;
        written[last] ^= 0x01;
        let err = bitrot_verify(
            Cursor::new(written),
            bitrot_shard_file_size(logical_size, shard_size, algo.clone()),
            logical_size,
            algo,
            shard_size,
        )
        .await
        .expect_err("final block hash mismatch must be rejected");
        assert!(err.to_string().contains("hash mismatch"));
    }

    #[tokio::test]
    async fn bitrot_verify_accepts_exact_legacy_streaming_layout() {
        let data = b"legacy streaming bitrot";
        let shard_size = 8;
        let algo = HashAlgorithm::HighwayHash256SLegacy;
        let mut writer = BitrotWriter::new(Cursor::new(Vec::new()), shard_size, algo.clone());
        for chunk in data.chunks(shard_size) {
            writer.write(chunk).await.expect("legacy streaming shard should encode");
        }
        let written = writer.into_inner().into_inner();

        bitrot_verify(Cursor::new(written.clone()), written.len(), data.len(), algo, shard_size)
            .await
            .expect("exact legacy streaming shard should remain valid");
    }

    #[tokio::test]
    async fn write_all_vectored_retries_partial_hash_and_data_writes_and_rejects_zero_write() {
        let mut writer = LimitedVectoredWriter {
            max_write: 2,
            writes: Vec::new(),
        };

        write_all_vectored(&mut writer, b"hash", b"payload").await.unwrap();
        assert_eq!(writer.writes, b"hashpayload");

        let mut zero_writer = LimitedVectoredWriter {
            max_write: 0,
            writes: Vec::new(),
        };
        let err = write_all_vectored(&mut zero_writer, b"hash", b"payload")
            .await
            .expect_err("zero-byte vectored writes must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::WriteZero);
    }

    #[tokio::test]
    async fn bitrot_reader_rejects_output_buffers_larger_than_shard_size() {
        let mut reader = BitrotReader::new(std::io::Cursor::new(Vec::<u8>::new()), 4, HashAlgorithm::None, false);
        let mut out = [0u8; 5];
        let err = reader
            .read(&mut out)
            .await
            .expect_err("oversized output buffers must be rejected before reading");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("exceeds shard size"));
    }

    #[tokio::test]
    async fn custom_writer_other_forwards_io_and_wrapper_reports_non_inline_state() {
        let writer = CountingWriter::default();
        let mut custom = CustomWriter::new_tokio_writer(writer);
        assert!(custom.get_inline_data().is_none());
        assert!(!custom.is_write_vectored());
        custom.write_all(b"abc").await.unwrap();
        custom.flush().await.unwrap();
        custom.shutdown().await.unwrap();
        assert!(custom.into_inline_data().is_none());

        let other = BitrotWriterWrapper::new(CustomWriter::new_tokio_writer(CountingWriter::default()), 8, HashAlgorithm::None);
        assert!(format!("{other:?}").contains("Other"));
        assert!(other.into_inline_data().is_none());

        let inline = BitrotWriterWrapper::new(CustomWriter::new_inline_buffer(), 8, HashAlgorithm::None);
        assert!(format!("{inline:?}").contains("InlineBuffer"));
    }

    #[tokio::test]
    async fn test_bitrot_read_hash_mismatch() {
        let data = b"test data for bitrot";
        let data_size = data.len();
        let shard_size = 8;
        let buf: Vec<u8> = Vec::new();
        let writer = Cursor::new(buf);
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, HashAlgorithm::HighwayHash256);
        for chunk in data.chunks(shard_size) {
            let _ = bitrot_writer.write(chunk).await.unwrap();
        }
        let mut written = bitrot_writer.into_inner().into_inner();
        // change the last byte to make hash mismatch
        let pos = written.len() - 1;
        written[pos] ^= 0xFF;
        let reader = Cursor::new(written);
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256, false);

        let count = data_size.div_ceil(shard_size);

        let mut idx = 0;
        let mut n = 0;
        while n < data_size {
            let this_size = shard_size.min(data_size - n);
            let mut buf = vec![0u8; this_size];
            let res = bitrot_reader.read(&mut buf).await;

            if idx == count - 1 {
                // The last chunk should trigger an error
                assert!(res.is_err());
                assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
                break;
            }

            let m = res.unwrap();

            assert_eq!(&buf[..m], &data[n..n + m]);

            n += m;
            idx += 1;
        }
    }

    #[tokio::test]
    async fn test_bitrot_read_write_none_hash() {
        let data = b"bitrot none hash test data!";
        let data_size = data.len();
        let shard_size = 8;

        let buf: Vec<u8> = Vec::new();
        let writer = Cursor::new(buf);
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, HashAlgorithm::None);

        let mut n = 0;
        for chunk in data.chunks(shard_size) {
            n += bitrot_writer.write(chunk).await.unwrap();
        }
        assert_eq!(n, data.len());

        let reader = bitrot_writer.into_inner();
        let reader = Cursor::new(reader.into_inner());
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::None, false);
        let mut out = Vec::new();
        let mut n = 0;
        while n < data_size {
            // Size the buffer to the expected shard length for this stripe (the
            // last stripe is legitimately shorter); BitrotReader now requires the
            // buffer to be filled exactly, matching how the decode/heal paths size
            // per-stripe shard buffers (backlog#799 B2).
            let this_size = shard_size.min(data_size - n);
            let mut buf = vec![0u8; this_size];
            let m = bitrot_reader.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..m], &data[n..n + m]);
            out.extend_from_slice(&buf[..m]);
            n += m;
        }
        assert_eq!(n, data_size);
        assert_eq!(data, &out[..]);
    }

    #[tokio::test]
    async fn bitrot_read_short_shard_errors_even_when_skip_verify() {
        // A truncated shard (fewer bytes than the caller's expected-size buffer)
        // must be an error, not a silent Ok(short) — including on the skip-verify
        // / no-hash paths where there is no bitrot hash to catch it. This is the
        // core B2 fix (backlog#799): a short read is a shard error so the decoder
        // drops it and reconstructs from parity instead of shifting downstream
        // bytes.
        let shard_size = 16usize;
        for (algo, skip_verify) in [
            (HashAlgorithm::None, true),
            (HashAlgorithm::None, false),
            (HashAlgorithm::HighwayHash256, true),
        ] {
            let label = format!("{algo:?}");
            let writer = std::io::Cursor::new(Vec::<u8>::new());
            let mut w = BitrotWriter::new(writer, shard_size, algo.clone());
            w.write(&[7u8; 16]).await.unwrap();
            let written = w.into_inner().into_inner();
            // Drop the last 4 data bytes so the shard is truncated.
            let truncated = written[..written.len() - 4].to_vec();

            let mut r = BitrotReader::new(Cursor::new(truncated), shard_size, algo, skip_verify);
            let mut out = vec![0u8; shard_size];
            let res = r.read(&mut out).await;
            assert!(res.is_err(), "short shard must error (algo={label}, skip_verify={skip_verify})");
            assert_eq!(
                res.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof,
                "short shard must be UnexpectedEof (algo={label}, skip_verify={skip_verify})"
            );
        }
    }

    #[tokio::test]
    async fn test_bitrot_writer_flushes_once_on_shutdown() {
        let flushes = Arc::new(AtomicUsize::new(0));
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let writer = CountingWriter {
            flushes: flushes.clone(),
            shutdowns: shutdowns.clone(),
            writes: Vec::new(),
        };
        let mut bitrot_writer = BitrotWriter::new(writer, 8, HashAlgorithm::None);

        bitrot_writer.write(b"12345678").await.unwrap();
        bitrot_writer.write(b"abc").await.unwrap();

        assert_eq!(flushes.load(Ordering::SeqCst), 0);
        assert_eq!(shutdowns.load(Ordering::SeqCst), 0);

        bitrot_writer.shutdown().await.unwrap();

        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_bitrot_writer_uses_vectored_write_for_hash_and_data() {
        let vectored_writes = Arc::new(AtomicUsize::new(0));
        let writer = VectoredCountingWriter {
            vectored_writes: vectored_writes.clone(),
            writes: Vec::new(),
        };
        let mut bitrot_writer = BitrotWriter::new(writer, 8, HashAlgorithm::HighwayHash256);

        bitrot_writer.write(b"payload").await.unwrap();

        assert!(vectored_writes.load(Ordering::SeqCst) > 0);
    }

    /// A reader that hands back at most `chunk` bytes per `poll_read` and counts
    /// how many times it is polled — a stand-in for the streaming disk File where
    /// each poll is a spawn_blocking round-trip.
    struct CountingReader {
        data: std::io::Cursor<Vec<u8>>,
        reads: Arc<AtomicUsize>,
        chunk: usize,
    }

    impl tokio::io::AsyncRead for CountingReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let cap = buf.remaining().min(self.chunk);
            if cap == 0 {
                return Poll::Ready(Ok(()));
            }
            let mut scratch = vec![0u8; cap];
            let n = std::io::Read::read(&mut self.data, &mut scratch).unwrap_or(0);
            buf.put_slice(&scratch[..n]);
            let _ = cx;
            Poll::Ready(Ok(()))
        }
    }

    async fn encode_one_block(payload: &[u8], shard_size: usize, algo: HashAlgorithm) -> Vec<u8> {
        let mut w = BitrotWriter::new(std::io::Cursor::new(Vec::<u8>::new()), shard_size, algo);
        w.write(payload).await.unwrap();
        w.into_inner().into_inner()
    }

    #[tokio::test]
    async fn hashed_read_issues_a_single_dispatch_per_block() {
        // The hashed path must pull the contiguous [hash][data] block in one
        // read, not one dispatch for the 32-byte hash and another for the shard
        // (backlog#933 item 2). With a reader large enough to satisfy the whole
        // block in a single poll, exactly one poll_read per block is expected.
        let shard_size = 64usize;
        let payload = vec![9u8; shard_size];
        let block = encode_one_block(&payload, shard_size, HashAlgorithm::HighwayHash256).await;
        assert!(block.len() > shard_size, "block must carry a leading hash");

        let reads = Arc::new(AtomicUsize::new(0));
        let reader = CountingReader {
            data: Cursor::new(block),
            reads: reads.clone(),
            chunk: usize::MAX,
        };
        let mut r = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256, false);
        let mut out = vec![0u8; shard_size];
        let n = r.read(&mut out).await.unwrap();

        assert_eq!(n, shard_size);
        assert_eq!(out, payload);
        assert_eq!(reads.load(Ordering::SeqCst), 1, "one contiguous read for hash+data");
    }

    #[tokio::test]
    async fn hashed_read_reassembles_across_partial_reads() {
        // When the underlying reader dribbles the block out a few bytes at a
        // time, the fill loop must reassemble the full [hash][data] run before
        // splitting — no byte shifting, hash still verifies.
        let shard_size = 40usize;
        let payload: Vec<u8> = (0..shard_size as u8).collect();
        let block = encode_one_block(&payload, shard_size, HashAlgorithm::HighwayHash256).await;

        let reads = Arc::new(AtomicUsize::new(0));
        let reader = CountingReader {
            data: Cursor::new(block),
            reads: reads.clone(),
            chunk: 7, // force many partial reads
        };
        let mut r = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256, false);
        let mut out = vec![0u8; shard_size];
        let n = r.read(&mut out).await.unwrap();

        assert_eq!(n, shard_size);
        assert_eq!(out, payload);
        assert!(reads.load(Ordering::SeqCst) > 1, "partial reads should loop");
    }

    // --- backlog#959 / ECA-18 invariant guards -------------------------------
    //
    // These tests pin the *intended* per-algorithm bitrot layout so a future
    // change can't silently drift `bitrot_shard_file_size`, `BitrotWriter`, and
    // `bitrot_verify` out of agreement. The design contract (see the doc
    // comments above): only the two streaming Highway variants use interleaved
    // per-block `[hash][data]` layout; every other algorithm maps to MinIO
    // whole-file bitrot and its shard file is exactly `size` bytes on disk.

    #[test]
    fn bitrot_shard_file_size_counts_hash_only_for_streaming_variants() {
        // For a range of sizes (including exact multiples and non-multiples of
        // shard_size), streaming variants add one hash per block; all others
        // return the bare `size`.
        let shard_size = 16usize;
        let hash = 32usize; // both streaming Highway variants are 32 bytes
        for &size in &[0usize, 1, 15, 16, 17, 31, 32, 33, 100, 160] {
            let blocks = size.div_ceil(shard_size);

            for algo in [HashAlgorithm::HighwayHash256S, HashAlgorithm::HighwayHash256SLegacy] {
                assert_eq!(
                    bitrot_shard_file_size(size, shard_size, algo.clone()),
                    size + blocks * hash,
                    "streaming variant {algo:?} must count per-block hash bytes (size={size})"
                );
            }

            for algo in [
                HashAlgorithm::SHA256,
                HashAlgorithm::HighwayHash256,
                HashAlgorithm::BLAKE2b512,
                HashAlgorithm::Md5,
                HashAlgorithm::None,
            ] {
                assert_eq!(
                    bitrot_shard_file_size(size, shard_size, algo.clone()),
                    size,
                    "non-streaming variant {algo:?} must return the bare on-disk size (size={size})"
                );
            }
        }
    }

    #[tokio::test]
    async fn bitrot_shard_file_size_matches_streaming_writer_output() {
        // The size formula for streaming variants must equal the byte count that
        // BitrotWriter actually writes to disk, across integral and partial
        // final blocks — this is the invariant `bitrot_verify`'s size guard
        // relies on.
        let shard_size = 16usize;
        for algo in [HashAlgorithm::HighwayHash256S, HashAlgorithm::HighwayHash256SLegacy] {
            for &size in &[1usize, 16, 17, 32, 40, 48] {
                let payload: Vec<u8> = (0..size).map(|i| i as u8).collect();
                let mut w = BitrotWriter::new(std::io::Cursor::new(Vec::<u8>::new()), shard_size, algo.clone());
                for chunk in payload.chunks(shard_size) {
                    w.write(chunk).await.unwrap();
                }
                let on_disk = w.into_inner().into_inner().len();
                assert_eq!(
                    on_disk,
                    bitrot_shard_file_size(size, shard_size, algo.clone()),
                    "writer output must match size formula ({algo:?}, size={size})"
                );
            }
        }
    }

    #[test]
    fn streaming_variants_are_the_only_per_block_bitrot_algorithms() {
        // Documents the known, intentional divergence at the heart of ECA-18:
        // BitrotWriter interleaves a hash whenever `size() > 0`, but the shard
        // size formula only counts hash bytes for the streaming variants. The
        // two conditions agree ONLY for the streaming variants; this test locks
        // that boundary so nobody "fixes" one side without the other. If a new
        // algorithm is added, this test forces an explicit decision here.
        let shard_size = 16usize;
        let size = 40usize; // spans multiple blocks
        for algo in [
            HashAlgorithm::SHA256,
            HashAlgorithm::HighwayHash256,
            HashAlgorithm::HighwayHash256S,
            HashAlgorithm::HighwayHash256SLegacy,
            HashAlgorithm::BLAKE2b512,
            HashAlgorithm::Md5,
            HashAlgorithm::None,
        ] {
            let streaming = matches!(algo, HashAlgorithm::HighwayHash256S | HashAlgorithm::HighwayHash256SLegacy);
            let formula_counts_hash = bitrot_shard_file_size(size, shard_size, algo.clone()) > size;
            assert_eq!(
                formula_counts_hash, streaming,
                "only streaming Highway variants may count per-block hash bytes ({algo:?})"
            );
        }
    }

    #[tokio::test]
    async fn hashed_read_truncated_within_hash_is_unexpected_eof() {
        // Truncation that lands inside the leading hash (not the data) must still
        // surface as UnexpectedEof, so the stripe drops this reader and rebuilds
        // from parity instead of hanging or splitting it wrong (backlog#799 B2).
        let shard_size = 32usize;
        let block = encode_one_block(&vec![3u8; shard_size], shard_size, HashAlgorithm::HighwayHash256).await;
        let hash_size = HashAlgorithm::HighwayHash256.size();
        // Keep only part of the hash; drop the rest of the block.
        let truncated = block[..hash_size / 2].to_vec();

        let mut r = BitrotReader::new(Cursor::new(truncated), shard_size, HashAlgorithm::HighwayHash256, false);
        let mut out = vec![0u8; shard_size];
        let err = r.read(&mut out).await.expect_err("truncated hash must error");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    /// `read_appending` must be byte-for-byte identical to `read`, for both the
    /// hashed and the no-hash path (rustfs/backlog#1159). It exists so callers can
    /// hand over an *uninitialized* buffer; if it ever diverged from `read`, the
    /// GET path would silently return different bytes.
    #[tokio::test]
    async fn read_appending_matches_read_for_both_paths() {
        for algo in [HashAlgorithm::HighwayHash256, HashAlgorithm::None] {
            const SHARD: usize = 4096;
            let data: Vec<u8> = (0..SHARD).map(|i| (i * 31 + 7) as u8).collect();

            let mut encoded = Vec::new();
            let mut w = BitrotWriter::new(&mut encoded, SHARD, algo.clone());
            w.write(&data).await.expect("write shard");

            let mut via_read = vec![0u8; SHARD];
            let n1 = BitrotReader::new(std::io::Cursor::new(encoded.clone()), SHARD, algo.clone(), false)
                .read(&mut via_read)
                .await
                .expect("read");

            // A buffer with only capacity — no initialized bytes at all.
            let mut via_append: Vec<u8> = Vec::with_capacity(SHARD);
            let n2 = BitrotReader::new(std::io::Cursor::new(encoded), SHARD, algo.clone(), false)
                .read_appending(&mut via_append, SHARD)
                .await
                .expect("read_appending");

            assert_eq!(n1, n2, "{algo:?}: both must report the same length");
            assert_eq!(via_append.len(), n2, "{algo:?}: the buffer grows by exactly n");
            assert_eq!(via_read, via_append, "{algo:?}: bytes must be identical");
            assert_eq!(via_append, data, "{algo:?}: and equal to what was written");
        }
    }

    /// A truncated shard must be an error, never a partially filled buffer — that
    /// contract is what lets the pool hand out uninitialized capacity.
    #[tokio::test]
    async fn read_appending_rejects_a_short_shard_instead_of_returning_partial_bytes() {
        for algo in [HashAlgorithm::HighwayHash256, HashAlgorithm::None] {
            const SHARD: usize = 4096;
            let data = vec![9u8; SHARD];
            let mut encoded = Vec::new();
            let mut w = BitrotWriter::new(&mut encoded, SHARD, algo.clone());
            w.write(&data).await.expect("write shard");
            encoded.truncate(encoded.len() - 1);

            let mut out: Vec<u8> = Vec::with_capacity(SHARD);
            let err = BitrotReader::new(std::io::Cursor::new(encoded), SHARD, algo.clone(), false)
                .read_appending(&mut out, SHARD)
                .await
                .expect_err("a truncated shard must not succeed");
            assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof, "{algo:?}");
            assert!(
                out.len() < SHARD,
                "{algo:?}: a failed read must not claim a full shard; the caller drops the buffer"
            );
        }
    }

    /// A corrupt shard must fail verification, and the corrupt bytes must NOT be
    /// appended: `read_appending` writes into a buffer the caller may recycle.
    #[tokio::test]
    async fn read_appending_does_not_expose_bytes_that_fail_the_hash() {
        const SHARD: usize = 4096;
        let algo = HashAlgorithm::HighwayHash256;
        let data = vec![3u8; SHARD];
        let mut encoded = Vec::new();
        let mut w = BitrotWriter::new(&mut encoded, SHARD, algo.clone());
        w.write(&data).await.expect("write shard");
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;

        let mut out: Vec<u8> = Vec::with_capacity(SHARD);
        let err = BitrotReader::new(std::io::Cursor::new(encoded), SHARD, algo, false)
            .read_appending(&mut out, SHARD)
            .await
            .expect_err("a corrupt shard must not verify");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(out.is_empty(), "corrupt bytes must never reach the caller's buffer");
    }

    #[tokio::test]
    async fn chunked_handoff_verifies_data_split_across_hash_boundaries() {
        const SHARD: usize = 4096;
        let algo = HashAlgorithm::HighwayHash256S;
        let data: Vec<u8> = (0..SHARD).map(|index| (index % 251) as u8).collect();
        let mut encoded = Vec::new();
        BitrotWriter::new(&mut encoded, SHARD, algo.clone())
            .write(&data)
            .await
            .expect("write shard");

        let mut output = Vec::with_capacity(SHARD);
        BitrotReader::new(FragmentedSource::new(encoded, &[3, 11, 19, 37, 128]), SHARD, algo, false)
            .read_appending(&mut output, SHARD)
            .await
            .expect("fragmented shard must verify");

        assert_eq!(output, data);
    }

    #[tokio::test]
    async fn chunked_handoff_never_appends_a_corrupt_shard() {
        const SHARD: usize = 4096;
        let algo = HashAlgorithm::HighwayHash256S;
        let mut encoded = Vec::new();
        BitrotWriter::new(&mut encoded, SHARD, algo.clone())
            .write(&vec![9u8; SHARD])
            .await
            .expect("write shard");
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;

        let mut output = Vec::with_capacity(SHARD);
        let err = BitrotReader::new(FragmentedSource::new(encoded, &[7, 17, 31]), SHARD, algo, false)
            .read_appending(&mut output, SHARD)
            .await
            .expect_err("corrupt fragmented shard must fail");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn chunked_handoff_does_not_hash_when_verification_is_skipped() {
        const SHARD: usize = 4096;
        let algo = HashAlgorithm::HighwayHash256S;
        let mut encoded = Vec::new();
        BitrotWriter::new(&mut encoded, SHARD, algo.clone())
            .write(&vec![9u8; SHARD])
            .await
            .expect("write shard");
        encoded[0] ^= 0xff;

        let mut output = Vec::with_capacity(SHARD);
        let mut reader = BitrotReader::new(FragmentedSource::new(encoded, &[7, 17, 31]), SHARD, algo, true);
        reader
            .read_appending(&mut output, SHARD)
            .await
            .expect("skipped verification must accept fragmented shard bytes");

        assert_eq!(reader.last_verify_duration(), Duration::ZERO);
        assert_eq!(output, vec![9u8; SHARD]);
    }

    #[tokio::test]
    async fn read_appending_rejects_a_want_larger_than_the_shard() {
        let algo = HashAlgorithm::HighwayHash256;
        let mut out: Vec<u8> = Vec::new();
        let err = BitrotReader::new(Cursor::new(Vec::new()), 16, algo, false)
            .read_appending(&mut out, 17)
            .await
            .expect_err("want > shard_size must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    /// The in-memory fast path (rustfs/backlog#1159) must be *equivalent* to the
    /// streaming path, not merely present. `Cursor<Bytes>` slices the
    /// `[hash][data]` block instead of copying it into the scratch buffer; if that
    /// ever diverged, GET would return different bytes.
    ///
    /// The first assertion is the non-vacuity gate: it proves `try_take_block`
    /// actually fires for `Cursor<Bytes>` (and does not for `Cursor<Vec<u8>>`), so
    /// the equivalence below is really comparing two different code paths.
    #[tokio::test]
    async fn in_memory_fast_path_fires_and_matches_the_streaming_path() {
        use bytes::Bytes;
        use std::io::Cursor;

        const SHARD: usize = 4096;
        let algo = HashAlgorithm::HighwayHash256;
        let data: Vec<u8> = (0..SHARD).map(|i| (i * 17 + 3) as u8).collect();
        let mut encoded = Vec::new();
        BitrotWriter::new(&mut encoded, SHARD, algo.clone())
            .write(&data)
            .await
            .expect("write shard");

        // Non-vacuity: the fast path exists for Bytes and not for Vec.
        let mut mem = Cursor::new(Bytes::from(encoded.clone()));
        assert!(
            ShardSource::try_take_block(&mut mem, 8).is_some(),
            "Cursor<Bytes> must be able to hand out a block, otherwise the fast path is dead code"
        );
        assert_eq!(mem.position(), 8, "taking a block must advance like a read of the same length");
        let mut streamed = std::io::Cursor::new(encoded.clone());
        assert!(
            ShardSource::try_take_block(&mut streamed, 8).is_none(),
            "a non-Bytes source must stay on the streaming path"
        );
        // A block larger than what is left must decline rather than truncate.
        let mut short = Cursor::new(Bytes::from_static(b"1234"));
        assert!(ShardSource::try_take_block(&mut short, 5).is_none());

        // Equivalence: same bytes out of both paths.
        let mut via_mem: Vec<u8> = Vec::with_capacity(SHARD);
        let mut memory_reader = BitrotReader::new(Cursor::new(Bytes::from(encoded.clone())), SHARD, algo.clone(), false);
        memory_reader
            .read_appending(&mut via_mem, SHARD)
            .await
            .expect("in-memory read");
        assert_eq!(
            memory_reader.chunks.capacity(),
            0,
            "the synchronous fast path must not allocate chunk storage"
        );
        assert_eq!(
            memory_reader.buf.capacity(),
            0,
            "the synchronous fast path must not allocate scratch storage"
        );

        let mut via_stream: Vec<u8> = Vec::with_capacity(SHARD);
        BitrotReader::new(std::io::Cursor::new(encoded), SHARD, algo, false)
            .read_appending(&mut via_stream, SHARD)
            .await
            .expect("streaming read");

        assert_eq!(via_mem, via_stream, "the two paths must return identical bytes");
        assert_eq!(via_mem, data);
    }

    /// A corrupt shard must fail on the fast path too — the slice is verified
    /// before anything is appended, exactly as on the streaming path.
    #[tokio::test]
    async fn in_memory_fast_path_rejects_a_corrupt_shard_without_appending() {
        use bytes::Bytes;
        use std::io::Cursor;

        const SHARD: usize = 4096;
        let algo = HashAlgorithm::HighwayHash256;
        let mut encoded = Vec::new();
        BitrotWriter::new(&mut encoded, SHARD, algo.clone())
            .write(&vec![5u8; SHARD])
            .await
            .expect("write shard");
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;

        let mut out: Vec<u8> = Vec::with_capacity(SHARD);
        let err = BitrotReader::new(Cursor::new(Bytes::from(encoded)), SHARD, algo, false)
            .read_appending(&mut out, SHARD)
            .await
            .expect_err("a corrupt shard must not verify on the fast path either");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(out.is_empty(), "corrupt bytes must never reach the caller's buffer");
    }

    #[tokio::test]
    async fn streaming_fallback_reuses_initialized_scratch() {
        const SHARD: usize = 4096;
        let algo = HashAlgorithm::HighwayHash256S;
        let data = vec![7u8; SHARD];
        let encoded = encode_one_block(&data, SHARD, algo.clone()).await;
        let source = ScratchReuseSource {
            block: Some(Bytes::copy_from_slice(&encoded)),
            saw_reused_scratch: false,
        };
        let mut reader = BitrotReader::new(source, SHARD, algo, false);
        reader.buf = vec![0xa5; encoded.len()];
        let mut output = Vec::new();

        reader
            .read_appending(&mut output, SHARD)
            .await
            .expect("streaming fallback should verify");

        assert!(reader.inner.saw_reused_scratch, "capability probing must not clear reusable scratch");
        assert_eq!(output, data);
    }

    #[tokio::test]
    async fn chunked_handoff_bounds_production_sized_one_byte_fragments() {
        const SHARD: usize = 1024 * 1024 / 4;
        let algo = HashAlgorithm::HighwayHash256S;
        let data: Vec<u8> = (0..SHARD).map(|index| (index % 251) as u8).collect();
        let encoded = encode_one_block(&data, SHARD, algo.clone()).await;
        let encoded_len = encoded.len();
        let mut reader = BitrotReader::new(GeneratedChunkSource::new(encoded, 1), SHARD, algo, false);
        let mut output = Vec::with_capacity(SHARD);

        reader
            .read_appending(&mut output, SHARD)
            .await
            .expect("one-byte fragments should verify with bounded retained state");

        assert_eq!(output, data);
        assert_eq!(reader.chunks.len(), MAX_RETAINED_CHUNKS_PER_BLOCK);
        assert!(reader.chunks.capacity() <= MAX_RETAINED_CHUNKS_PER_BLOCK);
        assert_eq!(reader.buf.len(), encoded_len - MAX_RETAINED_CHUNKS_PER_BLOCK);
    }

    #[tokio::test]
    async fn chunked_handoff_keeps_sixty_four_frames_zero_copy_and_respects_poll_budget() {
        const SHARD: usize = 1024 * 1024;
        const FRAME: usize = 16 * 1024;
        let algo = HashAlgorithm::HighwayHash256S;

        let small_data = vec![3u8; 4096];
        let small_encoded = encode_one_block(&small_data, 4096, algo.clone()).await;
        let mut exact_reader =
            BitrotReader::new(FragmentedSource::new(small_encoded.clone(), &[1; 63]), 4096, algo.clone(), false);
        let mut exact_output = Vec::new();
        exact_reader
            .read_appending(&mut exact_output, 4096)
            .await
            .expect("exactly sixty-four frames should verify");
        assert_eq!(exact_output, small_data);
        assert_eq!(exact_reader.chunks.len(), MAX_RETAINED_CHUNKS_PER_BLOCK);
        assert!(exact_reader.buf.is_empty(), "the threshold itself must remain zero-copy");

        let mut yielded_reader = BitrotReader::new(FragmentedSource::new(small_encoded, &[1; 65]), 4096, algo.clone(), false);
        let mut yielded_output = Vec::new();
        let mut yielded_read = Box::pin(yielded_reader.read_appending(&mut yielded_output, 4096));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(yielded_read.as_mut(), &mut cx).is_pending());
        assert!(matches!(std::future::Future::poll(yielded_read.as_mut(), &mut cx), Poll::Ready(Ok(4096))));
        drop(yielded_read);
        assert_eq!(yielded_output, small_data);

        let data = vec![7u8; SHARD];
        let encoded = encode_one_block(&data, SHARD, algo.clone()).await;
        let mut reader = BitrotReader::new(FragmentedSource::new(encoded, &[FRAME; 64]), SHARD, algo, false);
        let mut output = Vec::with_capacity(SHARD);
        let mut read = Box::pin(reader.read_appending(&mut output, SHARD));
        assert!(
            matches!(std::future::Future::poll(read.as_mut(), &mut cx), Poll::Ready(Ok(SHARD))),
            "sixty-five normal HTTP frames should complete without a cooperative yield"
        );
        drop(read);

        assert_eq!(output, data);
        assert_eq!(reader.chunks.len(), MAX_RETAINED_CHUNKS_PER_BLOCK);
        assert_eq!(reader.buf.len(), HashAlgorithm::HighwayHash256S.size());
    }

    #[tokio::test]
    async fn chunked_tail_failures_preserve_errors_and_output() {
        const SHARD: usize = 4096;
        let algo = HashAlgorithm::HighwayHash256S;
        let data = vec![7u8; SHARD];
        let encoded = encode_one_block(&data, SHARD, algo.clone()).await;
        let sentinel = vec![1u8, 2, 3];

        let mut short_output = sentinel.clone();
        let short_err = BitrotReader::new(GeneratedChunkSource::new(encoded[..100].to_vec(), 1), SHARD, algo.clone(), false)
            .read_appending(&mut short_output, SHARD)
            .await
            .expect_err("EOF after the retention threshold must stay a short read");
        assert_eq!(short_err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(short_output, sentinel);

        let mut corrupt = encoded.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        let mut corrupt_output = sentinel.clone();
        let corrupt_err = BitrotReader::new(GeneratedChunkSource::new(corrupt, 1), SHARD, algo.clone(), false)
            .read_appending(&mut corrupt_output, SHARD)
            .await
            .expect_err("corrupt coalesced tail must fail verification");
        assert_eq!(corrupt_err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(corrupt_output, sentinel);

        let mut failed_output = sentinel.clone();
        let body_err = BitrotReader::new(GeneratedChunkSource::failing(encoded, 1, 65), SHARD, algo, false)
            .read_appending(&mut failed_output, SHARD)
            .await
            .expect_err("a terminal body error must not become EOF");
        let source = body_err
            .get_ref()
            .and_then(|source| source.downcast_ref::<rustfs_rio::InternodeHttpError>())
            .expect("body error should retain internode classification");
        assert_eq!(source.kind(), rustfs_rio::InternodeHttpErrorKind::BodyStreamAborted);
        assert_eq!(failed_output, sentinel);
    }

    #[tokio::test]
    async fn chunked_handoff_rejects_invalid_source_contracts() {
        const SHARD: usize = 64;
        for mode in [
            InvalidChunkMode::Empty,
            InvalidChunkMode::Oversized,
            InvalidChunkMode::UnsupportedAfterChunk,
        ] {
            let source = InvalidChunkSource { mode };
            let mut output = vec![9u8];
            let err = BitrotReader::new(source, SHARD, HashAlgorithm::HighwayHash256S, false)
                .read_appending(&mut output, SHARD)
                .await
                .expect_err("invalid chunk contracts must fail closed");

            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert_eq!(output, vec![9u8]);
        }
    }
}
