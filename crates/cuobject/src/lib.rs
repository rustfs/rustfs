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

//! Server side of NVIDIA cuObject S3-over-RDMA.
//!
//! A client that supports the protocol (NVIDIA cuObject, NIXL's object plugin,
//! …) sends an ordinary S3 GET with an `x-amz-rdma-token` header describing a
//! registered client buffer (host or GPU memory). Instead of streaming the
//! body over HTTP, the server RDMA-writes the object bytes into that buffer
//! through NVIDIA's `libcuobjserver` and answers with an empty body plus
//! `x-amz-rdma-reply` and `x-amz-rdma-bytes-transferred`.
//!
//! The library bindings come from the [`cuobject`] crate. This crate owns
//! RustFS's pool of registered, page-aligned host staging buffers: the regular
//! read path fills a staging chunk and
//! [`CuObjectServer::write_to_client_owned`] pushes it.
//!
//! Everything below needs the `server` feature, which compiles the
//! NVIDIA cuObject server bindings; without it the crate is empty, so a
//! workspace build does not need the cuObject SDK.
#![cfg(feature = "server")]

use bytes::Bytes;
use cuobject::{Channel, Completion, RegisteredBuffer, Server, ServerOptions, SgBuffer, SgEntry, Source};
use std::collections::VecDeque;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::sync::Semaphore;

/// Header carrying the client's RDMA descriptor on a request.
pub const RDMA_TOKEN_HEADER: &str = cuobject::TOKEN_HEADER;
/// Header carrying the transfer status on the response (HTTP-style code).
pub const RDMA_REPLY_HEADER: &str = cuobject::REPLY_HEADER;
/// Header carrying the payload byte count moved over RDMA.
pub const RDMA_BYTES_TRANSFERRED_HEADER: &str = cuobject::BYTES_TRANSFERRED_HEADER;
/// Reply code for a declined transfer (the client then errors or falls back).
pub const RDMA_REPLY_NOT_IMPLEMENTED: u16 = cuobject::REPLY_NOT_IMPLEMENTED;
/// Largest single `handleGetObject` the library accepts.
pub const MAX_RDMA_OP_BYTES: usize = cuobject::MAX_TRANSFER_BYTES;
/// Default RDMA control port of the cuObject server.
pub const DEFAULT_PORT: u16 = cuobject::DEFAULT_PORT;
/// Most ranges one scatter-gather transfer may carry (the library's `max_sge`).
pub const MAX_SG_RANGES: usize = cuobject::DEFAULT_MAX_SG_ENTRIES;

/// Version of the cuObject server headers the bindings were built against, as
/// (major, minor).
pub fn library_version() -> (u32, u32) {
    cuobject::header_version()
}

#[derive(Debug, Error)]
pub enum CuObjectError {
    #[error("cuobject {op} failed: {source}")]
    Os {
        op: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("cuobject rdma token is malformed: {0}")]
    Token(String),
    #[error("cuobject rdma token buffer ({buffer} bytes) is smaller than the response ({needed} bytes)")]
    TokenTooSmall { buffer: u64, needed: u64 },
    #[error("cuobject rejected the request: {0}")]
    Invalid(&'static str),
    #[error("cuobject transfer failed with verbs status {verbs_status}: {source}")]
    Transfer {
        verbs_status: i32,
        #[source]
        source: std::io::Error,
    },
    #[error("cuobject transfer moved {moved} of {expected} bytes")]
    ShortTransfer { moved: u64, expected: u64 },
    #[error("cuobject staging pool exhausted")]
    PoolExhausted,
    #[error("cuobject library offers no completion descriptor for channel {0}")]
    AsyncUnavailable(u16),
    #[error("cuobject read source failed: {0}")]
    Source(#[source] std::io::Error),
}

impl From<cuobject::Error> for CuObjectError {
    fn from(error: cuobject::Error) -> Self {
        match error {
            cuobject::Error::Library { op, source } => Self::Os { op, source },
            cuobject::Error::Transfer { verbs_status, source } => Self::Transfer { verbs_status, source },
            cuobject::Error::InvalidArgument(reason) => Self::Invalid(reason),
            cuobject::Error::InvalidToken(reason) => Self::Token(reason.to_owned()),
            cuobject::Error::TokenTooSmall { buffer, needed } => Self::TokenTooSmall { buffer, needed },
            cuobject::Error::AsyncUnavailable(channel) => Self::AsyncUnavailable(channel),
            other => Self::Os {
                op: "cuobject",
                source: std::io::Error::other(other),
            },
        }
    }
}

/// Parsed `x-amz-rdma-token`.
///
/// Wire format (cuObject RDMA descriptor protocol): colon-separated hex
/// fields `addr:size:rkey:lid:dctn:gid_present:gid`; only the leading
/// address and size are interpreted here, the library consumes the rest.
#[derive(Debug, Clone)]
pub struct RdmaToken {
    inner: cuobject::RdmaToken,
    /// Start of the client buffer (a copy of the parsed field).
    pub remote_addr: u64,
    /// Length of the client buffer (a copy of the parsed field).
    pub buffer_len: u64,
}

impl RdmaToken {
    pub fn parse(raw: &str) -> Result<Self, CuObjectError> {
        let inner = cuobject::RdmaToken::parse(raw)?;
        Ok(Self {
            remote_addr: inner.remote_addr(),
            buffer_len: inner.buffer_len(),
            inner,
        })
    }

    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }
}

/// Tuning for one server instance.
#[derive(Debug, Clone)]
pub struct CuObjectConfig {
    pub port: u16,
    pub num_dcis: i32,
    pub cq_depth: u32,
    /// Size of one staging buffer; one RDMA write moves at most this much.
    pub chunk_bytes: usize,
    /// Total registered staging memory per server instance.
    pub pool_bytes: usize,
    /// Complete transfers through the library's asynchronous mode (submit,
    /// then wait on the channel's completion descriptor) instead of one
    /// blocking library call per transfer.
    pub async_transfers: bool,
}

impl Default for CuObjectConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            num_dcis: 128,
            cq_depth: 640,
            chunk_bytes: 64 * 1024 * 1024,
            pool_bytes: 1024 * 1024 * 1024,
            async_transfers: true,
        }
    }
}

struct Staging {
    buffer: RegisteredBuffer<Arc<Server>>,
    free: parking_lot::Mutex<VecDeque<usize>>,
    permits: Semaphore,
    chunk_bytes: usize,
}

/// One cuObject server bound to the RDMA device that owns an IP address.
pub struct CuObjectServer {
    server: Arc<Server>,
    staging: Staging,
    addr: IpAddr,
    port: u16,
    /// One permit per library channel (DC initiator): a transfer waits here
    /// instead of failing with `EBUSY` when every channel is in use.
    channels: Arc<Semaphore>,
    async_transfers: bool,
}

impl std::fmt::Debug for CuObjectServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CuObjectServer")
            .field("addr", &self.addr)
            .field("port", &self.port)
            .field("chunk_bytes", &self.staging.chunk_bytes)
            .finish()
    }
}

/// Several byte ranges of one [`RegisteredArena`], sent in order as one
/// RDMA operation (at most [`MAX_SG_RANGES`]).
pub struct ArenaRanges {
    pub arena: Arc<RegisteredArena>,
    /// `(local_offset, len)` pairs, in stream order.
    pub ranges: Vec<(usize, usize)>,
}

impl ArenaRanges {
    pub fn total_len(&self) -> usize {
        self.ranges.iter().map(|(_, len)| *len).sum()
    }
}

/// A byte range of a [`RegisteredArena`] to send from.
pub struct ArenaRange {
    pub arena: Arc<RegisteredArena>,
    pub local_offset: usize,
    pub len: usize,
}

/// Host memory registered with one server, owned by the caller (see
/// [`CuObjectServer::register_arena`]).
pub struct RegisteredArena {
    buffer: RegisteredBuffer<Arc<Server>>,
    server: Arc<CuObjectServer>,
    id: u64,
}

impl RegisteredArena {
    /// Length of the registered memory.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Process-unique id of this registration.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The server this memory is registered with.
    pub fn server(&self) -> &Arc<CuObjectServer> {
        &self.server
    }

    /// Raw pointer to the start of the registered memory. The memory lives as
    /// long as this arena; callers hand out disjoint ranges of it and never
    /// touch a range while a transfer sends it.
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.buffer.as_mut_ptr()
    }
}

impl std::fmt::Debug for RegisteredArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredArena")
            .field("id", &self.id)
            .field("len", &self.len())
            .finish()
    }
}

/// A reserved staging chunk; returned to the pool on drop.
///
/// The chunk's index is unique while it is reserved, so its range of the
/// staging buffer has one owner. Transfers take the chunk by value, so no
/// Rust borrow of the range overlaps an RDMA operation on it.
pub struct StagingChunk {
    server: Arc<CuObjectServer>,
    index: usize,
    len: usize,
}

impl StagingChunk {
    pub fn capacity(&self) -> usize {
        self.server.staging.chunk_bytes
    }

    /// Bytes currently staged.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Copies `data` at `self.len()` and advances the staged length.
    pub fn append(&mut self, data: &[u8]) -> Result<(), CuObjectError> {
        if self.len + data.len() > self.capacity() {
            return Err(CuObjectError::Invalid("staging append exceeds chunk"));
        }
        self.spare_mut()?[..data.len()].copy_from_slice(data);
        self.len += data.len();
        Ok(())
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn is_full(&self) -> bool {
        self.len >= self.capacity()
    }

    /// The bytes staged so far.
    #[allow(unsafe_code)]
    pub fn staged(&self) -> Result<&[u8], CuObjectError> {
        // SAFETY: the range lies in this chunk (see the type docs); `&self`
        // excludes `spare_mut`, and transfers own the chunk they move.
        Ok(unsafe { self.server.staging.buffer.slice(self.start(), self.len)? })
    }

    /// The unfilled remainder of the chunk, for a reader to fill directly.
    #[allow(unsafe_code)]
    pub fn spare_mut(&mut self) -> Result<&mut [u8], CuObjectError> {
        let len = self.capacity() - self.len;
        // SAFETY: the range lies in this chunk (see the type docs) and
        // `&mut self` excludes every other borrow of it.
        Ok(unsafe { self.server.staging.buffer.slice_mut(self.start() + self.len, len)? })
    }

    /// Marks `n` more bytes of the spare area as staged.
    pub fn advance(&mut self, n: usize) -> Result<(), CuObjectError> {
        if self.len + n > self.capacity() {
            return Err(CuObjectError::Invalid("staging advance exceeds chunk"));
        }
        self.len += n;
        Ok(())
    }

    fn start(&self) -> usize {
        self.index * self.capacity()
    }
}

impl Drop for StagingChunk {
    fn drop(&mut self) {
        self.server.staging.free.lock().push_back(self.index);
        self.server.staging.permits.add_permits(1);
    }
}

/// The local side of one GET, owned by the transfer until the DMA finished.
enum Local {
    /// A range of the staging buffer, inside a chunk the transfer also owns.
    Staging { offset: usize, len: usize },
    /// A range of a caller-owned arena.
    Arena {
        arena: Arc<RegisteredArena>,
        offset: usize,
        len: usize,
    },
    /// Several ranges of a caller-owned arena, sent as one operation.
    ArenaList {
        arena: Arc<RegisteredArena>,
        entries: Vec<SgEntry>,
    },
}

impl Local {
    fn len(&self) -> usize {
        match self {
            Self::Staging { len, .. } | Self::Arena { len, .. } => *len,
            Self::ArenaList { entries, .. } => entries.iter().map(|entry| entry.size).sum(),
        }
    }
}

/// Why an asynchronous GET did not complete.
enum AsyncFailure {
    /// The library has no completion descriptor; nothing was submitted.
    Unavailable,
    /// The GET failed, or was never submitted; nothing is in flight.
    Failed(CuObjectError),
    /// Waiting failed after submission: the NIC may still read the memory.
    InFlight(CuObjectError),
}

impl From<cuobject::Error> for AsyncFailure {
    fn from(error: cuobject::Error) -> Self {
        Self::Failed(error.into())
    }
}

/// The handle of the one GET in flight on an asynchronous channel.
const ASYNC_HANDLE: NonZeroUsize = NonZeroUsize::MIN;

impl CuObjectServer {
    /// Creates the server and its registered staging pool.
    pub fn new(addr: IpAddr, config: &CuObjectConfig) -> Result<Arc<Self>, CuObjectError> {
        if config.chunk_bytes == 0 || config.chunk_bytes > MAX_RDMA_OP_BYTES || config.pool_bytes < config.chunk_bytes {
            return Err(CuObjectError::Invalid("invalid staging configuration"));
        }
        let mut options = ServerOptions::default();
        // A non-positive count keeps the library default.
        options.num_dcis = u32::try_from(config.num_dcis).unwrap_or(0);
        options.cq_depth = config.cq_depth;
        let server = Arc::new(Server::new(addr, config.port, &options)?);
        let chunks = config.pool_bytes / config.chunk_bytes;
        let buffer = RegisteredBuffer::new(Arc::clone(&server), chunks * config.chunk_bytes)?;
        Ok(Arc::new(Self {
            channels: Arc::new(Semaphore::new(usize::try_from(config.num_dcis).unwrap_or(1).max(1))),
            async_transfers: config.async_transfers,
            server,
            staging: Staging {
                buffer,
                free: parking_lot::Mutex::new((0..chunks).collect()),
                permits: Semaphore::new(chunks),
                chunk_bytes: config.chunk_bytes,
            },
            addr,
            port: config.port,
        }))
    }

    pub fn addr(&self) -> IpAddr {
        self.addr
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn chunk_bytes(&self) -> usize {
        self.staging.chunk_bytes
    }

    /// Reserves one staging chunk, waiting up to `timeout` for a free one.
    pub async fn reserve(self: &Arc<Self>, timeout: Duration) -> Result<StagingChunk, CuObjectError> {
        let permit = tokio::time::timeout(timeout, self.staging.permits.acquire())
            .await
            .map_err(|_| CuObjectError::PoolExhausted)?
            .map_err(|_| CuObjectError::PoolExhausted)?;
        permit.forget();
        let index = self.staging.free.lock().pop_front().ok_or(CuObjectError::PoolExhausted)?;
        Ok(StagingChunk {
            server: Arc::clone(self),
            index,
            len: 0,
        })
    }

    /// Reserves one staging chunk without waiting; `None` when the pool is empty.
    pub fn try_reserve(self: &Arc<Self>) -> Option<StagingChunk> {
        let permit = self.staging.permits.try_acquire().ok()?;
        permit.forget();
        let index = self.staging.free.lock().pop_front()?;
        Some(StagingChunk {
            server: Arc::clone(self),
            index,
            len: 0,
        })
    }

    /// Staging chunks currently free in the pool (a hint; another reservation may win).
    pub fn available_chunks(&self) -> usize {
        self.staging.permits.available_permits()
    }

    /// Registers `len` bytes of zero-filled, page-aligned host memory with this
    /// server for callers that manage the memory themselves (a shard arena the
    /// decoder reads into and the server RDMA-writes from without staging).
    pub fn register_arena(self: &Arc<Self>, len: usize) -> Result<Arc<RegisteredArena>, CuObjectError> {
        let buffer = RegisteredBuffer::new(Arc::clone(&self.server), len)?;
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Ok(Arc::new(RegisteredArena {
            buffer,
            server: Arc::clone(self),
            id: NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }))
    }

    /// RDMA-writes the staged bytes of `chunk` to `token.remote_addr +
    /// remote_offset`. The transfer owns the chunk until the DMA finished, so
    /// cancelling the awaiting future can never release the staging slot under
    /// an in-flight write. Returns the chunk with the byte count for reuse.
    pub async fn write_to_client_owned(
        self: &Arc<Self>,
        key: &str,
        token: &RdmaToken,
        remote_offset: u64,
        chunk: StagingChunk,
    ) -> Result<(u64, StagingChunk), CuObjectError> {
        if chunk.is_empty() {
            return Ok((0, chunk));
        }
        let local = Local::Staging {
            offset: chunk.start(),
            len: chunk.len(),
        };
        self.get_owned(key, token, remote_offset, local, chunk).await
    }

    /// RDMA-writes `source.len` bytes at `source.local_offset` of
    /// `source.arena` to `token.remote_addr + remote_offset`, keeping `keep`
    /// alive until the DMA finished (the caller passes whatever owns the arena
    /// bytes). The arena must have been registered with this server.
    pub async fn write_to_client_from_arena_owned<T: Send + 'static>(
        self: &Arc<Self>,
        key: &str,
        token: &RdmaToken,
        remote_offset: u64,
        source: ArenaRange,
        keep: T,
    ) -> Result<(u64, T), CuObjectError> {
        if source.len == 0 {
            return Ok((0, keep));
        }
        let local = Local::Arena {
            arena: source.arena,
            offset: source.local_offset,
            len: source.len,
        };
        self.get_owned(key, token, remote_offset, local, keep).await
    }

    /// RDMA-writes all of `source.ranges` (in order, as one operation) to
    /// `token.remote_addr + remote_offset`, keeping `keep` alive until the DMA
    /// finished. The arena must have been registered with this server.
    pub async fn write_to_client_sg_owned<T: Send + 'static>(
        self: &Arc<Self>,
        key: &str,
        token: &RdmaToken,
        remote_offset: u64,
        source: ArenaRanges,
        keep: T,
    ) -> Result<(u64, T), CuObjectError> {
        if source.ranges.len() > MAX_SG_RANGES {
            return Err(CuObjectError::Invalid("too many scatter-gather ranges"));
        }
        if source.total_len() == 0 {
            return Ok((0, keep));
        }
        let entries = source
            .ranges
            .iter()
            .map(|&(offset, size)| SgEntry {
                offset: offset as u64,
                size,
            })
            .collect();
        let local = Local::ArenaList {
            arena: source.arena,
            entries,
        };
        self.get_owned(key, token, remote_offset, local, keep).await
    }

    /// RDMA-reads `len` bytes from `token.remote_addr + remote_offset` into the
    /// (empty) `chunk`, owning it for the whole DMA like
    /// [`Self::write_to_client_owned`]. On success the chunk holds `len` staged
    /// bytes.
    pub async fn read_from_client_owned(
        self: &Arc<Self>,
        key: &str,
        token: &RdmaToken,
        remote_offset: u64,
        len: usize,
        mut chunk: StagingChunk,
    ) -> Result<StagingChunk, CuObjectError> {
        chunk.clear();
        if len == 0 {
            return Ok(chunk);
        }
        if len > chunk.capacity() {
            return Err(CuObjectError::Invalid("read exceeds the staging chunk"));
        }
        let server = Arc::clone(self);
        let key = key.to_owned();
        let token = token.inner.clone();
        let channel_permit = self.channel_permit().await?;
        tokio::task::spawn_blocking(move || {
            let _channel_permit = channel_permit;
            let mut channel = Channel::lease(Arc::clone(&server.server))?;
            let moved = channel.put(&key, &token, remote_offset, &server.staging.buffer, chunk.start(), len)?;
            check_moved(moved as u64, len as u64)?;
            chunk.advance(len)?;
            Ok(chunk)
        })
        .await
        .map_err(|error| join_error("read task", error))?
    }

    async fn channel_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, CuObjectError> {
        Arc::clone(&self.channels)
            .acquire_owned()
            .await
            .map_err(|_| CuObjectError::Invalid("server closed"))
    }

    /// RDMA-writes `local` to the client and hands `keep` back once the DMA
    /// finished. The transfer runs in a task of its own that owns `local`,
    /// `keep` and the channel, so dropping this future never releases memory
    /// the NIC may still read.
    async fn get_owned<T: Send + 'static>(
        self: &Arc<Self>,
        key: &str,
        token: &RdmaToken,
        remote_offset: u64,
        local: Local,
        keep: T,
    ) -> Result<(u64, T), CuObjectError> {
        let expected = local.len() as u64;
        let server = Arc::clone(self);
        let key = key.to_owned();
        let token = token.inner.clone();
        let channel_permit = self.channel_permit().await?;
        tokio::spawn(async move {
            if server.async_transfers {
                match server.get_async(&key, &token, remote_offset, &local).await {
                    Ok(()) => {
                        drop(channel_permit);
                        return Ok((expected, keep));
                    }
                    Err(AsyncFailure::Unavailable) => {}
                    Err(AsyncFailure::Failed(error)) => return Err(error),
                    Err(AsyncFailure::InFlight(error)) => {
                        // The NIC may still read this memory: never release it.
                        std::mem::forget(local);
                        std::mem::forget(keep);
                        return Err(error);
                    }
                }
            }
            tokio::task::spawn_blocking(move || {
                let _channel_permit = channel_permit;
                let mut channel = Channel::lease(Arc::clone(&server.server))?;
                let mut list = None;
                let source = server.source(&local, &mut list)?;
                let moved = channel.get(&key, &token, remote_offset, source)?;
                check_moved(moved as u64, expected)?;
                // `keep` (the chunk or the arena owner) is released only here.
                Ok((expected, keep))
            })
            .await
            .map_err(|error| join_error("write task", error))?
        })
        .await
        .map_err(|error| join_error("write task", error))?
    }

    /// Moves `local` with the library's asynchronous mode: the GET is
    /// submitted on a channel of its own and the task waits on that channel's
    /// completion descriptor instead of holding a blocking thread.
    // SAFETY: the allowance covers only this function; its two unsafe calls
    // each state the invariant they rely on.
    #[allow(unsafe_code)]
    async fn get_async(
        &self,
        key: &str,
        token: &cuobject::RdmaToken,
        remote_offset: u64,
        local: &Local,
    ) -> Result<(), AsyncFailure> {
        let mut channel = Channel::lease(Arc::clone(&self.server))?;
        let fd = match channel.completion_fd() {
            Ok(fd) => fd,
            Err(cuobject::Error::AsyncUnavailable(_)) => return Err(AsyncFailure::Unavailable),
            Err(error) => return Err(error.into()),
        };
        // SAFETY: the library owns the descriptor and keeps it open while the
        // server lives; `fd` holds the server, and nothing here closes or
        // replaces the descriptor while it is registered.
        let completions = unsafe { AsyncFd::register_with_interest(fd, Interest::READABLE) }.map_err(|error| {
            AsyncFailure::Failed(CuObjectError::Os {
                op: "completion fd",
                source: error.into_parts().1,
            })
        })?;
        let mut list = None;
        let source = self.source(local, &mut list).map_err(AsyncFailure::Failed)?;
        // SAFETY: the memory behind `source` belongs to `local`, owned by the
        // calling task together with the transfer's `keep`; staging chunks and
        // arena ranges given to a transfer are not borrowed by anyone else.
        // This function returns only after the completion carrying the handle
        // was drained, except through `InFlight`, where the channel and the
        // list are leaked here and the caller leaks `local` and `keep`.
        unsafe { channel.get_async(key, token, remote_offset, source, ASYNC_HANDLE) }?;
        let mut events = [Completion::default(); 8];
        loop {
            let mut ready = match completions.readable().await {
                Ok(ready) => ready,
                Err(source) => {
                    std::mem::forget(list);
                    std::mem::forget(channel);
                    return Err(AsyncFailure::InFlight(CuObjectError::Os {
                        op: "completion wait",
                        source,
                    }));
                }
            };
            loop {
                let landed = match channel.process_completions(&mut events) {
                    Ok(landed) => landed,
                    Err(error) => {
                        std::mem::forget(list);
                        std::mem::forget(channel);
                        return Err(AsyncFailure::InFlight(error.into()));
                    }
                };
                if let Some(done) = events[..landed].iter().find(|event| event.handle() == ASYNC_HANDLE.get()) {
                    // A completion carries no byte count: success moved the whole range.
                    return done
                        .result()
                        .map_err(|source| AsyncFailure::Failed(CuObjectError::Transfer { verbs_status: 0, source }));
                }
                if landed < events.len() {
                    break;
                }
            }
            ready.clear_ready();
        }
    }

    /// The library view of `local`; a scatter-gather list is built into `list`.
    fn source<'a: 'l, 'l>(
        &'a self,
        local: &'a Local,
        list: &'l mut Option<SgBuffer<'a, Arc<Server>>>,
    ) -> Result<Source<'l, Arc<Server>>, CuObjectError> {
        Ok(match local {
            Local::Staging { offset, len } => Source::Range {
                buffer: &self.staging.buffer,
                offset: *offset,
                len: *len,
            },
            Local::Arena { arena, offset, len } => Source::Range {
                buffer: &arena.buffer,
                offset: *offset,
                len: *len,
            },
            Local::ArenaList { arena, entries } => Source::List(list.insert(arena.buffer.sg_buffer(entries)?)),
        })
    }
}

fn check_moved(moved: u64, expected: u64) -> Result<u64, CuObjectError> {
    if moved != expected {
        return Err(CuObjectError::ShortTransfer { moved, expected });
    }
    Ok(moved)
}

fn join_error(op: &'static str, error: tokio::task::JoinError) -> CuObjectError {
    CuObjectError::Os {
        op,
        source: std::io::Error::other(error),
    }
}

/// Copies owned bytes into staging; convenience for callers holding `Bytes`.
pub fn fill_chunk(chunk: &mut StagingChunk, data: &Bytes) -> Result<(), CuObjectError> {
    chunk.append(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_parses_leading_fields_and_rejects_garbage() {
        let token =
            RdmaToken::parse("0000f52eddc00000:00100000:001824d2:0000:002873:1:00000000000000000000ffff0a0d5410").expect("token");
        assert_eq!(token.remote_addr, 0x0000_f52e_ddc0_0000);
        assert_eq!(token.buffer_len, 0x0010_0000);
        assert!(matches!(RdmaToken::parse("nothex:00100000:1:2:3:4:5"), Err(CuObjectError::Token(_))));
        assert!(RdmaToken::parse("0000f52eddc00000:00000000:1:2:3:4:5").is_err());
        assert!(RdmaToken::parse("0000f52eddc00000:00100000").is_err());
    }
}
