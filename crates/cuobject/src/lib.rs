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
//! This crate binds the NVIDIA server SDK directly and owns
//! RustFS's pool of registered, page-aligned host staging buffers: the regular
//! read path fills a staging chunk and
//! [`CuObjectServer::write_to_client_owned`] pushes it.
//!
//! Everything below needs the `server` feature, which compiles the
//! NVIDIA cuObject server bindings; without it the crate is empty, so a
//! workspace build does not need the cuObject SDK.
#![cfg(feature = "server")]

use bytes::Bytes;
// SAFETY: the private bridge declares the SDK ABI and channel ownership rules.
#[allow(unsafe_code)]
mod sdk;
use cxx::SharedPtr;
use sdk::{Channel, ffi};
use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{future::Future, mem::ManuallyDrop};
use thiserror::Error;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Header carrying the client's RDMA descriptor on a request.
pub const RDMA_TOKEN_HEADER: &str = "x-amz-rdma-token";
/// Header carrying the transfer status on the response (HTTP-style code).
pub const RDMA_REPLY_HEADER: &str = "x-amz-rdma-reply";
/// Header carrying the payload byte count moved over RDMA.
pub const RDMA_BYTES_TRANSFERRED_HEADER: &str = "x-amz-rdma-bytes-transferred";
/// Reply code for a declined transfer (the client then errors or falls back).
pub const RDMA_REPLY_NOT_IMPLEMENTED: u16 = 501;
/// Largest single `handleGetObject` the library accepts.
pub const MAX_RDMA_OP_BYTES: usize = 1 << 30;
/// Default RDMA control port of the cuObject server.
pub const DEFAULT_PORT: u16 = 18515;
/// Most ranges one scatter-gather transfer may carry (the library's `max_sge`).
pub const MAX_SG_RANGES: usize = 10;

/// Version of the cuObject server headers the bindings were built against, as
/// (major, minor).
pub fn library_version() -> (u32, u32) {
    let version = ffi::header_version();
    (version / 1000, version % 1000)
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
    #[error("cuobject native transfer submission outcome is unknown: {0}")]
    SubmissionUnknown(#[source] std::io::Error),
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

/// Parsed `x-amz-rdma-token`.
///
/// Wire format (cuObject RDMA descriptor protocol): colon-separated hex
/// fields `addr:size:rkey:lid:dctn:gid_present:gid`, optionally followed
/// by a second GID slot. The SDK receives the original descriptor.
#[derive(Debug, Clone)]
pub struct RdmaToken {
    inner: Arc<Descriptor>,
    /// Start of the client buffer (a copy of the parsed field).
    pub remote_addr: u64,
    /// Length of the client buffer (a copy of the parsed field).
    pub buffer_len: u64,
}

#[derive(Debug)]
struct Descriptor {
    raw: String,
    address: u64,
    size: u64,
}

impl RdmaToken {
    pub fn parse(raw: &str) -> Result<Self, CuObjectError> {
        let raw = raw.trim();
        let mut fields = raw.split(':');
        let mut hex = || {
            let field = fields.next()?;
            field.as_bytes().first().filter(|byte| byte.is_ascii_hexdigit())?;
            u64::from_str_radix(field, 16).ok()
        };
        let address = hex().ok_or_else(|| CuObjectError::Token("invalid address".into()))?;
        let size = hex()
            .filter(|&s| s > 0)
            .ok_or_else(|| CuObjectError::Token("invalid length".into()))?;
        if hex().and_then(|key| u32::try_from(key).ok()).is_none()
            || hex().and_then(|lid| u16::try_from(lid).ok()).is_none()
            || hex().and_then(|dctn| u32::try_from(dctn).ok()).is_none()
        {
            return Err(CuObjectError::Token("invalid routing fields".into()));
        }
        let mut routes = fields.peekable();
        if !valid_gid_slot(routes.next(), routes.next())
            || (routes.peek().is_some() && !valid_gid_slot(routes.next(), routes.next()))
            || routes.next().is_some()
        {
            return Err(CuObjectError::Token("invalid GID".into()));
        }
        Ok(Self {
            inner: Arc::new(Descriptor {
                raw: raw.into(),
                address,
                size,
            }),
            remote_addr: address,
            buffer_len: size,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.inner.raw
    }
}

/// LID-only descriptors can omit the GID or carry a fixed-width placeholder.
fn valid_gid_slot(flag: Option<&str>, gid: Option<&str>) -> bool {
    match (flag, gid) {
        (Some("0"), None | Some("")) => true,
        (Some("0" | "1"), Some(gid)) => gid.len() == 32 && gid.bytes().all(|byte| byte.is_ascii_hexdigit()),
        _ => false,
    }
}

impl Descriptor {
    fn address(&self, offset: u64, length: usize) -> Result<u64, CuObjectError> {
        if length == 0 || length > MAX_RDMA_OP_BYTES {
            return Err(CuObjectError::Invalid("invalid RDMA transfer length"));
        }
        let needed = offset
            .checked_add(length as u64)
            .ok_or(CuObjectError::Invalid("remote range overflow"))?;
        if needed > self.size {
            return Err(CuObjectError::TokenTooSmall {
                buffer: self.size,
                needed,
            });
        }
        self.address
            .checked_add(needed - 1)
            .ok_or(CuObjectError::Invalid("remote address overflow"))?;
        Ok(self.address + offset)
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
    buffer: SharedPtr<ffi::Buffer>,
    free: parking_lot::Mutex<VecDeque<usize>>,
    permits: Semaphore,
    chunk_bytes: usize,
}

/// One cuObject server bound to the RDMA device that owns an IP address.
pub struct CuObjectServer {
    server: SharedPtr<ffi::Server>,
    staging: Staging,
    addr: IpAddr,
    port: u16,
    /// One permit per library channel (DC initiator): a transfer waits here
    /// instead of failing with `EBUSY` when every channel is in use.
    channels: Arc<ChannelCapacity>,
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
        self.ranges.iter().fold(0usize, |total, (_, len)| total.saturating_add(*len))
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
    buffer: SharedPtr<ffi::Buffer>,
    server: Arc<CuObjectServer>,
    id: u64,
}

impl RegisteredArena {
    /// Length of the registered memory.
    pub fn len(&self) -> usize {
        ffi::size(&self.buffer)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        ffi::data(&self.buffer)
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
        if data.len() > self.capacity() - self.len {
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
    pub fn staged(&self) -> Result<&[u8], CuObjectError> {
        Ok(self.as_ref())
    }

    /// The unfilled remainder of the chunk, for a reader to fill directly.
    #[allow(unsafe_code)]
    pub fn spare_mut(&mut self) -> Result<&mut [u8], CuObjectError> {
        let len = self.capacity() - self.len;
        // SAFETY: the range lies in this chunk (see the type docs) and
        // `&mut self` excludes every other borrow of it.
        Ok(unsafe { std::slice::from_raw_parts_mut(ffi::data(&self.server.staging.buffer).add(self.start() + self.len), len) })
    }

    /// Marks `n` more bytes of the spare area as staged.
    pub fn advance(&mut self, n: usize) -> Result<(), CuObjectError> {
        if n > self.capacity() - self.len {
            return Err(CuObjectError::Invalid("staging advance exceeds chunk"));
        }
        self.len += n;
        Ok(())
    }

    fn start(&self) -> usize {
        self.index * self.capacity()
    }
}

impl AsRef<[u8]> for StagingChunk {
    #[allow(unsafe_code)]
    fn as_ref(&self) -> &[u8] {
        // SAFETY: the chunk owns this range of the registration. A shared borrow
        // excludes mutation and DMA reads into the chunk require owned access.
        unsafe { std::slice::from_raw_parts(ffi::data(&self.server.staging.buffer).add(self.start()), self.len) }
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
    ArenaList(ArenaRanges),
}

impl CuObjectServer {
    /// Creates the server and its registered staging pool.
    pub fn new(addr: IpAddr, config: &CuObjectConfig) -> Result<Arc<Self>, CuObjectError> {
        if config.chunk_bytes == 0 || config.chunk_bytes > MAX_RDMA_OP_BYTES || config.pool_bytes < config.chunk_bytes {
            return Err(CuObjectError::Invalid("invalid staging configuration"));
        }
        let server = ffi::open_server(&addr.to_string(), config.port, config.num_dcis, config.cq_depth)
            .map_err(|e| sdk::failure("open server", e))?;
        let chunks = config.pool_bytes / config.chunk_bytes;
        let buffer = ffi::register_buffer(server.clone(), chunks * config.chunk_bytes)
            .map_err(|e| sdk::failure("register staging memory", e))?;
        Ok(Arc::new(Self {
            channels: Arc::new(ChannelCapacity::new(usize::try_from(config.num_dcis).unwrap_or(1).max(1))),
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
        let buffer = ffi::register_buffer(self.server.clone(), len).map_err(|e| sdk::failure("register arena", e))?;
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
        if !Arc::ptr_eq(self, &chunk.server) {
            return Err(CuObjectError::Invalid("chunk belongs to another server"));
        }
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
        if source.ranges.iter().all(|&(_, size)| size == 0) {
            return Ok((0, keep));
        }
        let local = Local::ArenaList(source);
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
        if !Arc::ptr_eq(self, &chunk.server) {
            return Err(CuObjectError::Invalid("chunk belongs to another server"));
        }
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
            let region = Region {
                buffer: server.staging.buffer.clone(),
                offset: chunk.start(),
                length: len,
            };
            let transfer = Transfer {
                capacity: Arc::clone(&server.channels),
                channel: Channel::lease(&server.server)?,
                region,
                keep: chunk,
                _permit: channel_permit,
            };
            let (_, mut chunk) = transfer.blocking(&key, &token, remote_offset, Mode::Put)?;
            chunk.advance(len)?;
            Ok(chunk)
        })
        .await
        .map_err(|error| join_error("read task", error))?
    }

    async fn channel_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, CuObjectError> {
        Arc::clone(&self.channels.permits)
            .acquire_owned()
            .await
            .map_err(|_| CuObjectError::Invalid("no usable RDMA channels remain"))
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
        let server = Arc::clone(self);
        let key = key.to_owned();
        let token = token.inner.clone();
        let permit = self.channel_permit().await?;
        let prepare = move || -> Result<_, CuObjectError> {
            Ok(Transfer {
                capacity: Arc::clone(&server.channels),
                channel: Channel::lease(&server.server)?,
                region: server.region(&local)?,
                keep,
                _permit: permit,
            })
        };
        if !self.async_transfers {
            return tokio::task::spawn_blocking(move || prepare()?.blocking(&key, &token, remote_offset, Mode::Get))
                .await
                .map_err(|error| join_error("write task", error))?;
        }
        tokio::spawn(async move {
            let mut transfer = prepare()?;
            if let Some(fd) = transfer.channel.fd() {
                // SAFETY: the FD is owned by the SDK. The registered object holds
                // its server and the channel remains leased until deregistration.
                #[allow(unsafe_code)]
                let completions =
                    unsafe { AsyncFd::register_with_interest(fd, Interest::READABLE) }.map_err(|e| CuObjectError::Os {
                        op: "completion fd",
                        source: e.into_parts().1,
                    })?;
                if let Err(error) = transfer.submit(&key, &token, remote_offset, Mode::AsyncGet) {
                    // Deregister before the channel lease can be returned and its fd reused.
                    drop(completions);
                    return Err(transfer.submission_failed(error));
                }
                let capacity = Arc::clone(&transfer.capacity);
                let (transfer, status) = retain_until_completion(transfer, capacity, await_completion(&completions)).await?;
                drop(completions);
                if status < 0 {
                    return Err(CuObjectError::Transfer {
                        verbs_status: 0,
                        source: sdk::errno(status),
                    });
                }
                return Ok((transfer.region.length as u64, transfer.keep));
            }
            tokio::task::spawn_blocking(move || transfer.blocking(&key, &token, remote_offset, Mode::Get))
                .await
                .map_err(|error| join_error("write task", error))?
        })
        .await
        .map_err(|error| join_error("write task", error))?
    }

    fn region(&self, local: &Local) -> Result<Region, CuObjectError> {
        if let Local::Arena { arena, .. } | Local::ArenaList(ArenaRanges { arena, .. }) = local
            && !std::ptr::eq(self, arena.server().as_ref())
        {
            return Err(CuObjectError::Invalid("arena belongs to another server"));
        }
        let (buffer, offset, length) = match local {
            Local::Staging { offset, len } => (self.staging.buffer.clone(), *offset, *len),
            Local::Arena { arena, offset, len } => (arena.buffer.clone(), *offset, *len),
            Local::ArenaList(ArenaRanges { arena, ranges }) => {
                if let Some((offset, len)) = contiguous_range(ranges) {
                    (arena.buffer.clone(), offset, len)
                } else {
                    let mut entries = [ffi::Range { offset: 0, size: 0 }; MAX_SG_RANGES];
                    let entries = entries
                        .get_mut(..ranges.len())
                        .ok_or(CuObjectError::Invalid("too many scatter-gather ranges"))?;
                    for (entry, &(offset, size)) in entries.iter_mut().zip(ranges) {
                        *entry = ffi::Range { offset, size };
                    }
                    let list =
                        ffi::scatter_list(arena.buffer.clone(), entries).map_err(|e| sdk::failure("register scatter list", e))?;
                    let size = ffi::size(&list);
                    (list, 0, size)
                }
            }
        };
        Ok(Region { buffer, offset, length })
    }
}

/// A contiguous slice can use the original registration and local offset.
fn contiguous_range(ranges: &[(usize, usize)]) -> Option<(usize, usize)> {
    let &(start, _) = ranges.first()?;
    let mut end = start;
    for &(offset, size) in ranges {
        if end != offset || size == 0 {
            return None;
        }
        end = offset.checked_add(size)?;
    }
    Some((start, end - start))
}

struct ChannelCapacity {
    permits: Arc<Semaphore>,
    usable: AtomicUsize,
}

impl ChannelCapacity {
    fn new(count: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(count)),
            usable: AtomicUsize::new(count),
        }
    }

    fn retire(&self) {
        if self.usable.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.permits.close();
        }
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Get,
    AsyncGet,
    Put,
}

struct Region {
    buffer: SharedPtr<ffi::Buffer>,
    offset: usize,
    length: usize,
}

impl Region {
    fn operation<'a>(
        &self,
        key: &'a str,
        token: &'a Descriptor,
        remote_offset: u64,
        mode: Mode,
    ) -> Result<ffi::Operation<'a>, CuObjectError> {
        if key.contains('\0') {
            return Err(CuObjectError::Invalid("key contains NUL"));
        }
        let end = self
            .offset
            .checked_add(self.length)
            .ok_or(CuObjectError::Invalid("local range overflow"))?;
        if end > ffi::size(&self.buffer) {
            return Err(CuObjectError::Invalid("transfer exceeds registration"));
        }
        Ok(ffi::Operation {
            key,
            token: &token.raw,
            address: token.address(remote_offset, self.length)?,
            offset: self.offset,
            length: self.length,
            channel: 0,
            handle: usize::from(matches!(mode, Mode::AsyncGet)),
            put: matches!(mode, Mode::Put),
        })
    }
}

/// Owns every resource the SDK may access or the allocator could reissue.
struct Transfer<T> {
    capacity: Arc<ChannelCapacity>,
    channel: Channel,
    region: Region,
    keep: T,
    _permit: OwnedSemaphorePermit,
}

impl<T> Transfer<T> {
    fn submit(&mut self, key: &str, token: &Descriptor, offset: u64, mode: Mode) -> Result<u64, CuObjectError> {
        let op = self.region.operation(key, token, offset, mode)?;
        // SAFETY: this owner holds the exclusive range, registration and channel.
        // The async caller retains it through CQ completion. An ambiguous SDK
        // exception retains the entire owner, including its channel permit.
        #[allow(unsafe_code)]
        let outcome = unsafe { self.channel.submit(&self.region.buffer, op) };
        if outcome.uncertain {
            return Err(CuObjectError::SubmissionUnknown(sdk::errno(outcome.count)));
        }
        u64::try_from(outcome.count).map_err(|_| CuObjectError::Transfer {
            verbs_status: outcome.verbs,
            source: sdk::errno(outcome.count),
        })
    }

    fn submission_failed(self, error: CuObjectError) -> CuObjectError {
        if matches!(error, CuObjectError::SubmissionUnknown(_)) {
            self.capacity.retire();
            std::mem::forget(self);
        }
        error
    }

    fn blocking(mut self, key: &str, token: &Descriptor, offset: u64, mode: Mode) -> Result<(u64, T), CuObjectError> {
        let moved = match self.submit(key, token, offset, mode) {
            Ok(moved) => moved,
            Err(error) => return Err(self.submission_failed(error)),
        };
        check_moved(moved, self.region.length as u64)?;
        Ok((moved, self.keep))
    }
}

/// Once submitted, only a completion event permits the channel and registered
/// memory to be reused. Dropping the wait or losing the CQ leaves them pinned.
fn retain_until_completion<T>(
    owner: T,
    capacity: Arc<ChannelCapacity>,
    event: impl Future<Output = Result<isize, CuObjectError>>,
) -> impl Future<Output = Result<(T, isize), CuObjectError>> {
    struct RetireOnLoss(Option<Arc<ChannelCapacity>>);
    impl Drop for RetireOnLoss {
        fn drop(&mut self) {
            if let Some(capacity) = &self.0 {
                capacity.retire();
            }
        }
    }

    let owner = ManuallyDrop::new(owner);
    let mut retirement = RetireOnLoss(Some(capacity));
    async move {
        let status = event.await?;
        retirement.0.take();
        drop(retirement);
        Ok((ManuallyDrop::into_inner(owner), status))
    }
}

async fn await_completion(fd: &AsyncFd<sdk::CompletionFd>) -> Result<isize, CuObjectError> {
    let mut events = [ffi::Completion::default(); 8];
    loop {
        let mut ready = fd.readable().await.map_err(|source| CuObjectError::Os {
            op: "completion wait",
            source,
        })?;
        loop {
            let n = fd.get_ref().drain(&mut events)?;
            if let Some(done) = events[..n].iter().find(|e| e.handle == 1) {
                return Ok(done.status);
            }
            if n < events.len() {
                break;
            }
        }
        ready.clear_ready();
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
    fn token_parses_dc_routing_and_rejects_malformed_fields() {
        let token =
            RdmaToken::parse("0000f52eddc00000:00100000:001824d2:0000:002873:1:00000000000000000000ffff0a0d5410").expect("token");
        assert_eq!(token.remote_addr, 0x0000_f52e_ddc0_0000);
        assert_eq!(token.buffer_len, 0x0010_0000);
        for raw in [
            "1:10:a:b:c:0",
            "1:10:a:b:c:0:",
            "1:10:a:b:c:0:00000000000000000000000000000000",
            "1:10:a:b:c:0::1:00000000000000000000ffff0a0d5410",
        ] {
            assert!(RdmaToken::parse(raw).is_ok(), "{raw}");
        }
        for raw in [
            "nothex:10:1:2:3:0",
            "1:0:1:2:3:0",
            "1:10",
            "1:10::::",
            "1:10:nothex:0:1:0",
            "1:10:100000000:0:1:0",
            "1:10:1:10000:1:0",
            "1:10:1:0:+1:0",
            "1:10:1:0:1:2",
            "1:10:1:0:1:1",
            "1:10:1:0:1:1:face",
            "1:10:1:0:1:1:00000000000000000000ffff0a0d541z",
            "1:10:1:0:1:0::1:",
            "1:10:1:0:1:0::0::extra",
        ] {
            assert!(matches!(RdmaToken::parse(raw), Err(CuObjectError::Token(_))), "{raw}");
        }
    }
    #[test]
    fn validates_the_entire_remote_range_and_uses_the_parsed_descriptor() {
        let mut token = RdmaToken::parse("  fffffffffffffff0:10:a:b:c:0  ").unwrap();
        token.buffer_len = u64::MAX;
        token.remote_addr = 0;
        assert_eq!(token.inner.address(15, 1).unwrap(), u64::MAX);
        assert!(matches!(token.inner.address(15, 2), Err(CuObjectError::TokenTooSmall { .. })));
        let wrapping = RdmaToken::parse("ffffffffffffffff:2:a:b:c:0").unwrap();
        assert!(matches!(wrapping.inner.address(0, 2), Err(CuObjectError::Invalid(_))));
        assert!(token.inner.address(u64::MAX, 2).is_err());
        assert!(token.inner.address(0, MAX_RDMA_OP_BYTES + 1).is_err());
        assert!(RdmaToken::parse("1:10:a:b:c:d\0x").is_err());
    }

    #[test]
    fn direct_ranges_preserve_order_and_require_nonempty_adjacent_slices() {
        assert_eq!(contiguous_range(&[(8, 4), (12, 3)]), Some((8, 7)));
        for ranges in [
            &[][..],
            &[(0, 1), (2, 1)],
            &[(8, 4), (4, 4)],
            &[(8, 4), (12, 0)],
            &[(usize::MAX, 1)],
        ] {
            assert_eq!(contiguous_range(ranges), None);
        }
    }

    #[test]
    fn retiring_the_last_channel_fails_existing_waiters() {
        let capacity = ChannelCapacity::new(1);
        let _used = capacity.permits.try_acquire().expect("first channel");
        let mut waiting = std::pin::pin!(capacity.permits.acquire());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        capacity.retire();
        assert!(matches!(waiting.as_mut().poll(&mut cx), std::task::Poll::Ready(Err(_))));
    }

    #[test]
    fn only_delivered_completions_release_dma_owners() {
        use std::{
            future,
            sync::atomic::{AtomicUsize, Ordering},
        };
        struct Owner<'a>(&'a AtomicUsize);
        impl Drop for Owner<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let freed = AtomicUsize::new(0);
        let capacity = Arc::new(ChannelCapacity::new(3));
        use std::task::{Context, Poll, Waker};
        let mut ctx = Context::from_waker(Waker::noop());
        drop(retain_until_completion(Owner(&freed), Arc::clone(&capacity), future::pending()));
        {
            let mut waiting = std::pin::pin!(retain_until_completion(Owner(&freed), Arc::clone(&capacity), future::pending()));
            assert!(waiting.as_mut().poll(&mut ctx).is_pending());
        }
        assert!(!capacity.permits.is_closed());
        let refused =
            retain_until_completion(Owner(&freed), Arc::clone(&capacity), future::ready(Err(CuObjectError::PoolExhausted)));
        assert!(matches!(std::pin::pin!(refused).as_mut().poll(&mut ctx), Poll::Ready(Err(_))));
        assert_eq!(freed.load(Ordering::SeqCst), 0);
        assert!(capacity.permits.is_closed());
        let capacity = Arc::new(ChannelCapacity::new(1));
        for status in [0, -5] {
            let completed = retain_until_completion(Owner(&freed), Arc::clone(&capacity), future::ready(Ok(status)));
            let Poll::Ready(Ok((owner, observed))) = std::pin::pin!(completed).as_mut().poll(&mut ctx) else {
                panic!("expected completion");
            };
            assert_eq!(observed, status);
            drop(owner);
        }
        assert_eq!(freed.load(Ordering::SeqCst), 2);
        assert!(!capacity.permits.is_closed());
    }
    /// Set CUOBJECT_TEST_ADDR and CUOBJECT_TEST_PORT to a free RDMA endpoint.
    #[test]
    #[ignore = "requires an NVIDIA-supported RDMA device and a free listener port"]
    fn native_staging_and_scatter_views_keep_their_registration() {
        let addr = std::env::var("CUOBJECT_TEST_ADDR").unwrap().parse().unwrap();
        let port = std::env::var("CUOBJECT_TEST_PORT").unwrap().parse().unwrap();
        let server = CuObjectServer::new(
            addr,
            &CuObjectConfig {
                port,
                num_dcis: 2,
                cq_depth: 64,
                chunk_bytes: 4096,
                pool_bytes: 8192,
                async_transfers: true,
            },
        )
        .unwrap();
        let mut first = server.try_reserve().unwrap();
        let mut second = server.try_reserve().unwrap();
        assert!(server.try_reserve().is_none());
        first.append(b"first").unwrap();
        second.append(b"second").unwrap();
        assert_eq!(first.staged().unwrap(), b"first");
        assert_eq!(second.staged().unwrap(), b"second");
        assert!(second.advance(usize::MAX).is_err());
        let addr = first.as_ref().as_ptr();
        let owned = Bytes::from_owner(first);
        let part = owned.slice(1..);
        assert_eq!(owned.as_ptr(), addr);
        drop(owned);
        drop(second);
        assert_eq!(&part[..], b"irst");
        assert_eq!(server.available_chunks(), 1);
        drop(part);
        assert_eq!(server.available_chunks(), 2);
        let arena = server.register_arena(8192).unwrap();
        for ranges in [vec![(8, 40)], vec![(8, 17), (25, 23)]] {
            let local = Local::ArenaList(ArenaRanges {
                arena: Arc::clone(&arena),
                ranges,
            });
            let region = server.region(&local).expect("contiguous arena range");
            assert!(std::ptr::eq(&*region.buffer, &*arena.buffer));
            assert_eq!((region.offset, region.length), (8, 40));
        }
        let local = Local::ArenaList(ArenaRanges {
            arena,
            ranges: vec![(4096, 17), (1, 23)],
        });
        let region = server.region(&local).unwrap();
        drop(local);
        drop(server);
        assert_eq!(ffi::size(&region.buffer), 40);
    }
}
