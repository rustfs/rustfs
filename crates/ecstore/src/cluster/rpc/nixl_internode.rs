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

//! Internode shard reads whose bytes move over NIXL/UCX.
//!
//! The reading node keeps a pool of receive buffers registered with its NIXL
//! agent. For every chunk of a remote shard stream it sends a signed request
//! over the existing internode HTTP control plane naming the shard range, its
//! agent, one registered buffer and a digest of its agent metadata; the serving
//! node reads the range into its own registered staging memory and writes it
//! into that buffer with a NIXL WRITE, then answers with the byte count. The
//! buffer reaches the decoder as a zero-copy `Bytes`. Several chunk requests
//! run per stream so request latency overlaps the transfers.
//!
//! UCX addresses ports by LID or GID, so this works on InfiniBand rails that
//! have no IP addresses. Writes, walks and any stream whose first chunk cannot
//! be served this way use the TCP/HTTP transport.
//!
//! Buffer safety: the serving node may still write into a buffer whose request
//! was abandoned (a dropped reader, chunks past the end of the file). Such
//! buffers are held for [`ABANDONED_BUFFER_HOLD`] before they return to the
//! pool, which covers the serving node's transfer timeout.

use super::internode_data_transport::{
    InternodeDataTransport, InternodeDataTransportCapabilities, NsScannerCapabilityRequest, NsScannerStreamRequest,
    ReadStreamRequest, TcpHttpInternodeDataTransport, WalkDirStreamRequest, WriteStreamRequest, build_read_file_stream_url,
    json_headers,
};
use crate::cluster::rpc::build_auth_headers;
use crate::disk::error::Result;
use crate::disk::{FileReader, FileWriter};
use crate::storage_api_contracts::internode::NsScannerCapabilityResponse;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{FuturesOrdered, Stream};
use http::{HeaderValue, Method};
use rustfs_config::{DEFAULT_RUSTFS_NIXL_INTERNODE_DEPTH, ENV_RUSTFS_NIXL_INTERNODE_DEPTH};
use rustfs_rio::{ChunkReader, ChunkReaderBox};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tracing::debug;
use uuid::Uuid;

/// Internode route of a NIXL chunk read.
pub const READ_FILE_NIXL_PATH: &str = "/rustfs/rpc/read_file_nixl";
/// Internode route of a NIXL shard write: the disk's node pulls the named buffers.
pub const WRITE_FILE_NIXL_PATH: &str = "/rustfs/rpc/write_file_nixl";
/// Comma-separated buffers a NIXL shard write pulls, in file order (signed).
pub const NIXL_BUFFERS_QUERY: &str = "nixl_buffers";
/// One-time nonce of a NIXL shard write (signed; recorded to refuse replays).
pub const NIXL_NONCE_QUERY: &str = "nixl_nonce";
/// Buffers a writer fills before it sends a segment; a part's shard file
/// usually fits in one.
const WRITE_SEGMENT_BUFFERS: usize = 4;
/// The route the URL builder emits, replaced by [`READ_FILE_NIXL_PATH`].
const READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/read_file_stream";
/// Query parameters of a NIXL chunk read (signed with the URL).
pub const NIXL_AGENT_QUERY: &str = "nixl_agent";
pub const NIXL_BUFFER_QUERY: &str = "nixl_buffer";
pub const NIXL_METADATA_SHA256_QUERY: &str = "nixl_md";
/// The reader's agent metadata, base64; must hash to the signed digest.
pub const NIXL_METADATA_HEADER: &str = "x-rustfs-nixl-metadata";
/// Bytes the serving node wrote into the buffer; fewer than asked means end of file.
pub const NIXL_BYTES_HEADER: &str = "x-rustfs-nixl-bytes";

/// How long a buffer whose request was abandoned stays out of the pool.
pub const ABANDONED_BUFFER_HOLD: Duration = Duration::from_secs(120);
/// Request timeout when the caller sets no stall timeout.
const DEFAULT_CHUNK_TIMEOUT: Duration = Duration::from_secs(60);
const EVENT_NIXL_INTERNODE_FALLBACK: &str = "nixl_internode_fallback";
const LOG_COMPONENT_INTERNODE: &str = "internode";
const LOG_SUBSYSTEM_NIXL: &str = "nixl";

/// Receive buffers registered with this node's NIXL agent.
pub trait NixlRecvPool: Send + Sync + 'static {
    /// This node's agent name.
    fn agent_name(&self) -> &str;
    /// This node's agent metadata, base64.
    fn metadata_base64(&self) -> &str;
    /// Lowercase hex SHA-256 of the raw agent metadata.
    fn metadata_sha256_hex(&self) -> &str;
    /// Size of one receive buffer; one chunk request moves at most this much.
    fn chunk_bytes(&self) -> usize;
    /// One free buffer, waiting for one when the pool is busy.
    fn reserve(&self) -> BoxFuture<'static, io::Result<Box<dyn NixlRecvBuffer>>>;
}

/// One registered buffer, back in the pool when dropped. Reads land in it;
/// writes stage shard bytes in it for the disk's node to pull.
pub trait NixlRecvBuffer: Send + Sync + 'static {
    /// `<dram|vram>:<device>:<address>:<length>` as the serving node parses it.
    fn descriptor(&self) -> String;
    /// The same for the first `len` bytes.
    fn descriptor_for(&self, len: usize) -> String;
    /// The whole buffer, writable.
    fn spare_mut(&mut self) -> &mut [u8];
    /// The first `len` bytes as an owned, zero-copy `Bytes`.
    fn into_bytes(self: Box<Self>, len: usize) -> io::Result<Bytes>;
}

static RECV_POOL: OnceLock<Arc<dyn NixlRecvPool>> = OnceLock::new();

/// Installs this node's receive pool; only the first installation wins.
pub fn install_nixl_recv_pool(pool: Arc<dyn NixlRecvPool>) -> bool {
    RECV_POOL.set(pool).is_ok()
}

fn recv_pool() -> Option<Arc<dyn NixlRecvPool>> {
    RECV_POOL.get().cloned()
}

/// Internode transport that reads shard streams over NIXL and delegates
/// everything else to TCP/HTTP.
#[derive(Debug)]
pub struct NixlInternodeDataTransport {
    tcp: TcpHttpInternodeDataTransport,
    depth: usize,
}

impl NixlInternodeDataTransport {
    pub fn from_env() -> Self {
        Self {
            tcp: TcpHttpInternodeDataTransport,
            depth: rustfs_utils::get_env_usize(ENV_RUSTFS_NIXL_INTERNODE_DEPTH, DEFAULT_RUSTFS_NIXL_INTERNODE_DEPTH).clamp(1, 64),
        }
    }

    /// A NIXL chunk reader for `request` with its first chunk already landed,
    /// or `None` when this read should use TCP/HTTP.
    async fn open_nixl(&self, request: &ReadStreamRequest) -> Option<NixlChunkReader> {
        let pool = recv_pool()?;
        if request.length == 0 {
            // Unbounded reads need the file size; TCP/HTTP streams them.
            return None;
        }
        let mut reader = NixlChunkReader::new(pool, request.clone(), self.depth);
        match futures::future::poll_fn(|cx| reader.state_mut().poll_chunk(cx, usize::MAX)).await {
            Ok(Some(first)) => {
                reader.state_mut().current = first;
                Some(reader)
            }
            Ok(None) => Some(reader),
            Err(error) => {
                debug!(
                    event = EVENT_NIXL_INTERNODE_FALLBACK,
                    component = LOG_COMPONENT_INTERNODE,
                    subsystem = LOG_SUBSYSTEM_NIXL,
                    result = "fallback",
                    endpoint = %request.endpoint,
                    error = %error,
                    "nixl internode read fell back to tcp"
                );
                None
            }
        }
    }
}

#[async_trait]
impl InternodeDataTransport for NixlInternodeDataTransport {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
        match self.open_nixl(&request).await {
            Some(reader) => Ok(Box::new(reader)),
            None => self.tcp.open_read(request).await,
        }
    }

    async fn open_read_fresh(&self, request: ReadStreamRequest) -> Result<FileReader> {
        match self.open_nixl(&request).await {
            Some(reader) => Ok(Box::new(reader)),
            None => self.tcp.open_read_fresh(request).await,
        }
    }

    async fn open_read_chunks(&self, request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        match self.open_nixl(&request).await {
            Some(reader) => Ok(Some(Box::new(reader))),
            None => self.tcp.open_read_chunks(request).await,
        }
    }

    async fn open_read_chunks_fresh(&self, request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        match self.open_nixl(&request).await {
            Some(reader) => Ok(Some(Box::new(reader))),
            None => self.tcp.open_read_chunks_fresh(request).await,
        }
    }

    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
        match recv_pool() {
            Some(pool) => Ok(Box::new(NixlFileWriter::new(pool, request))),
            None => self.tcp.open_write(request).await,
        }
    }

    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader> {
        self.tcp.open_walk_dir(request).await
    }

    async fn open_ns_scanner(&self, request: NsScannerStreamRequest) -> Result<FileReader> {
        self.tcp.open_ns_scanner(request).await
    }

    async fn probe_ns_scanner(&self, request: NsScannerCapabilityRequest) -> Result<Uuid> {
        self.tcp.probe_ns_scanner(request).await
    }

    async fn probe_ns_scanner_capability(&self, request: NsScannerCapabilityRequest) -> Result<NsScannerCapabilityResponse> {
        self.tcp.probe_ns_scanner_capability(request).await
    }

    fn name(&self) -> &'static str {
        "nixl"
    }

    fn capabilities(&self) -> InternodeDataTransportCapabilities {
        InternodeDataTransportCapabilities::tcp_http()
    }
}

/// A receive buffer lent to one chunk request. Unless the request completed,
/// dropping it holds the buffer out of the pool for [`ABANDONED_BUFFER_HOLD`]
/// because the serving node may still be writing into it.
struct LentBuffer {
    buffer: Option<Box<dyn NixlRecvBuffer>>,
}

impl LentBuffer {
    fn complete(mut self, len: usize) -> io::Result<Bytes> {
        let buffer = self.buffer.take().expect("buffer present until completion");
        buffer.into_bytes(len)
    }

    /// The peer is done with the buffer: back to the pool now.
    fn release(mut self) {
        drop(self.buffer.take());
    }
}

impl Drop for LentBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        tokio::time::sleep(ABANDONED_BUFFER_HOLD).await;
                        drop(buffer);
                    });
                }
                // No runtime to wait on: never hand the memory back.
                Err(_) => std::mem::forget(buffer),
            }
        }
    }
}

/// Fetches one chunk: the requested length and the bytes that landed.
async fn fetch_chunk(pool: Arc<dyn NixlRecvPool>, request: ReadStreamRequest, timeout: Duration) -> io::Result<(usize, Bytes)> {
    let buffer = LentBuffer {
        buffer: Some(pool.reserve().await?),
    };
    let descriptor = buffer.buffer.as_ref().map(|buffer| buffer.descriptor()).unwrap_or_default();
    let mut url = build_read_file_stream_url(&request).replacen(READ_FILE_STREAM_PATH, READ_FILE_NIXL_PATH, 1);
    url.push_str(&format!(
        "&{NIXL_AGENT_QUERY}={}&{NIXL_BUFFER_QUERY}={}&{NIXL_METADATA_SHA256_QUERY}={}",
        urlencoding::encode(pool.agent_name()),
        urlencoding::encode(&descriptor),
        pool.metadata_sha256_hex()
    ));
    let mut headers = json_headers();
    headers.insert(
        NIXL_METADATA_HEADER,
        HeaderValue::from_str(pool.metadata_base64())
            .map_err(|_| io::Error::other("nixl agent metadata is not a header value"))?,
    );
    build_auth_headers(&url, &Method::GET, &mut headers)?;
    let (status, response_headers, _) = rustfs_rio::internode_http_request(&url, Method::GET, headers, Some(timeout)).await?;
    if !status.is_success() {
        debug!(
            component = LOG_COMPONENT_INTERNODE,
            subsystem = LOG_SUBSYSTEM_NIXL,
            result = "failed",
            status = status.as_u16(),
            "nixl internode read refused by peer"
        );
        return Err(io::Error::other("nixl internode read refused by peer"));
    }
    let landed = response_headers
        .get(NIXL_BYTES_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| io::Error::other("nixl internode read reply lacks the byte count"))?;
    if landed > request.length {
        return Err(io::Error::other("nixl internode read reports more bytes than requested"));
    }
    Ok((request.length, buffer.complete(landed)?))
}

type ChunkFuture = BoxFuture<'static, io::Result<(usize, Bytes)>>;

struct ReaderState {
    pool: Arc<dyn NixlRecvPool>,
    base: ReadStreamRequest,
    next: usize,
    end: usize,
    chunk: usize,
    depth: usize,
    timeout: Duration,
    inflight: FuturesOrdered<ChunkFuture>,
    current: Bytes,
    done: bool,
}

impl ReaderState {
    fn refill(&mut self) {
        while !self.done && self.inflight.len() < self.depth && self.next < self.end {
            let len = self.chunk.min(self.end - self.next);
            let mut request = self.base.clone();
            request.offset = self.next;
            request.length = len;
            self.next += len;
            self.inflight
                .push_back(Box::pin(fetch_chunk(Arc::clone(&self.pool), request, self.timeout)));
        }
    }

    fn poll_chunk(&mut self, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
        loop {
            if !self.current.is_empty() {
                let take = self.current.len().min(max.max(1));
                return Poll::Ready(Ok(Some(self.current.split_to(take))));
            }
            if self.done {
                return Poll::Ready(Ok(None));
            }
            self.refill();
            match Pin::new(&mut self.inflight).poll_next(cx) {
                Poll::Ready(Some(Ok((asked, bytes)))) => {
                    if bytes.len() < asked {
                        // End of file: later chunks hold nothing; their buffers
                        // are held back as they are dropped.
                        self.done = true;
                        self.inflight = FuturesOrdered::new();
                    }
                    if bytes.is_empty() {
                        self.done = true;
                        return Poll::Ready(Ok(None));
                    }
                    self.current = bytes;
                }
                Poll::Ready(Some(Err(error))) => {
                    self.done = true;
                    self.inflight = FuturesOrdered::new();
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(Ok(None));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// A remote shard stream read over NIXL in pipelined chunks.
pub struct NixlChunkReader {
    // The mutex only makes the reader `Sync`; every access goes through `&mut self`.
    state: std::sync::Mutex<ReaderState>,
}

impl NixlChunkReader {
    fn new(pool: Arc<dyn NixlRecvPool>, request: ReadStreamRequest, depth: usize) -> Self {
        let chunk = pool.chunk_bytes().max(1);
        let timeout = request.stall_timeout.unwrap_or(DEFAULT_CHUNK_TIMEOUT);
        Self {
            state: std::sync::Mutex::new(ReaderState {
                next: request.offset,
                end: request.offset.saturating_add(request.length),
                pool,
                base: request,
                chunk,
                depth,
                timeout,
                inflight: FuturesOrdered::new(),
                current: Bytes::new(),
                done: false,
            }),
        }
    }

    fn state_mut(&mut self) -> &mut ReaderState {
        self.state.get_mut().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ChunkReader for NixlChunkReader {
    fn poll_read_chunk(self: Pin<&mut Self>, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
        self.get_mut().state_mut().poll_chunk(cx, max)
    }
}

impl AsyncRead for NixlChunkReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        match self.get_mut().state_mut().poll_chunk(cx, buf.remaining()) {
            Poll::Ready(Ok(Some(bytes))) => {
                buf.put_slice(&bytes);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Sends one segment of a shard write: the disk's node pulls `buffers` (in
/// file order) and writes them at the end of the file, creating it unless
/// `append`. The buffers return to the pool only after the node answered.
async fn send_segment(
    pool: Arc<dyn NixlRecvPool>,
    request: WriteStreamRequest,
    buffers: Vec<(LentBuffer, usize)>,
    append: bool,
    last: bool,
    timeout: Duration,
) -> io::Result<()> {
    let total: usize = buffers.iter().map(|(_, len)| *len).sum();
    let descriptors = buffers
        .iter()
        .map(|(buffer, len)| {
            buffer
                .buffer
                .as_ref()
                .map(|buffer| buffer.descriptor_for(*len))
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(",");
    let url = format!(
        "{}{WRITE_FILE_NIXL_PATH}?disk={}&volume={}&path={}&append={append}&size={}&last={last}&{NIXL_NONCE_QUERY}={}&{NIXL_AGENT_QUERY}={}&{NIXL_BUFFERS_QUERY}={}&{NIXL_METADATA_SHA256_QUERY}={}",
        request.endpoint,
        urlencoding::encode(&request.disk),
        urlencoding::encode(&request.volume),
        urlencoding::encode(&request.path),
        request.size,
        Uuid::new_v4(),
        urlencoding::encode(pool.agent_name()),
        urlencoding::encode(&descriptors),
        pool.metadata_sha256_hex()
    );
    let mut headers = json_headers();
    headers.insert(
        NIXL_METADATA_HEADER,
        HeaderValue::from_str(pool.metadata_base64())
            .map_err(|_| io::Error::other("nixl agent metadata is not a header value"))?,
    );
    build_auth_headers(&url, &Method::PUT, &mut headers)?;
    let request_started = rustfs_io_metrics::put_stage_timer();
    let (status, response_headers, body) = rustfs_rio::internode_http_request(&url, Method::PUT, headers, Some(timeout)).await?;
    rustfs_io_metrics::record_put_object_stage_duration_from("nixl_write_segment_request", request_started);
    if !status.is_success() {
        debug!(
            component = LOG_COMPONENT_INTERNODE,
            subsystem = LOG_SUBSYSTEM_NIXL,
            result = "failed",
            status = status.as_u16(),
            body_len = body.len(),
            "nixl internode write refused by peer"
        );
        return Err(io::Error::other("nixl internode write refused by peer"));
    }
    let written = response_headers
        .get(NIXL_BYTES_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());
    if written != Some(total) {
        debug!(
            component = LOG_COMPONENT_INTERNODE,
            subsystem = LOG_SUBSYSTEM_NIXL,
            result = "failed",
            written = ?written,
            total,
            "nixl internode write stored a different byte count"
        );
        return Err(io::Error::other("nixl internode write stored a different byte count"));
    }
    for (buffer, _) in buffers {
        buffer.release();
    }
    Ok(())
}

type SegmentFuture = BoxFuture<'static, io::Result<()>>;
type ReserveFuture = BoxFuture<'static, io::Result<Box<dyn NixlRecvBuffer>>>;

struct WriterState {
    pool: Arc<dyn NixlRecvPool>,
    base: WriteStreamRequest,
    timeout: Duration,
    current: Option<(LentBuffer, usize)>,
    filled: Vec<(LentBuffer, usize)>,
    reserving: Option<ReserveFuture>,
    sending: Option<SegmentFuture>,
    segments_sent: usize,
    shut_down: bool,
}

impl WriterState {
    fn start_segment(&mut self, last: bool) {
        let buffers = std::mem::take(&mut self.filled);
        let append = self.base.append || self.segments_sent > 0;
        self.segments_sent += 1;
        self.sending = Some(Box::pin(send_segment(
            Arc::clone(&self.pool),
            self.base.clone(),
            buffers,
            append,
            last,
            self.timeout,
        )));
    }

    fn poll_sending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.sending.as_mut() {
            None => Poll::Ready(Ok(())),
            Some(sending) => match sending.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    self.sending = None;
                    Poll::Ready(result)
                }
            },
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        if self.shut_down {
            return Poll::Ready(Err(io::Error::other("nixl internode writer is shut down")));
        }
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if let Err(error) = futures::ready!(self.poll_sending(cx)) {
            return Poll::Ready(Err(error));
        }
        while self.current.is_none() {
            let pool = Arc::clone(&self.pool);
            let reserving = self.reserving.get_or_insert_with(|| pool.reserve());
            match reserving.as_mut().poll(cx) {
                Poll::Ready(buffer) => {
                    self.reserving = None;
                    self.current = Some((LentBuffer { buffer: Some(buffer?) }, 0));
                }
                Poll::Pending if !self.filled.is_empty() => {
                    // Send held buffers before waiting for capacity that their acknowledgement releases.
                    self.reserving = None;
                    self.start_segment(false);
                    futures::ready!(self.poll_sending(cx))?;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        let (lent, filled) = self.current.as_mut().expect("buffer reserved above");
        let spare = lent.buffer.as_mut().expect("lent buffer present").spare_mut();
        let n = data.len().min(spare.len() - *filled);
        spare[*filled..*filled + n].copy_from_slice(&data[..n]);
        *filled += n;
        if *filled == spare.len() {
            let full = self.current.take().expect("current buffer");
            self.filled.push(full);
            if self.filled.len() >= WRITE_SEGMENT_BUFFERS {
                self.start_segment(false);
            }
        }
        Poll::Ready(Ok(n))
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.shut_down {
            if let Err(error) = futures::ready!(self.poll_sending(cx)) {
                return Poll::Ready(Err(error));
            }
            if let Some((buffer, filled)) = self.current.take() {
                if filled > 0 {
                    self.filled.push((buffer, filled));
                } else {
                    buffer.release();
                }
            }
            self.shut_down = true;
            if !self.filled.is_empty() || self.segments_sent == 0 {
                // The last segment; with nothing written at all it still creates the file.
                self.start_segment(true);
            }
        }
        self.poll_sending(cx)
    }
}

/// A remote shard file written through NIXL: bytes are staged in registered
/// buffers and the disk's node pulls them, a segment at a time.
pub struct NixlFileWriter {
    // The mutex only makes the writer `Sync`; every access goes through `&mut self`.
    state: std::sync::Mutex<WriterState>,
}

impl NixlFileWriter {
    fn new(pool: Arc<dyn NixlRecvPool>, request: WriteStreamRequest) -> Self {
        Self {
            state: std::sync::Mutex::new(WriterState {
                pool,
                base: request,
                timeout: DEFAULT_CHUNK_TIMEOUT,
                current: None,
                filled: Vec::new(),
                reserving: None,
                sending: None,
                segments_sent: 0,
                shut_down: false,
            }),
        }
    }

    fn state_mut(&mut self) -> &mut WriterState {
        self.state.get_mut().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl tokio::io::AsyncWrite for NixlFileWriter {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        self.get_mut().state_mut().poll_write(cx, data)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Bytes stay staged until a segment fills or the writer shuts down; a
        // flush only waits for a segment already on its way.
        self.get_mut().state_mut().poll_sending(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().state_mut().poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingBuffer {
        released: Arc<AtomicUsize>,
    }

    impl NixlRecvBuffer for CountingBuffer {
        fn descriptor(&self) -> String {
            "dram:0:0x1000:16".to_owned()
        }

        fn descriptor_for(&self, len: usize) -> String {
            format!("dram:0:0x1000:{len}")
        }

        fn spare_mut(&mut self) -> &mut [u8] {
            &mut []
        }

        fn into_bytes(self: Box<Self>, len: usize) -> io::Result<Bytes> {
            Ok(Bytes::from(vec![7u8; len]))
        }
    }

    impl Drop for CountingBuffer {
        fn drop(&mut self) {
            self.released.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A buffer backed by a plain vector, for the writer's staging logic.
    struct VecBuffer(Vec<u8>);

    impl NixlRecvBuffer for VecBuffer {
        fn descriptor(&self) -> String {
            self.descriptor_for(self.0.len())
        }

        fn descriptor_for(&self, len: usize) -> String {
            format!("dram:0:0x2000:{len}")
        }

        fn spare_mut(&mut self) -> &mut [u8] {
            &mut self.0
        }

        fn into_bytes(self: Box<Self>, len: usize) -> io::Result<Bytes> {
            Ok(Bytes::copy_from_slice(&self.0[..len]))
        }
    }

    struct VecPool;

    impl NixlRecvPool for VecPool {
        fn agent_name(&self) -> &str {
            "test-agent"
        }

        fn metadata_base64(&self) -> &str {
            ""
        }

        fn metadata_sha256_hex(&self) -> &str {
            ""
        }

        fn chunk_bytes(&self) -> usize {
            8
        }

        fn reserve(&self) -> BoxFuture<'static, io::Result<Box<dyn NixlRecvBuffer>>> {
            Box::pin(async { Ok(Box::new(VecBuffer(vec![0; 8])) as Box<dyn NixlRecvBuffer>) })
        }
    }

    #[tokio::test]
    async fn the_writer_stages_bytes_in_order_until_a_segment_fills() {
        use tokio::io::AsyncWriteExt;
        let request = WriteStreamRequest {
            endpoint: "http://127.0.0.1:1".to_owned(),
            disk: "disk".to_owned(),
            volume: "volume".to_owned(),
            path: "path".to_owned(),
            append: false,
            size: 20,
        };
        let mut writer = NixlFileWriter::new(Arc::new(VecPool), request);
        let data: Vec<u8> = (0u8..20).collect();
        writer
            .write_all(&data)
            .await
            .expect("staging never sends below a full segment");
        let state = writer.state_mut();
        assert!(state.sending.is_none(), "two full 8-byte buffers stay below the four-buffer segment");
        let staged: Vec<u8> = state
            .filled
            .iter_mut()
            .chain(state.current.iter_mut())
            .flat_map(|(lent, len)| lent.buffer.as_mut().expect("buffer").spare_mut()[..*len].to_vec())
            .collect();
        assert_eq!(staged, data);
        assert_eq!(state.filled.len(), 2);
        assert_eq!(state.current.as_ref().map(|(_, len)| *len), Some(4));
    }

    struct LimitedBuffer {
        bytes: VecBuffer,
        _permit: tokio::sync::OwnedSemaphorePermit,
    }

    impl NixlRecvBuffer for LimitedBuffer {
        fn descriptor(&self) -> String {
            self.bytes.descriptor()
        }

        fn descriptor_for(&self, len: usize) -> String {
            self.bytes.descriptor_for(len)
        }

        fn spare_mut(&mut self) -> &mut [u8] {
            self.bytes.spare_mut()
        }

        fn into_bytes(self: Box<Self>, len: usize) -> io::Result<Bytes> {
            Ok(Bytes::copy_from_slice(&self.bytes.0[..len]))
        }
    }

    struct LimitedPool(Arc<tokio::sync::Semaphore>);

    impl NixlRecvPool for LimitedPool {
        fn agent_name(&self) -> &str {
            "test-agent"
        }

        fn metadata_base64(&self) -> &str {
            ""
        }

        fn metadata_sha256_hex(&self) -> &str {
            ""
        }

        fn chunk_bytes(&self) -> usize {
            8
        }

        fn reserve(&self) -> ReserveFuture {
            let permits = Arc::clone(&self.0);
            Box::pin(async move {
                let permit = permits.acquire_owned().await.map_err(io::Error::other)?;
                Ok(Box::new(LimitedBuffer {
                    bytes: VecBuffer(vec![0; 8]),
                    _permit: permit,
                }) as Box<dyn NixlRecvBuffer>)
            })
        }
    }

    #[tokio::test]
    async fn the_writer_sends_staged_bytes_when_the_pool_is_full() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        crate::runtime::sources::ensure_test_rpc_secret();
        for chunks in 1..=3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("segment listener");
            let address = listener.local_addr().expect("segment address");
            let server = tokio::spawn(async move {
                let mut received = 0;
                while received < 40 {
                    let (mut socket, _) = listener.accept().await.expect("segment connection");
                    let mut request = Vec::new();
                    while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        let mut bytes = [0; 1024];
                        let n = socket.read(&mut bytes).await.expect("segment request");
                        assert!(n > 0 && request.len() < 16384, "bounded request headers");
                        request.extend_from_slice(&bytes[..n]);
                    }
                    let request = std::str::from_utf8(&request).expect("request text");
                    let target = request.split_whitespace().nth(1).expect("request target");
                    let url = url::Url::parse(&format!("http://{address}{target}")).expect("segment URL");
                    let descriptors = url
                        .query_pairs()
                        .find(|(name, _)| name == NIXL_BUFFERS_QUERY)
                        .expect("buffers")
                        .1;
                    let bytes: usize = descriptors
                        .split(',')
                        .map(|descriptor| {
                            descriptor
                                .rsplit(':')
                                .next()
                                .expect("buffer length")
                                .parse::<usize>()
                                .expect("numeric length")
                        })
                        .sum();
                    assert!(bytes > 0 && bytes <= chunks * 8);
                    received += bytes;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\nx-rustfs-nixl-bytes: {bytes}\r\n\r\n"
                    );
                    socket.write_all(response.as_bytes()).await.expect("segment acknowledgement");
                }
                received
            });
            let pool = Arc::new(LimitedPool(Arc::new(tokio::sync::Semaphore::new(chunks))));
            let request = WriteStreamRequest {
                endpoint: format!("http://{address}"),
                disk: "disk".to_owned(),
                volume: "volume".to_owned(),
                path: "path".to_owned(),
                append: false,
                size: 40,
            };
            let mut writer = NixlFileWriter::new(pool.clone(), request);
            let result = tokio::time::timeout(Duration::from_secs(5), async {
                writer.write_all(&[7; 40]).await?;
                writer.shutdown().await
            })
            .await;
            if result.is_err() {
                server.abort();
            }
            result
                .expect("a writer must dispatch before exhausting its pool")
                .expect("shard write");
            assert_eq!(server.await.expect("segment server"), 40);
            assert_eq!(pool.0.available_permits(), chunks, "acknowledged buffers return to the pool");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn abandoned_buffers_return_only_after_the_hold() {
        let released = Arc::new(AtomicUsize::new(0));
        let lent = LentBuffer {
            buffer: Some(Box::new(CountingBuffer {
                released: Arc::clone(&released),
            })),
        };
        drop(lent);
        tokio::task::yield_now().await;
        assert_eq!(released.load(Ordering::SeqCst), 0, "an abandoned buffer must not return at once");
        tokio::time::sleep(ABANDONED_BUFFER_HOLD + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn completed_buffers_return_with_their_bytes() {
        let released = Arc::new(AtomicUsize::new(0));
        let lent = LentBuffer {
            buffer: Some(Box::new(CountingBuffer {
                released: Arc::clone(&released),
            })),
        };
        let bytes = lent.complete(5).expect("bytes");
        assert_eq!(bytes.len(), 5);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }
}
