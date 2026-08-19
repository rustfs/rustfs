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

use crate::cluster::rpc::{
    build_auth_headers, build_put_file_auth_trailer, verify_ns_scanner_capability, verify_put_file_capability,
};
use crate::disk::error::{Error, Result};
use crate::disk::{FileReader, FileWriter};
use crate::storage_api_contracts::internode::{
    NS_SCANNER_BODY_SHA256_QUERY, NS_SCANNER_CAPABILITY_CHALLENGE_QUERY, NS_SCANNER_CYCLE_QUERY, NS_SCANNER_LEADER_EPOCH_QUERY,
    NS_SCANNER_PROTOCOL_VERSION, NS_SCANNER_PROTOCOL_VERSION_QUERY, NS_SCANNER_REQUEST_ID_QUERY, NS_SCANNER_SERVER_EPOCH_QUERY,
    NS_SCANNER_SESSION_ID_QUERY, NS_SCANNER_SESSION_SEQUENCE_QUERY, NsScannerCapabilityResponse, PUT_FILE_AUTH_QUERY,
    PUT_FILE_AUTH_V1, PUT_FILE_CAPABILITY_CHALLENGE_QUERY, PUT_FILE_CAPABILITY_QUERY, PUT_FILE_CAPABILITY_VERSION,
    PUT_FILE_NONCE_QUERY, PUT_FILE_SERVER_EPOCH_QUERY, PutFileCapabilityResponse, WALK_DIR_BODY_SHA256_QUERY,
    WALK_DIR_STREAM_COMPLETION_QUERY, WALK_DIR_STREAM_COMPLETION_V1,
};
use async_trait::async_trait;
use http::{HeaderMap, HeaderValue, Method, header::CONTENT_TYPE};
use rustfs_config::{
    DEFAULT_INTERNODE_DATA_TRANSPORT, ENV_RUSTFS_INTERNODE_DATA_TRANSPORT, INTERNODE_DATA_TRANSPORT_TCP,
    KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS,
};
use rustfs_rio::{ChunkReaderBox, HttpChunkReader, HttpReader, HttpWriter};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::sync::OnceCell;
use uuid::Uuid;

#[allow(
    dead_code,
    reason = "live in the cfg(not(test)) half of build_internode_data_transport_from_env (backlog#1823)"
)]
static INTERNODE_DATA_TRANSPORT: OnceLock<std::result::Result<Arc<dyn InternodeDataTransport>, String>> = OnceLock::new();

const READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/read_file_stream";
const PUT_FILE_STREAM_PATH: &str = "/rustfs/rpc/put_file_stream";
const PUT_FILE_AUTH_STREAM_PATH: &str = "/rustfs/rpc/put_file_stream_v1";
const PUT_FILE_CAPABILITY_PATH: &str = "/rustfs/rpc/put_file_capability";
const WALK_DIR_PATH: &str = "/rustfs/rpc/walk_dir";
const NS_SCANNER_PATH: &str = "/rustfs/rpc/ns_scanner";
const NS_SCANNER_MAX_CAPABILITY_RESPONSE_SIZE: usize = 1024;
const PUT_FILE_MAX_CAPABILITY_RESPONSE_SIZE: usize = 1024;
const PUT_FILE_LEGACY_CAPABILITY_TTL: Duration = Duration::from_secs(30);
const PUT_FILE_V1_CAPABILITY_TTL: Duration = Duration::from_secs(30);
const PUT_FILE_CAPABILITY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const CONTENT_TYPE_JSON: &str = "application/json";
const CONTENT_TYPE_MSGPACK: &str = "application/msgpack";

fn unsupported_transport_message(transport: &str) -> String {
    format!(
        "invalid {ENV_RUSTFS_INTERNODE_DATA_TRANSPORT}={transport:?}; supported values: {}",
        KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS.join(", ")
    )
}

#[derive(Debug, Clone, Copy)]
enum PutFileCapabilityState {
    LegacyUntil(Instant),
    V1 { server_epoch: Uuid, revalidate_after: Instant },
}

#[derive(Debug)]
struct PutFileCapabilityProbeFailure(Error);

impl PutFileCapabilityProbeFailure {
    fn to_error(&self) -> Error {
        match &self.0 {
            Error::Io(error) => rustfs_rio::clone_internode_http_io_error(error)
                .map(Error::Io)
                .unwrap_or_else(|| self.0.clone()),
            _ => self.0.clone(),
        }
    }
}

type PutFileCapabilityProbeOutcome = std::result::Result<Option<Uuid>, PutFileCapabilityProbeFailure>;

#[derive(Debug, Clone)]
struct PutFileCapabilityFlight {
    generation: u64,
    v1_was_pinned: bool,
    outcome: Arc<OnceCell<PutFileCapabilityProbeOutcome>>,
}

#[derive(Debug, Default)]
struct PutFileCapabilityCacheState {
    cached: Option<PutFileCapabilityState>,
    generation: u64,
    in_flight: Option<PutFileCapabilityFlight>,
}

type PutFileCapabilityCacheEntry = Arc<tokio::sync::RwLock<PutFileCapabilityCacheState>>;

static PUT_FILE_CAPABILITY_CACHE: LazyLock<parking_lot::RwLock<HashMap<String, PutFileCapabilityCacheEntry>>> =
    LazyLock::new(|| parking_lot::RwLock::new(HashMap::new()));

fn put_file_capability_cache_entry(endpoint: &str) -> PutFileCapabilityCacheEntry {
    if let Some(entry) = PUT_FILE_CAPABILITY_CACHE.read().get(endpoint).cloned() {
        return entry;
    }
    PUT_FILE_CAPABILITY_CACHE
        .write()
        .entry(endpoint.to_owned())
        .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(PutFileCapabilityCacheState::default())))
        .clone()
}

fn fresh_put_file_capability(state: Option<PutFileCapabilityState>, now: Instant) -> Option<Option<Uuid>> {
    match state {
        Some(PutFileCapabilityState::V1 {
            server_epoch,
            revalidate_after,
        }) if now < revalidate_after => Some(Some(server_epoch)),
        Some(PutFileCapabilityState::LegacyUntil(expires_at)) if now < expires_at => Some(None),
        Some(PutFileCapabilityState::V1 { .. }) | Some(PutFileCapabilityState::LegacyUntil(_)) | None => None,
    }
}

fn put_file_capability_status_is_legacy(status: u16) -> bool {
    status == 404
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "capability-negotiation seam; constructed only by transport test doubles (backlog#1823)"
)]
pub struct InternodeDataTransportCapabilities {
    /// Backend can open a streaming remote disk reader.
    pub streaming_read: bool,
    /// Backend can open a streaming remote disk writer.
    pub streaming_write: bool,
    /// Backend can stream walk-dir responses.
    pub streaming_walk_dir: bool,
    /// Backend preserves in-order delivery for each opened transfer.
    pub ordered_delivery: bool,
    /// Largest payload the backend accepts for one transfer, or no RustFS-level cap.
    pub max_transfer_size: Option<usize>,
    /// Backend can participate in the behavior-preserving TCP fallback path.
    pub fallback_supported: bool,
}

impl InternodeDataTransportCapabilities {
    #[allow(
        dead_code,
        reason = "capability-negotiation seam; used by transport test doubles (backlog#1823)"
    )]
    pub const fn tcp_http() -> Self {
        Self {
            streaming_read: true,
            streaming_write: true,
            streaming_walk_dir: true,
            ordered_delivery: true,
            max_transfer_size: None,
            fallback_supported: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadStreamRequest {
    pub endpoint: String,
    pub disk: String,
    pub volume: String,
    pub path: String,
    pub offset: usize,
    pub length: usize,
    pub stall_timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct WriteStreamRequest {
    pub endpoint: String,
    pub disk: String,
    pub volume: String,
    pub path: String,
    pub append: bool,
    pub size: i64,
}

#[derive(Debug, Clone)]
pub struct WalkDirStreamRequest {
    pub endpoint: String,
    pub disk: String,
    pub body: Vec<u8>,
    pub stall_timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct NsScannerStreamRequest {
    pub endpoint: String,
    pub disk: String,
    pub request_id: Uuid,
    pub server_epoch: Uuid,
    pub session_id: Uuid,
    pub session_sequence: u64,
    pub next_cycle: u64,
    pub leader_epoch: u64,
    pub body: Vec<u8>,
    pub stall_timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct NsScannerCapabilityRequest {
    pub endpoint: String,
}

/// Data-plane stream opener used by `RemoteDisk`.
///
/// This boundary is limited to remote disk streams that can move large payloads.
/// Internode metadata, lock, health, and administrative calls remain on the
/// existing gRPC control plane.
///
/// Buffer ownership, backend selection, and fallback expectations are documented
/// in `crates/ecstore/docs/internode-transport/`.
#[async_trait]
pub trait InternodeDataTransport: Send + Sync + std::fmt::Debug {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader>;
    async fn open_read_fresh(&self, request: ReadStreamRequest) -> Result<FileReader> {
        self.open_read(request).await
    }
    /// Opens an owned-chunk stream when this transport can retain receive-buffer
    /// ownership. `None` preserves the established `open_read` fallback.
    async fn open_read_chunks(&self, _request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        Ok(None)
    }
    async fn open_read_chunks_fresh(&self, request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        self.open_read_chunks(request).await
    }
    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter>;
    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader>;
    async fn open_ns_scanner(&self, _request: NsScannerStreamRequest) -> Result<FileReader> {
        Err(Error::MethodNotAllowed)
    }
    async fn probe_ns_scanner(&self, _request: NsScannerCapabilityRequest) -> Result<Uuid> {
        Err(Error::MethodNotAllowed)
    }
    // Interface facet nobody calls yet: every transport implements both, but no
    // caller negotiates on them. Kept for the internode transport split
    // (backlog#1350); deleting them would delete the seam and six impls.
    #[allow(dead_code, reason = "unused capability-negotiation facet (backlog#1823)")]
    fn name(&self) -> &'static str;
    #[allow(dead_code, reason = "unused capability-negotiation facet (backlog#1823)")]
    fn capabilities(&self) -> InternodeDataTransportCapabilities;
}

#[derive(Debug, Default)]
pub struct TcpHttpInternodeDataTransport;

#[async_trait]
impl InternodeDataTransport for TcpHttpInternodeDataTransport {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
        let url = build_read_file_stream_url(&request);
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        Ok(Box::new(
            HttpReader::new_with_stall_timeout(url, Method::GET, headers, None, request.stall_timeout).await?,
        ))
    }

    async fn open_read_fresh(&self, request: ReadStreamRequest) -> Result<FileReader> {
        let url = build_read_file_stream_url(&request);
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        Ok(Box::new(
            HttpReader::new_fresh_connection_with_stall_timeout(url, Method::GET, headers, None, request.stall_timeout).await?,
        ))
    }

    async fn open_read_chunks(&self, request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        let url = build_read_file_stream_url(&request);
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        Ok(Some(Box::new(
            HttpChunkReader::new_with_stall_timeout(url, Method::GET, headers, None, request.stall_timeout).await?,
        )))
    }

    async fn open_read_chunks_fresh(&self, request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        let url = build_read_file_stream_url(&request);
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        Ok(Some(Box::new(
            HttpChunkReader::new_fresh_connection_with_stall_timeout(url, Method::GET, headers, None, request.stall_timeout)
                .await?,
        )))
    }

    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
        let server_epoch = self.put_file_auth_capability(&request.endpoint).await?;
        let nonce = server_epoch.map(|_| Uuid::new_v4());
        let url = build_put_file_stream_url(&request, nonce.zip(server_epoch));
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::PUT, &mut headers)?;
        let writer = HttpWriter::new(url.clone(), Method::PUT, headers).await?;
        match nonce {
            Some(nonce) => Ok(Box::new(PutFileAuthWriter::new(writer, url, nonce))),
            None => Ok(Box::new(writer)),
        }
    }

    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader> {
        let url = build_walk_dir_url(&request);
        let mut headers = json_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        Ok(Box::new(
            HttpReader::new_with_stall_timeout(url, Method::GET, headers, Some(request.body), request.stall_timeout).await?,
        ))
    }

    async fn open_ns_scanner(&self, request: NsScannerStreamRequest) -> Result<FileReader> {
        let url = build_ns_scanner_url(&request);
        let mut headers = msgpack_headers();
        build_auth_headers(&url, &Method::POST, &mut headers)?;
        Ok(Box::new(
            HttpReader::new_with_stall_timeout(url, Method::POST, headers, Some(request.body), request.stall_timeout).await?,
        ))
    }

    async fn probe_ns_scanner(&self, request: NsScannerCapabilityRequest) -> Result<Uuid> {
        let challenge = Uuid::new_v4();
        let url = build_ns_scanner_capability_url(&request, challenge);
        let mut headers = msgpack_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        let reader = HttpReader::new(url, Method::GET, headers, None).await?;
        let mut body = Vec::new();
        reader
            .take(u64::try_from(NS_SCANNER_MAX_CAPABILITY_RESPONSE_SIZE + 1).unwrap_or(u64::MAX))
            .read_to_end(&mut body)
            .await?;
        if body.is_empty() || body.len() > NS_SCANNER_MAX_CAPABILITY_RESPONSE_SIZE {
            return Err(Error::other("invalid remote namespace scanner capability response size"));
        }
        let response: NsScannerCapabilityResponse =
            rmp_serde::from_slice(&body).map_err(|_| Error::other("invalid remote namespace scanner capability response"))?;
        if response.version != NS_SCANNER_PROTOCOL_VERSION || response.server_epoch.is_nil() {
            return Err(Error::other("incompatible remote namespace scanner capability response"));
        }
        verify_ns_scanner_capability(challenge, response.server_epoch, &response.proof)
            .map_err(|err| Error::other(format!("remote namespace scanner capability authentication failed: {err}")))?;
        Ok(response.server_epoch)
    }

    fn name(&self) -> &'static str {
        DEFAULT_INTERNODE_DATA_TRANSPORT
    }

    fn capabilities(&self) -> InternodeDataTransportCapabilities {
        InternodeDataTransportCapabilities::tcp_http()
    }
}

impl TcpHttpInternodeDataTransport {
    async fn put_file_auth_capability(&self, endpoint: &str) -> Result<Option<Uuid>> {
        resolve_put_file_auth_capability(endpoint, || async {
            tokio::time::timeout(PUT_FILE_CAPABILITY_PROBE_TIMEOUT, self.probe_put_file_auth(endpoint))
                .await
                .map_err(|_| {
                    Error::from(rustfs_rio::internode_http_timeout_error(
                        &Method::GET,
                        &format!("{endpoint}{PUT_FILE_CAPABILITY_PATH}"),
                    ))
                })?
        })
        .await
    }

    async fn probe_put_file_auth(&self, endpoint: &str) -> Result<Option<Uuid>> {
        let challenge = Uuid::new_v4();
        let url = build_put_file_capability_url(endpoint, challenge);
        let mut headers = msgpack_headers();
        build_auth_headers(&url, &Method::GET, &mut headers)?;
        let reader = match HttpReader::new(url, Method::GET, headers, None).await {
            Ok(reader) => reader,
            Err(err) => {
                let err = Error::from(err);
                if matches!(
                    err.internode_http_error_kind(),
                    Some(rustfs_rio::InternodeHttpErrorKind::HttpStatus(status))
                        if put_file_capability_status_is_legacy(status.as_u16())
                ) {
                    return Ok(None);
                }
                return Err(err);
            }
        };
        let mut body = Vec::new();
        reader
            .take(u64::try_from(PUT_FILE_MAX_CAPABILITY_RESPONSE_SIZE + 1).unwrap_or(u64::MAX))
            .read_to_end(&mut body)
            .await?;
        Ok(Some(verify_put_file_capability_response(challenge, &body)?))
    }
}

async fn resolve_put_file_auth_capability<F, Fut>(endpoint: &str, probe: F) -> Result<Option<Uuid>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Option<Uuid>>>,
{
    let entry = put_file_capability_cache_entry(endpoint);
    {
        let state = entry.read().await;
        if let Some(cached) = fresh_put_file_capability(state.cached, Instant::now()) {
            return Ok(cached);
        }
    }

    let flight = {
        let mut state = entry.write().await;
        if let Some(cached) = fresh_put_file_capability(state.cached, Instant::now()) {
            return Ok(cached);
        }
        if let Some(flight) = state.in_flight.clone() {
            flight
        } else {
            state.generation = state
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::other("put_file capability probe generation exhausted"))?;
            let flight = PutFileCapabilityFlight {
                generation: state.generation,
                v1_was_pinned: matches!(state.cached, Some(PutFileCapabilityState::V1 { .. })),
                outcome: Arc::new(OnceCell::new()),
            };
            state.in_flight = Some(flight.clone());
            flight
        }
    };

    let outcome = flight
        .outcome
        .get_or_init(|| async { probe().await.map_err(PutFileCapabilityProbeFailure) })
        .await;

    {
        let mut state = entry.write().await;
        let is_current_flight = state
            .in_flight
            .as_ref()
            .is_some_and(|current| current.generation == flight.generation && Arc::ptr_eq(&current.outcome, &flight.outcome));
        if is_current_flight {
            match outcome {
                Ok(Some(server_epoch)) => {
                    state.cached = Some(PutFileCapabilityState::V1 {
                        server_epoch: *server_epoch,
                        revalidate_after: Instant::now() + PUT_FILE_V1_CAPABILITY_TTL,
                    });
                }
                Ok(None) if !flight.v1_was_pinned => {
                    state.cached = Some(PutFileCapabilityState::LegacyUntil(Instant::now() + PUT_FILE_LEGACY_CAPABILITY_TTL));
                }
                Ok(None) | Err(_) => {}
            }
            state.in_flight = None;
        }
    }

    match outcome {
        Ok(Some(server_epoch)) => Ok(Some(*server_epoch)),
        Ok(None) if flight.v1_was_pinned => Err(Error::other("remote put_file capability downgrade rejected")),
        Ok(None) => Ok(None),
        Err(failure) => Err(failure.to_error()),
    }
}

fn verify_put_file_capability_response(challenge: Uuid, body: &[u8]) -> Result<Uuid> {
    if body.is_empty() || body.len() > PUT_FILE_MAX_CAPABILITY_RESPONSE_SIZE {
        return Err(Error::other("invalid remote put_file capability response size"));
    }
    let response: PutFileCapabilityResponse =
        rmp_serde::from_slice(body).map_err(|_| Error::other("invalid remote put_file capability response"))?;
    if response.version != PUT_FILE_CAPABILITY_VERSION || response.server_epoch.is_nil() {
        return Err(Error::other("incompatible remote put_file capability response"));
    }
    verify_put_file_capability(challenge, response.server_epoch, response.version, &response.proof)
        .map_err(|err| Error::other(format!("remote put_file capability authentication failed: {err}")))?;
    Ok(response.server_epoch)
}

fn build_read_file_stream_url(request: &ReadStreamRequest) -> String {
    format!(
        "{}{}?disk={}&volume={}&path={}&offset={}&length={}",
        request.endpoint,
        READ_FILE_STREAM_PATH,
        urlencoding::encode(&request.disk),
        urlencoding::encode(&request.volume),
        urlencoding::encode(&request.path),
        request.offset,
        request.length
    )
}

fn build_put_file_stream_url(request: &WriteStreamRequest, auth_scope: Option<(Uuid, Uuid)>) -> String {
    let stream_path = if auth_scope.is_some() {
        PUT_FILE_AUTH_STREAM_PATH
    } else {
        PUT_FILE_STREAM_PATH
    };
    let mut url = format!(
        "{}{}?disk={}&volume={}&path={}&append={}&size={}",
        request.endpoint,
        stream_path,
        urlencoding::encode(&request.disk),
        urlencoding::encode(&request.volume),
        urlencoding::encode(&request.path),
        request.append,
        request.size
    );
    if let Some((nonce, server_epoch)) = auth_scope {
        url.push_str(&format!(
            "&{}={}&{}={}&{}={}",
            PUT_FILE_AUTH_QUERY, PUT_FILE_AUTH_V1, PUT_FILE_NONCE_QUERY, nonce, PUT_FILE_SERVER_EPOCH_QUERY, server_epoch
        ));
    }
    url
}

fn build_put_file_capability_url(endpoint: &str, challenge: Uuid) -> String {
    format!(
        "{}{}?{}={}&{}={}",
        endpoint,
        PUT_FILE_CAPABILITY_PATH,
        PUT_FILE_CAPABILITY_QUERY,
        PUT_FILE_CAPABILITY_VERSION,
        PUT_FILE_CAPABILITY_CHALLENGE_QUERY,
        challenge
    )
}

struct PutFileAuthWriter<W> {
    inner: W,
    url: String,
    nonce: Uuid,
    hasher: Sha256,
    trailer: Option<Vec<u8>>,
    trailer_offset: usize,
}

impl<W> PutFileAuthWriter<W> {
    fn new(inner: W, url: String, nonce: Uuid) -> Self {
        Self {
            inner,
            url,
            nonce,
            hasher: Sha256::new(),
            trailer: None,
            trailer_offset: 0,
        }
    }

    fn ensure_trailer(&mut self) -> std::io::Result<()> {
        if self.trailer.is_some() {
            return Ok(());
        }
        let digest = hex_simd::encode_to_string(self.hasher.clone().finalize(), hex_simd::AsciiCase::Lower);
        self.trailer = Some(build_put_file_auth_trailer(&self.url, &Method::PUT, self.nonce, &digest)?);
        Ok(())
    }

    fn poll_write_trailer(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>>
    where
        W: AsyncWrite + Unpin,
    {
        self.ensure_trailer()?;
        let Some(trailer) = self.trailer.as_ref() else {
            return Poll::Ready(Err(std::io::Error::other("put_file auth trailer missing")));
        };
        while self.trailer_offset < trailer.len() {
            let written = match Pin::new(&mut self.inner).poll_write(cx, &trailer[self.trailer_offset..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write put_file auth trailer",
                    )));
                }
                Poll::Ready(Ok(written)) => written,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            };
            self.trailer_offset += written;
        }
        Poll::Ready(Ok(()))
    }
}

impl<W> AsyncWrite for PutFileAuthWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        if self.trailer.is_some() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "cannot write after put_file auth trailer",
            )));
        }
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                self.hasher.update(&buf[..written]);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.poll_write_trailer(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn build_walk_dir_url(request: &WalkDirStreamRequest) -> String {
    let body_sha256 = hex_simd::encode_to_string(Sha256::digest(&request.body), hex_simd::AsciiCase::Lower);
    format!(
        "{}{}?disk={}&{}={}&{}={}",
        request.endpoint,
        WALK_DIR_PATH,
        urlencoding::encode(&request.disk),
        WALK_DIR_STREAM_COMPLETION_QUERY,
        WALK_DIR_STREAM_COMPLETION_V1,
        WALK_DIR_BODY_SHA256_QUERY,
        body_sha256
    )
}

fn build_ns_scanner_url(request: &NsScannerStreamRequest) -> String {
    let body_sha256 = hex_simd::encode_to_string(Sha256::digest(&request.body), hex_simd::AsciiCase::Lower);
    format!(
        "{}{}?disk={}&{}={}&{}={}&{}={}&{}={}&{}={}&{}={}&{}={}",
        request.endpoint,
        NS_SCANNER_PATH,
        urlencoding::encode(&request.disk),
        NS_SCANNER_REQUEST_ID_QUERY,
        request.request_id,
        NS_SCANNER_SERVER_EPOCH_QUERY,
        request.server_epoch,
        NS_SCANNER_SESSION_ID_QUERY,
        request.session_id,
        NS_SCANNER_SESSION_SEQUENCE_QUERY,
        request.session_sequence,
        NS_SCANNER_CYCLE_QUERY,
        request.next_cycle,
        NS_SCANNER_LEADER_EPOCH_QUERY,
        request.leader_epoch,
        NS_SCANNER_BODY_SHA256_QUERY,
        body_sha256
    )
}

fn build_ns_scanner_capability_url(request: &NsScannerCapabilityRequest, challenge: Uuid) -> String {
    format!(
        "{}{}?{}={}&{}={}",
        request.endpoint,
        NS_SCANNER_PATH,
        NS_SCANNER_PROTOCOL_VERSION_QUERY,
        NS_SCANNER_PROTOCOL_VERSION,
        NS_SCANNER_CAPABILITY_CHALLENGE_QUERY,
        challenge
    )
}

fn json_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_JSON));
    headers
}

fn msgpack_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_MSGPACK));
    headers
}

fn build_internode_data_transport_result(
    configured_transport: Option<&str>,
) -> std::result::Result<Arc<dyn InternodeDataTransport>, String> {
    match configured_transport.map(str::trim).filter(|transport| !transport.is_empty()) {
        None => Ok(Arc::new(TcpHttpInternodeDataTransport)),
        Some(transport)
            if transport.eq_ignore_ascii_case(DEFAULT_INTERNODE_DATA_TRANSPORT)
                || transport.eq_ignore_ascii_case(INTERNODE_DATA_TRANSPORT_TCP) =>
        {
            Ok(Arc::new(TcpHttpInternodeDataTransport))
        }
        Some(transport) => Err(unsupported_transport_message(transport)),
    }
}

#[allow(
    dead_code,
    reason = "live in the cfg(test) half of build_internode_data_transport_from_env, which bypasses the process static (backlog#1823)"
)]
pub fn build_internode_data_transport(configured_transport: Option<&str>) -> Result<Arc<dyn InternodeDataTransport>> {
    build_internode_data_transport_result(configured_transport).map_err(Error::other)
}

pub fn build_internode_data_transport_from_env() -> Result<Arc<dyn InternodeDataTransport>> {
    let configured_transport = std::env::var(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT).ok();
    #[cfg(test)]
    {
        build_internode_data_transport(configured_transport.as_deref())
    }

    #[cfg(not(test))]
    INTERNODE_DATA_TRANSPORT
        .get_or_init(|| build_internode_data_transport_result(configured_transport.as_deref()))
        .as_ref()
        .map(Arc::clone)
        .map_err(|err| Error::other(err.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::{Barrier, Notify};

    async fn wait_for_capability_flight_waiters(entry: &PutFileCapabilityCacheEntry, waiters: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let strong_count = entry
                    .read()
                    .await
                    .in_flight
                    .as_ref()
                    .map(|flight| Arc::strong_count(&flight.outcome))
                    .unwrap_or_default();
                if strong_count > waiters {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("capability callers should join the in-flight probe");
    }

    #[derive(Debug)]
    struct LegacyTestTransport;

    #[async_trait::async_trait]
    impl InternodeDataTransport for LegacyTestTransport {
        async fn open_read(&self, _request: ReadStreamRequest) -> Result<FileReader> {
            Ok(Box::new(tokio::io::empty()))
        }

        async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
            Ok(Box::new(tokio::io::sink()))
        }

        async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
            Ok(Box::new(tokio::io::empty()))
        }

        fn name(&self) -> &'static str {
            "legacy-test"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    #[tokio::test]
    async fn legacy_transport_defaults_namespace_scanner_to_unsupported() {
        let transport = LegacyTestTransport;

        let probe_err = transport
            .probe_ns_scanner(NsScannerCapabilityRequest {
                endpoint: "http://node1:9000".to_string(),
            })
            .await
            .expect_err("legacy transport should report namespace scanner as unsupported");
        assert!(matches!(probe_err, Error::MethodNotAllowed));

        let open_result = transport
            .open_ns_scanner(NsScannerStreamRequest {
                endpoint: "http://node1:9000".to_string(),
                disk: "http://node1:9000/data/rustfs0".to_string(),
                request_id: Uuid::new_v4(),
                server_epoch: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                session_sequence: 0,
                next_cycle: 7,
                leader_epoch: 9,
                body: Vec::new(),
                stall_timeout: None,
            })
            .await;
        let open_err = match open_result {
            Ok(_) => panic!("legacy transport should not open namespace scanner streams"),
            Err(err) => err,
        };
        assert!(matches!(open_err, Error::MethodNotAllowed));
    }

    #[test]
    fn tcp_http_capabilities_are_behavior_preserving() {
        let transport = TcpHttpInternodeDataTransport;

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
        assert_eq!(
            transport.capabilities(),
            InternodeDataTransportCapabilities {
                streaming_read: true,
                streaming_write: true,
                streaming_walk_dir: true,
                ordered_delivery: true,
                max_transfer_size: None,
                fallback_supported: true,
            }
        );
    }

    #[test]
    fn tcp_http_capabilities_are_conservative() {
        let capabilities = TcpHttpInternodeDataTransport.capabilities();

        assert!(capabilities.ordered_delivery);
        assert_eq!(capabilities.max_transfer_size, None);
        assert!(capabilities.fallback_supported);
    }

    #[test]
    fn read_file_stream_url_encodes_query_values() {
        let url = build_read_file_stream_url(&ReadStreamRequest {
            endpoint: "http://node1:9000".to_string(),
            disk: "http://node1:9000/data/rustfs0".to_string(),
            volume: ".rustfs.sys".to_string(),
            path: "pool.bin/../part.1".to_string(),
            offset: 7,
            length: 11,
            stall_timeout: None,
        });

        assert_eq!(
            url,
            "http://node1:9000/rustfs/rpc/read_file_stream?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0&volume=.rustfs.sys&path=pool.bin%2F..%2Fpart.1&offset=7&length=11"
        );
    }

    #[test]
    fn put_file_stream_url_encodes_query_values() {
        let url = build_put_file_stream_url(
            &WriteStreamRequest {
                endpoint: "http://node1:9000".to_string(),
                disk: "http://node1:9000/data/rustfs0".to_string(),
                volume: "bucket".to_string(),
                path: "object/part.1".to_string(),
                append: false,
                size: 4096,
            },
            None,
        );

        assert_eq!(
            url,
            "http://node1:9000/rustfs/rpc/put_file_stream?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0&volume=bucket&path=object%2Fpart.1&append=false&size=4096"
        );
    }

    #[test]
    fn put_file_stream_url_advertises_auth_nonce_when_enabled() {
        let nonce = Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("nonce");
        let server_epoch = Uuid::parse_str("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").expect("server epoch");
        let url = build_put_file_stream_url(
            &WriteStreamRequest {
                endpoint: "http://node1:9000".to_string(),
                disk: "http://node1:9000/data/rustfs0".to_string(),
                volume: "bucket".to_string(),
                path: "object/part.1".to_string(),
                append: false,
                size: 4096,
            },
            Some((nonce, server_epoch)),
        );

        assert_eq!(
            url,
            concat!(
                "http://node1:9000/rustfs/rpc/put_file_stream_v1?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0",
                "&volume=bucket&path=object%2Fpart.1&append=false&size=4096",
                "&put_file_auth=digest-trailer-v1&put_file_nonce=11111111-2222-4333-8444-555555555555",
                "&put_file_server_epoch=aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
            )
        );
    }

    #[test]
    fn put_file_capability_url_binds_version_and_challenge() {
        let challenge = Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("challenge");

        assert_eq!(
            build_put_file_capability_url("http://node1:9000", challenge),
            concat!(
                "http://node1:9000/rustfs/rpc/put_file_capability?put_file_capability=1",
                "&put_file_challenge=11111111-2222-4333-8444-555555555555"
            )
        );
    }

    #[test]
    fn put_file_capability_legacy_statuses_are_exact() {
        assert!(put_file_capability_status_is_legacy(404));
        for status in [200, 400, 401, 403, 405, 408, 426, 429, 500, 503] {
            assert!(!put_file_capability_status_is_legacy(status));
        }
    }

    #[test]
    fn put_file_capability_timeout_is_retryable() {
        let error = Error::from(rustfs_rio::internode_http_timeout_error(
            &Method::GET,
            "http://node:9000/rustfs/rpc/put_file_capability",
        ));

        assert_eq!(
            error.internode_http_error_kind(),
            Some(rustfs_rio::InternodeHttpErrorKind::ConnectTimeout)
        );
        assert!(error.is_retryable_internode_write_failure());
    }

    #[tokio::test]
    async fn put_file_capability_cache_pins_v1_and_honors_live_legacy_ttl() {
        let transport = TcpHttpInternodeDataTransport;
        let v1_endpoint = format!("http://v1-{}.invalid", Uuid::new_v4());
        let v1_entry = put_file_capability_cache_entry(&v1_endpoint);
        let server_epoch = Uuid::new_v4();
        v1_entry.write().await.cached = Some(PutFileCapabilityState::V1 {
            server_epoch,
            revalidate_after: Instant::now() + PUT_FILE_V1_CAPABILITY_TTL,
        });
        assert_eq!(
            transport.put_file_auth_capability(&v1_endpoint).await.expect("v1 cache"),
            Some(server_epoch)
        );
        let cache_probe_called = AtomicBool::new(false);
        assert_eq!(
            resolve_put_file_auth_capability(&v1_endpoint, || async {
                cache_probe_called.store(true, Ordering::SeqCst);
                Ok(None)
            })
            .await
            .expect("live v1 cache"),
            Some(server_epoch)
        );
        assert!(!cache_probe_called.load(Ordering::SeqCst));
        v1_entry.write().await.cached = Some(PutFileCapabilityState::V1 {
            server_epoch,
            revalidate_after: Instant::now(),
        });
        assert!(
            resolve_put_file_auth_capability(&v1_endpoint, || async { Ok(None) })
                .await
                .is_err()
        );
        let replacement_epoch = Uuid::new_v4();
        assert_eq!(
            resolve_put_file_auth_capability(&v1_endpoint, || async { Ok(Some(replacement_epoch)) })
                .await
                .expect("authenticated replacement should refresh the epoch"),
            Some(replacement_epoch)
        );

        let legacy_endpoint = format!("http://legacy-{}.invalid", Uuid::new_v4());
        let legacy_entry = put_file_capability_cache_entry(&legacy_endpoint);
        legacy_entry.write().await.cached =
            Some(PutFileCapabilityState::LegacyUntil(Instant::now() + PUT_FILE_LEGACY_CAPABILITY_TTL));
        assert!(
            transport
                .put_file_auth_capability(&legacy_endpoint)
                .await
                .expect("legacy cache")
                .is_none()
        );

        let expired_endpoint = format!("http://expired-legacy-{}.invalid", Uuid::new_v4());
        let expired_entry = put_file_capability_cache_entry(&expired_endpoint);
        expired_entry.write().await.cached = Some(PutFileCapabilityState::LegacyUntil(Instant::now()));
        let reprobed = std::sync::atomic::AtomicBool::new(false);
        assert_eq!(
            resolve_put_file_auth_capability(&expired_endpoint, || async {
                reprobed.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(Some(server_epoch))
            })
            .await
            .expect("expired legacy cache should reprobe"),
            Some(server_epoch)
        );
        assert!(reprobed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn legacy_put_file_capability_omits_the_auth_trailer_protocol() {
        let endpoint = format!("http://legacy-selection-{}.invalid", Uuid::new_v4());
        let server_epoch = resolve_put_file_auth_capability(&endpoint, || async { Ok(None) })
            .await
            .expect("legacy capability result");
        let auth_scope = server_epoch.map(|epoch| (Uuid::new_v4(), epoch));
        let url = build_put_file_stream_url(
            &WriteStreamRequest {
                endpoint,
                disk: "http://node1:9000/data/rustfs0".to_string(),
                volume: "bucket".to_string(),
                path: "object/part.1".to_string(),
                append: false,
                size: 4096,
            },
            auth_scope,
        );

        assert!(auth_scope.is_none());
        assert!(!url.contains(PUT_FILE_AUTH_QUERY));
        assert!(!url.contains(PUT_FILE_NONCE_QUERY));
    }

    #[tokio::test]
    async fn put_file_capability_probe_is_singleflight_per_endpoint() {
        let endpoint = format!("http://singleflight-{}.invalid", Uuid::new_v4());
        let entry = put_file_capability_cache_entry(&endpoint);
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let start = Arc::new(Barrier::new(65));
        let mut tasks = Vec::with_capacity(64);

        for _ in 0..64 {
            let endpoint = endpoint.clone();
            let calls = Arc::clone(&calls);
            let release = Arc::clone(&release);
            let start = Arc::clone(&start);
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                resolve_put_file_auth_capability(&endpoint, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Err(Error::from(rustfs_rio::new_test_internode_http_io_error(
                        rustfs_rio::InternodeHttpErrorKind::ConnectionRefused,
                    )))
                })
                .await
            }));
        }

        start.wait().await;
        wait_for_capability_flight_waiters(&entry, 64).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.notify_waiters();

        let results = tokio::time::timeout(Duration::from_secs(1), futures::future::join_all(tasks))
            .await
            .expect("all callers should finish within one probe window");
        for result in results {
            let error = result.expect("capability task should finish").expect_err("probe should fail");
            assert_eq!(
                error.internode_http_error_kind(),
                Some(rustfs_rio::InternodeHttpErrorKind::ConnectionRefused)
            );
            assert!(error.is_retryable_internode_write_failure());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn put_file_capability_probe_recovers_when_initializer_is_cancelled() {
        let endpoint = format!("http://cancelled-singleflight-{}.invalid", Uuid::new_v4());
        let entry = put_file_capability_cache_entry(&endpoint);
        let calls = Arc::new(AtomicUsize::new(0));
        let initializer_started = Arc::new(Notify::new());
        let never_release = Arc::new(Notify::new());

        let first = {
            let endpoint = endpoint.clone();
            let calls = Arc::clone(&calls);
            let initializer_started = Arc::clone(&initializer_started);
            let never_release = Arc::clone(&never_release);
            tokio::spawn(async move {
                resolve_put_file_auth_capability(&endpoint, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    initializer_started.notify_one();
                    never_release.notified().await;
                    Ok(Some(Uuid::new_v4()))
                })
                .await
            })
        };
        initializer_started.notified().await;

        let replacement_epoch = Uuid::new_v4();
        let second = {
            let endpoint = endpoint.clone();
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                resolve_put_file_auth_capability(&endpoint, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(replacement_epoch))
                })
                .await
            })
        };
        wait_for_capability_flight_waiters(&entry, 2).await;
        first.abort();
        assert!(first.await.expect_err("initializer should be cancelled").is_cancelled());

        assert_eq!(
            second.await.expect("waiter should finish").expect("waiter should take over"),
            Some(replacement_epoch)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn put_file_capability_probe_recovers_after_all_callers_cancel() {
        let endpoint = format!("http://all-cancelled-{}.invalid", Uuid::new_v4());
        let entry = put_file_capability_cache_entry(&endpoint);
        let calls = Arc::new(AtomicUsize::new(0));
        let initializer_started = Arc::new(Notify::new());
        let never_release = Arc::new(Notify::new());

        let first = {
            let endpoint = endpoint.clone();
            let calls = Arc::clone(&calls);
            let initializer_started = Arc::clone(&initializer_started);
            let never_release = Arc::clone(&never_release);
            tokio::spawn(async move {
                resolve_put_file_auth_capability(&endpoint, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    initializer_started.notify_one();
                    never_release.notified().await;
                    Ok(None)
                })
                .await
            })
        };
        initializer_started.notified().await;
        let second = {
            let endpoint = endpoint.clone();
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                resolve_put_file_auth_capability(&endpoint, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(None)
                })
                .await
            })
        };
        wait_for_capability_flight_waiters(&entry, 2).await;
        first.abort();
        second.abort();
        assert!(first.await.expect_err("initializer should be cancelled").is_cancelled());
        assert!(second.await.expect_err("waiter should be cancelled").is_cancelled());

        let server_epoch = Uuid::new_v4();
        assert_eq!(
            resolve_put_file_auth_capability(&endpoint, || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(server_epoch))
            })
            .await
            .expect("later caller should initialize the abandoned flight"),
            Some(server_epoch)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn put_file_capability_failed_wave_can_retry_immediately() {
        let endpoint = format!("http://retry-after-failure-{}.invalid", Uuid::new_v4());
        let first = resolve_put_file_auth_capability(&endpoint, || async { Err(Error::Timeout) }).await;
        assert!(matches!(first, Err(Error::Timeout)));

        let server_epoch = Uuid::new_v4();
        assert_eq!(
            resolve_put_file_auth_capability(&endpoint, || async { Ok(Some(server_epoch)) })
                .await
                .expect("new request should reprobe"),
            Some(server_epoch)
        );
    }

    #[tokio::test]
    async fn put_file_capability_probes_different_endpoints_in_parallel() {
        let first_endpoint = format!("http://parallel-a-{}.invalid", Uuid::new_v4());
        let second_endpoint = format!("http://parallel-b-{}.invalid", Uuid::new_v4());
        let probes_started = Arc::new(Barrier::new(2));
        let first_barrier = Arc::clone(&probes_started);
        let second_barrier = Arc::clone(&probes_started);

        let results = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                resolve_put_file_auth_capability(&first_endpoint, || async move {
                    first_barrier.wait().await;
                    Ok(None)
                }),
                resolve_put_file_auth_capability(&second_endpoint, || async move {
                    second_barrier.wait().await;
                    Ok(None)
                })
            )
        })
        .await
        .expect("different endpoints should not serialize");
        assert!(results.0.expect("first result").is_none());
        assert!(results.1.expect("second result").is_none());
    }

    #[tokio::test]
    async fn stale_put_file_capability_flight_cannot_overwrite_newer_state() {
        let endpoint = format!("http://stale-flight-{}.invalid", Uuid::new_v4());
        let entry = put_file_capability_cache_entry(&endpoint);
        let probe_started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let stale_epoch = Uuid::new_v4();
        let newer_epoch = Uuid::new_v4();

        let task = {
            let endpoint = endpoint.clone();
            let probe_started = Arc::clone(&probe_started);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                resolve_put_file_auth_capability(&endpoint, || async move {
                    probe_started.notify_one();
                    release.notified().await;
                    Ok(Some(stale_epoch))
                })
                .await
            })
        };
        probe_started.notified().await;
        {
            let mut state = entry.write().await;
            state.generation = state.generation.checked_add(1).expect("test generation should advance");
            state.cached = Some(PutFileCapabilityState::V1 {
                server_epoch: newer_epoch,
                revalidate_after: Instant::now() + PUT_FILE_V1_CAPABILITY_TTL,
            });
            state.in_flight = None;
        }
        release.notify_one();
        assert_eq!(
            task.await.expect("stale task should finish").expect("stale probe result"),
            Some(stale_epoch)
        );
        assert_eq!(
            fresh_put_file_capability(entry.read().await.cached, Instant::now()),
            Some(Some(newer_epoch))
        );
    }

    #[test]
    fn put_file_capability_response_fails_closed_on_malformed_or_unbound_data() {
        let _ = rustfs_credentials::set_global_rpc_secret("put-file-capability-response-test-secret".to_string());
        let challenge = Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("challenge");
        let server_epoch = Uuid::parse_str("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").expect("server epoch");
        let proof = crate::cluster::rpc::sign_put_file_capability(challenge, server_epoch, PUT_FILE_CAPABILITY_VERSION)
            .expect("proof should build");
        let response = PutFileCapabilityResponse {
            version: PUT_FILE_CAPABILITY_VERSION,
            server_epoch,
            proof,
        };
        let body = rmp_serde::to_vec_named(&response).expect("response should encode");

        assert_eq!(
            verify_put_file_capability_response(challenge, &body).expect("response should verify"),
            server_epoch
        );
        assert!(verify_put_file_capability_response(Uuid::new_v4(), &body).is_err());
        assert!(verify_put_file_capability_response(challenge, &body[..body.len() - 1]).is_err());
        assert!(verify_put_file_capability_response(challenge, &[]).is_err());
        assert!(verify_put_file_capability_response(challenge, &vec![0_u8; PUT_FILE_MAX_CAPABILITY_RESPONSE_SIZE + 1]).is_err());
    }

    #[tokio::test]
    async fn put_file_auth_writer_appends_trailer_on_shutdown() {
        use tokio::io::AsyncWriteExt;

        let _ = rustfs_credentials::set_global_rpc_secret("put-file-auth-writer-test-secret".to_string());
        let nonce = Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("nonce");
        let url = concat!(
            "http://node1:9000/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=false&size=11&put_file_auth=digest-trailer-v1&put_file_nonce=11111111-2222-4333-8444-555555555555"
        )
        .to_string();
        let mut sink = Vec::new();

        {
            let mut writer = PutFileAuthWriter::new(&mut sink, url.clone(), nonce);
            writer.write_all(b"hello world").await.expect("body write should succeed");
            writer.shutdown().await.expect("shutdown should append auth trailer");
            let err = writer
                .write_all(b"!")
                .await
                .expect_err("post-trailer writes must be rejected");
            assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
        }

        assert_eq!(&sink[..11], b"hello world");
        let trailer = &sink[11..];
        let expected_digest = hex_simd::encode_to_string(Sha256::digest(b"hello world"), hex_simd::AsciiCase::Lower);
        let verified = crate::cluster::rpc::verify_put_file_auth_trailer(&url, &Method::PUT, nonce, trailer)
            .expect("emitted trailer should verify");
        assert_eq!(verified, expected_digest);
    }

    #[test]
    fn walk_dir_url_encodes_disk_ref() {
        let url = build_walk_dir_url(&WalkDirStreamRequest {
            endpoint: "http://node1:9000".to_string(),
            disk: "http://node1:9000/data/rustfs0".to_string(),
            body: Vec::new(),
            stall_timeout: None,
        });

        assert_eq!(
            url,
            concat!(
                "http://node1:9000/rustfs/rpc/walk_dir?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0",
                "&walk_dir_stream_completion=error-v1",
                "&walk_dir_body_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
        );
    }

    #[test]
    fn ns_scanner_url_binds_body_and_encodes_disk_ref() {
        let request_id = Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("request ID");
        let server_epoch = Uuid::parse_str("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").expect("server epoch");
        let session_id = Uuid::parse_str("99999999-8888-4777-8666-555555555555").expect("session ID");
        let url = build_ns_scanner_url(&NsScannerStreamRequest {
            endpoint: "http://node1:9000".to_string(),
            disk: "http://node1:9000/data/rustfs0".to_string(),
            request_id,
            server_epoch,
            session_id,
            session_sequence: 3,
            next_cycle: 7,
            leader_epoch: 9,
            body: b"scanner-request".to_vec(),
            stall_timeout: None,
        });

        assert_eq!(
            url,
            concat!(
                "http://node1:9000/rustfs/rpc/ns_scanner?disk=http%3A%2F%2Fnode1%3A9000%2Fdata%2Frustfs0",
                "&ns_scanner_request_id=11111111-2222-4333-8444-555555555555",
                "&ns_scanner_server_epoch=aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                "&ns_scanner_session_id=99999999-8888-4777-8666-555555555555",
                "&ns_scanner_session_sequence=3",
                "&ns_scanner_cycle=7",
                "&ns_scanner_leader_epoch=9",
                "&ns_scanner_body_sha256=c958f15ca28422275c1245399f4c44eaba628ca453fcd77d6b3d4484573e4387"
            )
        );
    }

    #[test]
    fn ns_scanner_capability_url_binds_version_and_challenge() {
        let challenge = Uuid::parse_str("12345678-1234-4234-8234-123456789abc").expect("challenge");
        let url = build_ns_scanner_capability_url(
            &NsScannerCapabilityRequest {
                endpoint: "http://node1:9000".to_string(),
            },
            challenge,
        );

        assert_eq!(
            url,
            format!(
                "http://node1:9000/rustfs/rpc/ns_scanner?ns_scanner_protocol={NS_SCANNER_PROTOCOL_VERSION}&ns_scanner_challenge={challenge}"
            )
        );
    }

    #[test]
    fn transport_config_defaults_to_tcp_http() {
        let transport = build_internode_data_transport(None).unwrap();

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
    }

    #[test]
    fn transport_config_blank_value_falls_back_to_default() {
        let transport = build_internode_data_transport(Some("   ")).unwrap();

        assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
    }

    #[test]
    fn transport_config_accepts_tcp_aliases() {
        for configured in [
            DEFAULT_INTERNODE_DATA_TRANSPORT,
            INTERNODE_DATA_TRANSPORT_TCP,
            "TCP-HTTP",
            "TCP",
        ] {
            let transport = build_internode_data_transport(Some(configured)).unwrap();

            assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
        }
    }

    #[test]
    fn transport_config_known_backends_are_current_oss_values() {
        assert_eq!(
            KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS,
            &[DEFAULT_INTERNODE_DATA_TRANSPORT, INTERNODE_DATA_TRANSPORT_TCP]
        );

        for configured in KNOWN_INTERNODE_DATA_TRANSPORT_BACKENDS {
            let transport = build_internode_data_transport(Some(configured)).unwrap();

            assert_eq!(transport.name(), DEFAULT_INTERNODE_DATA_TRANSPORT);
        }
    }

    #[test]
    fn transport_config_rejects_unknown_backend() {
        let err = build_internode_data_transport(Some("unsupported-backend")).expect_err("unknown backend should fail closed");

        assert!(err.to_string().contains(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT));
        assert!(err.to_string().contains("unsupported-backend"));
        assert!(err.to_string().contains("supported values: tcp-http, tcp"));
    }

    #[test]
    fn cached_transport_config_error_uses_raw_message() {
        let err =
            build_internode_data_transport_result(Some("unsupported-backend")).expect_err("unknown backend should fail closed");

        assert!(!err.starts_with("io error "));
        assert!(err.contains(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT));
        assert!(err.contains("unsupported-backend"));
    }
}
