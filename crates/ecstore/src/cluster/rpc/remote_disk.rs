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

use crate::cluster::rpc::client::{
    AuthenticatedChannel, TonicInterceptor, gen_tonic_signature_interceptor, is_network_like_disk_error,
    node_service_time_out_client, node_service_time_out_client_for_class, node_service_time_out_client_no_auth,
};
use crate::cluster::rpc::internode_data_transport::{
    InternodeDataTransport, NsScannerCapabilityRequest, NsScannerStreamRequest, ReadStreamRequest, WalkDirStreamRequest,
    WriteStreamRequest,
};
use crate::disk::error::{Error, Result};
use crate::disk::{
    BatchReadVersionReq, BatchReadVersionResp, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation,
    DiskOption, FileInfoVersions, FileReader, FileWriter, PartTransactionAction, ReadMultipleReq, ReadMultipleResp, ReadOptions,
    RenameDataResp, SnapshotLeaseToken, UpdateMetadataOpts, VolumeInfo, WalkDirOptions, batch_read_version_one_by_one,
    disk_store::{
        DEFAULT_RUSTFS_DRIVE_ACTIVE_MONITORING, ENV_RUSTFS_DRIVE_ACTIVE_MONITORING, SKIP_IF_SUCCESS_BEFORE,
        get_drive_active_check_interval, get_drive_active_check_timeout, get_drive_disk_info_timeout, get_drive_list_dir_timeout,
        get_drive_metadata_timeout, get_drive_walkdir_stall_timeout, get_drive_walkdir_timeout, get_max_timeout_duration,
        get_object_disk_read_timeout,
    },
    endpoint::Endpoint,
    health_state::{RuntimeDriveHealthState, get_drive_returning_probe_interval, record_drive_runtime_state},
    validate_batch_read_version_item_count,
};
use crate::disk::{disk_store::DiskHealthTracker, error::DiskError, local::ScanGuard};
use crate::set_disk::DEFAULT_READ_BUFFER_SIZE;
use bytes::Bytes;
use futures::lock::Mutex;
use metrics::counter;
use rustfs_filemeta::{FileInfo, ObjectPartInfo, RawFileInfo};
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_STAGE_BATCH_READ_VERSION_REQUEST_ENCODE, INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_DECODE,
    INTERNODE_STAGE_BATCH_READ_VERSION_RPC_ROUNDTRIP, INTERNODE_STAGE_READ_VERSION_REQUEST_ENCODE,
    INTERNODE_STAGE_READ_VERSION_RESPONSE_DECODE, INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP,
};
use rustfs_protos::ChannelClass;
use rustfs_protos::evict_failed_connection;
use rustfs_protos::proto_gen::node_service::RenamePartRequest;
use rustfs_protos::proto_gen::node_service::{
    BatchReadVersionRequest, BatchReadVersionResponse, CheckPartsRequest, DeletePathsRequest, DeleteRequest,
    DeleteVersionRequest, DeleteVersionsRequest, DeleteVersionsResponse, DeleteVolumeRequest, DiskInfoRequest, ListDirRequest,
    ListVolumesRequest, MakeVolumeRequest, MakeVolumesRequest, PreparePartTransactionRequest, ReadAllRequest,
    ReadMetadataRequest, ReadMultipleRequest, ReadMultipleResponse, ReadPartsRequest, ReadVersionRequest, ReadXlRequest,
    RenameDataRequest, RenameFileRequest, SettlePartTransactionRequest, SnapshotLeaseReleaseRequest, SnapshotLeaseRenewRequest,
    SnapshotLeaseRequest, SnapshotLeaseResponse, StatVolumeRequest, UpdateMetadataRequest, VerifyFileRequest, WriteAllRequest,
    WriteMetadataRequest, node_service_client::NodeServiceClient,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    future::Future,
    io::Cursor,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::time;
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
    task::{JoinError, JoinHandle},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, service::interceptor::InterceptedService};
use tracing::{debug, trace, warn};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailureHealthAction {
    MarkFailure,
    IgnoreFailure,
}

const REMOTE_DISK_OPEN_WRITE_MAX_ATTEMPTS: usize = 2;
const REMOTE_DISK_OPEN_WRITE_RETRY_BACKOFF: Duration = Duration::from_millis(20);
const REMOTE_DISK_OPEN_READ_MAX_ATTEMPTS: usize = 2;
const REMOTE_DISK_OPEN_READ_RETRY_BACKOFF: Duration = Duration::from_millis(20);
const REMOTE_READ_TIMEOUT_PARTS: u32 = 3;
const NS_SCANNER_CAPABILITY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Base backoff for idempotent read-only RPC retries (grpc-optimization P3-3); doubles per attempt.
const REMOTE_DISK_READ_RETRY_BASE_BACKOFF: Duration = Duration::from_millis(50);
const ENV_RUSTFS_METADATA_BATCH_READ: &str = "RUSTFS_METADATA_BATCH_READ";
const LEGACY_ENV_RUSTFS_BATCH_METADATA_RPC: &str = "RUSTFS_BATCH_METADATA_RPC";
const ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE: &str = "RUSTFS_GET_METADATA_READ_VERSION_COALESCE";
const BATCH_METADATA_RPC_OFF: &str = "off";
const BATCH_METADATA_RPC_AUTO: &str = "auto";
const BATCH_METADATA_RPC_ON: &str = "on";
const BATCH_READ_VERSION_GATE_ATTEMPT: &str = "attempt";
const BATCH_READ_VERSION_GATE_OFF_UNARY: &str = "off_unary";
const BATCH_READ_VERSION_GATE_FALLBACK_UNIMPLEMENTED: &str = "fallback_unimplemented";
const BATCH_READ_VERSION_GATE_UNSUPPORTED_NO_FALLBACK: &str = "unsupported_no_fallback";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_REMOTE_DISK: &str = "remote_disk";
const EVENT_REMOTE_DISK_HEALTH: &str = "remote_disk_health";
const EVENT_REMOTE_DISK_RPC: &str = "remote_disk_rpc";
const SNAPSHOT_LEASE_PROTOCOL_VERSION: u32 = 1;
pub const REMOTE_SNAPSHOT_LEASE_TTL: Duration = Duration::from_secs(60);

fn decode_delete_versions_errors(response: DeleteVersionsResponse, expected_len: usize) -> Vec<Option<Error>> {
    if !response.item_errors.is_empty() {
        if response.item_errors.len() != expected_len {
            return vec![Some(Error::other("malformed delete_versions item errors")); expected_len];
        }
        return response
            .item_errors
            .into_iter()
            .map(|error| (error.code != 0).then(|| error.into()))
            .collect();
    }

    if response.errors.len() != expected_len {
        return vec![Some(Error::other("malformed delete_versions errors")); expected_len];
    }
    response
        .errors
        .into_iter()
        .map(|error| (!error.is_empty()).then(|| Error::other(error)))
        .collect()
}

fn snapshot_lease_token_from_response(response: SnapshotLeaseResponse) -> Result<SnapshotLeaseToken> {
    if !response.success {
        return Err(response.error.unwrap_or_default().into());
    }
    if response.protocol_version != SNAPSHOT_LEASE_PROTOCOL_VERSION {
        return Err(Error::other("remote snapshot lease protocol is incompatible"));
    }
    SnapshotLeaseToken::from_slice(&response.token)
}

/// Bind a mutating disk RPC to its canonical body: the digest lands in the request metadata, and
/// the signing interceptor folds it (plus a replay-protected nonce) into the v2 signature scope
/// (backlog#1327).
fn attach_mutation_body_digest<T>(
    request: &mut Request<T>,
    canonical_body: std::result::Result<Vec<u8>, std::num::TryFromIntError>,
    op: &'static str,
) -> Result<()> {
    let canonical_body = canonical_body.map_err(|_| Error::other(format!("{op} request length cannot be represented")))?;
    crate::cluster::rpc::set_tonic_rolling_canonical_body_digest(request, &canonical_body).map_err(Error::other)
}

fn decode_volume_infos(volume_infos: Vec<String>) -> Result<Vec<VolumeInfo>> {
    volume_infos
        .into_iter()
        .enumerate()
        .map(|(index, json)| {
            serde_json::from_str::<VolumeInfo>(&json)
                .map_err(|err| Error::other(format!("decode list volumes entry {index} failed: {err}")))
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchMetadataRpcMode {
    Off,
    Auto,
    On,
}

impl BatchMetadataRpcMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Off => BATCH_METADATA_RPC_OFF,
            Self::Auto => BATCH_METADATA_RPC_AUTO,
            Self::On => BATCH_METADATA_RPC_ON,
        }
    }

    fn should_attempt(self) -> bool {
        matches!(self, Self::Auto | Self::On)
    }

    fn should_fallback_on_unimplemented(self) -> bool {
        matches!(self, Self::Auto)
    }
}

fn parse_batch_metadata_rpc_mode(raw: &str) -> BatchMetadataRpcMode {
    match raw.trim() {
        value if value.eq_ignore_ascii_case(BATCH_METADATA_RPC_AUTO) => BatchMetadataRpcMode::Auto,
        value if value.eq_ignore_ascii_case(BATCH_METADATA_RPC_ON) => BatchMetadataRpcMode::On,
        value if value.eq_ignore_ascii_case(BATCH_METADATA_RPC_OFF) => BatchMetadataRpcMode::Off,
        _ => BatchMetadataRpcMode::Off,
    }
}

fn batch_metadata_rpc_mode_from_env() -> BatchMetadataRpcMode {
    rustfs_utils::get_env_opt_str(ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE)
        .or_else(|| rustfs_utils::get_env_opt_str(ENV_RUSTFS_METADATA_BATCH_READ))
        .or_else(|| rustfs_utils::get_env_opt_str(LEGACY_ENV_RUSTFS_BATCH_METADATA_RPC))
        .as_deref()
        .map(parse_batch_metadata_rpc_mode)
        .unwrap_or(BatchMetadataRpcMode::Off)
}

fn batch_metadata_rpc_mode() -> BatchMetadataRpcMode {
    // The gate cannot change at runtime; parse it once instead of re-reading
    // the environment on every batch RPC.
    static MODE: std::sync::LazyLock<BatchMetadataRpcMode> = std::sync::LazyLock::new(batch_metadata_rpc_mode_from_env);
    *MODE
}

fn record_batch_read_version_gate_decision(mode: BatchMetadataRpcMode, decision: &'static str) {
    counter!(
        "rustfs_remote_disk_batch_read_version_gate_total",
        "mode" => mode.as_str(),
        "decision" => decision
    )
    .increment(1);
}

async fn copy_stream_with_buffer<R, W>(reader: &mut R, writer: &mut W, buffer_size: usize) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut copied = 0_u64;
    let mut buffer = vec![0_u8; buffer_size];

    loop {
        let bytes_read = reader.read(&mut buffer).await?;
        if bytes_read == 0 {
            writer.flush().await?;
            return Ok(copied);
        }

        writer.write_all(&buffer[..bytes_read]).await?;
        copied += bytes_read as u64;
    }
}

fn is_retryable_remote_body_error(error: &io::Error) -> bool {
    if error
        .get_ref()
        .and_then(|source| source.downcast_ref::<rustfs_rio::BodyStalled>())
        .is_some()
    {
        return true;
    }

    matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::UnexpectedEof
    )
}

fn resumed_read_request(request: &ReadStreamRequest, emitted: usize) -> io::Result<ReadStreamRequest> {
    let offset = request
        .offset
        .checked_add(emitted)
        .ok_or_else(|| io::Error::other("remote read resume offset overflow"))?;
    let length = if request.length == 0 {
        0
    } else {
        request
            .length
            .checked_sub(emitted)
            .ok_or_else(|| io::Error::other("remote read resume offset exceeds requested length"))?
    };
    Ok(ReadStreamRequest {
        offset,
        length,
        ..request.clone()
    })
}

#[derive(Clone, Copy)]
struct RemoteReadTimeouts {
    body_stall: Option<Duration>,
    initial_read: Option<Duration>,
    recovery: Option<Duration>,
}

fn remote_read_timeouts(read_timeout: Duration) -> RemoteReadTimeouts {
    let Some(recovery) = read_timeout
        .checked_div(REMOTE_READ_TIMEOUT_PARTS)
        .filter(|timeout| !timeout.is_zero())
    else {
        return RemoteReadTimeouts {
            body_stall: None,
            initial_read: None,
            recovery: None,
        };
    };
    RemoteReadTimeouts {
        body_stall: Some(recovery),
        initial_read: Some(read_timeout.saturating_sub(recovery)),
        recovery: Some(recovery),
    }
}

async fn with_remote_read_recovery_timeout<T, F>(recovery_timeout: Option<Duration>, future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match recovery_timeout {
        Some(recovery_timeout) => match time::timeout(recovery_timeout, future).await {
            Ok(result) => result,
            Err(_) => Err(DiskError::Timeout),
        },
        None => future.await,
    }
}

struct AbortOnDropTask<T>(JoinHandle<T>);

impl<T> AbortOnDropTask<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self(handle)
    }
}

impl<T> Future for AbortOnDropTask<T>
where
    T: Send + 'static,
{
    type Output = std::result::Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn retry_cutoff_elapsed(
    initial_read_timeout: &mut Option<Duration>,
    cutoff: &mut Option<Pin<Box<time::Sleep>>>,
    cx: &mut Context<'_>,
) -> bool {
    if cutoff.is_none()
        && let Some(timeout) = initial_read_timeout.take()
    {
        *cutoff = Some(Box::pin(time::sleep(timeout)));
    }
    cutoff.as_mut().is_some_and(|cutoff| cutoff.as_mut().poll(cx).is_ready())
}

type ReadResumeFuture = AbortOnDropTask<Result<FileReader>>;

struct RetryingRemoteReader {
    reader: Option<FileReader>,
    transport: Arc<dyn InternodeDataTransport>,
    request: ReadStreamRequest,
    emitted: usize,
    retried: bool,
    initial_read_timeout: Option<Duration>,
    retry_cutoff: Option<Pin<Box<time::Sleep>>>,
    recovery_timeout: Option<Duration>,
    resume: Option<ReadResumeFuture>,
}

impl RetryingRemoteReader {
    fn new_with_timeouts(
        reader: FileReader,
        transport: Arc<dyn InternodeDataTransport>,
        request: ReadStreamRequest,
        initial_read_timeout: Option<Duration>,
        recovery_timeout: Option<Duration>,
    ) -> Self {
        Self {
            reader: Some(reader),
            transport,
            request,
            emitted: 0,
            retried: false,
            initial_read_timeout,
            retry_cutoff: None,
            recovery_timeout,
            resume: None,
        }
    }

    fn start_resume(&mut self) -> io::Result<()> {
        if self.request.length != 0 && self.emitted >= self.request.length {
            self.reader = None;
            return Ok(());
        }
        let request = resumed_read_request(&self.request, self.emitted)?;
        let recovery_timeout = self.recovery_timeout;
        let transport = Arc::clone(&self.transport);
        self.resume = Some(AbortOnDropTask::new(tokio::spawn(async move {
            with_remote_read_recovery_timeout(recovery_timeout, transport.open_read_fresh(request)).await
        })));
        Ok(())
    }

    fn retry_cutoff_elapsed(&mut self, cx: &mut Context<'_>) -> bool {
        retry_cutoff_elapsed(&mut self.initial_read_timeout, &mut self.retry_cutoff, cx)
    }
}

impl AsyncRead for RetryingRemoteReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        loop {
            // After the absolute cutoff, let initial progress win over a stale fresh-open.
            let resume_pending = if let Some(resume) = self.resume.as_mut() {
                match Pin::new(resume).poll(cx) {
                    Poll::Pending => true,
                    Poll::Ready(Ok(Ok(reader))) => {
                        self.resume = None;
                        self.reader = Some(reader);
                        false
                    }
                    Poll::Ready(Ok(Err(error))) => {
                        self.resume = None;
                        if self.reader.is_none() {
                            return Poll::Ready(Err(io::Error::other(error)));
                        }
                        continue;
                    }
                    Poll::Ready(Err(error)) => {
                        self.resume = None;
                        if self.reader.is_none() {
                            return Poll::Ready(Err(io::Error::other(error)));
                        }
                        continue;
                    }
                }
            } else {
                false
            };

            if !self.retried && self.retry_cutoff_elapsed(cx) {
                self.retried = true;
                if let Err(resume_error) = self.start_resume() {
                    return Poll::Ready(Err(resume_error));
                }
                continue;
            }

            let Some(reader) = self.reader.as_mut() else {
                if resume_pending {
                    return Poll::Pending;
                }
                return Poll::Ready(Ok(()));
            };
            let before = buf.filled().len();
            match Pin::new(reader).poll_read(cx, buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let produced = buf.filled().len() - before;
                    self.emitted = match self.emitted.checked_add(produced) {
                        Some(emitted) => emitted,
                        None => return Poll::Ready(Err(io::Error::other("remote read emitted byte count overflow"))),
                    };
                    if resume_pending {
                        if produced == 0 && (self.request.length == 0 || self.emitted >= self.request.length) {
                            self.resume = None;
                        } else if produced == 0 {
                            self.reader = None;
                            continue;
                        } else {
                            self.resume = None;
                        }
                    }
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(error)) if !self.retried && is_retryable_remote_body_error(&error) => {
                    self.retried = true;
                    self.reader = None;
                    if let Err(resume_error) = self.start_resume() {
                        return Poll::Ready(Err(resume_error));
                    }
                    continue;
                }
                Poll::Ready(Err(error)) if resume_pending && is_retryable_remote_body_error(&error) => {
                    self.reader = None;
                    continue;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            }
        }
    }
}

type ChunkResumeFuture = AbortOnDropTask<Result<Option<rustfs_rio::ChunkReaderBox>>>;

struct RetryingRemoteChunkReader {
    reader: Option<rustfs_rio::ChunkReaderBox>,
    transport: Arc<dyn InternodeDataTransport>,
    request: ReadStreamRequest,
    emitted: usize,
    retried: bool,
    initial_read_timeout: Option<Duration>,
    retry_cutoff: Option<Pin<Box<time::Sleep>>>,
    recovery_timeout: Option<Duration>,
    resume: Option<ChunkResumeFuture>,
}

impl RetryingRemoteChunkReader {
    fn new_with_timeouts(
        reader: rustfs_rio::ChunkReaderBox,
        transport: Arc<dyn InternodeDataTransport>,
        request: ReadStreamRequest,
        initial_read_timeout: Option<Duration>,
        recovery_timeout: Option<Duration>,
    ) -> Self {
        Self {
            reader: Some(reader),
            transport,
            request,
            emitted: 0,
            retried: false,
            initial_read_timeout,
            retry_cutoff: None,
            recovery_timeout,
            resume: None,
        }
    }

    fn start_resume(&mut self) -> io::Result<()> {
        if self.request.length != 0 && self.emitted >= self.request.length {
            self.reader = None;
            return Ok(());
        }
        let request = resumed_read_request(&self.request, self.emitted)?;
        let recovery_timeout = self.recovery_timeout;
        let transport = Arc::clone(&self.transport);
        self.resume = Some(AbortOnDropTask::new(tokio::spawn(async move {
            with_remote_read_recovery_timeout(recovery_timeout, transport.open_read_chunks_fresh(request)).await
        })));
        Ok(())
    }

    fn retry_cutoff_elapsed(&mut self, cx: &mut Context<'_>) -> bool {
        retry_cutoff_elapsed(&mut self.initial_read_timeout, &mut self.retry_cutoff, cx)
    }
}

impl AsyncRead for RetryingRemoteChunkReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        match rustfs_rio::ChunkReader::poll_read_chunk(self.as_mut(), cx, buf.remaining()) {
            Poll::Ready(Ok(Some(chunk))) => {
                buf.put_slice(&chunk);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl rustfs_rio::ChunkReader for RetryingRemoteChunkReader {
    fn poll_read_chunk(mut self: Pin<&mut Self>, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
        loop {
            let resume_pending = if let Some(resume) = self.resume.as_mut() {
                match Pin::new(resume).poll(cx) {
                    Poll::Pending => true,
                    Poll::Ready(Ok(Ok(Some(reader)))) => {
                        self.resume = None;
                        self.reader = Some(reader);
                        false
                    }
                    Poll::Ready(Ok(Ok(None))) => {
                        self.resume = None;
                        if self.reader.is_none() {
                            return Poll::Ready(Err(io::Error::other("remote resume transport did not provide a chunk reader")));
                        }
                        continue;
                    }
                    Poll::Ready(Ok(Err(error))) => {
                        self.resume = None;
                        if self.reader.is_none() {
                            return Poll::Ready(Err(io::Error::other(error)));
                        }
                        continue;
                    }
                    Poll::Ready(Err(error)) => {
                        self.resume = None;
                        if self.reader.is_none() {
                            return Poll::Ready(Err(io::Error::other(error)));
                        }
                        continue;
                    }
                }
            } else {
                false
            };

            if !self.retried && self.retry_cutoff_elapsed(cx) {
                self.retried = true;
                if let Err(resume_error) = self.start_resume() {
                    return Poll::Ready(Err(resume_error));
                }
                continue;
            }

            let Some(reader) = self.reader.as_mut() else {
                if resume_pending {
                    return Poll::Pending;
                }
                return Poll::Ready(Ok(None));
            };
            match rustfs_rio::ChunkReader::poll_read_chunk(Pin::new(reader.as_mut()), cx, max) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(Some(chunk))) => {
                    self.emitted = match self.emitted.checked_add(chunk.len()) {
                        Some(emitted) => emitted,
                        None => return Poll::Ready(Err(io::Error::other("remote read emitted byte count overflow"))),
                    };
                    if resume_pending {
                        self.resume = None;
                    }
                    return Poll::Ready(Ok(Some(chunk)));
                }
                Poll::Ready(Ok(None)) if resume_pending => {
                    self.reader = None;
                    continue;
                }
                Poll::Ready(Ok(None)) => return Poll::Ready(Ok(None)),
                Poll::Ready(Err(error)) if !self.retried && is_retryable_remote_body_error(&error) => {
                    self.retried = true;
                    self.reader = None;
                    if let Err(resume_error) = self.start_resume() {
                        return Poll::Ready(Err(resume_error));
                    }
                    continue;
                }
                Poll::Ready(Err(error)) if resume_pending && is_retryable_remote_body_error(&error) => {
                    self.reader = None;
                    continue;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            }
        }
    }
}

#[derive(Debug)]
pub struct RemoteDisk {
    /// Stable identity for this handle instance; replacement handles receive a new identity.
    handle_id: Uuid,
    pub id: Mutex<Option<Uuid>>,
    pub addr: String,
    endpoint: Endpoint,
    pub scanning: Arc<AtomicU32>,
    /// Whether health checking is enabled
    health_check: bool,
    /// Health tracker for connection monitoring
    health: Arc<DiskHealthTracker>,
    /// Cancellation token for monitoring tasks
    cancel_token: CancellationToken,
    recovery_monitor_active: Arc<AtomicBool>,
    #[cfg(test)]
    recovery_monitor_start_count: Arc<AtomicU32>,
    #[cfg(test)]
    recovery_monitor_teardown_hook: Arc<tokio::sync::Mutex<Option<Arc<RecoveryMonitorTeardownHook>>>>,
    data_transport: Arc<dyn InternodeDataTransport>,
}

struct RecoveryMonitorLease {
    active: Arc<AtomicBool>,
}

impl Drop for RecoveryMonitorLease {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct RecoveryMonitorTeardownHook {
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
#[derive(Clone)]
struct RecoveryMonitorTestState {
    start_count: Arc<AtomicU32>,
    teardown_hook: Arc<tokio::sync::Mutex<Option<Arc<RecoveryMonitorTeardownHook>>>>,
}

// ── Connection lifecycle (grpc-optimization P3) ──

/// Whether to prewarm the internode control channel in the background at construction (default off).
fn internode_prewarm_enabled() -> bool {
    rustfs_utils::get_env_bool(rustfs_config::ENV_INTERNODE_PREWARM, rustfs_config::DEFAULT_INTERNODE_PREWARM)
}

/// Whether to fast-fail RPCs to peers already marked offline (default off).
fn internode_offline_bypass_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_OFFLINE_BYPASS,
        rustfs_config::DEFAULT_INTERNODE_OFFLINE_BYPASS,
    )
}

/// Re-probe interval for the offline bypass (>= 1s).
fn internode_offline_reprobe_interval() -> Duration {
    Duration::from_secs(
        rustfs_utils::get_env_u64(
            rustfs_config::ENV_INTERNODE_OFFLINE_REPROBE_SECS,
            rustfs_config::DEFAULT_INTERNODE_OFFLINE_REPROBE_SECS,
        )
        .max(1),
    )
}

/// If the offline bypass is enabled and `addr` is marked offline, return a reason string to
/// fast-fail with instead of paying the connect timeout (grpc-optimization P3-2). Self-healing:
/// one request per re-probe interval is let through so the peer can recover. Shared by the data
/// path (`remote_disk`) and the lock path (`remote_locker`).
pub(crate) fn internode_offline_bypass_reason(addr: &str) -> Option<String> {
    if internode_offline_bypass_enabled()
        && rustfs_io_metrics::internode_metrics::cluster_peer_should_bypass(addr, internode_offline_reprobe_interval())
    {
        return Some(format!("internode peer {addr} offline; fast-fail bypass (P3)"));
    }
    None
}

/// Number of extra attempts for idempotent read-only control-plane and object-read RPCs on
/// transient network failures (grpc-optimization P3-3). Defaults to `1` so a single reset-by-peer
/// during the read-after-write window does not erode read quorum (#2761). Write/lock RPCs never retry.
fn internode_idempotent_read_retries() -> usize {
    rustfs_utils::get_env_usize(
        rustfs_config::ENV_INTERNODE_IDEMPOTENT_READ_RETRIES,
        rustfs_config::DEFAULT_INTERNODE_IDEMPOTENT_READ_RETRIES,
    )
}

/// Peers for which a control-channel prewarm has already been triggered, to dedup the N remote
/// disks that map to a single peer address.
static PREWARMED_PEERS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

/// Best-effort background prewarm of a peer's control channel (grpc-optimization P3-1). Deduped per
/// peer; a dial failure just falls through to the existing lazy connect and recovery monitor.
fn spawn_control_channel_prewarm(addr: String) {
    {
        let Ok(mut prewarmed) = PREWARMED_PEERS.lock() else {
            return;
        };
        if !prewarmed.insert(addr.clone()) {
            return;
        }
    }
    tokio::spawn(async move {
        match node_service_time_out_client_no_auth(&addr).await {
            Ok(_) => debug!(addr = %addr, "internode control channel prewarmed"),
            Err(err) => debug!(addr = %addr, error = %err, "internode control channel prewarm failed (best-effort)"),
        }
    });
}

impl RemoteDisk {
    pub(crate) async fn ns_scanner_server_epoch(&self) -> Result<Option<Uuid>> {
        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }
        let probe = self.data_transport.probe_ns_scanner(NsScannerCapabilityRequest {
            endpoint: self.endpoint.grid_host(),
        });
        let result = timeout(NS_SCANNER_CAPABILITY_PROBE_TIMEOUT, probe)
            .await
            .map_err(|_| DiskError::other("remote namespace scanner capability probe timed out"))?;
        match result {
            Ok(server_epoch) => Ok(Some(server_epoch)),
            // RUSTFS_COMPAT_TODO(ns-scanner-rpc-v3): old peers and legacy transports lack the authenticated startup-epoch handshake. Remove after every supported peer implements namespace scanner protocol v3.
            Err(DiskError::MethodNotAllowed) => Ok(None),
            Err(err)
                if matches!(
                    err.internode_http_error_kind(),
                    Some(rustfs_rio::InternodeHttpErrorKind::HttpStatus(status))
                        if matches!(status.as_u16(), 404 | 405)
                ) =>
            {
                Ok(None)
            }
            Err(err)
                if matches!(
                    err.internode_http_error_kind(),
                    Some(rustfs_rio::InternodeHttpErrorKind::HttpStatus(status)) if status.as_u16() == 426
                ) =>
            {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn open_ns_scanner_stream(&self, request: crate::disk::NsScannerOpenRequest) -> Result<FileReader> {
        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }
        let crate::disk::NsScannerOpenRequest {
            request_id,
            server_epoch,
            session_id,
            session_sequence,
            next_cycle,
            leader_epoch,
            body,
            stall_timeout,
        } = request;
        self.data_transport
            .open_ns_scanner(NsScannerStreamRequest {
                endpoint: self.endpoint.grid_host(),
                disk: self.disk_ref().await,
                request_id,
                server_epoch,
                session_id,
                session_sequence,
                next_cycle,
                leader_epoch,
                body,
                stall_timeout,
            })
            .await
    }

    fn recovery_monitor_span(addr: &str, endpoint: &Endpoint, handle_id: Uuid) -> tracing::Span {
        tracing::info_span!(
            "recovery-monitor",
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            kind = "remote_disk",
            endpoint = %endpoint,
            addr = %addr,
            handle_id = %handle_id
        )
    }

    fn is_retryable_walk_dir_error(err: &DiskError) -> bool {
        if is_network_like_disk_error(err) {
            return true;
        }

        let err_text = err.to_string().to_ascii_lowercase();
        err_text.contains("httpreader stream error") || err_text.contains("error decoding response body")
    }

    fn is_retryable_open_write_error(err: &DiskError) -> bool {
        err.is_retryable_internode_write_failure()
    }

    fn is_retryable_open_read_error(err: &DiskError) -> bool {
        // Opening a read stream is idempotent (no data has been consumed yet), so any transient
        // internode transport failure classified as retryable can be safely re-dialed. The
        // classifier is direction-agnostic — it inspects the InternodeHttpError kind — so it is
        // reused here from the write path.
        err.is_retryable_internode_write_failure()
    }

    pub(crate) async fn new(ep: &Endpoint, opt: &DiskOption, data_transport: Arc<dyn InternodeDataTransport>) -> Result<Self> {
        let addr = if let Some(port) = ep.url.port() {
            format!("{}://{}:{}", ep.url.scheme(), ep.url.host_str().expect("operation should succeed"), port)
        } else {
            format!("{}://{}", ep.url.scheme(), ep.url.host_str().expect("operation should succeed"))
        };

        let env_health_check =
            rustfs_utils::get_env_bool(ENV_RUSTFS_DRIVE_ACTIVE_MONITORING, DEFAULT_RUSTFS_DRIVE_ACTIVE_MONITORING);

        let disk = Self {
            handle_id: Uuid::new_v4(),
            id: Mutex::new(None),
            addr,
            endpoint: ep.clone(),
            scanning: Arc::new(AtomicU32::new(0)),
            health_check: opt.health_check && env_health_check,
            health: Arc::new(DiskHealthTracker::new()),
            cancel_token: CancellationToken::new(),
            recovery_monitor_active: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            recovery_monitor_start_count: Arc::new(AtomicU32::new(0)),
            #[cfg(test)]
            recovery_monitor_teardown_hook: Arc::new(tokio::sync::Mutex::new(None)),
            data_transport,
        };
        record_drive_runtime_state(ep, RuntimeDriveHealthState::Online);

        // P3-1: move the connect cost off the first RPC by prewarming the control channel in the
        // background. Deduped per peer, best-effort, opt-in.
        if internode_prewarm_enabled() {
            spawn_control_channel_prewarm(disk.addr.clone());
        }

        Ok(disk)
    }

    pub fn runtime_state(&self) -> RuntimeDriveHealthState {
        self.health.runtime_state()
    }

    #[cfg(test)]
    fn recovery_monitor_is_active(&self) -> bool {
        self.recovery_monitor_active.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn recovery_monitor_start_count(&self) -> u32 {
        self.recovery_monitor_start_count.load(Ordering::Acquire)
    }

    pub fn offline_duration_secs(&self) -> Option<u64> {
        self.health.offline_duration().map(|duration| duration.as_secs())
    }

    pub fn last_capacity_snapshot(&self) -> Option<(u64, u64, u64, u64)> {
        self.health.last_capacity_snapshot()
    }

    async fn open_write_with_retry(&self, request: WriteStreamRequest) -> Result<FileWriter> {
        let mut attempt = 1;
        let mut last_retry_classification = None;
        loop {
            match self.data_transport.open_write(request.clone()).await {
                Ok(writer) => {
                    if attempt > 1
                        && let Some(classification) = last_retry_classification
                    {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_open_write_retry_success(classification);
                    }
                    return Ok(writer);
                }
                Err(err) if attempt < REMOTE_DISK_OPEN_WRITE_MAX_ATTEMPTS && Self::is_retryable_open_write_error(&err) => {
                    if let Some(classification) = err.internode_http_error_kind() {
                        let classification = classification.metric_label();
                        crate::cluster::rpc::runtime_sources::record_remote_disk_open_write_retry(classification);
                        last_retry_classification = Some(classification);
                    }
                    debug!(
                        endpoint = %request.endpoint,
                        volume = %request.volume,
                        path = %request.path,
                        append = request.append,
                        size = request.size,
                        attempt,
                        "retrying remote open_write after retryable transport error"
                    );
                    tokio::time::sleep(REMOTE_DISK_OPEN_WRITE_RETRY_BACKOFF).await;
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Open a remote read stream with a bounded retry on transient transport failures, mirroring
    /// [`Self::open_write_with_retry`]. A freshly-committed object exists on only `write_quorum`
    /// disks until background heal reconstructs the rest, so a single transient network error
    /// (BrokenPipe/ConnectionReset/reset-by-peer) on a shard read during that read-after-write
    /// window can drop the readable-shard count below `data_shards` and surface as a spurious
    /// `InsufficientReadQuorum`. Opening the stream is idempotent, so re-dialing once absorbs the
    /// transient failure instead of eroding quorum. See issue #2761.
    async fn open_read_with_retry(&self, request: ReadStreamRequest) -> Result<FileReader> {
        let mut attempt = 1;
        let mut last_retry_classification = None;
        loop {
            match self.data_transport.open_read(request.clone()).await {
                Ok(reader) => {
                    if attempt > 1
                        && let Some(classification) = last_retry_classification
                    {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_open_read_retry_success(classification);
                    }
                    return Ok(reader);
                }
                Err(err) if attempt < REMOTE_DISK_OPEN_READ_MAX_ATTEMPTS && Self::is_retryable_open_read_error(&err) => {
                    if let Some(classification) = err.internode_http_error_kind() {
                        let classification = classification.metric_label();
                        crate::cluster::rpc::runtime_sources::record_remote_disk_open_read_retry(classification);
                        last_retry_classification = Some(classification);
                    }
                    debug!(
                        endpoint = %request.endpoint,
                        volume = %request.volume,
                        path = %request.path,
                        offset = request.offset,
                        length = request.length,
                        attempt,
                        "retrying remote open_read after retryable transport error"
                    );
                    tokio::time::sleep(REMOTE_DISK_OPEN_READ_RETRY_BACKOFF).await;
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn open_read_chunks_with_retry(&self, request: ReadStreamRequest) -> Result<Option<rustfs_rio::ChunkReaderBox>> {
        let mut attempt = 1;
        let mut last_retry_classification = None;
        loop {
            match self.data_transport.open_read_chunks(request.clone()).await {
                Ok(reader) => {
                    if attempt > 1
                        && let Some(classification) = last_retry_classification
                    {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_open_read_retry_success(classification);
                    }
                    return Ok(reader);
                }
                Err(err) if attempt < REMOTE_DISK_OPEN_READ_MAX_ATTEMPTS && Self::is_retryable_open_read_error(&err) => {
                    if let Some(classification) = err.internode_http_error_kind() {
                        let classification = classification.metric_label();
                        crate::cluster::rpc::runtime_sources::record_remote_disk_open_read_retry(classification);
                        last_retry_classification = Some(classification);
                    }
                    tokio::time::sleep(REMOTE_DISK_OPEN_READ_RETRY_BACKOFF).await;
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn record_capacity_probe(&self, total: u64, used: u64, free: u64) {
        self.health.record_capacity_probe(total, used, free);
    }

    #[cfg(test)]
    pub fn force_runtime_state_for_test(&self, state: RuntimeDriveHealthState) {
        self.health.force_runtime_state_for_test(state);
    }

    /// Same as [`DiskHealthTracker::reset_for_store_init_retry`]: undo a transient faulty mark before another format load attempt.
    pub fn reset_health_for_store_init_retry(&self) {
        self.health.reset_for_store_init_retry(&self.endpoint);
    }

    #[cfg(test)]
    pub fn health_check_enabled_for_test(&self) -> bool {
        self.health_check
    }

    fn spawn_recovery_monitor_if_needed(&self) {
        if !self.health_check {
            return;
        }

        Self::schedule_recovery_monitor(
            self.addr.clone(),
            self.endpoint.clone(),
            self.handle_id,
            Arc::clone(&self.health),
            self.cancel_token.clone(),
            Arc::clone(&self.recovery_monitor_active),
            #[cfg(test)]
            RecoveryMonitorTestState {
                start_count: Arc::clone(&self.recovery_monitor_start_count),
                teardown_hook: Arc::clone(&self.recovery_monitor_teardown_hook),
            },
        );
    }

    fn schedule_recovery_monitor(
        addr: String,
        endpoint: Endpoint,
        handle_id: Uuid,
        health: Arc<DiskHealthTracker>,
        cancel_token: CancellationToken,
        active: Arc<AtomicBool>,
        #[cfg(test)] test_state: RecoveryMonitorTestState,
    ) {
        if active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let span = Self::recovery_monitor_span(&addr, &endpoint, handle_id);
        super::spawn_background_monitor(span, async move {
            #[cfg(test)]
            test_state.start_count.fetch_add(1, Ordering::AcqRel);
            let lease = RecoveryMonitorLease {
                active: Arc::clone(&active),
            };
            Self::monitor_remote_disk_recovery(addr.clone(), endpoint.clone(), Arc::clone(&health), cancel_token.clone()).await;
            #[cfg(test)]
            if let Some(hook) = test_state.teardown_hook.lock().await.take() {
                hook.arrived.notify_one();
                hook.release.notified().await;
            }
            drop(lease);
            if !cancel_token.is_cancelled() && health.runtime_state() != RuntimeDriveHealthState::Online {
                Self::schedule_recovery_monitor(
                    addr,
                    endpoint,
                    handle_id,
                    health,
                    cancel_token,
                    active,
                    #[cfg(test)]
                    test_state,
                );
            }
        });
    }

    #[cfg(test)]
    fn spawn_recovery_monitor_log_probe_for_test(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let endpoint = self.endpoint.clone();
        let addr = self.addr.clone();
        let span = Self::recovery_monitor_span(&addr, &endpoint, self.handle_id);
        super::spawn_background_monitor(span, async move {
            warn!(
                event = EVENT_REMOTE_DISK_HEALTH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %endpoint,
                addr,
                state = "probe",
                "remote disk recovery monitor log probe"
            );
            let _ = tx.send(());
        });
        rx
    }

    fn mark_suspect_or_offline(&self, reason: &'static str) -> bool {
        self.health.mark_failure(&self.endpoint, reason)
    }

    /// Enable health monitoring after disk creation.
    /// Used to defer health checks until after startup format loading completes,
    /// so that remote peers have time to come online.
    pub fn enable_health_check(&self) {
        if !self.health_check {
            return;
        }
        let health = Arc::clone(&self.health);
        let cancel_token = self.cancel_token.clone();
        let addr = self.addr.clone();
        let endpoint = self.endpoint.clone();
        let handle_id = self.handle_id;
        let recovery_monitor_active = Arc::clone(&self.recovery_monitor_active);
        #[cfg(test)]
        let recovery_monitor_teardown_hook = Arc::clone(&self.recovery_monitor_teardown_hook);

        tokio::spawn(async move {
            Self::monitor_remote_disk_health(
                addr,
                endpoint,
                handle_id,
                health,
                cancel_token,
                recovery_monitor_active,
                #[cfg(test)]
                recovery_monitor_teardown_hook,
            )
            .await;
        });
    }

    /// Monitor remote disk health periodically
    async fn monitor_remote_disk_health(
        addr: String,
        endpoint: Endpoint,
        handle_id: Uuid,
        health: Arc<DiskHealthTracker>,
        cancel_token: CancellationToken,
        recovery_monitor_active: Arc<AtomicBool>,
        #[cfg(test)] recovery_monitor_teardown_hook: Arc<tokio::sync::Mutex<Option<Arc<RecoveryMonitorTeardownHook>>>>,
    ) {
        let mut interval = time::interval(get_drive_active_check_interval());

        // Perform basic connectivity check
        let initial_probe_ok = Self::perform_connectivity_check(&addr).await.is_ok();
        if initial_probe_ok {
            health.record_operation_success(&endpoint, "connectivity_probe_success");
        } else if health.mark_failure(&endpoint, "connectivity_probe_failed") {
            warn!(
                event = EVENT_REMOTE_DISK_HEALTH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %endpoint,
                addr,
                state = "initial_probe_failed",
                result = "mark_faulty",
                "Remote disk initial health probe failed"
            );

            // Start recovery monitoring
            let health_clone = Arc::clone(&health);
            let addr_clone = addr.clone();
            let endpoint_clone = endpoint.clone();
            let cancel_clone = cancel_token.clone();
            Self::schedule_recovery_monitor(
                addr_clone,
                endpoint_clone,
                handle_id,
                health_clone,
                cancel_clone,
                Arc::clone(&recovery_monitor_active),
                #[cfg(test)]
                RecoveryMonitorTestState {
                    start_count: Arc::new(AtomicU32::new(0)),
                    teardown_hook: Arc::clone(&recovery_monitor_teardown_hook),
                },
            );
        }

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(
                        event = EVENT_REMOTE_DISK_HEALTH,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                        endpoint = %endpoint,
                        addr,
                        state = "monitor_cancelled",
                        "Remote disk health monitor cancelled"
                    );
                    return;
                }
                _ = interval.tick() => {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    // Skip health check if disk is already marked as faulty
                    if health.is_faulty() {
                        continue;
                    }

                    let last_success_nanos = health.last_success.load(Ordering::Relaxed);
                    let elapsed = Duration::from_nanos(
                        (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("operation should succeed")
                            .as_nanos() as i64 - last_success_nanos) as u64
                    );

                    if elapsed < SKIP_IF_SUCCESS_BEFORE {
                        continue;
                    }

                    // Perform basic connectivity check
                    if Self::perform_connectivity_check(&addr).await.is_ok() {
                        health.record_operation_success(&endpoint, "connectivity_probe_success");
                    } else if health.mark_failure(&endpoint, "connectivity_probe_failed") {
                        warn!(
                            event = EVENT_REMOTE_DISK_HEALTH,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                            endpoint = %endpoint,
                            addr,
                            state = "probe_failed",
                            result = "mark_faulty",
                            "Remote disk health probe failed"
                        );

                        // Start recovery monitoring
                        let health_clone = Arc::clone(&health);
                        let addr_clone = addr.clone();
                        let endpoint_clone = endpoint.clone();
                        let cancel_clone = cancel_token.clone();
                        Self::schedule_recovery_monitor(
                            addr_clone,
                            endpoint_clone,
                            handle_id,
                            health_clone,
                            cancel_clone,
                            Arc::clone(&recovery_monitor_active),
                            #[cfg(test)]
                            RecoveryMonitorTestState {
                                start_count: Arc::new(AtomicU32::new(0)),
                                teardown_hook: Arc::clone(&recovery_monitor_teardown_hook),
                            },
                        );
                    }
                }
            }
        }
    }

    /// Monitor remote disk recovery and mark as healthy when recovered
    async fn monitor_remote_disk_recovery(
        addr: String,
        endpoint: Endpoint,
        health: Arc<DiskHealthTracker>,
        cancel_token: CancellationToken,
    ) {
        let mut interval = time::interval(get_drive_returning_probe_interval());
        debug!(
            event = EVENT_REMOTE_DISK_HEALTH,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %endpoint,
            addr,
            state = "recovery_monitor_started",
            "Remote disk recovery monitor started"
        );

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(
                        event = EVENT_REMOTE_DISK_HEALTH,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                        endpoint = %endpoint,
                        addr,
                        state = "recovery_monitor_cancelled",
                        "Remote disk recovery monitor cancelled"
                    );
                    return;
                }
                _ = interval.tick() => {
                    if Self::perform_recovery_probe(&addr, &endpoint).await.is_ok() {
                        let became_online = health.mark_recovery_success(&endpoint, "disk_info_probe_success");
                        debug!(
                            event = EVENT_REMOTE_DISK_HEALTH,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                            endpoint = %endpoint,
                            addr,
                            state = "recovery_probe_succeeded",
                            "Remote disk recovery probe succeeded"
                        );
                        if became_online {
                            debug!(
                                event = EVENT_REMOTE_DISK_HEALTH,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                                endpoint = %endpoint,
                                addr,
                                state = "recovered",
                                "Remote disk recovered"
                            );
                            return;
                        }
                    } else {
                        health.mark_failure(&endpoint, "disk_info_probe_failed");
                    }
                }
            }
        }
    }

    async fn perform_recovery_probe(addr: &str, endpoint: &Endpoint) -> Result<()> {
        let mut evict_cached_connection = false;
        let result = match timeout(get_drive_active_check_timeout(), async {
            let opts = serde_json::to_string(&DiskInfoOptions {
                noop: true,
                ..Default::default()
            })
            .map_err(|err| (Error::other(format!("encode DiskInfoOptions failed: {err}")), false))?;
            let addr = addr.to_string();
            let mut client = node_service_time_out_client(&addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
                .await
                .map_err(|err| (Error::other(format!("can not get client, err: {err}")), true))?;
            let request = Request::new(DiskInfoRequest {
                disk: endpoint.to_string(),
                opts,
            });
            let response = client
                .disk_info(request)
                .await
                .map_err(|err| (Error::from(err), true))?
                .into_inner();
            if !response.success {
                return Err((response.error.unwrap_or_default().into(), false));
            }
            Ok(())
        })
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err((err, should_evict))) => {
                evict_cached_connection = should_evict;
                Err(err)
            }
            Err(_) => {
                evict_cached_connection = true;
                Err(DiskError::Timeout)
            }
        };

        if evict_cached_connection {
            evict_failed_connection(addr).await;
        }

        result
    }

    /// Perform basic connectivity check for remote disk
    async fn perform_connectivity_check(addr: &str) -> Result<()> {
        let url = url::Url::parse(addr).map_err(|e| Error::other(format!("Invalid URL: {e}")))?;

        let Some(host) = url.host_str() else {
            return Err(Error::other("No host in URL".to_string()));
        };

        let port = url.port_or_known_default().unwrap_or(80);

        // Try to establish TCP connection
        match timeout(get_drive_active_check_timeout(), TcpStream::connect((host, port))).await {
            Ok(Ok(stream)) => {
                drop(stream);
                Ok(())
            }
            _ => Err(Error::other(format!("Cannot connect to {host}:{port}"))),
        }
    }

    /// Execute operation with timeout and health tracking
    async fn execute_with_timeout<T, F, Fut>(&self, operation: F, timeout_duration: Duration) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.execute_with_timeout_for_op("unknown", operation, timeout_duration).await
    }

    /// Execute an **idempotent, read-only/reentrant** RPC with a bounded number of retries on
    /// transient network failures, with exponential backoff (grpc-optimization P3-3). Retries
    /// default to 1 (see [`internode_idempotent_read_retries`]). MUST NOT be used for write/lock
    /// RPCs — those must never auto-retry (quorum/idempotency safety). The `operation` closure is
    /// re-invoked per attempt, so it must be `Fn` (rebuild the request from borrowed inputs, do not
    /// move captured state out). Attempts and backoff share one total timeout budget.
    async fn execute_read_with_retry<T, F, Fut>(&self, op: &'static str, operation: F, timeout_duration: Duration) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let deadline = (!timeout_duration.is_zero()).then(|| {
            time::Instant::now()
                .checked_add(timeout_duration)
                .unwrap_or_else(|| time::sleep(timeout_duration).deadline())
        });
        let max_retries = internode_idempotent_read_retries();
        let mut attempt = 0usize;
        loop {
            let attempt_timeout = deadline
                .map(|deadline| deadline.saturating_duration_since(time::Instant::now()))
                .unwrap_or(Duration::ZERO);
            if deadline.is_some() && attempt_timeout.is_zero() {
                self.record_timeout(op, timeout_duration);
                return Err(DiskError::Timeout);
            }

            let health_action = if attempt >= max_retries {
                FailureHealthAction::MarkFailure
            } else {
                FailureHealthAction::IgnoreFailure
            };
            match self
                .execute_with_timeout_for_op_and_health_action(op, &operation, attempt_timeout, health_action)
                .await
            {
                Err(err) if attempt < max_retries && is_network_like_disk_error(&err) => {
                    if matches!(err, DiskError::Timeout) && deadline.is_some_and(|deadline| time::Instant::now() >= deadline) {
                        self.mark_faulty("read_operation_deadline");
                        return Err(err);
                    }
                    attempt += 1;
                    let backoff = REMOTE_DISK_READ_RETRY_BASE_BACKOFF
                        .saturating_mul(1u32 << u32::try_from(attempt - 1).unwrap_or(4).min(4));
                    if deadline.is_some_and(|deadline| deadline.saturating_duration_since(time::Instant::now()) <= backoff) {
                        attempt = max_retries;
                        continue;
                    }
                    debug!(
                        endpoint = %self.endpoint,
                        addr = %self.addr,
                        op,
                        attempt,
                        "retrying idempotent read-only RPC after transient network error"
                    );
                    if let Some(deadline) = deadline {
                        if time::timeout_at(deadline, time::sleep(backoff)).await.is_err() {
                            self.record_timeout(op, timeout_duration);
                            return Err(DiskError::Timeout);
                        }
                    } else {
                        time::sleep(backoff).await;
                    }
                    if self.health.is_faulty() {
                        return Err(DiskError::FaultyDisk);
                    }
                }
                other => return other,
            }
        }
    }

    async fn execute_with_timeout_for_op<T, F, Fut>(
        &self,
        op: &'static str,
        operation: F,
        timeout_duration: Duration,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.execute_with_timeout_for_op_and_health_action(op, operation, timeout_duration, FailureHealthAction::MarkFailure)
            .await
    }

    async fn execute_with_timeout_for_op_and_health_action<T, F, Fut>(
        &self,
        op: &'static str,
        operation: F,
        timeout_duration: Duration,
        failure_health_action: FailureHealthAction,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check if disk is faulty
        if self.health.is_faulty() {
            debug!(
                event = EVENT_REMOTE_DISK_HEALTH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %self.endpoint,
                addr = %self.addr,
                handle_id = %self.handle_id,
                op,
                state = "faulty_short_circuit",
                "Remote disk operation short-circuited by faulty state"
            );
            return Err(DiskError::FaultyDisk);
        }

        // Record operation start
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("operation should succeed")
            .as_nanos() as i64;
        self.health.last_started.store(now, std::sync::atomic::Ordering::Relaxed);
        let _waiting_guard = self.health.waiting_guard();

        if timeout_duration == Duration::ZERO {
            let operation_result = operation().await;
            if operation_result.is_ok() {
                self.health.log_success();
            }
            self.handle_network_like_error(op, timeout_duration, &operation_result, failure_health_action)
                .await;
            return operation_result;
        }

        // Execute operation with timeout
        let result = time::timeout(timeout_duration, operation()).await;

        match result {
            Ok(operation_result) => {
                // Log success; the waiting guard balances every exit path.
                if operation_result.is_ok() {
                    self.health.log_success();
                }
                self.handle_network_like_error(op, timeout_duration, &operation_result, failure_health_action)
                    .await;
                operation_result
            }
            Err(_) => {
                self.record_timeout(op, timeout_duration);
                if failure_health_action == FailureHealthAction::MarkFailure {
                    self.mark_faulty_and_evict("operation_timeout").await;
                }
                Err(DiskError::Timeout)
            }
        }
    }

    fn record_timeout(&self, op: &'static str, timeout_duration: Duration) {
        counter!(
            "rustfs_drive_op_timeout_total",
            "endpoint" => self.endpoint.to_string(),
            "op" => op.to_string()
        )
        .increment(1);
        warn!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            addr = %self.addr,
            op,
            timeout_ms = timeout_duration.as_millis(),
            state = "timeout",
            "Remote disk operation timed out"
        );
    }

    async fn handle_network_like_error<T>(
        &self,
        op: &'static str,
        timeout_duration: Duration,
        operation_result: &Result<T>,
        failure_health_action: FailureHealthAction,
    ) {
        if let Err(err) = operation_result
            && is_network_like_disk_error(err)
        {
            counter!(
                "rustfs_drive_op_network_error_total",
                "endpoint" => self.endpoint.to_string(),
                "op" => op.to_string()
            )
            .increment(1);
            warn!(
                event = EVENT_REMOTE_DISK_RPC,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %self.endpoint,
                addr = %self.addr,
                op,
                timeout_ms = timeout_duration.as_millis(),
                state = "network_like_error",
                "Remote disk operation returned network-like error"
            );
            if failure_health_action == FailureHealthAction::MarkFailure {
                self.mark_faulty_and_evict("operation_network_error").await;
            }
        }
    }

    fn mark_faulty(&self, reason: &'static str) -> bool {
        let previous_state = self.runtime_state();
        let transitioned_to_offline = self.mark_suspect_or_offline(reason);
        let state = self.runtime_state();

        if state != previous_state {
            self.spawn_recovery_monitor_if_needed();
            counter!(
                "rustfs_drive_faulty_mark_total",
                "endpoint" => self.endpoint.to_string(),
                "reason" => reason.to_string()
            )
            .increment(1);
            if transitioned_to_offline {
                warn!(
                    event = EVENT_REMOTE_DISK_HEALTH,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                    endpoint = %self.endpoint,
                    addr = %self.addr,
                    reason,
                    state = "marked_faulty",
                    "Remote disk marked faulty"
                );
            } else {
                warn!(
                    event = EVENT_REMOTE_DISK_HEALTH,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                    endpoint = %self.endpoint,
                    addr = %self.addr,
                    reason,
                    runtime_state = ?state,
                    state = "marked_suspect",
                    "Remote disk marked suspect"
                );
            }
        }
        state != previous_state
    }

    async fn mark_faulty_and_evict(&self, reason: &'static str) {
        if self.mark_faulty(reason) {
            counter!(
                "rustfs_drive_connection_evict_total",
                "endpoint" => self.endpoint.to_string(),
                "reason" => reason.to_string()
            )
            .increment(1);
            debug!(
                event = EVENT_REMOTE_DISK_HEALTH,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %self.endpoint,
                addr = %self.addr,
                reason,
                state = "evict_cached_connection",
                "Remote disk cached connection evicted"
            );
            evict_failed_connection(&self.addr).await;
        }
    }

    /// P3-2 offline bypass: when enabled and this peer is marked offline, fast-fail instead of
    /// paying the connect timeout, so the erasure layer proceeds on quorum sooner. This does not
    /// change quorum. Self-healing — one request per re-probe interval is let through so the peer
    /// recovers even without a background monitor. The recovery monitor's own probe path calls the
    /// client directly and is unaffected.
    fn offline_bypass_error(&self) -> Option<Error> {
        internode_offline_bypass_reason(&self.addr).map(Error::other)
    }

    async fn get_client(&self) -> Result<NodeServiceClient<InterceptedService<AuthenticatedChannel, TonicInterceptor>>> {
        if let Some(err) = self.offline_bypass_error() {
            return Err(err);
        }
        node_service_time_out_client(&self.addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))
    }

    /// Client for large `bytes`-carrying RPCs (ReadAll/WriteAll/ReadMultiple/BatchReadVersion).
    /// Routes onto the isolated bulk channel pool so large transfers cannot head-of-line block
    /// lock/health RPCs (grpc-optimization P1). Falls back to the control channel when isolation
    /// is disabled.
    async fn get_bulk_client(&self) -> Result<NodeServiceClient<InterceptedService<AuthenticatedChannel, TonicInterceptor>>> {
        if let Some(err) = self.offline_bypass_error() {
            return Err(err);
        }
        node_service_time_out_client_for_class(
            &self.addr,
            TonicInterceptor::Signature(gen_tonic_signature_interceptor()),
            ChannelClass::Bulk,
        )
        .await
        .map_err(|err| Error::other(format!("can not get client, err: {err}")))
    }

    async fn disk_ref(&self) -> String {
        (*self.id.lock().await)
            .map(|id| id.to_string())
            .unwrap_or_else(|| self.endpoint.to_string())
    }
}

/// Initial capacity hint (bytes) for msgpack encode buffers, sized to cover a typical single-
/// request without repeated growth reallocations. Larger payloads still grow as needed.
const MSGPACK_ENCODE_CAPACITY_HINT: usize = 512;
const FILE_INFO_MSGPACK_ENCODE_CAPACITY_HINT: usize = 1024;

fn encode_msgpack_with_capacity<T: Serialize>(value: &T, capacity: usize) -> Result<Vec<u8>> {
    let mut serializer = rmp_serde::Serializer::new(Vec::with_capacity(capacity));
    value.serialize(&mut serializer)?;
    Ok(serializer.into_inner())
}

fn encode_msgpack<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    encode_msgpack_with_capacity(value, MSGPACK_ENCODE_CAPACITY_HINT)
}

fn encode_file_info_msgpack(value: &FileInfo) -> Result<Vec<u8>> {
    encode_msgpack_with_capacity(value, FILE_INFO_MSGPACK_ENCODE_CAPACITY_HINT)
}

fn encode_file_info_versions_msgpack(value: &FileInfoVersions) -> Result<Vec<u8>> {
    let version_count = value.versions.len().saturating_add(value.free_versions.len());
    let capacity =
        MSGPACK_ENCODE_CAPACITY_HINT.saturating_add(FILE_INFO_MSGPACK_ENCODE_CAPACITY_HINT.saturating_mul(version_count));
    encode_msgpack_with_capacity(value, capacity)
}

/// JSON compatibility string for a dual-encoded (`_bin` + text) request field. Returns an empty
/// string only when msgpack-only mode and its explicit fleet confirmation guard are both enabled;
/// otherwise the legacy JSON encoding is retained for old peers.
fn compat_json<T: Serialize>(value: &T) -> Result<String> {
    if rustfs_protos::internode_rpc_msgpack_only() {
        return Ok(String::new());
    }
    Ok(serde_json::to_string(value)?)
}

fn decode_msgpack_or_json<T: DeserializeOwned>(binary: &[u8], json: &str, value_name: &'static str) -> Result<T> {
    if !binary.is_empty() {
        let mut deserializer = rmp_serde::Deserializer::new(Cursor::new(binary));
        return match T::deserialize(&mut deserializer) {
            Ok(value) => {
                crate::cluster::rpc::runtime_sources::record_response_msgpack_decode(value_name);
                Ok(value)
            }
            Err(err) => {
                crate::cluster::rpc::runtime_sources::record_response_msgpack_decode_error(value_name);
                Err(Error::from(err))
            }
        };
    }

    // The msgpack payload was absent, so fall back to the JSON compatibility field. This branch
    // must read zero across a release window before the redundant JSON fields can be dropped (P2).
    crate::cluster::rpc::runtime_sources::record_response_json_fallback(value_name);
    match serde_json::from_str(json) {
        Ok(value) => {
            crate::cluster::rpc::runtime_sources::record_response_json_decode(value_name);
            Ok(value)
        }
        Err(err) => {
            crate::cluster::rpc::runtime_sources::record_response_json_decode_error(value_name);
            Err(Error::from(err))
        }
    }
}

fn read_version_stage_timer(attribution_enabled: bool) -> Option<Instant> {
    attribution_enabled.then(Instant::now)
}

fn record_read_version_stage(stage: &'static str, started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_version_stage(stage, started_at.elapsed());
    }
}

fn record_batch_read_version_stage(stage: &'static str, started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_batch_read_version_stage(stage, started_at.elapsed());
    }
}

/// Aggregate encoded size (bytes) of a `ReadMultiple` response, preferring the msgpack payloads
/// and falling back to the JSON compatibility strings. Used to size the RPC for the payload
/// histogram / large-payload alerting (grpc-optimization P0 instrumentation).
fn read_multiple_response_payload_len(response: &ReadMultipleResponse) -> usize {
    if !response.read_multiple_resps_bin.is_empty() {
        response.read_multiple_resps_bin.iter().map(|buf| buf.len()).sum()
    } else {
        response.read_multiple_resps.iter().map(|item| item.len()).sum()
    }
}

fn decode_read_multiple_response_items(response: ReadMultipleResponse, endpoint: &Endpoint) -> Result<Vec<ReadMultipleResp>> {
    if !response.read_multiple_resps_bin.is_empty() {
        if !response.read_multiple_resps.is_empty()
            && response.read_multiple_resps.len() != response.read_multiple_resps_bin.len()
        {
            warn!(
                event = EVENT_REMOTE_DISK_RPC,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %endpoint,
                json_count = response.read_multiple_resps.len(),
                msgpack_count = response.read_multiple_resps_bin.len(),
                op = "read_multiple",
                state = "response_count_mismatch",
                "Remote disk ReadMultiple compatibility payload counts differ"
            );
        }

        let mut read_multiple_resps = Vec::with_capacity(response.read_multiple_resps_bin.len());
        for (index, buf) in response.read_multiple_resps_bin.iter().enumerate() {
            let resp = decode_msgpack_or_json::<ReadMultipleResp>(buf, "", "ReadMultipleResp").map_err(|err| {
                Error::other(format!("decode ReadMultipleResp msgpack item {index} from {endpoint} failed: {err}"))
            })?;
            read_multiple_resps.push(resp);
        }
        return Ok(read_multiple_resps);
    }

    // No msgpack payloads present: the whole list fell back to the JSON compatibility field (P2).
    if !response.read_multiple_resps.is_empty() {
        crate::cluster::rpc::runtime_sources::record_response_json_fallback("ReadMultipleResp");
    }
    let mut read_multiple_resps = Vec::with_capacity(response.read_multiple_resps.len());
    for (index, json_str) in response.read_multiple_resps.iter().enumerate() {
        let resp = serde_json::from_str::<ReadMultipleResp>(json_str).map_err(|err| {
            crate::cluster::rpc::runtime_sources::record_response_json_decode_error("ReadMultipleResp");
            Error::other(format!("decode ReadMultipleResp json item {index} from {endpoint} failed: {err}"))
        })?;
        crate::cluster::rpc::runtime_sources::record_response_json_decode("ReadMultipleResp");
        read_multiple_resps.push(resp);
    }

    Ok(read_multiple_resps)
}

fn decode_batch_read_version_response_items(
    response: BatchReadVersionResponse,
    endpoint: &Endpoint,
) -> Result<Vec<BatchReadVersionResp>> {
    if !response.batch_read_version_resps_bin.is_empty() {
        if !response.batch_read_version_resps.is_empty()
            && response.batch_read_version_resps.len() != response.batch_read_version_resps_bin.len()
        {
            warn!(
                event = EVENT_REMOTE_DISK_RPC,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                endpoint = %endpoint,
                json_count = response.batch_read_version_resps.len(),
                msgpack_count = response.batch_read_version_resps_bin.len(),
                op = "batch_read_version",
                state = "response_count_mismatch",
                "Remote disk BatchReadVersion compatibility payload counts differ"
            );
        }

        let mut batch_read_version_resps = Vec::with_capacity(response.batch_read_version_resps_bin.len());
        for (index, buf) in response.batch_read_version_resps_bin.iter().enumerate() {
            let resp = decode_msgpack_or_json::<BatchReadVersionResp>(buf, "", "BatchReadVersionResp").map_err(|err| {
                Error::other(format!("decode BatchReadVersionResp msgpack item {index} from {endpoint} failed: {err}"))
            })?;
            if resp.success {
                validate_decoded_file_info(&resp.file_info)?;
            }
            batch_read_version_resps.push(resp);
        }
        return Ok(batch_read_version_resps);
    }

    // No msgpack payloads present: the whole list fell back to the JSON compatibility field (P2).
    if !response.batch_read_version_resps.is_empty() {
        crate::cluster::rpc::runtime_sources::record_response_json_fallback("BatchReadVersionResp");
    }
    let mut batch_read_version_resps = Vec::with_capacity(response.batch_read_version_resps.len());
    for (index, json_str) in response.batch_read_version_resps.iter().enumerate() {
        let resp = serde_json::from_str::<BatchReadVersionResp>(json_str).map_err(|err| {
            crate::cluster::rpc::runtime_sources::record_response_json_decode_error("BatchReadVersionResp");
            Error::other(format!("decode BatchReadVersionResp json item {index} from {endpoint} failed: {err}"))
        })?;
        crate::cluster::rpc::runtime_sources::record_response_json_decode("BatchReadVersionResp");
        if resp.success {
            validate_decoded_file_info(&resp.file_info)?;
        }
        batch_read_version_resps.push(resp);
    }

    Ok(batch_read_version_resps)
}

fn batch_read_version_request_payload_len(req: &BatchReadVersionReq, req_json: &str, req_bin: &[u8]) -> usize {
    req.items
        .iter()
        .fold(req_json.len().saturating_add(req_bin.len()), |total, item| {
            total
                .saturating_add(item.org_volume.len())
                .saturating_add(item.volume.len())
                .saturating_add(item.path.len())
                .saturating_add(item.version_id.len())
        })
}

fn batch_read_version_response_payload_len(response: &BatchReadVersionResponse) -> usize {
    response
        .batch_read_version_resps
        .iter()
        .map(String::len)
        .sum::<usize>()
        .saturating_add(response.batch_read_version_resps_bin.iter().map(Bytes::len).sum::<usize>())
}

fn validate_decoded_file_info(file_info: &FileInfo) -> Result<()> {
    file_info.validate_for_metadata_read().map_err(Into::into)
}

impl RemoteDisk {
    #[tracing::instrument(level = "trace", skip_all)]
    pub(crate) async fn rename_data_borrowed(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: &FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            src_volume,
            src_path,
            dst_volume,
            dst_path,
            op = "rename_data",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout_for_op(
            "rename_data",
            || async {
                let file_info = compat_json(fi)?;
                let file_info_bin = encode_file_info_msgpack(fi)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(RenameDataRequest {
                    disk: self.endpoint.to_string(),
                    src_volume: src_volume.to_string(),
                    src_path: src_path.to_string(),
                    file_info,
                    dst_volume: dst_volume.to_string(),
                    dst_path: dst_path.to_string(),
                    file_info_bin: file_info_bin.into(),
                });
                let canonical_body = rustfs_protos::canonical_rename_data_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "rename_data")?;

                let response = client.rename_data(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let rename_data_resp = decode_msgpack_or_json::<RenameDataResp>(
                    &response.rename_data_resp_bin,
                    &response.rename_data_resp,
                    "RenameDataResp",
                )?;

                Ok(rename_data_resp)
            },
            get_max_timeout_duration(),
        )
        .await
    }
}

#[async_trait::async_trait]
impl DiskAPI for RemoteDisk {
    #[tracing::instrument(level = "trace", skip_all)]
    fn to_string(&self) -> String {
        self.endpoint.to_string()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn is_online(&self) -> bool {
        // If disk is marked as faulty, consider it offline
        !self.health.is_faulty()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn is_local(&self) -> bool {
        false
    }
    #[tracing::instrument(level = "trace", skip_all)]
    fn host_name(&self) -> String {
        self.endpoint.host_port()
    }
    #[tracing::instrument(level = "trace", skip_all)]
    fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }
    #[tracing::instrument(level = "trace", skip_all)]
    async fn close(&self) -> Result<()> {
        self.cancel_token.cancel();
        Ok(())
    }
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        Ok(*self.id.lock().await)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        let mut lock = self.id.lock().await;
        *lock = id;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn path(&self) -> PathBuf {
        PathBuf::from(self.endpoint.get_file_path())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn get_disk_location(&self) -> DiskLocation {
        DiskLocation {
            pool_idx: {
                if self.endpoint.pool_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.pool_idx as usize)
                }
            },
            set_idx: {
                if self.endpoint.set_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.set_idx as usize)
                }
            },
            disk_idx: {
                if self.endpoint.disk_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.disk_idx as usize)
                }
            },
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn make_volume(&self, volume: &str) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            op = "make_volume",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(MakeVolumeRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                });
                let canonical_body = rustfs_protos::canonical_make_volume_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "make_volume")?;

                let response = client.make_volume(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume_count = volumes.len(),
            op = "make_volumes",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(MakeVolumesRequest {
                    disk: self.endpoint.to_string(),
                    volumes: volumes.iter().map(|s| (*s).to_string()).collect(),
                });
                let canonical_body = rustfs_protos::canonical_make_volumes_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "make_volumes")?;

                let response = client.make_volumes(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            op = "list_volumes",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ListVolumesRequest {
                    disk: self.endpoint.to_string(),
                });

                let response = client.list_volumes(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let infos = decode_volume_infos(response.volume_infos)?;

                Ok(infos)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            op = "stat_volume",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(StatVolumeRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                });

                let response = client.stat_volume(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let volume_info = serde_json::from_str::<VolumeInfo>(&response.volume_info)?;

                Ok(volume_info)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete_volume(&self, volume: &str, force_delete: bool) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            op = "delete_volume",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(DeleteVolumeRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    force: force_delete,
                });
                let canonical_body = rustfs_protos::canonical_delete_volume_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "delete_volume")?;

                let response = client.delete_volume(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "delete_version",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                // `_bin` support for DeleteVersion is new (grpc-optimization P2); always dual-write
                // JSON + msgpack until its fallback counter has read zero across a release window.
                let file_info_bin = encode_file_info_msgpack(&fi)?;
                let opts_bin = encode_msgpack(&opts)?;
                let file_info = serde_json::to_string(&fi)?;
                let opts = serde_json::to_string(&opts)?;

                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(DeleteVersionRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                    force_del_marker,
                    opts,
                    file_info_bin: file_info_bin.into(),
                    opts_bin: opts_bin.into(),
                });
                let canonical_body = rustfs_protos::canonical_delete_version_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "delete_version")?;

                let response = client.delete_version(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                // let raw_file_info = serde_json::from_str::<RawFileInfo>(&response.raw_file_info)?;

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete_versions(&self, volume: &str, versions: Vec<FileInfoVersions>, opts: DeleteOptions) -> Vec<Option<Error>> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            version_count = versions.len(),
            op = "delete_versions",
            state = "started",
            "Remote disk RPC started"
        );

        if self.health.is_faulty() {
            return vec![Some(DiskError::FaultyDisk); versions.len()];
        }

        // `_bin` support for DeleteVersions is new (grpc-optimization P2); always dual-write JSON +
        // msgpack until its fallback counter has read zero across a release window.
        let opts_bin = match encode_msgpack(&opts) {
            Ok(opts_bin) => opts_bin,
            Err(err) => {
                let mut errors = Vec::with_capacity(versions.len());
                for _ in 0..versions.len() {
                    errors.push(Some(Error::other(err.to_string())));
                }
                return errors;
            }
        };
        let opts = match serde_json::to_string(&opts) {
            Ok(opts) => opts,
            Err(err) => {
                let mut errors = Vec::with_capacity(versions.len());
                for _ in 0..versions.len() {
                    errors.push(Some(Error::other(err.to_string())));
                }
                return errors;
            }
        };
        let mut versions_str = Vec::with_capacity(versions.len());
        let mut versions_bin = Vec::with_capacity(versions.len());
        for file_info_versions in versions.iter() {
            versions_str.push(match serde_json::to_string(file_info_versions) {
                Ok(versions_str) => versions_str,
                Err(err) => {
                    let mut errors = Vec::with_capacity(versions.len());
                    for _ in 0..versions.len() {
                        errors.push(Some(Error::other(err.to_string())));
                    }
                    return errors;
                }
            });
            versions_bin.push(match encode_file_info_versions_msgpack(file_info_versions) {
                Ok(versions_bin) => Bytes::from(versions_bin),
                Err(err) => {
                    let mut errors = Vec::with_capacity(versions.len());
                    for _ in 0..versions.len() {
                        errors.push(Some(Error::other(err.to_string())));
                    }
                    return errors;
                }
            });
        }
        let mut client = match self.get_client().await {
            Ok(client) => client,
            Err(err) => {
                let mut errors = Vec::with_capacity(versions.len());
                for _ in 0..versions.len() {
                    errors.push(Some(Error::other(err.to_string())));
                }
                return errors;
            }
        };

        let mut request = Request::new(DeleteVersionsRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            versions: versions_str,
            opts,
            versions_bin,
            opts_bin: opts_bin.into(),
        });
        let canonical_body = rustfs_protos::canonical_delete_versions_request_body(request.get_ref());
        if let Err(err) = attach_mutation_body_digest(&mut request, canonical_body, "delete_versions") {
            let mut errors = Vec::with_capacity(versions.len());
            for _ in 0..versions.len() {
                errors.push(Some(err.clone()));
            }
            return errors;
        }

        let result = self
            .execute_with_timeout(
                || async {
                    client
                        .delete_versions(request)
                        .await
                        .map_err(|err| Error::other(format!("delete_versions failed: {err}")))
                },
                get_max_timeout_duration(),
            )
            .await;

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                let mut errors = Vec::with_capacity(versions.len());
                for _ in 0..versions.len() {
                    errors.push(Some(err.clone()));
                }
                return errors;
            }
        };

        let response = response.into_inner();
        if !response.success {
            let mut errors = Vec::with_capacity(versions.len());
            for _ in 0..versions.len() {
                errors.push(Some(Error::other(response.error.clone().map(|e| e.error_info).unwrap_or_default())));
            }
            return errors;
        }
        decode_delete_versions_errors(response, versions.len())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path_count = paths.len(),
            op = "delete_paths",
            state = "started",
            "Remote disk RPC started"
        );
        let paths = paths.to_owned();

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(DeletePathsRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    paths: paths.clone(),
                });
                let canonical_body = rustfs_protos::canonical_delete_paths_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "delete_paths")?;

                let response = client.delete_paths(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn acquire_snapshot_lease(&self, volume: &str, path: &str) -> Result<SnapshotLeaseToken> {
        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(SnapshotLeaseRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    ttl_ms: u64::try_from(REMOTE_SNAPSHOT_LEASE_TTL.as_millis())
                        .map_err(|_| Error::other("snapshot lease TTL cannot be represented"))?,
                });
                let canonical_body = rustfs_protos::canonical_snapshot_lease_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "acquire_snapshot_lease")?;
                let response = client.acquire_snapshot_lease(request).await?.into_inner();
                snapshot_lease_token_from_response(response)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn renew_snapshot_lease(&self, volume: &str, path: &str, token: SnapshotLeaseToken) -> Result<SnapshotLeaseToken> {
        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(SnapshotLeaseRenewRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    token: token.as_bytes().to_vec().into(),
                    ttl_ms: u64::try_from(REMOTE_SNAPSHOT_LEASE_TTL.as_millis())
                        .map_err(|_| Error::other("snapshot lease TTL cannot be represented"))?,
                });
                let canonical_body = rustfs_protos::canonical_snapshot_lease_renew_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "renew_snapshot_lease")?;
                let response = client.renew_snapshot_lease(request).await?.into_inner();
                snapshot_lease_token_from_response(response)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn release_snapshot_lease(&self, volume: &str, path: &str, token: SnapshotLeaseToken) -> Result<()> {
        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(SnapshotLeaseReleaseRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    token: token.as_bytes().to_vec().into(),
                });
                let canonical_body = rustfs_protos::canonical_snapshot_lease_release_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "release_snapshot_lease")?;
                let response = client.release_snapshot_lease(request).await?.into_inner();
                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }
                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "write_metadata",
            state = "started",
            "Remote disk RPC started"
        );
        let file_info = compat_json(&fi)?;
        let file_info_bin = encode_file_info_msgpack(&fi)?;

        self.execute_with_timeout_for_op(
            "write_metadata",
            move || async move {
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(WriteMetadataRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                    file_info_bin: file_info_bin.into(),
                });
                let canonical_body = rustfs_protos::canonical_write_metadata_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "write_metadata")?;

                let response = client.write_metadata(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    async fn read_metadata(&self, volume: &str, path: &str) -> Result<Bytes> {
        // Idempotent metadata read: eligible for the bounded transient-network retry so a single
        // reset-by-peer during the read-after-write window does not erode the metadata read
        // quorum (see issue #2761).
        self.execute_read_with_retry(
            "read_metadata",
            || async {
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ReadMetadataRequest {
                    volume: volume.to_string(),
                    path: path.to_string(),
                    disk,
                });

                let response = client.read_metadata(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(response.data)
            },
            get_drive_metadata_timeout(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "update_metadata",
            state = "started",
            "Remote disk RPC started"
        );
        let file_info = compat_json(&fi)?;
        let opts_str = compat_json(&opts)?;
        let file_info_bin = encode_file_info_msgpack(&fi)?;
        let opts_bin = encode_msgpack(opts)?;

        self.execute_with_timeout_for_op(
            "update_metadata",
            move || async move {
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(UpdateMetadataRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                    opts: opts_str,
                    file_info_bin: file_info_bin.into(),
                    opts_bin: opts_bin.into(),
                });
                let canonical_body = rustfs_protos::canonical_update_metadata_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "update_metadata")?;

                let response = client.update_metadata(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_version(
        &self,
        _org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            version_id,
            op = "read_version",
            state = "started",
            "Remote disk RPC started"
        );
        let read_version_attribution_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        let encode_started = read_version_stage_timer(read_version_attribution_enabled);
        let encoded_opts = compat_json(opts).and_then(|opts_str| encode_msgpack(opts).map(|opts_bin| (opts_str, opts_bin)));
        record_read_version_stage(INTERNODE_STAGE_READ_VERSION_REQUEST_ENCODE, encode_started);
        let (opts_str, opts_bin) = encoded_opts?;

        // Idempotent version read: eligible for the bounded transient-network retry so a single
        // reset-by-peer during the read-after-write window does not erode the metadata read
        // quorum (see issue #2761). The request payload is rebuilt (cloned) per attempt so the
        // operation closure stays re-invocable (`Fn`).
        self.execute_read_with_retry(
            "read_version",
            || async {
                let opts_str = opts_str.clone();
                let opts_bin = opts_bin.clone();
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request_payload_bytes = read_version_attribution_enabled.then(|| {
                    disk.len()
                        .saturating_add(volume.len())
                        .saturating_add(path.len())
                        .saturating_add(version_id.len())
                        .saturating_add(opts_str.len())
                        .saturating_add(opts_bin.len())
                });
                let request = Request::new(ReadVersionRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    version_id: version_id.to_string(),
                    opts: opts_str,
                    opts_bin: opts_bin.into(),
                });

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_version_request();
                if let Some(request_payload_bytes) = request_payload_bytes {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_version_sent_bytes(request_payload_bytes);
                }
                let rpc_started = read_version_stage_timer(read_version_attribution_enabled);
                let response = match client.read_version(request).await {
                    Ok(response) => {
                        record_read_version_stage(INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP, rpc_started);
                        response.into_inner()
                    }
                    Err(err) => {
                        record_read_version_stage(INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP, rpc_started);
                        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_version_error();
                        return Err(err.into());
                    }
                };

                if !response.success {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_version_error();
                    return Err(response.error.unwrap_or_default().into());
                }

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_version_recv_bytes(
                    response.file_info.len().saturating_add(response.file_info_bin.len()),
                );
                let decode_started = read_version_stage_timer(read_version_attribution_enabled);
                let file_info = match decode_msgpack_or_json::<FileInfo>(&response.file_info_bin, &response.file_info, "FileInfo")
                    .and_then(|file_info| {
                        validate_decoded_file_info(&file_info)?;
                        Ok(file_info)
                    }) {
                    Ok(file_info) => {
                        record_read_version_stage(INTERNODE_STAGE_READ_VERSION_RESPONSE_DECODE, decode_started);
                        file_info
                    }
                    Err(err) => {
                        record_read_version_stage(INTERNODE_STAGE_READ_VERSION_RESPONSE_DECODE, decode_started);
                        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_version_error();
                        return Err(err);
                    }
                };

                Ok(file_info)
            },
            get_drive_metadata_timeout(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn batch_read_version(&self, req: BatchReadVersionReq) -> Result<Vec<BatchReadVersionResp>> {
        validate_batch_read_version_item_count(req.items.len())?;

        let mode = batch_metadata_rpc_mode();
        if !mode.should_attempt() {
            record_batch_read_version_gate_decision(mode, BATCH_READ_VERSION_GATE_OFF_UNARY);
            return batch_read_version_one_by_one(self, req).await;
        }
        record_batch_read_version_gate_decision(mode, BATCH_READ_VERSION_GATE_ATTEMPT);

        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            item_count = req.items.len(),
            batch_metadata_rpc_mode = mode.as_str(),
            op = "batch_read_version",
            state = "started",
            "Remote disk RPC started"
        );
        let batch_read_version_attribution_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        let encode_started = read_version_stage_timer(batch_read_version_attribution_enabled);
        let batch_read_version_req = compat_json(&req)?;
        let batch_read_version_req_bin = encode_msgpack(&req)?;
        record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_REQUEST_ENCODE, encode_started);
        let request_payload_bytes = batch_read_version_attribution_enabled
            .then(|| batch_read_version_request_payload_len(&req, &batch_read_version_req, &batch_read_version_req_bin));
        let batch_result = self
            .execute_with_timeout_for_op(
                "batch_read_version",
                move || async move {
                    let disk = self.disk_ref().await;
                    let disk_len = disk.len();
                    let mut client = self
                        .get_bulk_client()
                        .await
                        .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                    let request = Request::new(BatchReadVersionRequest {
                        disk,
                        batch_read_version_req,
                        batch_read_version_req_bin: batch_read_version_req_bin.into(),
                    });

                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_batch_read_version_request();
                    if let Some(request_payload_bytes) = request_payload_bytes {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_batch_read_version_sent_bytes(
                            request_payload_bytes.saturating_add(disk_len),
                        );
                    }
                    let rpc_started = read_version_stage_timer(batch_read_version_attribution_enabled);
                    let response = match client.batch_read_version(request).await {
                        Ok(response) => {
                            record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_RPC_ROUNDTRIP, rpc_started);
                            response.into_inner()
                        }
                        Err(status) if status.code() == Code::Unimplemented => {
                            record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_RPC_ROUNDTRIP, rpc_started);
                            if mode.should_fallback_on_unimplemented() {
                                record_batch_read_version_gate_decision(mode, BATCH_READ_VERSION_GATE_FALLBACK_UNIMPLEMENTED);
                                warn!(
                                    event = EVENT_REMOTE_DISK_RPC,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                                    endpoint = %self.endpoint,
                                    batch_metadata_rpc_mode = mode.as_str(),
                                    op = "batch_read_version",
                                    state = "fallback_unimplemented",
                                    "Remote disk BatchReadVersion unsupported; falling back to unary read_version"
                                );
                                return Ok(None);
                            }

                            record_batch_read_version_gate_decision(mode, BATCH_READ_VERSION_GATE_UNSUPPORTED_NO_FALLBACK);
                            crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_batch_read_version_error();
                            warn!(
                                event = EVENT_REMOTE_DISK_RPC,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
                                endpoint = %self.endpoint,
                                batch_metadata_rpc_mode = mode.as_str(),
                                op = "batch_read_version",
                                state = "unsupported_no_fallback",
                                "Remote disk BatchReadVersion unsupported and explicit batch RPC mode forbids fallback"
                            );
                            return Err(Error::from(status));
                        }
                        Err(status) => {
                            record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_RPC_ROUNDTRIP, rpc_started);
                            crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_batch_read_version_error();
                            return Err(Error::from(status));
                        }
                    };

                    if !response.success {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_batch_read_version_error();
                        return Err(response.error.unwrap_or_default().into());
                    }

                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_batch_read_version_recv_bytes(
                        batch_read_version_response_payload_len(&response),
                    );
                    let decode_started = read_version_stage_timer(batch_read_version_attribution_enabled);
                    match decode_batch_read_version_response_items(response, &self.endpoint) {
                        Ok(batch_read_version_resps) => {
                            record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_DECODE, decode_started);
                            Ok(Some(batch_read_version_resps))
                        }
                        Err(err) => {
                            record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_DECODE, decode_started);
                            crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_batch_read_version_error();
                            Err(err)
                        }
                    }
                },
                get_max_timeout_duration(),
            )
            .await?;

        match batch_result {
            Some(batch_read_version_resps) => Ok(batch_read_version_resps),
            // Run the unary fallback outside the batch RPC deadline so each
            // read_version keeps its own per-op timeout and health accounting
            // instead of racing the whole batch against one drive timeout.
            None => batch_read_version_one_by_one(self, req).await,
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            read_data,
            op = "read_xl",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ReadXlRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    read_data,
                });

                let response = client.read_xl(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let raw_file_info =
                    decode_msgpack_or_json::<RawFileInfo>(&response.raw_file_info_bin, &response.raw_file_info, "RawFileInfo")?;

                Ok(raw_file_info)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        self.rename_data_borrowed(src_volume, src_path, &fi, dst_volume, dst_path)
            .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn list_dir(&self, _origvolume: &str, volume: &str, dir_path: &str, count: i32) -> Result<Vec<String>> {
        trace!(volume, dir_path, "Remote disk list_dir RPC started");

        self.execute_with_timeout(
            || async {
                let disk = self.disk_ref().await;

                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ListDirRequest {
                    disk,
                    volume: volume.to_string(),
                    dir_path: dir_path.to_string(),
                    count,
                });

                let response = client.list_dir(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(response.volumes)
            },
            get_drive_list_dir_timeout(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            bucket = %opts.bucket,
            base_dir = %opts.base_dir,
            op = "walk_dir",
            state = "started",
            "Remote disk RPC started"
        );

        let disk = self.disk_ref().await;
        let body = serde_json::to_vec(&opts)?;
        let stall_timeout = opts.stall_timeout_duration().unwrap_or_else(get_drive_walkdir_stall_timeout);
        let bucket = opts.bucket.clone();
        let base_dir = opts.base_dir.clone();
        let disk_for_log = disk.clone();
        let timeout_duration = if opts.skip_total_timeout {
            Duration::ZERO
        } else {
            opts.timeout_duration().unwrap_or_else(get_drive_walkdir_timeout)
        };

        self.execute_with_timeout_for_op_and_health_action(
            "walk_dir",
            || async {
                let mut last_err = None;

                for attempt in 1..=2 {
                    let mut reader = match self
                        .data_transport
                        .open_walk_dir(WalkDirStreamRequest {
                            endpoint: self.endpoint.grid_host(),
                            disk: disk.clone(),
                            body: body.clone(),
                            stall_timeout: Some(stall_timeout),
                        })
                        .await
                    {
                        Ok(reader) => reader,
                        Err(err) => {
                            if attempt == 1 && Self::is_retryable_walk_dir_error(&err) {
                                warn!(
                                    endpoint = %self.endpoint,
                                    addr = %self.addr,
                                    disk = %disk_for_log,
                                    bucket = %bucket,
                                    base_dir = %base_dir,
                                    attempt,
                                    stall_timeout_ms = stall_timeout.as_millis(),
                                    error = %err,
                                    "remote walk_dir returned retryable transport error; retrying"
                                );
                                last_err = Some(err);
                                continue;
                            }

                            return Err(err);
                        }
                    };

                    match copy_stream_with_buffer(&mut reader, wr, DEFAULT_READ_BUFFER_SIZE).await {
                        Ok(_) => return Ok(()),
                        Err(io_err) => return Err(DiskError::Io(io_err)),
                    }
                }

                Err(last_err.unwrap_or_else(|| DiskError::other("walk_dir retry exhausted without captured error")))
            },
            timeout_duration,
            FailureHealthAction::IgnoreFailure,
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        self.read_file_stream(volume, path, 0, 0).await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        // warn!(
        //     "disk remote read_file_stream {}/{}/{} offset={} length={}",
        //     self.endpoint.to_string(),
        //     volume,
        //     path,
        //     offset,
        //     length
        // );

        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }
        let disk = self.disk_ref().await;
        let timeouts = remote_read_timeouts(get_object_disk_read_timeout());
        let request = ReadStreamRequest {
            endpoint: self.endpoint.grid_host(),
            disk,
            volume: volume.to_string(),
            path: path.to_string(),
            offset,
            length,
            stall_timeout: timeouts.body_stall,
        };
        let reader = self.open_read_with_retry(request.clone()).await?;
        Ok(Box::new(RetryingRemoteReader::new_with_timeouts(
            reader,
            Arc::clone(&self.data_transport),
            request,
            timeouts.initial_read,
            timeouts.recovery,
        )))
    }

    async fn read_file_stream_chunks(
        &self,
        volume: &str,
        path: &str,
        offset: usize,
        length: usize,
    ) -> Result<Option<rustfs_rio::ChunkReaderBox>> {
        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }
        let disk = self.disk_ref().await;
        let timeouts = remote_read_timeouts(get_object_disk_read_timeout());
        let request = ReadStreamRequest {
            endpoint: self.endpoint.grid_host(),
            disk,
            volume: volume.to_string(),
            path: path.to_string(),
            offset,
            length,
            stall_timeout: timeouts.body_stall,
        };
        let reader = self.open_read_chunks_with_retry(request.clone()).await?;
        Ok(reader.map(|reader| {
            Box::new(RetryingRemoteChunkReader::new_with_timeouts(
                reader,
                Arc::clone(&self.data_transport),
                request,
                timeouts.initial_read,
                timeouts.recovery,
            )) as rustfs_rio::ChunkReaderBox
        }))
    }

    /// Buffered read for remote disks.
    /// The transport stream is collected into owned Bytes for caller sharing.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_file_mmap_copy(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<Bytes> {
        // For remote disks, use the regular reader and read into Bytes
        let reader = self.read_file_stream(volume, path, offset, length).await?;

        use tokio::io::AsyncReadExt;
        let mut reader = reader;

        // Read all data into Bytes (single allocation)
        let mut buffer = Vec::with_capacity(length);
        reader.read_to_end(&mut buffer).await?;

        Ok(Bytes::from(buffer))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "append_file",
            state = "started",
            "Remote disk RPC started"
        );

        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }
        let disk = self.disk_ref().await;
        self.open_write_with_retry(WriteStreamRequest {
            endpoint: self.endpoint.grid_host(),
            disk,
            volume: volume.to_string(),
            path: path.to_string(),
            append: true,
            size: 0,
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, file_size: i64) -> Result<FileWriter> {
        // warn!(
        //     "disk remote create_file {}/{}/{} file_size={}",
        //     self.endpoint.to_string(),
        //     volume,
        //     path,
        //     file_size
        // );

        if self.health.is_faulty() {
            return Err(DiskError::FaultyDisk);
        }
        let disk = self.disk_ref().await;
        self.open_write_with_retry(WriteStreamRequest {
            endpoint: self.endpoint.grid_host(),
            disk,
            volume: volume.to_string(),
            path: path.to_string(),
            append: false,
            size: file_size,
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            src_volume,
            src_path,
            dst_volume,
            dst_path,
            op = "rename_file",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(RenameFileRequest {
                    disk: self.endpoint.to_string(),
                    src_volume: src_volume.to_string(),
                    src_path: src_path.to_string(),
                    dst_volume: dst_volume.to_string(),
                    dst_path: dst_path.to_string(),
                });
                let canonical_body = rustfs_protos::canonical_rename_file_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "rename_file")?;

                let response = client.rename_file(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            src_volume,
            src_path,
            dst_volume,
            dst_path,
            op = "rename_part",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(RenamePartRequest {
                    disk: self.endpoint.to_string(),
                    src_volume: src_volume.to_string(),
                    src_path: src_path.to_string(),
                    dst_volume: dst_volume.to_string(),
                    dst_path: dst_path.to_string(),
                    meta,
                });
                let canonical_body = rustfs_protos::canonical_rename_part_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "rename_part")?;

                let response = client.rename_part(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn prepare_part_transaction(
        &self,
        src_volume: &str,
        src_path: &str,
        dst_volume: &str,
        dst_path: &str,
        meta: Bytes,
    ) -> Result<()> {
        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(PreparePartTransactionRequest {
                    disk: self.endpoint.to_string(),
                    src_volume: src_volume.to_string(),
                    src_path: src_path.to_string(),
                    dst_volume: dst_volume.to_string(),
                    dst_path: dst_path.to_string(),
                    meta,
                });
                let canonical_body = rustfs_protos::canonical_prepare_part_transaction_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "prepare_part_transaction")?;

                let response = client.prepare_part_transaction(request).await?.into_inner();
                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }
                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn settle_part_transaction(&self, volume: &str, path: &str, action: PartTransactionAction) -> Result<()> {
        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(SettlePartTransactionRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    rollback: action == PartTransactionAction::Rollback,
                });
                let canonical_body = rustfs_protos::canonical_settle_part_transaction_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "settle_part_transaction")?;

                let response = client.settle_part_transaction(request).await?.into_inner();
                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }
                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            recursive = opt.recursive,
            immediate = opt.immediate,
            op = "delete",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let options = serde_json::to_string(&opt)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let mut request = Request::new(DeleteRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    options,
                });
                let canonical_body = rustfs_protos::canonical_delete_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "delete")?;

                let response = client.delete(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "verify_file",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let file_info = serde_json::to_string(&fi)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(VerifyFileRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                });

                let response = client.verify_file(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let check_parts_resp = serde_json::from_str::<CheckPartsResp>(&response.check_parts_resp)?;

                Ok(check_parts_resp)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_parts(&self, bucket: &str, paths: &[String]) -> Result<Vec<ObjectPartInfo>> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            bucket,
            path_count = paths.len(),
            op = "read_parts",
            state = "started",
            "Remote disk RPC started"
        );
        self.execute_with_timeout(
            || async {
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ReadPartsRequest {
                    disk: self.endpoint.to_string(),
                    bucket: bucket.to_string(),
                    paths: paths.to_vec(),
                });

                let response = client.read_parts(request).await?.into_inner();
                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let read_parts_resp = rmp_serde::from_slice::<Vec<ObjectPartInfo>>(&response.object_part_infos)?;

                Ok(read_parts_resp)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "check_parts",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let file_info = serde_json::to_string(&fi)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(CheckPartsRequest {
                    disk: self.endpoint.to_string(),
                    volume: volume.to_string(),
                    path: path.to_string(),
                    file_info,
                });

                let response = client.check_parts(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let check_parts_resp = serde_json::from_str::<CheckPartsResp>(&response.check_parts_resp)?;

                Ok(check_parts_resp)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            bucket = %req.bucket,
            prefix = %req.prefix,
            op = "read_multiple",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let read_multiple_req = compat_json(&req)?;
                let read_multiple_req_bin = encode_msgpack(&req)?;
                let disk = self.disk_ref().await;
                let mut client = self
                    .get_bulk_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(ReadMultipleRequest {
                    disk,
                    read_multiple_req,
                    read_multiple_req_bin: read_multiple_req_bin.into(),
                });

                let response = client.read_multiple(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_multiple_recv_bytes(
                    read_multiple_response_payload_len(&response),
                );

                let read_multiple_resps = decode_read_multiple_response_items(response, &self.endpoint)?;

                Ok(read_multiple_resps)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            bytes = data.len(),
            op = "write_all",
            state = "started",
            "Remote disk RPC started"
        );

        self.execute_with_timeout(
            || async {
                let data_len = data.len();
                let disk = self.disk_ref().await;
                let mut client = self.get_bulk_client().await.map_err(|err| {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_error();
                    Error::other(format!("can not get client, err: {err}"))
                })?;
                let mut request = Request::new(WriteAllRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                    data,
                });
                let canonical_body = rustfs_protos::canonical_write_all_request_body(request.get_ref());
                attach_mutation_body_digest(&mut request, canonical_body, "write_all")?;

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_request();
                let response = match client.write_all(request).await {
                    Ok(response) => response.into_inner(),
                    Err(err) => {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_error();
                        return Err(err.into());
                    }
                };

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_sent_bytes(data_len);

                if !response.success {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_write_all_error();
                    return Err(response.error.unwrap_or_default().into());
                }

                Ok(())
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        trace!(
            event = EVENT_REMOTE_DISK_RPC,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_DISK,
            endpoint = %self.endpoint,
            volume,
            path,
            op = "read_all",
            state = "started",
            "Remote disk RPC started"
        );

        // Idempotent full read: eligible for the bounded transient-network retry so a single
        // reset-by-peer during the read-after-write window does not erode the read quorum
        // (see issue #2761).
        self.execute_read_with_retry(
            "read_all",
            || async {
                let disk = self.disk_ref().await;
                let mut client = self.get_bulk_client().await.map_err(|err| {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_error();
                    Error::other(format!("can not get client, err: {err}"))
                })?;
                let request = Request::new(ReadAllRequest {
                    disk,
                    volume: volume.to_string(),
                    path: path.to_string(),
                });

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_request();
                let response = match client.read_all(request).await {
                    Ok(response) => response.into_inner(),
                    Err(err) => {
                        crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_error();
                        return Err(err.into());
                    }
                };

                if !response.success {
                    crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_error();
                    return Err(response.error.unwrap_or_default().into());
                }

                crate::cluster::rpc::runtime_sources::record_remote_disk_grpc_read_all_recv_bytes(response.data.len());
                Ok(response.data)
            },
            get_max_timeout_duration(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo> {
        // disk_info is idempotent/read-only, so it is eligible for the P3-3 bounded retry.
        self.execute_read_with_retry(
            "disk_info",
            || async {
                let opts = serde_json::to_string(&opts)?;
                let mut client = self
                    .get_client()
                    .await
                    .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
                let request = Request::new(DiskInfoRequest {
                    disk: self.endpoint.to_string(),
                    opts,
                });

                let response = client.disk_info(request).await?.into_inner();

                if !response.success {
                    return Err(response.error.unwrap_or_default().into());
                }

                let disk_info = serde_json::from_str::<DiskInfo>(&response.disk_info)?;

                Ok(disk_info)
            },
            get_drive_disk_info_timeout(),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn start_scan(&self) -> ScanGuard {
        self.scanning.fetch_add(1, Ordering::Relaxed);
        ScanGuard(Arc::clone(&self.scanning))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::rpc::internode_data_transport::{InternodeDataTransportCapabilities, TcpHttpInternodeDataTransport};
    use crate::erasure::coding::{BitrotReader, Erasure, decode::ParallelReader};
    use crate::io_support::bitrot::ShardReader;
    use crate::runtime::sources as runtime_sources;
    use rustfs_protos::proto_gen::node_service::{DiskInfoResponse, ReadAllResponse};
    use serde_json::Value;
    use serial_test::serial;
    use std::convert::Infallible;
    use std::future::Future;
    use std::io::{self as std_io, Write};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex, Mutex as StdMutex, Once, atomic::AtomicUsize};
    use std::task::{Context, Poll};
    use tokio::io::{ReadBuf, duplex};
    use tokio::net::TcpListener;
    use tonic::transport::{Endpoint as TonicEndpoint, Server};
    use tonic::{Response, Status};
    use tonic::{
        codegen::{Body as HttpBody, BoxFuture, StdError, http},
        server::NamedService,
    };
    use tracing::Level;
    use tracing_subscriber::{Registry, fmt::MakeWriter, layer::SubscriberExt};
    use uuid::Uuid;

    static INIT: Once = Once::new();

    #[test]
    fn delete_versions_response_preserves_typed_item_errors() {
        let errors = decode_delete_versions_errors(
            DeleteVersionsResponse {
                success: true,
                errors: vec!["file not found".to_string(), String::new()],
                error: None,
                item_errors: vec![
                    rustfs_protos::proto_gen::node_service::Error {
                        code: DiskError::FileNotFound.to_u32(),
                        error_info: "file not found".to_string(),
                    },
                    rustfs_protos::proto_gen::node_service::Error::default(),
                ],
            },
            2,
        );

        assert!(matches!(errors.as_slice(), [Some(DiskError::FileNotFound), None]));
    }

    #[test]
    fn delete_versions_response_accepts_legacy_string_errors() {
        let errors = decode_delete_versions_errors(
            DeleteVersionsResponse {
                success: true,
                errors: vec!["legacy error".to_string(), String::new()],
                error: None,
                item_errors: Vec::new(),
            },
            2,
        );

        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].as_ref().map(ToString::to_string).as_deref(), Some("io error legacy error"));
        assert!(errors[1].is_none());
    }

    #[test]
    fn delete_versions_response_rejects_misaligned_item_errors() {
        let errors = decode_delete_versions_errors(
            DeleteVersionsResponse {
                success: true,
                errors: vec!["file not found".to_string()],
                error: None,
                item_errors: vec![rustfs_protos::proto_gen::node_service::Error {
                    code: DiskError::FileNotFound.to_u32(),
                    error_info: "file not found".to_string(),
                }],
            },
            2,
        );

        assert_eq!(errors.len(), 2);
        assert!(errors.iter().all(Option::is_some));
    }

    #[test]
    fn disk_mutation_digest_marks_rolling_compatibility() {
        let mut request = Request::new(());

        attach_mutation_body_digest(&mut request, Ok(b"canonical disk mutation".to_vec()), "WriteAll")
            .expect("disk mutation digest must be attached");

        assert!(
            request
                .extensions()
                .get::<crate::cluster::rpc::http_auth::RollingMutationBodyDigest>()
                .is_some(),
            "remote-disk mutations must reach the cache-free compatibility gate"
        );
    }

    // `#[serial(internode_metrics)]` marks every test that observes
    // `global_internode_metrics()`. Those counters are a process-wide singleton:
    // some of these tests snapshot a counter, run one decode, and assert on the
    // delta, while others deliberately record decode errors or call
    // `reset_internode_metrics_for_test()`. Run concurrently in one process they
    // corrupt each other's deltas — a sibling's error bumps the "no decode error"
    // assertion off zero, and a sibling's reset can drive an `after > before`
    // assertion backwards.
    //
    // The marker only takes effect under the `cargo test` fallback; nextest
    // already isolates each test in its own process, so every test there gets its
    // own copy of the counters (see `docs/testing/README.md`). Any new test that
    // reads or mutates the global internode metrics belongs in this group.

    #[test]
    fn snapshot_lease_response_requires_current_protocol_and_valid_token() {
        let token = SnapshotLeaseToken::new();
        let response = SnapshotLeaseResponse {
            success: true,
            token: token.as_bytes().to_vec().into(),
            protocol_version: SNAPSHOT_LEASE_PROTOCOL_VERSION,
            error: None,
        };
        assert_eq!(snapshot_lease_token_from_response(response).unwrap(), token);

        let incompatible = SnapshotLeaseResponse {
            success: true,
            token: token.as_bytes().to_vec().into(),
            protocol_version: SNAPSHOT_LEASE_PROTOCOL_VERSION + 1,
            error: None,
        };
        assert!(snapshot_lease_token_from_response(incompatible).is_err());

        let malformed = SnapshotLeaseResponse {
            success: true,
            token: Bytes::from_static(b"not-a-uuid"),
            protocol_version: SNAPSHOT_LEASE_PROTOCOL_VERSION,
            error: None,
        };
        assert!(snapshot_lease_token_from_response(malformed).is_err());
    }

    #[test]
    fn list_volumes_decode_rejects_a_malformed_entry() {
        let valid = serde_json::to_string(&VolumeInfo {
            name: "bucket".to_string(),
            created: None,
        })
        .expect("volume info should serialize");
        let err = decode_volume_infos(vec![valid, "{".to_string()])
            .expect_err("a malformed volume entry must fail the complete response");

        assert!(err.to_string().contains("entry 1"));
    }

    #[test]
    fn decoded_remote_metadata_rejects_default_like_delete_marker() {
        let forged = FileInfo {
            deleted: true,
            ..Default::default()
        };
        assert!(matches!(validate_decoded_file_info(&forged), Err(DiskError::FileCorrupt)));

        let marker = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            deleted: true,
            mod_time: Some(::time::OffsetDateTime::now_utc()),
            ..Default::default()
        };
        validate_decoded_file_info(&marker).expect("canonical remote delete marker should validate");
    }

    #[derive(Clone, Default)]
    struct CapturedLogs {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn lines(&self) -> Vec<Value> {
            let buffer = self
                .buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .clone();
            String::from_utf8(buffer)
                .expect("captured logs should be valid UTF-8")
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).expect("captured log line should be valid JSON"))
                .collect()
        }
    }

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std_io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std_io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    #[derive(Debug, Clone)]
    enum RecordedTransportCall {
        Read(ReadStreamRequest),
        Write(WriteStreamRequest),
        WalkDir(WalkDirStreamRequest),
        NsScanner(NsScannerStreamRequest),
        NsScannerProbe(NsScannerCapabilityRequest),
    }

    #[derive(Debug, Clone, Default)]
    struct RecordingInternodeDataTransport {
        calls: Arc<StdMutex<Vec<RecordedTransportCall>>>,
        ns_scanner_probe_status: Arc<StdMutex<Option<u16>>>,
    }

    #[derive(Clone, Debug)]
    struct AuthenticatedReadPeer {
        audience: String,
        disk_info_calls: Arc<AtomicU32>,
        read_all_calls: Arc<AtomicU32>,
        object_read_all_disks: Arc<StdMutex<Vec<String>>>,
        format_data: Bytes,
        read_all_data: Bytes,
    }

    impl AuthenticatedReadPeer {
        fn new(audience: String, format_data: Bytes, read_all_data: Bytes) -> Self {
            Self {
                audience,
                disk_info_calls: Arc::new(AtomicU32::new(0)),
                read_all_calls: Arc::new(AtomicU32::new(0)),
                object_read_all_disks: Arc::default(),
                format_data,
                read_all_data,
            }
        }

        fn disk_info_calls(&self) -> u32 {
            self.disk_info_calls.load(Ordering::Acquire)
        }

        fn read_all_calls(&self) -> u32 {
            self.read_all_calls.load(Ordering::Acquire)
        }

        fn object_read_all_disks(&self) -> Vec<String> {
            self.object_read_all_disks
                .lock()
                .expect("object read_all disk list lock poisoned")
                .clone()
        }

        fn verify_auth<T>(&self, request: &Request<T>, path: &str) -> std::result::Result<(), Status> {
            let headers = request.metadata().clone().into_headers();
            crate::cluster::rpc::verify_tonic_rpc_signature(&self.audience, path, &headers)
                .map_err(|err| Status::unauthenticated(err.to_string()))
        }
    }

    #[derive(Clone, Debug)]
    struct AuthenticatedReadPeerService {
        peer: AuthenticatedReadPeer,
    }

    impl NamedService for AuthenticatedReadPeerService {
        const NAME: &'static str = "node_service.NodeService";
    }

    impl<B> tower::Service<http::Request<B>> for AuthenticatedReadPeerService
    where
        B: HttpBody + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<B>) -> Self::Future {
            match request.uri().path() {
                "/node_service.NodeService/DiskInfo" => {
                    #[derive(Clone)]
                    struct DiskInfoSvc(AuthenticatedReadPeer);

                    impl tonic::server::UnaryService<DiskInfoRequest> for DiskInfoSvc {
                        type Response = DiskInfoResponse;
                        type Future = Pin<Box<dyn Future<Output = std::result::Result<Response<Self::Response>, Status>> + Send>>;

                        fn call(&mut self, request: Request<DiskInfoRequest>) -> Self::Future {
                            let peer = self.0.clone();
                            Box::pin(async move {
                                peer.verify_auth(&request, "/node_service.NodeService/DiskInfo")?;
                                let request = request.into_inner();
                                let opts = serde_json::from_str::<DiskInfoOptions>(&request.opts)
                                    .map_err(|err| Status::invalid_argument(err.to_string()))?;
                                if !opts.noop {
                                    return Err(Status::invalid_argument("recovery probe must use noop disk_info"));
                                }
                                peer.disk_info_calls.fetch_add(1, Ordering::AcqRel);
                                let disk_info = serde_json::to_string(&DiskInfo {
                                    total: 1,
                                    free: 1,
                                    endpoint: request.disk,
                                    ..Default::default()
                                })
                                .map_err(|err| Status::internal(err.to_string()))?;
                                Ok(Response::new(DiskInfoResponse {
                                    success: true,
                                    disk_info,
                                    error: None,
                                }))
                            })
                        }
                    }

                    let peer = self.peer.clone();
                    Box::pin(async move {
                        let method = DiskInfoSvc(peer);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec);
                        Ok(grpc.unary(method, request).await)
                    })
                }
                "/node_service.NodeService/ReadAll" => {
                    #[derive(Clone)]
                    struct ReadAllSvc(AuthenticatedReadPeer);

                    impl tonic::server::UnaryService<ReadAllRequest> for ReadAllSvc {
                        type Response = ReadAllResponse;
                        type Future = Pin<Box<dyn Future<Output = std::result::Result<Response<Self::Response>, Status>> + Send>>;

                        fn call(&mut self, request: Request<ReadAllRequest>) -> Self::Future {
                            let peer = self.0.clone();
                            Box::pin(async move {
                                peer.verify_auth(&request, "/node_service.NodeService/ReadAll")?;
                                let request = request.into_inner();
                                let is_format_read = request.volume == crate::disk::RUSTFS_META_BUCKET
                                    && request.path == crate::disk::FORMAT_CONFIG_FILE;
                                let disk = request.disk;
                                peer.read_all_calls.fetch_add(1, Ordering::AcqRel);
                                let data = if is_format_read {
                                    peer.format_data.clone()
                                } else {
                                    peer.object_read_all_disks
                                        .lock()
                                        .expect("object read_all disk list lock poisoned")
                                        .push(disk);
                                    peer.read_all_data.clone()
                                };
                                Ok(Response::new(ReadAllResponse {
                                    success: true,
                                    data,
                                    error: None,
                                }))
                            })
                        }
                    }

                    let peer = self.peer.clone();
                    Box::pin(async move {
                        let method = ReadAllSvc(peer);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec);
                        Ok(grpc.unary(method, request).await)
                    })
                }
                _ => Box::pin(async move {
                    let mut response = http::Response::new(tonic::body::Body::default());
                    let headers = response.headers_mut();
                    headers.insert(tonic::Status::GRPC_STATUS, (tonic::Code::Unimplemented as i32).into());
                    headers.insert(http::header::CONTENT_TYPE, tonic::metadata::GRPC_CONTENT_TYPE);
                    Ok(response)
                }),
            }
        }
    }

    struct TestGrpcPeer {
        addr: String,
        peer: AuthenticatedReadPeer,
        shutdown: CancellationToken,
        task: tokio::task::JoinHandle<()>,
    }

    impl TestGrpcPeer {
        async fn spawn(format_data: Bytes, read_all_data: Bytes) -> Option<Self> {
            let listener = match TcpListener::bind("127.0.0.1:0").await {
                Ok(listener) => listener,
                Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
                Err(err) => panic!("test gRPC listener should bind: {err}"),
            };
            let socket_addr = listener.local_addr().expect("listener local address should be available");
            let addr = format!("http://{socket_addr}");
            let audience = crate::cluster::rpc::normalize_tonic_rpc_audience(&socket_addr.to_string())
                .expect("test audience should normalize");
            let peer = AuthenticatedReadPeer::new(audience, format_data, read_all_data);
            let service = AuthenticatedReadPeerService { peer: peer.clone() };
            let shutdown = CancellationToken::new();
            let shutdown_for_task = shutdown.clone();
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                Some((listener.accept().await.map(|(stream, _)| stream), listener))
            });
            let task = tokio::spawn(async move {
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, shutdown_for_task.cancelled_owned())
                    .await
                    .expect("test gRPC peer should serve");
            });

            Some(Self {
                addr,
                peer,
                shutdown,
                task,
            })
        }

        async fn stop(self) {
            self.shutdown.cancel();
            let _ = self.task.await;
        }
    }

    impl RecordingInternodeDataTransport {
        fn with_ns_scanner_probe_status(status: u16) -> Self {
            Self {
                calls: Arc::default(),
                ns_scanner_probe_status: Arc::new(StdMutex::new(Some(status))),
            }
        }

        fn set_ns_scanner_probe_status(&self, status: Option<u16>) {
            *self
                .ns_scanner_probe_status
                .lock()
                .expect("namespace scanner probe status lock poisoned") = status;
        }

        fn calls(&self) -> Vec<RecordedTransportCall> {
            self.calls.lock().expect("recorded transport calls lock poisoned").clone()
        }

        fn record(&self, call: RecordedTransportCall) {
            self.calls.lock().expect("recorded transport calls lock poisoned").push(call);
        }
    }

    #[derive(Debug)]
    enum WalkDirTestStep {
        Error(DiskError),
        Data(Vec<u8>),
        PartialDataThenError { data: Vec<u8>, error: io::Error },
    }

    #[derive(Debug, Clone, Default)]
    struct RetryingWalkDirInternodeDataTransport {
        calls: Arc<StdMutex<Vec<RecordedTransportCall>>>,
        steps: Arc<StdMutex<Vec<WalkDirTestStep>>>,
    }

    impl RetryingWalkDirInternodeDataTransport {
        fn with_steps(steps: Vec<WalkDirTestStep>) -> Self {
            Self {
                calls: Arc::new(StdMutex::new(Vec::new())),
                steps: Arc::new(StdMutex::new(steps)),
            }
        }

        fn calls(&self) -> Vec<RecordedTransportCall> {
            self.calls.lock().expect("recorded transport calls lock poisoned").clone()
        }

        fn record(&self, call: RecordedTransportCall) {
            self.calls.lock().expect("recorded transport calls lock poisoned").push(call);
        }
    }

    #[derive(Debug, Clone)]
    enum OpenWriteTestStep {
        Error(DiskError),
        Success,
    }

    #[derive(Debug, Clone, Default)]
    struct RetryingOpenWriteInternodeDataTransport {
        calls: Arc<StdMutex<Vec<RecordedTransportCall>>>,
        steps: Arc<StdMutex<Vec<OpenWriteTestStep>>>,
    }

    impl RetryingOpenWriteInternodeDataTransport {
        fn with_steps(steps: Vec<OpenWriteTestStep>) -> Self {
            Self {
                calls: Arc::new(StdMutex::new(Vec::new())),
                steps: Arc::new(StdMutex::new(steps)),
            }
        }

        fn calls(&self) -> Vec<RecordedTransportCall> {
            self.calls.lock().expect("recorded transport calls lock poisoned").clone()
        }

        fn record(&self, call: RecordedTransportCall) {
            self.calls.lock().expect("recorded transport calls lock poisoned").push(call);
        }
    }

    #[derive(Debug, Clone, Default)]
    struct RetryingOpenReadInternodeDataTransport {
        calls: Arc<StdMutex<Vec<RecordedTransportCall>>>,
        // Reuses OpenWriteTestStep as a transport-agnostic open outcome (Error | Success).
        steps: Arc<StdMutex<Vec<OpenWriteTestStep>>>,
    }

    impl RetryingOpenReadInternodeDataTransport {
        fn with_steps(steps: Vec<OpenWriteTestStep>) -> Self {
            Self {
                calls: Arc::new(StdMutex::new(Vec::new())),
                steps: Arc::new(StdMutex::new(steps)),
            }
        }

        fn calls(&self) -> Vec<RecordedTransportCall> {
            self.calls.lock().expect("recorded transport calls lock poisoned").clone()
        }

        fn record(&self, call: RecordedTransportCall) {
            self.calls.lock().expect("recorded transport calls lock poisoned").push(call);
        }
    }

    #[derive(Debug, Default)]
    struct EmptyTestReader;

    impl AsyncRead for EmptyTestReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn sample_rename_data_file_info() -> FileInfo {
        FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            data_dir: Some(Uuid::new_v4()),
            size: 64 * 1024,
            mod_time: Some(::time::OffsetDateTime::UNIX_EPOCH + ::time::Duration::seconds(1)),
            metadata: [
                ("etag".to_string(), "etag-value".to_string()),
                ("content-type".to_string(), "application/octet-stream".to_string()),
            ]
            .into_iter()
            .collect(),
            erasure: rustfs_filemeta::ErasureInfo {
                algorithm: rustfs_filemeta::ERASURE_ALGORITHM.to_string(),
                data_blocks: 4,
                parity_blocks: 2,
                block_size: 1024 * 1024,
                index: 1,
                distribution: vec![1, 2, 3, 4, 5, 6],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn sample_read_multiple_resp(file: &str, data: &[u8]) -> ReadMultipleResp {
        ReadMultipleResp {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            file: file.to_string(),
            exists: true,
            data: data.to_vec(),
            ..Default::default()
        }
    }

    fn sample_remote_endpoint() -> Endpoint {
        Endpoint {
            url: url::Url::parse("http://server:9000/disk-a").expect("endpoint URL should parse"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        }
    }

    #[test]
    #[serial(internode_metrics)]
    fn read_multiple_response_decode_prefers_msgpack_payloads() {
        let endpoint = sample_remote_endpoint();
        let msgpack_resp = sample_read_multiple_resp("msgpack", b"binary");
        let json_resp = sample_read_multiple_resp("json", b"fallback");
        let response = ReadMultipleResponse {
            success: true,
            read_multiple_resps: vec![serde_json::to_string(&json_resp).expect("json fallback should encode")],
            read_multiple_resps_bin: vec![encode_msgpack(&msgpack_resp).expect("msgpack response should encode").into()],
            error: None,
        };
        let before = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_total_for_test();

        let decoded = decode_read_multiple_response_items(response, &endpoint).expect("msgpack response should decode");
        let after = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_total_for_test();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].file, "msgpack");
        assert_eq!(decoded[0].data, b"binary");
        assert!(after > before, "successful response msgpack decode should increment traffic metrics");
    }

    #[test]
    #[serial(internode_metrics)]
    fn read_multiple_response_decode_falls_back_to_json_payloads() {
        let endpoint = sample_remote_endpoint();
        let json_resp = sample_read_multiple_resp("json", b"fallback");
        let response = ReadMultipleResponse {
            success: true,
            read_multiple_resps: vec![serde_json::to_string(&json_resp).expect("json fallback should encode")],
            read_multiple_resps_bin: Vec::new(),
            error: None,
        };
        let before = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_total_for_test();

        let decoded = decode_read_multiple_response_items(response, &endpoint).expect("json response should decode");
        let after = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_total_for_test();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].file, "json");
        assert_eq!(decoded[0].data, b"fallback");
        assert!(after > before, "successful response JSON decode should increment traffic metrics");
    }

    #[test]
    #[serial(internode_metrics)]
    fn rename_data_response_accepts_legacy_json_without_decode_error() {
        crate::cluster::rpc::runtime_sources::reset_internode_metrics_for_test();
        let response = RenameDataResp {
            old_data_dir: Some(Uuid::new_v4()),
            rollback_data_dir: Some(Uuid::new_v4()),
            cleanup_data_dir: Some(Uuid::new_v4()),
            sign: Some(vec![0x14, 0x35]),
            old_current_size: Some(crate::disk::OldCurrentSize::Present(64 * 1024)),
        };
        let json = serde_json::to_string(&response).expect("legacy rename_data JSON response should encode");
        let decode_before = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_total_for_test();
        let decode_errors_before = crate::cluster::rpc::runtime_sources::internode_msgpack_json_decode_error_total_for_test();

        let decoded = decode_msgpack_or_json::<RenameDataResp>(&[], &json, "RenameDataResp")
            .expect("legacy rename_data JSON response should decode");
        let decode_after = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_total_for_test();
        let decode_errors_after = crate::cluster::rpc::runtime_sources::internode_msgpack_json_decode_error_total_for_test();

        assert_eq!(decoded.old_data_dir, response.old_data_dir);
        assert_eq!(decoded.rollback_data_dir, response.rollback_data_dir);
        assert_eq!(decoded.cleanup_data_dir, response.cleanup_data_dir);
        assert_eq!(decoded.sign, response.sign);
        assert_eq!(decoded.old_current_size, response.old_current_size);
        assert!(
            decode_after > decode_before,
            "legacy JSON response should increment successful decode traffic"
        );
        assert_eq!(
            decode_errors_after, decode_errors_before,
            "legacy JSON compatibility fallback must stay observable without becoming a decode error"
        );
    }

    #[test]
    fn read_multiple_response_payload_len_prefers_msgpack_and_falls_back_to_json() {
        let bin_a = encode_msgpack(&sample_read_multiple_resp("a", b"binary")).expect("msgpack should encode");
        let bin_b = encode_msgpack(&sample_read_multiple_resp("b", b"more")).expect("msgpack should encode");
        let json = serde_json::to_string(&sample_read_multiple_resp("j", b"fallback")).expect("json should encode");

        // When msgpack bins are present, the length is their aggregate size (JSON strings ignored).
        let with_bin = ReadMultipleResponse {
            success: true,
            read_multiple_resps: vec![json.clone()],
            read_multiple_resps_bin: vec![bin_a.clone().into(), bin_b.clone().into()],
            error: None,
        };
        assert_eq!(read_multiple_response_payload_len(&with_bin), bin_a.len() + bin_b.len());

        // With no msgpack bins, the JSON compatibility strings are summed instead.
        let json_only = ReadMultipleResponse {
            success: true,
            read_multiple_resps: vec![json.clone()],
            read_multiple_resps_bin: Vec::new(),
            error: None,
        };
        assert_eq!(read_multiple_response_payload_len(&json_only), json.len());

        // An empty response has zero payload.
        assert_eq!(read_multiple_response_payload_len(&ReadMultipleResponse::default()), 0);
    }

    #[test]
    fn compat_json_dual_writes_by_default() {
        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, None::<&str>),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, None::<&str>),
            ],
            || {
                let resp = sample_read_multiple_resp("file", b"data");
                let json = compat_json(&resp).expect("compat_json should encode");
                assert!(!json.is_empty());
                assert_eq!(json, serde_json::to_string(&resp).expect("json should encode"));
            },
        );
    }

    fn with_internode_msgpack_env<R>(vars: [(&'static str, Option<&'static str>); 2], f: impl FnOnce() -> R) -> R {
        temp_env::with_vars(vars, || {
            rustfs_protos::reset_internode_rpc_msgpack_only_cache();
            let result = f();
            rustfs_protos::reset_internode_rpc_msgpack_only_cache();
            result
        })
    }

    #[test]
    fn compat_json_keeps_json_when_msgpack_only_lacks_fleet_confirmation() {
        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, None::<&str>),
            ],
            || {
                let resp = sample_read_multiple_resp("file", b"data");
                let json = compat_json(&resp).expect("compat_json should encode");

                assert!(!json.is_empty(), "old JSON peers must remain compatible without fleet confirmation");
                assert_eq!(json, serde_json::to_string(&resp).expect("json should encode"));
            },
        );
    }

    #[test]
    fn compat_json_omits_json_only_after_fleet_confirmation() {
        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
            || {
                let resp = sample_read_multiple_resp("file", b"data");
                let json = compat_json(&resp).expect("compat_json should encode");

                assert!(json.is_empty(), "msgpack-only may empty JSON only after explicit fleet confirmation");
            },
        );
    }

    #[test]
    fn compat_json_restores_json_when_either_msgpack_only_gate_is_removed() {
        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
            || {
                let resp = sample_read_multiple_resp("file", b"data");
                let json = compat_json(&resp).expect("compat_json should encode");

                assert!(json.is_empty(), "both gates should enter msgpack-only send mode");
            },
        );

        for vars in [
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("false")),
            ],
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("false")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
        ] {
            with_internode_msgpack_env(vars, || {
                let resp = sample_read_multiple_resp("file", b"data");
                let json = compat_json(&resp).expect("compat_json should encode");

                assert!(!json.is_empty(), "removing either gate should restore old-peer JSON compatibility");
                assert_eq!(json, serde_json::to_string(&resp).expect("json should encode"));
            });
        }
    }

    #[test]
    #[serial(internode_metrics)]
    fn read_multiple_response_decode_reports_corrupt_msgpack_item() {
        let endpoint = sample_remote_endpoint();
        let response = ReadMultipleResponse {
            success: true,
            read_multiple_resps: Vec::new(),
            read_multiple_resps_bin: vec![
                encode_msgpack(&sample_read_multiple_resp("ok", b"data"))
                    .expect("msgpack response should encode")
                    .into(),
                bytes::Bytes::from_static(b"not-msgpack"),
            ],
            error: None,
        };
        let before = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_error_total_for_test();

        let err = decode_read_multiple_response_items(response, &endpoint).expect_err("corrupt msgpack item should fail");
        let after = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_error_total_for_test();
        let err = err.to_string();

        assert!(err.contains("ReadMultipleResp msgpack item 1"), "unexpected error: {err}");
        assert!(err.contains("server:9000"), "unexpected error: {err}");
        assert!(after > before, "corrupt response msgpack should increment decode-error metrics");
    }

    #[test]
    #[serial(internode_metrics)]
    fn read_multiple_response_decode_reports_corrupt_json_item() {
        let endpoint = sample_remote_endpoint();
        let response = ReadMultipleResponse {
            success: true,
            read_multiple_resps: vec![
                serde_json::to_string(&sample_read_multiple_resp("good", b"ok")).expect("json response should encode"),
                "{not-json".to_string(),
            ],
            read_multiple_resps_bin: Vec::new(),
            error: None,
        };
        let before = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_error_total_for_test();

        let err = decode_read_multiple_response_items(response, &endpoint).expect_err("corrupt json item should fail");
        let after = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_error_total_for_test();
        let err = err.to_string();

        assert!(err.contains("ReadMultipleResp json item 1"), "unexpected error: {err}");
        assert!(err.contains("server:9000"), "unexpected error: {err}");
        assert!(after > before, "corrupt response JSON should increment decode-error metrics");
    }

    fn sample_batch_read_version_resp(index: usize, path: &str, success: bool) -> BatchReadVersionResp {
        let mut file_info = FileInfo::new(path, 1, 0);
        file_info.erasure.index = 1;
        BatchReadVersionResp {
            index,
            path: path.to_string(),
            version_id: "version-a".to_string(),
            success,
            file_info,
            error: if success {
                String::new()
            } else {
                "file version not found".to_string()
            },
            error_code: if success { 0 } else { DiskError::FileVersionNotFound.to_u32() },
        }
    }

    #[test]
    #[serial(internode_metrics)]
    fn batch_read_version_response_decode_prefers_msgpack_payloads() {
        let endpoint = sample_remote_endpoint();
        let msgpack_resp = sample_batch_read_version_resp(7, "msgpack-object", true);
        let json_resp = sample_batch_read_version_resp(1, "json-object", false);
        let response = BatchReadVersionResponse {
            success: true,
            batch_read_version_resps: vec![serde_json::to_string(&json_resp).expect("json fallback should encode")],
            batch_read_version_resps_bin: vec![encode_msgpack(&msgpack_resp).expect("msgpack response should encode").into()],
            error: None,
        };

        let decoded = decode_batch_read_version_response_items(response, &endpoint).expect("msgpack response should decode");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].index, 7);
        assert_eq!(decoded[0].path, "msgpack-object");
        assert!(decoded[0].success);
    }

    #[test]
    #[serial(internode_metrics)]
    fn batch_read_version_response_rejects_invalid_success_metadata() {
        let endpoint = sample_remote_endpoint();
        let mut response_item = sample_batch_read_version_resp(0, "invalid-object", true);
        response_item.file_info.erasure.data_blocks = 0;
        response_item.file_info.erasure.parity_blocks = 2;
        let response = BatchReadVersionResponse {
            success: true,
            batch_read_version_resps: Vec::new(),
            batch_read_version_resps_bin: vec![encode_msgpack(&response_item).expect("msgpack response should encode").into()],
            error: None,
        };

        let err = decode_batch_read_version_response_items(response, &endpoint)
            .expect_err("successful remote response with invalid metadata must fail closed");

        assert_eq!(err, DiskError::FileCorrupt);
    }

    #[test]
    #[serial(internode_metrics)]
    fn batch_read_version_response_decode_reports_corrupt_msgpack_item() {
        let endpoint = sample_remote_endpoint();
        let response = BatchReadVersionResponse {
            success: true,
            batch_read_version_resps: Vec::new(),
            batch_read_version_resps_bin: vec![
                encode_msgpack(&sample_batch_read_version_resp(0, "ok", true))
                    .expect("msgpack response should encode")
                    .into(),
                bytes::Bytes::from_static(b"not-msgpack"),
            ],
            error: None,
        };
        let before = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_error_total_for_test();

        let err = decode_batch_read_version_response_items(response, &endpoint)
            .expect_err("corrupt msgpack item should fail")
            .to_string();
        let after = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_error_total_for_test();

        assert!(err.contains("BatchReadVersionResp msgpack item 1"), "unexpected error: {err}");
        assert!(err.contains("server:9000"), "unexpected error: {err}");
        assert!(after > before, "corrupt batch response msgpack should increment decode-error metrics");
    }

    #[test]
    #[serial(internode_metrics)]
    fn batch_read_version_response_decode_reports_corrupt_json_item() {
        let endpoint = sample_remote_endpoint();
        let response = BatchReadVersionResponse {
            success: true,
            batch_read_version_resps: vec![
                serde_json::to_string(&sample_batch_read_version_resp(0, "ok", false)).expect("json should encode"),
                "{not-json".to_string(),
            ],
            batch_read_version_resps_bin: Vec::new(),
            error: None,
        };
        let before = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_error_total_for_test();

        let err = decode_batch_read_version_response_items(response, &endpoint)
            .expect_err("corrupt json item should fail")
            .to_string();
        let after = rustfs_io_metrics::internode_metrics::global_internode_metrics().msgpack_json_decode_error_total_for_test();

        assert!(err.contains("BatchReadVersionResp json item 1"), "unexpected error: {err}");
        assert!(err.contains("server:9000"), "unexpected error: {err}");
        assert!(after > before, "corrupt batch response JSON should increment decode-error metrics");
    }

    #[test]
    fn batch_metadata_rpc_mode_defaults_to_off_and_parses_supported_values() {
        assert_eq!(parse_batch_metadata_rpc_mode(""), BatchMetadataRpcMode::Off);
        assert_eq!(parse_batch_metadata_rpc_mode("off"), BatchMetadataRpcMode::Off);
        assert_eq!(parse_batch_metadata_rpc_mode("auto"), BatchMetadataRpcMode::Auto);
        assert_eq!(parse_batch_metadata_rpc_mode("on"), BatchMetadataRpcMode::On);
        assert_eq!(parse_batch_metadata_rpc_mode("unknown"), BatchMetadataRpcMode::Off);
        assert_eq!(BatchMetadataRpcMode::Off.as_str(), "off");
        assert_eq!(BatchMetadataRpcMode::Auto.as_str(), "auto");
        assert_eq!(BatchMetadataRpcMode::On.as_str(), "on");
        assert!(!BatchMetadataRpcMode::Off.should_attempt());
        assert!(BatchMetadataRpcMode::Auto.should_attempt());
        assert!(BatchMetadataRpcMode::On.should_attempt());
        assert_eq!(BATCH_READ_VERSION_GATE_ATTEMPT, "attempt");
        assert_eq!(BATCH_READ_VERSION_GATE_OFF_UNARY, "off_unary");
        assert_eq!(BATCH_READ_VERSION_GATE_FALLBACK_UNIMPLEMENTED, "fallback_unimplemented");
        assert_eq!(BATCH_READ_VERSION_GATE_UNSUPPORTED_NO_FALLBACK, "unsupported_no_fallback");
    }

    #[test]
    fn batch_metadata_rpc_mode_uses_documented_env_before_legacy_alias() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE, None::<&str>),
                (ENV_RUSTFS_METADATA_BATCH_READ, Some("auto")),
                (LEGACY_ENV_RUSTFS_BATCH_METADATA_RPC, Some("on")),
            ],
            || {
                assert_eq!(batch_metadata_rpc_mode_from_env(), BatchMetadataRpcMode::Auto);
            },
        );
    }

    #[test]
    fn batch_metadata_rpc_mode_uses_get_coalescer_env_before_batch_env() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE, Some("on")),
                (ENV_RUSTFS_METADATA_BATCH_READ, Some("off")),
                (LEGACY_ENV_RUSTFS_BATCH_METADATA_RPC, Some("off")),
            ],
            || {
                assert_eq!(batch_metadata_rpc_mode_from_env(), BatchMetadataRpcMode::On);
            },
        );
    }

    #[test]
    fn batch_metadata_rpc_mode_falls_back_to_legacy_env_alias() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_METADATA_READ_VERSION_COALESCE, None::<&str>),
                (ENV_RUSTFS_METADATA_BATCH_READ, None::<&str>),
                (LEGACY_ENV_RUSTFS_BATCH_METADATA_RPC, Some("on")),
            ],
            || {
                assert_eq!(batch_metadata_rpc_mode_from_env(), BatchMetadataRpcMode::On);
            },
        );
    }

    #[test]
    fn batch_metadata_rpc_mode_only_auto_falls_back_on_unimplemented() {
        assert!(!BatchMetadataRpcMode::Off.should_fallback_on_unimplemented());
        assert!(BatchMetadataRpcMode::Auto.should_fallback_on_unimplemented());
        assert!(!BatchMetadataRpcMode::On.should_fallback_on_unimplemented());
    }

    #[test]
    fn rename_data_file_info_named_msgpack_is_smaller_than_json() {
        let file_info = sample_rename_data_file_info();
        let json = serde_json::to_vec(&file_info).expect("file info json should encode");
        let named_msgpack = encode_file_info_msgpack(&file_info).expect("file info named msgpack should encode");

        assert!(
            named_msgpack.len() <= FILE_INFO_MSGPACK_ENCODE_CAPACITY_HINT,
            "typical FileInfo should fit the msgpack capacity hint (msgpack={}, hint={FILE_INFO_MSGPACK_ENCODE_CAPACITY_HINT})",
            named_msgpack.len()
        );
        assert!(
            named_msgpack.len() < json.len(),
            "expected named msgpack payload to be smaller than json (msgpack={}, json={})",
            named_msgpack.len(),
            json.len()
        );
    }

    #[test]
    fn rename_data_resp_named_msgpack_is_smaller_than_json() {
        let response = RenameDataResp {
            old_data_dir: Some(Uuid::new_v4()),
            rollback_data_dir: Some(Uuid::new_v4()),
            cleanup_data_dir: Some(Uuid::new_v4()),
            sign: Some(vec![1_u8; 32]),
            old_current_size: Some(crate::disk::OldCurrentSize::Present(4096)),
        };
        let json = serde_json::to_vec(&response).expect("rename data response json should encode");
        let named_msgpack = rmp_serde::encode::to_vec_named(&response).expect("rename data response named msgpack should encode");

        assert!(
            named_msgpack.len() < json.len(),
            "expected named msgpack payload to be smaller than json (msgpack={}, json={})",
            named_msgpack.len(),
            json.len()
        );
    }

    #[derive(Debug, Default)]
    struct SinkTestWriter;

    impl AsyncWrite for SinkTestWriter {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for RecordingInternodeDataTransport {
        async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
            self.record(RecordedTransportCall::Read(request));
            Ok(Box::new(EmptyTestReader))
        }

        async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
            self.record(RecordedTransportCall::Write(request));
            Ok(Box::new(SinkTestWriter))
        }

        async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader> {
            self.record(RecordedTransportCall::WalkDir(request));
            Ok(Box::new(EmptyTestReader))
        }

        async fn open_ns_scanner(&self, request: NsScannerStreamRequest) -> Result<FileReader> {
            self.record(RecordedTransportCall::NsScanner(request));
            Ok(Box::new(EmptyTestReader))
        }

        async fn probe_ns_scanner(&self, request: NsScannerCapabilityRequest) -> Result<Uuid> {
            self.record(RecordedTransportCall::NsScannerProbe(request));
            if let Some(status) = *self
                .ns_scanner_probe_status
                .lock()
                .expect("namespace scanner probe status lock poisoned")
            {
                let status = reqwest::StatusCode::from_u16(status).expect("test status code should be valid");
                return Err(
                    rustfs_rio::new_test_internode_http_io_error(rustfs_rio::InternodeHttpErrorKind::HttpStatus(status)).into(),
                );
            }
            Ok(Uuid::from_u128(1))
        }

        fn name(&self) -> &'static str {
            "recording"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for RetryingWalkDirInternodeDataTransport {
        async fn open_read(&self, _request: ReadStreamRequest) -> Result<FileReader> {
            panic!("open_read should not be used in walk_dir retry test");
        }

        async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
            panic!("open_write should not be used in walk_dir retry test");
        }

        async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader> {
            self.record(RecordedTransportCall::WalkDir(request));
            let step = self.steps.lock().expect("walk_dir retry steps lock poisoned").remove(0);
            match step {
                WalkDirTestStep::Error(err) => Err(err),
                WalkDirTestStep::Data(data) => Ok(Box::new(Cursor::new(data))),
                WalkDirTestStep::PartialDataThenError { data, error } => Ok(Box::new(PartialThenErrorReader {
                    cursor: Cursor::new(data),
                    error: Some(error),
                })),
            }
        }

        async fn open_ns_scanner(&self, _request: NsScannerStreamRequest) -> Result<FileReader> {
            panic!("open_ns_scanner should not be used in walk_dir retry test");
        }

        async fn probe_ns_scanner(&self, _request: NsScannerCapabilityRequest) -> Result<Uuid> {
            Ok(Uuid::from_u128(1))
        }

        fn name(&self) -> &'static str {
            "retrying-walk-dir"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for RetryingOpenWriteInternodeDataTransport {
        async fn open_read(&self, _request: ReadStreamRequest) -> Result<FileReader> {
            panic!("open_read should not be used in open_write retry test");
        }

        async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
            self.record(RecordedTransportCall::Write(request));
            let step = self.steps.lock().expect("open_write retry steps lock poisoned").remove(0);
            match step {
                OpenWriteTestStep::Error(err) => Err(err),
                OpenWriteTestStep::Success => Ok(Box::new(SinkTestWriter)),
            }
        }

        async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
            panic!("open_walk_dir should not be used in open_write retry test");
        }

        async fn open_ns_scanner(&self, _request: NsScannerStreamRequest) -> Result<FileReader> {
            panic!("open_ns_scanner should not be used in open_write retry test");
        }

        async fn probe_ns_scanner(&self, _request: NsScannerCapabilityRequest) -> Result<Uuid> {
            Ok(Uuid::from_u128(1))
        }

        fn name(&self) -> &'static str {
            "retrying-open-write"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for RetryingOpenReadInternodeDataTransport {
        async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
            self.record(RecordedTransportCall::Read(request));
            let step = self.steps.lock().expect("open_read retry steps lock poisoned").remove(0);
            match step {
                OpenWriteTestStep::Error(err) => Err(err),
                OpenWriteTestStep::Success => Ok(Box::new(EmptyTestReader)),
            }
        }

        async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
            panic!("open_write should not be used in open_read retry test");
        }

        async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
            panic!("open_walk_dir should not be used in open_read retry test");
        }

        fn name(&self) -> &'static str {
            "retrying-open-read"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    async fn new_remote_disk_with_transport(data_transport: Arc<dyn InternodeDataTransport>) -> RemoteDisk {
        let endpoint = Endpoint {
            url: url::Url::parse("http://remote-node:9000/data/rustfs0").expect("operation should succeed"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        RemoteDisk::new(&endpoint, &disk_option, data_transport)
            .await
            .expect("operation should succeed")
    }

    #[tokio::test]
    async fn remote_disk_health_wrapper_balances_task_cancellation() {
        let disk = Arc::new(new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await);
        let task_disk = Arc::clone(&disk);
        let task = tokio::spawn(async move {
            task_disk
                .execute_with_timeout_for_op(
                    "cancellation-test",
                    || async { std::future::pending::<Result<()>>().await },
                    Duration::ZERO,
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while disk.health.waiting_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("operation should enter remote disk health tracking");
        task.abort();
        let _ = task.await;

        assert_eq!(disk.health.waiting_count(), 0);
    }

    #[derive(Debug)]
    struct PartialThenErrorReader {
        cursor: Cursor<Vec<u8>>,
        error: Option<io::Error>,
    }

    impl AsyncRead for PartialThenErrorReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let filled_before = buf.filled().len();
            match Pin::new(&mut self.cursor).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    if buf.filled().len() > filled_before {
                        return Poll::Ready(Ok(()));
                    }

                    if let Some(err) = self.error.take() {
                        return Poll::Ready(Err(err));
                    }

                    Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
    }

    #[derive(Debug, Clone)]
    enum ResumeReadStep {
        PartialThenReset(Vec<u8>),
        Data(Vec<u8>),
    }

    #[derive(Debug, Default)]
    struct ResumeTransport {
        read_steps: Mutex<Vec<ResumeReadStep>>,
        chunk_steps: Mutex<Vec<ResumeReadStep>>,
        read_requests: Mutex<Vec<ReadStreamRequest>>,
        chunk_requests: Mutex<Vec<ReadStreamRequest>>,
        fresh_read_requests: Mutex<Vec<ReadStreamRequest>>,
        fresh_chunk_requests: Mutex<Vec<ReadStreamRequest>>,
    }

    impl ResumeTransport {
        fn with_read_steps(read_steps: Vec<ResumeReadStep>) -> Self {
            Self {
                read_steps: Mutex::new(read_steps),
                ..Self::default()
            }
        }

        fn with_chunk_steps(chunk_steps: Vec<ResumeReadStep>) -> Self {
            Self {
                chunk_steps: Mutex::new(chunk_steps),
                ..Self::default()
            }
        }
    }

    #[derive(Debug)]
    struct ChunkPartialThenErrorReader {
        data: Option<Bytes>,
        error: Option<io::Error>,
    }

    impl rustfs_rio::ChunkReader for ChunkPartialThenErrorReader {
        fn poll_read_chunk(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
            if let Some(mut data) = self.data.take() {
                let take = data.len().min(max);
                let chunk = data.split_to(take);
                if !data.is_empty() {
                    self.data = Some(data);
                }
                return Poll::Ready(Ok(Some(chunk)));
            }
            if let Some(error) = self.error.take() {
                return Poll::Ready(Err(error));
            }
            Poll::Ready(Ok(None))
        }
    }

    impl AsyncRead for ChunkPartialThenErrorReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("chunk reader must use chunk handoff")))
        }
    }

    #[derive(Debug)]
    struct BodyStallTestReader {
        data: Option<Bytes>,
        next_data: Option<(Bytes, Pin<Box<time::Sleep>>)>,
        initial_delay: Option<Pin<Box<time::Sleep>>>,
        timeout: Duration,
        stall_timer: Option<Pin<Box<time::Sleep>>>,
    }

    impl BodyStallTestReader {
        fn new(data: Bytes, initial_delay: Duration, timeout: Option<Duration>) -> Self {
            let timeout = timeout.expect("parallel resume test requires a body stall timeout");
            Self {
                data: Some(data),
                next_data: None,
                initial_delay: Some(Box::pin(time::sleep(initial_delay))),
                timeout,
                stall_timer: None,
            }
        }

        fn with_next_data(
            data: Bytes,
            initial_delay: Duration,
            next_data: Bytes,
            next_delay: Duration,
            timeout: Option<Duration>,
        ) -> Self {
            let mut reader = Self::new(data, initial_delay, timeout);
            reader.next_data = Some((next_data, Box::pin(time::sleep(next_delay))));
            reader
        }

        fn poll_chunk(&mut self, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
            if let Some(delay) = self.initial_delay.as_mut() {
                if delay.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.initial_delay = None;
            }
            if let Some(mut data) = self.data.take() {
                let chunk = data.split_to(data.len().min(max));
                if !data.is_empty() {
                    self.data = Some(data);
                }
                return Poll::Ready(Ok(Some(chunk)));
            }
            if let Some((_, delay)) = self.next_data.as_mut()
                && delay.as_mut().poll(cx).is_pending()
            {
                return Poll::Pending;
            }
            if let Some((mut data, _)) = self.next_data.take() {
                let chunk = data.split_to(data.len().min(max));
                if !data.is_empty() {
                    self.next_data = Some((data, Box::pin(time::sleep(Duration::ZERO))));
                }
                return Poll::Ready(Ok(Some(chunk)));
            }
            let timer = self.stall_timer.get_or_insert_with(|| Box::pin(time::sleep(self.timeout)));
            match timer.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                    std_io::ErrorKind::TimedOut,
                    rustfs_rio::BodyStalled { timeout: self.timeout },
                ))),
            }
        }
    }

    impl AsyncRead for BodyStallTestReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            match self.poll_chunk(cx, buf.remaining()) {
                Poll::Ready(Ok(Some(chunk))) => {
                    buf.put_slice(&chunk);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Ok(None)) => Poll::Ready(Ok(())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl rustfs_rio::ChunkReader for BodyStallTestReader {
        fn poll_read_chunk(mut self: Pin<&mut Self>, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
            self.poll_chunk(cx, max)
        }
    }

    struct CountOnDrop(Arc<AtomicUsize>);

    impl Drop for CountOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Debug)]
    struct ParallelResumeTransport {
        initial_data: Bytes,
        resumed_data: Bytes,
        initial_delay: Duration,
        fresh_delay: Duration,
        fresh_read_requests: Mutex<Vec<ReadStreamRequest>>,
        fresh_chunk_requests: Mutex<Vec<ReadStreamRequest>>,
        initial_next_data: Option<(Bytes, Duration)>,
    }

    impl ParallelResumeTransport {
        fn new(initial_delay: Duration, fresh_delay: Duration) -> Self {
            Self {
                initial_data: Bytes::from_static(b"da"),
                resumed_data: Bytes::from_static(b"ta"),
                initial_delay,
                fresh_delay,
                fresh_read_requests: Mutex::new(Vec::new()),
                fresh_chunk_requests: Mutex::new(Vec::new()),
                initial_next_data: None,
            }
        }

        fn with_initial_next_data(initial_delay: Duration, next_delay: Duration, fresh_delay: Duration) -> Self {
            let mut transport = Self::new(initial_delay, fresh_delay);
            transport.initial_next_data = Some((Bytes::from_static(b"ta"), next_delay));
            transport
        }
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for ParallelResumeTransport {
        async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
            let reader = match self.initial_next_data.as_ref() {
                Some((next_data, next_delay)) => BodyStallTestReader::with_next_data(
                    self.initial_data.clone(),
                    self.initial_delay,
                    next_data.clone(),
                    *next_delay,
                    request.stall_timeout,
                ),
                None => BodyStallTestReader::new(self.initial_data.clone(), self.initial_delay, request.stall_timeout),
            };
            Ok(Box::new(reader))
        }

        async fn open_read_fresh(&self, request: ReadStreamRequest) -> Result<FileReader> {
            self.fresh_read_requests
                .lock()
                .expect("fresh read request lock should not be poisoned")
                .push(request);
            time::sleep(self.fresh_delay).await;
            Ok(Box::new(Cursor::new(self.resumed_data.clone())))
        }

        async fn open_read_chunks(&self, request: ReadStreamRequest) -> Result<Option<rustfs_rio::ChunkReaderBox>> {
            let reader = match self.initial_next_data.as_ref() {
                Some((next_data, next_delay)) => BodyStallTestReader::with_next_data(
                    self.initial_data.clone(),
                    self.initial_delay,
                    next_data.clone(),
                    *next_delay,
                    request.stall_timeout,
                ),
                None => BodyStallTestReader::new(self.initial_data.clone(), self.initial_delay, request.stall_timeout),
            };
            Ok(Some(Box::new(reader)))
        }

        async fn open_read_chunks_fresh(&self, request: ReadStreamRequest) -> Result<Option<rustfs_rio::ChunkReaderBox>> {
            self.fresh_chunk_requests
                .lock()
                .expect("fresh chunk request lock should not be poisoned")
                .push(request);
            time::sleep(self.fresh_delay).await;
            Ok(Some(Box::new(ChunkPartialThenErrorReader {
                data: Some(self.resumed_data.clone()),
                error: None,
            })))
        }

        async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
            panic!("open_write should not be used in parallel resume tests");
        }

        async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
            panic!("open_walk_dir should not be used in parallel resume tests");
        }

        fn name(&self) -> &'static str {
            "parallel-resume-test"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    #[derive(Debug, Default)]
    struct PendingFreshOpenTransport {
        fresh_read_drops: Arc<AtomicUsize>,
        fresh_chunk_drops: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for PendingFreshOpenTransport {
        async fn open_read(&self, _request: ReadStreamRequest) -> Result<FileReader> {
            Ok(Box::new(PartialThenErrorReader {
                cursor: Cursor::new(Vec::new()),
                error: Some(io::Error::new(std_io::ErrorKind::ConnectionReset, "stream reset")),
            }))
        }

        async fn open_read_fresh(&self, _request: ReadStreamRequest) -> Result<FileReader> {
            let _drop = CountOnDrop(Arc::clone(&self.fresh_read_drops));
            std::future::pending().await
        }

        async fn open_read_chunks(&self, _request: ReadStreamRequest) -> Result<Option<rustfs_rio::ChunkReaderBox>> {
            Ok(Some(Box::new(ChunkPartialThenErrorReader {
                data: None,
                error: Some(io::Error::new(std_io::ErrorKind::ConnectionReset, "stream reset")),
            })))
        }

        async fn open_read_chunks_fresh(&self, _request: ReadStreamRequest) -> Result<Option<rustfs_rio::ChunkReaderBox>> {
            let _drop = CountOnDrop(Arc::clone(&self.fresh_chunk_drops));
            std::future::pending().await
        }

        async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
            panic!("open_write should not be used in fresh open cancellation tests");
        }

        async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
            panic!("open_walk_dir should not be used in fresh open cancellation tests");
        }

        fn name(&self) -> &'static str {
            "pending-fresh-open-test"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    fn resume_step_reader(step: ResumeReadStep) -> FileReader {
        match step {
            ResumeReadStep::PartialThenReset(data) => Box::new(PartialThenErrorReader {
                cursor: Cursor::new(data),
                error: Some(io::Error::new(std_io::ErrorKind::ConnectionReset, "stream reset")),
            }),
            ResumeReadStep::Data(data) => Box::new(Cursor::new(data)),
        }
    }

    fn resume_step_chunk_reader(step: ResumeReadStep) -> rustfs_rio::ChunkReaderBox {
        match step {
            ResumeReadStep::PartialThenReset(data) => Box::new(ChunkPartialThenErrorReader {
                data: Some(Bytes::from(data)),
                error: Some(io::Error::new(std_io::ErrorKind::ConnectionReset, "stream reset")),
            }),
            ResumeReadStep::Data(data) => Box::new(ChunkPartialThenErrorReader {
                data: Some(Bytes::from(data)),
                error: None,
            }),
        }
    }

    #[async_trait::async_trait]
    impl InternodeDataTransport for ResumeTransport {
        async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
            self.read_requests
                .lock()
                .expect("read request lock should not be poisoned")
                .push(request);
            let step = self
                .read_steps
                .lock()
                .expect("read steps lock should not be poisoned")
                .remove(0);
            Ok(resume_step_reader(step))
        }

        async fn open_read_fresh(&self, request: ReadStreamRequest) -> Result<FileReader> {
            self.fresh_read_requests
                .lock()
                .expect("fresh read request lock should not be poisoned")
                .push(request.clone());
            self.open_read(request).await
        }

        async fn open_read_chunks(&self, request: ReadStreamRequest) -> Result<Option<rustfs_rio::ChunkReaderBox>> {
            self.chunk_requests
                .lock()
                .expect("chunk request lock should not be poisoned")
                .push(request);
            let step = self
                .chunk_steps
                .lock()
                .expect("chunk steps lock should not be poisoned")
                .remove(0);
            Ok(Some(resume_step_chunk_reader(step)))
        }

        async fn open_read_chunks_fresh(&self, request: ReadStreamRequest) -> Result<Option<rustfs_rio::ChunkReaderBox>> {
            self.fresh_chunk_requests
                .lock()
                .expect("fresh chunk request lock should not be poisoned")
                .push(request.clone());
            self.open_read_chunks(request).await
        }

        async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
            panic!("open_write should not be used in remote read resume tests");
        }

        async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
            panic!("open_walk_dir should not be used in remote read resume tests");
        }

        fn name(&self) -> &'static str {
            "resume-test"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    fn resume_request(length: usize) -> ReadStreamRequest {
        ReadStreamRequest {
            endpoint: "http://remote".to_string(),
            disk: "disk".to_string(),
            volume: "volume".to_string(),
            path: "path".to_string(),
            offset: 7,
            length,
            stall_timeout: None,
        }
    }

    #[tokio::test]
    async fn remote_reader_resumes_from_emitted_bytes_without_duplicates() {
        let transport = Arc::new(ResumeTransport::with_read_steps(vec![ResumeReadStep::Data(b"456789".to_vec())]));
        let request = resume_request(10);
        let reader = resume_step_reader(ResumeReadStep::PartialThenReset(b"0123".to_vec()));
        let mut reader = RetryingRemoteReader::new_with_timeouts(reader, transport.clone(), request, None, None);
        let mut output = Vec::new();
        reader
            .read_to_end(&mut output)
            .await
            .expect("one body reset should be resumed");

        assert_eq!(output, b"0123456789");
        let requests = transport
            .read_requests
            .lock()
            .expect("read request lock should not be poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].offset, 11);
        assert_eq!(requests[0].length, 6);
        assert_eq!(
            transport
                .fresh_read_requests
                .lock()
                .expect("fresh read request lock should not be poisoned")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn remote_chunk_reader_resumes_from_emitted_bytes_without_duplicates() {
        let transport = Arc::new(ResumeTransport::with_chunk_steps(vec![ResumeReadStep::Data(b"456789".to_vec())]));
        let request = resume_request(10);
        let reader = resume_step_chunk_reader(ResumeReadStep::PartialThenReset(b"0123".to_vec()));
        let mut reader = RetryingRemoteChunkReader::new_with_timeouts(reader, transport.clone(), request, None, None);
        let mut output = Vec::new();
        reader
            .read_to_end(&mut output)
            .await
            .expect("chunk body reset should be resumed");

        assert_eq!(output, b"0123456789");
        let requests = transport
            .chunk_requests
            .lock()
            .expect("chunk request lock should not be poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].offset, 11);
        assert_eq!(requests[0].length, 6);
        assert_eq!(
            transport
                .fresh_chunk_requests
                .lock()
                .expect("fresh chunk request lock should not be poisoned")
                .len(),
            1
        );
    }

    #[derive(Clone, Copy)]
    enum ParallelResumePath {
        Regular,
        Chunk,
    }

    async fn assert_parallel_resume_case(
        path: ParallelResumePath,
        initial_delay: Duration,
        initial_next_delay: Option<Duration>,
        fresh_delay: Duration,
        expect_success: bool,
    ) {
        const DATA: &[u8] = b"data";
        let transport = Arc::new(match initial_next_delay {
            Some(next_delay) => ParallelResumeTransport::with_initial_next_data(initial_delay, next_delay, fresh_delay),
            None => ParallelResumeTransport::new(initial_delay, fresh_delay),
        });
        let remote_disk = new_remote_disk_with_transport(transport.clone()).await;
        let erasure = Erasure::new(1, 1, DATA.len());
        let (buffers, errors) = match path {
            ParallelResumePath::Regular => {
                let reader = remote_disk
                    .read_file_stream("bucket", "object/part.1", 0, DATA.len())
                    .await
                    .expect("initial remote reader should open");
                let readers = vec![
                    Some(BitrotReader::new(reader, DATA.len(), rustfs_utils::HashAlgorithm::None, false)),
                    None,
                ];
                ParallelReader::new_with_metrics_path_and_reconstruction_verification(readers, erasure, 0, DATA.len(), None)
                    .read()
                    .await
            }
            ParallelResumePath::Chunk => {
                let reader = remote_disk
                    .read_file_stream_chunks("bucket", "object/part.1", 0, DATA.len())
                    .await
                    .expect("initial remote chunk reader should open")
                    .expect("chunk transport should return a reader");
                let readers = vec![
                    Some(BitrotReader::new(
                        ShardReader::Chunked(reader),
                        DATA.len(),
                        rustfs_utils::HashAlgorithm::None,
                        false,
                    )),
                    None,
                ];
                ParallelReader::new_with_metrics_path_and_reconstruction_verification(readers, erasure, 0, DATA.len(), None)
                    .read()
                    .await
            }
        };

        if expect_success {
            assert_eq!(buffers[0].as_deref(), Some(DATA));
            assert!(errors[0].is_none());
        } else {
            assert!(buffers[0].is_none());
            assert!(matches!(errors[0], Some(DiskError::Timeout)));
        }
        let requests = match path {
            ParallelResumePath::Regular => transport
                .fresh_read_requests
                .lock()
                .expect("fresh read request lock should not be poisoned"),
            ParallelResumePath::Chunk => transport
                .fresh_chunk_requests
                .lock()
                .expect("fresh chunk request lock should not be poisoned"),
        };
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].offset, 2);
        assert_eq!(requests[0].length, 2);
    }

    async fn assert_parallel_resume_path(path: ParallelResumePath) {
        assert_parallel_resume_case(path, Duration::ZERO, None, Duration::from_millis(100), true).await;
        assert_parallel_resume_case(path, Duration::from_millis(500), None, Duration::from_millis(100), true).await;
        assert_parallel_resume_case(path, Duration::ZERO, None, Duration::from_millis(500), false).await;
        assert_parallel_resume_case(
            path,
            Duration::from_millis(650),
            Some(Duration::from_millis(700)),
            Duration::from_millis(500),
            true,
        )
        .await;
    }

    async fn assert_retry_drop_cancels_fresh_open(path: ParallelResumePath) {
        let transport = Arc::new(PendingFreshOpenTransport::default());
        let remote_disk = new_remote_disk_with_transport(transport.clone()).await;
        let mut output = [0_u8; 1];
        match path {
            ParallelResumePath::Regular => {
                let mut reader = remote_disk
                    .read_file_stream("bucket", "object/part.1", 0, 1)
                    .await
                    .expect("initial remote reader should open");
                assert!(
                    time::timeout(Duration::from_millis(20), reader.read(&mut output))
                        .await
                        .is_err()
                );
                drop(reader);
            }
            ParallelResumePath::Chunk => {
                let mut reader = remote_disk
                    .read_file_stream_chunks("bucket", "object/part.1", 0, 1)
                    .await
                    .expect("initial remote chunk reader should open")
                    .expect("chunk transport should return a reader");
                assert!(
                    time::timeout(Duration::from_millis(20), reader.read(&mut output))
                        .await
                        .is_err()
                );
                drop(reader);
            }
        }
        time::timeout(Duration::from_secs(1), async {
            loop {
                let drops = match path {
                    ParallelResumePath::Regular => transport.fresh_read_drops.load(Ordering::Relaxed),
                    ParallelResumePath::Chunk => transport.fresh_chunk_drops.load(Ordering::Relaxed),
                };
                if drops == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the retrying reader should cancel the pending fresh open");
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn remote_reader_recovers_body_stall_through_parallel_reader() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, Some("1"))], async {
            assert_parallel_resume_path(ParallelResumePath::Regular).await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn remote_chunk_reader_recovers_body_stall_through_parallel_reader() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, Some("1"))], async {
            assert_parallel_resume_path(ParallelResumePath::Chunk).await;
        })
        .await;
    }

    #[tokio::test]
    async fn remote_reader_drop_cancels_pending_fresh_open() {
        assert_retry_drop_cancels_fresh_open(ParallelResumePath::Regular).await;
    }

    #[tokio::test]
    async fn remote_chunk_reader_drop_cancels_pending_fresh_open() {
        assert_retry_drop_cancels_fresh_open(ParallelResumePath::Chunk).await;
    }

    #[tokio::test]
    async fn remote_reader_treats_error_after_requested_length_as_eof() {
        let transport = Arc::new(ResumeTransport::default());
        let reader = resume_step_reader(ResumeReadStep::PartialThenReset(b"0123".to_vec()));
        let mut reader = RetryingRemoteReader::new_with_timeouts(reader, transport.clone(), resume_request(4), None, None);
        let mut output = Vec::new();

        reader
            .read_to_end(&mut output)
            .await
            .expect("error after the requested bytes should not trigger a redundant resume");
        assert_eq!(output, b"0123");
        assert!(
            transport
                .fresh_read_requests
                .lock()
                .expect("fresh read request lock should not be poisoned")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn remote_chunk_reader_treats_error_after_requested_length_as_eof() {
        let transport = Arc::new(ResumeTransport::default());
        let reader = resume_step_chunk_reader(ResumeReadStep::PartialThenReset(b"0123".to_vec()));
        let mut reader = RetryingRemoteChunkReader::new_with_timeouts(reader, transport.clone(), resume_request(4), None, None);
        let mut output = Vec::new();

        reader
            .read_to_end(&mut output)
            .await
            .expect("error after the requested bytes should not trigger a redundant chunk resume");
        assert_eq!(output, b"0123");
        assert!(
            transport
                .fresh_chunk_requests
                .lock()
                .expect("fresh chunk request lock should not be poisoned")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn remote_reader_retries_at_most_once_and_preserves_non_retryable_errors() {
        let transport = Arc::new(ResumeTransport::with_read_steps(vec![ResumeReadStep::PartialThenReset(b"456".to_vec())]));
        let mut reader = RetryingRemoteReader::new_with_timeouts(
            resume_step_reader(ResumeReadStep::PartialThenReset(b"0123".to_vec())),
            transport.clone(),
            resume_request(7),
            None,
            None,
        );
        let error = reader
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("second reset must not retry");
        assert_eq!(error.kind(), std_io::ErrorKind::ConnectionReset);
        assert_eq!(
            transport
                .read_requests
                .lock()
                .expect("read request lock should not be poisoned")
                .len(),
            1
        );

        let transport = Arc::new(ResumeTransport::default());
        let reader = PartialThenErrorReader {
            cursor: Cursor::new(b"data".to_vec()),
            error: Some(io::Error::new(std_io::ErrorKind::PermissionDenied, "permission denied")),
        };
        let mut reader =
            RetryingRemoteReader::new_with_timeouts(Box::new(reader), transport.clone(), resume_request(4), None, None);
        let error = reader
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("non-retryable errors must not retry");
        assert_eq!(error.kind(), std_io::ErrorKind::PermissionDenied);
        assert!(
            transport
                .read_requests
                .lock()
                .expect("read request lock should not be poisoned")
                .is_empty()
        );
    }

    #[test]
    fn resumed_read_request_checks_large_offsets() {
        let request = ReadStreamRequest {
            offset: usize::MAX - 1,
            length: 0,
            ..resume_request(0)
        };
        assert!(resumed_read_request(&request, 2).is_err());

        let request = resume_request(4);
        assert!(resumed_read_request(&request, 5).is_err());
    }

    fn init_tracing(filter_level: Level) {
        INIT.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_max_level(filter_level)
                .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                .with_thread_names(true)
                .try_init();
        });
    }

    #[tokio::test]
    async fn test_remote_disk_creation() {
        let url = url::Url::parse("http://example.com:9000/path").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 1,
            disk_idx: 2,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");

        assert!(!remote_disk.is_local());
        assert_eq!(remote_disk.endpoint.url, url);
        assert_eq!(remote_disk.endpoint.pool_idx, 0);
        assert_eq!(remote_disk.endpoint.set_idx, 1);
        assert_eq!(remote_disk.endpoint.disk_idx, 2);
        assert_eq!(remote_disk.host_name(), "example.com:9000");
    }

    #[tokio::test]
    async fn test_remote_disk_basic_properties() {
        let url = url::Url::parse("http://remote-server:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: -1,
            set_idx: -1,
            disk_idx: -1,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");

        // Test basic properties
        assert!(!remote_disk.is_local());
        assert_eq!(remote_disk.host_name(), "remote-server:9000");
        assert!(remote_disk.to_string().contains("remote-server"));
        assert!(remote_disk.to_string().contains("9000"));

        // Test disk location
        let location = remote_disk.get_disk_location();
        assert_eq!(location.pool_idx, None);
        assert_eq!(location.set_idx, None);
        assert_eq!(location.disk_idx, None);
        assert!(!location.valid()); // None values make it invalid
    }

    #[tokio::test]
    async fn test_remote_disk_path() {
        let url = url::Url::parse("http://remote-server:9000/storage").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        let path = remote_disk.path();

        // Remote disk path should be based on the URL path
        assert!(path.to_string_lossy().contains("storage"));
    }

    #[tokio::test]
    async fn test_remote_disk_is_online_detects_active_listener() {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");

        let url =
            url::Url::parse(&format!("http://{}:{}/data/rustfs0", addr.ip(), addr.port())).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        assert!(remote_disk.is_online().await);

        drop(listener);
    }

    #[tokio::test]
    async fn test_remote_disk_is_online_detects_missing_listener() {
        init_tracing(Level::ERROR);

        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");
        let ip = addr.ip();
        let port = addr.port();

        drop(listener);

        let url = url::Url::parse(&format!("http://{ip}:{port}/data/rustfs0")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_ACTIVE_CHECK_INTERVAL_SECS, Some("1")),
                (rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, Some("1")),
            ],
            async {
                let disk_option = DiskOption {
                    cleanup: false,
                    health_check: true,
                };

                let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
                    .await
                    .expect("operation should succeed");
                remote_disk.enable_health_check();

                // Wait out the initial success-grace window so the active probe loop
                // actually attempts a connectivity check. Under the new
                // suspect-first semantics we only need to prove that the drive
                // transitions away from a clean Online state at least once.
                tokio::time::sleep(SKIP_IF_SUCCESS_BEFORE + Duration::from_secs(2)).await;
                assert!(
                    remote_disk.offline_duration_secs().is_some(),
                    "missing listener should transition the drive through suspect/offline tracking"
                );
                assert_ne!(
                    remote_disk.runtime_state(),
                    RuntimeDriveHealthState::Online,
                    "missing listener should not remain in a clean Online state after probing"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_remote_disk_recovery_requires_disk_rpc_readiness() {
        init_tracing(Level::ERROR);
        runtime_sources::ensure_test_rpc_secret();

        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");
        let accept_task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let base_addr = format!("http://{}:{}", addr.ip(), addr.port());
        let url = url::Url::parse(&format!("{base_addr}/data/rustfs0")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let health = Arc::new(DiskHealthTracker::new());
        health.mark_failure(&endpoint, "test_failure");
        health.mark_failure(&endpoint, "test_failure");
        assert_eq!(health.runtime_state(), RuntimeDriveHealthState::Offline);
        let channel = TonicEndpoint::from_shared(base_addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(base_addr.clone(), channel).await;
        assert!(runtime_sources::test_node_channel_is_cached(&base_addr).await);

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_RETURNING_PROBE_INTERVAL_SECS, Some("1")),
                (rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, Some("1")),
            ],
            async {
                let cancel_token = CancellationToken::new();
                let monitor = tokio::spawn(RemoteDisk::monitor_remote_disk_recovery(
                    base_addr.clone(),
                    endpoint,
                    Arc::clone(&health),
                    cancel_token.clone(),
                ));

                tokio::time::sleep(Duration::from_millis(2_500)).await;
                cancel_token.cancel();
                let _ = monitor.await;

                assert_ne!(
                    health.runtime_state(),
                    RuntimeDriveHealthState::Online,
                    "a plain TCP listener without disk_info RPC readiness must not restore the remote disk online"
                );
                assert!(
                    !runtime_sources::test_node_channel_is_cached(&base_addr).await,
                    "failed recovery probes should evict stale cached gRPC channels"
                );
            },
        )
        .await;

        accept_task.abort();
    }

    #[tokio::test]
    async fn faulty_handle_runs_only_one_recovery_monitor() {
        let endpoint = Endpoint {
            url: url::Url::parse("http://remote-node:9000/data/rustfs0").expect("endpoint should parse"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: true,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("remote disk should construct");
        if !disk.health_check {
            return;
        }

        disk.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);
        disk.spawn_recovery_monitor_if_needed();
        disk.spawn_recovery_monitor_if_needed();
        tokio::time::timeout(Duration::from_secs(1), async {
            while disk.recovery_monitor_start_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery monitor should start");

        assert!(disk.recovery_monitor_is_active(), "only one recovery monitor should own the handle");
        assert_eq!(
            disk.recovery_monitor_start_count(),
            1,
            "the failed compare-exchange path must not start a second monitor"
        );

        disk.cancel_token.cancel();
        tokio::time::timeout(Duration::from_secs(1), async {
            while disk.recovery_monitor_is_active() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled recovery monitor should release its single-flight state");
        assert!(!disk.recovery_monitor_is_active());
    }

    #[tokio::test]
    #[serial(remote_disk_recovery_probe)]
    async fn recovery_monitor_rearms_if_disk_fails_during_teardown() {
        runtime_sources::ensure_test_rpc_secret();
        let Some(peer) = TestGrpcPeer::spawn(Bytes::new(), Bytes::new()).await else {
            return;
        };
        let endpoint = Endpoint {
            url: url::Url::parse(&format!("{}/data/rustfs0", peer.addr)).expect("endpoint should parse"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: true,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("remote disk should construct");
        if !disk.health_check {
            peer.stop().await;
            return;
        }

        disk.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);
        let hook = Arc::new(RecoveryMonitorTeardownHook::default());
        *disk.recovery_monitor_teardown_hook.lock().await = Some(Arc::clone(&hook));

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_RETURNING_PROBE_INTERVAL_SECS, Some("1")),
                (rustfs_config::ENV_DRIVE_RETURNING_SUCCESS_THRESHOLD, Some("1")),
                (rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, Some("1")),
            ],
            async {
                disk.spawn_recovery_monitor_if_needed();
                tokio::time::timeout(Duration::from_secs(5), hook.arrived.notified())
                    .await
                    .expect("first recovery monitor should reach teardown");
                assert_eq!(disk.runtime_state(), RuntimeDriveHealthState::Online);

                disk.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);
                hook.release.notify_one();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while disk.recovery_monitor_start_count() < 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("teardown failure should re-arm recovery monitoring");
                assert!(
                    disk.recovery_monitor_is_active(),
                    "re-armed monitor should retain single-flight ownership"
                );

                disk.cancel_token.cancel();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while disk.recovery_monitor_is_active() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("cancelled re-armed monitor should release single-flight state");
            },
        )
        .await;

        peer.stop().await;
    }

    #[tokio::test]
    #[serial(remote_disk_recovery_probe)]
    async fn recovery_monitor_restores_online_then_real_reads_use_replacement_handle() {
        runtime_sources::ensure_test_rpc_secret();
        let mut format = crate::layout::format::FormatV3::new(1, 1);
        let disk_id = format.erasure.sets[0][0];
        format.erasure.this = disk_id;
        let format_data = Bytes::from(format.to_json().expect("test format should serialize"));
        let Some(peer) = TestGrpcPeer::spawn(format_data, Bytes::from_static(b"replacement-data")).await else {
            return;
        };
        let url = url::Url::parse(&format!("{}/data/rustfs0", peer.addr)).expect("endpoint should parse");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: true,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("remote disk should construct");
        disk.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_RETURNING_PROBE_INTERVAL_SECS, Some("1")),
                (rustfs_config::ENV_DRIVE_RETURNING_SUCCESS_THRESHOLD, Some("3")),
                (rustfs_config::ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS, Some("1")),
            ],
            async {
                let monitor = tokio::spawn(RemoteDisk::monitor_remote_disk_recovery(
                    disk.addr.clone(),
                    endpoint.clone(),
                    Arc::clone(&disk.health),
                    disk.cancel_token.clone(),
                ));

                tokio::time::timeout(Duration::from_secs(5), async {
                    while disk.runtime_state() != RuntimeDriveHealthState::Online {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .expect("three authenticated recovery probes should restore the disk online");
                monitor.await.expect("recovery monitor should exit after restoring Online");

                assert_eq!(
                    peer.peer.disk_info_calls(),
                    3,
                    "RemoteDisk recovery requires the configured three successful disk_info probes"
                );
                let recovered_read = disk.read_all("bucket", "object").await.expect("recovered handle should read");
                assert_eq!(recovered_read, Bytes::from_static(b"replacement-data"));

                let old_disk = crate::disk::new_disk(
                    &endpoint,
                    &DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await
                .expect("old slot disk should construct");
                let set_disks = crate::set_disk::SetDisks::new(
                    "remote-recovery-test".to_string(),
                    Arc::new(tokio::sync::RwLock::new(vec![Some(old_disk.clone())])),
                    1,
                    0,
                    0,
                    0,
                    vec![endpoint.clone()],
                    format,
                    Vec::new(),
                )
                .await;
                set_disks.disks.write().await[0] = None;
                set_disks.renew_disk(&endpoint).await;

                let slots = set_disks.disks.read().await;
                let replacement = slots[0]
                    .as_ref()
                    .expect("renew_disk should publish the replacement slot")
                    .clone();
                drop(slots);
                assert!(!Arc::ptr_eq(&replacement, &old_disk), "renew_disk must replace the stale slot handle");
                let replacement_read = replacement
                    .read_all("bucket", "object")
                    .await
                    .expect("production slot should route real reads through the replacement");
                assert_eq!(replacement_read, Bytes::from_static(b"replacement-data"));
                let object_reads = peer.peer.object_read_all_disks();
                assert_eq!(object_reads.len(), 2, "standalone and production-slot reads should both reach the peer");
                assert_eq!(object_reads[1], disk_id.to_string(), "production slot must use the renewed disk identity");
                assert!(peer.peer.read_all_calls() >= 3, "renewal must read format metadata before the slot read");
                disk.cancel_token.cancel();
                old_disk.close().await.expect("old slot disk should close");
                replacement.close().await.expect("replacement slot disk should close");
            },
        )
        .await;

        peer.stop().await;
    }

    #[tokio::test]
    async fn test_copy_stream_with_buffer_copies_full_payload() {
        let payload = b"walk-dir-stream".repeat(1024);
        let expected = payload.clone();
        let (mut write_half, mut read_half) = duplex(128);

        let copy_task = tokio::spawn(async move {
            let mut cursor = Cursor::new(payload);
            copy_stream_with_buffer(&mut cursor, &mut write_half, 4 * 1024)
                .await
                .expect("operation should succeed");
        });

        let mut copied = Vec::new();
        read_half.read_to_end(&mut copied).await.expect("operation should succeed");
        copy_task.await.expect("operation should succeed");

        assert_eq!(copied, expected);
    }

    #[tokio::test]
    async fn test_remote_disk_disk_id() {
        let url = url::Url::parse("http://remote-server:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");

        // Initially, disk ID should be None
        let initial_id = remote_disk.get_disk_id().await.expect("operation should succeed");
        assert!(initial_id.is_none());

        // Set a disk ID
        let test_id = Uuid::new_v4();
        remote_disk
            .set_disk_id(Some(test_id))
            .await
            .expect("operation should succeed");

        // Verify the disk ID was set
        let retrieved_id = remote_disk.get_disk_id().await.expect("operation should succeed");
        assert_eq!(retrieved_id, Some(test_id));

        // Clear the disk ID
        remote_disk.set_disk_id(None).await.expect("operation should succeed");
        let cleared_id = remote_disk.get_disk_id().await.expect("operation should succeed");
        assert!(cleared_id.is_none());
    }

    #[tokio::test]
    async fn test_remote_disk_ref_prefers_disk_id() {
        let url = url::Url::parse("http://remote-server:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        assert_eq!(remote_disk.disk_ref().await, endpoint.to_string());

        let disk_id = Uuid::new_v4();
        remote_disk
            .set_disk_id(Some(disk_id))
            .await
            .expect("operation should succeed");

        assert_eq!(remote_disk.disk_ref().await, disk_id.to_string());
    }

    #[tokio::test]
    async fn test_remote_disk_read_file_stream_uses_configured_data_transport() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, None::<&str>)], async {
            let transport = RecordingInternodeDataTransport::default();
            let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
            let expected_disk = remote_disk.disk_ref().await;

            let _reader = remote_disk
                .read_file_stream("bucket", "object/part.1", 7, 11)
                .await
                .expect("operation should succeed");

            let calls = transport.calls();
            assert_eq!(calls.len(), 1);
            match &calls[0] {
                RecordedTransportCall::Read(request) => {
                    assert_eq!(request.endpoint, "http://remote-node:9000");
                    assert_eq!(request.disk, expected_disk);
                    assert_eq!(request.volume, "bucket");
                    assert_eq!(request.path, "object/part.1");
                    assert_eq!(request.offset, 7);
                    assert_eq!(request.length, 11);
                    assert_eq!(request.stall_timeout, remote_read_timeouts(get_object_disk_read_timeout()).body_stall);
                }
                other => panic!("expected read transport call, got {other:?}"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_remote_disk_read_file_stream_disables_stall_timeout_when_configured_zero() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, Some("0"))], async {
            let transport = RecordingInternodeDataTransport::default();
            let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;

            let _reader = remote_disk
                .read_file_stream("bucket", "object/part.1", 7, 11)
                .await
                .expect("operation should succeed");

            let calls = transport.calls();
            assert_eq!(calls.len(), 1);
            match &calls[0] {
                RecordedTransportCall::Read(request) => assert_eq!(request.stall_timeout, None),
                other => panic!("expected read transport call, got {other:?}"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_remote_disk_create_and_append_file_use_configured_data_transport() {
        let transport = RecordingInternodeDataTransport::default();
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let expected_disk = remote_disk.disk_ref().await;

        let _created = remote_disk
            .create_file("orig-bucket", "bucket", "object/part.1", 4096)
            .await
            .expect("operation should succeed");
        let _appended = remote_disk
            .append_file("bucket", "object/part.2")
            .await
            .expect("operation should succeed");

        let calls = transport.calls();
        assert_eq!(calls.len(), 2);

        match &calls[0] {
            RecordedTransportCall::Write(request) => {
                assert_eq!(request.endpoint, "http://remote-node:9000");
                assert_eq!(request.disk, expected_disk);
                assert_eq!(request.volume, "bucket");
                assert_eq!(request.path, "object/part.1");
                assert!(!request.append);
                assert_eq!(request.size, 4096);
            }
            other => panic!("expected create write transport call, got {other:?}"),
        }

        match &calls[1] {
            RecordedTransportCall::Write(request) => {
                assert_eq!(request.endpoint, "http://remote-node:9000");
                assert_eq!(request.disk, expected_disk);
                assert_eq!(request.volume, "bucket");
                assert_eq!(request.path, "object/part.2");
                assert!(request.append);
                assert_eq!(request.size, 0);
            }
            other => panic!("expected append write transport call, got {other:?}"),
        }
    }

    #[tokio::test]
    #[serial(internode_metrics)]
    async fn test_remote_disk_create_file_retries_once_on_retryable_open_write_error() {
        let transport = RetryingOpenWriteInternodeDataTransport::with_steps(vec![
            OpenWriteTestStep::Error(DiskError::from(rustfs_rio::new_test_internode_http_io_error(
                rustfs_rio::InternodeHttpErrorKind::ConnectionReset,
            ))),
            OpenWriteTestStep::Success,
        ]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        crate::cluster::rpc::runtime_sources::reset_internode_metrics_for_test();

        let _created = remote_disk
            .create_file("orig-bucket", "bucket", "object/part.1", 4096)
            .await
            .expect("retryable open_write error should recover");

        let calls = transport.calls();
        assert_eq!(calls.len(), 2, "create_file should retry exactly once");
        let snapshot = crate::cluster::rpc::runtime_sources::internode_metrics_snapshot_for_test();
        assert_eq!(snapshot.outgoing_requests_total, 0);
    }

    #[tokio::test]
    #[serial(internode_metrics)]
    async fn test_remote_disk_create_file_retries_once_on_capability_probe_timeout() {
        let transport = RetryingOpenWriteInternodeDataTransport::with_steps(vec![
            OpenWriteTestStep::Error(DiskError::from(rustfs_rio::internode_http_timeout_error(
                &http::Method::GET,
                "http://remote-node:9000/rustfs/rpc/put_file_capability",
            ))),
            OpenWriteTestStep::Success,
        ]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        crate::cluster::rpc::runtime_sources::reset_internode_metrics_for_test();

        let _created = remote_disk
            .create_file("orig-bucket", "bucket", "object/part.1", 4096)
            .await
            .expect("capability probe timeout should recover on retry");

        assert_eq!(transport.calls().len(), 2, "create_file should retry capability probe timeouts once");
    }

    #[tokio::test]
    async fn test_remote_disk_append_file_does_not_retry_non_retryable_open_write_error() {
        let transport = RetryingOpenWriteInternodeDataTransport::with_steps(vec![OpenWriteTestStep::Error(DiskError::from(
            rustfs_rio::new_test_internode_http_io_error(rustfs_rio::InternodeHttpErrorKind::Unknown),
        ))]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;

        let err = match remote_disk.append_file("bucket", "object/part.2").await {
            Ok(_) => panic!("non-retryable open_write error should be returned directly"),
            Err(err) => err,
        };

        assert_eq!(err.internode_http_error_kind(), Some(rustfs_rio::InternodeHttpErrorKind::Unknown));
        assert_eq!(transport.calls().len(), 1, "append_file should not retry non-retryable errors");
    }

    #[tokio::test]
    #[serial(internode_metrics)]
    async fn test_remote_disk_read_file_stream_retries_once_on_retryable_open_read_error() {
        // A transient reset-by-peer on a shard read during the read-after-write window must be
        // absorbed by one re-dial rather than eroding read quorum (issue #2761).
        let transport = RetryingOpenReadInternodeDataTransport::with_steps(vec![
            OpenWriteTestStep::Error(DiskError::from(rustfs_rio::new_test_internode_http_io_error(
                rustfs_rio::InternodeHttpErrorKind::ConnectionReset,
            ))),
            OpenWriteTestStep::Success,
        ]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        crate::cluster::rpc::runtime_sources::reset_internode_metrics_for_test();

        let _reader = remote_disk
            .read_file_stream("bucket", "object/part.1", 0, 4096)
            .await
            .expect("retryable open_read error should recover");

        assert_eq!(transport.calls().len(), 2, "read_file_stream should retry exactly once");
    }

    #[tokio::test]
    async fn test_remote_disk_read_file_stream_does_not_retry_non_retryable_open_read_error() {
        let transport = RetryingOpenReadInternodeDataTransport::with_steps(vec![OpenWriteTestStep::Error(DiskError::from(
            rustfs_rio::new_test_internode_http_io_error(rustfs_rio::InternodeHttpErrorKind::Unknown),
        ))]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;

        let err = match remote_disk.read_file_stream("bucket", "object/part.2", 0, 4096).await {
            Ok(_) => panic!("non-retryable open_read error should be returned directly"),
            Err(err) => err,
        };

        assert_eq!(err.internode_http_error_kind(), Some(rustfs_rio::InternodeHttpErrorKind::Unknown));
        assert_eq!(transport.calls().len(), 1, "read_file_stream should not retry non-retryable errors");
    }

    #[tokio::test]
    async fn test_remote_disk_walk_dir_uses_configured_data_transport() {
        let transport = RecordingInternodeDataTransport::default();
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let expected_disk = remote_disk.disk_ref().await;
        let opts = WalkDirOptions {
            bucket: "bucket".to_string(),
            base_dir: "prefix".to_string(),
            recursive: true,
            report_notfound: false,
            filter_prefix: Some("part".to_string()),
            forward_to: None,
            limit: 10,
            disk_id: String::new(),
            ..Default::default()
        };
        let expected_body = serde_json::to_vec(&opts).expect("operation should succeed");
        let mut writer = Vec::new();

        remote_disk
            .walk_dir(opts, &mut writer)
            .await
            .expect("operation should succeed");

        let calls = transport.calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            RecordedTransportCall::WalkDir(request) => {
                assert_eq!(request.endpoint, "http://remote-node:9000");
                assert_eq!(request.disk, expected_disk);
                assert_eq!(request.body, expected_body);
                assert_eq!(request.stall_timeout, Some(get_drive_walkdir_stall_timeout()));
            }
            other => panic!("expected walk-dir transport call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_disk_namespace_scanner_uses_configured_data_transport() {
        let transport = RecordingInternodeDataTransport::default();
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let expected_disk = remote_disk.disk_ref().await;
        let expected_body = b"namespace-scanner-request".to_vec();
        let expected_request_id = Uuid::new_v4();
        let expected_server_epoch = Uuid::new_v4();
        let expected_session_id = Uuid::new_v4();

        let _reader = remote_disk
            .open_ns_scanner_stream(crate::disk::NsScannerOpenRequest {
                request_id: expected_request_id,
                server_epoch: expected_server_epoch,
                session_id: expected_session_id,
                session_sequence: 3,
                next_cycle: 7,
                leader_epoch: 9,
                body: expected_body.clone(),
                stall_timeout: Some(Duration::from_secs(15)),
            })
            .await
            .expect("namespace scanner stream should open");

        let calls = transport.calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            RecordedTransportCall::NsScanner(request) => {
                assert_eq!(request.endpoint, "http://remote-node:9000");
                assert_eq!(request.disk, expected_disk);
                assert_eq!(request.request_id, expected_request_id);
                assert_eq!(request.server_epoch, expected_server_epoch);
                assert_eq!(request.session_id, expected_session_id);
                assert_eq!(request.session_sequence, 3);
                assert_eq!(request.next_cycle, 7);
                assert_eq!(request.leader_epoch, 9);
                assert_eq!(request.body, expected_body);
                assert_eq!(request.stall_timeout, Some(Duration::from_secs(15)));
            }
            other => panic!("expected namespace scanner transport call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_disk_namespace_scanner_capability_uses_configured_data_transport() {
        let transport = RecordingInternodeDataTransport::default();
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;

        assert_eq!(
            remote_disk
                .ns_scanner_server_epoch()
                .await
                .expect("namespace scanner capability probe should succeed"),
            Some(Uuid::from_u128(1))
        );

        let calls = transport.calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            RecordedTransportCall::NsScannerProbe(request) => {
                assert_eq!(request.endpoint, "http://remote-node:9000");
            }
            other => panic!("expected namespace scanner capability probe, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_disk_namespace_scanner_capability_rejects_old_and_incompatible_peers() {
        for status in [404, 405, 426] {
            let transport = RecordingInternodeDataTransport::with_ns_scanner_probe_status(status);
            let remote_disk = new_remote_disk_with_transport(Arc::new(transport)).await;

            assert_eq!(
                remote_disk
                    .ns_scanner_server_epoch()
                    .await
                    .expect("unsupported namespace scanner response should be classified"),
                None
            );
        }
    }

    #[tokio::test]
    async fn test_remote_disk_namespace_scanner_capability_rejects_legacy_transport() {
        let remote_disk = new_remote_disk_with_transport(Arc::new(RetryingOpenReadInternodeDataTransport::default())).await;

        assert_eq!(
            remote_disk
                .ns_scanner_server_epoch()
                .await
                .expect("legacy transport should be classified as unsupported"),
            None
        );
    }

    #[tokio::test]
    async fn test_remote_disk_namespace_scanner_capability_reprobes_after_peer_upgrade() {
        let transport = RecordingInternodeDataTransport::with_ns_scanner_probe_status(404);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;

        assert_eq!(
            remote_disk
                .ns_scanner_server_epoch()
                .await
                .expect("old peer should be classified as unsupported"),
            None
        );
        transport.set_ns_scanner_probe_status(None);
        assert_eq!(
            remote_disk
                .ns_scanner_server_epoch()
                .await
                .expect("upgraded peer should be re-probed"),
            Some(Uuid::from_u128(1))
        );
        assert_eq!(transport.calls().len(), 2);
    }

    #[tokio::test]
    async fn test_remote_disk_namespace_scanner_capability_propagates_transient_failure() {
        let transport = RecordingInternodeDataTransport::with_ns_scanner_probe_status(503);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport)).await;

        let err = remote_disk
            .ns_scanner_server_epoch()
            .await
            .expect_err("transient capability failure must not be reported as unsupported");
        assert!(matches!(
            err.internode_http_error_kind(),
            Some(rustfs_rio::InternodeHttpErrorKind::HttpStatus(status)) if status.as_u16() == 503
        ));
    }

    #[tokio::test]
    async fn test_remote_disk_walk_dir_preserves_skip_total_timeout_option() {
        let transport = RecordingInternodeDataTransport::default();
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let opts = WalkDirOptions {
            bucket: "bucket".to_string(),
            base_dir: "prefix".to_string(),
            recursive: true,
            skip_total_timeout: true,
            ..Default::default()
        };
        let mut writer = Vec::new();

        remote_disk
            .walk_dir(opts, &mut writer)
            .await
            .expect("walk_dir should be sent through configured data transport");

        let calls = transport.calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            RecordedTransportCall::WalkDir(request) => {
                let sent_opts: WalkDirOptions =
                    serde_json::from_slice(&request.body).expect("walk_dir request body should deserialize");
                assert!(sent_opts.skip_total_timeout);
                assert_eq!(request.stall_timeout, Some(get_drive_walkdir_stall_timeout()));
            }
            other => panic!("expected walk-dir transport call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_disk_walk_dir_uses_per_request_stall_timeout() {
        let transport = RecordingInternodeDataTransport::default();
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let opts = WalkDirOptions {
            bucket: "bucket".to_string(),
            base_dir: "prefix".to_string(),
            recursive: true,
            stall_timeout_ms: Some(60_000),
            ..Default::default()
        };
        let mut writer = Vec::new();

        remote_disk
            .walk_dir(opts, &mut writer)
            .await
            .expect("walk_dir should be sent through configured data transport");

        let calls = transport.calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            RecordedTransportCall::WalkDir(request) => {
                assert_eq!(request.stall_timeout, Some(Duration::from_secs(60)));
            }
            other => panic!("expected walk-dir transport call, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_remote_disk_walk_dir_retries_once_on_retryable_transport_error() {
        let transport = RetryingWalkDirInternodeDataTransport::with_steps(vec![
            WalkDirTestStep::Error(DiskError::other("HttpReader stream error: error decoding response body")),
            WalkDirTestStep::Data(b"walk-dir-retry-ok".to_vec()),
        ]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let opts = WalkDirOptions {
            bucket: "bucket".to_string(),
            base_dir: "config/iam".to_string(),
            recursive: true,
            report_notfound: false,
            filter_prefix: None,
            forward_to: None,
            limit: 10,
            disk_id: String::new(),
            ..Default::default()
        };
        let mut writer = Vec::new();

        remote_disk
            .walk_dir(opts, &mut writer)
            .await
            .expect("retryable walk_dir error should recover");

        assert_eq!(writer, b"walk-dir-retry-ok");
        assert_eq!(transport.calls().len(), 2, "walk_dir should retry exactly once");
    }

    #[tokio::test]
    async fn test_remote_disk_walk_dir_does_not_retry_after_partial_stream_failure() {
        let transport = RetryingWalkDirInternodeDataTransport::with_steps(vec![
            WalkDirTestStep::PartialDataThenError {
                data: b"partial-walk-dir".to_vec(),
                error: io::Error::new(io::ErrorKind::ConnectionReset, "connection reset"),
            },
            WalkDirTestStep::Data(b"walk-dir-retry-ok".to_vec()),
        ]);
        let remote_disk = new_remote_disk_with_transport(Arc::new(transport.clone())).await;
        let opts = WalkDirOptions {
            bucket: "bucket".to_string(),
            base_dir: "config/iam".to_string(),
            recursive: true,
            report_notfound: false,
            filter_prefix: None,
            forward_to: None,
            limit: 10,
            disk_id: String::new(),
            ..Default::default()
        };
        let mut writer = Vec::new();

        let err = remote_disk
            .walk_dir(opts, &mut writer)
            .await
            .expect_err("partial stream failure should be returned without retry");

        assert!(matches!(err, DiskError::Io(ref io_err) if io_err.kind() == io::ErrorKind::ConnectionReset));
        assert_eq!(writer, b"partial-walk-dir");
        assert_eq!(transport.calls().len(), 1, "walk_dir should not retry after writing partial bytes");
    }

    #[tokio::test]
    async fn test_remote_disk_endpoints_with_different_schemes() {
        let test_cases = vec![
            ("http://server:9000", "server:9000"),
            ("http://plain-server:80", "plain-server"),
            ("http://plain-server", "plain-server"),
            ("https://secure-server:443", "secure-server"),
            ("http://192.168.1.100:8080", "192.168.1.100:8080"),
            ("https://secure-server", "secure-server"),
        ];

        for (url_str, expected_hostname) in test_cases {
            let url = url::Url::parse(url_str).expect("operation should succeed");
            let endpoint = Endpoint {
                url: url.clone(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 0,
            };

            let disk_option = DiskOption {
                cleanup: false,
                health_check: false,
            };

            let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
                .await
                .expect("operation should succeed");

            assert!(!remote_disk.is_local());
            assert_eq!(remote_disk.host_name(), expected_hostname);
            // Note: to_string() might not contain the exact hostname format
            assert!(!remote_disk.to_string().is_empty());
        }
    }

    #[tokio::test]
    async fn test_remote_disk_location_validation() {
        // Test valid location
        let url = url::Url::parse("http://server:9000").expect("operation should succeed");
        let valid_endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 1,
            disk_idx: 2,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&valid_endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        let location = remote_disk.get_disk_location();
        assert!(location.valid());
        assert_eq!(location.pool_idx, Some(0));
        assert_eq!(location.set_idx, Some(1));
        assert_eq!(location.disk_idx, Some(2));

        // Test invalid location (negative indices)
        let invalid_endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: -1,
            set_idx: -1,
            disk_idx: -1,
        };

        let remote_disk_invalid = RemoteDisk::new(&invalid_endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");
        let invalid_location = remote_disk_invalid.get_disk_location();
        assert!(!invalid_location.valid());
        assert_eq!(invalid_location.pool_idx, None);
        assert_eq!(invalid_location.set_idx, None);
        assert_eq!(invalid_location.disk_idx, None);
    }

    #[tokio::test]
    async fn test_remote_disk_close() {
        let url = url::Url::parse("http://server:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option, Arc::new(TcpHttpInternodeDataTransport))
            .await
            .expect("operation should succeed");

        // Test close operation (should succeed)
        let result = remote_disk.close().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_with_timeout_marks_remote_disk_faulty() {
        let url = url::Url::parse("http://remote-timeout:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let err = remote_disk
            .execute_with_timeout(
                || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<(), Error>(())
                },
                Duration::from_millis(10),
            )
            .await
            .expect_err("timeout should fail");

        assert!(err.to_string().contains("timeout"));
        assert!(remote_disk.is_online().await, "first timeout should keep the remote disk online");
        assert_eq!(
            remote_disk.runtime_state(),
            RuntimeDriveHealthState::Suspect,
            "first timeout should move the remote disk into suspect state"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_can_ignore_remote_timeout_failure() {
        let url = url::Url::parse("http://remote-timeout-ignored:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let err = remote_disk
            .execute_with_timeout_for_op_and_health_action(
                "walk_dir",
                || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<(), Error>(())
                },
                Duration::from_millis(10),
                FailureHealthAction::IgnoreFailure,
            )
            .await
            .expect_err("timeout should fail");

        assert!(err.to_string().contains("timeout"));
        assert!(remote_disk.is_online().await, "ignored timeout should not mark remote disk faulty");
    }

    #[tokio::test]
    async fn test_execute_with_timeout_zero_duration_waits_for_operation() {
        let url = url::Url::parse("http://remote-no-timeout:9000").expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        remote_disk
            .execute_with_timeout(
                || async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok::<(), Error>(())
                },
                Duration::ZERO,
            )
            .await
            .expect("zero duration should disable the operation timeout");

        assert!(
            remote_disk.is_online().await,
            "successful no-timeout operation should keep remote disk online"
        );
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_reset_during_backoff_preserves_recovery() {
        let remote_disk = Arc::new(new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await);
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let first_attempt = Arc::new(tokio::sync::Notify::new());
        let started = time::Instant::now();

        let task_disk = Arc::clone(&remote_disk);
        let task_attempts = Arc::clone(&attempts);
        let task_first_attempt = Arc::clone(&first_attempt);
        let task = tokio::spawn(async move {
            task_disk
                .execute_read_with_retry(
                    "read_version",
                    move || {
                        let attempt = task_attempts.fetch_add(1, Ordering::SeqCst);
                        let first_attempt = Arc::clone(&task_first_attempt);
                        async move {
                            if attempt == 0 {
                                time::sleep(Duration::from_millis(20)).await;
                                first_attempt.notify_one();
                                return Err::<(), Error>(DiskError::Io(std_io::Error::new(
                                    std_io::ErrorKind::ConnectionRefused,
                                    "connection refused",
                                )));
                            }
                            Ok(())
                        }
                    },
                    Duration::from_millis(100),
                )
                .await
        });

        first_attempt.notified().await;
        tokio::task::yield_now().await;
        remote_disk.health.reset_for_store_init_retry(&remote_disk.endpoint);
        let channel = TonicEndpoint::from_shared(remote_disk.addr.clone())
            .expect("remote disk address should parse")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(remote_disk.addr.clone(), channel).await;
        task.await
            .expect("retry task should finish")
            .expect("the retry should succeed after the health reset");

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(started.elapsed(), Duration::from_millis(70));
        assert_eq!(
            remote_disk.health.waiting_count(),
            0,
            "health reset must not underflow the waiting counter"
        );
        assert_eq!(remote_disk.runtime_state(), RuntimeDriveHealthState::Online);
        assert!(
            runtime_sources::test_node_channel_is_cached(&remote_disk.addr).await,
            "a recovered channel must survive the retry backoff"
        );
        remote_disk.cancel_token.cancel();
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_still_retries_within_shared_deadline() {
        let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let channel = TonicEndpoint::from_shared(remote_disk.addr.clone())
            .expect("remote disk address should parse")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(remote_disk.addr.clone(), channel).await;

        remote_disk
            .execute_read_with_retry(
                "read_version",
                || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            return Err::<(), Error>(DiskError::Io(std_io::Error::new(
                                std_io::ErrorKind::ConnectionReset,
                                "connection reset",
                            )));
                        }
                        Ok(())
                    }
                },
                Duration::from_millis(100),
            )
            .await
            .expect("a retry that fits the shared deadline should succeed");

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(remote_disk.runtime_state(), RuntimeDriveHealthState::Online);
        assert!(runtime_sources::test_node_channel_is_cached(&remote_disk.addr).await);
        remote_disk.cancel_token.cancel();
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_uses_remaining_budget_for_final_attempt() {
        let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = time::Instant::now();
        let channel = TonicEndpoint::from_shared(remote_disk.addr.clone())
            .expect("remote disk address should parse")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(remote_disk.addr.clone(), channel).await;

        let err = remote_disk
            .execute_read_with_retry(
                "read_version",
                || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            time::sleep(Duration::from_millis(20)).await;
                            return Err::<(), Error>(DiskError::Io(std_io::Error::new(
                                std_io::ErrorKind::ConnectionRefused,
                                "connection refused",
                            )));
                        }
                        std::future::pending::<Result<()>>().await
                    }
                },
                Duration::from_millis(100),
            )
            .await
            .expect_err("the final retry should consume only the remaining total budget");

        assert_eq!(err, DiskError::Timeout);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(started.elapsed(), Duration::from_millis(100));
        assert_eq!(remote_disk.runtime_state(), RuntimeDriveHealthState::Suspect);
        assert!(!runtime_sources::test_node_channel_is_cached(&remote_disk.addr).await);
        remote_disk.cancel_token.cancel();
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_uses_final_attempt_at_exact_backoff_boundary() {
        let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = time::Instant::now();

        let err = remote_disk
            .execute_read_with_retry(
                "read_version",
                || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            time::sleep(Duration::from_millis(50)).await;
                            return Err::<(), Error>(DiskError::Io(std_io::Error::new(
                                std_io::ErrorKind::ConnectionRefused,
                                "connection refused",
                            )));
                        }
                        std::future::pending::<Result<()>>().await
                    }
                },
                Duration::from_millis(100),
            )
            .await
            .expect_err("the exact backoff boundary should be reserved for a final attempt");

        assert_eq!(err, DiskError::Timeout);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(started.elapsed(), Duration::from_millis(100));
        assert_eq!(remote_disk.runtime_state(), RuntimeDriveHealthState::Suspect);
        remote_disk.cancel_token.cancel();
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_uses_final_attempt_below_backoff_budget() {
        let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = time::Instant::now();

        let err = remote_disk
            .execute_read_with_retry(
                "read_version",
                || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            time::sleep(Duration::from_millis(80)).await;
                            return Err::<(), Error>(DiskError::Io(std_io::Error::new(
                                std_io::ErrorKind::ConnectionRefused,
                                "connection refused",
                            )));
                        }
                        std::future::pending::<Result<()>>().await
                    }
                },
                Duration::from_millis(100),
            )
            .await
            .expect_err("remaining budget below backoff should be reserved for a final attempt");

        assert_eq!(err, DiskError::Timeout);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(started.elapsed(), Duration::from_millis(100));
        assert_eq!(remote_disk.runtime_state(), RuntimeDriveHealthState::Suspect);
        remote_disk.cancel_token.cancel();
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_zero_timeout_disables_the_deadline() {
        let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = time::Instant::now();

        remote_disk
            .execute_read_with_retry(
                "read_version",
                || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            return Err::<(), Error>(DiskError::Io(std_io::Error::new(
                                std_io::ErrorKind::ConnectionReset,
                                "connection reset",
                            )));
                        }
                        Ok(())
                    }
                },
                Duration::ZERO,
            )
            .await
            .expect("zero timeout should allow a retry without a deadline");

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(started.elapsed(), REMOTE_DISK_READ_RETRY_BASE_BACKOFF);
        remote_disk.cancel_token.cancel();
    }

    #[tokio::test]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_accepts_max_metadata_timeout() {
        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, Some(u64::MAX.to_string()))], async {
            let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;

            remote_disk
                .execute_read_with_retry("read_version", || async { Ok::<(), Error>(()) }, get_drive_metadata_timeout())
                .await
                .expect("the maximum configured metadata timeout must not panic");

            remote_disk.cancel_token.cancel();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_zero_retries_runs_once() {
        temp_env::async_with_vars([(rustfs_config::ENV_INTERNODE_IDEMPOTENT_READ_RETRIES, Some("0"))], async {
            let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;
            let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let started = time::Instant::now();
            let channel = TonicEndpoint::from_shared(remote_disk.addr.clone())
                .expect("remote disk address should parse")
                .connect_lazy();
            runtime_sources::cache_test_node_channel(remote_disk.addr.clone(), channel).await;

            let err = remote_disk
                .execute_read_with_retry(
                    "read_version",
                    || {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        async {
                            Err::<(), Error>(DiskError::Io(std_io::Error::new(
                                std_io::ErrorKind::ConnectionReset,
                                "connection reset",
                            )))
                        }
                    },
                    Duration::from_secs(1),
                )
                .await
                .expect_err("zero retries should return the first network error");

            assert!(matches!(err, DiskError::Io(ref io_err) if io_err.kind() == std_io::ErrorKind::ConnectionReset));
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert_eq!(started.elapsed(), Duration::ZERO);
            assert_eq!(remote_disk.runtime_state(), RuntimeDriveHealthState::Suspect);
            assert!(!runtime_sources::test_node_channel_is_cached(&remote_disk.addr).await);
            remote_disk.cancel_token.cancel();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_attempt_timeout_marks_health_without_evicting() {
        let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;
        let recorder = crate::test_metrics::CapturingRecorder::default();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let channel = TonicEndpoint::from_shared(remote_disk.addr.clone())
            .expect("remote disk address should parse")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(remote_disk.addr.clone(), channel).await;

        let err = remote_disk
            .execute_read_with_retry(
                "read_version",
                || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<Result<()>>()
                },
                Duration::from_millis(100),
            )
            .await
            .expect_err("an in-flight attempt that consumes the deadline should time out");

        assert_eq!(err, DiskError::Timeout);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(remote_disk.runtime_state(), RuntimeDriveHealthState::Suspect);
        assert!(runtime_sources::test_node_channel_is_cached(&remote_disk.addr).await);
        assert_eq!(
            recorder.counter_value(
                "rustfs_drive_op_timeout_total",
                &[
                    ("endpoint", remote_disk.endpoint.to_string().as_str()),
                    ("op", "read_version")
                ]
            ),
            1
        );
        remote_disk.cancel_token.cancel();
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_does_not_retry_business_errors() {
        let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let err = remote_disk
            .execute_read_with_retry(
                "read_version",
                || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Err::<(), Error>(DiskError::FileNotFound) }
                },
                Duration::from_secs(1),
            )
            .await
            .expect_err("business errors should be returned directly");

        assert_eq!(err, DiskError::FileNotFound);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        remote_disk.cancel_token.cancel();
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_honors_configured_retry_count() {
        temp_env::async_with_vars([(rustfs_config::ENV_INTERNODE_IDEMPOTENT_READ_RETRIES, Some("2"))], async {
            let remote_disk = new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await;
            let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let started = time::Instant::now();
            let channel = TonicEndpoint::from_shared(remote_disk.addr.clone())
                .expect("remote disk address should parse")
                .connect_lazy();
            runtime_sources::cache_test_node_channel(remote_disk.addr.clone(), channel).await;

            let err = remote_disk
                .execute_read_with_retry(
                    "read_version",
                    || {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        async {
                            Err::<(), Error>(DiskError::Io(std_io::Error::new(
                                std_io::ErrorKind::ConnectionReset,
                                "connection reset",
                            )))
                        }
                    },
                    Duration::from_secs(1),
                )
                .await
                .expect_err("exhausted retries should return the last network error");

            assert!(matches!(err, DiskError::Io(ref io_err) if io_err.kind() == std_io::ErrorKind::ConnectionReset));
            assert_eq!(attempts.load(Ordering::SeqCst), 3);
            assert_eq!(started.elapsed(), Duration::from_millis(150));
            assert_eq!(remote_disk.runtime_state(), RuntimeDriveHealthState::Suspect);
            assert!(!runtime_sources::test_node_channel_is_cached(&remote_disk.addr).await);
            remote_disk.cancel_token.cancel();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    #[serial(remote_disk_read_retry)]
    async fn execute_read_with_retry_stops_when_disk_turns_offline_during_backoff() {
        let remote_disk = Arc::new(new_remote_disk_with_transport(Arc::new(RecordingInternodeDataTransport::default())).await);
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let first_attempt = Arc::new(tokio::sync::Notify::new());
        let task_disk = Arc::clone(&remote_disk);
        let task_attempts = Arc::clone(&attempts);
        let task_first_attempt = Arc::clone(&first_attempt);

        let task = tokio::spawn(async move {
            task_disk
                .execute_read_with_retry(
                    "read_version",
                    move || {
                        let attempt = task_attempts.fetch_add(1, Ordering::SeqCst);
                        let first_attempt = Arc::clone(&task_first_attempt);
                        async move {
                            if attempt == 0 {
                                first_attempt.notify_one();
                                return Err::<(), Error>(DiskError::Io(std_io::Error::new(
                                    std_io::ErrorKind::ConnectionReset,
                                    "connection reset",
                                )));
                            }
                            Ok(())
                        }
                    },
                    Duration::from_secs(1),
                )
                .await
        });

        first_attempt.notified().await;
        tokio::task::yield_now().await;
        remote_disk
            .health
            .force_runtime_state_for_test(RuntimeDriveHealthState::Offline);
        time::advance(REMOTE_DISK_READ_RETRY_BASE_BACKOFF).await;
        let err = task
            .await
            .expect("retry task should finish")
            .expect_err("an offline disk must stop before the next attempt");

        assert_eq!(err, DiskError::FaultyDisk);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        remote_disk.cancel_token.cancel();
    }

    #[tokio::test]
    async fn test_execute_with_timeout_evicts_cached_connection() {
        let addr = "http://127.0.0.1:59991".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;
        assert!(runtime_sources::test_node_channel_is_cached(&addr).await);

        let _ = remote_disk
            .execute_with_timeout(
                || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<(), Error>(())
                },
                Duration::from_millis(10),
            )
            .await
            .expect_err("timeout should fail");

        assert!(
            !runtime_sources::test_node_channel_is_cached(&addr).await,
            "timeout should evict cached connection"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_marks_faulty_on_timeout_like_error() {
        let addr = "http://127.0.0.1:59992".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;

        let err = remote_disk
            .execute_with_timeout(
                || async { Err::<(), Error>(DiskError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "stall timeout"))) },
                Duration::from_secs(1),
            )
            .await
            .expect_err("timeout-like operation error should fail");

        assert_eq!(
            match &err {
                DiskError::Io(io_err) => io_err.kind(),
                other => panic!("expected io timeout error, got {other:?}"),
            },
            std::io::ErrorKind::TimedOut
        );
        assert!(
            remote_disk.is_online().await,
            "first timeout-like error should keep the remote disk online"
        );
        assert_eq!(
            remote_disk.runtime_state(),
            RuntimeDriveHealthState::Suspect,
            "first timeout-like error should move the remote disk into suspect state"
        );
        assert!(
            !runtime_sources::test_node_channel_is_cached(&addr).await,
            "timeout-like errors should evict cached connection"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_marks_faulty_on_network_like_error() {
        let addr = "http://127.0.0.1:59993".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;

        let err = remote_disk
            .execute_with_timeout(
                || async {
                    Err::<(), Error>(DiskError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "connection refused",
                    )))
                },
                Duration::from_secs(1),
            )
            .await
            .expect_err("network-like operation error should fail");

        assert_eq!(
            match &err {
                DiskError::Io(io_err) => io_err.kind(),
                other => panic!("expected io network error, got {other:?}"),
            },
            std::io::ErrorKind::ConnectionRefused
        );
        assert!(
            remote_disk.is_online().await,
            "first network-like error should keep the remote disk online"
        );
        assert_eq!(
            remote_disk.runtime_state(),
            RuntimeDriveHealthState::Suspect,
            "first network-like error should move the remote disk into suspect state"
        );
        assert!(
            !runtime_sources::test_node_channel_is_cached(&addr).await,
            "network-like errors should evict cached connection"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_can_ignore_network_like_error() {
        let addr = "http://127.0.0.1:59995".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;

        let err = remote_disk
            .execute_with_timeout_for_op_and_health_action(
                "walk_dir",
                || async { Err::<(), Error>(DiskError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "stall timeout"))) },
                Duration::from_secs(1),
                FailureHealthAction::IgnoreFailure,
            )
            .await
            .expect_err("timeout-like operation error should fail");

        assert_eq!(
            match &err {
                DiskError::Io(io_err) => io_err.kind(),
                other => panic!("expected io timeout error, got {other:?}"),
            },
            std::io::ErrorKind::TimedOut
        );
        assert!(
            remote_disk.is_online().await,
            "ignored network-like error should not mark remote disk faulty"
        );
        assert!(
            runtime_sources::test_node_channel_is_cached(&addr).await,
            "ignored network-like error should not evict cached connection"
        );
    }

    #[tokio::test]
    async fn test_execute_with_timeout_keeps_remote_disk_online_for_business_error() {
        let addr = "http://127.0.0.1:59994".to_string();
        let url = url::Url::parse(&format!("{addr}/data")).expect("operation should succeed");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("operation should succeed");

        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("operation should succeed")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;

        let err = remote_disk
            .execute_with_timeout(|| async { Err::<(), Error>(DiskError::FileNotFound) }, Duration::from_secs(1))
            .await
            .expect_err("business error should still fail the operation");

        assert_eq!(err, DiskError::FileNotFound);
        assert!(remote_disk.is_online().await, "business errors should not mark remote disk faulty");
        assert!(
            runtime_sources::test_node_channel_is_cached(&addr).await,
            "business errors should not evict cached connection"
        );
    }

    #[test]
    fn test_remote_disk_sync_properties() {
        let url = url::Url::parse("https://secure-remote:9000/data").expect("operation should succeed");
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 1,
            set_idx: 2,
            disk_idx: 3,
        };

        // Test endpoint method - we can't test this without creating RemoteDisk instance
        // but we can test that the endpoint contains expected values
        assert_eq!(endpoint.url, url);
        assert!(!endpoint.is_local);
        assert_eq!(endpoint.pool_idx, 1);
        assert_eq!(endpoint.set_idx, 2);
        assert_eq!(endpoint.disk_idx, 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remote_disk_recovery_probe_logs_keep_request_id_span_context() {
        let logs = CapturedLogs::default();
        let subscriber = Registry::default().with(
            tracing_subscriber::fmt::layer()
                .with_writer(logs.clone())
                .with_ansi(false)
                .without_time()
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(true),
        );
        let _guard = tracing::subscriber::set_default(subscriber);
        // The `recovery-monitor` span and the monitor's own log events are
        // production callsites that sibling tests exercise from subscriber-less
        // threads; without this they can be cached as `Interest::never()` and go
        // silently missing here.
        let _callsite_pin = crate::test_tracing::pin_callsite_interest_for_test();

        let endpoint = Endpoint {
            url: url::Url::parse("http://127.0.0.1:59996/data").expect("endpoint URL should parse"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: true,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("remote disk should construct");
        let replacement = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: true,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("replacement remote disk should construct");
        assert_ne!(
            remote_disk.handle_id, replacement.handle_id,
            "replacement handles need distinct log identities"
        );

        let span = tracing::info_span!("request-span", request_id = "req-remote-disk");
        let _entered = span.enter();
        let done = remote_disk.spawn_recovery_monitor_log_probe_for_test();
        done.await
            .expect("remote disk recovery monitor probe should signal completion");

        let log = logs
            .lines()
            .into_iter()
            .find(|value| value.get("message").and_then(Value::as_str) == Some("remote disk recovery monitor log probe"))
            .expect("expected remote disk recovery monitor probe log");

        assert_eq!(log["span"]["name"], Value::String("recovery-monitor".to_string()));
        assert_eq!(log["span"]["kind"], Value::String("remote_disk".to_string()));
        assert_eq!(log["span"]["handle_id"], Value::String(remote_disk.handle_id.to_string()));
        let spans = log["spans"].as_array().expect("spans should be present");
        assert!(spans.iter().any(|span| {
            span.get("name").and_then(Value::as_str) == Some("request-span")
                && span.get("request_id").and_then(Value::as_str) == Some("req-remote-disk")
        }));

        remote_disk.force_runtime_state_for_test(RuntimeDriveHealthState::Offline);
        remote_disk
            .execute_with_timeout(|| async { Ok::<(), Error>(()) }, Duration::from_secs(1))
            .await
            .expect_err("faulty handle should short-circuit");
        let faulty_log = logs
            .lines()
            .into_iter()
            .find(|value| value.get("state").and_then(Value::as_str) == Some("faulty_short_circuit"))
            .expect("expected faulty short-circuit log");
        assert_eq!(faulty_log["handle_id"], Value::String(remote_disk.handle_id.to_string()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remote_disk_network_error_starts_recovery_monitor_with_request_context() {
        let logs = CapturedLogs::default();
        let subscriber = Registry::default().with(
            tracing_subscriber::fmt::layer()
                .with_writer(logs.clone())
                .with_ansi(false)
                .without_time()
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(true),
        );
        let _guard = tracing::subscriber::set_default(subscriber);
        // The `recovery-monitor` span and the monitor's own log events are
        // production callsites that sibling tests exercise from subscriber-less
        // threads; without this they can be cached as `Interest::never()` and go
        // silently missing here.
        let _callsite_pin = crate::test_tracing::pin_callsite_interest_for_test();

        let addr = "http://127.0.0.1:59997".to_string();
        let endpoint = Endpoint {
            url: url::Url::parse(&format!("{addr}/data")).expect("endpoint URL should parse"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let remote_disk = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: true,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("remote disk should construct");

        let span = tracing::info_span!("request-span", request_id = "req-remote-disk-e2e");
        let _entered = span.enter();

        let err = remote_disk
            .execute_with_timeout(
                || async {
                    Err::<(), Error>(DiskError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "connection refused",
                    )))
                },
                Duration::from_secs(1),
            )
            .await
            .expect_err("network-like operation error should fail");
        assert_eq!(
            match &err {
                DiskError::Io(io_err) => io_err.kind(),
                other => panic!("expected io network error, got {other:?}"),
            },
            std::io::ErrorKind::ConnectionRefused
        );

        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        remote_disk.cancel_token.cancel();
        tokio::task::yield_now().await;

        let lines = logs.lines();
        let marked_suspect = lines
            .iter()
            .find(|value| value.get("state").and_then(Value::as_str) == Some("marked_suspect"))
            .expect("expected marked_suspect log");
        assert!(
            marked_suspect["spans"]
                .as_array()
                .expect("spans should be present")
                .iter()
                .any(|span| {
                    span.get("name").and_then(Value::as_str) == Some("request-span")
                        && span.get("request_id").and_then(Value::as_str) == Some("req-remote-disk-e2e")
                })
        );

        let recovery_started = lines
            .iter()
            .find(|value| value.get("state").and_then(Value::as_str) == Some("recovery_monitor_started"))
            .expect("expected recovery_monitor_started log");
        assert_eq!(recovery_started["span"]["name"], Value::String("recovery-monitor".to_string()));
        assert_eq!(recovery_started["span"]["kind"], Value::String("remote_disk".to_string()));
        assert!(
            recovery_started["spans"]
                .as_array()
                .expect("spans should be present")
                .iter()
                .any(|span| {
                    span.get("name").and_then(Value::as_str) == Some("request-span")
                        && span.get("request_id").and_then(Value::as_str) == Some("req-remote-disk-e2e")
                })
        );
    }

    /// Peer that completes the TCP connect and then goes silent, so every RPC issued over the
    /// cached lazy channel stays pending until the caller's own deadline fires.
    async fn spawn_stalled_grpc_peer() -> Option<(String, tokio::task::JoinHandle<()>)> {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");
        let accept_task = tokio::spawn(async move {
            let mut accepted = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                accepted.push(stream);
            }
        });

        let base_addr = format!("http://{}:{}", addr.ip(), addr.port());
        let channel = TonicEndpoint::from_shared(base_addr.clone())
            .expect("stalled peer endpoint should parse")
            .connect_lazy();
        runtime_sources::cache_test_node_channel(base_addr.clone(), channel).await;
        Some((base_addr, accept_task))
    }

    async fn remote_disk_for_addr(base_addr: &str) -> RemoteDisk {
        let url = url::Url::parse(&format!("{base_addr}/data/rustfs0")).expect("endpoint url should parse");
        let endpoint = Endpoint {
            url,
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(TcpHttpInternodeDataTransport),
        )
        .await
        .expect("remote disk should construct")
    }

    #[tokio::test]
    async fn list_volumes_bounds_the_wait_on_a_stalled_peer() {
        runtime_sources::ensure_test_rpc_secret();
        let Some((base_addr, accept_task)) = spawn_stalled_grpc_peer().await else {
            return;
        };
        let remote_disk = remote_disk_for_addr(&base_addr).await;

        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("1"))], async {
            let err = tokio::time::timeout(Duration::from_secs(10), remote_disk.list_volumes())
                .await
                .expect("list_volumes must bound the wait on a stalled peer")
                .expect_err("a stalled peer must fail list_volumes");
            assert!(matches!(err, DiskError::Timeout), "expected the operation deadline to fire, got {err:?}");
        })
        .await;

        accept_task.abort();
    }

    #[tokio::test]
    #[serial]
    async fn read_version_uses_the_metadata_timeout_on_a_stalled_peer() {
        runtime_sources::ensure_test_rpc_secret();
        let Some((base_addr, accept_task)) = spawn_stalled_grpc_peer().await else {
            return;
        };
        let remote_disk = remote_disk_for_addr(&base_addr).await;
        let metrics = rustfs_io_metrics::internode_metrics::global_internode_metrics();
        let previous_stage_metrics = rustfs_io_metrics::get_stage_metrics_enabled();
        metrics.reset_for_test();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_DRIVE_METADATA_TIMEOUT_SECS, Some("1")),
                (rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("10")),
            ],
            async {
                let started = time::Instant::now();
                let err = tokio::time::timeout(
                    Duration::from_secs(5),
                    remote_disk.read_version("bucket", "bucket", "object", "", &ReadOptions::default()),
                )
                .await
                .expect("read_version must use the shorter metadata deadline")
                .expect_err("a stalled peer must fail read_version");

                assert!(matches!(err, DiskError::Timeout), "expected the metadata deadline to fire, got {err:?}");
                assert!(started.elapsed() >= Duration::from_millis(900));
                assert!(started.elapsed() < Duration::from_secs(2));
            },
        )
        .await;

        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_stage_metrics);
        let snapshot = metrics.snapshot();
        assert!(
            snapshot.outgoing_requests_total >= 1,
            "ReadVersion call site should record outgoing attempts when attribution is enabled"
        );
        assert!(
            snapshot.sent_bytes_total > 0,
            "ReadVersion call site should record request payload bytes when attribution is enabled"
        );
        metrics.reset_for_test();

        remote_disk.cancel_token.cancel();
        accept_task.abort();
    }

    #[tokio::test]
    async fn delete_volume_bounds_the_wait_on_a_stalled_peer() {
        runtime_sources::ensure_test_rpc_secret();
        let Some((base_addr, accept_task)) = spawn_stalled_grpc_peer().await else {
            return;
        };
        let remote_disk = remote_disk_for_addr(&base_addr).await;

        temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("1"))], async {
            let err = tokio::time::timeout(Duration::from_secs(10), remote_disk.delete_volume("bucket", false))
                .await
                .expect("delete_volume must bound the wait on a stalled peer")
                .expect_err("a stalled peer must fail delete_volume");
            assert!(matches!(err, DiskError::Timeout), "expected the operation deadline to fire, got {err:?}");
        })
        .await;

        accept_task.abort();
    }
}
