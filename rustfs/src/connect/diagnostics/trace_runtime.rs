// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Owner-only local transport between diagnostic CLI commands and the running server.
//!
//! Version 1 accepts TRACE_RECORD, RUNTIME_PROFILE, NATIVE_THREADS_PROFILE, TOP_API, TOP_DISK, TOP_LOCKS, TOP_RPC, NETWORK_PERFORMANCE and HEALTH. Signed requests select
//! an existing offline key by SPKI digest; this is not proof of Connect enrollment.
//! The receiver checks enrollment, target ownership and consent at import. The
//! server owns provenance, nonce generation, capture and signing; the CLI receives
//! only a bounded archive and saves it without uploading. No paths cross this IPC.
//! The write half stays open during collection. EOF cancels the server collector;
//! cancellation is acknowledged only after joining it and releasing its lease.

use std::io;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::profile_cpu::{
    LocalProfileConsent, LocalRuntimeProfileRequest, MAX_ARCHIVE_BYTES, MAX_PROFILE_DURATION, ProfileCaptureRequest,
    ProfileError, ProfileOutcome, ProfileReasonCode, ProfileTool, SignedProfileExport, encode_signed_profile_export,
};
use base64_simd::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use super::{LocalTelemetryConsent, TelemetryProducerError, TraceRecordCapture, TraceRecordLimits, record_trace_bus};

const SOCKET_FILE: &str = "telemetry-record.sock";
const PROTOCOL_VERSION: u16 = 1;
const MAX_REQUEST_BYTES: u64 = 8_192;
const MAX_RUNTIME_RESPONSE_BYTES: u64 = (MAX_ARCHIVE_BYTES as u64).div_ceil(3) * 4 + 1_024;
const MAX_RESPONSE_BYTES: u64 = 300_000;
const MAX_CONNECTIONS: usize = 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const STATE_DIRECTORY_MODE: u32 = 0o700;
const SOCKET_MODE: u32 = 0o600;

#[derive(Clone, Copy, PartialEq, Eq)]
enum LocalTopKind {
    Api,
    Disk,
    Locks,
    Rpc,
}

impl LocalTopKind {
    const fn tool_id(self) -> &'static str {
        match self {
            Self::Api => "top.api",
            Self::Disk => "top.disk",
            Self::Locks => "top.locks",
            Self::Rpc => "top.rpc",
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum LocalTraceCaptureError {
    #[error("telemetry server runtime is unavailable")]
    RuntimeUnavailable,
    #[error("telemetry server runtime state is not owner-only")]
    StateSecurity,
    #[error("telemetry server runtime protocol failed")]
    Protocol,
    #[error("telemetry server runtime I/O failed")]
    Io(#[source] io::Error),
    #[error(transparent)]
    Producer(#[from] TelemetryProducerError),
    #[error("runtime profile request rejected: {0}")]
    RuntimeProfile(String),
    #[error("health request rejected: {0}")]
    Health(String),
    #[error("top request rejected: {0}")]
    Top(String),
    #[error("network performance request rejected: {0}")]
    Network(String),
    #[error("local diagnostic cancellation was not acknowledged")]
    CancellationUnconfirmed,
    #[error("selected offline identity is unavailable or unsafe")]
    OfflineIdentity,
}

pub(crate) struct LocalTraceCaptureRuntime {
    shutdown: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl LocalTraceCaptureRuntime {
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for LocalTraceCaptureRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "operation",
    rename_all = "SCREAMING_SNAKE_CASE",
    rename_all_fields = "camelCase"
)]
enum CaptureRequest {
    TraceRecord {
        protocol_version: u16,
        consent_expires_at_unix: i64,
        duration_millis: u64,
        max_spans: usize,
    },
    RuntimeProfile {
        protocol_version: u16,
        request: LocalRuntimeProfileRequest,
    },
    NativeThreadsProfile {
        protocol_version: u16,
        request: LocalRuntimeProfileRequest,
    },
    TopDisk {
        protocol_version: u16,
        request: super::top_disk::LocalTopRequest,
    },
    TopApi {
        protocol_version: u16,
        request: super::top_disk::LocalTopRequest,
    },
    TopLocks {
        protocol_version: u16,
        request: super::top_disk::LocalTopRequest,
    },
    TopRpc {
        protocol_version: u16,
        request: super::top_disk::LocalTopRequest,
    },
    NetworkPerformance {
        protocol_version: u16,
        request: LocalNetworkRequest,
    },
    Health {
        protocol_version: u16,
        request: LocalHealthRequest,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct LocalHealthRequest {
    pub offline_key_id: String,
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub capability: String,
    pub consent_uid: String,
    pub policy_revision: u64,
    pub consent_expires_at_unix: i64,
    pub acknowledge_l0: bool,
    pub expires_at_unix: i64,
}

pub(crate) struct LocalHealthArchive {
    pub artifact_uid: String,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct LocalNetworkRequest {
    pub offline_key_id: String,
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub consent_uid: String,
    pub policy_revision: u64,
    pub consent_expires_at_unix: i64,
    pub acknowledge_l1: bool,
    pub expires_at_unix: i64,
    pub duration_millis: u64,
    pub traffic_bytes: u64,
}

pub(crate) struct LocalNetworkArchive {
    pub artifact_uid: String,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ProducerErrorCode {
    Busy,
    ConsentExpired,
    InvalidDuration,
    InvalidSpanLimit,
    Cancelled,
    SourceUnavailable,
    DurationOverflow,
    ResultTooLarge,
}

impl From<&TelemetryProducerError> for ProducerErrorCode {
    fn from(error: &TelemetryProducerError) -> Self {
        match error {
            TelemetryProducerError::Busy => Self::Busy,
            TelemetryProducerError::ConsentExpired => Self::ConsentExpired,
            TelemetryProducerError::InvalidDuration => Self::InvalidDuration,
            TelemetryProducerError::InvalidSpanLimit => Self::InvalidSpanLimit,
            TelemetryProducerError::Cancelled => Self::Cancelled,
            TelemetryProducerError::SourceUnavailable => Self::SourceUnavailable,
            TelemetryProducerError::DurationOverflow => Self::DurationOverflow,
            TelemetryProducerError::ResultTooLarge => Self::ResultTooLarge,
        }
    }
}

impl From<ProducerErrorCode> for TelemetryProducerError {
    fn from(code: ProducerErrorCode) -> Self {
        match code {
            ProducerErrorCode::Busy => Self::Busy,
            ProducerErrorCode::ConsentExpired => Self::ConsentExpired,
            ProducerErrorCode::InvalidDuration => Self::InvalidDuration,
            ProducerErrorCode::InvalidSpanLimit => Self::InvalidSpanLimit,
            ProducerErrorCode::Cancelled => Self::Cancelled,
            ProducerErrorCode::SourceUnavailable => Self::SourceUnavailable,
            ProducerErrorCode::DurationOverflow => Self::DurationOverflow,
            ProducerErrorCode::ResultTooLarge => Self::ResultTooLarge,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "status",
    rename_all = "SCREAMING_SNAKE_CASE",
    rename_all_fields = "camelCase"
)]
enum CaptureResponse {
    Ok {
        capture: TraceRecordCapture,
    },
    Error {
        code: ProducerErrorCode,
    },
    RuntimeOk {
        archive_base64: String,
        archive_sha256: String,
        artifact_uid: String,
        outcome: ProfileOutcome,
        reason_code: ProfileReasonCode,
    },
    RuntimeError {
        code: RuntimeErrorCode,
    },
    TopDiskOk {
        archive_base64: String,
        archive_sha256: String,
        artifact_uid: String,
    },
    TopDiskError {
        code: RuntimeErrorCode,
    },
    TopApiOk {
        archive_base64: String,
        archive_sha256: String,
        artifact_uid: String,
    },
    TopApiError {
        code: RuntimeErrorCode,
    },
    TopLocksOk {
        archive_base64: String,
        archive_sha256: String,
        artifact_uid: String,
    },
    TopLocksError {
        code: RuntimeErrorCode,
    },
    TopRpcOk {
        archive_base64: String,
        archive_sha256: String,
        artifact_uid: String,
    },
    TopRpcError {
        code: RuntimeErrorCode,
    },
    NetworkOk {
        archive_base64: String,
        archive_sha256: String,
        artifact_uid: String,
    },
    NetworkError {
        code: RuntimeErrorCode,
    },
    HealthOk {
        archive_base64: String,
        archive_sha256: String,
        artifact_uid: String,
    },
    HealthError {
        code: RuntimeErrorCode,
    },
}

pub(crate) fn spawn_local_trace_capture_runtime(
    state_root: &Path,
    parent_shutdown: &CancellationToken,
) -> Result<LocalTraceCaptureRuntime, LocalTraceCaptureError> {
    let owner = private_state_owner(state_root)?;
    let socket_path = state_root.join(SOCKET_FILE);
    let listener = bind_listener(&socket_path, owner)?;
    let socket_identity = socket_identity(&socket_path, owner)?;
    let shutdown = parent_shutdown.child_token();
    let task_shutdown = shutdown.clone();
    let state_root = state_root.to_path_buf();
    let task = tokio::spawn(async move {
        run_listener(listener, owner, state_root, task_shutdown).await;
        remove_own_socket(&socket_path, socket_identity);
    });
    Ok(LocalTraceCaptureRuntime {
        shutdown,
        task: Some(task),
    })
}

pub(crate) async fn request_local_trace_capture(
    state_root: &Path,
    consent_expires_at_unix: i64,
    limits: TraceRecordLimits,
    cancel: &CancellationToken,
) -> Result<TraceRecordCapture, LocalTraceCaptureError> {
    limits.validate()?;
    let owner = private_state_owner(state_root)?;
    let socket_path = state_root.join(SOCKET_FILE);
    socket_identity(&socket_path, owner)?;
    let stream = UnixStream::connect(&socket_path).await.map_err(|error| match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused => LocalTraceCaptureError::RuntimeUnavailable,
        _ => LocalTraceCaptureError::Io(error),
    })?;
    if !stream.peer_cred().is_ok_and(|credentials| credentials.uid() == owner) {
        return Err(LocalTraceCaptureError::StateSecurity);
    }
    let (reader, mut writer) = stream.into_split();
    let request = CaptureRequest::TraceRecord {
        protocol_version: PROTOCOL_VERSION,
        consent_expires_at_unix,
        duration_millis: u64::try_from(limits.duration.as_millis()).map_err(|_| TelemetryProducerError::InvalidDuration)?,
        max_spans: limits.max_spans,
    };
    let mut encoded = serde_json::to_vec(&request).map_err(|_| LocalTraceCaptureError::Protocol)?;
    encoded.push(b'\n');
    if encoded.len() > 1_024 {
        return Err(LocalTraceCaptureError::Protocol);
    }
    writer.write_all(&encoded).await.map_err(LocalTraceCaptureError::Io)?;

    let response = async move {
        let mut bytes = Vec::new();
        reader
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(LocalTraceCaptureError::Io)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_RESPONSE_BYTES {
            return Err(LocalTraceCaptureError::Protocol);
        }
        serde_json::from_slice::<CaptureResponse>(&bytes).map_err(|_| LocalTraceCaptureError::Protocol)
    };
    let response = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(TelemetryProducerError::Cancelled.into()),
        response = response => response?,
    };
    match response {
        CaptureResponse::Ok { capture } => Ok(capture),
        CaptureResponse::Error { code } => Err(TelemetryProducerError::from(code).into()),
        _ => Err(LocalTraceCaptureError::Protocol),
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum RuntimeErrorCode {
    InvalidRequest,
    IdentityUnavailable,
    ConsentRequired,
    ConsentExpired,
    Expired,
    LimitExceeded,
    Busy,
    Cancelled,
    TimedOut,
    UnsupportedPlatform,
    SourceUnavailable,
    CollectionFailed,
}

impl From<ProfileError> for RuntimeErrorCode {
    fn from(error: ProfileError) -> Self {
        match error {
            ProfileError::ConsentRequired => Self::ConsentRequired,
            ProfileError::ConsentExpired => Self::ConsentExpired,
            ProfileError::Expired => Self::Expired,
            ProfileError::LimitExceeded => Self::LimitExceeded,
            ProfileError::Busy => Self::Busy,
            ProfileError::Cancelled => Self::Cancelled,
            ProfileError::TimedOut => Self::TimedOut,
            ProfileError::InvalidRequest | ProfileError::UnsupportedCapability | ProfileError::UnsupportedVersion => {
                Self::InvalidRequest
            }
            _ => Self::CollectionFailed,
        }
    }
}

pub(crate) async fn request_local_runtime_profile(
    state_root: &Path,
    request: LocalRuntimeProfileRequest,
    cancel: &CancellationToken,
) -> Result<SignedProfileExport, LocalTraceCaptureError> {
    request_local_profile_capture(state_root, request, cancel, false).await
}

pub(crate) async fn request_local_health(
    state_root: &Path,
    request: LocalHealthRequest,
    cancel: &CancellationToken,
) -> Result<LocalHealthArchive, LocalTraceCaptureError> {
    if request.capability != super::health::HEALTH_SERVICE_CAPABILITY {
        return Err(LocalTraceCaptureError::Protocol);
    }
    let owner = private_state_owner(state_root)?;
    let socket_path = state_root.join(SOCKET_FILE);
    socket_identity(&socket_path, owner)?;
    let stream = UnixStream::connect(&socket_path).await.map_err(LocalTraceCaptureError::Io)?;
    if !stream.peer_cred().is_ok_and(|credentials| credentials.uid() == owner) {
        return Err(LocalTraceCaptureError::StateSecurity);
    }
    let artifact_uid = request.artifact_uid.clone();
    let mut bytes = serde_json::to_vec(&CaptureRequest::Health {
        protocol_version: PROTOCOL_VERSION,
        request,
    })
    .map_err(|_| LocalTraceCaptureError::Protocol)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_REQUEST_BYTES {
        return Err(LocalTraceCaptureError::Protocol);
    }
    let (reader, mut writer) = stream.into_split();
    tokio::time::timeout(REQUEST_TIMEOUT, writer.write_all(&bytes))
        .await
        .map_err(|_| LocalTraceCaptureError::Protocol)?
        .map_err(LocalTraceCaptureError::Io)?;
    let response = async {
        let mut bytes = Vec::new();
        reader
            .take(MAX_RUNTIME_RESPONSE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(LocalTraceCaptureError::Io)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_RUNTIME_RESPONSE_BYTES {
            return Err(LocalTraceCaptureError::Protocol);
        }
        serde_json::from_slice::<CaptureResponse>(&bytes).map_err(|_| LocalTraceCaptureError::Protocol)
    };
    let response = tokio::time::timeout(Duration::from_secs(super::health::HEALTH_TIMEOUT_SECONDS + 5), response);
    tokio::pin!(response);
    let response = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            writer.shutdown().await.map_err(|_| LocalTraceCaptureError::CancellationUnconfirmed)?;
            let acknowledged = matches!(tokio::time::timeout(Duration::from_secs(2), &mut response).await,
                Ok(Ok(Ok(CaptureResponse::HealthError { .. } | CaptureResponse::HealthOk { .. }))));
            if !acknowledged { return Err(LocalTraceCaptureError::CancellationUnconfirmed); }
            return Err(LocalTraceCaptureError::Health("CANCELLED".to_owned()));
        }
        response = &mut response => response.map_err(|_| LocalTraceCaptureError::Protocol)??,
    };
    match response {
        CaptureResponse::HealthOk {
            archive_base64,
            archive_sha256,
            artifact_uid: returned_uid,
        } => {
            let archive_bytes = URL_SAFE_NO_PAD
                .decode_to_vec(&archive_base64)
                .map_err(|_| LocalTraceCaptureError::Protocol)?;
            if archive_bytes.is_empty()
                || archive_bytes.len() as u64 > super::health::MAX_HEALTH_OUTPUT_BYTES
                || returned_uid != artifact_uid
                || URL_SAFE_NO_PAD.encode_to_string(&archive_bytes) != archive_base64
                || hex_simd::encode_to_string(Sha256::digest(&archive_bytes), hex_simd::AsciiCase::Lower) != archive_sha256
            {
                return Err(LocalTraceCaptureError::Protocol);
            }
            Ok(LocalHealthArchive {
                artifact_uid,
                archive_bytes,
                archive_sha256,
            })
        }
        CaptureResponse::HealthError { code } => Err(LocalTraceCaptureError::Health(format!("{code:?}"))),
        _ => Err(LocalTraceCaptureError::Protocol),
    }
}

pub(crate) async fn request_local_network(
    state_root: &Path,
    request: LocalNetworkRequest,
    cancel: &CancellationToken,
) -> Result<LocalNetworkArchive, LocalTraceCaptureError> {
    let owner = private_state_owner(state_root)?;
    let socket_path = state_root.join(SOCKET_FILE);
    socket_identity(&socket_path, owner)?;
    let stream = UnixStream::connect(&socket_path).await.map_err(LocalTraceCaptureError::Io)?;
    if !stream.peer_cred().is_ok_and(|credentials| credentials.uid() == owner) {
        return Err(LocalTraceCaptureError::StateSecurity);
    }
    let artifact_uid = request.artifact_uid.clone();
    let mut bytes = serde_json::to_vec(&CaptureRequest::NetworkPerformance {
        protocol_version: PROTOCOL_VERSION,
        request,
    })
    .map_err(|_| LocalTraceCaptureError::Protocol)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_REQUEST_BYTES {
        return Err(LocalTraceCaptureError::Protocol);
    }
    let (reader, mut writer) = stream.into_split();
    tokio::time::timeout(REQUEST_TIMEOUT, writer.write_all(&bytes))
        .await
        .map_err(|_| LocalTraceCaptureError::Protocol)?
        .map_err(LocalTraceCaptureError::Io)?;
    let response = async {
        let mut bytes = Vec::new();
        reader
            .take(MAX_RUNTIME_RESPONSE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(LocalTraceCaptureError::Io)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_RUNTIME_RESPONSE_BYTES {
            return Err(LocalTraceCaptureError::Protocol);
        }
        serde_json::from_slice::<CaptureResponse>(&bytes).map_err(|_| LocalTraceCaptureError::Protocol)
    };
    let response = tokio::time::timeout(super::perf_network::MAX_NETWORK_DURATION + Duration::from_secs(5), response);
    tokio::pin!(response);
    let response = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            writer.shutdown().await.map_err(|_| LocalTraceCaptureError::CancellationUnconfirmed)?;
            let acknowledged = matches!(tokio::time::timeout(Duration::from_secs(2), &mut response).await,
                Ok(Ok(Ok(CaptureResponse::NetworkError { .. } | CaptureResponse::NetworkOk { .. }))));
            if !acknowledged { return Err(LocalTraceCaptureError::CancellationUnconfirmed); }
            return Err(LocalTraceCaptureError::Network("CANCELLED".to_owned()));
        }
        response = &mut response => response.map_err(|_| LocalTraceCaptureError::Protocol)??,
    };
    match response {
        CaptureResponse::NetworkOk {
            archive_base64,
            archive_sha256,
            artifact_uid: returned_uid,
        } => {
            let archive_bytes = URL_SAFE_NO_PAD
                .decode_to_vec(&archive_base64)
                .map_err(|_| LocalTraceCaptureError::Protocol)?;
            if archive_bytes.is_empty()
                || archive_bytes.len() > super::perf_network::MAX_ARCHIVE_BYTES
                || returned_uid != artifact_uid
                || URL_SAFE_NO_PAD.encode_to_string(&archive_bytes) != archive_base64
                || hex_simd::encode_to_string(Sha256::digest(&archive_bytes), hex_simd::AsciiCase::Lower) != archive_sha256
            {
                return Err(LocalTraceCaptureError::Protocol);
            }
            Ok(LocalNetworkArchive {
                artifact_uid,
                archive_bytes,
                archive_sha256,
            })
        }
        CaptureResponse::NetworkError { code } => Err(LocalTraceCaptureError::Network(format!("{code:?}"))),
        _ => Err(LocalTraceCaptureError::Protocol),
    }
}

pub(crate) async fn request_local_native_threads_profile(
    state_root: &Path,
    request: LocalRuntimeProfileRequest,
    cancel: &CancellationToken,
) -> Result<SignedProfileExport, LocalTraceCaptureError> {
    request_local_profile_capture(state_root, request, cancel, true).await
}

async fn request_local_profile_capture(
    state_root: &Path,
    request: LocalRuntimeProfileRequest,
    cancel: &CancellationToken,
    native_threads: bool,
) -> Result<SignedProfileExport, LocalTraceCaptureError> {
    let tool = match request.capability.as_str() {
        super::profile_cpu::THREAD_PROFILE_CAPABILITY => ProfileTool::Threads,
        super::profile_cpu::MEMORY_PROFILE_CAPABILITY if !native_threads => ProfileTool::Memory,
        super::profile_cpu::CPU_PROFILE_CAPABILITY if !native_threads => ProfileTool::Cpu,
        _ => return Err(LocalTraceCaptureError::Protocol),
    };
    let owner = private_state_owner(state_root)?;
    let socket_path = state_root.join(SOCKET_FILE);
    socket_identity(&socket_path, owner)?;
    let stream = UnixStream::connect(&socket_path).await.map_err(LocalTraceCaptureError::Io)?;
    if !stream.peer_cred().is_ok_and(|credentials| credentials.uid() == owner) {
        return Err(LocalTraceCaptureError::StateSecurity);
    }
    let artifact_uid = request.artifact_uid.clone();
    let message = if native_threads {
        CaptureRequest::NativeThreadsProfile {
            protocol_version: PROTOCOL_VERSION,
            request,
        }
    } else {
        CaptureRequest::RuntimeProfile {
            protocol_version: PROTOCOL_VERSION,
            request,
        }
    };
    let mut bytes = serde_json::to_vec(&message).map_err(|_| LocalTraceCaptureError::Protocol)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_REQUEST_BYTES {
        return Err(LocalTraceCaptureError::Protocol);
    }
    let (reader, mut writer) = stream.into_split();
    tokio::time::timeout(REQUEST_TIMEOUT, writer.write_all(&bytes))
        .await
        .map_err(|_| LocalTraceCaptureError::Protocol)?
        .map_err(LocalTraceCaptureError::Io)?;
    // Keep the write half open: EOF is the server's cancellation signal.
    let response = async {
        let mut bytes = Vec::new();
        reader
            .take(MAX_RUNTIME_RESPONSE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(LocalTraceCaptureError::Io)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_RUNTIME_RESPONSE_BYTES {
            return Err(LocalTraceCaptureError::Protocol);
        }
        serde_json::from_slice::<CaptureResponse>(&bytes).map_err(|_| LocalTraceCaptureError::Protocol)
    };
    let response = tokio::time::timeout(MAX_PROFILE_DURATION + Duration::from_secs(5), response);
    tokio::pin!(response);
    let response = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            writer.shutdown().await.map_err(|_| LocalTraceCaptureError::CancellationUnconfirmed)?;
            // The service closes the response only after the blocking collector has joined.
            let acknowledged = matches!(tokio::time::timeout(Duration::from_secs(2), &mut response).await,
                Ok(Ok(Ok(CaptureResponse::RuntimeError { .. } | CaptureResponse::RuntimeOk { .. }))));
            if !acknowledged { return Err(LocalTraceCaptureError::CancellationUnconfirmed); }
            return Err(LocalTraceCaptureError::RuntimeProfile("CANCELLED".to_owned()));
        }
        response = &mut response => response.map_err(|_| LocalTraceCaptureError::Protocol)??,
    };
    match response {
        CaptureResponse::RuntimeOk {
            archive_base64,
            archive_sha256,
            artifact_uid: returned_uid,
            outcome,
            reason_code,
        } => {
            let archive_bytes = URL_SAFE_NO_PAD
                .decode_to_vec(&archive_base64)
                .map_err(|_| LocalTraceCaptureError::Protocol)?;
            if archive_bytes.is_empty()
                || archive_bytes.len() > MAX_ARCHIVE_BYTES
                || returned_uid != artifact_uid
                || URL_SAFE_NO_PAD.encode_to_string(&archive_bytes) != archive_base64
                || hex_simd::encode_to_string(Sha256::digest(&archive_bytes), hex_simd::AsciiCase::Lower) != archive_sha256
            {
                return Err(LocalTraceCaptureError::Protocol);
            }
            Ok(SignedProfileExport {
                artifact_uid,
                tool,
                outcome,
                reason_code,
                archive_bytes,
                archive_sha256,
            })
        }
        CaptureResponse::RuntimeError { code } => Err(LocalTraceCaptureError::RuntimeProfile(format!("{code:?}"))),
        _ => Err(LocalTraceCaptureError::Protocol),
    }
}

pub(crate) async fn request_local_top_disk(
    state_root: &Path,
    request: super::top_disk::LocalTopRequest,
    cancel: &CancellationToken,
) -> Result<super::top_disk::LocalTopArchive, LocalTraceCaptureError> {
    request_local_top_capture(state_root, request, cancel, LocalTopKind::Disk).await
}

pub(crate) async fn request_local_top_locks(
    state_root: &Path,
    request: super::top_disk::LocalTopRequest,
    cancel: &CancellationToken,
) -> Result<super::top_disk::LocalTopArchive, LocalTraceCaptureError> {
    request_local_top_capture(state_root, request, cancel, LocalTopKind::Locks).await
}

pub(crate) async fn request_local_top_api(
    state_root: &Path,
    request: super::top_disk::LocalTopRequest,
    cancel: &CancellationToken,
) -> Result<super::top_disk::LocalTopArchive, LocalTraceCaptureError> {
    request_local_top_capture(state_root, request, cancel, LocalTopKind::Api).await
}

pub(crate) async fn request_local_top_rpc(
    state_root: &Path,
    request: super::top_disk::LocalTopRequest,
    cancel: &CancellationToken,
) -> Result<super::top_disk::LocalTopArchive, LocalTraceCaptureError> {
    request_local_top_capture(state_root, request, cancel, LocalTopKind::Rpc).await
}

async fn request_local_top_capture(
    state_root: &Path,
    request: super::top_disk::LocalTopRequest,
    cancel: &CancellationToken,
    kind: LocalTopKind,
) -> Result<super::top_disk::LocalTopArchive, LocalTraceCaptureError> {
    let owner = private_state_owner(state_root)?;
    let socket_path = state_root.join(SOCKET_FILE);
    socket_identity(&socket_path, owner)?;
    let stream = UnixStream::connect(&socket_path).await.map_err(LocalTraceCaptureError::Io)?;
    if !stream.peer_cred().is_ok_and(|credentials| credentials.uid() == owner) {
        return Err(LocalTraceCaptureError::StateSecurity);
    }
    let artifact_uid = request.artifact_uid.clone();
    let message = match kind {
        LocalTopKind::Disk => CaptureRequest::TopDisk {
            protocol_version: PROTOCOL_VERSION,
            request,
        },
        LocalTopKind::Locks => CaptureRequest::TopLocks {
            protocol_version: PROTOCOL_VERSION,
            request,
        },
        LocalTopKind::Api => CaptureRequest::TopApi {
            protocol_version: PROTOCOL_VERSION,
            request,
        },
        LocalTopKind::Rpc => CaptureRequest::TopRpc {
            protocol_version: PROTOCOL_VERSION,
            request,
        },
    };
    let mut bytes = serde_json::to_vec(&message).map_err(|_| LocalTraceCaptureError::Protocol)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_REQUEST_BYTES {
        return Err(LocalTraceCaptureError::Protocol);
    }
    let (reader, mut writer) = stream.into_split();
    tokio::time::timeout(REQUEST_TIMEOUT, writer.write_all(&bytes))
        .await
        .map_err(|_| LocalTraceCaptureError::Protocol)?
        .map_err(LocalTraceCaptureError::Io)?;
    // Keep the write half open: EOF is the server's cancellation signal.
    let response = async {
        let mut bytes = Vec::new();
        reader
            .take(MAX_RUNTIME_RESPONSE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(LocalTraceCaptureError::Io)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_RUNTIME_RESPONSE_BYTES {
            return Err(LocalTraceCaptureError::Protocol);
        }
        serde_json::from_slice::<CaptureResponse>(&bytes).map_err(|_| LocalTraceCaptureError::Protocol)
    };
    let response = tokio::time::timeout(MAX_PROFILE_DURATION + Duration::from_secs(5), response);
    tokio::pin!(response);
    let response = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            writer.shutdown().await.map_err(|_| LocalTraceCaptureError::CancellationUnconfirmed)?;
            // The service closes the response only after the blocking collector has joined.
            let acknowledged = match tokio::time::timeout(Duration::from_secs(2), &mut response).await {
                Ok(Ok(Ok(CaptureResponse::TopDiskError { .. } | CaptureResponse::TopDiskOk { .. }))) => kind == LocalTopKind::Disk,
                Ok(Ok(Ok(CaptureResponse::TopLocksError { .. } | CaptureResponse::TopLocksOk { .. }))) => kind == LocalTopKind::Locks,
                Ok(Ok(Ok(CaptureResponse::TopApiError { .. } | CaptureResponse::TopApiOk { .. }))) => kind == LocalTopKind::Api,
                Ok(Ok(Ok(CaptureResponse::TopRpcError { .. } | CaptureResponse::TopRpcOk { .. }))) => kind == LocalTopKind::Rpc,
                _ => false,
            };
            if !acknowledged { return Err(LocalTraceCaptureError::CancellationUnconfirmed); }
            return Err(LocalTraceCaptureError::Top("CANCELLED".to_owned()));
        }
        response = &mut response => response.map_err(|_| LocalTraceCaptureError::Protocol)??,
    };
    match (kind, response) {
        (
            LocalTopKind::Disk,
            CaptureResponse::TopDiskOk {
                archive_base64,
                archive_sha256,
                artifact_uid: returned_uid,
            },
        )
        | (
            LocalTopKind::Locks,
            CaptureResponse::TopLocksOk {
                archive_base64,
                archive_sha256,
                artifact_uid: returned_uid,
            },
        )
        | (
            LocalTopKind::Api,
            CaptureResponse::TopApiOk {
                archive_base64,
                archive_sha256,
                artifact_uid: returned_uid,
            },
        )
        | (
            LocalTopKind::Rpc,
            CaptureResponse::TopRpcOk {
                archive_base64,
                archive_sha256,
                artifact_uid: returned_uid,
            },
        ) => {
            let archive_bytes = URL_SAFE_NO_PAD
                .decode_to_vec(&archive_base64)
                .map_err(|_| LocalTraceCaptureError::Protocol)?;
            if archive_bytes.is_empty()
                || archive_bytes.len() > super::top_api::MAX_ARCHIVE_BYTES
                || returned_uid != artifact_uid
                || URL_SAFE_NO_PAD.encode_to_string(&archive_bytes) != archive_base64
                || hex_simd::encode_to_string(Sha256::digest(&archive_bytes), hex_simd::AsciiCase::Lower) != archive_sha256
            {
                return Err(LocalTraceCaptureError::Protocol);
            }
            Ok(super::top_disk::LocalTopArchive {
                artifact_uid,
                archive_bytes,
                archive_sha256,
            })
        }
        (LocalTopKind::Disk, CaptureResponse::TopDiskError { code })
        | (LocalTopKind::Locks, CaptureResponse::TopLocksError { code })
        | (LocalTopKind::Api, CaptureResponse::TopApiError { code })
        | (LocalTopKind::Rpc, CaptureResponse::TopRpcError { code }) => Err(LocalTraceCaptureError::Top(format!("{code:?}"))),
        _ => Err(LocalTraceCaptureError::Protocol),
    }
}

async fn handle_runtime_profile(
    mut reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    mut writer: tokio::net::unix::OwnedWriteHalf,
    state_root: &Path,
    protocol_version: u16,
    request: LocalRuntimeProfileRequest,
    shutdown: CancellationToken,
    native_threads: bool,
) {
    let cancel = shutdown.child_token();
    let capture = async {
        if native_threads {
            capture_local_native_threads_profile(state_root, protocol_version, request, &cancel).await
        } else {
            capture_local_runtime_profile(state_root, protocol_version, request, &cancel).await
        }
    };
    tokio::pin!(capture);
    let mut unexpected = [0_u8; 1];
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => { cancel.cancel(); let _ = capture.await; return; }
        _ = reader.read(&mut unexpected) => { cancel.cancel(); let _ = capture.await; Err(RuntimeErrorCode::Cancelled) }
        result = &mut capture => result,
    };
    let response = match result {
        Ok(export) => CaptureResponse::RuntimeOk {
            archive_base64: URL_SAFE_NO_PAD.encode_to_string(&export.archive_bytes),
            archive_sha256: export.archive_sha256,
            artifact_uid: export.artifact_uid,
            outcome: export.outcome,
            reason_code: export.reason_code,
        },
        Err(code) => CaptureResponse::RuntimeError { code },
    };
    if let Ok(bytes) = serde_json::to_vec(&response)
        && bytes.len() as u64 <= MAX_RUNTIME_RESPONSE_BYTES
    {
        let _ = tokio::time::timeout(REQUEST_TIMEOUT, async {
            writer.write_all(&bytes).await?;
            writer.shutdown().await
        })
        .await;
    }
}

async fn handle_health(
    mut reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    mut writer: tokio::net::unix::OwnedWriteHalf,
    state_root: &Path,
    protocol_version: u16,
    request: LocalHealthRequest,
    shutdown: CancellationToken,
) {
    let cancel = shutdown.child_token();
    let capture = async {
        match tokio::time::timeout(
            Duration::from_secs(super::health::HEALTH_TIMEOUT_SECONDS),
            capture_local_health(state_root, protocol_version, request, &cancel),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                cancel.cancel();
                Err(RuntimeErrorCode::TimedOut)
            }
        }
    };
    tokio::pin!(capture);
    let mut unexpected = [0_u8; 1];
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => { cancel.cancel(); let _ = capture.await; return; }
        _ = reader.read(&mut unexpected) => { cancel.cancel(); let _ = capture.await; Err(RuntimeErrorCode::Cancelled) }
        result = &mut capture => result,
    };
    let response = match result {
        Ok(export) => CaptureResponse::HealthOk {
            archive_base64: URL_SAFE_NO_PAD.encode_to_string(&export.archive_bytes),
            archive_sha256: export.archive_sha256,
            artifact_uid: export.artifact_uid,
        },
        Err(code) => CaptureResponse::HealthError { code },
    };
    if let Ok(bytes) = serde_json::to_vec(&response)
        && bytes.len() as u64 <= MAX_RUNTIME_RESPONSE_BYTES
    {
        let _ = tokio::time::timeout(REQUEST_TIMEOUT, async {
            writer.write_all(&bytes).await?;
            writer.shutdown().await
        })
        .await;
    }
}

async fn handle_top_capture(
    mut reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    mut writer: tokio::net::unix::OwnedWriteHalf,
    state_root: &Path,
    protocol_version: u16,
    request: super::top_disk::LocalTopRequest,
    shutdown: CancellationToken,
    kind: LocalTopKind,
) {
    let cancel = shutdown.child_token();
    let capture = capture_local_top(state_root, protocol_version, request, &cancel, kind);
    tokio::pin!(capture);
    let mut unexpected = [0_u8; 1];
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => { cancel.cancel(); let _ = capture.await; return; }
        _ = reader.read(&mut unexpected) => { cancel.cancel(); let _ = capture.await; Err(RuntimeErrorCode::Cancelled) }
        result = &mut capture => result,
    };
    let response = match (kind, result) {
        (LocalTopKind::Disk, Ok(export)) => CaptureResponse::TopDiskOk {
            archive_base64: URL_SAFE_NO_PAD.encode_to_string(&export.archive_bytes),
            archive_sha256: export.archive_sha256,
            artifact_uid: export.artifact_uid,
        },
        (LocalTopKind::Locks, Ok(export)) => CaptureResponse::TopLocksOk {
            archive_base64: URL_SAFE_NO_PAD.encode_to_string(&export.archive_bytes),
            archive_sha256: export.archive_sha256,
            artifact_uid: export.artifact_uid,
        },
        (LocalTopKind::Api, Ok(export)) => CaptureResponse::TopApiOk {
            archive_base64: URL_SAFE_NO_PAD.encode_to_string(&export.archive_bytes),
            archive_sha256: export.archive_sha256,
            artifact_uid: export.artifact_uid,
        },
        (LocalTopKind::Rpc, Ok(export)) => CaptureResponse::TopRpcOk {
            archive_base64: URL_SAFE_NO_PAD.encode_to_string(&export.archive_bytes),
            archive_sha256: export.archive_sha256,
            artifact_uid: export.artifact_uid,
        },
        (LocalTopKind::Disk, Err(code)) => CaptureResponse::TopDiskError { code },
        (LocalTopKind::Locks, Err(code)) => CaptureResponse::TopLocksError { code },
        (LocalTopKind::Api, Err(code)) => CaptureResponse::TopApiError { code },
        (LocalTopKind::Rpc, Err(code)) => CaptureResponse::TopRpcError { code },
    };
    if let Ok(bytes) = serde_json::to_vec(&response)
        && bytes.len() as u64 <= MAX_RUNTIME_RESPONSE_BYTES
    {
        let _ = tokio::time::timeout(REQUEST_TIMEOUT, async {
            writer.write_all(&bytes).await?;
            writer.shutdown().await
        })
        .await;
    }
}

async fn handle_network_capture(
    mut reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    mut writer: tokio::net::unix::OwnedWriteHalf,
    state_root: &Path,
    protocol_version: u16,
    request: LocalNetworkRequest,
    shutdown: CancellationToken,
) {
    let cancel = shutdown.child_token();
    let capture = capture_local_network(state_root, protocol_version, request, &cancel);
    tokio::pin!(capture);
    let mut unexpected = [0_u8; 1];
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => { cancel.cancel(); let _ = capture.await; return; }
        _ = reader.read(&mut unexpected) => { cancel.cancel(); let _ = capture.await; Err(RuntimeErrorCode::Cancelled) }
        result = &mut capture => result,
    };
    let response = match result {
        Ok(export) => CaptureResponse::NetworkOk {
            archive_base64: URL_SAFE_NO_PAD.encode_to_string(&export.archive_bytes),
            archive_sha256: export.archive_sha256,
            artifact_uid: export.artifact_uid,
        },
        Err(code) => CaptureResponse::NetworkError { code },
    };
    if let Ok(bytes) = serde_json::to_vec(&response)
        && bytes.len() as u64 <= MAX_RUNTIME_RESPONSE_BYTES
    {
        let _ = tokio::time::timeout(REQUEST_TIMEOUT, async {
            writer.write_all(&bytes).await?;
            writer.shutdown().await
        })
        .await;
    }
}

async fn capture_local_network(
    state_root: &Path,
    protocol_version: u16,
    input: LocalNetworkRequest,
    cancel: &CancellationToken,
) -> Result<super::perf_network::SignedNetworkExport, RuntimeErrorCode> {
    use super::perf_network::{
        LocalNetworkConsent, MAX_BANDWIDTH_BYTES_PER_SECOND, MAX_NETWORK_DURATION, MAX_TRAFFIC_BYTES, NETWORK_CAPABILITY,
        NETWORK_SCHEMA_VERSION, NetworkOutcome, NetworkPerformanceRequest, NetworkProvenance, measure_network,
        runtime_network_peer_aliases, sign_network_export,
    };
    use rand::{TryRng as _, rngs::SysRng};

    if protocol_version != PROTOCOL_VERSION
        || input.offline_key_id.len() != 64
        || !input
            .offline_key_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(RuntimeErrorCode::InvalidRequest);
    }
    if cancel.is_cancelled() {
        return Err(RuntimeErrorCode::Cancelled);
    }
    if !input.acknowledge_l1 || input.policy_revision == 0 {
        return Err(RuntimeErrorCode::ConsentRequired);
    }
    let now = unix_now().map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    if input.consent_expires_at_unix <= now || input.expires_at_unix > input.consent_expires_at_unix {
        return Err(RuntimeErrorCode::ConsentExpired);
    }
    if input.expires_at_unix <= now || input.expires_at_unix.saturating_sub(now) > 2_592_000 {
        return Err(RuntimeErrorCode::Expired);
    }
    if input.duration_millis == 0
        || input.duration_millis > MAX_NETWORK_DURATION.as_millis() as u64
        || input.traffic_bytes == 0
        || input.traffic_bytes > MAX_TRAFFIC_BYTES
        || input.traffic_bytes > MAX_BANDWIDTH_BYTES_PER_SECOND.saturating_mul(input.duration_millis) / 1_000
    {
        return Err(RuntimeErrorCode::LimitExceeded);
    }
    if input.duration_millis.div_ceil(1_000) as i64 > input.expires_at_unix.min(input.consent_expires_at_unix).saturating_sub(now)
    {
        return Err(RuntimeErrorCode::Expired);
    }
    let key = load_offline_key(state_root, &input.offline_key_id)?;
    let peer_aliases = runtime_network_peer_aliases().ok_or(RuntimeErrorCode::SourceUnavailable)?;
    let peer_count = u64::try_from(peer_aliases.len()).map_err(|_| RuntimeErrorCode::LimitExceeded)?;
    let traffic_bytes_per_peer = input
        .traffic_bytes
        .checked_div(peer_count)
        .filter(|bytes| *bytes > 0)
        .ok_or(RuntimeErrorCode::LimitExceeded)?;
    let provenance = super::job_delivery::executable_provenance()
        .await
        .map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    let mut nonce = [0_u8; 32];
    SysRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    let request = NetworkPerformanceRequest {
        organization_name: input.organization_name,
        cluster_name: input.cluster_name,
        device_name: input.device_name,
        run_uid: input.run_uid,
        artifact_uid: input.artifact_uid,
        schema_version: NETWORK_SCHEMA_VERSION,
        capability: NETWORK_CAPABILITY.to_owned(),
        consent: LocalNetworkConsent {
            consent_uid: input.consent_uid,
            policy_revision: input.policy_revision,
            expires_at_unix: input.consent_expires_at_unix,
            confirmed: input.acknowledge_l1,
        },
        produced_at_unix: unix_now().map_err(|_| RuntimeErrorCode::CollectionFailed)?,
        expires_at_unix: input.expires_at_unix,
        nonce,
        duration: Duration::from_millis(input.duration_millis),
        peer_aliases,
        traffic_bytes_per_peer,
        provenance: NetworkProvenance::new(
            provenance.source_commit(),
            provenance.executable_sha256(),
            provenance.rustfs_version(),
            provenance.build_features().to_vec(),
        ),
    };
    let measurement = measure_network(&request, cancel).await.map_err(network_error)?;
    match measurement.result.outcome() {
        NetworkOutcome::Succeeded | NetworkOutcome::Partial => {
            sign_network_export(&request, &measurement, &key, cancel).map_err(network_error)
        }
        NetworkOutcome::Cancelled => Err(RuntimeErrorCode::Cancelled),
        NetworkOutcome::Unsupported => Err(RuntimeErrorCode::SourceUnavailable),
        NetworkOutcome::Failed => Err(RuntimeErrorCode::CollectionFailed),
    }
}

fn network_error(error: super::perf_network::NetworkPerformanceError) -> RuntimeErrorCode {
    use super::perf_network::NetworkPerformanceError;
    match error {
        NetworkPerformanceError::ConsentRequired => RuntimeErrorCode::ConsentRequired,
        NetworkPerformanceError::ConsentExpired => RuntimeErrorCode::ConsentExpired,
        NetworkPerformanceError::Expired => RuntimeErrorCode::Expired,
        NetworkPerformanceError::LimitExceeded => RuntimeErrorCode::LimitExceeded,
        NetworkPerformanceError::Cancelled => RuntimeErrorCode::Cancelled,
        NetworkPerformanceError::Busy => RuntimeErrorCode::Busy,
        NetworkPerformanceError::InvalidRequest
        | NetworkPerformanceError::UnsupportedCapability
        | NetworkPerformanceError::UnsupportedVersion => RuntimeErrorCode::InvalidRequest,
        _ => RuntimeErrorCode::CollectionFailed,
    }
}

async fn capture_local_health(
    state_root: &Path,
    protocol_version: u16,
    input: LocalHealthRequest,
    cancel: &CancellationToken,
) -> Result<super::health::SignedHealthExport, RuntimeErrorCode> {
    use super::health::{HEALTH_SERVICE_CAPABILITY, HealthServiceRequest, LocalHealthConsent};
    use rand::{TryRng as _, rngs::SysRng};
    if protocol_version != PROTOCOL_VERSION
        || input.offline_key_id.len() != 64
        || !input
            .offline_key_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || input.schema_version != 1
        || input.capability != HEALTH_SERVICE_CAPABILITY
    {
        return Err(RuntimeErrorCode::InvalidRequest);
    }
    if cancel.is_cancelled() {
        return Err(RuntimeErrorCode::Cancelled);
    }
    if !input.acknowledge_l0 || input.policy_revision == 0 {
        return Err(RuntimeErrorCode::ConsentRequired);
    }
    let now = unix_now().map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    if input.consent_expires_at_unix <= now || input.expires_at_unix > input.consent_expires_at_unix {
        return Err(RuntimeErrorCode::ConsentExpired);
    }
    if input.expires_at_unix <= now {
        return Err(RuntimeErrorCode::Expired);
    }
    let key = load_offline_key(state_root, &input.offline_key_id)?;
    let provenance = super::job_delivery::executable_provenance()
        .await
        .map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    let mut nonce = [0_u8; 32];
    SysRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    let request = HealthServiceRequest {
        organization_name: input.organization_name,
        cluster_name: input.cluster_name,
        device_name: input.device_name,
        run_uid: input.run_uid,
        artifact_uid: input.artifact_uid,
        schema_version: input.schema_version,
        capability: input.capability,
        consent: LocalHealthConsent {
            consent_uid: input.consent_uid,
            policy_revision: input.policy_revision,
            expires_at_unix: input.consent_expires_at_unix,
            active: input.acknowledge_l0,
        },
        produced_at_unix: unix_now().map_err(|_| RuntimeErrorCode::CollectionFailed)?,
        expires_at_unix: input.expires_at_unix,
        nonce,
        max_evidence_age_seconds: 300,
        provenance,
    };
    super::health::collect_runtime_health(&request, &key, cancel)
        .await
        .map_err(health_error)
}

fn health_error(error: super::health::HealthError) -> RuntimeErrorCode {
    use super::health::HealthError;
    match error {
        HealthError::ConsentRequired => RuntimeErrorCode::ConsentRequired,
        HealthError::ConsentExpired => RuntimeErrorCode::ConsentExpired,
        HealthError::Expired => RuntimeErrorCode::Expired,
        HealthError::LimitExceeded => RuntimeErrorCode::LimitExceeded,
        HealthError::Busy => RuntimeErrorCode::Busy,
        HealthError::Cancelled => RuntimeErrorCode::Cancelled,
        HealthError::Unsupported | HealthError::InvalidRequest => RuntimeErrorCode::InvalidRequest,
        HealthError::SourceUnavailable | HealthError::CollectionFailed | HealthError::Signing | HealthError::Encoding => {
            RuntimeErrorCode::CollectionFailed
        }
    }
}

#[cfg(test)]
async fn capture_local_top_disk(
    state_root: &Path,
    protocol_version: u16,
    input: super::top_disk::LocalTopRequest,
    cancel: &CancellationToken,
) -> Result<super::top_api::SignedTopExport, RuntimeErrorCode> {
    capture_local_top(state_root, protocol_version, input, cancel, LocalTopKind::Disk).await
}

async fn capture_local_top(
    state_root: &Path,
    protocol_version: u16,
    input: super::top_disk::LocalTopRequest,
    cancel: &CancellationToken,
    kind: LocalTopKind,
) -> Result<super::top_api::SignedTopExport, RuntimeErrorCode> {
    use super::top_api::{LocalTopConsent, TOP_CLASSIFICATION, TopCaptureLimits, TopCaptureRequest, TopCaptureScope};
    if protocol_version != PROTOCOL_VERSION
        || input.offline_key_id.len() != 64
        || !input
            .offline_key_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(RuntimeErrorCode::InvalidRequest);
    }
    if cancel.is_cancelled() {
        return Err(RuntimeErrorCode::Cancelled);
    }
    // Reject unconsented, expired and unbounded work before reading a key or executable.
    if !input.acknowledge_l3 || input.policy_revision == 0 {
        return Err(RuntimeErrorCode::ConsentRequired);
    }
    let now = unix_now().map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    if input.consent_expires_at_unix <= now {
        return Err(RuntimeErrorCode::ConsentExpired);
    }
    if input.run_expires_at_unix <= now
        || input.window_millis.div_ceil(1000)
            > input
                .run_expires_at_unix
                .min(input.consent_expires_at_unix)
                .saturating_sub(now) as u64
    {
        return Err(RuntimeErrorCode::Expired);
    }
    if input.window_millis == 0
        || input.window_millis > 30_000
        || input.export_validity_seconds == 0
        || input.export_validity_seconds > super::top_api::MAX_TOP_EXPORT_VALIDITY.as_secs()
    {
        return Err(RuntimeErrorCode::LimitExceeded);
    }
    let key = load_offline_key(state_root, &input.offline_key_id)?;
    let provenance = super::job_delivery::executable_provenance()
        .await
        .map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    let request = TopCaptureRequest {
        scope: TopCaptureScope {
            organization_name: input.organization_name,
            cluster_name: input.cluster_name,
            device_name: input.device_name,
            run_uid: input.run_uid,
            artifact_uid: input.artifact_uid,
            policy_revision: input.policy_revision,
            run_expires_at_unix: input.run_expires_at_unix,
            executable_sha256: provenance.executable_sha256().to_owned(),
            build_features: provenance.build_features().to_vec(),
            consent: LocalTopConsent {
                uid: input.consent_uid,
                tool_id: kind.tool_id().to_owned(),
                classification: TOP_CLASSIFICATION.to_owned(),
                active: input.acknowledge_l3,
                expires_at_unix: input.consent_expires_at_unix,
            },
        },
        limits: TopCaptureLimits::default(),
        window: Duration::from_millis(input.window_millis),
        export_validity: Duration::from_secs(input.export_validity_seconds),
    };
    match kind {
        LocalTopKind::Disk => {
            let result = super::top_disk::capture_top_disk(&request, cancel).await.map_err(top_error)?;
            sign_local_top_result(&request, &result, &key, cancel)
        }
        LocalTopKind::Locks => {
            let result = super::top_locks::capture_top_locks(&request, cancel)
                .await
                .map_err(top_error)?;
            sign_local_top_result(&request, &result, &key, cancel)
        }
        LocalTopKind::Api => {
            let result = super::top_api::capture_top_api(&request, super::top_api::TopApiOperation::GetObject, cancel)
                .await
                .map_err(top_error)?;
            sign_local_top_result(&request, &result, &key, cancel)
        }
        LocalTopKind::Rpc => {
            let result = super::top_rpc::capture_top_rpc(&request, cancel).await.map_err(top_error)?;
            sign_local_top_result(&request, &result, &key, cancel)
        }
    }
}

fn sign_local_top_result<T: Serialize>(
    request: &super::top_api::TopCaptureRequest,
    result: &super::top_api::TopResult<T>,
    key: &crate::connect::DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<super::top_api::SignedTopExport, RuntimeErrorCode> {
    if result.outcome == super::top_api::TopOutcome::Cancelled {
        return Err(RuntimeErrorCode::Cancelled);
    }
    if result.outcome == super::top_api::TopOutcome::Unsupported {
        return Err(RuntimeErrorCode::UnsupportedPlatform);
    }
    if result.outcome != super::top_api::TopOutcome::Succeeded {
        return Err(RuntimeErrorCode::CollectionFailed);
    }
    super::top_api::sign_top_export(request, result, key, cancel).map_err(top_error)
}

fn top_error(error: super::top_api::TopCaptureError) -> RuntimeErrorCode {
    use super::top_api::TopCaptureError;
    match error {
        TopCaptureError::ConsentRequired => RuntimeErrorCode::ConsentRequired,
        TopCaptureError::ConsentExpired => RuntimeErrorCode::ConsentExpired,
        TopCaptureError::Expired => RuntimeErrorCode::Expired,
        TopCaptureError::Limits | TopCaptureError::ResultTooLarge => RuntimeErrorCode::LimitExceeded,
        TopCaptureError::Cancelled => RuntimeErrorCode::Cancelled,
        TopCaptureError::Scope | TopCaptureError::ConsentScope => RuntimeErrorCode::InvalidRequest,
        _ => RuntimeErrorCode::CollectionFailed,
    }
}

fn load_offline_key(state_root: &Path, offline_key_id: &str) -> Result<crate::connect::DeviceIdentity, RuntimeErrorCode> {
    let owner = private_state_owner(state_root).map_err(|_| RuntimeErrorCode::IdentityUnavailable)?;
    let key_root = state_root.join("offline");
    let key_directory = std::fs::symlink_metadata(&key_root).map_err(|_| RuntimeErrorCode::IdentityUnavailable)?;
    // Existing offline enrollment creates this subdirectory with the process umask.
    // The state root is 0700; require the child to remain owned and non-writable by others.
    if !key_directory.is_dir()
        || key_directory.file_type().is_symlink()
        || key_directory.uid() != owner
        || key_directory.permissions().mode() & 0o022 != 0
    {
        return Err(RuntimeErrorCode::IdentityUnavailable);
    }
    // IdentityStore::load follows symlinks; this IPC boundary must reject them before reading.
    use std::os::unix::fs::OpenOptionsExt as _;
    let path = crate::connect::OfflineKeyStore::new(state_root).key_path();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| RuntimeErrorCode::IdentityUnavailable)?;
    let metadata = file.metadata().map_err(|_| RuntimeErrorCode::IdentityUnavailable)?;
    if !metadata.is_file()
        || metadata.uid() != owner
        || metadata.permissions().mode() & 0o7777 != 0o600
        || metadata.len() == 0
        || metadata.len() > 4_096
    {
        return Err(RuntimeErrorCode::IdentityUnavailable);
    }
    let mut der = zeroize::Zeroizing::new(Vec::new());
    std::io::Read::read_to_end(&mut std::io::Read::take(file, 4_097), &mut der)
        .map_err(|_| RuntimeErrorCode::IdentityUnavailable)?;
    if der.len() > 4_096 {
        return Err(RuntimeErrorCode::IdentityUnavailable);
    }
    let key = crate::connect::DeviceIdentity::from_pkcs8_der(&der).map_err(|_| RuntimeErrorCode::IdentityUnavailable)?;
    if hex_simd::encode_to_string(Sha256::digest(key.public_key_der()), hex_simd::AsciiCase::Lower) != offline_key_id {
        return Err(RuntimeErrorCode::IdentityUnavailable);
    }
    Ok(key)
}

pub(crate) fn load_selected_offline_key(
    state_root: &Path,
    offline_key_id: &str,
) -> Result<crate::connect::DeviceIdentity, LocalTraceCaptureError> {
    if offline_key_id.len() != 64
        || !offline_key_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(LocalTraceCaptureError::OfflineIdentity);
    }
    load_offline_key(state_root, offline_key_id).map_err(|_| LocalTraceCaptureError::OfflineIdentity)
}

async fn capture_local_runtime_profile(
    state_root: &Path,
    protocol_version: u16,
    input: LocalRuntimeProfileRequest,
    cancel: &CancellationToken,
) -> Result<SignedProfileExport, RuntimeErrorCode> {
    use rand::{TryRng as _, rngs::SysRng};
    if protocol_version != PROTOCOL_VERSION
        || input.offline_key_id.len() != 64
        || !input
            .offline_key_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(RuntimeErrorCode::InvalidRequest);
    }
    if cancel.is_cancelled() {
        return Err(RuntimeErrorCode::Cancelled);
    }
    if !input.acknowledge_l3 || input.policy_revision == 0 {
        return Err(RuntimeErrorCode::ConsentRequired);
    }
    let now = unix_now().map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    if input.consent_expires_at_unix <= now || input.expires_at_unix > input.consent_expires_at_unix {
        return Err(RuntimeErrorCode::ConsentExpired);
    }
    if input.expires_at_unix <= now || input.expires_at_unix.saturating_sub(now) > super::profile_cpu::MAX_VALIDITY_SECONDS {
        return Err(RuntimeErrorCode::Expired);
    }
    if input.duration_millis == 0
        || input.duration_millis > 30_000
        || input.sample_period_micros == 0
        || input.sample_period_micros > input.duration_millis * 1_000
    {
        return Err(RuntimeErrorCode::LimitExceeded);
    }
    let tool = match input.capability.as_str() {
        super::profile_cpu::THREAD_PROFILE_CAPABILITY => ProfileTool::Threads,
        super::profile_cpu::MEMORY_PROFILE_CAPABILITY => ProfileTool::Memory,
        super::profile_cpu::CPU_PROFILE_CAPABILITY => ProfileTool::Cpu,
        _ => return Err(RuntimeErrorCode::InvalidRequest),
    };
    if input.schema_version != 1 {
        return Err(RuntimeErrorCode::InvalidRequest);
    }
    let key = load_offline_key(state_root, &input.offline_key_id)?;
    let provenance = super::job_delivery::executable_provenance()
        .await
        .map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    let mut nonce = [0_u8; 32];
    SysRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    let request = ProfileCaptureRequest {
        organization_name: input.organization_name,
        cluster_name: input.cluster_name,
        device_name: input.device_name,
        run_uid: input.run_uid,
        artifact_uid: input.artifact_uid,
        schema_version: input.schema_version,
        capability: input.capability,
        consent: LocalProfileConsent {
            consent_uid: input.consent_uid,
            policy_revision: input.policy_revision,
            expires_at_unix: input.consent_expires_at_unix,
            confirmed: input.acknowledge_l3,
        },
        produced_at_unix: unix_now().map_err(|_| RuntimeErrorCode::CollectionFailed)?,
        expires_at_unix: input.expires_at_unix,
        nonce,
        duration: Duration::from_millis(input.duration_millis),
        sample_period: Duration::from_micros(input.sample_period_micros),
        provenance,
    };
    match tool {
        ProfileTool::Memory => super::profile_memory::export_memory_profile(&request, &key, cancel)
            .await
            .map_err(RuntimeErrorCode::from),
        ProfileTool::Threads => {
            let metrics = tokio::runtime::Handle::current().metrics();
            let cancel = cancel.clone();
            // Always await this task, including after disconnect, so the lease is released before acknowledgement.
            tokio::task::spawn_blocking(move || {
                let result = super::profile_threads::capture_runtime_profile(&request, &metrics, MAX_PROFILE_DURATION, &cancel)?;
                if result.outcome() != ProfileOutcome::Succeeded {
                    return Err(ProfileError::CollectionFailed);
                }
                encode_signed_profile_export(&request, &result, &key, &cancel)
            })
            .await
            .map_err(|_| RuntimeErrorCode::CollectionFailed)?
            .map_err(RuntimeErrorCode::from)
        }
        ProfileTool::Cpu => {
            let export = super::profile_cpu::export_cpu_profile(&request, &key, cancel)
                .await
                .map_err(RuntimeErrorCode::from)?;
            if export.outcome == ProfileOutcome::Unsupported {
                return Err(RuntimeErrorCode::SourceUnavailable);
            }
            Ok(export)
        }
    }
}

async fn capture_local_native_threads_profile(
    state_root: &Path,
    protocol_version: u16,
    input: LocalRuntimeProfileRequest,
    cancel: &CancellationToken,
) -> Result<SignedProfileExport, RuntimeErrorCode> {
    use rand::{TryRng as _, rngs::SysRng};
    if protocol_version != PROTOCOL_VERSION
        || input.offline_key_id.len() != 64
        || !input
            .offline_key_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || input.schema_version != 1
        || input.capability != super::profile_cpu::THREAD_PROFILE_CAPABILITY
    {
        return Err(RuntimeErrorCode::InvalidRequest);
    }
    if cancel.is_cancelled() {
        return Err(RuntimeErrorCode::Cancelled);
    }
    if !input.acknowledge_l3 || input.policy_revision == 0 {
        return Err(RuntimeErrorCode::ConsentRequired);
    }
    let now = unix_now().map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    if input.consent_expires_at_unix <= now || input.expires_at_unix > input.consent_expires_at_unix {
        return Err(RuntimeErrorCode::ConsentExpired);
    }
    if input.expires_at_unix <= now || input.expires_at_unix.saturating_sub(now) > super::profile_cpu::MAX_VALIDITY_SECONDS {
        return Err(RuntimeErrorCode::Expired);
    }
    if input.duration_millis == 0
        || input.duration_millis > 30_000
        || input.sample_period_micros == 0
        || input.sample_period_micros > input.duration_millis * 1_000
    {
        return Err(RuntimeErrorCode::LimitExceeded);
    }
    let key = load_offline_key(state_root, &input.offline_key_id)?;
    let provenance = super::job_delivery::executable_provenance()
        .await
        .map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    let mut nonce = [0_u8; 32];
    SysRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| RuntimeErrorCode::CollectionFailed)?;
    let request = ProfileCaptureRequest {
        organization_name: input.organization_name,
        cluster_name: input.cluster_name,
        device_name: input.device_name,
        run_uid: input.run_uid,
        artifact_uid: input.artifact_uid,
        schema_version: input.schema_version,
        capability: input.capability,
        consent: LocalProfileConsent {
            consent_uid: input.consent_uid,
            policy_revision: input.policy_revision,
            expires_at_unix: input.consent_expires_at_unix,
            confirmed: input.acknowledge_l3,
        },
        produced_at_unix: unix_now().map_err(|_| RuntimeErrorCode::CollectionFailed)?,
        expires_at_unix: input.expires_at_unix,
        nonce,
        duration: Duration::from_millis(input.duration_millis),
        sample_period: Duration::from_micros(input.sample_period_micros),
        provenance,
    };
    let cancel = cancel.clone();
    // The blocking collector runs in this serving process; joining it also releases its lease.
    tokio::task::spawn_blocking(move || {
        let result = super::profile_threads::capture_thread_profile(
            &request,
            super::profile_cpu::ThreadProfileScope::NativeThreads,
            &cancel,
        )?;
        if result.outcome() != ProfileOutcome::Succeeded {
            return Err(ProfileError::CollectionFailed);
        }
        encode_signed_profile_export(&request, &result, &key, &cancel)
    })
    .await
    .map_err(|_| RuntimeErrorCode::CollectionFailed)?
    .map_err(RuntimeErrorCode::from)
}

async fn run_listener(listener: UnixListener, owner: u32, state_root: PathBuf, shutdown: CancellationToken) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    if connections.len() < MAX_CONNECTIONS
                        && stream.peer_cred().is_ok_and(|credentials| credentials.uid() == owner)
                    {
                        connections.spawn(handle_connection(stream, state_root.clone(), shutdown.clone()));
                    }
                }
                Err(_) => break,
            },
        }
    }
    while connections.join_next().await.is_some() {}
}

async fn handle_connection(stream: UnixStream, state_root: PathBuf, shutdown: CancellationToken) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let request = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return,
        result = tokio::time::timeout(REQUEST_TIMEOUT, read_request(&mut reader)) => match result {
            Ok(Ok(request)) => request,
            Ok(Err(_)) | Err(_) => return,
        },
    };
    let (protocol_version, consent_expires_at_unix, duration_millis, max_spans) = match request {
        CaptureRequest::RuntimeProfile {
            protocol_version,
            request,
        } => {
            handle_runtime_profile(reader, writer, &state_root, protocol_version, request, shutdown, false).await;
            return;
        }
        CaptureRequest::NativeThreadsProfile {
            protocol_version,
            request,
        } => {
            handle_runtime_profile(reader, writer, &state_root, protocol_version, request, shutdown, true).await;
            return;
        }
        CaptureRequest::TopDisk {
            protocol_version,
            request,
        } => {
            handle_top_capture(reader, writer, &state_root, protocol_version, request, shutdown, LocalTopKind::Disk).await;
            return;
        }
        CaptureRequest::TopLocks {
            protocol_version,
            request,
        } => {
            handle_top_capture(reader, writer, &state_root, protocol_version, request, shutdown, LocalTopKind::Locks).await;
            return;
        }
        CaptureRequest::TopApi {
            protocol_version,
            request,
        } => {
            handle_top_capture(reader, writer, &state_root, protocol_version, request, shutdown, LocalTopKind::Api).await;
            return;
        }
        CaptureRequest::TopRpc {
            protocol_version,
            request,
        } => {
            handle_top_capture(reader, writer, &state_root, protocol_version, request, shutdown, LocalTopKind::Rpc).await;
            return;
        }
        CaptureRequest::NetworkPerformance {
            protocol_version,
            request,
        } => {
            handle_network_capture(reader, writer, &state_root, protocol_version, request, shutdown).await;
            return;
        }
        CaptureRequest::Health {
            protocol_version,
            request,
        } => {
            handle_health(reader, writer, &state_root, protocol_version, request, shutdown).await;
            return;
        }
        CaptureRequest::TraceRecord {
            protocol_version,
            consent_expires_at_unix,
            duration_millis,
            max_spans,
        } => (protocol_version, consent_expires_at_unix, duration_millis, max_spans),
    };
    let limits = TraceRecordLimits {
        duration: Duration::from_millis(duration_millis),
        max_spans,
    };
    let capture = async {
        if protocol_version != PROTOCOL_VERSION {
            return Err(TelemetryProducerError::SourceUnavailable);
        }
        limits.validate()?;
        let remaining = consent_expires_at_unix
            .checked_sub(unix_now().map_err(|_| TelemetryProducerError::ConsentExpired)?)
            .and_then(|seconds| u64::try_from(seconds).ok())
            .filter(|seconds| *seconds > 0)
            .ok_or(TelemetryProducerError::ConsentExpired)?;
        let expires_at = Instant::now()
            .checked_add(Duration::from_secs(remaining))
            .ok_or(TelemetryProducerError::ConsentExpired)?;
        let consent = LocalTelemetryConsent::new(expires_at)?;
        record_trace_bus(consent, limits, &shutdown).await
    };
    tokio::pin!(capture);
    let mut unexpected = [0_u8; 1];
    let disconnected = reader.read(&mut unexpected);
    tokio::pin!(disconnected);
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => Err(TelemetryProducerError::Cancelled),
        _ = &mut disconnected => return,
        result = &mut capture => result,
    };
    let response = match result {
        Ok(capture) => CaptureResponse::Ok { capture },
        Err(error) => CaptureResponse::Error {
            code: ProducerErrorCode::from(&error),
        },
    };
    if let Ok(bytes) = serde_json::to_vec(&response) {
        let write_response = async {
            writer.write_all(&bytes).await?;
            writer.shutdown().await
        };
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {}
            _ = tokio::time::timeout(REQUEST_TIMEOUT, write_response) => {}
        }
    }
}

async fn read_request(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> Result<CaptureRequest, LocalTraceCaptureError> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_REQUEST_BYTES + 1)
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(LocalTraceCaptureError::Io)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_REQUEST_BYTES || bytes.pop() != Some(b'\n') {
        return Err(LocalTraceCaptureError::Protocol);
    }
    let request = serde_json::from_slice(&bytes).map_err(|_| LocalTraceCaptureError::Protocol)?;
    if matches!(request, CaptureRequest::TraceRecord { .. }) && bytes.len() + 1 > 1_024 {
        return Err(LocalTraceCaptureError::Protocol);
    }
    Ok(request)
}

fn private_state_owner(path: &Path) -> Result<u32, LocalTraceCaptureError> {
    let metadata = std::fs::symlink_metadata(path).map_err(LocalTraceCaptureError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() || metadata.permissions().mode() & 0o777 != STATE_DIRECTORY_MODE {
        return Err(LocalTraceCaptureError::StateSecurity);
    }
    Ok(metadata.uid())
}

fn bind_listener(path: &Path, owner: u32) -> Result<UnixListener, LocalTraceCaptureError> {
    match UnixListener::bind(path) {
        Ok(listener) => seal_listener(path, owner, listener),
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            let identity = socket_identity(path, owner)?;
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => Err(LocalTraceCaptureError::RuntimeUnavailable),
                Err(connect_error) if connect_error.kind() == io::ErrorKind::ConnectionRefused => {
                    remove_own_socket(path, identity);
                    let listener = UnixListener::bind(path).map_err(LocalTraceCaptureError::Io)?;
                    seal_listener(path, owner, listener)
                }
                Err(connect_error) => Err(LocalTraceCaptureError::Io(connect_error)),
            }
        }
        Err(error) => Err(LocalTraceCaptureError::Io(error)),
    }
}

fn seal_listener(path: &Path, owner: u32, listener: UnixListener) -> Result<UnixListener, LocalTraceCaptureError> {
    let metadata = std::fs::symlink_metadata(path).map_err(LocalTraceCaptureError::Io)?;
    if !metadata.file_type().is_socket() || metadata.uid() != owner {
        return Err(LocalTraceCaptureError::StateSecurity);
    }
    let identity = (metadata.dev(), metadata.ino());
    let sealed = std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE))
        .map_err(LocalTraceCaptureError::Io)
        .and_then(|()| socket_identity(path, owner));
    match sealed {
        Ok(sealed_identity) if sealed_identity == identity => Ok(listener),
        Ok(_) => Err(LocalTraceCaptureError::StateSecurity),
        Err(error) => {
            drop(listener);
            remove_own_socket(path, identity);
            Err(error)
        }
    }
}

fn socket_identity(path: &Path, owner: u32) -> Result<(u64, u64), LocalTraceCaptureError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => LocalTraceCaptureError::RuntimeUnavailable,
        _ => LocalTraceCaptureError::Io(error),
    })?;
    if !metadata.file_type().is_socket() || metadata.uid() != owner || metadata.permissions().mode() & 0o777 != SOCKET_MODE {
        return Err(LocalTraceCaptureError::StateSecurity);
    }
    Ok((metadata.dev(), metadata.ino()))
}

fn remove_own_socket(path: &Path, identity: (u64, u64)) {
    if std::fs::symlink_metadata(path).is_ok_and(|metadata| (metadata.dev(), metadata.ino()) == identity) {
        let _ = std::fs::remove_file(path);
    }
}

fn unix_now() -> io::Result<i64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).map_err(io::Error::other)?;
    i64::try_from(duration.as_secs()).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use rustfs_common::trace_bus::{
        TelemetryTraceEvent, TelemetryTraceOperation, TelemetryTraceStatus, telemetry_trace_emit,
        telemetry_trace_subscriber_count,
    };
    use serial_test::serial;
    use tokio_util::sync::CancellationToken;

    use super::{LocalTraceCaptureError, request_local_trace_capture, spawn_local_trace_capture_runtime};
    use crate::connect::{TelemetryOperation, TelemetryProducerError, TraceRecordCompletion, TraceRecordLimits};

    async fn wait_for_subscriber() {
        tokio::time::timeout(Duration::from_secs(1), async {
            while telemetry_trace_subscriber_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("telemetry subscriber");
    }

    #[tokio::test]
    #[serial]
    async fn local_runtime_captures_server_process_events() {
        let state = tempfile::tempdir().expect("state");
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).expect("private state");
        let shutdown = CancellationToken::new();
        let runtime = spawn_local_trace_capture_runtime(state.path(), &shutdown).expect("runtime");
        let request_state = state.path().to_path_buf();
        let consent_expiry = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_secs() as i64 + 5;
        let request = tokio::spawn(async move {
            request_local_trace_capture(
                &request_state,
                consent_expiry,
                TraceRecordLimits {
                    duration: Duration::from_millis(40),
                    max_spans: 8,
                },
                &CancellationToken::new(),
            )
            .await
        });
        wait_for_subscriber().await;
        assert!(telemetry_trace_emit(|| TelemetryTraceEvent::new(
            TelemetryTraceOperation::GetObject,
            Duration::from_micros(7),
            TelemetryTraceStatus::Ok,
        )));
        let capture = request.await.expect("request task").expect("capture");
        assert_eq!(capture.data.spans.len(), 1);
        assert_eq!(capture.data.spans[0].operation, TelemetryOperation::GetObject);
        runtime.shutdown().await;
    }

    #[tokio::test]
    #[serial]
    async fn local_runtime_rejects_non_private_state() {
        let state = tempfile::tempdir().expect("state");
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o755)).expect("public state");
        assert!(spawn_local_trace_capture_runtime(state.path(), &CancellationToken::new()).is_err());
    }

    #[tokio::test]
    #[serial]
    async fn local_runtime_enforces_consent_and_span_limits() {
        let state = tempfile::tempdir().expect("state");
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).expect("private state");
        let shutdown = CancellationToken::new();
        let runtime = spawn_local_trace_capture_runtime(state.path(), &shutdown).expect("runtime");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_secs() as i64;

        let expired = request_local_trace_capture(
            state.path(),
            now - 1,
            TraceRecordLimits {
                duration: Duration::from_millis(10),
                max_spans: 1,
            },
            &CancellationToken::new(),
        )
        .await
        .expect_err("expired consent");
        assert!(matches!(
            expired,
            LocalTraceCaptureError::Producer(TelemetryProducerError::ConsentExpired)
        ));

        let request_state = state.path().to_path_buf();
        let request = tokio::spawn(async move {
            request_local_trace_capture(
                &request_state,
                now + 5,
                TraceRecordLimits {
                    duration: Duration::from_secs(1),
                    max_spans: 1,
                },
                &CancellationToken::new(),
            )
            .await
        });
        wait_for_subscriber().await;
        for _ in 0..3 {
            assert!(telemetry_trace_emit(|| TelemetryTraceEvent::new(
                TelemetryTraceOperation::InternalRpc,
                Duration::from_micros(9),
                TelemetryTraceStatus::Error,
            )));
        }
        let capture = request.await.expect("request task").expect("bounded capture");
        assert_eq!(capture.completion, TraceRecordCompletion::LimitExceeded);
        assert_eq!(capture.data.spans.len(), 1);
        assert_eq!(capture.data.dropped_span_count, 2);
        runtime.shutdown().await;
    }

    #[tokio::test]
    #[serial]
    async fn local_runtime_releases_capture_when_the_client_stops() {
        let state = tempfile::tempdir().expect("state");
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).expect("private state");
        let shutdown = CancellationToken::new();
        let runtime = spawn_local_trace_capture_runtime(state.path(), &shutdown).expect("runtime");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_secs() as i64;
        let request_state = state.path().to_path_buf();
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let request = tokio::spawn(async move {
            request_local_trace_capture(
                &request_state,
                now + 5,
                TraceRecordLimits {
                    duration: Duration::from_secs(1),
                    max_spans: 8,
                },
                &task_cancel,
            )
            .await
        });
        wait_for_subscriber().await;
        cancel.cancel();
        assert!(matches!(
            request.await.expect("request task"),
            Err(LocalTraceCaptureError::Producer(TelemetryProducerError::Cancelled))
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while telemetry_trace_subscriber_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server capture stops after disconnect");
        runtime.shutdown().await;
    }
    use sha2::Digest as _;

    fn runtime_request(state: &std::path::Path) -> (super::LocalRuntimeProfileRequest, crate::connect::DeviceIdentity) {
        let key = crate::connect::OfflineKeyStore::new(state).load_or_create().unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let organization = "organizations/019e3ae0-0000-7000-8000-000000000021";
        let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000022");
        (
            super::LocalRuntimeProfileRequest {
                offline_key_id: hex_simd::encode_to_string(
                    sha2::Sha256::digest(key.public_key_der()),
                    hex_simd::AsciiCase::Lower,
                ),
                organization_name: organization.to_owned(),
                cluster_name: cluster.clone(),
                device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000023"),
                run_uid: "019e3ae0-0000-7000-8000-000000000024".to_owned(),
                artifact_uid: "019e3ae0-0000-7000-8000-000000000025".to_owned(),
                schema_version: 1,
                capability: "profile.threads@1".to_owned(),
                consent_uid: "019e3ae0-0000-7000-8000-000000000026".to_owned(),
                policy_revision: 1,
                consent_expires_at_unix: now + 120,
                acknowledge_l3: true,
                expires_at_unix: now + 60,
                duration_millis: 20,
                sample_period_micros: 1000,
            },
            key,
        )
    }

    fn disk_request(state: &std::path::Path) -> super::super::top_disk::LocalTopRequest {
        let (request, _) = runtime_request(state);
        super::super::top_disk::LocalTopRequest {
            offline_key_id: request.offline_key_id,
            organization_name: request.organization_name,
            cluster_name: request.cluster_name,
            device_name: request.device_name,
            run_uid: request.run_uid,
            artifact_uid: request.artifact_uid,
            consent_uid: request.consent_uid,
            policy_revision: request.policy_revision,
            consent_expires_at_unix: request.consent_expires_at_unix,
            acknowledge_l3: true,
            run_expires_at_unix: request.expires_at_unix,
            window_millis: 200,
            export_validity_seconds: 30,
        }
    }

    fn health_request(state: &std::path::Path) -> super::LocalHealthRequest {
        let (request, _) = runtime_request(state);
        super::LocalHealthRequest {
            offline_key_id: request.offline_key_id,
            organization_name: request.organization_name,
            cluster_name: request.cluster_name,
            device_name: request.device_name,
            run_uid: request.run_uid,
            artifact_uid: request.artifact_uid,
            schema_version: 1,
            capability: "health.check.service@1".to_owned(),
            consent_uid: request.consent_uid,
            policy_revision: request.policy_revision,
            consent_expires_at_unix: request.consent_expires_at_unix,
            acknowledge_l0: true,
            expires_at_unix: request.expires_at_unix,
        }
    }

    fn network_request(state: &std::path::Path) -> super::LocalNetworkRequest {
        let (request, _) = runtime_request(state);
        super::LocalNetworkRequest {
            offline_key_id: request.offline_key_id,
            organization_name: request.organization_name,
            cluster_name: request.cluster_name,
            device_name: request.device_name,
            run_uid: request.run_uid,
            artifact_uid: request.artifact_uid,
            consent_uid: request.consent_uid,
            policy_revision: request.policy_revision,
            consent_expires_at_unix: request.consent_expires_at_unix,
            acknowledge_l1: true,
            expires_at_unix: request.expires_at_unix,
            duration_millis: 1_000,
            traffic_bytes: 65_536,
        }
    }

    #[test]
    fn selected_offline_key_never_falls_back_or_follows_unsafe_state() {
        use std::os::unix::fs::symlink;

        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (request, key) = runtime_request(state.path());
        let store = crate::connect::OfflineKeyStore::new(state.path());
        assert_eq!(
            super::load_selected_offline_key(state.path(), &request.offline_key_id)
                .unwrap()
                .public_key_der(),
            key.public_key_der()
        );
        assert!(super::load_selected_offline_key(state.path(), &"0".repeat(64)).is_err());

        let path = store.key_path();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(super::load_selected_offline_key(state.path(), &request.offline_key_id).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(super::load_selected_offline_key(state.path(), &request.offline_key_id).is_err());
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();

        let target = state.path().join("key-target");
        std::fs::rename(&path, &target).unwrap();
        crate::connect::IdentityStore::new(state.path().join("identity"))
            .load_or_create()
            .unwrap();
        assert!(super::load_selected_offline_key(state.path(), &request.offline_key_id).is_err());
        symlink(&target, &path).unwrap();
        assert!(super::load_selected_offline_key(state.path(), &request.offline_key_id).is_err());
        std::fs::remove_file(&path).unwrap();
        std::fs::rename(&target, &path).unwrap();
        std::fs::set_permissions(state.path().join("offline"), std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(super::load_selected_offline_key(state.path(), &request.offline_key_id).is_err());
        std::fs::set_permissions(state.path().join("offline"), std::fs::Permissions::from_mode(0o755)).unwrap();
        let linked_root = state.path().join("linked-root");
        symlink(state.path(), &linked_root).unwrap();
        assert!(super::load_selected_offline_key(&linked_root, &request.offline_key_id).is_err());
    }

    #[tokio::test]
    #[serial]
    async fn local_network_rejects_unapproved_expired_and_unbounded_work_before_using_key() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut request = network_request(state.path());
        std::fs::remove_file(crate::connect::OfflineKeyStore::new(state.path()).key_path()).unwrap();
        request.acknowledge_l1 = false;
        assert!(matches!(
            super::capture_local_network(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        request.acknowledge_l1 = true;
        request.consent_expires_at_unix = 1;
        assert!(matches!(
            super::capture_local_network(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::ConsentExpired)
        ));
        request.consent_expires_at_unix = request.expires_at_unix + 60;
        request.traffic_bytes = 1_048_577;
        assert!(matches!(
            super::capture_local_network(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::LimitExceeded)
        ));
        request.duration_millis = 1;
        request.traffic_bytes = 2_000;
        assert!(matches!(
            super::capture_local_network(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::LimitExceeded)
        ));
        request.duration_millis = 30_001;
        request.traffic_bytes = 65_536;
        assert!(matches!(
            super::capture_local_network(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::LimitExceeded)
        ));
        request.duration_millis = 1_000;
        request.traffic_bytes = 65_536;
        assert!(matches!(
            super::capture_local_network(state.path(), 1, request, &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
    }

    #[tokio::test]
    async fn local_network_wire_rejects_caller_selected_peer_address() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let request = network_request(state.path());
        let mut wire = serde_json::to_value(request).unwrap();
        wire["peerAddress"] = serde_json::json!("example.com");
        assert!(serde_json::from_value::<super::LocalNetworkRequest>(wire).is_err());
    }

    #[tokio::test]
    #[serial]
    async fn local_network_service_fails_closed_without_cluster_peers() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let request = network_request(state.path());
        let runtime = spawn_local_trace_capture_runtime(state.path(), &CancellationToken::new()).unwrap();
        assert!(matches!(
            super::request_local_network(state.path(), request, &CancellationToken::new()).await,
            Err(LocalTraceCaptureError::Network(code)) if code == "SourceUnavailable"
        ));
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn local_health_requires_consent_bounds_and_separate_offline_identity() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut request = health_request(state.path());
        std::fs::remove_file(crate::connect::OfflineKeyStore::new(state.path()).key_path()).unwrap();
        request.acknowledge_l0 = false;
        assert!(matches!(
            super::capture_local_health(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        request.acknowledge_l0 = true;
        let valid_consent_expiry = request.consent_expires_at_unix;
        request.consent_expires_at_unix = request.expires_at_unix - 1;
        assert!(matches!(
            super::capture_local_health(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::ConsentExpired)
        ));
        request.consent_expires_at_unix = valid_consent_expiry;
        let valid_expiry = request.expires_at_unix;
        request.expires_at_unix = request.consent_expires_at_unix - 121;
        assert!(matches!(
            super::capture_local_health(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::Expired)
        ));
        request.expires_at_unix = valid_expiry;
        assert!(matches!(
            super::capture_local_health(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
        assert!(
            super::request_local_health(state.path(), request.clone(), &CancellationToken::new())
                .await
                .is_err()
        );
        let mut value = serde_json::to_value(&request).unwrap();
        value["command"] = serde_json::json!("shell.exec");
        assert!(serde_json::from_value::<super::LocalHealthRequest>(value).is_err());
    }

    #[tokio::test]
    async fn local_top_disk_rejects_invalid_requests_without_identity_or_socket_fallback() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut request = disk_request(state.path());
        std::fs::remove_file(crate::connect::OfflineKeyStore::new(state.path()).key_path()).unwrap();
        request.acknowledge_l3 = false;
        assert!(matches!(
            super::capture_local_top_disk(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        request.acknowledge_l3 = true;
        request.window_millis = 30_001;
        assert!(matches!(
            super::capture_local_top_disk(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::LimitExceeded)
        ));
        request.window_millis = 200;
        assert!(matches!(
            super::capture_local_top_disk(state.path(), 1, request.clone(), &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
        assert!(
            super::request_local_top_disk(state.path(), request.clone(), &CancellationToken::new())
                .await
                .is_err()
        );
        let mut value = serde_json::to_value(&request).unwrap();
        value["pid"] = serde_json::json!(1);
        assert!(serde_json::from_value::<super::super::top_disk::LocalTopRequest>(value).is_err());
        assert!(
            serde_json::from_str::<super::CaptureRequest>(
                r#"{"operation":"TOP_DISK","protocolVersion":1,"protocolVersion":1,"request":{}}"#
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn local_top_locks_rejects_consent_expiry_and_missing_offline_key() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut request = disk_request(state.path());
        std::fs::remove_file(crate::connect::OfflineKeyStore::new(state.path()).key_path()).unwrap();
        request.acknowledge_l3 = false;
        assert!(matches!(
            super::capture_local_top(state.path(), 1, request.clone(), &CancellationToken::new(), super::LocalTopKind::Locks)
                .await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        request.acknowledge_l3 = true;
        request.consent_expires_at_unix = 1;
        assert!(matches!(
            super::capture_local_top(state.path(), 1, request.clone(), &CancellationToken::new(), super::LocalTopKind::Locks)
                .await,
            Err(super::RuntimeErrorCode::ConsentExpired)
        ));
        request.consent_expires_at_unix = request.run_expires_at_unix + 60;
        assert!(matches!(
            super::capture_local_top(state.path(), 1, request.clone(), &CancellationToken::new(), super::LocalTopKind::Locks)
                .await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
        assert!(
            super::request_local_top_locks(state.path(), request.clone(), &CancellationToken::new())
                .await
                .is_err()
        );
        let mut value = serde_json::to_value(&request).unwrap();
        value["pid"] = serde_json::json!(1);
        assert!(serde_json::from_value::<super::super::top_disk::LocalTopRequest>(value).is_err());
    }

    #[tokio::test]
    async fn local_top_api_rejects_consent_expiry_and_missing_offline_key() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut request = disk_request(state.path());
        std::fs::remove_file(crate::connect::OfflineKeyStore::new(state.path()).key_path()).unwrap();
        request.acknowledge_l3 = false;
        assert!(matches!(
            super::capture_local_top(state.path(), 1, request.clone(), &CancellationToken::new(), super::LocalTopKind::Api).await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        request.acknowledge_l3 = true;
        request.consent_expires_at_unix = 1;
        assert!(matches!(
            super::capture_local_top(state.path(), 1, request.clone(), &CancellationToken::new(), super::LocalTopKind::Api).await,
            Err(super::RuntimeErrorCode::ConsentExpired)
        ));
        request.consent_expires_at_unix = request.run_expires_at_unix + 60;
        assert!(matches!(
            super::capture_local_top(state.path(), 1, request, &CancellationToken::new(), super::LocalTopKind::Api).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
    }

    #[tokio::test]
    async fn local_top_rpc_rejects_consent_expiry_and_missing_offline_key() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut request = disk_request(state.path());
        std::fs::remove_file(crate::connect::OfflineKeyStore::new(state.path()).key_path()).unwrap();
        request.acknowledge_l3 = false;
        assert!(matches!(
            super::capture_local_top(state.path(), 1, request.clone(), &CancellationToken::new(), super::LocalTopKind::Rpc).await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        request.acknowledge_l3 = true;
        request.consent_expires_at_unix = 1;
        assert!(matches!(
            super::capture_local_top(state.path(), 1, request.clone(), &CancellationToken::new(), super::LocalTopKind::Rpc).await,
            Err(super::RuntimeErrorCode::ConsentExpired)
        ));
        request.consent_expires_at_unix = request.run_expires_at_unix + 60;
        assert!(matches!(
            super::capture_local_top(state.path(), 1, request, &CancellationToken::new(), super::LocalTopKind::Rpc).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
    }

    #[test]
    fn local_top_rpc_service_child() {
        use bytes::Bytes;
        use http::{Method, Request, Response, StatusCode};
        use http_body_util::{BodyExt as _, Empty};
        use hyper::{client::conn::http1 as client_http1, server::conn::http1 as server_http1};
        use hyper_util::{rt::TokioIo, service::TowerToHyperService};
        use std::{convert::Infallible, io::Read as _};
        use tokio::net::{TcpListener, TcpStream};

        let Some(state) = std::env::var_os("RUSTFS_TEST_TOP_RPC_STATE") else {
            return;
        };
        let stop = CancellationToken::new();
        let input_stop = stop.clone();
        std::thread::spawn(move || {
            let _ = std::io::stdin().read(&mut [0u8]);
            input_stop.cancel();
        });
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            rustfs_credentials::set_global_rpc_secret("top-rpc-local-test-secret".to_owned()).unwrap();
            let runtime = spawn_local_trace_capture_runtime(std::path::Path::new(&state), &stop).unwrap();
            let emit = async {
                // Executable hashing precedes subscription. The parent's bounded
                // request and stdin cancellation govern this readiness wait.
                while telemetry_trace_subscriber_count() == 0 {
                    tokio::task::yield_now().await;
                }
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let server = tokio::spawn(async move {
                    let (socket, _) = listener.accept().await.unwrap();
                    let fallback = tower::service_fn(|_| async {
                        Ok::<_, Infallible>(Response::new(crate::storage_api::server::http::rpc::Body::empty()))
                    });
                    server_http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(socket),
                            TowerToHyperService::new(crate::storage_api::server::http::rpc::InternodeRpcService::new(fallback)),
                        )
                        .await
                        .unwrap();
                });
                let stream = TcpStream::connect(addr).await.unwrap();
                let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await.unwrap();
                let client = tokio::spawn(async move { connection.await.unwrap() });
                let challenge = uuid::Uuid::new_v4();
                let uri = format!("/rustfs/rpc/put_file_capability?put_file_capability=1&put_file_challenge={challenge}");
                let mut signed_request = Request::builder()
                    .method(Method::GET)
                    .uri(&uri)
                    .header(http::header::HOST, addr.to_string())
                    .body(Empty::<Bytes>::new())
                    .unwrap();
                signed_request
                    .headers_mut()
                    .extend(crate::storage_api::server::http::gen_signature_headers(&uri, &Method::GET).unwrap());
                sender.ready().await.unwrap();
                let success = sender.send_request(signed_request).await.unwrap();
                assert_eq!(success.status(), StatusCode::OK);
                success.into_body().collect().await.unwrap();
                sender.ready().await.unwrap();
                let rejected = sender
                    .send_request(
                        Request::builder()
                            .method(Method::GET)
                            .uri(&uri)
                            .header(http::header::HOST, addr.to_string())
                            .body(Empty::<Bytes>::new())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert!(rejected.status().is_client_error());
                rejected.into_body().collect().await.unwrap();
                sender.ready().await.unwrap();
                let unrelated = sender
                    .send_request(Request::builder().uri("/not-rpc").body(Empty::<Bytes>::new()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(unrelated.status(), StatusCode::OK);
                unrelated.into_body().collect().await.unwrap();
                drop(sender);
                client.await.unwrap();
                server.await.unwrap();
            };
            tokio::select! {
                _ = stop.cancelled() => {},
                _ = emit => stop.cancelled().await,
            }
            runtime.shutdown().await;
        });
    }

    #[tokio::test]
    #[serial]
    async fn local_top_rpc_reads_service_http_completions_and_signs_offline() {
        use std::io::Read as _;
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut request = disk_request(state.path());
        request.window_millis = 500;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "connect::diagnostics::trace_runtime::tests::local_top_rpc_service_child",
                "--exact",
                "--nocapture",
            ])
            .env("RUSTFS_TEST_TOP_RPC_STATE", state.path())
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !state.path().join(super::SOCKET_FILE).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let result = if ready.is_ok() {
            super::request_local_top_rpc(state.path(), request.clone(), &CancellationToken::new()).await
        } else {
            Err(LocalTraceCaptureError::Protocol)
        };
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success(), "capture error: {:?}", result.as_ref().err());
        let export = result.unwrap();
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&export.archive_bytes)).unwrap();
        let result: serde_json::Value = serde_json::from_reader(zip.by_name("result.json").unwrap()).unwrap();
        assert_eq!(result["toolId"], "top.rpc");
        assert_eq!(result["outcome"], "SUCCEEDED");
        assert_eq!(result["runUid"], request.run_uid);
        assert_eq!(result["data"]["requestCount"], 2);
        assert_eq!(result["data"]["errorCount"], 1);
        let mut envelope = Vec::new();
        zip.by_name("envelope.json").unwrap().read_to_end(&mut envelope).unwrap();
        let envelope_value: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
        assert_eq!(envelope_value["deviceKeyId"], request.offline_key_id);
        assert_eq!(envelope_value["organizationName"], request.organization_name);
        assert_eq!(envelope_value["clusterName"], request.cluster_name);
        assert_eq!(envelope_value["deviceName"], request.device_name);
        assert_eq!(envelope_value["classification"], "L3");
        let key = super::load_offline_key(state.path(), &request.offline_key_id).unwrap();
        let signature: serde_json::Value = serde_json::from_reader(zip.by_name("envelope.sig").unwrap()).unwrap();
        let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
        signed.extend_from_slice(&envelope);
        assert!(key.verifies_pending_registration_state(&signed, signature["value"].as_str().unwrap()));
    }

    #[test]
    fn local_top_api_service_child() {
        use rustfs_io_metrics::record_s3_op;
        use rustfs_s3_ops::S3Operation;
        use std::io::Read as _;

        let Some(state) = std::env::var_os("RUSTFS_TEST_TOP_API_STATE") else {
            return;
        };
        let stop = CancellationToken::new();
        let input_stop = stop.clone();
        std::thread::spawn(move || {
            let _ = std::io::stdin().read(&mut [0u8]);
            input_stop.cancel();
        });
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let runtime = spawn_local_trace_capture_runtime(std::path::Path::new(&state), &stop).unwrap();
            let emit = async {
                // Executable hashing precedes subscription. The parent's bounded
                // request and stdin cancellation govern this readiness wait.
                while telemetry_trace_subscriber_count() == 0 {
                    tokio::task::yield_now().await;
                }
                for (operation, status) in [
                    (S3Operation::GetObject, 200),
                    (S3Operation::GetObject, 503),
                    (S3Operation::PutObject, 500),
                ] {
                    let mut guard = crate::server::s3_http_request_guard("GET");
                    guard.in_scope(|| record_s3_op(operation));
                    tokio::task::yield_now().await;
                    guard.response(status);
                }
            };
            tokio::select! {
                _ = stop.cancelled() => {},
                _ = emit => stop.cancelled().await,
            }
            runtime.shutdown().await;
        });
    }

    #[tokio::test]
    #[serial]
    async fn local_top_api_reads_service_s3_events_and_signs_offline() {
        use std::io::Read as _;
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut request = disk_request(state.path());
        request.window_millis = 500;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "connect::diagnostics::trace_runtime::tests::local_top_api_service_child",
                "--exact",
                "--nocapture",
            ])
            .env("RUSTFS_TEST_TOP_API_STATE", state.path())
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !state.path().join(super::SOCKET_FILE).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let result = if ready.is_ok() {
            super::request_local_top_api(state.path(), request.clone(), &CancellationToken::new()).await
        } else {
            Err(LocalTraceCaptureError::Protocol)
        };
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success(), "capture error: {:?}", result.as_ref().err());
        let export = result.unwrap();
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&export.archive_bytes)).unwrap();
        let result: serde_json::Value = serde_json::from_reader(zip.by_name("result.json").unwrap()).unwrap();
        assert_eq!(result["toolId"], "top.api");
        assert_eq!(result["outcome"], "SUCCEEDED");
        assert_eq!(result["runUid"], request.run_uid);
        assert_eq!(result["data"]["operation"], "GET_OBJECT");
        assert_eq!(result["data"]["requestCount"], 2);
        assert_eq!(result["data"]["errorCount"], 1);
        let mut envelope = Vec::new();
        zip.by_name("envelope.json").unwrap().read_to_end(&mut envelope).unwrap();
        let envelope_value: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
        assert_eq!(envelope_value["deviceKeyId"], request.offline_key_id);
        assert_eq!(envelope_value["organizationName"], request.organization_name);
        assert_eq!(envelope_value["clusterName"], request.cluster_name);
        assert_eq!(envelope_value["deviceName"], request.device_name);
        assert_eq!(envelope_value["classification"], "L3");
        let key = super::load_offline_key(state.path(), &request.offline_key_id).unwrap();
        let signature: serde_json::Value = serde_json::from_reader(zip.by_name("envelope.sig").unwrap()).unwrap();
        let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
        signed.extend_from_slice(&envelope);
        assert!(key.verifies_pending_registration_state(&signed, signature["value"].as_str().unwrap()));
    }

    #[test]
    fn local_top_locks_service_child() {
        use std::io::Read as _;
        let Some(state) = std::env::var_os("RUSTFS_TEST_TOP_LOCKS_STATE") else {
            return;
        };
        let stop = CancellationToken::new();
        let input_stop = stop.clone();
        std::thread::spawn(move || {
            let _ = std::io::stdin().read(&mut [0u8]);
            input_stop.cancel();
        });
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            assert!(rustfs_lock::get_global_lock_manager().as_fast_lock_manager().is_some());
            let runtime = spawn_local_trace_capture_runtime(std::path::Path::new(&state), &stop).unwrap();
            stop.cancelled().await;
            runtime.shutdown().await;
        });
    }

    #[tokio::test]
    #[serial]
    async fn local_top_locks_reads_service_manager_and_signs_offline() {
        use std::io::Read as _;
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let request = disk_request(state.path());
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "connect::diagnostics::trace_runtime::tests::local_top_locks_service_child",
                "--exact",
                "--nocapture",
            ])
            .env("RUSTFS_TEST_TOP_LOCKS_STATE", state.path())
            .env("RUSTFS_LOCK_ENABLED", "true")
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !state.path().join(super::SOCKET_FILE).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let result = if ready.is_ok() {
            super::request_local_top_locks(state.path(), request.clone(), &CancellationToken::new()).await
        } else {
            Err(LocalTraceCaptureError::Protocol)
        };
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success());
        let export = result.unwrap();
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&export.archive_bytes)).unwrap();
        let result: serde_json::Value = serde_json::from_reader(zip.by_name("result.json").unwrap()).unwrap();
        assert_eq!(result["toolId"], "top.locks");
        assert_eq!(result["outcome"], "SUCCEEDED");
        assert_eq!(result["data"]["heldCount"], 0);
        assert_eq!(result["data"]["waitingCount"], 0);
        let mut envelope = Vec::new();
        zip.by_name("envelope.json").unwrap().read_to_end(&mut envelope).unwrap();
        let envelope_value: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
        assert_eq!(envelope_value["deviceKeyId"], request.offline_key_id);
        assert_eq!(envelope_value["classification"], "L3");
        let key = super::load_offline_key(state.path(), &request.offline_key_id).unwrap();
        let signature: serde_json::Value = serde_json::from_reader(zip.by_name("envelope.sig").unwrap()).unwrap();
        let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
        signed.extend_from_slice(&envelope);
        assert!(key.verifies_pending_registration_state(&signed, signature["value"].as_str().unwrap()));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn local_top_disk_service_child() {
        use std::io::{Read as _, Write as _};
        let Some(state) = std::env::var_os("RUSTFS_TEST_TOP_DISK_STATE") else {
            return;
        };
        super::super::top_disk::AFTER_INITIAL_DISK_SNAPSHOT
            .set(|| {
                // Keep real writes between the collector's snapshots in this dedicated child.
                let mut file = tempfile::tempfile().expect("create top.disk fixture file");
                for _ in 0..32 {
                    file.write_all(&[1u8; 4096]).expect("write top.disk fixture data");
                }
                file.sync_all().expect("flush top.disk fixture data");
            })
            .expect("install top.disk fixture writer once");
        let state = std::path::PathBuf::from(state);
        let stop = CancellationToken::new();
        let input_stop = stop.clone();
        std::thread::spawn(move || {
            let _ = std::io::stdin().read(&mut [0u8]);
            input_stop.cancel();
        });
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let runtime = spawn_local_trace_capture_runtime(&state, &stop).unwrap();
            stop.cancelled().await;
            runtime.shutdown().await;
        });
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[serial]
    async fn local_top_disk_measures_service_process_and_signs_offline() {
        use std::io::Read as _;
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let request = disk_request(state.path());
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "connect::diagnostics::trace_runtime::tests::local_top_disk_service_child",
                "--exact",
                "--nocapture",
            ])
            .env("RUSTFS_TEST_TOP_DISK_STATE", state.path())
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !state.path().join(super::SOCKET_FILE).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let result = if ready.is_ok() {
            super::request_local_top_disk(state.path(), request.clone(), &CancellationToken::new()).await
        } else {
            Err(LocalTraceCaptureError::Protocol)
        };
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success());
        let export = result.unwrap();
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&export.archive_bytes)).unwrap();
        let result: serde_json::Value = serde_json::from_reader(zip.by_name("result.json").unwrap()).unwrap();
        assert_eq!(result["outcome"], "SUCCEEDED");
        assert!(result["data"]["writeBytes"].as_u64().unwrap() >= 4096);
        assert!(result["data"]["ioCount"].as_u64().unwrap() >= 20);
        let mut envelope = Vec::new();
        zip.by_name("envelope.json").unwrap().read_to_end(&mut envelope).unwrap();
        let envelope_value: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
        assert_eq!(envelope_value["deviceKeyId"], request.offline_key_id);
        assert_eq!(envelope_value["classification"], "L3");
        let key = super::load_offline_key(state.path(), &request.offline_key_id).unwrap();
        let signature: serde_json::Value = serde_json::from_reader(zip.by_name("envelope.sig").unwrap()).unwrap();
        let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
        signed.extend_from_slice(&envelope);
        assert!(key.verifies_pending_registration_state(&signed, signature["value"].as_str().unwrap()));
    }

    #[tokio::test]
    #[serial]
    async fn local_runtime_profile_uses_server_workers_and_offline_signature() {
        use std::io::Read as _;
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (request, key) = runtime_request(state.path());
        let state_root = state.path().to_path_buf();
        let stop = CancellationToken::new();
        let server_stop = stop.clone();
        let (ready, wait) = tokio::sync::oneshot::channel();
        let server = std::thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let runtime = spawn_local_trace_capture_runtime(&state_root, &server_stop).unwrap();
                    ready.send(()).unwrap();
                    server_stop.cancelled().await;
                    runtime.shutdown().await;
                });
        });
        wait.await.unwrap();
        let export = super::request_local_runtime_profile(state.path(), request.clone(), &CancellationToken::new())
            .await
            .unwrap();
        stop.cancel();
        server.join().unwrap();
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&export.archive_bytes)).unwrap();
        let result: serde_json::Value = serde_json::from_reader(zip.by_name("result.json").unwrap()).unwrap();
        assert_eq!(tokio::runtime::Handle::current().metrics().num_workers(), 1);
        assert_eq!(result["data"]["workerCount"], 2);
        assert_eq!(result["data"]["samples"].as_array().unwrap().len(), 2);
        assert_eq!(result["provenance"]["sourceCommit"], crate::version::build::COMMIT_HASH);
        let mut binary = std::fs::File::open(std::env::current_exe().unwrap()).unwrap();
        let mut hasher = sha2::Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = binary.read(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        let binary_hash = hasher.finalize();
        assert_eq!(
            result["provenance"]["executableSha256"],
            hex_simd::encode_to_string(binary_hash, hex_simd::AsciiCase::Lower)
        );
        let mut envelope = Vec::new();
        zip.by_name("envelope.json").unwrap().read_to_end(&mut envelope).unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
        assert_eq!(metadata["classification"], "L3");
        assert_eq!(metadata["deviceKeyId"], request.offline_key_id);
        let signature: serde_json::Value = serde_json::from_reader(zip.by_name("envelope.sig").unwrap()).unwrap();
        let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
        signed.extend_from_slice(&envelope);
        assert!(key.verifies_pending_registration_state(&signed, signature["value"].as_str().unwrap()));
        assert!(!state.path().join("identity").exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn local_native_threads_service_child() {
        let Some(state) = std::env::var_os("RUSTFS_TEST_NATIVE_THREADS_STATE") else {
            return;
        };
        let stop = CancellationToken::new();
        let input_stop = stop.clone();
        std::thread::spawn(move || {
            use std::io::Read as _;
            let _ = std::io::stdin().read(&mut [0u8]);
            input_stop.cancel();
        });
        let sleepers: Vec<_> = (0..16)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.is_cancelled() {
                        std::thread::park_timeout(Duration::from_millis(100));
                    }
                })
            })
            .collect();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let runtime = spawn_local_trace_capture_runtime(std::path::Path::new(&state), &stop).unwrap();
            stop.cancelled().await;
            runtime.shutdown().await;
        });
        for sleeper in sleepers {
            sleeper.join().unwrap();
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[serial]
    async fn local_native_threads_profiles_service_process_and_signs_offline() {
        use std::io::Read as _;
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (request, key) = runtime_request(state.path());
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "connect::diagnostics::trace_runtime::tests::local_native_threads_service_child",
                "--exact",
                "--nocapture",
            ])
            .env("RUSTFS_TEST_NATIVE_THREADS_STATE", state.path())
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !state.path().join(super::SOCKET_FILE).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let export = if ready.is_ok() {
            super::request_local_native_threads_profile(state.path(), request.clone(), &CancellationToken::new()).await
        } else {
            Err(LocalTraceCaptureError::Protocol)
        };
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success());
        let export = export.unwrap();
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&export.archive_bytes)).unwrap();
        let result: serde_json::Value = serde_json::from_reader(zip.by_name("result.json").unwrap()).unwrap();
        assert_eq!(result["toolId"], "profile.threads");
        assert_eq!(result["outcome"], "SUCCEEDED");
        assert_eq!(result["data"]["scope"], "NATIVE_THREADS");
        let states = result["data"]["states"].as_array().unwrap();
        assert_eq!(states.len(), 4);
        assert!(states.iter().map(|state| state["threadCount"].as_u64().unwrap()).sum::<u64>() >= 16);
        assert!(result["data"].get("threadNames").is_none());
        let mut envelope = Vec::new();
        zip.by_name("envelope.json").unwrap().read_to_end(&mut envelope).unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
        assert_eq!(metadata["classification"], "L3");
        assert_eq!(metadata["deviceKeyId"], request.offline_key_id);
        let signature: serde_json::Value = serde_json::from_reader(zip.by_name("envelope.sig").unwrap()).unwrap();
        let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
        signed.extend_from_slice(&envelope);
        assert!(key.verifies_pending_registration_state(&signed, signature["value"].as_str().unwrap()));
        assert!(!state.path().join("identity").exists());
    }

    #[tokio::test]
    async fn local_native_threads_rejects_consent_scope_and_missing_identity() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (request, _) = runtime_request(state.path());
        let cancel = CancellationToken::new();
        assert!(matches!(
            super::capture_local_native_threads_profile(state.path(), 2, request.clone(), &cancel).await,
            Err(super::RuntimeErrorCode::InvalidRequest)
        ));
        let mut invalid = request.clone();
        invalid.acknowledge_l3 = false;
        assert!(matches!(
            super::capture_local_native_threads_profile(state.path(), 1, invalid, &cancel).await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        let mut invalid = request.clone();
        invalid.capability = "profile.memory@1".to_owned();
        assert!(matches!(
            super::capture_local_native_threads_profile(state.path(), 1, invalid, &cancel).await,
            Err(super::RuntimeErrorCode::InvalidRequest)
        ));
        let mut invalid = request.clone();
        invalid.expires_at_unix = 1;
        assert!(matches!(
            super::capture_local_native_threads_profile(state.path(), 1, invalid, &cancel).await,
            Err(super::RuntimeErrorCode::Expired)
        ));
        std::fs::remove_file(crate::connect::OfflineKeyStore::new(state.path()).key_path()).unwrap();
        assert!(matches!(
            super::capture_local_native_threads_profile(state.path(), 1, request, &cancel).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
    }

    #[tokio::test]
    #[serial]
    async fn local_memory_profile_signs_service_capture_with_offline_identity() {
        use std::io::Read as _;
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (mut request, key) = runtime_request(state.path());
        request.capability = super::super::profile_cpu::MEMORY_PROFILE_CAPABILITY.to_owned();
        let runtime = spawn_local_trace_capture_runtime(state.path(), &CancellationToken::new()).unwrap();
        let export = super::request_local_runtime_profile(state.path(), request.clone(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(export.tool, super::ProfileTool::Memory);
        runtime.shutdown().await;
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&export.archive_bytes)).unwrap();
        let result: serde_json::Value = serde_json::from_reader(zip.by_name("result.json").unwrap()).unwrap();
        assert_eq!(result["toolId"], "profile.memory");
        assert_eq!(result["outcome"], "SUCCEEDED");
        assert!(result["data"]["allocatedBytes"].is_u64());
        assert!(result["data"]["allocationCount"].is_u64());
        let mut envelope = Vec::new();
        zip.by_name("envelope.json").unwrap().read_to_end(&mut envelope).unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
        assert_eq!(metadata["classification"], "L3");
        assert_eq!(metadata["deviceKeyId"], request.offline_key_id);
        let signature: serde_json::Value = serde_json::from_reader(zip.by_name("envelope.sig").unwrap()).unwrap();
        let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
        signed.extend_from_slice(&envelope);
        assert!(key.verifies_pending_registration_state(&signed, signature["value"].as_str().unwrap()));
        assert!(!state.path().join("identity").exists());
    }

    #[tokio::test]
    #[serial]
    async fn local_cpu_profile_uses_service_capture_and_offline_identity() {
        #[cfg(feature = "pyroscope")]
        use std::io::Read as _;
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (mut request, _key) = runtime_request(state.path());
        request.capability = super::super::profile_cpu::CPU_PROFILE_CAPABILITY.to_owned();
        request.duration_millis = 1_000;
        request.sample_period_micros = 10_000;
        let mut invalid = request.clone();
        invalid.acknowledge_l3 = false;
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, invalid, &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        let mut invalid = request.clone();
        invalid.offline_key_id = "0".repeat(64);
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, invalid, &CancellationToken::new()).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
        let runtime = spawn_local_trace_capture_runtime(state.path(), &CancellationToken::new()).unwrap();
        let export = super::request_local_runtime_profile(state.path(), request.clone(), &CancellationToken::new()).await;
        runtime.shutdown().await;

        #[cfg(not(feature = "pyroscope"))]
        {
            assert!(matches!(
                export,
                Err(LocalTraceCaptureError::RuntimeProfile(ref code)) if code == "SourceUnavailable"
            ));
            return;
        }

        #[cfg(feature = "pyroscope")]
        {
            let export = export.unwrap();
            assert_eq!(export.tool, super::ProfileTool::Cpu);
            assert!(matches!(
                export.outcome,
                super::ProfileOutcome::Succeeded | super::ProfileOutcome::Partial
            ));
            let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&export.archive_bytes)).unwrap();
            let result: serde_json::Value = serde_json::from_reader(zip.by_name("result.json").unwrap()).unwrap();
            assert_eq!(result["toolId"], "profile.cpu");
            assert_eq!(result["outcome"], export.outcome.as_str());
            assert_eq!(result["reasonCode"], export.reason_code.as_str());
            assert!(result["data"]["samples"].is_array());
            let mut envelope = Vec::new();
            zip.by_name("envelope.json").unwrap().read_to_end(&mut envelope).unwrap();
            let metadata: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
            assert_eq!(metadata["classification"], "L3");
            assert_eq!(metadata["deviceKeyId"], request.offline_key_id);
            let signature: serde_json::Value = serde_json::from_reader(zip.by_name("envelope.sig").unwrap()).unwrap();
            let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
            signed.extend_from_slice(&envelope);
            assert!(_key.verifies_pending_registration_state(&signed, signature["value"].as_str().unwrap()));
            assert!(!state.path().join("identity").exists());
        }
    }

    #[tokio::test]
    #[serial]
    async fn local_runtime_profile_listener_checks_real_peer_credentials() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        for same_owner in [false, true] {
            let state = tempfile::tempdir().unwrap();
            std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let (request, _) = runtime_request(state.path());
            let owner = super::private_state_owner(state.path()).unwrap();
            let socket = state.path().join(super::SOCKET_FILE);
            let listener = super::bind_listener(&socket, owner).unwrap();
            let mut client = tokio::net::UnixStream::connect(&socket).await.unwrap();
            assert_eq!(client.peer_cred().unwrap().uid(), owner);
            let mut bytes = serde_json::to_vec(&super::CaptureRequest::RuntimeProfile {
                protocol_version: super::PROTOCOL_VERSION,
                request,
            })
            .unwrap();
            bytes.push(b'\n');
            // Queue the same valid request before either listener checks its real peer UID.
            client.write_all(&bytes).await.unwrap();
            let shutdown = CancellationToken::new();
            let expected_owner = if same_owner { owner } else { owner ^ 1 };
            let server = tokio::spawn(super::run_listener(
                listener,
                expected_owner,
                state.path().to_path_buf(),
                shutdown.clone(),
            ));
            let mut response = Vec::new();
            let read = tokio::time::timeout(Duration::from_secs(30), client.read_to_end(&mut response))
                .await
                .expect("listener must close the connection");
            shutdown.cancel();
            server.await.unwrap();
            if same_owner {
                read.unwrap();
                assert!(matches!(
                    serde_json::from_slice::<super::CaptureResponse>(&response).unwrap(),
                    super::CaptureResponse::RuntimeOk { .. }
                ));
            } else {
                // Unix platforms either report EOF or reset when unread request bytes are discarded.
                if let Err(error) = read {
                    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
                }
                assert!(
                    response.is_empty(),
                    "a foreign peer must receive no signed archive or diagnostic response"
                );
            }
            let lease = super::super::profile_cpu::CollectorLease::acquire().expect("listener leaves no collector lease");
            drop(lease);
        }
    }

    #[tokio::test]
    #[serial]
    async fn local_runtime_profile_rejects_invalid_consent_identity_and_protocol() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (request, _) = runtime_request(state.path());
        let cancel = CancellationToken::new();
        let mut invalid = request.clone();
        invalid.acknowledge_l3 = false;
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, invalid, &cancel).await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        let mut invalid = request.clone();
        invalid.capability = super::super::profile_cpu::MEMORY_PROFILE_CAPABILITY.to_owned();
        invalid.acknowledge_l3 = false;
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, invalid, &cancel).await,
            Err(super::RuntimeErrorCode::ConsentRequired)
        ));
        let mut invalid = request.clone();
        invalid.consent_expires_at_unix = 1;
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, invalid, &cancel).await,
            Err(super::RuntimeErrorCode::ConsentExpired)
        ));
        let mut invalid = request.clone();
        invalid.duration_millis = 30_001;
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, invalid, &cancel).await,
            Err(super::RuntimeErrorCode::LimitExceeded)
        ));
        let mut invalid = request.clone();
        invalid.offline_key_id = "0".repeat(64);
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, invalid, &cancel).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
        let mut value = serde_json::to_value(&request).unwrap();
        value["provenance"] = serde_json::json!({});
        assert!(serde_json::from_value::<super::LocalRuntimeProfileRequest>(value).is_err());
        assert!(serde_json::from_str::<super::CaptureRequest>(r#"{"operation":"TRACE_RECORD","protocolVersion":1,"protocolVersion":1,"consentExpiresAtUnix":1,"durationMillis":1,"maxSpans":1}"#).is_err());
        let key_dir = state.path().join("offline");
        std::fs::set_permissions(&key_dir, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, request.clone(), &cancel).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
        std::fs::set_permissions(&key_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = crate::connect::OfflineKeyStore::new(state.path()).key_path();
        let real = state.path().join("original-key");
        std::fs::rename(&path, &real).unwrap();
        std::os::unix::fs::symlink(&real, &path).unwrap();
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, request.clone(), &cancel).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
        std::fs::remove_file(&path).unwrap();
        assert!(matches!(
            super::capture_local_runtime_profile(state.path(), 1, request, &cancel).await,
            Err(super::RuntimeErrorCode::IdentityUnavailable)
        ));
        assert!(!path.exists());
    }

    #[tokio::test]
    #[serial]
    async fn local_runtime_profile_cancellation_waits_for_lease_release() {
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let provenance = super::super::job_delivery::executable_provenance()
            .await
            .expect("test executable provenance should be available");
        assert!(provenance.is_valid(), "profile capture requires valid executable provenance");
        let (mut request, _) = runtime_request(state.path());
        request.duration_millis = 5_000;
        let server_state_root = state.path().to_path_buf();
        let (ready, wait) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = std::thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let server_shutdown = CancellationToken::new();
                    let runtime = spawn_local_trace_capture_runtime(&server_state_root, &server_shutdown).unwrap();
                    ready.send(()).unwrap();
                    let _ = stopped.await;
                    runtime.shutdown().await;
                });
        });
        wait.await.unwrap();
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let state_root = state.path().to_path_buf();
        let mut task =
            tokio::spawn(async move { super::request_local_runtime_profile(&state_root, request, &task_cancel).await });
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if super::super::profile_cpu::CollectorLease::acquire().is_err() {
                    break;
                }
                if task.is_finished() {
                    let detail = match (&mut task).await.expect("request task should not panic") {
                        Ok(_) => "request completed successfully".to_owned(),
                        Err(error) => format!("request failed: {error}"),
                    };
                    panic!("runtime profile finished before acquiring its collector lease: {detail}");
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        assert!(matches!(task.await.unwrap(), Err(LocalTraceCaptureError::RuntimeProfile(code)) if code == "CANCELLED"));
        let lease = super::super::profile_cpu::CollectorLease::acquire().expect("client cancellation joins collector");
        drop(lease);
        stop.send(()).unwrap();
        server.join().unwrap();
    }
    #[tokio::test]
    #[serial]
    async fn local_signed_capture_does_not_claim_unacknowledged_cancellation() {
        for kind in 0..6 {
            let state = tempfile::tempdir().unwrap();
            std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let (request, _) = runtime_request(state.path());
            let owner = super::private_state_owner(state.path()).unwrap();
            let listener = super::bind_listener(&state.path().join(super::SOCKET_FILE), owner).unwrap();
            let (ready, received) = tokio::sync::oneshot::channel();
            let stop = CancellationToken::new();
            let peer_stop = stop.clone();
            let peer = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (reader, _writer) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(reader);
                super::read_request(&mut reader).await.unwrap();
                ready.send(()).unwrap();
                peer_stop.cancelled().await;
            });
            let cancel = CancellationToken::new();
            let task_cancel = cancel.clone();
            let state_root = state.path().to_path_buf();
            let disk_input = disk_request(state.path());
            let network_input = network_request(state.path());
            let task = tokio::spawn(async move {
                match kind {
                    0 => super::request_local_runtime_profile(&state_root, request, &task_cancel)
                        .await
                        .map(|_| ()),
                    1 => super::request_local_native_threads_profile(&state_root, request, &task_cancel)
                        .await
                        .map(|_| ()),
                    2 => super::request_local_top_disk(&state_root, disk_input, &task_cancel)
                        .await
                        .map(|_| ()),
                    3 => super::request_local_top_api(&state_root, disk_input, &task_cancel)
                        .await
                        .map(|_| ()),
                    4 => super::request_local_top_rpc(&state_root, disk_input, &task_cancel)
                        .await
                        .map(|_| ()),
                    _ => super::request_local_network(&state_root, network_input, &task_cancel)
                        .await
                        .map(|_| ()),
                }
            });
            received.await.unwrap();
            cancel.cancel();
            assert!(matches!(task.await.unwrap(), Err(LocalTraceCaptureError::CancellationUnconfirmed)));
            stop.cancel();
            peer.await.unwrap();
        }
    }
    #[tokio::test]
    async fn local_capture_wire_keeps_separate_request_limits() {
        use tokio::io::AsyncWriteExt as _;
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        for (disk, size, accepted) in [
            (false, 1_024, true),
            (false, 1_025, false),
            (false, 8_193, false),
            (true, 8_192, true),
            (true, 8_193, false),
        ] {
            let (mut sender, receiver) = tokio::net::UnixStream::pair().unwrap();
            let request = if disk {
                super::CaptureRequest::TopDisk {
                    protocol_version: 1,
                    request: disk_request(state.path()),
                }
            } else {
                super::CaptureRequest::TraceRecord {
                    protocol_version: 1,
                    consent_expires_at_unix: 1,
                    duration_millis: 1,
                    max_spans: 1,
                }
            };
            let mut bytes = serde_json::to_vec(&request).unwrap();
            bytes.resize(size - 1, b' ');
            bytes.push(b'\n');
            let writer = tokio::spawn(async move {
                sender.write_all(&bytes).await.unwrap();
            });
            let (reader, _) = receiver.into_split();
            let result = super::read_request(&mut tokio::io::BufReader::new(reader)).await;
            assert_eq!(result.is_ok(), accepted, "wire size {size}");
            writer.await.unwrap();
        }
    }
}
