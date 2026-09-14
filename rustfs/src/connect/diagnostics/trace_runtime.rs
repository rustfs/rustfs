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

//! Owner-only local transport between the telemetry CLI and the running server.

use std::io;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use super::{LocalTelemetryConsent, TelemetryProducerError, TraceRecordCapture, TraceRecordLimits, record_trace_bus};

const SOCKET_FILE: &str = "telemetry-record.sock";
const PROTOCOL_VERSION: u16 = 1;
const MAX_REQUEST_BYTES: u64 = 1_024;
const MAX_RESPONSE_BYTES: u64 = 300_000;
const MAX_CONNECTIONS: usize = 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const STATE_DIRECTORY_MODE: u32 = 0o700;
const SOCKET_MODE: u32 = 0o600;

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
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CaptureRequest {
    protocol_version: u16,
    consent_expires_at_unix: i64,
    duration_millis: u64,
    max_spans: usize,
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
#[serde(deny_unknown_fields, tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
enum CaptureResponse {
    Ok { capture: TraceRecordCapture },
    Error { code: ProducerErrorCode },
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
    let task = tokio::spawn(async move {
        run_listener(listener, owner, task_shutdown).await;
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
    let (reader, mut writer) = stream.into_split();
    let request = CaptureRequest {
        protocol_version: PROTOCOL_VERSION,
        consent_expires_at_unix,
        duration_millis: u64::try_from(limits.duration.as_millis()).map_err(|_| TelemetryProducerError::InvalidDuration)?,
        max_spans: limits.max_spans,
    };
    let mut encoded = serde_json::to_vec(&request).map_err(|_| LocalTraceCaptureError::Protocol)?;
    encoded.push(b'\n');
    if encoded.len() as u64 > MAX_REQUEST_BYTES {
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
    }
}

async fn run_listener(listener: UnixListener, owner: u32, shutdown: CancellationToken) {
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
                        connections.spawn(handle_connection(stream, shutdown.clone()));
                    }
                }
                Err(_) => break,
            },
        }
    }
    while connections.join_next().await.is_some() {}
}

async fn handle_connection(stream: UnixStream, shutdown: CancellationToken) {
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
    let limits = TraceRecordLimits {
        duration: Duration::from_millis(request.duration_millis),
        max_spans: request.max_spans,
    };
    let capture = async {
        if request.protocol_version != PROTOCOL_VERSION {
            return Err(TelemetryProducerError::SourceUnavailable);
        }
        limits.validate()?;
        let remaining = request
            .consent_expires_at_unix
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
    serde_json::from_slice(&bytes).map_err(|_| LocalTraceCaptureError::Protocol)
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
}
