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

//! Bounded capture of allow-listed RustFS structured log events.
//!
//! The collector reads only the active file configured for RustFS local JSON
//! logging. It exports timestamp offsets, severity, and one of three reviewed
//! event IDs. Raw messages, paths, headers, fields, and malformed lines are
//! counted as dropped and never enter the signed L3 artifact.

use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
use p256::pkcs8::DecodePrivateKey as _;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::connect::DeviceIdentity;

pub const LOGS_SCHEMA_VERSION: u16 = 1;
pub const LOGS_CAPABILITY: &str = "logs.capture@1";
pub const MAX_CAPTURE_DURATION: Duration = Duration::from_secs(30);
pub const MAX_EVENTS: usize = 1_024;
pub const MAX_RESULT_BYTES: usize = 262_144;
pub const MAX_SOURCE_BYTES: usize = 1_048_576;
pub const MAX_LINE_BYTES: usize = 4_096;
pub const MAX_BUILD_FEATURES: usize = 64;
pub const MAX_ARCHIVE_BYTES: usize = 524_288;
pub const MAX_DECOMPRESSED_BYTES: usize = 278_528;
pub const MAX_ENVELOPE_BYTES: usize = 16_384;

const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";
const ENVELOPE_PATH: &str = "envelope.json";
const SIGNATURE_PATH: &str = "envelope.sig";
const RESULT_PATH: &str = "result.json";
const OUTPUT_MODE: u32 = 0o600;
const MAX_VALIDITY_SECONDS: i64 = 2_592_000;
const MAX_FUTURE_SKEW_SECONDS: i64 = 300;
const POLL_INTERVAL: Duration = Duration::from_millis(50);

static LOG_COLLECTOR_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureMode {
    Batch,
    Live,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalLogConsent {
    pub consent_uid: String,
    pub policy_revision: u64,
    pub expires_at_unix: i64,
    pub confirmed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogProvenance {
    repository: &'static str,
    source_commit: String,
    executable_sha256: String,
    rustfs_version: String,
    os_family: OsFamily,
    architecture: Architecture,
    build_features: Vec<String>,
}

impl LogProvenance {
    pub fn new(
        source_commit: impl Into<String>,
        executable_sha256: impl Into<String>,
        rustfs_version: impl Into<String>,
        build_features: Vec<String>,
    ) -> Self {
        Self {
            repository: "rustfs/rustfs",
            source_commit: source_commit.into(),
            executable_sha256: executable_sha256.into(),
            rustfs_version: rustfs_version.into(),
            os_family: OsFamily::current(),
            architecture: Architecture::current(),
            build_features,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum OsFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

impl OsFamily {
    fn current() -> Self {
        match std::env::consts::OS {
            "linux" => Self::Linux,
            "macos" => Self::Darwin,
            "windows" => Self::Windows,
            "freebsd" => Self::Freebsd,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Architecture {
    #[serde(rename = "x86_64")]
    X86_64,
    Aarch64,
    Other,
}

impl Architecture {
    fn current() -> Self {
        match std::env::consts::ARCH {
            "x86_64" => Self::X86_64,
            "aarch64" => Self::Aarch64,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogCaptureRequest {
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub capability: String,
    pub consent: LocalLogConsent,
    pub produced_at_unix: i64,
    pub expires_at_unix: i64,
    pub nonce: [u8; 32],
    pub mode: CaptureMode,
    pub duration: Duration,
    pub max_events: usize,
    pub provenance: LogProvenance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogOutcome {
    Succeeded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogReasonCode {
    Complete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogSeverity {
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogEventId {
    DriveUnavailable,
    RequestFailed,
    ServiceStarted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapturedLogEvent {
    offset_millis: u64,
    severity: LogSeverity,
    event_id: LogEventId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct LogData {
    events: Vec<CapturedLogEvent>,
    dropped_event_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct Coverage {
    requested_units: u32,
    completed_units: u32,
    unit: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct LogResult {
    schema_version: u16,
    run_uid: String,
    tool_id: &'static str,
    capability: &'static str,
    outcome: LogOutcome,
    reason_code: LogReasonCode,
    duration_millis: u64,
    provenance: LogProvenance,
    coverage: Coverage,
    data: LogData,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedLogExport {
    pub artifact_uid: String,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
    pub event_count: usize,
    pub dropped_event_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedLogExport {
    pub artifact_uid: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

#[derive(Debug, Error)]
pub enum LogCaptureError {
    #[error("logs_local_consent_required")]
    ConsentRequired,
    #[error("logs_local_consent_expired")]
    ConsentExpired,
    #[error("logs_request_expired")]
    Expired,
    #[error("logs_invalid_request")]
    InvalidRequest,
    #[error("logs_unsupported_version")]
    UnsupportedVersion,
    #[error("logs_unsupported_capability")]
    UnsupportedCapability,
    #[error("logs_limit_exceeded")]
    LimitExceeded,
    #[error("logs_collection_cancelled")]
    Cancelled,
    #[error("logs_collection_already_running")]
    Busy,
    #[error("logs_file_source_unavailable")]
    SourceUnavailable,
    #[error("logs_export_signing_failed")]
    Signing,
    #[error("logs_export_exists")]
    AlreadyExists,
    #[error("logs_io_failed")]
    Io(#[source] std::io::Error),
    #[error("logs_export_encoding_failed")]
    Encoding,
    #[error("logs_export_durability_failed_after_commit")]
    DurabilityAfterCommit(#[source] std::io::Error),
}

#[derive(Clone, Debug)]
pub(crate) struct ConfiguredLogSource {
    path: PathBuf,
}

impl ConfiguredLogSource {
    pub(crate) fn discover() -> Result<Self, LogCaptureError> {
        let config = rustfs_obs::OtelConfig::new();
        let directory = config.log_directory.ok_or(LogCaptureError::SourceUnavailable)?;
        let filename = config.log_filename.ok_or(LogCaptureError::SourceUnavailable)?;
        Self::new(directory, filename)
    }

    pub(crate) fn new(directory: impl AsRef<Path>, filename: impl AsRef<Path>) -> Result<Self, LogCaptureError> {
        let filename = filename.as_ref();
        if filename.is_absolute() || filename.file_name() != Some(filename.as_os_str()) {
            return Err(LogCaptureError::SourceUnavailable);
        }
        let directory = fs::canonicalize(directory).map_err(|_| LogCaptureError::SourceUnavailable)?;
        let path = directory.join(filename);
        let metadata = fs::symlink_metadata(&path).map_err(|_| LogCaptureError::SourceUnavailable)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(LogCaptureError::SourceUnavailable);
        }
        let canonical = fs::canonicalize(&path).map_err(|_| LogCaptureError::SourceUnavailable)?;
        if canonical.parent() != Some(directory.as_path()) {
            return Err(LogCaptureError::SourceUnavailable);
        }
        Ok(Self { path: canonical })
    }

    fn len(&self) -> Result<u64, LogCaptureError> {
        fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .map_err(LogCaptureError::Io)
    }

    fn read(&self, start: u64, limit: usize) -> Result<SourceChunk, LogCaptureError> {
        let mut file = open_read_only(&self.path)?;
        let length = file.metadata().map_err(LogCaptureError::Io)?.len();
        let start = start.min(length);
        file.seek(SeekFrom::Start(start)).map_err(LogCaptureError::Io)?;
        let remaining = usize::try_from(length.saturating_sub(start)).unwrap_or(usize::MAX);
        let mut bytes = Vec::with_capacity(limit.min(remaining));
        file.take(limit as u64).read_to_end(&mut bytes).map_err(LogCaptureError::Io)?;
        let bytes_read = bytes.len() as u64;
        Ok(SourceChunk {
            bytes,
            next_offset: start.saturating_add(bytes_read),
            truncated_prefix: false,
        })
    }
}

struct SourceChunk {
    bytes: Vec<u8>,
    next_offset: u64,
    truncated_prefix: bool,
}

struct CollectorLease;

impl CollectorLease {
    fn acquire() -> Result<Self, LogCaptureError> {
        LOG_COLLECTOR_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self)
            .map_err(|_| LogCaptureError::Busy)
    }
}

impl Drop for CollectorLease {
    fn drop(&mut self) {
        LOG_COLLECTOR_ACTIVE.store(false, Ordering::Release);
    }
}

pub async fn export_logs(
    request: &LogCaptureRequest,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedLogExport, LogCaptureError> {
    request.validate(unix_now()?)?;
    check_cancel(cancel)?;
    let source = ConfiguredLogSource::discover()?;
    export_logs_from(request, key, cancel, &source).await
}

pub(crate) async fn export_logs_from(
    request: &LogCaptureRequest,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
    source: &ConfiguredLogSource,
) -> Result<SignedLogExport, LogCaptureError> {
    request.validate(unix_now()?)?;
    check_cancel(cancel)?;
    let _lease = CollectorLease::acquire()?;
    let started = Instant::now();
    let capture = match request.mode {
        CaptureMode::Batch => capture_batch(source, cancel)?,
        CaptureMode::Live => capture_live(source, request.duration, cancel).await?,
    };
    check_cancel(cancel)?;

    let (events, dropped_event_count) = parse_events(capture.bytes, capture.truncated_prefix, request);
    let result = LogResult {
        schema_version: LOGS_SCHEMA_VERSION,
        run_uid: request.run_uid.clone(),
        tool_id: "logs.capture",
        capability: LOGS_CAPABILITY,
        outcome: LogOutcome::Succeeded,
        reason_code: LogReasonCode::Complete,
        duration_millis: u64::try_from(started.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .max(1)
            .min(30_000),
        provenance: request.provenance.clone(),
        coverage: Coverage {
            requested_units: 1,
            completed_units: 1,
            unit: "WINDOW",
        },
        data: LogData {
            events,
            dropped_event_count,
        },
    };
    encode_signed_export(request, result, key, cancel)
}

fn capture_batch(source: &ConfiguredLogSource, cancel: &CancellationToken) -> Result<SourceChunk, LogCaptureError> {
    check_cancel(cancel)?;
    let length = source.len()?;
    let start = length.saturating_sub(MAX_SOURCE_BYTES as u64);
    let mut chunk = source.read(start, MAX_SOURCE_BYTES)?;
    chunk.truncated_prefix = start > 0;
    Ok(chunk)
}

async fn capture_live(
    source: &ConfiguredLogSource,
    duration: Duration,
    cancel: &CancellationToken,
) -> Result<SourceChunk, LogCaptureError> {
    let deadline = Instant::now() + duration;
    let mut offset = source.len()?;
    let mut bytes = Vec::new();
    let mut truncated_prefix = false;
    loop {
        check_cancel(cancel)?;
        let length = source.len()?;
        if length < offset {
            offset = 0;
            truncated_prefix = true;
        }
        if length > offset {
            let remaining = MAX_SOURCE_BYTES.saturating_sub(bytes.len());
            if remaining == 0 {
                truncated_prefix = true;
                break;
            }
            let chunk = source.read(offset, remaining)?;
            offset = chunk.next_offset;
            bytes.extend_from_slice(&chunk.bytes);
            truncated_prefix |= chunk.truncated_prefix;
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::select! {
            () = cancel.cancelled() => return Err(LogCaptureError::Cancelled),
            () = tokio::time::sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now()))) => {}
        }
    }
    Ok(SourceChunk {
        bytes,
        next_offset: offset,
        truncated_prefix,
    })
}

fn parse_events(bytes: Vec<u8>, truncated_prefix: bool, request: &LogCaptureRequest) -> (Vec<CapturedLogEvent>, u64) {
    let mut dropped = u64::from(truncated_prefix);
    let mut parsed = Vec::new();
    let batch_end_millis = request.produced_at_unix.saturating_mul(1_000);
    let batch_start_millis =
        batch_end_millis.saturating_sub(i64::try_from(request.duration.as_millis()).unwrap_or(i64::MAX).max(1));
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() || (truncated_prefix && index == 0) {
            continue;
        }
        if line.len() > MAX_LINE_BYTES {
            dropped = dropped.saturating_add(1);
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            dropped = dropped.saturating_add(1);
            continue;
        };
        let Some(event) = typed_event(&value) else {
            dropped = dropped.saturating_add(1);
            continue;
        };
        if request.mode == CaptureMode::Batch
            && (event.offset_millis < u64::try_from(batch_start_millis).unwrap_or(0)
                || event.offset_millis > u64::try_from(batch_end_millis).unwrap_or(0))
        {
            dropped = dropped.saturating_add(1);
            continue;
        }
        if parsed.len() == request.max_events {
            dropped = dropped.saturating_add(1);
            continue;
        }
        parsed.push(event);
    }
    let base = parsed.first().map_or(0, |event| event.offset_millis);
    for event in &mut parsed {
        event.offset_millis = event.offset_millis.saturating_sub(base);
    }
    (parsed, dropped)
}

fn typed_event(value: &Value) -> Option<CapturedLogEvent> {
    let fields = value.as_object()?;
    let timestamp = fields.get("timestamp")?.as_str()?;
    let timestamp = OffsetDateTime::parse(timestamp, &Rfc3339).ok()?;
    let timestamp_millis = u64::try_from(timestamp.unix_timestamp_nanos() / 1_000_000).ok()?;
    let severity = match fields.get("level")?.as_str()? {
        "INFO" => LogSeverity::Info,
        "WARN" => LogSeverity::Warn,
        "ERROR" => LogSeverity::Error,
        _ => return None,
    };
    let event_id = match fields.get("event")?.as_str()? {
        "drive_unavailable" => LogEventId::DriveUnavailable,
        "rpc_request_failed" | "admin_request_failed" | "http_request_failed" => LogEventId::RequestFailed,
        "http_startup_endpoints" => LogEventId::ServiceStarted,
        _ => return None,
    };
    Some(CapturedLogEvent {
        offset_millis: timestamp_millis,
        severity,
        event_id,
    })
}

fn encode_signed_export(
    request: &LogCaptureRequest,
    result: LogResult,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedLogExport, LogCaptureError> {
    check_cancel(cancel)?;
    if unix_now()? >= request.expires_at_unix {
        return Err(LogCaptureError::Expired);
    }
    let event_count = result.data.events.len();
    let dropped_event_count = result.data.dropped_event_count;
    let result_bytes = serde_json::to_vec(&result).map_err(|_| LogCaptureError::Encoding)?;
    if result_bytes.is_empty() || result_bytes.len() > MAX_RESULT_BYTES {
        return Err(LogCaptureError::LimitExceeded);
    }
    let device_key_id = hex_lower(&Sha256::digest(key.public_key_der()));
    let envelope = Envelope {
        format_version: "rustfs.connect.diagnosticEnvelope/1",
        protocol_version: "v1",
        organization_name: &request.organization_name,
        cluster_name: &request.cluster_name,
        device_name: &request.device_name,
        run_uid: &request.run_uid,
        artifact_uid: &request.artifact_uid,
        tool_id: "logs.capture",
        schema_version: LOGS_SCHEMA_VERSION,
        classification: "L3",
        consent_uid: &request.consent.consent_uid,
        policy_revision: request.consent.policy_revision,
        produced_at: timestamp(request.produced_at_unix)?,
        expires_at: timestamp(request.expires_at_unix)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(request.nonce),
        device_key_id: &device_key_id,
        payload: Payload {
            path: RESULT_PATH,
            media_type: "application/json",
            size_bytes: result_bytes.len() as u64,
            sha256: hex_lower(&Sha256::digest(&result_bytes)),
        },
    };
    let envelope_bytes = serde_json::to_vec(&envelope).map_err(|_| LogCaptureError::Encoding)?;
    if envelope_bytes.is_empty() || envelope_bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(LogCaptureError::LimitExceeded);
    }
    let signature_bytes = signature_document(key, &device_key_id, &envelope_bytes)?;
    let decompressed = result_bytes
        .len()
        .checked_add(envelope_bytes.len())
        .and_then(|size| size.checked_add(signature_bytes.len()))
        .ok_or(LogCaptureError::LimitExceeded)?;
    if decompressed > MAX_DECOMPRESSED_BYTES {
        return Err(LogCaptureError::LimitExceeded);
    }
    check_cancel(cancel)?;
    let archive_bytes = archive(&envelope_bytes, &signature_bytes, &result_bytes)?;
    if archive_bytes.len() > MAX_ARCHIVE_BYTES {
        return Err(LogCaptureError::LimitExceeded);
    }
    Ok(SignedLogExport {
        artifact_uid: request.artifact_uid.clone(),
        archive_sha256: hex_lower(&Sha256::digest(&archive_bytes)),
        archive_bytes,
        event_count,
        dropped_event_count,
    })
}

pub fn save_signed_log_export(
    output: &Path,
    export: &SignedLogExport,
    cancel: &CancellationToken,
) -> Result<SavedLogExport, LogCaptureError> {
    check_cancel(cancel)?;
    if !uuid7(&export.artifact_uid) {
        return Err(LogCaptureError::InvalidRequest);
    }
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = output.file_name().ok_or(LogCaptureError::InvalidRequest)?.to_string_lossy();
    let temporary = parent.join(format!(".{filename}.{}.partial", export.artifact_uid));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(OUTPUT_MODE);
    }
    let mut file = options.open(&temporary).map_err(map_create_error)?;
    let result = (|| {
        file.write_all(&export.archive_bytes).map_err(LogCaptureError::Io)?;
        check_cancel(cancel)?;
        file.sync_all().map_err(LogCaptureError::Io)?;
        fs::hard_link(&temporary, output).map_err(map_publish_error)?;
        if let Err(error) = fs::remove_file(&temporary) {
            return Err(LogCaptureError::DurabilityAfterCommit(error));
        }
        #[cfg(unix)]
        if let Err(error) = File::open(parent).and_then(|directory| directory.sync_all()) {
            return Err(LogCaptureError::DurabilityAfterCommit(error));
        }
        Ok(SavedLogExport {
            artifact_uid: export.artifact_uid.clone(),
            archive_size_bytes: export.archive_bytes.len() as u64,
            archive_sha256: export.archive_sha256.clone(),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

impl LogCaptureRequest {
    fn validate(&self, now_unix: i64) -> Result<(), LogCaptureError> {
        if self.schema_version != LOGS_SCHEMA_VERSION {
            return Err(LogCaptureError::UnsupportedVersion);
        }
        if self.capability != LOGS_CAPABILITY {
            return Err(LogCaptureError::UnsupportedCapability);
        }
        if !self.consent.confirmed || self.consent.policy_revision == 0 {
            return Err(LogCaptureError::ConsentRequired);
        }
        if self.consent.expires_at_unix <= now_unix || self.expires_at_unix > self.consent.expires_at_unix {
            return Err(LogCaptureError::ConsentExpired);
        }
        let validity = self
            .expires_at_unix
            .checked_sub(self.produced_at_unix)
            .ok_or(LogCaptureError::Expired)?;
        if self.produced_at_unix > now_unix.saturating_add(MAX_FUTURE_SKEW_SECONDS)
            || validity <= 0
            || self.expires_at_unix <= now_unix
            || validity > MAX_VALIDITY_SECONDS
        {
            return Err(LogCaptureError::Expired);
        }
        if self.duration.is_zero() || self.duration > MAX_CAPTURE_DURATION || self.max_events == 0 || self.max_events > MAX_EVENTS
        {
            return Err(LogCaptureError::LimitExceeded);
        }
        if !uuid7(&self.run_uid)
            || !uuid7(&self.artifact_uid)
            || !uuid7(&self.consent.consent_uid)
            || !resource_names_match(self)
            || !lower_hex(&self.provenance.source_commit, 40)
            || !lower_hex(&self.provenance.executable_sha256, 64)
            || !version(&self.provenance.rustfs_version)
            || self.provenance.build_features.len() > MAX_BUILD_FEATURES
            || !self.provenance.build_features.iter().all(|feature| build_feature(feature))
        {
            return Err(LogCaptureError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<'a> {
    format_version: &'static str,
    protocol_version: &'static str,
    organization_name: &'a str,
    cluster_name: &'a str,
    device_name: &'a str,
    run_uid: &'a str,
    artifact_uid: &'a str,
    tool_id: &'static str,
    schema_version: u16,
    classification: &'static str,
    consent_uid: &'a str,
    policy_revision: u64,
    produced_at: String,
    expires_at: String,
    nonce: String,
    device_key_id: &'a str,
    payload: Payload,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Payload {
    path: &'static str,
    media_type: &'static str,
    size_bytes: u64,
    sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SignatureDocument<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: String,
}

fn signature_document(key: &DeviceIdentity, key_id: &str, envelope: &[u8]) -> Result<Vec<u8>, LogCaptureError> {
    let pkcs8 = key.to_pkcs8_der().map_err(|_| LogCaptureError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| LogCaptureError::Signing)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + envelope.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(envelope);
    let signature: Signature = signing_key.sign(&input);
    let value = URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes());
    serde_json::to_vec(&SignatureDocument {
        algorithm: "ES256",
        key_id,
        value,
    })
    .map_err(|_| LogCaptureError::Encoding)
}

fn archive(envelope: &[u8], signature: &[u8], result: &[u8]) -> Result<Vec<u8>, LogCaptureError> {
    let cursor = Cursor::new(Vec::with_capacity(envelope.len() + signature.len() + result.len() + 512));
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(OUTPUT_MODE);
    for (name, bytes) in [(ENVELOPE_PATH, envelope), (SIGNATURE_PATH, signature), (RESULT_PATH, result)] {
        writer.start_file(name, options).map_err(|_| LogCaptureError::Encoding)?;
        writer.write_all(bytes).map_err(LogCaptureError::Io)?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|_| LogCaptureError::Encoding)
}

fn open_read_only(path: &Path) -> Result<File, LogCaptureError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).map_err(LogCaptureError::Io)
}

fn resource_names_match(request: &LogCaptureRequest) -> bool {
    let Some(organization_uid) = request.organization_name.strip_prefix("organizations/") else {
        return false;
    };
    if !uuid7(organization_uid) {
        return false;
    }
    let cluster_prefix = format!("{}/clusters/", request.organization_name);
    let Some(cluster_uid) = request.cluster_name.strip_prefix(&cluster_prefix) else {
        return false;
    };
    if !uuid7(cluster_uid) {
        return false;
    }
    let device_prefix = format!("{}/clusterDevices/", request.cluster_name);
    request.device_name.strip_prefix(&device_prefix).is_some_and(uuid7)
}

fn uuid7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn version(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return false;
    }
    let (core, suffix) = value
        .split_once('-')
        .map_or((value, None), |(core, suffix)| (core, Some(suffix)));
    if suffix.is_some_and(str::is_empty) {
        return false;
    }
    let mut parts = core.split('.');
    parts.clone().count() == 3 && parts.all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

fn build_feature(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-'))
}

fn timestamp(unix: i64) -> Result<String, LogCaptureError> {
    OffsetDateTime::from_unix_timestamp(unix)
        .map_err(|_| LogCaptureError::InvalidRequest)?
        .format(&Rfc3339)
        .map_err(|_| LogCaptureError::InvalidRequest)
}

fn unix_now() -> Result<i64, LogCaptureError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| LogCaptureError::InvalidRequest)?;
    i64::try_from(duration.as_secs()).map_err(|_| LogCaptureError::InvalidRequest)
}

fn check_cancel(cancel: &CancellationToken) -> Result<(), LogCaptureError> {
    if cancel.is_cancelled() {
        Err(LogCaptureError::Cancelled)
    } else {
        Ok(())
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing hexadecimal to a string cannot fail");
    }
    value
}

fn map_create_error(error: std::io::Error) -> LogCaptureError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        LogCaptureError::AlreadyExists
    } else {
        LogCaptureError::Io(error)
    }
}

fn map_publish_error(error: std::io::Error) -> LogCaptureError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        LogCaptureError::AlreadyExists
    } else {
        LogCaptureError::Io(error)
    }
}
