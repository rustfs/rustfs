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

//! Bounded, locally-authorized capture of already classified trace spans.
//!
//! The producer accepts only the five operations frozen by the Connect
//! diagnostic contract. Request paths, bucket/object names, HTTP headers and
//! arbitrary attributes cannot enter the captured representation.

use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
use p256::pkcs8::DecodePrivateKey as _;
use rustfs_common::trace_bus::subscribe_trace_events;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::connect::DeviceIdentity;

pub const MAX_TELEMETRY_DURATION: Duration = Duration::from_secs(30);
pub const MAX_TELEMETRY_SPANS: usize = 1024;
pub const MAX_TELEMETRY_RESULT_BYTES: usize = 262_144;
pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
pub const TELEMETRY_SCHEMA_VERSION: u16 = 1;
pub const TELEMETRY_RECORD_CAPABILITY: &str = "telemetry.record@1";
pub const TELEMETRY_OTLP_CAPABILITY: &str = "telemetry.otlp@1";
pub const TELEMETRY_REPLAY_CAPABILITY: &str = "telemetry.replay@1";

const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";
const ENVELOPE_PATH: &str = "envelope.json";
const SIGNATURE_PATH: &str = "envelope.sig";
const RESULT_PATH: &str = "result.json";
const MAX_ARCHIVE_BYTES: usize = 524_288;
const MAX_ENVELOPE_BYTES: usize = 16_384;
const MAX_DECOMPRESSED_BYTES: usize = 278_528;
const MAX_VALIDITY_SECONDS: i64 = 2_592_000;
const MAX_FUTURE_SKEW_SECONDS: i64 = 300;
const MAX_BUILD_FEATURES: usize = 64;
static TELEMETRY_LEASED: AtomicBool = AtomicBool::new(false);
#[cfg(unix)]
const OUTPUT_MODE: u32 = 0o600;

pub(super) struct TelemetryLease;

impl Drop for TelemetryLease {
    fn drop(&mut self) {
        TELEMETRY_LEASED.store(false, Ordering::Release);
    }
}

pub(super) fn acquire_telemetry_lease() -> Result<TelemetryLease, TelemetryProducerError> {
    TELEMETRY_LEASED
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .map(|_| TelemetryLease)
        .map_err(|_| TelemetryProducerError::Busy)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TelemetryOperation {
    GetObject,
    PutObject,
    HeadObject,
    ListObjects,
    InternalRpc,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TelemetrySpanStatus {
    Ok,
    Error,
}

/// A classified observation supplied by a server-side adapter.
///
/// Its closed shape deliberately has nowhere to retain a URL, header, object
/// name, trace attribute, or message body. The adapter must classify an event
/// before crossing this boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedTelemetrySpan {
    operation: TelemetryOperation,
    duration: Duration,
    status: TelemetrySpanStatus,
}

impl ObservedTelemetrySpan {
    pub fn new(operation: TelemetryOperation, duration: Duration, status: TelemetrySpanStatus) -> Self {
        Self {
            operation,
            duration,
            status,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TelemetrySpan {
    pub operation: TelemetryOperation,
    pub duration_micros: u64,
    pub status: TelemetrySpanStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RecordedTrace {
    pub spans: Vec<TelemetrySpan>,
    pub dropped_span_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceRecordCompletion {
    Complete,
    LimitExceeded,
    SourceUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceRecordCapture {
    pub data: RecordedTrace,
    pub completion: TraceRecordCompletion,
}

#[derive(Clone, Copy, Debug)]
pub struct LocalTelemetryConsent {
    expires_at: Instant,
}

impl LocalTelemetryConsent {
    pub fn new(expires_at: Instant) -> Result<Self, TelemetryProducerError> {
        if expires_at <= Instant::now() {
            return Err(TelemetryProducerError::ConsentExpired);
        }
        Ok(Self { expires_at })
    }

    pub fn remaining(self) -> Result<Duration, TelemetryProducerError> {
        self.expires_at
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(TelemetryProducerError::ConsentExpired)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelemetryArtifactConsent {
    pub consent_uid: String,
    pub policy_revision: u64,
    pub expires_at_unix: i64,
    pub confirmed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryProvenance {
    repository: &'static str,
    source_commit: String,
    executable_sha256: String,
    rustfs_version: String,
    os_family: TelemetryOsFamily,
    architecture: TelemetryArchitecture,
    build_features: Vec<String>,
}

impl TelemetryProvenance {
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
            os_family: TelemetryOsFamily::current(),
            architecture: TelemetryArchitecture::current(),
            build_features,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum TelemetryOsFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

impl TelemetryOsFamily {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum TelemetryArchitecture {
    #[serde(rename = "x86_64")]
    X86_64,
    Aarch64,
    Other,
}

impl TelemetryArchitecture {
    fn current() -> Self {
        match std::env::consts::ARCH {
            "x86_64" => Self::X86_64,
            "aarch64" => Self::Aarch64,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelemetryArtifactRequest {
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub consent: TelemetryArtifactConsent,
    pub produced_at_unix: i64,
    pub expires_at_unix: i64,
    pub nonce: [u8; 32],
    pub provenance: TelemetryProvenance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum TelemetryTool {
    #[serde(rename = "telemetry.record")]
    Record,
    #[serde(rename = "telemetry.otlp")]
    Otlp,
    #[serde(rename = "telemetry.replay")]
    Replay,
}

impl TelemetryTool {
    pub const fn id(self) -> &'static str {
        match self {
            Self::Record => "telemetry.record",
            Self::Otlp => "telemetry.otlp",
            Self::Replay => "telemetry.replay",
        }
    }

    pub const fn capability(self) -> &'static str {
        match self {
            Self::Record => TELEMETRY_RECORD_CAPABILITY,
            Self::Otlp => TELEMETRY_OTLP_CAPABILITY,
            Self::Replay => TELEMETRY_REPLAY_CAPABILITY,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TelemetryOutcome {
    Succeeded,
    Partial,
    Failed,
    Unsupported,
    Cancelled,
}

impl TelemetryOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "SUCCEEDED",
            Self::Partial => "PARTIAL",
            Self::Failed => "FAILED",
            Self::Unsupported => "UNSUPPORTED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TelemetryReasonCode {
    Complete,
    LimitExceeded,
    SourceUnavailable,
    PermissionDenied,
    UnsupportedTool,
    Cancelled,
    InvalidInput,
    CollectionFailed,
    CounterReset,
}

impl TelemetryReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "COMPLETE",
            Self::LimitExceeded => "LIMIT_EXCEEDED",
            Self::SourceUnavailable => "SOURCE_UNAVAILABLE",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::UnsupportedTool => "UNSUPPORTED_TOOL",
            Self::Cancelled => "CANCELLED",
            Self::InvalidInput => "INVALID_INPUT",
            Self::CollectionFailed => "COLLECTION_FAILED",
            Self::CounterReset => "COUNTER_RESET",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryCoverage {
    requested_units: u64,
    completed_units: u64,
    unit: &'static str,
}

impl TelemetryCoverage {
    pub const fn window(completed: bool) -> Self {
        Self {
            requested_units: 1,
            completed_units: completed as u64,
            unit: "WINDOW",
        }
    }

    const fn partial_windows() -> Self {
        Self {
            requested_units: 2,
            completed_units: 1,
            unit: "WINDOW",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryDiagnosticResult<T: Serialize> {
    schema_version: u16,
    run_uid: String,
    tool_id: TelemetryTool,
    capability: &'static str,
    outcome: TelemetryOutcome,
    reason_code: TelemetryReasonCode,
    duration_millis: u64,
    provenance: TelemetryProvenance,
    coverage: TelemetryCoverage,
    data: Option<T>,
}

impl<T: Serialize> TelemetryDiagnosticResult<T> {
    pub fn succeeded(request: &TelemetryArtifactRequest, tool: TelemetryTool, duration: Duration, data: T) -> Self {
        Self {
            schema_version: TELEMETRY_SCHEMA_VERSION,
            run_uid: request.run_uid.clone(),
            tool_id: tool,
            capability: tool.capability(),
            outcome: TelemetryOutcome::Succeeded,
            reason_code: TelemetryReasonCode::Complete,
            duration_millis: bounded_duration_millis(duration),
            provenance: request.provenance.clone(),
            coverage: TelemetryCoverage::window(true),
            data: Some(data),
        }
    }

    pub fn partial(
        request: &TelemetryArtifactRequest,
        tool: TelemetryTool,
        duration: Duration,
        reason_code: TelemetryReasonCode,
        data: T,
    ) -> Self {
        Self {
            schema_version: TELEMETRY_SCHEMA_VERSION,
            run_uid: request.run_uid.clone(),
            tool_id: tool,
            capability: tool.capability(),
            outcome: TelemetryOutcome::Partial,
            reason_code,
            duration_millis: bounded_duration_millis(duration),
            provenance: request.provenance.clone(),
            coverage: TelemetryCoverage::partial_windows(),
            data: Some(data),
        }
    }

    fn failed(
        request: &TelemetryArtifactRequest,
        tool: TelemetryTool,
        duration: Duration,
        reason_code: TelemetryReasonCode,
    ) -> Self {
        Self {
            schema_version: TELEMETRY_SCHEMA_VERSION,
            run_uid: request.run_uid.clone(),
            tool_id: tool,
            capability: tool.capability(),
            outcome: TelemetryOutcome::Failed,
            reason_code,
            duration_millis: bounded_duration_millis(duration),
            provenance: request.provenance.clone(),
            coverage: TelemetryCoverage::window(false),
            data: None,
        }
    }

    pub fn unsupported(request: &TelemetryArtifactRequest, tool: TelemetryTool, duration: Duration) -> Self {
        Self {
            schema_version: TELEMETRY_SCHEMA_VERSION,
            run_uid: request.run_uid.clone(),
            tool_id: tool,
            capability: tool.capability(),
            outcome: TelemetryOutcome::Unsupported,
            reason_code: TelemetryReasonCode::UnsupportedTool,
            duration_millis: bounded_duration_millis(duration),
            provenance: request.provenance.clone(),
            coverage: TelemetryCoverage::window(false),
            data: None,
        }
    }

    pub fn outcome(&self) -> TelemetryOutcome {
        self.outcome
    }

    pub fn reason_code(&self) -> TelemetryReasonCode {
        self.reason_code
    }

    pub fn data(&self) -> Option<&T> {
        self.data.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedTelemetryExport {
    pub artifact_uid: String,
    pub tool: TelemetryTool,
    pub outcome: TelemetryOutcome,
    pub reason_code: TelemetryReasonCode,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SavedTelemetryExport {
    pub artifact_uid: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

#[derive(Clone, Copy, Debug)]
pub struct TraceRecordLimits {
    pub duration: Duration,
    pub max_spans: usize,
}

impl TraceRecordLimits {
    pub fn validate(self) -> Result<Self, TelemetryProducerError> {
        if self.duration.is_zero() || self.duration > MAX_TELEMETRY_DURATION {
            return Err(TelemetryProducerError::InvalidDuration);
        }
        if self.max_spans == 0 || self.max_spans > MAX_TELEMETRY_SPANS {
            return Err(TelemetryProducerError::InvalidSpanLimit);
        }
        Ok(self)
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TelemetryProducerError {
    #[error("another telemetry operation is already running")]
    Busy,
    #[error("local telemetry consent is expired")]
    ConsentExpired,
    #[error("telemetry duration must be between 1ns and 30s")]
    InvalidDuration,
    #[error("telemetry span limit must be between 1 and 1024")]
    InvalidSpanLimit,
    #[error("telemetry capture was cancelled")]
    Cancelled,
    #[error("telemetry source closed before the requested window completed")]
    SourceUnavailable,
    #[error("telemetry duration exceeds the contract integer range")]
    DurationOverflow,
    #[error("telemetry result exceeds 262144 bytes")]
    ResultTooLarge,
}

#[derive(Debug, Error)]
pub enum TelemetryArtifactError {
    #[error("telemetry_invalid_request")]
    InvalidRequest,
    #[error("telemetry_unsupported_version")]
    UnsupportedVersion,
    #[error("telemetry_local_consent_required")]
    ConsentRequired,
    #[error("telemetry_local_consent_expired")]
    ConsentExpired,
    #[error("telemetry_request_expired")]
    Expired,
    #[error("telemetry_collection_cancelled")]
    Cancelled,
    #[error("telemetry_limit_exceeded")]
    LimitExceeded,
    #[error("telemetry_export_signing_failed")]
    Signing,
    #[error("telemetry_export_exists")]
    AlreadyExists,
    #[error("telemetry_export_encoding_failed")]
    Encoding,
    #[error("telemetry_export_io_failed")]
    Io(#[source] std::io::Error),
    #[error("telemetry_export_durability_failed_after_commit")]
    DurabilityAfterCommit(#[source] std::io::Error),
}

/// Capture classified spans until the bounded window ends.
///
/// Reaching `max_spans` stops capture immediately and increments the drop count
/// for every already queued observation that could be counted without waiting.
pub async fn record_trace(
    mut source: mpsc::Receiver<ObservedTelemetrySpan>,
    consent: LocalTelemetryConsent,
    limits: TraceRecordLimits,
    cancel: &CancellationToken,
) -> Result<TraceRecordCapture, TelemetryProducerError> {
    let limits = limits.validate()?;
    if cancel.is_cancelled() {
        return Err(TelemetryProducerError::Cancelled);
    }
    let remaining = consent.remaining()?;
    if remaining < limits.duration {
        return Err(TelemetryProducerError::ConsentExpired);
    }
    let _lease = acquire_telemetry_lease()?;
    let deadline = Instant::now() + limits.duration;
    let mut spans = Vec::with_capacity(limits.max_spans.min(64));
    let mut dropped_span_count = 0u64;
    let mut completion = TraceRecordCompletion::Complete;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(TelemetryProducerError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => break,
            observed = source.recv() => {
                let Some(observed) = observed else {
                    if spans.is_empty() {
                        return Err(TelemetryProducerError::SourceUnavailable);
                    }
                    completion = TraceRecordCompletion::SourceUnavailable;
                    break;
                };
                if spans.len() == limits.max_spans {
                    completion = TraceRecordCompletion::LimitExceeded;
                    dropped_span_count = dropped_span_count.saturating_add(1).min(MAX_SAFE_INTEGER);
                    while source.try_recv().is_ok() {
                        dropped_span_count = dropped_span_count.saturating_add(1).min(MAX_SAFE_INTEGER);
                    }
                    break;
                }
                spans.push(to_contract_span(observed)?);
            }
        }
    }

    let result = RecordedTrace {
        spans,
        dropped_span_count,
    };
    ensure_result_size(&result)?;
    Ok(TraceRecordCapture {
        data: result,
        completion,
    })
}

/// Capture the process-local RustFS trace bus.
///
/// The current bus exposes only heal and scanner operations, none of which has
/// the frozen GET/PUT/HEAD/LIST/INTERNAL_RPC semantics. The adapter consumes
/// that real source but refuses to infer an operation or status from raw
/// fields, so it remains unavailable until RustFS publishes an approved typed
/// event.
pub async fn record_trace_bus(
    consent: LocalTelemetryConsent,
    limits: TraceRecordLimits,
    cancel: &CancellationToken,
) -> Result<TraceRecordCapture, TelemetryProducerError> {
    let limits = limits.validate()?;
    if cancel.is_cancelled() {
        return Err(TelemetryProducerError::Cancelled);
    }
    let remaining = consent.remaining()?;
    if remaining < limits.duration {
        return Err(TelemetryProducerError::ConsentExpired);
    }
    let _lease = acquire_telemetry_lease()?;
    let deadline = Instant::now() + limits.duration;
    let mut subscription = subscribe_trace_events();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(TelemetryProducerError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(TelemetryProducerError::SourceUnavailable),
            received = subscription.recv() => match received {
                Ok(_event) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_dropped)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Err(TelemetryProducerError::SourceUnavailable),
            }
        }
    }
}

pub fn record_diagnostic_result(
    request: &TelemetryArtifactRequest,
    capture: TraceRecordCapture,
    duration: Duration,
) -> TelemetryDiagnosticResult<RecordedTrace> {
    match capture.completion {
        TraceRecordCompletion::Complete => {
            TelemetryDiagnosticResult::succeeded(request, TelemetryTool::Record, duration, capture.data)
        }
        TraceRecordCompletion::LimitExceeded => {
            TelemetryDiagnosticResult::failed(request, TelemetryTool::Record, duration, TelemetryReasonCode::CollectionFailed)
        }
        TraceRecordCompletion::SourceUnavailable => {
            TelemetryDiagnosticResult::failed(request, TelemetryTool::Record, duration, TelemetryReasonCode::SourceUnavailable)
        }
    }
}

fn to_contract_span(observed: ObservedTelemetrySpan) -> Result<TelemetrySpan, TelemetryProducerError> {
    let duration_micros = u64::try_from(observed.duration.as_micros()).map_err(|_| TelemetryProducerError::DurationOverflow)?;
    if duration_micros > MAX_SAFE_INTEGER {
        return Err(TelemetryProducerError::DurationOverflow);
    }
    Ok(TelemetrySpan {
        operation: observed.operation,
        duration_micros,
        status: observed.status,
    })
}

pub(crate) fn ensure_result_size(value: &impl Serialize) -> Result<(), TelemetryProducerError> {
    let bytes = serde_json::to_vec(value).map_err(|_| TelemetryProducerError::ResultTooLarge)?;
    if bytes.len() > MAX_TELEMETRY_RESULT_BYTES {
        return Err(TelemetryProducerError::ResultTooLarge);
    }
    Ok(())
}

pub fn encode_signed_telemetry_export<T: Serialize>(
    request: &TelemetryArtifactRequest,
    result: &TelemetryDiagnosticResult<T>,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedTelemetryExport, TelemetryArtifactError> {
    request.validate()?;
    check_cancel(cancel)?;
    if result.schema_version != request.schema_version || result.run_uid != request.run_uid {
        return Err(TelemetryArtifactError::InvalidRequest);
    }
    if !matches!(result.outcome, TelemetryOutcome::Succeeded | TelemetryOutcome::Partial) {
        return Err(TelemetryArtifactError::InvalidRequest);
    }
    let valid_publishable_shape = match result.outcome {
        TelemetryOutcome::Succeeded => {
            result.data.is_some()
                && result.reason_code == TelemetryReasonCode::Complete
                && result.coverage.requested_units == 1
                && result.coverage.completed_units == 1
                && result.coverage.unit == "WINDOW"
        }
        TelemetryOutcome::Partial => {
            result.data.is_some()
                && matches!(
                    result.reason_code,
                    TelemetryReasonCode::LimitExceeded
                        | TelemetryReasonCode::SourceUnavailable
                        | TelemetryReasonCode::CounterReset
                )
                && result.coverage.requested_units == 2
                && result.coverage.completed_units == 1
                && result.coverage.unit == "WINDOW"
        }
        _ => false,
    };
    if !valid_publishable_shape {
        return Err(TelemetryArtifactError::InvalidRequest);
    }
    if result.capability != result.tool_id.capability() {
        return Err(TelemetryArtifactError::InvalidRequest);
    }
    let result_bytes = serde_json::to_vec(result).map_err(|_| TelemetryArtifactError::Encoding)?;
    if result_bytes.is_empty() || result_bytes.len() > MAX_TELEMETRY_RESULT_BYTES {
        return Err(TelemetryArtifactError::LimitExceeded);
    }
    let device_key_id = hex_lower(&Sha256::digest(key.public_key_der()));
    let envelope = TelemetryEnvelope {
        format_version: "rustfs.connect.diagnosticEnvelope/1",
        protocol_version: "v1",
        organization_name: &request.organization_name,
        cluster_name: &request.cluster_name,
        device_name: &request.device_name,
        run_uid: &request.run_uid,
        artifact_uid: &request.artifact_uid,
        tool_id: result.tool_id,
        schema_version: TELEMETRY_SCHEMA_VERSION,
        classification: "L3",
        consent_uid: &request.consent.consent_uid,
        policy_revision: request.consent.policy_revision,
        produced_at: timestamp(request.produced_at_unix)?,
        expires_at: timestamp(request.expires_at_unix)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(request.nonce),
        device_key_id: &device_key_id,
        payload: TelemetryPayload {
            path: RESULT_PATH,
            media_type: "application/json",
            size_bytes: result_bytes.len() as u64,
            sha256: hex_lower(&Sha256::digest(&result_bytes)),
        },
    };
    let envelope_bytes = serde_json::to_vec(&envelope).map_err(|_| TelemetryArtifactError::Encoding)?;
    if envelope_bytes.is_empty() || envelope_bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(TelemetryArtifactError::LimitExceeded);
    }
    let signature_bytes = signature_document(key, &device_key_id, &envelope_bytes)?;
    let decompressed = result_bytes
        .len()
        .checked_add(envelope_bytes.len())
        .and_then(|size| size.checked_add(signature_bytes.len()))
        .ok_or(TelemetryArtifactError::LimitExceeded)?;
    if decompressed > MAX_DECOMPRESSED_BYTES {
        return Err(TelemetryArtifactError::LimitExceeded);
    }
    check_cancel(cancel)?;
    if unix_now()? >= request.expires_at_unix {
        return Err(TelemetryArtifactError::Expired);
    }
    let archive_bytes = archive(&envelope_bytes, &signature_bytes, &result_bytes)?;
    if archive_bytes.len() > MAX_ARCHIVE_BYTES {
        return Err(TelemetryArtifactError::LimitExceeded);
    }
    Ok(SignedTelemetryExport {
        artifact_uid: request.artifact_uid.clone(),
        tool: result.tool_id,
        outcome: result.outcome,
        reason_code: result.reason_code,
        archive_sha256: hex_lower(&Sha256::digest(&archive_bytes)),
        archive_bytes,
    })
}

pub fn save_signed_telemetry_export(
    output: &Path,
    export: &SignedTelemetryExport,
    cancel: &CancellationToken,
) -> Result<SavedTelemetryExport, TelemetryArtifactError> {
    check_cancel(cancel)?;
    if !uuid7(&export.artifact_uid)
        || export.archive_bytes.is_empty()
        || export.archive_bytes.len() > MAX_ARCHIVE_BYTES
        || hex_lower(&Sha256::digest(&export.archive_bytes)) != export.archive_sha256
    {
        return Err(TelemetryArtifactError::InvalidRequest);
    }
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = output
        .file_name()
        .ok_or(TelemetryArtifactError::InvalidRequest)?
        .to_string_lossy();
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
        file.write_all(&export.archive_bytes).map_err(TelemetryArtifactError::Io)?;
        check_cancel(cancel)?;
        file.sync_all().map_err(TelemetryArtifactError::Io)?;
        check_cancel(cancel)?;
        fs::hard_link(&temporary, output).map_err(map_publish_error)?;
        if let Err(error) = fs::remove_file(&temporary) {
            return Err(TelemetryArtifactError::DurabilityAfterCommit(error));
        }
        #[cfg(unix)]
        if let Err(error) = File::open(parent).and_then(|directory| directory.sync_all()) {
            return Err(TelemetryArtifactError::DurabilityAfterCommit(error));
        }
        Ok(SavedTelemetryExport {
            artifact_uid: export.artifact_uid.clone(),
            archive_size_bytes: export.archive_bytes.len() as u64,
            archive_sha256: export.archive_sha256.clone(),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

impl TelemetryArtifactRequest {
    pub fn validate(&self) -> Result<(), TelemetryArtifactError> {
        self.validate_at(unix_now()?)
    }

    fn validate_at(&self, now_unix: i64) -> Result<(), TelemetryArtifactError> {
        if self.schema_version != TELEMETRY_SCHEMA_VERSION {
            return Err(TelemetryArtifactError::UnsupportedVersion);
        }
        if !self.consent.confirmed || self.consent.policy_revision == 0 {
            return Err(TelemetryArtifactError::ConsentRequired);
        }
        if self.consent.expires_at_unix <= now_unix || self.expires_at_unix > self.consent.expires_at_unix {
            return Err(TelemetryArtifactError::ConsentExpired);
        }
        let validity = self
            .expires_at_unix
            .checked_sub(self.produced_at_unix)
            .ok_or(TelemetryArtifactError::Expired)?;
        if self.produced_at_unix > now_unix.saturating_add(MAX_FUTURE_SKEW_SECONDS)
            || validity <= 0
            || self.expires_at_unix <= now_unix
            || validity > MAX_VALIDITY_SECONDS
        {
            return Err(TelemetryArtifactError::Expired);
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
            return Err(TelemetryArtifactError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TelemetryEnvelope<'a> {
    format_version: &'static str,
    protocol_version: &'static str,
    organization_name: &'a str,
    cluster_name: &'a str,
    device_name: &'a str,
    run_uid: &'a str,
    artifact_uid: &'a str,
    tool_id: TelemetryTool,
    schema_version: u16,
    classification: &'static str,
    consent_uid: &'a str,
    policy_revision: u64,
    produced_at: String,
    expires_at: String,
    nonce: String,
    device_key_id: &'a str,
    payload: TelemetryPayload,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TelemetryPayload {
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

fn signature_document(key: &DeviceIdentity, key_id: &str, envelope: &[u8]) -> Result<Vec<u8>, TelemetryArtifactError> {
    let pkcs8 = key.to_pkcs8_der().map_err(|_| TelemetryArtifactError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| TelemetryArtifactError::Signing)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + envelope.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(envelope);
    let signature: Signature = signing_key.sign(&input);
    serde_json::to_vec(&SignatureDocument {
        algorithm: "ES256",
        key_id,
        value: URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes()),
    })
    .map_err(|_| TelemetryArtifactError::Encoding)
}

fn archive(envelope: &[u8], signature: &[u8], result: &[u8]) -> Result<Vec<u8>, TelemetryArtifactError> {
    let cursor = Cursor::new(Vec::with_capacity(envelope.len() + signature.len() + result.len() + 512));
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(0o600);
    for (name, bytes) in [(ENVELOPE_PATH, envelope), (SIGNATURE_PATH, signature), (RESULT_PATH, result)] {
        writer
            .start_file(name, options)
            .map_err(|_| TelemetryArtifactError::Encoding)?;
        writer.write_all(bytes).map_err(TelemetryArtifactError::Io)?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|_| TelemetryArtifactError::Encoding)
}

fn resource_names_match(request: &TelemetryArtifactRequest) -> bool {
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

fn timestamp(unix: i64) -> Result<String, TelemetryArtifactError> {
    OffsetDateTime::from_unix_timestamp(unix)
        .map_err(|_| TelemetryArtifactError::InvalidRequest)?
        .format(&Rfc3339)
        .map_err(|_| TelemetryArtifactError::InvalidRequest)
}

fn unix_now() -> Result<i64, TelemetryArtifactError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TelemetryArtifactError::InvalidRequest)?;
    i64::try_from(duration.as_secs()).map_err(|_| TelemetryArtifactError::InvalidRequest)
}

fn check_cancel(cancel: &CancellationToken) -> Result<(), TelemetryArtifactError> {
    if cancel.is_cancelled() {
        Err(TelemetryArtifactError::Cancelled)
    } else {
        Ok(())
    }
}

fn bounded_duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).min(30_000)
}

fn hex_lower(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}

fn map_create_error(error: std::io::Error) -> TelemetryArtifactError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        TelemetryArtifactError::AlreadyExists
    } else {
        TelemetryArtifactError::Io(error)
    }
}

fn map_publish_error(error: std::io::Error) -> TelemetryArtifactError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        TelemetryArtifactError::AlreadyExists
    } else {
        TelemetryArtifactError::Io(error)
    }
}
