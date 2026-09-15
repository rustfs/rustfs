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

//! In-memory replay of a locally reviewed telemetry record.
//!
//! This module never opens a path and never replays S3 operations. The caller
//! supplies reviewed bytes; replay validates the exact closed record shape and
//! returns only its redacted operation/timing/status observations.

use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::trace_record::{
    LocalTelemetryConsent, MAX_SAFE_INTEGER, MAX_TELEMETRY_RESULT_BYTES, MAX_TELEMETRY_SPANS, RecordedTrace,
    TelemetryArtifactRequest, TelemetryDiagnosticResult, TelemetryProducerError, TelemetrySpan, TelemetryTool,
    acquire_telemetry_lease, ensure_result_size,
};

pub struct LocallyReviewedTraceArtifact<'a> {
    bytes: &'a [u8],
}

impl<'a> LocallyReviewedTraceArtifact<'a> {
    pub fn new(bytes: &'a [u8]) -> Result<Self, TraceReplayError> {
        if bytes.is_empty() || bytes.len() > MAX_TELEMETRY_RESULT_BYTES {
            return Err(TraceReplayError::InvalidArtifact);
        }
        Ok(Self { bytes })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReplayedTrace {
    pub input_artifact_sha256: String,
    pub spans: Vec<TelemetrySpan>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TraceReplayError {
    #[error("another telemetry operation is already running")]
    Busy,
    #[error("reviewed telemetry artifact is invalid")]
    InvalidArtifact,
    #[error("telemetry replay was cancelled")]
    Cancelled,
    #[error("local telemetry consent is expired")]
    ConsentExpired,
    #[error("telemetry replay result exceeds 262144 bytes")]
    ResultTooLarge,
}

pub fn replay_trace(
    artifact: LocallyReviewedTraceArtifact<'_>,
    consent: LocalTelemetryConsent,
    cancel: &CancellationToken,
) -> Result<ReplayedTrace, TraceReplayError> {
    consent.remaining().map_err(map_consent_error)?;
    if cancel.is_cancelled() {
        return Err(TraceReplayError::Cancelled);
    }
    let _lease = acquire_telemetry_lease().map_err(map_consent_error)?;
    let record: RecordedTrace = serde_json::from_slice(artifact.bytes).map_err(|_| TraceReplayError::InvalidArtifact)?;
    if record.spans.len() > MAX_TELEMETRY_SPANS
        || record.dropped_span_count > MAX_SAFE_INTEGER
        || record.spans.iter().any(|span| span.duration_micros > MAX_SAFE_INTEGER)
    {
        return Err(TraceReplayError::InvalidArtifact);
    }
    if cancel.is_cancelled() {
        return Err(TraceReplayError::Cancelled);
    }
    let result = ReplayedTrace {
        input_artifact_sha256: hex_lower(&Sha256::digest(artifact.bytes)),
        spans: record.spans,
    };
    ensure_result_size(&result).map_err(|_| TraceReplayError::ResultTooLarge)?;
    Ok(result)
}

pub fn replay_trace_result(
    request: &TelemetryArtifactRequest,
    artifact: LocallyReviewedTraceArtifact<'_>,
    consent: LocalTelemetryConsent,
    cancel: &CancellationToken,
) -> Result<TelemetryDiagnosticResult<ReplayedTrace>, TraceReplayError> {
    let started = Instant::now();
    let replay = replay_trace(artifact, consent, cancel)?;
    Ok(TelemetryDiagnosticResult::succeeded(
        request,
        TelemetryTool::Replay,
        started.elapsed(),
        replay,
    ))
}

fn map_consent_error(error: TelemetryProducerError) -> TraceReplayError {
    match error {
        TelemetryProducerError::Busy => TraceReplayError::Busy,
        TelemetryProducerError::ConsentExpired => TraceReplayError::ConsentExpired,
        _ => TraceReplayError::InvalidArtifact,
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut output, byte| {
        let _ = write!(output, "{byte:02x}");
        output
    })
}
