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

//! Customer-side OTLP/HTTP forwarding with bounded payloads and local secrets.

use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message as _;
use reqwest::{
    Client, StatusCode, Url,
    header::{self, HeaderMap},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::trace_record::{
    LocalTelemetryConsent, MAX_TELEMETRY_DURATION, TelemetryArtifactRequest, TelemetryDiagnosticResult, TelemetryLease,
    TelemetryProducerError, TelemetryTool, acquire_telemetry_lease, ensure_result_size,
};

pub const MAX_OTLP_BODY_BYTES: usize = 1_048_576;

/// An encoded OTLP/HTTP protobuf batch from the local OpenTelemetry adapter.
/// The bytes are never included in the diagnostic result or error values.
pub struct OtlpBatch {
    body: Vec<u8>,
    span_count: u64,
    _lease: TelemetryLease,
}

impl OtlpBatch {
    pub fn new(body: Vec<u8>) -> Result<Self, OtlpForwardError> {
        if body.is_empty() || body.len() > MAX_OTLP_BODY_BYTES {
            return Err(OtlpForwardError::InvalidBatch);
        }
        let lease = acquire_telemetry_lease().map_err(map_producer_error)?;
        let request = ExportTraceServiceRequest::decode(body.as_slice()).map_err(|_| OtlpForwardError::InvalidBatch)?;
        if request.resource_spans.len() > 1024 {
            return Err(OtlpForwardError::InvalidBatch);
        }
        let scope_count = request.resource_spans.iter().try_fold(0_usize, |count, resource| {
            count
                .checked_add(resource.scope_spans.len())
                .filter(|count| *count <= 1024)
                .ok_or(OtlpForwardError::InvalidBatch)
        })?;
        if scope_count == 0 {
            return Err(OtlpForwardError::InvalidBatch);
        }
        let span_count = request
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .try_fold(0_u64, |count, scope| {
                let spans = u64::try_from(scope.spans.len()).map_err(|_| OtlpForwardError::InvalidBatch)?;
                count.checked_add(spans).ok_or(OtlpForwardError::InvalidBatch)
            })?;
        if span_count == 0 || span_count > 1024 {
            return Err(OtlpForwardError::InvalidBatch);
        }
        Ok(Self {
            body,
            span_count,
            _lease: lease,
        })
    }
}

pub async fn export_trace_otlp_result(
    request: &TelemetryArtifactRequest,
    endpoint: Url,
    headers: LocalOtlpHeaders,
    batch: OtlpBatch,
    consent: LocalTelemetryConsent,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<TelemetryDiagnosticResult<OtlpReceipt>, OtlpForwardError> {
    let started = Instant::now();
    let receipt = export_trace_otlp(endpoint, headers, batch, consent, timeout, cancel).await?;
    Ok(TelemetryDiagnosticResult::succeeded(
        request,
        TelemetryTool::Otlp,
        started.elapsed(),
        receipt,
    ))
}

/// Locally supplied collector authentication. This type has no `Debug` or
/// serialization implementation, keeping credentials out of results and logs.
pub struct LocalOtlpHeaders(HeaderMap);

impl LocalOtlpHeaders {
    pub fn new(headers: HeaderMap) -> Self {
        Self(headers)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OtlpReceipt {
    pub accepted_span_count: u64,
    pub rejected_span_count: u64,
    pub exported_bytes: u64,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum OtlpForwardError {
    #[error("another telemetry operation is already running")]
    Busy,
    #[error("OTLP endpoint must be HTTPS or an HTTP loopback address without credentials, query or fragment")]
    Endpoint,
    #[error("OTLP batch must contain 1..1024 spans and 1..1048576 bytes")]
    InvalidBatch,
    #[error("OTLP forward timeout must be between 1ms and 30s")]
    InvalidTimeout,
    #[error("OTLP forwarding was cancelled")]
    Cancelled,
    #[error("local OTLP collector rejected the batch")]
    Rejected,
    #[error("local OTLP collector is unavailable")]
    Unavailable,
    #[error("OTLP receipt exceeds its result limit")]
    ResultTooLarge,
    #[error("local telemetry consent is expired")]
    ConsentExpired,
}

pub async fn export_trace_otlp(
    endpoint: Url,
    headers: LocalOtlpHeaders,
    batch: OtlpBatch,
    consent: LocalTelemetryConsent,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<OtlpReceipt, OtlpForwardError> {
    validate_endpoint(&endpoint)?;
    if timeout.is_zero() || timeout > MAX_TELEMETRY_DURATION {
        return Err(OtlpForwardError::InvalidTimeout);
    }
    let remaining = consent.remaining().map_err(map_consent_error)?;
    let consent_binds = remaining <= timeout;
    let timeout = timeout.min(remaining);
    let timeout_millis = usize::try_from(timeout.as_millis()).map_err(|_| OtlpForwardError::InvalidTimeout)?;
    if timeout_millis == 0 {
        return Err(OtlpForwardError::InvalidTimeout);
    }
    let deadline = tokio::time::Instant::now() + timeout;
    let OtlpBatch {
        body,
        span_count,
        _lease,
    } = batch;
    let byte_budget = MAX_OTLP_BODY_BYTES
        .checked_mul(timeout_millis.min(1000))
        .and_then(|bytes| bytes.checked_div(1000))
        .ok_or(OtlpForwardError::InvalidBatch)?;
    if body.len() > byte_budget {
        return Err(OtlpForwardError::InvalidBatch);
    }
    let exported_bytes = u64::try_from(body.len()).map_err(|_| OtlpForwardError::InvalidBatch)?;
    let mut headers = headers.0;
    for transport_header in [
        header::HOST,
        header::CONTENT_LENGTH,
        header::CONTENT_TYPE,
        header::CONNECTION,
        header::TRANSFER_ENCODING,
    ] {
        headers.remove(transport_header);
    }
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()
        .map_err(|_| OtlpForwardError::Unavailable)?;

    let request = client
        .post(endpoint)
        .headers(headers)
        .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
        .body(body);
    let mut response = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(OtlpForwardError::Cancelled),
        _ = tokio::time::sleep_until(deadline) => return Err(deadline_error(consent_binds)),
        response = request.send() => response.map_err(|_| OtlpForwardError::Unavailable)?,
    };
    if response.status() != StatusCode::OK {
        return Err(if response.status().is_client_error() {
            OtlpForwardError::Rejected
        } else {
            OtlpForwardError::Unavailable
        });
    }
    loop {
        let chunk = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(OtlpForwardError::Cancelled),
            _ = tokio::time::sleep_until(deadline) => return Err(deadline_error(consent_binds)),
            chunk = response.chunk() => chunk.map_err(|_| OtlpForwardError::Unavailable)?,
        };
        let Some(chunk) = chunk else {
            break;
        };
        if !chunk.is_empty() {
            // A successful OTLP response may carry partial-success details.
            // Until the approved protobuf adapter is wired, refusing such a
            // response avoids claiming every span was accepted.
            return Err(OtlpForwardError::Rejected);
        }
    }

    let receipt = OtlpReceipt {
        accepted_span_count: span_count,
        rejected_span_count: 0,
        exported_bytes,
    };
    ensure_result_size(&receipt).map_err(|_| OtlpForwardError::ResultTooLarge)?;
    Ok(receipt)
}

fn deadline_error(consent_binds: bool) -> OtlpForwardError {
    if consent_binds {
        OtlpForwardError::ConsentExpired
    } else {
        OtlpForwardError::Unavailable
    }
}

fn validate_endpoint(endpoint: &Url) -> Result<(), OtlpForwardError> {
    let loopback_http = endpoint.scheme() == "http"
        && endpoint
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1"));
    if (endpoint.scheme() != "https" && !loopback_http)
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(OtlpForwardError::Endpoint);
    }
    Ok(())
}

fn map_consent_error(error: TelemetryProducerError) -> OtlpForwardError {
    match error {
        TelemetryProducerError::ConsentExpired => OtlpForwardError::ConsentExpired,
        _ => OtlpForwardError::Unavailable,
    }
}

fn map_producer_error(error: TelemetryProducerError) -> OtlpForwardError {
    match error {
        TelemetryProducerError::Busy => OtlpForwardError::Busy,
        _ => OtlpForwardError::InvalidBatch,
    }
}
