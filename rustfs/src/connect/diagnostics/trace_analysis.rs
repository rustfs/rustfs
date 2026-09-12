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

//! Deterministic local summaries over redacted telemetry spans.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::trace_record::{
    LocalTelemetryConsent, TelemetryOperation, TelemetryProducerError, TelemetrySpanStatus, acquire_telemetry_lease,
};
use super::trace_replay::ReplayedTrace;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TraceAnalysis {
    pub input_artifact_sha256: String,
    pub span_count: u64,
    pub error_count: u64,
    pub total_duration_micros: u64,
    pub operations: Vec<OperationSummary>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OperationSummary {
    pub operation: TelemetryOperation,
    pub span_count: u64,
    pub error_count: u64,
    pub total_duration_micros: u64,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TraceAnalysisError {
    #[error("another telemetry operation is already running")]
    Busy,
    #[error("local telemetry consent is expired")]
    ConsentExpired,
    #[error("telemetry analysis was cancelled")]
    Cancelled,
    #[error("telemetry analysis counters overflowed")]
    CounterOverflow,
}

pub fn analyze_trace(
    replay: &ReplayedTrace,
    consent: LocalTelemetryConsent,
    cancel: &CancellationToken,
) -> Result<TraceAnalysis, TraceAnalysisError> {
    consent.remaining().map_err(map_consent_error)?;
    if cancel.is_cancelled() {
        return Err(TraceAnalysisError::Cancelled);
    }
    let _lease = acquire_telemetry_lease().map_err(map_consent_error)?;

    let mut operations = [
        OperationSummary {
            operation: TelemetryOperation::GetObject,
            span_count: 0,
            error_count: 0,
            total_duration_micros: 0,
        },
        OperationSummary {
            operation: TelemetryOperation::PutObject,
            span_count: 0,
            error_count: 0,
            total_duration_micros: 0,
        },
        OperationSummary {
            operation: TelemetryOperation::HeadObject,
            span_count: 0,
            error_count: 0,
            total_duration_micros: 0,
        },
        OperationSummary {
            operation: TelemetryOperation::ListObjects,
            span_count: 0,
            error_count: 0,
            total_duration_micros: 0,
        },
        OperationSummary {
            operation: TelemetryOperation::InternalRpc,
            span_count: 0,
            error_count: 0,
            total_duration_micros: 0,
        },
    ];
    let mut error_count = 0u64;
    let mut total_duration_micros = 0u64;
    for span in &replay.spans {
        if cancel.is_cancelled() {
            return Err(TraceAnalysisError::Cancelled);
        }
        let summary = &mut operations[operation_index(span.operation)];
        summary.span_count = summary.span_count.checked_add(1).ok_or(TraceAnalysisError::CounterOverflow)?;
        summary.total_duration_micros = summary
            .total_duration_micros
            .checked_add(span.duration_micros)
            .ok_or(TraceAnalysisError::CounterOverflow)?;
        total_duration_micros = total_duration_micros
            .checked_add(span.duration_micros)
            .ok_or(TraceAnalysisError::CounterOverflow)?;
        if span.status == TelemetrySpanStatus::Error {
            summary.error_count = summary
                .error_count
                .checked_add(1)
                .ok_or(TraceAnalysisError::CounterOverflow)?;
            error_count = error_count.checked_add(1).ok_or(TraceAnalysisError::CounterOverflow)?;
        }
    }
    let span_count = u64::try_from(replay.spans.len()).map_err(|_| TraceAnalysisError::CounterOverflow)?;
    Ok(TraceAnalysis {
        input_artifact_sha256: replay.input_artifact_sha256.clone(),
        span_count,
        error_count,
        total_duration_micros,
        operations: operations.into_iter().filter(|summary| summary.span_count != 0).collect(),
    })
}

const fn operation_index(operation: TelemetryOperation) -> usize {
    match operation {
        TelemetryOperation::GetObject => 0,
        TelemetryOperation::PutObject => 1,
        TelemetryOperation::HeadObject => 2,
        TelemetryOperation::ListObjects => 3,
        TelemetryOperation::InternalRpc => 4,
    }
}

fn map_consent_error(error: TelemetryProducerError) -> TraceAnalysisError {
    match error {
        TelemetryProducerError::Busy => TraceAnalysisError::Busy,
        TelemetryProducerError::ConsentExpired => TraceAnalysisError::ConsentExpired,
        _ => TraceAnalysisError::CounterOverflow,
    }
}
