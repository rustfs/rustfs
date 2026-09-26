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

//! Bounded `top.rpc` aggregation over pre-classified internode HTTP RPC completions.

use std::time::Duration;

use rustfs_common::trace_bus::{TelemetryTraceOperation, TelemetryTraceStatus, subscribe_telemetry_trace_events};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::top_api::{MAX_SAFE_INTEGER, TopCaptureError, TopCaptureRequest, TopReasonCode, TopResult};

const TOOL_ID: &str = "top.rpc";
pub const TOP_RPC_CAPABILITY: &str = "top.rpc@1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopRpcData {
    pub request_count: u64,
    pub error_count: u64,
    pub window_millis: u64,
    pub total_duration_micros: u64,
}

pub async fn capture_top_rpc(
    request: &TopCaptureRequest,
    cancel: &CancellationToken,
) -> Result<TopResult<TopRpcData>, TopCaptureError> {
    request.validate_capture(TOOL_ID)?;
    if cancel.is_cancelled() {
        return request.cancelled(TOOL_ID);
    }
    let Some(_permit) = request.acquire(cancel).await? else {
        return request.cancelled(TOOL_ID);
    };
    // The typed source is emitted only after an internode HTTP response has a
    // final status. Tonic streams need a separate body-completion boundary.
    let mut subscription = subscribe_telemetry_trace_events();
    let started = tokio::time::Instant::now();
    let deadline = started + request.window;
    let mut request_count = 0_u64;
    let mut error_count = 0_u64;
    let mut total_duration_micros = 0_u64;

    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return request.cancelled(TOOL_ID),
            () = tokio::time::sleep_until(deadline) => break,
            received = subscription.recv() => match received {
                Ok(event) if event.operation == TelemetryTraceOperation::InternalRpc => {
                    if request_count >= u64::from(request.limits.max_operations)
                        || request_count >= u64::from(request.limits.max_records)
                    {
                        return request.failed(TOOL_ID, elapsed_millis(started.elapsed()), TopReasonCode::LimitExceeded);
                    }
                    let Ok(duration_micros) = u64::try_from(event.duration.as_micros()) else {
                        return request.failed(TOOL_ID, elapsed_millis(started.elapsed()), TopReasonCode::CollectionFailed);
                    };
                    let Some(next_duration) = total_duration_micros.checked_add(duration_micros) else {
                        return request.failed(TOOL_ID, elapsed_millis(started.elapsed()), TopReasonCode::CollectionFailed);
                    };
                    request_count += 1;
                    error_count += u64::from(event.status == TelemetryTraceStatus::Error);
                    total_duration_micros = next_duration;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    return request.failed(TOOL_ID, elapsed_millis(started.elapsed()), TopReasonCode::LimitExceeded);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return request.failed(TOOL_ID, elapsed_millis(started.elapsed()), TopReasonCode::SourceUnavailable);
                }
            }
        }
    }

    request.validate_scope(TOOL_ID)?;
    if request_count > MAX_SAFE_INTEGER || error_count > MAX_SAFE_INTEGER || total_duration_micros > MAX_SAFE_INTEGER {
        return request.failed(TOOL_ID, elapsed_millis(started.elapsed()), TopReasonCode::CollectionFailed);
    }
    if request_count == 0 {
        return request.unsupported(TOOL_ID, TopReasonCode::UnsupportedTool);
    }
    let window_millis = u64::try_from(request.window.as_millis()).map_err(|_| TopCaptureError::Limits)?;
    request.succeeded(
        TOOL_ID,
        window_millis,
        TopRpcData {
            request_count,
            error_count,
            window_millis,
            total_duration_micros,
        },
    )
}

fn elapsed_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).max(1)
}
