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

//! Honest `top.locks` capability result.

use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::top_api::{MAX_SAFE_INTEGER, TopCaptureError, TopCaptureRequest, TopReasonCode, TopResult};

const TOOL_ID: &str = "top.locks";
pub const TOP_LOCKS_CAPABILITY: &str = "top.locks@1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopLocksData {
    pub held_count: u64,
    pub waiting_count: u64,
    pub truncated: bool,
}

pub async fn capture_top_locks(
    request: &TopCaptureRequest,
    cancel: &CancellationToken,
) -> Result<TopResult<TopLocksData>, TopCaptureError> {
    request.validate_capture(TOOL_ID)?;
    if cancel.is_cancelled() {
        return request.cancelled(TOOL_ID);
    }
    let Some(global_manager) = rustfs_lock::get_initialized_global_lock_manager() else {
        return request.failed(TOOL_ID, 0, TopReasonCode::SourceUnavailable);
    };
    let Some(manager) = global_manager.as_fast_lock_manager() else {
        return request.unsupported(TOOL_ID, TopReasonCode::UnsupportedTool);
    };
    let Some(_permit) = request.acquire(cancel).await? else {
        return request.cancelled(TOOL_ID);
    };
    if !request.wait_window(TOOL_ID, cancel).await? {
        return request.cancelled(TOOL_ID);
    }
    let (held_count, waiting_count) = manager.current_lock_counts();
    // Report the admitted capture window. Scheduler wake-up jitter is not part
    // of the measurement and must not turn an exactly bounded job into a
    // LIMIT_EXCEEDED result.
    let duration_millis = u64::try_from(request.window.as_millis()).unwrap_or(u64::MAX).max(1);
    evaluate_lock_snapshot(request, held_count, waiting_count, duration_millis)
}

pub fn evaluate_lock_snapshot(
    request: &TopCaptureRequest,
    held_count: u64,
    waiting_count: u64,
    duration_millis: u64,
) -> Result<TopResult<TopLocksData>, TopCaptureError> {
    request.validate_scope(TOOL_ID)?;
    if duration_millis == 0 || duration_millis > request.limits.max_duration_millis {
        return Err(TopCaptureError::Limits);
    }
    if held_count > MAX_SAFE_INTEGER || waiting_count > MAX_SAFE_INTEGER {
        return request.failed(TOOL_ID, duration_millis, TopReasonCode::CollectionFailed);
    }

    request.succeeded(
        TOOL_ID,
        duration_millis,
        TopLocksData {
            held_count,
            waiting_count,
            truncated: false,
        },
    )
}
