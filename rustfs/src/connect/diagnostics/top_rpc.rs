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

//! Honest `top.rpc` capability result.

use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::top_api::{TopCaptureError, TopCaptureRequest, TopReasonCode, TopResult};

const TOOL_ID: &str = "top.rpc";

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

    // Internode metrics expose traffic, dial failures, and average dial time.
    // They do not expose one matching RPC request/error/duration cohort, so v1
    // cannot be produced without mixing unrelated counters.
    request.unsupported(TOOL_ID, TopReasonCode::UnsupportedTool)
}
