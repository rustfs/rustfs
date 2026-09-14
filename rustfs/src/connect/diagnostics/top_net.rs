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

//! Bounded host-network window backed by a persistent OS interface snapshot.

use serde::Serialize;
use sysinfo::Networks;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::top_api::{MAX_SAFE_INTEGER, TopCaptureError, TopCaptureRequest, TopReasonCode, TopResult};

const TOOL_ID: &str = "top.net";
pub const TOP_NET_CAPABILITY: &str = "top.net@1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetworkCounterSnapshot {
    pub received_bytes: u64,
    pub sent_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopNetData {
    pub received_bytes: u64,
    pub sent_bytes: u64,
    pub window_millis: u64,
}

pub async fn capture_top_net(
    request: &TopCaptureRequest,
    cancel: &CancellationToken,
) -> Result<TopResult<TopNetData>, TopCaptureError> {
    request.validate_capture(TOOL_ID)?;
    if cancel.is_cancelled() {
        return request.cancelled(TOOL_ID);
    }
    let Some(_permit) = request.acquire(cancel).await? else {
        return request.cancelled(TOOL_ID);
    };
    if !sysinfo::IS_SUPPORTED_SYSTEM {
        return request.failed(TOOL_ID, 0, TopReasonCode::SourceUnavailable);
    }
    let mut networks = Networks::new();
    networks.refresh(true);
    if networks.is_empty() {
        return request.failed(TOOL_ID, 0, TopReasonCode::SourceUnavailable);
    }
    let started = Instant::now();
    if !request.wait_window(TOOL_ID, cancel).await? {
        return request.cancelled(TOOL_ID);
    }
    let observed = rustfs_obs::metrics::stats_collector::collect_host_network_stats(&mut networks);
    evaluate_network_window(
        request,
        NetworkCounterSnapshot {
            received_bytes: 0,
            sent_bytes: 0,
        },
        NetworkCounterSnapshot {
            received_bytes: observed.total_received,
            sent_bytes: observed.total_transmitted,
        },
        elapsed_millis(started.elapsed()),
    )
}

pub fn evaluate_network_window(
    request: &TopCaptureRequest,
    before: NetworkCounterSnapshot,
    after: NetworkCounterSnapshot,
    window_millis: u64,
) -> Result<TopResult<TopNetData>, TopCaptureError> {
    request.validate_scope(TOOL_ID)?;
    if window_millis == 0 || window_millis > request.limits.max_duration_millis {
        return Err(TopCaptureError::Limits);
    }
    let Some(received_bytes) = after.received_bytes.checked_sub(before.received_bytes) else {
        return request.failed(TOOL_ID, window_millis, TopReasonCode::CollectionFailed);
    };
    let Some(sent_bytes) = after.sent_bytes.checked_sub(before.sent_bytes) else {
        return request.failed(TOOL_ID, window_millis, TopReasonCode::CollectionFailed);
    };
    if received_bytes > MAX_SAFE_INTEGER || sent_bytes > MAX_SAFE_INTEGER {
        return request.failed(TOOL_ID, window_millis, TopReasonCode::CollectionFailed);
    }
    if received_bytes == 0 && sent_bytes == 0 {
        return request.failed(TOOL_ID, window_millis, TopReasonCode::SourceUnavailable);
    }

    request.succeeded(
        TOOL_ID,
        window_millis,
        TopNetData {
            received_bytes,
            sent_bytes,
            window_millis,
        },
    )
}

fn elapsed_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).max(1)
}
