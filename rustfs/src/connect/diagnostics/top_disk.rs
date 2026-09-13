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

//! Bounded process disk-I/O window backed by RustFS's existing process sampler.

use serde::Serialize;
#[cfg(target_os = "linux")]
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::top_api::{MAX_SAFE_INTEGER, TopCaptureError, TopCaptureRequest, TopReasonCode, TopResult};

const TOOL_ID: &str = "top.disk";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskCounterSnapshot {
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub io_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopDiskData {
    pub resource_alias: &'static str,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub io_count: u64,
    pub window_millis: u64,
}

pub async fn capture_top_disk(
    request: &TopCaptureRequest,
    cancel: &CancellationToken,
) -> Result<TopResult<TopDiskData>, TopCaptureError> {
    request.validate_capture(TOOL_ID)?;
    if cancel.is_cancelled() {
        return request.cancelled(TOOL_ID);
    }

    #[cfg(not(target_os = "linux"))]
    return request.unsupported(TOOL_ID, TopReasonCode::UnsupportedPlatform);

    #[cfg(target_os = "linux")]
    {
        let Some(_permit) = request.acquire(cancel).await? else {
            return request.cancelled(TOOL_ID);
        };
        let mut sampler = rustfs_io_metrics::ProcessSampler::new();
        let before = process_snapshot(&mut sampler)?;
        let started = Instant::now();
        if !request.wait_window(TOOL_ID, cancel).await? {
            return request.cancelled(TOOL_ID);
        }
        let after = process_snapshot(&mut sampler)?;
        evaluate_disk_window(request, before, after, elapsed_millis(started.elapsed()))
    }
}

pub fn evaluate_disk_window(
    request: &TopCaptureRequest,
    before: DiskCounterSnapshot,
    after: DiskCounterSnapshot,
    window_millis: u64,
) -> Result<TopResult<TopDiskData>, TopCaptureError> {
    request.validate_scope(TOOL_ID)?;
    if window_millis == 0 || window_millis > request.limits.max_duration_millis {
        return Err(TopCaptureError::Limits);
    }
    let Some(read_bytes) = after.read_bytes.checked_sub(before.read_bytes) else {
        return request.failed(TOOL_ID, window_millis, TopReasonCode::CollectionFailed);
    };
    let Some(write_bytes) = after.write_bytes.checked_sub(before.write_bytes) else {
        return request.failed(TOOL_ID, window_millis, TopReasonCode::CollectionFailed);
    };
    let Some(io_count) = after.io_count.checked_sub(before.io_count) else {
        return request.failed(TOOL_ID, window_millis, TopReasonCode::CollectionFailed);
    };
    if [read_bytes, write_bytes, io_count]
        .into_iter()
        .any(|value| value > MAX_SAFE_INTEGER)
    {
        return request.failed(TOOL_ID, window_millis, TopReasonCode::CollectionFailed);
    }

    request.succeeded(
        TOOL_ID,
        window_millis,
        TopDiskData {
            // The v1 contract excludes disk paths. This alias denotes the
            // RustFS process-wide disk counter source for this one run.
            resource_alias: "resource-1",
            read_bytes,
            write_bytes,
            io_count,
            window_millis,
        },
    )
}

#[cfg(target_os = "linux")]
fn process_snapshot(sampler: &mut rustfs_io_metrics::ProcessSampler) -> Result<DiskCounterSnapshot, TopCaptureError> {
    let (_, snapshot) = sampler.snapshot_resource_and_system();
    let io_count = snapshot
        .syscall_read_total
        .checked_add(snapshot.syscall_write_total)
        .ok_or(TopCaptureError::Result)?;
    Ok(DiskCounterSnapshot {
        read_bytes: snapshot.io_read_bytes,
        write_bytes: snapshot.io_write_bytes,
        io_count,
    })
}

#[cfg(target_os = "linux")]
fn elapsed_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).max(1)
}
