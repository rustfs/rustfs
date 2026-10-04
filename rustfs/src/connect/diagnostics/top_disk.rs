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

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::top_api::{MAX_SAFE_INTEGER, TopCaptureError, TopCaptureRequest, TopReasonCode, TopResult};

const TOOL_ID: &str = "top.disk";
pub const TOP_DISK_CAPABILITY: &str = "top.disk@1";

#[cfg(all(test, target_os = "linux"))]
pub(super) static AFTER_INITIAL_DISK_SNAPSHOT: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();

/// Closed local-service request for supported top tools: no process selector, paths, or supplied provenance.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LocalTopRequest {
    pub offline_key_id: String,
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub consent_uid: String,
    pub policy_revision: u64,
    pub consent_expires_at_unix: i64,
    pub acknowledge_l3: bool,
    pub run_expires_at_unix: i64,
    pub window_millis: u64,
    pub export_validity_seconds: u64,
}

/// Archive received from the owner-only service socket.
pub(crate) struct LocalTopArchive {
    pub artifact_uid: String,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

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
        // A queued request may outlive its authorization while another capture owns the lease.
        request.validate_capture(TOOL_ID)?;
        if cancel.is_cancelled() {
            return request.cancelled(TOOL_ID);
        }
        let mut sampler = rustfs_io_metrics::ProcessSampler::new();
        let before = process_snapshot(&mut sampler)?;
        #[cfg(test)]
        if let Some(write_fixture_data) = AFTER_INITIAL_DISK_SNAPSHOT.get() {
            write_fixture_data();
        }
        if !request.wait_window(TOOL_ID, cancel).await? {
            return request.cancelled(TOOL_ID);
        }
        let after = process_snapshot(&mut sampler)?;
        // Report the authorized window, as Top API does. The timer can only
        // overshoot it, and validate_capture already bounded it by the limit;
        // measuring the sleep would reject a capture that ran as requested.
        let window_millis = u64::try_from(request.window.as_millis()).map_err(|_| TopCaptureError::Limits)?;
        evaluate_disk_window(request, before, after, window_millis)
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
