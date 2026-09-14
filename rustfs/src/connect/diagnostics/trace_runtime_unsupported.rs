// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::path::Path;

use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{TelemetryProducerError, TraceRecordCapture, TraceRecordLimits};

#[derive(Debug, Error)]
pub(crate) enum LocalTraceCaptureError {
    #[error("telemetry server runtime is unavailable")]
    RuntimeUnavailable,
    #[error("telemetry server runtime state is not owner-only")]
    StateSecurity,
    #[error("telemetry server runtime protocol failed")]
    Protocol,
    #[error("telemetry server runtime I/O failed")]
    Io(#[source] io::Error),
    #[error(transparent)]
    Producer(#[from] TelemetryProducerError),
}

pub(crate) struct LocalTraceCaptureRuntime;

impl LocalTraceCaptureRuntime {
    pub async fn shutdown(self) {}
}

pub(crate) fn spawn_local_trace_capture_runtime(
    _state_root: &Path,
    _parent_shutdown: &CancellationToken,
) -> Result<LocalTraceCaptureRuntime, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}

pub(crate) async fn request_local_trace_capture(
    _state_root: &Path,
    _consent_expires_at_unix: i64,
    _limits: TraceRecordLimits,
    _cancel: &CancellationToken,
) -> Result<TraceRecordCapture, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}
