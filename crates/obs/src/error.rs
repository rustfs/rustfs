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

use crate::OtelGuard;
use std::sync::{Arc, Mutex};
use tokio::sync::SetError;

/// Error type for global guard operations
#[derive(Debug, thiserror::Error)]
pub enum GlobalError {
    /// Occurs when attempting to set a global recorder (e.g., via [`crate::Recorder::install_global`] or [`metrics::set_global_recorder`])
    /// but a global recorder is already initialized.
    ///
    /// [`crate::Recorder::install_global`]: crate::Recorder::install_global
    /// [`metrics::set_global_recorder`]: https://docs.rs/metrics/latest/metrics/fn.set_global_recorder.html
    #[error("Failed to set a global recorder: {0}")]
    SetRecorder(#[from] metrics::SetRecorderError<crate::Recorder>),
    #[error("Failed to set global guard: {0}")]
    SetError(#[from] SetError<Arc<Mutex<OtelGuard>>>),
    #[error("Global guard not initialized")]
    NotInitialized,
    #[error("Global system metrics err: {0}")]
    MetricsError(String),
    #[error("Failed to get current PID: {0}")]
    PidError(String),
    #[error("Process with PID {0} not found")]
    ProcessNotFound(u32),
    #[error("Failed to get physical core count")]
    CoreCountError,
    #[error("GPU initialization failed: {0}")]
    GpuInitError(String),
    #[error("GPU device not found: {0}")]
    GpuDeviceError(String),
    #[error("Failed to send log: {0}")]
    SendFailed(&'static str),
    #[error("Operation timed out: {0}")]
    Timeout(&'static str),
    #[error("Telemetry initialization failed: {0}")]
    TelemetryError(#[from] TelemetryError),
}

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("Span exporter build failed: {0}")]
    BuildSpanExporter(String),
    #[error("Metric exporter build failed: {0}")]
    BuildMetricExporter(String),
    #[error("Log exporter build failed: {0}")]
    BuildLogExporter(String),
    #[error("Install metrics recorder failed: {0}")]
    InstallMetricsRecorder(String),
    #[error("Tracing subscriber init failed: {0}")]
    SubscriberInit(String),
    #[error("I/O error: {0}")]
    Io(String),
    #[error("Set permissions failed: {0}")]
    SetPermissions(String),
}

impl From<std::io::Error> for TelemetryError {
    fn from(e: std::io::Error) -> Self {
        TelemetryError::Io(e.to_string())
    }
}
