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

use crate::{OtelGuard, TelemetryError};
use std::sync::{Arc, Mutex};
use tokio::sync::SetError;

/// Error type for global guard operations
#[derive(Debug, thiserror::Error)]
pub enum GlobalError {
    /// *Called from [`Recorder::install_global`]*: Failed to set
    /// a global recorder as one is already initialised.
    ///
    /// [`Recorder::install_global`]: struct.Recorder.html#method.install_global
    /// [`Recorder`]: struct.Recorder.html
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
