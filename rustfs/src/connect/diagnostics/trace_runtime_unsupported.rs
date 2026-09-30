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

pub(crate) struct LocalHealthRequest {
    pub offline_key_id: String,
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub capability: String,
    pub consent_uid: String,
    pub policy_revision: u64,
    pub consent_expires_at_unix: i64,
    pub acknowledge_l0: bool,
    pub expires_at_unix: i64,
}

pub(crate) struct LocalHealthArchive {
    pub artifact_uid: String,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

pub(crate) async fn request_local_health(
    _state_root: &Path,
    _request: LocalHealthRequest,
    _cancel: &CancellationToken,
) -> Result<LocalHealthArchive, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}

pub(crate) struct LocalNetworkRequest {
    pub offline_key_id: String,
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub consent_uid: String,
    pub policy_revision: u64,
    pub consent_expires_at_unix: i64,
    pub acknowledge_l1: bool,
    pub expires_at_unix: i64,
    pub duration_millis: u64,
    pub traffic_bytes: u64,
}

pub(crate) struct LocalNetworkArchive {
    pub artifact_uid: String,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

pub(crate) async fn request_local_network(
    _state_root: &Path,
    _request: LocalNetworkRequest,
    _cancel: &CancellationToken,
) -> Result<LocalNetworkArchive, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}

pub(crate) fn load_selected_offline_key(
    _state_root: &Path,
    _offline_key_id: &str,
) -> Result<crate::connect::DeviceIdentity, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}

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

pub(crate) async fn request_local_runtime_profile(
    _state_root: &Path,
    _request: super::profile_cpu::LocalRuntimeProfileRequest,
    _cancel: &CancellationToken,
) -> Result<super::profile_cpu::SignedProfileExport, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}

pub(crate) async fn request_local_native_threads_profile(
    _state_root: &Path,
    _request: super::profile_cpu::LocalRuntimeProfileRequest,
    _cancel: &CancellationToken,
) -> Result<super::profile_cpu::SignedProfileExport, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}

pub(crate) async fn request_local_top_disk(
    _state_root: &Path,
    _request: super::top_disk::LocalTopRequest,
    _cancel: &CancellationToken,
) -> Result<super::top_disk::LocalTopArchive, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}

pub(crate) async fn request_local_top_locks(
    _state_root: &Path,
    _request: super::top_disk::LocalTopRequest,
    _cancel: &CancellationToken,
) -> Result<super::top_disk::LocalTopArchive, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}

pub(crate) async fn request_local_top_api(
    _state_root: &Path,
    _request: super::top_disk::LocalTopRequest,
    _cancel: &CancellationToken,
) -> Result<super::top_disk::LocalTopArchive, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}

pub(crate) async fn request_local_top_rpc(
    _state_root: &Path,
    _request: super::top_disk::LocalTopRequest,
    _cancel: &CancellationToken,
) -> Result<super::top_disk::LocalTopArchive, LocalTraceCaptureError> {
    Err(LocalTraceCaptureError::RuntimeUnavailable)
}
