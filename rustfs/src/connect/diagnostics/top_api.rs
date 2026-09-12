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

//! Shared bounded result/export contract and the `top.api` producer.

use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Write as _};
use std::path::Path;
use std::time::Duration;

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
use p256::pkcs8::DecodePrivateKey as _;
use rand::{TryRng as _, rngs::SysRng};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::connect::identity::DeviceIdentity;

pub const TOP_SCHEMA_VERSION: u8 = 1;
pub const TOP_CLASSIFICATION: &str = "L3";
pub const MAX_TOP_DURATION: Duration = Duration::from_secs(30);
pub const MAX_TOP_RESULT_BYTES: usize = 262_144;
pub const MAX_TOP_WORKING_MEMORY_BYTES: u64 = 67_108_864;
pub const MAX_TOP_RECORDS: u16 = 1_024;
pub const MAX_TOP_CPU_MILLIS: u64 = 5_000;
pub const MAX_TOP_TEMPORARY_BYTES: u64 = 2_097_152;
pub const MAX_TOP_TRAFFIC_BYTES: u64 = 1_048_576;
pub const MAX_TOP_BANDWIDTH_BYTES_PER_SECOND: u64 = 1_048_576;
pub const MAX_TOP_OPERATIONS: u16 = 1_024;
pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

const ENVELOPE_FORMAT: &str = "rustfs.connect.diagnosticEnvelope/1";
const PROTOCOL_VERSION: &str = "v1";
const SIGNATURE_ALGORITHM: &str = "ES256";
const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1";
const MAX_ENVELOPE_BYTES: usize = 16_384;
const MAX_ARCHIVE_BYTES: usize = 524_288;
const MAX_DECOMPRESSED_BYTES: usize = 278_528;
pub const MAX_TOP_EXPORT_VALIDITY: Duration = Duration::from_secs(2_592_000);
const TOP_CAPTURE_WORKING_SET_BYTES: u64 = 1_048_576;
const ENVELOPE_PATH: &str = "envelope.json";
const SIGNATURE_PATH: &str = "envelope.sig";
const RESULT_PATH: &str = "result.json";
const OUTPUT_MODE: u32 = 0o600;
static TOP_CAPTURE: Semaphore = Semaphore::const_new(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalTopConsent {
    pub uid: String,
    pub tool_id: String,
    pub classification: String,
    pub active: bool,
    pub expires_at_unix: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopCaptureScope {
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub policy_revision: u64,
    pub run_expires_at_unix: i64,
    pub executable_sha256: String,
    pub build_features: Vec<String>,
    pub consent: LocalTopConsent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopCaptureLimits {
    pub max_duration_millis: u64,
    pub max_result_bytes: usize,
    pub max_working_memory_bytes: u64,
    pub max_records: u16,
    pub max_concurrency: u8,
    pub max_cpu_millis: u64,
    pub max_temporary_bytes: u64,
    pub max_traffic_bytes: u64,
    pub max_bandwidth_bytes_per_second: u64,
    pub max_operations: u16,
}

impl Default for TopCaptureLimits {
    fn default() -> Self {
        Self {
            max_duration_millis: MAX_TOP_DURATION.as_millis() as u64,
            max_result_bytes: MAX_TOP_RESULT_BYTES,
            max_working_memory_bytes: MAX_TOP_WORKING_MEMORY_BYTES,
            max_records: MAX_TOP_RECORDS,
            max_concurrency: 1,
            max_cpu_millis: MAX_TOP_CPU_MILLIS,
            max_temporary_bytes: MAX_TOP_TEMPORARY_BYTES,
            max_traffic_bytes: MAX_TOP_TRAFFIC_BYTES,
            max_bandwidth_bytes_per_second: MAX_TOP_BANDWIDTH_BYTES_PER_SECOND,
            max_operations: MAX_TOP_OPERATIONS,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopCaptureRequest {
    pub scope: TopCaptureScope,
    pub limits: TopCaptureLimits,
    pub window: Duration,
    pub export_validity: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TopOutcome {
    Succeeded,
    Partial,
    Failed,
    Unsupported,
    Cancelled,
}

impl TopOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "SUCCEEDED",
            Self::Partial => "PARTIAL",
            Self::Failed => "FAILED",
            Self::Unsupported => "UNSUPPORTED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TopReasonCode {
    Complete,
    LimitExceeded,
    SourceUnavailable,
    UnsupportedTool,
    UnsupportedPlatform,
    Cancelled,
    CounterReset,
    CollectionFailed,
}

impl TopReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "COMPLETE",
            Self::LimitExceeded => "LIMIT_EXCEEDED",
            Self::SourceUnavailable => "SOURCE_UNAVAILABLE",
            Self::UnsupportedTool => "UNSUPPORTED_TOOL",
            Self::UnsupportedPlatform => "UNSUPPORTED_PLATFORM",
            Self::Cancelled => "CANCELLED",
            Self::CounterReset => "COUNTER_RESET",
            Self::CollectionFailed => "COLLECTION_FAILED",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopProvenance {
    repository: &'static str,
    source_commit: String,
    executable_sha256: String,
    rustfs_version: &'static str,
    os_family: &'static str,
    architecture: &'static str,
    build_features: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopCoverage {
    pub requested_units: u32,
    pub completed_units: u32,
    pub unit: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopResult<T: Serialize> {
    pub schema_version: u8,
    pub run_uid: String,
    pub tool_id: &'static str,
    pub capability: String,
    pub outcome: TopOutcome,
    pub reason_code: TopReasonCode,
    pub duration_millis: u64,
    pub provenance: TopProvenance,
    pub coverage: TopCoverage,
    pub data: Option<T>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TopApiOperation {
    GetObject,
    PutObject,
    HeadObject,
    ListObjects,
    InternalRpc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopApiData {
    pub operation: TopApiOperation,
    pub request_count: u64,
    pub error_count: u64,
    pub window_millis: u64,
    pub total_duration_micros: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedTopExport {
    pub artifact_uid: String,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
    pub envelope_json: Vec<u8>,
    pub envelope_signature: Vec<u8>,
    pub result_json: Vec<u8>,
    pub result_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedTopExport {
    pub artifact_uid: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TopCaptureError {
    #[error("connect_top_consent_required")]
    ConsentRequired,
    #[error("connect_top_consent_expired")]
    ConsentExpired,
    #[error("connect_top_consent_scope_invalid")]
    ConsentScope,
    #[error("connect_top_scope_invalid")]
    Scope,
    #[error("connect_top_provenance_invalid")]
    Provenance,
    #[error("connect_top_limits_invalid")]
    Limits,
    #[error("connect_top_run_expired")]
    Expired,
    #[error("connect_top_result_invalid")]
    Result,
    #[error("connect_top_cancelled")]
    Cancelled,
    #[error("connect_top_result_too_large")]
    ResultTooLarge,
    #[error("connect_top_serialization_failed")]
    Serialization,
    #[error("connect_top_signing_failed")]
    Signing,
    #[error("connect_top_random_failed")]
    Random,
    #[error("connect_top_export_exists")]
    AlreadyExists,
    #[error("connect_top_io_failed: {0:?}")]
    Io(std::io::ErrorKind),
    #[error("connect_top_export_durability_failed_after_commit: {0:?}")]
    DurabilityAfterCommit(std::io::ErrorKind),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticEnvelope<'a> {
    format_version: &'static str,
    protocol_version: &'static str,
    organization_name: &'a str,
    cluster_name: &'a str,
    device_name: &'a str,
    run_uid: &'a str,
    artifact_uid: &'a str,
    tool_id: &'static str,
    schema_version: u8,
    classification: &'static str,
    consent_uid: &'a str,
    policy_revision: u64,
    produced_at: String,
    expires_at: String,
    nonce: String,
    device_key_id: String,
    payload: DiagnosticPayload,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticPayload {
    path: &'static str,
    media_type: &'static str,
    size_bytes: usize,
    sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticSignature {
    algorithm: &'static str,
    key_id: String,
    value: String,
}

pub async fn capture_top_api(
    request: &TopCaptureRequest,
    _operation: TopApiOperation,
    cancel: &CancellationToken,
) -> Result<TopResult<TopApiData>, TopCaptureError> {
    request.validate_capture("top.api")?;
    if cancel.is_cancelled() {
        return request.cancelled("top.api");
    }

    // RustFS has exact bounded S3 operation/outcome counters, but no snapshot of
    // total request duration for the same operation/window. The v1 shape makes
    // that field mandatory, so publishing the available counters would invent a
    // duration or mix unrelated histograms.
    request.unsupported("top.api", TopReasonCode::UnsupportedTool)
}

pub fn sign_top_export<T: Serialize>(
    request: &TopCaptureRequest,
    result: &TopResult<T>,
    identity: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedTopExport, TopCaptureError> {
    if !matches!(result.tool_id, "top.api" | "top.disk" | "top.locks" | "top.net" | "top.rpc") {
        return Err(TopCaptureError::Result);
    }
    request.validate_scope(result.tool_id)?;
    if cancel.is_cancelled() {
        return Err(TopCaptureError::Cancelled);
    }
    if result.schema_version != TOP_SCHEMA_VERSION
        || result.run_uid != request.scope.run_uid
        || result.capability != format!("{}@{TOP_SCHEMA_VERSION}", result.tool_id)
        || result.provenance != provenance(&request.scope)?
        || !valid_export_outcome(result)
    {
        return Err(TopCaptureError::Result);
    }

    let data =
        serde_json::to_value(result.data.as_ref().ok_or(TopCaptureError::Result)?).map_err(|_| TopCaptureError::Serialization)?;
    if !data.is_object() {
        return Err(TopCaptureError::Result);
    }

    let result_json = serde_json::to_vec(result).map_err(|_| TopCaptureError::Serialization)?;
    if result_json.len() > request.limits.max_result_bytes || result_json.len() > MAX_TOP_RESULT_BYTES {
        return Err(TopCaptureError::ResultTooLarge);
    }
    let result_sha256 = hex_lower(&Sha256::digest(&result_json));
    let now = OffsetDateTime::now_utc();
    let export_validity = time::Duration::try_from(request.export_validity).map_err(|_| TopCaptureError::Expired)?;
    let requested_expiry = now
        .checked_add(export_validity)
        .ok_or(TopCaptureError::Expired)?
        .unix_timestamp();
    let expires_at_unix = requested_expiry
        .min(request.scope.run_expires_at_unix)
        .min(request.scope.consent.expires_at_unix);
    if expires_at_unix <= now.unix_timestamp() {
        return Err(TopCaptureError::Expired);
    }

    let mut nonce = [0u8; 32];
    SysRng.try_fill_bytes(&mut nonce).map_err(|_| TopCaptureError::Random)?;
    let device_key_id = hex_lower(&Sha256::digest(identity.public_key_der()));
    let envelope = DiagnosticEnvelope {
        format_version: ENVELOPE_FORMAT,
        protocol_version: PROTOCOL_VERSION,
        organization_name: &request.scope.organization_name,
        cluster_name: &request.scope.cluster_name,
        device_name: &request.scope.device_name,
        run_uid: &request.scope.run_uid,
        artifact_uid: &request.scope.artifact_uid,
        tool_id: result.tool_id,
        schema_version: TOP_SCHEMA_VERSION,
        classification: TOP_CLASSIFICATION,
        consent_uid: &request.scope.consent.uid,
        policy_revision: request.scope.policy_revision,
        produced_at: now.format(&Rfc3339).map_err(|_| TopCaptureError::Serialization)?,
        expires_at: OffsetDateTime::from_unix_timestamp(expires_at_unix)
            .map_err(|_| TopCaptureError::Expired)?
            .format(&Rfc3339)
            .map_err(|_| TopCaptureError::Serialization)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(nonce),
        device_key_id: device_key_id.clone(),
        payload: DiagnosticPayload {
            path: "result.json",
            media_type: "application/json",
            size_bytes: result_json.len(),
            sha256: result_sha256.clone(),
        },
    };
    let envelope_json = serde_json::to_vec(&envelope).map_err(|_| TopCaptureError::Serialization)?;
    if envelope_json.len() > MAX_ENVELOPE_BYTES {
        return Err(TopCaptureError::ResultTooLarge);
    }
    if cancel.is_cancelled() {
        return Err(TopCaptureError::Cancelled);
    }

    let key = identity.to_pkcs8_der().map_err(|_| TopCaptureError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(key.as_slice()).map_err(|_| TopCaptureError::Signing)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + 1 + envelope_json.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.push(0);
    input.extend_from_slice(&envelope_json);
    let signature: Signature = signing_key.sign(&input);
    let signature = signature.normalize_s();
    let envelope_signature = serde_json::to_vec(&DiagnosticSignature {
        algorithm: SIGNATURE_ALGORITHM,
        key_id: device_key_id,
        value: URL_SAFE_NO_PAD.encode_to_string(signature.to_bytes()),
    })
    .map_err(|_| TopCaptureError::Serialization)?;
    let decompressed_size = envelope_json
        .len()
        .checked_add(envelope_signature.len())
        .and_then(|size| size.checked_add(result_json.len()))
        .ok_or(TopCaptureError::ResultTooLarge)?;
    if decompressed_size > MAX_DECOMPRESSED_BYTES {
        return Err(TopCaptureError::ResultTooLarge);
    }
    if cancel.is_cancelled() {
        return Err(TopCaptureError::Cancelled);
    }
    let archive_bytes = archive(&envelope_json, &envelope_signature, &result_json)?;
    if archive_bytes.len() > MAX_ARCHIVE_BYTES {
        return Err(TopCaptureError::ResultTooLarge);
    }
    let archive_sha256 = hex_lower(&Sha256::digest(&archive_bytes));

    Ok(SignedTopExport {
        artifact_uid: request.scope.artifact_uid.clone(),
        archive_bytes,
        archive_sha256,
        envelope_json,
        envelope_signature,
        result_json,
        result_sha256,
    })
}

fn valid_export_outcome<T: Serialize>(result: &TopResult<T>) -> bool {
    if result.duration_millis == 0
        || result.duration_millis > MAX_TOP_DURATION.as_millis() as u64
        || result.coverage.unit != "WINDOW"
    {
        return false;
    }
    match result.outcome {
        TopOutcome::Succeeded => {
            result.reason_code == TopReasonCode::Complete
                && result.data.is_some()
                && result.coverage.requested_units == 1
                && result.coverage.completed_units == 1
        }
        TopOutcome::Partial => {
            matches!(
                result.reason_code,
                TopReasonCode::LimitExceeded | TopReasonCode::SourceUnavailable | TopReasonCode::CounterReset
            ) && result.data.is_some()
                && result.coverage.completed_units > 0
                && result.coverage.completed_units < result.coverage.requested_units
                && result.coverage.requested_units <= 1_048_576
        }
        TopOutcome::Failed | TopOutcome::Unsupported | TopOutcome::Cancelled => false,
    }
}

pub fn save_signed_top_export(
    output: &Path,
    export: &SignedTopExport,
    cancel: &CancellationToken,
) -> Result<SavedTopExport, TopCaptureError> {
    if cancel.is_cancelled() {
        return Err(TopCaptureError::Cancelled);
    }
    if !is_uuid_v7(&export.artifact_uid) {
        return Err(TopCaptureError::Scope);
    }
    if export.archive_bytes.is_empty()
        || export.archive_bytes.len() > MAX_ARCHIVE_BYTES
        || hex_lower(&Sha256::digest(&export.archive_bytes)) != export.archive_sha256
    {
        return Err(TopCaptureError::Result);
    }
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = output.file_name().ok_or(TopCaptureError::Scope)?.to_string_lossy();
    let temporary = parent.join(format!(".{filename}.{}.partial", export.artifact_uid));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(OUTPUT_MODE);
    }
    let mut file = options.open(&temporary).map_err(map_create_error)?;
    let result = (|| {
        file.write_all(&export.archive_bytes).map_err(io_error)?;
        if cancel.is_cancelled() {
            return Err(TopCaptureError::Cancelled);
        }
        file.sync_all().map_err(io_error)?;
        if cancel.is_cancelled() {
            return Err(TopCaptureError::Cancelled);
        }
        fs::hard_link(&temporary, output).map_err(map_publish_error)?;
        if let Err(error) = fs::remove_file(&temporary) {
            return Err(TopCaptureError::DurabilityAfterCommit(error.kind()));
        }
        #[cfg(unix)]
        if let Err(error) = File::open(parent).and_then(|directory| directory.sync_all()) {
            return Err(TopCaptureError::DurabilityAfterCommit(error.kind()));
        }
        Ok(SavedTopExport {
            artifact_uid: export.artifact_uid.clone(),
            archive_size_bytes: export.archive_bytes.len() as u64,
            archive_sha256: export.archive_sha256.clone(),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

impl TopCaptureRequest {
    pub(crate) fn validate_capture(&self, tool_id: &'static str) -> Result<(), TopCaptureError> {
        self.validate_scope(tool_id)?;
        if self.window.is_zero()
            || self.window > MAX_TOP_DURATION
            || self.window.as_millis() > u128::from(self.limits.max_duration_millis)
        {
            return Err(TopCaptureError::Limits);
        }
        let remaining = self
            .scope
            .run_expires_at_unix
            .min(self.scope.consent.expires_at_unix)
            .checked_sub(unix_now()?)
            .ok_or(TopCaptureError::Expired)?;
        if remaining <= 0 || self.window.as_secs_f64().ceil() as i64 > remaining {
            return Err(TopCaptureError::Expired);
        }
        Ok(())
    }

    pub(crate) fn validate_scope(&self, tool_id: &'static str) -> Result<(), TopCaptureError> {
        let now = unix_now()?;
        validate_resource_scope(&self.scope)?;
        validate_limits(self.limits)?;
        validate_provenance(&self.scope)?;
        if !self.scope.consent.active {
            return Err(TopCaptureError::ConsentRequired);
        }
        if self.scope.consent.classification != TOP_CLASSIFICATION || self.scope.consent.tool_id != tool_id {
            return Err(TopCaptureError::ConsentScope);
        }
        if self.scope.consent.expires_at_unix <= now {
            return Err(TopCaptureError::ConsentExpired);
        }
        if self.scope.run_expires_at_unix <= now {
            return Err(TopCaptureError::Expired);
        }
        if self.export_validity.is_zero() || self.export_validity > MAX_TOP_EXPORT_VALIDITY {
            return Err(TopCaptureError::Limits);
        }
        Ok(())
    }

    pub(crate) async fn acquire(&self, cancel: &CancellationToken) -> Result<Option<SemaphorePermit<'static>>, TopCaptureError> {
        tokio::select! {
            permit = TOP_CAPTURE.acquire() => Ok(Some(permit.map_err(|_| TopCaptureError::Limits)?)),
            () = cancel.cancelled() => Ok(None),
        }
    }

    pub(crate) async fn wait_window(&self, tool_id: &'static str, cancel: &CancellationToken) -> Result<bool, TopCaptureError> {
        tokio::select! {
            () = tokio::time::sleep(self.window) => {
                self.validate_scope(tool_id)?;
                Ok(true)
            }
            () = cancel.cancelled() => Ok(false),
        }
    }

    pub(crate) fn succeeded<T: Serialize>(
        &self,
        tool_id: &'static str,
        duration_millis: u64,
        data: T,
    ) -> Result<TopResult<T>, TopCaptureError> {
        let result = TopResult {
            schema_version: TOP_SCHEMA_VERSION,
            run_uid: self.scope.run_uid.clone(),
            tool_id,
            capability: format!("{tool_id}@{TOP_SCHEMA_VERSION}"),
            outcome: TopOutcome::Succeeded,
            reason_code: TopReasonCode::Complete,
            duration_millis,
            provenance: provenance(&self.scope)?,
            coverage: TopCoverage {
                requested_units: 1,
                completed_units: 1,
                unit: "WINDOW",
            },
            data: Some(data),
        };
        self.bound_result(result)
    }

    pub(crate) fn failed<T: Serialize>(
        &self,
        tool_id: &'static str,
        duration_millis: u64,
        reason_code: TopReasonCode,
    ) -> Result<TopResult<T>, TopCaptureError> {
        self.terminal(tool_id, duration_millis, TopOutcome::Failed, reason_code)
    }

    pub(crate) fn unsupported<T: Serialize>(
        &self,
        tool_id: &'static str,
        reason_code: TopReasonCode,
    ) -> Result<TopResult<T>, TopCaptureError> {
        self.terminal(tool_id, 0, TopOutcome::Unsupported, reason_code)
    }

    pub(crate) fn cancelled<T: Serialize>(&self, tool_id: &'static str) -> Result<TopResult<T>, TopCaptureError> {
        self.terminal(tool_id, 0, TopOutcome::Cancelled, TopReasonCode::Cancelled)
    }

    fn terminal<T: Serialize>(
        &self,
        tool_id: &'static str,
        duration_millis: u64,
        outcome: TopOutcome,
        reason_code: TopReasonCode,
    ) -> Result<TopResult<T>, TopCaptureError> {
        let result = TopResult {
            schema_version: TOP_SCHEMA_VERSION,
            run_uid: self.scope.run_uid.clone(),
            tool_id,
            capability: format!("{tool_id}@{TOP_SCHEMA_VERSION}"),
            outcome,
            reason_code,
            duration_millis,
            provenance: provenance(&self.scope)?,
            coverage: TopCoverage {
                requested_units: 1,
                completed_units: 0,
                unit: "WINDOW",
            },
            data: None,
        };
        self.bound_result(result)
    }

    fn bound_result<T: Serialize>(&self, result: TopResult<T>) -> Result<TopResult<T>, TopCaptureError> {
        let size = serde_json::to_vec(&result).map_err(|_| TopCaptureError::Serialization)?.len();
        if size > self.limits.max_result_bytes || size > MAX_TOP_RESULT_BYTES {
            return Err(TopCaptureError::ResultTooLarge);
        }
        Ok(result)
    }
}

fn validate_resource_scope(scope: &TopCaptureScope) -> Result<(), TopCaptureError> {
    let organization_uid = scope
        .organization_name
        .strip_prefix("organizations/")
        .filter(|tail| !tail.contains('/'))
        .ok_or(TopCaptureError::Scope)?;
    let cluster_prefix = format!("{}/clusters/", scope.organization_name);
    let cluster_uid = scope
        .cluster_name
        .strip_prefix(&cluster_prefix)
        .filter(|tail| !tail.contains('/'))
        .ok_or(TopCaptureError::Scope)?;
    let device_prefix = format!("{}/clusterDevices/", scope.cluster_name);
    let device_uid = scope
        .device_name
        .strip_prefix(&device_prefix)
        .filter(|tail| !tail.contains('/'))
        .ok_or(TopCaptureError::Scope)?;
    if scope.policy_revision == 0
        || ![
            organization_uid,
            cluster_uid,
            device_uid,
            scope.run_uid.as_str(),
            scope.artifact_uid.as_str(),
            scope.consent.uid.as_str(),
        ]
        .into_iter()
        .all(is_uuid_v7)
    {
        return Err(TopCaptureError::Scope);
    }
    Ok(())
}

fn validate_limits(limits: TopCaptureLimits) -> Result<(), TopCaptureError> {
    if limits.max_duration_millis == 0
        || limits.max_duration_millis > MAX_TOP_DURATION.as_millis() as u64
        || limits.max_result_bytes == 0
        || limits.max_result_bytes > MAX_TOP_RESULT_BYTES
        || limits.max_working_memory_bytes < TOP_CAPTURE_WORKING_SET_BYTES
        || limits.max_working_memory_bytes > MAX_TOP_WORKING_MEMORY_BYTES
        || limits.max_records == 0
        || limits.max_records > MAX_TOP_RECORDS
        || limits.max_concurrency != 1
        || limits.max_cpu_millis == 0
        || limits.max_cpu_millis > MAX_TOP_CPU_MILLIS
        || limits.max_temporary_bytes > MAX_TOP_TEMPORARY_BYTES
        || limits.max_traffic_bytes > MAX_TOP_TRAFFIC_BYTES
        || limits.max_bandwidth_bytes_per_second > MAX_TOP_BANDWIDTH_BYTES_PER_SECOND
        || limits.max_operations == 0
        || limits.max_operations > MAX_TOP_OPERATIONS
    {
        return Err(TopCaptureError::Limits);
    }
    Ok(())
}

fn validate_provenance(scope: &TopCaptureScope) -> Result<(), TopCaptureError> {
    if !lower_hex(&scope.executable_sha256, 64)
        || scope.build_features.len() > 64
        || scope.build_features.iter().any(|feature| {
            feature.is_empty()
                || feature.len() > 64
                || !feature.as_bytes()[0].is_ascii_lowercase()
                || !feature
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-')
        })
    {
        return Err(TopCaptureError::Provenance);
    }
    let commit = crate::version::build::COMMIT_HASH;
    if !lower_hex(commit, 40) {
        return Err(TopCaptureError::Provenance);
    }
    Ok(())
}

fn provenance(scope: &TopCaptureScope) -> Result<TopProvenance, TopCaptureError> {
    validate_provenance(scope)?;
    let mut build_features = scope.build_features.clone();
    build_features.sort();
    build_features.dedup();
    Ok(TopProvenance {
        repository: "rustfs/rustfs",
        source_commit: crate::version::build::COMMIT_HASH.to_owned(),
        executable_sha256: scope.executable_sha256.clone(),
        rustfs_version: env!("CARGO_PKG_VERSION"),
        os_family: match std::env::consts::OS {
            "linux" => "LINUX",
            "macos" => "DARWIN",
            "windows" => "WINDOWS",
            "freebsd" => "FREEBSD",
            _ => "OTHER",
        },
        architecture: match std::env::consts::ARCH {
            "x86_64" => "x86_64",
            "aarch64" => "aarch64",
            _ => "other",
        },
        build_features,
    })
}

fn archive(envelope: &[u8], signature: &[u8], result: &[u8]) -> Result<Vec<u8>, TopCaptureError> {
    let cursor = Cursor::new(Vec::with_capacity(envelope.len() + signature.len() + result.len() + 512));
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(OUTPUT_MODE);
    for (name, bytes) in [(ENVELOPE_PATH, envelope), (SIGNATURE_PATH, signature), (RESULT_PATH, result)] {
        writer.start_file(name, options).map_err(|_| TopCaptureError::Serialization)?;
        writer.write_all(bytes).map_err(io_error)?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|_| TopCaptureError::Serialization)
}

fn map_create_error(error: std::io::Error) -> TopCaptureError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        TopCaptureError::AlreadyExists
    } else {
        io_error(error)
    }
}

fn map_publish_error(error: std::io::Error) -> TopCaptureError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        TopCaptureError::AlreadyExists
    } else {
        io_error(error)
    }
}

fn io_error(error: std::io::Error) -> TopCaptureError {
    TopCaptureError::Io(error.kind())
}

fn unix_now() -> Result<i64, TopCaptureError> {
    Ok(OffsetDateTime::now_utc().unix_timestamp())
}

fn is_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}

fn lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_lower(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}
