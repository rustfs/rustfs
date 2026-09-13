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

//! Consent-bound object-to-deployment performance measurement.
//!
//! The producer creates a dedicated temporary bucket, writes or reads one
//! generated object, and removes the namespace before returning. It never
//! accepts customer bucket or object names and keeps credentials local.

use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Cursor, Read, Write as _};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use bytes::Bytes;
use futures::StreamExt as _;
use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
use p256::pkcs8::DecodePrivateKey as _;
use reqwest::{Client, Method, Response, StatusCode, Url};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};
use zeroize::Zeroizing;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::connect::DeviceIdentity;

pub const OBJECT_SCHEMA_VERSION: u16 = 1;
pub const OBJECT_TOOL_ID: &str = "performance.object";
pub const OBJECT_CAPABILITY: &str = "performance.object@1";
pub const MAX_OBJECT_DURATION: Duration = Duration::from_secs(30);
pub const MAX_OBJECT_TRAFFIC_BYTES: u64 = 1_048_576;
pub const MAX_OBJECT_BANDWIDTH_BYTES_PER_SECOND: u64 = 1_048_576;
pub const MAX_OBJECT_RESULT_BYTES: usize = 262_144;

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_BUILD_FEATURES: usize = 64;
const MAX_VALIDITY_SECONDS: i64 = 2_592_000;
const MAX_FUTURE_SKEW_SECONDS: i64 = 300;
const MAX_ENVELOPE_BYTES: usize = 16_384;
const MAX_ARCHIVE_BYTES: usize = 524_288;
const MAX_DECOMPRESSED_BYTES: usize = 278_528;
const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";
const ENVELOPE_PATH: &str = "envelope.json";
const SIGNATURE_PATH: &str = "envelope.sig";
const RESULT_PATH: &str = "result.json";
const MAX_OBJECT_RESPONSE_BYTES: usize = 16_384;
const CLEANUP_RESERVE_MAX: Duration = Duration::from_secs(1);
const OUTPUT_MODE: u32 = 0o600;

static OBJECT_COLLECTOR_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObjectOperation {
    GetObject,
    PutObject,
}

impl ObjectOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GetObject => "GET_OBJECT",
            Self::PutObject => "PUT_OBJECT",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObjectOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

impl ObjectOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObjectReasonCode {
    Complete,
    SourceUnavailable,
    PermissionDenied,
    Cancelled,
    CollectionFailed,
}

impl ObjectReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "COMPLETE",
            Self::SourceUnavailable => "SOURCE_UNAVAILABLE",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::Cancelled => "CANCELLED",
            Self::CollectionFailed => "COLLECTION_FAILED",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObjectTargetReasonCode {
    Complete,
    EndpointUnavailable,
    ProxyFailure,
    PermissionDenied,
    TimedOut,
    Cancelled,
    NamespaceConflict,
    CleanupFailed,
    ProtocolFailure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectTargetParameters {
    pub operation: ObjectOperation,
    pub requested_bytes: u64,
    pub duration_millis: u64,
    pub concurrency: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectTargetUnits {
    pub bytes: &'static str,
    pub duration: &'static str,
    pub latency: &'static str,
    pub operation_count: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectProvenance {
    repository: &'static str,
    source_commit: String,
    executable_sha256: String,
    rustfs_version: String,
    os_family: ObjectOsFamily,
    architecture: ObjectArchitecture,
    build_features: Vec<String>,
}

impl ObjectProvenance {
    pub fn new(
        source_commit: impl Into<String>,
        executable_sha256: impl Into<String>,
        rustfs_version: impl Into<String>,
        build_features: Vec<String>,
    ) -> Self {
        Self {
            repository: "rustfs/rustfs",
            source_commit: source_commit.into(),
            executable_sha256: executable_sha256.into(),
            rustfs_version: rustfs_version.into(),
            os_family: ObjectOsFamily::current(),
            architecture: ObjectArchitecture::current(),
            build_features,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ObjectOsFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

impl ObjectOsFamily {
    fn current() -> Self {
        match std::env::consts::OS {
            "linux" => Self::Linux,
            "macos" => Self::Darwin,
            "windows" => Self::Windows,
            "freebsd" => Self::Freebsd,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum ObjectArchitecture {
    #[serde(rename = "x86_64")]
    X86_64,
    Aarch64,
    Other,
}

impl ObjectArchitecture {
    fn current() -> Self {
        match std::env::consts::ARCH {
            "x86_64" => Self::X86_64,
            "aarch64" => Self::Aarch64,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalObjectConsent {
    pub consent_uid: String,
    pub policy_revision: u64,
    pub expires_at_unix: i64,
    pub confirmed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectPerformanceRequest {
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub capability: String,
    pub consent: LocalObjectConsent,
    pub produced_at_unix: i64,
    pub expires_at_unix: i64,
    pub nonce: [u8; 32],
    pub duration: Duration,
    pub operation: ObjectOperation,
    pub traffic_bytes: u64,
    pub target_alias: String,
    pub provenance: ObjectProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectPerformanceData {
    pub operation: ObjectOperation,
    pub transferred_bytes: u64,
    pub completed_operations: u64,
    pub duration_millis: u64,
    pub error_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectCoverage {
    requested_units: u32,
    completed_units: u32,
    unit: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectDiagnosticResult {
    schema_version: u16,
    run_uid: String,
    tool_id: &'static str,
    capability: &'static str,
    outcome: ObjectOutcome,
    reason_code: ObjectReasonCode,
    duration_millis: u64,
    provenance: ObjectProvenance,
    coverage: ObjectCoverage,
    data: Option<ObjectPerformanceData>,
}

impl ObjectDiagnosticResult {
    pub fn outcome(&self) -> ObjectOutcome {
        self.outcome
    }

    pub fn reason_code(&self) -> ObjectReasonCode {
        self.reason_code
    }

    pub fn data(&self) -> Option<&ObjectPerformanceData> {
        self.data.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectTargetResult {
    pub target_alias: String,
    pub outcome: ObjectOutcome,
    pub reason_code: ObjectTargetReasonCode,
    pub parameters: ObjectTargetParameters,
    pub units: ObjectTargetUnits,
    pub transferred_bytes: u64,
    pub completed_operations: u64,
    pub latency_micros: Option<u64>,
    pub duration_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeasurement {
    pub result: ObjectDiagnosticResult,
    pub target: ObjectTargetResult,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectProbeMeasurement {
    pub transferred_bytes: u64,
    pub duration: Duration,
    pub latency: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectProbeError {
    EndpointUnavailable,
    ProxyFailure,
    PermissionDenied,
    TimedOut,
    Cancelled,
    NamespaceConflict,
    CleanupFailed,
    ProtocolFailure,
}

pub type ObjectProbeFuture<'a> = Pin<Box<dyn Future<Output = Result<ObjectProbeMeasurement, ObjectProbeError>> + Send + 'a>>;

pub trait ObjectProbe: Send + Sync {
    fn probe<'a>(&'a self, request: &'a ObjectPerformanceRequest, cancel: &'a CancellationToken) -> ObjectProbeFuture<'a>;
}

pub struct S3ObjectProbe {
    endpoint: Url,
    client: Client,
    access_key: Zeroizing<String>,
    secret_key: Zeroizing<String>,
    session_token: Zeroizing<String>,
    proxy_configured: bool,
}

impl S3ObjectProbe {
    pub fn new(
        endpoint: &str,
        root_ca_pem: Option<&[u8]>,
        proxy: Option<&str>,
        access_key: Zeroizing<String>,
        secret_key: Zeroizing<String>,
        session_token: Zeroizing<String>,
        timeout: Duration,
    ) -> Result<Self, ObjectPerformanceError> {
        let endpoint = deployment_endpoint(endpoint)?;
        if access_key.is_empty() || secret_key.is_empty() {
            return Err(ObjectPerformanceError::InvalidCredential);
        }
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout);
        if let Some(root_ca_pem) = root_ca_pem {
            let certificate =
                reqwest::Certificate::from_pem(root_ca_pem).map_err(|_| ObjectPerformanceError::InvalidRootCertificate)?;
            builder = builder.add_root_certificate(certificate);
        }
        if let Some(proxy) = proxy {
            let proxy_url = proxy_url(proxy)?;
            builder = builder.proxy(reqwest::Proxy::all(proxy_url).map_err(|_| ObjectPerformanceError::InvalidProxy)?);
        }
        let client = builder.build().map_err(|_| ObjectPerformanceError::TransportConfiguration)?;
        Ok(Self {
            endpoint,
            client,
            access_key,
            secret_key,
            session_token,
            proxy_configured: proxy.is_some(),
        })
    }

    async fn execute(
        &self,
        request: &ObjectPerformanceRequest,
        cancel: &CancellationToken,
    ) -> Result<ObjectProbeMeasurement, ObjectProbeError> {
        let started = Instant::now();
        let reserve = CLEANUP_RESERVE_MAX.min(request.duration / 4);
        let operation_deadline = started + request.duration.saturating_sub(reserve);
        let cleanup_deadline = started + request.duration;
        let bucket = format!("rustfs-connect-perf-{}", request.artifact_uid.replace('-', ""));
        let object = "synthetic-object";
        let bucket_url = self.object_url(&bucket, None)?;
        let object_url = self.object_url(&bucket, Some(object))?;
        let payload_len = usize::try_from(request.traffic_bytes).map_err(|_| ObjectProbeError::ProtocolFailure)?;
        let payload = Bytes::from(vec![0xa5; payload_len]);

        let create = self
            .send(Method::PUT, bucket_url.clone(), Bytes::new(), operation_deadline, Some(cancel))
            .await?;
        if create.status() == StatusCode::CONFLICT {
            return Err(ObjectProbeError::NamespaceConflict);
        }
        if !create.status().is_success() {
            return self.status_error(create.status());
        }
        let measurement = match self.require_success(create, operation_deadline, Some(cancel)).await {
            Ok(()) => {
                self.measure_in_namespace(request, object_url.clone(), payload, operation_deadline, cancel)
                    .await
            }
            Err(error) => Err(error),
        };
        let cleanup = self.cleanup(object_url, bucket_url, cleanup_deadline).await;
        if cleanup.is_err() {
            return Err(ObjectProbeError::CleanupFailed);
        }
        measurement.map(|(transferred_bytes, latency)| ObjectProbeMeasurement {
            transferred_bytes,
            duration: latency,
            latency,
        })
    }

    async fn measure_in_namespace(
        &self,
        request: &ObjectPerformanceRequest,
        object_url: Url,
        payload: Bytes,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<(u64, Duration), ObjectProbeError> {
        if request.operation == ObjectOperation::GetObject {
            let preload = self
                .send(Method::PUT, object_url.clone(), payload.clone(), deadline, Some(cancel))
                .await?;
            self.require_success(preload, deadline, Some(cancel)).await?;
        }

        let operation_started = Instant::now();
        match request.operation {
            ObjectOperation::PutObject => {
                let response = self.send(Method::PUT, object_url, payload, deadline, Some(cancel)).await?;
                self.require_success(response, deadline, Some(cancel)).await?;
            }
            ObjectOperation::GetObject => {
                let response = self
                    .send(Method::GET, object_url, Bytes::new(), deadline, Some(cancel))
                    .await?;
                if !response.status().is_success() {
                    return self.status_error(response.status());
                }
                let body = self
                    .read_response_body(response, request.traffic_bytes, deadline, Some(cancel))
                    .await?;
                if body.len() != usize::try_from(request.traffic_bytes).map_err(|_| ObjectProbeError::ProtocolFailure)?
                    || body.iter().any(|byte| *byte != 0xa5)
                {
                    return Err(ObjectProbeError::ProtocolFailure);
                }
            }
        }
        Ok((request.traffic_bytes, operation_started.elapsed()))
    }

    async fn cleanup(&self, object_url: Url, bucket_url: Url, deadline: Instant) -> Result<(), ObjectProbeError> {
        let object = self.send(Method::DELETE, object_url, Bytes::new(), deadline, None).await?;
        if !object.status().is_success() && object.status() != StatusCode::NOT_FOUND {
            return Err(ObjectProbeError::CleanupFailed);
        }
        let bucket = self.send(Method::DELETE, bucket_url, Bytes::new(), deadline, None).await?;
        if !bucket.status().is_success() {
            return Err(ObjectProbeError::CleanupFailed);
        }
        Ok(())
    }

    async fn send(
        &self,
        method: Method,
        url: Url,
        payload: Bytes,
        deadline: Instant,
        cancel: Option<&CancellationToken>,
    ) -> Result<Response, ObjectProbeError> {
        let payload_hash = hex_lower(&Sha256::digest(&payload));
        let unsigned = http::Request::builder()
            .method(method.clone())
            .uri(url.as_str())
            .header("x-amz-content-sha256", payload_hash)
            .body(())
            .map_err(|_| ObjectProbeError::ProtocolFailure)?;
        let signed_headers = rustfs_signer::try_sign_v4_headers(
            unsigned.into_parts().0,
            i64::try_from(payload.len()).map_err(|_| ObjectProbeError::ProtocolFailure)?,
            &self.access_key,
            &self.secret_key,
            &self.session_token,
            "us-east-1",
        )
        .map_err(|_| ObjectProbeError::ProtocolFailure)?;
        let send = self.client.request(method, url).headers(signed_headers).body(payload).send();
        let timed = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), send);
        let response = if let Some(cancel) = cancel {
            tokio::select! {
                () = cancel.cancelled() => return Err(ObjectProbeError::Cancelled),
                response = timed => response,
            }
        } else {
            timed.await
        };
        response
            .map_err(|_| ObjectProbeError::TimedOut)?
            .map_err(|error| self.transport_error(&error))
    }

    async fn require_success(
        &self,
        response: Response,
        deadline: Instant,
        cancel: Option<&CancellationToken>,
    ) -> Result<(), ObjectProbeError> {
        if !response.status().is_success() {
            return self.status_error(response.status());
        }
        self.read_response_body(response, MAX_OBJECT_RESPONSE_BYTES as u64, deadline, cancel)
            .await
            .map(|_| ())
    }

    async fn read_response_body(
        &self,
        response: Response,
        max_bytes: u64,
        deadline: Instant,
        cancel: Option<&CancellationToken>,
    ) -> Result<Vec<u8>, ObjectProbeError> {
        if response.content_length().is_some_and(|length| length > max_bytes) {
            return Err(ObjectProbeError::ProtocolFailure);
        }
        let max_bytes = usize::try_from(max_bytes).map_err(|_| ObjectProbeError::ProtocolFailure)?;
        let mut body = Vec::with_capacity(max_bytes.min(MAX_OBJECT_RESPONSE_BYTES));
        let mut stream = response.bytes_stream();
        loop {
            let next = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), stream.next());
            let chunk = if let Some(cancel) = cancel {
                tokio::select! {
                    () = cancel.cancelled() => return Err(ObjectProbeError::Cancelled),
                    chunk = next => chunk,
                }
            } else {
                next.await
            }
            .map_err(|_| ObjectProbeError::TimedOut)?;
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = chunk.map_err(|error| self.transport_error(&error))?;
            let length = body.len().checked_add(chunk.len()).ok_or(ObjectProbeError::ProtocolFailure)?;
            if length > max_bytes {
                return Err(ObjectProbeError::ProtocolFailure);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    fn object_url(&self, bucket: &str, object: Option<&str>) -> Result<Url, ObjectProbeError> {
        let path = object.map_or_else(|| format!("{bucket}/"), |object| format!("{bucket}/{object}"));
        self.endpoint.join(&path).map_err(|_| ObjectProbeError::ProtocolFailure)
    }

    fn status_error<T>(&self, status: StatusCode) -> Result<T, ObjectProbeError> {
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            Err(ObjectProbeError::PermissionDenied)
        } else {
            Err(ObjectProbeError::ProtocolFailure)
        }
    }

    fn transport_error(&self, error: &reqwest::Error) -> ObjectProbeError {
        if error.is_timeout() {
            ObjectProbeError::TimedOut
        } else if self.proxy_configured {
            ObjectProbeError::ProxyFailure
        } else if error.is_connect() {
            ObjectProbeError::EndpointUnavailable
        } else {
            ObjectProbeError::ProtocolFailure
        }
    }
}

impl ObjectProbe for S3ObjectProbe {
    fn probe<'a>(&'a self, request: &'a ObjectPerformanceRequest, cancel: &'a CancellationToken) -> ObjectProbeFuture<'a> {
        Box::pin(self.execute(request, cancel))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedObjectExport {
    pub artifact_uid: String,
    pub outcome: ObjectOutcome,
    pub reason_code: ObjectReasonCode,
    pub envelope_json: Vec<u8>,
    pub envelope_signature: Vec<u8>,
    pub result_json: Vec<u8>,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedObjectExport {
    pub artifact_uid: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

#[derive(Debug, Error)]
pub enum ObjectPerformanceError {
    #[error("object_performance_local_consent_required")]
    ConsentRequired,
    #[error("object_performance_local_consent_expired")]
    ConsentExpired,
    #[error("object_performance_request_expired")]
    Expired,
    #[error("object_performance_invalid_request")]
    InvalidRequest,
    #[error("object_performance_unsupported_version")]
    UnsupportedVersion,
    #[error("object_performance_unsupported_capability")]
    UnsupportedCapability,
    #[error("object_performance_limit_exceeded")]
    LimitExceeded,
    #[error("object_performance_collection_cancelled")]
    Cancelled,
    #[error("object_performance_busy")]
    Busy,
    #[error("object_performance_invalid_endpoint")]
    InvalidEndpoint,
    #[error("object_performance_invalid_proxy")]
    InvalidProxy,
    #[error("object_performance_invalid_root_certificate")]
    InvalidRootCertificate,
    #[error("object_performance_invalid_credential")]
    InvalidCredential,
    #[error("object_performance_transport_configuration")]
    TransportConfiguration,
    #[error("object_performance_signing_failed")]
    Signing,
    #[error("object_performance_encoding_failed")]
    Encoding,
    #[error("object_performance_output_exists")]
    AlreadyExists,
    #[error("object_performance_io_failed")]
    Io(#[source] std::io::Error),
    #[error("object_performance_output_durability_failed")]
    DurabilityAfterCommit(#[source] std::io::Error),
}

pub async fn measure_object(
    request: &ObjectPerformanceRequest,
    probe: &impl ObjectProbe,
    cancel: &CancellationToken,
) -> Result<ObjectMeasurement, ObjectPerformanceError> {
    request.validate(unix_now()?)?;
    if OBJECT_COLLECTOR_ACTIVE
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(ObjectPerformanceError::Busy);
    }
    let _guard = ActiveGuard;
    if cancel.is_cancelled() {
        return Ok(terminal_measurement(
            request,
            ObjectOutcome::Cancelled,
            ObjectReasonCode::Cancelled,
            ObjectTargetReasonCode::Cancelled,
            Duration::ZERO,
        ));
    }
    let started = Instant::now();
    // The concrete probe owns the task namespace and must observe cancellation
    // before returning so its async cleanup cannot be dropped halfway through.
    let result = probe.probe(request, cancel).await;
    Ok(match result {
        Ok(sample) if sample.transferred_bytes == request.traffic_bytes => success_measurement(request, sample),
        Ok(_) => failed_measurement(request, started.elapsed(), ObjectTargetReasonCode::ProtocolFailure),
        Err(ObjectProbeError::Cancelled) => terminal_measurement(
            request,
            ObjectOutcome::Cancelled,
            ObjectReasonCode::Cancelled,
            ObjectTargetReasonCode::Cancelled,
            started.elapsed(),
        ),
        Err(error) => failed_measurement(request, started.elapsed(), target_reason(error)),
    })
}

pub fn sign_object_export(
    request: &ObjectPerformanceRequest,
    measurement: &ObjectMeasurement,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedObjectExport, ObjectPerformanceError> {
    request.validate(unix_now()?)?;
    check_cancel(cancel)?;
    let result = &measurement.result;
    if result.outcome != ObjectOutcome::Succeeded
        || result.data.is_none()
        || result.run_uid != request.run_uid
        || result.schema_version != OBJECT_SCHEMA_VERSION
        || result.tool_id != OBJECT_TOOL_ID
        || result.capability != OBJECT_CAPABILITY
    {
        return Err(ObjectPerformanceError::InvalidRequest);
    }
    let result_json = serde_json::to_vec(result).map_err(|_| ObjectPerformanceError::Encoding)?;
    if result_json.is_empty() || result_json.len() > MAX_OBJECT_RESULT_BYTES {
        return Err(ObjectPerformanceError::LimitExceeded);
    }
    let device_key_id = hex_lower(&Sha256::digest(key.public_key_der()));
    let result_sha256 = hex_lower(&Sha256::digest(&result_json));
    let envelope = ObjectEnvelope {
        format_version: "rustfs.connect.diagnosticEnvelope/1",
        protocol_version: "v1",
        organization_name: &request.organization_name,
        cluster_name: &request.cluster_name,
        device_name: &request.device_name,
        run_uid: &request.run_uid,
        artifact_uid: &request.artifact_uid,
        tool_id: OBJECT_TOOL_ID,
        schema_version: OBJECT_SCHEMA_VERSION,
        classification: "L1",
        consent_uid: &request.consent.consent_uid,
        policy_revision: request.consent.policy_revision,
        produced_at: timestamp(request.produced_at_unix)?,
        expires_at: timestamp(request.expires_at_unix)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(request.nonce),
        device_key_id: &device_key_id,
        payload: ObjectPayload {
            path: RESULT_PATH,
            media_type: "application/json",
            size_bytes: u64::try_from(result_json.len()).map_err(|_| ObjectPerformanceError::LimitExceeded)?,
            sha256: &result_sha256,
        },
    };
    let envelope_json = serde_json::to_vec(&envelope).map_err(|_| ObjectPerformanceError::Encoding)?;
    if envelope_json.is_empty() || envelope_json.len() > MAX_ENVELOPE_BYTES {
        return Err(ObjectPerformanceError::LimitExceeded);
    }
    let envelope_signature = signature_document(key, &device_key_id, &envelope_json)?;
    let decompressed = result_json
        .len()
        .checked_add(envelope_json.len())
        .and_then(|size| size.checked_add(envelope_signature.len()))
        .ok_or(ObjectPerformanceError::LimitExceeded)?;
    if decompressed > MAX_DECOMPRESSED_BYTES {
        return Err(ObjectPerformanceError::LimitExceeded);
    }
    check_cancel(cancel)?;
    if unix_now()? >= request.expires_at_unix {
        return Err(ObjectPerformanceError::Expired);
    }
    let archive_bytes = archive(&envelope_json, &envelope_signature, &result_json)?;
    if archive_bytes.len() > MAX_ARCHIVE_BYTES {
        return Err(ObjectPerformanceError::LimitExceeded);
    }
    let archive_sha256 = hex_lower(&Sha256::digest(&archive_bytes));
    Ok(SignedObjectExport {
        artifact_uid: request.artifact_uid.clone(),
        outcome: result.outcome,
        reason_code: result.reason_code,
        envelope_json,
        envelope_signature,
        result_json,
        archive_bytes,
        archive_sha256,
    })
}

pub fn save_signed_object_export(
    output: &Path,
    export: &SignedObjectExport,
    cancel: &CancellationToken,
) -> Result<SavedObjectExport, ObjectPerformanceError> {
    check_cancel(cancel)?;
    if !uuid7(&export.artifact_uid) || export.archive_bytes.is_empty() {
        return Err(ObjectPerformanceError::InvalidRequest);
    }
    if export.archive_bytes.len() > MAX_ARCHIVE_BYTES {
        return Err(ObjectPerformanceError::LimitExceeded);
    }
    if hex_lower(&Sha256::digest(&export.archive_bytes)) != export.archive_sha256 {
        return Err(ObjectPerformanceError::InvalidRequest);
    }
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = output
        .file_name()
        .ok_or(ObjectPerformanceError::InvalidRequest)?
        .to_string_lossy();
    let temporary = parent.join(format!(".{filename}.{}.partial", export.artifact_uid));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(OUTPUT_MODE);
    }
    let mut file = options.open(&temporary).map_err(map_create_error)?;
    let saved = (|| {
        file.write_all(&export.archive_bytes).map_err(ObjectPerformanceError::Io)?;
        check_cancel(cancel)?;
        file.sync_all().map_err(ObjectPerformanceError::Io)?;
        check_cancel(cancel)?;
        fs::hard_link(&temporary, output).map_err(map_publish_error)?;
        fs::remove_file(&temporary).map_err(ObjectPerformanceError::DurabilityAfterCommit)?;
        #[cfg(unix)]
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(ObjectPerformanceError::DurabilityAfterCommit)?;
        Ok(SavedObjectExport {
            artifact_uid: export.artifact_uid.clone(),
            archive_size_bytes: u64::try_from(export.archive_bytes.len()).map_err(|_| ObjectPerformanceError::LimitExceeded)?,
            archive_sha256: export.archive_sha256.clone(),
        })
    })();
    if saved.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    saved
}

pub fn validate_object_limits(
    duration: Duration,
    _operation: ObjectOperation,
    traffic_bytes: u64,
) -> Result<(), ObjectPerformanceError> {
    if duration.is_zero()
        || duration.as_millis() == 0
        || duration > MAX_OBJECT_DURATION
        || traffic_bytes == 0
        || traffic_bytes > MAX_OBJECT_TRAFFIC_BYTES
    {
        return Err(ObjectPerformanceError::LimitExceeded);
    }
    let bandwidth_budget = u64::try_from(
        u128::from(MAX_OBJECT_BANDWIDTH_BYTES_PER_SECOND)
            .checked_mul(duration.as_millis())
            .ok_or(ObjectPerformanceError::LimitExceeded)?
            / 1_000,
    )
    .map_err(|_| ObjectPerformanceError::LimitExceeded)?;
    if traffic_bytes > bandwidth_budget.max(1) {
        return Err(ObjectPerformanceError::LimitExceeded);
    }
    Ok(())
}

pub fn read_protected_object_credential(path: &Path) -> Result<Zeroizing<String>, ObjectPerformanceError> {
    let metadata = fs::symlink_metadata(path).map_err(ObjectPerformanceError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ObjectPerformanceError::InvalidCredential);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
        if metadata.uid() != process_uid() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(ObjectPerformanceError::InvalidCredential);
        }
        let mut options = OpenOptions::new();
        options.read(true).custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let mut file = options.open(path).map_err(ObjectPerformanceError::Io)?;
        let opened = file.metadata().map_err(ObjectPerformanceError::Io)?;
        if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
            return Err(ObjectPerformanceError::InvalidCredential);
        }
        read_credential_value(&mut file)
    }
    #[cfg(not(unix))]
    {
        let mut file = File::open(path).map_err(ObjectPerformanceError::Io)?;
        read_credential_value(&mut file)
    }
}

fn read_credential_value(reader: &mut impl Read) -> Result<Zeroizing<String>, ObjectPerformanceError> {
    let mut bytes = Vec::with_capacity(256);
    reader
        .take(4_097)
        .read_to_end(&mut bytes)
        .map_err(ObjectPerformanceError::Io)?;
    if bytes.is_empty() || bytes.len() > 4_096 || bytes.contains(&0) {
        return Err(ObjectPerformanceError::InvalidCredential);
    }
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    let value = String::from_utf8(bytes).map_err(|_| ObjectPerformanceError::InvalidCredential)?;
    if value.is_empty() || value.len() > 4_096 || value.trim() != value {
        return Err(ObjectPerformanceError::InvalidCredential);
    }
    Ok(Zeroizing::new(value))
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn process_uid() -> u32 {
    // SAFETY: geteuid has no pointer arguments or caller preconditions.
    unsafe { libc::geteuid() }
}

impl ObjectPerformanceRequest {
    fn validate(&self, now_unix: i64) -> Result<(), ObjectPerformanceError> {
        if self.schema_version != OBJECT_SCHEMA_VERSION {
            return Err(ObjectPerformanceError::UnsupportedVersion);
        }
        if self.capability != OBJECT_CAPABILITY {
            return Err(ObjectPerformanceError::UnsupportedCapability);
        }
        if !self.consent.confirmed || self.consent.policy_revision == 0 {
            return Err(ObjectPerformanceError::ConsentRequired);
        }
        if self.consent.expires_at_unix <= now_unix || self.expires_at_unix > self.consent.expires_at_unix {
            return Err(ObjectPerformanceError::ConsentExpired);
        }
        let validity = self
            .expires_at_unix
            .checked_sub(self.produced_at_unix)
            .ok_or(ObjectPerformanceError::Expired)?;
        if self.produced_at_unix > now_unix.saturating_add(MAX_FUTURE_SKEW_SECONDS)
            || validity <= 0
            || validity > MAX_VALIDITY_SECONDS
            || self.expires_at_unix <= now_unix
        {
            return Err(ObjectPerformanceError::Expired);
        }
        validate_object_limits(self.duration, self.operation, self.traffic_bytes)?;
        if !uuid7(&self.run_uid)
            || !uuid7(&self.artifact_uid)
            || !uuid7(&self.consent.consent_uid)
            || !resource_names_match(self)
            || self.target_alias.is_empty()
            || self.target_alias.len() > 128
            || !self
                .target_alias
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
            || !lower_hex_string(&self.provenance.source_commit, 40)
            || !lower_hex_string(&self.provenance.executable_sha256, 64)
            || !version(&self.provenance.rustfs_version)
            || self.provenance.build_features.len() > MAX_BUILD_FEATURES
            || !self.provenance.build_features.iter().all(|value| build_feature(value))
        {
            return Err(ObjectPerformanceError::InvalidRequest);
        }
        Ok(())
    }
}

struct ActiveGuard;

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        OBJECT_COLLECTOR_ACTIVE.store(false, Ordering::Release);
    }
}

fn success_measurement(request: &ObjectPerformanceRequest, sample: ObjectProbeMeasurement) -> ObjectMeasurement {
    let duration_millis = elapsed_millis(sample.duration);
    let data = ObjectPerformanceData {
        operation: request.operation,
        transferred_bytes: sample.transferred_bytes,
        completed_operations: 1,
        duration_millis,
        error_count: 0,
    };
    ObjectMeasurement {
        result: result(
            request,
            ObjectOutcome::Succeeded,
            ObjectReasonCode::Complete,
            duration_millis,
            1,
            Some(data),
        ),
        target: ObjectTargetResult {
            target_alias: request.target_alias.clone(),
            outcome: ObjectOutcome::Succeeded,
            reason_code: ObjectTargetReasonCode::Complete,
            parameters: target_parameters(request),
            units: target_units(),
            transferred_bytes: sample.transferred_bytes,
            completed_operations: 1,
            latency_micros: Some(elapsed_micros(sample.latency)),
            duration_millis,
        },
    }
}

fn failed_measurement(
    request: &ObjectPerformanceRequest,
    elapsed: Duration,
    target_reason: ObjectTargetReasonCode,
) -> ObjectMeasurement {
    let reason = match target_reason {
        ObjectTargetReasonCode::EndpointUnavailable => ObjectReasonCode::SourceUnavailable,
        ObjectTargetReasonCode::PermissionDenied => ObjectReasonCode::PermissionDenied,
        _ => ObjectReasonCode::CollectionFailed,
    };
    terminal_measurement(request, ObjectOutcome::Failed, reason, target_reason, elapsed)
}

fn terminal_measurement(
    request: &ObjectPerformanceRequest,
    outcome: ObjectOutcome,
    reason_code: ObjectReasonCode,
    target_reason: ObjectTargetReasonCode,
    elapsed: Duration,
) -> ObjectMeasurement {
    let duration_millis = elapsed_millis_allow_zero(elapsed);
    ObjectMeasurement {
        result: result(request, outcome, reason_code, duration_millis, 0, None),
        target: ObjectTargetResult {
            target_alias: request.target_alias.clone(),
            outcome,
            reason_code: target_reason,
            parameters: target_parameters(request),
            units: target_units(),
            transferred_bytes: 0,
            completed_operations: 0,
            latency_micros: None,
            duration_millis,
        },
    }
}

fn target_reason(error: ObjectProbeError) -> ObjectTargetReasonCode {
    match error {
        ObjectProbeError::EndpointUnavailable => ObjectTargetReasonCode::EndpointUnavailable,
        ObjectProbeError::ProxyFailure => ObjectTargetReasonCode::ProxyFailure,
        ObjectProbeError::PermissionDenied => ObjectTargetReasonCode::PermissionDenied,
        ObjectProbeError::TimedOut => ObjectTargetReasonCode::TimedOut,
        ObjectProbeError::Cancelled => ObjectTargetReasonCode::Cancelled,
        ObjectProbeError::NamespaceConflict => ObjectTargetReasonCode::NamespaceConflict,
        ObjectProbeError::CleanupFailed => ObjectTargetReasonCode::CleanupFailed,
        ObjectProbeError::ProtocolFailure => ObjectTargetReasonCode::ProtocolFailure,
    }
}

fn target_parameters(request: &ObjectPerformanceRequest) -> ObjectTargetParameters {
    ObjectTargetParameters {
        operation: request.operation,
        requested_bytes: request.traffic_bytes,
        duration_millis: elapsed_millis_allow_zero(request.duration),
        concurrency: 1,
    }
}

fn target_units() -> ObjectTargetUnits {
    ObjectTargetUnits {
        bytes: "BYTE",
        duration: "MILLISECOND",
        latency: "MICROSECOND",
        operation_count: "OPERATION",
    }
}

fn result(
    request: &ObjectPerformanceRequest,
    outcome: ObjectOutcome,
    reason_code: ObjectReasonCode,
    duration_millis: u64,
    completed_units: u32,
    data: Option<ObjectPerformanceData>,
) -> ObjectDiagnosticResult {
    ObjectDiagnosticResult {
        schema_version: OBJECT_SCHEMA_VERSION,
        run_uid: request.run_uid.clone(),
        tool_id: OBJECT_TOOL_ID,
        capability: OBJECT_CAPABILITY,
        outcome,
        reason_code,
        duration_millis: duration_millis.min(30_000),
        provenance: request.provenance.clone(),
        coverage: ObjectCoverage {
            requested_units: 1,
            completed_units,
            unit: "WINDOW",
        },
        data,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectEnvelope<'a> {
    format_version: &'static str,
    protocol_version: &'static str,
    organization_name: &'a str,
    cluster_name: &'a str,
    device_name: &'a str,
    run_uid: &'a str,
    artifact_uid: &'a str,
    tool_id: &'static str,
    schema_version: u16,
    classification: &'static str,
    consent_uid: &'a str,
    policy_revision: u64,
    produced_at: String,
    expires_at: String,
    nonce: String,
    device_key_id: &'a str,
    payload: ObjectPayload<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectPayload<'a> {
    path: &'static str,
    media_type: &'static str,
    size_bytes: u64,
    sha256: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectSignature<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: String,
}

fn signature_document(key: &DeviceIdentity, key_id: &str, envelope: &[u8]) -> Result<Vec<u8>, ObjectPerformanceError> {
    let pkcs8 = key.to_pkcs8_der().map_err(|_| ObjectPerformanceError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| ObjectPerformanceError::Signing)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + envelope.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(envelope);
    let signature: Signature = signing_key.sign(&input);
    serde_json::to_vec(&ObjectSignature {
        algorithm: "ES256",
        key_id,
        value: URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes()),
    })
    .map_err(|_| ObjectPerformanceError::Encoding)
}

fn archive(envelope: &[u8], signature: &[u8], result: &[u8]) -> Result<Vec<u8>, ObjectPerformanceError> {
    let cursor = Cursor::new(Vec::with_capacity(envelope.len() + signature.len() + result.len() + 512));
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(OUTPUT_MODE);
    for (name, bytes) in [(ENVELOPE_PATH, envelope), (SIGNATURE_PATH, signature), (RESULT_PATH, result)] {
        writer
            .start_file(name, options)
            .map_err(|_| ObjectPerformanceError::Encoding)?;
        writer.write_all(bytes).map_err(ObjectPerformanceError::Io)?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|_| ObjectPerformanceError::Encoding)
}

fn deployment_endpoint(value: &str) -> Result<Url, ObjectPerformanceError> {
    let mut url = Url::parse(value).map_err(|_| ObjectPerformanceError::InvalidEndpoint)?;
    let local_http = url.scheme() == "http"
        && url
            .host_str()
            .is_some_and(|host| host == "localhost" || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback()));
    if (url.scheme() != "https" && !local_http)
        || url.cannot_be_a_base()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ObjectPerformanceError::InvalidEndpoint);
    }
    url.set_query(None);
    url.set_fragment(None);
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn proxy_url(value: &str) -> Result<Url, ObjectPerformanceError> {
    let url = Url::parse(value).map_err(|_| ObjectPerformanceError::InvalidProxy)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.cannot_be_a_base()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ObjectPerformanceError::InvalidProxy);
    }
    Ok(url)
}

fn resource_names_match(request: &ObjectPerformanceRequest) -> bool {
    let Some(organization_uid) = request.organization_name.strip_prefix("organizations/") else {
        return false;
    };
    if !uuid7(organization_uid) {
        return false;
    }
    let cluster_prefix = format!("{}/clusters/", request.organization_name);
    let Some(cluster_uid) = request.cluster_name.strip_prefix(&cluster_prefix) else {
        return false;
    };
    if !uuid7(cluster_uid) {
        return false;
    }
    let device_prefix = format!("{}/clusterDevices/", request.cluster_name);
    request.device_name.strip_prefix(&device_prefix).is_some_and(uuid7)
}

fn uuid7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}

fn lower_hex_string(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn version(value: &str) -> bool {
    if value.is_empty() || value.len() > 64 {
        return false;
    }
    let (core, prerelease) = value
        .split_once('-')
        .map_or((value, None), |(core, prerelease)| (core, Some(prerelease)));
    let mut parts = core.split('.');
    let valid_core = (0..3).all(|_| {
        parts
            .next()
            .is_some_and(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
    }) && parts.next().is_none();
    valid_core
        && prerelease.is_none_or(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
        })
}

fn build_feature(value: &str) -> bool {
    value.len() <= 64
        && value.as_bytes().split_first().is_some_and(|(first, rest)| {
            first.is_ascii_lowercase()
                && rest
                    .iter()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_'))
        })
}

fn elapsed_millis(duration: Duration) -> u64 {
    elapsed_millis_allow_zero(duration).max(1)
}

fn elapsed_millis_allow_zero(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).min(MAX_SAFE_INTEGER)
}

fn elapsed_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX).min(MAX_SAFE_INTEGER)
}

fn unix_now() -> Result<i64, ObjectPerformanceError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ObjectPerformanceError::InvalidRequest)
        .and_then(|duration| i64::try_from(duration.as_secs()).map_err(|_| ObjectPerformanceError::InvalidRequest))
}

fn timestamp(unix: i64) -> Result<String, ObjectPerformanceError> {
    OffsetDateTime::from_unix_timestamp(unix)
        .map_err(|_| ObjectPerformanceError::InvalidRequest)?
        .format(&Rfc3339)
        .map_err(|_| ObjectPerformanceError::Encoding)
}

fn check_cancel(cancel: &CancellationToken) -> Result<(), ObjectPerformanceError> {
    if cancel.is_cancelled() {
        Err(ObjectPerformanceError::Cancelled)
    } else {
        Ok(())
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn map_create_error(error: std::io::Error) -> ObjectPerformanceError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        ObjectPerformanceError::AlreadyExists
    } else {
        ObjectPerformanceError::Io(error)
    }
}

fn map_publish_error(error: std::io::Error) -> ObjectPerformanceError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        ObjectPerformanceError::AlreadyExists
    } else {
        ObjectPerformanceError::Io(error)
    }
}
