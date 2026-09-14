// Copyright 2026 RustFS Team
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

//! Consent-bound, destination-confirmed site-replication measurement.
//!
//! The producer uses a pre-provisioned versioned scratch bucket. It never
//! creates or removes buckets. A sample succeeds only after the destination
//! returns the exact source version and generated payload. Cleanup lists the
//! unique task key on both sites and deletes every version, including versions
//! that arrive after cancellation or timeout during a bounded grace window.

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
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Client, Method, Response, StatusCode, Url};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};
use zeroize::Zeroizing;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::connect::DeviceIdentity;

pub const SITE_REPLICATION_SCHEMA_VERSION: u16 = 1;
pub const SITE_REPLICATION_TOOL_ID: &str = "performance.siteReplication";
pub const SITE_REPLICATION_CAPABILITY: &str = "performance.siteReplication@1";
pub const MAX_SITE_REPLICATION_DURATION: Duration = Duration::from_secs(30);
pub const MAX_SITE_REPLICATION_TRAFFIC_BYTES: u64 = 1_048_576;
const MAX_LATE_ARRIVAL_CLEANUP: Duration = Duration::from_secs(5);
const MAX_RESPONSE_BYTES: u64 = 1_064_960;
const MAX_BUILD_FEATURES: usize = 64;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_FUTURE_SKEW_SECONDS: i64 = 300;
const MAX_VALIDITY_SECONDS: i64 = 2_592_000;
const MAX_RESULT_BYTES: usize = 262_144;
const MAX_ENVELOPE_BYTES: usize = 16_384;
const MAX_ARCHIVE_BYTES: usize = 524_288;
const MAX_DECOMPRESSED_BYTES: usize = 278_528;
const OUTPUT_MODE: u32 = 0o600;
const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";
const ENVELOPE_PATH: &str = "envelope.json";
const SIGNATURE_PATH: &str = "envelope.sig";
const RESULT_PATH: &str = "result.json";

static SITE_REPLICATION_COLLECTOR_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SiteReplicationOutcome {
    Succeeded,
    Failed,
    Cancelled,
    Unsupported,
}

impl SiteReplicationOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Unsupported => "UNSUPPORTED",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SiteReplicationReasonCode {
    Complete,
    CollectionFailed,
    PermissionDenied,
    Cancelled,
    UnsupportedTool,
}

impl SiteReplicationReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "COMPLETE",
            Self::CollectionFailed => "COLLECTION_FAILED",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::Cancelled => "CANCELLED",
            Self::UnsupportedTool => "UNSUPPORTED_TOOL",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SiteReplicationTargetReasonCode {
    Complete,
    SiteReplicationUnavailable,
    EndpointUnavailable,
    PermissionDenied,
    TimedOut,
    Cancelled,
    VersioningRequired,
    DestinationUnconfirmed,
    CleanupFailed,
    ProtocolFailure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SiteReplicationProvenance {
    repository: &'static str,
    source_commit: String,
    executable_sha256: String,
    rustfs_version: String,
    os_family: OsFamily,
    architecture: Architecture,
    build_features: Vec<String>,
}

impl SiteReplicationProvenance {
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
            os_family: OsFamily::current(),
            architecture: Architecture::current(),
            build_features,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum OsFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

impl OsFamily {
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
enum Architecture {
    #[serde(rename = "x86_64")]
    X86_64,
    Aarch64,
    Other,
}

impl Architecture {
    fn current() -> Self {
        match std::env::consts::ARCH {
            "x86_64" => Self::X86_64,
            "aarch64" => Self::Aarch64,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalSiteReplicationConsent {
    pub consent_uid: String,
    pub policy_revision: u64,
    pub expires_at_unix: i64,
    pub nonce: [u8; 32],
    pub confirmed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SiteReplicationPerformanceRequest {
    pub organization_name: String,
    pub cluster_name: String,
    pub destination_cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub capability: String,
    pub consent: LocalSiteReplicationConsent,
    pub produced_at_unix: i64,
    pub expires_at_unix: i64,
    pub duration: Duration,
    pub traffic_bytes: u64,
    pub source_alias: String,
    pub source_deployment_id: String,
    pub destination_alias: String,
    pub destination_deployment_id: String,
    pub scratch_bucket: String,
    pub late_arrival_cleanup: Duration,
    pub provenance: SiteReplicationProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SiteReplicationPerformanceData {
    pub replicated_bytes: u64,
    pub confirmed_objects: u64,
    pub duration_millis: u64,
    pub max_observed_lag_millis: u64,
    pub error_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct Coverage {
    requested_units: u32,
    completed_units: u32,
    unit: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SiteReplicationDiagnosticResult {
    schema_version: u16,
    run_uid: String,
    tool_id: &'static str,
    capability: &'static str,
    outcome: SiteReplicationOutcome,
    reason_code: SiteReplicationReasonCode,
    duration_millis: u64,
    provenance: SiteReplicationProvenance,
    coverage: Coverage,
    data: Option<SiteReplicationPerformanceData>,
}

impl SiteReplicationDiagnosticResult {
    pub fn outcome(&self) -> SiteReplicationOutcome {
        self.outcome
    }

    pub fn reason_code(&self) -> SiteReplicationReasonCode {
        self.reason_code
    }

    pub fn data(&self) -> Option<&SiteReplicationPerformanceData> {
        self.data.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SiteReplicationTargetResult {
    pub source_alias: String,
    pub destination_alias: String,
    pub outcome: SiteReplicationOutcome,
    pub reason_code: SiteReplicationTargetReasonCode,
    pub requested_bytes: u64,
    pub duration_millis: u64,
    pub concurrency: u8,
    pub bytes_unit: &'static str,
    pub duration_unit: &'static str,
    pub operation_count_unit: &'static str,
    pub replicated_bytes: u64,
    pub confirmed_objects: u64,
    pub max_observed_lag_millis: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SiteReplicationMeasurement {
    pub result: SiteReplicationDiagnosticResult,
    pub target: SiteReplicationTargetResult,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SiteReplicationProbeMeasurement {
    pub replicated_bytes: u64,
    pub confirmed_objects: u64,
    pub duration: Duration,
    pub max_observed_lag: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SiteReplicationProbeError {
    SiteReplicationUnavailable,
    EndpointUnavailable,
    PermissionDenied,
    TimedOut,
    Cancelled,
    VersioningRequired,
    DestinationUnconfirmed,
    CleanupFailed,
    ProtocolFailure,
}

pub type SiteReplicationProbeFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SiteReplicationProbeMeasurement, SiteReplicationProbeError>> + Send + 'a>>;

pub trait SiteReplicationProbe: Send + Sync {
    fn probe<'a>(
        &'a self,
        request: &'a SiteReplicationPerformanceRequest,
        cancel: &'a CancellationToken,
    ) -> SiteReplicationProbeFuture<'a>;
}

/// Static credential set for one site-replication endpoint connection.
pub struct SiteReplicationCredentials {
    pub access_key: Zeroizing<String>,
    pub secret_key: Zeroizing<String>,
    pub session_token: Zeroizing<String>,
}

pub struct SiteReplicationEndpoint {
    pub alias: String,
    pub deployment_id: String,
    endpoint: Url,
    client: Client,
    access_key: Zeroizing<String>,
    secret_key: Zeroizing<String>,
    session_token: Zeroizing<String>,
}

impl SiteReplicationEndpoint {
    pub fn new(
        alias: impl Into<String>,
        deployment_id: impl Into<String>,
        endpoint: &str,
        root_ca_pem: Option<&[u8]>,
        credentials: SiteReplicationCredentials,
        timeout: Duration,
    ) -> Result<Self, SiteReplicationPerformanceError> {
        let endpoint = deployment_endpoint(endpoint)?;
        if credentials.access_key.is_empty() || credentials.secret_key.is_empty() {
            return Err(SiteReplicationPerformanceError::InvalidCredential);
        }
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout);
        if let Some(root_ca_pem) = root_ca_pem {
            let certificate = reqwest::Certificate::from_pem(root_ca_pem)
                .map_err(|_| SiteReplicationPerformanceError::InvalidRootCertificate)?;
            builder = builder.add_root_certificate(certificate);
        }
        let client = builder
            .build()
            .map_err(|_| SiteReplicationPerformanceError::TransportConfiguration)?;
        Ok(Self {
            alias: alias.into(),
            deployment_id: deployment_id.into(),
            endpoint,
            client,
            access_key: credentials.access_key,
            secret_key: credentials.secret_key,
            session_token: credentials.session_token,
        })
    }
}

pub struct S3SiteReplicationProbe {
    source: SiteReplicationEndpoint,
    destination: SiteReplicationEndpoint,
}

impl S3SiteReplicationProbe {
    pub fn new(source: SiteReplicationEndpoint, destination: SiteReplicationEndpoint) -> Self {
        Self { source, destination }
    }

    async fn execute(
        &self,
        request: &SiteReplicationPerformanceRequest,
        cancel: &CancellationToken,
    ) -> Result<SiteReplicationProbeMeasurement, SiteReplicationProbeError> {
        self.validate_endpoint_bindings(request)?;
        let started = Instant::now();
        let cleanup_window = request.late_arrival_cleanup.min(MAX_LATE_ARRIVAL_CLEANUP);
        let operation_deadline = started + request.duration.saturating_sub(cleanup_window);
        self.confirm_topology(request, operation_deadline, cancel).await?;
        let key = format!("rustfs-connect/site-replication/{}", request.artifact_uid);
        let payload_len = usize::try_from(request.traffic_bytes).map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
        let payload = Bytes::from(vec![0xa5; payload_len]);

        let measured = self.measure(request, &key, payload, operation_deadline, cancel).await;
        let cleanup_deadline = (started + request.duration).min(Instant::now() + cleanup_window);
        let cleanup = self.cleanup_late_arrivals(request, &key, cleanup_deadline).await;
        if cleanup.is_err() {
            return Err(SiteReplicationProbeError::CleanupFailed);
        }
        measured
    }

    fn validate_endpoint_bindings(&self, request: &SiteReplicationPerformanceRequest) -> Result<(), SiteReplicationProbeError> {
        if self.source.alias != request.source_alias
            || self.source.deployment_id != request.source_deployment_id
            || self.destination.alias != request.destination_alias
            || self.destination.deployment_id != request.destination_deployment_id
        {
            return Err(SiteReplicationProbeError::ProtocolFailure);
        }
        Ok(())
    }

    async fn measure(
        &self,
        request: &SiteReplicationPerformanceRequest,
        key: &str,
        payload: Bytes,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<SiteReplicationProbeMeasurement, SiteReplicationProbeError> {
        let source_url = object_url(&self.source.endpoint, &request.scratch_bucket, key, None)?;
        let replication_started = Instant::now();
        // Do not cancel an in-flight PUT: a cancelled request may still commit at
        // the server. Waiting for its bounded response preserves the version ID;
        // the fallback version listing in cleanup also covers a lost response.
        let response = self
            .source
            .send(Method::PUT, source_url, payload.clone(), deadline, None)
            .await?;
        if !response.status().is_success() {
            return status_error(response.status());
        }
        let version_id = response
            .headers()
            .get("x-amz-version-id")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && *value != "null")
            .ok_or(SiteReplicationProbeError::VersioningRequired)?
            .to_owned();
        drain_response(response, 16_384, deadline, None).await?;

        loop {
            if cancel.is_cancelled() {
                return Err(SiteReplicationProbeError::Cancelled);
            }
            if Instant::now() >= deadline {
                return Err(SiteReplicationProbeError::DestinationUnconfirmed);
            }
            let destination_url = object_url(&self.destination.endpoint, &request.scratch_bucket, key, Some(&version_id))?;
            let response = self
                .destination
                .send(Method::GET, destination_url, Bytes::new(), deadline, Some(cancel))
                .await?;
            if response.status() == StatusCode::NOT_FOUND {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            if !response.status().is_success() {
                return status_error(response.status());
            }
            let returned_version = response
                .headers()
                .get("x-amz-version-id")
                .and_then(|value| value.to_str().ok());
            if returned_version != Some(version_id.as_str()) {
                return Err(SiteReplicationProbeError::DestinationUnconfirmed);
            }
            let body = drain_response(response, request.traffic_bytes, deadline, Some(cancel)).await?;
            if body.as_slice() != payload.as_ref() {
                return Err(SiteReplicationProbeError::DestinationUnconfirmed);
            }
            let elapsed = replication_started.elapsed();
            return Ok(SiteReplicationProbeMeasurement {
                replicated_bytes: request.traffic_bytes,
                confirmed_objects: 1,
                duration: elapsed,
                max_observed_lag: elapsed,
            });
        }
    }

    async fn confirm_topology(
        &self,
        request: &SiteReplicationPerformanceRequest,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<(), SiteReplicationProbeError> {
        let url = self
            .source
            .endpoint
            .join("rustfs/admin/v3/site-replication/info")
            .map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
        let response = self
            .source
            .send(Method::GET, url, Bytes::new(), deadline, Some(cancel))
            .await?;
        if !response.status().is_success() {
            return status_error(response.status());
        }
        let body = drain_response(response, 262_144, deadline, Some(cancel)).await?;
        let info: SiteReplicationInfo = serde_json::from_slice(&body).map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
        let expected = [&request.source_deployment_id, &request.destination_deployment_id];
        if !info.enabled
            || !expected
                .iter()
                .all(|id| info.sites.iter().any(|site| &site.deployment_id == *id))
        {
            return Err(SiteReplicationProbeError::SiteReplicationUnavailable);
        }
        Ok(())
    }

    async fn cleanup_late_arrivals(
        &self,
        request: &SiteReplicationPerformanceRequest,
        key: &str,
        deadline: Instant,
    ) -> Result<(), SiteReplicationProbeError> {
        let mut clean = false;
        loop {
            if Instant::now() >= deadline {
                return clean.then_some(()).ok_or(SiteReplicationProbeError::CleanupFailed);
            }
            self.delete_key_versions(&self.source, &request.scratch_bucket, key, deadline)
                .await?;
            self.delete_key_versions(&self.destination, &request.scratch_bucket, key, deadline)
                .await?;
            clean = true;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn delete_key_versions(
        &self,
        endpoint: &SiteReplicationEndpoint,
        bucket: &str,
        key: &str,
        deadline: Instant,
    ) -> Result<usize, SiteReplicationProbeError> {
        let list_url = list_versions_url(&endpoint.endpoint, bucket, key)?;
        let response = endpoint.send(Method::GET, list_url, Bytes::new(), deadline, None).await?;
        if !response.status().is_success() {
            return status_error(response.status());
        }
        let body = drain_response(response, MAX_RESPONSE_BYTES, deadline, None).await?;
        let listed: ListVersionsResult =
            quick_xml::de::from_reader(body.as_slice()).map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
        if listed.is_truncated {
            return Err(SiteReplicationProbeError::CleanupFailed);
        }
        let versions = listed
            .versions
            .into_iter()
            .chain(listed.delete_markers)
            .filter(|version| version.key == key)
            .collect::<Vec<_>>();
        for version in &versions {
            let url = object_url(&endpoint.endpoint, bucket, key, Some(&version.version_id))?;
            let response = endpoint.send(Method::DELETE, url, Bytes::new(), deadline, None).await?;
            if !response.status().is_success() && response.status() != StatusCode::NOT_FOUND {
                return Err(SiteReplicationProbeError::CleanupFailed);
            }
            drain_response(response, 16_384, deadline, None).await?;
        }
        Ok(versions.len())
    }
}

impl SiteReplicationEndpoint {
    async fn send(
        &self,
        method: Method,
        url: Url,
        payload: Bytes,
        deadline: Instant,
        cancel: Option<&CancellationToken>,
    ) -> Result<Response, SiteReplicationProbeError> {
        let payload_hash = hex_lower(&Sha256::digest(&payload));
        let unsigned = http::Request::builder()
            .method(method.clone())
            .uri(url.as_str())
            .header("x-amz-content-sha256", payload_hash)
            .body(())
            .map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
        let headers = rustfs_signer::try_sign_v4_headers(
            unsigned.into_parts().0,
            i64::try_from(payload.len()).map_err(|_| SiteReplicationProbeError::ProtocolFailure)?,
            &self.access_key,
            &self.secret_key,
            &self.session_token,
            "us-east-1",
        )
        .map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
        let sent = self.client.request(method, url).headers(headers).body(payload).send();
        let timed = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), sent);
        let response = if let Some(cancel) = cancel {
            tokio::select! {
                () = cancel.cancelled() => return Err(SiteReplicationProbeError::Cancelled),
                response = timed => response,
            }
        } else {
            timed.await
        };
        response.map_err(|_| SiteReplicationProbeError::TimedOut)?.map_err(|error| {
            if error.is_timeout() {
                SiteReplicationProbeError::TimedOut
            } else if error.is_connect() {
                SiteReplicationProbeError::EndpointUnavailable
            } else {
                SiteReplicationProbeError::ProtocolFailure
            }
        })
    }
}

impl SiteReplicationProbe for S3SiteReplicationProbe {
    fn probe<'a>(
        &'a self,
        request: &'a SiteReplicationPerformanceRequest,
        cancel: &'a CancellationToken,
    ) -> SiteReplicationProbeFuture<'a> {
        Box::pin(self.execute(request, cancel))
    }
}

#[derive(Debug, Error)]
pub enum SiteReplicationPerformanceError {
    #[error("site_replication_performance_local_consent_required")]
    ConsentRequired,
    #[error("site_replication_performance_local_consent_expired")]
    ConsentExpired,
    #[error("site_replication_performance_request_expired")]
    Expired,
    #[error("site_replication_performance_invalid_request")]
    InvalidRequest,
    #[error("site_replication_performance_unsupported_version")]
    UnsupportedVersion,
    #[error("site_replication_performance_unsupported_capability")]
    UnsupportedCapability,
    #[error("site_replication_performance_limit_exceeded")]
    LimitExceeded,
    #[error("site_replication_performance_collection_cancelled")]
    Cancelled,
    #[error("site_replication_performance_busy")]
    Busy,
    #[error("site_replication_performance_invalid_endpoint")]
    InvalidEndpoint,
    #[error("site_replication_performance_invalid_root_certificate")]
    InvalidRootCertificate,
    #[error("site_replication_performance_invalid_credential")]
    InvalidCredential,
    #[error("site_replication_performance_transport_configuration")]
    TransportConfiguration,
    #[error("site_replication_performance_signing_failed")]
    Signing,
    #[error("site_replication_performance_encoding_failed")]
    Encoding,
    #[error("site_replication_performance_output_exists")]
    AlreadyExists,
    #[error("site_replication_performance_io_failed")]
    Io(#[source] std::io::Error),
    #[error("site_replication_performance_output_durability_failed")]
    DurabilityAfterCommit(#[source] std::io::Error),
}

pub async fn measure_site_replication(
    request: &SiteReplicationPerformanceRequest,
    probe: &impl SiteReplicationProbe,
    cancel: &CancellationToken,
) -> Result<SiteReplicationMeasurement, SiteReplicationPerformanceError> {
    request.validate(unix_now()?)?;
    if SITE_REPLICATION_COLLECTOR_ACTIVE
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(SiteReplicationPerformanceError::Busy);
    }
    let _guard = ActiveGuard;
    if cancel.is_cancelled() {
        return Ok(terminal_measurement(
            request,
            SiteReplicationOutcome::Cancelled,
            SiteReplicationReasonCode::Cancelled,
            SiteReplicationTargetReasonCode::Cancelled,
            Duration::ZERO,
        ));
    }
    let started = Instant::now();
    Ok(match probe.probe(request, cancel).await {
        Ok(sample)
            if sample.replicated_bytes == request.traffic_bytes
                && sample.confirmed_objects == 1
                && !sample.duration.is_zero() =>
        {
            success_measurement(request, sample)
        }
        Ok(_) => terminal_measurement(
            request,
            SiteReplicationOutcome::Failed,
            SiteReplicationReasonCode::CollectionFailed,
            SiteReplicationTargetReasonCode::DestinationUnconfirmed,
            started.elapsed(),
        ),
        Err(SiteReplicationProbeError::Cancelled) => terminal_measurement(
            request,
            SiteReplicationOutcome::Cancelled,
            SiteReplicationReasonCode::Cancelled,
            SiteReplicationTargetReasonCode::Cancelled,
            started.elapsed(),
        ),
        Err(error) => failed_measurement(request, started.elapsed(), error),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedSiteReplicationExport {
    pub artifact_uid: String,
    pub outcome: SiteReplicationOutcome,
    pub reason_code: SiteReplicationReasonCode,
    pub envelope_json: Vec<u8>,
    pub envelope_signature: Vec<u8>,
    pub result_json: Vec<u8>,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedSiteReplicationExport {
    pub artifact_uid: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

pub fn sign_site_replication_export(
    request: &SiteReplicationPerformanceRequest,
    measurement: &SiteReplicationMeasurement,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedSiteReplicationExport, SiteReplicationPerformanceError> {
    request.validate(unix_now()?)?;
    check_cancel(cancel)?;
    let result = &measurement.result;
    let data_matches_outcome = (result.outcome == SiteReplicationOutcome::Succeeded) == result.data.is_some();
    if !data_matches_outcome
        || result.run_uid != request.run_uid
        || result.schema_version != SITE_REPLICATION_SCHEMA_VERSION
        || result.tool_id != SITE_REPLICATION_TOOL_ID
        || result.capability != SITE_REPLICATION_CAPABILITY
        || measurement.target.source_alias != request.source_alias
        || measurement.target.destination_alias != request.destination_alias
    {
        return Err(SiteReplicationPerformanceError::InvalidRequest);
    }
    let result_json = serde_json::to_vec(result).map_err(|_| SiteReplicationPerformanceError::Encoding)?;
    if result_json.is_empty() || result_json.len() > MAX_RESULT_BYTES {
        return Err(SiteReplicationPerformanceError::LimitExceeded);
    }
    let device_key_id = hex_lower(&Sha256::digest(key.public_key_der()));
    let result_sha256 = hex_lower(&Sha256::digest(&result_json));
    let envelope = Envelope {
        format_version: "rustfs.connect.diagnosticEnvelope/1",
        protocol_version: "v1",
        organization_name: &request.organization_name,
        cluster_name: &request.cluster_name,
        device_name: &request.device_name,
        run_uid: &request.run_uid,
        artifact_uid: &request.artifact_uid,
        tool_id: SITE_REPLICATION_TOOL_ID,
        schema_version: SITE_REPLICATION_SCHEMA_VERSION,
        classification: "L2",
        consent_uid: &request.consent.consent_uid,
        policy_revision: request.consent.policy_revision,
        produced_at: timestamp(request.produced_at_unix)?,
        expires_at: timestamp(request.expires_at_unix)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(request.consent.nonce),
        device_key_id: &device_key_id,
        targets: Targets {
            source_deployment: &request.cluster_name,
            destination_deployment: &request.destination_cluster_name,
        },
        payload: Payload {
            path: RESULT_PATH,
            media_type: "application/json",
            size_bytes: u64::try_from(result_json.len()).map_err(|_| SiteReplicationPerformanceError::LimitExceeded)?,
            sha256: &result_sha256,
        },
    };
    let envelope_json = serde_json::to_vec(&envelope).map_err(|_| SiteReplicationPerformanceError::Encoding)?;
    if envelope_json.is_empty() || envelope_json.len() > MAX_ENVELOPE_BYTES {
        return Err(SiteReplicationPerformanceError::LimitExceeded);
    }
    let envelope_signature = signature_document(key, &device_key_id, &envelope_json)?;
    let decompressed = result_json
        .len()
        .checked_add(envelope_json.len())
        .and_then(|size| size.checked_add(envelope_signature.len()))
        .ok_or(SiteReplicationPerformanceError::LimitExceeded)?;
    if decompressed > MAX_DECOMPRESSED_BYTES {
        return Err(SiteReplicationPerformanceError::LimitExceeded);
    }
    check_cancel(cancel)?;
    if unix_now()? >= request.expires_at_unix {
        return Err(SiteReplicationPerformanceError::Expired);
    }
    let archive_bytes = archive(&envelope_json, &envelope_signature, &result_json)?;
    if archive_bytes.len() > MAX_ARCHIVE_BYTES {
        return Err(SiteReplicationPerformanceError::LimitExceeded);
    }
    let archive_sha256 = hex_lower(&Sha256::digest(&archive_bytes));
    Ok(SignedSiteReplicationExport {
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

pub fn save_signed_site_replication_export(
    output: &Path,
    export: &SignedSiteReplicationExport,
    cancel: &CancellationToken,
) -> Result<SavedSiteReplicationExport, SiteReplicationPerformanceError> {
    check_cancel(cancel)?;
    if !uuid7(&export.artifact_uid)
        || export.archive_bytes.is_empty()
        || export.archive_bytes.len() > MAX_ARCHIVE_BYTES
        || hex_lower(&Sha256::digest(&export.archive_bytes)) != export.archive_sha256
    {
        return Err(SiteReplicationPerformanceError::InvalidRequest);
    }
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = output
        .file_name()
        .ok_or(SiteReplicationPerformanceError::InvalidRequest)?
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
        file.write_all(&export.archive_bytes)
            .map_err(SiteReplicationPerformanceError::Io)?;
        check_cancel(cancel)?;
        file.sync_all().map_err(SiteReplicationPerformanceError::Io)?;
        check_cancel(cancel)?;
        fs::hard_link(&temporary, output).map_err(map_publish_error)?;
        fs::remove_file(&temporary).map_err(SiteReplicationPerformanceError::DurabilityAfterCommit)?;
        #[cfg(unix)]
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(SiteReplicationPerformanceError::DurabilityAfterCommit)?;
        Ok(SavedSiteReplicationExport {
            artifact_uid: export.artifact_uid.clone(),
            archive_size_bytes: u64::try_from(export.archive_bytes.len())
                .map_err(|_| SiteReplicationPerformanceError::LimitExceeded)?,
            archive_sha256: export.archive_sha256.clone(),
        })
    })();
    if saved.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    saved
}

pub fn validate_site_replication_limits(
    duration: Duration,
    traffic_bytes: u64,
    late_arrival_cleanup: Duration,
) -> Result<(), SiteReplicationPerformanceError> {
    if duration.is_zero()
        || duration.as_millis() == 0
        || duration > MAX_SITE_REPLICATION_DURATION
        || traffic_bytes == 0
        || traffic_bytes > MAX_SITE_REPLICATION_TRAFFIC_BYTES
        || late_arrival_cleanup.is_zero()
        || late_arrival_cleanup > MAX_LATE_ARRIVAL_CLEANUP
        || late_arrival_cleanup >= duration
    {
        return Err(SiteReplicationPerformanceError::LimitExceeded);
    }
    Ok(())
}

pub fn read_protected_site_replication_credential(path: &Path) -> Result<Zeroizing<String>, SiteReplicationPerformanceError> {
    let metadata = fs::symlink_metadata(path).map_err(SiteReplicationPerformanceError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(SiteReplicationPerformanceError::InvalidCredential);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
        if metadata.uid() != process_uid() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(SiteReplicationPerformanceError::InvalidCredential);
        }
        let mut options = OpenOptions::new();
        options.read(true).custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let mut file = options.open(path).map_err(SiteReplicationPerformanceError::Io)?;
        let opened = file.metadata().map_err(SiteReplicationPerformanceError::Io)?;
        if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
            return Err(SiteReplicationPerformanceError::InvalidCredential);
        }
        read_credential(&mut file)
    }
    #[cfg(not(unix))]
    {
        read_credential(&mut File::open(path).map_err(SiteReplicationPerformanceError::Io)?)
    }
}

fn read_credential(reader: &mut impl Read) -> Result<Zeroizing<String>, SiteReplicationPerformanceError> {
    let mut bytes = Vec::with_capacity(256);
    reader
        .take(4_097)
        .read_to_end(&mut bytes)
        .map_err(SiteReplicationPerformanceError::Io)?;
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    let value = String::from_utf8(bytes).map_err(|_| SiteReplicationPerformanceError::InvalidCredential)?;
    if value.is_empty() || value.len() > 4_096 || value.trim() != value || value.contains('\0') {
        return Err(SiteReplicationPerformanceError::InvalidCredential);
    }
    Ok(Zeroizing::new(value))
}

impl SiteReplicationPerformanceRequest {
    fn validate(&self, now: i64) -> Result<(), SiteReplicationPerformanceError> {
        if self.schema_version != SITE_REPLICATION_SCHEMA_VERSION {
            return Err(SiteReplicationPerformanceError::UnsupportedVersion);
        }
        if self.capability != SITE_REPLICATION_CAPABILITY {
            return Err(SiteReplicationPerformanceError::UnsupportedCapability);
        }
        if !self.consent.confirmed || self.consent.policy_revision == 0 {
            return Err(SiteReplicationPerformanceError::ConsentRequired);
        }
        if self.consent.expires_at_unix <= now || self.expires_at_unix > self.consent.expires_at_unix {
            return Err(SiteReplicationPerformanceError::ConsentExpired);
        }
        let validity = self
            .expires_at_unix
            .checked_sub(self.produced_at_unix)
            .ok_or(SiteReplicationPerformanceError::Expired)?;
        if self.produced_at_unix > now.saturating_add(MAX_FUTURE_SKEW_SECONDS)
            || validity <= 0
            || validity > MAX_VALIDITY_SECONDS
            || self.expires_at_unix <= now
        {
            return Err(SiteReplicationPerformanceError::Expired);
        }
        validate_site_replication_limits(self.duration, self.traffic_bytes, self.late_arrival_cleanup)?;
        if !uuid7(&self.run_uid)
            || !uuid7(&self.artifact_uid)
            || !uuid7(&self.consent.consent_uid)
            || !resource_names_match(self)
            || !opaque_alias(&self.source_alias)
            || !opaque_alias(&self.destination_alias)
            || self.source_alias == self.destination_alias
            || !deployment_id(&self.source_deployment_id)
            || !deployment_id(&self.destination_deployment_id)
            || self.source_deployment_id == self.destination_deployment_id
            || !scratch_bucket(&self.scratch_bucket)
            || !lower_hex(&self.provenance.source_commit, 40)
            || !lower_hex(&self.provenance.executable_sha256, 64)
            || self.provenance.build_features.len() > MAX_BUILD_FEATURES
            || !self.provenance.build_features.iter().all(|value| build_feature(value))
            || !rustfs_version(&self.provenance.rustfs_version)
        {
            return Err(SiteReplicationPerformanceError::InvalidRequest);
        }
        Ok(())
    }
}

struct ActiveGuard;

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        SITE_REPLICATION_COLLECTOR_ACTIVE.store(false, Ordering::Release);
    }
}

fn success_measurement(
    request: &SiteReplicationPerformanceRequest,
    sample: SiteReplicationProbeMeasurement,
) -> SiteReplicationMeasurement {
    let duration_millis = millis(sample.duration);
    let max_lag = millis(sample.max_observed_lag);
    SiteReplicationMeasurement {
        result: result(
            request,
            SiteReplicationOutcome::Succeeded,
            SiteReplicationReasonCode::Complete,
            duration_millis,
            1,
            Some(SiteReplicationPerformanceData {
                replicated_bytes: sample.replicated_bytes,
                confirmed_objects: sample.confirmed_objects,
                duration_millis,
                max_observed_lag_millis: max_lag,
                error_count: 0,
            }),
        ),
        target: target_result(
            request,
            SiteReplicationOutcome::Succeeded,
            SiteReplicationTargetReasonCode::Complete,
            sample.replicated_bytes,
            sample.confirmed_objects,
            duration_millis,
            Some(max_lag),
        ),
    }
}

fn failed_measurement(
    request: &SiteReplicationPerformanceRequest,
    elapsed: Duration,
    error: SiteReplicationProbeError,
) -> SiteReplicationMeasurement {
    let reason = if error == SiteReplicationProbeError::PermissionDenied {
        SiteReplicationReasonCode::PermissionDenied
    } else {
        SiteReplicationReasonCode::CollectionFailed
    };
    terminal_measurement(request, SiteReplicationOutcome::Failed, reason, target_reason(error), elapsed)
}

fn terminal_measurement(
    request: &SiteReplicationPerformanceRequest,
    outcome: SiteReplicationOutcome,
    reason: SiteReplicationReasonCode,
    target_reason: SiteReplicationTargetReasonCode,
    elapsed: Duration,
) -> SiteReplicationMeasurement {
    let duration_millis = millis_allow_zero(elapsed);
    SiteReplicationMeasurement {
        result: result(request, outcome, reason, duration_millis, 0, None),
        target: target_result(request, outcome, target_reason, 0, 0, duration_millis, None),
    }
}

fn result(
    request: &SiteReplicationPerformanceRequest,
    outcome: SiteReplicationOutcome,
    reason_code: SiteReplicationReasonCode,
    duration_millis: u64,
    completed_units: u32,
    data: Option<SiteReplicationPerformanceData>,
) -> SiteReplicationDiagnosticResult {
    SiteReplicationDiagnosticResult {
        schema_version: SITE_REPLICATION_SCHEMA_VERSION,
        run_uid: request.run_uid.clone(),
        tool_id: SITE_REPLICATION_TOOL_ID,
        capability: SITE_REPLICATION_CAPABILITY,
        outcome,
        reason_code,
        duration_millis: duration_millis.min(30_000),
        provenance: request.provenance.clone(),
        coverage: Coverage {
            requested_units: 1,
            completed_units,
            unit: "WINDOW",
        },
        data,
    }
}

fn target_result(
    request: &SiteReplicationPerformanceRequest,
    outcome: SiteReplicationOutcome,
    reason_code: SiteReplicationTargetReasonCode,
    replicated_bytes: u64,
    confirmed_objects: u64,
    duration_millis: u64,
    max_observed_lag_millis: Option<u64>,
) -> SiteReplicationTargetResult {
    SiteReplicationTargetResult {
        source_alias: request.source_alias.clone(),
        destination_alias: request.destination_alias.clone(),
        outcome,
        reason_code,
        requested_bytes: request.traffic_bytes,
        duration_millis,
        concurrency: 1,
        bytes_unit: "BYTE",
        duration_unit: "MILLISECOND",
        operation_count_unit: "OPERATION",
        replicated_bytes,
        confirmed_objects,
        max_observed_lag_millis,
    }
}

fn target_reason(error: SiteReplicationProbeError) -> SiteReplicationTargetReasonCode {
    match error {
        SiteReplicationProbeError::SiteReplicationUnavailable => SiteReplicationTargetReasonCode::SiteReplicationUnavailable,
        SiteReplicationProbeError::EndpointUnavailable => SiteReplicationTargetReasonCode::EndpointUnavailable,
        SiteReplicationProbeError::PermissionDenied => SiteReplicationTargetReasonCode::PermissionDenied,
        SiteReplicationProbeError::TimedOut => SiteReplicationTargetReasonCode::TimedOut,
        SiteReplicationProbeError::Cancelled => SiteReplicationTargetReasonCode::Cancelled,
        SiteReplicationProbeError::VersioningRequired => SiteReplicationTargetReasonCode::VersioningRequired,
        SiteReplicationProbeError::DestinationUnconfirmed => SiteReplicationTargetReasonCode::DestinationUnconfirmed,
        SiteReplicationProbeError::CleanupFailed => SiteReplicationTargetReasonCode::CleanupFailed,
        SiteReplicationProbeError::ProtocolFailure => SiteReplicationTargetReasonCode::ProtocolFailure,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<'a> {
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
    targets: Targets<'a>,
    payload: Payload<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Targets<'a> {
    source_deployment: &'a str,
    destination_deployment: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Payload<'a> {
    path: &'static str,
    media_type: &'static str,
    size_bytes: u64,
    sha256: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvelopeSignature<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: String,
}

fn signature_document(key: &DeviceIdentity, key_id: &str, envelope: &[u8]) -> Result<Vec<u8>, SiteReplicationPerformanceError> {
    let pkcs8 = key.to_pkcs8_der().map_err(|_| SiteReplicationPerformanceError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| SiteReplicationPerformanceError::Signing)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + envelope.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(envelope);
    let signature: Signature = signing_key.sign(&input);
    serde_json::to_vec(&EnvelopeSignature {
        algorithm: "ES256",
        key_id,
        value: URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes()),
    })
    .map_err(|_| SiteReplicationPerformanceError::Encoding)
}

fn archive(envelope: &[u8], signature: &[u8], result: &[u8]) -> Result<Vec<u8>, SiteReplicationPerformanceError> {
    let cursor = Cursor::new(Vec::with_capacity(envelope.len() + signature.len() + result.len() + 512));
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(OUTPUT_MODE);
    for (name, bytes) in [(ENVELOPE_PATH, envelope), (SIGNATURE_PATH, signature), (RESULT_PATH, result)] {
        writer
            .start_file(name, options)
            .map_err(|_| SiteReplicationPerformanceError::Encoding)?;
        writer.write_all(bytes).map_err(SiteReplicationPerformanceError::Io)?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|_| SiteReplicationPerformanceError::Encoding)
}

fn timestamp(value: i64) -> Result<String, SiteReplicationPerformanceError> {
    OffsetDateTime::from_unix_timestamp(value)
        .map_err(|_| SiteReplicationPerformanceError::InvalidRequest)?
        .format(&Rfc3339)
        .map_err(|_| SiteReplicationPerformanceError::Encoding)
}

fn check_cancel(cancel: &CancellationToken) -> Result<(), SiteReplicationPerformanceError> {
    if cancel.is_cancelled() {
        Err(SiteReplicationPerformanceError::Cancelled)
    } else {
        Ok(())
    }
}

fn map_create_error(error: std::io::Error) -> SiteReplicationPerformanceError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        SiteReplicationPerformanceError::AlreadyExists
    } else {
        SiteReplicationPerformanceError::Io(error)
    }
}

fn map_publish_error(error: std::io::Error) -> SiteReplicationPerformanceError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        SiteReplicationPerformanceError::AlreadyExists
    } else {
        SiteReplicationPerformanceError::Io(error)
    }
}

#[derive(Deserialize)]
struct SiteReplicationInfo {
    enabled: bool,
    #[serde(default)]
    sites: Vec<SiteInfo>,
}

#[derive(Deserialize)]
struct SiteInfo {
    #[serde(rename = "deploymentID", alias = "deploymentId")]
    deployment_id: String,
}

#[derive(Deserialize)]
struct ListVersionsResult {
    #[serde(rename = "IsTruncated", default)]
    is_truncated: bool,
    #[serde(rename = "Version", default)]
    versions: Vec<ListedVersion>,
    #[serde(rename = "DeleteMarker", default)]
    delete_markers: Vec<ListedVersion>,
}

#[derive(Deserialize)]
struct ListedVersion {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: String,
}

async fn drain_response(
    response: Response,
    max_bytes: u64,
    deadline: Instant,
    cancel: Option<&CancellationToken>,
) -> Result<Vec<u8>, SiteReplicationProbeError> {
    if response.content_length().is_some_and(|length| length > max_bytes) {
        return Err(SiteReplicationProbeError::ProtocolFailure);
    }
    let max_bytes = usize::try_from(max_bytes).map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
    let mut body = Vec::with_capacity(max_bytes.min(16_384));
    let mut stream = response.bytes_stream();
    loop {
        let next = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), stream.next());
        let chunk = if let Some(cancel) = cancel {
            tokio::select! {
                () = cancel.cancelled() => return Err(SiteReplicationProbeError::Cancelled),
                chunk = next => chunk,
            }
        } else {
            next.await
        }
        .map_err(|_| SiteReplicationProbeError::TimedOut)?;
        let Some(chunk) = chunk else { break };
        let chunk = chunk.map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(SiteReplicationProbeError::ProtocolFailure);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn object_url(endpoint: &Url, bucket: &str, key: &str, version_id: Option<&str>) -> Result<Url, SiteReplicationProbeError> {
    let mut url = endpoint
        .join(&format!("{bucket}/{}", encode_path(key)))
        .map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
    if let Some(version_id) = version_id {
        url.query_pairs_mut().append_pair("versionId", version_id);
    }
    Ok(url)
}

fn list_versions_url(endpoint: &Url, bucket: &str, key: &str) -> Result<Url, SiteReplicationProbeError> {
    let mut url = endpoint
        .join(&format!("{bucket}/"))
        .map_err(|_| SiteReplicationProbeError::ProtocolFailure)?;
    url.query_pairs_mut().append_pair("versions", "").append_pair("prefix", key);
    Ok(url)
}

fn encode_path(value: &str) -> String {
    value
        .split('/')
        .map(|segment| utf8_percent_encode(segment, NON_ALPHANUMERIC).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn deployment_endpoint(value: &str) -> Result<Url, SiteReplicationPerformanceError> {
    let mut url = Url::parse(value).map_err(|_| SiteReplicationPerformanceError::InvalidEndpoint)?;
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
        return Err(SiteReplicationPerformanceError::InvalidEndpoint);
    }
    if !url.path().ends_with('/') {
        url.set_path("/");
    }
    Ok(url)
}

fn status_error<T>(status: StatusCode) -> Result<T, SiteReplicationProbeError> {
    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        Err(SiteReplicationProbeError::PermissionDenied)
    } else {
        Err(SiteReplicationProbeError::ProtocolFailure)
    }
}

fn resource_names_match(request: &SiteReplicationPerformanceRequest) -> bool {
    let Some(organization_uid) = request.organization_name.strip_prefix("organizations/") else {
        return false;
    };
    let cluster_prefix = format!("{}/clusters/", request.organization_name);
    let device_prefix = format!("{}/clusterDevices/", request.cluster_name);
    uuid7(organization_uid)
        && request.cluster_name.strip_prefix(&cluster_prefix).is_some_and(uuid7)
        && request
            .destination_cluster_name
            .strip_prefix(&cluster_prefix)
            .is_some_and(uuid7)
        && request.destination_cluster_name != request.cluster_name
        && request.device_name.strip_prefix(&device_prefix).is_some_and(uuid7)
}

fn uuid7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}

fn opaque_alias(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn deployment_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

fn scratch_bucket(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-'))
        && value.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && value.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
        && !value.contains("..")
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn build_feature(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_'))
}

fn rustfs_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+'))
}

fn millis(value: Duration) -> u64 {
    millis_allow_zero(value).max(1)
}

fn millis_allow_zero(value: Duration) -> u64 {
    u64::try_from(value.as_millis())
        .unwrap_or(MAX_SAFE_INTEGER)
        .min(MAX_SAFE_INTEGER)
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn unix_now() -> Result<i64, SiteReplicationPerformanceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SiteReplicationPerformanceError::InvalidRequest)?;
    i64::try_from(duration.as_secs()).map_err(|_| SiteReplicationPerformanceError::InvalidRequest)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn process_uid() -> u32 {
    // SAFETY: geteuid has no pointer arguments or caller preconditions.
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn now() -> i64 {
        i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs())
            .expect("current time fits i64")
    }

    fn request() -> SiteReplicationPerformanceRequest {
        let now = now();
        let organization = "organizations/019e3ae0-0000-7000-8000-000000000010";
        let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000011");
        SiteReplicationPerformanceRequest {
            organization_name: organization.to_owned(),
            cluster_name: cluster.clone(),
            destination_cluster_name: format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000016"),
            device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000012"),
            run_uid: "019e3ae0-0000-7000-8000-000000000013".to_owned(),
            artifact_uid: "019e3ae0-0000-7000-8000-000000000014".to_owned(),
            schema_version: 1,
            capability: SITE_REPLICATION_CAPABILITY.to_owned(),
            consent: LocalSiteReplicationConsent {
                consent_uid: "019e3ae0-0000-7000-8000-000000000015".to_owned(),
                policy_revision: 7,
                expires_at_unix: now + 120,
                confirmed: true,
                nonce: [0x6b; 32],
            },
            produced_at_unix: now,
            expires_at_unix: now + 60,
            duration: Duration::from_secs(2),
            traffic_bytes: 65_536,
            source_alias: "site-a".to_owned(),
            source_deployment_id: "deployment-a".to_owned(),
            destination_alias: "site-b".to_owned(),
            destination_deployment_id: "deployment-b".to_owned(),
            scratch_bucket: "connect-replication-scratch".to_owned(),
            late_arrival_cleanup: Duration::from_millis(500),
            provenance: SiteReplicationProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0-rc.6", Vec::new()),
        }
    }

    struct CountingProbe {
        calls: AtomicUsize,
        result: Result<SiteReplicationProbeMeasurement, SiteReplicationProbeError>,
    }

    impl SiteReplicationProbe for CountingProbe {
        fn probe<'a>(
            &'a self,
            _request: &'a SiteReplicationPerformanceRequest,
            _cancel: &'a CancellationToken,
        ) -> SiteReplicationProbeFuture<'a> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move { self.result })
        }
    }

    #[tokio::test]
    async fn destination_confirmation_produces_frozen_aggregate_shape() {
        let request = request();
        let probe = CountingProbe {
            calls: AtomicUsize::new(0),
            result: Ok(SiteReplicationProbeMeasurement {
                replicated_bytes: 65_536,
                confirmed_objects: 1,
                duration: Duration::from_secs(1),
                max_observed_lag: Duration::from_millis(250),
            }),
        };
        let measured = measure_site_replication(&request, &probe, &CancellationToken::new())
            .await
            .expect("site replication measurement");
        let value = serde_json::to_value(&measured.result).expect("result JSON");
        assert_eq!(value["toolId"], SITE_REPLICATION_TOOL_ID);
        assert_eq!(value["capability"], SITE_REPLICATION_CAPABILITY);
        assert_eq!(value["outcome"], "SUCCEEDED");
        assert_eq!(value["data"]["replicatedBytes"], 65_536);
        assert_eq!(value["data"]["confirmedObjects"], 1);
        for forbidden in [
            "sourceEndpoint",
            "destinationEndpoint",
            "credentialFile",
            "caFile",
            "scratchBucket",
            "cleanupVersionId",
            "lateArrivalCleanup",
        ] {
            assert!(value["data"].get(forbidden).is_none(), "result leaked {forbidden}");
        }
        let export = sign_site_replication_export(&request, &measured, &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("signed site replication export");
        let envelope = String::from_utf8(export.envelope_json.clone()).expect("envelope UTF-8");
        let result = String::from_utf8(export.result_json).expect("result UTF-8");
        let envelope_value: serde_json::Value = serde_json::from_str(&envelope).expect("envelope JSON");
        assert_eq!(envelope_value["targets"]["sourceDeployment"], request.cluster_name);
        assert_eq!(envelope_value["targets"]["destinationDeployment"], request.destination_cluster_name);
        for secret in [
            "source.example",
            "destination.example",
            "connect-replication-scratch",
            "deployment-a",
            "deployment-b",
        ] {
            assert!(!envelope.contains(secret), "envelope leaked {secret}");
            assert!(!result.contains(secret), "result leaked {secret}");
        }
    }

    #[tokio::test]
    async fn invalid_consent_and_budgets_fail_before_probe() {
        let probe = CountingProbe {
            calls: AtomicUsize::new(0),
            result: Err(SiteReplicationProbeError::ProtocolFailure),
        };
        let mut invalid = request();
        invalid.consent.confirmed = false;
        assert!(matches!(
            measure_site_replication(&invalid, &probe, &CancellationToken::new()).await,
            Err(SiteReplicationPerformanceError::ConsentRequired)
        ));
        invalid = request();
        invalid.traffic_bytes = MAX_SITE_REPLICATION_TRAFFIC_BYTES + 1;
        assert!(matches!(
            measure_site_replication(&invalid, &probe, &CancellationToken::new()).await,
            Err(SiteReplicationPerformanceError::LimitExceeded)
        ));
        invalid = request();
        invalid.source_deployment_id = invalid.destination_deployment_id.clone();
        assert!(matches!(
            measure_site_replication(&invalid, &probe, &CancellationToken::new()).await,
            Err(SiteReplicationPerformanceError::InvalidRequest)
        ));
        invalid = request();
        invalid.destination_cluster_name = invalid.cluster_name.clone();
        assert!(matches!(
            measure_site_replication(&invalid, &probe, &CancellationToken::new()).await,
            Err(SiteReplicationPerformanceError::InvalidRequest)
        ));
        invalid = request();
        invalid.destination_cluster_name = format!("{}/clusters/not-a-uuid", invalid.organization_name);
        assert!(matches!(
            measure_site_replication(&invalid, &probe, &CancellationToken::new()).await,
            Err(SiteReplicationPerformanceError::InvalidRequest)
        ));
        invalid = request();
        invalid.destination_cluster_name =
            "organizations/019e3ae0-0000-7000-8000-000000000099/clusters/019e3ae0-0000-7000-8000-000000000016".to_owned();
        assert!(matches!(
            measure_site_replication(&invalid, &probe, &CancellationToken::new()).await,
            Err(SiteReplicationPerformanceError::InvalidRequest)
        ));
        assert_eq!(probe.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn unconfirmed_and_cancelled_results_never_report_success() {
        for (error, expected) in [
            (SiteReplicationProbeError::DestinationUnconfirmed, SiteReplicationOutcome::Failed),
            (SiteReplicationProbeError::SiteReplicationUnavailable, SiteReplicationOutcome::Failed),
            (SiteReplicationProbeError::Cancelled, SiteReplicationOutcome::Cancelled),
            (SiteReplicationProbeError::CleanupFailed, SiteReplicationOutcome::Failed),
        ] {
            let probe = CountingProbe {
                calls: AtomicUsize::new(0),
                result: Err(error),
            };
            let measured = measure_site_replication(&request(), &probe, &CancellationToken::new())
                .await
                .expect("terminal measurement");
            assert_eq!(measured.result.outcome(), expected);
            assert!(measured.result.data().is_none());
        }
    }

    #[test]
    fn cleanup_queries_are_version_specific_and_task_scoped() {
        let endpoint = Url::parse("https://source.example/").expect("endpoint");
        let object = object_url(&endpoint, "scratch-bucket", "path/a b", Some("version+1")).expect("object URL");
        assert_eq!(object.as_str(), "https://source.example/scratch-bucket/path/a%20b?versionId=version%2B1");
        let list = list_versions_url(&endpoint, "scratch-bucket", "path/a b").expect("list URL");
        assert!(list.query_pairs().any(|(key, value)| key == "versions" && value.is_empty()));
        assert!(list.query_pairs().any(|(key, value)| key == "prefix" && value == "path/a b"));
    }

    #[test]
    fn version_listing_parses_versions_and_delete_markers() {
        let xml = br#"<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>task-key</Key><VersionId>v1</VersionId></Version><DeleteMarker><Key>task-key</Key><VersionId>m1</VersionId></DeleteMarker></ListVersionsResult>"#;
        let listed: ListVersionsResult = quick_xml::de::from_reader(xml.as_slice()).expect("version listing");
        assert_eq!(listed.versions[0].version_id, "v1");
        assert_eq!(listed.delete_markers[0].version_id, "m1");
    }
}
