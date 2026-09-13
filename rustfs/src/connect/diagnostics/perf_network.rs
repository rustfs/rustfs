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

//! Consent-bound inter-node network performance results.

use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Cursor, Write as _};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
use p256::pkcs8::DecodePrivateKey as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::connect::DeviceIdentity;
use crate::storage_api::cluster::network_probe::{NetworkPeerProbeClient, NetworkPeerProbeError as NativePeerProbeError};

pub const NETWORK_SCHEMA_VERSION: u16 = 1;
pub const NETWORK_TOOL_ID: &str = "performance.network";
pub const NETWORK_CAPABILITY: &str = "performance.network@1";
pub const MAX_NETWORK_DURATION: Duration = Duration::from_secs(30);
pub const MAX_TRAFFIC_BYTES: u64 = 1_048_576;
pub const MAX_BANDWIDTH_BYTES_PER_SECOND: u64 = 1_048_576;
pub const MAX_PEERS: usize = 256;
pub const MAX_OPERATIONS: usize = 1_024;
pub const MAX_RESULT_BYTES: usize = 262_144;
pub const MAX_ENVELOPE_BYTES: usize = 16_384;
pub const MAX_ARCHIVE_BYTES: usize = 524_288;
pub const MAX_DECOMPRESSED_BYTES: usize = 278_528;

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_BUILD_FEATURES: usize = 64;
const MAX_VALIDITY_SECONDS: i64 = 2_592_000;
const MAX_FUTURE_SKEW_SECONDS: i64 = 300;
const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";
const ENVELOPE_PATH: &str = "envelope.json";
const SIGNATURE_PATH: &str = "envelope.sig";
const RESULT_PATH: &str = "result.json";
const OUTPUT_MODE: u32 = 0o600;

static NETWORK_COLLECTOR_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NetworkOutcome {
    Succeeded,
    Partial,
    Failed,
    Unsupported,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NetworkReasonCode {
    Complete,
    LimitExceeded,
    SourceUnavailable,
    PermissionDenied,
    UnsupportedTool,
    UnsupportedVersion,
    UnsupportedPlatform,
    Cancelled,
    InvalidInput,
    CollectionFailed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PeerReasonCode {
    Complete,
    Unreachable,
    TimedOut,
    ProtocolFailure,
    LimitExceeded,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkProvenance {
    repository: &'static str,
    source_commit: String,
    executable_sha256: String,
    rustfs_version: String,
    os_family: NetworkOsFamily,
    architecture: NetworkArchitecture,
    build_features: Vec<String>,
}

impl NetworkProvenance {
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
            os_family: NetworkOsFamily::current(),
            architecture: NetworkArchitecture::current(),
            build_features,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum NetworkOsFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

impl NetworkOsFamily {
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
enum NetworkArchitecture {
    #[serde(rename = "x86_64")]
    X86_64,
    Aarch64,
    Other,
}

impl NetworkArchitecture {
    fn current() -> Self {
        match std::env::consts::ARCH {
            "x86_64" => Self::X86_64,
            "aarch64" => Self::Aarch64,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalNetworkConsent {
    pub consent_uid: String,
    pub policy_revision: u64,
    pub expires_at_unix: i64,
    pub confirmed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkPerformanceRequest {
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub capability: String,
    pub consent: LocalNetworkConsent,
    pub produced_at_unix: i64,
    pub expires_at_unix: i64,
    pub nonce: [u8; 32],
    pub duration: Duration,
    pub peer_aliases: Vec<String>,
    pub traffic_bytes_per_peer: u64,
    pub provenance: NetworkProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPerformanceData {
    pub transferred_bytes: u64,
    pub duration_millis: u64,
    pub error_count: u64,
    pub peer_count: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkCoverage {
    requested_units: u32,
    completed_units: u32,
    unit: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkDiagnosticResult {
    schema_version: u16,
    run_uid: String,
    tool_id: &'static str,
    capability: &'static str,
    outcome: NetworkOutcome,
    reason_code: NetworkReasonCode,
    duration_millis: u64,
    provenance: NetworkProvenance,
    coverage: NetworkCoverage,
    data: Option<NetworkPerformanceData>,
}

impl NetworkDiagnosticResult {
    pub fn outcome(&self) -> NetworkOutcome {
        self.outcome
    }

    pub fn reason_code(&self) -> NetworkReasonCode {
        self.reason_code
    }

    pub fn data(&self) -> Option<&NetworkPerformanceData> {
        self.data.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPeerResult {
    pub peer_alias: String,
    pub outcome: NetworkOutcome,
    pub reason_code: PeerReasonCode,
    pub transferred_bytes: u64,
    pub duration_millis: u64,
    pub latency_micros: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkMeasurement {
    pub result: NetworkDiagnosticResult,
    pub peers: Vec<NetworkPeerResult>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerProbeMeasurement {
    pub transferred_bytes: u64,
    pub duration: Duration,
    pub latency: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerProbeError {
    Unreachable,
    TimedOut,
    ProtocolFailure,
    Cancelled,
}

pub type PeerProbeFuture<'a> = Pin<Box<dyn Future<Output = Result<PeerProbeMeasurement, PeerProbeError>> + Send + 'a>>;

pub trait NetworkPeerHarness: Send + Sync {
    fn probe<'a>(&'a self, peer_alias: &'a str, traffic_bytes: u64, cancel: &'a CancellationToken) -> PeerProbeFuture<'a>;
}

struct RuntimeNetworkPeerHarness {
    client: NetworkPeerProbeClient,
}

impl NetworkPeerHarness for RuntimeNetworkPeerHarness {
    fn probe<'a>(&'a self, peer_alias: &'a str, traffic_bytes: u64, cancel: &'a CancellationToken) -> PeerProbeFuture<'a> {
        Box::pin(async move {
            self.client
                .probe(peer_alias, traffic_bytes, MAX_NETWORK_DURATION, cancel)
                .await
                .map(|measurement| PeerProbeMeasurement {
                    transferred_bytes: measurement.transferred_bytes,
                    duration: measurement.duration,
                    latency: measurement.latency,
                })
                .map_err(map_native_probe_error)
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedNetworkExport {
    pub artifact_uid: String,
    pub outcome: NetworkOutcome,
    pub reason_code: NetworkReasonCode,
    pub envelope_json: Vec<u8>,
    pub envelope_signature: Vec<u8>,
    pub result_json: Vec<u8>,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedNetworkExport {
    pub artifact_uid: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

#[derive(Debug, Error)]
pub enum NetworkPerformanceError {
    #[error("network_performance_local_consent_required")]
    ConsentRequired,
    #[error("network_performance_local_consent_expired")]
    ConsentExpired,
    #[error("network_performance_request_expired")]
    Expired,
    #[error("network_performance_invalid_request")]
    InvalidRequest,
    #[error("network_performance_unsupported_version")]
    UnsupportedVersion,
    #[error("network_performance_unsupported_capability")]
    UnsupportedCapability,
    #[error("network_performance_limit_exceeded")]
    LimitExceeded,
    #[error("network_performance_collection_cancelled")]
    Cancelled,
    #[error("network_performance_collection_already_running")]
    Busy,
    #[error("network_performance_export_signing_failed")]
    Signing,
    #[error("network_performance_export_exists")]
    AlreadyExists,
    #[error("network_performance_export_io_failed")]
    Io(#[source] std::io::Error),
    #[error("network_performance_export_encoding_failed")]
    Encoding,
    #[error("network_performance_export_durability_failed_after_commit")]
    DurabilityAfterCommit(#[source] std::io::Error),
}

/// Return the current native capability state without generating test traffic.
pub async fn measure_network(
    request: &NetworkPerformanceRequest,
    cancel: &CancellationToken,
) -> Result<NetworkMeasurement, NetworkPerformanceError> {
    request.validate(unix_now()?)?;
    if cancel.is_cancelled() {
        return Ok(terminal_measurement(request, NetworkOutcome::Cancelled, NetworkReasonCode::Cancelled));
    }

    let Some(endpoint_pools) = crate::runtime_sources::current_endpoints_handle() else {
        return Ok(terminal_measurement(
            request,
            NetworkOutcome::Unsupported,
            NetworkReasonCode::SourceUnavailable,
        ));
    };
    let harness = RuntimeNetworkPeerHarness {
        client: NetworkPeerProbeClient::from_endpoint_pools(&endpoint_pools),
    };
    let aliases = harness
        .client
        .targets()
        .into_iter()
        .map(|target| target.alias)
        .collect::<Vec<_>>();
    if aliases.is_empty() {
        return Ok(terminal_measurement(
            request,
            NetworkOutcome::Unsupported,
            NetworkReasonCode::SourceUnavailable,
        ));
    }
    if aliases != request.peer_aliases {
        return Err(NetworkPerformanceError::InvalidRequest);
    }
    measure_network_with_harness(request, &harness, cancel).await
}

pub async fn measure_network_with_harness(
    request: &NetworkPerformanceRequest,
    harness: &dyn NetworkPeerHarness,
    cancel: &CancellationToken,
) -> Result<NetworkMeasurement, NetworkPerformanceError> {
    request.validate(unix_now()?)?;
    if cancel.is_cancelled() {
        return Ok(terminal_measurement(request, NetworkOutcome::Cancelled, NetworkReasonCode::Cancelled));
    }
    let _lease = CollectorLease::acquire()?;
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + request.duration;
    let mut peers = Vec::with_capacity(request.peer_aliases.len());
    let mut transferred_bytes = 0_u64;
    let mut completed_units = 0_u32;

    for alias in &request.peer_aliases {
        let peer_started = Instant::now();
        let outcome = tokio::select! {
            () = cancel.cancelled() => {
                return Ok(cancelled_measurement(request, started.elapsed(), completed_units, peers));
            }
            () = tokio::time::sleep_until(deadline) => Err(PeerProbeError::TimedOut),
            result = harness.probe(alias, request.traffic_bytes_per_peer, cancel) => result,
        };
        completed_units = completed_units.saturating_add(1);
        match outcome {
            Ok(value) => {
                let bounded = value.transferred_bytes <= MAX_TRAFFIC_BYTES
                    && value.duration > Duration::ZERO
                    && value.duration <= request.duration
                    && value.latency <= value.duration
                    && value.duration.as_millis() <= u128::from(u64::MAX)
                    && value.latency.as_micros() <= u128::from(MAX_SAFE_INTEGER);
                if bounded && value.transferred_bytes == request.traffic_bytes_per_peer {
                    transferred_bytes = transferred_bytes
                        .checked_add(value.transferred_bytes)
                        .ok_or(NetworkPerformanceError::LimitExceeded)?;
                    peers.push(NetworkPeerResult {
                        peer_alias: alias.clone(),
                        outcome: NetworkOutcome::Succeeded,
                        reason_code: PeerReasonCode::Complete,
                        transferred_bytes: value.transferred_bytes,
                        duration_millis: elapsed_millis(value.duration),
                        latency_micros: Some(
                            u64::try_from(value.latency.as_micros()).map_err(|_| NetworkPerformanceError::LimitExceeded)?,
                        ),
                    });
                } else if !bounded || value.transferred_bytes > request.traffic_bytes_per_peer {
                    peers.push(failed_peer(
                        alias,
                        PeerReasonCode::LimitExceeded,
                        elapsed_millis_allow_zero(peer_started.elapsed()),
                    ));
                } else {
                    peers.push(failed_peer(
                        alias,
                        PeerReasonCode::ProtocolFailure,
                        elapsed_millis_allow_zero(peer_started.elapsed()),
                    ));
                }
            }
            Err(PeerProbeError::Cancelled) => {
                return Ok(cancelled_measurement(request, started.elapsed(), completed_units, peers));
            }
            Err(error) => peers.push(failed_peer(alias, peer_reason(error), elapsed_millis_allow_zero(peer_started.elapsed()))),
        }
    }

    let elapsed = started.elapsed().min(request.duration);
    let error_count = peers.iter().filter(|peer| peer.outcome != NetworkOutcome::Succeeded).count() as u64;
    let outcome = match (error_count, peers.len()) {
        (0, _) => NetworkOutcome::Succeeded,
        (errors, count) if errors == count as u64 => NetworkOutcome::Failed,
        _ => NetworkOutcome::Partial,
    };
    let reason_code = if error_count == 0 {
        NetworkReasonCode::Complete
    } else {
        NetworkReasonCode::CollectionFailed
    };
    let data = if outcome == NetworkOutcome::Failed {
        None
    } else {
        Some(NetworkPerformanceData {
            transferred_bytes,
            duration_millis: elapsed_millis(elapsed),
            error_count,
            peer_count: u16::try_from(peers.len()).map_err(|_| NetworkPerformanceError::LimitExceeded)?,
        })
    };
    let result = result(request, outcome, reason_code, elapsed_millis_allow_zero(elapsed), completed_units, data);
    Ok(NetworkMeasurement { result, peers })
}

pub fn sign_network_export(
    request: &NetworkPerformanceRequest,
    measurement: &NetworkMeasurement,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedNetworkExport, NetworkPerformanceError> {
    request.validate(unix_now()?)?;
    check_cancel(cancel)?;
    let result = &measurement.result;
    if !matches!(result.outcome, NetworkOutcome::Succeeded | NetworkOutcome::Partial)
        || result.data.is_none()
        || result.run_uid != request.run_uid
        || result.schema_version != NETWORK_SCHEMA_VERSION
        || result.tool_id != NETWORK_TOOL_ID
        || result.capability != NETWORK_CAPABILITY
    {
        return Err(NetworkPerformanceError::InvalidRequest);
    }
    let result_json = serde_json::to_vec(result).map_err(|_| NetworkPerformanceError::Encoding)?;
    if result_json.is_empty() || result_json.len() > MAX_RESULT_BYTES {
        return Err(NetworkPerformanceError::LimitExceeded);
    }
    let device_key_id = hex_lower(&Sha256::digest(key.public_key_der()));
    let result_sha256 = hex_lower(&Sha256::digest(&result_json));
    let envelope = NetworkEnvelope {
        format_version: "rustfs.connect.diagnosticEnvelope/1",
        protocol_version: "v1",
        organization_name: &request.organization_name,
        cluster_name: &request.cluster_name,
        device_name: &request.device_name,
        run_uid: &request.run_uid,
        artifact_uid: &request.artifact_uid,
        tool_id: NETWORK_TOOL_ID,
        schema_version: NETWORK_SCHEMA_VERSION,
        classification: "L1",
        consent_uid: &request.consent.consent_uid,
        policy_revision: request.consent.policy_revision,
        produced_at: timestamp(request.produced_at_unix)?,
        expires_at: timestamp(request.expires_at_unix)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(request.nonce),
        device_key_id: &device_key_id,
        payload: NetworkPayload {
            path: RESULT_PATH,
            media_type: "application/json",
            size_bytes: result_json.len() as u64,
            sha256: &result_sha256,
        },
    };
    let envelope_json = serde_json::to_vec(&envelope).map_err(|_| NetworkPerformanceError::Encoding)?;
    if envelope_json.is_empty() || envelope_json.len() > MAX_ENVELOPE_BYTES {
        return Err(NetworkPerformanceError::LimitExceeded);
    }
    let envelope_signature = signature_document(key, &device_key_id, &envelope_json)?;
    let decompressed = result_json
        .len()
        .checked_add(envelope_json.len())
        .and_then(|size| size.checked_add(envelope_signature.len()))
        .ok_or(NetworkPerformanceError::LimitExceeded)?;
    if decompressed > MAX_DECOMPRESSED_BYTES {
        return Err(NetworkPerformanceError::LimitExceeded);
    }
    check_cancel(cancel)?;
    if unix_now()? >= request.expires_at_unix {
        return Err(NetworkPerformanceError::Expired);
    }
    let archive_bytes = archive(&envelope_json, &envelope_signature, &result_json)?;
    if archive_bytes.len() > MAX_ARCHIVE_BYTES {
        return Err(NetworkPerformanceError::LimitExceeded);
    }
    let archive_sha256 = hex_lower(&Sha256::digest(&archive_bytes));
    Ok(SignedNetworkExport {
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

pub fn save_signed_network_export(
    output: &Path,
    export: &SignedNetworkExport,
    cancel: &CancellationToken,
) -> Result<SavedNetworkExport, NetworkPerformanceError> {
    check_cancel(cancel)?;
    if !uuid7(&export.artifact_uid) {
        return Err(NetworkPerformanceError::InvalidRequest);
    }
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = output
        .file_name()
        .ok_or(NetworkPerformanceError::InvalidRequest)?
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
        file.write_all(&export.archive_bytes).map_err(NetworkPerformanceError::Io)?;
        check_cancel(cancel)?;
        file.sync_all().map_err(NetworkPerformanceError::Io)?;
        check_cancel(cancel)?;
        fs::hard_link(&temporary, output).map_err(map_publish_error)?;
        fs::remove_file(&temporary).map_err(NetworkPerformanceError::DurabilityAfterCommit)?;
        #[cfg(unix)]
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(NetworkPerformanceError::DurabilityAfterCommit)?;
        Ok(SavedNetworkExport {
            artifact_uid: export.artifact_uid.clone(),
            archive_size_bytes: export.archive_bytes.len() as u64,
            archive_sha256: export.archive_sha256.clone(),
        })
    })();
    if saved.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    saved
}

impl NetworkPerformanceRequest {
    fn validate(&self, now_unix: i64) -> Result<(), NetworkPerformanceError> {
        if self.schema_version != NETWORK_SCHEMA_VERSION {
            return Err(NetworkPerformanceError::UnsupportedVersion);
        }
        if self.capability != NETWORK_CAPABILITY {
            return Err(NetworkPerformanceError::UnsupportedCapability);
        }
        if !self.consent.confirmed || self.consent.policy_revision == 0 {
            return Err(NetworkPerformanceError::ConsentRequired);
        }
        if self.consent.expires_at_unix <= now_unix || self.expires_at_unix > self.consent.expires_at_unix {
            return Err(NetworkPerformanceError::ConsentExpired);
        }
        let validity = self
            .expires_at_unix
            .checked_sub(self.produced_at_unix)
            .ok_or(NetworkPerformanceError::Expired)?;
        if self.produced_at_unix > now_unix.saturating_add(MAX_FUTURE_SKEW_SECONDS)
            || validity <= 0
            || validity > MAX_VALIDITY_SECONDS
            || self.expires_at_unix <= now_unix
        {
            return Err(NetworkPerformanceError::Expired);
        }
        if self.duration.is_zero()
            || self.duration.as_millis() == 0
            || self.duration > MAX_NETWORK_DURATION
            || self.peer_aliases.is_empty()
            || self.peer_aliases.len() > MAX_PEERS
            || self.peer_aliases.len() > MAX_OPERATIONS
            || self.traffic_bytes_per_peer == 0
        {
            return Err(NetworkPerformanceError::LimitExceeded);
        }
        let traffic = self
            .traffic_bytes_per_peer
            .checked_mul(self.peer_aliases.len() as u64)
            .ok_or(NetworkPerformanceError::LimitExceeded)?;
        let bandwidth_budget = u64::try_from(
            u128::from(MAX_BANDWIDTH_BYTES_PER_SECOND)
                .checked_mul(self.duration.as_millis())
                .ok_or(NetworkPerformanceError::LimitExceeded)?
                / 1_000,
        )
        .map_err(|_| NetworkPerformanceError::LimitExceeded)?;
        if traffic > MAX_TRAFFIC_BYTES || traffic > bandwidth_budget.max(1) {
            return Err(NetworkPerformanceError::LimitExceeded);
        }
        if !uuid7(&self.run_uid)
            || !uuid7(&self.artifact_uid)
            || !uuid7(&self.consent.consent_uid)
            || !resource_names_match(self)
            || !lower_hex(&self.provenance.source_commit, 40)
            || !lower_hex(&self.provenance.executable_sha256, 64)
            || !version(&self.provenance.rustfs_version)
            || self.provenance.build_features.len() > MAX_BUILD_FEATURES
            || !self.provenance.build_features.iter().all(|value| build_feature(value))
            || !valid_peer_aliases(&self.peer_aliases)
        {
            return Err(NetworkPerformanceError::InvalidRequest);
        }
        Ok(())
    }
}

struct CollectorLease;

impl CollectorLease {
    fn acquire() -> Result<Self, NetworkPerformanceError> {
        NETWORK_COLLECTOR_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self)
            .map_err(|_| NetworkPerformanceError::Busy)
    }
}

impl Drop for CollectorLease {
    fn drop(&mut self) {
        NETWORK_COLLECTOR_ACTIVE.store(false, Ordering::Release);
    }
}

fn terminal_measurement(
    request: &NetworkPerformanceRequest,
    outcome: NetworkOutcome,
    reason_code: NetworkReasonCode,
) -> NetworkMeasurement {
    NetworkMeasurement {
        result: result(request, outcome, reason_code, 0, 0, None),
        peers: Vec::new(),
    }
}

fn cancelled_measurement(
    request: &NetworkPerformanceRequest,
    elapsed: Duration,
    completed_units: u32,
    peers: Vec<NetworkPeerResult>,
) -> NetworkMeasurement {
    NetworkMeasurement {
        result: result(
            request,
            NetworkOutcome::Cancelled,
            NetworkReasonCode::Cancelled,
            elapsed_millis_allow_zero(elapsed),
            completed_units,
            None,
        ),
        peers,
    }
}

fn result(
    request: &NetworkPerformanceRequest,
    outcome: NetworkOutcome,
    reason_code: NetworkReasonCode,
    duration_millis: u64,
    completed_units: u32,
    data: Option<NetworkPerformanceData>,
) -> NetworkDiagnosticResult {
    NetworkDiagnosticResult {
        schema_version: NETWORK_SCHEMA_VERSION,
        run_uid: request.run_uid.clone(),
        tool_id: NETWORK_TOOL_ID,
        capability: NETWORK_CAPABILITY,
        outcome,
        reason_code,
        duration_millis: duration_millis.min(30_000),
        provenance: request.provenance.clone(),
        coverage: NetworkCoverage {
            requested_units: request.peer_aliases.len() as u32,
            completed_units,
            unit: "OPERATION",
        },
        data,
    }
}

fn failed_peer(alias: &str, reason_code: PeerReasonCode, duration_millis: u64) -> NetworkPeerResult {
    NetworkPeerResult {
        peer_alias: alias.to_owned(),
        outcome: NetworkOutcome::Failed,
        reason_code,
        transferred_bytes: 0,
        duration_millis,
        latency_micros: None,
    }
}

fn peer_reason(error: PeerProbeError) -> PeerReasonCode {
    match error {
        PeerProbeError::Unreachable => PeerReasonCode::Unreachable,
        PeerProbeError::TimedOut => PeerReasonCode::TimedOut,
        PeerProbeError::ProtocolFailure => PeerReasonCode::ProtocolFailure,
        PeerProbeError::Cancelled => PeerReasonCode::Cancelled,
    }
}

fn map_native_probe_error(error: NativePeerProbeError) -> PeerProbeError {
    match error {
        NativePeerProbeError::Cancelled => PeerProbeError::Cancelled,
        NativePeerProbeError::Unreachable => PeerProbeError::Unreachable,
        NativePeerProbeError::TimedOut => PeerProbeError::TimedOut,
        NativePeerProbeError::UnknownPeer | NativePeerProbeError::LimitExceeded | NativePeerProbeError::ProtocolFailure => {
            PeerProbeError::ProtocolFailure
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NetworkEnvelope<'a> {
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
    payload: NetworkPayload<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NetworkPayload<'a> {
    path: &'static str,
    media_type: &'static str,
    size_bytes: u64,
    sha256: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NetworkSignature<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: String,
}

fn signature_document(key: &DeviceIdentity, key_id: &str, envelope: &[u8]) -> Result<Vec<u8>, NetworkPerformanceError> {
    let pkcs8 = key.to_pkcs8_der().map_err(|_| NetworkPerformanceError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| NetworkPerformanceError::Signing)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + envelope.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(envelope);
    let signature: Signature = signing_key.sign(&input);
    serde_json::to_vec(&NetworkSignature {
        algorithm: "ES256",
        key_id,
        value: URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes()),
    })
    .map_err(|_| NetworkPerformanceError::Encoding)
}

fn archive(envelope: &[u8], signature: &[u8], result: &[u8]) -> Result<Vec<u8>, NetworkPerformanceError> {
    let cursor = Cursor::new(Vec::with_capacity(envelope.len() + signature.len() + result.len() + 512));
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(OUTPUT_MODE);
    for (name, bytes) in [(ENVELOPE_PATH, envelope), (SIGNATURE_PATH, signature), (RESULT_PATH, result)] {
        writer
            .start_file(name, options)
            .map_err(|_| NetworkPerformanceError::Encoding)?;
        writer.write_all(bytes).map_err(NetworkPerformanceError::Io)?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|_| NetworkPerformanceError::Encoding)
}

fn resource_names_match(request: &NetworkPerformanceRequest) -> bool {
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

fn valid_peer_aliases(aliases: &[String]) -> bool {
    aliases
        .iter()
        .enumerate()
        .all(|(index, alias)| alias == &format!("peer-{}", index + 1))
}

fn uuid7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn build_feature(value: &str) -> bool {
    value.len() <= 64
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-'))
}

fn version(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return false;
    }
    let (core, suffix) = value
        .split_once('-')
        .map_or((value, None), |(core, suffix)| (core, Some(suffix)));
    if suffix.is_some_and(str::is_empty) {
        return false;
    }
    let mut parts = core.split('.');
    parts.clone().count() == 3 && parts.all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

fn timestamp(unix: i64) -> Result<String, NetworkPerformanceError> {
    OffsetDateTime::from_unix_timestamp(unix)
        .map_err(|_| NetworkPerformanceError::InvalidRequest)?
        .format(&Rfc3339)
        .map_err(|_| NetworkPerformanceError::InvalidRequest)
}

fn unix_now() -> Result<i64, NetworkPerformanceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| NetworkPerformanceError::InvalidRequest)?;
    i64::try_from(duration.as_secs()).map_err(|_| NetworkPerformanceError::InvalidRequest)
}

fn check_cancel(cancel: &CancellationToken) -> Result<(), NetworkPerformanceError> {
    if cancel.is_cancelled() {
        Err(NetworkPerformanceError::Cancelled)
    } else {
        Ok(())
    }
}

fn elapsed_millis(duration: Duration) -> u64 {
    elapsed_millis_allow_zero(duration).max(1)
}

fn elapsed_millis_allow_zero(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).min(30_000)
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing hexadecimal to a string cannot fail");
    }
    value
}

fn map_create_error(error: std::io::Error) -> NetworkPerformanceError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        NetworkPerformanceError::AlreadyExists
    } else {
        NetworkPerformanceError::Io(error)
    }
}

fn map_publish_error(error: std::io::Error) -> NetworkPerformanceError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        NetworkPerformanceError::AlreadyExists
    } else {
        NetworkPerformanceError::Io(error)
    }
}
