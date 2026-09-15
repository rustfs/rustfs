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

//! Verification and execution of the small allow-list of Connect diagnostic jobs.

use std::time::Duration;
use std::{fs, io::Read as _, path::Path};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

use base64_simd::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier as _, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};

use super::{
    CPU_PROFILE_CAPABILITY, DRIVE_CAPABILITY, DRIVE_SCHEMA_VERSION, DriveOutcome, DrivePerformanceError, DrivePerformanceRequest,
    DriveProvenance, LocalDriveConsent, LocalNetworkConsent, LocalProfileConsent, LocalTopConsent, MAX_NETWORK_TRAFFIC_BYTES,
    MAX_TOP_EXPORT_VALIDITY, NETWORK_CAPABILITY, NETWORK_SCHEMA_VERSION, NetworkOutcome, NetworkPerformanceError,
    NetworkPerformanceRequest, NetworkProvenance, NetworkReasonCode, PROFILE_SCHEMA_VERSION, ProfileCaptureRequest,
    ProfileOutcome, ProfileProvenance, THREAD_PROFILE_CAPABILITY, TOP_API_CAPABILITY, TOP_CLASSIFICATION, TOP_LOCKS_CAPABILITY,
    TOP_RPC_CAPABILITY, TOP_SCHEMA_VERSION, ThreadProfileScope, TopApiOperation, TopCaptureLimits, TopCaptureRequest,
    TopCaptureScope, TopOutcome, capture_cpu_profile, capture_thread_profile, capture_top_api, capture_top_locks,
    capture_top_rpc, encode_signed_profile_export, measure_drive, measure_network, runtime_network_peer_aliases,
    sign_drive_export, sign_network_export, sign_top_export_with_nonce,
};
use crate::connect::DeviceIdentity;

const PROTOCOL_VERSION: &str = "v1";
const PROFILE_CPU_JOB_TYPE: &str = "profile.cpu";
const PROFILE_THREADS_JOB_TYPE: &str = "profile.threads";
const PERFORMANCE_DRIVE_JOB_TYPE: &str = "performance.drive";
const PERFORMANCE_NETWORK_JOB_TYPE: &str = "performance.network";
const TOP_API_JOB_TYPE: &str = "top.api";
const TOP_LOCKS_JOB_TYPE: &str = "top.locks";
const TOP_RPC_JOB_TYPE: &str = "top.rpc";
pub const DIAGNOSTIC_JOB_SIGNATURE_DOMAIN: &[u8] = b"rustfs-connect-agent-job-v1\0";
const MAX_JOB_LIFETIME_SECONDS: i64 = 1_800;
const MAX_FUTURE_SKEW_SECONDS: i64 = 300;
const MAX_OUTPUT_BYTES: u64 = 524_288;
const MAX_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CPU_MILLIS: u64 = 30_000;
const MAX_NETWORK_CPU_MILLIS: u64 = 5_000;
const MIN_NETWORK_MEMORY_BYTES: u64 = 1_048_576;
const DRIVE_TARGET_ALIAS: &str = "drive-1";
const DRIVE_DURATION_MILLIS: u64 = 5_000;
const DRIVE_SCRATCH_BYTES: u64 = 524_288;
const DRIVE_BLOCK_BYTES: u64 = 65_536;
const DRIVE_SAMPLE_PERIOD_MICROS: u64 = 10_000;
const DRIVE_SCRATCH_DIRECTORY: &str = ".rustfs-connect-drive-scratch";
const MAX_TOP_API_CPU_MILLIS: u64 = 5_000;
const MIN_TOP_API_MEMORY_BYTES: u64 = 1_048_576;
const MAX_TOP_LOCKS_CPU_MILLIS: u64 = 5_000;
const MIN_TOP_LOCKS_MEMORY_BYTES: u64 = 1_048_576;
const MAX_TOP_RPC_CPU_MILLIS: u64 = 5_000;
const MIN_TOP_RPC_MEMORY_BYTES: u64 = 1_048_576;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DiagnosticJobKind {
    ProfileCpu,
    ProfileThreads,
    PerformanceDrive,
    PerformanceNetwork,
    TopApi,
    TopLocks,
    TopRpc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticJobTarget {
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiagnosticJobLimits {
    pub timeout_seconds: u64,
    pub max_output_bytes: u64,
    pub max_memory_bytes: u64,
    pub max_cpu_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiagnosticJobParameters {
    pub artifact_uid: String,
    pub consent_uid: String,
    pub consent_policy_revision: u64,
    pub consent_expires_at: String,
    pub duration_millis: u64,
    pub sample_period_micros: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_alias: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiagnosticJobSignature {
    algorithm: String,
    key_id: String,
    value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiagnosticJobAuthorization {
    actor_type: String,
    actor_name: String,
    request_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiagnosticJobEnvelope {
    pub job_id: String,
    pub protocol_version: String,
    pub job_type: String,
    pub schema_version: u16,
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub create_time: String,
    pub expire_time: String,
    pub nonce: String,
    pub required_capabilities: Vec<String>,
    pub authorization: DiagnosticJobAuthorization,
    pub limits: DiagnosticJobLimits,
    pub parameters: DiagnosticJobParameters,
    pub signature: DiagnosticJobSignature,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnsignedDiagnosticJob<'a> {
    job_id: &'a str,
    protocol_version: &'a str,
    job_type: &'a str,
    schema_version: u16,
    organization_name: &'a str,
    cluster_name: &'a str,
    device_name: &'a str,
    create_time: &'a str,
    expire_time: &'a str,
    nonce: &'a str,
    required_capabilities: &'a [String],
    authorization: &'a DiagnosticJobAuthorization,
    limits: &'a DiagnosticJobLimits,
    parameters: &'a DiagnosticJobParameters,
}

#[derive(Clone, Debug)]
pub struct TrustedDiagnosticJobSigner {
    key_id: String,
    key: VerifyingKey,
}

impl TrustedDiagnosticJobSigner {
    pub fn new(key_id: String, public_key: [u8; 32]) -> Result<Self, DiagnosticJobError> {
        if !lower_hex(&key_id, 64) || hex_lower(&Sha256::digest(public_key)) != key_id {
            return Err(DiagnosticJobError::TrustInvalid);
        }
        let key = VerifyingKey::from_bytes(&public_key).map_err(|_| DiagnosticJobError::TrustInvalid)?;
        Ok(Self { key_id, key })
    }

    pub fn from_public_key_file(path: &Path, key_id: String) -> Result<Self, DiagnosticJobError> {
        #[cfg(not(unix))]
        {
            let _ = (path, key_id);
            return Err(DiagnosticJobError::TrustInvalid);
        }
        #[cfg(unix)]
        {
            let initial = fs::symlink_metadata(path).map_err(|_| DiagnosticJobError::TrustInvalid)?;
            if !initial.file_type().is_file() || initial.permissions().mode() & 0o077 != 0 || initial.len() > 256 {
                return Err(DiagnosticJobError::TrustInvalid);
            }
            let mut options = fs::OpenOptions::new();
            options.read(true).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
            let file = options.open(path).map_err(|_| DiagnosticJobError::TrustInvalid)?;
            let opened = file.metadata().map_err(|_| DiagnosticJobError::TrustInvalid)?;
            if !opened.is_file()
                || opened.uid() != rustix::process::geteuid().as_raw()
                || opened.dev() != initial.dev()
                || opened.ino() != initial.ino()
            {
                return Err(DiagnosticJobError::TrustInvalid);
            }
            let mut encoded = Vec::new();
            file.take(257)
                .read_to_end(&mut encoded)
                .map_err(|_| DiagnosticJobError::TrustInvalid)?;
            if encoded.len() > 256 {
                return Err(DiagnosticJobError::TrustInvalid);
            }
            let encoded = std::str::from_utf8(&encoded)
                .map_err(|_| DiagnosticJobError::TrustInvalid)?
                .trim();
            let public_key = URL_SAFE_NO_PAD
                .decode_to_vec(encoded.as_bytes())
                .map_err(|_| DiagnosticJobError::TrustInvalid)?;
            if URL_SAFE_NO_PAD.encode_to_string(&public_key) != encoded {
                return Err(DiagnosticJobError::TrustInvalid);
            }
            Self::new(key_id, public_key.try_into().map_err(|_| DiagnosticJobError::TrustInvalid)?)
        }
    }

    pub fn verify(
        &self,
        envelope: &DiagnosticJobEnvelope,
        target: &DiagnosticJobTarget,
        now: DateTime<Utc>,
    ) -> Result<VerifiedDiagnosticJob, DiagnosticJobError> {
        envelope.validate(target, now)?;
        if envelope.signature.algorithm != "Ed25519" || envelope.signature.key_id != self.key_id {
            return Err(DiagnosticJobError::SignerUntrusted);
        }
        let encoded = URL_SAFE_NO_PAD
            .decode_to_vec(envelope.signature.value.as_bytes())
            .map_err(|_| DiagnosticJobError::SignatureInvalid)?;
        let signature = Signature::from_slice(&encoded).map_err(|_| DiagnosticJobError::SignatureInvalid)?;
        let payload = envelope.signing_payload()?;
        self.key
            .verify(&payload, &signature)
            .map_err(|_| DiagnosticJobError::SignatureInvalid)?;
        let nonce = URL_SAFE_NO_PAD
            .decode_to_vec(envelope.nonce.as_bytes())
            .map_err(|_| DiagnosticJobError::Invalid)?
            .try_into()
            .map_err(|_| DiagnosticJobError::Invalid)?;
        Ok(VerifiedDiagnosticJob {
            envelope: envelope.clone(),
            nonce,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedDiagnosticJob {
    envelope: DiagnosticJobEnvelope,
    nonce: [u8; 32],
}

impl VerifiedDiagnosticJob {
    pub fn job_id(&self) -> &str {
        &self.envelope.job_id
    }

    pub(crate) fn expire_time(&self) -> &str {
        &self.envelope.expire_time
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiagnosticJobExecution {
    pub job_id: String,
    pub outcome: String,
    pub reason: String,
    pub artifact_uid: Option<String>,
    pub artifact_sha256: Option<String>,
    pub artifact_bytes: Option<Vec<u8>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DiagnosticJobError {
    #[error("connect_diagnostic_job_invalid")]
    Invalid,
    #[error("connect_diagnostic_job_target_mismatch")]
    TargetMismatch,
    #[error("connect_diagnostic_job_expired")]
    Expired,
    #[error("connect_diagnostic_job_unsupported")]
    Unsupported,
    #[error("connect_diagnostic_job_limit_exceeded")]
    LimitExceeded,
    #[error("connect_diagnostic_job_trust_invalid")]
    TrustInvalid,
    #[error("connect_diagnostic_job_signer_untrusted")]
    SignerUntrusted,
    #[error("connect_diagnostic_job_signature_invalid")]
    SignatureInvalid,
    #[error("connect_diagnostic_job_encoding_failed")]
    Encoding,
    #[error("connect_diagnostic_job_cancelled")]
    Cancelled,
    #[error("connect_diagnostic_job_collection_failed")]
    CollectionFailed,
    #[error("connect_diagnostic_job_profile_source_unavailable")]
    ProfileSourceUnavailable,
    #[error("connect_diagnostic_job_export_failed")]
    ExportFailed,
}

impl DiagnosticJobError {
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Invalid => "INVALID",
            Self::TargetMismatch => "TARGET_MISMATCH",
            Self::Expired => "EXPIRED",
            Self::Unsupported => "UNSUPPORTED",
            Self::LimitExceeded => "LIMIT_EXCEEDED",
            Self::TrustInvalid => "TRUST_INVALID",
            Self::SignerUntrusted => "SIGNER_UNTRUSTED",
            Self::SignatureInvalid => "SIGNATURE_INVALID",
            Self::Encoding => "ENCODING_FAILED",
            Self::Cancelled => "CANCELLED",
            Self::CollectionFailed => "COLLECTION_FAILED",
            Self::ProfileSourceUnavailable => "PROFILE_SOURCE_UNAVAILABLE",
            Self::ExportFailed => "EXPORT_FAILED",
        }
    }
}

impl DiagnosticJobEnvelope {
    fn unsigned(&self) -> UnsignedDiagnosticJob<'_> {
        UnsignedDiagnosticJob {
            job_id: &self.job_id,
            protocol_version: &self.protocol_version,
            job_type: &self.job_type,
            schema_version: self.schema_version,
            organization_name: &self.organization_name,
            cluster_name: &self.cluster_name,
            device_name: &self.device_name,
            create_time: &self.create_time,
            expire_time: &self.expire_time,
            nonce: &self.nonce,
            required_capabilities: &self.required_capabilities,
            authorization: &self.authorization,
            limits: &self.limits,
            parameters: &self.parameters,
        }
    }

    pub fn signing_payload(&self) -> Result<Vec<u8>, DiagnosticJobError> {
        let payload = serde_json::to_vec(&self.unsigned()).map_err(|_| DiagnosticJobError::Encoding)?;
        let mut signed = Vec::with_capacity(DIAGNOSTIC_JOB_SIGNATURE_DOMAIN.len() + payload.len());
        signed.extend_from_slice(DIAGNOSTIC_JOB_SIGNATURE_DOMAIN);
        signed.extend_from_slice(&payload);
        Ok(signed)
    }

    fn validate(&self, target: &DiagnosticJobTarget, now: DateTime<Utc>) -> Result<(), DiagnosticJobError> {
        let kind = self.kind()?;
        if self.protocol_version != PROTOCOL_VERSION || self.schema_version != PROFILE_SCHEMA_VERSION {
            return Err(DiagnosticJobError::Unsupported);
        }
        if self.organization_name != target.organization_name
            || self.cluster_name != target.cluster_name
            || self.device_name != target.device_name
        {
            return Err(DiagnosticJobError::TargetMismatch);
        }
        if !uuid7(&self.job_id)
            || !uuid7(&self.parameters.artifact_uid)
            || !uuid7(&self.parameters.consent_uid)
            || self.authorization.actor_type != "BROWSER_USER"
            || !self.authorization.actor_name.strip_prefix("users/").is_some_and(uuid7)
            || !Uuid::parse_str(&self.authorization.request_id)
                .is_ok_and(|value| value.get_version_num() == 4 && value.to_string() == self.authorization.request_id)
            || self.parameters.consent_policy_revision == 0
        {
            return Err(DiagnosticJobError::Invalid);
        }
        let create = parse_time(&self.create_time)?;
        let expire = parse_time(&self.expire_time)?;
        let consent_expire = parse_time(&self.parameters.consent_expires_at)?;
        if create > now + chrono::Duration::seconds(MAX_FUTURE_SKEW_SECONDS)
            || expire <= now
            || expire <= create
            || expire > create + chrono::Duration::seconds(MAX_JOB_LIFETIME_SECONDS)
            || consent_expire < expire
        {
            return Err(DiagnosticJobError::Expired);
        }
        let nonce = URL_SAFE_NO_PAD
            .decode_to_vec(self.nonce.as_bytes())
            .map_err(|_| DiagnosticJobError::Invalid)?;
        if nonce.len() != 32
            || URL_SAFE_NO_PAD.encode_to_string(&nonce) != self.nonce
            || self.limits.timeout_seconds == 0
            || self.limits.timeout_seconds > 30
            || self.limits.max_output_bytes == 0
            || self.limits.max_output_bytes > MAX_OUTPUT_BYTES
            || self.limits.max_memory_bytes == 0
            || self.limits.max_memory_bytes > MAX_MEMORY_BYTES
            || self.limits.max_cpu_millis == 0
            || self.limits.max_cpu_millis > MAX_CPU_MILLIS
            || self.parameters.duration_millis == 0
            || self.parameters.duration_millis > self.limits.timeout_seconds.saturating_mul(1_000)
            || self.parameters.duration_millis > self.limits.max_cpu_millis
            || self.parameters.sample_period_micros == 0
            || self.parameters.sample_period_micros > self.parameters.duration_millis.saturating_mul(1_000)
        {
            return Err(DiagnosticJobError::LimitExceeded);
        }
        if kind == DiagnosticJobKind::TopApi
            && (self.limits.max_cpu_millis > MAX_TOP_API_CPU_MILLIS || self.limits.max_memory_bytes < MIN_TOP_API_MEMORY_BYTES)
        {
            return Err(DiagnosticJobError::LimitExceeded);
        }
        match (kind, self.parameters.traffic_bytes) {
            (DiagnosticJobKind::PerformanceNetwork, Some(1..=MAX_NETWORK_TRAFFIC_BYTES)) => {
                if self.limits.max_cpu_millis > MAX_NETWORK_CPU_MILLIS || self.limits.max_memory_bytes < MIN_NETWORK_MEMORY_BYTES
                {
                    return Err(DiagnosticJobError::LimitExceeded);
                }
            }
            (DiagnosticJobKind::PerformanceNetwork, _) => return Err(DiagnosticJobError::LimitExceeded),
            (_, None) => {}
            (_, Some(_)) => return Err(DiagnosticJobError::Invalid),
        }
        let has_drive_parameters = self.parameters.target_alias.is_some()
            || self.parameters.scratch_bytes.is_some()
            || self.parameters.block_bytes.is_some();
        if kind == DiagnosticJobKind::PerformanceDrive {
            if self.parameters.target_alias.as_deref() != Some(DRIVE_TARGET_ALIAS) {
                return Err(DiagnosticJobError::Invalid);
            }
            if self.parameters.scratch_bytes != Some(DRIVE_SCRATCH_BYTES)
                || self.parameters.block_bytes != Some(DRIVE_BLOCK_BYTES)
                || self.parameters.duration_millis != DRIVE_DURATION_MILLIS
                || self.parameters.sample_period_micros != DRIVE_SAMPLE_PERIOD_MICROS
                || self.limits.timeout_seconds != 30
                || self.limits.max_output_bytes != MAX_OUTPUT_BYTES
                || self.limits.max_memory_bytes != MAX_MEMORY_BYTES
                || self.limits.max_cpu_millis != DRIVE_DURATION_MILLIS
            {
                return Err(DiagnosticJobError::LimitExceeded);
            }
        } else if has_drive_parameters {
            return Err(DiagnosticJobError::Invalid);
        }
        if kind == DiagnosticJobKind::TopLocks
            && (self.limits.max_cpu_millis > MAX_TOP_LOCKS_CPU_MILLIS
                || self.limits.max_memory_bytes < MIN_TOP_LOCKS_MEMORY_BYTES)
        {
            return Err(DiagnosticJobError::LimitExceeded);
        }
        if kind == DiagnosticJobKind::TopRpc
            && (self.limits.max_cpu_millis > MAX_TOP_RPC_CPU_MILLIS || self.limits.max_memory_bytes < MIN_TOP_RPC_MEMORY_BYTES)
        {
            return Err(DiagnosticJobError::LimitExceeded);
        }
        Ok(())
    }

    fn kind(&self) -> Result<DiagnosticJobKind, DiagnosticJobError> {
        match (self.job_type.as_str(), self.required_capabilities.as_slice(), self.schema_version) {
            (PROFILE_CPU_JOB_TYPE, [capability], PROFILE_SCHEMA_VERSION) if capability == CPU_PROFILE_CAPABILITY => {
                Ok(DiagnosticJobKind::ProfileCpu)
            }
            (PROFILE_THREADS_JOB_TYPE, [capability], PROFILE_SCHEMA_VERSION) if capability == THREAD_PROFILE_CAPABILITY => {
                Ok(DiagnosticJobKind::ProfileThreads)
            }
            (PERFORMANCE_DRIVE_JOB_TYPE, [capability], DRIVE_SCHEMA_VERSION) if capability == DRIVE_CAPABILITY => {
                Ok(DiagnosticJobKind::PerformanceDrive)
            }
            (PERFORMANCE_NETWORK_JOB_TYPE, [capability], NETWORK_SCHEMA_VERSION) if capability == NETWORK_CAPABILITY => {
                Ok(DiagnosticJobKind::PerformanceNetwork)
            }
            (TOP_API_JOB_TYPE, [capability], version)
                if capability == TOP_API_CAPABILITY && version == u16::from(TOP_SCHEMA_VERSION) =>
            {
                Ok(DiagnosticJobKind::TopApi)
            }
            (TOP_LOCKS_JOB_TYPE, [capability], version)
                if capability == TOP_LOCKS_CAPABILITY && version == u16::from(TOP_SCHEMA_VERSION) =>
            {
                Ok(DiagnosticJobKind::TopLocks)
            }
            (TOP_RPC_JOB_TYPE, [capability], version)
                if capability == TOP_RPC_CAPABILITY && version == u16::from(TOP_SCHEMA_VERSION) =>
            {
                Ok(DiagnosticJobKind::TopRpc)
            }
            _ => Err(DiagnosticJobError::Unsupported),
        }
    }
}

pub async fn execute_diagnostic_job(
    job: VerifiedDiagnosticJob,
    identity: &DeviceIdentity,
    provenance: ProfileProvenance,
    cancel: &CancellationToken,
) -> Result<DiagnosticJobExecution, DiagnosticJobError> {
    if cancel.is_cancelled() {
        return Err(DiagnosticJobError::Cancelled);
    }
    let nonce = job.nonce;
    let envelope = job.envelope;
    match envelope.kind()? {
        DiagnosticJobKind::ProfileCpu => execute_profile_cpu_job(envelope, nonce, identity, provenance, cancel).await,
        DiagnosticJobKind::ProfileThreads => execute_profile_threads_job(envelope, nonce, identity, provenance, cancel).await,
        DiagnosticJobKind::PerformanceDrive => {
            let Some(scratch_root) = runtime_drive_scratch_root() else {
                return Ok(failed_drive_execution(&envelope.job_id, "SOURCE_UNAVAILABLE"));
            };
            if !ensure_drive_scratch_root(&scratch_root) {
                return Ok(failed_drive_execution(&envelope.job_id, "SOURCE_UNAVAILABLE"));
            }
            execute_performance_drive_job(envelope, nonce, identity, provenance, &scratch_root, cancel).await
        }
        DiagnosticJobKind::PerformanceNetwork => {
            execute_performance_network_job(envelope, nonce, identity, provenance, cancel).await
        }
        DiagnosticJobKind::TopApi => execute_top_api_job(envelope, nonce, identity, provenance, cancel).await,
        DiagnosticJobKind::TopLocks => execute_top_locks_job(envelope, nonce, identity, provenance, cancel).await,
        DiagnosticJobKind::TopRpc => execute_top_rpc_job(envelope, nonce, identity, provenance, cancel).await,
    }
}

fn runtime_drive_scratch_root() -> Option<std::path::PathBuf> {
    let endpoint_pools = crate::runtime_sources::current_endpoints_handle()?;
    drive_scratch_root_from_endpoints(&endpoint_pools)
}

fn drive_scratch_root_from_endpoints(
    endpoint_pools: &crate::storage_api::cluster::EndpointServerPools,
) -> Option<std::path::PathBuf> {
    endpoint_pools
        .as_ref()
        .iter()
        .flat_map(|pool| pool.endpoints.as_ref())
        .find(|endpoint| endpoint.is_local)
        .map(|endpoint| std::path::PathBuf::from(endpoint.get_file_path()).join(DRIVE_SCRATCH_DIRECTORY))
}

fn ensure_drive_scratch_root(path: &Path) -> bool {
    let created = if path.exists() {
        true
    } else {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        builder.create(path).is_ok()
    };
    if !created {
        return false;
    }
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return false;
    }
    #[cfg(unix)]
    if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.permissions().mode() & 0o077 != 0 {
        return false;
    }
    true
}

fn failed_drive_execution(job_id: &str, reason: &str) -> DiagnosticJobExecution {
    DiagnosticJobExecution {
        job_id: job_id.to_owned(),
        outcome: "FAILED".to_owned(),
        reason: reason.to_owned(),
        artifact_uid: None,
        artifact_sha256: None,
        artifact_bytes: None,
    }
}

async fn execute_performance_drive_job(
    envelope: DiagnosticJobEnvelope,
    nonce: [u8; 32],
    identity: &DeviceIdentity,
    provenance: ProfileProvenance,
    scratch_root: &Path,
    cancel: &CancellationToken,
) -> Result<DiagnosticJobExecution, DiagnosticJobError> {
    let expire = parse_time(&envelope.expire_time)?;
    let consent_expire = parse_time(&envelope.parameters.consent_expires_at)?;
    let request = DrivePerformanceRequest {
        organization_name: envelope.organization_name,
        cluster_name: envelope.cluster_name,
        device_name: envelope.device_name,
        run_uid: envelope.job_id.clone(),
        artifact_uid: envelope.parameters.artifact_uid,
        schema_version: envelope.schema_version,
        capability: DRIVE_CAPABILITY.to_owned(),
        consent: LocalDriveConsent {
            consent_uid: envelope.parameters.consent_uid,
            policy_revision: envelope.parameters.consent_policy_revision,
            expires_at_unix: consent_expire.timestamp(),
            confirmed: true,
        },
        produced_at_unix: Utc::now().timestamp(),
        expires_at_unix: expire.timestamp(),
        nonce,
        duration: Duration::from_millis(envelope.parameters.duration_millis),
        target_alias: DRIVE_TARGET_ALIAS.to_owned(),
        scratch_root: scratch_root.to_path_buf(),
        scratch_bytes: envelope.parameters.scratch_bytes.ok_or(DiagnosticJobError::LimitExceeded)?,
        block_bytes: envelope.parameters.block_bytes.ok_or(DiagnosticJobError::LimitExceeded)?,
        provenance: DriveProvenance::new(
            provenance.source_commit(),
            provenance.executable_sha256(),
            provenance.rustfs_version(),
            provenance.build_features().to_vec(),
        ),
    };
    let measurement = measure_drive(&request, cancel).await.map_err(drive_capture_failure)?;
    let outcome = measurement.result.outcome();
    let reason = measurement.result.reason_code();
    if outcome != DriveOutcome::Succeeded {
        return Ok(DiagnosticJobExecution {
            job_id: envelope.job_id,
            outcome: outcome.as_str().to_owned(),
            reason: reason.as_str().to_owned(),
            artifact_uid: None,
            artifact_sha256: None,
            artifact_bytes: None,
        });
    }
    let export = sign_drive_export(&request, &measurement, identity, cancel).map_err(drive_export_failure)?;
    if export.archive_bytes.len() > usize::try_from(envelope.limits.max_output_bytes).unwrap_or(usize::MAX) {
        return Err(DiagnosticJobError::LimitExceeded);
    }
    Ok(DiagnosticJobExecution {
        job_id: envelope.job_id,
        outcome: outcome.as_str().to_owned(),
        reason: reason.as_str().to_owned(),
        artifact_uid: Some(export.artifact_uid),
        artifact_sha256: Some(export.archive_sha256),
        artifact_bytes: Some(export.archive_bytes),
    })
}

async fn execute_performance_network_job(
    envelope: DiagnosticJobEnvelope,
    nonce: [u8; 32],
    identity: &DeviceIdentity,
    provenance: ProfileProvenance,
    cancel: &CancellationToken,
) -> Result<DiagnosticJobExecution, DiagnosticJobError> {
    let Some(peer_aliases) = runtime_network_peer_aliases() else {
        return Ok(DiagnosticJobExecution {
            job_id: envelope.job_id,
            outcome: "FAILED".to_owned(),
            reason: "SOURCE_UNAVAILABLE".to_owned(),
            artifact_uid: None,
            artifact_sha256: None,
            artifact_bytes: None,
        });
    };
    let traffic_bytes = envelope.parameters.traffic_bytes.ok_or(DiagnosticJobError::LimitExceeded)?;
    let peer_count = u64::try_from(peer_aliases.len()).map_err(|_| DiagnosticJobError::LimitExceeded)?;
    let traffic_bytes_per_peer = traffic_bytes
        .checked_div(peer_count)
        .filter(|value| *value > 0)
        .ok_or(DiagnosticJobError::LimitExceeded)?;
    let expire = parse_time(&envelope.expire_time)?;
    let consent_expire = parse_time(&envelope.parameters.consent_expires_at)?;
    let request = NetworkPerformanceRequest {
        organization_name: envelope.organization_name,
        cluster_name: envelope.cluster_name,
        device_name: envelope.device_name,
        run_uid: envelope.job_id.clone(),
        artifact_uid: envelope.parameters.artifact_uid,
        schema_version: envelope.schema_version,
        capability: NETWORK_CAPABILITY.to_owned(),
        consent: LocalNetworkConsent {
            consent_uid: envelope.parameters.consent_uid,
            policy_revision: envelope.parameters.consent_policy_revision,
            expires_at_unix: consent_expire.timestamp(),
            confirmed: true,
        },
        produced_at_unix: Utc::now().timestamp(),
        expires_at_unix: expire.timestamp(),
        nonce,
        duration: Duration::from_millis(envelope.parameters.duration_millis),
        peer_aliases,
        traffic_bytes_per_peer,
        provenance: NetworkProvenance::new(
            provenance.source_commit(),
            provenance.executable_sha256(),
            provenance.rustfs_version(),
            provenance.build_features().to_vec(),
        ),
    };
    let measurement = measure_network(&request, cancel).await.map_err(network_capture_failure)?;
    let measured_outcome = measurement.result.outcome();
    let measured_reason = measurement.result.reason_code();
    let outcome = measured_outcome.as_str().to_owned();
    let reason = measured_reason.as_str().to_owned();
    if !matches!(measured_outcome, NetworkOutcome::Succeeded | NetworkOutcome::Partial) {
        let (outcome, reason) = match (measured_outcome, measured_reason) {
            (NetworkOutcome::Unsupported, NetworkReasonCode::SourceUnavailable) => {
                ("FAILED".to_owned(), "SOURCE_UNAVAILABLE".to_owned())
            }
            (NetworkOutcome::Unsupported, _) => ("FAILED".to_owned(), "COLLECTION_FAILED".to_owned()),
            _ => (outcome, reason),
        };
        return Ok(DiagnosticJobExecution {
            job_id: envelope.job_id,
            outcome,
            reason,
            artifact_uid: None,
            artifact_sha256: None,
            artifact_bytes: None,
        });
    }
    let export = sign_network_export(&request, &measurement, identity, cancel).map_err(network_export_failure)?;
    if export.archive_bytes.len() > usize::try_from(envelope.limits.max_output_bytes).unwrap_or(usize::MAX) {
        return Err(DiagnosticJobError::LimitExceeded);
    }
    Ok(DiagnosticJobExecution {
        job_id: envelope.job_id,
        outcome,
        reason,
        artifact_uid: Some(export.artifact_uid),
        artifact_sha256: Some(export.archive_sha256),
        artifact_bytes: Some(export.archive_bytes),
    })
}

async fn execute_profile_threads_job(
    envelope: DiagnosticJobEnvelope,
    nonce: [u8; 32],
    identity: &DeviceIdentity,
    provenance: ProfileProvenance,
    cancel: &CancellationToken,
) -> Result<DiagnosticJobExecution, DiagnosticJobError> {
    let expire = parse_time(&envelope.expire_time)?;
    let consent_expire = parse_time(&envelope.parameters.consent_expires_at)?;
    let request = ProfileCaptureRequest {
        organization_name: envelope.organization_name,
        cluster_name: envelope.cluster_name,
        device_name: envelope.device_name,
        run_uid: envelope.job_id.clone(),
        artifact_uid: envelope.parameters.artifact_uid.clone(),
        schema_version: envelope.schema_version,
        capability: THREAD_PROFILE_CAPABILITY.to_owned(),
        consent: LocalProfileConsent {
            consent_uid: envelope.parameters.consent_uid,
            policy_revision: envelope.parameters.consent_policy_revision,
            expires_at_unix: consent_expire.timestamp(),
            confirmed: true,
        },
        produced_at_unix: Utc::now().timestamp(),
        expires_at_unix: expire.timestamp(),
        nonce,
        duration: Duration::from_millis(envelope.parameters.duration_millis),
        sample_period: Duration::from_micros(envelope.parameters.sample_period_micros),
        provenance,
    };
    let owned_request = request.clone();
    let owned_cancel = cancel.clone();
    let result = tokio::task::spawn_blocking(move || {
        capture_thread_profile(&owned_request, ThreadProfileScope::NativeThreads, &owned_cancel)
    })
    .await
    .map_err(|_| DiagnosticJobError::CollectionFailed)?
    .map_err(capture_failure)?;
    if result.outcome() == ProfileOutcome::Unsupported {
        return Ok(DiagnosticJobExecution {
            job_id: envelope.job_id,
            outcome: result.outcome().as_str().to_owned(),
            reason: result.reason_code().as_str().to_owned(),
            artifact_uid: None,
            artifact_sha256: None,
            artifact_bytes: None,
        });
    }
    let export = encode_signed_profile_export(&request, &result, identity, cancel).map_err(export_failure)?;
    if export.archive_bytes.len() > usize::try_from(envelope.limits.max_output_bytes).unwrap_or(usize::MAX) {
        return Err(DiagnosticJobError::LimitExceeded);
    }
    Ok(DiagnosticJobExecution {
        job_id: envelope.job_id,
        outcome: result.outcome().as_str().to_owned(),
        reason: result.reason_code().as_str().to_owned(),
        artifact_uid: Some(export.artifact_uid),
        artifact_sha256: Some(export.archive_sha256),
        artifact_bytes: Some(export.archive_bytes),
    })
}

async fn execute_profile_cpu_job(
    envelope: DiagnosticJobEnvelope,
    nonce: [u8; 32],
    identity: &DeviceIdentity,
    provenance: ProfileProvenance,
    cancel: &CancellationToken,
) -> Result<DiagnosticJobExecution, DiagnosticJobError> {
    let expire = parse_time(&envelope.expire_time)?;
    let consent_expire = parse_time(&envelope.parameters.consent_expires_at)?;
    let request = ProfileCaptureRequest {
        organization_name: envelope.organization_name,
        cluster_name: envelope.cluster_name,
        device_name: envelope.device_name,
        run_uid: envelope.job_id.clone(),
        artifact_uid: envelope.parameters.artifact_uid.clone(),
        schema_version: envelope.schema_version,
        capability: CPU_PROFILE_CAPABILITY.to_owned(),
        consent: LocalProfileConsent {
            consent_uid: envelope.parameters.consent_uid,
            policy_revision: envelope.parameters.consent_policy_revision,
            expires_at_unix: consent_expire.timestamp(),
            confirmed: true,
        },
        produced_at_unix: Utc::now().timestamp(),
        expires_at_unix: expire.timestamp(),
        nonce,
        duration: Duration::from_millis(envelope.parameters.duration_millis),
        sample_period: Duration::from_micros(envelope.parameters.sample_period_micros),
        provenance,
    };
    let result = capture_cpu_profile(&request, cancel).await.map_err(capture_failure)?;
    let export = encode_signed_profile_export(&request, &result, identity, cancel).map_err(export_failure)?;
    if export.archive_bytes.len() > usize::try_from(envelope.limits.max_output_bytes).unwrap_or(usize::MAX) {
        return Err(DiagnosticJobError::LimitExceeded);
    }
    let outcome = result.outcome().as_str();
    let reason = result.reason_code().as_str();
    Ok(DiagnosticJobExecution {
        job_id: envelope.job_id,
        outcome: outcome.to_owned(),
        reason: reason.to_owned(),
        artifact_uid: Some(export.artifact_uid),
        artifact_sha256: Some(export.archive_sha256),
        artifact_bytes: Some(export.archive_bytes),
    })
}

async fn execute_top_api_job(
    envelope: DiagnosticJobEnvelope,
    nonce: [u8; 32],
    identity: &DeviceIdentity,
    provenance: ProfileProvenance,
    cancel: &CancellationToken,
) -> Result<DiagnosticJobExecution, DiagnosticJobError> {
    let expire = parse_time(&envelope.expire_time)?;
    let consent_expire = parse_time(&envelope.parameters.consent_expires_at)?;
    let request = TopCaptureRequest {
        scope: TopCaptureScope {
            organization_name: envelope.organization_name,
            cluster_name: envelope.cluster_name,
            device_name: envelope.device_name,
            run_uid: envelope.job_id.clone(),
            artifact_uid: envelope.parameters.artifact_uid,
            policy_revision: envelope.parameters.consent_policy_revision,
            run_expires_at_unix: expire.timestamp(),
            executable_sha256: provenance.executable_sha256().to_owned(),
            build_features: provenance.build_features().to_vec(),
            consent: LocalTopConsent {
                uid: envelope.parameters.consent_uid,
                tool_id: TOP_API_JOB_TYPE.to_owned(),
                classification: TOP_CLASSIFICATION.to_owned(),
                active: true,
                expires_at_unix: consent_expire.timestamp(),
            },
        },
        limits: TopCaptureLimits {
            max_duration_millis: envelope.parameters.duration_millis,
            max_working_memory_bytes: envelope.limits.max_memory_bytes,
            max_cpu_millis: envelope.limits.max_cpu_millis,
            ..TopCaptureLimits::default()
        },
        window: Duration::from_millis(envelope.parameters.duration_millis),
        export_validity: MAX_TOP_EXPORT_VALIDITY,
    };
    let result = capture_top_api(&request, TopApiOperation::GetObject, cancel)
        .await
        .map_err(top_capture_failure)?;
    let outcome = result.outcome.as_str().to_owned();
    let reason = result.reason_code.as_str().to_owned();
    if !matches!(result.outcome, TopOutcome::Succeeded | TopOutcome::Partial) {
        return Ok(DiagnosticJobExecution {
            job_id: envelope.job_id,
            outcome,
            reason,
            artifact_uid: None,
            artifact_sha256: None,
            artifact_bytes: None,
        });
    }
    let export = sign_top_export_with_nonce(&request, &result, identity, cancel, nonce).map_err(top_export_failure)?;
    if export.archive_bytes.len() > usize::try_from(envelope.limits.max_output_bytes).unwrap_or(usize::MAX) {
        return Err(DiagnosticJobError::LimitExceeded);
    }
    Ok(DiagnosticJobExecution {
        job_id: envelope.job_id,
        outcome,
        reason,
        artifact_uid: Some(export.artifact_uid),
        artifact_sha256: Some(export.archive_sha256),
        artifact_bytes: Some(export.archive_bytes),
    })
}

async fn execute_top_locks_job(
    envelope: DiagnosticJobEnvelope,
    nonce: [u8; 32],
    identity: &DeviceIdentity,
    provenance: ProfileProvenance,
    cancel: &CancellationToken,
) -> Result<DiagnosticJobExecution, DiagnosticJobError> {
    let expire = parse_time(&envelope.expire_time)?;
    let consent_expire = parse_time(&envelope.parameters.consent_expires_at)?;
    let request = TopCaptureRequest {
        scope: TopCaptureScope {
            organization_name: envelope.organization_name,
            cluster_name: envelope.cluster_name,
            device_name: envelope.device_name,
            run_uid: envelope.job_id.clone(),
            artifact_uid: envelope.parameters.artifact_uid,
            policy_revision: envelope.parameters.consent_policy_revision,
            run_expires_at_unix: expire.timestamp(),
            executable_sha256: provenance.executable_sha256().to_owned(),
            build_features: provenance.build_features().to_vec(),
            consent: LocalTopConsent {
                uid: envelope.parameters.consent_uid,
                tool_id: TOP_LOCKS_JOB_TYPE.to_owned(),
                classification: TOP_CLASSIFICATION.to_owned(),
                active: true,
                expires_at_unix: consent_expire.timestamp(),
            },
        },
        limits: TopCaptureLimits {
            max_duration_millis: envelope.parameters.duration_millis,
            max_working_memory_bytes: envelope.limits.max_memory_bytes,
            max_cpu_millis: envelope.limits.max_cpu_millis,
            ..TopCaptureLimits::default()
        },
        window: Duration::from_millis(envelope.parameters.duration_millis),
        export_validity: MAX_TOP_EXPORT_VALIDITY,
    };
    let result = capture_top_locks(&request, cancel).await.map_err(top_capture_failure)?;
    let outcome = result.outcome.as_str().to_owned();
    let reason = result.reason_code.as_str().to_owned();
    if !matches!(result.outcome, TopOutcome::Succeeded | TopOutcome::Partial) {
        return Ok(DiagnosticJobExecution {
            job_id: envelope.job_id,
            outcome,
            reason,
            artifact_uid: None,
            artifact_sha256: None,
            artifact_bytes: None,
        });
    }
    let export = sign_top_export_with_nonce(&request, &result, identity, cancel, nonce).map_err(top_export_failure)?;
    if export.archive_bytes.len() > usize::try_from(envelope.limits.max_output_bytes).unwrap_or(usize::MAX) {
        return Err(DiagnosticJobError::LimitExceeded);
    }
    Ok(DiagnosticJobExecution {
        job_id: envelope.job_id,
        outcome,
        reason,
        artifact_uid: Some(export.artifact_uid),
        artifact_sha256: Some(export.archive_sha256),
        artifact_bytes: Some(export.archive_bytes),
    })
}

async fn execute_top_rpc_job(
    envelope: DiagnosticJobEnvelope,
    nonce: [u8; 32],
    identity: &DeviceIdentity,
    provenance: ProfileProvenance,
    cancel: &CancellationToken,
) -> Result<DiagnosticJobExecution, DiagnosticJobError> {
    let expire = parse_time(&envelope.expire_time)?;
    let consent_expire = parse_time(&envelope.parameters.consent_expires_at)?;
    let request = TopCaptureRequest {
        scope: TopCaptureScope {
            organization_name: envelope.organization_name,
            cluster_name: envelope.cluster_name,
            device_name: envelope.device_name,
            run_uid: envelope.job_id.clone(),
            artifact_uid: envelope.parameters.artifact_uid,
            policy_revision: envelope.parameters.consent_policy_revision,
            run_expires_at_unix: expire.timestamp(),
            executable_sha256: provenance.executable_sha256().to_owned(),
            build_features: provenance.build_features().to_vec(),
            consent: LocalTopConsent {
                uid: envelope.parameters.consent_uid,
                tool_id: TOP_RPC_JOB_TYPE.to_owned(),
                classification: TOP_CLASSIFICATION.to_owned(),
                active: true,
                expires_at_unix: consent_expire.timestamp(),
            },
        },
        limits: TopCaptureLimits {
            max_duration_millis: envelope.parameters.duration_millis,
            max_working_memory_bytes: envelope.limits.max_memory_bytes,
            max_cpu_millis: envelope.limits.max_cpu_millis,
            ..TopCaptureLimits::default()
        },
        window: Duration::from_millis(envelope.parameters.duration_millis),
        export_validity: MAX_TOP_EXPORT_VALIDITY,
    };
    let result = capture_top_rpc(&request, cancel).await.map_err(top_capture_failure)?;
    let outcome = result.outcome.as_str().to_owned();
    let reason = result.reason_code.as_str().to_owned();
    if !matches!(result.outcome, TopOutcome::Succeeded | TopOutcome::Partial) {
        return Ok(DiagnosticJobExecution {
            job_id: envelope.job_id,
            outcome,
            reason,
            artifact_uid: None,
            artifact_sha256: None,
            artifact_bytes: None,
        });
    }
    let export = sign_top_export_with_nonce(&request, &result, identity, cancel, nonce).map_err(top_export_failure)?;
    if export.archive_bytes.len() > usize::try_from(envelope.limits.max_output_bytes).unwrap_or(usize::MAX) {
        return Err(DiagnosticJobError::LimitExceeded);
    }
    Ok(DiagnosticJobExecution {
        job_id: envelope.job_id,
        outcome,
        reason,
        artifact_uid: Some(export.artifact_uid),
        artifact_sha256: Some(export.archive_sha256),
        artifact_bytes: Some(export.archive_bytes),
    })
}

fn capture_failure(error: super::ProfileError) -> DiagnosticJobError {
    match error {
        super::ProfileError::Cancelled => DiagnosticJobError::Cancelled,
        super::ProfileError::LimitExceeded | super::ProfileError::TimedOut => DiagnosticJobError::LimitExceeded,
        super::ProfileError::SourceUnavailable => DiagnosticJobError::ProfileSourceUnavailable,
        _ => DiagnosticJobError::CollectionFailed,
    }
}

fn export_failure(error: super::ProfileError) -> DiagnosticJobError {
    match error {
        super::ProfileError::Cancelled => DiagnosticJobError::Cancelled,
        super::ProfileError::LimitExceeded => DiagnosticJobError::LimitExceeded,
        _ => DiagnosticJobError::ExportFailed,
    }
}

fn top_capture_failure(error: super::TopCaptureError) -> DiagnosticJobError {
    match error {
        super::TopCaptureError::Cancelled => DiagnosticJobError::Cancelled,
        super::TopCaptureError::Limits | super::TopCaptureError::ResultTooLarge => DiagnosticJobError::LimitExceeded,
        _ => DiagnosticJobError::CollectionFailed,
    }
}

fn network_capture_failure(error: NetworkPerformanceError) -> DiagnosticJobError {
    match error {
        NetworkPerformanceError::Cancelled => DiagnosticJobError::Cancelled,
        NetworkPerformanceError::LimitExceeded | NetworkPerformanceError::Busy => DiagnosticJobError::LimitExceeded,
        _ => DiagnosticJobError::CollectionFailed,
    }
}

fn network_export_failure(error: NetworkPerformanceError) -> DiagnosticJobError {
    match error {
        NetworkPerformanceError::Cancelled => DiagnosticJobError::Cancelled,
        NetworkPerformanceError::LimitExceeded => DiagnosticJobError::LimitExceeded,
        _ => DiagnosticJobError::ExportFailed,
    }
}

fn drive_capture_failure(error: DrivePerformanceError) -> DiagnosticJobError {
    match error {
        DrivePerformanceError::Cancelled => DiagnosticJobError::Cancelled,
        DrivePerformanceError::LimitExceeded | DrivePerformanceError::Busy => DiagnosticJobError::LimitExceeded,
        _ => DiagnosticJobError::CollectionFailed,
    }
}

fn drive_export_failure(error: DrivePerformanceError) -> DiagnosticJobError {
    match error {
        DrivePerformanceError::Cancelled => DiagnosticJobError::Cancelled,
        DrivePerformanceError::LimitExceeded => DiagnosticJobError::LimitExceeded,
        _ => DiagnosticJobError::ExportFailed,
    }
}

fn top_export_failure(error: super::TopCaptureError) -> DiagnosticJobError {
    match error {
        super::TopCaptureError::Cancelled => DiagnosticJobError::Cancelled,
        super::TopCaptureError::Limits | super::TopCaptureError::ResultTooLarge => DiagnosticJobError::LimitExceeded,
        _ => DiagnosticJobError::ExportFailed,
    }
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, DiagnosticJobError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| DiagnosticJobError::Invalid)
}

fn uuid7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uid| {
        uid.get_variant() == Variant::RFC4122 && uid.get_version() == Some(Version::SortRand) && uid.to_string() == value
    })
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_lower(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_api::cluster::{Endpoint, EndpointServerPools, Endpoints, PoolEndpoints};
    use ed25519_dalek::{Signer as _, SigningKey};

    fn envelope() -> DiagnosticJobEnvelope {
        DiagnosticJobEnvelope {
            job_id: "018cc251-f400-7abc-8def-0123456789ab".to_owned(),
            protocol_version: "v1".to_owned(),
            job_type: "profile.cpu".to_owned(),
            schema_version: 1,
            organization_name: "organizations/018cc251-f400-7abc-8def-0123456789ab".to_owned(),
            cluster_name: "organizations/018cc251-f400-7abc-8def-0123456789ab/clusters/018cc251-f400-7abc-8def-0123456789ac".to_owned(),
            device_name: "organizations/018cc251-f400-7abc-8def-0123456789ab/clusters/018cc251-f400-7abc-8def-0123456789ac/clusterDevices/018cc251-f400-7abc-8def-0123456789ad".to_owned(),
            create_time: "2030-01-01T00:00:00Z".to_owned(),
            expire_time: "2030-01-01T00:00:30Z".to_owned(),
            nonce: URL_SAFE_NO_PAD.encode_to_string([7_u8; 32]),
            required_capabilities: vec![CPU_PROFILE_CAPABILITY.to_owned()],
            authorization: DiagnosticJobAuthorization {
                actor_type: "BROWSER_USER".to_owned(),
                actor_name: "users/018cc251-f400-7abc-8def-0123456789ab".to_owned(),
                request_id: "123e4567-e89b-42d3-a456-426614174001".to_owned(),
            },
            limits: DiagnosticJobLimits {
                timeout_seconds: 30,
                max_output_bytes: 524_288,
                max_memory_bytes: 64 * 1024 * 1024,
                max_cpu_millis: 30_000,
            },
            parameters: DiagnosticJobParameters {
                artifact_uid: "018cc251-f400-7abc-8def-0123456789ae".to_owned(),
                consent_uid: "018cc251-f400-7abc-8def-0123456789af".to_owned(),
                consent_policy_revision: 1,
                consent_expires_at: "2030-01-01T00:01:00Z".to_owned(),
                duration_millis: 1_000,
                sample_period_micros: 10_000,
                traffic_bytes: None,
                target_alias: None,
                scratch_bytes: None,
                block_bytes: None,
            },
            signature: DiagnosticJobSignature {
                algorithm: "Ed25519".to_owned(),
                key_id: String::new(),
                value: String::new(),
            },
        }
    }

    fn signed_envelope(mut envelope: DiagnosticJobEnvelope) -> (DiagnosticJobEnvelope, TrustedDiagnosticJobSigner) {
        let signing = SigningKey::from_bytes(&[9_u8; 32]);
        let key_id = hex_lower(&Sha256::digest(signing.verifying_key().as_bytes()));
        let trusted =
            TrustedDiagnosticJobSigner::new(key_id.clone(), *signing.verifying_key().as_bytes()).expect("trusted signer");
        envelope.signature.key_id = key_id;
        envelope.signature.value =
            URL_SAFE_NO_PAD.encode_to_string(signing.sign(&envelope.signing_payload().expect("payload")).to_bytes());
        (envelope, trusted)
    }

    fn signed() -> (DiagnosticJobEnvelope, TrustedDiagnosticJobSigner) {
        signed_envelope(envelope())
    }

    fn target(envelope: &DiagnosticJobEnvelope) -> DiagnosticJobTarget {
        DiagnosticJobTarget {
            organization_name: envelope.organization_name.clone(),
            cluster_name: envelope.cluster_name.clone(),
            device_name: envelope.device_name.clone(),
        }
    }

    #[test]
    fn advertised_diagnostic_capabilities_have_execution_paths() {
        use crate::config::Cli;
        use clap::CommandFactory;

        let expected = [
            ("performance.client@1", &["performance", "client"][..]),
            ("performance.drive@1", &["performance", "drive"][..]),
            ("performance.network@1", &[][..]),
            ("performance.object@1", &["performance", "object"][..]),
            ("performance.siteReplication@1", &["performance", "site-replication"][..]),
            ("logs.capture@1", &["logs"][..]),
            ("profile.cpu@1", &["profile"][..]),
            ("profile.memory@1", &["profile"][..]),
            ("profile.threads@1", &["profile"][..]),
            ("telemetry.record@1", &["telemetry", "record"][..]),
            ("telemetry.otlp@1", &["telemetry", "otlp"][..]),
            ("telemetry.replay@1", &["telemetry", "replay"][..]),
            ("top.api@1", &["top", "api"][..]),
            ("top.disk@1", &["top", "disk"][..]),
            ("top.locks@1", &["top", "locks"][..]),
            ("top.net@1", &["top", "net"][..]),
            ("top.rpc@1", &["top", "rpc"][..]),
            ("inspect.object@1", &["inspect", "object"][..]),
        ];
        assert_eq!(
            super::super::CONNECT_DIAGNOSTIC_CAPABILITIES,
            expected.iter().map(|(capability, _)| *capability).collect::<Vec<_>>()
        );

        let command = Cli::command();
        let connect = command.find_subcommand("connect").expect("connect command");
        for (capability, path) in expected {
            if path.is_empty() {
                // Network probes use the authenticated service dispatcher and
                // locally resolved peers, not a standalone CLI command.
                let mut job = envelope();
                job.job_type = PERFORMANCE_NETWORK_JOB_TYPE.to_owned();
                job.required_capabilities = vec![capability.to_owned()];
                job.schema_version = NETWORK_SCHEMA_VERSION;
                assert_eq!(job.kind(), Ok(DiagnosticJobKind::PerformanceNetwork));
                continue;
            }
            let mut command = connect;
            for segment in path {
                command = command
                    .find_subcommand(segment)
                    .unwrap_or_else(|| panic!("{capability} is missing CLI dispatch at {segment}"));
            }
        }
    }

    #[test]
    fn accepts_a_bounded_signed_profile_job_for_the_exact_device() {
        let (envelope, signer) = signed();
        signer
            .verify(&envelope, &target(&envelope), "2030-01-01T00:00:10Z".parse().expect("time"))
            .expect("valid job");
    }

    #[test]
    fn accepts_only_the_thread_profile_capability_pair() {
        let mut threads = envelope();
        threads.job_type = PROFILE_THREADS_JOB_TYPE.to_owned();
        threads.required_capabilities = vec![THREAD_PROFILE_CAPABILITY.to_owned()];
        let (threads, signer) = signed_envelope(threads);
        signer
            .verify(&threads, &target(&threads), "2030-01-01T00:00:10Z".parse().expect("time"))
            .expect("valid profile.threads job");

        let mut mismatched = threads;
        mismatched.required_capabilities = vec![CPU_PROFILE_CAPABILITY.to_owned()];
        assert_eq!(
            signer.verify(&mismatched, &target(&mismatched), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::Unsupported)
        );
    }

    #[tokio::test]
    async fn executes_thread_profile_jobs_against_the_service_process() {
        let now = Utc::now();
        let mut envelope = envelope();
        envelope.job_type = PROFILE_THREADS_JOB_TYPE.to_owned();
        envelope.required_capabilities = vec![THREAD_PROFILE_CAPABILITY.to_owned()];
        envelope.create_time = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        envelope.expire_time = (now + chrono::Duration::seconds(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        envelope.parameters.consent_expires_at =
            (now + chrono::Duration::seconds(60)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let execution = execute_diagnostic_job(
            VerifiedDiagnosticJob {
                envelope,
                nonce: [7_u8; 32],
            },
            &DeviceIdentity::generate(),
            ProfileProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0", vec![]),
            &CancellationToken::new(),
        )
        .await
        .expect("native thread profile job should execute");

        #[cfg(target_os = "linux")]
        {
            assert_eq!(execution.outcome, "SUCCEEDED");
            assert_eq!(execution.reason, "COMPLETE");
            assert!(execution.artifact_bytes.is_some_and(|bytes| !bytes.is_empty()));
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(execution.outcome, "UNSUPPORTED");
            assert_eq!(execution.reason, "UNSUPPORTED_PLATFORM");
            assert!(execution.artifact_bytes.is_none());
        }
    }

    #[test]
    fn accepts_only_the_bounded_top_api_capability_pair() {
        let mut top = envelope();
        top.job_type = TOP_API_JOB_TYPE.to_owned();
        top.required_capabilities = vec![TOP_API_CAPABILITY.to_owned()];
        top.limits.max_cpu_millis = MAX_TOP_API_CPU_MILLIS;
        let (top, signer) = signed_envelope(top);
        signer
            .verify(&top, &target(&top), "2030-01-01T00:00:10Z".parse().expect("time"))
            .expect("valid top.api job");

        let mut mismatched = top.clone();
        mismatched.required_capabilities = vec![CPU_PROFILE_CAPABILITY.to_owned()];
        assert_eq!(
            signer.verify(&mismatched, &target(&mismatched), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::Unsupported)
        );

        let mut unbounded = top;
        unbounded.limits.max_cpu_millis += 1;
        assert_eq!(
            signer.verify(&unbounded, &target(&unbounded), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::LimitExceeded)
        );
    }

    #[test]
    fn accepts_only_a_bounded_network_traffic_budget() {
        let mut network = envelope();
        network.job_type = PERFORMANCE_NETWORK_JOB_TYPE.to_owned();
        network.required_capabilities = vec![NETWORK_CAPABILITY.to_owned()];
        network.limits.max_cpu_millis = MAX_NETWORK_CPU_MILLIS;
        network.parameters.duration_millis = MAX_NETWORK_CPU_MILLIS;
        network.parameters.traffic_bytes = Some(MAX_NETWORK_TRAFFIC_BYTES);
        let (network, signer) = signed_envelope(network);
        signer
            .verify(&network, &target(&network), "2030-01-01T00:00:10Z".parse().expect("time"))
            .expect("valid performance.network job");

        let mut missing = network.clone();
        missing.parameters.traffic_bytes = None;
        assert_eq!(
            signer.verify(&missing, &target(&missing), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::LimitExceeded)
        );

        let mut zero = network.clone();
        zero.parameters.traffic_bytes = Some(0);
        assert_eq!(
            signer.verify(&zero, &target(&zero), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::LimitExceeded)
        );

        let mut unbounded = network.clone();
        unbounded.parameters.traffic_bytes = Some(MAX_NETWORK_TRAFFIC_BYTES + 1);
        assert_eq!(
            signer.verify(&unbounded, &target(&unbounded), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::LimitExceeded)
        );

        let mut profile = signed().0;
        profile.parameters.traffic_bytes = Some(1);
        assert_eq!(
            signer.verify(&profile, &target(&profile), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::Invalid)
        );

        let unsigned_network = serde_json::to_value(network.unsigned()).expect("network envelope");
        let unsigned_profile = serde_json::to_value(envelope().unsigned()).expect("profile envelope");
        assert_eq!(unsigned_network["parameters"]["trafficBytes"], MAX_NETWORK_TRAFFIC_BYTES);
        assert!(unsigned_profile["parameters"].get("trafficBytes").is_none());
    }

    #[test]
    fn accepts_only_the_fixed_drive_target_and_budget_without_a_path() {
        let mut drive = envelope();
        drive.job_type = PERFORMANCE_DRIVE_JOB_TYPE.to_owned();
        drive.required_capabilities = vec![DRIVE_CAPABILITY.to_owned()];
        drive.limits.max_cpu_millis = DRIVE_DURATION_MILLIS;
        drive.parameters.duration_millis = DRIVE_DURATION_MILLIS;
        drive.parameters.target_alias = Some(DRIVE_TARGET_ALIAS.to_owned());
        drive.parameters.scratch_bytes = Some(DRIVE_SCRATCH_BYTES);
        drive.parameters.block_bytes = Some(DRIVE_BLOCK_BYTES);
        let (drive, signer) = signed_envelope(drive);
        signer
            .verify(&drive, &target(&drive), "2030-01-01T00:00:10Z".parse().expect("time"))
            .expect("valid performance.drive job");

        let mut wrong_target = drive.clone();
        wrong_target.parameters.target_alias = Some("../../customer-data".to_owned());
        assert_eq!(
            signer.verify(&wrong_target, &target(&wrong_target), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::Invalid)
        );

        let mut unbounded = drive.clone();
        unbounded.parameters.scratch_bytes = Some(DRIVE_SCRATCH_BYTES + 1);
        assert_eq!(
            signer.verify(&unbounded, &target(&unbounded), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::LimitExceeded)
        );

        let unsigned = serde_json::to_value(drive.unsigned()).expect("drive envelope");
        assert_eq!(unsigned["parameters"]["targetAlias"], DRIVE_TARGET_ALIAS);
        assert_eq!(unsigned["parameters"]["scratchBytes"], DRIVE_SCRATCH_BYTES);
        assert_eq!(unsigned["parameters"]["blockBytes"], DRIVE_BLOCK_BYTES);
        assert!(unsigned["parameters"].get("scratchRoot").is_none());

        let mut with_path = serde_json::to_value(&drive).expect("drive envelope");
        with_path["parameters"]["scratchRoot"] = serde_json::Value::String("/customer/data".to_owned());
        assert!(serde_json::from_value::<DiagnosticJobEnvelope>(with_path).is_err());
    }

    #[tokio::test]
    async fn drive_adapter_exports_only_successful_measurements() {
        let now = Utc::now();
        let mut drive = envelope();
        drive.job_type = PERFORMANCE_DRIVE_JOB_TYPE.to_owned();
        drive.required_capabilities = vec![DRIVE_CAPABILITY.to_owned()];
        drive.create_time = now.to_rfc3339();
        drive.expire_time = (now + chrono::Duration::seconds(30)).to_rfc3339();
        drive.parameters.consent_expires_at = (now + chrono::Duration::seconds(60)).to_rfc3339();
        drive.limits.max_cpu_millis = DRIVE_DURATION_MILLIS;
        drive.parameters.duration_millis = DRIVE_DURATION_MILLIS;
        drive.parameters.target_alias = Some(DRIVE_TARGET_ALIAS.to_owned());
        drive.parameters.scratch_bytes = Some(DRIVE_SCRATCH_BYTES);
        drive.parameters.block_bytes = Some(DRIVE_BLOCK_BYTES);
        let (drive, signer) = signed_envelope(drive);
        let verified = signer.verify(&drive, &target(&drive), now).expect("valid drive job");
        let provenance = ProfileProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0", Vec::new());
        let data_drive = tempfile::tempdir().expect("data drive");
        let scratch = data_drive.path().join(DRIVE_SCRATCH_DIRECTORY);
        assert!(ensure_drive_scratch_root(&scratch));
        let result = execute_performance_drive_job(
            verified.envelope,
            verified.nonce,
            &DeviceIdentity::generate(),
            provenance.clone(),
            &scratch,
            &CancellationToken::new(),
        )
        .await
        .expect("drive execution");
        assert_eq!(result.outcome, "SUCCEEDED");
        assert!(result.artifact_bytes.is_some());
        assert_eq!(scratch.read_dir().expect("scratch contents").count(), 0);

        let verified = signer.verify(&drive, &target(&drive), now).expect("valid drive job");
        let missing_root = data_drive.path().join("not-configured");
        let result = execute_performance_drive_job(
            verified.envelope,
            verified.nonce,
            &DeviceIdentity::generate(),
            provenance,
            &missing_root,
            &CancellationToken::new(),
        )
        .await
        .expect("terminal drive execution");
        assert_eq!(result.outcome, "FAILED");
        assert_eq!(result.reason, "SOURCE_UNAVAILABLE");
        assert!(result.artifact_uid.is_none());
        assert!(result.artifact_sha256.is_none());
        assert!(result.artifact_bytes.is_none());
    }

    #[test]
    fn drive_alias_resolves_to_the_first_local_storage_endpoint() {
        let local_drive = tempfile::tempdir().expect("local drive");
        let mut remote = Endpoint::try_from("http://node-b.example:9000/remote-drive").expect("remote endpoint");
        remote.set_pool_index(0);
        remote.set_set_index(0);
        remote.set_disk_index(0);
        let mut local = Endpoint::try_from(local_drive.path().to_str().expect("local path")).expect("local endpoint");
        local.set_pool_index(0);
        local.set_set_index(0);
        local.set_disk_index(1);
        let pools = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 2,
            endpoints: Endpoints::from(vec![remote, local]),
            cmd_line: "test storage endpoints".to_owned(),
            platform: "test".to_owned(),
        }]);

        assert_eq!(
            drive_scratch_root_from_endpoints(&pools),
            Some(local_drive.path().join(DRIVE_SCRATCH_DIRECTORY))
        );
    }

    #[test]
    fn accepts_only_the_bounded_top_locks_capability_pair() {
        let mut top = envelope();
        top.job_type = TOP_LOCKS_JOB_TYPE.to_owned();
        top.required_capabilities = vec![TOP_LOCKS_CAPABILITY.to_owned()];
        top.limits.max_cpu_millis = MAX_TOP_LOCKS_CPU_MILLIS;
        let (top, signer) = signed_envelope(top);
        signer
            .verify(&top, &target(&top), "2030-01-01T00:00:10Z".parse().expect("time"))
            .expect("valid top.locks job");

        let mut mismatched = top.clone();
        mismatched.required_capabilities = vec![TOP_API_CAPABILITY.to_owned()];
        assert_eq!(
            signer.verify(&mismatched, &target(&mismatched), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::Unsupported)
        );

        let mut unbounded = top;
        unbounded.limits.max_cpu_millis += 1;
        assert_eq!(
            signer.verify(&unbounded, &target(&unbounded), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::LimitExceeded)
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn top_locks_job_preserves_the_signed_job_nonce() {
        rustfs_lock::get_global_lock_manager();
        let now = Utc::now();
        let mut top = envelope();
        top.job_type = TOP_LOCKS_JOB_TYPE.to_owned();
        top.required_capabilities = vec![TOP_LOCKS_CAPABILITY.to_owned()];
        top.limits.max_cpu_millis = MAX_TOP_LOCKS_CPU_MILLIS;
        top.parameters.duration_millis = 1;
        top.create_time = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        top.expire_time = (now + chrono::Duration::seconds(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        top.parameters.consent_expires_at =
            (now + chrono::Duration::seconds(60)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let nonce = [7_u8; 32];
        let execution = execute_diagnostic_job(
            VerifiedDiagnosticJob { envelope: top, nonce },
            &DeviceIdentity::generate(),
            ProfileProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0", vec![]),
            &CancellationToken::new(),
        )
        .await
        .expect("top.locks job should execute");

        let bytes = execution.artifact_bytes.expect("top.locks artifact");
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("top.locks archive");
        let mut envelope = String::new();
        std::io::Read::read_to_string(&mut archive.by_name("envelope.json").expect("top.locks envelope"), &mut envelope)
            .expect("read top.locks envelope");
        let envelope: serde_json::Value = serde_json::from_str(&envelope).expect("valid top.locks envelope");
        assert_eq!(envelope["nonce"], URL_SAFE_NO_PAD.encode_to_string(nonce));
    }

    #[test]
    fn accepts_only_the_bounded_top_rpc_capability_pair() {
        let mut top = envelope();
        top.job_type = TOP_RPC_JOB_TYPE.to_owned();
        top.required_capabilities = vec![TOP_RPC_CAPABILITY.to_owned()];
        top.limits.max_cpu_millis = MAX_TOP_RPC_CPU_MILLIS;
        let (top, signer) = signed_envelope(top);
        signer
            .verify(&top, &target(&top), "2030-01-01T00:00:10Z".parse().expect("time"))
            .expect("valid top.rpc job");

        let mut mismatched = top.clone();
        mismatched.required_capabilities = vec![TOP_LOCKS_CAPABILITY.to_owned()];
        assert_eq!(
            signer.verify(&mismatched, &target(&mismatched), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::Unsupported)
        );

        let mut unbounded = top;
        unbounded.limits.max_cpu_millis += 1;
        assert_eq!(
            signer.verify(&unbounded, &target(&unbounded), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::LimitExceeded)
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn top_rpc_job_captures_service_process_events_and_exports_a_signed_artifact() {
        use rustfs_common::trace_bus::{
            TelemetryTraceEvent, TelemetryTraceOperation, TelemetryTraceStatus, telemetry_trace_emit,
        };

        let mut top = envelope();
        top.job_type = TOP_RPC_JOB_TYPE.to_owned();
        top.required_capabilities = vec![TOP_RPC_CAPABILITY.to_owned()];
        top.limits.max_cpu_millis = MAX_TOP_RPC_CPU_MILLIS;
        top.parameters.duration_millis = 50;

        let emit = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert!(telemetry_trace_emit(|| {
                TelemetryTraceEvent::new(
                    TelemetryTraceOperation::InternalRpc,
                    Duration::from_micros(37),
                    TelemetryTraceStatus::Ok,
                )
            }));
        };
        let identity = DeviceIdentity::generate();
        let cancellation = CancellationToken::new();
        let execute = execute_diagnostic_job(
            VerifiedDiagnosticJob {
                envelope: top,
                nonce: [7_u8; 32],
            },
            &identity,
            ProfileProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0", vec![]),
            &cancellation,
        );
        let (execution, ()) = tokio::join!(execute, emit);
        let execution = execution.expect("top.rpc job should execute");

        assert_eq!(execution.outcome, "SUCCEEDED");
        assert_eq!(execution.reason, "COMPLETE");
        assert!(execution.artifact_bytes.is_some_and(|bytes| !bytes.is_empty()));
    }

    #[test]
    fn rejects_tampering_cross_device_replay_and_expiry() {
        let (envelope, signer) = signed();
        let mut tampered = envelope.clone();
        tampered.parameters.duration_millis = 2_000;
        assert_eq!(
            signer.verify(&tampered, &target(&tampered), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::SignatureInvalid)
        );
        let mut wrong_target = target(&envelope);
        wrong_target.device_name.push('0');
        assert_eq!(
            signer.verify(&envelope, &wrong_target, "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::TargetMismatch)
        );
        assert_eq!(
            signer.verify(&envelope, &target(&envelope), "2030-01-01T00:00:30Z".parse().expect("time")),
            Err(DiagnosticJobError::Expired)
        );
        let (mut actor_tampered, signer) = signed();
        actor_tampered.authorization.actor_name.push('0');
        assert_eq!(
            signer.verify(&actor_tampered, &target(&actor_tampered), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::Invalid)
        );
    }

    #[test]
    fn rejects_unbounded_and_non_allow_listed_jobs_before_signature_use() {
        let (mut envelope, signer) = signed();
        envelope.limits.max_output_bytes += 1;
        assert_eq!(
            signer.verify(&envelope, &target(&envelope), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::LimitExceeded)
        );
        envelope = signed().0;
        envelope.job_type = "shell.exec".to_owned();
        assert_eq!(
            signer.verify(&envelope, &target(&envelope), "2030-01-01T00:00:10Z".parse().expect("time")),
            Err(DiagnosticJobError::Unsupported)
        );
    }

    #[test]
    fn exposes_only_allow_listed_profile_failure_reasons() {
        assert_eq!(
            capture_failure(super::super::ProfileError::SourceUnavailable),
            DiagnosticJobError::ProfileSourceUnavailable
        );
        assert_eq!(capture_failure(super::super::ProfileError::TimedOut), DiagnosticJobError::LimitExceeded);
        assert_eq!(capture_failure(super::super::ProfileError::Cancelled), DiagnosticJobError::Cancelled);
        assert_eq!(export_failure(super::super::ProfileError::Encoding), DiagnosticJobError::ExportFailed);
        assert_eq!(DiagnosticJobError::ProfileSourceUnavailable.reason(), "PROFILE_SOURCE_UNAVAILABLE");
        assert_eq!(DiagnosticJobError::ExportFailed.reason(), "EXPORT_FAILED");
    }
}
