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
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

use base64_simd::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier as _, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::{Uuid, Variant, Version};

use super::{
    CPU_PROFILE_CAPABILITY, LocalProfileConsent, PROFILE_SCHEMA_VERSION, ProfileCaptureRequest, ProfileProvenance,
    capture_cpu_profile, encode_signed_profile_export,
};
use crate::connect::DeviceIdentity;

const PROTOCOL_VERSION: &str = "v1";
const JOB_TYPE: &str = "profile.cpu";
pub const DIAGNOSTIC_JOB_SIGNATURE_DOMAIN: &[u8] = b"rustfs-connect-agent-job-v1\0";
const MAX_JOB_LIFETIME_SECONDS: i64 = 1_800;
const MAX_FUTURE_SKEW_SECONDS: i64 = 300;
const MAX_OUTPUT_BYTES: u64 = 524_288;
const MAX_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CPU_MILLIS: u64 = 30_000;

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
        if self.protocol_version != PROTOCOL_VERSION
            || self.job_type != JOB_TYPE
            || self.schema_version != PROFILE_SCHEMA_VERSION
            || self.required_capabilities != [CPU_PROFILE_CAPABILITY]
        {
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
        Ok(())
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
    let envelope = job.envelope;
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
        nonce: job.nonce,
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
            },
            signature: DiagnosticJobSignature {
                algorithm: "Ed25519".to_owned(),
                key_id: String::new(),
                value: String::new(),
            },
        }
    }

    fn signed() -> (DiagnosticJobEnvelope, TrustedDiagnosticJobSigner) {
        let signing = SigningKey::from_bytes(&[9_u8; 32]);
        let key_id = hex_lower(&Sha256::digest(signing.verifying_key().as_bytes()));
        let trusted =
            TrustedDiagnosticJobSigner::new(key_id.clone(), *signing.verifying_key().as_bytes()).expect("trusted signer");
        let mut envelope = envelope();
        envelope.signature.key_id = key_id;
        envelope.signature.value =
            URL_SAFE_NO_PAD.encode_to_string(signing.sign(&envelope.signing_payload().expect("payload")).to_bytes());
        (envelope, trusted)
    }

    fn target(envelope: &DiagnosticJobEnvelope) -> DiagnosticJobTarget {
        DiagnosticJobTarget {
            organization_name: envelope.organization_name.clone(),
            cluster_name: envelope.cluster_name.clone(),
            device_name: envelope.device_name.clone(),
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
