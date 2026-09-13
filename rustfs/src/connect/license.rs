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

//! Verification and local persistence for RustFS Connect service licenses.
//!
//! Service licenses are separate from Connect device credentials. This module
//! has no signing capability and does not affect S3 availability.

use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const LICENSE_PURPOSE: &str = "RUSTFS_CONNECT_SERVICE_LICENSE";
pub const LICENSE_SCHEMA: &str = "rustfs.connect.serviceLicense/1";
pub const LICENSE_ALGORITHM: &str = "Ed25519";
pub const LICENSE_DOMAIN_SEPARATION_TAG: &str = "rustfs-connect-service-license-v1";

const INSTALLED_SCHEMA: &str = "rustfs.connect.installedServiceLicense/1";
const MAX_ARTIFACT_BYTES: u64 = 64 * 1024;
const MAX_STATE_BYTES: u64 = MAX_ARTIFACT_BYTES + 1024;
const MAX_PAYLOAD_BYTES: usize = 32 * 1024;
const MAX_PUBLIC_KEY_BYTES: u64 = 256;
const LOCK_FILE: &str = ".service-license.lock";

#[cfg(unix)]
const STATE_FILE_MODE: u32 = 0o600;

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LicenseArtifactStatus {
    Valid,
    Missing,
    InvalidConfiguration,
    InvalidArtifact,
    Unsupported,
    UntrustedKey,
    InvalidSignature,
    WrongScope,
    NotYetValid,
    Expired,
    Rollback,
    SequenceConflict,
    SupersessionRequired,
    StateUnavailable,
}

impl LicenseArtifactStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "VALID",
            Self::Missing => "MISSING",
            Self::InvalidConfiguration => "INVALID_CONFIGURATION",
            Self::InvalidArtifact => "INVALID_ARTIFACT",
            Self::Unsupported => "UNSUPPORTED",
            Self::UntrustedKey => "UNTRUSTED_KEY",
            Self::InvalidSignature => "INVALID_SIGNATURE",
            Self::WrongScope => "WRONG_SCOPE",
            Self::NotYetValid => "NOT_YET_VALID",
            Self::Expired => "EXPIRED",
            Self::Rollback => "ROLLBACK",
            Self::SequenceConflict => "SEQUENCE_CONFLICT",
            Self::SupersessionRequired => "SUPERSESSION_REQUIRED",
            Self::StateUnavailable => "STATE_UNAVAILABLE",
        }
    }
}

impl std::fmt::Display for LicenseArtifactStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct LicenseClaims {
    pub purpose: String,
    pub schema: String,
    pub algorithm: String,
    pub key_id: String,
    pub license_uid: String,
    pub grant_uid: String,
    pub sequence: u64,
    pub issuer: String,
    pub audience: String,
    pub organization: String,
    pub deployment: String,
    pub plan_code: String,
    pub policy_revision: String,
    pub service_code: String,
    pub issue_time: String,
    pub not_before: String,
    pub expire_time: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LicenseReport {
    pub status: LicenseArtifactStatus,
    pub installed: bool,
    pub idempotent: bool,
    pub license: Option<LicenseClaims>,
    pub message: Option<String>,
}

impl LicenseReport {
    pub const fn is_valid(&self) -> bool {
        matches!(self.status, LicenseArtifactStatus::Valid)
    }

    fn valid(license: LicenseClaims, installed: bool, idempotent: bool) -> Self {
        Self {
            status: LicenseArtifactStatus::Valid,
            installed,
            idempotent,
            license: Some(license),
            message: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{status}: {message}")]
pub struct LicenseArtifactError {
    pub status: LicenseArtifactStatus,
    pub message: String,
    pub license: Option<Box<LicenseClaims>>,
}

impl LicenseArtifactError {
    pub fn report(self, installed: bool) -> LicenseReport {
        LicenseReport {
            status: self.status,
            installed,
            idempotent: false,
            license: self.license.map(|license| *license),
            message: Some(self.message),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LicenseVerificationContext {
    verifying_key: VerifyingKey,
    pub key_id: String,
    pub issuer: String,
    pub audience: String,
    pub organization: String,
    pub deployment: String,
    pub service_code: String,
    pub now_unix: i64,
}

impl LicenseVerificationContext {
    #[allow(clippy::too_many_arguments)]
    pub fn from_public_key_file(
        public_key_file: &Path,
        key_id: String,
        issuer: String,
        audience: String,
        organization: String,
        deployment: String,
        service_code: String,
    ) -> Result<Self, LicenseArtifactError> {
        let encoded = read_bounded_regular_file(
            public_key_file,
            MAX_PUBLIC_KEY_BYTES,
            LicenseArtifactStatus::InvalidConfiguration,
            "public key",
        )?;
        let encoded = std::str::from_utf8(&encoded)
            .map_err(|_| failure(LicenseArtifactStatus::InvalidConfiguration, "the trusted public key is not UTF-8"))?
            .trim();
        let public_key = decode_canonical_base64url(encoded, 32, LicenseArtifactStatus::InvalidConfiguration, "public key")?;
        let public_key: [u8; 32] = public_key.try_into().map_err(|_| {
            failure(
                LicenseArtifactStatus::InvalidConfiguration,
                "the trusted public key must contain 32 bytes",
            )
        })?;
        let now_unix = system_now()?;
        Self::new(public_key, key_id, issuer, audience, organization, deployment, service_code, now_unix)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        public_key: [u8; 32],
        key_id: String,
        issuer: String,
        audience: String,
        organization: String,
        deployment: String,
        service_code: String,
        now_unix: i64,
    ) -> Result<Self, LicenseArtifactError> {
        let verifying_key = VerifyingKey::from_bytes(&public_key)
            .map_err(|_| failure(LicenseArtifactStatus::InvalidConfiguration, "the trusted Ed25519 public key is invalid"))?;
        let actual_key_id = hex_simd::encode_to_string(Sha256::digest(public_key), hex_simd::AsciiCase::Lower);
        if !is_lower_hex_sha256(&key_id) || key_id != actual_key_id {
            return Err(failure(
                LicenseArtifactStatus::InvalidConfiguration,
                "the trusted key ID does not match the public key",
            ));
        }
        if !is_bounded_label(&issuer, 128) || !is_bounded_label(&audience, 128) {
            return Err(failure(
                LicenseArtifactStatus::InvalidConfiguration,
                "issuer and audience must be bounded identifiers",
            ));
        }
        validate_resource_scope(&organization, &deployment).map_err(|message| {
            failure(
                LicenseArtifactStatus::InvalidConfiguration,
                format!("the local license scope is invalid: {message}"),
            )
        })?;
        if !is_service_code(&service_code) {
            return Err(failure(LicenseArtifactStatus::InvalidConfiguration, "the local service code is invalid"));
        }

        Ok(Self {
            verifying_key,
            key_id,
            issuer,
            audience,
            organization,
            deployment,
            service_code,
            now_unix,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LicenseArtifact {
    payload: String,
    signature: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct InstalledLicense {
    schema: String,
    artifact: LicenseArtifact,
}

struct ValidatedLicense {
    artifact: LicenseArtifact,
    claims: LicenseClaims,
}

pub fn apply_license_artifact(
    artifact_path: &Path,
    state_directory: &Path,
    context: &LicenseVerificationContext,
) -> Result<LicenseReport, LicenseArtifactError> {
    fs::create_dir_all(state_directory).map_err(|source| state_io(state_directory, source))?;
    let _lock = lock_state(state_directory)?;
    let candidate = validate_artifact(read_artifact(artifact_path)?, context, true)?;
    let state_path = state_path(state_directory, context);

    if let Some(current) = load_installed(&state_path, context)? {
        match compare_sequence(&candidate, &current)? {
            SequenceDecision::Idempotent => return Ok(LicenseReport::valid(candidate.claims, true, true)),
            SequenceDecision::Replace => {}
        }
    }

    persist_installed(&state_path, &candidate.artifact)?;
    Ok(LicenseReport::valid(candidate.claims, true, false))
}

pub fn verify_license_artifact(
    artifact_path: &Path,
    state_directory: &Path,
    context: &LicenseVerificationContext,
) -> Result<LicenseReport, LicenseArtifactError> {
    let candidate = validate_artifact(read_artifact(artifact_path)?, context, true)?;
    let state_path = state_path(state_directory, context);
    if let Some(current) = load_installed(&state_path, context)?
        && matches!(compare_sequence(&candidate, &current)?, SequenceDecision::Idempotent)
    {
        return Ok(LicenseReport::valid(candidate.claims, true, true));
    }
    Ok(LicenseReport::valid(candidate.claims, false, false))
}

pub fn inspect_installed_license(
    state_directory: &Path,
    context: &LicenseVerificationContext,
) -> Result<LicenseReport, LicenseArtifactError> {
    let state_path = state_path(state_directory, context);
    let artifact = read_installed_artifact(&state_path)?.ok_or_else(|| {
        failure(
            LicenseArtifactStatus::Missing,
            "no service license is installed for the requested deployment and service",
        )
    })?;
    let validated = validate_artifact(artifact, context, true).map_err(|mut error| {
        if !matches!(error.status, LicenseArtifactStatus::Expired | LicenseArtifactStatus::NotYetValid) {
            error.status = LicenseArtifactStatus::StateUnavailable;
            error.message = "the installed service license could not be verified".to_owned();
            error.license = None;
        }
        error
    })?;
    Ok(LicenseReport::valid(validated.claims, true, false))
}

enum SequenceDecision {
    Idempotent,
    Replace,
}

fn compare_sequence(candidate: &ValidatedLicense, current: &ValidatedLicense) -> Result<SequenceDecision, LicenseArtifactError> {
    if candidate.claims.grant_uid != current.claims.grant_uid {
        return Err(failure_with_license(
            LicenseArtifactStatus::SupersessionRequired,
            "a different grant cannot replace the installed grant without a signed supersession claim",
            candidate.claims.clone(),
        ));
    }
    match candidate.claims.sequence.cmp(&current.claims.sequence) {
        std::cmp::Ordering::Less => Err(failure_with_license(
            LicenseArtifactStatus::Rollback,
            "the license sequence is older than the installed sequence",
            candidate.claims.clone(),
        )),
        std::cmp::Ordering::Equal if candidate.artifact == current.artifact => Ok(SequenceDecision::Idempotent),
        std::cmp::Ordering::Equal => Err(failure_with_license(
            LicenseArtifactStatus::SequenceConflict,
            "different license bytes use the installed sequence",
            candidate.claims.clone(),
        )),
        std::cmp::Ordering::Greater => Ok(SequenceDecision::Replace),
    }
}

fn validate_artifact(
    artifact: LicenseArtifact,
    context: &LicenseVerificationContext,
    check_time: bool,
) -> Result<ValidatedLicense, LicenseArtifactError> {
    let payload =
        decode_canonical_base64url(&artifact.payload, MAX_PAYLOAD_BYTES, LicenseArtifactStatus::InvalidArtifact, "payload")?;
    let signature = decode_canonical_base64url(&artifact.signature, 64, LicenseArtifactStatus::InvalidArtifact, "signature")?;
    let claims: LicenseClaims = serde_json::from_slice(&payload)
        .map_err(|_| failure(LicenseArtifactStatus::InvalidArtifact, "the license payload is invalid"))?;
    if !matches!(serde_json::to_vec(&claims), Ok(canonical) if canonical == payload) {
        return Err(failure(
            LicenseArtifactStatus::InvalidArtifact,
            "the license payload is not canonical JSON",
        ));
    }
    validate_claim_shape(&claims)?;
    if claims.purpose != LICENSE_PURPOSE || claims.schema != LICENSE_SCHEMA || claims.algorithm != LICENSE_ALGORITHM {
        return Err(failure(
            LicenseArtifactStatus::Unsupported,
            "the license purpose, schema, or algorithm is unsupported",
        ));
    }
    if claims.key_id != context.key_id {
        return Err(failure(LicenseArtifactStatus::UntrustedKey, "the license key ID is not trusted"));
    }

    let signature = Signature::from_slice(&signature)
        .map_err(|_| failure(LicenseArtifactStatus::InvalidArtifact, "the license signature has an invalid length"))?;
    let mut signed = Vec::with_capacity(LICENSE_DOMAIN_SEPARATION_TAG.len() + 1 + payload.len());
    signed.extend_from_slice(LICENSE_DOMAIN_SEPARATION_TAG.as_bytes());
    signed.push(0);
    signed.extend_from_slice(&payload);
    context
        .verifying_key
        .verify_strict(&signed, &signature)
        .map_err(|_| failure(LicenseArtifactStatus::InvalidSignature, "the license signature is invalid"))?;

    if claims.issuer != context.issuer
        || claims.audience != context.audience
        || claims.organization != context.organization
        || claims.deployment != context.deployment
        || claims.service_code != context.service_code
    {
        return Err(failure(
            LicenseArtifactStatus::WrongScope,
            "the license is not valid for the requested scope",
        ));
    }

    if check_time {
        let not_before = parse_timestamp(&claims.not_before)?;
        let expire_time = parse_timestamp(&claims.expire_time)?;
        if context.now_unix < not_before {
            return Err(failure_with_license(
                LicenseArtifactStatus::NotYetValid,
                "the license is not yet valid",
                claims,
            ));
        }
        if context.now_unix >= expire_time {
            return Err(failure_with_license(LicenseArtifactStatus::Expired, "the license has expired", claims));
        }
    }

    Ok(ValidatedLicense { artifact, claims })
}

fn validate_claim_shape(claims: &LicenseClaims) -> Result<(), LicenseArtifactError> {
    if !is_lower_hex_sha256(&claims.key_id)
        || !is_canonical_uuid_v7(&claims.license_uid)
        || !is_canonical_uuid_v7(&claims.grant_uid)
        || claims.sequence == 0
        || !is_bounded_label(&claims.issuer, 128)
        || !is_bounded_label(&claims.audience, 128)
        || !is_service_code(&claims.plan_code)
        || !is_bounded_label(&claims.policy_revision, 128)
        || !is_service_code(&claims.service_code)
    {
        return Err(failure(
            LicenseArtifactStatus::InvalidArtifact,
            "the license claims contain an invalid identifier",
        ));
    }
    validate_resource_scope(&claims.organization, &claims.deployment)
        .map_err(|_| failure(LicenseArtifactStatus::InvalidArtifact, "the license resource scope is invalid"))?;
    let issue_time = parse_timestamp(&claims.issue_time)?;
    let not_before = parse_timestamp(&claims.not_before)?;
    let expire_time = parse_timestamp(&claims.expire_time)?;
    if issue_time > not_before || issue_time >= expire_time || not_before >= expire_time {
        return Err(failure(LicenseArtifactStatus::InvalidArtifact, "the license time interval is invalid"));
    }
    Ok(())
}

fn parse_timestamp(value: &str) -> Result<i64, LicenseArtifactError> {
    if value.len() != 20 || !value.ends_with('Z') {
        return Err(failure(
            LicenseArtifactStatus::InvalidArtifact,
            "license timestamps must use whole-second UTC RFC 3339",
        ));
    }
    OffsetDateTime::parse(value, &Rfc3339)
        .map(|time| time.unix_timestamp())
        .map_err(|_| failure(LicenseArtifactStatus::InvalidArtifact, "the license timestamp is invalid"))
}

fn read_artifact(path: &Path) -> Result<LicenseArtifact, LicenseArtifactError> {
    let bytes = read_bounded_regular_file(path, MAX_ARTIFACT_BYTES, LicenseArtifactStatus::InvalidArtifact, "license artifact")?;
    serde_json::from_slice(&bytes).map_err(|_| failure(LicenseArtifactStatus::InvalidArtifact, "the license artifact is invalid"))
}

fn read_bounded_regular_file(
    path: &Path,
    maximum: u64,
    status: LicenseArtifactStatus,
    label: &str,
) -> Result<Vec<u8>, LicenseArtifactError> {
    let metadata = fs::metadata(path).map_err(|source| failure(status, format!("the {label} could not be read: {source}")))?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(failure(
            status,
            format!("the {label} must be a regular file no larger than {maximum} bytes"),
        ));
    }
    fs::read(path).map_err(|source| failure(status, format!("the {label} could not be read: {source}")))
}

fn load_installed(path: &Path, context: &LicenseVerificationContext) -> Result<Option<ValidatedLicense>, LicenseArtifactError> {
    let Some(artifact) = read_installed_artifact(path)? else {
        return Ok(None);
    };
    validate_artifact(artifact, context, false).map(Some).map_err(|_| {
        failure(
            LicenseArtifactStatus::StateUnavailable,
            "the installed service license could not be verified and was left untouched",
        )
    })
}

fn read_installed_artifact(path: &Path) -> Result<Option<LicenseArtifact>, LicenseArtifactError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(state_io(path, source)),
    };
    check_state_mode(path)?;
    if bytes.len() as u64 > MAX_STATE_BYTES {
        return Err(failure(
            LicenseArtifactStatus::StateUnavailable,
            "the installed service license exceeds the size limit",
        ));
    }
    let installed: InstalledLicense = serde_json::from_slice(&bytes)
        .map_err(|_| failure(LicenseArtifactStatus::StateUnavailable, "the installed service license state is invalid"))?;
    if installed.schema != INSTALLED_SCHEMA {
        return Err(failure(
            LicenseArtifactStatus::StateUnavailable,
            "the installed service license state schema is unsupported",
        ));
    }
    Ok(Some(installed.artifact))
}

fn persist_installed(path: &Path, artifact: &LicenseArtifact) -> Result<(), LicenseArtifactError> {
    let parent = path
        .parent()
        .ok_or_else(|| failure(LicenseArtifactStatus::StateUnavailable, "the license state path has no parent directory"))?;
    let bytes = serde_json::to_vec(&InstalledLicense {
        schema: INSTALLED_SCHEMA.to_owned(),
        artifact: artifact.clone(),
    })
    .map_err(|_| failure(LicenseArtifactStatus::StateUnavailable, "the license state could not be encoded"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("service-license.json");
    let (temporary, mut file) = loop {
        let temporary = parent.join(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(STATE_FILE_MODE);
        }
        match options.open(&temporary) {
            Ok(file) => break (temporary, file),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(state_io(&temporary, source)),
        }
    };
    let result = (|| -> io::Result<()> {
        file.write_all(&bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(fs::Permissions::from_mode(STATE_FILE_MODE))?;
        }
        file.sync_all()
    })();
    drop(file);
    if let Err(source) = result {
        let _ = fs::remove_file(&temporary);
        return Err(state_io(&temporary, source));
    }
    if let Err(source) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(state_io(path, source));
    }
    sync_directory(parent).map_err(|source| state_io(parent, source))?;
    Ok(())
}

fn lock_state(directory: &Path) -> Result<fs::File, LicenseArtifactError> {
    let path = directory.join(LOCK_FILE);
    let mut options = fs::OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(STATE_FILE_MODE);
    }
    let file = options.open(&path).map_err(|source| state_io(&path, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(fs::Permissions::from_mode(STATE_FILE_MODE))
            .map_err(|source| state_io(&path, source))?;
    }
    file.lock().map_err(|source| state_io(&path, source))?;
    Ok(file)
}

fn state_path(directory: &Path, context: &LicenseVerificationContext) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(context.organization.as_bytes());
    hasher.update([0]);
    hasher.update(context.deployment.as_bytes());
    hasher.update([0]);
    hasher.update(context.service_code.as_bytes());
    let scope = hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower);
    directory.join(format!("service-license-{scope}.json"))
}

fn validate_resource_scope(organization: &str, deployment: &str) -> Result<(), &'static str> {
    let organization_uid = organization
        .strip_prefix("organizations/")
        .ok_or("organization resource name is invalid")?;
    if !is_canonical_uuid_v7(organization_uid) {
        return Err("organization UID is invalid");
    }
    let cluster_uid = deployment
        .strip_prefix(organization)
        .and_then(|suffix| suffix.strip_prefix("/clusters/"))
        .ok_or("deployment resource name is invalid")?;
    if !is_canonical_uuid_v7(cluster_uid) {
        return Err("deployment UID is invalid");
    }
    Ok(())
}

fn is_canonical_uuid_v7(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|parsed| parsed.get_version_num() == 7 && parsed.to_string() == value)
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_bounded_label(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn is_service_code(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z'))
        && value.len() <= 64
        && bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn decode_canonical_base64url(
    value: &str,
    maximum: usize,
    status: LicenseArtifactStatus,
    label: &str,
) -> Result<Vec<u8>, LicenseArtifactError> {
    if value.is_empty()
        || value.len() > maximum.saturating_mul(2)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(failure(status, format!("the {label} is not canonical base64url")));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode_to_vec(value.as_bytes())
        .map_err(|_| failure(status, format!("the {label} is not canonical base64url")))?;
    if decoded.len() > maximum || URL_SAFE_NO_PAD.encode_to_string(&decoded) != value {
        return Err(failure(status, format!("the {label} is not canonical base64url")));
    }
    Ok(decoded)
}

fn system_now() -> Result<i64, LicenseArtifactError> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| failure(LicenseArtifactStatus::InvalidConfiguration, "the system clock is before the Unix epoch"))?
        .as_secs();
    i64::try_from(seconds).map_err(|_| {
        failure(
            LicenseArtifactStatus::InvalidConfiguration,
            "the system clock is outside the supported range",
        )
    })
}

fn state_io(path: &Path, source: io::Error) -> LicenseArtifactError {
    failure(
        LicenseArtifactStatus::StateUnavailable,
        format!("license I/O failed at {}: {source}", path.display()),
    )
}

fn failure(status: LicenseArtifactStatus, message: impl Into<String>) -> LicenseArtifactError {
    LicenseArtifactError {
        status,
        message: message.into(),
        license: None,
    }
}

fn failure_with_license(
    status: LicenseArtifactStatus,
    message: impl Into<String>,
    license: LicenseClaims,
) -> LicenseArtifactError {
    LicenseArtifactError {
        status,
        message: message.into(),
        license: Some(Box::new(license)),
    }
}

#[cfg(unix)]
fn check_state_mode(path: &Path) -> Result<(), LicenseArtifactError> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = fs::metadata(path)
        .map_err(|source| state_io(path, source))?
        .permissions()
        .mode()
        & 0o7777;
    if mode != STATE_FILE_MODE {
        return Err(failure(
            LicenseArtifactStatus::StateUnavailable,
            format!("the installed service license has mode {mode:o}, expected {STATE_FILE_MODE:o}"),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_state_mode(_path: &Path) -> Result<(), LicenseArtifactError> {
    Ok(())
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}
