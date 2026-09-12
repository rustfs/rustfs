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

//! Bounded profile result and signed local export primitives.
//!
//! RustFS currently has no reviewed local CPU symbol catalogue or in-process
//! sampler. [`capture_cpu_profile`] therefore returns an explicit, typed
//! unsupported result. It never substitutes Pyroscope delivery state, raw
//! symbol strings, or zero samples for a local CPU measurement.

use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

pub const PROFILE_SCHEMA_VERSION: u16 = 1;
pub const CPU_PROFILE_CAPABILITY: &str = "profile.cpu@1";
pub const MEMORY_PROFILE_CAPABILITY: &str = "profile.memory@1";
pub const THREAD_PROFILE_CAPABILITY: &str = "profile.threads@1";
pub const MAX_PROFILE_DURATION: Duration = Duration::from_secs(30);
pub const MAX_RESULT_BYTES: usize = 262_144;
pub const MAX_ENVELOPE_BYTES: usize = 16_384;
pub const MAX_ARCHIVE_BYTES: usize = 524_288;
pub const MAX_DECOMPRESSED_BYTES: usize = 278_528;
pub const MAX_BUILD_FEATURES: usize = 64;
pub const MAX_VALIDITY_SECONDS: i64 = 2_592_000;
pub const MAX_FUTURE_SKEW_SECONDS: i64 = 300;

const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";
const ENVELOPE_PATH: &str = "envelope.json";
const SIGNATURE_PATH: &str = "envelope.sig";
const RESULT_PATH: &str = "result.json";
const OUTPUT_MODE: u32 = 0o600;

static PROFILE_COLLECTOR_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum ProfileTool {
    #[serde(rename = "profile.cpu")]
    Cpu,
    #[serde(rename = "profile.memory")]
    Memory,
    #[serde(rename = "profile.threads")]
    Threads,
}

impl ProfileTool {
    pub const fn id(self) -> &'static str {
        match self {
            Self::Cpu => "profile.cpu",
            Self::Memory => "profile.memory",
            Self::Threads => "profile.threads",
        }
    }

    pub const fn capability(self) -> &'static str {
        match self {
            Self::Cpu => CPU_PROFILE_CAPABILITY,
            Self::Memory => MEMORY_PROFILE_CAPABILITY,
            Self::Threads => THREAD_PROFILE_CAPABILITY,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProfileOutcome {
    Succeeded,
    Partial,
    Failed,
    Unsupported,
    Cancelled,
}

impl ProfileOutcome {
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
pub enum ProfileReasonCode {
    Complete,
    LimitExceeded,
    SourceUnavailable,
    UnsupportedTool,
    UnsupportedVersion,
    UnsupportedPlatform,
    Cancelled,
    CounterReset,
    InvalidInput,
    CollectionFailed,
}

impl ProfileReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "COMPLETE",
            Self::LimitExceeded => "LIMIT_EXCEEDED",
            Self::SourceUnavailable => "SOURCE_UNAVAILABLE",
            Self::UnsupportedTool => "UNSUPPORTED_TOOL",
            Self::UnsupportedVersion => "UNSUPPORTED_VERSION",
            Self::UnsupportedPlatform => "UNSUPPORTED_PLATFORM",
            Self::Cancelled => "CANCELLED",
            Self::CounterReset => "COUNTER_RESET",
            Self::InvalidInput => "INVALID_INPUT",
            Self::CollectionFailed => "COLLECTION_FAILED",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileProvenance {
    repository: &'static str,
    source_commit: String,
    executable_sha256: String,
    rustfs_version: String,
    os_family: ProfileOsFamily,
    architecture: ProfileArchitecture,
    build_features: Vec<String>,
}

impl ProfileProvenance {
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
            os_family: ProfileOsFamily::current(),
            architecture: ProfileArchitecture::current(),
            build_features,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProfileOsFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

impl ProfileOsFamily {
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
pub enum ProfileArchitecture {
    #[serde(rename = "x86_64")]
    X86_64,
    Aarch64,
    Other,
}

impl ProfileArchitecture {
    fn current() -> Self {
        match std::env::consts::ARCH {
            "x86_64" => Self::X86_64,
            "aarch64" => Self::Aarch64,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalProfileConsent {
    pub consent_uid: String,
    pub policy_revision: u64,
    pub expires_at_unix: i64,
    pub confirmed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileCaptureRequest {
    pub organization_name: String,
    pub cluster_name: String,
    pub device_name: String,
    pub run_uid: String,
    pub artifact_uid: String,
    pub schema_version: u16,
    pub capability: String,
    pub consent: LocalProfileConsent,
    pub produced_at_unix: i64,
    pub expires_at_unix: i64,
    pub nonce: [u8; 32],
    pub duration: Duration,
    pub sample_period: Duration,
    pub provenance: ProfileProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileCoverage {
    requested_units: u32,
    completed_units: u32,
    unit: &'static str,
}

impl ProfileCoverage {
    pub(super) const fn complete_window() -> Self {
        Self {
            requested_units: 1,
            completed_units: 1,
            unit: "WINDOW",
        }
    }

    const fn none() -> Self {
        Self {
            requested_units: 0,
            completed_units: 0,
            unit: "WINDOW",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CpuProfileData {
    sample_period_micros: u64,
    samples: Vec<CpuProfileSample>,
    dropped_sample_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CpuProfileSample {
    symbol_id: String,
    sample_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryProfileData {
    scope: &'static str,
    allocated_bytes: u64,
    allocation_count: u64,
    sample_period_micros: u64,
}

impl MemoryProfileData {
    pub(super) const fn allocation_aggregates(allocated_bytes: u64, allocation_count: u64, sample_period_micros: u64) -> Self {
        Self {
            scope: "ALLOCATION_AGGREGATES",
            allocated_bytes,
            allocation_count,
            sample_period_micros,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ThreadProfileScope {
    TokioRuntime,
    NativeThreads,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ProfileData {
    Cpu(CpuProfileData),
    Memory(MemoryProfileData),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileResult {
    schema_version: u16,
    run_uid: String,
    tool_id: ProfileTool,
    capability: &'static str,
    outcome: ProfileOutcome,
    reason_code: ProfileReasonCode,
    duration_millis: u64,
    provenance: ProfileProvenance,
    coverage: ProfileCoverage,
    data: Option<ProfileData>,
}

impl ProfileResult {
    pub fn outcome(&self) -> ProfileOutcome {
        self.outcome
    }

    pub fn reason_code(&self) -> ProfileReasonCode {
        self.reason_code
    }

    pub fn data(&self) -> Option<&ProfileData> {
        self.data.as_ref()
    }

    pub(super) fn succeeded(request: &ProfileCaptureRequest, tool: ProfileTool, duration: Duration, data: ProfileData) -> Self {
        Self {
            schema_version: PROFILE_SCHEMA_VERSION,
            run_uid: request.run_uid.clone(),
            tool_id: tool,
            capability: tool.capability(),
            outcome: ProfileOutcome::Succeeded,
            reason_code: ProfileReasonCode::Complete,
            duration_millis: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).min(30_000),
            provenance: request.provenance.clone(),
            coverage: ProfileCoverage::complete_window(),
            data: Some(data),
        }
    }

    pub(super) fn unsupported(request: &ProfileCaptureRequest, tool: ProfileTool, reason_code: ProfileReasonCode) -> Self {
        Self {
            schema_version: PROFILE_SCHEMA_VERSION,
            run_uid: request.run_uid.clone(),
            tool_id: tool,
            capability: tool.capability(),
            outcome: ProfileOutcome::Unsupported,
            reason_code,
            duration_millis: 0,
            provenance: request.provenance.clone(),
            coverage: ProfileCoverage::none(),
            data: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedProfileExport {
    pub artifact_uid: String,
    pub tool: ProfileTool,
    pub outcome: ProfileOutcome,
    pub reason_code: ProfileReasonCode,
    pub archive_bytes: Vec<u8>,
    pub archive_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedProfileExport {
    pub artifact_uid: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
}

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("profile_local_consent_required")]
    ConsentRequired,
    #[error("profile_local_consent_expired")]
    ConsentExpired,
    #[error("profile_request_expired")]
    Expired,
    #[error("profile_invalid_request")]
    InvalidRequest,
    #[error("profile_unsupported_version")]
    UnsupportedVersion,
    #[error("profile_unsupported_capability")]
    UnsupportedCapability,
    #[error("profile_limit_exceeded")]
    LimitExceeded,
    #[error("profile_collection_cancelled")]
    Cancelled,
    #[error("profile_collection_timed_out")]
    TimedOut,
    #[error("profile_collection_already_running")]
    Busy,
    #[error("profile_source_unavailable")]
    SourceUnavailable,
    #[error("profile_counter_reset")]
    CounterReset,
    #[error("profile_export_signing_failed")]
    Signing,
    #[error("profile_export_exists")]
    AlreadyExists,
    #[error("profile_export_io_failed")]
    Io(#[source] std::io::Error),
    #[error("profile_export_encoding_failed")]
    Encoding,
    #[error("profile_export_durability_failed_after_commit")]
    DurabilityAfterCommit(#[source] std::io::Error),
}

pub fn capture_cpu_profile(request: &ProfileCaptureRequest, cancel: &CancellationToken) -> Result<ProfileResult, ProfileError> {
    request.validate(ProfileTool::Cpu, unix_now()?)?;
    check_cancel(cancel)?;

    // The only existing CPU profiler exports to Pyroscope and does not provide
    // a reviewed local symbol-id catalogue. Publishing samples here would turn
    // unreviewed process symbols into L3 evidence or invent their mapping.
    Ok(ProfileResult::unsupported(request, ProfileTool::Cpu, ProfileReasonCode::UnsupportedTool))
}

pub fn export_cpu_profile(
    request: &ProfileCaptureRequest,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedProfileExport, ProfileError> {
    let result = capture_cpu_profile(request, cancel)?;
    encode_signed_profile_export(request, &result, key, cancel)
}

pub fn encode_signed_profile_export(
    request: &ProfileCaptureRequest,
    result: &ProfileResult,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedProfileExport, ProfileError> {
    let now = unix_now()?;
    request.validate(result.tool_id, now)?;
    check_cancel(cancel)?;
    let valid_data = match result.outcome {
        ProfileOutcome::Succeeded | ProfileOutcome::Partial => result.data.is_some(),
        ProfileOutcome::Failed | ProfileOutcome::Unsupported | ProfileOutcome::Cancelled => result.data.is_none(),
    };
    if !valid_data
        || result.run_uid != request.run_uid
        || result.schema_version != request.schema_version
        || result.capability != request.capability
    {
        return Err(ProfileError::InvalidRequest);
    }

    let result_bytes = serde_json::to_vec(result).map_err(|_| ProfileError::Encoding)?;
    if result_bytes.is_empty() || result_bytes.len() > MAX_RESULT_BYTES {
        return Err(ProfileError::LimitExceeded);
    }

    let device_key_id = hex_lower(&Sha256::digest(key.public_key_der()));
    let envelope = ProfileEnvelope {
        format_version: "rustfs.connect.diagnosticEnvelope/1",
        protocol_version: "v1",
        organization_name: &request.organization_name,
        cluster_name: &request.cluster_name,
        device_name: &request.device_name,
        run_uid: &request.run_uid,
        artifact_uid: &request.artifact_uid,
        tool_id: result.tool_id,
        schema_version: PROFILE_SCHEMA_VERSION,
        classification: "L3",
        consent_uid: &request.consent.consent_uid,
        policy_revision: request.consent.policy_revision,
        produced_at: timestamp(request.produced_at_unix)?,
        expires_at: timestamp(request.expires_at_unix)?,
        nonce: URL_SAFE_NO_PAD.encode_to_string(request.nonce),
        device_key_id: &device_key_id,
        payload: ProfilePayload {
            path: RESULT_PATH,
            media_type: "application/json",
            size_bytes: result_bytes.len() as u64,
            sha256: hex_lower(&Sha256::digest(&result_bytes)),
        },
    };
    let envelope_bytes = serde_json::to_vec(&envelope).map_err(|_| ProfileError::Encoding)?;
    if envelope_bytes.is_empty() || envelope_bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(ProfileError::LimitExceeded);
    }
    let signature_bytes = signature_document(key, &device_key_id, &envelope_bytes)?;
    let decompressed = result_bytes
        .len()
        .checked_add(envelope_bytes.len())
        .and_then(|size| size.checked_add(signature_bytes.len()))
        .ok_or(ProfileError::LimitExceeded)?;
    if decompressed > MAX_DECOMPRESSED_BYTES {
        return Err(ProfileError::LimitExceeded);
    }
    check_cancel(cancel)?;
    if unix_now()? >= request.expires_at_unix {
        return Err(ProfileError::Expired);
    }

    let archive_bytes = archive(&envelope_bytes, &signature_bytes, &result_bytes)?;
    if archive_bytes.len() > MAX_ARCHIVE_BYTES {
        return Err(ProfileError::LimitExceeded);
    }
    let archive_sha256 = hex_lower(&Sha256::digest(&archive_bytes));

    Ok(SignedProfileExport {
        artifact_uid: request.artifact_uid.clone(),
        tool: result.tool_id,
        outcome: result.outcome,
        reason_code: result.reason_code,
        archive_bytes,
        archive_sha256,
    })
}

pub fn save_signed_profile_export(
    output: &Path,
    export: &SignedProfileExport,
    cancel: &CancellationToken,
) -> Result<SavedProfileExport, ProfileError> {
    check_cancel(cancel)?;
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = output.file_name().ok_or(ProfileError::InvalidRequest)?.to_string_lossy();
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
        file.write_all(&export.archive_bytes).map_err(ProfileError::Io)?;
        check_cancel(cancel)?;
        file.sync_all().map_err(ProfileError::Io)?;
        check_cancel(cancel)?;
        fs::hard_link(&temporary, output).map_err(map_publish_error)?;
        if let Err(error) = fs::remove_file(&temporary) {
            return Err(ProfileError::DurabilityAfterCommit(error));
        }
        #[cfg(unix)]
        if let Err(error) = File::open(parent).and_then(|directory| directory.sync_all()) {
            return Err(ProfileError::DurabilityAfterCommit(error));
        }
        Ok(SavedProfileExport {
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

impl ProfileCaptureRequest {
    pub(super) fn validate(&self, tool: ProfileTool, now_unix: i64) -> Result<(), ProfileError> {
        if self.schema_version != PROFILE_SCHEMA_VERSION {
            return Err(ProfileError::UnsupportedVersion);
        }
        if self.capability != tool.capability() {
            return Err(ProfileError::UnsupportedCapability);
        }
        if !self.consent.confirmed || self.consent.policy_revision == 0 {
            return Err(ProfileError::ConsentRequired);
        }
        if self.consent.expires_at_unix <= now_unix || self.expires_at_unix > self.consent.expires_at_unix {
            return Err(ProfileError::ConsentExpired);
        }
        let validity = self
            .expires_at_unix
            .checked_sub(self.produced_at_unix)
            .ok_or(ProfileError::Expired)?;
        if self.produced_at_unix > now_unix.saturating_add(MAX_FUTURE_SKEW_SECONDS)
            || validity <= 0
            || self.expires_at_unix <= now_unix
            || validity > MAX_VALIDITY_SECONDS
        {
            return Err(ProfileError::Expired);
        }
        if self.duration.is_zero()
            || self.duration > MAX_PROFILE_DURATION
            || self.sample_period.is_zero()
            || self.sample_period > self.duration
        {
            return Err(ProfileError::LimitExceeded);
        }
        if !uuid7(&self.run_uid)
            || !uuid7(&self.artifact_uid)
            || !uuid7(&self.consent.consent_uid)
            || !resource_names_match(self)
            || !lower_hex(&self.provenance.source_commit, 40)
            || !lower_hex(&self.provenance.executable_sha256, 64)
            || !version(&self.provenance.rustfs_version)
            || self.provenance.build_features.len() > MAX_BUILD_FEATURES
            || !self.provenance.build_features.iter().all(|feature| build_feature(feature))
        {
            return Err(ProfileError::InvalidRequest);
        }
        Ok(())
    }
}

pub(super) struct CollectorLease;

impl CollectorLease {
    pub(super) fn acquire() -> Result<Self, ProfileError> {
        PROFILE_COLLECTOR_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self)
            .map_err(|_| ProfileError::Busy)
    }
}

impl Drop for CollectorLease {
    fn drop(&mut self) {
        PROFILE_COLLECTOR_ACTIVE.store(false, Ordering::Release);
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfileEnvelope<'a> {
    format_version: &'static str,
    protocol_version: &'static str,
    organization_name: &'a str,
    cluster_name: &'a str,
    device_name: &'a str,
    run_uid: &'a str,
    artifact_uid: &'a str,
    tool_id: ProfileTool,
    schema_version: u16,
    classification: &'static str,
    consent_uid: &'a str,
    policy_revision: u64,
    produced_at: String,
    expires_at: String,
    nonce: String,
    device_key_id: &'a str,
    payload: ProfilePayload,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfilePayload {
    path: &'static str,
    media_type: &'static str,
    size_bytes: u64,
    sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfileSignature<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: String,
}

fn signature_document(key: &DeviceIdentity, key_id: &str, envelope: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let pkcs8 = key.to_pkcs8_der().map_err(|_| ProfileError::Signing)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| ProfileError::Signing)?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + envelope.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(envelope);
    let signature: Signature = signing_key.sign(&input);
    let value = URL_SAFE_NO_PAD.encode_to_string(signature.normalize_s().to_bytes());
    serde_json::to_vec(&ProfileSignature {
        algorithm: "ES256",
        key_id,
        value,
    })
    .map_err(|_| ProfileError::Encoding)
}

fn archive(envelope: &[u8], signature: &[u8], result: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let cursor = Cursor::new(Vec::with_capacity(envelope.len() + signature.len() + result.len() + 512));
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::DEFAULT
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(OUTPUT_MODE);
    for (name, bytes) in [(ENVELOPE_PATH, envelope), (SIGNATURE_PATH, signature), (RESULT_PATH, result)] {
        writer.start_file(name, options).map_err(|_| ProfileError::Encoding)?;
        writer.write_all(bytes).map_err(ProfileError::Io)?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|_| ProfileError::Encoding)
}

fn resource_names_match(request: &ProfileCaptureRequest) -> bool {
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

fn timestamp(unix: i64) -> Result<String, ProfileError> {
    OffsetDateTime::from_unix_timestamp(unix)
        .map_err(|_| ProfileError::InvalidRequest)?
        .format(&Rfc3339)
        .map_err(|_| ProfileError::InvalidRequest)
}

pub(super) fn unix_now() -> Result<i64, ProfileError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProfileError::InvalidRequest)?;
    i64::try_from(duration.as_secs()).map_err(|_| ProfileError::InvalidRequest)
}

pub(super) fn check_cancel(cancel: &CancellationToken) -> Result<(), ProfileError> {
    if cancel.is_cancelled() {
        Err(ProfileError::Cancelled)
    } else {
        Ok(())
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing hexadecimal to a string cannot fail");
    }
    value
}

fn map_create_error(error: std::io::Error) -> ProfileError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        ProfileError::AlreadyExists
    } else {
        ProfileError::Io(error)
    }
}

fn map_publish_error(error: std::io::Error) -> ProfileError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        ProfileError::AlreadyExists
    } else {
        ProfileError::Io(error)
    }
}
