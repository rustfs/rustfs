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

//! Device-side retrieval of operator-approved RustFS Connect service licenses.

use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::config::HeartbeatConfig;
use super::license::{
    LICENSE_SCHEMA, LicenseArtifactError, LicenseClaims, LicenseReport, LicenseVerificationContext, apply_license_artifact,
    inspect_installed_license, verify_license_artifact,
};
use super::telemetry::{TelemetryDelivery, TelemetryError, TelemetryTransport, is_exact_utc_seconds};

const PROTOCOL_VERSION: &str = "v1";
const DELIVERY_VERSION: u32 = 1;
const MAX_ATTEMPTS: usize = 3;

#[cfg(unix)]
const ARTIFACT_FILE_MODE: u32 = 0o600;

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Result of checking the currently installed license for an approved renewal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LicenseRenewalOutcome {
    Requested,
    Pending { replacement_license_uid: String },
    Installed(Box<LicenseReport>),
}

/// An mTLS client for the read-only Connect license-renewal delivery surface.
pub struct LicenseRenewalClient {
    transport: TelemetryTransport,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl LicenseRenewalClient {
    /// Reuses the registered device identity, explicit proxy, trust roots, and
    /// bounded request timeout from the Connect heartbeat transport.
    pub fn new(config: HeartbeatConfig) -> Result<Self, LicenseRenewalError> {
        let initial_backoff = config.schedule.initial_backoff;
        let max_backoff = config.schedule.max_backoff;
        let transport = TelemetryTransport::new(config).map_err(transport_error)?;
        Ok(Self {
            transport,
            initial_backoff,
            max_backoff,
        })
    }

    /// Checks the license currently installed in `state_directory` and installs
    /// only an operator-approved, signed replacement for the same scope.
    pub async fn renew_installed(
        &self,
        state_directory: &Path,
        context: &LicenseVerificationContext,
    ) -> Result<LicenseRenewalOutcome, LicenseRenewalError> {
        let current = inspect_installed_license(state_directory, context)?;
        let current = current.license.ok_or(LicenseRenewalError::InstalledLicense)?;
        let status_request = LicenseRenewalRequest::new(&current)?;
        let status_body = self.post_with_retry("licenseRenewal:status", &status_request).await?;
        let status = decode_status(&status_body, &status_request)?;

        match status.status {
            RenewalStatus::Requested => Ok(LicenseRenewalOutcome::Requested),
            RenewalStatus::Pending => Ok(LicenseRenewalOutcome::Pending {
                replacement_license_uid: status.replacement_license_uid.ok_or(LicenseRenewalError::Response)?,
            }),
            RenewalStatus::Ready => {
                let expected_replacement = status
                    .replacement_license_uid
                    .as_deref()
                    .ok_or(LicenseRenewalError::Response)?;
                let expected_digest = status.artifact_sha256.as_deref().ok_or(LicenseRenewalError::Response)?;
                let download_request = LicenseRenewalRequest::new(&current)?;
                let body = self.post_with_retry("licenseRenewal:download", &download_request).await?;
                let artifact =
                    decode_artifact(&body, &download_request, expected_replacement, expected_digest, &status.expire_time)?;
                let mut current_context = context.clone();
                current_context.now_unix = Utc::now().timestamp();
                install_downloaded_artifact(state_directory, &current_context, artifact)
            }
        }
    }

    async fn post_with_retry<T: Serialize>(&self, operation: &str, body: &T) -> Result<Vec<u8>, LicenseRenewalError> {
        let mut backoff = self.initial_backoff;
        for attempt in 0..MAX_ATTEMPTS {
            match self.transport.post(operation, body).await.map_err(transport_error)? {
                TelemetryDelivery::Accepted { body, .. } => return Ok(body),
                TelemetryDelivery::Retry { retry_after } if attempt + 1 < MAX_ATTEMPTS => {
                    let delay = retry_after.unwrap_or(backoff).clamp(self.initial_backoff, self.max_backoff);
                    tokio::time::sleep(delay).await;
                    backoff = backoff.saturating_mul(2).min(self.max_backoff);
                }
                TelemetryDelivery::Retry { .. } => return Err(LicenseRenewalError::RetryExhausted),
                TelemetryDelivery::AuthenticationStopped { status, .. } => {
                    return Err(LicenseRenewalError::AuthenticationStopped { status });
                }
                TelemetryDelivery::Rejected { status, .. } => return Err(LicenseRenewalError::Rejected { status }),
            }
        }
        Err(LicenseRenewalError::RetryExhausted)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LicenseRenewalRequest {
    protocol_version: &'static str,
    current_license_uid: String,
    service_code: String,
    request_id: String,
}

impl LicenseRenewalRequest {
    fn new(current: &LicenseClaims) -> Result<Self, LicenseRenewalError> {
        if !is_uuid_v7(&current.license_uid) || !is_service_code(&current.service_code) {
            return Err(LicenseRenewalError::InstalledLicense);
        }
        Ok(Self {
            protocol_version: PROTOCOL_VERSION,
            current_license_uid: current.license_uid.clone(),
            service_code: current.service_code.clone(),
            request_id: Uuid::new_v4().to_string(),
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum RenewalStatus {
    Requested,
    Pending,
    Ready,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LicenseRenewalResponse {
    protocol_version: String,
    status: RenewalStatus,
    source_license_uid: String,
    replacement_license_uid: Option<String>,
    service_code: String,
    schema: String,
    version: u32,
    expire_time: String,
    artifact_sha256: Option<String>,
    manual_approval_required: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DownloadedArtifact {
    payload: String,
    signature: String,
}

struct DownloadedLicense {
    artifact: DownloadedArtifact,
    replacement_license_uid: String,
    expire_time: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LicenseArtifactResponse {
    protocol_version: String,
    status: RenewalStatus,
    source_license_uid: String,
    replacement_license_uid: Option<String>,
    service_code: String,
    schema: String,
    version: u32,
    expire_time: String,
    artifact_sha256: Option<String>,
    artifact: DownloadedArtifact,
}

fn decode_status(body: &[u8], request: &LicenseRenewalRequest) -> Result<LicenseRenewalResponse, LicenseRenewalError> {
    let response: LicenseRenewalResponse = serde_json::from_slice(body).map_err(|_| LicenseRenewalError::Response)?;
    validate_common(
        &response.protocol_version,
        &response.source_license_uid,
        response.replacement_license_uid.as_deref(),
        &response.service_code,
        &response.schema,
        response.version,
        &response.expire_time,
        response.artifact_sha256.as_deref(),
        request,
    )?;
    let valid_state = match response.status {
        RenewalStatus::Requested => {
            response.replacement_license_uid.is_none() && response.artifact_sha256.is_none() && response.manual_approval_required
        }
        RenewalStatus::Pending => {
            response.replacement_license_uid.is_some() && response.artifact_sha256.is_none() && response.manual_approval_required
        }
        RenewalStatus::Ready => {
            response.replacement_license_uid.is_some()
                && response.artifact_sha256.as_deref().is_some_and(is_sha256)
                && !response.manual_approval_required
        }
    };
    if !valid_state {
        return Err(LicenseRenewalError::Response);
    }
    Ok(response)
}

fn decode_artifact(
    body: &[u8],
    request: &LicenseRenewalRequest,
    expected_replacement: &str,
    expected_digest: &str,
    expected_expire_time: &str,
) -> Result<DownloadedLicense, LicenseRenewalError> {
    let response: LicenseArtifactResponse = serde_json::from_slice(body).map_err(|_| LicenseRenewalError::Response)?;
    validate_common(
        &response.protocol_version,
        &response.source_license_uid,
        response.replacement_license_uid.as_deref(),
        &response.service_code,
        &response.schema,
        response.version,
        &response.expire_time,
        response.artifact_sha256.as_deref(),
        request,
    )?;
    let replacement = response.replacement_license_uid.as_deref();
    let digest = response.artifact_sha256.as_deref();
    if response.status != RenewalStatus::Ready
        || replacement != Some(expected_replacement)
        || digest != Some(expected_digest)
        || response.expire_time != expected_expire_time
        || !is_sha256(expected_digest)
    {
        return Err(LicenseRenewalError::Response);
    }
    let canonical = serde_json::to_vec(&response.artifact).map_err(|_| LicenseRenewalError::Response)?;
    let actual_digest = hex_simd::encode_to_string(Sha256::digest(&canonical), hex_simd::AsciiCase::Lower);
    if !constant_time_eq(actual_digest.as_bytes(), expected_digest.as_bytes()) {
        return Err(LicenseRenewalError::DigestMismatch);
    }
    Ok(DownloadedLicense {
        artifact: response.artifact,
        replacement_license_uid: expected_replacement.to_owned(),
        expire_time: response.expire_time,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_common(
    protocol_version: &str,
    source_license_uid: &str,
    replacement_license_uid: Option<&str>,
    service_code: &str,
    schema: &str,
    version: u32,
    expire_time: &str,
    artifact_sha256: Option<&str>,
    request: &LicenseRenewalRequest,
) -> Result<(), LicenseRenewalError> {
    if protocol_version != PROTOCOL_VERSION
        || source_license_uid != request.current_license_uid
        || service_code != request.service_code
        || schema != LICENSE_SCHEMA
        || version != DELIVERY_VERSION
        || !is_exact_utc_seconds(expire_time)
        || replacement_license_uid.is_some_and(|uid| !is_uuid_v7(uid))
        || artifact_sha256.is_some_and(|digest| !is_sha256(digest))
    {
        return Err(LicenseRenewalError::Response);
    }
    Ok(())
}

fn install_downloaded_artifact(
    state_directory: &Path,
    context: &LicenseVerificationContext,
    download: DownloadedLicense,
) -> Result<LicenseRenewalOutcome, LicenseRenewalError> {
    fs::create_dir_all(state_directory).map_err(LicenseRenewalError::InstallState)?;
    let bytes = serde_json::to_vec(&download.artifact).map_err(|_| LicenseRenewalError::Response)?;
    let path = stage_artifact(state_directory, &bytes)?;
    let result = (|| {
        let verified = verify_license_artifact(&path, state_directory, context)?;
        let claims = verified.license.as_ref().ok_or(LicenseRenewalError::Response)?;
        if claims.license_uid != download.replacement_license_uid || claims.expire_time != download.expire_time {
            return Err(LicenseRenewalError::Response);
        }
        apply_license_artifact(&path, state_directory, context)
            .map(|report| LicenseRenewalOutcome::Installed(Box::new(report)))
            .map_err(LicenseRenewalError::License)
    })();
    let _ = fs::remove_file(path);
    result
}

fn stage_artifact(directory: &Path, bytes: &[u8]) -> Result<PathBuf, LicenseRenewalError> {
    loop {
        let path = directory.join(format!(
            ".license-renewal.{}.{}.tmp",
            std::process::id(),
            STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(ARTIFACT_FILE_MODE);
        }
        let mut file = match options.open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(LicenseRenewalError::InstallState(source)),
        };
        if let Err(source) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            let _ = fs::remove_file(path);
            return Err(LicenseRenewalError::InstallState(source));
        }
        return Ok(path);
    }
}

fn is_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| uuid.get_version_num() == 7 && uuid.to_string() == value)
}

fn is_service_code(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z'))
        && value.len() <= 64
        && bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

fn transport_error(error: TelemetryError) -> LicenseRenewalError {
    match error {
        TelemetryError::Endpoint => LicenseRenewalError::Endpoint,
        TelemetryError::RootCertificate => LicenseRenewalError::RootCertificate,
        TelemetryError::ProxyConfiguration => LicenseRenewalError::ProxyConfiguration,
        TelemetryError::ProxyAuthentication => LicenseRenewalError::ProxyAuthentication,
        TelemetryError::ProxyRejected => LicenseRenewalError::ProxyRejected,
        TelemetryError::TlsPeer => LicenseRenewalError::TlsPeer,
        TelemetryError::Schedule => LicenseRenewalError::Schedule,
        TelemetryError::NotRegistered => LicenseRenewalError::NotRegistered,
        TelemetryError::IdentityMissing => LicenseRenewalError::IdentityMissing,
        TelemetryError::IdentityCertificate => LicenseRenewalError::IdentityCertificate,
        TelemetryError::CredentialName => LicenseRenewalError::CredentialName,
        TelemetryError::CredentialExpired => LicenseRenewalError::CredentialExpired,
        TelemetryError::StateConflict => LicenseRenewalError::CredentialState,
        TelemetryError::ResponseTooLarge => LicenseRenewalError::ResponseTooLarge,
        TelemetryError::Url(_)
        | TelemetryError::Transport(_)
        | TelemetryError::Identity(_)
        | TelemetryError::IdentityStore(_)
        | TelemetryError::CredentialStore(_)
        | TelemetryError::CredentialValidation(_) => LicenseRenewalError::Transport,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LicenseRenewalError {
    #[error("Connect license endpoint must be an HTTPS base URL without credentials, query, or fragment")]
    Endpoint,
    #[error("Connect license root CA configuration is invalid")]
    RootCertificate,
    #[error("Connect license proxy configuration is invalid")]
    ProxyConfiguration,
    #[error("Connect proxy authentication failed; verify the configured proxy credential files")]
    ProxyAuthentication,
    #[error("Connect proxy connection failed; verify proxy availability, credentials, and the Connect endpoint")]
    ProxyRejected,
    #[error("Connect TLS peer certificate validation failed; verify the endpoint and configured root CA")]
    TlsPeer,
    #[error("Connect license retry schedule is invalid")]
    Schedule,
    #[error("RustFS is not registered with Connect")]
    NotRegistered,
    #[error("the Connect device private key is missing")]
    IdentityMissing,
    #[error("the stored Connect certificate and device private key cannot form a TLS identity")]
    IdentityCertificate,
    #[error("the stored Connect credential name is invalid")]
    CredentialName,
    #[error("the stored Connect device certificate is not currently valid")]
    CredentialExpired,
    #[error("the persisted Connect credential transition is invalid")]
    CredentialState,
    #[error("Connect license response exceeded 64 KiB")]
    ResponseTooLarge,
    #[error("Connect license transport failed")]
    Transport,
    #[error("Connect license request failed after three bounded attempts")]
    RetryExhausted,
    #[error("Connect rejected the device license request with HTTP {status}")]
    AuthenticationStopped { status: u16 },
    #[error("Connect rejected the license request with HTTP {status}")]
    Rejected { status: u16 },
    #[error("the installed service license cannot identify the renewal scope")]
    InstalledLicense,
    #[error("the Connect license response is invalid")]
    Response,
    #[error("the Connect license artifact digest does not match the approved digest")]
    DigestMismatch,
    #[error("the Connect license artifact staging file could not be written")]
    InstallState(#[source] io::Error),
    #[error(transparent)]
    License(#[from] LicenseArtifactError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64_simd::URL_SAFE_NO_PAD;
    use chrono::DateTime;
    use serde::Deserialize;

    const SOURCE_UID: &str = "018cc251-f400-7000-8000-000000000005";
    const REPLACEMENT_UID: &str = "018cc251-f400-7000-8000-000000000002";

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Vector {
        public_key: String,
        key_id: String,
        connect_artifact: String,
    }

    fn vector() -> Vector {
        serde_json::from_str(include_str!("../../tests/fixtures/connect-license-ed25519-vector.json"))
            .expect("license vector must decode")
    }

    fn request() -> LicenseRenewalRequest {
        LicenseRenewalRequest {
            protocol_version: PROTOCOL_VERSION,
            current_license_uid: SOURCE_UID.to_owned(),
            service_code: "SUPPORT".to_owned(),
            request_id: "018cc251-f400-4000-8000-000000000001".to_owned(),
        }
    }

    fn artifact_response(digest: &str, artifact: &DownloadedArtifact) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "protocolVersion": "v1",
            "status": "READY",
            "sourceLicenseUid": SOURCE_UID,
            "replacementLicenseUid": REPLACEMENT_UID,
            "serviceCode": "SUPPORT",
            "schema": LICENSE_SCHEMA,
            "version": 1,
            "expireTime": "2030-02-01T00:00:00Z",
            "artifactSha256": digest,
            "artifact": artifact,
        }))
        .expect("response must encode")
    }

    #[test]
    fn status_accepts_only_consistent_manual_states() {
        let requested = serde_json::json!({
            "protocolVersion": "v1",
            "status": "REQUESTED",
            "sourceLicenseUid": SOURCE_UID,
            "replacementLicenseUid": null,
            "serviceCode": "SUPPORT",
            "schema": LICENSE_SCHEMA,
            "version": 1,
            "expireTime": "2030-01-01T00:00:00Z",
            "artifactSha256": null,
            "manualApprovalRequired": true,
        });
        assert_eq!(
            decode_status(&serde_json::to_vec(&requested).expect("status"), &request())
                .expect("valid requested state")
                .status,
            RenewalStatus::Requested
        );

        let mut inconsistent = requested;
        inconsistent["manualApprovalRequired"] = false.into();
        assert!(matches!(
            decode_status(&serde_json::to_vec(&inconsistent).expect("status"), &request()),
            Err(LicenseRenewalError::Response)
        ));
    }

    #[test]
    fn artifact_digest_and_signed_scope_are_verified_before_install() {
        let vector = vector();
        let artifact: DownloadedArtifact = serde_json::from_str(&vector.connect_artifact).expect("artifact");
        let bytes = serde_json::to_vec(&artifact).expect("canonical artifact");
        let digest = hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower);
        let downloaded = decode_artifact(
            &artifact_response(&digest, &artifact),
            &request(),
            REPLACEMENT_UID,
            &digest,
            "2030-02-01T00:00:00Z",
        )
        .expect("matching response");
        let public_key = URL_SAFE_NO_PAD.decode_to_vec(vector.public_key).expect("public key");
        let context = LicenseVerificationContext::new(
            public_key.try_into().expect("32-byte public key"),
            vector.key_id,
            "test-connect-issuer".to_owned(),
            "test-rustfs-cluster".to_owned(),
            "organizations/018cc251-f400-7000-8000-000000000003".to_owned(),
            "organizations/018cc251-f400-7000-8000-000000000003/clusters/018cc251-f400-7000-8000-000000000004".to_owned(),
            "SUPPORT".to_owned(),
            DateTime::parse_from_rfc3339("2030-01-15T00:00:00Z")
                .expect("time")
                .timestamp(),
        )
        .expect("verification context");
        let state = tempfile::tempdir().expect("state directory");

        let installed = install_downloaded_artifact(state.path(), &context, downloaded).expect("install");
        let LicenseRenewalOutcome::Installed(report) = installed else {
            panic!("expected installed outcome");
        };
        assert_eq!(report.license.expect("claims").license_uid, REPLACEMENT_UID);
        assert!(
            inspect_installed_license(state.path(), &context)
                .expect("installed license")
                .is_valid()
        );
    }

    #[test]
    fn digest_mismatch_and_wrong_audience_never_install() {
        let vector = vector();
        let artifact: DownloadedArtifact = serde_json::from_str(&vector.connect_artifact).expect("artifact");
        let digest = "0".repeat(64);
        assert!(matches!(
            decode_artifact(
                &artifact_response(&digest, &artifact),
                &request(),
                REPLACEMENT_UID,
                &digest,
                "2030-02-01T00:00:00Z",
            ),
            Err(LicenseRenewalError::DigestMismatch)
        ));

        let public_key = URL_SAFE_NO_PAD.decode_to_vec(vector.public_key).expect("public key");
        let context = LicenseVerificationContext::new(
            public_key.try_into().expect("32-byte public key"),
            vector.key_id,
            "test-connect-issuer".to_owned(),
            "another-audience".to_owned(),
            "organizations/018cc251-f400-7000-8000-000000000003".to_owned(),
            "organizations/018cc251-f400-7000-8000-000000000003/clusters/018cc251-f400-7000-8000-000000000004".to_owned(),
            "SUPPORT".to_owned(),
            DateTime::parse_from_rfc3339("2030-01-15T00:00:00Z")
                .expect("time")
                .timestamp(),
        )
        .expect("verification context");
        let state = tempfile::tempdir().expect("state directory");
        let result = install_downloaded_artifact(
            state.path(),
            &context,
            DownloadedLicense {
                artifact,
                replacement_license_uid: REPLACEMENT_UID.to_owned(),
                expire_time: "2030-02-01T00:00:00Z".to_owned(),
            },
        );

        assert!(matches!(result, Err(LicenseRenewalError::License(LicenseArtifactError { .. }))));
        assert!(matches!(
            inspect_installed_license(state.path(), &context),
            Err(LicenseArtifactError { .. })
        ));
    }

    #[test]
    fn transport_errors_do_not_expose_remote_or_proxy_details() {
        let errors = [
            LicenseRenewalError::ProxyAuthentication,
            LicenseRenewalError::ProxyRejected,
            LicenseRenewalError::TlsPeer,
            LicenseRenewalError::Transport,
        ];
        for error in errors {
            let message = error.to_string();
            for secret in ["proxy.example", "proxy-user", "proxy-password", "remote response"] {
                assert!(!message.contains(secret));
            }
        }
    }
}
