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

//! Device-authenticated upload of bounded support bundle archives.

use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::client::{TransportFailure, classify_transport_failure};
use super::config::HeartbeatConfig;
use super::telemetry::{TelemetryDelivery, TelemetryError, TelemetryTransport, is_exact_utc_seconds};

const PROTOCOL_VERSION: &str = "v1";
const CONTENT_TYPE: &str = "application/octet-stream";
const MAX_ATTEMPTS: usize = 3;
const MAX_UPLOAD_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const UPLOAD_BUFFER_BYTES: usize = 64 * 1024;

/// Maximum archive size accepted by the Connect agent API.
pub const MAX_SUPPORT_BUNDLE_BYTES: u64 = 256 * 1024 * 1024;

/// Verified outcome returned after Connect pins the uploaded object version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportUploadReceipt {
    pub name: String,
    pub uid: String,
    pub state: String,
    pub declared_size_bytes: u64,
    pub declared_sha256: String,
}

/// Uploads one local archive through the registered device identity.
pub struct ReportUploadClient {
    transport: TelemetryTransport,
    upload_client: Client,
    identity_store: super::IdentityStore,
    initial_backoff: Duration,
    max_backoff: Duration,
    proxy_configured: bool,
}

impl ReportUploadClient {
    /// Reuses the Connect mTLS identity, root bundle, explicit proxy, and
    /// disabled redirect/environment-proxy policy.
    pub fn new(config: HeartbeatConfig, upload_timeout: Duration) -> Result<Self, ReportUploadError> {
        if upload_timeout.is_zero() || upload_timeout > MAX_UPLOAD_TIMEOUT {
            return Err(ReportUploadError::UploadTimeout);
        }
        let initial_backoff = config.schedule.initial_backoff;
        let max_backoff = config.schedule.max_backoff;
        let proxy_configured = config.proxy.is_some();
        let identity_store = config.identity_store.clone();
        let transport = TelemetryTransport::new(config).map_err(transport_error)?;
        let upload_client = transport.presigned_client(upload_timeout).map_err(transport_error)?;
        Ok(Self {
            transport,
            upload_client,
            identity_store,
            initial_backoff,
            max_backoff,
            proxy_configured,
        })
    }

    /// Wraps a typed diagnostic in its signed upload manifest when needed,
    /// hashes the exact upload bytes, reserves one object, and completes the
    /// reservation only after the object store accepts the archive.
    pub async fn upload(
        &self,
        archive: &Path,
        cancellation: &CancellationToken,
    ) -> Result<ReportUploadReceipt, ReportUploadError> {
        if cancellation.is_cancelled() {
            return Err(ReportUploadError::Cancelled);
        }
        let generated_bundle_uid = Uuid::now_v7().to_string();
        let source = super::report_bundle::upload_source(archive, &generated_bundle_uid, &self.identity_store)
            .map_err(report_bundle_error)?;
        let prepared = prepare_file(File::from_std(source.file), cancellation).await?;
        let request_id = Uuid::new_v4().to_string();
        let bundle_uid = source.bundle_uid;
        let reserve = ReserveRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: &request_id,
            bundle_uid: &bundle_uid,
            content_type: CONTENT_TYPE,
            declared_size_bytes: prepared.size,
            declared_sha256: &prepared.sha256,
        };

        let mut backoff = self.initial_backoff;
        let mut expected_name = None;
        for attempt in 0..MAX_ATTEMPTS {
            let (cluster_name, body) = self
                .post_control("supportBundles", &reserve, StatusCode::CREATED, cancellation)
                .await?;
            let name = format!("{cluster_name}/supportBundles/{bundle_uid}");
            if expected_name.as_ref().is_some_and(|expected| expected != &name) {
                return Err(ReportUploadError::Response);
            }
            expected_name = Some(name.clone());
            let reservation = decode_reservation(&body, &name, &bundle_uid, &prepared)?;

            match self.put(&prepared.file, &reservation.authorization, cancellation).await? {
                UploadDelivery::Accepted | UploadDelivery::AlreadyPresent => break,
                UploadDelivery::Retry { retry_after } if attempt + 1 < MAX_ATTEMPTS => {
                    let delay = retry_after.unwrap_or(backoff).clamp(self.initial_backoff, self.max_backoff);
                    sleep_or_cancel(cancellation, delay).await?;
                    backoff = backoff.saturating_mul(2).min(self.max_backoff);
                }
                UploadDelivery::Retry { .. } => return Err(ReportUploadError::RetryExhausted),
            }
        }

        let complete_request_id = Uuid::new_v4().to_string();
        let complete = CompleteRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: &complete_request_id,
        };
        let path = format!("supportBundles/{bundle_uid}:completeUpload");
        let (cluster_name, body) = self.post_control(&path, &complete, StatusCode::OK, cancellation).await?;
        let expected_name = expected_name.ok_or(ReportUploadError::Response)?;
        if expected_name != format!("{cluster_name}/supportBundles/{bundle_uid}") {
            return Err(ReportUploadError::Response);
        }
        decode_receipt(&body, &expected_name, &bundle_uid, &prepared)
    }

    async fn post_control<T: Serialize>(
        &self,
        operation: &str,
        body: &T,
        expected_status: StatusCode,
        cancellation: &CancellationToken,
    ) -> Result<(String, Vec<u8>), ReportUploadError> {
        let mut backoff = self.initial_backoff;
        for attempt in 0..MAX_ATTEMPTS {
            let delivery = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(ReportUploadError::Cancelled),
                result = self.transport.post_expect(operation, body, expected_status) => result.map_err(transport_error)?,
            };
            match delivery {
                TelemetryDelivery::Accepted { cluster_name, body } => return Ok((cluster_name, body)),
                TelemetryDelivery::Retry { retry_after } if attempt + 1 < MAX_ATTEMPTS => {
                    let delay = retry_after.unwrap_or(backoff).clamp(self.initial_backoff, self.max_backoff);
                    sleep_or_cancel(cancellation, delay).await?;
                    backoff = backoff.saturating_mul(2).min(self.max_backoff);
                }
                TelemetryDelivery::Retry { .. } => return Err(ReportUploadError::RetryExhausted),
                TelemetryDelivery::AuthenticationStopped { status, .. } => {
                    return Err(ReportUploadError::AuthenticationStopped { status });
                }
                TelemetryDelivery::Rejected { status, .. } => {
                    return Err(ReportUploadError::ControlRejected { status });
                }
            }
        }
        Err(ReportUploadError::RetryExhausted)
    }

    async fn put(
        &self,
        file: &File,
        authorization: &UploadAuthorization,
        cancellation: &CancellationToken,
    ) -> Result<UploadDelivery, ReportUploadError> {
        let mut source = file.try_clone().await.map_err(ReportUploadError::ArchiveRead)?;
        source
            .seek(SeekFrom::Start(0))
            .await
            .map_err(ReportUploadError::ArchiveRead)?;
        let body = reqwest::Body::wrap_stream(ReaderStream::with_capacity(source, UPLOAD_BUFFER_BYTES));
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ReportUploadError::Cancelled),
            result = self.upload_client.put(authorization.url.clone()).headers(authorization.headers.clone()).body(body).send() => result,
        };
        let response = match response {
            Ok(response) => response,
            Err(error) if error.is_body() => return Ok(UploadDelivery::Retry { retry_after: None }),
            Err(error) if error.is_timeout() || error.is_connect() || error.is_request() => {
                if let Some(failure) = classify_transport_failure(&error, self.proxy_configured) {
                    return Err(match failure {
                        TransportFailure::ProxyAuthentication => ReportUploadError::ProxyAuthentication,
                        TransportFailure::ProxyRejected => ReportUploadError::ProxyRejected,
                        TransportFailure::TlsPeer => ReportUploadError::TlsPeer,
                    });
                }
                return Ok(UploadDelivery::Retry { retry_after: None });
            }
            Err(_) => return Err(ReportUploadError::UploadTransport),
        };
        let status = response.status();
        if status.is_success() {
            return Ok(UploadDelivery::Accepted);
        }
        if status == StatusCode::PRECONDITION_FAILED {
            // A lost successful PUT response can make the create-only replay
            // fail. Connect still verifies size and digest before completion.
            return Ok(UploadDelivery::AlreadyPresent);
        }
        if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::REQUEST_TIMEOUT || status.is_server_error() {
            return Ok(UploadDelivery::Retry {
                retry_after: retry_after(response.headers(), Utc::now(), self.max_backoff),
            });
        }
        Err(ReportUploadError::UploadRejected { status: status.as_u16() })
    }
}

struct PreparedArchive {
    file: File,
    size: u64,
    sha256: String,
    checksum_base64: String,
}

#[cfg(test)]
async fn prepare_archive(path: &Path, cancellation: &CancellationToken) -> Result<PreparedArchive, ReportUploadError> {
    let file = File::open(path).await.map_err(ReportUploadError::ArchiveOpen)?;
    prepare_file(file, cancellation).await
}

async fn prepare_file(mut file: File, cancellation: &CancellationToken) -> Result<PreparedArchive, ReportUploadError> {
    let metadata = file.metadata().await.map_err(ReportUploadError::ArchiveRead)?;
    if !metadata.is_file() {
        return Err(ReportUploadError::ArchiveType);
    }
    if metadata.len() == 0 || metadata.len() > MAX_SUPPORT_BUNDLE_BYTES {
        return Err(ReportUploadError::ArchiveSize);
    }

    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; UPLOAD_BUFFER_BYTES];
    loop {
        let read = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ReportUploadError::Cancelled),
            result = file.read(&mut buffer) => result.map_err(ReportUploadError::ArchiveRead)?,
        };
        if read == 0 {
            break;
        }
        size = size.checked_add(read as u64).ok_or(ReportUploadError::ArchiveSize)?;
        if size > MAX_SUPPORT_BUNDLE_BYTES {
            return Err(ReportUploadError::ArchiveSize);
        }
        hasher.update(&buffer[..read]);
    }
    if size != metadata.len() {
        return Err(ReportUploadError::ArchiveChanged);
    }
    let current = file.metadata().await.map_err(ReportUploadError::ArchiveRead)?;
    if current.len() != size {
        return Err(ReportUploadError::ArchiveChanged);
    }
    let digest = hasher.finalize();
    Ok(PreparedArchive {
        file,
        size,
        sha256: faster_hex::hex_string(&digest),
        checksum_base64: base64_simd::STANDARD.encode_to_string(digest),
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReserveRequest<'a> {
    protocol_version: &'static str,
    request_id: &'a str,
    bundle_uid: &'a str,
    content_type: &'static str,
    declared_size_bytes: u64,
    declared_sha256: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CompleteRequest<'a> {
    protocol_version: &'static str,
    request_id: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReservationResponse {
    support_bundle: BundleResource,
    upload_authorization: RawUploadAuthorization,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawUploadAuthorization {
    method: String,
    url: String,
    headers: HashMap<String, String>,
    expire_time: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BundleResource {
    name: String,
    uid: String,
    state: String,
    declared_size_bytes: u64,
    declared_sha256: String,
    expire_time: String,
    create_time: String,
    update_time: String,
}

struct Reservation {
    authorization: UploadAuthorization,
}

struct UploadAuthorization {
    url: Url,
    headers: HeaderMap,
}

enum UploadDelivery {
    Accepted,
    AlreadyPresent,
    Retry { retry_after: Option<Duration> },
}

fn decode_reservation(
    body: &[u8],
    expected_name: &str,
    expected_uid: &str,
    archive: &PreparedArchive,
) -> Result<Reservation, ReportUploadError> {
    let response: ReservationResponse = serde_json::from_slice(body).map_err(|_| ReportUploadError::Response)?;
    validate_resource(&response.support_bundle, "PENDING", expected_name, expected_uid, archive)?;
    if response.upload_authorization.method != "PUT" || !is_short_lived_future_instant(&response.upload_authorization.expire_time)
    {
        return Err(ReportUploadError::UploadAuthorization);
    }
    let url = Url::parse(&response.upload_authorization.url).map_err(|_| ReportUploadError::UploadAuthorization)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || url.cannot_be_a_base()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(ReportUploadError::UploadAuthorization);
    }
    let headers = validate_headers(response.upload_authorization.headers, archive)?;
    Ok(Reservation {
        authorization: UploadAuthorization { url, headers },
    })
}

fn decode_receipt(
    body: &[u8],
    expected_name: &str,
    expected_uid: &str,
    archive: &PreparedArchive,
) -> Result<ReportUploadReceipt, ReportUploadError> {
    let response: BundleResource = serde_json::from_slice(body).map_err(|_| ReportUploadError::Response)?;
    validate_resource(&response, "UPLOADED", expected_name, expected_uid, archive)?;
    Ok(ReportUploadReceipt {
        name: response.name,
        uid: response.uid,
        state: response.state,
        declared_size_bytes: response.declared_size_bytes,
        declared_sha256: response.declared_sha256,
    })
}

fn validate_resource(
    resource: &BundleResource,
    expected_state: &str,
    expected_name: &str,
    expected_uid: &str,
    archive: &PreparedArchive,
) -> Result<(), ReportUploadError> {
    if resource.name != expected_name
        || resource.uid != expected_uid
        || resource.state != expected_state
        || resource.declared_size_bytes != archive.size
        || resource.declared_sha256 != archive.sha256
        || !is_exact_utc_seconds(&resource.expire_time)
        || !is_exact_utc_seconds(&resource.create_time)
        || !is_exact_utc_seconds(&resource.update_time)
    {
        return Err(ReportUploadError::Response);
    }
    Ok(())
}

fn validate_headers(raw: HashMap<String, String>, archive: &PreparedArchive) -> Result<HeaderMap, ReportUploadError> {
    let mut headers = HeaderMap::new();
    let mut names = HashSet::new();
    for (name, value) in raw {
        let normalized = name.to_ascii_lowercase();
        if !names.insert(normalized.clone()) || forbidden_header(&normalized) {
            return Err(ReportUploadError::UploadAuthorization);
        }
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| ReportUploadError::UploadAuthorization)?;
        let value = HeaderValue::from_str(&value).map_err(|_| ReportUploadError::UploadAuthorization)?;
        headers.insert(name, value);
    }
    let expected_length = archive.size.to_string();
    let encryption_authorized = match headers
        .get("x-amz-server-side-encryption")
        .and_then(|value| value.to_str().ok())
    {
        Some("AES256") => true,
        Some("aws:kms") => headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| !value.trim().is_empty()),
        _ => false,
    };
    if headers.get(header::CONTENT_TYPE).and_then(|value| value.to_str().ok()) != Some(CONTENT_TYPE)
        || headers.get(header::CONTENT_LENGTH).and_then(|value| value.to_str().ok()) != Some(expected_length.as_str())
        || headers.get(header::IF_NONE_MATCH).and_then(|value| value.to_str().ok()) != Some("*")
        || headers.get("x-amz-checksum-sha256").and_then(|value| value.to_str().ok()) != Some(archive.checksum_base64.as_str())
        || !encryption_authorized
    {
        return Err(ReportUploadError::UploadAuthorization);
    }
    Ok(headers)
}

fn forbidden_header(name: &str) -> bool {
    matches!(
        name,
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "set-cookie"
            | "host"
            | "connection"
            | "transfer-encoding"
            | "upgrade"
            | "te"
            | "trailer"
    )
}

fn is_short_lived_future_instant(value: &str) -> bool {
    if !is_exact_utc_seconds(value) {
        return false;
    }
    let now = Utc::now();
    DateTime::parse_from_rfc3339(value).is_ok_and(|instant| {
        let instant = instant.with_timezone(&Utc);
        instant > now && (instant - now).to_std().is_ok_and(|duration| duration <= MAX_UPLOAD_TIMEOUT)
    })
}

fn retry_after(headers: &HeaderMap, now: DateTime<Utc>, maximum: Duration) -> Option<Duration> {
    let value = headers.get(header::RETRY_AFTER)?.to_str().ok()?;
    let delay = value.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
        DateTime::parse_from_rfc2822(value)
            .ok()
            .and_then(|at| (at.with_timezone(&Utc) - now).to_std().ok())
    })?;
    Some(delay.min(maximum))
}

async fn sleep_or_cancel(cancellation: &CancellationToken, delay: Duration) -> Result<(), ReportUploadError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ReportUploadError::Cancelled),
        () = tokio::time::sleep(delay) => Ok(()),
    }
}

fn transport_error(error: TelemetryError) -> ReportUploadError {
    match error {
        TelemetryError::Endpoint => ReportUploadError::Endpoint,
        TelemetryError::RootCertificate => ReportUploadError::RootCertificate,
        TelemetryError::ProxyConfiguration => ReportUploadError::ProxyConfiguration,
        TelemetryError::ProxyAuthentication => ReportUploadError::ProxyAuthentication,
        TelemetryError::ProxyRejected => ReportUploadError::ProxyRejected,
        TelemetryError::TlsPeer => ReportUploadError::TlsPeer,
        TelemetryError::Schedule => ReportUploadError::Schedule,
        TelemetryError::NotRegistered => ReportUploadError::NotRegistered,
        TelemetryError::IdentityMissing => ReportUploadError::IdentityMissing,
        TelemetryError::IdentityCertificate => ReportUploadError::IdentityCertificate,
        TelemetryError::CredentialName => ReportUploadError::CredentialName,
        TelemetryError::CredentialExpired => ReportUploadError::CredentialExpired,
        TelemetryError::StateConflict => ReportUploadError::CredentialState,
        TelemetryError::ResponseTooLarge => ReportUploadError::ResponseTooLarge,
        TelemetryError::Url(_)
        | TelemetryError::Transport(_)
        | TelemetryError::Identity(_)
        | TelemetryError::IdentityStore(_)
        | TelemetryError::CredentialStore(_)
        | TelemetryError::CredentialValidation(_) => ReportUploadError::UploadTransport,
    }
}

fn report_bundle_error(error: super::report_bundle::ReportBundleError) -> ReportUploadError {
    use super::report_bundle::ReportBundleError;

    match error {
        ReportBundleError::Expired => ReportUploadError::DiagnosticExpired,
        ReportBundleError::IdentityMissing => ReportUploadError::IdentityMissing,
        ReportBundleError::Io(error) => ReportUploadError::ArchiveRead(error),
        ReportBundleError::Invalid | ReportBundleError::Zip(_) => ReportUploadError::DiagnosticArchive,
    }
}

/// Safe, credential-redacted report upload failures.
#[derive(Debug, thiserror::Error)]
pub enum ReportUploadError {
    #[error("Connect report upload timeout must be from one second through fifteen minutes")]
    UploadTimeout,
    #[error("Connect report upload endpoint must be an HTTPS base URL without credentials, query, or fragment")]
    Endpoint,
    #[error("Connect report upload root CA configuration is invalid")]
    RootCertificate,
    #[error("Connect report upload proxy configuration is invalid")]
    ProxyConfiguration,
    #[error("Connect proxy authentication failed; verify the configured proxy credential files")]
    ProxyAuthentication,
    #[error("Connect proxy connection failed; verify proxy availability, credentials, and the approved targets")]
    ProxyRejected,
    #[error("Connect TLS peer certificate validation failed; verify the endpoint and configured root CA")]
    TlsPeer,
    #[error("Connect report upload retry schedule is invalid")]
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
    #[error("Connect report upload response exceeded 64 KiB")]
    ResponseTooLarge,
    #[error("the report archive could not be opened")]
    ArchiveOpen(#[source] std::io::Error),
    #[error("the report archive could not be read")]
    ArchiveRead(#[source] std::io::Error),
    #[error("the report archive must be a regular file")]
    ArchiveType,
    #[error("the report archive must contain 1 byte through 256 MiB")]
    ArchiveSize,
    #[error("the report archive changed while its digest was calculated")]
    ArchiveChanged,
    #[error("the signed diagnostic archive is invalid")]
    DiagnosticArchive,
    #[error("the signed diagnostic archive has expired")]
    DiagnosticExpired,
    #[error("Connect returned an invalid report upload response")]
    Response,
    #[error("Connect returned an invalid or expired report upload authorization")]
    UploadAuthorization,
    #[error("Connect authentication stopped report upload with HTTP {status}")]
    AuthenticationStopped { status: u16 },
    #[error("Connect rejected report upload control request with HTTP {status}")]
    ControlRejected { status: u16 },
    #[error("the report object upload was rejected with HTTP {status}")]
    UploadRejected { status: u16 },
    #[error("the report object upload transport failed")]
    UploadTransport,
    #[error("Connect report upload exhausted its bounded retries")]
    RetryExhausted,
    #[error("Connect report upload was cancelled")]
    Cancelled,
}

#[cfg(test)]
mod tests {
    use chrono::SecondsFormat;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn prepares_the_exact_bounded_archive() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("bundle.tar.zst");
        tokio::fs::write(&path, b"redacted support bundle")
            .await
            .expect("write bundle");

        let archive = prepare_archive(&path, &CancellationToken::new())
            .await
            .expect("prepare archive");

        assert_eq!(archive.size, 23);
        let digest = Sha256::digest(b"redacted support bundle");
        assert_eq!(archive.sha256, faster_hex::hex_string(&digest));
        assert_eq!(archive.checksum_base64, base64_simd::STANDARD.encode_to_string(digest));
    }

    #[tokio::test]
    async fn cancellation_stops_archive_preparation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("bundle.tar.zst");
        tokio::fs::write(&path, b"bundle").await.expect("write bundle");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        assert!(matches!(prepare_archive(&path, &cancellation).await, Err(ReportUploadError::Cancelled)));
    }

    #[tokio::test]
    async fn reservation_is_bound_to_archive_and_safe_headers() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("bundle.tar.zst");
        tokio::fs::write(&path, b"bundle").await.expect("write bundle");
        let archive = prepare_archive(&path, &CancellationToken::new())
            .await
            .expect("prepare archive");
        let bundle_uid = Uuid::now_v7().to_string();
        let name = format!("organizations/o/clusters/c/supportBundles/{bundle_uid}");
        let now = Utc::now();
        let instant = |minutes| (now + chrono::Duration::minutes(minutes)).to_rfc3339_opts(SecondsFormat::Secs, true);
        let response = json!({
            "supportBundle": {
                "name": &name,
                "uid": &bundle_uid,
                "state": "PENDING",
                "declaredSizeBytes": archive.size,
                "declaredSha256": &archive.sha256,
                "expireTime": instant(60),
                "createTime": now.to_rfc3339_opts(SecondsFormat::Secs, true),
                "updateTime": now.to_rfc3339_opts(SecondsFormat::Secs, true)
            },
            "uploadAuthorization": {
                "method": "PUT",
                "url": "https://objects.example.test/upload?signature=hidden",
                "headers": {
                    "Content-Type": CONTENT_TYPE,
                    "Content-Length": archive.size.to_string(),
                    "If-None-Match": "*",
                    "x-amz-checksum-sha256": &archive.checksum_base64,
                    "x-amz-server-side-encryption": "AES256"
                },
                "expireTime": instant(5)
            }
        });

        decode_reservation(&serde_json::to_vec(&response).expect("response JSON"), &name, &bundle_uid, &archive)
            .expect("valid reservation");

        let mut kms_response = response.clone();
        kms_response["uploadAuthorization"]["headers"]["x-amz-server-side-encryption"] = json!("aws:kms");
        kms_response["uploadAuthorization"]["headers"]["x-amz-server-side-encryption-aws-kms-key-id"] =
            json!("connect-support-bundles");
        decode_reservation(&serde_json::to_vec(&kms_response).expect("response JSON"), &name, &bundle_uid, &archive)
            .expect("valid KMS reservation");

        kms_response["uploadAuthorization"]["headers"]
            .as_object_mut()
            .expect("headers object")
            .remove("x-amz-server-side-encryption-aws-kms-key-id");
        assert!(matches!(
            decode_reservation(&serde_json::to_vec(&kms_response).expect("response JSON"), &name, &bundle_uid, &archive,),
            Err(ReportUploadError::UploadAuthorization)
        ));

        let mut wrong_target = response.clone();
        wrong_target["supportBundle"]["name"] = json!(format!("organizations/o/clusters/other/supportBundles/{bundle_uid}"));
        assert!(matches!(
            decode_reservation(&serde_json::to_vec(&wrong_target).expect("response JSON"), &name, &bundle_uid, &archive,),
            Err(ReportUploadError::Response)
        ));

        let mut unsafe_response = response;
        unsafe_response["uploadAuthorization"]["headers"]["Authorization"] = json!("secret");
        assert!(matches!(
            decode_reservation(
                &serde_json::to_vec(&unsafe_response).expect("response JSON"),
                &name,
                &bundle_uid,
                &archive,
            ),
            Err(ReportUploadError::UploadAuthorization)
        ));
    }
}
