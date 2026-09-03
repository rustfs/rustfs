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

//! Outbound client for an on-demand migration source bucket.
//!
//! `SourceClient` wraps an `aws_sdk_s3::Client` built through the shared
//! remote builder and exposes the read-only surface the migration path
//! needs (HEAD, ranged streaming GET, ListObjectsV2, GetObjectTagging, a
//! probe for admin validation). Every request carries the
//! `source-proxy-request` anti-loop marker in both the `x-rustfs-` and
//! `x-minio-` prefixes so a RustFS/MinIO source answers locally instead of
//! proxying the miss back, and the SDK `User-Agent` is suffixed with
//! `RustFS-OnDemandMigration/<version>` for source-side log attribution.
//! Client-supplied `If-*`, `Authorization`, `Host` and SSE-C headers are never
//! forwarded: v1 rejects SSE-C source objects outright.

use crate::bucket::remote_s3_client::{
    PathStyle, RemoteCredentials, RemoteS3ClientError, RemoteS3EndpointSpec, RemoteS3RetryPolicy, build_remote_s3_config,
};
use crate::storage_api_contracts::range::HTTPRangeSpec;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::{ByteStream, DateTime as SdkDateTime};
use aws_sdk_s3::types::{Object as SdkObject, ServerSideEncryption};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use aws_smithy_types::error::display::DisplayErrorContext;
use http::HeaderMap;
use rustfs_utils::http::{SUFFIX_SOURCE_PROXY_REQUEST, insert_header};
use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU64;
use std::time::{Duration, SystemTime};
use url::Url;

/// Appended to the SDK `User-Agent` on every source request.
pub const USER_AGENT_SUFFIX: &str = concat!("RustFS-OnDemandMigration/", env!("CARGO_PKG_VERSION"));

/// Source provider family; drives the `PathStyle::Auto` decision.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SourceProvider {
    Aws,
    Gcs,
    R2,
    Minio,
    Rustfs,
    /// Generic S3-compatible service.
    #[default]
    S3,
}

impl SourceProvider {
    pub fn from_label(label: &str) -> Option<Self> {
        match label.trim().to_ascii_lowercase().as_str() {
            "aws" => Some(Self::Aws),
            "gcs" => Some(Self::Gcs),
            "r2" => Some(Self::R2),
            "minio" => Some(Self::Minio),
            "rustfs" => Some(Self::Rustfs),
            "s3" => Some(Self::S3),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::Gcs => "gcs",
            Self::R2 => "r2",
            Self::Minio => "minio",
            Self::Rustfs => "rustfs",
            Self::S3 => "s3",
        }
    }

    fn prefers_virtual_host(self) -> bool {
        matches!(self, Self::Aws | Self::Gcs | Self::R2)
    }
}

/// Resolves `PathStyle::Auto` for a source: IP-literal or `localhost`
/// endpoints cannot carry a bucket subdomain and always use path-style;
/// otherwise AWS/GCS/R2 use virtual-host addressing and MinIO/RustFS/generic
/// S3 use path-style. Explicit choices pass through unchanged.
pub fn resolve_path_style(path_style: PathStyle, provider: SourceProvider, endpoint_host: &str) -> PathStyle {
    match path_style {
        PathStyle::Auto => {
            if host_is_ip_or_localhost(endpoint_host) || !provider.prefers_virtual_host() {
                PathStyle::Path
            } else {
                PathStyle::VirtualHost
            }
        }
        explicit => explicit,
    }
}

fn host_is_ip_or_localhost(host: &str) -> bool {
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    bare.eq_ignore_ascii_case("localhost") || bare.parse::<std::net::IpAddr>().is_ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceTimeouts {
    pub connect: Duration,
    pub read: Duration,
}

impl Default for SourceTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            read: Duration::from_secs(60),
        }
    }
}

/// Plain description of a source bucket; ODM-05 converts the persisted
/// bucket configuration into this shape.
#[derive(Clone, Debug)]
pub struct SourceClientSpec {
    /// `scheme://host[:port]` with no path, query or userinfo.
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    /// Prepended to every local key when addressing the source; `None` or
    /// empty means the local namespace maps 1:1 onto the source bucket.
    pub source_prefix: Option<String>,
    pub provider: SourceProvider,
    pub path_style: PathStyle,
    pub credentials: Option<RemoteCredentials>,
    pub skip_tls_verify: bool,
    pub ca_cert_pem: Option<String>,
    pub timeouts: SourceTimeouts,
    /// Wire requests one logical source call may cost. The pull pipeline and
    /// the backfill job own the retry budget (`pull.rs` `PULL_MAX_RETRIES`,
    /// `backfill.rs` `LIST_MAX_RETRIES`) and the breaker counts logical calls,
    /// so ODM declares [`RemoteS3RetryPolicy::Disabled`] and keeps one counted
    /// failure equal to one request against a struggling source.
    pub retry: RemoteS3RetryPolicy,
    /// Bytes per second the pull pipeline may consume from this source;
    /// `None` means unlimited. Enforced by the consumer, not by this client.
    pub bandwidth_limit: Option<NonZeroU64>,
}

impl SourceClientSpec {
    fn endpoint_spec(&self) -> Result<RemoteS3EndpointSpec, RemoteS3ClientError> {
        let url = Url::parse(self.endpoint.trim()).map_err(|err| RemoteS3ClientError::InvalidEndpoint(err.to_string()))?;
        let secure = match url.scheme() {
            "https" => true,
            "http" => false,
            other => {
                return Err(RemoteS3ClientError::InvalidEndpoint(format!(
                    "unsupported scheme {other}; expected http or https"
                )));
            }
        };
        let Some(host) = url.host_str() else {
            return Err(RemoteS3ClientError::InvalidEndpoint("endpoint has no host".to_string()));
        };
        if !url.username().is_empty() || url.password().is_some() {
            return Err(RemoteS3ClientError::InvalidEndpoint("endpoint must not carry userinfo".to_string()));
        }
        if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
            return Err(RemoteS3ClientError::InvalidEndpoint(
                "endpoint must be an origin without path, query or fragment".to_string(),
            ));
        }
        let endpoint = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };

        Ok(RemoteS3EndpointSpec {
            endpoint,
            secure,
            region: self.region.clone(),
            path_style: resolve_path_style(self.path_style, self.provider, host),
            credentials: self.credentials.clone(),
            skip_tls_verify: self.skip_tls_verify,
            ca_cert_pem: self.ca_cert_pem.clone(),
            connect_timeout: Some(self.timeouts.connect),
            read_timeout: Some(self.timeouts.read),
            retry: self.retry,
            user_agent_suffix: USER_AGENT_SUFFIX,
        })
    }
}

/// Failure classes of a source request. Variants carry a rendered message
/// rather than the SDK error so callers stay independent of the operation
/// error types; `class_label` is stable for metrics.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("source object not found")]
    NotFound,
    #[error("source denied access")]
    AccessDenied,
    #[error("source throttled the request")]
    Throttled,
    #[error("source request timed out")]
    Timeout,
    #[error("failed to connect to source: {0}")]
    Connect(String),
    #[error("source returned server error {0}")]
    ServerError(u16),
    #[error("unsupported source object: {0}")]
    Unsupported(String),
    #[error("source request failed: {0}")]
    Other(String),
}

impl SourceError {
    /// Transient classes a caller may retry (subject to its own budget).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            SourceError::Throttled | SourceError::Timeout | SourceError::Connect(_) | SourceError::ServerError(_)
        )
    }

    pub fn class_label(&self) -> &'static str {
        match self {
            SourceError::NotFound => "not_found",
            SourceError::AccessDenied => "access_denied",
            SourceError::Throttled => "throttled",
            SourceError::Timeout => "timeout",
            SourceError::Connect(_) => "connect",
            SourceError::ServerError(_) => "server_error",
            SourceError::Unsupported(_) => "unsupported",
            SourceError::Other(_) => "other",
        }
    }
}

const THROTTLE_CODES: &[&str] = &[
    "SlowDown",
    "Throttling",
    "ThrottlingException",
    "RequestLimitExceeded",
    "TooManyRequests",
    "RequestThrottled",
];
const NOT_FOUND_CODES: &[&str] = &["NoSuchKey", "NotFound", "NoSuchBucket", "NoSuchVersion"];
const ACCESS_DENIED_CODES: &[&str] = &[
    "AccessDenied",
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
    "AllAccessDisabled",
    "ExpiredToken",
    "InvalidToken",
];

fn classify_status(status: u16, code: Option<&str>, message: String) -> SourceError {
    if let Some(code) = code {
        if THROTTLE_CODES.contains(&code) {
            return SourceError::Throttled;
        }
        if NOT_FOUND_CODES.contains(&code) {
            return SourceError::NotFound;
        }
        if ACCESS_DENIED_CODES.contains(&code) {
            return SourceError::AccessDenied;
        }
    }
    match status {
        404 => SourceError::NotFound,
        401 | 403 => SourceError::AccessDenied,
        429 | 503 => SourceError::Throttled,
        500..=599 => SourceError::ServerError(status),
        _ => SourceError::Other(message),
    }
}

fn classify_sdk_error<E>(err: SdkError<E, HttpResponse>) -> SourceError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let message = format!("{}", DisplayErrorContext(&err));
    match &err {
        SdkError::TimeoutError(_) => SourceError::Timeout,
        SdkError::DispatchFailure(failure) => {
            if failure.is_timeout() {
                SourceError::Timeout
            } else if failure.is_io() {
                SourceError::Connect(message)
            } else {
                SourceError::Other(message)
            }
        }
        SdkError::ConstructionFailure(_) => SourceError::Other(message),
        SdkError::ResponseError(response) => classify_status(response.raw().status().as_u16(), None, message),
        SdkError::ServiceError(service) => classify_status(service.raw().status().as_u16(), err.code(), message),
        _ => SourceError::Other(message),
    }
}

/// Server-side encryption the source reports for an object. Recognized only:
/// the write-back path stores plaintext bytes the source already decrypted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceSse {
    S3,
    Kms { key_id: Option<String> },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceHead {
    /// ETag with surrounding quotes stripped.
    pub etag: Option<String>,
    /// `Content-Length` of the response: the object size for HEAD and
    /// unranged GET, the range length for a ranged GET.
    pub size: u64,
    pub last_modified: Option<SystemTime>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub cache_control: Option<String>,
    pub expires: Option<String>,
    /// `x-amz-meta-*` values keyed without the prefix, matching the stored
    /// user-metadata shape.
    pub user_metadata: HashMap<String, String>,
    pub version_id: Option<String>,
    pub storage_class: Option<String>,
    pub sse: Option<SourceSse>,
    pub is_multipart_etag: bool,
}

/// Per-operation fields shared by HEAD and GET outputs.
struct HeadParts {
    etag: Option<String>,
    content_length: Option<i64>,
    last_modified: Option<SdkDateTime>,
    content_type: Option<String>,
    content_encoding: Option<String>,
    content_disposition: Option<String>,
    content_language: Option<String>,
    cache_control: Option<String>,
    expires: Option<String>,
    metadata: Option<HashMap<String, String>>,
    version_id: Option<String>,
    storage_class: Option<String>,
    server_side_encryption: Option<ServerSideEncryption>,
    ssekms_key_id: Option<String>,
    sse_customer_algorithm: Option<String>,
}

fn normalize_etag(etag: Option<String>) -> Option<String> {
    etag.map(|etag| etag.trim().trim_matches('"').to_string())
        .filter(|etag| !etag.is_empty())
}

/// Multipart ETags end in `-<part count>`; single-part ETags are bare MD5.
pub fn is_multipart_etag(etag: &str) -> bool {
    etag.rsplit_once('-')
        .is_some_and(|(_, parts)| !parts.is_empty() && parts.bytes().all(|b| b.is_ascii_digit()))
}

fn system_time(value: Option<SdkDateTime>) -> Option<SystemTime> {
    value.and_then(|value| SystemTime::try_from(value).ok())
}

fn source_head(parts: HeadParts) -> Result<SourceHead, SourceError> {
    if parts.sse_customer_algorithm.is_some() {
        return Err(SourceError::Unsupported(
            "source object is encrypted with SSE-C; customer-key sources are not supported".to_string(),
        ));
    }
    let size = parts
        .content_length
        .and_then(|length| u64::try_from(length).ok())
        .ok_or_else(|| SourceError::Other("source response has no valid content-length".to_string()))?;
    let etag = normalize_etag(parts.etag);
    let is_multipart_etag = etag.as_deref().is_some_and(is_multipart_etag);
    let sse = parts.server_side_encryption.map(|sse| match sse {
        ServerSideEncryption::Aes256 => SourceSse::S3,
        _ => SourceSse::Kms {
            key_id: parts.ssekms_key_id,
        },
    });

    Ok(SourceHead {
        etag,
        size,
        last_modified: system_time(parts.last_modified),
        content_type: parts.content_type,
        content_encoding: parts.content_encoding,
        content_disposition: parts.content_disposition,
        content_language: parts.content_language,
        cache_control: parts.cache_control,
        expires: parts.expires,
        user_metadata: parts.metadata.unwrap_or_default(),
        version_id: parts.version_id,
        storage_class: parts.storage_class,
        sse,
        is_multipart_etag,
    })
}

fn source_head_from_head_output(output: HeadObjectOutput) -> Result<SourceHead, SourceError> {
    source_head(HeadParts {
        etag: output.e_tag,
        content_length: output.content_length,
        last_modified: output.last_modified,
        content_type: output.content_type,
        content_encoding: output.content_encoding,
        content_disposition: output.content_disposition,
        content_language: output.content_language,
        cache_control: output.cache_control,
        expires: output.expires_string,
        metadata: output.metadata,
        version_id: output.version_id,
        storage_class: output.storage_class.map(|class| class.as_str().to_string()),
        server_side_encryption: output.server_side_encryption,
        ssekms_key_id: output.ssekms_key_id,
        sse_customer_algorithm: output.sse_customer_algorithm,
    })
}

/// Ranged/unranged GET response: `head` describes the returned bytes.
pub struct SourceGet {
    pub head: SourceHead,
    pub body: ByteStream,
    /// `Content-Range` of a ranged response (`bytes a-b/total`).
    pub content_range: Option<String>,
}

impl fmt::Debug for SourceGet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceGet")
            .field("head", &self.head)
            .field("content_range", &self.content_range)
            .finish_non_exhaustive()
    }
}

fn source_get_from_output(output: GetObjectOutput) -> Result<SourceGet, SourceError> {
    let content_range = output.content_range;
    let body = output.body;
    let head = source_head(HeadParts {
        etag: output.e_tag,
        content_length: output.content_length,
        last_modified: output.last_modified,
        content_type: output.content_type,
        content_encoding: output.content_encoding,
        content_disposition: output.content_disposition,
        content_language: output.content_language,
        cache_control: output.cache_control,
        expires: output.expires_string,
        metadata: output.metadata,
        version_id: output.version_id,
        storage_class: output.storage_class.map(|class| class.as_str().to_string()),
        server_side_encryption: output.server_side_encryption,
        ssekms_key_id: output.ssekms_key_id,
        sse_customer_algorithm: output.sse_customer_algorithm,
    })?;
    Ok(SourceGet {
        head,
        body,
        content_range,
    })
}

/// Renders an `HTTPRangeSpec` as the `Range` header value sent to the source.
pub fn range_header_value(range: &HTTPRangeSpec) -> Result<String, SourceError> {
    if range.is_suffix_length {
        let suffix = range.start.unsigned_abs();
        if suffix == 0 {
            return Err(SourceError::Other("invalid range: zero suffix length".to_string()));
        }
        return Ok(format!("bytes=-{suffix}"));
    }
    if range.start < 0 {
        return Err(SourceError::Other("invalid range: negative start".to_string()));
    }
    match range.end {
        -1 => Ok(format!("bytes={}-", range.start)),
        end if end >= range.start => Ok(format!("bytes={}-{end}", range.start)),
        _ => Err(SourceError::Other("invalid range: end precedes start".to_string())),
    }
}

/// One listing entry, keyed in the local namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceObject {
    pub key: String,
    pub etag: Option<String>,
    pub size: u64,
    pub last_modified: Option<SystemTime>,
    pub storage_class: Option<String>,
    pub is_multipart_etag: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourcePage {
    pub objects: Vec<SourceObject>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

/// Result of [`SourceClient::probe`]: the bucket answered HEAD and a
/// one-key listing succeeded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceProbe {
    pub sample_object: Option<SourceObject>,
    pub has_more_objects: bool,
}

/// Adds the `source-proxy-request` anti-loop markers before signing so they
/// join the SigV4 canonical request (same shape as the replication proxy).
#[derive(Debug)]
struct SourceProxyMarkerInterceptor {
    headers: HeaderMap,
}

impl SourceProxyMarkerInterceptor {
    fn new() -> Self {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_PROXY_REQUEST, "true");
        Self { headers }
    }
}

impl Intercept for SourceProxyMarkerInterceptor {
    fn name(&self) -> &'static str {
        "RustfsSourceProxyMarker"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request_headers = context.request_mut().headers_mut();
        for (name, value) in &self.headers {
            request_headers.try_insert(name.clone(), value.clone())?;
        }
        Ok(())
    }
}

pub struct SourceClient {
    client: S3Client,
    endpoint: String,
    bucket: String,
    source_prefix: Option<String>,
    timeouts: SourceTimeouts,
    bandwidth_limit: Option<NonZeroU64>,
}

impl fmt::Debug for SourceClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceClient")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("source_prefix", &self.source_prefix)
            .field("timeouts", &self.timeouts)
            .field("bandwidth_limit", &self.bandwidth_limit)
            .finish_non_exhaustive()
    }
}

impl SourceClient {
    pub async fn new(spec: &SourceClientSpec) -> Result<Self, RemoteS3ClientError> {
        let endpoint = spec.endpoint_spec()?;
        let config = build_remote_s3_config(&endpoint).await?;
        Ok(Self::from_config_builder(config, endpoint.endpoint_url(), spec))
    }

    /// `config` must come from [`SourceClientSpec::endpoint_spec`], which is
    /// where the retry policy that keeps one logical call equal to one wire
    /// request is declared.
    fn from_config_builder(config: aws_sdk_s3::config::Builder, endpoint: String, spec: &SourceClientSpec) -> Self {
        let client = S3Client::from_conf(config.interceptor(SourceProxyMarkerInterceptor::new()).build());
        Self {
            client,
            endpoint,
            bucket: spec.bucket.clone(),
            source_prefix: spec.source_prefix.clone().filter(|prefix| !prefix.is_empty()),
            timeouts: spec.timeouts,
            bandwidth_limit: spec.bandwidth_limit,
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn source_prefix(&self) -> Option<&str> {
        self.source_prefix.as_deref()
    }

    pub fn timeouts(&self) -> SourceTimeouts {
        self.timeouts
    }

    pub fn bandwidth_limit(&self) -> Option<NonZeroU64> {
        self.bandwidth_limit
    }

    /// Source-side key for a local key.
    pub fn source_key(&self, local_key: &str) -> String {
        match &self.source_prefix {
            Some(prefix) => format!("{prefix}{local_key}"),
            None => local_key.to_string(),
        }
    }

    /// Local key for a source key; `None` when the key lies outside the
    /// configured prefix.
    pub fn local_key<'a>(&self, source_key: &'a str) -> Option<&'a str> {
        match &self.source_prefix {
            Some(prefix) => source_key.strip_prefix(prefix.as_str()),
            None => Some(source_key),
        }
    }

    pub async fn head_object(&self, key: &str) -> Result<SourceHead, SourceError> {
        let output = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.source_key(key))
            .send()
            .await
            .map_err(classify_sdk_error)?;
        source_head_from_head_output(output)
    }

    /// Streams the object; `range` is passed through as an HTTP `Range`
    /// header and omitted entirely when `None`.
    pub async fn get_object(&self, key: &str, range: Option<&HTTPRangeSpec>) -> Result<SourceGet, SourceError> {
        let range = range.map(range_header_value).transpose()?;
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.source_key(key))
            .set_range(range)
            .send()
            .await
            .map_err(classify_sdk_error)?;
        source_get_from_output(output)
    }

    /// Lists one page under the local `prefix`. Keys are returned in the
    /// local namespace; entries outside `source_prefix` are skipped.
    pub async fn list_objects_v2(
        &self,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: i32,
    ) -> Result<SourcePage, SourceError> {
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(self.source_key(prefix.unwrap_or_default()))
            .set_continuation_token(continuation_token.map(str::to_string))
            .max_keys(max_keys)
            .send()
            .await
            .map_err(classify_sdk_error)?;

        let is_truncated = output.is_truncated.unwrap_or(false);
        let next_continuation_token = output.next_continuation_token;
        if is_truncated && next_continuation_token.is_none() {
            return Err(SourceError::Other(
                "source reported a truncated listing without a continuation token".to_string(),
            ));
        }
        let objects = output
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| self.source_object(object))
            .collect();

        Ok(SourcePage {
            objects,
            is_truncated,
            next_continuation_token,
        })
    }

    fn source_object(&self, object: SdkObject) -> Option<SourceObject> {
        let key = self.local_key(object.key.as_deref()?)?.to_string();
        let etag = normalize_etag(object.e_tag);
        let is_multipart_etag = etag.as_deref().is_some_and(is_multipart_etag);
        Some(SourceObject {
            key,
            etag,
            size: object.size.and_then(|size| u64::try_from(size).ok()).unwrap_or(0),
            last_modified: system_time(object.last_modified),
            storage_class: object.storage_class.map(|class| class.as_str().to_string()),
            is_multipart_etag,
        })
    }

    pub async fn get_object_tagging(&self, key: &str) -> Result<HashMap<String, String>, SourceError> {
        let output = self
            .client
            .get_object_tagging()
            .bucket(&self.bucket)
            .key(self.source_key(key))
            .send()
            .await
            .map_err(classify_sdk_error)?;
        Ok(output.tag_set.into_iter().map(|tag| (tag.key, tag.value)).collect())
    }

    /// Admin validation: HeadBucket plus a one-key listing under the prefix.
    pub async fn probe(&self) -> Result<SourceProbe, SourceError> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(classify_sdk_error)?;
        let page = self.list_objects_v2(None, None, 1).await?;
        Ok(SourceProbe {
            sample_object: page.objects.into_iter().next(),
            has_more_objects: page.is_truncated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn};
    use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_runtime_api::http::StatusCode as SmithyStatusCode;
    use aws_smithy_types::body::SdkBody;
    use proptest::prelude::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug)]
    struct RecordedRequest {
        method: String,
        uri: String,
        headers: Vec<(String, String)>,
    }

    impl RecordedRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        }
    }

    #[derive(Clone, Debug)]
    enum Scripted {
        Response {
            status: u16,
            headers: Vec<(&'static str, String)>,
            body: Vec<u8>,
        },
        Io,
        Timeout,
    }

    fn ok(headers: Vec<(&'static str, String)>, body: &str) -> Scripted {
        Scripted::Response {
            status: 200,
            headers,
            body: body.as_bytes().to_vec(),
        }
    }

    fn status(status: u16, body: &str) -> Scripted {
        Scripted::Response {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        }
    }

    type Recorded = Arc<Mutex<Vec<RecordedRequest>>>;

    #[derive(Clone, Debug)]
    struct ScriptedConnector {
        requests: Recorded,
        responses: Arc<Mutex<VecDeque<Scripted>>>,
    }

    impl HttpConnector for ScriptedConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            self.requests
                .lock()
                .expect("recorded request lock should not be poisoned")
                .push(RecordedRequest {
                    method: request.method().to_string(),
                    uri: request.uri().to_string(),
                    headers: request
                        .headers()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                });
            let next = self
                .responses
                .lock()
                .expect("scripted response lock should not be poisoned")
                .pop_front()
                .expect("test script must provide a response for every request");
            match next {
                Scripted::Response { status, headers, body } => {
                    let mut response = HttpResponse::new(
                        SmithyStatusCode::try_from(status).expect("scripted status should be valid"),
                        SdkBody::from(body),
                    );
                    for (name, value) in headers {
                        response.headers_mut().insert(name, value);
                    }
                    HttpConnectorFuture::ready(Ok(response))
                }
                Scripted::Io => HttpConnectorFuture::ready(Err(ConnectorError::io("connection refused".into()))),
                Scripted::Timeout => HttpConnectorFuture::ready(Err(ConnectorError::timeout("connect timed out".into()))),
            }
        }
    }

    fn spec(source_prefix: Option<&str>) -> SourceClientSpec {
        SourceClientSpec {
            endpoint: "https://source.example.com".to_string(),
            region: "us-east-1".to_string(),
            bucket: "source-bucket".to_string(),
            source_prefix: source_prefix.map(str::to_string),
            provider: SourceProvider::Minio,
            path_style: PathStyle::Auto,
            credentials: Some(RemoteCredentials {
                access_key: "access".to_string(),
                secret_key: "very-secret".to_string(),
                session_token: Some("session-token".to_string()),
                expiration: None,
                account_id: String::new(),
            }),
            skip_tls_verify: false,
            ca_cert_pem: None,
            retry: RemoteS3RetryPolicy::Disabled,
            timeouts: SourceTimeouts::default(),
            bandwidth_limit: NonZeroU64::new(1_000_000),
        }
    }

    async fn scripted_client(spec: &SourceClientSpec, responses: Vec<Scripted>) -> (SourceClient, Recorded) {
        let requests: Recorded = Arc::new(Mutex::new(Vec::new()));
        let connector = SharedHttpConnector::new(ScriptedConnector {
            requests: Arc::clone(&requests),
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        });
        let http_client = http_client_fn(move |_settings, _components| connector.clone());
        let endpoint = spec.endpoint_spec().expect("test spec endpoint should parse");
        let config = build_remote_s3_config(&endpoint)
            .await
            .expect("test spec should build")
            .http_client(http_client);
        (SourceClient::from_config_builder(config, endpoint.endpoint_url(), spec), requests)
    }

    fn recorded(requests: &Recorded) -> Vec<RecordedRequest> {
        requests.lock().expect("recorded request lock should not be poisoned").clone()
    }

    fn assert_outbound_markers(request: &RecordedRequest) {
        assert_eq!(
            request.header("x-rustfs-source-proxy-request"),
            Some("true"),
            "{} {} must carry the rustfs anti-loop marker",
            request.method,
            request.uri
        );
        assert_eq!(
            request.header("x-minio-source-proxy-request"),
            Some("true"),
            "{} {} must carry the minio anti-loop marker",
            request.method,
            request.uri
        );
        let user_agent = request.header("user-agent").expect("SDK request must carry a user-agent");
        assert!(
            user_agent.ends_with(&format!(" {USER_AGENT_SUFFIX}")),
            "user-agent {user_agent} must end with the migration suffix"
        );
        assert!(request.header("authorization").is_some(), "request must be signed");
        assert!(request.header("x-amz-security-token").is_some(), "session token must be signed in");
    }

    fn head_headers() -> Vec<(&'static str, String)> {
        vec![
            ("etag", "\"d41d8cd98f00b204e9800998ecf8427e-3\"".to_string()),
            ("content-length", "1234".to_string()),
            ("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT".to_string()),
            ("content-type", "text/plain".to_string()),
            ("content-encoding", "gzip".to_string()),
            ("content-disposition", "attachment; filename=\"a.txt\"".to_string()),
            ("content-language", "en".to_string()),
            ("cache-control", "max-age=60".to_string()),
            ("expires", "Thu, 01 Jan 2026 00:00:00 GMT".to_string()),
            ("x-amz-meta-owner", "alice".to_string()),
            ("x-amz-meta-tier", "hot".to_string()),
            ("x-amz-version-id", "v1".to_string()),
            ("x-amz-storage-class", "STANDARD_IA".to_string()),
            ("x-amz-server-side-encryption", "aws:kms".to_string()),
            ("x-amz-server-side-encryption-aws-kms-key-id", "key-1".to_string()),
        ]
    }

    #[tokio::test]
    async fn head_object_maps_source_head_fields() {
        let (client, requests) = scripted_client(&spec(Some("data/")), vec![ok(head_headers(), "")]).await;
        let head = client.head_object("dir/obj.txt").await.expect("HEAD should map");

        let requests = recorded(&requests);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "HEAD");
        assert!(
            requests[0]
                .uri
                .starts_with("https://source.example.com/source-bucket/data/dir/obj.txt"),
            "path-style URI with prefix expected, got {}",
            requests[0].uri
        );
        assert_outbound_markers(&requests[0]);

        assert_eq!(head.etag.as_deref(), Some("d41d8cd98f00b204e9800998ecf8427e-3"));
        assert!(head.is_multipart_etag);
        assert_eq!(head.size, 1234);
        assert_eq!(head.last_modified, Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_445_412_480)));
        assert_eq!(head.content_type.as_deref(), Some("text/plain"));
        assert_eq!(head.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(head.content_disposition.as_deref(), Some("attachment; filename=\"a.txt\""));
        assert_eq!(head.content_language.as_deref(), Some("en"));
        assert_eq!(head.cache_control.as_deref(), Some("max-age=60"));
        assert_eq!(head.expires.as_deref(), Some("Thu, 01 Jan 2026 00:00:00 GMT"));
        assert_eq!(
            head.user_metadata,
            HashMap::from([
                ("owner".to_string(), "alice".to_string()),
                ("tier".to_string(), "hot".to_string())
            ])
        );
        assert_eq!(head.version_id.as_deref(), Some("v1"));
        assert_eq!(head.storage_class.as_deref(), Some("STANDARD_IA"));
        assert_eq!(
            head.sse,
            Some(SourceSse::Kms {
                key_id: Some("key-1".to_string())
            })
        );
    }

    #[tokio::test]
    async fn head_object_recognizes_sse_s3_and_single_part_etag() {
        let headers = vec![
            ("etag", "\"d41d8cd98f00b204e9800998ecf8427e\"".to_string()),
            ("content-length", "0".to_string()),
            ("x-amz-server-side-encryption", "AES256".to_string()),
        ];
        let (client, _) = scripted_client(&spec(None), vec![ok(headers, "")]).await;
        let head = client.head_object("obj").await.expect("HEAD should map");
        assert_eq!(head.sse, Some(SourceSse::S3));
        assert!(!head.is_multipart_etag);
        assert_eq!(head.size, 0);
        assert!(head.user_metadata.is_empty());
    }

    #[tokio::test]
    async fn head_object_rejects_sse_c_source_objects() {
        let headers = vec![
            ("etag", "\"abc\"".to_string()),
            ("content-length", "10".to_string()),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256".to_string()),
        ];
        let (client, _) = scripted_client(&spec(None), vec![ok(headers, "")]).await;
        let err = client
            .head_object("obj")
            .await
            .expect_err("SSE-C source objects are unsupported");
        assert!(matches!(err, SourceError::Unsupported(_)), "{err:?}");
        assert_eq!(err.class_label(), "unsupported");
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn get_object_passes_range_through_and_streams_body() {
        let headers = vec![
            ("etag", "\"abc\"".to_string()),
            ("content-length", "5".to_string()),
            ("content-range", "bytes 10-14/100".to_string()),
        ];
        let (client, requests) = scripted_client(&spec(Some("data/")), vec![ok(headers, "hello")]).await;
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 10,
            end: 14,
        };
        let get = client
            .get_object("obj", Some(&range))
            .await
            .expect("ranged GET should succeed");

        let requests = recorded(&requests);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].header("range"), Some("bytes=10-14"));
        assert_outbound_markers(&requests[0]);

        assert_eq!(get.content_range.as_deref(), Some("bytes 10-14/100"));
        assert_eq!(get.head.size, 5);
        let body = get.body.collect().await.expect("body should stream").into_bytes();
        assert_eq!(body.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn get_object_without_range_sends_no_range_header() {
        let headers = vec![("etag", "\"abc\"".to_string()), ("content-length", "5".to_string())];
        let (client, requests) = scripted_client(&spec(None), vec![ok(headers, "hello")]).await;
        let get = client.get_object("obj", None).await.expect("GET should succeed");
        let requests = recorded(&requests);
        assert!(requests[0].header("range").is_none(), "unranged GET must not send Range");
        assert!(get.content_range.is_none());
    }

    #[test]
    fn range_header_value_covers_open_and_suffix_forms() {
        let render = |is_suffix_length, start, end| {
            range_header_value(&HTTPRangeSpec {
                is_suffix_length,
                start,
                end,
            })
        };
        assert_eq!(render(false, 0, 99).expect("closed range"), "bytes=0-99");
        assert_eq!(render(false, 5, -1).expect("open range"), "bytes=5-");
        assert_eq!(render(true, 10, -1).expect("suffix range"), "bytes=-10");
        assert_eq!(render(true, -10, -1).expect("negative suffix range"), "bytes=-10");
        assert!(render(true, 0, -1).is_err());
        assert!(render(false, -1, 5).is_err());
        assert!(render(false, 10, 5).is_err());
    }

    const LIST_PAGE_ONE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>source-bucket</Name>
  <Prefix>data/photos/</Prefix>
  <MaxKeys>2</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>token-1</NextContinuationToken>
  <Contents>
    <Key>data/photos/a.jpg</Key>
    <LastModified>2015-10-21T07:28:00.000Z</LastModified>
    <ETag>&quot;aaaa-2&quot;</ETag>
    <Size>42</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>outside/b.jpg</Key>
    <ETag>&quot;bbbb&quot;</ETag>
    <Size>7</Size>
  </Contents>
</ListBucketResult>"#;

    const LIST_PAGE_TWO: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>source-bucket</Name>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>data/photos/c.jpg</Key>
    <ETag>&quot;cccc&quot;</ETag>
    <Size>1</Size>
  </Contents>
</ListBucketResult>"#;

    const LIST_TRUNCATED_WITHOUT_TOKEN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>source-bucket</Name>
  <IsTruncated>true</IsTruncated>
</ListBucketResult>"#;

    #[tokio::test]
    async fn list_objects_v2_pages_and_strips_source_prefix() {
        let (client, requests) =
            scripted_client(&spec(Some("data/")), vec![ok(Vec::new(), LIST_PAGE_ONE), ok(Vec::new(), LIST_PAGE_TWO)]).await;

        let page = client
            .list_objects_v2(Some("photos/"), None, 2)
            .await
            .expect("first page should list");
        assert!(page.is_truncated);
        assert_eq!(page.next_continuation_token.as_deref(), Some("token-1"));
        assert_eq!(
            page.objects,
            vec![SourceObject {
                key: "photos/a.jpg".to_string(),
                etag: Some("aaaa-2".to_string()),
                size: 42,
                last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_445_412_480)),
                storage_class: Some("STANDARD".to_string()),
                is_multipart_etag: true,
            }],
            "entries outside the source prefix are dropped"
        );

        let page = client
            .list_objects_v2(Some("photos/"), page.next_continuation_token.as_deref(), 2)
            .await
            .expect("second page should list");
        assert!(!page.is_truncated);
        assert!(page.next_continuation_token.is_none());
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "photos/c.jpg");
        assert!(!page.objects[0].is_multipart_etag);

        let requests = recorded(&requests);
        assert_eq!(requests.len(), 2);
        for request in &requests {
            assert_eq!(request.method, "GET");
            assert!(request.uri.contains("list-type=2"), "{}", request.uri);
            assert!(request.uri.contains("prefix=data%2Fphotos%2F"), "{}", request.uri);
            assert!(request.uri.contains("max-keys=2"), "{}", request.uri);
            assert_outbound_markers(request);
        }
        assert!(!requests[0].uri.contains("continuation-token"), "{}", requests[0].uri);
        assert!(requests[1].uri.contains("continuation-token=token-1"), "{}", requests[1].uri);
    }

    #[tokio::test]
    async fn list_objects_v2_rejects_truncated_page_without_token() {
        let (client, _) = scripted_client(&spec(None), vec![ok(Vec::new(), LIST_TRUNCATED_WITHOUT_TOKEN)]).await;
        let err = client
            .list_objects_v2(None, None, 10)
            .await
            .expect_err("truncated page without token is corrupt");
        assert!(matches!(err, SourceError::Other(_)), "{err:?}");
    }

    const TAGGING_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <TagSet>
    <Tag><Key>env</Key><Value>prod</Value></Tag>
    <Tag><Key>team</Key><Value>storage</Value></Tag>
  </TagSet>
</Tagging>"#;

    #[tokio::test]
    async fn get_object_tagging_and_probe_carry_markers_on_every_request() {
        let (client, requests) = scripted_client(
            &spec(Some("data/")),
            vec![
                ok(Vec::new(), TAGGING_BODY),
                ok(Vec::new(), ""),
                ok(Vec::new(), LIST_PAGE_ONE),
            ],
        )
        .await;

        let tags = client.get_object_tagging("obj").await.expect("tagging should parse");
        assert_eq!(
            tags,
            HashMap::from([
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "storage".to_string())
            ])
        );

        let probe = client.probe().await.expect("probe should succeed");
        assert!(probe.has_more_objects);
        assert_eq!(probe.sample_object.as_ref().map(|object| object.key.as_str()), Some("photos/a.jpg"));

        let requests = recorded(&requests);
        assert_eq!(requests.len(), 3);
        assert!(requests[0].uri.contains("tagging"), "{}", requests[0].uri);
        assert_eq!(requests[1].method, "HEAD");
        assert!(requests[2].uri.contains("max-keys=1"), "{}", requests[2].uri);
        for request in &requests {
            assert_outbound_markers(request);
        }
    }

    const SLOW_DOWN_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>"#;
    const ACCESS_DENIED_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"#;

    #[tokio::test]
    async fn source_error_classification_covers_every_class() {
        let cases: Vec<(Scripted, &str, bool)> = vec![
            (status(404, ""), "not_found", false),
            (status(403, ACCESS_DENIED_BODY), "access_denied", false),
            (status(401, ""), "access_denied", false),
            (status(429, ""), "throttled", true),
            (status(503, SLOW_DOWN_BODY), "throttled", true),
            (status(500, ""), "server_error", true),
            (status(502, ""), "server_error", true),
            (Scripted::Io, "connect", true),
            (Scripted::Timeout, "timeout", true),
        ];
        for (scripted, expected_label, retryable) in cases {
            let (client, _) = scripted_client(&spec(None), vec![scripted.clone()]).await;
            let err = match client.get_object("obj", None).await {
                Ok(_) => panic!("{scripted:?} must fail"),
                Err(err) => err,
            };
            assert_eq!(err.class_label(), expected_label, "{scripted:?} -> {err:?}");
            assert_eq!(err.is_retryable(), retryable, "{scripted:?} -> {err:?}");
            if let SourceError::ServerError(code) = &err {
                assert!(matches!(scripted, Scripted::Response { status, .. } if status == *code));
            }
        }

        // HEAD carries no error body, so the classification must work from the
        // status alone as well.
        let (client, _) = scripted_client(&spec(None), vec![status(404, "")]).await;
        assert!(matches!(client.head_object("missing").await, Err(SourceError::NotFound)));
        let (client, _) = scripted_client(&spec(None), vec![status(403, "")]).await;
        assert!(matches!(client.head_object("secret").await, Err(SourceError::AccessDenied)));
    }

    #[tokio::test]
    async fn source_client_debug_redacts_credentials() {
        let (client, _) = scripted_client(&spec(Some("data/")), Vec::new()).await;
        let rendered = format!("{client:?}");
        assert!(rendered.contains("source-bucket"));
        assert!(rendered.contains("data/"));
        assert!(rendered.contains("https://source.example.com"));
        assert!(!rendered.contains("very-secret"));
        assert!(!rendered.contains("session-token"));
        assert!(!rendered.contains("access"), "access key must not be rendered either: {rendered}");
    }

    #[test]
    fn source_client_spec_endpoint_parsing() {
        let mut s = spec(None);
        let endpoint = s.endpoint_spec().expect("https origin should parse");
        assert_eq!(endpoint.endpoint, "source.example.com");
        assert!(endpoint.secure);
        assert_eq!(endpoint.user_agent_suffix, USER_AGENT_SUFFIX);
        assert_eq!(endpoint.connect_timeout, Some(Duration::from_secs(10)));
        assert_eq!(endpoint.read_timeout, Some(Duration::from_secs(60)));
        assert_eq!(endpoint.path_style, PathStyle::Path);

        s.endpoint = "http://[::1]:9000".to_string();
        let endpoint = s.endpoint_spec().expect("bracketed IPv6 origin should parse");
        assert_eq!(endpoint.endpoint, "[::1]:9000");
        assert!(!endpoint.secure);

        for bad in [
            "ftp://source.example.com",
            "https://user:pw@source.example.com",
            "https://source.example.com/bucket",
            "https://source.example.com/?x=1",
            "not a url",
        ] {
            s.endpoint = bad.to_string();
            assert!(
                matches!(s.endpoint_spec(), Err(RemoteS3ClientError::InvalidEndpoint(_))),
                "{bad} must be rejected"
            );
        }
    }

    #[test]
    fn resolve_path_style_auto_follows_provider_and_host() {
        use SourceProvider::*;
        for provider in [Aws, Gcs, R2] {
            assert_eq!(
                resolve_path_style(PathStyle::Auto, provider, "s3.example.com"),
                PathStyle::VirtualHost,
                "{provider:?}"
            );
        }
        for provider in [Minio, Rustfs, S3] {
            assert_eq!(
                resolve_path_style(PathStyle::Auto, provider, "s3.example.com"),
                PathStyle::Path,
                "{provider:?}"
            );
        }
        for host in ["10.0.0.1", "[::1]", "localhost", "LOCALHOST"] {
            assert_eq!(resolve_path_style(PathStyle::Auto, Aws, host), PathStyle::Path, "{host}");
        }
        assert_eq!(resolve_path_style(PathStyle::VirtualHost, Minio, "10.0.0.1"), PathStyle::VirtualHost);
        assert_eq!(resolve_path_style(PathStyle::Path, Aws, "s3.amazonaws.com"), PathStyle::Path);
        assert_eq!(SourceProvider::from_label(" AWS "), Some(Aws));
        assert_eq!(SourceProvider::from_label("azure"), None);
    }

    fn prefix_client(prefix: Option<String>) -> SourceClient {
        SourceClient {
            client: S3Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                    .build(),
            ),
            endpoint: "https://source.example.com".to_string(),
            bucket: "bucket".to_string(),
            source_prefix: prefix.filter(|prefix| !prefix.is_empty()),
            timeouts: SourceTimeouts::default(),
            bandwidth_limit: None,
        }
    }

    proptest! {
        #[test]
        fn source_and_local_keys_round_trip(prefix in proptest::option::of("[a-z0-9/_-]{0,16}"), key in "[a-zA-Z0-9/._ -]{0,32}") {
            let client = prefix_client(prefix.clone());
            let source_key = client.source_key(&key);
            prop_assert_eq!(client.local_key(&source_key), Some(key.as_str()));
            match prefix.as_deref().filter(|prefix| !prefix.is_empty()) {
                Some(prefix) => {
                    prop_assert!(source_key.starts_with(prefix));
                    prop_assert_eq!(&source_key[prefix.len()..], key.as_str());
                }
                None => prop_assert_eq!(source_key.as_str(), key.as_str()),
            }
        }

        #[test]
        fn local_key_rejects_keys_outside_prefix(prefix in "[a-z]{1,8}/", key in "[a-z]{1,8}/[a-z]{0,8}") {
            let client = prefix_client(Some(prefix.clone()));
            let source_key = format!("{prefix}{key}");
            let inside = client.local_key(&source_key);
            prop_assert_eq!(inside, Some(key.as_str()));
            if !key.starts_with(&prefix) {
                prop_assert_eq!(client.local_key(&key), None);
            }
        }
    }
}
