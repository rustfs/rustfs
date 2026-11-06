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

//! Native Azure Blob tier transport, including Azure Stack Hub 2301 and later.
//!
//! Writes use one conditional commit and never replay a failed HTTP request.
//! Larger bodies are staged in bounded blocks with a unique upload namespace;
//! an interrupted staging operation leaves only Azure's expiring uncommitted
//! blocks. Lifecycle uploads use the existing transaction journal after a lost
//! commit acknowledgement; validation probes retain in-process compensation.
//! Versioned GET/DELETE address the persisted version. Current-root cleanup
//! may demote that matching version under a strong ETag before exact deletion.
//! Unversioned DELETE also requires a strong ETag to refuse replacement.

use super::tier_config::TierAzure;
use super::warm_backend::{
    TransitionCandidateIdentity, TransitionCandidateProbe, TransitionCandidateReconciler, WarmBackend, WarmBackendGetOpts,
    build_transition_put_options, transition_client_timeouts_from_env,
};
use super::warm_backend_s3::transition_candidate_metadata_matches;
use crate::bucket::lifecycle::transition_transaction::canonical_transition_remote_object;
use bytes::Bytes;
use futures_util::stream;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use quick_xml::{Reader, events::Event};
use rustfs_s3_client::transition_api::{ReadCloser, ReaderImpl};
use rustfs_signer::azure_shared_key_signature;
use rustfs_utils::crypto::hex;
use rustfs_utils::egress::validate_outbound_url;
use rustfs_utils::hash::FastMd5;
use rustfs_utils::http::metadata_compat::{
    MINIO_INTERNAL_PREFIX, SUFFIX_TRANSITION_TIER_DESTINATION_ID, SUFFIX_TRANSITION_TRANSACTION_ID, internal_key_rustfs,
};
use std::collections::HashMap;
use std::io::{self, Cursor, ErrorKind};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt};
use url::Url;

const API_VERSION: &str = "2020-10-02";
const MIN_BLOCK_BYTES: u64 = 16 * 1024 * 1024;
const MAX_BLOCK_BYTES: u64 = 100 * 1024 * 1024;
const MAX_BLOCKS: u64 = 50_000;
const MAX_CONTROL_BYTES: usize = 64 * 1024;
const MAX_METADATA_BYTES: usize = 8 * 1024;
const METADATA_PREFIX: &str = "x-ms-meta-rustfs_";
const HTTP_DATE: &[time::format_description::BorrowedFormatItem<'static>] =
    time::macros::format_description!("[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT");

/// A bounded diagnostic that never contains credentials, URLs or response bodies.
#[derive(Debug, thiserror::Error)]
#[error("Azure Blob {operation} failed (HTTP {status}, code {code}, request ID {request_id})")]
pub(crate) struct AzureBlobError {
    operation: &'static str,
    status: StatusCode,
    code: String,
    request_id: String,
}

impl AzureBlobError {
    pub(super) fn from_io(error: &io::Error) -> Option<&Self> {
        error.get_ref()?.downcast_ref()
    }

    pub(super) fn rejects_write(&self) -> bool {
        matches!(
            self.status,
            StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND | StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED
        )
    }
}

pub struct WarmBackendAzureBlob {
    client: reqwest::Client,
    endpoint: Url,
    account: String,
    key: Vec<u8>,
    container: String,
    prefix: String,
    request_timeout: Duration,
}

impl WarmBackendAzureBlob {
    /// Build a native Blob backend without performing remote I/O.
    ///
    /// # Errors
    ///
    /// Rejects unsupported credentials or storage classes, an invalid account,
    /// container, prefix, endpoint or account key, and TLS/client setup failures.
    pub fn new(conf: &TierAzure) -> io::Result<Self> {
        validate_config(conf)?;
        let endpoint = if conf.endpoint.is_empty() {
            format!("https://{}.blob.core.windows.net", conf.access_key)
        } else {
            conf.endpoint.clone()
        };
        let endpoint = Url::parse(&endpoint).map_err(|_| invalid_input("Azure Blob endpoint is not a valid URL"))?;
        if !matches!(endpoint.scheme(), "https" | "http")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || !matches!(endpoint.path(), "" | "/")
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(invalid_input(
                "Azure Blob endpoint must be an HTTP(S) origin without credentials, path or query",
            ));
        }
        validate_outbound_url(&endpoint).map_err(|_| invalid_input("Azure Blob endpoint is not allowed"))?;
        let key = decode_key(&conf.secret_key)?;
        rustfs_tls_runtime::ensure_crypto_provider().map_err(io::Error::other)?;
        let timeouts = transition_client_timeouts_from_env();
        let client = reqwest::Client::builder()
            .connect_timeout(timeouts.connect_timeout)
            .read_timeout(timeouts.response_body_idle_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .build()
            .map_err(transport_error)?;
        Ok(Self {
            client,
            endpoint,
            account: conf.access_key.clone(),
            key,
            container: conf.bucket.clone(),
            prefix: conf.prefix.strip_suffix('/').unwrap_or(&conf.prefix).to_string(),
            request_timeout: timeouts.request_timeout,
        })
    }

    fn object_url(&self, object: &str, version: &str) -> io::Result<Url> {
        validate_version(version)?;
        let name = if self.prefix.is_empty() {
            object.to_string()
        } else {
            format!("{}/{object}", self.prefix)
        };
        validate_object_name(&name)?;
        let mut url = self.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| invalid_input("Azure Blob endpoint has no path"))?;
            segments.clear().push(&self.container);
            for segment in name.split('/') {
                segments.push(segment);
            }
        }
        if !version.is_empty() {
            url.query_pairs_mut().append_pair("versionid", version);
        }
        Ok(url)
    }

    fn container_url(&self) -> io::Result<Url> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| invalid_input("Azure Blob endpoint has no path"))?
            .clear()
            .push(&self.container);
        Ok(url)
    }

    async fn send(&self, method: Method, url: Url, mut headers: HeaderMap, body: Option<Bytes>) -> io::Result<reqwest::Response> {
        if let Some(body) = &body {
            insert_header(&mut headers, "content-length", &body.len().to_string())?;
            let bytes = body.clone();
            let digest = tokio::task::spawn_blocking(move || FastMd5::digest(&bytes))
                .await
                .map_err(|_| io::Error::other("Azure Blob content checksum task failed"))?;
            insert_header(&mut headers, "content-md5", &base64_simd::STANDARD.encode_to_string(digest))?;
        }
        headers.insert("x-ms-version", HeaderValue::from_static(API_VERSION));
        headers.insert("accept-encoding", HeaderValue::from_static("identity"));
        let date = OffsetDateTime::now_utc()
            .format(HTTP_DATE)
            .map_err(|_| io::Error::other("Azure Blob request date is not representable"))?;
        insert_header(&mut headers, "x-ms-date", &date)?;
        let signature = azure_shared_key_signature(&self.key, &self.account, method.as_str(), &url, &headers)
            .map_err(|err| io::Error::new(ErrorKind::InvalidInput, err))?;
        insert_header(&mut headers, "authorization", &format!("SharedKey {}:{signature}", self.account))?;
        let mut request = reqwest::Request::new(method, url);
        *request.headers_mut() = headers;
        *request.body_mut() = body.map(reqwest::Body::from);
        tokio::time::timeout(self.request_timeout, self.client.execute(request))
            .await
            .map_err(|_| io::Error::new(ErrorKind::TimedOut, "Azure Blob request timed out"))?
            .map_err(transport_error)
    }

    async fn expect_status(
        &self,
        response: reqwest::Response,
        expected: StatusCode,
        operation: &'static str,
    ) -> io::Result<reqwest::Response> {
        if response.status() == expected {
            return Ok(response);
        }
        Err(service_error(response, operation, self.request_timeout).await)
    }

    async fn head(&self, object: &str, version: &str) -> io::Result<Option<HeaderMap>> {
        let response = self
            .send(Method::HEAD, self.object_url(object, version)?, HeaderMap::new(), None)
            .await?;
        if response.status() == StatusCode::OK {
            if !version.is_empty() {
                validate_response_version(response.headers(), version)?;
            }
            return Ok(Some(response.headers().clone()));
        }
        let err = service_error(response, "HEAD", self.request_timeout).await;
        if err.kind() == ErrorKind::NotFound {
            return Ok(None);
        }
        Err(err)
    }

    async fn delete(&self, object: &str, version: &str, headers: HeaderMap) -> io::Result<()> {
        let response = self
            .send(Method::DELETE, self.object_url(object, version)?, headers, None)
            .await?;
        if response.status() == StatusCode::ACCEPTED {
            return Ok(());
        }
        let error = service_error(response, "DELETE", self.request_timeout).await;
        if error.kind() == ErrorKind::NotFound {
            return Ok(());
        }
        Err(error)
    }

    async fn write(
        &self,
        object: &str,
        reader: ReaderImpl,
        length: i64,
        metadata: HashMap<String, String>,
    ) -> io::Result<String> {
        let size = u64::try_from(length).map_err(|_| invalid_input("Azure Blob upload requires a known, non-negative length"))?;
        let block_size = block_size(size)?;
        let url = self.object_url(object, "")?;
        let mut headers = metadata_headers(metadata)?;
        headers.insert("if-none-match", HeaderValue::from_static("*"));
        let mut reader: Box<dyn AsyncRead + Send + Sync + Unpin> = match reader {
            ReaderImpl::Body(bytes) => Box::new(Cursor::new(bytes)),
            ReaderImpl::ObjectBody(reader) => Box::new(reader),
        };
        if size <= block_size {
            let body = read_part(&mut reader, size).await?;
            ensure_eof(&mut reader).await?;
            headers.insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
            let response = self.send(Method::PUT, url, headers, Some(body)).await?;
            let response = self.expect_status(response, StatusCode::CREATED, "PUT").await?;
            return response_version(response.headers());
        }

        let upload = uuid::Uuid::new_v4().simple().to_string();
        let mut blocks = Vec::new();
        let mut remaining = size;
        while remaining > 0 {
            let count = remaining.min(block_size);
            let body = read_part(&mut reader, count).await?;
            let block_id = base64_simd::STANDARD.encode_to_string(format!("{upload}-{:05}", blocks.len()));
            let mut block_url = url.clone();
            block_url
                .query_pairs_mut()
                .append_pair("comp", "block")
                .append_pair("blockid", &block_id);
            let response = self.send(Method::PUT, block_url, HeaderMap::new(), Some(body)).await?;
            self.expect_status(response, StatusCode::CREATED, "PUT block").await?;
            blocks.push(block_id);
            remaining -= count;
        }
        ensure_eof(&mut reader).await?;
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for block in blocks {
            // IDs are generated base64, so they contain no XML delimiters.
            xml.push_str("<Uncommitted>");
            xml.push_str(&block);
            xml.push_str("</Uncommitted>");
        }
        xml.push_str("</BlockList>");
        let mut commit_url = url;
        commit_url.query_pairs_mut().append_pair("comp", "blocklist");
        headers.insert("content-type", HeaderValue::from_static("application/xml"));
        let response = self.send(Method::PUT, commit_url, headers, Some(Bytes::from(xml))).await?;
        let response = self.expect_status(response, StatusCode::CREATED, "PUT block list").await?;
        response_version(response.headers())
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendAzureBlob {
    fn validate_remote_version_id(&self, version: &str) -> io::Result<()> {
        validate_version(version)
    }

    async fn put(&self, object: &str, reader: ReaderImpl, length: i64) -> io::Result<String> {
        self.write(object, reader, length, HashMap::new()).await
    }

    async fn put_with_meta(
        &self,
        object: &str,
        reader: ReaderImpl,
        length: i64,
        metadata: HashMap<String, String>,
    ) -> io::Result<String> {
        self.write(object, reader, length, metadata).await
    }

    async fn get(&self, object: &str, version: &str, opts: WarmBackendGetOpts) -> io::Result<ReadCloser> {
        let range = requested_range(&opts)?;
        let mut headers = HeaderMap::new();
        if let Some((start, end)) = range {
            let value = end.map_or_else(|| format!("bytes={start}-"), |end| format!("bytes={start}-{end}"));
            insert_header(&mut headers, "range", &value)?;
        }
        let response = self
            .send(Method::GET, self.object_url(object, version)?, headers, None)
            .await?;
        let expected = if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        let response = self.expect_status(response, expected, "GET").await?;
        validate_response_version(response.headers(), version)?;
        let length = response
            .headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| invalid_data("Azure Blob GET omitted a valid Content-Length"))?;
        if let Some(range) = range {
            validate_content_range(response.headers(), range, length)?;
        }
        let body = stream::try_unfold((response, length), |(mut response, remaining)| async move {
            loop {
                match response.chunk().await.map_err(transport_error)? {
                    Some(chunk) if chunk.is_empty() => continue,
                    Some(chunk) => {
                        let count =
                            u64::try_from(chunk.len()).map_err(|_| invalid_data("Azure Blob response length overflow"))?;
                        let remaining = remaining
                            .checked_sub(count)
                            .ok_or_else(|| invalid_data("Azure Blob returned more bytes than declared"))?;
                        return Ok::<_, io::Error>(Some((chunk, (response, remaining))));
                    }
                    None if remaining == 0 => return Ok(None),
                    None => {
                        return Err(io::Error::new(
                            ErrorKind::UnexpectedEof,
                            "Azure Blob response ended before its declared length",
                        ));
                    }
                }
            }
        });
        Ok(ReadCloser::new(tokio_util::io::StreamReader::new(Box::pin(body))))
    }

    async fn remove(&self, object: &str, version: &str) -> io::Result<()> {
        let mut headers = HeaderMap::new();
        if version.is_empty() {
            let Some(current) = self.head(object, "").await? else {
                return Ok(());
            };
            if !response_version(&current)?.is_empty() {
                return Err(invalid_data("Azure Blob became versioned; refusing an unversioned delete"));
            }
            headers = delete_preconditions(&current)?;
        }
        let Err(error) = self.delete(object, version, headers).await else {
            return Ok(());
        };
        if version.is_empty()
            || !AzureBlobError::from_io(&error)
                .is_some_and(|azure| azure.status == StatusCode::CONFLICT && azure.code == "OperationNotAllowedOnRootBlob")
        {
            return Err(error);
        }

        // Azure may require the current version to become a previous version
        // before exact deletion. Only its matching generation can be demoted.
        if let Some(current) = self.head(object, "").await? {
            validate_response_version(&current, version)?;
            self.delete(object, "", delete_preconditions(&current)?).await?;
        }
        // A successful base delete alone does not complete version cleanup.
        // An error or lost acknowledgement remains retryable by the caller.
        self.delete(object, version, HeaderMap::new()).await
    }

    async fn probe_transition_candidate(&self, object: &str) -> io::Result<TransitionCandidateProbe> {
        Ok(match self.head(object, "").await? {
            None => TransitionCandidateProbe::Missing,
            Some(headers) => classify_head(&headers)?,
        })
    }

    async fn probe_transition_version(&self, object: &str, version: &str) -> io::Result<TransitionCandidateProbe> {
        if version.is_empty() {
            return Err(invalid_input("Azure Blob exact probe requires a version ID"));
        }
        Ok(match self.head(object, version).await? {
            None => TransitionCandidateProbe::Missing,
            Some(_) => TransitionCandidateProbe::VersionedPresent(version.to_string()),
        })
    }

    async fn in_use(&self) -> io::Result<bool> {
        let mut url = self.container_url()?;
        let prefix = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        url.query_pairs_mut()
            .append_pair("restype", "container")
            .append_pair("comp", "list")
            .append_pair("prefix", &prefix)
            .append_pair("maxresults", "1");
        let response = self.send(Method::GET, url, HeaderMap::new(), None).await?;
        let response = self.expect_status(response, StatusCode::OK, "LIST").await?;
        list_has_objects(&read_control_body(response, self.request_timeout).await?)
    }
}

#[async_trait::async_trait]
impl TransitionCandidateReconciler for WarmBackendAzureBlob {
    async fn probe_transition_candidate_for(
        &self,
        object: &str,
        identity: TransitionCandidateIdentity,
    ) -> io::Result<TransitionCandidateProbe> {
        let Some(headers) = self.head(object, "").await? else {
            return Ok(TransitionCandidateProbe::Missing);
        };
        let mut metadata = HashMap::new();
        for suffix in [SUFFIX_TRANSITION_TRANSACTION_ID, SUFFIX_TRANSITION_TIER_DESTINATION_ID] {
            for key in [internal_key_rustfs(suffix), format!("{MINIO_INTERNAL_PREFIX}{suffix}")] {
                if let Some(value) = headers.get(metadata_header_name(&key)) {
                    let decoded = base64_simd::STANDARD
                        .decode_to_vec(value.as_bytes())
                        .map_err(|_| invalid_data("Azure Blob candidate metadata is not valid base64"))?;
                    let value =
                        String::from_utf8(decoded).map_err(|_| invalid_data("Azure Blob candidate metadata is not UTF-8"))?;
                    metadata.insert(key, value);
                }
            }
        }
        if !transition_candidate_metadata_matches(&metadata, identity)? {
            return Ok(TransitionCandidateProbe::Ambiguous);
        }
        classify_head(&headers)
    }
}

fn validate_config(conf: &TierAzure) -> io::Result<()> {
    if !(3..=24).contains(&conf.access_key.len())
        || !conf
            .access_key
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err(invalid_input("Azure Blob account name must contain 3 to 24 lowercase letters or digits"));
    }
    let container = conf.bucket.as_bytes();
    if !(3..=63).contains(&container.len())
        || !container
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
        || container.first() == Some(&b'-')
        || container.last() == Some(&b'-')
        || conf.bucket.contains("--")
    {
        return Err(invalid_input("Azure Blob container name is invalid"));
    }
    if !(conf.storage_class.is_empty() || conf.storage_class == "STANDARD")
        || !conf.sp_auth.tenant_id.is_empty()
        || !conf.sp_auth.client_id.is_empty()
        || !conf.sp_auth.client_secret.is_empty()
    {
        return Err(invalid_input(
            "Azure Blob tiers currently require an account key and the default storage class",
        ));
    }
    let prefix = conf.prefix.strip_suffix('/').unwrap_or(&conf.prefix);
    if !prefix.is_empty() {
        validate_object_name(prefix)?;
        // Canonical suffix length is independent of the bucket and UUID values.
        let object = canonical_transition_remote_object(
            uuid::Uuid::from_u128(1),
            &conf.bucket,
            uuid::Uuid::from_u128(2),
            uuid::Uuid::from_u128(3),
        )
        .map_err(io::Error::other)?;
        validate_object_name(&format!("{prefix}/{object}"))?;
    }
    Ok(())
}

fn decode_key(key: &str) -> io::Result<Vec<u8>> {
    let key = base64_simd::STANDARD
        .decode_to_vec(key.trim().as_bytes())
        .map_err(|_| invalid_input("Azure Blob account key must be base64"))?;
    if key.is_empty() {
        return Err(invalid_input("Azure Blob account key must not be empty"));
    }
    Ok(key)
}

fn validate_object_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name.len() > 1760
        || name.chars().count() > 880
        || name.split('/').any(|segment| matches!(segment, "." | ".."))
    {
        return Err(invalid_input(
            "Azure Blob tier name exceeds Stack Hub limits or contains a standalone dot segment",
        ));
    }
    Ok(())
}

fn validate_version(version: &str) -> io::Result<()> {
    if version.len() > 1024 || version.bytes().any(|byte| !byte.is_ascii_graphic()) {
        return Err(invalid_input("Azure Blob version ID is invalid"));
    }
    Ok(())
}

fn response_version(headers: &HeaderMap) -> io::Result<String> {
    let Some(version) = headers.get("x-ms-version-id") else {
        return Ok(String::new());
    };
    let version = version
        .to_str()
        .map_err(|_| invalid_data("Azure Blob version ID is not ASCII"))?;
    validate_version(version)?;
    Ok(version.to_string())
}

fn validate_response_version(headers: &HeaderMap, requested: &str) -> io::Result<()> {
    if response_version(headers)? != requested {
        return Err(invalid_data("Azure Blob did not return the requested exact version"));
    }
    Ok(())
}

fn delete_preconditions(current: &HeaderMap) -> io::Result<HeaderMap> {
    let etag = current
        .get("etag")
        .ok_or_else(|| invalid_data("Azure Blob HEAD omitted the delete precondition"))?;
    let bytes = etag.as_bytes();
    // A wildcard, weak tag or list cannot identify a single matching generation.
    if bytes.len() < 3
        || bytes.first() != Some(&b'"')
        || bytes.last() != Some(&b'"')
        || !bytes[1..bytes.len() - 1]
            .iter()
            .all(|byte| *byte == b'!' || (b'#'..=b'~').contains(byte))
    {
        return Err(invalid_data("Azure Blob HEAD omitted a single strong ETag"));
    }
    let mut headers = HeaderMap::new();
    headers.insert("if-match", etag.clone());
    Ok(headers)
}

fn classify_head(headers: &HeaderMap) -> io::Result<TransitionCandidateProbe> {
    let version = response_version(headers)?;
    Ok(if version.is_empty() {
        TransitionCandidateProbe::UnversionedPresent
    } else {
        TransitionCandidateProbe::VersionedPresent(version)
    })
}

fn block_size(size: u64) -> io::Result<u64> {
    if size > MAX_BLOCK_BYTES * MAX_BLOCKS {
        return Err(invalid_input("Azure Blob exceeds the Stack Hub block count or object size limit"));
    }
    Ok(size.div_ceil(MAX_BLOCKS).max(MIN_BLOCK_BYTES))
}

async fn read_part(reader: &mut (dyn AsyncRead + Send + Sync + Unpin), length: u64) -> io::Result<Bytes> {
    let length = usize::try_from(length).map_err(|_| invalid_input("Azure Blob block length is not representable"))?;
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    Ok(Bytes::from(bytes))
}

async fn ensure_eof(reader: &mut (dyn AsyncRead + Send + Sync + Unpin)) -> io::Result<()> {
    if reader.read(&mut [0_u8; 1]).await? != 0 {
        return Err(invalid_input("Azure Blob upload body exceeds its declared length"));
    }
    Ok(())
}

fn metadata_header_name(key: &str) -> String {
    format!("{METADATA_PREFIX}{}", hex(key.as_bytes()))
}

fn metadata_headers(metadata: HashMap<String, String>) -> io::Result<HeaderMap> {
    let opts = build_transition_put_options(String::new(), metadata);
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ms-blob-content-type", opts.content_type),
        ("x-ms-blob-content-encoding", opts.content_encoding),
        ("x-ms-blob-content-language", opts.content_language),
        ("x-ms-blob-content-disposition", opts.content_disposition),
        ("x-ms-blob-cache-control", opts.cache_control),
    ] {
        if !value.is_empty() {
            insert_header(&mut headers, name, &value)?;
        }
    }
    let mut size = 0_usize;
    for (key, value) in opts.user_metadata {
        let key = key.to_ascii_lowercase();
        let key = key.strip_prefix("x-amz-meta-").unwrap_or(&key);
        let name = metadata_header_name(key);
        let value = base64_simd::STANDARD.encode_to_string(value.as_bytes());
        size = size.saturating_add(name.len()).saturating_add(value.len());
        if size > MAX_METADATA_BYTES {
            return Err(invalid_input("Azure Blob encoded metadata exceeds 8 KiB"));
        }
        if headers.contains_key(&name) {
            return Err(invalid_input("Azure Blob metadata contains conflicting names"));
        }
        insert_header(&mut headers, &name, &value)?;
    }
    Ok(headers)
}

fn requested_range(opts: &WarmBackendGetOpts) -> io::Result<Option<(u64, Option<u64>)>> {
    let start = u64::try_from(opts.start_offset).map_err(|_| invalid_input("Azure Blob range starts before zero"))?;
    let length = u64::try_from(opts.length).map_err(|_| invalid_input("Azure Blob range length is negative"))?;
    if length > 0 {
        let end = start
            .checked_add(length - 1)
            .ok_or_else(|| invalid_input("Azure Blob range overflows"))?;
        return Ok(Some((start, Some(end))));
    }
    Ok((start > 0).then_some((start, None)))
}

fn validate_content_range(headers: &HeaderMap, requested: (u64, Option<u64>), length: u64) -> io::Result<()> {
    let value = headers
        .get("content-range")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let range = value
        .strip_prefix("bytes ")
        .and_then(|value| value.split_once('/'))
        .and_then(|(range, total)| {
            let (start, end) = range.split_once('-')?;
            Some((start.parse::<u64>().ok()?, end.parse::<u64>().ok()?, total.parse::<u64>().ok()?))
        });
    let Some((start, end, total)) = range else {
        return Err(invalid_data("Azure Blob returned an invalid Content-Range"));
    };
    if start != requested.0
        || end < start
        || end >= total
        || end != requested.1.unwrap_or(total - 1).min(total - 1)
        || end.checked_sub(start).and_then(|size| size.checked_add(1)) != Some(length)
    {
        return Err(invalid_data("Azure Blob returned a different range from the one requested"));
    }
    Ok(())
}

async fn read_control_body(mut response: reqwest::Response, timeout: Duration) -> io::Result<Vec<u8>> {
    tokio::time::timeout(timeout, async move {
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
            if body.len().saturating_add(chunk.len()) > MAX_CONTROL_BYTES {
                return Err(invalid_data("Azure Blob control response exceeds 64 KiB"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    })
    .await
    .map_err(|_| io::Error::new(ErrorKind::TimedOut, "Azure Blob control response timed out"))?
}

async fn service_error(response: reqwest::Response, operation: &'static str, timeout: Duration) -> io::Error {
    let status = response.status();
    let request_id = diagnostic_token(
        response
            .headers()
            .get("x-ms-request-id")
            .and_then(|value| value.to_str().ok()),
    );
    let mut code = diagnostic_token(
        response
            .headers()
            .get("x-ms-error-code")
            .and_then(|value| value.to_str().ok()),
    );
    if code == "unknown"
        && let Ok(body) = read_control_body(response, timeout).await
        && let Some(xml_code) = xml_error_code(&body)
    {
        code = diagnostic_token(Some(&xml_code));
    }
    let kind = if status == StatusCode::NOT_FOUND && code == "BlobNotFound" {
        ErrorKind::NotFound
    } else if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        ErrorKind::PermissionDenied
    } else if status == StatusCode::PRECONDITION_FAILED {
        ErrorKind::AlreadyExists
    } else {
        ErrorKind::Other
    };
    io::Error::new(
        kind,
        AzureBlobError {
            operation,
            status,
            code,
            request_id,
        },
    )
}

fn diagnostic_token(value: Option<&str>) -> String {
    value
        .filter(|value| {
            !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .unwrap_or("unknown")
        .to_string()
}

fn xml_error_code(body: &[u8]) -> Option<String> {
    let mut reader = Reader::from_reader(body);
    let mut depth = 0_u32;
    let mut root = false;
    let mut code = None;
    loop {
        match reader.read_event().ok()? {
            Event::Start(start) => {
                if depth == 0 {
                    if root || start.local_name().as_ref() != "Error" {
                        return None;
                    }
                    root = true;
                }
                if depth == 1 && start.local_name().as_ref() == "Code" {
                    if code.is_some() {
                        return None;
                    }
                    let text = reader.read_text(start.name()).ok()?;
                    code = Some(quick_xml::escape::unescape(text.as_ref()).ok()?.into_owned());
                    continue;
                }
                depth += 1;
                if depth > 16 {
                    return None;
                }
            }
            Event::End(_) => depth = depth.checked_sub(1)?,
            Event::Text(text) if depth == 0 && !text.as_ref().trim().is_empty() => return None,
            Event::CData(_) if depth == 0 => return None,
            Event::Eof => return (root && depth == 0).then_some(code).flatten(),
            Event::DocType(_) => return None,
            _ => {}
        }
    }
}

fn list_has_objects(body: &[u8]) -> io::Result<bool> {
    let mut reader = Reader::from_reader(body);
    let mut depth = 0_u32;
    let mut root = false;
    let mut blobs = false;
    let mut found = false;
    loop {
        let event = reader
            .read_event()
            .map_err(|_| invalid_data("Azure Blob listing is not valid XML"))?;
        let empty = matches!(event, Event::Empty(_));
        match event {
            Event::Start(start) | Event::Empty(start) => {
                if depth == 0 && (root || start.local_name().as_ref() != "EnumerationResults") {
                    return Err(invalid_data("Azure Blob listing has an unexpected root"));
                }
                root |= start.local_name().as_ref() == "EnumerationResults";
                if start.local_name().as_ref() == "Blobs" {
                    if blobs || depth != 1 {
                        return Err(invalid_data("Azure Blob listing has an invalid Blobs element"));
                    }
                    blobs = true;
                }
                found |= matches!(start.local_name().as_ref(), "Blob" | "BlobPrefix");
                if !empty && start.local_name().as_ref() == "NextMarker" {
                    let marker = reader
                        .read_text(start.name())
                        .map_err(|_| invalid_data("Azure Blob listing marker is invalid"))?;
                    found |= !marker.is_empty();
                    continue;
                }
                if !empty {
                    depth += 1;
                    if depth > 16 {
                        return Err(invalid_data("Azure Blob listing exceeds the XML depth limit"));
                    }
                }
            }
            Event::End(_) => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| invalid_data("Azure Blob listing has an invalid closing tag"))?;
            }
            Event::DocType(_) => return Err(invalid_data("Azure Blob listing must not contain a document type")),
            Event::Text(text) if depth == 0 && !text.as_ref().trim().is_empty() => {
                return Err(invalid_data("Azure Blob listing contains text outside its root"));
            }
            Event::CData(_) if depth == 0 => return Err(invalid_data("Azure Blob listing contains text outside its root")),
            Event::Eof if root && blobs && depth == 0 => return Ok(found),
            Event::Eof => return Err(invalid_data("Azure Blob listing is incomplete")),
            _ => {}
        }
    }
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) -> io::Result<()> {
    let name =
        HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid_input("Azure Blob metadata header name is invalid"))?;
    let value = HeaderValue::from_str(value).map_err(|_| invalid_input("Azure Blob metadata header value is invalid"))?;
    headers.insert(name, value);
    Ok(())
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

fn transport_error(err: reqwest::Error) -> io::Error {
    let kind = if err.is_timeout() {
        ErrorKind::TimedOut
    } else {
        ErrorKind::Other
    };
    io::Error::new(kind, err.without_url())
}

#[cfg(test)]
#[path = "warm_backend_azure_blob_tests.rs"]
mod tests;
