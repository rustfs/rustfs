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

//! Shared HTTP transport for the on-demand migration source backends that do
//! not speak S3 (Azure Blob, native GCS).
//!
//! The S3 backend rides the AWS SDK; these providers have no SigV4 dialect, so
//! they talk plain HTTP through one `reqwest` client that carries the same
//! connect/read timeouts and TLS policy the operator configured for the source.
//! Redirects are refused: the endpoint passed the outbound policy gate once, and
//! following a source-chosen `Location` would leave that gate behind.
//!
//! Errors never render the request URL. A SAS token lives in the query string,
//! so a `reqwest` error rendered with its URL would print the credential into
//! the log line and the admin response.

use super::source_client::{SourceError, SourceHead, SourceTimeouts, USER_AGENT_SUFFIX, classify_status, is_multipart_etag};
use super::storage_api::remote_s3_client::{RemoteS3ClientError, validate_remote_endpoint, validate_target_ca_pem};
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::body::SdkBody;
use futures::StreamExt;
use http::HeaderMap;
use std::collections::HashMap;
use std::time::SystemTime;
use time::OffsetDateTime;
use time::format_description::well_known::{Rfc2822, Rfc3339};
use url::Url;

/// Origin the native backends are allowed to address, plus the HTTP client
/// that reaches it.
pub(super) struct NativeHttp {
    client: reqwest::Client,
    endpoint: Url,
}

impl NativeHttp {
    /// `endpoint` must be a bare `scheme://host[:port]` origin; it is checked
    /// against the outbound policy exactly like an S3 source endpoint.
    pub(super) fn new(
        endpoint: &str,
        timeouts: SourceTimeouts,
        skip_tls_verify: bool,
        ca_cert_pem: Option<&str>,
    ) -> Result<Self, RemoteS3ClientError> {
        let endpoint = Url::parse(endpoint.trim()).map_err(|err| RemoteS3ClientError::InvalidEndpoint(err.to_string()))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(RemoteS3ClientError::InvalidEndpoint(format!(
                "unsupported scheme {}; expected http or https",
                endpoint.scheme()
            )));
        }
        if endpoint.host_str().is_none_or(str::is_empty) {
            return Err(RemoteS3ClientError::InvalidEndpoint("endpoint has no host".to_string()));
        }
        if !endpoint.username().is_empty() || endpoint.password().is_some() {
            return Err(RemoteS3ClientError::InvalidEndpoint("endpoint must not carry userinfo".to_string()));
        }
        if !matches!(endpoint.path(), "" | "/") || endpoint.query().is_some() || endpoint.fragment().is_some() {
            return Err(RemoteS3ClientError::InvalidEndpoint(
                "endpoint must be an origin without path, query or fragment".to_string(),
            ));
        }
        validate_remote_endpoint(&endpoint).map_err(RemoteS3ClientError::EndpointNotAllowed)?;

        let mut builder = reqwest::Client::builder()
            .connect_timeout(timeouts.connect)
            .read_timeout(timeouts.read)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(USER_AGENT_SUFFIX);
        if skip_tls_verify {
            builder = builder.danger_accept_invalid_certs(true);
        } else if let Some(pem) = ca_cert_pem.map(str::trim).filter(|pem| !pem.is_empty()) {
            // Reject a malformed bundle the same way the S3 path does, so the
            // operator sees "invalid CA PEM" instead of a TLS handshake failure.
            validate_target_ca_pem(pem)?;
            let certificate = reqwest::Certificate::from_pem(pem.as_bytes())
                .map_err(|err| RemoteS3ClientError::InvalidCaPem(err.to_string()))?;
            builder = builder.add_root_certificate(certificate);
        }

        let client = builder
            .build()
            .map_err(|err| RemoteS3ClientError::InvalidEndpoint(format!("http client cannot be built: {err}")))?;
        Ok(Self { client, endpoint })
    }

    #[cfg(test)]
    pub(super) fn for_test(endpoint: Url) -> Self {
        Self {
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("test http client should build"),
            endpoint,
        }
    }

    /// A URL under the endpoint origin. `segments` are percent-encoded as
    /// path segments, so a key containing `?`, `#` or a space cannot rewrite
    /// the request target.
    pub(super) fn url<'a>(&self, segments: impl IntoIterator<Item = &'a str>) -> Result<Url, SourceError> {
        let mut url = self.endpoint.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| SourceError::Other("source endpoint cannot carry a path".to_string()))?;
            path.clear();
            path.extend(segments);
        }
        Ok(url)
    }

    /// Sends the request and returns the response only for a 2xx status.
    /// Non-2xx statuses are classified from the status and an optional provider
    /// error-code header; response bodies are not read, so no provider message
    /// can smuggle credentials or markup into a log line.
    pub(super) async fn send(
        &self,
        request: reqwest::Request,
        error_code_header: Option<&str>,
    ) -> Result<reqwest::Response, SourceError> {
        self.send_classified(request, error_code_header, false).await
    }

    #[cfg(feature = "gcs")]
    pub(super) async fn send_object(
        &self,
        request: reqwest::Request,
        error_code_header: Option<&str>,
    ) -> Result<reqwest::Response, SourceError> {
        self.send_classified(request, error_code_header, true).await
    }

    async fn send_classified(
        &self,
        request: reqwest::Request,
        error_code_header: Option<&str>,
        not_found_on_404_without_code: bool,
    ) -> Result<reqwest::Response, SourceError> {
        let response = self.execute(request).await?;
        let status = response.status();
        match Self::check_response(response, error_code_header) {
            Err(SourceError::Other(_)) if not_found_on_404_without_code && status.as_u16() == 404 => Err(SourceError::NotFound),
            result => result,
        }
    }

    pub(super) async fn execute(&self, request: reqwest::Request) -> Result<reqwest::Response, SourceError> {
        self.client.execute(request).await.map_err(classify_transport_error)
    }

    pub(super) fn check_response(
        response: reqwest::Response,
        error_code_header: Option<&str>,
    ) -> Result<reqwest::Response, SourceError> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let code = error_code_header
            .and_then(|header| response.headers().get(header))
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let message = match &code {
            Some(code) => format!("source returned HTTP {status} ({code})"),
            None => format!("source returned HTTP {status}"),
        };
        match classify_status(status.as_u16(), code.as_deref(), message.clone()) {
            // Native object absence needs provider-specific evidence or a
            // successful bucket probe, never an alias from the S3 classifier.
            SourceError::NotFound => Err(classify_status(status.as_u16(), None, message)),
            error => Err(error),
        }
    }
}

/// Renders a transport failure without the request URL: a SAS token or a
/// signed query would otherwise reach logs and admin responses.
pub(super) fn classify_transport_error(err: reqwest::Error) -> SourceError {
    let is_timeout = err.is_timeout();
    let is_connect = err.is_connect();
    let message = err.without_url().to_string();
    if is_timeout {
        SourceError::Timeout
    } else if is_connect {
        SourceError::Connect(message)
    } else {
        SourceError::Other(message)
    }
}

/// Streams the response body without buffering it.
pub(super) fn response_body(response: reqwest::Response) -> ByteStream {
    let stream = response.bytes_stream().map(|chunk| {
        chunk
            .map(http_body::Frame::data)
            .map_err(|err| std::io::Error::other(err.without_url().to_string()))
    });
    ByteStream::new(SdkBody::from_body_1_x(http_body_util::StreamBody::new(stream)))
}

/// Reads a bounded response body as UTF-8, for the XML and JSON listings.
pub(super) async fn read_text(response: reqwest::Response, max_bytes: usize) -> Result<String, SourceError> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(classify_transport_error)?;
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(SourceError::Other("source listing response exceeded the size limit".to_string()));
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|_| SourceError::Other("source listing response is not valid UTF-8".to_string()))
}

/// Base64 digest (`Content-MD5`, `md5Hash`, `x-goog-hash`) as lowercase hex.
/// `None` when the value is not a 16-byte digest, so a CRC32C never passes as
/// an MD5.
#[cfg(any(test, feature = "gcs"))]
pub(super) fn base64_md5_to_hex(value: &str) -> Option<String> {
    let raw = base64_simd::STANDARD.decode_to_vec(value.trim().as_bytes()).ok()?;
    (raw.len() == 16).then(|| faster_hex::hex_string(&raw))
}

pub(super) fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok()).map(str::trim)
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    header(headers, name).filter(|value| !value.is_empty()).map(str::to_string)
}

/// `Last-Modified` and friends arrive as an HTTP date; the JSON dialects use
/// RFC 3339 for the same field, so both are accepted.
pub(super) fn parse_http_timestamp(value: &str) -> Option<SystemTime> {
    OffsetDateTime::parse(value, &Rfc2822)
        .or_else(|_| OffsetDateTime::parse(value, &Rfc3339))
        .ok()
        .map(SystemTime::from)
}

/// Provider-specific fields the shared header mapping cannot infer.
pub(super) struct NativeHeadFields {
    pub(super) etag: Option<String>,
    /// The ETag is an opaque token rather than a digest of the bytes.
    pub(super) etag_is_opaque: bool,
    pub(super) version_id: Option<String>,
    pub(super) storage_class: Option<String>,
}

/// Maps a HEAD or GET response onto [`SourceHead`]. `metadata_prefix` is the
/// provider's user-metadata header prefix (`x-ms-meta-`, `x-goog-meta-`); the
/// stored shape drops it, matching the `x-amz-meta-` handling of the S3 path.
pub(super) fn native_source_head(
    headers: &HeaderMap,
    metadata_prefix: &str,
    fields: NativeHeadFields,
) -> Result<SourceHead, SourceError> {
    let size = header(headers, "content-length")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| SourceError::Other("source response has no valid content-length".to_string()))?;

    let mut user_metadata = HashMap::new();
    for (name, value) in headers {
        let name = name.as_str();
        if let Some(key) = name.strip_prefix(metadata_prefix)
            && !key.is_empty()
            && let Ok(value) = value.to_str()
        {
            user_metadata.insert(key.to_string(), value.to_string());
        }
    }

    let etag = fields
        .etag
        .map(|etag| etag.trim().trim_matches('"').to_string())
        .filter(|etag| !etag.is_empty());
    // An opaque ETag never encodes a part count, so the multipart flag stays
    // false for it however the provider happens to spell the token.
    let is_multipart_etag = !fields.etag_is_opaque && etag.as_deref().is_some_and(is_multipart_etag);

    Ok(SourceHead {
        etag,
        size,
        last_modified: header(headers, "last-modified").and_then(parse_http_timestamp),
        content_type: header_string(headers, "content-type"),
        content_encoding: header_string(headers, "content-encoding"),
        content_disposition: header_string(headers, "content-disposition"),
        content_language: header_string(headers, "content-language"),
        cache_control: header_string(headers, "cache-control"),
        expires: header_string(headers, "expires"),
        user_metadata,
        version_id: fields.version_id,
        storage_class: fields.storage_class,
        // Neither native provider hands back ciphertext: a customer-key object
        // is refused by the backend before it reaches this mapping, and the
        // service-managed encryption is transparent to the reader.
        sse: None,
        is_multipart_etag,
        etag_is_opaque: fields.etag_is_opaque,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                http::HeaderName::from_bytes(name.as_bytes()).expect("test header name"),
                HeaderValue::from_str(value).expect("test header value"),
            );
        }
        headers
    }

    fn fields() -> NativeHeadFields {
        NativeHeadFields {
            etag: None,
            etag_is_opaque: false,
            version_id: None,
            storage_class: None,
        }
    }

    #[test]
    fn native_source_head_maps_content_headers_and_prefixed_metadata() {
        let headers = headers(&[
            ("content-length", "1234"),
            ("content-type", "text/plain"),
            ("content-encoding", "gzip"),
            ("content-language", "en"),
            ("content-disposition", "attachment"),
            ("cache-control", "max-age=60"),
            ("expires", "Thu, 01 Jan 2026 00:00:00 GMT"),
            ("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
            ("x-ms-meta-owner", "alice"),
            ("x-goog-meta-owner", "not-mine"),
        ]);
        let head = native_source_head(
            &headers,
            "x-ms-meta-",
            NativeHeadFields {
                etag: Some("\"0x8DCE1D2\"".to_string()),
                etag_is_opaque: true,
                version_id: Some("2026-01-01T00:00:00.0000000Z".to_string()),
                storage_class: Some("Hot".to_string()),
            },
        )
        .expect("head should map");

        assert_eq!(head.size, 1234);
        assert_eq!(head.content_type.as_deref(), Some("text/plain"));
        assert_eq!(head.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(head.content_language.as_deref(), Some("en"));
        assert_eq!(head.content_disposition.as_deref(), Some("attachment"));
        assert_eq!(head.cache_control.as_deref(), Some("max-age=60"));
        assert_eq!(head.expires.as_deref(), Some("Thu, 01 Jan 2026 00:00:00 GMT"));
        assert_eq!(
            head.last_modified,
            Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_445_412_480)),
            "HTTP-date Last-Modified must parse"
        );
        assert_eq!(
            head.user_metadata,
            HashMap::from([("owner".to_string(), "alice".to_string())]),
            "only the provider's own metadata prefix is read"
        );
        assert_eq!(head.etag.as_deref(), Some("0x8DCE1D2"), "quotes are stripped, the token is kept");
        assert!(head.etag_is_opaque);
        assert!(!head.is_multipart_etag);
        assert_eq!(head.storage_class.as_deref(), Some("Hot"));
        assert!(head.sse.is_none());
    }

    #[test]
    fn native_source_head_requires_a_content_length() {
        let err = native_source_head(&headers(&[("content-type", "text/plain")]), "x-ms-meta-", fields())
            .expect_err("a response without content-length is unusable");
        assert!(matches!(err, SourceError::Other(_)), "{err:?}");
    }

    #[test]
    fn opaque_etag_never_reads_as_a_multipart_etag() {
        // A digest-shaped ETag keeps the S3 reading; the same string marked
        // opaque must not be split into "digest-partcount".
        for (opaque, expected) in [(false, true), (true, false)] {
            let head = native_source_head(
                &headers(&[("content-length", "1")]),
                "x-ms-meta-",
                NativeHeadFields {
                    etag: Some("d41d8cd98f00b204e9800998ecf8427e-3".to_string()),
                    etag_is_opaque: opaque,
                    ..fields()
                },
            )
            .expect("head should map");
            assert_eq!(head.is_multipart_etag, expected, "opaque = {opaque}");
        }
    }

    #[test]
    fn base64_md5_converts_only_sixteen_byte_digests() {
        assert_eq!(
            base64_md5_to_hex("1B2M2Y8AsgTpgAmY7PhCfg==").as_deref(),
            Some("d41d8cd98f00b204e9800998ecf8427e")
        );
        assert_eq!(base64_md5_to_hex("not base64!").as_deref(), None);
        // A CRC32C digest is four bytes: it must not pass as an MD5.
        assert_eq!(base64_md5_to_hex("AAAAAA==").as_deref(), None);
    }

    #[test]
    fn native_http_rejects_endpoints_that_are_not_bare_origins() {
        for bad in [
            "ftp://source.example.com",
            "https://user:pw@source.example.com",
            "https://source.example.com/container",
            "https://source.example.com/?x=1",
            "not a url",
        ] {
            assert!(
                NativeHttp::new(bad, SourceTimeouts::default(), false, None).is_err(),
                "{bad} must be rejected"
            );
        }
    }

    #[test]
    fn native_http_percent_encodes_every_path_segment() {
        let http = NativeHttp::for_test(Url::parse("https://acct.blob.core.windows.net").expect("origin"));
        let url = http.url(["container", "dir", "a b?c#d.txt"]).expect("url should build");
        assert_eq!(url.as_str(), "https://acct.blob.core.windows.net/container/dir/a%20b%3Fc%23d.txt");
        assert_eq!(url.query(), None, "a key with '?' must not become a query");
    }
}
