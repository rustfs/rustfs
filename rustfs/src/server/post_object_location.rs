// Copyright 2026 RustFS Team
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

//! POST Object `PostResponse` Location rewrite for AWS-compatible absolute URIs.
//!
//! Both protocol stacks currently emit a relative, unencoded path
//! (`/{bucket}/{raw-key}`) when `success_action_status=201`. The s3s serializer
//! hard-codes that spelling, and the gateway's RustFS-profile POST forms path
//! does the same. This layer rebuilds `<Location>` from the response's `<Bucket>`
//! and `<Key>` plus the request's host, scheme, and addressing style.
//!
//! Encoding follows RFC 3986 path rules (spaces as `%20`, `/` left as a
//! separator, uppercase hex), matching MinIO's `getObjectLocation` and the
//! gateway's non-legacy POST serializer. `<Key>` is left unchanged.

use crate::server::hybrid::HybridBody;
use crate::server::legacy_compat::MAX_XML_COMPAT_BODY_BYTES;
use crate::server::rate_limit::virtual_host_bucket;
use bytes::Bytes;
use http::{HeaderMap, Method, Request as HttpRequest, Response, StatusCode, Uri, header};
use http_body::Body;
use http_body_util::BodyExt;
use rustfs_utils::http::get_source_scheme;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

#[derive(Clone, Debug)]
pub struct PostObjectLocationFixLayer {
    vh_domain_hosts: Arc<[String]>,
    connection_is_tls: bool,
}

impl PostObjectLocationFixLayer {
    pub fn new(vh_domain_hosts: Arc<[String]>, connection_is_tls: bool) -> Self {
        Self {
            vh_domain_hosts,
            connection_is_tls,
        }
    }
}

impl<S> Layer<S> for PostObjectLocationFixLayer {
    type Service = PostObjectLocationFixService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PostObjectLocationFixService {
            inner,
            vh_domain_hosts: self.vh_domain_hosts.clone(),
            connection_is_tls: self.connection_is_tls,
        }
    }
}

#[derive(Clone)]
pub struct PostObjectLocationFixService<S> {
    inner: S,
    vh_domain_hosts: Arc<[String]>,
    connection_is_tls: bool,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for PostObjectLocationFixService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    RestBody: Body<Data = Bytes> + From<Bytes> + Send + 'static,
    RestBody::Error: Into<S::Error> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let should_inspect = req.method() == Method::POST;
        let scheme = request_scheme(req.headers(), req.uri(), self.connection_is_tls);
        let host = request_host(req.headers(), req.uri()).map(str::to_owned);
        let virtual_hosted_bucket = virtual_host_bucket(&req, &self.vh_domain_hosts).map(str::to_owned);
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if !should_inspect || response.status() != StatusCode::CREATED || !is_xml_content_type(response.headers()) {
                return Ok(response);
            }

            let (parts, body) = response.into_parts();
            let response = match body {
                HybridBody::Rest { rest_body } if !is_bounded_xml_body(&rest_body) => {
                    Response::from_parts(parts, HybridBody::Rest { rest_body })
                }
                HybridBody::Rest { rest_body } => {
                    let rest_body = rewrite_location_body(rest_body, &scheme, host.as_deref(), virtual_hosted_bucket.as_deref())
                        .await
                        .map_err(Into::into)?;
                    let mut parts = parts;
                    parts.headers.remove(header::CONTENT_LENGTH);
                    Response::from_parts(parts, HybridBody::Rest { rest_body })
                }
                HybridBody::Grpc {
                    grpc_body,
                    first_frame_started,
                    body_started,
                    first_frame_recorded,
                } => Response::from_parts(
                    parts,
                    HybridBody::Grpc {
                        grpc_body,
                        first_frame_started,
                        body_started,
                        first_frame_recorded,
                    },
                ),
            };
            Ok(response)
        })
    }
}

async fn rewrite_location_body<RestBody>(
    body: RestBody,
    scheme: &str,
    host: Option<&str>,
    virtual_hosted_bucket: Option<&str>,
) -> Result<RestBody, RestBody::Error>
where
    RestBody: Body<Data = Bytes> + From<Bytes>,
{
    let bytes = BodyExt::collect(body).await?.to_bytes();
    let xml = String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
    match rewrite_post_response_location(&xml, scheme, host, virtual_hosted_bucket) {
        Some(fixed) => Ok(RestBody::from(Bytes::from(fixed))),
        None => Ok(RestBody::from(Bytes::from(xml))),
    }
}

fn is_bounded_xml_body<B: Body>(body: &B) -> bool {
    body.size_hint()
        .upper()
        .is_some_and(|length| length <= MAX_XML_COMPAT_BODY_BYTES)
}

fn is_xml_content_type(headers: &HeaderMap) -> bool {
    let is_xml = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|content_type| content_type.to_ascii_lowercase().contains("xml"))
        .unwrap_or(false);
    if !is_xml {
        return false;
    }
    match headers.get(header::CONTENT_ENCODING).and_then(|value| value.to_str().ok()) {
        Some(encoding) => encoding.trim().is_empty() || encoding.eq_ignore_ascii_case("identity"),
        None => true,
    }
}

fn request_scheme(headers: &HeaderMap, uri: &Uri, connection_is_tls: bool) -> String {
    get_source_scheme(headers)
        .and_then(|value| {
            value
                .split(',')
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        })
        .or_else(|| uri.scheme_str().map(str::to_owned))
        .or_else(|| connection_is_tls.then(|| "https".to_string()))
        .unwrap_or_else(|| "http".to_string())
        .to_ascii_lowercase()
}

fn request_host<'a>(headers: &'a HeaderMap, uri: &'a Uri) -> Option<&'a str> {
    uri.authority().map(|authority| authority.as_str()).or_else(|| {
        headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    })
}

/// Absolute object URI for a POST Object `PostResponse` `<Location>`.
///
/// Path-style requests include the bucket in the path. Virtual-hosted requests
/// already carry the bucket in the host, so only the encoded key remains.
/// When no host is available the encoded path is returned without a scheme.
pub(crate) fn post_object_location(scheme: &str, host: Option<&str>, bucket: &str, key: &str, virtual_hosted: bool) -> String {
    let mut path = String::new();
    path.push('/');
    if !virtual_hosted {
        path.push_str(bucket);
        path.push('/');
    }
    push_encoded_object_path(&mut path, key.as_bytes());

    match host.map(str::trim).filter(|value| !value.is_empty()) {
        Some(host) => format!("{scheme}://{host}{path}"),
        None => path,
    }
}

fn push_encoded_object_path(out: &mut String, bytes: &[u8]) {
    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(char::from(hex_digit(byte >> 4)));
            out.push(char::from(hex_digit(byte & 0x0f)));
        }
    }
}

const fn hex_digit(nibble: u8) -> u8 {
    match nibble {
        0..=9 => b'0' + nibble,
        10..=15 => b'A' + nibble - 10,
        _ => b'?',
    }
}

pub(crate) fn rewrite_post_response_location(
    xml: &str,
    scheme: &str,
    host: Option<&str>,
    virtual_hosted_bucket: Option<&str>,
) -> Option<String> {
    if !xml.contains("<PostResponse") {
        return None;
    }
    let bucket = quick_xml::escape::unescape(xml_element(xml, "Bucket")?).ok()?;
    let key = quick_xml::escape::unescape(xml_element(xml, "Key")?).ok()?;
    let virtual_hosted = virtual_hosted_bucket.is_some_and(|name| name.eq_ignore_ascii_case(&bucket));
    let location = xml_escape(&post_object_location(scheme, host, &bucket, &key, virtual_hosted));
    let (start, end) = xml_element_span(xml, "Location")?;
    if xml[start..end] == location {
        return None;
    }
    let mut fixed = String::with_capacity(xml.len() - (end - start) + location.len());
    fixed.push_str(&xml[..start]);
    fixed.push_str(&location);
    fixed.push_str(&xml[end..]);
    Some(fixed)
}

fn xml_element<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let (start, end) = xml_element_span(xml, name)?;
    Some(&xml[start..end])
}

fn xml_element_span(xml: &str, name: &str) -> Option<(usize, usize)> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = xml.find(&open)? + open.len();
    let end = start + xml[start..].find(&close)?;
    Some((start, end))
}

fn xml_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_reserved_and_unicode_key_characters_in_an_absolute_path_style_url() {
        let location = post_object_location("http", Some("s3.rustfs.localhost:9000"), "my-bucket", "test/a+b c%d.txt", false);
        assert_eq!(location, "http://s3.rustfs.localhost:9000/my-bucket/test/a%2Bb%20c%25d.txt");
    }

    #[test]
    fn virtual_host_omits_the_bucket_from_the_path() {
        let location =
            post_object_location("https", Some("my-bucket.s3.rustfs.localhost:9000"), "my-bucket", "test/a+b c%d.txt", true);
        assert_eq!(location, "https://my-bucket.s3.rustfs.localhost:9000/test/a%2Bb%20c%25d.txt");
    }

    #[test]
    fn leaves_unreserved_characters_and_slashes_in_the_key() {
        let location = post_object_location("http", Some("host.example:9000"), "bucket", "a-b_c.d~e/photo.jpg", false);
        assert_eq!(location, "http://host.example:9000/bucket/a-b_c.d~e/photo.jpg");
    }

    #[test]
    fn percent_encodes_ampersand_question_hash_and_utf8() {
        let location = post_object_location("http", Some("host"), "bucket", "naïve/a&b?c#d.txt", false);
        assert_eq!(location, "http://host/bucket/na%C3%AFve/a%26b%3Fc%23d.txt");
    }

    #[test]
    fn falls_back_to_an_encoded_path_when_the_host_is_missing() {
        assert_eq!(
            post_object_location("http", None, "bucket", "dir/file name.txt", false),
            "/bucket/dir/file%20name.txt"
        );
    }

    #[test]
    fn uses_x_forwarded_proto_and_host_header_when_rewriting_xml() {
        let xml = concat!(
            r#"<?xml version="1.0" encoding="UTF-8"?>"#,
            "<PostResponse>",
            "<Location>/my-bucket/test/a+b c%d.txt</Location>",
            "<Bucket>my-bucket</Bucket>",
            "<Key>test/a+b c%d.txt</Key>",
            r#"<ETag>9dd4e461268c8034f5c8564e155c67a6</ETag>"#,
            "</PostResponse>"
        );
        let rewritten = rewrite_post_response_location(xml, "https", Some("s3.example:9000"), None).unwrap();
        assert!(
            rewritten.contains("<Location>https://s3.example:9000/my-bucket/test/a%2Bb%20c%25d.txt</Location>"),
            "{rewritten}"
        );
        assert!(rewritten.contains("<Key>test/a+b c%d.txt</Key>"), "{rewritten}");
        assert!(rewritten.contains("<Bucket>my-bucket</Bucket>"), "{rewritten}");
    }

    #[test]
    fn unescapes_xml_entities_in_the_stored_key_before_encoding() {
        let xml = concat!(
            "<PostResponse>",
            "<Location>/example-bucket/uploads/a b&amp;résumé?.txt</Location>",
            "<Bucket>example-bucket</Bucket>",
            "<Key>uploads/a b&amp;résumé?.txt</Key>",
            "<ETag>storage-etag</ETag>",
            "</PostResponse>"
        );
        let rewritten = rewrite_post_response_location(xml, "http", Some("host.invalid"), None).unwrap();
        assert!(
            rewritten.contains("<Location>http://host.invalid/example-bucket/uploads/a%20b%26r%C3%A9sum%C3%A9%3F.txt</Location>"),
            "{rewritten}"
        );
        assert!(rewritten.contains("<Key>uploads/a b&amp;résumé?.txt</Key>"), "{rewritten}");
    }

    #[test]
    fn keeps_entity_like_text_in_the_object_key() {
        let xml = concat!(
            "<PostResponse><Location>/bucket/&amp;lt;.txt</Location>",
            "<Bucket>bucket</Bucket><Key>&amp;lt;.txt</Key></PostResponse>"
        );
        let rewritten = rewrite_post_response_location(xml, "https", Some("s3.example"), None).unwrap();
        assert!(rewritten.contains("<Location>https://s3.example/bucket/%26lt%3B.txt</Location>"));
        assert!(rewritten.contains("<Key>&amp;lt;.txt</Key>"));
    }

    #[test]
    fn leaves_non_post_response_xml_unchanged() {
        let xml = "<CompleteMultipartUploadResult><Location>/bucket/key</Location></CompleteMultipartUploadResult>";
        assert_eq!(rewrite_post_response_location(xml, "http", Some("host"), None), None);
    }

    #[test]
    fn request_scheme_prefers_forwarded_proto() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", http::HeaderValue::from_static("https,http"));
        assert_eq!(request_scheme(&headers, &Uri::from_static("http://ignored/bucket"), false), "https");
    }

    #[test]
    fn request_scheme_falls_back_to_a_tls_connection() {
        assert_eq!(request_scheme(&HeaderMap::new(), &Uri::from_static("/bucket"), true), "https");
        assert_eq!(request_scheme(&HeaderMap::new(), &Uri::from_static("/bucket"), false), "http");
    }
}
