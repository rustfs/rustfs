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

use bytes::BytesMut;
use http::request;
use hyper::Uri;
use std::collections::HashMap;
use std::fmt::Write;
use time::{OffsetDateTime, format_description};
use tracing::warn;

use http::HeaderValue;

use super::utils::{HostAddrError, get_host_addr};
use rustfs_utils::crypto::{hex, hmac_sha1};
use s3s::Body;

const _SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SIGN_V2_ALGORITHM: &str = "AWS";

#[derive(Debug, thiserror::Error)]
pub enum SignV2Error {
    #[error("invalid UTF-8 header value for `{name}`")]
    InvalidHeaderValue { name: String },
    #[error("failed to format signing timestamp: {reason}")]
    TimeFormat { reason: String },
    #[error("failed to build signing timestamp: {reason}")]
    TimeComponent { reason: String },
    #[error("failed to encode query parameters: {reason}")]
    QueryEncode { reason: String },
    #[error("failed to parse uri: {reason}")]
    InvalidUri { reason: String },
    #[error("failed to build uri from parts: {reason}")]
    InvalidUriParts { reason: String },
    #[error("failed to convert canonical headers to UTF-8: {reason}")]
    CanonicalUtf8 { reason: String },
    #[error("failed to parse header value for `{name}`: {reason}")]
    HeaderValueParse { name: String, reason: String },
    #[error("failed to resolve host address: {0}")]
    HostAddr(#[from] HostAddrError),
}

#[derive(Debug)]
struct SignV2Failure {
    request: request::Request<Body>,
    error: SignV2Error,
}

type SignV2Outcome = std::result::Result<request::Request<Body>, Box<SignV2Failure>>;

fn sign_v2_fail(request: request::Request<Body>, error: SignV2Error) -> SignV2Outcome {
    Err(Box::new(SignV2Failure { request, error }))
}

fn encode_url2path(req: &request::Request<Body>, _virtual_host: bool) -> String {
    req.uri().path().to_string()
}

fn pre_sign_v2_inner(
    mut req: request::Request<Body>,
    access_key_id: &str,
    secret_access_key: &str,
    expires: i64,
    virtual_host: bool,
) -> SignV2Outcome {
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return Ok(req);
    }

    let d = OffsetDateTime::now_utc();
    let d = match time::Time::from_hms(0, 0, 0) {
        Ok(midnight) => d.replace_time(midnight),
        Err(err) => return sign_v2_fail(req, SignV2Error::TimeComponent { reason: err.to_string() }),
    };
    let epoch_expires = d.unix_timestamp() + expires;

    let headers = req.headers_mut();
    let expires_str = headers.get("Expires");
    if expires_str.is_none() {
        let expires_value = match format!("{epoch_expires:010}").parse::<HeaderValue>() {
            Ok(v) => v,
            Err(err) => {
                return sign_v2_fail(
                    req,
                    SignV2Error::HeaderValueParse {
                        name: "Expires".to_string(),
                        reason: err.to_string(),
                    },
                );
            }
        };
        headers.insert("Expires", expires_value);
    }

    let string_to_sign = match try_pre_string_to_sign_v2(&req, virtual_host) {
        Ok(v) => v,
        Err(err) => return sign_v2_fail(req, err),
    };
    let signature = hex(hmac_sha1(secret_access_key, string_to_sign));

    let query_source = req.uri().query().unwrap_or("");
    let result = serde_urlencoded::from_str::<HashMap<String, String>>(query_source);
    let mut query = result.unwrap_or_default();
    let host_addr = match get_host_addr(&req) {
        Ok(v) => v,
        Err(err) => return sign_v2_fail(req, SignV2Error::HostAddr(err)),
    };
    if host_addr.contains(".storage.googleapis.com") {
        query.insert("GoogleAccessId".to_string(), access_key_id.to_string());
    } else {
        query.insert("AWSAccessKeyId".to_string(), access_key_id.to_string());
    }

    query.insert("Expires".to_string(), format!("{epoch_expires:010}"));

    let uri = req.uri().clone();
    let mut parts = req.uri().clone().into_parts();
    let query_str = match serde_urlencoded::to_string(&query) {
        Ok(v) => v,
        Err(err) => return sign_v2_fail(req, SignV2Error::QueryEncode { reason: err.to_string() }),
    };
    parts.path_and_query = Some(
        match format!("{}?{}&Signature={}", uri.path(), query_str, signature).parse() {
            Ok(v) => v,
            Err(err) => return sign_v2_fail(req, SignV2Error::InvalidUri { reason: err.to_string() }),
        },
    );

    *req.uri_mut() = match Uri::from_parts(parts) {
        Ok(v) => v,
        Err(err) => return sign_v2_fail(req, SignV2Error::InvalidUriParts { reason: err.to_string() }),
    };

    Ok(req)
}

pub fn try_pre_sign_v2(
    req: request::Request<Body>,
    access_key_id: &str,
    secret_access_key: &str,
    expires: i64,
    virtual_host: bool,
) -> Result<request::Request<Body>, SignV2Error> {
    pre_sign_v2_inner(req, access_key_id, secret_access_key, expires, virtual_host).map_err(|f| f.error)
}

pub fn pre_sign_v2(
    req: request::Request<Body>,
    access_key_id: &str,
    secret_access_key: &str,
    expires: i64,
    virtual_host: bool,
) -> request::Request<Body> {
    match pre_sign_v2_inner(req, access_key_id, secret_access_key, expires, virtual_host) {
        Ok(request) => request,
        Err(failure) => {
            warn!(error = %failure.error, "failed to presign v2 request");
            failure.request
        }
    }
}

fn _post_pre_sign_signature_v2(policy_base64: &str, secret_access_key: &str) -> String {
    hex(hmac_sha1(secret_access_key, policy_base64))
}

fn sign_v2_inner(
    mut req: request::Request<Body>,
    _content_len: i64,
    access_key_id: &str,
    secret_access_key: &str,
    virtual_host: bool,
) -> SignV2Outcome {
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return Ok(req);
    }

    let d = OffsetDateTime::now_utc();
    let d2 = match time::Time::from_hms(0, 0, 0) {
        Ok(midnight) => d.replace_time(midnight),
        Err(err) => return sign_v2_fail(req, SignV2Error::TimeComponent { reason: err.to_string() }),
    };

    {
        let headers = req.headers_mut();
        let need_default_date = headers.get("Date").and_then(|v| v.to_str().ok()).is_none_or(|v| v.is_empty());
        if need_default_date {
            let date_str = match d2.format(&format_description::well_known::Rfc2822) {
                Ok(v) => v,
                Err(err) => return sign_v2_fail(req, SignV2Error::TimeFormat { reason: err.to_string() }),
            };
            let date_value = match date_str.parse::<HeaderValue>() {
                Ok(v) => v,
                Err(err) => {
                    return sign_v2_fail(
                        req,
                        SignV2Error::HeaderValueParse {
                            name: "Date".to_string(),
                            reason: err.to_string(),
                        },
                    );
                }
            };
            headers.insert("Date", date_value);
        }
    }
    let string_to_sign = match try_string_to_sign_v2(&req, virtual_host) {
        Ok(v) => v,
        Err(err) => return sign_v2_fail(req, err),
    };
    let headers = req.headers_mut();

    let auth_header = format!("{SIGN_V2_ALGORITHM} {access_key_id}:");
    let auth_header = format!(
        "{}{}",
        auth_header,
        base64_simd::URL_SAFE_NO_PAD.encode_to_string(hmac_sha1(secret_access_key, string_to_sign))
    );

    let auth_value = match auth_header.parse::<HeaderValue>() {
        Ok(v) => v,
        Err(err) => {
            return sign_v2_fail(
                req,
                SignV2Error::HeaderValueParse {
                    name: "Authorization".to_string(),
                    reason: err.to_string(),
                },
            );
        }
    };
    headers.insert("Authorization", auth_value);

    Ok(req)
}

pub fn try_sign_v2(
    req: request::Request<Body>,
    content_len: i64,
    access_key_id: &str,
    secret_access_key: &str,
    virtual_host: bool,
) -> Result<request::Request<Body>, SignV2Error> {
    sign_v2_inner(req, content_len, access_key_id, secret_access_key, virtual_host).map_err(|f| f.error)
}

pub fn sign_v2(
    req: request::Request<Body>,
    content_len: i64,
    access_key_id: &str,
    secret_access_key: &str,
    virtual_host: bool,
) -> request::Request<Body> {
    match sign_v2_inner(req, content_len, access_key_id, secret_access_key, virtual_host) {
        Ok(request) => request,
        Err(failure) => {
            warn!(error = %failure.error, "failed to sign v2 request");
            failure.request
        }
    }
}

fn try_pre_string_to_sign_v2(req: &request::Request<Body>, virtual_host: bool) -> Result<String, SignV2Error> {
    let mut buf = BytesMut::new();
    write_pre_sign_v2_headers(&mut buf, req);
    write_canonicalized_headers(&mut buf, req);
    write_canonicalized_resource(&mut buf, req, virtual_host);
    String::from_utf8(buf.to_vec()).map_err(|err| SignV2Error::CanonicalUtf8 { reason: err.to_string() })
}

#[allow(dead_code)]
fn pre_string_to_sign_v2(req: &request::Request<Body>, virtual_host: bool) -> String {
    try_pre_string_to_sign_v2(req, virtual_host).expect("pre_string_to_sign_v2 produced non-UTF8 data")
}

fn write_pre_sign_v2_headers(buf: &mut BytesMut, req: &request::Request<Body>) {
    let _ = buf.write_str(req.method().as_str());
    let _ = buf.write_char('\n');
    let _ = buf.write_str(req.headers().get("Content-Md5").and_then(|v| v.to_str().ok()).unwrap_or(""));
    let _ = buf.write_char('\n');
    let _ = buf.write_str(req.headers().get("Content-Type").and_then(|v| v.to_str().ok()).unwrap_or(""));
    let _ = buf.write_char('\n');
    let _ = buf.write_str(req.headers().get("Expires").and_then(|v| v.to_str().ok()).unwrap_or(""));
    let _ = buf.write_char('\n');
}

fn try_string_to_sign_v2(req: &request::Request<Body>, virtual_host: bool) -> Result<String, SignV2Error> {
    let mut buf = BytesMut::new();
    write_sign_v2_headers(&mut buf, req);
    write_canonicalized_headers(&mut buf, req);
    write_canonicalized_resource(&mut buf, req, virtual_host);
    String::from_utf8(buf.to_vec()).map_err(|err| SignV2Error::CanonicalUtf8 { reason: err.to_string() })
}

#[allow(dead_code)]
fn string_to_sign_v2(req: &request::Request<Body>, virtual_host: bool) -> String {
    try_string_to_sign_v2(req, virtual_host).expect("string_to_sign_v2 produced non-UTF8 data")
}

fn write_sign_v2_headers(buf: &mut BytesMut, req: &request::Request<Body>) {
    let headers = req.headers();
    let _ = buf.write_str(req.method().as_str());
    let _ = buf.write_char('\n');
    let _ = buf.write_str(headers.get("Content-Md5").and_then(|v| v.to_str().ok()).unwrap_or(""));
    let _ = buf.write_char('\n');
    let _ = buf.write_str(headers.get("Content-Type").and_then(|v| v.to_str().ok()).unwrap_or(""));
    let _ = buf.write_char('\n');
    let _ = buf.write_str(headers.get("Date").and_then(|v| v.to_str().ok()).unwrap_or(""));
    let _ = buf.write_char('\n');
}

fn write_canonicalized_headers(buf: &mut BytesMut, req: &request::Request<Body>) {
    let mut proto_headers = Vec::<String>::new();
    let mut vals = HashMap::<String, Vec<String>>::new();
    for k in req.headers().keys() {
        let lk = k.as_str().to_lowercase();
        if lk.starts_with("x-amz") {
            proto_headers.push(lk.clone());
            let vv = req
                .headers()
                .get_all(k)
                .iter()
                .filter_map(|e| e.to_str().ok().map(ToString::to_string))
                .collect();
            vals.insert(lk, vv);
        }
    }
    proto_headers.sort();
    for k in proto_headers {
        let _ = buf.write_str(&k);
        let _ = buf.write_char(':');
        for (idx, v) in vals[&k].iter().enumerate() {
            if idx > 0 {
                let _ = buf.write_char(',');
            }
            let _ = buf.write_str(v);
        }
        let _ = buf.write_char('\n');
    }
}

const INCLUDED_QUERY: &[&str] = &[
    "acl",
    "delete",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "partNumber",
    "policy",
    "requestPayment",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website",
];

fn write_canonicalized_resource(buf: &mut BytesMut, req: &request::Request<Body>, virtual_host: bool) {
    let request_url = req.uri();
    let _ = buf.write_str(&encode_url2path(req, virtual_host));
    if let Some(query_str) = request_url.query().filter(|query| !query.is_empty()) {
        let mut query_vals = HashMap::new();
        for pair in query_str.split('&') {
            let mut iter = pair.splitn(2, '=');
            let key = match iter.next() {
                Some(k) if !k.is_empty() => k,
                _ => continue,
            };
            let value = iter.next().unwrap_or("");
            query_vals.insert(key.to_string(), value.to_string());
        }

        let mut canonical = Vec::<String>::new();
        for resource in INCLUDED_QUERY {
            if let Some(value) = query_vals.get(*resource) {
                let mut item = resource.to_string();
                if !value.is_empty() {
                    let _ = write!(&mut item, "={value}");
                }
                canonical.push(item);
            }
        }
        if !canonical.is_empty() {
            let _ = buf.write_char('?');
            let _ = buf.write_str(&canonical.join("&"));
        }
    }
}

#[cfg(test)]
#[allow(unused_variables, unused_mut)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_pre_sign_v2_without_query_should_keep_safe() {
        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://examplebucket.s3.amazonaws.com/object")
            .body(Body::empty())
            .unwrap();
        req.headers_mut()
            .insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());

        let req = pre_sign_v2(req, "AKIAEXAMPLE", "SECRET", 60, false);
        let query = req.uri().query().unwrap_or_default();
        let values = serde_urlencoded::from_str::<HashMap<String, String>>(query).unwrap();

        assert!(values.contains_key("AWSAccessKeyId"));
        assert!(values.contains_key("Expires"));
        assert!(values.contains_key("Signature"));
    }

    #[test]
    fn test_sign_v2_missing_optional_headers() {
        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://examplebucket.s3.amazonaws.com/object")
            .body(Body::empty())
            .unwrap();
        req.headers_mut()
            .insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());

        let req = sign_v2(req, 0, "AKIAEXAMPLE", "SECRET", false);
        assert!(req.headers().get("Date").is_some());
        assert!(req.headers().get("Authorization").is_some());
    }

    #[test]
    fn test_write_canonicalized_resource_with_single_query_param() {
        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://examplebucket.s3.amazonaws.com/object?acl")
            .body(Body::empty())
            .unwrap();
        req.headers_mut()
            .insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());
        let mut buf = BytesMut::new();
        write_canonicalized_resource(&mut buf, &req, false);
        assert_eq!(String::from_utf8(buf.to_vec()).unwrap(), "/object?acl");
    }

    #[test]
    fn test_sign_v2_signature_matches_injected_date() {
        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://examplebucket.s3.amazonaws.com/object")
            .body(Body::empty())
            .unwrap();
        req.headers_mut()
            .insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());

        let req = sign_v2(req, 0, "AKIAEXAMPLE", "SECRET", false);
        let expected_string_to_sign = string_to_sign_v2(&req, false);
        let expected_signature = base64_simd::URL_SAFE_NO_PAD.encode_to_string(hmac_sha1("SECRET", expected_string_to_sign));

        assert_eq!(
            req.headers().get("Authorization").unwrap().to_str().unwrap(),
            format!("AWS AKIAEXAMPLE:{expected_signature}")
        );
    }
}
