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

use super::utils::get_host_addr;
use rustfs_utils::crypto::{base64_encode, hex, hmac_sha1};
use s3s::Body;

const _SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SIGN_V2_ALGORITHM: &str = "AWS";

fn encode_url2path(req: &request::Request<Body>, virtual_host: bool) -> String {
    // In virtual-hosted-style, the canonical resource must include "/{bucket}" prefix
    // extracted from the Host header: bucket.domain.tld -> "/bucket"
    let mut path = req.uri().path().to_string();
    if virtual_host {
        let host = super::utils::get_host_addr(req);
        // strip port if present
        let host = match host.split_once(':') {
            Some((h, _)) => h,
            None => host.as_str(),
        };
        // If host has at least 3 labels (bucket + domain + tld), take first label as bucket
        if let Some((bucket, _rest)) = host.split_once('.') {
            if !bucket.is_empty() {
                // avoid duplicating if path already starts with /bucket/
                let expected_prefix = format!("/{bucket}");
                if !path.starts_with(&expected_prefix) {
                    // Only prefix for bucket-level paths; ensure a single slash separation
                    if path == "/" {
                        path = expected_prefix;
                    } else {
                        path = format!(
                            "{}{}",
                            expected_prefix,
                            if path.starts_with('/') {
                                path.clone()
                            } else {
                                format!("/{path}")
                            }
                        );
                    }
                }
            }
        }
    }
    path
}

pub fn pre_sign_v2(
    mut req: request::Request<Body>,
    access_key_id: &str,
    secret_access_key: &str,
    expires: i64,
    virtual_host: bool,
) -> request::Request<Body> {
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return req;
    }

    let d = OffsetDateTime::now_utc();
    let d = d.replace_time(time::Time::from_hms(0, 0, 0).unwrap());
    let epoch_expires = d.unix_timestamp() + expires;

    let headers = req.headers_mut();
    let expires_str = headers.get("Expires");
    if expires_str.is_none() {
        headers.insert("Expires", format!("{epoch_expires:010}").parse().unwrap());
    }

    let string_to_sign = pre_string_to_sign_v2(&req, virtual_host);
    let signature = hex(hmac_sha1(secret_access_key, string_to_sign));

    let result = serde_urlencoded::from_str::<HashMap<String, String>>(req.uri().query().unwrap());
    let mut query = result.unwrap_or_default();
    if get_host_addr(&req).contains(".storage.googleapis.com") {
        query.insert("GoogleAccessId".to_string(), access_key_id.to_string());
    } else {
        query.insert("AWSAccessKeyId".to_string(), access_key_id.to_string());
    }

    query.insert("Expires".to_string(), format!("{epoch_expires:010}"));

    let uri = req.uri().clone();
    let mut parts = req.uri().clone().into_parts();
    parts.path_and_query = Some(
        format!("{}?{}&Signature={}", uri.path(), serde_urlencoded::to_string(&query).unwrap(), signature)
            .parse()
            .unwrap(),
    );

    *req.uri_mut() = Uri::from_parts(parts).unwrap();

    req
}

fn _post_pre_sign_signature_v2(policy_base64: &str, secret_access_key: &str) -> String {
    hex(hmac_sha1(secret_access_key, policy_base64))
}

pub fn sign_v2(
    mut req: request::Request<Body>,
    _content_len: i64,
    access_key_id: &str,
    secret_access_key: &str,
    virtual_host: bool,
) -> request::Request<Body> {
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return req;
    }

    let d = OffsetDateTime::now_utc();
    let d2 = d.replace_time(time::Time::from_hms(0, 0, 0).unwrap());

    let string_to_sign = string_to_sign_v2(&req, virtual_host);
    let headers = req.headers_mut();

    let date = headers.get("Date").unwrap();
    if date.to_str().unwrap() == "" {
        headers.insert(
            "Date",
            d2.format(&format_description::well_known::Rfc2822)
                .unwrap()
                .to_string()
                .parse()
                .unwrap(),
        );
    }

    let auth_header = format!("{SIGN_V2_ALGORITHM} {access_key_id}:");
    let auth_header = format!("{}{}", auth_header, base64_encode(&hmac_sha1(secret_access_key, string_to_sign)));

    headers.insert("Authorization", auth_header.parse().unwrap());

    req
}

fn pre_string_to_sign_v2(req: &request::Request<Body>, virtual_host: bool) -> String {
    let mut buf = BytesMut::new();
    write_pre_sign_v2_headers(&mut buf, req);
    write_canonicalized_headers(&mut buf, req);
    write_canonicalized_resource(&mut buf, req, virtual_host);
    String::from_utf8(buf.to_vec()).unwrap()
}

fn write_pre_sign_v2_headers(buf: &mut BytesMut, req: &request::Request<Body>) {
    let _ = buf.write_str(req.method().as_str());
    let _ = buf.write_char('\n');
    let _ = buf.write_str(req.headers().get("Content-Md5").unwrap().to_str().unwrap());
    let _ = buf.write_char('\n');
    let _ = buf.write_str(req.headers().get("Content-Type").unwrap().to_str().unwrap());
    let _ = buf.write_char('\n');
    let _ = buf.write_str(req.headers().get("Expires").unwrap().to_str().unwrap());
    let _ = buf.write_char('\n');
}

fn string_to_sign_v2(req: &request::Request<Body>, virtual_host: bool) -> String {
    let mut buf = BytesMut::new();
    write_sign_v2_headers(&mut buf, req);
    write_canonicalized_headers(&mut buf, req);
    write_canonicalized_resource(&mut buf, req, virtual_host);
    String::from_utf8(buf.to_vec()).unwrap()
}

fn write_sign_v2_headers(buf: &mut BytesMut, req: &request::Request<Body>) {
    let headers = req.headers();
    let _ = buf.write_str(req.method().as_str());
    let _ = buf.write_char('\n');
    let _ = buf.write_str(headers.get("Content-Md5").unwrap().to_str().unwrap());
    let _ = buf.write_char('\n');
    let _ = buf.write_str(headers.get("Content-Type").unwrap().to_str().unwrap());
    let _ = buf.write_char('\n');
    let _ = buf.write_str(headers.get("Date").unwrap().to_str().unwrap());
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
                .map(|e| e.to_str().unwrap().to_string())
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
    if request_url.query().unwrap() != "" {
        let mut n: i64 = 0;
        let result = serde_urlencoded::from_str::<HashMap<String, Vec<String>>>(req.uri().query().unwrap());
        let vals = result.unwrap_or_default();
        for resource in INCLUDED_QUERY {
            let vv = &vals[*resource];
            if !vv.is_empty() {
                n += 1;
                match n {
                    1 => {
                        let _ = buf.write_char('?');
                    }
                    _ => {
                        let _ = buf.write_char('&');
                        let _ = buf.write_str(resource);
                        if !vv[0].is_empty() {
                            let _ = buf.write_char('=');
                            let _ = buf.write_str(&vv[0]);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;

    fn mk_req(host: &str, path: &str, query: &str) -> request::Request<Body> {
        let uri = if query.is_empty() {
            format!("http://{host}{path}")
        } else {
            format!("http://{host}{path}?{query}")
        };
        let mut req = Request::builder().uri(uri).method("GET").body(Body::empty()).unwrap();
        // minimal headers used by signers
        let h = req.headers_mut();
        h.insert("Content-Md5", "".parse().unwrap());
        h.insert("Content-Type", "".parse().unwrap());
        h.insert("Date", "Thu, 21 Aug 2025 00:00:00 +0000".parse().unwrap());
        h.insert("host", host.parse().unwrap());
        req
    }

    #[test]
    fn test_encode_url2path_vhost_prefixes_bucket() {
        let req = mk_req("test.example.com", "/obj.txt", "");
        let path = encode_url2path(&req, true);
        assert_eq!(path, "/test/obj.txt");
    }

    #[test]
    fn test_encode_url2path_path_style_unchanged() {
        let req = mk_req("example.com", "/test/obj.txt", "uploads=");
        let path = encode_url2path(&req, false);
        assert_eq!(path, "/test/obj.txt");
    }
}
