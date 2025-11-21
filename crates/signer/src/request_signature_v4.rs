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
use http::HeaderMap;
use http::Uri;
use http::request;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::LazyLock;
use time::{OffsetDateTime, macros::format_description};
use tracing::debug;

use super::constants::UNSIGNED_PAYLOAD;
use super::request_signature_streaming_unsigned_trailer::streaming_unsigned_v4;
use super::utils::{get_host_addr, sign_v4_trim_all};
use rustfs_utils::crypto::{hex, hex_sha256, hmac_sha256};
use s3s::Body;

pub const SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const SERVICE_TYPE_S3: &str = "s3";
pub const SERVICE_TYPE_STS: &str = "sts";

#[allow(non_upper_case_globals)] // FIXME
static v4_ignored_headers: LazyLock<HashMap<String, bool>> = LazyLock::new(|| {
    let mut m = <HashMap<String, bool>>::new();
    m.insert("accept-encoding".to_string(), true);
    m.insert("authorization".to_string(), true);
    m.insert("user-agent".to_string(), true);
    m
});

pub fn get_signing_key(secret: &str, loc: &str, t: OffsetDateTime, service_type: &str) -> [u8; 32] {
    let mut s = "AWS4".to_string();
    s.push_str(secret);
    let format = format_description!("[year][month][day]");
    let date = hmac_sha256(s.into_bytes(), t.format(&format).unwrap().into_bytes());
    let location = hmac_sha256(date, loc);
    let service = hmac_sha256(location, service_type);

    hmac_sha256(service, "aws4_request")
}

pub fn get_signature(signing_key: [u8; 32], string_to_sign: &str) -> String {
    hex(hmac_sha256(signing_key, string_to_sign))
}

pub fn get_scope(location: &str, t: OffsetDateTime, service_type: &str) -> String {
    let format = format_description!("[year][month][day]");
    let mut ans = String::from("");
    ans.push_str(&t.format(&format).unwrap().to_string());
    ans.push('/');
    ans.push_str(location);
    ans.push('/');
    ans.push_str(service_type);
    ans.push_str("/aws4_request");
    ans
}

fn get_credential(access_key_id: &str, location: &str, t: OffsetDateTime, service_type: &str) -> String {
    let scope = get_scope(location, t, service_type);
    let mut s = access_key_id.to_string();
    s.push('/');
    s.push_str(&scope);
    s
}

fn get_hashed_payload(req: &request::Request<Body>) -> String {
    let headers = req.headers();
    let mut hashed_payload = "";
    if let Some(payload) = headers.get("X-Amz-Content-Sha256") {
        hashed_payload = payload.to_str().unwrap();
    }
    if hashed_payload.is_empty() {
        hashed_payload = UNSIGNED_PAYLOAD;
    }
    hashed_payload.to_string()
}

fn get_canonical_headers(req: &request::Request<Body>, ignored_headers: &HashMap<String, bool>) -> String {
    let mut headers = Vec::<String>::new();
    let mut vals = HashMap::<String, Vec<String>>::new();
    for k in req.headers().keys() {
        if ignored_headers.get(&k.to_string()).is_some() {
            continue;
        }
        headers.push(k.as_str().to_lowercase());
        let vv = req
            .headers()
            .get_all(k)
            .iter()
            .map(|e| e.to_str().unwrap().to_string())
            .collect();
        vals.insert(k.as_str().to_lowercase(), vv);
    }
    if !header_exists("host", &headers) {
        headers.push("host".to_string());
    }
    headers.sort();

    debug!("get_canonical_headers vals: {:?}", vals);
    debug!("get_canonical_headers headers: {:?}", headers);

    let mut buf = BytesMut::new();
    for k in headers {
        let _ = buf.write_str(&k);
        let _ = buf.write_char(':');
        let k: &str = &k;
        match k {
            "host" => {
                let _ = buf.write_str(&get_host_addr(req));
                let _ = buf.write_char('\n');
            }
            _ => {
                for (idx, v) in vals[k].iter().enumerate() {
                    if idx > 0 {
                        let _ = buf.write_char(',');
                    }
                    let _ = buf.write_str(&sign_v4_trim_all(v));
                }
                let _ = buf.write_char('\n');
            }
        }
    }
    String::from_utf8(buf.to_vec()).unwrap()
}

fn header_exists(key: &str, headers: &[String]) -> bool {
    for k in headers {
        if k == key {
            return true;
        }
    }
    false
}

fn get_signed_headers(req: &request::Request<Body>, ignored_headers: &HashMap<String, bool>) -> String {
    let mut headers = Vec::<String>::new();
    let headers_ref = req.headers();
    debug!("get_signed_headers headers: {:?}", headers_ref);
    for (k, _) in headers_ref {
        if ignored_headers.get(&k.to_string()).is_some() {
            continue;
        }
        headers.push(k.as_str().to_lowercase());
    }
    if !header_exists("host", &headers) {
        headers.push("host".to_string());
    }
    headers.sort();
    headers.join(";")
}

fn get_canonical_request(req: &request::Request<Body>, ignored_headers: &HashMap<String, bool>, hashed_payload: &str) -> String {
    let mut canonical_query_string = "".to_string();
    if let Some(q) = req.uri().query() {
        // Parse query string into key-value pairs
        let mut query_params: Vec<(String, String)> = Vec::new();
        if !q.is_empty() {
            for param in q.split('&') {
                if let Some((key, value)) = param.split_once('=') {
                    query_params.push((key.to_string(), value.to_string()));
                } else {
                    query_params.push((param.to_string(), "".to_string()));
                }
            }
        }

        // Sort by key name
        query_params.sort_by(|a, b| a.0.cmp(&b.0));

        // Build canonical query string
        //println!("query_params: {query_params:?}");
        let sorted_params: Vec<String> = query_params.iter().map(|(k, v)| format!("{k}={v}")).collect();

        canonical_query_string = sorted_params.join("&");
        canonical_query_string = canonical_query_string.replace("+", "%20");
    }

    let canonical_request = [
        req.method().to_string(),
        req.uri().path().to_string(),
        canonical_query_string,
        get_canonical_headers(req, ignored_headers),
        get_signed_headers(req, ignored_headers),
        hashed_payload.to_string(),
    ];
    canonical_request.join("\n")
}

fn get_string_to_sign_v4(t: OffsetDateTime, location: &str, canonical_request: &str, service_type: &str) -> String {
    let mut string_to_sign = SIGN_V4_ALGORITHM.to_string();
    string_to_sign.push('\n');
    let format = format_description!("[year][month][day]T[hour][minute][second]Z");
    string_to_sign.push_str(&t.format(&format).unwrap());
    string_to_sign.push('\n');
    string_to_sign.push_str(&get_scope(location, t, service_type));
    string_to_sign.push('\n');
    string_to_sign.push_str(&hex_sha256(canonical_request.as_bytes(), |s| s.to_string()));
    string_to_sign
}

pub fn pre_sign_v4(
    req: request::Request<Body>,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    location: &str,
    expires: i64,
    t: OffsetDateTime,
) -> request::Request<Body> {
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return req;
    }

    let credential = get_credential(access_key_id, location, t, SERVICE_TYPE_S3);
    let signed_headers = get_signed_headers(&req, &v4_ignored_headers);

    let mut query = <Vec<(String, String)>>::new();
    if let Some(q) = req.uri().query() {
        let result = serde_urlencoded::from_str::<Vec<(String, String)>>(q);
        query = result.unwrap_or_default();
    }
    query.push(("X-Amz-Algorithm".to_string(), SIGN_V4_ALGORITHM.to_string()));
    let format = format_description!("[year][month][day]T[hour][minute][second]Z");
    query.push(("X-Amz-Date".to_string(), t.format(&format).unwrap().to_string()));
    query.push(("X-Amz-Expires".to_string(), format!("{expires:010}")));
    query.push(("X-Amz-SignedHeaders".to_string(), signed_headers));
    query.push(("X-Amz-Credential".to_string(), credential));
    if !session_token.is_empty() {
        query.push(("X-Amz-Security-Token".to_string(), session_token.to_string()));
    }

    let uri = req.uri().clone();
    let mut parts = req.uri().clone().into_parts();
    parts.path_and_query = Some(
        format!("{}?{}", uri.path(), serde_urlencoded::to_string(&query).unwrap())
            .parse()
            .unwrap(),
    );
    let mut req = req;
    *req.uri_mut() = Uri::from_parts(parts).unwrap();

    let canonical_request = get_canonical_request(&req, &v4_ignored_headers, &get_hashed_payload(&req));
    let string_to_sign = get_string_to_sign_v4(t, location, &canonical_request, SERVICE_TYPE_S3);
    //println!("canonical_request: \n{}\n", canonical_request);
    //println!("string_to_sign: \n{}\n", string_to_sign);
    let signing_key = get_signing_key(secret_access_key, location, t, SERVICE_TYPE_S3);
    let signature = get_signature(signing_key, &string_to_sign);

    let uri = req.uri().clone();
    let mut parts = req.uri().clone().into_parts();
    parts.path_and_query = Some(
        format!(
            "{}?{}&X-Amz-Signature={}",
            uri.path(),
            serde_urlencoded::to_string(&query).unwrap(),
            signature
        )
        .parse()
        .unwrap(),
    );

    *req.uri_mut() = Uri::from_parts(parts).unwrap();

    req
}

fn _post_pre_sign_signature_v4(policy_base64: &str, t: OffsetDateTime, secret_access_key: &str, location: &str) -> String {
    let signing_key = get_signing_key(secret_access_key, location, t, SERVICE_TYPE_S3);

    get_signature(signing_key, policy_base64)
}

fn _sign_v4_sts(
    req: request::Request<Body>,
    access_key_id: &str,
    secret_access_key: &str,
    location: &str,
) -> request::Request<Body> {
    sign_v4_inner(req, 0, access_key_id, secret_access_key, "", location, SERVICE_TYPE_STS, HeaderMap::new())
}

#[allow(clippy::too_many_arguments)]
fn sign_v4_inner(
    mut req: request::Request<Body>,
    content_len: i64,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    location: &str,
    service_type: &str,
    trailer: HeaderMap,
) -> request::Request<Body> {
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return req;
    }

    let t = OffsetDateTime::now_utc();
    let t2 = t.replace_time(time::Time::from_hms(0, 0, 0).unwrap());

    let headers = req.headers_mut();
    let format = format_description!("[year][month][day]T[hour][minute][second]Z");
    headers.insert("X-Amz-Date", t.format(&format).unwrap().to_string().parse().unwrap());

    if !session_token.is_empty() {
        headers.insert("X-Amz-Security-Token", session_token.parse().unwrap());
    }

    if !trailer.is_empty() {
        for (k, _) in &trailer {
            headers.append("X-Amz-Trailer", k.as_str().to_lowercase().parse().unwrap());
        }

        headers.insert("Content-Encoding", "aws-chunked".parse().unwrap());
        headers.insert("x-amz-decoded-content-length", format!("{content_len:010}").parse().unwrap());
    }

    if service_type == SERVICE_TYPE_STS {
        headers.remove("X-Amz-Content-Sha256");
    }

    let hashed_payload = get_hashed_payload(&req);
    let canonical_request = get_canonical_request(&req, &v4_ignored_headers, &hashed_payload);
    let string_to_sign = get_string_to_sign_v4(t, location, &canonical_request, service_type);
    let signing_key = get_signing_key(secret_access_key, location, t, service_type);
    let credential = get_credential(access_key_id, location, t2, service_type);
    let signed_headers = get_signed_headers(&req, &v4_ignored_headers);
    let signature = get_signature(signing_key, &string_to_sign);
    //debug!("\n\ncanonical_request: \n{}\nstring_to_sign: \n{}\nsignature: \n{}\n\n", &canonical_request, &string_to_sign, &signature);

    let headers = req.headers_mut();

    let auth = format!("{SIGN_V4_ALGORITHM} Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}");
    headers.insert("Authorization", auth.parse().unwrap());

    if !trailer.is_empty() {
        //req.Trailer = trailer;
        for (_, v) in &trailer {
            headers.append(http::header::TRAILER, v.clone());
        }
        return streaming_unsigned_v4(req, session_token, content_len, t);
    }
    req
}

fn _unsigned_trailer(mut req: request::Request<Body>, content_len: i64, trailer: HeaderMap) {
    if !trailer.is_empty() {
        return;
    }
    let t = OffsetDateTime::now_utc();
    let t = t.replace_time(time::Time::from_hms(0, 0, 0).unwrap());

    let headers = req.headers_mut();
    let format = format_description!("[year][month][day]T[hour][minute][second]Z");
    headers.insert("X-Amz-Date", t.format(&format).unwrap().to_string().parse().unwrap());

    for (k, _) in &trailer {
        headers.append("X-Amz-Trailer", k.as_str().to_lowercase().parse().unwrap());
    }

    headers.insert("Content-Encoding", "aws-chunked".parse().unwrap());
    headers.insert("x-amz-decoded-content-length", format!("{content_len:010}").parse().unwrap());

    if !trailer.is_empty() {
        for (_, v) in &trailer {
            headers.append(http::header::TRAILER, v.clone());
        }
    }
    streaming_unsigned_v4(req, "", content_len, t);
}

pub fn sign_v4(
    req: request::Request<Body>,
    content_len: i64,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    location: &str,
) -> request::Request<Body> {
    sign_v4_inner(
        req,
        content_len,
        access_key_id,
        secret_access_key,
        session_token,
        location,
        SERVICE_TYPE_S3,
        HeaderMap::new(),
    )
}

pub fn sign_v4_trailer(
    req: request::Request<Body>,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    location: &str,
    trailer: HeaderMap,
) -> request::Request<Body> {
    sign_v4_inner(
        req,
        0,
        access_key_id,
        secret_access_key,
        session_token,
        location,
        SERVICE_TYPE_S3,
        trailer,
    )
}

#[cfg(test)]
#[allow(unused_variables, unused_mut)]
mod tests {
    use http::request;
    use time::macros::datetime;

    use super::*;

    #[test]
    fn example_list_objects() {
        // let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let timestamp = "20130524T000000Z";
        let t = datetime!(2013-05-24 0:00 UTC);
        // let bucket = "examplebucket";
        let region = "us-east-1";
        let service = "s3";
        let path = "/";

        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://examplebucket.s3.amazonaws.com/?")
            .body(Body::empty())
            .unwrap();
        let mut headers = req.headers_mut();
        headers.insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
        );
        headers.insert("x-amz-date", timestamp.parse().unwrap());

        let query = vec![
            ("max-keys".to_string(), "2".to_string()),
            ("prefix".to_string(), "J".to_string()),
        ];
        let uri = req.uri().clone();
        let mut parts = req.uri().clone().into_parts();
        parts.path_and_query = Some(
            format!("{}?{}", uri.path(), serde_urlencoded::to_string(&query).unwrap())
                .parse()
                .unwrap(),
        );
        *req.uri_mut() = Uri::from_parts(parts).unwrap();

        let canonical_request = get_canonical_request(&req, &v4_ignored_headers, &get_hashed_payload(&req));
        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/\n",
                "max-keys=2&prefix=J\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "x-amz-date:",
                "20130524T000000Z",
                "\n",
                "\n",
                "host;x-amz-content-sha256;x-amz-date\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
        );

        let string_to_sign = get_string_to_sign_v4(t, region, &canonical_request, service);
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20130524T000000Z",
                "\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7",
            )
        );

        let signing_key = get_signing_key(secret_access_key, region, t, service);
        let signature = get_signature(signing_key, &string_to_sign);

        assert_eq!(signature, "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7");
    }

    #[test]
    fn example_signature() {
        // let access_key_id = "rustfsadmin";
        let secret_access_key = "rustfsadmin";
        let timestamp = "20250505T011054Z";
        let t = datetime!(2025-05-05 01:10:54 UTC);
        // let bucket = "mblock2";
        let region = "us-east-1";
        let service = "s3";
        let path = "/mblock2/";

        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://192.168.1.11:9020/mblock2/?")
            .body(Body::empty())
            .unwrap();

        let mut headers = req.headers_mut();
        headers.insert("host", "192.168.1.11:9020".parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
        );
        headers.insert("x-amz-date", timestamp.parse().unwrap());

        let mut query: Vec<(String, String)> = Vec::new();
        let uri = req.uri().clone();
        let mut parts = req.uri().clone().into_parts();
        parts.path_and_query = Some(
            format!("{}?{}", uri.path(), serde_urlencoded::to_string(&query).unwrap())
                .parse()
                .unwrap(),
        );
        //println!("parts.path_and_query: {:?}", parts.path_and_query);
        *req.uri_mut() = Uri::from_parts(parts).unwrap();

        let canonical_request = get_canonical_request(&req, &v4_ignored_headers, &get_hashed_payload(&req));
        println!("canonical_request: \n{canonical_request}\n");
        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/mblock2/\n",
                "\n",
                "host:192.168.1.11:9020\n",
                "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "x-amz-date:",
                "20250505T011054Z",
                "\n",
                "\n",
                "host;x-amz-content-sha256;x-amz-date\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
        );

        let string_to_sign = get_string_to_sign_v4(t, region, &canonical_request, service);
        println!("string_to_sign: \n{string_to_sign}\n");
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20250505T011054Z",
                "\n",
                "20250505/us-east-1/s3/aws4_request\n",
                "c2960d00cc7de7bed3e2e2d1330ec298ded8f78a231c1d32dedac72ebec7f9b0",
            )
        );

        let signing_key = get_signing_key(secret_access_key, region, t, service);
        let signature = get_signature(signing_key, &string_to_sign);
        println!("signature: \n{signature}\n");
        assert_eq!(signature, "73fad2dfea0727e10a7179bf49150360a56f2e6b519c53999fd6e011152187d0");
    }

    #[test]
    fn example_signature2() {
        // let access_key_id = "rustfsadmin";
        let secret_access_key = "rustfsadmin";
        let timestamp = "20250507T051030Z";
        let t = datetime!(2025-05-07 05:10:30 UTC);
        // let bucket = "mblock2";
        let region = "us-east-1";
        let service = "s3";
        let path = "/mblock2/";

        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://192.168.1.11:9020/mblock2/?list-type=2&encoding-type=url&prefix=mypre&delimiter=%2F&fetch-owner=true&max-keys=1")
            .body(Body::empty()).unwrap();

        let mut headers = req.headers_mut();
        headers.insert("host", "192.168.1.11:9020".parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
        );
        headers.insert("x-amz-date", timestamp.parse().unwrap());

        println!("{:?}", req.uri().query());
        let canonical_request = get_canonical_request(&req, &v4_ignored_headers, &get_hashed_payload(&req));
        println!("canonical_request: \n{canonical_request}\n");
        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/mblock2/\n",
                "delimiter=%2F&encoding-type=url&fetch-owner=true&list-type=2&max-keys=1&prefix=mypre\n",
                "host:192.168.1.11:9020\n",
                "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "x-amz-date:",
                "20250507T051030Z",
                "\n",
                "\n",
                "host;x-amz-content-sha256;x-amz-date\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
        );

        let string_to_sign = get_string_to_sign_v4(t, region, &canonical_request, service);
        println!("string_to_sign: \n{string_to_sign}\n");
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20250507T051030Z",
                "\n",
                "20250507/us-east-1/s3/aws4_request\n",
                "e6db9e09e9c873aff0b9ca170998b4753f6a6c36c90bc2dca80613affb47f999",
            )
        );

        let signing_key = get_signing_key(secret_access_key, region, t, service);
        let signature = get_signature(signing_key, &string_to_sign);
        println!("signature: \n{signature}\n");
        assert_eq!(signature, "dfbed913d1982428f6224ee506431fc133dbcad184194c0cbf01bc517435788a");
    }

    #[test]
    fn example_signature3() {
        // let access_key_id = "rustfsadmin";
        let secret_access_key = "rustfsadmin";
        let timestamp = "20250628T061107Z";
        let t = datetime!(2025-06-28 06:11:07 UTC);
        // let bucket = "mbver";
        let region = "";
        let service = "s3";
        let path = "/mbver/";

        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://192.168.1.11:9020/mbver/?list-type=2&encoding-type=url&prefix=mypre99&delimiter=%2F&fetch-owner=true&max-keys=1")
            .body(Body::empty()).unwrap();

        let mut headers = req.headers_mut();
        headers.insert("host", "127.0.0.1:9000".parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
        );
        headers.insert("x-amz-date", timestamp.parse().unwrap());

        println!("{:?}", req.uri().query());
        let canonical_request = get_canonical_request(&req, &v4_ignored_headers, &get_hashed_payload(&req));
        println!("canonical_request: \n{canonical_request}\n");
        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/mbver/\n",
                "delimiter=%2F&encoding-type=url&fetch-owner=true&list-type=2&max-keys=1&prefix=mypre99\n",
                "host:127.0.0.1:9000\n",
                "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "x-amz-date:",
                "20250628T061107Z",
                "\n",
                "\n",
                "host;x-amz-content-sha256;x-amz-date\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
        );

        let string_to_sign = get_string_to_sign_v4(t, region, &canonical_request, service);
        println!("string_to_sign: \n{string_to_sign}\n");
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20250628T061107Z",
                "\n",
                "20250628//s3/aws4_request\n",
                "9dcfa3d3139baf71a046e7fa17dacab8ee11676771e25e7cd09098bf39f09d5b", //payload hash
            )
        );

        let signing_key = get_signing_key(secret_access_key, region, t, service);
        let signature = get_signature(signing_key, &string_to_sign);
        println!("signature: \n{signature}\n");
        assert_eq!(signature, "c7c7c6e12e5709c0c2ffc4707600a86c3cd261dd1de7409126a17f5b08c58dfa");
    }

    #[test]
    fn example_presigned_url() {
        let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let timestamp = "20130524T000000Z";
        let t = datetime!(2013-05-24 0:00 UTC);
        // let bucket = "mblock2";
        let region = "us-east-1";
        let service = "s3";
        let path = "/";
        let session_token = "";

        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://examplebucket.s3.amazonaws.com/test.txt")
            .body(Body::empty())
            .unwrap();

        let mut headers = req.headers_mut();
        headers.insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());

        req = pre_sign_v4(req, access_key_id, secret_access_key, "", region, 86400, t);

        let mut canonical_request = req.method().as_str().to_string();
        canonical_request.push('\n');
        canonical_request.push_str(req.uri().path());
        canonical_request.push('\n');
        canonical_request.push_str(req.uri().query().unwrap());
        canonical_request.push('\n');
        canonical_request.push_str(&get_canonical_headers(&req, &v4_ignored_headers));
        canonical_request.push('\n');
        canonical_request.push_str(&get_signed_headers(&req, &v4_ignored_headers));
        canonical_request.push('\n');
        canonical_request.push_str(&get_hashed_payload(&req));
        //println!("canonical_request: \n{}\n", canonical_request);
        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/test.txt\n",
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20130524T000000Z&X-Amz-Expires=0000086400&X-Amz-SignedHeaders=host&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=98f1c9f47b39a4c40662680a9b029b046b7da5542c2e35d67edb8ff18d2ccf5c\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "\n",
                "host\n",
                "UNSIGNED-PAYLOAD",
            )
        );
    }

    #[test]
    fn example_presigned_url2() {
        let access_key_id = "rustfsadmin";
        let secret_access_key = "rustfsadmin";
        let timestamp = "20130524T000000Z";
        let t = datetime!(2013-05-24 0:00 UTC);
        // let bucket = "mblock2";
        let region = "us-east-1";
        let service = "s3";
        let path = "/mblock2/";
        let session_token = "";

        let mut req = request::Request::builder()
            .method(http::Method::GET)
            .uri("http://192.168.1.11:9020/mblock2/test.txt?delimiter=%2F&fetch-owner=true&prefix=mypre&encoding-type=url&max-keys=1&list-type=2")
            .body(Body::empty()).unwrap();

        let mut headers = req.headers_mut();
        headers.insert("host", "192.168.1.11:9020".parse().unwrap());

        req = pre_sign_v4(req, access_key_id, secret_access_key, "", region, 86400, t);

        let mut canonical_request = req.method().as_str().to_string();
        canonical_request.push('\n');
        canonical_request.push_str(req.uri().path());
        canonical_request.push('\n');
        canonical_request.push_str(req.uri().query().unwrap());
        canonical_request.push('\n');
        canonical_request.push_str(&get_canonical_headers(&req, &v4_ignored_headers));
        canonical_request.push('\n');
        canonical_request.push_str(&get_signed_headers(&req, &v4_ignored_headers));
        canonical_request.push('\n');
        canonical_request.push_str(&get_hashed_payload(&req));
        //println!("canonical_request: \n{}\n", canonical_request);
        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/mblock2/test.txt\n",
                "delimiter=%2F&fetch-owner=true&prefix=mypre&encoding-type=url&max-keys=1&list-type=2&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20130524T000000Z&X-Amz-Expires=0000086400&X-Amz-SignedHeaders=host&X-Amz-Credential=rustfsadmin%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=fe7f63f41e4ca18be9e70f560bbe9c079cf06ab97630934e04f7524751ff302d\n",
                "host:192.168.1.11:9020\n",
                "\n",
                "host\n",
                "UNSIGNED-PAYLOAD",
            )
        );
    }
}
