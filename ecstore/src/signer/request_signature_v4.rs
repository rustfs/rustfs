#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use bytes::{Bytes, BytesMut};
use http::HeaderMap;
use http::Uri;
use http::header::TRAILER;
use http::request::{self, Request};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fmt::Write;
use time::{OffsetDateTime, format_description, macros::datetime, macros::format_description};
use tracing::{debug, error, info, warn};

use super::ordered_qs::OrderedQs;
use super::request_signature_streaming_unsigned_trailer::streaming_unsigned_v4;
use super::utils::stable_sort_by_first;
use super::utils::{get_host_addr, sign_v4_trim_all};
use crate::client::constants::UNSIGNED_PAYLOAD;
use rustfs_utils::crypto::{hex, hex_sha256, hmac_sha256};
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;

pub const SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const SERVICE_TYPE_S3: &str = "s3";
pub const SERVICE_TYPE_STS: &str = "sts";

lazy_static! {
    static ref v4_ignored_headers: HashMap<String, bool> = {
        let mut m = <HashMap<String, bool>>::new();
        m.insert("accept-encoding".to_string(), true);
        m.insert("authorization".to_string(), true);
        m.insert("user-agent".to_string(), true);
        m
    };
}

pub fn get_signing_key(secret: &str, loc: &str, t: OffsetDateTime, service_type: &str) -> [u8; 32] {
    let mut s = "AWS4".to_string();
    s.push_str(secret);
    let format = format_description!("[year][month][day]");
    let date = hmac_sha256(s.into_bytes(), t.format(&format).unwrap().into_bytes());
    let location = hmac_sha256(date, loc);
    let service = hmac_sha256(location, service_type);
    let signing_key = hmac_sha256(service, "aws4_request");
    signing_key
}

pub fn get_signature(signing_key: [u8; 32], string_to_sign: &str) -> String {
    hex(hmac_sha256(signing_key, string_to_sign))
}

pub fn get_scope(location: &str, t: OffsetDateTime, service_type: &str) -> String {
    let format = format_description!("[year][month][day]");
    let mut ans = String::from("");
    ans.push_str(&t.format(&format).unwrap().to_string());
    ans.push('/');
    ans.push_str(location); // TODO: use a `Region` type
    ans.push('/');
    ans.push_str(service_type);
    ans.push_str("/aws4_request");
    ans
}

fn get_credential(access_key_id: &str, location: &str, t: OffsetDateTime, service_type: &str) -> String {
    let scope = get_scope(location, t, service_type);
    let mut s = access_key_id.to_string();
    s.push_str("/");
    s.push_str(&scope);
    s
}

fn get_hashed_payload(req: &request::Builder) -> String {
    let headers = req.headers_ref().unwrap();
    let mut hashed_payload = "";
    if let Some(payload) = headers.get("X-Amz-Content-Sha256") {
        hashed_payload = payload.to_str().unwrap();
    }
    if hashed_payload == "" {
        hashed_payload = UNSIGNED_PAYLOAD;
    }
    hashed_payload.to_string()
}

fn get_canonical_headers(req: &request::Builder, ignored_headers: &HashMap<String, bool>) -> String {
    let mut headers = Vec::<String>::new();
    let mut vals = HashMap::<String, Vec<String>>::new();
    for k in req.headers_ref().expect("err").keys() {
        if ignored_headers.get(&k.to_string()).is_some() {
            continue;
        }
        headers.push(k.as_str().to_lowercase());
        let vv = req
            .headers_ref()
            .expect("err")
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
                let _ = buf.write_str(&get_host_addr(&req));
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

fn get_signed_headers(req: &request::Builder, ignored_headers: &HashMap<String, bool>) -> String {
    let mut headers = Vec::<String>::new();
    let headers_ref = req.headers_ref().expect("err");
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

fn get_canonical_request(req: &request::Builder, ignored_headers: &HashMap<String, bool>, hashed_payload: &str) -> String {
    let mut canonical_query_string = "".to_string();
    if let Some(q) = req.uri_ref().unwrap().query() {
        let mut query = q.split('&').map(|h| h.to_string()).collect::<Vec<String>>();
        query.sort();
        canonical_query_string = query.join("&");
        canonical_query_string = canonical_query_string.replace("+", "%20");
    }

    let mut canonical_request = <Vec<String>>::new();
    canonical_request.push(req.method_ref().unwrap().to_string());
    canonical_request.push(req.uri_ref().unwrap().path().to_string());
    canonical_request.push(canonical_query_string);
    canonical_request.push(get_canonical_headers(&req, ignored_headers));
    canonical_request.push(get_signed_headers(&req, ignored_headers));
    canonical_request.push(hashed_payload.to_string());
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
    req: request::Builder,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    location: &str,
    expires: i64,
    t: OffsetDateTime,
) -> request::Builder {
    if access_key_id == "" || secret_access_key == "" {
        return req;
    }

    //let t = OffsetDateTime::now_utc();
    //let date = AmzDate::parse(timestamp).unwrap();
    let t2 = t.replace_time(time::Time::from_hms(0, 0, 0).unwrap());

    //let credential = get_scope(location, t, SERVICE_TYPE_S3);
    let credential = get_credential(access_key_id, location, t, SERVICE_TYPE_S3);
    let signed_headers = get_signed_headers(&req, &v4_ignored_headers);

    let mut query = <Vec<(String, String)>>::new();
    if let Some(q) = req.uri_ref().unwrap().query() {
        let result = serde_urlencoded::from_str::<Vec<(String, String)>>(q);
        query = result.unwrap_or_default();
    }
    query.push(("X-Amz-Algorithm".to_string(), SIGN_V4_ALGORITHM.to_string()));
    let format = format_description!("[year][month][day]T[hour][minute][second]Z");
    query.push(("X-Amz-Date".to_string(), t.format(&format).unwrap().to_string()));
    query.push(("X-Amz-Expires".to_string(), format!("{:010}", expires)));
    query.push(("X-Amz-SignedHeaders".to_string(), signed_headers));
    query.push(("X-Amz-Credential".to_string(), credential));
    if session_token != "" {
        query.push(("X-Amz-Security-Token".to_string(), session_token.to_string()));
    }

    let uri = req.uri_ref().unwrap().clone();
    let mut parts = req.uri_ref().unwrap().clone().into_parts();
    parts.path_and_query = Some(
        format!("{}?{}", uri.path(), serde_urlencoded::to_string(&query).unwrap())
            .parse()
            .unwrap(),
    );
    let req = req.uri(Uri::from_parts(parts).unwrap());

    let canonical_request = get_canonical_request(&req, &v4_ignored_headers, &get_hashed_payload(&req));
    let string_to_sign = get_string_to_sign_v4(t, location, &canonical_request, SERVICE_TYPE_S3);
    //println!("canonical_request: \n{}\n", canonical_request);
    //println!("string_to_sign: \n{}\n", string_to_sign);
    let signing_key = get_signing_key(secret_access_key, location, t, SERVICE_TYPE_S3);
    let signature = get_signature(signing_key, &string_to_sign);

    let uri = req.uri_ref().unwrap().clone();
    let mut parts = req.uri_ref().unwrap().clone().into_parts();
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
    let req = req.uri(Uri::from_parts(parts).unwrap());

    req
}

fn post_pre_sign_signature_v4(policy_base64: &str, t: OffsetDateTime, secret_access_key: &str, location: &str) -> String {
    let signing_key = get_signing_key(secret_access_key, location, t, SERVICE_TYPE_S3);
    let signature = get_signature(signing_key, policy_base64);
    signature
}

fn sign_v4_sts(mut req: request::Builder, access_key_id: &str, secret_access_key: &str, location: &str) -> request::Builder {
    sign_v4_inner(req, 0, access_key_id, secret_access_key, "", location, SERVICE_TYPE_STS, HeaderMap::new())
}

fn sign_v4_inner(
    mut req: request::Builder,
    content_len: i64,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    location: &str,
    service_type: &str,
    trailer: HeaderMap,
) -> request::Builder {
    if access_key_id == "" || secret_access_key == "" {
        return req;
    }

    let t = OffsetDateTime::now_utc();
    let t2 = t.replace_time(time::Time::from_hms(0, 0, 0).unwrap());

    let mut headers = req.headers_mut().expect("err");
    let format = format_description!("[year][month][day]T[hour][minute][second]Z");
    headers.insert("X-Amz-Date", t.format(&format).unwrap().to_string().parse().unwrap());

    if session_token != "" {
        headers.insert("X-Amz-Security-Token", session_token.parse().unwrap());
    }

    if trailer.len() > 0 {
        for (k, _) in &trailer {
            headers.append("X-Amz-Trailer", k.as_str().to_lowercase().parse().unwrap());
        }

        headers.insert("Content-Encoding", "aws-chunked".parse().unwrap());
        headers.insert("x-amz-decoded-content-length", format!("{:010}", content_len).parse().unwrap());
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

    let mut headers = req.headers_mut().expect("err");

    let auth = format!(
        "{} Credential={}, SignedHeaders={}, Signature={}",
        SIGN_V4_ALGORITHM, credential, signed_headers, signature
    );
    headers.insert("Authorization", auth.parse().unwrap());

    if trailer.len() > 0 {
        //req.Trailer = trailer;
        for (_, v) in &trailer {
            headers.append(http::header::TRAILER, v.clone());
        }
        return streaming_unsigned_v4(req, &session_token, content_len, t);
    }
    req
}

fn unsigned_trailer(mut req: request::Builder, content_len: i64, trailer: HeaderMap) {
    if trailer.len() > 0 {
        return;
    }
    let t = OffsetDateTime::now_utc();
    let t = t.replace_time(time::Time::from_hms(0, 0, 0).unwrap());

    let mut headers = req.headers_mut().expect("err");
    let format = format_description!("[year][month][day]T[hour][minute][second]Z");
    headers.insert("X-Amz-Date", t.format(&format).unwrap().to_string().parse().unwrap());

    for (k, _) in &trailer {
        headers.append("X-Amz-Trailer", k.as_str().to_lowercase().parse().unwrap());
    }

    headers.insert("Content-Encoding", "aws-chunked".parse().unwrap());
    headers.insert("x-amz-decoded-content-length", format!("{:010}", content_len).parse().unwrap());

    if trailer.len() > 0 {
        for (_, v) in &trailer {
            headers.append(http::header::TRAILER, v.clone());
        }
    }
    streaming_unsigned_v4(req, "", content_len, t);
}

pub fn sign_v4(
    mut req: request::Builder,
    content_len: i64,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    location: &str,
) -> request::Builder {
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
    req: request::Builder,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    location: &str,
    trailer: HeaderMap,
) -> request::Builder {
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
mod tests {
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

        let mut req = Request::builder()
            .method(http::Method::GET)
            .uri("http://examplebucket.s3.amazonaws.com/?");
        let mut headers = req.headers_mut().expect("err");
        headers.insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
        );
        headers.insert("x-amz-date", timestamp.parse().unwrap());

        let mut query = <Vec<(String, String)>>::new();
        query.push(("max-keys".to_string(), "2".to_string()));
        query.push(("prefix".to_string(), "J".to_string()));
        let uri = req.uri_ref().unwrap().clone();
        let mut parts = req.uri_ref().unwrap().clone().into_parts();
        parts.path_and_query = Some(
            format!("{}?{}", uri.path(), serde_urlencoded::to_string(&query).unwrap())
                .parse()
                .unwrap(),
        );
        let req = req.uri(Uri::from_parts(parts).unwrap());

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

        let mut req = Request::builder()
            .method(http::Method::GET)
            .uri("http://192.168.1.11:9020/mblock2/?");

        let mut headers = req.headers_mut().expect("err");
        headers.insert("host", "192.168.1.11:9020".parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
        );
        headers.insert("x-amz-date", timestamp.parse().unwrap());

        let mut query: Vec<(String, String)> = Vec::new();
        let uri = req.uri_ref().unwrap().clone();
        let mut parts = req.uri_ref().unwrap().clone().into_parts();
        parts.path_and_query = Some(
            format!("{}?{}", uri.path(), serde_urlencoded::to_string(&query).unwrap())
                .parse()
                .unwrap(),
        );
        let req = req.uri(Uri::from_parts(parts).unwrap());

        let canonical_request = get_canonical_request(&req, &v4_ignored_headers, &get_hashed_payload(&req));
        println!("canonical_request: \n{}\n", canonical_request);
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
        println!("string_to_sign: \n{}\n", string_to_sign);
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
        println!("signature: \n{}\n", signature);
        assert_eq!(signature, "df4116595e27b0dfd1103358947d9199378cc6386c4657abd8c5f0b11ebb4931");
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

        let mut req = Request::builder().method(http::Method::GET).uri("http://192.168.1.11:9020/mblock2/?list-type=2&encoding-type=url&prefix=mypre&delimiter=%2F&fetch-owner=true&max-keys=1");

        let mut headers = req.headers_mut().expect("err");
        headers.insert("host", "192.168.1.11:9020".parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
        );
        headers.insert("x-amz-date", timestamp.parse().unwrap());

        /*let uri = req.uri_ref().unwrap().clone();
        println!("{:?}", uri);
        let mut canonical_query_string = "".to_string();
        if let Some(q) = uri.query() {
            let result = serde_urlencoded::from_str::<Vec<String>>(q);
            let mut query = result.unwrap_or_default();
            query.sort();
            canonical_query_string = query.join("&");
            canonical_query_string.replace("+", "%20");
        }
        let mut parts = req.uri_ref().unwrap().clone().into_parts();
        parts.path_and_query = Some(format!("{}?{}", uri.path(), canonical_query_string).parse().unwrap());
        let req = req.uri(Uri::from_parts(parts).unwrap());*/
        println!("{:?}", req.uri_ref().unwrap().query());
        let canonical_request = get_canonical_request(&req, &v4_ignored_headers, &get_hashed_payload(&req));
        println!("canonical_request: \n{}\n", canonical_request);
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
        println!("string_to_sign: \n{}\n", string_to_sign);
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
        println!("signature: \n{}\n", signature);
        assert_eq!(signature, "760278c9a77d5c245ac83d85917bddc3e3b14343091e8f4ad8edbbf73107d685");
    }

    #[test]
    fn example_presigned_url() {
        use hyper::Uri;

        let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let timestamp = "20130524T000000Z";
        let t = datetime!(2013-05-24 0:00 UTC);
        // let bucket = "mblock2";
        let region = "us-east-1";
        let service = "s3";
        let path = "/";
        let session_token = "";

        let mut req = Request::builder()
            .method(http::Method::GET)
            .uri("http://examplebucket.s3.amazonaws.com/test.txt");

        let mut headers = req.headers_mut().expect("err");
        headers.insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());

        req = pre_sign_v4(req, access_key_id, secret_access_key, "", region, 86400, t);

        let mut canonical_request = req.method_ref().unwrap().as_str().to_string();
        canonical_request.push('\n');
        canonical_request.push_str(req.uri_ref().unwrap().path());
        canonical_request.push('\n');
        canonical_request.push_str(req.uri_ref().unwrap().query().unwrap());
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
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20130524T000000Z&X-Amz-Expires=0000086400&X-Amz-SignedHeaders=host&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "\n",
                "host\n",
                "UNSIGNED-PAYLOAD",
            )
        );
    }

    #[test]
    fn example_presigned_url2() {
        use hyper::Uri;

        let access_key_id = "rustfsadmin";
        let secret_access_key = "rustfsadmin";
        let timestamp = "20130524T000000Z";
        let t = datetime!(2013-05-24 0:00 UTC);
        // let bucket = "mblock2";
        let region = "us-east-1";
        let service = "s3";
        let path = "/mblock2/";
        let session_token = "";

        let mut req = Request::builder().method(http::Method::GET).uri("http://192.168.1.11:9020/mblock2/test.txt?delimiter=%2F&fetch-owner=true&prefix=mypre&encoding-type=url&max-keys=1&list-type=2");

        let mut headers = req.headers_mut().expect("err");
        headers.insert("host", "192.168.1.11:9020".parse().unwrap());

        req = pre_sign_v4(req, access_key_id, secret_access_key, "", region, 86400, t);

        let mut canonical_request = req.method_ref().unwrap().as_str().to_string();
        canonical_request.push('\n');
        canonical_request.push_str(req.uri_ref().unwrap().path());
        canonical_request.push('\n');
        canonical_request.push_str(req.uri_ref().unwrap().query().unwrap());
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
                "delimiter=%2F&fetch-owner=true&prefix=mypre&encoding-type=url&max-keys=1&list-type=2&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20130524T000000Z&X-Amz-Expires=0000086400&X-Amz-SignedHeaders=host&X-Amz-Credential=rustfsadmin%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=e4af975e4a7e2c0449451740c7e9a425123681d2a8830bfb188789ea19618b20\n",
                "host:192.168.1.11:9020\n",
                "\n",
                "host\n",
                "UNSIGNED-PAYLOAD",
            )
        );
    }
}
