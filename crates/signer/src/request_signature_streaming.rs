use http::{HeaderMap, HeaderValue, request};
use lazy_static::lazy_static;
use std::collections::HashMap;
use time::{OffsetDateTime, macros::format_description};

use super::request_signature_v4::{SERVICE_TYPE_S3, get_scope, get_signature, get_signing_key};
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;

const STREAMING_SIGN_ALGORITHM: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
const STREAMING_SIGN_TRAILER_ALGORITHM: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
const STREAMING_PAYLOAD_HDR: &str = "AWS4-HMAC-SHA256-PAYLOAD";
const _STREAMING_TRAILER_HDR: &str = "AWS4-HMAC-SHA256-TRAILER";
const _PAYLOAD_CHUNK_SIZE: i64 = 64 * 1024;
const _CHUNK_SIGCONST_LEN: i64 = 17;
const _SIGNATURESTR_LEN: i64 = 64;
const _CRLF_LEN: i64 = 2;
const _TRAILER_KV_SEPARATOR: &str = ":";
const _TRAILER_SIGNATURE: &str = "x-amz-trailer-signature";

lazy_static! {
    static ref ignored_streaming_headers: HashMap<String, bool> = {
        let mut m = <HashMap<String, bool>>::new();
        m.insert("authorization".to_string(), true);
        m.insert("user-agent".to_string(), true);
        m.insert("content-type".to_string(), true);
        m
    };
}

#[allow(dead_code)]
fn build_chunk_string_to_sign(t: OffsetDateTime, region: &str, previous_sig: &str, chunk_check_sum: &str) -> String {
    let mut string_to_sign_parts = <Vec<String>>::new();
    string_to_sign_parts.push(STREAMING_PAYLOAD_HDR.to_string());
    let format = format_description!("[year][month][day]T[hour][minute][second]Z");
    string_to_sign_parts.push(t.format(&format).unwrap());
    string_to_sign_parts.push(get_scope(region, t, SERVICE_TYPE_S3));
    string_to_sign_parts.push(previous_sig.to_string());
    string_to_sign_parts.push(EMPTY_STRING_SHA256_HASH.to_string());
    string_to_sign_parts.push(chunk_check_sum.to_string());
    string_to_sign_parts.join("\n")
}

fn _build_chunk_signature(
    chunk_check_sum: &str,
    req_time: OffsetDateTime,
    region: &str,
    previous_signature: &str,
    secret_access_key: &str,
) -> String {
    let chunk_string_to_sign = build_chunk_string_to_sign(req_time, region, previous_signature, chunk_check_sum);
    let signing_key = get_signing_key(secret_access_key, region, req_time, SERVICE_TYPE_S3);
    get_signature(signing_key, &chunk_string_to_sign)
}

pub fn streaming_sign_v4(
    mut req: request::Builder,
    _access_key_id: &str,
    _secret_access_key: &str,
    session_token: &str,
    _region: &str,
    data_len: i64,
    req_time: OffsetDateTime, /*, sh256: md5simd::Hasher*/
    trailer: HeaderMap,
) -> request::Builder {
    let headers = req.headers_mut().expect("err");

    if trailer.is_empty() {
        headers.append("X-Amz-Content-Sha256", HeaderValue::from_str(STREAMING_SIGN_ALGORITHM).expect("err"));
    } else {
        headers.append(
            "X-Amz-Content-Sha256",
            HeaderValue::from_str(STREAMING_SIGN_TRAILER_ALGORITHM).expect("err"),
        );
        for (k, _) in &trailer {
            headers.append("X-Amz-Trailer", k.as_str().to_lowercase().parse().unwrap());
        }
        let chunked_value = HeaderValue::from_str(&vec!["aws-chunked"].join(",")).expect("err");
        headers.insert(http::header::TRANSFER_ENCODING, chunked_value);
    }

    if !session_token.is_empty() {
        headers.insert("X-Amz-Security-Token", HeaderValue::from_str(&session_token).expect("err"));
    }

    let format = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z");
    headers.insert("X-Amz-Date", HeaderValue::from_str(&req_time.format(&format).unwrap()).expect("err"));

    //req.content_length = 100；
    headers.insert("x-amz-decoded-content-length", format!("{:010}", data_len).parse().unwrap());

    req
}
