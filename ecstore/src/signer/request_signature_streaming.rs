use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use futures::task;
use http::HeaderMap;
use http::Uri;
use http::header::TRAILER;
use http::request::{self, Request};
use hyper::Method;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fmt::Write;
use std::pin::Pin;
use std::sync::Mutex;
use stdx::str::StrExt;
use time::{OffsetDateTime, format_description, macros::datetime, macros::format_description};
use tracing::{debug, error, info, warn};

use super::request_signature_v4::{SERVICE_TYPE_S3, get_scope, get_signature, get_signing_key};
use crate::client::constants::UNSIGNED_PAYLOAD;
use rustfs_utils::{
    crypto::{hex, hex_sha256, hex_sha256_chunk, hmac_sha256},
    hash::EMPTY_STRING_SHA256_HASH,
};

const STREAMING_SIGN_ALGORITHM: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
const STREAMING_SIGN_TRAILER_ALGORITHM: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
const STREAMING_PAYLOAD_HDR: &str = "AWS4-HMAC-SHA256-PAYLOAD";
const STREAMING_TRAILER_HDR: &str = "AWS4-HMAC-SHA256-TRAILER";
const PAYLOAD_CHUNK_SIZE: i64 = 64 * 1024;
const CHUNK_SIGCONST_LEN: i64 = 17;
const SIGNATURESTR_LEN: i64 = 64;
const CRLF_LEN: i64 = 2;
const TRAILER_KV_SEPARATOR: &str = ":";
const TRAILER_SIGNATURE: &str = "x-amz-trailer-signature";

lazy_static! {
    static ref ignored_streaming_headers: HashMap<String, bool> = {
        let mut m = <HashMap<String, bool>>::new();
        m.insert("authorization".to_string(), true);
        m.insert("user-agent".to_string(), true);
        m.insert("content-type".to_string(), true);
        m
    };
}

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

fn build_chunk_signature(
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
    req: request::Builder,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    region: &str,
    data_len: i64,
    req_time: OffsetDateTime, /*, sh256: md5simd.Hasher*/
) -> request::Builder {
    todo!();
}
