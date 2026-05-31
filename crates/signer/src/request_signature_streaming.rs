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

use http::{HeaderMap, HeaderValue, request};
use time::{OffsetDateTime, macros::format_description};
use tracing::warn;

use super::request_signature_v4::{SERVICE_TYPE_S3, SignV4Error, get_scope, get_signature, get_signing_key};
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
use s3s::Body;

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

// static ignored_streaming_headers: LazyLock<HashMap<String, bool>> = LazyLock::new(|| {
//     let mut m = <HashMap<String, bool>>::new();
//     m.insert("authorization".to_string(), true);
//     m.insert("user-agent".to_string(), true);
//     m.insert("content-type".to_string(), true);
//     m
// });

#[derive(Debug)]
struct StreamingSignFailure {
    request: request::Request<Body>,
    error: SignV4Error,
}

type StreamingSignOutcome = std::result::Result<request::Request<Body>, Box<StreamingSignFailure>>;

fn streaming_fail(request: request::Request<Body>, error: SignV4Error) -> StreamingSignOutcome {
    Err(Box::new(StreamingSignFailure { request, error }))
}

#[allow(dead_code)]
fn try_build_chunk_string_to_sign(
    t: OffsetDateTime,
    region: &str,
    previous_sig: &str,
    chunk_check_sum: &str,
) -> Result<String, SignV4Error> {
    let mut string_to_sign_parts = <Vec<String>>::new();
    string_to_sign_parts.push(STREAMING_PAYLOAD_HDR.to_string());
    let format = format_description!("[year][month][day]T[hour][minute][second]Z");
    string_to_sign_parts.push(
        t.format(&format)
            .map_err(|err| SignV4Error::TimeFormat { reason: err.to_string() })?,
    );
    string_to_sign_parts.push(get_scope(region, t, SERVICE_TYPE_S3));
    string_to_sign_parts.push(previous_sig.to_string());
    string_to_sign_parts.push(EMPTY_STRING_SHA256_HASH.to_string());
    string_to_sign_parts.push(chunk_check_sum.to_string());
    Ok(string_to_sign_parts.join("\n"))
}

fn _try_build_chunk_signature(
    chunk_check_sum: &str,
    req_time: OffsetDateTime,
    region: &str,
    previous_signature: &str,
    secret_access_key: &str,
) -> Result<String, SignV4Error> {
    let chunk_string_to_sign = try_build_chunk_string_to_sign(req_time, region, previous_signature, chunk_check_sum)?;
    let signing_key = get_signing_key(secret_access_key, region, req_time, SERVICE_TYPE_S3);
    Ok(get_signature(signing_key, &chunk_string_to_sign))
}

#[allow(clippy::too_many_arguments)]
fn streaming_sign_v4_inner(
    mut req: request::Request<Body>,
    _access_key_id: &str,
    _secret_access_key: &str,
    session_token: &str,
    _region: &str,
    data_len: i64,
    req_time: OffsetDateTime,
    trailer: HeaderMap,
) -> StreamingSignOutcome {
    let headers = req.headers_mut();

    if trailer.is_empty() {
        let value = match HeaderValue::from_str(STREAMING_SIGN_ALGORITHM) {
            Ok(v) => v,
            Err(err) => {
                return streaming_fail(
                    req,
                    SignV4Error::HeaderValueParse {
                        name: "X-Amz-Content-Sha256".to_string(),
                        reason: err.to_string(),
                    },
                );
            }
        };
        headers.append("X-Amz-Content-Sha256", value);
    } else {
        let trailer_algo = match HeaderValue::from_str(STREAMING_SIGN_TRAILER_ALGORITHM) {
            Ok(v) => v,
            Err(err) => {
                return streaming_fail(
                    req,
                    SignV4Error::HeaderValueParse {
                        name: "X-Amz-Content-Sha256".to_string(),
                        reason: err.to_string(),
                    },
                );
            }
        };
        headers.append("X-Amz-Content-Sha256", trailer_algo);
        for (k, _) in &trailer {
            let parsed = match k.as_str().to_lowercase().parse::<HeaderValue>() {
                Ok(v) => v,
                Err(err) => {
                    return streaming_fail(
                        req,
                        SignV4Error::HeaderValueParse {
                            name: "X-Amz-Trailer".to_string(),
                            reason: err.to_string(),
                        },
                    );
                }
            };
            headers.append("X-Amz-Trailer", parsed);
        }
        let chunked_value = match HeaderValue::from_str(&["aws-chunked"].join(",")) {
            Ok(v) => v,
            Err(err) => {
                return streaming_fail(
                    req,
                    SignV4Error::HeaderValueParse {
                        name: "Transfer-Encoding".to_string(),
                        reason: err.to_string(),
                    },
                );
            }
        };
        headers.insert(http::header::TRANSFER_ENCODING, chunked_value);
    }

    if !session_token.is_empty() {
        let token_value = match HeaderValue::from_str(session_token) {
            Ok(v) => v,
            Err(err) => {
                return streaming_fail(
                    req,
                    SignV4Error::HeaderValueParse {
                        name: "X-Amz-Security-Token".to_string(),
                        reason: err.to_string(),
                    },
                );
            }
        };
        headers.insert("X-Amz-Security-Token", token_value);
    }

    let format = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z");
    let date_str = match req_time.format(&format) {
        Ok(v) => v,
        Err(err) => return streaming_fail(req, SignV4Error::TimeFormat { reason: err.to_string() }),
    };
    let date_value = match HeaderValue::from_str(&date_str) {
        Ok(v) => v,
        Err(err) => {
            return streaming_fail(
                req,
                SignV4Error::HeaderValueParse {
                    name: "X-Amz-Date".to_string(),
                    reason: err.to_string(),
                },
            );
        }
    };
    headers.insert("X-Amz-Date", date_value);

    //req.content_length = 100；
    let decoded_len = match format!("{data_len:010}").parse::<HeaderValue>() {
        Ok(v) => v,
        Err(err) => {
            return streaming_fail(
                req,
                SignV4Error::HeaderValueParse {
                    name: "x-amz-decoded-content-length".to_string(),
                    reason: err.to_string(),
                },
            );
        }
    };
    headers.insert("x-amz-decoded-content-length", decoded_len);

    Ok(req)
}

#[allow(clippy::too_many_arguments)]
pub fn try_streaming_sign_v4(
    req: request::Request<Body>,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    region: &str,
    data_len: i64,
    req_time: OffsetDateTime,
    trailer: HeaderMap,
) -> Result<request::Request<Body>, SignV4Error> {
    streaming_sign_v4_inner(req, access_key_id, secret_access_key, session_token, region, data_len, req_time, trailer)
        .map_err(|f| f.error)
}

#[allow(clippy::too_many_arguments)]
pub fn streaming_sign_v4(
    req: request::Request<Body>,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    region: &str,
    data_len: i64,
    req_time: OffsetDateTime,
    trailer: HeaderMap,
) -> request::Request<Body> {
    match streaming_sign_v4_inner(req, access_key_id, secret_access_key, session_token, region, data_len, req_time, trailer) {
        Ok(request) => request,
        Err(failure) => {
            warn!(error = %failure.error, "failed to sign streaming v4 request");
            failure.request
        }
    }
}
