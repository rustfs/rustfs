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

use crate::server::{MINIO_ADMIN_PREFIX, has_path_prefix};
use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use rustfs_crypto::{decrypt_data, decrypt_stream_io, encrypt_stream_io};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;

/// Returns `true` if `s` contains any whitespace character.
///
/// Used to reject identifiers (access keys, user/group/policy names) that
/// contain spaces. Detects whitespace anywhere in the string — leading,
/// trailing, or internal — so values like `"my key"` are correctly rejected.
pub(crate) fn has_space_be(s: &str) -> bool {
    s.chars().any(char::is_whitespace)
}

pub(crate) fn is_compat_admin_request(path: &str) -> bool {
    has_path_prefix(path, MINIO_ADMIN_PREFIX)
}

pub(crate) async fn read_compatible_admin_body(
    mut input: Body,
    max_len: usize,
    path: &str,
    secret_key: &str,
) -> S3Result<Vec<u8>> {
    let body = input
        .store_all_limited(max_len)
        .await
        .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

    if is_compat_admin_request(path) {
        decrypt_stream_io(secret_key.as_bytes(), body.as_ref())
            .or_else(|_| decrypt_data(secret_key.as_bytes(), body.as_ref()))
            .map_err(|e| s3_error!(InvalidRequest, "failed to decrypt MinIO admin payload: {}", e))
    } else {
        Ok(body.to_vec())
    }
}

pub(crate) fn encode_compatible_admin_payload(path: &str, secret_key: &str, data: Vec<u8>) -> S3Result<(Vec<u8>, &'static str)> {
    if is_compat_admin_request(path) {
        let encrypted = encrypt_stream_io(secret_key.as_bytes(), &data)
            .map_err(|e| s3_error!(InternalError, "failed to encrypt MinIO admin payload: {}", e))?;
        Ok((encrypted, "application/octet-stream"))
    } else {
        Ok((data, "application/json"))
    }
}

/// Serialize `value` as the JSON body of an admin response with `status`.
///
/// The admin surface answers almost every endpoint this way, so the shape is
/// pinned here rather than re-derived per handler: `Content-Type:
/// application/json`, no other header, and the serialized bytes verbatim as
/// the body. Serialization failure is reported as `InternalError`; the
/// response structs the admin handlers pass here are plain owned data, so that
/// arm is unreachable in practice.
pub(crate) fn json_response<T: Serialize>(status: StatusCode, value: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(value)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize response: {e}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(S3Response::with_headers((status, Body::from(data)), headers))
}

/// A bodiless admin response carrying only `status`.
///
/// Used by the endpoints whose success answer is the status code itself
/// (`204 No Content`, or a `200 OK` acknowledgement with nothing to report).
/// No `Content-Type` is set, because there is no content to type.
pub(crate) fn empty_response(status: StatusCode) -> S3Response<(StatusCode, Body)> {
    S3Response::new((status, Body::empty()))
}

/// Collect a request URI's query string into a parameter map.
///
/// Parsed with `form_urlencoded`, as the rest of the admin surface does, so a
/// parameter written without a value (`?status`) arrives as an empty value
/// rather than disappearing: a validated parameter must be able to tell "not
/// asked for" from "asked for, unreadable". Percent escapes and `+` are
/// decoded, and a repeated key keeps its last occurrence.
pub(crate) fn extract_query_params(uri: &Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            params.insert(key.into_owned(), value.into_owned());
        }
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_crypto::encrypt_data;
    use s3s::Body;

    #[test]
    fn has_space_be_detects_any_whitespace() {
        // Internal space — the original bug: access keys like "my key" slipped
        // past validation because only leading/trailing space was detected.
        assert!(has_space_be("my key"));
        assert!(has_space_be("ab\tcd"));
        // Leading / trailing whitespace (already caught before the fix).
        assert!(has_space_be(" abcd"));
        assert!(has_space_be("abcd "));
        // Clean identifiers must pass.
        assert!(!has_space_be("abcd"));
        assert!(!has_space_be("my-key_01"));
        assert!(!has_space_be(""));
    }

    #[test]
    fn detects_compat_admin_paths_only_for_external_prefix() {
        assert!(is_compat_admin_request("/minio/admin/v3/list-users"));
        assert!(!is_compat_admin_request("/minio/adminx/list-users"));
        assert!(!is_compat_admin_request("/rustfs/admin/v3/list-users"));
    }

    #[test]
    fn encodes_plain_payload_for_rustfs_admin_paths() {
        let payload = b"{\"ok\":true}".to_vec();
        let (encoded, content_type) =
            encode_compatible_admin_payload("/rustfs/admin/v3/list-users", "secret", payload.clone()).expect("encode payload");

        assert_eq!(encoded, payload);
        assert_eq!(content_type, "application/json");
    }

    #[test]
    fn encodes_compat_payload_with_compatible_encryption() {
        let payload = b"{\"ok\":true}".to_vec();
        let (encoded, content_type) =
            encode_compatible_admin_payload("/minio/admin/v3/list-users", "secret", payload.clone()).expect("encode payload");

        assert_ne!(encoded, payload);
        assert_eq!(content_type, "application/octet-stream");
        assert_eq!(decrypt_stream_io(b"secret", &encoded).expect("decrypt payload"), payload);
    }

    #[tokio::test]
    async fn reads_legacy_compat_payload_as_fallback() {
        let payload = b"{\"ok\":true}".to_vec();
        let encrypted = encrypt_data(b"secret", &payload).expect("encrypt payload");

        let decoded = read_compatible_admin_body(Body::from(encrypted), 1024, "/minio/admin/v3/list-users", "secret")
            .await
            .expect("decode payload");

        assert_eq!(decoded, payload);
    }

    async fn body_bytes(mut body: Body) -> Vec<u8> {
        body.store_all_limited(64 * 1024).await.expect("body should read").to_vec()
    }

    /// The wire contract every admin endpoint that returns JSON depends on:
    /// the requested status, `application/json`, and the serialized bytes
    /// verbatim — nothing else.
    #[tokio::test]
    async fn json_response_carries_status_content_type_and_serialized_body() {
        #[derive(Serialize)]
        struct Payload {
            success: bool,
            message: &'static str,
        }

        let response = json_response(
            StatusCode::ACCEPTED,
            &Payload {
                success: true,
                message: "queued",
            },
        )
        .expect("payload should serialize");

        assert_eq!(response.output.0, StatusCode::ACCEPTED);
        assert_eq!(
            response.headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(response.headers.len(), 1);
        assert_eq!(body_bytes(response.output.1).await, br#"{"success":true,"message":"queued"}"#.to_vec());
    }

    /// A serialization failure must surface as `InternalError` rather than a
    /// panic or a half-written body.
    #[test]
    fn json_response_reports_serialization_failure_as_internal_error() {
        struct Unserializable;

        impl Serialize for Unserializable {
            fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("nope"))
            }
        }

        let err = json_response(StatusCode::OK, &Unserializable).expect_err("serialization must fail");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert!(err.message().unwrap_or_default().contains("failed to serialize response"));
    }

    /// The bodiless answer must stay bodiless and must not claim a content
    /// type: callers use it for `204 No Content` and bare acknowledgements.
    #[tokio::test]
    async fn empty_response_has_no_body_and_no_headers() {
        for status in [StatusCode::OK, StatusCode::NO_CONTENT] {
            let response = empty_response(status);
            assert_eq!(response.output.0, status);
            assert!(response.headers.is_empty());
            assert!(body_bytes(response.output.1).await.is_empty());
        }
    }

    /// Percent escapes must be decoded, so a job id or key id containing `/`
    /// arrives whole rather than as its escape sequence.
    #[test]
    fn extract_query_params_decodes_percent_escapes() {
        let uri: Uri = "/rustfs/admin/v3/status-job?jobId=abc%2F123"
            .parse()
            .expect("uri should parse");
        let params = extract_query_params(&uri);
        assert_eq!(params.get("jobId"), Some(&"abc/123".to_string()));
    }

    /// A parameter written without a value must arrive as an empty value, not
    /// vanish: a validated parameter has to tell "not asked for" from "asked
    /// for, unreadable". A missing query string yields no parameters at all.
    #[test]
    fn extract_query_params_keeps_valueless_parameters_and_survives_no_query() {
        let valueless: Uri = "/rustfs/admin/v3/kms/keys?status".parse().expect("uri should parse");
        let params = extract_query_params(&valueless);
        assert_eq!(params.get("status"), Some(&String::new()));

        let plus: Uri = "/rustfs/admin/v3/kms/keys?name=a+b".parse().expect("uri should parse");
        assert_eq!(extract_query_params(&plus).get("name"), Some(&"a b".to_string()));

        let bare: Uri = "/rustfs/admin/v3/kms/keys".parse().expect("uri should parse");
        assert!(extract_query_params(&bare).is_empty());
    }
}
