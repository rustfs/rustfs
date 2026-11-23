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

#![cfg(test)]

//! Differential wire tests against the pinned legacy HTTP renderer.

use super::{S3Error, S3ErrorCode};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING};
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body::Body as _;
use http_body_util::BodyExt;
use legacy_s3s::{S3Error as LegacyError, S3ErrorCode as LegacyCode};

fn errors(name: &str) -> (S3Error, LegacyError) {
    let Ok(owned) = name.parse();
    let Ok(legacy) = name.parse();
    (S3Error::new(owned), LegacyError::new(legacy))
}

async fn assert_response(owned: S3Error, legacy: LegacyError) {
    let name = legacy.code().as_str().to_owned();
    let expected = legacy.to_http_response().expect("legacy response should render");
    let actual = owned.to_http_response().expect("owned response should render");
    assert_eq!(actual.status(), expected.status(), "{name}");
    assert_eq!(actual.headers(), expected.headers(), "{name}");
    assert_eq!(actual.body().is_end_stream(), expected.body().is_end_stream(), "{name}");
    let length = expected
        .body()
        .size_hint()
        .exact()
        .expect("legacy error body has an exact size");
    assert_eq!(actual.body().size_hint().exact(), Some(length), "{name}");
    let expected = expected
        .into_body()
        .collect()
        .await
        .expect("read legacy error body")
        .to_bytes();
    let actual = actual.into_body().collect().await.expect("read owned error body").to_bytes();
    assert_eq!(actual, expected, "{name}");
    assert_eq!(u64::try_from(actual.len()).expect("body length fits in u64"), length, "{name}");
}

#[tokio::test]
async fn named_and_custom_code_defaults_match_legacy() {
    for code in S3ErrorCode::NAMED {
        let (owned, legacy) = errors(code.as_str());
        assert_response(owned, legacy).await;
    }
    for name in ["", "TierNotFound", "Unknown&<Code>"] {
        let (owned, legacy) = errors(name);
        assert_response(owned, legacy).await;
    }
}

#[tokio::test]
async fn explicit_xml_fields_and_headers_match_legacy() {
    let mut headers = HeaderMap::new();
    headers.append("x-amz-meta-repeated", HeaderValue::from_static("first"));
    headers.append("x-amz-meta-repeated", HeaderValue::from_static("second"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/custom"));
    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("999"));
    for (message, request_id, headers) in [
        ("", "", None),
        ("雪<&>\"'\t\n\r", "req<&>\"'雪", Some(HeaderMap::new())),
        ("message", "request", Some(headers)),
    ] {
        let (mut owned, mut legacy) = errors("Custom&Code");
        owned.set_message(message);
        legacy.set_message(message);
        owned.set_request_id(request_id);
        legacy.set_request_id(request_id);
        owned.set_status_code(StatusCode::CONFLICT);
        legacy.set_status_code(StatusCode::CONFLICT);
        if let Some(headers) = headers {
            owned.set_headers(headers.clone());
            legacy.set_headers(headers);
        }
        assert_response(owned, legacy).await;
    }
}

#[tokio::test]
async fn bodyless_statuses_skip_xml_and_strip_framing_like_legacy() {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/custom"));
    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("11"));
    headers.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
    headers.append("warning", HeaderValue::from_static("first"));
    headers.append("warning", HeaderValue::from_static("second"));
    for status in [100, 101, 103, 199, 204, 205, 304] {
        let status = StatusCode::from_u16(status).expect("valid status");
        let (mut owned, mut legacy) = errors("InvalidRequest");
        owned.set_status_code(status);
        legacy.set_status_code(status);
        owned.set_message("invalid XML \0");
        legacy.set_message("invalid XML \0");
        owned.set_headers(headers.clone());
        legacy.set_headers(headers.clone());
        assert_response(owned, legacy).await;
    }
}

#[tokio::test]
async fn invalid_xml_in_each_field_returns_the_legacy_error() {
    for (code, message, request_id) in [
        ("Bad\u{1f}Code", "message", "request"),
        ("InvalidRequest", "bad\u{b}message", "request"),
        ("InvalidRequest", "message", "bad\0request"),
    ] {
        let (mut owned, mut legacy) = errors(code);
        owned.set_message(message);
        legacy.set_message(message);
        owned.set_request_id(request_id);
        legacy.set_request_id(request_id);
        let expected = legacy.to_http_response().expect_err("legacy should reject invalid XML");
        let actual = owned.to_http_response().expect_err("owned should reject invalid XML");
        assert_eq!(actual.code().as_str(), expected.code().as_str());
        assert_eq!(actual.message(), expected.message());
        assert_response(actual, expected).await;
    }
}

#[tokio::test]
async fn legacy_cause_retains_missing_message_until_the_owned_code_changes() {
    // set_code on a legacy error retains its original, absent message.
    let legacy_without_message = || {
        let mut error =
            LegacyError::with_source(LegacyCode::Custom("SourceOnly".into()), Box::new(std::io::Error::other("cause")));
        error.set_code(LegacyCode::AccessDenied);
        error
    };
    assert_response(S3Error::from(legacy_without_message()), legacy_without_message()).await;

    let mut changed = S3Error::from(legacy_without_message());
    changed.set_code(S3ErrorCode::NoSuchKey);
    assert_response(changed, LegacyError::new(LegacyCode::NoSuchKey)).await;
}
