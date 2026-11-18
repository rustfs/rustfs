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
//! `ObjectAttributesEtagFixLayer` against the gateway stack alone: the `<ETag>` of a
//! `GetObjectAttributes` answer is written without quotes, and every other answer that carries a
//! tag keeps them.
//!
//! The layer strips the quotes of the first `<ETag>` of a successful XML `GET` that names
//! `attributes`. The gateway writes the element bare itself: the quirk
//! `q-mpu-attributes-etag-0036` (`EtagRender::XmlBare`).

use super::*;
use crate::server::gateway_handlers::test_support::signed_request_with_headers;
use http::HeaderValue;
use rustfs_gateway_types::ETag;

fn attributes(headers: &[(&'static str, &'static str)]) -> Request<http_body_util::Empty<Bytes>> {
    let headers = headers
        .iter()
        .map(|(name, value)| (http::HeaderName::from_static(name), HeaderValue::from_static(value)))
        .collect();
    signed_request_with_headers(
        Signed::Header,
        Method::GET,
        "/photos/key",
        "attributes",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        headers,
    )
}

fn signed(method: Method) -> Request<http_body_util::Empty<Bytes>> {
    signed_request_with_headers(
        Signed::Header,
        method,
        "/photos/key",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        HeaderMap::new(),
    )
}

#[tokio::test]
async fn the_attributes_etag_is_written_without_quotes() {
    let probe = Probe::with_etag(ETag::from_stored("d41d8cd98f00b204e9800998ecf8427e"));
    let stack = probe_stack(&probe);
    let answer = observe(&stack, attributes(&[("x-amz-object-attributes", "ETag,ObjectSize")])).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert!(answer.body.contains("<ETag>d41d8cd98f00b204e9800998ecf8427e</ETag>"), "{}", answer.body);
    assert!(!answer.body.contains("&quot;") && !answer.body.contains("\"d41d"), "{}", answer.body);
    assert_eq!(
        answer.headers.get(http::header::CONTENT_TYPE).map(|value| value.as_bytes()),
        Some(&b"application/xml"[..])
    );
}

#[tokio::test]
async fn a_composite_multipart_tag_keeps_its_part_count_and_loses_only_the_quotes() {
    let probe = Probe::with_etag(ETag::from_stored("9b2cf535f27731c974343645a3985328-3"));
    let stack = probe_stack(&probe);
    let answer = observe(&stack, attributes(&[("x-amz-object-attributes", "ETag")])).await;
    assert!(answer.body.contains("<ETag>9b2cf535f27731c974343645a3985328-3</ETag>"), "{}", answer.body);
}

#[tokio::test]
async fn every_other_answer_that_carries_a_tag_keeps_its_quotes() {
    let probe = Probe::with_etag(ETag::from_stored("abc"));
    let stack = probe_stack(&probe);
    for method in [Method::GET, Method::PUT] {
        let answer = observe(&stack, signed(method.clone())).await;
        assert_eq!(answer.status, StatusCode::OK, "{method}: {}", answer.body);
        assert_eq!(
            answer.headers.get(http::header::ETAG),
            Some(&HeaderValue::from_static("\"abc\"")),
            "{method}"
        );
    }
    // The `x-amz-object-attributes` header alone, on a plain GET, selects nothing: it is the
    // operation that decides, not the header.
    let request = signed_request_with_headers(
        Signed::Header,
        Method::GET,
        "/photos/key",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        [(http::HeaderName::from_static("x-amz-object-attributes"), HeaderValue::from_static("ETag"))]
            .into_iter()
            .collect(),
    );
    let answer = observe(&stack, request).await;
    assert_eq!(answer.headers.get(http::header::ETAG), Some(&HeaderValue::from_static("\"abc\"")));
}

#[tokio::test]
async fn an_attributes_request_without_a_tag_writes_none() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let answer = observe(&stack, attributes(&[("x-amz-object-attributes", "ObjectSize")])).await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert!(!answer.body.contains("<ETag>"), "{}", answer.body);
}
