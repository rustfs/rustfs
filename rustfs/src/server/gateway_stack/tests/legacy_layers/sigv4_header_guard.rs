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
//! `SigV4HeaderGuardLayer` (GHSA-xm99-m3gq-83g8, GHSA-g8w9-qw9q-fghr) against the gateway stack
//! alone: an `x-amz-*` header the signature does not name, a swapped algorithm token and an
//! unreadable SigV4 value are `403 AccessDenied` before the handler runs.
//!
//! The legacy answer is the layer's own: `<Error><Code>` and `<Message>` only, `application/xml`,
//! and `Connection: close` on HTTP/1.x because the body is not drained. The gateway mechanism is
//! `refuse_unsigned_amz_headers_before_routing` (before routing, before the signature is judged)
//! with the verifier's signed-header check behind it.

use super::*;
use crate::server::gateway_handlers::test_support::signed_request_with_headers;
use http::HeaderValue;

const UNSIGNED: &str = "There were headers present in the request which were not signed";
const UNSUPPORTED: &str = "Unsupported SigV4 authorization algorithm";
const INVALID: &str = "Invalid SigV4 authorization header";

/// The document the layer writes, byte for byte.
fn layer_document(message: &str) -> String {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>AccessDenied</Code><Message>{message}</Message></Error>")
}

/// `body` without the two things the gateway's document adds to the layer's: a line feed after the
/// declaration and the request's `<RequestId>`, which every gateway error document names.
fn without_the_gateway_additions(body: &str) -> String {
    let body = body.replacen("?>\n<Error>", "?><Error>", 1);
    match (body.find("<RequestId>"), body.find("</RequestId>")) {
        (Some(start), Some(end)) => format!("{}{}", &body[..start], &body[end + "</RequestId>".len()..]),
        _ => body,
    }
}

fn assert_refused_as_the_layer_refuses(answer: &Observed, message: &str, label: &str) {
    assert_eq!(answer.status, StatusCode::FORBIDDEN, "{label}: {}", answer.body);
    assert_eq!(without_the_gateway_additions(&answer.body), layer_document(message), "{label}");
    let request_id = answer
        .headers
        .get("x-amz-request-id")
        .and_then(|id| id.to_str().ok())
        .expect("a request id header");
    assert!(
        answer.body.contains(&format!("<RequestId>{request_id}</RequestId>")),
        "{label}: {}",
        answer.body
    );
    assert_eq!(
        answer.headers.get(http::header::CONTENT_TYPE),
        Some(&HeaderValue::from_static("application/xml")),
        "{label}"
    );
    assert_eq!(
        answer.headers.get(http::header::CONNECTION),
        Some(&HeaderValue::from_static("close")),
        "{label}"
    );
}

fn header_signed_put(headers: HeaderMap) -> Request<http_body_util::Empty<Bytes>> {
    signed_request_with_headers(Signed::Header, Method::PUT, "/photos/key", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY, headers)
}

fn presigned_put(headers: HeaderMap) -> Request<http_body_util::Empty<Bytes>> {
    signed_request_with_headers(Signed::Query, Method::PUT, "/photos/key", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY, headers)
}

fn with_header<B>(mut request: Request<B>, name: &'static str, value: &'static str) -> Request<B> {
    request.headers_mut().insert(name, HeaderValue::from_static(value));
    request
}

fn headers_of(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
    pairs
        .iter()
        .map(|(name, value)| (http::HeaderName::from_static(name), HeaderValue::from_static(value)))
        .collect()
}

fn with_authorization<B>(mut request: Request<B>, value: &str) -> Request<B> {
    request
        .headers_mut()
        .insert(http::header::AUTHORIZATION, HeaderValue::from_str(value).expect("a header value"));
    request
}

#[tokio::test]
async fn a_signed_put_replayed_with_an_unsigned_amz_header_is_refused_before_the_handler() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    // GHSA-xm99: the signer's own PutObject, replayed with the header that turns it into a copy.
    for (name, value) in [
        ("x-amz-copy-source", "/private-bucket/source"),
        ("x-amz-meta-user", "mallory"),
        ("x-amz-tagging", "k=v"),
        ("x-amz-acl", "public-read"),
        ("x-amz-storage-class", "GLACIER"),
        ("x-amz-server-side-encryption", "AES256"),
    ] {
        let answer = observe(&stack, with_header(header_signed_put(HeaderMap::new()), name, value)).await;
        assert_refused_as_the_layer_refuses(&answer, UNSIGNED, name);
    }
    assert_eq!(probe.calls(), 0, "no handler ran");
}

#[tokio::test]
async fn the_same_put_signed_over_the_header_is_served() {
    // The control: the header the replay adds is covered by the signature, so it is not refused.
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    for headers in [
        headers_of(&[("x-amz-copy-source", "/private-bucket/source")]),
        headers_of(&[("x-amz-meta-user", "alice"), ("x-amz-tagging", "k=v")]),
        HeaderMap::new(),
    ] {
        let answer = observe(&stack, header_signed_put(headers)).await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    }
    assert_eq!(probe.calls(), 3);
}

#[tokio::test]
async fn an_unsigned_amz_header_is_refused_before_the_signature_is_judged() {
    // The layer reads the header list first, so a forged signature learns which rule it broke.
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let forged = signed_with_a_wrong_signature(request(Method::PUT, "/photos/key"), ROOT_ACCESS_KEY)
        .header("x-amz-copy-source", "/private-bucket/source");
    let answer = observe(&stack, forged.body(http_body_util::Empty::<Bytes>::new()).expect("request")).await;
    assert_refused_as_the_layer_refuses(&answer, UNSIGNED, "forged signature");
    assert_eq!(probe.calls(), 0);
}

#[tokio::test]
async fn a_presigned_put_with_an_unsigned_amz_header_is_refused_before_the_handler() {
    // GHSA-g8w9: the URL authorises the headers it names and no others.
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    for (name, value) in [
        ("x-amz-tagging", "k=v"),
        ("x-amz-acl", "public-read"),
        ("x-amz-meta-user", "mallory"),
    ] {
        let answer = observe(&stack, with_header(presigned_put(HeaderMap::new()), name, value)).await;
        assert_refused_as_the_layer_refuses(&answer, UNSIGNED, name);
    }
    assert_eq!(probe.calls(), 0);
}

#[tokio::test]
async fn a_presigned_put_is_served_when_its_header_is_signed() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let signed = observe(&stack, presigned_put(headers_of(&[("x-amz-tagging", "k=v")]))).await;
    assert_eq!(signed.status, StatusCode::OK, "{}", signed.body);
    assert_eq!(probe.calls(), 1);
}

#[tokio::test]
async fn the_header_cloudfront_adds_after_signing_is_served_and_its_lookalikes_are_refused() {
    // CloudFront stamps `x-amz-cf-id` on the origin request after the client signed it; the layer
    // let exactly that name through, on header-signed and presigned requests alike.
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    for request in [header_signed_put(HeaderMap::new()), presigned_put(HeaderMap::new())] {
        let answer = observe(&stack, with_header(request, "x-amz-cf-id", "edge-request")).await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    }
    assert_eq!(probe.calls(), 2);
    for name in ["x-amz-cf-id-2", "x-amz-cf", "x-amz-cf-idx"] {
        for request in [header_signed_put(HeaderMap::new()), presigned_put(HeaderMap::new())] {
            let answer = observe(&stack, with_header(request, name, "edge-request")).await;
            assert_refused_as_the_layer_refuses(&answer, UNSIGNED, name);
        }
    }
    assert_eq!(probe.calls(), 2, "no lookalike reached a handler");
}

#[tokio::test]
async fn a_swapped_or_unreadable_authorization_is_refused_before_routing() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let scope = format!(
        "Credential={ROOT_ACCESS_KEY}/20260914/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={}",
        "0".repeat(64)
    );
    for token in ["OTHER", "AWS4-HMAC-SHA512"] {
        let swapped = with_authorization(header_signed_put(HeaderMap::new()), &format!("{token} {scope}"));
        let answer = observe(&stack, swapped).await;
        assert_refused_as_the_layer_refuses(&answer, UNSUPPORTED, token);
    }
    // A value that claims SigV4 and does not read fails closed.
    for value in ["AWS4-HMAC-SHA256 garbage", "AWS4-HMAC-SHA256 Credential=x"] {
        let answer = observe(&stack, with_authorization(header_signed_put(HeaderMap::new()), value)).await;
        assert_refused_as_the_layer_refuses(&answer, INVALID, value);
    }
    // The swap is refused even where the same request would be refused for its route.
    let unrouted = with_authorization(
        signed_request_with_headers(
            Signed::Header,
            Method::PUT,
            "/",
            "acl",
            ROOT_ACCESS_KEY,
            ROOT_SECRET_KEY,
            HeaderMap::new(),
        ),
        &format!("OTHER {scope}"),
    );
    assert_refused_as_the_layer_refuses(&observe(&stack, unrouted).await, UNSUPPORTED, "unrouted");
    assert_eq!(probe.calls(), 0);
}

#[tokio::test]
async fn the_envelope_headers_and_a_non_sigv4_scheme_are_not_the_guard_s() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    // The four headers the verifier reads around the signature may stay unsigned.
    let mut request = header_signed_put(HeaderMap::new());
    request
        .headers_mut()
        .insert("x-amz-decoded-content-length", HeaderValue::from_static("0"));
    let answer = observe(&stack, request).await;
    assert_ne!(answer.body, layer_document(UNSIGNED), "{}", answer.body);
    // SigV2 signs every `x-amz-*` header itself, so the guard has nothing to read in it.
    let v2 = with_header(
        with_authorization(header_signed_put(HeaderMap::new()), &format!("AWS {ROOT_ACCESS_KEY}:c2lnbmF0dXJl")),
        "x-amz-copy-source",
        "/private-bucket/source",
    );
    let answer = observe(&stack, v2).await;
    for sentence in [UNSIGNED, UNSUPPORTED, INVALID] {
        assert!(!answer.body.contains(sentence), "{}", answer.body);
    }
}

#[tokio::test]
async fn unsigned_extension_headers_are_refused_before_the_handler_on_sigv4_and_sigv2() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let extension_headers = [
        ("x-rustfs-force-delete", "true"),
        ("x-rustfs-source-version-id", "0194e0f1-0000-7000-8000-000000000000"),
        ("x-rustfs-source-mtime", "2099-01-01T00:00:00Z"),
        ("x-rustfs-source-replication-request", "true"),
        ("x-rustfs-meta-owner", "mallory"),
        ("x-minio-force-delete", "true"),
        ("x-minio-meta-owner", "mallory"),
    ];

    for (name, value) in extension_headers {
        // SigV4 header-signed PUT with an unsigned extension header.
        let answer = observe(&stack, with_header(header_signed_put(HeaderMap::new()), name, value)).await;
        assert_refused_as_the_layer_refuses(&answer, UNSIGNED, &format!("sigv4 header {name}"));

        // SigV4 presigned PUT with an unsigned extension header.
        let answer = observe(&stack, with_header(presigned_put(HeaderMap::new()), name, value)).await;
        assert_refused_as_the_layer_refuses(&answer, UNSIGNED, &format!("sigv4 presigned {name}"));

        // SigV4 presigned DELETE with an unsigned extension header.
        let presigned_delete = signed_request_with_headers(
            Signed::Query,
            Method::DELETE,
            "/photos/key",
            "",
            ROOT_ACCESS_KEY,
            ROOT_SECRET_KEY,
            HeaderMap::new(),
        );
        let answer = observe(&stack, with_header(presigned_delete, name, value)).await;
        assert_refused_as_the_layer_refuses(&answer, UNSIGNED, &format!("sigv4 presigned delete {name}"));

        // SigV2 header-signed request with an extension header.
        let v2_header = with_header(
            with_authorization(header_signed_put(HeaderMap::new()), &format!("AWS {ROOT_ACCESS_KEY}:c2lnbmF0dXJl")),
            name,
            value,
        );
        let answer = observe(&stack, v2_header).await;
        assert_refused_as_the_layer_refuses(&answer, UNSIGNED, &format!("sigv2 header {name}"));

        // SigV2 presigned URL (including with appended SigV4 signed-headers query) with an extension header.
        let v2_presigned = request(
            Method::PUT,
            &format!("/photos/key?AWSAccessKeyId={ROOT_ACCESS_KEY}&Expires=2000000000&Signature=abc&X-Amz-Signature=def&X-Amz-SignedHeaders=host%3B{name}"),
        )
        .header(name, value)
        .body(http_body_util::Empty::<Bytes>::new())
        .expect("sigv2 presigned request");
        let answer = observe(&stack, v2_presigned).await;
        assert_refused_as_the_layer_refuses(&answer, UNSIGNED, &format!("sigv2 presigned {name}"));
    }
    assert_eq!(probe.calls(), 0, "no handler may run when extension headers are unsigned");
}

#[tokio::test]
async fn sigv4_requests_that_sign_extension_headers_are_served() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);

    for headers in [
        headers_of(&[("x-rustfs-meta-owner", "alice")]),
        headers_of(&[("x-minio-meta-owner", "alice")]),
        headers_of(&[
            ("x-rustfs-source-replication-request", "true"),
            ("x-rustfs-source-version-id", "0194e0f1-0000-7000-8000-000000000000"),
        ]),
    ] {
        let answer = observe(&stack, header_signed_put(headers)).await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    }

    for headers in [
        headers_of(&[("x-rustfs-meta-owner", "alice")]),
        headers_of(&[("x-minio-meta-owner", "alice")]),
    ] {
        let answer = observe(&stack, presigned_put(headers)).await;
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    }

    let signed_delete = signed_request_with_headers(
        Signed::Header,
        Method::DELETE,
        "/photos/key",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        headers_of(&[("x-rustfs-force-delete", "true")]),
    );
    let answer = observe(&stack, signed_delete).await;
    assert_eq!(answer.status, StatusCode::NO_CONTENT, "{}", answer.body);

    assert_eq!(probe.calls(), 6, "every signed extension-header request must reach the handler");
}

#[tokio::test]
async fn a_head_request_with_an_unsigned_extension_header_answers_forbidden_without_a_body() {
    let probe = Probe::new();
    let stack = probe_stack(&probe);
    let head = signed_request_with_headers(
        Signed::Query,
        Method::HEAD,
        "/photos/key",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        HeaderMap::new(),
    );
    let answer = observe(&stack, with_header(head, "x-rustfs-meta-owner", "mallory")).await;
    assert_eq!(answer.status, StatusCode::FORBIDDEN);
    assert!(answer.body.is_empty(), "HEAD refusal must carry no body: {}", answer.body);
    assert!(answer.headers.get("x-amz-request-id").is_some());
    assert_eq!(
        answer.headers.get(http::header::CONTENT_TYPE),
        Some(&HeaderValue::from_static("application/xml"))
    );
    assert_eq!(answer.headers.get(http::header::CONNECTION), Some(&HeaderValue::from_static("close")));
    assert_eq!(probe.calls(), 0);
}
