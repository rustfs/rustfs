// Copyright 2026 RustFS Team
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

//! Reserved console paths on a console-disabled listener (for example, the S3 listener while a
//! dedicated console port serves static assets): anonymous requests get `403 AccessDenied`, valid
//! SigV4 requests get `501 NotImplemented`, and custom single-segment prefixes like `/ui` and
//! `/UI_assets` verify over their original URI without tripping S3 bucket-name parsing
//! (rustfs/backlog#2771).

use super::*;
use crate::server::{APPLE_TOUCH_ICON_PATH, APPLE_TOUCH_ICON_PRECOMPOSED_PATH, FAVICON_PATH};

fn host_for(prefix: &'static str) -> GatewayStack {
    super::super::reserved_console::assemble_for_prefix(
        assembly(),
        Arc::new(ScriptedIam { ready: true }),
        root().source(),
        prefix,
        None,
    )
    .expect("reserved console host assembles")
}

fn not_ready_host_for(prefix: &'static str) -> GatewayStack {
    let assembly = assembly();
    let server_ctx = Some(Arc::clone(&assembly.server_ctx));
    super::super::reserved_console::assemble_for_prefix(
        assembly,
        Arc::new(ScriptedIam { ready: true }),
        root().source(),
        prefix,
        server_ctx,
    )
    .expect("reserved console host assembles")
}

#[tokio::test]
async fn default_and_single_segment_console_prefixes_return_403_anonymous_and_501_signed() {
    for (prefix, targets) in [
        (
            "/rustfs/console",
            [
                ("/rustfs/console", ""),
                ("/rustfs/console/", ""),
                ("/rustfs/console/index.html", ""),
                ("/rustfs/console/index.html", "v=1"),
                (FAVICON_PATH, ""),
                (APPLE_TOUCH_ICON_PATH, ""),
                (APPLE_TOUCH_ICON_PRECOMPOSED_PATH, ""),
            ]
            .as_slice(),
        ),
        (
            "/ui",
            [("/ui", ""), ("/ui/", ""), ("/ui/index.html", ""), ("/ui/index.html", "v=1")].as_slice(),
        ),
        (
            "/UI_assets",
            [
                ("/UI_assets", ""),
                ("/UI_assets/", ""),
                ("/UI_assets/index.html", ""),
                ("/UI_assets/index.html", "v=1"),
            ]
            .as_slice(),
        ),
    ] {
        let host = host_for(prefix);
        for &(path, query) in targets {
            let uri = if query.is_empty() {
                path.to_owned()
            } else {
                format!("{path}?{query}")
            };

            // Anonymous request -> 403 AccessDenied (never 501 or 400 InvalidBucketName).
            let anon_req = request(Method::GET, &uri).body(Empty::new()).expect("anon request");
            let (status, _, body) = answer_request(&host, anon_req).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "anonymous {prefix} -> {uri}: {body}");
            assert!(
                body.contains("<Code>AccessDenied</Code>"),
                "anonymous {prefix} -> {uri} expected AccessDenied XML, got: {body}"
            );

            // Valid SigV4 header-signed request over original URI -> 501 NotImplemented.
            let signed_req = signed_request(Signed::Header, Method::GET, path, query, ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
            let (status, _, body) = answer_request(&host, signed_req).await;
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "header-signed {prefix} -> {uri}: {body}");
            assert!(
                body.contains("<Code>NotImplemented</Code>"),
                "header-signed {prefix} -> {uri} expected NotImplemented XML, got: {body}"
            );

            // Valid SigV4 presigned request over original URI -> 501 NotImplemented.
            let presigned_req = signed_request(Signed::Query, Method::GET, path, query, ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
            let (status, _, body) = answer_request(&host, presigned_req).await;
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "presigned {prefix} -> {uri}: {body}");
            assert!(
                body.contains("<Code>NotImplemented</Code>"),
                "presigned {prefix} -> {uri} expected NotImplemented XML, got: {body}"
            );
        }
    }
}

#[tokio::test]
async fn reserved_console_verifies_original_uri_and_enforces_sigv4_and_readiness() {
    let host = host_for("/UI_assets");

    // Wrong secret -> 403 SignatureDoesNotMatch (signature verified over original `/UI_assets/index.html?v=1`).
    let bad_sig = signed_request(
        Signed::Header,
        Method::GET,
        "/UI_assets/index.html",
        "v=1",
        ROOT_ACCESS_KEY,
        "wrong-secret",
    );
    let (status, _, body) = answer_request(&host, bad_sig).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "bad sig: {body}");
    assert!(
        body.contains("<Code>SignatureDoesNotMatch</Code>"),
        "bad sig expected SignatureDoesNotMatch, got: {body}"
    );

    // Tampered query after signing -> 403 SignatureDoesNotMatch.
    let mut tampered = signed_request(
        Signed::Header,
        Method::GET,
        "/UI_assets/index.html",
        "v=1",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
    );
    *tampered.uri_mut() = "/UI_assets/index.html?v=2".parse().expect("tampered uri");
    let (status, _, body) = answer_request(&host, tampered).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "tampered query: {body}");
    assert!(
        body.contains("<Code>SignatureDoesNotMatch</Code>"),
        "tampered query expected SignatureDoesNotMatch, got: {body}"
    );

    // SigV2 Authorization -> refused at the SigV4-only floor with 403 AccessDenied.
    let sigv2 = request(Method::GET, "/UI_assets/index.html")
        .header(http::header::DATE, chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header(http::header::AUTHORIZATION, format!("AWS {ROOT_ACCESS_KEY}:invalid"))
        .body(Empty::new())
        .expect("sigv2 request");
    let (status, _, body) = answer_request(&host, sigv2).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "sigv2: {body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "sigv2 expected AccessDenied, got: {body}");

    // Repeated Authorization headers -> 403 AccessDenied.
    let mut repeated = signed_request(Signed::Header, Method::GET, "/UI_assets/index.html", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
    let first = repeated
        .headers()
        .get(http::header::AUTHORIZATION)
        .expect("authorization")
        .clone();
    repeated.headers_mut().append(http::header::AUTHORIZATION, first);
    let (status, _, body) = answer_request(&host, repeated).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "repeated auth: {body}");
    assert!(
        body.contains("<Code>AccessDenied</Code>"),
        "repeated auth expected AccessDenied, got: {body}"
    );

    // Unsigned x-amz-* header on a SigV4 request -> 403 AccessDenied (GHSA-g8w9).
    let mut unsigned_amz =
        signed_request(Signed::Header, Method::GET, "/UI_assets/index.html", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
    unsigned_amz
        .headers_mut()
        .insert("x-amz-meta-unsigned", http::HeaderValue::from_static("injected"));
    let (status, _, body) = answer_request(&host, unsigned_amz).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "unsigned x-amz header: {body}");
    assert!(
        body.contains("<Code>AccessDenied</Code>"),
        "unsigned x-amz header expected AccessDenied, got: {body}"
    );

    // Signed request while server context is not installed -> 503 ServiceUnavailable.
    let not_ready = not_ready_host_for("/ui");
    let signed_req = signed_request(Signed::Header, Method::GET, "/ui/index.html", "v=1", ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
    let (status, _, body) = answer_request(&not_ready, signed_req).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "not ready: {body}");
}
