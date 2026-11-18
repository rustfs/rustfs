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

//! The control-plane host over a router of probes: which claimed requests reach a handler, with
//! which caller and body, and which are refused before it. Real IAM and the legacy comparison are
//! in `server/http/tests/native_admin_tests/control_plane_tests.rs`.

use super::*;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation as OwnedOperation};
use crate::admin::router::{AdminOperation, S3Router};
use crate::server::gateway_handlers::test_support::signed_body_request;

/// Every request a probe answered: its method, target, caller key, region, service and body.
static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Probe;

#[async_trait::async_trait]
impl OwnedOperation for Probe {
    async fn call(&self, mut req: AdminRequest, _params: matchit::Params<'_, '_>) -> rustfs_s3_types::S3Result<AdminResponse> {
        let body = req.take_body().store_all_limited(1024).await.unwrap_or_default();
        let seen = format!(
            "{} {} key={:?} region={:?} service={:?} body={:?}",
            req.method(),
            req.uri(),
            req.credentials().map(|credentials| credentials.access_key()),
            req.region(),
            req.service(),
            String::from_utf8_lossy(&body),
        );
        SEEN.lock().expect("probe lock").push(seen);
        Ok(AdminResponse::new(StatusCode::OK, rustfs_s3_types::Body::from("probe")))
    }
}

static PROBE: Probe = Probe;

/// Answers without reading its body, as several admin handlers do.
struct Ignoring;

#[async_trait::async_trait]
impl OwnedOperation for Ignoring {
    async fn call(&self, _req: AdminRequest, _params: matchit::Params<'_, '_>) -> rustfs_s3_types::S3Result<AdminResponse> {
        Ok(AdminResponse::new(StatusCode::NO_CONTENT, rustfs_s3_types::Body::empty()))
    }
}

static IGNORING: Ignoring = Ignoring;

fn host() -> GatewayStack {
    let mut router = S3Router::new(false);
    for (method, path) in [
        (Method::GET, "/rustfs/admin/v3/probe"),
        (Method::PUT, "/rustfs/admin/v3/probe"),
        (Method::GET, "?replication-metrics/{bucket}"),
        (Method::PUT, "?replication-reset/{bucket}"),
        (Method::POST, "/"),
    ] {
        router.insert(method, path, AdminOperation(&PROBE)).expect("a probe route");
    }
    router
        .insert(Method::PUT, "/rustfs/admin/v3/ignore", AdminOperation(&IGNORING))
        .expect("an ignoring route");
    let assembly = assembly();
    let iam = Arc::new(ScriptedIam { ready: true });
    super::super::control_plane::assemble(assembly, iam, root().source(), Arc::new(router)).expect("the host assembles")
}

fn signed(method: Method, path: &str, query: &str) -> Request<Empty<Bytes>> {
    signed_request(Signed::Header, method, path, query, ROOT_ACCESS_KEY, ROOT_SECRET_KEY)
}

async fn served(request: Request<Empty<Bytes>>) -> ((StatusCode, String), Vec<String>) {
    SEEN.lock().expect("probe lock").clear();
    let (status, _, body) = answer_request(&host(), request).await;
    ((status, body), std::mem::take(&mut *SEEN.lock().expect("probe lock")))
}

#[tokio::test]
#[serial_test::serial]
async fn a_signed_claimed_request_reaches_its_route_as_the_signer() {
    for (method, path, query) in [
        (Method::GET, "/rustfs/admin/v3/probe", ""),
        (Method::GET, "/missing-bucket", "replication-metrics="),
        (Method::PUT, "/missing-bucket", "replication-reset="),
    ] {
        let ((status, body), seen) = served(signed(method.clone(), path, query)).await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "probe"), "{method} {path}?{query}");
        assert_eq!(seen.len(), 1, "{method} {path}?{query}");
        assert!(
            seen[0].ends_with(&format!(
                "key=Some({ROOT_ACCESS_KEY:?}) region=Some(\"us-east-1\") service=Some(\"s3\") body=\"\""
            )),
            "{seen:?}"
        );
    }
}

#[tokio::test]
#[serial_test::serial]
async fn a_signed_body_reaches_the_handler_whole() {
    let request = signed_body_request(
        Method::PUT,
        "/rustfs/admin/v3/probe",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        Bytes::from_static(b"{\"a\":1}"),
    );
    SEEN.lock().expect("probe lock").clear();
    let (status, _, _) = answer_request_full(&host(), request).await;
    assert_eq!(status, StatusCode::OK);
    let seen = std::mem::take(&mut *SEEN.lock().expect("probe lock"));
    assert_eq!(seen.len(), 1);
    assert!(seen[0].ends_with("body=\"{\\\"a\\\":1}\""), "{seen:?}");
}

#[tokio::test]
#[serial_test::serial]
async fn a_handler_that_never_reads_its_body_is_answered_as_it_answered() {
    let request = signed_body_request(
        Method::PUT,
        "/rustfs/admin/v3/ignore",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        Bytes::from_static(b"unread"),
    );
    let (status, _, body) = answer_request_full(&host(), request).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
}

#[tokio::test]
#[serial_test::serial]
async fn a_body_the_signature_does_not_cover_is_refused_before_the_handler_keeps_it() {
    let mut request = signed_body_request(
        Method::PUT,
        "/rustfs/admin/v3/probe",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        Bytes::from_static(b"{\"a\":1}"),
    );
    *request.body_mut() = http_body_util::Full::new(Bytes::from_static(b"{\"a\":2}"));
    SEEN.lock().expect("probe lock").clear();
    let (status, _, _) = answer_request_full(&host(), request).await;
    let seen = std::mem::take(&mut *SEEN.lock().expect("probe lock"));
    assert!(!status.is_success(), "a tampered body was answered {status}");
    assert!(seen.iter().all(|seen| !seen.contains("{\\\"a\\\":2}")), "{seen:?}");
}

#[tokio::test]
#[serial_test::serial]
async fn an_anonymous_admin_or_extension_request_is_refused_before_any_handler() {
    for (method, target) in [
        (Method::GET, "/rustfs/admin/v3/probe"),
        (Method::PUT, "/rustfs/admin/v3/probe"),
        (Method::GET, "/missing-bucket?replication-metrics="),
        (Method::PUT, "/missing-bucket?replication-reset="),
    ] {
        let request = super::request(method.clone(), target)
            .body(Empty::new())
            .expect("static request");
        let ((status, body), seen) = served(request).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {target}");
        assert!(body.contains("<Code>AccessDenied</Code>"), "{method} {target}: {body}");
        assert!(seen.is_empty(), "{method} {target} reached a handler: {seen:?}");
    }
}

#[tokio::test]
#[serial_test::serial]
async fn a_wrong_signature_is_refused_before_any_handler() {
    for target in ["/rustfs/admin/v3/probe", "/missing-bucket?replication-metrics="] {
        let request = signed_with_a_wrong_signature(super::request(Method::GET, target), ROOT_ACCESS_KEY)
            .body(Empty::new())
            .expect("static request");
        let ((status, body), seen) = served(request).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{target}");
        assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"), "{target}: {body}");
        assert!(seen.is_empty(), "{target}: {seen:?}");
    }
}

#[tokio::test]
#[serial_test::serial]
async fn a_sigv2_header_is_refused_by_the_admin_rule() {
    let request = super::request(Method::GET, "/rustfs/admin/v3/probe")
        .header("date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("authorization", format!("AWS {ROOT_ACCESS_KEY}:AAAAAAAAAAAAAAAAAAAAAAAAAAA="))
        .body(Empty::new())
        .expect("static request");
    let ((status, body), seen) = served(request).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("<Code>"), "{body}");
    assert!(seen.is_empty(), "{seen:?}");
}

#[tokio::test]
#[serial_test::serial]
async fn the_anonymous_sts_form_reaches_its_handler_and_another_post_does_not() {
    let form = super::request(Method::POST, "/")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Empty::new())
        .expect("static request");
    let ((status, _), seen) = served(form).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].contains("key=None region=None service=None"), "{seen:?}");

    let plain = super::request(Method::POST, "/").body(Empty::new()).expect("static request");
    let ((status, _), seen) = served(plain).await;
    assert_ne!(status, StatusCode::OK);
    assert!(seen.is_empty(), "{seen:?}");
}

#[tokio::test]
#[serial_test::serial]
async fn a_claimed_request_with_no_route_is_answered_without_a_handler() {
    for (signed_request, expected) in [(true, StatusCode::NOT_IMPLEMENTED), (false, StatusCode::FORBIDDEN)] {
        let request = if signed_request {
            signed(Method::GET, "/rustfs/admin/v3/no-such-route", "")
        } else {
            super::request(Method::GET, "/rustfs/admin/v3/no-such-route")
                .body(Empty::new())
                .expect("static request")
        };
        let ((status, _), seen) = served(request).await;
        assert_eq!(status, expected, "signed={signed_request}");
        assert!(seen.is_empty(), "{seen:?}");
    }
}

async fn answer_request_full(
    stack: &GatewayStack,
    request: Request<http_body_util::Full<Bytes>>,
) -> (StatusCode, HeaderMap, String) {
    let response = stack.call(request).await;
    let (parts, body) = response.into_parts();
    let body = body.collect().await.expect("the answer is read").to_bytes();
    (parts.status, parts.headers, String::from_utf8_lossy(&body).into_owned())
}
