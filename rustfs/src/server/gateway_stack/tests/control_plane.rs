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
use crate::server::gateway_handlers::test_support::{CountedBody, signed_body_request, signed_request_with_headers};
use std::sync::atomic::Ordering;

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
        (Method::GET, "/rustfs/admin/v3/object-zip-downloads/{file}"),
        (Method::GET, "/rustfs/admin/v3/oidc/providers"),
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
async fn a_valid_sigv2_header_is_refused_by_the_admin_rule() {
    let date = chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
    let request = super::request(Method::GET, "/rustfs/admin/v3/probe")
        .header("date", date)
        .body(Empty::new())
        .expect("static request");
    let request = rustfs_signer::try_sign_v2(request, 0, ROOT_ACCESS_KEY, ROOT_SECRET_KEY, false).expect("a SigV2 signature");
    let ((status, body), seen) = served(request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(
        refusal(&body),
        (
            "AccessDenied".to_owned(),
            "Header or query Signature Version 4 is required for this operation".to_owned()
        )
    );
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

/// The error code and message of an `<Error>` document.
fn refusal(body: &str) -> (String, String) {
    let field = |name: &str| {
        body.split_once(&format!("<{name}>"))
            .and_then(|(_, rest)| rest.split_once(&format!("</{name}>")))
            .map(|(value, _)| value.to_owned())
            .unwrap_or_default()
    };
    (field("Code"), field("Message"))
}

/// What the host answered `request`, and what the probes saw.
async fn served_body<B>(request: Request<B>) -> ((StatusCode, String), Vec<String>)
where
    B: http_body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    SEEN.lock().expect("probe lock").clear();
    let response = host().call(request).await;
    let (parts, body) = response.into_parts();
    let body = body.collect().await.expect("the answer is read").to_bytes();
    let seen = std::mem::take(&mut *SEEN.lock().expect("probe lock"));
    ((parts.status, String::from_utf8_lossy(&body).into_owned()), seen)
}

/// `request` carrying `size` bytes in a body that declares no length, and the count of the bytes
/// read from it.
fn unsized_body(request: Request<Empty<Bytes>>, size: usize) -> (Request<CountedBody>, Arc<std::sync::atomic::AtomicUsize>) {
    let (body, taken) = CountedBody::new(size);
    let (mut parts, _) = request.into_parts();
    parts
        .headers
        .insert(http::header::TRANSFER_ENCODING, http::HeaderValue::from_static("chunked"));
    (Request::from_parts(parts, body), taken)
}

const MIB: usize = 1024 * 1024;

/// The admin rule's refusal of an anonymous caller, as legacy RustFS words it.
const ADMIN_RULE_ANONYMOUS: &str = "Signature is required";

/// The gateway floor's refusal of an anonymous caller on an extension row.
const EXTENSION_FLOOR_ANONYMOUS: &str = "the request is not allowed";

fn anonymous(method: Method, target: &str) -> Request<Empty<Bytes>> {
    super::request(method, target).body(Empty::new()).expect("static request")
}

fn sts_form() -> Request<Empty<Bytes>> {
    super::request(Method::POST, "/")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Empty::new())
        .expect("static request")
}

#[tokio::test]
#[serial_test::serial]
async fn an_anonymous_unsized_admin_body_is_refused_before_any_of_it_is_read() {
    // An extension row's floor admits no anonymous caller, so the gateway refuses it before the
    // rule, in its own words.
    for (method, target, message) in [
        (Method::PUT, "/rustfs/admin/v3/probe", ADMIN_RULE_ANONYMOUS),
        (Method::PUT, "/minio/admin/v3/probe", ADMIN_RULE_ANONYMOUS),
        (Method::PUT, "/missing-bucket?replication-reset=", EXTENSION_FLOOR_ANONYMOUS),
    ] {
        let (request, taken) = unsized_body(anonymous(method.clone(), target), 2 * MIB);
        let ((status, body), seen) = served_body(request).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {target}: {body}");
        assert_eq!(refusal(&body), ("AccessDenied".to_owned(), message.to_owned()), "{method} {target}");
        assert_eq!(taken.load(Ordering::SeqCst), 0, "{method} {target}: the body was read");
        assert!(seen.is_empty(), "{method} {target}: {seen:?}");
    }
}

#[tokio::test]
#[serial_test::serial]
async fn an_anonymous_unsized_sts_form_is_read_no_further_than_the_claimed_ceiling() {
    let (request, taken) = unsized_body(sts_form(), 4 * MIB);
    let ((status, body), seen) = served_body(request).await;
    assert!(status.is_client_error(), "{status}: {body}");
    // At most the frame that crossed the ceiling is read past it.
    assert!(
        taken.load(Ordering::SeqCst) <= MIB + 64 * 1024,
        "{} bytes read",
        taken.load(Ordering::SeqCst)
    );
    assert!(seen.is_empty(), "{seen:?}");
}

#[tokio::test]
#[serial_test::serial]
async fn a_signed_unsized_admin_body_is_refused_unread_as_legacy_refuses_it() {
    for size in [MIB, 2 * MIB] {
        let (request, taken) = unsized_body(signed(Method::PUT, "/rustfs/admin/v3/probe", ""), size);
        let ((status, body), seen) = served_body(request).await;
        assert_eq!(status, StatusCode::LENGTH_REQUIRED, "{size}: {body}");
        assert_eq!(
            refusal(&body),
            ("MissingContentLength".to_owned(), "missing header: content-length".to_owned()),
            "{size}"
        );
        assert_eq!(taken.load(Ordering::SeqCst), 0, "{size}: the body was read");
        assert!(seen.is_empty(), "{size}: {seen:?}");
    }
}

#[tokio::test]
#[serial_test::serial]
async fn a_declared_body_at_the_claimed_ceiling_reaches_the_handler_and_one_byte_more_does_not() {
    for (size, expected) in [(MIB, StatusCode::OK), (MIB + 1, StatusCode::BAD_REQUEST)] {
        let request = signed_body_request(
            Method::PUT,
            "/rustfs/admin/v3/probe",
            "",
            ROOT_ACCESS_KEY,
            ROOT_SECRET_KEY,
            Bytes::from(vec![b'a'; size]),
        );
        let ((status, body), seen) = served_body(request).await;
        assert_eq!(status, expected, "{size}: {body}");
        if expected == StatusCode::OK {
            assert_eq!(seen.len(), 1, "{size}: {seen:?}");
        } else {
            assert_eq!(
                refusal(&body),
                (
                    "EntityTooLarge".to_owned(),
                    "Custom route request body exceeds the configured maximum size.".to_owned()
                )
            );
            assert!(seen.is_empty(), "{size}: {seen:?}");
        }
    }
}

/// `request` with its `Authorization` line sent twice.
fn twice_authorized<B>(mut request: Request<B>) -> Request<B> {
    let authorization = request.headers()[http::header::AUTHORIZATION].clone();
    request.headers_mut().append(http::header::AUTHORIZATION, authorization);
    request
}

#[tokio::test]
#[serial_test::serial]
async fn a_repeated_authorization_is_answered_as_legacy_answers_an_anonymous_caller() {
    for (method, path, query, message) in [
        (Method::GET, "/rustfs/admin/v3/probe", "", ADMIN_RULE_ANONYMOUS),
        (Method::PUT, "/rustfs/admin/v3/probe", "", ADMIN_RULE_ANONYMOUS),
        (Method::GET, "/missing-bucket", "replication-metrics=", EXTENSION_FLOOR_ANONYMOUS),
    ] {
        let ((status, body), seen) = served(twice_authorized(signed(method.clone(), path, query))).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}?{query}: {body}");
        assert_eq!(refusal(&body), ("AccessDenied".to_owned(), message.to_owned()), "{method} {path}?{query}");
        assert!(seen.is_empty(), "{method} {path}?{query}: {seen:?}");
    }
}

/// Builds one anonymous request.
type Anonymous = fn() -> Request<Empty<Bytes>>;

/// A route class the rule admits anonymous callers to, and its anonymous request.
fn anonymous_classes() -> [(&'static str, Anonymous); 3] {
    [
        ("sts form", sts_form),
        ("zip token", || {
            anonymous(Method::GET, "/rustfs/admin/v3/object-zip-downloads/id.zip?token=t")
        }),
        ("oidc providers", || anonymous(Method::GET, "/rustfs/admin/v3/oidc/providers")),
    ]
}

#[tokio::test]
#[serial_test::serial]
async fn each_anonymous_class_reaches_its_handler_as_anonymous() {
    for (class, request) in anonymous_classes() {
        let ((status, body), seen) = served(request()).await;
        assert_eq!(status, StatusCode::OK, "{class}: {body}");
        assert_eq!(seen.len(), 1, "{class}: {seen:?}");
        assert!(seen[0].contains("key=None"), "{class}: {seen:?}");
    }
}

#[tokio::test]
#[serial_test::serial]
async fn a_presented_credential_on_an_anonymous_class_is_never_served_as_anonymous() {
    for (class, request) in anonymous_classes() {
        let (parts, body) = request().into_parts();
        let wrong = signed_with_a_wrong_signature(super::request(parts.method.clone(), &parts.uri.to_string()), ROOT_ACCESS_KEY);
        let mut wrong = wrong.body(Empty::<Bytes>::new()).expect("static request");
        wrong.headers_mut().extend(parts.headers.clone());
        let mut garbage = Request::from_parts(parts.clone(), Empty::<Bytes>::new());
        garbage
            .headers_mut()
            .insert(http::header::AUTHORIZATION, http::HeaderValue::from_static("AWS4-HMAC-SHA256 garbage"));
        let mut token = Request::from_parts(parts, body);
        token
            .headers_mut()
            .insert("x-amz-security-token", http::HeaderValue::from_static("stray-session-token"));
        let mut twice = signed_request_with_headers(
            Signed::Header,
            token.method().clone(),
            token.uri().path(),
            token.uri().query().unwrap_or_default(),
            ROOT_ACCESS_KEY,
            ROOT_SECRET_KEY,
            HeaderMap::new(),
        );
        if let Some(content_type) = token.headers().get(http::header::CONTENT_TYPE) {
            twice.headers_mut().insert(http::header::CONTENT_TYPE, content_type.clone());
        }
        for (variant, request) in [
            ("wrong signature", wrong),
            ("malformed authorization", garbage),
            ("stray session token", token),
            ("repeated authorization", twice_authorized(twice)),
        ] {
            let ((status, body), seen) = served(request).await;
            assert!(status.is_client_error(), "{class} {variant}: {status} {body}");
            assert!(seen.is_empty(), "{class} {variant} was served: {seen:?}");
        }
    }
}

/// GHSA-xm99-m3gq-83g8 on the control-plane host: a signed request replayed with an `x-amz-*`
/// header its signature does not name is refused before any handler, and the same request without
/// the header is served (the control, which proves the refusal is the header's).
#[tokio::test]
#[serial_test::serial]
async fn an_unsigned_amz_header_is_refused_before_any_control_plane_handler() {
    for (path, query) in [("/rustfs/admin/v3/probe", ""), ("/missing-bucket", "replication-metrics=")] {
        let ((status, body), seen) = served(signed(Method::GET, path, query)).await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "probe"), "control {path}?{query}");
        assert_eq!(seen.len(), 1, "the control reached the handler: {path}?{query}");

        for (name, value) in [
            ("x-amz-copy-source", "/private-bucket/source"),
            ("x-amz-meta-user", "mallory"),
        ] {
            let mut request = signed(Method::GET, path, query);
            request.headers_mut().insert(name, http::HeaderValue::from_static(value));
            let ((status, body), seen) = served(request).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{name} {path}?{query}: {body}");
            assert!(body.contains("<Code>AccessDenied</Code>"), "{name} {path}?{query}: {body}");
            assert!(body.contains("not signed"), "{name} {path}?{query}: {body}");
            assert!(seen.is_empty(), "{name} {path}?{query} reached a handler: {seen:?}");
        }
    }
}

#[tokio::test]
#[serial_test::serial]
async fn typed_admin_routes_serve_canonical_alias_verified_body_and_presigned_object_lambda() {
    use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::runtime_sources::{AppContext, publish_test_app_context};
    use rustfs_iam::store::object::{IAM_CONFIG_PREFIX, ObjectStore};
    use rustfs_iam::store::{Store as _, UserType};
    use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
    use rustfs_policy::policy::Policy;
    use serde_json::{Value, json};

    const USER: &str = "typed-admin-caller";
    const SECRET: &str = "typed-admin-caller-test-secret";
    const BUCKET: &str = "typed-etag-bucket";
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("typed_admin_routes")
        .disk_count(1)
        .init_bucket_metadata(true)
        .build()
        .await;
    env.ecstore
        .make_bucket(BUCKET, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    ObjectStore::new(env.ecstore.clone())
        .save_iam_config(json!({"version":1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
        .await
        .expect("IAM format");
    let iam = rustfs_iam::build_iam_sys(env.ecstore.clone()).await.expect("ready IAM");
    iam.create_user(
        USER,
        &AddOrUpdateUserReq {
            secret_key: SECRET.to_owned(),
            policy: None,
            status: AccountStatus::Enabled,
        },
    )
    .await
    .expect("create caller");
    let policy = Policy::parse_config(
        br#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"admin:SiteReplicationRemove"},
            {"Effect":"Allow","Action":"s3:GetObject","Resource":["arn:aws:s3:::rustfs/*","arn:aws:s3:::minio/*"]}
        ]}"#,
    )
    .expect("caller policy");
    iam.set_policy(USER, policy).await.expect("save caller policy");
    iam.policy_db_set(USER, UserType::Reg, false, USER)
        .await
        .expect("attach caller policy");

    let context = Arc::new(AppContext::with_default_interfaces(
        env.ecstore.clone(),
        iam,
        Arc::new(rustfs_kms::KmsServiceManager::new()),
    ));
    assert!(context.iam().is_ready());
    assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
        access_key: ROOT_ACCESS_KEY.to_owned(),
        secret_key: ROOT_SECRET_KEY.to_owned(),
        status: "on".to_owned(),
        ..Default::default()
    }));
    publish_test_app_context(context.clone());
    let assembly = assembly();
    assert!(assembly.server_ctx.install(context));
    let router = crate::admin::make_admin_router(false, assembly.server_ctx.clone()).expect("production router");
    let stack = GatewayStack::assemble_control_plane(assembly, Arc::new(router)).expect("control-plane stack");
    for row in rustfs_gateway_dialect_rustfs_admin::native::extensions::control_plane::ROWS {
        assert!(stack.service.operations().any(|registered| registered == row.name), "{}", row.name);
    }

    for prefix in ["/rustfs/admin", "/minio/admin"] {
        let request = signed_body_request(
            Method::POST,
            &format!("{prefix}/v4/account/permissions"),
            "",
            USER,
            SECRET,
            Bytes::from_static(br#"{"checks":[{"id":"capabilities","action":"admin:SiteReplicationRemove"}]}"#),
        );
        let (status, _, body) = answer_request_full(&stack, request).await;
        assert_eq!(status, StatusCode::OK, "{prefix}: {body}");
        assert_eq!(
            serde_json::from_str::<Value>(&body).expect("permission results"),
            json!({"results":[{"id":"capabilities","decision":"allowed"}]})
        );
    }

    let root_put = signed_body_request(
        Method::PUT,
        &format!("/rustfs/admin/v3/bucket-etag/{BUCKET}"),
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        Bytes::from_static(br#"{"algorithm":"xxh3"}"#),
    );
    let (status, _, body) = answer_request_full(&stack, root_put).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        serde_json::from_str::<Value>(&body).expect("set bucket etag"),
        json!({"bucket": BUCKET, "algorithm": "xxh3"})
    );

    let payload = Bytes::from_static(b"aaaa");
    let unauth_delete = signed_body_request(
        Method::DELETE,
        &format!("/rustfs/admin/v3/bucket-etag/{BUCKET}"),
        "",
        USER,
        SECRET,
        payload.clone(),
    );
    let (status, _, _) = answer_request_full(&stack, unauth_delete).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Unused payload on a control-plane DELETE is buffered and verified before the handler answers.
    let root_delete = signed_body_request(
        Method::DELETE,
        &format!("/rustfs/admin/v3/bucket-etag/{BUCKET}"),
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        payload.clone(),
    );
    let (counted, bytes_read) = CountedBody::new(payload.len());
    let response = stack.call(root_delete.map(|_| counted)).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(bytes_read.load(Ordering::SeqCst), payload.len());
    let deleted = response.into_body().collect().await.expect("delete etag body").to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&deleted).expect("deleted bucket etag"),
        json!({"bucket": BUCKET, "algorithm": null})
    );

    // Admin GET paths preserve header-signed and presigned Object Lambda priority (#1577).
    let expected = json!({"contractVersion":2,"durableCleanup":true});
    for prefix in ["/rustfs/admin", "/minio/admin"] {
        let path = format!("{prefix}/v3/site-replication/remove-v2/capabilities");
        let admin_get = signed_request(Signed::Header, Method::GET, &path, "local=true", USER, SECRET);
        let (status, _, body) = answer_request(&stack, admin_get).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert_eq!(serde_json::from_str::<Value>(&body).expect("removal capabilities"), expected);

        let lambda_query = "lambdaArn=arn%3Aacme%3As3-object-lambda%3A%3Atransformer%3Awebhook";
        for form in [Signed::Header, Signed::Query] {
            let lambda_get = signed_request(form, Method::GET, &path, lambda_query, USER, SECRET);
            let (status, _, body) = answer_request(&stack, lambda_get).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}?{lambda_query}: {body}");
            assert!(body.contains("<Code>NoSuchBucket</Code>"), "{path}?{lambda_query}: {body}");
        }

        let encoded_query = "lambda%41rn=arn%3Aacme%3As3-object-lambda%3A%3Atransformer%3Awebhook";
        let encoded_get = signed_request(Signed::Query, Method::GET, &path, encoded_query, USER, SECRET);
        let (status, _, body) = answer_request(&stack, encoded_get).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}?{encoded_query}: {body}");
        assert!(body.contains("<Code>InvalidArgument</Code>"), "{path}?{encoded_query}: {body}");
    }
}
