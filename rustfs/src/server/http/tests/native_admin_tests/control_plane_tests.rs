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

//! The control-plane host against the legacy stack, over the same admin router, real IAM and
//! real signatures: each request class answers the same, and none reaches the legacy service.

use super::*;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation as OwnedOperation};
use crate::admin::router::{AdminOperation, S3Router};
use crate::server::gateway_handlers::test_support::signed_body_request;
use crate::server::stack::select_stack;
use http_body_util::Full;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A host both stacks read path-style. With a server domain configured, the gateway reads a bare
/// bucket-shaped host such as `localhost` as a CNAME bucket, while the admin router's claim reads
/// the path alone.
const HOST: &str = "localhost:9000";

/// The legacy service, counting every request it is handed.
#[derive(Clone)]
struct Recorded(s3s::service::S3Service, Arc<AtomicUsize>);

impl<B> Service<HttpRequest<B>> for Recorded
where
    s3s::service::S3Service: Service<HttpRequest<B>>,
{
    type Response = <s3s::service::S3Service as Service<HttpRequest<B>>>::Response;
    type Error = <s3s::service::S3Service as Service<HttpRequest<B>>>::Error;
    type Future = <s3s::service::S3Service as Service<HttpRequest<B>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Service::<HttpRequest<B>>::poll_ready(&mut self.0, cx)
    }

    fn call(&mut self, request: HttpRequest<B>) -> Self::Future {
        self.1.fetch_add(1, Ordering::SeqCst);
        Service::call(&mut self.0, request)
    }
}

type Edge = crate::server::stack::S3StackService<Recorded, GatewayEdgeService>;

/// The gateway selection over `router`, with a legacy service that records every call.
fn edge(
    slot: &Arc<ServerContextSlot>,
    legacy: s3s::service::S3Service,
    router: S3Router<AdminOperation>,
) -> (Edge, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let gateway =
        || Ok::<_, std::convert::Infallible>(GatewayEdgeService(GatewayStack::assemble(assembly(slot.clone())).unwrap()));
    let service = select_stack(config::S3Stack::Gateway, Recorded(legacy, Arc::clone(&calls)), gateway)
        .unwrap()
        .with_native_admin(Some(GatewayEdgeService(GatewayStack::assemble_admin(assembly(slot.clone())).unwrap())))
        .with_control_plane(Some(GatewayEdgeService(
            GatewayStack::assemble_control_plane(assembly(slot.clone()), Arc::new(router)).unwrap(),
        )));
    (service, calls)
}

fn anonymous(method: Method, target: &str, content_type: Option<&str>, body: &'static str) -> HttpRequest<Full<Bytes>> {
    let mut builder = HttpRequest::builder()
        .method(method)
        .uri(target)
        .header("host", HOST)
        .header("x-request-id", REQUEST_ID);
    if let Some(content_type) = content_type {
        builder = builder.header(http::header::CONTENT_TYPE, content_type);
    }
    builder.body(Full::new(Bytes::from_static(body.as_bytes()))).unwrap()
}

fn signed_as(method: Method, path: &str, query: &str, key: &str, secret: &str, form: Signed) -> HttpRequest<Empty<Bytes>> {
    let mut headers = HeaderMap::new();
    headers.insert(http::header::HOST, HOST.parse().unwrap());
    headers.insert("x-request-id", REQUEST_ID.parse().unwrap());
    signed_request_with_headers(form, method, path, query, key, secret, headers)
}

/// One request class: how to build it, and what of the two answers must agree.
enum Compare {
    /// Status, content type and the whole body.
    Bytes,
    /// Status, and the JSON documents the two bodies hold.
    Json,
    /// Status, and the error code and message.
    Refusal,
}

#[tokio::test]
#[serial_test::serial]
async fn every_control_plane_class_answers_as_legacy_and_never_reaches_it() {
    let (_env, slot, legacy) = admin_fixture().await;
    let router = crate::admin::make_admin_router(false, slot.clone()).unwrap();
    let (edge, legacy_calls) = edge(&slot, legacy.clone(), router);
    let mut failures = Vec::new();

    type Build = Box<dyn Fn() -> HttpRequest<Full<Bytes>>>;
    let to_full = |request: HttpRequest<Empty<Bytes>>| request.map(|_| Full::new(Bytes::new()));
    let cases: Vec<(&str, Build, Compare)> = vec![
        (
            "signed admin GET",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/rustfs/admin/v3/list-users",
                    "",
                    ROOT,
                    ROOT_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Json,
        ),
        (
            "presigned admin GET",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/rustfs/admin/v3/list-users",
                    "",
                    ROOT,
                    ROOT_SECRET,
                    Signed::Query,
                ))
            }),
            Compare::Json,
        ),
        (
            "user without the admin action",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/rustfs/admin/v3/list-users",
                    "",
                    "info-no-policy",
                    USER_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Bytes,
        ),
        (
            "disabled user",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/rustfs/admin/v3/list-users",
                    "",
                    "info-disabled",
                    USER_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Refusal,
        ),
        (
            "wrong secret",
            Box::new(move || to_full(signed_as(Method::GET, "/rustfs/admin/v3/list-users", "", ROOT, "wrong", Signed::Header))),
            Compare::Refusal,
        ),
        (
            "anonymous admin GET",
            Box::new(|| anonymous(Method::GET, "/rustfs/admin/v3/list-users", None, "")),
            Compare::Bytes,
        ),
        (
            "anonymous MinIO alias PUT",
            Box::new(|| anonymous(Method::PUT, "/minio/admin/v3/add-user?accessKey=a", None, "{}")),
            Compare::Bytes,
        ),
        (
            "anonymous profiling trigger",
            Box::new(|| anonymous(Method::GET, "/profile/cpu", None, "")),
            Compare::Bytes,
        ),
        (
            "unknown v3 admin path",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/rustfs/admin/v3/no-such-route",
                    "",
                    ROOT,
                    ROOT_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Bytes,
        ),
        (
            "unknown v4 admin path",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/minio/admin/v4/no-such-route",
                    "",
                    ROOT,
                    ROOT_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Bytes,
        ),
        (
            "anonymous OIDC provider list",
            Box::new(|| anonymous(Method::GET, "/rustfs/admin/v3/oidc/providers", None, "")),
            Compare::Json,
        ),
        (
            "zip download with a forged token",
            Box::new(|| anonymous(Method::GET, "/rustfs/admin/v3/object-zip-downloads/id.zip?token=forged", None, "")),
            Compare::Refusal,
        ),
        (
            "anonymous STS form",
            Box::new(|| {
                anonymous(
                    Method::POST,
                    "/",
                    Some("application/x-www-form-urlencoded"),
                    "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&WebIdentityToken=forged",
                )
            }),
            Compare::Refusal,
        ),
        (
            "anonymous STS AssumeRole",
            Box::new(|| {
                anonymous(
                    Method::POST,
                    "/",
                    Some("application/x-www-form-urlencoded"),
                    "Action=AssumeRole&Version=2011-06-15",
                )
            }),
            Compare::Refusal,
        ),
        (
            "signed replication extension",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/missing-bucket",
                    "replication-metrics=",
                    ROOT,
                    ROOT_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Refusal,
        ),
        (
            "signed replication reset",
            Box::new(move || {
                to_full(signed_as(
                    Method::PUT,
                    "/missing-bucket",
                    "replication-reset=",
                    ROOT,
                    ROOT_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Refusal,
        ),
        (
            "signed replication check",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/missing-bucket",
                    "replication-check=",
                    ROOT,
                    ROOT_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Refusal,
        ),
        (
            "signed object lambda",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/missing-bucket/key",
                    "lambdaArn=arn%3Arustfs%3As3-object-lambda%3A%3Ax%3Awebhook",
                    ROOT,
                    ROOT_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Refusal,
        ),
        (
            "object lambda on an admin path",
            Box::new(move || {
                to_full(signed_as(
                    Method::GET,
                    "/rustfs/admin/v3/list-users",
                    "lambdaArn=",
                    ROOT,
                    ROOT_SECRET,
                    Signed::Header,
                ))
            }),
            Compare::Refusal,
        ),
    ];

    for (label, request, compare) in &cases {
        let old = answer(legacy.clone(), request()).await;
        let new = answer(edge.clone(), request()).await;
        eprintln!(
            "control-plane {label}: legacy={} {:?} {:?} gateway={} {:?} {:?}",
            old.0,
            old.1.get(http::header::CONTENT_TYPE),
            String::from_utf8_lossy(&old.2),
            new.0,
            new.1.get(http::header::CONTENT_TYPE),
            String::from_utf8_lossy(&new.2)
        );
        if old.0 != new.0 {
            failures.push(format!("{label}: status legacy={} gateway={}", old.0, new.0));
            continue;
        }
        let agrees = match compare {
            Compare::Bytes => {
                old.1.get(http::header::CONTENT_TYPE) == new.1.get(http::header::CONTENT_TYPE)
                    && snapshot_error_xml_or_bytes(&old.2) == snapshot_error_xml_or_bytes(&new.2)
            }
            Compare::Json => {
                serde_json::from_slice::<serde_json::Value>(&old.2).ok()
                    == serde_json::from_slice::<serde_json::Value>(&new.2).ok()
            }
            // The gateway's authenticator words the disabled-key refusal itself, as on the native routes.
            Compare::Refusal if *label == "disabled user" => {
                refusal(&old.2) == ("InvalidAccessKeyId".to_owned(), "check key failed".to_owned())
                    && refusal(&new.2)
                        == (
                            "InvalidAccessKeyId".to_owned(),
                            "The Access Key Id you provided does not exist in our records.".to_owned(),
                        )
            }
            Compare::Refusal => refusal(&old.2) == refusal(&new.2),
        };
        if !agrees {
            failures.push(format!("{label}: answers differ"));
        }
    }

    // A signed body reaches the handler whole: the policy it stores reads back the same.
    for (name, service) in [("p-legacy", None), ("p-gateway", Some(edge.clone()))] {
        let document = Bytes::from_static(br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::b/*"]}]}"#);
        let put = || {
            signed_body_request(
                Method::PUT,
                "/rustfs/admin/v3/add-canned-policy",
                &format!("name={name}"),
                ROOT,
                ROOT_SECRET,
                document.clone(),
            )
        };
        let stored = match &service {
            Some(edge) => answer(edge.clone(), put()).await,
            None => answer(legacy.clone(), put()).await,
        };
        let read = to_full(signed_as(
            Method::GET,
            "/rustfs/admin/v3/info-canned-policy",
            &format!("name={name}"),
            ROOT,
            ROOT_SECRET,
            Signed::Header,
        ));
        let info = answer(legacy.clone(), read).await;
        let policy = serde_json::from_slice::<serde_json::Value>(&info.2).ok();
        let statement = policy
            .as_ref()
            .map(|policy| policy["policy"]["Statement"][0]["Action"].clone());
        if stored.0 != StatusCode::OK || statement != Some(serde_json::json!(["s3:GetObject"])) {
            failures.push(format!("{name}: stored={} {:?} read={:?}", stored.0, refusal(&stored.2), policy));
        }
    }
    // A body whose digest the signature does not cover is refused before the handler stores it.
    let mut tampered = signed_body_request(
        Method::PUT,
        "/rustfs/admin/v3/add-canned-policy",
        "name=p-tampered",
        ROOT,
        ROOT_SECRET,
        Bytes::from_static(b"{}"),
    );
    *tampered.body_mut() = Full::new(Bytes::from_static(b"[]"));
    let refused = answer(edge.clone(), tampered).await;
    let read = signed_as(
        Method::GET,
        "/rustfs/admin/v3/info-canned-policy",
        "name=p-tampered",
        ROOT,
        ROOT_SECRET,
        Signed::Header,
    );
    if refused.0.is_success() || answer(legacy.clone(), read).await.0.is_success() {
        failures.push(format!("tampered body: {} {:?}", refused.0, refusal(&refused.2)));
    }

    assert_eq!(
        legacy_calls.load(Ordering::SeqCst),
        0,
        "a gateway-stack request reached the legacy service"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn snapshot_error_xml_or_bytes(body: &[u8]) -> Vec<u8> {
    match std::str::from_utf8(body) {
        Ok(text) if text.starts_with("<?xml") => snapshot_error_xml(body).into_bytes(),
        _ => body.to_vec(),
    }
}

/// What the probe handler saw of the caller.
static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Probe;

#[async_trait::async_trait]
impl OwnedOperation for Probe {
    async fn call(&self, req: AdminRequest, _params: matchit::Params<'_, '_>) -> rustfs_s3_types::S3Result<AdminResponse> {
        let credentials = req.credentials();
        let seen = format!(
            "{} key={:?} secret={:?} token={:?} region={:?} service={:?} sigv4={} presigned={}",
            req.method(),
            credentials.map(|credentials| credentials.access_key()),
            credentials.and_then(|credentials| credentials.secret_key()),
            credentials.and_then(|credentials| credentials.session_token()),
            req.region(),
            req.service(),
            req.envelope().verified_sigv4().is_some(),
            req.envelope().verified_presigned().is_some(),
        );
        SEEN.lock().unwrap().push(seen);
        Ok(AdminResponse::new(StatusCode::OK, rustfs_s3_types::Body::empty()))
    }
}

static PROBE: Probe = Probe;

fn probe_router(slot: &Arc<ServerContextSlot>) -> S3Router<AdminOperation> {
    let mut router = S3Router::new(false);
    router.set_server_ctx(slot.clone());
    router
        .insert(Method::GET, "/rustfs/admin/v3/probe", AdminOperation(&PROBE))
        .unwrap();
    router
        .insert(Method::PUT, "/rustfs/admin/v3/probe", AdminOperation(&PROBE))
        .unwrap();
    router
}

#[tokio::test]
#[serial_test::serial]
async fn the_handler_sees_the_caller_the_legacy_stack_hands_it() {
    let (_env, slot, _) = admin_fixture().await;
    let mut legacy = s3s::service::S3ServiceBuilder::new(UnusedS3);
    legacy.set_auth(crate::auth::IAMAuth::with_server_context(ROOT, ROOT_SECRET, slot.clone()));
    legacy.set_route(probe_router(&slot));
    let legacy = legacy.build();
    let (edge, legacy_calls) = edge(&slot, legacy.clone(), probe_router(&slot));

    let requests: Vec<Box<dyn Fn() -> HttpRequest<Full<Bytes>>>> = vec![
        Box::new(|| {
            signed_as(Method::GET, "/rustfs/admin/v3/probe", "", ROOT, ROOT_SECRET, Signed::Header).map(|_| Full::default())
        }),
        Box::new(|| {
            signed_as(Method::GET, "/rustfs/admin/v3/probe", "", "info-allowed", USER_SECRET, Signed::Query)
                .map(|_| Full::default())
        }),
        Box::new(|| {
            signed_body_request(Method::PUT, "/rustfs/admin/v3/probe", "", ROOT, ROOT_SECRET, Bytes::from_static(b"body"))
        }),
        Box::new(|| {
            signed_body_request(
                Method::PUT,
                "/rustfs/admin/v3/probe",
                "",
                "info-no-policy",
                USER_SECRET,
                Bytes::from_static(b"body"),
            )
        }),
    ];
    let mut seen = Vec::new();
    for (index, request) in requests.iter().enumerate() {
        for service in [None, Some(edge.clone())] {
            SEEN.lock().unwrap().clear();
            let gateway = service.is_some();
            let response = match service {
                Some(edge) => answer(edge, request()).await,
                None => answer(legacy.clone(), request()).await,
            };
            assert_eq!(response.0, StatusCode::OK, "request {index} gateway={gateway} {:?}", refusal(&response.2));
            seen.push(SEEN.lock().unwrap().clone());
        }
    }
    for pair in seen.chunks(2) {
        assert_eq!(pair[0].len(), 1, "{pair:?}");
        assert_eq!(pair[0], pair[1], "the gateway handed the handler a different caller");
    }
    assert_eq!(legacy_calls.load(Ordering::SeqCst), 0);
}
