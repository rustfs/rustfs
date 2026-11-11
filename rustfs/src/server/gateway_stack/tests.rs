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
//! The assembled gateway stack, driven through [`GatewayStack::call`] exactly as `server/http.rs`
//! drives it. Requests that need object storage are in `rustfs/tests/embedded_gateway_stack_test.rs`.

use super::authorizer::AUTHORIZATION_TABLE;
use super::*;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use http_body_util::{BodyExt, Empty};
use rustfs_gateway::{AuthzRequest, ClientFacts, Decision, InputAuthzRequest, InputDecisions, NO_DEADLINE};
use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::time::Instant;

const ROOT_ACCESS_KEY: &str = "rustfsgatewayroot";
const ROOT_SECRET_KEY: &str = "rustfs-gateway-root-secret";
const REQUEST_ID: &str = "7c9e6679-7425-40de-944b-e07fc1f90ae7";
const PEER: ([u8; 4], u16) = ([10, 0, 0, 7], 41000);
const CLIENT: [u8; 4] = [203, 0, 113, 9];
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn assembly() -> GatewayAssembly {
    GatewayAssembly {
        server_ctx: ServerContextSlot::new(),
        root_access_key: ROOT_ACCESS_KEY.to_owned(),
        root_secret_key: ROOT_SECRET_KEY.to_owned(),
        region: Some("us-east-1".to_owned()),
        server_domains: Vec::new(),
        body_read_timeout: Duration::from_secs(300),
    }
}

/// An IAM whose readiness and answer are scripted.
struct ScriptedIam {
    ready: bool,
}

impl IamKeyLookup for ScriptedIam {
    fn is_ready(&self) -> bool {
        self.ready
    }

    fn check_key<'a>(&'a self, _access_key: &'a str) -> BoxFuture<'a, Result<(Option<UserIdentity>, bool), IamError>> {
        Box::pin(async { Ok((None, false)) })
    }
}

/// The stack as production assembles it, over an IAM that knows no key but the root's.
fn stack_over(iam: ScriptedIam) -> GatewayStack {
    let authorizer = RustfsAuthorizer::new(AUTHORIZATION_TABLE, Arc::new(UnwiredDecisionCore));
    GatewayStack::assemble_with(assembly(), Arc::new(iam), authorizer).expect("the stack assembles")
}

fn request(method: Method, target: &str) -> http::request::Builder {
    Request::builder().method(method).uri(target).header("host", "localhost")
}

/// A well-formed SigV4 header signature for `access_key` whose signature is not the right one.
fn signed_with_a_wrong_signature(builder: http::request::Builder, access_key: &str) -> http::request::Builder {
    let now = jiff::Timestamp::now().strftime("%Y%m%dT%H%M%SZ").to_string();
    let date = &now[..8];
    builder
        .header("x-amz-date", now.as_str())
        .header("x-amz-content-sha256", EMPTY_SHA256)
        .header(
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={access_key}/{date}/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={}",
                "0".repeat(64)
            ),
        )
}

async fn answer(stack: &GatewayStack, builder: http::request::Builder) -> (StatusCode, HeaderMap, String) {
    let response = stack.call(builder.body(Empty::<Bytes>::new()).expect("static request")).await;
    let (parts, body) = response.into_parts();
    let body = body.collect().await.expect("the answer is read").to_bytes();
    (parts.status, parts.headers, String::from_utf8_lossy(&body).into_owned())
}

fn rustfs_request_context(request_id: &str) -> RequestContext {
    RequestContext {
        request_id: request_id.to_owned(),
        x_amz_request_id: request_id.to_owned(),
        trace_id: None,
        span_id: None,
        start_time: Instant::now(),
    }
}

#[test]
fn every_registered_operation_has_an_authorization_row_and_every_row_an_operation() {
    let stack = stack_over(ScriptedIam { ready: true });

    let registered: BTreeSet<&str> = stack.registered_operations().into_iter().collect();
    let mapped: BTreeSet<&str> = AUTHORIZATION_TABLE.iter().map(|row| row.operation).collect();

    assert_eq!(registered, mapped);
    assert_eq!(registered, BTreeSet::from(["ListBuckets"]), "the walking skeleton serves one operation");
}

#[test]
fn production_assembly_registers_what_the_test_assembly_does() {
    let stack = GatewayStack::assemble(assembly()).expect("the stack assembles");

    assert_eq!(stack.registered_operations(), ["ListBuckets"]);
}

#[test]
fn an_unusable_region_fails_the_assembly() {
    let refused = GatewayStack::assemble(GatewayAssembly {
        region: Some("Not A Region".to_owned()),
        ..assembly()
    });

    assert!(matches!(refused, Err(GatewayAssemblyError::Region { .. })), "{:?}", refused.err());
}

#[test]
fn an_unusable_root_access_key_fails_the_assembly_without_naming_the_secret() {
    let refused = GatewayStack::assemble(GatewayAssembly {
        root_access_key: String::new(),
        ..assembly()
    });

    let error = refused.err().expect("an empty access key verifies nothing");
    assert!(matches!(error, GatewayAssemblyError::RootCredentials(_)), "{error:?}");
    for rendered in [error.to_string(), format!("{error:?}")] {
        assert!(!rendered.contains(ROOT_SECRET_KEY), "{rendered}");
    }
}

#[test]
fn no_handler_runs_under_a_total_deadline_and_the_body_keeps_the_rustfs_idle_bound() {
    let config = service_config(Duration::from_secs(300));

    assert_eq!(config.handler_deadline(HandlerDeadlineClass::Standard), NO_DEADLINE);
    assert_eq!(config.handler_deadline(HandlerDeadlineClass::Extended), NO_DEADLINE);
    assert_eq!(config.commit_progress_deadline(), NO_DEADLINE);
    assert_eq!(config.request_body_deadlines().read_idle(), Duration::from_secs(300));
    assert_eq!(config.request_body_deadlines().first_byte(), Duration::from_secs(300));
    assert_eq!(config.request_body_deadlines().throughput_window(), NO_DEADLINE);
}

#[test]
fn a_disabled_body_read_timeout_lifts_the_idle_bound_instead_of_arming_zero() {
    let deadlines = body_deadlines(Duration::ZERO);

    assert_eq!(deadlines.read_idle(), NO_DEADLINE);
    assert_eq!(deadlines.first_byte(), NO_DEADLINE);
}

#[test]
fn host_facts_come_from_what_rustfs_layers_attached() {
    let peer = SocketAddr::from(PEER);
    let mut extensions = Extensions::new();
    extensions.insert(Some(RemoteAddr(peer)));
    extensions.insert(ClientInfo::direct(SocketAddr::new(IpAddr::from(CLIENT), 443)));
    extensions.insert(NativeTlsTransport(true));
    extensions.insert(rustfs_request_context(REQUEST_ID));

    attach_host_facts(&mut extensions);

    assert_eq!(extensions.get::<TransportSecurity>(), Some(&TransportSecurity::Encrypted));
    assert_eq!(
        extensions.get::<ClientAddr>().map(|address| address.ip()),
        Some(IpAddr::from(CLIENT)),
        "the trusted-proxy-corrected address wins over the socket peer"
    );
    assert_eq!(
        extensions
            .get::<HostRequestId>()
            .map(|id| id.request_id().as_str().to_owned()),
        Some(REQUEST_ID.to_owned())
    );
}

#[test]
fn host_facts_never_lean_towards_secure_or_invent_an_address() {
    let mut bare = Extensions::new();
    attach_host_facts(&mut bare);
    assert_eq!(bare.get::<TransportSecurity>(), Some(&TransportSecurity::Plaintext));
    assert!(bare.get::<ClientAddr>().is_none(), "no address is known");
    assert!(bare.get::<HostRequestId>().is_none(), "no identifier is known");

    let mut plaintext = Extensions::new();
    plaintext.insert(Some(RemoteAddr(SocketAddr::from(PEER))));
    plaintext.insert(NativeTlsTransport(false));
    plaintext.insert(rustfs_request_context("not a request id"));
    attach_host_facts(&mut plaintext);
    assert_eq!(plaintext.get::<TransportSecurity>(), Some(&TransportSecurity::Plaintext));
    assert_eq!(
        plaintext.get::<ClientAddr>().map(|address| address.ip()),
        Some(IpAddr::from(PEER.0)),
        "without a proxy the socket peer is the client"
    );
    assert!(
        plaintext.get::<HostRequestId>().is_none(),
        "an identifier outside the alphabet is not handed over"
    );

    let mut peerless = Extensions::new();
    peerless.insert(None::<RemoteAddr>);
    attach_host_facts(&mut peerless);
    assert!(peerless.get::<ClientAddr>().is_none());
}

/// Records the client facts the gateway hands its authorizer, and refuses.
struct RecordingAuthorizer {
    seen: Arc<Mutex<Option<Option<ClientFacts>>>>,
}

impl Authorizer for RecordingAuthorizer {
    fn authorize_route<'a>(
        &'a self,
        context: &'a rustfs_gateway::RequestContext<'a>,
        _request: &'a AuthzRequest<'a>,
    ) -> BoxFuture<'a, Decision> {
        *self.seen.lock().expect("recording lock") = Some(context.client().copied());
        Box::pin(async { Decision::Deny })
    }

    fn authorize_input<'a>(
        &'a self,
        _context: &'a rustfs_gateway::RequestContext<'a>,
        request: &'a InputAuthzRequest<'a>,
    ) -> BoxFuture<'a, InputDecisions> {
        let decisions = request.decide_all(Decision::Deny, |_| Decision::Deny);
        Box::pin(async move { decisions })
    }
}

#[tokio::test]
async fn the_authorizer_sees_the_client_facts_the_host_attached() {
    let seen = Arc::new(Mutex::new(None));
    let authorizer = RecordingAuthorizer { seen: Arc::clone(&seen) };
    let stack = GatewayStack::assemble_with(assembly(), Arc::new(ScriptedIam { ready: true }), authorizer).expect("assembles");
    let proxied = request(Method::GET, "/")
        .extension(Some(RemoteAddr(SocketAddr::from(PEER))))
        .extension(ClientInfo::direct(SocketAddr::new(IpAddr::from(CLIENT), 443)))
        .extension(NativeTlsTransport(true));

    let (status, _, _) = answer(&stack, proxied).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    let facts = seen
        .lock()
        .expect("recording lock")
        .take()
        .expect("the authorizer was asked")
        .expect("the host attached client facts");
    assert!(facts.transport_secure, "the listener terminated TLS");
    assert_eq!(facts.client_ip, Some(IpAddr::from(CLIENT)));
    assert_eq!(facts.source_ip(), Some(IpAddr::from(CLIENT)));
}

#[tokio::test]
async fn the_authorizer_sees_a_cleartext_transport_as_cleartext() {
    let seen = Arc::new(Mutex::new(None));
    let authorizer = RecordingAuthorizer { seen: Arc::clone(&seen) };
    let stack = GatewayStack::assemble_with(assembly(), Arc::new(ScriptedIam { ready: true }), authorizer).expect("assembles");

    let (status, _, _) = answer(&stack, request(Method::GET, "/").extension(NativeTlsTransport(false))).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    let facts = seen
        .lock()
        .expect("recording lock")
        .take()
        .expect("the authorizer was asked")
        .expect("the transport declaration alone makes the facts present");
    assert!(!facts.transport_secure);
    assert_eq!(facts.client_ip, None);
}

#[tokio::test]
async fn an_anonymous_list_buckets_is_denied() {
    let stack = stack_over(ScriptedIam { ready: true });

    let (status, _, body) = answer(&stack, request(Method::GET, "/")).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}

#[tokio::test]
async fn an_unregistered_operation_is_not_implemented() {
    let stack = stack_over(ScriptedIam { ready: true });

    for (method, target) in [
        (Method::GET, "/bucket/key"),
        (Method::PUT, "/bucket"),
        (Method::DELETE, "/bucket/key"),
    ] {
        let (status, _, body) = answer(&stack, request(method.clone(), target)).await;

        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{method} {target}: {body}");
        assert!(body.contains("<Code>NotImplemented</Code>"), "{method} {target}: {body}");
    }
}

#[tokio::test]
async fn a_wrong_signature_is_a_signature_mismatch() {
    let stack = stack_over(ScriptedIam { ready: true });

    let (status, _, body) = answer(&stack, signed_with_a_wrong_signature(request(Method::GET, "/"), ROOT_ACCESS_KEY)).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"), "{body}");
    assert!(!body.contains(ROOT_SECRET_KEY), "{body}");
}

#[tokio::test]
async fn an_unknown_access_key_is_refused_as_the_caller_s_fault() {
    let stack = stack_over(ScriptedIam { ready: true });

    let (status, _, body) = answer(&stack, signed_with_a_wrong_signature(request(Method::GET, "/"), "AKIDUNKNOWN0001")).await;

    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(body.contains("<Code>InvalidAccessKeyId</Code>"), "{body}");
}

#[tokio::test]
async fn an_unavailable_iam_is_a_server_fault_and_never_an_answer() {
    let stack = stack_over(ScriptedIam { ready: false });

    let (status, _, body) = answer(&stack, signed_with_a_wrong_signature(request(Method::GET, "/"), "AKIDUSER0001")).await;

    assert!(status.is_server_error(), "{status}: {body}");
    assert!(!body.contains("ListAllMyBucketsResult"), "{body}");
}

#[tokio::test]
async fn the_root_key_verifies_while_iam_is_unavailable() {
    let stack = stack_over(ScriptedIam { ready: false });

    let (status, _, body) = answer(&stack, signed_with_a_wrong_signature(request(Method::GET, "/"), ROOT_ACCESS_KEY)).await;

    assert_eq!(status, StatusCode::FORBIDDEN, "the root secret is known without IAM: {body}");
    assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"), "{body}");
}

#[tokio::test]
async fn the_answer_carries_the_request_identifier_rustfs_assigned() {
    let stack = stack_over(ScriptedIam { ready: true });

    let (_, headers, _) = answer(&stack, request(Method::GET, "/").extension(rustfs_request_context(REQUEST_ID))).await;

    assert_eq!(headers.get("x-amz-request-id").and_then(|value| value.to_str().ok()), Some(REQUEST_ID));
    assert_eq!(headers.get("x-request-id").and_then(|value| value.to_str().ok()), Some(REQUEST_ID));
}

#[tokio::test]
async fn without_a_rustfs_identifier_the_gateway_mints_its_own() {
    let stack = stack_over(ScriptedIam { ready: true });

    let (_, headers, _) = answer(&stack, request(Method::GET, "/")).await;

    let minted = headers
        .get("x-amz-request-id")
        .and_then(|value| value.to_str().ok())
        .expect("every answer names its request");
    assert!(!minted.is_empty());
    assert_ne!(minted, REQUEST_ID);
}

#[tokio::test]
async fn the_gateway_writes_no_cors_header() {
    let stack = stack_over(ScriptedIam { ready: true });
    let browser = |builder: http::request::Builder| builder.header("origin", "https://app.example.test");

    for (method, target) in [(Method::GET, "/"), (Method::GET, "/bucket/key"), (Method::PUT, "/bucket/key")] {
        let (_, headers, _) = answer(&stack, browser(request(method.clone(), target))).await;

        let cors: Vec<&str> = headers
            .keys()
            .map(http::HeaderName::as_str)
            .filter(|name| name.starts_with("access-control-"))
            .collect();
        assert!(cors.is_empty(), "{method} {target}: {cors:?}");
    }
}
