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

mod ssec;

use super::*;
use crate::server::gateway_handlers::rows;
use crate::server::gateway_handlers::test_support::{CLIENT, PEER, REQUEST_ID, Signed, request_context, signed_request};
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use http_body_util::{BodyExt, Empty};
use rustfs_gateway::{AuthzRequest, ClientFacts, HostRequestId, InputAuthzRequest, InputDecisions, NO_DEADLINE};
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;

const ROOT_ACCESS_KEY: &str = "rustfsgatewayroot";
const ROOT_SECRET_KEY: &str = "rustfs-gateway-root-secret";
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn assembly() -> GatewayAssembly {
    GatewayAssembly {
        server_ctx: ServerContextSlot::new(),
        root_access_key: ROOT_ACCESS_KEY.to_owned(),
        root_secret_key: ROOT_SECRET_KEY.to_owned(),
        region: Some("us-east-1".to_owned()),
        server_domains: Vec::new(),
        body_read_timeout: Duration::from_secs(300),
        keystone_enabled: false,
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

/// A settable live root, standing in for the server's runtime credentials.
#[derive(Clone)]
struct LiveRoot(Arc<Mutex<Option<Arc<RootCredentials>>>>);

impl LiveRoot {
    fn set(&self, root: RootCredentials) {
        *self.0.lock().expect("root lock") = Some(Arc::new(root));
    }

    fn clear(&self) {
        *self.0.lock().expect("root lock") = None;
    }

    fn source(&self) -> Arc<dyn RootSource> {
        let cell = Arc::clone(&self.0);
        Arc::new(move || cell.lock().expect("root lock").clone())
    }
}

/// A live root holding the test root.
fn root() -> LiveRoot {
    let root = RootCredentials::new(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, ROOT_SECRET_KEY).expect("a usable root");
    LiveRoot(Arc::new(Mutex::new(Some(Arc::new(root)))))
}

/// The stack as production assembles it, over an IAM that knows no key but the root's.
fn stack_over(iam: ScriptedIam) -> GatewayStack {
    GatewayStack::assemble_with(assembly(), Arc::new(iam), root().source(), early_gate(OPERATIONS)).expect("the stack assembles")
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
    answer_request(stack, builder.body(Empty::<Bytes>::new()).expect("static request")).await
}

async fn answer_request(stack: &GatewayStack, request: Request<Empty<Bytes>>) -> (StatusCode, HeaderMap, String) {
    let response = stack.call(request).await;
    let (parts, body) = response.into_parts();
    let body = body.collect().await.expect("the answer is read").to_bytes();
    (parts.status, parts.headers, String::from_utf8_lossy(&body).into_owned())
}

#[test]
fn the_assembled_stack_registers_exactly_the_table() {
    let stack = GatewayStack::assemble(assembly()).expect("the stack assembles");

    let registered: std::collections::BTreeSet<&str> = stack.service.operations().collect();
    assert_eq!(registered, rows(OPERATIONS).map(|row| row.name).collect());
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
    assert!(
        matches!(error, GatewayAssemblyError::RootCredentials(RootCredentialsError::InvalidAccessKey)),
        "{error:?}"
    );
    for rendered in [error.to_string(), format!("{error:?}")] {
        assert!(!rendered.contains(ROOT_SECRET_KEY), "{rendered}");
    }
}

#[test]
fn a_missing_or_short_root_secret_fails_the_assembly() {
    for weak in ["", "1234567"] {
        let refused = GatewayStack::assemble(GatewayAssembly {
            root_secret_key: weak.to_owned(),
            ..assembly()
        });

        assert!(
            matches!(
                refused.as_ref().err(),
                Some(GatewayAssemblyError::RootCredentials(RootCredentialsError::WeakSecretKey))
            ),
            "{} bytes",
            weak.len()
        );
    }
}

#[tokio::test]
async fn a_root_change_reaches_the_next_request_and_a_withdrawn_root_signs_nothing() {
    const ROTATED_ACCESS_KEY: &str = "rotatedgatewayroot";
    let live = root();
    let stack =
        GatewayStack::assemble_with(assembly(), Arc::new(ScriptedIam { ready: true }), live.source(), early_gate(OPERATIONS))
            .expect("the stack assembles");
    let signed_as = |access_key: &str| signed_with_a_wrong_signature(request(Method::GET, "/"), access_key);

    let (_, _, known) = answer(&stack, signed_as(ROOT_ACCESS_KEY)).await;
    assert!(known.contains("<Code>SignatureDoesNotMatch</Code>"), "the root key is known: {known}");

    // The root access key changes: the old one is now just an unknown key, and the new one is
    // known to the very next request.
    live.set(RootCredentials::new(ROTATED_ACCESS_KEY, ROOT_SECRET_KEY, ROOT_SECRET_KEY).expect("a usable root"));
    let (status, _, retired) = answer(&stack, signed_as(ROOT_ACCESS_KEY)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{retired}");
    assert!(retired.contains("<Code>InvalidAccessKeyId</Code>"), "{retired}");
    let (_, _, rotated) = answer(&stack, signed_as(ROTATED_ACCESS_KEY)).await;
    assert!(
        rotated.contains("<Code>SignatureDoesNotMatch</Code>"),
        "the new root key is known: {rotated}"
    );

    // Injected: a root source that returns nothing, so the provider returns an error for the
    // lookup. The key that was root a moment ago verifies nothing, and the fault is answered as
    // one, `500`, as the legacy stack answers a failed lookup. Ledger entry R-068
    // (rustfs/backlog#2684): the gateway's RustFS authenticator preset sets the switch of
    // rustfs/gateway#1585; the assembly sets nothing of its own.
    live.clear();
    let (status, _, withdrawn) = answer(&stack, signed_as(ROTATED_ACCESS_KEY)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{withdrawn}");
    assert!(withdrawn.contains("<Code>InternalError</Code>"), "{withdrawn}");
    assert!(!withdrawn.contains("ListAllMyBucketsResult"), "{withdrawn}");
    assert_fault_answer_names_nothing(&withdrawn, ROTATED_ACCESS_KEY);
}

#[test]
fn keystone_enabled_fails_the_assembly_and_names_both_settings() {
    let refused = GatewayStack::assemble(GatewayAssembly {
        keystone_enabled: true,
        ..assembly()
    });

    let error = refused.err().expect("the gateway stack is not served with Keystone enabled");
    assert!(matches!(error, GatewayAssemblyError::KeystoneEnabled), "{error:?}");
    let message = error.to_string();
    for named in ["RUSTFS_S3_STACK=gateway", "RUSTFS_KEYSTONE_ENABLE", "rustfs/gateway#1586"] {
        assert!(message.contains(named), "{named} is missing from: {message}");
    }
}

#[test]
fn keystone_is_read_from_configuration_and_an_unreadable_one_counts_as_enabled() {
    const ENABLE: &str = "RUSTFS_KEYSTONE_ENABLE";
    const AUTH_URL: &str = "RUSTFS_KEYSTONE_AUTH_URL";

    temp_env::with_vars([(ENABLE, None::<&str>), (AUTH_URL, None)], || {
        assert!(!keystone_enabled_in_configuration(), "unset is off");
    });
    temp_env::with_vars([(ENABLE, Some("false")), (AUTH_URL, None)], || {
        assert!(!keystone_enabled_in_configuration(), "false is off");
    });
    temp_env::with_vars([(ENABLE, Some("true")), (AUTH_URL, Some("http://keystone.example.test:5000"))], || {
        assert!(keystone_enabled_in_configuration(), "true is on");
    });
    temp_env::with_vars([(ENABLE, Some("true")), (AUTH_URL, None)], || {
        assert!(keystone_enabled_in_configuration(), "on but unreadable is still on");
    });
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
    extensions.insert(request_context(REQUEST_ID));

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
    plaintext.insert(request_context("not a request id"));
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

/// Records what the gateway hands its authorizer about the client, and refuses.
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
    let stack = GatewayStack::assemble_with(assembly(), Arc::new(ScriptedIam { ready: true }), root().source(), authorizer)
        .expect("assembles");
    let proxied = request(Method::GET, "/?prefix=probe")
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
    let stack = GatewayStack::assemble_with(assembly(), Arc::new(ScriptedIam { ready: true }), root().source(), authorizer)
        .expect("assembles");

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

/// The answer to a credential-store fault names neither the key that was looked up, nor a secret,
/// nor why the lookup failed.
fn assert_fault_answer_names_nothing(body: &str, access_key: &str) {
    assert!(!body.contains(access_key), "the looked-up key is not echoed: {body}");
    assert!(!body.contains(ROOT_SECRET_KEY), "{body}");
    let lowered = body.to_ascii_lowercase();
    for detail in ["iam", "credential", "lookup", "store", "not ready", "unavailable", "root"] {
        assert!(!lowered.contains(detail), "the answer says `{detail}`: {body}");
    }
}

/// Injected: a credential provider that returns an error for the lookup of a non-root key.
///
/// The fault is answered as one, `500 InternalError`, as the legacy stack answers a failed lookup,
/// and not as an unknown key. Up to gateway pin 21283e87 this test observed `403
/// InvalidAccessKeyId`; pin 7915d069 changed the answer. Ledger entry R-068 (rustfs/backlog#2684): the gateway's RustFS
/// authenticator preset sets the switch of rustfs/gateway#1585; the assembly sets nothing of its
/// own. Nothing is served either way.
#[tokio::test]
async fn a_credential_lookup_that_fails_is_refused_and_never_served() {
    let stack = stack_over(ScriptedIam { ready: false });

    let (status, _, body) = answer(&stack, signed_with_a_wrong_signature(request(Method::GET, "/"), "AKIDUSER0001")).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert!(body.contains("<Code>InternalError</Code>"), "{body}");
    assert!(!body.contains("InvalidAccessKeyId"), "a store fault is not an unknown key: {body}");
    assert_fault_answer_names_nothing(&body, "AKIDUSER0001");
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

    let (_, headers, _) = answer(&stack, request(Method::GET, "/").extension(request_context(REQUEST_ID))).await;

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
}

/// A correctly signed request for a served operation passes the early gate to its handler; the same
/// request is refused at the gate of a stack that serves nothing.
#[tokio::test]
async fn the_early_gate_allows_only_what_the_table_serves() {
    static NOTHING: OperationTable = &[];
    let request = || signed_request(Signed::Header, Method::GET, "/", "", ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
    let gated = |table| {
        GatewayStack::assemble_with(assembly(), Arc::new(ScriptedIam { ready: true }), root().source(), early_gate(table))
            .expect("the stack assembles")
    };

    let refused = gated(NOTHING).call(request()).await;
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);

    let (status, _, body) = answer_request(&gated(OPERATIONS), request()).await;
    assert_ne!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(!body.contains("AccessDenied"), "{body}");
}

#[tokio::test]
async fn the_reported_owner_is_every_bucket_s_owner_and_without_one_nothing_is() {
    let photos = BucketName::new("photos").expect("a bucket name");
    let reported = owner().id.expect("RustFS reports an owner id");

    assert_eq!(
        ReportedOwner(Some(Arc::from(reported.as_str())))
            .owner(&photos)
            .await
            .as_deref(),
        Ok(reported.as_str())
    );
    assert_eq!(ReportedOwner(None).owner(&photos).await, Err(BucketOwnerError::unavailable()));
}
