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

#[path = "admin/tests.rs"]
mod admin;
mod control_plane;
mod keystone;
mod legacy_layers;
mod reserved_console;
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

struct StalledBody(Arc<std::sync::atomic::AtomicBool>);

impl Drop for StalledBody {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl http_body::Body for StalledBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Pending
    }
}

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
        keystone_auth: None,
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
    let assembly = assembly();
    let server_ctx = Arc::clone(&assembly.server_ctx);
    GatewayStack::assemble_with(assembly, Arc::new(iam), root().source(), early_gate(OPERATIONS, server_ctx))
        .expect("the stack assembles")
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
    let table = rows(OPERATIONS).map(|row| row.name);
    assert_eq!(registered, table.chain(metadata_listing::NAMES).collect());
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
fn a_region_list_assembles_and_an_empty_entry_does_not() {
    let listed = GatewayStack::assemble(GatewayAssembly {
        region: Some("us-east-1, ap-southeast-2,us-west-2".to_owned()),
        ..assembly()
    });
    assert!(listed.is_ok(), "{:?}", listed.err());

    let refused = GatewayStack::assemble(GatewayAssembly {
        region: Some("us-east-1,".to_owned()),
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
    let assembly = assembly();
    let server_ctx = Arc::clone(&assembly.server_ctx);
    let stack = GatewayStack::assemble_with(
        assembly,
        Arc::new(ScriptedIam { ready: true }),
        live.source(),
        early_gate(OPERATIONS, server_ctx),
    )
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
fn keystone_enabled_requires_an_initialized_provider_and_names_both_settings() {
    temp_env::with_vars(
        [
            ("RUSTFS_KEYSTONE_ENABLE", Some("true")),
            ("RUSTFS_KEYSTONE_AUTH_URL", Some("http://keystone.example.test:5000")),
        ],
        || {
            let error = keystone_provider_in_configuration()
                .err()
                .expect("the provider must be initialized before assembly");
            assert!(matches!(error, GatewayAssemblyError::KeystoneNotInitialized));
            let message = error.to_string();
            for named in ["gateway stack", "RUSTFS_KEYSTONE_ENABLE"] {
                assert!(message.contains(named), "{named} is missing from: {message}");
            }
        },
    );
}

#[test]
fn keystone_is_read_from_configuration_and_an_unreadable_one_is_refused() {
    const ENABLE: &str = "RUSTFS_KEYSTONE_ENABLE";
    const AUTH_URL: &str = "RUSTFS_KEYSTONE_AUTH_URL";
    temp_env::with_vars([(ENABLE, None::<&str>), (AUTH_URL, None)], || {
        assert!(keystone_provider_in_configuration().unwrap().is_none());
    });
    temp_env::with_vars([(ENABLE, Some("false")), (AUTH_URL, None)], || {
        assert!(keystone_provider_in_configuration().unwrap().is_none());
    });
    temp_env::with_vars([(ENABLE, Some("true")), (AUTH_URL, None)], || {
        assert!(matches!(
            keystone_provider_in_configuration(),
            Err(GatewayAssemblyError::KeystoneConfig(_))
        ));
    });
}

#[test]
fn no_handler_runs_under_a_total_deadline_and_the_body_keeps_the_rustfs_idle_bound() {
    let config = service_config(Duration::from_secs(300), DEFAULT_MAX_BUFFERED_BODY_BYTES);

    assert_eq!(config.handler_deadline(HandlerDeadlineClass::Standard), NO_DEADLINE);
    assert_eq!(config.handler_deadline(HandlerDeadlineClass::Extended), NO_DEADLINE);
    assert_eq!(config.commit_progress_deadline(), NO_DEADLINE);
    assert_eq!(config.request_body_deadlines().read_idle(), Duration::from_secs(300));
    assert_eq!(config.request_body_deadlines().first_byte(), Duration::from_secs(300));
    assert_eq!(config.request_body_deadlines().throughput_window(), NO_DEADLINE);
    assert_eq!(config.upload_object_ceiling(), Some(MAX_SINGLE_PUT_OBJECT_SIZE));
}

#[test]
fn a_disabled_body_read_timeout_lifts_the_idle_bound_instead_of_arming_zero() {
    let deadlines = body_deadlines(Duration::ZERO);

    assert_eq!(deadlines.read_idle(), NO_DEADLINE);
    assert_eq!(deadlines.first_byte(), NO_DEADLINE);
}

/// The stack hands a body its answer left unread back to the host when it answers: dropping it is
/// what moves it into `EarlyResponseBodyService`, whose drain the HTTP/1-2 shutdown accounting
/// owns. A drain of the stack's own would hold it here, in a task nothing accounts for. The
/// configured idle bound of the host's drain is `server::http`'s
/// `gateway_rejected_http1_body_uses_the_configured_drain_timeout`.
#[tokio::test]
async fn rejected_http1_body_is_released_to_the_host_drain() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let assembly = assembly();
    let server_ctx = Arc::clone(&assembly.server_ctx);
    let stack = GatewayStack::assemble_with(
        assembly,
        Arc::new(ScriptedIam { ready: true }),
        root().source(),
        early_gate(OPERATIONS, server_ctx),
    )
    .expect("the stack assembles");
    let dropped = Arc::new(AtomicBool::new(false));
    let request = signed_with_a_wrong_signature(request(Method::PUT, "/test-bucket/test-key"), ROOT_ACCESS_KEY)
        .version(http::Version::HTTP_11)
        .body(StalledBody(Arc::clone(&dropped)))
        .expect("request");
    let response = stack.call(request).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        dropped.load(Ordering::SeqCst),
        "the stack must release the unread body to the host when it answers"
    );
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
        let assembly = assembly();
        let server_ctx = Arc::clone(&assembly.server_ctx);
        GatewayStack::assemble_with(
            assembly,
            Arc::new(ScriptedIam { ready: true }),
            root().source(),
            early_gate(table, server_ctx),
        )
        .expect("the stack assembles")
    };

    let refused = gated(NOTHING).call(request()).await;
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);

    let (status, _, body) = answer_request(&gated(OPERATIONS), request()).await;
    assert_ne!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(!body.contains("AccessDenied"), "{body}");

    // An empty server slot cannot evaluate GetBucketLocation. The core's store error is
    // indeterminate, so the gate refuses before the handler can answer InternalError.
    let location = signed_request(Signed::Header, Method::GET, "/photos", "location", ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
    let (status, _, body) = answer_request(&gated(OPERATIONS), location).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(body.contains("AccessDenied"), "{body}");
    assert!(!body.contains("InternalError"), "{body}");
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

struct CountingStalledBody {
    polled: Arc<std::sync::atomic::AtomicUsize>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for CountingStalledBody {
    fn drop(&mut self) {
        self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl http_body::Body for CountingStalledBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.polled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::task::Poll::Pending
    }
}

struct TableIam {
    users: std::collections::HashMap<String, (UserIdentity, bool)>,
}

impl IamKeyLookup for TableIam {
    fn is_ready(&self) -> bool {
        true
    }

    fn check_key<'a>(&'a self, access_key: &'a str) -> BoxFuture<'a, Result<(Option<UserIdentity>, bool), IamError>> {
        let answer = self
            .users
            .get(access_key)
            .cloned()
            .map_or((None, false), |(id, ok)| (Some(id), ok));
        Box::pin(async move { Ok(answer) })
    }
}

#[tokio::test]
async fn expired_sts_returns_400_expired_token_only_to_holder_and_keeps_disabled_403() {
    use crate::server::gateway_handlers::test_support::signed_request_with_headers;
    use rustfs_credentials::Credentials as IamCredential;

    const EXPIRED_STS_AK: &str = "AKIAEXPIREDSTS01";
    const EXPIRED_STS_SK: &str = "expired-sts-secret-key-01";
    const EXPIRED_STS_TOKEN: &str = "expired-sts-session-token-01";
    const DISABLED_STS_AK: &str = "AKIADISABLEDSTS1";
    const DISABLED_STS_SK: &str = "disabled-sts-secret-key-01";
    const DISABLED_STS_TOKEN: &str = "disabled-sts-session-token-01";
    const EXPIRED_SA_AK: &str = "AKIAEXPIREDSA001";
    const EXPIRED_SA_SK: &str = "expired-sa-secret-key-01";

    let past = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let mut users = std::collections::HashMap::new();
    users.insert(
        EXPIRED_STS_AK.to_owned(),
        (
            UserIdentity::new(IamCredential {
                access_key: EXPIRED_STS_AK.to_owned(),
                secret_key: EXPIRED_STS_SK.to_owned(),
                session_token: EXPIRED_STS_TOKEN.to_owned(),
                parent_user: "parent".to_owned(),
                status: "on".to_owned(),
                expiration: Some(past),
                ..Default::default()
            }),
            false,
        ),
    );
    users.insert(
        DISABLED_STS_AK.to_owned(),
        (
            UserIdentity::new(IamCredential {
                access_key: DISABLED_STS_AK.to_owned(),
                secret_key: DISABLED_STS_SK.to_owned(),
                session_token: DISABLED_STS_TOKEN.to_owned(),
                parent_user: "parent".to_owned(),
                status: "off".to_owned(),
                expiration: Some(past),
                ..Default::default()
            }),
            false,
        ),
    );
    users.insert(
        EXPIRED_SA_AK.to_owned(),
        (
            UserIdentity::new(IamCredential {
                access_key: EXPIRED_SA_AK.to_owned(),
                secret_key: EXPIRED_SA_SK.to_owned(),
                session_token: "sa-internal-claims-token".to_owned(),
                parent_user: "parent".to_owned(),
                status: "on".to_owned(),
                expiration: Some(past),
                claims: Some([(rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_owned(), "inherited-policy".into())].into()),
                ..Default::default()
            }),
            false,
        ),
    );

    let assembly = assembly();
    let server_ctx = Arc::clone(&assembly.server_ctx);
    let stack = GatewayStack::assemble_with(
        assembly,
        Arc::new(TableIam { users }),
        root().source(),
        early_gate(OPERATIONS, server_ctx),
    )
    .expect("stack assembles");

    let sign_with_token = |ak: &str, sk: &str, token: Option<&str>| {
        let mut headers = HeaderMap::new();
        if let Some(token) = token {
            headers.insert("x-amz-security-token", token.parse().expect("token header"));
        }
        signed_request_with_headers(Signed::Header, Method::GET, "/", "", ak, sk, headers)
    };

    // Holder proves possession with matching secret and token -> 400 ExpiredToken.
    let (status, _, body) =
        answer_request(&stack, sign_with_token(EXPIRED_STS_AK, EXPIRED_STS_SK, Some(EXPIRED_STS_TOKEN))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("<Code>ExpiredToken</Code>"), "{body}");
    assert!(!body.contains("InvalidAccessKeyId"), "{body}");

    // Wrong secret, wrong token, or missing token must never leak ExpiredToken -> 403.
    for (label, req) in [
        (
            "wrong secret",
            sign_with_token(EXPIRED_STS_AK, "wrong-secret-key-01", Some(EXPIRED_STS_TOKEN)),
        ),
        ("wrong token", sign_with_token(EXPIRED_STS_AK, EXPIRED_STS_SK, Some("other-token"))),
        ("missing token", sign_with_token(EXPIRED_STS_AK, EXPIRED_STS_SK, None)),
    ] {
        let (status, _, body) = answer_request(&stack, req).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {body}");
        assert!(!body.contains("ExpiredToken"), "{label}: {body}");
    }

    // Disabled STS and expired service account stay disabled -> 403 InvalidAccessKeyId.
    for (label, req) in [
        (
            "disabled expired STS",
            sign_with_token(DISABLED_STS_AK, DISABLED_STS_SK, Some(DISABLED_STS_TOKEN)),
        ),
        ("expired service account", sign_with_token(EXPIRED_SA_AK, EXPIRED_SA_SK, None)),
    ] {
        let (status, _, body) = answer_request(&stack, req).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {body}");
        assert!(body.contains("<Code>InvalidAccessKeyId</Code>"), "{label}: {body}");
    }
}

#[tokio::test]
async fn delete_objects_fails_auth_before_body_read_and_verifies_digests_post_auth() {
    use crate::server::gateway_handlers::test_support::{signed_request_with_headers, signed_streaming_trailer_request};
    use md5::Digest as _;
    use rustfs_gateway::{Handler, HandlerResult, Req, Resp, dto};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const DELETE_XML: &str = "<Delete><Object><Key>safe-object</Key></Object></Delete>";

    struct DeleteProbe(Arc<AtomicUsize>);
    impl Handler<dto::DeleteObjects> for DeleteProbe {
        async fn call(&self, request: Req<dto::DeleteObjects>) -> HandlerResult<dto::DeleteObjects> {
            let keys: Vec<_> = request
                .resources()
                .resolve_objects(request.read_proof())
                .expect("DeleteObjects resources")
                .map(|object| object.key.as_str())
                .collect();
            assert_eq!(keys, ["safe-object"]);
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Resp::new(dto::DeleteObjectsOutput::default()))
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let assembly = assembly();
    let builder = GatewayStack::configured_builder(
        &assembly,
        Arc::new(ScriptedIam { ready: true }),
        root().source(),
        decide_with(|_| Decision::Allow),
        std::convert::identity,
    )
    .expect("configured builder");
    let stack = GatewayStack {
        service: builder
            .register::<dto::DeleteObjects, _>(Arc::new(DeleteProbe(Arc::clone(&calls))))
            .build()
            .expect("delete probe stack"),
        keystone_auth: None,
        control_plane: false,
        object_lambda_fallback: None,
    };

    // 1. Unauthenticated / wrongly-signed POST /bucket?delete fails auth before polling a byte.
    let polled = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let bad_auth_req = signed_with_a_wrong_signature(request(Method::POST, "/bucket?delete"), ROOT_ACCESS_KEY)
        .header(http::header::CONTENT_LENGTH, 20 * 1024 * 1024)
        .header("content-md5", "not-valid-base64")
        .body(CountingStalledBody {
            polled: Arc::clone(&polled),
            dropped: Arc::clone(&dropped),
        })
        .expect("bad auth delete request");
    let response = stack.call(bad_auth_req).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(polled.load(Ordering::SeqCst), 0, "auth refusal must not read body bytes");
    assert!(dropped.load(Ordering::SeqCst), "unread body must be released on auth refusal");
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // 2. Signed aws-chunked + x-amz-trailer DeleteObjects reaches gateway post-auth trailer
    // verification (no pre-auth MissingContentMD5 from ControlBodyDigestService). A tampered
    // trailer is rejected post-auth with 400 BadDigest; a valid trailer passes trailer checking
    // and currently hits gateway's rustfs_profile() refuse_unsized_buffered_bodies_as_legacy_rustfs
    // gate (400 IncompleteBody) until gateway drops that switch from rustfs_profile().
    let crc32 = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, DELETE_XML.as_bytes())
        .expect("crc32 checksum")
        .encoded;
    let chunked_request = |trailer_value: &str| {
        let wire = format!(
            "{:x}\r\n{DELETE_XML}\r\n0\r\nx-amz-checksum-crc32:{trailer_value}\r\n\r\n",
            DELETE_XML.len()
        );
        let signed = signed_streaming_trailer_request(
            Method::POST,
            "/bucket",
            "delete=",
            ROOT_ACCESS_KEY,
            ROOT_SECRET_KEY,
            DELETE_XML.len() as u64,
            "x-amz-checksum-crc32",
        );
        let wire_bytes = Bytes::from(wire);
        let mut req = signed.map(|_| http_body_util::Full::new(wire_bytes.clone()));
        req.headers_mut()
            .insert(http::header::CONTENT_LENGTH, http::HeaderValue::from(wire_bytes.len()));
        req
    };

    let tampered = stack.call(chunked_request("AAAAAA==")).await;
    let tampered_status = tampered.status();
    let tampered_body = tampered.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(tampered_status, StatusCode::BAD_REQUEST);
    assert!(
        String::from_utf8_lossy(&tampered_body).contains("<Code>BadDigest</Code>"),
        "{}",
        String::from_utf8_lossy(&tampered_body)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let valid_chunked = stack.call(chunked_request(&crc32)).await;
    let valid_chunked_status = valid_chunked.status();
    let valid_chunked_body = valid_chunked.into_body().collect().await.unwrap().to_bytes();
    let valid_chunked_text = String::from_utf8_lossy(&valid_chunked_body);
    assert!(
        !valid_chunked_text.contains("<Code>MissingContentMD5</Code>"),
        "pre-auth ControlBodyDigestService must not reject trailer DeleteObjects: {valid_chunked_text}"
    );
    assert!(
        valid_chunked_status == StatusCode::OK
            || (valid_chunked_status == StatusCode::BAD_REQUEST && valid_chunked_text.contains("<Code>IncompleteBody</Code>")),
        "{valid_chunked_status}: {valid_chunked_text}"
    );
    let baseline_calls = calls.load(Ordering::SeqCst);

    // 3. Supplied Content-MD5, head x-amz-checksum-*, and SigV4 without MD5/checksum are all
    // verified post-auth by gateway.
    let good_md5 = base64_simd::STANDARD.encode_to_string(md5::Md5::digest(DELETE_XML.as_bytes()));
    for (header_pair, payload, expected_status, expected_code) in [
        (Some(("content-md5", good_md5.as_str())), DELETE_XML, StatusCode::OK, None),
        (
            Some(("content-md5", good_md5.as_str())),
            "<Delete><Object><Key>evil-object</Key></Object></Delete>",
            StatusCode::BAD_REQUEST,
            Some("BadDigest"),
        ),
        (
            Some(("content-md5", "invalid-md5")),
            DELETE_XML,
            StatusCode::BAD_REQUEST,
            Some("InvalidDigest"),
        ),
        (Some(("x-amz-checksum-crc32", crc32.as_str())), DELETE_XML, StatusCode::OK, None),
        (
            Some(("x-amz-checksum-crc32", "AAAAAA==")),
            DELETE_XML,
            StatusCode::BAD_REQUEST,
            Some("BadDigest"),
        ),
        (None, DELETE_XML, StatusCode::OK, None),
    ] {
        let mut headers = HeaderMap::new();
        if let Some((name, value)) = header_pair {
            headers.insert(name, value.parse().unwrap());
        }
        let signed = signed_request_with_headers(
            Signed::Header,
            Method::POST,
            "/bucket",
            "delete=",
            ROOT_ACCESS_KEY,
            ROOT_SECRET_KEY,
            headers,
        );
        let payload_bytes = Bytes::from_static(payload.as_bytes());
        let mut req = signed.map(|_| http_body_util::Full::new(payload_bytes.clone()));
        req.headers_mut()
            .insert(http::header::CONTENT_LENGTH, http::HeaderValue::from(payload_bytes.len()));
        let resp = stack.call(req).await;
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8_lossy(&body);
        assert_eq!(status, expected_status, "{header_pair:?}: {text}");
        if let Some(code) = expected_code {
            assert!(text.contains(&format!("<Code>{code}</Code>")), "{header_pair:?}: {text}");
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), baseline_calls + 3);
}

#[tokio::test]
#[serial_test::serial]
async fn live_root_reads_strictly_from_server_context_and_ignores_global_signing_key() {
    // Seed a weak process-global secret if not yet set in this process (in nextest each test runs
    // in its own process, so `get_token_signing_key()` observes `"weak"`, which previously caused
    // `RootCredentials::new` to fail with `WeakSigningKey` and return `None`).
    let _ = rustfs_credentials::init_global_action_credentials(Some("global-weak-root".to_owned()), Some("weak".to_owned()));

    // An uninstalled ServerContextSlot must never fall back to global action credentials.
    let uninstalled = ServerContextSlot::new();
    assert!(
        live_root(uninstalled)().is_none(),
        "uninstalled ServerContextSlot must not fall back to process-global action credentials"
    );

    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    if crate::runtime_sources::current_app_context().is_none() {
        crate::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
    }
    let ambient = crate::runtime_sources::current_app_context().expect("ambient context");
    let context = Arc::new(crate::runtime_sources::AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
    const SLOT_AK: &str = "slotisolatedroot";
    const SLOT_SK: &str = "slot-isolated-root-secret";
    assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
        access_key: SLOT_AK.to_owned(),
        secret_key: SLOT_SK.to_owned(),
        status: "on".to_owned(),
        ..Default::default()
    }));
    let slot = ServerContextSlot::new();
    assert!(slot.install(context));

    let root = live_root(Arc::clone(&slot))()
        .expect("live_root must stay healthy despite a weak or unrelated process-global signing key");
    assert_eq!(root.access_key(), SLOT_AK);

    // End-to-end check: a GatewayStack assembled with `live_root(slot)` verifies the slot's root
    // secret (returning SignatureDoesNotMatch rather than 500 InternalError or InvalidAccessKeyId).
    let assembly = GatewayAssembly {
        server_ctx: Arc::clone(&slot),
        root_access_key: SLOT_AK.to_owned(),
        root_secret_key: SLOT_SK.to_owned(),
        ..assembly()
    };
    let stack = GatewayStack::assemble(assembly).expect("stack assembles");
    let (status, _, body) = answer(&stack, signed_with_a_wrong_signature(request(Method::GET, "/"), SLOT_AK)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"), "{body}");
}

#[tokio::test]
async fn upload_ceilings_admit_5_gib_framed_upload_and_refuse_above_5_gib_before_body_read() {
    use crate::server::gateway_handlers::test_support::signed_streaming_trailer_request;
    use rustfs_gateway::{Handler, HandlerError, HandlerResult, Req, dto};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct PutProbe(Arc<AtomicUsize>);
    impl Handler<dto::PutObject> for PutProbe {
        async fn call(&self, _request: Req<dto::PutObject>) -> HandlerResult<dto::PutObject> {
            self.0.fetch_add(1, Ordering::SeqCst);
            // Return an explicit sentinel error so the test verifies handler admission for a 5 GiB
            // declared upload without materializing 5 GiB of body bytes for BodyMonitor completion.
            Err(HandlerError::not_implemented("admitted-5gib-probe"))
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let assembly = assembly();
    let builder = GatewayStack::configured_builder(
        &assembly,
        Arc::new(ScriptedIam { ready: true }),
        root().source(),
        decide_with(|_| Decision::Allow),
        std::convert::identity,
    )
    .expect("configured builder");
    let stack = GatewayStack {
        service: builder
            .register::<dto::PutObject, _>(Arc::new(PutProbe(Arc::clone(&calls))))
            .build()
            .expect("put probe stack"),
        keystone_auth: None,
        control_plane: false,
        object_lambda_fallback: None,
    };

    let wire_limit = max_framed_upload_bytes(MAX_SINGLE_PUT_OBJECT_SIZE);
    assert_eq!(
        stack.service.limits().max_body_bytes,
        wire_limit,
        "wire limit must accommodate aws-chunked framing for a 5 GiB object"
    );
    assert!(stack.service.limits().max_body_bytes > MAX_SINGLE_PUT_OBJECT_SIZE);

    // 1. A 5 GiB + 1 byte decoded upload is refused with 400 EntityTooLarge before body read.
    let polled = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let mut oversized = signed_streaming_trailer_request(
        Method::PUT,
        "/bucket/oversized",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        MAX_SINGLE_PUT_OBJECT_SIZE + 1,
        "x-amz-checksum-crc32",
    )
    .map(|_| CountingStalledBody {
        polled: Arc::clone(&polled),
        dropped: Arc::clone(&dropped),
    });
    oversized.headers_mut().insert(
        http::header::CONTENT_LENGTH,
        (MAX_SINGLE_PUT_OBJECT_SIZE + 128).to_string().parse().unwrap(),
    );
    let resp = stack.call(oversized).await;
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", String::from_utf8_lossy(&body));
    assert!(
        String::from_utf8_lossy(&body).contains("<Code>EntityTooLarge</Code>"),
        "{}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(polled.load(Ordering::SeqCst), 0, "oversized upload must fail before reading body");
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // 2. A wire Content-Length above `max_framed_upload_bytes(5 GiB)` is refused before body read.
    let wire_polled = Arc::new(AtomicUsize::new(0));
    let wire_dropped = Arc::new(AtomicBool::new(false));
    let mut over_wire = signed_streaming_trailer_request(
        Method::PUT,
        "/bucket/over-wire",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        MAX_SINGLE_PUT_OBJECT_SIZE,
        "x-amz-checksum-crc32",
    )
    .map(|_| CountingStalledBody {
        polled: Arc::clone(&wire_polled),
        dropped: Arc::clone(&wire_dropped),
    });
    over_wire
        .headers_mut()
        .insert(http::header::CONTENT_LENGTH, (wire_limit + 1).to_string().parse().unwrap());
    let resp = stack.call(over_wire).await;
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(
        matches!(status, StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE),
        "{status}: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(wire_polled.load(Ordering::SeqCst), 0, "over-wire upload must fail before reading body");
    assert!(wire_dropped.load(Ordering::SeqCst));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // 3. A 5 GiB decoded aws-chunked upload whose framed wire Content-Length exceeds 5 GiB passes
    // both the wire limit and object ceiling gates and reaches the handler.
    let mut framed_5gib = signed_streaming_trailer_request(
        Method::PUT,
        "/bucket/five-gib",
        "",
        ROOT_ACCESS_KEY,
        ROOT_SECRET_KEY,
        MAX_SINGLE_PUT_OBJECT_SIZE,
        "x-amz-checksum-crc32",
    );
    framed_5gib.headers_mut().insert(
        http::header::CONTENT_LENGTH,
        (MAX_SINGLE_PUT_OBJECT_SIZE + 65_536).to_string().parse().unwrap(),
    );
    let resp = stack.call(framed_5gib).await;
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&body);
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{text}");
    assert!(text.contains("admitted-5gib-probe"), "{text}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn assemble_initializes_s3_and_list_objects_metrics() {
    const S3_OPS_METRIC: &str = "rustfs_s3_operations_total";
    const LIST_OBJECTS_GATHER_TOTAL: &str = "rustfs_s3_list_objects_gather_total";

    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let _stack = GatewayStack::assemble(assembly()).expect("stack assembles");
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::ListBuckets);
        metrics::counter!(LIST_OBJECTS_GATHER_TOTAL).increment(1);
    });
    let entries = snapshotter.snapshot().into_vec();
    let description_for = |name: &str| {
        entries
            .iter()
            .find(|(key, _, _, _)| key.key().name() == name)
            .and_then(|(_, _, desc, _)| desc.as_deref())
            .map(str::to_owned)
    };
    assert!(
        description_for(S3_OPS_METRIC).is_some(),
        "GatewayStack::assemble must initialize S3 operation metrics"
    );
    assert!(
        description_for(LIST_OBJECTS_GATHER_TOTAL).is_some(),
        "GatewayStack::assemble must initialize ListObjects metrics"
    );
}

/// Signs `method path?query` for `access_key`/`secret_key` with AWS SigV4a
/// (`AWS4-ECDSA-P256-SHA256`), either in the `Authorization` header or as a presigned query string.
fn sigv4a_request(
    form: Signed,
    method: Method,
    path: &str,
    query: &str,
    headers: HeaderMap,
    access_key: &str,
    secret_key: &str,
) -> Request<Empty<Bytes>> {
    use rustfs_gateway::sig::{AmzDate, PayloadMode, SigService, SigV4aSigner, SigningCredentials, SigningRequest};
    use rustfs_gateway::{Limits, WireRequest};

    let mut headers = headers;
    headers
        .entry(http::header::HOST)
        .or_insert(http::HeaderValue::from_static("localhost"));
    let probe = Request::builder()
        .method(method.clone())
        .uri(path)
        .header(http::header::HOST, headers.get(http::header::HOST).expect("a host"))
        .body(Bytes::new())
        .expect("static request");
    let accepted = WireRequest::accept(probe, &Limits::default()).expect("an acceptable host");
    let stamp = AmzDate::parse(&chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()).expect("a stamp");
    let credentials = SigningCredentials::new(access_key, secret_key.as_bytes()).expect("credentials");
    let signer = SigV4aSigner::new(credentials, stamp.day(), SigService::S3, "*").expect("a SigV4a signer");
    let signing = SigningRequest::new(
        &method,
        path,
        query,
        &headers,
        accepted.host().raw_for_signing(),
        PayloadMode::Unsigned,
        stamp,
    );
    let signed = match form {
        Signed::Header => signer.sign_headers(&signing),
        Signed::Query => signer.presign(&signing, 300),
    }
    .expect("a signable SigV4a request");
    let mut builder = Request::builder().method(method).uri(signed.target());
    for (name, value) in signed.headers() {
        builder = builder.header(name, value);
    }
    builder.body(Empty::new()).expect("a built SigV4a request")
}

struct SigV4aProbe(std::sync::atomic::AtomicUsize);

impl rustfs_gateway::Handler<rustfs_gateway::dto::ListBuckets> for SigV4aProbe {
    async fn call(
        &self,
        request: rustfs_gateway::Req<rustfs_gateway::dto::ListBuckets>,
    ) -> rustfs_gateway::HandlerResult<rustfs_gateway::dto::ListBuckets> {
        assert_eq!(
            request.context().principal().map(|principal| principal.access_key_id()),
            Some(ROOT_ACCESS_KEY)
        );
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(rustfs_gateway::Resp::new(rustfs_gateway::dto::ListBucketsOutput::default()))
    }
}

/// SigV4a header and presigned requests verify through `GatewayStack`, enforce signed
/// `x-rustfs-*` extension headers before reaching handlers, and reject tampered signatures.
#[tokio::test]
async fn sigv4a_requests_verify_and_enforce_signed_extension_headers() {
    let probe = Arc::new(SigV4aProbe(std::sync::atomic::AtomicUsize::new(0)));
    let a = assembly();
    let builder = GatewayStack::configured_builder(
        &a,
        Arc::new(ScriptedIam { ready: true }),
        root().source(),
        decide_with(|request| {
            if request.operation == "ListBuckets" && request.identity.is_some() {
                Decision::Allow
            } else {
                Decision::Deny
            }
        }),
        std::convert::identity,
    )
    .expect("the production builder assembles");
    let stack = GatewayStack {
        service: builder
            .register::<rustfs_gateway::dto::ListBuckets, _>(Arc::clone(&probe))
            .build()
            .expect("probe registration"),
        keystone_auth: None,
        control_plane: false,
        object_lambda_fallback: None,
    };

    for form in [Signed::Header, Signed::Query] {
        let req = sigv4a_request(form, Method::GET, "/", "", HeaderMap::new(), ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
        let (status, _, body) = answer_request(&stack, req).await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let mut signed_ext = HeaderMap::new();
        signed_ext.insert("x-rustfs-meta-probe", http::HeaderValue::from_static("ok"));
        let req = sigv4a_request(form, Method::GET, "/", "", signed_ext, ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
        let (status, _, body) = answer_request(&stack, req).await;
        assert_eq!(status, StatusCode::OK, "signed extension header must pass: {body}");

        let mut unsigned_ext = sigv4a_request(form, Method::GET, "/", "", HeaderMap::new(), ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
        unsigned_ext
            .headers_mut()
            .insert("x-rustfs-force-delete", http::HeaderValue::from_static("true"));
        let before = probe.0.load(std::sync::atomic::Ordering::SeqCst);
        let (status, _, body) = answer_request(&stack, unsigned_ext).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "unsigned extension header must fail: {body}");
        assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
        assert_eq!(
            probe.0.load(std::sync::atomic::Ordering::SeqCst),
            before,
            "unsigned extension header must never reach the handler"
        );
    }

    let mut tampered = sigv4a_request(Signed::Header, Method::GET, "/", "", HeaderMap::new(), ROOT_ACCESS_KEY, ROOT_SECRET_KEY);
    let auth = tampered
        .headers()
        .get(http::header::AUTHORIZATION)
        .expect("authorization")
        .to_str()
        .expect("utf-8")
        .to_owned();
    let last = auth.as_bytes().last().copied().expect("non-empty signature");
    let flipped = if last == b'0' { '1' } else { '0' };
    let mut broken = auth[..auth.len() - 1].to_owned();
    broken.push(flipped);
    tampered
        .headers_mut()
        .insert(http::header::AUTHORIZATION, broken.parse().expect("header value"));
    let before = probe.0.load(std::sync::atomic::Ordering::SeqCst);
    let (status, _, body) = answer_request(&stack, tampered).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"), "{body}");
    assert_eq!(probe.0.load(std::sync::atomic::Ordering::SeqCst), before);
    assert_eq!(probe.0.load(std::sync::atomic::Ordering::SeqCst), 4);
}

/// Real `GatewayStack::assemble` (real `EarlyAuthorizer` + real `GatewayBackend` handlers) serves
/// header SigV4a `ListBuckets`, allows a clean presigned SigV4a `UploadPart` to reach storage
/// (`404 NoSuchUpload`), and denies a presigned SigV4a `UploadPart` carrying a signed
/// `x-rustfs-replication-part-actual-size` header (`403 AccessDenied`).
#[tokio::test]
#[serial_test::serial]
async fn production_gateway_stack_blocks_presigned_sigv4a_upload_part_replication_headers() {
    use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    if crate::runtime_sources::current_app_context().is_none() {
        crate::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
    }
    let ambient = crate::runtime_sources::current_app_context().expect("ambient AppContext");
    let context = Arc::new(crate::runtime_sources::AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
    let bucket = format!("sigv4a-upload-part-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create SigV4a test bucket");
    let slot = ServerContextSlot::new();
    assert!(slot.install(Arc::clone(&context)));
    let gateway = TestGateway::over(&context, slot);

    // Header SigV4a passes real EarlyAuthorizer and ListBuckets handler.
    let list_req = sigv4a_request(
        Signed::Header,
        Method::GET,
        "/",
        "",
        HeaderMap::new(),
        TestGateway::ACCESS_KEY,
        TestGateway::SECRET_KEY,
    );
    let (status, _, body) = answer_request(&gateway.0, list_req).await;
    assert_eq!(status, StatusCode::OK, "header SigV4a ListBuckets must succeed: {body}");

    // Positive control: presigned SigV4a UploadPart without replication headers passes auth,
    // EarlyAuthorizer, and admit_operation, reaching storage (404 NoSuchUpload).
    let path = format!("/{bucket}/obj");
    let query = "partNumber=1&uploadId=nonexistent-upload";
    let control_req = sigv4a_request(
        Signed::Query,
        Method::PUT,
        &path,
        query,
        HeaderMap::new(),
        TestGateway::ACCESS_KEY,
        TestGateway::SECRET_KEY,
    );
    let (status, _, body) = answer_request(&gateway.0, control_req).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "control UploadPart must reach storage: {body}");
    assert!(body.contains("<Code>NoSuchUpload</Code>"), "{body}");

    // Negative regression: the identical presigned SigV4a UploadPart carrying a *signed*
    // `x-rustfs-replication-part-actual-size` header must be denied 403 AccessDenied.
    let mut repl_headers = HeaderMap::new();
    repl_headers.insert("x-rustfs-replication-part-actual-size", http::HeaderValue::from_static("128"));
    let repl_req = sigv4a_request(
        Signed::Query,
        Method::PUT,
        &path,
        query,
        repl_headers,
        TestGateway::ACCESS_KEY,
        TestGateway::SECRET_KEY,
    );
    let (status, _, body) = answer_request(&gateway.0, repl_req).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "presigned SigV4a UploadPart with signed replication header must be denied: {body}"
    );
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}
