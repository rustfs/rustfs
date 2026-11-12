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

//! Test fixtures: a fake Keystone that `rustfs-keystone` validates tokens against, and an
//! assembled gateway service with the SigV4 authenticator and, optionally, the Keystone claim.
//!
//! Responsible for: [`validated`] and [`proof`], the only way a test obtains a
//! `ValidatedKeystoneToken`; the service and its recorders; signed and token requests; and
//! [`host_token_identity`], a principal the gateway itself marked host-reported.
//! NOT responsible for: any assertion about Keystone behaviour.
//! Upstream: `rustfs_keystone`, `rustfs_gateway::ServiceBuilder`. Downstream: this crate's tests.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rustfs_credentials::Credentials;
use rustfs_gateway::dto::{ListBuckets, ListBucketsOutput};
use rustfs_gateway::sig::{AmzDate, PayloadMode, SigService, SigV4Signer, SigningCredentials, SigningRequest, SigningScope};
use rustfs_gateway::{
    AuthSchemeRef, Authorizer, AuthzRequest, BoxFuture, ClockSkewAck, Decision, FixedClock, Handler, HandlerResult, Identity,
    IdentityOrigin, InputAuthzRequest, InputDecisions, RegionSet, Req, RequestContext, Resp, S3Service, SecurityFloor,
    ServiceBuilder, SigV4Authenticator, WireResponse,
};
use rustfs_iam::error::Error as IamError;
use rustfs_keystone::{KeystoneAuthProvider, KeystoneClient, KeystoneVersion, ValidatedKeystoneToken};
use rustfs_policy::auth::UserIdentity;
use serde_json::json;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::{KeystoneVerifier, establish_keystone_identity, keystone_header_token_claim};
use crate::credentials::{IamCredentials, IamKeyLookup};
use crate::root::{RootCell, RootCredentials};

/// The instant the service's clock is fixed to: far enough ahead that `rustfs-keystone`, which
/// refuses a token expired at the real clock, accepts every expiry a case uses.
pub(crate) const NOW: i64 = 4_000_000_000;
/// [`NOW`] as a SigV4 stamp.
const SIGNED_AT: &str = "20961002T070640Z";
/// The token every case presents. A sentinel, so "nothing holds it" is a search for bytes.
pub(crate) const TOKEN: &str = "gAAAAABkeystone-token-SENTINEL";
pub(crate) const USER_ID: &str = "4a7d1ed414474e4033ac29ccb8653d9b";
/// A sentinel too: a principal name must not contain it.
pub(crate) const USERNAME: &str = "alice-USERNAME-SENTINEL";
/// The access key [`signed`] requests carry; [`FakeIam`] knows every key under [`SECRET`].
const ACCESS_KEY: &str = "AKIASIGNEDCALLER";
const SECRET: &str = "iam-user-secret-0001";

/// What `rustfs-keystone` returns once a fake Keystone has validated [`TOKEN`] for `user_id`,
/// expiring at `expires_at`.
pub(crate) async fn validated(user_id: &str, expires_at: OffsetDateTime) -> (Credentials, ValidatedKeystoneToken) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a fake Keystone");
    let endpoint = format!("http://{}", listener.local_addr().expect("a bound address"));
    let body = json!({"token": {
        "user": {"id": user_id, "name": USERNAME},
        "roles": [{"name": "member"}],
        "expires_at": expires_at.format(&Rfc3339).expect("an RFC 3339 expiry"),
    }})
    .to_string();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("one validation request");
        let mut request = [0; 4096];
        assert!(socket.read(&mut request).expect("the request head") > 0);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).expect("the validation answer");
    });
    let client = KeystoneClient::new(
        endpoint,
        KeystoneVersion::V3,
        None,
        None,
        None,
        "Default".to_owned(),
        true,
        Duration::from_secs(5),
    );
    let provider = KeystoneAuthProvider::new(client, 1, Duration::from_secs(1), false);
    let validated = provider
        .authenticate_with_token_and_proof(TOKEN)
        .await
        .expect("the fake Keystone validates the token");
    server.join().expect("the fake Keystone answered");
    validated
}

/// The proof of [`validated`], expiring at the whole second `expires_at`.
pub(crate) async fn proof(user_id: &str, expires_at: i64) -> ValidatedKeystoneToken {
    let expires_at = OffsetDateTime::from_unix_timestamp(expires_at).expect("a valid timestamp");
    validated(user_id, expires_at).await.1
}

/// An IAM that holds an enabled user under every key, and counts the asking.
#[derive(Default)]
pub(crate) struct FakeIam {
    key_lookups: AtomicUsize,
}

impl IamKeyLookup for FakeIam {
    fn is_ready(&self) -> bool {
        true
    }

    fn check_key<'a>(&'a self, access_key: &'a str) -> BoxFuture<'a, Result<(Option<UserIdentity>, bool), IamError>> {
        self.key_lookups.fetch_add(1, Ordering::SeqCst);
        let user = Credentials {
            access_key: access_key.to_owned(),
            secret_key: SECRET.to_owned(),
            status: "on".to_owned(),
            ..Default::default()
        };
        Box::pin(async move { Ok((Some(UserIdentity::new(user)), true)) })
    }
}

/// What one handler read from its request's context.
pub(crate) struct Handed {
    pub(crate) token_principal: Option<Identity>,
    pub(crate) anonymous: bool,
}

/// One authorization stage, as the recording authorizer was asked it.
pub(crate) struct Asked {
    pub(crate) scheme: AuthSchemeRef,
    pub(crate) identity: Option<Identity>,
}

/// Serves `ListBuckets`, allows every request, and records what its handler and its
/// authorization stages were handed.
#[derive(Default)]
struct Recorder {
    handed: Mutex<Vec<Handed>>,
    asked: Mutex<Vec<Asked>>,
}

impl Recorder {
    fn ask(&self, context: &RequestContext<'_>, request: &AuthzRequest<'_>) {
        self.asked.lock().expect("not poisoned").push(Asked {
            scheme: context.auth_scheme(),
            identity: request.identity.cloned(),
        });
    }
}

impl Handler<ListBuckets> for Recorder {
    async fn call(&self, request: Req<ListBuckets>) -> HandlerResult<ListBuckets> {
        let context = request.context();
        self.handed.lock().expect("not poisoned").push(Handed {
            token_principal: context.header_token_principal().cloned(),
            anonymous: context.is_anonymous(),
        });
        Ok(Resp::new(ListBucketsOutput::default()))
    }
}

impl Authorizer for Recorder {
    fn authorize_route<'a>(&'a self, context: &'a RequestContext<'a>, request: &'a AuthzRequest<'a>) -> BoxFuture<'a, Decision> {
        self.ask(context, request);
        Box::pin(async { Decision::Allow })
    }

    fn authorize_input<'a>(
        &'a self,
        context: &'a RequestContext<'a>,
        request: &'a InputAuthzRequest<'a>,
    ) -> BoxFuture<'a, InputDecisions> {
        self.ask(context, request.route());
        let decisions = request.decide_all(Decision::Allow, |_| Decision::Allow);
        Box::pin(async move { decisions })
    }
}

/// One assembled service, its recorder and its IAM.
pub(crate) struct Fixture {
    service: S3Service,
    recorder: Arc<Recorder>,
    iam: Arc<FakeIam>,
}

impl Fixture {
    pub(crate) async fn exchange(&self, request: http::Request<Bytes>) -> WireResponse {
        rustfs_gateway::collect(self.service.call_bytes(request).await)
            .await
            .expect("an in-memory body")
    }

    pub(crate) fn handed(&self) -> Vec<Handed> {
        std::mem::take(&mut *self.recorder.handed.lock().expect("not poisoned"))
    }

    pub(crate) fn asked(&self) -> Vec<Asked> {
        std::mem::take(&mut *self.recorder.asked.lock().expect("not poisoned"))
    }

    pub(crate) fn key_lookups(&self) -> usize {
        self.iam.key_lookups.load(Ordering::SeqCst)
    }
}

/// A service with the SigV4 authenticator over this crate's credential provider and, when
/// `keystone`, the Keystone claim with its verifier. No policy source is installed: how a
/// Keystone principal is authorised is not decided, and `IamPolicySource` refuses one.
///
/// With `anonymous_reaches_the_handler`, a request that presents no credential is served, so
/// "served as nobody" differs from "refused".
pub(crate) fn assemble(keystone: bool, anonymous_reaches_the_handler: bool) -> Fixture {
    let recorder = Arc::new(Recorder::default());
    let iam = Arc::new(FakeIam::default());
    let root = RootCredentials::new("rootadmin", "root-secret-key-0001", "sts-signing-key-0001").expect("valid root credentials");
    let provider = IamCredentials::new(iam.clone(), Arc::new(RootCell::new(Some(root))));
    let floor = if anonymous_reaches_the_handler {
        SecurityFloor::new().delegate_anonymous_to_authorizer_after_listing_in_the_posture_report()
    } else {
        SecurityFloor::new()
    };
    let mut builder = ServiceBuilder::new()
        .authenticator(SigV4Authenticator::new(
            Arc::new(provider),
            RegionSet::new(["us-east-1"]).expect("a region"),
        ))
        .authorizer(Arc::clone(&recorder))
        .security_floor(floor)
        .clock_with_skew_ack(
            FixedClock::at_unix_seconds(NOW),
            ClockSkewAck::i_understand_a_skewed_clock_can_disable_signature_expiry(),
        )
        .register::<ListBuckets, _>(Arc::clone(&recorder));
    if keystone {
        builder = builder.host_verified_header_token(keystone_header_token_claim().expect("a legal claim"), KeystoneVerifier);
    }
    Fixture {
        service: builder.build().expect("a complete assembly"),
        recorder,
        iam,
    }
}

const HOST: &str = "s3.example.com";

/// `GET /`, which routes to `ListBuckets`, with a host and no credential.
pub(crate) fn plain() -> http::Request<Bytes> {
    http::Request::builder()
        .uri("/")
        .header("host", HOST)
        .body(Bytes::new())
        .expect("a valid request")
}

/// [`plain`], SigV4-signed at [`NOW`] by [`ACCESS_KEY`] with the secret [`FakeIam`] holds.
pub(crate) fn signed() -> http::Request<Bytes> {
    let method = http::Method::GET;
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::HOST, http::HeaderValue::from_static(HOST));
    let host = rustfs_gateway_http::RawHost::from_host_header(HOST.as_bytes()).expect("an acceptable host");
    let stamp = AmzDate::parse(SIGNED_AT).expect("a stamp");
    let scope = SigningScope::new(stamp.day(), "us-east-1", SigService::S3).expect("a scope");
    let credentials = SigningCredentials::new(ACCESS_KEY, SECRET.as_bytes()).expect("credentials");
    let signing = SigningRequest::new(&method, "/", "", &headers, &host, PayloadMode::Empty, stamp);
    let signed = SigV4Signer::new(credentials, scope)
        .sign_headers(&signing)
        .expect("a signable request");
    let mut request = plain();
    for (name, value) in signed.headers() {
        request.headers_mut().insert(name, value.clone());
    }
    request
}

/// `request` carrying [`TOKEN`] under `header`.
pub(crate) fn with_token_under(header: &'static str, mut request: http::Request<Bytes>) -> http::Request<Bytes> {
    request.headers_mut().insert(header, http::HeaderValue::from_static(TOKEN));
    request
}

/// A request whose only credential is the Keystone token, with nothing established.
pub(crate) fn token_request() -> http::Request<Bytes> {
    with_token_under("x-auth-token", plain())
}

/// `request` as the Keystone layer leaves it once it has validated the token as `validated`.
pub(crate) fn established_as(validated: ValidatedKeystoneToken, mut request: http::Request<Bytes>) -> http::Request<Bytes> {
    establish_keystone_identity(request.extensions_mut(), validated).expect("a validated token establishes an identity");
    request
}

/// The principal of a Keystone user exactly as the gateway hands it on: read by the handler of a
/// request the assembled service served by its token. Only the gateway marks a name host-reported.
pub(crate) async fn host_token_identity(user_id: &str) -> Identity {
    let fixture = assemble(true, false);
    let request = established_as(proof(user_id, NOW + 3600).await, token_request());
    let response = fixture.exchange(request).await;
    assert_eq!(response.status(), http::StatusCode::OK, "{:?}", response.body());
    let identity = fixture
        .handed()
        .pop()
        .and_then(|handed| handed.token_principal)
        .expect("the handler read a header-token principal");
    assert_eq!(identity.origin(), IdentityOrigin::HostToken);
    identity
}
