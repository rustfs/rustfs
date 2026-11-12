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

//! Test fixtures: an assembled gateway service with this crate's adapters installed.
//!
//! Responsible for: the service the Keystone cases drive, recorders for what a handler and the
//! authorization stages were handed, the credentials a Keystone layer builds for a validated
//! token, and [`host_token_identity`], the only way a test can hold a principal the gateway
//! marked host-reported.
//! NOT responsible for: any assertion about Keystone behaviour.
//! Upstream: `rustfs_gateway::ServiceBuilder`. Downstream: this crate's test modules.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rustfs_credentials::Credentials;
use rustfs_gateway::dto::{ListBuckets, ListBucketsOutput};
use rustfs_gateway::{
    AuthSchemeRef, Authorizer, AuthzRequest, BoxFuture, ClockSkewAck, Decision, FixedClock, Handler, HandlerResult, Identity,
    IdentityOrigin, InputAuthzRequest, InputDecisions, RegionSet, Req, RequestContext, Resp, S3Service, SecurityFloor,
    ServiceBuilder, SigV4Authenticator, WireResponse,
};
use rustfs_iam::error::Error as IamError;
use rustfs_policy::auth::UserIdentity;
use serde_json::json;
use time::OffsetDateTime;

use super::{KeystoneCaller, KeystoneCallerRefused, KeystoneVerifier, establish_keystone_identity, keystone_header_token_claim};
use crate::credentials::IamKeyLookup;
use crate::policy::{IamPrincipalLookup, RustfsPrincipal};
use crate::root::{RootCell, RootCredentials};

/// The instant the fixture service's clock is fixed to.
pub(crate) const NOW: i64 = 1_800_000_000;
/// The token every case presents. A sentinel, so "nothing holds it" is a search for bytes.
pub(crate) const TOKEN: &str = "gAAAAABkeystone-token-SENTINEL";
pub(crate) const USER_ID: &str = "4a7d1ed414474e4033ac29ccb8653d9b";
/// Sentinels too: a principal name must contain neither.
pub(crate) const USERNAME: &str = "alice-USERNAME-SENTINEL";
pub(crate) const PROJECT_NAME: &str = "PROJECT-NAME-SENTINEL";
/// The one group the fixture IAM lists, and only for [`USERNAME`].
pub(crate) const LIVE_GROUP: &str = "live-group";

/// The credentials the Keystone layer builds for a validated token, field for field as
/// `keystone_token_to_credentials` (`crates/keystone/src/auth.rs`) sets them: the token itself is
/// the session token.
pub(crate) fn validated(user_id: &str, roles: &[&str], expires_at: i64) -> Credentials {
    let roles: Vec<String> = roles.iter().map(|role| (*role).to_owned()).collect();
    let claims = HashMap::from([
        ("keystone_user_id".to_owned(), json!(user_id)),
        ("keystone_username".to_owned(), json!(USERNAME)),
        ("keystone_project_id".to_owned(), json!("1f3870be274f6c49b3e31a0c6728957f")),
        ("keystone_project_name".to_owned(), json!(PROJECT_NAME)),
        ("keystone_domain_id".to_owned(), json!("default")),
        ("keystone_domain_name".to_owned(), json!("Default")),
        ("keystone_roles".to_owned(), json!(roles)),
        ("auth_source".to_owned(), json!("keystone")),
    ]);
    Credentials {
        access_key: format!("keystone:{user_id}"),
        secret_key: String::new(),
        session_token: TOKEN.to_owned(),
        expiration: Some(OffsetDateTime::from_unix_timestamp(expires_at).expect("a valid timestamp")),
        status: "active".to_owned(),
        parent_user: USERNAME.to_owned(),
        groups: Some(roles),
        claims: Some(claims),
        name: Some(USERNAME.to_owned()),
        description: Some(format!("Keystone user: {USERNAME}")),
    }
}

/// An IAM that holds an enabled user under every name it is asked about, and counts the asking:
/// a lookup of a Keystone principal's name would find a user, so "IAM was not asked" is measured.
#[derive(Default)]
pub(crate) struct FakeIam {
    pub(crate) key_lookups: AtomicUsize,
}

impl IamKeyLookup for FakeIam {
    fn is_ready(&self) -> bool {
        true
    }

    fn check_key<'a>(&'a self, access_key: &'a str) -> BoxFuture<'a, Result<(Option<UserIdentity>, bool), IamError>> {
        self.key_lookups.fetch_add(1, Ordering::SeqCst);
        let user = Credentials {
            access_key: access_key.to_owned(),
            secret_key: "iam-user-secret-0001".to_owned(),
            status: "on".to_owned(),
            ..Default::default()
        };
        Box::pin(async move { Ok((Some(UserIdentity::new(user)), true)) })
    }
}

impl IamPrincipalLookup for FakeIam {
    fn group_memberships(&self, user: &str) -> Vec<String> {
        if user == USERNAME {
            vec![LIVE_GROUP.to_owned()]
        } else {
            Vec::new()
        }
    }
}

/// A Keystone caller as a handler or an authorization stage read it: its verdict identity and
/// the facts a decision would be made with.
pub(crate) type Seen = Result<Option<(Identity, RustfsPrincipal)>, KeystoneCallerRefused>;

fn seen(caller: Result<Option<KeystoneCaller<'_>>, KeystoneCallerRefused>, iam: &FakeIam) -> Seen {
    Ok(caller?.map(|caller| (caller.identity().clone(), caller.principal(iam))))
}

/// What one handler read from its request's context.
pub(crate) struct Handed {
    pub(crate) caller: Seen,
    pub(crate) token_principal: Option<Identity>,
    pub(crate) anonymous: bool,
}

/// One authorization stage, as the recording authorizer was asked it.
pub(crate) struct Asked {
    pub(crate) scheme: AuthSchemeRef,
    pub(crate) identity: Option<Identity>,
    pub(crate) caller: Seen,
}

/// Serves `ListBuckets`, allows every request, and records what its handler and both of its
/// authorization stages were handed.
#[derive(Default)]
struct Recorder {
    iam: Arc<FakeIam>,
    handed: Mutex<Vec<Handed>>,
    asked: Mutex<Vec<Asked>>,
}

impl Recorder {
    fn ask(&self, context: &RequestContext<'_>, request: &AuthzRequest<'_>) {
        self.asked.lock().expect("not poisoned").push(Asked {
            scheme: context.auth_scheme(),
            identity: request.identity.cloned(),
            caller: seen(KeystoneCaller::from_authorizer(context, request), &self.iam),
        });
    }
}

impl Handler<ListBuckets> for Recorder {
    async fn call(&self, request: Req<ListBuckets>) -> HandlerResult<ListBuckets> {
        let context = request.context();
        self.handed.lock().expect("not poisoned").push(Handed {
            caller: seen(KeystoneCaller::from_handler(context), &self.iam),
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

/// One assembled service and its recorder.
pub(crate) struct Fixture {
    service: S3Service,
    recorder: Arc<Recorder>,
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
        self.recorder.iam.key_lookups.load(Ordering::SeqCst)
    }
}

/// The service a RustFS assembly builds: the IAM adapters and, when `keystone`, the Keystone
/// claim with its verifier, as an assembly installs them only when Keystone is enabled.
///
/// With `anonymous_reaches_the_handler`, a request that presents no credential is served, so the
/// cases that need "served as nobody" to differ from "refused" can tell the two apart.
pub(crate) fn assemble(keystone: bool, anonymous_reaches_the_handler: bool) -> Fixture {
    let recorder = Arc::new(Recorder::default());
    let root = RootCredentials::new("rootadmin", "root-secret-key-0001", "sts-signing-key-0001").expect("valid root credentials");
    let floor = if anonymous_reaches_the_handler {
        SecurityFloor::new().delegate_anonymous_to_authorizer_after_listing_in_the_posture_report()
    } else {
        SecurityFloor::new()
    };
    let mut builder = crate::IamAdapters::new(recorder.iam.clone(), Arc::new(RootCell::new(Some(root))))
        .install(ServiceBuilder::new(), |provider| {
            SigV4Authenticator::new(provider, RegionSet::new(["us-east-1"]).expect("a region"))
        })
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
    }
}

/// `GET /`, which routes to `ListBuckets`, with a host and no credential.
pub(crate) fn plain() -> http::Request<Bytes> {
    http::Request::builder()
        .uri("/")
        .header("host", "s3.example.com")
        .body(Bytes::new())
        .expect("a valid request")
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
pub(crate) fn established_as(validated: &Credentials, mut request: http::Request<Bytes>) -> http::Request<Bytes> {
    establish_keystone_identity(request.extensions_mut(), validated).expect("a validated token establishes an identity");
    request
}

/// The principal of a Keystone user exactly as the gateway hands it to a policy source, an
/// authorizer, a handler and an audit sink: read by the handler of a request the assembled
/// service served by its token. Only the gateway can mark a name host-reported.
pub(crate) async fn host_token_identity(user_id: &str) -> Identity {
    let fixture = assemble(true, false);
    let request = established_as(&validated(user_id, &["member"], NOW + 3600), token_request());
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
