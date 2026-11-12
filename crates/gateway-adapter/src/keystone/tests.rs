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

//! The Keystone adapter, in isolation and through an assembled gateway service.
//!
//! Responsible for: the claim, what establishes an identity, what the verifier reports, how a
//! caller is bound to its own request, and what a request is served as once the claim and the
//! verifier are installed.
//! NOT responsible for: the gateway's own rules for the scheme (which operations admit it, the
//! token beside an AWS credential, the cache headers), which nothing in this crate can change;
//! the policy snapshot of a Keystone caller (`policy.rs`); or the audit record (`authz_audit.rs`).
//! Upstream: `keystone.rs`, `fixture.rs`. Downstream: nothing.

use bytes::Bytes;
use rustfs_gateway::WireResponse;
use serde_json::json;
use time::OffsetDateTime;

use super::fixture::{
    FakeIam, Fixture, LIVE_GROUP, NOW, PROJECT_NAME, TOKEN, USER_ID, USERNAME, assemble, established_as, host_token_identity,
    plain, token_request, validated, with_token_under,
};
use super::*;
use crate::policy::PrincipalKind;

fn principal_name(user_id: &str) -> String {
    format!("keystone:{user_id}")
}

// ── the claim ──────────────────────────────────────────────────────────────────────────────────

/// Positive — the claim is exactly the one the gateway-side review spelled.
#[test]
fn claim_is_x_auth_token_refusing_x_storage_token() {
    let spelled = HeaderTokenClaim::new("x-auth-token")
        .and_then(|claim| claim.refusing_aliases(&["x-storage-token"]))
        .expect("a legal claim");
    assert_eq!(keystone_header_token_claim(), Ok(spelled));
}

// ── establishing an identity ───────────────────────────────────────────────────────────────────

fn establish(validated: &Credentials) -> Result<KeystoneEstablished, KeystoneNotEstablished> {
    let mut extensions = http::Extensions::new();
    let outcome = establish_keystone_identity(&mut extensions, validated);
    let inserted = extensions.remove::<KeystoneEstablished>();
    assert_eq!(outcome.is_ok(), inserted.is_some(), "the outcome and the extensions disagree");
    inserted.ok_or(KeystoneNotEstablished)
}

fn established(user_id: &str, roles: &[&str], expires_at: i64) -> KeystoneEstablished {
    establish(&validated(user_id, roles, expires_at)).expect("a validated token")
}

/// Positive — a validated token leaves exactly its own facts in the request, and so not its
/// token: the pattern names every field, each is compared with a value that holds no token, and
/// a field added later does not compile here until it is compared too.
#[test]
fn establish_records_exactly_what_was_validated() {
    let credentials = validated(USER_ID, &["member", "reader"], NOW + 60);
    assert_eq!(
        credentials.session_token, TOKEN,
        "the fixture carries the token, as the Keystone layer's does"
    );
    let KeystoneEstablished {
        identity,
        username,
        roles,
        claims,
        expires_at_unix_seconds,
    } = establish(&credentials).expect("a validated token");
    assert_eq!(
        (
            identity.access_key_id(),
            username.as_str(),
            &roles,
            Some(&claims),
            expires_at_unix_seconds
        ),
        (
            principal_name(USER_ID).as_str(),
            USERNAME,
            &credentials.groups,
            credentials.claims.as_ref(),
            NOW + 60
        )
    );
}

fn set_claim(credentials: &mut Credentials, name: &str, value: serde_json::Value) {
    credentials
        .claims
        .as_mut()
        .expect("the fixture has claims")
        .insert(name.to_owned(), value);
}

fn rename(credentials: &mut Credentials, user_id: &str) {
    credentials.access_key = principal_name(user_id);
    set_claim(credentials, "keystone_user_id", json!(user_id));
}

/// Negative — credentials that cannot name a Keystone principal, that carry a claim the Keystone
/// layer does not write, or that never expire establish nothing, and the extensions stay empty.
/// The unbroken credentials establish, so each refusal is its one change's doing.
#[test]
fn n_establish_inserts_nothing_for_credentials_it_cannot_vouch_for() {
    type Break = fn(&mut Credentials);
    let cases: &[(&str, Break)] = &[
        ("no prefix", |c| c.access_key = USER_ID.to_owned()),
        ("the prefix in another case", |c| c.access_key = format!("Keystone:{USER_ID}")),
        ("an empty user id", |c| rename(c, "")),
        ("a user id that is not the claimed one", |c| c.access_key = principal_name(USERNAME)),
        ("no user id claim", |c| {
            c.claims.as_mut().expect("claims").remove("keystone_user_id");
        }),
        ("a user id claim that is not a string", |c| {
            set_claim(c, "keystone_user_id", json!([USER_ID]))
        }),
        ("a user id with a space", |c| rename(c, "u 1")),
        ("a user id with a line break", |c| rename(c, "u\r\n1")),
        ("a user id outside ASCII", |c| rename(c, "\u{fc}ser")),
        ("a user id too long for a principal name", |c| rename(c, &"u".repeat(120))),
        ("a session policy among the claims", |c| set_claim(c, "sessionPolicy", json!("e30"))),
        ("a parent among the claims", |c| set_claim(c, "parent", json!("rootadmin"))),
        ("no expiry", |c| c.expiration = None),
    ];
    let established: Vec<&str> = cases
        .iter()
        .filter(|(_, break_it)| {
            let mut credentials = validated(USER_ID, &["member"], NOW + 60);
            break_it(&mut credentials);
            establish(&credentials).is_ok()
        })
        .map(|(name, _)| *name)
        .collect();
    assert_eq!(established, [""; 0], "an identity was established");
    assert!(establish(&validated(USER_ID, &["member"], NOW + 60)).is_ok(), "the control");
}

/// Negative — a failed validation leaves no identity behind, not even the one an earlier
/// validation of the same request established.
#[test]
fn n_a_failed_establish_removes_what_an_earlier_one_left() {
    let mut extensions = http::Extensions::new();
    assert_eq!(
        establish_keystone_identity(&mut extensions, &validated(USER_ID, &["member"], NOW + 60)),
        Ok(())
    );
    assert!(extensions.get::<KeystoneEstablished>().is_some(), "the control");
    let mut never_expires = validated("another-id", &["member"], NOW + 60);
    never_expires.expiration = None;
    assert_eq!(establish_keystone_identity(&mut extensions, &never_expires), Err(KeystoneNotEstablished));
    assert!(extensions.get::<KeystoneEstablished>().is_none(), "the earlier identity survived");
}

/// Compile-time — neither the established value nor the caller has `Debug`, so no formatting of
/// an extension bag or a context prints a user name or a claim. The two blanket impls make the
/// path ambiguous, and so a compile error, as soon as either type is `Debug`.
#[test]
fn n_established_and_caller_have_no_debug() {
    trait AmbiguousIfDebug<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfDebug<()> for T {}
    impl<T: ?Sized + core::fmt::Debug> AmbiguousIfDebug<u8> for T {}
    let _ = <KeystoneEstablished as AmbiguousIfDebug<_>>::some_item;
    let _ = <KeystoneCaller<'static> as AmbiguousIfDebug<_>>::some_item;
}

// ── the verifier ───────────────────────────────────────────────────────────────────────────────

fn reported(established: &KeystoneEstablished, now: i64) -> Option<String> {
    let identity = KeystoneVerifier
        .identity(established, RequestNow::from_unix_seconds(now))
        .ok()?;
    Some(identity.access_key_id().to_owned())
}

/// Positive — the principal is `keystone:<user_id>` and holds nothing else of the token: not the
/// user name, not a project, not a role.
#[test]
fn verifier_reports_keystone_user_id_and_nothing_else() {
    let name = reported(&established(USER_ID, &["admin"], NOW + 1), NOW).expect("a live token");
    assert_eq!(name, principal_name(USER_ID));
    for other in [USERNAME, PROJECT_NAME, "admin", TOKEN] {
        assert!(!name.contains(other), "{other} reached the principal name {name}");
    }
}

/// Negative — a token is refused from the second of its expiry onwards, against the clock reading
/// the verifier is handed, and accepted one second earlier. A fraction of a second never extends
/// it: an expiry inside second `NOW` is refused at the reading `NOW`.
#[test]
fn n_verifier_refuses_from_the_second_of_expiry_onwards() {
    let mut inside_the_second = validated(USER_ID, &["member"], NOW);
    inside_the_second.expiration = inside_the_second.expiration.map(|at| at + time::Duration::milliseconds(900));
    assert_eq!(inside_the_second.expiration.map(OffsetDateTime::unix_timestamp), Some(NOW));
    for token in [
        established(USER_ID, &["member"], NOW),
        establish(&inside_the_second).expect("validated"),
    ] {
        assert_eq!(reported(&token, NOW - 1), Some(principal_name(USER_ID)));
        for now in [NOW, NOW + 1, NOW + 86_400, i64::MAX] {
            assert_eq!(reported(&token, now), None, "accepted at {now}, expiry {NOW}");
        }
    }
}

/// Negative — the verifier holds nothing between calls: in whichever order it is asked, each
/// answer is the handed value's own. An expired value asked right after a live one is refused,
/// and a live one asked after another user's is still itself.
#[test]
fn n_verifier_answers_from_the_value_it_is_handed_alone() {
    let alice = established("alice-id", &["admin"], NOW + 10);
    let bob = established("bob-id", &["member"], NOW + 10);
    let expired = established("carol-id", &["admin"], NOW - 10);
    for _ in 0..2 {
        assert_eq!(reported(&alice, NOW).as_deref(), Some("keystone:alice-id"));
        assert_eq!(reported(&expired, NOW), None);
        assert_eq!(reported(&bob, NOW).as_deref(), Some("keystone:bob-id"));
    }
}

// ── the caller ─────────────────────────────────────────────────────────────────────────────────

const EVERY_OTHER_SCHEME: [AuthSchemeRef; 8] = [
    AuthSchemeRef::Anonymous,
    AuthSchemeRef::SigV4Header,
    AuthSchemeRef::SigV4Presigned,
    AuthSchemeRef::SigV2Header,
    AuthSchemeRef::SigV2Presigned,
    AuthSchemeRef::PostPolicy,
    AuthSchemeRef::OtherSigned,
    AuthSchemeRef::Bearer,
];

/// The principal a lookup bound, or that it refused.
fn bound(
    scheme: AuthSchemeRef,
    identity: Option<&Identity>,
    established: Option<&KeystoneEstablished>,
) -> Result<Option<String>, KeystoneCallerRefused> {
    let caller = KeystoneCaller::of_scheme(scheme, identity, established)?;
    Ok(caller.map(|caller| caller.identity().access_key_id().to_owned()))
}

/// Negative — under every scheme but the header token there is no Keystone caller, whatever the
/// request's extensions hold and although the signer's access key is spelled exactly like the
/// established principal. Under the header token, no principal or an access key is a refusal:
/// not "no Keystone caller", which a handler would go on to serve as someone else.
#[test]
fn n_only_a_host_reported_principal_under_the_header_token_is_a_keystone_caller() {
    let own = established(USER_ID, &["admin"], NOW + 60);
    let spelled_like_it = Identity::new(&principal_name(USER_ID)).expect("a legal access key");
    for identity in [None, Some(&spelled_like_it)] {
        for scheme in EVERY_OTHER_SCHEME {
            assert_eq!(bound(scheme, identity, Some(&own)), Ok(None), "{scheme:?}");
        }
        assert_eq!(bound(AuthSchemeRef::HeaderToken, identity, Some(&own)), Err(KeystoneCallerRefused));
    }
}

/// Positive and negative — a host-reported principal is a Keystone caller exactly when its own
/// request carries the value that names it. With nothing established, or with another user's
/// value, it is refused; and under any other scheme a host-reported name is a contradiction, not
/// an access key.
#[tokio::test]
async fn n_a_host_reported_principal_is_bound_to_what_its_own_request_carries() {
    let alice = host_token_identity("alice-id").await;
    let own = established("alice-id", &["member"], NOW + 60);
    let another = established("bob-id", &["admin"], NOW + 60);
    let token = AuthSchemeRef::HeaderToken;
    assert_eq!(bound(token, Some(&alice), Some(&own)), Ok(Some("keystone:alice-id".to_owned())));
    assert_eq!(bound(token, Some(&alice), None), Err(KeystoneCallerRefused));
    assert_eq!(bound(token, Some(&alice), Some(&another)), Err(KeystoneCallerRefused));
    for scheme in EVERY_OTHER_SCHEME {
        assert_eq!(bound(scheme, Some(&alice), Some(&own)), Err(KeystoneCallerRefused), "{scheme:?}");
    }
}

/// Positive and negative — the caller's principal carries the values a Keystone request is judged
/// with, unchanged from what the Keystone layer wrote, and is an owner exactly when a role is
/// `admin` or `reseller_admin`, byte for byte.
#[tokio::test]
async fn caller_principal_carries_the_keystone_decision_inputs() {
    let identity = host_token_identity(USER_ID).await;
    let cases: [(&[&str], bool); 9] = [
        (&["admin"], true),
        (&["member", "reseller_admin"], true),
        (&["Admin"], false),
        (&["ADMIN"], false),
        (&["ResellerAdmin"], false),
        (&["administrator"], false),
        (&["admin "], false),
        (&["member", "reader"], false),
        (&[], false),
    ];
    for (roles, owner) in cases {
        let credentials = validated(USER_ID, roles, NOW + 60);
        let own = establish(&credentials).expect("a validated token");
        let caller = KeystoneCaller::of_scheme(AuthSchemeRef::HeaderToken, Some(&identity), Some(&own))
            .ok()
            .flatten()
            .expect("a Keystone caller");
        assert_eq!(caller.identity().origin(), IdentityOrigin::HostToken);
        let principal = caller.principal(&FakeIam::default());
        assert_eq!(
            (
                principal.kind() == PrincipalKind::Keystone,
                principal.account(),
                (principal.username(), principal.parent_user()),
                principal.credential_groups(),
                principal.group_memberships(),
                principal.claims(),
                principal.is_owner(),
            ),
            (
                true,
                principal_name(USER_ID).as_str(),
                (USERNAME, USERNAME),
                &credentials.groups,
                &[LIVE_GROUP.to_owned()][..],
                credentials.claims.as_ref(),
                owner,
            ),
            "{roles:?}"
        );
    }
}

// ── through the assembled service ──────────────────────────────────────────────────────────────

/// `403 AccessDenied`, and neither an authorization stage nor a handler ran.
fn assert_refused_before_anything_ran(fixture: &Fixture, response: &WireResponse, at: &str) {
    let body = String::from_utf8_lossy(response.body()).into_owned();
    assert_eq!(response.status(), http::StatusCode::FORBIDDEN, "{at}: {body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "{at}: {body}");
    assert!(fixture.asked().is_empty(), "{at}: an authorization stage was asked");
    assert!(fixture.handed().is_empty(), "{at}: a handler ran");
}

/// Positive — a request whose only credential is the Keystone token, in which the outer layer
/// established an identity, is served as `keystone:<user_id>`: the handler and both authorization
/// stages read the same host-reported principal with the token's own roles, the scheme is the
/// header token, and IAM is asked for no key.
#[tokio::test]
async fn a_token_request_is_served_as_the_keystone_principal() {
    let fixture = assemble(true, false);
    let request = established_as(&validated(USER_ID, &["member"], NOW + 60), token_request());
    let response = fixture.exchange(request).await;
    assert_eq!(response.status(), http::StatusCode::OK, "{:?}", response.body());

    let handed = fixture.handed();
    let Ok(Some((identity, principal))) = &handed[0].caller else {
        panic!("the handler read no Keystone caller");
    };
    assert_eq!(
        (identity.access_key_id(), identity.origin()),
        (principal_name(USER_ID).as_str(), IdentityOrigin::HostToken)
    );
    assert_eq!(handed[0].token_principal.as_ref(), Some(identity));
    assert!(!handed[0].anonymous);
    assert_eq!(principal.credential_groups(), &Some(vec!["member".to_owned()]));

    let asked = fixture.asked();
    assert!(!asked.is_empty(), "no authorization stage was asked");
    for stage in &asked {
        assert_eq!(stage.scheme, AuthSchemeRef::HeaderToken);
        assert_eq!(stage.identity.as_ref(), Some(identity));
        assert!(
            matches!(&stage.caller, Ok(Some((at_stage, _))) if at_stage == identity),
            "a stage read another caller"
        );
    }
    assert_eq!(fixture.key_lookups(), 0, "IAM was asked about a Keystone principal's name");
}

/// Negative — a token in a request where the outer layer established nothing is `403`, and no
/// stage or handler runs, although the validated credentials sit in the Keystone crate's
/// task-local around the whole exchange, as that crate's layer scopes them: the task-local alone
/// authenticates nothing. The same exchange with the value established is served, and on a
/// service without the claim the token is an ordinary header and the request is served as
/// nobody, so the refusal is the claim's doing and the missing value's.
#[tokio::test]
async fn n_a_token_with_nothing_established_is_refused() {
    let credentials = validated(USER_ID, &["admin"], NOW + 60);
    let in_the_legacy_scope = async |fixture: &Fixture, request: http::Request<Bytes>| {
        rustfs_keystone::KEYSTONE_CREDENTIALS
            .scope(Some(credentials.clone()), fixture.exchange(request))
            .await
    };

    let fixture = assemble(true, true);
    let response = in_the_legacy_scope(&fixture, token_request()).await;
    assert_refused_before_anything_ran(&fixture, &response, "a token with nothing established");

    let response = in_the_legacy_scope(&fixture, established_as(&credentials, token_request())).await;
    assert_eq!(response.status(), http::StatusCode::OK, "the control: {:?}", response.body());
    assert!(matches!(fixture.handed()[0].caller, Ok(Some(_))), "the control: served as the caller");

    let unclaimed = assemble(false, true);
    let response = in_the_legacy_scope(&unclaimed, established_as(&credentials, token_request())).await;
    assert_eq!(response.status(), http::StatusCode::OK, "the control: {:?}", response.body());
    let handed = unclaimed.handed();
    assert!(
        handed[0].anonymous && matches!(handed[0].caller, Ok(None)),
        "the control: served as nobody"
    );
}

/// Negative — a request that carries the refused alias is `403` whatever the outer layer
/// established from it: alone, where it would otherwise be served as nobody while the host held
/// an identity, and beside the claimed token, where it would otherwise be served by that token.
/// The claimed token alone is served by the same service.
#[tokio::test]
async fn n_the_refused_alias_is_a_403_whatever_was_established() {
    let fixture = assemble(true, true);
    let credentials = validated(USER_ID, &["admin"], NOW + 60);
    for request in [
        with_token_under("x-storage-token", plain()),
        with_token_under("x-storage-token", token_request()),
    ] {
        let response = fixture.exchange(established_as(&credentials, request)).await;
        assert_refused_before_anything_ran(&fixture, &response, "the refused alias");
    }
    let response = fixture.exchange(established_as(&credentials, token_request())).await;
    assert_eq!(response.status(), http::StatusCode::OK, "the control: {:?}", response.body());
}

/// Negative — a token that expires at the request's clock reading, or earlier, is refused
/// although its identity was established, and no stage or handler runs; one second more of life
/// is served.
#[tokio::test]
async fn n_an_expired_token_is_refused_at_the_request_clock() {
    let fixture = assemble(true, false);
    for expires_at in [NOW, NOW - 1, NOW - 86_400] {
        let request = established_as(&validated(USER_ID, &["admin"], expires_at), token_request());
        let response = fixture.exchange(request).await;
        assert_refused_before_anything_ran(&fixture, &response, "an expired token");
    }
    let request = established_as(&validated(USER_ID, &["admin"], NOW + 1), token_request());
    assert_eq!(fixture.exchange(request).await.status(), http::StatusCode::OK, "the control");
}

/// Negative — an established value in a request that carries no token is nobody: the gateway
/// serves the request as anonymous, and neither the handler nor a stage reads a Keystone caller
/// out of the extensions. The value authenticates nothing without the gateway's verdict.
#[tokio::test]
async fn n_an_established_value_without_the_token_is_nobody() {
    let fixture = assemble(true, true);
    let request = established_as(&validated(USER_ID, &["admin"], NOW + 60), plain());
    assert_eq!(fixture.exchange(request).await.status(), http::StatusCode::OK);

    let handed = fixture.handed();
    assert!(handed[0].anonymous && handed[0].token_principal.is_none());
    assert!(matches!(handed[0].caller, Ok(None)), "the handler read a Keystone caller");
    let asked = fixture.asked();
    assert!(!asked.is_empty(), "no authorization stage was asked");
    for stage in asked {
        assert_eq!(stage.scheme, AuthSchemeRef::Anonymous);
        assert!(
            stage.identity.is_none() && matches!(stage.caller, Ok(None)),
            "a stage read a Keystone caller"
        );
    }
}
