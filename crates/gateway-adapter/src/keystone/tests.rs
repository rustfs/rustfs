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
//! Responsible for: the claim, what establishes an identity, what the verifier reports, and what
//! a request is served as once the claim and the verifier are installed.
//! NOT responsible for: the gateway's own rules for the scheme, which are measured in its
//! repository; or the policy source's answer for a Keystone principal (`policy.rs`).
//! Upstream: `keystone.rs`, `fixture.rs`. Downstream: nothing.

use rustfs_gateway::{AuthSchemeRef, IdentityOrigin, WireResponse};
use time::OffsetDateTime;

use super::fixture::{
    Fixture, NOW, TOKEN, USER_ID, USERNAME, assemble, established_as, plain, proof, signed, token_request, validated,
    with_token_under,
};
use super::*;

fn principal_name(user_id: &str) -> String {
    format!("keystone:{user_id}")
}

/// Positive — the claim is exactly `x-auth-token` refusing `x-storage-token`.
#[test]
fn claim_is_x_auth_token_refusing_x_storage_token() {
    let spelled = HeaderTokenClaim::new("x-auth-token")
        .and_then(|claim| claim.refusing_aliases(&["x-storage-token"]))
        .expect("a legal claim");
    assert_eq!(keystone_header_token_claim(), Ok(spelled));
}

fn establish(validated: ValidatedKeystoneToken) -> Result<KeystoneEstablished, KeystoneNotEstablished> {
    let mut extensions = http::Extensions::new();
    let outcome = establish_keystone_identity(&mut extensions, validated);
    let inserted = extensions.remove::<KeystoneEstablished>();
    assert_eq!(outcome.is_ok(), inserted.is_some(), "the outcome and the extensions disagree");
    inserted.ok_or(KeystoneNotEstablished)
}

async fn established(user_id: &str, expires_at: i64) -> KeystoneEstablished {
    establish(proof(user_id, expires_at).await).expect("a validated token")
}

/// Positive — a validation establishes `keystone:<user_id>` and its expiry, and nothing else:
/// the pattern names every field, so a field added later does not compile here until compared.
#[tokio::test]
async fn establish_records_the_validated_user_id_and_expiry() {
    let KeystoneEstablished {
        identity,
        expires_at_unix_seconds,
    } = established(USER_ID, NOW + 60).await;
    assert_eq!(
        (identity.access_key_id(), expires_at_unix_seconds),
        (principal_name(USER_ID).as_str(), NOW + 60)
    );
}

/// Negative — a user id that cannot name a principal establishes nothing; the control
/// establishes, so each refusal is its user id's doing.
#[tokio::test]
async fn n_establish_inserts_nothing_for_a_user_id_it_cannot_name() {
    let long = "u".repeat(120);
    let mut established = Vec::new();
    for user_id in ["", "u 1", "u\r\n1", "\u{fc}ser", long.as_str()] {
        if establish(proof(user_id, NOW + 60).await).is_ok() {
            established.push(user_id.to_owned());
        }
    }
    assert_eq!(established, [""; 0].map(str::to_owned), "an identity was established");
    assert!(establish(proof(USER_ID, NOW + 60).await).is_ok(), "the control");
}

/// Negative — a failed establish leaves no identity behind, not even an earlier one.
#[tokio::test]
async fn n_a_failed_establish_removes_what_an_earlier_one_left() {
    let mut extensions = http::Extensions::new();
    assert_eq!(establish_keystone_identity(&mut extensions, proof(USER_ID, NOW + 60).await), Ok(()));
    assert!(extensions.get::<KeystoneEstablished>().is_some(), "the control");
    assert_eq!(
        establish_keystone_identity(&mut extensions, proof("u 1", NOW + 60).await),
        Err(KeystoneNotEstablished)
    );
    assert!(extensions.get::<KeystoneEstablished>().is_none(), "the earlier identity survived");
}

/// Compile-time — neither the established value nor the proof is `Debug`, and the proof is not
/// `Clone`: each blanket pair below is ambiguous, and so a compile error, once its bound holds.
#[test]
fn n_established_and_proof_are_not_debug_and_the_proof_is_not_clone() {
    trait AmbiguousIfDebug<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfDebug<()> for T {}
    impl<T: ?Sized + core::fmt::Debug> AmbiguousIfDebug<u8> for T {}
    trait AmbiguousIfClone<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <KeystoneEstablished as AmbiguousIfDebug<_>>::some_item;
    let _ = <ValidatedKeystoneToken as AmbiguousIfDebug<_>>::some_item;
    let _ = <ValidatedKeystoneToken as AmbiguousIfClone<_>>::some_item;
}

fn reported(established: &KeystoneEstablished, now: i64) -> Option<String> {
    let identity = KeystoneVerifier
        .identity(established, RequestNow::from_unix_seconds(now))
        .ok()?;
    Some(identity.access_key_id().to_owned())
}

/// Positive — the principal is `keystone:<user_id>`: not the user name, and not the token.
#[tokio::test]
async fn verifier_reports_keystone_user_id_and_nothing_else() {
    let name = reported(&established(USER_ID, NOW + 1).await, NOW).expect("a live token");
    assert_eq!(name, principal_name(USER_ID));
    for other in [USERNAME, TOKEN] {
        assert!(!name.contains(other), "{other} reached the principal name {name}");
    }
}

/// Negative — a token is refused from the second of its expiry onwards and accepted one second
/// earlier; a fraction of a second never extends it.
#[tokio::test]
async fn n_verifier_refuses_from_the_second_of_expiry_onwards() {
    let inside_the_second = OffsetDateTime::from_unix_timestamp(NOW).expect("a timestamp") + time::Duration::milliseconds(900);
    for token in [
        established(USER_ID, NOW).await,
        establish(validated(USER_ID, inside_the_second).await.1).expect("validated"),
    ] {
        assert_eq!(reported(&token, NOW - 1), Some(principal_name(USER_ID)));
        for now in [NOW, NOW + 1, NOW + 86_400, i64::MAX] {
            assert_eq!(reported(&token, now), None, "accepted at {now}, expiry {NOW}");
        }
    }
}

/// Negative — the verifier holds nothing between calls: an expired value asked right after a
/// live one is refused, and a live one asked after another user's is still itself.
#[tokio::test]
async fn n_verifier_answers_from_the_value_it_is_handed_alone() {
    let alice = established("alice-id", NOW + 10).await;
    let bob = established("bob-id", NOW + 10).await;
    let expired = established("carol-id", NOW - 10).await;
    for _ in 0..2 {
        assert_eq!(reported(&alice, NOW).as_deref(), Some("keystone:alice-id"));
        assert_eq!(reported(&expired, NOW), None);
        assert_eq!(reported(&bob, NOW).as_deref(), Some("keystone:bob-id"));
    }
}

/// `403 AccessDenied`, and neither an authorization stage nor a handler ran.
fn assert_refused_before_anything_ran(fixture: &Fixture, response: &WireResponse, at: &str) {
    let body = String::from_utf8_lossy(response.body()).into_owned();
    assert_eq!(response.status(), http::StatusCode::FORBIDDEN, "{at}: {body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "{at}: {body}");
    assert!(fixture.asked().is_empty(), "{at}: an authorization stage was asked");
    assert!(fixture.handed().is_empty(), "{at}: a handler ran");
}

/// Positive — a token request with an established identity is served as `keystone:<user_id>`,
/// host-reported, under the header-token scheme at every stage, and IAM is asked for no key.
#[tokio::test]
async fn a_token_request_is_served_as_the_keystone_principal() {
    let fixture = assemble(true, false);
    let response = fixture
        .exchange(established_as(proof(USER_ID, NOW + 60).await, token_request()))
        .await;
    assert_eq!(response.status(), http::StatusCode::OK, "{:?}", response.body());

    let handed = fixture.handed();
    let identity = handed[0].token_principal.as_ref().expect("a header-token principal");
    assert_eq!(
        (identity.access_key_id(), identity.origin(), handed[0].anonymous),
        (principal_name(USER_ID).as_str(), IdentityOrigin::HostToken, false)
    );
    let asked = fixture.asked();
    assert!(!asked.is_empty(), "no authorization stage was asked");
    for stage in &asked {
        assert_eq!((stage.scheme, stage.identity.as_ref()), (AuthSchemeRef::HeaderToken, Some(identity)));
    }
    assert_eq!(fixture.key_lookups(), 0, "IAM was asked about a Keystone principal's name");
}

/// Negative — a token with nothing established is `403` before any stage, run inside the Keystone
/// layer's task-local scope as that layer runs a request it validated. With the value established
/// the same exchange is served; without the claim the token is an ordinary header and the request
/// is served as nobody, so the refusal is the claim's and the missing value's.
#[tokio::test]
async fn n_a_token_with_nothing_established_is_refused_as_the_legacy_layer_runs_it() {
    let in_the_legacy_scope = async |fixture: &Fixture, request: http::Request<bytes::Bytes>| {
        let (credentials, _) = validated(USER_ID, OffsetDateTime::from_unix_timestamp(NOW + 60).expect("a timestamp")).await;
        rustfs_keystone::KEYSTONE_CREDENTIALS
            .scope(Some(credentials), fixture.exchange(request))
            .await
    };

    let fixture = assemble(true, true);
    let response = in_the_legacy_scope(&fixture, token_request()).await;
    assert_refused_before_anything_ran(&fixture, &response, "a token with nothing established");

    let request = established_as(proof(USER_ID, NOW + 60).await, token_request());
    let response = in_the_legacy_scope(&fixture, request).await;
    assert_eq!(response.status(), http::StatusCode::OK, "the control: {:?}", response.body());
    assert!(fixture.handed()[0].token_principal.is_some(), "the control: served as the caller");

    let unclaimed = assemble(false, true);
    let request = established_as(proof(USER_ID, NOW + 60).await, token_request());
    let response = in_the_legacy_scope(&unclaimed, request).await;
    assert_eq!(response.status(), http::StatusCode::OK, "the control: {:?}", response.body());
    let handed = unclaimed.handed();
    assert!(
        handed[0].anonymous && handed[0].token_principal.is_none(),
        "the control: served as nobody"
    );
}

/// Negative — a request carrying the refused alias is `403` whatever was established, alone and
/// beside the claimed token; the claimed token alone is served by the same service.
#[tokio::test]
async fn n_the_refused_alias_is_a_403_whatever_was_established() {
    let fixture = assemble(true, true);
    for request in [
        with_token_under("x-storage-token", plain()),
        with_token_under("x-storage-token", token_request()),
    ] {
        let response = fixture
            .exchange(established_as(proof(USER_ID, NOW + 60).await, request))
            .await;
        assert_refused_before_anything_ran(&fixture, &response, "the refused alias");
    }
    let response = fixture
        .exchange(established_as(proof(USER_ID, NOW + 60).await, token_request()))
        .await;
    assert_eq!(response.status(), http::StatusCode::OK, "the control: {:?}", response.body());
}

/// Negative — a token that expires at the request's clock reading, or earlier, is refused
/// although its identity was established; one second more of life is served.
#[tokio::test]
async fn n_an_expired_token_is_refused_at_the_request_clock() {
    let fixture = assemble(true, false);
    for expires_at in [NOW, NOW - 1, NOW - 86_400] {
        let response = fixture
            .exchange(established_as(proof(USER_ID, expires_at).await, token_request()))
            .await;
        assert_refused_before_anything_ran(&fixture, &response, "an expired token");
    }
    let request = established_as(proof(USER_ID, NOW + 1).await, token_request());
    assert_eq!(fixture.exchange(request).await.status(), http::StatusCode::OK, "the control");
}

/// Negative — an established value in a request without the token is nobody: the request is
/// served as anonymous at the handler and at every stage.
#[tokio::test]
async fn n_an_established_value_without_the_token_is_nobody() {
    let fixture = assemble(true, true);
    let request = established_as(proof(USER_ID, NOW + 60).await, plain());
    assert_eq!(fixture.exchange(request).await.status(), http::StatusCode::OK);

    let handed = fixture.handed();
    assert!(handed[0].anonymous && handed[0].token_principal.is_none());
    let asked = fixture.asked();
    assert!(!asked.is_empty(), "no authorization stage was asked");
    for stage in asked {
        assert_eq!((stage.scheme, stage.identity), (AuthSchemeRef::Anonymous, None));
    }
}

/// Negative — beside the SigV4 authenticator, a validly signed request that also carries an
/// established `x-auth-token` is `403` and no credential is looked up; the same signed request
/// without the token is served as its signer, after one lookup.
#[tokio::test]
async fn n_a_signed_request_with_an_established_token_is_refused_without_a_lookup() {
    let fixture = assemble(true, false);
    let request = established_as(proof(USER_ID, NOW + 60).await, with_token_under("x-auth-token", signed()));
    let response = fixture.exchange(request).await;
    assert_refused_before_anything_ran(&fixture, &response, "a signature beside a token");
    assert_eq!(fixture.key_lookups(), 0, "a credential was looked up");

    let response = fixture.exchange(signed()).await;
    assert_eq!(response.status(), http::StatusCode::OK, "the control: {:?}", response.body());
    assert_eq!(fixture.key_lookups(), 1, "the control: the signer was looked up");
    let asked = fixture.asked();
    assert!(
        !asked.is_empty() && asked.iter().all(|stage| stage.scheme == AuthSchemeRef::SigV4Header),
        "the control"
    );
}
