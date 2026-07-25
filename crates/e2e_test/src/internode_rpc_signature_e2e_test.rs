#![cfg(test)]
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

//! Cross-process replay / tamper acceptance for the internode NodeService v2 RPC
//! signature (<https://github.com/rustfs/backlog/issues/1327>).
//!
//! # Why this exists on top of the in-process tests
//!
//! `http_auth.rs` unit-tests the signature algebra by calling the verifier
//! directly. That proves the crypto, but it cannot prove that a *deployed*
//! server actually reaches it: the request has to survive the hybrid HTTP/gRPC
//! router, `check_auth`, tonic's own metadata handling, and finally the
//! per-handler body-digest gate. A handler that forgets its
//! `verify_disk_mutation_digest` call, or a router change that bypasses
//! `check_auth`, is invisible in-process and wide open in production. These
//! tests drive a real `rustfs` child process over a real TCP socket, so every
//! one of those layers is in the path.
//!
//! # Attacker model
//!
//! The adversary is on-path: it observed one legitimately signed request and
//! can resend, retarget, or edit those bytes — including individual headers.
//! It does **not** hold the RPC secret. The test process does hold the secret,
//! but uses it for exactly one purpose: minting the request that stands in for
//! the captured one. Every attack then only *reuses or edits* an already-minted
//! header set; no attack step ever re-signs. If any of these tests could pass
//! by re-signing, it would be testing nothing.
//!
//! # Isolating one variable at a time
//!
//! Each rejection is paired with an acceptance that differs in exactly one
//! respect, because a misconfigured harness (wrong audience, dead server,
//! ambient strict env) would otherwise make every "rejected" assertion pass
//! vacuously. Two pairings carry most of the weight:
//!
//! - Editing the body alone is caught by the *handler* (`PermissionDenied`);
//!   editing the body **and** repairing the digest header to match is caught by
//!   the *signature* (`Unauthenticated`). The second only fails closed if the
//!   digest is genuinely inside the signed scope, so the pair pins both layers.
//! - Replaying a captured nonce is caught by the replay cache; swapping in a
//!   fresh nonce is caught by the signature. Again, only the pair proves the
//!   nonce is signed rather than merely cached.
//!
//! # Why `MakeVolume` against a non-existent disk
//!
//! Every covered handler checks the digest before touching storage, and
//! `MakeVolume` resolves its disk *after* that check. Aiming at a disk that
//! cannot exist gives three cleanly separable outcomes with zero side effects
//! on the server's real data:
//!
//! - `Err(Unauthenticated)` — rejected by `check_auth` (signature layer).
//! - `Err(PermissionDenied)` — rejected by the handler's body-digest gate.
//! - `Ok(success: false)` — **authentication passed**; the request reached
//!   handler logic and only then failed on the bogus disk.
//!
//! # Coverage of the issue's acceptance matrix
//!
//! | Acceptance item | Test |
//! |---|---|
//! | replay a signature onto another method → reject | [`cross_method_signature_transplant_is_rejected`] |
//! | replay same method + body after nonce consumed → reject | [`nonce_replay_of_a_captured_mutation_is_rejected`] |
//! | nonce is signed, not just cached → reject a swapped nonce | [`swapping_in_a_fresh_nonce_is_rejected`] |
//! | tamper one byte of the body → reject | [`tampered_mutation_body_is_rejected`] |
//! | body digest is inside the signed scope → reject a repaired digest | [`rewriting_the_digest_to_match_a_tampered_body_is_rejected`] |
//! | wrong destination node identity → reject | [`signature_minted_for_another_node_is_rejected`] |
//! | mixed version: legacy-only still served, not blocked | [`legacy_only_signature_is_accepted_in_default_posture`] |
//! | strict flip closes the signature downgrade | [`signature_strict_rejects_legacy_only_downgrade`] |
//! | strict flip closes the body-digest downgrade, incl. v1 | [`body_digest_strict_rejects_digestless_mutation`] |
//!
//! Two acceptance items are deliberately left to the in-process tests. A stale
//! timestamp cannot be forged from outside — it is inside the HMAC — so
//! observing it would mean idling out the full freshness window. And the
//! `signature_v1_fallback_total` / `body_digest_fallback_total` counter deltas
//! that gate the strict flips are asserted directly in `http_auth.rs`; the
//! legacy test below proves only the *accepted* half of that behaviour.

use crate::common::{RustFSTestEnvironment, init_logging};
use crate::storage_api::internode_rpc_signature::{
    TONIC_RPC_PREFIX, gen_signature_headers, gen_tonic_signature_headers, node_service_time_out_client_no_auth,
};
use http::{HeaderMap, Method};
use rustfs_config::{
    ENV_INTERNODE_RPC_BODY_DIGEST_STRICT, ENV_INTERNODE_RPC_REPLAY_CACHE_CAPACITY, ENV_INTERNODE_RPC_SIGNATURE_STRICT,
};
use rustfs_protos::canonical_make_volume_request_body;
use rustfs_protos::proto_gen::node_service::{MakeVolumeRequest, MakeVolumeResponse};
use serial_test::serial;
use sha2::{Digest, Sha256};
use std::error::Error;
use tonic::{Code, Request, Status};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

/// Shared internode secret handed to both the child server and this process.
///
/// Must not be the default credential: `resolve_rpc_secret` fails closed on
/// defaults (GHSA-r5qv), so a default here would break every request rather
/// than test anything.
const TEST_RPC_SECRET: &str = "rustfs-internode-signature-e2e-secret";

/// A disk path the server cannot possibly have configured, so a request that
/// clears authentication stops harmlessly at `find_disk`.
const ABSENT_DISK: &str = "/nonexistent/rustfs-signature-e2e-disk";

/// Wire names of the two v2 headers these tests edit. They are `pub(crate)` in
/// ecstore, so they are repeated here rather than imported — [`overwrite_header`]
/// asserts the header it replaces was actually present, which turns a rename
/// into a loud failure instead of silently reducing an attack to a no-op.
const CONTENT_SHA256_HEADER: &str = "x-rustfs-content-sha256";
const NONCE_HEADER: &str = "x-rustfs-rpc-nonce";

/// gRPC service name carried in the signed scope, i.e. `TONIC_RPC_PREFIX`
/// without its leading `/`.
fn node_service_name() -> &'static str {
    TONIC_RPC_PREFIX.trim_start_matches('/')
}

/// Make the RPC secret of this test process match the child server's.
///
/// The secret lands in a process-wide `OnceLock`, so the first writer wins for
/// the whole test binary. Every test here uses the same constant, and the
/// assertion turns a cross-test collision into an explicit failure instead of a
/// confusing wall of signature rejections.
fn align_rpc_secret_with_server() {
    let _ = rustfs_credentials::set_global_rpc_secret(TEST_RPC_SECRET.to_string());
    let effective = rustfs_credentials::try_get_rpc_token().expect("RPC secret must resolve in the test process");
    assert_eq!(
        effective, TEST_RPC_SECRET,
        "another test in this binary already fixed a different process-wide RPC secret; \
         the signature tests cannot mint requests the child server will accept"
    );
}

/// Start a `rustfs` child process sharing [`TEST_RPC_SECRET`], with the rollout
/// posture pinned explicitly.
///
/// The child inherits the ambient environment, so the strict gates and the
/// replay-cache capacity are set here rather than assumed: a developer or CI
/// runner exporting `RUSTFS_INTERNODE_RPC_*` would otherwise silently flip the
/// posture and fail these tests for a non-security reason. `extra_env` is
/// applied last so the strict tests can still override.
///
/// Uses the no-cleanup spawn so a `pkill` pattern cannot reap servers belonging
/// to other tests running in the same binary.
async fn start_server(extra_env: &[(&str, &str)]) -> Result<RustFSTestEnvironment, Box<dyn Error + Send + Sync>> {
    let mut env = RustFSTestEnvironment::new().await?;
    let mut child_env = vec![
        ("RUSTFS_RPC_SECRET", TEST_RPC_SECRET),
        (ENV_INTERNODE_RPC_SIGNATURE_STRICT, "false"),
        (ENV_INTERNODE_RPC_BODY_DIGEST_STRICT, "false"),
        (ENV_INTERNODE_RPC_REPLAY_CACHE_CAPACITY, "1048576"),
    ];
    child_env.extend_from_slice(extra_env);
    env.start_rustfs_server_without_cleanup_with_env(&child_env).await?;
    Ok(env)
}

/// Stop the child and drop the cached gRPC channel for its address.
///
/// `node_service_time_out_client_no_auth` memoises channels in a process-global
/// map keyed by URL, and ports handed out by `find_available_port` can recur
/// within one test binary. Evicting here keeps a later test from inheriting a
/// channel aimed at this test's dead server.
async fn stop_server(mut env: RustFSTestEnvironment, url: &str) {
    env.stop_server();
    rustfs_protos::evict_failed_connection(url).await;
}

/// The audience the server binds into the v2 signature: its own node authority.
///
/// A single-node server started with `--address 127.0.0.1:PORT` over filesystem
/// endpoints has no URL peer set, so `init_local_peer` falls back to
/// `host:port` — exactly the address we dialed. The positive controls below
/// fail loudly if that ever stops holding.
fn audience_of(env: &RustFSTestEnvironment) -> String {
    env.address.clone()
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().fold(String::new(), |mut acc, byte| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{byte:02x}");
        acc
    })
}

fn make_volume_request(volume: &str) -> MakeVolumeRequest {
    MakeVolumeRequest {
        disk: ABSENT_DISK.to_string(),
        volume: volume.to_string(),
    }
}

fn canonical_digest(request: &MakeVolumeRequest) -> String {
    hex_sha256(&canonical_make_volume_request_body(request).expect("canonical body must encode"))
}

/// Mint a full v2 header set for `(audience, rpc_method, content_sha256)`.
///
/// This is the only place a signature is produced. Tests treat the returned map
/// as an opaque captured artifact.
fn mint_v2_headers(audience: &str, rpc_method: &str, content_sha256: Option<&str>) -> HeaderMap {
    gen_tonic_signature_headers(audience, node_service_name(), rpc_method, content_sha256)
        .expect("minting a v2 signature must succeed once the RPC secret is aligned")
}

/// Mint the pre-v2 header set: a signature over the fixed
/// `TONIC_RPC_PREFIX|GET|timestamp` constant, with no v2 headers at all. This is
/// both what an un-upgraded peer sends and what an attacker sends to force a
/// downgrade.
fn mint_legacy_only_headers() -> HeaderMap {
    gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET).expect("minting a legacy signature must succeed")
}

/// Replace one header of a captured set, asserting it was there to begin with.
fn overwrite_header(headers: &mut HeaderMap, name: &'static str, value: &str) {
    assert!(
        headers.contains_key(name),
        "minted headers must carry {name}; the wire contract changed and this attack would edit nothing"
    );
    headers.insert(name, value.parse().expect("header value must be valid"));
}

/// Send `request` to the server's NodeService with exactly `headers` attached
/// and nothing else — no interceptor adds or rewrites auth metadata, so the
/// bytes on the wire are the ones the test chose.
async fn call_make_volume(url: &str, request: MakeVolumeRequest, headers: HeaderMap) -> Result<MakeVolumeResponse, Status> {
    let mut client = node_service_time_out_client_no_auth(&url.to_string())
        .await
        .map_err(|err| Status::unavailable(format!("cannot reach the node service: {err}")))?;
    let mut rpc_request = Request::new(request);
    rpc_request.metadata_mut().as_mut().extend(headers);
    client.make_volume(rpc_request).await.map(|response| response.into_inner())
}

/// Assert a call cleared authentication.
///
/// Receiving *any* `Ok` response is the load-bearing signal: both auth layers
/// reject with a `Status`, so an `Ok` means the request reached handler logic.
/// The failed disk lookup underneath is what keeps it side-effect free.
fn assert_authenticated(result: Result<MakeVolumeResponse, Status>, context: &str) {
    match result {
        Ok(response) => {
            assert!(
                !response.success,
                "{context}: the absent disk {ABSENT_DISK} must not yield a successful volume creation"
            );
            assert!(
                response.error.is_some(),
                "{context}: expected the request to reach disk lookup and fail there, got no error"
            );
        }
        Err(status) => panic!(
            "{context}: the request must clear authentication, but was rejected with {:?}: {}",
            status.code(),
            status.message()
        ),
    }
}

/// Assert a call was rejected, optionally pinning which check spoke.
///
/// `PermissionDenied` responses carry the reason on the wire, so the digest
/// tests pin it and cannot be satisfied by an unrelated digest-gate failure.
/// `Unauthenticated` is deliberately generic on the wire; those tests pin their
/// cause structurally instead, by differing from a passing request in exactly
/// one respect.
fn assert_rejected(result: Result<MakeVolumeResponse, Status>, expected: Code, expected_message: Option<&str>, context: &str) {
    match result {
        Ok(response) => panic!(
            "{context}: the request must be rejected, but the server accepted it and ran the handler \
             (success={}, error={:?})",
            response.success, response.error
        ),
        Err(status) => {
            assert_eq!(
                status.code(),
                expected,
                "{context}: expected {expected:?}, got {:?}: {}",
                status.code(),
                status.message()
            );
            if let Some(needle) = expected_message {
                assert!(
                    status.message().contains(needle),
                    "{context}: expected the rejection to cite {needle:?}, got {:?}",
                    status.message()
                );
            }
        }
    }
}

/// Default posture (both strict gates off): the protections that hold without
/// any operator flip.
///
/// Grouped into one server start because each case is independent and spawning
/// a `rustfs` process per assertion would dominate the runtime.
#[tokio::test]
#[serial]
async fn internode_rpc_signature_default_posture_e2e() -> TestResult {
    init_logging();
    align_rpc_secret_with_server();
    let env = start_server(&[]).await?;
    let url = env.url.clone();
    let audience = audience_of(&env);

    signed_mutations_are_accepted(&url, &audience).await;
    unsigned_request_is_rejected(&url).await;
    cross_method_signature_transplant_is_rejected(&url, &audience).await;
    nonce_replay_of_a_captured_mutation_is_rejected(&url, &audience).await;
    swapping_in_a_fresh_nonce_is_rejected(&url, &audience).await;
    tampered_mutation_body_is_rejected(&url, &audience).await;
    rewriting_the_digest_to_match_a_tampered_body_is_rejected(&url, &audience).await;
    signature_minted_for_another_node_is_rejected(&url).await;
    legacy_only_signature_is_accepted_in_default_posture(&url).await;

    stop_server(env, &url).await;
    Ok(())
}

/// Baseline: correctly signed mutations are accepted, both with and without a
/// body digest.
///
/// These anchor every rejection below. The body-bound case proves the audience
/// the server verifies against really is the address we dialed. The digestless
/// case is the control the transplant test needs: without it, a regression that
/// rejected every `UNSIGNED-PAYLOAD` request would make the transplant
/// assertion pass for entirely the wrong reason. It also documents that the
/// default posture still serves digestless mutations.
async fn signed_mutations_are_accepted(url: &str, audience: &str) {
    let bound = make_volume_request("signature-e2e-control-bound");
    let bound_headers = mint_v2_headers(audience, "MakeVolume", Some(&canonical_digest(&bound)));
    assert_authenticated(
        call_make_volume(url, bound, bound_headers).await,
        "a correctly signed body-bound mutation",
    );

    let digestless = make_volume_request("signature-e2e-control-digestless");
    let digestless_headers = mint_v2_headers(audience, "MakeVolume", None);
    assert_authenticated(
        call_make_volume(url, digestless, digestless_headers).await,
        "a correctly signed digestless mutation in the default posture",
    );
}

/// A request with no auth metadata at all must never reach a handler.
async fn unsigned_request_is_rejected(url: &str) {
    let result = call_make_volume(url, make_volume_request("signature-e2e-unsigned"), HeaderMap::new()).await;
    assert_rejected(result, Code::Unauthenticated, None, "an entirely unsigned mutation");
}

/// GHSA-c667 class: a signature captured from one gRPC method must not be
/// replayable onto another.
///
/// Before method-path binding every NodeService call signed the same constant,
/// so a captured `Ping` — the cheapest, least privileged call on the service —
/// authenticated a `MakeVolume` just as well. The captured `Ping` signature is
/// transplanted verbatim; the server recomputes the scope with
/// `rpc_method = MakeVolume` and the HMAC no longer matches. It differs from the
/// accepted digestless control above only in the method it was minted for.
async fn cross_method_signature_transplant_is_rejected(url: &str, audience: &str) {
    let captured_ping = mint_v2_headers(audience, "Ping", None);
    let result = call_make_volume(url, make_volume_request("signature-e2e-transplant"), captured_ping).await;
    assert_rejected(
        result,
        Code::Unauthenticated,
        None,
        "a Ping signature transplanted onto a MakeVolume mutation",
    );
}

/// A body-bound mutation must be consumable exactly once.
///
/// The first send establishes that the captured artifact is genuinely valid —
/// without it, the second rejection could just mean the headers were malformed
/// all along. The replay reuses the identical `(signature, timestamp, nonce)`
/// well inside the freshness window, so only the server's replay cache can
/// stop it.
async fn nonce_replay_of_a_captured_mutation_is_rejected(url: &str, audience: &str) {
    let request = make_volume_request("signature-e2e-replay");
    let captured = mint_v2_headers(audience, "MakeVolume", Some(&canonical_digest(&request)));

    let first = call_make_volume(url, request.clone(), captured.clone()).await;
    assert_authenticated(first, "the captured mutation on its first delivery");

    let replayed = call_make_volume(url, request, captured).await;
    assert_rejected(
        replayed,
        Code::Unauthenticated,
        None,
        "the same captured mutation replayed after its nonce was consumed",
    );
}

/// The nonce must be *signed*, not merely remembered.
///
/// A replay cache alone would be trivially defeated: swap in a fresh UUID and
/// the cache has never seen it. This request is byte-identical to one the server
/// would accept apart from that one header, so it can only be stopped by the
/// nonce being inside the signed scope.
async fn swapping_in_a_fresh_nonce_is_rejected(url: &str, audience: &str) {
    let request = make_volume_request("signature-e2e-nonce-swap");
    let mut captured = mint_v2_headers(audience, "MakeVolume", Some(&canonical_digest(&request)));
    overwrite_header(&mut captured, NONCE_HEADER, &Uuid::new_v4().to_string());

    let result = call_make_volume(url, request, captured).await;
    assert_rejected(
        result,
        Code::Unauthenticated,
        None,
        "a captured mutation resent under a freshly minted nonce",
    );
}

/// Editing the body of a captured request must invalidate it, in the default
/// posture, with no operator flip required.
///
/// The headers are left byte-identical — including the signed digest of the
/// original body — so `check_auth` still passes. Only the handler, recomputing
/// the canonical body from the fields it actually received, can catch this. It
/// is the test that fails if a handler ever loses its digest gate.
async fn tampered_mutation_body_is_rejected(url: &str, audience: &str) {
    let signed = make_volume_request("signature-e2e-original");
    let captured = mint_v2_headers(audience, "MakeVolume", Some(&canonical_digest(&signed)));

    // One byte of the volume name differs from what the digest covers.
    let tampered = make_volume_request("signature-e2e-originaX");
    let result = call_make_volume(url, tampered, captured).await;
    assert_rejected(
        result,
        Code::PermissionDenied,
        Some("RPC content SHA-256 mismatch"),
        "a mutation whose body was edited after signing",
    );
}

/// The body digest must be *inside the signed scope*, not merely cross-checked
/// by the handler.
///
/// This is the same tampered body as above, except the attacker also repairs the
/// digest header so it matches what it sends — defeating the handler's
/// comparison. The only thing left standing is the signature, which covers the
/// digest header itself. Drop `content_sha256` from `update_signature_v2` and
/// this is the test that goes green when it should not.
async fn rewriting_the_digest_to_match_a_tampered_body_is_rejected(url: &str, audience: &str) {
    let signed = make_volume_request("signature-e2e-scope-original");
    let mut captured = mint_v2_headers(audience, "MakeVolume", Some(&canonical_digest(&signed)));

    let tampered = make_volume_request("signature-e2e-scope-originaX");
    overwrite_header(&mut captured, CONTENT_SHA256_HEADER, &canonical_digest(&tampered));

    let result = call_make_volume(url, tampered, captured).await;
    assert_rejected(
        result,
        Code::Unauthenticated,
        None,
        "a tampered mutation whose digest header was repaired to match",
    );
}

/// A signature is bound to its destination node, so a request captured against
/// one node cannot be aimed at another.
///
/// `127.0.0.1:1` stands in for a different peer; the audience is inside the
/// HMAC, so the server's own authority no longer reproduces it.
async fn signature_minted_for_another_node_is_rejected(url: &str) {
    let request = make_volume_request("signature-e2e-wrong-node");
    let headers = mint_v2_headers("127.0.0.1:1", "MakeVolume", Some(&canonical_digest(&request)));
    let result = call_make_volume(url, request, headers).await;
    assert_rejected(result, Code::Unauthenticated, None, "a signature minted for a different node");
}

/// Rolling-upgrade compatibility: a peer that predates v2 must still be served
/// while the strict gates are off.
///
/// This is the case the issue insists must not fail closed during an upgrade.
/// It is also, honestly, the open downgrade window: an attacker can strip the
/// v2 headers and land here too. That window is what
/// [`signature_strict_rejects_legacy_only_downgrade`] closes.
async fn legacy_only_signature_is_accepted_in_default_posture(url: &str) {
    let result = call_make_volume(url, make_volume_request("signature-e2e-legacy"), mint_legacy_only_headers()).await;
    assert_authenticated(result, "a legacy-only signature in the default posture");
}

/// With `RUSTFS_INTERNODE_RPC_SIGNATURE_STRICT` on, the legacy downgrade lane is
/// closed: the exact request accepted in the default posture is now refused.
///
/// The paired v2 positive control rules out "strict simply breaks everything".
#[tokio::test]
#[serial]
async fn signature_strict_rejects_legacy_only_downgrade() -> TestResult {
    init_logging();
    align_rpc_secret_with_server();
    let env = start_server(&[(ENV_INTERNODE_RPC_SIGNATURE_STRICT, "true")]).await?;
    let url = env.url.clone();
    let audience = audience_of(&env);

    let downgraded = call_make_volume(&url, make_volume_request("signature-e2e-strict-legacy"), mint_legacy_only_headers()).await;
    assert_rejected(
        downgraded,
        Code::Unauthenticated,
        None,
        "a legacy-only signature once signature-strict is enabled",
    );

    let request = make_volume_request("signature-e2e-strict-v2");
    let signed = mint_v2_headers(&audience, "MakeVolume", Some(&canonical_digest(&request)));
    assert_authenticated(
        call_make_volume(&url, request, signed).await,
        "a v2-signed mutation under signature-strict",
    );

    stop_server(env, &url).await;
    Ok(())
}

/// With `RUSTFS_INTERNODE_RPC_BODY_DIGEST_STRICT` on, any mutation that arrives
/// without a body digest is refused — including one that downgraded all the way
/// to the legacy signature.
///
/// This gate converges independently of the signature gate, so it is exercised
/// on its own server with signature-strict left off. Both rejected requests
/// clear `check_auth` on their own terms (one is properly v2-signed, the other
/// takes the still-open legacy lane), which is what pins the rejection to the
/// handler's digest gate; the cited message confirms which check spoke.
#[tokio::test]
#[serial]
async fn body_digest_strict_rejects_digestless_mutation() -> TestResult {
    init_logging();
    align_rpc_secret_with_server();
    let env = start_server(&[(ENV_INTERNODE_RPC_BODY_DIGEST_STRICT, "true")]).await?;
    let url = env.url.clone();
    let audience = audience_of(&env);

    let digestless = mint_v2_headers(&audience, "MakeVolume", None);
    assert_rejected(
        call_make_volume(&url, make_volume_request("signature-e2e-digestless"), digestless).await,
        Code::PermissionDenied,
        Some("RPC mutation requires a body-bound v2 signature"),
        "a v2-signed but digestless mutation once body-digest-strict is enabled",
    );

    assert_rejected(
        call_make_volume(&url, make_volume_request("signature-e2e-digestless-legacy"), mint_legacy_only_headers()).await,
        Code::PermissionDenied,
        Some("RPC mutation requires a body-bound v2 signature"),
        "a v1-downgraded mutation once body-digest-strict is enabled",
    );

    let request = make_volume_request("signature-e2e-digest-bound");
    let bound = mint_v2_headers(&audience, "MakeVolume", Some(&canonical_digest(&request)));
    assert_authenticated(
        call_make_volume(&url, request, bound).await,
        "a body-bound mutation under body-digest-strict",
    );

    stop_server(env, &url).await;
    Ok(())
}
