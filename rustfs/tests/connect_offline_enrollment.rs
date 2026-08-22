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

//! Offline enrollment conformance against the frozen Connect fixtures.
//!
//! The device half of the air-gapped exchange verifies a challenge Connect
//! signed and produces a response Connect will verify. Neither side can talk to
//! the other while it does so, which means every disagreement about encoding,
//! trust, or clock windows surfaces as a failed enrollment in the field rather
//! than as an error at development time. The fixtures under
//! `protocol/agent/v1/fixtures/offline-enrollment/` are the shared statement of
//! what both sides must do, so this suite replays them rather than restating
//! them: accept vectors must be accepted with the fields the document carries,
//! reject vectors must fail with the single reason `error-codes.json` freezes,
//! and the signature encoding rules in `trust-model.json` must hold even where
//! the underlying ECDSA library is happy.

use std::fs;
use std::path::PathBuf;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL_NO_PAD;
use rustfs::connect::identity::DeviceIdentity;
use rustfs::connect::offline::{EnrollmentError, OfflineEnrollment, VerifiedChallenge};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// DER prefix of a P-256 `SubjectPublicKeyInfo`, frozen by
/// `trust-model.json` as `signature.subjectPublicKeyInfoDerPrefix`. The 65
/// octet uncompressed point follows it, so a SEC1 point published in a fixture
/// becomes a decodable public key by concatenation.
const SPKI_PREFIX_HEX: &str = "3059301306072a8648ce3d020106082a8648ce3d030107034200";

/// `clockSkew.toleranceSeconds` in `trust-model.json`.
const SKEW_TOLERANCE_SECONDS: i64 = 300;

// ---------------------------------------------------------------------------
// Fixture access
// ---------------------------------------------------------------------------

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol/agent/v1/fixtures/offline-enrollment")
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Read one fixture file and refuse it unless its bytes match the digest
/// `MANIFEST.sha256` freezes.
///
/// Every vector in this suite arrives through here. A fixture edited on this
/// side therefore fails the tests that depend on it instead of quietly
/// redefining what conformance means, which is the failure mode a
/// fixture-driven suite is otherwise blind to.
fn read_fixture(name: &str) -> Vec<u8> {
    let dir = fixture_dir();
    let manifest = fs::read_to_string(dir.join("MANIFEST.sha256")).expect("read MANIFEST.sha256");

    let expected = manifest
        .lines()
        .filter(|line| !line.trim().is_empty())
        .find_map(|line| {
            let (digest, file) = line
                .split_once("  ")
                .unwrap_or_else(|| panic!("malformed manifest line: {line}"));
            (file == name).then(|| digest.to_string())
        })
        .unwrap_or_else(|| panic!("{name} is not listed in MANIFEST.sha256"));

    let bytes = fs::read(dir.join(name)).unwrap_or_else(|error| panic!("read {name}: {error}"));
    assert_eq!(sha256_hex(&bytes), expected, "{name} does not match the digest MANIFEST.sha256 freezes");
    bytes
}

fn fixture_json(name: &str) -> Value {
    serde_json::from_slice(&read_fixture(name)).unwrap_or_else(|error| panic!("{name} parses: {error}"))
}

fn accept_vectors() -> Value {
    fixture_json("accept-vectors.json")
}

fn reject_vectors() -> Value {
    fixture_json("reject-vectors.json")
}

fn trust_model() -> Value {
    fixture_json("trust-model.json")
}

fn vector_list(fixture: &Value) -> Vec<Value> {
    fixture["vectors"].as_array().expect("fixture carries a vector list").clone()
}

fn field<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key]
        .as_str()
        .unwrap_or_else(|| panic!("expected a string at '{key}' in {value}"))
}

/// The octets an operator carries in on removable media.
///
/// The fixture's `document` object *is* the transmitted artifact: a padded
/// base64 `bytes` field holding the raw signed octets, plus the detached
/// signature over them. Only `bytes` is covered by the signature, so
/// re-serialising the surrounding envelope here cannot change what a verifier
/// checks.
fn envelope(document: &Value) -> Vec<u8> {
    serde_json::to_vec(document).expect("envelope serialises")
}

/// The raw octets the signature covers, exactly as transmitted.
fn signed_octets(document: &Value) -> Vec<u8> {
    BASE64_STANDARD
        .decode(field(document, "bytes"))
        .expect("document bytes are padded base64")
}

/// The parsed signed document. Parsing is a convenience for the assertions
/// below; the implementation under test is required to verify before it parses.
fn signed_document(document: &Value) -> Value {
    serde_json::from_slice(&signed_octets(document)).expect("signed document parses")
}

fn unix(rfc3339: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .unwrap_or_else(|error| panic!("'{rfc3339}' is not RFC 3339: {error}"))
        .timestamp()
}

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
        .collect()
}

/// Turn a fixture's unpadded-base64url SEC1 point into a usable verifying key.
fn verifying_key(sec1_base64url: &str) -> p256::ecdsa::VerifyingKey {
    let point = BASE64_URL_NO_PAD.decode(sec1_base64url).expect("public key is base64url");
    assert_eq!(point.len(), 65, "the protocol freezes a 65 octet uncompressed SEC1 point");

    let mut der = hex_to_bytes(SPKI_PREFIX_HEX);
    der.extend_from_slice(&point);
    <p256::ecdsa::VerifyingKey as p256::pkcs8::DecodePublicKey>::from_public_key_der(&der).expect("public key decodes")
}

fn published_key(role_or_name: &str) -> Value {
    fixture_json("trust-chain.json")["keys"]
        .as_array()
        .expect("trust chain publishes keys")
        .iter()
        .find(|key| field(key, "name") == role_or_name)
        .unwrap_or_else(|| panic!("trust-chain.json publishes no key named '{role_or_name}'"))
        .clone()
}

/// `signatureInput = domainSeparationTag || 0x00 || the received octets`, the
/// rule `trust-model.json` freezes under `domainSeparation`.
fn signing_input(artifact_tag: &str, received: &[u8]) -> Vec<u8> {
    let mut input = artifact_tag.as_bytes().to_vec();
    input.push(0x00);
    input.extend_from_slice(received);
    input
}

fn domain_tag(artifact: &str) -> String {
    let model = trust_model();
    assert_eq!(
        field(&model["domainSeparation"], "separatorByte"),
        "0x00",
        "the separator byte this suite encodes is the one the trust model freezes"
    );
    field(&model["domainSeparation"]["tags"], artifact).to_string()
}

/// Locate an accept vector by the name other vectors reference it by.
fn accept_vector_named(name: &str) -> Value {
    vector_list(&accept_vectors())
        .into_iter()
        .find(|vector| field(vector, "name") == name)
        .unwrap_or_else(|| panic!("accept-vectors.json carries no vector named '{name}'"))
}

/// Verify the challenge a response vector answers, at that challenge's own
/// evaluation time.
fn answered_challenge(response_vector: &Value) -> (Value, VerifiedChallenge) {
    let challenge_vector = accept_vector_named(field(response_vector, "answersChallenge"));
    let now = unix(field(&challenge_vector, "evaluationTime"));
    let verified = OfflineEnrollment::verify_challenge(&envelope(&challenge_vector["document"]), now)
        .expect("the answered challenge is an accept vector and must verify");
    (challenge_vector, verified)
}

fn device_nonce_of(document: &Value) -> [u8; 32] {
    let raw = BASE64_URL_NO_PAD
        .decode(field(&signed_document(document), "deviceNonce"))
        .expect("deviceNonce is base64url");
    raw.try_into().expect("replay.nonceLengthBytes freezes a 32 octet nonce")
}

// ---------------------------------------------------------------------------
// Accept vectors
// ---------------------------------------------------------------------------

/// Every challenge accept vector must verify at its own evaluation time and
/// expose exactly what the signed document says.
///
/// Two of these vectors sit on the skew boundary — 120 seconds before
/// `issuedAt` and 300 seconds after `expiresAt` — so a verifier that compares
/// against the raw window instead of the tolerated one fails here rather than
/// in an air-gapped data centre. `challenge_proof` is pinned to the challenge's
/// own detached signature value because that is what the response has to echo;
/// deriving it from anything else would silently break the binding.
#[test]
fn every_challenge_accept_vector_verifies_and_exposes_the_signed_fields() {
    let mut verified_count = 0usize;

    for vector in vector_list(&accept_vectors()) {
        if field(&vector, "artifact") != "challenge" {
            continue;
        }

        let name = field(&vector, "name");
        let document = &vector["document"];
        let now = unix(field(&vector, "evaluationTime"));

        let verified = OfflineEnrollment::verify_challenge(&envelope(document), now)
            .unwrap_or_else(|error| panic!("accept vector '{name}' must verify: {}", error.reason()));

        let signed = signed_document(document);
        assert_eq!(verified.challenge_id, field(&signed, "challengeId"), "vector '{name}' challengeId");
        assert_eq!(
            verified.organization_name,
            field(&signed, "organizationName"),
            "vector '{name}' organizationName"
        );
        assert_eq!(verified.cluster_name, field(&signed, "clusterName"), "vector '{name}' clusterName");
        assert_eq!(verified.nonce, field(&signed, "nonce"), "vector '{name}' nonce");
        assert_eq!(verified.issued_at, field(&signed, "issuedAt"), "vector '{name}' issuedAt");
        assert_eq!(verified.expires_at, field(&signed, "expiresAt"), "vector '{name}' expiresAt");
        assert_eq!(verified.connect_key_id, field(&signed, "connectKeyId"), "vector '{name}' connectKeyId");
        assert_eq!(
            verified.challenge_proof,
            field(&document["signature"], "value"),
            "vector '{name}' must carry the challenge's own signature as the proof a response echoes"
        );

        verified_count += 1;
    }

    assert_eq!(
        verified_count, 3,
        "accept-vectors.json publishes three challenge vectors; a fourth is a protocol change"
    );
}

/// Connect's own producer wrote the response accept vectors. Rebuilding them
/// from the challenge they answer, with the device nonce and production time
/// they used, must reproduce every field that does not depend on which device
/// key signed — including the discarded-unknown-field vector, whose extra
/// `telemetryHint` must not survive into anything this side produces.
#[test]
fn response_accept_vectors_are_reproduced_field_for_field_by_build_response() {
    let key = DeviceIdentity::generate();
    let mut reproduced = 0usize;

    for vector in vector_list(&accept_vectors()) {
        if field(&vector, "artifact") != "response" {
            continue;
        }

        let name = field(&vector, "name");
        let published = signed_document(&vector["document"]);
        let (_, challenge) = answered_challenge(&vector);

        let produced_at = unix(field(&published, "producedAt"));
        let nonce = device_nonce_of(&vector["document"]);

        let built_envelope: Value = serde_json::from_slice(
            &OfflineEnrollment::build_response(&challenge, &key, &nonce, produced_at)
                .unwrap_or_else(|error| panic!("vector '{name}' must be reproducible: {}", error.reason())),
        )
        .expect("the built response is JSON");
        let built = signed_document(&built_envelope);

        for shared in [
            "formatVersion",
            "protocolVersion",
            "challengeId",
            "organizationName",
            "clusterName",
            "challengeNonce",
            "challengeProof",
            "deviceNonce",
        ] {
            assert_eq!(
                field(&built, shared),
                field(&published, shared),
                "vector '{name}' field {shared} must match the response Connect published"
            );
        }
        assert_eq!(
            unix(field(&built, "producedAt")),
            produced_at,
            "vector '{name}' producedAt must be the instant it was given"
        );

        // `versioning.additive` says an unknown optional field is discarded and
        // never echoed back; a producer that copied the challenge or a previous
        // response wholesale would carry it forward.
        assert!(
            built.get("telemetryHint").is_none(),
            "vector '{name}' must not echo an unknown optional field"
        );

        reproduced += 1;
    }

    assert_eq!(
        reproduced, 2,
        "accept-vectors.json publishes two response vectors; a third is a protocol change"
    );
}

// ---------------------------------------------------------------------------
// Reject vectors
// ---------------------------------------------------------------------------

/// Every challenge reject vector must fail, and fail for the one reason
/// `error-codes.json` freezes.
///
/// Asserting only that verification failed would pass for an implementation
/// that rejects everything, and would let a tampered document be reported as an
/// expiry — a rejection reason is what an operator acts on, so it is part of the
/// contract rather than a diagnostic detail.
#[test]
fn every_challenge_reject_vector_fails_with_its_frozen_reason() {
    let known_reasons: Vec<String> = fixture_json("error-codes.json")["reasons"]
        .as_array()
        .expect("error-codes.json carries reasons")
        .iter()
        .map(|entry| field(entry, "reason").to_string())
        .collect();

    let mut rejected = 0usize;

    for vector in vector_list(&reject_vectors()) {
        if field(&vector, "artifact") != "challenge" {
            continue;
        }

        let name = field(&vector, "name");
        let expected = field(&vector["expected"], "reason");
        assert!(
            known_reasons.iter().any(|reason| reason == expected),
            "vector '{name}' names reason {expected}, which error-codes.json does not freeze"
        );

        let now = unix(field(&vector, "evaluationTime"));
        let error = OfflineEnrollment::verify_challenge(&envelope(&vector["document"]), now)
            .expect_err(&format!("reject vector '{name}' must not verify"));

        assert_eq!(error.reason(), expected, "vector '{name}' must fail as {expected}");
        rejected += 1;
    }

    assert_eq!(
        rejected, 8,
        "reject-vectors.json publishes eight challenge vectors; losing one silently narrows the suite"
    );
}

/// The response reject vectors are artifacts Connect refuses. This side never
/// verifies a response, so the device-side statement is the stronger one: given
/// the challenge each vector answers, `build_response` must not be capable of
/// emitting that artifact in the first place.
///
/// Each arm pins the specific field a compromised or careless producer would
/// have to get wrong, so an implementation that copied values out of the wrong
/// place — the response's own document, an operator-supplied argument, a
/// previous exchange — fails here.
#[test]
fn response_reject_vectors_are_artifacts_build_response_cannot_emit() {
    let key = DeviceIdentity::generate();
    let mut covered = 0usize;

    for vector in vector_list(&reject_vectors()) {
        if field(&vector, "artifact") != "response" {
            continue;
        }

        let name = field(&vector, "name");
        let refused = signed_document(&vector["document"]);
        let (_, challenge) = answered_challenge(&vector);
        let produced_at = unix(field(&refused, "producedAt"));
        let nonce = device_nonce_of(&vector["document"]);

        let outcome = OfflineEnrollment::build_response(&challenge, &key, &nonce, produced_at);

        match field(&vector["expected"], "reason") {
            // `responseWindow` in trust-model.json: a device that emits a
            // response outside the tolerated challenge window has produced an
            // artifact Connect will refuse, so the refusal belongs here rather
            // than at the far end of a courier run.
            "CHALLENGE_EXPIRED" => {
                let error = outcome.expect_err(&format!("vector '{name}': producing this response must be refused"));
                assert_eq!(error.reason(), "CHALLENGE_EXPIRED", "vector '{name}' must refuse as CHALLENGE_EXPIRED");
                covered += 1;
                continue;
            }
            reason => {
                let built_envelope: Value = serde_json::from_slice(
                    &outcome.unwrap_or_else(|error| panic!("vector '{name}' baseline must build: {}", error.reason())),
                )
                .expect("the built response is JSON");
                let built = signed_document(&built_envelope);

                match reason {
                    "ORGANIZATION_MISMATCH" => {
                        assert_ne!(
                            field(&refused, "organizationName"),
                            challenge.organization_name,
                            "vector '{name}' is only a mismatch if it names another organization"
                        );
                        assert_eq!(
                            field(&built, "organizationName"),
                            challenge.organization_name,
                            "vector '{name}': the organization must come from the challenge, never from elsewhere"
                        );
                    }
                    "CLUSTER_MISMATCH" => {
                        assert_ne!(
                            field(&refused, "clusterName"),
                            challenge.cluster_name,
                            "vector '{name}' is only a mismatch if it names another cluster"
                        );
                        assert_eq!(
                            field(&built, "clusterName"),
                            challenge.cluster_name,
                            "vector '{name}': the cluster must come from the challenge, never from elsewhere"
                        );
                    }
                    "CHALLENGE_PROOF_INVALID" => {
                        // Two distinct vectors land here: a nonce the challenge
                        // never carried, and a proof lifted from another
                        // challenge. Both must be impossible to produce.
                        assert_eq!(
                            field(&built, "challengeNonce"),
                            challenge.nonce,
                            "vector '{name}': the echoed nonce must be the challenge's own"
                        );
                        assert_eq!(
                            field(&built, "challengeProof"),
                            challenge.challenge_proof,
                            "vector '{name}': the proof must be the answered challenge's signature"
                        );
                        assert!(
                            field(&refused, "challengeNonce") != challenge.nonce
                                || field(&refused, "challengeProof") != challenge.challenge_proof,
                            "vector '{name}' must differ from the challenge in nonce or proof to be rejectable"
                        );
                    }
                    "DEVICE_PROOF_INVALID" => {
                        // The refused vector presents one key and is signed by
                        // another; hold the fixture to that claim, then require
                        // the built response to be the opposite. Proof of
                        // possession is the only thing that makes presenting a
                        // key in an unauthenticated document safe.
                        use p256::ecdsa::signature::Verifier as _;

                        let presented = verifying_key(field(&refused, "devicePublicKey"));
                        let raw = BASE64_URL_NO_PAD
                            .decode(field(&vector["document"]["signature"], "value"))
                            .expect("signature is base64url");
                        let signature = p256::ecdsa::Signature::from_slice(&raw).expect("signature parses");
                        assert!(
                            presented
                                .verify(
                                    &signing_input(&domain_tag("enrollmentResponse"), &signed_octets(&vector["document"])),
                                    &signature
                                )
                                .is_err(),
                            "vector '{name}' is only a possession failure if it does not verify under the key it presents"
                        );

                        assert_response_proves_possession(&built_envelope, name);
                    }
                    "UNSUPPORTED_FORMAT" => {
                        assert_ne!(
                            field(&refused, "formatVersion"),
                            field(&built, "formatVersion"),
                            "vector '{name}' is only unsupported if it names another format version"
                        );
                        assert_eq!(
                            field(&built, "formatVersion"),
                            "rustfs.connect.offline.enrollmentResponse/1",
                            "vector '{name}': the format version is frozen"
                        );
                    }
                    "UNSUPPORTED_PROTOCOL" => {
                        assert_ne!(
                            field(&refused, "protocolVersion"),
                            field(&built, "protocolVersion"),
                            "vector '{name}' is only unsupported if it names another protocol major"
                        );
                        assert_eq!(field(&built, "protocolVersion"), "v1", "vector '{name}': the protocol major is frozen");
                    }
                    "ENROLLMENT_REPLAYED" => {
                        // The vector claims to be a byte-identical replay of an
                        // accepted response; hold it to that, because a replay
                        // vector that is not byte identical proves nothing about
                        // single use.
                        let accepted = accept_vector_named("response binding the device public key and the challenge proof");
                        assert_eq!(
                            signed_octets(&vector["document"]),
                            signed_octets(&accepted["document"]),
                            "vector '{name}' must be the accepted response octet for octet"
                        );
                        assert_eq!(
                            field(&vector["document"]["signature"], "value"),
                            field(&accepted["document"]["signature"], "value"),
                            "vector '{name}' must carry the accepted response's signature"
                        );

                        // A fresh device nonce is a different artifact, so a
                        // second enrollment is never mistaken for a replay of
                        // the first.
                        let other = OfflineEnrollment::build_response(&challenge, &key, &[0x5a; 32], produced_at)
                            .expect("a second response builds");
                        assert_ne!(
                            signed_octets(&built_envelope),
                            signed_octets(&serde_json::from_slice::<Value>(&other).expect("JSON")),
                            "vector '{name}': a different device nonce must yield a different artifact"
                        );
                    }
                    other => panic!("vector '{name}' names an unhandled reason {other}; extend this test"),
                }
            }
        }

        covered += 1;
    }

    assert_eq!(
        covered, 9,
        "reject-vectors.json publishes nine response vectors; losing one silently narrows the suite"
    );
}

// ---------------------------------------------------------------------------
// Signature encoding
// ---------------------------------------------------------------------------

/// The high-S malleation is the rejection the whole encoding rule exists for.
///
/// `(r, n - s)` is a second valid signature over the same document under the
/// same key. Every mainstream ECDSA library verifies it, so an implementation
/// that hands the decoded octets straight to `p256` accepts a forged-looking
/// duplicate of a genuine challenge — and because the 64 octets differ, that
/// duplicate is a distinct artifact identity that slips past any deduplication
/// keyed on the signature. This test proves the rejection came from the
/// encoding rule and not from a failed verification: it first shows the
/// malleated signature verifying mathematically, then requires
/// `verify_challenge` to refuse it as SIGNATURE_NOT_CANONICAL.
#[test]
fn malleated_high_s_signature_is_refused_although_it_verifies_mathematically() {
    use p256::ecdsa::signature::Verifier as _;

    let model = trust_model();
    let malleated = model["rejectedSignatureEncodings"]
        .as_array()
        .expect("trust-model.json publishes rejected encodings")
        .iter()
        .find(|entry| field(entry, "reason") == "SIGNATURE_NOT_CANONICAL")
        .expect("trust-model.json publishes the high-S malleation")
        .clone();
    assert!(
        malleated["acceptedByALenientVerifier"].as_bool() == Some(true),
        "this vector is only interesting because a lenient verifier accepts it"
    );

    let vector = accept_vector_named("challenge signed by a chained signing key under the pinned root");
    let genuine_value = field(&vector["document"]["signature"], "value").to_string();
    let malleated_value = field(&malleated, "value").to_string();
    assert_ne!(genuine_value, malleated_value, "the malleation must be a different encoding");

    let genuine = BASE64_URL_NO_PAD.decode(&genuine_value).expect("signature is base64url");
    let raw = BASE64_URL_NO_PAD.decode(&malleated_value).expect("signature is base64url");
    assert_eq!(raw.len(), 64, "the malleation is well formed at 64 octets");
    assert_eq!(raw[..32], genuine[..32], "the malleation shares r with the genuine signature");
    assert_ne!(raw[32..], genuine[32..], "the malleation replaces s with n - s");

    // Step one: the malleated pair really does verify under the signing key, so
    // a verifier cannot be excused for accepting it on mathematical grounds.
    let signature = p256::ecdsa::Signature::from_slice(&raw).expect("the malleated signature parses");
    assert!(signature.normalize_s().is_some(), "the malleated signature must be the high-S form");
    let key = verifying_key(field(&published_key("signing"), "publicKey"));
    let input = signing_input(&domain_tag("enrollmentChallenge"), &signed_octets(&vector["document"]));
    key.verify(&input, &signature)
        .expect("the malleated signature must verify mathematically, or this test proves nothing");

    // Step two: the implementation must refuse it anyway, and say why.
    let mut tampered = vector["document"].clone();
    tampered["signature"]["value"] = Value::String(malleated_value);

    let now = unix(field(&vector, "evaluationTime"));
    let error = OfflineEnrollment::verify_challenge(&envelope(&tampered), now)
        .expect_err("a high-S signature must be refused even though it verifies");
    assert_eq!(
        error.reason(),
        "SIGNATURE_NOT_CANONICAL",
        "a malleated signature is a canonicality failure, not a verification failure"
    );
}

/// Every encoding `trust-model.json` names as rejected must fail with the
/// reason it names — DER, padded base64url, truncation, and out-of-range
/// scalars alongside the malleation. Three of the five are accepted by a
/// lenient verifier, so a single blanket "signature did not verify" answer would
/// be both wrong and undiagnosable.
#[test]
fn every_rejected_signature_encoding_fails_with_its_frozen_reason() {
    let vector = accept_vector_named("challenge signed by a chained signing key under the pinned root");
    let now = unix(field(&vector, "evaluationTime"));
    let model = trust_model();
    let encodings = model["rejectedSignatureEncodings"]
        .as_array()
        .expect("trust-model.json publishes rejected encodings");

    for entry in encodings {
        let name = field(entry, "name");
        let mut tampered = vector["document"].clone();
        tampered["signature"]["value"] = Value::String(field(entry, "value").to_string());

        let error: EnrollmentError = OfflineEnrollment::verify_challenge(&envelope(&tampered), now)
            .err()
            .unwrap_or_else(|| panic!("rejected encoding '{name}' must not verify"));

        assert_eq!(error.reason(), field(entry, "reason"), "rejected encoding '{name}'");
    }

    assert_eq!(encodings.len(), 5, "trust-model.json freezes five rejected encodings");
}

// ---------------------------------------------------------------------------
// Clock window
// ---------------------------------------------------------------------------

/// The tolerated window is `[issuedAt - 300, expiresAt + 300]`, inclusive at
/// both ends. An air-gapped device has no synchronised clock, so an
/// off-by-one here either strands a legitimate enrollment or widens the window
/// a stolen challenge stays usable in. Both ends are checked at the exact bound
/// and one second past it, and the reason distinguishes the two directions.
#[test]
fn challenge_is_accepted_at_the_exact_skew_bound_and_refused_one_second_past_it() {
    let vector = accept_vector_named("challenge signed by a chained signing key under the pinned root");
    let document = envelope(&vector["document"]);
    let signed = signed_document(&vector["document"]);

    let issued_at = unix(field(&signed, "issuedAt"));
    let expires_at = unix(field(&signed, "expiresAt"));

    let earliest = issued_at - SKEW_TOLERANCE_SECONDS;
    OfflineEnrollment::verify_challenge(&document, earliest).expect("the earliest tolerated instant is inside the window");
    let error =
        OfflineEnrollment::verify_challenge(&document, earliest - 1).expect_err("one second earlier is outside the window");
    assert_eq!(error.reason(), "CHALLENGE_NOT_YET_VALID");

    let latest = expires_at + SKEW_TOLERANCE_SECONDS;
    OfflineEnrollment::verify_challenge(&document, latest).expect("the latest tolerated instant is inside the window");
    let error = OfflineEnrollment::verify_challenge(&document, latest + 1).expect_err("one second later is outside the window");
    assert_eq!(error.reason(), "CHALLENGE_EXPIRED");
}

// ---------------------------------------------------------------------------
// Response production
// ---------------------------------------------------------------------------

/// Assert a built response proves possession of the key it presents: the
/// fingerprint matches the presented key, and the detached signature is a
/// canonical low-S ES256 signature that verifies under that key over the exact
/// octets transmitted.
fn assert_response_proves_possession(built_envelope: &Value, label: &str) {
    use p256::ecdsa::signature::Verifier as _;

    let raw = signed_octets(built_envelope);
    let built = signed_document(built_envelope);
    let signature_block = &built_envelope["signature"];

    assert_eq!(field(signature_block, "algorithm"), "ES256", "{label}: the algorithm is frozen");

    let value = field(signature_block, "value");
    assert_eq!(value.len(), 86, "{label}: the transfer encoding is 86 unpadded base64url characters");
    assert!(
        value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "{label}: the signature must use the base64url alphabet with no padding"
    );

    let bytes = BASE64_URL_NO_PAD.decode(value).expect("signature is base64url");
    assert_eq!(bytes.len(), 64, "{label}: the signature is a fixed-width r || s");
    let signature = p256::ecdsa::Signature::from_slice(&bytes).expect("signature parses");
    assert!(
        signature.normalize_s().is_none(),
        "{label}: this side must never emit the malleated high-S form it refuses to accept"
    );

    let presented = field(&built, "devicePublicKey");
    let key = verifying_key(presented);
    key.verify(&signing_input(&domain_tag("enrollmentResponse"), &raw), &signature)
        .unwrap_or_else(|error| panic!("{label}: the response must verify under the key it presents: {error}"));

    // `signature.keyIdAlgorithm`: the lowercase SHA-256 of the DER
    // SubjectPublicKeyInfo, not of the bare point and not of the transfer
    // encoding.
    let mut spki = hex_to_bytes(SPKI_PREFIX_HEX);
    spki.extend_from_slice(&BASE64_URL_NO_PAD.decode(presented).expect("public key is base64url"));
    let fingerprint = sha256_hex(&spki);
    assert_eq!(
        field(&built, "deviceKeyId"),
        fingerprint,
        "{label}: deviceKeyId must be the fingerprint of the key the document presents"
    );
    assert_eq!(
        field(signature_block, "keyId"),
        fingerprint,
        "{label}: the detached signature must name the same key"
    );
}

/// A response is the only thing Connect will ever see from this device, so it
/// has to carry the whole binding on its own: the challenge it answers, the
/// proof that challenge was genuine, the key being enrolled, and possession of
/// that key.
#[test]
fn built_response_binds_the_challenge_proof_and_proves_possession_of_the_device_key() {
    let vector = accept_vector_named("response binding the device public key and the challenge proof");
    let (challenge_vector, challenge) = answered_challenge(&vector);
    let key = DeviceIdentity::generate();
    let produced_at = unix(field(&signed_document(&vector["document"]), "producedAt"));

    let bytes = OfflineEnrollment::build_response(&challenge, &key, &[0x11; 32], produced_at).expect("the response builds");
    let built_envelope: Value = serde_json::from_slice(&bytes).expect("the response is JSON");
    let built = signed_document(&built_envelope);

    assert_response_proves_possession(&built_envelope, "built response");

    // The proof is the challenge's own detached signature. A producer that
    // echoed the nonce alone, or hashed something, would let a response be
    // built from an unverified challenge.
    assert_eq!(
        field(&built, "challengeProof"),
        field(&challenge_vector["document"]["signature"], "value"),
        "the proof must be the signature of the challenge being answered"
    );
    assert_eq!(field(&built, "challengeNonce"), challenge.nonce);
    assert_eq!(field(&built, "challengeId"), challenge.challenge_id);

    assert_eq!(
        field(&built, "devicePublicKey"),
        BASE64_URL_NO_PAD.encode(&key.public_key_der()[hex_to_bytes(SPKI_PREFIX_HEX).len()..]),
        "the presented key must be the key that was passed in"
    );
    assert_eq!(
        field(&built, "deviceNonce"),
        BASE64_URL_NO_PAD.encode([0x11; 32]),
        "the device nonce must be the one that was passed in"
    );
    assert!(field(&built, "producedAt").ends_with('Z'), "producedAt is a UTC RFC 3339 instant");
}

/// The response leaves the air gap on removable media and is read by anyone who
/// handles it. A producer that serialised the key pair instead of the public
/// key, or logged a debug rendering into the document, would put the enrolled
/// private key on that medium — and the enrollment would still succeed, so
/// nothing else in this suite would notice.
#[test]
fn built_response_carries_no_private_key_material() {
    let vector = accept_vector_named("response binding the device public key and the challenge proof");
    let (_, challenge) = answered_challenge(&vector);
    let key = DeviceIdentity::generate();
    let produced_at = unix(field(&signed_document(&vector["document"]), "producedAt"));

    let response = OfflineEnrollment::build_response(&challenge, &key, &[0x22; 32], produced_at).expect("the response builds");

    // The envelope carries the signed document base64-encoded, so a needle
    // present in the document is not present in the envelope octets. Both
    // layers are searched: an operator handling the medium can read either.
    let envelope_value: Value = serde_json::from_slice(&response).expect("the response is JSON");
    let mut haystack = response;
    haystack.extend_from_slice(&signed_octets(&envelope_value));

    let pkcs8 = key.to_pkcs8_der().expect("serialise the key");
    let secret = <p256::SecretKey as p256::pkcs8::DecodePrivateKey>::from_pkcs8_der(&pkcs8).expect("the key parses");
    let scalar = secret.to_bytes();

    // Every spelling the scalar could plausibly reach a document in: raw, and
    // the three encodings this protocol already uses elsewhere.
    let scalar_hex: String = scalar.iter().map(|byte| format!("{byte:02x}")).collect();
    for (description, needle) in [
        ("the PKCS#8 encoding", pkcs8.to_vec()),
        ("the raw private scalar", scalar.to_vec()),
        ("the scalar in base64url", BASE64_URL_NO_PAD.encode(scalar).into_bytes()),
        ("the scalar in standard base64", BASE64_STANDARD.encode(scalar).into_bytes()),
        ("the scalar in hex", scalar_hex.into_bytes()),
    ] {
        assert!(
            !haystack.windows(needle.len()).any(|window| window == needle.as_slice()),
            "the response must not contain {description}"
        );
    }

    // The public half must be there, so the absence above is a statement about
    // what was excluded rather than about a haystack that would not have found
    // the private half either.
    let point = BASE64_URL_NO_PAD.encode(&key.public_key_der()[hex_to_bytes(SPKI_PREFIX_HEX).len()..]);
    assert!(
        haystack.windows(point.len()).any(|window| window == point.as_bytes()),
        "the response must still present the public key"
    );
}

// ---------------------------------------------------------------------------
// The offline invariant
// ---------------------------------------------------------------------------

/// The whole surface exists because there is no network. This asserts that
/// three different ways, because no single one of them is conclusive on its own.
///
/// 1. The process opens no descriptor across a full verify-and-respond cycle. A
///    socket, a DNS resolver, a pooled HTTP client, or a revocation-list fetch
///    all show up here — including one that is opened and cached rather than
///    opened and closed, which is what a lazily built client does.
/// 2. The cycle is a pure byte transform: the same inputs produce the same
///    verified fields, and the evaluation instant is an argument rather than an
///    ambient read, so nothing about the outcome can depend on reachability.
/// 3. Repeating the cycle changes nothing observable, so a first call cannot be
///    quietly initialising shared state that a later one reuses.
#[cfg(unix)]
#[test]
fn enrollment_opens_no_descriptor_and_is_a_pure_byte_transform() {
    let vector = accept_vector_named("challenge signed by a chained signing key under the pinned root");
    let document = envelope(&vector["document"]);
    let now = unix(field(&vector, "evaluationTime"));
    let key = DeviceIdentity::generate();

    // Warm anything the test harness itself lazily opens before the baseline.
    let _ = open_descriptors();
    let baseline = open_descriptors();
    assert!(
        !baseline.is_empty(),
        "the descriptor table must be readable for this test to mean anything"
    );

    let mut fields = Vec::new();
    for _ in 0..2 {
        let challenge = OfflineEnrollment::verify_challenge(&document, now).expect("the challenge verifies");
        let response = OfflineEnrollment::build_response(&challenge, &key, &[0x33; 32], now).expect("the response builds");
        fields.push((
            challenge.challenge_id.clone(),
            challenge.nonce.clone(),
            challenge.challenge_proof.clone(),
            signed_octets(&serde_json::from_slice::<Value>(&response).expect("JSON")),
        ));
    }

    assert_eq!(
        open_descriptors(),
        baseline,
        "the enrollment path must not open a descriptor: no socket, no resolver, no cached client"
    );

    let (first, second) = (&fields[0], &fields[1]);
    assert_eq!(first.0, second.0, "verification must be deterministic");
    assert_eq!(first.1, second.1, "verification must be deterministic");
    assert_eq!(first.2, second.2, "verification must be deterministic");
    assert_eq!(
        first.3, second.3,
        "the signed response octets are a function of the challenge, the key, the nonce, and the instant"
    );
}

#[cfg(unix)]
fn open_descriptors() -> Vec<String> {
    // Linux publishes the table at /proc/self/fd; the BSDs and macOS at /dev/fd.
    let path = if PathBuf::from("/proc/self/fd").is_dir() {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };

    let mut entries: Vec<String> = fs::read_dir(path)
        .unwrap_or_else(|error| panic!("read {path}: {error}"))
        .map(|entry| entry.expect("read dir entry").file_name().to_string_lossy().into_owned())
        .collect();
    entries.sort();
    entries
}

/// A descriptor count taken around a call cannot see a socket that was opened
/// and closed inside it, so the invariant is also asserted where it can be
/// stated absolutely: the implementation names no network API at all.
///
/// This is the shape the regression actually takes — someone adds a
/// revocation-list fetch, a time-server check, or a "just confirm the challenge
/// with Connect" call — and it is caught at the source rather than by observing
/// its effects.
#[test]
fn enrollment_implementation_names_no_network_api() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/connect/offline/enrollment.rs");
    let source = fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));

    // Prose is allowed to discuss the invariant it is documenting, so only code
    // is scanned.
    let code: String = source
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");

    for forbidden in [
        "std::net",
        "tokio::net",
        "TcpStream",
        "TcpListener",
        "UdpSocket",
        "UnixStream",
        "ToSocketAddrs",
        "reqwest",
        "hyper",
        "tonic",
    ] {
        assert!(
            !code.contains(forbidden),
            "offline enrollment must not reach the network, but the implementation names {forbidden}"
        );
    }
}
