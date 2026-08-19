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

//! Connect device identity: transcript conformance, key durability, and the
//! properties the registration exchange depends on.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL_NO_PAD;
use rustfs::connect::identity::{DeviceIdentity, IdentityError, RegistrationTranscript};
use rustfs::connect::identity_store::{IdentityStore, StoreError};

fn transcript_fixture() -> serde_json::Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol/agent/v1/fixtures/registration/transcript.json");
    serde_json::from_slice(&fs::read(path).expect("read transcript.json")).expect("transcript.json parses")
}

fn accept_vectors() -> serde_json::Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../protocol/agent/v1/fixtures/registration/accept-vectors.json");
    serde_json::from_slice(&fs::read(path).expect("read accept-vectors.json")).expect("accept-vectors.json parses")
}

/// Extract the SubjectPublicKeyInfo from a PKCS#10 request.
///
/// The protocol freezes the DER prefix of a P-256 SubjectPublicKeyInfo, and the
/// key that follows it is a 65 octet uncompressed point, so the whole structure
/// is a fixed 91 octets located by its prefix. This is a test reading a fixture,
/// not a parser: Connect owns certificate request parsing.
fn subject_public_key_info(csr_der: &[u8]) -> Vec<u8> {
    let prefix = hex_to_bytes("3059301306072a8648ce3d020106082a8648ce3d030107034200");
    let start = csr_der
        .windows(prefix.len())
        .position(|window| window == prefix)
        .expect("certificate request carries a P-256 SubjectPublicKeyInfo");
    csr_der[start..start + prefix.len() + 65].to_vec()
}

/// Rebuild each accept vector's transcript from the values a verifier holds.
///
/// This is the interoperability assertion the protocol asks a producer to make:
/// the five hidden fields come from the token row, the two visible ones from the
/// request, and the result must equal the transcript Connect published.
#[test]
fn transcript_reproduces_every_accept_vector() {
    let vectors = accept_vectors();
    let list = vectors["vectors"].as_array().expect("accept vectors are a list");
    assert!(!list.is_empty(), "the accept vector set must not be empty");

    for vector in list {
        let name = vector["name"].as_str().unwrap_or("<unnamed>");
        let token = &vector["tokenRecord"];
        let request = &vector["request"];

        let csr = base64::engine::general_purpose::STANDARD
            .decode(
                request["certificateRequest"]
                    .as_str()
                    .expect("vector carries a certificate request"),
            )
            .expect("certificate request is base64");

        let transcript = RegistrationTranscript::build(
            token["registrationTokenUid"].as_str().unwrap(),
            token["organizationUid"].as_str().unwrap(),
            token["clusterUid"].as_str().unwrap(),
            request["requestId"].as_str().unwrap(),
            token["challengeNonce"].as_str().unwrap(),
            token["expiresUnix"].as_i64().unwrap(),
            &csr,
        )
        .unwrap_or_else(|error| panic!("vector '{name}' must build: {error}"));

        assert_eq!(
            transcript.as_bytes(),
            vector["serverTranscript"].as_str().unwrap().as_bytes(),
            "vector '{name}' transcript must match octet for octet"
        );
        assert_eq!(
            transcript.sha256_hex(),
            vector["serverTranscriptSha256"].as_str().unwrap(),
            "vector '{name}' transcript digest must match"
        );
    }
}

/// The published proofs were produced by the Connect-side implementation over
/// keys this repository does not hold. Verifying them against a transcript this
/// module rebuilt is the strongest available statement that the two
/// implementations agree: a single wrong octet anywhere in the transcript makes
/// real ECDSA verification fail.
#[test]
fn published_proofs_verify_over_locally_rebuilt_transcripts() {
    use p256::ecdsa::signature::Verifier as _;

    let vectors = accept_vectors();
    let mut verified = 0usize;

    for vector in vectors["vectors"].as_array().expect("accept vectors are a list") {
        let name = vector["name"].as_str().unwrap_or("<unnamed>");
        if vector["expected"]["verifiesMathematically"].as_bool() != Some(true) {
            continue;
        }

        let token = &vector["tokenRecord"];
        let request = &vector["request"];
        let csr = base64::engine::general_purpose::STANDARD
            .decode(request["certificateRequest"].as_str().unwrap())
            .expect("certificate request is base64");

        let transcript = RegistrationTranscript::build(
            token["registrationTokenUid"].as_str().unwrap(),
            token["organizationUid"].as_str().unwrap(),
            token["clusterUid"].as_str().unwrap(),
            request["requestId"].as_str().unwrap(),
            token["challengeNonce"].as_str().unwrap(),
            token["expiresUnix"].as_i64().unwrap(),
            &csr,
        )
        .expect("transcript builds");

        let raw = BASE64_URL_NO_PAD
            .decode(request["proof"]["value"].as_str().expect("vector carries a proof"))
            .expect("proof decodes");
        let signature = p256::ecdsa::Signature::from_slice(&raw).expect("signature parses");
        assert!(
            signature.normalize_s().is_none(),
            "vector '{name}' publishes a proof that is already low-S"
        );

        let verifying =
            <p256::ecdsa::VerifyingKey as p256::pkcs8::DecodePublicKey>::from_public_key_der(&subject_public_key_info(&csr))
                .expect("public key decodes");

        verifying
            .verify(transcript.as_bytes(), &signature)
            .unwrap_or_else(|error| panic!("vector '{name}' proof must verify over the rebuilt transcript: {error}"));
        verified += 1;
    }

    assert!(verified > 0, "no accept vector was cross-verified");
}

/// Drive the builder with the golden example's own inputs, using the accept
/// vector whose certificate request produces the digest it publishes.
fn transcript_from_fixture_inputs(csr_octets: &[u8]) -> Result<RegistrationTranscript, IdentityError> {
    let fixture = transcript_fixture();
    let inputs = &fixture["example"]["inputs"];

    RegistrationTranscript::build(
        inputs["registrationTokenUid"].as_str().unwrap(),
        inputs["organizationUid"].as_str().unwrap(),
        inputs["clusterUid"].as_str().unwrap(),
        inputs["requestId"].as_str().unwrap(),
        inputs["challengeNonce"].as_str().unwrap(),
        inputs["expiresUnix"].as_i64().unwrap(),
        csr_octets,
    )
}

fn csr_octets_matching_golden_digest() -> Vec<u8> {
    let want = transcript_fixture()["example"]["inputs"]["certificateRequestSha256"]
        .as_str()
        .unwrap()
        .to_string();

    for vector in accept_vectors()["vectors"].as_array().expect("accept vectors are a list") {
        let Some(encoded) = vector["request"]["certificateRequest"].as_str() else {
            continue;
        };
        let der = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("certificate request is base64");
        let digest = BASE64_URL_NO_PAD.encode(<sha2::Sha256 as sha2::Digest>::digest(&der));
        if digest == want {
            return der;
        }
    }

    panic!("no accept vector carries the certificate request the golden example digests");
}

#[test]
fn transcript_reproduces_the_golden_example_byte_for_byte() {
    let fixture = transcript_fixture();
    let example = &fixture["example"];

    let transcript = transcript_from_fixture_inputs(&csr_octets_matching_golden_digest()).expect("golden inputs build");

    assert_eq!(
        transcript.as_bytes(),
        example["canonicalTranscript"].as_str().unwrap().as_bytes(),
        "the canonical transcript must match octet for octet"
    );
    assert_eq!(
        transcript.as_bytes().len() as u64,
        example["canonicalTranscriptLengthBytes"].as_u64().unwrap(),
        "the transcript length is frozen"
    );
    assert_eq!(
        transcript.sha256_hex(),
        example["canonicalTranscriptSha256"].as_str().unwrap(),
        "the transcript digest is frozen"
    );
}

#[test]
fn transcript_refuses_a_field_carrying_the_terminator() {
    // A newline inside a value would move the boundary a verifier rebuilds
    // from its own token row, which is the substitution the encoding exists to
    // prevent. Length-prefixing alone would still parse it.
    let error = RegistrationTranscript::build(
        "0198f4b0-6f00-7b60-9271-7d8e9fa0b1c5",
        "0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70\n36:evil",
        "0198f4b0-2b00-7d20-9e31-3f4a5b6c7d81",
        "3f2a1c94-5b6d-4e8f-9a0b-1c2d3e4f5a6b",
        "a3f1c07d9b2e4856af0c1d3b5e7f9012c4a6b8d0e2f4061738495a6b7c8d9e0f",
        1_787_228_100,
        b"csr",
    )
    .expect_err("a field carrying 0x0a must be refused");

    assert!(
        matches!(error, IdentityError::UnencodableField { field } if field == "organizationUid"),
        "unexpected error: {error}"
    );
}

#[test]
fn transcript_refuses_a_non_ascii_field() {
    let error = RegistrationTranscript::build(
        "0198f4b0-6f00-7b60-9271-7d8e9fa0b1c5",
        "0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70",
        "0198f4b0-2b00-7d20-9e31-3f4a5b6c7d81",
        "3f2a1c94-5b6d-4e8f-9a0b-1c2d3e4f5a6b",
        // Multi-byte input would make the octet length and the character count
        // disagree, which is the exact confusion the length rule forbids.
        "a3f1c07d9b2e4856af0c1d3b5e7f9012c4a6b8d0e2f4061738495a6b7c8d9e0é",
        1_787_228_100,
        b"csr",
    )
    .expect_err("a non-ASCII field must be refused");

    assert!(
        matches!(error, IdentityError::UnencodableField { field } if field == "challengeNonce"),
        "unexpected error: {error}"
    );
}

#[test]
fn transcript_refuses_a_negative_expiry() {
    let error = transcript_negative_expiry().expect_err("a negative expiry has no unsigned spelling");
    assert!(
        matches!(error, IdentityError::NegativeExpiry { expires_unix: -1 }),
        "unexpected error: {error}"
    );
}

fn transcript_negative_expiry() -> Result<RegistrationTranscript, IdentityError> {
    RegistrationTranscript::build(
        "0198f4b0-6f00-7b60-9271-7d8e9fa0b1c5",
        "0198f4b0-1a00-7c10-8d21-2e3f4a5b6c70",
        "0198f4b0-2b00-7d20-9e31-3f4a5b6c7d81",
        "3f2a1c94-5b6d-4e8f-9a0b-1c2d3e4f5a6b",
        "a3f1c07d9b2e4856af0c1d3b5e7f9012c4a6b8d0e2f4061738495a6b7c8d9e0f",
        -1,
        b"csr",
    )
}

#[test]
fn proof_is_a_canonical_low_s_signature_that_verifies() {
    use p256::ecdsa::signature::Verifier as _;

    let identity = DeviceIdentity::generate();
    let csr = identity.certificate_request_der().expect("certificate request builds");
    let transcript = transcript_from_fixture_inputs(&csr).expect("transcript builds");

    let proof = identity.sign_registration(&transcript);
    assert_eq!(proof.algorithm, "ES256");
    assert_eq!(proof.value.len(), 86, "the transfer encoding is 86 unpadded base64url characters");
    assert!(
        proof
            .value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "the proof must use the base64url alphabet with no padding"
    );

    let raw = BASE64_URL_NO_PAD.decode(&proof.value).expect("proof decodes");
    assert_eq!(raw.len(), 64, "the signature is a fixed-width r || s");

    let signature = p256::ecdsa::Signature::from_slice(&raw).expect("signature parses");
    assert!(
        signature.normalize_s().is_none(),
        "s must already be in the lower half of the group order"
    );

    let spki = identity.public_key_der();
    let verifying =
        <p256::ecdsa::VerifyingKey as p256::pkcs8::DecodePublicKey>::from_public_key_der(&spki).expect("public key decodes");
    verifying
        .verify(transcript.as_bytes(), &signature)
        .expect("the proof must verify over the transcript octets");
}

#[test]
fn proof_does_not_verify_over_a_different_transcript() {
    use p256::ecdsa::signature::Verifier as _;

    let identity = DeviceIdentity::generate();
    let csr = identity.certificate_request_der().expect("certificate request builds");
    let transcript = transcript_from_fixture_inputs(&csr).expect("transcript builds");
    let proof = identity.sign_registration(&transcript);

    // A different certificate request is a different artifact and therefore a
    // different transcript; this is the proof-of-possession binding itself.
    let other = transcript_from_fixture_inputs(b"a different certificate request").expect("transcript builds");
    assert_ne!(transcript.as_bytes(), other.as_bytes());

    let raw = BASE64_URL_NO_PAD.decode(&proof.value).expect("proof decodes");
    let signature = p256::ecdsa::Signature::from_slice(&raw).expect("signature parses");
    let verifying = <p256::ecdsa::VerifyingKey as p256::pkcs8::DecodePublicKey>::from_public_key_der(&identity.public_key_der())
        .expect("public key decodes");

    assert!(
        verifying.verify(other.as_bytes(), &signature).is_err(),
        "a proof must not carry over to another transcript"
    );
}

#[test]
fn certificate_request_presents_a_p256_key() {
    let identity = DeviceIdentity::generate();
    let der = identity.certificate_request_der().expect("certificate request builds");

    // The prefix the protocol freezes for a P-256 SubjectPublicKeyInfo. Its
    // presence proves the request carries the curve Connect requires.
    let spki_prefix = hex_to_bytes("3059301306072a8648ce3d020106082a8648ce3d030107034200");
    assert!(
        der.windows(spki_prefix.len()).any(|window| window == spki_prefix),
        "the certificate request must present an ECDSA P-256 SubjectPublicKeyInfo"
    );
    assert_eq!(der[0], 0x30, "a PKCS#10 request is a DER SEQUENCE");
}

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
        .collect()
}

#[test]
fn unenrolled_deployment_holds_no_identity_and_reading_creates_none() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = IdentityStore::new(dir.path().join("connect"));

    assert!(store.load().expect("load succeeds").is_none(), "an unenrolled server has no identity");
    assert!(
        !dir.path().join("connect").exists(),
        "reading must not create the store directory, let alone a key"
    );
}

#[test]
fn identity_survives_restart_and_retry_does_not_mint_a_second() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = IdentityStore::new(dir.path());

    let first = store.load_or_create().expect("first create");
    let first_key = first.public_key_der();

    // A restart is a fresh store over the same directory.
    let reopened = IdentityStore::new(dir.path());
    let second = reopened.load_or_create().expect("second create");

    assert_eq!(first_key, second.public_key_der(), "a retry must return the original identity");
}

#[test]
fn concurrent_initialisation_converges_on_one_identity() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();
    let started = Arc::new(AtomicUsize::new(0));

    // Every thread must be spawned before any is joined: the barrier below
    // makes each one wait for all eight, so joining as we spawn would both
    // serialise the race this test exists to create and deadlock on the first
    // thread. A lazy iterator chain here is not equivalent.
    let mut handles = Vec::with_capacity(8);
    for _ in 0..8 {
        let path = path.clone();
        let started = Arc::clone(&started);
        handles.push(std::thread::spawn(move || {
            // Line the threads up so publication actually races.
            started.fetch_add(1, Ordering::SeqCst);
            while started.load(Ordering::SeqCst) < 8 {
                std::hint::spin_loop();
            }
            IdentityStore::new(&path).load_or_create().expect("create").public_key_der()
        }));
    }

    let keys: Vec<Vec<u8>> = handles.into_iter().map(|handle| handle.join().expect("thread")).collect();

    assert!(
        keys.windows(2).all(|pair| pair[0] == pair[1]),
        "every concurrent initialiser must observe the same device identity"
    );
}

#[test]
fn corrupt_key_is_refused_and_left_on_disk() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = IdentityStore::new(dir.path());
    store.load_or_create().expect("create");

    let key_path = store.key_path();
    fs::write(&key_path, b"not a pkcs8 key").expect("corrupt the key");
    set_mode(&key_path, 0o600);

    let error = store.load().expect_err("a corrupt key must fail closed");
    assert!(matches!(error, StoreError::Corrupt { .. }), "unexpected error: {error}");

    // Regenerating would strand a certificate the control plane still trusts,
    // so the damaged file has to survive for an operator to inspect.
    assert_eq!(fs::read(&key_path).expect("key still present"), b"not a pkcs8 key");
}

#[cfg(unix)]
#[test]
fn key_is_sealed_and_widened_permissions_are_refused() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir().expect("temp dir");
    let store = IdentityStore::new(dir.path());
    store.load_or_create().expect("create");

    let key_path = store.key_path();
    let mode = fs::metadata(&key_path).expect("metadata").permissions().mode() & 0o7777;
    assert_eq!(mode, 0o600, "the device key must be owner-only");

    set_mode(&key_path, 0o644);
    let error = store.load().expect_err("a world-readable key must be refused");
    assert!(matches!(error, StoreError::Permissions { mode: 0o644, .. }), "unexpected error: {error}");
}

#[cfg(unix)]
fn set_mode(path: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("set mode");
}

#[cfg(not(unix))]
fn set_mode(_path: &std::path::Path, _mode: u32) {}

#[test]
fn unwritable_directory_fails_closed_without_publishing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store_dir = dir.path().join("sealed");
    fs::create_dir(&store_dir).expect("create store dir");
    set_mode(&store_dir, 0o500);

    let store = IdentityStore::new(&store_dir);
    let result = store.load_or_create();

    set_mode(&store_dir, 0o700);

    #[cfg(unix)]
    {
        assert!(result.is_err(), "an unwritable store must not silently succeed");
        assert!(!store.key_path().exists(), "no key may be published when the write failed");
    }
    #[cfg(not(unix))]
    let _ = result;
}

#[test]
fn stored_key_round_trips_through_pkcs8() {
    let identity = DeviceIdentity::generate();
    let der = identity.to_pkcs8_der().expect("serialise");
    let reloaded = DeviceIdentity::from_pkcs8_der(&der).expect("deserialise");

    assert_eq!(identity.public_key_der(), reloaded.public_key_der(), "the key must survive a round trip");
}

#[test]
fn device_identity_does_not_render_key_material() {
    let identity = DeviceIdentity::generate();
    assert_eq!(format!("{identity:?}"), "DeviceIdentity(<redacted>)");
}
