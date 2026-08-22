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

//! Challenge verification and response production for offline enrolment.
//!
//! Two invariants carry the security of this surface and both are easy to break
//! by accident:
//!
//! - Every signature is checked over the octets that arrived, never over a
//!   re-serialised document. Parsing happens only to route the verification, and
//!   nothing a parse yields is believed until the signature over those same
//!   octets has verified.
//! - The enrolment root is the constant in this file. It is never taken from a
//!   challenge, a configuration file, or an operator prompt, so there is no
//!   trust-on-first-use path an operator could be talked into.
//!
//! The order of the checks in [`OfflineEnrollment::verify_challenge`] is frozen
//! by `verificationOrder.enrollmentChallenge` in
//! `protocol/agent/v1/fixtures/offline-enrollment/trust-model.json`, and the
//! signature encoding, the domain separation tags, and every rejection reason
//! are frozen beside it. Reordering the checks changes which reason a given
//! artifact produces, which is itself part of the contract.

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD as BASE64_URL_NO_PAD};
use p256::ecdsa::signature::{Signer as _, Verifier as _};
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use p256::pkcs8::DecodePrivateKey as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

use crate::connect::identity::DeviceIdentity;

/// The hosted enrolment root, compiled in. Both halves are pinned: the
/// fingerprint identifies the root, and the point is what actually verifies the
/// first link, so a build cannot be pointed at a different key by supplying one.
const PINNED_ROOT_KEY_ID: &str = "df22e2806112debbe953672aafa186d699af0e97dd3fd2b09fa8359005fe348f";
const PINNED_ROOT_PUBLIC_KEY: &str = "BFfx-K-FfEA5nK_Rz3IHacvRCkJyQ7JOd1geLyU6HKRZDgNezmVuKhvJ22VhemyjV__Gshk8JGGqOBzYPMD0p6s";

/// Domain separation tags. A document that verifies under one of these must not
/// be accepted for another artifact type, so the tag is part of the signature
/// input rather than a property of the caller.
const TAG_TRUST_LINK: &[u8] = b"rustfs-offline-trust-link-v1";
const TAG_CHALLENGE: &[u8] = b"rustfs-offline-enrollment-challenge-v1";
const TAG_RESPONSE: &[u8] = b"rustfs-offline-enrollment-response-v1";

/// The single octet between the tag and the signed document.
const DOMAIN_SEPARATOR: u8 = 0x00;

const SIGNATURE_ALGORITHM: &str = "ES256";
const PROTOCOL_VERSION: &str = "v1";
const FORMAT_TRUST_LINK: &str = "rustfs.connect.offline.trustLink/1";
const FORMAT_CHALLENGE: &str = "rustfs.connect.offline.enrollmentChallenge/1";
const FORMAT_RESPONSE: &str = "rustfs.connect.offline.enrollmentResponse/1";

/// DER SubjectPublicKeyInfo header for an uncompressed P-256 point. A keyId is
/// the SHA-256 of this prefix followed by the 65 octet point, so the prefix is
/// also how a device public key is recovered from its own DER encoding.
const SPKI_PREFIX: [u8; 26] = [
    0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03,
    0x01, 0x07, 0x03, 0x42, 0x00,
];

/// Order of the P-256 group, and half of it. `r` and `s` must lie in `[1, n)`,
/// and `s` additionally in `[1, n/2]`: ECDSA admits both `s` and `n - s`, and a
/// signature with two spellings cannot serve as an artifact identity. Every
/// ECDSA library accepts the malleated form, so the encoding layer rejects it.
const GROUP_ORDER: [u8; 32] = [
    0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xbc, 0xe6, 0xfa, 0xad, 0xa7,
    0x17, 0x9e, 0x84, 0xf3, 0xb9, 0xca, 0xc2, 0xfc, 0x63, 0x25, 0x51,
];
const MAX_S: [u8; 32] = [
    0x7f, 0xff, 0xff, 0xff, 0x80, 0x00, 0x00, 0x00, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xde, 0x73, 0x7d, 0x56, 0xd3,
    0x8b, 0xcf, 0x42, 0x79, 0xdc, 0xe5, 0x61, 0x7e, 0x31, 0x92, 0xa8,
];

const SCALAR_OCTETS: usize = 32;
const SIGNATURE_OCTETS: usize = 64;
/// 64 octets as unpadded base64url. The length is checked before decoding so
/// that `=` padding, the standard alphabet, DER, and a truncated value are all
/// refused rather than repaired.
const SIGNATURE_VALUE_CHARS: usize = 86;

const PUBLIC_KEY_OCTETS: usize = 65;
const PUBLIC_KEY_CHARS: usize = 87;
/// SEC1 tag of an uncompressed point. Compressed and hybrid forms are refused.
const UNCOMPRESSED_POINT: u8 = 0x04;

const TIMESTAMP_CHARS: usize = 20;

/// The chain is exactly two links: a pinned root issues the intermediate, and
/// the intermediate issues the signing key. Roles are positional and the
/// enumeration is closed.
const CHAIN_LINK_COUNT: usize = 2;
const CHAIN_ROLES: [&str; CHAIN_LINK_COUNT] = ["intermediate", "signing"];

/// Skew allowed on the challenge window. A device may have no synchronised
/// clock at all, so its own reading of "now" is advisory.
const CLOCK_SKEW_TOLERANCE: i64 = 300;

/// Longest life a challenge may claim. The issuer sets both ends of its own
/// window, so the protocol bound is applied on top of the declared expiry
/// rather than trusted from it.
const MAX_CHALLENGE_LIFETIME: i64 = 604_800;

/// A challenge that verified, with the fields the response has to echo.
///
/// Construction is the proof: a value of this type only exists after the chain
/// closed on the pinned root and the challenge signature verified over the
/// received octets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedChallenge {
    pub challenge_id: String,
    pub organization_name: String,
    pub cluster_name: String,
    pub nonce: String,
    pub issued_at: String,
    pub expires_at: String,
    pub connect_key_id: String,
    /// The signature value of the challenge, verbatim. It binds a response to
    /// the one challenge it answers, so it is carried rather than recomputed.
    pub challenge_proof: String,
}

/// Why an offline enrolment artifact was refused.
///
/// The variants are the frozen `reason` vocabulary of
/// `fixtures/offline-enrollment/error-codes.json`, which spans both halves of
/// the exchange. The device half implemented here produces the encoding, chain,
/// version, and freshness reasons; the reasons that describe a response being
/// evaluated against stored state — [`Self::ChallengeUnknown`],
/// [`Self::ChallengeProofInvalid`], [`Self::DeviceProofInvalid`],
/// [`Self::EnrollmentReplayed`], [`Self::OrganizationMismatch`], and
/// [`Self::ClusterMismatch`] — are Connect's to raise and are named here so the
/// two sides share one vocabulary.
///
/// No variant carries a payload: a rejection must never disclose key material,
/// signature octets, nonces, or document bytes.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum EnrollmentError {
    #[error("protocolVersion is missing, malformed, or names an unsupported major version")]
    UnsupportedProtocol,

    #[error("formatVersion is not a supported offline enrollment format")]
    UnsupportedFormat,

    #[error("the signature is not 64 octets of fixed-width r||s in unpadded base64url")]
    SignatureMalformed,

    #[error("the signature is not in its canonical low-S form")]
    SignatureNotCanonical,

    #[error("the signature does not verify over the received octets")]
    SignatureInvalid,

    #[error("the trust chain is not issued by a root pinned in this build")]
    EnrollmentRootUnknown,

    #[error("a trust link is invalid, misordered, or outside its validity at the challenge issuedAt")]
    TrustChainInvalid,

    #[error("connectKeyId is not the subject of the last trust link")]
    ConnectKeyUnchained,

    #[error("no issued challenge matches this challengeId")]
    ChallengeUnknown,

    #[error("the challenge is not yet valid at the evaluation time")]
    ChallengeNotYetValid,

    #[error("the challenge has expired at the evaluation time")]
    ChallengeExpired,

    #[error("the response nonce or challengeProof is not the one issued for this challenge")]
    ChallengeProofInvalid,

    #[error("the response does not prove possession of the device key it presents")]
    DeviceProofInvalid,

    #[error("the challenge was already consumed")]
    EnrollmentReplayed,

    #[error("the response names a different organization than the challenge it answers")]
    OrganizationMismatch,

    #[error("the response names a different cluster than the challenge it answers")]
    ClusterMismatch,

    /// The artifact could not be read as a signed enrolment document at all: the
    /// envelope, the base64 of the signed octets, or a field the frozen order
    /// reads before the signature verifies did not parse. The frozen reason set
    /// has no code for a structurally unreadable document, so this variant maps
    /// to none of them.
    #[error("the offline enrollment document is not well formed")]
    MalformedDocument,

    /// A fault on this side of the exchange rather than in the artifact: the
    /// device key did not round-trip through its own PKCS#8 encoding, or the
    /// caller named an instant outside the representable calendar. Fails closed
    /// because a half-produced response must never reach removable media.
    #[error("the enrollment response could not be produced on this device")]
    ResponseNotProduced,
}

impl EnrollmentError {
    /// The frozen `reason` an operator and Connect both branch on.
    ///
    /// The `Display` message is prose and may be reworded; this is the stable
    /// identifier, so nothing should parse the message instead. The two
    /// variants with no frozen counterpart deliberately return codes outside
    /// the frozen set rather than borrowing the nearest one, so a document that
    /// simply failed to parse can never be reported as a signature or freshness
    /// failure.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::UnsupportedProtocol => "UNSUPPORTED_PROTOCOL",
            Self::UnsupportedFormat => "UNSUPPORTED_FORMAT",
            Self::SignatureMalformed => "SIGNATURE_MALFORMED",
            Self::SignatureNotCanonical => "SIGNATURE_NOT_CANONICAL",
            Self::SignatureInvalid => "SIGNATURE_INVALID",
            Self::EnrollmentRootUnknown => "ENROLLMENT_ROOT_UNKNOWN",
            Self::TrustChainInvalid => "TRUST_CHAIN_INVALID",
            Self::ConnectKeyUnchained => "CONNECT_KEY_UNCHAINED",
            Self::ChallengeUnknown => "CHALLENGE_UNKNOWN",
            Self::ChallengeNotYetValid => "CHALLENGE_NOT_YET_VALID",
            Self::ChallengeExpired => "CHALLENGE_EXPIRED",
            Self::ChallengeProofInvalid => "CHALLENGE_PROOF_INVALID",
            Self::DeviceProofInvalid => "DEVICE_PROOF_INVALID",
            Self::EnrollmentReplayed => "ENROLLMENT_REPLAYED",
            Self::OrganizationMismatch => "ORGANIZATION_MISMATCH",
            Self::ClusterMismatch => "CLUSTER_MISMATCH",
            Self::MalformedDocument => "MALFORMED_DOCUMENT",
            Self::ResponseNotProduced => "RESPONSE_NOT_PRODUCED",
        }
    }
}

/// A signed document, in the shape both directions carry it. `bytes` is
/// standard padded base64 of the exact octets that were signed; nothing else is
/// ever used as the signature input.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignedDocument {
    bytes: String,
    signature: DocumentSignature,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DocumentSignature {
    algorithm: String,
    key_id: String,
    value: String,
}

/// The three fields the frozen order permits reading before anything verifies.
/// They route the verification and are not facts until it has.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChallengeRouting {
    connect_key_id: String,
    issued_at: String,
    trust_chain: Vec<SignedDocument>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChallengeDocument {
    format_version: String,
    protocol_version: String,
    challenge_id: String,
    organization_name: String,
    cluster_name: String,
    nonce: String,
    issued_at: String,
    expires_at: String,
    connect_key_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrustLink {
    format_version: String,
    protocol_version: String,
    role: String,
    issuer_key_id: String,
    subject_key_id: String,
    subject_public_key: String,
    not_before: String,
    not_after: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResponseDocument<'a> {
    format_version: &'a str,
    protocol_version: &'a str,
    challenge_id: &'a str,
    organization_name: &'a str,
    cluster_name: &'a str,
    challenge_nonce: &'a str,
    challenge_proof: &'a str,
    device_key_id: String,
    device_public_key: String,
    device_nonce: String,
    produced_at: String,
}

/// The device half of the offline enrolment exchange: bytes in, bytes out.
pub struct OfflineEnrollment;

impl OfflineEnrollment {
    /// Verify an enrolment challenge and return what a response must echo.
    ///
    /// `now_unix` is the device's reading of the current time, which the clock
    /// skew tolerance treats as advisory.
    pub fn verify_challenge(document: &[u8], now_unix: i64) -> Result<VerifiedChallenge, EnrollmentError> {
        let envelope: SignedDocument = serde_json::from_slice(document).map_err(|_| EnrollmentError::MalformedDocument)?;

        // Step 1: the encoding is checked before anything is decoded from it, so
        // a DER, padded, truncated, out-of-range, or high-S signature is refused
        // on its spelling rather than handed to a library that would accept it.
        let signature = decode_signature(&envelope.signature)?;

        // The octets that were transmitted. They are never re-serialised: every
        // later step signs and parses this same buffer.
        let bytes = BASE64_STANDARD
            .decode(envelope.bytes.as_bytes())
            .map_err(|_| EnrollmentError::MalformedDocument)?;

        // Step 2: routing only.
        let routing: ChallengeRouting = serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::MalformedDocument)?;
        let issued_at = parse_timestamp(&routing.issued_at)?;

        // Steps 3 to 5.
        let connect_key = verify_trust_chain(&routing.trust_chain, &routing.connect_key_id, issued_at)?;

        // Step 6. The verification key comes from the chain, so `signature.keyId`
        // is a label rather than an input: a value naming some other key simply
        // fails to verify here.
        if !verifies(&connect_key, TAG_CHALLENGE, &bytes, &signature) {
            return Err(EnrollmentError::SignatureInvalid);
        }

        // Step 7: only now is the document read as a fact.
        let challenge: ChallengeDocument = serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::MalformedDocument)?;
        if challenge.protocol_version != PROTOCOL_VERSION {
            return Err(EnrollmentError::UnsupportedProtocol);
        }
        if challenge.format_version != FORMAT_CHALLENGE {
            return Err(EnrollmentError::UnsupportedFormat);
        }

        // Step 8.
        let expires_at = parse_timestamp(&challenge.expires_at)?;
        check_challenge_window(issued_at, expires_at, now_unix)?;

        Ok(VerifiedChallenge {
            challenge_id: challenge.challenge_id,
            organization_name: challenge.organization_name,
            cluster_name: challenge.cluster_name,
            nonce: challenge.nonce,
            issued_at: challenge.issued_at,
            expires_at: challenge.expires_at,
            connect_key_id: challenge.connect_key_id,
            challenge_proof: envelope.signature.value,
        })
    }

    /// Build the signed response an operator carries back to Connect.
    ///
    /// `device_nonce` is the response's own replay value and must come from a
    /// cryptographic source. The private key never appears in the result: only
    /// the public point, its fingerprint, and a signature over the document
    /// that presents them, which is what makes presenting the key safe.
    pub fn build_response(
        challenge: &VerifiedChallenge,
        key: &DeviceIdentity,
        device_nonce: &[u8; 32],
        produced_at_unix: i64,
    ) -> Result<Vec<u8>, EnrollmentError> {
        let issued_at = parse_timestamp(&challenge.issued_at)?;
        let expires_at = parse_timestamp(&challenge.expires_at)?;
        // Connect re-checks producedAt against the same window, so a response
        // outside it is refused here rather than written to media and rejected
        // after the operator has carried it out.
        check_challenge_window(issued_at, expires_at, produced_at_unix)?;

        let point = device_public_point(key)?;
        let produced_at = format_timestamp(produced_at_unix)?;

        let document = ResponseDocument {
            format_version: FORMAT_RESPONSE,
            protocol_version: PROTOCOL_VERSION,
            challenge_id: &challenge.challenge_id,
            organization_name: &challenge.organization_name,
            cluster_name: &challenge.cluster_name,
            challenge_nonce: &challenge.nonce,
            challenge_proof: &challenge.challenge_proof,
            device_key_id: key_id(&point),
            device_public_key: BASE64_URL_NO_PAD.encode(point),
            device_nonce: BASE64_URL_NO_PAD.encode(device_nonce),
            produced_at,
        };

        // Serialised once. These octets are what is signed and what is carried,
        // so no second serialisation can disagree with the signature.
        let bytes = serde_json::to_vec(&document).map_err(|_| EnrollmentError::ResponseNotProduced)?;
        let signature = sign(key, TAG_RESPONSE, &bytes)?;

        let envelope = SignedDocument {
            bytes: BASE64_STANDARD.encode(&bytes),
            signature: DocumentSignature {
                algorithm: SIGNATURE_ALGORITHM.to_owned(),
                key_id: document.device_key_id,
                value: signature,
            },
        };

        serde_json::to_vec(&envelope).map_err(|_| EnrollmentError::ResponseNotProduced)
    }
}

/// Walk the chain from the pinned root to the signing key, returning the key
/// `connect_key_id` names once the chain vouches for it.
fn verify_trust_chain(
    chain: &[SignedDocument],
    connect_key_id: &str,
    challenge_issued_at: i64,
) -> Result<VerifyingKey, EnrollmentError> {
    // The pinned root gate runs before the chain's shape is examined, so a
    // chain that is internally consistent under a foreign root — exactly what
    // trust on first use would have accepted — is refused for its root rather
    // than for its length.
    let first = chain.first().ok_or(EnrollmentError::EnrollmentRootUnknown)?;
    let root = decode_trust_link(first)?;
    if root.0.issuer_key_id != PINNED_ROOT_KEY_ID {
        return Err(EnrollmentError::EnrollmentRootUnknown);
    }

    let [_, second] = chain else {
        return Err(EnrollmentError::TrustChainInvalid);
    };
    let links = [root, decode_trust_link(second)?];

    let mut issuer_key_id = PINNED_ROOT_KEY_ID.to_owned();
    let (mut issuer_key, _) = decode_public_key(PINNED_ROOT_PUBLIC_KEY).ok_or(EnrollmentError::EnrollmentRootUnknown)?;

    for (index, ((link, link_bytes), entry)) in links.iter().zip(chain).enumerate() {
        if link.format_version != FORMAT_TRUST_LINK
            || link.protocol_version != PROTOCOL_VERSION
            || link.role != CHAIN_ROLES[index]
            || link.issuer_key_id != issuer_key_id
            // A link that names itself as its own issuer would let a stolen
            // intermediate mint its own root.
            || link.subject_key_id == link.issuer_key_id
        {
            return Err(EnrollmentError::TrustChainInvalid);
        }

        let (subject_key, subject_point) =
            decode_public_key(&link.subject_public_key).ok_or(EnrollmentError::TrustChainInvalid)?;
        if key_id(&subject_point) != link.subject_key_id {
            return Err(EnrollmentError::TrustChainInvalid);
        }

        let signature = decode_signature(&entry.signature)?;
        if !verifies(&issuer_key, TAG_TRUST_LINK, link_bytes, &signature) {
            return Err(EnrollmentError::TrustChainInvalid);
        }

        // The issuer controls both ends of a link's window, so it is evaluated
        // with no skew tolerance, and against the challenge's issuedAt rather
        // than against the device clock: a challenge carries the chain that was
        // valid when it was issued.
        let not_before = parse_timestamp(&link.not_before)?;
        let not_after = parse_timestamp(&link.not_after)?;
        if challenge_issued_at < not_before || challenge_issued_at > not_after {
            return Err(EnrollmentError::TrustChainInvalid);
        }

        issuer_key_id = link.subject_key_id.clone();
        issuer_key = subject_key;
    }

    if issuer_key_id != connect_key_id {
        return Err(EnrollmentError::ConnectKeyUnchained);
    }

    Ok(issuer_key)
}

/// Decode a link and keep the octets it was signed over: the signature is
/// checked against these, never against a re-encoding of the parsed link.
fn decode_trust_link(entry: &SignedDocument) -> Result<(TrustLink, Vec<u8>), EnrollmentError> {
    let bytes = BASE64_STANDARD
        .decode(entry.bytes.as_bytes())
        .map_err(|_| EnrollmentError::MalformedDocument)?;
    let link = serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::TrustChainInvalid)?;
    Ok((link, bytes))
}

/// Check a signature's spelling and range, then admit it.
///
/// `r` and `s` are compared against the group order here rather than left to
/// the ECDSA library, because a library that accepts high-S — every library
/// does — would let a malleated copy of an artifact pass as a second artifact.
fn decode_signature(signature: &DocumentSignature) -> Result<Signature, EnrollmentError> {
    if signature.algorithm != SIGNATURE_ALGORITHM {
        return Err(EnrollmentError::SignatureMalformed);
    }

    let value = signature.value.as_bytes();
    if value.len() != SIGNATURE_VALUE_CHARS || !value.iter().all(|byte| is_base64url(*byte)) {
        return Err(EnrollmentError::SignatureMalformed);
    }

    let decoded = BASE64_URL_NO_PAD
        .decode(value)
        .map_err(|_| EnrollmentError::SignatureMalformed)?;
    let octets: [u8; SIGNATURE_OCTETS] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| EnrollmentError::SignatureMalformed)?;

    // Big-endian octets of equal length order lexicographically exactly as the
    // integers they spell, so a slice comparison is the range check.
    let (r, s) = octets.split_at(SCALAR_OCTETS);
    let out_of_range = |scalar: &[u8]| scalar.iter().all(|byte| *byte == 0) || scalar >= &GROUP_ORDER[..];
    if out_of_range(r) || out_of_range(s) {
        return Err(EnrollmentError::SignatureMalformed);
    }
    if s > &MAX_S[..] {
        return Err(EnrollmentError::SignatureNotCanonical);
    }

    Signature::from_slice(&octets).map_err(|_| EnrollmentError::SignatureMalformed)
}

fn verifies(key: &VerifyingKey, tag: &[u8], bytes: &[u8], signature: &Signature) -> bool {
    key.verify(&signature_input(tag, bytes), signature).is_ok()
}

fn signature_input(tag: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut input = Vec::with_capacity(tag.len() + 1 + bytes.len());
    input.extend_from_slice(tag);
    input.push(DOMAIN_SEPARATOR);
    input.extend_from_slice(bytes);
    input
}

fn sign(key: &DeviceIdentity, tag: &[u8], bytes: &[u8]) -> Result<String, EnrollmentError> {
    // `DeviceIdentity` publishes no general signing operation, so the key is
    // rebuilt from its own PKCS#8 encoding; the encoding is wiped when the
    // wrapper drops.
    let pkcs8 = key.to_pkcs8_der().map_err(|_| EnrollmentError::ResponseNotProduced)?;
    let signing_key = SigningKey::from_pkcs8_der(pkcs8.as_slice()).map_err(|_| EnrollmentError::ResponseNotProduced)?;

    let signature: Signature = signing_key.sign(&signature_input(tag, bytes));
    let canonical = signature.normalize_s().unwrap_or(signature);

    Ok(BASE64_URL_NO_PAD.encode(canonical.to_bytes()))
}

/// The device's public point, recovered from the DER encoding the identity
/// publishes so that one prefix constant governs both the fingerprint and the
/// wire form.
fn device_public_point(key: &DeviceIdentity) -> Result<[u8; PUBLIC_KEY_OCTETS], EnrollmentError> {
    key.public_key_der()
        .strip_prefix(&SPKI_PREFIX)
        .and_then(|point| <[u8; PUBLIC_KEY_OCTETS]>::try_from(point).ok())
        .ok_or(EnrollmentError::ResponseNotProduced)
}

/// Decode an uncompressed SEC1 point and check that it is on the curve.
///
/// The length and alphabet are checked before decoding so that a padded or
/// standard-alphabet spelling is refused, and the point tag is checked so that
/// the compressed and hybrid forms — which no keyId would match — cannot be
/// spelled at all.
fn decode_public_key(value: &str) -> Option<(VerifyingKey, [u8; PUBLIC_KEY_OCTETS])> {
    let value = value.as_bytes();
    if value.len() != PUBLIC_KEY_CHARS || !value.iter().all(|byte| is_base64url(*byte)) {
        return None;
    }

    let point: [u8; PUBLIC_KEY_OCTETS] = BASE64_URL_NO_PAD.decode(value).ok()?.try_into().ok()?;
    if point[0] != UNCOMPRESSED_POINT {
        return None;
    }

    VerifyingKey::from_sec1_bytes(&point).ok().map(|key| (key, point))
}

/// Lowercase SHA-256 hex of the DER SubjectPublicKeyInfo built from a 65 octet
/// uncompressed point.
fn key_id(point: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(SPKI_PREFIX);
    digest.update(point);
    hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower)
}

fn is_base64url(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'
}

/// Parse `YYYY-MM-DDTHH:MM:SSZ` into a Unix instant.
///
/// The shape is checked before the fields are read: offsets other than `Z` and
/// fractional seconds are refused rather than normalised, so two producers
/// cannot spell the same instant two ways.
fn parse_timestamp(value: &str) -> Result<i64, EnrollmentError> {
    let octets = value.as_bytes();
    if octets.len() != TIMESTAMP_CHARS
        || octets[4] != b'-'
        || octets[7] != b'-'
        || octets[10] != b'T'
        || octets[13] != b':'
        || octets[16] != b':'
        || octets[19] != b'Z'
    {
        return Err(EnrollmentError::MalformedDocument);
    }

    let field = |range: std::ops::Range<usize>| -> Result<u32, EnrollmentError> {
        let text = &value[range];
        if !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(EnrollmentError::MalformedDocument);
        }
        text.parse().map_err(|_| EnrollmentError::MalformedDocument)
    };

    let month = Month::try_from(field(5..7)? as u8).map_err(|_| EnrollmentError::MalformedDocument)?;
    let date = Date::from_calendar_date(field(0..4)? as i32, month, field(8..10)? as u8)
        .map_err(|_| EnrollmentError::MalformedDocument)?;
    let clock = Time::from_hms(field(11..13)? as u8, field(14..16)? as u8, field(17..19)? as u8)
        .map_err(|_| EnrollmentError::MalformedDocument)?;

    Ok(PrimitiveDateTime::new(date, clock).assume_utc().unix_timestamp())
}

fn format_timestamp(unix: i64) -> Result<String, EnrollmentError> {
    let moment = OffsetDateTime::from_unix_timestamp(unix).map_err(|_| EnrollmentError::ResponseNotProduced)?;
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        moment.year(),
        u8::from(moment.month()),
        moment.day(),
        moment.hour(),
        moment.minute(),
        moment.second()
    ))
}

/// `at` must fall within `[issuedAt - 300, expiresAt + 300]`.
///
/// The declared expiry is capped at the protocol's maximum challenge lifetime
/// because the issuer sets both ends of its own window; a challenge claiming a
/// longer life expires at the bound.
fn check_challenge_window(issued_at: i64, expires_at: i64, at: i64) -> Result<(), EnrollmentError> {
    if at < issued_at.saturating_sub(CLOCK_SKEW_TOLERANCE) {
        return Err(EnrollmentError::ChallengeNotYetValid);
    }

    let effective_expiry = expires_at.min(issued_at.saturating_add(MAX_CHALLENGE_LIFETIME));
    if at > effective_expiry.saturating_add(CLOCK_SKEW_TOLERANCE) {
        return Err(EnrollmentError::ChallengeExpired);
    }

    Ok(())
}
