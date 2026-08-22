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

//! Device key, certificate request, and registration proof of possession.
//!
//! The transcript and signature rules implemented here are frozen by
//! `protocol/agent/v1/registration-proof.md` and by the golden fixtures under
//! `protocol/agent/v1/fixtures/registration/`. Connect verifies what this
//! module produces, so any divergence is a protocol break rather than a
//! local behaviour change.

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD as BASE64_URL_NO_PAD};
use p256::ecdsa::signature::Signer as _;
use p256::ecdsa::{Signature, SigningKey};
use p256::pkcs8::{DecodePrivateKey as _, EncodePrivateKey as _, LineEnding};
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

/// The 30 US-ASCII octets that open every registration transcript. Case is
/// significant: a lowercase spelling is a different transcript, and the
/// protocol publishes it as a reject vector so the two can never be confused.
const REGISTRATION_DOMAIN: &[u8] = b"RUSTFS-CONNECT-REGISTRATION-V1";

/// Separator between a field's decimal octet length and its value.
const FIELD_SEPARATOR: u8 = b':';

/// Terminator after the domain and after every field value, including the last.
const FIELD_TERMINATOR: u8 = b'\n';

/// The transcript binds exactly seven fields, always present, always in order.
const FIELD_COUNT: usize = 7;

/// The one algorithm this surface accepts. The enumeration is closed: an
/// unrecognised value is refused rather than discarded.
pub const PROOF_ALGORITHM: &str = "ES256";

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// A transcript field carried an octet the encoding cannot represent
    /// unambiguously. The transcript is length-prefixed, so a newline inside a
    /// value would still parse; it is refused because a caller that can place
    /// one can shift the boundary a verifier reconstructs from its own row.
    #[error("registration transcript field {field} is not printable US-ASCII without a line feed")]
    UnencodableField { field: &'static str },

    /// An expiry that predates the epoch cannot be spelled without a sign, and
    /// the length rule admits no sign.
    #[error("registration token expiry {expires_unix} is negative")]
    NegativeExpiry { expires_unix: i64 },

    #[error("device key is not a valid P-256 private key: {0}")]
    MalformedKey(String),

    #[error("failed to generate the device certificate request: {0}")]
    CertificateRequest(String),
}

/// The canonical byte sequence a device signs, and its digest.
///
/// Built, never parsed: nothing reads a transcript back, so there is no such
/// thing as a malformed one once it has been constructed.
#[derive(Clone)]
pub struct RegistrationTranscript {
    bytes: Vec<u8>,
}

impl std::fmt::Debug for RegistrationTranscript {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The transcript embeds the challenge nonce, which is disclosed to an
        // operator exactly once beside the token secret and is deliberately
        // never republished. Rendering the octets would put it into any log or
        // panic message that formats a transcript, so only the length and the
        // digest — both already public in the fixtures — are shown.
        f.debug_struct("RegistrationTranscript")
            .field("len", &self.bytes.len())
            .field("sha256", &self.sha256_hex())
            .finish()
    }
}

impl RegistrationTranscript {
    /// Assemble the transcript from the seven bound values.
    ///
    /// Five of them reach the device out of band with the token secret and are
    /// never sent back, which is what stops a device choosing its own
    /// transcript. They cross an operator-supplied boundary, so each one is
    /// checked here rather than trusted.
    pub fn build(
        registration_token_uid: &str,
        organization_uid: &str,
        cluster_uid: &str,
        request_id: &str,
        challenge_nonce: &str,
        expires_unix: i64,
        certificate_request: &[u8],
    ) -> Result<Self, IdentityError> {
        if expires_unix < 0 {
            return Err(IdentityError::NegativeExpiry { expires_unix });
        }

        let expiry = expires_unix.to_string();
        let csr_digest = BASE64_URL_NO_PAD.encode(Sha256::digest(certificate_request));

        let fields: [(&'static str, &str); FIELD_COUNT] = [
            ("registrationTokenUid", registration_token_uid),
            ("organizationUid", organization_uid),
            ("clusterUid", cluster_uid),
            ("requestId", request_id),
            ("challengeNonce", challenge_nonce),
            ("expiresUnix", &expiry),
            ("certificateRequestSha256", &csr_digest),
        ];

        let mut bytes = Vec::with_capacity(REGISTRATION_DOMAIN.len() + 1 + 320);
        bytes.extend_from_slice(REGISTRATION_DOMAIN);
        bytes.push(FIELD_TERMINATOR);

        for (name, value) in fields {
            if !value.is_ascii() || value.as_bytes().contains(&FIELD_TERMINATOR) {
                return Err(IdentityError::UnencodableField { field: name });
            }
            // The length is the octet count, and `is_ascii` above makes octets
            // and characters the same count for these values.
            bytes.extend_from_slice(value.len().to_string().as_bytes());
            bytes.push(FIELD_SEPARATOR);
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(FIELD_TERMINATOR);
        }

        Ok(Self { bytes })
    }

    /// The exact octets that are signed.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// SHA-256 over the transcript, as lowercase hex. Published beside the
    /// canonical string in `transcript.json` so a producer can prove its
    /// builder without performing any cryptography.
    pub fn sha256_hex(&self) -> String {
        let digest = Sha256::digest(&self.bytes);
        digest.iter().fold(String::with_capacity(64), |mut out, byte| {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
            out
        })
    }
}

/// A proof of possession, in the shape the exchange body carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistrationProof {
    pub algorithm: String,
    /// 86 base64url characters, unpadded, decoding to a fixed-width 64 octet
    /// `r || s`.
    pub value: String,
}

/// A device's P-256 key and the operations that key authorises.
///
/// The private key never leaves this type: it is not exposed by a getter, not
/// rendered by `Debug`, and not written anywhere except the sealed store.
pub struct DeviceIdentity {
    signing_key: SigningKey,
}

impl std::fmt::Debug for DeviceIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A device identity is a private key. Rendering any part of it, even a
        // fingerprint, puts key-derived material into logs and support bundles.
        f.write_str("DeviceIdentity(<redacted>)")
    }
}

impl DeviceIdentity {
    /// Generate a fresh P-256 key.
    pub fn generate() -> Self {
        // p256 is pinned to rand_core 0.6 while the workspace `rand` is 0.10, so
        // the RNG comes from p256's own re-export rather than the workspace one.
        Self {
            signing_key: SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng),
        }
    }

    /// Load a key from its PKCS#8 DER encoding. A key that does not decode is
    /// an error rather than a reason to mint a replacement: silently
    /// regenerating would strand the certificate already issued for the old one.
    pub fn from_pkcs8_der(der: &[u8]) -> Result<Self, IdentityError> {
        SigningKey::from_pkcs8_der(der)
            .map(|signing_key| Self { signing_key })
            .map_err(|error| IdentityError::MalformedKey(error.to_string()))
    }

    /// Serialise the key for the sealed store. The result is wrapped so it is
    /// wiped when the caller drops it.
    pub fn to_pkcs8_der(&self) -> Result<Zeroizing<Vec<u8>>, IdentityError> {
        self.signing_key
            .to_pkcs8_der()
            .map(|der| Zeroizing::new(der.as_bytes().to_vec()))
            .map_err(|error| IdentityError::MalformedKey(error.to_string()))
    }

    pub(crate) fn to_pkcs8_pem(&self) -> Result<Zeroizing<String>, IdentityError> {
        self.signing_key
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|error| IdentityError::MalformedKey(error.to_string()))
    }

    /// Build the PKCS#10 certificate request Connect consumes.
    ///
    /// Connect reads the request for its SubjectPublicKeyInfo and its
    /// self-signature and for nothing else: it assigns the device uid itself,
    /// so the subject and SAN carried here name nothing Connect will honour.
    pub fn certificate_request_der(&self) -> Result<Vec<u8>, IdentityError> {
        let pkcs8 = self.to_pkcs8_der()?;
        let key_pair =
            rcgen::KeyPair::try_from(pkcs8.as_slice()).map_err(|error| IdentityError::CertificateRequest(error.to_string()))?;

        let params = rcgen::CertificateParams::default();
        let request = params
            .serialize_request(&key_pair)
            .map_err(|error| IdentityError::CertificateRequest(error.to_string()))?;

        Ok(request.der().to_vec())
    }

    /// Standard padded base64 of the certificate request, as the body carries it.
    pub fn certificate_request_base64(&self) -> Result<String, IdentityError> {
        Ok(BASE64_STANDARD.encode(self.certificate_request_der()?))
    }

    /// Sign a transcript, producing the low-S fixed-width proof.
    ///
    /// ECDSA admits two valid spellings of every signature, and a proof with
    /// two spellings is not an identity, so `s` is normalised into the lower
    /// half of the group order before encoding.
    pub fn sign_registration(&self, transcript: &RegistrationTranscript) -> RegistrationProof {
        let signature: Signature = self.signing_key.sign(transcript.as_bytes());
        let canonical = signature.normalize_s().unwrap_or(signature);

        RegistrationProof {
            algorithm: PROOF_ALGORITHM.to_string(),
            value: BASE64_URL_NO_PAD.encode(canonical.to_bytes()),
        }
    }

    /// The device public key, DER SubjectPublicKeyInfo.
    pub fn public_key_der(&self) -> Vec<u8> {
        use p256::pkcs8::EncodePublicKey as _;

        self.signing_key
            .verifying_key()
            .to_public_key_der()
            .expect("a P-256 verifying key always encodes as SubjectPublicKeyInfo")
            .as_bytes()
            .to_vec()
    }
}
