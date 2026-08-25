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

//! Login challenges: the token a client echoes back with its second factor.
//!
//! # Why these are stateless
//!
//! The obvious design is a TTL cache, the way the OIDC flow stores its PKCE
//! verifiers. That store is node-local, which is fine for OIDC because the
//! whole authorization round trip returns to the node that started it. A second
//! factor does not: a cluster behind a load balancer without session affinity
//! will issue the challenge on one node and receive the code on another, and a
//! node-local challenge would fail there for reasons no operator could debug.
//!
//! So a challenge carries its own state and a signature over it. Any node
//! validates one without shared storage, and nothing needs replicating.
//!
//! Statelessness costs nothing here because a challenge is not what makes the
//! exchange single-use — the consumed TOTP time step is
//! ([`super::totp::TotpSecret::verify`]). A replayed challenge with a replayed
//! code is refused by the step check; a replayed challenge with a fresh code is
//! just a normal second attempt inside the window, which the rate limiter
//! bounds.

use base64_simd::URL_SAFE_NO_PAD;
use hmac::{Hmac, KeyInit as _, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq as _;

type HmacSha256 = Hmac<Sha256>;

/// How long a challenge stays valid.
///
/// Long enough to fetch a phone and type a code, short enough that one
/// intercepted from a log or a proxy is stale by the time it is useful.
pub const CHALLENGE_TTL_SECONDS: u64 = 300;

/// Domain separator, so a challenge signature can never be mistaken for — or
/// produced by — another HMAC over the same key.
const CHALLENGE_DOMAIN: &[u8] = b"rustfs-mfa-challenge:v1";

/// Wire format version, so a future change to the payload is a decode failure
/// rather than a misparse.
const CHALLENGE_VERSION: u8 = 1;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ChallengeError {
    #[error("the challenge is malformed")]
    Malformed,
    #[error("the challenge signature is invalid")]
    BadSignature,
    #[error("the challenge has expired")]
    Expired,
    #[error("the challenge was issued for a different identity")]
    IdentityMismatch,
}

/// Issue a challenge for `access_key`, valid from `issued_at_unix`.
///
/// `signing_key` must be a server secret; the deployment's root secret key is
/// what the STS token path already uses for the same purpose.
pub fn issue(access_key: &str, issued_at_unix: u64, signing_key: &[u8]) -> String {
    let payload = format!("{CHALLENGE_VERSION}:{issued_at_unix}:{access_key}");
    let signature = sign(&payload, signing_key);
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode_to_string(payload.as_bytes()),
        URL_SAFE_NO_PAD.encode_to_string(signature)
    )
}

/// Validate `challenge` for `access_key` at `now_unix`.
///
/// Checks the signature before anything else, so a forged challenge cannot
/// reach the expiry or identity comparisons and learn from them.
pub fn validate(challenge: &str, access_key: &str, now_unix: u64, signing_key: &[u8]) -> Result<(), ChallengeError> {
    let (payload_b64, signature_b64) = challenge.split_once('.').ok_or(ChallengeError::Malformed)?;

    let payload = URL_SAFE_NO_PAD
        .decode_to_vec(payload_b64.as_bytes())
        .map_err(|_| ChallengeError::Malformed)?;
    let payload = String::from_utf8(payload).map_err(|_| ChallengeError::Malformed)?;
    let signature = URL_SAFE_NO_PAD
        .decode_to_vec(signature_b64.as_bytes())
        .map_err(|_| ChallengeError::Malformed)?;

    let expected = sign(&payload, signing_key);
    if !bool::from(expected.ct_eq(&signature)) {
        return Err(ChallengeError::BadSignature);
    }

    let mut parts = payload.splitn(3, ':');
    let version = parts.next().ok_or(ChallengeError::Malformed)?;
    let issued_at = parts.next().ok_or(ChallengeError::Malformed)?;
    let challenge_access_key = parts.next().ok_or(ChallengeError::Malformed)?;

    if version != CHALLENGE_VERSION.to_string() {
        return Err(ChallengeError::Malformed);
    }

    let issued_at: u64 = issued_at.parse().map_err(|_| ChallengeError::Malformed)?;
    // A challenge stamped in the future is treated as expired rather than
    // accepted: it means the issuing clock is wrong, and honouring it would
    // extend the window by however far off that clock is.
    if now_unix < issued_at || now_unix.saturating_sub(issued_at) > CHALLENGE_TTL_SECONDS {
        return Err(ChallengeError::Expired);
    }

    if !bool::from(challenge_access_key.as_bytes().ct_eq(access_key.as_bytes())) {
        return Err(ChallengeError::IdentityMismatch);
    }

    Ok(())
}

fn sign(payload: &str, signing_key: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(signing_key).expect("HMAC accepts keys of any length");
    mac.update(CHALLENGE_DOMAIN);
    mac.update(&[0]);
    mac.update(payload.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"root-secret-key-for-tests";
    const NOW: u64 = 1_700_000_000;

    #[test]
    fn a_fresh_challenge_validates_for_its_identity() {
        let challenge = issue("sinan", NOW, KEY);
        assert_eq!(validate(&challenge, "sinan", NOW, KEY), Ok(()));
    }

    #[test]
    fn a_challenge_validates_anywhere_in_its_window() {
        let challenge = issue("sinan", NOW, KEY);

        assert_eq!(validate(&challenge, "sinan", NOW, KEY), Ok(()));
        assert_eq!(validate(&challenge, "sinan", NOW + CHALLENGE_TTL_SECONDS, KEY), Ok(()));
    }

    #[test]
    fn an_expired_challenge_is_rejected() {
        let challenge = issue("sinan", NOW, KEY);
        assert_eq!(
            validate(&challenge, "sinan", NOW + CHALLENGE_TTL_SECONDS + 1, KEY),
            Err(ChallengeError::Expired)
        );
    }

    #[test]
    fn a_challenge_from_the_future_is_rejected_rather_than_honoured() {
        // A skewed issuing clock must not silently widen the window.
        let challenge = issue("sinan", NOW + 60, KEY);
        assert_eq!(validate(&challenge, "sinan", NOW, KEY), Err(ChallengeError::Expired));
    }

    #[test]
    fn a_challenge_for_another_identity_is_rejected() {
        // Without this, a caller who can authenticate as one identity could
        // carry its challenge into another identity's verification.
        let challenge = issue("sinan", NOW, KEY);
        assert_eq!(validate(&challenge, "someone-else", NOW, KEY), Err(ChallengeError::IdentityMismatch));
    }

    #[test]
    fn a_challenge_signed_with_another_key_is_rejected() {
        let challenge = issue("sinan", NOW, KEY);
        assert_eq!(validate(&challenge, "sinan", NOW, b"different-key"), Err(ChallengeError::BadSignature));
    }

    #[test]
    fn tampering_with_the_payload_is_rejected() {
        // The point of signing: an attacker must not be able to extend the
        // expiry or swap the identity by editing the token.
        let forged_payload = format!("{CHALLENGE_VERSION}:{}:{}", NOW, "attacker");
        let genuine = issue("sinan", NOW, KEY);
        let (_, signature) = genuine.split_once('.').expect("well-formed challenge");
        let forged = format!("{}.{signature}", URL_SAFE_NO_PAD.encode_to_string(forged_payload.as_bytes()));

        assert_eq!(validate(&forged, "attacker", NOW, KEY), Err(ChallengeError::BadSignature));
    }

    #[test]
    fn tampering_with_the_signature_is_rejected() {
        let challenge = issue("sinan", NOW, KEY);
        let (payload, signature) = challenge.split_once('.').expect("well-formed challenge");
        let mut bytes = URL_SAFE_NO_PAD.decode_to_vec(signature.as_bytes()).expect("decode");
        bytes[0] ^= 0xff;
        let forged = format!("{payload}.{}", URL_SAFE_NO_PAD.encode_to_string(&bytes));

        assert_eq!(validate(&forged, "sinan", NOW, KEY), Err(ChallengeError::BadSignature));
    }

    #[test]
    fn malformed_challenges_are_rejected() {
        for bad in ["", "no-separator", ".", "a.b", "!!!.!!!"] {
            let result = validate(bad, "sinan", NOW, KEY);
            assert!(
                matches!(result, Err(ChallengeError::Malformed) | Err(ChallengeError::BadSignature)),
                "input {bad:?} gave {result:?}"
            );
        }
    }

    #[test]
    fn a_challenge_from_an_unknown_version_is_rejected() {
        // Forward compatibility: an older node must refuse a payload shape it
        // cannot interpret rather than guess at its fields.
        let payload = format!("99:{NOW}:sinan");
        let signature = sign(&payload, KEY);
        let challenge = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode_to_string(payload.as_bytes()),
            URL_SAFE_NO_PAD.encode_to_string(&signature)
        );

        assert_eq!(validate(&challenge, "sinan", NOW, KEY), Err(ChallengeError::Malformed));
    }

    #[test]
    fn any_node_validates_a_challenge_another_node_issued() {
        // The reason challenges are stateless: no shared store is consulted, so
        // a cluster without session affinity still completes the exchange.
        let issuing_node_challenge = issue("sinan", NOW, KEY);
        // A second "node" holds only the same signing key.
        assert_eq!(validate(&issuing_node_challenge, "sinan", NOW + 10, KEY), Ok(()));
    }

    #[test]
    fn access_keys_containing_the_separator_still_round_trip() {
        // The payload splits on the first two colons only, so a colon in the
        // access key cannot truncate the identity.
        let challenge = issue("team:sinan", NOW, KEY);
        assert_eq!(validate(&challenge, "team:sinan", NOW, KEY), Ok(()));
        assert_eq!(validate(&challenge, "team", NOW, KEY), Err(ChallengeError::IdentityMismatch));
    }
}
