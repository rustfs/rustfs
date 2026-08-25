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

//! RFC 6238 time-based one-time passwords.
//!
//! Implemented here rather than pulled in, because the algorithm is short and
//! the workspace already carries every primitive it needs (`hmac`, `sha1`,
//! `rand`, `subtle`). The parameters are fixed at the values every mainstream
//! authenticator app implements — SHA-1, 6 digits, a 30-second step — because
//! an enrollment that a user cannot scan into Google Authenticator, 1Password,
//! Ente Auth or Authy is worthless regardless of how modern its hash is.
//!
//! SHA-1 here is not a collision-resistance claim: HMAC-SHA-1 remains sound as
//! a PRF, which is all TOTP asks of it.

use data_encoding::BASE32_NOPAD;
use hmac::{Hmac, KeyInit as _, Mac};
use rand::Rng as _;
use sha1::Sha1;
use subtle::ConstantTimeEq as _;

type HmacSha1 = Hmac<Sha1>;

/// Hash algorithm advertised in the provisioning URI.
pub const TOTP_ALGORITHM: &str = "SHA1";

/// Digits in a generated code.
pub const TOTP_DIGITS: u8 = 6;

/// Length of one time step, in seconds.
pub const TOTP_PERIOD_SECONDS: u32 = 30;

/// Shared-secret length. RFC 4226 requires at least 128 bits and recommends
/// 160, which is also the HMAC-SHA-1 block-aligned choice.
const TOTP_SECRET_BYTES: usize = 20;

/// Steps of clock skew tolerated on either side of the current one.
///
/// One step (±30s) is the usual compromise: it absorbs realistic phone clock
/// drift and the time a person spends typing, while keeping the number of
/// simultaneously-valid codes at three. Widening this multiplies an attacker's
/// odds per guess by the same factor.
const TOTP_SKEW_STEPS: u64 = 1;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TotpError {
    #[error("the shared secret is not valid unpadded base32")]
    MalformedSecret,
    #[error("the shared secret is too short")]
    SecretTooShort,
    #[error("the submitted code is not {TOTP_DIGITS} digits")]
    MalformedCode,
}

/// A TOTP shared secret.
///
/// Wrapped rather than passed as a bare `String` so a secret cannot be
/// accidentally logged: [`Debug`] is redacted, and the base32 form is only
/// reachable through the explicitly-named [`Self::to_base32`].
#[derive(Clone, PartialEq, Eq)]
pub struct TotpSecret(Vec<u8>);

impl std::fmt::Debug for TotpSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TotpSecret([REDACTED])")
    }
}

impl TotpSecret {
    /// Generate a fresh secret.
    ///
    /// Uses the same CSPRNG the credential generator does (`rand::rng()`, a
    /// thread-local ChaCha seeded from the OS), so a predictable TOTP secret
    /// would require the same failure that already breaks access-key
    /// generation.
    pub fn generate() -> Self {
        let mut bytes = vec![0u8; TOTP_SECRET_BYTES];
        rand::rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Parse a stored or user-typed secret.
    pub fn from_base32(encoded: &str) -> Result<Self, TotpError> {
        // Authenticator apps and humans both introduce spaces and lowercase
        // when a key is transcribed by hand.
        let normalized: String = encoded.chars().filter(|c| !c.is_whitespace()).collect();
        let bytes = BASE32_NOPAD
            .decode(normalized.to_ascii_uppercase().as_bytes())
            .map_err(|_| TotpError::MalformedSecret)?;

        if bytes.len() < 16 {
            return Err(TotpError::SecretTooShort);
        }

        Ok(Self(bytes))
    }

    /// Render for manual entry into an authenticator app.
    pub fn to_base32(&self) -> String {
        BASE32_NOPAD.encode(&self.0)
    }

    /// The code for a given time step.
    fn code_at_step(&self, step: u64) -> u32 {
        // `new_from_slice` only rejects keys for algorithms with a fixed key
        // size; HMAC accepts any length, so this cannot fail here.
        let mut mac = HmacSha1::new_from_slice(&self.0).expect("HMAC accepts keys of any length");
        mac.update(&step.to_be_bytes());
        let digest = mac.finalize().into_bytes();

        // RFC 4226 dynamic truncation.
        let offset = (digest[digest.len() - 1] & 0x0f) as usize;
        let binary = u32::from_be_bytes([
            digest[offset] & 0x7f,
            digest[offset + 1],
            digest[offset + 2],
            digest[offset + 3],
        ]);

        binary % 10u32.pow(TOTP_DIGITS as u32)
    }

    /// The code for a given Unix timestamp. Exposed for tests and for clients
    /// that need to display the current code.
    pub fn code_at(&self, unix_seconds: u64) -> String {
        format_code(self.code_at_step(step_for(unix_seconds)))
    }

    /// Verify `code` against `unix_seconds`, returning the time step it matched.
    ///
    /// The returned step is what makes replay detection possible: the caller
    /// persists it and refuses any later attempt at the same step, so a code
    /// observed in transit cannot be reused inside its own validity window.
    ///
    /// `last_used_step` rejects a match at or before a step already consumed.
    /// Comparison is constant-time, and the candidate steps are all evaluated
    /// so a match late in the window does not take measurably longer than one
    /// early in it.
    pub fn verify(&self, code: &str, unix_seconds: u64, last_used_step: Option<u64>) -> Result<u64, TotpError> {
        let candidate = parse_code(code)?;
        let current = step_for(unix_seconds);

        let mut matched: Option<u64> = None;
        for offset in -(TOTP_SKEW_STEPS as i64)..=(TOTP_SKEW_STEPS as i64) {
            let Some(step) = current.checked_add_signed(offset) else {
                continue;
            };
            if last_used_step.is_some_and(|used| step <= used) {
                continue;
            }
            // `bool::from(..)` on a `Choice`, not `==`: the digest-derived code
            // is compared without an early exit.
            if bool::from(self.code_at_step(step).ct_eq(&candidate)) && matched.is_none() {
                matched = Some(step);
            }
        }

        matched.ok_or(TotpError::MalformedCode)
    }
}

/// Whether `code` has the shape of a TOTP code.
///
/// Used to route a submitted second factor to TOTP or recovery-code
/// verification without asking the user which kind they typed.
pub fn looks_like_totp_code(code: &str) -> bool {
    let trimmed: String = code.chars().filter(|c| !c.is_whitespace()).collect();
    trimmed.len() == TOTP_DIGITS as usize && trimmed.bytes().all(|b| b.is_ascii_digit())
}

fn parse_code(code: &str) -> Result<u32, TotpError> {
    let trimmed: String = code.chars().filter(|c| !c.is_whitespace()).collect();
    if trimmed.len() != TOTP_DIGITS as usize || !trimmed.bytes().all(|b| b.is_ascii_digit()) {
        return Err(TotpError::MalformedCode);
    }
    trimmed.parse::<u32>().map_err(|_| TotpError::MalformedCode)
}

fn format_code(code: u32) -> String {
    format!("{code:0width$}", width = TOTP_DIGITS as usize)
}

/// The time step containing `unix_seconds`.
pub fn step_for(unix_seconds: u64) -> u64 {
    unix_seconds / TOTP_PERIOD_SECONDS as u64
}

/// The `otpauth://` provisioning URI an authenticator app scans.
///
/// `issuer` is repeated in the label and the query parameter: apps disagree on
/// which one they read, and one that reads only the label would otherwise show
/// the account with no issuer at all.
pub fn provisioning_uri(issuer: &str, account: &str, secret: &TotpSecret) -> String {
    let label = format!("{}:{}", encode_uri_component(issuer), encode_uri_component(account));
    format!(
        "otpauth://totp/{label}?secret={secret}&issuer={issuer}&algorithm={TOTP_ALGORITHM}&digits={TOTP_DIGITS}&period={TOTP_PERIOD_SECONDS}",
        secret = secret.to_base32(),
        issuer = encode_uri_component(issuer),
    )
}

/// Percent-encode everything outside the unreserved set.
///
/// Hand-rolled rather than pulled from a URL crate because the input is a label
/// component, not a path: `/`, `:` and `?` must all be escaped here, which the
/// path-oriented encoders in the graph do not do.
fn encode_uri_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => encoded.push(byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The RFC 6238 appendix B secret: the ASCII string "12345678901234567890".
    fn rfc_secret() -> TotpSecret {
        TotpSecret(b"12345678901234567890".to_vec())
    }

    #[test]
    fn matches_rfc6238_sha1_test_vectors() {
        // RFC 6238 Appendix B, the SHA-1 rows, truncated to 6 digits.
        // Anchoring on the published vectors is what proves interoperability
        // with authenticator apps; a self-consistent implementation could be
        // wrong in exactly the same way in both directions.
        let secret = rfc_secret();
        for (unix_seconds, expected_8_digits) in [
            (59u64, "94287082"),
            (1_111_111_109, "07081804"),
            (1_111_111_111, "14050471"),
            (1_234_567_890, "89005924"),
            (2_000_000_000, "69279037"),
            (20_000_000_000, "65353130"),
        ] {
            let expected = &expected_8_digits[expected_8_digits.len() - 6..];
            assert_eq!(secret.code_at(unix_seconds), expected, "at t={unix_seconds}");
        }
    }

    #[test]
    fn generated_secrets_are_distinct_and_round_trip() {
        let first = TotpSecret::generate();
        let second = TotpSecret::generate();
        assert_ne!(first.to_base32(), second.to_base32());

        let parsed = TotpSecret::from_base32(&first.to_base32()).expect("round-trip");
        assert_eq!(parsed, first);
    }

    #[test]
    fn secret_parsing_tolerates_transcription_noise() {
        let secret = rfc_secret();
        let grouped = secret
            .to_base32()
            .to_ascii_lowercase()
            .as_bytes()
            .chunks(4)
            .map(|c| String::from_utf8_lossy(c).to_string())
            .collect::<Vec<_>>()
            .join(" ");

        assert_eq!(TotpSecret::from_base32(&grouped).expect("parse"), secret);
    }

    #[test]
    fn secret_debug_never_reveals_the_secret() {
        let secret = rfc_secret();
        let rendered = format!("{secret:?}");
        assert_eq!(rendered, "TotpSecret([REDACTED])");
        assert!(!rendered.contains(&secret.to_base32()));
    }

    #[test]
    fn short_secrets_are_rejected() {
        // 10 bytes: below the RFC 4226 128-bit floor.
        let short = BASE32_NOPAD.encode(&[0u8; 10]);
        assert_eq!(TotpSecret::from_base32(&short), Err(TotpError::SecretTooShort));
    }

    #[test]
    fn malformed_secrets_are_rejected() {
        assert_eq!(TotpSecret::from_base32("not base32!!!"), Err(TotpError::MalformedSecret));
    }

    #[test]
    fn a_current_code_verifies_and_reports_its_step() {
        let secret = rfc_secret();
        let now = 1_700_000_000;
        let code = secret.code_at(now);

        assert_eq!(secret.verify(&code, now, None), Ok(step_for(now)));
    }

    #[test]
    fn codes_one_step_away_are_accepted() {
        let secret = rfc_secret();
        let now = 1_700_000_000u64;

        let previous = secret.code_at(now - TOTP_PERIOD_SECONDS as u64);
        let next = secret.code_at(now + TOTP_PERIOD_SECONDS as u64);

        assert_eq!(secret.verify(&previous, now, None), Ok(step_for(now) - 1));
        assert_eq!(secret.verify(&next, now, None), Ok(step_for(now) + 1));
    }

    #[test]
    fn codes_two_steps_away_are_rejected() {
        let secret = rfc_secret();
        let now = 1_700_000_000u64;
        let stale = secret.code_at(now - 2 * TOTP_PERIOD_SECONDS as u64);

        assert_eq!(secret.verify(&stale, now, None), Err(TotpError::MalformedCode));
    }

    #[test]
    fn a_consumed_step_cannot_be_replayed() {
        // The core anti-replay property: a code captured in transit stays valid
        // for up to 90 seconds by the clock, so the step must be burned.
        let secret = rfc_secret();
        let now = 1_700_000_000u64;
        let code = secret.code_at(now);
        let step = secret.verify(&code, now, None).expect("first use");

        assert_eq!(secret.verify(&code, now, Some(step)), Err(TotpError::MalformedCode));
    }

    #[test]
    fn a_consumed_step_also_blocks_earlier_steps() {
        // Rejecting `step <= used` rather than `step == used` closes the window
        // where an attacker replays the *previous* step's code after the
        // current one has been consumed.
        let secret = rfc_secret();
        let now = 1_700_000_000u64;
        let previous = secret.code_at(now - TOTP_PERIOD_SECONDS as u64);
        let current_step = step_for(now);

        assert_eq!(secret.verify(&previous, now, Some(current_step)), Err(TotpError::MalformedCode));
    }

    #[test]
    fn a_later_step_still_verifies_after_an_earlier_one_was_consumed() {
        let secret = rfc_secret();
        let now = 1_700_000_000u64;
        let current_step = step_for(now);
        let next = secret.code_at(now + TOTP_PERIOD_SECONDS as u64);

        assert_eq!(secret.verify(&next, now, Some(current_step)), Ok(current_step + 1));
    }

    #[test]
    fn wrong_codes_are_rejected() {
        let secret = rfc_secret();
        let now = 1_700_000_000u64;
        let correct = secret.code_at(now);
        // Perturb one digit rather than using a constant, so the test cannot
        // pass by coincidence.
        let wrong = format_code((correct.parse::<u32>().expect("digits") + 1) % 1_000_000);

        assert_eq!(secret.verify(&wrong, now, None), Err(TotpError::MalformedCode));
    }

    #[test]
    fn malformed_codes_are_rejected_without_consulting_the_secret() {
        let secret = rfc_secret();
        let now = 1_700_000_000u64;

        for bad in ["", "12345", "1234567", "abcdef", "12 34 5", "-12345"] {
            assert_eq!(secret.verify(bad, now, None), Err(TotpError::MalformedCode), "input {bad:?}");
        }
    }

    #[test]
    fn codes_keep_leading_zeros() {
        // A code of 1234 must be shown and accepted as "001234"; dropping the
        // padding is the classic TOTP interop bug.
        assert_eq!(format_code(1234), "001234");
        assert_eq!(format_code(0), "000000");
        assert_eq!(format_code(999_999), "999999");
    }

    #[test]
    fn code_shape_detection_separates_totp_from_recovery_codes() {
        assert!(looks_like_totp_code("123456"));
        assert!(looks_like_totp_code("123 456"));
        assert!(!looks_like_totp_code("ABCD-EFGH-IJKL"));
        assert!(!looks_like_totp_code("12345"));
        assert!(!looks_like_totp_code("1234567"));
    }

    #[test]
    fn provisioning_uri_carries_the_parameters_apps_need() {
        let secret = rfc_secret();
        let uri = provisioning_uri("RustFS", "sinan", &secret);

        assert!(uri.starts_with("otpauth://totp/RustFS:sinan?"), "{uri}");
        assert!(uri.contains(&format!("secret={}", secret.to_base32())));
        assert!(uri.contains("issuer=RustFS"));
        assert!(uri.contains("algorithm=SHA1"));
        assert!(uri.contains("digits=6"));
        assert!(uri.contains("period=30"));
    }

    #[test]
    fn provisioning_uri_escapes_label_separators() {
        // An access key may legitimately contain a colon or a slash, either of
        // which would otherwise split the label and mis-attribute the entry.
        let uri = provisioning_uri("Rust FS", "team:sinan/admin", &TotpSecret::generate());

        assert!(uri.contains("otpauth://totp/Rust%20FS:team%3Asinan%2Fadmin?"), "{uri}");
        assert!(uri.contains("issuer=Rust%20FS"));
    }
}
