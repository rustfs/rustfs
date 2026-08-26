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

//! Single-use recovery codes.
//!
//! The escape hatch for a lost authenticator. Codes are shown once at
//! generation and stored only as hashes, so losing the store does not hand an
//! attacker a working second factor and RustFS cannot show a user their codes
//! again — only replace them.
//!
//! # Why a plain hash and not a password KDF
//!
//! A code carries [`RECOVERY_CODE_ENTROPY_BITS`] bits of uniform randomness, so
//! the attacks a slow KDF defends against do not apply: there is no dictionary
//! to try and no human-chosen pattern to exploit, and a brute-force search of a
//! 100-bit space stays infeasible against a fast hash. Meanwhile a memory-hard
//! KDF would have to run once per stored code on every verification attempt,
//! which turns each guess into an attacker-controlled multiple of that cost.
//! This is the standard treatment for high-entropy bearer tokens, and the same
//! reasoning is why a per-code salt is absent: with no dictionary and no
//! repeated values, a salt would protect nothing.

use rand::Rng as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use time::OffsetDateTime;

/// How many codes a generation produces.
///
/// Enough that a user can lose a few without being locked out, few enough that
/// a verification attempt only ever compares against a short list.
pub const RECOVERY_CODE_COUNT: usize = 10;

/// Characters per group, and groups per code.
const GROUP_LEN: usize = 4;
const GROUP_COUNT: usize = 5;

/// Significant characters in one code.
const CODE_LEN: usize = GROUP_LEN * GROUP_COUNT;

/// Entropy per code: one alphabet symbol is 5 bits.
pub const RECOVERY_CODE_ENTROPY_BITS: usize = CODE_LEN * 5;

/// Crockford base32: base32 with `I`, `L`, `O` and `U` removed.
///
/// Dropping them means a handwritten code cannot be ambiguous between `1`/`I`/`L`
/// or `0`/`O`, and `U` is excluded so a random code cannot spell an unfortunate
/// word. Exactly 32 symbols, so indexing with `byte % 32` is unbiased.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Domain separator, so a hash from this scheme can never be confused with one
/// computed over the same bytes for another purpose.
const HASH_DOMAIN: &[u8] = b"rustfs-recovery-code:v1";

/// A stored recovery code: its hash, and whether it has been spent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredRecoveryCode {
    /// Lowercase hex of the domain-separated SHA-256 digest.
    pub hash: String,
    /// When this code was consumed. `None` while it is still usable.
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub used_at: Option<OffsetDateTime>,
}

impl StoredRecoveryCode {
    pub const fn is_available(&self) -> bool {
        self.used_at.is_none()
    }
}

/// A freshly generated set: the plaintext to show once, and what to persist.
#[derive(Debug)]
pub struct GeneratedRecoveryCodes {
    /// Formatted for display. Never persisted, never logged.
    pub plaintext: Vec<String>,
    pub stored: Vec<StoredRecoveryCode>,
}

/// Generate a fresh set of codes.
pub fn generate() -> GeneratedRecoveryCodes {
    let mut rng = rand::rng();
    let mut plaintext = Vec::with_capacity(RECOVERY_CODE_COUNT);
    let mut stored = Vec::with_capacity(RECOVERY_CODE_COUNT);

    for _ in 0..RECOVERY_CODE_COUNT {
        let mut bytes = [0u8; CODE_LEN];
        rng.fill_bytes(&mut bytes);

        let symbols: Vec<u8> = bytes.iter().map(|byte| ALPHABET[(*byte % 32) as usize]).collect();
        let formatted = symbols
            .chunks(GROUP_LEN)
            .map(|chunk| String::from_utf8_lossy(chunk).to_string())
            .collect::<Vec<_>>()
            .join("-");

        stored.push(StoredRecoveryCode {
            hash: hash_code(&formatted),
            used_at: None,
        });
        plaintext.push(formatted);
    }

    GeneratedRecoveryCodes { plaintext, stored }
}

/// Hash a code for storage or comparison.
///
/// Normalizes first, so a code retyped in lowercase, without dashes, or with
/// `O` for `0` still hashes to the stored value.
pub fn hash_code(code: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(HASH_DOMAIN);
    hasher.update([0u8]);
    hasher.update(normalize(code).as_bytes());
    hex_lower(&hasher.finalize())
}

/// Whether `code` could be a recovery code.
///
/// Used to route a submitted second factor without asking the user which kind
/// they typed. Deliberately shape-only: it says nothing about validity.
pub fn looks_like_recovery_code(code: &str) -> bool {
    let normalized = normalize(code);
    normalized.len() == CODE_LEN && normalized.bytes().all(|b| ALPHABET.contains(&b))
}

/// Outcome of consuming a code against a stored set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeOutcome {
    /// The code matched an unused entry, now marked spent at `index`.
    Consumed { index: usize, remaining: u32 },
    /// The code matched an entry that had already been spent.
    AlreadyUsed,
    /// No entry matched.
    NoMatch,
}

/// Spend `code` against `codes`, marking the matching entry used.
///
/// Every entry is examined and the comparison is constant-time, so neither the
/// timing nor the outcome reveals *which* code was close. A code that matches an
/// already-spent entry is reported distinctly from one that matches nothing:
/// the caller audits them the same way but an operator investigating a
/// compromise needs to tell "replayed a used code" from "guessed wrong".
pub fn consume(codes: &mut [StoredRecoveryCode], code: &str, now: OffsetDateTime) -> ConsumeOutcome {
    let candidate = hash_code(code);

    let mut matched_unused: Option<usize> = None;
    let mut matched_used = false;

    for (index, stored) in codes.iter().enumerate() {
        if !bool::from(stored.hash.as_bytes().ct_eq(candidate.as_bytes())) {
            continue;
        }
        if stored.is_available() {
            if matched_unused.is_none() {
                matched_unused = Some(index);
            }
        } else {
            matched_used = true;
        }
    }

    if let Some(index) = matched_unused {
        codes[index].used_at = Some(now);
        return ConsumeOutcome::Consumed {
            index,
            remaining: remaining(codes),
        };
    }

    if matched_used {
        return ConsumeOutcome::AlreadyUsed;
    }

    ConsumeOutcome::NoMatch
}

/// How many codes are still usable.
pub fn remaining(codes: &[StoredRecoveryCode]) -> u32 {
    codes.iter().filter(|code| code.is_available()).count() as u32
}

/// Strip formatting and fold the characters Crockford treats as equivalent.
fn normalize(code: &str) -> String {
    code.chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .map(|c| match c.to_ascii_uppercase() {
            // Crockford's decode aliases: a handwritten code must survive being
            // read back by a human.
            'O' => '0',
            'I' | 'L' => '1',
            other => other,
        })
        .collect()
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp")
    }

    #[test]
    fn generation_produces_the_expected_shape() {
        let generated = generate();

        assert_eq!(generated.plaintext.len(), RECOVERY_CODE_COUNT);
        assert_eq!(generated.stored.len(), RECOVERY_CODE_COUNT);

        for code in &generated.plaintext {
            // Five groups of four, dash-separated, as the UI renders them.
            let groups: Vec<&str> = code.split('-').collect();
            assert_eq!(groups.len(), GROUP_COUNT, "code {code} has the wrong group count");
            for group in groups {
                assert_eq!(group.len(), GROUP_LEN, "group {group} has the wrong length");
                assert!(group.bytes().all(|b| ALPHABET.contains(&b)), "group {group} has a stray symbol");
            }
        }
    }

    #[test]
    fn codes_carry_the_documented_entropy() {
        assert_eq!(RECOVERY_CODE_ENTROPY_BITS, 100);
    }

    #[test]
    fn generated_codes_are_distinct() {
        let generated = generate();
        let unique: HashSet<&String> = generated.plaintext.iter().collect();
        assert_eq!(unique.len(), RECOVERY_CODE_COUNT, "generation produced a duplicate");
    }

    #[test]
    fn the_alphabet_excludes_ambiguous_characters() {
        for excluded in *b"ILOU" {
            assert!(!ALPHABET.contains(&excluded), "{} must not be in the alphabet", excluded as char);
        }
        assert_eq!(ALPHABET.len(), 32, "indexing with `% 32` requires exactly 32 symbols");
    }

    #[test]
    fn plaintext_is_never_recoverable_from_what_is_stored() {
        let generated = generate();
        for (code, stored) in generated.plaintext.iter().zip(&generated.stored) {
            let normalized = normalize(code);
            assert!(!stored.hash.contains(code), "stored hash leaks the formatted code");
            assert!(!stored.hash.contains(&normalized), "stored hash leaks the normalized code");
            assert_eq!(stored.hash.len(), 64, "expected a hex SHA-256 digest");
        }
    }

    #[test]
    fn a_generated_code_can_be_consumed_once() {
        let generated = generate();
        let mut stored = generated.stored;
        let code = &generated.plaintext[3];

        assert_eq!(
            consume(&mut stored, code, now()),
            ConsumeOutcome::Consumed {
                index: 3,
                remaining: (RECOVERY_CODE_COUNT - 1) as u32
            }
        );
        assert_eq!(stored[3].used_at, Some(now()));
    }

    #[test]
    fn a_consumed_code_cannot_be_reused() {
        // The single-use property, which is the whole point of the `used_at`
        // column: an attacker who saw a code being typed must not be able to
        // reuse it.
        let generated = generate();
        let mut stored = generated.stored;
        let code = &generated.plaintext[0];

        consume(&mut stored, code, now());
        assert_eq!(consume(&mut stored, code, now()), ConsumeOutcome::AlreadyUsed);
        assert_eq!(remaining(&stored), (RECOVERY_CODE_COUNT - 1) as u32);
    }

    #[test]
    fn an_unknown_code_does_not_match() {
        let generated = generate();
        let mut stored = generated.stored;

        assert_eq!(consume(&mut stored, "ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ", now()), ConsumeOutcome::NoMatch);
        assert_eq!(remaining(&stored), RECOVERY_CODE_COUNT as u32);
    }

    #[test]
    fn consuming_does_not_disturb_the_other_codes() {
        let generated = generate();
        let mut stored = generated.stored;

        consume(&mut stored, &generated.plaintext[5], now());

        for (index, entry) in stored.iter().enumerate() {
            assert_eq!(entry.is_available(), index != 5, "entry {index} changed unexpectedly");
        }
    }

    #[test]
    fn codes_verify_after_realistic_transcription() {
        let generated = generate();
        let code = &generated.plaintext[1];

        for variant in [
            code.to_ascii_lowercase(),
            code.replace('-', ""),
            code.replace('-', " "),
            format!("  {code}  "),
        ] {
            let mut stored = generated.stored.clone();
            assert!(
                matches!(consume(&mut stored, &variant, now()), ConsumeOutcome::Consumed { .. }),
                "variant {variant:?} should verify"
            );
        }
    }

    #[test]
    fn crockford_aliases_are_folded() {
        // A code containing 0 or 1 must still verify when a human writes O or I.
        assert_eq!(normalize("O1IL-0000-0000-0000-0000"), "0111000000000000 0000".replace(' ', ""));
        assert_eq!(hash_code("o1il-0000-0000-0000-0000"), hash_code("0111-0000-0000-0000-0000"));
    }

    #[test]
    fn shape_detection_separates_recovery_codes_from_totp_codes() {
        let generated = generate();

        assert!(looks_like_recovery_code(&generated.plaintext[0]));
        assert!(looks_like_recovery_code(&generated.plaintext[0].to_ascii_lowercase()));
        assert!(!looks_like_recovery_code("123456"));
        assert!(!looks_like_recovery_code(""));
        // Right length, wrong alphabet.
        assert!(!looks_like_recovery_code("UUUU-UUUU-UUUU-UUUU-UUUU"));
    }

    #[test]
    fn hashing_is_domain_separated() {
        // The same bytes hashed without the domain prefix must not collide with
        // this scheme's output.
        let code = "ABCD-EFGH-JKMN-PQRS-TVWX";
        let mut bare = Sha256::new();
        bare.update(normalize(code).as_bytes());

        assert_ne!(hash_code(code), hex_lower(&bare.finalize()));
    }

    #[test]
    fn remaining_counts_only_unused_codes() {
        let generated = generate();
        let mut stored = generated.stored;
        assert_eq!(remaining(&stored), RECOVERY_CODE_COUNT as u32);

        for index in 0..3 {
            consume(&mut stored, &generated.plaintext[index], now());
        }
        assert_eq!(remaining(&stored), (RECOVERY_CODE_COUNT - 3) as u32);
    }

    #[test]
    fn stored_codes_round_trip_through_serde() {
        let generated = generate();
        let mut stored = generated.stored;
        consume(&mut stored, &generated.plaintext[0], now());

        let encoded = serde_json::to_string(&stored).expect("serialize");
        let decoded: Vec<StoredRecoveryCode> = serde_json::from_str(&encoded).expect("deserialize");

        assert_eq!(decoded, stored);
        assert!(!encoded.contains(&generated.plaintext[0]), "serialized form must not carry plaintext");
    }
}
