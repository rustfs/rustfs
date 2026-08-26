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

//! The persisted per-identity MFA record and the state machine over it.
//!
//! Everything here is pure: no storage, no clock, no I/O. The caller supplies
//! `now`, which is what makes the replay, expiry and lockout rules testable at
//! their edges instead of only in the happy case.
//!
//! Enrollment is deliberately two-phase. A started enrollment lands in
//! `pending_secret` and only becomes the active secret once the user proves they
//! can generate a code from it. Without that, a user who scanned the QR into the
//! wrong app — or never scanned it — would be locked out of their own account
//! the moment enrollment was recorded. The same two-phase shape is what makes
//! re-configuring safe: the existing secret keeps working until the new one is
//! confirmed.

use super::recovery::{self, ConsumeOutcome, StoredRecoveryCode};
use super::totp::{self, TOTP_ALGORITHM, TOTP_DIGITS, TOTP_PERIOD_SECONDS, TotpSecret};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// On-disk format version. A record written by a newer node is refused rather
/// than misread.
pub const MFA_RECORD_VERSION: u8 = 1;

/// How long a started enrollment stays confirmable.
///
/// Long enough to install an authenticator app mid-flow, short enough that an
/// abandoned enrollment does not leave a usable secret lying in the store.
pub const PENDING_ENROLLMENT_TTL_SECONDS: i64 = 600;

/// Consecutive failures before the identity is locked out.
const MAX_FAILED_ATTEMPTS: u32 = 5;

/// First lockout duration, doubling for each further run of failures.
const LOCKOUT_BASE_SECONDS: i64 = 900;

/// Ceiling on the lockout, so a sustained attack cannot lock a legitimate user
/// out indefinitely — the point is to make guessing infeasible, not to hand an
/// attacker a denial of service against the account owner.
const LOCKOUT_MAX_SECONDS: i64 = 3600;

/// A six-digit code has a million values. Bounding attempts is what turns that
/// into an infeasible guess; without it, an unattended script clears the space
/// in hours.
const _: () = assert!(MAX_FAILED_ATTEMPTS > 0);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MfaRecordError {
    #[error("the stored MFA record has an unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("the stored MFA secret is unusable")]
    CorruptSecret,
}

/// Why a verification attempt failed.
///
/// The API surface collapses most of these into one opaque answer; the
/// distinctions exist so the audit trail can tell an operator what actually
/// happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MfaVerifyError {
    /// No active second factor for this identity.
    NotEnabled,
    /// Too many recent failures.
    Locked { retry_after_seconds: u64 },
    /// The code did not match anything.
    InvalidCode,
    /// A correct TOTP code for a time step that was already spent.
    ReplayedTotpCode,
    /// A recovery code that had already been used.
    ReplayedRecoveryCode,
}

/// What satisfied a verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MfaVerification {
    Totp,
    RecoveryCode { remaining: u32 },
}

/// The persisted MFA state for one identity.
///
/// Field-level notes:
///
/// * `secret_b32` present means the second factor is active.
/// * `pending_secret_b32` present and unexpired means an enrollment awaits
///   confirmation. Both may be present at once, which is a re-configuration.
/// * `last_used_step` is the anti-replay high-water mark, not a timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MfaRecord {
    pub version: u8,
    pub access_key: String,
    pub algorithm: String,
    pub digits: u8,
    pub period_seconds: u32,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_b32: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_secret_b32: Option<String>,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub pending_expires_at: Option<OffsetDateTime>,

    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub activated_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub last_verified_at: Option<OffsetDateTime>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_step: Option<u64>,

    #[serde(default)]
    pub recovery_codes: Vec<StoredRecoveryCode>,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub recovery_codes_generated_at: Option<OffsetDateTime>,

    #[serde(default)]
    pub failed_attempts: u32,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub locked_until: Option<OffsetDateTime>,
}

impl MfaRecord {
    /// A record with nothing enrolled.
    pub fn new(access_key: &str, now: OffsetDateTime) -> Self {
        Self {
            version: MFA_RECORD_VERSION,
            access_key: access_key.to_string(),
            algorithm: TOTP_ALGORITHM.to_string(),
            digits: TOTP_DIGITS,
            period_seconds: TOTP_PERIOD_SECONDS,
            secret_b32: None,
            pending_secret_b32: None,
            pending_expires_at: None,
            created_at: now,
            activated_at: None,
            last_verified_at: None,
            last_used_step: None,
            recovery_codes: Vec::new(),
            recovery_codes_generated_at: None,
            failed_attempts: 0,
            locked_until: None,
        }
    }

    /// Reject a record this build cannot interpret.
    pub fn validate_version(&self) -> Result<(), MfaRecordError> {
        if self.version != MFA_RECORD_VERSION {
            return Err(MfaRecordError::UnsupportedVersion(self.version));
        }
        Ok(())
    }

    /// Whether a second factor is active.
    pub const fn is_enabled(&self) -> bool {
        self.secret_b32.is_some()
    }

    /// Whether an enrollment is awaiting confirmation.
    pub fn has_pending_enrollment(&self, now: OffsetDateTime) -> bool {
        self.pending_secret_b32.is_some() && self.pending_expires_at.is_some_and(|expires| now < expires)
    }

    /// Recovery codes still usable.
    pub fn recovery_codes_remaining(&self) -> u32 {
        recovery::remaining(&self.recovery_codes)
    }

    /// Whether verification is currently refused because of prior failures.
    pub fn lock_remaining_seconds(&self, now: OffsetDateTime) -> Option<u64> {
        let locked_until = self.locked_until?;
        if now >= locked_until {
            return None;
        }
        Some((locked_until - now).whole_seconds().max(1) as u64)
    }

    /// Start (or restart) an enrollment.
    ///
    /// Overwrites any earlier pending secret: a user who reopened the setup
    /// dialog expects a fresh QR, and the previous unconfirmed secret has no
    /// standing. The active secret is untouched, so a re-configuration cannot
    /// lock the user out if they abandon it.
    pub fn begin_enrollment(&mut self, secret: &TotpSecret, now: OffsetDateTime) {
        self.pending_secret_b32 = Some(secret.to_base32());
        self.pending_expires_at = Some(now + time::Duration::seconds(PENDING_ENROLLMENT_TTL_SECONDS));
    }

    /// Confirm the pending enrollment with `code`.
    ///
    /// On success the pending secret becomes active, the replay high-water mark
    /// is seeded with the step that was just proven, and the caller is expected
    /// to install a fresh set of recovery codes.
    pub fn activate_enrollment(&mut self, code: &str, now: OffsetDateTime) -> Result<(), MfaVerifyError> {
        if let Some(retry_after_seconds) = self.lock_remaining_seconds(now) {
            return Err(MfaVerifyError::Locked { retry_after_seconds });
        }

        let Some(pending) = self.pending_secret_b32.clone() else {
            return Err(MfaVerifyError::NotEnabled);
        };
        if !self.has_pending_enrollment(now) {
            // Treated as "nothing to confirm" rather than a code failure: the
            // secret is gone, so no code could ever have matched.
            self.clear_pending();
            return Err(MfaVerifyError::NotEnabled);
        }

        let secret = TotpSecret::from_base32(&pending).map_err(|_| MfaVerifyError::NotEnabled)?;
        let unix = now.unix_timestamp().max(0) as u64;

        match secret.verify(code, unix, None) {
            Ok(step) => {
                self.secret_b32 = Some(pending);
                self.activated_at = Some(now);
                self.last_used_step = Some(step);
                self.last_verified_at = Some(now);
                self.clear_pending();
                self.clear_failures();
                Ok(())
            }
            Err(_) => {
                self.register_failure(now);
                Err(MfaVerifyError::InvalidCode)
            }
        }
    }

    /// Verify a second factor against the active enrollment.
    ///
    /// Accepts either a TOTP code or a recovery code and decides which by shape,
    /// so a client never has to ask the user to declare what they typed.
    pub fn verify(&mut self, code: &str, now: OffsetDateTime) -> Result<MfaVerification, MfaVerifyError> {
        if let Some(retry_after_seconds) = self.lock_remaining_seconds(now) {
            return Err(MfaVerifyError::Locked { retry_after_seconds });
        }

        let Some(secret_b32) = self.secret_b32.clone() else {
            return Err(MfaVerifyError::NotEnabled);
        };

        // Shape routing happens before either verification so a recovery code is
        // never fed to the TOTP path (where it would burn a failed attempt for
        // the wrong reason) and vice versa.
        if recovery::looks_like_recovery_code(code) {
            return self.verify_recovery_code(code, now);
        }

        if !totp::looks_like_totp_code(code) {
            self.register_failure(now);
            return Err(MfaVerifyError::InvalidCode);
        }

        let secret = TotpSecret::from_base32(&secret_b32).map_err(|_| MfaVerifyError::NotEnabled)?;
        let unix = now.unix_timestamp().max(0) as u64;

        match secret.verify(code, unix, self.last_used_step) {
            Ok(step) => {
                self.last_used_step = Some(step);
                self.last_verified_at = Some(now);
                self.clear_failures();
                Ok(MfaVerification::Totp)
            }
            Err(_) => {
                self.register_failure(now);
                // Re-check ignoring the high-water mark purely to classify the
                // failure for the audit trail. The caller still gets a single
                // opaque rejection, so this tells an attacker nothing while
                // telling an operator whether they are seeing replays.
                if secret.verify(code, unix, None).is_ok() {
                    Err(MfaVerifyError::ReplayedTotpCode)
                } else {
                    Err(MfaVerifyError::InvalidCode)
                }
            }
        }
    }

    fn verify_recovery_code(&mut self, code: &str, now: OffsetDateTime) -> Result<MfaVerification, MfaVerifyError> {
        match recovery::consume(&mut self.recovery_codes, code, now) {
            ConsumeOutcome::Consumed { remaining, .. } => {
                self.last_verified_at = Some(now);
                self.clear_failures();
                Ok(MfaVerification::RecoveryCode { remaining })
            }
            ConsumeOutcome::AlreadyUsed => {
                self.register_failure(now);
                Err(MfaVerifyError::ReplayedRecoveryCode)
            }
            ConsumeOutcome::NoMatch => {
                self.register_failure(now);
                Err(MfaVerifyError::InvalidCode)
            }
        }
    }

    /// Replace the recovery code set.
    pub fn set_recovery_codes(&mut self, codes: Vec<StoredRecoveryCode>, now: OffsetDateTime) {
        self.recovery_codes = codes;
        self.recovery_codes_generated_at = Some(now);
    }

    /// Turn the second factor off, clearing every trace of the enrollment.
    ///
    /// Recovery codes go too: keeping them would leave a live bypass for a
    /// factor the user believes is gone.
    pub fn disable(&mut self) {
        self.secret_b32 = None;
        self.activated_at = None;
        self.last_used_step = None;
        self.recovery_codes.clear();
        self.recovery_codes_generated_at = None;
        self.clear_pending();
        self.clear_failures();
    }

    fn clear_pending(&mut self) {
        self.pending_secret_b32 = None;
        self.pending_expires_at = None;
    }

    fn clear_failures(&mut self) {
        self.failed_attempts = 0;
        self.locked_until = None;
    }

    /// Record a failed attempt, locking the identity once the threshold is hit.
    ///
    /// The lockout doubles for each further run of failures so a persistent
    /// attacker faces a rapidly growing cost, capped so the account owner is
    /// never locked out for longer than [`LOCKOUT_MAX_SECONDS`].
    fn register_failure(&mut self, now: OffsetDateTime) {
        self.failed_attempts = self.failed_attempts.saturating_add(1);

        if !self.failed_attempts.is_multiple_of(MAX_FAILED_ATTEMPTS) {
            return;
        }

        let runs = self.failed_attempts / MAX_FAILED_ATTEMPTS;
        // `runs` is at least 1 here; shift is bounded so the doubling cannot
        // overflow before the cap applies.
        let scale = 1i64.checked_shl(runs.saturating_sub(1).min(16)).unwrap_or(i64::MAX);
        let seconds = LOCKOUT_BASE_SECONDS.saturating_mul(scale).min(LOCKOUT_MAX_SECONDS);
        self.locked_until = Some(now + time::Duration::seconds(seconds));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(unix: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(unix).expect("valid timestamp")
    }

    const T0: i64 = 1_700_000_000;

    fn enrolled() -> (MfaRecord, TotpSecret) {
        let secret = TotpSecret::generate();
        let mut record = MfaRecord::new("sinan", at(T0));
        record.begin_enrollment(&secret, at(T0));
        let code = secret.code_at(T0 as u64);
        record.activate_enrollment(&code, at(T0)).expect("activate");
        (record, secret)
    }

    #[test]
    fn a_new_record_has_nothing_enrolled() {
        let record = MfaRecord::new("sinan", at(T0));

        assert!(!record.is_enabled());
        assert!(!record.has_pending_enrollment(at(T0)));
        assert_eq!(record.recovery_codes_remaining(), 0);
        assert_eq!(record.algorithm, "SHA1");
        assert_eq!(record.digits, 6);
        assert_eq!(record.period_seconds, 30);
    }

    #[test]
    fn a_record_from_an_unknown_version_is_refused() {
        let mut record = MfaRecord::new("sinan", at(T0));
        record.version = 99;
        assert_eq!(record.validate_version(), Err(MfaRecordError::UnsupportedVersion(99)));
    }

    #[test]
    fn enrollment_is_only_active_after_confirmation() {
        // The property that keeps a mis-scanned QR from locking the user out.
        let secret = TotpSecret::generate();
        let mut record = MfaRecord::new("sinan", at(T0));
        record.begin_enrollment(&secret, at(T0));

        assert!(record.has_pending_enrollment(at(T0)));
        assert!(!record.is_enabled(), "a pending enrollment must not gate logins");

        record
            .activate_enrollment(&secret.code_at(T0 as u64), at(T0))
            .expect("activate");

        assert!(record.is_enabled());
        assert!(!record.has_pending_enrollment(at(T0)));
        assert_eq!(record.activated_at, Some(at(T0)));
    }

    #[test]
    fn activation_with_a_wrong_code_leaves_the_enrollment_pending() {
        let secret = TotpSecret::generate();
        let mut record = MfaRecord::new("sinan", at(T0));
        record.begin_enrollment(&secret, at(T0));

        assert_eq!(record.activate_enrollment("000000", at(T0)), Err(MfaVerifyError::InvalidCode));
        assert!(!record.is_enabled());
        assert!(record.has_pending_enrollment(at(T0)), "the user must be able to retry");
        assert_eq!(record.failed_attempts, 1);
    }

    #[test]
    fn an_expired_enrollment_cannot_be_confirmed() {
        let secret = TotpSecret::generate();
        let mut record = MfaRecord::new("sinan", at(T0));
        record.begin_enrollment(&secret, at(T0));

        let late = T0 + PENDING_ENROLLMENT_TTL_SECONDS + 1;
        assert_eq!(
            record.activate_enrollment(&secret.code_at(late as u64), at(late)),
            Err(MfaVerifyError::NotEnabled)
        );
        assert!(!record.is_enabled());
        assert!(record.pending_secret_b32.is_none(), "an expired secret must not linger");
    }

    #[test]
    fn re_enrolling_keeps_the_existing_factor_working_until_confirmed() {
        // Re-configuration must not be a window with no second factor.
        let (mut record, original) = enrolled();
        let replacement = TotpSecret::generate();
        record.begin_enrollment(&replacement, at(T0 + 60));

        assert!(record.is_enabled());
        assert_eq!(record.verify(&original.code_at((T0 + 60) as u64), at(T0 + 60)), Ok(MfaVerification::Totp));

        record
            .activate_enrollment(&replacement.code_at((T0 + 120) as u64), at(T0 + 120))
            .expect("activate replacement");

        // The old secret stops working only once the new one is confirmed.
        assert_eq!(
            record.verify(&original.code_at((T0 + 180) as u64), at(T0 + 180)),
            Err(MfaVerifyError::InvalidCode)
        );
        assert_eq!(
            record.verify(&replacement.code_at((T0 + 180) as u64), at(T0 + 180)),
            Ok(MfaVerification::Totp)
        );
    }

    #[test]
    fn a_current_totp_code_verifies() {
        let (mut record, secret) = enrolled();
        let later = T0 + 60;

        assert_eq!(record.verify(&secret.code_at(later as u64), at(later)), Ok(MfaVerification::Totp));
        assert_eq!(record.last_verified_at, Some(at(later)));
    }

    #[test]
    fn a_replayed_totp_code_is_rejected_and_classified() {
        let (mut record, secret) = enrolled();
        let later = T0 + 60;
        let code = secret.code_at(later as u64);

        assert_eq!(record.verify(&code, at(later)), Ok(MfaVerification::Totp));
        // Same code, same window: correct by the clock, refused by the step.
        assert_eq!(record.verify(&code, at(later)), Err(MfaVerifyError::ReplayedTotpCode));
    }

    #[test]
    fn activation_seeds_the_replay_high_water_mark() {
        // Without this, the code used to confirm enrollment would still be
        // valid for its remaining window.
        let secret = TotpSecret::generate();
        let mut record = MfaRecord::new("sinan", at(T0));
        record.begin_enrollment(&secret, at(T0));
        let code = secret.code_at(T0 as u64);
        record.activate_enrollment(&code, at(T0)).expect("activate");

        assert_eq!(record.verify(&code, at(T0)), Err(MfaVerifyError::ReplayedTotpCode));
    }

    #[test]
    fn a_wrong_code_is_rejected_as_invalid() {
        let (mut record, secret) = enrolled();
        let later = T0 + 60;
        let correct: u32 = secret.code_at(later as u64).parse().expect("digits");
        let wrong = format!("{:06}", (correct + 1) % 1_000_000);

        assert_eq!(record.verify(&wrong, at(later)), Err(MfaVerifyError::InvalidCode));
    }

    #[test]
    fn verification_fails_when_nothing_is_enrolled() {
        let mut record = MfaRecord::new("sinan", at(T0));
        assert_eq!(record.verify("123456", at(T0)), Err(MfaVerifyError::NotEnabled));
    }

    #[test]
    fn a_recovery_code_verifies_and_is_consumed() {
        let (mut record, _) = enrolled();
        let generated = recovery::generate();
        record.set_recovery_codes(generated.stored, at(T0));

        let outcome = record.verify(&generated.plaintext[0], at(T0 + 60));
        assert_eq!(outcome, Ok(MfaVerification::RecoveryCode { remaining: 9 }));
        assert_eq!(record.recovery_codes_remaining(), 9);
    }

    #[test]
    fn a_reused_recovery_code_is_rejected_and_classified() {
        let (mut record, _) = enrolled();
        let generated = recovery::generate();
        record.set_recovery_codes(generated.stored, at(T0));

        record.verify(&generated.plaintext[0], at(T0 + 60)).expect("first use");
        assert_eq!(
            record.verify(&generated.plaintext[0], at(T0 + 90)),
            Err(MfaVerifyError::ReplayedRecoveryCode)
        );
    }

    #[test]
    fn a_recovery_code_is_not_charged_against_the_totp_path() {
        // Shape routing: a recovery code must not be tried as a TOTP code, or a
        // legitimate recovery attempt would also register a TOTP failure.
        let (mut record, _) = enrolled();
        let generated = recovery::generate();
        record.set_recovery_codes(generated.stored, at(T0));

        record.verify(&generated.plaintext[0], at(T0 + 60)).expect("recovery code");
        assert_eq!(record.failed_attempts, 0);
    }

    #[test]
    fn repeated_failures_lock_the_identity() {
        let (mut record, secret) = enrolled();
        let later = T0 + 60;

        for attempt in 1..MAX_FAILED_ATTEMPTS {
            assert_eq!(record.verify("000000", at(later)), Err(MfaVerifyError::InvalidCode));
            assert!(record.lock_remaining_seconds(at(later)).is_none(), "locked too early at {attempt}");
        }

        assert_eq!(record.verify("000000", at(later)), Err(MfaVerifyError::InvalidCode));
        let remaining = record.lock_remaining_seconds(at(later)).expect("must be locked");
        assert_eq!(remaining, LOCKOUT_BASE_SECONDS as u64);

        // A correct code is refused while locked: that is the whole point.
        assert_eq!(
            record.verify(&secret.code_at(later as u64), at(later)),
            Err(MfaVerifyError::Locked {
                retry_after_seconds: LOCKOUT_BASE_SECONDS as u64
            })
        );
    }

    #[test]
    fn the_lock_expires_and_a_correct_code_then_works() {
        let (mut record, secret) = enrolled();
        let later = T0 + 60;
        for _ in 0..MAX_FAILED_ATTEMPTS {
            let _ = record.verify("000000", at(later));
        }

        let after_lock = later + LOCKOUT_BASE_SECONDS + 1;
        assert!(record.lock_remaining_seconds(at(after_lock)).is_none());
        assert_eq!(
            record.verify(&secret.code_at(after_lock as u64), at(after_lock)),
            Ok(MfaVerification::Totp)
        );
    }

    #[test]
    fn a_successful_verification_clears_the_failure_count() {
        let (mut record, secret) = enrolled();
        let later = T0 + 60;

        let _ = record.verify("000000", at(later));
        let _ = record.verify("000000", at(later));
        assert_eq!(record.failed_attempts, 2);

        record.verify(&secret.code_at(later as u64), at(later)).expect("correct code");
        assert_eq!(record.failed_attempts, 0);
        assert!(record.locked_until.is_none());
    }

    #[test]
    fn successive_lockouts_lengthen_and_then_stop_growing() {
        let (mut record, _) = enrolled();
        let mut clock = T0 + 60;
        let mut previous = 0u64;

        for round in 1..=8 {
            for _ in 0..MAX_FAILED_ATTEMPTS {
                let _ = record.verify("000000", at(clock));
            }
            let locked_for = record.lock_remaining_seconds(at(clock)).expect("must be locked");

            assert!(locked_for <= LOCKOUT_MAX_SECONDS as u64, "round {round} exceeded the cap");
            assert!(locked_for >= previous, "round {round} went backwards");
            previous = locked_for;

            // Step past the lock without succeeding, so the failure run continues.
            clock += locked_for as i64 + 1;
        }

        assert_eq!(previous, LOCKOUT_MAX_SECONDS as u64, "the lockout should reach its cap");
    }

    #[test]
    fn disabling_clears_the_secret_and_the_recovery_codes() {
        // A user who turns the factor off must not be left with codes that
        // still bypass a factor they believe is gone.
        let (mut record, secret) = enrolled();
        record.set_recovery_codes(recovery::generate().stored, at(T0));

        record.disable();

        assert!(!record.is_enabled());
        assert!(record.secret_b32.is_none());
        assert_eq!(record.recovery_codes_remaining(), 0);
        assert!(record.recovery_codes.is_empty());
        assert!(record.activated_at.is_none());
        assert_eq!(record.verify(&secret.code_at(T0 as u64), at(T0)), Err(MfaVerifyError::NotEnabled));
    }

    #[test]
    fn malformed_input_is_rejected_without_matching_anything() {
        let (mut record, _) = enrolled();
        for bad in ["", "abc", "12345", "1234567", "!!!!"] {
            assert_eq!(record.verify(bad, at(T0 + 60)), Err(MfaVerifyError::InvalidCode), "input {bad:?}");
        }
    }

    #[test]
    fn the_record_round_trips_through_serde_without_leaking_plaintext_codes() {
        let (mut record, _) = enrolled();
        let generated = recovery::generate();
        record.set_recovery_codes(generated.stored, at(T0));

        let encoded = serde_json::to_string(&record).expect("serialize");
        let decoded: MfaRecord = serde_json::from_str(&encoded).expect("deserialize");

        assert_eq!(decoded.access_key, "sinan");
        assert!(decoded.is_enabled());
        assert_eq!(decoded.recovery_codes_remaining(), 10);
        assert_eq!(decoded.last_used_step, record.last_used_step);

        for plaintext in &generated.plaintext {
            assert!(!encoded.contains(plaintext), "serialized record leaked a recovery code");
        }
    }

    #[test]
    fn a_record_written_by_an_older_node_decodes_without_the_optional_fields() {
        // Forward/backward tolerance: the optional fields are skipped when
        // absent, so a minimal record from a rolling upgrade still loads.
        let minimal = serde_json::json!({
            "version": 1,
            "access_key": "sinan",
            "algorithm": "SHA1",
            "digits": 6,
            "period_seconds": 30,
            "created_at": "2026-08-25T00:00:00Z",
        });
        let decoded: MfaRecord = serde_json::from_value(minimal).expect("deserialize");

        assert!(!decoded.is_enabled());
        assert_eq!(decoded.failed_attempts, 0);
        assert!(decoded.recovery_codes.is_empty());
        decoded.validate_version().expect("version 1 is supported");
    }
}
