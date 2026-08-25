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

//! The MFA operations the admin API exposes.
//!
//! This is the whole feature's surface: the console and the `rc` CLI reach it
//! through the same admin endpoints, and neither carries any of the logic. The
//! functions return `rustfs-madmin` wire types directly rather than a private
//! shape plus a mapping layer, so there is one definition of what a client sees.
//!
//! Errors are deliberately coarse at this boundary. [`MfaServiceError`] tells a
//! handler which HTTP status and which audit class to use, and nothing more: a
//! caller must not be able to tell a wrong code from a replayed one, or an
//! identity with no enrollment from one that is locked out.

use super::record::{MfaVerification, MfaVerifyError};
use super::totp::{TOTP_ALGORITHM, TOTP_DIGITS, TOTP_PERIOD_SECONDS, TotpSecret, provisioning_uri};
use super::{challenge, qr, recovery, store};
use crate::IamStore;
use crate::error::Error;
use rustfs_madmin::account::{MfaEnrollResponse, MfaStatus, RecoveryCodesResponse, UserMfaStatus};
use std::sync::Arc;
use time::OffsetDateTime;

/// Issuer shown by the authenticator app.
///
/// Constant rather than derived from the deployment: apps key their entries on
/// issuer plus account, so a value that changed with the hostname would make an
/// existing enrollment look like a different account after a rename.
pub const MFA_ISSUER: &str = "RustFS";

/// What went wrong, at the granularity a handler needs.
#[derive(Debug, thiserror::Error)]
pub enum MfaServiceError {
    #[error("two-factor authentication is not available: {0}")]
    EnrollmentUnavailable(&'static str),
    #[error("two-factor authentication is not enabled for this identity")]
    NotEnabled,
    #[error("two-factor authentication is already enabled for this identity")]
    AlreadyEnabled,
    #[error("no pending enrollment to confirm")]
    NoPendingEnrollment,
    #[error("the verification code is invalid")]
    InvalidCode,
    #[error("too many failed attempts; retry in {retry_after_seconds}s")]
    Locked { retry_after_seconds: u64 },
    #[error("the login challenge is invalid or has expired")]
    InvalidChallenge,
    #[error("{0}")]
    Internal(String),
}

impl From<Error> for MfaServiceError {
    fn from(value: Error) -> Self {
        Self::Internal(value.to_string())
    }
}

/// The audit class for a failure, so every handler classifies identically.
impl MfaServiceError {
    pub const fn audit_class(&self) -> &'static str {
        match self {
            Self::EnrollmentUnavailable(_) => "enrollment_unavailable",
            Self::NotEnabled | Self::NoPendingEnrollment => "not_enrolled",
            Self::AlreadyEnabled => "already_enabled",
            Self::InvalidCode => "invalid_code",
            Self::Locked { .. } => "rate_limited",
            Self::InvalidChallenge => "challenge_invalid",
            Self::Internal(_) => "internal_error",
        }
    }
}

fn map_verify_error(error: MfaVerifyError) -> MfaServiceError {
    match error {
        MfaVerifyError::NotEnabled => MfaServiceError::NotEnabled,
        MfaVerifyError::Locked { retry_after_seconds } => MfaServiceError::Locked { retry_after_seconds },
        // Wrong, replayed and malformed all collapse to one answer on the wire.
        // The distinction survives only in the audit trail, which the caller
        // reads from `MfaVerification`/the record, not from this error.
        MfaVerifyError::InvalidCode | MfaVerifyError::ReplayedTotpCode | MfaVerifyError::ReplayedRecoveryCode => {
            MfaServiceError::InvalidCode
        }
    }
}

/// Report the caller's own MFA state.
pub async fn status(api: Arc<IamStore>, access_key: &str, now: OffsetDateTime) -> Result<MfaStatus, MfaServiceError> {
    let loaded = store::load(api, access_key, now).await?;
    let record = loaded.record;
    let available = store::at_rest_protection_available();

    Ok(MfaStatus {
        enabled: record.is_enabled(),
        pending: record.has_pending_enrollment(now),
        algorithm: record.algorithm.clone(),
        digits: record.digits,
        period_seconds: record.period_seconds,
        activated_at: record.activated_at,
        pending_expires_at: record
            .has_pending_enrollment(now)
            .then_some(record.pending_expires_at)
            .flatten(),
        recovery_codes_remaining: record.recovery_codes_remaining(),
        last_verified_at: record.last_verified_at,
        enrollment_available: available,
        enrollment_blocked_reason: (!available).then(|| store::ENROLLMENT_UNAVAILABLE_REASON.to_string()),
    })
}

/// Report another identity's MFA state, for an administrator.
///
/// Narrower than [`status`] on purpose: an administrator inspecting someone
/// else's account has no need for their enrollment internals.
pub async fn admin_status(api: Arc<IamStore>, access_key: &str, now: OffsetDateTime) -> Result<UserMfaStatus, MfaServiceError> {
    let record = store::load(api, access_key, now).await?.record;

    Ok(UserMfaStatus {
        access_key: access_key.to_string(),
        enabled: record.is_enabled(),
        activated_at: record.activated_at,
        recovery_codes_remaining: record.recovery_codes_remaining(),
    })
}

/// Whether `access_key` must present a second factor.
///
/// The login path's gate. Propagates a store failure rather than answering
/// `false`, so an outage cannot silently disable the second factor.
pub async fn is_enabled(api: Arc<IamStore>, access_key: &str, now: OffsetDateTime) -> Result<bool, MfaServiceError> {
    Ok(store::is_enabled(api, access_key, now).await?)
}

/// Begin an enrollment and return everything needed to complete it.
///
/// Calling this on an already-enrolled identity is a re-configuration, not an
/// error: the active factor keeps working until the new one is confirmed.
pub async fn enroll(api: Arc<IamStore>, access_key: &str, now: OffsetDateTime) -> Result<MfaEnrollResponse, MfaServiceError> {
    if !store::at_rest_protection_available() {
        return Err(MfaServiceError::EnrollmentUnavailable(store::ENROLLMENT_UNAVAILABLE_REASON));
    }

    let secret = TotpSecret::generate();
    let uri = provisioning_uri(MFA_ISSUER, access_key, &secret);
    let rendered = qr::render(&uri).map_err(|err| MfaServiceError::Internal(err.to_string()))?;

    let secret_for_store = secret.clone();
    let expires_at = store::update(api, access_key, now, move |record| {
        record.begin_enrollment(&secret_for_store, now);
        Ok::<_, MfaServiceError>(record.pending_expires_at)
    })
    .await?;

    let expires_at = expires_at.ok_or_else(|| MfaServiceError::Internal("enrollment expiry was not recorded".to_string()))?;

    Ok(MfaEnrollResponse {
        secret_base32: secret.to_base32(),
        otpauth_uri: uri,
        qr_svg: rendered.svg,
        qr_utf8: rendered.utf8,
        algorithm: TOTP_ALGORITHM.to_string(),
        digits: TOTP_DIGITS,
        period_seconds: TOTP_PERIOD_SECONDS,
        expires_at,
    })
}

/// Confirm a pending enrollment and issue the first set of recovery codes.
///
/// The codes are returned once and never again: only their hashes are stored.
pub async fn activate(
    api: Arc<IamStore>,
    access_key: &str,
    code: &str,
    now: OffsetDateTime,
) -> Result<RecoveryCodesResponse, MfaServiceError> {
    let code = code.to_string();

    let plaintext = store::update(api, access_key, now, move |record| {
        if !record.has_pending_enrollment(now) {
            return Err(MfaServiceError::NoPendingEnrollment);
        }

        record.activate_enrollment(&code, now).map_err(map_verify_error)?;

        // Activation always replaces the recovery codes. Reusing a previous set
        // would leave codes valid for a secret they were never issued against.
        let generated = recovery::generate();
        record.set_recovery_codes(generated.stored, now);
        Ok(generated.plaintext)
    })
    .await?;

    Ok(RecoveryCodesResponse {
        recovery_codes: plaintext,
        generated_at: now,
    })
}

/// Replace the recovery codes, after proving possession of the second factor.
pub async fn regenerate_recovery_codes(
    api: Arc<IamStore>,
    access_key: &str,
    code: &str,
    now: OffsetDateTime,
) -> Result<RecoveryCodesResponse, MfaServiceError> {
    let code = code.to_string();

    let plaintext = store::update(api, access_key, now, move |record| {
        if !record.is_enabled() {
            return Err(MfaServiceError::NotEnabled);
        }

        record.verify(&code, now).map_err(map_verify_error)?;

        let generated = recovery::generate();
        record.set_recovery_codes(generated.stored, now);
        Ok(generated.plaintext)
    })
    .await?;

    Ok(RecoveryCodesResponse {
        recovery_codes: plaintext,
        generated_at: now,
    })
}

/// Turn the second factor off, after proving possession of it.
///
/// The caller is expected to have already re-verified the account password:
/// this function checks the factor, not the identity.
pub async fn disable(api: Arc<IamStore>, access_key: &str, code: &str, now: OffsetDateTime) -> Result<(), MfaServiceError> {
    let code = code.to_string();

    store::update(api, access_key, now, move |record| {
        if !record.is_enabled() {
            return Err(MfaServiceError::NotEnabled);
        }

        record.verify(&code, now).map_err(map_verify_error)?;

        record.disable();
        Ok(())
    })
    .await
}

/// Clear an identity's second factor administratively.
///
/// The break-glass path for a user who lost both their authenticator and their
/// recovery codes. Deletes the record outright rather than disabling it, so no
/// stale lockout counter survives to block the user's next enrollment.
pub async fn admin_reset(api: Arc<IamStore>, access_key: &str) -> Result<(), MfaServiceError> {
    Ok(store::delete(api, access_key).await?)
}

/// Verify a second factor during session minting.
///
/// Returns which factor satisfied it, so the caller can audit a recovery-code
/// login differently from a routine one and warn when the codes run low.
pub async fn verify(
    api: Arc<IamStore>,
    access_key: &str,
    code: &str,
    now: OffsetDateTime,
) -> Result<MfaVerification, MfaServiceError> {
    let code = code.to_string();

    store::update(api, access_key, now, move |record| record.verify(&code, now).map_err(map_verify_error)).await
}

/// Issue a login challenge for `access_key`.
pub fn issue_challenge(access_key: &str, now: OffsetDateTime, signing_key: &[u8]) -> String {
    challenge::issue(access_key, now.unix_timestamp().max(0) as u64, signing_key)
}

/// Validate a login challenge presented alongside a second factor.
pub fn validate_challenge(
    challenge_token: &str,
    access_key: &str,
    now: OffsetDateTime,
    signing_key: &[u8],
) -> Result<(), MfaServiceError> {
    challenge::validate(challenge_token, access_key, now.unix_timestamp().max(0) as u64, signing_key)
        .map_err(|_| MfaServiceError::InvalidChallenge)
}

/// How long an issued challenge remains valid.
pub const fn challenge_ttl_seconds() -> u64 {
    challenge::CHALLENGE_TTL_SECONDS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrong_replayed_and_malformed_codes_are_indistinguishable_on_the_wire() {
        // The security property: a caller probing the endpoint must not be able
        // to tell that a code was *correct but replayed*, which would confirm
        // they had captured a real code.
        for error in [
            MfaVerifyError::InvalidCode,
            MfaVerifyError::ReplayedTotpCode,
            MfaVerifyError::ReplayedRecoveryCode,
        ] {
            let mapped = map_verify_error(error);
            assert!(matches!(mapped, MfaServiceError::InvalidCode), "{error:?} leaked as {mapped}");
            assert_eq!(mapped.to_string(), "the verification code is invalid");
        }
    }

    #[test]
    fn a_lockout_reports_its_retry_hint() {
        let mapped = map_verify_error(MfaVerifyError::Locked {
            retry_after_seconds: 900,
        });
        assert!(matches!(
            mapped,
            MfaServiceError::Locked {
                retry_after_seconds: 900
            }
        ));
        assert_eq!(mapped.audit_class(), "rate_limited");
    }

    #[test]
    fn verification_failures_map_to_their_audit_classes() {
        for (error, expected) in [
            (MfaVerifyError::InvalidCode, "invalid_code"),
            (MfaVerifyError::ReplayedTotpCode, "invalid_code"),
            (MfaVerifyError::ReplayedRecoveryCode, "invalid_code"),
            (MfaVerifyError::NotEnabled, "not_enrolled"),
            (MfaVerifyError::Locked { retry_after_seconds: 60 }, "rate_limited"),
        ] {
            assert_eq!(map_verify_error(error).audit_class(), expected, "for {error:?}");
        }
    }

    #[test]
    fn a_storage_error_stays_internal() {
        // A disk or quorum failure must not be reported to the user as a bad
        // code, or they will keep retrying a request that cannot succeed.
        let mapped = MfaServiceError::from(Error::other("erasure set is offline"));

        assert!(matches!(mapped, MfaServiceError::Internal(_)));
        assert_eq!(mapped.audit_class(), "internal_error");
    }

    #[test]
    fn the_issuer_is_stable() {
        // Authenticator apps key entries on issuer plus account; a value that
        // moved with the hostname would orphan existing enrollments.
        assert_eq!(MFA_ISSUER, "RustFS");
    }

    #[test]
    fn challenges_issued_here_validate_here() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let key = b"signing-key";
        let token = issue_challenge("sinan", now, key);

        assert!(validate_challenge(&token, "sinan", now, key).is_ok());
        assert!(matches!(
            validate_challenge(&token, "someone-else", now, key),
            Err(MfaServiceError::InvalidChallenge)
        ));
    }

    #[test]
    fn every_error_has_a_distinct_audit_class_where_it_matters() {
        // Enrollment-unavailable and locked must not be reported as a bad code:
        // both are actionable by the operator or the user, and conflating them
        // with a wrong code hides the remedy.
        assert_eq!(MfaServiceError::EnrollmentUnavailable("x").audit_class(), "enrollment_unavailable");
        assert_eq!(MfaServiceError::Locked { retry_after_seconds: 1 }.audit_class(), "rate_limited");
        assert_eq!(MfaServiceError::InvalidCode.audit_class(), "invalid_code");
        assert_eq!(MfaServiceError::InvalidChallenge.audit_class(), "challenge_invalid");
    }
}
