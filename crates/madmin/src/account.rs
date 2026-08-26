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

//! Wire contract for self-service account and multi-factor authentication.
//!
//! Console and the `rc` CLI both decode these shapes, so this module is the
//! single definition of the account/MFA API surface. Adding a field here is
//! additive; renaming or removing one is a breaking change for both clients.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Error code returned when a session-minting request needs an MFA proof.
///
/// Emitted by `AssumeRole` when the caller's identity has TOTP enabled and the
/// request carried no `TokenCode`. Clients match on this exact string to decide
/// whether to prompt for a second factor instead of reporting a login failure.
pub const ERR_MFA_REQUIRED: &str = "MultiFactorAuthRequired";

/// Error code returned when too many second-factor attempts have failed.
pub const ERR_MFA_LOCKED: &str = "MultiFactorAuthLocked";

/// How the calling credential was established.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityType {
    /// The server's bootstrap root credential.
    Root,
    /// A built-in IAM user stored under `config/iam/users/`.
    Iam,
    /// A temporary STS session credential.
    Sts,
    /// A service account minted from a parent identity.
    ServiceAccount,
}

impl IdentityType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Iam => "iam",
            Self::Sts => "sts",
            Self::ServiceAccount => "service-account",
        }
    }
}

/// Where the identity's long-term secret lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialsSource {
    /// Provisioned from the server process environment; immutable at runtime.
    Env,
    /// Stored in the IAM object store; mutable through the admin API.
    Iam,
}

impl CredentialsSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::Iam => "iam",
        }
    }
}

/// Which self-service mutations the server will accept for this identity.
///
/// Clients use this to disable controls instead of letting the user submit a
/// request that is guaranteed to fail. Root credentials report `false` for both
/// because they are pinned by a process-wide `OnceLock` and feed the derived
/// internode RPC secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AccountMutability {
    #[serde(default)]
    pub password: bool,
    #[serde(default)]
    pub username: bool,
}

/// MFA state as reported alongside the account summary.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccountMfaSummary {
    #[serde(default)]
    pub enabled: bool,
    /// An enrollment has been started but not yet activated.
    #[serde(default)]
    pub pending: bool,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub activated_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub recovery_codes_remaining: u32,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub last_verified_at: Option<OffsetDateTime>,
    /// Server-side at-rest protection is unavailable, so enrollment is refused.
    #[serde(default)]
    pub enrollment_available: bool,
    /// Human-readable reason when `enrollment_available` is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enrollment_blocked_reason: Option<String>,
}

/// Response of `GET /rustfs/admin/v3/account/info`.
///
/// Describes the caller to itself. It never accepts a target parameter, so it
/// cannot be used to enumerate other identities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfAccountInfo {
    /// The long-term identity behind the request. For STS and service-account
    /// credentials this is the parent, not the ephemeral access key.
    pub access_key: String,
    pub identity_type: IdentityType,
    /// The ephemeral access key actually presented, when it differs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_access_key: Option<String>,
    pub is_admin: bool,
    pub status: String,
    #[serde(default)]
    pub member_of: Vec<String>,
    #[serde(default)]
    pub policies: Vec<String>,
    pub credentials_source: CredentialsSource,
    pub mutable: AccountMutability,
    pub mfa: AccountMfaSummary,
}

/// Request body of `POST /rustfs/admin/v3/account/password`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePasswordRequest {
    /// Proof of possession. Required even though the request is already signed:
    /// a signature only proves the credential was used, not that the human at
    /// the keyboard knows it.
    pub current_secret_key: String,
    pub new_secret_key: String,
}

/// Request body of `PUT /rustfs/admin/v3/set-user-secret-key`.
///
/// Administrative reset of another user's secret key. Unlike `add-user` this
/// preserves the target's status, policies and group memberships.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetUserSecretKeyRequest {
    pub secret_key: String,
}

/// Response of `GET /rustfs/admin/v3/account/mfa`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MfaStatus {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub pending: bool,
    pub algorithm: String,
    pub digits: u8,
    pub period_seconds: u32,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub activated_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub pending_expires_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub recovery_codes_remaining: u32,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub last_verified_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub enrollment_available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enrollment_blocked_reason: Option<String>,
}

/// Response of `POST /rustfs/admin/v3/account/mfa/enroll`.
///
/// The secret appears here exactly once per enrollment. Clients must render it
/// and then discard it; persisting it in browser storage or a config file
/// defeats the second factor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MfaEnrollResponse {
    /// Base32 (RFC 4648, unpadded) shared secret for manual entry.
    pub secret_base32: String,
    /// `otpauth://totp/...` provisioning URI for QR scanning.
    pub otpauth_uri: String,
    /// Server-rendered QR as a standalone SVG document. Rendered by the server
    /// so neither client needs its own QR encoder.
    pub qr_svg: String,
    /// Server-rendered QR as Unicode half-block art for terminals.
    pub qr_utf8: String,
    pub algorithm: String,
    pub digits: u8,
    pub period_seconds: u32,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

/// Request body carrying a single second-factor code.
///
/// Accepts either a TOTP digit code or a recovery code; the server decides
/// which by format, so clients do not need to classify user input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MfaCodeRequest {
    pub code: String,
}

/// Request body of `DELETE /rustfs/admin/v3/account/mfa`.
///
/// Turning off the second factor is a step-up operation: a hijacked browser
/// session holding only STS credentials must not be able to do it silently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MfaDisableRequest {
    pub code: String,
    pub current_secret_key: String,
}

/// Response of MFA activation and recovery-code regeneration.
///
/// This is the only place recovery codes appear in plaintext; the server keeps
/// only their hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryCodesResponse {
    pub recovery_codes: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub generated_at: OffsetDateTime,
}

/// Response of `GET /rustfs/admin/v3/mfa/challenge`.
///
/// Requires a valid signature, so a caller only ever learns the MFA state of
/// the identity whose secret key it already holds.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MfaChallengeResponse {
    pub required: bool,
    /// Opaque, signed, time-bound value to echo back as `SerialNumber`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub challenge: Option<String>,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<OffsetDateTime>,
}

/// Response of `GET /rustfs/admin/v3/user/mfa?accessKey=...`.
///
/// Deliberately narrower than [`MfaStatus`]: an administrator inspecting
/// someone else's account has no need for their enrollment internals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserMfaStatus {
    pub access_key: String,
    pub enabled: bool,
    #[serde(with = "time::serde::rfc3339::option", default, skip_serializing_if = "Option::is_none")]
    pub activated_at: Option<OffsetDateTime>,
    #[serde(default)]
    pub recovery_codes_remaining: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_type_wire_values_are_kebab_case() {
        assert_eq!(serde_json::to_string(&IdentityType::Root).expect("serialize"), "\"root\"");
        assert_eq!(
            serde_json::to_string(&IdentityType::ServiceAccount).expect("serialize"),
            "\"service-account\""
        );
        assert_eq!(IdentityType::ServiceAccount.as_str(), "service-account");
    }

    #[test]
    fn credentials_source_wire_values_are_stable() {
        assert_eq!(serde_json::to_string(&CredentialsSource::Env).expect("serialize"), "\"env\"");
        assert_eq!(serde_json::to_string(&CredentialsSource::Iam).expect("serialize"), "\"iam\"");
    }

    #[test]
    fn account_info_round_trips() {
        let info = SelfAccountInfo {
            access_key: "sinan".to_string(),
            identity_type: IdentityType::Iam,
            session_access_key: Some("temp".to_string()),
            is_admin: true,
            status: "enabled".to_string(),
            member_of: vec!["ops".to_string()],
            policies: vec!["consoleAdmin".to_string()],
            credentials_source: CredentialsSource::Iam,
            mutable: AccountMutability {
                password: true,
                username: false,
            },
            mfa: AccountMfaSummary {
                enabled: true,
                recovery_codes_remaining: 7,
                enrollment_available: true,
                ..Default::default()
            },
        };

        let encoded = serde_json::to_string(&info).expect("serialize");
        let decoded: SelfAccountInfo = serde_json::from_str(&encoded).expect("deserialize");

        assert_eq!(decoded.access_key, "sinan");
        assert_eq!(decoded.identity_type, IdentityType::Iam);
        assert!(decoded.mutable.password);
        assert!(!decoded.mutable.username);
        assert_eq!(decoded.mfa.recovery_codes_remaining, 7);
    }

    #[test]
    fn mfa_challenge_defaults_to_not_required() {
        // Older servers omit the whole payload; clients must not prompt.
        let decoded: MfaChallengeResponse = serde_json::from_str("{\"required\":false}").expect("deserialize");
        assert!(!decoded.required);
        assert!(decoded.challenge.is_none());
    }

    #[test]
    fn mfa_status_tolerates_absent_optional_fields() {
        let decoded: MfaStatus =
            serde_json::from_str("{\"algorithm\":\"SHA1\",\"digits\":6,\"period_seconds\":30}").expect("deserialize");
        assert!(!decoded.enabled);
        assert!(!decoded.pending);
        assert_eq!(decoded.digits, 6);
        assert!(decoded.activated_at.is_none());
    }
}
