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

//! Audit adapter for the self-service account and MFA endpoints.
//!
//! Emits onto the same pipeline as the S3 and KMS entries, so account and
//! authentication activity lands in whatever SIEM a deployment already
//! operates. Modelled on [`super::kms_audit`], which established this shape.
//!
//! # Redaction
//!
//! Nothing carried here can reconstruct a credential. Secret keys, TOTP
//! secrets, provisioning URIs, submitted codes and recovery codes never enter
//! an entry — not even hashed, and not even on the failure paths where the
//! submitted value would be the most tempting thing to record. Failures are
//! described by the [`AccountAuditFailure`] vocabulary, which is a closed set
//! of static strings, so no caller-supplied bytes can reach a log target
//! through this module.

use crate::admin::storage_api::s3::{Body, S3Request};
use crate::server::RemoteAddr;
use crate::storage::access::request_context_from_extensions;
use crate::storage::helper::spawn_background_with_context;
use crate::storage::request_context::RequestContext;
use hashbrown::HashMap;
use rustfs_audit::entity::{ApiDetailsBuilder, AuditEntry, AuditEntryBuilder};
use rustfs_audit::global::AuditLogger;
use rustfs_madmin::account::IdentityType;
use rustfs_s3_types::EventName;
use rustfs_targets::get_request_user_agent;
use serde_json::Value;

/// Audit entry schema version, shared with the S3 and KMS paths so a consumer
/// parses these entries with the parser it already has.
const AUDIT_ENTRY_VERSION: &str = "1.0";

/// `trigger` value marking an entry as produced by the account/MFA API.
const AUDIT_TRIGGER: &str = "account-admin";

/// `type` value letting a consumer separate identity entries from S3 and KMS
/// ones without enumerating operation names.
const AUDIT_ENTRY_TYPE: &str = "iam-identity";

/// The operations audited by this module.
///
/// The [`EventName`] enum is deliberately coarse for IAM (two variants for the
/// whole surface, because `mask()` is nearly out of bits), so this is where the
/// per-operation detail lives. Consumers filter on `api.name` and the
/// `iamOperation` tag, both fed from here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountAuditOperation {
    /// The caller rotated its own secret key.
    ChangeOwnPassword,
    /// An administrator reset another identity's secret key.
    ResetUserPassword,
    /// A TOTP enrollment was started.
    MfaEnroll,
    /// A started enrollment was confirmed and the second factor became active.
    MfaActivate,
    /// The second factor was turned off.
    MfaDisable,
    /// Recovery codes were replaced.
    MfaRecoveryCodesRegenerated,
    /// An administrator cleared another identity's second factor.
    AdminResetUserMfa,
    /// A second factor was presented during session minting.
    MfaVerify,
    /// A login challenge was issued because the identity requires a second
    /// factor.
    MfaChallengeIssued,
}

impl AccountAuditOperation {
    /// Stable operation name, recorded as `api.name`.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ChangeOwnPassword => "AccountChangePassword",
            Self::ResetUserPassword => "AdminSetUserSecretKey",
            Self::MfaEnroll => "AccountMfaEnroll",
            Self::MfaActivate => "AccountMfaActivate",
            Self::MfaDisable => "AccountMfaDisable",
            Self::MfaRecoveryCodesRegenerated => "AccountMfaRecoveryCodes",
            Self::AdminResetUserMfa => "AdminResetUserMfa",
            Self::MfaVerify => "MfaVerify",
            Self::MfaChallengeIssued => "MfaChallenge",
        }
    }

    /// Which of the two IAM event classes this operation belongs to.
    const fn event(self) -> EventName {
        match self {
            Self::MfaVerify | Self::MfaChallengeIssued => EventName::IamIdentityAuthChallenge,
            _ => EventName::IamIdentityCredentialChanged,
        }
    }
}

/// Closed vocabulary of audited failure reasons.
///
/// A closed set rather than the error message: an error rendered from request
/// data would otherwise carry that data into the audit log, and an audit log is
/// a poor place to discover a leaked code or secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountAuditFailure {
    /// The submitted current secret key did not match.
    InvalidCurrentSecret,
    /// The submitted TOTP or recovery code did not verify.
    InvalidCode,
    /// Too many failed attempts; the identity is temporarily locked.
    RateLimited,
    /// The submitted challenge was malformed, unsigned, or for another identity.
    ChallengeInvalid,
    /// The authorization gate rejected the request.
    AccessDenied,
    /// The request was well-formed but not allowed for this credential kind.
    NotPermittedForCredential,
    /// The new secret key failed validation.
    InvalidNewSecret,
    /// No enrollment exists to act on.
    NotEnrolled,
    /// At-rest protection for the TOTP secret is unavailable.
    EnrollmentUnavailable,
    /// The operation failed for an internal reason.
    Internal,
}

impl AccountAuditFailure {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidCurrentSecret => "invalid_current_secret",
            Self::InvalidCode => "invalid_code",
            Self::RateLimited => "rate_limited",
            Self::ChallengeInvalid => "challenge_invalid",
            Self::AccessDenied => "access_denied",
            Self::NotPermittedForCredential => "not_permitted_for_credential",
            Self::InvalidNewSecret => "invalid_new_secret",
            Self::NotEnrolled => "not_enrolled",
            Self::EnrollmentUnavailable => "enrollment_unavailable",
            Self::Internal => "internal_error",
        }
    }
}

/// Which second factor satisfied a verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MfaMethod {
    Totp,
    RecoveryCode,
}

impl MfaMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Totp => "totp",
            Self::RecoveryCode => "recovery-code",
        }
    }
}

/// Request-scoped context copied out of a request before it is consumed.
///
/// Handlers take the body by value, so the fields an entry needs are captured
/// up front rather than borrowed at emit time.
#[derive(Debug, Clone, Default)]
pub(crate) struct AccountAuditContext {
    remote_host: Option<String>,
    request_id: Option<String>,
    user_agent: Option<String>,
    req_path: Option<String>,
    request_context: Option<RequestContext>,
}

impl AccountAuditContext {
    pub(crate) fn from_request(req: &S3Request<Body>) -> Self {
        let user_agent = get_request_user_agent(&req.headers);
        let request_context = request_context_from_extensions(&req.extensions);

        Self {
            remote_host: req
                .extensions
                .get::<Option<RemoteAddr>>()
                .and_then(|opt| opt.map(|addr| addr.0.ip().to_string())),
            request_id: request_context.as_ref().map(|ctx| ctx.request_id.clone()),
            user_agent: (!user_agent.is_empty()).then_some(user_agent),
            req_path: Some(req.uri.path().to_string()),
            request_context,
        }
    }
}

/// One audited account or MFA operation.
pub(crate) struct AccountAuditRecord<'a> {
    pub(crate) operation: AccountAuditOperation,
    /// The durable identity the operation acted on.
    pub(crate) identity: &'a str,
    pub(crate) identity_type: IdentityType,
    /// The credential that signed the request, when it differs from `identity`.
    pub(crate) session_access_key: Option<&'a str>,
    pub(crate) failure: Option<AccountAuditFailure>,
    pub(crate) mfa_method: Option<MfaMethod>,
    /// Sessions invalidated as a side effect, when the operation revokes any.
    pub(crate) sessions_revoked: Option<usize>,
    /// Recovery codes left after the operation, when it changes the count.
    pub(crate) recovery_codes_remaining: Option<u32>,
}

impl<'a> AccountAuditRecord<'a> {
    /// A successful operation on `identity`.
    pub(crate) fn success(operation: AccountAuditOperation, identity: &'a str, identity_type: IdentityType) -> Self {
        Self {
            operation,
            identity,
            identity_type,
            session_access_key: None,
            failure: None,
            mfa_method: None,
            sessions_revoked: None,
            recovery_codes_remaining: None,
        }
    }

    /// A rejected operation on `identity`.
    pub(crate) fn failure(
        operation: AccountAuditOperation,
        identity: &'a str,
        identity_type: IdentityType,
        failure: AccountAuditFailure,
    ) -> Self {
        Self {
            failure: Some(failure),
            ..Self::success(operation, identity, identity_type)
        }
    }

    pub(crate) const fn with_session_access_key(mut self, session_access_key: Option<&'a str>) -> Self {
        self.session_access_key = session_access_key;
        self
    }

    pub(crate) const fn with_mfa_method(mut self, method: MfaMethod) -> Self {
        self.mfa_method = Some(method);
        self
    }

    pub(crate) const fn with_sessions_revoked(mut self, revoked: usize) -> Self {
        self.sessions_revoked = Some(revoked);
        self
    }

    pub(crate) const fn with_recovery_codes_remaining(mut self, remaining: u32) -> Self {
        self.recovery_codes_remaining = Some(remaining);
        self
    }
}

/// Emit one entry, best effort.
///
/// The operation has already completed when this is called and nothing here can
/// change its result, matching the pipeline's established semantics.
pub(crate) fn emit(context: &AccountAuditContext, record: AccountAuditRecord<'_>) {
    let entry = build_entry(context, &record);
    let request_context = context.request_context.clone();
    spawn_background_with_context(request_context, async move {
        AuditLogger::log(entry).await;
    });
}

fn build_entry(context: &AccountAuditContext, record: &AccountAuditRecord<'_>) -> AuditEntry {
    let status = if record.failure.is_some() { "failure" } else { "success" };

    let api = ApiDetailsBuilder::new()
        .name(record.operation.as_str())
        .status(status)
        .build();

    let mut builder = AuditEntryBuilder::new(AUDIT_ENTRY_VERSION, record.operation.event(), AUDIT_TRIGGER, api)
        .entry_type(AUDIT_ENTRY_TYPE)
        .access_key(record.identity)
        .tags(entry_tags(record));

    // The durable identity goes in `access_key`; when a derived credential
    // signed the request, `parent_user` records which one, so an investigator
    // can tell "root changed its own password" from "an STS session did".
    if let Some(session_access_key) = record.session_access_key {
        builder = builder.parent_user(session_access_key);
    }
    if let Some(remote_host) = context.remote_host.as_deref() {
        builder = builder.remote_host(remote_host);
    }
    if let Some(request_id) = context.request_id.as_deref() {
        builder = builder.request_id(request_id);
    }
    if let Some(user_agent) = context.user_agent.as_deref() {
        builder = builder.user_agent(user_agent);
    }
    if let Some(req_path) = context.req_path.as_deref() {
        builder = builder.req_path(req_path);
    }
    if let Some(failure) = record.failure {
        builder = builder.error(failure.as_str());
    }

    builder.build()
}

fn entry_tags(record: &AccountAuditRecord<'_>) -> HashMap<String, Value> {
    let mut tags = HashMap::new();
    tags.insert("iamOperation".to_string(), Value::String(record.operation.as_str().to_string()));
    tags.insert("identityType".to_string(), Value::String(record.identity_type.as_str().to_string()));

    if let Some(method) = record.mfa_method {
        tags.insert("mfaMethod".to_string(), Value::String(method.as_str().to_string()));
    }
    if let Some(revoked) = record.sessions_revoked {
        tags.insert("sessionsRevoked".to_string(), Value::Number(revoked.into()));
    }
    if let Some(remaining) = record.recovery_codes_remaining {
        tags.insert("recoveryCodesRemaining".to_string(), Value::Number(remaining.into()));
    }

    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> AccountAuditContext {
        AccountAuditContext {
            remote_host: Some("203.0.113.7".to_string()),
            request_id: Some("req-1".to_string()),
            user_agent: Some("rc/0.1".to_string()),
            req_path: Some("/rustfs/admin/v3/account/password".to_string()),
            request_context: None,
        }
    }

    #[test]
    fn successful_password_change_is_recorded_as_a_credential_change() {
        let entry = build_entry(
            &context(),
            &AccountAuditRecord::success(AccountAuditOperation::ChangeOwnPassword, "sinan", IdentityType::Iam)
                .with_session_access_key(Some("TEMPKEY"))
                .with_sessions_revoked(3),
        );

        assert_eq!(entry.event, EventName::IamIdentityCredentialChanged);
        assert_eq!(entry.api.name.as_deref(), Some("AccountChangePassword"));
        assert_eq!(entry.api.status.as_deref(), Some("success"));
        assert_eq!(entry.access_key.as_deref(), Some("sinan"));
        assert_eq!(entry.parent_user.as_deref(), Some("TEMPKEY"));
        assert!(entry.error.is_none());

        let tags = entry.tags.expect("tags");
        assert_eq!(tags.get("iamOperation"), Some(&Value::String("AccountChangePassword".into())));
        assert_eq!(tags.get("identityType"), Some(&Value::String("iam".into())));
        assert_eq!(tags.get("sessionsRevoked"), Some(&Value::Number(3.into())));
    }

    #[test]
    fn mfa_verification_is_recorded_as_an_auth_challenge() {
        let entry = build_entry(
            &context(),
            &AccountAuditRecord::success(AccountAuditOperation::MfaVerify, "sinan", IdentityType::Iam)
                .with_mfa_method(MfaMethod::RecoveryCode)
                .with_recovery_codes_remaining(9),
        );

        assert_eq!(entry.event, EventName::IamIdentityAuthChallenge);
        let tags = entry.tags.expect("tags");
        assert_eq!(tags.get("mfaMethod"), Some(&Value::String("recovery-code".into())));
        assert_eq!(tags.get("recoveryCodesRemaining"), Some(&Value::Number(9.into())));
    }

    #[test]
    fn failures_record_the_class_and_never_the_submitted_value() {
        let entry = build_entry(
            &context(),
            &AccountAuditRecord::failure(
                AccountAuditOperation::MfaVerify,
                "sinan",
                IdentityType::Iam,
                AccountAuditFailure::InvalidCode,
            ),
        );

        assert_eq!(entry.api.status.as_deref(), Some("failure"));
        assert_eq!(entry.error.as_deref(), Some("invalid_code"));

        // The whole serialized entry must not contain anything code-shaped: the
        // point of the closed failure vocabulary is that no submitted value can
        // reach a log target through here.
        let encoded = serde_json::to_string(&entry).expect("serialize");
        assert!(!encoded.contains("123456"), "audit entry must never echo a submitted code");
    }

    #[test]
    fn every_operation_maps_to_an_iam_event() {
        for operation in [
            AccountAuditOperation::ChangeOwnPassword,
            AccountAuditOperation::ResetUserPassword,
            AccountAuditOperation::MfaEnroll,
            AccountAuditOperation::MfaActivate,
            AccountAuditOperation::MfaDisable,
            AccountAuditOperation::MfaRecoveryCodesRegenerated,
            AccountAuditOperation::AdminResetUserMfa,
            AccountAuditOperation::MfaVerify,
            AccountAuditOperation::MfaChallengeIssued,
        ] {
            let event = operation.event();
            assert!(event.is_iam(), "{} must map to an IAM event, got {event}", operation.as_str());
            assert!(!operation.as_str().is_empty());
        }
    }
}
