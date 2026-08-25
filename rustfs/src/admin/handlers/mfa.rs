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

//! Two-factor authentication endpoints.
//!
//! Self-service, acting on the caller:
//!
//! * `GET  /rustfs/admin/v3/account/mfa`                 — current state
//! * `POST /rustfs/admin/v3/account/mfa/enroll`          — start enrollment
//! * `POST /rustfs/admin/v3/account/mfa/activate`         — confirm enrollment
//! * `POST /rustfs/admin/v3/account/mfa/disable`          — turn it off
//! * `POST /rustfs/admin/v3/account/mfa/recovery-codes`   — replace the codes
//!
//! Login:
//!
//! * `GET  /rustfs/admin/v3/mfa/challenge`                — is a factor needed?
//!
//! Administrative, acting on another identity:
//!
//! * `GET    /rustfs/admin/v3/user/mfa?accessKey=…`       — inspect
//! * `DELETE /rustfs/admin/v3/user/mfa?accessKey=…`       — break-glass reset
//!
//! Every handler here is HTTP plumbing: authorization, deserialization,
//! serialization and audit. The state machine lives in
//! [`rustfs_iam::mfa`], so the console and the CLI exercise identical logic.
//!
//! `disable` is a `POST` rather than a `DELETE` because it carries a body — the
//! second factor *and* the account password. A `DELETE` with a signed body is
//! legal but awkward for enough HTTP clients that it is not worth the purity.

use super::account_audit::{
    AccountAuditContext, AccountAuditFailure, AccountAuditOperation, AccountAuditRecord, MfaMethod, emit as emit_audit,
};
use super::admin_json_response;
use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{current_token_signing_key, object_store_from_req};
use crate::admin::service::caller_identity::CallerIdentity;
use crate::admin::storage_api::runtime::ECStore;
use crate::admin::storage_api::s3::{self, Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use crate::admin::utils::read_compatible_admin_body;
use crate::auth::constant_time_eq;
use crate::server::RemoteAddr;
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_iam::mfa::{MfaServiceError, MfaVerification, service as mfa_service};
use rustfs_madmin::account::{MfaChallengeResponse, MfaCodeRequest, MfaDisableRequest, UserMfaStatus};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_utils::MaskedAccessKey;
use serde::Deserialize;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{info, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_MFA: &str = "mfa";
const EVENT_ADMIN_MFA_STATE: &str = "admin_mfa_state";

pub(crate) const ACCOUNT_MFA_ROUTE: &str = "/rustfs/admin/v3/account/mfa";
pub(crate) const ACCOUNT_MFA_ENROLL_ROUTE: &str = "/rustfs/admin/v3/account/mfa/enroll";
pub(crate) const ACCOUNT_MFA_ACTIVATE_ROUTE: &str = "/rustfs/admin/v3/account/mfa/activate";
pub(crate) const ACCOUNT_MFA_DISABLE_ROUTE: &str = "/rustfs/admin/v3/account/mfa/disable";
pub(crate) const ACCOUNT_MFA_RECOVERY_CODES_ROUTE: &str = "/rustfs/admin/v3/account/mfa/recovery-codes";
pub(crate) const MFA_CHALLENGE_ROUTE: &str = "/rustfs/admin/v3/mfa/challenge";
pub(crate) const USER_MFA_ROUTE: &str = "/rustfs/admin/v3/user/mfa";

pub fn register_mfa_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(Method::GET, ACCOUNT_MFA_ROUTE, AdminOperation(&AccountMfaStatusHandler {}))?;
    r.insert(Method::POST, ACCOUNT_MFA_ENROLL_ROUTE, AdminOperation(&AccountMfaEnrollHandler {}))?;
    r.insert(Method::POST, ACCOUNT_MFA_ACTIVATE_ROUTE, AdminOperation(&AccountMfaActivateHandler {}))?;
    r.insert(Method::POST, ACCOUNT_MFA_DISABLE_ROUTE, AdminOperation(&AccountMfaDisableHandler {}))?;
    r.insert(
        Method::POST,
        ACCOUNT_MFA_RECOVERY_CODES_ROUTE,
        AdminOperation(&AccountMfaRecoveryCodesHandler {}),
    )?;
    r.insert(Method::GET, MFA_CHALLENGE_ROUTE, AdminOperation(&MfaChallengeHandler {}))?;
    r.insert(Method::GET, USER_MFA_ROUTE, AdminOperation(&UserMfaStatusHandler {}))?;
    r.insert(Method::DELETE, USER_MFA_ROUTE, AdminOperation(&UserMfaResetHandler {}))?;

    Ok(())
}

/// Map a service failure onto the wire.
///
/// `InvalidCode` becomes `AccessDenied` with a fixed message: the service has
/// already collapsed wrong, replayed and malformed codes into one variant, and
/// the message must not reintroduce the distinction.
fn map_service_error(error: MfaServiceError) -> S3Error {
    match error {
        MfaServiceError::EnrollmentUnavailable(reason) => S3Error::with_message(S3ErrorCode::NotImplemented, reason.to_string()),
        MfaServiceError::NotEnabled => {
            S3Error::with_message(S3ErrorCode::InvalidRequest, "two-factor authentication is not enabled".to_string())
        }
        MfaServiceError::NoPendingEnrollment => S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "there is no pending enrollment to confirm; start setup again".to_string(),
        ),
        MfaServiceError::AlreadyEnabled => {
            S3Error::with_message(S3ErrorCode::InvalidRequest, "two-factor authentication is already enabled".to_string())
        }
        MfaServiceError::InvalidCode => {
            S3Error::with_message(S3ErrorCode::AccessDenied, "the verification code is invalid".to_string())
        }
        // `SlowDown` is the S3 vocabulary's closest analogue to 429, and clients
        // already treat it as "back off" rather than "retry immediately".
        MfaServiceError::Locked { retry_after_seconds } => S3Error::with_message(
            S3ErrorCode::SlowDown,
            format!("too many failed attempts; try again in {retry_after_seconds} seconds"),
        ),
        MfaServiceError::InvalidChallenge => {
            S3Error::with_message(S3ErrorCode::AccessDenied, "the login challenge is invalid or has expired".to_string())
        }
        MfaServiceError::Internal(message) => S3Error::with_message(S3ErrorCode::InternalError, message),
    }
}

/// Audit class for a service failure, so every handler classifies the same way.
fn audit_failure_for(error: &MfaServiceError) -> AccountAuditFailure {
    match error {
        MfaServiceError::EnrollmentUnavailable(_) => AccountAuditFailure::EnrollmentUnavailable,
        MfaServiceError::NotEnabled | MfaServiceError::NoPendingEnrollment => AccountAuditFailure::NotEnrolled,
        MfaServiceError::AlreadyEnabled => AccountAuditFailure::NotPermittedForCredential,
        MfaServiceError::InvalidCode => AccountAuditFailure::InvalidCode,
        MfaServiceError::Locked { .. } => AccountAuditFailure::RateLimited,
        MfaServiceError::InvalidChallenge => AccountAuditFailure::ChallengeInvalid,
        MfaServiceError::Internal(_) => AccountAuditFailure::Internal,
    }
}

fn store_from_req(req: &S3Request<Body>) -> S3Result<Arc<ECStore>> {
    object_store_from_req(req).ok_or_else(|| s3::error(S3ErrorCode::ServiceUnavailable, "the object store is not ready"))
}

/// Resolve the caller and confirm this credential kind may manage a second
/// factor for its identity.
async fn resolve_self_service_caller(req: &S3Request<Body>) -> S3Result<CallerIdentity> {
    let caller = CallerIdentity::resolve(req).await?;
    // The MFA capability, not the password one: a root identity may enroll a
    // second factor even though its secret key is fixed for the life of the
    // process.
    caller.ensure_mfa_management_allowed()?;
    Ok(caller)
}

/// `GET /rustfs/admin/v3/account/mfa`
pub struct AccountMfaStatusHandler {}

#[async_trait::async_trait]
impl Operation for AccountMfaStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Reading state is allowed for every credential kind: a service account
        // may need to know whether its parent is protected, even though it may
        // not change that.
        let caller = CallerIdentity::resolve(&req).await?;
        let store = store_from_req(&req)?;

        let status = mfa_service::status(store, &caller.access_key, OffsetDateTime::now_utc())
            .await
            .map_err(map_service_error)?;

        admin_json_response(req.uri.path(), &caller.credentials.secret_key, StatusCode::OK, &status)
    }
}

/// `POST /rustfs/admin/v3/account/mfa/enroll`
pub struct AccountMfaEnrollHandler {}

#[async_trait::async_trait]
impl Operation for AccountMfaEnrollHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let caller = resolve_self_service_caller(&req).await?;
        let audit = AccountAuditContext::from_request(&req);
        let store = store_from_req(&req)?;

        let response = match mfa_service::enroll(store, &caller.access_key, OffsetDateTime::now_utc()).await {
            Ok(response) => response,
            Err(err) => {
                emit_audit(
                    &audit,
                    AccountAuditRecord::failure(
                        AccountAuditOperation::MfaEnroll,
                        &caller.access_key,
                        caller.identity_type,
                        audit_failure_for(&err),
                    )
                    .with_session_access_key(caller.session_access_key.as_deref()),
                );
                return Err(map_service_error(err));
            }
        };

        emit_audit(
            &audit,
            AccountAuditRecord::success(AccountAuditOperation::MfaEnroll, &caller.access_key, caller.identity_type)
                .with_session_access_key(caller.session_access_key.as_deref()),
        );
        info!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_MFA,
            event = EVENT_ADMIN_MFA_STATE,
            action = "enroll",
            access_key = %MaskedAccessKey(&caller.access_key),
            result = "pending",
            "admin mfa state"
        );

        admin_json_response(req.uri.path(), &caller.credentials.secret_key, StatusCode::OK, &response)
    }
}

/// `POST /rustfs/admin/v3/account/mfa/activate`
pub struct AccountMfaActivateHandler {}

#[async_trait::async_trait]
impl Operation for AccountMfaActivateHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let caller = resolve_self_service_caller(&req).await?;
        let audit = AccountAuditContext::from_request(&req);
        let store = store_from_req(&req)?;
        let path = req.uri.path().to_string();

        let body =
            read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, &path, &caller.credentials.secret_key).await?;
        let request: MfaCodeRequest = serde_json::from_slice(&body)
            .map_err(|e| s3::error(S3ErrorCode::InvalidRequest, format!("invalid activation request: {e}")))?;

        let response = match mfa_service::activate(store, &caller.access_key, &request.code, OffsetDateTime::now_utc()).await {
            Ok(response) => response,
            Err(err) => {
                warn!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_MFA,
                    event = EVENT_ADMIN_MFA_STATE,
                    action = "activate",
                    access_key = %MaskedAccessKey(&caller.access_key),
                    result = %err.audit_class(),
                    "admin mfa state"
                );
                emit_audit(
                    &audit,
                    AccountAuditRecord::failure(
                        AccountAuditOperation::MfaActivate,
                        &caller.access_key,
                        caller.identity_type,
                        audit_failure_for(&err),
                    )
                    .with_session_access_key(caller.session_access_key.as_deref()),
                );
                return Err(map_service_error(err));
            }
        };

        emit_audit(
            &audit,
            AccountAuditRecord::success(AccountAuditOperation::MfaActivate, &caller.access_key, caller.identity_type)
                .with_session_access_key(caller.session_access_key.as_deref())
                .with_recovery_codes_remaining(response.recovery_codes.len() as u32),
        );

        admin_json_response(&path, &caller.credentials.secret_key, StatusCode::OK, &response)
    }
}

/// `POST /rustfs/admin/v3/account/mfa/disable`
pub struct AccountMfaDisableHandler {}

#[async_trait::async_trait]
impl Operation for AccountMfaDisableHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let caller = resolve_self_service_caller(&req).await?;
        let audit = AccountAuditContext::from_request(&req);
        let store = store_from_req(&req)?;
        let path = req.uri.path().to_string();

        let body =
            read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, &path, &caller.credentials.secret_key).await?;
        let request: MfaDisableRequest = serde_json::from_slice(&body)
            .map_err(|e| s3::error(S3ErrorCode::InvalidRequest, format!("invalid disable request: {e}")))?;

        // Step-up: the second factor alone is not enough to remove the second
        // factor. The console signs with a short-lived STS session, so a
        // hijacked tab would otherwise be able to strip the protection using
        // only a code shoulder-surfed once.
        let iam_store = crate::admin::runtime_sources::current_ready_iam_handle()
            .map_err(|_| s3::error(S3ErrorCode::InternalError, "iam is not initialized"))?;
        let Some(stored) = iam_store.get_user(&caller.access_key).await else {
            return Err(s3::error(S3ErrorCode::InvalidRequest, "the calling identity no longer exists"));
        };
        if !constant_time_eq(&request.current_secret_key, &stored.credentials.secret_key) {
            emit_audit(
                &audit,
                AccountAuditRecord::failure(
                    AccountAuditOperation::MfaDisable,
                    &caller.access_key,
                    caller.identity_type,
                    AccountAuditFailure::InvalidCurrentSecret,
                )
                .with_session_access_key(caller.session_access_key.as_deref()),
            );
            return Err(s3::error(S3ErrorCode::AccessDenied, "the current secret key is incorrect"));
        }

        if let Err(err) = mfa_service::disable(store, &caller.access_key, &request.code, OffsetDateTime::now_utc()).await {
            emit_audit(
                &audit,
                AccountAuditRecord::failure(
                    AccountAuditOperation::MfaDisable,
                    &caller.access_key,
                    caller.identity_type,
                    audit_failure_for(&err),
                )
                .with_session_access_key(caller.session_access_key.as_deref()),
            );
            return Err(map_service_error(err));
        }

        emit_audit(
            &audit,
            AccountAuditRecord::success(AccountAuditOperation::MfaDisable, &caller.access_key, caller.identity_type)
                .with_session_access_key(caller.session_access_key.as_deref()),
        );
        info!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_MFA,
            event = EVENT_ADMIN_MFA_STATE,
            action = "disable",
            access_key = %MaskedAccessKey(&caller.access_key),
            result = "disabled",
            "admin mfa state"
        );

        Ok(empty_ok())
    }
}

/// `POST /rustfs/admin/v3/account/mfa/recovery-codes`
pub struct AccountMfaRecoveryCodesHandler {}

#[async_trait::async_trait]
impl Operation for AccountMfaRecoveryCodesHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let caller = resolve_self_service_caller(&req).await?;
        let audit = AccountAuditContext::from_request(&req);
        let store = store_from_req(&req)?;
        let path = req.uri.path().to_string();

        let body =
            read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, &path, &caller.credentials.secret_key).await?;
        let request: MfaCodeRequest = serde_json::from_slice(&body)
            .map_err(|e| s3::error(S3ErrorCode::InvalidRequest, format!("invalid recovery-code request: {e}")))?;

        let response =
            match mfa_service::regenerate_recovery_codes(store, &caller.access_key, &request.code, OffsetDateTime::now_utc())
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    emit_audit(
                        &audit,
                        AccountAuditRecord::failure(
                            AccountAuditOperation::MfaRecoveryCodesRegenerated,
                            &caller.access_key,
                            caller.identity_type,
                            audit_failure_for(&err),
                        )
                        .with_session_access_key(caller.session_access_key.as_deref()),
                    );
                    return Err(map_service_error(err));
                }
            };

        emit_audit(
            &audit,
            AccountAuditRecord::success(
                AccountAuditOperation::MfaRecoveryCodesRegenerated,
                &caller.access_key,
                caller.identity_type,
            )
            .with_session_access_key(caller.session_access_key.as_deref())
            .with_recovery_codes_remaining(response.recovery_codes.len() as u32),
        );

        admin_json_response(&path, &caller.credentials.secret_key, StatusCode::OK, &response)
    }
}

/// `GET /rustfs/admin/v3/mfa/challenge`
///
/// Answers "does this identity need a second factor?" for a caller that has
/// already proved it holds the identity's secret key, since the request is
/// signed. That signature requirement is what keeps this from being an
/// enumeration oracle: a caller only ever learns about the identity whose
/// credentials it already has.
pub struct MfaChallengeHandler {}

#[async_trait::async_trait]
impl Operation for MfaChallengeHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let caller = CallerIdentity::resolve(&req).await?;
        let audit = AccountAuditContext::from_request(&req);
        let store = store_from_req(&req)?;
        let now = OffsetDateTime::now_utc();

        let required = mfa_service::is_enabled(store, &caller.access_key, now)
            .await
            .map_err(map_service_error)?;

        let response = if required {
            let Some(signing_key) = current_token_signing_key() else {
                return Err(s3::error(S3ErrorCode::InternalError, "the session signing key is not initialized"));
            };
            let challenge = mfa_service::issue_challenge(&caller.access_key, now, signing_key.as_bytes());

            emit_audit(
                &audit,
                AccountAuditRecord::success(AccountAuditOperation::MfaChallengeIssued, &caller.access_key, caller.identity_type)
                    .with_session_access_key(caller.session_access_key.as_deref()),
            );

            MfaChallengeResponse {
                required: true,
                challenge: Some(challenge),
                expires_at: Some(now + time::Duration::seconds(mfa_service::challenge_ttl_seconds() as i64)),
            }
        } else {
            MfaChallengeResponse {
                required: false,
                challenge: None,
                expires_at: None,
            }
        };

        admin_json_response(req.uri.path(), &caller.credentials.secret_key, StatusCode::OK, &response)
    }
}

#[derive(Debug, Deserialize, Default)]
struct UserMfaQuery {
    #[serde(rename = "accessKey", alias = "access-key")]
    access_key: Option<String>,
}

fn parse_user_mfa_query(req: &S3Request<Body>) -> S3Result<String> {
    let query: UserMfaQuery = match req.uri.query() {
        Some(query) => {
            serde_urlencoded::from_str(query).map_err(|_| s3::error(S3ErrorCode::InvalidArgument, "failed to decode query"))?
        }
        None => UserMfaQuery::default(),
    };

    let access_key = query.access_key.unwrap_or_default();
    if access_key.is_empty() {
        return Err(s3::error(S3ErrorCode::InvalidArgument, "access key is empty"));
    }
    Ok(access_key)
}

/// `GET /rustfs/admin/v3/user/mfa?accessKey=…`
pub struct UserMfaStatusHandler {}

#[async_trait::async_trait]
impl Operation for UserMfaStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let target = parse_user_mfa_query(&req)?;
        let caller = CallerIdentity::resolve(&req).await?;

        validate_admin_request(
            &req.headers,
            &caller.credentials,
            caller.is_owner,
            false,
            vec![Action::AdminAction(AdminAction::GetUserAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let store = store_from_req(&req)?;
        let status: UserMfaStatus = mfa_service::admin_status(store, &target, OffsetDateTime::now_utc())
            .await
            .map_err(map_service_error)?;

        admin_json_response(req.uri.path(), &caller.credentials.secret_key, StatusCode::OK, &status)
    }
}

/// `DELETE /rustfs/admin/v3/user/mfa?accessKey=…`
///
/// The break-glass path: an administrator clears the second factor for a user
/// who lost both their authenticator and their recovery codes.
///
/// Gated on `EnableUser`, not on a bespoke action, because the capability being
/// exercised is the same one that can already re-enable a disabled account —
/// anyone who can do that can already take over the identity, so a separate
/// action would be a distinction without a security difference.
pub struct UserMfaResetHandler {}

#[async_trait::async_trait]
impl Operation for UserMfaResetHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let target = parse_user_mfa_query(&req)?;
        let caller = CallerIdentity::resolve(&req).await?;
        let audit = AccountAuditContext::from_request(&req);

        validate_admin_request(
            &req.headers,
            &caller.credentials,
            caller.is_owner,
            false,
            vec![Action::AdminAction(AdminAction::EnableUserAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await
        .inspect_err(|_| {
            emit_audit(
                &audit,
                AccountAuditRecord::failure(
                    AccountAuditOperation::AdminResetUserMfa,
                    &target,
                    caller.identity_type,
                    AccountAuditFailure::AccessDenied,
                )
                .with_session_access_key(Some(caller.access_key.as_str())),
            );
        })?;

        let store = store_from_req(&req)?;
        mfa_service::admin_reset(store, &target).await.map_err(map_service_error)?;

        emit_audit(
            &audit,
            AccountAuditRecord::success(AccountAuditOperation::AdminResetUserMfa, &target, caller.identity_type)
                // The acting administrator, recorded so a reset is always
                // attributable to a person and not just to the target.
                .with_session_access_key(Some(caller.access_key.as_str())),
        );
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_MFA,
            event = EVENT_ADMIN_MFA_STATE,
            action = "admin_reset",
            target_access_key = %MaskedAccessKey(&target),
            actor_access_key = %MaskedAccessKey(&caller.access_key),
            result = "reset",
            "admin mfa state"
        );

        Ok(empty_ok())
    }
}

/// Verify a second factor on behalf of the session-minting path.
///
/// Lives here so the STS handler does not have to know the MFA service, the
/// audit vocabulary, or how a challenge is validated.
pub(crate) async fn verify_for_session(
    store: Arc<ECStore>,
    audit: &AccountAuditContext,
    access_key: &str,
    identity_type: rustfs_madmin::account::IdentityType,
    challenge: Option<&str>,
    code: &str,
) -> S3Result<MfaVerification> {
    let now = OffsetDateTime::now_utc();

    // The challenge is validated first: it is cheap, and a stale one should not
    // consume an attempt against the rate limiter.
    if let Some(challenge) = challenge.filter(|value| !value.is_empty()) {
        let Some(signing_key) = current_token_signing_key() else {
            return Err(s3::error(S3ErrorCode::InternalError, "the session signing key is not initialized"));
        };
        if let Err(err) = mfa_service::validate_challenge(challenge, access_key, now, signing_key.as_bytes()) {
            emit_audit(
                audit,
                AccountAuditRecord::failure(
                    AccountAuditOperation::MfaVerify,
                    access_key,
                    identity_type,
                    AccountAuditFailure::ChallengeInvalid,
                ),
            );
            return Err(map_service_error(err));
        }
    }

    match mfa_service::verify(store, access_key, code, now).await {
        Ok(verification) => {
            let method = match verification {
                MfaVerification::Totp => MfaMethod::Totp,
                MfaVerification::RecoveryCode { .. } => MfaMethod::RecoveryCode,
            };
            let mut record =
                AccountAuditRecord::success(AccountAuditOperation::MfaVerify, access_key, identity_type).with_mfa_method(method);
            if let MfaVerification::RecoveryCode { remaining } = verification {
                record = record.with_recovery_codes_remaining(remaining);
                warn!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_MFA,
                    event = EVENT_ADMIN_MFA_STATE,
                    action = "verify",
                    access_key = %MaskedAccessKey(access_key),
                    recovery_codes_remaining = remaining,
                    result = "recovery_code_used",
                    "admin mfa state"
                );
            }
            emit_audit(audit, record);
            Ok(verification)
        }
        Err(err) => {
            emit_audit(
                audit,
                AccountAuditRecord::failure(AccountAuditOperation::MfaVerify, access_key, identity_type, audit_failure_for(&err)),
            );
            Err(map_service_error(err))
        }
    }
}

fn empty_ok() -> S3Response<(StatusCode, Body)> {
    let mut header = hyper::HeaderMap::new();
    header.insert(s3::header::CONTENT_LENGTH, "0".parse().expect("valid header value"));
    S3Response::with_headers((StatusCode::OK, Body::empty()), header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::ADMIN_PREFIX;

    #[test]
    fn routes_are_registered() {
        let mut router: S3Router<AdminOperation> = S3Router::new(false);
        register_mfa_route(&mut router).expect("register mfa routes");

        assert!(router.contains_route(Method::GET, ACCOUNT_MFA_ROUTE));
        assert!(router.contains_route(Method::POST, ACCOUNT_MFA_ENROLL_ROUTE));
        assert!(router.contains_route(Method::POST, ACCOUNT_MFA_ACTIVATE_ROUTE));
        assert!(router.contains_route(Method::POST, ACCOUNT_MFA_DISABLE_ROUTE));
        assert!(router.contains_route(Method::POST, ACCOUNT_MFA_RECOVERY_CODES_ROUTE));
        assert!(router.contains_route(Method::GET, MFA_CHALLENGE_ROUTE));
        assert!(router.contains_route(Method::GET, USER_MFA_ROUTE));
        assert!(router.contains_route(Method::DELETE, USER_MFA_ROUTE));
    }

    #[test]
    fn route_constants_stay_under_the_admin_prefix() {
        for route in [
            ACCOUNT_MFA_ROUTE,
            ACCOUNT_MFA_ENROLL_ROUTE,
            ACCOUNT_MFA_ACTIVATE_ROUTE,
            ACCOUNT_MFA_DISABLE_ROUTE,
            ACCOUNT_MFA_RECOVERY_CODES_ROUTE,
            MFA_CHALLENGE_ROUTE,
            USER_MFA_ROUTE,
        ] {
            assert!(route.starts_with(ADMIN_PREFIX), "{route} is outside the admin prefix");
        }
    }

    #[test]
    fn wrong_and_replayed_codes_produce_the_same_response() {
        // The service already collapses them; this pins that the HTTP layer does
        // not reintroduce the distinction through its message or status.
        let first = map_service_error(MfaServiceError::InvalidCode);
        let second = map_service_error(MfaServiceError::InvalidCode);

        assert_eq!(first.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(first.to_string(), second.to_string());
        assert!(!first.to_string().contains("replay"));
    }

    #[test]
    fn a_lockout_is_reported_as_backpressure_with_its_retry_hint() {
        let error = map_service_error(MfaServiceError::Locked {
            retry_after_seconds: 900,
        });

        assert_eq!(error.code(), &S3ErrorCode::SlowDown);
        assert!(error.to_string().contains("900"), "{error}");
    }

    #[test]
    fn an_unavailable_enrollment_reports_the_remedy() {
        let error = map_service_error(MfaServiceError::EnrollmentUnavailable(
            rustfs_iam::mfa::store::ENROLLMENT_UNAVAILABLE_REASON,
        ));

        assert_eq!(error.code(), &S3ErrorCode::NotImplemented);
        assert!(error.to_string().contains("RUSTFS_IAM_MASTER_KEY"), "{error}");
    }

    #[test]
    fn service_failures_all_have_an_audit_class() {
        for error in [
            MfaServiceError::NotEnabled,
            MfaServiceError::NoPendingEnrollment,
            MfaServiceError::InvalidCode,
            MfaServiceError::Locked { retry_after_seconds: 1 },
            MfaServiceError::InvalidChallenge,
            MfaServiceError::Internal("x".to_string()),
            MfaServiceError::EnrollmentUnavailable("x"),
        ] {
            // Every variant must map, so a new one cannot silently audit as a
            // wrong code.
            let class = audit_failure_for(&error);
            assert!(!class.as_str().is_empty());
        }
    }

    #[test]
    fn the_target_access_key_is_required_for_the_administrative_routes() {
        let request = |uri: &str| S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: uri.parse().expect("uri should parse"),
            headers: hyper::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        // No query at all, and an explicitly empty value, must both be refused
        // rather than resolving to some default identity.
        assert!(parse_user_mfa_query(&request("http://localhost/rustfs/admin/v3/user/mfa")).is_err());
        assert!(parse_user_mfa_query(&request("http://localhost/rustfs/admin/v3/user/mfa?accessKey=")).is_err());
        assert_eq!(
            parse_user_mfa_query(&request("http://localhost/rustfs/admin/v3/user/mfa?accessKey=sinan")).expect("parse"),
            "sinan"
        );
    }
}
