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

//! Self-service account endpoints.
//!
//! * `GET  /rustfs/admin/v3/account/info`     — describe the caller to itself
//! * `POST /rustfs/admin/v3/account/password` — rotate the caller's own secret
//!
//! These act on whoever is calling rather than on a target named in the
//! request, so they carry no admin-action gate: every authenticated identity
//! may inspect and manage itself. What they do carry instead is a proof-of-
//! knowledge check, because a signature only proves that a credential was
//! *used* — the Console signs with a short-lived STS session, so a hijacked
//! browser tab could otherwise rewrite the parent identity's password without
//! ever knowing it.
//!
//! Who may rotate a secret at all is decided by
//! [`crate::admin::service::caller_identity`], not here.

use super::account_audit::{
    AccountAuditContext, AccountAuditFailure, AccountAuditOperation, AccountAuditRecord, emit as emit_audit,
};
use super::admin_json_response;
use super::iam_error::iam_error_to_s3_error;
use super::supervise_admin_mutation;
use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{current_action_credentials, current_ready_iam_handle, object_store_from_req};
use crate::admin::service::caller_identity::CallerIdentity;
use crate::admin::storage_api::s3::{self, Body, S3ErrorCode, S3Request, S3Response, S3Result};
use crate::admin::utils::read_compatible_admin_body;
use crate::auth::constant_time_eq;
use crate::server::RemoteAddr;
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_iam::mfa::service as mfa_service;
use rustfs_madmin::account::{AccountMfaSummary, ChangePasswordRequest, IdentityType, SelfAccountInfo, SetUserSecretKeyRequest};
use rustfs_policy::auth::is_secret_key_valid;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_utils::MaskedAccessKey;
use time::OffsetDateTime;
use tracing::{info, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_ACCOUNT: &str = "account";
const EVENT_ADMIN_ACCOUNT_STATE: &str = "admin_account_state";

pub(crate) const ACCOUNT_INFO_ROUTE: &str = "/rustfs/admin/v3/account/info";
pub(crate) const ACCOUNT_PASSWORD_ROUTE: &str = "/rustfs/admin/v3/account/password";

pub fn register_account_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(Method::GET, ACCOUNT_INFO_ROUTE, AdminOperation(&SelfAccountInfoHandler {}))?;
    r.insert(Method::POST, ACCOUNT_PASSWORD_ROUTE, AdminOperation(&ChangeOwnPasswordHandler {}))?;

    Ok(())
}

/// `GET /rustfs/admin/v3/account/info`
pub struct SelfAccountInfoHandler {}

#[async_trait::async_trait]
impl Operation for SelfAccountInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let caller = CallerIdentity::resolve(&req).await?;
        let iam_store =
            current_ready_iam_handle().map_err(|_| s3::error(S3ErrorCode::InternalError, "iam is not initialized"))?;

        // Root has no IAM record at all — `check_key` special-cases it — so its
        // status and memberships are synthesized rather than looked up.
        let (status, member_of) = if matches!(caller.identity_type, IdentityType::Root) {
            ("enabled".to_string(), Vec::new())
        } else {
            match iam_store.get_user_info(&caller.access_key).await {
                Ok(info) => (info.status.as_ref().to_string(), info.member_of.unwrap_or_default()),
                // A federated session has no builtin user record; that is not an
                // error, it just means there is nothing builtin to report.
                Err(_) => ("enabled".to_string(), Vec::new()),
            }
        };

        let policies = iam_store
            .policy_db_get(&caller.access_key, &caller.credentials.groups)
            .await
            .unwrap_or_default();

        // Reported inline rather than behind a second round trip, so a client
        // can render the whole security surface from one response.
        let mfa = match object_store_from_req(&req) {
            Some(store) => {
                let status = mfa_service::status(store, &caller.access_key, OffsetDateTime::now_utc())
                    .await
                    .map_err(|err| s3::error(S3ErrorCode::InternalError, format!("{err}")))?;
                AccountMfaSummary {
                    enabled: status.enabled,
                    pending: status.pending,
                    activated_at: status.activated_at,
                    recovery_codes_remaining: status.recovery_codes_remaining,
                    last_verified_at: status.last_verified_at,
                    // Enrollment availability is the MFA capability, not the
                    // password one: a root identity may protect its console
                    // login even though its secret key is fixed.
                    enrollment_available: status.enrollment_available && caller.mfa_denial.is_none(),
                    enrollment_blocked_reason: match caller.mfa_denial {
                        Some(denial) => Some(denial.message().to_string()),
                        None => status.enrollment_blocked_reason,
                    },
                }
            }
            None => return Err(s3::error(S3ErrorCode::ServiceUnavailable, "the object store is not ready")),
        };

        let info = SelfAccountInfo {
            access_key: caller.access_key.clone(),
            identity_type: caller.identity_type,
            session_access_key: caller.session_access_key.clone(),
            is_admin: caller.is_owner,
            status,
            member_of,
            policies,
            credentials_source: caller.credentials_source,
            mutable: caller.mutability(),
            mfa,
        };

        admin_json_response(req.uri.path(), &caller.credentials.secret_key, StatusCode::OK, &info)
    }
}

/// `POST /rustfs/admin/v3/account/password`
pub struct ChangeOwnPasswordHandler {}

#[async_trait::async_trait]
impl Operation for ChangeOwnPasswordHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let caller = CallerIdentity::resolve(&req).await?;
        let audit = AccountAuditContext::from_request(&req);

        if let Err(err) = caller.ensure_credential_mutation_allowed() {
            emit_audit(
                &audit,
                AccountAuditRecord::failure(
                    AccountAuditOperation::ChangeOwnPassword,
                    &caller.access_key,
                    caller.identity_type,
                    AccountAuditFailure::NotPermittedForCredential,
                )
                .with_session_access_key(caller.session_access_key.as_deref()),
            );
            return Err(err);
        }

        let path = req.uri.path().to_string();
        let body =
            read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, &path, &caller.credentials.secret_key).await?;
        let request: ChangePasswordRequest = serde_json::from_slice(&body)
            .map_err(|e| s3::error(S3ErrorCode::InvalidRequest, format!("invalid change-password request: {e}")))?;

        let iam_store =
            current_ready_iam_handle().map_err(|_| s3::error(S3ErrorCode::InternalError, "iam is not initialized"))?;

        let Some(stored) = iam_store.get_user(&caller.access_key).await else {
            // Reached only if the identity was deleted between authentication
            // and this lookup.
            emit_audit(
                &audit,
                AccountAuditRecord::failure(
                    AccountAuditOperation::ChangeOwnPassword,
                    &caller.access_key,
                    caller.identity_type,
                    AccountAuditFailure::Internal,
                ),
            );
            return Err(s3::error(S3ErrorCode::InvalidRequest, "the calling identity no longer exists"));
        };

        if !constant_time_eq(&request.current_secret_key, &stored.credentials.secret_key) {
            warn!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ACCOUNT,
                event = EVENT_ADMIN_ACCOUNT_STATE,
                action = "change_own_password",
                access_key = %MaskedAccessKey(&caller.access_key),
                result = "invalid_current_secret",
                "admin account state"
            );
            emit_audit(
                &audit,
                AccountAuditRecord::failure(
                    AccountAuditOperation::ChangeOwnPassword,
                    &caller.access_key,
                    caller.identity_type,
                    AccountAuditFailure::InvalidCurrentSecret,
                )
                .with_session_access_key(caller.session_access_key.as_deref()),
            );
            // Deliberately the same message the validation failures below use,
            // so a caller cannot distinguish "wrong current password" from
            // "new password rejected" by probing.
            return Err(s3::error(S3ErrorCode::InvalidRequest, "the current secret key is incorrect"));
        }

        if let Err(err) = validate_new_secret_key(&request) {
            emit_audit(
                &audit,
                AccountAuditRecord::failure(
                    AccountAuditOperation::ChangeOwnPassword,
                    &caller.access_key,
                    caller.identity_type,
                    AccountAuditFailure::InvalidNewSecret,
                )
                .with_session_access_key(caller.session_access_key.as_deref()),
            );
            return Err(err);
        }

        let access_key = caller.access_key.clone();
        let new_secret_key = request.new_secret_key.clone();
        let identity_type = caller.identity_type;
        let session_access_key = caller.session_access_key.clone();
        let audit_for_task = audit.clone();

        // Detached from request cancellation: a client that disconnects between
        // the secret write and the session revocation must not leave the old
        // sessions alive against a rotated secret.
        let sessions_revoked = supervise_admin_mutation("change own password", async move {
            let iam_store =
                current_ready_iam_handle().map_err(|_| s3::error(S3ErrorCode::InternalError, "iam is not initialized"))?;

            iam_store
                .set_user_secret_key(&access_key, &new_secret_key)
                .await
                .map_err(iam_error_to_s3_error)?;

            // Sessions minted under the old secret must not outlive it. A
            // failure here is reported but does not undo the rotation: the new
            // secret is already authoritative, and re-running the revocation is
            // safe, whereas rolling the secret back would resurrect it.
            let revoked = match iam_store.revoke_sts_sessions_for_parent(&access_key).await {
                Ok(revoked) => revoked,
                Err(err) => {
                    warn!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_ACCOUNT,
                        event = EVENT_ADMIN_ACCOUNT_STATE,
                        action = "change_own_password",
                        access_key = %MaskedAccessKey(&access_key),
                        result = "session_revocation_incomplete",
                        error = ?err,
                        "admin account state"
                    );
                    0
                }
            };

            emit_audit(
                &audit_for_task,
                AccountAuditRecord::success(AccountAuditOperation::ChangeOwnPassword, &access_key, identity_type)
                    .with_session_access_key(session_access_key.as_deref())
                    .with_sessions_revoked(revoked),
            );

            info!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ACCOUNT,
                event = EVENT_ADMIN_ACCOUNT_STATE,
                action = "change_own_password",
                access_key = %MaskedAccessKey(&access_key),
                sessions_revoked = revoked,
                result = "changed",
                "admin account state"
            );

            Ok(revoked)
        })
        .await?;

        admin_json_response(
            &path,
            &caller.credentials.secret_key,
            StatusCode::OK,
            &ChangePasswordResult {
                sessions_revoked: sessions_revoked as u32,
            },
        )
    }
}

/// `PUT /rustfs/admin/v3/set-user-secret-key?accessKey=…`
///
/// Administrative reset of another identity's secret key.
///
/// Exists because the only way to change a password before this was to re-POST
/// the whole user through `add-user`, which rewrites `status` and drops the
/// policy field along with it — a password reset that silently re-enabled a
/// disabled account. This touches the secret and nothing else.
pub struct SetUserSecretKeyHandler {}

#[derive(Debug, serde::Deserialize, Default)]
struct SetUserSecretKeyQuery {
    #[serde(rename = "accessKey", alias = "access-key")]
    access_key: Option<String>,
}

#[async_trait::async_trait]
impl Operation for SetUserSecretKeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query: SetUserSecretKeyQuery = match req.uri.query() {
            Some(query) => serde_urlencoded::from_str(query)
                .map_err(|_| s3::error(S3ErrorCode::InvalidArgument, "failed to decode query"))?,
            None => SetUserSecretKeyQuery::default(),
        };
        let target = query.access_key.unwrap_or_default();
        if target.is_empty() {
            return Err(s3::error(S3ErrorCode::InvalidArgument, "access key is empty"));
        }

        let caller = CallerIdentity::resolve(&req).await?;
        let audit = AccountAuditContext::from_request(&req);

        // The root identity is provisioned from the environment; its secret is a
        // process-wide `OnceLock` that also derives the internode RPC secret, so
        // there is nothing here that could change it.
        if current_action_credentials().is_some_and(|root| constant_time_eq(&root.access_key, &target)) {
            return Err(s3::error(
                S3ErrorCode::InvalidRequest,
                "the root identity is provisioned from the server environment and cannot be changed at runtime",
            ));
        }

        // A derived credential must not rewrite the secret of the identity it
        // was minted from: the session would otherwise be able to promote
        // itself into permanent control of that account.
        if caller.session_access_key.is_some() && caller.access_key == target {
            return Err(s3::error(
                S3ErrorCode::InvalidRequest,
                "cannot change the credentials of the parent identity of this session",
            ));
        }

        validate_admin_request(
            &req.headers,
            &caller.credentials,
            caller.is_owner,
            false,
            vec![Action::AdminAction(AdminAction::CreateUserAdminAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await
        .inspect_err(|_| {
            emit_audit(
                &audit,
                AccountAuditRecord::failure(
                    AccountAuditOperation::ResetUserPassword,
                    &target,
                    caller.identity_type,
                    AccountAuditFailure::AccessDenied,
                )
                .with_session_access_key(Some(caller.access_key.as_str())),
            );
        })?;

        let path = req.uri.path().to_string();
        let body =
            read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, &path, &caller.credentials.secret_key).await?;
        let request: SetUserSecretKeyRequest = serde_json::from_slice(&body)
            .map_err(|e| s3::error(S3ErrorCode::InvalidRequest, format!("invalid set-user-secret-key request: {e}")))?;

        if !is_secret_key_valid(&request.secret_key) {
            return Err(s3::error(S3ErrorCode::InvalidArgument, "the new secret key is too short"));
        }

        let actor = caller.access_key.clone();
        let identity_type = caller.identity_type;
        let audit_for_task = audit.clone();

        let sessions_revoked = supervise_admin_mutation("set user secret key", async move {
            let iam_store =
                current_ready_iam_handle().map_err(|_| s3::error(S3ErrorCode::InternalError, "iam is not initialized"))?;

            iam_store
                .set_user_secret_key(&target, &request.secret_key)
                .await
                .map_err(iam_error_to_s3_error)?;

            let revoked = match iam_store.revoke_sts_sessions_for_parent(&target).await {
                Ok(revoked) => revoked,
                Err(err) => {
                    warn!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_ACCOUNT,
                        event = EVENT_ADMIN_ACCOUNT_STATE,
                        action = "set_user_secret_key",
                        access_key = %MaskedAccessKey(&target),
                        result = "session_revocation_incomplete",
                        error = ?err,
                        "admin account state"
                    );
                    0
                }
            };

            emit_audit(
                &audit_for_task,
                AccountAuditRecord::success(AccountAuditOperation::ResetUserPassword, &target, identity_type)
                    .with_session_access_key(Some(actor.as_str()))
                    .with_sessions_revoked(revoked),
            );
            info!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ACCOUNT,
                event = EVENT_ADMIN_ACCOUNT_STATE,
                action = "set_user_secret_key",
                access_key = %MaskedAccessKey(&target),
                actor_access_key = %MaskedAccessKey(&actor),
                sessions_revoked = revoked,
                result = "changed",
                "admin account state"
            );

            Ok(revoked)
        })
        .await?;

        admin_json_response(
            &path,
            &caller.credentials.secret_key,
            StatusCode::OK,
            &ChangePasswordResult {
                sessions_revoked: sessions_revoked as u32,
            },
        )
    }
}

/// Number of sessions the rotation invalidated, so a client can tell the user
/// they have been signed out elsewhere.
#[derive(Debug, serde::Serialize)]
struct ChangePasswordResult {
    sessions_revoked: u32,
}

/// Reject a new secret that would be useless or a no-op.
fn validate_new_secret_key(request: &ChangePasswordRequest) -> S3Result<()> {
    if !is_secret_key_valid(&request.new_secret_key) {
        return Err(s3::error(S3ErrorCode::InvalidArgument, "the new secret key is too short"));
    }

    if constant_time_eq(&request.current_secret_key, &request.new_secret_key) {
        return Err(s3::error(
            S3ErrorCode::InvalidArgument,
            "the new secret key must differ from the current one",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::ADMIN_PREFIX;

    fn change_request(current: &str, new: &str) -> ChangePasswordRequest {
        ChangePasswordRequest {
            current_secret_key: current.to_string(),
            new_secret_key: new.to_string(),
        }
    }

    #[test]
    fn new_secret_key_must_meet_the_length_floor() {
        // Same floor the IAM layer enforces, checked here so the caller gets a
        // useful message instead of a generic IAM error.
        let err = validate_new_secret_key(&change_request("old-secret-key", "short")).expect_err("must reject");
        assert!(err.to_string().contains("too short"), "{err}");
    }

    #[test]
    fn new_secret_key_must_differ_from_the_current_one() {
        let err = validate_new_secret_key(&change_request("same-secret-key", "same-secret-key")).expect_err("must reject");
        assert!(err.to_string().contains("must differ"), "{err}");
    }

    #[test]
    fn a_valid_rotation_passes_validation() {
        validate_new_secret_key(&change_request("old-secret-key", "new-secret-key")).expect("must accept");
    }

    #[test]
    fn route_constants_stay_under_the_admin_prefix() {
        // The constants spell the full path so registration has a single source
        // of truth; this pins them to the prefix the router canonicalises on.
        assert!(ACCOUNT_INFO_ROUTE.starts_with(ADMIN_PREFIX));
        assert!(ACCOUNT_PASSWORD_ROUTE.starts_with(ADMIN_PREFIX));
    }

    #[test]
    fn routes_are_registered_under_the_admin_prefix() {
        let mut router: S3Router<AdminOperation> = S3Router::new(false);
        register_account_route(&mut router).expect("register account routes");

        assert!(router.contains_route(Method::GET, ACCOUNT_INFO_ROUTE));
        assert!(router.contains_route(Method::POST, ACCOUNT_PASSWORD_ROUTE));
    }
}
