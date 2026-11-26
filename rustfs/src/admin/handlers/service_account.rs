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

use super::iam_error::iam_error_to_s3_error;
use crate::admin::access_key_identity;
use crate::admin::handlers::site_replication::{encode_service_account_replication_policy, site_replication_iam_change_hook};
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::runtime_sources::current_action_credentials;
use crate::admin::utils::{encode_compatible_admin_payload, has_space_be, is_compat_admin_request, read_compatible_admin_body};
use crate::auth::{constant_time_eq, get_condition_values, get_session_token};
use crate::server::ADMIN_PREFIX;
use crate::{
    admin::router::{AdminOperation, S3Router},
    auth::check_key_valid,
};
use http::HeaderMap;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::Credentials as StoredCredentials;
use rustfs_iam::error::{is_err_no_such_service_account, is_err_no_such_temp_account};
use rustfs_iam::store::Store as IamStore;
use rustfs_iam::sys::{NewServiceAccountOpts, UpdateServiceAccountOpts, claims_have_policy_boundary};
use rustfs_madmin::{
    ACCESS_KEY_LIST_ALL, ACCESS_KEY_LIST_STS_ONLY, ACCESS_KEY_LIST_SVCACC_ONLY, ACCESS_KEY_LIST_USERS_ONLY, AddServiceAccountReq,
    AddServiceAccountResp, Credentials, InfoServiceAccountResp, ListAccessKeysResp, ListServiceAccountsResp,
    SITE_REPL_API_VERSION, SRIAMItem, SRSessionPolicy, SRSvcAccChange, SRSvcAccCreate, SRSvcAccDelete, SRSvcAccUpdate,
    ServiceAccountInfo, TemporaryAccountInfoResp, UpdateServiceAccountReq,
};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_policy::policy::{Args, DEFAULT_VERSION, Policy, SVC_TARGET_USER};
use rustfs_s3_types::{Body, S3ErrorCode::InvalidRequest};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use time::OffsetDateTime;
use tracing::{debug, warn};
use url::form_urlencoded;

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_SERVICE_ACCOUNT: &str = "service_account";
const EVENT_ADMIN_SERVICE_ACCOUNT_STATE: &str = "admin_service_account_state";
const EMPTY_EXPLICIT_SERVICE_ACCOUNT_POLICY: &str =
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]}]}"#;

fn sr_session_policy_from_policy(policy: Option<&Policy>) -> S3Result<SRSessionPolicy> {
    let Some(policy) = policy else {
        return Ok(SRSessionPolicy::default());
    };

    let raw = serde_json::to_string(policy).map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))?;
    SRSessionPolicy::from_json(&raw).map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))
}

fn normalize_service_account_policy(mut policy: Policy) -> S3Result<Policy> {
    if policy.statements.is_empty() && (!policy.id.is_empty() || !policy.version.is_empty()) {
        let id = policy.id;
        policy = Policy::parse_config(EMPTY_EXPLICIT_SERVICE_ACCOUNT_POLICY.as_bytes())
            .map_err(|e| s3_error!(InternalError, "parse empty service account policy failed: {:?}", e))?;
        policy.id = id;
    } else if policy.version.is_empty() && !policy.statements.is_empty() {
        policy.version = DEFAULT_VERSION.to_string();
    }
    Ok(policy)
}

fn parse_new_service_account_policy(policy: Option<&serde_json::Value>) -> S3Result<Option<Policy>> {
    let Some(policy) = policy else {
        return Ok(None);
    };
    let policy = normalize_service_account_policy(parse_service_account_policy(policy)?)?;
    Ok((!policy.id.is_empty() || !policy.version.is_empty() || !policy.statements.is_empty()).then_some(policy))
}

/// Service accounts and policy-bounded STS sessions cannot delegate beyond
/// their own lifetime. Unrestricted STS sessions (such as Console logins)
/// retain the ability to create independently expiring service accounts.
fn bounded_service_account_expiration(caller: &StoredCredentials, requested: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    let should_bound = caller.is_service_account() || claims_have_policy_boundary(caller.claims_or_empty());
    if !should_bound {
        return requested;
    }

    let caller_expiration = caller.expiration.or_else(|| {
        caller
            .claims_or_empty()
            .get("exp")
            .and_then(serde_json::Value::as_i64)
            .and_then(|ts| OffsetDateTime::from_unix_timestamp(ts).ok())
    });

    match caller_expiration {
        Some(limit) => Some(requested.map_or(limit, |expiration| expiration.min(limit))),
        None => requested,
    }
}

fn set_service_account_duration_condition(
    conditions: &mut HashMap<String, Vec<String>>,
    expiration: Option<OffsetDateTime>,
    now: OffsetDateTime,
) {
    // An unlimited credential must not bypass a finite NumericGreaterThan Deny.
    let seconds = expiration.map_or(i64::MAX, |expiration| (expiration - now).whole_seconds().max(0));
    conditions.insert("DurationSeconds".to_string(), vec![seconds.to_string()]);
}

fn compat_time_sentinel() -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH
}

fn list_expiration_or_sentinel(expiration: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    Some(expiration.unwrap_or_else(compat_time_sentinel))
}

fn expiration_for_admin_path(path: &str, expiration: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    if is_compat_admin_request(path) {
        list_expiration_or_sentinel(expiration)
    } else {
        expiration
    }
}

fn delete_service_account_success_status(path: &str) -> StatusCode {
    if is_compat_admin_request(path) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::OK
    }
}

fn is_service_account_owner_of(caller: &StoredCredentials, target_parent_user: &str) -> bool {
    let caller_parent = if caller.parent_user.is_empty() {
        caller.access_key.as_str()
    } else {
        caller.parent_user.as_str()
    };

    caller_parent == target_parent_user
}

/// Scope guard for `UpdateServiceAccount`: only an owner may cross parent
/// boundaries or update root-parented service accounts.
fn update_service_account_parent_within_scope(
    root_access_key: &str,
    caller: &StoredCredentials,
    target_parent_user: &str,
    owner: bool,
) -> bool {
    owner || (!constant_time_eq(target_parent_user, root_access_key) && is_service_account_owner_of(caller, target_parent_user))
}

/// Whether `update` would let a policy-bounded caller (an STS session carrying
/// a session policy, an embedded-policy service account, or an OIDC service
/// account with a persisted federated boundary) act beyond its policy boundary.
///
/// A bounded caller may only update its own key (preventing sibling takeover
/// via `newSecretKey` or sibling disruption/widening), and even on its own key
/// may not replace its policy, alter its expiration, or change its status.
/// Rotating its own secret key and editing its own name or description remain
/// permitted.
fn session_bounded_caller_escapes_through_update(
    caller: &StoredCredentials,
    target_access_key: &str,
    update: &UpdateServiceAccountReq,
) -> bool {
    if !claims_have_policy_boundary(caller.claims_or_empty()) {
        return false;
    }

    target_access_key != caller.access_key
        || update.new_policy.is_some()
        || update.new_expiration.is_some()
        || update.new_status.is_some()
}

/// Scope available with the base create permission. Creating for another local
/// user requires separate authorization in IAM, even when the caller is owner.
fn add_service_account_parent_within_scope(
    root_access_key: &str,
    target_user: &str,
    req_user: &str,
    req_parent_user: &str,
    owner: bool,
) -> bool {
    let effective_parent = if target_user == req_user {
        req_parent_user
    } else {
        target_user
    };
    owner || (effective_parent != root_access_key && (target_user == req_user || target_user == req_parent_user))
}

/// Whether `cred` may use the owning user's self-service fallback on
/// service-account read, list, and delete handlers when the caller lacks an
/// explicit admin `Allow`.
///
/// Only the user itself and unrestricted STS sessions (such as Console logins)
/// qualify. Service accounts and credentials narrowed by a session policy or
/// policy boundary must pass the full `Allow`/`Deny` IAM evaluation instead of
/// acting as their parent identity.
fn may_use_owner_self_service_fallback(cred: &StoredCredentials) -> bool {
    !cred.is_service_account() && !claims_have_policy_boundary(cred.claims_or_empty())
}

/// Fail-closed ownership check used by non-admin service-account read/delete
/// fallbacks after the primary `is_allowed(..., deny_only: false)` check fails.
///
/// Returns `true` only when:
/// 1. The caller has no explicit `Deny` on the required admin action (`no_explicit_deny`),
/// 2. The caller qualifies for owner self-service fallback (not a service
///    account and not narrowed by a session policy or policy boundary),
/// 3. The target service account was successfully loaded (`Some(parent)`) with a
///    non-empty parent matching the caller's resolved identity (`request_user_name`).
///    A `None` target — meaning `get_service_account` failed with a not-found or
///    decode error — denies fail-closed (backlog#806).
fn non_admin_may_access_owned_service_account(
    caller: &StoredCredentials,
    target_parent_user: Option<&str>,
    no_explicit_deny: bool,
) -> bool {
    no_explicit_deny
        && may_use_owner_self_service_fallback(caller)
        && matches!(target_parent_user, Some(parent) if !parent.is_empty() && parent == request_user_name(caller))
}

fn map_service_account_lookup_error(err: rustfs_iam::error::Error, action: &str) -> S3Error {
    debug!(
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
        event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
        action,
        result = "lookup_failed",
        error = ?err,
        "admin service account state"
    );
    if is_err_no_such_service_account(&err) {
        iam_error_to_s3_error(err)
    } else {
        s3_error!(InternalError, "{action}")
    }
}

fn map_temp_account_lookup_error(err: rustfs_iam::error::Error, action: &str) -> S3Error {
    debug!(
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
        event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
        action,
        result = "temporary_lookup_failed",
        error = ?err,
        "admin service account state"
    );
    if is_err_no_such_temp_account(&err) {
        iam_error_to_s3_error(err)
    } else {
        s3_error!(InternalError, "{action}")
    }
}

fn parse_service_account_policy(policy: &serde_json::Value) -> S3Result<Policy> {
    let policy_bytes = serde_json::to_vec(policy).map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))?;
    Policy::parse_config(&policy_bytes).map_err(|e| {
        debug!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
            event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
            result = "policy_parse_failed",
            error = ?e,
            "admin service account state"
        );
        match e {
            rustfs_policy::error::Error::PolicyError(rustfs_policy::policy::Error::NonResource) => {
                s3_error!(InvalidArgument, "invalid service account policy: Resource is empty")
            }
            rustfs_policy::error::Error::PolicyError(err) => {
                s3_error!(InvalidArgument, "invalid service account policy: {}", err)
            }
            rustfs_policy::error::Error::Io(err) if err.get_ref().is_some_and(|source| source.is::<serde_json::Error>()) => {
                s3_error!(
                    InvalidArgument,
                    "Policy format is invalid. Please check the JSON structure and ensure it follows the IAM policy format."
                )
            }
            err => s3_error!(InvalidArgument, "invalid service account policy: {}", err),
        }
    })
}

fn parse_update_service_account_policy(new_policy: Option<serde_json::Value>) -> S3Result<Option<Policy>> {
    let Some(policy) = new_policy else {
        return Ok(None);
    };

    Ok(Some(parse_service_account_policy(&policy)?))
}

fn service_account_update_replication_change(
    access_key: &str,
    update: &UpdateServiceAccountReq,
    session_policy: Option<&Policy>,
) -> S3Result<SRSvcAccChange> {
    Ok(SRSvcAccChange {
        update: Some(SRSvcAccUpdate {
            access_key: access_key.to_string(),
            secret_key: update.new_secret_key.clone().unwrap_or_default(),
            status: update.new_status.clone().unwrap_or_default(),
            name: update.new_name.clone().unwrap_or_default(),
            description: update.new_description.clone().unwrap_or_default(),
            session_policy: sr_session_policy_from_policy(session_policy)?,
            expiration: update.new_expiration,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        }),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    })
}

pub fn register_service_account_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/update-service-account").as_str(),
        AdminOperation(&UpdateServiceAccount {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-service-account").as_str(),
        AdminOperation(&InfoServiceAccount {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/temporary-account-info").as_str(),
        AdminOperation(&TemporaryAccountInfo {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info-access-key").as_str(),
        AdminOperation(&InfoAccessKey {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-service-accounts").as_str(),
        AdminOperation(&ListServiceAccount {}),
    )?;
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-access-keys-bulk").as_str(),
        AdminOperation(&ListAccessKeysBulk {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/delete-service-accounts").as_str(),
        AdminOperation(&DeleteServiceAccount {}),
    )?;
    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/delete-service-account").as_str(),
        AdminOperation(&DeleteServiceAccount {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-service-accounts").as_str(),
        AdminOperation(&AddServiceAccount {}),
    )?;
    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/add-service-account").as_str(),
        AdminOperation(&AddServiceAccount {}),
    )?;

    Ok(())
}

fn map_service_account_create_error(error: rustfs_iam::error::Error) -> S3Error {
    match error {
        rustfs_iam::error::Error::NoSuchUser(_) => s3_error!(InvalidRequest, "target user not exist"),
        rustfs_iam::error::Error::InvalidArgument
        | rustfs_iam::error::Error::InvalidAccessKeyLength
        | rustfs_iam::error::Error::InvalidSecretKeyLength
        | rustfs_iam::error::Error::AccessKeyAlreadyExists => iam_error_to_s3_error(error),
        rustfs_iam::error::Error::IAMActionNotAllowed => s3_error!(AccessDenied, "access denied"),
        _ => s3_error!(InternalError, "create service account failed"),
    }
}

// The Admin operation owns the local write and site notification together.
// A disconnected requester can stop waiting without splitting those effects.
async fn create_and_replicate_service_account<R: Future<Output = S3Result<()>> + Send>(
    create: impl Future<Output = S3Result<(StoredCredentials, OffsetDateTime, HashMap<String, serde_json::Value>)>> + Send + 'static,
    replication_policy: Option<String>,
    replication_expiration: Option<OffsetDateTime>,
    replicate: impl FnOnce(SRIAMItem) -> R + Send + 'static,
) -> S3Result<StoredCredentials> {
    super::supervise_admin_mutation("service account creation", async move {
        let (new_cred, updated_at, replication_claims) = create.await?;
        let (replication_session_policy, oidc_service_account_envelope) =
            encode_service_account_replication_policy(&replication_claims, replication_policy.as_deref())?;

        if let Err(err) = replicate(SRIAMItem {
            r#type: "service-account".to_string(),
            svc_acc_change: Some(SRSvcAccChange {
                create: Some(SRSvcAccCreate {
                    parent: new_cred.parent_user.clone(),
                    access_key: new_cred.access_key.clone(),
                    secret_key: new_cred.secret_key.clone(),
                    groups: new_cred.groups.clone().unwrap_or_default(),
                    claims: replication_claims,
                    session_policy: replication_session_policy,
                    status: String::new(),
                    name: new_cred.name.clone().unwrap_or_default(),
                    description: new_cred.description.clone().unwrap_or_default(),
                    expiration: replication_expiration,
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                }),
                oidc_service_account_envelope,
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            }),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
                event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
                access_key = %new_cred.access_key,
                action = "create",
                result = "site_replication_hook_failed",
                error = ?err,
                "admin service account state"
            );
        }
        Ok(new_cred)
    })
    .await
}

pub struct AddServiceAccount {}
#[async_trait::async_trait]
impl Operation for AddServiceAccount {
    async fn call(&self, mut req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let Some(req_cred) = req.credentials() else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(req.uri(), req.headers()).unwrap_or_default(), req_cred.access_key()).await?;

        if !req_cred.secret_key_matches(&cred.secret_key) {
            return Err(S3Error::with_message(S3ErrorCode::AccessDenied, "access denied"));
        }

        let body =
            read_compatible_admin_body(req.take_body(), MAX_ADMIN_REQUEST_BODY_SIZE, req.uri().path(), &cred.secret_key).await?;

        let mut create_req: AddServiceAccountReq =
            serde_json::from_slice(&body[..]).map_err(|e| s3_error!(InvalidRequest, "unmarshal body failed, e: {:?}", e))?;

        // create_req.expiration = create_req.expiration.and_then(|expire| expire.replace_millisecond(0).ok());

        if has_space_be(&create_req.access_key) {
            return Err(s3_error!(InvalidRequest, "access key has spaces"));
        }

        create_req.validate().map_err(|e| S3Error::with_message(InvalidRequest, e))?;

        let session_policy = parse_new_service_account_policy(create_req.policy.as_ref())?;
        let replication_policy = session_policy
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| s3_error!(InvalidArgument, "marshal policy failed: {:?}", e))?;

        let Some(sys_cred) = current_action_credentials() else {
            return Err(s3_error!(InvalidRequest, "get sys cred failed"));
        };

        if constant_time_eq(&sys_cred.access_key, &create_req.access_key) {
            return Err(s3_error!(InvalidArgument, "can't create user with system access key"));
        }

        let requested_user = create_req.target_user.as_deref().unwrap_or(&cred.access_key);
        let derived = cred.is_service_account() || cred.is_temp();
        let caller_parent = if derived { &cred.parent_user } else { &cred.access_key };
        let self_scoped = requested_user == cred.access_key || requested_user == caller_parent;
        let target_user = if self_scoped { caller_parent.as_str() } else { requested_user };
        if target_user.is_empty() {
            return Err(s3_error!(AccessDenied, "service account requires a parent"));
        }

        let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        create_req.expiration = bounded_service_account_expiration(&cred, create_req.expiration);
        let mut conditions = get_condition_values(req.headers(), &cred, None, None, req.policy_source_addr());
        set_service_account_duration_condition(&mut conditions, create_req.expiration, OffsetDateTime::now_utc());

        // The policy and the write must see the same resolved parent, including
        // when a derived credential names itself. OPA receives this map too.
        conditions.retain(|key, _| !key.eq_ignore_ascii_case(SVC_TARGET_USER));
        conditions.insert(SVC_TARGET_USER.to_owned(), vec![target_user.to_owned()]);
        if self_scoped {
            if !iam_store
                .is_allowed(&Args {
                    account: &cred.access_key,
                    groups: &cred.groups,
                    action: Action::AdminAction(AdminAction::CreateServiceAccountAdminAction),
                    bucket: "",
                    conditions: &conditions,
                    is_owner: owner,
                    object: "",
                    claims: cred.claims_or_empty(),
                    deny_only: false,
                })
                .await
            {
                return Err(s3_error!(AccessDenied, "access denied"));
            }
            if !add_service_account_parent_within_scope(
                &sys_cred.access_key,
                requested_user,
                &cred.access_key,
                caller_parent,
                owner,
            ) {
                return Err(s3_error!(AccessDenied, "service account parent is outside requester scope"));
            }
            if requested_user != cred.access_key
                && requested_user != sys_cred.access_key
                && iam_store.get_user(requested_user).await.is_none()
            {
                return Err(s3_error!(InvalidRequest, "target user not exist"));
            }
        }

        let target_groups = if self_scoped { cred.groups.clone() } else { None };
        let opts = NewServiceAccountOpts {
            access_key: create_req.access_key,
            secret_key: create_req.secret_key,
            name: create_req.name,
            description: create_req.description.or(create_req.comment),
            expiration: create_req.expiration,
            session_policy,
            ..Default::default()
        };

        let target_user = target_user.to_owned();
        let response_secret = cred.secret_key.clone();
        let replication_expiration = opts.expiration;
        let create = async move {
            let create_result = if self_scoped {
                iam_store
                    .new_service_account_from_caller(&target_user, target_groups, opts, &cred)
                    .await
            } else {
                iam_store
                    .new_service_account_for_user(&target_user, opts, &cred, &conditions, owner)
                    .await
            };
            create_result.map_err(|e| {
                debug!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
                    event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
                    target_user = %target_user,
                    result = "create_failed",
                    error = ?e,
                    "admin service account state"
                );
                map_service_account_create_error(e)
            })
        };
        let new_cred = create_and_replicate_service_account(
            create,
            replication_policy,
            replication_expiration,
            site_replication_iam_change_hook,
        )
        .await?;

        let resp = AddServiceAccountResp {
            credentials: Credentials {
                access_key: &new_cred.access_key,
                secret_key: &new_cred.secret_key,
                session_token: None,
                expiration: new_cred.expiration,
            },
        };

        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;
        let (body, content_type) = encode_compatible_admin_payload(req.uri().path(), &response_secret, body)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().expect("valid header value"));

        Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(body), header))
    }
}

#[derive(Debug, Default, Deserialize)]
struct AccessKeyQuery {
    #[serde(rename = "accessKey", alias = "access-key")]
    pub access_key: String,
}

fn request_user_name(cred: &StoredCredentials) -> &str {
    if cred.parent_user.is_empty() {
        &cred.access_key
    } else {
        &cred.parent_user
    }
}

fn can_fallback_view_access_key_info(requester: &StoredCredentials, target: &StoredCredentials) -> bool {
    if !may_use_owner_self_service_fallback(requester) {
        return false;
    }

    if target.is_temp() || target.is_service_account() {
        return request_user_name(requester) == target.parent_user;
    }

    request_user_name(requester) == target.access_key
}

fn can_fallback_view_access_key_info_after_admin_check_failed(
    requester: &StoredCredentials,
    target: &StoredCredentials,
    no_explicit_deny: bool,
) -> bool {
    no_explicit_deny && can_fallback_view_access_key_info(requester, target)
}

async fn build_info_service_account_resp<T: IamStore>(
    iam_store: &rustfs_iam::sys::IamSys<T>,
    account: &StoredCredentials,
    session_policy: Option<rustfs_policy::policy::Policy>,
) -> S3Result<InfoServiceAccountResp> {
    access_key_identity::build_info_service_account_resp(iam_store, account, session_policy).await
}

pub struct UpdateServiceAccount {}
#[async_trait::async_trait]
impl Operation for UpdateServiceAccount {
    async fn call(&self, mut req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let query = {
            if let Some(query) = req.uri().query() {
                let input: AccessKeyQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                AccessKeyQuery::default()
            }
        };

        if query.access_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let access_key = query.access_key;

        let input = req.take_body();
        let Some(input_cred) = req.credentials() else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(req.uri(), req.headers()).unwrap_or_default(), input_cred.access_key()).await?;

        let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        // let svc_account = iam_store.get_service_account(&access_key).await.map_err(|e| {
        //     debug!("get service account failed, e: {:?}", e);
        //     s3_error!(InternalError, "get service account failed")
        // })?;

        let body = read_compatible_admin_body(input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri().path(), &cred.secret_key).await?;

        let mut update_req: UpdateServiceAccountReq =
            serde_json::from_slice(&body[..]).map_err(|e| s3_error!(InvalidRequest, "unmarshal body failed, e: {:?}", e))?;

        update_req.validate().map_err(|e| S3Error::with_message(InvalidRequest, e))?;

        // Keep lookup errors behind authorization even though lifetime conditions
        // need the existing expiration when the request omits it.
        let existing_account = iam_store.get_service_account(&access_key).await;

        if update_req.new_expiration.is_some() {
            update_req.new_expiration = bounded_service_account_expiration(&cred, update_req.new_expiration);
        }
        let mut conditions = get_condition_values(req.headers(), &cred, None, None, req.policy_source_addr());
        set_service_account_duration_condition(
            &mut conditions,
            update_req
                .new_expiration
                .or_else(|| existing_account.as_ref().ok().and_then(|(account, _)| account.expiration)),
            OffsetDateTime::now_utc(),
        );

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::UpdateServiceAccountAdminAction),
                bucket: "",
                conditions: &conditions,
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only: false,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        if !owner && session_bounded_caller_escapes_through_update(&cred, &access_key, &update_req) {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        let (svc_account, _) = existing_account.map_err(|e| map_service_account_lookup_error(e, "get service account failed"))?;

        let Some(sys_cred) = current_action_credentials() else {
            return Err(s3_error!(InvalidRequest, "get sys cred failed"));
        };

        // The admin action permits updates within a caller's scope; only an
        // owner may cross service-account parent boundaries or modify
        // root-parented service accounts.
        if !update_service_account_parent_within_scope(&sys_cred.access_key, &cred, &svc_account.parent_user, owner) {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        let new_secret_key = update_req.new_secret_key.clone();
        let new_status = update_req.new_status.clone();
        let new_name = update_req.new_name.clone();
        let new_description = update_req.new_description.clone();
        let new_expiration = update_req.new_expiration;
        let new_policy = update_req.new_policy.clone();

        let sp = parse_update_service_account_policy(new_policy.clone())?;
        let svc_acc_change = service_account_update_replication_change(&access_key, &update_req, sp.as_ref())?;

        let opts = UpdateServiceAccountOpts {
            secret_key: new_secret_key.clone(),
            status: new_status.clone(),
            name: new_name.clone(),
            description: new_description.clone(),
            expiration: new_expiration,
            session_policy: sp,
            parent_user: None,
            allow_site_replicator_account: false,
        };

        let updated_at = iam_store
            .update_service_account(&access_key, opts)
            .await
            .map_err(|e| map_service_account_lookup_error(e, "update service account failed"))?;

        if let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "service-account".to_string(),
            svc_acc_change: Some(svc_acc_change),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(access_key = %access_key, error = ?err, "site replication update service account hook failed");
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));
        header.insert(CONTENT_LENGTH, "0".parse().expect("valid header value"));
        Ok(AdminResponse::with_headers(StatusCode::NO_CONTENT, Body::empty(), header))
    }
}

pub struct InfoServiceAccount {}
#[async_trait::async_trait]
impl Operation for InfoServiceAccount {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let query = {
            if let Some(query) = req.uri().query() {
                let input: AccessKeyQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                AccessKeyQuery::default()
            }
        };

        if query.access_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let access_key = query.access_key;

        let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let Some(input_cred) = req.credentials() else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(req.uri(), req.headers()).unwrap_or_default(), input_cred.access_key()).await?;

        // Keep lookup errors behind authorization so unauthorized callers cannot
        // distinguish missing service accounts from forbidden ones.
        let existing_account = iam_store.get_service_account(&access_key).await;
        let conditions = get_condition_values(req.headers(), &cred, None, None, req.policy_source_addr());
        let claims = cred.claims_or_empty();

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::ListServiceAccountsAdminAction),
                bucket: "",
                conditions: &conditions,
                is_owner: owner,
                object: "",
                claims,
                deny_only: false,
            })
            .await
        {
            let no_explicit_deny = if !may_use_owner_self_service_fallback(&cred) {
                false
            } else {
                iam_store
                    .is_allowed(&Args {
                        account: &cred.access_key,
                        groups: &cred.groups,
                        action: Action::AdminAction(AdminAction::ListServiceAccountsAdminAction),
                        bucket: "",
                        conditions: &conditions,
                        is_owner: owner,
                        object: "",
                        claims,
                        deny_only: true,
                    })
                    .await
            };
            let target_parent = existing_account
                .as_ref()
                .ok()
                .map(|(account, _)| account.parent_user.as_str());
            if !non_admin_may_access_owned_service_account(&cred, target_parent, no_explicit_deny) {
                return Err(s3_error!(AccessDenied, "access denied"));
            }
        }

        let (svc_account, session_policy) =
            existing_account.map_err(|e| map_service_account_lookup_error(e, "get service account failed"))?;

        let resp = build_info_service_account_resp(&iam_store, &svc_account, session_policy).await?;

        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;
        let (body, content_type) = encode_compatible_admin_payload(req.uri().path(), &cred.secret_key, body)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().expect("valid header value"));

        Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(body), header))
    }
}

pub struct TemporaryAccountInfo {}
#[async_trait::async_trait]
impl Operation for TemporaryAccountInfo {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let query = {
            if let Some(query) = req.uri().query() {
                let input: AccessKeyQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                AccessKeyQuery::default()
            }
        };

        if query.access_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let Some(input_cred) = req.credentials() else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(req.uri(), req.headers()).unwrap_or_default(), input_cred.access_key()).await?;

        let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::ListTemporaryAccountsAdminAction),
                bucket: "",
                conditions: &get_condition_values(req.headers(), &cred, None, None, req.policy_source_addr()),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only: false,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        let (temp_account, session_policy) = iam_store
            .get_temporary_account(&query.access_key)
            .await
            .map_err(|e| map_temp_account_lookup_error(e, "get temporary account failed"))?;

        let resp: TemporaryAccountInfoResp = build_info_service_account_resp(&iam_store, &temp_account, session_policy).await?;
        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;
        let (body, content_type) = encode_compatible_admin_payload(req.uri().path(), &cred.secret_key, body)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().expect("valid header value"));

        Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(body), header))
    }
}

pub struct InfoAccessKey {}
#[async_trait::async_trait]
impl Operation for InfoAccessKey {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let query = {
            if let Some(query) = req.uri().query() {
                let input: AccessKeyQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed1"))?;
                input
            } else {
                AccessKeyQuery::default()
            }
        };

        let Some(input_cred) = req.credentials() else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(req.uri(), req.headers()).unwrap_or_default(), input_cred.access_key()).await?;

        let access_key = if query.access_key.is_empty() {
            cred.access_key.clone()
        } else {
            query.access_key
        };

        let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let target_cred = iam_store.get_user(&access_key).await.map(|identity| identity.credentials);

        let conditions = get_condition_values(req.headers(), &cred, None, None, req.policy_source_addr());
        let claims = cred.claims_or_empty();

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::ListServiceAccountsAdminAction),
                bucket: "",
                conditions: &conditions,
                is_owner: owner,
                object: "",
                claims,
                deny_only: false,
            })
            .await
        {
            let no_explicit_deny = if !may_use_owner_self_service_fallback(&cred) {
                false
            } else {
                iam_store
                    .is_allowed(&Args {
                        account: &cred.access_key,
                        groups: &cred.groups,
                        action: Action::AdminAction(AdminAction::ListServiceAccountsAdminAction),
                        bucket: "",
                        conditions: &conditions,
                        is_owner: owner,
                        object: "",
                        claims,
                        deny_only: true,
                    })
                    .await
            };
            if !no_explicit_deny {
                return Err(s3_error!(AccessDenied, "access denied"));
            }

            let Some(target_cred) = target_cred.as_ref() else {
                return Err(s3_error!(AccessDenied, "access denied"));
            };

            if !can_fallback_view_access_key_info_after_admin_check_failed(&cred, target_cred, no_explicit_deny) {
                return Err(s3_error!(AccessDenied, "access denied"));
            }
        }

        let Some(target_cred) = target_cred else {
            return Err(s3_error!(InvalidRequest, "access key not exist"));
        };

        let resp = access_key_identity::resolve_info_access_key_resp(&iam_store, access_key, target_cred).await?;

        let body = serde_json::to_vec(&resp).map_err(|e| s3_error!(InternalError, "marshal body failed, e: {:?}", e))?;
        let (body, content_type) = encode_compatible_admin_payload(req.uri().path(), &cred.secret_key, body)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().expect("valid header value"));

        Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(body), header))
    }
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct ListServiceAccountQuery {
    pub user: Option<String>,
}

pub struct ListServiceAccount {}
#[async_trait::async_trait]
impl Operation for ListServiceAccount {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let query = {
            if let Some(query) = req.uri().query() {
                let input: ListServiceAccountQuery = from_bytes(query.as_bytes())
                    .map_err(|_e| s3_error!(InvalidArgument, "invalid service account query parameters"))?;
                input
            } else {
                ListServiceAccountQuery::default()
            }
        };

        let Some(input_cred) = req.credentials() else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(req.uri(), req.headers()).unwrap_or_default(), input_cred.access_key())
                .await
                .map_err(|e| {
                    debug!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
                        event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
                        result = "check_key_failed",
                        error = ?e,
                        "admin service account state"
                    );
                    s3_error!(InternalError, "check key failed")
                })?;

        // let target_account = if let Some(user) = query.user {
        //     if user != input_cred.access_key {
        //         user
        //     } else if cred.parent_user.is_empty() {
        //         input_cred.access_key
        //     } else {
        //         cred.parent_user
        //     }
        // } else if cred.parent_user.is_empty() {
        //     input_cred.access_key
        // } else {
        //     cred.parent_user
        // };

        let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        // Cross-user listing, service accounts, and policy-bounded STS sessions
        // must be explicitly authorized by ListServiceAccounts; self-listing by
        // the user or an unrestricted STS session still enforces explicit Deny
        // statements (deny_only).
        let cross_user = query.user.as_ref().is_some_and(|v| v != &cred.access_key);
        let deny_only = !cross_user && may_use_owner_self_service_fallback(&cred);
        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::ListServiceAccountsAdminAction),
                bucket: "",
                conditions: &get_condition_values(req.headers(), &cred, None, None, req.policy_source_addr()),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only,
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        let target_account = if cross_user {
            query.user.unwrap_or_default()
        } else {
            request_user_name(&cred).to_string()
        };

        let service_accounts = iam_store.list_service_accounts(&target_account).await.map_err(|e| {
            debug!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
                event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
                target_user = %target_account,
                result = "list_service_accounts_failed",
                error = ?e,
                "admin service account state"
            );
            s3_error!(InternalError, "list service account failed")
        })?;

        let accounts: Vec<ServiceAccountInfo> = service_accounts
            .into_iter()
            .map(|sa| ServiceAccountInfo {
                parent_user: sa.parent_user.clone(),
                account_status: sa.status.clone(),
                implied_policy: sa.is_implied_policy(), // or set according to your logic
                access_key: sa.access_key,
                name: sa.name,
                description: sa.description,
                expiration: expiration_for_admin_path(req.uri().path(), sa.expiration),
            })
            .collect();

        let data = serde_json::to_vec(&ListServiceAccountsResp { accounts })
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal users err {e}")))?;
        let (data, content_type) = encode_compatible_admin_payload(req.uri().path(), &cred.secret_key, data)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().expect("valid header value"));

        Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(data), header))
    }
}

#[derive(Debug, Deserialize)]
struct ListAccessKeysQuery {
    users: Vec<String>,
    all: bool,
    list_type: String,
}

fn default_access_key_list_type() -> String {
    ACCESS_KEY_LIST_ALL.to_string()
}

impl Default for ListAccessKeysQuery {
    fn default() -> Self {
        Self {
            users: Vec::new(),
            all: false,
            list_type: default_access_key_list_type(),
        }
    }
}

fn parse_bool_param(value: &str) -> bool {
    matches!(value, "true" | "1" | "on" | "yes")
}

fn parse_list_access_keys_query(query: Option<&str>) -> ListAccessKeysQuery {
    let mut parsed = ListAccessKeysQuery::default();

    let Some(query) = query else {
        return parsed;
    };

    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "users" if !value.is_empty() => {
                parsed.users.push(value.into_owned());
            }
            "all" => parsed.all = parse_bool_param(value.as_ref()),
            "listType" => parsed.list_type = value.into_owned(),
            _ => {}
        }
    }

    parsed
}

pub struct ListAccessKeysBulk {}
#[async_trait::async_trait]
impl Operation for ListAccessKeysBulk {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let query = { parse_list_access_keys_query(req.uri().query()) };

        if query.all && !query.users.is_empty() {
            return Err(s3_error!(InvalidRequest, "either specify users or all, not both"));
        }

        let Some(input_cred) = req.credentials() else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(req.uri(), req.headers()).unwrap_or_default(), input_cred.access_key()).await?;

        let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let mut requested_users = query.users;
        let mut self_only = !query.all && requested_users.is_empty();
        if !query.all && requested_users.len() == 1 {
            let requested_user = &requested_users[0];
            if requested_user == &cred.access_key || requested_user == &cred.parent_user {
                self_only = true;
            }
        }

        if query.all
            && !iam_store
                .is_allowed(&Args {
                    account: &cred.access_key,
                    groups: &cred.groups,
                    action: Action::AdminAction(AdminAction::ListUsersAdminAction),
                    bucket: "",
                    conditions: &get_condition_values(req.headers(), &cred, None, None, req.policy_source_addr()),
                    is_owner: owner,
                    object: "",
                    claims: cred.claims_or_empty(),
                    deny_only: false,
                })
                .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        if !iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::ListServiceAccountsAdminAction),
                bucket: "",
                conditions: &get_condition_values(req.headers(), &cred, None, None, req.policy_source_addr()),
                is_owner: owner,
                object: "",
                claims: cred.claims_or_empty(),
                deny_only: self_only && may_use_owner_self_service_fallback(&cred),
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "access denied"));
        }

        if self_only && requested_users.is_empty() {
            requested_users.push(request_user_name(&cred).to_string());
        }

        let checked_user_list = if query.all {
            let mut users = iam_store
                .list_users()
                .await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("list users err {e}")))?
                .into_keys()
                .collect::<Vec<_>>();

            if let Some(sys_cred) = current_action_credentials() {
                users.push(sys_cred.access_key);
            }
            users
        } else {
            // Keep requested identities as-is. Some valid parent users (for example external
            // identities) may not be persisted as regular IAM users, but can still own keys.
            requested_users
        };

        let (list_sts_keys, list_service_accounts) = match query.list_type.as_str() {
            ACCESS_KEY_LIST_USERS_ONLY => (false, false),
            ACCESS_KEY_LIST_STS_ONLY => (true, false),
            ACCESS_KEY_LIST_SVCACC_ONLY => (false, true),
            ACCESS_KEY_LIST_ALL => (true, true),
            _ => return Err(s3_error!(InvalidRequest, "invalid list type")),
        };

        let mut access_key_map = HashMap::new();
        for user in checked_user_list {
            let mut access_keys = ListAccessKeysResp::default();

            if list_sts_keys {
                let sts_keys = iam_store.list_sts_accounts(&user).await.map_err(|e| {
                    debug!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
                        event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
                        target_user = %user,
                        result = "list_sts_accounts_failed",
                        error = ?e,
                        "admin service account state"
                    );
                    s3_error!(InternalError, "list sts account failed")
                })?;

                access_keys.sts_keys = sts_keys
                    .into_iter()
                    .map(|sts| {
                        access_key_identity::list_entry_from_credentials(
                            &sts,
                            expiration_for_admin_path(req.uri().path(), sts.expiration),
                        )
                    })
                    .collect();

                if !list_service_accounts && access_keys.sts_keys.is_empty() {
                    continue;
                }
            }

            if list_service_accounts {
                let service_accounts = iam_store.list_service_accounts(&user).await.map_err(|e| {
                    debug!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
                        event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
                        target_user = %user,
                        result = "list_service_accounts_failed",
                        error = ?e,
                        "admin service account state"
                    );
                    s3_error!(InternalError, "list service account failed")
                })?;

                access_keys.service_accounts = service_accounts
                    .into_iter()
                    .map(|svc| {
                        access_key_identity::list_entry_from_credentials(
                            &svc,
                            expiration_for_admin_path(req.uri().path(), svc.expiration),
                        )
                    })
                    .collect();

                if !list_sts_keys && access_keys.service_accounts.is_empty() {
                    continue;
                }
            }

            access_key_map.insert(user, access_keys);
        }

        let data = serde_json::to_vec(&access_key_map)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal access keys err {e}")))?;
        let (data, content_type) = encode_compatible_admin_payload(req.uri().path(), &cred.secret_key, data)?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, content_type.parse().expect("valid header value"));

        Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(data), header))
    }
}

fn map_owner_service_account_delete_error(access_key: &str, err: rustfs_iam::error::Error) -> S3Error {
    if matches!(err, rustfs_iam::error::Error::IAMActionNotAllowed) {
        return s3_error!(AccessDenied, "access denied");
    }
    if is_err_no_such_service_account(&err) {
        return s3_error!(InvalidRequest, "service account not exist");
    }
    debug!(
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
        event = "service_account_delete_failed",
        access_key = %access_key,
        error = ?err,
        "Failed to delete service account"
    );
    s3_error!(InternalError, "delete service account failed")
}

pub struct DeleteServiceAccount {}
#[async_trait::async_trait]
impl Operation for DeleteServiceAccount {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let Some(input_cred) = req.credentials() else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(req.uri(), req.headers()).unwrap_or_default(), input_cred.access_key())
                .await
                .map_err(|e| {
                    debug!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
                        event = "service_account_auth_failed",
                        action = "delete",
                        error = ?e,
                        "Service account authentication failed"
                    );
                    s3_error!(InternalError, "check key failed")
                })?;

        let query = {
            if let Some(query) = req.uri().query() {
                let input: AccessKeyQuery = from_bytes(query.as_bytes())
                    .map_err(|_e| s3_error!(InvalidArgument, "invalid access key query parameters"))?;
                input
            } else {
                AccessKeyQuery::default()
            }
        };

        if query.access_key.is_empty() {
            return Err(s3_error!(InvalidArgument, "access key is empty"));
        }

        let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let conditions = get_condition_values(req.headers(), &cred, None, None, req.policy_source_addr());
        let claims = cred.claims_or_empty();

        let deleted_at = if iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::AdminAction(AdminAction::RemoveServiceAccountAdminAction),
                bucket: "",
                conditions: &conditions,
                is_owner: owner,
                object: "",
                claims,
                deny_only: false,
            })
            .await
        {
            let existing_account = iam_store.get_service_account(&query.access_key).await;
            if let Err(err) = &existing_account
                && is_err_no_such_service_account(err)
                && matches!(iam_store.get_service_account_revision(&query.access_key).await, Ok(None))
            {
                return Err(s3_error!(InvalidRequest, "service account not exist"));
            }

            iam_store
                .delete_service_account_with_revision(&query.access_key, true)
                .await
                .map_err(|e| {
                    debug!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
                        event = "service_account_delete_failed",
                        access_key = %query.access_key,
                        error = ?e,
                        "Failed to delete service account"
                    );
                    s3_error!(InternalError, "delete service account failed")
                })?
        } else {
            let no_explicit_deny = if !may_use_owner_self_service_fallback(&cred) {
                false
            } else {
                iam_store
                    .is_allowed(&Args {
                        account: &cred.access_key,
                        groups: &cred.groups,
                        action: Action::AdminAction(AdminAction::RemoveServiceAccountAdminAction),
                        bucket: "",
                        conditions: &conditions,
                        is_owner: owner,
                        object: "",
                        claims,
                        deny_only: true,
                    })
                    .await
            };

            // Fail closed: a caller without RemoveServiceAccount may delete a
            // service account only when it qualifies for owner self-service
            // fallback (neither a service account nor a policy-bounded STS
            // session), has no explicit Deny on RemoveServiceAccount, and owns
            // the loaded target (backlog#806, #9514). Bind deletion to the
            // authorized target revision so concurrent replacement cannot be
            // deleted under a stale check.
            if !no_explicit_deny {
                return Err(s3_error!(AccessDenied, "access denied"));
            }

            let Ok(Some((target_cred, revision))) = iam_store.get_service_account_revision(&query.access_key).await else {
                return Err(s3_error!(AccessDenied, "access denied"));
            };

            if !non_admin_may_access_owned_service_account(&cred, Some(target_cred.parent_user.as_str()), no_explicit_deny) {
                return Err(s3_error!(AccessDenied, "access denied"));
            }

            let deleted_at = iam_store
                .delete_service_account_if_revision(&query.access_key, &target_cred.parent_user, &revision, true)
                .await
                .map_err(|err| map_owner_service_account_delete_error(&query.access_key, err))?;
            Some(deleted_at)
        };

        if let Some(deleted_at) = deleted_at
            && let Err(err) = site_replication_iam_change_hook(SRIAMItem {
                r#type: "service-account".to_string(),
                svc_acc_change: Some(SRSvcAccChange {
                    delete: Some(SRSvcAccDelete {
                        access_key: query.access_key.clone(),
                        api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    }),
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                }),
                updated_at: Some(deleted_at),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            })
            .await
        {
            warn!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SERVICE_ACCOUNT,
                event = EVENT_ADMIN_SERVICE_ACCOUNT_STATE,
                access_key = %query.access_key,
                action = "delete",
                result = "site_replication_hook_failed",
                error = ?err,
                "admin service account state"
            );
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));
        header.insert(CONTENT_LENGTH, "0".parse().expect("valid header value"));
        Ok(AdminResponse::with_headers(
            delete_service_account_success_status(req.uri().path()),
            Body::empty(),
            header,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA;
    use serde_json::json;
    use serde_urlencoded::from_bytes;

    #[test]
    #[serial_test::serial]
    fn service_account_handlers_authenticate_before_decryption_or_lookup() {
        crate::app::gating_test_env::run_large_stack_test("service-account-auth-first", || async {
            use crate::admin::runtime_sources::IamInterface;
            use crate::admin::runtime_sources::{AppContext, publish_test_app_context};
            use rustfs_iam::store::object::ObjectStore;
            use std::sync::Arc;
            struct ReadyIam(Arc<rustfs_iam::sys::IamSys<ObjectStore>>);
            impl IamInterface for ReadyIam {
                fn handle(&self) -> Arc<rustfs_iam::sys::IamSys<ObjectStore>> {
                    self.0.clone()
                }
                fn is_ready(&self) -> bool {
                    true
                }
            }
            let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
            let (_directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
            ObjectStore::new(store.clone())
                .save_iam_config(
                    serde_json::json!({"version": 1}),
                    format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
                )
                .await
                .expect("initialize IAM storage");
            let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("initialized IAM");
            let disabled_key = format!("disabled-{}", uuid::Uuid::new_v4());
            iam.create_user(
                &disabled_key,
                &rustfs_madmin::AddOrUpdateUserReq {
                    secret_key: "disabled-user-secret".to_owned(),
                    policy: None,
                    status: rustfs_madmin::AccountStatus::Disabled,
                },
            )
            .await
            .expect("create disabled IAM caller");
            let context = Arc::new(AppContext::new(store, Arc::new(ReadyIam(iam)), ambient.kms()));
            assert!(context.publish_action_credentials(StoredCredentials {
                access_key: "auth-first-root".to_owned(),
                secret_key: "auth-first-root-secret".to_owned(),
                status: "on".to_owned(),
                ..Default::default()
            }));
            publish_test_app_context(context);
            for caller in ["keystone:forged", disabled_key.as_str()] {
                for operation in ["update-service-account", "info-service-account"] {
                    let method = if operation == "update-service-account" {
                        http::Method::POST
                    } else {
                        http::Method::GET
                    };
                    let req = crate::admin::operation::test_support::TestRequest::new(
                        method,
                        format!("/minio/admin/v3/{operation}?accessKey=missing-target").as_str(),
                    )
                    .credentials(caller, "disabled-user-secret", None)
                    .body(vec![0u8; 64])
                    .build();
                    let result = if operation == "update-service-account" {
                        UpdateServiceAccount {}.call(req, Params::new()).await
                    } else {
                        InfoServiceAccount {}.call(req, Params::new()).await
                    };
                    assert_eq!(
                        result
                            .expect_err("invalid identity precedes KDF and missing target lookup")
                            .code(),
                        &S3ErrorCode::InvalidAccessKeyId,
                        "{caller}: {operation}"
                    );
                }
            }
            publish_test_app_context(ambient);
        });
    }

    #[test]
    fn access_key_query_supports_external_alias() {
        let query: AccessKeyQuery = from_bytes(b"access-key=test-access-key").expect("parse query");
        assert_eq!(query.access_key, "test-access-key");
    }

    #[test]
    fn service_account_cannot_delegate_a_longer_lifetime() {
        let deadline = OffsetDateTime::UNIX_EPOCH + time::Duration::days(10);
        let earlier = deadline - time::Duration::hours(1);
        let later = deadline + time::Duration::days(365);
        let mut caller = StoredCredentials {
            access_key: "expiring-service".to_string(),
            parent_user: "parent".to_string(),
            expiration: Some(deadline),
            claims: Some(HashMap::from([("sa-policy".to_string(), json!("inherited-policy"))])),
            ..Default::default()
        };
        assert_eq!(bounded_service_account_expiration(&caller, None), Some(deadline));
        assert_eq!(bounded_service_account_expiration(&caller, Some(later)), Some(deadline));
        assert_eq!(bounded_service_account_expiration(&caller, Some(earlier)), Some(earlier));
        caller.expiration = None;
        assert_eq!(bounded_service_account_expiration(&caller, None), None);
        caller.expiration = Some(deadline);
        caller.claims = None;
        caller.session_token = "signed-sts".to_string();
        assert_eq!(
            bounded_service_account_expiration(&caller, None),
            None,
            "unrestricted STS retains independent service lifetime"
        );
        assert_eq!(bounded_service_account_expiration(&caller, Some(later)), Some(later));

        // A policy-bounded STS session must cap created/updated service accounts at its own lifetime.
        for claim_key in [
            rustfs_iam::sys::SESSION_POLICY_NAME,
            rustfs_iam::sys::SESSION_POLICY_NAME_EXTRACTED,
        ] {
            let mut bounded_sts = StoredCredentials {
                access_key: "bounded-sts".to_string(),
                parent_user: "parent".to_string(),
                session_token: "signed-sts".to_string(),
                expiration: Some(deadline),
                claims: Some(HashMap::from([(claim_key.to_string(), json!("e30"))])),
                ..Default::default()
            };
            assert_eq!(bounded_service_account_expiration(&bounded_sts, None), Some(deadline));
            assert_eq!(bounded_service_account_expiration(&bounded_sts, Some(later)), Some(deadline));
            assert_eq!(bounded_service_account_expiration(&bounded_sts, Some(earlier)), Some(earlier));

            // Also falls back to the JWT `exp` claim if `expiration` was not populated on the struct.
            bounded_sts.expiration = None;
            bounded_sts
                .claims
                .as_mut()
                .expect("claims")
                .insert("exp".to_string(), json!(deadline.unix_timestamp()));
            assert_eq!(bounded_service_account_expiration(&bounded_sts, None), Some(deadline));
            assert_eq!(bounded_service_account_expiration(&bounded_sts, Some(later)), Some(deadline));
        }
    }

    #[tokio::test]
    async fn service_account_duration_condition_enforces_finite_and_unlimited_lifetimes() {
        let policy = Policy::parse_config(
            br#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["admin:CreateServiceAccount","admin:UpdateServiceAccount"],"Resource":"*"},
            {"Effect":"Deny","Action":["admin:CreateServiceAccount","admin:UpdateServiceAccount"],"Resource":"*",
             "Condition":{"NumericGreaterThan":{"svc:DurationSeconds":"3600"}}}
        ]}"#,
        )
        .expect("service lifetime policy");
        let now = OffsetDateTime::UNIX_EPOCH;
        for (expiration, allowed) in [
            (Some(now + time::Duration::seconds(3600)), true),
            (Some(now + time::Duration::seconds(3601)), false),
            (None, false),
        ] {
            let mut conditions = HashMap::from([("DurationSeconds".to_string(), vec!["1".to_string()])]);
            set_service_account_duration_condition(&mut conditions, expiration, now);
            for action in [
                AdminAction::CreateServiceAccountAdminAction,
                AdminAction::UpdateServiceAccountAdminAction,
            ] {
                assert_eq!(
                    policy
                        .is_allowed(&Args {
                            account: "user",
                            groups: &None,
                            action: Action::AdminAction(action),
                            conditions: &conditions,
                            is_owner: false,
                            claims: &HashMap::new(),
                            deny_only: false,
                            bucket: "",
                            object: "",
                        })
                        .await,
                    allowed
                );
            }
        }
    }

    #[test]
    fn non_admin_delete_is_fail_closed_on_unloadable_target() {
        // Regression (backlog#806, #9514): a non-admin caller may fall back to
        // owner access ONLY when it is not itself a service account, has no
        // explicit Deny, and owns the successfully loaded target.
        let alice = StoredCredentials {
            access_key: "alice".to_string(),
            ..Default::default()
        };
        let empty = StoredCredentials::default();
        let alice_sts = StoredCredentials {
            access_key: "alice-sts".to_string(),
            session_token: "session-token".to_string(),
            parent_user: "alice".to_string(),
            ..Default::default()
        };
        let alice_sa = StoredCredentials {
            access_key: "alice-sa".to_string(),
            parent_user: "alice".to_string(),
            claims: Some(HashMap::from([(IAM_POLICY_CLAIM_NAME_SA.to_string(), json!("session-policy"))])),
            ..Default::default()
        };

        assert!(
            non_admin_may_access_owned_service_account(&alice, Some("alice"), true),
            "owner without explicit Deny may access own service account"
        );
        assert!(
            !non_admin_may_access_owned_service_account(&alice, Some("alice"), false),
            "owner with explicit Deny must be denied"
        );
        assert!(
            !non_admin_may_access_owned_service_account(&alice, Some("bob"), true),
            "non-owner must be denied"
        );
        assert!(
            !non_admin_may_access_owned_service_account(&alice, None, true),
            "unloadable target (None) must be denied (fail-closed), not bypass the owner check"
        );
        assert!(
            !non_admin_may_access_owned_service_account(&alice, Some(""), true),
            "empty target parent must be denied"
        );
        assert!(
            !non_admin_may_access_owned_service_account(&empty, None, true),
            "empty caller + unloadable target must still deny"
        );
        assert!(
            !non_admin_may_access_owned_service_account(&alice_sa, Some("alice"), true),
            "service account caller must never impersonate its parent (#9514)"
        );
        assert!(
            !non_admin_may_access_owned_service_account(&alice_sa, Some("alice"), false),
            "service account caller with explicit Deny must be denied (#9514)"
        );
        assert!(
            non_admin_may_access_owned_service_account(&alice_sts, Some("alice"), true),
            "unrestricted STS session without explicit Deny retains parent owner fallback"
        );
        assert!(
            !non_admin_may_access_owned_service_account(&alice_sts, Some("alice"), false),
            "STS session with explicit Deny must be denied"
        );
        for claim_key in [
            rustfs_iam::sys::SESSION_POLICY_NAME,
            rustfs_iam::sys::SESSION_POLICY_NAME_EXTRACTED,
        ] {
            let restricted_sts = StoredCredentials {
                claims: Some(HashMap::from([(claim_key.to_string(), json!("e30"))])),
                ..alice_sts.clone()
            };
            assert!(
                !non_admin_may_access_owned_service_account(&restricted_sts, Some("alice"), true),
                "session-policy-restricted STS ({claim_key}) must not use parent owner fallback"
            );
        }
    }

    #[test]
    fn ghsa_5354_non_owner_service_account_parent_confined_to_scope() {
        // Regression (GHSA-5354): a caller holding CreateServiceAccountAdminAction
        // but resolving to owner=false must not be able to parent a new service
        // account to the root credential (or any other user). A root-parented SA
        // authenticates as owner, so allowing this is a full-takeover escalation.
        let root = rustfs_credentials::DEFAULT_ACCESS_KEY; // "rustfsadmin"

        // Attack: non-owner "alice" targets root as the parent -> DENY.
        assert!(
            !add_service_account_parent_within_scope(root, root, "alice", "alice", false),
            "non-owner must not parent a service account to root"
        );
        // Non-owner targeting an unrelated user -> DENY.
        assert!(
            !add_service_account_parent_within_scope(root, "bob", "alice", "alice", false),
            "non-owner must not parent a service account to another user"
        );
        // Self-scoped creation (default target == own key) -> ALLOW.
        assert!(
            add_service_account_parent_within_scope(root, "alice", "alice", "alice", false),
            "non-owner may create a service account for itself"
        );
        // Derived credential targeting its own parent -> ALLOW.
        assert!(
            add_service_account_parent_within_scope(root, "parent-user", "svc-key", "parent-user", false),
            "derived credential may create a service account under its own parent"
        );
        // Owner may target any parent, including root (cross-user creation intact).
        assert!(
            add_service_account_parent_within_scope(root, root, root, root, true),
            "owner retains cross-user / root-parent creation"
        );
        assert!(
            add_service_account_parent_within_scope(root, "bob", root, root, true),
            "owner may create a service account for another user"
        );
    }

    #[test]
    fn ghsa_5354_scope_guard_preserves_non_root_self_scope() {
        let root = rustfs_credentials::DEFAULT_ACCESS_KEY;
        let cases: &[(&str, &str, &str, bool, bool)] = &[
            // Derived non-owner (own key != parent) aiming at root -> deny.
            (root, "attacker-sa", "alice", false, false),
            // Derived non-owner aiming at an unrelated user -> deny.
            ("bob", "attacker-sa", "alice", false, false),
            // Derived non-owner aiming at its own parent -> allow.
            ("alice", "attacker-sa", "alice", false, true),
            // Derived non-owner aiming at its own key -> allow.
            ("attacker-sa", "attacker-sa", "alice", false, true),
            // Top-level non-owner aiming at itself -> allow.
            ("alice", "alice", "alice", false, true),
            // Owner is unrestricted, including root and a derived shape.
            (root, "attacker-sa", "alice", true, true),
            ("bob", root, root, true, true),
        ];
        for &(target_user, req_user, req_parent_user, owner, expected) in cases {
            assert_eq!(
                add_service_account_parent_within_scope(root, target_user, req_user, req_parent_user, owner),
                expected,
                "scope guard mismatch for (target={target_user}, req_user={req_user}, req_parent={req_parent_user}, owner={owner})"
            );
        }
    }

    #[test]
    fn credential_boundary_root_derived_non_owner_cannot_create_owner_service_account() {
        let root = "deployment-root";
        for target in [root, "restricted-service-account", "other-user"] {
            assert!(
                !add_service_account_parent_within_scope(root, target, "restricted-service-account", root, false),
                "a root-derived non-owner must be denied even when targeting its own key: {target}"
            );
        }
        assert!(add_service_account_parent_within_scope(root, root, root, root, true));
        assert!(add_service_account_parent_within_scope(
            root,
            "alice",
            "alice-service-account",
            "alice",
            false
        ));
        assert!(add_service_account_parent_within_scope(
            root,
            "alice-service-account",
            "alice-service-account",
            "alice",
            false
        ));
    }

    #[test]
    fn guess_user_provider_detects_builtin_accounts() {
        let credentials = StoredCredentials {
            access_key: "builtin-user".to_string(),
            secret_key: "secret-key".to_string(),
            ..Default::default()
        };

        assert_eq!(access_key_identity::guess_user_provider(&credentials), "builtin");
    }

    #[test]
    fn guess_user_provider_detects_ldap_accounts() {
        let credentials = StoredCredentials {
            access_key: "svc".to_string(),
            secret_key: "secret-key".to_string(),
            parent_user: "parent".to_string(),
            claims: Some(HashMap::from([
                (IAM_POLICY_CLAIM_NAME_SA.to_string(), json!("embedded")),
                ("ldap:user".to_string(), json!("uid=rustfs,ou=people,dc=example,dc=com")),
            ])),
            ..Default::default()
        };

        assert_eq!(access_key_identity::guess_user_provider(&credentials), "ldap");
    }

    #[test]
    fn guess_user_provider_detects_openid_accounts() {
        let credentials = StoredCredentials {
            access_key: "sts".to_string(),
            secret_key: "secret-key".to_string(),
            session_token: "session-token".to_string(),
            parent_user: "parent".to_string(),
            claims: Some(HashMap::from([("sub".to_string(), json!("subject-123"))])),
            ..Default::default()
        };

        assert_eq!(access_key_identity::guess_user_provider(&credentials), "openid");
    }

    #[test]
    fn provider_specific_info_uses_known_claims() {
        let claims = HashMap::from([
            ("ldap:user".to_string(), json!("uid=rustfs,ou=people,dc=example,dc=com")),
            ("sub".to_string(), json!("subject-123")),
            ("name".to_string(), json!("RustFS User")),
        ]);

        let ldap_info = access_key_identity::ldap_specific_info(Some(&claims));
        let openid_info = access_key_identity::openid_specific_info(Some(&claims));

        assert_eq!(ldap_info.username.as_deref(), Some("uid=rustfs,ou=people,dc=example,dc=com"));
        assert_eq!(openid_info.user_id.as_deref(), Some("subject-123"));
        assert_eq!(openid_info.user_id_claim.as_deref(), Some("sub"));
        assert_eq!(openid_info.display_name.as_deref(), Some("RustFS User"));
        assert_eq!(openid_info.display_name_claim.as_deref(), Some("name"));
    }

    #[test]
    fn list_access_keys_query_parses_external_parameters() {
        let query = parse_list_access_keys_query(Some("users=alice&users=bob&all=true&listType=svcacc-only"));

        assert_eq!(query.users, vec!["alice".to_string(), "bob".to_string()]);
        assert!(query.all);
        assert_eq!(query.list_type, ACCESS_KEY_LIST_SVCACC_ONLY);
    }

    #[test]
    fn list_access_keys_query_ignores_empty_users_values() {
        let query = parse_list_access_keys_query(Some("users=&users=alice&users=&listType=all"));

        assert_eq!(query.users, vec!["alice".to_string()]);
        assert!(!query.all);
        assert_eq!(query.list_type, ACCESS_KEY_LIST_ALL);
    }

    #[test]
    fn list_access_keys_query_all_with_empty_users_does_not_conflict() {
        let query = parse_list_access_keys_query(Some("users=&all=true&listType=all"));

        assert!(query.users.is_empty());
        assert!(query.all);
        assert_eq!(query.list_type, ACCESS_KEY_LIST_ALL);
        assert!(!query.all || query.users.is_empty());
    }

    #[test]
    fn list_access_keys_query_defaults_to_all_list_type() {
        let query = ListAccessKeysQuery::default();
        assert_eq!(query.list_type, ACCESS_KEY_LIST_ALL);
    }

    #[test]
    fn list_service_account_cross_user_uses_list_service_accounts_action() {
        let src = include_str!("service_account.rs");
        let list_start = src
            .find("impl Operation for ListServiceAccount")
            .expect("ListServiceAccount operation should exist");
        let list_block = &src[list_start..];
        let list_end = list_block
            .find("struct ListAccessKeysQuery")
            .expect("ListAccessKeysQuery marker should exist");
        let list_block = &list_block[..list_end];

        assert!(
            list_block.contains("query.user.as_ref().is_some_and(") && list_block.contains("v != &cred.access_key"),
            "cross-user ListServiceAccount path should stay explicitly guarded"
        );
        assert!(
            list_block.contains("ListServiceAccountsAdminAction"),
            "cross-user ListServiceAccount should authorize with ListServiceAccountsAdminAction"
        );
        assert!(
            !list_block.contains("UpdateServiceAccountAdminAction"),
            "cross-user ListServiceAccount must not require UpdateServiceAccountAdminAction"
        );
    }

    #[test]
    fn delete_service_account_uses_external_success_status() {
        assert_eq!(
            delete_service_account_success_status("/minio/admin/v3/delete-service-account"),
            StatusCode::NO_CONTENT
        );
    }

    #[test]
    fn delete_service_account_keeps_rustfs_console_success_status() {
        assert_eq!(
            delete_service_account_success_status("/rustfs/admin/v3/delete-service-account"),
            StatusCode::OK
        );
    }

    #[test]
    fn expiration_for_external_admin_path_uses_sentinel() {
        assert_eq!(
            expiration_for_admin_path("/minio/admin/v3/list-service-accounts", None),
            Some(OffsetDateTime::UNIX_EPOCH)
        );
    }

    #[test]
    fn expiration_for_rustfs_admin_path_preserves_none() {
        assert_eq!(expiration_for_admin_path("/rustfs/admin/v3/list-service-accounts", None), None);
    }

    #[test]
    fn map_service_account_lookup_error_reports_missing_accounts_explicitly() {
        let err = map_service_account_lookup_error(
            rustfs_iam::error::Error::NoSuchServiceAccount("missing".to_string()),
            "get service account failed",
        );

        assert_eq!(*err.code(), S3ErrorCode::NoSuchResource);
        assert_eq!(err.message(), Some("service account 'missing' does not exist"));
    }

    #[tokio::test]
    async fn delegated_service_account_replication_survives_request_cancellation() {
        use tokio::sync::oneshot;
        use tokio::time::{Duration, timeout};

        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (event_tx, event_rx) = oneshot::channel();
        let updated_at = OffsetDateTime::now_utc();
        let create = async move {
            started_tx.send(()).expect("signal creation");
            release_rx.await.expect("release write");
            let credentials = StoredCredentials {
                access_key: "backup-key".to_owned(),
                parent_user: "backup-user".to_owned(),
                ..Default::default()
            };
            Ok((credentials, updated_at, HashMap::new()))
        };
        let waiter = tokio::spawn(create_and_replicate_service_account(create, None, None, |item| async move {
            event_tx.send(item).expect("capture site replication event");
            Ok(())
        }));
        let deadline = Duration::from_secs(5);
        timeout(deadline, started_rx)
            .await
            .expect("creation started")
            .expect("start signal");
        waiter.abort();
        assert!(waiter.await.expect_err("request cancelled").is_cancelled());
        release_tx.send(()).expect("write still admitted");
        let item = timeout(deadline, event_rx)
            .await
            .expect("replication completed")
            .expect("replication event");
        assert_eq!(item.updated_at, Some(updated_at));
        let created = item.svc_acc_change.and_then(|change| change.create).expect("create event");
        assert_eq!(created.access_key, "backup-key");
        assert_eq!(created.parent, "backup-user");
    }

    #[test]
    fn add_service_account_maps_iam_create_errors_to_s3_errors() {
        use rustfs_iam::error::Error;

        for (error, expected) in [
            (Error::AccessKeyAlreadyExists, S3ErrorCode::InvalidArgument),
            (Error::NoSuchUser("missing".into()), S3ErrorCode::InvalidRequest),
            (Error::Io(std::io::Error::other("storage unavailable")), S3ErrorCode::InternalError),
        ] {
            assert_eq!(map_service_account_create_error(error).code(), &expected);
        }
    }

    #[test]
    fn owner_service_account_delete_maps_revision_mismatch_to_access_denied() {
        use rustfs_iam::error::Error;

        assert_eq!(
            map_owner_service_account_delete_error("sa-key", Error::IAMActionNotAllowed).code(),
            &S3ErrorCode::AccessDenied
        );
        assert_eq!(
            map_owner_service_account_delete_error("sa-key", Error::NoSuchServiceAccount("sa-key".into())).code(),
            &S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            map_owner_service_account_delete_error("sa-key", Error::Io(std::io::Error::other("storage unavailable"))).code(),
            &S3ErrorCode::InternalError
        );
    }

    #[test]
    fn map_temp_account_lookup_error_reports_missing_access_keys_explicitly() {
        let err = map_temp_account_lookup_error(
            rustfs_iam::error::Error::NoSuchTempAccount("missing".to_string()),
            "get temporary account failed",
        );

        assert_eq!(*err.code(), S3ErrorCode::NoSuchResource);
        assert_eq!(err.message(), Some("temp account 'missing' does not exist"));
    }

    #[test]
    fn parse_update_service_account_policy_keeps_explicit_empty_policy() {
        let policy = parse_update_service_account_policy(Some(json!({}))).expect("empty policy should parse");

        assert!(policy.is_some());
        let policy = policy.unwrap();
        assert!(policy.version.is_empty());
        assert!(policy.statements.is_empty());
    }

    #[test]
    fn sparse_explicit_create_policy_is_normalized_before_persistence_and_replication() {
        let id_only = parse_new_service_account_policy(Some(&json!({"ID": "deny-boundary", "Version": "", "Statement": []})))
            .expect("ID-only policy should normalize")
            .expect("normalized explicit policy");
        assert_eq!(id_only.id.as_str(), "deny-boundary");
        assert_eq!(id_only.version, DEFAULT_VERSION);
        assert!(!id_only.statements.is_empty());

        let missing_version = parse_new_service_account_policy(Some(&json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": ["s3:GetObject"],
                "Resource": ["arn:aws:s3:::bucket/*"]
            }]
        })))
        .expect("policy version should normalize")
        .expect("normalized explicit policy");
        assert_eq!(missing_version.version, DEFAULT_VERSION);
    }

    #[test]
    fn explicit_empty_create_policy_is_canonicalized_as_inherited() {
        let policy = parse_new_service_account_policy(Some(&json!({}))).expect("parse empty create policy");

        assert!(policy.is_none());
    }

    #[test]
    fn sparse_explicit_update_policy_preserves_existing_semantics() {
        let id_only = parse_update_service_account_policy(Some(json!({"ID": "clear-boundary", "Version": "", "Statement": []})))
            .expect("parse ID-only update")
            .expect("explicit update policy");
        assert_eq!(id_only.id.as_str(), "clear-boundary");
        assert!(id_only.version.is_empty());
        assert!(id_only.statements.is_empty());

        let missing_version = parse_update_service_account_policy(Some(json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": ["s3:GetObject"],
                "Resource": ["arn:aws:s3:::bucket/*"]
            }]
        })))
        .expect("parse versionless update")
        .expect("explicit update policy");
        assert!(missing_version.version.is_empty());
        assert!(!missing_version.statements.is_empty());
    }

    #[test]
    fn replication_update_stays_partial() {
        let update = UpdateServiceAccountReq {
            new_policy: Some(json!({})),
            new_secret_key: None,
            new_status: None,
            new_name: None,
            new_description: None,
            new_expiration: None,
        };

        let session_policy = parse_update_service_account_policy(update.new_policy.clone()).expect("parse update policy");
        let change = service_account_update_replication_change("OIDCSERVICEACCOUNT01", &update, session_policy.as_ref())
            .expect("build replication update");

        assert!(change.create.is_none());
        assert!(change.delete.is_none());
        let update = change.update.expect("partial update");
        let cleared: Policy =
            serde_json::from_str(update.session_policy.as_str().expect("explicit policy clear")).expect("parse policy clear");
        assert!(cleared.id.is_empty());
        assert!(cleared.version.is_empty());
        assert!(cleared.statements.is_empty());
    }

    #[test]
    fn parse_service_account_policy_reports_missing_resource() {
        let err = parse_service_account_policy(&json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"]
                }
            ]
        }))
        .expect_err("policy without Resource should be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("invalid service account policy: Resource is empty"));
    }

    #[test]
    fn parse_service_account_policy_preserves_policy_validation_details() {
        let err = parse_service_account_policy(&json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "admin:ServerInfo"],
                    "Resource": ["arn:aws:s3:::bucket/*"]
                }
            ]
        }))
        .expect_err("policy with mixed action families should be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
        assert_eq!(
            err.message(),
            Some("invalid service account policy: 'Action' contains mixed action families in the same statement")
        );
    }

    #[test]
    fn update_service_account_requires_requester_parent_match() {
        let parent_owner = StoredCredentials {
            access_key: "owner-user".to_string(),
            parent_user: String::new(),
            ..Default::default()
        };
        let derived_owner = StoredCredentials {
            access_key: "sa-user".to_string(),
            parent_user: "owner-user".to_string(),
            ..Default::default()
        };
        let foreign_user = StoredCredentials {
            access_key: "other".to_string(),
            parent_user: String::new(),
            ..Default::default()
        };

        assert!(is_service_account_owner_of(&parent_owner, "owner-user"));
        assert!(is_service_account_owner_of(&derived_owner, "owner-user"));
        assert!(!is_service_account_owner_of(&foreign_user, "owner-user"));
    }

    #[test]
    fn update_service_account_denies_non_owner_root_parented_updates() {
        let root = "deployment-root";
        let root_derived_sa = StoredCredentials {
            access_key: "bounded-root-sa".to_string(),
            parent_user: root.to_string(),
            ..Default::default()
        };
        let alice_sa = StoredCredentials {
            access_key: "alice-sa".to_string(),
            parent_user: "alice".to_string(),
            ..Default::default()
        };

        assert!(
            !update_service_account_parent_within_scope(root, &root_derived_sa, root, false),
            "non-owner root-derived credential must not update a root-parented service account"
        );
        assert!(
            update_service_account_parent_within_scope(root, &root_derived_sa, root, true),
            "owner retains updates on root-parented service accounts"
        );
        assert!(
            update_service_account_parent_within_scope(root, &alice_sa, "alice", false),
            "non-root parent match remains in scope"
        );
        assert!(
            !update_service_account_parent_within_scope(root, &alice_sa, "bob", false),
            "cross-parent update remains denied for non-owner"
        );
    }

    #[test]
    fn session_bounded_caller_cannot_escape_through_update() {
        let update = |new_policy: Option<serde_json::Value>,
                      new_secret_key: Option<&str>,
                      new_status: Option<&str>,
                      new_name: Option<&str>,
                      new_expiration: Option<OffsetDateTime>| UpdateServiceAccountReq {
            new_policy,
            new_secret_key: new_secret_key.map(str::to_string),
            new_status: new_status.map(str::to_string),
            new_name: new_name.map(str::to_string),
            new_description: None,
            new_expiration,
        };
        let deadline = OffsetDateTime::UNIX_EPOCH + time::Duration::days(30);
        let new_policy = update(Some(json!({})), None, None, None, None);
        let new_secret = update(None, Some("rotatedSecretKey123"), None, None, None);
        let new_status = update(None, None, Some("off"), None, None);
        let new_name = update(None, None, None, Some("renamed"), None);
        let new_expiration = update(None, None, None, None, Some(deadline));

        let bounded_callers = [
            StoredCredentials {
                access_key: "bounded-sa".to_string(),
                parent_user: "owner-user".to_string(),
                claims: Some(HashMap::from([(
                    rustfs_iam::sys::SESSION_POLICY_NAME.to_string(),
                    json!("eyJWZXJzaW9uIjoiMjAxMi0xMC0xNyJ9"),
                )])),
                ..Default::default()
            },
            StoredCredentials {
                access_key: "bounded-sa".to_string(),
                parent_user: "owner-user".to_string(),
                claims: Some(HashMap::from([(
                    rustfs_iam::sys::SESSION_POLICY_NAME_EXTRACTED.to_string(),
                    json!(r#"{"Version":"2012-10-17"}"#),
                )])),
                ..Default::default()
            },
            StoredCredentials {
                access_key: "bounded-sa".to_string(),
                parent_user: "openid=parent".to_string(),
                claims: Some(HashMap::from([(
                    "x-rustfs-internal-federated-boundary".to_string(),
                    json!("eyJWZXJzaW9uIjoiMjAxMi0xMC0xNyJ9"),
                )])),
                ..Default::default()
            },
        ];

        for bounded in &bounded_callers {
            // Self-modifications that widen/alter policy, expiration, or status are denied.
            assert!(session_bounded_caller_escapes_through_update(bounded, "bounded-sa", &new_policy));
            assert!(session_bounded_caller_escapes_through_update(bounded, "bounded-sa", &new_expiration));
            assert!(session_bounded_caller_escapes_through_update(bounded, "bounded-sa", &new_status));
            // Rotating own secret key and updating own name remain permitted.
            assert!(!session_bounded_caller_escapes_through_update(bounded, "bounded-sa", &new_secret));
            assert!(!session_bounded_caller_escapes_through_update(bounded, "bounded-sa", &new_name));

            // Any mutation targeting a sibling service account is denied.
            for req in [&new_policy, &new_secret, &new_status, &new_name, &new_expiration] {
                assert!(session_bounded_caller_escapes_through_update(bounded, "sibling-sa", req));
            }
        }

        let unbounded = StoredCredentials {
            access_key: "plain-sa".to_string(),
            parent_user: "owner-user".to_string(),
            ..Default::default()
        };
        for req in [&new_policy, &new_secret, &new_status, &new_name, &new_expiration] {
            assert!(!session_bounded_caller_escapes_through_update(&unbounded, "sibling-sa", req));
        }
    }

    #[test]
    fn fallback_access_key_info_allows_same_regular_user() {
        let requester = StoredCredentials {
            access_key: "owner-user".to_string(),
            ..Default::default()
        };
        let target = StoredCredentials {
            access_key: "owner-user".to_string(),
            ..Default::default()
        };

        assert!(can_fallback_view_access_key_info(&requester, &target));
    }

    #[test]
    fn fallback_access_key_info_allows_parent_for_derived_credentials() {
        let requester = StoredCredentials {
            access_key: "sts-user".to_string(),
            session_token: "session-token".to_string(),
            parent_user: "owner-user".to_string(),
            ..Default::default()
        };
        let target = StoredCredentials {
            access_key: "owner-user".to_string(),
            ..Default::default()
        };

        assert!(can_fallback_view_access_key_info(&requester, &target));
    }

    #[test]
    fn fallback_access_key_info_denies_service_account_for_parent_regular_user() {
        let requester = StoredCredentials {
            access_key: "svc-user".to_string(),
            session_token: "session-token".to_string(),
            parent_user: "owner-user".to_string(),
            claims: Some(HashMap::from([("sa-policy".to_string(), json!("inherited-policy"))])),
            ..Default::default()
        };
        let target = StoredCredentials {
            access_key: "owner-user".to_string(),
            ..Default::default()
        };

        assert!(!can_fallback_view_access_key_info(&requester, &target));
    }

    #[test]
    fn fallback_access_key_info_denies_session_policy_restricted_sts() {
        let target = StoredCredentials {
            access_key: "owner-user".to_string(),
            ..Default::default()
        };
        for claim_key in [
            rustfs_iam::sys::SESSION_POLICY_NAME,
            rustfs_iam::sys::SESSION_POLICY_NAME_EXTRACTED,
        ] {
            let requester = StoredCredentials {
                access_key: "sts-user".to_string(),
                session_token: "session-token".to_string(),
                parent_user: "owner-user".to_string(),
                claims: Some(HashMap::from([(claim_key.to_string(), json!("e30"))])),
                ..Default::default()
            };
            assert!(
                !can_fallback_view_access_key_info(&requester, &target),
                "restricted STS ({claim_key}) must not fall back to viewing parent access key info"
            );
        }
    }

    #[test]
    fn fallback_access_key_info_denies_other_regular_user() {
        let requester = StoredCredentials {
            access_key: "alice".to_string(),
            ..Default::default()
        };
        let target = StoredCredentials {
            access_key: "bob".to_string(),
            ..Default::default()
        };

        assert!(!can_fallback_view_access_key_info(&requester, &target));
    }

    #[test]
    fn fallback_access_key_info_requires_no_explicit_deny() {
        let requester = StoredCredentials {
            access_key: "owner-user".to_string(),
            ..Default::default()
        };
        let target = StoredCredentials {
            access_key: "owner-user".to_string(),
            ..Default::default()
        };

        assert!(can_fallback_view_access_key_info_after_admin_check_failed(&requester, &target, true));
        assert!(!can_fallback_view_access_key_info_after_admin_check_failed(&requester, &target, false));
    }

    #[test]
    fn fallback_access_key_info_requires_no_explicit_deny_for_parent_owned_derived_credentials() {
        let requester = StoredCredentials {
            access_key: "sts-user".to_string(),
            session_token: "session-token".to_string(),
            parent_user: "owner-user".to_string(),
            ..Default::default()
        };
        let target = StoredCredentials {
            access_key: "owner-user".to_string(),
            ..Default::default()
        };

        assert!(can_fallback_view_access_key_info_after_admin_check_failed(&requester, &target, true));
        assert!(!can_fallback_view_access_key_info_after_admin_check_failed(&requester, &target, false));
    }

    #[test]
    fn build_info_regular_user_resp_maps_user_metadata() {
        let account = StoredCredentials {
            access_key: "owner-user".to_string(),
            name: Some("Owner".to_string()),
            description: Some("Primary user".to_string()),
            ..Default::default()
        };
        let user_info = rustfs_madmin::UserInfo {
            policy_name: Some("readwrite".to_string()),
            status: rustfs_madmin::AccountStatus::Enabled,
            ..Default::default()
        };

        let resp = access_key_identity::build_info_regular_user_resp(&account, &user_info);

        assert_eq!(resp.parent_user, "");
        assert_eq!(resp.account_status, "enabled");
        assert!(!resp.implied_policy);
        assert_eq!(resp.policy.as_deref(), Some("readwrite"));
        assert_eq!(resp.name.as_deref(), Some("Owner"));
        assert_eq!(resp.description.as_deref(), Some("Primary user"));
        assert_eq!(resp.expiration, None);
    }

    #[test]
    #[serial_test::serial]
    fn restricted_service_account_cannot_delete_inspect_or_list_sibling_accounts_issue_9514() {
        crate::app::gating_test_env::run_large_stack_test("service-account-sibling-authz-9514", || async {
            use crate::admin::operation::test_support::TestRequest;
            use crate::admin::runtime_sources::{AppContext, IamInterface, KmsInterface, publish_test_app_context};
            use rustfs_iam::store::UserType;
            use rustfs_iam::store::object::ObjectStore;
            use rustfs_iam::sys::{PLUGIN_ATTEMPT_CREDENTIAL_CLAIM, SESSION_POLICY_NAME};
            use rustfs_policy::auth::get_new_credentials_with_metadata;
            use std::sync::Arc;

            struct ReadyIam(Arc<rustfs_iam::sys::IamSys<ObjectStore>>);
            impl IamInterface for ReadyIam {
                fn handle(&self) -> Arc<rustfs_iam::sys::IamSys<ObjectStore>> {
                    self.0.clone()
                }
                fn is_ready(&self) -> bool {
                    true
                }
            }

            struct TestKms;
            impl KmsInterface for TestKms {
                fn handle(&self) -> Arc<rustfs_kms::KmsServiceManager> {
                    Arc::new(rustfs_kms::KmsServiceManager::new())
                }
            }

            let root_cred = rustfs_credentials::get_global_action_cred().unwrap_or_else(|| {
                let _ = rustfs_credentials::init_global_action_credentials(
                    Some("root-9514".to_owned()),
                    Some("root-secret-9514".to_owned()),
                );
                rustfs_credentials::get_global_action_cred().expect("global action credentials initialized")
            });

            let (_isolated_dir, canonical) = if let Some(existing) = crate::runtime_sources::current_app_context()
                && existing.iam().is_ready()
            {
                if existing.action_credentials().get().is_none() {
                    let _ = existing.publish_action_credentials(root_cred.clone());
                }
                (None, existing)
            } else {
                let (directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
                ObjectStore::new(store.clone())
                    .save_iam_config(
                        json!({"version": 1}),
                        format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
                    )
                    .await
                    .expect("initialize IAM storage");
                let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("initialized IAM");
                let context = Arc::new(AppContext::new(store, Arc::new(ReadyIam(iam)), Arc::new(TestKms)));
                let _ = context.publish_action_credentials(root_cred.clone());
                let canonical = publish_test_app_context(context);
                if canonical.action_credentials().get().is_none() {
                    let _ = canonical.publish_action_credentials(root_cred.clone());
                }
                (Some(directory), canonical)
            };
            let iam = canonical.iam().handle();
            let active_root = canonical
                .action_credentials()
                .get()
                .expect("canonical AppContext must expose action credentials");
            assert!(
                Arc::ptr_eq(&iam, &crate::admin::runtime_sources::current_ready_iam_handle().expect("ready IAM")),
                "test setup and handlers must share the exact same IAM instance"
            );

            let s3_rw_policy = Policy::parse_config(
                br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]}]}"#,
            )
            .expect("s3 rw policy");
            let deny_admin_policy = Policy::parse_config(
                br#"{"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]},
                    {"Effect":"Deny","Action":["admin:*"],"Resource":["arn:aws:s3:::*"]}
                ]}"#,
            )
            .expect("deny admin policy");
            let svc_admin_policy = Policy::parse_config(
                br#"{"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Action":["admin:ListServiceAccounts","admin:RemoveServiceAccount"],"Resource":["arn:aws:s3:::*"]}
                ]}"#,
            )
            .expect("service account admin policy");

            iam.set_policy("s3-rw-9514", s3_rw_policy).await.expect("set s3-rw policy");
            iam.set_policy("deny-admin-9514", deny_admin_policy)
                .await
                .expect("set deny-admin policy");
            iam.set_policy("svc-admin-9514", svc_admin_policy.clone())
                .await
                .expect("set svc-admin policy");

            let secret = "secret-key-9514";
            iam.create_user(
                "nopolicy-9514",
                &rustfs_madmin::AddOrUpdateUserReq {
                    secret_key: secret.to_owned(),
                    policy: None,
                    status: rustfs_madmin::AccountStatus::Enabled,
                },
            )
            .await
            .expect("create policyless test user");
            for (user, policy_name) in [
                ("alice-9514", "s3-rw-9514"),
                ("carol-9514", "deny-admin-9514"),
                ("admin-9514", "svc-admin-9514"),
            ] {
                iam.create_user(
                    user,
                    &rustfs_madmin::AddOrUpdateUserReq {
                        secret_key: secret.to_owned(),
                        policy: None,
                        status: rustfs_madmin::AccountStatus::Enabled,
                    },
                )
                .await
                .expect("create test user");
                iam.policy_db_set(user, UserType::Reg, false, policy_name)
                    .await
                    .expect("attach test policy");
            }

            let restricted_deny_policy = Policy::parse_config(
                br#"{"Version":"2012-10-17","Statement":[
                    {"Sid":"ReadOneSharedFile","Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::test-bucket/share-test/hello.txt"]},
                    {"Sid":"DenyCredentialAndAdminManagement","Effect":"Deny","Action":["admin:*"],"Resource":["arn:aws:s3:::*"]}
                ]}"#,
            )
            .expect("restricted deny policy");
            let restricted_s3_only_policy = Policy::parse_config(
                br#"{"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::test-bucket/*"]}
                ]}"#,
            )
            .expect("restricted s3-only policy");
            let s3_only_policy_b64 = base64_simd::URL_SAFE_NO_PAD
                .encode_to_string(serde_json::to_vec(&restricted_s3_only_policy).expect("serialize s3-only policy"));

            let (nopolicy_sa, _) = iam
                .new_service_account(
                    "nopolicy-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "nopolicy-sa-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        ..Default::default()
                    },
                )
                .await
                .expect("create policyless user service account");
            let (sa_denied, _) = iam
                .new_service_account(
                    "alice-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "sa-denied-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        session_policy: Some(restricted_deny_policy.clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("create restricted service account with explicit admin deny");
            let (sa_s3_only, _) = iam
                .new_service_account(
                    "alice-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "sa-s3only-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        session_policy: Some(restricted_s3_only_policy.clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("create s3-only service account");
            let (sa_sibling, _) = iam
                .new_service_account(
                    "alice-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "sa-sibling-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        ..Default::default()
                    },
                )
                .await
                .expect("create sibling service account");
            let (carol_sa, _) = iam
                .new_service_account(
                    "carol-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "carol-sa-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        ..Default::default()
                    },
                )
                .await
                .expect("create carol service account");
            let (admin_sa_denied, _) = iam
                .new_service_account(
                    "admin-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "admin-sa-denied-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        session_policy: Some(restricted_deny_policy),
                        ..Default::default()
                    },
                )
                .await
                .expect("create admin-parented restricted service account with explicit admin deny");
            let (admin_sa_s3_only, _) = iam
                .new_service_account(
                    "admin-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "admin-sa-s3only-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        session_policy: Some(restricted_s3_only_policy),
                        ..Default::default()
                    },
                )
                .await
                .expect("create admin-parented s3-only service account");
            let (admin_sa, _) = iam
                .new_service_account(
                    "admin-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "admin-sa-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        session_policy: Some(svc_admin_policy),
                        ..Default::default()
                    },
                )
                .await
                .expect("create authorized admin service account");
            let (admin_target, _) = iam
                .new_service_account(
                    "admin-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "admin-target-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        ..Default::default()
                    },
                )
                .await
                .expect("create admin sibling target");

            let (alice_console_target, _) = iam
                .new_service_account(
                    "alice-9514",
                    None,
                    NewServiceAccountOpts {
                        access_key: "sa-console-target-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        ..Default::default()
                    },
                )
                .await
                .expect("create console STS delete target");
            let (root_sa, _) = iam
                .new_service_account(
                    &active_root.access_key,
                    None,
                    NewServiceAccountOpts {
                        access_key: "root-sa-9514".to_owned(),
                        secret_key: secret.to_owned(),
                        ..Default::default()
                    },
                )
                .await
                .expect("create root-owned service account");

            let alice_cred = iam.get_user("alice-9514").await.expect("alice user exists").credentials;
            let exp_ts = (OffsetDateTime::now_utc() + time::Duration::hours(1)).unix_timestamp();
            let mut sts_claims = HashMap::from([
                ("parent".to_owned(), serde_json::Value::String("alice-9514".to_owned())),
                ("exp".to_owned(), json!(exp_ts)),
                (SESSION_POLICY_NAME.to_owned(), serde_json::Value::String(s3_only_policy_b64.clone())),
                (PLUGIN_ATTEMPT_CREDENTIAL_CLAIM.to_owned(), json!(true)),
            ]);
            let mut plugin_sts =
                get_new_credentials_with_metadata(&sts_claims, &active_root.secret_key).expect("mint plugin-attempt sts");
            plugin_sts.parent_user = "alice-9514".to_owned();
            iam.set_temp_user(&plugin_sts.access_key, &plugin_sts, None)
                .await
                .expect("persist plugin-attempt sts");

            sts_claims.remove(PLUGIN_ATTEMPT_CREDENTIAL_CLAIM);
            let mut restricted_sts =
                get_new_credentials_with_metadata(&sts_claims, &active_root.secret_key).expect("mint restricted sts");
            restricted_sts.parent_user = "alice-9514".to_owned();
            iam.set_temp_user_from_parent(&restricted_sts, &alice_cred)
                .await
                .expect("persist restricted sts");

            let root_sts_claims = HashMap::from([
                ("parent".to_owned(), serde_json::Value::String(active_root.access_key.clone())),
                ("exp".to_owned(), json!(exp_ts)),
                (SESSION_POLICY_NAME.to_owned(), serde_json::Value::String(s3_only_policy_b64)),
            ]);
            let mut root_restricted_sts = get_new_credentials_with_metadata(&root_sts_claims, &active_root.secret_key)
                .expect("mint root-derived restricted sts");
            root_restricted_sts.parent_user = active_root.access_key.clone();
            iam.set_temp_user(&root_restricted_sts.access_key, &root_restricted_sts, None)
                .await
                .expect("persist root-derived restricted sts");

            let console_sts_claims = HashMap::from([
                ("parent".to_owned(), serde_json::Value::String("alice-9514".to_owned())),
                ("exp".to_owned(), json!(exp_ts)),
            ]);
            let mut console_sts = get_new_credentials_with_metadata(&console_sts_claims, &active_root.secret_key)
                .expect("mint unrestricted console sts");
            console_sts.parent_user = "alice-9514".to_owned();
            iam.set_temp_user_from_parent(&console_sts, &alice_cred)
                .await
                .expect("persist unrestricted console sts");

            // Restricted service accounts (both explicit Deny admin:* and least-privilege S3-only,
            // under either a regular parent or an admin parent) must be denied from deleting,
            // inspecting, or listing sibling/own service accounts.
            for (caller, parent, sibling) in [
                (&sa_denied.access_key, "alice-9514", sa_sibling.access_key.as_str()),
                (&sa_s3_only.access_key, "alice-9514", sa_sibling.access_key.as_str()),
                (&admin_sa_denied.access_key, "admin-9514", admin_target.access_key.as_str()),
                (&admin_sa_s3_only.access_key, "admin-9514", admin_target.access_key.as_str()),
            ] {
                for target in [sibling, caller.as_str(), "missing-sa-9514"] {
                    let req = TestRequest::new(
                        http::Method::DELETE,
                        format!("/rustfs/admin/v3/delete-service-account?accessKey={target}").as_str(),
                    )
                    .credentials(caller, secret, None)
                    .build();
                    let err = DeleteServiceAccount {}
                        .call(req, Params::new())
                        .await
                        .expect_err("restricted service account delete must fail");
                    assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{caller} delete {target}");
                }
                assert!(
                    iam.get_service_account(sibling).await.is_ok(),
                    "sibling service account {sibling} must still exist after rejected delete by {caller}"
                );

                for target in [sibling, "missing-sa-9514"] {
                    let req = TestRequest::new(
                        http::Method::GET,
                        format!("/rustfs/admin/v3/info-service-account?accessKey={target}").as_str(),
                    )
                    .credentials(caller, secret, None)
                    .build();
                    let err = InfoServiceAccount {}
                        .call(req, Params::new())
                        .await
                        .expect_err("restricted service account info must fail");
                    assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{caller} info {target}");
                }

                for uri in [
                    "/rustfs/admin/v3/list-service-accounts".to_owned(),
                    format!("/rustfs/admin/v3/list-service-accounts?user={caller}"),
                    format!("/rustfs/admin/v3/list-service-accounts?user={parent}"),
                ] {
                    let req = TestRequest::new(http::Method::GET, uri.as_str())
                        .credentials(caller, secret, None)
                        .build();
                    let err = ListServiceAccount {}
                        .call(req, Params::new())
                        .await
                        .expect_err("restricted service account list must fail");
                    assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{caller} {uri}");
                }

                for uri in [
                    "/rustfs/admin/v3/list-access-keys-bulk".to_owned(),
                    format!("/rustfs/admin/v3/list-access-keys-bulk?users={caller}"),
                    format!("/rustfs/admin/v3/list-access-keys-bulk?users={parent}"),
                ] {
                    let req = TestRequest::new(http::Method::GET, uri.as_str())
                        .credentials(caller, secret, None)
                        .build();
                    let err = ListAccessKeysBulk {}
                        .call(req, Params::new())
                        .await
                        .expect_err("restricted service account bulk list must fail");
                    assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{caller} {uri}");
                }
            }

            // A regular parent user with explicit Deny admin:* must also be blocked from owner fallback.
            let carol_delete = TestRequest::new(
                http::Method::DELETE,
                format!("/rustfs/admin/v3/delete-service-account?accessKey={}", carol_sa.access_key).as_str(),
            )
            .credentials("carol-9514", secret, None)
            .build();
            assert_eq!(
                DeleteServiceAccount {}
                    .call(carol_delete, Params::new())
                    .await
                    .expect_err("explicit admin deny must block owner delete")
                    .code(),
                &S3ErrorCode::AccessDenied
            );
            assert!(
                iam.get_service_account(&carol_sa.access_key).await.is_ok(),
                "carol service account must remain after denied delete"
            );
            let carol_info = TestRequest::new(
                http::Method::GET,
                format!("/rustfs/admin/v3/info-service-account?accessKey={}", carol_sa.access_key).as_str(),
            )
            .credentials("carol-9514", secret, None)
            .build();
            assert_eq!(
                InfoServiceAccount {}
                    .call(carol_info, Params::new())
                    .await
                    .expect_err("explicit admin deny must block owner info")
                    .code(),
                &S3ErrorCode::AccessDenied
            );
            let carol_list = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-service-accounts")
                .credentials("carol-9514", secret, None)
                .build();
            assert_eq!(
                ListServiceAccount {}
                    .call(carol_list, Params::new())
                    .await
                    .expect_err("explicit admin deny must block owner list")
                    .code(),
                &S3ErrorCode::AccessDenied
            );
            let carol_bulk = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-access-keys-bulk")
                .credentials("carol-9514", secret, None)
                .build();
            assert_eq!(
                ListAccessKeysBulk {}
                    .call(carol_bulk, Params::new())
                    .await
                    .expect_err("explicit admin deny must block owner bulk list")
                    .code(),
                &S3ErrorCode::AccessDenied
            );

            // Plugin-attempt STS sessions are confined to their S3 session policy even
            // when their parent user (alice-9514) has no explicit admin Deny.
            for target in [sa_sibling.access_key.as_str(), "missing-sa-9514"] {
                let delete_req = TestRequest::new(
                    http::Method::DELETE,
                    format!("/rustfs/admin/v3/delete-service-account?accessKey={target}").as_str(),
                )
                .header("x-amz-security-token", &plugin_sts.session_token)
                .credentials(&plugin_sts.access_key, &plugin_sts.secret_key, Some(&plugin_sts.session_token))
                .build();
                assert_eq!(
                    DeleteServiceAccount {}
                        .call(delete_req, Params::new())
                        .await
                        .expect_err("plugin-attempt STS delete must fail")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "plugin-attempt STS delete {target}"
                );

                let info_req = TestRequest::new(
                    http::Method::GET,
                    format!("/rustfs/admin/v3/info-service-account?accessKey={target}").as_str(),
                )
                .header("x-amz-security-token", &plugin_sts.session_token)
                .credentials(&plugin_sts.access_key, &plugin_sts.secret_key, Some(&plugin_sts.session_token))
                .build();
                assert_eq!(
                    InfoServiceAccount {}
                        .call(info_req, Params::new())
                        .await
                        .expect_err("plugin-attempt STS info must fail")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "plugin-attempt STS info {target}"
                );
            }
            assert!(
                iam.get_service_account(&sa_sibling.access_key).await.is_ok(),
                "sibling service account must remain after rejected plugin-attempt STS delete"
            );

            let plugin_list_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-service-accounts")
                .header("x-amz-security-token", &plugin_sts.session_token)
                .credentials(&plugin_sts.access_key, &plugin_sts.secret_key, Some(&plugin_sts.session_token))
                .build();
            assert_eq!(
                ListServiceAccount {}
                    .call(plugin_list_req, Params::new())
                    .await
                    .expect_err("plugin-attempt STS list must fail")
                    .code(),
                &S3ErrorCode::AccessDenied
            );

            let plugin_bulk_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-access-keys-bulk")
                .header("x-amz-security-token", &plugin_sts.session_token)
                .credentials(&plugin_sts.access_key, &plugin_sts.secret_key, Some(&plugin_sts.session_token))
                .build();
            assert_eq!(
                ListAccessKeysBulk {}
                    .call(plugin_bulk_req, Params::new())
                    .await
                    .expect_err("plugin-attempt STS bulk list must fail")
                    .code(),
                &S3ErrorCode::AccessDenied
            );

            // Allowlist-restricted STS sessions (carrying sessionPolicy with no explicit admin Deny,
            // whether derived from a regular IAM user or from root) must not use owner self-service
            // fallback to list, inspect, or delete their parent's service accounts.
            for (label, sts_caller, target_sa) in [
                ("alice-restricted-sts", &restricted_sts, sa_sibling.access_key.as_str()),
                ("root-restricted-sts", &root_restricted_sts, root_sa.access_key.as_str()),
            ] {
                for target in [target_sa, "missing-sa-9514"] {
                    let delete_req = TestRequest::new(
                        http::Method::DELETE,
                        format!("/rustfs/admin/v3/delete-service-account?accessKey={target}").as_str(),
                    )
                    .header("x-amz-security-token", &sts_caller.session_token)
                    .credentials(&sts_caller.access_key, &sts_caller.secret_key, Some(&sts_caller.session_token))
                    .build();
                    assert_eq!(
                        DeleteServiceAccount {}
                            .call(delete_req, Params::new())
                            .await
                            .expect_err("restricted STS delete must fail")
                            .code(),
                        &S3ErrorCode::AccessDenied,
                        "{label} delete {target}"
                    );

                    let info_req = TestRequest::new(
                        http::Method::GET,
                        format!("/rustfs/admin/v3/info-service-account?accessKey={target}").as_str(),
                    )
                    .header("x-amz-security-token", &sts_caller.session_token)
                    .credentials(&sts_caller.access_key, &sts_caller.secret_key, Some(&sts_caller.session_token))
                    .build();
                    assert_eq!(
                        InfoServiceAccount {}
                            .call(info_req, Params::new())
                            .await
                            .expect_err("restricted STS info must fail")
                            .code(),
                        &S3ErrorCode::AccessDenied,
                        "{label} info {target}"
                    );
                }
                assert!(
                    iam.get_service_account(target_sa).await.is_ok(),
                    "target service account {target_sa} must remain after rejected delete by {label}"
                );

                let access_key_info_req = TestRequest::new(
                    http::Method::GET,
                    format!("/rustfs/admin/v3/info-access-key?accessKey={target_sa}").as_str(),
                )
                .header("x-amz-security-token", &sts_caller.session_token)
                .credentials(&sts_caller.access_key, &sts_caller.secret_key, Some(&sts_caller.session_token))
                .build();
                assert_eq!(
                    InfoAccessKey {}
                        .call(access_key_info_req, Params::new())
                        .await
                        .expect_err("restricted STS info-access-key must fail")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "{label} info-access-key {target_sa}"
                );

                let list_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-service-accounts")
                    .header("x-amz-security-token", &sts_caller.session_token)
                    .credentials(&sts_caller.access_key, &sts_caller.secret_key, Some(&sts_caller.session_token))
                    .build();
                assert_eq!(
                    ListServiceAccount {}
                        .call(list_req, Params::new())
                        .await
                        .expect_err("restricted STS list must fail")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "{label} list"
                );

                let bulk_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-access-keys-bulk")
                    .header("x-amz-security-token", &sts_caller.session_token)
                    .credentials(&sts_caller.access_key, &sts_caller.secret_key, Some(&sts_caller.session_token))
                    .build();
                assert_eq!(
                    ListAccessKeysBulk {}
                        .call(bulk_req, Params::new())
                        .await
                        .expect_err("restricted STS bulk list must fail")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "{label} bulk list"
                );
            }

            // An unrestricted Console STS session (no sessionPolicy claim) retains owner self-service
            // fallback when neither parent nor session carries an explicit admin Deny.
            let console_list_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-service-accounts")
                .header("x-amz-security-token", &console_sts.session_token)
                .credentials(&console_sts.access_key, &console_sts.secret_key, Some(&console_sts.session_token))
                .build();
            assert_eq!(
                ListServiceAccount {}
                    .call(console_list_req, Params::new())
                    .await
                    .expect("unrestricted console STS list should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let console_info_req = TestRequest::new(
                http::Method::GET,
                format!("/rustfs/admin/v3/info-service-account?accessKey={}", sa_sibling.access_key).as_str(),
            )
            .header("x-amz-security-token", &console_sts.session_token)
            .credentials(&console_sts.access_key, &console_sts.secret_key, Some(&console_sts.session_token))
            .build();
            assert_eq!(
                InfoServiceAccount {}
                    .call(console_info_req, Params::new())
                    .await
                    .expect("unrestricted console STS info should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let console_ak_info_req = TestRequest::new(
                http::Method::GET,
                format!("/rustfs/admin/v3/info-access-key?accessKey={}", sa_sibling.access_key).as_str(),
            )
            .header("x-amz-security-token", &console_sts.session_token)
            .credentials(&console_sts.access_key, &console_sts.secret_key, Some(&console_sts.session_token))
            .build();
            assert_eq!(
                InfoAccessKey {}
                    .call(console_ak_info_req, Params::new())
                    .await
                    .expect("unrestricted console STS info-access-key should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let console_bulk_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-access-keys-bulk")
                .header("x-amz-security-token", &console_sts.session_token)
                .credentials(&console_sts.access_key, &console_sts.secret_key, Some(&console_sts.session_token))
                .build();
            assert_eq!(
                ListAccessKeysBulk {}
                    .call(console_bulk_req, Params::new())
                    .await
                    .expect("unrestricted console STS bulk list should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let console_delete_req = TestRequest::new(
                http::Method::DELETE,
                format!("/rustfs/admin/v3/delete-service-account?accessKey={}", alice_console_target.access_key).as_str(),
            )
            .header("x-amz-security-token", &console_sts.session_token)
            .credentials(&console_sts.access_key, &console_sts.secret_key, Some(&console_sts.session_token))
            .build();
            assert_eq!(
                DeleteServiceAccount {}
                    .call(console_delete_req, Params::new())
                    .await
                    .expect("unrestricted console STS delete should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            assert!(
                iam.get_service_account(&alice_console_target.access_key)
                    .await
                    .is_err_and(|err| is_err_no_such_service_account(&err)),
                "unrestricted console STS delete must remove owned service account"
            );

            // An active regular owner with NO attached policies retains self-service over own accounts
            // while remaining denied from cross-user operations.
            let nopolicy_list = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-service-accounts")
                .credentials("nopolicy-9514", secret, None)
                .build();
            assert_eq!(
                ListServiceAccount {}
                    .call(nopolicy_list, Params::new())
                    .await
                    .expect("policyless owner list should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let nopolicy_info = TestRequest::new(
                http::Method::GET,
                format!("/rustfs/admin/v3/info-service-account?accessKey={}", nopolicy_sa.access_key).as_str(),
            )
            .credentials("nopolicy-9514", secret, None)
            .build();
            assert_eq!(
                InfoServiceAccount {}
                    .call(nopolicy_info, Params::new())
                    .await
                    .expect("policyless owner info should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let nopolicy_bulk = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-access-keys-bulk")
                .credentials("nopolicy-9514", secret, None)
                .build();
            assert_eq!(
                ListAccessKeysBulk {}
                    .call(nopolicy_bulk, Params::new())
                    .await
                    .expect("policyless owner bulk list should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let nopolicy_cross_delete = TestRequest::new(
                http::Method::DELETE,
                format!("/rustfs/admin/v3/delete-service-account?accessKey={}", sa_sibling.access_key).as_str(),
            )
            .credentials("nopolicy-9514", secret, None)
            .build();
            assert_eq!(
                DeleteServiceAccount {}
                    .call(nopolicy_cross_delete, Params::new())
                    .await
                    .expect_err("policyless owner must not delete another user's service account")
                    .code(),
                &S3ErrorCode::AccessDenied
            );
            assert!(
                iam.get_service_account(&sa_sibling.access_key).await.is_ok(),
                "alice sibling service account must remain after rejected cross-user delete by nopolicy-9514"
            );
            let nopolicy_cross_info = TestRequest::new(
                http::Method::GET,
                format!("/rustfs/admin/v3/info-service-account?accessKey={}", sa_sibling.access_key).as_str(),
            )
            .credentials("nopolicy-9514", secret, None)
            .build();
            assert_eq!(
                InfoServiceAccount {}
                    .call(nopolicy_cross_info, Params::new())
                    .await
                    .expect_err("policyless owner must not inspect another user's service account")
                    .code(),
                &S3ErrorCode::AccessDenied
            );
            let nopolicy_cross_list =
                TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-service-accounts?user=alice-9514")
                    .credentials("nopolicy-9514", secret, None)
                    .build();
            assert_eq!(
                ListServiceAccount {}
                    .call(nopolicy_cross_list, Params::new())
                    .await
                    .expect_err("policyless owner must not list another user's service accounts")
                    .code(),
                &S3ErrorCode::AccessDenied
            );
            let nopolicy_delete = TestRequest::new(
                http::Method::DELETE,
                format!("/rustfs/admin/v3/delete-service-account?accessKey={}", nopolicy_sa.access_key).as_str(),
            )
            .credentials("nopolicy-9514", secret, None)
            .build();
            assert_eq!(
                DeleteServiceAccount {}
                    .call(nopolicy_delete, Params::new())
                    .await
                    .expect("policyless owner delete should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            assert!(
                iam.get_service_account(&nopolicy_sa.access_key)
                    .await
                    .is_err_and(|err| is_err_no_such_service_account(&err)),
                "policyless owner delete must remove its own service account"
            );

            // Regular parent user without explicit admin Deny retains self-service access to own accounts.
            let alice_list = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-service-accounts")
                .credentials("alice-9514", secret, None)
                .build();
            assert_eq!(
                ListServiceAccount {}
                    .call(alice_list, Params::new())
                    .await
                    .expect("owner list should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let alice_info = TestRequest::new(
                http::Method::GET,
                format!("/rustfs/admin/v3/info-service-account?accessKey={}", sa_sibling.access_key).as_str(),
            )
            .credentials("alice-9514", secret, None)
            .build();
            assert_eq!(
                InfoServiceAccount {}
                    .call(alice_info, Params::new())
                    .await
                    .expect("owner info should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let alice_bulk = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-access-keys-bulk")
                .credentials("alice-9514", secret, None)
                .build();
            assert_eq!(
                ListAccessKeysBulk {}
                    .call(alice_bulk, Params::new())
                    .await
                    .expect("owner bulk list should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let alice_delete = TestRequest::new(
                http::Method::DELETE,
                format!("/rustfs/admin/v3/delete-service-account?accessKey={}", sa_sibling.access_key).as_str(),
            )
            .credentials("alice-9514", secret, None)
            .build();
            assert_eq!(
                DeleteServiceAccount {}
                    .call(alice_delete, Params::new())
                    .await
                    .expect("owner delete should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            assert!(
                iam.get_service_account(&sa_sibling.access_key)
                    .await
                    .is_err_and(|err| is_err_no_such_service_account(&err)),
                "owner delete must remove the target service account"
            );

            // A service account explicitly granted ListServiceAccounts and RemoveServiceAccount
            // continues to work through the primary IAM policy check.
            let admin_sa_list = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-service-accounts")
                .credentials(&admin_sa.access_key, secret, None)
                .build();
            assert_eq!(
                ListServiceAccount {}
                    .call(admin_sa_list, Params::new())
                    .await
                    .expect("authorized service account list should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let admin_sa_info = TestRequest::new(
                http::Method::GET,
                format!("/rustfs/admin/v3/info-service-account?accessKey={}", admin_target.access_key).as_str(),
            )
            .credentials(&admin_sa.access_key, secret, None)
            .build();
            assert_eq!(
                InfoServiceAccount {}
                    .call(admin_sa_info, Params::new())
                    .await
                    .expect("authorized service account info should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let admin_sa_bulk = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-access-keys-bulk")
                .credentials(&admin_sa.access_key, secret, None)
                .build();
            assert_eq!(
                ListAccessKeysBulk {}
                    .call(admin_sa_bulk, Params::new())
                    .await
                    .expect("authorized service account bulk list should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let admin_sa_delete = TestRequest::new(
                http::Method::DELETE,
                format!("/rustfs/admin/v3/delete-service-account?accessKey={}", admin_target.access_key).as_str(),
            )
            .credentials(&admin_sa.access_key, secret, None)
            .build();
            assert_eq!(
                DeleteServiceAccount {}
                    .call(admin_sa_delete, Params::new())
                    .await
                    .expect("authorized service account delete should succeed")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            assert!(
                iam.get_service_account(&admin_target.access_key)
                    .await
                    .is_err_and(|err| is_err_no_such_service_account(&err)),
                "authorized service account delete must remove the target service account"
            );

            // Unrestricted admin RemoveServiceAccount must still clean up corrupt on-disk
            // service-account records, while non-admin owner fallback fails closed with AccessDenied.
            let object_store =
                crate::admin::runtime_sources::current_object_store_handle().expect("canonical object store must be initialized");
            let corrupt_key = "corrupt-sa-9514";
            let corrupt_identity_path = format!(
                "{}{corrupt_key}/identity.json",
                *rustfs_iam::store::object::IAM_CONFIG_SERVICE_ACCOUNTS_PREFIX
            );
            let corrupt_policy_path = format!(
                "{}{corrupt_key}.json",
                *rustfs_iam::store::object::IAM_CONFIG_POLICY_DB_SERVICE_ACCOUNTS_PREFIX
            );
            crate::admin::storage_api::config::save_admin_config(
                object_store.clone(),
                &corrupt_identity_path,
                b"{corrupt-service-account".to_vec(),
            )
            .await
            .expect("seed corrupt service account identity");
            crate::admin::storage_api::config::save_admin_config(
                object_store.clone(),
                &corrupt_policy_path,
                br#"{"version":1,"policy":"readwrite"}"#.to_vec(),
            )
            .await
            .expect("seed corrupt service account mapped policy");

            let non_admin_corrupt_delete = TestRequest::new(
                http::Method::DELETE,
                format!("/rustfs/admin/v3/delete-service-account?accessKey={corrupt_key}").as_str(),
            )
            .credentials("alice-9514", secret, None)
            .build();
            assert_eq!(
                DeleteServiceAccount {}
                    .call(non_admin_corrupt_delete, Params::new())
                    .await
                    .expect_err("non-admin delete of corrupt record must fail closed")
                    .code(),
                &S3ErrorCode::AccessDenied
            );
            assert!(
                iam.get_service_account_revision(corrupt_key).await.is_err(),
                "corrupt service account record must survive rejected non-admin delete"
            );

            let admin_corrupt_delete = TestRequest::new(
                http::Method::DELETE,
                format!("/rustfs/admin/v3/delete-service-account?accessKey={corrupt_key}").as_str(),
            )
            .credentials(&admin_sa.access_key, secret, None)
            .build();
            assert_eq!(
                DeleteServiceAccount {}
                    .call(admin_corrupt_delete, Params::new())
                    .await
                    .expect("unrestricted admin must delete corrupt service account record")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            assert!(
                iam.get_service_account_revision(corrupt_key)
                    .await
                    .expect("check cleaned corrupt record")
                    .is_none(),
                "corrupt service account identity must be removed by unrestricted admin delete"
            );
            assert!(
                crate::admin::storage_api::config::read_admin_config(object_store, &corrupt_policy_path)
                    .await
                    .is_err(),
                "corrupt service account mapped policy must be removed by unrestricted admin delete"
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn bounded_caller_cannot_escape_session_policy_via_create_or_update() {
        crate::app::gating_test_env::run_large_stack_test("service-account-session-policy-escape", || async {
            use crate::admin::operation::test_support::TestRequest;
            use rustfs_credentials::EMBEDDED_POLICY_TYPE;
            use rustfs_iam::store::UserType;
            use rustfs_iam::sys::SESSION_POLICY_NAME;
            use rustfs_policy::auth::get_new_credentials_with_metadata;
            use rustfs_policy::policy::action::S3Action;

            let root_cred = rustfs_credentials::get_global_action_cred().unwrap_or_else(|| {
                let _ = rustfs_credentials::init_global_action_credentials(
                    Some("root-sa-bound".to_owned()),
                    Some("root-secret-sa-bound".to_owned()),
                );
                rustfs_credentials::get_global_action_cred().expect("global action credentials initialized")
            });

            let canonical = crate::app::gating_test_env::shared_gating_ambient().await;
            if canonical.action_credentials().get().is_none() {
                let _ = canonical.publish_action_credentials(root_cred.clone());
            }
            let iam = canonical.iam().handle();
            let active_root = canonical
                .action_credentials()
                .get()
                .expect("canonical AppContext must expose action credentials");

            let suffix = uuid::Uuid::new_v4().simple().to_string();
            let user_name = format!("alice-bound-{suffix}");
            let policy_name = format!("alice-parent-policy-{suffix}");
            let secret = "secret-bound-0123456789";

            let parent_policy = Policy::parse_config(
                br#"{"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::bucket-a/*","arn:aws:s3:::bucket-b/*"]},
                    {"Effect":"Allow","Action":["admin:CreateServiceAccount","admin:UpdateServiceAccount","admin:ListUsers"]}
                ]}"#,
            )
            .expect("parent policy");
            iam.set_policy(&policy_name, parent_policy).await.expect("set parent policy");
            iam.create_user(
                &user_name,
                &rustfs_madmin::AddOrUpdateUserReq {
                    secret_key: secret.to_owned(),
                    policy: None,
                    status: rustfs_madmin::AccountStatus::Enabled,
                },
            )
            .await
            .expect("create user");
            iam.policy_db_set(&user_name, UserType::Reg, false, &policy_name)
                .await
                .expect("attach parent policy");

            let bounded_policy = Policy::parse_config(
                br#"{"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket-a/*"]},
                    {"Effect":"Allow","Action":["admin:CreateServiceAccount","admin:UpdateServiceAccount"]}
                ]}"#,
            )
            .expect("bounded policy");
            let bounded_policy_b64 = base64_simd::URL_SAFE_NO_PAD
                .encode_to_string(serde_json::to_vec(&bounded_policy).expect("serialize bounded policy"));

            let sibling_ak = format!("sib-{suffix}");
            let (sibling_sa, _) = iam
                .new_service_account(
                    &user_name,
                    None,
                    NewServiceAccountOpts {
                        access_key: sibling_ak.clone(),
                        secret_key: secret.to_owned(),
                        ..Default::default()
                    },
                )
                .await
                .expect("create unbounded sibling service account");

            let bounded_sa_ak = format!("bsa-{suffix}");
            let (bounded_sa, _) = iam
                .new_service_account(
                    &user_name,
                    None,
                    NewServiceAccountOpts {
                        access_key: bounded_sa_ak.clone(),
                        secret_key: secret.to_owned(),
                        session_policy: Some(bounded_policy.clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("create bounded service account");

            let root_bounded_sa_ak = format!("rbsa-{suffix}");
            let (root_bounded_sa, _) = iam
                .new_service_account(
                    &active_root.access_key,
                    None,
                    NewServiceAccountOpts {
                        access_key: root_bounded_sa_ak.clone(),
                        secret_key: secret.to_owned(),
                        session_policy: Some(bounded_policy.clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("create root-derived bounded service account");

            let alice_cred = iam.get_user(&user_name).await.expect("alice exists").credentials;
            let session_exp = OffsetDateTime::now_utc() + time::Duration::minutes(15);
            let session_exp_ts = session_exp.unix_timestamp();
            let sts_claims = HashMap::from([
                ("parent".to_owned(), json!(user_name)),
                ("exp".to_owned(), json!(session_exp_ts)),
                (SESSION_POLICY_NAME.to_owned(), json!(bounded_policy_b64)),
            ]);
            let mut bounded_sts =
                get_new_credentials_with_metadata(&sts_claims, &active_root.secret_key).expect("mint bounded sts");
            bounded_sts.parent_user = user_name.clone();
            iam.set_temp_user_from_parent(&bounded_sts, &alice_cred)
                .await
                .expect("persist bounded sts");

            // 1. Bounded STS creating a service account with no policy inherits the session policy and expiration cap.
            let child_ak = format!("child-sts-{suffix}");
            let create_body = serde_json::to_vec(&json!({
                "accessKey": child_ak,
                "secretKey": "child-secret-0123456789"
            }))
            .expect("serialize create body");
            let create_req = TestRequest::new(http::Method::PUT, "/rustfs/admin/v3/add-service-account")
                .header("x-amz-security-token", &bounded_sts.session_token)
                .credentials(&bounded_sts.access_key, &bounded_sts.secret_key, Some(&bounded_sts.session_token))
                .body(create_body)
                .build();
            assert_eq!(
                AddServiceAccount {}
                    .call(create_req, Params::new())
                    .await
                    .expect("bounded STS create without policy should succeed with inherited bounds")
                    .into_parts()
                    .0,
                StatusCode::OK
            );
            let child_claims = iam.get_claims_for_svc_acc(&child_ak).await.expect("child claims");
            assert_eq!(
                child_claims.get(IAM_POLICY_CLAIM_NAME_SA),
                Some(&json!(EMBEDDED_POLICY_TYPE)),
                "created service account must embed creator sessionPolicy"
            );
            let (child_sa, _) = iam.get_service_account(&child_ak).await.expect("child service account");
            assert_eq!(
                child_sa.expiration.map(OffsetDateTime::unix_timestamp),
                Some(session_exp_ts),
                "service account created by bounded STS must be capped at the STS session expiration"
            );
            let empty_conditions = HashMap::new();
            assert!(
                iam.is_allowed(&Args {
                    account: &child_ak,
                    groups: &None,
                    action: Action::S3Action(S3Action::GetObjectAction),
                    bucket: "bucket-a",
                    object: "ok.txt",
                    conditions: &empty_conditions,
                    is_owner: false,
                    claims: &child_claims,
                    deny_only: false,
                })
                .await
            );
            assert!(
                !iam.is_allowed(&Args {
                    account: &child_ak,
                    groups: &None,
                    action: Action::S3Action(S3Action::GetObjectAction),
                    bucket: "bucket-b",
                    object: "secret.txt",
                    conditions: &empty_conditions,
                    is_owner: false,
                    claims: &child_claims,
                    deny_only: false,
                })
                .await,
                "service account created by bounded STS must not access buckets outside session policy"
            );
            assert!(
                !iam.is_allowed(&Args {
                    account: &child_ak,
                    groups: &None,
                    action: Action::AdminAction(AdminAction::ListUsersAdminAction),
                    bucket: "",
                    object: "",
                    conditions: &empty_conditions,
                    is_owner: false,
                    claims: &child_claims,
                    deny_only: false,
                })
                .await,
                "service account created by bounded STS must not gain parent admin actions"
            );

            // 2. Bounded STS attempting to supply a wider policy on create is denied.
            let wider_ak = format!("child-wide-{suffix}");
            let wide_body = serde_json::to_vec(&json!({
                "accessKey": wider_ak,
                "secretKey": "child-secret-0123456789",
                "policy": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"]}]}
            }))
            .expect("serialize wide create body");
            let wide_req = TestRequest::new(http::Method::PUT, "/rustfs/admin/v3/add-service-account")
                .header("x-amz-security-token", &bounded_sts.session_token)
                .credentials(&bounded_sts.access_key, &bounded_sts.secret_key, Some(&bounded_sts.session_token))
                .body(wide_body)
                .build();
            assert_eq!(
                AddServiceAccount {}
                    .call(wide_req, Params::new())
                    .await
                    .expect_err("bounded STS must not replace its session policy on create")
                    .code(),
                &S3ErrorCode::AccessDenied
            );

            // 3. Bounded STS and bounded SA cannot update an unbounded sibling (secret, policy, status, expiration).
            for payload in [
                json!({"newSecretKey": "hijacked-secret-0123456789"}),
                json!({"newPolicy": {}}),
                json!({"newStatus": "off"}),
                json!({"newExpiration": (OffsetDateTime::now_utc() + time::Duration::days(30)).format(&time::format_description::well_known::Rfc3339).expect("rfc3339")}),
            ] {
                let body = serde_json::to_vec(&payload).expect("serialize update payload");
                let sts_update = TestRequest::new(
                    http::Method::POST,
                    format!("/rustfs/admin/v3/update-service-account?accessKey={}", sibling_sa.access_key).as_str(),
                )
                .header("x-amz-security-token", &bounded_sts.session_token)
                .credentials(&bounded_sts.access_key, &bounded_sts.secret_key, Some(&bounded_sts.session_token))
                .body(body.clone())
                .build();
                assert_eq!(
                    UpdateServiceAccount {}
                        .call(sts_update, Params::new())
                        .await
                        .expect_err("bounded STS must not update sibling service account")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "bounded STS update sibling payload {payload}"
                );

                let sa_update = TestRequest::new(
                    http::Method::POST,
                    format!("/rustfs/admin/v3/update-service-account?accessKey={}", sibling_sa.access_key).as_str(),
                )
                .credentials(&bounded_sa.access_key, secret, None)
                .body(body)
                .build();
                assert_eq!(
                    UpdateServiceAccount {}
                        .call(sa_update, Params::new())
                        .await
                        .expect_err("bounded SA must not update sibling service account")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "bounded SA update sibling payload {payload}"
                );
            }

            // 4. Bounded SA cannot rewrite its own policy, expiration, or status, but may rotate its own secret.
            for payload in [
                json!({"newPolicy": {}}),
                json!({"newPolicy": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"]}]}}),
                json!({"newStatus": "off"}),
                json!({"newExpiration": (OffsetDateTime::now_utc() + time::Duration::days(30)).format(&time::format_description::well_known::Rfc3339).expect("rfc3339")}),
            ] {
                let body = serde_json::to_vec(&payload).expect("serialize self update payload");
                let req = TestRequest::new(
                    http::Method::POST,
                    format!("/rustfs/admin/v3/update-service-account?accessKey={}", bounded_sa.access_key).as_str(),
                )
                .credentials(&bounded_sa.access_key, secret, None)
                .body(body)
                .build();
                assert_eq!(
                    UpdateServiceAccount {}
                        .call(req, Params::new())
                        .await
                        .expect_err("bounded SA must not widen/modify own policy, expiration, or status")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "bounded SA self payload {payload}"
                );
            }
            let rotate_self = TestRequest::new(
                http::Method::POST,
                format!("/rustfs/admin/v3/update-service-account?accessKey={}", bounded_sa.access_key).as_str(),
            )
            .credentials(&bounded_sa.access_key, secret, None)
            .body(serde_json::to_vec(&json!({"newSecretKey": "rotated-secret-0123456789"})).expect("serialize rotate"))
            .build();
            assert_eq!(
                UpdateServiceAccount {}
                    .call(rotate_self, Params::new())
                    .await
                    .expect("bounded SA rotating its own secret should remain allowed")
                    .into_parts()
                    .0,
                StatusCode::NO_CONTENT
            );

            // 5. Root-derived non-owner bounded SA is denied even when rotating its own secret or policy.
            for payload in [
                json!({"newPolicy": {}}),
                json!({"newSecretKey": "root-rotated-secret-0123456789"}),
            ] {
                let req = TestRequest::new(
                    http::Method::POST,
                    format!("/rustfs/admin/v3/update-service-account?accessKey={}", root_bounded_sa.access_key).as_str(),
                )
                .credentials(&root_bounded_sa.access_key, secret, None)
                .body(serde_json::to_vec(&payload).expect("serialize root update"))
                .build();
                assert_eq!(
                    UpdateServiceAccount {}
                        .call(req, Params::new())
                        .await
                        .expect_err("non-owner root-derived SA must not update root-parented service account")
                        .code(),
                    &S3ErrorCode::AccessDenied,
                    "root-derived bounded SA self payload {payload}"
                );
            }
        });
    }
}
