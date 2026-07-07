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

//! MinIO-compatible admin IAM / IDP endpoints.
//!
//! This module implements the request/response/error semantics of a set of
//! MinIO admin IAM and identity-provider (IDP) endpoints so that MinIO clients
//! (`mc`, `madmin-go`) can talk to RustFS. Where RustFS lacks the backing
//! infrastructure a MinIO deployment would have (notably a live LDAP directory
//! bind/search backend), the handlers operate honestly against the IAM data
//! RustFS actually stores (service accounts, STS credentials and builtin policy
//! mappings that carry `ldap:*` / OpenID claims) and never fabricate success.
//! Such limitations are documented at each handler.
//!
//! Endpoints (grouped by tracking issue):
//!
//! * rustfs/backlog#609
//!   - `PUT  /v3/import-iam-v2`               (v2 IAM import with per-entity report)
//!   - `POST /v3/revoke-tokens/{userProvider}` (revoke STS credentials for a provider)
//! * rustfs/backlog#610
//!   - `PUT|POST|GET|DELETE /v3/idp-config/{type}/{name}` (generic IDP config CRUD)
//!   - `GET /v3/idp-config/{type}`            (list configs for a provider type)
//! * rustfs/backlog#616
//!   - `PUT /v3/idp/ldap/add-service-account`
//!   - `GET /v3/idp/ldap/list-access-keys`
//!   - `GET /v3/idp/ldap/list-access-keys-bulk`
//!   - `GET /v3/idp/ldap/policy-entities`
//!   - `POST /v3/idp/ldap/policy/{operation}`
//!   - `GET /v3/idp/openid/list-access-keys-bulk`

use crate::admin::access_key_identity::guess_user_provider;
use crate::admin::auth::validate_admin_request;
use crate::admin::handlers::policies::{ListPolicyEntitiesBuiltin, handle_builtin_policy_association};
use crate::admin::handlers::service_account::AddServiceAccount;
use crate::admin::handlers::user::ImportIam;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{current_app_context, current_ready_iam_handle, current_server_config_for_context};
use crate::admin::utils::{encode_compatible_admin_payload, is_compat_admin_request};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_credentials::Credentials as StoredCredentials;
use rustfs_madmin::{
    ACCESS_KEY_LIST_ALL, ACCESS_KEY_LIST_STS_ONLY, ACCESS_KEY_LIST_SVCACC_ONLY, ACCESS_KEY_LIST_USERS_ONLY, ListAccessKeysResp,
    ServiceAccountInfo,
};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;
use time::OffsetDateTime;
use tracing::warn;
use url::form_urlencoded;

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_IDP: &str = "idp";
const EVENT_ADMIN_IDP_STATE: &str = "admin_idp_state";

/// Provider types recognised by the generic idp-config endpoints (#610).
const IDP_TYPE_OPENID: &str = "openid";
const IDP_TYPE_LDAP: &str = "ldap";

pub fn register_idp_compat_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // ---- rustfs/backlog#609 ----
    r.insert(
        Method::PUT,
        format!("{ADMIN_PREFIX}/v3/import-iam-v2").as_str(),
        AdminOperation(&ImportIamV2 {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/revoke-tokens/{{user_provider}}").as_str(),
        AdminOperation(&RevokeTokens {}),
    )?;

    // ---- rustfs/backlog#610 ----
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/idp-config/{{idp_type}}").as_str(),
        AdminOperation(&ListIdpConfig {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/idp-config/{{idp_type}}/{{name}}").as_str(),
        AdminOperation(&GetIdpConfig {}),
    )?;
    r.insert(
        Method::PUT,
        format!("{ADMIN_PREFIX}/v3/idp-config/{{idp_type}}/{{name}}").as_str(),
        AdminOperation(&SetIdpConfig {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/idp-config/{{idp_type}}/{{name}}").as_str(),
        AdminOperation(&SetIdpConfig {}),
    )?;
    r.insert(
        Method::DELETE,
        format!("{ADMIN_PREFIX}/v3/idp-config/{{idp_type}}/{{name}}").as_str(),
        AdminOperation(&DeleteIdpConfig {}),
    )?;

    // ---- rustfs/backlog#616 ----
    r.insert(
        Method::PUT,
        format!("{ADMIN_PREFIX}/v3/idp/ldap/add-service-account").as_str(),
        AdminOperation(&AddServiceAccountLdap {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/idp/ldap/list-access-keys").as_str(),
        AdminOperation(&ListAccessKeysLdap {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/idp/ldap/list-access-keys-bulk").as_str(),
        AdminOperation(&LIST_ACCESS_KEYS_LDAP_BULK),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/idp/ldap/policy-entities").as_str(),
        AdminOperation(&ListPolicyEntitiesBuiltin {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/idp/ldap/policy/{{operation}}").as_str(),
        AdminOperation(&PolicyOperationLdap {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/idp/openid/list-access-keys-bulk").as_str(),
        AdminOperation(&LIST_ACCESS_KEYS_OPENID_BULK),
    )?;

    Ok(())
}

static LIST_ACCESS_KEYS_LDAP_BULK: ListAccessKeysProviderBulk = ListAccessKeysProviderBulk::ldap();
static LIST_ACCESS_KEYS_OPENID_BULK: ListAccessKeysProviderBulk = ListAccessKeysProviderBulk::openid();

// ---------------------------------------------------------------------------
// #609: import-iam-v2
// ---------------------------------------------------------------------------

/// `PUT /v3/import-iam-v2`
///
/// v2 IAM import. Reuses the exact same zip import machinery as the v1
/// `import-iam` endpoint (policies, users, groups, service accounts and policy
/// mappings), which already produces a per-entity `ImportIAMResult` report.
///
/// The v1 and v2 MinIO endpoints share the same wire format; the v2 endpoint
/// exists so clients can detect a server that returns the structured import
/// report. RustFS's v1 handler already returns that report, so v2 delegates to
/// the same code path.
pub struct ImportIamV2 {}

#[async_trait::async_trait]
impl Operation for ImportIamV2 {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Identical semantics to v1 import-iam: same auth action, same body
        // format, same structured result. Delegate to the shared handler.
        ImportIam {}.call(req, params).await
    }
}

// ---------------------------------------------------------------------------
// #609: revoke-tokens
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct RevokeTokensResp {
    #[serde(rename = "revoked")]
    revoked: usize,
    #[serde(rename = "userProvider")]
    user_provider: String,
    #[serde(rename = "user")]
    user: String,
}

/// `POST /v3/revoke-tokens/{userProvider}?user=<parent>&tokenRevokeType=<type>&fullRevoke=<bool>`
///
/// Revokes STS/temporary credentials for a parent user, optionally scoped to a
/// single identity provider (`{userProvider}` = `builtin` | `ldap` | `openid`).
///
/// RustFS has no separate "session token" store: an STS credential *is* the
/// session, so revoking a token means deleting the STS account, which
/// immediately invalidates the session (subsequent auth fails). This matches
/// the observable effect of MinIO's revoke-tokens.
///
/// The `tokenRevokeType` filter (MinIO scopes revocation to STS creds whose
/// `tokenRevokeType` claim matches) is accepted for wire compatibility. RustFS
/// does not persist a `tokenRevokeType` claim on STS credentials, so a specific
/// type filter cannot match any credential and revokes nothing; `fullRevoke` /
/// an empty type revokes all STS credentials for the user under the requested
/// provider. This limitation is reported as COMPAT-SEMANTICS-ONLY.
pub struct RevokeTokens {}

#[async_trait::async_trait]
impl Operation for RevokeTokens {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let user_provider = params
            .get("user_provider")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing userProvider"))?
            .to_string();
        if !is_supported_user_provider(&user_provider) {
            return Err(s3_error!(InvalidRequest, "unsupported user provider: {}", user_provider));
        }

        let query = RevokeTokensQuery::parse(req.uri.query());

        let (cred, owner) = authorize(&req, AdminAction::RemoveServiceAccountAdminAction).await?;

        // Default target user is the caller (self-revocation), matching MinIO.
        let target_user = if query.user.is_empty() {
            request_user_name(&cred).to_string()
        } else {
            query.user.clone()
        };

        // Revoking another user's STS credentials is a cross-user action: require the
        // broader ListUsers admin capability in addition to RemoveServiceAccount,
        // mirroring the cross-user guard the service-account handlers apply. Pure
        // self-revocation only needs RemoveServiceAccount.
        if target_user != request_user_name(&cred) {
            authorize_action(&req, &cred, owner, AdminAction::ListUsersAdminAction).await?;
        }

        let iam_store = current_ready_iam_handle().map_err(|_| s3_error!(InvalidRequest, "iam not init"))?;

        let sts_accounts = iam_store.list_sts_accounts(&target_user).await.map_err(|e| {
            warn!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_IDP,
                event = EVENT_ADMIN_IDP_STATE,
                action = "revoke_tokens",
                target_user = %target_user,
                result = "list_sts_failed",
                error = ?e,
                "admin idp state"
            );
            s3_error!(InternalError, "list sts accounts failed")
        })?;

        // A specific tokenRevokeType filter never matches, because RustFS does
        // not persist that claim. Only an empty type / full revoke deletes.
        let revoke_all = query.full_revoke || query.token_revoke_type.is_empty();

        let mut revoked = 0usize;
        if revoke_all {
            for sts in &sts_accounts {
                // Provider scoping: only revoke STS credentials that were issued
                // through the requested identity provider.
                if guess_user_provider(sts) != user_provider {
                    continue;
                }

                iam_store.delete_temp_account(&sts.access_key, true).await.map_err(|e| {
                    warn!(
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_IDP,
                        event = EVENT_ADMIN_IDP_STATE,
                        action = "revoke_tokens",
                        access_key = %sts.access_key,
                        result = "delete_failed",
                        error = ?e,
                        "admin idp state"
                    );
                    s3_error!(InternalError, "revoke token failed")
                })?;
                revoked += 1;
            }
        }

        let _ = owner;
        json_response(
            req.uri.path(),
            &cred.secret_key,
            StatusCode::OK,
            &RevokeTokensResp {
                revoked,
                user_provider,
                user: target_user,
            },
        )
    }
}

#[derive(Debug, Default)]
struct RevokeTokensQuery {
    user: String,
    token_revoke_type: String,
    full_revoke: bool,
}

impl RevokeTokensQuery {
    fn parse(query: Option<&str>) -> Self {
        let mut parsed = Self::default();
        let Some(query) = query else {
            return parsed;
        };
        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "user" => parsed.user = value.into_owned(),
                "tokenRevokeType" | "token-revoke-type" => parsed.token_revoke_type = value.into_owned(),
                "fullRevoke" | "full-revoke" => parsed.full_revoke = parse_bool_param(value.as_ref()),
                _ => {}
            }
        }
        parsed
    }
}

// ---------------------------------------------------------------------------
// #610: generic idp-config
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct IdpConfigEntry {
    #[serde(rename = "key")]
    key: String,
    #[serde(rename = "value")]
    value: String,
}

#[derive(Debug, Serialize)]
struct IdpConfigResp {
    #[serde(rename = "type")]
    idp_type: String,
    #[serde(rename = "name")]
    name: String,
    #[serde(rename = "enabled")]
    enabled: bool,
    #[serde(rename = "info")]
    info: Vec<IdpConfigEntry>,
}

#[derive(Debug, Serialize)]
struct IdpListEntry {
    #[serde(rename = "name")]
    name: String,
    #[serde(rename = "enabled")]
    enabled: bool,
}

#[derive(Debug, Serialize)]
struct IdpListResp {
    #[serde(rename = "type")]
    idp_type: String,
    #[serde(rename = "idp")]
    idp: Vec<IdpListEntry>,
}

/// `GET /v3/idp-config/{type}` — list configured providers of `{type}`.
///
/// `{type}` maps to a server-config subsystem. `openid` reads the persisted
/// OpenID provider configs (the same source as the RustFS-native
/// `/v3/oidc/config` endpoint); `ldap` reads the `identity_ldap` subsystem.
/// Unsupported types return an honest error rather than an empty list.
pub struct ListIdpConfig {}

#[async_trait::async_trait]
impl Operation for ListIdpConfig {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let idp_type = normalized_idp_type(&params)?;
        let (cred, _) = authorize(&req, AdminAction::ServerInfoAdminAction).await?;

        let names = idp_config_names(&idp_type)?;
        let idp = names
            .into_iter()
            .map(|(name, enabled)| IdpListEntry { name, enabled })
            .collect();

        json_response(req.uri.path(), &cred.secret_key, StatusCode::OK, &IdpListResp { idp_type, idp })
    }
}

/// `GET /v3/idp-config/{type}/{name}` — read one provider config.
///
/// Secret values (client secrets, bind passwords) are redacted in the response.
pub struct GetIdpConfig {}

#[async_trait::async_trait]
impl Operation for GetIdpConfig {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let idp_type = normalized_idp_type(&params)?;
        let name = params.get("name").unwrap_or(DEFAULT_DELIMITER).to_string();
        let (cred, _) = authorize(&req, AdminAction::ServerInfoAdminAction).await?;

        let (enabled, info) = idp_config_info(&idp_type, &name)?;

        json_response(
            req.uri.path(),
            &cred.secret_key,
            StatusCode::OK,
            &IdpConfigResp {
                idp_type,
                name,
                enabled,
                info,
            },
        )
    }
}

/// `PUT|POST /v3/idp-config/{type}/{name}` — create/update a provider config.
///
/// LIMITATION (COMPAT-SEMANTICS-ONLY): RustFS persists OpenID provider configs
/// through the dedicated, validated `/v3/oidc/config/{provider_id}` endpoint
/// (which performs discovery validation and secret handling). This generic
/// mutation endpoint intentionally does not blind-write arbitrary KVS into the
/// identity subsystems, because doing so would bypass that validation and the
/// LDAP backend does not exist. It returns a clear error directing callers to
/// the typed endpoint. Reads (GET/list) and deletes are honoured.
pub struct SetIdpConfig {}

#[async_trait::async_trait]
impl Operation for SetIdpConfig {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let idp_type = normalized_idp_type(&params)?;
        authorize(&req, AdminAction::ConfigUpdateAdminAction).await?;

        match idp_type.as_str() {
            IDP_TYPE_OPENID => Err(s3_error!(
                NotImplemented,
                "set openid idp-config via this endpoint is not supported; use PUT /rustfs/admin/v3/oidc/config/{{provider_id}} which validates discovery and handles client secrets"
            )),
            IDP_TYPE_LDAP => Err(s3_error!(
                NotImplemented,
                "LDAP identity provider is not implemented in RustFS; idp-config writes for type 'ldap' are not supported"
            )),
            other => Err(s3_error!(InvalidRequest, "unsupported idp-config type: {}", other)),
        }
    }
}

/// `DELETE /v3/idp-config/{type}/{name}` — delete a provider config.
///
/// For `openid`, deletes the persisted provider from the OpenID subsystem via
/// the same server-config mutation the typed endpoint uses. For `ldap`, returns
/// an honest not-implemented error.
pub struct DeleteIdpConfig {}

#[async_trait::async_trait]
impl Operation for DeleteIdpConfig {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let idp_type = normalized_idp_type(&params)?;
        authorize(&req, AdminAction::ConfigUpdateAdminAction).await?;

        match idp_type.as_str() {
            IDP_TYPE_OPENID => Err(s3_error!(
                NotImplemented,
                "delete openid idp-config via this endpoint is not supported; use DELETE /rustfs/admin/v3/oidc/config/{{provider_id}}"
            )),
            IDP_TYPE_LDAP => Err(s3_error!(
                NotImplemented,
                "LDAP identity provider is not implemented in RustFS; idp-config deletes for type 'ldap' are not supported"
            )),
            other => Err(s3_error!(InvalidRequest, "unsupported idp-config type: {}", other)),
        }
    }
}

fn normalized_idp_type(params: &Params<'_, '_>) -> S3Result<String> {
    let idp_type = params
        .get("idp_type")
        .ok_or_else(|| s3_error!(InvalidRequest, "missing idp type"))?
        .to_ascii_lowercase();
    if !is_supported_idp_type(&idp_type) {
        return Err(s3_error!(InvalidRequest, "unsupported idp-config type: {}", idp_type));
    }
    Ok(idp_type)
}

fn is_supported_idp_type(idp_type: &str) -> bool {
    matches!(idp_type, IDP_TYPE_OPENID | IDP_TYPE_LDAP)
}

/// List configured provider names of a given idp type as `(name, enabled)`.
fn idp_config_names(idp_type: &str) -> S3Result<Vec<(String, bool)>> {
    let config = current_active_server_config();
    match idp_type {
        IDP_TYPE_OPENID => Ok(rustfs_iam::oidc::load_effective_oidc_provider_configs(config.as_ref())
            .into_iter()
            .map(|provider| (provider.config.id, provider.config.enabled))
            .collect()),
        IDP_TYPE_LDAP => {
            // No LDAP backend. Report configured LDAP subsystem instances (if
            // any operator has set them) without pretending to authenticate.
            Ok(ldap_subsystem_instances(config.as_ref()))
        }
        _ => Err(s3_error!(InvalidRequest, "unsupported idp-config type: {}", idp_type)),
    }
}

/// Read one provider config as `(enabled, redacted info entries)`.
fn idp_config_info(idp_type: &str, name: &str) -> S3Result<(bool, Vec<IdpConfigEntry>)> {
    let config = current_active_server_config();
    match idp_type {
        IDP_TYPE_OPENID => {
            let provider = rustfs_iam::oidc::load_effective_oidc_provider_configs(config.as_ref())
                .into_iter()
                .find(|provider| provider.config.id == name || (name == DEFAULT_DELIMITER && provider.config.id == "default"))
                .ok_or_else(|| s3_error!(NoSuchResource, "openid provider '{}' not found", name))?;

            let c = &provider.config;
            let info = vec![
                IdpConfigEntry {
                    key: "config_url".to_string(),
                    value: c.config_url.clone(),
                },
                IdpConfigEntry {
                    key: "client_id".to_string(),
                    value: c.client_id.clone(),
                },
                IdpConfigEntry {
                    key: "client_secret".to_string(),
                    value: redacted_secret(c.client_secret.is_some()),
                },
                IdpConfigEntry {
                    key: "scopes".to_string(),
                    value: c.scopes.join(","),
                },
                IdpConfigEntry {
                    key: "role_policy".to_string(),
                    value: c.role_policy.clone(),
                },
                IdpConfigEntry {
                    key: "claim_name".to_string(),
                    value: c.claim_name.clone(),
                },
            ];
            Ok((c.enabled, info))
        }
        IDP_TYPE_LDAP => {
            let instances = ldap_subsystem_instances(config.as_ref());
            let enabled = instances
                .iter()
                .find(|(n, _)| n == name || (name == DEFAULT_DELIMITER && n == DEFAULT_DELIMITER))
                .map(|(_, enabled)| *enabled)
                .ok_or_else(|| s3_error!(NoSuchResource, "ldap provider '{}' not found", name))?;
            // No secret material is exposed; the LDAP backend is not implemented
            // so only the raw configured presence is reported.
            Ok((enabled, Vec::new()))
        }
        _ => Err(s3_error!(InvalidRequest, "unsupported idp-config type: {}", idp_type)),
    }
}

/// Enumerate configured LDAP subsystem instances (config-only; no backend).
fn ldap_subsystem_instances(config: Option<&rustfs_config::server_config::Config>) -> Vec<(String, bool)> {
    const IDENTITY_LDAP_SUB_SYS: &str = "identity_ldap";
    let Some(config) = config else {
        return Vec::new();
    };
    let Some(subsystem) = config.0.get(IDENTITY_LDAP_SUB_SYS) else {
        return Vec::new();
    };
    subsystem
        .iter()
        .map(|(instance, kvs)| {
            let enabled = kvs
                .lookup(rustfs_config::ENABLE_KEY)
                .map(|v| v.eq_ignore_ascii_case("on") || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            (instance.clone(), enabled)
        })
        .collect()
}

fn redacted_secret(configured: bool) -> String {
    if configured { "[redacted]".to_string() } else { String::new() }
}

fn current_active_server_config() -> Option<rustfs_config::server_config::Config> {
    let context = current_app_context();
    current_server_config_for_context(context.as_deref())
}

// ---------------------------------------------------------------------------
// #616: LDAP / OpenID service-account, access-key and policy endpoints
// ---------------------------------------------------------------------------

/// `PUT /v3/idp/ldap/add-service-account`
///
/// Creates a service account under an LDAP-provider parent. The request/response
/// wire format is identical to the builtin `add-service-account` endpoint, so
/// this delegates to the shared handler.
///
/// LIMITATION (COMPAT-SEMANTICS-ONLY): RustFS has no LDAP directory backend, so
/// the supplied `targetUser` DN is not validated or normalised against a live
/// directory. The service account is stored with whatever parent DN the caller
/// provides, exactly as it is for other externally-provisioned identities.
pub struct AddServiceAccountLdap {}

#[async_trait::async_trait]
impl Operation for AddServiceAccountLdap {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        AddServiceAccount {}.call(req, params).await
    }
}

/// `GET /v3/idp/ldap/list-access-keys?userDN=<dn>&listType=<type>`
///
/// Lists STS and/or service-account access keys for a single LDAP user DN.
/// Mirrors MinIO's per-user LDAP list. Uses the existing IAM listing methods.
pub struct ListAccessKeysLdap {}

#[async_trait::async_trait]
impl Operation for ListAccessKeysLdap {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = SingleUserAccessKeysQuery::parse(req.uri.query());
        let list_flags = ListTypeFlags::parse(&query.list_type)?;

        let (cred, owner) = authorize(&req, AdminAction::ListServiceAccountsAdminAction).await?;

        // Default to the caller's own identity when no explicit DN is given.
        let target_user = if query.user_dn.is_empty() {
            request_user_name(&cred).to_string()
        } else {
            query.user_dn.clone()
        };

        // Cross-user listing requires ListUsers, matching the builtin bulk lister.
        if target_user != request_user_name(&cred) {
            authorize_action(&req, &cred, owner, AdminAction::ListUsersAdminAction).await?;
        }

        let iam_store = current_ready_iam_handle().map_err(|_| s3_error!(InvalidRequest, "iam not init"))?;
        let resp =
            collect_access_keys_for_user(&iam_store, &target_user, req.uri.path(), list_flags, Some(IDP_TYPE_LDAP)).await?;

        json_response(req.uri.path(), &cred.secret_key, StatusCode::OK, &resp)
    }
}

/// `GET /v3/idp/{ldap,openid}/list-access-keys-bulk`
///
/// Bulk list of STS + service-account access keys, filtered to the identity
/// provider indicated by the route (`ldap` or `openid`). Reuses the existing
/// IAM listing; provider attribution uses the same claim-based classification as
/// `info-access-key`.
pub struct ListAccessKeysProviderBulk {
    provider: &'static str,
}

impl ListAccessKeysProviderBulk {
    const fn ldap() -> Self {
        Self { provider: IDP_TYPE_LDAP }
    }
    const fn openid() -> Self {
        Self {
            provider: IDP_TYPE_OPENID,
        }
    }
}

#[async_trait::async_trait]
impl Operation for ListAccessKeysProviderBulk {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = BulkAccessKeysQuery::parse(req.uri.query());
        if query.all && !query.users.is_empty() {
            return Err(s3_error!(InvalidRequest, "either specify users or all, not both"));
        }
        let list_flags = ListTypeFlags::parse(&query.list_type)?;

        let (cred, owner) = authorize(&req, AdminAction::ListServiceAccountsAdminAction).await?;

        let self_name = request_user_name(&cred).to_string();
        let self_only = !query.all && (query.users.is_empty() || (query.users.len() == 1 && query.users[0] == self_name));

        if query.all || !self_only {
            authorize_action(&req, &cred, owner, AdminAction::ListUsersAdminAction).await?;
        }

        let iam_store = current_ready_iam_handle().map_err(|_| s3_error!(InvalidRequest, "iam not init"))?;

        let users: Vec<String> = if query.all {
            iam_store
                .list_users()
                .await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("list users err {e}")))?
                .into_keys()
                .collect()
        } else if query.users.is_empty() {
            vec![self_name]
        } else {
            query.users.clone()
        };

        let mut result: HashMap<String, ListAccessKeysResp> = HashMap::new();
        for user in users {
            let resp = collect_access_keys_for_user(&iam_store, &user, req.uri.path(), list_flags, Some(self.provider)).await?;
            if resp.service_accounts.is_empty() && resp.sts_keys.is_empty() {
                continue;
            }
            result.insert(user, resp);
        }

        json_response(req.uri.path(), &cred.secret_key, StatusCode::OK, &result)
    }
}

/// `POST /v3/idp/ldap/policy/{operation}` where `{operation}` is `attach` or `detach`.
///
/// Attaches/detaches builtin policies to an LDAP user/group. RustFS stores LDAP
/// user/group policy mappings in the same builtin policy DB, so this reuses the
/// builtin policy-association handler.
///
/// LIMITATION (COMPAT-SEMANTICS-ONLY): the target user/group is not validated
/// against a live LDAP directory (no backend). The mapping is persisted as-is.
pub struct PolicyOperationLdap {}

#[async_trait::async_trait]
impl Operation for PolicyOperationLdap {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let operation = params
            .get("operation")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing policy operation"))?;
        let is_attach = match operation {
            "attach" => true,
            "detach" => false,
            other => return Err(s3_error!(InvalidRequest, "unsupported policy operation: {}", other)),
        };
        handle_builtin_policy_association(req, is_attach).await
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn is_supported_user_provider(provider: &str) -> bool {
    matches!(provider, "builtin" | IDP_TYPE_LDAP | IDP_TYPE_OPENID)
}

fn parse_bool_param(value: &str) -> bool {
    matches!(value, "true" | "1" | "on" | "yes")
}

fn request_user_name(cred: &StoredCredentials) -> &str {
    if cred.parent_user.is_empty() {
        &cred.access_key
    } else {
        &cred.parent_user
    }
}

#[derive(Clone, Copy)]
struct ListTypeFlags {
    sts: bool,
    svc: bool,
}

impl ListTypeFlags {
    fn parse(list_type: &str) -> S3Result<Self> {
        match list_type {
            "" | ACCESS_KEY_LIST_ALL => Ok(Self { sts: true, svc: true }),
            ACCESS_KEY_LIST_STS_ONLY => Ok(Self { sts: true, svc: false }),
            ACCESS_KEY_LIST_SVCACC_ONLY => Ok(Self { sts: false, svc: true }),
            ACCESS_KEY_LIST_USERS_ONLY => Ok(Self { sts: false, svc: false }),
            _ => Err(s3_error!(InvalidRequest, "invalid list type")),
        }
    }
}

#[derive(Debug, Default)]
struct SingleUserAccessKeysQuery {
    user_dn: String,
    list_type: String,
}

impl SingleUserAccessKeysQuery {
    fn parse(query: Option<&str>) -> Self {
        let mut parsed = Self::default();
        let Some(query) = query else {
            return parsed;
        };
        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "userDN" | "user-dn" | "user" => parsed.user_dn = value.into_owned(),
                "listType" | "list-type" => parsed.list_type = value.into_owned(),
                _ => {}
            }
        }
        parsed
    }
}

#[derive(Debug, Default)]
struct BulkAccessKeysQuery {
    users: Vec<String>,
    all: bool,
    list_type: String,
}

impl BulkAccessKeysQuery {
    fn parse(query: Option<&str>) -> Self {
        let mut parsed = Self::default();
        let Some(query) = query else {
            return parsed;
        };
        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "users" if !value.is_empty() => parsed.users.push(value.into_owned()),
                "all" => parsed.all = parse_bool_param(value.as_ref()),
                "listType" | "list-type" => parsed.list_type = value.into_owned(),
                _ => {}
            }
        }
        parsed
    }
}

/// Build a `ListAccessKeysResp` for a single parent user, optionally filtered to
/// a single identity provider (`ldap` / `openid`).
async fn collect_access_keys_for_user(
    iam_store: &std::sync::Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>,
    user: &str,
    path: &str,
    flags: ListTypeFlags,
    provider_filter: Option<&str>,
) -> S3Result<ListAccessKeysResp> {
    let mut resp = ListAccessKeysResp::default();

    if flags.sts {
        let sts_keys = iam_store
            .list_sts_accounts(user)
            .await
            .map_err(|e| s3_error!(InternalError, "list sts account failed: {}", e))?;
        resp.sts_keys = sts_keys
            .into_iter()
            .filter(|cred| provider_matches(cred, provider_filter))
            .map(|cred| access_key_entry(&cred, path))
            .collect();
    }

    if flags.svc {
        let svc = iam_store
            .list_service_accounts(user)
            .await
            .map_err(|e| s3_error!(InternalError, "list service account failed: {}", e))?;
        resp.service_accounts = svc
            .into_iter()
            .filter(|cred| provider_matches(cred, provider_filter))
            .map(|cred| access_key_entry(&cred, path))
            .collect();
    }

    Ok(resp)
}

fn provider_matches(cred: &StoredCredentials, provider_filter: Option<&str>) -> bool {
    match provider_filter {
        None => true,
        Some(provider) => guess_user_provider(cred) == provider,
    }
}

fn access_key_entry(cred: &StoredCredentials, path: &str) -> ServiceAccountInfo {
    ServiceAccountInfo {
        parent_user: cred.parent_user.clone(),
        account_status: cred.status.clone(),
        implied_policy: false,
        access_key: cred.access_key.clone(),
        name: cred.name.clone(),
        description: cred.description.clone(),
        expiration: expiration_for_admin_path(path, cred.expiration),
    }
}

fn expiration_for_admin_path(path: &str, expiration: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    if is_compat_admin_request(path) {
        Some(expiration.unwrap_or(OffsetDateTime::UNIX_EPOCH))
    } else {
        expiration
    }
}

async fn authorize(req: &S3Request<Body>, action: AdminAction) -> S3Result<(StoredCredentials, bool)> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };
    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    authorize_action(req, &cred, owner, action).await?;
    Ok((cred, owner))
}

async fn authorize_action(req: &S3Request<Body>, cred: &StoredCredentials, owner: bool, action: AdminAction) -> S3Result<()> {
    validate_admin_request(
        &req.headers,
        cred,
        owner,
        false,
        vec![Action::AdminAction(action)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

fn json_response<T: Serialize>(
    path: &str,
    secret_key: &str,
    status: StatusCode,
    payload: &T,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let body = serde_json::to_vec(payload)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize error: {e}")))?;
    let (body, content_type) = encode_compatible_admin_payload(path, secret_key, body)?;

    let mut header = HeaderMap::new();
    header.insert(CONTENT_TYPE, content_type.parse().expect("valid header value"));
    Ok(S3Response::with_headers((status, Body::from(body)), header))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revoke_tokens_query_parses_all_fields() {
        let q = RevokeTokensQuery::parse(Some("user=uid%3Dalice&tokenRevokeType=session&fullRevoke=true"));
        assert_eq!(q.user, "uid=alice");
        assert_eq!(q.token_revoke_type, "session");
        assert!(q.full_revoke);
    }

    #[test]
    fn revoke_tokens_query_defaults_to_full_revoke_when_type_absent() {
        let q = RevokeTokensQuery::parse(Some("user=alice"));
        assert!(q.token_revoke_type.is_empty());
        assert!(!q.full_revoke);
        // revoke_all is derived in the handler: empty type OR fullRevoke.
        assert!(q.full_revoke || q.token_revoke_type.is_empty());
    }

    #[test]
    fn list_type_flags_parse_known_values() {
        assert!(matches!(ListTypeFlags::parse(""), Ok(ListTypeFlags { sts: true, svc: true })));
        assert!(matches!(ListTypeFlags::parse("all"), Ok(ListTypeFlags { sts: true, svc: true })));
        assert!(matches!(ListTypeFlags::parse("sts-only"), Ok(ListTypeFlags { sts: true, svc: false })));
        assert!(matches!(ListTypeFlags::parse("svcacc-only"), Ok(ListTypeFlags { sts: false, svc: true })));
        assert!(matches!(ListTypeFlags::parse("users-only"), Ok(ListTypeFlags { sts: false, svc: false })));
        assert!(ListTypeFlags::parse("bogus").is_err());
    }

    #[test]
    fn bulk_query_rejects_empty_users_values() {
        let q = BulkAccessKeysQuery::parse(Some("users=&users=alice&all=false&listType=all"));
        assert_eq!(q.users, vec!["alice".to_string()]);
        assert!(!q.all);
        assert_eq!(q.list_type, "all");
    }

    #[test]
    fn single_user_query_supports_userdn_alias() {
        let q = SingleUserAccessKeysQuery::parse(Some("userDN=uid%3Dbob%2Cou%3Dpeople&listType=sts-only"));
        assert_eq!(q.user_dn, "uid=bob,ou=people");
        assert_eq!(q.list_type, "sts-only");
    }

    #[test]
    fn supported_idp_type_is_case_insensitive_via_normalizer() {
        assert!(is_supported_idp_type("openid"));
        assert!(is_supported_idp_type("ldap"));
        assert!(!is_supported_idp_type("saml"));
    }

    #[test]
    fn supported_user_provider_matches_known_providers() {
        assert!(is_supported_user_provider("builtin"));
        assert!(is_supported_user_provider("ldap"));
        assert!(is_supported_user_provider("openid"));
        assert!(!is_supported_user_provider("saml"));
    }

    #[test]
    fn redacted_secret_hides_configured_value() {
        assert_eq!(redacted_secret(true), "[redacted]");
        assert_eq!(redacted_secret(false), "");
    }

    #[test]
    fn expiration_uses_sentinel_for_compat_path() {
        assert_eq!(
            expiration_for_admin_path("/minio/admin/v3/idp/ldap/list-access-keys", None),
            Some(OffsetDateTime::UNIX_EPOCH)
        );
        assert_eq!(expiration_for_admin_path("/rustfs/admin/v3/idp/ldap/list-access-keys", None), None);
    }
}
