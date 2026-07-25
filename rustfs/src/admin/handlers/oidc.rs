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

use crate::admin::auth::validate_admin_request;
use crate::admin::handlers::supervise_admin_mutation;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    current_app_context, current_federated_identity_service, current_object_store_handle_for_context,
    current_server_config_for_context,
};
use crate::admin::service::federated_identity::DefaultFederatedSessionBinding;
use crate::admin::storage_api::config::{
    read_admin_config_without_migrate, read_admin_server_config_snapshot, save_admin_server_config_snapshot,
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, CONSOLE_PREFIX, MINIO_ADMIN_PREFIX, RemoteAddr};
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use rustfs_config::oidc::{
    IDENTITY_OPENID_SUB_SYS, OIDC_CLAIM_NAME, OIDC_CLAIM_PREFIX, OIDC_CLIENT_ID, OIDC_CLIENT_SECRET, OIDC_CONFIG_URL,
    OIDC_DEFAULT_CLAIM_NAME, OIDC_DEFAULT_EMAIL_CLAIM, OIDC_DEFAULT_GROUPS_CLAIM, OIDC_DEFAULT_ROLES_CLAIM, OIDC_DEFAULT_SCOPES,
    OIDC_DEFAULT_USERNAME_CLAIM, OIDC_DISPLAY_NAME, OIDC_EMAIL_CLAIM, OIDC_GROUPS_CLAIM, OIDC_HIDE_FROM_UI, OIDC_ISSUER,
    OIDC_OTHER_AUDIENCES, OIDC_REDIRECT_URI, OIDC_REDIRECT_URI_DYNAMIC, OIDC_ROLE_POLICY, OIDC_ROLES_CLAIM, OIDC_SCOPES,
    OIDC_USERNAME_CLAIM,
};
use rustfs_config::server_config::Config as ServerConfig;
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, ENV_RUSTFS_BROWSER_REDIRECT_URL, EnableState, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_iam::federation::{FederatedSessionBindingError, FederationError};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_utils::egress::validate_outbound_url;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{debug, error, warn};
use url::Url;

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_OIDC: &str = "oidc";
const EVENT_ADMIN_OIDC_STATE: &str = "admin_oidc_state";

const OIDC_PUBLIC_PROVIDERS_SUFFIX: &str = "/v3/oidc/providers";
const OIDC_AUTHORIZE_SUFFIX: &str = "/v3/oidc/authorize/";
const OIDC_CALLBACK_SUFFIX: &str = "/v3/oidc/callback/";
const OIDC_LOGOUT_SUFFIX: &str = "/v3/oidc/logout";
const CONSOLE_OIDC_CALLBACK_SUFFIX: &str = "/auth/oidc-callback/";
const CONSOLE_LOGIN_SUFFIX: &str = "/auth/login";
const OIDC_STATE_LB_HINT: &str =
    "check load balancer session affinity for OIDC authorize/callback requests or configure RUSTFS_BROWSER_REDIRECT_URL";

fn callback_federation_error(error: FederationError) -> S3Error {
    match error {
        FederationError::CodeExchange(message) => {
            S3Error::with_message(S3ErrorCode::AccessDenied, format!("code exchange failed: {message}"))
        }
        FederationError::Logout(message) => {
            S3Error::with_message(S3ErrorCode::InternalError, format!("logout session creation failed: {message}"))
        }
        FederationError::Binding(FederatedSessionBindingError::InvalidRequest(message)) => {
            S3Error::with_message(S3ErrorCode::InvalidRequest, message)
        }
        FederationError::Binding(FederatedSessionBindingError::Internal(message)) => {
            S3Error::with_message(S3ErrorCode::InternalError, message)
        }
        other => S3Error::with_message(S3ErrorCode::InternalError, other.to_string()),
    }
}

/// Validate that a provider ID contains only safe characters (alphanumeric, underscore, hyphen).
fn is_valid_provider_id(id: &str) -> bool {
    !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Validate that a redirect_after path is a safe relative path (starts with `/`, no scheme).
fn is_safe_redirect_path(path: &str) -> bool {
    path.starts_with('/') && !path.starts_with("//") && !path.contains("://")
}

/// Validate that a scheme is either "http" or "https".
fn is_valid_scheme(scheme: &str) -> bool {
    scheme == "http" || scheme == "https"
}

/// Register OIDC routes on the admin router.
pub fn register_oidc_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/oidc/providers"),
        AdminOperation(&ListOidcProvidersHandler {}),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/oidc/authorize/{{provider_id}}"),
        AdminOperation(&OidcAuthorizeHandler {}),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/oidc/callback/{{provider_id}}"),
        AdminOperation(&OidcCallbackHandler {}),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}{OIDC_LOGOUT_SUFFIX}"),
        AdminOperation(&OidcLogoutHandler {}),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/oidc/config"),
        AdminOperation(&GetOidcConfigHandler {}),
    )?;
    r.insert(
        Method::PUT,
        &format!("{ADMIN_PREFIX}/v3/oidc/config/{{provider_id}}"),
        AdminOperation(&PutOidcConfigHandler {}),
    )?;
    r.insert(
        Method::DELETE,
        &format!("{ADMIN_PREFIX}/v3/oidc/config/{{provider_id}}"),
        AdminOperation(&DeleteOidcConfigHandler {}),
    )?;
    r.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/oidc/validate"),
        AdminOperation(&ValidateOidcConfigHandler {}),
    )?;

    Ok(())
}

/// Returns true if the given path is an OIDC endpoint (requires unauthenticated access).
pub fn is_oidc_path(path: &str) -> bool {
    let public_prefixes = [ADMIN_PREFIX, MINIO_ADMIN_PREFIX];

    public_prefixes.iter().any(|prefix| {
        path == format!("{prefix}{OIDC_PUBLIC_PROVIDERS_SUFFIX}")
            || path.starts_with(&format!("{prefix}{OIDC_AUTHORIZE_SUFFIX}"))
            || path.starts_with(&format!("{prefix}{OIDC_CALLBACK_SUFFIX}"))
            || path == format!("{prefix}{OIDC_LOGOUT_SUFFIX}")
    })
}

#[derive(Debug, Serialize)]
struct OidcConfigListResponse {
    providers: Vec<OidcConfigView>,
    restart_required: bool,
}

#[derive(Debug, Serialize)]
struct OidcConfigView {
    provider_id: String,
    source: rustfs_iam::oidc::OidcProviderConfigSource,
    editable: bool,
    enabled: bool,
    display_name: String,
    config_url: String,
    issuer: Option<String>,
    client_id: String,
    client_secret_configured: bool,
    scopes: Vec<String>,
    other_audiences: Vec<String>,
    redirect_uri: Option<String>,
    redirect_uri_dynamic: bool,
    claim_name: String,
    claim_prefix: String,
    role_policy: String,
    groups_claim: String,
    roles_claim: String,
    email_claim: String,
    username_claim: String,
    hide_from_ui: bool,
}

#[derive(Debug, Serialize)]
struct OidcMutationResponse {
    success: bool,
    message: String,
    restart_required: bool,
}

#[derive(Debug, Serialize)]
struct OidcValidationResponse {
    valid: bool,
    message: String,
    issuer: Option<String>,
    authorization_endpoint: Option<String>,
    token_endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OidcConfigUpsertRequest {
    enabled: bool,
    display_name: String,
    config_url: String,
    issuer: Option<String>,
    client_id: String,
    client_secret: Option<String>,
    scopes: Vec<String>,
    other_audiences: Vec<String>,
    redirect_uri: Option<String>,
    redirect_uri_dynamic: bool,
    claim_name: String,
    claim_prefix: String,
    role_policy: String,
    groups_claim: String,
    roles_claim: String,
    email_claim: String,
    username_claim: String,
    hide_from_ui: bool,
}

impl Default for OidcConfigUpsertRequest {
    fn default() -> Self {
        Self {
            enabled: true,
            display_name: String::new(),
            config_url: String::new(),
            issuer: None,
            client_id: String::new(),
            client_secret: None,
            scopes: OIDC_DEFAULT_SCOPES.split(',').map(ToString::to_string).collect(),
            other_audiences: Vec::new(),
            redirect_uri: None,
            redirect_uri_dynamic: true,
            claim_name: OIDC_DEFAULT_CLAIM_NAME.to_string(),
            claim_prefix: String::new(),
            role_policy: String::new(),
            groups_claim: OIDC_DEFAULT_GROUPS_CLAIM.to_string(),
            roles_claim: OIDC_DEFAULT_ROLES_CLAIM.to_string(),
            email_claim: OIDC_DEFAULT_EMAIL_CLAIM.to_string(),
            username_claim: OIDC_DEFAULT_USERNAME_CLAIM.to_string(),
            hide_from_ui: false,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OidcConfigValidateRequest {
    provider_id: String,
    enabled: bool,
    display_name: String,
    config_url: String,
    issuer: Option<String>,
    client_id: String,
    client_secret: Option<String>,
    scopes: Vec<String>,
    other_audiences: Vec<String>,
    redirect_uri: Option<String>,
    redirect_uri_dynamic: bool,
    claim_name: String,
    claim_prefix: String,
    role_policy: String,
    groups_claim: String,
    roles_claim: String,
    email_claim: String,
    username_claim: String,
    hide_from_ui: bool,
}

impl Default for OidcConfigValidateRequest {
    fn default() -> Self {
        Self {
            provider_id: "default".to_string(),
            enabled: true,
            display_name: String::new(),
            config_url: String::new(),
            issuer: None,
            client_id: String::new(),
            client_secret: None,
            scopes: OIDC_DEFAULT_SCOPES.split(',').map(ToString::to_string).collect(),
            other_audiences: Vec::new(),
            redirect_uri: None,
            redirect_uri_dynamic: true,
            claim_name: OIDC_DEFAULT_CLAIM_NAME.to_string(),
            claim_prefix: String::new(),
            role_policy: String::new(),
            groups_claim: OIDC_DEFAULT_GROUPS_CLAIM.to_string(),
            roles_claim: OIDC_DEFAULT_ROLES_CLAIM.to_string(),
            email_claim: OIDC_DEFAULT_EMAIL_CLAIM.to_string(),
            username_claim: OIDC_DEFAULT_USERNAME_CLAIM.to_string(),
            hide_from_ui: false,
        }
    }
}

/// Handler: GET /rustfs/admin/v3/oidc/providers
/// Returns list of configured OIDC providers for the login page.
pub struct ListOidcProvidersHandler {}

#[async_trait::async_trait]
impl Operation for ListOidcProvidersHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let federation = current_federated_identity_service().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

        let providers = federation.list_visible_providers();
        let json_body = serde_json::to_vec(&providers)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize error: {e}")))?;

        let mut resp = S3Response::new((StatusCode::OK, Body::from(json_body)));
        resp.headers
            .insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
        Ok(resp)
    }
}

pub struct GetOidcConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetOidcConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_oidc_config_request(&req, AdminAction::ServerInfoAdminAction).await?;

        let config = load_server_config_from_store().await?;
        let restart_required = oidc_restart_required(&config);
        let providers = rustfs_iam::oidc::load_effective_oidc_provider_configs(Some(&config))
            .into_iter()
            .map(|provider| OidcConfigView {
                provider_id: provider.config.id.clone(),
                source: provider.source,
                editable: provider.source != rustfs_iam::oidc::OidcProviderConfigSource::Env,
                enabled: provider.config.enabled,
                display_name: provider.config.display_name.clone(),
                config_url: provider.config.config_url.clone(),
                issuer: provider.config.issuer.clone(),
                client_id: provider.config.client_id.clone(),
                client_secret_configured: provider.config.client_secret.is_some(),
                scopes: provider.config.scopes.clone(),
                other_audiences: provider.config.other_audiences.clone(),
                redirect_uri: provider.config.redirect_uri.clone(),
                redirect_uri_dynamic: provider.config.redirect_uri_dynamic,
                claim_name: provider.config.claim_name.clone(),
                claim_prefix: provider.config.claim_prefix.clone(),
                role_policy: provider.config.role_policy.clone(),
                groups_claim: provider.config.groups_claim.clone(),
                roles_claim: provider.config.roles_claim.clone(),
                email_claim: provider.config.email_claim.clone(),
                username_claim: provider.config.username_claim,
                hide_from_ui: provider.config.hide_from_ui,
            })
            .collect();

        json_response(
            StatusCode::OK,
            &OidcConfigListResponse {
                providers,
                restart_required,
            },
        )
    }
}

pub struct PutOidcConfigHandler {}

#[async_trait::async_trait]
impl Operation for PutOidcConfigHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_oidc_config_request(&req, AdminAction::ConfigUpdateAdminAction).await?;

        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;
        if !is_valid_provider_id(provider_id) {
            return Err(s3_error!(InvalidRequest, "invalid provider_id"));
        }
        if is_env_managed_provider(provider_id) {
            return Err(s3_error!(AccessDenied, "provider is managed by environment variables"));
        }
        let provider_id = provider_id.to_owned();

        let request: OidcConfigUpsertRequest = parse_json_body(&mut req).await?;
        update_oidc_server_config(move |config| {
            let existing_secret = persisted_provider_secret(config, &provider_id);
            let provider_config = build_provider_config_from_upsert(&provider_id, request, existing_secret)?;
            upsert_persisted_provider_config(config, &provider_config);
            Ok(())
        })
        .await?;

        json_response(
            StatusCode::OK,
            &OidcMutationResponse {
                success: true,
                message: "OIDC provider saved".to_string(),
                restart_required: true,
            },
        )
    }
}

pub struct DeleteOidcConfigHandler {}

#[async_trait::async_trait]
impl Operation for DeleteOidcConfigHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_oidc_config_request(&req, AdminAction::ConfigUpdateAdminAction).await?;

        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;
        if !is_valid_provider_id(provider_id) {
            return Err(s3_error!(InvalidRequest, "invalid provider_id"));
        }
        if is_env_managed_provider(provider_id) {
            return Err(s3_error!(AccessDenied, "provider is managed by environment variables"));
        }
        let provider_id = provider_id.to_owned();

        update_oidc_server_config(move |config| {
            delete_persisted_provider_config(config, &provider_id)?;
            Ok(())
        })
        .await?;

        json_response(
            StatusCode::OK,
            &OidcMutationResponse {
                success: true,
                message: "OIDC provider deleted".to_string(),
                restart_required: true,
            },
        )
    }
}

pub struct ValidateOidcConfigHandler {}

#[async_trait::async_trait]
impl Operation for ValidateOidcConfigHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_oidc_config_request(&req, AdminAction::ServerInfoAdminAction).await?;

        let request: OidcConfigValidateRequest = parse_json_body(&mut req).await?;
        let provider_id = if request.provider_id.trim().is_empty() {
            "default".to_string()
        } else {
            request.provider_id.trim().to_string()
        };
        let provider_config = build_provider_config_from_validate(request, &provider_id)?;
        let validation = rustfs_iam::oidc::validate_oidc_provider_config(&provider_config)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("validation failed: {e}")))?;

        json_response(
            StatusCode::OK,
            &OidcValidationResponse {
                valid: true,
                message: "OIDC configuration is valid".to_string(),
                issuer: Some(validation.issuer),
                authorization_endpoint: Some(validation.authorization_endpoint),
                token_endpoint: validation.token_endpoint,
            },
        )
    }
}

/// Handler: GET /rustfs/admin/v3/oidc/authorize/:provider_id
/// Generates PKCE challenge, stores state, and returns 302 redirect to IdP.
pub struct OidcAuthorizeHandler {}

#[async_trait::async_trait]
impl Operation for OidcAuthorizeHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;

        if !is_valid_provider_id(provider_id) {
            return Err(s3_error!(InvalidRequest, "invalid provider_id"));
        }

        let federation = current_federated_identity_service().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

        // Derive the callback redirect URI from the request
        let redirect_uri = derive_callback_uri(&req, provider_id)?;

        // Optional: redirect_after query parameter (must be a safe relative path)
        let redirect_after = extract_safe_redirect_after(&req.uri)?;
        let redirect_after_log = redirect_after.clone();

        let auth_url = federation
            .authorize_url(provider_id, &redirect_uri, redirect_after)
            .await
            .map_err(|e| {
                error!(
                    event = EVENT_ADMIN_OIDC_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_OIDC,
                    result = "authorize_url_failed",
                    provider_id = %provider_id,
                    redirect_uri = %redirect_uri,
                    redirect_after = ?redirect_after_log,
                    error = %e,
                    "admin oidc state"
                );
                S3Error::with_message(S3ErrorCode::InvalidRequest, format!("authorize failed: {e}"))
            })?;

        debug!(
            event = EVENT_ADMIN_OIDC_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_OIDC,
            provider_id = %provider_id,
            redirect_uri = %redirect_uri,
            redirect_after = ?redirect_after_log,
            auth_url = %auth_url,
            state = "authorize_redirect",
            "admin oidc state"
        );

        // Return 302 redirect
        let mut resp = S3Response::new((StatusCode::FOUND, Body::empty()));
        resp.headers.insert(
            http::header::LOCATION,
            auth_url
                .parse()
                .map_err(|_| s3_error!(InternalError, "failed to construct authorization URL"))?,
        );
        Ok(resp)
    }
}

/// Handler: GET /rustfs/admin/v3/oidc/callback/:provider_id?code=...&state=...
/// Exchanges authorization code for tokens, maps claims, issues STS credentials.
pub struct OidcCallbackHandler {}

#[async_trait::async_trait]
impl Operation for OidcCallbackHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;

        if !is_valid_provider_id(provider_id) {
            return Err(s3_error!(InvalidRequest, "invalid provider_id"));
        }

        // Check for error response from IdP
        if let Some((error, desc)) = extract_idp_callback_error(&req.uri) {
            warn!(
                event = EVENT_ADMIN_OIDC_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_OIDC,
                result = "idp_callback_error",
                provider_id = %provider_id,
                error_code = %error,
                error_description = %desc,
                "admin oidc state"
            );
            return Err(S3Error::with_message(
                S3ErrorCode::AccessDenied,
                format!("OIDC authentication failed: {error} - {desc}"),
            ));
        }

        // Extract code and state from query parameters
        let code =
            extract_query_param(&req.uri, "code").ok_or_else(|| s3_error!(InvalidRequest, "missing 'code' query parameter"))?;
        let state =
            extract_query_param(&req.uri, "state").ok_or_else(|| s3_error!(InvalidRequest, "missing 'state' query parameter"))?;

        let federation = current_federated_identity_service().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

        let redirect_uri = derive_callback_uri(&req, provider_id)?;

        let login = federation
            .complete_authorization_code(&state, &code, &redirect_uri, 3600, &DefaultFederatedSessionBinding)
            .await
            .map_err(|error| {
                if let FederationError::CodeExchange(message) = &error {
                    let lb_hint = if is_invalid_oidc_state_error(message) {
                        OIDC_STATE_LB_HINT
                    } else {
                        ""
                    };
                    error!(
                        event = EVENT_ADMIN_OIDC_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "code_exchange_failed",
                        requested_provider_id = %provider_id,
                        redirect_uri = %redirect_uri,
                        code_len = code.len(),
                        state_len = state.len(),
                        error = %message,
                        lb_hint = %lb_hint,
                        "admin oidc state"
                    );
                }
                callback_federation_error(error)
            })?;
        let authorization = &login.session.authorization;
        let actual_provider_id = &authorization.provider_id;

        debug!(
            event = EVENT_ADMIN_OIDC_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_OIDC,
            provider_id = %actual_provider_id,
            state = "authentication_succeeded",
            "admin oidc state"
        );

        debug!(
            event = EVENT_ADMIN_OIDC_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_OIDC,
            provider_id = %actual_provider_id,
            policy_count = authorization.policies.len(),
            group_count = authorization.groups.len(),
            policies = ?authorization.policies,
            groups = ?authorization.groups,
            state = "claims_mapped",
            "admin oidc state"
        );

        let new_cred = &login.session.credentials;

        // Build redirect URL to console with credentials in the fragment
        let console_redirect = build_console_redirect(
            &req,
            &new_cred.access_key,
            &new_cred.secret_key,
            &new_cred.session_token,
            new_cred.expiration,
            login.redirect_after.as_deref(),
            Some(login.logout_token.as_str()),
        )?;

        let mut resp = S3Response::new((StatusCode::FOUND, Body::empty()));
        resp.headers.insert(
            http::header::LOCATION,
            console_redirect
                .parse()
                .map_err(|_| s3_error!(InternalError, "failed to construct console redirect URL"))?,
        );
        Ok(resp)
    }
}

/// Handler: GET /rustfs/admin/v3/oidc/logout?logout_token=...
/// Consumes the logout token and redirects either to the IdP end-session URL
/// or back to the console login page when federated logout is unavailable.
pub struct OidcLogoutHandler {}

#[async_trait::async_trait]
impl Operation for OidcLogoutHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let fallback_location = build_console_login_redirect(&req)?;
        let Some(logout_token) = extract_query_param(&req.uri, "logout_token") else {
            return redirect_response(&fallback_location);
        };

        let location = match current_federated_identity_service() {
            Some(federation) => match federation.build_logout_url(&logout_token, &fallback_location).await {
                Ok(Some(url)) => url,
                Ok(None) => fallback_location.clone(),
                Err(err) => {
                    warn!(
                        event = EVENT_ADMIN_OIDC_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "logout_fallback_triggered",
                        error = %err,
                        "admin oidc state"
                    );
                    fallback_location.clone()
                }
            },
            None => fallback_location.clone(),
        };

        redirect_response(&location)
    }
}

/// Derive the OIDC callback URI.
/// Uses the provider's configured redirect_uri if set, otherwise derives dynamically
/// from request headers. For production deployments behind a reverse proxy, configuring
/// an explicit redirect_uri is recommended to prevent header manipulation.
fn derive_callback_uri(req: &S3Request<Body>, provider_id: &str) -> S3Result<String> {
    if let Some(federation) = current_federated_identity_service()
        && let Some(config) = federation.get_provider_config(provider_id)
    {
        return derive_callback_uri_with_provider_config(req, provider_id, Some(config));
    }

    derive_callback_uri_with_provider_config(req, provider_id, None)
}

fn derive_callback_uri_with_provider_config(
    req: &S3Request<Body>,
    provider_id: &str,
    config: Option<&rustfs_iam::oidc::OidcProviderConfig>,
) -> S3Result<String> {
    if let Some(config) = config {
        if let Some(ref uri) = config.redirect_uri {
            let parsed = Url::parse(uri).map_err(|_| s3_error!(InvalidRequest, "invalid configured redirect_uri"))?;
            if !is_valid_scheme(parsed.scheme()) || parsed.host_str().is_none() {
                return Err(s3_error!(InvalidRequest, "configured redirect_uri must be absolute http/https URL"));
            }
            return Ok(uri.clone());
        }

        if let Some(url) = browser_redirect_url(&oidc_callback_path(provider_id))? {
            return Ok(url);
        }

        if !config.redirect_uri_dynamic {
            return Err(s3_error!(
                InvalidRequest,
                "provider requires explicit redirect_uri because redirect_uri_dynamic is disabled"
            ));
        }
    }

    if let Some(url) = browser_redirect_url(&oidc_callback_path(provider_id))? {
        return Ok(url);
    }

    let scheme = extract_request_scheme(req)?;
    let host = extract_request_host(req)?;

    Ok(format!("{scheme}://{host}{}", oidc_callback_path(provider_id)))
}

fn oidc_callback_path(provider_id: &str) -> String {
    format!("{ADMIN_PREFIX}{OIDC_CALLBACK_SUFFIX}{provider_id}")
}

/// Extract a query parameter from the URI.
fn extract_query_param(uri: &http::Uri, key: &str) -> Option<String> {
    uri.query().and_then(|q| {
        // Parse query string manually without external dependency
        q.split('&')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let k = parts.next()?;
                let v = parts.next().unwrap_or("");
                if k == key {
                    Some(urlencoding::decode(v).unwrap_or_default().into_owned())
                } else {
                    None
                }
            })
            .next()
    })
}

fn extract_idp_callback_error(uri: &http::Uri) -> Option<(String, String)> {
    let error = extract_query_param(uri, "error")?;
    let desc = extract_query_param(uri, "error_description").unwrap_or_default();
    Some((error, desc))
}

fn extract_safe_redirect_after(uri: &http::Uri) -> S3Result<Option<String>> {
    let redirect_after = extract_query_param(uri, "redirect_after");
    match redirect_after {
        Some(value) if !is_safe_redirect_path(&value) => Err(s3_error!(InvalidRequest, "invalid redirect_after")),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

/// Build the console redirect URL with STS credentials in the hash fragment.
fn build_console_callback_fragment(
    access_key: &str,
    secret_key: &str,
    session_token: &str,
    expiration: Option<OffsetDateTime>,
    redirect_after: Option<&str>,
    logout_token: Option<&str>,
) -> String {
    let page = redirect_after.filter(|p| is_safe_redirect_path(p)).unwrap_or("/");
    let exp_str = expiration
        .map(|e| e.format(&time::format_description::well_known::Rfc3339).unwrap_or_default())
        .unwrap_or_default();

    let mut fragment = format!(
        "accessKey={}&secretKey={}&sessionToken={}&expiration={}&redirect={}",
        urlencoding::encode(access_key),
        urlencoding::encode(secret_key),
        urlencoding::encode(session_token),
        urlencoding::encode(&exp_str),
        urlencoding::encode(page),
    );

    if let Some(logout_token) = logout_token.filter(|value| !value.is_empty()) {
        fragment.push_str("&logoutToken=");
        fragment.push_str(&urlencoding::encode(logout_token));
    }

    fragment
}

/// Build the console redirect URL with STS credentials in the hash fragment.
fn build_console_redirect(
    req: &S3Request<Body>,
    access_key: &str,
    secret_key: &str,
    session_token: &str,
    expiration: Option<OffsetDateTime>,
    redirect_after: Option<&str>,
    logout_token: Option<&str>,
) -> S3Result<String> {
    let fragment =
        build_console_callback_fragment(access_key, secret_key, session_token, expiration, redirect_after, logout_token);

    let callback_path = format!("{CONSOLE_PREFIX}{CONSOLE_OIDC_CALLBACK_SUFFIX}");
    if let Some(base_url) = browser_redirect_url(&callback_path)? {
        return Ok(format!("{base_url}#{fragment}"));
    }

    let scheme = extract_request_scheme(req)?;
    let host = extract_request_host(req)?;

    Ok(format!("{scheme}://{host}{callback_path}#{fragment}"))
}

fn build_console_login_redirect(req: &S3Request<Body>) -> S3Result<String> {
    let login_path = format!("{CONSOLE_PREFIX}{CONSOLE_LOGIN_SUFFIX}");
    if let Some(url) = browser_redirect_url(&login_path)? {
        return Ok(url);
    }

    let scheme = extract_request_scheme(req)?;
    let host = extract_request_host(req)?;
    Ok(format!("{scheme}://{host}{login_path}"))
}

fn redirect_response(location: &str) -> S3Result<S3Response<(StatusCode, Body)>> {
    let mut resp = S3Response::new((StatusCode::FOUND, Body::empty()));
    resp.headers.insert(
        http::header::LOCATION,
        location
            .parse()
            .map_err(|_| s3_error!(InternalError, "failed to construct redirect URL"))?,
    );
    Ok(resp)
}

async fn authorize_oidc_config_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(action)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

async fn parse_json_body<T: DeserializeOwned>(req: &mut S3Request<Body>) -> S3Result<T> {
    let body = req
        .input
        .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
        .await
        .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

    if body.is_empty() {
        return Err(s3_error!(InvalidRequest, "request body is required"));
    }

    serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))
}

fn json_response<T: Serialize>(status: StatusCode, payload: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let body = serde_json::to_vec(payload)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize error: {e}")))?;

    let mut resp = S3Response::new((status, Body::from(body)));
    resp.headers
        .insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
    Ok(resp)
}

async fn load_server_config_from_store() -> S3Result<ServerConfig> {
    let store = oidc_config_store()?;

    read_admin_config_without_migrate(store)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to load server config: {e}")))
}

fn oidc_config_store() -> S3Result<std::sync::Arc<crate::admin::storage_api::runtime::ECStore>> {
    let context = current_app_context();
    current_object_store_handle_for_context(context.as_deref())
        .ok_or_else(|| s3_error!(InternalError, "storage layer not initialized"))
}

async fn update_oidc_server_config<F>(modifier: F) -> S3Result<()>
where
    F: FnOnce(&mut ServerConfig) -> S3Result<()> + Send + 'static,
{
    let store = oidc_config_store()?;
    supervise_admin_mutation("OIDC config update", async move {
        let snapshot = read_admin_server_config_snapshot(store.clone())
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to load server config: {e}")))?;
        let mut config = snapshot.config.clone();
        modifier(&mut config)?;
        save_admin_server_config_snapshot(store, &config, &snapshot)
            .await
            .map(|_| ())
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to save server config: {e}")))
    })
    .await
}

fn is_env_managed_provider(provider_id: &str) -> bool {
    rustfs_iam::oidc::load_oidc_provider_configs_from_env()
        .iter()
        .any(|config| config.id == provider_id)
}

fn provider_instance_key(provider_id: &str) -> String {
    if provider_id == "default" {
        DEFAULT_DELIMITER.to_string()
    } else {
        provider_id.to_string()
    }
}

fn oidc_restart_required(config: &ServerConfig) -> bool {
    let context = current_app_context();
    let active_config = current_server_config_for_context(context.as_deref());
    oidc_restart_required_from_active_config(config, active_config.as_ref())
}

fn oidc_restart_required_from_active_config(config: &ServerConfig, active_config: Option<&ServerConfig>) -> bool {
    rustfs_iam::oidc::load_effective_oidc_provider_configs(Some(config))
        != rustfs_iam::oidc::load_effective_oidc_provider_configs(active_config)
}

fn default_oidc_kvs() -> s3s::S3Result<rustfs_config::server_config::KVS> {
    ServerConfig::new()
        .get_value(IDENTITY_OPENID_SUB_SYS, DEFAULT_DELIMITER)
        .ok_or_else(|| s3_error!(InternalError, "default OIDC configuration missing"))
}

fn set_kvs_value(kvs: &mut rustfs_config::server_config::KVS, key: &str, value: String) {
    if let Some(existing) = kvs.0.iter_mut().find(|kv| kv.key == key) {
        existing.value = value;
        return;
    }

    kvs.insert(key.to_string(), value);
}

fn normalize_scopes(scopes: &[String]) -> Vec<String> {
    scopes
        .iter()
        .map(|scope| scope.trim().to_string())
        .filter(|scope| !scope.is_empty())
        .collect()
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn validate_absolute_http_url(value: &str, field_name: &str) -> S3Result<()> {
    let parsed = Url::parse(value).map_err(|_| s3_error!(InvalidRequest, "{} must be an absolute http/https URL", field_name))?;

    if !is_valid_scheme(parsed.scheme()) || parsed.host_str().is_none() {
        return Err(s3_error!(InvalidRequest, "{} must be an absolute http/https URL", field_name));
    }

    validate_outbound_url(&parsed).map_err(|err| s3_error!(InvalidRequest, "{} is not allowed: {}", field_name, err))?;

    Ok(())
}

fn validate_absolute_http_url_without_outbound_check(value: &str, field_name: &str) -> S3Result<()> {
    let parsed = Url::parse(value).map_err(|_| s3_error!(InvalidRequest, "{} must be an absolute http/https URL", field_name))?;

    if !is_valid_scheme(parsed.scheme()) || parsed.host_str().is_none() {
        return Err(s3_error!(InvalidRequest, "{} must be an absolute http/https URL", field_name));
    }

    Ok(())
}

fn validate_provider_config_fields(config: &rustfs_iam::oidc::OidcProviderConfig) -> S3Result<()> {
    if !is_valid_provider_id(&config.id) {
        return Err(s3_error!(InvalidRequest, "invalid provider_id"));
    }
    if config.config_url.trim().is_empty() {
        return Err(s3_error!(InvalidRequest, "config_url is required"));
    }
    validate_absolute_http_url(&config.config_url, "config_url")?;
    if let Some(issuer) = config.issuer.as_deref() {
        validate_absolute_http_url_without_outbound_check(issuer, "issuer")?;
    }

    if config.client_id.trim().is_empty() {
        return Err(s3_error!(InvalidRequest, "client_id is required"));
    }

    if !config.redirect_uri_dynamic {
        let redirect_uri = config
            .redirect_uri
            .as_deref()
            .ok_or_else(|| s3_error!(InvalidRequest, "redirect_uri is required when redirect_uri_dynamic is off"))?;
        validate_absolute_http_url(redirect_uri, "redirect_uri")?;
    } else if let Some(redirect_uri) = config.redirect_uri.as_deref() {
        validate_absolute_http_url(redirect_uri, "redirect_uri")?;
    }

    if !config.scopes.iter().any(|scope| scope == "openid") {
        return Err(s3_error!(InvalidRequest, "scopes must include openid"));
    }

    Ok(())
}

fn or_default(value: &str, default: &str) -> String {
    if value.trim().is_empty() {
        default.to_string()
    } else {
        value.trim().to_string()
    }
}

/// Normalize an `OidcProviderConfig` by trimming strings and applying defaults.
fn normalize_provider_config(mut config: rustfs_iam::oidc::OidcProviderConfig) -> rustfs_iam::oidc::OidcProviderConfig {
    config.config_url = config.config_url.trim().to_string();
    config.issuer = normalize_optional(config.issuer);
    config.client_id = config.client_id.trim().to_string();
    config.scopes = normalize_scopes(&config.scopes);
    config.redirect_uri = normalize_optional(config.redirect_uri);
    config.claim_name = or_default(&config.claim_name, OIDC_DEFAULT_CLAIM_NAME);
    config.claim_prefix = config.claim_prefix.trim().to_string();
    config.role_policy = config.role_policy.trim().to_string();
    config.display_name = or_default(&config.display_name, &config.id);
    config.groups_claim = or_default(&config.groups_claim, OIDC_DEFAULT_GROUPS_CLAIM);
    config.roles_claim = or_default(&config.roles_claim, OIDC_DEFAULT_ROLES_CLAIM);
    config.email_claim = or_default(&config.email_claim, OIDC_DEFAULT_EMAIL_CLAIM);
    config.username_claim = or_default(&config.username_claim, OIDC_DEFAULT_USERNAME_CLAIM);
    config
}

fn build_provider_config_from_upsert(
    provider_id: &str,
    request: OidcConfigUpsertRequest,
    existing_secret: Option<String>,
) -> S3Result<rustfs_iam::oidc::OidcProviderConfig> {
    let client_secret = match request.client_secret {
        Some(value) if !value.trim().is_empty() => Some(value),
        _ => existing_secret.filter(|value| !value.trim().is_empty()),
    };

    let config = normalize_provider_config(rustfs_iam::oidc::OidcProviderConfig {
        id: provider_id.to_string(),
        enabled: request.enabled,
        config_url: request.config_url,
        issuer: request.issuer,
        client_id: request.client_id,
        client_secret,
        scopes: request.scopes,
        other_audiences: request.other_audiences,
        redirect_uri: request.redirect_uri,
        redirect_uri_dynamic: request.redirect_uri_dynamic,
        claim_name: request.claim_name,
        claim_prefix: request.claim_prefix,
        role_policy: request.role_policy,
        display_name: request.display_name,
        groups_claim: request.groups_claim,
        roles_claim: request.roles_claim,
        email_claim: request.email_claim,
        username_claim: request.username_claim,
        hide_from_ui: request.hide_from_ui,
    });

    validate_provider_config_fields(&config)?;
    Ok(config)
}

fn build_provider_config_from_validate(
    request: OidcConfigValidateRequest,
    provider_id: &str,
) -> S3Result<rustfs_iam::oidc::OidcProviderConfig> {
    let config = normalize_provider_config(rustfs_iam::oidc::OidcProviderConfig {
        id: provider_id.to_string(),
        enabled: request.enabled,
        config_url: request.config_url,
        issuer: request.issuer,
        client_id: request.client_id,
        client_secret: request.client_secret.filter(|value| !value.trim().is_empty()),
        scopes: request.scopes,
        other_audiences: request.other_audiences,
        redirect_uri: request.redirect_uri,
        redirect_uri_dynamic: request.redirect_uri_dynamic,
        claim_name: request.claim_name,
        claim_prefix: request.claim_prefix,
        role_policy: request.role_policy,
        display_name: request.display_name,
        groups_claim: request.groups_claim,
        roles_claim: request.roles_claim,
        email_claim: request.email_claim,
        username_claim: request.username_claim,
        hide_from_ui: request.hide_from_ui,
    });

    validate_provider_config_fields(&config)?;
    Ok(config)
}

fn persisted_provider_secret(config: &ServerConfig, provider_id: &str) -> Option<String> {
    config
        .0
        .get(IDENTITY_OPENID_SUB_SYS)
        .and_then(|subsystem| subsystem.get(&provider_instance_key(provider_id)))
        .and_then(|kvs| kvs.lookup(OIDC_CLIENT_SECRET))
        .filter(|value| !value.trim().is_empty())
}

fn upsert_persisted_provider_config(config: &mut ServerConfig, provider_config: &rustfs_iam::oidc::OidcProviderConfig) {
    let instance_key = provider_instance_key(&provider_config.id);
    let mut kvs = default_oidc_kvs().unwrap_or_default();

    set_kvs_value(
        &mut kvs,
        ENABLE_KEY,
        if provider_config.enabled {
            EnableState::On.to_string()
        } else {
            EnableState::Off.to_string()
        },
    );
    set_kvs_value(&mut kvs, OIDC_CONFIG_URL, provider_config.config_url.clone());
    set_kvs_value(&mut kvs, OIDC_ISSUER, provider_config.issuer.clone().unwrap_or_default());
    set_kvs_value(&mut kvs, OIDC_CLIENT_ID, provider_config.client_id.clone());
    set_kvs_value(&mut kvs, OIDC_CLIENT_SECRET, provider_config.client_secret.clone().unwrap_or_default());
    set_kvs_value(&mut kvs, OIDC_SCOPES, provider_config.scopes.join(","));
    set_kvs_value(&mut kvs, OIDC_OTHER_AUDIENCES, provider_config.other_audiences.join(","));
    set_kvs_value(&mut kvs, OIDC_REDIRECT_URI, provider_config.redirect_uri.clone().unwrap_or_default());
    set_kvs_value(
        &mut kvs,
        OIDC_REDIRECT_URI_DYNAMIC,
        if provider_config.redirect_uri_dynamic {
            EnableState::On.to_string()
        } else {
            EnableState::Off.to_string()
        },
    );
    set_kvs_value(&mut kvs, OIDC_CLAIM_NAME, provider_config.claim_name.clone());
    set_kvs_value(&mut kvs, OIDC_CLAIM_PREFIX, provider_config.claim_prefix.clone());
    set_kvs_value(&mut kvs, OIDC_ROLE_POLICY, provider_config.role_policy.clone());
    set_kvs_value(&mut kvs, OIDC_DISPLAY_NAME, provider_config.display_name.clone());
    set_kvs_value(&mut kvs, OIDC_GROUPS_CLAIM, provider_config.groups_claim.clone());
    set_kvs_value(&mut kvs, OIDC_ROLES_CLAIM, provider_config.roles_claim.clone());
    set_kvs_value(&mut kvs, OIDC_EMAIL_CLAIM, provider_config.email_claim.clone());
    set_kvs_value(&mut kvs, OIDC_USERNAME_CLAIM, provider_config.username_claim.clone());
    set_kvs_value(
        &mut kvs,
        OIDC_HIDE_FROM_UI,
        if provider_config.hide_from_ui {
            EnableState::On.to_string()
        } else {
            EnableState::Off.to_string()
        },
    );

    config
        .0
        .entry(IDENTITY_OPENID_SUB_SYS.to_string())
        .or_default()
        .insert(instance_key, kvs);
}

fn delete_persisted_provider_config(config: &mut ServerConfig, provider_id: &str) -> S3Result<()> {
    let Some(subsystem) = config.0.get_mut(IDENTITY_OPENID_SUB_SYS) else {
        return Err(s3_error!(InvalidRequest, "provider not found"));
    };

    if subsystem.remove(&provider_instance_key(provider_id)).is_none() {
        return Err(s3_error!(InvalidRequest, "provider not found"));
    }

    if subsystem.is_empty() {
        config.0.remove(IDENTITY_OPENID_SUB_SYS);
    }

    Ok(())
}

fn extract_request_scheme(req: &S3Request<Body>) -> S3Result<String> {
    let raw_scheme = req
        .headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(',').next().map(str::trim).unwrap_or(""))
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| req.uri.scheme_str().map(str::to_owned))
        .unwrap_or_else(|| "http".to_owned())
        .to_ascii_lowercase();

    if !is_valid_scheme(&raw_scheme) {
        return Err(s3_error!(InvalidRequest, "invalid scheme in request"));
    }

    Ok(raw_scheme)
}

fn extract_request_host(req: &S3Request<Body>) -> S3Result<String> {
    let host = req
        .headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let v = v.trim();
            if v.is_empty() { None } else { Some(v.to_owned()) }
        })
        .or_else(|| req.uri.authority().map(|a| a.as_str().to_owned()))
        .ok_or_else(|| s3_error!(InvalidRequest, "cannot determine host for redirect URI"))?;

    parse_host_authority(&host)
}

fn parse_host_authority(raw_host: &str) -> S3Result<String> {
    let host = raw_host.trim();
    if host.is_empty() {
        return Err(s3_error!(InvalidRequest, "invalid host header"));
    }

    // Parse as authority to normalize and validate, while rejecting URL-style
    // constructions (userinfo, query, fragment, and explicit paths).
    let parsed = Url::parse(&format!("http://{host}")).map_err(|_| s3_error!(InvalidRequest, "invalid host header"))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(s3_error!(InvalidRequest, "invalid host header"));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(s3_error!(InvalidRequest, "invalid host header"));
    }
    if parsed.path() != "/" {
        return Err(s3_error!(InvalidRequest, "invalid host"));
    }

    Ok(parsed.authority().to_string())
}

fn browser_redirect_url(path: &str) -> S3Result<Option<String>> {
    let Some(base) = browser_redirect_base()? else {
        return Ok(None);
    };
    Ok(Some(format!("{base}{path}")))
}

fn browser_redirect_base() -> S3Result<Option<String>> {
    let Some(raw) = rustfs_utils::get_env_opt_str(ENV_RUSTFS_BROWSER_REDIRECT_URL) else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    validate_browser_redirect_base(trimmed).map(Some)
}

fn validate_browser_redirect_base(raw_url: &str) -> S3Result<String> {
    let parsed = Url::parse(raw_url).map_err(|_| s3_error!(InvalidRequest, "invalid browser redirect URL"))?;
    if !is_valid_scheme(parsed.scheme()) || parsed.host_str().is_none() {
        return Err(s3_error!(InvalidRequest, "browser redirect URL must be an absolute http/https URL"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(s3_error!(InvalidRequest, "browser redirect URL must not contain userinfo"));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(s3_error!(InvalidRequest, "browser redirect URL must not contain query or fragment"));
    }
    if !matches!(parsed.path(), "" | "/") {
        return Err(s3_error!(InvalidRequest, "browser redirect URL must not contain a path"));
    }

    Ok(format!("{}://{}", parsed.scheme(), parsed.authority()))
}

fn is_invalid_oidc_state_error(error: &str) -> bool {
    error.contains("invalid or expired OIDC state")
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Extensions, HeaderMap, HeaderValue, Uri};
    use temp_env::with_var;

    fn build_oidc_request(
        uri: &'static str,
        host: Option<&'static str>,
        forwarded_proto: Option<&'static str>,
    ) -> S3Request<Body> {
        let mut headers = HeaderMap::new();
        if let Some(host) = host {
            headers.insert(http::header::HOST, HeaderValue::from_static(host));
        }
        if let Some(proto) = forwarded_proto {
            headers.insert("x-forwarded-proto", HeaderValue::from_static(proto));
        }

        S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: Uri::from_static(uri),
            headers,
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn test_provider_config(redirect_uri: Option<&str>, redirect_uri_dynamic: bool) -> rustfs_iam::oidc::OidcProviderConfig {
        rustfs_iam::oidc::OidcProviderConfig {
            id: "default".to_string(),
            enabled: true,
            config_url: "https://idp.example.com/.well-known/openid-configuration".to_string(),
            issuer: None,
            client_id: "rustfs-console".to_string(),
            client_secret: None,
            scopes: vec!["openid".to_string()],
            other_audiences: Vec::new(),
            redirect_uri: redirect_uri.map(ToString::to_string),
            redirect_uri_dynamic,
            claim_name: OIDC_DEFAULT_CLAIM_NAME.to_string(),
            claim_prefix: String::new(),
            role_policy: String::new(),
            display_name: "default".to_string(),
            groups_claim: OIDC_DEFAULT_GROUPS_CLAIM.to_string(),
            roles_claim: OIDC_DEFAULT_ROLES_CLAIM.to_string(),
            email_claim: OIDC_DEFAULT_EMAIL_CLAIM.to_string(),
            username_claim: OIDC_DEFAULT_USERNAME_CLAIM.to_string(),
            hide_from_ui: false,
        }
    }

    #[test]
    fn test_is_oidc_path() {
        assert!(is_oidc_path("/rustfs/admin/v3/oidc/providers"));
        assert!(is_oidc_path("/rustfs/admin/v3/oidc/authorize/okta"));
        assert!(is_oidc_path("/rustfs/admin/v3/oidc/callback/okta"));
        assert!(is_oidc_path("/minio/admin/v3/oidc/providers"));
        assert!(is_oidc_path("/minio/admin/v3/oidc/authorize/okta"));
        assert!(is_oidc_path("/minio/admin/v3/oidc/callback/okta"));
        assert!(!is_oidc_path("/rustfs/admin/v3/oidc/config"));
        assert!(!is_oidc_path("/rustfs/admin/v3/oidc/config/default"));
        assert!(!is_oidc_path("/rustfs/admin/v3/oidc/validate"));
        assert!(!is_oidc_path("/minio/admin/v3/oidc/config"));
        assert!(!is_oidc_path("/rustfs/admin/v3/users"));
        assert!(!is_oidc_path("/health"));
    }

    #[test]
    fn test_extract_query_param() {
        let uri: http::Uri = "http://localhost/callback?code=abc123&state=xyz789".parse().unwrap();
        assert_eq!(extract_query_param(&uri, "code"), Some("abc123".to_string()));
        assert_eq!(extract_query_param(&uri, "state"), Some("xyz789".to_string()));
        assert_eq!(extract_query_param(&uri, "missing"), None);
    }

    #[test]
    fn callback_errors_preserve_existing_s3_semantics() {
        let cases = [
            (
                FederationError::CodeExchange("invalid state".to_string()),
                S3ErrorCode::AccessDenied,
                "code exchange failed: invalid state",
            ),
            (
                FederationError::Logout("session unavailable".to_string()),
                S3ErrorCode::InternalError,
                "logout session creation failed: session unavailable",
            ),
            (
                FederationError::Binding(FederatedSessionBindingError::InvalidRequest("invalid policy".to_string())),
                S3ErrorCode::InvalidRequest,
                "invalid policy",
            ),
            (
                FederationError::Binding(FederatedSessionBindingError::Internal("failed to store temp user".to_string())),
                S3ErrorCode::InternalError,
                "failed to store temp user",
            ),
        ];

        for (error, expected_code, expected_message) in cases {
            let error = callback_federation_error(error);
            assert_eq!(error.code(), &expected_code);
            assert_eq!(error.message(), Some(expected_message));
        }
    }

    #[test]
    fn test_extract_query_param_empty() {
        let uri: http::Uri = "http://localhost/callback".parse().unwrap();
        assert_eq!(extract_query_param(&uri, "code"), None);
    }

    #[test]
    fn test_extract_query_param_encoded() {
        let uri: http::Uri = "http://localhost/callback?redirect_after=%2Fdashboard".parse().unwrap();
        assert_eq!(extract_query_param(&uri, "redirect_after"), Some("/dashboard".to_string()));
    }

    #[test]
    fn test_extract_idp_callback_error_without_code() {
        let uri: http::Uri = "http://localhost/callback?error=access_denied&error_description=Denied%20by%20IdP&state=xyz789"
            .parse()
            .expect("valid callback URI should parse");

        let (error, desc) = extract_idp_callback_error(&uri).expect("IdP callback error should be detected");
        assert_eq!(error, "access_denied");
        assert_eq!(desc, "Denied by IdP");
        assert_eq!(extract_query_param(&uri, "code"), None);
    }

    #[test]
    fn test_parse_host_authority_rejects_userinfo() {
        assert!(parse_host_authority("evil.com@victim.com").is_err());
    }

    #[test]
    fn test_parse_host_authority_rejects_query_fragment() {
        assert!(parse_host_authority("example.com?x=y").is_err());
        assert!(parse_host_authority("example.com#fragment").is_err());
    }

    #[test]
    fn test_parse_host_authority_rejects_path() {
        assert!(parse_host_authority("example.com/path").is_err());
    }

    #[test]
    fn test_parse_host_authority_accepts_valid_host_with_port() {
        assert_eq!(
            parse_host_authority("example.com:8443").expect("valid host should pass"),
            "example.com:8443"
        );
    }

    #[test]
    fn test_extract_safe_redirect_after() {
        let uri: http::Uri = "http://localhost/callback?redirect_after=%2Fdashboard".parse().unwrap();
        assert_eq!(
            extract_safe_redirect_after(&uri).expect("valid redirect should pass"),
            Some("/dashboard".to_string())
        );

        let uri: http::Uri = "http://localhost/callback?redirect_after=javascript:alert(1)"
            .parse()
            .unwrap();
        assert!(extract_safe_redirect_after(&uri).is_err());
    }

    #[test]
    fn test_build_console_callback_fragment_includes_logout_token() {
        let fragment =
            build_console_callback_fragment("access", "secret", "token", None, Some("/dashboard"), Some("logout-token"));

        assert!(fragment.contains("accessKey=access"));
        assert!(fragment.contains("redirect=%2Fdashboard"));
        assert!(fragment.contains("logoutToken=logout-token"));
    }

    #[test]
    fn test_validate_browser_redirect_base_accepts_origin() {
        assert_eq!(
            validate_browser_redirect_base("https://console.example.com/").expect("browser redirect origin should be valid"),
            "https://console.example.com"
        );
        assert_eq!(
            validate_browser_redirect_base("http://20.78.1.4:9000").expect("browser redirect origin should be valid"),
            "http://20.78.1.4:9000"
        );
    }

    #[test]
    fn test_validate_browser_redirect_base_rejects_unsafe_parts() {
        assert!(validate_browser_redirect_base("ftp://console.example.com").is_err());
        assert!(validate_browser_redirect_base("https://user:pass@console.example.com").is_err());
        assert!(validate_browser_redirect_base("https://console.example.com/proxy").is_err());
        assert!(validate_browser_redirect_base("https://console.example.com?next=/").is_err());
        assert!(validate_browser_redirect_base("https://console.example.com/#fragment").is_err());
    }

    #[test]
    fn test_derive_callback_uri_uses_browser_redirect_url() {
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/authorize/default", Some("internal:9000"), None);

        let callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("https://console.example.com"), || {
            derive_callback_uri(&req, "default").expect("callback URI should use browser redirect URL")
        });

        assert_eq!(callback, "https://console.example.com/rustfs/admin/v3/oidc/callback/default");
    }

    #[test]
    fn test_derive_callback_uri_falls_back_to_request_headers() {
        let req = build_oidc_request(
            "http://internal/rustfs/admin/v3/oidc/authorize/default",
            Some("internal:9000"),
            Some("https"),
        );

        let callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            derive_callback_uri(&req, "default").expect("callback URI should fall back to request headers")
        });

        assert_eq!(callback, "https://internal:9000/rustfs/admin/v3/oidc/callback/default");
    }

    #[test]
    fn test_derive_callback_uri_configured_redirect_uri_wins() {
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/authorize/default", Some("internal:9000"), None);
        let config = test_provider_config(Some("https://configured.example.com/rustfs/admin/v3/oidc/callback/default"), false);

        let callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("https://console.example.com"), || {
            derive_callback_uri_with_provider_config(&req, "default", Some(&config))
                .expect("configured redirect_uri should be preferred")
        });

        assert_eq!(callback, "https://configured.example.com/rustfs/admin/v3/oidc/callback/default");
    }

    #[test]
    fn test_derive_callback_uri_browser_redirect_url_satisfies_static_provider() {
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/authorize/default", Some("internal:9000"), None);
        let config = test_provider_config(None, false);

        let callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("https://console.example.com"), || {
            derive_callback_uri_with_provider_config(&req, "default", Some(&config))
                .expect("browser redirect URL should satisfy a non-dynamic provider")
        });

        assert_eq!(callback, "https://console.example.com/rustfs/admin/v3/oidc/callback/default");
    }

    #[test]
    fn test_derive_callback_uri_static_provider_requires_redirect_source() {
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/authorize/default", Some("internal:9000"), None);
        let config = test_provider_config(None, false);

        let err = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            derive_callback_uri_with_provider_config(&req, "default", Some(&config))
                .expect_err("non-dynamic provider without redirect source should fail")
        });

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_derive_callback_uri_dynamic_provider_falls_back_to_request_headers() {
        let req = build_oidc_request(
            "http://internal/rustfs/admin/v3/oidc/authorize/default",
            Some("internal:9000"),
            Some("https"),
        );
        let config = test_provider_config(None, true);

        let callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            derive_callback_uri_with_provider_config(&req, "default", Some(&config))
                .expect("dynamic provider should fall back to request headers")
        });

        assert_eq!(callback, "https://internal:9000/rustfs/admin/v3/oidc/callback/default");
    }

    #[test]
    fn test_build_console_redirect_uses_browser_redirect_url() {
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/callback/default", Some("internal:9000"), None);

        let redirect = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("https://console.example.com/"), || {
            build_console_redirect(&req, "access", "secret", "token", None, Some("/buckets"), Some("logout-token"))
                .expect("console redirect should use browser redirect URL")
        });

        assert!(redirect.starts_with("https://console.example.com/rustfs/console/auth/oidc-callback/#"));
        assert!(redirect.contains("redirect=%2Fbuckets"));
        assert!(redirect.contains("logoutToken=logout-token"));
    }

    #[test]
    fn test_build_console_login_redirect_uses_browser_redirect_url() {
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/logout", Some("internal:9000"), None);

        let redirect = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("https://console.example.com"), || {
            build_console_login_redirect(&req).expect("login redirect should use browser redirect URL")
        });

        assert_eq!(redirect, "https://console.example.com/rustfs/console/auth/login");
    }

    #[test]
    fn test_is_oidc_path_includes_logout() {
        assert!(is_oidc_path("/rustfs/admin/v3/oidc/logout"));
        assert!(is_oidc_path("/minio/admin/v3/oidc/logout"));
    }

    #[test]
    fn test_is_valid_provider_id() {
        assert!(is_valid_provider_id("AUTHENTIK"));
        assert!(is_valid_provider_id("my-provider"));
        assert!(is_valid_provider_id("okta_prod"));
        assert!(is_valid_provider_id("Azure123"));
        assert!(!is_valid_provider_id(""));
        assert!(!is_valid_provider_id("../evil"));
        assert!(!is_valid_provider_id("foo bar"));
        assert!(!is_valid_provider_id("foo/bar"));
        assert!(!is_valid_provider_id("provider;drop"));
    }

    #[test]
    fn test_is_safe_redirect_path() {
        assert!(is_safe_redirect_path("/"));
        assert!(is_safe_redirect_path("/dashboard"));
        assert!(is_safe_redirect_path("/buckets/my-bucket"));
        assert!(!is_safe_redirect_path("https://evil.com"));
        assert!(!is_safe_redirect_path("javascript:alert(1)"));
        assert!(!is_safe_redirect_path("//evil.com/path"));
        assert!(!is_safe_redirect_path("relative/path"));
        assert!(!is_safe_redirect_path(""));
    }

    #[test]
    fn test_is_valid_scheme() {
        assert!(is_valid_scheme("http"));
        assert!(is_valid_scheme("https"));
        assert!(!is_valid_scheme("ftp"));
        assert!(!is_valid_scheme("javascript"));
        assert!(!is_valid_scheme(""));
    }

    #[test]
    fn test_validate_absolute_http_url_rejects_loopback_targets() {
        let err = validate_absolute_http_url("https://127.0.0.1/.well-known/openid-configuration", "config_url")
            .expect_err("loopback config URL should be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.message().unwrap_or_default().contains("not allowed"));
    }

    #[test]
    fn test_provider_instance_key() {
        assert_eq!(provider_instance_key("default"), "_");
        assert_eq!(provider_instance_key("okta"), "okta");
    }

    #[test]
    fn test_build_provider_config_requires_openid_scope() {
        let req = OidcConfigUpsertRequest {
            scopes: vec!["profile".to_string()],
            config_url: "https://example.com/.well-known/openid-configuration".to_string(),
            client_id: "client-id".to_string(),
            ..Default::default()
        };

        assert!(build_provider_config_from_upsert("default", req, None).is_err());
    }

    #[test]
    fn test_build_provider_config_preserves_existing_secret_when_request_is_empty() {
        let req = OidcConfigUpsertRequest {
            config_url: "https://example.com/.well-known/openid-configuration".to_string(),
            client_id: "client-id".to_string(),
            client_secret: Some("".to_string()),
            ..Default::default()
        };

        let config =
            build_provider_config_from_upsert("default", req, Some("existing-secret".to_string())).expect("config should build");

        assert_eq!(config.client_secret.as_deref(), Some("existing-secret"));
        assert_eq!(config.roles_claim, OIDC_DEFAULT_ROLES_CLAIM);
    }

    #[test]
    fn test_oidc_config_upsert_request_rejects_unknown_fields() {
        let err = serde_json::from_str::<OidcConfigUpsertRequest>(
            r#"{"config_url":"https://example.com/.well-known/openid-configuration","client_id":"client","unexpected_field":true}"#,
        )
        .expect_err("unknown upsert field should fail");

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn test_oidc_config_validate_request_rejects_unknown_fields() {
        let err = serde_json::from_str::<OidcConfigValidateRequest>(
            r#"{"provider_id":"default","config_url":"https://example.com/.well-known/openid-configuration","client_id":"client","unexpected_field":true}"#,
        )
        .expect_err("unknown validate field should fail");

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn test_build_provider_config_uses_custom_roles_claim() {
        let req = OidcConfigUpsertRequest {
            config_url: "https://example.com/.well-known/openid-configuration".to_string(),
            client_id: "client-id".to_string(),
            roles_claim: "app_roles".to_string(),
            ..Default::default()
        };

        let config = build_provider_config_from_upsert("default", req, None).expect("config should build");
        assert_eq!(config.roles_claim, "app_roles");
    }

    #[test]
    fn test_oidc_restart_required_detects_persisted_changes() {
        let active_config = ServerConfig::new();
        let mut persisted_config = ServerConfig::new();
        let provider_config = rustfs_iam::oidc::OidcProviderConfig {
            id: "default".to_string(),
            enabled: true,
            config_url: "https://example.com/.well-known/openid-configuration".to_string(),
            issuer: None,
            client_id: "console".to_string(),
            client_secret: Some("secret".to_string()),
            scopes: vec!["openid".to_string(), "profile".to_string()],
            other_audiences: vec![],
            redirect_uri: None,
            redirect_uri_dynamic: true,
            claim_name: OIDC_DEFAULT_CLAIM_NAME.to_string(),
            claim_prefix: String::new(),
            role_policy: String::new(),
            display_name: "default".to_string(),
            groups_claim: OIDC_DEFAULT_GROUPS_CLAIM.to_string(),
            roles_claim: OIDC_DEFAULT_ROLES_CLAIM.to_string(),
            email_claim: OIDC_DEFAULT_EMAIL_CLAIM.to_string(),
            username_claim: OIDC_DEFAULT_USERNAME_CLAIM.to_string(),
            hide_from_ui: false,
        };

        upsert_persisted_provider_config(&mut persisted_config, &provider_config);

        assert!(oidc_restart_required_from_active_config(&persisted_config, Some(&active_config)));
        assert!(!oidc_restart_required_from_active_config(&persisted_config, Some(&persisted_config)));
    }

    #[test]
    fn test_upsert_persists_hide_from_ui_on() {
        let mut config = ServerConfig::new();
        let mut provider_config = rustfs_iam::oidc::OidcProviderConfig {
            id: "kubernetes".to_string(),
            enabled: true,
            config_url: "https://example.com/.well-known/openid-configuration".to_string(),
            issuer: None,
            client_id: "test".to_string(),
            client_secret: None,
            scopes: vec!["openid".to_string()],
            other_audiences: vec![],
            redirect_uri: None,
            redirect_uri_dynamic: true,
            claim_name: "sub".to_string(),
            claim_prefix: String::new(),
            role_policy: String::new(),
            display_name: "Kubernetes".to_string(),
            groups_claim: OIDC_DEFAULT_GROUPS_CLAIM.to_string(),
            roles_claim: OIDC_DEFAULT_ROLES_CLAIM.to_string(),
            email_claim: OIDC_DEFAULT_EMAIL_CLAIM.to_string(),
            username_claim: OIDC_DEFAULT_USERNAME_CLAIM.to_string(),
            hide_from_ui: true,
        };

        upsert_persisted_provider_config(&mut config, &provider_config);

        let kvs = config
            .0
            .get(IDENTITY_OPENID_SUB_SYS)
            .and_then(|m| m.get("kubernetes"))
            .expect("provider KVS should exist");
        assert_eq!(kvs.get(OIDC_HIDE_FROM_UI), EnableState::On.to_string());

        // Flip to false and verify
        provider_config.hide_from_ui = false;
        upsert_persisted_provider_config(&mut config, &provider_config);

        let kvs = config
            .0
            .get(IDENTITY_OPENID_SUB_SYS)
            .and_then(|m| m.get("kubernetes"))
            .expect("provider KVS should exist");
        assert_eq!(kvs.get(OIDC_HIDE_FROM_UI), EnableState::Off.to_string());
    }

    #[test]
    fn test_upsert_persists_issuer() {
        let mut config = ServerConfig::new();
        let provider_config = rustfs_iam::oidc::OidcProviderConfig {
            id: "kubernetes".to_string(),
            enabled: true,
            config_url: "http://keycloak.ns.svc.cluster.local:8080/realms/app/.well-known/openid-configuration".to_string(),
            issuer: Some("https://app.local/realms/app".to_string()),
            client_id: "test".to_string(),
            client_secret: None,
            scopes: vec!["openid".to_string()],
            other_audiences: vec![],
            redirect_uri: None,
            redirect_uri_dynamic: true,
            claim_name: "sub".to_string(),
            claim_prefix: String::new(),
            role_policy: String::new(),
            display_name: "Kubernetes".to_string(),
            groups_claim: OIDC_DEFAULT_GROUPS_CLAIM.to_string(),
            roles_claim: OIDC_DEFAULT_ROLES_CLAIM.to_string(),
            email_claim: OIDC_DEFAULT_EMAIL_CLAIM.to_string(),
            username_claim: OIDC_DEFAULT_USERNAME_CLAIM.to_string(),
            hide_from_ui: false,
        };

        upsert_persisted_provider_config(&mut config, &provider_config);

        let kvs = config
            .0
            .get(IDENTITY_OPENID_SUB_SYS)
            .and_then(|m| m.get("kubernetes"))
            .expect("provider KVS should exist");
        assert_eq!(kvs.get(OIDC_ISSUER), "https://app.local/realms/app");
    }
}
