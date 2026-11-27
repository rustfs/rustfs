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

use crate::admin::auth::authorize_admin_envelope;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::router::{AdminOperation, S3Router};
use crate::admin::runtime_sources::current_federated_identity_service;
use crate::admin::service::federated_identity::DefaultFederatedSessionBinding;
use crate::admin::service::oidc_config;
use crate::admin::utils::owned_json_response;
use crate::auth::constant_time_eq;
use crate::server::rate_limit::{RateLimitDecision, RateLimitQuota, RateLimiter, ThrottleInfo, apply_throttle_headers};
use crate::server::{ADMIN_PREFIX, MINIO_ADMIN_PREFIX, console_prefix};
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use rustfs_config::{ENV_RUSTFS_BROWSER_REDIRECT_URL, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_iam::federation::{FederatedSessionBindingError, FederationError};
use rustfs_iam::oidc::{OidcProviderConfigInput, OidcProviderValidationInput, console_session_duration_secs};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::{Body, S3Error, S3ErrorCode, S3Result, s3_error};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::net::IpAddr;
use std::sync::LazyLock;
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
/// Cookie that binds an in-flight login to the browser that started it. It carries the
/// `state` issued by the authorize handler and is scoped to the callback URL.
const OIDC_STATE_COOKIE: &str = "rustfs_oidc_state";
/// Matches the TTL of the in-flight state store in `rustfs_iam::oidc`.
const OIDC_STATE_COOKIE_MAX_AGE_SECS: u64 = 300;
const OIDC_STATE_COOKIE_MISMATCH: &str =
    "OIDC login was not started by this browser; start the login again and use the same host for authorize and callback";
/// Per-client-IP rate limit on `/oidc/authorize/{provider_id}` so a single unauthenticated
/// client cannot roll over the 10,000-entry OIDC state store during another user's IdP
/// round-trip even under LRU eviction. Shared across S3 and Console listeners and bounded in
/// memory by `RateLimiter`'s sharded cap and idle-bucket sweep.
const OIDC_AUTHORIZE_RATE_LIMIT_RPM: u32 = 120;
const OIDC_AUTHORIZE_RATE_LIMIT_BURST: u32 = 30;
static OIDC_AUTHORIZE_RATE_LIMITER: LazyLock<RateLimiter<Option<IpAddr>>> = LazyLock::new(|| {
    RateLimiter::new(RateLimitQuota {
        requests_per_minute: OIDC_AUTHORIZE_RATE_LIMIT_RPM,
        burst: OIDC_AUTHORIZE_RATE_LIMIT_BURST,
    })
});

fn oidc_authorize_client_ip(req: &AdminRequest) -> Option<IpAddr> {
    req.policy_source_addr().map(|addr| addr.ip().to_canonical())
}

fn check_oidc_authorize_rate_limit(
    limiter: &RateLimiter<Option<IpAddr>>,
    req: &AdminRequest,
    provider_id: &str,
) -> Option<AdminResponse> {
    let client_ip = oidc_authorize_client_ip(req);
    match limiter.check(&client_ip) {
        RateLimitDecision::Allowed => None,
        RateLimitDecision::Limited(throttle) => {
            warn!(
                event = EVENT_ADMIN_OIDC_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_OIDC,
                result = "authorize_rate_limited",
                provider_id = %provider_id,
                retry_after_secs = throttle.retry_after_secs,
                "admin oidc state"
            );
            Some(oidc_authorize_rate_limited_response(limiter.quota().requests_per_minute, &throttle))
        }
    }
}

fn oidc_authorize_rate_limited_response(limit_rpm: u32, throttle: &ThrottleInfo) -> AdminResponse {
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/xml"));
    apply_throttle_headers(&mut headers, limit_rpm, throttle);
    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <Error><Code>TooManyRequests</Code>\
        <Message>Too many OIDC authorization requests. Please retry after the indicated delay.</Message></Error>";
    AdminResponse::with_headers(StatusCode::TOO_MANY_REQUESTS, Body::from(body), headers)
}

fn callback_federation_error(error: FederationError) -> S3Error {
    match error {
        // Operator diagnostics in `CodeExchange` embed internal provider configuration
        // (`config_url`, `token_endpoint`, `client_id`). The handler logs the full
        // diagnostic; the unauthenticated callback response returns fixed messages only.
        FederationError::CodeExchange(message) if is_invalid_oidc_state_error(&message) => {
            s3_error!(AccessDenied, "code exchange failed: invalid or expired OIDC state")
        }
        FederationError::CodeExchange(_) => s3_error!(AccessDenied, "code exchange failed"),
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

fn config_service_error(error: oidc_config::OidcAdminConfigError) -> S3Error {
    use oidc_config::OidcAdminConfigError;
    let code = match &error {
        OidcAdminConfigError::Config(rustfs_iam::oidc::OidcConfigError::EnvironmentManaged) => S3ErrorCode::AccessDenied,
        OidcAdminConfigError::Config(_) | OidcAdminConfigError::Validation(_) => S3ErrorCode::InvalidRequest,
        OidcAdminConfigError::StorageUnavailable
        | OidcAdminConfigError::Load(_)
        | OidcAdminConfigError::Save(_)
        | OidcAdminConfigError::Task(_) => S3ErrorCode::InternalError,
    };
    S3Error::with_message(code, error.to_string())
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

/// Handler: GET /rustfs/admin/v3/oidc/providers
/// Returns list of configured OIDC providers for the login page.
pub struct ListOidcProvidersHandler {}

#[async_trait::async_trait]
impl Operation for ListOidcProvidersHandler {
    async fn call(&self, _req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let federation = current_federated_identity_service().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

        let providers = federation.list_visible_providers();
        let json_body = serde_json::to_vec(&providers)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize error: {e}")))?;

        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
        Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(json_body), headers))
    }
}

pub struct GetOidcConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetOidcConfigHandler {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_oidc_config_request(&req, AdminAction::ServerInfoAdminAction).await?;

        let config = oidc_config::list_config().await.map_err(config_service_error)?;
        let providers = config
            .snapshot
            .into_providers()
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

        owned_json_response(
            StatusCode::OK,
            &OidcConfigListResponse {
                providers,
                restart_required: config.restart_required,
            },
        )
    }
}

pub struct PutOidcConfigHandler {}

#[async_trait::async_trait]
impl Operation for PutOidcConfigHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_oidc_config_request(&req, AdminAction::ConfigUpdateAdminAction).await?;
        authorize_oidc_config_request(&req, AdminAction::ManageOpenIDConfigAdminAction).await?;

        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;
        oidc_config::ensure_mutable_provider_id(provider_id).map_err(config_service_error)?;
        let provider_id = provider_id.to_owned();

        let request: OidcProviderConfigInput = parse_json_body(&mut req).await?;
        oidc_config::upsert_config(provider_id, request)
            .await
            .map_err(config_service_error)?;

        owned_json_response(
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
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_oidc_config_request(&req, AdminAction::ConfigUpdateAdminAction).await?;
        authorize_oidc_config_request(&req, AdminAction::ManageOpenIDConfigAdminAction).await?;

        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;
        oidc_config::ensure_mutable_provider_id(provider_id).map_err(config_service_error)?;
        let provider_id = provider_id.to_owned();
        oidc_config::delete_config(provider_id).await.map_err(config_service_error)?;

        owned_json_response(
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
    async fn call(&self, mut req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_oidc_config_request(&req, AdminAction::ServerInfoAdminAction).await?;

        let request: OidcProviderValidationInput = parse_json_body(&mut req).await?;
        let validation = oidc_config::validate_config(request).await.map_err(config_service_error)?;

        owned_json_response(
            StatusCode::OK,
            &OidcValidationResponse {
                valid: true,
                message: "OIDC configuration is valid".to_string(),
                issuer: Some(validation.issuer),
                authorization_endpoint: validation.authorization_endpoint,
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
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;

        if !is_valid_provider_id(provider_id) {
            return Err(s3_error!(InvalidRequest, "invalid provider_id"));
        }

        if let Some(throttled) = check_oidc_authorize_rate_limit(&OIDC_AUTHORIZE_RATE_LIMITER, &req, provider_id) {
            return Ok(throttled);
        }

        let federation = current_federated_identity_service().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

        // Derive the callback redirect URI from the request
        let redirect_uri = derive_callback_uri(&req, provider_id)?;

        // Optional: redirect_after query parameter (must be a safe relative path)
        let redirect_after = extract_safe_redirect_after(req.uri())?;
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

        // Bind the login to this browser: the callback only accepts a `state`
        // that returns alongside the cookie set here.
        let state = extract_authorize_state(&auth_url)?;
        let state_cookie = build_oidc_state_cookie(&state, &redirect_uri, OIDC_STATE_COOKIE_MAX_AGE_SECS)?;

        // Return 302 redirect
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::LOCATION,
            auth_url
                .parse()
                .map_err(|_| s3_error!(InternalError, "failed to construct authorization URL"))?,
        );
        headers.insert(http::header::SET_COOKIE, state_cookie);
        Ok(AdminResponse::with_headers(StatusCode::FOUND, Body::empty(), headers))
    }
}

/// Handler: GET /rustfs/admin/v3/oidc/callback/:provider_id?code=...&state=...
/// Exchanges authorization code for tokens, maps claims, issues STS credentials.
pub struct OidcCallbackHandler {}

#[async_trait::async_trait]
impl Operation for OidcCallbackHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;

        if !is_valid_provider_id(provider_id) {
            return Err(s3_error!(InvalidRequest, "invalid provider_id"));
        }

        // Check for error response from IdP
        if let Some((error, desc)) = extract_idp_callback_error(req.uri()) {
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
            extract_query_param(req.uri(), "code").ok_or_else(|| s3_error!(InvalidRequest, "missing 'code' query parameter"))?;
        let state = extract_query_param(req.uri(), "state")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing 'state' query parameter"))?;

        // A stored `state` alone does not prove which browser started the login. Refuse
        // requests without the matching browser cookie before consulting the federation
        // service so an unauthenticated caller cannot consume another user's pending state
        // or complete a login CSRF.
        if !oidc_state_cookie_matches(req.headers(), &state) {
            warn!(
                event = EVENT_ADMIN_OIDC_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_OIDC,
                result = "state_cookie_mismatch",
                requested_provider_id = %provider_id,
                code_len = code.len(),
                state_len = state.len(),
                "admin oidc state"
            );
            return Err(S3Error::with_message(S3ErrorCode::AccessDenied, OIDC_STATE_COOKIE_MISMATCH));
        }

        let federation = current_federated_identity_service().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

        let redirect_uri = derive_callback_uri(&req, provider_id)?;

        let session_duration_seconds = console_session_duration_secs();
        let login = federation
            .complete_authorization_code(
                provider_id,
                &state,
                &code,
                &redirect_uri,
                session_duration_seconds,
                &DefaultFederatedSessionBinding,
            )
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
        let actual_provider_id = authorization.provider_id();

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
            policy_count = authorization.policies().len(),
            group_count = authorization.mapped_iam_groups().len(),
            policies = ?authorization.policies(),
            groups = ?authorization.mapped_iam_groups(),
            state = "claims_mapped",
            "admin oidc state"
        );

        let new_cred = &login.session.credentials;
        let redirect_policy = federation.redirect_policy(actual_provider_id);

        // Build redirect URL to console with credentials in the fragment
        let console_redirect = build_console_redirect_with_policy(
            redirect_policy.as_ref(),
            &new_cred.access_key,
            &new_cred.secret_key,
            &new_cred.session_token,
            new_cred.expiration,
            login.redirect_after.as_deref(),
            Some(login.logout_token.as_str()),
        )?;

        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::LOCATION,
            console_redirect
                .parse()
                .map_err(|_| s3_error!(InternalError, "failed to construct console redirect URL"))?,
        );
        // The state is consumed; clear the browser cookie that carried it.
        headers.insert(http::header::SET_COOKIE, build_oidc_state_cookie("", &redirect_uri, 0)?);
        Ok(AdminResponse::with_headers(StatusCode::FOUND, Body::empty(), headers))
    }
}

/// Handler: GET /rustfs/admin/v3/oidc/logout?logout_token=...
/// Consumes the logout token and redirects either to the IdP end-session URL
/// or back to the console login page when federated logout is unavailable.
pub struct OidcLogoutHandler {}

#[async_trait::async_trait]
impl Operation for OidcLogoutHandler {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let fallback_location = build_console_login_redirect()?;
        let Some(logout_token) = extract_query_param(req.uri(), "logout_token") else {
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
fn derive_callback_uri(req: &AdminRequest, provider_id: &str) -> S3Result<String> {
    let policy = current_federated_identity_service().and_then(|federation| federation.redirect_policy(provider_id));

    derive_callback_uri_with_redirect_policy(req, provider_id, policy.as_ref())
}

fn derive_callback_uri_with_redirect_policy(
    req: &AdminRequest,
    provider_id: &str,
    policy: Option<&rustfs_iam::federation::FederatedRedirectPolicy>,
) -> S3Result<String> {
    if let Some(policy) = policy
        && let Some(ref uri) = policy.redirect_uri
    {
        let parsed = Url::parse(uri).map_err(|_| s3_error!(InvalidRequest, "invalid configured redirect_uri"))?;
        if !is_valid_scheme(parsed.scheme()) || parsed.host_str().is_none() {
            return Err(s3_error!(InvalidRequest, "configured redirect_uri must be absolute http/https URL"));
        }
        return Ok(uri.clone());
    }

    if let Some(url) = browser_redirect_url(&oidc_callback_path(provider_id))? {
        return Ok(url);
    }

    if policy.is_some_and(|policy| !policy.allow_request_origin) {
        return Err(s3_error!(
            InvalidRequest,
            "provider requires explicit redirect_uri because redirect_uri_dynamic is disabled"
        ));
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

/// Read back the `state` generated for the authorization request.
fn extract_authorize_state(auth_url: &str) -> S3Result<String> {
    Url::parse(auth_url)
        .ok()
        .and_then(|url| {
            url.query_pairs()
                .find(|(key, _)| key == "state")
                .map(|(_, value)| value.into_owned())
        })
        .filter(|state| !state.is_empty())
        .ok_or_else(|| s3_error!(InternalError, "authorization URL has no state parameter"))
}

/// True if `value` contains only RFC 6265 cookie-value-safe ASCII characters
/// (no whitespace, control characters, quotes, commas, semicolons, or backslashes).
fn is_safe_cookie_value(value: &str) -> bool {
    value
        .bytes()
        .all(|b| b.is_ascii_graphic() && !matches!(b, b'"' | b',' | b';' | b'\\'))
}

/// Build the `Set-Cookie` value that carries the login state. `Path` is scoped to the
/// callback URL's path so the cookie accompanies no other request; `Max-Age=0` clears it.
fn build_oidc_state_cookie(state: &str, callback_uri: &str, max_age_secs: u64) -> S3Result<http::HeaderValue> {
    if !is_safe_cookie_value(state) {
        return Err(s3_error!(InternalError, "invalid OIDC state for cookie"));
    }
    let callback = Url::parse(callback_uri).map_err(|_| s3_error!(InternalError, "invalid OIDC callback URI"))?;
    let path = callback.path();
    if path.contains(';') || path.bytes().any(|b| b.is_ascii_control()) {
        return Err(s3_error!(InternalError, "invalid OIDC callback path for cookie"));
    }
    let mut cookie = format!("{OIDC_STATE_COOKIE}={state}; Path={path}; Max-Age={max_age_secs}; HttpOnly; SameSite=Lax");
    if callback.scheme() == "https" {
        cookie.push_str("; Secure");
    }
    cookie
        .parse()
        .map_err(|_| s3_error!(InternalError, "failed to construct OIDC state cookie"))
}

/// The login state cookie presented by the browser, from any of its `Cookie` headers.
fn extract_oidc_state_cookie(headers: &http::HeaderMap) -> Option<&str> {
    headers
        .get_all(http::header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .find_map(|pair| {
            let (name, value) = pair.trim().split_once('=')?;
            (name == OIDC_STATE_COOKIE).then_some(value)
        })
}

/// True when the browser presents the cookie the authorize handler set for this `state`.
fn oidc_state_cookie_matches(headers: &http::HeaderMap, state: &str) -> bool {
    !state.is_empty() && extract_oidc_state_cookie(headers).is_some_and(|cookie| constant_time_eq(cookie, state))
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

fn configured_redirect_origin(policy: Option<&rustfs_iam::federation::FederatedRedirectPolicy>) -> S3Result<Option<String>> {
    let Some(uri) = policy.and_then(|policy| policy.redirect_uri.as_deref()) else {
        return Ok(None);
    };
    let parsed = Url::parse(uri).map_err(|_| s3_error!(InvalidRequest, "invalid configured redirect_uri"))?;
    if !is_valid_scheme(parsed.scheme()) || parsed.host_str().is_none() {
        return Err(s3_error!(InvalidRequest, "configured redirect_uri must be absolute http/https URL"));
    }
    Ok(Some(parsed.origin().ascii_serialization()))
}

fn build_console_redirect_with_policy(
    policy: Option<&rustfs_iam::federation::FederatedRedirectPolicy>,
    access_key: &str,
    secret_key: &str,
    session_token: &str,
    expiration: Option<OffsetDateTime>,
    redirect_after: Option<&str>,
    logout_token: Option<&str>,
) -> S3Result<String> {
    let fragment =
        build_console_callback_fragment(access_key, secret_key, session_token, expiration, redirect_after, logout_token);

    let console_prefix = console_prefix();
    let callback_path = format!("{console_prefix}{CONSOLE_OIDC_CALLBACK_SUFFIX}");
    if let Some(base_url) = browser_redirect_url(&callback_path)? {
        return Ok(format!("{base_url}#{fragment}"));
    }

    if let Some(origin) = configured_redirect_origin(policy)? {
        return Ok(format!("{origin}{callback_path}#{fragment}"));
    }

    if policy.is_some_and(|policy| !policy.allow_request_origin) {
        return Err(s3_error!(
            InvalidRequest,
            "provider requires explicit redirect_uri or RUSTFS_BROWSER_REDIRECT_URL because redirect_uri_dynamic is disabled"
        ));
    }

    // Never reflect untrusted Host or X-Forwarded-Proto headers into a redirect that carries
    // STS credentials in its fragment. A same-origin relative Location stays bound to the
    // origin the browser already navigated to for the callback.
    Ok(format!("{callback_path}#{fragment}"))
}

fn build_console_login_redirect() -> S3Result<String> {
    let console_prefix = console_prefix();
    let login_path = format!("{console_prefix}{CONSOLE_LOGIN_SUFFIX}");
    if let Some(url) = browser_redirect_url(&login_path)? {
        return Ok(url);
    }

    Ok(login_path)
}

fn redirect_response(location: &str) -> S3Result<AdminResponse> {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::LOCATION,
        location
            .parse()
            .map_err(|_| s3_error!(InternalError, "failed to construct redirect URL"))?,
    );
    Ok(AdminResponse::with_headers(StatusCode::FOUND, Body::empty(), headers))
}

/// The pre-check keeps this endpoint family's historical missing-credentials
/// message; the shared gate reports "get cred failed".
async fn authorize_oidc_config_request(req: &AdminRequest, action: AdminAction) -> S3Result<()> {
    if req.credentials().is_none() {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    }

    authorize_admin_envelope(req.envelope(), vec![Action::AdminAction(action)]).await?;
    Ok(())
}

async fn parse_json_body<T: DeserializeOwned>(req: &mut AdminRequest) -> S3Result<T> {
    let body = req
        .take_body()
        .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
        .await
        .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

    if body.is_empty() {
        return Err(s3_error!(InvalidRequest, "request body is required"));
    }

    serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))
}

fn extract_request_scheme(req: &AdminRequest) -> S3Result<String> {
    // Only trust X-Forwarded-Proto when validated by the trusted-proxy middleware.
    let trusted_proto = req
        .client_info()
        .filter(|info| info.is_from_trusted_proxy)
        .and_then(|info| info.forwarded_proto.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let raw_scheme = trusted_proto
        .or_else(|| req.uri().scheme_str().map(str::to_owned))
        .unwrap_or_else(|| "http".to_owned())
        .to_ascii_lowercase();

    if !is_valid_scheme(&raw_scheme) {
        return Err(s3_error!(InvalidRequest, "invalid scheme in request"));
    }

    Ok(raw_scheme)
}

fn extract_request_host(req: &AdminRequest) -> S3Result<String> {
    let trusted_host = req
        .client_info()
        .filter(|info| info.is_from_trusted_proxy)
        .and_then(|info| info.forwarded_host.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let host = trusted_host
        .or_else(|| {
            req.headers()
                .get(http::header::HOST)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| {
                    let v = v.trim();
                    if v.is_empty() { None } else { Some(v.to_owned()) }
                })
        })
        .or_else(|| req.uri().authority().map(|a| a.as_str().to_owned()))
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

    fn build_console_redirect(
        access_key: &str,
        secret_key: &str,
        session_token: &str,
        expiration: Option<OffsetDateTime>,
        redirect_after: Option<&str>,
        logout_token: Option<&str>,
    ) -> S3Result<String> {
        build_console_redirect_with_policy(None, access_key, secret_key, session_token, expiration, redirect_after, logout_token)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn validate_handler_preserves_workload_null_and_console_endpoints() {
        use crate::admin::runtime_sources::{AppContext, publish_test_app_context};
        use http_body_util::BodyExt as _;
        use rustfs_iam::store::{Store as _, object::IAM_CONFIG_PREFIX};
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        // The admin URL boundary rejects literal loopback hosts. A local proxy
        // serves the public-shaped test origin without external DNS or traffic.
        let proxy = format!("http://{}", listener.local_addr().unwrap());
        let base = "http://oidc-handler.example.invalid".to_string();
        temp_env::async_with_vars(
            [
                ("RUSTFS_OUTBOUND_ALLOW_ORIGINS", Some(base.as_str())),
                (rustfs_config::oidc::ENV_IDENTITY_OPENID_ALLOW_INSECURE_TRANSPORT, Some("true")),
                ("HTTP_PROXY", Some(proxy.as_str())),
                ("http_proxy", Some(proxy.as_str())),
                ("HTTPS_PROXY", None),
                ("https_proxy", None),
                ("ALL_PROXY", None),
                ("all_proxy", None),
                ("NO_PROXY", Some("")),
                ("no_proxy", Some("")),
            ],
            async {
                let _ = rustfs_credentials::init_global_action_credentials(Some("OIDCVALIDATEROOT".into()), Some("oidcValidateRootSecret123".into()));
                let env = rustfs_test_utils::TestECStoreEnv::builder().prefix("oidc_validate_handler")
                    .disk_count(1).init_bucket_metadata(false).build().await;
                rustfs_iam::store::object::ObjectStore::new(Arc::clone(&env.ecstore))
                    .save_iam_config(serde_json::json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX)).await.unwrap();
                let iam = rustfs_iam::init_iam_sys(Arc::clone(&env.ecstore)).await.unwrap();
                publish_test_app_context(Arc::new(AppContext::with_default_interfaces(
                    Arc::clone(&env.ecstore), iam, Arc::new(rustfs_kms::KmsServiceManager::new()),
                )));
                let server_base = base.clone();
                let server = tokio::spawn(async move {
                    for path in ["/.well-known/openid-configuration", "/jwks", "/.well-known/openid-configuration", "/complete/.well-known/openid-configuration", "/complete/jwks"] {
                        let (mut stream, _) = tokio::time::timeout(std::time::Duration::from_secs(15), listener.accept()).await.unwrap().unwrap();
                        let mut request = Vec::new();
                        while !request.ends_with(b"\r\n\r\n") {
                            request.push(stream.read_u8().await.unwrap());
                            assert!(request.len() < 8192);
                        }
                        let request = String::from_utf8(request).unwrap();
                        let target = Url::parse(request.lines().next().unwrap().split_whitespace().nth(1).unwrap()).unwrap();
                        assert_eq!(target.origin().ascii_serialization(), server_base);
                        assert_eq!(target.path(), path);
                        let mut body = if path.ends_with("/jwks") { serde_json::json!({"keys": []}) } else {
                            serde_json::json!({"issuer": server_base, "jwks_uri": format!("{server_base}/jwks"), "id_token_signing_alg_values_supported": ["RS256"]})
                        };
                        if path == "/complete/.well-known/openid-configuration" {
                            body["authorization_endpoint"] = serde_json::json!(format!("{server_base}/authorize"));
                            body["token_endpoint"] = serde_json::json!(format!("{server_base}/token"));
                            body["response_types_supported"] = serde_json::json!(["code"]);
                            body["subject_types_supported"] = serde_json::json!(["public"]);
                        }
                        let body = body.to_string();
                        stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                    }
                });
                for (hidden, complete) in [(true, false), (false, false), (false, true)] {
                    let document = serde_json::json!({"provider_id": "workload", "client_id": "rustfs-test", "issuer": base,
                        "config_url": format!("{base}{}/.well-known/openid-configuration", if complete { "/complete" } else { "" }), "hide_from_ui": hidden});
                    let request = |credentials: Option<(&str, &str)>| {
                        let request = crate::admin::operation::test_support::TestRequest::new(Method::POST, "/rustfs/admin/v3/oidc/validate")
                            .body(Body::from(document.to_string()));
                        match credentials {
                            Some((access_key, secret_key)) => request.credentials(access_key, secret_key, None),
                            None => request,
                        }
                        .build()
                    };
                    let denied = ValidateOidcConfigHandler {}.call(request(None), Params::new()).await.unwrap_err();
                    assert_eq!(denied.code(), &S3ErrorCode::InvalidRequest);
                    assert_eq!(denied.message(), Some("authentication required"));
                    let req = request(Some(("OIDCVALIDATEROOT", "oidcValidateRootSecret123")));
                    let result = ValidateOidcConfigHandler {}.call(req, Params::new()).await;
                    if !hidden && !complete {
                        let err = result.unwrap_err();
                        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
                        assert!(err.message().unwrap().contains("authorization_endpoint"));
                        continue;
                    }
                    let (status, _headers, body) = result.unwrap().into_parts();
                    assert_eq!(status, StatusCode::OK);
                    let body = body.collect().await.unwrap().to_bytes();
                    let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    assert_eq!(response["valid"], true);
                    assert_eq!(response["issuer"], base);
                    if complete {
                        assert_eq!(response["authorization_endpoint"], format!("{base}/authorize"));
                        assert_eq!(response["token_endpoint"], format!("{base}/token"));
                    } else {
                        assert_eq!(response.get("authorization_endpoint"), Some(&serde_json::Value::Null));
                        assert_eq!(response.get("token_endpoint"), Some(&serde_json::Value::Null));
                    }
                }
                server.await.unwrap();
            },
        ).await;
    }

    fn build_oidc_request(uri: &'static str, host: Option<&'static str>, forwarded_proto: Option<&'static str>) -> AdminRequest {
        let mut headers = HeaderMap::new();
        if let Some(host) = host {
            headers.insert(http::header::HOST, HeaderValue::from_static(host));
        }
        if let Some(proto) = forwarded_proto {
            headers.insert("x-forwarded-proto", HeaderValue::from_static(proto));
        }

        crate::admin::operation::test_support::TestRequest::new(Method::GET, Uri::from_static(uri))
            .headers(headers)
            .extensions(Extensions::new())
            .body(Body::empty())
            .build()
    }

    fn test_redirect_policy(
        redirect_uri: Option<&str>,
        allow_request_origin: bool,
    ) -> rustfs_iam::federation::FederatedRedirectPolicy {
        rustfs_iam::federation::FederatedRedirectPolicy {
            redirect_uri: redirect_uri.map(ToString::to_string),
            allow_request_origin,
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
                FederationError::CodeExchange("invalid or expired OIDC state".to_string()),
                S3ErrorCode::AccessDenied,
                "code exchange failed: invalid or expired OIDC state",
            ),
            (
                FederationError::CodeExchange(
                    "token exchange failed: invalid_grant: stage=token_server_response, provider_id=default, \
                     config_url=http://keycloak.keycloak.svc.cluster.local:8080/realms/rustfs, \
                     issuer=https://auth.example.com/realms/rustfs, \
                     token_endpoint=http://keycloak.keycloak.svc.cluster.local:8080/realms/rustfs/token, \
                     redirect_uri=https://console.example.com/rustfs/admin/v3/oidc/callback/default, \
                     client_id=rustfs-console"
                        .to_string(),
                ),
                S3ErrorCode::AccessDenied,
                "code exchange failed",
            ),
            (
                FederationError::CodeExchange("OIDC state was issued for a different provider".to_string()),
                S3ErrorCode::AccessDenied,
                "code exchange failed",
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
    fn callback_errors_never_echo_internal_provider_urls() {
        let internal_url = "http://keycloak.keycloak.svc.cluster.local:8080/realms/rustfs";
        let diagnostic = format!(
            "token exchange failed: stage=token_request_failed, provider_id=corp, config_url={internal_url}, \
             token_endpoint={internal_url}/protocol/openid-connect/token, client_id=rustfs-console"
        );
        assert!(!is_invalid_oidc_state_error(&diagnostic));
        assert!(is_invalid_oidc_state_error("invalid or expired OIDC state"));

        let err = callback_federation_error(FederationError::CodeExchange(diagnostic));
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        let msg = err.message().expect("error should have a message");
        assert_eq!(msg, "code exchange failed");
        assert!(!msg.contains(internal_url));
        assert!(!msg.contains("config_url"));
        assert!(!msg.contains("token_endpoint"));
        assert!(!msg.contains("rustfs-console"));
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
    fn test_derive_callback_uri_missing_provider_ignores_untrusted_forwarded_proto() {
        let req = build_oidc_request(
            "http://internal/rustfs/admin/v3/oidc/authorize/default",
            Some("internal:9000"),
            Some("https"),
        );

        let callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            derive_callback_uri_with_redirect_policy(&req, "missing", None)
                .expect("a missing provider should keep the request-origin fallback without trusting raw X-Forwarded-Proto")
        });

        assert_eq!(callback, "http://internal:9000/rustfs/admin/v3/oidc/callback/missing");
    }

    #[test]
    fn test_derive_callback_uri_configured_redirect_uri_wins() {
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/authorize/default", Some("internal:9000"), None);
        let policy = test_redirect_policy(Some("https://configured.example.com/rustfs/admin/v3/oidc/callback/default"), false);

        let callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("https://console.example.com"), || {
            derive_callback_uri_with_redirect_policy(&req, "default", Some(&policy))
                .expect("configured redirect_uri should be preferred")
        });

        assert_eq!(callback, "https://configured.example.com/rustfs/admin/v3/oidc/callback/default");
    }

    #[test]
    fn test_derive_callback_uri_browser_redirect_url_satisfies_static_provider() {
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/authorize/default", Some("internal:9000"), None);
        let policy = test_redirect_policy(None, false);

        let callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("https://console.example.com"), || {
            derive_callback_uri_with_redirect_policy(&req, "default", Some(&policy))
                .expect("browser redirect URL should satisfy a non-dynamic provider")
        });

        assert_eq!(callback, "https://console.example.com/rustfs/admin/v3/oidc/callback/default");
    }

    #[test]
    fn test_derive_callback_uri_static_provider_requires_redirect_source() {
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/authorize/default", Some("internal:9000"), None);
        let policy = test_redirect_policy(None, false);

        let err = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            derive_callback_uri_with_redirect_policy(&req, "default", Some(&policy))
                .expect_err("non-dynamic provider without redirect source should fail")
        });

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_derive_callback_uri_dynamic_provider_honors_only_trusted_proxy_headers() {
        let untrusted_req = build_oidc_request(
            "http://internal/rustfs/admin/v3/oidc/authorize/default",
            Some("internal:9000"),
            Some("https"),
        );
        let policy = test_redirect_policy(None, true);

        let callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            derive_callback_uri_with_redirect_policy(&untrusted_req, "default", Some(&policy))
                .expect("dynamic provider should ignore untrusted X-Forwarded-Proto")
        });
        assert_eq!(callback, "http://internal:9000/rustfs/admin/v3/oidc/callback/default");

        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("internal:9000"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        headers.insert("x-forwarded-host", HeaderValue::from_static("public.example.com"));
        let trusted_req = crate::admin::operation::test_support::TestRequest::new(
            Method::GET,
            Uri::from_static("http://internal/rustfs/admin/v3/oidc/authorize/default"),
        )
        .headers(headers)
        .extension(rustfs_trusted_proxies::ClientInfo::from_trusted_proxy(
            "203.0.113.10".parse().expect("valid client IP"),
            Some("public.example.com".to_string()),
            Some("https".to_string()),
            "127.0.0.1".parse().expect("valid proxy IP"),
            1,
            rustfs_trusted_proxies::ValidationMode::Strict,
            Vec::new(),
        ))
        .body(Body::empty())
        .build();

        let trusted_callback = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            derive_callback_uri_with_redirect_policy(&trusted_req, "default", Some(&policy))
                .expect("trusted proxy headers should be honored for dynamic provider")
        });
        assert_eq!(trusted_callback, "https://public.example.com/rustfs/admin/v3/oidc/callback/default");
    }

    #[test]
    fn test_console_redirects_never_reflect_spoofed_host_or_forwarded_proto() {
        // 1. When provider has a configured redirect_uri, callback redirects to that origin,
        // never reflecting untrusted request headers.
        let static_policy =
            test_redirect_policy(Some("https://console.example.com/rustfs/admin/v3/oidc/callback/default"), false);
        let configured_redirect = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            build_console_redirect_with_policy(Some(&static_policy), "ak", "sk", "st", None, Some("/buckets"), Some("logout-tok"))
                .expect("configured provider origin should be used")
        });
        assert!(
            configured_redirect.starts_with(&format!("https://console.example.com{}/auth/oidc-callback/#", console_prefix())),
            "unexpected redirect: {configured_redirect}"
        );
        assert!(!configured_redirect.contains("attacker.example"));

        // 2. When no explicit redirect URL is configured and dynamic mode is enabled,
        // emit a same-origin relative Location so credentials never leave the browser's origin.
        let dynamic_policy = test_redirect_policy(None, true);
        let relative_redirect = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            build_console_redirect_with_policy(Some(&dynamic_policy), "ak", "sk", "st", None, None, None)
                .expect("dynamic provider should emit same-origin relative redirect")
        });
        assert!(
            relative_redirect.starts_with(&format!("{}/auth/oidc-callback/#", console_prefix())),
            "expected same-origin relative redirect, got: {relative_redirect}"
        );
        assert!(!relative_redirect.contains("attacker.example"));
        assert!(!relative_redirect.starts_with("http://"));
        assert!(!relative_redirect.starts_with("https://"));

        // 3. Static provider without redirect_uri or RUSTFS_BROWSER_REDIRECT_URL fails closed.
        let incomplete_static = test_redirect_policy(None, false);
        let err = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            build_console_redirect_with_policy(Some(&incomplete_static), "ak", "sk", "st", None, None, None)
                .expect_err("static provider without configured origin must fail closed")
        });
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

        // 4. Login redirect fallback also uses a relative path and ignores spoofed Host/proto.
        let login_redirect = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, None::<&str>, || {
            build_console_login_redirect().expect("login redirect should be relative")
        });
        assert_eq!(login_redirect, format!("{}/auth/login", console_prefix()));
        assert!(!login_redirect.contains("attacker.example"));
    }

    #[test]
    fn console_prefix_process_case_oidc() {
        if std::env::var_os("RUSTFS_TEST_CONSOLE_PREFIX_PROCESS").is_none() {
            return;
        }
        crate::server::init_console_prefix().expect("initialize console prefix");
        let prefix = console_prefix();
        let req = build_oidc_request("http://internal/rustfs/admin/v3/oidc/callback/default", Some("internal:9000"), None);
        assert_eq!(
            build_console_login_redirect().expect("login URL"),
            format!("https://console.example.com{prefix}/auth/login")
        );
        let redirect = build_console_redirect("access", "secret", "token", None, None, None).expect("console callback URL");
        assert!(redirect.starts_with(&format!("https://console.example.com{prefix}/auth/oidc-callback/#")));
        assert_eq!(
            derive_callback_uri(&req, "default").expect("admin callback URL"),
            "https://console.example.com/rustfs/admin/v3/oidc/callback/default"
        );
    }

    #[test]
    fn test_build_console_redirect_uses_browser_redirect_url() {
        let redirect = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("https://console.example.com/"), || {
            build_console_redirect("access", "secret", "token", None, Some("/buckets"), Some("logout-token"))
                .expect("console redirect should use browser redirect URL")
        });

        assert!(redirect.starts_with(&format!("https://console.example.com{}/auth/oidc-callback/#", console_prefix())));
        assert!(redirect.contains("redirect=%2Fbuckets"));
        assert!(redirect.contains("logoutToken=logout-token"));
    }

    #[test]
    fn test_build_console_login_redirect_uses_browser_redirect_url() {
        let redirect = with_var(ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("https://console.example.com"), || {
            build_console_login_redirect().expect("login redirect should use browser redirect URL")
        });

        assert_eq!(redirect, format!("https://console.example.com{}/auth/login", console_prefix()));
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
    fn oidc_config_errors_keep_admin_codes() {
        let error = config_service_error(oidc_config::OidcAdminConfigError::Config(
            rustfs_iam::oidc::OidcConfigError::EnvironmentManaged,
        ));
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(error.message(), Some("provider is managed by environment variables"));

        let error = config_service_error(oidc_config::OidcAdminConfigError::Config(
            rustfs_iam::oidc::OidcConfigError::ProviderNotFound,
        ));
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(error.message(), Some("provider not found"));

        let error = config_service_error(oidc_config::OidcAdminConfigError::Task("cancelled"));
        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert_eq!(error.message(), Some("OIDC config update task cancelled"));

        let error = rustfs_iam::oidc::build_upsert_provider_config(
            "default",
            rustfs_iam::oidc::OidcProviderConfigInput {
                config_url: "https://127.0.0.1/.well-known/openid-configuration".to_string(),
                client_id: "client".to_string(),
                ..Default::default()
            },
            None,
        )
        .expect_err("loopback config URL should be rejected");
        let error = config_service_error(oidc_config::OidcAdminConfigError::Config(error));
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(error.message().unwrap_or_default().contains("not allowed"));
    }

    /// The OIDC config gate now authorizes through the shared admin gate, which
    /// reports "get cred failed"; its pre-check keeps the message these endpoints
    /// have always returned (rustfs/backlog#1829).
    #[tokio::test]
    async fn oidc_config_gate_keeps_its_missing_credentials_message() {
        for action in [AdminAction::ServerInfoAdminAction, AdminAction::ConfigUpdateAdminAction] {
            let err = authorize_oidc_config_request(&build_oidc_request("/rustfs/admin/v3/idp/openid", None, None), action)
                .await
                .expect_err("a request without credentials must be rejected");
            assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
            assert_eq!(err.message(), Some("authentication required"));
        }
    }

    #[test]
    fn oidc_config_gate_routes_through_the_shared_gate() {
        let production = include_str!("oidc.rs")
            .split("\n#[cfg(test)]\n")
            .next()
            .expect("production source must precede tests");
        let wrapper = production
            .split_once("async fn authorize_oidc_config_request")
            .expect("the OIDC config gate must exist")
            .1
            .split_once("\nasync fn parse_json_body")
            .expect("the OIDC config gate must be followed by parse_json_body")
            .0;

        assert_eq!(
            wrapper.matches("authorize_admin_envelope(").count(),
            1,
            "the OIDC config gate must use exactly one shared gate"
        );
        assert!(
            wrapper.contains("authorize_admin_envelope(req.envelope(), vec![Action::AdminAction(action)])"),
            "the OIDC config gate must forward its parameterized action unchanged"
        );
        assert!(
            !wrapper.contains("let cred = authorize_admin_envelope("),
            "the OIDC config gate does not need the authenticated credentials"
        );

        for (handler, action) in [
            ("GetOidcConfigHandler", "AdminAction::ServerInfoAdminAction"),
            ("PutOidcConfigHandler", "AdminAction::ConfigUpdateAdminAction"),
            ("DeleteOidcConfigHandler", "AdminAction::ConfigUpdateAdminAction"),
            ("PutOidcConfigHandler", "AdminAction::ManageOpenIDConfigAdminAction"),
            ("DeleteOidcConfigHandler", "AdminAction::ManageOpenIDConfigAdminAction"),
            ("ValidateOidcConfigHandler", "AdminAction::ServerInfoAdminAction"),
        ] {
            let marker = format!("impl Operation for {handler}");
            let block = production
                .split_once(marker.as_str())
                .unwrap_or_else(|| panic!("{handler} should exist"))
                .1;
            let block = &block[..block.find("\npub struct ").unwrap_or(block.len())];
            assert!(
                block.contains(&format!("authorize_oidc_config_request(&req, {action})")),
                "{handler} must keep authorizing with {action}"
            );
        }
    }

    #[test]
    fn console_callback_uses_the_configured_session_duration() {
        let production = include_str!("oidc.rs")
            .split("\n#[cfg(test)]\n")
            .next()
            .expect("production source must precede tests");
        let callback = production
            .split_once("impl Operation for OidcCallbackHandler")
            .expect("the OIDC callback handler must exist")
            .1;
        let callback = callback.split_whitespace().collect::<String>();

        assert!(
            callback.contains("session_duration_seconds=console_session_duration_secs()"),
            "the callback must resolve the Console session duration from the environment"
        );
        assert!(
            callback.contains(".complete_authorization_code(provider_id,&state,&code,&redirect_uri,session_duration_seconds,"),
            "the callback must hand the resolved session duration to the code exchange"
        );
        assert!(
            !callback.contains(".complete_authorization_code(provider_id,&state,&code,&redirect_uri,3600,"),
            "the callback must not hard-code the session duration"
        );
    }

    fn build_oidc_request_from_peer(
        uri: &'static str,
        peer: std::net::SocketAddr,
        forwarded_for: Option<&'static str>,
        trusted_client_ip: Option<IpAddr>,
    ) -> AdminRequest {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("localhost:9000"));
        if let Some(xff) = forwarded_for {
            headers.insert("x-forwarded-for", HeaderValue::from_static(xff));
        }

        let mut req = crate::admin::operation::test_support::TestRequest::new(Method::GET, Uri::from_static(uri))
            .headers(headers)
            .extension(Some(crate::server::RemoteAddr(peer)));
        if let Some(real_ip) = trusted_client_ip {
            req = req.extension(rustfs_trusted_proxies::ClientInfo::from_trusted_proxy(
                real_ip,
                None,
                None,
                peer.ip(),
                1,
                rustfs_trusted_proxies::ValidationMode::Strict,
                Vec::new(),
            ));
        }
        req.body(Body::empty()).build()
    }

    #[tokio::test]
    async fn oidc_authorize_rate_limit_caps_single_ip_and_isolates_others() {
        use http_body_util::BodyExt as _;

        let limiter = RateLimiter::new(RateLimitQuota {
            requests_per_minute: 60,
            burst: 2,
        });
        let attacker_ip: IpAddr = "198.51.100.1".parse().expect("valid test IP");
        let victim_ip: IpAddr = "198.51.100.2".parse().expect("valid test IP");
        let proxied_ip: IpAddr = "203.0.113.77".parse().expect("valid test IP");

        // Distinct ephemeral ports from the same client IP share one budget.
        for port in [41000, 41001] {
            let req = build_oidc_request_from_peer(
                "http://localhost:9000/rustfs/admin/v3/oidc/authorize/default",
                std::net::SocketAddr::new(attacker_ip, port),
                None,
                None,
            );
            assert!(
                check_oidc_authorize_rate_limit(&limiter, &req, "default").is_none(),
                "requests within burst must be allowed"
            );
        }

        // Next request from the same IP is throttled with 429 and retry headers.
        let throttled_req = build_oidc_request_from_peer(
            "http://localhost:9000/rustfs/admin/v3/oidc/authorize/default",
            std::net::SocketAddr::new(attacker_ip, 41002),
            None,
            None,
        );
        let throttled = check_oidc_authorize_rate_limit(&limiter, &throttled_req, "default")
            .expect("request exceeding burst must be rate-limited");
        let (status, headers, body) = throttled.into_parts();
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert!(headers.contains_key(http::header::RETRY_AFTER));
        assert_eq!(headers.get("x-ratelimit-remaining").and_then(|v| v.to_str().ok()), Some("0"));
        let body = body.collect().await.expect("rate limit body should read").to_bytes();
        let body_text = std::str::from_utf8(&body).expect("rate limit body should be UTF-8");
        assert!(body_text.contains("<Code>TooManyRequests</Code>"));

        // Spoofed X-Forwarded-For without trusted-proxy ClientInfo cannot bypass the peer IP cap.
        let spoofed_req = build_oidc_request_from_peer(
            "http://localhost:9000/rustfs/admin/v3/oidc/authorize/default",
            std::net::SocketAddr::new(attacker_ip, 41003),
            Some("203.0.113.99"),
            None,
        );
        let spoofed = check_oidc_authorize_rate_limit(&limiter, &spoofed_req, "default")
            .expect("spoofed X-Forwarded-For must not escape the peer IP rate limit");
        assert_eq!(spoofed.into_parts().0, StatusCode::TOO_MANY_REQUESTS);

        // Another client IP retains its own independent budget.
        let victim_req = build_oidc_request_from_peer(
            "http://localhost:9000/rustfs/admin/v3/oidc/authorize/default",
            std::net::SocketAddr::new(victim_ip, 42000),
            None,
            None,
        );
        assert!(
            check_oidc_authorize_rate_limit(&limiter, &victim_req, "default").is_none(),
            "throttling one IP must not affect other clients"
        );

        // Verified ClientInfo from a trusted proxy keys on the resolved client IP.
        let proxied_req = build_oidc_request_from_peer(
            "http://localhost:9000/rustfs/admin/v3/oidc/authorize/default",
            std::net::SocketAddr::new(attacker_ip, 41004),
            Some("203.0.113.77"),
            Some(proxied_ip),
        );
        assert!(
            check_oidc_authorize_rate_limit(&limiter, &proxied_req, "default").is_none(),
            "verified trusted-proxy client IP must receive its own bucket"
        );
    }

    #[tokio::test]
    async fn oidc_authorize_handler_throttles_excessive_requests_per_ip() {
        let mut router = matchit::Router::new();
        router
            .insert("/rustfs/admin/v3/oidc/authorize/{provider_id}", ())
            .expect("route should insert");

        let flood_ip: IpAddr = "198.51.100.200".parse().expect("valid test IP");
        let other_ip: IpAddr = "198.51.100.201".parse().expect("valid test IP");

        for i in 0..OIDC_AUTHORIZE_RATE_LIMIT_BURST {
            let matched = router
                .at("/rustfs/admin/v3/oidc/authorize/default")
                .expect("route should match");
            let req = build_oidc_request_from_peer(
                "http://localhost:9000/rustfs/admin/v3/oidc/authorize/default",
                std::net::SocketAddr::new(flood_ip, 50000 + u16::try_from(i).expect("burst fits in u16")),
                None,
                None,
            );
            let outcome = OidcAuthorizeHandler {}.call(req, matched.params).await;
            if let Ok(resp) = outcome {
                assert_ne!(
                    resp.into_parts().0,
                    StatusCode::TOO_MANY_REQUESTS,
                    "request {i} within burst should not be rate-limited"
                );
            }
        }

        // Next request from the same IP is rejected with 429 before state allocation.
        let matched = router
            .at("/rustfs/admin/v3/oidc/authorize/default")
            .expect("route should match");
        let req = build_oidc_request_from_peer(
            "http://localhost:9000/rustfs/admin/v3/oidc/authorize/default",
            std::net::SocketAddr::new(flood_ip, 51000),
            None,
            None,
        );
        let resp = OidcAuthorizeHandler {}
            .call(req, matched.params)
            .await
            .expect("rate-limited request should return a 429 AdminResponse");
        let (status, headers, _) = resp.into_parts();
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert!(headers.contains_key(http::header::RETRY_AFTER));

        // Another client IP is unaffected by the first IP's flood.
        let matched = router
            .at("/rustfs/admin/v3/oidc/authorize/default")
            .expect("route should match");
        let other_req = build_oidc_request_from_peer(
            "http://localhost:9000/rustfs/admin/v3/oidc/authorize/default",
            std::net::SocketAddr::new(other_ip, 52000),
            None,
            None,
        );
        let other_outcome = OidcAuthorizeHandler {}.call(other_req, matched.params).await;
        if let Ok(resp) = other_outcome {
            assert_ne!(resp.into_parts().0, StatusCode::TOO_MANY_REQUESTS);
        }
    }

    #[test]
    fn test_extract_authorize_state() {
        assert_eq!(
            extract_authorize_state("https://idp.example.com/authorize?response_type=code&state=abc-123&nonce=n")
                .expect("state should be found"),
            "abc-123"
        );
        assert!(extract_authorize_state("https://idp.example.com/authorize?response_type=code").is_err());
        assert!(extract_authorize_state("https://idp.example.com/authorize?state=").is_err());
    }

    #[test]
    fn test_build_oidc_state_cookie_is_scoped_to_callback() {
        let cookie = build_oidc_state_cookie("abc-123", "https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default", 300)
            .expect("https cookie should build");
        assert_eq!(
            cookie.to_str().expect("cookie should be ASCII"),
            "rustfs_oidc_state=abc-123; Path=/rustfs/admin/v3/oidc/callback/default; Max-Age=300; HttpOnly; SameSite=Lax; Secure"
        );

        let cookie = build_oidc_state_cookie("", "http://localhost:9000/rustfs/admin/v3/oidc/callback/default", 0)
            .expect("clearing cookie should build");
        assert_eq!(
            cookie.to_str().expect("cookie should be ASCII"),
            "rustfs_oidc_state=; Path=/rustfs/admin/v3/oidc/callback/default; Max-Age=0; HttpOnly; SameSite=Lax"
        );

        // Reject unsafe cookie-attribute injection characters in state or path.
        assert!(
            build_oidc_state_cookie(
                "abc;Domain=evil.example",
                "https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default",
                300,
            )
            .is_err()
        );
        assert!(
            build_oidc_state_cookie(
                "abc-123",
                "https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default;Domain=evil.example",
                300,
            )
            .is_err()
        );
    }

    #[test]
    fn test_oidc_state_cookie_must_match_callback_state() {
        let state = "abc-123";

        // No cookie: a browser that never started this login.
        assert!(!oidc_state_cookie_matches(&HeaderMap::new(), state));

        // Cookie from a different login.
        let mut headers = HeaderMap::new();
        headers.insert(http::header::COOKIE, HeaderValue::from_static("rustfs_oidc_state=other"));
        assert!(!oidc_state_cookie_matches(&headers, state));

        // An empty state never matches, even against an empty cookie.
        let mut headers = HeaderMap::new();
        headers.insert(http::header::COOKIE, HeaderValue::from_static("rustfs_oidc_state="));
        assert!(!oidc_state_cookie_matches(&headers, ""));

        // Matching cookie, among other cookies and across multiple Cookie header lines.
        let mut headers = HeaderMap::new();
        headers.append(http::header::COOKIE, HeaderValue::from_static("session=1; theme=dark"));
        headers.append(http::header::COOKIE, HeaderValue::from_static("lang=en; rustfs_oidc_state=abc-123"));
        assert!(oidc_state_cookie_matches(&headers, state));
    }

    #[tokio::test]
    async fn callback_rejects_state_without_matching_browser_cookie() {
        const CALLBACK: &str = "http://localhost:9000/rustfs/admin/v3/oidc/callback/default?code=abc123&state=xyz789";
        let mut router = matchit::Router::new();
        router
            .insert("/rustfs/admin/v3/oidc/callback/{provider_id}", ())
            .expect("route should insert");

        for cookie in [None, Some("rustfs_oidc_state=another-login")] {
            let matched = router
                .at("/rustfs/admin/v3/oidc/callback/default")
                .expect("route should match");
            let mut req = crate::admin::operation::test_support::TestRequest::new(Method::GET, Uri::from_static(CALLBACK))
                .header("host", "localhost:9000");
            if let Some(cookie) = cookie {
                req = req.header("cookie", cookie);
            }

            let Err(err) = OidcCallbackHandler {}.call(req.build(), matched.params).await else {
                panic!("a callback without the matching state cookie must be rejected");
            };
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
            assert_eq!(err.message(), Some(OIDC_STATE_COOKIE_MISMATCH));
        }

        // With the matching cookie, the binding check passes; this synthetic state was never
        // issued so the request still fails later for a different reason.
        let matched = router
            .at("/rustfs/admin/v3/oidc/callback/default")
            .expect("route should match");
        let req = crate::admin::operation::test_support::TestRequest::new(Method::GET, Uri::from_static(CALLBACK))
            .header("host", "localhost:9000")
            .header("cookie", "rustfs_oidc_state=xyz789")
            .build();
        let Err(err) = OidcCallbackHandler {}.call(req, matched.params).await else {
            panic!("an unknown state must not complete a login");
        };
        assert_ne!(err.message(), Some(OIDC_STATE_COOKIE_MISMATCH));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn authorize_and_callback_handlers_enforce_browser_state_cookie_lifecycle() {
        use crate::admin::runtime_sources::{AppContext, publish_test_app_context};
        use base64_simd::URL_SAFE_NO_PAD as B64URL;
        use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
        use p256::elliptic_curve::Generate as _;
        use p256::pkcs8::EncodePublicKey as _;
        use rustfs_iam::store::{Store as _, object::IAM_CONFIG_PREFIX};
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let signing_key = SigningKey::generate();
        let spki = signing_key
            .verifying_key()
            .to_public_key_der()
            .expect("P-256 verifying key should encode as SPKI");
        let spki_bytes = spki.as_bytes();
        let sec1 = &spki_bytes[spki_bytes.len() - 65..];
        assert_eq!(sec1[0], 0x04, "SPKI payload must hold an uncompressed P-256 point");
        let jwk = serde_json::json!({
            "kty": "EC",
            "crv": "P-256",
            "kid": "oidc-cookie-test-key",
            "use": "sig",
            "alg": "ES256",
            "x": B64URL.encode_to_string(&sec1[1..33]),
            "y": B64URL.encode_to_string(&sec1[33..65]),
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock OIDC IdP");
        let base = format!("http://{}", listener.local_addr().expect("mock IdP addr"));
        let (nonce_tx, nonce_rx) = tokio::sync::oneshot::channel::<String>();
        let server_base = base.clone();

        let idp_task = tokio::spawn(async move {
            let mut nonce_rx = Some(nonce_rx);
            for expected_path in ["/.well-known/openid-configuration", "/jwks", "/token"] {
                let (mut stream, _) = tokio::time::timeout(std::time::Duration::from_secs(15), listener.accept())
                    .await
                    .expect("mock IdP accept timeout")
                    .expect("mock IdP accept");
                let mut raw = Vec::new();
                while !raw.ends_with(b"\r\n\r\n") {
                    raw.push(stream.read_u8().await.expect("read request header byte"));
                    assert!(raw.len() < 16_384);
                }
                let header_text = String::from_utf8(raw).expect("HTTP headers UTF-8");
                let request_line = header_text.lines().next().expect("HTTP request line");
                let path = request_line.split_whitespace().nth(1).expect("HTTP request path");
                assert_eq!(path, expected_path);

                let content_len = header_text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                if content_len > 0 {
                    let mut body_buf = vec![0u8; content_len];
                    stream.read_exact(&mut body_buf).await.expect("read POST body");
                }

                let response_body = match expected_path {
                    "/.well-known/openid-configuration" => serde_json::json!({
                        "issuer": server_base,
                        "authorization_endpoint": format!("{server_base}/authorize"),
                        "token_endpoint": format!("{server_base}/token"),
                        "jwks_uri": format!("{server_base}/jwks"),
                        "response_types_supported": ["code"],
                        "subject_types_supported": ["public"],
                        "id_token_signing_alg_values_supported": ["ES256"],
                    })
                    .to_string(),
                    "/jwks" => serde_json::json!({ "keys": [jwk] }).to_string(),
                    "/token" => {
                        let nonce = nonce_rx
                            .take()
                            .expect("nonce receiver should exist once")
                            .await
                            .expect("nonce should arrive from authorize response");
                        let now = OffsetDateTime::now_utc().unix_timestamp();
                        let header = serde_json::json!({
                            "alg": "ES256",
                            "typ": "JWT",
                            "kid": "oidc-cookie-test-key",
                        });
                        let payload = serde_json::json!({
                            "iss": server_base,
                            "sub": "alice-cookie-test",
                            "aud": "rustfs-cookie-test",
                            "nonce": nonce,
                            "iat": now,
                            "exp": now + 300,
                            "groups": ["readonly"],
                        });
                        let signing_input = format!(
                            "{}.{}",
                            B64URL.encode_to_string(serde_json::to_vec(&header).expect("header json")),
                            B64URL.encode_to_string(serde_json::to_vec(&payload).expect("payload json")),
                        );
                        let sig: Signature = signing_key.sign(signing_input.as_bytes());
                        let id_token = format!("{signing_input}.{}", B64URL.encode_to_string(sig.to_bytes()));
                        serde_json::json!({
                            "access_token": "mock-access-token",
                            "token_type": "Bearer",
                            "id_token": id_token,
                        })
                        .to_string()
                    }
                    _ => unreachable!(),
                };

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                stream.write_all(response.as_bytes()).await.expect("write mock IdP response");
            }
        });

        let callback_uri = "http://localhost:9000/rustfs/admin/v3/oidc/callback/default";
        temp_env::async_with_vars(
            [
                ("RUSTFS_IDENTITY_OPENID_ENABLE", Some("on")),
                ("RUSTFS_IDENTITY_OPENID_CONFIG_URL", Some(base.as_str())),
                ("RUSTFS_IDENTITY_OPENID_CLIENT_ID", Some("rustfs-cookie-test")),
                ("RUSTFS_IDENTITY_OPENID_CLIENT_SECRET", Some("rustfs-cookie-secret")),
                ("RUSTFS_IDENTITY_OPENID_REDIRECT_URI", Some(callback_uri)),
                ("RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC", Some("off")),
                (ENV_RUSTFS_BROWSER_REDIRECT_URL, Some("http://localhost:9000")),
                ("RUSTFS_OUTBOUND_ALLOW_ORIGINS", Some(base.as_str())),
                ("HTTP_PROXY", None),
                ("http_proxy", None),
                ("HTTPS_PROXY", None),
                ("https_proxy", None),
                ("ALL_PROXY", None),
                ("all_proxy", None),
                ("NO_PROXY", None),
                ("no_proxy", None),
            ],
            async {
                let _ = rustfs_credentials::init_global_action_credentials(
                    Some("OIDCCOOKIEROOT".into()),
                    Some("oidcCookieRootSecret123".into()),
                );
                let env = rustfs_test_utils::TestECStoreEnv::builder()
                    .prefix("oidc_state_cookie_lifecycle")
                    .disk_count(1)
                    .init_bucket_metadata(false)
                    .build()
                    .await;
                rustfs_iam::store::object::ObjectStore::new(Arc::clone(&env.ecstore))
                    .save_iam_config(serde_json::json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
                    .await
                    .expect("save IAM format");
                let iam = rustfs_iam::init_iam_sys(Arc::clone(&env.ecstore))
                    .await
                    .expect("init IAM sys");
                let context = Arc::new(AppContext::with_default_interfaces(
                    Arc::clone(&env.ecstore),
                    iam,
                    Arc::new(rustfs_kms::KmsServiceManager::new()),
                ));
                let oidc = rustfs_iam::build_oidc_sys_with_extra_root_ca_provider(
                    rustfs_iam::oidc::OidcExtraRootCaProvider::new(|| async {
                        Ok(rustfs_iam::oidc::OidcExtraRootCaMaterial::default())
                    }),
                )
                .await
                .expect("build OIDC sys against mock IdP");
                let adapter = Arc::new(rustfs_iam::oidc::StandardOidcAdapter::new(oidc));
                let oidc_config_query: Arc<dyn rustfs_iam::oidc::OidcConfigQuery> = adapter.clone();
                let mapper = rustfs_iam::federation::CoreFederatedAuthorizationMapper::new(adapter.authorization_rules());
                let service = Arc::new(adapter.into_service(mapper));
                assert!(context.publish_federated_identity_runtime(service, oidc_config_query));
                publish_test_app_context(context);

                let mut authorize_router = matchit::Router::new();
                authorize_router
                    .insert("/rustfs/admin/v3/oidc/authorize/{provider_id}", ())
                    .expect("authorize route should insert");
                let mut callback_router = matchit::Router::new();
                callback_router
                    .insert("/rustfs/admin/v3/oidc/callback/{provider_id}", ())
                    .expect("callback route should insert");

                // 1. Authorize sets the browser-bound state cookie scoped to the callback path.
                let auth_matched = authorize_router
                    .at("/rustfs/admin/v3/oidc/authorize/default")
                    .expect("authorize route should match");
                let auth_req = build_oidc_request_from_peer(
                    "http://localhost:9000/rustfs/admin/v3/oidc/authorize/default",
                    "198.51.100.210:40000".parse().expect("valid peer"),
                    None,
                    None,
                );
                let (auth_status, auth_headers, _) = OidcAuthorizeHandler {}
                    .call(auth_req, auth_matched.params)
                    .await
                    .expect("authorize should succeed")
                    .into_parts();
                assert_eq!(auth_status, StatusCode::FOUND);
                let location = auth_headers
                    .get(http::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .expect("authorize redirect location");
                let parsed_location = Url::parse(location).expect("valid IdP authorize URL");
                let state = parsed_location
                    .query_pairs()
                    .find_map(|(k, v)| (k == "state").then(|| v.into_owned()))
                    .expect("authorize URL should carry state");
                let nonce = parsed_location
                    .query_pairs()
                    .find_map(|(k, v)| (k == "nonce").then(|| v.into_owned()))
                    .expect("authorize URL should carry nonce");
                nonce_tx.send(nonce).expect("deliver nonce to mock IdP");

                let set_cookie = auth_headers
                    .get(http::header::SET_COOKIE)
                    .and_then(|v| v.to_str().ok())
                    .expect("authorize must emit Set-Cookie");
                assert_eq!(
                    set_cookie,
                    format!(
                        "{OIDC_STATE_COOKIE}={state}; Path=/rustfs/admin/v3/oidc/callback/default; Max-Age={OIDC_STATE_COOKIE_MAX_AGE_SECS}; HttpOnly; SameSite=Lax"
                    )
                );

                // 2. Callback without a cookie or with a mismatched cookie is rejected WITHOUT consuming state.
                let callback_uri_with_query = format!("{callback_uri}?code=test-code&state={state}");
                for bad_cookie in [None, Some("rustfs_oidc_state=wrong-browser-state")] {
                    let cb_matched = callback_router
                        .at("/rustfs/admin/v3/oidc/callback/default")
                        .expect("callback route should match");
                    let mut req = crate::admin::operation::test_support::TestRequest::new(
                        Method::GET,
                        callback_uri_with_query.clone(),
                    )
                    .header("host", "localhost:9000");
                    if let Some(cookie) = bad_cookie {
                        req = req.header("cookie", cookie);
                    }
                    let err = OidcCallbackHandler {}
                        .call(req.build(), cb_matched.params)
                        .await
                        .expect_err("callback without matching cookie must fail");
                    assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
                    assert_eq!(err.message(), Some(OIDC_STATE_COOKIE_MISMATCH));
                }

                // 3. Callback with the matching cookie succeeds (state was preserved) and clears the cookie.
                let cb_matched = callback_router
                    .at("/rustfs/admin/v3/oidc/callback/default")
                    .expect("callback route should match");
                let ok_req = crate::admin::operation::test_support::TestRequest::new(
                    Method::GET,
                    callback_uri_with_query.clone(),
                )
                .header("host", "localhost:9000")
                .header("cookie", &format!("{OIDC_STATE_COOKIE}={state}"))
                .build();
                let (cb_status, cb_headers, _) = OidcCallbackHandler {}
                    .call(ok_req, cb_matched.params)
                    .await
                    .expect("callback with matching cookie should succeed")
                    .into_parts();
                assert_eq!(cb_status, StatusCode::FOUND);
                let console_location = cb_headers
                    .get(http::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .expect("callback redirect location");
                assert!(console_location.starts_with(&format!(
                    "http://localhost:9000{}/auth/oidc-callback/#",
                    console_prefix()
                )));
                assert!(console_location.contains("accessKey="));
                assert!(console_location.contains("sessionToken="));
                let cleared_cookie = cb_headers
                    .get(http::header::SET_COOKIE)
                    .and_then(|v| v.to_str().ok())
                    .expect("callback must clear state cookie");
                assert_eq!(
                    cleared_cookie,
                    format!(
                        "{OIDC_STATE_COOKIE}=; Path=/rustfs/admin/v3/oidc/callback/default; Max-Age=0; HttpOnly; SameSite=Lax"
                    )
                );

                // 4. Replaying the same callback with the cookie now fails because state is single-use.
                let cb_matched = callback_router
                    .at("/rustfs/admin/v3/oidc/callback/default")
                    .expect("callback route should match");
                let replay_req = crate::admin::operation::test_support::TestRequest::new(
                    Method::GET,
                    callback_uri_with_query,
                )
                .header("host", "localhost:9000")
                .header("cookie", &format!("{OIDC_STATE_COOKIE}={state}"))
                .build();
                let replay_err = OidcCallbackHandler {}
                    .call(replay_req, cb_matched.params)
                    .await
                    .expect_err("replayed callback must be rejected");
                assert_eq!(replay_err.code(), &S3ErrorCode::AccessDenied);
                assert_eq!(
                    replay_err.message(),
                    Some("code exchange failed: invalid or expired OIDC state")
                );
            },
        )
        .await;

        idp_task.await.expect("mock IdP server should complete cleanly");
    }
}
