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
use super::{
    config::{
        OidcConfigQuery, OidcConfigSnapshot, OidcProviderConfig, OidcProviderSummary, SourcedOidcProviderConfig,
        load_oidc_config_snapshot,
    },
    provider::{ProviderRuntime, discover_provider},
    state::{OidcAuthSession, OidcLogoutSession, OidcStateStore},
    transport::{OidcExtraRootCaProvider, ReqwestHttpClient, inspect_token_response_body, oidc_http_error_diagnostics},
};
#[cfg(test)]
use crate::federation::FederatedAuthorizationRuleRef;
use crate::{
    EVENT_OIDC_DIAGNOSTICS, LOG_COMPONENT_IAM, LOG_SUBSYSTEM_OIDC,
    federation::{
        CoreFederatedAuthorizationMapper, FederatedAuthorizationRule, FederatedAuthorizationRules, FederatedClaims,
        FederatedIdentityService, FederatedProviderQuery, FederatedProviderRef, FederatedProviderView, FederatedRedirectPolicy,
        FederationError, OpaqueLogoutContinuation, Result as FederationResult, StandardOidcAuthentication,
        VerifiedFederatedCodeExchange, VerifiedFederatedIdentity,
    },
};
use openidconnect::core::{CoreAuthenticationFlow, CoreClient, CoreIdToken};
use openidconnect::{
    Audience, AuthType, AuthorizationCode, ClientId, ClientSecret, CsrfToken, LogoutRequest, Nonce, PkceCodeChallenge,
    PkceCodeVerifier, PostLogoutRedirectUrl, RedirectUrl, RequestTokenError, Scope,
};
use rustfs_policy::policy::{ClaimLookup, get_claim_case_insensitive};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Claims extracted from an OIDC ID token.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct OidcClaims {
    pub sub: String,
    pub email: String,
    pub username: String,
    pub groups: Vec<String>,
    pub raw: HashMap<String, serde_json::Value>,
}

// ---- Core OIDC system ----

/// Global OIDC manager for all configured providers.
pub struct OidcSys {
    pub(super) configs: HashMap<String, SourcedOidcProviderConfig>,
    pub(super) provider_runtime: ProviderRuntime,
    pub(super) state_store: OidcStateStore,
}

pub(super) fn trusted_aud(other_audiences: &[String], audience: &Audience) -> bool {
    for aud in other_audiences {
        if audience.as_str() == aud.as_str() {
            return true;
        }
    }
    false
}

impl OidcSys {
    pub(crate) async fn new_with_extra_root_ca_provider(extra_root_ca_provider: OidcExtraRootCaProvider) -> Result<Self, String> {
        let http_client = ReqwestHttpClient::new_with_extra_root_ca_provider(extra_root_ca_provider)?;
        http_client.current_extra_root_certs().await.map_err(|err| err.to_string())?;
        Self::new_with_http_client(http_client).await
    }

    async fn new_with_http_client(http_client: ReqwestHttpClient) -> Result<Self, String> {
        let server_config = crate::server_config::current_server_config();
        let parsed_configs = load_oidc_config_snapshot(server_config.as_ref());
        let mut configs = HashMap::new();
        let mut provider_states = HashMap::new();

        for sourced_config in parsed_configs.into_providers() {
            let config = &sourced_config.config;
            if !config.enabled {
                debug!(provider = %config.id, "OIDC provider disabled");
                continue;
            }

            match discover_provider(config, &http_client).await {
                Ok(state) => {
                    debug!(provider = %config.id, "OIDC provider discovered");
                    provider_states.insert(config.id.clone(), state);
                    configs.insert(config.id.clone(), sourced_config);
                }
                Err(e) => {
                    error!(
                        event = EVENT_OIDC_DIAGNOSTICS,
                        component = LOG_COMPONENT_IAM,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "provider_discovery_failed",
                        provider_id = %config.id,
                        config_url = %config.config_url,
                        client_id = %config.client_id,
                        scopes = ?config.scopes,
                        redirect_uri = %config.redirect_uri.as_deref().unwrap_or(""),
                        redirect_uri_dynamic = config.redirect_uri_dynamic,
                        error = %e,
                        "oidc provider discovery failed"
                    );
                }
            }
        }

        Ok(Self {
            configs,
            provider_runtime: ProviderRuntime::new(http_client, provider_states),
            state_store: OidcStateStore::new(),
        })
    }

    /// Create an OidcSys with no providers (useful for when OIDC is not configured).
    pub fn empty() -> Result<Self, String> {
        Ok(Self {
            configs: HashMap::new(),
            provider_runtime: ProviderRuntime::new(ReqwestHttpClient::new()?, HashMap::new()),
            state_store: OidcStateStore::new(),
        })
    }

    /// Return true if any OIDC providers are configured and enabled.
    pub(crate) fn has_providers(&self) -> bool {
        !self.configs.is_empty()
    }

    pub(crate) fn provider_configs(&self) -> impl Iterator<Item = &OidcProviderConfig> {
        self.configs.values().map(|provider| &provider.config)
    }

    /// List all providers, including providers hidden from the login UI.
    pub(crate) fn list_providers(&self) -> Vec<OidcProviderSummary> {
        self.provider_configs()
            .map(|config| OidcProviderSummary {
                provider_id: config.id.clone(),
                display_name: config.display_name.clone(),
            })
            .collect()
    }

    /// List providers visible in the login UI.
    pub(crate) fn list_visible_providers(&self) -> Vec<OidcProviderSummary> {
        self.provider_configs()
            .filter(|config| !config.hide_from_ui)
            .map(|config| OidcProviderSummary {
                provider_id: config.id.clone(),
                display_name: config.display_name.clone(),
            })
            .collect()
    }

    pub(crate) fn config_snapshot(&self) -> OidcConfigSnapshot {
        OidcConfigSnapshot::new(self.configs.values().cloned().collect())
    }

    /// Build the PKCE authorization URL for a provider, store state in the state store.
    pub(crate) async fn authorize_url(
        &self,
        provider_id: &str,
        redirect_uri: &str,
        redirect_after: Option<String>,
    ) -> Result<String, String> {
        let config = self
            .get_provider_config(provider_id)
            .ok_or_else(|| format!("unknown OIDC provider: {provider_id}"))?;
        let state = self.provider_runtime.ensure_provider_state(provider_id, config).await?;

        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        let redirect = RedirectUrl::new(redirect_uri.to_string()).map_err(|e| format!("invalid redirect URI: {e}"))?;

        let client = CoreClient::from_provider_metadata(
            state.metadata.console()?.clone(),
            ClientId::new(config.client_id.clone()),
            config.client_secret.as_ref().map(|s| ClientSecret::new(s.clone())),
        )
        .set_auth_type(AuthType::RequestBody);

        let mut auth_req =
            client.authorize_url(CoreAuthenticationFlow::AuthorizationCode, CsrfToken::new_random, Nonce::new_random);
        auth_req = auth_req.set_redirect_uri(Cow::Owned(redirect));

        for scope in &config.scopes {
            auth_req = auth_req.add_scope(Scope::new(scope.clone()));
        }

        auth_req = auth_req.set_pkce_challenge(pkce_challenge);

        let (auth_url, csrf_token, nonce) = auth_req.url();

        // Store the state for callback validation
        self.state_store
            .insert(
                csrf_token.secret().clone(),
                OidcAuthSession {
                    provider_id: provider_id.to_string(),
                    pkce_verifier: pkce_verifier.secret().clone(),
                    nonce: nonce.secret().clone(),
                    redirect_after,
                },
            )
            .await;

        Ok(auth_url.to_string())
    }

    /// Exchange an authorization code for tokens and extract claims.
    pub(super) async fn exchange_code(
        &self,
        state: &str,
        code: &str,
        redirect_uri: &str,
    ) -> Result<(OidcClaims, String, OidcAuthSession, String), String> {
        // Retrieve and consume the state (single-use)
        let session = self
            .state_store
            .take(state)
            .await
            .ok_or_else(|| "invalid or expired OIDC state".to_string())?;

        let config = self
            .get_provider_config(&session.provider_id)
            .ok_or_else(|| format!("unknown provider: {}", session.provider_id))?;
        let provider_state = self.provider_runtime.get_provider_state(&session.provider_id)?;
        let issuer = provider_state.metadata.issuer().to_string();
        let token_endpoint = provider_state
            .metadata
            .token_endpoint()
            .map(ToString::to_string)
            .unwrap_or_default();

        // Construct CoreClient on-the-fly with JWKS from discovery
        let client = CoreClient::from_provider_metadata(
            provider_state.metadata.console()?.clone(),
            ClientId::new(config.client_id.clone()),
            config.client_secret.as_ref().map(|s| ClientSecret::new(s.clone())),
        )
        .set_auth_type(AuthType::RequestBody);

        let redirect = RedirectUrl::new(redirect_uri.to_string()).map_err(|e| format!("invalid redirect URI: {e}"))?;

        // Exchange code for tokens
        let token_response = client
            .exchange_code(AuthorizationCode::new(code.to_string()))
            .map_err(|e| {
                error!(
                    event = EVENT_OIDC_DIAGNOSTICS,
                    component = LOG_COMPONENT_IAM,
                    subsystem = LOG_SUBSYSTEM_OIDC,
                    result = "token_endpoint_missing",
                    provider_id = %session.provider_id,
                    config_url = %config.config_url,
                    issuer = %issuer,
                    client_id = %config.client_id,
                    redirect_uri = %redirect_uri,
                    scopes = ?config.scopes,
                    error = %e,
                    "oidc token exchange failed"
                );
                format!(
                    "token endpoint not configured: {e}: provider_id={}, config_url={}, issuer={}, redirect_uri={}, client_id={}",
                    session.provider_id, config.config_url, issuer, redirect_uri, config.client_id
                )
            })?
            .set_pkce_verifier(PkceCodeVerifier::new(session.pkce_verifier.clone()))
            .set_redirect_uri(Cow::Owned(redirect))
            .request_async(self.provider_runtime.http_client())
            .await
            .map_err(|e| match &e {
                RequestTokenError::ServerResponse(response) => {
                    error!(
                        event = EVENT_OIDC_DIAGNOSTICS,
                        component = LOG_COMPONENT_IAM,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "token_server_response",
                        provider_id = %session.provider_id,
                        config_url = %config.config_url,
                        issuer = %issuer,
                        token_endpoint = %token_endpoint,
                        client_id = %config.client_id,
                        client_secret_configured = config.client_secret.as_deref().is_some_and(|secret| !secret.is_empty()),
                        redirect_uri = %redirect_uri,
                        scopes = ?config.scopes,
                        oauth_error = %response.error(),
                        oauth_error_description = %response.error_description().map(String::as_str).unwrap_or(""),
                        oauth_error_uri = %response.error_uri().map(String::as_str).unwrap_or(""),
                        error = %e,
                        "oidc token exchange failed"
                    );
                    format!(
                        "token exchange failed: {e}: stage=token_server_response, provider_id={}, config_url={}, issuer={}, token_endpoint={}, redirect_uri={}, client_id={}, oauth_error={}, oauth_error_description={}",
                        session.provider_id,
                        config.config_url,
                        issuer,
                        token_endpoint,
                        redirect_uri,
                        config.client_id,
                        response.error(),
                        response.error_description().map(String::as_str).unwrap_or("")
                    )
                }
                RequestTokenError::Request(err) => {
                    let (request_error_kind, request_error_status) = oidc_http_error_diagnostics(err);
                    error!(
                        event = EVENT_OIDC_DIAGNOSTICS,
                        component = LOG_COMPONENT_IAM,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "token_request_failed",
                        provider_id = %session.provider_id,
                        config_url = %config.config_url,
                        issuer = %issuer,
                        token_endpoint = %token_endpoint,
                        client_id = %config.client_id,
                        redirect_uri = %redirect_uri,
                        request_error_kind = %request_error_kind,
                        request_error_status = %request_error_status,
                        error = %err,
                        "oidc token exchange failed"
                    );
                    format!(
                        "token exchange failed: stage=token_request_failed, provider_id={}, config_url={}, issuer={}, token_endpoint={}, redirect_uri={}, client_id={}, request_error_kind={}, request_error_status={}, request_error={}",
                        session.provider_id,
                        config.config_url,
                        issuer,
                        token_endpoint,
                        redirect_uri,
                        config.client_id,
                        request_error_kind,
                        request_error_status,
                        err
                    )
                }
                RequestTokenError::Parse(parse_err, body) => {
                    let shape = inspect_token_response_body(body);
                    error!(
                        event = EVENT_OIDC_DIAGNOSTICS,
                        component = LOG_COMPONENT_IAM,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "token_response_parse_failed",
                        provider_id = %session.provider_id,
                        config_url = %config.config_url,
                        issuer = %issuer,
                        token_endpoint = %token_endpoint,
                        client_id = %config.client_id,
                        redirect_uri = %redirect_uri,
                        parse_error_path = %parse_err.path(),
                        response_body_len = body.len(),
                        response_json_object = shape.json_object,
                        response_json_keys = %shape.json_keys,
                        response_has_access_token = shape.has_access_token,
                        response_has_id_token = shape.has_id_token,
                        response_has_token_type = shape.has_token_type,
                        response_has_expires_in = shape.has_expires_in,
                        response_has_error = shape.has_error,
                        response_has_error_description = shape.has_error_description,
                        response_looks_like_html = shape.looks_like_html,
                        error = %e,
                        "oidc token exchange failed"
                    );
                    format!(
                        "token exchange failed: {e}: stage=token_response_parse_failed, provider_id={}, config_url={}, issuer={}, token_endpoint={}, redirect_uri={}, client_id={}, parse_error_path={}, response_body_len={}, response_json_keys={}, response_has_id_token={}, response_has_error={}, response_looks_like_html={}",
                        session.provider_id,
                        config.config_url,
                        issuer,
                        token_endpoint,
                        redirect_uri,
                        config.client_id,
                        parse_err.path(),
                        body.len(),
                        shape.json_keys,
                        shape.has_id_token,
                        shape.has_error,
                        shape.looks_like_html
                    )
                }
                RequestTokenError::Other(message) => {
                    error!(
                        event = EVENT_OIDC_DIAGNOSTICS,
                        component = LOG_COMPONENT_IAM,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "token_exchange_other_error",
                        provider_id = %session.provider_id,
                        config_url = %config.config_url,
                        issuer = %issuer,
                        token_endpoint = %token_endpoint,
                        client_id = %config.client_id,
                        redirect_uri = %redirect_uri,
                        error = %message,
                        "oidc token exchange failed"
                    );
                    format!(
                        "token exchange failed: {e}: stage=token_exchange_other_error, provider_id={}, config_url={}, issuer={}, token_endpoint={}, redirect_uri={}, client_id={}",
                        session.provider_id, config.config_url, issuer, token_endpoint, redirect_uri, config.client_id
                    )
                }
            })?;

        // Verify the ID token (signature, issuer, audience, expiry, nonce)
        let id_token = token_response
            .extra_fields()
            .id_token()
            .ok_or_else(|| {
                error!(
                    event = EVENT_OIDC_DIAGNOSTICS,
                    component = LOG_COMPONENT_IAM,
                    subsystem = LOG_SUBSYSTEM_OIDC,
                    result = "token_response_missing_id_token",
                    provider_id = %session.provider_id,
                    config_url = %config.config_url,
                    issuer = %issuer,
                    token_endpoint = %token_endpoint,
                    client_id = %config.client_id,
                    redirect_uri = %redirect_uri,
                    scopes = ?config.scopes,
                    "oidc token exchange failed"
                );
                format!(
                    "no id_token in token response: provider_id={}, config_url={}, issuer={}, token_endpoint={}, redirect_uri={}, client_id={}, scopes={}",
                    session.provider_id,
                    config.config_url,
                    issuer,
                    token_endpoint,
                    redirect_uri,
                    config.client_id,
                    config.scopes.join(",")
                )
            })?;

        let verifier = client
            .id_token_verifier()
            .set_other_audience_verifier_fn(|aud| trusted_aud(&config.other_audiences, aud));
        let verified = id_token.claims(&verifier, &Nonce::new(session.nonce.clone()));
        if let Err(e) = verified {
            let verification_error = e.to_string();
            warn!(
                event = EVENT_OIDC_DIAGNOSTICS,
                component = LOG_COMPONENT_IAM,
                subsystem = LOG_SUBSYSTEM_OIDC,
                result = "id_token_verification_retry",
                provider_id = %session.provider_id,
                config_url = %config.config_url,
                issuer = %issuer,
                token_endpoint = %token_endpoint,
                client_id = %config.client_id,
                other_audiences = ?config.other_audiences,
                error = %verification_error,
                "oidc id token verification failed"
            );
            let refreshed_state = self
                .provider_runtime
                .refresh_provider_state(&session.provider_id, config)
                .await
                .map_err(|refresh_err| {
                    format!(
                        "ID token verification failed: {verification_error}; failed to refresh provider metadata: {refresh_err}"
                    )
                })?;

            warn!(
                event = EVENT_OIDC_DIAGNOSTICS,
                component = LOG_COMPONENT_IAM,
                subsystem = LOG_SUBSYSTEM_OIDC,
                result = "jwks_metadata_refreshed",
                provider_id = %session.provider_id,
                config_url = %config.config_url,
                issuer = %issuer,
                "oidc provider metadata refreshed"
            );

            let client = CoreClient::from_provider_metadata(
                refreshed_state.metadata.console()?.clone(),
                ClientId::new(config.client_id.clone()),
                config.client_secret.as_ref().map(|s| ClientSecret::new(s.clone())),
            )
            .set_auth_type(AuthType::RequestBody);

            let verifier = client
                .id_token_verifier()
                .set_other_audience_verifier_fn(|aud| trusted_aud(&config.other_audiences, aud));
            id_token
                .claims(&verifier, &Nonce::new(session.nonce.clone()))
                .map_err(|retry_err| {
                    error!(
                        event = EVENT_OIDC_DIAGNOSTICS,
                        component = LOG_COMPONENT_IAM,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "id_token_verification_failed",
                        provider_id = %session.provider_id,
                        config_url = %config.config_url,
                        issuer = %issuer,
                        token_endpoint = %token_endpoint,
                        client_id = %config.client_id,
                        other_audiences = ?config.other_audiences,
                        original_error = %verification_error,
                        retry_error = %retry_err,
                        "oidc id token verification failed"
                    );
                    format!(
                        "ID token verification failed after JWKS refresh: {retry_err}; original_error={verification_error}; provider_id={}, config_url={}, issuer={}, token_endpoint={}, client_id={}, other_audiences={}",
                        session.provider_id,
                        config.config_url,
                        issuer,
                        token_endpoint,
                        config.client_id,
                        config.other_audiences.join(",")
                    )
                })?;
        }

        // Extract raw claims from the verified JWT for custom claim support
        // (the crate verifies signature/expiry/nonce; we decode payload for non-standard claims)
        let raw_jwt = id_token.to_string();
        let raw = decode_jwt_payload(&raw_jwt);

        let claims = OidcClaims {
            sub: extract_string_claim(&raw, "sub"),
            email: extract_string_claim(&raw, &config.email_claim),
            username: extract_string_claim(&raw, &config.username_claim),
            groups: extract_canonical_group_values(&raw, &config.groups_claim, &config.roles_claim),
            raw,
        };

        Ok((claims, session.provider_id.clone(), session, raw_jwt))
    }

    /// Store a one-time logout session keyed by an opaque token so the console can
    /// trigger browser logout without persisting the raw ID token.
    pub(crate) async fn create_logout_token(&self, provider_id: &str, id_token: &str) -> Result<String, String> {
        if !self.configs.contains_key(provider_id) {
            return Err(format!("unknown OIDC provider: {provider_id}"));
        }

        let token = CsrfToken::new_random().secret().clone();
        self.state_store
            .insert_logout(
                token.clone(),
                OidcLogoutSession {
                    provider_id: provider_id.to_string(),
                    id_token: id_token.to_string(),
                },
            )
            .await;

        Ok(token)
    }

    /// Build the RP-initiated logout URL for a previously issued logout token.
    /// Returns `Ok(None)` when the provider does not advertise an end-session endpoint.
    pub(crate) async fn build_logout_url(
        &self,
        logout_token: &str,
        post_logout_redirect_uri: &str,
    ) -> Result<Option<String>, String> {
        let session = self
            .state_store
            .take_logout(logout_token)
            .await
            .ok_or_else(|| "invalid or expired OIDC logout token".to_string())?;

        let config = self
            .get_provider_config(&session.provider_id)
            .ok_or_else(|| format!("unknown OIDC provider: {}", session.provider_id))?;
        let state = self
            .provider_runtime
            .ensure_provider_state(&session.provider_id, config)
            .await?;
        let Some(end_session_endpoint) = state.metadata.console()?.additional_metadata().end_session_endpoint.clone() else {
            return Ok(None);
        };

        let id_token: CoreIdToken = session
            .id_token
            .parse()
            .map_err(|e: serde_json::Error| format!("failed to parse ID token for logout: {e}"))?;
        let post_logout_redirect_uri = PostLogoutRedirectUrl::new(post_logout_redirect_uri.to_string())
            .map_err(|e| format!("invalid post logout redirect URI: {e}"))?;

        let logout_url = LogoutRequest::from(end_session_endpoint)
            .set_id_token_hint(&id_token)
            .set_client_id(ClientId::new(config.client_id.clone()))
            .set_post_logout_redirect_uri(post_logout_redirect_uri)
            .http_get_url()
            .to_string();

        Ok(Some(logout_url))
    }

    /// Map OIDC claims to rustfs policy names.
    #[cfg(test)]
    pub(crate) fn map_claims_to_policies(&self, provider_id: &str, claims: &OidcClaims) -> (Vec<String>, Vec<String>) {
        let Some(rule) = self.authorization_rule(provider_id) else {
            return (Vec::new(), Vec::new());
        };
        CoreFederatedAuthorizationMapper::map_policies_and_groups(provider_id, rule, &claims.groups, &claims.raw, true)
    }

    /// Policy names produced only by the canonical groups in claim-based mode: the groups claim
    /// plus the roles claim values that `extract_canonical_group_values` merges into it.
    ///
    /// Identity providers emit built-in groups and roles that can never have a matching policy
    /// (for example `DOMAIN\Domain Users` or Keycloak's `offline_access`), so the session binding
    /// may ignore these names when no such policy exists. Names from a fixed role policy or a
    /// dedicated policy claim are never included, so those configurations keep requiring every
    /// policy to resolve.
    #[cfg(test)]
    pub(crate) fn group_claim_policy_names(&self, provider_id: &str, claims: &OidcClaims) -> Vec<String> {
        let Some(rule) = self.authorization_rule(provider_id) else {
            return Vec::new();
        };
        CoreFederatedAuthorizationMapper::group_claim_policy_names(rule, &claims.groups, &claims.raw)
    }

    #[cfg(test)]
    fn authorization_rule(&self, provider_id: &str) -> Option<FederatedAuthorizationRuleRef<'_>> {
        self.get_provider_config(provider_id).map(|config| {
            FederatedAuthorizationRuleRef::new(
                &config.claim_name,
                &config.claim_prefix,
                &config.role_policy,
                &config.groups_claim,
                &config.roles_claim,
            )
        })
    }

    /// Verify a raw JWT (id_token) for the AssumeRoleWithWebIdentity flow.
    ///
    /// Unlike the authorization code flow, ARWWI receives a raw JWT directly
    /// (not via code exchange). This method:
    /// 1. Decodes the JWT payload to extract the `iss` claim
    /// 2. Finds the OIDC provider whose issuer matches
    /// 3. Verifies signature, issuer, audience, and expiry (nonce is skipped)
    /// 4. Extracts claims using the provider's claim configuration
    pub(crate) async fn verify_web_identity_token(&self, jwt: &str) -> Result<(OidcClaims, String /* provider_id */), String> {
        // Decode JWT payload without verification to get the issuer claim
        let raw_claims = decode_jwt_payload(jwt);
        let issuer = raw_claims
            .get("iss")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "JWT missing 'iss' claim".to_string())?;

        // Find matching provider by issuer
        let (provider_id, config, mut state) = self
            .provider_runtime
            .find_provider_by_issuer(issuer, &self.configs)
            .ok_or_else(|| format!("no OIDC provider configured for issuer: {issuer}"))?;

        state = self
            .provider_runtime
            .ensure_provider_state_if_stale(&provider_id, &config, &state)
            .await?;

        // Parse raw JWT string into CoreIdToken
        let id_token: CoreIdToken = jwt
            .parse()
            .map_err(|e: serde_json::Error| format!("failed to parse JWT as ID token: {e}"))?;

        // Verify the token (signature, issuer, audience, expiry) — skip nonce
        // (nonce is only required for the authorization code flow)
        let verifier = state
            .metadata
            .verifier(&config)
            .set_other_audience_verifier_fn(|aud| trusted_aud(&config.other_audiences, aud));
        if let Err(e) = id_token.claims(&verifier, |_: Option<&Nonce>| Ok(())) {
            state = self
                .provider_runtime
                .refresh_provider_state(&provider_id, &config)
                .await
                .map_err(|refresh_err| {
                    format!("ID token verification failed: {e}; failed to refresh provider metadata: {refresh_err}")
                })?;

            let verifier = state
                .metadata
                .verifier(&config)
                .set_other_audience_verifier_fn(|aud| trusted_aud(&config.other_audiences, aud));
            id_token
                .claims(&verifier, |_: Option<&Nonce>| Ok(()))
                .map_err(|retry_err| format!("ID token verification failed after JWKS refresh: {retry_err}"))?;
        }

        // Extract claims using the provider's claim configuration
        let claims = OidcClaims {
            sub: extract_string_claim(&raw_claims, "sub"),
            email: extract_string_claim(&raw_claims, &config.email_claim),
            username: extract_string_claim(&raw_claims, &config.username_claim),
            groups: extract_canonical_group_values(&raw_claims, &config.groups_claim, &config.roles_claim),
            raw: raw_claims,
        };

        Ok((claims, provider_id.to_string()))
    }
    /// Get a provider config by ID.
    pub(crate) fn get_provider_config(&self, id: &str) -> Option<&OidcProviderConfig> {
        self.configs.get(id).map(|provider| &provider.config)
    }
}

/// Decode the payload section of a JWT without validation (token must already be verified).
pub(crate) fn decode_jwt_payload(token: &str) -> HashMap<String, serde_json::Value> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return HashMap::new();
    }
    let payload_bytes = base64_simd::URL_SAFE_NO_PAD.decode_to_vec(parts[1]);
    match payload_bytes {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

/// Extract a string claim from raw claims with case-insensitive fallback.
fn extract_string_claim(claims: &HashMap<String, serde_json::Value>, key: &str) -> String {
    match get_claim_case_insensitive(claims, key) {
        ClaimLookup::Found(value) => value.as_str().unwrap_or_default().to_string(),
        ClaimLookup::Missing | ClaimLookup::Ambiguous => String::new(),
    }
}

/// Extract a groups/array claim from raw claims with case-insensitive fallback. Handles both string arrays and single strings.
fn extract_groups_claim(claims: &HashMap<String, serde_json::Value>, key: &str) -> Vec<String> {
    match get_claim_case_insensitive(claims, key) {
        ClaimLookup::Found(serde_json::Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
        ClaimLookup::Found(serde_json::Value::String(s)) => s.split(',').map(|s| s.trim().to_string()).collect(),
        _ => vec![],
    }
}

fn extract_canonical_group_values(
    claims: &HashMap<String, serde_json::Value>,
    groups_claim: &str,
    roles_claim: &str,
) -> Vec<String> {
    let mut groups = extract_groups_claim(claims, groups_claim);
    if !roles_claim.is_empty() && roles_claim != groups_claim {
        groups.extend(extract_groups_claim(claims, roles_claim));
    }
    groups.retain(|g| !g.is_empty());
    groups.sort();
    groups.dedup();
    groups
}

fn verified_identity(provider: FederatedProviderRef, claims: OidcClaims) -> VerifiedFederatedIdentity {
    VerifiedFederatedIdentity::from_claims(
        provider,
        FederatedClaims {
            sub: claims.sub,
            email: claims.email,
            username: claims.username,
            groups: claims.groups,
            raw: claims.raw,
        },
    )
}

fn authorization_rules(oidc: &OidcSys) -> FederatedAuthorizationRules {
    FederatedAuthorizationRules::new(oidc.provider_configs().map(|config| {
        FederatedAuthorizationRule::new(
            config.id.clone(),
            config.claim_name.clone(),
            config.claim_prefix.clone(),
            config.role_policy.clone(),
            config.groups_claim.clone(),
            config.roles_claim.clone(),
        )
    }))
}

pub struct StandardOidcAdapter {
    oidc: Arc<OidcSys>,
    authorization_rules: FederatedAuthorizationRules,
}

impl StandardOidcAdapter {
    pub fn new(oidc: Arc<OidcSys>) -> Self {
        let authorization_rules = authorization_rules(&oidc);
        Self {
            oidc,
            authorization_rules,
        }
    }

    pub fn authorization_rules(&self) -> FederatedAuthorizationRules {
        self.authorization_rules.clone()
    }

    pub fn into_service(self: Arc<Self>, mapper: CoreFederatedAuthorizationMapper) -> FederatedIdentityService {
        let authentication: Arc<dyn StandardOidcAuthentication> = self.clone();
        let provider_query: Arc<dyn FederatedProviderQuery> = self;
        FederatedIdentityService::from_standard_oidc_parts(provider_query, authentication, mapper)
    }
}

impl OidcConfigQuery for StandardOidcAdapter {
    fn config_snapshot(&self) -> OidcConfigSnapshot {
        self.oidc.config_snapshot()
    }
}

#[async_trait::async_trait]
impl StandardOidcAuthentication for StandardOidcAdapter {
    async fn authorize_url(
        &self,
        provider_id: &str,
        redirect_uri: &str,
        redirect_after: Option<String>,
    ) -> FederationResult<String> {
        self.oidc
            .authorize_url(provider_id, redirect_uri, redirect_after)
            .await
            .map_err(FederationError::Authorization)
    }

    async fn exchange_identity(
        &self,
        state: &str,
        code: &str,
        redirect_uri: &str,
    ) -> FederationResult<VerifiedFederatedCodeExchange> {
        let (claims, provider_id, session, id_token) = self
            .oidc
            .exchange_code(state, code, redirect_uri)
            .await
            .map_err(FederationError::CodeExchange)?;
        let provider = FederatedProviderRef::new(provider_id);
        Ok(VerifiedFederatedCodeExchange::new(
            verified_identity(provider.clone(), claims),
            session.redirect_after,
            OpaqueLogoutContinuation::new(provider, id_token),
        ))
    }

    async fn verify_identity(&self, jwt: &str) -> FederationResult<VerifiedFederatedIdentity> {
        let (claims, provider_id) = self
            .oidc
            .verify_web_identity_token(jwt)
            .await
            .map_err(FederationError::TokenVerification)?;
        Ok(verified_identity(FederatedProviderRef::new(provider_id), claims))
    }

    async fn create_logout_token(&self, continuation: OpaqueLogoutContinuation) -> FederationResult<String> {
        let (provider, id_token) = continuation.into_parts();
        self.oidc
            .create_logout_token(provider.as_str(), &id_token)
            .await
            .map_err(FederationError::Logout)
    }

    async fn build_logout_url(&self, logout_token: &str, post_logout_redirect_uri: &str) -> FederationResult<Option<String>> {
        self.oidc
            .build_logout_url(logout_token, post_logout_redirect_uri)
            .await
            .map_err(FederationError::Logout)
    }
}

impl FederatedProviderQuery for StandardOidcAdapter {
    fn has_providers(&self) -> bool {
        self.oidc.has_providers()
    }

    fn list_providers(&self) -> Vec<FederatedProviderView> {
        self.oidc.list_providers()
    }

    fn list_visible_providers(&self) -> Vec<FederatedProviderView> {
        self.oidc.list_visible_providers()
    }

    fn redirect_policy(&self, provider_id: &str) -> Option<FederatedRedirectPolicy> {
        self.oidc
            .get_provider_config(provider_id)
            .map(|config| FederatedRedirectPolicy {
                redirect_uri: config.redirect_uri.clone(),
                allow_request_origin: config.redirect_uri_dynamic,
            })
    }
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
