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

//! OIDC Provider Manager
//!
//! Implements the OpenID Connect Authorization Code Flow with PKCE using the
//! `openidconnect` crate for standards-compliant discovery, token exchange,
//! and ID token verification.

use crate::oidc_state::{OidcAuthSession, OidcStateStore};
use openidconnect::core::{CoreAuthenticationFlow, CoreClient, CoreIdToken, CoreProviderMetadata};
use openidconnect::{
    AsyncHttpClient, Audience, AuthType, AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope,
};
use reqwest::Client;
use rustfs_config::oidc::*;
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, EnableState};
use rustfs_ecstore::config::{Config as ServerConfig, KVS, get_global_server_config};
use rustfs_policy::policy::{ClaimLookup, get_claim_case_insensitive};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::RwLock;
use std::time::{Duration as StdDuration, Instant};
use tokio::time::sleep;
use tracing::{error, info, warn};
use url::Url;

const OIDC_JWKS_REFRESH_INTERVAL: StdDuration = StdDuration::from_secs(24 * 60 * 60);
const OIDC_DISCOVERY_TRANSPORT_RETRIES: usize = 3;
const OIDC_DISCOVERY_TRANSPORT_RETRY_DELAY: StdDuration = StdDuration::from_millis(50);

// ---- HTTP Client Adapter ----

/// Error type for the OIDC HTTP client adapter.
#[derive(Debug)]
pub enum OidcHttpError {
    Reqwest(reqwest::Error),
    Http(http::Error),
}

impl std::fmt::Display for OidcHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reqwest(e) => write!(f, "{e}"),
            Self::Http(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for OidcHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Reqwest(e) => Some(e),
            Self::Http(e) => Some(e),
        }
    }
}

/// HTTP client adapter bridging reqwest 0.13 to the `openidconnect` `AsyncHttpClient` trait.
pub(crate) struct ReqwestHttpClient {
    default: Client,
    no_proxy: Client,
}

fn build_oidc_http_client(disable_proxy: bool) -> Result<Client, String> {
    let mut builder = reqwest::Client::builder();
    if disable_proxy {
        builder = builder.no_proxy();
    }
    builder
        .build()
        .map_err(|err| format!("failed to build OIDC reqwest client: {err}"))
}

fn should_bypass_proxy_for_oidc_uri(uri: &str) -> bool {
    let Some(host) = Url::parse(uri).ok().and_then(|url| url.host_str().map(str::to_owned)) else {
        return false;
    };
    let host = host.trim_matches(['[', ']']);

    host.eq_ignore_ascii_case("localhost") || host.parse::<IpAddr>().is_ok_and(|addr| addr.is_loopback())
}

impl ReqwestHttpClient {
    fn new() -> Result<Self, String> {
        Ok(Self {
            default: build_oidc_http_client(false)?,
            no_proxy: build_oidc_http_client(true)?,
        })
    }

    fn client_for_uri(&self, uri: &str) -> &Client {
        if should_bypass_proxy_for_oidc_uri(uri) {
            &self.no_proxy
        } else {
            &self.default
        }
    }
}

impl<'c> AsyncHttpClient<'c> for ReqwestHttpClient {
    type Error = OidcHttpError;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<Vec<u8>>, Self::Error>> + Send + 'c>>;

    fn call(&'c self, request: http::Request<Vec<u8>>) -> Self::Future {
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let uri = parts.uri.to_string();
            let client = self.client_for_uri(&uri);
            let response = client
                .request(parts.method, uri)
                .headers(parts.headers)
                .body(body)
                .send()
                .await
                .map_err(OidcHttpError::Reqwest)?;

            let status = response.status();
            let headers = response.headers().clone();
            let body_bytes = response.bytes().await.map_err(OidcHttpError::Reqwest)?;

            let mut http_response = http::Response::builder()
                .status(status)
                .body(body_bytes.to_vec())
                .map_err(OidcHttpError::Http)?;
            *http_response.headers_mut() = headers;

            Ok(http_response)
        })
    }
}

// ---- Public types (unchanged API) ----

/// Parsed configuration for a single OIDC provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcProviderConfig {
    pub id: String,
    pub enabled: bool,
    pub config_url: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
    pub other_audiences: Vec<String>,
    pub redirect_uri: Option<String>,
    pub redirect_uri_dynamic: bool,
    pub claim_name: String,
    pub claim_prefix: String,
    pub role_policy: String,
    pub display_name: String,
    pub groups_claim: String,
    pub roles_claim: String,
    pub email_claim: String,
    pub username_claim: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OidcProviderConfigSource {
    Env,
    Persisted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcedOidcProviderConfig {
    pub config: OidcProviderConfig,
    pub source: OidcProviderConfigSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OidcProviderValidationResult {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: Option<String>,
}

/// Summary info about a provider, returned to the console.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcProviderSummary {
    pub provider_id: String,
    pub display_name: String,
}

/// Claims extracted from an OIDC ID token.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OidcClaims {
    pub sub: String,
    pub email: String,
    pub username: String,
    pub groups: Vec<String>,
    pub raw: HashMap<String, serde_json::Value>,
}

// ---- Internal provider state ----

/// Discovered OIDC provider metadata.
/// We store metadata (which includes JWKS after discovery) separately rather than
/// a `CoreClient` because the crate uses type-state generics that make storing
/// the configured client in a HashMap impractical. The client is reconstructed
/// on-the-fly from metadata when needed.
#[derive(Clone)]
struct ProviderState {
    metadata: CoreProviderMetadata,
    discovered_at: Instant,
}

impl ProviderState {
    fn is_stale(&self) -> bool {
        self.discovered_at.elapsed() >= OIDC_JWKS_REFRESH_INTERVAL
    }
}

// ---- Core OIDC system ----

/// Global OIDC manager for all configured providers.
pub struct OidcSys {
    configs: HashMap<String, OidcProviderConfig>,
    provider_states: RwLock<HashMap<String, ProviderState>>,
    state_store: OidcStateStore,
    http_client: ReqwestHttpClient,
}

fn trusted_aud(other_audiences: &[String], audience: &Audience) -> bool {
    for aud in other_audiences {
        if audience.as_str() == aud.as_str() {
            return true;
        }
    }
    false
}

impl OidcSys {
    /// Parse environment variables and discover all configured OIDC providers.
    pub async fn new() -> Result<Self, String> {
        let http_client = ReqwestHttpClient::new()?;
        let parsed_configs = load_effective_oidc_provider_configs(get_global_server_config().as_ref());
        let mut configs = HashMap::new();
        let mut provider_states = HashMap::new();

        for sourced_config in parsed_configs {
            let config = sourced_config.config;
            if !config.enabled {
                info!("OIDC provider '{}' is disabled, skipping", config.id);
                continue;
            }

            match Self::discover_provider(&config, &http_client).await {
                Ok(state) => {
                    info!("OIDC provider '{}' discovered successfully", config.id);
                    provider_states.insert(config.id.clone(), state);
                    configs.insert(config.id.clone(), config);
                }
                Err(e) => {
                    error!("Failed to discover OIDC provider '{}': {}", config.id, e);
                }
            }
        }

        Ok(Self {
            configs,
            provider_states: RwLock::new(provider_states),
            state_store: OidcStateStore::new(),
            http_client,
        })
    }

    /// Create an OidcSys with no providers (useful for when OIDC is not configured).
    pub fn empty() -> Result<Self, String> {
        Ok(Self {
            configs: HashMap::new(),
            provider_states: RwLock::new(HashMap::new()),
            state_store: OidcStateStore::new(),
            http_client: ReqwestHttpClient::new()?,
        })
    }

    /// Return true if any OIDC providers are configured and enabled.
    pub fn has_providers(&self) -> bool {
        !self.configs.is_empty()
    }

    /// Return provider summaries for the console UI.
    pub fn list_providers(&self) -> Vec<OidcProviderSummary> {
        self.configs
            .values()
            .map(|c| OidcProviderSummary {
                provider_id: c.id.clone(),
                display_name: c.display_name.clone(),
            })
            .collect()
    }

    /// Build the PKCE authorization URL for a provider, store state in the state store.
    pub async fn authorize_url(
        &self,
        provider_id: &str,
        redirect_uri: &str,
        redirect_after: Option<String>,
    ) -> Result<String, String> {
        let config = self
            .configs
            .get(provider_id)
            .ok_or_else(|| format!("unknown OIDC provider: {provider_id}"))?;
        let state = self.ensure_provider_state(provider_id, config).await?;

        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        let redirect = RedirectUrl::new(redirect_uri.to_string()).map_err(|e| format!("invalid redirect URI: {e}"))?;

        let client = CoreClient::from_provider_metadata(
            state.metadata.clone(),
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
    pub async fn exchange_code(
        &self,
        state: &str,
        code: &str,
        redirect_uri: &str,
    ) -> Result<(OidcClaims, String, OidcAuthSession), String> {
        // Retrieve and consume the state (single-use)
        let session = self
            .state_store
            .take(state)
            .await
            .ok_or_else(|| "invalid or expired OIDC state".to_string())?;

        let config = self
            .configs
            .get(&session.provider_id)
            .ok_or_else(|| format!("unknown provider: {}", session.provider_id))?;
        let provider_state = self.get_provider_state(&session.provider_id)?;

        // Construct CoreClient on-the-fly with JWKS from discovery
        let client = CoreClient::from_provider_metadata(
            provider_state.metadata.clone(),
            ClientId::new(config.client_id.clone()),
            config.client_secret.as_ref().map(|s| ClientSecret::new(s.clone())),
        )
        .set_auth_type(AuthType::RequestBody);

        let redirect = RedirectUrl::new(redirect_uri.to_string()).map_err(|e| format!("invalid redirect URI: {e}"))?;

        // Exchange code for tokens
        let token_response = client
            .exchange_code(AuthorizationCode::new(code.to_string()))
            .map_err(|e| format!("token endpoint not configured: {e}"))?
            .set_pkce_verifier(PkceCodeVerifier::new(session.pkce_verifier.clone()))
            .set_redirect_uri(Cow::Owned(redirect))
            .request_async(&self.http_client)
            .await
            .map_err(|e| format!("token exchange failed: {e}"))?;

        // Verify the ID token (signature, issuer, audience, expiry, nonce)
        let id_token = token_response
            .extra_fields()
            .id_token()
            .ok_or_else(|| "no id_token in token response".to_string())?;

        let verifier = client
            .id_token_verifier()
            .set_other_audience_verifier_fn(|aud| trusted_aud(&config.other_audiences, aud));
        let verified = id_token.claims(&verifier, &Nonce::new(session.nonce.clone()));
        if let Err(e) = verified {
            let refreshed_state = self
                .refresh_provider_state(&session.provider_id, config)
                .await
                .map_err(|refresh_err| {
                    format!("ID token verification failed: {e}; failed to refresh provider metadata: {refresh_err}")
                })?;

            warn!(
                "OIDC provider '{}' JWKS metadata refreshed and verification retried after failure",
                session.provider_id
            );

            let client = CoreClient::from_provider_metadata(
                refreshed_state.metadata,
                ClientId::new(config.client_id.clone()),
                config.client_secret.as_ref().map(|s| ClientSecret::new(s.clone())),
            )
            .set_auth_type(AuthType::RequestBody);

            let verifier = client
                .id_token_verifier()
                .set_other_audience_verifier_fn(|aud| trusted_aud(&config.other_audiences, aud));
            id_token
                .claims(&verifier, &Nonce::new(session.nonce.clone()))
                .map_err(|retry_err| format!("ID token verification failed after JWKS refresh: {retry_err}"))?;
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

        Ok((claims, session.provider_id.clone(), session))
    }

    /// Map OIDC claims to rustfs policy names.
    pub fn map_claims_to_policies(&self, provider_id: &str, claims: &OidcClaims) -> (Vec<String>, Vec<String>) {
        let config = match self.configs.get(provider_id) {
            Some(c) => c,
            None => return (vec![], vec![]),
        };

        let mut policies = Vec::new();
        let mut groups = Vec::new();

        // Add default role policy if configured
        if !config.role_policy.is_empty() {
            for policy in config.role_policy.split(',') {
                let policy = policy.trim();
                if !policy.is_empty() {
                    policies.push(policy.to_string());
                }
            }
        }

        // Map groups claim to policies
        for group in &claims.groups {
            groups.push(group.clone());
            let policy_name = if config.claim_prefix.is_empty() {
                group.clone()
            } else {
                format!("{}{}", config.claim_prefix, group)
            };
            policies.push(policy_name);
        }

        // Map primary claim (if different from groups)
        if config.claim_name != config.groups_claim {
            let claim_values = extract_groups_claim(&claims.raw, &config.claim_name);
            for val in claim_values {
                let policy_name = if config.claim_prefix.is_empty() {
                    val
                } else {
                    format!("{}{}", config.claim_prefix, val)
                };
                policies.push(policy_name);
            }
        }

        // Deduplicate
        policies.sort();
        policies.dedup();
        groups.sort();
        groups.dedup();

        (policies, groups)
    }

    /// Verify a raw JWT (id_token) for the AssumeRoleWithWebIdentity flow.
    ///
    /// Unlike the authorization code flow, ARWWI receives a raw JWT directly
    /// (not via code exchange). This method:
    /// 1. Decodes the JWT payload to extract the `iss` claim
    /// 2. Finds the OIDC provider whose issuer matches
    /// 3. Verifies signature, issuer, audience, and expiry (nonce is skipped)
    /// 4. Extracts claims using the provider's claim configuration
    pub async fn verify_web_identity_token(&self, jwt: &str) -> Result<(OidcClaims, String /* provider_id */), String> {
        // Decode JWT payload without verification to get the issuer claim
        let raw_claims = decode_jwt_payload(jwt);
        let issuer = raw_claims
            .get("iss")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "JWT missing 'iss' claim".to_string())?;

        // Find matching provider by issuer
        let (provider_id, config, mut state) = self
            .find_provider_by_issuer(issuer)
            .ok_or_else(|| format!("no OIDC provider configured for issuer: {issuer}"))?;

        state = self.ensure_provider_state_if_stale(&provider_id, &config, &state).await?;

        // Reconstruct CoreClient from provider metadata
        let client = CoreClient::from_provider_metadata(
            state.metadata.clone(),
            ClientId::new(config.client_id.clone()),
            config.client_secret.as_ref().map(|s| ClientSecret::new(s.clone())),
        )
        .set_auth_type(AuthType::RequestBody);

        // Parse raw JWT string into CoreIdToken
        let id_token: CoreIdToken = jwt
            .parse()
            .map_err(|e: serde_json::Error| format!("failed to parse JWT as ID token: {e}"))?;

        // Verify the token (signature, issuer, audience, expiry) — skip nonce
        // (nonce is only required for the authorization code flow)
        let verifier = client
            .id_token_verifier()
            .set_other_audience_verifier_fn(|aud| trusted_aud(&config.other_audiences, aud));
        if let Err(e) = id_token.claims(&verifier, |_: Option<&Nonce>| Ok(())) {
            state = self
                .refresh_provider_state(&provider_id, &config)
                .await
                .map_err(|refresh_err| {
                    format!("ID token verification failed: {e}; failed to refresh provider metadata: {refresh_err}")
                })?;

            let client = CoreClient::from_provider_metadata(
                state.metadata,
                ClientId::new(config.client_id.clone()),
                config.client_secret.as_ref().map(|s| ClientSecret::new(s.clone())),
            )
            .set_auth_type(AuthType::RequestBody);
            let verifier = client
                .id_token_verifier()
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

    /// Find a provider whose discovered issuer matches the given JWT issuer string.
    fn find_provider_by_issuer(&self, issuer: &str) -> Option<(String, OidcProviderConfig, ProviderState)> {
        let (issuer_scheme, issuer_host, issuer_port, issuer_path) = normalize_issuer(issuer)?;
        let map = self
            .provider_states
            .read()
            .map_err(|e| format!("provider state lock poisoned: {e}"))
            .ok()?;
        for (id, state) in map.iter() {
            let provider_issuer = state.metadata.issuer().as_str();
            let Some((provider_scheme, provider_host, provider_port, provider_path)) = normalize_issuer(provider_issuer) else {
                continue;
            };

            if issuer_scheme == provider_scheme
                && issuer_host == provider_host
                && issuer_port == provider_port
                && issuer_path == provider_path
                && let Some(config) = self.configs.get(id)
            {
                return Some((id.clone(), config.clone(), state.clone()));
            }
        }
        None
    }

    fn get_provider_state(&self, provider_id: &str) -> Result<ProviderState, String> {
        self.provider_states
            .read()
            .map_err(|e| format!("provider state lock poisoned: {e}"))?
            .get(provider_id)
            .cloned()
            .ok_or_else(|| format!("provider not discovered: {provider_id}"))
    }

    async fn refresh_provider_state(&self, provider_id: &str, config: &OidcProviderConfig) -> Result<ProviderState, String> {
        let state = Self::discover_provider(config, &self.http_client).await?;
        let mut map = self.provider_states.write().map_err(|e| {
            let msg = e.to_string();
            format!("provider state lock poisoned: {msg}")
        })?;
        map.insert(provider_id.to_string(), state.clone());

        Ok(state)
    }

    async fn ensure_provider_state(&self, provider_id: &str, config: &OidcProviderConfig) -> Result<ProviderState, String> {
        let state = self.get_provider_state(provider_id)?;
        if state.is_stale() {
            self.refresh_provider_state(provider_id, config).await.or_else(|refresh_err| {
                warn!(
                    "OIDC provider '{}' JWKS metadata refresh skipped due to transient network issue: {}",
                    provider_id, refresh_err
                );
                Ok(state)
            })
        } else {
            Ok(state)
        }
    }

    async fn ensure_provider_state_if_stale(
        &self,
        provider_id: &str,
        config: &OidcProviderConfig,
        state: &ProviderState,
    ) -> Result<ProviderState, String> {
        if state.is_stale() {
            self.refresh_provider_state(provider_id, config).await.or_else(|refresh_err| {
                warn!(
                    "OIDC provider '{}' JWKS metadata refresh skipped due to transient network issue: {}",
                    provider_id, refresh_err
                );
                Ok(state.clone())
            })
        } else {
            Ok(state.clone())
        }
    }

    /// Get the state store (used by HTTP handlers).
    pub fn state_store(&self) -> &OidcStateStore {
        &self.state_store
    }

    /// Get a provider config by ID.
    pub fn get_provider_config(&self, id: &str) -> Option<&OidcProviderConfig> {
        self.configs.get(id)
    }

    /// Parse all OIDC provider configs from environment variables.
    fn parse_env_configs() -> Vec<OidcProviderConfig> {
        let mut configs = Vec::new();

        // Check for the default provider (no suffix)
        if let Some(config) = Self::parse_single_provider("", "default") {
            configs.push(config);
        }

        // Scan for suffixed providers by checking all OIDC env var prefixes.
        // This allows providers to be discovered without requiring a separate ENABLE_ key.
        let mut provider_ids: Vec<String> = Vec::new();
        let scan_prefixes: Vec<String> = ENV_IDENTITY_OPENID_KEYS.iter().map(|k| format!("{k}_")).collect();
        for (key, _) in std::env::vars() {
            for prefix in &scan_prefixes {
                if let Some(suffix) = key.strip_prefix(prefix.as_str())
                    && !suffix.is_empty()
                    && suffix != "default"
                {
                    provider_ids.push(suffix.to_string());
                    break;
                }
            }
        }

        provider_ids.sort();
        provider_ids.dedup();

        for id in provider_ids {
            let suffix = format!("_{id}");
            if let Some(config) = Self::parse_single_provider(&suffix, &id) {
                configs.push(config);
            }
        }

        configs
    }

    fn parse_persisted_configs(cfg: &ServerConfig) -> Vec<OidcProviderConfig> {
        let Some(subsystem) = cfg.0.get(IDENTITY_OPENID_SUB_SYS) else {
            return Vec::new();
        };

        let mut configs = Vec::new();
        let mut provider_ids: Vec<String> = subsystem.keys().cloned().collect();
        provider_ids.sort();

        for raw_id in provider_ids {
            let Some(kvs) = subsystem.get(&raw_id) else {
                continue;
            };

            let id = if raw_id == DEFAULT_DELIMITER {
                "default"
            } else {
                raw_id.as_str()
            };
            if let Some(config) = Self::parse_single_persisted_provider(kvs, id) {
                configs.push(config);
            }
        }

        configs
    }

    /// Parse a single provider's config from env vars with the given suffix.
    fn parse_single_provider(env_suffix: &str, id: &str) -> Option<OidcProviderConfig> {
        let get_env = |base: &str| -> String { std::env::var(format!("{base}{env_suffix}")).unwrap_or_default() };

        let enable_val = get_env(ENV_IDENTITY_OPENID_ENABLE);
        let config_url = get_env(ENV_IDENTITY_OPENID_CONFIG_URL);

        // Skip if no config URL
        if config_url.is_empty() {
            return None;
        }

        let enabled = enable_val.is_empty()
            || enable_val
                .parse::<rustfs_config::EnableState>()
                .map(|s| s.is_enabled())
                .unwrap_or(false);

        let scopes_str = get_env(ENV_IDENTITY_OPENID_SCOPES);
        let scopes = if scopes_str.is_empty() {
            OIDC_DEFAULT_SCOPES.split(',').map(String::from).collect()
        } else {
            scopes_str.split(',').map(|s| s.trim().to_string()).collect()
        };

        let other_audiences_str = get_env(ENV_IDENTITY_OPENID_OTHER_AUDIENCES);
        let other_audiences = other_audiences_str.split(',').map(|s| s.trim().to_string()).collect();

        let redirect_uri_dynamic_str = get_env(ENV_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC);
        let redirect_uri_dynamic = redirect_uri_dynamic_str.is_empty()
            || redirect_uri_dynamic_str
                .parse::<rustfs_config::EnableState>()
                .map(|s| s.is_enabled())
                .unwrap_or(true);

        let claim_name = {
            let v = get_env(ENV_IDENTITY_OPENID_CLAIM_NAME);
            if v.is_empty() {
                OIDC_DEFAULT_CLAIM_NAME.to_string()
            } else {
                v
            }
        };
        let groups_claim = {
            let v = get_env(ENV_IDENTITY_OPENID_GROUPS_CLAIM);
            if v.is_empty() {
                OIDC_DEFAULT_GROUPS_CLAIM.to_string()
            } else {
                v
            }
        };
        let roles_claim = get_env(ENV_IDENTITY_OPENID_ROLES_CLAIM);
        let email_claim = {
            let v = get_env(ENV_IDENTITY_OPENID_EMAIL_CLAIM);
            if v.is_empty() {
                OIDC_DEFAULT_EMAIL_CLAIM.to_string()
            } else {
                v
            }
        };
        let username_claim = {
            let v = get_env(ENV_IDENTITY_OPENID_USERNAME_CLAIM);
            if v.is_empty() {
                OIDC_DEFAULT_USERNAME_CLAIM.to_string()
            } else {
                v
            }
        };
        let display_name = {
            let v = get_env(ENV_IDENTITY_OPENID_DISPLAY_NAME);
            if v.is_empty() { id.to_string() } else { v }
        };
        let redirect_uri = {
            let v = get_env(ENV_IDENTITY_OPENID_REDIRECT_URI);
            if v.is_empty() { None } else { Some(v) }
        };
        let client_secret = {
            let v = get_env(ENV_IDENTITY_OPENID_CLIENT_SECRET);
            if v.is_empty() { None } else { Some(v) }
        };

        Some(OidcProviderConfig {
            id: id.to_string(),
            enabled,
            config_url,
            client_id: get_env(ENV_IDENTITY_OPENID_CLIENT_ID),
            client_secret,
            scopes,
            other_audiences,
            redirect_uri,
            redirect_uri_dynamic,
            claim_name,
            claim_prefix: get_env(ENV_IDENTITY_OPENID_CLAIM_PREFIX),
            role_policy: get_env(ENV_IDENTITY_OPENID_ROLE_POLICY),
            display_name,
            groups_claim,
            roles_claim,
            email_claim,
            username_claim,
        })
    }

    fn parse_single_persisted_provider(kvs: &KVS, id: &str) -> Option<OidcProviderConfig> {
        let config_url = kvs.get(OIDC_CONFIG_URL);
        if config_url.is_empty() {
            return None;
        }

        let enabled = kvs
            .lookup(ENABLE_KEY)
            .unwrap_or_else(|| EnableState::Off.to_string())
            .parse::<EnableState>()
            .map(|s| s.is_enabled())
            .unwrap_or(false);

        let scopes_str = kvs.get(OIDC_SCOPES);
        let scopes = if scopes_str.is_empty() {
            OIDC_DEFAULT_SCOPES.split(',').map(String::from).collect()
        } else {
            scopes_str.split(',').map(|s| s.trim().to_string()).collect()
        };

        let other_audiences_str = kvs.get(OIDC_OTHER_AUDIENCES);
        let other_audiences = other_audiences_str.split(',').map(|s| s.trim().to_string()).collect();

        let redirect_uri_dynamic = kvs
            .lookup(OIDC_REDIRECT_URI_DYNAMIC)
            .unwrap_or_else(|| EnableState::On.to_string())
            .parse::<EnableState>()
            .map(|s| s.is_enabled())
            .unwrap_or(true);

        let claim_name = kvs
            .lookup(OIDC_CLAIM_NAME)
            .unwrap_or_else(|| OIDC_DEFAULT_CLAIM_NAME.to_string());
        let groups_claim = kvs
            .lookup(OIDC_GROUPS_CLAIM)
            .unwrap_or_else(|| OIDC_DEFAULT_GROUPS_CLAIM.to_string());
        let roles_claim = kvs
            .lookup(OIDC_ROLES_CLAIM)
            .unwrap_or_else(|| OIDC_DEFAULT_ROLES_CLAIM.to_string());
        let email_claim = kvs
            .lookup(OIDC_EMAIL_CLAIM)
            .unwrap_or_else(|| OIDC_DEFAULT_EMAIL_CLAIM.to_string());
        let username_claim = kvs
            .lookup(OIDC_USERNAME_CLAIM)
            .unwrap_or_else(|| OIDC_DEFAULT_USERNAME_CLAIM.to_string());
        let display_name = kvs.lookup(OIDC_DISPLAY_NAME).unwrap_or_else(|| id.to_string());
        let redirect_uri = kvs.lookup(OIDC_REDIRECT_URI).filter(|v| !v.is_empty());
        let client_secret = kvs.lookup(OIDC_CLIENT_SECRET).filter(|v| !v.is_empty());

        Some(OidcProviderConfig {
            id: id.to_string(),
            enabled,
            config_url,
            client_id: kvs.get(OIDC_CLIENT_ID),
            client_secret,
            scopes,
            other_audiences,
            redirect_uri,
            redirect_uri_dynamic,
            claim_name,
            claim_prefix: kvs.get(OIDC_CLAIM_PREFIX),
            role_policy: kvs.get(OIDC_ROLE_POLICY),
            display_name,
            groups_claim,
            roles_claim,
            email_claim,
            username_claim,
        })
    }

    /// Perform OIDC discovery for a provider.
    /// `discover_async` fetches the discovery document and JWKS in one step.
    async fn discover_provider(config: &OidcProviderConfig, http_client: &ReqwestHttpClient) -> Result<ProviderState, String> {
        // The openidconnect crate expects the issuer URL (base), not the
        // .well-known/openid-configuration URL.
        let base_issuer = normalize_config_url(&config.config_url)?;
        let candidates = issuer_candidates(&base_issuer);
        let mut last_errors = Vec::new();

        for candidate_issuer in candidates.iter() {
            let issuer_url = IssuerUrl::new(candidate_issuer.clone()).map_err(|e| format!("invalid issuer URL: {e}"))?;

            for attempt in 0..OIDC_DISCOVERY_TRANSPORT_RETRIES {
                match CoreProviderMetadata::discover_async(issuer_url.clone(), http_client)
                    .await
                    .map_err(|e| format!("discovery failed: {e}"))
                {
                    Ok(metadata) => {
                        return Ok(ProviderState {
                            metadata,
                            discovered_at: Instant::now(),
                        });
                    }
                    Err(error) => {
                        let is_transient_transport = error.contains("Request failed");
                        let should_retry = is_transient_transport && attempt + 1 < OIDC_DISCOVERY_TRANSPORT_RETRIES;
                        if should_retry {
                            warn!(
                                "OIDC provider '{}' discovery transport attempt {}/{} failed for issuer '{}': {}",
                                config.id,
                                attempt + 1,
                                OIDC_DISCOVERY_TRANSPORT_RETRIES,
                                candidate_issuer,
                                error
                            );
                            sleep(OIDC_DISCOVERY_TRANSPORT_RETRY_DELAY).await;
                            continue;
                        }

                        last_errors.push(format!("issuer '{candidate_issuer}': {error}"));
                        warn!(
                            "OIDC provider '{}' discovery attempt failed for issuer '{}': {}",
                            config.id, candidate_issuer, error
                        );
                        break;
                    }
                }
            }
        }

        Err(format!(
            "discovery failed for all issuer variants {:?}: {}",
            candidates,
            last_errors.join("; ")
        ))
    }
}

pub fn load_oidc_provider_configs_from_env() -> Vec<OidcProviderConfig> {
    OidcSys::parse_env_configs()
}

pub fn load_oidc_provider_configs_from_server_config(cfg: &ServerConfig) -> Vec<OidcProviderConfig> {
    OidcSys::parse_persisted_configs(cfg)
}

pub fn merge_oidc_provider_configs(
    env_configs: Vec<OidcProviderConfig>,
    persisted_configs: Vec<OidcProviderConfig>,
) -> Vec<SourcedOidcProviderConfig> {
    let mut effective = HashMap::new();

    for config in persisted_configs {
        effective.insert(
            config.id.clone(),
            SourcedOidcProviderConfig {
                config,
                source: OidcProviderConfigSource::Persisted,
            },
        );
    }

    for config in env_configs {
        effective.insert(
            config.id.clone(),
            SourcedOidcProviderConfig {
                config,
                source: OidcProviderConfigSource::Env,
            },
        );
    }

    let mut configs: Vec<SourcedOidcProviderConfig> = effective.into_values().collect();
    configs.sort_by(|lhs, rhs| lhs.config.id.cmp(&rhs.config.id));
    configs
}

pub fn load_effective_oidc_provider_configs(server_config: Option<&ServerConfig>) -> Vec<SourcedOidcProviderConfig> {
    let env_configs = load_oidc_provider_configs_from_env();
    let persisted_configs = server_config
        .map(load_oidc_provider_configs_from_server_config)
        .unwrap_or_default();
    merge_oidc_provider_configs(env_configs, persisted_configs)
}

pub async fn validate_oidc_provider_config(config: &OidcProviderConfig) -> Result<OidcProviderValidationResult, String> {
    let http_client = ReqwestHttpClient::new()?;
    let state = OidcSys::discover_provider(config, &http_client).await?;

    Ok(OidcProviderValidationResult {
        issuer: state.metadata.issuer().to_string(),
        authorization_endpoint: state.metadata.authorization_endpoint().to_string(),
        token_endpoint: state.metadata.token_endpoint().map(ToString::to_string),
    })
}

// --- Helper functions ---

fn normalize_issuer(raw: &str) -> Option<(String, String, u16, String)> {
    let parsed = Url::parse(raw).ok()?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return None;
    }

    let host = parsed.host_str()?.to_ascii_lowercase();
    let port = parsed.port_or_known_default()?;
    let normalized_path = {
        let path = parsed.path().trim_end_matches('/').to_string();
        if path.is_empty() { "/".to_string() } else { path }
    };

    Some((parsed.scheme().to_string(), host, port, normalized_path))
}

fn normalize_config_url(config_url: &str) -> Result<String, String> {
    let config_url = config_url.trim();
    let url = Url::parse(config_url).map_err(|e| format!("invalid config_url: {e}"))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(format!("invalid config_url scheme: {}", url.scheme()));
    }
    let host = url.host_str().ok_or_else(|| "config_url missing host".to_string())?;
    let path = url.path();

    // Strip `/.well-known/openid-configuration` (with optional trailing slash) if present.
    // Everything else is preserved exactly so the issuer URL matches the provider's discovery
    // document (e.g. Authentik includes a trailing slash, Keycloak does not).
    let normalized_path = path
        .strip_suffix('/')
        .unwrap_or(path)
        .strip_suffix("/.well-known/openid-configuration")
        .unwrap_or(if path == "/" { "" } else { path });

    if normalized_path.contains("/.well-known/") {
        return Err("config_url uses an unsupported .well-known discovery URL".into());
    }

    let mut issuer = format!("{}://{host}", url.scheme());
    if let Some(port) = url.port() {
        issuer.push(':');
        issuer.push_str(&port.to_string());
    }

    if !normalized_path.is_empty() {
        issuer.push_str(normalized_path);
    }

    Ok(issuer)
}

fn issuer_candidates(base: &str) -> Vec<String> {
    let original = base.trim();
    let mut variants = Vec::with_capacity(2);
    variants.push(original.to_string());

    let toggled = if original.ends_with('/') {
        original.trim_end_matches('/').to_string()
    } else {
        format!("{original}/")
    };
    variants.push(toggled);

    variants
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_string_claim() {
        let mut claims = HashMap::new();
        claims.insert("email".to_string(), serde_json::json!("user@example.com"));
        claims.insert("sub".to_string(), serde_json::json!("12345"));

        assert_eq!(extract_string_claim(&claims, "email"), "user@example.com");
        assert_eq!(extract_string_claim(&claims, "sub"), "12345");
        assert_eq!(extract_string_claim(&claims, "missing"), "");
    }

    #[test]
    fn test_extract_groups_claim_array() {
        let mut claims = HashMap::new();
        claims.insert("groups".to_string(), serde_json::json!(["admin", "developers", "readonly"]));

        let groups = extract_groups_claim(&claims, "groups");
        assert_eq!(groups, vec!["admin", "developers", "readonly"]);
    }

    #[test]
    fn test_extract_groups_claim_string() {
        let mut claims = HashMap::new();
        claims.insert("groups".to_string(), serde_json::json!("admin,developers"));

        let groups = extract_groups_claim(&claims, "groups");
        assert_eq!(groups, vec!["admin", "developers"]);
    }

    #[test]
    fn test_extract_groups_claim_missing() {
        let claims = HashMap::new();
        let groups = extract_groups_claim(&claims, "groups");
        assert!(groups.is_empty());
    }

    #[test]
    fn test_extract_groups_claim_number() {
        let mut claims = HashMap::new();
        claims.insert("groups".to_string(), serde_json::json!(42));
        let groups = extract_groups_claim(&claims, "groups");
        assert!(groups.is_empty());
    }

    #[test]
    fn test_extract_canonical_group_values_merges_groups_and_roles() {
        let mut claims = HashMap::new();
        claims.insert("groups".to_string(), serde_json::json!(["devs", "admins"]));
        claims.insert("roles".to_string(), serde_json::json!(["admins", "consoleAdmin"]));

        let merged = extract_canonical_group_values(&claims, "groups", "roles");
        assert_eq!(merged, vec!["admins", "consoleAdmin", "devs"]);
    }

    #[test]
    fn test_extract_canonical_group_values_skips_duplicate_claim_name() {
        let mut claims = HashMap::new();
        claims.insert("roles".to_string(), serde_json::json!(["consoleAdmin"]));

        let merged = extract_canonical_group_values(&claims, "roles", "roles");
        assert_eq!(merged, vec!["consoleAdmin"]);
    }

    #[test]
    fn test_extract_canonical_group_values_roles_only() {
        let mut claims = HashMap::new();
        claims.insert("roles".to_string(), serde_json::json!(["consoleAdmin", "bucket-reader"]));

        let merged = extract_canonical_group_values(&claims, "groups", "roles");
        assert_eq!(merged, vec!["bucket-reader", "consoleAdmin"]);
    }

    #[test]
    fn test_extract_string_claim_case_insensitive() {
        let mut claims = HashMap::new();
        claims.insert("policyminio".to_string(), serde_json::json!("consoleAdmin"));

        assert_eq!(extract_string_claim(&claims, "policyMinio"), "consoleAdmin");
        assert_eq!(extract_string_claim(&claims, "POLICYMINIO"), "consoleAdmin");
        assert_eq!(extract_string_claim(&claims, "policyminio"), "consoleAdmin");
    }

    #[test]
    fn test_extract_groups_claim_case_insensitive() {
        let mut claims = HashMap::new();
        claims.insert("policyminio".to_string(), serde_json::json!(["consoleAdmin", "readwrite"]));

        let groups = extract_groups_claim(&claims, "policyMinio");
        assert_eq!(groups, vec!["consoleAdmin", "readwrite"]);

        let groups = extract_groups_claim(&claims, "POLICYMINIO");
        assert_eq!(groups, vec!["consoleAdmin", "readwrite"]);

        let groups = extract_groups_claim(&claims, "policyminio");
        assert_eq!(groups, vec!["consoleAdmin", "readwrite"]);
    }

    #[test]
    fn test_extract_groups_claim_exact_match_preferred() {
        let mut claims = HashMap::new();
        claims.insert("Policy".to_string(), serde_json::json!(["exact_match"]));
        claims.insert("policy".to_string(), serde_json::json!(["lowercase"]));

        let groups = extract_groups_claim(&claims, "Policy");
        assert_eq!(groups, vec!["exact_match"]);
    }

    #[test]
    fn test_extract_string_claim_ambiguous_case_insensitive_match_returns_empty() {
        let mut claims = HashMap::new();
        claims.insert("Policy".to_string(), serde_json::json!("exact_match"));
        claims.insert("policy".to_string(), serde_json::json!("lowercase"));

        assert_eq!(extract_string_claim(&claims, "POLICY"), "");
    }

    #[test]
    fn test_extract_groups_claim_ambiguous_case_insensitive_match_returns_empty() {
        let mut claims = HashMap::new();
        claims.insert("Policy".to_string(), serde_json::json!(["exact_match"]));
        claims.insert("policy".to_string(), serde_json::json!(["lowercase"]));

        let groups = extract_groups_claim(&claims, "POLICY");
        assert!(groups.is_empty());
    }

    #[test]
    fn test_decode_jwt_payload() {
        let payload = r#"{"sub":"user123","email":"user@example.com"}"#;
        let payload_b64 = base64_simd::URL_SAFE_NO_PAD.encode_to_string(payload.as_bytes());
        let token = format!("eyJhbGciOiJSUzI1NiJ9.{payload_b64}.signature");

        let claims = decode_jwt_payload(&token);
        assert_eq!(claims.get("sub").and_then(|v| v.as_str()), Some("user123"));
        assert_eq!(claims.get("email").and_then(|v| v.as_str()), Some("user@example.com"));
    }

    #[test]
    fn test_normalize_issuer_matches() {
        let lhs = normalize_issuer("https://idp.example.com/.well-known/openid-configuration/").unwrap();
        let rhs = normalize_issuer("https://idp.example.com/.well-known/openid-configuration").unwrap();
        assert_eq!(lhs, rhs);
        assert_eq!(
            lhs,
            (
                "https".to_string(),
                "idp.example.com".to_string(),
                443,
                "/.well-known/openid-configuration".to_string()
            )
        );
    }

    #[test]
    fn test_normalize_config_url() {
        // --- Well-known suffix stripping ---
        // Bare well-known URL → stripped to just the host
        assert_eq!(
            normalize_config_url("https://idp.example.com/.well-known/openid-configuration").unwrap(),
            "https://idp.example.com"
        );
        // Trailing slash after well-known suffix is also stripped
        assert_eq!(
            normalize_config_url("https://idp.example.com/.well-known/openid-configuration/").unwrap(),
            "https://idp.example.com"
        );
        // Well-known under a sub-path (Keycloak realms)
        assert_eq!(
            normalize_config_url("https://keycloak.example.com/realms/myrealm/.well-known/openid-configuration").unwrap(),
            "https://keycloak.example.com/realms/myrealm"
        );

        // --- Providers WITHOUT trailing slash (Keycloak, Auth0, Okta, Google) ---
        assert_eq!(
            normalize_config_url("https://keycloak.example.com/realms/myrealm").unwrap(),
            "https://keycloak.example.com/realms/myrealm"
        );
        assert_eq!(
            normalize_config_url("https://idp.example.com/custom/realm").unwrap(),
            "https://idp.example.com/custom/realm"
        );

        // --- Providers WITH trailing slash (Authentik) ---
        assert_eq!(
            normalize_config_url("https://auth.example.com/application/o/myapp/").unwrap(),
            "https://auth.example.com/application/o/myapp/"
        );

        // --- Root-level issuer (bare host) ---
        assert_eq!(normalize_config_url("https://idp.example.com").unwrap(), "https://idp.example.com");
        assert_eq!(normalize_config_url("https://idp.example.com/").unwrap(), "https://idp.example.com");

        // --- Custom port ---
        assert_eq!(
            normalize_config_url("https://idp.example.com:8443/auth/realms/test").unwrap(),
            "https://idp.example.com:8443/auth/realms/test"
        );
        assert_eq!(
            normalize_config_url("http://localhost:8080/application/o/app/").unwrap(),
            "http://localhost:8080/application/o/app/"
        );

        // --- Error cases ---
        assert!(normalize_config_url("https://idp.example.com/.well-known/invalid").is_err());
        assert!(normalize_config_url("gopher://idp.example.com").is_err());
        assert!(normalize_config_url("not-a-url").is_err());
    }

    #[test]
    fn test_issuer_candidates() {
        assert_eq!(
            issuer_candidates("https://idp.example.com/realm"),
            vec![
                "https://idp.example.com/realm".to_string(),
                "https://idp.example.com/realm/".to_string()
            ]
        );
        assert_eq!(
            issuer_candidates("https://idp.example.com/realm/"),
            vec![
                "https://idp.example.com/realm/".to_string(),
                "https://idp.example.com/realm".to_string()
            ]
        );
        assert_eq!(
            issuer_candidates("https://idp.example.com"),
            vec!["https://idp.example.com".to_string(), "https://idp.example.com/".to_string()]
        );
    }

    fn build_mocked_oidc_provider_config(id: &str, config_url: &str) -> OidcProviderConfig {
        OidcProviderConfig {
            id: id.to_string(),
            enabled: true,
            config_url: config_url.to_string(),
            client_id: "rustfs-oidc-test".to_string(),
            client_secret: None,
            scopes: vec!["openid".to_string()],
            other_audiences: vec![],
            redirect_uri: None,
            redirect_uri_dynamic: false,
            claim_name: "sub".to_string(),
            claim_prefix: "oidc".to_string(),
            role_policy: String::new(),
            display_name: "mock-oidc".to_string(),
            groups_claim: "groups".to_string(),
            roles_claim: String::new(),
            email_claim: "email".to_string(),
            username_claim: "username".to_string(),
        }
    }

    fn start_mock_oidc_discovery_server<F>(
        build_discovery_issuer: F,
        max_requests: usize,
    ) -> (String, std::thread::JoinHandle<()>)
    where
        F: Fn(&str) -> String + Send + 'static,
    {
        use std::io::Read;
        use std::io::Write;
        use std::net::{Shutdown, TcpListener};
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        // After the last completed response, exit if no new connection arrives within this window.
        const IDLE_SHUTDOWN: Duration = Duration::from_millis(500);
        const ABSOLUTE_CAP: Duration = Duration::from_secs(5);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let discovery_issuer = build_discovery_issuer(&base);
        let discovery_body = serde_json::json!({
            "issuer": discovery_issuer,
            "authorization_endpoint": format!("{base}/authorize"),
            "token_endpoint": format!("{base}/token"),
            "jwks_uri": format!("{base}/jwks"),
            "response_types_supported": ["code"],
            "response_modes_supported": ["query"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
        })
        .to_string();
        let jwks_body = r#"{"keys":[]}"#;
        let (ready_tx, ready_rx) = mpsc::channel();

        let handle = std::thread::spawn(move || {
            listener
                .set_nonblocking(true)
                .expect("failed to set discovery mock listener non-blocking");
            let _ = ready_tx.send(());

            let mut seen = 0usize;
            let start = Instant::now();
            let mut last_completed = Instant::now();

            loop {
                if seen > 0 && last_completed.elapsed() >= IDLE_SHUTDOWN {
                    break;
                }
                if start.elapsed() >= ABSOLUTE_CAP {
                    break;
                }

                let mut stream = match listener.accept() {
                    Ok((stream, _)) => stream,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(_) => break,
                };

                seen += 1;

                let mut request_bytes = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let n = stream.read(&mut buffer).unwrap_or_default();
                    if n == 0 {
                        break;
                    }
                    request_bytes.extend_from_slice(&buffer[..n]);
                    if request_bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    if request_bytes.len() >= 8192 {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&request_bytes);
                let path = request.lines().next().unwrap_or("").split_whitespace().nth(1).unwrap_or("");

                let (status, body) = if path.contains("/.well-known/openid-configuration") {
                    (200, discovery_body.as_str())
                } else if path.contains("/jwks") {
                    (200, jwks_body)
                } else {
                    (404, r#"{"error":"not found"}"#)
                };

                let response = format!(
                    "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    if status == 200 { "OK" } else { "Not Found" },
                    body.len()
                );

                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
                let _ = stream.shutdown(Shutdown::Both);
                last_completed = Instant::now();

                if seen >= max_requests {
                    break;
                }
            }
        });
        ready_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("mock OIDC discovery server should become ready");

        (base, handle)
    }

    fn discovery_error_contains_all_variants(err: &str, base: &str) -> bool {
        err.contains(base) && err.contains(&format!("{base}/")) && err.contains("discovery failed for all issuer variants")
    }

    #[tokio::test]
    async fn test_validate_oidc_provider_config_retries_with_issuer_candidates() {
        // Discovery document must advertise the canonical issuer path. The first candidate has no
        // trailing slash; openidconnect rejects issuer mismatch, then the second variant succeeds.
        let (base, handle) = start_mock_oidc_discovery_server(|base| format!("{base}/application/o/rustfs/"), 8);
        let config_url = format!("{base}/application/o/rustfs");
        let config = build_mocked_oidc_provider_config("default", &config_url);

        let result = validate_oidc_provider_config(&config).await;

        let validation_result = result.expect("OIDC provider validation should succeed");
        assert_eq!(validation_result.issuer, format!("{base}/application/o/rustfs/"));
        assert!(handle.join().is_ok());
    }

    #[tokio::test]
    async fn test_validate_oidc_provider_config_returns_detailed_errors() {
        let (base, handle) = start_mock_oidc_discovery_server(|base| format!("{base}/application/o/other"), 8);
        let config_url = format!("{base}/application/o/rustfs");
        let config = build_mocked_oidc_provider_config("default", &config_url);

        let err = validate_oidc_provider_config(&config)
            .await
            .expect_err("OIDC provider validation should fail");
        assert!(discovery_error_contains_all_variants(&err, &base));
        assert!(err.contains("issuer '"));
        assert!(err.contains(&format!("issuer '{base}/application/o/rustfs'")));
        assert!(err.contains(&format!("issuer '{base}/application/o/rustfs/'")));
        assert!(handle.join().is_ok());
    }

    #[test]
    fn test_decode_jwt_payload_invalid() {
        assert!(decode_jwt_payload("not-a-jwt").is_empty());
        assert!(decode_jwt_payload("").is_empty());
    }

    #[test]
    fn test_map_claims_to_policies_no_provider() {
        let sys = OidcSys::empty().expect("failed to initialize empty OIDC system");

        let claims = OidcClaims {
            sub: "user123".to_string(),
            email: "user@example.com".to_string(),
            username: "user".to_string(),
            groups: vec!["admin".to_string(), "devs".to_string()],
            raw: HashMap::new(),
        };

        let (policies, groups) = sys.map_claims_to_policies("nonexistent", &claims);
        assert!(policies.is_empty());
        assert!(groups.is_empty());
    }

    #[test]
    fn test_oidc_claims_default() {
        let claims = OidcClaims::default();
        assert!(claims.sub.is_empty());
        assert!(claims.email.is_empty());
        assert!(claims.username.is_empty());
        assert!(claims.groups.is_empty());
        assert!(claims.raw.is_empty());
    }

    #[test]
    fn test_oidc_claims_serde_roundtrip() {
        let claims = OidcClaims {
            sub: "user123".to_string(),
            email: "user@example.com".to_string(),
            username: "testuser".to_string(),
            groups: vec!["admin".to_string(), "devs".to_string()],
            raw: {
                let mut m = HashMap::new();
                m.insert("custom".to_string(), serde_json::json!("value"));
                m
            },
        };

        let json = serde_json::to_string(&claims).unwrap();
        let deserialized: OidcClaims = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sub, "user123");
        assert_eq!(deserialized.email, "user@example.com");
        assert_eq!(deserialized.groups.len(), 2);
    }

    #[test]
    fn test_oidc_provider_summary_serde() {
        let summary = OidcProviderSummary {
            provider_id: "okta".to_string(),
            display_name: "Okta SSO".to_string(),
        };

        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: OidcProviderSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.provider_id, "okta");
        assert_eq!(deserialized.display_name, "Okta SSO");
    }

    #[test]
    fn test_parse_single_provider_no_config_url() {
        let config = OidcSys::parse_single_provider("_TEST_EMPTY", "test_empty");
        assert!(config.is_none());
    }

    #[test]
    fn test_parse_persisted_provider_config() {
        let mut cfg = ServerConfig::new();
        let mut kvs = KVS(vec![
            rustfs_ecstore::config::KV {
                key: ENABLE_KEY.to_string(),
                value: EnableState::Off.to_string(),
                hidden_if_empty: false,
            },
            rustfs_ecstore::config::KV {
                key: OIDC_CONFIG_URL.to_string(),
                value: String::new(),
                hidden_if_empty: false,
            },
            rustfs_ecstore::config::KV {
                key: OIDC_CLIENT_ID.to_string(),
                value: String::new(),
                hidden_if_empty: false,
            },
        ]);
        kvs.insert(
            OIDC_CONFIG_URL.to_string(),
            "https://example.com/.well-known/openid-configuration".to_string(),
        );
        kvs.insert(OIDC_CLIENT_ID.to_string(), "console".to_string());
        kvs.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
        kvs.insert(OIDC_ROLES_CLAIM.to_string(), "app_roles".to_string());

        cfg.0
            .entry(IDENTITY_OPENID_SUB_SYS.to_string())
            .or_default()
            .insert(DEFAULT_DELIMITER.to_string(), kvs);

        let parsed = OidcSys::parse_persisted_configs(&cfg);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, "default");
        assert_eq!(parsed[0].client_id, "console");
        assert!(parsed[0].enabled);
        assert_eq!(parsed[0].roles_claim, "app_roles");
    }

    #[test]
    fn test_parse_persisted_provider_config_omitted_roles_claim_is_empty() {
        let mut cfg = ServerConfig::new();
        let mut kvs = KVS(vec![
            rustfs_ecstore::config::KV {
                key: ENABLE_KEY.to_string(),
                value: EnableState::Off.to_string(),
                hidden_if_empty: false,
            },
            rustfs_ecstore::config::KV {
                key: OIDC_CONFIG_URL.to_string(),
                value: String::new(),
                hidden_if_empty: false,
            },
            rustfs_ecstore::config::KV {
                key: OIDC_CLIENT_ID.to_string(),
                value: String::new(),
                hidden_if_empty: false,
            },
        ]);
        kvs.insert(
            OIDC_CONFIG_URL.to_string(),
            "https://example.com/.well-known/openid-configuration".to_string(),
        );
        kvs.insert(OIDC_CLIENT_ID.to_string(), "console".to_string());
        kvs.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());

        cfg.0
            .entry(IDENTITY_OPENID_SUB_SYS.to_string())
            .or_default()
            .insert(DEFAULT_DELIMITER.to_string(), kvs);

        let parsed = OidcSys::parse_persisted_configs(&cfg);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].roles_claim, "");
    }

    #[test]
    fn test_merge_oidc_provider_configs_prefers_env() {
        let mut persisted = test_config("default");
        persisted.display_name = "Persisted".to_string();

        let mut env = test_config("default");
        env.display_name = "Environment".to_string();

        let merged = merge_oidc_provider_configs(vec![env], vec![persisted]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].config.display_name, "Environment");
        assert_eq!(merged[0].source, OidcProviderConfigSource::Env);
    }

    #[test]
    fn test_oidc_sys_empty() {
        let sys = OidcSys::empty().expect("failed to initialize empty OIDC system");
        assert!(!sys.has_providers());
        assert!(sys.list_providers().is_empty());
    }

    #[test]
    fn test_should_bypass_proxy_for_oidc_uri_loopback_only() {
        assert!(should_bypass_proxy_for_oidc_uri("http://127.0.0.1:9000/.well-known/openid-configuration"));
        assert!(should_bypass_proxy_for_oidc_uri("http://localhost:9000/.well-known/openid-configuration"));
        assert!(should_bypass_proxy_for_oidc_uri("http://[::1]:9000/.well-known/openid-configuration"));
        assert!(!should_bypass_proxy_for_oidc_uri(
            "https://idp.example.com/.well-known/openid-configuration"
        ));
        assert!(!should_bypass_proxy_for_oidc_uri("not-a-url"));
    }

    /// Helper to create an OidcSys with configs only (no provider states needed).
    fn make_test_sys(configs: Vec<OidcProviderConfig>) -> OidcSys {
        let mut config_map = HashMap::new();
        for c in configs {
            config_map.insert(c.id.clone(), c);
        }
        OidcSys {
            configs: config_map,
            provider_states: RwLock::new(HashMap::new()),
            state_store: OidcStateStore::new(),
            http_client: ReqwestHttpClient::new().expect("failed to initialize OIDC HTTP clients"),
        }
    }

    fn test_config(id: &str) -> OidcProviderConfig {
        OidcProviderConfig {
            id: id.to_string(),
            enabled: true,
            config_url: format!("https://example.com/{id}/.well-known/openid-configuration"),
            client_id: "client-id".to_string(),
            client_secret: None,
            scopes: vec!["openid".to_string()],
            other_audiences: vec![],
            redirect_uri: None,
            redirect_uri_dynamic: true,
            claim_name: "groups".to_string(),
            claim_prefix: "".to_string(),
            role_policy: "".to_string(),
            display_name: id.to_string(),
            groups_claim: "groups".to_string(),
            roles_claim: String::new(),
            email_claim: "email".to_string(),
            username_claim: "preferred_username".to_string(),
        }
    }

    #[test]
    fn test_map_claims_to_policies_with_provider() {
        let mut config = test_config("okta");
        config.role_policy = "readwrite".to_string();
        config.display_name = "Okta".to_string();

        let sys = make_test_sys(vec![config]);

        let claims = OidcClaims {
            sub: "user123".to_string(),
            email: "user@example.com".to_string(),
            username: "user".to_string(),
            groups: vec!["admin".to_string(), "devs".to_string()],
            raw: HashMap::new(),
        };

        let (policies, groups) = sys.map_claims_to_policies("okta", &claims);
        assert_eq!(groups, vec!["admin", "devs"]);
        assert!(policies.contains(&"readwrite".to_string()));
        assert!(policies.contains(&"admin".to_string()));
        assert!(policies.contains(&"devs".to_string()));
    }

    #[test]
    fn test_map_claims_to_policies_with_prefix() {
        let mut config = test_config("azure");
        config.claim_prefix = "oidc-".to_string();
        config.display_name = "Azure AD".to_string();

        let sys = make_test_sys(vec![config]);

        let claims = OidcClaims {
            sub: "user456".to_string(),
            email: "user@corp.com".to_string(),
            username: "user".to_string(),
            groups: vec!["engineers".to_string()],
            raw: HashMap::new(),
        };

        let (policies, groups) = sys.map_claims_to_policies("azure", &claims);
        assert_eq!(groups, vec!["engineers"]);
        assert!(policies.contains(&"oidc-engineers".to_string()));
        assert_eq!(policies.len(), 1);
    }

    #[test]
    fn test_list_providers() {
        let mut config = test_config("keycloak");
        config.display_name = "Keycloak SSO".to_string();

        let sys = make_test_sys(vec![config]);

        assert!(sys.has_providers());
        let summaries = sys.list_providers();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].provider_id, "keycloak");
        assert_eq!(summaries[0].display_name, "Keycloak SSO");
    }

    #[test]
    fn test_get_provider_config() {
        let mut config = test_config("test");
        config.client_id = "my-client".to_string();
        config.client_secret = Some("secret".to_string());

        let sys = make_test_sys(vec![config]);

        assert!(sys.get_provider_config("test").is_some());
        assert_eq!(sys.get_provider_config("test").unwrap().client_id, "my-client");
        assert!(sys.get_provider_config("nonexistent").is_none());
    }

    #[test]
    fn test_oidc_provider_config_defaults() {
        let config = OidcProviderConfig {
            id: "test".to_string(),
            enabled: true,
            config_url: "https://example.com/.well-known/openid-configuration".to_string(),
            client_id: "my-client".to_string(),
            client_secret: Some("secret".to_string()),
            scopes: vec!["openid".to_string(), "profile".to_string(), "email".to_string()],
            other_audiences: vec![],
            redirect_uri: None,
            redirect_uri_dynamic: true,
            claim_name: "groups".to_string(),
            claim_prefix: "".to_string(),
            role_policy: "readwrite".to_string(),
            display_name: "Test Provider".to_string(),
            groups_claim: "groups".to_string(),
            roles_claim: String::new(),
            email_claim: "email".to_string(),
            username_claim: "preferred_username".to_string(),
        };

        assert_eq!(config.id, "test");
        assert!(config.enabled);
        assert_eq!(config.scopes.len(), 3);
        assert!(config.redirect_uri_dynamic);
    }
}
