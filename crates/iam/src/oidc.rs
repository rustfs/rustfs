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
    AsyncHttpClient, AuthType, AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce, PkceCodeChallenge,
    PkceCodeVerifier, RedirectUrl, Scope,
};
use rustfs_config::oidc::*;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use tracing::{error, info};

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
pub(crate) struct ReqwestHttpClient(reqwest::Client);

impl<'c> AsyncHttpClient<'c> for ReqwestHttpClient {
    type Error = OidcHttpError;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<Vec<u8>>, Self::Error>> + Send + 'c>>;

    fn call(&'c self, request: http::Request<Vec<u8>>) -> Self::Future {
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let response = self
                .0
                .request(parts.method, parts.uri.to_string())
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
#[derive(Debug, Clone)]
pub struct OidcProviderConfig {
    pub id: String,
    pub enabled: bool,
    pub config_url: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
    pub redirect_uri: Option<String>,
    pub redirect_uri_dynamic: bool,
    pub claim_name: String,
    pub claim_prefix: String,
    pub role_policy: String,
    pub display_name: String,
    pub groups_claim: String,
    pub email_claim: String,
    pub username_claim: String,
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
struct ProviderState {
    metadata: CoreProviderMetadata,
}

// ---- Core OIDC system ----

/// Global OIDC manager for all configured providers.
pub struct OidcSys {
    configs: HashMap<String, OidcProviderConfig>,
    provider_states: HashMap<String, ProviderState>,
    state_store: OidcStateStore,
    http_client: ReqwestHttpClient,
}

impl OidcSys {
    /// Parse environment variables and discover all configured OIDC providers.
    pub async fn new() -> Result<Self, String> {
        let http_client = ReqwestHttpClient(reqwest::Client::new());
        let parsed_configs = Self::parse_env_configs();
        let mut configs = HashMap::new();
        let mut provider_states = HashMap::new();

        for config in parsed_configs {
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
            provider_states,
            state_store: OidcStateStore::new(),
            http_client,
        })
    }

    /// Create an OidcSys with no providers (useful for when OIDC is not configured).
    pub fn empty() -> Self {
        Self {
            configs: HashMap::new(),
            provider_states: HashMap::new(),
            state_store: OidcStateStore::new(),
            http_client: ReqwestHttpClient(reqwest::Client::new()),
        }
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
        let state = self
            .provider_states
            .get(provider_id)
            .ok_or_else(|| format!("provider not discovered: {provider_id}"))?;

        // Construct CoreClient on-the-fly (avoids type-state storage issues)
        let client = CoreClient::from_provider_metadata(
            state.metadata.clone(),
            ClientId::new(config.client_id.clone()),
            config.client_secret.as_ref().map(|s| ClientSecret::new(s.clone())),
        )
        .set_auth_type(AuthType::RequestBody);

        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        let redirect = RedirectUrl::new(redirect_uri.to_string()).map_err(|e| format!("invalid redirect URI: {e}"))?;

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
        let provider_state = self
            .provider_states
            .get(&session.provider_id)
            .ok_or_else(|| format!("provider not discovered: {}", session.provider_id))?;

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

        let verifier = client.id_token_verifier();
        let _verified_claims = id_token
            .claims(&verifier, &Nonce::new(session.nonce.clone()))
            .map_err(|e| format!("ID token verification failed: {e}"))?;

        // Extract raw claims from the verified JWT for custom claim support
        // (the crate verifies signature/expiry/nonce; we decode payload for non-standard claims)
        let raw_jwt = serde_json::to_value(id_token)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        let raw = decode_jwt_payload(&raw_jwt);

        let claims = OidcClaims {
            sub: extract_string_claim(&raw, "sub"),
            email: extract_string_claim(&raw, &config.email_claim),
            username: extract_string_claim(&raw, &config.username_claim),
            groups: extract_groups_claim(&raw, &config.groups_claim),
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
        let (provider_id, config, state) = self
            .find_provider_by_issuer(issuer)
            .ok_or_else(|| format!("no OIDC provider configured for issuer: {issuer}"))?;

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
        let verifier = client.id_token_verifier();
        let _verified = id_token
            .claims(&verifier, |_: Option<&Nonce>| Ok(()))
            .map_err(|e| format!("ID token verification failed: {e}"))?;

        // Extract claims using the provider's claim configuration
        let claims = OidcClaims {
            sub: extract_string_claim(&raw_claims, "sub"),
            email: extract_string_claim(&raw_claims, &config.email_claim),
            username: extract_string_claim(&raw_claims, &config.username_claim),
            groups: extract_groups_claim(&raw_claims, &config.groups_claim),
            raw: raw_claims,
        };

        Ok((claims, provider_id.to_string()))
    }

    /// Find a provider whose discovered issuer matches the given JWT issuer string.
    fn find_provider_by_issuer(&self, issuer: &str) -> Option<(&str, &OidcProviderConfig, &ProviderState)> {
        let issuer_normalized = issuer.trim_end_matches('/');
        for (id, state) in &self.provider_states {
            let provider_issuer = state.metadata.issuer().as_str();
            let provider_normalized = provider_issuer.trim_end_matches('/');
            if issuer_normalized == provider_normalized
                && let Some(config) = self.configs.get(id)
            {
                return Some((id, config, state));
            }
        }
        None
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
            redirect_uri,
            redirect_uri_dynamic,
            claim_name,
            claim_prefix: get_env(ENV_IDENTITY_OPENID_CLAIM_PREFIX),
            role_policy: get_env(ENV_IDENTITY_OPENID_ROLE_POLICY),
            display_name,
            groups_claim,
            email_claim,
            username_claim,
        })
    }

    /// Perform OIDC discovery for a provider.
    /// `discover_async` fetches the discovery document and JWKS in one step.
    async fn discover_provider(config: &OidcProviderConfig, http_client: &ReqwestHttpClient) -> Result<ProviderState, String> {
        // The openidconnect crate expects the issuer URL (base), not the
        // .well-known/openid-configuration URL. Strip the suffix if present.
        let issuer_str = config
            .config_url
            .strip_suffix("/.well-known/openid-configuration")
            .unwrap_or(&config.config_url);

        // Ensure trailing slash for correct URL joining in the crate
        let issuer_str = if issuer_str.ends_with('/') {
            issuer_str.to_string()
        } else {
            format!("{issuer_str}/")
        };

        let issuer_url = IssuerUrl::new(issuer_str).map_err(|e| format!("invalid issuer URL: {e}"))?;

        let metadata = CoreProviderMetadata::discover_async(issuer_url, http_client)
            .await
            .map_err(|e| format!("discovery failed: {e}"))?;

        Ok(ProviderState { metadata })
    }
}

// --- Helper functions ---

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

/// Extract a string claim from raw claims.
fn extract_string_claim(claims: &HashMap<String, serde_json::Value>, key: &str) -> String {
    claims.get(key).and_then(|v| v.as_str()).unwrap_or_default().to_string()
}

/// Extract a groups/array claim from raw claims. Handles both string arrays and single strings.
fn extract_groups_claim(claims: &HashMap<String, serde_json::Value>, key: &str) -> Vec<String> {
    match claims.get(key) {
        Some(serde_json::Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
        Some(serde_json::Value::String(s)) => s.split(',').map(|s| s.trim().to_string()).collect(),
        _ => vec![],
    }
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
    fn test_decode_jwt_payload() {
        let payload = r#"{"sub":"user123","email":"user@example.com"}"#;
        let payload_b64 = base64_simd::URL_SAFE_NO_PAD.encode_to_string(payload.as_bytes());
        let token = format!("eyJhbGciOiJSUzI1NiJ9.{payload_b64}.signature");

        let claims = decode_jwt_payload(&token);
        assert_eq!(claims.get("sub").and_then(|v| v.as_str()), Some("user123"));
        assert_eq!(claims.get("email").and_then(|v| v.as_str()), Some("user@example.com"));
    }

    #[test]
    fn test_decode_jwt_payload_invalid() {
        assert!(decode_jwt_payload("not-a-jwt").is_empty());
        assert!(decode_jwt_payload("").is_empty());
    }

    #[test]
    fn test_map_claims_to_policies_no_provider() {
        let sys = OidcSys::empty();

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
    fn test_oidc_sys_empty() {
        let sys = OidcSys::empty();
        assert!(!sys.has_providers());
        assert!(sys.list_providers().is_empty());
    }

    /// Helper to create an OidcSys with configs only (no provider states needed).
    fn make_test_sys(configs: Vec<OidcProviderConfig>) -> OidcSys {
        let mut config_map = HashMap::new();
        for c in configs {
            config_map.insert(c.id.clone(), c);
        }
        OidcSys {
            configs: config_map,
            provider_states: HashMap::new(),
            state_store: OidcStateStore::new(),
            http_client: ReqwestHttpClient(reqwest::Client::new()),
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
            redirect_uri: None,
            redirect_uri_dynamic: true,
            claim_name: "groups".to_string(),
            claim_prefix: "".to_string(),
            role_policy: "".to_string(),
            display_name: id.to_string(),
            groups_claim: "groups".to_string(),
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
            redirect_uri: None,
            redirect_uri_dynamic: true,
            claim_name: "groups".to_string(),
            claim_prefix: "".to_string(),
            role_policy: "readwrite".to_string(),
            display_name: "Test Provider".to_string(),
            groups_claim: "groups".to_string(),
            email_claim: "email".to_string(),
            username_claim: "preferred_username".to_string(),
        };

        assert_eq!(config.id, "test");
        assert!(config.enabled);
        assert_eq!(config.scopes.len(), 3);
        assert!(config.redirect_uri_dynamic);
    }
}
