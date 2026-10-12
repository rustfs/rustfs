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
    config::{OidcProviderConfig, OidcProviderValidationResult, SourcedOidcProviderConfig},
    transport::{OidcHttpError, ReqwestHttpClient, oidc_extra_root_certs},
};
use crate::{EVENT_OIDC_DIAGNOSTICS, LOG_COMPONENT_IAM, LOG_SUBSYSTEM_OIDC};
use openidconnect::core::{CoreIdTokenVerifier, CoreJsonWebKeySet, CoreJwsSigningAlgorithm};
use openidconnect::{
    AsyncHttpClient, ClientId, ClientSecret, DiscoveryError, IssuerUrl, JsonWebKeySetUrl, ProviderMetadataWithLogout, TokenUrl,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration as StdDuration, Instant};
use tokio::time::sleep;
use tracing::warn;
use url::Url;

pub(super) const OIDC_JWKS_REFRESH_INTERVAL: StdDuration = StdDuration::from_secs(24 * 60 * 60);
pub(super) const OIDC_DISCOVERY_TRANSPORT_RETRIES: usize = 3;
pub(super) const OIDC_DISCOVERY_TRANSPORT_RETRY_DELAY: StdDuration = StdDuration::from_millis(50);
pub(super) const OIDC_JWKS_BLOCKED_BY_OUTBOUND_POLICY: &str = "JWKS request blocked by outbound policy";
pub(super) const OIDC_DISCOVERY_BLOCKED_BY_OUTBOUND_POLICY: &str = "OIDC provider discovery blocked by outbound policy";

// ---- Internal provider state ----

/// Discovered OIDC provider metadata.
/// We store metadata (which includes JWKS after discovery) separately rather than
/// a `CoreClient` because the crate uses type-state generics that make storing
/// the configured client in a HashMap impractical. The client is reconstructed
/// on-the-fly from metadata when needed.
#[derive(Clone)]
pub(super) struct ProviderState {
    pub(super) metadata: DiscoveredProviderMetadata,
    pub(super) discovered_at: Instant,
}

pub(super) struct ProviderRuntime {
    states: RwLock<HashMap<String, ProviderState>>,
    http_client: ReqwestHttpClient,
}

// Workload issuers do not implement the browser authorization flow. Keep their
// verification metadata separate rather than inventing an authorization URL.
#[serde_with::serde_as]
#[derive(Clone, Deserialize)]
pub(super) struct WorkloadProviderMetadata {
    issuer: IssuerUrl,
    jwks_uri: JsonWebKeySetUrl,
    token_endpoint: Option<TokenUrl>,
    #[serde_as(as = "serde_with::VecSkipError<_>")]
    id_token_signing_alg_values_supported: Vec<CoreJwsSigningAlgorithm>,
    #[serde(skip)]
    jwks: CoreJsonWebKeySet,
    // Discovery is extensible; report unsupported fields without logging values.
    #[serde(flatten)]
    additional_fields: HashMap<String, serde_json::Value>,
}

#[derive(Clone)]
pub(super) enum DiscoveredProviderMetadata {
    Console(Box<ProviderMetadataWithLogout>),
    Workload(Box<WorkloadProviderMetadata>),
}

impl DiscoveredProviderMetadata {
    pub(super) fn parse(body: &[u8], hide_from_ui: bool) -> Result<Self, String> {
        let document: serde_json::Value = serde_json::from_slice(body).map_err(|err| err.to_string())?;
        if hide_from_ui
            && document
                .as_object()
                .is_some_and(|fields| !fields.contains_key("authorization_endpoint"))
        {
            let mut metadata: WorkloadProviderMetadata = serde_json::from_slice(body).map_err(|err| err.to_string())?;
            if !metadata.additional_fields.is_empty() {
                warn!(
                    event = EVENT_OIDC_DIAGNOSTICS,
                    component = LOG_COMPONENT_IAM,
                    subsystem = LOG_SUBSYSTEM_OIDC,
                    result = "workload_discovery_additional_fields",
                    field_count = metadata.additional_fields.len(),
                    "workload discovery contains additional fields"
                );
                metadata.additional_fields.clear();
            }
            Ok(Self::Workload(Box::new(metadata)))
        } else {
            serde_json::from_slice(body)
                .map(|metadata| Self::Console(Box::new(metadata)))
                .map_err(|err| err.to_string())
        }
    }

    pub(super) fn console(&self) -> Result<&ProviderMetadataWithLogout, String> {
        match self {
            Self::Console(metadata) => Ok(metadata),
            Self::Workload(_) => Err("OIDC provider has no authorization endpoint; only web identity is supported".into()),
        }
    }

    pub(super) fn issuer(&self) -> &IssuerUrl {
        match self {
            Self::Console(metadata) => metadata.issuer(),
            Self::Workload(metadata) => &metadata.issuer,
        }
    }

    pub(super) fn jwks_uri(&self) -> &JsonWebKeySetUrl {
        match self {
            Self::Console(metadata) => metadata.jwks_uri(),
            Self::Workload(metadata) => &metadata.jwks_uri,
        }
    }

    pub(super) fn set_jwks(self, jwks: CoreJsonWebKeySet) -> Self {
        match self {
            Self::Console(metadata) => Self::Console(Box::new(metadata.set_jwks(jwks))),
            Self::Workload(mut metadata) => {
                metadata.jwks = jwks;
                Self::Workload(metadata)
            }
        }
    }

    pub(super) fn authorization_endpoint(&self) -> Option<String> {
        match self {
            Self::Console(metadata) => Some(metadata.authorization_endpoint().to_string()),
            Self::Workload(_) => None,
        }
    }

    pub(super) fn token_endpoint(&self) -> Option<&TokenUrl> {
        match self {
            Self::Console(metadata) => metadata.token_endpoint(),
            Self::Workload(metadata) => metadata.token_endpoint.as_ref(),
        }
    }

    pub(super) fn verifier(&self, config: &OidcProviderConfig) -> CoreIdTokenVerifier<'static> {
        let client_id = ClientId::new(config.client_id.clone());
        let secret = config.client_secret.as_ref().map(|secret| ClientSecret::new(secret.clone()));
        let (issuer, jwks, algorithms) = match self {
            Self::Console(metadata) => (metadata.issuer(), metadata.jwks(), metadata.id_token_signing_alg_values_supported()),
            Self::Workload(metadata) => (&metadata.issuer, &metadata.jwks, &metadata.id_token_signing_alg_values_supported),
        };
        let verifier = match secret {
            Some(secret) => CoreIdTokenVerifier::new_confidential_client(client_id, secret, issuer.clone(), jwks.clone()),
            None => CoreIdTokenVerifier::new_public_client(client_id, issuer.clone(), jwks.clone()),
        };
        verifier.set_allowed_algs(algorithms.clone())
    }
}

// This adapter is used only for discovery/JWKS fetches, never token exchange.
struct JwksAcceptClient<'a> {
    inner: &'a ReqwestHttpClient,
    discovery_url: Option<Url>,
}

impl<'c> AsyncHttpClient<'c> for JwksAcceptClient<'_> {
    type Error = OidcHttpError;
    type Future = <ReqwestHttpClient as AsyncHttpClient<'c>>::Future;

    fn call(&'c self, mut request: http::Request<Vec<u8>>) -> Self::Future {
        if !self.discovery_url.as_ref().is_some_and(|url| request.uri() == url.as_str()) {
            request.headers_mut().insert(
                http::header::ACCEPT,
                http::HeaderValue::from_static("application/json, application/jwk-set+json"),
            );
        }
        self.inner.call(request)
    }
}

impl ProviderState {
    fn is_stale(&self) -> bool {
        self.discovered_at.elapsed() >= OIDC_JWKS_REFRESH_INTERVAL
    }
}

impl ProviderRuntime {
    pub(super) fn new(http_client: ReqwestHttpClient, states: HashMap<String, ProviderState>) -> Self {
        Self {
            states: RwLock::new(states),
            http_client,
        }
    }

    pub(super) fn http_client(&self) -> &ReqwestHttpClient {
        &self.http_client
    }

    /// Find a provider whose discovered issuer matches the given JWT issuer string.
    pub(super) fn find_provider_by_issuer(
        &self,
        issuer: &str,
        configs: &HashMap<String, SourcedOidcProviderConfig>,
    ) -> Option<(String, OidcProviderConfig, ProviderState)> {
        let (issuer_scheme, issuer_host, issuer_port, issuer_path) = normalize_issuer(issuer)?;
        let map = self
            .states
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
                && let Some(config) = configs.get(id)
            {
                return Some((id.clone(), config.config.clone(), state.clone()));
            }
        }
        None
    }

    pub(super) fn get_provider_state(&self, provider_id: &str) -> Result<ProviderState, String> {
        self.states
            .read()
            .map_err(|e| format!("provider state lock poisoned: {e}"))?
            .get(provider_id)
            .cloned()
            .ok_or_else(|| format!("provider not discovered: {provider_id}"))
    }

    pub(super) async fn refresh_provider_state(
        &self,
        provider_id: &str,
        config: &OidcProviderConfig,
    ) -> Result<ProviderState, String> {
        let state = discover_provider(config, &self.http_client).await?;
        let mut map = self.states.write().map_err(|e| {
            let msg = e.to_string();
            format!("provider state lock poisoned: {msg}")
        })?;
        map.insert(provider_id.to_string(), state.clone());

        Ok(state)
    }

    pub(super) async fn ensure_provider_state(
        &self,
        provider_id: &str,
        config: &OidcProviderConfig,
    ) -> Result<ProviderState, String> {
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

    pub(super) async fn ensure_provider_state_if_stale(
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
}

/// Perform OIDC discovery for a provider.
/// `discover_async` fetches the discovery document and JWKS in one step.
pub(super) async fn discover_provider(
    config: &OidcProviderConfig,
    http_client: &ReqwestHttpClient,
) -> Result<ProviderState, String> {
    if let Some(issuer) = config.issuer.as_deref().filter(|issuer| !issuer.trim().is_empty()) {
        return discover_provider_from_config_url(config, issuer, http_client).await;
    }

    // The openidconnect crate expects the issuer URL (base), not the
    // .well-known/openid-configuration URL.
    let base_issuer = normalize_config_url(&config.config_url)?;
    let candidates = issuer_candidates(&base_issuer);
    let mut last_errors = Vec::new();

    for candidate_issuer in candidates.iter() {
        let issuer_url = IssuerUrl::new(candidate_issuer.clone()).map_err(|e| format!("invalid issuer URL: {e}"))?;

        for attempt in 0..OIDC_DISCOVERY_TRANSPORT_RETRIES {
            let discovered = if config.hide_from_ui {
                discover_provider_from_config_url(config, candidate_issuer, http_client).await
            } else {
                let client = JwksAcceptClient {
                    inner: http_client,
                    discovery_url: Some(
                        issuer_url
                            .join(".well-known/openid-configuration")
                            .map_err(|err| err.to_string())?,
                    ),
                };
                ProviderMetadataWithLogout::discover_async(issuer_url.clone(), &client)
                    .await
                    .map(|metadata| ProviderState {
                        metadata: DiscoveredProviderMetadata::Console(Box::new(metadata)),
                        discovered_at: Instant::now(),
                    })
                    .map_err(|err| match err {
                        DiscoveryError::Request(OidcHttpError::ForbiddenOutbound(reason)) => {
                            format!("{OIDC_DISCOVERY_BLOCKED_BY_OUTBOUND_POLICY}: {reason}")
                        }
                        err => format!("discovery failed: {err}"),
                    })
            };
            match discovered {
                Ok(state) => return Ok(state),
                Err(error)
                    if error.starts_with(OIDC_DISCOVERY_BLOCKED_BY_OUTBOUND_POLICY)
                        || error.starts_with(OIDC_JWKS_BLOCKED_BY_OUTBOUND_POLICY) =>
                {
                    return Err(error);
                }
                Err(error) => {
                    let is_transient_transport = error.contains("Request failed");
                    let should_retry = is_transient_transport && attempt + 1 < OIDC_DISCOVERY_TRANSPORT_RETRIES;
                    if should_retry {
                        warn!(
                            event = EVENT_OIDC_DIAGNOSTICS,
                            component = LOG_COMPONENT_IAM,
                            subsystem = LOG_SUBSYSTEM_OIDC,
                            result = "provider_discovery_transport_retry",
                            provider_id = %config.id,
                            config_url = %config.config_url,
                            issuer_candidate = %candidate_issuer,
                            attempt = attempt + 1,
                            max_attempts = OIDC_DISCOVERY_TRANSPORT_RETRIES,
                            error = %error,
                            "oidc provider discovery failed"
                        );
                        sleep(OIDC_DISCOVERY_TRANSPORT_RETRY_DELAY).await;
                        continue;
                    }

                    last_errors.push(format!("issuer '{candidate_issuer}': {error}"));
                    warn!(
                        event = EVENT_OIDC_DIAGNOSTICS,
                        component = LOG_COMPONENT_IAM,
                        subsystem = LOG_SUBSYSTEM_OIDC,
                        result = "provider_discovery_candidate_failed",
                        provider_id = %config.id,
                        config_url = %config.config_url,
                        issuer_candidate = %candidate_issuer,
                        attempt = attempt + 1,
                        max_attempts = OIDC_DISCOVERY_TRANSPORT_RETRIES,
                        error = %error,
                        "oidc provider discovery failed"
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

pub(super) async fn validate_oidc_provider_config(config: &OidcProviderConfig) -> Result<OidcProviderValidationResult, String> {
    let http_client = ReqwestHttpClient::new()?;
    validate_oidc_provider_config_with_http_client(config, &http_client).await
}

pub async fn validate_oidc_provider_config_with_extra_root_ca(
    config: &OidcProviderConfig,
    root_ca_pem: Option<&[u8]>,
) -> Result<OidcProviderValidationResult, String> {
    if root_ca_pem.is_none() {
        return validate_oidc_provider_config(config).await;
    }

    let http_client = ReqwestHttpClient::new_with_extra_root_certs(oidc_extra_root_certs(root_ca_pem)?)?;
    validate_oidc_provider_config_with_http_client(config, &http_client).await
}

async fn validate_oidc_provider_config_with_http_client(
    config: &OidcProviderConfig,
    http_client: &ReqwestHttpClient,
) -> Result<OidcProviderValidationResult, String> {
    let state = discover_provider(config, http_client).await?;

    Ok(OidcProviderValidationResult {
        issuer: state.metadata.issuer().to_string(),
        authorization_endpoint: state.metadata.authorization_endpoint(),
        token_endpoint: state.metadata.token_endpoint().map(ToString::to_string),
    })
}

async fn discover_provider_from_config_url(
    config: &OidcProviderConfig,
    issuer: &str,
    http_client: &ReqwestHttpClient,
) -> Result<ProviderState, String> {
    let issuer_url = IssuerUrl::new(issuer.trim().to_string()).map_err(|e| format!("invalid issuer URL: {e}"))?;
    let explicit_issuer = config.issuer.as_deref().is_some_and(|issuer| !issuer.trim().is_empty());
    let discovery_url = if explicit_issuer {
        discovery_url_from_config_url(&config.config_url)?
    } else {
        issuer_url
            .join(".well-known/openid-configuration")
            .map_err(|err| err.to_string())?
    };
    let request = http::Request::builder()
        .uri(discovery_url.to_string())
        .method(http::Method::GET)
        .header(http::header::ACCEPT, "application/json")
        .body(Vec::new())
        .map_err(|err| format!("failed to prepare discovery request: {err}"))?;

    let response = match http_client.call(request).await {
        Ok(response) => response,
        Err(OidcHttpError::ForbiddenOutbound(reason)) => {
            return Err(format!("{OIDC_DISCOVERY_BLOCKED_BY_OUTBOUND_POLICY}: {reason}"));
        }
        Err(err) => return Err(format!("discovery request failed: Request failed: {err}")),
    };
    if response.status() != http::StatusCode::OK {
        return Err(format!("discovery failed: HTTP status code {} at {}", response.status(), discovery_url));
    }

    if !explicit_issuer
        && let Some(content_type) = response.headers().get(http::header::CONTENT_TYPE)
        && !content_type.to_str().ok().is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|essence| essence.eq_ignore_ascii_case("application/json"))
        })
    {
        return Err("Unexpected response Content-Type: expected application/json".into());
    }

    let provider_metadata = DiscoveredProviderMetadata::parse(response.body(), config.hide_from_ui)
        .map_err(|err| format!("failed to parse discovery response: {err}"))?;
    if provider_metadata.issuer() != &issuer_url {
        return Err(format!(
            "unexpected issuer URI `{}` (expected `{}`)",
            provider_metadata.issuer().as_str(),
            issuer_url.as_str()
        ));
    }

    let jwks_url = if explicit_issuer {
        jwks_url_from_config_url(&config.config_url, &issuer_url, provider_metadata.jwks_uri())?
    } else {
        provider_metadata.jwks_uri().clone()
    };
    let jwks = match CoreJsonWebKeySet::fetch_async(
        &jwks_url,
        &JwksAcceptClient {
            inner: http_client,
            discovery_url: None,
        },
    )
    .await
    {
        Ok(jwks) => jwks,
        Err(DiscoveryError::Request(OidcHttpError::ForbiddenOutbound(reason))) => {
            return Err(format!("{OIDC_JWKS_BLOCKED_BY_OUTBOUND_POLICY}: {reason}"));
        }
        Err(err) => return Err(format!("failed to fetch JWKS: {err}")),
    };

    Ok(ProviderState {
        metadata: provider_metadata.set_jwks(jwks),
        discovered_at: Instant::now(),
    })
}

pub(super) fn normalize_issuer(raw: &str) -> Option<(String, String, u16, String)> {
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

pub(super) fn normalize_config_url(config_url: &str) -> Result<String, String> {
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

pub(super) fn discovery_url_from_config_url(config_url: &str) -> Result<Url, String> {
    let mut url = Url::parse(config_url.trim()).map_err(|e| format!("invalid config_url: {e}"))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(format!("invalid config_url scheme: {}", url.scheme()));
    }
    if url.host_str().is_none() {
        return Err("config_url missing host".to_string());
    }

    let path = url.path().to_string();
    let without_trailing_slash = path.strip_suffix('/').unwrap_or(&path);
    if without_trailing_slash.ends_with("/.well-known/openid-configuration") {
        url.set_path(without_trailing_slash);
        return Ok(url);
    }
    if without_trailing_slash.contains("/.well-known/") {
        return Err("config_url uses an unsupported .well-known discovery URL".into());
    }

    let discovery_path = if without_trailing_slash.is_empty() || without_trailing_slash == "/" {
        "/.well-known/openid-configuration".to_string()
    } else {
        format!("{without_trailing_slash}/.well-known/openid-configuration")
    };
    url.set_path(&discovery_path);
    Ok(url)
}

pub(super) fn jwks_url_from_config_url(
    config_url: &str,
    issuer_url: &IssuerUrl,
    jwks_url: &JsonWebKeySetUrl,
) -> Result<JsonWebKeySetUrl, String> {
    let issuer = issuer_url.url();
    let jwks = jwks_url.url();
    if issuer.origin() != jwks.origin() {
        return Ok(jwks_url.clone());
    }

    let issuer_path = issuer.path().trim_end_matches('/');
    let Some(suffix) = jwks.path().strip_prefix(issuer_path) else {
        return Ok(jwks_url.clone());
    };
    if !suffix.is_empty() && !suffix.starts_with('/') {
        return Ok(jwks_url.clone());
    }

    let mut internal_url =
        Url::parse(&normalize_config_url(config_url)?).map_err(|err| format!("invalid config_url issuer base: {err}"))?;
    let internal_path = internal_url.path().trim_end_matches('/');
    internal_url.set_path(&format!("{internal_path}{suffix}"));
    internal_url.set_query(jwks.query());
    Ok(JsonWebKeySetUrl::from_url(internal_url))
}

pub(super) fn issuer_candidates(base: &str) -> Vec<String> {
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

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;
