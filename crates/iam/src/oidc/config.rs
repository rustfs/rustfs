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
use base64_simd::URL_SAFE_NO_PAD;
use rustfs_config::oidc::*;
use rustfs_config::server_config::{Config as ServerConfig, KVS};
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, EnableState};
use rustfs_utils::egress::{OutboundUrlError, validate_outbound_url};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use url::Url;

pub(super) const REDACTED_SECRET: &str = "***redacted***";

fn redacted_optional_secret(value: Option<&str>) -> &'static str {
    value.filter(|secret| !secret.is_empty()).map_or("", |_| REDACTED_SECRET)
}

/// Parsed configuration for a single OIDC provider.
#[derive(Clone, PartialEq, Eq)]
pub struct OidcProviderConfig {
    pub id: String,
    pub enabled: bool,
    pub config_url: String,
    pub issuer: Option<String>,
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
    pub hide_from_ui: bool,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OidcProviderConfigInput {
    pub enabled: bool,
    pub display_name: String,
    pub config_url: String,
    pub issuer: Option<String>,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
    pub other_audiences: Vec<String>,
    pub redirect_uri: Option<String>,
    pub redirect_uri_dynamic: bool,
    pub claim_name: String,
    pub claim_prefix: String,
    pub role_policy: String,
    pub groups_claim: String,
    pub roles_claim: String,
    pub email_claim: String,
    pub username_claim: String,
    pub hide_from_ui: bool,
}

impl Default for OidcProviderConfigInput {
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

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OidcProviderValidationInput {
    pub provider_id: String,
    pub enabled: bool,
    pub display_name: String,
    pub config_url: String,
    pub issuer: Option<String>,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
    pub other_audiences: Vec<String>,
    pub redirect_uri: Option<String>,
    pub redirect_uri_dynamic: bool,
    pub claim_name: String,
    pub claim_prefix: String,
    pub role_policy: String,
    pub groups_claim: String,
    pub roles_claim: String,
    pub email_claim: String,
    pub username_claim: String,
    pub hide_from_ui: bool,
}

impl Default for OidcProviderValidationInput {
    fn default() -> Self {
        let input = OidcProviderConfigInput::default();
        Self {
            provider_id: "default".to_string(),
            enabled: input.enabled,
            display_name: input.display_name,
            config_url: input.config_url,
            issuer: input.issuer,
            client_id: input.client_id,
            client_secret: input.client_secret,
            scopes: input.scopes,
            other_audiences: input.other_audiences,
            redirect_uri: input.redirect_uri,
            redirect_uri_dynamic: input.redirect_uri_dynamic,
            claim_name: input.claim_name,
            claim_prefix: input.claim_prefix,
            role_policy: input.role_policy,
            groups_claim: input.groups_claim,
            roles_claim: input.roles_claim,
            email_claim: input.email_claim,
            username_claim: input.username_claim,
            hide_from_ui: input.hide_from_ui,
        }
    }
}

impl From<OidcProviderValidationInput> for (String, OidcProviderConfigInput) {
    fn from(input: OidcProviderValidationInput) -> Self {
        let provider_id = if input.provider_id.trim().is_empty() {
            "default".to_string()
        } else {
            input.provider_id.trim().to_string()
        };
        let config = OidcProviderConfigInput {
            enabled: input.enabled,
            display_name: input.display_name,
            config_url: input.config_url,
            issuer: input.issuer,
            client_id: input.client_id,
            client_secret: input.client_secret,
            scopes: input.scopes,
            other_audiences: input.other_audiences,
            redirect_uri: input.redirect_uri,
            redirect_uri_dynamic: input.redirect_uri_dynamic,
            claim_name: input.claim_name,
            claim_prefix: input.claim_prefix,
            role_policy: input.role_policy,
            groups_claim: input.groups_claim,
            roles_claim: input.roles_claim,
            email_claim: input.email_claim,
            username_claim: input.username_claim,
            hide_from_ui: input.hide_from_ui,
        };
        (provider_id, config)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OidcConfigError {
    #[error("invalid provider_id")]
    InvalidProviderId,
    #[error("provider is managed by environment variables")]
    EnvironmentManaged,
    #[error("provider not found")]
    ProviderNotFound,
    #[error("config_url is required")]
    ConfigUrlRequired,
    #[error("client_id is required")]
    ClientIdRequired,
    #[error("redirect_uri is required when redirect_uri_dynamic is off")]
    RedirectUriRequired,
    #[error("scopes must include openid")]
    OpenidScopeRequired,
    #[error("{0} must be an absolute http/https URL")]
    InvalidAbsoluteUrl(&'static str),
    #[error("{field} is not allowed: {source}")]
    ForbiddenOutbound {
        field: &'static str,
        #[source]
        source: OutboundUrlError,
    },
}

impl fmt::Debug for OidcProviderConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OidcProviderConfig")
            .field("id", &self.id)
            .field("enabled", &self.enabled)
            .field("config_url", &self.config_url)
            .field("issuer", &self.issuer)
            .field("client_id", &self.client_id)
            .field("client_secret", &redacted_optional_secret(self.client_secret.as_deref()))
            .field("scopes", &self.scopes)
            .field("other_audiences", &self.other_audiences)
            .field("redirect_uri", &self.redirect_uri)
            .field("redirect_uri_dynamic", &self.redirect_uri_dynamic)
            .field("claim_name", &self.claim_name)
            .field("claim_prefix", &self.claim_prefix)
            .field("role_policy", &self.role_policy)
            .field("display_name", &self.display_name)
            .field("groups_claim", &self.groups_claim)
            .field("roles_claim", &self.roles_claim)
            .field("email_claim", &self.email_claim)
            .field("username_claim", &self.username_claim)
            .field("hide_from_ui", &self.hide_from_ui)
            .finish()
    }
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

/// Immutable OIDC configuration entries returned by configuration queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcConfigSnapshot {
    providers: Vec<SourcedOidcProviderConfig>,
}

impl OidcConfigSnapshot {
    pub fn new(mut providers: Vec<SourcedOidcProviderConfig>) -> Self {
        providers.sort_by(|left, right| {
            (left.config.id != "default")
                .cmp(&(right.config.id != "default"))
                .then_with(|| left.config.id.cmp(&right.config.id))
        });
        Self { providers }
    }

    pub fn providers(&self) -> &[SourcedOidcProviderConfig] {
        &self.providers
    }

    pub fn into_providers(self) -> Vec<SourcedOidcProviderConfig> {
        self.providers
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OidcProviderValidationResult {
    pub issuer: String,
    pub authorization_endpoint: Option<String>,
    pub token_endpoint: Option<String>,
}

/// Compatibility name for the provider summary returned by OIDC list APIs.
pub(crate) type OidcProviderSummary = crate::federation::FederatedProviderView;

/// OIDC fields required to compare identity providers across replicated sites.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcSiteReplicationProvider {
    pub provider_id: String,
    pub claim_name: String,
    pub role_policy: String,
    pub client_id: String,
    pub hashed_client_secret: String,
}

/// Active OIDC settings exposed to site replication without raw credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcSiteReplicationSnapshot {
    providers: Vec<OidcSiteReplicationProvider>,
}

impl OidcSiteReplicationSnapshot {
    pub fn from_config_snapshot(snapshot: &OidcConfigSnapshot) -> Self {
        let providers = snapshot
            .providers()
            .iter()
            .map(|provider| {
                let config = &provider.config;
                OidcSiteReplicationProvider {
                    provider_id: config.id.clone(),
                    claim_name: config.claim_name.clone(),
                    role_policy: config.role_policy.clone(),
                    client_id: config.client_id.clone(),
                    hashed_client_secret: hash_client_secret(config.client_secret.as_deref()),
                }
            })
            .collect();
        Self { providers }
    }

    pub fn providers(&self) -> &[OidcSiteReplicationProvider] {
        &self.providers
    }
}

fn hash_client_secret(secret: Option<&str>) -> String {
    let Some(secret) = secret.filter(|secret| !secret.is_empty()) else {
        return String::new();
    };

    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    URL_SAFE_NO_PAD.encode_to_string(hasher.finalize())
}

/// Read-only access to the active OIDC settings needed by site replication.
pub trait OidcConfigQuery: Send + Sync {
    fn site_replication_snapshot(&self) -> OidcSiteReplicationSnapshot;
}

pub fn validate_mutable_provider_id(provider_id: &str) -> Result<(), OidcConfigError> {
    if !is_valid_provider_id(provider_id) {
        return Err(OidcConfigError::InvalidProviderId);
    }
    if load_oidc_provider_configs_from_env()
        .iter()
        .any(|config| config.id == provider_id)
    {
        return Err(OidcConfigError::EnvironmentManaged);
    }
    Ok(())
}

fn is_valid_provider_id(id: &str) -> bool {
    !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn provider_instance_key(provider_id: &str) -> String {
    if provider_id == "default" {
        DEFAULT_DELIMITER.to_string()
    } else {
        provider_id.to_string()
    }
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.map(|value| value.trim().to_string()).filter(|value| !value.is_empty())
}

fn or_default(value: &str, default: &str) -> String {
    if value.trim().is_empty() {
        default.to_string()
    } else {
        value.trim().to_string()
    }
}

fn validate_absolute_http_url(value: &str, field: &'static str, check_outbound: bool) -> Result<(), OidcConfigError> {
    let parsed = Url::parse(value).map_err(|_| OidcConfigError::InvalidAbsoluteUrl(field))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(OidcConfigError::InvalidAbsoluteUrl(field));
    }
    if check_outbound {
        validate_outbound_url(&parsed).map_err(|source| OidcConfigError::ForbiddenOutbound { field, source })?;
    }
    Ok(())
}

fn normalize_provider_config(mut config: OidcProviderConfig) -> OidcProviderConfig {
    config.config_url = config.config_url.trim().to_string();
    config.issuer = normalize_optional(config.issuer);
    config.client_id = config.client_id.trim().to_string();
    config.scopes = config
        .scopes
        .iter()
        .map(|scope| scope.trim().to_string())
        .filter(|scope| !scope.is_empty())
        .collect();
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

fn validate_provider_config_fields(config: &OidcProviderConfig) -> Result<(), OidcConfigError> {
    if !is_valid_provider_id(&config.id) {
        return Err(OidcConfigError::InvalidProviderId);
    }
    if config.config_url.trim().is_empty() {
        return Err(OidcConfigError::ConfigUrlRequired);
    }
    validate_absolute_http_url(&config.config_url, "config_url", true)?;
    if let Some(issuer) = config.issuer.as_deref() {
        validate_absolute_http_url(issuer, "issuer", false)?;
    }
    if config.client_id.trim().is_empty() {
        return Err(OidcConfigError::ClientIdRequired);
    }
    if !config.redirect_uri_dynamic {
        let redirect_uri = config.redirect_uri.as_deref().ok_or(OidcConfigError::RedirectUriRequired)?;
        validate_absolute_http_url(redirect_uri, "redirect_uri", true)?;
    } else if let Some(redirect_uri) = config.redirect_uri.as_deref() {
        validate_absolute_http_url(redirect_uri, "redirect_uri", true)?;
    }
    if !config.scopes.iter().any(|scope| scope == "openid") {
        return Err(OidcConfigError::OpenidScopeRequired);
    }
    Ok(())
}

fn build_provider_config(
    provider_id: &str,
    input: OidcProviderConfigInput,
    existing_secret: Option<String>,
) -> Result<OidcProviderConfig, OidcConfigError> {
    let client_secret = match input.client_secret {
        Some(value) if !value.trim().is_empty() => Some(value),
        _ => existing_secret.filter(|value| !value.trim().is_empty()),
    };
    let config = normalize_provider_config(OidcProviderConfig {
        id: provider_id.to_string(),
        enabled: input.enabled,
        config_url: input.config_url,
        issuer: input.issuer,
        client_id: input.client_id,
        client_secret,
        scopes: input.scopes,
        other_audiences: input.other_audiences,
        redirect_uri: input.redirect_uri,
        redirect_uri_dynamic: input.redirect_uri_dynamic,
        claim_name: input.claim_name,
        claim_prefix: input.claim_prefix,
        role_policy: input.role_policy,
        display_name: input.display_name,
        groups_claim: input.groups_claim,
        roles_claim: input.roles_claim,
        email_claim: input.email_claim,
        username_claim: input.username_claim,
        hide_from_ui: input.hide_from_ui,
    });
    validate_provider_config_fields(&config)?;
    Ok(config)
}

pub fn build_upsert_provider_config(
    provider_id: &str,
    input: OidcProviderConfigInput,
    existing_secret: Option<String>,
) -> Result<OidcProviderConfig, OidcConfigError> {
    build_provider_config(provider_id, input, existing_secret)
}

pub fn build_validation_provider_config(input: OidcProviderValidationInput) -> Result<OidcProviderConfig, OidcConfigError> {
    let (provider_id, input) = input.into();
    build_provider_config(&provider_id, input, None)
}

pub fn persisted_provider_secret(config: &ServerConfig, provider_id: &str) -> Option<String> {
    config
        .0
        .get(IDENTITY_OPENID_SUB_SYS)
        .and_then(|subsystem| subsystem.get(&provider_instance_key(provider_id)))
        .and_then(|kvs| kvs.lookup(OIDC_CLIENT_SECRET))
        .filter(|value| !value.trim().is_empty())
}

fn set_kvs_value(kvs: &mut KVS, key: &str, value: String) {
    if let Some(existing) = kvs.0.iter_mut().find(|kv| kv.key == key) {
        existing.value = value;
        return;
    }
    kvs.insert(key.to_string(), value);
}

pub fn upsert_persisted_provider_config(config: &mut ServerConfig, provider: &OidcProviderConfig) {
    let mut kvs = ServerConfig::new()
        .get_value(IDENTITY_OPENID_SUB_SYS, DEFAULT_DELIMITER)
        .unwrap_or_default();
    set_kvs_value(
        &mut kvs,
        ENABLE_KEY,
        if provider.enabled {
            EnableState::On.to_string()
        } else {
            EnableState::Off.to_string()
        },
    );
    set_kvs_value(&mut kvs, OIDC_CONFIG_URL, provider.config_url.clone());
    set_kvs_value(&mut kvs, OIDC_ISSUER, provider.issuer.clone().unwrap_or_default());
    set_kvs_value(&mut kvs, OIDC_CLIENT_ID, provider.client_id.clone());
    set_kvs_value(&mut kvs, OIDC_CLIENT_SECRET, provider.client_secret.clone().unwrap_or_default());
    set_kvs_value(&mut kvs, OIDC_SCOPES, provider.scopes.join(","));
    set_kvs_value(&mut kvs, OIDC_OTHER_AUDIENCES, provider.other_audiences.join(","));
    set_kvs_value(&mut kvs, OIDC_REDIRECT_URI, provider.redirect_uri.clone().unwrap_or_default());
    set_kvs_value(
        &mut kvs,
        OIDC_REDIRECT_URI_DYNAMIC,
        if provider.redirect_uri_dynamic {
            EnableState::On.to_string()
        } else {
            EnableState::Off.to_string()
        },
    );
    set_kvs_value(&mut kvs, OIDC_CLAIM_NAME, provider.claim_name.clone());
    set_kvs_value(&mut kvs, OIDC_CLAIM_PREFIX, provider.claim_prefix.clone());
    set_kvs_value(&mut kvs, OIDC_ROLE_POLICY, provider.role_policy.clone());
    set_kvs_value(&mut kvs, OIDC_DISPLAY_NAME, provider.display_name.clone());
    set_kvs_value(&mut kvs, OIDC_GROUPS_CLAIM, provider.groups_claim.clone());
    set_kvs_value(&mut kvs, OIDC_ROLES_CLAIM, provider.roles_claim.clone());
    set_kvs_value(&mut kvs, OIDC_EMAIL_CLAIM, provider.email_claim.clone());
    set_kvs_value(&mut kvs, OIDC_USERNAME_CLAIM, provider.username_claim.clone());
    set_kvs_value(
        &mut kvs,
        OIDC_HIDE_FROM_UI,
        if provider.hide_from_ui {
            EnableState::On.to_string()
        } else {
            EnableState::Off.to_string()
        },
    );
    config
        .0
        .entry(IDENTITY_OPENID_SUB_SYS.to_string())
        .or_default()
        .insert(provider_instance_key(&provider.id), kvs);
}

pub fn delete_persisted_provider_config(config: &mut ServerConfig, provider_id: &str) -> Result<(), OidcConfigError> {
    let Some(subsystem) = config.0.get_mut(IDENTITY_OPENID_SUB_SYS) else {
        return Err(OidcConfigError::ProviderNotFound);
    };
    if subsystem.remove(&provider_instance_key(provider_id)).is_none() {
        return Err(OidcConfigError::ProviderNotFound);
    }
    if subsystem.is_empty() {
        config.0.remove(IDENTITY_OPENID_SUB_SYS);
    }
    Ok(())
}

/// Parse all OIDC provider configs from environment variables.
pub(super) fn parse_env_configs() -> Vec<OidcProviderConfig> {
    let mut configs = Vec::new();

    // Check for the default provider (no suffix)
    if let Some(config) = parse_single_provider("", "default") {
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
        if let Some(config) = parse_single_provider(&suffix, &id) {
            configs.push(config);
        }
    }

    configs
}

pub(super) fn parse_persisted_configs(cfg: &ServerConfig) -> Vec<OidcProviderConfig> {
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
        if let Some(config) = parse_single_persisted_provider(kvs, id) {
            configs.push(config);
        }
    }

    configs
}

/// Parse a string as an `EnableState` boolean.
/// Returns `default_if_empty` when the input is empty, and `default_on_error`
/// when parsing fails.
pub(super) fn parse_enable_state(value: &str, default_if_empty: bool, default_on_error: bool) -> bool {
    if value.is_empty() {
        return default_if_empty;
    }
    value
        .parse::<EnableState>()
        .map(|s| s.is_enabled())
        .unwrap_or(default_on_error)
}

/// Parse a single provider's config from env vars with the given suffix.
pub(super) fn parse_single_provider(env_suffix: &str, id: &str) -> Option<OidcProviderConfig> {
    let get_env = |base: &str| -> String { std::env::var(format!("{base}{env_suffix}")).unwrap_or_default() };

    let enable_val = get_env(ENV_IDENTITY_OPENID_ENABLE);
    let config_url = get_env(ENV_IDENTITY_OPENID_CONFIG_URL);
    let issuer = get_env(ENV_IDENTITY_OPENID_ISSUER);

    // Skip if no config URL
    if config_url.is_empty() {
        return None;
    }

    let enabled = parse_enable_state(&enable_val, true, false);

    let scopes_str = get_env(ENV_IDENTITY_OPENID_SCOPES);
    let scopes = if scopes_str.is_empty() {
        OIDC_DEFAULT_SCOPES.split(',').map(String::from).collect()
    } else {
        scopes_str.split(',').map(|s| s.trim().to_string()).collect()
    };

    let other_audiences_str = get_env(ENV_IDENTITY_OPENID_OTHER_AUDIENCES);
    let other_audiences = other_audiences_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let redirect_uri_dynamic = parse_enable_state(&get_env(ENV_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC), true, true);

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
    let hide_from_ui = parse_enable_state(&get_env(ENV_IDENTITY_OPENID_HIDE_FROM_UI), false, false);

    Some(OidcProviderConfig {
        id: id.to_string(),
        enabled,
        config_url,
        issuer: if issuer.is_empty() { None } else { Some(issuer) },
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
        hide_from_ui,
    })
}

pub(super) fn parse_single_persisted_provider(kvs: &KVS, id: &str) -> Option<OidcProviderConfig> {
    let config_url = kvs.get(OIDC_CONFIG_URL);
    if config_url.is_empty() {
        return None;
    }

    let enabled = parse_enable_state(&kvs.lookup(ENABLE_KEY).unwrap_or_default(), false, false);

    let scopes_str = kvs.get(OIDC_SCOPES);
    let scopes = if scopes_str.is_empty() {
        OIDC_DEFAULT_SCOPES.split(',').map(String::from).collect()
    } else {
        scopes_str.split(',').map(|s| s.trim().to_string()).collect()
    };

    let other_audiences_str = kvs.get(OIDC_OTHER_AUDIENCES);
    let other_audiences = other_audiences_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let redirect_uri_dynamic = parse_enable_state(&kvs.lookup(OIDC_REDIRECT_URI_DYNAMIC).unwrap_or_default(), true, true);

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
    let hide_from_ui = parse_enable_state(&kvs.lookup(OIDC_HIDE_FROM_UI).unwrap_or_default(), false, false);

    Some(OidcProviderConfig {
        id: id.to_string(),
        enabled,
        config_url,
        issuer: kvs.lookup(OIDC_ISSUER).filter(|v| !v.is_empty()),
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
        hide_from_ui,
    })
}

pub fn load_oidc_provider_configs_from_env() -> Vec<OidcProviderConfig> {
    parse_env_configs()
}

pub fn load_oidc_provider_configs_from_server_config(cfg: &ServerConfig) -> Vec<OidcProviderConfig> {
    parse_persisted_configs(cfg)
}

pub(super) fn merge_oidc_provider_configs(
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

pub(super) fn load_effective_oidc_provider_configs(server_config: Option<&ServerConfig>) -> Vec<SourcedOidcProviderConfig> {
    let env_configs = load_oidc_provider_configs_from_env();
    let persisted_configs = server_config
        .map(load_oidc_provider_configs_from_server_config)
        .unwrap_or_default();
    merge_oidc_provider_configs(env_configs, persisted_configs)
}

pub fn load_oidc_config_snapshot(server_config: Option<&ServerConfig>) -> OidcConfigSnapshot {
    OidcConfigSnapshot::new(load_effective_oidc_provider_configs(server_config))
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
