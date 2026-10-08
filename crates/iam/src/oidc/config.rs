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
use rustfs_config::oidc::*;
use rustfs_config::server_config::{Config as ServerConfig, KVS};
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, EnableState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

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

/// Read-only access to the active standard OIDC configuration.
///
/// This interface stays separate from authentication and generic provider
/// views because its snapshots include OIDC-specific fields and secrets.
pub trait OidcConfigQuery: Send + Sync {
    fn config_snapshot(&self) -> OidcConfigSnapshot;
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
