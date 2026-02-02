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

//! Environment variable configuration constants and helpers for the trusted proxy system.

use crate::ConfigError;
use ipnetwork::IpNetwork;
use rustfs_config::{
    ENV_TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK, ENV_TRUSTED_PROXY_CLOUD_METADATA_ENABLED, ENV_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT,
    ENV_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED, ENV_TRUSTED_PROXY_ENABLE_RFC7239, ENV_TRUSTED_PROXY_ENABLED,
    ENV_TRUSTED_PROXY_EXTRA_PROXIES, ENV_TRUSTED_PROXY_IPS, ENV_TRUSTED_PROXY_MAX_HOPS, ENV_TRUSTED_PROXY_PROXIES,
    ENV_TRUSTED_PROXY_VALIDATION_MODE,
};
use std::str::FromStr;
// ==================== Helper Functions ====================

/// Parses a comma-separated list of IP/CIDR strings from an environment variable.
pub fn parse_ip_list_from_env(key: &str, default: &str) -> Result<Vec<IpNetwork>, ConfigError> {
    let value = std::env::var(key).unwrap_or_else(|_| default.to_string());

    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut networks = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }

        match IpNetwork::from_str(item) {
            Ok(network) => networks.push(network),
            Err(e) => {
                tracing::warn!("Failed to parse network '{}' from environment variable {}: {}", item, key, e);
            }
        }
    }

    Ok(networks)
}

/// Parses a comma-separated list of strings from an environment variable.
pub fn parse_string_list_from_env(key: &str, default: &str) -> Vec<String> {
    let value = std::env::var(key).unwrap_or_else(|_| default.to_string());

    value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Checks if an environment variable is set.
pub fn is_env_set(key: &str) -> bool {
    std::env::var(key).is_ok()
}

/// Returns a list of all proxy-related environment variables and their current values.
#[allow(dead_code)]
pub fn get_all_proxy_env_vars() -> Vec<(String, String)> {
    let vars = [
        ENV_TRUSTED_PROXY_ENABLED,
        ENV_TRUSTED_PROXY_VALIDATION_MODE,
        ENV_TRUSTED_PROXY_ENABLE_RFC7239,
        ENV_TRUSTED_PROXY_MAX_HOPS,
        ENV_TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK,
        ENV_TRUSTED_PROXY_PROXIES,
        ENV_TRUSTED_PROXY_EXTRA_PROXIES,
        ENV_TRUSTED_PROXY_IPS,
        ENV_TRUSTED_PROXY_CLOUD_METADATA_ENABLED,
        ENV_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT,
        ENV_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED,
    ];

    vars.iter()
        .filter_map(|&key| std::env::var(key).ok().map(|value| (key.to_string(), value)))
        .collect()
}
