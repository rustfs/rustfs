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

use crate::{DEFAULT_TRUSTED_PROXIES, ENV_TRUSTED_PROXIES, TrustedProxiesConfig, parse_proxy_list};
use std::sync::LazyLock;
use tokio::sync::RwLock;
use tracing::error;

static TRUSTED_PROXIES_CONFIG: LazyLock<RwLock<TrustedProxiesConfig>> =
    LazyLock::new(|| RwLock::new(TrustedProxiesConfig::default()));

static TRUSTED_PROXIES_CONFIG_ENABLE: LazyLock<RwLock<bool>> = LazyLock::new(|| RwLock::new(false));

/// Check if the trusted proxies configuration is enabled
///
/// # Returns
/// A boolean indicating whether the trusted proxies configuration is enabled
pub async fn is_trusted_proxies_config_enabled() -> bool {
    let guard = TRUSTED_PROXIES_CONFIG_ENABLE.read().await;
    *guard
}

/// Enable or disable the trusted proxies configuration
///
/// # Arguments
/// * `enabled` - A boolean indicating whether to enable (true) or disable (false) the trusted proxies configuration
pub async fn set_trusted_proxies_config_enabled(enabled: bool) {
    let mut guard = TRUSTED_PROXIES_CONFIG_ENABLE.write().await;
    *guard = enabled;
}

/// Get the global trusted proxies configuration
///
/// # Returns
/// An Option containing the TrustedProxiesConfig if set, or None if not set
pub async fn get_trusted_proxies_config() -> TrustedProxiesConfig {
    let guard = TRUSTED_PROXIES_CONFIG.read().await;
    guard.clone()
}

/// Set the global trusted proxies configuration
///
/// # Arguments
/// * `config` - The TrustedProxiesConfig to set as the global configuration
pub async fn set_trusted_proxies_config(config: TrustedProxiesConfig) {
    let mut guard = TRUSTED_PROXIES_CONFIG.write().await;
    *guard = config;
}

/// Initialize the trusted proxies configuration from environment variables or config file path
/// If both are provided, environment variables take precedence over the config file
/// Returns an error if loading from both sources fails
///
/// # Returns
/// Ok(()) if the configuration is successfully loaded and set
///
/// # Errors
/// Returns an error if loading from both environment variables and config file fails
///
/// # Examples
/// ```no_run
/// use rustfs::proxy::config_loader::initialize;
///
/// #[tokio::main]
/// async fn main() {
///     if let Err(e) = initialize().await {
///         eprintln!("Failed to initialize trusted proxies configuration: {:?}", e);
///     }
/// }
/// ```
pub async fn initialize() -> anyhow::Result<()> {
    match from_env() {
        Ok(env_config) => {
            set_trusted_proxies_config(env_config).await;
            set_trusted_proxies_config_enabled(true).await;
            Ok(())
        }
        Err(e) => {
            error!(
                "Failed to load trusted proxies configuration from environment variables, falling back to config file if provided. error: {:?}",
                e
            );
            Err(e)
        }
    }
}

/// Load configurations from environment variables
pub fn from_env() -> anyhow::Result<TrustedProxiesConfig> {
    // Read environment variables, e.g.: TRUSTED_PROXIES="127.0.0.1,10.0.0.0/8,::1"
    let proxies_str = rustfs_utils::get_env_str(ENV_TRUSTED_PROXIES, DEFAULT_TRUSTED_PROXIES);

    let proxy_list = parse_proxy_list(&proxies_str);

    TrustedProxiesConfig::from_strs(&proxy_list)
}

/// Loading from the configuration file
pub fn from_config_file(path: &str) -> anyhow::Result<TrustedProxiesConfig> {
    use serde_json;
    use std::fs;

    let content = fs::read_to_string(path)?;
    let config: serde_json::Value = serde_json::from_str(&content)?;

    let proxies = config["rustfs_http_trusted_proxies"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("The trusted_proxies array is missing from the configuration file"))?
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<&str>>();

    TrustedProxiesConfig::from_strs(&proxies)
}
