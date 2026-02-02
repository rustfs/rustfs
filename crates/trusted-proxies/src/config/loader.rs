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

//! Configuration loader for environment variables and files.

use crate::{
    AppConfig, CacheConfig, CloudConfig, ConfigError, MonitoringConfig, TrustedProxy, TrustedProxyConfig, ValidationMode,
    parse_ip_list_from_env, parse_string_list_from_env,
};
use ipnetwork::IpNetwork;
use rustfs_config::{
    DEFAULT_TRUSTED_PROXIES_LOG_LEVEL, DEFAULT_TRUSTED_PROXY_CACHE_CAPACITY, DEFAULT_TRUSTED_PROXY_CACHE_CLEANUP_INTERVAL,
    DEFAULT_TRUSTED_PROXY_CACHE_TTL_SECONDS, DEFAULT_TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK,
    DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_ENABLED, DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT,
    DEFAULT_TRUSTED_PROXY_CLOUD_PROVIDER_FORCE, DEFAULT_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED,
    DEFAULT_TRUSTED_PROXY_ENABLE_RFC7239, DEFAULT_TRUSTED_PROXY_EXTRA_PROXIES, DEFAULT_TRUSTED_PROXY_IPS,
    DEFAULT_TRUSTED_PROXY_LOG_FAILED_VALIDATIONS, DEFAULT_TRUSTED_PROXY_MAX_HOPS, DEFAULT_TRUSTED_PROXY_METRICS_ENABLED,
    DEFAULT_TRUSTED_PROXY_PRIVATE_NETWORKS, DEFAULT_TRUSTED_PROXY_PROXIES, DEFAULT_TRUSTED_PROXY_STRUCTURED_LOGGING,
    DEFAULT_TRUSTED_PROXY_TRACING_ENABLED, DEFAULT_TRUSTED_PROXY_VALIDATION_MODE, ENV_TRUSTED_PROXIES_LOG_LEVEL,
    ENV_TRUSTED_PROXY_CACHE_CAPACITY, ENV_TRUSTED_PROXY_CACHE_CLEANUP_INTERVAL, ENV_TRUSTED_PROXY_CACHE_TTL_SECONDS,
    ENV_TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK, ENV_TRUSTED_PROXY_CLOUD_METADATA_ENABLED, ENV_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT,
    ENV_TRUSTED_PROXY_CLOUD_PROVIDER_FORCE, ENV_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED, ENV_TRUSTED_PROXY_ENABLE_RFC7239,
    ENV_TRUSTED_PROXY_EXTRA_PROXIES, ENV_TRUSTED_PROXY_IPS, ENV_TRUSTED_PROXY_LOG_FAILED_VALIDATIONS, ENV_TRUSTED_PROXY_MAX_HOPS,
    ENV_TRUSTED_PROXY_METRICS_ENABLED, ENV_TRUSTED_PROXY_PRIVATE_NETWORKS, ENV_TRUSTED_PROXY_PROXIES,
    ENV_TRUSTED_PROXY_STRUCTURED_LOGGING, ENV_TRUSTED_PROXY_TRACING_ENABLED, ENV_TRUSTED_PROXY_VALIDATION_MODE,
};
use rustfs_utils::{get_env_bool, get_env_str, get_env_u64, get_env_usize, parse_and_resolve_address};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use tracing::info;

/// Loader for application configuration.
#[derive(Debug, Clone)]
pub struct ConfigLoader;

impl ConfigLoader {
    /// Loads the complete application configuration from environment variables.
    pub fn from_env() -> Result<AppConfig, ConfigError> {
        // Load proxy-specific configuration.
        let proxy_config = Self::load_proxy_config()?;

        // Load cache configuration.
        let cache_config = Self::load_cache_config();

        // Load monitoring and observability configuration.
        let monitoring_config = Self::load_monitoring_config();

        // Load cloud provider integration configuration.
        let cloud_config = Self::load_cloud_config();

        // Load server binding address.
        let server_addr = Self::load_server_addr();

        Ok(AppConfig::new(proxy_config, cache_config, monitoring_config, cloud_config, server_addr))
    }

    /// Loads trusted proxy configuration from environment variables.
    fn load_proxy_config() -> Result<TrustedProxyConfig, ConfigError> {
        let mut proxies = Vec::new();

        // Parse base trusted proxies from environment.
        let base_networks = parse_ip_list_from_env(ENV_TRUSTED_PROXY_PROXIES, DEFAULT_TRUSTED_PROXY_PROXIES)?;
        for network in base_networks {
            proxies.push(TrustedProxy::Cidr(network));
        }

        // Parse extra trusted proxies from environment.
        let extra_networks = parse_ip_list_from_env(ENV_TRUSTED_PROXY_EXTRA_PROXIES, DEFAULT_TRUSTED_PROXY_EXTRA_PROXIES)?;
        for network in extra_networks {
            proxies.push(TrustedProxy::Cidr(network));
        }

        // Parse individual trusted proxy IPs.
        let ip_strings = parse_string_list_from_env(ENV_TRUSTED_PROXY_IPS, DEFAULT_TRUSTED_PROXY_IPS);
        for ip_str in ip_strings {
            if let Ok(ip) = ip_str.parse::<IpAddr>() {
                proxies.push(TrustedProxy::Single(ip));
            }
        }

        // Determine validation mode.
        let validation_mode_str = get_env_str(ENV_TRUSTED_PROXY_VALIDATION_MODE, DEFAULT_TRUSTED_PROXY_VALIDATION_MODE);
        let validation_mode = ValidationMode::from_str(&validation_mode_str)?;

        // Load other proxy settings.
        let enable_rfc7239 = get_env_bool(ENV_TRUSTED_PROXY_ENABLE_RFC7239, DEFAULT_TRUSTED_PROXY_ENABLE_RFC7239);
        let max_hops = get_env_usize(ENV_TRUSTED_PROXY_MAX_HOPS, DEFAULT_TRUSTED_PROXY_MAX_HOPS);
        let enable_chain_check =
            get_env_bool(ENV_TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK, DEFAULT_TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK);

        // Load private network ranges.
        let private_networks =
            parse_ip_list_from_env(ENV_TRUSTED_PROXY_PRIVATE_NETWORKS, DEFAULT_TRUSTED_PROXY_PRIVATE_NETWORKS)?;

        Ok(TrustedProxyConfig::new(
            proxies,
            validation_mode,
            enable_rfc7239,
            max_hops,
            enable_chain_check,
            private_networks,
        ))
    }

    /// Loads cache configuration from environment variables.
    fn load_cache_config() -> CacheConfig {
        CacheConfig {
            capacity: get_env_usize(ENV_TRUSTED_PROXY_CACHE_CAPACITY, DEFAULT_TRUSTED_PROXY_CACHE_CAPACITY),
            ttl_seconds: get_env_u64(ENV_TRUSTED_PROXY_CACHE_TTL_SECONDS, DEFAULT_TRUSTED_PROXY_CACHE_TTL_SECONDS),
            cleanup_interval_seconds: get_env_u64(
                ENV_TRUSTED_PROXY_CACHE_CLEANUP_INTERVAL,
                DEFAULT_TRUSTED_PROXY_CACHE_CLEANUP_INTERVAL,
            ),
        }
    }

    /// Loads monitoring configuration from environment variables.
    fn load_monitoring_config() -> MonitoringConfig {
        MonitoringConfig {
            metrics_enabled: get_env_bool(ENV_TRUSTED_PROXY_METRICS_ENABLED, DEFAULT_TRUSTED_PROXY_METRICS_ENABLED),
            log_level: get_env_str(ENV_TRUSTED_PROXIES_LOG_LEVEL, DEFAULT_TRUSTED_PROXIES_LOG_LEVEL),
            structured_logging: get_env_bool(ENV_TRUSTED_PROXY_STRUCTURED_LOGGING, DEFAULT_TRUSTED_PROXY_STRUCTURED_LOGGING),
            tracing_enabled: get_env_bool(ENV_TRUSTED_PROXY_TRACING_ENABLED, DEFAULT_TRUSTED_PROXY_TRACING_ENABLED),
            log_failed_validations: get_env_bool(
                ENV_TRUSTED_PROXY_LOG_FAILED_VALIDATIONS,
                DEFAULT_TRUSTED_PROXY_LOG_FAILED_VALIDATIONS,
            ),
        }
    }

    /// Loads cloud configuration from environment variables.
    fn load_cloud_config() -> CloudConfig {
        let forced_provider_str = get_env_str(ENV_TRUSTED_PROXY_CLOUD_PROVIDER_FORCE, DEFAULT_TRUSTED_PROXY_CLOUD_PROVIDER_FORCE);
        let forced_provider = if forced_provider_str.is_empty() {
            None
        } else {
            Some(forced_provider_str)
        };

        CloudConfig {
            metadata_enabled: get_env_bool(
                ENV_TRUSTED_PROXY_CLOUD_METADATA_ENABLED,
                DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_ENABLED,
            ),
            metadata_timeout_seconds: get_env_u64(
                ENV_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT,
                DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT,
            ),
            cloudflare_ips_enabled: get_env_bool(
                ENV_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED,
                DEFAULT_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED,
            ),
            forced_provider,
        }
    }

    /// Loads the server binding address from environment variables.
    fn load_server_addr() -> SocketAddr {
        let address = get_env_str("RUSTFS_ADDRESS", rustfs_config::DEFAULT_ADDRESS);
        parse_and_resolve_address(&address)
            .unwrap_or_else(|_| SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), rustfs_config::DEFAULT_PORT))
    }

    /// Loads configuration from environment, falling back to defaults on failure.
    pub fn from_env_or_default() -> AppConfig {
        match Self::from_env() {
            Ok(config) => {
                info!("Configuration loaded successfully from environment variables");
                config
            }
            Err(e) => {
                tracing::warn!("Failed to load configuration from environment: {}. Using defaults", e);
                Self::default_config()
            }
        }
    }

    /// Returns a default configuration.
    pub fn default_config() -> AppConfig {
        let proxy_config = TrustedProxyConfig::new(
            vec![
                TrustedProxy::Single(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                TrustedProxy::Single(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            ],
            ValidationMode::HopByHop,
            true,
            10,
            true,
            DEFAULT_TRUSTED_PROXY_PRIVATE_NETWORKS
                .split(',')
                .filter_map(|s| s.trim().parse::<IpNetwork>().ok())
                .collect(),
        );

        AppConfig::new(
            proxy_config,
            CacheConfig::default(),
            MonitoringConfig::default(),
            CloudConfig::default(),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), rustfs_config::DEFAULT_PORT),
        )
    }

    /// Prints a summary of the configuration to the log.
    pub fn print_summary(config: &AppConfig) {
        info!("=== Application Configuration ===");
        info!("Server: {}", config.server_addr);
        info!("Trusted Proxies: {}", config.proxy.proxies.len());
        info!("Validation Mode: {:?}", config.proxy.validation_mode);
        info!("Cache Capacity: {}", config.cache.capacity);
        info!("Metrics Enabled: {}", config.monitoring.metrics_enabled);
        info!("Cloud Metadata: {}", config.cloud.metadata_enabled);

        if config.monitoring.log_failed_validations {
            info!("Failed validations will be logged");
        }

        if !config.proxy.proxies.is_empty() {
            tracing::debug!("Trusted networks: {:?}", config.proxy.get_network_strings());
        }
    }
}
