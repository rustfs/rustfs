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

use std::net::{IpAddr, SocketAddr};

use crate::config::env::*;
use crate::config::{AppConfig, CacheConfig, CloudConfig, MonitoringConfig, TrustedProxy, TrustedProxyConfig, ValidationMode};
use crate::error::ConfigError;
use rustfs_utils::*;

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
        let base_networks = parse_ip_list_from_env(ENV_TRUSTED_PROXIES, DEFAULT_TRUSTED_PROXIES)?;
        for network in base_networks {
            proxies.push(TrustedProxy::Cidr(network));
        }

        // Parse extra trusted proxies from environment.
        let extra_networks = parse_ip_list_from_env(ENV_EXTRA_TRUSTED_PROXIES, DEFAULT_EXTRA_TRUSTED_PROXIES)?;
        for network in extra_networks {
            proxies.push(TrustedProxy::Cidr(network));
        }

        // Parse individual trusted proxy IPs.
        let ip_strings = parse_string_list_from_env("TRUSTED_PROXY_IPS", "");
        for ip_str in ip_strings {
            if let Ok(ip) = ip_str.parse::<IpAddr>() {
                proxies.push(TrustedProxy::Single(ip));
            }
        }

        // Determine validation mode.
        let validation_mode_str = get_env_str(ENV_PROXY_VALIDATION_MODE, DEFAULT_PROXY_VALIDATION_MODE);
        let validation_mode = ValidationMode::from_str(&validation_mode_str)?;

        // Load other proxy settings.
        let enable_rfc7239 = get_env_bool(ENV_PROXY_ENABLE_RFC7239, DEFAULT_PROXY_ENABLE_RFC7239);
        let max_hops = get_env_usize(ENV_PROXY_MAX_HOPS, DEFAULT_PROXY_MAX_HOPS);
        let enable_chain_check = get_env_bool(ENV_PROXY_CHAIN_CONTINUITY_CHECK, DEFAULT_PROXY_CHAIN_CONTINUITY_CHECK);

        // Load private network ranges.
        let private_networks = parse_ip_list_from_env(ENV_PRIVATE_NETWORKS, DEFAULT_PRIVATE_NETWORKS)?;

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
            capacity: get_env_usize(ENV_CACHE_CAPACITY, DEFAULT_CACHE_CAPACITY),
            ttl_seconds: get_env_u64(ENV_CACHE_TTL_SECONDS, DEFAULT_CACHE_TTL_SECONDS),
            cleanup_interval_seconds: get_env_u64(ENV_CACHE_CLEANUP_INTERVAL, DEFAULT_CACHE_CLEANUP_INTERVAL),
        }
    }

    /// Loads monitoring configuration from environment variables.
    fn load_monitoring_config() -> MonitoringConfig {
        MonitoringConfig {
            metrics_enabled: get_env_bool(ENV_METRICS_ENABLED, DEFAULT_METRICS_ENABLED),
            log_level: get_env_str(ENV_LOG_LEVEL, DEFAULT_LOG_LEVEL),
            structured_logging: get_env_bool(ENV_STRUCTURED_LOGGING, DEFAULT_STRUCTURED_LOGGING),
            tracing_enabled: get_env_bool(ENV_TRACING_ENABLED, DEFAULT_TRACING_ENABLED),
            log_failed_validations: get_env_bool(ENV_PROXY_LOG_FAILED_VALIDATIONS, DEFAULT_PROXY_LOG_FAILED_VALIDATIONS),
        }
    }

    /// Loads cloud configuration from environment variables.
    fn load_cloud_config() -> CloudConfig {
        let forced_provider_str = get_env_str(ENV_CLOUD_PROVIDER_FORCE, DEFAULT_CLOUD_PROVIDER_FORCE);
        let forced_provider = if forced_provider_str.is_empty() {
            None
        } else {
            Some(forced_provider_str)
        };

        CloudConfig {
            metadata_enabled: get_env_bool(ENV_CLOUD_METADATA_ENABLED, DEFAULT_CLOUD_METADATA_ENABLED),
            metadata_timeout_seconds: get_env_u64(ENV_CLOUD_METADATA_TIMEOUT, DEFAULT_CLOUD_METADATA_TIMEOUT),
            cloudflare_ips_enabled: get_env_bool(ENV_CLOUDFLARE_IPS_ENABLED, DEFAULT_CLOUDFLARE_IPS_ENABLED),
            forced_provider,
        }
    }

    /// Loads the server binding address from environment variables.
    fn load_server_addr() -> SocketAddr {
        let host = get_env_str("SERVER_HOST", "0.0.0.0");
        let port = get_env_usize("SERVER_PORT", 3000) as u16;

        format!("{}:{}", host, port)
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 3000)))
    }

    /// Loads configuration from environment, falling back to defaults on failure.
    pub fn from_env_or_default() -> AppConfig {
        match Self::from_env() {
            Ok(config) => {
                tracing::info!("Configuration loaded successfully from environment variables");
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
                TrustedProxy::Single("127.0.0.1".parse().unwrap()),
                TrustedProxy::Single("::1".parse().unwrap()),
            ],
            ValidationMode::HopByHop,
            true,
            10,
            true,
            vec![
                "10.0.0.0/8".parse().unwrap(),
                "172.16.0.0/12".parse().unwrap(),
                "192.168.0.0/16".parse().unwrap(),
            ],
        );

        AppConfig::new(
            proxy_config,
            CacheConfig::default(),
            MonitoringConfig::default(),
            CloudConfig::default(),
            "0.0.0.0:3000".parse().unwrap(),
        )
    }

    /// Prints a summary of the configuration to the log.
    pub fn print_summary(config: &AppConfig) {
        tracing::info!("=== Application Configuration ===");
        tracing::info!("Server: {}", config.server_addr);
        tracing::info!("Trusted Proxies: {}", config.proxy.proxies.len());
        tracing::info!("Validation Mode: {:?}", config.proxy.validation_mode);
        tracing::info!("Cache Capacity: {}", config.cache.capacity);
        tracing::info!("Metrics Enabled: {}", config.monitoring.metrics_enabled);
        tracing::info!("Cloud Metadata: {}", config.cloud.metadata_enabled);

        if config.monitoring.log_failed_validations {
            tracing::info!("Failed validations will be logged");
        }

        if !config.proxy.proxies.is_empty() {
            tracing::debug!("Trusted networks: {:?}", config.proxy.get_network_strings());
        }
    }
}
