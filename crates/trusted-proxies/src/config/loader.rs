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

//! Configuration loader for environment variables and files

use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use crate::config::env::*;
use crate::config::{AppConfig, CacheConfig, CloudConfig, MonitoringConfig, TrustedProxy, TrustedProxyConfig, ValidationMode};
use crate::error::ConfigError;

/// 配置加载器
#[derive(Debug, Clone)]
pub struct ConfigLoader;

impl ConfigLoader {
    /// 从环境变量加载完整应用配置
    pub fn from_env() -> Result<AppConfig, ConfigError> {
        // 加载可信代理配置
        let proxy_config = Self::load_proxy_config()?;

        // 加载缓存配置
        let cache_config = Self::load_cache_config();

        // 加载监控配置
        let monitoring_config = Self::load_monitoring_config();

        // 加载云服务配置
        let cloud_config = Self::load_cloud_config();

        // 服务器地址
        let server_addr = Self::load_server_addr();

        Ok(AppConfig::new(proxy_config, cache_config, monitoring_config, cloud_config, server_addr))
    }

    /// 加载可信代理配置
    fn load_proxy_config() -> Result<TrustedProxyConfig, ConfigError> {
        // 解析可信代理列表
        let mut proxies = Vec::new();

        // 基础可信代理
        let base_networks = parse_ip_list_from_env(ENV_TRUSTED_PROXIES, DEFAULT_TRUSTED_PROXIES)?;
        for network in base_networks {
            proxies.push(TrustedProxy::Cidr(network));
        }

        // 额外可信代理
        let extra_networks = parse_ip_list_from_env(ENV_EXTRA_TRUSTED_PROXIES, DEFAULT_EXTRA_TRUSTED_PROXIES)?;
        for network in extra_networks {
            proxies.push(TrustedProxy::Cidr(network));
        }

        // 单个 IP（从环境变量解析）
        let ip_strings = parse_string_list_from_env("TRUSTED_PROXY_IPS", "");
        for ip_str in ip_strings {
            if let Ok(ip) = ip_str.parse::<IpAddr>() {
                proxies.push(TrustedProxy::Single(ip));
            }
        }

        // 验证模式
        let validation_mode_str = get_string_from_env(ENV_PROXY_VALIDATION_MODE, DEFAULT_PROXY_VALIDATION_MODE);
        let validation_mode = ValidationMode::from_str(&validation_mode_str)?;

        // 其他配置
        let enable_rfc7239 = get_bool_from_env(ENV_PROXY_ENABLE_RFC7239, DEFAULT_PROXY_ENABLE_RFC7239);
        let max_hops = get_usize_from_env(ENV_PROXY_MAX_HOPS, DEFAULT_PROXY_MAX_HOPS);
        let enable_chain_check = get_bool_from_env(ENV_PROXY_CHAIN_CONTINUITY_CHECK, DEFAULT_PROXY_CHAIN_CONTINUITY_CHECK);

        // 私有网络
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

    /// 加载缓存配置
    fn load_cache_config() -> CacheConfig {
        CacheConfig {
            capacity: get_usize_from_env(ENV_CACHE_CAPACITY, DEFAULT_CACHE_CAPACITY),
            ttl_seconds: get_u64_from_env(ENV_CACHE_TTL_SECONDS, DEFAULT_CACHE_TTL_SECONDS),
            cleanup_interval_seconds: get_u64_from_env(ENV_CACHE_CLEANUP_INTERVAL, DEFAULT_CACHE_CLEANUP_INTERVAL),
        }
    }

    /// 加载监控配置
    fn load_monitoring_config() -> MonitoringConfig {
        MonitoringConfig {
            metrics_enabled: get_bool_from_env(ENV_METRICS_ENABLED, DEFAULT_METRICS_ENABLED),
            log_level: get_string_from_env(ENV_LOG_LEVEL, DEFAULT_LOG_LEVEL),
            structured_logging: get_bool_from_env(ENV_STRUCTURED_LOGGING, DEFAULT_STRUCTURED_LOGGING),
            tracing_enabled: get_bool_from_env(ENV_TRACING_ENABLED, DEFAULT_TRACING_ENABLED),
            log_failed_validations: get_bool_from_env(ENV_PROXY_LOG_FAILED_VALIDATIONS, DEFAULT_PROXY_LOG_FAILED_VALIDATIONS),
        }
    }

    /// 加载云服务配置
    fn load_cloud_config() -> CloudConfig {
        let forced_provider_str = get_string_from_env(ENV_CLOUD_PROVIDER_FORCE, DEFAULT_CLOUD_PROVIDER_FORCE);
        let forced_provider = if forced_provider_str.is_empty() {
            None
        } else {
            Some(forced_provider_str)
        };

        CloudConfig {
            metadata_enabled: get_bool_from_env(ENV_CLOUD_METADATA_ENABLED, DEFAULT_CLOUD_METADATA_ENABLED),
            metadata_timeout_seconds: get_u64_from_env(ENV_CLOUD_METADATA_TIMEOUT, DEFAULT_CLOUD_METADATA_TIMEOUT),
            cloudflare_ips_enabled: get_bool_from_env(ENV_CLOUDFLARE_IPS_ENABLED, DEFAULT_CLOUDFLARE_IPS_ENABLED),
            forced_provider,
        }
    }

    /// 加载服务器地址
    fn load_server_addr() -> SocketAddr {
        let host = get_string_from_env("SERVER_HOST", "0.0.0.0");
        let port = get_usize_from_env("SERVER_PORT", 3000) as u16;

        format!("{}:{}", host, port)
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 3000)))
    }

    /// 从环境变量加载配置，如果失败则使用默认值
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

    /// 创建默认配置
    pub fn default_config() -> AppConfig {
        // 默认可信代理配置
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

        // 默认应用配置
        AppConfig::new(
            proxy_config,
            CacheConfig::default(),
            MonitoringConfig::default(),
            CloudConfig::default(),
            "0.0.0.0:3000".parse().unwrap(),
        )
    }

    /// 打印配置摘要
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
