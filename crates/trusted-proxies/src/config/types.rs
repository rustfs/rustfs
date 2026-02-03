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

//! Configuration type definitions for the trusted proxy system.

use crate::ConfigError;
use ipnetwork::IpNetwork;
use rustfs_config::{
    DEFAULT_TRUSTED_PROXIES_LOG_LEVEL, DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_ENABLED,
    DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT, DEFAULT_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

/// Proxy validation mode defining how the proxy chain is verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ValidationMode {
    /// Lenient mode: Accepts the entire chain as long as the last proxy is trusted.
    Lenient,
    /// Strict mode: Requires all proxies in the chain to be trusted.
    Strict,
    /// Hop-by-hop mode: Finds the first untrusted proxy from right to left.
    /// This is the recommended mode for most production environments.
    #[default]
    HopByHop,
}

impl FromStr for ValidationMode {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "lenient" => Ok(Self::Lenient),
            "strict" => Ok(Self::Strict),
            "hop_by_hop" | "hopbyhop" => Ok(Self::HopByHop),
            _ => Err(ConfigError::InvalidConfig(format!(
                "Invalid validation mode: '{}'. Must be one of: lenient, strict, hop_by_hop",
                s
            ))),
        }
    }
}

impl ValidationMode {
    /// Returns the string representation of the validation mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lenient => "lenient",
            Self::Strict => "strict",
            Self::HopByHop => "hop_by_hop",
        }
    }
}

/// Represents a trusted proxy entry, which can be a single IP or a CIDR range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustedProxy {
    /// A single IP address.
    Single(IpAddr),
    /// An IP network range (CIDR notation).
    Cidr(IpNetwork),
}

impl fmt::Display for TrustedProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single(ip) => write!(f, "{}", ip),
            Self::Cidr(network) => write!(f, "{}", network),
        }
    }
}

impl TrustedProxy {
    /// Checks if the given IP address matches this proxy configuration.
    pub fn contains(&self, ip: &IpAddr) -> bool {
        match self {
            Self::Single(proxy_ip) => ip == proxy_ip,
            Self::Cidr(network) => network.contains(*ip),
        }
    }
}

/// Configuration for trusted proxies and validation logic.
#[derive(Debug, Clone)]
pub struct TrustedProxyConfig {
    /// List of trusted proxy entries.
    pub proxies: Vec<TrustedProxy>,
    /// The validation mode to use for verifying proxy chains.
    pub validation_mode: ValidationMode,
    /// Whether to enable RFC 7239 "Forwarded" header support.
    pub enable_rfc7239: bool,
    /// Maximum allowed proxy hops in the chain.
    pub max_hops: usize,
    /// Whether to enable continuity checks for the proxy chain.
    pub enable_chain_continuity_check: bool,
    /// Private network ranges that should be treated with caution.
    pub private_networks: Vec<IpNetwork>,
}

impl TrustedProxyConfig {
    /// Creates a new trusted proxy configuration.
    pub fn new(
        proxies: Vec<TrustedProxy>,
        validation_mode: ValidationMode,
        enable_rfc7239: bool,
        max_hops: usize,
        enable_chain_continuity_check: bool,
        private_networks: Vec<IpNetwork>,
    ) -> Self {
        Self {
            proxies,
            validation_mode,
            enable_rfc7239,
            max_hops,
            enable_chain_continuity_check,
            private_networks,
        }
    }

    /// Checks if a SocketAddr originates from a trusted proxy.
    pub fn is_trusted(&self, addr: &SocketAddr) -> bool {
        let ip = addr.ip();
        self.proxies.iter().any(|proxy| proxy.contains(&ip))
    }

    /// Checks if an IP address belongs to a private network range.
    pub fn is_private_network(&self, ip: &IpAddr) -> bool {
        self.private_networks.iter().any(|network| network.contains(*ip))
    }

    /// Returns a list of all network strings for debugging purposes.
    pub fn get_network_strings(&self) -> Vec<String> {
        self.proxies.iter().map(|p| p.to_string()).collect()
    }

    /// Returns a summary of the configuration.
    pub fn summary(&self) -> String {
        format!(
            "TrustedProxyConfig {{ proxies: {}, mode: {}, max_hops: {} }}",
            self.proxies.len(),
            self.validation_mode.as_str(),
            self.max_hops
        )
    }
}

/// Configuration for the internal caching mechanism.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum number of entries in the cache.
    pub capacity: usize,
    /// Time-to-live for cache entries in seconds.
    pub ttl_seconds: u64,
    /// Interval for cache cleanup in seconds.
    pub cleanup_interval_seconds: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            ttl_seconds: 300,
            cleanup_interval_seconds: 60,
        }
    }
}

impl CacheConfig {
    /// Returns the TTL as a Duration.
    pub fn ttl_duration(&self) -> Duration {
        Duration::from_secs(self.ttl_seconds)
    }

    /// Returns the cleanup interval as a Duration.
    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup_interval_seconds)
    }
}

/// Configuration for monitoring and observability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Whether to enable Prometheus metrics.
    pub metrics_enabled: bool,
    /// The logging level (e.g., "info", "debug").
    pub log_level: String,
    /// Whether to use structured JSON logging.
    pub structured_logging: bool,
    /// Whether to enable distributed tracing.
    pub tracing_enabled: bool,
    /// Whether to log detailed information about failed validations.
    pub log_failed_validations: bool,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            log_level: DEFAULT_TRUSTED_PROXIES_LOG_LEVEL.to_string(),
            structured_logging: false,
            tracing_enabled: true,
            log_failed_validations: true,
        }
    }
}

/// Configuration for cloud provider integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudConfig {
    /// Whether to enable automatic cloud metadata discovery.
    pub metadata_enabled: bool,
    /// Timeout for cloud metadata requests in seconds.
    pub metadata_timeout_seconds: u64,
    /// Whether to automatically include Cloudflare IP ranges.
    pub cloudflare_ips_enabled: bool,
    /// Optionally force a specific cloud provider.
    pub forced_provider: Option<String>,
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            metadata_enabled: DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_ENABLED,
            metadata_timeout_seconds: DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT,
            cloudflare_ips_enabled: DEFAULT_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED,
            forced_provider: None,
        }
    }
}

impl CloudConfig {
    /// Returns the metadata timeout as a Duration.
    pub fn metadata_timeout(&self) -> Duration {
        Duration::from_secs(self.metadata_timeout_seconds)
    }
}

/// Complete application configuration.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// Trusted proxy settings.
    pub proxy: TrustedProxyConfig,
    /// Cache settings.
    pub cache: CacheConfig,
    /// Monitoring and observability settings.
    pub monitoring: MonitoringConfig,
    /// Cloud integration settings.
    pub cloud: CloudConfig,
    /// The address the server should bind to.
    pub server_addr: SocketAddr,
}

impl AppConfig {
    /// Creates a new application configuration.
    pub fn new(
        proxy: TrustedProxyConfig,
        cache: CacheConfig,
        monitoring: MonitoringConfig,
        cloud: CloudConfig,
        server_addr: SocketAddr,
    ) -> Self {
        Self {
            proxy,
            cache,
            monitoring,
            cloud,
            server_addr,
        }
    }

    /// Returns a summary of the application configuration.
    pub fn summary(&self) -> String {
        format!(
            "AppConfig {{\n\
            \x20\x20proxy: {},\n\
            \x20\x20cache_capacity: {},\n\
            \x20\x20metrics: {},\n\
            \x20\x20cloud_metadata: {}\n\
            }}",
            self.proxy.summary(),
            self.cache.capacity,
            self.monitoring.metrics_enabled,
            self.cloud.metadata_enabled
        )
    }
}
