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

use crate::{
    CloudProviderIps, ConfigError, DEFAULT_ADDITIONAL_TRUSTED_PROXIES, DEFAULT_CLOUDFLARE_IPS_ENABLED, DEFAULT_TRUSTED_PROXIES,
    ENV_ADDITIONAL_TRUSTED_PROXIES, ENV_CLOUDFLARE_IPS_ENABLED, ENV_TRUSTED_PROXIES, EnvConfigLoader, ValidationMode,
    parse_proxy_list,
};
use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use tracing::debug;

/// Trusted Proxy Configuration: Supports a single IP or CIDR block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrustedProxy {
    /// A single IP address
    Single(IpAddr),
    /// IP address segment (CIDR notation)
    Cidr(IpNetwork),
}

impl TrustedProxy {
    /// Parsing trusted agent configurations from strings
    /// Support: "127.0.0.1", "10.0.0.0/8", "::1"
    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        if s.contains('/') {
            // CIDR notation
            let network = IpNetwork::from_str(s).map_err(|e| anyhow::anyhow!("Invalid CIDR format '{}': {}", s, e))?;
            Ok(TrustedProxy::Cidr(network))
        } else {
            // Single IP
            let ip = IpAddr::from_str(s).map_err(|e| anyhow::anyhow!("Invalid IP address '{}': {}", s, e))?;
            Ok(TrustedProxy::Single(ip))
        }
    }

    /// Check if the IP matches this configuration
    pub fn contains(&self, ip: &IpAddr) -> bool {
        match self {
            TrustedProxy::Single(proxy_ip) => ip == proxy_ip,
            TrustedProxy::Cidr(network) => network.contains(*ip),
        }
    }
}

/// Trusted Agent Configuration Collection
///
/// Supports multiple trusted agents (single IP or CIDR blocks)
/// Used to verify if a request comes from a trusted agent based on its SocketAddr
/// Refer to `TrustedProxy` for individual configurations
///
#[derive(Debug, Clone)]
pub struct TrustedProxiesConfig {
    pub proxies: Vec<TrustedProxy>,
}

impl Default for TrustedProxiesConfig {
    fn default() -> Self {
        // Default trusted agents: Local loopback and common private networks
        let default_proxies = parse_proxy_list(DEFAULT_TRUSTED_PROXIES);
        Self::from_strs(&default_proxies).unwrap_or_else(|_| Self { proxies: vec![] })
    }
}

impl TrustedProxiesConfig {
    /// Create a new configuration
    ///
    /// # Arguments
    /// * `proxies` - A vector of TrustedProxy configurations
    ///
    /// # Returns
    /// A new TrustedProxiesConfig instance
    pub fn new(proxies: Vec<TrustedProxy>) -> Self {
        Self { proxies }
    }

    /// Create a configuration from a string slice
    ///
    /// # Arguments
    /// * `proxy_strs` - A slice of string slices representing trusted agents
    ///
    /// # Returns
    /// A Result containing the TrustedProxiesConfig or an error if parsing fails
    pub fn from_strs(proxy_strs: &[&str]) -> anyhow::Result<Self> {
        let mut proxies = Vec::new();

        for s in proxy_strs {
            proxies.push(TrustedProxy::from_str(s)?);
        }

        Ok(Self::new(proxies))
    }

    /// Check if the SocketAddr is coming from a trusted agent
    ///
    /// # Arguments
    /// * `addr` - The SocketAddr to check
    ///
    /// # Returns
    /// true if the address is from a trusted agent, false otherwise
    pub fn is_trusted(&self, addr: &SocketAddr) -> bool {
        let ip = addr.ip();
        self.proxies.iter().any(|proxy| proxy.contains(&ip))
    }

    /// Get a list of trusted agents (for debugging/logging)
    ///
    /// # Returns
    /// A vector of strings representing the trusted agents
    pub fn get_trusted_ranges(&self) -> Vec<String> {
        self.proxies
            .iter()
            .map(|p| match p {
                TrustedProxy::Single(ip) => ip.to_string(),
                TrustedProxy::Cidr(network) => network.to_string(),
            })
            .collect()
    }

    /// Create a configuration from an environment variable
    pub fn from_env() -> Result<Self, ConfigError> {
        // Load the underlying trusted agent
        let mut proxies = EnvConfigLoader::parse_ip_list(ENV_TRUSTED_PROXIES, DEFAULT_TRUSTED_PROXIES)?;

        // Load additional trusted agents
        let additional_proxies =
            EnvConfigLoader::parse_ip_list(ENV_ADDITIONAL_TRUSTED_PROXIES, DEFAULT_ADDITIONAL_TRUSTED_PROXIES)?;
        proxies.extend(additional_proxies);

        // Get IP ranges from cloud metadata
        let cloud_ips = EnvConfigLoader::fetch_cloud_metadata_ips()?;
        for ip_range in cloud_ips {
            if let Ok(network) = ip_range.parse::<IpNetwork>() {
                proxies.push(TrustedProxy::Cidr(network));
                debug!("Add cloud metadata IP range: {}", network);
            }
        }

        // If Cloudflare IP is enabled, add
        if EnvConfigLoader::get_bool(ENV_CLOUDFLARE_IPS_ENABLED, DEFAULT_CLOUDFLARE_IPS_ENABLED)? {
            for network in CloudProviderIps::cloudflare_ranges() {
                proxies.push(TrustedProxy::Cidr(network));
            }
        }

        Ok(Self { proxies })
    }

    /// Get the trusted agent scope configured in the environment variable (for debugging/logging)
    pub fn get_trusted_ranges_from_env() -> Result<Vec<String>, ConfigError> {
        let config = Self::from_env()?;
        Ok(config.get_trusted_ranges())
    }
}

/// Client real information stored in the request extension
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// Real client IP address (verified)
    pub real_ip: IpAddr,
    /// Original request hostname (if from a trusted agent)
    pub forwarded_host: Option<String>,
    /// Original request agreement (if from a trusted agent)
    pub forwarded_proto: Option<String>,
    /// Whether the request comes from a trusted agent
    pub is_from_trusted_proxy: bool,
    /// Direct Connected Proxy IP (if Proxyed)
    pub proxy_ip: Option<IpAddr>,
}

impl ClientInfo {
    /// Create client information for direct connections (no agents)
    ///
    /// # Arguments
    /// * `addr` - The SocketAddr of the direct client connection
    ///
    /// # Returns
    /// A new ClientInfo instance representing a direct connection
    pub fn direct(addr: SocketAddr) -> Self {
        Self {
            real_ip: addr.ip(),
            forwarded_host: None,
            forwarded_proto: None,
            is_from_trusted_proxy: false,
            proxy_ip: None,
        }
    }

    /// Create client information from trusted agents
    ///
    /// # Arguments
    /// * `real_ip` - The real client IP address
    /// * `forwarded_host` - The original request hostname
    /// * `forwarded_proto` - The original request agreement
    /// * `proxy_ip` - The direct connected proxy IP
    ///
    /// # Returns
    /// A new ClientInfo instance representing a proxied connection
    pub fn from_trusted_proxy(
        real_ip: IpAddr,
        forwarded_host: Option<String>,
        forwarded_proto: Option<String>,
        proxy_ip: IpAddr,
    ) -> Self {
        Self {
            real_ip,
            forwarded_host,
            forwarded_proto,
            is_from_trusted_proxy: true,
            proxy_ip: Some(proxy_ip),
        }
    }
}

/// Multi-tier proxy processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiProxyConfig {
    /// Proxy chain validation mode
    pub validation_mode: ValidationMode,
    /// Is RFC 7239 Forwarded header support enabled?
    pub enable_rfc7239: bool,
    /// Maximum proxy hop limit
    pub max_proxy_hops: usize,
    /// Allowed private network CIDR (for internal proxy authentication)
    pub allowed_private_nets: Vec<IpNetwork>,
    /// Whether to enable proxy chain continuity checking
    pub enable_chain_continuity_check: bool,
}

impl Default for MultiProxyConfig {
    fn default() -> Self {
        Self {
            validation_mode: ValidationMode::HopByHop,
            enable_rfc7239: true,
            max_proxy_hops: 10,
            allowed_private_nets: vec![
                IpNetwork::from_str("10.0.0.0/8").unwrap(),
                IpNetwork::from_str("172.16.0.0/12").unwrap(),
                IpNetwork::from_str("192.168.0.0/16").unwrap(),
                IpNetwork::from_str("fd00::/8").unwrap(),
            ],
            enable_chain_continuity_check: true,
        }
    }
}
