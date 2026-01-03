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

use crate::cloud::fetch_cloud_provider_ips;
use crate::{ENV_PROXY_VALIDATION_MODE, MultiProxyConfig, TrustedProxiesConfig, TrustedProxy};
use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use thiserror::Error;

/// Agent handling error types
#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("Invalid IP address format: {0}")]
    InvalidIpFormat(String),

    #[error("Proxy Chain Verification Failed: {0}")]
    ChainValidationFailed(String),

    #[error("Head parsing error: {0}")]
    HeaderParseError(String),
}

/// Proxy chain validation mode
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ValidationMode {
    /// Loose mode: Accept the entire chain as long as the last agent is trusted
    Lenient,
    /// Strict mode: All agents in the chain are required to be trustworthy
    Strict,
    /// Hop Validation: Find the first untrusted agent from right to left
    HopByHop,
}

/// RFC 7239 Forwarded head parser
/// Format:Forwarded: for=192.0.2.60;proto=http;by=203.0.113.43
#[derive(Debug, Clone)]
pub struct ForwardedHeader {
    /// Client address
    pub for_client: Option<IpAddr>,
    /// Proxy identification
    pub by_proxy: Option<IpAddr>,
    /// protocol
    pub proto: Option<String>,
    /// Host
    pub host: Option<String>,
}

/// Proxy chain analysis results
#[derive(Debug, Clone)]
pub struct ProxyChainAnalysis {
    /// Trusted client IP (verified)
    pub trusted_client_ip: IpAddr,
    /// Complete proxy chain (from client to current agent)
    pub full_proxy_chain: Vec<IpAddr>,
    /// Trusted Proxy Chain section
    pub trusted_proxy_chain: Vec<IpAddr>,
    /// Verify the number of hops that pass
    pub validated_hops: usize,
    /// Whether it passes the integrity check
    pub chain_integrity: bool,
    /// The validation mode used
    pub validation_mode_used: ValidationMode,
    /// Possible safety warnings
    pub security_warnings: Vec<String>,
}

/// Optimized CIDR matcher
pub struct CidrMatcher {
    ipv4_networks: Vec<IpNetwork>,
    ipv6_networks: Vec<IpNetwork>,
    single_ips: HashSet<IpAddr>,
}

impl CidrMatcher {
    pub fn new(config: &TrustedProxiesConfig) -> Self {
        let mut ipv4_networks = Vec::new();
        let mut ipv6_networks = Vec::new();
        let mut single_ips = HashSet::new();

        for proxy in &config.proxies {
            match proxy {
                TrustedProxy::Single(ip) => {
                    single_ips.insert(*ip);
                }
                TrustedProxy::Cidr(network) => match network {
                    IpNetwork::V4(_) => ipv4_networks.push(*network),
                    IpNetwork::V6(_) => ipv6_networks.push(*network),
                },
            }
        }

        // Sort by prefix length, with more specific networks taking precedence
        ipv4_networks.sort_by(|a, b| b.prefix().cmp(&a.prefix()));
        ipv6_networks.sort_by(|a, b| b.prefix().cmp(&a.prefix()));

        Self {
            ipv4_networks,
            ipv6_networks,
            single_ips,
        }
    }

    pub fn contains(&self, ip: &IpAddr) -> bool {
        // Start by checking individual IPs
        if self.single_ips.contains(ip) {
            return true;
        }

        // Then check the CIDR range
        match ip {
            IpAddr::V4(ipv4) => {
                for network in &self.ipv4_networks {
                    if let IpNetwork::V4(v4_net) = network
                        && v4_net.contains(*ipv4)
                    {
                        return true;
                    }
                }
            }
            IpAddr::V6(ipv6) => {
                for network in &self.ipv6_networks {
                    if let IpNetwork::V6(v6_net) = network
                        && v6_net.contains(*ipv6)
                    {
                        return true;
                    }
                }
            }
        }

        false
    }
}

/// Environment-specific configuration loading
pub async fn load_production_config() -> MultiProxyConfig {
    let mut config = MultiProxyConfig::default();

    // Read from environment variables
    if let Ok(mode_str) = std::env::var(ENV_PROXY_VALIDATION_MODE) {
        config.validation_mode = match mode_str.as_str() {
            "strict" => ValidationMode::Strict,
            "lenient" => ValidationMode::Lenient,
            _ => ValidationMode::HopByHop,
        };
    }

    // Add trusted agents from cloud provider metadata
    if let Ok(cloud_ips) = fetch_cloud_provider_ips().await {
        for ip_range in cloud_ips {
            if let Ok(network) = ip_range.parse::<IpNetwork>() {
                config.allowed_private_nets.push(network);
            }
        }
    }

    config
}

/// Dynamic configuration updates
pub struct DynamicConfigManager {
    current_config: Arc<std::sync::RwLock<MultiProxyConfig>>,
    config_watcher: tokio::sync::watch::Sender<MultiProxyConfig>,
}

impl DynamicConfigManager {
    pub fn new(initial_config: MultiProxyConfig) -> Self {
        let (sender, _) = tokio::sync::watch::channel(initial_config.clone());

        Self {
            current_config: Arc::new(std::sync::RwLock::new(initial_config)),
            config_watcher: sender,
        }
    }

    pub fn update_config(&self, new_config: MultiProxyConfig) {
        let mut config = self.current_config.write().unwrap();
        *config = new_config.clone();

        // Notify all listeners
        let _ = self.config_watcher.send(new_config);
    }

    pub fn get_config(&self) -> MultiProxyConfig {
        self.current_config.read().unwrap().clone()
    }
}
