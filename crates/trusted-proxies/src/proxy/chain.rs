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

//! Proxy chain analysis and validation logic.

use crate::{ProxyError, TrustedProxyConfig, ValidationMode, is_valid_ip_address};
use axum::http::HeaderMap;
use std::collections::HashSet;
use std::net::IpAddr;
use tracing::trace;

/// Result of analyzing a proxy chain.
#[derive(Debug, Clone)]
pub struct ChainAnalysis {
    /// The identified real client IP address.
    pub client_ip: IpAddr,
    /// The number of validated proxy hops.
    pub hops: usize,
    /// Whether the proxy chain is continuous and trusted.
    pub is_continuous: bool,
    /// List of warnings generated during analysis.
    pub warnings: Vec<String>,
    /// The validation mode used for analysis.
    pub validation_mode: ValidationMode,
    /// The portion of the chain that consists of trusted proxies.
    pub trusted_chain: Vec<IpAddr>,
}

/// Analyzer for verifying the integrity of proxy chains.
#[derive(Debug, Clone)]
pub struct ProxyChainAnalyzer {
    /// Configuration for trusted proxies.
    config: TrustedProxyConfig,
    /// Cache of trusted IP addresses for fast lookup.
    trusted_ip_cache: HashSet<IpAddr>,
}

impl ProxyChainAnalyzer {
    /// Creates a new `ProxyChainAnalyzer`.
    pub fn new(config: TrustedProxyConfig) -> Self {
        let mut trusted_ip_cache = HashSet::new();

        for proxy in &config.proxies {
            match proxy {
                crate::TrustedProxy::Single(ip) => {
                    trusted_ip_cache.insert(*ip);
                }
                crate::TrustedProxy::Cidr(network) => {
                    // For small networks, cache all IPs to speed up lookups.
                    // Only cache IPv4 networks to avoid iterating huge IPv6 ranges.
                    if network.is_ipv4() && network.prefix() >= 24 {
                        for ip in network.iter() {
                            trusted_ip_cache.insert(ip);
                        }
                    }
                }
            }
        }

        Self {
            config,
            trusted_ip_cache,
        }
    }

    /// Analyzes a proxy chain to identify the real client IP and verify trust.
    pub fn analyze_chain(
        &self,
        proxy_chain: &[IpAddr],
        current_proxy_ip: IpAddr,
        headers: &HeaderMap,
    ) -> Result<ChainAnalysis, ProxyError> {
        trace!("Analyzing proxy chain: {:?} with current proxy: {}", proxy_chain, current_proxy_ip);

        // Validate all IP addresses in the chain.
        self.validate_ip_addresses(proxy_chain)?;

        // Construct the full chain including the direct peer.
        let mut full_chain = proxy_chain.to_vec();
        full_chain.push(current_proxy_ip);

        // Enforce maximum hop limit.
        if full_chain.len() > self.config.max_hops {
            return Err(ProxyError::ChainTooLong(full_chain.len(), self.config.max_hops));
        }

        // Analyze the chain based on the configured validation mode.
        let (client_ip, trusted_chain, hops) = match self.config.validation_mode {
            ValidationMode::Lenient => self.analyze_lenient(&full_chain),
            ValidationMode::Strict => self.analyze_strict(&full_chain)?,
            ValidationMode::HopByHop => self.analyze_hop_by_hop(&full_chain),
        };

        // Check for chain continuity if enabled.
        let is_continuous = if self.config.enable_chain_continuity_check {
            self.check_chain_continuity(&full_chain, &trusted_chain)
        } else {
            true
        };

        // Collect any warnings.
        let warnings = self.collect_warnings(&full_chain, &trusted_chain, headers);

        // Final validation of the identified client IP.
        if !is_valid_ip_address(&client_ip) {
            return Err(ProxyError::internal(format!("Invalid client IP identified: {}", client_ip)));
        }

        Ok(ChainAnalysis {
            client_ip,
            hops,
            is_continuous,
            warnings,
            validation_mode: self.config.validation_mode,
            trusted_chain,
        })
    }

    /// Lenient mode: Accepts the entire chain if the last proxy is trusted.
    fn analyze_lenient(&self, chain: &[IpAddr]) -> (IpAddr, Vec<IpAddr>, usize) {
        if chain.is_empty() {
            return (IpAddr::from([0, 0, 0, 0]), Vec::new(), 0);
        }

        if let Some(last_proxy) = chain.last()
            && self.is_ip_trusted(last_proxy)
        {
            let client_ip = chain.first().copied().unwrap_or(*last_proxy);
            return (client_ip, chain.to_vec(), chain.len());
        }

        let client_ip = chain.first().copied().unwrap_or(IpAddr::from([0, 0, 0, 0]));
        (client_ip, Vec::new(), 0)
    }

    /// Strict mode: Requires every IP in the chain to be trusted.
    fn analyze_strict(&self, chain: &[IpAddr]) -> Result<(IpAddr, Vec<IpAddr>, usize), ProxyError> {
        if chain.is_empty() {
            return Ok((IpAddr::from([0, 0, 0, 0]), Vec::new(), 0));
        }

        for (i, ip) in chain.iter().enumerate() {
            if !self.is_ip_trusted(ip) {
                return Err(ProxyError::chain_failed(format!("Proxy at position {} ({}) is not trusted", i, ip)));
            }
        }

        let client_ip = chain.first().copied().unwrap_or(IpAddr::from([0, 0, 0, 0]));
        Ok((client_ip, chain.to_vec(), chain.len()))
    }

    /// Hop-by-hop mode: Traverses the chain from right to left to find the first untrusted IP.
    fn analyze_hop_by_hop(&self, chain: &[IpAddr]) -> (IpAddr, Vec<IpAddr>, usize) {
        if chain.is_empty() {
            return (IpAddr::from([0, 0, 0, 0]), Vec::new(), 0);
        }

        let mut trusted_chain = Vec::new();
        let mut validated_hops = 0;

        // Traverse from the most recent proxy back towards the client.
        for ip in chain.iter().rev() {
            if self.is_ip_trusted(ip) {
                trusted_chain.insert(0, *ip);
                validated_hops += 1;
            } else {
                break;
            }
        }

        if trusted_chain.is_empty() {
            let client_ip = *chain.last().unwrap();
            (client_ip, vec![client_ip], 0)
        } else {
            let client_ip_index = chain.len().saturating_sub(trusted_chain.len());
            let client_ip = if client_ip_index > 0 {
                chain[client_ip_index - 1]
            } else {
                chain[0]
            };

            (client_ip, trusted_chain, validated_hops)
        }
    }

    /// Verifies that the trusted portion of the chain is a continuous suffix of the full chain.
    fn check_chain_continuity(&self, full_chain: &[IpAddr], trusted_chain: &[IpAddr]) -> bool {
        if full_chain.len() <= 1 || trusted_chain.is_empty() {
            return true;
        }

        if trusted_chain.len() > full_chain.len() {
            return false;
        }

        let expected_tail = &full_chain[full_chain.len() - trusted_chain.len()..];
        expected_tail == trusted_chain
    }

    /// Validates that IP addresses are not unspecified, multicast, or otherwise invalid.
    fn validate_ip_addresses(&self, chain: &[IpAddr]) -> Result<(), ProxyError> {
        for ip in chain {
            if ip.is_unspecified() {
                return Err(ProxyError::invalid_xff("IP address cannot be unspecified (0.0.0.0 or ::)"));
            }

            if ip.is_multicast() {
                return Err(ProxyError::invalid_xff("IP address cannot be multicast"));
            }

            if !is_valid_ip_address(ip) {
                return Err(ProxyError::IpParseError(format!("Invalid IP address in chain: {}", ip)));
            }
        }

        Ok(())
    }

    /// Checks if an IP address is trusted based on the configuration.
    fn is_ip_trusted(&self, ip: &IpAddr) -> bool {
        if self.trusted_ip_cache.contains(ip) {
            return true;
        }

        self.config.proxies.iter().any(|proxy| proxy.contains(ip))
    }

    /// Collects warnings about potential issues in the proxy chain.
    fn collect_warnings(&self, full_chain: &[IpAddr], trusted_chain: &[IpAddr], headers: &HeaderMap) -> Vec<String> {
        let mut warnings = Vec::new();

        if !trusted_chain.is_empty() && !headers.contains_key("x-forwarded-for") && !headers.contains_key("forwarded") {
            warnings.push("No proxy headers found for request from trusted proxy".to_string());
        }

        let mut seen_ips = HashSet::new();
        for ip in full_chain {
            if !seen_ips.insert(ip) {
                warnings.push(format!("Duplicate IP address detected in proxy chain: {}", ip));
                break;
            }
        }

        warnings
    }
}
