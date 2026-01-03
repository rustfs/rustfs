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
    CidrMatcher, ForwardedHeader, IpValidationCache, MultiProxyConfig, ProxyChainAnalysis, ProxyError, TrustedProxiesConfig,
    TrustedProxy, ValidationMode,
};
use ipnetwork::IpNetwork;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Securely handle X-Forwarded-For headers for multi-layer agents
pub struct ProxyChainProcessor {
    config: TrustedProxiesConfig,
}

impl ProxyChainProcessor {
    pub fn new(config: TrustedProxiesConfig) -> Self {
        Self { config }
    }

    /// Parse the X-Forwarded-For chain to find the rightmost untrusted IP
    /// Principle: Traverse from right to left to find the first IP that is not on the trusted list
    pub fn extract_client_ip_from_chain(&self, x_forwarded_for: &str, current_proxy_ip: IpAddr) -> Option<IpAddr> {
        // Split the IP chain
        let ip_chain: Vec<&str> = x_forwarded_for
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        if ip_chain.is_empty() {
            return None;
        }

        // Build a complete chain: client, proxy1, proxy2, ..., current_proxy
        let mut full_chain: Vec<IpAddr> = ip_chain.iter().filter_map(|s| s.parse().ok()).collect();
        full_chain.push(current_proxy_ip);

        // Find from right to left (starting with the agent closest to us)
        // Find the first untrusted IP
        for ip in full_chain.iter().rev() {
            if !self.is_ip_trusted(ip) {
                return Some(*ip);
            }
        }

        // If all IPs are trusted, return the first IP of the original chain
        full_chain.first().copied()
    }

    /// Check if a single IP is trustworthy
    fn is_ip_trusted(&self, ip: &IpAddr) -> bool {
        // Simplifying the implementation here, you should actually use the full SocketAddr
        // Port information needs to be considered in a production environment
        use std::net::SocketAddr;
        let dummy_port = 0;
        let addr = SocketAddr::new(*ip, dummy_port);
        self.config.is_trusted(&addr)
    }
}

/// Multi-layer proxy processor core implementation
pub struct MultiProxyProcessor {
    /// Basic trusted agent configuration
    base_config: TrustedProxiesConfig,
    /// Advanced configuration
    advanced_config: MultiProxyConfig,
    /// Known collection of trusted agents (cache, accelerated lookup)
    trusted_ips_cache: HashSet<IpAddr>,
    /// A collection of known private networks
    private_nets_cache: Vec<IpNetwork>,
}

impl MultiProxyProcessor {
    /// Create a new processor
    pub fn new(base_config: TrustedProxiesConfig, advanced_config: Option<MultiProxyConfig>) -> Self {
        let config = advanced_config.unwrap_or_default();

        // Build IP caches to improve performance
        let mut trusted_ips_cache = HashSet::new();
        for proxy in &base_config.proxies {
            match proxy {
                TrustedProxy::Single(ip) => {
                    trusted_ips_cache.insert(*ip);
                }
                TrustedProxy::Cidr(network) => {
                    // For large networks, we only cache the network itself, runtime checks
                    // Here we cache the prefix of the network
                    if network.prefix() >= 24 {
                        // For small networks (/24 and above), we can cache all IPs
                        // But for simplicity, only the network representation is cached here
                    }
                }
            }
        }

        Self {
            base_config,
            advanced_config: config.clone(),
            trusted_ips_cache,
            private_nets_cache: config.allowed_private_nets.clone(),
        }
    }

    /// Main processing function: Extracts and verifies the client IP from the request
    pub fn process_request(&self, peer_addr: &SocketAddr, headers: &http::HeaderMap) -> Result<ProxyChainAnalysis, ProxyError> {
        debug!("Start processing proxy request, peer address: {}", peer_addr);

        // 1. Check if it's from a trusted agent
        if !self.base_config.is_trusted(peer_addr) {
            warn!("Request from Untrusted Agent: {}", peer_addr);
            return self.handle_untrusted_source(peer_addr);
        }

        // 2. Try multiple head resolution strategies
        let analysis = self.analyze_proxy_chain(peer_addr.ip(), headers)?;

        // 3. Perform security checks
        self.perform_security_checks(&analysis)?;

        Ok(analysis)
    }

    /// Analyze the core approach of proxy chains
    fn analyze_proxy_chain(&self, current_proxy_ip: IpAddr, headers: &http::HeaderMap) -> Result<ProxyChainAnalysis, ProxyError> {
        // Collect all available proxy chain information
        let mut proxy_chains = Vec::new();
        let mut security_warnings = Vec::new();

        // Strategy 1: Prioritize the use of RFC 7239 Forwarded headers
        if self.advanced_config.enable_rfc7239
            && let Some(forwarded) = Self::parse_forwarded_header(headers)
            && let Some(rfc_chain) = Self::extract_chain_from_forwarded(&forwarded)
        {
            proxy_chains.push(("rfc7239", rfc_chain));
        }

        // Strategy 2: Use traditional X-Forwarded-For heads
        if let Some(xff_chain) = Self::parse_x_forwarded_for(headers) {
            proxy_chains.push(("xff", xff_chain));
        }

        // Strategy 3: Use X-Real-IP as a backup
        if let Some(real_ip) = Self::parse_x_real_ip(headers) {
            proxy_chains.push(("x-real-ip", vec![real_ip]));
        }

        // If no proxy information is found
        if proxy_chains.is_empty() {
            debug!("No proxy header found, using direct connection IP");
            return Ok(ProxyChainAnalysis {
                trusted_client_ip: current_proxy_ip,
                full_proxy_chain: vec![current_proxy_ip],
                trusted_proxy_chain: vec![current_proxy_ip],
                validated_hops: 0,
                chain_integrity: true,
                validation_mode_used: ValidationMode::Lenient,
                security_warnings: vec!["Agent header not used, possibly direct connection".to_string()],
            });
        }

        // Choose the most reliable proxy chain (RFC 7239 preferred)
        let (source, mut full_chain) = proxy_chains
            .into_iter()
            .max_by_key(|(source, _)| match *source {
                "rfc7239" => 3,
                "xff" => 2,
                "x-real-ip" => 1,
                _ => 0,
            })
            .unwrap();

        debug!("Use proxy chain source: {}, chain: {:?}", source, full_chain);

        // Add the current proxy IP to the end of the chain
        full_chain.push(current_proxy_ip);

        // Handle the proxy chain according to the configured validation mode
        let (trusted_client_ip, trusted_proxy_chain, validated_hops) = match self.advanced_config.validation_mode {
            ValidationMode::Lenient => self.validate_lenient(&full_chain),
            ValidationMode::Strict => self.validate_strict(&full_chain)?,
            ValidationMode::HopByHop => self.validate_hop_by_hop(&full_chain)?,
        };

        // Check for chain continuity
        let chain_integrity = if self.advanced_config.enable_chain_continuity_check {
            self.check_chain_continuity(&full_chain, &trusted_proxy_chain)
        } else {
            true
        };

        // Collect safety warnings
        if !chain_integrity {
            security_warnings.push("Proxy chain continuity check failed".to_string());
        }

        if full_chain.len() > self.advanced_config.max_proxy_hops {
            security_warnings.push(format!(
                "Proxy chain length exceeds the limit:{}/{}",
                full_chain.len(),
                self.advanced_config.max_proxy_hops
            ));
        }

        Ok(ProxyChainAnalysis {
            trusted_client_ip,
            full_proxy_chain: full_chain,
            trusted_proxy_chain,
            validated_hops,
            chain_integrity,
            validation_mode_used: self.advanced_config.validation_mode,
            security_warnings,
        })
    }

    /// Permissive validation mode: As long as the last agent is trusted, the entire chain is accepted
    fn validate_lenient(&self, chain: &[IpAddr]) -> (IpAddr, Vec<IpAddr>, usize) {
        if chain.is_empty() {
            return (IpAddr::from([0, 0, 0, 0]), vec![], 0);
        }

        // Take the first IP in the chain as the client IP
        let client_ip = chain[0];

        // Verify that the last agent is trustworthy
        let last_proxy = chain.last().unwrap();
        let is_last_trusted = self.is_ip_trusted(last_proxy);

        if is_last_trusted {
            (client_ip, chain.to_vec(), chain.len())
        } else {
            // If the last proxy is not trusted, use the IP of the last proxy
            (*last_proxy, vec![*last_proxy], 0)
        }
    }

    /// Strict Verification Model: All agents in the chain are required to be trustworthy
    fn validate_strict(&self, chain: &[IpAddr]) -> Result<(IpAddr, Vec<IpAddr>, usize), ProxyError> {
        if chain.is_empty() {
            return Ok((IpAddr::from([0, 0, 0, 0]), vec![], 0));
        }

        // Check if each agent is trustworthy
        for (i, ip) in chain.iter().enumerate() {
            if !self.is_ip_trusted(ip) {
                return Err(ProxyError::ChainValidationFailed(format!(
                    "The {} agent ({}) in the chain is not trustworthy",
                    i + 1,
                    ip
                )));
            }
        }

        Ok((
            chain[0], // The first IP is the client
            chain.to_vec(),
            chain.len(),
        ))
    }

    /// Hop Validation Mode: Find the first untrusted agent from right to left
    fn validate_hop_by_hop(&self, chain: &[IpAddr]) -> Result<(IpAddr, Vec<IpAddr>, usize), ProxyError> {
        if chain.is_empty() {
            return Ok((IpAddr::from([0, 0, 0, 0]), vec![], 0));
        }

        let mut trusted_chain = Vec::new();
        let mut validated_hops = 0;

        // Traversing from right to left (starting with the agent closest to us)
        for ip in chain.iter().rev() {
            if self.is_ip_trusted(ip) {
                trusted_chain.insert(0, *ip);
                validated_hops += 1;
            } else {
                // Find the first untrusted agent and stop traversing
                break;
            }
        }

        if trusted_chain.is_empty() {
            // No trusted proxy, using the last IP of the chain
            let last_ip = *chain.last().unwrap();
            Ok((last_ip, vec![last_ip], 0))
        } else {
            // The client IP is the one that preceded the first IP of the trusted chain
            // Or if the entire chain is trustworthy, it is the first IP of the original chain
            let client_ip_index = chain.len().saturating_sub(trusted_chain.len());
            let client_ip = if client_ip_index > 0 {
                chain[client_ip_index - 1]
            } else {
                chain[0]
            };

            Ok((client_ip, trusted_chain, validated_hops))
        }
    }

    /// Check the continuity of the proxy chain
    /// Validation rules: The path from the client to the server should be continuous
    fn check_chain_continuity(&self, full_chain: &[IpAddr], trusted_chain: &[IpAddr]) -> bool {
        if full_chain.len() <= 1 || trusted_chain.is_empty() {
            return true;
        }

        // Verify that the trusted chain is indeed the tail continuous part of the complete chain
        let expected_tail = &full_chain[full_chain.len() - trusted_chain.len()..];
        expected_tail == trusted_chain
    }

    /// Handle requests from untrusted sources
    fn handle_untrusted_source(&self, peer_addr: &SocketAddr) -> Result<ProxyChainAnalysis, ProxyError> {
        let ip = peer_addr.ip();

        // Check if it's a private address (it could be an internal service call)
        let is_private = self.private_nets_cache.iter().any(|network| network.contains(ip));

        let warnings = if is_private {
            vec!["From an internal network but not configured as a trusted agent".to_string()]
        } else {
            vec!["From an untrusted public address".to_string()]
        };

        Ok(ProxyChainAnalysis {
            trusted_client_ip: ip,
            full_proxy_chain: vec![ip],
            trusted_proxy_chain: vec![ip],
            validated_hops: 0,
            chain_integrity: true,
            validation_mode_used: ValidationMode::Lenient,
            security_warnings: warnings,
        })
    }

    /// Perform security checks
    fn perform_security_checks(&self, analysis: &ProxyChainAnalysis) -> Result<(), ProxyError> {
        // Check the proxy chain length
        if analysis.full_proxy_chain.len() > self.advanced_config.max_proxy_hops {
            return Err(ProxyError::ChainValidationFailed(format!(
                "The proxy chain is too long:{} > {}",
                analysis.full_proxy_chain.len(),
                self.advanced_config.max_proxy_hops
            )));
        }

        // Check if the client IP is valid
        if analysis.trusted_client_ip.is_unspecified() {
            return Err(ProxyError::ChainValidationFailed(
                "The client IP is an unspecified address (0.0.0.0 or::)".to_string(),
            ));
        }

        // Check if the loopback address (it could be a misconfiguration or an attack)
        if analysis.trusted_client_ip.is_loopback() && analysis.validated_hops > 1 {
            warn!(
                "The client IP is a loopback address, but it goes through multiple layers of proxies: {}",
                analysis.trusted_client_ip
            );
        }

        // Check the multicast address
        if analysis.trusted_client_ip.is_multicast() {
            return Err(ProxyError::ChainValidationFailed("The client IP is the multicast address".to_string()));
        }

        Ok(())
    }

    /// Check if a single IP is trustworthy
    fn is_ip_trusted(&self, ip: &IpAddr) -> bool {
        // Check the cache first
        if self.trusted_ips_cache.contains(ip) {
            return true;
        }

        // Then check the CIDR range
        let dummy_port = 0;
        let addr = SocketAddr::new(*ip, dummy_port);
        self.base_config.is_trusted(&addr)
    }

    /// Parsing RFC 7239 Forwarded head
    fn parse_forwarded_header(headers: &http::HeaderMap) -> Option<ForwardedHeader> {
        let forwarded_value = headers.get("forwarded")?.to_str().ok()?;

        // Forwarded headers can have multiple values, separated by commas
        // We take the first one
        let first_part = forwarded_value.split(',').next()?.trim();

        let mut result = ForwardedHeader {
            for_client: None,
            by_proxy: None,
            proto: None,
            host: None,
        };

        // Parsing key-value pairs, such as:for=192.0.2.60;proto=http;by=203.0.113.43
        for part in first_part.split(';') {
            let part = part.trim();
            if let Some((key, value)) = part.split_once('=') {
                match key.trim().to_lowercase().as_str() {
                    "for" => {
                        // Remove possible quotes and port numbers
                        let clean_value = value.trim_matches('"');
                        if let Ok(ip) = clean_value.split(':').next().unwrap_or(clean_value).parse() {
                            result.for_client = Some(ip);
                        }
                    }
                    "by" => {
                        let clean_value = value.trim_matches('"');
                        if let Ok(ip) = clean_value.split(':').next().unwrap_or(clean_value).parse() {
                            result.by_proxy = Some(ip);
                        }
                    }
                    "proto" => {
                        result.proto = Some(value.trim_matches('"').to_string());
                    }
                    "host" => {
                        result.host = Some(value.trim_matches('"').to_string());
                    }
                    _ => {}
                }
            }
        }

        Some(result)
    }

    /// Extract the proxy chain from the Forwarded header
    fn extract_chain_from_forwarded(forwarded: &ForwardedHeader) -> Option<Vec<IpAddr>> {
        let mut chain = Vec::new();

        // If there is a for field, it is the client IP
        if let Some(client_ip) = &forwarded.for_client {
            chain.push(*client_ip);
        }

        // If there is a by field, it may be a proxy IP
        // Note: In RFC 7239, multiple agents will have multiple Forwarded header values
        // Simplify the processing here, just take one

        if chain.is_empty() { None } else { Some(chain) }
    }

    /// Parsing X-Forwarded-For head
    fn parse_x_forwarded_for(headers: &http::HeaderMap) -> Option<Vec<IpAddr>> {
        let xff_value = headers.get("x-forwarded-for")?.to_str().ok()?;

        let ips: Vec<IpAddr> = xff_value
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .filter_map(|s| {
                // May contain the port number, take only the IP part
                let ip_part = s.split(':').next().unwrap_or(s);
                ip_part.parse().ok()
            })
            .collect();

        if ips.is_empty() { None } else { Some(ips) }
    }

    /// Parsing the X-Real-IP head
    fn parse_x_real_ip(headers: &http::HeaderMap) -> Option<IpAddr> {
        let value = headers.get("x-real-ip")?.to_str().ok()?;
        value.parse().ok()
    }
}

// Update the MultiProxyProcessor to use the cache
pub struct OptimizedMultiProxyProcessor {
    base_config: Arc<TrustedProxiesConfig>,
    advanced_config: MultiProxyConfig,
    ip_cache: std::sync::Mutex<IpValidationCache>,
    cidr_matcher: CidrMatcher,
}

impl OptimizedMultiProxyProcessor {
    pub fn new(base_config: TrustedProxiesConfig, advanced_config: Option<MultiProxyConfig>) -> Self {
        let config = advanced_config.unwrap_or_default();

        // Pre-compiled CIDR matcher
        let cidr_matcher = CidrMatcher::new(&base_config);

        Self {
            base_config: Arc::new(base_config),
            advanced_config: config,
            ip_cache: std::sync::Mutex::new(IpValidationCache::new(10000, Duration::from_secs(300))),
            cidr_matcher,
        }
    }

    /// Optimized IP trusted checks
    async fn is_ip_trusted_optimized(&self, ip: &IpAddr) -> bool {
        let cache = self.ip_cache.lock().unwrap();

        cache
            .is_trusted(ip, |ip| {
                // Fast Path: Check individual IP caches
                // Slow path: Check the CIDR range
                self.cidr_matcher.contains(ip)
            })
            .await
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MultiProxyProcessor, ProxyChainAnalysis, TrustedProxiesConfig, ValidationMode};
    use axum::http::HeaderMap;
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;

    fn create_test_processor() -> MultiProxyProcessor {
        let base_config =
            TrustedProxiesConfig::from_strs(&["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.1"]).unwrap();

        MultiProxyProcessor::new(base_config, None)
    }

    #[test]
    fn test_parse_x_forwarded_for() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Forwarded-For", "203.0.113.195, 198.51.100.1, 10.0.1.100".parse().unwrap());

        let result = MultiProxyProcessor::parse_x_forwarded_for(&headers).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], IpAddr::from_str("203.0.113.195").unwrap());
    }

    #[test]
    fn test_parse_x_forwarded_for_with_ports() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Forwarded-For", "203.0.113.195:1234, 198.51.100.1:80".parse().unwrap());

        let result = MultiProxyProcessor::parse_x_forwarded_for(&headers).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], IpAddr::from_str("203.0.113.195").unwrap());
    }

    #[test]
    fn test_parse_forwarded_header() {
        let mut headers = HeaderMap::new();
        headers.insert("Forwarded", r#"for=192.0.2.60;proto=http;by=203.0.113.43"#.parse().unwrap());

        let result = MultiProxyProcessor::parse_forwarded_header(&headers).unwrap();
        assert_eq!(result.for_client, Some(IpAddr::from_str("192.0.2.60").unwrap()));
        assert_eq!(result.by_proxy, Some(IpAddr::from_str("203.0.113.43").unwrap()));
        assert_eq!(result.proto, Some("http".to_string()));
    }

    #[test]
    fn test_validate_hop_by_hop() {
        let processor = create_test_processor();

        // Test chain: Client (public) -> Proxy 1 (private) -> Proxy 2 (private)
        let chain = vec![
            IpAddr::from_str("8.8.8.8").unwrap(),      // Client (not trusted)
            IpAddr::from_str("10.0.1.100").unwrap(),   // Agent 1 (Trusted)
            IpAddr::from_str("192.168.1.50").unwrap(), // Agent 2 (Trusted)
        ];

        let result = processor.validate_hop_by_hop(&chain).unwrap();
        assert_eq!(result.0, IpAddr::from_str("8.8.8.8").unwrap()); // Client IP
        assert_eq!(result.2, 2); // Verified 2 jumps
    }

    #[test]
    fn test_chain_continuity_check() {
        let processor = create_test_processor();

        // Complete chain
        let full_chain = vec![
            IpAddr::from_str("8.8.8.8").unwrap(),
            IpAddr::from_str("10.0.1.100").unwrap(),
            IpAddr::from_str("192.168.1.50").unwrap(),
        ];

        // The trusted chain should be the tail continuous part of the complete chain
        let trusted_chain = vec![
            IpAddr::from_str("10.0.1.100").unwrap(),
            IpAddr::from_str("192.168.1.50").unwrap(),
        ];

        assert!(processor.check_chain_continuity(&full_chain, &trusted_chain));

        // Discontinuous cases should fail
        let bad_trusted_chain = vec![IpAddr::from_str("192.168.1.50").unwrap()];
        assert!(!processor.check_chain_continuity(&full_chain, &bad_trusted_chain));
    }

    #[test]
    fn test_security_checks() {
        let processor = create_test_processor();

        // Create test analysis results
        let analysis = ProxyChainAnalysis {
            trusted_client_ip: IpAddr::from([0, 0, 0, 0]), // Invalid address
            full_proxy_chain: Vec::new(),
            trusted_proxy_chain: Vec::new(),
            validated_hops: 0,
            chain_integrity: true,
            validation_mode_used: ValidationMode::Lenient,
            security_warnings: Vec::new(),
        };

        // Should fail because the client IP is an unspecified address
        assert!(processor.perform_security_checks(&analysis).is_err());
    }

    #[tokio::test]
    async fn test_complex_proxy_chain() {
        use crate::TrustedProxiesConfig;
        use crate::{MultiProxyConfig, MultiProxyProcessor, ValidationMode};
        use axum::http::HeaderMap;
        use std::net::SocketAddr;

        // Create a test configuration
        let base_config = TrustedProxiesConfig::from_strs(&["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]).unwrap();

        let advanced_config = MultiProxyConfig {
            validation_mode: ValidationMode::HopByHop,
            enable_rfc7239: true,
            max_proxy_hops: 5,
            ..Default::default()
        };

        let processor = MultiProxyProcessor::new(base_config, Some(advanced_config));

        // Simulate complex proxy chains
        let peer_addr = SocketAddr::from(([192, 168, 1, 100], 8080));

        let mut headers = HeaderMap::new();
        headers.insert("X-Forwarded-For", "203.0.113.195, 10.0.1.50, 172.16.0.10, 192.168.1.1".parse().unwrap());
        headers.insert("X-Forwarded-Proto", "https".parse().unwrap());
        headers.insert("X-Forwarded-Host", "api.example.com".parse().unwrap());

        // Test processing
        let result = processor.process_request(&peer_addr, &headers).unwrap();

        assert_eq!(result.trusted_client_ip.to_string(), "203.0.113.195");
        assert_eq!(result.validated_hops, 3); // 192.168.1.1, 172.16.0.10, 10.0.1.50
        assert!(result.chain_integrity);

        // Test RFC 7239 head
        let mut rfc_headers = HeaderMap::new();
        rfc_headers.insert(
            "Forwarded",
            r#"for=192.0.2.60;proto=https;by=203.0.113.43,for=198.51.100.17"#.parse().unwrap(),
        );

        let rfc_result = processor.process_request(&peer_addr, &rfc_headers).unwrap();
        assert_eq!(rfc_result.trusted_client_ip.to_string(), "192.0.2.60");
    }

    #[tokio::test]
    async fn test_proxy_chain_attack_scenarios() {
        use crate::MultiProxyProcessor;

        let base_config = TrustedProxiesConfig::from_strs(&["10.0.0.0/8"]).unwrap();
        let processor = MultiProxyProcessor::new(base_config, None);

        // Scenario 1: Excessive Proxy Chain Attack
        let peer_addr = SocketAddr::from(([10, 0, 0, 1], 80));
        let mut headers = HeaderMap::new();

        // Create a very long chain (10 hops over the default limit)
        let long_chain = (0..15).map(|i| format!("10.0.{}.1", i)).collect::<Vec<_>>().join(", ");

        headers.insert("X-Forwarded-For", long_chain.parse().unwrap());

        let result = processor.process_request(&peer_addr, &headers);
        assert!(result.is_err());

        // Scenario 2: IP spoofing attack
        let mut attack_headers = HeaderMap::new();
        attack_headers.insert("X-Forwarded-For", "8.8.8.8".parse().unwrap());

        let attack_result = processor.process_request(&peer_addr, &attack_headers).unwrap();
        // 8.8.8.8 should be correctly identified as the client IP (because the proxy is trusted)
        assert_eq!(attack_result.trusted_client_ip.to_string(), "8.8.8.8");

        // Scenario 3: Untrustworthy agent attempts to spoof
        let untrusted_peer = SocketAddr::from(([8, 8, 8, 8], 80));
        let mut fake_headers = HeaderMap::new();
        fake_headers.insert("X-Forwarded-For", "10.0.0.100".parse().unwrap());

        let fake_result = processor.process_request(&untrusted_peer, &fake_headers).unwrap();
        // X-Forwarded-For should be ignored and 8.8.8.8 is used as the client IP
        assert_eq!(fake_result.trusted_client_ip.to_string(), "8.8.8.8");
    }
}
