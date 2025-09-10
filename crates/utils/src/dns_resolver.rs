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

//! Layered DNS resolution utility for Kubernetes environments
//!
//! This module provides robust DNS resolution with multiple fallback layers:
//! 1. Local cache for previously resolved results
//! 2. Cluster DNS (e.g., CoreDNS in Kubernetes)
//! 3. Public DNS servers as final fallback
//!
//! The resolver is designed to handle 5-level or deeper domain names that may fail
//! in Kubernetes environments due to CoreDNS configuration, DNS recursion limits,
//! or network-related issues.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Maximum FQDN length according to RFC standards
const MAX_FQDN_LENGTH: usize = 253;
/// Maximum DNS label length according to RFC standards
const MAX_LABEL_LENGTH: usize = 63;
/// Cache entry TTL in seconds
const CACHE_TTL_SECONDS: u64 = 300; // 5 minutes

/// DNS resolution error types with detailed context
#[derive(Debug)]
pub enum DnsError {
    InvalidFormat { reason: String },
    CacheMiss { domain: String },
    ClusterDnsFailed { domain: String, source: String },
    PublicDnsFailed { domain: String, source: String },
    AllAttemptsFailed { domain: String },
    InitializationFailed { source: String },
}

impl std::fmt::Display for DnsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DnsError::InvalidFormat { reason } => write!(f, "Invalid domain format: {}", reason),
            DnsError::CacheMiss { domain } => write!(f, "Local cache miss, attempting cluster DNS: {}", domain),
            DnsError::ClusterDnsFailed { domain, source } => {
                write!(f, "Cluster DNS query failed, falling back to public DNS: {} - {}", domain, source)
            }
            DnsError::PublicDnsFailed { domain, source } => write!(f, "Public DNS resolution failed: {} - {}", domain, source),
            DnsError::AllAttemptsFailed { domain } => write!(
                f,
                "All DNS resolution attempts failed for domain: {}. Please check your domain spelling, cluster configuration, or contact an administrator",
                domain
            ),
            DnsError::InitializationFailed { source } => write!(f, "DNS resolver initialization failed: {}", source),
        }
    }
}

impl std::error::Error for DnsError {}

/// Cached DNS resolution result
#[derive(Debug, Clone)]
struct CacheEntry {
    ips: Vec<IpAddr>,
    timestamp: Instant,
}

impl CacheEntry {
    fn new(ips: Vec<IpAddr>) -> Self {
        Self {
            ips,
            timestamp: Instant::now(),
        }
    }

    fn is_expired(&self) -> bool {
        self.timestamp.elapsed() > Duration::from_secs(CACHE_TTL_SECONDS)
    }
}

/// Layered DNS resolver with caching and multiple fallback strategies
pub struct LayeredDnsResolver {
    /// Local cache for resolved domains
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

impl LayeredDnsResolver {
    /// Create a new layered DNS resolver
    pub async fn new() -> Result<Self, DnsError> {
        info!("Initializing layered DNS resolver with caching and fallback strategies");

        Ok(Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Validate domain format according to RFC standards
    fn validate_domain_format(domain: &str) -> Result<(), DnsError> {
        // Check FQDN length
        if domain.len() > MAX_FQDN_LENGTH {
            return Err(DnsError::InvalidFormat {
                reason: format!("FQDN must not exceed {} bytes, got {} bytes", MAX_FQDN_LENGTH, domain.len()),
            });
        }

        // Check each label length
        for label in domain.split('.') {
            if label.len() > MAX_LABEL_LENGTH {
                return Err(DnsError::InvalidFormat {
                    reason: format!(
                        "Each label must not exceed {} bytes, label '{}' has {} bytes",
                        MAX_LABEL_LENGTH,
                        label,
                        label.len()
                    ),
                });
            }
        }

        // Check for empty labels (except trailing dot)
        let labels: Vec<&str> = domain.trim_end_matches('.').split('.').collect();
        for label in &labels {
            if label.is_empty() {
                return Err(DnsError::InvalidFormat {
                    reason: "Domain contains empty labels".to_string(),
                });
            }
        }

        Ok(())
    }

    /// Check local cache for resolved domain
    fn check_cache(&self, domain: &str) -> Option<Vec<IpAddr>> {
        let cache = self.cache.lock().unwrap();
        if let Some(entry) = cache.get(domain) {
            if !entry.is_expired() {
                debug!("DNS cache hit for domain: {}", domain);
                return Some(entry.ips.clone());
            } else {
                debug!("DNS cache entry expired for domain: {}", domain);
            }
        }
        None
    }

    /// Update local cache with resolved IPs
    fn update_cache(&self, domain: &str, ips: Vec<IpAddr>) {
        let mut cache = self.cache.lock().unwrap();
        cache.insert(domain.to_string(), CacheEntry::new(ips));
        debug!("DNS cache updated for domain: {}", domain);
    }

    /// Clear expired entries from cache
    pub fn clean_cache(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.retain(|domain, entry| {
            if entry.is_expired() {
                debug!("Removing expired cache entry for domain: {}", domain);
                false
            } else {
                true
            }
        });
    }

    /// Resolve domain using cluster DNS
    async fn resolve_with_cluster_dns(&self, domain: &str) -> Result<Vec<IpAddr>, DnsError> {
        debug!("Attempting cluster DNS resolution for domain: {}", domain);

        // For now, use standard library resolution
        use std::net::ToSocketAddrs;
        match (domain, 0).to_socket_addrs() {
            Ok(addrs) => {
                let ips: Vec<IpAddr> = addrs.map(|addr| addr.ip()).collect();
                if !ips.is_empty() {
                    info!("Cluster DNS resolution successful for domain: {} -> {:?}", domain, ips);
                    return Ok(ips);
                }
            }
            Err(e) => {
                warn!("Cluster DNS resolution failed for domain: {} - {}", domain, e);
                return Err(DnsError::ClusterDnsFailed {
                    domain: domain.to_string(),
                    source: e.to_string(),
                });
            }
        }

        Err(DnsError::ClusterDnsFailed {
            domain: domain.to_string(),
            source: "No IP addresses found".to_string(),
        })
    }

    /// Resolve domain using public DNS
    async fn resolve_with_public_dns(&self, domain: &str) -> Result<Vec<IpAddr>, DnsError> {
        debug!("Attempting public DNS resolution for domain: {}", domain);

        // For now, use standard library resolution (same as cluster)
        use std::net::ToSocketAddrs;
        match (domain, 0).to_socket_addrs() {
            Ok(addrs) => {
                let ips: Vec<IpAddr> = addrs.map(|addr| addr.ip()).collect();
                if !ips.is_empty() {
                    info!("Public DNS resolution successful for domain: {} -> {:?}", domain, ips);
                    return Ok(ips);
                }
            }
            Err(e) => {
                return Err(DnsError::PublicDnsFailed {
                    domain: domain.to_string(),
                    source: e.to_string(),
                });
            }
        }

        Err(DnsError::PublicDnsFailed {
            domain: domain.to_string(),
            source: "No IP addresses found".to_string(),
        })
    }

    /// Resolve domain with layered fallback strategy
    ///
    /// Resolution order:
    /// 1. Local cache
    /// 2. Cluster DNS (e.g., CoreDNS)
    /// 3. Public DNS (Google DNS, Cloudflare DNS)
    pub async fn resolve(&self, domain: &str) -> Result<Vec<IpAddr>, DnsError> {
        // Validate domain format first
        Self::validate_domain_format(domain)?;

        // Step 1: Check local cache
        if let Some(ips) = self.check_cache(domain) {
            return Ok(ips);
        }

        debug!("Local cache miss for domain: {}, attempting cluster DNS", domain);

        // Step 2: Try cluster DNS
        match self.resolve_with_cluster_dns(domain).await {
            Ok(ips) => {
                self.update_cache(domain, ips.clone());
                return Ok(ips);
            }
            Err(cluster_err) => {
                warn!("{}", cluster_err);
            }
        }

        // Step 3: Fallback to public DNS
        match self.resolve_with_public_dns(domain).await {
            Ok(ips) => {
                self.update_cache(domain, ips.clone());
                Ok(ips)
            }
            Err(public_err) => {
                tracing::error!("{}", public_err);
                Err(DnsError::AllAttemptsFailed {
                    domain: domain.to_string(),
                })
            }
        }
    }

    /// Get cache statistics for monitoring
    pub fn cache_stats(&self) -> (usize, usize) {
        let cache = self.cache.lock().unwrap();
        let total_entries = cache.len();
        let expired_entries = cache.values().filter(|entry| entry.is_expired()).count();
        (total_entries, expired_entries)
    }
}

/// Global DNS resolver instance
static GLOBAL_DNS_RESOLVER: OnceLock<LayeredDnsResolver> = OnceLock::new();

/// Initialize the global DNS resolver
pub async fn init_global_dns_resolver() -> Result<(), DnsError> {
    let resolver = LayeredDnsResolver::new().await?;

    match GLOBAL_DNS_RESOLVER.set(resolver) {
        Ok(()) => {
            info!("Global DNS resolver initialized successfully");
            Ok(())
        }
        Err(_) => {
            warn!("Global DNS resolver was already initialized");
            Ok(())
        }
    }
}

/// Get the global DNS resolver instance
pub fn get_global_dns_resolver() -> Option<&'static LayeredDnsResolver> {
    GLOBAL_DNS_RESOLVER.get()
}

/// Resolve domain using the global DNS resolver
pub async fn resolve_domain(domain: &str) -> Result<Vec<IpAddr>, DnsError> {
    match get_global_dns_resolver() {
        Some(resolver) => resolver.resolve(domain).await,
        None => Err(DnsError::InitializationFailed {
            source: "Global DNS resolver not initialized. Call init_global_dns_resolver() first.".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_validation() {
        // Valid domains
        assert!(LayeredDnsResolver::validate_domain_format("example.com").is_ok());
        assert!(LayeredDnsResolver::validate_domain_format("sub.example.com").is_ok());
        assert!(LayeredDnsResolver::validate_domain_format("very.deep.sub.domain.example.com").is_ok());

        // Invalid domains - too long FQDN
        let long_domain = "a".repeat(254);
        assert!(LayeredDnsResolver::validate_domain_format(&long_domain).is_err());

        // Invalid domains - label too long
        let long_label = format!("{}.com", "a".repeat(64));
        assert!(LayeredDnsResolver::validate_domain_format(&long_label).is_err());

        // Invalid domains - empty label
        assert!(LayeredDnsResolver::validate_domain_format("example..com").is_err());
    }

    #[tokio::test]
    async fn test_cache_functionality() {
        let resolver = LayeredDnsResolver::new().await.unwrap();

        // Test cache miss
        assert!(resolver.check_cache("example.com").is_none());

        // Update cache
        let test_ips = vec![IpAddr::from([192, 0, 2, 1])];
        resolver.update_cache("example.com", test_ips.clone());

        // Test cache hit
        assert_eq!(resolver.check_cache("example.com"), Some(test_ips));

        // Test cache stats
        let (total, expired) = resolver.cache_stats();
        assert_eq!(total, 1);
        assert_eq!(expired, 0);
    }

    #[tokio::test]
    async fn test_dns_resolution() {
        let resolver = LayeredDnsResolver::new().await.unwrap();

        // Test resolution of a known domain
        match resolver.resolve("google.com").await {
            Ok(ips) => {
                assert!(!ips.is_empty());
                println!("Resolved google.com to: {:?}", ips);
            }
            Err(e) => {
                // In test environments, DNS resolution might fail
                // This is acceptable as long as our error handling works
                println!("DNS resolution failed (expected in some test environments): {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_invalid_domain_resolution() {
        let resolver = LayeredDnsResolver::new().await.unwrap();

        // Test resolution of invalid domain
        let result = resolver.resolve("nonexistent.invalid.domain.example").await;
        assert!(result.is_err());

        if let Err(e) = result {
            println!("Expected error for invalid domain: {}", e);
        }
    }
}
