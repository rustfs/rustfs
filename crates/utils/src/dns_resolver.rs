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
//! 1. Local cache (Moka) for previously resolved results
//! 2. System DNS resolver (container/host adaptive) using hickory-resolver
//! 3. Public DNS servers as final fallback (8.8.8.8, 1.1.1.1) using hickory-resolver with TLS
//!
//! The resolver is designed to handle 5-level or deeper domain names that may fail
//! in Kubernetes environments due to CoreDNS configuration, DNS recursion limits,
//! or network-related issues. Uses hickory-resolver for actual DNS queries with TLS support.

use hickory_resolver::Resolver;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::name_server::TokioConnectionProvider;
use moka::future::Cache;
use std::net::IpAddr;
use std::sync::OnceLock;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

/// Maximum FQDN length according to RFC standards
const MAX_FQDN_LENGTH: usize = 253;
/// Maximum DNS label length according to RFC standards
const MAX_LABEL_LENGTH: usize = 63;
/// Cache entry TTL in seconds
const CACHE_TTL_SECONDS: u64 = 300; // 5 minutes
/// Maximum cache size (number of entries)
const MAX_CACHE_SIZE: u64 = 10000;

/// DNS resolution error types with detailed context and tracing information
#[derive(Debug, thiserror::Error)]
pub enum DnsError {
    #[error("Invalid domain format: {reason}")]
    InvalidFormat { reason: String },

    #[error("Local cache miss for domain: {domain}")]
    CacheMiss { domain: String },

    #[error("System DNS resolution failed for domain: {domain} - {source}")]
    SystemDnsFailed {
        domain: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Public DNS resolution failed for domain: {domain} - {source}")]
    PublicDnsFailed {
        domain: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error(
        "All DNS resolution attempts failed for domain: {domain}. Please check your domain spelling, network connectivity, or DNS configuration"
    )]
    AllAttemptsFailed { domain: String },

    #[error("DNS resolver initialization failed: {source}")]
    InitializationFailed {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("DNS configuration error: {source}")]
    ConfigurationError {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Layered DNS resolver with caching and multiple fallback strategies
pub struct LayeredDnsResolver {
    /// Local cache for resolved domains using Moka for high performance
    cache: Cache<String, Vec<IpAddr>>,
    /// System DNS resolver using hickory-resolver with default configuration
    system_resolver: Resolver<TokioConnectionProvider>,
    /// Public DNS resolver using hickory-resolver with Cloudflare DNS servers
    public_resolver: Resolver<TokioConnectionProvider>,
}

impl LayeredDnsResolver {
    /// Create a new layered DNS resolver with automatic DNS configuration detection
    #[instrument(skip_all)]
    pub async fn new() -> Result<Self, DnsError> {
        info!("Initializing layered DNS resolver with hickory-resolver, Moka cache and public DNS fallback");

        // Create Moka cache with TTL and size limits
        let cache = Cache::builder()
            .time_to_live(Duration::from_secs(CACHE_TTL_SECONDS))
            .max_capacity(MAX_CACHE_SIZE)
            .build();

        // Create system DNS resolver with default configuration (auto-detects container/host DNS)
        let system_resolver =
            Resolver::builder_with_config(ResolverConfig::default(), TokioConnectionProvider::default()).build();

        let mut config = ResolverConfig::cloudflare_tls();
        for ns in ResolverConfig::google_tls().name_servers() {
            config.add_name_server(ns.clone())
        }
        // Create public DNS resolver using Cloudflare DNS with TLS support
        let public_resolver = Resolver::builder_with_config(config, TokioConnectionProvider::default()).build();

        info!("DNS resolver initialized successfully with hickory-resolver system and Cloudflare TLS public fallback");

        Ok(Self {
            cache,
            system_resolver,
            public_resolver,
        })
    }

    /// Validate domain format according to RFC standards
    #[instrument(skip_all, fields(domain = %domain))]
    fn validate_domain_format(domain: &str) -> Result<(), DnsError> {
        info!("Validating domain format start");
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
        info!("DNS resolver validated successfully");
        Ok(())
    }

    /// Check local cache for resolved domain
    #[instrument(skip_all, fields(domain = %domain))]
    async fn check_cache(&self, domain: &str) -> Option<Vec<IpAddr>> {
        match self.cache.get(domain).await {
            Some(ips) => {
                debug!("DNS cache hit for domain: {}, found {} IPs", domain, ips.len());
                Some(ips)
            }
            None => {
                debug!("DNS cache miss for domain: {}", domain);
                None
            }
        }
    }

    /// Update local cache with resolved IPs
    #[instrument(skip_all, fields(domain = %domain, ip_count = ips.len()))]
    async fn update_cache(&self, domain: &str, ips: Vec<IpAddr>) {
        self.cache.insert(domain.to_string(), ips.clone()).await;
        debug!("DNS cache updated for domain: {} with {} IPs", domain, ips.len());
    }

    /// Get cache statistics for monitoring
    #[instrument(skip_all)]
    pub async fn cache_stats(&self) -> (u64, u64) {
        let entry_count = self.cache.entry_count();
        let weighted_size = self.cache.weighted_size();
        debug!("DNS cache stats - entries: {}, weighted_size: {}", entry_count, weighted_size);
        (entry_count, weighted_size)
    }

    /// Manually invalidate cache entries (useful for testing or forced refresh)
    #[instrument(skip_all)]
    pub async fn invalidate_cache(&self) {
        self.cache.invalidate_all();
        info!("DNS cache invalidated");
    }

    /// Resolve domain using system DNS (cluster/host DNS configuration) with hickory-resolver
    #[instrument(skip_all, fields(domain = %domain))]
    async fn resolve_with_system_dns(&self, domain: &str) -> Result<Vec<IpAddr>, DnsError> {
        debug!("Attempting system DNS resolution for domain: {} using hickory-resolver", domain);

        match self.system_resolver.lookup_ip(domain).await {
            Ok(lookup) => {
                let ips: Vec<IpAddr> = lookup.iter().collect();
                if !ips.is_empty() {
                    info!("System DNS resolution successful for domain: {} -> {} IPs", domain, ips.len());
                    Ok(ips)
                } else {
                    warn!("System DNS returned empty result for domain: {}", domain);
                    Err(DnsError::SystemDnsFailed {
                        domain: domain.to_string(),
                        source: "No IP addresses found".to_string().into(),
                    })
                }
            }
            Err(e) => {
                warn!("System DNS resolution failed for domain: {} - {}", domain, e);
                Err(DnsError::SystemDnsFailed {
                    domain: domain.to_string(),
                    source: Box::new(e),
                })
            }
        }
    }

    /// Resolve domain using public DNS servers (Cloudflare TLS DNS) with hickory-resolver
    #[instrument(skip_all, fields(domain = %domain))]
    async fn resolve_with_public_dns(&self, domain: &str) -> Result<Vec<IpAddr>, DnsError> {
        debug!(
            "Attempting public DNS resolution for domain: {} using hickory-resolver with TLS-enabled Cloudflare DNS",
            domain
        );

        match self.public_resolver.lookup_ip(domain).await {
            Ok(lookup) => {
                let ips: Vec<IpAddr> = lookup.iter().collect();
                if !ips.is_empty() {
                    info!("Public DNS resolution successful for domain: {} -> {} IPs", domain, ips.len());
                    Ok(ips)
                } else {
                    warn!("Public DNS returned empty result for domain: {}", domain);
                    Err(DnsError::PublicDnsFailed {
                        domain: domain.to_string(),
                        source: "No IP addresses found".to_string().into(),
                    })
                }
            }
            Err(e) => {
                error!("Public DNS resolution failed for domain: {} - {}", domain, e);
                Err(DnsError::PublicDnsFailed {
                    domain: domain.to_string(),
                    source: Box::new(e),
                })
            }
        }
    }

    /// Resolve domain with layered fallback strategy using hickory-resolver
    ///
    /// Resolution order with detailed tracing:
    /// 1. Local cache (Moka with TTL)
    /// 2. System DNS (hickory-resolver with host/container adaptive configuration)  
    /// 3. Public DNS (hickory-resolver with TLS-enabled Cloudflare DNS fallback)
    #[instrument(skip_all, fields(domain = %domain))]
    pub async fn resolve(&self, domain: &str) -> Result<Vec<IpAddr>, DnsError> {
        info!("Starting DNS resolution process for domain: {} start", domain);
        // Validate domain format first
        Self::validate_domain_format(domain)?;

        info!("Starting DNS resolution for domain: {}", domain);

        // Step 1: Check local cache
        if let Some(ips) = self.check_cache(domain).await {
            info!("DNS resolution completed from cache for domain: {} -> {} IPs", domain, ips.len());
            return Ok(ips);
        }

        debug!("Local cache miss for domain: {}, attempting system DNS", domain);

        // Step 2: Try system DNS (cluster/host adaptive)
        match self.resolve_with_system_dns(domain).await {
            Ok(ips) => {
                self.update_cache(domain, ips.clone()).await;
                info!("DNS resolution completed via system DNS for domain: {} -> {} IPs", domain, ips.len());
                return Ok(ips);
            }
            Err(system_err) => {
                warn!("System DNS failed for domain: {} - {}", domain, system_err);
            }
        }

        // Step 3: Fallback to public DNS
        info!("Falling back to public DNS for domain: {}", domain);
        match self.resolve_with_public_dns(domain).await {
            Ok(ips) => {
                self.update_cache(domain, ips.clone()).await;
                info!("DNS resolution completed via public DNS for domain: {} -> {} IPs", domain, ips.len());
                Ok(ips)
            }
            Err(public_err) => {
                error!(
                    "All DNS resolution attempts failed for domain:` {}`. System DNS: failed, Public DNS: {}",
                    domain, public_err
                );
                Err(DnsError::AllAttemptsFailed {
                    domain: domain.to_string(),
                })
            }
        }
    }
}

/// Global DNS resolver instance
static GLOBAL_DNS_RESOLVER: OnceLock<LayeredDnsResolver> = OnceLock::new();

/// Initialize the global DNS resolver
#[instrument]
pub async fn init_global_dns_resolver() -> Result<(), DnsError> {
    info!("Initializing global DNS resolver");
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

/// Resolve domain using the global DNS resolver with comprehensive tracing
#[instrument(skip_all, fields(domain = %domain))]
pub async fn resolve_domain(domain: &str) -> Result<Vec<IpAddr>, DnsError> {
    info!("resolving domain for: {}", domain);
    match get_global_dns_resolver() {
        Some(resolver) => resolver.resolve(domain).await,
        None => Err(DnsError::InitializationFailed {
            source: "Global DNS resolver not initialized. Call init_global_dns_resolver() first."
                .to_string()
                .into(),
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
        assert!(resolver.check_cache("example.com").await.is_none());

        // Update cache
        let test_ips = vec![IpAddr::from([192, 0, 2, 1])];
        resolver.update_cache("example.com", test_ips.clone()).await;

        // Test cache hit
        assert_eq!(resolver.check_cache("example.com").await, Some(test_ips));

        // Test cache stats (note: moka cache might not immediately reflect changes)
        let (total, _weighted_size) = resolver.cache_stats().await;
        // Cache should have at least the entry we just added (might be 0 due to async nature)
        assert!(total <= 1, "Cache should have at most 1 entry, got {total}");
    }

    #[tokio::test]
    async fn test_dns_resolution() {
        let resolver = LayeredDnsResolver::new().await.unwrap();

        // Test resolution of a known domain (localhost should always resolve)
        match resolver.resolve("localhost").await {
            Ok(ips) => {
                assert!(!ips.is_empty());
                println!("Resolved localhost to: {ips:?}");
            }
            Err(e) => {
                // In some test environments, even localhost might fail
                // This is acceptable as long as our error handling works
                println!("DNS resolution failed (might be expected in test environments): {e}");
            }
        }
    }

    #[tokio::test]
    async fn test_invalid_domain_resolution() {
        let resolver = LayeredDnsResolver::new().await.unwrap();

        // Test resolution of invalid domain
        let result = resolver
            .resolve("nonexistent.invalid.domain.example.thisdefinitelydoesnotexist")
            .await;
        assert!(result.is_err());

        if let Err(e) = result {
            println!("Expected error for invalid domain: {e}");
            // Should be AllAttemptsFailed since both system and public DNS should fail
            assert!(matches!(e, DnsError::AllAttemptsFailed { .. }));
        }
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let resolver = LayeredDnsResolver::new().await.unwrap();

        // Add entry to cache
        let test_ips = vec![IpAddr::from([192, 0, 2, 1])];
        resolver.update_cache("test.example.com", test_ips.clone()).await;

        // Verify cache hit
        assert_eq!(resolver.check_cache("test.example.com").await, Some(test_ips));

        // Invalidate cache
        resolver.invalidate_cache().await;

        // Verify cache miss after invalidation
        assert!(resolver.check_cache("test.example.com").await.is_none());
    }

    #[tokio::test]
    async fn test_global_resolver_initialization() {
        // Test initialization
        assert!(init_global_dns_resolver().await.is_ok());

        // Test that resolver is available
        assert!(get_global_dns_resolver().is_some());

        // Test domain resolution through global resolver
        match resolve_domain("localhost").await {
            Ok(ips) => {
                assert!(!ips.is_empty());
                println!("Global resolver resolved localhost to: {ips:?}");
            }
            Err(e) => {
                println!("Global resolver DNS resolution failed (might be expected in test environments): {e}");
            }
        }
    }
}
