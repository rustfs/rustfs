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

//! Environment variable definitions for trusted agent configurations
//!
//! All configuration items are read by environment variables and support the following priorities:
//! 1. Environment Variables (Highest Priority)
//! 2. The default value set in the code
//! 3. Hard-coded values in the default implementation of the struct

use crate::TrustedProxy;
use crate::cloud::fetch_cloud_provider_ips_sync;
use ipnetwork::IpNetwork;
use std::str::FromStr;
use tracing::{debug, info, warn};
// Environment variable key constant definition
// Format: RUSTFS_HTTP_{SECTION}_{KEY}, all caps, separated by underscores

// ==================== Agent configuration ====================
/// Agent verification mode
pub const ENV_PROXY_VALIDATION_MODE: &str = "RUSTFS_HTTP_PROXY_VALIDATION_MODE";
pub const DEFAULT_PROXY_VALIDATION_MODE: &str = "hop_by_hop";

/// Whether to enable RFC 7239 Forwarded headers
pub const ENV_PROXY_ENABLE_RFC7239: &str = "RUSTFS_HTTP_PROXY_ENABLE_RFC7239";
pub const DEFAULT_PROXY_ENABLE_RFC7239: bool = true;

/// Maximum number of proxy hops
pub const ENV_PROXY_MAX_PROXY_HOPS: &str = "RUSTFS_HTTP_PROXY_MAX_PROXY_HOPS";
pub const DEFAULT_PROXY_MAX_PROXY_HOPS: usize = 10;

/// whether chain continuity checking is enabled
pub const ENV_PROXY_ENABLE_CHAIN_CONTINUITY_CHECK: &str = "RUSTFS_HTTP_PROXY_ENABLE_CHAIN_CONTINUITY_CHECK";
pub const DEFAULT_PROXY_ENABLE_CHAIN_CONTINUITY_CHECK: bool = true;

// ==================== Trusted agent configuration ====================
/// Underlying Trusted Agent List (Comma-Separated IP/CIDR)
pub const ENV_TRUSTED_PROXIES: &str = "RUSTFS_HTTP_TRUSTED_PROXIES";
pub const DEFAULT_TRUSTED_PROXIES: &str = "127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fd00::/8";

/// Additional Trusted Agent List (production only, can be overridden)
pub const ENV_ADDITIONAL_TRUSTED_PROXIES: &str = "RUSTFS_HTTP_ADDITIONAL_TRUSTED_PROXIES";
pub const DEFAULT_ADDITIONAL_TRUSTED_PROXIES: &str = "";

/// Allowed private networks (for internal proxy authentication)
pub const ENV_ALLOWED_PRIVATE_NETS: &str = "RUSTFS_HTTP_ALLOWED_PRIVATE_NETS";
pub const DEFAULT_ALLOWED_PRIVATE_NETS: &str = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fd00::/8";

// ==================== Cache configuration ====================
/// Cache capacity
pub const ENV_CACHE_CAPACITY: &str = "RUSTFS_HTTP_CACHE_CAPACITY";
pub const DEFAULT_CACHE_CAPACITY: usize = 10000;

/// Cache TTL (seconds)
pub const ENV_CACHE_TTL_SECONDS: &str = "RUSTFS_HTTP_CACHE_TTL_SECONDS";
pub const DEFAULT_CACHE_TTL_SECONDS: u64 = 300;

/// Cache Cleanup Interval (Seconds)
pub const ENV_CACHE_CLEANUP_INTERVAL_SECONDS: &str = "RUSTFS_HTTP_CACHE_CLEANUP_INTERVAL_SECONDS";
pub const DEFAULT_CACHE_CLEANUP_INTERVAL_SECONDS: u64 = 60;

// ==================== Monitor configuration ====================
/// Whether monitoring metrics are enabled
pub const ENV_MONITORING_ENABLE_METRICS: &str = "RUSTFS_HTTP_MONITORING_ENABLE_METRICS";
pub const DEFAULT_MONITORING_ENABLE_METRICS: bool = true;

/// Log level
pub const ENV_MONITORING_LOG_LEVEL: &str = "RUSTFS_HTTP_MONITORING_LOG_LEVEL";
pub const DEFAULT_MONITORING_LOG_LEVEL: &str = "info";

/// Whether to log validation failures
pub const ENV_MONITORING_LOG_FAILED_VALIDATIONS: &str = "RUSTFS_HTTP_MONITORING_LOG_FAILED_VALIDATIONS";
pub const DEFAULT_MONITORING_LOG_FAILED_VALIDATIONS: bool = true;

/// Cloud service provider-specific IP ranges (Cloudflare, etc.)
pub const ENV_CLOUDFLARE_IPS_ENABLED: &str = "RUSTFS_HTTP_CLOUDFLARE_IPS_ENABLED";
pub const DEFAULT_CLOUDFLARE_IPS_ENABLED: bool = false;

/// Cloud metadata configuration
pub const ENV_CLOUD_METADATA_ENABLED: &str = "RUSTFS_HTTP_CLOUD_METADATA_ENABLED";
pub const DEFAULT_CLOUD_METADATA_ENABLED: bool = true;

pub const ENV_CLOUD_METADATA_TIMEOUT_SECS: &str = "RUSTFS_HTTP_CLOUD_METADATA_TIMEOUT_SECS";
pub const DEFAULT_CLOUD_METADATA_TIMEOUT_SECS: u64 = 5;

pub const ENV_CLOUD_PROVIDER_FORCE: &str = "RUSTFS_HTTP_CLOUD_PROVIDER_FORCE";
pub const DEFAULT_CLOUD_PROVIDER_FORCE: &str = ""; // Null means automatic detection

/// Environment variables resolve error types
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Environment variable resolution failed: {0}")]
    EnvParseError(String),

    #[error("IP/CIDR Format Error: {0}")]
    IpFormatError(String),

    #[error("Boolean parsing failed: {0}")]
    BoolParseError(String),

    #[error("Numeric parsing failed: {0}")]
    NumberParseError(String),

    #[error("Enum value parsing failed: {0}")]
    EnumParseError(String),
}

/// Environment variables configure loaders
pub struct EnvConfigLoader;

impl EnvConfigLoader {
    /// Get the string value from the environment variable
    pub fn get_string(key: &str, default: &str) -> String {
        std::env::var(key).unwrap_or_else(|_| default.to_string())
    }

    /// Get Boolean values from environment variables
    pub fn get_bool(key: &str, default: bool) -> Result<bool, ConfigError> {
        let value = Self::get_string(key, if default { "true" } else { "false" });
        value
            .parse()
            .map_err(|_| ConfigError::BoolParseError(format!("{}={}", key, value)))
    }

    /// Get an integer value from an environment variable
    pub fn get_usize(key: &str, default: usize) -> Result<usize, ConfigError> {
        let value = Self::get_string(key, &default.to_string());
        value
            .parse()
            .map_err(|e| ConfigError::NumberParseError(format!("{}={}: {}", key, value, e)))
    }

    /// Get the u64 value from the environment variable
    pub fn get_u64(key: &str, default: u64) -> Result<u64, ConfigError> {
        let value = Self::get_string(key, &default.to_string());
        value
            .parse()
            .map_err(|e| ConfigError::NumberParseError(format!("{}={}: {}", key, value, e)))
    }

    /// Parsing comma-separated IP/CIDR lists from environment variables
    pub fn parse_ip_list(key: &str, default: &str) -> Result<Vec<TrustedProxy>, ConfigError> {
        let value = Self::get_string(key, default);
        if value.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut proxies = Vec::new();
        for item in value.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }

            // Attempt to resolve to CIDR
            if item.contains('/') {
                match IpNetwork::from_str(item) {
                    Ok(network) => proxies.push(TrustedProxy::Cidr(network)),
                    Err(e) => return Err(ConfigError::IpFormatError(format!("{}: {}: {}", key, item, e))),
                }
            } else {
                // Attempt to resolve to a single IP
                match item.parse() {
                    Ok(ip) => proxies.push(TrustedProxy::Single(ip)),
                    Err(e) => return Err(ConfigError::IpFormatError(format!("{}: {}: {}", key, item, e))),
                }
            }
        }

        Ok(proxies)
    }

    /// Parses comma-separated CIDR lists from environment variables
    pub fn parse_cidr_list(key: &str, default: &str) -> Result<Vec<IpNetwork>, ConfigError> {
        let value = Self::get_string(key, default);
        if value.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut networks = Vec::new();
        for item in value.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }

            match IpNetwork::from_str(item) {
                Ok(network) => networks.push(network),
                Err(e) => return Err(ConfigError::IpFormatError(format!("{}: {}: {}", key, item, e))),
            }
        }

        Ok(networks)
    }

    /// Get the validation schema enumeration value
    pub fn get_validation_mode(key: &str, default: &str) -> Result<crate::advanced::ValidationMode, ConfigError> {
        let value = Self::get_string(key, default);
        match value.to_lowercase().as_str() {
            "lenient" => Ok(crate::advanced::ValidationMode::Lenient),
            "strict" => Ok(crate::advanced::ValidationMode::Strict),
            "hop_by_hop" => Ok(crate::advanced::ValidationMode::HopByHop),
            _ => Err(ConfigError::EnumParseError(format!(
                "{}: Must be 'lenient', 'strict' or 'hop_by_hop'",
                value
            ))),
        }
    }

    /// Get the log level
    pub fn get_log_level(key: &str, default: &str) -> String {
        let value = Self::get_string(key, default).to_lowercase();
        match value.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => value,
            _ => default.to_string(),
        }
    }

    /// Get the cloud metadata IP range
    pub fn fetch_cloud_metadata_ips() -> Result<Vec<String>, ConfigError> {
        // Check if cloud metadata is enabled
        let enabled = Self::get_bool(ENV_CLOUD_METADATA_ENABLED, DEFAULT_CLOUD_METADATA_ENABLED)?;

        if !enabled {
            debug!("Cloud metadata fetching is disabled");
            return Ok(Vec::new());
        }

        let timeout_secs = Self::get_u64(ENV_CLOUD_METADATA_TIMEOUT_SECS, DEFAULT_CLOUD_METADATA_TIMEOUT_SECS)?;

        // If there is a mandatory designation of a cloud service provider, set the environment variable
        let forced_provider = Self::get_string(ENV_CLOUD_PROVIDER_FORCE, DEFAULT_CLOUD_PROVIDER_FORCE);
        if !forced_provider.is_empty() {
            // std::env::set_var("CLOUD_PROVIDER_FORCE", forced_provider);
        }

        match fetch_cloud_provider_ips_sync(timeout_secs) {
            Ok(ips) => {
                info!("{} IP ranges were obtained from cloud metadata", ips.len());
                if !ips.is_empty() {
                    debug!("Cloud IP range: {:?}", ips);
                }
                Ok(ips)
            }
            Err(e) => {
                warn!("Cloud metadata fetch failed: {}", e);
                Ok(Vec::new())
            }
        }
    }
}

/// Cloud service provider IP range configuration
pub struct CloudProviderIps {
    /// Cloudflare IP range
    pub cloudflare: Vec<IpNetwork>,
}

impl CloudProviderIps {
    /// Get the Cloudflare IP range
    pub fn cloudflare_ranges() -> Vec<IpNetwork> {
        let ranges = vec![
            "103.21.244.0/22",
            "103.22.200.0/22",
            "103.31.4.0/22",
            "104.16.0.0/13",
            "104.24.0.0/14",
            "108.162.192.0/18",
            "131.0.72.0/22",
            "141.101.64.0/18",
            "162.158.0.0/15",
            "172.64.0.0/13",
            "173.245.48.0/20",
            "188.114.96.0/20",
            "190.93.240.0/20",
            "197.234.240.0/22",
            "198.41.128.0/17",
        ];

        ranges.into_iter().filter_map(|s| IpNetwork::from_str(s).ok()).collect()
    }
}
