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

use crate::DEFAULT_LOG_LEVEL;

// ==================== Base Proxy Configuration ====================
/// Environment variable to enable the trusted proxy middleware.
pub const ENV_TRUSTED_PROXY_ENABLED: &str = "RUSTFS_TRUSTED_PROXY_ENABLED";
/// Trusted proxy middleware is enabled by default.
pub const DEFAULT_TRUSTED_PROXY_ENABLED: bool = true;

/// Environment variable for the proxy validation mode.
pub const ENV_TRUSTED_PROXY_VALIDATION_MODE: &str = "RUSTFS_TRUSTED_PROXY_VALIDATION_MODE";
/// Default validation mode is "hop_by_hop".
pub const DEFAULT_TRUSTED_PROXY_VALIDATION_MODE: &str = "hop_by_hop";

/// Environment variable to enable RFC 7239 "Forwarded" header support.
pub const ENV_TRUSTED_PROXY_ENABLE_RFC7239: &str = "RUSTFS_TRUSTED_PROXY_ENABLE_RFC7239";
/// RFC 7239 support is enabled by default.
pub const DEFAULT_TRUSTED_PROXY_ENABLE_RFC7239: bool = true;

/// Environment variable for the maximum allowed proxy hops.
pub const ENV_TRUSTED_PROXY_MAX_HOPS: &str = "RUSTFS_TRUSTED_PROXY_MAX_HOPS";
/// Default maximum hops is 10.
pub const DEFAULT_TRUSTED_PROXY_MAX_HOPS: usize = 10;

/// Environment variable to enable proxy chain continuity checks.
pub const ENV_TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK: &str = "RUSTFS_TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK";
/// Continuity checks are enabled by default.
pub const DEFAULT_TRUSTED_PROXY_CHAIN_CONTINUITY_CHECK: bool = true;

/// Environment variable to enable logging of failed proxy validations.
pub const ENV_TRUSTED_PROXY_LOG_FAILED_VALIDATIONS: &str = "RUSTFS_TRUSTED_PROXY_LOG_FAILED_VALIDATIONS";
/// Logging of failed validations is enabled by default.
pub const DEFAULT_TRUSTED_PROXY_LOG_FAILED_VALIDATIONS: bool = true;

// ==================== Trusted Proxy Networks ====================
/// Environment variable for the list of trusted proxy networks (comma-separated IP/CIDR).
pub const ENV_TRUSTED_PROXY_PROXIES: &str = "RUSTFS_TRUSTED_PROXY_NETWORKS";
/// Default trusted networks include localhost and common private ranges.
pub const DEFAULT_TRUSTED_PROXY_PROXIES: &str = "127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fd00::/8";

/// Environment variable for additional trusted proxy networks (production specific).
pub const ENV_TRUSTED_PROXY_EXTRA_PROXIES: &str = "RUSTFS_TRUSTED_PROXY_EXTRA_NETWORKS";
/// No extra trusted networks by default.
pub const DEFAULT_TRUSTED_PROXY_EXTRA_PROXIES: &str = "";

/// Environment variable for individual trusted proxy IPs.
pub const ENV_TRUSTED_PROXY_IPS: &str = "RUSTFS_TRUSTED_PROXY_IPS";
/// No individual trusted IPs by default.
pub const DEFAULT_TRUSTED_PROXY_IPS: &str = "";

/// Environment variable for private network ranges used in internal validation.
pub const ENV_TRUSTED_PROXY_PRIVATE_NETWORKS: &str = "RUSTFS_TRUSTED_PROXY_PRIVATE_NETWORKS";
/// Default private networks include common RFC 1918 and RFC 4193 ranges.
pub const DEFAULT_TRUSTED_PROXY_PRIVATE_NETWORKS: &str = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fd00::/8";

// ==================== Cache Configuration ====================
/// Environment variable for the proxy validation cache capacity.
pub const ENV_TRUSTED_PROXY_CACHE_CAPACITY: &str = "RUSTFS_TRUSTED_PROXY_CACHE_CAPACITY";
/// Default cache capacity is 10,000 entries.
pub const DEFAULT_TRUSTED_PROXY_CACHE_CAPACITY: usize = 10_000;

/// Environment variable for the cache entry time-to-live (TTL) in seconds.
pub const ENV_TRUSTED_PROXY_CACHE_TTL_SECONDS: &str = "RUSTFS_TRUSTED_PROXY_CACHE_TTL_SECONDS";
/// Default cache TTL is 300 seconds (5 minutes).
pub const DEFAULT_TRUSTED_PROXY_CACHE_TTL_SECONDS: u64 = 300;

/// Environment variable for the cache cleanup interval in seconds.
pub const ENV_TRUSTED_PROXY_CACHE_CLEANUP_INTERVAL: &str = "RUSTFS_TRUSTED_PROXY_CACHE_CLEANUP_INTERVAL";
/// Default cleanup interval is 60 seconds.
pub const DEFAULT_TRUSTED_PROXY_CACHE_CLEANUP_INTERVAL: u64 = 60;

// ==================== Monitoring Configuration ====================
/// Environment variable to enable Prometheus metrics.
pub const ENV_TRUSTED_PROXY_METRICS_ENABLED: &str = "RUSTFS_TRUSTED_PROXY_METRICS_ENABLED";
/// Metrics are enabled by default.
pub const DEFAULT_TRUSTED_PROXY_METRICS_ENABLED: bool = true;

/// Environment variable for the application log level.
pub const ENV_TRUSTED_PROXIES_LOG_LEVEL: &str = "RUSTFS_TRUSTED_PROXY_LOG_LEVEL";
/// Default log level is "info".
pub const DEFAULT_TRUSTED_PROXIES_LOG_LEVEL: &str = DEFAULT_LOG_LEVEL;

/// Environment variable to enable structured JSON logging.
pub const ENV_TRUSTED_PROXY_STRUCTURED_LOGGING: &str = "RUSTFS_TRUSTED_PROXY_STRUCTURED_LOGGING";
/// Structured logging is disabled by default.
pub const DEFAULT_TRUSTED_PROXY_STRUCTURED_LOGGING: bool = false;

/// Environment variable to enable distributed tracing.
pub const ENV_TRUSTED_PROXY_TRACING_ENABLED: &str = "RUSTFS_TRUSTED_PROXY_TRACING_ENABLED";
/// Tracing is enabled by default.
pub const DEFAULT_TRUSTED_PROXY_TRACING_ENABLED: bool = true;

// ==================== Cloud Integration ====================
/// Environment variable to enable automatic cloud metadata discovery.
pub const ENV_TRUSTED_PROXY_CLOUD_METADATA_ENABLED: &str = "RUSTFS_TRUSTED_PROXY_CLOUD_METADATA_ENABLED";
/// Cloud metadata discovery is disabled by default.
pub const DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_ENABLED: bool = false;

/// Environment variable for the cloud metadata request timeout in seconds.
pub const ENV_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT: &str = "RUSTFS_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT";
/// Default cloud metadata timeout is 5 seconds.
pub const DEFAULT_TRUSTED_PROXY_CLOUD_METADATA_TIMEOUT: u64 = 5;

/// Environment variable to enable Cloudflare IP range integration.
pub const ENV_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED: &str = "RUSTFS_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED";
/// Cloudflare integration is disabled by default.
pub const DEFAULT_TRUSTED_PROXY_CLOUDFLARE_IPS_ENABLED: bool = false;

/// Environment variable to force a specific cloud provider (overrides auto-detection).
pub const ENV_TRUSTED_PROXY_CLOUD_PROVIDER_FORCE: &str = "RUSTFS_TRUSTED_PROXY_CLOUD_PROVIDER_FORCE";
/// No forced provider by default.
pub const DEFAULT_TRUSTED_PROXY_CLOUD_PROVIDER_FORCE: &str = "";
