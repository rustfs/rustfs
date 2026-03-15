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

use crate::{KeystoneError, KeystoneVersion, Result};
use rustfs_utils::{get_env_bool, get_env_opt_str, get_env_str, get_env_u64};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Keystone integration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeystoneConfig {
    /// Enable Keystone authentication
    pub enable: bool,

    /// Keystone auth URL (e.g., http://keystone:5000)
    pub auth_url: String,

    /// Keystone API version ("v3" or "v2.0")
    pub version: String,

    /// Admin user for privileged operations
    pub admin_user: Option<String>,

    /// Admin password
    pub admin_password: Option<String>,

    /// Admin project/tenant
    pub admin_project: Option<String>,

    /// Admin domain (default: "Default")
    pub admin_domain: Option<String>,

    /// Verify SSL certificates
    pub verify_ssl: bool,

    /// Enable token caching
    pub enable_cache: bool,

    /// Token cache size (number of entries)
    pub cache_size: u64,

    /// Token cache TTL (seconds)
    pub cache_ttl_seconds: u64,

    /// Enable tenant/project prefixing for buckets
    /// When true, buckets are prefixed with project_id: "project_id:bucket_name"
    pub enable_tenant_prefix: bool,

    /// Enable implicit tenant creation
    /// When true, automatically create tenants on first access
    pub implicit_tenants: bool,

    /// Request timeout (seconds)
    pub timeout_seconds: u64,

    /// Role-to-policy mappings
    /// Maps Keystone roles to RustFS policy names
    pub role_mappings: Option<Vec<RoleMapping>>,
}

/// Role to policy mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleMapping {
    /// Keystone role name
    pub keystone_role: String,
    /// RustFS policy name
    pub rustfs_policy: String,
}

impl KeystoneConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let enable = get_env_bool("RUSTFS_KEYSTONE_ENABLE", false);

        if !enable {
            return Ok(Self::default());
        }

        let auth_url = get_env_opt_str("RUSTFS_KEYSTONE_AUTH_URL")
            .ok_or_else(|| KeystoneError::ConfigError("RUSTFS_KEYSTONE_AUTH_URL not set".to_string()))?;

        let version = get_env_str("RUSTFS_KEYSTONE_VERSION", "v3");

        let admin_user = get_env_opt_str("RUSTFS_KEYSTONE_ADMIN_USER");
        let admin_password = get_env_opt_str("RUSTFS_KEYSTONE_ADMIN_PASSWORD");
        let admin_project = get_env_opt_str("RUSTFS_KEYSTONE_ADMIN_PROJECT");
        let admin_domain = get_env_opt_str("RUSTFS_KEYSTONE_ADMIN_DOMAIN");

        let verify_ssl = get_env_bool("RUSTFS_KEYSTONE_VERIFY_SSL", true);

        let enable_cache = get_env_bool("RUSTFS_KEYSTONE_ENABLE_CACHE", true);

        let cache_size = get_env_u64("RUSTFS_KEYSTONE_CACHE_SIZE", 10000);

        let cache_ttl_seconds = get_env_u64("RUSTFS_KEYSTONE_CACHE_TTL", 300);

        let enable_tenant_prefix = get_env_bool("RUSTFS_KEYSTONE_TENANT_PREFIX", true);

        let implicit_tenants = get_env_bool("RUSTFS_KEYSTONE_IMPLICIT_TENANTS", true);

        let timeout_seconds = get_env_u64("RUSTFS_KEYSTONE_TIMEOUT", 30);

        Ok(Self {
            enable,
            auth_url,
            version,
            admin_user,
            admin_password,
            admin_project,
            admin_domain,
            verify_ssl,
            enable_cache,
            cache_size,
            cache_ttl_seconds,
            enable_tenant_prefix,
            implicit_tenants,
            timeout_seconds,
            role_mappings: None,
        })
    }

    /// Get Keystone API version
    pub fn get_version(&self) -> Result<KeystoneVersion> {
        match self.version.as_str() {
            "v3" | "3" => Ok(KeystoneVersion::V3),
            "v2.0" | "v2" | "2.0" | "2" => Ok(KeystoneVersion::V2_0),
            _ => Err(KeystoneError::ConfigError(format!("Invalid Keystone version: {}", self.version))),
        }
    }

    /// Get cache TTL duration
    pub fn get_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.cache_ttl_seconds)
    }

    /// Get request timeout duration
    pub fn get_timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_seconds)
    }

    /// Get admin domain (defaults to "Default")
    pub fn get_admin_domain(&self) -> String {
        self.admin_domain.clone().unwrap_or_else(|| "Default".to_string())
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if !self.enable {
            return Ok(());
        }

        if self.auth_url.is_empty() {
            return Err(KeystoneError::ConfigError("auth_url is required".to_string()));
        }

        // Validate version
        self.get_version()?;

        // Warn if admin credentials are missing (needed for some operations)
        if self.admin_user.is_none() || self.admin_password.is_none() {
            tracing::warn!("Keystone admin credentials not configured - some operations may fail");
        }

        Ok(())
    }
}

impl Default for KeystoneConfig {
    fn default() -> Self {
        Self {
            enable: false,
            auth_url: String::new(),
            version: "v3".to_string(),
            admin_user: None,
            admin_password: None,
            admin_project: None,
            admin_domain: None,
            verify_ssl: true,
            enable_cache: true,
            cache_size: 10000,
            cache_ttl_seconds: 300,
            enable_tenant_prefix: true,
            implicit_tenants: true,
            timeout_seconds: 30,
            role_mappings: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_env::with_vars;

    #[test]
    fn test_default_config() {
        let config = KeystoneConfig::default();
        assert!(!config.enable);
        assert_eq!(config.version, "v3");
        assert!(config.verify_ssl);
        assert!(config.enable_cache);
    }

    #[test]
    fn test_get_version() {
        let mut config = KeystoneConfig {
            version: "v3".to_string(),
            ..Default::default()
        };
        assert_eq!(config.get_version().unwrap(), KeystoneVersion::V3);

        config.version = "v2.0".to_string();
        assert_eq!(config.get_version().unwrap(), KeystoneVersion::V2_0);

        config.version = "invalid".to_string();
        assert!(config.get_version().is_err());
    }

    #[test]
    fn test_from_env_reads_configuration() {
        with_vars(
            vec![
                ("RUSTFS_KEYSTONE_ENABLE", Some("true")),
                ("RUSTFS_KEYSTONE_AUTH_URL", Some("https://keystone.example.com")),
                ("RUSTFS_KEYSTONE_VERSION", Some("v2.0")),
                ("RUSTFS_KEYSTONE_ADMIN_USER", Some("admin")),
                ("RUSTFS_KEYSTONE_ADMIN_PASSWORD", Some("secret")),
                ("RUSTFS_KEYSTONE_ADMIN_PROJECT", Some("service")),
                ("RUSTFS_KEYSTONE_ADMIN_DOMAIN", Some("Default")),
                ("RUSTFS_KEYSTONE_VERIFY_SSL", Some("false")),
                ("RUSTFS_KEYSTONE_ENABLE_CACHE", Some("false")),
                ("RUSTFS_KEYSTONE_CACHE_SIZE", Some("2048")),
                ("RUSTFS_KEYSTONE_CACHE_TTL", Some("900")),
                ("RUSTFS_KEYSTONE_TENANT_PREFIX", Some("false")),
                ("RUSTFS_KEYSTONE_IMPLICIT_TENANTS", Some("false")),
                ("RUSTFS_KEYSTONE_TIMEOUT", Some("99")),
            ],
            || {
                let config = KeystoneConfig::from_env().expect("keystone config should load from env");

                assert!(config.enable);
                assert_eq!(config.auth_url, "https://keystone.example.com");
                assert_eq!(config.version, "v2.0");
                assert_eq!(config.admin_user.as_deref(), Some("admin"));
                assert_eq!(config.admin_password.as_deref(), Some("secret"));
                assert_eq!(config.admin_project.as_deref(), Some("service"));
                assert_eq!(config.admin_domain.as_deref(), Some("Default"));
                assert!(!config.verify_ssl);
                assert!(!config.enable_cache);
                assert_eq!(config.cache_size, 2048);
                assert_eq!(config.cache_ttl_seconds, 900);
                assert!(!config.enable_tenant_prefix);
                assert!(!config.implicit_tenants);
                assert_eq!(config.timeout_seconds, 99);
            },
        );
    }
}
