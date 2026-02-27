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
        let enable = std::env::var("RUSTFS_KEYSTONE_ENABLE")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .unwrap_or(false);

        if !enable {
            return Ok(Self::default());
        }

        let auth_url = std::env::var("RUSTFS_KEYSTONE_AUTH_URL")
            .map_err(|_| KeystoneError::ConfigError("RUSTFS_KEYSTONE_AUTH_URL not set".to_string()))?;

        let version = std::env::var("RUSTFS_KEYSTONE_VERSION").unwrap_or_else(|_| "v3".to_string());

        let admin_user = std::env::var("RUSTFS_KEYSTONE_ADMIN_USER").ok();
        let admin_password = std::env::var("RUSTFS_KEYSTONE_ADMIN_PASSWORD").ok();
        let admin_project = std::env::var("RUSTFS_KEYSTONE_ADMIN_PROJECT").ok();
        let admin_domain = std::env::var("RUSTFS_KEYSTONE_ADMIN_DOMAIN").ok();

        let verify_ssl = std::env::var("RUSTFS_KEYSTONE_VERIFY_SSL")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);

        let enable_cache = std::env::var("RUSTFS_KEYSTONE_ENABLE_CACHE")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);

        let cache_size = std::env::var("RUSTFS_KEYSTONE_CACHE_SIZE")
            .unwrap_or_else(|_| "10000".to_string())
            .parse()
            .unwrap_or(10000);

        let cache_ttl_seconds = std::env::var("RUSTFS_KEYSTONE_CACHE_TTL")
            .unwrap_or_else(|_| "300".to_string())
            .parse()
            .unwrap_or(300);

        let enable_tenant_prefix = std::env::var("RUSTFS_KEYSTONE_TENANT_PREFIX")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);

        let implicit_tenants = std::env::var("RUSTFS_KEYSTONE_IMPLICIT_TENANTS")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);

        let timeout_seconds = std::env::var("RUSTFS_KEYSTONE_TIMEOUT")
            .unwrap_or_else(|_| "30".to_string())
            .parse()
            .unwrap_or(30);

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
}
