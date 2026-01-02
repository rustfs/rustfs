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

//! KMS configuration management

use crate::error::{KmsError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

/// KMS backend types
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KmsBackend {
    /// Vault backend (recommended for production)
    Vault,
    /// Local file-based backend for development and testing only
    #[default]
    Local,
}

/// Main KMS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsConfig {
    /// Backend type
    pub backend: KmsBackend,
    /// Default master key ID for auto-encryption
    pub default_key_id: Option<String>,
    /// Backend-specific configuration
    pub backend_config: BackendConfig,
    /// Operation timeout
    pub timeout: Duration,
    /// Number of retry attempts
    pub retry_attempts: u32,
    /// Enable caching
    pub enable_cache: bool,
    /// Cache configuration
    pub cache_config: CacheConfig,
}

impl Default for KmsConfig {
    fn default() -> Self {
        Self {
            backend: KmsBackend::default(),
            default_key_id: None,
            backend_config: BackendConfig::default(),
            timeout: Duration::from_secs(30),
            retry_attempts: 3,
            enable_cache: true,
            cache_config: CacheConfig::default(),
        }
    }
}

/// Backend-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackendConfig {
    /// Local backend configuration
    Local(LocalConfig),
    /// Vault backend configuration
    Vault(VaultConfig),
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self::Local(LocalConfig::default())
    }
}

/// Local KMS backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    /// Directory to store key files
    pub key_dir: PathBuf,
    /// Master key for encrypting stored keys (if None, keys are stored in plaintext)
    pub master_key: Option<String>,
    /// File permissions for key files (octal)
    pub file_permissions: Option<u32>,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            key_dir: std::env::temp_dir().join("rustfs_kms_keys"),
            master_key: None,
            file_permissions: Some(0o600), // Owner read/write only
        }
    }
}

/// Vault backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultConfig {
    /// Vault server URL
    pub address: String,
    /// Authentication method
    pub auth_method: VaultAuthMethod,
    /// Vault namespace (Vault Enterprise)
    pub namespace: Option<String>,
    /// Transit engine mount path
    pub mount_path: String,
    /// KV engine mount path for storing keys
    pub kv_mount: String,
    /// Path prefix for keys in KV store
    pub key_path_prefix: String,
    /// TLS configuration
    pub tls: Option<TlsConfig>,
}

impl Default for VaultConfig {
    fn default() -> Self {
        Self {
            address: "http://localhost:8200".to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "dev-token".to_string(),
            },
            namespace: None,
            mount_path: "transit".to_string(),
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/keys".to_string(),
            tls: None,
        }
    }
}

/// Vault authentication methods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VaultAuthMethod {
    /// Token authentication
    Token { token: String },
    /// AppRole authentication
    AppRole { role_id: String, secret_id: String },
}

/// TLS configuration for Vault
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to CA certificate file
    pub ca_cert_path: Option<PathBuf>,
    /// Path to client certificate file
    pub client_cert_path: Option<PathBuf>,
    /// Path to client private key file
    pub client_key_path: Option<PathBuf>,
    /// Skip TLS verification (insecure, for development only)
    pub skip_verify: bool,
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum number of keys to cache
    pub max_keys: usize,
    /// TTL for cached keys
    pub ttl: Duration,
    /// Enable cache metrics
    pub enable_metrics: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_keys: 1000,
            ttl: Duration::from_secs(3600), // 1 hour
            enable_metrics: true,
        }
    }
}

impl KmsConfig {
    /// Create a new KMS configuration for local backend (for development and testing only)
    pub fn local(key_dir: PathBuf) -> Self {
        Self {
            backend: KmsBackend::Local,
            backend_config: BackendConfig::Local(LocalConfig {
                key_dir,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Create a new KMS configuration for Vault backend with token authentication (recommended for production)
    pub fn vault(address: Url, token: String) -> Self {
        Self {
            backend: KmsBackend::Vault,
            backend_config: BackendConfig::Vault(VaultConfig {
                address: address.to_string(),
                auth_method: VaultAuthMethod::Token { token },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Create a new KMS configuration for Vault backend with AppRole authentication (recommended for production)
    pub fn vault_approle(address: Url, role_id: String, secret_id: String) -> Self {
        Self {
            backend: KmsBackend::Vault,
            backend_config: BackendConfig::Vault(VaultConfig {
                address: address.to_string(),
                auth_method: VaultAuthMethod::AppRole { role_id, secret_id },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Get the local configuration if backend is Local
    pub fn local_config(&self) -> Option<&LocalConfig> {
        match &self.backend_config {
            BackendConfig::Local(config) => Some(config),
            _ => None,
        }
    }

    /// Get the Vault configuration if backend is Vault
    pub fn vault_config(&self) -> Option<&VaultConfig> {
        match &self.backend_config {
            BackendConfig::Vault(config) => Some(config),
            _ => None,
        }
    }

    /// Set default key ID
    pub fn with_default_key(mut self, key_id: String) -> Self {
        self.default_key_id = Some(key_id);
        self
    }

    /// Set operation timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Enable or disable caching
    pub fn with_cache(mut self, enable: bool) -> Self {
        self.enable_cache = enable;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Validate timeout
        if self.timeout.is_zero() {
            return Err(KmsError::configuration_error("Timeout must be greater than 0"));
        }

        // Validate retry attempts
        if self.retry_attempts == 0 {
            return Err(KmsError::configuration_error("Retry attempts must be greater than 0"));
        }

        // Validate backend-specific configuration
        match &self.backend_config {
            BackendConfig::Local(config) => {
                if !config.key_dir.is_absolute() {
                    return Err(KmsError::configuration_error("Local key directory must be an absolute path"));
                }
            }
            BackendConfig::Vault(config) => {
                if !config.address.starts_with("http://") && !config.address.starts_with("https://") {
                    return Err(KmsError::configuration_error("Vault address must use http or https scheme"));
                }

                if config.mount_path.is_empty() {
                    return Err(KmsError::configuration_error("Vault mount path cannot be empty"));
                }

                // Validate TLS configuration if using HTTPS
                if config.address.starts_with("https://")
                    && let Some(ref tls) = config.tls
                    && !tls.skip_verify
                {
                    // In production, we should have proper TLS configuration
                    if tls.ca_cert_path.is_none() && tls.client_cert_path.is_none() {
                        tracing::warn!("Using HTTPS without custom TLS configuration - relying on system CA");
                    }
                }
            }
        }

        // Validate cache configuration
        if self.enable_cache && self.cache_config.max_keys == 0 {
            return Err(KmsError::configuration_error("Cache max_keys must be greater than 0"));
        }

        Ok(())
    }

    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();

        // Backend type
        if let Ok(backend_type) = std::env::var("RUSTFS_KMS_BACKEND") {
            config.backend = match backend_type.to_lowercase().as_str() {
                "local" => KmsBackend::Local,
                "vault" => KmsBackend::Vault,
                _ => return Err(KmsError::configuration_error(format!("Unknown KMS backend: {backend_type}"))),
            };
        }

        // Default key ID
        if let Ok(key_id) = std::env::var("RUSTFS_KMS_DEFAULT_KEY_ID") {
            config.default_key_id = Some(key_id);
        }

        // Timeout
        if let Ok(timeout_str) = std::env::var("RUSTFS_KMS_TIMEOUT_SECS") {
            let timeout_secs = timeout_str
                .parse::<u64>()
                .map_err(|_| KmsError::configuration_error("Invalid timeout value"))?;
            config.timeout = Duration::from_secs(timeout_secs);
        }

        // Retry attempts
        if let Ok(retries_str) = std::env::var("RUSTFS_KMS_RETRY_ATTEMPTS") {
            config.retry_attempts = retries_str
                .parse()
                .map_err(|_| KmsError::configuration_error("Invalid retry attempts value"))?;
        }

        // Enable cache
        if let Ok(cache_str) = std::env::var("RUSTFS_KMS_ENABLE_CACHE") {
            config.enable_cache = cache_str.parse().unwrap_or(true);
        }

        // Backend-specific configuration
        match config.backend {
            KmsBackend::Local => {
                let key_dir = std::env::var("RUSTFS_KMS_LOCAL_KEY_DIR").unwrap_or_else(|_| "./kms_keys".to_string());
                let master_key = std::env::var("RUSTFS_KMS_LOCAL_MASTER_KEY").ok();

                config.backend_config = BackendConfig::Local(LocalConfig {
                    key_dir: PathBuf::from(key_dir),
                    master_key,
                    file_permissions: Some(0o600),
                });
            }
            KmsBackend::Vault => {
                let address = std::env::var("RUSTFS_KMS_VAULT_ADDRESS").unwrap_or_else(|_| "http://localhost:8200".to_string());
                let token = std::env::var("RUSTFS_KMS_VAULT_TOKEN").unwrap_or_else(|_| "dev-token".to_string());

                config.backend_config = BackendConfig::Vault(VaultConfig {
                    address,
                    auth_method: VaultAuthMethod::Token { token },
                    namespace: std::env::var("RUSTFS_KMS_VAULT_NAMESPACE").ok(),
                    mount_path: std::env::var("RUSTFS_KMS_VAULT_MOUNT_PATH").unwrap_or_else(|_| "transit".to_string()),
                    kv_mount: std::env::var("RUSTFS_KMS_VAULT_KV_MOUNT").unwrap_or_else(|_| "secret".to_string()),
                    key_path_prefix: std::env::var("RUSTFS_KMS_VAULT_KEY_PREFIX")
                        .unwrap_or_else(|_| "rustfs/kms/keys".to_string()),
                    tls: None,
                });
            }
        }

        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = KmsConfig::default();
        assert_eq!(config.backend, KmsBackend::Local);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_local_config() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf());

        assert_eq!(config.backend, KmsBackend::Local);
        assert!(config.validate().is_ok());

        let local_config = config.local_config().expect("Should have local config");
        assert_eq!(local_config.key_dir, temp_dir.path());
    }

    #[test]
    fn test_vault_config() {
        let address = Url::parse("https://vault.example.com:8200").expect("Valid URL");
        let config = KmsConfig::vault(address.clone(), "test-token".to_string());

        assert_eq!(config.backend, KmsBackend::Vault);
        assert!(config.validate().is_ok());

        let vault_config = config.vault_config().expect("Should have vault config");
        assert_eq!(vault_config.address, address.as_str());
    }

    #[test]
    fn test_config_validation() {
        let mut config = KmsConfig::default();

        // Valid config
        assert!(config.validate().is_ok());

        // Invalid timeout
        config.timeout = Duration::from_secs(0);
        assert!(config.validate().is_err());

        // Reset timeout and test invalid retry attempts
        config.timeout = Duration::from_secs(30);
        config.retry_attempts = 0;
        assert!(config.validate().is_err());
    }
}
