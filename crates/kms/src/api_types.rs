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

//! API types for KMS dynamic configuration

use crate::config::{
    BackendConfig, CacheConfig, KmsBackend, KmsConfig, LocalConfig, TlsConfig, VaultAuthMethod, VaultConfig, VaultTransitConfig,
};
use crate::service_manager::KmsServiceStatus;
use crate::types::{KeyMetadata, KeyUsage};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// Request to configure KMS with Local backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigureLocalKmsRequest {
    /// Directory to store key files
    pub key_dir: PathBuf,
    /// Master key for encrypting stored keys (optional)
    pub master_key: Option<String>,
    /// File permissions for key files (octal, optional)
    pub file_permissions: Option<u32>,
    /// Default master key ID for auto-encryption
    pub default_key_id: Option<String>,
    /// Operation timeout in seconds
    pub timeout_seconds: Option<u64>,
    /// Number of retry attempts
    pub retry_attempts: Option<u32>,
    /// Enable caching
    pub enable_cache: Option<bool>,
    /// Maximum number of keys to cache
    pub max_cached_keys: Option<usize>,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: Option<u64>,
}

/// Request to configure KMS with Vault KV v2 + Transit backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigureVaultKmsRequest {
    /// Vault server URL
    pub address: String,
    /// Authentication method
    pub auth_method: VaultAuthMethod,
    /// Vault namespace (Vault Enterprise, optional)
    pub namespace: Option<String>,
    /// Transit engine mount path
    pub mount_path: Option<String>,
    /// KV engine mount path for storing keys  
    pub kv_mount: Option<String>,
    /// Path prefix for keys in KV store
    pub key_path_prefix: Option<String>,
    /// Skip TLS verification (insecure, for development only)
    pub skip_tls_verify: Option<bool>,
    /// Default master key ID for auto-encryption
    pub default_key_id: Option<String>,
    /// Operation timeout in seconds
    pub timeout_seconds: Option<u64>,
    /// Number of retry attempts
    pub retry_attempts: Option<u32>,
    /// Enable caching
    pub enable_cache: Option<bool>,
    /// Maximum number of keys to cache
    pub max_cached_keys: Option<usize>,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: Option<u64>,
}

/// Request to configure KMS with Vault Transit backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigureVaultTransitKmsRequest {
    /// Vault server URL
    pub address: String,
    /// Authentication method
    pub auth_method: VaultAuthMethod,
    /// Vault namespace (Vault Enterprise, optional)
    pub namespace: Option<String>,
    /// Transit engine mount path
    pub mount_path: Option<String>,
    /// Skip TLS verification (insecure, for development only)
    pub skip_tls_verify: Option<bool>,
    /// Default master key ID for auto-encryption
    pub default_key_id: Option<String>,
    /// Operation timeout in seconds
    pub timeout_seconds: Option<u64>,
    /// Number of retry attempts
    pub retry_attempts: Option<u32>,
    /// Enable caching
    pub enable_cache: Option<bool>,
    /// Maximum number of keys to cache
    pub max_cached_keys: Option<usize>,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: Option<u64>,
}

/// Generic KMS configuration request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend_type")]
pub enum ConfigureKmsRequest {
    /// Configure with Local backend
    #[serde(alias = "local", alias = "Local")]
    Local(ConfigureLocalKmsRequest),
    /// Configure with Vault KV v2 + Transit backend
    #[serde(
        rename = "VaultKV2",
        alias = "Vault",
        alias = "vault",
        alias = "vault-kv2",
        alias = "vault_kv2"
    )]
    VaultKv2(ConfigureVaultKmsRequest),
    /// Configure with Vault Transit backend
    #[serde(rename = "VaultTransit", alias = "vault-transit", alias = "vault_transit")]
    VaultTransit(ConfigureVaultTransitKmsRequest),
}

/// KMS configuration response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigureKmsResponse {
    /// Whether configuration was successful
    pub success: bool,
    /// Status message
    pub message: String,
    /// New service status
    pub status: KmsServiceStatus,
}

/// Request to start KMS service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartKmsRequest {
    /// Whether to force start (restart if already running)
    pub force: Option<bool>,
}

/// KMS start response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartKmsResponse {
    /// Whether start was successful
    pub success: bool,
    /// Status message
    pub message: String,
    /// New service status
    pub status: KmsServiceStatus,
}

/// KMS stop response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopKmsResponse {
    /// Whether stop was successful
    pub success: bool,
    /// Status message
    pub message: String,
    /// New service status
    pub status: KmsServiceStatus,
}

/// KMS status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsStatusResponse {
    /// Current service status
    pub status: KmsServiceStatus,
    /// Current backend type (if configured)
    pub backend_type: Option<KmsBackend>,
    /// Whether KMS is healthy (if running)
    pub healthy: Option<bool>,
    /// Configuration summary (if configured)
    pub config_summary: Option<KmsConfigSummary>,
}

/// Summary of KMS configuration (without sensitive data)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsConfigSummary {
    /// Backend type
    pub backend_type: KmsBackend,
    /// Default key ID (if configured)
    pub default_key_id: Option<String>,
    /// Operation timeout in seconds
    pub timeout_seconds: u64,
    /// Number of retry attempts
    pub retry_attempts: u32,
    /// Whether caching is enabled
    pub enable_cache: bool,
    /// Maximum number of cached keys
    pub max_cached_keys: usize,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: u64,
    /// Cache configuration summary
    pub cache_summary: Option<CacheSummary>,
    /// Backend-specific summary
    pub backend_summary: BackendSummary,
}

/// Cache configuration summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSummary {
    /// Maximum number of keys to cache
    pub max_keys: usize,
    /// Cache TTL in seconds
    pub ttl_seconds: u64,
    /// Whether cache metrics are enabled
    pub enable_metrics: bool,
}

/// Backend-specific configuration summary
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend_type", rename_all = "kebab-case")]
pub enum BackendSummary {
    /// Local backend summary
    Local {
        /// Key directory path
        key_dir: PathBuf,
        /// Whether master key is configured
        has_master_key: bool,
        /// File permissions (octal)
        file_permissions: Option<u32>,
    },
    /// Vault KV v2 + Transit backend summary
    #[serde(alias = "vault")]
    VaultKv2 {
        /// Vault server address
        address: String,
        /// Authentication method type
        auth_method_type: String,
        /// Whether backend credentials are configured
        has_stored_credentials: bool,
        /// Namespace (if configured)
        namespace: Option<String>,
        /// Transit engine mount path
        mount_path: String,
        /// KV engine mount path
        kv_mount: String,
        /// Key path prefix
        key_path_prefix: String,
        /// Skip TLS verification
        skip_tls_verify: bool,
    },
    /// Vault Transit backend summary
    VaultTransit {
        /// Vault server address
        address: String,
        /// Authentication method type
        auth_method_type: String,
        /// Whether backend credentials are configured
        has_stored_credentials: bool,
        /// Namespace (if configured)
        namespace: Option<String>,
        /// Transit engine mount path
        mount_path: String,
        /// Skip TLS verification
        skip_tls_verify: bool,
    },
}

impl From<&KmsConfig> for KmsConfigSummary {
    fn from(config: &KmsConfig) -> Self {
        let cache_summary = if config.enable_cache {
            Some(CacheSummary {
                max_keys: config.cache_config.max_keys,
                ttl_seconds: config.cache_config.ttl.as_secs(),
                enable_metrics: config.cache_config.enable_metrics,
            })
        } else {
            None
        };

        let backend_summary = match &config.backend_config {
            BackendConfig::Local(local_config) => BackendSummary::Local {
                key_dir: local_config.key_dir.clone(),
                has_master_key: local_config.master_key.is_some(),
                file_permissions: local_config.file_permissions,
            },
            BackendConfig::VaultKv2(vault_config) => BackendSummary::VaultKv2 {
                address: vault_config.address.clone(),
                auth_method_type: match &vault_config.auth_method {
                    VaultAuthMethod::Token { .. } => "token".to_string(),
                    VaultAuthMethod::AppRole { .. } => "approle".to_string(),
                },
                has_stored_credentials: true,
                namespace: vault_config.namespace.clone(),
                mount_path: vault_config.mount_path.clone(),
                kv_mount: vault_config.kv_mount.clone(),
                key_path_prefix: vault_config.key_path_prefix.clone(),
                skip_tls_verify: vault_config.tls.as_ref().is_some_and(|tls| tls.skip_verify),
            },
            BackendConfig::VaultTransit(vault_config) => BackendSummary::VaultTransit {
                address: vault_config.address.clone(),
                auth_method_type: match &vault_config.auth_method {
                    VaultAuthMethod::Token { .. } => "token".to_string(),
                    VaultAuthMethod::AppRole { .. } => "approle".to_string(),
                },
                has_stored_credentials: true,
                namespace: vault_config.namespace.clone(),
                mount_path: vault_config.mount_path.clone(),
                skip_tls_verify: vault_config.tls.as_ref().is_some_and(|tls| tls.skip_verify),
            },
        };

        Self {
            backend_type: config.backend.clone(),
            default_key_id: config.default_key_id.clone(),
            timeout_seconds: config.timeout.as_secs(),
            retry_attempts: config.retry_attempts,
            enable_cache: config.enable_cache,
            max_cached_keys: config.cache_config.max_keys,
            cache_ttl_seconds: config.cache_config.ttl.as_secs(),
            cache_summary,
            backend_summary,
        }
    }
}

impl ConfigureLocalKmsRequest {
    /// Convert to KmsConfig
    pub fn to_kms_config(&self) -> KmsConfig {
        KmsConfig {
            backend: KmsBackend::Local,
            default_key_id: self.default_key_id.clone(),
            backend_config: BackendConfig::Local(LocalConfig {
                key_dir: self.key_dir.clone(),
                master_key: self.master_key.clone(),
                file_permissions: self.file_permissions,
            }),
            timeout: Duration::from_secs(self.timeout_seconds.unwrap_or(30)),
            retry_attempts: self.retry_attempts.unwrap_or(3),
            enable_cache: self.enable_cache.unwrap_or(true),
            cache_config: CacheConfig {
                max_keys: self.max_cached_keys.unwrap_or(1000),
                ttl: Duration::from_secs(self.cache_ttl_seconds.unwrap_or(3600)),
                enable_metrics: true,
            },
        }
    }
}

impl ConfigureVaultKmsRequest {
    /// Convert to KmsConfig
    pub fn to_kms_config(&self) -> KmsConfig {
        KmsConfig {
            backend: KmsBackend::VaultKv2,
            default_key_id: self.default_key_id.clone(),
            backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
                address: self.address.clone(),
                auth_method: self.auth_method.clone(),
                namespace: self.namespace.clone(),
                mount_path: self.mount_path.clone().unwrap_or_else(|| "transit".to_string()),
                kv_mount: self.kv_mount.clone().unwrap_or_else(|| "secret".to_string()),
                key_path_prefix: self.key_path_prefix.clone().unwrap_or_else(|| "rustfs/kms/keys".to_string()),
                tls: if self.skip_tls_verify.unwrap_or(false) {
                    Some(TlsConfig {
                        ca_cert_path: None,
                        client_cert_path: None,
                        client_key_path: None,
                        skip_verify: true,
                    })
                } else {
                    None
                },
            })),
            timeout: Duration::from_secs(self.timeout_seconds.unwrap_or(30)),
            retry_attempts: self.retry_attempts.unwrap_or(3),
            enable_cache: self.enable_cache.unwrap_or(true),
            cache_config: CacheConfig {
                max_keys: self.max_cached_keys.unwrap_or(1000),
                ttl: Duration::from_secs(self.cache_ttl_seconds.unwrap_or(3600)),
                enable_metrics: true,
            },
        }
    }
}

impl ConfigureVaultTransitKmsRequest {
    /// Convert to KmsConfig
    pub fn to_kms_config(&self) -> KmsConfig {
        KmsConfig {
            backend: KmsBackend::VaultTransit,
            default_key_id: self.default_key_id.clone(),
            backend_config: BackendConfig::VaultTransit(Box::new(VaultTransitConfig {
                address: self.address.clone(),
                auth_method: self.auth_method.clone(),
                namespace: self.namespace.clone(),
                mount_path: self.mount_path.clone().unwrap_or_else(|| "transit".to_string()),
                tls: if self.skip_tls_verify.unwrap_or(false) {
                    Some(TlsConfig {
                        ca_cert_path: None,
                        client_cert_path: None,
                        client_key_path: None,
                        skip_verify: true,
                    })
                } else {
                    None
                },
            })),
            timeout: Duration::from_secs(self.timeout_seconds.unwrap_or(30)),
            retry_attempts: self.retry_attempts.unwrap_or(3),
            enable_cache: self.enable_cache.unwrap_or(true),
            cache_config: CacheConfig {
                max_keys: self.max_cached_keys.unwrap_or(1000),
                ttl: Duration::from_secs(self.cache_ttl_seconds.unwrap_or(3600)),
                enable_metrics: true,
            },
        }
    }
}

impl ConfigureKmsRequest {
    /// Convert to KmsConfig
    pub fn to_kms_config(&self) -> KmsConfig {
        match self {
            ConfigureKmsRequest::Local(req) => req.to_kms_config(),
            ConfigureKmsRequest::VaultKv2(req) => req.to_kms_config(),
            ConfigureKmsRequest::VaultTransit(req) => req.to_kms_config(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_vault_kv2_configure_request_accepts_type_aliases() {
        let bases = ["VaultKV2", "Vault", "vault", "vault-kv2", "vault_kv2"];
        for backend_type in bases {
            let raw = serde_json::json!({
                "backend_type": backend_type,
                "address": "http://127.0.0.1:8200",
                "auth_method": {
                    "Token": {
                        "token": "dev-root-token"
                    }
                },
                "mount_path": "transit",
                "default_key_id": "rustfs-master-key"
            });

            let request: ConfigureKmsRequest = serde_json::from_value(raw).unwrap_or_else(|e| panic!("{backend_type}: {e}"));
            let config = request.to_kms_config();
            assert_eq!(config.backend, KmsBackend::VaultKv2, "backend_type={backend_type}");
            let vault = config.vault_config().expect("vault-kv2 config");
            assert_eq!(vault.mount_path, "transit");
        }
    }

    #[test]
    fn test_deserialize_vault_transit_configure_request() {
        let cases = ["VaultTransit", "vault-transit", "vault_transit"];
        for raw_backend in cases {
            let raw = serde_json::json!({
                "backend_type": raw_backend,
                "address": "http://127.0.0.1:8200",
                "auth_method": {
                    "Token": {
                        "token": "dev-root-token"
                    }
                },
                "mount_path": "transit",
                "default_key_id": "rustfs-master-key"
            });
            let request: ConfigureKmsRequest = serde_json::from_value(raw).expect("vault-transit request should deserialize");
            let config = request.to_kms_config();
            assert_eq!(config.backend, KmsBackend::VaultTransit);
            let vault = config.vault_transit_config().expect("vault-transit config should be present");
            assert_eq!(vault.mount_path, "transit");
        }
    }

    #[test]
    fn test_deserialize_local_configure_request() {
        let raw = serde_json::json!({
            "backend_type": "local",
            "key_dir": "./target/kms-key-dir"
        });

        let request: ConfigureKmsRequest = serde_json::from_value(raw).expect("vault-transit request should deserialize");
        let config = request.to_kms_config();

        assert_eq!(config.backend, KmsBackend::Local);
    }

    #[test]
    fn test_vault_transit_summary_reports_backend_details() {
        let config = KmsConfig {
            backend: KmsBackend::VaultTransit,
            default_key_id: Some("rustfs-master-key".to_string()),
            backend_config: BackendConfig::VaultTransit(Box::new(VaultTransitConfig {
                address: "http://127.0.0.1:8200".to_string(),
                auth_method: VaultAuthMethod::Token {
                    token: "dev-root-token".to_string(),
                },
                namespace: Some("tenant-a".to_string()),
                mount_path: "transit".to_string(),
                tls: None,
            })),
            timeout: Duration::from_secs(30),
            retry_attempts: 3,
            enable_cache: true,
            cache_config: CacheConfig::default(),
        };

        let summary = KmsConfigSummary::from(&config);
        assert_eq!(summary.backend_type, KmsBackend::VaultTransit);
        assert_eq!(summary.timeout_seconds, 30);
        assert_eq!(summary.retry_attempts, 3);
        assert_eq!(summary.max_cached_keys, 1000);
        assert_eq!(summary.cache_ttl_seconds, 3600);

        match summary.backend_summary {
            BackendSummary::VaultTransit {
                address,
                auth_method_type,
                has_stored_credentials,
                namespace,
                mount_path,
                skip_tls_verify,
                ..
            } => {
                assert_eq!(address, "http://127.0.0.1:8200");
                assert_eq!(auth_method_type, "token");
                assert!(has_stored_credentials);
                assert_eq!(namespace.as_deref(), Some("tenant-a"));
                assert_eq!(mount_path, "transit");
                assert!(!skip_tls_verify);
            }
            other => panic!("expected vault-transit summary, got {other:?}"),
        }
    }
}

// ========================================
// Key Management API Types
// ========================================

/// Request to create a new key with optional custom name
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateKeyRequest {
    /// Custom key name (optional, will auto-generate UUID if not provided)
    pub key_name: Option<String>,
    /// Key usage type
    pub key_usage: KeyUsage,
    /// Key description
    pub description: Option<String>,
    /// Key policy JSON string
    pub policy: Option<String>,
    /// Tags for the key
    pub tags: HashMap<String, String>,
    /// Origin of the key
    pub origin: Option<String>,
}

impl Default for CreateKeyRequest {
    fn default() -> Self {
        Self {
            key_name: None,
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            policy: None,
            tags: HashMap::new(),
            origin: None,
        }
    }
}

/// Response from create key operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateKeyResponse {
    /// Success flag
    pub success: bool,
    /// Status message
    pub message: String,
    /// Created key ID (either custom name or auto-generated UUID)
    pub key_id: String,
    /// Key metadata
    pub key_metadata: KeyMetadata,
}

/// Request to delete a key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteKeyRequest {
    /// Key ID to delete
    pub key_id: String,
    /// Number of days to wait before deletion (7-30 days, optional)
    pub pending_window_in_days: Option<u32>,
    /// Force immediate deletion (for development/testing only)
    pub force_immediate: Option<bool>,
}

/// Response from delete key operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteKeyResponse {
    /// Success flag
    pub success: bool,
    /// Status message
    pub message: String,
    /// Key ID that was deleted or scheduled for deletion
    pub key_id: String,
    /// Deletion date (if scheduled)
    pub deletion_date: Option<String>,
}

/// Request to list all keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListKeysRequest {
    /// Maximum number of keys to return (1-1000)
    pub limit: Option<u32>,
    /// Pagination marker
    pub marker: Option<String>,
}

/// Response from list keys operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListKeysResponse {
    /// Success flag
    pub success: bool,
    /// Status message
    pub message: String,
    /// List of key IDs
    pub keys: Vec<String>,
    /// Whether more keys are available
    pub truncated: bool,
    /// Next marker for pagination
    pub next_marker: Option<String>,
}

/// Request to describe a key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeKeyRequest {
    /// Key ID to describe
    pub key_id: String,
}

/// Response from describe key operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeKeyResponse {
    /// Success flag
    pub success: bool,
    /// Status message
    pub message: String,
    /// Key metadata
    pub key_metadata: Option<KeyMetadata>,
}

/// Request to cancel key deletion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelKeyDeletionRequest {
    /// Key ID to cancel deletion for
    pub key_id: String,
}

/// Response from cancel key deletion operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelKeyDeletionResponse {
    /// Success flag
    pub success: bool,
    /// Status message
    pub message: String,
    /// Key ID
    pub key_id: String,
}

/// Request to update key description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateKeyDescriptionRequest {
    /// Key ID to update
    pub key_id: String,
    /// New description
    pub description: String,
}

/// Response from update key description operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateKeyDescriptionResponse {
    /// Success flag
    pub success: bool,
    /// Status message
    pub message: String,
    /// Key ID
    pub key_id: String,
}

/// Request to add/update key tags
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagKeyRequest {
    /// Key ID to tag
    pub key_id: String,
    /// Tags to add/update
    pub tags: HashMap<String, String>,
}

/// Response from tag key operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagKeyResponse {
    /// Success flag
    pub success: bool,
    /// Status message
    pub message: String,
    /// Key ID
    pub key_id: String,
}

/// Request to remove key tags
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UntagKeyRequest {
    /// Key ID to untag
    pub key_id: String,
    /// Tag keys to remove
    pub tag_keys: Vec<String>,
}

/// Response from untag key operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UntagKeyResponse {
    /// Success flag
    pub success: bool,
    /// Status message
    pub message: String,
    /// Key ID
    pub key_id: String,
}
