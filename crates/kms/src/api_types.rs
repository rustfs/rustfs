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
    redacted_secret_option,
};
use crate::service_manager::KmsServiceStatus;
use crate::types::{KeyMetadata, KeyUsage};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

/// Request to configure KMS with Local backend
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Allow development-only insecure defaults
    pub allow_insecure_dev_defaults: Option<bool>,
}

impl fmt::Debug for ConfigureLocalKmsRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let master_key = redacted_secret_option(self.master_key.as_deref());
        f.debug_struct("ConfigureLocalKmsRequest")
            .field("key_dir", &self.key_dir)
            .field("master_key", &master_key)
            .field("file_permissions", &self.file_permissions)
            .field("default_key_id", &self.default_key_id)
            .field("timeout_seconds", &self.timeout_seconds)
            .field("retry_attempts", &self.retry_attempts)
            .field("enable_cache", &self.enable_cache)
            .field("max_cached_keys", &self.max_cached_keys)
            .field("cache_ttl_seconds", &self.cache_ttl_seconds)
            .field("allow_insecure_dev_defaults", &self.allow_insecure_dev_defaults)
            .finish()
    }
}

/// Request to configure KMS with Vault KV v2 + Transit backend
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigureVaultKmsRequest {
    /// Vault server URL
    pub address: String,
    /// Authentication method
    #[serde(deserialize_with = "deserialize_strict_vault_auth_method")]
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
    /// Allow development-only insecure defaults
    pub allow_insecure_dev_defaults: Option<bool>,
}

/// Request to configure KMS with Vault Transit backend
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigureVaultTransitKmsRequest {
    /// Vault server URL
    pub address: String,
    /// Authentication method
    #[serde(deserialize_with = "deserialize_strict_vault_auth_method")]
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
    /// Allow development-only insecure defaults
    pub allow_insecure_dev_defaults: Option<bool>,
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
#[serde(deny_unknown_fields)]
pub struct StartKmsRequest {
    /// Whether to force start (restart if already running)
    pub force: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
enum StrictVaultAuthMethod {
    Token { token: String },
    AppRole { role_id: String, secret_id: String },
}

impl From<StrictVaultAuthMethod> for VaultAuthMethod {
    fn from(value: StrictVaultAuthMethod) -> Self {
        match value {
            StrictVaultAuthMethod::Token { token } => Self::Token { token },
            StrictVaultAuthMethod::AppRole { role_id, secret_id } => Self::AppRole { role_id, secret_id },
        }
    }
}

fn deserialize_strict_vault_auth_method<'de, D>(deserializer: D) -> Result<VaultAuthMethod, D::Error>
where
    D: Deserializer<'de>,
{
    StrictVaultAuthMethod::deserialize(deserializer).map(Into::into)
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
            allow_insecure_dev_defaults: self.allow_insecure_dev_defaults.unwrap_or(false),
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
            allow_insecure_dev_defaults: self.allow_insecure_dev_defaults.unwrap_or(false),
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
            allow_insecure_dev_defaults: self.allow_insecure_dev_defaults.unwrap_or(false),
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
    use crate::config::REDACTED_SECRET;
    use serde_json::Value;

    fn stable_json_value(value: impl serde::Serialize) -> Value {
        sorted_json_value(serde_json::to_value(value).expect("KMS snapshot value should serialize"))
    }

    fn sorted_json_value(value: Value) -> Value {
        match value {
            Value::Array(values) => Value::Array(values.into_iter().map(sorted_json_value).collect()),
            Value::Object(object) => {
                let mut entries: Vec<_> = object.into_iter().collect();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));

                Value::Object(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key, sorted_json_value(value)))
                        .collect(),
                )
            }
            value => value,
        }
    }

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
    fn test_configure_request_development_defaults_require_opt_in() {
        let local_raw = serde_json::json!({
            "backend_type": "local",
            "key_dir": "/tmp/kms-key-dir"
        });
        let request: ConfigureKmsRequest = serde_json::from_value(local_raw).expect("local request should deserialize");
        let config = request.to_kms_config();
        assert!(config.validate().is_err());

        let local_opt_in_raw = serde_json::json!({
            "backend_type": "local",
            "key_dir": "/tmp/kms-key-dir",
            "allow_insecure_dev_defaults": true
        });
        let request: ConfigureKmsRequest = serde_json::from_value(local_opt_in_raw).expect("local request should deserialize");
        assert!(request.to_kms_config().validate().is_ok());

        let vault_raw = serde_json::json!({
            "backend_type": "vault",
            "address": "http://127.0.0.1:8200",
            "auth_method": {
                "Token": {
                    "token": "dev-token"
                }
            },
            "skip_tls_verify": true
        });
        let request: ConfigureKmsRequest = serde_json::from_value(vault_raw).expect("vault request should deserialize");
        let config = request.to_kms_config();
        assert!(config.validate().is_err());

        let vault_opt_in_raw = serde_json::json!({
            "backend_type": "vault",
            "address": "http://127.0.0.1:8200",
            "auth_method": {
                "Token": {
                    "token": "dev-token"
                }
            },
            "skip_tls_verify": true,
            "allow_insecure_dev_defaults": true
        });
        let request: ConfigureKmsRequest = serde_json::from_value(vault_opt_in_raw).expect("vault request should deserialize");
        assert!(request.to_kms_config().validate().is_ok());
    }

    #[test]
    fn test_configure_request_rejects_unknown_fields() {
        let raw = serde_json::json!({
            "backend_type": "local",
            "key_dir": "./target/kms-key-dir",
            "unexpected_field": true
        });

        let err = serde_json::from_value::<ConfigureKmsRequest>(raw).expect_err("unknown configure field should fail");
        assert!(err.to_string().contains("unknown field"));

        let raw = serde_json::json!({
            "backend_type": "vault",
            "address": "http://127.0.0.1:8200",
            "auth_method": {
                "Token": {
                    "token": "dev-root-token",
                    "unexpected_field": true
                }
            }
        });

        let err = serde_json::from_value::<ConfigureKmsRequest>(raw).expect_err("unknown auth field should fail");
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn test_start_request_rejects_unknown_fields() {
        let err = serde_json::from_str::<StartKmsRequest>(r#"{"force":true,"unexpected_field":true}"#)
            .expect_err("unknown start field should fail");
        assert!(err.to_string().contains("unknown field"));
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
            allow_insecure_dev_defaults: true,
            timeout: Duration::from_secs(30),
            retry_attempts: 3,
            enable_cache: true,
            cache_config: CacheConfig::default(),
        };

        let summary = KmsConfigSummary::from(&config);
        insta::assert_json_snapshot!("kms_vault_transit_config_summary", stable_json_value(&summary));
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

    #[test]
    fn test_configure_request_debug_redacts_kms_secret_fields() {
        let local = ConfigureKmsRequest::Local(ConfigureLocalKmsRequest {
            key_dir: PathBuf::from("/tmp/kms"),
            master_key: Some("local-configure-master-secret".to_string()),
            file_permissions: Some(0o600),
            default_key_id: Some("default-key".to_string()),
            timeout_seconds: Some(30),
            retry_attempts: Some(3),
            enable_cache: Some(true),
            max_cached_keys: Some(16),
            cache_ttl_seconds: Some(60),
            allow_insecure_dev_defaults: None,
        });
        let vault = ConfigureKmsRequest::VaultTransit(ConfigureVaultTransitKmsRequest {
            address: "https://vault.example.com:8200".to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "configure-vault-token-secret".to_string(),
            },
            namespace: None,
            mount_path: Some("transit".to_string()),
            skip_tls_verify: Some(false),
            default_key_id: None,
            timeout_seconds: None,
            retry_attempts: None,
            enable_cache: None,
            max_cached_keys: None,
            cache_ttl_seconds: None,
            allow_insecure_dev_defaults: None,
        });
        let approle = ConfigureKmsRequest::VaultKv2(ConfigureVaultKmsRequest {
            address: "https://vault.example.com:8200".to_string(),
            auth_method: VaultAuthMethod::AppRole {
                role_id: "configure-role-id".to_string(),
                secret_id: "configure-approle-secret-id".to_string(),
            },
            namespace: None,
            mount_path: Some("transit".to_string()),
            kv_mount: Some("secret".to_string()),
            key_path_prefix: Some("rustfs/kms/keys".to_string()),
            skip_tls_verify: Some(false),
            default_key_id: None,
            timeout_seconds: None,
            retry_attempts: None,
            enable_cache: None,
            max_cached_keys: None,
            cache_ttl_seconds: None,
            allow_insecure_dev_defaults: None,
        });

        let rendered = format!("{local:?}\n{vault:?}\n{approle:?}");

        assert!(!rendered.contains("local-configure-master-secret"));
        assert!(!rendered.contains("configure-vault-token-secret"));
        assert!(!rendered.contains("configure-approle-secret-id"));
        assert!(rendered.contains("configure-role-id"));
        assert!(rendered.contains(REDACTED_SECRET));
    }

    #[test]
    fn test_kms_status_response_omits_secret_values_from_json_and_debug() {
        let configs = [
            KmsConfig {
                backend: KmsBackend::Local,
                backend_config: BackendConfig::Local(LocalConfig {
                    key_dir: PathBuf::from("/tmp/kms"),
                    master_key: Some("local-summary-master-secret".to_string()),
                    file_permissions: Some(0o600),
                }),
                ..Default::default()
            },
            KmsConfig::vault(
                url::Url::parse("https://vault.example.com:8200").expect("vault URL"),
                "summary-vault-token-secret".to_string(),
            ),
            KmsConfig::vault_approle(
                url::Url::parse("https://vault.example.com:8200").expect("vault URL"),
                "summary-role-id".to_string(),
                "summary-approle-secret-id".to_string(),
            ),
        ];

        for config in configs {
            let summary = KmsConfigSummary::from(&config);
            let response = KmsStatusResponse {
                status: KmsServiceStatus::Configured,
                backend_type: Some(config.backend.clone()),
                healthy: None,
                config_summary: Some(summary),
            };
            let json = serde_json::to_string(&response).expect("kms status response should serialize");
            let debug = format!("{response:?}");
            let rendered = format!("{json}\n{debug}");

            assert!(!rendered.contains("local-summary-master-secret"));
            assert!(!rendered.contains("summary-vault-token-secret"));
            assert!(!rendered.contains("summary-approle-secret-id"));
            assert!(rendered.contains("has_master_key") || rendered.contains("has_stored_credentials"));
        }
    }

    #[test]
    fn kms_management_responses_have_stable_json_shapes() {
        insta::assert_json_snapshot!(
            "kms_configure_response",
            stable_json_value(ConfigureKmsResponse {
                success: true,
                message: "kms configured".to_string(),
                status: KmsServiceStatus::Configured,
            })
        );
        insta::assert_json_snapshot!(
            "kms_start_response",
            stable_json_value(StartKmsResponse {
                success: true,
                message: "kms started".to_string(),
                status: KmsServiceStatus::Running,
            })
        );
        insta::assert_json_snapshot!(
            "kms_stop_response",
            stable_json_value(StopKmsResponse {
                success: true,
                message: "kms stopped".to_string(),
                status: KmsServiceStatus::Configured,
            })
        );
        insta::assert_json_snapshot!(
            "kms_status_response",
            stable_json_value(KmsStatusResponse {
                status: KmsServiceStatus::Running,
                backend_type: Some(KmsBackend::VaultTransit),
                healthy: Some(true),
                config_summary: None,
            })
        );
        insta::assert_json_snapshot!(
            "kms_delete_key_response",
            stable_json_value(DeleteKeyResponse {
                success: true,
                message: "key scheduled for deletion".to_string(),
                key_id: "key-a".to_string(),
                deletion_date: Some("2026-07-01T00:00:00Z".to_string()),
            })
        );
        insta::assert_json_snapshot!(
            "kms_list_keys_response",
            stable_json_value(ListKeysResponse {
                success: true,
                message: "keys listed".to_string(),
                keys: vec!["key-a".to_string(), "key-b".to_string()],
                truncated: true,
                next_marker: Some("key-b".to_string()),
            })
        );
        insta::assert_json_snapshot!(
            "kms_describe_key_response_missing",
            stable_json_value(DescribeKeyResponse {
                success: false,
                message: "key not found".to_string(),
                key_metadata: None,
            })
        );
        insta::assert_json_snapshot!(
            "kms_cancel_key_deletion_response",
            stable_json_value(CancelKeyDeletionResponse {
                success: true,
                message: "key deletion canceled".to_string(),
                key_id: "key-a".to_string(),
            })
        );
        insta::assert_json_snapshot!(
            "kms_update_key_description_response",
            stable_json_value(UpdateKeyDescriptionResponse {
                success: true,
                message: "key description updated".to_string(),
                key_id: "key-a".to_string(),
            })
        );
        insta::assert_json_snapshot!(
            "kms_tag_key_response",
            stable_json_value(TagKeyResponse {
                success: true,
                message: "key tags updated".to_string(),
                key_id: "key-a".to_string(),
            })
        );
        insta::assert_json_snapshot!(
            "kms_untag_key_response",
            stable_json_value(UntagKeyResponse {
                success: true,
                message: "key tags removed".to_string(),
                key_id: "key-a".to_string(),
            })
        );
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
