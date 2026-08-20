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
    AwsKmsConfig, BackendConfig, CacheConfig, DEFAULT_CACHE_TTL, DEFAULT_MAX_CACHED_KEYS,
    DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX, DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT, KmsBackend, KmsConfig, LocalConfig,
    StaticConfig, TlsConfig, VaultAuthMethod, VaultConfig, VaultTransitConfig, allow_immediate_deletion_from_env,
    redacted_secret, redacted_secret_option,
};
use crate::service_manager::KmsServiceStatus;
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

/// Request to configure KMS with the Vault KV v2 storage backend.
///
/// This backend stores master key material directly in KV v2; confidentiality relies on
/// Vault ACLs and KV v2 at-rest encryption, with no Transit wrapping involved.
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
    /// Deprecated: legacy Transit engine mount path. Still accepted so older clients keep
    /// working, but the Vault KV2 backend never uses it.
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

/// Request to configure KMS with Static single-key backend
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigureStaticKmsRequest {
    /// Key identifier (name) for the single configured key
    pub key_id: String,
    /// Base64-encoded 32-byte AES-256 key material
    pub secret_key: String,
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

/// Request to configure KMS with the AWS KMS backend.
///
/// Accepts no credential material by design: every node resolves AWS
/// credentials through the standard `aws-config` provider chain, so nothing
/// secret is submitted here, persisted with the cluster configuration, or
/// echoed back by the status API.
///
/// Keys are not created by this path. The backend refuses caller-named key
/// creation because AWS assigns identifiers itself, so `default_key_id` must
/// name a key that already exists in AWS, by key id or ARN.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigureAwsKmsRequest {
    /// AWS region hosting the keys.
    ///
    /// Mandatory here, unlike the environment-variable path: this
    /// configuration is persisted once and replayed on every node, so leaving
    /// the region to each node's ambient provider chain would let nodes
    /// silently address different regions — and therefore different keys —
    /// while reporting the same configuration.
    pub region: String,
    /// Endpoint override for local emulators and private endpoints. Unset in
    /// production, where the SDK derives the regional endpoint; a plaintext
    /// endpoint stays gated behind the development opt-in.
    pub endpoint_url: Option<String>,
    /// Default master key ID for auto-encryption, as an AWS key id or ARN
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

impl Drop for ConfigureStaticKmsRequest {
    fn drop(&mut self) {
        use zeroize::Zeroize;

        self.secret_key.zeroize();
    }
}

impl fmt::Debug for ConfigureStaticKmsRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigureStaticKmsRequest")
            .field("key_id", &self.key_id)
            .field("secret_key", &redacted_secret(&self.secret_key))
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

/// Generic KMS configuration request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend_type")]
pub enum ConfigureKmsRequest {
    /// Configure with Local backend
    #[serde(alias = "local", alias = "Local")]
    Local(ConfigureLocalKmsRequest),
    /// Configure with the Vault KV v2 storage backend
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
    /// Configure with Static single-key backend
    #[serde(rename = "Static", alias = "static")]
    Static(ConfigureStaticKmsRequest),
    /// Configure with the AWS KMS backend
    #[serde(rename = "AWS", alias = "AwsKms", alias = "aws", alias = "aws-kms", alias = "aws_kms")]
    Aws(ConfigureAwsKmsRequest),
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
    Token {
        token: String,
    },
    AppRole {
        role_id: String,
        #[serde(default)]
        secret_id: String,
        #[serde(default)]
        secret_id_file: Option<std::path::PathBuf>,
        #[serde(default)]
        mount: Option<String>,
        #[serde(default)]
        refresh_safety_window_secs: Option<u64>,
    },
    Kubernetes {
        role: String,
        #[serde(default)]
        mount: Option<String>,
        #[serde(default)]
        jwt_path: Option<std::path::PathBuf>,
        #[serde(default)]
        refresh_safety_window_secs: Option<u64>,
    },
    TokenFile {
        path: std::path::PathBuf,
        #[serde(default)]
        poll_interval_secs: Option<u64>,
        #[serde(default)]
        refresh_safety_window_secs: Option<u64>,
    },
}

impl From<StrictVaultAuthMethod> for VaultAuthMethod {
    fn from(value: StrictVaultAuthMethod) -> Self {
        match value {
            StrictVaultAuthMethod::Token { token } => Self::Token { token },
            StrictVaultAuthMethod::AppRole {
                role_id,
                secret_id,
                secret_id_file,
                mount,
                refresh_safety_window_secs,
            } => Self::AppRole {
                role_id,
                secret_id,
                secret_id_file,
                mount: mount.unwrap_or_else(|| crate::config::DEFAULT_VAULT_APPROLE_MOUNT.to_string()),
                refresh_safety_window_secs,
            },
            StrictVaultAuthMethod::Kubernetes {
                role,
                mount,
                jwt_path,
                refresh_safety_window_secs,
            } => Self::Kubernetes {
                role,
                mount: mount.unwrap_or_else(|| crate::config::DEFAULT_VAULT_KUBERNETES_MOUNT.to_string()),
                jwt_path: jwt_path.unwrap_or_else(|| std::path::PathBuf::from(crate::config::DEFAULT_VAULT_KUBERNETES_JWT_PATH)),
                refresh_safety_window_secs,
            },
            StrictVaultAuthMethod::TokenFile {
                path,
                poll_interval_secs,
                refresh_safety_window_secs,
            } => Self::TokenFile {
                path,
                poll_interval_secs,
                refresh_safety_window_secs,
            },
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
    /// Vault KV v2 storage backend summary
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
        /// Deprecated: legacy Transit mount path. Unused by the backend; kept only so the
        /// serialized response shape stays stable for existing consumers.
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
    /// Static single-key backend summary
    Static {
        /// Configured key identifier
        key_id: String,
    },
    /// AWS KMS backend summary
    Aws {
        /// Configured region, when pinned instead of resolved by the AWS chain
        region: Option<String>,
        /// Endpoint override, when set for an emulator or private endpoint
        endpoint_url: Option<String>,
    },
}

impl From<&KmsConfig> for KmsConfigSummary {
    fn from(config: &KmsConfig) -> Self {
        // Report the lifetime the cache was built with, not the raw configured
        // value: an oversized `ttl` is clamped rather than rejected, and this
        // response is what operators check the cache against.
        let cache_ttl_seconds = config.cache_config.effective_ttl().as_secs();

        let cache_summary = if config.enable_cache {
            Some(CacheSummary {
                max_keys: config.cache_config.max_keys,
                ttl_seconds: cache_ttl_seconds,
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
                    VaultAuthMethod::Kubernetes { .. } => "kubernetes".to_string(),
                    VaultAuthMethod::TokenFile { .. } => "token_file".to_string(),
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
                    VaultAuthMethod::Kubernetes { .. } => "kubernetes".to_string(),
                    VaultAuthMethod::TokenFile { .. } => "token_file".to_string(),
                },
                has_stored_credentials: true,
                namespace: vault_config.namespace.clone(),
                mount_path: vault_config.mount_path.clone(),
                skip_tls_verify: vault_config.tls.as_ref().is_some_and(|tls| tls.skip_verify),
            },
            BackendConfig::Static(static_config) => BackendSummary::Static {
                key_id: static_config.key_id.clone(),
            },
            BackendConfig::Aws(aws_config) => BackendSummary::Aws {
                region: aws_config.region.clone(),
                endpoint_url: aws_config.endpoint_url.clone(),
            },
        };

        Self {
            backend_type: config.backend.clone(),
            default_key_id: config.default_key_id.clone(),
            timeout_seconds: config.timeout.as_secs(),
            retry_attempts: config.retry_attempts,
            enable_cache: config.enable_cache,
            max_cached_keys: config.cache_config.max_keys,
            cache_ttl_seconds,
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
            // Read from server configuration, never from the request body: the
            // gate must mean the same thing whether KMS was configured at
            // startup or through this endpoint.
            allow_immediate_deletion: allow_immediate_deletion_from_env(),
            timeout: Duration::from_secs(self.timeout_seconds.unwrap_or(30)),
            retry_attempts: self.retry_attempts.unwrap_or(3),
            enable_cache: self.enable_cache.unwrap_or(true),
            cache_config: CacheConfig {
                max_keys: self.max_cached_keys.unwrap_or(DEFAULT_MAX_CACHED_KEYS),
                ttl: self.cache_ttl_seconds.map_or(DEFAULT_CACHE_TTL, Duration::from_secs),
                ..CacheConfig::default()
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
            // Read from server configuration, never from the request body: the
            // gate must mean the same thing whether KMS was configured at
            // startup or through this endpoint.
            allow_immediate_deletion: allow_immediate_deletion_from_env(),
            timeout: Duration::from_secs(self.timeout_seconds.unwrap_or(30)),
            retry_attempts: self.retry_attempts.unwrap_or(3),
            enable_cache: self.enable_cache.unwrap_or(true),
            cache_config: CacheConfig {
                max_keys: self.max_cached_keys.unwrap_or(DEFAULT_MAX_CACHED_KEYS),
                ttl: self.cache_ttl_seconds.map_or(DEFAULT_CACHE_TTL, Duration::from_secs),
                ..CacheConfig::default()
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
                metadata_kv_mount: DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT.to_string(),
                metadata_key_prefix: DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX.to_string(),
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
            // Read from server configuration, never from the request body: the
            // gate must mean the same thing whether KMS was configured at
            // startup or through this endpoint.
            allow_immediate_deletion: allow_immediate_deletion_from_env(),
            timeout: Duration::from_secs(self.timeout_seconds.unwrap_or(30)),
            retry_attempts: self.retry_attempts.unwrap_or(3),
            enable_cache: self.enable_cache.unwrap_or(true),
            cache_config: CacheConfig {
                max_keys: self.max_cached_keys.unwrap_or(DEFAULT_MAX_CACHED_KEYS),
                ttl: self.cache_ttl_seconds.map_or(DEFAULT_CACHE_TTL, Duration::from_secs),
                ..CacheConfig::default()
            },
        }
    }
}

impl ConfigureStaticKmsRequest {
    /// Convert to KmsConfig
    pub fn to_kms_config(&self) -> KmsConfig {
        KmsConfig {
            backend: KmsBackend::Static,
            default_key_id: self.default_key_id.clone(),
            backend_config: BackendConfig::Static(StaticConfig {
                key_id: self.key_id.clone(),
                secret_key: self.secret_key.clone(),
            }),
            allow_insecure_dev_defaults: self.allow_insecure_dev_defaults.unwrap_or(false),
            // Read from server configuration, never from the request body: the
            // gate must mean the same thing whether KMS was configured at
            // startup or through this endpoint.
            allow_immediate_deletion: allow_immediate_deletion_from_env(),
            timeout: Duration::from_secs(self.timeout_seconds.unwrap_or(30)),
            retry_attempts: self.retry_attempts.unwrap_or(3),
            enable_cache: self.enable_cache.unwrap_or(true),
            cache_config: CacheConfig {
                max_keys: self.max_cached_keys.unwrap_or(DEFAULT_MAX_CACHED_KEYS),
                ttl: self.cache_ttl_seconds.map_or(DEFAULT_CACHE_TTL, Duration::from_secs),
                ..CacheConfig::default()
            },
        }
    }
}

impl ConfigureAwsKmsRequest {
    /// Convert to KmsConfig
    pub fn to_kms_config(&self) -> KmsConfig {
        KmsConfig {
            backend: KmsBackend::Aws,
            default_key_id: self.default_key_id.clone(),
            backend_config: BackendConfig::Aws(Box::new(AwsKmsConfig {
                region: Some(self.region.clone()),
                endpoint_url: self.endpoint_url.clone(),
            })),
            allow_insecure_dev_defaults: self.allow_insecure_dev_defaults.unwrap_or(false),
            // Read from server configuration, never from the request body: the
            // gate must mean the same thing whether KMS was configured at
            // startup or through this endpoint.
            allow_immediate_deletion: allow_immediate_deletion_from_env(),
            timeout: Duration::from_secs(self.timeout_seconds.unwrap_or(30)),
            retry_attempts: self.retry_attempts.unwrap_or(3),
            enable_cache: self.enable_cache.unwrap_or(true),
            cache_config: CacheConfig {
                max_keys: self.max_cached_keys.unwrap_or(DEFAULT_MAX_CACHED_KEYS),
                ttl: self.cache_ttl_seconds.map_or(DEFAULT_CACHE_TTL, Duration::from_secs),
                ..CacheConfig::default()
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
            ConfigureKmsRequest::Static(req) => req.to_kms_config(),
            ConfigureKmsRequest::Aws(req) => req.to_kms_config(),
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
    fn test_deserialize_vault_kv2_configure_request_mount_path_optional_but_accepted() {
        // deny_unknown_fields regression guard: mount_path is deprecated but must remain
        // accepted so older clients that still send it do not get a 400.
        let with_mount_path = serde_json::json!({
            "backend_type": "VaultKV2",
            "address": "http://127.0.0.1:8200",
            "auth_method": { "Token": { "token": "dev-root-token" } },
            "mount_path": "transit"
        });
        let request: ConfigureKmsRequest =
            serde_json::from_value(with_mount_path).expect("request with deprecated mount_path should deserialize");
        let config = request.to_kms_config();
        assert_eq!(config.vault_config().expect("vault-kv2 config").mount_path, "transit");

        let without_mount_path = serde_json::json!({
            "backend_type": "VaultKV2",
            "address": "http://127.0.0.1:8200",
            "auth_method": { "Token": { "token": "dev-root-token" } }
        });
        let request: ConfigureKmsRequest =
            serde_json::from_value(without_mount_path).expect("request without mount_path should deserialize");
        let config = request.to_kms_config();
        assert_eq!(config.vault_config().expect("vault-kv2 config").mount_path, "transit");
    }

    #[test]
    fn test_vault_kv2_status_summary_does_not_mention_transit() {
        let config = KmsConfig::vault(
            url::Url::parse("https://vault.example.com:8200").expect("vault URL"),
            "summary-token".to_string(),
        );
        let response = KmsStatusResponse {
            status: KmsServiceStatus::Running,
            backend_type: Some(config.backend.clone()),
            healthy: Some(true),
            config_summary: Some(KmsConfigSummary::from(&config)),
        };

        let json = serde_json::to_string(&response).expect("kms status response should serialize");
        assert!(
            !json.contains("Transit"),
            "vault-kv2 status output must not describe the backend as Transit: {json}"
        );
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

    /// The admin API reaches Kubernetes auth with the role alone; the mount and
    /// the projected token path fall back to the cluster defaults, so a Tenant
    /// manifest carries no credential and no cluster-specific paths.
    #[test]
    fn test_deserialize_vault_configure_request_accepts_kubernetes_auth() {
        let raw = serde_json::json!({
            "backend_type": "vault-transit",
            "address": "https://vault.example.com:8200",
            "mount_path": "rustfs",
            "auth_method": { "Kubernetes": { "role": "rustfs" } }
        });

        let request: ConfigureKmsRequest = serde_json::from_value(raw).expect("kubernetes auth should deserialize");
        let config = request.to_kms_config();
        config.validate().expect("kubernetes auth must validate");

        let vault = config.vault_transit_config().expect("vault transit backend config");
        let VaultAuthMethod::Kubernetes {
            role, mount, jwt_path, ..
        } = &vault.auth_method
        else {
            panic!("expected Kubernetes auth, got {:?}", vault.auth_method);
        };
        assert_eq!(role, "rustfs");
        assert_eq!(mount, crate::config::DEFAULT_VAULT_KUBERNETES_MOUNT);
        assert_eq!(jwt_path, std::path::Path::new(crate::config::DEFAULT_VAULT_KUBERNETES_JWT_PATH));

        let unknown_field = serde_json::json!({
            "backend_type": "vault-transit",
            "address": "https://vault.example.com:8200",
            "auth_method": { "Kubernetes": { "role": "rustfs", "service_account": "rustfs" } }
        });
        serde_json::from_value::<ConfigureKmsRequest>(unknown_field)
            .expect_err("an unknown auth field must be rejected rather than silently dropped");
    }

    #[test]
    fn test_deserialize_aws_configure_request_accepts_type_aliases() {
        for backend_type in ["AWS", "AwsKms", "aws", "aws-kms", "aws_kms"] {
            let raw = serde_json::json!({
                "backend_type": backend_type,
                "region": "eu-central-1",
                "default_key_id": "arn:aws:kms:eu-central-1:111122223333:key/1234abcd"
            });

            let request: ConfigureKmsRequest = serde_json::from_value(raw).unwrap_or_else(|e| panic!("{backend_type}: {e}"));
            let config = request.to_kms_config();
            assert_eq!(config.backend, KmsBackend::Aws, "backend_type={backend_type}");
            let aws = config.aws_kms_config().expect("aws backend config");
            assert_eq!(aws.region.as_deref(), Some("eu-central-1"));
            assert_eq!(aws.endpoint_url, None);
            assert!(config.validate().is_ok(), "backend_type={backend_type}");
        }
    }

    /// A cluster-persisted AWS configuration must pin its own region: a
    /// request that leaves it to each node's ambient provider chain is refused
    /// rather than accepted into a configuration every node interprets
    /// differently.
    #[test]
    fn test_aws_configure_request_requires_an_explicit_region() {
        let missing = serde_json::json!({
            "backend_type": "AWS",
            "default_key_id": "arn:aws:kms:eu-central-1:111122223333:key/1234abcd"
        });
        let err = serde_json::from_value::<ConfigureKmsRequest>(missing).expect_err("a region-less AWS request must be refused");
        assert!(err.to_string().contains("region"), "{err}");

        let empty = serde_json::json!({ "backend_type": "AWS", "region": "" });
        let request: ConfigureKmsRequest = serde_json::from_value(empty).expect("an empty region deserializes");
        assert!(request.to_kms_config().validate().is_err(), "an empty region must not validate");
    }

    #[test]
    fn test_aws_configure_request_rejects_plaintext_endpoint_without_opt_in() {
        let raw = serde_json::json!({
            "backend_type": "AWS",
            "region": "us-east-1",
            "endpoint_url": "http://localhost:4566"
        });
        let request: ConfigureKmsRequest = serde_json::from_value(raw).expect("aws request should deserialize");
        assert!(request.to_kms_config().validate().is_err());

        let opt_in = serde_json::json!({
            "backend_type": "AWS",
            "region": "us-east-1",
            "endpoint_url": "http://localhost:4566",
            "allow_insecure_dev_defaults": true
        });
        let request: ConfigureKmsRequest = serde_json::from_value(opt_in).expect("aws request should deserialize");
        assert!(request.to_kms_config().validate().is_ok());
    }

    /// The AWS summary carries only non-credential settings, because the
    /// backend never holds AWS credential material to begin with.
    #[test]
    fn test_aws_status_summary_reports_only_non_credential_settings() {
        let config = ConfigureAwsKmsRequest {
            region: "us-east-1".to_string(),
            endpoint_url: None,
            default_key_id: Some("arn:aws:kms:us-east-1:111122223333:key/1234abcd".to_string()),
            timeout_seconds: None,
            retry_attempts: None,
            enable_cache: None,
            max_cached_keys: None,
            cache_ttl_seconds: None,
            allow_insecure_dev_defaults: None,
        }
        .to_kms_config();

        let summary = KmsConfigSummary::from(&config);
        assert_eq!(summary.backend_type, KmsBackend::Aws);
        match &summary.backend_summary {
            BackendSummary::Aws { region, endpoint_url } => {
                assert_eq!(region.as_deref(), Some("us-east-1"));
                assert_eq!(endpoint_url.as_deref(), None);
            }
            other => panic!("expected aws summary, got {other:?}"),
        }

        let response = KmsStatusResponse {
            status: KmsServiceStatus::Running,
            backend_type: Some(config.backend),
            healthy: Some(true),
            config_summary: Some(summary),
        };
        let rendered = format!(
            "{}\n{response:?}",
            serde_json::to_string(&response).expect("kms status response should serialize")
        );
        for credential_field in ["access_key", "secret_key", "session_token", "has_stored_credentials"] {
            assert!(
                !rendered.contains(credential_field),
                "aws status output must not describe credential material: {rendered}"
            );
        }
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

        // AWS credentials belong to the provider chain: a request that tries to
        // smuggle them in must be refused, not silently ignored.
        let raw = serde_json::json!({
            "backend_type": "AWS",
            "region": "us-east-1",
            "secret_access_key": "AKIA-not-accepted-here"
        });

        let err = serde_json::from_value::<ConfigureKmsRequest>(raw).expect_err("unknown aws field should fail");
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
                metadata_kv_mount: DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT.to_string(),
                metadata_key_prefix: DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX.to_string(),
                tls: None,
            })),
            allow_insecure_dev_defaults: true,
            allow_immediate_deletion: false,
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
        assert_eq!(summary.max_cached_keys, DEFAULT_MAX_CACHED_KEYS);
        assert_eq!(summary.cache_ttl_seconds, DEFAULT_CACHE_TTL.as_secs());

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
            auth_method: VaultAuthMethod::approle("configure-role-id".to_string(), "configure-approle-secret-id".to_string()),
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

    /// The shapes this crate owns, and only those.
    ///
    /// The first four are served verbatim by the dynamic-configuration admin
    /// handlers, so pinning them here pins the wire. The last three never
    /// reach a socket: `ObjectEncryptionService` returns them and the admin
    /// layer answers with its own `KmsKeyMetadataResponse` instead, so what
    /// they pin is this crate's public API, not the wire.
    ///
    /// No key-management response belongs in this test. Those endpoints are
    /// served from types defined in the `rustfs` crate, and a copy here could
    /// only ever agree with them by accident — see
    /// `kms_key_admin_responses_have_stable_json_shapes` in
    /// `rustfs/src/admin/handlers/kms_keys.rs`.
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
//
// What remains here is the key-metadata trio, and nothing else belongs.
// Create, delete, list, describe and cancel-deletion are served from types
// defined in the `rustfs` crate (`rustfs/src/admin/handlers/kms_keys.rs`)
// carrying fields this crate knows nothing about, so a copy here would shadow
// `crate::types` under the same name while agreeing with the wire only by
// accident.
//
// The same holds for `DeleteKeyRequest`: it lives in `crate::types` alone, so
// the immediate-deletion gate (`force_immediate` + `confirm_key_id`) has
// exactly one definition and cannot be silently dropped by deserializing into
// a copy that lacks it.

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
