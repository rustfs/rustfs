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
use rustfs_security_governance::{RedactionLevel, RedactionRule};
use rustfs_utils::{get_env_bool, get_env_opt_str, get_env_str};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use url::Url;

pub const ENV_KMS_ALLOW_INSECURE_DEV_DEFAULTS: &str = "RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS";
pub const ENV_KMS_ALLOW_IMMEDIATE_DELETION: &str = "RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION";
pub const ENV_KMS_VAULT_ADDRESS: &str = "RUSTFS_KMS_VAULT_ADDRESS";
pub const ENV_KMS_VAULT_TOKEN: &str = "RUSTFS_KMS_VAULT_TOKEN";
pub const ENV_KMS_VAULT_NAMESPACE: &str = "RUSTFS_KMS_VAULT_NAMESPACE";
pub const ENV_KMS_VAULT_MOUNT_PATH: &str = "RUSTFS_KMS_VAULT_MOUNT_PATH";
pub const ENV_KMS_VAULT_SKIP_TLS_VERIFY: &str = "RUSTFS_KMS_VAULT_SKIP_TLS_VERIFY";
pub const ENV_KMS_VAULT_TRANSIT_METADATA_KV_MOUNT: &str = "RUSTFS_KMS_VAULT_TRANSIT_METADATA_KV_MOUNT";
pub const ENV_KMS_VAULT_TRANSIT_METADATA_PREFIX: &str = "RUSTFS_KMS_VAULT_TRANSIT_METADATA_PREFIX";
pub const ENV_KMS_STATIC_SECRET_KEY: &str = "RUSTFS_KMS_STATIC_SECRET_KEY";
pub const ENV_KMS_STATIC_SECRET_KEY_FILE: &str = "RUSTFS_KMS_STATIC_SECRET_KEY_FILE";
pub const ENV_KMS_VAULT_APPROLE_ROLE_ID: &str = "RUSTFS_KMS_VAULT_APPROLE_ROLE_ID";
pub const ENV_KMS_VAULT_APPROLE_SECRET_ID: &str = "RUSTFS_KMS_VAULT_APPROLE_SECRET_ID";
pub const ENV_KMS_VAULT_APPROLE_SECRET_ID_FILE: &str = "RUSTFS_KMS_VAULT_APPROLE_SECRET_ID_FILE";
pub const ENV_KMS_VAULT_APPROLE_MOUNT: &str = "RUSTFS_KMS_VAULT_APPROLE_MOUNT";
pub const ENV_KMS_VAULT_TOKEN_FILE: &str = "RUSTFS_KMS_VAULT_TOKEN_FILE";
pub const ENV_KMS_VAULT_KUBERNETES_ROLE: &str = "RUSTFS_KMS_VAULT_KUBERNETES_ROLE";
pub const ENV_KMS_VAULT_KUBERNETES_MOUNT: &str = "RUSTFS_KMS_VAULT_KUBERNETES_MOUNT";
pub const ENV_KMS_VAULT_KUBERNETES_JWT_PATH: &str = "RUSTFS_KMS_VAULT_KUBERNETES_JWT_PATH";
pub const ENV_KMS_AWS_REGION: &str = "RUSTFS_KMS_AWS_REGION";
pub const ENV_KMS_AWS_ENDPOINT_URL: &str = "RUSTFS_KMS_AWS_ENDPOINT_URL";
/// Age in whole seconds beyond which a key is reported as due for rotation;
/// unset leaves rotation readiness unreported. Read once when the manager is
/// built, by [`crate::manager::KmsManager`].
pub const ENV_KMS_ROTATION_MAX_AGE_SECS: &str = "RUSTFS_KMS_ROTATION_MAX_AGE_SECS";
pub const ENV_KMS_ROTATION_MAX_WRAPS: &str = "RUSTFS_KMS_ROTATION_MAX_WRAPS";
pub const DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT: &str = "secret";
pub const DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX: &str = "rustfs/kms/transit-metadata";
pub const DEFAULT_VAULT_APPROLE_MOUNT: &str = "approle";
pub const DEFAULT_VAULT_KUBERNETES_MOUNT: &str = "kubernetes";
/// Where the kubelet projects a pod's ServiceAccount token by default.
pub const DEFAULT_VAULT_KUBERNETES_JWT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";

/// Upper bound applied to `KmsConfig::timeout` when deriving backend behavior.
///
/// Out-of-range values are clamped at use rather than rejected so existing
/// deployments with oversized settings keep starting after an upgrade.
pub(crate) const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(300);

/// Upper bound applied to `KmsConfig::retry_attempts` when deriving backend behavior.
pub(crate) const MAX_RETRY_ATTEMPTS: u32 = 10;

/// Default number of key metadata entries the cache holds.
pub const DEFAULT_MAX_CACHED_KEYS: usize = 1000;

/// Default lifetime of a cached key metadata entry.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);

/// Upper bound applied to `CacheConfig::ttl` when building the metadata cache.
///
/// Out-of-range values are clamped at use rather than rejected, matching
/// `MAX_OPERATION_TIMEOUT`, so existing deployments with an oversized setting
/// keep starting after an upgrade.
pub(crate) const MAX_CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

fn default_vault_transit_metadata_kv_mount() -> String {
    DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT.to_string()
}

fn default_vault_transit_metadata_key_prefix() -> String {
    DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX.to_string()
}

fn default_vault_kv2_mount_path() -> String {
    "transit".to_string()
}

fn default_vault_approle_mount() -> String {
    DEFAULT_VAULT_APPROLE_MOUNT.to_string()
}

fn default_vault_kubernetes_mount() -> String {
    DEFAULT_VAULT_KUBERNETES_MOUNT.to_string()
}

fn default_vault_kubernetes_jwt_path() -> PathBuf {
    PathBuf::from(DEFAULT_VAULT_KUBERNETES_JWT_PATH)
}

pub const KMS_CONFIG_REDACTION_RULES: &[RedactionRule] = &[
    RedactionRule::new("kms.local.master_key", RedactionLevel::Secret, "local backend key encryption material"),
    RedactionRule::new("kms.vault.token", RedactionLevel::Secret, "vault authentication token"),
    RedactionRule::new("kms.vault.approle.secret_id", RedactionLevel::Secret, "vault approle secret"),
    RedactionRule::new("kms.vault_transit.token", RedactionLevel::Secret, "vault transit authentication token"),
    RedactionRule::new(
        "kms.vault_transit.approle.secret_id",
        RedactionLevel::Secret,
        "vault transit approle secret",
    ),
    RedactionRule::new(
        "kms.configure.local.master_key",
        RedactionLevel::Secret,
        "admin configure request local master key",
    ),
    RedactionRule::new(
        "kms.configure.vault.token",
        RedactionLevel::Secret,
        "admin configure request vault authentication token",
    ),
    RedactionRule::new(
        "kms.configure.vault.approle.secret_id",
        RedactionLevel::Secret,
        "admin configure request vault approle secret",
    ),
    RedactionRule::new(
        "kms.configure.vault_transit.token",
        RedactionLevel::Secret,
        "admin configure request vault transit authentication token",
    ),
    RedactionRule::new(
        "kms.configure.vault_transit.approle.secret_id",
        RedactionLevel::Secret,
        "admin configure request vault transit approle secret",
    ),
    RedactionRule::new("kms.static.secret_key", RedactionLevel::Secret, "static backend secret key material"),
];

pub(crate) const REDACTED_SECRET: &str = "***redacted***";

pub(crate) fn redacted_secret(value: &str) -> &'static str {
    if value.is_empty() { "" } else { REDACTED_SECRET }
}

pub(crate) fn redacted_secret_option(value: Option<&str>) -> Option<&'static str> {
    value.map(redacted_secret)
}

/// KMS backend types
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KmsBackend {
    /// Vault KV v2 storage backend: master key material is stored directly in KV v2.
    /// Confidentiality relies on Vault ACLs, KV v2 at-rest encryption, and TLS; the
    /// backend performs no Transit wrapping of key material.
    #[serde(rename = "VaultKV2", alias = "Vault")]
    VaultKv2,
    /// Vault Transit backend using Vault as the cryptographic source of truth
    #[serde(rename = "VaultTransit")]
    VaultTransit,
    /// Local file-based backend for development and testing only
    #[default]
    Local,
    /// Static single-key backend that derives DEKs from a pre-configured key
    #[serde(rename = "Static")]
    Static,
    /// AWS KMS backend: AWS is the cryptographic source of truth and owns key
    /// state, versioning, and the deletion window.
    #[serde(rename = "AWS", alias = "AwsKms")]
    Aws,
}

impl KmsBackend {
    /// Stable identifier for logs, metrics, and audit records.
    ///
    /// External consumers key off these values, so treat them as a wire
    /// contract rather than a rendering detail.
    pub fn as_str(&self) -> &'static str {
        match self {
            KmsBackend::VaultKv2 => "vault-kv2",
            KmsBackend::VaultTransit => "vault-transit",
            KmsBackend::Local => "local",
            KmsBackend::Static => "static",
            KmsBackend::Aws => "aws",
        }
    }
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
    /// Allow development-only insecure defaults such as plaintext local keys or HTTP Vault.
    #[serde(default)]
    pub allow_insecure_dev_defaults: bool,
    /// Allow `DeleteKey` requests to skip the pending-deletion waiting window and
    /// destroy key material right away.
    ///
    /// Off by default: an immediate deletion is unrecoverable and takes every
    /// object encrypted under the key with it, so the waiting window (plus
    /// `CancelKeyDeletion`) is the only recovery path. Operators who genuinely
    /// need immediate deletion — throwaway test clusters, key material that was
    /// never used — must turn it on through server configuration
    /// ([`ENV_KMS_ALLOW_IMMEDIATE_DELETION`]); the request must still echo the
    /// key id back for confirmation.
    ///
    /// Not part of the serialized configuration, and not settable through the
    /// admin configure API. It is per-server operator state that has to be
    /// re-stated to survive a restart: persisting it would carry one operator's
    /// one-time enablement into the cluster-wide config that every node reloads,
    /// long after the deletion it was turned on for.
    #[serde(skip)]
    pub allow_immediate_deletion: bool,
    /// Timeout for a single backend attempt.
    ///
    /// This bounds one outbound request, not the whole operation: the operation
    /// policy owns the total deadline across retries. Values above 300 seconds
    /// are clamped at use (see `KmsConfig::effective_timeout`).
    pub timeout: Duration,
    /// Number of retry attempts.
    ///
    /// Values above 10 are clamped at use (see `KmsConfig::effective_retry_attempts`).
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
            allow_insecure_dev_defaults: false,
            allow_immediate_deletion: false,
            timeout: Duration::from_secs(30),
            retry_attempts: 3,
            enable_cache: true,
            cache_config: CacheConfig::default(),
        }
    }
}

/// Backend-specific configuration
#[derive(Clone, Serialize, Deserialize)]
pub enum BackendConfig {
    /// Local backend configuration
    Local(LocalConfig),
    /// Vault KV v2 storage backend configuration
    #[serde(rename = "VaultKV2", alias = "Vault")]
    VaultKv2(Box<VaultConfig>),
    /// Vault Transit backend configuration
    VaultTransit(Box<VaultTransitConfig>),
    /// Static single-key backend configuration
    Static(StaticConfig),
    /// AWS KMS backend configuration
    #[serde(rename = "AWS", alias = "AwsKms")]
    Aws(Box<AwsKmsConfig>),
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self::Local(LocalConfig::default())
    }
}

impl fmt::Debug for BackendConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(config) => f.debug_tuple("Local").field(config).finish(),
            Self::VaultKv2(config) => f.debug_tuple("VaultKv2").field(config).finish(),
            Self::VaultTransit(config) => f.debug_tuple("VaultTransit").field(config).finish(),
            Self::Static(config) => f.debug_tuple("Static").field(config).finish(),
            Self::Aws(config) => f.debug_tuple("Aws").field(config).finish(),
        }
    }
}

/// Local KMS backend configuration
#[derive(Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    /// Directory to store key files
    pub key_dir: PathBuf,
    /// Master key for encrypting stored keys (if None, only explicit development-mode plaintext storage is allowed)
    pub master_key: Option<String>,
    /// File permissions for key files (octal)
    pub file_permissions: Option<u32>,
}

impl fmt::Debug for LocalConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let master_key = redacted_secret_option(self.master_key.as_deref());
        f.debug_struct("LocalConfig")
            .field("key_dir", &self.key_dir)
            .field("master_key", &master_key)
            .field("file_permissions", &self.file_permissions)
            .finish()
    }
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

/// Static single-key KMS backend configuration
///
/// The configured 32-byte key is used directly as the AES-256-GCM key that
/// wraps data encryption keys — there is no HMAC-SHA256 derivation step — and
/// each wrapped DEK is serialized as a RustFS `DataKeyEnvelope` JSON blob.
///
/// This mirrors the *concept* of MinIO's builtin/static single-key KMS, but is
/// not wire-compatible with it: MinIO wraps DEKs in a different (`{"aead": ...}`)
/// blob that this backend neither produces nor accepts, so KMS ciphertext
/// written by MinIO cannot be opened here. Reading MinIO-written SSE objects is
/// tracked separately in rustfs/backlog#1638.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct StaticConfig {
    /// Key identifier (name) for the single configured key
    pub key_id: String,
    /// Base64-encoded 32-byte AES-256 key material (zeroed on drop)
    #[serde(skip_serializing, default)]
    pub secret_key: String,
}

impl Drop for StaticConfig {
    fn drop(&mut self) {
        use zeroize::Zeroize;

        self.secret_key.zeroize();
    }
}

impl fmt::Debug for StaticConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticConfig")
            .field("key_id", &self.key_id)
            .field("secret_key", &redacted_secret(&self.secret_key))
            .finish()
    }
}

impl StaticConfig {
    /// Decode the base64-encoded secret key into raw bytes.
    /// Returns an error if the key is not valid base64 or is not exactly 32 bytes.
    pub fn decode_key(&self) -> Result<[u8; 32]> {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&self.secret_key)
            .map_err(|e| KmsError::configuration_error(format!("Static KMS secret key is not valid base64: {e}")))?;
        if bytes.len() != 32 {
            return Err(KmsError::configuration_error(format!(
                "Static KMS secret key must be exactly 32 bytes after base64 decoding, got {} bytes",
                bytes.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(key)
    }
}

/// Vault KV v2 backend configuration.
///
/// Key material and metadata are stored directly in KV v2; any identity with KV read
/// access to the key path can recover plaintext master key material. Use the Vault
/// Transit backend when cryptographic isolation of key material is required.
#[derive(Clone, Serialize, Deserialize)]
pub struct VaultConfig {
    /// Vault server URL
    pub address: String,
    /// Authentication method
    pub auth_method: VaultAuthMethod,
    /// Vault namespace (Vault Enterprise)
    pub namespace: Option<String>,
    /// Deprecated: legacy Transit engine mount path. The Vault KV2 backend never calls
    /// the Transit engine, so this value is unused; the field is retained (and
    /// defaulted) only so previously persisted configurations keep deserializing.
    #[serde(default = "default_vault_kv2_mount_path")]
    pub mount_path: String,
    /// KV engine mount path for storing keys
    pub kv_mount: String,
    /// Path prefix for keys in KV store
    pub key_path_prefix: String,
    /// TLS configuration
    pub tls: Option<TlsConfig>,
}

impl fmt::Debug for VaultConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VaultConfig")
            .field("address", &self.address)
            .field("auth_method", &self.auth_method)
            .field("namespace", &self.namespace)
            .field("mount_path", &self.mount_path)
            .field("kv_mount", &self.kv_mount)
            .field("key_path_prefix", &self.key_path_prefix)
            .field("tls", &self.tls)
            .finish()
    }
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

/// Vault Transit backend configuration
#[derive(Clone, Serialize, Deserialize)]
pub struct VaultTransitConfig {
    /// Vault server URL
    pub address: String,
    /// Authentication method
    pub auth_method: VaultAuthMethod,
    /// Vault namespace (Vault Enterprise)
    pub namespace: Option<String>,
    /// Transit engine mount path
    pub mount_path: String,
    /// KV v2 mount path for persisting transit key metadata
    #[serde(default = "default_vault_transit_metadata_kv_mount")]
    pub metadata_kv_mount: String,
    /// Key path prefix under metadata_kv_mount for transit key metadata storage
    #[serde(default = "default_vault_transit_metadata_key_prefix")]
    pub metadata_key_prefix: String,
    /// TLS configuration
    pub tls: Option<TlsConfig>,
}

impl fmt::Debug for VaultTransitConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VaultTransitConfig")
            .field("address", &self.address)
            .field("auth_method", &self.auth_method)
            .field("namespace", &self.namespace)
            .field("mount_path", &self.mount_path)
            .field("metadata_kv_mount", &self.metadata_kv_mount)
            .field("metadata_key_prefix", &self.metadata_key_prefix)
            .field("tls", &self.tls)
            .finish()
    }
}

impl Default for VaultTransitConfig {
    fn default() -> Self {
        Self {
            address: "http://localhost:8200".to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "dev-token".to_string(),
            },
            namespace: None,
            mount_path: "transit".to_string(),
            metadata_kv_mount: default_vault_transit_metadata_kv_mount(),
            metadata_key_prefix: default_vault_transit_metadata_key_prefix(),
            tls: None,
        }
    }
}

/// Vault authentication methods
#[derive(Clone, Serialize, Deserialize)]
pub enum VaultAuthMethod {
    /// Token authentication
    Token { token: String },
    /// AppRole authentication: login with `role_id` + `secret_id` for a
    /// lease-bound token that is renewed in the background.
    AppRole {
        role_id: String,
        /// Inline secret_id; used only when `secret_id_file` is unset.
        secret_id: String,
        /// Path to a file holding the secret_id. Re-read on every login so an
        /// externally rotated secret_id is picked up; takes precedence over the
        /// inline value.
        #[serde(default)]
        secret_id_file: Option<PathBuf>,
        /// AppRole auth engine mount path.
        #[serde(default = "default_vault_approle_mount")]
        mount: String,
        /// Fail-closed margin in seconds: once the current token is within this
        /// window of expiry without a successful refresh, requests are refused
        /// instead of sent with a token that may lapse mid-flight. Defaults to
        /// the per-attempt timeout.
        #[serde(default)]
        refresh_safety_window_secs: Option<u64>,
    },
    /// Kubernetes authentication: the pod's ServiceAccount token is exchanged
    /// for a lease-bound Vault token that is renewed in the background.
    Kubernetes {
        /// Vault role bound to this ServiceAccount.
        role: String,
        /// Kubernetes auth engine mount path.
        #[serde(default = "default_vault_kubernetes_mount")]
        mount: String,
        /// Projected ServiceAccount token to present. Re-read on every login so
        /// a token the kubelet rotates is picked up without a restart.
        #[serde(default = "default_vault_kubernetes_jwt_path")]
        jwt_path: PathBuf,
        /// Fail-closed margin in seconds, as on `AppRole`. Defaults to the
        /// per-attempt timeout.
        #[serde(default)]
        refresh_safety_window_secs: Option<u64>,
    },
    /// Agent-managed token file (for example a Vault Agent auto-auth sink):
    /// the token is read from `path` and re-read periodically so a token
    /// rotated by the agent is picked up without a restart.
    TokenFile {
        path: PathBuf,
        /// Seconds between token file re-reads. Each successful read also
        /// extends the token's observed validity to twice this value, so a
        /// file that stops being readable eventually trips the fail-closed
        /// window. Defaults to 30 seconds.
        #[serde(default)]
        poll_interval_secs: Option<u64>,
        /// Fail-closed margin in seconds, as on `AppRole`. Defaults to the
        /// per-attempt timeout.
        #[serde(default)]
        refresh_safety_window_secs: Option<u64>,
    },
}

impl VaultAuthMethod {
    /// AppRole authentication with the default mount and no secret-id file.
    pub fn approle(role_id: String, secret_id: String) -> Self {
        Self::AppRole {
            role_id,
            secret_id,
            secret_id_file: None,
            mount: default_vault_approle_mount(),
            refresh_safety_window_secs: None,
        }
    }

    /// Kubernetes authentication with the default mount and projected token path.
    pub fn kubernetes(role: String) -> Self {
        Self::Kubernetes {
            role,
            mount: default_vault_kubernetes_mount(),
            jwt_path: default_vault_kubernetes_jwt_path(),
            refresh_safety_window_secs: None,
        }
    }

    /// Agent-managed token file with the default poll interval.
    pub fn token_file(path: PathBuf) -> Self {
        Self::TokenFile {
            path,
            poll_interval_secs: None,
            refresh_safety_window_secs: None,
        }
    }
}

impl fmt::Debug for VaultAuthMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token { token } => f.debug_struct("Token").field("token", &redacted_secret(token)).finish(),
            Self::AppRole {
                role_id,
                secret_id,
                secret_id_file,
                mount,
                refresh_safety_window_secs,
            } => f
                .debug_struct("AppRole")
                .field("role_id", role_id)
                .field("secret_id", &redacted_secret(secret_id))
                .field("secret_id_file", secret_id_file)
                .field("mount", mount)
                .field("refresh_safety_window_secs", refresh_safety_window_secs)
                .finish(),
            // No redaction: the role and mount name a Vault binding, and the
            // ServiceAccount token itself is never held on this type.
            Self::Kubernetes {
                role,
                mount,
                jwt_path,
                refresh_safety_window_secs,
            } => f
                .debug_struct("Kubernetes")
                .field("role", role)
                .field("mount", mount)
                .field("jwt_path", jwt_path)
                .field("refresh_safety_window_secs", refresh_safety_window_secs)
                .finish(),
            Self::TokenFile {
                path,
                poll_interval_secs,
                refresh_safety_window_secs,
            } => f
                .debug_struct("TokenFile")
                .field("path", path)
                .field("poll_interval_secs", poll_interval_secs)
                .field("refresh_safety_window_secs", refresh_safety_window_secs)
                .finish(),
        }
    }
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
    /// Lifetime of a cached key metadata entry.
    ///
    /// This bounds how long a describe can answer from metadata that another
    /// node has since changed (disable, schedule-deletion); encrypt, decrypt
    /// and data key generation never read the cache. Values above 24 hours are
    /// clamped at use (see [`CacheConfig::effective_ttl`]).
    pub ttl: Duration,
    /// Publish the `rustfs_kms_metadata_cache_*` metrics.
    ///
    /// Only metrics-recorder output is gated: the counters behind the admin
    /// status API are maintained either way.
    pub enable_metrics: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_keys: DEFAULT_MAX_CACHED_KEYS,
            ttl: DEFAULT_CACHE_TTL,
            enable_metrics: true,
        }
    }
}

impl CacheConfig {
    /// Metadata lifetime with the configured value clamped to the supported maximum.
    ///
    /// This is the value the cache is built with and the value reported back to
    /// operators, so what the admin API advertises is what the cache does.
    pub fn effective_ttl(&self) -> Duration {
        self.ttl.min(MAX_CACHE_TTL)
    }
}

/// AWS KMS backend configuration.
///
/// Deliberately holds no credential material: the backend resolves credentials
/// through the standard `aws-config` provider chain (environment, shared
/// profile, container/IMDS role), so RustFS never stores, persists, or redacts
/// AWS secrets of its own.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AwsKmsConfig {
    /// AWS region hosting the KMS keys. When unset, the region is resolved by
    /// the standard chain (`AWS_REGION`, profile, IMDS).
    #[serde(default)]
    pub region: Option<String>,
    /// Override for the KMS endpoint, for local emulators and private
    /// endpoints. Unset in production, where the SDK derives the regional
    /// endpoint.
    #[serde(default)]
    pub endpoint_url: Option<String>,
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

    /// Create a new KMS configuration for the Vault KV v2 backend with token authentication.
    ///
    /// Master key material is stored directly in Vault KV v2: confidentiality relies on
    /// Vault ACLs, KV v2 at-rest encryption, and TLS. KV read access to the key path is
    /// equivalent to holding the plaintext master keys. Use [`KmsConfig::vault_transit`]
    /// when key material must never be readable through Vault storage APIs.
    pub fn vault(address: Url, token: String) -> Self {
        Self {
            backend: KmsBackend::VaultKv2,
            backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
                address: address.to_string(),
                auth_method: VaultAuthMethod::Token { token },
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    /// Create a new KMS configuration for the Vault KV v2 backend with AppRole authentication.
    ///
    /// Shares the security boundary described on [`KmsConfig::vault`]: key material lives
    /// in KV v2 and is protected only by Vault ACLs and KV v2 at-rest encryption.
    pub fn vault_approle(address: Url, role_id: String, secret_id: String) -> Self {
        Self {
            backend: KmsBackend::VaultKv2,
            backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
                address: address.to_string(),
                auth_method: VaultAuthMethod::approle(role_id, secret_id),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    /// Create a new KMS configuration for Vault Transit backend with token authentication
    pub fn vault_transit(address: Url, token: String) -> Self {
        Self {
            backend: KmsBackend::VaultTransit,
            backend_config: BackendConfig::VaultTransit(Box::new(VaultTransitConfig {
                address: address.to_string(),
                auth_method: VaultAuthMethod::Token { token },
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    /// Create a new KMS configuration for static single-key backend
    ///
    /// # Arguments
    /// * `key_id` - The key identifier (name) for the configured key
    /// * `secret_key` - Base64-encoded 32-byte AES-256 key material
    pub fn static_kms(key_id: String, secret_key: String) -> Self {
        Self {
            backend: KmsBackend::Static,
            backend_config: BackendConfig::Static(StaticConfig {
                key_id: key_id.clone(),
                secret_key,
            }),
            default_key_id: Some(key_id),
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

    /// Get the Vault KV2 configuration if backend is VaultKv2
    pub fn vault_config(&self) -> Option<&VaultConfig> {
        match &self.backend_config {
            BackendConfig::VaultKv2(config) => Some(config),
            _ => None,
        }
    }

    /// Get the Vault Transit configuration if backend is VaultTransit
    pub fn vault_transit_config(&self) -> Option<&VaultTransitConfig> {
        match &self.backend_config {
            BackendConfig::VaultTransit(config) => Some(config),
            _ => None,
        }
    }

    /// Get the static configuration if backend is Static
    pub fn static_config(&self) -> Option<&StaticConfig> {
        match &self.backend_config {
            BackendConfig::Static(config) => Some(config),
            _ => None,
        }
    }

    /// Create a new KMS configuration for the AWS KMS backend.
    ///
    /// Credentials are resolved by the standard `aws-config` provider chain;
    /// only the region is configured here.
    pub fn aws(region: Option<String>) -> Self {
        Self {
            backend: KmsBackend::Aws,
            backend_config: BackendConfig::Aws(Box::new(AwsKmsConfig {
                region,
                endpoint_url: None,
            })),
            ..Default::default()
        }
    }

    /// Get the AWS configuration if backend is AWS KMS
    pub fn aws_kms_config(&self) -> Option<&AwsKmsConfig> {
        match &self.backend_config {
            BackendConfig::Aws(config) => Some(config),
            _ => None,
        }
    }

    /// Set default key ID
    pub fn with_default_key(mut self, key_id: String) -> Self {
        self.default_key_id = Some(key_id);
        self
    }

    /// Explicitly allow development-only KMS defaults.
    pub fn with_insecure_development_defaults(mut self) -> Self {
        self.allow_insecure_dev_defaults = true;
        self
    }

    /// Explicitly allow deletions that bypass the pending-deletion waiting window.
    pub fn with_immediate_deletion_allowed(mut self) -> Self {
        self.allow_immediate_deletion = true;
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

    /// Per-attempt timeout with the configured value clamped to the supported maximum.
    pub(crate) fn effective_timeout(&self) -> Duration {
        self.timeout.min(MAX_OPERATION_TIMEOUT)
    }

    /// Retry attempts with the configured value clamped to the supported maximum.
    pub(crate) fn effective_retry_attempts(&self) -> u32 {
        self.retry_attempts.min(MAX_RETRY_ATTEMPTS)
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

        // Oversized values are clamped at use (not rejected) so pre-existing
        // configurations cannot keep the service from starting after upgrade.
        if self.timeout > MAX_OPERATION_TIMEOUT {
            tracing::warn!(
                configured_secs = self.timeout.as_secs(),
                max_secs = MAX_OPERATION_TIMEOUT.as_secs(),
                "KMS timeout exceeds the supported maximum; backend operations clamp it to the maximum"
            );
        }
        if self.retry_attempts > MAX_RETRY_ATTEMPTS {
            tracing::warn!(
                configured = self.retry_attempts,
                max = MAX_RETRY_ATTEMPTS,
                "KMS retry_attempts exceeds the supported maximum; backend operations clamp it to the maximum"
            );
        }

        // Validate backend-specific configuration
        match &self.backend_config {
            BackendConfig::Local(config) => {
                if !config.key_dir.is_absolute() {
                    return Err(KmsError::configuration_error("Local key directory must be an absolute path"));
                }

                if !self.allow_insecure_dev_defaults {
                    if config.master_key.as_deref().is_none_or(str::is_empty) {
                        return Err(development_default_error(
                            "Local KMS requires a master key outside explicit development mode",
                        ));
                    }

                    if is_under_temp_dir(&config.key_dir) {
                        return Err(development_default_error(
                            "Local KMS key directory must not be under the process temp directory outside explicit development mode",
                        ));
                    }
                }
            }
            BackendConfig::VaultKv2(config) => {
                if !config.address.starts_with("http://") && !config.address.starts_with("https://") {
                    return Err(KmsError::configuration_error("Vault KV2 address must use http or https scheme"));
                }

                validate_vault_auth_method("Vault KV2", &config.auth_method)?;

                if !self.allow_insecure_dev_defaults {
                    validate_vault_development_defaults("Vault KV2", &config.address, &config.auth_method, config.tls.as_ref())?;
                }

                // `mount_path` is deprecated and unused by this backend, so an empty value
                // is deliberately not an error.

                // `kv_mount` is: it is the mount every read, write and listing is
                // routed through, and an empty one produces a path Vault has no
                // handler for. Rejecting it here names the setting; letting it
                // through spends a round-trip to report an unroutable path.
                if config.kv_mount.is_empty() {
                    return Err(KmsError::configuration_error("Vault KV2 mount cannot be empty"));
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
            BackendConfig::VaultTransit(config) => {
                if !config.address.starts_with("http://") && !config.address.starts_with("https://") {
                    return Err(KmsError::configuration_error("Vault Transit address must use http or https scheme"));
                }

                validate_vault_auth_method("Vault Transit", &config.auth_method)?;

                if !self.allow_insecure_dev_defaults {
                    validate_vault_development_defaults(
                        "Vault Transit",
                        &config.address,
                        &config.auth_method,
                        config.tls.as_ref(),
                    )?;
                }

                if config.mount_path.is_empty() {
                    return Err(KmsError::configuration_error("Vault Transit mount path cannot be empty"));
                }

                if config.metadata_kv_mount.is_empty() {
                    return Err(KmsError::configuration_error("Vault Transit metadata KV mount cannot be empty"));
                }

                if config.metadata_key_prefix.is_empty() {
                    return Err(KmsError::configuration_error("Vault Transit metadata key prefix cannot be empty"));
                }

                if config.address.starts_with("https://")
                    && let Some(ref tls) = config.tls
                    && !tls.skip_verify
                    && tls.ca_cert_path.is_none()
                    && tls.client_cert_path.is_none()
                {
                    tracing::warn!("Using HTTPS without custom TLS configuration - relying on system CA");
                }
            }
            BackendConfig::Static(config) => {
                if config.key_id.is_empty() {
                    return Err(KmsError::configuration_error("Static KMS key_id cannot be empty"));
                }
                if config.secret_key.is_empty() {
                    return Err(KmsError::configuration_error("Static KMS secret_key cannot be empty"));
                }
                // Validate that the key can be decoded (right length, valid base64)
                config.decode_key()?;
            }
            BackendConfig::Aws(config) => {
                if let Some(region) = &config.region
                    && region.is_empty()
                {
                    return Err(KmsError::configuration_error("AWS KMS region cannot be empty when set"));
                }

                if let Some(endpoint) = &config.endpoint_url {
                    if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                        return Err(KmsError::configuration_error("AWS KMS endpoint URL must use http or https scheme"));
                    }
                    // A plaintext endpoint override exposes every KMS request,
                    // including plaintext data keys, so it stays gated on the
                    // explicit development opt-in.
                    if endpoint.starts_with("http://") && !self.allow_insecure_dev_defaults {
                        return Err(development_default_error("AWS KMS endpoint URL must use https"));
                    }
                }
            }
        }

        // Validate cache configuration
        if self.enable_cache {
            if self.cache_config.max_keys == 0 {
                return Err(KmsError::configuration_error("Cache max_keys must be greater than 0"));
            }

            // A zero TTL expires every entry on insert, which is a cache that
            // cannot serve anything; reject it instead of silently disabling
            // the cache the operator asked for.
            if self.cache_config.ttl.is_zero() {
                return Err(KmsError::configuration_error("Cache ttl must be greater than 0"));
            }
        }

        Ok(())
    }

    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();

        // Backend type
        if let Some(backend_type) = get_env_opt_str("RUSTFS_KMS_BACKEND") {
            config.backend = match backend_type.to_lowercase().as_str() {
                "local" => KmsBackend::Local,
                "vault" | "vault-kv2" | "vault_kv2" => KmsBackend::VaultKv2,
                "vault-transit" | "vault_transit" => KmsBackend::VaultTransit,
                "static" => KmsBackend::Static,
                "aws" | "aws-kms" | "aws_kms" => KmsBackend::Aws,
                _ => return Err(KmsError::configuration_error(format!("Unknown KMS backend: {backend_type}"))),
            };
        }

        // Default key ID
        if let Some(key_id) = get_env_opt_str("RUSTFS_KMS_DEFAULT_KEY_ID") {
            config.default_key_id = Some(key_id);
        }

        // Timeout
        if let Some(timeout_str) = get_env_opt_str("RUSTFS_KMS_TIMEOUT_SECS") {
            let timeout_secs = timeout_str
                .parse::<u64>()
                .map_err(|_| KmsError::configuration_error("Invalid timeout value"))?;
            config.timeout = Duration::from_secs(timeout_secs);
        }

        // Retry attempts
        if let Some(retries_str) = get_env_opt_str("RUSTFS_KMS_RETRY_ATTEMPTS") {
            config.retry_attempts = retries_str
                .parse()
                .map_err(|_| KmsError::configuration_error("Invalid retry attempts value"))?;
        }

        // Enable cache
        config.enable_cache = get_env_bool("RUSTFS_KMS_ENABLE_CACHE", config.enable_cache);
        config.allow_insecure_dev_defaults =
            get_env_bool(ENV_KMS_ALLOW_INSECURE_DEV_DEFAULTS, config.allow_insecure_dev_defaults);
        config.allow_immediate_deletion = get_env_bool(ENV_KMS_ALLOW_IMMEDIATE_DELETION, config.allow_immediate_deletion);

        // Backend-specific configuration
        match config.backend {
            KmsBackend::Local => {
                let key_dir = get_env_str("RUSTFS_KMS_LOCAL_KEY_DIR", "./kms_keys");
                let master_key = get_env_opt_str("RUSTFS_KMS_LOCAL_MASTER_KEY");

                config.backend_config = BackendConfig::Local(LocalConfig {
                    key_dir: PathBuf::from(key_dir),
                    master_key,
                    file_permissions: Some(0o600),
                });
            }
            KmsBackend::VaultKv2 => {
                config.backend_config =
                    BackendConfig::VaultKv2(Box::new(vault_kv2_config_from_env(VaultCliOverrides::default())?));
            }
            KmsBackend::VaultTransit => {
                config.backend_config =
                    BackendConfig::VaultTransit(Box::new(vault_transit_config_from_env(VaultCliOverrides::default())?));
            }
            KmsBackend::Static => {
                // Read from file first, then fall back to direct env var
                let secret_str = if let Some(file_path) = get_env_opt_str(ENV_KMS_STATIC_SECRET_KEY_FILE) {
                    std::fs::read_to_string(&file_path).map_err(|e| {
                        KmsError::configuration_error(format!("Failed to read static KMS secret key file {file_path}: {e}"))
                    })?
                } else {
                    get_env_str(ENV_KMS_STATIC_SECRET_KEY, "")
                };

                let secret_str = secret_str.trim().to_string();
                if secret_str.is_empty() {
                    return Err(KmsError::configuration_error(format!(
                        "Static KMS requires {ENV_KMS_STATIC_SECRET_KEY} or {ENV_KMS_STATIC_SECRET_KEY_FILE} to be set"
                    )));
                }

                // Parse format: <key-id>:<base64-key>
                let colon_pos = secret_str.find(':').ok_or_else(|| {
                    KmsError::configuration_error("Static KMS secret key must be in format <key-name>:<base64-key>")
                })?;
                let key_id = secret_str[..colon_pos].to_string();
                let secret_key = secret_str[colon_pos + 1..].to_string();

                if key_id.is_empty() {
                    return Err(KmsError::configuration_error(
                        "Static KMS key name must not be empty in secret key string",
                    ));
                }
                if secret_key.is_empty() {
                    return Err(KmsError::configuration_error(
                        "Static KMS base64 key must not be empty in secret key string",
                    ));
                }

                config.backend_config = BackendConfig::Static(StaticConfig {
                    key_id: key_id.clone(),
                    secret_key,
                });
                config.default_key_id = Some(key_id);
            }
            KmsBackend::Aws => {
                // Only non-credential settings are read here; access keys,
                // profiles, and role assumption stay with the aws-config chain.
                config.backend_config = BackendConfig::Aws(Box::new(AwsKmsConfig {
                    region: get_env_opt_str(ENV_KMS_AWS_REGION),
                    endpoint_url: get_env_opt_str(ENV_KMS_AWS_ENDPOINT_URL),
                }));
            }
        }

        config.validate()?;
        Ok(config)
    }
}

/// Read the immediate-deletion gate from the environment.
///
/// Callers that assemble a [`KmsConfig`] field by field instead of going
/// through [`KmsConfig::from_env`] use this, so the gate keeps one name, one
/// default, and one place to look it up.
pub fn allow_immediate_deletion_from_env() -> bool {
    get_env_bool(ENV_KMS_ALLOW_IMMEDIATE_DELETION, false)
}

impl crate::persisted_observability::UnknownFieldSummary {
    fn record_for_kms_config(&self) {
        let Some((field, field_name_truncated, field_count)) = self.record("kms-config") else {
            return;
        };

        static RECORDS_WITH_UNKNOWN_FIELDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let observed_records = RECORDS_WITH_UNKNOWN_FIELDS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        if observed_records.is_power_of_two() {
            tracing::warn!(
                field = ?field,
                field_name_truncated,
                field_count,
                observed_records,
                "persisted KMS configuration contains unknown fields"
            );
        }
    }
}

/// Deserialize a persisted KMS configuration, observing ignored fields.
///
/// The persisted configuration deliberately tolerates unknown fields — a
/// rolling upgrade writes fields the previous build does not know, and
/// rejecting them would turn every upgrade into a hard stop (see the
/// regression test pinning that tolerance). Tolerated must not mean
/// invisible: this loader wraps the deserializer with `serde_ignored`, so
/// every field the configuration silently dropped is counted and sampled
/// into a warning, per the repository rule that formats too
/// compatibility-bound for `deny_unknown_fields` must at least log unknown
/// fields. Only field paths are recorded, never values — a mistyped field
/// name can sit next to a secret.
pub fn kms_config_from_persisted_json(data: &[u8]) -> serde_json::Result<KmsConfig> {
    use crate::persisted_observability::{BoundedUnknownFieldName, UnknownFieldSummary};

    let mut deserializer = serde_json::Deserializer::from_slice(data);
    let mut unknown_fields = UnknownFieldSummary::default();
    let config: KmsConfig = serde_ignored::deserialize(&mut deserializer, |path| {
        unknown_fields.observe(BoundedUnknownFieldName::new(&path.to_string()));
    })?;
    deserializer.end()?;
    unknown_fields.record_for_kms_config();
    Ok(config)
}

fn vault_tls_config(skip_tls_verify: bool) -> Option<TlsConfig> {
    skip_tls_verify.then_some(TlsConfig {
        ca_cert_path: None,
        client_cert_path: None,
        client_key_path: None,
        skip_verify: true,
    })
}

fn development_default_error(reason: &str) -> KmsError {
    KmsError::configuration_error(format!("{reason}; set {ENV_KMS_ALLOW_INSECURE_DEV_DEFAULTS}=true only for development"))
}

fn is_under_temp_dir(path: &Path) -> bool {
    path.starts_with(std::env::temp_dir())
}

/// Command-line values that take precedence over the matching environment
/// variables when assembling a Vault backend configuration.
///
/// Every field has a `RUSTFS_KMS_VAULT_*` equivalent that the CLI layer already
/// reads, so these are only set when the operator passed an explicit flag.
///
/// Deliberately not `Debug`: `token` holds the raw Vault token, and the
/// redacting `Debug` impls elsewhere in this module exist because a derived one
/// would print it. Denying the derive makes a future `{overrides:?}` a compile
/// error instead of a leak.
#[derive(Default, Clone, Copy)]
pub struct VaultCliOverrides<'a> {
    pub address: Option<&'a str>,
    pub token: Option<&'a str>,
    pub mount_path: Option<&'a str>,
}

/// Assemble the Vault KV2 backend configuration from the environment.
///
/// Shared by [`KmsConfig::from_env`] and the server's command-line startup path
/// so both resolve the same auth method, namespace, TLS and mount settings.
pub fn vault_kv2_config_from_env(overrides: VaultCliOverrides<'_>) -> Result<VaultConfig> {
    let mount_path = match overrides
        .mount_path
        .map(str::to_string)
        .or_else(|| get_env_opt_str(ENV_KMS_VAULT_MOUNT_PATH))
    {
        Some(path) => {
            tracing::warn!(
                "RUSTFS_KMS_VAULT_MOUNT_PATH is deprecated for the Vault KV2 backend: it never calls the Transit engine and the value is stored but unused"
            );
            path
        }
        None => default_vault_kv2_mount_path(),
    };

    Ok(VaultConfig {
        address: vault_address_from_env(overrides.address),
        auth_method: vault_auth_method_from_env(overrides.token)?,
        namespace: get_env_opt_str(ENV_KMS_VAULT_NAMESPACE),
        mount_path,
        kv_mount: get_env_str("RUSTFS_KMS_VAULT_KV_MOUNT", "secret"),
        key_path_prefix: get_env_str("RUSTFS_KMS_VAULT_KEY_PREFIX", "rustfs/kms/keys"),
        tls: vault_tls_config(get_env_bool(ENV_KMS_VAULT_SKIP_TLS_VERIFY, false)),
    })
}

/// Assemble the Vault Transit backend configuration from the environment.
///
/// Companion to [`vault_kv2_config_from_env`]; see there for why both entry
/// points share it.
pub fn vault_transit_config_from_env(overrides: VaultCliOverrides<'_>) -> Result<VaultTransitConfig> {
    Ok(VaultTransitConfig {
        address: vault_address_from_env(overrides.address),
        auth_method: vault_auth_method_from_env(overrides.token)?,
        namespace: get_env_opt_str(ENV_KMS_VAULT_NAMESPACE),
        mount_path: overrides
            .mount_path
            .map(str::to_string)
            .unwrap_or_else(|| get_env_str(ENV_KMS_VAULT_MOUNT_PATH, "transit")),
        metadata_kv_mount: get_env_str(ENV_KMS_VAULT_TRANSIT_METADATA_KV_MOUNT, DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT),
        metadata_key_prefix: get_env_str(ENV_KMS_VAULT_TRANSIT_METADATA_PREFIX, DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX),
        tls: vault_tls_config(get_env_bool(ENV_KMS_VAULT_SKIP_TLS_VERIFY, false)),
    })
}

fn vault_address_from_env(override_value: Option<&str>) -> String {
    override_value
        .map(str::to_string)
        .unwrap_or_else(|| get_env_str(ENV_KMS_VAULT_ADDRESS, "http://localhost:8200"))
}

/// Resolve the Vault auth method from environment variables.
///
/// Setting `RUSTFS_KMS_VAULT_APPROLE_ROLE_ID` selects AppRole authentication;
/// the secret_id then comes from `RUSTFS_KMS_VAULT_APPROLE_SECRET_ID_FILE`
/// (re-read on every login, mirroring the `RUSTFS_KMS_STATIC_SECRET_KEY_FILE`
/// precedent) or inline from `RUSTFS_KMS_VAULT_APPROLE_SECRET_ID`, with the
/// file taking precedence. Without a role id the legacy token flow applies.
///
/// `RUSTFS_KMS_VAULT_KUBERNETES_ROLE` selects Kubernetes authentication, which
/// presents the pod's projected ServiceAccount token.
///
/// `token_override` carries a token supplied on the command line; it stands in
/// for `RUSTFS_KMS_VAULT_TOKEN` everywhere below, including the conflict checks,
/// so a flag and the variable it mirrors select the same method.
fn vault_auth_method_from_env(token_override: Option<&str>) -> Result<VaultAuthMethod> {
    let token = token_override
        .map(str::to_string)
        .or_else(|| get_env_opt_str(ENV_KMS_VAULT_TOKEN));
    let role_id = get_env_opt_str(ENV_KMS_VAULT_APPROLE_ROLE_ID);
    let kubernetes_role = get_env_opt_str(ENV_KMS_VAULT_KUBERNETES_ROLE);

    if let Some(token_file) = get_env_opt_str(ENV_KMS_VAULT_TOKEN_FILE) {
        // A token file names one authoritative credential source; combining it
        // with another one would leave the effective identity ambiguous, so
        // that is a configuration error rather than a precedence rule.
        for (name, configured) in [
            (ENV_KMS_VAULT_APPROLE_ROLE_ID, role_id.is_some()),
            (ENV_KMS_VAULT_KUBERNETES_ROLE, kubernetes_role.is_some()),
            (ENV_KMS_VAULT_TOKEN, token.is_some()),
        ] {
            if configured {
                return Err(KmsError::configuration_error(format!(
                    "{ENV_KMS_VAULT_TOKEN_FILE} cannot be combined with {name}; configure exactly one Vault auth method"
                )));
            }
        }
        return Ok(VaultAuthMethod::token_file(PathBuf::from(token_file)));
    }

    if let Some(role) = kubernetes_role {
        // Unlike a leftover static token, a second login method is never a
        // stale remnant: both were configured deliberately and neither can be
        // ranked over the other.
        if role_id.is_some() {
            return Err(KmsError::configuration_error(format!(
                "{ENV_KMS_VAULT_KUBERNETES_ROLE} cannot be combined with {ENV_KMS_VAULT_APPROLE_ROLE_ID}; configure exactly one Vault auth method"
            )));
        }
        return Ok(VaultAuthMethod::Kubernetes {
            role,
            mount: get_env_str(ENV_KMS_VAULT_KUBERNETES_MOUNT, DEFAULT_VAULT_KUBERNETES_MOUNT),
            jwt_path: get_env_opt_str(ENV_KMS_VAULT_KUBERNETES_JWT_PATH)
                .map_or_else(default_vault_kubernetes_jwt_path, PathBuf::from),
            refresh_safety_window_secs: None,
        });
    }

    let Some(role_id) = role_id else {
        return Ok(VaultAuthMethod::Token {
            token: token.unwrap_or_else(|| "dev-token".to_string()),
        });
    };

    let secret_id_file = get_env_opt_str(ENV_KMS_VAULT_APPROLE_SECRET_ID_FILE).map(PathBuf::from);
    let secret_id = get_env_opt_str(ENV_KMS_VAULT_APPROLE_SECRET_ID).unwrap_or_default();
    if secret_id.is_empty() && secret_id_file.is_none() {
        return Err(KmsError::configuration_error(format!(
            "Vault AppRole requires {ENV_KMS_VAULT_APPROLE_SECRET_ID} or {ENV_KMS_VAULT_APPROLE_SECRET_ID_FILE} to be set"
        )));
    }

    Ok(VaultAuthMethod::AppRole {
        role_id,
        secret_id,
        secret_id_file,
        mount: get_env_str(ENV_KMS_VAULT_APPROLE_MOUNT, DEFAULT_VAULT_APPROLE_MOUNT),
        refresh_safety_window_secs: None,
    })
}

fn validate_vault_auth_method(backend_name: &str, auth_method: &VaultAuthMethod) -> Result<()> {
    match auth_method {
        VaultAuthMethod::Token { .. } => Ok(()),
        VaultAuthMethod::AppRole {
            role_id,
            secret_id,
            secret_id_file,
            mount,
            ..
        } => {
            if role_id.is_empty() {
                return Err(KmsError::configuration_error(format!("{backend_name} AppRole role_id cannot be empty")));
            }
            if secret_id.is_empty() && secret_id_file.is_none() {
                return Err(KmsError::configuration_error(format!(
                    "{backend_name} AppRole requires a secret_id or a secret_id_file"
                )));
            }
            if mount.is_empty() {
                return Err(KmsError::configuration_error(format!("{backend_name} AppRole mount cannot be empty")));
            }
            Ok(())
        }
        VaultAuthMethod::Kubernetes {
            role, mount, jwt_path, ..
        } => {
            if role.is_empty() {
                return Err(KmsError::configuration_error(format!("{backend_name} Kubernetes role cannot be empty")));
            }
            if mount.is_empty() {
                return Err(KmsError::configuration_error(format!("{backend_name} Kubernetes mount cannot be empty")));
            }
            if jwt_path.as_os_str().is_empty() {
                return Err(KmsError::configuration_error(format!(
                    "{backend_name} Kubernetes ServiceAccount token path cannot be empty"
                )));
            }
            Ok(())
        }
        VaultAuthMethod::TokenFile {
            path,
            poll_interval_secs,
            ..
        } => {
            if path.as_os_str().is_empty() {
                return Err(KmsError::configuration_error(format!("{backend_name} token file path cannot be empty")));
            }
            if poll_interval_secs == &Some(0) {
                return Err(KmsError::configuration_error(format!(
                    "{backend_name} token file poll interval must be greater than 0"
                )));
            }
            Ok(())
        }
    }
}

fn validate_vault_development_defaults(
    backend_name: &str,
    address: &str,
    auth_method: &VaultAuthMethod,
    tls: Option<&TlsConfig>,
) -> Result<()> {
    if address.starts_with("http://") {
        return Err(development_default_error(&format!(
            "{backend_name} requires HTTPS outside explicit development mode"
        )));
    }

    if matches!(auth_method, VaultAuthMethod::Token { token } if token == "dev-token") {
        return Err(development_default_error(&format!(
            "{backend_name} default dev-token is not allowed outside explicit development mode"
        )));
    }

    if tls.is_some_and(|tls| tls.skip_verify) {
        return Err(development_default_error(&format!(
            "{backend_name} skip TLS verification is not allowed outside explicit development mode"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_security_governance::validate_redaction_rules;
    use temp_env::with_vars;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = KmsConfig::default();
        assert_eq!(config.backend, KmsBackend::Local);
        assert!(config.validate().is_err());
        assert!(config.with_insecure_development_defaults().validate().is_ok());
    }

    #[test]
    fn test_local_config() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();

        assert_eq!(config.backend, KmsBackend::Local);
        assert!(config.validate().is_ok());

        let local_config = config.local_config().expect("Should have local config");
        assert_eq!(local_config.key_dir, temp_dir.path());
    }

    #[test]
    fn oversized_cache_ttl_is_clamped_not_rejected() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();

        assert_eq!(config.cache_config.ttl, DEFAULT_CACHE_TTL);
        assert_eq!(config.cache_config.effective_ttl(), DEFAULT_CACHE_TTL);

        // An oversized lifetime must not keep the service from starting, and
        // must not reach moka, whose builder panics beyond 1000 years.
        let config = KmsConfig {
            cache_config: CacheConfig {
                ttl: Duration::from_secs(u64::MAX),
                ..Default::default()
            },
            ..config
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.cache_config.effective_ttl(), MAX_CACHE_TTL);

        // In-range values pass through unchanged.
        let config = KmsConfig {
            cache_config: CacheConfig {
                ttl: Duration::from_secs(600),
                ..Default::default()
            },
            ..config
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.cache_config.effective_ttl(), Duration::from_secs(600));
    }

    #[test]
    fn zero_cache_ttl_is_rejected_only_while_caching_is_enabled() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = KmsConfig {
            cache_config: CacheConfig {
                ttl: Duration::ZERO,
                ..Default::default()
            },
            ..KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults()
        };

        // A cache that expires every entry on insert is a misconfiguration, not
        // a way to turn caching off.
        assert!(config.validate().is_err());

        let config = KmsConfig {
            enable_cache: false,
            ..config
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_oversized_timeout_and_retries_clamped_not_rejected() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = KmsConfig {
            timeout: Duration::from_secs(3_600),
            retry_attempts: 50,
            ..KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults()
        };

        // Out-of-range values must not keep the service from starting.
        assert!(config.validate().is_ok());
        assert_eq!(config.effective_timeout(), MAX_OPERATION_TIMEOUT);
        assert_eq!(config.effective_retry_attempts(), MAX_RETRY_ATTEMPTS);

        // In-range values pass through unchanged.
        let config = KmsConfig {
            timeout: Duration::from_secs(45),
            retry_attempts: 5,
            ..config
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.effective_timeout(), Duration::from_secs(45));
        assert_eq!(config.effective_retry_attempts(), 5);
    }

    #[test]
    fn test_local_development_defaults_require_opt_in() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf());

        assert!(config.validate().is_err());
        assert!(config.with_insecure_development_defaults().validate().is_ok());

        let production_config = KmsConfig {
            backend: KmsBackend::Local,
            backend_config: BackendConfig::Local(LocalConfig {
                key_dir: PathBuf::from("/var/lib/rustfs/kms"),
                master_key: Some("production-master-key".to_string()),
                file_permissions: Some(0o600),
            }),
            ..Default::default()
        };

        assert!(production_config.validate().is_ok());
    }

    #[test]
    fn test_vault_config() {
        let address = Url::parse("https://vault.example.com:8200").expect("Valid URL");
        let config = KmsConfig::vault(address.clone(), "test-token".to_string());

        assert_eq!(config.backend, KmsBackend::VaultKv2);
        assert!(config.validate().is_ok());

        let vault_config = config.vault_config().expect("Should have vault config");
        assert_eq!(vault_config.address, address.as_str());
    }

    #[test]
    fn test_vault_transit_config() {
        let address = Url::parse("https://vault.example.com:8200").expect("Valid URL");
        let config = KmsConfig::vault_transit(address.clone(), "test-token".to_string());

        assert_eq!(config.backend, KmsBackend::VaultTransit);
        assert!(config.validate().is_ok());

        let vault_config = config.vault_transit_config().expect("Should have vault transit config");
        assert_eq!(vault_config.address, address.as_str());
        assert_eq!(vault_config.mount_path, "transit");
    }

    #[test]
    fn test_vault_development_defaults_require_opt_in() {
        let http_address = Url::parse("http://127.0.0.1:8200").expect("Valid URL");
        let https_address = Url::parse("https://vault.example.com:8200").expect("Valid URL");

        let http_config = KmsConfig::vault(http_address, "vault-token".to_string());
        assert!(http_config.validate().is_err());
        assert!(http_config.with_insecure_development_defaults().validate().is_ok());

        let dev_token_config = KmsConfig::vault(https_address.clone(), "dev-token".to_string());
        assert!(dev_token_config.validate().is_err());
        assert!(dev_token_config.with_insecure_development_defaults().validate().is_ok());

        let skip_tls_config = KmsConfig {
            backend: KmsBackend::VaultTransit,
            backend_config: BackendConfig::VaultTransit(Box::new(VaultTransitConfig {
                address: https_address.to_string(),
                auth_method: VaultAuthMethod::Token {
                    token: "vault-token".to_string(),
                },
                namespace: None,
                mount_path: "transit".to_string(),
                metadata_kv_mount: DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT.to_string(),
                metadata_key_prefix: DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX.to_string(),
                tls: Some(TlsConfig {
                    ca_cert_path: None,
                    client_cert_path: None,
                    client_key_path: None,
                    skip_verify: true,
                }),
            })),
            ..Default::default()
        };

        assert!(skip_tls_config.validate().is_err());
        assert!(skip_tls_config.with_insecure_development_defaults().validate().is_ok());
    }

    #[test]
    fn test_vault_kv2_backend_serialization_uses_pascal_case() {
        let serialized = serde_json::to_string(&KmsBackend::VaultKv2).expect("backend should serialize");
        assert_eq!(serialized, "\"VaultKV2\"");
        let legacy: KmsBackend = serde_json::from_str("\"Vault\"").expect("legacy Vault label should deserialize");
        assert_eq!(legacy, KmsBackend::VaultKv2);
    }

    #[test]
    fn test_legacy_persisted_backend_config_vault_key_deserializes() {
        let raw = r#"{
            "backend": "Vault",
            "backend_config": {
                "Vault": {
                    "address": "http://127.0.0.1:8200",
                    "auth_method": { "Token": { "token": "t" } },
                    "namespace": null,
                    "mount_path": "transit",
                    "kv_mount": "secret",
                    "key_path_prefix": "rustfs/kms/keys",
                    "tls": null
                }
            },
            "default_key_id": null,
            "timeout": {"secs": 30, "nanos": 0},
            "retry_attempts": 3,
            "enable_cache": true,
            "cache_config": {
                "max_keys": 1000,
                "ttl": {"secs": 3600, "nanos": 0},
                "enable_metrics": true
            }
        }"#;
        let config: KmsConfig = serde_json::from_str(raw).expect("legacy persisted kms config");
        assert_eq!(config.backend, KmsBackend::VaultKv2);
        assert!(config.vault_config().is_some());
    }

    #[test]
    fn test_persisted_vault_kv2_config_without_mount_path_deserializes() {
        // Configurations persisted after mount_path was deprecated may omit the field;
        // it must default instead of failing deserialization.
        let raw = r#"{
            "backend": "VaultKV2",
            "backend_config": {
                "VaultKV2": {
                    "address": "http://127.0.0.1:8200",
                    "auth_method": { "Token": { "token": "t" } },
                    "namespace": null,
                    "kv_mount": "secret",
                    "key_path_prefix": "rustfs/kms/keys",
                    "tls": null
                }
            },
            "default_key_id": null,
            "timeout": {"secs": 30, "nanos": 0},
            "retry_attempts": 3,
            "enable_cache": true,
            "cache_config": {
                "max_keys": 1000,
                "ttl": {"secs": 3600, "nanos": 0},
                "enable_metrics": true
            }
        }"#;
        let config: KmsConfig = serde_json::from_str(raw).expect("persisted kms config without mount_path");
        assert_eq!(config.backend, KmsBackend::VaultKv2);
        let vault = config.vault_config().expect("vault-kv2 config");
        assert_eq!(vault.mount_path, "transit");
    }

    #[test]
    fn test_vault_kv2_empty_mount_path_passes_validation() {
        let address = Url::parse("https://vault.example.com:8200").expect("Valid URL");
        let mut config = KmsConfig::vault(address, "test-token".to_string());
        if let BackendConfig::VaultKv2(vault) = &mut config.backend_config {
            vault.mount_path = String::new();
        }
        assert!(config.validate().is_ok(), "deprecated mount_path must not be required");
    }

    // The "VaultKv2 must not claim Transit wrapping" documentation-claim
    // invariant is enforced by scripts/check_fips_wording.sh, which scans every
    // file in crates/kms rather than a fixed include_str! list
    // (rustfs/backlog#1884).

    #[test]
    fn test_legacy_persisted_vault_transit_config_uses_metadata_defaults() {
        let raw = r#"{
            "backend": "VaultTransit",
            "backend_config": {
                "VaultTransit": {
                    "address": "http://127.0.0.1:8200",
                    "auth_method": { "Token": { "token": "t" } },
                    "namespace": null,
                    "mount_path": "transit",
                    "tls": null
                }
            },
            "default_key_id": null,
            "timeout": {"secs": 30, "nanos": 0},
            "retry_attempts": 3,
            "enable_cache": true,
            "cache_config": {
                "max_keys": 1000,
                "ttl": {"secs": 3600, "nanos": 0},
                "enable_metrics": true
            }
        }"#;
        let config: KmsConfig = serde_json::from_str(raw).expect("legacy persisted vault-transit config");
        assert_eq!(config.backend, KmsBackend::VaultTransit);

        let vault = config
            .vault_transit_config()
            .expect("vault transit config should deserialize");
        assert_eq!(vault.metadata_kv_mount, DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT);
        assert_eq!(vault.metadata_key_prefix, DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX);
    }

    #[test]
    fn test_vault_transit_backend_serialization_uses_pascal_case() {
        let serialized = serde_json::to_string(&KmsBackend::VaultTransit).expect("backend should serialize");
        assert_eq!(serialized, "\"VaultTransit\"");
    }

    #[test]
    fn test_kms_redaction_rules_are_valid() {
        assert!(validate_redaction_rules(KMS_CONFIG_REDACTION_RULES).is_ok());
    }

    #[test]
    fn test_kms_config_debug_redacts_secret_fields() {
        let local = KmsConfig {
            backend: KmsBackend::Local,
            backend_config: BackendConfig::Local(LocalConfig {
                key_dir: PathBuf::from("/tmp/kms"),
                master_key: Some("local-master-secret".to_string()),
                file_permissions: Some(0o600),
            }),
            ..Default::default()
        };
        let vault = KmsConfig::vault(
            Url::parse("https://vault.example.com:8200").expect("vault URL"),
            "vault-token-secret".to_string(),
        );
        let approle = KmsConfig::vault_approle(
            Url::parse("https://vault.example.com:8200").expect("vault URL"),
            "role-id-visible".to_string(),
            "approle-secret-id".to_string(),
        );

        let rendered = format!("{local:?}\n{vault:?}\n{approle:?}");

        assert!(!rendered.contains("local-master-secret"));
        assert!(!rendered.contains("vault-token-secret"));
        assert!(!rendered.contains("approle-secret-id"));
        assert!(rendered.contains("role-id-visible"));
        assert!(rendered.contains(REDACTED_SECRET));
    }

    #[test]
    fn test_kms_config_serialization_preserves_secret_fields_for_persistence() {
        let config = KmsConfig::vault(
            Url::parse("https://vault.example.com:8200").expect("vault URL"),
            "persisted-token-secret".to_string(),
        );

        let serialized = serde_json::to_string(&config).expect("kms config should serialize for persistence");

        assert!(serialized.contains("persisted-token-secret"));
    }

    #[test]
    fn static_kms_config_serialization_does_not_expose_key_material() {
        use base64::Engine as _;

        let encoded_key = base64::engine::general_purpose::STANDARD.encode([0x5au8; 32]);
        let config = KmsConfig::static_kms("static-key".to_string(), encoded_key.clone());

        let serialized = serde_json::to_string(&config).expect("static KMS config should serialize");

        assert!(
            !serialized.contains(&encoded_key),
            "persisted static KMS configuration must not contain plaintext key material"
        );
    }

    #[test]
    fn test_config_validation() {
        let mut config = KmsConfig {
            allow_insecure_dev_defaults: true,
            ..Default::default()
        };

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

    #[test]
    fn test_from_env_reads_vault_settings() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault")),
                ("RUSTFS_KMS_DEFAULT_KEY_ID", Some("tenant-key")),
                ("RUSTFS_KMS_TIMEOUT_SECS", Some("42")),
                ("RUSTFS_KMS_RETRY_ATTEMPTS", Some("7")),
                ("RUSTFS_KMS_ENABLE_CACHE", Some("false")),
                ("RUSTFS_KMS_VAULT_ADDRESS", Some("https://vault.example.com")),
                ("RUSTFS_KMS_VAULT_TOKEN", Some("vault-token")),
                ("RUSTFS_KMS_VAULT_NAMESPACE", Some("tenant-a")),
                ("RUSTFS_KMS_VAULT_MOUNT_PATH", Some("transit-alt")),
                ("RUSTFS_KMS_VAULT_KV_MOUNT", Some("secret-alt")),
                ("RUSTFS_KMS_VAULT_KEY_PREFIX", Some("tenant/keys")),
            ],
            || {
                let config = KmsConfig::from_env().expect("kms config should load from env");

                assert_eq!(config.backend, KmsBackend::VaultKv2);
                assert_eq!(config.default_key_id.as_deref(), Some("tenant-key"));
                assert_eq!(config.timeout, Duration::from_secs(42));
                assert_eq!(config.retry_attempts, 7);
                assert!(!config.enable_cache);

                let vault = config.vault_config().expect("vault backend config");
                assert_eq!(vault.address, "https://vault.example.com");
                assert_eq!(vault.namespace.as_deref(), Some("tenant-a"));
                assert_eq!(vault.mount_path, "transit-alt");
                assert_eq!(vault.kv_mount, "secret-alt");
                assert_eq!(vault.key_path_prefix, "tenant/keys");
            },
        );
    }

    #[test]
    fn test_from_env_selects_approle_when_role_id_is_set() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault")),
                ("RUSTFS_KMS_VAULT_ADDRESS", Some("https://vault.example.com")),
                (ENV_KMS_VAULT_APPROLE_ROLE_ID, Some("env-role-id")),
                (ENV_KMS_VAULT_APPROLE_SECRET_ID, Some("env-approle-secret-id")),
                (ENV_KMS_VAULT_APPROLE_MOUNT, Some("approle-alt")),
                // A stale token env var must not override the AppRole selection.
                ("RUSTFS_KMS_VAULT_TOKEN", Some("vault-token")),
            ],
            || {
                let config = KmsConfig::from_env().expect("kms config should load from env");
                let vault = config.vault_config().expect("vault backend config");
                let VaultAuthMethod::AppRole {
                    role_id,
                    secret_id,
                    secret_id_file,
                    mount,
                    refresh_safety_window_secs,
                } = &vault.auth_method
                else {
                    panic!("role id in the environment must select AppRole auth, got {:?}", vault.auth_method);
                };
                assert_eq!(role_id, "env-role-id");
                assert_eq!(secret_id, "env-approle-secret-id");
                assert_eq!(secret_id_file, &None);
                assert_eq!(mount, "approle-alt");
                assert_eq!(refresh_safety_window_secs, &None);
            },
        );
    }

    #[test]
    fn test_from_env_approle_secret_id_file_is_stored_as_path() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault-transit")),
                ("RUSTFS_KMS_VAULT_ADDRESS", Some("https://vault.example.com")),
                (ENV_KMS_VAULT_APPROLE_ROLE_ID, Some("env-role-id")),
                (ENV_KMS_VAULT_APPROLE_SECRET_ID_FILE, Some("/etc/rustfs/approle-secret-id")),
            ],
            || {
                let config = KmsConfig::from_env().expect("kms config should load from env");
                let vault = config.vault_transit_config().expect("vault transit backend config");
                let VaultAuthMethod::AppRole {
                    secret_id,
                    secret_id_file,
                    mount,
                    ..
                } = &vault.auth_method
                else {
                    panic!("role id in the environment must select AppRole auth");
                };
                // The path is stored, not read: the secret_id file is re-read on
                // every login so external rotation is picked up.
                assert_eq!(secret_id_file.as_deref(), Some(std::path::Path::new("/etc/rustfs/approle-secret-id")));
                assert!(secret_id.is_empty());
                assert_eq!(mount, DEFAULT_VAULT_APPROLE_MOUNT);
            },
        );
    }

    /// The AWS backend reads only non-credential settings from the
    /// environment; access keys, profiles, and role assumption stay with the
    /// aws-config provider chain.
    #[test]
    fn test_from_env_selects_aws_backend_without_credentials() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("aws")),
                (ENV_KMS_AWS_REGION, Some("eu-central-1")),
                (ENV_KMS_AWS_ENDPOINT_URL, None::<&str>),
            ],
            || {
                let config = KmsConfig::from_env().expect("kms config should load from env");
                assert_eq!(config.backend, KmsBackend::Aws);
                let aws = config.aws_kms_config().expect("aws backend config");
                assert_eq!(aws.region.as_deref(), Some("eu-central-1"));
                assert_eq!(aws.endpoint_url, None);
            },
        );
    }

    /// A plaintext endpoint override exposes every KMS request, including the
    /// plaintext data keys, so it stays behind the explicit development opt-in.
    #[test]
    fn test_from_env_rejects_plaintext_aws_endpoint() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("aws")),
                (ENV_KMS_AWS_REGION, Some("us-east-1")),
                (ENV_KMS_AWS_ENDPOINT_URL, Some("http://localhost:4566")),
            ],
            || {
                KmsConfig::from_env().expect_err("a plaintext AWS endpoint must be rejected by default");
            },
        );
    }

    #[test]
    fn test_from_env_approle_requires_secret_id_or_file() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault")),
                (ENV_KMS_VAULT_APPROLE_ROLE_ID, Some("env-role-id")),
                (ENV_KMS_VAULT_APPROLE_SECRET_ID, None::<&str>),
                (ENV_KMS_VAULT_APPROLE_SECRET_ID_FILE, None::<&str>),
            ],
            || {
                let error = KmsConfig::from_env().expect_err("approle without a secret_id source must be rejected");
                assert!(error.to_string().contains(ENV_KMS_VAULT_APPROLE_SECRET_ID));
            },
        );
    }

    #[test]
    fn test_from_env_selects_token_file() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault")),
                ("RUSTFS_KMS_VAULT_ADDRESS", Some("https://vault.example.com")),
                (ENV_KMS_VAULT_TOKEN_FILE, Some("/run/vault-agent/token")),
                // Cleared explicitly: a static token in the ambient environment
                // outranks the token file, so leaving it up to the caller's shell
                // would make this assertion depend on who runs the test.
                ("RUSTFS_KMS_VAULT_TOKEN", None),
            ],
            || {
                let config = KmsConfig::from_env().expect("kms config should load from env");
                let vault = config.vault_config().expect("vault backend config");
                let VaultAuthMethod::TokenFile {
                    path,
                    poll_interval_secs,
                    refresh_safety_window_secs,
                } = &vault.auth_method
                else {
                    panic!("token file in the environment must select TokenFile auth, got {:?}", vault.auth_method);
                };
                assert_eq!(path, std::path::Path::new("/run/vault-agent/token"));
                assert_eq!(poll_interval_secs, &None);
                assert_eq!(refresh_safety_window_secs, &None);
            },
        );
    }

    #[test]
    fn test_from_env_token_file_is_mutually_exclusive_with_other_auth() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault")),
                (ENV_KMS_VAULT_TOKEN_FILE, Some("/run/vault-agent/token")),
                (ENV_KMS_VAULT_APPROLE_ROLE_ID, Some("env-role-id")),
            ],
            || {
                let error = KmsConfig::from_env().expect_err("token file combined with approle must be rejected");
                assert!(error.to_string().contains(ENV_KMS_VAULT_TOKEN_FILE));
                assert!(error.to_string().contains(ENV_KMS_VAULT_APPROLE_ROLE_ID));
            },
        );

        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault-transit")),
                (ENV_KMS_VAULT_TOKEN_FILE, Some("/run/vault-agent/token")),
                ("RUSTFS_KMS_VAULT_TOKEN", Some("vault-token")),
            ],
            || {
                let error = KmsConfig::from_env().expect_err("token file combined with a static token must be rejected");
                assert!(error.to_string().contains(ENV_KMS_VAULT_TOKEN_FILE));
                assert!(error.to_string().contains("RUSTFS_KMS_VAULT_TOKEN"));
            },
        );
    }

    #[test]
    fn test_validate_rejects_bad_token_file_settings() {
        let vault_config = |auth_method: VaultAuthMethod| KmsConfig {
            backend: KmsBackend::VaultKv2,
            backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
                address: "https://vault.example.com:8200".to_string(),
                auth_method,
                ..Default::default()
            })),
            ..Default::default()
        };

        let error = vault_config(VaultAuthMethod::token_file(PathBuf::new()))
            .validate()
            .expect_err("empty token file path must be rejected");
        assert!(error.to_string().contains("path"));

        let error = vault_config(VaultAuthMethod::TokenFile {
            path: PathBuf::from("/run/vault-agent/token"),
            poll_interval_secs: Some(0),
            refresh_safety_window_secs: None,
        })
        .validate()
        .expect_err("zero poll interval must be rejected");
        assert!(error.to_string().contains("poll interval"));

        vault_config(VaultAuthMethod::token_file(PathBuf::from("/run/vault-agent/token")))
            .validate()
            .expect("well-formed token file auth must validate");
    }

    /// A Kubernetes role alone configures the method: the credential is the
    /// pod's projected ServiceAccount token, so nothing secret is in the
    /// environment and the mount and token path fall back to the cluster
    /// defaults.
    #[test]
    fn test_from_env_selects_kubernetes() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault-transit")),
                (ENV_KMS_VAULT_ADDRESS, Some("https://vault.example.com")),
                (ENV_KMS_VAULT_KUBERNETES_ROLE, Some("rustfs")),
                (ENV_KMS_VAULT_KUBERNETES_MOUNT, None),
                (ENV_KMS_VAULT_KUBERNETES_JWT_PATH, None),
                (ENV_KMS_VAULT_TOKEN, None),
                (ENV_KMS_VAULT_TOKEN_FILE, None),
                (ENV_KMS_VAULT_APPROLE_ROLE_ID, None),
            ],
            || {
                let config = KmsConfig::from_env().expect("kms config should load from env");
                let vault = config.vault_transit_config().expect("vault transit backend config");
                let VaultAuthMethod::Kubernetes {
                    role,
                    mount,
                    jwt_path,
                    refresh_safety_window_secs,
                } = &vault.auth_method
                else {
                    panic!(
                        "a kubernetes role in the environment must select Kubernetes auth, got {:?}",
                        vault.auth_method
                    );
                };
                assert_eq!(role, "rustfs");
                assert_eq!(mount, DEFAULT_VAULT_KUBERNETES_MOUNT);
                assert_eq!(jwt_path, Path::new(DEFAULT_VAULT_KUBERNETES_JWT_PATH));
                assert_eq!(refresh_safety_window_secs, &None);
            },
        );
    }

    #[test]
    fn test_from_env_kubernetes_is_mutually_exclusive_with_other_auth() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault-transit")),
                (ENV_KMS_VAULT_KUBERNETES_ROLE, Some("rustfs")),
                (ENV_KMS_VAULT_APPROLE_ROLE_ID, Some("env-role-id")),
                (ENV_KMS_VAULT_TOKEN, None),
                (ENV_KMS_VAULT_TOKEN_FILE, None),
            ],
            || {
                let error = KmsConfig::from_env().expect_err("kubernetes combined with approle must be rejected");
                assert!(error.to_string().contains(ENV_KMS_VAULT_KUBERNETES_ROLE));
                assert!(error.to_string().contains(ENV_KMS_VAULT_APPROLE_ROLE_ID));
            },
        );
    }

    #[test]
    fn test_validate_rejects_bad_kubernetes_settings() {
        let vault_config = |auth_method: VaultAuthMethod| KmsConfig {
            backend: KmsBackend::VaultTransit,
            backend_config: BackendConfig::VaultTransit(Box::new(VaultTransitConfig {
                address: "https://vault.example.com:8200".to_string(),
                auth_method,
                ..Default::default()
            })),
            ..Default::default()
        };

        let error = vault_config(VaultAuthMethod::kubernetes(String::new()))
            .validate()
            .expect_err("an empty kubernetes role must be rejected");
        assert!(error.to_string().contains("role"), "got {error}");

        let error = vault_config(VaultAuthMethod::Kubernetes {
            role: "rustfs".to_string(),
            mount: String::new(),
            jwt_path: PathBuf::from(DEFAULT_VAULT_KUBERNETES_JWT_PATH),
            refresh_safety_window_secs: None,
        })
        .validate()
        .expect_err("an empty kubernetes mount must be rejected");
        assert!(error.to_string().contains("mount"), "got {error}");

        let error = vault_config(VaultAuthMethod::Kubernetes {
            role: "rustfs".to_string(),
            mount: DEFAULT_VAULT_KUBERNETES_MOUNT.to_string(),
            jwt_path: PathBuf::new(),
            refresh_safety_window_secs: None,
        })
        .validate()
        .expect_err("an empty ServiceAccount token path must be rejected");
        assert!(error.to_string().contains("token path"), "got {error}");

        vault_config(VaultAuthMethod::kubernetes("rustfs".to_string()))
            .validate()
            .expect("well-formed kubernetes auth must validate");
    }

    /// Every KV2 read, write and listing is routed through `kv_mount`, so an
    /// empty one names a path no Vault engine answers. The Transit backend
    /// already rejects its own empty mounts; this closes the same gap on the
    /// setting whose absence otherwise surfaces as an unroutable-path failure at
    /// the first Vault call.
    #[test]
    fn test_validate_rejects_an_empty_kv2_mount() {
        let kv2_config = |kv_mount: &str| KmsConfig {
            backend: KmsBackend::VaultKv2,
            backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
                address: "https://vault.example.com:8200".to_string(),
                auth_method: VaultAuthMethod::Token {
                    token: "a-real-token".to_string(),
                },
                kv_mount: kv_mount.to_string(),
                ..Default::default()
            })),
            ..Default::default()
        };

        let error = kv2_config("")
            .validate()
            .expect_err("an empty KV2 mount must be rejected as a configuration error");
        assert!(error.to_string().contains("mount"), "got {error}");

        kv2_config("secret").validate().expect("a named KV2 mount must validate");
    }

    #[test]
    fn test_approle_config_deserializes_legacy_shape_with_defaults() {
        // Persisted configurations from before the AppRole implementation only
        // carry role_id and secret_id; the new fields must fill with defaults.
        let legacy = serde_json::json!({
            "AppRole": {
                "role_id": "legacy-role",
                "secret_id": "legacy-secret-id",
            }
        });
        let auth: VaultAuthMethod = serde_json::from_value(legacy).expect("legacy AppRole config must keep deserializing");
        let VaultAuthMethod::AppRole {
            role_id,
            secret_id_file,
            mount,
            refresh_safety_window_secs,
            ..
        } = auth
        else {
            panic!("expected AppRole");
        };
        assert_eq!(role_id, "legacy-role");
        assert_eq!(secret_id_file, None);
        assert_eq!(mount, DEFAULT_VAULT_APPROLE_MOUNT);
        assert_eq!(refresh_safety_window_secs, None);
    }

    /// The gate lives in server configuration only: it never rides along in a
    /// serialized config, and a stored config that claims it must not be
    /// believed. Otherwise one operator's one-time enablement would reach every
    /// node that later reloads that config.
    #[test]
    fn immediate_deletion_gate_is_server_local_and_never_persisted() {
        let persisted =
            serde_json::to_value(KmsConfig::default().with_immediate_deletion_allowed()).expect("kms config should serialize");
        assert!(
            persisted.get("allow_immediate_deletion").is_none(),
            "the gate must not be written into a persisted config: {persisted}"
        );

        let mut forged = persisted;
        forged
            .as_object_mut()
            .expect("a persisted config must be a JSON object")
            .insert("allow_immediate_deletion".to_string(), serde_json::json!(true));
        let restored: KmsConfig = serde_json::from_value(forged).expect("an unknown gate field must not break loading");
        assert!(!restored.allow_immediate_deletion, "a stored gate must fail closed");

        with_vars(vec![(ENV_KMS_ALLOW_IMMEDIATE_DELETION, Some("true"))], || {
            assert!(
                allow_immediate_deletion_from_env(),
                "the gate must be reachable from server configuration"
            );
        });
        with_vars(vec![(ENV_KMS_ALLOW_IMMEDIATE_DELETION, None::<&str>)], || {
            assert!(!allow_immediate_deletion_from_env());
        });
    }

    #[test]
    fn persisted_config_unknown_fields_remain_readable_and_are_observed() {
        // Unknown fields in a persisted config are deliberately tolerated (a
        // rolling upgrade writes fields the previous build does not know), but
        // tolerated must not mean invisible (rustfs/backlog#1641): the
        // observing loader counts and warns, naming only the field path —
        // never the value, which can sit next to a secret. Coverage includes a
        // field nested inside the backend variant, which the externally tagged
        // enum exposes to the observer.
        let mut value = serde_json::to_value(KmsConfig::default()).expect("serialize config");
        value.as_object_mut().expect("config serializes to an object").insert(
            "top_level_field_from_the_future".to_string(),
            serde_json::json!("top-level value must not be logged"),
        );
        value
            .pointer_mut("/backend_config/Local")
            .expect("default config has a Local backend section")
            .as_object_mut()
            .expect("Local backend section is an object")
            .insert(
                "nested_field_from_the_future".to_string(),
                serde_json::json!("nested value must not be logged"),
            );
        let data = serde_json::to_vec(&value).expect("encode config");

        let logs = crate::test_support::CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(logs.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let config = metrics::with_local_recorder(&recorder, || {
            tracing::dispatcher::with_default(&dispatch, || {
                kms_config_from_persisted_json(&data).expect("unknown fields must remain readable")
            })
        });
        assert!(matches!(config.backend_config, BackendConfig::Local(_)));
        assert_eq!(crate::test_support::unknown_field_metric(&recorder, "kms-config"), 2);

        let output = logs.output();
        assert!(output.contains("persisted KMS configuration contains unknown fields"), "got: {output}");
        assert!(!output.contains("must not be logged"));

        // A clean config observes nothing and logs nothing.
        let clean = serde_json::to_vec(&KmsConfig::default()).expect("encode clean config");
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || kms_config_from_persisted_json(&clean).expect("clean config must parse"));
        assert_eq!(crate::test_support::unknown_field_metric(&recorder, "kms-config"), 0);
    }

    #[test]
    fn test_validate_rejects_incomplete_approle() {
        let mut config = KmsConfig::vault_approle(
            Url::parse("https://vault.example.com:8200").expect("vault URL"),
            String::new(),
            "secret-id".to_string(),
        );
        let error = config.validate().expect_err("empty role_id must be rejected");
        assert!(error.to_string().contains("role_id"));

        config = KmsConfig::vault_approle(
            Url::parse("https://vault.example.com:8200").expect("vault URL"),
            "role-id".to_string(),
            String::new(),
        );
        let error = config
            .validate()
            .expect_err("approle without secret_id or secret_id_file must be rejected");
        assert!(error.to_string().contains("secret_id"));
    }

    #[test]
    fn test_from_env_requires_vault_development_opt_in() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault")),
                ("RUSTFS_KMS_VAULT_ADDRESS", Some("http://127.0.0.1:8200")),
                ("RUSTFS_KMS_VAULT_TOKEN", Some("dev-token")),
            ],
            || {
                let error = KmsConfig::from_env().expect_err("vault dev defaults should fail closed");
                assert!(error.to_string().contains(ENV_KMS_ALLOW_INSECURE_DEV_DEFAULTS));
            },
        );

        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault")),
                ("RUSTFS_KMS_VAULT_ADDRESS", Some("http://127.0.0.1:8200")),
                ("RUSTFS_KMS_VAULT_TOKEN", Some("dev-token")),
                (ENV_KMS_ALLOW_INSECURE_DEV_DEFAULTS, Some("true")),
            ],
            || {
                let config = KmsConfig::from_env().expect("explicit development opt-in should allow vault dev defaults");
                assert!(config.allow_insecure_dev_defaults);
            },
        );
    }

    #[test]
    fn test_from_env_rejects_vault_skip_tls_verify_without_opt_in() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault-transit")),
                ("RUSTFS_KMS_VAULT_ADDRESS", Some("https://vault.example.com")),
                ("RUSTFS_KMS_VAULT_TOKEN", Some("vault-token")),
                (ENV_KMS_VAULT_SKIP_TLS_VERIFY, Some("true")),
            ],
            || {
                let error = KmsConfig::from_env().expect_err("skip TLS verify should fail closed");
                assert!(error.to_string().contains(ENV_KMS_ALLOW_INSECURE_DEV_DEFAULTS));
            },
        );
    }

    #[test]
    fn test_from_env_reads_vault_transit_settings() {
        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("vault-transit")),
                ("RUSTFS_KMS_DEFAULT_KEY_ID", Some("tenant-key")),
                ("RUSTFS_KMS_VAULT_ADDRESS", Some("https://vault.example.com")),
                ("RUSTFS_KMS_VAULT_TOKEN", Some("vault-token")),
                ("RUSTFS_KMS_VAULT_NAMESPACE", Some("tenant-a")),
                ("RUSTFS_KMS_VAULT_MOUNT_PATH", Some("transit-alt")),
            ],
            || {
                let config = KmsConfig::from_env().expect("kms config should load from env");

                assert_eq!(config.backend, KmsBackend::VaultTransit);
                assert_eq!(config.default_key_id.as_deref(), Some("tenant-key"));

                let vault = config.vault_transit_config().expect("vault transit backend config");
                assert_eq!(vault.address, "https://vault.example.com");
                assert_eq!(vault.namespace.as_deref(), Some("tenant-a"));
                assert_eq!(vault.mount_path, "transit-alt");
            },
        );
    }

    #[test]
    fn test_from_env_reads_static_secret_file_and_sets_default_key() {
        use base64::Engine as _;

        let temp_dir = TempDir::new().expect("create temp dir for static KMS secret");
        let secret_path = temp_dir.path().join("static-kms-secret");
        // Named `*_key_b64` (not `*_secret`) so the logging-guardrails check does not
        // flag these fixture interpolations as secrets leaking into log strings.
        let file_key_b64 = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        let env_key_b64 = base64::engine::general_purpose::STANDARD.encode([9u8; 32]);
        std::fs::write(&secret_path, format!("file-key:{file_key_b64}\n")).expect("write static KMS secret file");

        with_vars(
            vec![
                ("RUSTFS_KMS_BACKEND", Some("static")),
                (
                    ENV_KMS_STATIC_SECRET_KEY_FILE,
                    Some(secret_path.to_str().expect("secret path should be utf-8")),
                ),
                (ENV_KMS_STATIC_SECRET_KEY, Some(&format!("env-key:{env_key_b64}"))),
            ],
            || {
                let config = KmsConfig::from_env().expect("static KMS config should load from secret file");

                assert_eq!(config.backend, KmsBackend::Static);
                assert_eq!(config.default_key_id.as_deref(), Some("file-key"));
                let static_config = config.static_config().expect("static backend config");
                assert_eq!(static_config.key_id, "file-key");
                assert_eq!(static_config.secret_key, file_key_b64);
            },
        );
    }
}
