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

//! KMS configuration

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use url::Url;

/// KMS backend type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KmsType {
    /// HashiCorp Vault via rusty_vault
    Vault,
    /// Local file-based KMS for development
    Local,
    /// AWS KMS (future implementation)
    Aws,
    /// Azure Key Vault (future implementation)
    Azure,
    /// Google Cloud KMS (future implementation)
    GoogleCloud,
}

impl Default for KmsType {
    fn default() -> Self {
        Self::Vault
    }
}

/// Main KMS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsConfig {
    /// KMS backend type
    pub kms_type: KmsType,
    /// Default master key ID
    pub default_key_id: Option<String>,
    /// Backend-specific configuration
    pub backend_config: BackendConfig,
    /// Connection timeout in seconds
    pub timeout_secs: u64,
    /// Number of retry attempts
    pub retry_attempts: u32,
    /// Enable audit logging
    pub enable_audit: bool,
    /// Audit log file path
    pub audit_log_path: Option<PathBuf>,
}

impl Default for KmsConfig {
    fn default() -> Self {
        Self {
            kms_type: KmsType::default(),
            default_key_id: None,
            backend_config: BackendConfig::default(),
            timeout_secs: 30,
            retry_attempts: 3,
            enable_audit: true,
            audit_log_path: None,
        }
    }
}

/// Backend-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendConfig {
    /// HashiCorp Vault configuration
    Vault(Box<VaultConfig>),
    /// Local KMS configuration
    Local(LocalConfig),
    /// AWS KMS configuration
    Aws(AwsConfig),
    /// Azure Key Vault configuration
    Azure(AzureConfig),
    /// Google Cloud KMS configuration
    GoogleCloud(GoogleCloudConfig),
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self::Local(LocalConfig::default())
    }
}

/// HashiCorp Vault configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultConfig {
    /// Vault server URL
    pub address: Url,
    /// Authentication method
    pub auth_method: VaultAuthMethod,
    /// Vault namespace (Vault Enterprise)
    pub namespace: Option<String>,
    /// KV secrets engine mount path
    pub mount_path: String,
    /// TLS configuration
    pub tls_config: Option<TlsConfig>,
    /// Custom headers to send with requests
    pub headers: HashMap<String, String>,
}

impl Default for VaultConfig {
    fn default() -> Self {
        Self {
            address: Url::parse("http://localhost:8200").expect("Invalid default URL"),
            auth_method: VaultAuthMethod::Token {
                token: "dev-token".to_string(),
            },
            namespace: None,
            mount_path: "transit".to_string(),
            tls_config: None,
            headers: HashMap::new(),
        }
    }
}

/// Vault authentication methods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum VaultAuthMethod {
    /// Token authentication
    Token { token: String },
    /// AppRole authentication
    AppRole { role_id: String, secret_id: String },
    /// Kubernetes authentication
    Kubernetes { role: String, jwt_path: PathBuf },
    /// AWS IAM authentication
    AwsIam {
        role: String,
        access_key: String,
        secret_key: String,
        region: String,
    },
    /// TLS certificate authentication
    Cert { cert_path: PathBuf, key_path: PathBuf },
}

/// TLS configuration for Vault connection
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
    /// Server name for SNI
    pub server_name: Option<String>,
}

/// Local KMS configuration (for development/testing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    /// Directory to store key files
    pub key_dir: PathBuf,
    /// Master key for encrypting stored keys
    pub master_key: Option<String>,
    /// Enable file encryption
    pub encrypt_files: bool,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            key_dir: std::env::temp_dir().join("kms_keys"),
            master_key: None,
            encrypt_files: true,
        }
    }
}

/// AWS KMS configuration (future implementation)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct AwsConfig {
    /// AWS region
    pub region: String,
    /// AWS access key ID
    pub access_key_id: Option<String>,
    /// AWS secret access key
    pub secret_access_key: Option<String>,
    /// AWS session token
    pub session_token: Option<String>,
    /// KMS key ARN for default operations
    pub default_key_arn: Option<String>,
}

/// Azure Key Vault configuration (future implementation)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct AzureConfig {
    /// Key Vault URL
    pub vault_url: Url,
    /// Azure tenant ID
    pub tenant_id: String,
    /// Azure client ID
    pub client_id: String,
    /// Azure client secret
    pub client_secret: String,
}

/// Google Cloud KMS configuration (future implementation)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct GoogleCloudConfig {
    /// GCP project ID
    pub project_id: String,
    /// Key ring name
    pub key_ring: String,
    /// Location (e.g., "global", "us-central1")
    pub location: String,
    /// Service account key file path
    pub service_account_key: Option<PathBuf>,
}

impl KmsConfig {
    /// Create a new KMS configuration for Vault
    pub fn vault(address: Url, token: String) -> Self {
        Self {
            kms_type: KmsType::Vault,
            backend_config: BackendConfig::Vault(Box::new(VaultConfig {
                address,
                auth_method: VaultAuthMethod::Token { token },
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    /// Create a new KMS configuration for local development
    pub fn local(key_dir: PathBuf) -> Self {
        Self {
            kms_type: KmsType::Local,
            backend_config: BackendConfig::Local(LocalConfig {
                key_dir,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Get the Vault configuration if backend is Vault
    pub fn vault_config(&self) -> Option<&VaultConfig> {
        match &self.backend_config {
            BackendConfig::Vault(config) => Some(config),
            _ => None,
        }
    }

    /// Get the local configuration if backend is Local
    pub fn local_config(&self) -> Option<&LocalConfig> {
        match &self.backend_config {
            BackendConfig::Local(config) => Some(config),
            _ => None,
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        match &self.backend_config {
            BackendConfig::Vault(config) => {
                if config.address.scheme() != "http" && config.address.scheme() != "https" {
                    return Err("Vault address must use http or https scheme".to_string());
                }

                if config.mount_path.is_empty() {
                    return Err("Vault mount path cannot be empty".to_string());
                }
            }
            BackendConfig::Local(config) => {
                if !config.key_dir.is_absolute() {
                    return Err("Local key directory must be an absolute path".to_string());
                }
            }
            _ => {
                return Err("Backend configuration not yet implemented".to_string());
            }
        }

        if self.timeout_secs == 0 {
            return Err("Timeout must be greater than 0".to_string());
        }

        Ok(())
    }

    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self, String> {
        let mut config = Self::default();

        // KMS type
        if let Ok(kms_type) = std::env::var("RUSTFS_KMS_TYPE") {
            config.kms_type = match kms_type.to_lowercase().as_str() {
                "vault" => KmsType::Vault,
                "local" => KmsType::Local,
                "aws" => KmsType::Aws,
                "azure" => KmsType::Azure,
                "gcp" | "google" => KmsType::GoogleCloud,
                _ => return Err(format!("Unknown KMS type: {kms_type}")),
            };
        }

        // Default key ID
        if let Ok(key_id) = std::env::var("RUSTFS_KMS_DEFAULT_KEY_ID") {
            config.default_key_id = Some(key_id);
        }

        // Backend-specific configuration
        match config.kms_type {
            KmsType::Vault => {
                let address = std::env::var("RUSTFS_KMS_VAULT_ADDRESS").unwrap_or_else(|_| "http://localhost:8200".to_string());
                let token = std::env::var("RUSTFS_KMS_VAULT_TOKEN").unwrap_or_else(|_| "dev-token".to_string());

                config.backend_config = BackendConfig::Vault(Box::new(VaultConfig {
                    address: Url::parse(&address).map_err(|e| format!("Invalid Vault address: {e}"))?,
                    auth_method: VaultAuthMethod::Token { token },
                    namespace: std::env::var("RUSTFS_KMS_VAULT_NAMESPACE").ok(),
                    // Default to transit engine unless explicitly overridden
                    mount_path: std::env::var("RUSTFS_KMS_VAULT_MOUNT_PATH").unwrap_or_else(|_| "transit".to_string()),
                    ..Default::default()
                }));
            }
            KmsType::Local => {
                let key_dir = std::env::var("RUSTFS_KMS_LOCAL_KEY_DIR").unwrap_or_else(|_| "./kms_keys".to_string());

                config.backend_config = BackendConfig::Local(LocalConfig {
                    key_dir: PathBuf::from(key_dir),
                    master_key: std::env::var("RUSTFS_KMS_LOCAL_MASTER_KEY").ok(),
                    ..Default::default()
                });
            }
            _ => return Err("Backend type not yet supported".to_string()),
        }

        // Timeout
        if let Ok(timeout) = std::env::var("RUSTFS_KMS_TIMEOUT_SECS") {
            config.timeout_secs = timeout.parse().map_err(|_| "Invalid timeout value".to_string())?;
        }

        // Retry attempts
        if let Ok(retries) = std::env::var("RUSTFS_KMS_RETRY_ATTEMPTS") {
            config.retry_attempts = retries.parse().map_err(|_| "Invalid retry attempts value".to_string())?;
        }

        config.validate()?;
        Ok(config)
    }
}
