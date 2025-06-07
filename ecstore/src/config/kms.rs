// kms.rs - KMS configuration subsystem for RustFS
// Provides configuration management for KMS encryption similar to MinIO

use crate::config::{KV, KVS};
use common::error::{Error, Result};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::env;
use tracing::{debug, error, info};

// KMS configuration constants
pub const KMS_SUB_SYS: &str = "kms_vault";
pub const KMS_VAULT_ENDPOINT: &str = "endpoint";
pub const KMS_VAULT_KEY_NAME: &str = "key_name";
pub const KMS_VAULT_TOKEN: &str = "token";
pub const KMS_VAULT_CAPATH: &str = "ca_path";
pub const KMS_VAULT_SKIP_TLS_VERIFY: &str = "skip_tls_verify";
pub const KMS_VAULT_CLIENT_CERT: &str = "client_cert";
pub const KMS_VAULT_CLIENT_KEY: &str = "client_key";

// Environment variable names
pub const KMS_ENABLED_ENV: &str = "RUSTFS_KMS_ENABLED";
pub const KMS_VAULT_ENDPOINT_ENV: &str = "RUSTFS_KMS_VAULT_ENDPOINT";
pub const KMS_VAULT_KEY_NAME_ENV: &str = "RUSTFS_KMS_VAULT_KEY_NAME";
pub const KMS_VAULT_TOKEN_ENV: &str = "RUSTFS_KMS_VAULT_TOKEN";
pub const KMS_VAULT_CAPATH_ENV: &str = "RUSTFS_KMS_VAULT_CAPATH";
pub const KMS_VAULT_SKIP_TLS_VERIFY_ENV: &str = "RUSTFS_KMS_VAULT_SKIP_TLS_VERIFY";
pub const KMS_VAULT_CLIENT_CERT_ENV: &str = "RUSTFS_KMS_VAULT_CLIENT_CERT";
pub const KMS_VAULT_CLIENT_KEY_ENV: &str = "RUSTFS_KMS_VAULT_CLIENT_KEY";

lazy_static! {
    pub static ref DefaultKVS: KVS = {
        let mut kvs = KVS::new();
        kvs.0.push(KV {
            key: KMS_VAULT_ENDPOINT.to_string(),
            value: "http://localhost:8200".to_string(),
            hidden_if_empty: false,
        });
        kvs.0.push(KV {
            key: KMS_VAULT_KEY_NAME.to_string(),
            value: "rustfs-encryption-key".to_string(),
            hidden_if_empty: false,
        });
        kvs.0.push(KV {
            key: KMS_VAULT_TOKEN.to_string(),
            value: "".to_string(),
            hidden_if_empty: true,
        });
        kvs.0.push(KV {
            key: KMS_VAULT_CAPATH.to_string(),
            value: "".to_string(),
            hidden_if_empty: true,
        });
        kvs.0.push(KV {
            key: KMS_VAULT_SKIP_TLS_VERIFY.to_string(),
            value: "false".to_string(),
            hidden_if_empty: false,
        });
        kvs.0.push(KV {
            key: KMS_VAULT_CLIENT_CERT.to_string(),
            value: "".to_string(),
            hidden_if_empty: true,
        });
        kvs.0.push(KV {
            key: KMS_VAULT_CLIENT_KEY.to_string(),
            value: "".to_string(),
            hidden_if_empty: true,
        });
        kvs
    };
}

/// KMS configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub endpoint: String,
    pub key_name: String,
    pub token: String,
    pub ca_path: Option<String>,
    pub skip_tls_verify: bool,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8200".to_string(),
            key_name: "rustfs-encryption-key".to_string(),
            token: "".to_string(),
            ca_path: None,
            skip_tls_verify: false,
            client_cert_path: None,
            client_key_path: None,
            enabled: false,
        }
    }
}

impl Config {
    /// Validate KMS configuration
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if self.endpoint.is_empty() {
            return Err(Error::msg("KMS endpoint is required when KMS is enabled"));
        }

        if self.key_name.is_empty() {
            return Err(Error::msg("KMS key name is required when KMS is enabled"));
        }

        if self.token.is_empty() {
            return Err(Error::msg("KMS token is required when KMS is enabled"));
        }

        // Validate endpoint URL
        if let Err(_) = url::Url::parse(&self.endpoint) {
            return Err(Error::msg("Invalid KMS endpoint URL"));
        }

        // Validate TLS configuration
        if let Some(cert_path) = &self.client_cert_path {
            if cert_path.is_empty() {
                return Err(Error::msg("Client certificate path cannot be empty"));
            }
        }

        if let Some(key_path) = &self.client_key_path {
            if key_path.is_empty() {
                return Err(Error::msg("Client key path cannot be empty"));
            }
        }

        Ok(())
    }

    /// Test KMS connection
    pub async fn test_connection(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        debug!("Testing KMS connection to: {}", self.endpoint);
        
        // Create a test KMS client and try to connect
        use crypto::sse_kms::RustyVaultKMSClient;
        let client = RustyVaultKMSClient::new(
            self.endpoint.clone(),
            self.token.clone(),
            self.key_name.clone(),
        );

        // Test connection by trying to generate a data key
        match client.generate_data_key(None).await {
            Ok(_) => {
                info!("KMS connection test successful");
                Ok(())
            }
            Err(e) => {
                error!("KMS connection test failed: {}", e);
                Err(Error::msg(format!("KMS connection test failed: {}", e)))
            }
        }
    }
}

/// Parse KMS configuration from KVS
pub fn lookup_config(kvs: &KVS) -> Result<Config> {
    let mut config = Config::default();

    // Check if KMS is enabled via environment variable
    config.enabled = env::var(KMS_ENABLED_ENV)
        .unwrap_or_default()
        .parse::<bool>()
        .unwrap_or(false) || 
        env::var(KMS_ENABLED_ENV)
            .map(|v| v == "true" || v == "1" || v == "yes")
            .unwrap_or(false);

    if !config.enabled {
        debug!("KMS is disabled");
        return Ok(config);
    }

    // Parse configuration from KVS with environment variable fallback
    let endpoint_value = kvs.get(KMS_VAULT_ENDPOINT);
    config.endpoint = if endpoint_value.is_empty() {
        env::var(KMS_VAULT_ENDPOINT_ENV).unwrap_or_else(|_| config.endpoint)
    } else {
        endpoint_value
    };

    let key_name_value = kvs.get(KMS_VAULT_KEY_NAME);
    config.key_name = if key_name_value.is_empty() {
        env::var(KMS_VAULT_KEY_NAME_ENV).unwrap_or_else(|_| config.key_name)
    } else {
        key_name_value
    };

    let token_value = kvs.get(KMS_VAULT_TOKEN);
    config.token = if token_value.is_empty() {
        env::var(KMS_VAULT_TOKEN_ENV).unwrap_or_else(|_| config.token)
    } else {
        token_value
    };

    // Optional parameters
    let ca_path_value = kvs.get(KMS_VAULT_CAPATH);
    let ca_path = if ca_path_value.is_empty() {
        env::var(KMS_VAULT_CAPATH_ENV).ok()
    } else {
        Some(ca_path_value)
    };
    config.ca_path = if let Some(path) = ca_path {
        if path.is_empty() { None } else { Some(path) }
    } else {
        None
    };

    let skip_tls_value = kvs.get(KMS_VAULT_SKIP_TLS_VERIFY);
    config.skip_tls_verify = if skip_tls_value.is_empty() {
        env::var(KMS_VAULT_SKIP_TLS_VERIFY_ENV)
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false)
    } else {
        skip_tls_value.parse::<bool>().unwrap_or(false)
    };

    let client_cert_value = kvs.get(KMS_VAULT_CLIENT_CERT);
    let client_cert = if client_cert_value.is_empty() {
        env::var(KMS_VAULT_CLIENT_CERT_ENV).ok()
    } else {
        Some(client_cert_value)
    };
    config.client_cert_path = if let Some(path) = client_cert {
        if path.is_empty() { None } else { Some(path) }
    } else {
        None
    };

    let client_key_value = kvs.get(KMS_VAULT_CLIENT_KEY);
    let client_key = if client_key_value.is_empty() {
        env::var(KMS_VAULT_CLIENT_KEY_ENV).ok()
    } else {
        Some(client_key_value)
    };
    config.client_key_path = if let Some(path) = client_key {
        if path.is_empty() { None } else { Some(path) }
    } else {
        None
    };

    // Validate configuration
    config.validate()?;

    info!("KMS configuration loaded successfully");
    debug!("KMS endpoint: {}", config.endpoint);
    debug!("KMS key name: {}", config.key_name);

    Ok(config)
}

/// Convert Config to KVS for storage
pub fn config_to_kvs(config: &Config) -> KVS {
    let mut kvs = KVS::new();
    
    kvs.0.push(KV {
        key: KMS_VAULT_ENDPOINT.to_string(),
        value: config.endpoint.clone(),
        hidden_if_empty: false,
    });
    
    kvs.0.push(KV {
        key: KMS_VAULT_KEY_NAME.to_string(),
        value: config.key_name.clone(),
        hidden_if_empty: false,
    });
    
    kvs.0.push(KV {
        key: KMS_VAULT_TOKEN.to_string(),
        value: config.token.clone(),
        hidden_if_empty: true,
    });
    
    if let Some(ca_path) = &config.ca_path {
        kvs.0.push(KV {
            key: KMS_VAULT_CAPATH.to_string(),
            value: ca_path.clone(),
            hidden_if_empty: true,
        });
    }
    
    kvs.0.push(KV {
        key: KMS_VAULT_SKIP_TLS_VERIFY.to_string(),
        value: config.skip_tls_verify.to_string(),
        hidden_if_empty: false,
    });
    
    if let Some(cert_path) = &config.client_cert_path {
        kvs.0.push(KV {
            key: KMS_VAULT_CLIENT_CERT.to_string(),
            value: cert_path.clone(),
            hidden_if_empty: true,
        });
    }
    
    if let Some(key_path) = &config.client_key_path {
        kvs.0.push(KV {
            key: KMS_VAULT_CLIENT_KEY.to_string(),
            value: key_path.clone(),
            hidden_if_empty: true,
        });
    }
    
    kvs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(!config.enabled);
        assert_eq!(config.endpoint, "http://localhost:8200");
        assert_eq!(config.key_name, "rustfs-encryption-key");
        assert!(config.token.is_empty());
        assert!(!config.skip_tls_verify);
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        config.enabled = true;
        
        // Should fail with empty token
        assert!(config.validate().is_err());
        
        config.token = "test-token".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_lookup_config_disabled() {
        let kvs = KVS::new();
        let config = lookup_config(&kvs).unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn test_config_to_kvs_conversion() {
        let config = Config {
            endpoint: "http://vault:8200".to_string(),
            key_name: "test-key".to_string(),
            token: "test-token".to_string(),
            ca_path: Some("/path/to/ca.pem".to_string()),
            skip_tls_verify: true,
            client_cert_path: None,
            client_key_path: None,
            enabled: true,
        };

        let kvs = config_to_kvs(&config);
        assert!(!kvs.0.is_empty());
        
        // Check that endpoint is set correctly
        let endpoint_kv = kvs.0.iter().find(|kv| kv.key == KMS_VAULT_ENDPOINT).unwrap();
        assert_eq!(endpoint_kv.value, "http://vault:8200");
    }
}