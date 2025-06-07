use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use once_cell::sync::OnceCell;

/// Configuration manager for handling various configuration types
#[derive(Debug, Clone)]
pub struct ConfigManager {
    inner: Arc<ConfigManagerInner>,
}

#[derive(Debug)]
struct ConfigManagerInner {
    /// KMS configurations indexed by target name
    kms_configs: RwLock<HashMap<String, Value>>,
    /// General configurations
    general_configs: RwLock<HashMap<String, Value>>,
}

static GLOBAL_CONFIG_MANAGER: OnceCell<ConfigManager> = OnceCell::new();

impl ConfigManager {
    /// Create a new config manager
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ConfigManagerInner {
                kms_configs: RwLock::new(HashMap::new()),
                general_configs: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Get the global config manager instance
    pub fn global() -> &'static ConfigManager {
        GLOBAL_CONFIG_MANAGER.get_or_init(ConfigManager::new)
    }

    /// Set KMS configuration for a specific target
    pub async fn set_kms_config(&self, target: &str, config: Value) -> Result<(), ConfigError> {
        // Validate the KMS config structure
        self.validate_kms_config(&config)?;
        
        let mut kms_configs = self.inner.kms_configs.write().await;
        kms_configs.insert(target.to_string(), config);
        
        tracing::info!("Updated KMS configuration for target: {}", target);
        Ok(())
    }

    /// Get KMS configuration for a specific target
    pub async fn get_kms_config(&self, target: &str) -> Option<Value> {
        let kms_configs = self.inner.kms_configs.read().await;
        kms_configs.get(target).cloned()
    }

    /// Get all KMS configurations
    pub async fn get_all_kms_configs(&self) -> HashMap<String, Value> {
        let kms_configs = self.inner.kms_configs.read().await;
        kms_configs.clone()
    }

    /// Set general configuration
    pub async fn set_config(&self, key: &str, config: Value) -> Result<(), ConfigError> {
        let mut general_configs = self.inner.general_configs.write().await;
        general_configs.insert(key.to_string(), config);
        
        tracing::info!("Updated configuration for key: {}", key);
        Ok(())
    }

    /// Get general configuration
    pub async fn get_config(&self, key: &str) -> Option<Value> {
        let general_configs = self.inner.general_configs.read().await;
        general_configs.get(key).cloned()
    }

    /// Get all configurations
    pub async fn get_all_configs(&self) -> AllConfigurations {
        let kms_configs = self.inner.kms_configs.read().await;
        let general_configs = self.inner.general_configs.read().await;
        
        AllConfigurations {
            kms: kms_configs.clone(),
            general: general_configs.clone(),
        }
    }

    /// Remove KMS configuration for a specific target
    pub async fn remove_kms_config(&self, target: &str) -> Option<Value> {
        let mut kms_configs = self.inner.kms_configs.write().await;
        let removed = kms_configs.remove(target);
        
        if removed.is_some() {
            tracing::info!("Removed KMS configuration for target: {}", target);
        }
        
        removed
    }

    /// Clear all configurations
    pub async fn clear_all(&self) {
        let mut kms_configs = self.inner.kms_configs.write().await;
        let mut general_configs = self.inner.general_configs.write().await;
        
        kms_configs.clear();
        general_configs.clear();
        
        tracing::info!("Cleared all configurations");
    }

    /// Validate KMS configuration structure
    fn validate_kms_config(&self, config: &Value) -> Result<(), ConfigError> {
        // Basic validation - ensure it's an object
        if !config.is_object() {
            return Err(ConfigError::InvalidFormat("KMS config must be a JSON object".to_string()));
        }

        let obj = config.as_object().unwrap();

        // Check for required fields based on common KMS configurations
        if let Some(kms_type) = obj.get("type") {
            let kms_type_str = kms_type.as_str()
                .ok_or_else(|| ConfigError::InvalidFormat("KMS type must be a string".to_string()))?;

            match kms_type_str {
                "vault" => {
                    // Validate Vault-specific fields
                    if !obj.contains_key("endpoint") {
                        return Err(ConfigError::MissingField("endpoint".to_string()));
                    }
                    if !obj.contains_key("auth") {
                        return Err(ConfigError::MissingField("auth".to_string()));
                    }
                }
                "aws" => {
                    // Validate AWS KMS specific fields
                    if !obj.contains_key("region") {
                        return Err(ConfigError::MissingField("region".to_string()));
                    }
                }
                "azure" => {
                    // Validate Azure Key Vault specific fields
                    if !obj.contains_key("vault_url") {
                        return Err(ConfigError::MissingField("vault_url".to_string()));
                    }
                }
                _ => {
                    // Allow unknown types but log a warning
                    tracing::warn!("Unknown KMS type: {}", kms_type_str);
                }
            }
        }

        Ok(())
    }
}

impl Default for ConfigManager {
    fn default() -> Self {
        Self::new()
    }
}

/// All configurations structure for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllConfigurations {
    pub kms: HashMap<String, Value>,
    pub general: HashMap<String, Value>,
}

/// Configuration error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConfigError {
    #[error("Invalid configuration format: {0}")]
    InvalidFormat(String),
    
    #[error("Missing required field: {0}")]
    MissingField(String),
    
    #[error("Configuration not found: {0}")]
    NotFound(String),
    
    #[error("Internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_config_manager_creation() {
        let manager = ConfigManager::new();
        let all_configs = manager.get_all_configs().await;
        assert!(all_configs.kms.is_empty());
        assert!(all_configs.general.is_empty());
    }

    #[tokio::test]
    async fn test_kms_config_operations() {
        let manager = ConfigManager::new();
        
        let kms_config = json!({
            "type": "vault",
            "endpoint": "https://vault.example.com",
            "auth": {
                "token": "test-token"
            }
        });

        // Set KMS config
        let result = manager.set_kms_config("default", kms_config.clone()).await;
        assert!(result.is_ok());

        // Get KMS config
        let retrieved = manager.get_kms_config("default").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), kms_config);

        // Get all KMS configs
        let all_kms = manager.get_all_kms_configs().await;
        assert_eq!(all_kms.len(), 1);
        assert!(all_kms.contains_key("default"));
    }

    #[tokio::test]
    async fn test_kms_config_validation() {
        let manager = ConfigManager::new();

        // Test invalid format (not an object)
        let invalid_config = json!("not an object");
        let result = manager.set_kms_config("test", invalid_config).await;
        assert!(result.is_err());

        // Test missing required field for vault
        let incomplete_vault = json!({
            "type": "vault"
            // missing endpoint and auth
        });
        let result = manager.set_kms_config("test", incomplete_vault).await;
        assert!(result.is_err());

        // Test valid vault config
        let valid_vault = json!({
            "type": "vault",
            "endpoint": "https://vault.example.com",
            "auth": {
                "token": "test-token"
            }
        });
        let result = manager.set_kms_config("test", valid_vault).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_general_config_operations() {
        let manager = ConfigManager::new();
        
        let config_value = json!({
            "setting1": "value1",
            "setting2": 42
        });

        // Set general config
        let result = manager.set_config("app_settings", config_value.clone()).await;
        assert!(result.is_ok());

        // Get general config
        let retrieved = manager.get_config("app_settings").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), config_value);
    }

    #[tokio::test]
    async fn test_config_removal() {
        let manager = ConfigManager::new();
        
        let kms_config = json!({
            "type": "aws",
            "region": "us-east-1"
        });

        // Set and then remove KMS config
        manager.set_kms_config("aws-test", kms_config).await.unwrap();
        let removed = manager.remove_kms_config("aws-test").await;
        assert!(removed.is_some());

        // Verify it's removed
        let retrieved = manager.get_kms_config("aws-test").await;
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_clear_all_configs() {
        let manager = ConfigManager::new();
        
        // Add some configs
        manager.set_kms_config("test1", json!({"type": "vault", "endpoint": "test", "auth": {}})).await.unwrap();
        manager.set_config("general1", json!({"key": "value"})).await.unwrap();

        // Clear all
        manager.clear_all().await;

        // Verify all cleared
        let all_configs = manager.get_all_configs().await;
        assert!(all_configs.kms.is_empty());
        assert!(all_configs.general.is_empty());
    }

    #[tokio::test]
    async fn test_global_instance() {
        let manager1 = ConfigManager::global();
        let manager2 = ConfigManager::global();
        
        // Should be the same instance
        assert!(Arc::ptr_eq(&manager1.inner, &manager2.inner));
    }
}