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

//! Bucket-level encryption configuration management

use crate::error::{KmsError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{debug, info, warn};

/// Supported encryption algorithms for bucket-level encryption
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketEncryptionAlgorithm {
    /// AES-256 with Galois/Counter Mode
    #[serde(rename = "AES256")]
    Aes256,
    /// ChaCha20-Poly1305
    #[serde(rename = "CHACHA20_POLY1305")]
    ChaCha20Poly1305,
}

impl Default for BucketEncryptionAlgorithm {
    fn default() -> Self {
        Self::Aes256
    }
}

impl std::fmt::Display for BucketEncryptionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aes256 => write!(f, "AES256"),
            Self::ChaCha20Poly1305 => write!(f, "CHACHA20_POLY1305"),
        }
    }
}

/// Bucket encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketEncryptionConfig {
    /// Whether encryption is enabled for this bucket
    pub enabled: bool,
    /// Encryption algorithm to use
    pub algorithm: BucketEncryptionAlgorithm,
    /// KMS key ID for encrypting data keys
    pub kms_key_id: String,
    /// Whether to encrypt object metadata
    pub encrypt_metadata: bool,
    /// Additional encryption context
    pub encryption_context: HashMap<String, String>,
    /// Configuration creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Configuration last update timestamp
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl BucketEncryptionConfig {
    /// Create a new bucket encryption configuration
    pub fn new(algorithm: BucketEncryptionAlgorithm, kms_key_id: String, encrypt_metadata: bool) -> Self {
        let now = chrono::Utc::now();
        Self {
            enabled: true,
            algorithm,
            kms_key_id,
            encrypt_metadata,
            encryption_context: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Add encryption context
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.encryption_context.insert(key, value);
        self
    }

    /// Update the configuration
    pub fn update(&mut self) {
        self.updated_at = chrono::Utc::now();
    }

    /// Disable encryption for this bucket
    pub fn disable(&mut self) {
        self.enabled = false;
        self.update();
    }

    /// Enable encryption for this bucket
    pub fn enable(&mut self) {
        self.enabled = true;
        self.update();
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.enabled {
            if self.kms_key_id.is_empty() {
                return Err(KmsError::ConfigurationError {
                    message: "KMS key ID is required when encryption is enabled".to_string(),
                });
            }
        }
        Ok(())
    }
}

/// Bucket encryption configuration manager
#[derive(Debug)]
pub struct BucketEncryptionManager {
    /// In-memory cache of bucket encryption configurations
    configs: Arc<RwLock<HashMap<String, BucketEncryptionConfig>>>,
    /// Default encryption configuration for new buckets
    default_config: Option<BucketEncryptionConfig>,
}

impl BucketEncryptionManager {
    /// Create a new bucket encryption manager
    pub fn new() -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
            default_config: None,
        }
    }

    /// Create a new bucket encryption manager with default configuration
    pub fn with_default_config(default_config: BucketEncryptionConfig) -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
            default_config: Some(default_config),
        }
    }

    /// Set bucket encryption configuration
    pub async fn set_bucket_encryption(&self, bucket_name: &str, config: BucketEncryptionConfig) -> Result<()> {
        config.validate()?;

        let mut configs = self.configs.write().map_err(|e| KmsError::ConfigurationError {
            message: format!("Failed to acquire write lock: {}", e),
        })?;

        configs.insert(bucket_name.to_string(), config.clone());

        info!(
            bucket = bucket_name,
            algorithm = %config.algorithm,
            kms_key_id = config.kms_key_id,
            "Bucket encryption configuration set"
        );

        Ok(())
    }

    /// Get bucket encryption configuration
    pub async fn get_bucket_encryption(&self, bucket_name: &str) -> Result<Option<BucketEncryptionConfig>> {
        let configs = self.configs.read().map_err(|e| KmsError::ConfigurationError {
            message: format!("Failed to acquire read lock: {}", e),
        })?;

        if let Some(config) = configs.get(bucket_name) {
            debug!(
                bucket = bucket_name,
                algorithm = %config.algorithm,
                "Retrieved bucket encryption configuration"
            );
            Ok(Some(config.clone()))
        } else if let Some(default_config) = &self.default_config {
            debug!(bucket = bucket_name, "Using default encryption configuration");
            Ok(Some(default_config.clone()))
        } else {
            debug!(bucket = bucket_name, "No encryption configuration found");
            Ok(None)
        }
    }

    /// Delete bucket encryption configuration
    pub async fn delete_bucket_encryption(&self, bucket_name: &str) -> Result<bool> {
        let mut configs = self.configs.write().map_err(|e| KmsError::ConfigurationError {
            message: format!("Failed to acquire write lock: {}", e),
        })?;

        let removed = configs.remove(bucket_name).is_some();

        if removed {
            info!(bucket = bucket_name, "Bucket encryption configuration deleted");
        } else {
            warn!(bucket = bucket_name, "Attempted to delete non-existent bucket encryption configuration");
        }

        Ok(removed)
    }

    /// List all bucket encryption configurations
    pub async fn list_bucket_encryptions(&self) -> Result<HashMap<String, BucketEncryptionConfig>> {
        let configs = self.configs.read().map_err(|e| KmsError::ConfigurationError {
            message: format!("Failed to acquire read lock: {}", e),
        })?;

        Ok(configs.clone())
    }

    /// Check if a bucket has encryption enabled
    pub async fn is_encryption_enabled(&self, bucket_name: &str) -> Result<bool> {
        match self.get_bucket_encryption(bucket_name).await? {
            Some(config) => Ok(config.enabled),
            None => Ok(false),
        }
    }

    /// Get encryption algorithm for a bucket
    pub async fn get_encryption_algorithm(&self, bucket_name: &str) -> Result<Option<BucketEncryptionAlgorithm>> {
        match self.get_bucket_encryption(bucket_name).await? {
            Some(config) if config.enabled => Ok(Some(config.algorithm)),
            _ => Ok(None),
        }
    }

    /// Clear all configurations (for testing)
    #[cfg(test)]
    pub async fn clear_all(&self) -> Result<()> {
        let mut configs = self.configs.write().map_err(|e| KmsError::ConfigurationError {
            message: format!("Failed to acquire write lock: {}", e),
        })?;
        configs.clear();
        Ok(())
    }
}

impl Default for BucketEncryptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bucket_encryption_config() {
        let config = BucketEncryptionConfig::new(BucketEncryptionAlgorithm::Aes256, "test-key-id".to_string(), true);

        assert!(config.enabled);
        assert_eq!(config.algorithm, BucketEncryptionAlgorithm::Aes256);
        assert_eq!(config.kms_key_id, "test-key-id");
        assert!(config.encrypt_metadata);
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_bucket_encryption_manager() {
        let manager = BucketEncryptionManager::new();
        let bucket_name = "test-bucket";

        // Initially no configuration
        assert!(manager.get_bucket_encryption(bucket_name).await.unwrap().is_none());
        assert!(!manager.is_encryption_enabled(bucket_name).await.unwrap());

        // Set configuration
        let config = BucketEncryptionConfig::new(BucketEncryptionAlgorithm::ChaCha20Poly1305, "test-key-id".to_string(), false);

        manager.set_bucket_encryption(bucket_name, config.clone()).await.unwrap();

        // Verify configuration
        let retrieved = manager.get_bucket_encryption(bucket_name).await.unwrap().unwrap();
        assert_eq!(retrieved.algorithm, BucketEncryptionAlgorithm::ChaCha20Poly1305);
        assert_eq!(retrieved.kms_key_id, "test-key-id");
        assert!(!retrieved.encrypt_metadata);
        assert!(manager.is_encryption_enabled(bucket_name).await.unwrap());

        // Delete configuration
        assert!(manager.delete_bucket_encryption(bucket_name).await.unwrap());
        assert!(manager.get_bucket_encryption(bucket_name).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_default_configuration() {
        let default_config = BucketEncryptionConfig::new(BucketEncryptionAlgorithm::Aes256, "default-key-id".to_string(), true);

        let manager = BucketEncryptionManager::with_default_config(default_config.clone());
        let bucket_name = "test-bucket";

        // Should return default configuration
        let retrieved = manager.get_bucket_encryption(bucket_name).await.unwrap().unwrap();
        assert_eq!(retrieved.algorithm, default_config.algorithm);
        assert_eq!(retrieved.kms_key_id, default_config.kms_key_id);
    }
}
