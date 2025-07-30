use crate::{
    bucket_encryption::{BucketEncryptionAlgorithm, BucketEncryptionConfig},
    error::EncryptionResult,
};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};

/// Manager for bucket-level encryption configurations
pub struct BucketEncryptionManager {
    // In-memory cache for bucket encryption configs
    // In production, this should be backed by persistent storage (ECFS)
    configs: Arc<RwLock<HashMap<String, BucketEncryptionConfig>>>,
    // Rate limiting semaphore to control concurrent config operations
    rate_limiter: Arc<Semaphore>,
}

impl Default for BucketEncryptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketEncryptionManager {
    pub fn new() -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
            // Allow up to 10 concurrent config operations
            rate_limiter: Arc::new(Semaphore::new(10)),
        }
    }

    pub fn new_with_rate_limit(max_concurrent_ops: usize) -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
            rate_limiter: Arc::new(Semaphore::new(max_concurrent_ops)),
        }
    }

    /// Set encryption configuration for a bucket
    pub async fn set_bucket_encryption(&self, bucket_name: &str, config: BucketEncryptionConfig) -> EncryptionResult<()> {
        let _permit = self
            .rate_limiter
            .acquire()
            .await
            .map_err(|_| crate::error::EncryptionError::configuration_error("Rate limit exceeded for config operations"))?;

        let mut configs = self.configs.write().await;
        configs.insert(bucket_name.to_string(), config);
        Ok(())
    }

    /// Get encryption configuration for a bucket
    pub async fn get_bucket_encryption(&self, bucket_name: &str) -> EncryptionResult<Option<BucketEncryptionConfig>> {
        let _permit = self
            .rate_limiter
            .acquire()
            .await
            .map_err(|_| crate::error::EncryptionError::configuration_error("Rate limit exceeded for config operations"))?;

        let configs = self.configs.read().await;
        Ok(configs.get(bucket_name).cloned())
    }

    /// Delete encryption configuration for a bucket
    pub async fn delete_bucket_encryption(&self, bucket_name: &str) -> EncryptionResult<()> {
        let _permit = self
            .rate_limiter
            .acquire()
            .await
            .map_err(|_| crate::error::EncryptionError::configuration_error("Rate limit exceeded for config operations"))?;

        let mut configs = self.configs.write().await;
        configs.remove(bucket_name);
        Ok(())
    }

    /// Check if a bucket should encrypt objects by default
    pub async fn should_encrypt(&self, bucket_name: &str) -> EncryptionResult<bool> {
        let configs = self.configs.read().await;
        Ok(configs.get(bucket_name).map(|config| config.enabled).unwrap_or(false))
    }

    /// Get default encryption algorithm for a bucket
    pub async fn get_default_algorithm(&self, bucket_name: &str) -> EncryptionResult<Option<BucketEncryptionAlgorithm>> {
        let configs = self.configs.read().await;
        Ok(configs.get(bucket_name).map(|config| config.algorithm.clone()))
    }

    /// Get default KMS key ID for a bucket
    pub async fn get_default_kms_key_id(&self, bucket_name: &str) -> EncryptionResult<Option<String>> {
        let configs = self.configs.read().await;
        Ok(configs.get(bucket_name).map(|config| config.kms_key_id.clone()))
    }

    /// List all bucket encryption configurations
    pub async fn list_bucket_encryptions(&self) -> EncryptionResult<HashMap<String, BucketEncryptionConfig>> {
        let configs = self.configs.read().await;
        Ok(configs.clone())
    }

    /// Create default encryption configuration
    pub fn create_default_config(algorithm: BucketEncryptionAlgorithm, kms_key_id: Option<String>) -> BucketEncryptionConfig {
        BucketEncryptionConfig {
            enabled: true,
            algorithm,
            kms_key_id: kms_key_id.unwrap_or_default(),
            encrypt_metadata: false,
            encryption_context: HashMap::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Update existing encryption configuration
    pub async fn update_bucket_encryption(&self, bucket_name: &str, mut config: BucketEncryptionConfig) -> EncryptionResult<()> {
        let _permit = self
            .rate_limiter
            .acquire()
            .await
            .map_err(|_| crate::error::EncryptionError::configuration_error("Rate limit exceeded for config operations"))?;

        config.updated_at = Utc::now();
        let mut configs = self.configs.write().await;
        configs.insert(bucket_name.to_string(), config);
        Ok(())
    }

    /// Validate encryption configuration
    pub async fn validate_config(&self, config: &BucketEncryptionConfig) -> EncryptionResult<()> {
        // Validate algorithm and KMS key requirements
        match config.algorithm {
            BucketEncryptionAlgorithm::Aes256 => {
                // AES256 doesn't require KMS key
            }
            BucketEncryptionAlgorithm::ChaCha20Poly1305 => {
                // ChaCha20Poly1305 doesn't require KMS key
            }
            BucketEncryptionAlgorithm::AwsKms => {
                // AWS KMS requires a valid KMS key ID
                if config.kms_key_id.is_empty() {
                    return Err(crate::error::EncryptionError::configuration_error(
                        "KMS key ID is required for AWS KMS encryption",
                    ));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KmsConfig;

    #[tokio::test]
    async fn test_bucket_encryption_management() {
        let mut config = KmsConfig::default();
        config.kms_type = crate::config::KmsType::Local;
        config.default_key_id = Some("default".to_string());

        let manager = BucketEncryptionManager::new();

        let bucket_name = "test-bucket";
        let encryption_config = BucketEncryptionManager::create_default_config(BucketEncryptionAlgorithm::Aes256, None);

        // Set configuration
        manager
            .set_bucket_encryption(bucket_name, encryption_config.clone())
            .await
            .expect("Failed to set bucket encryption");

        // Get configuration
        let retrieved_config = manager
            .get_bucket_encryption(bucket_name)
            .await
            .expect("Failed to get bucket encryption")
            .expect("Bucket encryption config should exist");

        assert_eq!(retrieved_config.enabled, encryption_config.enabled);
        assert_eq!(retrieved_config.algorithm, encryption_config.algorithm);

        // Check should encrypt
        let should_encrypt = manager
            .should_encrypt(bucket_name)
            .await
            .expect("Failed to check encryption status");
        assert!(should_encrypt);

        // Delete configuration
        manager
            .delete_bucket_encryption(bucket_name)
            .await
            .expect("Failed to delete bucket encryption");

        let deleted_config = manager
            .get_bucket_encryption(bucket_name)
            .await
            .expect("Failed to get bucket encryption");
        assert!(deleted_config.is_none());
    }

    #[tokio::test]
    async fn test_config_validation() {
        let mut config = KmsConfig::default();
        config.kms_type = crate::config::KmsType::Local;
        config.default_key_id = Some("default".to_string());

        let manager = BucketEncryptionManager::new();

        // Valid AES256 config
        let valid_config = BucketEncryptionManager::create_default_config(BucketEncryptionAlgorithm::Aes256, None);
        manager
            .validate_config(&valid_config)
            .await
            .expect("valid AES256 config should pass validation");

        // Invalid AWS KMS config (missing key ID)
        let invalid_config = BucketEncryptionManager::create_default_config(BucketEncryptionAlgorithm::AwsKms, None);
        assert_eq!(
            manager
                .validate_config(&invalid_config)
                .await
                .expect_err("invalid config should fail validation")
                .to_string(),
            "Encryption configuration error: KMS key ID is required for AWS KMS encryption"
        );

        // Valid AWS KMS config
        let valid_kms_config =
            BucketEncryptionManager::create_default_config(BucketEncryptionAlgorithm::AwsKms, Some("test-key-id".to_string()));
        manager
            .validate_config(&valid_kms_config)
            .await
            .expect("valid AWS KMS config should pass validation");
    }
}
