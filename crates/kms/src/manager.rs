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

//! KMS manager for handling key operations and backend coordination

use crate::backends::KmsBackend;
use crate::cache::KmsCache;
use crate::config::KmsConfig;
use crate::error::Result;
use crate::types::{
    CancelKeyDeletionRequest, CancelKeyDeletionResponse, CreateKeyRequest, CreateKeyResponse, DecryptRequest, DecryptResponse,
    DeleteKeyRequest, DeleteKeyResponse, DescribeKeyRequest, DescribeKeyResponse, EncryptRequest, EncryptResponse,
    GenerateDataKeyRequest, GenerateDataKeyResponse, ListKeysRequest, ListKeysResponse,
};
use std::sync::Arc;
use tokio::sync::RwLock;

/// KMS Manager coordinates operations between backends and caching
#[derive(Clone)]
pub struct KmsManager {
    backend: Arc<dyn KmsBackend>,
    cache: Arc<RwLock<KmsCache>>,
    config: KmsConfig,
}

impl KmsManager {
    /// Create a new KMS manager with the given backend and config
    pub fn new(backend: Arc<dyn KmsBackend>, config: KmsConfig) -> Self {
        let cache = Arc::new(RwLock::new(KmsCache::new(config.cache_config.max_keys as u64)));
        Self { backend, cache, config }
    }

    /// Get the default key ID if configured
    pub fn get_default_key_id(&self) -> Option<&String> {
        self.config.default_key_id.as_ref()
    }

    /// Create a new master key
    pub async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let response = self.backend.create_key(request).await?;

        // Cache the key metadata if enabled
        if self.config.enable_cache {
            let mut cache = self.cache.write().await;
            cache.put_key_metadata(&response.key_id, &response.key_metadata).await;
        }

        Ok(response)
    }

    /// Encrypt data with a master key
    pub async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        self.backend.encrypt(request).await
    }

    /// Decrypt data with a master key
    pub async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        self.backend.decrypt(request).await
    }

    /// Generate a data encryption key
    pub async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        // Check cache first if enabled
        if self.config.enable_cache {
            let cache = self.cache.read().await;
            if let Some(cached_key) = cache.get_data_key(&request.key_id).await {
                if cached_key.key_spec == request.key_spec {
                    return Ok(GenerateDataKeyResponse {
                        key_id: request.key_id.clone(),
                        plaintext_key: cached_key.plaintext.clone(),
                        ciphertext_blob: cached_key.ciphertext.clone(),
                    });
                }
            }
        }

        // Generate new data key from backend
        let response = self.backend.generate_data_key(request).await?;

        // Cache the data key if enabled
        if self.config.enable_cache {
            let mut cache = self.cache.write().await;
            cache
                .put_data_key(&response.key_id, &response.plaintext_key, &response.ciphertext_blob)
                .await;
        }

        Ok(response)
    }

    /// Describe a key
    pub async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        // Check cache first if enabled
        if self.config.enable_cache {
            let cache = self.cache.read().await;
            if let Some(cached_metadata) = cache.get_key_metadata(&request.key_id).await {
                return Ok(DescribeKeyResponse {
                    key_metadata: cached_metadata,
                });
            }
        }

        // Get from backend and cache
        let response = self.backend.describe_key(request).await?;

        if self.config.enable_cache {
            let mut cache = self.cache.write().await;
            cache
                .put_key_metadata(&response.key_metadata.key_id, &response.key_metadata)
                .await;
        }

        Ok(response)
    }

    /// List keys
    pub async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        self.backend.list_keys(request).await
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> Option<(u64, u64)> {
        if self.config.enable_cache {
            let cache = self.cache.read().await;
            Some(cache.stats())
        } else {
            None
        }
    }

    /// Clear the cache
    pub async fn clear_cache(&self) -> Result<()> {
        if self.config.enable_cache {
            let mut cache = self.cache.write().await;
            cache.clear().await;
        }
        Ok(())
    }

    /// Delete a key
    pub async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        let response = self.backend.delete_key(request).await?;

        // Remove from cache if enabled and key is being deleted
        if self.config.enable_cache {
            let mut cache = self.cache.write().await;
            cache.remove_key_metadata(&response.key_id).await;
            cache.remove_data_key(&response.key_id).await;
        }

        Ok(response)
    }

    /// Cancel key deletion
    pub async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let response = self.backend.cancel_key_deletion(request).await?;

        // Update cache if enabled
        if self.config.enable_cache {
            let mut cache = self.cache.write().await;
            cache.put_key_metadata(&response.key_id, &response.key_metadata).await;
        }

        Ok(response)
    }

    /// Perform health check on the KMS backend
    pub async fn health_check(&self) -> Result<bool> {
        self.backend.health_check().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::local::LocalKmsBackend;
    use crate::types::{KeySpec, KeyState, KeyUsage};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_manager_operations() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf());

        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await.expect("Failed to create backend"));
        let manager = KmsManager::new(backend, config);

        // Test key creation
        let create_request = CreateKeyRequest {
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("Test key".to_string()),
            ..Default::default()
        };

        let create_response = manager.create_key(create_request).await.expect("Failed to create key");
        assert!(!create_response.key_id.is_empty());
        assert_eq!(create_response.key_metadata.key_state, KeyState::Enabled);

        // Test data key generation
        let data_key_request = GenerateDataKeyRequest {
            key_id: create_response.key_id.clone(),
            key_spec: KeySpec::Aes256,
            encryption_context: Default::default(),
        };

        let data_key_response = manager
            .generate_data_key(data_key_request)
            .await
            .expect("Failed to generate data key");
        assert_eq!(data_key_response.plaintext_key.len(), 32); // 256 bits
        assert!(!data_key_response.ciphertext_blob.is_empty());

        // Test describe key
        let describe_request = DescribeKeyRequest {
            key_id: create_response.key_id.clone(),
        };

        let describe_response = manager.describe_key(describe_request).await.expect("Failed to describe key");
        assert_eq!(describe_response.key_metadata.key_id, create_response.key_id);

        // Test cache stats
        let stats = manager.cache_stats().await;
        assert!(stats.is_some());

        // Test health check
        let health = manager.health_check().await.expect("Health check failed");
        assert!(health);
    }
}
