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

//! Caching layer for KMS operations to improve performance

use crate::types::{KeyMetadata, KeySpec};
use moka::future::Cache;
use std::time::Duration;

/// Cached data key entry
#[derive(Clone, Debug)]
pub struct CachedDataKey {
    pub plaintext: Vec<u8>,
    pub ciphertext: Vec<u8>,
    pub key_spec: KeySpec,
}

/// KMS cache for storing frequently accessed keys and metadata
pub struct KmsCache {
    key_metadata_cache: Cache<String, KeyMetadata>,
    data_key_cache: Cache<String, CachedDataKey>,
}

impl KmsCache {
    /// Create a new KMS cache with the specified capacity
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of entries in the cache
    ///
    /// # Returns
    /// A new instance of `KmsCache`
    ///
    pub fn new(capacity: u64) -> Self {
        Self {
            key_metadata_cache: Cache::builder()
                .max_capacity(capacity / 2)
                .time_to_live(Duration::from_secs(300)) // 5 minutes default TTL
                .build(),
            data_key_cache: Cache::builder()
                .max_capacity(capacity / 2)
                .time_to_live(Duration::from_secs(60)) // 1 minute for data keys (shorter for security)
                .build(),
        }
    }

    /// Get key metadata from cache
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to retrieve metadata for
    ///
    /// # Returns
    /// An `Option` containing the `KeyMetadata` if found, or `None` if not found
    ///
    pub async fn get_key_metadata(&self, key_id: &str) -> Option<KeyMetadata> {
        self.key_metadata_cache.get(key_id).await
    }

    /// Put key metadata into cache
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to store metadata for
    /// * `metadata` - The `KeyMetadata` to store in the cache
    ///
    pub async fn put_key_metadata(&mut self, key_id: &str, metadata: &KeyMetadata) {
        self.key_metadata_cache.insert(key_id.to_string(), metadata.clone()).await;
        self.key_metadata_cache.run_pending_tasks().await;
    }

    /// Get data key from cache
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to retrieve the data key for
    ///
    /// # Returns
    /// An `Option` containing the `CachedDataKey` if found, or `None` if not found
    ///
    pub async fn get_data_key(&self, key_id: &str) -> Option<CachedDataKey> {
        self.data_key_cache.get(key_id).await
    }

    /// Put data key into cache
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to store the data key for
    /// * `plaintext` - The plaintext data key bytes
    /// * `ciphertext` - The ciphertext data key bytes
    ///
    pub async fn put_data_key(&mut self, key_id: &str, plaintext: &[u8], ciphertext: &[u8]) {
        let cached_key = CachedDataKey {
            plaintext: plaintext.to_vec(),
            ciphertext: ciphertext.to_vec(),
            key_spec: KeySpec::Aes256, // Default to AES-256
        };
        self.data_key_cache.insert(key_id.to_string(), cached_key).await;
        self.data_key_cache.run_pending_tasks().await;
    }

    /// Remove key metadata from cache
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to remove metadata for
    ///
    pub async fn remove_key_metadata(&mut self, key_id: &str) {
        self.key_metadata_cache.remove(key_id).await;
    }

    /// Remove data key from cache
    ///
    /// # Arguments
    /// * `key_id` - The ID of the key to remove the data key for
    ///
    pub async fn remove_data_key(&mut self, key_id: &str) {
        self.data_key_cache.remove(key_id).await;
    }

    /// Clear all cached entries
    pub async fn clear(&mut self) {
        self.key_metadata_cache.invalidate_all();
        self.data_key_cache.invalidate_all();

        // Wait for invalidation to complete
        self.key_metadata_cache.run_pending_tasks().await;
        self.data_key_cache.run_pending_tasks().await;
    }

    /// Get cache statistics (hit count, miss count)
    ///
    /// # Returns
    /// A tuple containing total entries and total misses
    ///
    pub fn stats(&self) -> (u64, u64) {
        let metadata_stats = (
            self.key_metadata_cache.entry_count(),
            0u64, // moka doesn't provide miss count directly
        );
        let data_key_stats = (self.data_key_cache.entry_count(), 0u64);

        (metadata_stats.0 + data_key_stats.0, metadata_stats.1 + data_key_stats.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeyState, KeyUsage};
    use std::time::Duration;

    #[derive(Debug, Clone)]
    struct CacheInfo {
        key_metadata_count: u64,
        data_key_count: u64,
    }

    impl CacheInfo {
        fn total_entries(&self) -> u64 {
            self.key_metadata_count + self.data_key_count
        }
    }

    impl KmsCache {
        fn with_ttl_for_tests(capacity: u64, metadata_ttl: Duration, data_key_ttl: Duration) -> Self {
            Self {
                key_metadata_cache: Cache::builder().max_capacity(capacity / 2).time_to_live(metadata_ttl).build(),
                data_key_cache: Cache::builder().max_capacity(capacity / 2).time_to_live(data_key_ttl).build(),
            }
        }

        fn info_for_tests(&self) -> CacheInfo {
            CacheInfo {
                key_metadata_count: self.key_metadata_cache.entry_count(),
                data_key_count: self.data_key_cache.entry_count(),
            }
        }

        fn contains_key_metadata_for_tests(&self, key_id: &str) -> bool {
            self.key_metadata_cache.contains_key(key_id)
        }

        fn contains_data_key_for_tests(&self, key_id: &str) -> bool {
            self.data_key_cache.contains_key(key_id)
        }
    }

    #[tokio::test]
    async fn test_cache_operations() {
        let mut cache = KmsCache::new(100);

        // Test key metadata caching
        let metadata = KeyMetadata {
            key_id: "test-key-1".to_string(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("Test key".to_string()),
            creation_date: chrono::Utc::now(),
            deletion_date: None,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: std::collections::HashMap::new(),
        };

        // Put and get metadata
        cache.put_key_metadata("test-key-1", &metadata).await;
        let retrieved = cache.get_key_metadata("test-key-1").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.expect("metadata should be cached").key_id, "test-key-1");

        // Test data key caching
        let plaintext = vec![1, 2, 3, 4];
        let ciphertext = vec![5, 6, 7, 8];
        cache.put_data_key("test-key-1", &plaintext, &ciphertext).await;

        let cached_data_key = cache.get_data_key("test-key-1").await;
        assert!(cached_data_key.is_some());
        let cached_data_key = cached_data_key.expect("data key should be cached");
        assert_eq!(cached_data_key.plaintext, plaintext);
        assert_eq!(cached_data_key.ciphertext, ciphertext);
        assert_eq!(cached_data_key.key_spec, KeySpec::Aes256);

        // Test cache info
        let info = cache.info_for_tests();
        assert_eq!(info.key_metadata_count, 1);
        assert_eq!(info.data_key_count, 1);
        assert_eq!(info.total_entries(), 2);

        // Test cache clearing
        cache.clear().await;
        let info_after_clear = cache.info_for_tests();
        assert_eq!(info_after_clear.total_entries(), 0);
    }

    #[tokio::test]
    async fn test_cache_with_custom_ttl() {
        let mut cache = KmsCache::with_ttl_for_tests(
            100,
            Duration::from_millis(100), // Short TTL for testing
            Duration::from_millis(50),
        );

        let metadata = KeyMetadata {
            key_id: "ttl-test-key".to_string(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("TTL test key".to_string()),
            creation_date: chrono::Utc::now(),
            deletion_date: None,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: std::collections::HashMap::new(),
        };

        cache.put_key_metadata("ttl-test-key", &metadata).await;

        // Should be present immediately
        assert!(cache.get_key_metadata("ttl-test-key").await.is_some());

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should be expired now
        assert!(cache.get_key_metadata("ttl-test-key").await.is_none());
    }

    #[tokio::test]
    async fn test_cache_contains_methods() {
        let mut cache = KmsCache::new(100);

        assert!(!cache.contains_key_metadata_for_tests("nonexistent"));
        assert!(!cache.contains_data_key_for_tests("nonexistent"));

        let metadata = KeyMetadata {
            key_id: "contains-test".to_string(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            creation_date: chrono::Utc::now(),
            deletion_date: None,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: std::collections::HashMap::new(),
        };

        cache.put_key_metadata("contains-test", &metadata).await;
        cache.put_data_key("contains-test", &[1, 2, 3], &[4, 5, 6]).await;

        assert!(cache.contains_key_metadata_for_tests("contains-test"));
        assert!(cache.contains_data_key_for_tests("contains-test"));
    }
}
