//! Caching layer for KMS operations and configurations

use crate::EncryptionAlgorithm;
use crate::error::KmsError;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Cache entry with expiration time
#[derive(Debug, Clone)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

impl<T> CacheEntry<T> {
    fn new(value: T, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
}

/// Configuration for cache behavior
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Time-to-live for data encryption keys
    pub dek_ttl: Duration,
    /// Time-to-live for bucket encryption configurations
    pub bucket_config_ttl: Duration,
    /// Maximum number of entries in each cache
    pub max_entries: usize,
    /// Enable/disable caching
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            dek_ttl: Duration::from_secs(300),           // 5 minutes
            bucket_config_ttl: Duration::from_secs(600), // 10 minutes
            max_entries: 1000,
            enabled: true,
        }
    }
}

/// Cached data encryption key
#[derive(Debug, Clone)]
pub struct CachedDataKey {
    pub plaintext_key: Vec<u8>,
    pub encrypted_key: Vec<u8>,
    pub algorithm: EncryptionAlgorithm,
}

/// Cached bucket encryption configuration
#[derive(Debug, Clone)]
pub struct CachedBucketConfig {
    pub algorithm: EncryptionAlgorithm,
    pub kms_key_id: Option<String>,
    pub bucket_key_enabled: bool,
}

/// KMS cache manager for optimizing repeated operations
pub struct KmsCacheManager {
    config: CacheConfig,
    data_keys: Arc<RwLock<HashMap<String, CacheEntry<CachedDataKey>>>>,
    bucket_configs: Arc<RwLock<HashMap<String, CacheEntry<CachedBucketConfig>>>>,
    stats: Arc<RwLock<CacheStats>>,
}

/// Cache statistics for monitoring
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub dek_hits: u64,
    pub dek_misses: u64,
    pub bucket_config_hits: u64,
    pub bucket_config_misses: u64,
    pub evictions: u64,
}

impl CacheStats {
    pub fn dek_hit_rate(&self) -> f64 {
        let total = self.dek_hits + self.dek_misses;
        if total == 0 {
            0.0
        } else {
            self.dek_hits as f64 / total as f64
        }
    }

    pub fn bucket_config_hit_rate(&self) -> f64 {
        let total = self.bucket_config_hits + self.bucket_config_misses;
        if total == 0 {
            0.0
        } else {
            self.bucket_config_hits as f64 / total as f64
        }
    }
}

impl KmsCacheManager {
    /// Create a new cache manager with the given configuration
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            data_keys: Arc::new(RwLock::new(HashMap::new())),
            bucket_configs: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(CacheStats::default())),
        }
    }

    /// Get cached data encryption key
    pub async fn get_data_key(&self, key_id: &str) -> Option<CachedDataKey> {
        if !self.config.enabled {
            return None;
        }

        let mut cache = self.data_keys.write().await;
        let mut stats = self.stats.write().await;

        if let Some(entry) = cache.get(key_id) {
            if !entry.is_expired() {
                stats.dek_hits += 1;
                return Some(entry.value.clone());
            } else {
                // Remove expired entry
                cache.remove(key_id);
            }
        }

        stats.dek_misses += 1;
        None
    }

    /// Cache a data encryption key
    pub async fn put_data_key(&self, key_id: String, data_key: CachedDataKey) -> Result<(), KmsError> {
        if !self.config.enabled {
            return Ok(());
        }

        let mut cache = self.data_keys.write().await;
        let mut stats = self.stats.write().await;

        // Check if we need to evict entries
        if cache.len() >= self.config.max_entries {
            self.evict_expired_data_keys(&mut cache).await;

            // If still at capacity, remove oldest entry
            if cache.len() >= self.config.max_entries {
                if let Some(oldest_key) = cache.keys().next().cloned() {
                    cache.remove(&oldest_key);
                    stats.evictions += 1;
                }
            }
        }

        let entry = CacheEntry::new(data_key, self.config.dek_ttl);
        cache.insert(key_id, entry);
        Ok(())
    }

    /// Get cached bucket encryption configuration
    pub async fn get_bucket_config(&self, bucket: &str) -> Option<CachedBucketConfig> {
        if !self.config.enabled {
            return None;
        }

        let mut cache = self.bucket_configs.write().await;
        let mut stats = self.stats.write().await;

        if let Some(entry) = cache.get(bucket) {
            if !entry.is_expired() {
                stats.bucket_config_hits += 1;
                return Some(entry.value.clone());
            } else {
                // Remove expired entry
                cache.remove(bucket);
            }
        }

        stats.bucket_config_misses += 1;
        None
    }

    /// Cache a bucket encryption configuration
    pub async fn put_bucket_config(&self, bucket: String, config: CachedBucketConfig) -> Result<(), KmsError> {
        if !self.config.enabled {
            return Ok(());
        }

        let mut cache = self.bucket_configs.write().await;
        let mut stats = self.stats.write().await;

        // Check if we need to evict entries
        if cache.len() >= self.config.max_entries {
            self.evict_expired_bucket_configs(&mut cache).await;

            // If still at capacity, remove oldest entry
            if cache.len() >= self.config.max_entries {
                if let Some(oldest_key) = cache.keys().next().cloned() {
                    cache.remove(&oldest_key);
                    stats.evictions += 1;
                }
            }
        }

        let entry = CacheEntry::new(config, self.config.bucket_config_ttl);
        cache.insert(bucket, entry);
        Ok(())
    }

    /// Invalidate cached data key
    pub async fn invalidate_data_key(&self, key_id: &str) {
        if !self.config.enabled {
            return;
        }

        let mut cache = self.data_keys.write().await;
        cache.remove(key_id);
    }

    /// Invalidate cached bucket configuration
    pub async fn invalidate_bucket_config(&self, bucket: &str) {
        if !self.config.enabled {
            return;
        }

        let mut cache = self.bucket_configs.write().await;
        cache.remove(bucket);
    }

    /// Clear all cached entries
    pub async fn clear_all(&self) {
        if !self.config.enabled {
            return;
        }

        let mut data_keys = self.data_keys.write().await;
        let mut bucket_configs = self.bucket_configs.write().await;

        data_keys.clear();
        bucket_configs.clear();
    }

    /// Get cache statistics
    pub async fn get_stats(&self) -> CacheStats {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// Reset cache statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = CacheStats::default();
    }

    /// Background task to clean up expired entries
    pub async fn cleanup_expired(&self) {
        if !self.config.enabled {
            return;
        }

        let mut data_keys = self.data_keys.write().await;
        let mut bucket_configs = self.bucket_configs.write().await;

        self.evict_expired_data_keys(&mut data_keys).await;
        self.evict_expired_bucket_configs(&mut bucket_configs).await;
    }

    /// Remove expired data key entries
    async fn evict_expired_data_keys(&self, cache: &mut HashMap<String, CacheEntry<CachedDataKey>>) {
        let expired_keys: Vec<String> = cache
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();

        for key in expired_keys {
            cache.remove(&key);
        }
    }

    /// Remove expired bucket config entries
    async fn evict_expired_bucket_configs(&self, cache: &mut HashMap<String, CacheEntry<CachedBucketConfig>>) {
        let expired_keys: Vec<String> = cache
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();

        for key in expired_keys {
            cache.remove(&key);
        }
    }

    /// Start background cleanup task
    pub fn start_cleanup_task(self: Arc<Self>, interval: Duration) {
        let cache_manager = Arc::clone(&self);
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                cache_manager.cleanup_expired().await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_cache_entry_expiration() {
        let entry = CacheEntry::new("test".to_string(), Duration::from_millis(10));
        assert!(!entry.is_expired());

        sleep(Duration::from_millis(20)).await;
        assert!(entry.is_expired());
    }

    #[tokio::test]
    async fn test_data_key_caching() {
        let config = CacheConfig::default();
        let cache_manager = KmsCacheManager::new(config);

        let key_id = "test-key-id".to_string();
        let data_key = CachedDataKey {
            plaintext_key: vec![1, 2, 3, 4],
            encrypted_key: vec![5, 6, 7, 8],
            algorithm: EncryptionAlgorithm::Aes256Gcm,
        };

        // Cache miss
        assert!(cache_manager.get_data_key(&key_id).await.is_none());

        // Put and get
        cache_manager
            .put_data_key(key_id.clone(), data_key.clone())
            .await
            .expect("Failed to put data key");
        let cached = cache_manager.get_data_key(&key_id).await.expect("Data key should exist");
        assert_eq!(cached.plaintext_key, data_key.plaintext_key);
        assert_eq!(cached.algorithm, data_key.algorithm);

        // Check stats
        let stats = cache_manager.get_stats().await;
        assert_eq!(stats.dek_hits, 1);
        assert_eq!(stats.dek_misses, 1);
    }

    #[tokio::test]
    async fn test_bucket_config_caching() {
        let config = CacheConfig::default();
        let cache_manager = KmsCacheManager::new(config);

        let bucket = "test-bucket".to_string();
        let bucket_config = CachedBucketConfig {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            kms_key_id: Some("key-123".to_string()),
            bucket_key_enabled: true,
        };

        // Cache miss
        assert!(cache_manager.get_bucket_config(&bucket).await.is_none());

        // Put and get
        cache_manager
            .put_bucket_config(bucket.clone(), bucket_config.clone())
            .await
            .expect("Failed to put bucket config");
        let cached = cache_manager
            .get_bucket_config(&bucket)
            .await
            .expect("Bucket config should exist");
        assert_eq!(cached.algorithm, bucket_config.algorithm);
        assert_eq!(cached.kms_key_id, bucket_config.kms_key_id);
        assert_eq!(cached.bucket_key_enabled, bucket_config.bucket_key_enabled);

        // Check stats
        let stats = cache_manager.get_stats().await;
        assert_eq!(stats.bucket_config_hits, 1);
        assert_eq!(stats.bucket_config_misses, 1);
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let config = CacheConfig::default();
        let cache_manager = KmsCacheManager::new(config);

        let key_id = "test-key-id".to_string();
        let data_key = CachedDataKey {
            plaintext_key: vec![1, 2, 3, 4],
            encrypted_key: vec![5, 6, 7, 8],
            algorithm: EncryptionAlgorithm::Aes256Gcm,
        };

        // Cache and verify
        cache_manager
            .put_data_key(key_id.clone(), data_key)
            .await
            .expect("Failed to put data key");
        assert!(cache_manager.get_data_key(&key_id).await.is_some());

        // Invalidate and verify
        cache_manager.invalidate_data_key(&key_id).await;
        assert!(cache_manager.get_data_key(&key_id).await.is_none());
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let stats = CacheStats {
            dek_hits: 8,
            dek_misses: 2,
            bucket_config_hits: 6,
            bucket_config_misses: 4,
            evictions: 1,
        };

        assert_eq!(stats.dek_hit_rate(), 0.8);
        assert_eq!(stats.bucket_config_hit_rate(), 0.6);
    }

    #[tokio::test]
    async fn test_disabled_cache() {
        let config = CacheConfig {
            enabled: false,
            ..Default::default()
        };
        let cache_manager = KmsCacheManager::new(config);

        let key_id = "test-key-id".to_string();
        let data_key = CachedDataKey {
            plaintext_key: vec![1, 2, 3, 4],
            encrypted_key: vec![5, 6, 7, 8],
            algorithm: EncryptionAlgorithm::Aes256Gcm,
        };

        // Should not cache when disabled
        cache_manager
            .put_data_key(key_id.clone(), data_key)
            .await
            .expect("Failed to put data key");
        assert!(cache_manager.get_data_key(&key_id).await.is_none());
    }
}
