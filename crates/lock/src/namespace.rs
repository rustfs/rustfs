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

use async_trait::async_trait;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    Locker,
    core::local::LocalLockMap,
    error::{LockError, Result},
    client::remote::RemoteClient,
};

/// Connection health check result
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionHealth {
    /// Connection healthy
    Healthy,
    /// Connection unhealthy
    Unhealthy(String),
    /// Connection status unknown
    Unknown,
}

/// Namespace lock mapping table
///
/// Provides local lock or remote lock functionality based on whether it is a distributed mode
#[derive(Debug)]
pub struct NsLockMap {
    /// Whether it is a distributed mode
    is_dist: bool,
    /// Local lock mapping table (not used in distributed mode)
    local_map: Option<LocalLockMap>,
    /// Remote client cache (used in distributed mode)
    /// Using LRU cache to avoid infinite memory growth
    remote_clients: RemoteClientCache,
    /// Health check background task stop signal
    health_check_stop_tx: Option<tokio::sync::broadcast::Sender<()>>,
}

impl NsLockMap {
    /// Start health check background task, return stop signal Sender
    fn spawn_health_check_task(
        clients: Arc<Mutex<LruCache<String, Arc<Mutex<RemoteClient>>>>>,
        interval_secs: u64,
    ) -> tokio::sync::broadcast::Sender<()> {
        let (tx, mut rx) = tokio::sync::broadcast::channel(1);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut cache_guard = clients.lock().await;
                        let mut to_remove = Vec::new();
                        for (url, client) in cache_guard.iter() {
                            let online = client.lock().await.is_online().await;
                            if !online {
                                warn!("[auto-health-check] Remote client {} is unhealthy, will remove", url);
                                to_remove.push(url.clone());
                            }
                        }
                        for url in to_remove {
                            cache_guard.pop(&url);
                            info!("[auto-health-check] Removed unhealthy remote client: {}", url);
                        }
                    }
                    _ = rx.recv() => {
                        debug!("[auto-health-check] Health check task stopped");
                        break;
                    }
                }
            }
        });
        tx
    }

    /// Create a new namespace lock mapping table
    ///
    /// # Parameters
    /// - `is_dist`: Whether it is a distributed mode
    /// - `cache_size`: LRU cache size (only valid in distributed mode, default is 100)
    ///
    /// # Returns
    /// - New NsLockMap instance
    pub fn new(is_dist: bool, cache_size: Option<usize>) -> Self {
        let cache_size = cache_size.unwrap_or(100);
        let remote_clients: RemoteClientCache = if is_dist {
            Some(Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(cache_size).unwrap()))))
        } else {
            None
        };
        let health_check_stop_tx = if is_dist {
            Some(Self::spawn_health_check_task(remote_clients.as_ref().unwrap().clone(), 30))
        } else {
            None
        };
        Self {
            is_dist,
            local_map: if !is_dist { Some(LocalLockMap::new()) } else { None },
            remote_clients,
            health_check_stop_tx,
        }
    }

    /// Create namespace lock client
    ///
    /// # Parameters
    /// - `url`: Remote lock service URL (must be provided in distributed mode)
    ///
    /// # Returns
    /// - `Ok(NamespaceLock)`: Namespace lock client
    /// - `Err`: Creation failed
    pub async fn new_nslock(&self, url: Option<Url>) -> Result<NamespaceLock> {
        if self.is_dist {
            let url = url.ok_or_else(|| LockError::internal("remote_url is required for distributed lock mode"))?;

            // Check if the client for this URL is already in the cache
            if let Some(cache) = &self.remote_clients {
                let url_str = url.to_string();
                let mut cache_guard = cache.lock().await;

                if let Some(client) = cache_guard.get(&url_str) {
                    // Reuse existing client (LRU will automatically update access order)
                    return Ok(NamespaceLock::new_cached(client.clone()));
                }

                // Create new client and cache it
                let new_client = Arc::new(Mutex::new(RemoteClient::from_url(url.clone())));
                cache_guard.put(url_str, new_client.clone());
                Ok(NamespaceLock::new_cached(new_client))
            } else {
                // Should not reach here, but for safety
                Ok(NamespaceLock::new_remote(url))
            }
        } else {
            Ok(NamespaceLock::new_local())
        }
    }

    /// Batch lock (directly use local lock mapping table)
    pub async fn lock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        if let Some(local_map) = &self.local_map {
            local_map.lock_batch(resources, owner, timeout, None).await
        } else {
            Err(LockError::internal("local lock map not available in distributed mode"))
        }
    }

    /// Batch unlock (directly use local lock mapping table)
    pub async fn unlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        if let Some(local_map) = &self.local_map {
            local_map.unlock_batch(resources, owner).await
        } else {
            Err(LockError::internal("local lock map not available in distributed mode"))
        }
    }

    /// Batch read lock (directly use local lock mapping table)
    pub async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        if let Some(local_map) = &self.local_map {
            local_map.rlock_batch(resources, owner, timeout, None).await
        } else {
            Err(LockError::internal("local lock map not available in distributed mode"))
        }
    }

    /// Batch release read lock (directly use local lock mapping table)
    pub async fn runlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        if let Some(local_map) = &self.local_map {
            local_map.runlock_batch(resources, owner).await
        } else {
            Err(LockError::internal("local lock map not available in distributed mode"))
        }
    }

    /// Check if it is a distributed mode
    pub fn is_distributed(&self) -> bool {
        self.is_dist
    }

    /// Clean up remote client cache (optional, for memory management)
    pub async fn clear_remote_cache(&self) {
        if let Some(cache) = &self.remote_clients {
            let mut cache_guard = cache.lock().await;
            cache_guard.clear();
        }
    }

    /// Get the number of clients in the cache (for monitoring)
    pub async fn cached_client_count(&self) -> usize {
        if let Some(cache) = &self.remote_clients {
            let cache_guard = cache.lock().await;
            cache_guard.len()
        } else {
            0
        }
    }

    /// Get the cache capacity (for monitoring)
    pub async fn cache_capacity(&self) -> usize {
        if let Some(cache) = &self.remote_clients {
            let cache_guard = cache.lock().await;
            cache_guard.cap().get()
        } else {
            0
        }
    }

    /// Get the cache hit rate (for monitoring)
    /// Note: This is a simplified implementation, and actual use may require more complex statistics
    pub async fn cache_hit_rate(&self) -> f64 {
        // TODO: Implement more accurate hit rate statistics
        // Currently returning a placeholder value
        0.0
    }

    /// Remove specific remote client (for manual management)
    pub async fn remove_remote_client(&self, url: &str) -> bool {
        if let Some(cache) = &self.remote_clients {
            let mut cache_guard = cache.lock().await;
            cache_guard.pop(url).is_some()
        } else {
            false
        }
    }

    /// Check the connection health status of a specific remote client
    pub async fn check_client_health(&self, url: &str) -> ConnectionHealth {
        if let Some(cache) = &self.remote_clients {
            let mut cache_guard = cache.lock().await;
            if let Some(client) = cache_guard.get(url) {
                let online = client.lock().await.is_online().await;
                if online {
                    ConnectionHealth::Healthy
                } else {
                    ConnectionHealth::Unhealthy("Client reports offline".to_string())
                }
            } else {
                ConnectionHealth::Unknown
            }
        } else {
            ConnectionHealth::Unknown
        }
    }

    /// Health check all remote client connections
    ///
    /// # Parameters
    /// - `remove_unhealthy`: Whether to remove unhealthy connections
    ///
    /// # Returns
    /// - `Ok(HealthCheckResult)`: Health check result
    pub async fn health_check_all_clients(&self, remove_unhealthy: bool) -> Result<HealthCheckResult> {
        if let Some(cache) = &self.remote_clients {
            let mut cache_guard = cache.lock().await;
            let mut result = HealthCheckResult::default();
            let mut to_remove = Vec::new();

            // Check all clients
            for (url, client) in cache_guard.iter() {
                let online = client.lock().await.is_online().await;
                let health = if online {
                    result.healthy_count += 1;
                    ConnectionHealth::Healthy
                } else {
                    result.unhealthy_count += 1;
                    ConnectionHealth::Unhealthy("Client reports offline".to_string())
                };

                if health != ConnectionHealth::Healthy {
                    warn!("Remote client {} is unhealthy: {:?}", url, health);
                    if remove_unhealthy {
                        to_remove.push(url.clone());
                    }
                } else {
                    debug!("Remote client {} is healthy", url);
                }
            }

            // Remove unhealthy connections
            for url in to_remove {
                if cache_guard.pop(&url).is_some() {
                    result.removed_count += 1;
                    info!("Removed unhealthy remote client: {}", url);
                }
            }

            Ok(result)
        } else {
            Ok(HealthCheckResult::default())
        }
    }
}

impl Drop for NsLockMap {
    fn drop(&mut self) {
        if let Some(tx) = &self.health_check_stop_tx {
            let _ = tx.send(());
        }
    }
}

/// Health check result
#[derive(Debug, Default)]
pub struct HealthCheckResult {
    /// Healthy connection count
    pub healthy_count: usize,
    /// Unhealthy connection count
    pub unhealthy_count: usize,
    /// Removed connection count
    pub removed_count: usize,
}

impl HealthCheckResult {
    /// Get total connection count
    pub fn total_count(&self) -> usize {
        self.healthy_count + self.unhealthy_count
    }

    /// Get health rate
    pub fn health_rate(&self) -> f64 {
        let total = self.total_count();
        if total == 0 {
            1.0
        } else {
            self.healthy_count as f64 / total as f64
        }
    }
}

/// Namespace lock client enum
///
/// Supports both local lock and remote lock modes
#[derive(Debug)]
pub enum NamespaceLock {
    /// Local lock client
    Local(LocalLockMap),
    /// Remote lock client (new)
    Remote(Arc<Mutex<RemoteClient>>),
    /// Remote lock client (from cache)
    Cached(Arc<Mutex<RemoteClient>>),
}

impl NamespaceLock {
    /// Create local namespace lock
    pub fn new_local() -> Self {
        Self::Local(LocalLockMap::new())
    }

    /// Create remote namespace lock
    pub fn new_remote(url: Url) -> Self {
        Self::Remote(Arc::new(Mutex::new(RemoteClient::from_url(url))))
    }

    /// Create cached remote namespace lock
    pub fn new_cached(client: Arc<Mutex<RemoteClient>>) -> Self {
        Self::Cached(client)
    }

    /// Batch lock
    pub async fn lock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        match self {
            Self::Local(local) => local.lock_batch(resources, owner, timeout, None).await,
            Self::Remote(remote) | Self::Cached(remote) => {
                let args = crate::lock_args::LockArgs {
                    uid: uuid::Uuid::new_v4().to_string(),
                    resources: resources.to_vec(),
                    owner: owner.to_string(),
                    source: "namespace".to_string(),
                    quorum: 1,
                };
                let mut client = remote.lock().await;
                client.lock(&args).await
            }
        }
    }

    /// Batch unlock
    pub async fn unlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        match self {
            Self::Local(local) => local.unlock_batch(resources, owner).await,
            Self::Remote(remote) | Self::Cached(remote) => {
                let args = crate::lock_args::LockArgs {
                    uid: uuid::Uuid::new_v4().to_string(),
                    resources: resources.to_vec(),
                    owner: owner.to_string(),
                    source: "namespace".to_string(),
                    quorum: 1,
                };
                let mut client = remote.lock().await;
                client.unlock(&args).await.map(|_| ())
            }
        }
    }

    /// Batch read lock
    pub async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        match self {
            Self::Local(local) => local.rlock_batch(resources, owner, timeout, None).await,
            Self::Remote(remote) | Self::Cached(remote) => {
                let args = crate::lock_args::LockArgs {
                    uid: uuid::Uuid::new_v4().to_string(),
                    resources: resources.to_vec(),
                    owner: owner.to_string(),
                    source: "namespace".to_string(),
                    quorum: 1,
                };
                let mut client = remote.lock().await;
                client.rlock(&args).await
            }
        }
    }

    /// Batch release read lock
    pub async fn runlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        match self {
            Self::Local(local) => local.runlock_batch(resources, owner).await,
            Self::Remote(remote) | Self::Cached(remote) => {
                let args = crate::lock_args::LockArgs {
                    uid: uuid::Uuid::new_v4().to_string(),
                    resources: resources.to_vec(),
                    owner: owner.to_string(),
                    source: "namespace".to_string(),
                    quorum: 1,
                };
                let mut client = remote.lock().await;
                client.runlock(&args).await.map(|_| ())
            }
        }
    }

    /// Check connection health status
    pub async fn check_health(&self) -> ConnectionHealth {
        match self {
            Self::Local(_) => ConnectionHealth::Healthy, // Local connection is always healthy
            Self::Remote(remote) | Self::Cached(remote) => {
                let online = remote.lock().await.is_online().await;
                if online {
                    ConnectionHealth::Healthy
                } else {
                    ConnectionHealth::Unhealthy("Client reports offline".to_string())
                }
            }
        }
    }
}

/// Namespace lock manager trait
#[async_trait]
pub trait NamespaceLockManager: Send + Sync {
    /// Batch get write lock
    async fn lock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool>;

    /// Batch release write lock
    async fn unlock_batch(&self, resources: &[String], owner: &str) -> Result<()>;

    /// Batch get read lock
    async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool>;

    /// Batch release read lock
    async fn runlock_batch(&self, resources: &[String], owner: &str) -> Result<()>;
}

#[async_trait]
impl NamespaceLockManager for NsLockMap {
    async fn lock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        self.lock_batch(resources, owner, timeout).await
    }

    async fn unlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        self.unlock_batch(resources, owner).await
    }

    async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        self.rlock_batch(resources, owner, timeout).await
    }

    async fn runlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        self.runlock_batch(resources, owner).await
    }
}

#[async_trait]
impl NamespaceLockManager for NamespaceLock {
    async fn lock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        self.lock_batch(resources, owner, timeout).await
    }

    async fn unlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        self.unlock_batch(resources, owner).await
    }

    async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        self.rlock_batch(resources, owner, timeout).await
    }

    async fn runlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        self.runlock_batch(resources, owner).await
    }
}

type RemoteClientCache = Option<Arc<Mutex<LruCache<String, Arc<Mutex<RemoteClient>>>>>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_ns_lock_map() {
        let ns_lock = NsLockMap::new(false, None);
        let resources = vec!["test1".to_string(), "test2".to_string()];

        // Test batch lock
        let result = ns_lock.lock_batch(&resources, "test_owner", Duration::from_millis(100)).await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Test batch unlock
        let result = ns_lock.unlock_batch(&resources, "test_owner").await;
        assert!(result.is_ok());

        // Test new_nslock
        let client = ns_lock.new_nslock(None).await.unwrap();
        assert!(matches!(client, NamespaceLock::Local(_)));
    }

    #[tokio::test]
    async fn test_distributed_ns_lock_map() {
        let ns_lock = NsLockMap::new(true, None);
        let url = Url::parse("http://localhost:8080").unwrap();

        // Test new_nslock
        let client = ns_lock.new_nslock(Some(url.clone())).await.unwrap();
        assert!(matches!(client, NamespaceLock::Cached(_)));

        // Test cache reuse
        let client2 = ns_lock.new_nslock(Some(url)).await.unwrap();
        assert!(matches!(client2, NamespaceLock::Cached(_)));

        // Verify cache count
        assert_eq!(ns_lock.cached_client_count().await, 1);

        // Test direct operation should fail
        let resources = vec!["test1".to_string(), "test2".to_string()];
        let result = ns_lock.lock_batch(&resources, "test_owner", Duration::from_millis(100)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_namespace_lock_local() {
        let ns_lock = NamespaceLock::new_local();
        let resources = vec!["test1".to_string(), "test2".to_string()];

        // Test batch lock
        let result = ns_lock.lock_batch(&resources, "test_owner", Duration::from_millis(100)).await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Test batch unlock
        let result = ns_lock.unlock_batch(&resources, "test_owner").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_new_nslock_remote_without_url() {
        let ns_lock = NsLockMap::new(true, None);
        let result = ns_lock.new_nslock(None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let ns_lock = NsLockMap::new(false, None);
        let resources = vec!["batch1".to_string(), "batch2".to_string(), "batch3".to_string()];

        // Test batch write lock
        let result = ns_lock
            .lock_batch(&resources, "batch_owner", Duration::from_millis(100))
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Test batch unlock
        let result = ns_lock.unlock_batch(&resources, "batch_owner").await;
        assert!(result.is_ok());

        // Test batch read lock
        let result = ns_lock
            .rlock_batch(&resources, "batch_reader", Duration::from_millis(100))
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Test batch release read lock
        let result = ns_lock.runlock_batch(&resources, "batch_reader").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cache_management() {
        let ns_lock = NsLockMap::new(true, None);

        // Add multiple different URLs
        let url1 = Url::parse("http://localhost:8080").unwrap();
        let url2 = Url::parse("http://localhost:8081").unwrap();

        let _client1 = ns_lock.new_nslock(Some(url1)).await.unwrap();
        let _client2 = ns_lock.new_nslock(Some(url2)).await.unwrap();

        assert_eq!(ns_lock.cached_client_count().await, 2);

        // Clean up cache
        ns_lock.clear_remote_cache().await;
        assert_eq!(ns_lock.cached_client_count().await, 0);
    }

    #[tokio::test]
    async fn test_lru_cache_behavior() {
        // Create a cache with a capacity of 2
        let ns_lock = NsLockMap::new(true, Some(2));

        let url1 = Url::parse("http://localhost:8080").unwrap();
        let url2 = Url::parse("http://localhost:8081").unwrap();
        let url3 = Url::parse("http://localhost:8082").unwrap();

        // Add the first two URLs
        let _client1 = ns_lock.new_nslock(Some(url1.clone())).await.unwrap();
        let _client2 = ns_lock.new_nslock(Some(url2.clone())).await.unwrap();

        assert_eq!(ns_lock.cached_client_count().await, 2);
        assert_eq!(ns_lock.cache_capacity().await, 2);

        // Add the third URL, which should trigger LRU eviction
        let _client3 = ns_lock.new_nslock(Some(url3.clone())).await.unwrap();

        // Cache count should still be 2 (due to capacity limit)
        assert_eq!(ns_lock.cached_client_count().await, 2);

        // Verify that the first URL was evicted (least recently used)
        assert!(!ns_lock.remove_remote_client(url1.as_ref()).await);

        // Verify that the second and third URLs are still in the cache
        assert!(ns_lock.remove_remote_client(url2.as_ref()).await);
        assert!(ns_lock.remove_remote_client(url3.as_ref()).await);
    }

    #[tokio::test]
    async fn test_lru_access_order() {
        let ns_lock = NsLockMap::new(true, Some(2));

        let url1 = Url::parse("http://localhost:8080").unwrap();
        let url2 = Url::parse("http://localhost:8081").unwrap();
        let url3 = Url::parse("http://localhost:8082").unwrap();

        // Add the first two URLs
        let _client1 = ns_lock.new_nslock(Some(url1.clone())).await.unwrap();
        let _client2 = ns_lock.new_nslock(Some(url2.clone())).await.unwrap();

        // Re-access the first URL, making it the most recently used
        let _client1_again = ns_lock.new_nslock(Some(url1.clone())).await.unwrap();

        // Add the third URL, which should evict the second URL (least recently used)
        let _client3 = ns_lock.new_nslock(Some(url3.clone())).await.unwrap();

        // Verify that the second URL was evicted
        assert!(!ns_lock.remove_remote_client(url2.as_ref()).await);

        // Verify that the first and third URLs are still in the cache
        assert!(ns_lock.remove_remote_client(url1.as_ref()).await);
        assert!(ns_lock.remove_remote_client(url3.as_ref()).await);
    }

    #[tokio::test]
    async fn test_health_check_result() {
        let result = HealthCheckResult {
            healthy_count: 8,
            unhealthy_count: 2,
            removed_count: 1,
        };

        assert_eq!(result.total_count(), 10);
        assert_eq!(result.health_rate(), 0.8);
    }

    #[tokio::test]
    async fn test_connection_health() {
        let local_lock = NamespaceLock::new_local();
        let health = local_lock.check_health().await;
        assert_eq!(health, ConnectionHealth::Healthy);
    }

    #[tokio::test]
    async fn test_health_check_all_clients() {
        let ns_lock = NsLockMap::new(true, None);

        // Add some clients
        let url1 = Url::parse("http://localhost:8080").unwrap();
        let url2 = Url::parse("http://localhost:8081").unwrap();

        let _client1 = ns_lock.new_nslock(Some(url1)).await.unwrap();
        let _client2 = ns_lock.new_nslock(Some(url2)).await.unwrap();

        // Execute health check (without removing unhealthy connections)
        let result = ns_lock.health_check_all_clients(false).await.unwrap();

        // Verify results
        assert_eq!(result.total_count(), 2);
        // Note: Since there is no real remote service, health check may fail
        // Here we just verify that the method can execute normally
    }
}
