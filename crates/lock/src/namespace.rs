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
use std::sync::Arc;
use std::time::Duration;

use crate::{
    client::LockClient,
    error::{LockError, Result},
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStatus, LockType},
};

/// Namespace lock for managing locks by resource namespaces
#[derive(Debug)]
pub struct NamespaceLock {
    /// Lock clients for this namespace
    clients: Vec<Arc<dyn LockClient>>,
    /// Namespace identifier
    namespace: String,
    /// Quorum size for operations (1 for local, majority for distributed)
    quorum: usize,
}

impl NamespaceLock {
    /// Create new namespace lock
    pub fn new(namespace: String) -> Self {
        Self {
            clients: Vec::new(),
            namespace,
            quorum: 1,
        }
    }

    /// Create namespace lock with clients
    pub fn with_clients(namespace: String, clients: Vec<Arc<dyn LockClient>>) -> Self {
        let quorum = if clients.len() > 1 {
            // For multiple clients (distributed mode), require majority
            (clients.len() / 2) + 1
        } else {
            // For single client (local mode), only need 1
            1
        };

        Self {
            clients,
            namespace,
            quorum,
        }
    }

    /// Create namespace lock with client (compatibility)
    pub fn with_client(client: Arc<dyn LockClient>) -> Self {
        Self::with_clients("default".to_string(), vec![client])
    }

    /// Get namespace identifier
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get resource key for this namespace
    fn get_resource_key(&self, resource: &str) -> String {
        format!("{}:{}", self.namespace, resource)
    }

    /// Acquire lock using clients
    pub async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // For single client, use it directly
        if self.clients.len() == 1 {
            return self.clients[0].acquire_lock(request).await;
        }

        // For multiple clients, try to acquire from all clients and require quorum
        let futures: Vec<_> = self
            .clients
            .iter()
            .map(|client| async move { client.acquire_lock(request).await })
            .collect();

        let results = futures::future::join_all(futures).await;
        let successful = results.into_iter().filter_map(|r| r.ok()).filter(|r| r.success).count();

        if successful >= self.quorum {
            Ok(LockResponse::success(
                LockInfo {
                    id: LockId::new_deterministic(&request.resource),
                    resource: request.resource.clone(),
                    lock_type: request.lock_type,
                    status: LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.ttl,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                },
                Duration::ZERO,
            ))
        } else {
            Ok(LockResponse::failure("Failed to acquire quorum".to_string(), Duration::ZERO))
        }
    }

    /// Release lock using clients
    pub async fn release_lock(&self, lock_id: &LockId) -> Result<bool> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // For single client, use it directly
        if self.clients.len() == 1 {
            return self.clients[0].release(lock_id).await;
        }

        // For multiple clients, try to release from all clients
        let futures: Vec<_> = self
            .clients
            .iter()
            .map(|client| {
                let id = lock_id.clone();
                async move { client.release(&id).await }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let successful = results.into_iter().filter_map(|r| r.ok()).filter(|&r| r).count();

        // For release, if any succeed, consider it successful
        Ok(successful > 0)
    }

    /// Get health information
    pub async fn get_health(&self) -> crate::types::HealthInfo {
        let lock_stats = self.get_stats().await;
        let mut health = crate::types::HealthInfo {
            node_id: self.namespace.clone(),
            lock_stats,
            ..Default::default()
        };

        // Check client status
        let mut connected_clients = 0;
        for client in &self.clients {
            if client.is_online().await {
                connected_clients += 1;
            }
        }

        health.status = if connected_clients > 0 {
            crate::types::HealthStatus::Healthy
        } else {
            crate::types::HealthStatus::Degraded
        };
        health.connected_nodes = connected_clients;
        health.total_nodes = self.clients.len();

        health
    }

    /// Get namespace statistics
    pub async fn get_stats(&self) -> crate::types::LockStats {
        let mut stats = crate::types::LockStats::default();

        // Try to get stats from clients
        for client in &self.clients {
            if let Ok(client_stats) = client.get_stats().await {
                stats.successful_acquires += client_stats.successful_acquires;
                stats.failed_acquires += client_stats.failed_acquires;
            }
        }

        stats
    }
}

impl Default for NamespaceLock {
    fn default() -> Self {
        Self::new("default".to_string())
    }
}

/// Namespace lock manager trait
#[async_trait]
pub trait NamespaceLockManager: Send + Sync {
    /// Batch get write lock
    async fn lock_batch(&self, resources: &[String], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool>;

    /// Batch release write lock
    async fn unlock_batch(&self, resources: &[String], owner: &str) -> Result<()>;

    /// Batch get read lock
    async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool>;

    /// Batch release read lock
    async fn runlock_batch(&self, resources: &[String], owner: &str) -> Result<()>;
}

#[async_trait]
impl NamespaceLockManager for NamespaceLock {
    async fn lock_batch(&self, resources: &[String], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // For each resource, create a lock request and try to acquire using clients
        for resource in resources {
            let namespaced_resource = self.get_resource_key(resource);
            let request = LockRequest::new(&namespaced_resource, LockType::Exclusive, owner)
                .with_acquire_timeout(timeout)
                .with_ttl(ttl);

            let response = self.acquire_lock(&request).await?;
            if !response.success {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn unlock_batch(&self, resources: &[String], _owner: &str) -> Result<()> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // For each resource, create a lock ID and try to release using clients
        for resource in resources {
            let namespaced_resource = self.get_resource_key(resource);
            let lock_id = LockId::new_deterministic(&namespaced_resource);
            let _ = self.release_lock(&lock_id).await?;
        }
        Ok(())
    }

    async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // For each resource, create a shared lock request and try to acquire using clients
        for resource in resources {
            let namespaced_resource = self.get_resource_key(resource);
            let request = LockRequest::new(&namespaced_resource, LockType::Shared, owner)
                .with_acquire_timeout(timeout)
                .with_ttl(ttl);

            let response = self.acquire_lock(&request).await?;
            if !response.success {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn runlock_batch(&self, resources: &[String], _owner: &str) -> Result<()> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // For each resource, create a lock ID and try to release using clients
        for resource in resources {
            let namespaced_resource = self.get_resource_key(resource);
            let lock_id = LockId::new_deterministic(&namespaced_resource);
            let _ = self.release_lock(&lock_id).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::LocalClient;

    use super::*;

    #[tokio::test]
    async fn test_namespace_lock_local() {
        let ns_lock = NamespaceLock::with_client(Arc::new(LocalClient::new()));
        let resources = vec!["test1".to_string(), "test2".to_string()];

        // Test batch lock
        let result = ns_lock
            .lock_batch(&resources, "test_owner", Duration::from_millis(100), Duration::from_secs(10))
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Test batch unlock
        let result = ns_lock.unlock_batch(&resources, "test_owner").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_connection_health() {
        let local_lock = NamespaceLock::new("test-namespace".to_string());
        let health = local_lock.get_health().await;
        assert_eq!(health.status, crate::types::HealthStatus::Degraded); // No clients
    }

    #[tokio::test]
    async fn test_namespace_lock_creation() {
        let ns_lock = NamespaceLock::new("test-namespace".to_string());
        assert_eq!(ns_lock.namespace(), "test-namespace");
    }

    #[tokio::test]
    async fn test_namespace_lock_new_local() {
        let ns_lock = NamespaceLock::with_client(Arc::new(LocalClient::new()));
        assert_eq!(ns_lock.namespace(), "default");
        assert_eq!(ns_lock.clients.len(), 1);
        assert!(ns_lock.clients[0].is_local().await);

        // Test that it can perform lock operations
        let resources = vec!["test-resource".to_string()];
        let result = ns_lock
            .lock_batch(&resources, "test-owner", Duration::from_millis(100), Duration::from_secs(10))
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_namespace_lock_resource_key() {
        let ns_lock = NamespaceLock::new("test-namespace".to_string());

        // Test resource key generation
        let resource_key = ns_lock.get_resource_key("test-resource");
        assert_eq!(resource_key, "test-namespace:test-resource");
    }
}
