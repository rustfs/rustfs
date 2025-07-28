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
    pub fn get_resource_key(&self, resource: &str) -> String {
        format!("{}:{}", self.namespace, resource)
    }

    /// Acquire lock using clients with transactional semantics (all-or-nothing)
    pub async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // For single client, use it directly
        if self.clients.len() == 1 {
            return self.clients[0].acquire_lock(request).await;
        }

        // Two-phase commit for distributed lock acquisition
        self.acquire_lock_with_2pc(request).await
    }

    /// Two-phase commit lock acquisition: all nodes must succeed or all fail
    async fn acquire_lock_with_2pc(&self, request: &LockRequest) -> Result<LockResponse> {
        // Phase 1: Prepare - try to acquire lock on all clients
        let futures: Vec<_> = self
            .clients
            .iter()
            .enumerate()
            .map(|(idx, client)| async move {
                let result = client.acquire_lock(request).await;
                (idx, result)
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let mut successful_clients = Vec::new();
        let mut failed_clients = Vec::new();

        // Collect results
        for (idx, result) in results {
            match result {
                Ok(response) if response.success => {
                    successful_clients.push(idx);
                }
                _ => {
                    failed_clients.push(idx);
                }
            }
        }

        // Check if we have enough successful acquisitions for quorum
        if successful_clients.len() >= self.quorum {
            // Phase 2a: Commit - we have quorum, but need to ensure consistency
            // If not all clients succeeded, we need to rollback for consistency
            if successful_clients.len() < self.clients.len() {
                // Rollback all successful acquisitions to maintain consistency
                self.rollback_acquisitions(request, &successful_clients).await;
                return Ok(LockResponse::failure(
                    "Partial success detected, rolled back for consistency".to_string(),
                    Duration::ZERO,
                ));
            }

            // All clients succeeded - lock acquired successfully
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
            // Phase 2b: Abort - insufficient quorum, rollback any successful acquisitions
            if !successful_clients.is_empty() {
                self.rollback_acquisitions(request, &successful_clients).await;
            }
            Ok(LockResponse::failure(
                format!("Failed to acquire quorum: {}/{} required", successful_clients.len(), self.quorum),
                Duration::ZERO,
            ))
        }
    }

    /// Rollback lock acquisitions on specified clients
    async fn rollback_acquisitions(&self, request: &LockRequest, client_indices: &[usize]) {
        let lock_id = LockId::new_deterministic(&request.resource);
        let rollback_futures: Vec<_> = client_indices
            .iter()
            .filter_map(|&idx| self.clients.get(idx))
            .map(|client| async {
                if let Err(e) = client.release(&lock_id).await {
                    tracing::warn!("Failed to rollback lock on client: {}", e);
                }
            })
            .collect();

        futures::future::join_all(rollback_futures).await;
        tracing::info!(
            "Rolled back {} lock acquisitions for resource: {}",
            client_indices.len(),
            request.resource
        );
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

        // Transactional batch lock: all resources must be locked or none
        let mut acquired_resources = Vec::new();

        for resource in resources {
            let namespaced_resource = self.get_resource_key(resource);
            let request = LockRequest::new(&namespaced_resource, LockType::Exclusive, owner)
                .with_acquire_timeout(timeout)
                .with_ttl(ttl);

            let response = self.acquire_lock(&request).await?;
            if response.success {
                acquired_resources.push(namespaced_resource);
            } else {
                // Rollback all previously acquired locks
                self.rollback_batch_locks(&acquired_resources, owner).await;
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn unlock_batch(&self, resources: &[String], _owner: &str) -> Result<()> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // Release all locks (best effort)
        let release_futures: Vec<_> = resources
            .iter()
            .map(|resource| {
                let namespaced_resource = self.get_resource_key(resource);
                let lock_id = LockId::new_deterministic(&namespaced_resource);
                async move {
                    if let Err(e) = self.release_lock(&lock_id).await {
                        tracing::warn!("Failed to release lock for resource {}: {}", resource, e);
                    }
                }
            })
            .collect();

        futures::future::join_all(release_futures).await;
        Ok(())
    }

    async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // Transactional batch read lock: all resources must be locked or none
        let mut acquired_resources = Vec::new();

        for resource in resources {
            let namespaced_resource = self.get_resource_key(resource);
            let request = LockRequest::new(&namespaced_resource, LockType::Shared, owner)
                .with_acquire_timeout(timeout)
                .with_ttl(ttl);

            let response = self.acquire_lock(&request).await?;
            if response.success {
                acquired_resources.push(namespaced_resource);
            } else {
                // Rollback all previously acquired read locks
                self.rollback_batch_locks(&acquired_resources, owner).await;
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn runlock_batch(&self, resources: &[String], _owner: &str) -> Result<()> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // Release all read locks (best effort)
        let release_futures: Vec<_> = resources
            .iter()
            .map(|resource| {
                let namespaced_resource = self.get_resource_key(resource);
                let lock_id = LockId::new_deterministic(&namespaced_resource);
                async move {
                    if let Err(e) = self.release_lock(&lock_id).await {
                        tracing::warn!("Failed to release read lock for resource {}: {}", resource, e);
                    }
                }
            })
            .collect();

        futures::future::join_all(release_futures).await;
        Ok(())
    }
}

impl NamespaceLock {
    /// Rollback batch lock acquisitions
    async fn rollback_batch_locks(&self, acquired_resources: &[String], _owner: &str) {
        let rollback_futures: Vec<_> = acquired_resources
            .iter()
            .map(|resource| {
                let lock_id = LockId::new_deterministic(resource);
                async move {
                    if let Err(e) = self.release_lock(&lock_id).await {
                        tracing::warn!("Failed to rollback lock for resource {}: {}", resource, e);
                    }
                }
            })
            .collect();

        futures::future::join_all(rollback_futures).await;
        tracing::info!("Rolled back {} batch lock acquisitions", acquired_resources.len());
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

    #[tokio::test]
    async fn test_transactional_batch_lock() {
        let ns_lock = NamespaceLock::with_client(Arc::new(LocalClient::new()));
        let resources = vec!["resource1".to_string(), "resource2".to_string(), "resource3".to_string()];

        // First, acquire one of the resources to simulate conflict
        let conflicting_request = LockRequest::new(ns_lock.get_resource_key("resource2"), LockType::Exclusive, "other_owner")
            .with_ttl(Duration::from_secs(10));

        let response = ns_lock.acquire_lock(&conflicting_request).await.unwrap();
        assert!(response.success);

        // Now try batch lock - should fail and rollback
        let result = ns_lock
            .lock_batch(&resources, "test_owner", Duration::from_millis(10), Duration::from_secs(5))
            .await;

        assert!(result.is_ok());
        assert!(!result.unwrap()); // Should fail due to conflict

        // Verify that no locks were left behind (all rolled back)
        for resource in &resources {
            if resource != "resource2" {
                // Skip the one we intentionally locked
                let check_request = LockRequest::new(ns_lock.get_resource_key(resource), LockType::Exclusive, "verify_owner")
                    .with_ttl(Duration::from_secs(1));

                let check_response = ns_lock.acquire_lock(&check_request).await.unwrap();
                assert!(check_response.success, "Resource {resource} should be available after rollback");

                // Clean up
                let lock_id = LockId::new_deterministic(&ns_lock.get_resource_key(resource));
                let _ = ns_lock.release_lock(&lock_id).await;
            }
        }
    }

    #[tokio::test]
    async fn test_distributed_lock_consistency() {
        // Create a namespace with multiple local clients to simulate distributed scenario
        let client1: Arc<dyn LockClient> = Arc::new(LocalClient::new());
        let client2: Arc<dyn LockClient> = Arc::new(LocalClient::new());
        let clients = vec![client1, client2];

        let ns_lock = NamespaceLock::with_clients("test-namespace".to_string(), clients);

        let request = LockRequest::new("test-resource", LockType::Exclusive, "test_owner").with_ttl(Duration::from_secs(10));

        // This should succeed only if ALL clients can acquire the lock
        let response = ns_lock.acquire_lock(&request).await.unwrap();

        // Since we're using separate LocalClient instances, they don't share state
        // so this test demonstrates the consistency check
        assert!(response.success); // Either all succeed or rollback happens
    }
}
