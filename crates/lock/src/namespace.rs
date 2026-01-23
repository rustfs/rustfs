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
    ObjectKey,
    client::LockClient,
    error::{LockError, Result},
    guard::LockGuard,
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStatus, LockType},
};

/// Namespace lock for managing locks by resource namespaces
#[derive(Debug)]
pub struct NamespaceLock {
    /// Lock clients for this namespace
    clients: Vec<Option<Arc<dyn LockClient>>>,
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
    pub fn with_clients(namespace: String, clients: Vec<Option<Arc<dyn LockClient>>>) -> Self {
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

    /// Create namespace lock with clients and an explicit quorum size.
    /// Quorum will be clamped into [1, clients.len()]. For single client, quorum is always 1.
    pub fn with_clients_and_quorum(namespace: String, clients: Vec<Option<Arc<dyn LockClient>>>, quorum: usize) -> Self {
        let q = if clients.len() <= 1 {
            1
        } else {
            quorum.clamp(1, clients.len())
        };

        Self {
            clients,
            namespace,
            quorum: q,
        }
    }

    /// Create namespace lock with client (compatibility)
    pub fn with_client(client: Arc<dyn LockClient>) -> Self {
        Self::with_clients("default".to_string(), vec![Some(client)])
    }

    /// Get namespace identifier
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get resource key for this namespace
    pub fn get_resource_key(&self, resource: &ObjectKey) -> String {
        format!("{}:{}", self.namespace, resource)
    }

    /// Acquire lock using clients with transactional semantics (all-or-nothing)
    pub async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // For single client, use it directly
        if self.clients.len() == 1 {
            if let Some(client) = &self.clients[0] {
                return client.acquire_lock(request).await;
            } else {
                return Err(LockError::internal("No lock client available"));
            }
        }

        // Quorum-based acquisition for distributed mode
        let (resp, _idxs) = self.acquire_lock_quorum(request).await?;
        Ok(resp)
    }

    /// Acquire a lock and return a RAII guard that will release asynchronously on Drop.
    /// This is a thin wrapper around `acquire_lock` and will only create a guard when acquisition succeeds.
    pub async fn acquire_guard(&self, request: &LockRequest) -> Result<Option<LockGuard>> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        if self.clients.len() == 1 {
            if let Some(client) = &self.clients[0] {
                let resp = client.acquire_lock(request).await?;
                if resp.success {
                    return Ok(Some(LockGuard::new(LockId::new_deterministic(&request.resource), vec![client.clone()])));
                }
                return Ok(None);
            } else {
                return Err(LockError::internal("No lock client available"));
            }
        }

        let (resp, idxs) = self.acquire_lock_quorum(request).await?;
        if resp.success {
            let subset: Vec<_> = idxs
                .into_iter()
                .filter_map(|i| self.clients.get(i).and_then(|c| c.clone()))
                .collect();
            Ok(Some(LockGuard::new(LockId::new_deterministic(&request.resource), subset)))
        } else {
            Ok(None)
        }
    }

    /// Convenience: acquire exclusive lock as a guard
    pub async fn lock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<LockGuard>> {
        let req = LockRequest::new(resource, LockType::Exclusive, owner)
            .with_acquire_timeout(timeout)
            .with_ttl(ttl);
        self.acquire_guard(&req).await
    }

    /// Convenience: acquire shared lock as a guard
    pub async fn rlock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<LockGuard>> {
        let req = LockRequest::new(resource, LockType::Shared, owner)
            .with_acquire_timeout(timeout)
            .with_ttl(ttl);
        self.acquire_guard(&req).await
    }

    /// Quorum-based lock acquisition: success if at least `self.quorum` clients succeed.
    /// Returns the LockResponse and the indices of clients that acquired the lock.
    async fn acquire_lock_quorum(&self, request: &LockRequest) -> Result<(LockResponse, Vec<usize>)> {
        let futs: Vec<_> = self
            .clients
            .iter()
            .enumerate()
            .map(|(idx, client_opt)| async move {
                if let Some(client) = client_opt {
                    (idx, Some(client.acquire_lock(request).await))
                } else {
                    (idx, None)
                }
            })
            .collect();

        let results = futures::future::join_all(futs).await;
        let mut successful_clients = Vec::new();
        let mut failed_clients = Vec::new();

        for (idx, res) in results {
            if let Some(resp) = res {
                match resp {
                    Ok(resp) => {
                        if resp.success {
                            successful_clients.push(idx);
                        } else {
                            failed_clients.push(idx);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to acquire lock on client {}: {}", idx, e);
                    }
                }
            }
        }

        if successful_clients.len() >= self.quorum {
            let resp = LockResponse::success(
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
            );
            Ok((resp, successful_clients))
        } else {
            if !successful_clients.is_empty() || !failed_clients.is_empty() {
                successful_clients.extend_from_slice(&failed_clients);
                self.rollback_acquisitions(request, &successful_clients).await;
            }
            let resp = LockResponse::failure(
                format!("Failed to acquire quorum: {}/{} required", successful_clients.len(), self.quorum),
                Duration::ZERO,
            );
            Ok((resp, Vec::new()))
        }
    }

    /// Rollback lock acquisitions on specified clients
    async fn rollback_acquisitions(&self, request: &LockRequest, client_indices: &[usize]) {
        let lock_id = LockId::new_deterministic(&request.resource);
        let rollback_futures: Vec<_> = client_indices
            .iter()
            .filter_map(|&idx| self.clients.get(idx))
            .filter_map(|client| client.as_ref())
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
            if let Some(client) = &self.clients[0] {
                return client.release(lock_id).await;
            } else {
                return Err(LockError::internal("Single client is not available"));
            }
        }

        // For multiple clients, try to release from all clients
        let futures: Vec<_> = self
            .clients
            .iter()
            .filter_map(|client| client.as_ref())
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
            if let Some(client) = client {
                if client.is_online().await {
                    connected_clients += 1;
                }
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
            if let Some(client) = client {
                if let Ok(client_stats) = client.get_stats().await {
                    stats.successful_acquires += client_stats.successful_acquires;
                    stats.failed_acquires += client_stats.failed_acquires;
                }
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
    async fn lock_batch(&self, resources: &[ObjectKey], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool>;

    /// Batch release write lock
    async fn unlock_batch(&self, resources: &[ObjectKey], owner: &str) -> Result<()>;

    /// Batch get read lock
    async fn rlock_batch(&self, resources: &[ObjectKey], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool>;

    /// Batch release read lock
    async fn runlock_batch(&self, resources: &[ObjectKey], owner: &str) -> Result<()>;
}

#[async_trait]
impl NamespaceLockManager for NamespaceLock {
    async fn lock_batch(&self, resources: &[ObjectKey], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // Transactional batch lock: all resources must be locked or none
        let mut acquired_resources = Vec::new();

        for resource in resources {
            let request = LockRequest::new(resource.clone(), LockType::Exclusive, owner)
                .with_acquire_timeout(timeout)
                .with_ttl(ttl);

            let response = self.acquire_lock(&request).await?;
            if response.success {
                acquired_resources.push(resource.clone());
            } else {
                // Rollback all previously acquired locks
                self.rollback_batch_locks(&acquired_resources, owner).await;
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn unlock_batch(&self, resources: &[ObjectKey], _owner: &str) -> Result<()> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // Release all locks (best effort)
        let release_futures: Vec<_> = resources
            .iter()
            .map(|resource| {
                let lock_id = LockId::new_deterministic(resource);
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

    async fn rlock_batch(&self, resources: &[ObjectKey], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // Transactional batch read lock: all resources must be locked or none
        let mut acquired_resources = Vec::new();

        for resource in resources {
            let request = LockRequest::new(resource.clone(), LockType::Shared, owner)
                .with_acquire_timeout(timeout)
                .with_ttl(ttl);

            let response = self.acquire_lock(&request).await?;
            if response.success {
                acquired_resources.push(resource.clone());
            } else {
                // Rollback all previously acquired read locks
                self.rollback_batch_locks(&acquired_resources, owner).await;
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn runlock_batch(&self, resources: &[ObjectKey], _owner: &str) -> Result<()> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        // Release all read locks (best effort)
        let release_futures: Vec<_> = resources
            .iter()
            .map(|resource| {
                let lock_id = LockId::new_deterministic(resource);
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
    async fn rollback_batch_locks(&self, acquired_resources: &[ObjectKey], _owner: &str) {
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
