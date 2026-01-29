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

use crate::{
    ObjectKey,
    client::LockClient,
    distributed_lock::{DistributedLock, DistributedLockGuard},
    error::Result,
    fast_lock::FastLockGuard,
    local_lock::LocalLock,
    types::{LockId, LockRequest},
};
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
mod tests;

/// Unified guard for namespace locks
/// Supports both DistributedLockGuard (for Distributed locks) and FastLockGuard (for Local locks)
#[derive(Debug)]
pub enum NamespaceLockGuard {
    /// Standard guard for Distributed locks
    Standard(DistributedLockGuard),
    /// Fast guard for Local locks using GlobalLockManager
    Fast(FastLockGuard),
}

/// Wrapper for NamespaceLock that provides convenient lock acquisition methods
/// This wrapper holds the lock instance and resource information for easy lock acquisition
#[derive(Debug)]
pub struct NamespaceLockWrapper {
    lock: NamespaceLock,
    resource: ObjectKey,
    owner: String,
}

impl NamespaceLockWrapper {
    /// Create a new wrapper with the lock, resource, and owner
    pub fn new(lock: NamespaceLock, resource: ObjectKey, owner: String) -> Self {
        Self { lock, resource, owner }
    }

    /// Acquire write lock (exclusive lock) with timeout
    /// Returns the guard if acquisition succeeds, or an error if it fails
    pub async fn get_write_lock(&self, timeout: Duration) -> std::result::Result<NamespaceLockGuard, crate::error::LockError> {
        self.lock.get_write_lock(self.resource.clone(), &self.owner, timeout).await
    }

    /// Acquire read lock (shared lock) with timeout
    /// Returns the guard if acquisition succeeds, or an error if it fails
    pub async fn get_read_lock(&self, timeout: Duration) -> std::result::Result<NamespaceLockGuard, crate::error::LockError> {
        self.lock.get_read_lock(self.resource.clone(), &self.owner, timeout).await
    }
}

impl NamespaceLockGuard {
    /// Get the lock ID if available (only for Standard guards)
    pub fn lock_id(&self) -> Option<&LockId> {
        match self {
            Self::Standard(guard) => Some(guard.lock_id()),
            Self::Fast(_) => None,
        }
    }

    /// Get the object key if available (only for Fast guards)
    pub fn key(&self) -> Option<&ObjectKey> {
        match self {
            Self::Standard(_) => None,
            Self::Fast(guard) => Some(guard.key()),
        }
    }

    /// Manually release the lock early
    pub fn release(&mut self) -> bool {
        match self {
            Self::Standard(guard) => {
                // DistributedLockGuard::release() actually releases the lock and then disarms
                guard.release()
            }
            Self::Fast(guard) => guard.release(),
        }
    }

    /// Check if the lock has been released
    pub fn is_released(&self) -> bool {
        match self {
            Self::Standard(guard) => {
                // Check if the guard has been disarmed, which indicates the lock was released
                guard.is_disarmed()
            }
            Self::Fast(guard) => guard.is_released(),
        }
    }
}

/// Namespace lock for managing locks by resource namespaces
/// Supports DistributedLock and LocalLock
#[derive(Debug)]
pub enum NamespaceLock {
    /// Distributed lock (distributed use case)
    Distributed(DistributedLock),
    /// Local lock using GlobalLockManager (high-performance local locking)
    Local(LocalLock),
}

impl NamespaceLock {
    /// Create new namespace lock with single client (local use case)
    /// Uses DistributedLock with quorum=1 for single client
    pub fn new(namespace: String, client: Arc<dyn LockClient>) -> Self {
        Self::Distributed(DistributedLock::new(namespace, vec![client], 1))
    }

    /// Create namespace lock with client (compatibility)
    /// Uses DistributedLock with quorum=1 for single client
    pub fn with_client(client: Arc<dyn LockClient>) -> Self {
        Self::Distributed(DistributedLock::new("default".to_string(), vec![client], 1))
    }

    /// Create namespace lock with GlobalLockManager (high-performance local locking)
    pub fn with_local_manager(namespace: String, manager: Arc<crate::GlobalLockManager>) -> Self {
        Self::Local(LocalLock::new(namespace, manager))
    }

    /// Create namespace lock with clients
    /// Uses DistributedLock with appropriate quorum
    pub fn with_clients(namespace: String, clients: Vec<Arc<dyn LockClient>>) -> Self {
        // Multiple clients: use DistributedLock with majority quorum
        let quorum = if clients.len() > 1 { (clients.len() / 2) + 1 } else { 1 };
        Self::Distributed(DistributedLock::new(namespace, clients, quorum))
    }

    /// Create namespace lock with clients and an explicit quorum size.
    /// Quorum will be clamped into [1, clients.len()].
    pub fn with_clients_and_quorum(namespace: String, clients: Vec<Arc<dyn LockClient>>, quorum: usize) -> Self {
        Self::Distributed(DistributedLock::new(namespace, clients, quorum))
    }

    /// Get namespace identifier
    pub fn namespace(&self) -> &str {
        match self {
            Self::Distributed(lock) => lock.namespace(),
            Self::Local(lock) => lock.namespace(),
        }
    }

    /// Get resource key for this namespace
    pub fn get_resource_key(&self, resource: &ObjectKey) -> String {
        match self {
            Self::Distributed(lock) => lock.get_resource_key(resource),
            Self::Local(lock) => lock.get_resource_key(resource),
        }
    }

    /// Acquire a lock and return a RAII guard that will release asynchronously on Drop.
    /// This is a thin wrapper around `acquire_lock` and will only create a guard when acquisition succeeds.
    pub async fn acquire_guard(&self, request: &LockRequest) -> Result<Option<NamespaceLockGuard>> {
        match self {
            Self::Distributed(lock) => lock
                .acquire_guard(request)
                .await
                .map(|opt| opt.map(NamespaceLockGuard::Standard)),
            Self::Local(lock) => lock.acquire_guard(request).await.map(|opt| opt.map(NamespaceLockGuard::Fast)),
        }
    }

    /// Convenience: acquire exclusive lock as a guard
    pub async fn lock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<NamespaceLockGuard>> {
        match self {
            Self::Distributed(lock) => lock
                .lock_guard(resource, owner, timeout, ttl)
                .await
                .map(|opt| opt.map(NamespaceLockGuard::Standard)),
            Self::Local(lock) => lock
                .lock_guard(resource, owner, timeout, ttl)
                .await
                .map(|opt| opt.map(NamespaceLockGuard::Fast)),
        }
    }

    /// Convenience: acquire shared lock as a guard
    pub async fn rlock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<NamespaceLockGuard>> {
        match self {
            Self::Distributed(lock) => lock
                .rlock_guard(resource, owner, timeout, ttl)
                .await
                .map(|opt| opt.map(NamespaceLockGuard::Standard)),
            Self::Local(lock) => lock
                .rlock_guard(resource, owner, timeout, ttl)
                .await
                .map(|opt| opt.map(NamespaceLockGuard::Fast)),
        }
    }

    /// Acquire write lock (exclusive lock) with timeout
    /// Returns the guard if acquisition succeeds, or an error if it fails
    pub async fn get_write_lock(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
    ) -> std::result::Result<NamespaceLockGuard, crate::error::LockError> {
        let ttl = crate::fast_lock::DEFAULT_LOCK_TIMEOUT;
        let resource_str = format!("{}", resource);
        match self.lock_guard(resource, owner, timeout, ttl).await {
            Ok(Some(guard)) => Ok(guard),
            Ok(None) => {
                // None can mean timeout or other failure - check if it's a quorum error
                // For distributed locks, quorum errors are already converted to LockError::QuorumNotReached
                // So if we get None here, it's likely a timeout
                Err(crate::error::LockError::timeout(resource_str, timeout))
            }
            Err(e) => Err(e),
        }
    }

    /// Acquire read lock (shared lock) with timeout
    /// Returns the guard if acquisition succeeds, or an error if it fails
    pub async fn get_read_lock(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
    ) -> std::result::Result<NamespaceLockGuard, crate::error::LockError> {
        let ttl = crate::fast_lock::DEFAULT_LOCK_TIMEOUT;
        let resource_str = format!("{}", resource);
        match self.rlock_guard(resource, owner, timeout, ttl).await {
            Ok(Some(guard)) => Ok(guard),
            Ok(None) => Err(crate::error::LockError::timeout(resource_str, timeout)),
            Err(e) => Err(e),
        }
    }

    /// Get health information
    pub async fn get_health(&self) -> crate::types::HealthInfo {
        let lock_stats = self.get_stats().await;
        let namespace = self.namespace().to_string();
        let mut health = crate::types::HealthInfo {
            node_id: namespace.clone(),
            lock_stats,
            ..Default::default()
        };

        match self {
            Self::Distributed(lock) => {
                // Check client status - parallelize async calls for better performance
                let clients = lock.clients();
                let client_checks: Vec<_> = clients.iter().map(|client| client.is_online()).collect();

                let results = futures::future::join_all(client_checks).await;
                let connected_clients = results.iter().filter(|&&online| online).count();

                let quorum = if clients.len() > 1 { (clients.len() / 2) + 1 } else { 1 };

                health.status = if connected_clients >= quorum {
                    crate::types::HealthStatus::Healthy
                } else {
                    crate::types::HealthStatus::Degraded
                };
                health.connected_nodes = connected_clients;
                health.total_nodes = clients.len();
            }
            Self::Local(_) => {
                // Local locks are always healthy (they use GlobalLockManager which is always available)
                health.status = crate::types::HealthStatus::Healthy;
                health.connected_nodes = 1;
                health.total_nodes = 1;
            }
        }

        health
    }

    /// Get namespace statistics
    pub async fn get_stats(&self) -> crate::types::LockStats {
        let mut stats = crate::types::LockStats::default();

        match self {
            Self::Distributed(lock) => {
                // Parallelize stats collection for better performance
                let stats_futures: Vec<_> = lock.clients().iter().map(|client| client.get_stats()).collect();

                let results = futures::future::join_all(stats_futures).await;

                for result in results {
                    match result {
                        Ok(client_stats) => {
                            stats.successful_acquires += client_stats.successful_acquires;
                            stats.failed_acquires += client_stats.failed_acquires;
                        }
                        Err(e) => {
                            tracing::debug!("Failed to get stats from client: {}", e);
                        }
                    }
                }
            }
            Self::Local(_) => {
                // Local locks use GlobalLockManager which doesn't expose detailed stats
                // Stats are tracked internally but not exposed through the same interface
                // We leave stats at default (0) for now
            }
        }

        stats
    }
}

impl Default for NamespaceLock {
    fn default() -> Self {
        use crate::client::ClientFactory;
        Self::new("default".to_string(), ClientFactory::create_local())
    }
}
