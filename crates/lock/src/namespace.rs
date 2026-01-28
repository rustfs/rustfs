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
    error::{LockError, Result},
    guard::LockGuard,
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStatus, LockType},
};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};
use std::{sync::atomic::AtomicI32, time::Duration};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Generate a new aggregate lock ID for multiple client locks
fn generate_aggregate_lock_id(resource: &ObjectKey) -> LockId {
    LockId {
        resource: resource.clone(),
        uuid: Uuid::new_v4().to_string(),
    }
}

/// Single client lock handler for local use cases
/// Directly uses LocalClient without aggregation mapping
#[derive(Debug)]
pub struct SingleClientLock {
    /// Local client for lock operations
    client: Arc<dyn LockClient>,
    /// Namespace identifier
    namespace: String,
}

impl SingleClientLock {
    /// Create new single client lock
    pub fn new(namespace: String, client: Arc<dyn LockClient>) -> Self {
        Self { namespace, client }
    }

    /// Get namespace identifier
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get resource key for this namespace
    pub fn get_resource_key(&self, resource: &ObjectKey) -> String {
        format!("{}:{}", self.namespace, resource)
    }

    /// Acquire a lock and return a RAII guard
    async fn acquire_guard(&self, request: &LockRequest) -> Result<Option<LockGuard>> {
        let resp = self.client.acquire_lock(request).await?;
        if resp.success {
            // Use lock_id from LockResponse's LockInfo
            let lock_id = resp
                .lock_info
                .as_ref()
                .map(|info| info.id.clone())
                .unwrap_or_else(|| LockId::new_deterministic(&request.resource));
            // Single client: only one (LockId, client) entry.
            let entries = vec![(lock_id.clone(), self.client.clone())];
            Ok(Some(LockGuard::new(lock_id, entries)))
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
}

/// Multiple client lock handler for distributed use cases
/// Uses quorum-based acquisition and aggregate lock ID mapping
#[derive(Debug)]
pub struct MultiClientLock {
    /// Lock clients for this namespace
    clients: Vec<Option<Arc<dyn LockClient>>>,
    /// Namespace identifier
    namespace: String,
    /// Quorum size for operations (majority for distributed)
    quorum: usize,
}

impl MultiClientLock {
    /// Create new multi client lock
    pub fn new(namespace: String, clients: Vec<Option<Arc<dyn LockClient>>>, quorum: usize) -> Self {
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

    /// Get namespace identifier
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get resource key for this namespace
    pub fn get_resource_key(&self, resource: &ObjectKey) -> String {
        format!("{}:{}", self.namespace, resource)
    }

    /// Acquire a lock and return a RAII guard
    async fn acquire_guard(&self, request: &LockRequest) -> Result<Option<LockGuard>> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        let (resp, individual_locks) = self.acquire_lock_quorum(request).await?;
        if resp.success {
            // Use aggregate lock_id from LockResponse's LockInfo
            // The aggregate id is what we expose to callers; individual_locks carries
            // the real (LockId, client) pairs that must be released.
            let aggregate_lock_id = resp
                .lock_info
                .as_ref()
                .map(|info| info.id.clone())
                .unwrap_or_else(|| LockId::new_deterministic(&request.resource));

            Ok(Some(LockGuard::new(aggregate_lock_id, individual_locks)))
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
    /// Collects all individual lock_ids from successful clients and creates an aggregate lock_id.
    /// Returns the LockResponse with aggregate lock_id and individual lock mappings.
    async fn acquire_lock_quorum(&self, request: &LockRequest) -> Result<(LockResponse, Vec<(LockId, Arc<dyn LockClient>)>)> {
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
        // Store all individual lock_ids and their corresponding clients
        let mut individual_locks: Vec<(LockId, Arc<dyn LockClient>)> = Vec::new();

        for (idx, res) in results {
            if let Some(resp) = res {
                match resp {
                    Ok(resp) => {
                        if resp.success {
                            // Collect individual lock_id and client for each successful acquisition
                            if let Some(client) = self.clients.get(idx).and_then(|c| c.clone())
                                && let Some(lock_info) = &resp.lock_info
                            {
                                // Save the individual lock_id returned by each client
                                individual_locks.push((lock_info.id.clone(), client));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to acquire lock on client {}: {}", idx, e);
                    }
                }
            }
        }

        if individual_locks.len() >= self.quorum {
            // Generate a new aggregate lock_id for multiple client locks
            let aggregate_lock_id = generate_aggregate_lock_id(&request.resource);

            tracing::debug!(
                "Generated aggregate lock_id {} for {} individual locks on resource {}",
                aggregate_lock_id,
                individual_locks.len(),
                request.resource
            );

            let resp = LockResponse::success(
                LockInfo {
                    id: aggregate_lock_id,
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
            Ok((resp, individual_locks))
        } else {
            // Rollback: release all locks that were successfully acquired
            let rollback_count = individual_locks.len();
            for (individual_lock_id, client) in individual_locks {
                if let Err(e) = client.release(&individual_lock_id).await {
                    tracing::warn!("Failed to rollback lock {} on client: {}", individual_lock_id, e);
                }
            }

            let resp = LockResponse::failure(
                format!("Failed to acquire quorum: {}/{} required", rollback_count, self.quorum),
                Duration::ZERO,
            );
            Ok((resp, Vec::new()))
        }
    }
}

/// Namespace lock for managing locks by resource namespaces
/// Automatically selects SingleClientLock or MultiClientLock based on client count
#[derive(Debug)]
pub enum NamespaceLock {
    /// Single client lock (local use case)
    Single(SingleClientLock),
    /// Multiple client lock (distributed use case)
    Multi(MultiClientLock),
}

impl NamespaceLock {
    /// Create new namespace lock with single client (local use case)
    pub fn new(namespace: String, client: Arc<dyn LockClient>) -> Self {
        Self::Single(SingleClientLock::new(namespace, client))
    }

    /// Create namespace lock with client (compatibility)
    pub fn with_client(client: Arc<dyn LockClient>) -> Self {
        Self::Single(SingleClientLock::new("default".to_string(), client))
    }

    /// Create namespace lock with clients
    /// Automatically selects SingleClientLock or MultiClientLock based on client count
    pub fn with_clients(namespace: String, clients: Vec<Option<Arc<dyn LockClient>>>) -> Self {
        let valid_clients: Vec<_> = clients.iter().filter_map(|c| c.as_ref()).collect();

        if valid_clients.len() == 1 {
            // Single client: use SingleClientLock
            if let Some(client) = clients.into_iter().find_map(|c| c) {
                Self::Single(SingleClientLock::new(namespace, client))
            } else {
                // Fallback: create with empty clients (will error on use)
                // This shouldn't happen if valid_clients.len() == 1, but handle it anyway
                let quorum = 1;
                Self::Multi(MultiClientLock::new(namespace, vec![], quorum))
            }
        } else {
            // Multiple clients: use MultiClientLock with majority quorum
            let quorum = if clients.len() > 1 { (clients.len() / 2) + 1 } else { 1 };
            Self::Multi(MultiClientLock::new(namespace, clients, quorum))
        }
    }

    /// Create namespace lock with clients and an explicit quorum size.
    /// Quorum will be clamped into [1, clients.len()]. For single client, quorum is always 1.
    pub fn with_clients_and_quorum(namespace: String, clients: Vec<Option<Arc<dyn LockClient>>>, quorum: usize) -> Self {
        let valid_clients: Vec<_> = clients.iter().filter_map(|c| c.as_ref()).collect();

        if valid_clients.len() == 1 {
            // Single client: use SingleClientLock
            // Clone clients to avoid move issue
            let clients_clone = clients.clone();
            if let Some(client) = clients_clone.into_iter().find_map(|c| c) {
                Self::Single(SingleClientLock::new(namespace, client))
            } else {
                // Fallback: use MultiClientLock
                Self::Multi(MultiClientLock::new(namespace, clients, quorum))
            }
        } else {
            // Multiple clients: use MultiClientLock
            Self::Multi(MultiClientLock::new(namespace, clients, quorum))
        }
    }

    /// Get namespace identifier
    pub fn namespace(&self) -> &str {
        match self {
            Self::Single(lock) => lock.namespace(),
            Self::Multi(lock) => lock.namespace(),
        }
    }

    /// Get resource key for this namespace
    pub fn get_resource_key(&self, resource: &ObjectKey) -> String {
        match self {
            Self::Single(lock) => lock.get_resource_key(resource),
            Self::Multi(lock) => lock.get_resource_key(resource),
        }
    }

    /// Acquire a lock and return a RAII guard that will release asynchronously on Drop.
    /// This is a thin wrapper around `acquire_lock` and will only create a guard when acquisition succeeds.
    pub async fn acquire_guard(&self, request: &LockRequest) -> Result<Option<LockGuard>> {
        match self {
            Self::Single(lock) => lock.acquire_guard(request).await,
            Self::Multi(lock) => lock.acquire_guard(request).await,
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
        match self {
            Self::Single(lock) => lock.lock_guard(resource, owner, timeout, ttl).await,
            Self::Multi(lock) => lock.lock_guard(resource, owner, timeout, ttl).await,
        }
    }

    /// Convenience: acquire shared lock as a guard
    pub async fn rlock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<LockGuard>> {
        match self {
            Self::Single(lock) => lock.rlock_guard(resource, owner, timeout, ttl).await,
            Self::Multi(lock) => lock.rlock_guard(resource, owner, timeout, ttl).await,
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
            Self::Single(lock) => {
                let is_online = lock.client.is_online().await;
                health.status = if is_online {
                    crate::types::HealthStatus::Healthy
                } else {
                    crate::types::HealthStatus::Degraded
                };
                health.connected_nodes = if is_online { 1 } else { 0 };
                health.total_nodes = 1;
            }
            Self::Multi(lock) => {
                // Check client status
                let mut connected_clients = 0;
                for client in lock.clients.iter().flatten() {
                    if client.is_online().await {
                        connected_clients += 1;
                    }
                }

                let quorum = if lock.clients.len() > 1 {
                    (lock.clients.len() / 2) + 1
                } else {
                    1
                };

                health.status = if connected_clients >= quorum {
                    crate::types::HealthStatus::Healthy
                } else {
                    crate::types::HealthStatus::Degraded
                };
                health.connected_nodes = connected_clients;
                health.total_nodes = lock.clients.len();
            }
        }

        health
    }

    /// Get namespace statistics
    pub async fn get_stats(&self) -> crate::types::LockStats {
        let mut stats = crate::types::LockStats::default();

        match self {
            Self::Single(lock) => {
                if let Ok(client_stats) = lock.client.get_stats().await {
                    stats.successful_acquires += client_stats.successful_acquires;
                    stats.failed_acquires += client_stats.failed_acquires;
                }
            }
            Self::Multi(lock) => {
                // Try to get stats from clients
                for client in lock.clients.iter().flatten() {
                    if let Ok(client_stats) = client.get_stats().await {
                        stats.successful_acquires += client_stats.successful_acquires;
                        stats.failed_acquires += client_stats.failed_acquires;
                    }
                }
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

struct NsLock {
    lock: NamespaceLock,
    ref_count: AtomicI32,
}

impl NsLock {
    pub fn new(lock: NamespaceLock) -> Self {
        Self {
            lock,
            ref_count: AtomicI32::new(0),
        }
    }

    pub fn ref_add(&self) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn ref_sub(&self) {
        self.ref_count.fetch_sub(1, Ordering::Relaxed);
    }
}

struct NsLockManagerGuard {
    locks: Arc<RwLock<HashMap<String, NsLock>>>,
    guard: Option<LockGuard>,
}

impl NsLockManagerGuard {
    pub fn new(locks: Arc<RwLock<HashMap<String, NsLock>>>, guard: Option<LockGuard>) -> Self {
        Self { locks, guard }
    }
}

struct NsLockManager {
    clients: Vec<Option<Arc<dyn LockClient>>>,
    locks: Arc<RwLock<HashMap<String, NsLock>>>,
}

impl NsLockManager {
    pub fn new(clients: Vec<Option<Arc<dyn LockClient>>>) -> Self {
        Self {
            clients,
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Convenience: acquire exclusive lock as a guard
    pub async fn lock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<NsLockManagerGuard> {
        let name = resource.to_string();

        // if let Some(lock) = self.locks.read().await.get(&name) {
        //     lock.ref_add();
        //     return lock.lock.lock_guard(resource, owner, timeout, ttl).await;
        // }

        todo!()
    }

    /// Convenience: acquire shared lock as a guard
    pub async fn rlock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<LockGuard>> {
        todo!()
    }
}
