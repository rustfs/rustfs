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
use url::Url;

use crate::{
    client::{LockClient, local::LocalClient, remote::RemoteClient},
    distributed::DistributedLockManager,
    error::{LockError, Result},
    local::LocalLockMap,
    types::{LockId, LockInfo, LockMetadata, LockPriority, LockRequest, LockResponse, LockStatus, LockType},
};

/// Namespace lock manager
///
/// Provides unified interface for both local and remote locks
#[derive(Debug)]
pub struct NsLockMap {
    /// Whether it is distributed mode
    is_dist: bool,
    /// Lock client (local or remote)
    client: Arc<dyn LockClient>,
    /// Shared lock map for local mode
    local_lock_map: Arc<LocalLockMap>,
}

impl NsLockMap {
    /// Create a new namespace lock manager
    ///
    /// # Parameters
    /// - `is_dist`: Whether it is distributed mode
    /// - `cache_size`: Not used in simplified version
    ///
    /// # Returns
    /// - New NsLockMap instance
    pub fn new(is_dist: bool, _cache_size: Option<usize>) -> Self {
        // Use the global shared lock map to ensure all instances share the same lock state
        let local_lock_map = crate::get_global_lock_map();
        
        let client: Arc<dyn LockClient> = if is_dist {
            // In distributed mode, we need a remote client
            // For now, we'll use local client as fallback
            // TODO: Implement proper remote client creation with URL
            Arc::new(LocalClient::new())
        } else {
            Arc::new(LocalClient::new())
        };

        Self { is_dist, client, local_lock_map }
    }

    /// Create namespace lock client
    ///
    /// # Parameters
    /// - `url`: Remote lock service URL (required in distributed mode)
    ///
    /// # Returns
    /// - `Ok(NamespaceLock)`: Namespace lock client
    /// - `Err`: Creation failed
    pub async fn new_nslock(&self, url: Option<Url>) -> Result<NamespaceLock> {
        if self.is_dist {
            let url = url.ok_or_else(|| LockError::internal("remote_url is required for distributed lock mode"))?;
            let client = Arc::new(RemoteClient::from_url(url));
            Ok(NamespaceLock::with_client(client))
        } else {
            let client = Arc::new(LocalClient::new());
            Ok(NamespaceLock::with_client(client))
        }
    }

    /// Check if it is distributed mode
    pub fn is_distributed(&self) -> bool {
        self.is_dist
    }

    /// Batch acquire write locks, returns Vec<LockId>
    pub async fn lock_batch_id(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<Vec<crate::types::LockId>> {
        self.local_lock_map.lock_batch_id(resources, owner, timeout, None).await
    }

    /// Batch release write locks by LockIds
    pub async fn unlock_batch_id(&self, lock_ids: &[crate::types::LockId]) -> Result<()> {
        self.local_lock_map.unlock_batch_id(lock_ids).await
    }

    /// Batch acquire read locks, returns Vec<LockId>
    pub async fn rlock_batch_id(
        &self,
        resources: &[String],
        owner: &str,
        timeout: Duration,
    ) -> Result<Vec<crate::types::LockId>> {
        self.local_lock_map.rlock_batch_id(resources, owner, timeout, None).await
    }

    /// Batch release read locks by LockIds
    pub async fn runlock_batch_id(&self, lock_ids: &[crate::types::LockId]) -> Result<()> {
        self.local_lock_map.runlock_batch_id(lock_ids).await
    }

    /// Batch acquire write locks (compatibility)
    pub async fn lock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        let lock_ids = self.lock_batch_id(resources, owner, timeout).await?;
        Ok(!lock_ids.is_empty())
    }

    /// Batch acquire write locks with TTL
    pub async fn lock_batch_with_ttl(&self, resources: &[String], owner: &str, timeout: Duration, ttl: Option<Duration>) -> Result<bool> {
        if self.is_dist {
            // For distributed mode, use the client directly
            let request = crate::types::LockRequest::new(&resources[0], crate::types::LockType::Exclusive, owner)
                .with_timeout(timeout);
            match self.client.acquire_exclusive(request).await {
                Ok(response) => Ok(response.success),
                Err(e) => Err(e),
            }
        } else {
            // For local mode, use the shared local lock map
            self.local_lock_map.lock_batch(resources, owner, timeout, ttl).await
        }
    }

    /// Batch release write locks (compatibility)
    pub async fn unlock_batch(&self, resources: &[String], _owner: &str) -> Result<()> {
        // For compatibility, we need to find the LockIds for these resources
        // This is a simplified approach - in practice you might want to maintain a resource->LockId mapping
        for resource in resources {
            // Create a LockId that matches the resource name for release
            let lock_id = crate::types::LockId::new_deterministic(resource);
            let _ = self.local_lock_map.unlock_by_id(&lock_id).await;
        }
        Ok(())
    }

    /// Batch acquire read locks (compatibility)
    pub async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        let lock_ids = self.rlock_batch_id(resources, owner, timeout).await?;
        Ok(!lock_ids.is_empty())
    }

    /// Batch acquire read locks with TTL
    pub async fn rlock_batch_with_ttl(&self, resources: &[String], owner: &str, timeout: Duration, ttl: Option<Duration>) -> Result<bool> {
        if self.is_dist {
            // For distributed mode, use the client directly
            let request = crate::types::LockRequest::new(&resources[0], crate::types::LockType::Shared, owner)
                .with_timeout(timeout);
            match self.client.acquire_shared(request).await {
                Ok(response) => Ok(response.success),
                Err(e) => Err(e),
            }
        } else {
            // For local mode, use the shared local lock map
            self.local_lock_map.rlock_batch(resources, owner, timeout, ttl).await
        }
    }

    /// Batch release read locks (compatibility)
    pub async fn runlock_batch(&self, resources: &[String], _owner: &str) -> Result<()> {
        // For compatibility, we need to find the LockIds for these resources
        for resource in resources {
            // Create a LockId that matches the resource name for release
            let lock_id = crate::types::LockId::new_deterministic(resource);
            let _ = self.local_lock_map.runlock_by_id(&lock_id).await;
        }
        Ok(())
    }
}

/// Namespace lock for managing locks by resource namespaces
#[derive(Debug)]
pub struct NamespaceLock {
    /// Local lock map for this namespace
    local_locks: Arc<LocalLockMap>,
    /// Distributed lock manager (if enabled)
    distributed_manager: Option<Arc<DistributedLockManager>>,

    /// Namespace identifier
    namespace: String,
    /// Whether distributed locking is enabled
    distributed_enabled: bool,
}

impl NamespaceLock {
    /// Create new namespace lock
    pub fn new(namespace: String, distributed_enabled: bool) -> Self {
        Self {
            local_locks: Arc::new(LocalLockMap::new()),
            distributed_manager: None,
            namespace,
            distributed_enabled,
        }
    }

    /// Create namespace lock with client
    pub fn with_client(_client: Arc<dyn LockClient>) -> Self {
        Self {
            local_locks: Arc::new(LocalLockMap::new()),
            distributed_manager: None,
            namespace: "default".to_string(),
            distributed_enabled: false,
        }
    }

    /// Create namespace lock with distributed manager
    pub fn with_distributed(namespace: String, distributed_manager: Arc<DistributedLockManager>) -> Self {
        Self {
            local_locks: Arc::new(LocalLockMap::new()),
            distributed_manager: Some(distributed_manager),
            namespace,
            distributed_enabled: true,
        }
    }

    /// Get namespace identifier
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Check if distributed locking is enabled
    pub fn is_distributed(&self) -> bool {
        self.distributed_enabled && self.distributed_manager.is_some()
    }

    /// Set distributed manager
    pub fn set_distributed_manager(&mut self, manager: Arc<DistributedLockManager>) {
        self.distributed_manager = Some(manager);
        self.distributed_enabled = true;
    }

    /// Remove distributed manager
    pub fn remove_distributed_manager(&mut self) {
        self.distributed_manager = None;
        self.distributed_enabled = false;
    }

    /// Acquire lock in this namespace
    pub async fn acquire_lock(&self, request: LockRequest) -> Result<LockResponse> {
        let resource_key = self.get_resource_key(&request.resource);

        // Check if we already hold this lock
        if let Some(lock_info) = self.local_locks.get_lock(&resource_key).await {
            if lock_info.owner == request.owner && !lock_info.has_expired() {
                return Ok(LockResponse::success(lock_info, Duration::ZERO));
            }
        }

        // Try local lock first
        match self.try_local_lock(&request).await {
            Ok(response) => {
                if response.success {
                    return Ok(response);
                }
            }
            Err(e) => {
                tracing::debug!("Local lock acquisition failed: {}", e);
            }
        }

        // If distributed locking is enabled, try distributed lock
        if self.is_distributed() {
            if let Some(manager) = &self.distributed_manager {
                match manager.acquire_lock(request.clone()).await {
                    Ok(response) => {
                        if response.success {
                            // Also acquire local lock for consistency
                            let _ = self.try_local_lock(&request).await;
                            return Ok(response);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Distributed lock acquisition failed: {}", e);
                    }
                }
            }
        }

        // If all else fails, return timeout error
        Err(LockError::timeout("Lock acquisition failed", request.timeout))
    }

    /// Try to acquire local lock
    async fn try_local_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        let resource_key = self.get_resource_key(&request.resource);
        let lock_id = request.lock_id.clone();

        match request.lock_type {
            LockType::Exclusive => {
                self.local_locks
                    .acquire_exclusive_lock(&resource_key, &lock_id, &request.owner, request.timeout)
                    .await?;
            }
            LockType::Shared => {
                self.local_locks
                    .acquire_shared_lock(&resource_key, &lock_id, &request.owner, request.timeout)
                    .await?;
            }
        }

        Ok(LockResponse::success(
            LockInfo {
                id: lock_id,
                resource: request.resource.clone(),
                lock_type: request.lock_type,
                status: LockStatus::Acquired,
                owner: request.owner.clone(),
                acquired_at: std::time::SystemTime::now(),
                expires_at: std::time::SystemTime::now() + request.timeout,
                last_refreshed: std::time::SystemTime::now(),
                metadata: request.metadata.clone(),
                priority: request.priority,
                wait_start_time: None,
            },
            Duration::ZERO,
        ))
    }

    /// Release lock in this namespace
    pub async fn release_lock(&self, lock_id: &LockId, owner: &str) -> Result<LockResponse> {
        let resource_key = self.get_resource_key_from_lock_id(lock_id);

        // Release local lock
        let local_result = self.local_locks.release_lock(&resource_key, owner).await;

        // Release distributed lock if enabled
        if self.is_distributed() {
            if let Some(manager) = &self.distributed_manager {
                let _ = manager.release_lock(lock_id, owner).await;
            }
        }

        match local_result {
            Ok(_) => Ok(LockResponse::success(
                LockInfo {
                    id: lock_id.clone(),
                    resource: "unknown".to_string(),
                    lock_type: LockType::Exclusive,
                    status: LockStatus::Released,
                    owner: owner.to_string(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now(),
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: LockMetadata::default(),
                    priority: LockPriority::Normal,
                    wait_start_time: None,
                },
                Duration::ZERO,
            )),
            Err(e) => Err(e),
        }
    }

    /// Refresh lock in this namespace
    pub async fn refresh_lock(&self, lock_id: &LockId, owner: &str) -> Result<LockResponse> {
        let resource_key = self.get_resource_key_from_lock_id(lock_id);

        // Refresh local lock
        let local_result = self.local_locks.refresh_lock(&resource_key, owner).await;

        // Refresh distributed lock if enabled
        if self.is_distributed() {
            if let Some(manager) = &self.distributed_manager {
                let _ = manager.refresh_lock(lock_id, owner).await;
            }
        }

        match local_result {
            Ok(_) => Ok(LockResponse::success(
                LockInfo {
                    id: lock_id.clone(),
                    resource: "unknown".to_string(),
                    lock_type: LockType::Exclusive,
                    status: LockStatus::Acquired,
                    owner: "unknown".to_string(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + Duration::from_secs(30),
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: LockMetadata::default(),
                    priority: LockPriority::Normal,
                    wait_start_time: None,
                },
                Duration::ZERO,
            )),
            Err(e) => Err(e),
        }
    }

    /// Get lock information
    pub async fn get_lock_info(&self, lock_id: &LockId) -> Option<LockInfo> {
        let resource_key = self.get_resource_key_from_lock_id(lock_id);
        self.local_locks.get_lock(&resource_key).await
    }

    /// List all locks in this namespace
    pub async fn list_locks(&self) -> Vec<LockInfo> {
        self.local_locks.list_locks().await
    }

    /// Get locks for a specific resource
    pub async fn get_locks_for_resource(&self, resource: &str) -> Vec<LockInfo> {
        let resource_key = self.get_resource_key(resource);
        self.local_locks.get_locks_for_resource(&resource_key).await
    }

    /// Force release lock (admin operation)
    pub async fn force_release_lock(&self, lock_id: &LockId) -> Result<LockResponse> {
        let resource_key = self.get_resource_key_from_lock_id(lock_id);

        // Force release local lock
        let local_result = self.local_locks.force_release_lock(&resource_key).await;

        // Force release distributed lock if enabled
        if self.is_distributed() {
            if let Some(manager) = &self.distributed_manager {
                let _ = manager.force_release_lock(lock_id).await;
            }
        }

        match local_result {
            Ok(_) => Ok(LockResponse::success(
                LockInfo {
                    id: lock_id.clone(),
                    resource: "unknown".to_string(),
                    lock_type: LockType::Exclusive,
                    status: LockStatus::Released,
                    owner: "unknown".to_string(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now(),
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: LockMetadata::default(),
                    priority: LockPriority::Normal,
                    wait_start_time: None,
                },
                Duration::ZERO,
            )),
            Err(e) => Err(e),
        }
    }

    /// Clean up expired locks
    pub async fn cleanup_expired_locks(&self) -> usize {
        let local_cleaned = self.local_locks.cleanup_expired_locks().await;

        let distributed_cleaned = if self.is_distributed() {
            if let Some(manager) = &self.distributed_manager {
                manager.cleanup_expired_locks().await
            } else {
                0
            }
        } else {
            0
        };

        local_cleaned + distributed_cleaned
    }

    /// Get health information for all namespaces
    pub async fn get_all_health(&self) -> Vec<crate::types::HealthInfo> {
        let mut health_list = Vec::new();

        // Add current namespace health
        health_list.push(self.get_health().await);

        // In a real implementation, you might want to check other namespaces
        // For now, we only return the current namespace

        health_list
    }

    /// Get health information
    pub async fn get_health(&self) -> crate::types::HealthInfo {
        let lock_stats = self.get_stats().await;
        let mut health = crate::types::HealthInfo {
            node_id: self.namespace.clone(),
            lock_stats,
            ..Default::default()
        };

        // Check distributed status if enabled
        if self.is_distributed() {
            if let Some(_manager) = &self.distributed_manager {
                // For now, assume healthy if distributed manager exists
                health.status = crate::types::HealthStatus::Healthy;
                health.connected_nodes = 1;
                health.total_nodes = 1;
            } else {
                health.status = crate::types::HealthStatus::Degraded;
                health.error_message = Some("Distributed manager not available".to_string());
            }
        } else {
            health.status = crate::types::HealthStatus::Healthy;
            health.connected_nodes = 1;
            health.total_nodes = 1;
        }

        health
    }

    /// Get namespace statistics
    pub async fn get_stats(&self) -> crate::types::LockStats {
        let mut stats = self.local_locks.get_stats().await;

        // Add queue statistics

        // Add distributed statistics if enabled
        if self.is_distributed() {
            if let Some(manager) = &self.distributed_manager {
                let distributed_stats = manager.get_stats().await;
                stats.successful_acquires += distributed_stats.successful_acquires;
                stats.failed_acquires += distributed_stats.failed_acquires;
            }
        }

        stats
    }

    /// Get resource key for this namespace
    fn get_resource_key(&self, resource: &str) -> String {
        format!("{}:{}", self.namespace, resource)
    }

    /// Get resource key from lock ID
    fn get_resource_key_from_lock_id(&self, lock_id: &LockId) -> String {
        // This is a simplified implementation
        // In practice, you might want to store a mapping from lock_id to resource
        format!("{}:{}", self.namespace, lock_id)
    }
}

impl Default for NamespaceLock {
    fn default() -> Self {
        Self::new("default".to_string(), false)
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
        if self.distributed_enabled {
            // Use RPC to call the server
            use rustfs_protos::{node_service_time_out_client, proto_gen::node_service::GenerallyLockRequest};
            use tonic::Request;
            
            // Add namespace prefix to resources
            let namespaced_resources: Vec<String> = resources.iter()
                .map(|resource| self.get_resource_key(resource))
                .collect();
            
            let args = crate::client::remote::LockArgs {
                uid: uuid::Uuid::new_v4().to_string(),
                resources: namespaced_resources,
                owner: owner.to_string(),
                source: "".to_string(),
                quorum: 3,
            };
            let args_str = serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;

            let mut client = node_service_time_out_client(&"http://localhost:9000".to_string())
                .await
                .map_err(|e| LockError::internal(format!("Failed to connect to server: {e}")))?;
            
            let request = Request::new(GenerallyLockRequest { args: args_str });
            let response = client.lock(request).await.map_err(|e| LockError::internal(format!("RPC call failed: {e}")))?;
            let response = response.into_inner();
            
            if let Some(error_info) = response.error_info {
                return Err(LockError::internal(format!("Server error: {error_info}")));
            }
            
            Ok(response.success)
        } else {
            // Use local locks
            for resource in resources {
                let resource_key = self.get_resource_key(resource);
                let success = self
                    .local_locks
                    .lock_with_ttl_id(&resource_key, owner, timeout, None)
                    .await
                    .map_err(|e| LockError::internal(format!("Lock acquisition failed: {e}")))?;

                if !success {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }

    async fn unlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        if self.distributed_enabled {
            // Use RPC to call the server
            use rustfs_protos::{node_service_time_out_client, proto_gen::node_service::GenerallyLockRequest};
            use tonic::Request;
            
            // Add namespace prefix to resources
            let namespaced_resources: Vec<String> = resources.iter()
                .map(|resource| self.get_resource_key(resource))
                .collect();
            
            let args = crate::client::remote::LockArgs {
                uid: uuid::Uuid::new_v4().to_string(),
                resources: namespaced_resources,
                owner: owner.to_string(),
                source: "".to_string(),
                quorum: 3,
            };
            let args_str = serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;

            let mut client = node_service_time_out_client(&"http://localhost:9000".to_string())
                .await
                .map_err(|e| LockError::internal(format!("Failed to connect to server: {e}")))?;
            
            let request = Request::new(GenerallyLockRequest { args: args_str });
            let response = client.un_lock(request).await.map_err(|e| LockError::internal(format!("RPC call failed: {e}")))?;
            let response = response.into_inner();
            
            if let Some(error_info) = response.error_info {
                return Err(LockError::internal(format!("Server error: {error_info}")));
            }
            
            Ok(())
        } else {
            // Use local locks
            for resource in resources {
                let resource_key = self.get_resource_key(resource);
                self.local_locks
                    .unlock(&resource_key, owner)
                    .await
                    .map_err(|e| LockError::internal(format!("Lock release failed: {e}")))?;
            }
            Ok(())
        }
    }

    async fn rlock_batch(&self, resources: &[String], owner: &str, timeout: Duration) -> Result<bool> {
        if self.distributed_enabled {
            // Use RPC to call the server
            use rustfs_protos::{node_service_time_out_client, proto_gen::node_service::GenerallyLockRequest};
            use tonic::Request;
            
            // Add namespace prefix to resources
            let namespaced_resources: Vec<String> = resources.iter()
                .map(|resource| self.get_resource_key(resource))
                .collect();
            
            let args = crate::client::remote::LockArgs {
                uid: uuid::Uuid::new_v4().to_string(),
                resources: namespaced_resources,
                owner: owner.to_string(),
                source: "".to_string(),
                quorum: 3,
            };
            let args_str = serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;

            let mut client = node_service_time_out_client(&"http://localhost:9000".to_string())
                .await
                .map_err(|e| LockError::internal(format!("Failed to connect to server: {e}")))?;
            
            let request = Request::new(GenerallyLockRequest { args: args_str });
            let response = client.r_lock(request).await.map_err(|e| LockError::internal(format!("RPC call failed: {e}")))?;
            let response = response.into_inner();
            
            if let Some(error_info) = response.error_info {
                return Err(LockError::internal(format!("Server error: {error_info}")));
            }
            
            Ok(response.success)
        } else {
            // Use local locks
            for resource in resources {
                let resource_key = self.get_resource_key(resource);
                let success = self
                    .local_locks
                    .rlock_with_ttl_id(&resource_key, owner, timeout, None)
                    .await
                    .map_err(|e| LockError::internal(format!("Read lock acquisition failed: {e}")))?;

                if !success {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }

    async fn runlock_batch(&self, resources: &[String], owner: &str) -> Result<()> {
        if self.distributed_enabled {
            // Use RPC to call the server
            use rustfs_protos::{node_service_time_out_client, proto_gen::node_service::GenerallyLockRequest};
            use tonic::Request;
            
            // Add namespace prefix to resources
            let namespaced_resources: Vec<String> = resources.iter()
                .map(|resource| self.get_resource_key(resource))
                .collect();
            
            let args = crate::client::remote::LockArgs {
                uid: uuid::Uuid::new_v4().to_string(),
                resources: namespaced_resources,
                owner: owner.to_string(),
                source: "".to_string(),
                quorum: 3,
            };
            let args_str = serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;

            let mut client = node_service_time_out_client(&"http://localhost:9000".to_string())
                .await
                .map_err(|e| LockError::internal(format!("Failed to connect to server: {e}")))?;
            
            let request = Request::new(GenerallyLockRequest { args: args_str });
            let response = client.r_un_lock(request).await.map_err(|e| LockError::internal(format!("RPC call failed: {e}")))?;
            let response = response.into_inner();
            
            if let Some(error_info) = response.error_info {
                return Err(LockError::internal(format!("Server error: {error_info}")));
            }
            
            Ok(())
        } else {
            // Use local locks
            for resource in resources {
                let resource_key = self.get_resource_key(resource);
                self.local_locks
                    .runlock(&resource_key, owner)
                    .await
                    .map_err(|e| LockError::internal(format!("Read lock release failed: {e}")))?;
            }
            Ok(())
        }
    }
}

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
        assert!(matches!(client, NamespaceLock { .. }));
    }

    #[tokio::test]
    async fn test_namespace_lock_local() {
        let ns_lock = NamespaceLock::new("test-namespace".to_string(), false);
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
    async fn test_batch_operations() {
        let ns_lock = NsLockMap::new(false, None);
        let write_resources = vec!["batch_write".to_string()];
        let read_resources = vec!["batch_read".to_string()];

        // Test batch write lock
        let result = ns_lock
            .lock_batch(&write_resources, "batch_owner", Duration::from_millis(100))
            .await;
        println!("lock_batch result: {result:?}");
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Test batch unlock
        let result = ns_lock.unlock_batch(&write_resources, "batch_owner").await;
        println!("unlock_batch result: {result:?}");
        assert!(result.is_ok());

        // Test batch read lock (different resource)
        let result = ns_lock
            .rlock_batch(&read_resources, "batch_reader", Duration::from_millis(100))
            .await;
        println!("rlock_batch result: {result:?}");
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Test batch release read lock
        let result = ns_lock.runlock_batch(&read_resources, "batch_reader").await;
        println!("runlock_batch result: {result:?}");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_connection_health() {
        let local_lock = NamespaceLock::new("test-namespace".to_string(), false);
        let health = local_lock.get_health().await;
        assert_eq!(health.status, crate::types::HealthStatus::Healthy);
        assert!(!local_lock.is_distributed());
    }

    #[tokio::test]
    async fn test_namespace_lock_creation() {
        let ns_lock = NamespaceLock::new("test-namespace".to_string(), false);
        assert_eq!(ns_lock.namespace(), "test-namespace");
        assert!(!ns_lock.is_distributed());
    }

    #[tokio::test]
    async fn test_namespace_lock_with_distributed() {
        let distributed_manager = Arc::new(DistributedLockManager::default());
        let ns_lock = NamespaceLock::with_distributed("test-namespace".to_string(), distributed_manager);

        assert_eq!(ns_lock.namespace(), "test-namespace");
        assert!(ns_lock.is_distributed());
    }

    #[tokio::test]
    async fn test_namespace_lock_local_operations() {
        let ns_lock = NamespaceLock::new("test-namespace".to_string(), false);

        let request = LockRequest {
            lock_id: LockId::new("test-resource"),
            resource: "test-resource".to_string(),
            lock_type: LockType::Exclusive,
            owner: "test-owner".to_string(),
            priority: LockPriority::Normal,
            timeout: Duration::from_secs(30),
            metadata: LockMetadata::default(),
            wait_timeout: None,
            deadlock_detection: true,
        };

        // Test lock acquisition
        let response = ns_lock.acquire_lock(request.clone()).await;
        assert!(response.is_ok());

        let response = response.unwrap();
        assert!(response.success);

        // Test lock release
        let release_response = ns_lock.release_lock(&response.lock_info.unwrap().id, "test-owner").await;
        assert!(release_response.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_lock_resource_key() {
        let ns_lock = NamespaceLock::new("test-namespace".to_string(), false);

        // Test resource key generation
        let resource_key = ns_lock.get_resource_key("test-resource");
        assert_eq!(resource_key, "test-namespace:test-resource");
    }

    #[tokio::test]
    async fn test_distributed_namespace_lock_health() {
        let distributed_manager = Arc::new(DistributedLockManager::default());
        let ns_lock = NamespaceLock::with_distributed("test-distributed-namespace".to_string(), distributed_manager);

        let health = ns_lock.get_health().await;
        assert_eq!(health.status, crate::types::HealthStatus::Healthy);
        assert!(ns_lock.is_distributed());
    }
}
