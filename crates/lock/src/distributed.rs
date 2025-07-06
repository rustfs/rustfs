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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, timeout};
use uuid::Uuid;

use crate::{
    client::LockClient,
    error::{LockError, Result},
    types::{LockId, LockInfo, LockPriority, LockRequest, LockResponse, LockStats, LockStatus, LockType},
};

/// Quorum configuration for distributed locking
#[derive(Debug, Clone)]
pub struct QuorumConfig {
    /// Total number of nodes in the cluster
    pub total_nodes: usize,
    /// Number of nodes that can fail (tolerance)
    pub tolerance: usize,
    /// Minimum number of nodes required for quorum
    pub quorum: usize,
    /// Lock acquisition timeout
    pub acquisition_timeout: Duration,
    /// Lock refresh interval
    pub refresh_interval: Duration,
    /// Lock expiration time
    pub expiration_time: Duration,
}

impl QuorumConfig {
    /// Create new quorum configuration
    pub fn new(total_nodes: usize, tolerance: usize) -> Self {
        let quorum = total_nodes - tolerance;
        Self {
            total_nodes,
            tolerance,
            quorum,
            acquisition_timeout: Duration::from_secs(30),
            refresh_interval: Duration::from_secs(10),
            expiration_time: Duration::from_secs(60),
        }
    }

    /// Check if quorum is valid
    pub fn is_valid(&self) -> bool {
        self.quorum > 0 && self.quorum <= self.total_nodes && self.tolerance < self.total_nodes
    }
}

/// Distributed lock entry
#[derive(Debug)]
pub struct DistributedLockEntry {
    /// Lock ID
    pub lock_id: LockId,
    /// Resource being locked
    pub resource: String,
    /// Lock type
    pub lock_type: LockType,
    /// Lock owner
    pub owner: String,
    /// Lock priority
    pub priority: LockPriority,
    /// Lock acquisition time
    pub acquired_at: Instant,
    /// Lock expiration time
    pub expires_at: Instant,
    /// Nodes that hold this lock
    pub holders: Vec<String>,
    /// Lock refresh task handle
    pub refresh_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DistributedLockEntry {
    /// Create new distributed lock entry
    pub fn new(
        lock_id: LockId,
        resource: String,
        lock_type: LockType,
        owner: String,
        priority: LockPriority,
        expiration_time: Duration,
    ) -> Self {
        let now = Instant::now();
        Self {
            lock_id,
            resource,
            lock_type,
            owner,
            priority,
            acquired_at: now,
            expires_at: now + expiration_time,
            holders: Vec::new(),
            refresh_handle: None,
        }
    }

    /// Check if lock has expired
    pub fn has_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    /// Extend lock expiration
    pub fn extend(&mut self, duration: Duration) {
        self.expires_at = Instant::now() + duration;
    }

    /// Get remaining time until expiration
    pub fn remaining_time(&self) -> Duration {
        if self.has_expired() {
            Duration::ZERO
        } else {
            self.expires_at - Instant::now()
        }
    }
}

/// Distributed lock manager
#[derive(Debug)]
pub struct DistributedLockManager {
    /// Quorum configuration
    config: QuorumConfig,
    /// Lock clients for each node
    clients: Arc<RwLock<HashMap<String, Arc<dyn LockClient>>>>,
    /// Active locks
    locks: Arc<RwLock<HashMap<String, DistributedLockEntry>>>,

    /// Node ID
    node_id: String,
    /// Statistics
    stats: Arc<Mutex<LockStats>>,
}

impl DistributedLockManager {
    /// Create new distributed lock manager
    pub fn new(config: QuorumConfig, node_id: String) -> Self {
        Self {
            config,
            clients: Arc::new(RwLock::new(HashMap::new())),
            locks: Arc::new(RwLock::new(HashMap::new())),

            node_id,
            stats: Arc::new(Mutex::new(LockStats::default())),
        }
    }

    /// Add lock client for a node
    pub async fn add_client(&self, node_id: String, client: Arc<dyn LockClient>) {
        let mut clients = self.clients.write().await;
        clients.insert(node_id, client);
    }

    /// Remove lock client for a node
    pub async fn remove_client(&self, node_id: &str) {
        let mut clients = self.clients.write().await;
        clients.remove(node_id);
    }

    /// Acquire distributed lock
    pub async fn acquire_lock(&self, request: LockRequest) -> Result<LockResponse> {
        let resource_key = self.get_resource_key(&request.resource);

        // Check if we already hold this lock
        {
            let locks = self.locks.read().await;
            if let Some(lock) = locks.get(&resource_key) {
                if lock.owner == request.owner && !lock.has_expired() {
                    return Ok(LockResponse::success(
                        LockInfo {
                            id: lock.lock_id.clone(),
                            resource: request.resource.clone(),
                            lock_type: request.lock_type,
                            status: LockStatus::Acquired,
                            owner: request.owner.clone(),
                            acquired_at: SystemTime::now(),
                            expires_at: SystemTime::now() + lock.remaining_time(),
                            last_refreshed: SystemTime::now(),
                            metadata: request.metadata.clone(),
                            priority: request.priority,
                            wait_start_time: None,
                        },
                        Duration::ZERO,
                    ));
                }
            }
        }

        // Try to acquire lock directly
        match self.try_acquire_lock(&request).await {
            Ok(response) => {
                if response.success {
                    return Ok(response);
                }
            }
            Err(e) => {
                tracing::warn!("Direct lock acquisition failed: {}", e);
            }
        }

        // If direct acquisition fails, return timeout error
        Err(LockError::timeout("Distributed lock acquisition failed", request.timeout))
    }

    /// Try to acquire lock directly
    async fn try_acquire_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        let resource_key = self.get_resource_key(&request.resource);
        let clients = self.clients.read().await;

        if clients.len() < self.config.quorum {
            return Err(LockError::InsufficientNodes {
                required: self.config.quorum,
                available: clients.len(),
            });
        }

        // Prepare lock request for all nodes
        let lock_request = LockRequest {
            lock_id: request.lock_id.clone(),
            resource: request.resource.clone(),
            lock_type: request.lock_type,
            owner: request.owner.clone(),
            priority: request.priority,
            timeout: self.config.acquisition_timeout,
            metadata: request.metadata.clone(),
            wait_timeout: request.wait_timeout,
            deadlock_detection: request.deadlock_detection,
        };

        // Send lock request to all nodes
        let mut responses = Vec::new();
        let mut handles = Vec::new();

        for (node_id, client) in clients.iter() {
            let client = client.clone();
            let request = lock_request.clone();

            let handle = tokio::spawn(async move { client.acquire_lock(request).await });

            handles.push((node_id.clone(), handle));
        }

        // Collect responses with timeout
        for (node_id, handle) in handles {
            match timeout(self.config.acquisition_timeout, handle).await {
                Ok(Ok(response)) => {
                    responses.push((node_id, response));
                }
                Ok(Err(e)) => {
                    tracing::warn!("Lock request failed for node {}: {}", node_id, e);
                }
                Err(_) => {
                    tracing::warn!("Lock request timeout for node {}", node_id);
                }
            }
        }

        // Check if we have quorum
        let successful_responses = responses
            .iter()
            .filter(|(_, response)| response.as_ref().map(|r| r.success).unwrap_or(false))
            .count();

        if successful_responses >= self.config.quorum {
            // Create lock entry
            let mut lock_entry = DistributedLockEntry::new(
                request.lock_id.clone(),
                request.resource.clone(),
                request.lock_type,
                request.owner.clone(),
                request.priority,
                self.config.expiration_time,
            );

            // Add successful nodes as holders
            for (node_id, _) in responses
                .iter()
                .filter(|(_, r)| r.as_ref().map(|resp| resp.success).unwrap_or(false))
            {
                lock_entry.holders.push(node_id.clone());
            }

            // Start refresh task
            let refresh_handle = self.start_refresh_task(&lock_entry).await;

            // Store lock entry
            {
                let mut locks = self.locks.write().await;
                lock_entry.refresh_handle = Some(refresh_handle);
                locks.insert(resource_key, lock_entry);
            }

            // Update statistics
            self.update_stats(true).await;

            Ok(LockResponse::success(
                LockInfo {
                    id: request.lock_id.clone(),
                    resource: request.resource.clone(),
                    lock_type: request.lock_type,
                    status: LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: SystemTime::now(),
                    expires_at: SystemTime::now() + self.config.expiration_time,
                    last_refreshed: SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                },
                Duration::ZERO,
            ))
        } else {
            // Update statistics
            self.update_stats(false).await;

            Err(LockError::QuorumNotReached {
                required: self.config.quorum,
                achieved: successful_responses,
            })
        }
    }

    /// Release distributed lock
    pub async fn release_lock(&self, lock_id: &LockId, owner: &str) -> Result<LockResponse> {
        let resource_key = self.get_resource_key_from_lock_id(lock_id);

        // Check if we hold this lock
        {
            let locks = self.locks.read().await;
            if let Some(lock) = locks.get(&resource_key) {
                if lock.owner != owner {
                    return Err(LockError::NotOwner {
                        lock_id: lock_id.clone(),
                        owner: owner.to_string(),
                    });
                }
            }
        }

        // Release lock from all nodes
        let clients = self.clients.read().await;
        let mut responses = Vec::new();

        for (node_id, client) in clients.iter() {
            match client.release(lock_id).await {
                Ok(response) => {
                    responses.push((node_id.clone(), response));
                }
                Err(e) => {
                    tracing::warn!("Lock release failed for node {}: {}", node_id, e);
                }
            }
        }

        // Remove lock entry
        {
            let mut locks = self.locks.write().await;
            if let Some(lock) = locks.remove(&resource_key) {
                // Cancel refresh task
                if let Some(handle) = lock.refresh_handle {
                    handle.abort();
                }
            }
        }

        Ok(LockResponse::success(
            LockInfo {
                id: lock_id.clone(),
                resource: "unknown".to_string(),
                lock_type: LockType::Exclusive,
                status: LockStatus::Released,
                owner: owner.to_string(),
                acquired_at: SystemTime::now(),
                expires_at: SystemTime::now(),
                last_refreshed: SystemTime::now(),
                metadata: crate::types::LockMetadata::default(),
                priority: LockPriority::Normal,
                wait_start_time: None,
            },
            Duration::ZERO,
        ))
    }

    /// Start lock refresh task
    async fn start_refresh_task(&self, lock_entry: &DistributedLockEntry) -> tokio::task::JoinHandle<()> {
        let lock_id = lock_entry.lock_id.clone();
        let _owner = lock_entry.owner.clone();
        let _resource = lock_entry.resource.clone();
        let refresh_interval = self.config.refresh_interval;
        let clients = self.clients.clone();
        let quorum = self.config.quorum;

        tokio::spawn(async move {
            let mut interval = interval(refresh_interval);

            loop {
                interval.tick().await;

                // Try to refresh lock on all nodes
                let clients_guard = clients.read().await;
                let mut success_count = 0;

                for (node_id, client) in clients_guard.iter() {
                    match client.refresh(&lock_id).await {
                        Ok(success) if success => {
                            success_count += 1;
                        }
                        _ => {
                            tracing::warn!("Lock refresh failed for node {}", node_id);
                        }
                    }
                }

                // If we don't have quorum, stop refreshing
                if success_count < quorum {
                    tracing::error!("Lost quorum for lock {}, stopping refresh", lock_id);
                    break;
                }
            }
        })
    }

    /// Get resource key
    fn get_resource_key(&self, resource: &str) -> String {
        format!("{}:{}", self.node_id, resource)
    }

    /// Get resource key from lock ID
    fn get_resource_key_from_lock_id(&self, lock_id: &LockId) -> String {
        // This is a simplified implementation
        // In practice, you might want to store a mapping from lock_id to resource
        format!("{}:{}", self.node_id, lock_id)
    }

    /// Update statistics
    async fn update_stats(&self, success: bool) {
        let mut stats = self.stats.lock().await;
        if success {
            stats.successful_acquires += 1;
        } else {
            stats.failed_acquires += 1;
        }
    }

    /// Get lock statistics
    pub async fn get_stats(&self) -> LockStats {
        self.stats.lock().await.clone()
    }

    /// Clean up expired locks
    pub async fn cleanup_expired_locks(&self) -> usize {
        let mut locks = self.locks.write().await;
        let initial_len = locks.len();

        locks.retain(|_, lock| !lock.has_expired());

        initial_len - locks.len()
    }

    /// Force release lock (admin operation)
    pub async fn force_release_lock(&self, lock_id: &LockId) -> Result<LockResponse> {
        let resource_key = self.get_resource_key_from_lock_id(lock_id);

        // Check if we hold this lock
        {
            let locks = self.locks.read().await;
            if let Some(lock) = locks.get(&resource_key) {
                // Force release on all nodes
                let clients = self.clients.read().await;
                let mut _success_count = 0;

                for (node_id, client) in clients.iter() {
                    match client.force_release(lock_id).await {
                        Ok(success) if success => {
                            _success_count += 1;
                        }
                        _ => {
                            tracing::warn!("Force release failed for node {}", node_id);
                        }
                    }
                }

                // Remove from local locks
                let mut locks = self.locks.write().await;
                locks.remove(&resource_key);

                // Wake up waiting locks

                return Ok(LockResponse::success(
                    LockInfo {
                        id: lock_id.clone(),
                        resource: lock.resource.clone(),
                        lock_type: lock.lock_type,
                        status: LockStatus::ForceReleased,
                        owner: lock.owner.clone(),
                        acquired_at: SystemTime::now(),
                        expires_at: SystemTime::now(),
                        last_refreshed: SystemTime::now(),
                        metadata: crate::types::LockMetadata::default(),
                        priority: lock.priority,
                        wait_start_time: None,
                    },
                    Duration::ZERO,
                ));
            }
        }

        Err(LockError::internal("Lock not found"))
    }

    /// Refresh lock
    pub async fn refresh_lock(&self, lock_id: &LockId, owner: &str) -> Result<LockResponse> {
        let resource_key = self.get_resource_key_from_lock_id(lock_id);

        // Check if we hold this lock
        {
            let locks = self.locks.read().await;
            if let Some(lock) = locks.get(&resource_key) {
                if lock.owner == owner && !lock.has_expired() {
                    // 提前 clone 所需字段
                    let priority = lock.priority;
                    let lock_id = lock.lock_id.clone();
                    let resource = lock.resource.clone();
                    let lock_type = lock.lock_type;
                    let owner = lock.owner.clone();
                    let holders = lock.holders.clone();
                    let acquired_at = lock.acquired_at;
                    let expires_at = Instant::now() + self.config.expiration_time;

                    // 更新锁
                    let mut locks = self.locks.write().await;
                    locks.insert(
                        resource_key.clone(),
                        DistributedLockEntry {
                            lock_id: lock_id.clone(),
                            resource: resource.clone(),
                            lock_type,
                            owner: owner.clone(),
                            priority,
                            acquired_at,
                            expires_at,
                            holders,
                            refresh_handle: None,
                        },
                    );

                    return Ok(LockResponse::success(
                        LockInfo {
                            id: lock_id,
                            resource,
                            lock_type,
                            status: LockStatus::Acquired,
                            owner,
                            acquired_at: SystemTime::now(),
                            expires_at: SystemTime::now() + self.config.expiration_time,
                            last_refreshed: SystemTime::now(),
                            metadata: crate::types::LockMetadata::default(),
                            priority,
                            wait_start_time: None,
                        },
                        Duration::ZERO,
                    ));
                }
            }
        }

        Err(LockError::internal("Lock not found or expired"))
    }
}

impl Default for DistributedLockManager {
    fn default() -> Self {
        Self::new(QuorumConfig::new(3, 1), Uuid::new_v4().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LockType;

    #[tokio::test]
    async fn test_quorum_config() {
        let config = QuorumConfig::new(5, 2);
        assert!(config.is_valid());
        assert_eq!(config.quorum, 3);
    }

    #[tokio::test]
    async fn test_distributed_lock_entry() {
        let lock_id = LockId::new("test-resource");
        let entry = DistributedLockEntry::new(
            lock_id.clone(),
            "test-resource".to_string(),
            LockType::Exclusive,
            "test-owner".to_string(),
            LockPriority::Normal,
            Duration::from_secs(60),
        );

        assert_eq!(entry.lock_id, lock_id);
        assert!(!entry.has_expired());
        assert!(entry.remaining_time() > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_distributed_lock_manager_creation() {
        let config = QuorumConfig::new(3, 1);
        let manager = DistributedLockManager::new(config, "node-1".to_string());

        let stats = manager.get_stats().await;
        assert_eq!(stats.successful_acquires, 0);
        assert_eq!(stats.failed_acquires, 0);
    }

    #[tokio::test]
    async fn test_force_release_lock() {
        let config = QuorumConfig::new(3, 1);
        let manager = DistributedLockManager::new(config, "node-1".to_string());

        let lock_id = LockId::new("test-resource");
        let result = manager.force_release_lock(&lock_id).await;

        // Should fail because lock doesn't exist
        assert!(result.is_err());
    }
}
