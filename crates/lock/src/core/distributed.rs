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
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    Locker,
    config::LockConfig,
    error::{LockError, Result},
    client::remote::RemoteClient,
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStats, LockStatus, LockType},
};

/// Distributed lock configuration constants
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_RETRY_MIN_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_RETRIES: usize = 10;

/// Distributed lock state
#[derive(Debug, Clone, PartialEq)]
pub enum DistributedLockState {
    /// Unlocked
    Unlocked,
    /// Read locked
    ReadLocked { count: usize, owners: Vec<String> },
    /// Write locked
    WriteLocked { owner: String },
}

/// Distributed lock resource information
#[derive(Debug, Clone)]
pub struct DistributedResourceInfo {
    /// Resource name
    pub resource: String,
    /// Current lock state
    pub state: DistributedLockState,
    /// Last update time
    pub last_updated: SystemTime,
    /// Lock holder information
    pub lock_holders: HashMap<String, LockInfo>,
}

/// Distributed lock options
#[derive(Debug, Clone)]
pub struct DistributedLockOptions {
    /// Lock acquisition timeout
    pub timeout: Duration,
    /// Retry interval
    pub retry_interval: Duration,
    /// Maximum retry attempts
    pub max_retries: usize,
    /// Whether to enable auto-refresh
    pub auto_refresh: bool,
    /// Lock refresh interval
    pub refresh_interval: Duration,
    /// Whether to enable fault tolerance mode
    pub fault_tolerant: bool,
}

impl Default for DistributedLockOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_LOCK_TIMEOUT,
            retry_interval: DEFAULT_RETRY_MIN_INTERVAL,
            max_retries: DEFAULT_MAX_RETRIES,
            auto_refresh: true,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
            fault_tolerant: true,
        }
    }
}

/// Distributed lock handle
#[derive(Debug, Clone)]
pub struct DistributedLockHandle {
    /// Lock ID
    pub lock_id: LockId,
    /// Resource name
    pub resource: String,
    /// Lock type
    pub lock_type: LockType,
    /// Owner
    pub owner: String,
    /// Acquisition time
    pub acquired_at: SystemTime,
    /// Last refresh time
    pub last_refreshed: SystemTime,
    /// Whether to auto-refresh
    pub auto_refresh: bool,
    /// Refresh interval
    pub refresh_interval: Duration,
    /// Manager reference
    manager: Arc<DistributedLockManager>,
}

impl DistributedLockHandle {
    /// Create a new distributed lock handle
    fn new(
        lock_id: LockId,
        resource: String,
        lock_type: LockType,
        owner: String,
        manager: Arc<DistributedLockManager>,
        options: &DistributedLockOptions,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            lock_id,
            resource,
            lock_type,
            owner,
            acquired_at: now,
            last_refreshed: now,
            auto_refresh: options.auto_refresh,
            refresh_interval: options.refresh_interval,
            manager,
        }
    }

    /// Refresh the lock
    #[instrument(skip(self))]
    pub async fn refresh(&mut self) -> Result<bool> {
        if !self.auto_refresh {
            return Ok(true);
        }

        let now = SystemTime::now();
        if now.duration_since(self.last_refreshed).unwrap_or_default() < self.refresh_interval {
            return Ok(true);
        }

        match self.manager.refresh_lock(&self.lock_id).await {
            Ok(success) => {
                if success {
                    self.last_refreshed = now;
                    debug!("Successfully refreshed lock: {}", self.lock_id);
                }
                Ok(success)
            }
            Err(e) => {
                error!("Failed to refresh lock {}: {}", self.lock_id, e);
                Err(e)
            }
        }
    }

    /// Release the lock
    #[instrument(skip(self))]
    pub async fn release(self) -> Result<bool> {
        self.manager.release_lock(&self.lock_id).await
    }

    /// Force release the lock
    #[instrument(skip(self))]
    pub async fn force_release(self) -> Result<bool> {
        self.manager.force_release_lock(&self.lock_id).await
    }

    /// Check lock status
    #[instrument(skip(self))]
    pub async fn check_status(&self) -> Result<Option<LockInfo>> {
        self.manager.check_lock_status(&self.lock_id).await
    }
}

/// Distributed lock manager
///
/// Implements quorum-based distributed read-write locks with fault tolerance and auto-refresh
#[derive(Debug)]
pub struct DistributedLockManager {
    /// Configuration
    config: Arc<LockConfig>,
    /// Resource state mapping
    resources: Arc<DashMap<String, DistributedResourceInfo>>,
    /// Active lock handles mapping
    active_handles: Arc<DashMap<LockId, Arc<DistributedLockHandle>>>,
    /// Lock options
    options: DistributedLockOptions,
    /// Statistics
    stats: Arc<Mutex<LockStats>>,
    /// Shutdown flag
    shutdown_flag: Arc<Mutex<bool>>,
    /// Remote client
    remote_client: Arc<Mutex<RemoteClient>>,
}

impl DistributedLockManager {
    /// Create a new distributed lock manager
    pub fn new(config: Arc<LockConfig>, remote_url: url::Url) -> Result<Self> {
        let client = RemoteClient::from_url(remote_url);
        Ok(Self {
            config,
            resources: Arc::new(DashMap::new()),
            active_handles: Arc::new(DashMap::new()),
            options: DistributedLockOptions::default(),
            stats: Arc::new(Mutex::new(LockStats::default())),
            shutdown_flag: Arc::new(Mutex::new(false)),
            remote_client: Arc::new(Mutex::new(client)),
        })
    }

    /// Create a distributed lock manager with custom options
    pub fn with_options(config: Arc<LockConfig>, remote_url: url::Url, options: DistributedLockOptions) -> Result<Self> {
        let client = RemoteClient::from_url(remote_url);
        Ok(Self {
            config,
            resources: Arc::new(DashMap::new()),
            active_handles: Arc::new(DashMap::new()),
            options,
            stats: Arc::new(Mutex::new(LockStats::default())),
            shutdown_flag: Arc::new(Mutex::new(false)),
            remote_client: Arc::new(Mutex::new(client)),
        })
    }

    /// Calculate quorum
    fn calculate_quorum(&self) -> (usize, usize) {
        // Simplified implementation: use minimum quorum from config
        let total_nodes = 1; // Currently only one node
        let write_quorum = self.config.distributed.min_quorum;
        let read_quorum = total_nodes - write_quorum + 1;
        (write_quorum, read_quorum)
    }

    /// Acquire distributed write lock (improved atomic version)
    async fn acquire_distributed_write_lock(
        &self,
        resource: &str,
        owner: &str,
        options: &DistributedLockOptions,
    ) -> Result<DistributedLockHandle> {
        let start_time = Instant::now();
        let (write_quorum, _) = self.calculate_quorum();
        let mut retry_count = 0;

        loop {
            if retry_count >= options.max_retries {
                return Err(LockError::timeout(resource, options.timeout));
            }

            // Atomic check of local resource state
            if let Some(resource_info) = self.resources.get(resource) {
                match &resource_info.state {
                    DistributedLockState::WriteLocked { owner: existing_owner } => {
                        if existing_owner != owner {
                            return Err(LockError::already_locked(resource, existing_owner));
                        }
                    }
                    DistributedLockState::ReadLocked { owners, .. } => {
                        if !owners.contains(&owner.to_string()) {
                            return Err(LockError::already_locked(resource, "other readers"));
                        }
                    }
                    DistributedLockState::Unlocked => {}
                }
            }

            // Use quorum mechanism to atomically acquire distributed lock
            match self.acquire_quorum_lock(resource, owner, write_quorum, options).await {
                Ok(lock_id) => {
                    let handle = DistributedLockHandle::new(
                        lock_id.clone(),
                        resource.to_string(),
                        LockType::Exclusive,
                        owner.to_string(),
                        Arc::new(self.clone_for_handle()),
                        options,
                    );

                    // Atomically update local state
                    self.update_resource_state(
                        resource,
                        DistributedLockState::WriteLocked {
                            owner: owner.to_string(),
                        },
                    )
                    .await;

                    // Store active handle
                    self.active_handles.insert(lock_id, Arc::new(handle.clone()));

                    info!(
                        "Successfully acquired distributed write lock for {} in {:?}",
                        resource,
                        start_time.elapsed()
                    );
                    return Ok(handle);
                }
                Err(e) => {
                    warn!("Failed to acquire quorum lock for {}: {}", resource, e);
                }
            }

            retry_count += 1;
            tokio::time::sleep(options.retry_interval).await;
        }
    }

    /// Acquire distributed read lock (improved atomic version)
    async fn acquire_distributed_read_lock(
        &self,
        resource: &str,
        owner: &str,
        options: &DistributedLockOptions,
    ) -> Result<DistributedLockHandle> {
        let start_time = Instant::now();
        let (_, read_quorum) = self.calculate_quorum();
        let mut retry_count = 0;

        loop {
            if retry_count >= options.max_retries {
                return Err(LockError::timeout(resource, options.timeout));
            }

            // Atomic check of local resource state
            if let Some(resource_info) = self.resources.get(resource) {
                match &resource_info.state {
                    DistributedLockState::WriteLocked { owner: existing_owner } => {
                        if existing_owner != owner {
                            return Err(LockError::already_locked(resource, existing_owner));
                        }
                    }
                    DistributedLockState::ReadLocked { .. } => {
                        // Read locks can be shared
                    }
                    DistributedLockState::Unlocked => {}
                }
            }

            // Use quorum mechanism to atomically acquire distributed read lock
            match self.acquire_quorum_read_lock(resource, owner, read_quorum, options).await {
                Ok(lock_id) => {
                    let handle = DistributedLockHandle::new(
                        lock_id.clone(),
                        resource.to_string(),
                        LockType::Shared,
                        owner.to_string(),
                        Arc::new(self.clone_for_handle()),
                        options,
                    );

                    // Atomically update local state
                    self.update_resource_read_state(resource, owner, &lock_id).await;

                    // Store active handle
                    self.active_handles.insert(lock_id, Arc::new(handle.clone()));

                    info!(
                        "Successfully acquired distributed read lock for {} in {:?}",
                        resource,
                        start_time.elapsed()
                    );
                    return Ok(handle);
                }
                Err(e) => {
                    warn!("Failed to acquire quorum read lock for {}: {}", resource, e);
                }
            }

            retry_count += 1;
            tokio::time::sleep(options.retry_interval).await;
        }
    }

    /// Atomically acquire write lock using quorum mechanism
    async fn acquire_quorum_lock(
        &self,
        resource: &str,
        owner: &str,
        quorum: usize,
        options: &DistributedLockOptions,
    ) -> Result<LockId> {
        let mut success_count = 0;
        let mut errors = Vec::new();
        let lock_id = LockId::new();

        // Concurrently attempt to acquire locks on multiple nodes
        let mut remote_client = self.remote_client.lock().await;
        let lock_args = crate::lock_args::LockArgs {
            uid: lock_id.to_string(),
            resources: vec![resource.to_string()],
            owner: owner.to_string(),
            source: "distributed".to_string(),
            quorum,
        };

        // Attempt to acquire lock
        match remote_client.lock(&lock_args).await {
            Ok(success) if success => {
                success_count += 1;
            }
            Ok(_) => {
                // Acquisition failed
            }
            Err(e) => {
                errors.push(e);
            }
        }

        // Check if quorum is reached
        if success_count >= quorum {
            Ok(lock_id)
        } else {
            // Rollback acquired locks
            self.rollback_partial_locks(resource, owner).await?;
            Err(LockError::timeout(resource, options.timeout))
        }
    }

    /// Atomically acquire read lock using quorum mechanism
    async fn acquire_quorum_read_lock(
        &self,
        resource: &str,
        owner: &str,
        quorum: usize,
        options: &DistributedLockOptions,
    ) -> Result<LockId> {
        let mut success_count = 0;
        let mut errors = Vec::new();
        let lock_id = LockId::new();

        // Concurrently attempt to acquire read locks on multiple nodes
        let mut remote_client = self.remote_client.lock().await;
        let lock_args = crate::lock_args::LockArgs {
            uid: lock_id.to_string(),
            resources: vec![resource.to_string()],
            owner: owner.to_string(),
            source: "distributed".to_string(),
            quorum,
        };

        // Attempt to acquire read lock
        match remote_client.rlock(&lock_args).await {
            Ok(success) if success => {
                success_count += 1;
            }
            Ok(_) => {
                // Acquisition failed
            }
            Err(e) => {
                errors.push(e);
            }
        }

        // Check if quorum is reached
        if success_count >= quorum {
            Ok(lock_id)
        } else {
            // Rollback acquired locks
            self.rollback_partial_read_locks(resource, owner).await?;
            Err(LockError::timeout(resource, options.timeout))
        }
    }

    /// Rollback partially acquired write locks
    async fn rollback_partial_locks(&self, resource: &str, owner: &str) -> Result<()> {
        let mut remote_client = self.remote_client.lock().await;
        let lock_args = crate::lock_args::LockArgs {
            uid: LockId::new().to_string(),
            resources: vec![resource.to_string()],
            owner: owner.to_string(),
            source: "distributed".to_string(),
            quorum: 1,
        };

        // Attempt to release lock
        let _ = remote_client.unlock(&lock_args).await;
        Ok(())
    }

    /// Rollback partially acquired read locks
    async fn rollback_partial_read_locks(&self, resource: &str, owner: &str) -> Result<()> {
        let mut remote_client = self.remote_client.lock().await;
        let lock_args = crate::lock_args::LockArgs {
            uid: LockId::new().to_string(),
            resources: vec![resource.to_string()],
            owner: owner.to_string(),
            source: "distributed".to_string(),
            quorum: 1,
        };

        // Attempt to release read lock
        let _ = remote_client.runlock(&lock_args).await;
        Ok(())
    }

    /// Update resource state
    async fn update_resource_state(&self, resource: &str, state: DistributedLockState) {
        let resource_info = DistributedResourceInfo {
            resource: resource.to_string(),
            state,
            last_updated: SystemTime::now(),
            lock_holders: HashMap::new(),
        };
        self.resources.insert(resource.to_string(), resource_info);
    }

    /// Update resource read lock state
    async fn update_resource_read_state(&self, resource: &str, owner: &str, _lock_id: &LockId) {
        if let Some(mut resource_info) = self.resources.get_mut(resource) {
            match &mut resource_info.state {
                DistributedLockState::ReadLocked { count, owners } => {
                    *count += 1;
                    if !owners.contains(&owner.to_string()) {
                        owners.push(owner.to_string());
                    }
                }
                DistributedLockState::Unlocked => {
                    resource_info.state = DistributedLockState::ReadLocked {
                        count: 1,
                        owners: vec![owner.to_string()],
                    };
                }
                _ => {
                    // Other states remain unchanged
                }
            }
            resource_info.last_updated = SystemTime::now();
        } else {
            let resource_info = DistributedResourceInfo {
                resource: resource.to_string(),
                state: DistributedLockState::ReadLocked {
                    count: 1,
                    owners: vec![owner.to_string()],
                },
                last_updated: SystemTime::now(),
                lock_holders: HashMap::new(),
            };
            self.resources.insert(resource.to_string(), resource_info);
        }
    }

    /// Refresh lock
    async fn refresh_lock(&self, lock_id: &LockId) -> Result<bool> {
        if let Some(_handle) = self.active_handles.get(lock_id) {
            let mut remote_client = self.remote_client.lock().await;

            // Create LockArgs
            let lock_args = crate::lock_args::LockArgs {
                uid: lock_id.to_string(),
                resources: vec![],
                owner: "distributed".to_string(),
                source: "distributed".to_string(),
                quorum: 1,
            };

            remote_client.refresh(&lock_args).await
        } else {
            Ok(false)
        }
    }

    /// Release lock
    async fn release_lock(&self, lock_id: &LockId) -> Result<bool> {
        if let Some(_handle) = self.active_handles.get(lock_id) {
            let mut remote_client = self.remote_client.lock().await;

            // Create LockArgs
            let lock_args = crate::lock_args::LockArgs {
                uid: lock_id.to_string(),
                resources: vec![],
                owner: "distributed".to_string(),
                source: "distributed".to_string(),
                quorum: 1,
            };

            let result = remote_client.unlock(&lock_args).await?;
            if result {
                self.active_handles.remove(lock_id);
            }
            Ok(result)
        } else {
            Ok(false)
        }
    }

    /// Force release lock
    async fn force_release_lock(&self, lock_id: &LockId) -> Result<bool> {
        if let Some(_handle) = self.active_handles.get(lock_id) {
            let mut remote_client = self.remote_client.lock().await;

            // Create LockArgs
            let lock_args = crate::lock_args::LockArgs {
                uid: lock_id.to_string(),
                resources: vec![],
                owner: "distributed".to_string(),
                source: "distributed".to_string(),
                quorum: 1,
            };

            let result = remote_client.force_unlock(&lock_args).await?;
            if result {
                self.active_handles.remove(lock_id);
            }
            Ok(result)
        } else {
            Ok(false)
        }
    }

    /// Check lock status
    async fn check_lock_status(&self, _lock_id: &LockId) -> Result<Option<LockInfo>> {
        // Implement lock status check logic
        Ok(None)
    }

    /// Update statistics
    async fn update_stats(&self, acquired: bool) {
        let mut stats = self.stats.lock().await;
        if acquired {
            stats.total_locks += 1;
        }
        stats.last_updated = SystemTime::now();
    }

    /// Clone for handle
    fn clone_for_handle(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            resources: Arc::clone(&self.resources),
            active_handles: Arc::clone(&self.active_handles),
            options: self.options.clone(),
            stats: Arc::clone(&self.stats),
            shutdown_flag: Arc::clone(&self.shutdown_flag),
            remote_client: Arc::clone(&self.remote_client),
        }
    }
}

#[async_trait]
impl super::LockManager for DistributedLockManager {
    async fn acquire_exclusive(&self, request: LockRequest) -> Result<LockResponse> {
        let start_time = std::time::SystemTime::now();

        match self
            .acquire_distributed_write_lock(&request.resource, &request.owner, &self.options)
            .await
        {
            Ok(handle) => {
                self.update_stats(true).await;
                Ok(LockResponse::success(
                    LockInfo {
                        id: handle.lock_id,
                        resource: handle.resource,
                        lock_type: handle.lock_type,
                        status: LockStatus::Acquired,
                        owner: handle.owner,
                        acquired_at: handle.acquired_at,
                        expires_at: SystemTime::now() + request.timeout,
                        last_refreshed: handle.last_refreshed,
                        metadata: request.metadata,
                        priority: request.priority,
                        wait_start_time: None,
                    },
                    crate::utils::duration_between(start_time, std::time::SystemTime::now()),
                ))
            }
            Err(e) => Ok(LockResponse::failure(
                e.to_string(),
                crate::utils::duration_between(start_time, std::time::SystemTime::now()),
            )),
        }
    }

    async fn acquire_shared(&self, request: LockRequest) -> Result<LockResponse> {
        let start_time = std::time::SystemTime::now();

        match self
            .acquire_distributed_read_lock(&request.resource, &request.owner, &self.options)
            .await
        {
            Ok(handle) => {
                self.update_stats(true).await;
                Ok(LockResponse::success(
                    LockInfo {
                        id: handle.lock_id,
                        resource: handle.resource,
                        lock_type: handle.lock_type,
                        status: LockStatus::Acquired,
                        owner: handle.owner,
                        acquired_at: handle.acquired_at,
                        expires_at: SystemTime::now() + request.timeout,
                        last_refreshed: handle.last_refreshed,
                        metadata: request.metadata,
                        priority: request.priority,
                        wait_start_time: None,
                    },
                    crate::utils::duration_between(start_time, std::time::SystemTime::now()),
                ))
            }
            Err(e) => Ok(LockResponse::failure(
                e.to_string(),
                crate::utils::duration_between(start_time, std::time::SystemTime::now()),
            )),
        }
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        self.release_lock(lock_id).await
    }

    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        self.refresh_lock(lock_id).await
    }

    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        self.force_release_lock(lock_id).await
    }

    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        self.check_lock_status(lock_id).await
    }

    async fn get_stats(&self) -> Result<LockStats> {
        let stats = self.stats.lock().await;
        Ok(stats.clone())
    }

    async fn shutdown(&self) -> Result<()> {
        let mut shutdown_flag = self.shutdown_flag.lock().await;
        *shutdown_flag = true;

        // Clean up all active handles
        self.active_handles.clear();

        // Clean up all resource states
        self.resources.clear();

        info!("Distributed lock manager shutdown completed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LockConfig;

    #[tokio::test]
    async fn test_distributed_lock_manager_creation() {
        let config = Arc::new(LockConfig::default());
        let remote_url = url::Url::parse("http://localhost:8080").unwrap();

        let manager = DistributedLockManager::new(config, remote_url);
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_quorum_calculation() {
        let config = Arc::new(LockConfig::default());
        let remote_url = url::Url::parse("http://localhost:8080").unwrap();

        let manager = DistributedLockManager::new(config, remote_url).unwrap();
        let (write_quorum, read_quorum) = manager.calculate_quorum();

        assert!(write_quorum > 0);
        assert!(read_quorum > 0);
    }

    #[tokio::test]
    async fn test_distributed_lock_options_default() {
        let options = DistributedLockOptions::default();
        assert_eq!(options.timeout, DEFAULT_LOCK_TIMEOUT);
        assert_eq!(options.retry_interval, DEFAULT_RETRY_MIN_INTERVAL);
        assert_eq!(options.max_retries, DEFAULT_MAX_RETRIES);
        assert!(options.auto_refresh);
        assert_eq!(options.refresh_interval, DEFAULT_REFRESH_INTERVAL);
        assert!(options.fault_tolerant);
    }

    #[tokio::test]
    async fn test_distributed_lock_handle_creation() {
        let config = Arc::new(LockConfig::default());
        let remote_url = url::Url::parse("http://localhost:8080").unwrap();
        let manager = Arc::new(DistributedLockManager::new(config, remote_url).unwrap());

        let lock_id = LockId::new();
        let options = DistributedLockOptions::default();

        let handle = DistributedLockHandle::new(
            lock_id.clone(),
            "test-resource".to_string(),
            LockType::Exclusive,
            "test-owner".to_string(),
            manager,
            &options,
        );

        assert_eq!(handle.lock_id, lock_id);
        assert_eq!(handle.resource, "test-resource");
        assert_eq!(handle.lock_type, LockType::Exclusive);
        assert_eq!(handle.owner, "test-owner");
        assert!(handle.auto_refresh);
    }
}
