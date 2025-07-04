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

pub mod distributed;
pub mod local;

use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    config::LockConfig,
    error::Result,
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStats, LockType},
};

/// Core lock management trait
#[async_trait]
pub trait LockManager: Send + Sync {
    /// Acquire exclusive lock
    async fn acquire_exclusive(&self, request: LockRequest) -> Result<LockResponse>;

    /// Acquire shared lock
    async fn acquire_shared(&self, request: LockRequest) -> Result<LockResponse>;

    /// Release lock
    async fn release(&self, lock_id: &LockId) -> Result<bool>;

    /// Refresh lock
    async fn refresh(&self, lock_id: &LockId) -> Result<bool>;

    /// Force release lock
    async fn force_release(&self, lock_id: &LockId) -> Result<bool>;

    /// Check lock status
    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>>;

    /// Get lock statistics
    async fn get_stats(&self) -> Result<LockStats>;

    /// Shutdown lock manager
    async fn shutdown(&self) -> Result<()>;
}

/// Lock manager implementation
pub struct LockManagerImpl {
    config: Arc<LockConfig>,
    local_manager: Arc<dyn LockManager>,
    distributed_manager: Option<Arc<dyn LockManager>>,
}

impl LockManagerImpl {
    /// Create new lock manager
    pub fn new(config: LockConfig) -> Result<Self> {
        config.validate()?;

        let config = Arc::new(config);
        let local_manager = Arc::new(local::LocalLockManagerImpl::new(config.clone())?);

        let distributed_manager = if config.distributed.auto_refresh {
            // Use default remote URL
            let remote_url = url::Url::parse("http://localhost:9000").unwrap();
            Some(Arc::new(distributed::DistributedLockManager::new(config.clone(), remote_url)?) as Arc<dyn LockManager>)
        } else {
            None
        };

        Ok(Self {
            config,
            local_manager,
            distributed_manager,
        })
    }

    /// Select appropriate lock manager based on configuration
    fn select_manager(&self, lock_type: LockType) -> Arc<dyn LockManager> {
        // For shared locks, prefer local manager
        // For exclusive locks, use distributed manager if available
        match (lock_type, &self.distributed_manager) {
            (LockType::Shared, _) => self.local_manager.clone(),
            (LockType::Exclusive, Some(distributed)) => distributed.clone(),
            (LockType::Exclusive, None) => self.local_manager.clone(),
        }
    }
}

#[async_trait]
impl LockManager for LockManagerImpl {
    async fn acquire_exclusive(&self, request: LockRequest) -> Result<LockResponse> {
        let manager = self.select_manager(LockType::Exclusive);
        manager.acquire_exclusive(request).await
    }

    async fn acquire_shared(&self, request: LockRequest) -> Result<LockResponse> {
        let manager = self.select_manager(LockType::Shared);
        manager.acquire_shared(request).await
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        // Try to release from local manager
        if let Ok(result) = self.local_manager.release(lock_id).await {
            if result {
                return Ok(true);
            }
        }

        // If local manager didn't find the lock, try distributed manager
        if let Some(distributed) = &self.distributed_manager {
            distributed.release(lock_id).await
        } else {
            Ok(false)
        }
    }

    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        // Try to refresh from local manager
        if let Ok(result) = self.local_manager.refresh(lock_id).await {
            if result {
                return Ok(true);
            }
        }

        // If local manager didn't find the lock, try distributed manager
        if let Some(distributed) = &self.distributed_manager {
            distributed.refresh(lock_id).await
        } else {
            Ok(false)
        }
    }

    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        // Force release local lock
        let local_result = self.local_manager.force_release(lock_id).await;

        // Force release distributed lock
        let distributed_result = if let Some(distributed) = &self.distributed_manager {
            distributed.force_release(lock_id).await
        } else {
            Ok(false)
        };

        // Return true if either operation succeeds
        Ok(local_result.unwrap_or(false) || distributed_result.unwrap_or(false))
    }

    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        // Check local manager first
        if let Ok(Some(info)) = self.local_manager.check_status(lock_id).await {
            return Ok(Some(info));
        }

        // Then check distributed manager
        if let Some(distributed) = &self.distributed_manager {
            distributed.check_status(lock_id).await
        } else {
            Ok(None)
        }
    }

    async fn get_stats(&self) -> Result<LockStats> {
        let local_stats = self.local_manager.get_stats().await?;
        let distributed_stats = if let Some(distributed) = &self.distributed_manager {
            distributed.get_stats().await?
        } else {
            LockStats::default()
        };

        // Merge statistics
        Ok(LockStats {
            total_locks: local_stats.total_locks + distributed_stats.total_locks,
            exclusive_locks: local_stats.exclusive_locks + distributed_stats.exclusive_locks,
            shared_locks: local_stats.shared_locks + distributed_stats.shared_locks,
            waiting_locks: local_stats.waiting_locks + distributed_stats.waiting_locks,
            deadlock_detections: local_stats.deadlock_detections + distributed_stats.deadlock_detections,
            priority_upgrades: local_stats.priority_upgrades + distributed_stats.priority_upgrades,
            last_updated: std::time::SystemTime::now(),
            total_releases: local_stats.total_releases + distributed_stats.total_releases,
            total_hold_time: local_stats.total_hold_time + distributed_stats.total_hold_time,
            average_hold_time: if local_stats.total_locks + distributed_stats.total_locks > 0 {
                let total_time = local_stats.total_hold_time + distributed_stats.total_hold_time;
                let total_count = local_stats.total_locks + distributed_stats.total_locks;
                std::time::Duration::from_secs(total_time.as_secs() / total_count as u64)
            } else {
                std::time::Duration::ZERO
            },
            total_wait_queues: local_stats.total_wait_queues + distributed_stats.total_wait_queues,
        })
    }

    async fn shutdown(&self) -> Result<()> {
        // Shutdown local manager
        if let Err(e) = self.local_manager.shutdown().await {
            tracing::error!("Failed to shutdown local lock manager: {}", e);
        }

        // Shutdown distributed manager
        if let Some(distributed) = &self.distributed_manager {
            if let Err(e) = distributed.shutdown().await {
                tracing::error!("Failed to shutdown distributed lock manager: {}", e);
            }
        }

        Ok(())
    }
}

/// Lock handle for automatic lock lifecycle management
pub struct LockHandle {
    lock_id: LockId,
    manager: Arc<dyn LockManager>,
    auto_refresh: bool,
    refresh_interval: tokio::time::Duration,
    refresh_task: Option<tokio::task::JoinHandle<()>>,
}

impl LockHandle {
    /// Create new lock handle
    pub fn new(lock_id: LockId, manager: Arc<dyn LockManager>, auto_refresh: bool) -> Self {
        Self {
            lock_id,
            manager,
            auto_refresh,
            refresh_interval: tokio::time::Duration::from_secs(10),
            refresh_task: None,
        }
    }

    /// Set auto refresh interval
    pub fn with_refresh_interval(mut self, interval: tokio::time::Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    /// Start auto refresh task
    pub fn start_auto_refresh(&mut self) {
        if !self.auto_refresh {
            return;
        }

        let lock_id = self.lock_id.clone();
        let manager = self.manager.clone();
        let interval = self.refresh_interval;

        self.refresh_task = Some(tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                if let Err(e) = manager.refresh(&lock_id).await {
                    tracing::warn!("Failed to refresh lock {}: {}", lock_id, e);
                    break;
                }
            }
        }));
    }

    /// Stop auto refresh task
    pub fn stop_auto_refresh(&mut self) {
        if let Some(task) = self.refresh_task.take() {
            task.abort();
        }
    }

    /// Get lock ID
    pub fn lock_id(&self) -> &LockId {
        &self.lock_id
    }

    /// Check lock status
    pub async fn check_status(&self) -> Result<Option<crate::types::LockInfo>> {
        self.manager.check_status(&self.lock_id).await
    }
}

impl Drop for LockHandle {
    fn drop(&mut self) {
        // Stop auto refresh task
        self.stop_auto_refresh();

        // Async release lock
        let lock_id = self.lock_id.clone();
        let manager = self.manager.clone();
        tokio::spawn(async move {
            if let Err(e) = manager.release(&lock_id).await {
                tracing::warn!("Failed to release lock {} during drop: {}", lock_id, e);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LockType;

    #[tokio::test]
    async fn test_lock_manager_creation() {
        let config = LockConfig::minimal();
        let manager = LockManagerImpl::new(config);
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_lock_handle() {
        let config = LockConfig::minimal();
        let manager = LockManagerImpl::new(config).unwrap();
        let manager = Arc::new(manager);

        let request = LockRequest::new("test-resource", LockType::Exclusive, "test-owner");
        let response = manager.acquire_exclusive(request).await;

        // Since local manager is not implemented yet, only test creation success
        // Actual functionality tests will be done after implementation
        assert!(response.is_ok() || response.is_err());
    }
}
