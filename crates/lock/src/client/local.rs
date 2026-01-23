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
use tokio::sync::RwLock;

use crate::{
    FastLockGuard, GlobalLockManager, LockClient, LockId, LockInfo, LockManager, LockMetadata, LockPriority, LockRequest,
    LockResponse, LockStats, LockStatus, LockType, Result,
};

/// Local lock client using FastLock
#[derive(Debug, Clone)]
pub struct LocalClient {
    guard_storage: Arc<RwLock<HashMap<LockId, FastLockGuard>>>,
}

impl LocalClient {
    /// Create new local client
    pub fn new() -> Self {
        Self {
            guard_storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the global lock manager
    pub fn get_lock_manager(&self) -> Arc<GlobalLockManager> {
        crate::get_global_lock_manager()
    }
}

impl Default for LocalClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl LockClient for LocalClient {
    async fn acquire_exclusive(&self, request: &LockRequest) -> Result<LockResponse> {
        let lock_manager = self.get_lock_manager();
        let lock_request = crate::ObjectLockRequest::new_write(request.resource.clone(), request.owner.clone())
            .with_acquire_timeout(request.acquire_timeout);

        match lock_manager.acquire_lock(lock_request).await {
            Ok(guard) => {
                let lock_id = LockId::new_deterministic(&request.resource);

                {
                    let mut guards = self.guard_storage.write().await;
                    guards.insert(lock_id.clone(), guard);
                }

                let lock_info = LockInfo {
                    id: lock_id,
                    resource: request.resource.clone(),
                    lock_type: LockType::Exclusive,
                    status: crate::types::LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.ttl,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                };
                Ok(LockResponse::success(lock_info, std::time::Duration::ZERO))
            }
            Err(crate::fast_lock::LockResult::Timeout) => {
                Ok(LockResponse::failure("Lock acquisition timeout", request.acquire_timeout))
            }
            Err(crate::fast_lock::LockResult::Conflict {
                current_owner,
                current_mode,
            }) => Ok(LockResponse::failure(
                format!("Lock conflict: resource held by {current_owner} in {current_mode:?} mode"),
                std::time::Duration::ZERO,
            )),
            Err(crate::fast_lock::LockResult::Acquired) => {
                unreachable!("Acquired should not be an error")
            }
        }
    }

    async fn acquire_shared(&self, request: &LockRequest) -> Result<LockResponse> {
        let lock_manager = self.get_lock_manager();
        let lock_request = crate::ObjectLockRequest::new_read(request.resource.clone(), request.owner.clone())
            .with_acquire_timeout(request.acquire_timeout);

        match lock_manager.acquire_lock(lock_request).await {
            Ok(guard) => {
                let lock_id = LockId::new_deterministic(&request.resource);

                // Store guard for later release
                let mut guards = self.guard_storage.write().await;
                guards.insert(lock_id.clone(), guard);

                let lock_info = LockInfo {
                    id: lock_id,
                    resource: request.resource.clone(),
                    lock_type: LockType::Shared,
                    status: crate::types::LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.ttl,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                };
                Ok(LockResponse::success(lock_info, std::time::Duration::ZERO))
            }
            Err(crate::fast_lock::LockResult::Timeout) => {
                Ok(LockResponse::failure("Lock acquisition timeout", request.acquire_timeout))
            }
            Err(crate::fast_lock::LockResult::Conflict {
                current_owner,
                current_mode,
            }) => Ok(LockResponse::failure(
                format!("Lock conflict: resource held by {current_owner} in {current_mode:?} mode"),
                std::time::Duration::ZERO,
            )),
            Err(crate::fast_lock::LockResult::Acquired) => {
                unreachable!("Acquired should not be an error")
            }
        }
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        let mut guards = self.guard_storage.write().await;
        if let Some(guard) = guards.remove(lock_id) {
            // Guard automatically releases the lock when dropped
            drop(guard);
            Ok(true)
        } else {
            // Lock not found or already released
            Ok(false)
        }
    }

    async fn refresh(&self, _lock_id: &LockId) -> Result<bool> {
        // For local locks, refresh is not needed as they don't expire automatically
        Ok(true)
    }

    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        self.release(lock_id).await
    }

    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        let guards = self.guard_storage.read().await;
        if let Some(guard) = guards.get(lock_id) {
            // We have an active guard for this lock
            let lock_type = match guard.mode() {
                crate::LockMode::Shared => LockType::Shared,
                crate::LockMode::Exclusive => LockType::Exclusive,
            };
            Ok(Some(LockInfo {
                id: lock_id.clone(),
                resource: lock_id.resource.clone(),
                lock_type,
                status: LockStatus::Acquired,
                owner: guard.owner().to_string(),
                acquired_at: std::time::SystemTime::now(),
                expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
                last_refreshed: std::time::SystemTime::now(),
                metadata: LockMetadata::default(),
                priority: LockPriority::Normal,
                wait_start_time: None,
            }))
        } else {
            Ok(None)
        }
    }

    async fn get_stats(&self) -> Result<LockStats> {
        Ok(LockStats::default())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn is_online(&self) -> bool {
        true
    }

    async fn is_local(&self) -> bool {
        true
    }
}
