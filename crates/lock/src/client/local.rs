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
        let lock_request = crate::ObjectLockRequest::new_write("", request.resource.clone(), request.owner.clone())
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
        let lock_request = crate::ObjectLockRequest::new_read("", request.resource.clone(), request.owner.clone())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LockType;

    #[tokio::test]
    async fn test_local_client_acquire_exclusive() {
        let client = LocalClient::new();
        let resource_name = format!("test-resource-exclusive-{}", uuid::Uuid::new_v4());
        let request = LockRequest::new(&resource_name, LockType::Exclusive, "test-owner")
            .with_acquire_timeout(std::time::Duration::from_secs(30));

        let response = client.acquire_exclusive(&request).await.unwrap();
        assert!(response.is_success());

        // Clean up
        if let Some(lock_info) = response.lock_info() {
            let _ = client.release(&lock_info.id).await;
        }
    }

    #[tokio::test]
    async fn test_local_client_acquire_shared() {
        let client = LocalClient::new();
        let resource_name = format!("test-resource-shared-{}", uuid::Uuid::new_v4());
        let request = LockRequest::new(&resource_name, LockType::Shared, "test-owner")
            .with_acquire_timeout(std::time::Duration::from_secs(30));

        let response = client.acquire_shared(&request).await.unwrap();
        assert!(response.is_success());

        // Clean up
        if let Some(lock_info) = response.lock_info() {
            let _ = client.release(&lock_info.id).await;
        }
    }

    #[tokio::test]
    async fn test_local_client_release() {
        let client = LocalClient::new();
        let resource_name = format!("test-resource-release-{}", uuid::Uuid::new_v4());

        // First acquire a lock
        let request = LockRequest::new(&resource_name, LockType::Exclusive, "test-owner")
            .with_acquire_timeout(std::time::Duration::from_secs(30));
        let response = client.acquire_exclusive(&request).await.unwrap();
        assert!(response.is_success());

        // Get the lock ID from the response
        if let Some(lock_info) = response.lock_info() {
            let result = client.release(&lock_info.id).await.unwrap();
            assert!(result);
        } else {
            panic!("No lock info in response");
        }
    }

    #[tokio::test]
    async fn test_local_client_is_local() {
        let client = LocalClient::new();
        assert!(client.is_local().await);
    }

    #[tokio::test]
    async fn test_local_client_read_write_lock_exclusion() {
        let client = LocalClient::new();
        let resource_name = format!("test-resource-exclusion-{}", uuid::Uuid::new_v4());

        // First, acquire an exclusive lock
        let exclusive_request = LockRequest::new(&resource_name, LockType::Exclusive, "exclusive-owner")
            .with_acquire_timeout(std::time::Duration::from_millis(10));
        let exclusive_response = client.acquire_exclusive(&exclusive_request).await.unwrap();
        assert!(exclusive_response.is_success());

        // Try to acquire a shared lock on the same resource - should fail
        let shared_request = LockRequest::new(&resource_name, LockType::Shared, "shared-owner")
            .with_acquire_timeout(std::time::Duration::from_millis(10));
        let shared_response = client.acquire_shared(&shared_request).await.unwrap();
        assert!(!shared_response.is_success(), "Shared lock should fail when exclusive lock exists");

        // Clean up exclusive lock
        if let Some(exclusive_info) = exclusive_response.lock_info() {
            let _ = client.release(&exclusive_info.id).await;
        }

        // Now shared lock should succeed
        let shared_request2 = LockRequest::new(&resource_name, LockType::Shared, "shared-owner")
            .with_acquire_timeout(std::time::Duration::from_millis(10));
        let shared_response2 = client.acquire_shared(&shared_request2).await.unwrap();
        assert!(
            shared_response2.is_success(),
            "Shared lock should succeed after exclusive lock is released"
        );

        // Clean up
        if let Some(shared_info) = shared_response2.lock_info() {
            let _ = client.release(&shared_info.id).await;
        }
    }

    #[tokio::test]
    async fn test_local_client_read_write_lock_distinction() {
        let client = LocalClient::new();
        let resource_name = format!("test-resource-rw-{}", uuid::Uuid::new_v4());

        // Test exclusive lock
        let exclusive_request = LockRequest::new(&resource_name, LockType::Exclusive, "exclusive-owner")
            .with_acquire_timeout(std::time::Duration::from_secs(30));
        let exclusive_response = client.acquire_exclusive(&exclusive_request).await.unwrap();
        assert!(exclusive_response.is_success());

        if let Some(exclusive_info) = exclusive_response.lock_info() {
            assert_eq!(exclusive_info.lock_type, LockType::Exclusive);

            // Check status should return correct lock type
            let status = client.check_status(&exclusive_info.id).await.unwrap();
            assert!(status.is_some());
            assert_eq!(status.unwrap().lock_type, LockType::Exclusive);

            // Release exclusive lock
            let result = client.release(&exclusive_info.id).await.unwrap();
            assert!(result);
        }

        // Test shared lock
        let shared_request = LockRequest::new(&resource_name, LockType::Shared, "shared-owner")
            .with_acquire_timeout(std::time::Duration::from_secs(30));
        let shared_response = client.acquire_shared(&shared_request).await.unwrap();
        assert!(shared_response.is_success());

        if let Some(shared_info) = shared_response.lock_info() {
            assert_eq!(shared_info.lock_type, LockType::Shared);

            // Check status should return correct lock type
            let status = client.check_status(&shared_info.id).await.unwrap();
            assert!(status.is_some());
            assert_eq!(status.unwrap().lock_type, LockType::Shared);

            // Release shared lock
            let result = client.release(&shared_info.id).await.unwrap();
            assert!(result);
        }
    }

    #[tokio::test]
    async fn test_multiple_local_clients_exclusive_mutex() {
        let client1 = LocalClient::new();
        let client2 = LocalClient::new();
        let resource_name = format!("test-multi-client-mutex-{}", uuid::Uuid::new_v4());

        // client1 acquire exclusive lock
        let req1 = LockRequest::new(&resource_name, LockType::Exclusive, "owner1")
            .with_acquire_timeout(std::time::Duration::from_millis(50));
        let resp1 = client1.acquire_exclusive(&req1).await.unwrap();
        assert!(resp1.is_success(), "client1 should acquire exclusive lock");

        // client2 try to acquire exclusive lock, should fail
        let req2 = LockRequest::new(&resource_name, LockType::Exclusive, "owner2")
            .with_acquire_timeout(std::time::Duration::from_millis(50));
        let resp2 = client2.acquire_exclusive(&req2).await.unwrap();
        assert!(!resp2.is_success(), "client2 should not acquire exclusive lock while client1 holds it");

        // client1 release lock
        if let Some(lock_info) = resp1.lock_info() {
            let _ = client1.release(&lock_info.id).await;
        }

        // client2 try again, should succeed
        let resp3 = client2.acquire_exclusive(&req2).await.unwrap();
        assert!(resp3.is_success(), "client2 should acquire exclusive lock after client1 releases it");

        // clean up
        if let Some(lock_info) = resp3.lock_info() {
            let _ = client2.release(&lock_info.id).await;
        }
    }
}
