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

use std::sync::Arc;

use crate::{
    client::LockClient,
    error::Result,
    local::LocalLockMap,
    types::{LockId, LockInfo, LockMetadata, LockPriority, LockRequest, LockResponse, LockStats, LockType},
};

/// Local lock client
///
/// Uses global singleton LocalLockMap to ensure all clients access the same lock instance
#[derive(Debug, Clone)]
pub struct LocalClient;

impl LocalClient {
    /// Create new local client
    pub fn new() -> Self {
        Self
    }

    /// Get global lock map instance
    pub fn get_lock_map(&self) -> Arc<LocalLockMap> {
        crate::get_global_lock_map()
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
        let lock_map = self.get_lock_map();
        let success = lock_map
            .lock_with_ttl_id(request)
            .await
            .map_err(|e| crate::error::LockError::internal(format!("Lock acquisition failed: {e}")))?;
        if success {
            let lock_info = LockInfo {
                id: crate::types::LockId::new_deterministic(&request.resource),
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
        } else {
            Ok(LockResponse::failure("Lock acquisition failed".to_string(), std::time::Duration::ZERO))
        }
    }

    async fn acquire_shared(&self, request: &LockRequest) -> Result<LockResponse> {
        let lock_map = self.get_lock_map();
        let success = lock_map
            .rlock_with_ttl_id(request)
            .await
            .map_err(|e| crate::error::LockError::internal(format!("Shared lock acquisition failed: {e}")))?;
        if success {
            let lock_info = LockInfo {
                id: crate::types::LockId::new_deterministic(&request.resource),
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
        } else {
            Ok(LockResponse::failure("Lock acquisition failed".to_string(), std::time::Duration::ZERO))
        }
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        let lock_map = self.get_lock_map();

        // Try to release the lock directly by ID
        match lock_map.unlock_by_id(lock_id).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Try as read lock if exclusive unlock failed
                match lock_map.runlock_by_id(lock_id).await {
                    Ok(()) => Ok(true),
                    Err(_) => Err(crate::error::LockError::internal("Lock ID not found".to_string())),
                }
            }
            Err(e) => Err(crate::error::LockError::internal(format!("Release lock failed: {e}"))),
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
        let lock_map = self.get_lock_map();

        // Check if the lock exists in our locks map
        let locks_guard = lock_map.locks.read().await;
        if let Some(entry) = locks_guard.get(lock_id) {
            let entry_guard = entry.read().await;

            // Determine lock type and owner based on the entry
            if let Some(owner) = &entry_guard.writer {
                Ok(Some(LockInfo {
                    id: lock_id.clone(),
                    resource: lock_id.resource.clone(),
                    lock_type: crate::types::LockType::Exclusive,
                    status: crate::types::LockStatus::Acquired,
                    owner: owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: LockMetadata::default(),
                    priority: LockPriority::Normal,
                    wait_start_time: None,
                }))
            } else if !entry_guard.readers.is_empty() {
                Ok(Some(LockInfo {
                    id: lock_id.clone(),
                    resource: lock_id.resource.clone(),
                    lock_type: crate::types::LockType::Shared,
                    status: crate::types::LockStatus::Acquired,
                    owner: entry_guard.readers.iter().next().map(|(k, _)| k.clone()).unwrap_or_default(),
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
    use crate::types::LockType;

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
