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

    /// Convert LockRequest to batch operation
    async fn request_to_batch(&self, request: LockRequest) -> Result<bool> {
        let lock_map = self.get_lock_map();
        let resources = vec![request.resource];
        let timeout = request.timeout;

        match request.lock_type {
            LockType::Exclusive => lock_map.lock_batch(&resources, &request.owner, timeout, None).await,
            LockType::Shared => lock_map.rlock_batch(&resources, &request.owner, timeout, None).await,
        }
    }

    /// Convert LockId to resource for release
    async fn lock_id_to_batch_release(&self, lock_id: &LockId) -> Result<()> {
        let lock_map = self.get_lock_map();
        // For simplicity, we'll use the lock_id as resource name
        // In a real implementation, you might want to maintain a mapping
        let resources = vec![lock_id.as_str().to_string()];
        lock_map.unlock_batch(&resources, "unknown").await
    }
}

impl Default for LocalClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl LockClient for LocalClient {
    async fn acquire_exclusive(&self, request: LockRequest) -> Result<LockResponse> {
        let lock_map = self.get_lock_map();
        let success = lock_map
            .lock_with_ttl_id(&request.resource, &request.owner, request.timeout, None)
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
                expires_at: std::time::SystemTime::now() + request.timeout,
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

    async fn acquire_shared(&self, request: LockRequest) -> Result<LockResponse> {
        let lock_map = self.get_lock_map();
        let success = lock_map
            .rlock_with_ttl_id(&request.resource, &request.owner, request.timeout, None)
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
                expires_at: std::time::SystemTime::now() + request.timeout,
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
        lock_map
            .unlock_by_id(lock_id)
            .await
            .map_err(|e| crate::error::LockError::internal(format!("Release failed: {e}")))?;
        Ok(true)
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
        if let Some((resource, owner)) = lock_map.lockid_map.get(lock_id).map(|v| v.clone()) {
            let is_locked = lock_map.is_locked(&resource).await;
            if is_locked {
                Ok(Some(LockInfo {
                    id: lock_id.clone(),
                    resource,
                    lock_type: LockType::Exclusive, // 这里可进一步完善
                    status: crate::types::LockStatus::Acquired,
                    owner,
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
        let request =
            LockRequest::new("test-resource", LockType::Exclusive, "test-owner").with_timeout(std::time::Duration::from_secs(30));

        let response = client.acquire_exclusive(request).await.unwrap();
        assert!(response.is_success());
    }

    #[tokio::test]
    async fn test_local_client_acquire_shared() {
        let client = LocalClient::new();
        let request =
            LockRequest::new("test-resource", LockType::Shared, "test-owner").with_timeout(std::time::Duration::from_secs(30));

        let response = client.acquire_shared(request).await.unwrap();
        assert!(response.is_success());
    }

    #[tokio::test]
    async fn test_local_client_release() {
        let client = LocalClient::new();

        // First acquire a lock
        let request =
            LockRequest::new("test-resource", LockType::Exclusive, "test-owner").with_timeout(std::time::Duration::from_secs(30));
        let response = client.acquire_exclusive(request).await.unwrap();
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
}
