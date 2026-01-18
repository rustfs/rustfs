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

pub mod local;
// pub mod remote;

use crate::{LockId, LockInfo, LockRequest, LockResponse, LockStats, LockType, Result};
use async_trait::async_trait;
use std::sync::Arc;

/// Lock client trait
#[async_trait]
pub trait LockClient: Send + Sync + std::fmt::Debug {
    /// Acquire exclusive lock
    async fn acquire_exclusive(&self, request: &LockRequest) -> Result<LockResponse>;

    /// Acquire shared lock
    async fn acquire_shared(&self, request: &LockRequest) -> Result<LockResponse>;

    /// Acquire lock (generic method)
    async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        match request.lock_type {
            LockType::Exclusive => self.acquire_exclusive(request).await,
            LockType::Shared => self.acquire_shared(request).await,
        }
    }

    /// Release lock
    async fn release(&self, lock_id: &LockId) -> Result<bool>;

    /// Refresh lock
    async fn refresh(&self, lock_id: &LockId) -> Result<bool>;

    /// Force release lock
    async fn force_release(&self, lock_id: &LockId) -> Result<bool>;

    /// Check lock status
    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>>;

    /// Get statistics
    async fn get_stats(&self) -> Result<LockStats>;

    /// Close client
    async fn close(&self) -> Result<()>;

    /// Check if client is online
    async fn is_online(&self) -> bool;

    /// Check if client is local
    async fn is_local(&self) -> bool;
}

/// Client factory
pub struct ClientFactory;

impl ClientFactory {
    /// Create local client
    pub fn create_local() -> Arc<dyn LockClient> {
        Arc::new(local::LocalClient::new())
    }

    // /// Create remote client
    // pub fn create_remote(endpoint: String) -> Arc<dyn LockClient> {
    //     Arc::new(remote::RemoteClient::new(endpoint))
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LockType;

    #[tokio::test]
    async fn test_local_client_basic_operations() {
        let client = ClientFactory::create_local();

        let request = LockRequest::new("test-resource", LockType::Exclusive, "test-owner");

        // Test lock acquisition
        let response = client.acquire_exclusive(&request).await;
        assert!(response.is_ok());

        if let Ok(response) = response
            && response.success
        {
            let lock_info = response.lock_info.unwrap();

            // Test status check
            let status = client.check_status(&lock_info.id).await;
            assert!(status.is_ok());
            assert!(status.unwrap().is_some());

            // Test lock release
            let released = client.release(&lock_info.id).await;
            assert!(released.is_ok());
            assert!(released.unwrap());
        }
    }
}
