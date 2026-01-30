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

use crate::{LockId, LockInfo, LockRequest, LockResponse, LockStats, Result};
use async_trait::async_trait;
use std::sync::Arc;

/// Lock client trait
#[async_trait]
pub trait LockClient: Send + Sync + std::fmt::Debug {
    /// Acquire lock (generic method)
    async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse>;

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
