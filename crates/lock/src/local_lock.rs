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

use crate::{
    GlobalLockManager, ObjectKey,
    error::Result,
    fast_lock::{FastLockGuard, LockManager, LockMode, ObjectLockRequest},
    types::{LockPriority, LockRequest, LockType},
};
use std::sync::Arc;
use std::time::Duration;

/// Local lock handler using GlobalLockManager
/// Directly uses FastObjectLockManager for high-performance local locking
#[derive(Debug)]
pub struct LocalLock {
    /// Global lock manager for fast local locks
    manager: Arc<GlobalLockManager>,
    /// Namespace identifier
    namespace: String,
}

impl LocalLock {
    /// Create new local lock
    pub fn new(namespace: String, manager: Arc<GlobalLockManager>) -> Self {
        Self { namespace, manager }
    }

    /// Get namespace identifier
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get resource key for this namespace
    pub fn get_resource_key(&self, resource: &ObjectKey) -> String {
        format!("{}:{}", self.namespace, resource)
    }

    /// Acquire a lock and return a RAII guard
    pub(crate) async fn acquire_guard(&self, request: &LockRequest) -> Result<Option<FastLockGuard>> {
        // Convert LockRequest to ObjectLockRequest
        let object_key = request.resource.clone();
        let mode = match request.lock_type {
            LockType::Exclusive => LockMode::Exclusive,
            LockType::Shared => LockMode::Shared,
        };
        let owner: Arc<str> = request.owner.clone().into();

        // Convert LockPriority from types::LockPriority to fast_lock::types::LockPriority
        let fast_priority = match request.priority {
            LockPriority::Low => crate::fast_lock::types::LockPriority::Low,
            LockPriority::Normal => crate::fast_lock::types::LockPriority::Normal,
            LockPriority::High => crate::fast_lock::types::LockPriority::High,
            LockPriority::Critical => crate::fast_lock::types::LockPriority::Critical,
        };

        let object_request = ObjectLockRequest {
            key: object_key,
            mode,
            owner,
            acquire_timeout: request.acquire_timeout,
            lock_timeout: request.ttl,
            priority: fast_priority,
        };

        match self.manager.as_ref().acquire_lock(object_request).await {
            Ok(guard) => Ok(Some(guard)),
            Err(_) => Ok(None),
        }
    }

    /// Convenience: acquire exclusive lock as a guard
    pub async fn lock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<FastLockGuard>> {
        let req = LockRequest::new(resource, LockType::Exclusive, owner)
            .with_acquire_timeout(timeout)
            .with_ttl(ttl);
        self.acquire_guard(&req).await
    }

    /// Convenience: acquire shared lock as a guard
    pub async fn rlock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<FastLockGuard>> {
        let req = LockRequest::new(resource, LockType::Shared, owner)
            .with_acquire_timeout(timeout)
            .with_ttl(ttl);
        self.acquire_guard(&req).await
    }
}
