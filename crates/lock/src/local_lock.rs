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
    fast_lock::{FastLockGuard, LockManager, LockMode, LockResult, ObjectLockRequest},
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
    namespace: Arc<str>,
}

impl LocalLock {
    /// Create new local lock
    pub fn new(namespace: String, manager: Arc<GlobalLockManager>) -> Self {
        Self::new_shared(namespace.into(), manager)
    }

    /// Create a local lock that shares an existing namespace allocation.
    pub(crate) fn new_shared(namespace: Arc<str>, manager: Arc<GlobalLockManager>) -> Self {
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

    /// Acquire exclusive locks for many independent resources in one batched
    /// pass; see [`crate::distributed_lock::DistributedLock::acquire_guard_batch`].
    /// The manager's batch acquire sorts every request into one global
    /// (shard, key) order, so concurrent batches cannot deadlock. Returns one
    /// guard or error per resource, in `resources` order.
    pub async fn acquire_guard_batch(
        &self,
        resources: &[ObjectKey],
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Vec<Result<FastLockGuard>> {
        if resources.is_empty() {
            return Vec::new();
        }
        let batch = crate::fast_lock::BatchLockRequest {
            requests: resources
                .iter()
                .map(|resource| {
                    ObjectLockRequest::new_write(resource.clone(), owner)
                        .with_acquire_timeout(timeout)
                        .with_lock_timeout(ttl)
                })
                .collect(),
            owner: owner.into(),
            all_or_nothing: false,
        };
        let outcome = self.manager.acquire_locks_batch(batch).await;
        let mut guards = outcome
            .successful_locks
            .into_iter()
            .zip(outcome.guards)
            .collect::<std::collections::HashMap<ObjectKey, FastLockGuard>>();
        let mut failures = outcome
            .failed_locks
            .into_iter()
            .collect::<std::collections::HashMap<ObjectKey, LockResult>>();
        resources
            .iter()
            .map(|resource| {
                if let Some(guard) = guards.remove(resource) {
                    return Ok(guard);
                }
                match failures.remove(resource) {
                    Some(LockResult::Timeout) => Err(crate::error::LockError::timeout(resource.to_string(), timeout)),
                    Some(LockResult::Conflict { current_owner, .. }) => Err(crate::error::LockError::AlreadyLocked {
                        resource: resource.to_string(),
                        owner: current_owner.to_string(),
                    }),
                    Some(LockResult::Acquired) | None => {
                        Err(crate::error::LockError::internal("batch lock acquire returned no outcome for key"))
                    }
                }
            })
            .collect()
    }
}
