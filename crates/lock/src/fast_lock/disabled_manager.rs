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

//! Disabled lock manager that bypasses all locking operations
//! Used when RUSTFS_ENABLE_LOCKS environment variable is set to false

use std::sync::Arc;

use crate::fast_lock::{
    guard::FastLockGuard,
    manager_trait::LockManager,
    metrics::AggregatedMetrics,
    types::{BatchLockRequest, BatchLockResult, LockConfig, LockResult, ObjectKey, ObjectLockInfo, ObjectLockRequest},
};

/// Disabled lock manager that always returns success without actual locking
///
/// This manager is used when locks are disabled via environment variables.
/// All lock operations immediately return success, effectively bypassing
/// the locking mechanism entirely.
#[derive(Debug)]
pub struct DisabledLockManager {
    _config: LockConfig,
}

impl DisabledLockManager {
    /// Create new disabled lock manager
    pub fn new() -> Self {
        Self::with_config(LockConfig::default())
    }

    /// Create new disabled lock manager with custom config
    pub fn with_config(config: LockConfig) -> Self {
        Self { _config: config }
    }

    /// Always succeeds - returns a no-op guard
    pub async fn acquire_lock(&self, request: ObjectLockRequest) -> Result<FastLockGuard, LockResult> {
        Ok(FastLockGuard::new_disabled(request.key, request.mode, request.owner))
    }

    /// Always succeeds - returns a no-op guard
    pub async fn acquire_read_lock(&self, key: ObjectKey, owner: impl Into<Arc<str>>) -> Result<FastLockGuard, LockResult> {
        let request = ObjectLockRequest::new_read(key, owner);
        self.acquire_lock(request).await
    }

    /// Always succeeds - returns a no-op guard
    pub async fn acquire_read_lock_versioned(
        &self,
        key: ObjectKey,
        owner: impl Into<Arc<str>>,
    ) -> Result<FastLockGuard, LockResult> {
        let request = ObjectLockRequest::new_write(key, owner);
        self.acquire_lock(request).await
    }

    /// Always succeeds - returns a no-op guard
    /// Always succeeds - all locks acquired
    pub async fn acquire_locks_batch(&self, batch_request: BatchLockRequest) -> BatchLockResult {
        let successful_locks: Vec<ObjectKey> = batch_request.requests.iter().map(|req| req.key.clone()).collect();
        let guards = batch_request
            .requests
            .into_iter()
            .map(|req| FastLockGuard::new_disabled(req.key, req.mode, req.owner))
            .collect();

        BatchLockResult {
            successful_locks,
            failed_locks: Vec::new(),
            all_acquired: true,
            guards,
        }
    }

    /// Always returns None - no locks to query
    pub fn get_lock_info(&self, _key: &ObjectKey) -> Option<ObjectLockInfo> {
        None
    }

    /// Returns empty metrics
    pub fn get_metrics(&self) -> AggregatedMetrics {
        AggregatedMetrics::empty()
    }

    /// Always returns 0 - no locks exist
    pub fn total_lock_count(&self) -> usize {
        0
    }

    /// Returns empty pool stats
    pub fn get_pool_stats(&self) -> Vec<(u64, u64, u64, usize)> {
        Vec::new()
    }

    /// No-op cleanup - nothing to clean
    pub async fn cleanup_expired(&self) -> usize {
        0
    }

    /// No-op cleanup - nothing to clean
    pub async fn cleanup_expired_traditional(&self) -> usize {
        0
    }

    /// No-op shutdown
    pub async fn shutdown(&self) {
        // Nothing to shutdown
    }
}

impl Default for DisabledLockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl LockManager for DisabledLockManager {
    async fn acquire_lock(&self, request: ObjectLockRequest) -> Result<FastLockGuard, LockResult> {
        self.acquire_lock(request).await
    }

    async fn acquire_read_lock(&self, key: ObjectKey, owner: impl Into<Arc<str>> + Send) -> Result<FastLockGuard, LockResult> {
        self.acquire_read_lock(key, owner).await
    }

    async fn acquire_write_lock(&self, key: ObjectKey, owner: impl Into<Arc<str>> + Send) -> Result<FastLockGuard, LockResult> {
        self.acquire_write_lock(key, owner).await
    }

    async fn acquire_locks_batch(&self, batch_request: BatchLockRequest) -> BatchLockResult {
        self.acquire_locks_batch(batch_request).await
    }

    fn get_lock_info(&self, key: &ObjectKey) -> Option<ObjectLockInfo> {
        self.get_lock_info(key)
    }

    fn get_metrics(&self) -> AggregatedMetrics {
        self.get_metrics()
    }

    fn total_lock_count(&self) -> usize {
        self.total_lock_count()
    }

    fn get_pool_stats(&self) -> Vec<(u64, u64, u64, usize)> {
        self.get_pool_stats()
    }

    async fn cleanup_expired(&self) -> usize {
        self.cleanup_expired().await
    }

    async fn cleanup_expired_traditional(&self) -> usize {
        self.cleanup_expired_traditional().await
    }

    async fn shutdown(&self) {
        self.shutdown().await
    }

    fn is_disabled(&self) -> bool {
        true
    }
}
