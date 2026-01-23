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

//! Unified trait for lock managers (enabled and disabled)

use crate::fast_lock::{
    guard::FastLockGuard,
    metrics::AggregatedMetrics,
    types::{BatchLockRequest, BatchLockResult, LockResult, ObjectKey, ObjectLockInfo, ObjectLockRequest},
};
use std::sync::Arc;

/// Unified trait for lock managers
///
/// This trait allows transparent switching between enabled and disabled lock managers
/// based on environment variables.
#[async_trait::async_trait]
pub trait LockManager: Send + Sync {
    /// Acquire object lock
    async fn acquire_lock(&self, request: ObjectLockRequest) -> Result<FastLockGuard, LockResult>;

    /// Acquire shared (read) lock
    async fn acquire_read_lock(&self, key: ObjectKey, owner: impl Into<Arc<str>> + Send) -> Result<FastLockGuard, LockResult>;

    /// Acquire exclusive (write) lock
    async fn acquire_write_lock(&self, key: ObjectKey, owner: impl Into<Arc<str>> + Send) -> Result<FastLockGuard, LockResult>;

    /// Acquire multiple locks atomically
    async fn acquire_locks_batch(&self, batch_request: BatchLockRequest) -> BatchLockResult;

    /// Get lock information for monitoring
    fn get_lock_info(&self, key: &ObjectKey) -> Option<ObjectLockInfo>;

    /// Get aggregated metrics
    fn get_metrics(&self) -> AggregatedMetrics;

    /// Get total number of active locks across all shards
    fn total_lock_count(&self) -> usize;

    /// Get pool statistics from all shards
    fn get_pool_stats(&self) -> Vec<(u64, u64, u64, usize)>;

    /// Force cleanup of expired locks
    async fn cleanup_expired(&self) -> usize;

    /// Force cleanup with traditional strategy
    async fn cleanup_expired_traditional(&self) -> usize;

    /// Shutdown the lock manager and cleanup resources
    async fn shutdown(&self);

    /// Check if this manager is disabled
    fn is_disabled(&self) -> bool;
}
