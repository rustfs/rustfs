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

// ============================================================================
// Core Module Declarations
// ============================================================================

// Application Layer Modules
pub mod namespace;

// Abstraction Layer Modules
pub mod client;

// Fast Lock System (New High-Performance Implementation)
pub mod fast_lock;

// Core Modules
pub mod error;
pub mod guard;
pub mod types;

// ============================================================================
// Public API Exports
// ============================================================================

// Re-export main types for easy access
pub use crate::{
    // Client interfaces
    client::{LockClient, local::LocalClient},
    // Error types
    error::{LockError, Result},
    // Fast Lock System exports
    fast_lock::{
        BatchLockRequest, BatchLockResult, DisabledLockManager, FastLockGuard, FastObjectLockManager, LockManager, LockMode,
        LockResult, ObjectKey, ObjectLockInfo, ObjectLockRequest, metrics::AggregatedMetrics,
    },
    guard::LockGuard,
    // Main components
    namespace::{NamespaceLock, NamespaceLockManager},
    // Core types
    types::{
        HealthInfo, HealthStatus, LockId, LockInfo, LockMetadata, LockPriority, LockRequest, LockResponse, LockStats, LockStatus,
        LockType,
    },
};

// ============================================================================
// Version Information
// ============================================================================

/// Current version of the lock crate
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build timestamp
pub const BUILD_TIMESTAMP: &str = "unknown";

/// Maximum number of items in delete list
pub const MAX_DELETE_LIST: usize = 1000;

/// Default setting for RUSTFS_ENABLE_LOCKS environment variable
const DEFAULT_RUSTFS_LOCKS_ENABLED: bool = true;

// ============================================================================
// Global FastLock Manager
// ============================================================================

// Global singleton FastLock manager shared across all lock implementations
use crate::fast_lock::{DEFAULT_RUSTFS_ACQUIRE_TIMEOUT, DEFAULT_RUSTFS_MAX_ACQUIRE_TIMEOUT};
use std::sync::Arc;
use std::sync::OnceLock;

/// Enum wrapper for different lock manager implementations
pub enum GlobalLockManager {
    Enabled(Arc<FastObjectLockManager>),
    Disabled(DisabledLockManager),
}

impl Default for GlobalLockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalLockManager {
    /// Create a lock manager based on environment variable configuration
    pub fn new() -> Self {
        // Check RUSTFS_ENABLE_LOCKS environment variable
        let locks_enabled = rustfs_utils::get_env_bool("RUSTFS_ENABLE_LOCKS", DEFAULT_RUSTFS_LOCKS_ENABLED);
        if !locks_enabled {
            tracing::info!("Lock system disabled via RUSTFS_ENABLE_LOCKS environment variable");
            return Self::Disabled(DisabledLockManager::new());
        }
        tracing::info!("Lock system enabled");

        // Read lock acquire timeout from environment variable
        let mut acquire_secs = rustfs_utils::get_env_u64("RUSTFS_LOCK_ACQUIRE_TIMEOUT", DEFAULT_RUSTFS_ACQUIRE_TIMEOUT);

        // Enforce minimum of 1 second
        if acquire_secs == 0 {
            tracing::warn!("Requested lock acquire timeout {}s is below minimum 1s, using minimum", acquire_secs);
            acquire_secs = 1;
        }

        if acquire_secs > DEFAULT_RUSTFS_MAX_ACQUIRE_TIMEOUT {
            tracing::warn!(
                "Requested lock acquire timeout {}s exceeds maximum {}, using maximum",
                acquire_secs,
                DEFAULT_RUSTFS_MAX_ACQUIRE_TIMEOUT
            );
            acquire_secs = DEFAULT_RUSTFS_MAX_ACQUIRE_TIMEOUT;
        }
        let acquire_timeout = std::time::Duration::from_secs(acquire_secs);
        tracing::info!("Lock system enabled with acquire timeout: {}s", acquire_timeout.as_secs());

        // Create lock manager with custom configuration
        let config = fast_lock::LockConfig {
            default_acquire_timeout: acquire_timeout,
            ..Default::default()
        };

        Self::Enabled(Arc::new(FastObjectLockManager::with_config(config)))
    }

    /// Check if the lock manager is disabled
    pub fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled(_))
    }

    /// Get the FastObjectLockManager if enabled, otherwise returns None
    pub fn as_fast_lock_manager(&self) -> Option<Arc<FastObjectLockManager>> {
        match self {
            Self::Enabled(manager) => Some(manager.clone()),
            Self::Disabled(_) => None,
        }
    }
}

#[async_trait::async_trait]
impl LockManager for GlobalLockManager {
    async fn acquire_lock(&self, request: ObjectLockRequest) -> std::result::Result<FastLockGuard, LockResult> {
        match self {
            Self::Enabled(manager) => manager.acquire_lock(request).await,
            Self::Disabled(manager) => manager.acquire_lock(request).await,
        }
    }

    async fn acquire_read_lock(
        &self,
        bucket: impl Into<Arc<str>> + Send,
        object: impl Into<Arc<str>> + Send,
        owner: impl Into<Arc<str>> + Send,
    ) -> std::result::Result<FastLockGuard, LockResult> {
        match self {
            Self::Enabled(manager) => manager.acquire_read_lock(bucket, object, owner).await,
            Self::Disabled(manager) => manager.acquire_read_lock(bucket, object, owner).await,
        }
    }

    async fn acquire_read_lock_versioned(
        &self,
        bucket: impl Into<Arc<str>> + Send,
        object: impl Into<Arc<str>> + Send,
        version: impl Into<Arc<str>> + Send,
        owner: impl Into<Arc<str>> + Send,
    ) -> std::result::Result<FastLockGuard, LockResult> {
        match self {
            Self::Enabled(manager) => manager.acquire_read_lock_versioned(bucket, object, version, owner).await,
            Self::Disabled(manager) => manager.acquire_read_lock_versioned(bucket, object, version, owner).await,
        }
    }

    async fn acquire_write_lock(
        &self,
        bucket: impl Into<Arc<str>> + Send,
        object: impl Into<Arc<str>> + Send,
        owner: impl Into<Arc<str>> + Send,
    ) -> std::result::Result<FastLockGuard, LockResult> {
        match self {
            Self::Enabled(manager) => manager.acquire_write_lock(bucket, object, owner).await,
            Self::Disabled(manager) => manager.acquire_write_lock(bucket, object, owner).await,
        }
    }

    async fn acquire_write_lock_versioned(
        &self,
        bucket: impl Into<Arc<str>> + Send,
        object: impl Into<Arc<str>> + Send,
        version: impl Into<Arc<str>> + Send,
        owner: impl Into<Arc<str>> + Send,
    ) -> std::result::Result<FastLockGuard, LockResult> {
        match self {
            Self::Enabled(manager) => manager.acquire_write_lock_versioned(bucket, object, version, owner).await,
            Self::Disabled(manager) => manager.acquire_write_lock_versioned(bucket, object, version, owner).await,
        }
    }

    async fn acquire_locks_batch(&self, batch_request: BatchLockRequest) -> BatchLockResult {
        match self {
            Self::Enabled(manager) => manager.acquire_locks_batch(batch_request).await,
            Self::Disabled(manager) => manager.acquire_locks_batch(batch_request).await,
        }
    }

    fn get_lock_info(&self, key: &ObjectKey) -> Option<ObjectLockInfo> {
        match self {
            Self::Enabled(manager) => manager.get_lock_info(key),
            Self::Disabled(manager) => manager.get_lock_info(key),
        }
    }

    fn get_metrics(&self) -> AggregatedMetrics {
        match self {
            Self::Enabled(manager) => manager.get_metrics(),
            Self::Disabled(manager) => manager.get_metrics(),
        }
    }

    fn total_lock_count(&self) -> usize {
        match self {
            Self::Enabled(manager) => manager.total_lock_count(),
            Self::Disabled(manager) => manager.total_lock_count(),
        }
    }

    fn get_pool_stats(&self) -> Vec<(u64, u64, u64, usize)> {
        match self {
            Self::Enabled(manager) => manager.get_pool_stats(),
            Self::Disabled(manager) => manager.get_pool_stats(),
        }
    }

    async fn cleanup_expired(&self) -> usize {
        match self {
            Self::Enabled(manager) => manager.cleanup_expired().await,
            Self::Disabled(manager) => manager.cleanup_expired().await,
        }
    }

    async fn cleanup_expired_traditional(&self) -> usize {
        match self {
            Self::Enabled(manager) => manager.cleanup_expired_traditional().await,
            Self::Disabled(manager) => manager.cleanup_expired_traditional().await,
        }
    }

    async fn shutdown(&self) {
        match self {
            Self::Enabled(manager) => manager.shutdown().await,
            Self::Disabled(manager) => manager.shutdown().await,
        }
    }

    fn is_disabled(&self) -> bool {
        match self {
            Self::Enabled(manager) => manager.is_disabled(),
            Self::Disabled(manager) => manager.is_disabled(),
        }
    }
}

static GLOBAL_LOCK_MANAGER: OnceLock<Arc<GlobalLockManager>> = OnceLock::new();

/// Get the global shared lock manager instance
///
/// Returns either FastObjectLockManager or DisabledLockManager based on
/// the RUSTFS_ENABLE_LOCKS environment variable.
pub fn get_global_lock_manager() -> Arc<GlobalLockManager> {
    GLOBAL_LOCK_MANAGER.get_or_init(|| Arc::new(GlobalLockManager::new())).clone()
}

/// Get the global shared FastLock manager instance (legacy)
///
/// This function is deprecated. Use get_global_lock_manager() instead.
/// Returns FastObjectLockManager when locks are enabled, or panics when disabled.
#[deprecated(note = "Use get_global_lock_manager() instead")]
pub fn get_global_fast_lock_manager() -> Arc<FastObjectLockManager> {
    let manager = get_global_lock_manager();
    manager.as_fast_lock_manager().unwrap_or_else(|| {
        panic!("Cannot get FastObjectLockManager when locks are disabled. Use get_global_lock_manager() instead.");
    })
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Create a new namespace lock
pub fn create_namespace_lock(namespace: String, _distributed: bool) -> NamespaceLock {
    // The distributed behavior is now determined by the type of clients added to the NamespaceLock
    // This function just creates an empty NamespaceLock
    NamespaceLock::new(namespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_global_lock_manager_basic() {
        let manager = get_global_lock_manager();

        // Should be able to acquire locks
        let guard = manager.acquire_read_lock("bucket", "object", "owner").await;
        assert!(guard.is_ok());

        // Test metrics
        let _metrics = manager.get_metrics();
        // Even if locks are disabled, metrics should be available (empty or real)
        // shard_count is usize so always >= 0
    }

    #[tokio::test]
    async fn test_disabled_manager_direct() {
        let manager = DisabledLockManager::new();

        // All operations should succeed immediately
        let guard = manager.acquire_read_lock("bucket", "object", "owner").await;
        assert!(guard.is_ok());
        assert!(guard.unwrap().is_disabled());

        // Metrics should be empty
        let metrics = manager.get_metrics();
        assert!(metrics.is_empty());
        assert_eq!(manager.total_lock_count(), 0);
    }

    #[tokio::test]
    async fn test_enabled_manager_direct() {
        let manager = FastObjectLockManager::new();

        // Operations should work normally
        let guard = manager.acquire_read_lock("bucket", "object", "owner").await;
        assert!(guard.is_ok());
        assert!(!guard.unwrap().is_disabled());

        // Should have real metrics
        let _metrics = manager.get_metrics();
        // Note: total_lock_count might be > 0 due to previous lock acquisition
    }

    #[tokio::test]
    async fn test_global_manager_enum_wrapper() {
        // Test the GlobalLockManager enum directly
        let enabled_manager = GlobalLockManager::Enabled(Arc::new(FastObjectLockManager::new()));
        let disabled_manager = GlobalLockManager::Disabled(DisabledLockManager::new());

        assert!(!enabled_manager.is_disabled());
        assert!(disabled_manager.is_disabled());

        // Test trait methods work for both
        let enabled_guard = enabled_manager.acquire_read_lock("bucket", "obj", "owner").await;
        let disabled_guard = disabled_manager.acquire_read_lock("bucket", "obj", "owner").await;

        assert!(enabled_guard.is_ok());
        assert!(disabled_guard.is_ok());

        assert!(!enabled_guard.unwrap().is_disabled());
        assert!(disabled_guard.unwrap().is_disabled());
    }

    #[tokio::test]
    async fn test_batch_operations_work() {
        let manager = get_global_lock_manager();

        let batch = BatchLockRequest::new("owner")
            .add_read_lock("bucket", "obj1")
            .add_write_lock("bucket", "obj2");

        let result = manager.acquire_locks_batch(batch).await;

        // Should succeed regardless of whether locks are enabled or disabled
        assert!(result.all_acquired);
        assert_eq!(result.successful_locks.len(), 2);
        assert!(result.failed_locks.is_empty());
    }
}
