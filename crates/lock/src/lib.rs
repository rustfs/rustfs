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
    client::{LockClient, local::LocalClient, remote::RemoteClient},
    // Error types
    error::{LockError, Result},
    // Fast Lock System exports
    fast_lock::{
        BatchLockRequest, BatchLockResult, FastLockGuard, FastObjectLockManager, LockMode, LockResult, ObjectKey,
        ObjectLockRequest,
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

// ============================================================================
// Global FastLock Manager
// ============================================================================

// Global singleton FastLock manager shared across all lock implementations
use once_cell::sync::OnceCell;
use std::sync::Arc;

static GLOBAL_FAST_LOCK_MANAGER: OnceCell<Arc<fast_lock::FastObjectLockManager>> = OnceCell::new();

/// Get the global shared FastLock manager instance
pub fn get_global_fast_lock_manager() -> Arc<fast_lock::FastObjectLockManager> {
    GLOBAL_FAST_LOCK_MANAGER
        .get_or_init(|| Arc::new(fast_lock::FastObjectLockManager::new()))
        .clone()
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
