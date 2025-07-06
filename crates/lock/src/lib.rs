#![allow(dead_code)]
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

// Distributed Layer Modules
pub mod distributed;

// Local Layer Modules
pub mod local;

// Core Modules
pub mod config;
pub mod error;
pub mod types;

// ============================================================================
// Public API Exports
// ============================================================================

// Re-export main types for easy access
pub use crate::{
    // Client interfaces
    client::{LockClient, local::LocalClient, remote::{RemoteClient, LockArgs}},
    // Configuration
    config::{DistributedLockConfig, LocalLockConfig, LockConfig, NetworkConfig},
    distributed::{DistributedLockEntry, DistributedLockManager, QuorumConfig},
    // Error types
    error::{LockError, Result},
    local::LocalLockMap,
    // Main components
    namespace::{NamespaceLock, NamespaceLockManager, NsLockMap},
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
// Global Lock Map
// ============================================================================

// Global singleton lock map shared across all lock implementations
use once_cell::sync::OnceCell;
use std::sync::Arc;

static GLOBAL_LOCK_MAP: OnceCell<Arc<local::LocalLockMap>> = OnceCell::new();

/// Get the global shared lock map instance
pub fn get_global_lock_map() -> Arc<local::LocalLockMap> {
    GLOBAL_LOCK_MAP.get_or_init(|| Arc::new(local::LocalLockMap::new())).clone()
}

// ============================================================================
// Feature Flags
// ============================================================================

#[cfg(feature = "distributed")]
pub mod distributed_features {
    // Distributed locking specific features
}

#[cfg(feature = "metrics")]
pub mod metrics {
    // Metrics collection features
}

#[cfg(feature = "tracing")]
pub mod tracing_features {
    // Tracing features
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Create a new namespace lock
pub fn create_namespace_lock(namespace: String, distributed: bool) -> NamespaceLock {
    if distributed {
        // Create a namespace lock that uses RPC to communicate with the server
        // This will use the NsLockMap with distributed mode enabled
        NamespaceLock::new(namespace, true)
    } else {
        NamespaceLock::new(namespace, false)
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Generate a new lock ID
pub fn generate_lock_id() -> LockId {
    LockId::new_deterministic("default")
}

/// Create a lock request with default settings
pub fn create_lock_request(resource: String, lock_type: LockType, owner: String) -> LockRequest {
    LockRequest::new(resource, lock_type, owner)
}

/// Create an exclusive lock request
pub fn create_exclusive_lock_request(resource: String, owner: String) -> LockRequest {
    create_lock_request(resource, LockType::Exclusive, owner)
}

/// Create a shared lock request
pub fn create_shared_lock_request(resource: String, owner: String) -> LockRequest {
    create_lock_request(resource, LockType::Shared, owner)
}
