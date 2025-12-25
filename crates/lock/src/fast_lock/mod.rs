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

//! Fast Object Lock System
//!
//! High-performance versioned object locking system optimized for object storage scenarios
//!
//! ## Core Features
//!
//! 1. **Sharded Architecture** - Hash-based object key sharding to avoid global lock contention
//! 2. **Version Awareness** - Support for multi-version object locking with fine-grained control
//! 3. **Fast Path** - Lock-free fast paths for common operations
//! 4. **Async Optimized** - True async locks that avoid thread blocking
//! 5. **Auto Cleanup** - Access-time based automatic lock reclamation

pub mod disabled_manager;
pub mod guard;
pub mod integration_example;
pub mod integration_test;
pub mod manager;
pub mod manager_trait;
pub mod metrics;
pub mod object_pool;
pub mod optimized_notify;
pub mod shard;
pub mod state;
pub mod types;

// Re-export main types
pub use disabled_manager::DisabledLockManager;
pub use guard::FastLockGuard;
pub use manager::FastObjectLockManager;
pub use manager_trait::LockManager;
pub use types::*;

/// Default shard count (must be power of 2)
pub const DEFAULT_SHARD_COUNT: usize = 1024;

/// Default lock timeout
pub const DEFAULT_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Default acquire timeout - increased for database workloads  
pub const DEFAULT_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Maximum acquire timeout for high-load scenarios
pub const MAX_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Lock cleanup interval
pub const CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
