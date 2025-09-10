// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0

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

pub mod guard;
pub mod integration_example;
pub mod manager;
pub mod metrics;
pub mod shard;
pub mod state;
pub mod types;

// #[cfg(test)]
// pub mod benchmarks; // Temporarily disabled due to compilation issues

// Re-export main types
pub use guard::FastLockGuard;
pub use manager::FastObjectLockManager;
pub use types::*;

/// Default shard count (must be power of 2)
pub const DEFAULT_SHARD_COUNT: usize = 1024;

/// Default lock timeout
pub const DEFAULT_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Default acquire timeout  
pub const DEFAULT_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Lock cleanup interval
pub const CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
