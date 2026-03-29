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

//! Concurrency optimization module for high-performance object retrieval.
//!
//! This module provides concurrency management, I/O scheduling, and object caching
//! for high-performance object retrieval operations.
//!
//! # Architecture
//!
//! The module is organized into several components:
//! - **I/O Scheduling**: Adaptive buffer sizing and load management
//! - **Object Caching**: Tiered L1/L2 cache for frequently accessed objects
//! - **Concurrency Management**: Coordination of concurrent GetObject requests
//! - **Request Tracking**: RAII guards for request lifecycle management
//!
//! # Migration Note
//!
//! Core algorithms have been migrated to `rustfs-io-core` and metrics to
//! `rustfs-io-metrics`. This module maintains API compatibility while
//! delegating to the new implementations.

// Sub-modules
// pub mod bandwidth_monitor; // Migrated to rustfs-io-metrics
// pub mod global_metrics; // Migrated to rustfs-io-metrics
// pub mod io_profile; // Migrated to rustfs-io-core
pub mod io_schedule;
pub mod manager;
pub mod object_cache;
pub mod request_guard;

// ============================================
// Public API Re-exports
// ============================================

// I/O scheduling types (from io_schedule.rs for backward compatibility)
#[allow(unused_imports)]
pub use io_schedule::{
    IO_PRIORITY_METRICS, IoLoadLevel, IoPriority, IoPriorityMetrics, IoPriorityQueue, IoPriorityQueueConfig, IoQueueStatus,
    IoSchedulerConfig, IoStrategy, get_advanced_buffer_size, get_buffer_size_opt_in, get_concurrency_aware_buffer_size,
};

// Request tracking
pub use request_guard::GetObjectGuard;

// Cache types
#[allow(unused_imports)]
pub use object_cache::{CacheHealthStatus, CacheStats, CachedGetObject};

// Concurrency manager
pub use manager::ConcurrencyManager;

// ============================================
// New Module Re-exports (for gradual migration)
// ============================================

// Re-export types from rustfs-io-core for convenience
pub use rustfs_io_core::{
    // Backpressure types
    BackpressureMonitor,
    // Deadlock detection types
    DeadlockDetector,
    // Scheduler types
    IoScheduler,
    // Lock optimization types
    LockOptimizer,
};

// Re-export types from rustfs-io-metrics for convenience

// ============================================
// Helper Functions
// ============================================

/// Get the global concurrency manager instance.
pub fn get_concurrency_manager() -> &'static ConcurrencyManager {
    ConcurrencyManager::global()
}

/// Reset the active get requests counter (for testing).
#[allow(dead_code)]
pub fn reset_active_get_requests() {
    io_schedule::ACTIVE_GET_REQUESTS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Create a new I/O scheduler with default configuration.
#[allow(dead_code)]
pub fn create_io_scheduler() -> IoScheduler {
    IoScheduler::with_defaults()
}

/// Create a new backpressure monitor with default configuration.
#[allow(dead_code)]
pub fn create_backpressure_monitor() -> BackpressureMonitor {
    BackpressureMonitor::with_defaults()
}

/// Create a new deadlock detector with default configuration.
#[allow(dead_code)]
pub fn create_deadlock_detector() -> DeadlockDetector {
    DeadlockDetector::with_defaults()
}

/// Create a new lock optimizer with default configuration.
#[allow(dead_code)]
pub fn create_lock_optimizer() -> LockOptimizer {
    LockOptimizer::with_defaults()
}
