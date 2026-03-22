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
//! This module provides intelligent concurrency management to prevent performance
//! degradation when multiple concurrent GetObject requests are processed.
//!
//! # Key Features
//!
//! - **Adaptive Buffer Sizing**: Dynamically adjusts buffer sizes based on concurrent load
//! - **Moka Cache Integration**: Lock-free hot object caching with automatic TTL/TTI expiration
//! - **I/O Rate Limiting**: Semaphore-based disk read throttling
//! - **Priority-Based Scheduling**: Small requests get higher priority
//! - **Comprehensive Metrics**: Prometheus-compatible metrics for monitoring
//!
//! # Module Structure
//!
//! - `io_schedule`: I/O scheduling types and adaptive strategy calculation
//! - `request_guard`: RAII guard for tracking concurrent requests
//! - `object_cache`: Hot object cache with Moka integration
//! - `manager`: Central concurrency manager

// Sub-modules
pub mod io_schedule;
pub mod manager;
pub mod object_cache;
pub mod request_guard;

// ============================================
// Public API Re-exports
// ============================================

// I/O scheduling types
pub use io_schedule::{IoLoadLevel, IoPriority, IoQueueStatus, IoSchedulerConfig, IoStrategy, get_concurrency_aware_buffer_size};

// Request tracking
pub use request_guard::GetObjectGuard;

// Cache types
pub use object_cache::{CacheStats, CachedGetObject};

// Concurrency manager
pub use manager::ConcurrencyManager;

// ============================================
// Helper Functions
// ============================================

/// Get the global concurrency manager instance.
pub fn get_concurrency_manager() -> &'static ConcurrencyManager {
    ConcurrencyManager::global()
}
