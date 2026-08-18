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
//! This module provides concurrency management and I/O scheduling
//! for high-performance object retrieval operations.
//!
//! # Architecture
//!
//! The module is organized into several components:
//! - **I/O Scheduling**: Adaptive buffer sizing and load management
//! - **Concurrency Management**: Coordination of concurrent GetObject requests
//! - **Request Tracking**: RAII guards for request lifecycle management
//!
//! # Relationship to the shared crates
//!
//! The scheduling algorithm lives in [`io_schedule`], not in `rustfs-io-core`:
//! this module does not delegate to it. `rustfs-io-core` owns the shared
//! config shapes and the `io_profile` storage-media model that [`io_schedule`]
//! consumes, and `rustfs-io-metrics` owns bandwidth sampling and metric
//! recording.

pub mod io_schedule;
pub mod manager;
pub mod request_guard;

// ============================================
// Public API Re-exports
// ============================================

// I/O scheduling types (from io_schedule.rs for backward compatibility)
#[allow(unused_imports)]
pub use io_schedule::{
    IoLoadLevel, IoPriority, IoPriorityQueue, IoPriorityQueueConfig, IoQueueStatus, IoSchedulerConfig, IoStrategy,
    get_advanced_buffer_size, get_concurrency_aware_buffer_size, get_put_concurrency_aware_buffer_size,
};

// Request tracking
pub use request_guard::{GetObjectGuard, PutObjectGuard};

// Concurrency manager
pub use manager::{ConcurrencyManager, DiskReadAdmission, PutObjectAdmission};

// ============================================
// Helper Functions
// ============================================

/// Get the global concurrency manager instance.
pub fn get_concurrency_manager() -> &'static ConcurrencyManager {
    ConcurrencyManager::global()
}

/// Reset the active put requests counter (for testing).
#[cfg(test)]
pub fn reset_active_put_requests() {
    io_schedule::ACTIVE_PUT_REQUESTS.store(0, std::sync::atomic::Ordering::Relaxed);
}
