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

// Sub-modules
pub mod bandwidth_monitor;
pub mod io_profile;
pub mod io_schedule;
pub mod manager;
pub mod object_cache;
pub mod request_guard;

// ============================================
// Public API Re-exports
// ============================================

// I/O scheduling types
#[allow(unused_imports)]
pub use io_schedule::{
    IO_PRIORITY_METRICS, IoLoadLevel, IoPriority, IoPriorityMetrics, IoPriorityQueue, IoPriorityQueueConfig, IoQueueStatus,
    IoSchedulerConfig, IoStrategy, get_advanced_buffer_size, get_concurrency_aware_buffer_size,
};

// I/O profile types (storage media, access pattern detection)
#[allow(unused_imports)]
pub use io_profile::{AccessPattern, IoPatternDetector, StorageMedia, StorageProfile, detect_storage_media};

// Bandwidth monitoring types
#[allow(unused_imports)]
pub use bandwidth_monitor::{BandwidthMonitor, BandwidthSnapshot, BandwidthTier};

// Request tracking
pub use request_guard::GetObjectGuard;

// Cache types
#[allow(unused_imports)]
pub use object_cache::{CacheHealthStatus, CacheStats, CachedGetObject};

// Concurrency manager
pub use manager::ConcurrencyManager;

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
