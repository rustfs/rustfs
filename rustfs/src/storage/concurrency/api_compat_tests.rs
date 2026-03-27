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

//! API compatibility tests for the concurrency module migration.
//!
//! These tests verify that the migrated types and functions maintain
//! backward compatibility with the original API.

use std::time::Duration;

// Test that all original types are still accessible
#[test]
fn test_io_schedule_types_accessible() {
    // Original types from io_schedule.rs
    use crate::storage::concurrency::{
        IoLoadLevel, IoPriority, IoPriorityMetrics, IoPriorityQueue, IoPriorityQueueConfig, IoQueueStatus, IoSchedulerConfig,
        IoStrategy,
    };

    // Verify we can create instances
    let _level = IoLoadLevel::Low;
    let _priority = IoPriority::High;
    let _config = IoSchedulerConfig::default();
    let _queue_config = IoPriorityQueueConfig::default();
}

#[test]
fn test_cache_types_accessible() {
    // Original types from object_cache.rs
    use crate::storage::concurrency::{CacheHealthStatus, CacheStats};

    // Verify we can create instances (CacheStats is a data struct)
    let _stats = CacheStats {
        size: 0,
        entries: 0,
        max_size: 0,
        max_object_size: 0,
        hit_count: 0,
        miss_count: 0,
        avg_age_secs: 0.0,
        hit_rate: 0.0,
        eviction_count: 0,
        eviction_rate: 0.0,
        memory_usage: 0,
        memory_usage_ratio: 0.0,
        top_keys: Vec::new(),
        efficiency_score: 0,
    };
}

#[test]
fn test_manager_accessible() {
    // Original ConcurrencyManager
    use crate::storage::concurrency::ConcurrencyManager;

    // Verify we can get the global instance
    let _manager = crate::storage::concurrency::get_concurrency_manager();
}

#[test]
fn test_buffer_size_functions_accessible() {
    // Original buffer size functions
    use crate::storage::concurrency::{get_advanced_buffer_size, get_buffer_size_opt_in, get_concurrency_aware_buffer_size};

    // Verify functions work
    let size1 = get_concurrency_aware_buffer_size(1024 * 1024, 64 * 1024);
    let size2 = get_advanced_buffer_size(1024 * 1024, 64 * 1024, true);
    let size3 = get_buffer_size_opt_in(1024 * 1024);

    assert!(size1 > 0);
    assert!(size2 > 0);
    assert!(size3 > 0);
}

// Test that new types from rustfs-io-core are accessible
#[test]
fn test_new_io_core_types_accessible() {
    use crate::storage::concurrency::{
        BackpressureConfig, BackpressureMonitor, BackpressureState, BandwidthTier, DeadlockDetector, DeadlockDetectorConfig,
        IoLoadMetrics, IoScheduler, IoSchedulingContext, KI_B, LockInfo, LockOptimizeConfig, LockOptimizer, LockStats, LockType,
        MI_B, OperationProgress, RequestTimeoutWrapper, TimeoutConfig, TimeoutError, TimeoutStats, WaitGraphEdge,
        calculate_optimal_buffer_size, get_buffer_size_for_media,
    };

    // Verify we can create instances
    let _scheduler = IoScheduler::with_defaults();
    let _monitor = BackpressureMonitor::with_defaults();
    let _detector = DeadlockDetector::with_defaults();
    let _optimizer = LockOptimizer::with_defaults();

    // Verify constants
    assert_eq!(KI_B, 1024);
    assert_eq!(MI_B, 1024 * 1024);
}

// Test that new types from rustfs-io-metrics are accessible
#[test]
fn test_new_io_metrics_types_accessible() {
    use crate::storage::concurrency::{
        AccessRecord, AdaptiveTTL, AdaptiveTTLStats, BackpressureSettings, CacheConfig, CacheConfigError, CacheSettings,
        DeadlockDetectionSettings, IoConfig, IoSchedulerSettings, IoSchedulerStats, TimeoutSettings,
    };

    // Verify we can create instances
    let _config = CacheConfig::default();
    let _ttl = AdaptiveTTL::default();
    let _settings = IoConfig::new();
}

// Test helper functions
#[test]
fn test_helper_functions() {
    use crate::storage::concurrency::{
        create_backpressure_monitor, create_deadlock_detector, create_io_scheduler, create_lock_optimizer,
    };

    let _scheduler = create_io_scheduler();
    let _monitor = create_backpressure_monitor();
    let _detector = create_deadlock_detector();
    let _optimizer = create_lock_optimizer();
}

// Test that buffer size calculation works correctly
#[test]
fn test_buffer_size_calculation() {
    use crate::storage::concurrency::{calculate_optimal_buffer_size, get_buffer_size_for_media};
    use rustfs_io_core::IoLoadLevel;
    use rustfs_io_core::io_profile::StorageMedia; // Use the correct IoLoadLevel from io-core

    // Test with different parameters
    let size1 = calculate_optimal_buffer_size(
        10 * 1024 * 1024, // 10 MB file
        64 * 1024,        // 64 KB base
        true,             // sequential
        4,                // 4 concurrent requests
        StorageMedia::Ssd,
        IoLoadLevel::Low,
    );

    let size2 = get_buffer_size_for_media(64 * 1024, StorageMedia::Hdd);
    let size3 = get_buffer_size_for_media(64 * 1024, StorageMedia::Ssd);

    assert!(size1 > 0);
    assert!(size2 > 0);
    assert!(size3 > 0);
    // SSD should allow larger buffers
    assert!(size3 >= size2);
}

// Test backpressure monitor integration
#[test]
fn test_backpressure_integration() {
    use crate::storage::concurrency::{BackpressureMonitor, BackpressureState};

    let monitor = BackpressureMonitor::with_defaults();

    // Initial state should be Normal
    let state = monitor.state();
    assert!(matches!(state, BackpressureState::Normal));
}

// Test deadlock detector integration
#[test]
fn test_deadlock_detector_integration() {
    use crate::storage::concurrency::{DeadlockDetector, LockType};

    let detector = DeadlockDetector::with_defaults();

    // Register some locks (returns lock_id)
    let lock1_id = detector.register_lock(LockType::RwLockRead);
    let lock2_id = detector.register_lock(LockType::RwLockWrite);

    // Check for deadlocks (should be none)
    let deadlocks = detector.detect_deadlock();
    assert!(deadlocks.is_none());

    // Cleanup
    detector.unregister_lock(lock1_id);
    detector.unregister_lock(lock2_id);
}

// Test lock optimizer integration
#[test]
fn test_lock_optimizer_integration() {
    use crate::storage::concurrency::LockOptimizer;

    let optimizer = LockOptimizer::with_defaults();

    // Get stats
    let stats = optimizer.stats();
    assert_eq!(stats.locks_acquired.load(std::sync::atomic::Ordering::Relaxed), 0);
}

// Test timeout wrapper integration
#[test]
fn test_timeout_wrapper_integration() {
    use crate::storage::concurrency::{RequestTimeoutWrapper, TimeoutConfig};

    let config = TimeoutConfig {
        base_timeout: Duration::from_secs(5),
        max_timeout: Duration::from_secs(300),
        ..Default::default()
    };

    let wrapper = RequestTimeoutWrapper::new(config);
    assert_eq!(wrapper.config().base_timeout, Duration::from_secs(5));
}

// Test unified configuration
#[test]
fn test_unified_config() {
    use crate::storage::concurrency::{CacheSettings, IoConfig, IoSchedulerSettings};

    let config = IoConfig::new()
        .with_cache(CacheSettings::new().with_max_capacity(5000))
        .with_scheduler(IoSchedulerSettings::new().with_max_concurrent_reads(64));

    assert_eq!(config.cache.max_capacity, 5000);
    assert_eq!(config.scheduler.max_concurrent_reads, 64);
}

// Test access tracking
#[test]
fn test_access_tracking() {
    use crate::storage::concurrency::{AccessRecord, MetricsAccessTracker};
    use std::time::Duration;

    let mut tracker = MetricsAccessTracker::new(100, Duration::from_secs(60));

    // Record some accesses
    tracker.record_access("key1", 100);
    tracker.record_access("key1", 100);
    tracker.record_access("key2", 200);

    // Check counts
    assert_eq!(tracker.get_access_count("key1"), 2);
    assert_eq!(tracker.get_access_count("key2"), 1);
    assert_eq!(tracker.get_access_count("key3"), 0);

    // Check hot/cold detection
    assert!(tracker.is_hot("key1", 1));
    assert!(!tracker.is_hot("key2", 2));
}
