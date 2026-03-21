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

//! Integration tests for concurrent request fix.
//!
//! These tests verify that the timeout, backpressure, and deadlock detection
//! mechanisms work correctly under high concurrency scenarios.

#[cfg(test)]
mod tests {
    use crate::storage::backpressure::{BackpressureConfig, BackpressureMonitor, BackpressureState};
    use crate::storage::concurrency::{IoLoadLevel, IoPriority};
    use crate::storage::deadlock_detector::{
        DeadlockDetector, DeadlockDetectorConfig, LockInfo, LockType, RequestResourceTracker,
    };
    use crate::storage::lock_optimizer::{LockOptimizeConfig, LockOptimizer, LockStats};
    use crate::storage::timeout_wrapper::{RequestTimeoutWrapper, TimedGetObjectResult, TimeoutConfig};
    use std::time::Duration;

    // ============================================
    // Timeout Wrapper Tests
    // ============================================

    #[tokio::test]
    async fn test_timeout_wrapper_completes_within_timeout() {
        let config = TimeoutConfig {
            get_object_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let wrapper = RequestTimeoutWrapper::new(config);

        let result = wrapper
            .execute_with_timeout(|_token| async move { Ok::<i32, String>(42) })
            .await;

        match result {
            TimedGetObjectResult::Success(value) => assert_eq!(value, 42),
            _ => panic!("Expected Success, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_timeout_wrapper_times_out() {
        let config = TimeoutConfig {
            get_object_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let wrapper = RequestTimeoutWrapper::new(config);

        let result = wrapper
            .execute_with_timeout(|_token| async move {
                // Simulate a slow operation
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok::<i32, String>(42)
            })
            .await;

        match result {
            TimedGetObjectResult::Timeout(info) => {
                assert!(info.elapsed >= Duration::from_millis(50));
            }
            _ => panic!("Expected Timeout, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_timeout_wrapper_returns_error() {
        let config = TimeoutConfig {
            get_object_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let wrapper = RequestTimeoutWrapper::new(config);

        let result = wrapper
            .execute_with_timeout(|_token| async move { Err::<i32, String>("test error".to_string()) })
            .await;

        match result {
            TimedGetObjectResult::Error(e) => assert_eq!(e, "test error"),
            _ => panic!("Expected Error, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_timeout_wrapper_disabled() {
        let config = TimeoutConfig {
            get_object_timeout: Duration::ZERO,
            ..Default::default()
        };
        let wrapper = RequestTimeoutWrapper::new(config);

        // This would timeout if enabled
        let result = wrapper
            .execute_with_timeout(|_token| async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<i32, String>(42)
            })
            .await;

        match result {
            TimedGetObjectResult::Success(value) => assert_eq!(value, 42),
            _ => panic!("Expected Success when timeout disabled, got {:?}", result),
        }
    }

    // ============================================
    // Backpressure Tests
    // ============================================

    #[test]
    fn test_backpressure_config_defaults() {
        let config = BackpressureConfig::default();
        assert_eq!(config.buffer_size, 4 * 1024 * 1024); // 4MB
        assert_eq!(config.high_watermark, 80);
        assert_eq!(config.low_watermark, 50);
    }

    #[test]
    fn test_backpressure_monitor_state_transitions() {
        let config = BackpressureConfig {
            buffer_size: 1000,
            high_watermark: 80,
            low_watermark: 50,
        };
        let monitor = BackpressureMonitor::with_config(config);

        // Initially normal
        assert_eq!(monitor.state(), BackpressureState::Normal);

        // Write to reach high watermark
        let state = monitor.on_write(850);
        assert_eq!(state, BackpressureState::HighWatermark);

        // Read to go below low watermark
        let state = monitor.on_read(400);
        assert_eq!(state, BackpressureState::Normal);
    }

    #[test]
    fn test_backpressure_usage_percent() {
        let config = BackpressureConfig {
            buffer_size: 1000,
            high_watermark: 80,
            low_watermark: 50,
        };
        let monitor = BackpressureMonitor::with_config(config);

        monitor.on_write(500);
        assert!((monitor.usage_percent() - 50.0).abs() < 1.0);
    }

    // ============================================
    // Lock Optimizer Tests
    // ============================================

    #[test]
    fn test_lock_optimize_config_defaults() {
        let config = LockOptimizeConfig::default();
        assert!(config.enabled);
        assert_eq!(config.acquire_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_lock_stats_tracking() {
        let stats = LockStats::new();

        stats.record_acquire();
        stats.record_early_release(Duration::from_millis(100));
        stats.record_early_release(Duration::from_millis(200));

        assert_eq!(stats.locks_acquired.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(stats.locks_released_early.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(stats.max_hold_time(), Duration::from_millis(200));
    }

    #[test]
    fn test_lock_optimizer_creation() {
        let optimizer = LockOptimizer::new();
        assert!(optimizer.is_enabled());
    }

    // ============================================
    // I/O Priority Tests
    // ============================================

    #[test]
    fn test_io_priority_from_size() {
        // Small request (< 1MB) -> High priority
        assert_eq!(IoPriority::from_size(100 * 1024), IoPriority::High);
        assert_eq!(IoPriority::from_size(512 * 1024), IoPriority::High);

        // Medium request (1MB - 10MB) -> Normal priority
        assert_eq!(IoPriority::from_size(2 * 1024 * 1024), IoPriority::Normal);
        assert_eq!(IoPriority::from_size(5 * 1024 * 1024), IoPriority::Normal);

        // Large request (> 10MB) -> Low priority
        assert_eq!(IoPriority::from_size(20 * 1024 * 1024), IoPriority::Low);
        assert_eq!(IoPriority::from_size(100 * 1024 * 1024), IoPriority::Low);
    }

    #[test]
    fn test_io_priority_ordering() {
        assert!(IoPriority::High < IoPriority::Normal);
        assert!(IoPriority::Normal < IoPriority::Low);
    }

    #[test]
    fn test_io_load_level_from_wait() {
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(5)), IoLoadLevel::Low);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(30)), IoLoadLevel::Medium);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(100)), IoLoadLevel::High);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(500)), IoLoadLevel::Critical);
    }

    // ============================================
    // Deadlock Detector Tests
    // ============================================

    #[test]
    fn test_deadlock_detector_config_defaults() {
        let config = DeadlockDetectorConfig::default();
        assert!(!config.enabled); // Disabled by default
        assert_eq!(config.check_interval, Duration::from_secs(5));
        assert_eq!(config.hang_threshold, Duration::from_secs(10));
    }

    #[test]
    fn test_request_resource_tracker() {
        let tracker = RequestResourceTracker::new("req-1", "GetObject bucket/key");
        assert!(tracker.elapsed() < Duration::from_secs(1));
        assert!(!tracker.is_hung(Duration::from_secs(10)));
    }

    #[test]
    fn test_lock_info_creation() {
        let lock = LockInfo {
            id: "lock-1".to_string(),
            lock_type: LockType::Read,
            resource: "bucket/key".to_string(),
            acquire_time: std::time::Instant::now(),
        };
        assert_eq!(format!("{}", lock.lock_type), "read");
    }

    #[test]
    fn test_deadlock_detector_registration() {
        let config = DeadlockDetectorConfig {
            enabled: true,
            ..Default::default()
        };
        let detector = DeadlockDetector::new(config);

        detector.register_request("req-1", "Test request");
        assert_eq!(detector.tracked_count(), 1);

        detector.register_request("req-2", "Another request");
        assert_eq!(detector.tracked_count(), 2);

        detector.unregister_request("req-1");
        assert_eq!(detector.tracked_count(), 1);

        detector.unregister_request("req-2");
        assert_eq!(detector.tracked_count(), 0);
    }

    // ============================================
    // Integration Tests
    // ============================================

    #[tokio::test]
    async fn test_concurrent_requests_with_timeout() {
        use std::sync::Arc;
        use tokio::sync::Semaphore;

        let semaphore = Arc::new(Semaphore::new(10));
        let mut handles = vec![];

        // Spawn 20 concurrent "requests"
        for i in 0..20 {
            let sem = semaphore.clone();
            let handle = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                // Simulate some work
                tokio::time::sleep(Duration::from_millis(10)).await;
                i
            });
            handles.push(handle);
        }

        // Wait for all to complete
        let results: Vec<_> = futures::future::join_all(handles).await;
        assert_eq!(results.len(), 20);

        // All should complete successfully
        for result in results {
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_priority_scheduling_fairness() {
        // Simulate priority scheduling
        let mut high_priority_count = 0;
        let mut normal_priority_count = 0;
        let mut low_priority_count = 0;

        // Simulate 100 requests of various sizes
        for i in 0..100 {
            let size = match i % 3 {
                0 => 100 * 1024,       // Small: 100KB
                1 => 5 * 1024 * 1024,  // Medium: 5MB
                _ => 50 * 1024 * 1024, // Large: 50MB
            };

            match IoPriority::from_size(size) {
                IoPriority::High => high_priority_count += 1,
                IoPriority::Normal => normal_priority_count += 1,
                IoPriority::Low => low_priority_count += 1,
            }
        }

        // Each priority should have roughly 1/3 of requests
        assert_eq!(high_priority_count, 34);
        assert_eq!(normal_priority_count, 33);
        assert_eq!(low_priority_count, 33);
    }
}
