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

//! Integration tests for concurrent GetObject scheduling and request management.

#[cfg(test)]
mod tests {
    use crate::storage::concurrency::{
        ConcurrencyManager, GetObjectGuard, IoLoadLevel, IoStrategy, get_advanced_buffer_size, get_concurrency_aware_buffer_size,
    };
    use rustfs_config::{KI_B, MI_B};
    use serial_test::serial;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::{Instant, sleep};

    #[tokio::test]
    #[serial]
    async fn test_concurrent_request_tracking() {
        let initial = GetObjectGuard::concurrent_requests();

        let guard1 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 1);

        let guard2 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 2);

        let guard3 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 3);

        drop(guard1);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 2);

        drop(guard2);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 1);

        drop(guard3);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(GetObjectGuard::concurrent_requests(), initial);
    }

    #[tokio::test]
    #[serial]
    async fn test_adaptive_buffer_sizing() {
        let file_size = 32 * MI_B as i64;
        let base_buffer = 256 * KI_B;

        for concurrent_requests in [10, 6, 3] {
            let _guards: Vec<_> = (0..concurrent_requests)
                .map(|_| ConcurrencyManager::track_request())
                .collect();

            let buffer_size = get_concurrency_aware_buffer_size(file_size, base_buffer);
            assert!((64 * KI_B..=MI_B).contains(&buffer_size));
        }
    }

    #[tokio::test]
    async fn test_buffer_size_bounds() {
        let small_file = 1024i64;
        let min_buffer = get_concurrency_aware_buffer_size(small_file, 64 * KI_B);
        assert!(min_buffer >= 32 * KI_B);

        let huge_file = 10 * 1024 * MI_B as i64;
        let max_buffer = get_concurrency_aware_buffer_size(huge_file, MI_B);
        assert!(max_buffer <= MI_B);

        let medium_file = 200 * KI_B as i64;
        let buffer = get_concurrency_aware_buffer_size(medium_file, 128 * KI_B);
        assert!((64 * KI_B..=MI_B).contains(&buffer));
    }

    #[tokio::test]
    async fn test_disk_io_permits() {
        let manager = ConcurrencyManager::new();
        let start = Instant::now();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let mgr = Arc::new(manager.clone());
                tokio::spawn(async move {
                    let _permit = mgr.acquire_disk_read_permit().await.unwrap();
                    sleep(Duration::from_millis(10)).await;
                })
            })
            .collect();

        for handle in handles {
            handle.await.expect("task should complete");
        }

        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn test_advanced_buffer_size_uses_expected_bounds() {
        let sequential = get_advanced_buffer_size(64 * MI_B as i64, 256 * KI_B, true);
        let random = get_advanced_buffer_size(64 * MI_B as i64, 256 * KI_B, false);

        assert!(sequential >= random);
        assert!((32 * KI_B..=MI_B).contains(&sequential));
        assert!((32 * KI_B..=MI_B).contains(&random));
    }

    #[test]
    fn test_io_load_level_classification() {
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(0)), IoLoadLevel::Low);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(30)), IoLoadLevel::Medium);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(100)), IoLoadLevel::High);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(500)), IoLoadLevel::Critical);
    }

    #[test]
    fn test_io_strategy_buffer_sizing() {
        let base_buffer = 256 * KI_B;

        let strategy_low = IoStrategy::from_wait_duration(Duration::from_millis(5), base_buffer);
        assert_eq!(strategy_low.buffer_multiplier, 1.0);
        assert_eq!(strategy_low.buffer_size, base_buffer);
        assert!(strategy_low.enable_readahead);

        let strategy_med = IoStrategy::from_wait_duration(Duration::from_millis(30), base_buffer);
        assert_eq!(strategy_med.buffer_multiplier, 0.75);
        assert_eq!(strategy_med.buffer_size, (base_buffer as f64 * 0.75) as usize);
        assert!(strategy_med.enable_readahead);

        let strategy_high = IoStrategy::from_wait_duration(Duration::from_millis(100), base_buffer);
        assert_eq!(strategy_high.buffer_multiplier, 0.5);
        assert_eq!(strategy_high.buffer_size, (base_buffer as f64 * 0.5) as usize);
        assert!(!strategy_high.enable_readahead);

        let strategy_crit = IoStrategy::from_wait_duration(Duration::from_millis(500), base_buffer);
        assert_eq!(strategy_crit.buffer_multiplier, 0.4);
        let expected = ((base_buffer as f64) * 0.4) as usize;
        assert_eq!(strategy_crit.buffer_size, expected.clamp(32 * KI_B, MI_B));
        assert!(!strategy_crit.enable_readahead);
    }

    #[tokio::test]
    async fn test_calculate_io_strategy() {
        let manager = ConcurrencyManager::new();
        let base_buffer = 256 * KI_B;

        let strategy = manager.calculate_io_strategy(Duration::from_millis(5), base_buffer);
        assert_eq!(strategy.load_level, IoLoadLevel::Low);
        assert_eq!(strategy.buffer_size, base_buffer);

        let strategy = manager.calculate_io_strategy(Duration::from_millis(30), base_buffer);
        assert_eq!(strategy.load_level, IoLoadLevel::Medium);

        let strategy = manager.calculate_io_strategy(Duration::from_millis(100), base_buffer);
        assert_eq!(strategy.load_level, IoLoadLevel::High);
        assert!(!strategy.enable_readahead);

        let strategy = manager.calculate_io_strategy(Duration::from_millis(500), base_buffer);
        assert_eq!(strategy.load_level, IoLoadLevel::Critical);
    }

    #[tokio::test]
    async fn test_io_load_stats() {
        let manager = ConcurrencyManager::new();

        manager.record_permit_wait(Duration::from_millis(10));
        manager.record_permit_wait(Duration::from_millis(20));
        manager.record_permit_wait(Duration::from_millis(30));

        let (avg, p95, max, count) = manager.io_load_stats();

        assert_eq!(count, 3);
        assert!(avg >= Duration::from_millis(15) && avg <= Duration::from_millis(25));
        assert_eq!(max, Duration::from_millis(30));
        assert!(p95 >= Duration::from_millis(25));
    }
}
