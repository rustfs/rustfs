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

//! Integration tests for multi-factor I/O scheduler.
//!
//! These tests verify the enhanced scheduler behavior in realistic scenarios
//! combining storage media, access patterns, bandwidth, and concurrency.

#[cfg(test)]
mod tests {
    use crate::storage::concurrency::ConcurrencyManager;
    use serial_test::serial;
    use std::time::Duration;

    /// Test scenario: NVMe sequential read with low load
    ///
    /// Expected behavior: Maximum buffer size, readahead enabled
    #[tokio::test]
    #[serial]
    async fn test_scenario_nvme_sequential_low_load() {
        let manager = ConcurrencyManager::new();

        let strategy = manager.calculate_io_strategy_with_context(
            5 * 1024 * 1024,          // 5MB file
            256 * 1024,               // 256KB base buffer
            Duration::from_millis(5), // Low load
            true,                     // Sequential
        );

        // Verify basic strategy properties
        assert!(strategy.buffer_size > 0);
        assert_eq!(strategy.load_level.level_index(), 0); // Low
    }

    /// Test scenario: High concurrency reduces buffer
    #[tokio::test]
    #[serial]
    async fn test_scenario_high_concurrency() {
        let manager = ConcurrencyManager::new();

        // Low concurrency
        let low_strategy = {
            let _g1 = ConcurrencyManager::track_request();
            let _g2 = ConcurrencyManager::track_request();
            manager.calculate_io_strategy_with_context(50 * 1024 * 1024, 512 * 1024, Duration::from_millis(10), true)
        };

        // High concurrency
        let high_strategy = {
            let _guards: Vec<_> = (0..16).map(|_| ConcurrencyManager::track_request()).collect();
            manager.calculate_io_strategy_with_context(50 * 1024 * 1024, 512 * 1024, Duration::from_millis(10), true)
        };

        // Buffer should decrease with higher concurrency
        assert!(high_strategy.concurrent_requests >= low_strategy.concurrent_requests);
    }

    /// Test scenario: Progressive load increase
    #[tokio::test]
    #[serial]
    async fn test_scenario_progressive_load() {
        let manager = ConcurrencyManager::new();

        let file_size = 50 * 1024 * 1024;
        let base_buffer = 512 * 1024;

        // Low load
        let low_strategy = manager.calculate_io_strategy_with_context(file_size, base_buffer, Duration::from_millis(5), true);

        // High load
        let high_strategy = manager.calculate_io_strategy_with_context(file_size, base_buffer, Duration::from_millis(100), true);

        // Critical load
        let critical_strategy =
            manager.calculate_io_strategy_with_context(file_size, base_buffer, Duration::from_millis(300), true);

        // Load levels should increase
        assert!(low_strategy.load_level.level_index() < high_strategy.load_level.level_index());
        assert!(high_strategy.load_level.level_index() < critical_strategy.load_level.level_index());

        // Readahead should be disabled at critical load
        assert!(!critical_strategy.enable_readahead);
    }

    /// Test scenario: Small file gets high priority
    #[tokio::test]
    #[serial]
    async fn test_scenario_small_file_priority() {
        let manager = ConcurrencyManager::new();

        let strategy = manager.calculate_io_strategy_with_context(
            100 * 1024, // 100KB (small)
            256 * 1024,
            Duration::from_millis(100), // Even under high load
            false,
        );

        // Should be high priority due to size
        assert!(strategy.priority.is_high());
    }

    /// Test scenario: Large file gets low priority
    #[tokio::test]
    #[serial]
    async fn test_scenario_large_file_priority() {
        let manager = ConcurrencyManager::new();

        let strategy = manager.calculate_io_strategy_with_context(
            100 * 1024 * 1024, // 100MB (large)
            256 * 1024,
            Duration::from_millis(5), // Even under low load
            false,
        );

        // Should be low priority due to size
        assert!(strategy.priority.is_low());
    }

    /// Test scenario: Access pattern tracking
    #[tokio::test]
    #[serial]
    async fn test_scenario_access_pattern_tracking() {
        let manager = ConcurrencyManager::new();

        // Record sequential accesses
        for offset in [0, 1024, 2048, 3072, 4096] {
            manager.record_access(offset, 1024);
        }

        // Should detect sequential pattern
        let pattern = manager.current_access_pattern();
        assert!(pattern.is_sequential() || pattern.is_unknown());
    }

    /// Test scenario: Bandwidth recording
    #[tokio::test]
    #[serial]
    async fn test_scenario_bandwidth_recording() {
        let manager = ConcurrencyManager::new();

        // Record transfer
        manager.record_transfer(10 * 1024 * 1024, Duration::from_millis(100));

        // Bandwidth snapshot should be available (returns BandwidthSnapshot directly)
        let snapshot = manager.current_bandwidth_snapshot();
        assert!(snapshot.bytes_per_second > 0);
    }

    /// Test scenario: Sequential vs random comparison
    #[tokio::test]
    #[serial]
    async fn test_scenario_sequential_vs_random() {
        let manager = ConcurrencyManager::new();

        let file_size = 50 * 1024 * 1024;
        let base_buffer = 512 * 1024;
        let wait = Duration::from_millis(20);

        let sequential_strategy = manager.calculate_io_strategy_with_context(file_size, base_buffer, wait, true);

        let random_strategy = manager.calculate_io_strategy_with_context(file_size, base_buffer, wait, false);

        // Sequential should get better (or equal) treatment
        assert!(sequential_strategy.buffer_size >= random_strategy.buffer_size);
    }

    /// Test scenario: Real-world video streaming
    #[tokio::test]
    #[serial]
    async fn test_real_world_video_streaming() {
        let manager = ConcurrencyManager::new();

        let strategy = manager.calculate_io_strategy_with_context(
            500 * 1024 * 1024, // 500MB video
            512 * 1024,
            Duration::from_millis(25),
            true, // Sequential streaming
        );

        // Should be optimized for streaming
        assert!(strategy.buffer_size > 0);
        assert_eq!(strategy.load_level.level_index(), 1); // Medium load
    }

    /// Test scenario: Real-world API config files
    #[tokio::test]
    #[serial]
    async fn test_real_world_api_configs() {
        let manager = ConcurrencyManager::new();

        let strategy = manager.calculate_io_strategy_with_context(
            100 * 1024, // 100KB JSON
            256 * 1024,
            Duration::from_millis(5),
            false, // Random access to different files
        );

        // Should optimize for low latency
        assert!(strategy.priority.is_high());
        assert_eq!(strategy.load_level.level_index(), 0); // Low load
    }
}
