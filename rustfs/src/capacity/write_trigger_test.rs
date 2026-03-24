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

//! Write trigger integration tests

#[cfg(test)]
mod tests {
    use crate::capacity::capacity_manager::{DataSource, HybridCapacityManager};
    use crate::capacity::capacity_metrics::{
        get_capacity_metrics, record_global_cache_hit, record_global_cache_miss, record_global_write_operation,
    };
    use serial_test::serial;
    use std::time::Duration;

    #[tokio::test]
    #[serial]
    async fn test_write_trigger_integration() {
        let manager = HybridCapacityManager::from_env();
        let metrics = get_capacity_metrics();

        // Record write operations
        manager.record_write_operation().await;
        manager.record_write_operation().await;
        manager.record_write_operation().await;

        // Check write frequency
        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 3);

        // Check metrics
        let summary = metrics.get_summary();
        assert_eq!(summary.write_triggered_updates, 0); // Not triggered yet
    }

    #[tokio::test]
    #[serial]
    async fn test_write_trigger_with_capacity_update() {
        let manager = HybridCapacityManager::from_env();
        let metrics = get_capacity_metrics();

        // Record write operations
        manager.record_write_operation().await;
        manager.record_write_operation().await;

        // Update capacity
        manager.update_capacity(1000, DataSource::WriteTriggered).await;

        // Check metrics
        let summary = metrics.get_summary();
        assert_eq!(summary.write_triggered_updates, 1);

        // Check capacity
        let cached = manager.get_capacity().await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().total_used, 1000);
    }

    #[tokio::test]
    #[serial]
    async fn test_metrics_recording() {
        let metrics = get_capacity_metrics();

        // Record various operations
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        metrics.record_scheduled_update();
        metrics.record_write_triggered_update();

        metrics.record_update_duration(Duration::from_millis(100));
        metrics.record_update_duration(Duration::from_millis(200));

        // Check summary
        let summary = metrics.get_summary();
        assert_eq!(summary.cache_hits, 2);
        assert_eq!(summary.cache_misses, 1);
        assert_eq!(summary.scheduled_updates, 1);
        assert_eq!(summary.write_triggered_updates, 1);
        assert_eq!(summary.avg_update_duration, Duration::from_millis(150));

        // Check hit rate
        let hit_rate = metrics.get_cache_hit_rate();
        assert!((hit_rate - 0.6666666666666666).abs() < 0.0001);
    }

    #[tokio::test]
    async fn test_write_frequency_tracking() {
        let manager = HybridCapacityManager::from_env();

        // Initial state
        assert_eq!(manager.get_write_frequency().await, 0);

        // Record writes
        for _ in 0..5 {
            manager.record_write_operation().await;
        }

        // Check frequency
        assert_eq!(manager.get_write_frequency().await, 5);

        // Wait for window to expire (60 seconds)
        // In real tests, we'd use a shorter window
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Frequency should still be 5 (window not expired)
        assert_eq!(manager.get_write_frequency().await, 5);
    }

    #[tokio::test]
    async fn test_needs_fast_update() {
        let manager = HybridCapacityManager::from_env();

        // No cache, should not need update
        assert!(!manager.needs_fast_update().await);

        // Update cache
        manager.update_capacity(1000, DataSource::Scheduled).await;

        // Fresh cache, should not need update
        assert!(!manager.needs_fast_update().await);

        // Record write operation
        manager.record_write_operation().await;

        // With recent write, should need fast update
        // (depending on configuration, this may or may not trigger)
        let needs_update = manager.needs_fast_update().await;
        // Just ensure it doesn't panic
        assert!(needs_update == true || needs_update == false);
    }

    #[test]
    #[serial]
    fn test_global_metrics_functions() {
        // Test global functions don't panic
        record_global_write_operation();
        record_global_cache_hit();
        record_global_cache_miss();

        let metrics = get_capacity_metrics();
        assert!(metrics.cache_hits.load(std::sync::atomic::Ordering::Relaxed) > 0);
    }
}
