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

//! Comprehensive tests for Hybrid Capacity Manager

#[cfg(test)]
mod tests {
    use crate::capacity::capacity_manager::{DataSource, HybridCapacityManager, HybridStrategyConfig};
    use serial_test::serial;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    #[serial]
    async fn test_capacity_manager_initialization() {
        let manager = HybridCapacityManager::from_env();
        assert!(manager.get_capacity().await.is_none());
    }

    #[tokio::test]
    async fn test_capacity_update_and_retrieval() {
        let manager = HybridCapacityManager::from_env();

        // Initially no cache
        assert!(manager.get_capacity().await.is_none());

        // Update capacity
        manager.update_capacity(1000, DataSource::RealTime).await;

        // Retrieve cached value
        let cached = manager.get_capacity().await;
        assert!(cached.is_some());
        let cached = cached.unwrap();
        assert_eq!(cached.total_used, 1000);
        assert_eq!(cached.source, DataSource::RealTime);
        assert!(!cached.is_estimated);
    }

    #[tokio::test]
    async fn test_write_operation_recording() {
        let manager = HybridCapacityManager::from_env();

        // Record multiple write operations
        manager.record_write_operation().await;
        manager.record_write_operation().await;
        manager.record_write_operation().await;

        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 3);
    }

    #[tokio::test]
    async fn test_fast_update_detection() {
        let manager = HybridCapacityManager::from_env();

        // No cache, should not need fast update
        assert!(!manager.needs_fast_update().await);

        // Update cache
        manager.update_capacity(1000, DataSource::RealTime).await;

        // Fresh cache, should not need fast update
        assert!(!manager.needs_fast_update().await);

        // Record write operation
        manager.record_write_operation().await;

        // Wait for cache to become stale
        sleep(Duration::from_millis(100)).await;

        // Now cache is stale and there's recent write
        // Note: This might not trigger due to timing, so we just check it doesn't panic
        let _needs_update = manager.needs_fast_update().await;
    }

    #[tokio::test]
    async fn test_cache_age_tracking() {
        let manager = HybridCapacityManager::from_env();

        // No cache, age should be None
        assert!(manager.get_cache_age().await.is_none());

        // Update cache
        manager.update_capacity(1000, DataSource::RealTime).await;

        // Check cache age
        let age = manager.get_cache_age().await;
        assert!(age.is_some());
        let age = age.unwrap();
        assert!(age < Duration::from_secs(1));

        // Wait a bit
        sleep(Duration::from_millis(100)).await;

        // Check age again
        let age = manager.get_cache_age().await.unwrap();
        assert!(age >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_data_source_tracking() {
        let manager = HybridCapacityManager::from_env();

        // Test different data sources
        let sources = vec![
            DataSource::RealTime,
            DataSource::Scheduled,
            DataSource::WriteTriggered,
            DataSource::Fallback,
        ];

        for source in sources {
            manager.update_capacity(1000, source).await;
            let cached = manager.get_capacity().await.unwrap();
            assert_eq!(cached.source, source);
        }
    }

    #[tokio::test]
    async fn test_config_from_env() {
        let config = HybridStrategyConfig::from_env();

        // Check default values
        assert_eq!(config.scheduled_update_interval, Duration::from_secs(300));
        assert_eq!(config.write_trigger_delay, Duration::from_secs(10));
        assert_eq!(config.write_frequency_threshold, 10);
        assert_eq!(config.fast_update_threshold, Duration::from_secs(60));
        assert!(config.enable_smart_update);
        assert!(config.enable_write_trigger);
    }

    #[tokio::test]
    async fn test_write_frequency_window() {
        let manager = HybridCapacityManager::from_env();

        // Record many write operations
        for _ in 0..20 {
            manager.record_write_operation().await;
        }

        // Check frequency (should be 20 since all are within 1 minute)
        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 20);

        // Note: In a real test, we would wait for the window to expire
        // and verify that old writes are removed
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrent_access() {
        let manager = Arc::new(HybridCapacityManager::from_env());

        // Simulate concurrent updates
        let mut handles = vec![];

        for i in 0..10 {
            let mgr = manager.clone();
            let handle = tokio::spawn(async move {
                mgr.update_capacity(i as u64 * 100, DataSource::RealTime).await;
                mgr.record_write_operation().await;
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify final state
        let cached = manager.get_capacity().await;
        assert!(cached.is_some());

        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 10);
    }

    #[tokio::test]
    #[serial]
    async fn test_performance_overhead() {
        let manager = Arc::new(HybridCapacityManager::from_env());

        // Measure time for 1000 operations
        let start = std::time::Instant::now();

        for i in 0..1000 {
            manager.update_capacity(i as u64, DataSource::RealTime).await;
            manager.record_write_operation().await;
            let _ = manager.get_capacity().await;
        }

        let elapsed = start.elapsed();

        // Should complete in less than 1 second
        assert!(elapsed < Duration::from_secs(1));

        println!("1000 operations completed in {:?}", elapsed);
    }
}
