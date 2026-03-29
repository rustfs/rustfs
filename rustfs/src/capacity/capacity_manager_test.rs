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
    use crate::capacity::capacity_manager::{CapacityUpdate, DataSource, HybridCapacityManager, HybridStrategyConfig};
    use serial_test::serial;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
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

        assert!(manager.get_capacity().await.is_none());

        manager
            .update_capacity(CapacityUpdate::exact(1000, 10), DataSource::RealTime)
            .await;

        let cached = manager.get_capacity().await;
        assert!(cached.is_some());
        let cached = cached.unwrap();
        assert_eq!(cached.total_used, 1000);
        assert_eq!(cached.file_count, 10);
        assert_eq!(cached.source, DataSource::RealTime);
        assert!(!cached.is_estimated);
    }

    #[tokio::test]
    async fn test_write_operation_recording() {
        let manager = HybridCapacityManager::from_env();

        manager.record_write_operation().await;
        manager.record_write_operation().await;
        manager.record_write_operation().await;

        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 3);
    }

    #[tokio::test]
    async fn test_fast_update_detection() {
        let manager = HybridCapacityManager::from_env();

        assert!(!manager.needs_fast_update().await);

        manager
            .update_capacity(CapacityUpdate::exact(1000, 1), DataSource::RealTime)
            .await;

        assert!(!manager.needs_fast_update().await);

        manager.record_write_operation().await;
        sleep(Duration::from_millis(100)).await;

        let _needs_update = manager.needs_fast_update().await;
    }

    #[tokio::test]
    async fn test_cache_age_tracking() {
        let manager = HybridCapacityManager::from_env();

        assert!(manager.get_cache_age().await.is_none());

        manager
            .update_capacity(CapacityUpdate::exact(1000, 1), DataSource::RealTime)
            .await;

        let age = manager.get_cache_age().await;
        assert!(age.is_some());
        let age = age.unwrap();
        assert!(age < Duration::from_secs(1));

        sleep(Duration::from_millis(100)).await;

        let age = manager.get_cache_age().await.unwrap();
        assert!(age >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_data_source_tracking() {
        let manager = HybridCapacityManager::from_env();

        let sources = vec![
            DataSource::RealTime,
            DataSource::Scheduled,
            DataSource::WriteTriggered,
            DataSource::Fallback,
        ];

        for source in sources {
            manager.update_capacity(CapacityUpdate::exact(1000, 1), source).await;
            let cached = manager.get_capacity().await.unwrap();
            assert_eq!(cached.source, source);
        }
    }

    #[tokio::test]
    async fn test_config_from_env() {
        let config = HybridStrategyConfig::from_env();

        assert_eq!(config.scheduled_update_interval, Duration::from_secs(120));
        assert_eq!(config.write_trigger_delay, Duration::from_secs(5));
        assert_eq!(config.write_frequency_threshold, 5);
        assert_eq!(config.fast_update_threshold, Duration::from_secs(30));
        assert!(config.enable_smart_update);
        assert!(config.enable_write_trigger);
    }

    #[tokio::test]
    async fn test_write_frequency_window() {
        let manager = HybridCapacityManager::from_env();

        for _ in 0..20 {
            manager.record_write_operation().await;
        }

        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 20);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrent_access() {
        let manager = Arc::new(HybridCapacityManager::from_env());
        let mut handles = vec![];

        for i in 0..10 {
            let mgr = manager.clone();
            let handle = tokio::spawn(async move {
                mgr.update_capacity(CapacityUpdate::exact(i as u64 * 100, i), DataSource::RealTime)
                    .await;
                mgr.record_write_operation().await;
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let cached = manager.get_capacity().await;
        assert!(cached.is_some());

        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 10);
    }

    #[tokio::test]
    #[serial]
    async fn test_performance_overhead() {
        let manager = Arc::new(HybridCapacityManager::from_env());
        let start = std::time::Instant::now();

        for i in 0..1000 {
            manager
                .update_capacity(CapacityUpdate::exact(i as u64, i), DataSource::RealTime)
                .await;
            manager.record_write_operation().await;
            let _ = manager.get_capacity().await;
        }

        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_secs(1));

        println!("1000 operations completed in {:?}", elapsed);
    }

    #[tokio::test]
    async fn test_refresh_or_join_singleflight() {
        let manager = Arc::new(HybridCapacityManager::from_env());
        let calls = Arc::new(AtomicUsize::new(0));

        let mgr1 = manager.clone();
        let calls1 = calls.clone();
        let first = tokio::spawn(async move {
            mgr1.refresh_or_join(DataSource::Scheduled, move || async move {
                calls1.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(50)).await;
                Ok(CapacityUpdate::exact(2048, 8))
            })
            .await
        });

        sleep(Duration::from_millis(10)).await;

        let mgr2 = manager.clone();
        let calls2 = calls.clone();
        let second = tokio::spawn(async move {
            mgr2.refresh_or_join(DataSource::WriteTriggered, move || async move {
                calls2.fetch_add(1, Ordering::SeqCst);
                Ok(CapacityUpdate::exact(4096, 16))
            })
            .await
        });

        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.total_used, 2048);
        assert_eq!(second.total_used, 2048);
        let cached = manager.get_capacity().await.unwrap();
        assert_eq!(cached.total_used, 2048);
        assert_eq!(cached.file_count, 8);
    }

    #[tokio::test]
    async fn test_spawn_refresh_if_needed_deduplicates_background_refresh() {
        let manager = Arc::new(HybridCapacityManager::from_env());
        let calls = Arc::new(AtomicUsize::new(0));

        let first_manager = manager.clone();
        let first_calls = calls.clone();
        let started = first_manager
            .clone()
            .spawn_refresh_if_needed(DataSource::Scheduled, move || async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(50)).await;
                Ok(CapacityUpdate::estimated(8192, 32))
            })
            .await;
        assert!(started);

        let second_manager = manager.clone();
        let second_calls = calls.clone();
        let started = second_manager
            .clone()
            .spawn_refresh_if_needed(DataSource::Scheduled, move || async move {
                second_calls.fetch_add(1, Ordering::SeqCst);
                Ok(CapacityUpdate::exact(1, 1))
            })
            .await;
        assert!(!started);

        sleep(Duration::from_millis(100)).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!manager.refresh_in_progress().await);
        let cached = manager.get_capacity().await.unwrap();
        assert_eq!(cached.total_used, 8192);
        assert!(cached.is_estimated);
    }
}
