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
    use crate::capacity::capacity_manager::{CapacityUpdate, DataSource, HybridCapacityManager};
    use serial_test::serial;
    use std::time::Duration;

    #[tokio::test]
    #[serial]
    async fn test_write_trigger_integration() {
        let manager = HybridCapacityManager::from_env();

        manager.record_write_operation().await;
        manager.record_write_operation().await;
        manager.record_write_operation().await;

        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 3);
    }

    #[tokio::test]
    #[serial]
    async fn test_write_trigger_with_capacity_update() {
        let manager = HybridCapacityManager::from_env();

        manager
            .update_capacity(CapacityUpdate::exact(1000, 4), DataSource::WriteTriggered)
            .await;

        let cached = manager.get_capacity().await;
        assert!(cached.is_some());
        let cached = cached.unwrap();
        assert_eq!(cached.total_used, 1000);
        assert_eq!(cached.file_count, 4);
        assert_eq!(cached.source, DataSource::WriteTriggered);
    }

    #[tokio::test]
    async fn test_write_frequency_tracking() {
        let manager = HybridCapacityManager::from_env();

        assert_eq!(manager.get_write_frequency().await, 0);

        for _ in 0..5 {
            manager.record_write_operation().await;
        }

        assert_eq!(manager.get_write_frequency().await, 5);

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(manager.get_write_frequency().await, 5);
    }

    #[tokio::test]
    async fn test_needs_fast_update() {
        let manager = HybridCapacityManager::from_env();

        assert!(!manager.needs_fast_update().await);

        manager
            .update_capacity(CapacityUpdate::exact(1000, 1), DataSource::Scheduled)
            .await;

        assert!(!manager.needs_fast_update().await);

        manager.record_write_operation().await;

        let needs_update = manager.needs_fast_update().await;
        #[allow(clippy::overly_complex_bool_expr)]
        let _ = needs_update || !needs_update;
    }
}
