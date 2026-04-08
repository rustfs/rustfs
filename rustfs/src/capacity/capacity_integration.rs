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

//! Capacity management integration for application startup

use crate::capacity::{get_cached_capacity_with_metrics, init_capacity_management_for_local_disks};

/// Initialize capacity management system
/// This should be called during application startup after local disks are initialized
pub async fn init_capacity_management() {
    init_capacity_management_for_local_disks().await;
}

/// Get capacity statistics with metrics
#[allow(dead_code)]
pub async fn get_capacity_with_metrics() -> Option<(u64, String)> {
    get_cached_capacity_with_metrics()
        .await
        .map(|(capacity, source)| (capacity, source.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_object_capacity::capacity_manager::{CapacityUpdate, DataSource, get_capacity_manager};

    #[tokio::test]
    async fn test_get_capacity_with_metrics() {
        let manager = get_capacity_manager();
        manager
            .update_capacity(CapacityUpdate::exact(1000, 0), DataSource::RealTime)
            .await;

        let result = get_capacity_with_metrics().await;
        assert!(result.is_some());

        let (capacity, source) = result.unwrap();
        assert_eq!(capacity, 1000);
        assert_eq!(source, "real-time");
    }
}
