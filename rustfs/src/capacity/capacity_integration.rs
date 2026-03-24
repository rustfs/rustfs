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

use crate::capacity::capacity_manager::{get_capacity_manager, start_background_task, DataSource};
use crate::capacity::capacity_metrics::{get_capacity_metrics, start_metrics_logging};
use rustfs_ecstore::disk::DiskAPI;
use std::time::Duration;
use tracing::{info, warn};

/// Initialize capacity management system
/// This should be called during application startup after local disks are initialized
pub async fn init_capacity_management() {
    info!("Initializing capacity management system...");

    // Get all local disks
    let disks = rustfs_ecstore::store::all_local_disk().await;

    if disks.is_empty() {
        warn!("No local disks found, capacity management will not run");
        return;
    }

    info!("Found {} local disk(s)", disks.len());

    // Convert DiskStore to Disk (for compatibility with capacity_manager)
    let disk_refs: Vec<rustfs_madmin::Disk> = disks
        .iter()
        .map(|ds| rustfs_madmin::Disk {
            endpoint: ds.endpoint().to_string(),
            drive_path: ds.to_string(),
            root_disk: true,
            ..Default::default()
        })
        .collect();

    // Start background update task
    info!("Starting background capacity update task...");
    start_background_task(disk_refs).await;

    // Start metrics logging (log every 10 minutes)
    let metrics_interval = Duration::from_secs(600);
    info!("Starting metrics logging task (interval: {:?})...", metrics_interval);
    start_metrics_logging(metrics_interval).await;

    info!("Capacity management system initialized successfully");
}

/// Get capacity statistics with metrics
#[allow(dead_code)]
pub async fn get_capacity_with_metrics() -> Option<(u64, String)> {
    let manager = get_capacity_manager();
    let metrics = get_capacity_metrics();

    // Check cache
    if let Some(cached) = manager.get_capacity().await {
        metrics.record_cache_hit();

        let source = match cached.source {
            DataSource::RealTime => "real-time",
            DataSource::Scheduled => "scheduled",
            DataSource::WriteTriggered => "write-triggered",
            DataSource::Fallback => "fallback",
        };

        return Some((cached.total_used, source.to_string()));
    }

    metrics.record_cache_miss();
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::capacity_manager::{get_capacity_manager, DataSource};

    #[tokio::test]
    async fn test_get_capacity_with_metrics() {
        let manager = get_capacity_manager();
        manager.update_capacity(1000, DataSource::RealTime).await;

        let result = get_capacity_with_metrics().await;
        assert!(result.is_some());

        let (capacity, source) = result.unwrap();
        assert_eq!(capacity, 1000);
        assert_eq!(source, "real-time");
    }
}
