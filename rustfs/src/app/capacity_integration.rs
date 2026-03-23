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

use std::time::Duration;
use tracing::{info, error, warn};
use crate::app::capacity_manager::{start_background_task, get_capacity_manager};
use crate::app::capacity_metrics::{start_metrics_logging, get_capacity_metrics};

/// Initialize capacity management system
/// This should be called during application startup after local disks are initialized
pub async fn init_capacity_management() {
    info!("Initializing capacity management system...");
    
    // Get all local disks
    let disks = match rustfs_ecstore::store::all_local_disk().await {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to get local disks: {:?}", e);
            return;
        }
    };
    
    if disks.is_empty() {
        warn!("No local disks found, capacity management will not run");
        return;
    }
    
    info!("Found {} local disk(s)", disks.len());
    
    // Convert DiskStore to Disk (for compatibility with capacity_manager)
    let disk_refs: Vec<rustfs_madmin::Disk> = disks.iter()
        .map(|ds| rustfs_madmin::Disk {
            id: ds.id.clone(),
            endpoint: ds.endpoint.clone(),
            set_id: ds.set_id,
            pool_idx: ds.pool_idx,
            set_idx: ds.set_idx,
            root: ds.root.clone(),
            is_local: ds.is_local,
            total_capacity: ds.total_capacity,
            free_capacity: ds.free_capacity,
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
pub async fn get_capacity_with_metrics() -> Option<(u64, String)> {
    let manager = get_capacity_manager();
    let metrics = get_capacity_metrics();
    
    // Check cache
    if let Some(cached) = manager.get_capacity().await {
        metrics.record_cache_hit();
        
        let source = match cached.source {
            crate::app::capacity_manager::DataSource::RealTime => "real-time",
            crate::app::capacity_manager::DataSource::Scheduled => "scheduled",
            crate::app::capacity_manager::DataSource::WriteTriggered => "write-triggered",
            crate::app::capacity_manager::DataSource::Fallback => "fallback",
        };
        
        return Some((cached.total_used, source.to_string()));
    }
    
    metrics.record_cache_miss();
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_capacity_with_metrics() {
        let manager = get_capacity_manager();
        manager.update_capacity(1000, crate::app::capacity_manager::DataSource::RealTime).await;
        
        let result = get_capacity_with_metrics().await;
        assert!(result.is_some());
        
        let (capacity, source) = result.unwrap();
        assert_eq!(capacity, 1000);
        assert_eq!(source, "real-time");
    }
}
