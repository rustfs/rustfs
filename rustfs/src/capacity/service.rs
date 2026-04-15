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

use rustfs_ecstore::disk::DiskAPI;
use rustfs_io_metrics::capacity_metrics::{
    record_capacity_cache_hit, record_capacity_cache_miss, record_capacity_cache_served, record_capacity_refresh_request,
    record_capacity_scan_mode,
};
use rustfs_object_capacity::{CapacityDiskRef, capacity_manager, scan};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

pub fn capacity_disk_ref(endpoint: impl Into<String>, drive_path: impl Into<String>) -> CapacityDiskRef {
    CapacityDiskRef {
        endpoint: endpoint.into(),
        drive_path: drive_path.into(),
    }
}

fn capacity_disk_refs(disks: &[rustfs_madmin::Disk]) -> Vec<CapacityDiskRef> {
    disks
        .iter()
        .map(|disk| capacity_disk_ref(disk.endpoint.clone(), disk.drive_path.clone()))
        .collect()
}

async fn refresh_admin_disks_with_subset_fallback(
    capacity_manager: &capacity_manager::HybridCapacityManager,
    all_disks: Vec<CapacityDiskRef>,
    allow_dirty_subset: bool,
) -> Result<capacity_manager::CapacityUpdate, String> {
    let (refresh_disks, dirty_subset) = if allow_dirty_subset {
        scan::select_capacity_refresh_disks(capacity_manager, &all_disks).await
    } else {
        (all_disks.clone(), false)
    };

    match scan::refresh_capacity_with_scope(refresh_disks.clone(), dirty_subset).await {
        Ok(update) => Ok(update),
        Err(err) if dirty_subset => {
            warn!("Dirty-subset capacity refresh failed: {}. Retrying full-disk refresh for recovery", err);
            scan::refresh_capacity_with_scope(all_disks, false).await
        }
        Err(err) => Err(err),
    }
}

pub async fn refresh_or_join_admin_disks(
    capacity_manager: Arc<capacity_manager::HybridCapacityManager>,
    source: capacity_manager::DataSource,
    disks: &[rustfs_madmin::Disk],
    allow_dirty_subset: bool,
) -> Result<capacity_manager::CapacityUpdate, String> {
    let all_disks = capacity_disk_refs(disks);
    let refresh_manager = capacity_manager.clone();

    capacity_manager
        .refresh_or_join(source, move || {
            let capacity_manager = refresh_manager.clone();
            let all_disks = all_disks.clone();
            async move {
                refresh_admin_disks_with_subset_fallback(capacity_manager.as_ref(), all_disks, allow_dirty_subset).await
            }
        })
        .await
}

pub async fn spawn_refresh_if_needed_admin_disks(
    capacity_manager: Arc<capacity_manager::HybridCapacityManager>,
    source: capacity_manager::DataSource,
    disks: &[rustfs_madmin::Disk],
    allow_dirty_subset: bool,
) -> bool {
    let all_disks = capacity_disk_refs(disks);
    let refresh_manager = capacity_manager.clone();

    capacity_manager
        .spawn_refresh_if_needed(source, move || async move {
            refresh_admin_disks_with_subset_fallback(refresh_manager.as_ref(), all_disks, allow_dirty_subset).await
        })
        .await
}

pub async fn record_capacity_write(scope_token: Option<uuid::Uuid>) {
    capacity_manager::get_capacity_manager()
        .record_write_operation_with_scope_token(scope_token)
        .await;
}

pub async fn resolve_admin_used_capacity(disks: &[rustfs_madmin::Disk], fallback_used_capacity: u64) -> u64 {
    let capacity_manager = capacity_manager::get_capacity_manager();

    if let Some(cached) = capacity_manager.get_capacity().await {
        record_capacity_cache_hit();
        let cache_age = cached.last_update.elapsed();
        let fast_update_threshold = capacity_manager.get_config().fast_update_threshold;

        if cache_age < fast_update_threshold {
            record_capacity_cache_served("fresh");
            debug!(
                "Using cached capacity: {} bytes (age: {:?}, source: {:?}, files={}, estimated={})",
                cached.total_used, cache_age, cached.source, cached.file_count, cached.is_estimated
            );
            return cached.total_used;
        }

        let needs_update = capacity_manager.needs_fast_update().await;
        let should_block = capacity_manager.should_block_on_refresh(cache_age);

        if needs_update && should_block {
            let start = Instant::now();
            record_capacity_refresh_request("blocking", capacity_manager::DataSource::WriteTriggered.as_metric_label());
            return match refresh_or_join_admin_disks(
                capacity_manager.clone(),
                capacity_manager::DataSource::WriteTriggered,
                disks,
                true,
            )
            .await
            {
                Ok(update) => {
                    let elapsed = start.elapsed();
                    debug!(
                        "Foreground capacity refresh completed in {:?} (files={}, estimated={})",
                        elapsed, update.file_count, update.is_estimated
                    );
                    update.total_used
                }
                Err(err) => {
                    warn!("Foreground capacity refresh failed: {}, using cached value", err);
                    record_capacity_cache_served("stale");
                    cached.total_used
                }
            };
        }

        record_capacity_cache_served("stale");
        debug!(
            "Using stale cached capacity: {} bytes (age: {:?}, source: {:?}, files={}, estimated={}, needs_update={}, blocking={})",
            cached.total_used, cache_age, cached.source, cached.file_count, cached.is_estimated, needs_update, should_block
        );

        record_capacity_refresh_request("background", capacity_manager::DataSource::Scheduled.as_metric_label());
        if spawn_refresh_if_needed_admin_disks(capacity_manager.clone(), capacity_manager::DataSource::Scheduled, disks, true)
            .await
        {
            debug!("Background capacity update started");
        } else {
            debug!("Background update already in progress, skipping spawn");
        }

        return cached.total_used;
    }

    let start = Instant::now();
    record_capacity_cache_miss();
    record_capacity_refresh_request("initial", capacity_manager::DataSource::RealTime.as_metric_label());
    match refresh_or_join_admin_disks(capacity_manager.clone(), capacity_manager::DataSource::RealTime, disks, false).await {
        Ok(update) => {
            let elapsed = start.elapsed();
            info!(
                "Initial capacity calculation completed: {} bytes in {:?} (files={}, estimated={})",
                update.total_used, elapsed, update.file_count, update.is_estimated
            );
            update.total_used
        }
        Err(err) => {
            warn!(
                "Failed to calculate data directory used capacity: {}, falling back to disk used capacity",
                err
            );
            record_capacity_cache_served("fallback");
            record_capacity_scan_mode("fallback");
            capacity_manager
                .update_capacity(
                    capacity_manager::CapacityUpdate::fallback(fallback_used_capacity),
                    capacity_manager::DataSource::Fallback,
                )
                .await;
            fallback_used_capacity
        }
    }
}

pub async fn init_capacity_management_for_local_disks() {
    info!("Initializing capacity management system...");

    let disks = rustfs_ecstore::store::all_local_disk().await;
    if disks.is_empty() {
        warn!("No local disks found, capacity management will not run");
        return;
    }

    info!("Found {} local disk(s)", disks.len());

    let disk_refs = disks
        .iter()
        .map(|ds| capacity_disk_ref(ds.endpoint().to_string(), ds.to_string()))
        .collect();

    info!("Starting background capacity update task...");
    capacity_manager::start_background_task(disk_refs).await;

    info!("Capacity management system initialized successfully");
}

pub async fn get_cached_capacity_with_metrics() -> Option<(u64, &'static str)> {
    let manager = capacity_manager::get_capacity_manager();

    if let Some(cached) = manager.get_capacity().await {
        record_capacity_cache_hit();
        return Some((cached.total_used, capacity_source_label(cached.source)));
    }

    record_capacity_cache_miss();
    None
}

fn capacity_source_label(source: capacity_manager::DataSource) -> &'static str {
    match source {
        capacity_manager::DataSource::RealTime => "real-time",
        capacity_manager::DataSource::Scheduled => "scheduled",
        capacity_manager::DataSource::WriteTriggered => "write-triggered",
        capacity_manager::DataSource::Fallback => "fallback",
    }
}
