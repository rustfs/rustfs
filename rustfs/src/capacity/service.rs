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

use crate::storage_api::capacity::service::{all_local_disk, disk_drive_path, disk_endpoint};
use rustfs_io_metrics::capacity_metrics::{record_capacity_cache_hit, record_capacity_cache_miss};
use rustfs_object_capacity::{CapacityDiskRef, capacity_manager};
use tracing::{info, warn};

const LOG_COMPONENT_CAPACITY: &str = "capacity";
const LOG_SUBSYSTEM_CAPACITY: &str = "capacity";

pub fn capacity_disk_ref(endpoint: impl Into<String>, drive_path: impl Into<String>) -> CapacityDiskRef {
    CapacityDiskRef {
        endpoint: endpoint.into(),
        drive_path: drive_path.into(),
    }
}

pub async fn record_capacity_write(scope_token: Option<uuid::Uuid>) {
    capacity_manager::get_capacity_manager()
        .record_write_operation_with_scope_token(scope_token)
        .await;
}

pub async fn init_capacity_management_for_local_disks() {
    info!(
        component = LOG_COMPONENT_CAPACITY,
        subsystem = LOG_SUBSYSTEM_CAPACITY,
        event = "capacity_manager_state",
        state = "initializing",
        "Capacity manager state changed"
    );

    let disks = all_local_disk().await;
    if disks.is_empty() {
        warn!(
            component = LOG_COMPONENT_CAPACITY,
            subsystem = LOG_SUBSYSTEM_CAPACITY,
            event = "capacity_manager_state",
            state = "skipped",
            reason = "no_local_disks",
            "Capacity manager state changed"
        );
        return;
    }

    info!(
        component = LOG_COMPONENT_CAPACITY,
        subsystem = LOG_SUBSYSTEM_CAPACITY,
        event = "capacity_manager_disks_detected",
        disk_count = disks.len(),
        "Detected local disks for capacity management"
    );

    let disk_refs = disks
        .iter()
        .map(|ds| capacity_disk_ref(disk_endpoint(ds), disk_drive_path(ds)))
        .collect();

    info!(
        component = LOG_COMPONENT_CAPACITY,
        subsystem = LOG_SUBSYSTEM_CAPACITY,
        event = "capacity_manager_state",
        state = "starting_background_task",
        "Capacity manager state changed"
    );
    capacity_manager::start_background_task(disk_refs).await;

    info!(
        component = LOG_COMPONENT_CAPACITY,
        subsystem = LOG_SUBSYSTEM_CAPACITY,
        event = "capacity_manager_state",
        state = "initialized",
        "Capacity manager state changed"
    );
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
