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

use crate::site_replication_reconcile::spawn_site_replication_reconcile_task;
use crate::storage_api::startup::services::{ECStore, EndpointServerPools, ServerContextSlot, StorageAdminApi};
use crate::{
    config::Config,
    connect::{
        CoarseNodeSummary, HeartbeatConfig, HeartbeatRuntime, InventoryError, InventoryFlag, InventoryRuntime, InventorySchedule,
        InventorySnapshot, spawn_heartbeat_runtime, spawn_inventory_runtime,
    },
    init::{init_buffer_profile_system, init_kms_system},
    server::ServiceStateManager,
    startup_audit::init_audit_runtime,
    startup_auth::init_auth_integrations,
    startup_background::init_background_service_runtime,
    startup_bucket_metadata::{init_bucket_metadata_runtime, init_embedded_bucket_metadata_runtime},
    startup_deadlock::init_deadlock_detector_runtime,
    startup_embedded_optional::init_embedded_optional_service_runtime,
    startup_iam::{IamBootstrapDisposition, init_embedded_iam_runtime, init_iam_runtime},
    startup_notification::{init_embedded_notification_runtime, init_notification_runtime},
    startup_observability::init_observability_runtime,
    startup_optional_runtime_sidecars::{OptionalRuntimeServices, init_optional_runtime_services},
};
use rustfs_common::GlobalReadiness;
use std::{io::Result, sync::Arc};
use tokio_util::sync::CancellationToken;

pub(crate) struct StartupServiceRuntime {
    pub(crate) optional_runtimes: OptionalRuntimeServices,
    pub(crate) heartbeat: Option<HeartbeatRuntime>,
    pub(crate) iam_bootstrap: IamBootstrapDisposition,
    pub(crate) enable_scanner: bool,
}

pub(crate) struct EmbeddedStartupServiceRuntime {
    pub(crate) iam_bootstrap: IamBootstrapDisposition,
}

pub(crate) async fn init_embedded_startup_runtime_services(
    config: &Config,
    endpoint_pools: EndpointServerPools,
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
    server_ctx: Arc<ServerContextSlot>,
) -> Result<EmbeddedStartupServiceRuntime> {
    init_embedded_optional_service_runtime(config).await;
    let buckets = init_embedded_bucket_metadata_runtime(store.clone()).await?;
    let iam_bootstrap = init_embedded_iam_runtime(store, ctx, readiness, server_ctx)
        .await
        .map_err(|err| std::io::Error::other(format!("IAM bootstrap setup: {err}")))?;
    init_embedded_notification_runtime(endpoint_pools, buckets).await;

    Ok(EmbeddedStartupServiceRuntime { iam_bootstrap })
}

pub(crate) async fn init_startup_runtime_services(
    config: &Config,
    endpoint_pools: EndpointServerPools,
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
    state_manager: Arc<ServiceStateManager>,
    server_ctx: Arc<ServerContextSlot>,
) -> Result<StartupServiceRuntime> {
    init_kms_system(config).await?;

    let optional_runtimes = init_optional_runtime_services().await?;
    let heartbeat_config = HeartbeatConfig::from_env().map_err(std::io::Error::other)?;
    let heartbeat_nodes = heartbeat_config.as_ref().map(|_| endpoint_pools.get_nodes().len());
    let inventory_drives = heartbeat_config
        .as_ref()
        .map(|_| endpoint_pools.as_ref().iter().map(|pool| pool.endpoints.as_ref().len()).sum());

    init_buffer_profile_system(config);
    init_deadlock_detector_runtime();

    let buckets = init_bucket_metadata_runtime(store.clone(), ctx.clone()).await?;
    let iam_bootstrap = init_iam_runtime(store.clone(), ctx.clone(), readiness, state_manager, server_ctx).await?;

    // Audit initialization requires the AppContext (server config + object store)
    // which is published by ensure_startup_after_iam inside init_iam_runtime.
    init_audit_runtime().await;
    // Unconditional: deferred IAM recovers in the background and has no callback into this
    // scheduler, so gating on the inline disposition would leave a recovered node with
    // self-pointing replication rules until the next restart. The task waits for IAM and
    // bucket metadata itself, and its first pass runs off this path.
    spawn_site_replication_reconcile_task(ctx.clone());
    init_auth_integrations().await?;
    init_notification_runtime(endpoint_pools, buckets).await?;
    let enable_scanner = init_background_service_runtime(store.clone()).await?;
    init_observability_runtime(store.clone(), ctx.clone()).await;
    let heartbeat = start_heartbeat_runtime(heartbeat_config.clone(), heartbeat_nodes, &ctx)?;
    let inventory = start_inventory_runtime(heartbeat_config, heartbeat_nodes, inventory_drives, store, &ctx)?;
    let heartbeat = heartbeat.map(|heartbeat| heartbeat.with_inventory(inventory));

    Ok(StartupServiceRuntime {
        optional_runtimes,
        heartbeat,
        iam_bootstrap,
        enable_scanner,
    })
}

fn start_heartbeat_runtime(
    config: Option<HeartbeatConfig>,
    node_count: Option<usize>,
    shutdown: &CancellationToken,
) -> Result<Option<HeartbeatRuntime>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let summary = u16::try_from(node_count.unwrap_or_default())
        .ok()
        .and_then(|total| CoarseNodeSummary::new(total, 0, 0).ok())
        .ok_or_else(|| std::io::Error::other("Connect heartbeat node count is outside protocol bounds"))?;
    spawn_heartbeat_runtime(Some(config), shutdown, move || summary).map_err(std::io::Error::other)
}

fn start_inventory_runtime(
    config: Option<HeartbeatConfig>,
    node_count: Option<usize>,
    expected_drive_count: Option<usize>,
    store: Arc<ECStore>,
    shutdown: &CancellationToken,
) -> Result<Option<InventoryRuntime>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let node_count = node_count.unwrap_or_default();
    let expected_drive_count = expected_drive_count.unwrap_or_default();
    spawn_inventory_runtime(Some(config), InventorySchedule::default(), shutdown, move || {
        let store = store.clone();
        async move {
            let info = StorageAdminApi::storage_info(store.as_ref()).await;
            inventory_snapshot(node_count, expected_drive_count, info)
        }
    })
    .map_err(std::io::Error::other)
}

fn inventory_snapshot(
    node_count: usize,
    expected_drive_count: usize,
    info: rustfs_madmin::StorageInfo,
) -> std::result::Result<InventorySnapshot, InventoryError> {
    if info.disks.len() != expected_drive_count {
        return Err(InventoryError::SnapshotIncomplete {
            expected: expected_drive_count,
            observed: info.disks.len(),
        });
    }
    let total = crate::app::storage_api::capacity::get_total_usable_capacity(&info.disks, &info) as u64;
    let free = crate::app::storage_api::capacity::get_total_usable_capacity_free(&info.disks, &info) as u64;
    let mut flags = Vec::with_capacity(3);
    if info
        .disks
        .iter()
        .any(|disk| disk.state == rustfs_madmin::ITEM_OFFLINE || disk.state == "disk not found")
    {
        flags.extend([InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline]);
    }
    if info.disks.iter().any(|disk| disk.healing) {
        flags.push(InventoryFlag::ClusterHealing);
    }
    InventorySnapshot::current(node_count, info.disks.len(), total, free, flags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_rejects_a_partial_startup_storage_snapshot() {
        let info = rustfs_madmin::StorageInfo {
            disks: vec![rustfs_madmin::Disk::default()],
            ..Default::default()
        };

        assert!(matches!(
            inventory_snapshot(2, 2, info),
            Err(InventoryError::SnapshotIncomplete {
                expected: 2,
                observed: 1
            })
        ));
    }

    #[test]
    fn inventory_marks_a_missing_drive_as_offline() {
        let info = rustfs_madmin::StorageInfo {
            disks: vec![
                rustfs_madmin::Disk::default(),
                rustfs_madmin::Disk {
                    state: "disk not found".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            inventory_snapshot(1, 2, info).expect("complete inventory should encode"),
            InventorySnapshot::current(1, 2, 0, 0, [InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline])
                .expect("expected inventory should encode")
        );
    }
}
