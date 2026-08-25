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
        CoarseNodeSummary, HeartbeatConfig, HeartbeatError, HeartbeatRuntime, InventoryError, InventoryFlag, InventoryRuntime,
        InventorySchedule, InventorySnapshot, runtime::heartbeat_failure_reason, spawn_heartbeat_runtime,
        spawn_inventory_runtime,
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
use std::{collections::BTreeSet, io::Result, sync::Arc};
use tokio_util::sync::CancellationToken;

pub(crate) struct StartupServiceRuntime {
    pub(crate) optional_runtimes: OptionalRuntimeServices,
    pub(crate) heartbeat: Option<HeartbeatRuntime>,
    pub(crate) inventory: Option<InventoryRuntime>,
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
    let buckets = init_embedded_bucket_metadata_runtime(store.clone(), &ctx).await?;
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

    Ok(StartupServiceRuntime {
        optional_runtimes,
        heartbeat,
        inventory,
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
    if !config.transport_enabled() {
        return Ok(None);
    }
    let summary = u16::try_from(node_count.unwrap_or_default())
        .ok()
        .and_then(|total| CoarseNodeSummary::new(total, 0, 0).ok())
        .ok_or_else(|| std::io::Error::other("Connect heartbeat node count is outside protocol bounds"))?;
    spawn_heartbeat_runtime(Some(config), shutdown, move || summary).map_err(startup_heartbeat_error)
}

fn startup_heartbeat_error(error: HeartbeatError) -> std::io::Error {
    std::io::Error::other(heartbeat_failure_reason(&error))
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
    let drive_count = inventory_topology_slot_count(&info, expected_drive_count)?;
    let (total, free) = inventory_capacity(&info)?;
    let mut flags = Vec::with_capacity(3);
    if info.disks.iter().any(|disk| !inventory_disk_is_healthy(disk)) {
        flags.push(InventoryFlag::ClusterDegraded);
    }
    if info.disks.iter().any(inventory_disk_is_offline) {
        flags.push(InventoryFlag::DriveOffline);
    }
    if info.disks.iter().any(|disk| disk.healing) {
        flags.push(InventoryFlag::ClusterHealing);
    }
    InventorySnapshot::current(node_count, drive_count, total, free, flags)
}

fn inventory_topology_slot_count(
    info: &rustfs_madmin::StorageInfo,
    expected_drive_count: usize,
) -> std::result::Result<usize, InventoryError> {
    let total_sets = &info.backend.total_sets;
    let drives_per_set = &info.backend.drives_per_set;
    let geometry_drive_count = if total_sets.is_empty()
        || total_sets.len() != drives_per_set.len()
        || total_sets.contains(&0)
        || drives_per_set.contains(&0)
    {
        None
    } else {
        total_sets
            .iter()
            .zip(drives_per_set)
            .try_fold(0_usize, |total, (&sets, &drives)| {
                sets.checked_mul(drives).and_then(|pool| total.checked_add(pool))
            })
    };
    if geometry_drive_count != Some(expected_drive_count) {
        return Err(InventoryError::SnapshotIncomplete {
            expected: expected_drive_count,
            observed: 0,
        });
    }

    let mut slots = BTreeSet::new();
    let mut invalid = false;
    for disk in &info.disks {
        let key = match (
            usize::try_from(disk.pool_index),
            usize::try_from(disk.set_index),
            usize::try_from(disk.disk_index),
        ) {
            (Ok(pool_index), Ok(set_index), Ok(disk_index)) => (pool_index, set_index, disk_index),
            _ => {
                invalid = true;
                continue;
            }
        };
        if key.0 >= total_sets.len() || key.1 >= total_sets[key.0] || key.2 >= drives_per_set[key.0] {
            invalid = true;
            continue;
        }
        if !slots.insert(key) {
            invalid = true;
        }
    }
    if invalid || slots.len() != expected_drive_count {
        return Err(InventoryError::SnapshotIncomplete {
            expected: expected_drive_count,
            observed: slots.len(),
        });
    }
    Ok(slots.len())
}

fn inventory_disk_is_healthy(disk: &rustfs_madmin::Disk) -> bool {
    let disk_state_is_healthy = ["ok", rustfs_madmin::ITEM_ONLINE, "unformatted"]
        .iter()
        .any(|state| disk.state.eq_ignore_ascii_case(state));
    let runtime_state_is_healthy = disk.runtime_state.as_deref().is_none_or(|runtime_state| {
        [rustfs_madmin::ITEM_ONLINE, "returning"]
            .iter()
            .any(|state| runtime_state.eq_ignore_ascii_case(state))
    });
    disk_state_is_healthy && runtime_state_is_healthy
}

fn inventory_disk_is_offline(disk: &rustfs_madmin::Disk) -> bool {
    [rustfs_madmin::ITEM_OFFLINE, "missing", "disk not found"]
        .iter()
        .any(|state| disk.state.eq_ignore_ascii_case(state))
        || disk.runtime_state.as_deref().is_some_and(|runtime_state| {
            [rustfs_madmin::ITEM_OFFLINE, "missing"]
                .iter()
                .any(|state| runtime_state.eq_ignore_ascii_case(state))
        })
}

fn inventory_capacity(info: &rustfs_madmin::StorageInfo) -> std::result::Result<(u64, u64), InventoryError> {
    let configured_data_widths = (!info.backend.standard_sc_data.is_empty()).then_some(info.backend.standard_sc_data.as_slice());
    aggregate_inventory_capacity(&info.disks, configured_data_widths)
}

fn aggregate_inventory_capacity(
    disks: &[rustfs_madmin::Disk],
    data_widths: Option<&[usize]>,
) -> std::result::Result<(u64, u64), InventoryError> {
    let mut seen = BTreeSet::new();
    let mut indexed = Vec::with_capacity(disks.len());
    for disk in disks {
        let key = match (
            usize::try_from(disk.pool_index),
            usize::try_from(disk.set_index),
            usize::try_from(disk.disk_index),
        ) {
            (Ok(pool_index), Ok(set_index), Ok(disk_index)) => (pool_index, set_index, disk_index),
            _ => {
                return Err(InventoryError::SnapshotIncomplete {
                    expected: disks.len(),
                    observed: indexed.len(),
                });
            }
        };
        if seen.insert(key) {
            indexed.push((key, disk));
        }
    }

    let usable_widths = data_widths.filter(|widths| {
        indexed
            .iter()
            .all(|((pool_index, _, _), _)| widths.get(*pool_index).is_some_and(|width| *width > 0))
    });
    let mut total = 0_u64;
    let mut free = 0_u64;
    let mut included = 0;
    for ((pool_index, _, disk_index), disk) in indexed {
        let include = match usable_widths {
            Some(widths) => disk_index < widths[pool_index],
            None => {
                let state = disk.state.trim().to_ascii_lowercase();
                !state.contains("offline") && !state.contains("not found")
            }
        };
        if !include {
            continue;
        }
        included += 1;
        total = total.checked_add(disk.total_space).ok_or(InventoryError::Capacity)?;
        free = free.checked_add(disk.available_space).ok_or(InventoryError::Capacity)?;
    }
    if !disks.is_empty() && included == 0 {
        return Err(InventoryError::SnapshotIncomplete {
            expected: disks.len(),
            observed: 0,
        });
    }
    Ok((total, free))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_startup_errors_expose_only_stable_codes() {
        let error = startup_heartbeat_error(HeartbeatError::StateIo {
            path: std::path::PathBuf::from("/private/connect/canary/state.json"),
            source: std::io::Error::other("private-source-canary"),
        });

        assert_eq!(error.to_string(), "connect_heartbeat_state_io");
    }

    fn disk(state: &str, runtime_state: Option<&str>, disk_index: i32) -> rustfs_madmin::Disk {
        rustfs_madmin::Disk {
            state: state.to_owned(),
            runtime_state: runtime_state.map(str::to_owned),
            pool_index: 0,
            set_index: 0,
            disk_index,
            total_space: 100,
            available_space: 40,
            ..Default::default()
        }
    }

    fn info(disks: Vec<rustfs_madmin::Disk>) -> rustfs_madmin::StorageInfo {
        let drive_count = disks.len();
        rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![drive_count],
                total_sets: vec![1],
                drives_per_set: vec![drive_count],
                ..Default::default()
            },
            disks,
        }
    }

    #[test]
    fn inventory_rejects_a_partial_startup_storage_snapshot() {
        let mut info = info(vec![disk("ok", Some("online"), 0)]);
        info.backend.standard_sc_data = vec![2];
        info.backend.drives_per_set = vec![2];

        assert!(matches!(
            inventory_snapshot(2, 2, info),
            Err(InventoryError::SnapshotIncomplete {
                expected: 2,
                observed: 1
            })
        ));
    }

    #[test]
    fn inventory_rejects_duplicate_or_invalid_topology_slots() {
        let valid = disk("ok", Some("online"), 0);
        let duplicate = valid.clone();
        assert!(matches!(
            inventory_snapshot(1, 2, info(vec![valid.clone(), duplicate])),
            Err(InventoryError::SnapshotIncomplete {
                expected: 2,
                observed: 1
            })
        ));

        let invalid = disk("ok", Some("online"), -1);
        assert!(matches!(
            inventory_snapshot(1, 2, info(vec![valid, invalid])),
            Err(InventoryError::SnapshotIncomplete {
                expected: 2,
                observed: 1
            })
        ));
    }

    #[test]
    fn inventory_rejects_slots_outside_the_configured_geometry() {
        for (pool_index, set_index, disk_index) in [(0, 99, 0), (1, 0, 0), (0, 0, 2)] {
            let valid = disk("ok", Some("online"), 0);
            let mut invalid = disk("ok", Some("online"), disk_index);
            invalid.pool_index = pool_index;
            invalid.set_index = set_index;
            let mut info = info(vec![valid, invalid]);
            info.backend.total_sets = vec![1];
            info.backend.drives_per_set = vec![2];

            assert!(matches!(
                inventory_snapshot(1, 2, info),
                Err(InventoryError::SnapshotIncomplete {
                    expected: 2,
                    observed: 1
                })
            ));
        }
    }

    #[test]
    fn inventory_rejects_invalid_or_overflowing_geometry() {
        for (total_sets, drives_per_set) in [
            (Vec::new(), Vec::new()),
            (vec![1], Vec::new()),
            (Vec::new(), vec![1]),
            (vec![1, 1], vec![1]),
            (vec![0], vec![1]),
            (vec![1], vec![0]),
            (vec![1], vec![2]),
            (vec![usize::MAX], vec![2]),
            (vec![usize::MAX, 1], vec![1, 1]),
        ] {
            let mut info = info(vec![disk("ok", Some("online"), 0)]);
            info.backend.total_sets = total_sets;
            info.backend.drives_per_set = drives_per_set;

            assert!(matches!(
                inventory_snapshot(1, 1, info),
                Err(InventoryError::SnapshotIncomplete {
                    expected: 1,
                    observed: 0
                })
            ));
        }
    }

    #[test]
    fn inventory_marks_a_missing_drive_as_offline() {
        let info = info(vec![disk("ok", None, 0), disk("disk not found", None, 1)]);

        assert_eq!(
            inventory_snapshot(1, 2, info).expect("complete inventory should encode"),
            InventorySnapshot::current(1, 2, 200, 80, [InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline])
                .expect("expected inventory should encode")
        );
    }

    #[test]
    fn inventory_health_uses_the_readiness_allow_list() {
        let healthy = info(vec![
            disk("ok", None, 0),
            disk(rustfs_madmin::ITEM_ONLINE, Some("online"), 1),
            disk("unformatted", Some("returning"), 2),
        ]);
        assert_eq!(
            inventory_snapshot(1, 3, healthy).expect("healthy inventory should encode"),
            InventorySnapshot::current(1, 3, 300, 120, []).expect("expected inventory should encode")
        );

        for (state, runtime_state, offline) in [
            ("unknown", None, false),
            ("disk io error", Some("online"), false),
            ("ok", Some("suspect"), false),
            (rustfs_madmin::ITEM_OFFLINE, Some("offline"), true),
        ] {
            let flags = if offline {
                vec![InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline]
            } else {
                vec![InventoryFlag::ClusterDegraded]
            };
            assert_eq!(
                inventory_snapshot(1, 1, info(vec![disk(state, runtime_state, 0)])).expect("degraded inventory should encode"),
                InventorySnapshot::current(1, 1, 100, 40, flags).expect("expected inventory should encode"),
                "state={state}, runtime_state={runtime_state:?}"
            );
        }
    }

    #[test]
    fn inventory_capacity_uses_numeric_indices_without_logging_identifiers() {
        #[derive(Clone, Default)]
        struct CapturedLog(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

        struct CapturedLogWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

        impl std::io::Write for CapturedLogWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("captured log lock").extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLog {
            type Writer = CapturedLogWriter;

            fn make_writer(&'writer self) -> Self::Writer {
                CapturedLogWriter(self.0.clone())
            }
        }

        const ENDPOINT_CANARY: &str = "https://inventory-endpoint-secret.invalid";
        const PATH_CANARY: &str = "/inventory/path/secret";
        let mut first = disk("ok", Some("online"), 0);
        first.endpoint = ENDPOINT_CANARY.to_owned();
        first.drive_path = PATH_CANARY.to_owned();
        let mut second = disk("ok", Some("online"), 1);
        second.endpoint = "https://second-secret.invalid".to_owned();
        second.drive_path = "/second/path/secret".to_owned();
        second.total_space = 900;
        second.available_space = 800;
        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![2],
                total_sets: vec![1],
                drives_per_set: vec![2],
                ..Default::default()
            },
            disks: vec![first, second],
        };
        let captured = CapturedLog::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(captured.clone())
            .finish();
        let snapshot =
            tracing::subscriber::with_default(subscriber, || inventory_snapshot(1, 2, info).expect("inventory should encode"));

        assert_eq!(
            snapshot,
            InventorySnapshot::current(1, 2, 1_000, 840, []).expect("expected inventory should encode")
        );
        let encoded = serde_json::to_string(&snapshot).expect("snapshot JSON");
        let logs = String::from_utf8(captured.0.lock().expect("captured log lock").clone()).expect("UTF-8 logs");
        for canary in [ENDPOINT_CANARY, PATH_CANARY, "second-secret", "/second/path"] {
            assert!(!encoded.contains(canary), "snapshot exposed {canary}");
            assert!(!logs.contains(canary), "logs exposed {canary}");
        }
    }

    #[test]
    fn inventory_capacity_rejects_invalid_numeric_topology() {
        let mut invalid = disk("ok", Some("online"), -1);
        invalid.pool_index = -1;

        assert!(matches!(
            inventory_capacity(&info(vec![invalid])),
            Err(InventoryError::SnapshotIncomplete {
                expected: 1,
                observed: 0
            })
        ));
    }

    #[test]
    fn inventory_capacity_falls_back_to_unique_numeric_topology() {
        let mut pool_zero = disk("ok", Some("online"), 0);
        pool_zero.total_space = 100;
        pool_zero.available_space = 40;
        let mut pool_one = disk("ok", Some("online"), 0);
        pool_one.pool_index = 1;
        pool_one.set_index = 2;
        pool_one.total_space = 200;
        pool_one.available_space = 80;
        let empty_widths = rustfs_madmin::StorageInfo {
            disks: vec![pool_zero.clone(), pool_one.clone()],
            ..Default::default()
        };
        assert_eq!(inventory_capacity(&empty_widths).expect("numeric fallback"), (300, 120));

        let incomplete_widths = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1],
                ..Default::default()
            },
            disks: vec![pool_zero, pool_one],
        };
        assert_eq!(inventory_capacity(&incomplete_widths).expect("numeric fallback"), (300, 120));
    }

    #[test]
    fn inventory_capacity_aggregates_multiple_pools_and_sets_once() {
        let mut pool_zero_set_zero = disk("ok", Some("online"), 0);
        pool_zero_set_zero.total_space = 100;
        pool_zero_set_zero.available_space = 40;
        let mut pool_zero_set_one = pool_zero_set_zero.clone();
        pool_zero_set_one.set_index = 1;
        let mut pool_one = pool_zero_set_zero.clone();
        pool_one.pool_index = 1;
        pool_one.total_space = 200;
        pool_one.available_space = 80;
        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1, 1],
                ..Default::default()
            },
            disks: vec![pool_zero_set_zero, pool_zero_set_one, pool_one],
        };

        assert_eq!(inventory_capacity(&info).expect("configured topology"), (400, 160));
    }

    #[test]
    fn inventory_capacity_overflow_is_rejected() {
        let mut first = disk("ok", Some("online"), 0);
        first.total_space = u64::MAX;
        let mut second = disk("ok", Some("online"), 1);
        second.total_space = 1;
        let info = rustfs_madmin::StorageInfo {
            disks: vec![first, second],
            ..Default::default()
        };

        assert!(matches!(inventory_capacity(&info), Err(InventoryError::Capacity)));
    }
}
