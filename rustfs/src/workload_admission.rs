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

use crate::runtime_sources::resolve_replication_pool_handle;
use crate::storage_api::{bucket_metadata_runtime_initialized, replication_queue_current_count};
use rustfs_concurrency::{
    AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadAdmissionSnapshotProvider,
    WorkloadClass,
};

use crate::storage_api::concurrency::get_concurrency_manager;

const BUCKET_METADATA_RUNTIME_NOT_INITIALIZED: &str = "bucket metadata runtime not initialized";
const FOREGROUND_WRITE_NOT_EXPOSED_BY_PROVIDER: &str = "foreground write admission not yet exposed by RustFS runtime";
const HEAL_MANAGER_NOT_INITIALIZED: &str = "heal manager not initialized";
const REPAIR_QUEUE_BACKLOG_PRESENT: &str = "repair queue has pending work";
const REPLICATION_RUNTIME_NOT_INITIALIZED: &str = "replication runtime not initialized";
const REPLICATION_QUEUE_BACKLOG_PRESENT: &str = "replication queue has pending work";
const REPLICATION_QUEUE_STATS_UNAVAILABLE: &str = "replication queue stats unavailable";
const SCANNER_ADMISSION_DISABLED: &str = "scanner admission disabled because max concurrent set scans is zero";
const SCANNER_ADMISSION_SATURATED: &str = "scanner active work reached configured set-scan limit";
const SCANNER_ACTIVITY_IDLE_OR_NOT_INITIALIZED: &str = "scanner activity idle or not initialized";
const STORAGE_CONCURRENCY_PROVIDER_MISSING_FOREGROUND_READ: &str =
    "storage concurrency provider did not expose foreground read admission";

#[derive(Debug, Default, Clone, Copy)]
pub struct RustFsWorkloadAdmissionSnapshotProvider;

impl WorkloadAdmissionSnapshotProvider for RustFsWorkloadAdmissionSnapshotProvider {
    fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
        workload_admission_registry_snapshot()
    }
}

pub fn workload_admission_registry_snapshot() -> WorkloadAdmissionRegistrySnapshot {
    storage_concurrency_workload_admission_snapshot().overlay(runtime_owner_workload_admission_registry_snapshot())
}

fn storage_concurrency_workload_admission_snapshot() -> WorkloadAdmissionRegistrySnapshot {
    let storage_provider: &dyn WorkloadAdmissionSnapshotProvider = get_concurrency_manager();
    storage_provider.workload_admission_snapshot()
}

fn runtime_owner_workload_admission_registry_snapshot() -> WorkloadAdmissionRegistrySnapshot {
    let entries = vec![
        foreground_write_workload_admission_snapshot(),
        metadata_workload_admission_snapshot(),
        scanner_workload_admission_snapshot(),
        repair_workload_admission_snapshot(),
        replication_workload_admission_snapshot(),
    ];

    WorkloadAdmissionRegistrySnapshot::new(entries)
}

pub fn foreground_read_workload_admission_snapshot() -> WorkloadAdmissionSnapshot {
    storage_concurrency_workload_admission_snapshot()
        .get(WorkloadClass::ForegroundRead)
        .cloned()
        .unwrap_or_else(|| {
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Unknown)
                .with_reason(STORAGE_CONCURRENCY_PROVIDER_MISSING_FOREGROUND_READ)
        })
}

pub fn foreground_write_workload_admission_snapshot() -> WorkloadAdmissionSnapshot {
    WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundWrite, AdmissionState::Disabled)
        .with_counts(Some(0), None, Some(0))
        .with_reason(FOREGROUND_WRITE_NOT_EXPOSED_BY_PROVIDER)
}

pub fn metadata_workload_admission_snapshot() -> WorkloadAdmissionSnapshot {
    metadata_workload_admission_snapshot_from_initialized(bucket_metadata_runtime_initialized())
}

fn metadata_workload_admission_snapshot_from_initialized(runtime_initialized: bool) -> WorkloadAdmissionSnapshot {
    let state = if runtime_initialized {
        AdmissionState::Open
    } else {
        AdmissionState::Unknown
    };

    let snapshot = WorkloadAdmissionSnapshot::new(WorkloadClass::Metadata, state);

    if runtime_initialized {
        snapshot
    } else {
        snapshot.with_reason(BUCKET_METADATA_RUNTIME_NOT_INITIALIZED)
    }
}

pub fn scanner_workload_admission_snapshot() -> WorkloadAdmissionSnapshot {
    let runtime_config = rustfs_scanner::scanner_runtime_config_status();
    scanner_workload_admission_snapshot_from_activity(
        rustfs_scanner::current_scanner_activity(),
        runtime_config.max_concurrent_set_scans.value,
    )
}

fn scanner_workload_admission_snapshot_from_activity(active: u64, limit: usize) -> WorkloadAdmissionSnapshot {
    let state = if limit == 0 {
        AdmissionState::Disabled
    } else if usize::try_from(active).ok().is_some_and(|active| active >= limit) {
        AdmissionState::Saturated
    } else if active > 0 {
        AdmissionState::Open
    } else {
        AdmissionState::Unknown
    };

    let snapshot = WorkloadAdmissionSnapshot::new(WorkloadClass::Scanner, state).with_counts(
        Some(u64_to_usize_saturated(active)),
        None,
        Some(limit),
    );

    match state {
        AdmissionState::Disabled => snapshot.with_reason(SCANNER_ADMISSION_DISABLED),
        AdmissionState::Saturated => snapshot.with_reason(SCANNER_ADMISSION_SATURATED),
        AdmissionState::Unknown => snapshot.with_reason(SCANNER_ACTIVITY_IDLE_OR_NOT_INITIALIZED),
        _ => snapshot,
    }
}

pub fn repair_workload_admission_snapshot() -> WorkloadAdmissionSnapshot {
    repair_workload_admission_snapshot_from_counts(
        rustfs_heal::get_heal_manager().is_some(),
        rustfs_heal::current_heal_active_tasks(),
        rustfs_heal::current_heal_queue_length(),
    )
}

fn repair_workload_admission_snapshot_from_counts(
    manager_initialized: bool,
    active: u64,
    queued: u64,
) -> WorkloadAdmissionSnapshot {
    let state = if !manager_initialized && active == 0 && queued == 0 {
        AdmissionState::Unknown
    } else if queued > 0 {
        AdmissionState::Throttled
    } else if manager_initialized || active > 0 {
        AdmissionState::Open
    } else {
        AdmissionState::Unknown
    };

    let snapshot = WorkloadAdmissionSnapshot::new(WorkloadClass::Repair, state).with_counts(
        Some(u64_to_usize_saturated(active)),
        Some(u64_to_usize_saturated(queued)),
        None,
    );

    match state {
        AdmissionState::Unknown => snapshot.with_reason(HEAL_MANAGER_NOT_INITIALIZED),
        AdmissionState::Throttled => snapshot.with_reason(REPAIR_QUEUE_BACKLOG_PRESENT),
        _ => snapshot,
    }
}

pub fn replication_workload_admission_snapshot() -> WorkloadAdmissionSnapshot {
    let Some(pool) = resolve_replication_pool_handle() else {
        return replication_workload_admission_snapshot_from_counts(false, None, None);
    };

    let active = pool
        .active_workers()
        .saturating_add(pool.active_lrg_workers())
        .saturating_add(pool.active_mrf_workers());
    let queued = replication_queue_current_count().map(i64_to_usize_saturated);

    replication_workload_admission_snapshot_from_counts(true, Some(i32_to_usize_saturated(active)), queued)
}

fn replication_workload_admission_snapshot_from_counts(
    runtime_initialized: bool,
    active: Option<usize>,
    queued: Option<usize>,
) -> WorkloadAdmissionSnapshot {
    let state = if !runtime_initialized {
        AdmissionState::Unknown
    } else if queued.is_some_and(|queued| queued > 0) {
        AdmissionState::Throttled
    } else if queued.is_some() {
        AdmissionState::Open
    } else {
        AdmissionState::Unknown
    };

    let snapshot = WorkloadAdmissionSnapshot::new(WorkloadClass::Replication, state).with_counts(active, queued, None);

    match state {
        AdmissionState::Unknown if !runtime_initialized => snapshot.with_reason(REPLICATION_RUNTIME_NOT_INITIALIZED),
        AdmissionState::Unknown => snapshot.with_reason(REPLICATION_QUEUE_STATS_UNAVAILABLE),
        AdmissionState::Throttled => snapshot.with_reason(REPLICATION_QUEUE_BACKLOG_PRESENT),
        _ => snapshot,
    }
}

fn u64_to_usize_saturated(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn i64_to_usize_saturated(value: i64) -> usize {
    usize::try_from(value.max(0)).unwrap_or(usize::MAX)
}

fn i32_to_usize_saturated(value: i32) -> usize {
    usize::try_from(value.max(0)).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreground_read_snapshot_uses_storage_concurrency_provider_contract() {
        let snapshot = foreground_read_workload_admission_snapshot();
        let storage_snapshot = storage_concurrency_workload_admission_snapshot()
            .get(WorkloadClass::ForegroundRead)
            .cloned();

        assert_eq!(Some(snapshot.clone()), storage_snapshot);
        assert_eq!(snapshot.class, WorkloadClass::ForegroundRead);
        assert_ne!(snapshot.state, AdmissionState::Unknown);
        assert!(snapshot.active.is_some());
        assert!(snapshot.limit.is_some());
    }

    #[test]
    fn metadata_snapshot_reports_initialized_runtime() {
        let snapshot = metadata_workload_admission_snapshot_from_initialized(true);

        assert_eq!(snapshot.class, WorkloadClass::Metadata);
        assert_eq!(snapshot.state, AdmissionState::Open);
        assert_eq!(snapshot.active, None);
        assert_eq!(snapshot.queued, None);
        assert_eq!(snapshot.limit, None);
        assert_eq!(snapshot.reason, None);
    }

    #[test]
    fn metadata_snapshot_is_unknown_before_runtime_initializes() {
        let snapshot = metadata_workload_admission_snapshot_from_initialized(false);

        assert_eq!(snapshot.class, WorkloadClass::Metadata);
        assert_eq!(snapshot.state, AdmissionState::Unknown);
        assert_eq!(snapshot.reason.as_deref(), Some(BUCKET_METADATA_RUNTIME_NOT_INITIALIZED));
    }

    #[test]
    fn foreground_write_snapshot_reports_explicit_gap_reason() {
        let snapshot = foreground_write_workload_admission_snapshot();

        assert_eq!(snapshot.class, WorkloadClass::ForegroundWrite);
        assert_eq!(snapshot.state, AdmissionState::Disabled);
        assert_eq!(snapshot.active, Some(0));
        assert_eq!(snapshot.limit, Some(0));
        assert_eq!(snapshot.reason.as_deref(), Some(FOREGROUND_WRITE_NOT_EXPOSED_BY_PROVIDER));
    }

    #[test]
    fn scanner_snapshot_reports_active_work_units() {
        let snapshot = scanner_workload_admission_snapshot_from_activity(5, 8);

        assert_eq!(snapshot.class, WorkloadClass::Scanner);
        assert_eq!(snapshot.state, AdmissionState::Open);
        assert_eq!(snapshot.active, Some(5));
        assert_eq!(snapshot.queued, None);
        assert_eq!(snapshot.limit, Some(8));
        assert_eq!(snapshot.reason, None);
    }

    #[test]
    fn scanner_snapshot_is_unknown_when_idle_or_uninitialized() {
        let snapshot = scanner_workload_admission_snapshot_from_activity(0, 8);

        assert_eq!(snapshot.class, WorkloadClass::Scanner);
        assert_eq!(snapshot.state, AdmissionState::Unknown);
        assert_eq!(snapshot.active, Some(0));
        assert_eq!(snapshot.limit, Some(8));
        assert_eq!(snapshot.reason.as_deref(), Some(SCANNER_ACTIVITY_IDLE_OR_NOT_INITIALIZED));
    }

    #[test]
    fn scanner_snapshot_reports_disabled_when_set_scan_limit_is_zero() {
        let snapshot = scanner_workload_admission_snapshot_from_activity(0, 0);

        assert_eq!(snapshot.class, WorkloadClass::Scanner);
        assert_eq!(snapshot.state, AdmissionState::Disabled);
        assert_eq!(snapshot.limit, Some(0));
        assert_eq!(snapshot.reason.as_deref(), Some(SCANNER_ADMISSION_DISABLED));
    }

    #[test]
    fn scanner_snapshot_reports_saturation_when_active_work_reaches_limit() {
        let snapshot = scanner_workload_admission_snapshot_from_activity(4, 4);

        assert_eq!(snapshot.class, WorkloadClass::Scanner);
        assert_eq!(snapshot.state, AdmissionState::Saturated);
        assert_eq!(snapshot.limit, Some(4));
        assert_eq!(snapshot.reason.as_deref(), Some(SCANNER_ADMISSION_SATURATED));
    }

    #[test]
    fn repair_snapshot_reports_heal_counters() {
        let snapshot = repair_workload_admission_snapshot_from_counts(true, 2, 3);

        assert_eq!(snapshot.class, WorkloadClass::Repair);
        assert_eq!(snapshot.state, AdmissionState::Throttled);
        assert_eq!(snapshot.active, Some(2));
        assert_eq!(snapshot.queued, Some(3));
        assert_eq!(snapshot.limit, None);
        assert_eq!(snapshot.reason.as_deref(), Some(REPAIR_QUEUE_BACKLOG_PRESENT));
    }

    #[test]
    fn repair_snapshot_is_unknown_before_heal_manager_initializes() {
        let snapshot = repair_workload_admission_snapshot_from_counts(false, 0, 0);

        assert_eq!(snapshot.class, WorkloadClass::Repair);
        assert_eq!(snapshot.state, AdmissionState::Unknown);
        assert_eq!(snapshot.active, Some(0));
        assert_eq!(snapshot.queued, Some(0));
        assert_eq!(snapshot.reason.as_deref(), Some(HEAL_MANAGER_NOT_INITIALIZED));
    }

    #[test]
    fn replication_snapshot_reports_worker_and_queue_counts() {
        let snapshot = replication_workload_admission_snapshot_from_counts(true, Some(4), Some(9));

        assert_eq!(snapshot.class, WorkloadClass::Replication);
        assert_eq!(snapshot.state, AdmissionState::Throttled);
        assert_eq!(snapshot.active, Some(4));
        assert_eq!(snapshot.queued, Some(9));
        assert_eq!(snapshot.limit, None);
        assert_eq!(snapshot.reason.as_deref(), Some(REPLICATION_QUEUE_BACKLOG_PRESENT));
    }

    #[test]
    fn replication_snapshot_is_unknown_before_runtime_initializes() {
        let snapshot = replication_workload_admission_snapshot_from_counts(false, None, None);

        assert_eq!(snapshot.class, WorkloadClass::Replication);
        assert_eq!(snapshot.state, AdmissionState::Unknown);
        assert_eq!(snapshot.active, None);
        assert_eq!(snapshot.queued, None);
        assert_eq!(snapshot.reason.as_deref(), Some(REPLICATION_RUNTIME_NOT_INITIALIZED));
    }

    #[test]
    fn replication_snapshot_is_unknown_when_queue_stats_are_unavailable() {
        let snapshot = replication_workload_admission_snapshot_from_counts(true, Some(2), None);

        assert_eq!(snapshot.class, WorkloadClass::Replication);
        assert_eq!(snapshot.state, AdmissionState::Unknown);
        assert_eq!(snapshot.active, Some(2));
        assert_eq!(snapshot.queued, None);
        assert_eq!(snapshot.reason.as_deref(), Some(REPLICATION_QUEUE_STATS_UNAVAILABLE));
    }

    #[test]
    fn provider_covers_required_classes() {
        let provider = RustFsWorkloadAdmissionSnapshotProvider;
        let registry = provider.workload_admission_snapshot();

        assert_eq!(registry.entries().len(), WorkloadClass::REQUIRED.len());
        for class in WorkloadClass::REQUIRED {
            assert!(registry.get(class).is_some(), "missing workload class {class}");
        }

        assert!(registry.get(WorkloadClass::Repair).is_some());
        assert!(registry.get(WorkloadClass::Replication).is_some());
        assert_ne!(
            registry.get(WorkloadClass::ForegroundRead).map(|snapshot| snapshot.state),
            Some(AdmissionState::Unknown)
        );
        assert_eq!(registry.get(WorkloadClass::Scanner).and_then(|snapshot| snapshot.active), Some(0));
    }

    #[test]
    fn provider_overlays_runtime_owner_snapshots_on_storage_registry() {
        let storage_registry = storage_concurrency_workload_admission_snapshot();
        let registry = workload_admission_registry_snapshot();

        assert_eq!(
            registry.get(WorkloadClass::ForegroundRead),
            storage_registry.get(WorkloadClass::ForegroundRead)
        );
        assert_eq!(registry.get(WorkloadClass::Metadata), Some(&metadata_workload_admission_snapshot()));
        assert_eq!(registry.get(WorkloadClass::Scanner), Some(&scanner_workload_admission_snapshot()));
        assert_eq!(
            registry
                .get(WorkloadClass::ForegroundWrite)
                .map(|snapshot| (snapshot.state, snapshot.reason.as_deref())),
            Some((AdmissionState::Disabled, Some(FOREGROUND_WRITE_NOT_EXPOSED_BY_PROVIDER)))
        );
    }
}
