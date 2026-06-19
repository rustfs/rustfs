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

use rustfs_concurrency::{
    AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadAdmissionSnapshotProvider,
    WorkloadClass,
};

const HEAL_MANAGER_NOT_INITIALIZED: &str = "heal manager not initialized";
const NOT_EXPOSED_BY_PROVIDER: &str = "not exposed by RustFS workload admission provider";

#[derive(Debug, Default, Clone, Copy)]
pub struct RustFsWorkloadAdmissionSnapshotProvider;

impl WorkloadAdmissionSnapshotProvider for RustFsWorkloadAdmissionSnapshotProvider {
    fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
        workload_admission_registry_snapshot()
    }
}

pub fn workload_admission_registry_snapshot() -> WorkloadAdmissionRegistrySnapshot {
    let entries = WorkloadClass::REQUIRED
        .iter()
        .copied()
        .map(|class| match class {
            WorkloadClass::Repair => repair_workload_admission_snapshot(),
            class => WorkloadAdmissionSnapshot::new(class, AdmissionState::Unknown).with_reason(NOT_EXPOSED_BY_PROVIDER),
        })
        .collect();

    WorkloadAdmissionRegistrySnapshot::new(entries)
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
    let state = if manager_initialized || active > 0 || queued > 0 {
        AdmissionState::Open
    } else {
        AdmissionState::Unknown
    };

    let snapshot = WorkloadAdmissionSnapshot::new(WorkloadClass::Repair, state).with_counts(
        Some(u64_to_usize_saturated(active)),
        Some(u64_to_usize_saturated(queued)),
        None,
    );

    if state == AdmissionState::Unknown {
        snapshot.with_reason(HEAL_MANAGER_NOT_INITIALIZED)
    } else {
        snapshot
    }
}

fn u64_to_usize_saturated(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_snapshot_reports_heal_counters() {
        let snapshot = repair_workload_admission_snapshot_from_counts(true, 2, 3);

        assert_eq!(snapshot.class, WorkloadClass::Repair);
        assert_eq!(snapshot.state, AdmissionState::Open);
        assert_eq!(snapshot.active, Some(2));
        assert_eq!(snapshot.queued, Some(3));
        assert_eq!(snapshot.limit, None);
        assert_eq!(snapshot.reason, None);
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
    fn provider_covers_required_classes() {
        let provider = RustFsWorkloadAdmissionSnapshotProvider;
        let registry = provider.workload_admission_snapshot();

        assert_eq!(registry.entries().len(), WorkloadClass::REQUIRED.len());
        for class in WorkloadClass::REQUIRED {
            assert!(registry.get(class).is_some(), "missing workload class {class}");
        }

        assert!(registry.get(WorkloadClass::Repair).is_some());
        assert_eq!(
            registry.get(WorkloadClass::ForegroundRead).map(|snapshot| snapshot.state),
            Some(AdmissionState::Unknown)
        );
    }
}
