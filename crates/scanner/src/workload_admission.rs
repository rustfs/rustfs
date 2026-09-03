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

use std::sync::{Arc, LazyLock, RwLock};

use rustfs_concurrency::{AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshotProvider, WorkloadClass};

type WorkloadSnapshotProviderRef = Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>;

static SCANNER_WORKLOAD_ADMISSION_PROVIDER: LazyLock<RwLock<Option<WorkloadSnapshotProviderRef>>> =
    LazyLock::new(|| RwLock::new(None));

pub fn set_scanner_workload_admission_snapshot_provider(provider: WorkloadSnapshotProviderRef) {
    *SCANNER_WORKLOAD_ADMISSION_PROVIDER
        .write()
        .unwrap_or_else(|err| err.into_inner()) = Some(provider);
}

fn scanner_workload_admission_snapshot_provider() -> Option<WorkloadSnapshotProviderRef> {
    SCANNER_WORKLOAD_ADMISSION_PROVIDER
        .read()
        .unwrap_or_else(|err| err.into_inner())
        .clone()
}

#[cfg(test)]
pub(crate) fn clear_scanner_workload_admission_snapshot_provider_for_test() {
    *SCANNER_WORKLOAD_ADMISSION_PROVIDER
        .write()
        .unwrap_or_else(|err| err.into_inner()) = None;
}

pub(crate) fn foreground_workload_activity() -> u64 {
    let local_activity = crate::current_foreground_read_activity();
    let Some(provider) = scanner_workload_admission_snapshot_provider() else {
        return local_activity;
    };

    local_activity.max(foreground_activity_from_snapshot(&provider.workload_admission_snapshot()))
}

fn foreground_activity_from_snapshot(snapshot: &WorkloadAdmissionRegistrySnapshot) -> u64 {
    [WorkloadClass::ForegroundRead, WorkloadClass::ForegroundWrite]
        .into_iter()
        .filter_map(|class| snapshot.get(class))
        .map(|entry| {
            entry
                .active
                .or_else(|| {
                    matches!(entry.state, AdmissionState::Saturated).then(|| entry.limit.filter(|limit| *limit > 0).unwrap_or(1))
                })
                .map(usize_to_u64_saturated)
                .unwrap_or(0)
        })
        .max()
        .unwrap_or(0)
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_concurrency::{AdmissionState, WorkloadAdmissionSnapshot};
    use serial_test::serial;

    #[derive(Clone)]
    struct FixedWorkloadProvider {
        snapshot: WorkloadAdmissionRegistrySnapshot,
    }

    impl WorkloadAdmissionSnapshotProvider for FixedWorkloadProvider {
        fn workload_admission_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot {
            self.snapshot.clone()
        }
    }

    fn install_provider(snapshot: WorkloadAdmissionRegistrySnapshot) {
        set_scanner_workload_admission_snapshot_provider(Arc::new(FixedWorkloadProvider { snapshot }));
    }

    #[test]
    #[serial]
    fn foreground_workload_activity_falls_back_to_local_read_activity() {
        clear_scanner_workload_admission_snapshot_provider_for_test();
        crate::reset_foreground_read_activity_for_test();
        crate::set_foreground_read_activity(3);

        assert_eq!(foreground_workload_activity(), 3);

        crate::reset_foreground_read_activity_for_test();
    }

    #[test]
    #[serial]
    fn foreground_workload_activity_uses_shared_provider_counts() {
        clear_scanner_workload_admission_snapshot_provider_for_test();
        crate::reset_foreground_read_activity_for_test();
        install_provider(WorkloadAdmissionRegistrySnapshot::new(vec![
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Open).with_counts(
                Some(5),
                None,
                Some(8),
            ),
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundWrite, AdmissionState::Open).with_counts(
                Some(2),
                None,
                Some(4),
            ),
        ]));

        assert_eq!(foreground_workload_activity(), 5);

        clear_scanner_workload_admission_snapshot_provider_for_test();
    }

    #[test]
    #[serial]
    fn foreground_workload_activity_treats_saturation_without_counts_as_pressure() {
        clear_scanner_workload_admission_snapshot_provider_for_test();
        crate::reset_foreground_read_activity_for_test();
        install_provider(WorkloadAdmissionRegistrySnapshot::new(vec![
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Saturated).with_counts(
                None,
                None,
                Some(7),
            ),
        ]));

        assert_eq!(foreground_workload_activity(), 7);

        clear_scanner_workload_admission_snapshot_provider_for_test();
    }

    #[test]
    #[serial]
    fn foreground_workload_activity_treats_zero_limit_saturation_as_pressure() {
        clear_scanner_workload_admission_snapshot_provider_for_test();
        crate::reset_foreground_read_activity_for_test();
        install_provider(WorkloadAdmissionRegistrySnapshot::new(vec![
            WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundWrite, AdmissionState::Saturated).with_counts(
                None,
                None,
                Some(0),
            ),
        ]));

        assert_eq!(foreground_workload_activity(), 1);

        clear_scanner_workload_admission_snapshot_provider_for_test();
    }
}
