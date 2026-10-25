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

use std::sync::{Arc, LazyLock, OnceLock, RwLock};

use rustfs_concurrency::adaptive::permit_set::{AdaptivePermitSet, NodeResourcePermit};
use rustfs_concurrency::adaptive::{NodeAdmission, NodePermit};
use rustfs_concurrency::{AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshotProvider, WorkloadClass};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::ScannerError;
use crate::storage_api::owner::{PhysicalDiskBinding, PhysicalDiskPlan, ecstore_with_background_resource_set};

static SCANNER_NODE_ADMISSION: OnceLock<Arc<NodeAdmission>> = OnceLock::new();

tokio::task_local! {
    static SCANNER_NODE_REQUESTS: ScannerNodeRequestSender;
    static SCANNER_PHYSICAL_DISK: Option<Arc<PhysicalDiskBinding>>;
    static SCANNER_METADATA_UNITS: AdaptivePermitSet;
}

pub(crate) struct ScannerNodeRequest {
    pub(crate) owner: Arc<NodeAdmission>,
    pub(crate) physical: Option<Arc<PhysicalDiskPlan>>,
    pub(crate) respond: oneshot::Sender<Result<ScannerNodePermit, ScannerError>>,
    pub(crate) cancel: CancellationToken,
}

pub(crate) enum ScannerNodePermit {
    Node(NodePermit),
    Physical {
        permit: NodeResourcePermit,
        plan: Arc<PhysicalDiskPlan>,
    },
}

impl ScannerNodePermit {
    pub(crate) fn complete(self) {
        match self {
            Self::Node(permit) => permit.complete(),
            Self::Physical { permit, .. } => permit.complete(),
        }
    }
}

pub(crate) async fn with_scanner_physical_disk<F: std::future::Future>(
    binding: Option<Arc<PhysicalDiskBinding>>,
    operation: F,
) -> F::Output {
    SCANNER_PHYSICAL_DISK.scope(binding, operation).await
}

pub(crate) type ScannerNodeRequestSender = mpsc::Sender<ScannerNodeRequest>;
pub(crate) type ScannerNodeRequestReceiver = mpsc::Receiver<ScannerNodeRequest>;

/// One suspended operation per walk; disabled admission allocates no channel.
pub(crate) fn scanner_node_request_channel() -> (Option<ScannerNodeRequestSender>, Option<ScannerNodeRequestReceiver>) {
    if scanner_node_admission().is_none() {
        return (None, None);
    }
    let (sender, receiver) = mpsc::channel(1);
    (Some(sender), Some(receiver))
}

pub(crate) async fn with_scanner_node_requests<F: std::future::Future>(
    sender: Option<ScannerNodeRequestSender>,
    operation: F,
) -> F::Output {
    match sender {
        Some(sender) => SCANNER_NODE_REQUESTS.scope(sender, operation).await,
        None => operation.await,
    }
}

#[cfg(test)]
tokio::task_local! {
    pub(crate) static TEST_SCANNER_NODE_ADMISSION: Arc<NodeAdmission>;
}

/// Install the heal manager's node owner before starting scanner workers.
/// Refuse replacement: existing scans must not outlive an independently renewed owner.
pub fn set_scanner_node_admission(owner: Arc<NodeAdmission>) -> bool {
    SCANNER_NODE_ADMISSION.set(owner).is_ok()
}

pub(crate) fn scanner_node_admission() -> Option<Arc<NodeAdmission>> {
    #[cfg(test)]
    if let Ok(owner) = TEST_SCANNER_NODE_ADMISSION.try_with(Arc::clone) {
        return Some(owner);
    }
    SCANNER_NODE_ADMISSION.get().cloned()
}

/// Try the fast path first. A scoped coordinator releases the cache lease before
/// joining the node queue and validates the cache before returning a permit.
/// Uncoordinated callers defer instead of parking under an unknown lock.
pub(crate) async fn run_scanner_node_operation<T, E>(
    owner: Option<&Arc<NodeAdmission>>,
    cancel: &CancellationToken,
    operation: impl std::future::Future<Output = Result<T, E>>,
) -> Result<Result<T, E>, ScannerError> {
    run_scanner_operation(owner, cancel, operation, false).await
}

/// Metadata parsing may subsequently dispatch lifecycle work on other disks.
/// Only its local metadata read inherits the scanner's physical resource set.
pub(crate) async fn run_scanner_metadata_operation<T, E>(
    owner: Option<&Arc<NodeAdmission>>,
    cancel: &CancellationToken,
    operation: impl std::future::Future<Output = Result<T, E>>,
) -> Result<Result<T, E>, ScannerError> {
    run_scanner_operation(owner, cancel, operation, true).await
}

pub(crate) async fn with_scanner_metadata_io<F: std::future::Future>(operation: F) -> F::Output {
    match SCANNER_METADATA_UNITS.try_with(Clone::clone) {
        Ok(units) => ecstore_with_background_resource_set(units, operation).await,
        Err(_) => operation.await,
    }
}

async fn run_scanner_operation<T, E>(
    owner: Option<&Arc<NodeAdmission>>,
    cancel: &CancellationToken,
    operation: impl std::future::Future<Output = Result<T, E>>,
    metadata: bool,
) -> Result<Result<T, E>, ScannerError> {
    let Some(owner) = owner else {
        return Ok(operation.await);
    };
    if cancel.is_cancelled() {
        return Err(ScannerError::Other("Operation cancelled".to_string()));
    }
    let physical = SCANNER_PHYSICAL_DISK.try_with(|binding| binding.as_ref().and_then(|binding| binding.plan()));
    let physical = match physical {
        Ok(Some(plan)) => Some(plan),
        Ok(None) => return Err(ScannerError::ResourceDeferred),
        Err(_) => None,
    };
    let available = match &physical {
        Some(plan) => plan.try_acquire(owner).map(|permit| ScannerNodePermit::Physical {
            permit,
            plan: plan.clone(),
        }),
        None => owner.try_acquire().map(ScannerNodePermit::Node),
    };
    let permit = match available {
        Some(permit) => permit,
        None => {
            let sender = SCANNER_NODE_REQUESTS
                .try_with(Clone::clone)
                .map_err(|_| ScannerError::ResourceDeferred)?;
            let acquire = async {
                let (respond, response) = oneshot::channel();
                sender
                    .send(ScannerNodeRequest {
                        owner: owner.clone(),
                        physical,
                        respond,
                        cancel: cancel.clone(),
                    })
                    .await
                    .map_err(|_| ScannerError::ResourceDeferred)?;
                response.await.map_err(|_| ScannerError::ResourceDeferred)?
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(ScannerError::Other("Operation cancelled".to_string())),
                permit = acquire => permit?,
            }
        }
    };
    let result = match &permit {
        ScannerNodePermit::Node(_) => operation.await,
        ScannerNodePermit::Physical { permit, plan } => {
            if !plan.is_current() {
                return Err(ScannerError::ResourceDeferred);
            }
            if metadata {
                SCANNER_METADATA_UNITS.scope(permit.resources().clone(), operation).await
            } else {
                ecstore_with_background_resource_set(permit.resources().clone(), operation).await
            }
        }
    };
    if result.is_ok() {
        permit.complete();
    }
    Ok(result)
}

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
