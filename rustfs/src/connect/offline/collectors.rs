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

//! Fixed Q07 L0/L1 collectors for an operator-triggered offline diagnostic.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};
use sysinfo::{Disks, Networks, RefreshKind, System};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::super::inventory::{InventoryError, InventorySnapshot, InventoryStateStore};
use super::manifest_entry::ManifestEntry;
use super::redaction::RedactionError;

const COLLECT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ENTRY_BYTES: usize = 16 * 1024;
static SYSTEM_SCAN_PERMIT: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum DataClassification {
    L0,
    L1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OfflineCollector {
    RustfsVersion,
    NodeCount,
    DriveCount,
    CapacityUsedBytes,
    CapacityTotalBytes,
    CoarseHealthFlags,
    OsSummary,
    KernelSummary,
    CpuSummary,
    MemorySummary,
    FilesystemSummary,
    NetworkSummary,
}

const COLLECTORS: [OfflineCollector; 12] = [
    OfflineCollector::RustfsVersion,
    OfflineCollector::NodeCount,
    OfflineCollector::DriveCount,
    OfflineCollector::CapacityUsedBytes,
    OfflineCollector::CapacityTotalBytes,
    OfflineCollector::CoarseHealthFlags,
    OfflineCollector::OsSummary,
    OfflineCollector::KernelSummary,
    OfflineCollector::CpuSummary,
    OfflineCollector::MemorySummary,
    OfflineCollector::FilesystemSummary,
    OfflineCollector::NetworkSummary,
];

impl OfflineCollector {
    pub const fn field_id(self) -> &'static str {
        match self {
            Self::RustfsVersion => "offline.rustfsVersion",
            Self::NodeCount => "offline.nodeCount",
            Self::DriveCount => "offline.driveCount",
            Self::CapacityUsedBytes => "offline.capacityUsedBytes",
            Self::CapacityTotalBytes => "offline.capacityTotalBytes",
            Self::CoarseHealthFlags => "offline.coarseHealthFlags",
            Self::OsSummary => "offline.osSummary",
            Self::KernelSummary => "offline.kernelSummary",
            Self::CpuSummary => "offline.cpuSummary",
            Self::MemorySummary => "offline.memorySummary",
            Self::FilesystemSummary => "offline.filesystemSummary",
            Self::NetworkSummary => "offline.networkSummary",
        }
    }

    pub const fn classification(self) -> DataClassification {
        match self {
            Self::RustfsVersion
            | Self::NodeCount
            | Self::DriveCount
            | Self::CapacityUsedBytes
            | Self::CapacityTotalBytes
            | Self::CoarseHealthFlags => DataClassification::L0,
            Self::OsSummary
            | Self::KernelSummary
            | Self::CpuSummary
            | Self::MemorySummary
            | Self::FilesystemSummary
            | Self::NetworkSummary => DataClassification::L1,
        }
    }

    pub const fn max_entry_bytes(self) -> usize {
        MAX_ENTRY_BYTES
    }

    pub(crate) const fn timeout(self) -> Duration {
        COLLECT_TIMEOUT
    }

    pub(crate) fn field_name(self) -> &'static str {
        self.field_id().split_once('.').expect("collector field ids are frozen").1
    }

    fn value(self, inventory: &InventorySnapshot, system: &SystemSnapshot) -> Value {
        match self {
            Self::RustfsVersion => json!(inventory.rustfs_version()),
            Self::NodeCount => json!(inventory.node_count()),
            Self::DriveCount => json!(inventory.drive_count()),
            Self::CapacityUsedBytes => json!(inventory.capacity_used_bytes()),
            Self::CapacityTotalBytes => json!(inventory.capacity_total_bytes()),
            Self::CoarseHealthFlags => json!(inventory.coarse_flags()),
            Self::OsSummary => json!(system.os_summary),
            Self::KernelSummary => json!(system.kernel_summary),
            Self::CpuSummary => json!({ "architecture": system.architecture, "cores": system.cores }),
            Self::MemorySummary => json!({
                "totalBytes": system.total_memory_bytes,
                "underPressure": system.under_memory_pressure,
            }),
            Self::FilesystemSummary => json!(system.filesystem_types),
            Self::NetworkSummary => json!({
                "bondCount": system.bond_count,
                "interfaceCount": system.interface_count,
            }),
        }
    }
}

#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("offline diagnostic collection was cancelled")]
    Cancelled,
    #[error("offline diagnostic collection exceeded its 2 second budget")]
    TimedOut,
    #[error("offline diagnostic collector task failed")]
    TaskFailed,
    #[error("offline diagnostic field {field_id} exceeds its {limit} byte entry budget")]
    EntryTooLarge { field_id: &'static str, limit: usize },
    #[error("offline diagnostic entry is not representable as JSON")]
    NotRepresentable,
    #[error(transparent)]
    Inventory(#[from] InventoryError),
    #[error(transparent)]
    Redaction(#[from] RedactionError),
}

/// The bounded entries plus the capture time of their persisted L0 source.
#[derive(Debug, PartialEq)]
pub struct OfflineDiagnostics {
    pub entries: Vec<ManifestEntry>,
    pub inventory_captured_at: String,
    pub inventory_age: Duration,
}

#[derive(Debug)]
struct SystemSnapshot {
    os_summary: String,
    kernel_summary: String,
    architecture: &'static str,
    cores: usize,
    total_memory_bytes: u64,
    under_memory_pressure: bool,
    filesystem_types: Vec<String>,
    interface_count: usize,
    bond_count: usize,
}

impl SystemSnapshot {
    fn collect() -> Self {
        #[cfg(test)]
        let _scan = test_support::ScanGuard::start();

        // Do not enumerate processes: Q07 allows only CPU and memory summaries.
        let system = System::new_with_specifics(RefreshKind::everything().without_processes());
        let total_memory_bytes = system.total_memory();
        let available_memory = system.available_memory();
        let filesystem_types = Disks::new_with_refreshed_list()
            .iter()
            .map(|disk| disk.file_system().to_string_lossy().into_owned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let networks = Networks::new_with_refreshed_list();
        Self {
            os_summary: System::long_os_version().unwrap_or_else(|| "unknown".to_owned()),
            kernel_summary: System::kernel_long_version(),
            architecture: std::env::consts::ARCH,
            cores: system.cpus().len(),
            total_memory_bytes,
            under_memory_pressure: total_memory_bytes != 0 && available_memory.saturating_mul(10) < total_memory_bytes,
            filesystem_types,
            interface_count: networks.len(),
            bond_count: networks.keys().filter(|name| name.starts_with("bond")).count(),
        }
    }
}

async fn collect_system_snapshot(cancel: &CancellationToken) -> Result<SystemSnapshot, CollectorError> {
    let deadline = tokio::time::Instant::now() + COLLECT_TIMEOUT;
    let permit = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(CollectorError::Cancelled),
        result = tokio::time::timeout_at(deadline, SYSTEM_SCAN_PERMIT.clone().acquire_owned()) => {
            match result {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => return Err(CollectorError::TaskFailed),
                Err(_) => return Err(CollectorError::TimedOut),
            }
        }
    };
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        SystemSnapshot::collect()
    });
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(CollectorError::Cancelled),
        result = tokio::time::timeout_at(deadline, task) => {
            match result {
                Ok(Ok(snapshot)) => Ok(snapshot),
                Ok(Err(_)) => Err(CollectorError::TaskFailed),
                Err(_) => Err(CollectorError::TimedOut),
            }
        }
    }
}

/// Collect all and only the Q07 offline L0/L1 fields after acquiring the
/// stopped-runtime inventory lock.
pub async fn collect_offline_diagnostics(
    state_root: &Path,
    cancel: &CancellationToken,
) -> Result<OfflineDiagnostics, CollectorError> {
    if cancel.is_cancelled() {
        return Err(CollectorError::Cancelled);
    }
    let store = InventoryStateStore::from_state_root(state_root)?;
    let _lock = store.try_runtime_lock()?;
    let persisted = store.read_latest(chrono::Utc::now())?;
    let system = collect_system_snapshot(cancel).await?;

    let mut entries = Vec::with_capacity(COLLECTORS.len());
    for collector in COLLECTORS {
        entries.push(ManifestEntry::from_value(
            collector,
            collector.value(&persisted.snapshot, &system),
            cancel,
        )?);
    }
    Ok(OfflineDiagnostics {
        entries,
        inventory_captured_at: persisted.captured_at,
        inventory_age: persisted.age,
    })
}

#[cfg(test)]
mod test_support {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::Duration;

    pub(super) static DELAY_MILLIS: AtomicU64 = AtomicU64::new(0);
    pub(super) static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    pub(super) static MAX_ACTIVE: AtomicUsize = AtomicUsize::new(0);

    pub(super) struct ScanGuard;

    impl ScanGuard {
        pub(super) fn start() -> Self {
            let active = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
            MAX_ACTIVE.fetch_max(active, Ordering::SeqCst);
            let delay = DELAY_MILLIS.load(Ordering::SeqCst);
            if delay != 0 {
                std::thread::sleep(Duration::from_millis(delay));
            }
            Self
        }
    }

    impl Drop for ScanGuard {
        fn drop(&mut self) {
            ACTIVE.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    async fn wait_for_active(expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while test_support::ACTIVE.load(Ordering::SeqCst) != expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("system scan reaches expected state");
    }

    #[tokio::test]
    async fn connect_offline_collectors_timeout_and_cancel_never_overlap_system_scans() {
        test_support::MAX_ACTIVE.store(0, Ordering::SeqCst);
        test_support::DELAY_MILLIS.store((COLLECT_TIMEOUT + Duration::from_millis(200)).as_millis() as u64, Ordering::SeqCst);

        let first_cancel = CancellationToken::new();
        assert!(matches!(collect_system_snapshot(&first_cancel).await, Err(CollectorError::TimedOut)));
        assert_eq!(test_support::ACTIVE.load(Ordering::SeqCst), 1, "timed-out blocking scan remains active");

        let second_cancel = CancellationToken::new();
        let second = tokio::spawn({
            let second_cancel = second_cancel.clone();
            async move { collect_system_snapshot(&second_cancel).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            test_support::MAX_ACTIVE.load(Ordering::SeqCst),
            1,
            "a timed-out scan keeps the single-flight permit"
        );
        second_cancel.cancel();
        assert!(matches!(second.await.expect("second collector task"), Err(CollectorError::Cancelled)));
        wait_for_active(0).await;

        test_support::MAX_ACTIVE.store(0, Ordering::SeqCst);
        test_support::DELAY_MILLIS.store(250, Ordering::SeqCst);
        let third_cancel = CancellationToken::new();
        let third = tokio::spawn({
            let third_cancel = third_cancel.clone();
            async move { collect_system_snapshot(&third_cancel).await }
        });
        wait_for_active(1).await;
        third_cancel.cancel();
        assert!(matches!(third.await.expect("third collector task"), Err(CollectorError::Cancelled)));

        let fourth_cancel = CancellationToken::new();
        let fourth = tokio::spawn({
            let fourth_cancel = fourth_cancel.clone();
            async move { collect_system_snapshot(&fourth_cancel).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            test_support::MAX_ACTIVE.load(Ordering::SeqCst),
            1,
            "a cancelled scan keeps the single-flight permit"
        );
        fourth_cancel.cancel();
        assert!(matches!(fourth.await.expect("fourth collector task"), Err(CollectorError::Cancelled)));
        wait_for_active(0).await;
        test_support::DELAY_MILLIS.store(0, Ordering::SeqCst);
    }
}
