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
use std::time::Duration;

use rustfs_madmin::{ITEM_OFFLINE, StorageInfo};
use serde::Serialize;
use serde_json::{Value, json};
use sysinfo::{Disks, Networks, RefreshKind, System};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::manifest_entry::ManifestEntry;
use super::redaction::RedactionError;

const COLLECT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ENTRY_BYTES: usize = 16 * 1024;
const MAX_DRIVES: usize = 4_096;

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

    fn value(self, storage: &StorageSnapshot, system: &SystemSnapshot) -> Value {
        match self {
            Self::RustfsVersion => json!(env!("CARGO_PKG_VERSION")),
            Self::NodeCount => json!(storage.node_count),
            Self::DriveCount => json!(storage.drive_count),
            Self::CapacityUsedBytes => json!(storage.capacity_used_bytes),
            Self::CapacityTotalBytes => json!(storage.capacity_total_bytes),
            Self::CoarseHealthFlags => json!({
                "degraded": storage.degraded,
                "healing": storage.healing,
                "offlineDrives": storage.offline_drives,
                "scanning": storage.scanning,
            }),
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
    #[error("offline diagnostic storage topology exceeds its 4096 drive budget")]
    StorageTopologyTooLarge,
    #[error("offline diagnostic field {field_id} exceeds its {limit} byte entry budget")]
    EntryTooLarge { field_id: &'static str, limit: usize },
    #[error("offline diagnostic entry is not representable as JSON")]
    NotRepresentable,
    #[error(transparent)]
    Redaction(#[from] RedactionError),
}

#[derive(Debug)]
struct StorageSnapshot {
    node_count: usize,
    drive_count: usize,
    capacity_used_bytes: u64,
    capacity_total_bytes: u64,
    offline_drives: usize,
    degraded: bool,
    healing: bool,
    scanning: bool,
}

impl TryFrom<&StorageInfo> for StorageSnapshot {
    type Error = CollectorError;

    fn try_from(info: &StorageInfo) -> Result<Self, Self::Error> {
        if info.disks.len() > MAX_DRIVES {
            return Err(CollectorError::StorageTopologyTooLarge);
        }
        let node_count = info
            .disks
            .iter()
            .filter_map(|disk| (!disk.endpoint.is_empty()).then_some(disk.endpoint.as_str()))
            .collect::<BTreeSet<_>>()
            .len();
        let offline_drives = info.disks.iter().filter(|disk| disk.state == ITEM_OFFLINE).count();
        Ok(Self {
            node_count,
            drive_count: info.disks.len(),
            capacity_used_bytes: info
                .disks
                .iter()
                .fold(0_u64, |total, disk| total.saturating_add(disk.used_space)),
            capacity_total_bytes: info
                .disks
                .iter()
                .fold(0_u64, |total, disk| total.saturating_add(disk.total_space)),
            offline_drives,
            degraded: info.disks.iter().any(|disk| disk.state != rustfs_madmin::ITEM_ONLINE),
            healing: info.disks.iter().any(|disk| disk.healing),
            scanning: info.disks.iter().any(|disk| disk.scanning),
        })
    }
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

/// Collect all and only the Q07 offline L0/L1 fields, with one independently
/// bounded and redacted manifest entry per field.
pub async fn collect_offline_diagnostics(
    storage_info: &StorageInfo,
    cancel: &CancellationToken,
) -> Result<Vec<ManifestEntry>, CollectorError> {
    if cancel.is_cancelled() {
        return Err(CollectorError::Cancelled);
    }
    let storage = StorageSnapshot::try_from(storage_info)?;
    let task = tokio::task::spawn_blocking(SystemSnapshot::collect);
    let mut task = std::pin::pin!(task);
    let system = tokio::select! {
        biased;
        () = cancel.cancelled() => {
            task.as_mut().abort();
            return Err(CollectorError::Cancelled);
        }
        result = tokio::time::timeout(COLLECT_TIMEOUT, &mut task) => {
            match result {
                Ok(Ok(snapshot)) => snapshot,
                Ok(Err(_)) => return Err(CollectorError::TaskFailed),
                Err(_) => {
                    task.as_mut().abort();
                    return Err(CollectorError::TimedOut);
                }
            }
        }
    };

    let mut entries = Vec::with_capacity(COLLECTORS.len());
    for collector in COLLECTORS {
        entries.push(ManifestEntry::from_value(collector, collector.value(&storage, &system), cancel)?);
    }
    Ok(entries)
}
