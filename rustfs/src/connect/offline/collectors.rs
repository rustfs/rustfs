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

use std::path::Path;
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::super::environment::{EnvironmentError, HostEnvironment, collect_host_environment};
use super::super::inventory::{InventoryError, InventorySnapshot, InventoryStateStore};
use super::manifest_entry::ManifestEntry;
use super::redaction::RedactionError;

const COLLECT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ENTRY_BYTES: usize = 16 * 1024;

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

    fn value(self, inventory: &InventorySnapshot, system: &HostEnvironment) -> Value {
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
    #[error("offline diagnostic source is unavailable")]
    SourceUnavailable,
    #[error("offline diagnostic field {field_id} exceeds its {limit} byte entry budget")]
    EntryTooLarge { field_id: &'static str, limit: usize },
    #[error("offline diagnostic entry is not representable as JSON")]
    NotRepresentable,
    #[error(transparent)]
    Inventory(#[from] InventoryError),
    #[error(transparent)]
    Redaction(#[from] RedactionError),
}

impl From<EnvironmentError> for CollectorError {
    fn from(error: EnvironmentError) -> Self {
        match error {
            EnvironmentError::Cancelled => Self::Cancelled,
            EnvironmentError::TimedOut => Self::TimedOut,
            EnvironmentError::TaskFailed => Self::TaskFailed,
            EnvironmentError::SourceUnavailable(_) => Self::SourceUnavailable,
            EnvironmentError::UnsupportedVersion | EnvironmentError::UnsupportedCapability | EnvironmentError::InvalidTimeout => {
                Self::TaskFailed
            }
        }
    }
}

/// The bounded entries plus the capture time of their persisted L0 source.
#[derive(Debug, PartialEq)]
pub struct OfflineDiagnostics {
    pub entries: Vec<ManifestEntry>,
    pub inventory_captured_at: String,
    pub inventory_age: Duration,
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
    let system = collect_host_environment(COLLECT_TIMEOUT, cancel).await?;

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
