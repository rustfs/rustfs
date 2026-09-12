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

//! Bounded, identifier-free deployment environment inventory.

use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use serde::Serialize;
use sysinfo::{Disks, Networks, RefreshKind, System};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::inventory::InventorySnapshot;

pub const ENVIRONMENT_CAPABILITY: &str = "inventory.environment@1";
pub const ENVIRONMENT_SCHEMA_VERSION: u16 = 1;
pub const MAX_ENVIRONMENT_DURATION: Duration = Duration::from_secs(30);

static SYSTEM_SCAN_PERMIT: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvironmentCollectionRequest {
    timeout: Duration,
}

impl EnvironmentCollectionRequest {
    pub fn negotiate(schema_version: u16, capability: &str, timeout: Duration) -> Result<Self, EnvironmentError> {
        if schema_version != ENVIRONMENT_SCHEMA_VERSION {
            return Err(EnvironmentError::UnsupportedVersion);
        }
        if capability != ENVIRONMENT_CAPABILITY {
            return Err(EnvironmentError::UnsupportedCapability);
        }
        if timeout.is_zero() || timeout > MAX_ENVIRONMENT_DURATION {
            return Err(EnvironmentError::InvalidTimeout);
        }
        Ok(Self { timeout })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EnvironmentOsFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

impl EnvironmentOsFamily {
    fn current() -> Self {
        match std::env::consts::OS {
            "linux" => Self::Linux,
            "macos" => Self::Darwin,
            "windows" => Self::Windows,
            "freebsd" => Self::Freebsd,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EnvironmentFilesystemType {
    Ext4,
    Xfs,
    Zfs,
    Apfs,
    Other,
}

impl EnvironmentFilesystemType {
    fn from_reported(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "ext4" => Self::Ext4,
            "xfs" => Self::Xfs,
            "zfs" => Self::Zfs,
            "apfs" => Self::Apfs,
            _ => Self::Other,
        }
    }
}

fn filesystem_types<'a>(reported: impl IntoIterator<Item = &'a str>) -> Result<Vec<EnvironmentFilesystemType>, EnvironmentError> {
    let values = reported
        .into_iter()
        .map(EnvironmentFilesystemType::from_reported)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Filesystem));
    }
    Ok(values)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EnvironmentInventory {
    node_count: u16,
    drive_count: u32,
    os_family: EnvironmentOsFamily,
    filesystem_types: Vec<EnvironmentFilesystemType>,
}

impl EnvironmentInventory {
    pub fn node_count(&self) -> u16 {
        self.node_count
    }

    pub fn drive_count(&self) -> u32 {
        self.drive_count
    }

    pub fn os_family(&self) -> EnvironmentOsFamily {
        self.os_family
    }

    pub fn filesystem_types(&self) -> &[EnvironmentFilesystemType] {
        &self.filesystem_types
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvironmentSource {
    Filesystem,
    Cpu,
    Memory,
    Network,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnvironmentError {
    #[error("inventory_environment_unsupported_version")]
    UnsupportedVersion,
    #[error("inventory_environment_unsupported_capability")]
    UnsupportedCapability,
    #[error("inventory_environment_invalid_timeout")]
    InvalidTimeout,
    #[error("inventory_environment_source_unavailable")]
    SourceUnavailable(EnvironmentSource),
    #[error("inventory_environment_cancelled")]
    Cancelled,
    #[error("inventory_environment_timed_out")]
    TimedOut,
    #[error("inventory_environment_task_failed")]
    TaskFailed,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HostEnvironment {
    pub(crate) os_summary: String,
    pub(crate) kernel_summary: String,
    pub(crate) architecture: &'static str,
    pub(crate) cores: usize,
    pub(crate) total_memory_bytes: u64,
    pub(crate) under_memory_pressure: bool,
    pub(crate) filesystem_types: Vec<EnvironmentFilesystemType>,
    pub(crate) interface_count: usize,
    pub(crate) bond_count: usize,
}

impl HostEnvironment {
    fn collect() -> Result<Self, EnvironmentError> {
        #[cfg(test)]
        let _scan = test_support::ScanGuard::start();

        // Processes, names, addresses, paths, mount options and device labels are outside this schema.
        let system = System::new_with_specifics(RefreshKind::everything().without_processes());
        let cores = system.cpus().len();
        if cores == 0 {
            return Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Cpu));
        }
        let total_memory_bytes = system.total_memory();
        if total_memory_bytes == 0 {
            return Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Memory));
        }
        let available_memory = system.available_memory();
        let disks = Disks::new_with_refreshed_list();
        let reported_filesystems = disks
            .iter()
            .map(|disk| disk.file_system().to_string_lossy())
            .collect::<Vec<_>>();
        let filesystem_types = filesystem_types(reported_filesystems.iter().map(AsRef::as_ref))?;
        let networks = Networks::new_with_refreshed_list();
        if networks.is_empty() {
            return Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Network));
        }

        Ok(Self {
            os_summary: System::long_os_version().unwrap_or_else(|| "unknown".to_owned()),
            kernel_summary: System::kernel_long_version(),
            architecture: std::env::consts::ARCH,
            cores,
            total_memory_bytes,
            under_memory_pressure: available_memory.saturating_mul(10) < total_memory_bytes,
            filesystem_types,
            interface_count: networks.len(),
            bond_count: networks.keys().filter(|name| name.starts_with("bond")).count(),
        })
    }
}

pub async fn collect_environment(
    inventory: &InventorySnapshot,
    request: EnvironmentCollectionRequest,
    cancel: &CancellationToken,
) -> Result<EnvironmentInventory, EnvironmentError> {
    let host = collect_host_environment(request.timeout, cancel).await?;
    Ok(EnvironmentInventory {
        node_count: inventory.node_count(),
        drive_count: inventory.drive_count(),
        os_family: EnvironmentOsFamily::current(),
        filesystem_types: host.filesystem_types,
    })
}

pub(crate) async fn collect_host_environment(
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<HostEnvironment, EnvironmentError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let permit = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(EnvironmentError::Cancelled),
        result = tokio::time::timeout_at(deadline, SYSTEM_SCAN_PERMIT.clone().acquire_owned()) => {
            match result {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => return Err(EnvironmentError::TaskFailed),
                Err(_) => return Err(EnvironmentError::TimedOut),
            }
        }
    };
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        HostEnvironment::collect()
    });
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(EnvironmentError::Cancelled),
        result = tokio::time::timeout_at(deadline, task) => {
            match result {
                Ok(Ok(environment)) => environment,
                Ok(Err(_)) => Err(EnvironmentError::TaskFailed),
                Err(_) => Err(EnvironmentError::TimedOut),
            }
        }
    }
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
    async fn timed_out_and_cancelled_scans_remain_single_flight() {
        test_support::MAX_ACTIVE.store(0, Ordering::SeqCst);
        test_support::DELAY_MILLIS.store(150, Ordering::SeqCst);

        let cancel = CancellationToken::new();
        assert_eq!(
            collect_host_environment(Duration::from_millis(20), &cancel).await,
            Err(EnvironmentError::TimedOut)
        );
        assert_eq!(test_support::ACTIVE.load(Ordering::SeqCst), 1);

        let second_cancel = CancellationToken::new();
        let second = tokio::spawn({
            let second_cancel = second_cancel.clone();
            async move { collect_host_environment(Duration::from_secs(1), &second_cancel).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(test_support::MAX_ACTIVE.load(Ordering::SeqCst), 1);
        second_cancel.cancel();
        assert_eq!(second.await.expect("second scan"), Err(EnvironmentError::Cancelled));
        wait_for_active(0).await;
        test_support::DELAY_MILLIS.store(0, Ordering::SeqCst);
    }

    #[test]
    fn filesystem_projection_drops_paths_options_and_secret_like_values() {
        assert_eq!(
            filesystem_types(["xfs", "ext4", "/srv/customer-a", "rw,password=SYNTHETIC_SECRET_123"]),
            Ok(vec![
                EnvironmentFilesystemType::Ext4,
                EnvironmentFilesystemType::Xfs,
                EnvironmentFilesystemType::Other,
            ])
        );
        assert_eq!(
            filesystem_types(std::iter::empty()),
            Err(EnvironmentError::SourceUnavailable(EnvironmentSource::Filesystem))
        );
    }
}
