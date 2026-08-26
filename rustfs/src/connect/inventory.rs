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

use std::collections::BTreeSet;
use std::fs;
#[cfg(any(target_os = "linux", test))]
use std::io;
#[cfg(target_os = "linux")]
use std::io::Read as _;
#[cfg(target_os = "linux")]
use std::io::Write as _;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::config::HeartbeatConfig;
use super::telemetry::{TelemetryDelivery, TelemetryError, TelemetryTransport, is_exact_utc_seconds};

const PROTOCOL_VERSION: &str = "v1";
const RUSTFS_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION_MAJOR"),
    ".",
    env!("CARGO_PKG_VERSION_MINOR"),
    ".",
    env!("CARGO_PKG_VERSION_PATCH")
);
const HASH_PREFIX: &[u8] = b"rustfs-connect/agent/v1/inventory-snapshot\n";
const MAX_SEQUENCE: u64 = 9_007_199_254_740_991;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const ENVELOPE_FORMAT_VERSION: &str = "v1";
const ENVELOPE_HASH_PREFIX: &[u8] = b"rustfs-connect-inventory-envelope-v1";
#[cfg(target_os = "linux")]
const MAX_PERSISTED_BYTES: usize = 16 * 1024;
#[allow(dead_code)] // Kept for the crate-private stopped-server reader consumed by R06.
const MAX_FUTURE_SKEW: Duration = Duration::from_secs(5 * 60);
#[cfg(target_os = "linux")]
const FILE_MODE: u32 = 0o600;
#[cfg(target_os = "linux")]
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InventoryEnvelope {
    format_version: String,
    captured_at: String,
    snapshot: InventorySnapshot,
    envelope_hash: String,
}

#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)] // This is the intentionally narrow handoff to R06.
pub(crate) struct PersistedInventory {
    pub(crate) snapshot: InventorySnapshot,
    pub(crate) captured_at: String,
    pub(crate) age: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InventorySchedule {
    pub cadence: Duration,
    pub jitter: Duration,
}

impl Default for InventorySchedule {
    fn default() -> Self {
        Self {
            cadence: Duration::from_secs(6 * 60 * 60),
            jitter: Duration::from_secs(30 * 60),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InventoryStatus {
    Starting,
    Unchanged { content_hash: String },
    Online { content_hash: String, received_at: String },
    BackingOff { delay: Duration },
    AuthenticationStopped { status: u16, reason: Option<String> },
    Failed { reason: String },
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum InventoryFlag {
    #[serde(rename = "capacity.critical")]
    CapacityCritical,
    #[serde(rename = "capacity.warning")]
    CapacityWarning,
    #[serde(rename = "clock.skew")]
    ClockSkew,
    #[serde(rename = "cluster.degraded")]
    ClusterDegraded,
    #[serde(rename = "cluster.healing")]
    ClusterHealing,
    #[serde(rename = "cluster.readonly")]
    ClusterReadonly,
    #[serde(rename = "drive.offline")]
    DriveOffline,
    #[serde(rename = "node.offline")]
    NodeOffline,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperatingSystemFamily {
    Linux,
    Darwin,
    Windows,
    Freebsd,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InventoryOsVersion {
    family: OperatingSystemFamily,
    major: u16,
    minor: u16,
}

impl InventoryOsVersion {
    pub fn new(family: OperatingSystemFamily, major: u16, minor: u16) -> Result<Self, InventoryError> {
        if major > 9999 || minor > 9999 {
            return Err(InventoryError::OsVersion);
        }
        Ok(Self { family, major, minor })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InventorySnapshot {
    rustfs_version: String,
    os_version: Option<InventoryOsVersion>,
    node_count: u16,
    drive_count: u32,
    capacity_total_bytes: u64,
    capacity_used_bytes: u64,
    coarse_flags: Vec<InventoryFlag>,
}

impl InventorySnapshot {
    pub fn current(
        node_count: usize,
        drive_count: usize,
        capacity_total_bytes: u64,
        capacity_free_bytes: u64,
        coarse_flags: impl IntoIterator<Item = InventoryFlag>,
    ) -> Result<Self, InventoryError> {
        let capacity_used_bytes = capacity_total_bytes
            .checked_sub(capacity_free_bytes)
            .ok_or(InventoryError::Capacity)?;
        Self::new(
            RUSTFS_VERSION,
            None,
            node_count,
            drive_count,
            capacity_total_bytes,
            capacity_used_bytes,
            coarse_flags,
        )
    }

    pub fn new(
        rustfs_version: impl Into<String>,
        os_version: Option<InventoryOsVersion>,
        node_count: usize,
        drive_count: usize,
        capacity_total_bytes: u64,
        capacity_used_bytes: u64,
        coarse_flags: impl IntoIterator<Item = InventoryFlag>,
    ) -> Result<Self, InventoryError> {
        let snapshot = Self {
            rustfs_version: rustfs_version.into(),
            os_version,
            node_count: u16::try_from(node_count).map_err(|_| InventoryError::NodeCount)?,
            drive_count: u32::try_from(drive_count).map_err(|_| InventoryError::DriveCount)?,
            capacity_total_bytes,
            capacity_used_bytes,
            coarse_flags: coarse_flags.into_iter().collect::<BTreeSet<_>>().into_iter().collect(),
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn content_hash(&self) -> Result<String, InventoryError> {
        self.validate()?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Canonical<'a> {
            capacity_total_bytes: u64,
            capacity_used_bytes: u64,
            coarse_flags: &'a [InventoryFlag],
            drive_count: u32,
            node_count: u16,
            os_version: Option<InventoryOsVersion>,
            rustfs_version: &'a str,
        }

        let canonical = serde_json::to_vec(&Canonical {
            capacity_total_bytes: self.capacity_total_bytes,
            capacity_used_bytes: self.capacity_used_bytes,
            coarse_flags: &self.coarse_flags,
            drive_count: self.drive_count,
            node_count: self.node_count,
            os_version: self.os_version,
            rustfs_version: &self.rustfs_version,
        })?;
        let mut digest = Sha256::new();
        digest.update(HASH_PREFIX);
        digest.update(canonical);
        Ok(hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower))
    }

    pub(crate) fn rustfs_version(&self) -> &str {
        &self.rustfs_version
    }

    pub(crate) fn node_count(&self) -> u16 {
        self.node_count
    }

    pub(crate) fn drive_count(&self) -> u32 {
        self.drive_count
    }

    pub(crate) fn capacity_total_bytes(&self) -> u64 {
        self.capacity_total_bytes
    }

    pub(crate) fn capacity_used_bytes(&self) -> u64 {
        self.capacity_used_bytes
    }

    pub(crate) fn coarse_flags(&self) -> &[InventoryFlag] {
        &self.coarse_flags
    }

    fn validate(&self) -> Result<(), InventoryError> {
        if !valid_version(&self.rustfs_version) {
            return Err(InventoryError::RustfsVersion);
        }
        if self
            .os_version
            .is_some_and(|version| version.major > 9999 || version.minor > 9999)
        {
            return Err(InventoryError::OsVersion);
        }
        if self.node_count == 0 || self.node_count > 4096 {
            return Err(InventoryError::NodeCount);
        }
        if self.drive_count > 1_048_576 {
            return Err(InventoryError::DriveCount);
        }
        if self.capacity_total_bytes > MAX_SAFE_INTEGER || self.capacity_used_bytes > self.capacity_total_bytes {
            return Err(InventoryError::Capacity);
        }
        if self.coarse_flags.len() > 8 || !self.coarse_flags.windows(2).all(|flags| flags[0] < flags[1]) {
            return Err(InventoryError::CoarseFlags);
        }
        Ok(())
    }
}

fn valid_version(version: &str) -> bool {
    let components = version.split('.').collect::<Vec<_>>();
    components.len() == 3
        && components.iter().all(|component| {
            !component.is_empty()
                && component.len() <= 4
                && (component == &"0" || !component.starts_with('0'))
                && component.parse::<u16>().is_ok_and(|value| value <= 9999)
        })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct PendingInventory {
    protocol_version: String,
    request_id: String,
    sequence: u64,
    #[serde(flatten)]
    snapshot: InventorySnapshot,
}

impl PendingInventory {
    fn new(snapshot: InventorySnapshot, sequence: u64) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            request_id: Uuid::new_v4().to_string(),
            sequence,
            snapshot,
        }
    }

    #[cfg(target_os = "linux")]
    fn is_valid(&self) -> bool {
        self.protocol_version == PROTOCOL_VERSION
            && self.sequence <= MAX_SEQUENCE
            && self.snapshot.validate().is_ok()
            && Uuid::parse_str(&self.request_id)
                .is_ok_and(|request_id| request_id.get_version_num() == 4 && request_id.to_string() == self.request_id)
    }

    fn content_hash(&self) -> Result<String, InventoryError> {
        self.snapshot.content_hash()
    }

    pub(crate) fn snapshot(&self) -> &InventorySnapshot {
        &self.snapshot
    }
}

pub(crate) enum InventoryDelivery {
    Accepted { content_hash: String, received_at: String },
    Retry { retry_after: Option<Duration> },
    AuthenticationStopped { status: u16 },
    Rejected { status: u16 },
}

pub(crate) struct InventorySender {
    transport: TelemetryTransport,
}

impl InventorySender {
    pub(crate) fn new(config: HeartbeatConfig) -> Result<Self, InventoryError> {
        Ok(Self {
            transport: TelemetryTransport::new(config)?,
        })
    }

    pub(crate) async fn send(&self, inventory: &PendingInventory) -> Result<InventoryDelivery, InventoryError> {
        inventory.snapshot.validate()?;
        match self.transport.post("inventorySnapshots", inventory).await? {
            TelemetryDelivery::Accepted { cluster_name, body } => {
                #[derive(Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct InventoryResponse {
                    name: String,
                    uid: String,
                    content_hash: String,
                    received_at: String,
                }

                let accepted: InventoryResponse = serde_json::from_slice(&body).map_err(|_| InventoryError::Response)?;
                let uid = Uuid::parse_str(&accepted.uid).map_err(|_| InventoryError::Response)?;
                let content_hash = inventory.content_hash()?;
                if uid.get_version_num() != 7
                    || uid.to_string() != accepted.uid
                    || accepted.name != format!("{cluster_name}/inventorySnapshots/{}", accepted.uid)
                    || accepted.content_hash != content_hash
                    || !is_exact_utc_seconds(&accepted.received_at)
                {
                    return Err(InventoryError::Response);
                }
                Ok(InventoryDelivery::Accepted {
                    content_hash,
                    received_at: accepted.received_at,
                })
            }
            TelemetryDelivery::Retry { retry_after } => Ok(InventoryDelivery::Retry { retry_after }),
            TelemetryDelivery::AuthenticationStopped { status, .. } => Ok(InventoryDelivery::AuthenticationStopped { status }),
            TelemetryDelivery::Rejected { status, .. } => Ok(InventoryDelivery::Rejected { status }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InventoryStateStore {
    #[cfg(target_os = "linux")]
    directory: Arc<fs::File>,
    #[cfg(target_os = "linux")]
    state_root: Arc<StateRootAnchor>,
}

#[cfg(target_os = "linux")]
struct StateRootAnchor {
    root: fs::File,
    components: Vec<(std::ffi::OsString, fs::File)>,
}

#[cfg(target_os = "linux")]
impl StateRootAnchor {
    fn state_root(&self) -> Result<&fs::File, InventoryError> {
        self.components
            .last()
            .map(|(_, directory)| directory)
            .ok_or(InventoryError::StatePath)
    }

    fn validate(&self) -> Result<(), InventoryError> {
        validate_directory(&self.root, false)?;
        let mut current = None;
        for (index, (component, expected)) in self.components.iter().enumerate() {
            let parent = current.as_ref().unwrap_or(&self.root);
            let resolved = open_directory_component_at(parent, component)?;
            let dedicated = index + 1 == self.components.len();
            validate_directory(&resolved, dedicated)?;
            if file_identity(expected)? != file_identity(&resolved)? {
                return Err(InventoryError::PersistenceSecurity);
            }
            current = Some(resolved);
        }
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InventoryState {
    next_sequence: u64,
    pending: Option<PendingInventory>,
    last_accepted_content_hash: Option<String>,
}

impl InventoryStateStore {
    pub(crate) fn from_state_root(path: &Path) -> Result<Self, InventoryError> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err(InventoryError::PlatformSecurity)
        }
        #[cfg(target_os = "linux")]
        {
            let (state_root, directory) = open_inventory_directory(path)?;
            Ok(Self {
                directory: Arc::new(directory),
                state_root: Arc::new(state_root),
            })
        }
    }

    pub(crate) fn try_runtime_lock(&self) -> Result<fs::File, InventoryError> {
        #[cfg(not(target_os = "linux"))]
        {
            Err(InventoryError::PlatformSecurity)
        }
        #[cfg(target_os = "linux")]
        {
            self.validate_anchor()?;
            let lock = open_file_at(&self.directory, ".state.json.lock", true, true)?;
            validate_regular_file(&lock)?;
            lock.try_lock().map_err(|_| InventoryError::AlreadyRunning)?;
            self.validate_anchor()?;
            Ok(lock)
        }
    }

    pub(crate) async fn pending(&self) -> Result<Option<(PendingInventory, String)>, InventoryError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let (state, persisted_at) = store.read_with_persisted_at()?;
            if state.pending.is_none() && state.next_sequence > MAX_SEQUENCE {
                return Err(InventoryError::SequenceExhausted);
            }
            state
                .pending
                .map(|pending| {
                    persisted_at
                        .map(|persisted_at| (pending, persisted_at))
                        .ok_or(InventoryError::StateCorrupt)
                })
                .transpose()
        })
        .await
        .map_err(|_| InventoryError::StateIo)?
    }

    pub(crate) async fn prepare(&self, snapshot: InventorySnapshot) -> Result<Option<PendingInventory>, InventoryError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.prepare_sync(snapshot))
            .await
            .map_err(|_| InventoryError::StateIo)?
    }

    pub(crate) async fn mark_accepted(&self, accepted: &PendingInventory) -> Result<(), InventoryError> {
        let store = self.clone();
        let accepted = accepted.clone();
        tokio::task::spawn_blocking(move || store.mark_accepted_sync(&accepted))
            .await
            .map_err(|_| InventoryError::StateIo)?
    }

    pub(crate) async fn publish_latest(
        &self,
        snapshot: InventorySnapshot,
        captured_at: String,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), InventoryError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.publish_latest_sync(snapshot, captured_at, &shutdown))
            .await
            .map_err(|_| InventoryError::StateIo)?
    }

    pub(crate) async fn ensure_latest(
        &self,
        snapshot: InventorySnapshot,
        captured_at: String,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), InventoryError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || match store.read_latest(chrono::Utc::now()) {
            Ok(_) => Ok(()),
            Err(InventoryError::StateMissing) => store.publish_latest_sync(snapshot, captured_at, &shutdown),
            Err(error) => Err(error),
        })
        .await
        .map_err(|_| InventoryError::StateIo)?
    }

    #[allow(dead_code)] // R06 reads this after the server has stopped.
    pub(crate) fn read_latest(&self, now: chrono::DateTime<chrono::Utc>) -> Result<PersistedInventory, InventoryError> {
        self.read_latest_inner(now, || {})
    }

    #[cfg(all(test, target_os = "linux"))]
    fn read_latest_after_open(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        after_open: impl FnOnce(),
    ) -> Result<PersistedInventory, InventoryError> {
        self.read_latest_inner(now, after_open)
    }

    fn read_latest_inner(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        after_open: impl FnOnce(),
    ) -> Result<PersistedInventory, InventoryError> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (now, after_open);
            Err(InventoryError::PlatformSecurity)
        }
        #[cfg(target_os = "linux")]
        {
            self.validate_anchor()?;
            let mut file = open_file_at(&self.directory, "latest.json", false, false)?;
            validate_regular_file(&file)?;
            let before = file_identity(&file)?;
            after_open();
            let bytes = read_bounded(&mut file)?;
            validate_regular_file(&file)?;
            if before != file_identity(&file)? {
                return Err(InventoryError::PersistenceSecurity);
            }
            let current = open_file_at(&self.directory, "latest.json", false, false)?;
            validate_regular_file(&current)?;
            if before != file_identity(&current)? {
                return Err(InventoryError::PersistenceSecurity);
            }
            self.validate_anchor()?;
            decode_envelope(&bytes, now)
        }
    }

    #[cfg(target_os = "linux")]
    fn validate_anchor(&self) -> Result<(), InventoryError> {
        self.state_root.validate()?;
        let state_root = self.state_root.state_root()?;
        validate_directory(&self.directory, true)?;
        let current = open_directory_at(state_root, "inventory")?;
        validate_directory(&current, true)?;
        if file_identity(&self.directory)? != file_identity(&current)? {
            return Err(InventoryError::PersistenceSecurity);
        }
        Ok(())
    }

    fn prepare_sync(&self, snapshot: InventorySnapshot) -> Result<Option<PendingInventory>, InventoryError> {
        snapshot.validate()?;
        let mut state = self.read()?;
        if state.pending.is_some() {
            return Ok(state.pending);
        }
        let content_hash = snapshot.content_hash()?;
        if state.last_accepted_content_hash.as_deref() == Some(&content_hash) {
            return Ok(None);
        }
        if state.next_sequence > MAX_SEQUENCE {
            return Err(InventoryError::SequenceExhausted);
        }
        let pending = PendingInventory::new(snapshot, state.next_sequence);
        state.pending = Some(pending.clone());
        self.write(&state)?;
        Ok(Some(pending))
    }

    fn mark_accepted_sync(&self, accepted: &PendingInventory) -> Result<(), InventoryError> {
        let mut state = self.read()?;
        if state.pending.as_ref() != Some(accepted) {
            return Err(InventoryError::StateConflict);
        }
        state.next_sequence = accepted.sequence.checked_add(1).ok_or(InventoryError::SequenceExhausted)?;
        state.last_accepted_content_hash = Some(accepted.content_hash()?);
        state.pending = None;
        self.write(&state)
    }

    fn read(&self) -> Result<InventoryState, InventoryError> {
        self.read_with_persisted_at().map(|(state, _)| state)
    }

    fn read_with_persisted_at(&self) -> Result<(InventoryState, Option<String>), InventoryError> {
        #[cfg(not(target_os = "linux"))]
        {
            Err(InventoryError::PlatformSecurity)
        }
        #[cfg(target_os = "linux")]
        {
            self.validate_anchor()?;
            let mut file = match open_file_at(&self.directory, "state.json", false, false) {
                Ok(file) => file,
                Err(InventoryError::StateMissing) => return Ok((InventoryState::default(), None)),
                Err(error) => return Err(error),
            };
            validate_regular_file(&file)?;
            let bytes = read_bounded(&mut file)?;
            let modified = file
                .metadata()
                .and_then(|metadata| metadata.modified())
                .map_err(|_| InventoryError::StateIo)?;
            self.validate_anchor()?;
            let state: InventoryState = serde_json::from_slice(&bytes).map_err(|_| InventoryError::StateInvalid)?;
            let last_hash_valid = state.last_accepted_content_hash.as_deref().is_none_or(valid_content_hash);
            let pending_valid = state.pending.as_ref().is_none_or(|pending| {
                pending.sequence == state.next_sequence
                    && pending.is_valid()
                    && pending
                        .content_hash()
                        .is_ok_and(|hash| state.last_accepted_content_hash.as_deref() != Some(&hash))
            });
            if state.next_sequence > MAX_SEQUENCE + 1 || !last_hash_valid || !pending_valid {
                return Err(InventoryError::StateCorrupt);
            }
            let modified = chrono::DateTime::<chrono::Utc>::from(modified);
            if modified > chrono::Utc::now() + MAX_FUTURE_SKEW {
                return Err(InventoryError::StateCorrupt);
            }
            let persisted_at = modified.format("%Y-%m-%dT%H:%M:%SZ").to_string();
            Ok((state, Some(persisted_at)))
        }
    }

    fn write(&self, state: &InventoryState) -> Result<(), InventoryError> {
        let bytes = serde_json::to_vec(state).map_err(|_| InventoryError::StateInvalid)?;
        self.replace_file("state.json", &bytes, || false)
    }

    fn publish_latest_sync(
        &self,
        snapshot: InventorySnapshot,
        captured_at: String,
        shutdown: &tokio_util::sync::CancellationToken,
    ) -> Result<(), InventoryError> {
        snapshot.validate()?;
        let bytes = encode_envelope(snapshot, captured_at)?;
        self.replace_file("latest.json", &bytes, || shutdown.is_cancelled())
    }

    fn replace_file(&self, destination: &str, bytes: &[u8], cancelled: impl FnOnce() -> bool) -> Result<(), InventoryError> {
        self.replace_file_inner(
            destination,
            bytes,
            cancelled,
            #[cfg(all(test, target_os = "linux"))]
            None,
            #[cfg(all(test, target_os = "linux"))]
            || {},
        )
    }

    fn replace_file_inner(
        &self,
        destination: &str,
        bytes: &[u8],
        cancelled: impl FnOnce() -> bool,
        #[cfg(all(test, target_os = "linux"))] fault: Option<PersistFault>,
        #[cfg(all(test, target_os = "linux"))] before_commit: impl FnOnce(),
    ) -> Result<(), InventoryError> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (destination, bytes, cancelled);
            Err(InventoryError::PlatformSecurity)
        }
        #[cfg(target_os = "linux")]
        {
            self.validate_anchor()?;
            match open_file_at(&self.directory, destination, false, false) {
                Ok(existing) => validate_regular_file(&existing)?,
                Err(InventoryError::StateMissing) => {}
                Err(error) => return Err(error),
            }
            let (temp_name, mut temp) = stage_at(&self.directory, destination)?;
            #[cfg(test)]
            let injected_write = matches!(fault.as_ref(), Some(PersistFault::Write));
            #[cfg(not(test))]
            let injected_write = false;
            let staged = if injected_write {
                Err(InventoryError::StateIo)
            } else {
                temp.write_all(bytes).map_err(|_| InventoryError::StateIo)
            }
            .and_then(|()| {
                #[cfg(test)]
                if matches!(fault.as_ref(), Some(PersistFault::TempSync)) {
                    return Err(InventoryError::StateIo);
                }
                temp.sync_all().map_err(|_| InventoryError::StateIo)
            });
            if staged.is_err() || cancelled() {
                let _ = unlink_at(&self.directory, &temp_name);
                return staged.and(Err(InventoryError::Cancelled));
            }
            if let Err(error) = validate_regular_file(&temp) {
                let _ = unlink_at(&self.directory, &temp_name);
                return Err(error);
            }
            #[cfg(test)]
            before_commit();
            if let Err(error) = self.validate_anchor() {
                let _ = unlink_at(&self.directory, &temp_name);
                return Err(error);
            }
            #[cfg(test)]
            if let Some(PersistFault::CancelDuringCommit(token)) = fault.as_ref() {
                token.cancel();
            }
            #[cfg(test)]
            let rename_failed = matches!(fault.as_ref(), Some(PersistFault::Rename));
            #[cfg(not(test))]
            let rename_failed = false;
            if rename_failed || rename_at(&self.directory, &temp_name, destination).is_err() {
                let _ = unlink_at(&self.directory, &temp_name);
                return Err(InventoryError::StateIo);
            }
            #[cfg(test)]
            if matches!(fault.as_ref(), Some(PersistFault::DirectorySync)) {
                return Err(InventoryError::DurabilityAfterCommit);
            }
            self.directory.sync_all().map_err(|_| InventoryError::DurabilityAfterCommit)?;
            self.validate_anchor()
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
enum PersistFault {
    Write,
    TempSync,
    Rename,
    DirectorySync,
    CancelDuringCommit(tokio_util::sync::CancellationToken),
}

fn encode_envelope(snapshot: InventorySnapshot, captured_at: String) -> Result<Vec<u8>, InventoryError> {
    if !is_exact_utc_seconds(&captured_at) {
        return Err(InventoryError::EnvelopeTimestamp);
    }
    let envelope_hash = envelope_hash(ENVELOPE_FORMAT_VERSION, &captured_at, &snapshot)?;
    serde_json::to_vec(&InventoryEnvelope {
        format_version: ENVELOPE_FORMAT_VERSION.to_owned(),
        captured_at,
        snapshot,
        envelope_hash,
    })
    .map_err(|_| InventoryError::StateInvalid)
}

fn envelope_hash(format_version: &str, captured_at: &str, snapshot: &InventorySnapshot) -> Result<String, InventoryError> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Canonical<'a> {
        format_version: &'a str,
        captured_at: &'a str,
        snapshot: &'a InventorySnapshot,
    }
    let canonical = serde_json::to_vec(&Canonical {
        format_version,
        captured_at,
        snapshot,
    })
    .map_err(|_| InventoryError::StateInvalid)?;
    let mut digest = Sha256::new();
    digest.update(ENVELOPE_HASH_PREFIX);
    digest.update([0]);
    digest.update(canonical);
    Ok(hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower))
}

#[allow(dead_code)] // Used by the crate-private stopped-server reader.
fn decode_envelope(bytes: &[u8], now: chrono::DateTime<chrono::Utc>) -> Result<PersistedInventory, InventoryError> {
    let envelope: InventoryEnvelope = serde_json::from_slice(bytes).map_err(|_| InventoryError::EnvelopeInvalid)?;
    if envelope.format_version != ENVELOPE_FORMAT_VERSION {
        return Err(InventoryError::EnvelopeVersion);
    }
    if !is_exact_utc_seconds(&envelope.captured_at) {
        return Err(InventoryError::EnvelopeTimestamp);
    }
    envelope.snapshot.validate()?;
    let expected = envelope_hash(&envelope.format_version, &envelope.captured_at, &envelope.snapshot)?;
    if envelope.envelope_hash != expected || !valid_content_hash(&envelope.envelope_hash) {
        return Err(InventoryError::EnvelopeHash);
    }
    let captured_at = chrono::DateTime::parse_from_rfc3339(&envelope.captured_at)
        .map_err(|_| InventoryError::EnvelopeTimestamp)?
        .with_timezone(&chrono::Utc);
    let future = captured_at.signed_duration_since(now);
    if future > chrono::Duration::from_std(MAX_FUTURE_SKEW).map_err(|_| InventoryError::EnvelopeTimestamp)? {
        return Err(InventoryError::EnvelopeFuture);
    }
    let age = now.signed_duration_since(captured_at).to_std().unwrap_or_default();
    Ok(PersistedInventory {
        snapshot: envelope.snapshot,
        captured_at: envelope.captured_at,
        age,
    })
}

fn valid_content_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(target_os = "linux")]
fn read_bounded(file: &mut fs::File) -> Result<Vec<u8>, InventoryError> {
    let mut bytes = Vec::new();
    file.take((MAX_PERSISTED_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| InventoryError::StateIo)?;
    if bytes.len() > MAX_PERSISTED_BYTES {
        return Err(InventoryError::StateOversize);
    }
    Ok(bytes)
}

#[cfg(target_os = "linux")]
// SAFETY: libc path operations use validated directory descriptors and checked C strings; returned descriptors become owned files.
#[allow(unsafe_code)]
fn open_inventory_directory(path: &Path) -> Result<(StateRootAnchor, fs::File), InventoryError> {
    use std::os::fd::AsRawFd as _;
    use std::path::Component;

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(|_| InventoryError::StateIo)?.join(path)
    };
    let root = fs::File::open("/").map_err(|_| InventoryError::StateIo)?;
    validate_directory(&root, false)?;
    let components = absolute.components().collect::<Vec<_>>();
    let names = components
        .iter()
        .filter_map(|component| match component {
            Component::Normal(name) => Some(name.to_os_string()),
            Component::RootDir => None,
            _ => Some(std::ffi::OsString::new()),
        })
        .collect::<Vec<_>>();
    if names.iter().any(|name| name.is_empty()) || names.is_empty() {
        return Err(InventoryError::StatePath);
    }
    let component_count = names.len();
    let mut state_root = StateRootAnchor {
        root,
        components: Vec::with_capacity(component_count),
    };
    for (index, name) in names.into_iter().enumerate() {
        let parent = state_root
            .components
            .last()
            .map(|(_, directory)| directory)
            .unwrap_or(&state_root.root);
        let directory = open_directory_component_at(parent, &name)?;
        validate_directory(&directory, index + 1 == component_count)?;
        state_root.components.push((name, directory));
    }

    let directory = state_root.state_root()?;
    let inventory = c_name("inventory")?;
    // SAFETY: descriptor and C string are valid; mode is applied only if the directory is created.
    if unsafe { libc::mkdirat(directory.as_raw_fd(), inventory.as_ptr(), 0o700) } != 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(InventoryError::StateIo);
        }
    }
    let child = open_directory_at(directory, "inventory")?;
    validate_directory(&child, true)?;
    sync_inventory_anchor(&child, directory)?;
    Ok((state_root, child))
}

#[cfg(target_os = "linux")]
fn sync_inventory_anchor(inventory: &fs::File, state_root: &fs::File) -> Result<(), InventoryError> {
    sync_inventory_anchor_with(|inventory_target| {
        if inventory_target {
            inventory.sync_all()
        } else {
            state_root.sync_all()
        }
    })
}

#[cfg(any(target_os = "linux", test))]
fn sync_inventory_anchor_with(mut sync: impl FnMut(bool) -> io::Result<()>) -> Result<(), InventoryError> {
    sync(true).map_err(|_| InventoryError::StateIo)?;
    sync(false).map_err(|_| InventoryError::StateIo)
}

#[cfg(target_os = "linux")]
fn open_directory_at(parent: &fs::File, name: &str) -> Result<fs::File, InventoryError> {
    open_directory_component_at(parent, std::ffi::OsStr::new(name))
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn open_directory_component_at(parent: &fs::File, name: &std::ffi::OsStr) -> Result<fs::File, InventoryError> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;
    let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| InventoryError::StatePath)?;
    // SAFETY: the parent descriptor and C string are valid; ownership of a successful descriptor is transferred.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
        )
    };
    if fd < 0 {
        return Err(InventoryError::PersistenceSecurity);
    }
    // SAFETY: openat returned a new owned descriptor.
    Ok(unsafe { fs::File::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn validate_directory(directory: &fs::File, dedicated: bool) -> Result<(), InventoryError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let metadata = directory.metadata().map_err(|_| InventoryError::StateIo)?;
    let mode = metadata.permissions().mode() & 0o7777;
    let uid = process_uid();
    if !metadata.is_dir() || !unix_directory_is_trusted(metadata.uid(), mode, uid, dedicated) {
        return Err(InventoryError::PersistenceSecurity);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn unix_directory_is_trusted(owner: u32, mode: u32, process: u32, dedicated: bool) -> bool {
    let trusted_owner = owner == process || (!dedicated && owner == 0);
    let trusted_mode = if dedicated { mode == 0o700 } else { mode & 0o7022 == 0 };
    trusted_owner && trusted_mode
}

#[cfg(target_os = "linux")]
// SAFETY: openat receives a live directory descriptor and checked C string; a successful descriptor becomes an owned file.
#[allow(unsafe_code)]
fn open_file_at(directory: &fs::File, name: &str, create: bool, write: bool) -> Result<fs::File, InventoryError> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    let name = c_name(name)?;
    let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
    flags |= if write { libc::O_RDWR } else { libc::O_RDONLY };
    if create {
        flags |= libc::O_CREAT;
    }
    // SAFETY: the directory descriptor and C string are valid; ownership of a successful descriptor is transferred.
    let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags, FILE_MODE) };
    if fd < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Err(InventoryError::StateMissing)
        } else {
            Err(InventoryError::PersistenceSecurity)
        };
    }
    // SAFETY: openat returned a new owned descriptor.
    Ok(unsafe { fs::File::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
// SAFETY: openat receives a live directory descriptor and checked C string; a successful descriptor becomes an owned file.
#[allow(unsafe_code)]
fn stage_at(directory: &fs::File, destination: &str) -> Result<(String, fs::File), InventoryError> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    loop {
        let name = format!(
            ".{destination}.{}.{}.tmp",
            std::process::id(),
            STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let c_name = c_name(&name)?;
        // SAFETY: the directory descriptor and C string are valid; ownership of a successful descriptor is transferred.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                c_name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                FILE_MODE,
            )
        };
        if fd >= 0 {
            // SAFETY: openat returned a new owned descriptor.
            let file = unsafe { fs::File::from_raw_fd(fd) };
            if let Err(error) = validate_regular_file(&file) {
                let _ = unlink_at(directory, &name);
                return Err(error);
            }
            return Ok((name, file));
        }
        if io::Error::last_os_error().kind() != io::ErrorKind::AlreadyExists {
            return Err(InventoryError::StateIo);
        }
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn rename_at(directory: &fs::File, source: &str, destination: &str) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    let source = std::ffi::CString::new(source).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let destination = std::ffi::CString::new(destination).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: both names are valid C strings and both directory descriptors remain open.
    if unsafe { libc::renameat(directory.as_raw_fd(), source.as_ptr(), directory.as_raw_fd(), destination.as_ptr()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn unlink_at(directory: &fs::File, name: &str) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    let name = std::ffi::CString::new(name).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: the name is a valid C string and the directory descriptor remains open.
    if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn validate_regular_file(file: &fs::File) -> Result<(), InventoryError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let metadata = file.metadata().map_err(|_| InventoryError::StateIo)?;
    if !metadata.is_file()
        || !unix_regular_file_is_secure(metadata.uid(), metadata.permissions().mode() & 0o7777, metadata.nlink(), process_uid())
    {
        return Err(InventoryError::PersistenceSecurity);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn unix_regular_file_is_secure(owner: u32, mode: u32, links: u64, process: u32) -> bool {
    owner == process && mode == FILE_MODE && links == 1
}

#[cfg(target_os = "linux")]
#[allow(dead_code)] // Used by the crate-private stopped-server reader.
fn file_identity(file: &fs::File) -> Result<(u64, u64), InventoryError> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = file.metadata().map_err(|_| InventoryError::StateIo)?;
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(target_os = "linux")]
fn c_name(name: &str) -> Result<std::ffi::CString, InventoryError> {
    std::ffi::CString::new(name).map_err(|_| InventoryError::StatePath)
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn process_uid() -> u32 {
    // SAFETY: geteuid has no pointer arguments or caller preconditions.
    unsafe { libc::geteuid() }
}

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error("connect_inventory_snapshot_version")]
    RustfsVersion,
    #[error("connect_inventory_snapshot_os_version")]
    OsVersion,
    #[error("connect_inventory_snapshot_node_count")]
    NodeCount,
    #[error("connect_inventory_snapshot_drive_count")]
    DriveCount,
    #[error("connect_inventory_snapshot_capacity")]
    Capacity,
    #[error("connect_inventory_snapshot_flags")]
    CoarseFlags,
    #[error("connect_inventory_snapshot_incomplete")]
    SnapshotIncomplete { expected: usize, observed: usize },
    #[error("connect_inventory_schedule")]
    Schedule,
    #[error("connect_inventory_sequence_exhausted")]
    SequenceExhausted,
    #[error("connect_inventory_already_running")]
    AlreadyRunning,
    #[error("connect_inventory_state_conflict")]
    StateConflict,
    #[error("connect_inventory_state_path")]
    StatePath,
    #[error("connect_inventory_state_missing")]
    StateMissing,
    #[error("connect_inventory_state_io")]
    StateIo,
    #[error("connect_inventory_state_invalid")]
    StateInvalid,
    #[error("connect_inventory_state_corrupt")]
    StateCorrupt,
    #[error("connect_inventory_state_oversize")]
    StateOversize,
    #[error("connect_inventory_persistence_security")]
    PersistenceSecurity,
    #[error("connect_inventory_platform_security")]
    PlatformSecurity,
    #[error("connect_inventory_cancelled")]
    Cancelled,
    #[error("connect_inventory_durability_after_commit")]
    DurabilityAfterCommit,
    #[error("connect_inventory_envelope_invalid")]
    EnvelopeInvalid,
    #[error("connect_inventory_envelope_version")]
    EnvelopeVersion,
    #[error("connect_inventory_envelope_timestamp")]
    EnvelopeTimestamp,
    #[error("connect_inventory_envelope_future")]
    EnvelopeFuture,
    #[error("connect_inventory_envelope_hash")]
    EnvelopeHash,
    #[error("connect_inventory_response")]
    Response,
    #[error("connect_inventory_json")]
    Json(#[from] serde_json::Error),
    #[error("connect_inventory_telemetry")]
    Telemetry,
}

impl From<TelemetryError> for InventoryError {
    fn from(_error: TelemetryError) -> Self {
        Self::Telemetry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn safe_tempdir() -> tempfile::TempDir {
        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).expect("safe temporary directory");
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).expect("private temporary directory");
        }
        temp
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn persistence_fails_closed_before_accessing_state() {
        let temp = safe_tempdir();
        let state = temp.path().join("state-must-not-be-created");

        assert!(matches!(
            InventoryStateStore::from_state_root(&state),
            Err(InventoryError::PlatformSecurity)
        ));
        assert!(!state.exists());
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_code)]
    fn make_fifo(path: &Path) {
        use std::os::unix::ffi::OsStrExt as _;

        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("FIFO path");
        // SAFETY: the path is a valid C string and mkfifo does not retain it.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), FILE_MODE as libc::mode_t) }, 0, "create FIFO");
    }

    fn snapshot() -> InventorySnapshot {
        InventorySnapshot::new("1.2.3", None, 2, 4, 1_000, 400, [InventoryFlag::DriveOffline]).expect("snapshot")
    }

    #[test]
    fn envelope_has_stable_canonical_bytes_and_hash() {
        let bytes = encode_envelope(snapshot(), "2026-08-23T01:02:03Z".to_owned()).expect("envelope");

        assert_eq!(
            String::from_utf8(bytes).expect("JSON"),
            r#"{"formatVersion":"v1","capturedAt":"2026-08-23T01:02:03Z","snapshot":{"rustfsVersion":"1.2.3","osVersion":null,"nodeCount":2,"driveCount":4,"capacityTotalBytes":1000,"capacityUsedBytes":400,"coarseFlags":["drive.offline"]},"envelopeHash":"fb927e66c9635e0020b97993c868636bff1f8ddefe01b49345286de6239865e3"}"#
        );
    }

    #[test]
    fn telemetry_failures_are_normalized_before_runtime_status() {
        assert_eq!(InventoryError::from(TelemetryError::Endpoint).to_string(), "connect_inventory_telemetry");
    }

    #[test]
    fn inventory_anchor_syncs_child_then_state_root_and_retries_failures() {
        let mut calls = Vec::new();
        let error = sync_inventory_anchor_with(|inventory_target| {
            calls.push(inventory_target);
            if !inventory_target {
                Err(io::Error::other("injected state-root sync failure"))
            } else {
                Ok(())
            }
        })
        .expect_err("state-root sync failure");
        assert!(matches!(error, InventoryError::StateIo));
        assert_eq!(calls, [true, false]);

        calls.clear();
        sync_inventory_anchor_with(|inventory_target| {
            calls.push(inventory_target);
            Ok(())
        })
        .expect("retry syncs the whole anchor");
        assert_eq!(calls, [true, false]);
    }

    #[test]
    fn reader_rejects_tampering_unknown_members_and_future_time() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T01:02:03Z")
            .expect("time")
            .with_timezone(&chrono::Utc);
        let valid = encode_envelope(snapshot(), "2026-08-23T01:02:03Z".to_owned()).expect("envelope");
        assert_eq!(decode_envelope(&valid, now).expect("valid envelope").age, Duration::ZERO);

        let mut hash_tampered: serde_json::Value = serde_json::from_slice(&valid).expect("JSON");
        hash_tampered["snapshot"]["nodeCount"] = 3.into();
        assert!(matches!(
            decode_envelope(&serde_json::to_vec(&hash_tampered).expect("JSON"), now),
            Err(InventoryError::EnvelopeHash)
        ));

        let mut timestamp_tampered: serde_json::Value = serde_json::from_slice(&valid).expect("JSON");
        timestamp_tampered["capturedAt"] = "2026-08-23T01:02:04Z".into();
        assert!(matches!(
            decode_envelope(&serde_json::to_vec(&timestamp_tampered).expect("JSON"), now),
            Err(InventoryError::EnvelopeHash)
        ));

        let mut unknown: serde_json::Value = serde_json::from_slice(&valid).expect("JSON");
        unknown["extra"] = true.into();
        assert!(matches!(
            decode_envelope(&serde_json::to_vec(&unknown).expect("JSON"), now),
            Err(InventoryError::EnvelopeInvalid)
        ));

        let mut unknown_snapshot: serde_json::Value = serde_json::from_slice(&valid).expect("JSON");
        unknown_snapshot["snapshot"]["hostname"] = "secret.invalid".into();
        assert!(matches!(
            decode_envelope(&serde_json::to_vec(&unknown_snapshot).expect("JSON"), now),
            Err(InventoryError::EnvelopeInvalid)
        ));

        let mut version: serde_json::Value = serde_json::from_slice(&valid).expect("JSON");
        version["formatVersion"] = "v2".into();
        assert!(matches!(
            decode_envelope(&serde_json::to_vec(&version).expect("JSON"), now),
            Err(InventoryError::EnvelopeVersion)
        ));

        let mut timestamp: serde_json::Value = serde_json::from_slice(&valid).expect("JSON");
        timestamp["capturedAt"] = "2026-08-23T01:02:03.000Z".into();
        assert!(matches!(
            decode_envelope(&serde_json::to_vec(&timestamp).expect("JSON"), now),
            Err(InventoryError::EnvelopeTimestamp)
        ));

        let future = encode_envelope(snapshot(), "2026-08-23T01:07:04Z".to_owned()).expect("envelope");
        assert!(matches!(decode_envelope(&future, now), Err(InventoryError::EnvelopeFuture)));
        let skew_boundary = encode_envelope(snapshot(), "2026-08-23T01:07:03Z".to_owned()).expect("envelope");
        assert_eq!(decode_envelope(&skew_boundary, now).expect("five-minute skew").age, Duration::ZERO);
        let old = encode_envelope(snapshot(), "2026-08-22T01:02:03Z".to_owned()).expect("envelope");
        assert_eq!(decode_envelope(&old, now).expect("old envelope").age, Duration::from_secs(24 * 60 * 60));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cancelled_publish_keeps_last_good_and_removes_staging_file() {
        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        let last_good = encode_envelope(snapshot(), "2026-08-23T01:02:03Z".to_owned()).expect("last good");
        store.replace_file("latest.json", &last_good, || false).expect("seed latest");
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        let replacement = InventorySnapshot::new("1.2.3", None, 2, 4, 1_001, 401, []).expect("replacement");
        let error = store
            .publish_latest_sync(replacement, "2026-08-23T02:02:03Z".to_owned(), &token)
            .expect_err("cancelled before commit");

        assert!(matches!(error, InventoryError::Cancelled));
        assert_eq!(fs::read(temp.path().join("inventory/latest.json")).expect("last good"), last_good);
        let entries = fs::read_dir(temp.path().join("inventory"))
            .expect("inventory directory")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec![std::ffi::OsString::from("latest.json")]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reader_bounds_the_opened_latest_file_and_reports_missing_state() {
        use std::os::unix::fs::OpenOptionsExt as _;

        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T01:02:03Z")
            .expect("time")
            .with_timezone(&chrono::Utc);
        assert!(matches!(store.read_latest(now), Err(InventoryError::StateMissing)));

        let latest = temp.path().join("inventory/latest.json");
        let mut latest_file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(FILE_MODE)
            .open(&latest)
            .expect("secure bounded latest");
        let bounded = vec![b'x'; MAX_PERSISTED_BYTES];
        latest_file.write_all(&bounded).expect("bounded latest");
        drop(latest_file);
        assert!(matches!(store.read_latest(now), Err(InventoryError::EnvelopeInvalid)));
        fs::write(&latest, vec![b'x'; MAX_PERSISTED_BYTES + 1]).expect("oversized latest");
        assert!(matches!(store.read_latest(now), Err(InventoryError::StateOversize)));
        fs::write(&latest, b"not-json").expect("corrupt latest");
        assert!(matches!(store.read_latest(now), Err(InventoryError::EnvelopeInvalid)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fifo_children_are_rejected_without_blocking_readers_or_writers() {
        let latest_temp = safe_tempdir();
        let latest_store = InventoryStateStore::from_state_root(latest_temp.path()).expect("latest store");
        let latest = latest_temp.path().join("inventory/latest.json");
        make_fifo(&latest);
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T01:02:03Z")
            .expect("time")
            .with_timezone(&chrono::Utc);
        assert!(matches!(latest_store.read_latest(now), Err(InventoryError::PersistenceSecurity)));
        assert!(matches!(
            latest_store.publish_latest_sync(
                snapshot(),
                "2026-08-23T01:02:03Z".to_owned(),
                &tokio_util::sync::CancellationToken::new()
            ),
            Err(InventoryError::PersistenceSecurity)
        ));

        let state_temp = safe_tempdir();
        let state_store = InventoryStateStore::from_state_root(state_temp.path()).expect("state store");
        let state = state_temp.path().join("inventory/state.json");
        make_fifo(&state);
        assert!(matches!(state_store.read(), Err(InventoryError::PersistenceSecurity)));
        assert!(matches!(
            state_store.write(&InventoryState::default()),
            Err(InventoryError::PersistenceSecurity)
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reader_rejects_insecure_file_modes_and_hardlinks() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        store
            .publish_latest_sync(snapshot(), "2026-08-23T01:02:03Z".to_owned(), &tokio_util::sync::CancellationToken::new())
            .expect("publish");
        let latest = temp.path().join("inventory/latest.json");
        fs::set_permissions(&latest, fs::Permissions::from_mode(0o4600)).expect("special-bit mode");
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T01:02:03Z")
            .expect("time")
            .with_timezone(&chrono::Utc);
        assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));

        fs::set_permissions(&latest, fs::Permissions::from_mode(0o644)).expect("mode");
        assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));

        fs::set_permissions(&latest, fs::Permissions::from_mode(0o600)).expect("mode");
        fs::hard_link(&latest, temp.path().join("inventory/second-link")).expect("hard link");
        assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));

        fs::remove_file(&latest).expect("remove latest link");
        symlink(temp.path().join("inventory/second-link"), &latest).expect("latest symlink");
        assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn store_rejects_unsafe_ancestors_and_symlinked_inventory_directory() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temp = safe_tempdir();
        let unsafe_parent = temp.path().join("unsafe");
        fs::create_dir(&unsafe_parent).expect("unsafe parent");
        fs::set_permissions(&unsafe_parent, fs::Permissions::from_mode(0o777)).expect("unsafe mode");
        let state = unsafe_parent.join("state");
        fs::create_dir(&state).expect("state root");
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).expect("state mode");
        assert!(matches!(
            InventoryStateStore::from_state_root(&state),
            Err(InventoryError::PersistenceSecurity)
        ));

        let safe_state = temp.path().join("safe-state");
        fs::create_dir(&safe_state).expect("safe state root");
        fs::set_permissions(&safe_state, fs::Permissions::from_mode(0o1700)).expect("special-bit state mode");
        assert!(matches!(
            InventoryStateStore::from_state_root(&safe_state),
            Err(InventoryError::PersistenceSecurity)
        ));
        fs::set_permissions(&safe_state, fs::Permissions::from_mode(0o700)).expect("state mode");
        symlink(temp.path(), safe_state.join("inventory")).expect("inventory symlink");
        assert!(matches!(
            InventoryStateStore::from_state_root(&safe_state),
            Err(InventoryError::PersistenceSecurity)
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unix_persistence_policy_rejects_wrong_owners_modes_and_link_counts() {
        let uid = 501;
        assert!(unix_directory_is_trusted(uid, 0o700, uid, true));
        assert!(!unix_directory_is_trusted(uid + 1, 0o700, uid, true));
        assert!(!unix_directory_is_trusted(uid, 0o755, uid, true));
        assert!(!unix_directory_is_trusted(uid, 0o1700, uid, true));
        assert!(unix_directory_is_trusted(0, 0o755, uid, false));
        assert!(!unix_directory_is_trusted(0, 0o777, uid, false));
        assert!(!unix_directory_is_trusted(0, 0o1755, uid, false));

        assert!(unix_regular_file_is_secure(uid, 0o600, 1, uid));
        assert!(!unix_regular_file_is_secure(uid + 1, 0o600, 1, uid));
        assert!(!unix_regular_file_is_secure(uid, 0o644, 1, uid));
        assert!(!unix_regular_file_is_secure(uid, 0o4600, 1, uid));
        assert!(!unix_regular_file_is_secure(uid, 0o600, 2, uid));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reader_rejects_a_real_wrong_owner_when_chown_is_permitted() {
        use std::os::unix::fs::chown;

        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        store
            .publish_latest_sync(snapshot(), "2026-08-23T01:02:03Z".to_owned(), &tokio_util::sync::CancellationToken::new())
            .expect("publish");
        let latest = temp.path().join("inventory/latest.json");
        let wrong_uid = process_uid().checked_add(1).unwrap_or_else(|| process_uid() - 1);
        if let Err(error) = chown(&latest, Some(wrong_uid), None) {
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied, "unexpected chown failure");
            return;
        }
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T01:02:03Z")
            .expect("time")
            .with_timezone(&chrono::Utc);
        assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn directory_component_exchange_is_rejected_by_the_open_anchor() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        store
            .publish_latest_sync(snapshot(), "2026-08-23T01:02:03Z".to_owned(), &tokio_util::sync::CancellationToken::new())
            .expect("publish");
        fs::rename(temp.path().join("inventory"), temp.path().join("original-inventory")).expect("exchange original");
        fs::create_dir(temp.path().join("inventory")).expect("replacement inventory");
        fs::set_permissions(temp.path().join("inventory"), fs::Permissions::from_mode(0o700)).expect("replacement mode");

        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T01:02:03Z")
            .expect("time")
            .with_timezone(&chrono::Utc);
        assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));
        assert!(matches!(
            store.publish_latest_sync(snapshot(), "2026-08-23T02:02:03Z".to_owned(), &tokio_util::sync::CancellationToken::new()),
            Err(InventoryError::PersistenceSecurity)
        ));
        assert!(!temp.path().join("inventory/latest.json").exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn state_root_and_ancestor_exchanges_are_rejected_by_the_path_anchor() {
        use std::os::unix::fs::PermissionsExt as _;

        for exchange_ancestor in [false, true] {
            let temp = safe_tempdir();
            let ancestor = temp.path().join("anchor");
            let state_root = ancestor.join("state");
            fs::create_dir_all(&state_root).expect("state root");
            fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o700)).expect("ancestor mode");
            fs::set_permissions(&state_root, fs::Permissions::from_mode(0o700)).expect("state mode");
            let store = InventoryStateStore::from_state_root(&state_root).expect("store");
            store
                .publish_latest_sync(snapshot(), "2026-08-23T01:02:03Z".to_owned(), &tokio_util::sync::CancellationToken::new())
                .expect("publish");

            let exchanged = if exchange_ancestor { &ancestor } else { &state_root };
            fs::rename(exchanged, temp.path().join("original")).expect("exchange original directory");
            fs::create_dir_all(&state_root).expect("replacement state root");
            fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o700)).expect("replacement ancestor mode");
            fs::set_permissions(&state_root, fs::Permissions::from_mode(0o700)).expect("replacement state mode");

            let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T01:02:03Z")
                .expect("time")
                .with_timezone(&chrono::Utc);
            assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));
            assert!(matches!(
                store.publish_latest_sync(
                    snapshot(),
                    "2026-08-23T02:02:03Z".to_owned(),
                    &tokio_util::sync::CancellationToken::new()
                ),
                Err(InventoryError::PersistenceSecurity)
            ));
            assert!(!state_root.join("inventory/latest.json").exists());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fresh_state_concurrency_creates_one_anchor_and_allows_one_runtime_owner() {
        let temp = safe_tempdir();
        let state_root = Arc::new(temp.path().to_path_buf());
        let start = Arc::new(std::sync::Barrier::new(3));
        let release = Arc::new(std::sync::Barrier::new(3));
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut threads = Vec::new();
        for _ in 0..2 {
            let state_root = state_root.clone();
            let start = start.clone();
            let release = release.clone();
            let result_tx = result_tx.clone();
            threads.push(std::thread::spawn(move || {
                start.wait();
                let result = InventoryStateStore::from_state_root(&state_root).and_then(|store| store.try_runtime_lock());
                result_tx
                    .send(result.as_ref().map(|_| ()).map_err(|error| error.to_string()))
                    .expect("result");
                release.wait();
                result
            }));
        }
        start.wait();
        let results = [
            result_rx.recv().expect("first result"),
            result_rx.recv().expect("second result"),
        ];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| { matches!(result, Err(error) if error.as_str() == "connect_inventory_already_running") })
                .count(),
            1
        );
        release.wait();
        for thread in threads {
            let _ = thread.join().expect("thread");
        }
        assert!(state_root.join("inventory/.state.json.lock").is_file());
        assert!(!state_root.join("inventory/.state.lock").exists());
    }

    #[cfg(target_os = "linux")]
    fn exchange_path_component(temp: &Path, state_root: &Path, component: &str) {
        use std::os::unix::fs::PermissionsExt as _;

        let ancestor = state_root.parent().expect("state ancestor");
        let inventory = state_root.join("inventory");
        let exchanged = match component {
            "ancestor" => ancestor,
            "state-root" => state_root,
            "inventory" => &inventory,
            _ => unreachable!(),
        };
        fs::rename(exchanged, temp.join(format!("original-{component}"))).expect("exchange path component");
        fs::create_dir_all(&inventory).expect("replacement inventory path");
        for directory in [ancestor, state_root, inventory.as_path()] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).expect("replacement directory mode");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn path_exchanges_during_reads_and_writes_fail_closed() {
        for operation in ["read", "write"] {
            for component in ["ancestor", "state-root", "inventory"] {
                let temp = safe_tempdir();
                let ancestor = temp.path().join("anchor");
                let state_root = ancestor.join("state");
                fs::create_dir_all(&state_root).expect("state root");
                use std::os::unix::fs::PermissionsExt as _;
                fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o700)).expect("ancestor mode");
                fs::set_permissions(&state_root, fs::Permissions::from_mode(0o700)).expect("state mode");
                let store = InventoryStateStore::from_state_root(&state_root).expect("store");
                let captured_at = "2026-08-23T01:02:03Z";
                store
                    .publish_latest_sync(snapshot(), captured_at.to_owned(), &tokio_util::sync::CancellationToken::new())
                    .expect("seed latest");

                let error = if operation == "read" {
                    let now = chrono::DateTime::parse_from_rfc3339(captured_at)
                        .expect("time")
                        .with_timezone(&chrono::Utc);
                    store
                        .read_latest_after_open(now, || exchange_path_component(temp.path(), &state_root, component))
                        .expect_err("path exchange during read")
                } else {
                    let replacement = encode_envelope(snapshot(), "2026-08-23T02:02:03Z".to_owned()).expect("replacement");
                    store
                        .replace_file_inner(
                            "latest.json",
                            &replacement,
                            || false,
                            None,
                            || exchange_path_component(temp.path(), &state_root, component),
                        )
                        .expect_err("path exchange before commit")
                };

                assert!(matches!(error, InventoryError::PersistenceSecurity));
                assert!(!state_root.join("inventory/latest.json").exists());
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn legacy_pending_does_not_replace_a_newer_local_snapshot() {
        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        let current = snapshot();
        store
            .publish_latest_sync(
                current.clone(),
                "2026-08-23T01:02:03Z".to_owned(),
                &tokio_util::sync::CancellationToken::new(),
            )
            .expect("current latest");
        let legacy = InventorySnapshot::new("1.2.3", None, 2, 4, 900, 300, []).expect("legacy snapshot");

        store
            .ensure_latest(legacy, "2026-08-23T02:02:03Z".to_owned(), tokio_util::sync::CancellationToken::new())
            .await
            .expect("existing latest remains authoritative");

        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T02:02:03Z")
            .expect("time")
            .with_timezone(&chrono::Utc);
        let persisted = store.read_latest(now).expect("latest");
        assert_eq!(persisted.snapshot, current);
        assert_eq!(persisted.captured_at, "2026-08-23T01:02:03Z");
        assert_eq!(persisted.age, Duration::from_secs(60 * 60));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn precommit_failures_preserve_last_good_and_postcommit_sync_failure_keeps_new_file() {
        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        let old = encode_envelope(snapshot(), "2026-08-23T01:02:03Z".to_owned()).expect("old envelope");
        store.replace_file("latest.json", &old, || false).expect("seed latest");
        let new_snapshot = InventorySnapshot::new("1.2.3", None, 2, 4, 1_001, 401, []).expect("new snapshot");
        let new = encode_envelope(new_snapshot, "2026-08-23T02:02:03Z".to_owned()).expect("new envelope");

        for fault in [PersistFault::Write, PersistFault::TempSync, PersistFault::Rename] {
            assert!(matches!(
                store.replace_file_inner("latest.json", &new, || false, Some(fault), || {}),
                Err(InventoryError::StateIo)
            ));
            assert_eq!(fs::read(temp.path().join("inventory/latest.json")).expect("last good"), old);
            assert_eq!(
                fs::read_dir(temp.path().join("inventory"))
                    .expect("inventory directory")
                    .filter_map(Result::ok)
                    .count(),
                1,
                "failed staging must be removed"
            );
        }

        assert!(matches!(
            store.replace_file_inner("latest.json", &new, || false, Some(PersistFault::DirectorySync), || {}),
            Err(InventoryError::DurabilityAfterCommit)
        ));
        assert_eq!(fs::read(temp.path().join("inventory/latest.json")).expect("committed latest"), new);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cancellation_during_commit_does_not_interrupt_rename_or_sync() {
        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        let replacement = encode_envelope(snapshot(), "2026-08-23T01:02:03Z".to_owned()).expect("envelope");
        let cancellation = tokio_util::sync::CancellationToken::new();

        store
            .replace_file_inner(
                "latest.json",
                &replacement,
                || cancellation.is_cancelled(),
                Some(PersistFault::CancelDuringCommit(cancellation.clone())),
                || {},
            )
            .expect("commit ignores cancellation after its cancellation gate");

        assert!(cancellation.is_cancelled());
        assert_eq!(
            fs::read(temp.path().join("inventory/latest.json")).expect("committed latest"),
            replacement
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reader_rejects_replacement_of_the_file_it_opened() {
        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        let captured_at = "2026-08-23T01:02:03Z";
        let first = encode_envelope(snapshot(), captured_at.to_owned()).expect("first envelope");
        let second_snapshot = InventorySnapshot::new("1.2.3", None, 2, 4, 1_001, 401, []).expect("second snapshot");
        let second = encode_envelope(second_snapshot.clone(), captured_at.to_owned()).expect("second envelope");
        store.replace_file("latest.json", &first, || false).expect("seed latest");
        let now = chrono::DateTime::parse_from_rfc3339(captured_at)
            .expect("time")
            .with_timezone(&chrono::Utc);

        assert!(matches!(
            store.read_latest_after_open(now, || {
                store
                    .replace_file("latest.json", &second, || false)
                    .expect("replace opened file");
            }),
            Err(InventoryError::PersistenceSecurity)
        ));
        assert_eq!(store.read_latest(now).expect("replacement").snapshot, second_snapshot);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn concurrent_reader_observes_only_complete_old_or_new_envelopes() {
        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        let first = snapshot();
        let second = InventorySnapshot::new("1.2.3", None, 2, 4, 1_001, 401, []).expect("second snapshot");
        let captured_at = "2026-08-23T01:02:03Z";
        store
            .replace_file(
                "latest.json",
                &encode_envelope(first.clone(), captured_at.to_owned()).expect("first envelope"),
                || false,
            )
            .expect("seed latest");
        let writer = store.clone();
        let first_writer = first.clone();
        let second_writer = second.clone();
        let start = Arc::new(std::sync::Barrier::new(2));
        let writer_start = start.clone();
        let thread = std::thread::spawn(move || {
            writer_start.wait();
            for index in 0..100 {
                let snapshot = if index % 2 == 0 {
                    first_writer.clone()
                } else {
                    second_writer.clone()
                };
                let bytes = encode_envelope(snapshot, captured_at.to_owned()).expect("envelope");
                let mut committed = false;
                for _ in 0..100 {
                    match writer.replace_file("latest.json", &bytes, || false) {
                        Ok(()) => {
                            committed = true;
                            break;
                        }
                        Err(InventoryError::StateIo | InventoryError::PersistenceSecurity) => {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                        Err(error) => panic!("atomic replace: {error}"),
                    }
                }
                assert!(committed, "atomic replace did not recover from transient fail-closed errors");
            }
        });
        let now = chrono::DateTime::parse_from_rfc3339(captured_at)
            .expect("time")
            .with_timezone(&chrono::Utc);
        start.wait();
        for _ in 0..100 {
            match store.read_latest(now) {
                Ok(observed) => assert!(observed.snapshot == first || observed.snapshot == second),
                Err(InventoryError::PersistenceSecurity) => {}
                Err(error) => panic!("reader observed neither a complete envelope nor a replacement: {error}"),
            }
        }
        thread.join().expect("writer");
        let observed = store.read_latest(now).expect("stable final envelope").snapshot;
        assert!(observed == first || observed == second);
    }
}
