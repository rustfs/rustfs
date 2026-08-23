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
#[cfg(any(unix, test))]
use std::io;
#[cfg(unix)]
use std::io::Read as _;
#[cfg(unix)]
use std::io::Write as _;
use std::path::Path;
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
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
#[cfg(unix)]
const MAX_PERSISTED_BYTES: usize = 16 * 1024;
#[allow(dead_code)] // Kept for the crate-private stopped-server reader consumed by R06.
const MAX_FUTURE_SKEW: Duration = Duration::from_secs(5 * 60);
#[cfg(unix)]
const FILE_MODE: u32 = 0o600;
#[cfg(unix)]
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
}

pub(crate) enum InventoryDelivery {
    Accepted { content_hash: String, received_at: String },
    Retry { retry_after: Option<Duration> },
    AuthenticationStopped { status: u16, reason: Option<String> },
    Rejected { status: u16, reason: Option<String> },
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
            TelemetryDelivery::AuthenticationStopped { status, reason } => {
                Ok(InventoryDelivery::AuthenticationStopped { status, reason })
            }
            TelemetryDelivery::Rejected { status, reason } => Ok(InventoryDelivery::Rejected { status, reason }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InventoryStateStore {
    #[cfg(unix)]
    directory: Arc<fs::File>,
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
        #[cfg(not(unix))]
        {
            let _ = path;
            return Err(InventoryError::PlatformSecurity);
        }
        #[cfg(unix)]
        Ok(Self {
            directory: Arc::new(open_inventory_directory(path)?),
        })
    }

    pub(crate) fn try_runtime_lock(&self) -> Result<fs::File, InventoryError> {
        #[cfg(not(unix))]
        {
            Err(InventoryError::PlatformSecurity)
        }
        #[cfg(unix)]
        {
            validate_directory(&self.directory, true)?;
            let lock = open_file_at(&self.directory, ".state.lock", true, true)?;
            validate_regular_file(&lock)?;
            lock.try_lock().map_err(|_| InventoryError::AlreadyRunning)?;
            Ok(lock)
        }
    }

    pub(crate) async fn pending(&self) -> Result<Option<PendingInventory>, InventoryError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let state = store.read()?;
            if state.pending.is_none() && state.next_sequence > MAX_SEQUENCE {
                return Err(InventoryError::SequenceExhausted);
            }
            Ok(state.pending)
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

    #[allow(dead_code)] // R06 reads this after the server has stopped.
    pub(crate) fn read_latest(&self, now: chrono::DateTime<chrono::Utc>) -> Result<PersistedInventory, InventoryError> {
        #[cfg(not(unix))]
        return Err(InventoryError::PlatformSecurity);
        #[cfg(unix)]
        {
            validate_directory(&self.directory, true)?;
            let mut file = open_file_at(&self.directory, "latest.json", false, false)?;
            validate_regular_file(&file)?;
            let before = file_identity(&file)?;
            let bytes = read_bounded(&mut file)?;
            validate_regular_file(&file)?;
            if before != file_identity(&file)? {
                return Err(InventoryError::PersistenceSecurity);
            }
            decode_envelope(&bytes, now)
        }
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
        #[cfg(not(unix))]
        {
            Err(InventoryError::PlatformSecurity)
        }
        #[cfg(unix)]
        {
            validate_directory(&self.directory, true)?;
            let mut file = match open_file_at(&self.directory, "state.json", false, false) {
                Ok(file) => file,
                Err(InventoryError::StateMissing) => return Ok(InventoryState::default()),
                Err(error) => return Err(error),
            };
            validate_regular_file(&file)?;
            let bytes = read_bounded(&mut file)?;
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
            Ok(state)
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
            #[cfg(test)]
            None,
        )
    }

    fn replace_file_inner(
        &self,
        destination: &str,
        bytes: &[u8],
        cancelled: impl FnOnce() -> bool,
        #[cfg(test)] fault: Option<PersistFault>,
    ) -> Result<(), InventoryError> {
        #[cfg(not(unix))]
        {
            let _ = (destination, bytes, cancelled);
            return Err(InventoryError::PlatformSecurity);
        }
        #[cfg(unix)]
        {
            validate_directory(&self.directory, true)?;
            match open_file_at(&self.directory, destination, false, false) {
                Ok(existing) => validate_regular_file(&existing)?,
                Err(InventoryError::StateMissing) => {}
                Err(error) => return Err(error),
            }
            let (temp_name, mut temp) = stage_at(&self.directory, destination)?;
            #[cfg(test)]
            let injected_write = matches!(fault, Some(PersistFault::Write));
            #[cfg(not(test))]
            let injected_write = false;
            let staged = if injected_write {
                Err(InventoryError::StateIo)
            } else {
                temp.write_all(bytes).map_err(|_| InventoryError::StateIo)
            }
            .and_then(|()| {
                #[cfg(test)]
                if matches!(fault, Some(PersistFault::TempSync)) {
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
            let rename_failed = matches!(fault, Some(PersistFault::Rename));
            #[cfg(not(test))]
            let rename_failed = false;
            if rename_failed || rename_at(&self.directory, &temp_name, destination).is_err() {
                let _ = unlink_at(&self.directory, &temp_name);
                return Err(InventoryError::StateIo);
            }
            #[cfg(test)]
            if matches!(fault, Some(PersistFault::DirectorySync)) {
                return Err(InventoryError::DurabilityAfterCommit);
            }
            self.directory.sync_all().map_err(|_| InventoryError::DurabilityAfterCommit)
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum PersistFault {
    Write,
    TempSync,
    Rename,
    DirectorySync,
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

#[cfg(unix)]
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

#[cfg(unix)]
#[allow(unsafe_code)]
fn open_inventory_directory(path: &Path) -> Result<fs::File, InventoryError> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::path::Component;

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(|_| InventoryError::StateIo)?.join(path)
    };
    let mut directory = fs::File::open("/").map_err(|_| InventoryError::StateIo)?;
    validate_directory(&directory, false)?;
    let components = absolute.components().collect::<Vec<_>>();
    let names = components
        .iter()
        .filter_map(|component| match component {
            Component::Normal(name) => Some(*name),
            Component::RootDir => None,
            _ => Some(std::ffi::OsStr::new("")),
        })
        .collect::<Vec<_>>();
    if names.iter().any(|name| name.is_empty()) || names.is_empty() {
        return Err(InventoryError::StatePath);
    }
    for (index, name) in names.iter().enumerate() {
        use std::os::unix::ffi::OsStrExt as _;
        let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| InventoryError::StatePath)?;
        // SAFETY: the parent descriptor and C string are valid for this call; ownership of a successful descriptor is transferred.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
            )
        };
        if fd < 0 {
            return Err(InventoryError::PersistenceSecurity);
        }
        // SAFETY: openat returned a new owned descriptor.
        directory = unsafe { fs::File::from_raw_fd(fd) };
        validate_directory(&directory, index + 1 == names.len())?;
    }

    let inventory = c_name("inventory")?;
    // SAFETY: descriptor and C string are valid; mode is applied only if the directory is created.
    if unsafe { libc::mkdirat(directory.as_raw_fd(), inventory.as_ptr(), 0o700) } != 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(InventoryError::StateIo);
        }
    }
    let child = open_directory_at(&directory, "inventory")?;
    validate_directory(&child, true)?;
    sync_inventory_anchor(&child, &directory)?;
    Ok(child)
}

#[cfg(unix)]
fn sync_inventory_anchor(inventory: &fs::File, state_root: &fs::File) -> Result<(), InventoryError> {
    sync_inventory_anchor_with(|inventory_target| {
        if inventory_target {
            inventory.sync_all()
        } else {
            state_root.sync_all()
        }
    })
}

#[cfg(any(unix, test))]
fn sync_inventory_anchor_with(mut sync: impl FnMut(bool) -> io::Result<()>) -> Result<(), InventoryError> {
    sync(true).map_err(|_| InventoryError::StateIo)?;
    sync(false).map_err(|_| InventoryError::StateIo)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn open_directory_at(parent: &fs::File, name: &str) -> Result<fs::File, InventoryError> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    let name = c_name(name)?;
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

#[cfg(unix)]
fn validate_directory(directory: &fs::File, dedicated: bool) -> Result<(), InventoryError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let metadata = directory.metadata().map_err(|_| InventoryError::StateIo)?;
    let mode = metadata.permissions().mode() & 0o777;
    let uid = process_uid();
    if !metadata.is_dir() || !unix_directory_is_trusted(metadata.uid(), mode, uid, dedicated) {
        return Err(InventoryError::PersistenceSecurity);
    }
    Ok(())
}

#[cfg(unix)]
fn unix_directory_is_trusted(owner: u32, mode: u32, process: u32, dedicated: bool) -> bool {
    let trusted_owner = owner == process || (!dedicated && owner == 0);
    let trusted_mode = if dedicated { mode == 0o700 } else { mode & 0o022 == 0 };
    trusted_owner && trusted_mode
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn open_file_at(directory: &fs::File, name: &str, create: bool, write: bool) -> Result<fs::File, InventoryError> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    let name = c_name(name)?;
    let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW;
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
fn validate_regular_file(file: &fs::File) -> Result<(), InventoryError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let metadata = file.metadata().map_err(|_| InventoryError::StateIo)?;
    if !metadata.is_file()
        || !unix_regular_file_is_secure(metadata.uid(), metadata.permissions().mode() & 0o777, metadata.nlink(), process_uid())
    {
        return Err(InventoryError::PersistenceSecurity);
    }
    Ok(())
}

#[cfg(unix)]
fn unix_regular_file_is_secure(owner: u32, mode: u32, links: u64, process: u32) -> bool {
    owner == process && mode == FILE_MODE && links == 1
}

#[cfg(unix)]
#[allow(dead_code)] // Used by the crate-private stopped-server reader.
fn file_identity(file: &fs::File) -> Result<(u64, u64, u64), InventoryError> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = file.metadata().map_err(|_| InventoryError::StateIo)?;
    Ok((metadata.dev(), metadata.ino(), metadata.nlink()))
}

#[cfg(unix)]
fn c_name(name: &str) -> Result<std::ffi::CString, InventoryError> {
    std::ffi::CString::new(name).map_err(|_| InventoryError::StatePath)
}

#[cfg(unix)]
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
        tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).expect("safe temporary directory")
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

    #[cfg(unix)]
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

    #[cfg(unix)]
    #[test]
    fn reader_bounds_the_opened_latest_file_and_reports_missing_state() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T01:02:03Z")
            .expect("time")
            .with_timezone(&chrono::Utc);
        assert!(matches!(store.read_latest(now), Err(InventoryError::StateMissing)));

        let latest = temp.path().join("inventory/latest.json");
        fs::write(&latest, vec![b'x'; MAX_PERSISTED_BYTES]).expect("bounded latest");
        fs::set_permissions(&latest, fs::Permissions::from_mode(0o600)).expect("mode");
        assert!(matches!(store.read_latest(now), Err(InventoryError::EnvelopeInvalid)));
        fs::write(&latest, vec![b'x'; MAX_PERSISTED_BYTES + 1]).expect("oversized latest");
        assert!(matches!(store.read_latest(now), Err(InventoryError::StateOversize)));
        fs::write(&latest, b"not-json").expect("corrupt latest");
        assert!(matches!(store.read_latest(now), Err(InventoryError::EnvelopeInvalid)));
    }

    #[cfg(unix)]
    #[test]
    fn reader_rejects_insecure_file_modes_and_hardlinks() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temp = safe_tempdir();
        let store = InventoryStateStore::from_state_root(temp.path()).expect("store");
        store
            .publish_latest_sync(snapshot(), "2026-08-23T01:02:03Z".to_owned(), &tokio_util::sync::CancellationToken::new())
            .expect("publish");
        let latest = temp.path().join("inventory/latest.json");
        fs::set_permissions(&latest, fs::Permissions::from_mode(0o644)).expect("mode");
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-23T01:02:03Z")
            .expect("time")
            .with_timezone(&chrono::Utc);
        assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));

        fs::set_permissions(&latest, fs::Permissions::from_mode(0o600)).expect("mode");
        fs::hard_link(&latest, temp.path().join("inventory/second-link")).expect("hard link");
        assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));

        fs::remove_file(&latest).expect("remove latest link");
        symlink(temp.path().join("inventory/second-link"), &latest).expect("latest symlink");
        assert!(matches!(store.read_latest(now), Err(InventoryError::PersistenceSecurity)));
    }

    #[cfg(unix)]
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
        fs::set_permissions(&safe_state, fs::Permissions::from_mode(0o700)).expect("state mode");
        symlink(temp.path(), safe_state.join("inventory")).expect("inventory symlink");
        assert!(matches!(
            InventoryStateStore::from_state_root(&safe_state),
            Err(InventoryError::PersistenceSecurity)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn unix_persistence_policy_rejects_wrong_owners_modes_and_link_counts() {
        let uid = 501;
        assert!(unix_directory_is_trusted(uid, 0o700, uid, true));
        assert!(!unix_directory_is_trusted(uid + 1, 0o700, uid, true));
        assert!(!unix_directory_is_trusted(uid, 0o755, uid, true));
        assert!(unix_directory_is_trusted(0, 0o755, uid, false));
        assert!(!unix_directory_is_trusted(0, 0o777, uid, false));

        assert!(unix_regular_file_is_secure(uid, 0o600, 1, uid));
        assert!(!unix_regular_file_is_secure(uid + 1, 0o600, 1, uid));
        assert!(!unix_regular_file_is_secure(uid, 0o644, 1, uid));
        assert!(!unix_regular_file_is_secure(uid, 0o600, 2, uid));
    }

    #[cfg(unix)]
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
    }

    #[cfg(unix)]
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
                store.replace_file_inner("latest.json", &new, || false, Some(fault)),
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
            store.replace_file_inner("latest.json", &new, || false, Some(PersistFault::DirectorySync)),
            Err(InventoryError::DurabilityAfterCommit)
        ));
        assert_eq!(fs::read(temp.path().join("inventory/latest.json")).expect("committed latest"), new);
    }

    #[cfg(unix)]
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
                writer.replace_file("latest.json", &bytes, || false).expect("atomic replace");
            }
        });
        let now = chrono::DateTime::parse_from_rfc3339(captured_at)
            .expect("time")
            .with_timezone(&chrono::Utc);
        start.wait();
        for _ in 0..100 {
            let observed = store.read_latest(now).expect("complete envelope").snapshot;
            assert!(observed == first || observed == second);
        }
        thread.join().expect("writer");
    }
}
