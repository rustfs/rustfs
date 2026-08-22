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
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
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
#[cfg(unix)]
const FILE_MODE: u32 = 0o600;
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
        if self.node_count == 0 || self.node_count > 4096 {
            return Err(InventoryError::NodeCount);
        }
        if self.drive_count > 1_048_576 {
            return Err(InventoryError::DriveCount);
        }
        if self.capacity_total_bytes > MAX_SAFE_INTEGER || self.capacity_used_bytes > self.capacity_total_bytes {
            return Err(InventoryError::Capacity);
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
    path: PathBuf,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InventoryState {
    next_sequence: u64,
    pending: Option<PendingInventory>,
    last_accepted_content_hash: Option<String>,
}

impl InventoryStateStore {
    pub(crate) fn from_heartbeat_path(path: &Path) -> Result<Self, InventoryError> {
        let root = path.parent().and_then(Path::parent).ok_or(InventoryError::StatePath)?;
        Ok(Self {
            path: root.join("inventory/state.json"),
        })
    }

    pub(crate) fn try_runtime_lock(&self) -> Result<fs::File, InventoryError> {
        let directory = parent(&self.path)?;
        fs::create_dir_all(directory).map_err(|source| state_io(directory, source))?;
        let name = filename(&self.path)?;
        let path = directory.join(format!(".{name}.lock"));
        let mut options = fs::OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(FILE_MODE);
        }
        let lock = options.open(&path).map_err(|source| state_io(&path, source))?;
        check_mode(&path)?;
        lock.try_lock().map_err(|_| InventoryError::AlreadyRunning)?;
        Ok(lock)
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
        .map_err(|source| state_io(&self.path, io::Error::other(source)))?
    }

    pub(crate) async fn prepare(&self, snapshot: InventorySnapshot) -> Result<Option<PendingInventory>, InventoryError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.prepare_sync(snapshot))
            .await
            .map_err(|source| state_io(&self.path, io::Error::other(source)))?
    }

    pub(crate) async fn mark_accepted(&self, accepted: &PendingInventory) -> Result<(), InventoryError> {
        let store = self.clone();
        let accepted = accepted.clone();
        tokio::task::spawn_blocking(move || store.mark_accepted_sync(&accepted))
            .await
            .map_err(|source| state_io(&self.path, io::Error::other(source)))?
    }

    fn prepare_sync(&self, snapshot: InventorySnapshot) -> Result<Option<PendingInventory>, InventoryError> {
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
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(InventoryState::default()),
            Err(source) => return Err(state_io(&self.path, source)),
        };
        check_mode(&self.path)?;
        let state: InventoryState = serde_json::from_slice(&bytes).map_err(|source| InventoryError::StateInvalid {
            path: self.path.clone(),
            source,
        })?;
        let last_hash_valid = state.last_accepted_content_hash.as_deref().is_none_or(valid_content_hash);
        let pending_valid = state.pending.as_ref().is_none_or(|pending| {
            pending.sequence == state.next_sequence
                && pending.is_valid()
                && pending
                    .content_hash()
                    .is_ok_and(|hash| state.last_accepted_content_hash.as_deref() != Some(&hash))
        });
        if state.next_sequence > MAX_SEQUENCE + 1 || !last_hash_valid || !pending_valid {
            return Err(InventoryError::StateCorrupt { path: self.path.clone() });
        }
        Ok(state)
    }

    fn write(&self, state: &InventoryState) -> Result<(), InventoryError> {
        let bytes = serde_json::to_vec(state).map_err(|source| InventoryError::StateInvalid {
            path: self.path.clone(),
            source,
        })?;
        let directory = parent(&self.path)?;
        fs::create_dir_all(directory).map_err(|source| state_io(directory, source))?;
        let temp = stage(directory, &self.path, &bytes)?;
        let result = fs::rename(&temp, &self.path)
            .map_err(|source| state_io(&self.path, source))
            .and_then(|()| fsync_dir(directory).map_err(|source| state_io(directory, source)));
        if result.is_err() {
            let _ = fs::remove_file(temp);
        }
        result
    }
}

fn valid_content_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parent(path: &Path) -> Result<&Path, InventoryError> {
    path.parent()
        .ok_or_else(|| state_io(path, io::Error::new(io::ErrorKind::InvalidInput, "state path has no parent")))
}

fn filename(path: &Path) -> Result<&str, InventoryError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| state_io(path, io::Error::new(io::ErrorKind::InvalidInput, "state filename is invalid")))
}

fn stage(directory: &Path, destination: &Path, bytes: &[u8]) -> Result<PathBuf, InventoryError> {
    let name = filename(destination)?;
    loop {
        let path = directory.join(format!(
            ".{name}.{}.{}.tmp",
            std::process::id(),
            STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(FILE_MODE);
        }
        let mut file = match options.open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(state_io(&path, source)),
        };
        if let Err(source) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            let _ = fs::remove_file(&path);
            return Err(state_io(&path, source));
        }
        return Ok(path);
    }
}

fn state_io(path: &Path, source: io::Error) -> InventoryError {
    InventoryError::StateIo {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(unix)]
fn check_mode(path: &Path) -> Result<(), InventoryError> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = fs::metadata(path)
        .map_err(|source| state_io(path, source))?
        .permissions()
        .mode()
        & 0o7777;
    if mode != FILE_MODE {
        return Err(InventoryError::StatePermissions {
            path: path.to_path_buf(),
            mode,
            expected: FILE_MODE,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_mode(_path: &Path) -> Result<(), InventoryError> {
    Ok(())
}

fn fsync_dir(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error("the RustFS inventory version is outside protocol bounds")]
    RustfsVersion,
    #[error("the RustFS inventory operating-system version is outside protocol bounds")]
    OsVersion,
    #[error("the RustFS inventory node count is outside protocol bounds")]
    NodeCount,
    #[error("the RustFS inventory drive count is outside protocol bounds")]
    DriveCount,
    #[error("the RustFS inventory capacity is outside protocol bounds")]
    Capacity,
    #[error("the Connect inventory schedule is invalid")]
    Schedule,
    #[error("the Connect inventory sequence is exhausted")]
    SequenceExhausted,
    #[error("a Connect inventory runtime already owns this state")]
    AlreadyRunning,
    #[error("the persisted Connect inventory changed while delivery was in flight")]
    StateConflict,
    #[error("the Connect inventory state path is invalid")]
    StatePath,
    #[error("Connect inventory state I/O failed at {path}: {source}")]
    StateIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Connect inventory state at {path} is invalid: {source}")]
    StateInvalid {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("Connect inventory state at {path} violates the protocol invariants")]
    StateCorrupt { path: PathBuf },
    #[cfg(unix)]
    #[error("Connect inventory state at {path} has mode {mode:o}, expected {expected:o}")]
    StatePermissions { path: PathBuf, mode: u32, expected: u32 },
    #[error("Connect returned an invalid inventory response")]
    Response,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("Connect inventory delivery failed: {0}")]
    Telemetry(String),
}

impl From<TelemetryError> for InventoryError {
    fn from(error: TelemetryError) -> Self {
        Self::Telemetry(error.to_string())
    }
}
