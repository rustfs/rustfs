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

use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::config::HeartbeatConfig;
use super::credential_store::CredentialStoreError;
use super::identity::IdentityError;
use super::identity_store::StoreError;
use super::registration::CredentialValidationError;
use super::telemetry::{TelemetryDelivery, TelemetryError, TelemetryTransport, is_exact_utc_seconds};

const PROTOCOL_VERSION: &str = "v1";
const AGENT_VERSION: &str = concat!("rustfs-agent/", env!("CARGO_PKG_VERSION"));
const MAX_SEQUENCE: u64 = 9_007_199_254_740_991;
#[cfg(unix)]
const FILE_MODE: u32 = 0o600;
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CoarseNodeSummary {
    total: u16,
    healthy: u16,
    degraded: u16,
}

impl CoarseNodeSummary {
    pub fn new(total: u16, healthy: u16, degraded: u16) -> Result<Self, HeartbeatError> {
        let summary = Self {
            total,
            healthy,
            degraded,
        };
        if !summary.is_valid() {
            return Err(HeartbeatError::NodeSummary);
        }
        Ok(summary)
    }

    fn is_valid(&self) -> bool {
        self.total != 0
            && self.total <= 4096
            && self.healthy <= 4096
            && self.degraded <= 4096
            && self.healthy.saturating_add(self.degraded) <= self.total
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeartbeatStatus {
    Starting,
    Online { server_time: String },
    BackingOff { delay: Duration },
    AuthenticationStopped { status: u16, reason: Option<String> },
    Failed { reason: String },
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct PendingHeartbeat {
    protocol_version: String,
    request_id: String,
    agent_version: String,
    capabilities: [String; 1],
    sequence: u64,
    client_time: String,
    coarse_node_summary: CoarseNodeSummary,
}

impl PendingHeartbeat {
    fn is_valid(&self) -> bool {
        self.protocol_version == PROTOCOL_VERSION
            && self.agent_version == AGENT_VERSION
            && self.capabilities[0] == "heartbeat"
            && self.sequence <= MAX_SEQUENCE
            && self.coarse_node_summary.is_valid()
            && is_exact_utc_seconds(&self.client_time)
            && Uuid::parse_str(&self.request_id)
                .is_ok_and(|request_id| request_id.get_version_num() == 4 && request_id.to_string() == self.request_id)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeartbeatResponse {
    server_time: String,
    accepted_version: String,
    #[serde(default)]
    capability_hints: Vec<String>,
}

pub(crate) enum Delivery {
    Accepted { server_time: String },
    Retry { retry_after: Option<Duration> },
    AuthenticationStopped { status: u16, reason: Option<String> },
    Rejected { status: u16, reason: Option<String> },
}

pub(crate) struct HeartbeatSender {
    transport: TelemetryTransport,
}

impl HeartbeatSender {
    pub(crate) fn new(config: HeartbeatConfig) -> Result<Self, HeartbeatError> {
        let schedule = config.schedule;
        if schedule.cadence.is_zero() || schedule.jitter > schedule.cadence {
            return Err(HeartbeatError::Schedule);
        }
        Ok(Self {
            transport: TelemetryTransport::new(config)?,
        })
    }

    pub(crate) async fn send(&self, heartbeat: &PendingHeartbeat) -> Result<Delivery, HeartbeatError> {
        match self.transport.post("heartbeats", heartbeat).await? {
            TelemetryDelivery::Accepted { body, .. } => {
                let accepted: HeartbeatResponse = serde_json::from_slice(&body).map_err(|_| HeartbeatError::Response)?;
                if accepted.accepted_version != PROTOCOL_VERSION
                    || accepted.capability_hints.len() > 32
                    || accepted.capability_hints.iter().any(|hint| hint.len() > 32)
                    || !is_exact_utc_seconds(&accepted.server_time)
                {
                    return Err(HeartbeatError::Response);
                }
                Ok(Delivery::Accepted {
                    server_time: accepted.server_time,
                })
            }
            TelemetryDelivery::Retry { retry_after } => Ok(Delivery::Retry { retry_after }),
            TelemetryDelivery::AuthenticationStopped { status, reason } => Ok(Delivery::AuthenticationStopped { status, reason }),
            TelemetryDelivery::Rejected { status, reason } => Ok(Delivery::Rejected { status, reason }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct HeartbeatStateStore {
    path: PathBuf,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HeartbeatState {
    next_sequence: u64,
    pending: Option<PendingHeartbeat>,
}

impl HeartbeatStateStore {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub(crate) fn try_runtime_lock(&self) -> Result<fs::File, HeartbeatError> {
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
        lock.try_lock().map_err(|_| HeartbeatError::AlreadyRunning)?;
        Ok(lock)
    }

    pub(crate) async fn prepare(
        &self,
        summary: CoarseNodeSummary,
        now: DateTime<Utc>,
    ) -> Result<PendingHeartbeat, HeartbeatError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.prepare_sync(summary, now))
            .await
            .map_err(|source| state_io(&self.path, io::Error::other(source)))?
    }

    pub(crate) async fn mark_accepted(&self, accepted: &PendingHeartbeat) -> Result<(), HeartbeatError> {
        let store = self.clone();
        let accepted = accepted.clone();
        tokio::task::spawn_blocking(move || store.mark_accepted_sync(&accepted))
            .await
            .map_err(|source| state_io(&self.path, io::Error::other(source)))?
    }

    fn prepare_sync(&self, summary: CoarseNodeSummary, now: DateTime<Utc>) -> Result<PendingHeartbeat, HeartbeatError> {
        let mut state = self.read()?;
        if let Some(pending) = state.pending {
            return Ok(pending);
        }
        if state.next_sequence > MAX_SEQUENCE {
            return Err(HeartbeatError::SequenceExhausted);
        }
        let pending = PendingHeartbeat {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            request_id: Uuid::new_v4().to_string(),
            agent_version: AGENT_VERSION.to_owned(),
            capabilities: ["heartbeat".to_owned()],
            sequence: state.next_sequence,
            client_time: now.to_rfc3339_opts(SecondsFormat::Secs, true),
            coarse_node_summary: summary,
        };
        state.pending = Some(pending.clone());
        self.write(&state)?;
        Ok(pending)
    }

    fn mark_accepted_sync(&self, accepted: &PendingHeartbeat) -> Result<(), HeartbeatError> {
        let mut state = self.read()?;
        if state.pending.as_ref() != Some(accepted) {
            return Err(HeartbeatError::StateConflict);
        }
        state.next_sequence = accepted.sequence.checked_add(1).ok_or(HeartbeatError::SequenceExhausted)?;
        state.pending = None;
        self.write(&state)
    }

    fn read(&self) -> Result<HeartbeatState, HeartbeatError> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(HeartbeatState::default()),
            Err(source) => return Err(state_io(&self.path, source)),
        };
        check_mode(&self.path)?;
        let state: HeartbeatState = serde_json::from_slice(&bytes).map_err(|source| HeartbeatError::StateInvalid {
            path: self.path.clone(),
            source,
        })?;
        if state.next_sequence > MAX_SEQUENCE + 1
            || state
                .pending
                .as_ref()
                .is_some_and(|pending| pending.sequence != state.next_sequence || !pending.is_valid())
        {
            return Err(HeartbeatError::StateCorrupt { path: self.path.clone() });
        }
        Ok(state)
    }

    fn write(&self, state: &HeartbeatState) -> Result<(), HeartbeatError> {
        let bytes = serde_json::to_vec(state).map_err(|source| HeartbeatError::StateInvalid {
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

fn parent(path: &Path) -> Result<&Path, HeartbeatError> {
    path.parent()
        .ok_or_else(|| state_io(path, io::Error::new(io::ErrorKind::InvalidInput, "state path has no parent")))
}

fn filename(path: &Path) -> Result<&str, HeartbeatError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| state_io(path, io::Error::new(io::ErrorKind::InvalidInput, "state filename is invalid")))
}

fn stage(directory: &Path, destination: &Path, bytes: &[u8]) -> Result<PathBuf, HeartbeatError> {
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

fn state_io(path: &Path, source: io::Error) -> HeartbeatError {
    HeartbeatError::StateIo {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(unix)]
fn check_mode(path: &Path) -> Result<(), HeartbeatError> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = fs::metadata(path)
        .map_err(|source| state_io(path, source))?
        .permissions()
        .mode()
        & 0o7777;
    if mode != FILE_MODE {
        return Err(HeartbeatError::StatePermissions {
            path: path.to_path_buf(),
            mode,
            expected: FILE_MODE,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_mode(_path: &Path) -> Result<(), HeartbeatError> {
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
pub enum HeartbeatError {
    #[error("Connect heartbeat endpoint must be an HTTPS base URL without credentials, query, or fragment")]
    Endpoint,
    #[error("Connect heartbeat root CA configuration is invalid")]
    RootCertificate,
    #[error("Connect heartbeat schedule is invalid")]
    Schedule,
    #[error("RustFS is not registered with Connect")]
    NotRegistered,
    #[error("the Connect device private key is missing")]
    IdentityMissing,
    #[error("the stored Connect certificate and device private key cannot form a TLS identity")]
    IdentityCertificate,
    #[error("the stored Connect credential name is invalid")]
    CredentialName,
    #[error("the stored Connect device certificate is not currently valid")]
    CredentialExpired,
    #[error("the Connect heartbeat node summary is outside protocol bounds")]
    NodeSummary,
    #[error("the Connect heartbeat sequence is exhausted")]
    SequenceExhausted,
    #[error("a Connect heartbeat runtime already owns this state")]
    AlreadyRunning,
    #[error("the persisted Connect heartbeat changed while delivery was in flight")]
    StateConflict,
    #[error("Connect heartbeat state I/O failed at {path}: {source}")]
    StateIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Connect heartbeat state at {path} is invalid: {source}")]
    StateInvalid {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("Connect heartbeat state at {path} violates the protocol invariants")]
    StateCorrupt { path: PathBuf },
    #[cfg(unix)]
    #[error("Connect heartbeat state at {path} has mode {mode:o}, expected {expected:o}")]
    StatePermissions { path: PathBuf, mode: u32, expected: u32 },
    #[error("Connect heartbeat response exceeded 64 KiB")]
    ResponseTooLarge,
    #[error("Connect returned an invalid heartbeat response")]
    Response,
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    IdentityStore(#[from] StoreError),
    #[error(transparent)]
    CredentialStore(#[from] CredentialStoreError),
    #[error(transparent)]
    CredentialValidation(#[from] CredentialValidationError),
}

impl From<TelemetryError> for HeartbeatError {
    fn from(error: TelemetryError) -> Self {
        match error {
            TelemetryError::Endpoint => Self::Endpoint,
            TelemetryError::RootCertificate => Self::RootCertificate,
            TelemetryError::Schedule => Self::Schedule,
            TelemetryError::NotRegistered => Self::NotRegistered,
            TelemetryError::IdentityMissing => Self::IdentityMissing,
            TelemetryError::IdentityCertificate => Self::IdentityCertificate,
            TelemetryError::CredentialName => Self::CredentialName,
            TelemetryError::CredentialExpired => Self::CredentialExpired,
            TelemetryError::StateConflict => Self::StateConflict,
            TelemetryError::ResponseTooLarge => Self::ResponseTooLarge,
            TelemetryError::Url(error) => Self::Url(error),
            TelemetryError::Transport(error) => Self::Transport(error),
            TelemetryError::Identity(error) => Self::Identity(error),
            TelemetryError::IdentityStore(error) => Self::IdentityStore(error),
            TelemetryError::CredentialStore(error) => Self::CredentialStore(error),
            TelemetryError::CredentialValidation(error) => Self::CredentialValidation(error),
        }
    }
}
