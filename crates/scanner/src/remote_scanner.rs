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

use crate::scanner_budget::{ScannerCycleBudget, ScannerCycleBudgetConfig};
use crate::scanner_io::{
    ScannerDiskScanOutcome, ScannerIODisk, cache_root_entry_info, cache_snapshot_is_current, scanner_cache_lock_resource,
    scanner_cache_lock_timeout, scanner_set_disk_inventory,
};
use crate::storage_api::owner::NS_SCANNER_PROTOCOL_VERSION;
use crate::storage_api::scan::NamespaceLocking as _;
use crate::{
    DATA_USAGE_BLOOM_NAME_PATH, DATA_USAGE_CACHE_NAME, DataUsageCache, DataUsageCachePrepareOutcome, DataUsageCacheSource,
    DataUsageEntryInfo, DataUsageScanPlanDigest, Disk, EcstoreError, RUSTFS_META_BUCKET, ScannerDiskExt as _, ScannerError,
    ScannerObjectIO, StorageError, is_reserved_or_invalid_bucket, read_config, resolve_scanner_object_store_handle,
};
use hmac::{Hmac, KeyInit, Mac};
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::metrics::{Metric, Metrics};
use rustfs_credentials::try_get_rpc_token;
use rustfs_utils::path::path_join_buf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::future::Future;
use std::io::{Error as IoError, ErrorKind};
use std::pin::Pin;
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicU8, Ordering},
};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

const NS_SCANNER_MAX_FRAME_SIZE: usize = 2 * 1024 * 1024;
const NS_SCANNER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const NS_SCANNER_BUDGET_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(250);
const NS_SCANNER_STALL_TIMEOUT: Duration = Duration::from_secs(15);
const NS_SCANNER_SEMANTIC_STALL_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const NS_SCANNER_WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const NS_SCANNER_MAX_RPC_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);
const NS_SCANNER_DISCONNECT_GRACE_MAX: Duration = Duration::from_secs(2 * 60);
const NS_SCANNER_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(250);
const NS_SCANNER_FENCE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const NS_SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const NS_SCANNER_DISK_HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
const NS_SCANNER_VALIDATED_CYCLE_TTL: Duration = Duration::from_secs(1);
const NS_SCANNER_MAX_REPLAY_SESSIONS: usize = 65_536;
const NS_SCANNER_MAX_ERROR_CHARS: usize = 4096;
const NS_SCANNER_FRAME_AUTH_DOMAIN: &[u8] = b"rustfs-ns-scanner-frame-v3";

pub const NS_SCANNER_MAX_REQUEST_BODY_SIZE: usize = 16 * 1024;

static REMOTE_SCANNER_REPLAY_CACHE: LazyLock<Mutex<RemoteScannerReplayCache>> =
    LazyLock::new(|| Mutex::new(RemoteScannerReplayCache::default()));
static REMOTE_SCANNER_ADMISSION: LazyLock<Mutex<HashMap<String, Arc<Semaphore>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static REMOTE_SCANNER_VALIDATED_CYCLE: LazyLock<Mutex<Option<RemoteScannerValidatedCycle>>> = LazyLock::new(|| Mutex::new(None));
static REMOTE_SCANNER_CYCLE_REFRESH: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));

pub struct RemoteScannerAdmission {
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteScannerBudget {
    max_duration_ms: Option<u64>,
    max_objects: Option<u64>,
    max_directories: Option<u64>,
}

impl RemoteScannerBudget {
    fn from_config(config: ScannerCycleBudgetConfig) -> Self {
        Self {
            max_duration_ms: config
                .max_duration
                .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX).max(1)),
            max_objects: config.max_objects,
            max_directories: config.max_directories,
        }
    }

    fn into_config(self) -> ScannerCycleBudgetConfig {
        ScannerCycleBudgetConfig {
            max_duration: self.max_duration_ms.map(Duration::from_millis),
            max_objects: self.max_objects,
            max_directories: self.max_directories,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteScannerRequestWire {
    version: u16,
    request_id: Uuid,
    server_epoch: Uuid,
    session_id: Uuid,
    session_sequence: u64,
    bucket: String,
    next_cycle: u64,
    leader_epoch: u64,
    scan_plan_digest: DataUsageScanPlanDigest,
    skip_healing: bool,
    scan_mode: HealScanMode,
    budget: RemoteScannerBudget,
}

#[derive(Debug)]
pub struct RemoteScannerRequest(RemoteScannerRequestWire);

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RemoteScannerProgress {
    objects_scanned: u64,
    directories_started: u64,
    entries_visited: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RemoteScannerPhase {
    #[default]
    Scanning,
    Persisting,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteScannerComplete {
    source: DataUsageCacheSource,
    scan_plan_digest: DataUsageScanPlanDigest,
    usage: DataUsageEntryInfo,
    pending_maintenance_work: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RemoteScannerErrorScope {
    Bucket,
    Worker,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteScannerErrorFrame {
    scope: RemoteScannerErrorScope,
    message: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum RemoteScannerFrameResult {
    Progress,
    Complete(Box<RemoteScannerComplete>),
    Partial,
    NamespaceNotFound,
    CycleAhead { required_cycle: u64 },
    Error(RemoteScannerErrorFrame),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteScannerFrame {
    progress: RemoteScannerProgress,
    phase: RemoteScannerPhase,
    result: RemoteScannerFrameResult,
}

impl RemoteScannerFrame {
    #[cfg(test)]
    fn progress(progress: RemoteScannerProgress) -> Self {
        Self {
            progress,
            phase: RemoteScannerPhase::Scanning,
            result: RemoteScannerFrameResult::Progress,
        }
    }

    #[cfg(test)]
    fn terminal(progress: RemoteScannerProgress, result: RemoteScannerFrameResult) -> Self {
        Self {
            progress,
            phase: RemoteScannerPhase::Persisting,
            result,
        }
    }

    fn with_phase(progress: RemoteScannerProgress, phase: RemoteScannerPhase, result: RemoteScannerFrameResult) -> Self {
        Self { progress, phase, result }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteScannerFrameEnvelope {
    version: u16,
    sequence: u64,
    payload: Vec<u8>,
    mac: Vec<u8>,
}

#[derive(Debug)]
pub(crate) enum RemoteScannerOutcome {
    Complete {
        usage: Box<DataUsageEntryInfo>,
        pending_maintenance_work: bool,
    },
    Partial,
    NamespaceNotFound,
    CycleAhead(u64),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RemoteScannerScanSpec<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) next_cycle: u64,
    pub(crate) leader_epoch: u64,
    pub(crate) server_epoch: Uuid,
    pub(crate) session_id: Uuid,
    pub(crate) session_sequence: u64,
    pub(crate) scan_plan_digest: DataUsageScanPlanDigest,
    pub(crate) skip_healing: bool,
    pub(crate) scan_mode: HealScanMode,
}

#[derive(Clone, Copy)]
struct RemoteScannerResponseExpectation<'a> {
    bucket: &'a str,
    source: DataUsageCacheSource,
    next_cycle: u64,
    scan_plan_digest: DataUsageScanPlanDigest,
}

#[derive(Debug)]
pub(crate) struct RemoteScannerFailure {
    error: StorageError,
    disposition: RemoteScannerFailureDisposition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteScannerFailureDisposition {
    Bucket,
    RetireWorker,
    RetryBucket,
}

impl RemoteScannerFailure {
    fn transport(error: StorageError) -> Self {
        Self {
            error,
            disposition: RemoteScannerFailureDisposition::RetireWorker,
        }
    }

    fn bucket(error: StorageError) -> Self {
        Self {
            error,
            disposition: RemoteScannerFailureDisposition::Bucket,
        }
    }

    fn retry_bucket(error: StorageError) -> Self {
        Self {
            error,
            disposition: RemoteScannerFailureDisposition::RetryBucket,
        }
    }

    pub(crate) fn retire_worker(&self) -> bool {
        self.disposition == RemoteScannerFailureDisposition::RetireWorker
    }

    pub(crate) fn retry_bucket_work(&self) -> bool {
        self.disposition == RemoteScannerFailureDisposition::RetryBucket
    }
}

#[derive(Debug)]
struct RemoteScannerServerError {
    scope: RemoteScannerErrorScope,
    error: ScannerError,
}

impl RemoteScannerServerError {
    fn worker(message: impl Into<String>) -> Self {
        Self {
            scope: RemoteScannerErrorScope::Worker,
            error: ScannerError::Other(message.into()),
        }
    }

    fn disk_scan(error: ScannerError, disk_online: bool) -> Self {
        Self {
            scope: if disk_online {
                RemoteScannerErrorScope::Bucket
            } else {
                RemoteScannerErrorScope::Worker
            },
            error,
        }
    }

    fn into_frame(self) -> RemoteScannerErrorFrame {
        RemoteScannerErrorFrame {
            scope: self.scope,
            message: limit_error_message(self.error.to_string()),
        }
    }
}

impl std::fmt::Display for RemoteScannerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

#[derive(Debug)]
struct RemoteScannerStreamError {
    error: StorageError,
    progress_fully_reported: bool,
    retire_worker: bool,
}

impl RemoteScannerStreamError {
    fn uncertain(error: StorageError) -> Self {
        Self {
            error,
            progress_fully_reported: false,
            retire_worker: true,
        }
    }

    fn reconciled(error: StorageError) -> Self {
        Self {
            error,
            progress_fully_reported: true,
            retire_worker: true,
        }
    }

    fn bucket(error: StorageError) -> Self {
        Self {
            error,
            progress_fully_reported: true,
            retire_worker: false,
        }
    }

    fn for_phase(error: StorageError, phase: RemoteScannerPhase) -> Self {
        if phase == RemoteScannerPhase::Persisting {
            Self::reconciled(error)
        } else {
            Self::uncertain(error)
        }
    }
}

impl std::fmt::Display for RemoteScannerStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

type RemoteScannerStreamResult<T> = std::result::Result<T, RemoteScannerStreamError>;

#[derive(Default)]
struct RemoteScannerReplayCache {
    cycle: Option<u64>,
    leader_epoch: Option<u64>,
    sessions: HashMap<(String, Uuid), u64>,
}

#[derive(Clone, Copy)]
struct RemoteScannerValidatedCycle {
    cycle: u64,
    leader_epoch: u64,
    valid_until: Instant,
}

impl RemoteScannerValidatedCycle {
    fn matches(self, requested_cycle: u64, requested_leader_epoch: u64, now: Instant) -> bool {
        self.cycle == requested_cycle && self.leader_epoch == requested_leader_epoch && now < self.valid_until
    }
}

impl RemoteScannerReplayCache {
    fn preflight_key(&self, key: &(String, Uuid), cycle: u64, leader_epoch: u64, sequence: u64) -> Result<(), ScannerError> {
        match self.cycle.zip(self.leader_epoch) {
            Some((current_cycle, current_epoch)) if (leader_epoch, cycle) < (current_epoch, current_cycle) => {
                return Err(ScannerError::RemoteRequestReplay);
            }
            Some((current_cycle, current_epoch)) if (leader_epoch, cycle) > (current_epoch, current_cycle) => {
                return (sequence == 0).then_some(()).ok_or(ScannerError::RemoteRequestReplay);
            }
            Some(_) => {}
            None => {
                if sequence != 0 {
                    return Err(ScannerError::RemoteRequestReplay);
                }
                return (self.sessions.len() < NS_SCANNER_MAX_REPLAY_SESSIONS)
                    .then_some(())
                    .ok_or(ScannerError::RemoteReplayCapacity);
            }
        }
        if let Some(last_sequence) = self.sessions.get(key) {
            let expected = last_sequence.checked_add(1).ok_or(ScannerError::RemoteRequestReplay)?;
            return (sequence == expected).then_some(()).ok_or(ScannerError::RemoteRequestReplay);
        }
        if sequence != 0 {
            return Err(ScannerError::RemoteRequestReplay);
        }
        (self.sessions.len() < NS_SCANNER_MAX_REPLAY_SESSIONS)
            .then_some(())
            .ok_or(ScannerError::RemoteReplayCapacity)
    }

    fn preflight(
        &self,
        disk_key: &str,
        session_id: Uuid,
        cycle: u64,
        leader_epoch: u64,
        sequence: u64,
    ) -> Result<(), ScannerError> {
        self.preflight_key(&(disk_key.to_string(), session_id), cycle, leader_epoch, sequence)
    }

    fn claim(
        &mut self,
        disk_key: String,
        session_id: Uuid,
        cycle: u64,
        leader_epoch: u64,
        sequence: u64,
    ) -> Result<(), ScannerError> {
        let key = (disk_key, session_id);
        self.preflight_key(&key, cycle, leader_epoch, sequence)?;
        match self.cycle.zip(self.leader_epoch) {
            Some((current_cycle, current_epoch)) if (leader_epoch, cycle) > (current_epoch, current_cycle) => {
                self.sessions.clear();
                self.cycle = Some(cycle);
                self.leader_epoch = Some(leader_epoch);
            }
            None => {
                self.cycle = Some(cycle);
                self.leader_epoch = Some(leader_epoch);
            }
            Some(_) => {}
        }
        if let Some(last_sequence) = self.sessions.get_mut(&key) {
            *last_sequence = sequence;
            return Ok(());
        }
        self.sessions.insert(key, sequence);
        Ok(())
    }
}

fn cache_source_for_disk(disk: &Disk) -> Result<DataUsageCacheSource, ScannerError> {
    let location = disk.get_disk_location();
    let pool_index = location
        .pool_idx
        .ok_or_else(|| ScannerError::Other("remote namespace scanner disk has no pool index".to_string()))?;
    let set_index = location
        .set_idx
        .ok_or_else(|| ScannerError::Other("remote namespace scanner disk has no set index".to_string()))?;
    Ok(DataUsageCacheSource::new(pool_index, set_index))
}

struct FrameAuthenticator {
    request_id: Uuid,
    secret: String,
}

impl FrameAuthenticator {
    fn from_rpc_secret(request_id: Uuid) -> Result<Self, ScannerError> {
        let secret = try_get_rpc_token()
            .map_err(|err| ScannerError::Other(format!("remote namespace scanner authentication unavailable: {err}")))?;
        Ok(Self { request_id, secret })
    }

    #[cfg(test)]
    fn for_test(request_id: Uuid) -> Self {
        Self {
            request_id,
            secret: "test-remote-scanner-secret".to_string(),
        }
    }

    fn sign(&self, sequence: u64, payload: &[u8]) -> Result<Vec<u8>, ScannerError> {
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
            .map_err(|_| ScannerError::Other("invalid remote namespace scanner authentication key".to_string()))?;
        update_frame_mac(&mut mac, self.request_id, sequence, payload);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    fn verify(&self, sequence: u64, payload: &[u8], signature: &[u8]) -> std::io::Result<()> {
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
            .map_err(|_| IoError::other("invalid remote namespace scanner authentication key"))?;
        update_frame_mac(&mut mac, self.request_id, sequence, payload);
        mac.verify_slice(signature)
            .map_err(|_| IoError::new(ErrorKind::PermissionDenied, "remote namespace scanner frame authentication failed"))
    }
}

fn update_frame_mac(mac: &mut HmacSha256, request_id: Uuid, sequence: u64, payload: &[u8]) {
    mac.update(NS_SCANNER_FRAME_AUTH_DOMAIN);
    mac.update(request_id.as_bytes());
    mac.update(&sequence.to_be_bytes());
    mac.update(payload);
}

pub fn decode_remote_scanner_request(body: &[u8]) -> Result<RemoteScannerRequest, ScannerError> {
    if body.is_empty() || body.len() > NS_SCANNER_MAX_REQUEST_BODY_SIZE {
        return Err(ScannerError::Other("remote namespace scanner request size is invalid".to_string()));
    }

    let request: RemoteScannerRequestWire =
        rmp_serde::from_slice(body).map_err(|_| ScannerError::Other("invalid remote namespace scanner request".to_string()))?;
    if request.version != NS_SCANNER_PROTOCOL_VERSION {
        return Err(ScannerError::Other(format!(
            "unsupported remote namespace scanner protocol version: {}",
            request.version
        )));
    }
    if request.request_id.is_nil() {
        return Err(ScannerError::Other("remote namespace scanner request ID is invalid".to_string()));
    }
    if request.server_epoch.is_nil() {
        return Err(ScannerError::Other("remote namespace scanner server epoch is invalid".to_string()));
    }
    if request.session_id.is_nil() {
        return Err(ScannerError::Other("remote namespace scanner session ID is invalid".to_string()));
    }
    if request.leader_epoch == 0 {
        return Err(ScannerError::Other("remote namespace scanner leader epoch is invalid".to_string()));
    }
    if request.bucket.contains(['/', '\\']) || is_reserved_or_invalid_bucket(&request.bucket, false) {
        return Err(ScannerError::Other("remote namespace scanner bucket is invalid".to_string()));
    }

    Ok(RemoteScannerRequest(request))
}

pub fn remote_scanner_request_matches_envelope(
    request: &RemoteScannerRequest,
    request_id: Uuid,
    server_epoch: Uuid,
    session_id: Uuid,
    session_sequence: u64,
    next_cycle: u64,
    leader_epoch: u64,
) -> bool {
    request.0.request_id == request_id
        && request.0.server_epoch == server_epoch
        && request.0.session_id == session_id
        && request.0.session_sequence == session_sequence
        && request.0.next_cycle == next_cycle
        && request.0.leader_epoch == leader_epoch
}

pub async fn validate_remote_scanner_request_fence(
    requested_cycle: u64,
    requested_leader_epoch: u64,
) -> Result<(), ScannerError> {
    let store = resolve_scanner_object_store_handle()
        .ok_or_else(|| ScannerError::Other("remote namespace scanner object layer is unavailable".to_string()))?;
    validate_remote_scanner_request_fence_cached(
        requested_cycle,
        requested_leader_epoch,
        store,
        &REMOTE_SCANNER_VALIDATED_CYCLE,
        &REMOTE_SCANNER_CYCLE_REFRESH,
    )
    .await
}

async fn validate_remote_scanner_request_fence_cached(
    requested_cycle: u64,
    requested_leader_epoch: u64,
    store: Arc<impl ScannerObjectIO>,
    cache: &Mutex<Option<RemoteScannerValidatedCycle>>,
    refresh: &AsyncMutex<()>,
) -> Result<(), ScannerError> {
    if cached_remote_scanner_fence_matches(cache, requested_cycle, requested_leader_epoch, Instant::now())? {
        return Ok(());
    }

    let _refresh = refresh.lock().await;
    if cached_remote_scanner_fence_matches(cache, requested_cycle, requested_leader_epoch, Instant::now())? {
        return Ok(());
    }

    let (persisted_cycle, persisted_leader_epoch) =
        validate_remote_scanner_request_fence_with_store(requested_cycle, requested_leader_epoch, store).await?;
    let valid_until = Instant::now()
        .checked_add(NS_SCANNER_VALIDATED_CYCLE_TTL)
        .ok_or_else(|| ScannerError::Other("remote namespace scanner cycle cache expiry overflow".to_string()))?;
    *cache
        .lock()
        .map_err(|_| ScannerError::Other("remote namespace scanner cycle cache is unavailable".to_string()))? =
        Some(RemoteScannerValidatedCycle {
            cycle: persisted_cycle,
            leader_epoch: persisted_leader_epoch,
            valid_until,
        });
    Ok(())
}

fn cached_remote_scanner_fence_matches(
    cache: &Mutex<Option<RemoteScannerValidatedCycle>>,
    requested_cycle: u64,
    requested_leader_epoch: u64,
    now: Instant,
) -> Result<bool, ScannerError> {
    Ok(cache
        .lock()
        .map_err(|_| ScannerError::Other("remote namespace scanner cycle cache is unavailable".to_string()))?
        .is_some_and(|cached| cached.matches(requested_cycle, requested_leader_epoch, now)))
}

async fn validate_remote_scanner_request_fence_with_store(
    requested_cycle: u64,
    requested_leader_epoch: u64,
    store: Arc<impl ScannerObjectIO>,
) -> Result<(u64, u64), ScannerError> {
    let (persisted_cycle, persisted_leader_epoch) = match read_config(store, &DATA_USAGE_BLOOM_NAME_PATH).await {
        Ok(buf) => crate::scanner::decode_persisted_scanner_cycle_fence(&buf)?,
        Err(EcstoreError::ConfigNotFound) => (0, 0),
        Err(err) => {
            return Err(ScannerError::Other(format!("failed to read persisted scanner cycle state: {err}")));
        }
    };
    if requested_cycle != persisted_cycle || requested_leader_epoch != persisted_leader_epoch {
        return Err(ScannerError::Other(format!(
            "remote namespace scanner fence does not match persisted state: requested_cycle={requested_cycle}, \
             persisted_cycle={persisted_cycle}, requested_epoch={requested_leader_epoch}, persisted_epoch={persisted_leader_epoch}"
        )));
    }
    Ok((persisted_cycle, persisted_leader_epoch))
}

async fn watch_remote_scanner_request_fence(
    requested_cycle: u64,
    requested_leader_epoch: u64,
    store: Arc<impl ScannerObjectIO>,
    poll_interval: Duration,
) -> Result<(), ScannerError> {
    if poll_interval.is_zero() {
        return Err(ScannerError::Other(
            "remote namespace scanner fence poll interval must be positive".to_string(),
        ));
    }
    let first_poll = Instant::now()
        .checked_add(poll_interval)
        .ok_or_else(|| ScannerError::Other("remote namespace scanner fence poll deadline overflow".to_string()))?;
    let mut poll = tokio::time::interval_at(first_poll, poll_interval);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        poll.tick().await;
        validate_remote_scanner_request_fence_with_store(requested_cycle, requested_leader_epoch, store.clone()).await?;
    }
}

pub fn admit_remote_scanner_request(disk: &Disk) -> Result<RemoteScannerAdmission, ScannerError> {
    if !disk.is_local() {
        return Err(ScannerError::Other(
            "remote namespace scanner admission requires a local disk".to_string(),
        ));
    }
    try_admit_remote_scanner_key(disk.path().to_string_lossy().into_owned())
}

pub fn preflight_remote_scanner_request(
    disk: &Disk,
    session_id: Uuid,
    next_cycle: u64,
    leader_epoch: u64,
    session_sequence: u64,
) -> Result<(), ScannerError> {
    if !disk.is_local() {
        return Err(ScannerError::Other(
            "remote namespace scanner replay preflight requires a local disk".to_string(),
        ));
    }
    REMOTE_SCANNER_REPLAY_CACHE
        .lock()
        .map_err(|_| ScannerError::Other("remote namespace scanner replay cache is unavailable".to_string()))?
        .preflight(
            disk.path().to_string_lossy().as_ref(),
            session_id,
            next_cycle,
            leader_epoch,
            session_sequence,
        )
}

pub fn claim_remote_scanner_request(
    disk: &Disk,
    session_id: Uuid,
    next_cycle: u64,
    leader_epoch: u64,
    session_sequence: u64,
) -> Result<(), ScannerError> {
    if !disk.is_local() {
        return Err(ScannerError::Other(
            "remote namespace scanner replay claim requires a local disk".to_string(),
        ));
    }
    REMOTE_SCANNER_REPLAY_CACHE
        .lock()
        .map_err(|_| ScannerError::Other("remote namespace scanner replay cache is unavailable".to_string()))?
        .claim(
            disk.path().to_string_lossy().into_owned(),
            session_id,
            next_cycle,
            leader_epoch,
            session_sequence,
        )
}

pub(crate) fn try_admit_remote_scanner(disk: &Disk) -> Result<RemoteScannerAdmission, ScannerError> {
    if !disk.is_local() {
        return Err(ScannerError::Other(
            "remote namespace scanner admission requires a local disk".to_string(),
        ));
    }
    try_admit_remote_scanner_key(disk.path().to_string_lossy().into_owned())
}

fn try_admit_remote_scanner_key(disk_key: String) -> Result<RemoteScannerAdmission, ScannerError> {
    let semaphore = REMOTE_SCANNER_ADMISSION
        .lock()
        .map_err(|_| ScannerError::Other("remote namespace scanner admission is unavailable".to_string()))?
        .entry(disk_key)
        .or_insert_with(|| Arc::new(Semaphore::new(1)))
        .clone();
    let permit = semaphore.try_acquire_owned().map_err(|_| ScannerError::RemoteDiskBusy)?;
    Ok(RemoteScannerAdmission { _permit: permit })
}

pub async fn serve_remote_scanner_request<W>(
    disk: Arc<Disk>,
    request: RemoteScannerRequest,
    mut writer: W,
    disconnect: CancellationToken,
) -> Result<(), ScannerError>
where
    W: AsyncWrite + Unpin + Send,
{
    if !disk.is_local() {
        return Err(ScannerError::Other(
            "remote namespace scanner request resolved to a non-local disk".to_string(),
        ));
    }
    let request = request.0;
    let request_id = request.request_id;
    let budget_config = request.budget;
    let authenticator = FrameAuthenticator::from_rpc_secret(request_id)?;
    let heartbeat_interval = if budget_config.max_objects.is_some() || budget_config.max_directories.is_some() {
        NS_SCANNER_BUDGET_HEARTBEAT_INTERVAL
    } else {
        NS_SCANNER_HEARTBEAT_INTERVAL
    };
    let parent = CancellationToken::new();
    let _cancel_on_drop = parent.clone().drop_guard();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, budget_config.into_config());
    let ctx = budget.token();
    let phase = Arc::new(AtomicU8::new(RemoteScannerPhase::Scanning as u8));
    let phase_changed = Arc::new(Notify::new());
    let scan = scan_and_persist_local_bucket(disk, ctx, budget.clone(), phase.clone(), phase_changed.clone(), request);
    tokio::pin!(scan);

    let mut heartbeat = tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let rpc_lifetime = tokio::time::sleep(NS_SCANNER_MAX_RPC_LIFETIME);
    tokio::pin!(rpc_lifetime);
    let mut sequence = 0_u64;
    let mut persistence_announced = false;

    loop {
        tokio::select! {
            biased;
            result = &mut scan => {
                let frame_result = match result {
                    Ok(result) => result,
                    Err(err) => RemoteScannerFrameResult::Error(err.into_frame()),
                };
                write_frame_bounded(
                    &mut writer,
                    &authenticator,
                    &mut sequence,
                    &RemoteScannerFrame::with_phase(
                        budget_progress(&budget),
                        remote_scanner_phase(&phase),
                        frame_result,
                    ),
                    &disconnect,
                    NS_SCANNER_WRITE_TIMEOUT,
                )
                .await?;
                shutdown_writer_bounded(&mut writer, &disconnect, NS_SCANNER_WRITE_TIMEOUT).await?;
                return Ok(());
            }
            _ = disconnect.cancelled() => {
                parent.cancel();
                await_remote_scan_shutdown(scan.as_mut()).await;
                return Ok(());
            }
            _ = &mut rpc_lifetime => {
                parent.cancel();
                await_remote_scan_shutdown(scan.as_mut()).await;
                return Err(ScannerError::Other("remote namespace scanner RPC lifetime exceeded".to_string()));
            }
            _ = phase_changed.notified(), if !persistence_announced => {
                persistence_announced = true;
                if let Err(err) = write_frame_bounded(
                    &mut writer,
                    &authenticator,
                    &mut sequence,
                    &RemoteScannerFrame::with_phase(
                        budget_progress(&budget),
                        RemoteScannerPhase::Persisting,
                        RemoteScannerFrameResult::Progress,
                    ),
                    &disconnect,
                    NS_SCANNER_WRITE_TIMEOUT,
                )
                .await
                {
                    parent.cancel();
                    await_remote_scan_shutdown(scan.as_mut()).await;
                    return Err(err);
                }
                if let Err(err) = flush_writer_bounded(&mut writer, &disconnect, NS_SCANNER_WRITE_TIMEOUT).await {
                    parent.cancel();
                    await_remote_scan_shutdown(scan.as_mut()).await;
                    return Err(err);
                }
            }
            _ = heartbeat.tick() => {
                if let Err(err) = write_frame_bounded(
                    &mut writer,
                    &authenticator,
                    &mut sequence,
                    &RemoteScannerFrame::with_phase(
                        budget_progress(&budget),
                        remote_scanner_phase(&phase),
                        RemoteScannerFrameResult::Progress,
                    ),
                    &disconnect,
                    NS_SCANNER_WRITE_TIMEOUT,
                )
                .await
                {
                    parent.cancel();
                    await_remote_scan_shutdown(scan.as_mut()).await;
                    return Err(err);
                }
                if let Err(err) = flush_writer_bounded(&mut writer, &disconnect, NS_SCANNER_WRITE_TIMEOUT).await {
                    parent.cancel();
                    await_remote_scan_shutdown(scan.as_mut()).await;
                    return Err(err);
                }
            }
        }
    }
}

async fn scan_and_persist_local_bucket(
    disk: Arc<Disk>,
    ctx: CancellationToken,
    budget: Arc<ScannerCycleBudget>,
    phase: Arc<AtomicU8>,
    phase_changed: Arc<Notify>,
    request: RemoteScannerRequestWire,
) -> std::result::Result<RemoteScannerFrameResult, RemoteScannerServerError> {
    let RemoteScannerRequestWire {
        bucket,
        next_cycle,
        leader_epoch,
        scan_plan_digest,
        skip_healing,
        scan_mode,
        ..
    } = request;
    let store = resolve_scanner_object_store_handle()
        .ok_or_else(|| RemoteScannerServerError::worker("remote namespace scanner object layer is unavailable"))?;
    validate_remote_scanner_request_fence_with_store(next_cycle, leader_epoch, store.clone())
        .await
        .map_err(|err| RemoteScannerServerError::worker(format!("remote namespace scanner leader fence is stale: {err}")))?;
    let source = cache_source_for_disk(disk.as_ref())
        .map_err(|err| RemoteScannerServerError::worker(format!("remote namespace scanner source is unavailable: {err}")))?;
    let set = store
        .pools
        .get(source.pool_index)
        .and_then(|pool| pool.disk_set.get(source.set_index))
        .cloned()
        .ok_or_else(|| {
            RemoteScannerServerError::worker(format!(
                "remote namespace scanner set is unavailable: pool={}, set={}",
                source.pool_index, source.set_index
            ))
        })?;
    let cache_name = path_join_buf(&[&bucket, DATA_USAGE_CACHE_NAME]);
    let lock_resource = scanner_cache_lock_resource(&cache_name);
    let ns_lock = set
        .new_ns_lock(RUSTFS_META_BUCKET, &lock_resource)
        .await
        .map_err(|err| RemoteScannerServerError::worker(format!("remote namespace scanner cache lock creation failed: {err}")))?;
    let guard = ns_lock
        .get_write_lock_quiet(scanner_cache_lock_timeout())
        .await
        .map_err(|err| {
            RemoteScannerServerError::worker(format!("remote namespace scanner cache lock acquisition failed: {err}"))
        })?;
    let mut cache = DataUsageCache::default();
    let revisions = cache.load_with_revisions(set.clone(), &cache_name).await.map_err(|err| {
        RemoteScannerServerError::worker(format!("remote namespace scanner cache load or revision lookup failed: {err}"))
    })?;
    if cache_snapshot_is_current(&cache, &bucket, source, next_cycle, leader_epoch, scan_plan_digest) {
        if guard.is_lock_lost() {
            return Err(RemoteScannerServerError::worker(
                "remote namespace scanner cache lock was lost before reusing the current snapshot",
            ));
        }
        return Ok(RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
            source,
            scan_plan_digest,
            usage: cache_root_entry_info(&cache)
                .map_err(|err| RemoteScannerServerError::worker(format!("remote namespace scanner cache is corrupt: {err}")))?,
            pending_maintenance_work: !cache.info.pending_heals.is_empty(),
        })));
    }
    match cache.prepare_for_scan(&bucket, next_cycle, leader_epoch, source, scan_plan_digest, true) {
        DataUsageCachePrepareOutcome::RejectedNewerCycle => {
            return Ok(RemoteScannerFrameResult::CycleAhead {
                required_cycle: cache.info.next_cycle,
            });
        }
        DataUsageCachePrepareOutcome::RejectedNewerLeader => {
            return Err(RemoteScannerServerError::worker(
                "remote namespace scanner rejected work from an older leader epoch",
            ));
        }
        DataUsageCachePrepareOutcome::Reused | DataUsageCachePrepareOutcome::Reset => {}
    }
    cache.info.skip_healing = skip_healing;

    let set_disks = scanner_set_disk_inventory(set.as_ref()).await;
    let scan_ctx = ctx.child_token();
    let scan = ScannerIODisk::nsscanner_disk(disk.clone(), scan_ctx.clone(), budget, set_disks, cache, None, scan_mode);
    tokio::pin!(scan);
    let fence_watch = watch_remote_scanner_request_fence(next_cycle, leader_epoch, store.clone(), NS_SCANNER_FENCE_POLL_INTERVAL);
    tokio::pin!(fence_watch);
    let mut lock_watch = tokio::time::interval(NS_SCANNER_LOCK_POLL_INTERVAL);
    lock_watch.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let outcome = loop {
        tokio::select! {
            result = &mut scan => {
                match result {
                    Ok(outcome) => break outcome,
                    Err(err) => {
                        let disk_online =
                            tokio::time::timeout(NS_SCANNER_DISK_HEALTH_TIMEOUT, crate::scanner_disk_is_online(disk.as_ref()))
                                .await
                                .unwrap_or(false);
                        return Err(RemoteScannerServerError::disk_scan(
                            ScannerError::Other(format!("remote namespace scanner disk scan failed: {err}")),
                            disk_online,
                        ));
                    }
                }
            }
            _ = lock_watch.tick() => {
                if guard.is_lock_lost() {
                    scan_ctx.cancel();
                    let _ = tokio::time::timeout(NS_SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, scan.as_mut()).await;
                    return Err(RemoteScannerServerError::worker(
                        "remote namespace scanner cache lock was lost during bucket scan",
                    ));
                }
            }
            result = &mut fence_watch => {
                scan_ctx.cancel();
                let _ = tokio::time::timeout(NS_SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, scan.as_mut()).await;
                let err = match result {
                    Ok(()) => ScannerError::Other("remote namespace scanner fence watcher stopped unexpectedly".to_string()),
                    Err(err) => err,
                };
                return Err(RemoteScannerServerError::worker(format!(
                    "remote namespace scanner leader fence changed during bucket scan: {}",
                    err
                )));
            }
        }
    };
    let (cache, partial_result) = match outcome {
        ScannerDiskScanOutcome::Complete(cache) => (cache, None),
        ScannerDiskScanOutcome::Partial(cache) => (cache, Some(RemoteScannerFrameResult::Partial)),
        ScannerDiskScanOutcome::NamespaceNotFound(cache) => (cache, Some(RemoteScannerFrameResult::NamespaceNotFound)),
    };

    if guard.is_lock_lost() {
        return Err(RemoteScannerServerError::worker(
            "remote namespace scanner cache lock was lost before persistence",
        ));
    }
    phase.store(RemoteScannerPhase::Persisting as u8, Ordering::Release);
    phase_changed.notify_one();
    validate_remote_scanner_request_fence_with_store(next_cycle, leader_epoch, store.clone())
        .await
        .map_err(|err| RemoteScannerServerError::worker(format!("remote namespace scanner leader fence changed: {err}")))?;
    let done_save = Metrics::time(Metric::SaveUsage);
    let save_result = cache.save_with_revisions(set, &cache_name, &revisions).await;
    done_save();
    save_result.map_err(|err| RemoteScannerServerError::worker(format!("remote namespace scanner cache save failed: {err}")))?;
    validate_remote_scanner_request_fence_with_store(next_cycle, leader_epoch, store)
        .await
        .map_err(|err| RemoteScannerServerError::worker(format!("remote namespace scanner leader fence changed: {err}")))?;
    if guard.is_lock_lost() {
        return Err(RemoteScannerServerError::worker(
            "remote namespace scanner cache lock was lost during persistence",
        ));
    }

    if let Some(partial_result) = partial_result {
        return Ok(partial_result);
    }
    if cache.info.source != Some(source) || !cache.info.snapshot_complete {
        return Err(RemoteScannerServerError::worker(
            "remote namespace scanner completed without a complete source snapshot",
        ));
    }
    if cache.info.scan_plan_digest != Some(scan_plan_digest) {
        return Err(RemoteScannerServerError::worker(
            "remote namespace scanner completed with a different bucket plan",
        ));
    }
    Ok(RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
        source,
        scan_plan_digest,
        usage: cache_root_entry_info(&cache)
            .map_err(|err| RemoteScannerServerError::worker(format!("remote namespace scanner cache is corrupt: {err}")))?,
        pending_maintenance_work: !cache.info.pending_heals.is_empty(),
    })))
}

pub(crate) async fn scan_remote_bucket(
    disk: &Disk,
    ctx: CancellationToken,
    budget: Arc<ScannerCycleBudget>,
    spec: RemoteScannerScanSpec<'_>,
) -> std::result::Result<RemoteScannerOutcome, RemoteScannerFailure> {
    let RemoteScannerScanSpec {
        bucket,
        next_cycle,
        leader_epoch,
        server_epoch,
        session_id,
        session_sequence,
        scan_plan_digest,
        skip_healing,
        scan_mode,
    } = spec;
    let expected_source = cache_source_for_disk(disk).map_err(|err| {
        RemoteScannerFailure::transport(StorageError::other(format!("failed to resolve remote namespace scanner source: {err}")))
    })?;
    let request_id = Uuid::new_v4();
    let request = RemoteScannerRequestWire {
        version: NS_SCANNER_PROTOCOL_VERSION,
        request_id,
        server_epoch,
        session_id,
        session_sequence,
        bucket: bucket.to_string(),
        next_cycle,
        leader_epoch,
        scan_plan_digest,
        skip_healing,
        scan_mode,
        budget: RemoteScannerBudget::from_config(budget.remaining_config()),
    };
    let body = rmp_serde::to_vec_named(&request).map_err(|err| {
        RemoteScannerFailure::transport(StorageError::other(format!("failed to encode remote namespace scanner request: {err}")))
    })?;
    if body.is_empty() || body.len() > NS_SCANNER_MAX_REQUEST_BODY_SIZE {
        return Err(RemoteScannerFailure::transport(StorageError::other(
            "remote namespace scanner request size is invalid",
        )));
    }

    let rpc_deadline = Instant::now() + NS_SCANNER_MAX_RPC_LIFETIME;
    let open_deadline = (Instant::now() + NS_SCANNER_STALL_TIMEOUT).min(rpc_deadline);
    let open_stream = disk.open_ns_scanner_stream(crate::NsScannerOpenRequest {
        request_id,
        server_epoch,
        session_id,
        session_sequence,
        next_cycle,
        leader_epoch,
        body,
        stall_timeout: Some(NS_SCANNER_STALL_TIMEOUT),
    });
    tokio::pin!(open_stream);
    let reader = tokio::select! {
        _ = ctx.cancelled() => {
            budget.cancel_after_unreported_remote_progress();
            return Err(RemoteScannerFailure::transport(StorageError::other(
                "remote namespace scanner cancelled while opening stream",
            )));
        }
        result = tokio::time::timeout_at(open_deadline, &mut open_stream) => {
            match result {
                Err(_) => {
                    budget.cancel_after_unreported_remote_progress();
                    return Err(RemoteScannerFailure::transport(StorageError::other(
                        "remote namespace scanner response headers timed out",
                    )));
                }
                Ok(Err(err)) if err.is_internode_http_status(429) => {
                    return Err(RemoteScannerFailure::retry_bucket(StorageError::other(format!(
                        "remote namespace scanner worker rejected zero-progress work: {err}"
                    ))));
                }
                Ok(Err(err)) => {
                    budget.cancel_after_unreported_remote_progress();
                    return Err(RemoteScannerFailure::transport(StorageError::other(format!(
                        "failed to open remote namespace scanner stream: {err}"
                    ))));
                }
                Ok(Ok(reader)) => reader,
            }
        }
    };
    let authenticator = FrameAuthenticator::from_rpc_secret(request_id).map_err(|err| {
        RemoteScannerFailure::transport(StorageError::other(format!(
            "failed to authenticate remote namespace scanner stream: {err}"
        )))
    })?;

    let stream_result = consume_remote_scanner_stream_until(
        reader,
        ctx,
        budget.clone(),
        RemoteScannerResponseExpectation {
            bucket,
            source: expected_source,
            next_cycle,
            scan_plan_digest,
        },
        authenticator,
        rpc_deadline,
    )
    .await;
    finish_remote_scanner_stream(stream_result, budget.as_ref())
}

fn finish_remote_scanner_stream(
    result: RemoteScannerStreamResult<RemoteScannerOutcome>,
    budget: &ScannerCycleBudget,
) -> std::result::Result<RemoteScannerOutcome, RemoteScannerFailure> {
    match result {
        Ok(outcome) => Ok(outcome),
        Err(error) => {
            if !error.progress_fully_reported {
                budget.cancel_after_unreported_remote_progress();
            }
            let failure = if error.retire_worker {
                RemoteScannerFailure::transport(error.error)
            } else {
                RemoteScannerFailure::bucket(error.error)
            };
            Err(failure)
        }
    }
}

#[cfg(test)]
const TEST_NEXT_CYCLE: u64 = 11;

#[cfg(test)]
async fn consume_remote_scanner_stream<R>(
    reader: R,
    ctx: CancellationToken,
    budget: Arc<ScannerCycleBudget>,
    expected_bucket: &str,
    expected_source: DataUsageCacheSource,
    expected_scan_plan_digest: DataUsageScanPlanDigest,
    authenticator: FrameAuthenticator,
) -> RemoteScannerStreamResult<RemoteScannerOutcome>
where
    R: AsyncRead + Unpin,
{
    consume_remote_scanner_stream_until(
        reader,
        ctx,
        budget,
        RemoteScannerResponseExpectation {
            bucket: expected_bucket,
            source: expected_source,
            next_cycle: TEST_NEXT_CYCLE,
            scan_plan_digest: expected_scan_plan_digest,
        },
        authenticator,
        Instant::now() + NS_SCANNER_MAX_RPC_LIFETIME,
    )
    .await
}

async fn consume_remote_scanner_stream_until<R>(
    mut reader: R,
    ctx: CancellationToken,
    budget: Arc<ScannerCycleBudget>,
    expected: RemoteScannerResponseExpectation<'_>,
    authenticator: FrameAuthenticator,
    rpc_deadline: Instant,
) -> RemoteScannerStreamResult<RemoteScannerOutcome>
where
    R: AsyncRead + Unpin,
{
    let mut expected_sequence = 0_u64;
    let mut last_progress = RemoteScannerProgress::default();
    let mut last_phase = RemoteScannerPhase::Scanning;
    let mut semantic_progress_deadline =
        bounded_remote_scanner_deadline(Instant::now(), NS_SCANNER_SEMANTIC_STALL_TIMEOUT, rpc_deadline);

    loop {
        if ctx.is_cancelled() && budget.reason().is_none() {
            return Err(RemoteScannerStreamError::for_phase(
                StorageError::other("remote namespace scanner cancelled"),
                last_phase,
            ));
        }
        let lifetime_limited = rpc_deadline <= semantic_progress_deadline;
        let read_deadline = rpc_deadline.min(semantic_progress_deadline);
        let frame = tokio::select! {
            biased;
            _ = ctx.cancelled(), if budget.reason().is_none() => {
                return Err(RemoteScannerStreamError::for_phase(
                    StorageError::other("remote namespace scanner cancelled"),
                    last_phase,
                ));
            }
            result = tokio::time::timeout_at(
                read_deadline,
                read_frame(&mut reader, &authenticator, &mut expected_sequence),
            ) => match result {
                Ok(Ok(frame)) => frame,
                Ok(Err(err)) => {
                    return Err(RemoteScannerStreamError::for_phase(StorageError::other(err), last_phase));
                }
                Err(_) => {
                    let message = if lifetime_limited {
                        "remote namespace scanner RPC lifetime exceeded"
                    } else {
                        "remote namespace scanner made no semantic progress"
                    };
                    return Err(RemoteScannerStreamError::for_phase(StorageError::other(message), last_phase));
                }
            }
        };

        let advanced = apply_remote_progress(&budget, &mut last_progress, frame.progress)
            .map_err(|err| RemoteScannerStreamError::for_phase(err, last_phase))?;
        if last_phase == RemoteScannerPhase::Persisting && frame.phase == RemoteScannerPhase::Scanning {
            return Err(RemoteScannerStreamError::uncertain(StorageError::other(
                "remote namespace scanner phase moved backwards",
            )));
        }
        let phase_advanced = frame.phase != last_phase;
        last_phase = frame.phase;
        if phase_advanced && frame.phase == RemoteScannerPhase::Persisting {
            semantic_progress_deadline =
                bounded_remote_scanner_deadline(Instant::now(), DataUsageCache::persistence_timeout(), rpc_deadline);
        } else if advanced {
            semantic_progress_deadline =
                bounded_remote_scanner_deadline(Instant::now(), NS_SCANNER_SEMANTIC_STALL_TIMEOUT, rpc_deadline);
        }

        match frame.result {
            RemoteScannerFrameResult::Progress => {
                if budget.budget_elapsed() && frame.phase == RemoteScannerPhase::Scanning {
                    return Ok(RemoteScannerOutcome::Partial);
                }
            }
            RemoteScannerFrameResult::Complete(complete) => {
                if complete.usage.name != expected.bucket || complete.usage.parent != crate::DATA_USAGE_ROOT {
                    return Err(RemoteScannerStreamError::reconciled(StorageError::other(
                        "remote namespace scanner returned usage for the wrong bucket",
                    )));
                }
                if complete.source != expected.source {
                    return Err(RemoteScannerStreamError::reconciled(StorageError::other(
                        "remote namespace scanner returned usage for the wrong pool or set",
                    )));
                }
                if complete.scan_plan_digest != expected.scan_plan_digest {
                    return Err(RemoteScannerStreamError::reconciled(StorageError::other(
                        "remote namespace scanner returned usage for a different bucket plan",
                    )));
                }
                if !complete.usage.entry.children.is_empty() {
                    return Err(RemoteScannerStreamError::reconciled(StorageError::other(
                        "remote namespace scanner returned non-flattened bucket usage",
                    )));
                }
                if budget.budget_elapsed() {
                    return Ok(RemoteScannerOutcome::Partial);
                }
                return Ok(RemoteScannerOutcome::Complete {
                    usage: Box::new(complete.usage),
                    pending_maintenance_work: complete.pending_maintenance_work,
                });
            }
            RemoteScannerFrameResult::Partial => return Ok(RemoteScannerOutcome::Partial),
            RemoteScannerFrameResult::NamespaceNotFound => return Ok(RemoteScannerOutcome::NamespaceNotFound),
            RemoteScannerFrameResult::CycleAhead { required_cycle } => {
                if required_cycle <= expected.next_cycle || required_cycle == u64::MAX {
                    return Err(RemoteScannerStreamError::reconciled(StorageError::other(
                        "remote namespace scanner returned an invalid required cycle",
                    )));
                }
                return Ok(RemoteScannerOutcome::CycleAhead(required_cycle));
            }
            RemoteScannerFrameResult::Error(error_frame) => {
                let error =
                    StorageError::other(format!("remote namespace scanner failed: {}", limit_error_message(error_frame.message)));
                return Err(match error_frame.scope {
                    RemoteScannerErrorScope::Bucket => RemoteScannerStreamError::bucket(error),
                    RemoteScannerErrorScope::Worker => RemoteScannerStreamError::reconciled(error),
                });
            }
        }
    }
}

fn bounded_remote_scanner_deadline(now: Instant, timeout_duration: Duration, rpc_deadline: Instant) -> Instant {
    now.checked_add(timeout_duration).unwrap_or(rpc_deadline).min(rpc_deadline)
}

fn apply_remote_progress(
    budget: &ScannerCycleBudget,
    last: &mut RemoteScannerProgress,
    current: RemoteScannerProgress,
) -> std::result::Result<bool, StorageError> {
    if current.objects_scanned < last.objects_scanned
        || current.directories_started < last.directories_started
        || current.entries_visited < last.entries_visited
    {
        return Err(StorageError::other("remote namespace scanner progress moved backwards"));
    }

    let advanced = current != *last;
    budget.record_remote_progress(
        current.objects_scanned - last.objects_scanned,
        current.directories_started - last.directories_started,
    );
    *last = current;
    Ok(advanced)
}

fn budget_progress(budget: &ScannerCycleBudget) -> RemoteScannerProgress {
    let (objects_scanned, directories_started) = budget.progress();
    RemoteScannerProgress {
        objects_scanned,
        directories_started,
        entries_visited: budget.entries_visited(),
    }
}

fn remote_scanner_phase(phase: &AtomicU8) -> RemoteScannerPhase {
    if phase.load(Ordering::Acquire) == RemoteScannerPhase::Persisting as u8 {
        RemoteScannerPhase::Persisting
    } else {
        RemoteScannerPhase::Scanning
    }
}

async fn await_remote_scan_shutdown<F>(scan: Pin<&mut F>)
where
    F: Future,
{
    let grace = DataUsageCache::persistence_timeout().min(NS_SCANNER_DISCONNECT_GRACE_MAX);
    let _ = tokio::time::timeout(grace, scan).await;
}

fn disconnected_writer_error() -> ScannerError {
    IoError::new(ErrorKind::ConnectionAborted, "remote namespace scanner response disconnected").into()
}

fn writer_timeout_error(operation: &str, timeout_duration: Duration) -> ScannerError {
    ScannerError::Other(format!(
        "remote namespace scanner response {operation} timed out after {timeout_duration:?}"
    ))
}

async fn write_frame_bounded<W>(
    writer: &mut W,
    authenticator: &FrameAuthenticator,
    sequence: &mut u64,
    frame: &RemoteScannerFrame,
    disconnect: &CancellationToken,
    timeout_duration: Duration,
) -> Result<(), ScannerError>
where
    W: AsyncWrite + Unpin,
{
    tokio::select! {
        biased;
        _ = disconnect.cancelled() => Err(disconnected_writer_error()),
        result = tokio::time::timeout(timeout_duration, write_frame(writer, authenticator, sequence, frame)) => {
            result.map_err(|_| writer_timeout_error("write", timeout_duration))?
        }
    }
}

async fn flush_writer_bounded<W>(
    writer: &mut W,
    disconnect: &CancellationToken,
    timeout_duration: Duration,
) -> Result<(), ScannerError>
where
    W: AsyncWrite + Unpin,
{
    tokio::select! {
        biased;
        _ = disconnect.cancelled() => Err(disconnected_writer_error()),
        result = tokio::time::timeout(timeout_duration, writer.flush()) => {
            result
                .map_err(|_| writer_timeout_error("flush", timeout_duration))?
                .map_err(ScannerError::from)
        }
    }
}

async fn shutdown_writer_bounded<W>(
    writer: &mut W,
    disconnect: &CancellationToken,
    timeout_duration: Duration,
) -> Result<(), ScannerError>
where
    W: AsyncWrite + Unpin,
{
    tokio::select! {
        biased;
        _ = disconnect.cancelled() => Err(disconnected_writer_error()),
        result = tokio::time::timeout(timeout_duration, writer.shutdown()) => {
            result
                .map_err(|_| writer_timeout_error("shutdown", timeout_duration))?
                .map_err(ScannerError::from)
        }
    }
}

async fn write_frame<W>(
    writer: &mut W,
    authenticator: &FrameAuthenticator,
    sequence: &mut u64,
    frame: &RemoteScannerFrame,
) -> Result<(), ScannerError>
where
    W: AsyncWrite + Unpin,
{
    let payload = rmp_serde::to_vec_named(frame)
        .map_err(|err| ScannerError::Other(format!("failed to encode remote namespace scanner frame: {err}")))?;
    let envelope = RemoteScannerFrameEnvelope {
        version: NS_SCANNER_PROTOCOL_VERSION,
        sequence: *sequence,
        mac: authenticator.sign(*sequence, &payload)?,
        payload,
    };
    let encoded = rmp_serde::to_vec_named(&envelope)
        .map_err(|err| ScannerError::Other(format!("failed to encode remote namespace scanner envelope: {err}")))?;
    if encoded.is_empty() || encoded.len() > NS_SCANNER_MAX_FRAME_SIZE {
        return Err(ScannerError::Other(format!(
            "remote namespace scanner frame size {} is invalid",
            encoded.len()
        )));
    }
    let len = u32::try_from(encoded.len())
        .map_err(|_| ScannerError::Other("remote namespace scanner frame is too large".to_string()))?;

    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&encoded).await?;
    *sequence = sequence
        .checked_add(1)
        .ok_or_else(|| ScannerError::Other("remote namespace scanner frame sequence overflow".to_string()))?;
    Ok(())
}

async fn read_frame<R>(
    reader: &mut R,
    authenticator: &FrameAuthenticator,
    expected_sequence: &mut u64,
) -> std::io::Result<RemoteScannerFrame>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0_u8; 4];
    reader.read_exact(&mut len).await.map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            IoError::new(ErrorKind::UnexpectedEof, "remote namespace scanner stream ended before a terminal frame")
        } else {
            err
        }
    })?;
    let len = usize::try_from(u32::from_be_bytes(len))
        .map_err(|_| IoError::new(ErrorKind::InvalidData, "remote namespace scanner frame length is invalid"))?;
    if len == 0 || len > NS_SCANNER_MAX_FRAME_SIZE {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("remote namespace scanner frame size {len} is invalid"),
        ));
    }

    let mut encoded = vec![0_u8; len];
    reader.read_exact(&mut encoded).await?;
    let envelope: RemoteScannerFrameEnvelope = rmp_serde::from_slice(&encoded)
        .map_err(|_| IoError::new(ErrorKind::InvalidData, "invalid remote namespace scanner frame envelope"))?;
    if envelope.version != NS_SCANNER_PROTOCOL_VERSION {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("unsupported remote namespace scanner frame version: {}", envelope.version),
        ));
    }
    if envelope.sequence != *expected_sequence {
        return Err(IoError::new(ErrorKind::InvalidData, "remote namespace scanner frame sequence is invalid"));
    }
    authenticator.verify(envelope.sequence, &envelope.payload, &envelope.mac)?;
    let frame: RemoteScannerFrame = rmp_serde::from_slice(&envelope.payload)
        .map_err(|_| IoError::new(ErrorKind::InvalidData, "invalid remote namespace scanner frame payload"))?;
    *expected_sequence = expected_sequence
        .checked_add(1)
        .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "remote namespace scanner frame sequence overflow"))?;
    Ok(frame)
}

fn limit_error_message(message: String) -> String {
    message.chars().take(NS_SCANNER_MAX_ERROR_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::RwLock;
    use std::sync::atomic::AtomicUsize;

    const TEST_SOURCE: DataUsageCacheSource = DataUsageCacheSource::new(1, 2);
    const TEST_PLAN_DIGEST: DataUsageScanPlanDigest = DataUsageScanPlanDigest([5; 32]);

    #[test]
    fn semantic_deadline_is_capped_by_rpc_lifetime_without_overflow() {
        let now = Instant::now();
        let rpc_deadline = now + Duration::from_secs(10);

        assert_eq!(bounded_remote_scanner_deadline(now, Duration::MAX, rpc_deadline), rpc_deadline);
        assert_eq!(
            bounded_remote_scanner_deadline(now, Duration::from_secs(2), rpc_deadline),
            now + Duration::from_secs(2)
        );
    }

    #[derive(Debug)]
    struct CycleStateStore {
        state: Option<Vec<u8>>,
    }

    #[derive(Debug)]
    struct CountingCycleStateStore {
        state: Vec<u8>,
        reads: AtomicUsize,
    }

    #[derive(Debug)]
    struct MutableCycleStateStore {
        state: RwLock<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl crate::storage_api::owner::ObjectIO for CycleStateStore {
        type Error = EcstoreError;
        type RangeSpec = crate::storage_api::owner::HTTPRangeSpec;
        type HeaderMap = http::HeaderMap;
        type ObjectOptions = crate::ScannerObjectOptions;
        type ObjectInfo = crate::ScannerObjectInfo;
        type GetObjectReader = crate::ScannerGetObjectReader;
        type PutObjectReader = crate::ScannerPutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::GetObjectReader, Self::Error> {
            if bucket != RUSTFS_META_BUCKET || object != DATA_USAGE_BLOOM_NAME_PATH.as_str() {
                return Err(EcstoreError::FileNotFound);
            }
            let state = self.state.clone().ok_or(EcstoreError::FileNotFound)?;
            Ok(crate::ScannerGetObjectReader {
                stream: Box::new(Cursor::new(state)),
                object_info: crate::ScannerObjectInfo {
                    etag: Some("cycle-state".to_string()),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            _bucket: &str,
            _object: &str,
            _data: &mut Self::PutObjectReader,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::other("cycle state test store is read-only"))
        }
    }

    #[async_trait::async_trait]
    impl crate::storage_api::owner::ObjectIO for CountingCycleStateStore {
        type Error = EcstoreError;
        type RangeSpec = crate::storage_api::owner::HTTPRangeSpec;
        type HeaderMap = http::HeaderMap;
        type ObjectOptions = crate::ScannerObjectOptions;
        type ObjectInfo = crate::ScannerObjectInfo;
        type GetObjectReader = crate::ScannerGetObjectReader;
        type PutObjectReader = crate::ScannerPutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::GetObjectReader, Self::Error> {
            if bucket != RUSTFS_META_BUCKET || object != DATA_USAGE_BLOOM_NAME_PATH.as_str() {
                return Err(EcstoreError::FileNotFound);
            }
            self.reads.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok(crate::ScannerGetObjectReader {
                stream: Box::new(Cursor::new(self.state.clone())),
                object_info: crate::ScannerObjectInfo {
                    etag: Some("cycle-state".to_string()),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            _bucket: &str,
            _object: &str,
            _data: &mut Self::PutObjectReader,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::other("cycle state test store is read-only"))
        }
    }

    #[async_trait::async_trait]
    impl crate::storage_api::owner::ObjectIO for MutableCycleStateStore {
        type Error = EcstoreError;
        type RangeSpec = crate::storage_api::owner::HTTPRangeSpec;
        type HeaderMap = http::HeaderMap;
        type ObjectOptions = crate::ScannerObjectOptions;
        type ObjectInfo = crate::ScannerObjectInfo;
        type GetObjectReader = crate::ScannerGetObjectReader;
        type PutObjectReader = crate::ScannerPutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::GetObjectReader, Self::Error> {
            if bucket != RUSTFS_META_BUCKET || object != DATA_USAGE_BLOOM_NAME_PATH.as_str() {
                return Err(EcstoreError::FileNotFound);
            }
            let state = self
                .state
                .read()
                .map_err(|_| EcstoreError::other("cycle state test lock is poisoned"))?
                .clone();
            Ok(crate::ScannerGetObjectReader {
                stream: Box::new(Cursor::new(state)),
                object_info: crate::ScannerObjectInfo {
                    etag: Some("cycle-state".to_string()),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            _bucket: &str,
            _object: &str,
            _data: &mut Self::PutObjectReader,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::other("cycle state test store is read-only"))
        }
    }

    fn test_usage(bucket: &str, objects: usize) -> DataUsageEntryInfo {
        let entry = crate::DataUsageEntry {
            objects,
            ..Default::default()
        };
        DataUsageEntryInfo {
            name: bucket.to_string(),
            parent: crate::DATA_USAGE_ROOT.to_string(),
            entry,
        }
    }

    fn test_request(request_id: Uuid) -> RemoteScannerRequestWire {
        RemoteScannerRequestWire {
            version: NS_SCANNER_PROTOCOL_VERSION,
            request_id,
            server_epoch: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            session_sequence: 0,
            bucket: "bucket".to_string(),
            next_cycle: 7,
            leader_epoch: 9,
            scan_plan_digest: TEST_PLAN_DIGEST,
            skip_healing: true,
            scan_mode: HealScanMode::Deep,
            budget: RemoteScannerBudget {
                max_duration_ms: Some(1234),
                max_objects: Some(10),
                max_directories: Some(20),
            },
        }
    }

    #[test]
    fn request_round_trip_preserves_scan_inputs_without_cache_data() {
        let request_id = Uuid::new_v4();
        let body = rmp_serde::to_vec_named(&test_request(request_id)).expect("request should encode");
        let decoded = decode_remote_scanner_request(&body).expect("request should decode");

        assert_eq!(decoded.0.request_id, request_id);
        assert!(!decoded.0.server_epoch.is_nil());
        assert!(!decoded.0.session_id.is_nil());
        assert_eq!(decoded.0.session_sequence, 0);
        assert_eq!(decoded.0.bucket, "bucket");
        assert_eq!(decoded.0.next_cycle, 7);
        assert_eq!(decoded.0.leader_epoch, 9);
        assert_eq!(decoded.0.scan_plan_digest, TEST_PLAN_DIGEST);
        assert!(decoded.0.skip_healing);
        assert_eq!(decoded.0.scan_mode, HealScanMode::Deep);
        assert_eq!(decoded.0.budget.max_objects, Some(10));
        assert_eq!(decoded.0.budget.max_directories, Some(20));
        assert!(body.len() < 512);
    }

    #[test]
    fn request_envelope_must_match_every_pre_body_replay_field() {
        let request = RemoteScannerRequest(test_request(Uuid::new_v4()));
        assert!(remote_scanner_request_matches_envelope(
            &request,
            request.0.request_id,
            request.0.server_epoch,
            request.0.session_id,
            request.0.session_sequence,
            request.0.next_cycle,
            request.0.leader_epoch,
        ));
        assert!(!remote_scanner_request_matches_envelope(
            &request,
            request.0.request_id,
            Uuid::new_v4(),
            request.0.session_id,
            request.0.session_sequence,
            request.0.next_cycle,
            request.0.leader_epoch,
        ));
        assert!(!remote_scanner_request_matches_envelope(
            &request,
            request.0.request_id,
            request.0.server_epoch,
            request.0.session_id,
            request.0.session_sequence + 1,
            request.0.next_cycle,
            request.0.leader_epoch,
        ));
        assert!(!remote_scanner_request_matches_envelope(
            &request,
            request.0.request_id,
            request.0.server_epoch,
            request.0.session_id,
            request.0.session_sequence,
            request.0.next_cycle,
            request.0.leader_epoch + 1,
        ));
    }

    #[tokio::test]
    async fn bounded_frame_write_releases_a_stalled_response() {
        let request_id = Uuid::new_v4();
        let authenticator = FrameAuthenticator::for_test(request_id);
        let (_reader, mut writer) = tokio::io::duplex(1);
        let disconnect = CancellationToken::new();
        let mut sequence = 0;

        let error = write_frame_bounded(
            &mut writer,
            &authenticator,
            &mut sequence,
            &RemoteScannerFrame::progress(RemoteScannerProgress::default()),
            &disconnect,
            Duration::from_millis(10),
        )
        .await
        .expect_err("a response with no reader must time out");

        assert!(error.to_string().contains("timed out"));
        assert_eq!(sequence, 0);
    }

    #[tokio::test]
    async fn external_cancellation_interrupts_a_stalled_frame_read() {
        let request_id = Uuid::new_v4();
        let authenticator = FrameAuthenticator::for_test(request_id);
        let (_writer, reader) = tokio::io::duplex(1);
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let cancel = parent.clone();
        let task = tokio::spawn(async move {
            consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, authenticator).await
        });

        tokio::task::yield_now().await;
        cancel.cancel();
        let error = tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .expect("cancelled frame read should finish promptly")
            .expect("cancelled frame read task should not panic")
            .expect_err("cancelled frame read must fail");

        assert!(error.to_string().contains("cancelled"));
    }

    #[test]
    fn request_decode_rejects_unknown_fields_without_a_protocol_bump() {
        #[derive(Serialize)]
        struct AdditiveRequestWire {
            version: u16,
            request_id: Uuid,
            server_epoch: Uuid,
            session_id: Uuid,
            session_sequence: u64,
            bucket: String,
            next_cycle: u64,
            leader_epoch: u64,
            scan_plan_digest: DataUsageScanPlanDigest,
            skip_healing: bool,
            scan_mode: HealScanMode,
            budget: RemoteScannerBudget,
            future_optional_hint: bool,
        }

        let request_id = Uuid::new_v4();
        let body = rmp_serde::to_vec_named(&AdditiveRequestWire {
            version: NS_SCANNER_PROTOCOL_VERSION,
            request_id,
            server_epoch: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            session_sequence: 0,
            bucket: "bucket".to_string(),
            next_cycle: 7,
            leader_epoch: 9,
            scan_plan_digest: TEST_PLAN_DIGEST,
            skip_healing: true,
            scan_mode: HealScanMode::Deep,
            budget: RemoteScannerBudget::default(),
            future_optional_hint: true,
        })
        .expect("additive request should encode");
        assert!(decode_remote_scanner_request(&body).is_err());
    }

    #[test]
    fn request_rejects_invalid_bucket_and_nil_ids() {
        let mut invalid_bucket = test_request(Uuid::new_v4());
        invalid_bucket.bucket = "../bucket".to_string();
        let body = rmp_serde::to_vec_named(&invalid_bucket).expect("request should encode");
        assert!(decode_remote_scanner_request(&body).is_err());

        let body = rmp_serde::to_vec_named(&test_request(Uuid::nil())).expect("request should encode");
        assert!(decode_remote_scanner_request(&body).is_err());

        let mut nil_epoch = test_request(Uuid::new_v4());
        nil_epoch.server_epoch = Uuid::nil();
        let body = rmp_serde::to_vec_named(&nil_epoch).expect("request should encode");
        assert!(decode_remote_scanner_request(&body).is_err());

        let mut nil_session = test_request(Uuid::new_v4());
        nil_session.session_id = Uuid::nil();
        let body = rmp_serde::to_vec_named(&nil_session).expect("request should encode");
        assert!(decode_remote_scanner_request(&body).is_err());

        let mut zero_leader_epoch = test_request(Uuid::new_v4());
        zero_leader_epoch.leader_epoch = 0;
        let body = rmp_serde::to_vec_named(&zero_leader_epoch).expect("request should encode");
        assert!(decode_remote_scanner_request(&body).is_err());
    }

    #[test]
    fn request_rejects_empty_truncated_oversized_and_wrong_version_payloads() {
        assert!(decode_remote_scanner_request(&[]).is_err());

        let mut body = rmp_serde::to_vec_named(&test_request(Uuid::new_v4())).expect("request should encode");
        body.truncate(body.len() / 2);
        assert!(decode_remote_scanner_request(&body).is_err());

        let oversized = vec![0_u8; NS_SCANNER_MAX_REQUEST_BODY_SIZE + 1];
        assert!(decode_remote_scanner_request(&oversized).is_err());

        for version in [NS_SCANNER_PROTOCOL_VERSION - 1, NS_SCANNER_PROTOCOL_VERSION + 1] {
            let mut wrong_version = test_request(Uuid::new_v4());
            wrong_version.version = version;
            let body = rmp_serde::to_vec_named(&wrong_version).expect("request should encode");
            assert!(decode_remote_scanner_request(&body).is_err());
        }
    }

    #[test]
    fn disk_scan_error_scope_distinguishes_bucket_failures_from_offline_workers() {
        let bucket_error = RemoteScannerServerError::disk_scan(ScannerError::Other("metadata corrupt".to_string()), true);
        let worker_error = RemoteScannerServerError::disk_scan(ScannerError::Other("disk offline".to_string()), false);

        assert_eq!(bucket_error.scope, RemoteScannerErrorScope::Bucket);
        assert_eq!(worker_error.scope, RemoteScannerErrorScope::Worker);
    }

    #[test]
    fn persisted_cycle_decoder_requires_a_valid_leader_fence() {
        let extended = crate::scanner::encode_scanner_cycle_fence_for_test(42, 9);

        assert_eq!(
            crate::scanner::decode_persisted_scanner_cycle_fence(&extended).expect("fenced cycle"),
            (42, 9)
        );
        assert_eq!(
            crate::scanner::decode_persisted_scanner_cycle_fence(&42_u64.to_le_bytes()).expect("legacy cycle"),
            (42, 0)
        );
        assert!(crate::scanner::decode_persisted_scanner_cycle_fence(&[0_u8; 7]).is_err());
    }

    #[tokio::test]
    async fn request_cycle_validation_reads_persisted_store_state() {
        let request = RemoteScannerRequest(test_request(Uuid::new_v4()));
        let store = Arc::new(CycleStateStore {
            state: Some(crate::scanner::encode_scanner_cycle_fence_for_test(
                request.0.next_cycle,
                request.0.leader_epoch,
            )),
        });
        assert_eq!(
            validate_remote_scanner_request_fence_with_store(request.0.next_cycle, request.0.leader_epoch, store)
                .await
                .expect("matching persisted fence should validate"),
            (7, 9)
        );

        let stale_store = Arc::new(CycleStateStore {
            state: Some(crate::scanner::encode_scanner_cycle_fence_for_test(8, request.0.leader_epoch)),
        });
        let err = validate_remote_scanner_request_fence_with_store(request.0.next_cycle, request.0.leader_epoch, stale_store)
            .await
            .expect_err("mismatched persisted cycle must be rejected");
        assert!(err.to_string().contains("requested_cycle=7"));

        let mut initial_request = test_request(Uuid::new_v4());
        initial_request.next_cycle = 0;
        initial_request.leader_epoch = 0;
        assert_eq!(
            validate_remote_scanner_request_fence_with_store(
                initial_request.next_cycle,
                initial_request.leader_epoch,
                Arc::new(CycleStateStore { state: None }),
            )
            .await
            .expect("missing state should validate only the initial fence"),
            (0, 0)
        );
    }

    #[tokio::test]
    async fn fence_watcher_stops_work_after_persisted_leader_changes() {
        let store = Arc::new(MutableCycleStateStore {
            state: RwLock::new(crate::scanner::encode_scanner_cycle_fence_for_test(7, 9)),
        });
        let watcher = tokio::spawn(watch_remote_scanner_request_fence(7, 9, store.clone(), Duration::from_millis(5)));

        tokio::time::sleep(Duration::from_millis(20)).await;
        *store.state.write().expect("cycle state test lock should remain available") =
            crate::scanner::encode_scanner_cycle_fence_for_test(7, 10);

        let err = tokio::time::timeout(Duration::from_millis(100), watcher)
            .await
            .expect("fence watcher should observe the leader change")
            .expect("fence watcher task should not panic")
            .expect_err("a replacement leader must cancel old scanner work");
        assert!(err.to_string().contains("requested_epoch=9"));
    }

    #[tokio::test]
    async fn concurrent_cycle_cache_misses_share_one_backend_read() {
        let request = RemoteScannerRequest(test_request(Uuid::new_v4()));
        let store = Arc::new(CountingCycleStateStore {
            state: crate::scanner::encode_scanner_cycle_fence_for_test(request.0.next_cycle, request.0.leader_epoch),
            reads: AtomicUsize::new(0),
        });
        let cache = Mutex::new(None);
        let refresh = AsyncMutex::new(());

        let results = futures::future::join_all((0..16).map(|_| {
            validate_remote_scanner_request_fence_cached(
                request.0.next_cycle,
                request.0.leader_epoch,
                store.clone(),
                &cache,
                &refresh,
            )
        }))
        .await;

        assert!(results.into_iter().all(|result| result.is_ok()));
        assert_eq!(store.reads.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn remote_scanner_admission_allows_one_request_per_disk() {
        let key = format!("test-disk-{}", Uuid::new_v4());
        let first = try_admit_remote_scanner_key(key.clone()).expect("first request should be admitted");
        assert!(try_admit_remote_scanner_key(key.clone()).is_err());
        drop(first);
        assert!(try_admit_remote_scanner_key(key).is_ok());
    }

    #[test]
    fn admission_rejects_busy_disk_before_replay_state_changes() {
        let key = format!("test-disk-{}", Uuid::new_v4());
        let active = try_admit_remote_scanner_key(key.clone()).expect("first request should be admitted");

        assert!(matches!(try_admit_remote_scanner_key(key.clone()), Err(ScannerError::RemoteDiskBusy)));
        drop(active);

        assert!(try_admit_remote_scanner_key(key).is_ok());
    }

    #[test]
    fn replay_cache_requires_contiguous_sequence_per_disk_session() {
        let session_id = Uuid::new_v4();
        let mut cache = RemoteScannerReplayCache::default();

        cache
            .claim("disk-a".to_string(), session_id, 7, 9, 0)
            .expect("first session request should be accepted");
        assert!(matches!(
            cache.claim("disk-a".to_string(), session_id, 7, 9, 0),
            Err(ScannerError::RemoteRequestReplay)
        ));
        assert!(matches!(
            cache.claim("disk-a".to_string(), session_id, 7, 9, 2),
            Err(ScannerError::RemoteRequestReplay)
        ));
        cache
            .claim("disk-a".to_string(), session_id, 7, 9, 1)
            .expect("next contiguous session request should be accepted");
    }

    #[test]
    fn replay_preflight_rejects_a_claimed_request_without_mutating_state() {
        let session_id = Uuid::new_v4();
        let mut cache = RemoteScannerReplayCache::default();
        cache
            .claim("disk-a".to_string(), session_id, 7, 9, 0)
            .expect("first request should be accepted");
        let state_before = (cache.cycle, cache.leader_epoch, cache.sessions.clone());

        assert!(matches!(
            cache.preflight("disk-a", session_id, 7, 9, 0),
            Err(ScannerError::RemoteRequestReplay)
        ));
        assert_eq!((cache.cycle, cache.leader_epoch, cache.sessions), state_before);
    }

    #[test]
    fn replay_cache_discards_sessions_from_prior_cycle() {
        let session_id = Uuid::new_v4();
        let mut cache = RemoteScannerReplayCache::default();

        cache
            .claim("disk-a".to_string(), session_id, 7, 9, 0)
            .expect("first cycle session should be accepted");
        cache
            .claim("disk-a".to_string(), session_id, 8, 9, 0)
            .expect("new cycle should start a fresh session sequence");
        assert_eq!(cache.sessions.len(), 1);
        assert!(matches!(
            cache.claim("disk-a".to_string(), Uuid::new_v4(), 7, 9, 0),
            Err(ScannerError::RemoteRequestReplay)
        ));
        assert_eq!(cache.cycle, Some(8));
        assert_eq!(cache.sessions.len(), 1);
    }

    #[test]
    fn replay_cache_fences_sessions_by_leader_epoch() {
        let mut cache = RemoteScannerReplayCache::default();
        cache
            .claim("disk-a".to_string(), Uuid::new_v4(), 7, 9, 0)
            .expect("first leader session should be accepted");
        cache
            .claim("disk-a".to_string(), Uuid::new_v4(), 7, 10, 0)
            .expect("replacement leader should reset replay state");

        assert_eq!(cache.cycle, Some(7));
        assert_eq!(cache.leader_epoch, Some(10));
        assert_eq!(cache.sessions.len(), 1);
        assert!(matches!(
            cache.claim("disk-a".to_string(), Uuid::new_v4(), 8, 9, 0),
            Err(ScannerError::RemoteRequestReplay)
        ));
    }

    #[test]
    fn replay_cache_scales_with_sessions_instead_of_bucket_requests() {
        let session_id = Uuid::new_v4();
        let mut cache = RemoteScannerReplayCache::default();
        let request_count = u64::try_from(NS_SCANNER_MAX_REPLAY_SESSIONS)
            .expect("session limit should fit in u64")
            .saturating_add(1024);

        for sequence in 0..request_count {
            cache
                .claim("disk-a".to_string(), session_id, 7, 9, sequence)
                .expect("contiguous requests in one worker session should not consume session capacity");
        }

        assert_eq!(cache.sessions.len(), 1);
        assert_eq!(cache.sessions.get(&("disk-a".to_string(), session_id)), Some(&(request_count - 1)));
    }

    #[test]
    fn validated_cycle_cache_expires_and_rejects_other_cycles() {
        let now = Instant::now();
        let cached = RemoteScannerValidatedCycle {
            cycle: 7,
            leader_epoch: 9,
            valid_until: now + NS_SCANNER_VALIDATED_CYCLE_TTL,
        };

        assert!(cached.matches(7, 9, now));
        assert!(!cached.matches(8, 9, now));
        assert!(!cached.matches(7, 10, now));
        assert!(!cached.matches(7, 9, now + NS_SCANNER_VALIDATED_CYCLE_TTL));
    }

    #[test]
    fn replay_cache_fails_closed_at_capacity() {
        let mut cache = RemoteScannerReplayCache::default();
        for _ in 0..NS_SCANNER_MAX_REPLAY_SESSIONS {
            cache
                .claim("disk-a".to_string(), Uuid::new_v4(), 7, 9, 0)
                .expect("session should fit in replay cache");
        }

        let error = cache
            .claim("disk-a".to_string(), Uuid::new_v4(), 7, 9, 0)
            .expect_err("session beyond replay cache capacity must fail");
        assert!(matches!(error, ScannerError::RemoteReplayCapacity));
    }

    #[tokio::test]
    async fn complete_terminal_frame_reconciles_progress_and_usage() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::progress(RemoteScannerProgress {
                    objects_scanned: 2,
                    directories_started: 1,
                    ..Default::default()
                }),
            )
            .await
            .expect("progress should write");
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress {
                        objects_scanned: 3,
                        directories_started: 2,
                        ..Default::default()
                    },
                    RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                        source: TEST_SOURCE,
                        scan_plan_digest: TEST_PLAN_DIGEST,
                        usage: test_usage("bucket", 3),
                        pending_maintenance_work: true,
                    })),
                ),
            )
            .await
            .expect("terminal frame should write");
            writer.shutdown().await.expect("writer should shut down");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(10),
                max_directories: Some(10),
                ..Default::default()
            },
        );
        let outcome =
            consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
                .await
                .expect("stream should complete");

        let RemoteScannerOutcome::Complete {
            usage,
            pending_maintenance_work,
        } = outcome
        else {
            panic!("expected complete result");
        };
        assert_eq!(usage.entry.objects, 3);
        assert!(pending_maintenance_work);
        assert_eq!(budget.progress(), (3, 2));
    }

    #[tokio::test]
    async fn complete_terminal_frame_after_budget_expiry_is_partial() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress {
                        objects_scanned: 3,
                        directories_started: 1,
                        ..Default::default()
                    },
                    RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                        source: TEST_SOURCE,
                        scan_plan_digest: TEST_PLAN_DIGEST,
                        usage: test_usage("bucket", 3),
                        pending_maintenance_work: false,
                    })),
                ),
            )
            .await
            .expect("terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(3),
                ..Default::default()
            },
        );
        let outcome =
            consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
                .await
                .expect("stream should finish as partial");

        assert!(matches!(outcome, RemoteScannerOutcome::Partial));
        assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Objects));
    }

    #[tokio::test]
    async fn partial_terminal_frame_is_reported_without_usage() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress {
                        objects_scanned: 4,
                        directories_started: 2,
                        ..Default::default()
                    },
                    RemoteScannerFrameResult::Partial,
                ),
            )
            .await
            .expect("partial terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
        let outcome =
            consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
                .await
                .expect("partial terminal frame should be accepted");

        assert!(matches!(outcome, RemoteScannerOutcome::Partial));
        assert_eq!(budget.progress(), (4, 2));
    }

    #[tokio::test]
    async fn namespace_not_found_terminal_frame_is_reported_without_usage() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress {
                        objects_scanned: 4,
                        directories_started: 2,
                        ..Default::default()
                    },
                    RemoteScannerFrameResult::NamespaceNotFound,
                ),
            )
            .await
            .expect("namespace-not-found terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
        let outcome =
            consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
                .await
                .expect("namespace-not-found terminal frame should be accepted");

        assert!(matches!(outcome, RemoteScannerOutcome::NamespaceNotFound));
        assert_eq!(budget.progress(), (4, 2));
    }

    #[tokio::test]
    async fn error_terminal_frame_is_reported_as_failure() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress::default(),
                    RemoteScannerFrameResult::Error(RemoteScannerErrorFrame {
                        scope: RemoteScannerErrorScope::Bucket,
                        message: "cache save failed".to_string(),
                    }),
                ),
            )
            .await
            .expect("error terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(10),
                ..Default::default()
            },
        );
        let stream_result =
            consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
                .await;
        let error = finish_remote_scanner_stream(stream_result, budget.as_ref()).expect_err("error terminal frame must fail");

        assert!(error.to_string().contains("cache save failed"));
        assert!(!error.retire_worker());
        assert!(!budget.budget_elapsed());
    }

    #[tokio::test]
    async fn worker_error_terminal_frame_retires_worker_without_cancelling_reported_budget() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress::default(),
                    RemoteScannerFrameResult::Error(RemoteScannerErrorFrame {
                        scope: RemoteScannerErrorScope::Worker,
                        message: "object layer unavailable".to_string(),
                    }),
                ),
            )
            .await
            .expect("error terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(10),
                ..Default::default()
            },
        );
        let stream_result =
            consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
                .await;
        let error = finish_remote_scanner_stream(stream_result, budget.as_ref()).expect_err("worker error must fail");

        assert!(error.to_string().contains("object layer unavailable"));
        assert!(error.retire_worker());
        assert!(!error.retry_bucket_work());
        assert!(!budget.budget_elapsed());
    }

    #[test]
    fn uncertain_stream_failure_cancels_count_budget() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(10),
                ..Default::default()
            },
        );
        let result = Err(RemoteScannerStreamError::uncertain(StorageError::other("connection lost")));

        let error = finish_remote_scanner_stream(result, budget.as_ref()).expect_err("transport failure must fail");

        assert!(error.to_string().contains("connection lost"));
        assert!(error.retire_worker());
        assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Objects));
    }

    #[test]
    fn directory_entry_activity_counts_as_semantic_progress() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let mut last = RemoteScannerProgress::default();

        let advanced = apply_remote_progress(
            budget.as_ref(),
            &mut last,
            RemoteScannerProgress {
                entries_visited: 32,
                ..Default::default()
            },
        )
        .expect("entry activity should be valid progress");

        assert!(advanced);
        assert_eq!(last.entries_visited, 32);
        assert_eq!(budget.progress(), (0, 0));
    }

    #[tokio::test]
    async fn disconnect_after_persisting_keeps_reported_count_budget() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::with_phase(
                    RemoteScannerProgress {
                        objects_scanned: 3,
                        directories_started: 2,
                        ..Default::default()
                    },
                    RemoteScannerPhase::Persisting,
                    RemoteScannerFrameResult::Progress,
                ),
            )
            .await
            .expect("persisting progress should write");
            writer.shutdown().await.expect("writer should shut down");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(10),
                max_directories: Some(10),
                ..Default::default()
            },
        );
        let stream_result =
            consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
                .await;

        finish_remote_scanner_stream(stream_result, budget.as_ref())
            .expect_err("disconnect before terminal persistence result must fail");
        assert_eq!(budget.progress(), (3, 2));
        assert!(!budget.budget_elapsed());
    }

    #[tokio::test]
    async fn scanner_phase_cannot_move_back_to_scanning() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            for phase in [RemoteScannerPhase::Persisting, RemoteScannerPhase::Scanning] {
                write_frame(
                    &mut writer,
                    &writer_auth,
                    &mut sequence,
                    &RemoteScannerFrame::with_phase(RemoteScannerProgress::default(), phase, RemoteScannerFrameResult::Progress),
                )
                .await
                .expect("phase frame should write");
            }
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect_err("backwards phase must fail");

        assert!(error.to_string().contains("phase moved backwards"));
    }

    #[tokio::test]
    async fn terminal_usage_for_another_bucket_is_rejected() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress::default(),
                    RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                        source: TEST_SOURCE,
                        scan_plan_digest: TEST_PLAN_DIGEST,
                        usage: test_usage("other-bucket", 1),
                        pending_maintenance_work: false,
                    })),
                ),
            )
            .await
            .expect("terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect_err("wrong bucket must fail");
        assert!(error.to_string().contains("wrong bucket"));
    }

    #[tokio::test]
    async fn terminal_usage_for_another_source_is_rejected() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress::default(),
                    RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                        source: DataUsageCacheSource::new(TEST_SOURCE.pool_index + 1, TEST_SOURCE.set_index),
                        scan_plan_digest: TEST_PLAN_DIGEST,
                        usage: test_usage("bucket", 1),
                        pending_maintenance_work: false,
                    })),
                ),
            )
            .await
            .expect("terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect_err("wrong source must fail");
        assert!(error.to_string().contains("wrong pool or set"));
    }

    #[tokio::test]
    async fn terminal_usage_must_be_flattened() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut usage = test_usage("bucket", 1);
            usage.entry.children.insert("child-hash".to_string());
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress::default(),
                    RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                        source: TEST_SOURCE,
                        scan_plan_digest: TEST_PLAN_DIGEST,
                        usage,
                        pending_maintenance_work: false,
                    })),
                ),
            )
            .await
            .expect("terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect_err("non-flattened usage must fail");
        assert!(error.to_string().contains("non-flattened"));
    }

    #[tokio::test]
    async fn terminal_cycle_ahead_reports_required_cycle() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress::default(),
                    RemoteScannerFrameResult::CycleAhead {
                        required_cycle: TEST_NEXT_CYCLE + 1,
                    },
                ),
            )
            .await
            .expect("terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let outcome = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect("newer remote cache cycle should be reported");
        assert!(matches!(
            outcome,
            RemoteScannerOutcome::CycleAhead(required_cycle) if required_cycle == TEST_NEXT_CYCLE + 1
        ));
    }

    #[tokio::test]
    async fn terminal_cycle_ahead_rejects_nonincreasing_cycle() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress::default(),
                    RemoteScannerFrameResult::CycleAhead {
                        required_cycle: TEST_NEXT_CYCLE,
                    },
                ),
            )
            .await
            .expect("terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect_err("nonincreasing required cycle must fail");
        assert!(error.to_string().contains("invalid required cycle"));
    }

    #[tokio::test]
    async fn terminal_usage_accepts_large_replication_target_payload_within_frame_limit() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(NS_SCANNER_MAX_FRAME_SIZE);
        tokio::spawn(async move {
            let mut usage = test_usage("bucket", 1);
            let stats = usage.entry.replication_stats.get_or_insert_default();
            for index in 0..=1024 {
                stats.targets.insert(format!("target-{index}"), Default::default());
            }
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress::default(),
                    RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                        source: TEST_SOURCE,
                        scan_plan_digest: TEST_PLAN_DIGEST,
                        usage,
                        pending_maintenance_work: false,
                    })),
                ),
            )
            .await
            .expect("terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let outcome = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect("large historical replication target payload should remain readable");
        assert!(matches!(outcome, RemoteScannerOutcome::Complete { .. }));
    }

    #[tokio::test]
    async fn tampered_frame_authentication_is_rejected() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator {
            request_id,
            secret: "different-secret".to_string(),
        };
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(RemoteScannerProgress::default(), RemoteScannerFrameResult::Partial),
            )
            .await
            .expect("terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect_err("tampered authentication must fail");
        assert!(error.to_string().contains("authentication failed"));
    }

    #[tokio::test]
    async fn wrong_frame_version_and_sequence_are_rejected() {
        let request_id = Uuid::new_v4();
        let auth = FrameAuthenticator::for_test(request_id);
        let frame = RemoteScannerFrame::progress(RemoteScannerProgress::default());
        let payload = rmp_serde::to_vec_named(&frame).expect("frame should encode");

        for (version, sequence, expected_error) in [
            (NS_SCANNER_PROTOCOL_VERSION - 1, 0, "unsupported remote namespace scanner frame version"),
            (NS_SCANNER_PROTOCOL_VERSION + 1, 0, "unsupported remote namespace scanner frame version"),
            (NS_SCANNER_PROTOCOL_VERSION, 1, "frame sequence is invalid"),
        ] {
            let envelope = RemoteScannerFrameEnvelope {
                version,
                sequence,
                mac: auth.sign(sequence, &payload).expect("frame should sign"),
                payload: payload.clone(),
            };
            let encoded = rmp_serde::to_vec_named(&envelope).expect("envelope should encode");
            let mut input = std::io::Cursor::new(Vec::with_capacity(encoded.len() + 4));
            input.get_mut().extend_from_slice(
                &u32::try_from(encoded.len())
                    .expect("encoded frame length should fit u32")
                    .to_be_bytes(),
            );
            input.get_mut().extend_from_slice(&encoded);
            input.set_position(0);

            let mut expected_sequence = 0;
            let error = read_frame(&mut input, &auth, &mut expected_sequence)
                .await
                .expect_err("invalid frame must fail");
            assert!(error.to_string().contains(expected_error));
        }
    }

    #[tokio::test]
    async fn backwards_progress_is_rejected() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::progress(RemoteScannerProgress {
                    objects_scanned: 2,
                    directories_started: 1,
                    ..Default::default()
                }),
            )
            .await
            .expect("progress should write");
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::terminal(
                    RemoteScannerProgress {
                        objects_scanned: 1,
                        directories_started: 1,
                        ..Default::default()
                    },
                    RemoteScannerFrameResult::Partial,
                ),
            )
            .await
            .expect("terminal frame should write");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect_err("backwards progress must fail");
        assert!(error.to_string().contains("moved backwards"));
    }

    #[tokio::test]
    async fn eof_without_terminal_frame_is_rejected() {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut sequence = 0;
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::progress(RemoteScannerProgress::default()),
            )
            .await
            .expect("progress should write");
            writer.shutdown().await.expect("writer should shut down");
        });

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect_err("missing terminal frame must fail");
        assert!(error.to_string().contains("before a terminal frame"));
    }

    #[test]
    fn oversized_frame_is_rejected_before_allocation() {
        let request_id = Uuid::new_v4();
        let auth = FrameAuthenticator::for_test(request_id);
        let mut input = std::io::Cursor::new(
            u32::try_from(NS_SCANNER_MAX_FRAME_SIZE + 1)
                .expect("test frame size should fit u32")
                .to_be_bytes()
                .to_vec(),
        );
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        let mut sequence = 0;
        let error = runtime
            .block_on(read_frame(&mut input, &auth, &mut sequence))
            .expect_err("oversized frame must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }
}
