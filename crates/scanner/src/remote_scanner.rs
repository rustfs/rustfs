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

use crate::storage_api::owner::NS_SCANNER_PROTOCOL_VERSION;
use crate::{
    DATA_USAGE_BLOOM_NAME_PATH, Disk, EcstoreError, ScannerDiskExt as _, ScannerError, ScannerObjectIO,
    is_reserved_or_invalid_bucket, read_config, resolve_scanner_object_store_handle,
};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio::time::{Instant, MissedTickBehavior};
use uuid::Uuid;

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
const NS_SCANNER_RETRY_BUCKET_ERROR_PREFIX: &str = "retry_bucket:";
const NS_SCANNER_FRAME_AUTH_DOMAIN: &[u8] = b"rustfs-ns-scanner-frame-v3";

pub const NS_SCANNER_MAX_REQUEST_BODY_SIZE: usize = 16 * 1024;

static REMOTE_SCANNER_REPLAY_CACHE: LazyLock<Mutex<RemoteScannerReplayCache>> =
    LazyLock::new(|| Mutex::new(RemoteScannerReplayCache::default()));
static REMOTE_SCANNER_ADMISSION: LazyLock<Mutex<HashMap<String, Arc<Semaphore>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static REMOTE_SCANNER_VALIDATED_CYCLE: LazyLock<Mutex<Option<RemoteScannerValidatedCycle>>> = LazyLock::new(|| Mutex::new(None));
static REMOTE_SCANNER_CYCLE_REFRESH: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));

mod stream;

pub use stream::{RemoteScannerAdmission, RemoteScannerRequest, serve_remote_scanner_request};
pub(crate) use stream::{RemoteScannerOutcome, RemoteScannerScanSpec, scan_remote_bucket};
use stream::{RemoteScannerReplayCache, RemoteScannerRequestWire, RemoteScannerValidatedCycle};

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
        NS_SCANNER_VALIDATED_CYCLE_TTL,
    )
    .await
}

async fn validate_remote_scanner_request_fence_cached(
    requested_cycle: u64,
    requested_leader_epoch: u64,
    store: Arc<impl ScannerObjectIO>,
    cache: &Mutex<Option<RemoteScannerValidatedCycle>>,
    refresh: &AsyncMutex<()>,
    cache_ttl: Duration,
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
        .checked_add(cache_ttl)
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
    watch_remote_scanner_request_fence_with_cache(
        requested_cycle,
        requested_leader_epoch,
        store,
        poll_interval,
        &REMOTE_SCANNER_VALIDATED_CYCLE,
        &REMOTE_SCANNER_CYCLE_REFRESH,
        NS_SCANNER_VALIDATED_CYCLE_TTL,
    )
    .await
}

async fn watch_remote_scanner_request_fence_with_cache(
    requested_cycle: u64,
    requested_leader_epoch: u64,
    store: Arc<impl ScannerObjectIO>,
    poll_interval: Duration,
    cache: &Mutex<Option<RemoteScannerValidatedCycle>>,
    refresh: &AsyncMutex<()>,
    cache_ttl: Duration,
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
        validate_remote_scanner_request_fence_cached(
            requested_cycle,
            requested_leader_epoch,
            store.clone(),
            cache,
            refresh,
            cache_ttl,
        )
        .await?;
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
