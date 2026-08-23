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

#[cfg(test)]
use crate::RUSTFS_META_BUCKET;
use crate::scanner_budget::{ScannerCycleBudget, ScannerCycleBudgetConfig};
use crate::scanner_io::{
    DataUsageCacheScanState, ScannerDiskScanOutcome, ScannerIODisk, acquire_scanner_cache_locks, cache_root_entry_info,
    current_cache_root_or_prepare, scanner_set_disk_inventory,
};
use crate::storage_api::owner::NS_SCANNER_PROTOCOL_VERSION;
use crate::{
    DATA_USAGE_CACHE_NAME, DataUsageCache, DataUsageCachePrepareOutcome, DataUsageCacheSource, DataUsageEntryInfo,
    DataUsageScanPlanDigest, Disk, ScannerError, StorageError, resolve_scanner_object_store_handle,
    scanner_publication_admission_for_epoch, scanner_publication_epoch,
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
    Arc,
    atomic::{AtomicU8, Ordering},
};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, OwnedSemaphorePermit};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;
use super::*;

pub struct RemoteScannerAdmission {
    pub(super) _permit: OwnedSemaphorePermit,
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
pub(super) struct RemoteScannerRequestWire {
    pub(super) version: u16,
    pub(super) request_id: Uuid,
    pub(super) server_epoch: Uuid,
    pub(super) session_id: Uuid,
    pub(super) session_sequence: u64,
    pub(super) bucket: String,
    pub(super) next_cycle: u64,
    pub(super) leader_epoch: u64,
    scan_plan_digest: DataUsageScanPlanDigest,
    skip_healing: bool,
    scan_mode: HealScanMode,
    budget: RemoteScannerBudget,
}

#[derive(Debug)]
pub struct RemoteScannerRequest(pub(super) RemoteScannerRequestWire);

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

    fn retry_bucket(message: impl Into<String>) -> Self {
        Self {
            scope: RemoteScannerErrorScope::Bucket,
            error: ScannerError::Other(format!("{}{}", NS_SCANNER_RETRY_BUCKET_ERROR_PREFIX, message.into())),
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
    retry_bucket: bool,
}

impl RemoteScannerStreamError {
    fn uncertain(error: StorageError) -> Self {
        Self {
            error,
            progress_fully_reported: false,
            retire_worker: true,
            retry_bucket: false,
        }
    }

    fn reconciled(error: StorageError) -> Self {
        Self {
            error,
            progress_fully_reported: true,
            retire_worker: true,
            retry_bucket: false,
        }
    }

    fn bucket(error: StorageError) -> Self {
        Self {
            error,
            progress_fully_reported: true,
            retire_worker: false,
            retry_bucket: false,
        }
    }

    fn retry_bucket(error: StorageError) -> Self {
        Self {
            error,
            progress_fully_reported: true,
            retire_worker: false,
            retry_bucket: true,
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
pub(super) struct RemoteScannerReplayCache {
    cycle: Option<u64>,
    leader_epoch: Option<u64>,
    sessions: HashMap<(String, Uuid), u64>,
}

#[derive(Clone, Copy)]
pub(super) struct RemoteScannerValidatedCycle {
    pub(super) cycle: u64,
    pub(super) leader_epoch: u64,
    pub(super) valid_until: Instant,
}

impl RemoteScannerValidatedCycle {
    pub(super) fn matches(self, requested_cycle: u64, requested_leader_epoch: u64, now: Instant) -> bool {
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

    pub(super) fn preflight(
        &self,
        disk_key: &str,
        session_id: Uuid,
        cycle: u64,
        leader_epoch: u64,
        sequence: u64,
    ) -> Result<(), ScannerError> {
        self.preflight_key(&(disk_key.to_string(), session_id), cycle, leader_epoch, sequence)
    }

    pub(super) fn claim(
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
    let expected_publication_epoch = scanner_publication_epoch(set.clone()).await.ok_or_else(|| {
        RemoteScannerServerError::worker("remote namespace scanner cache publication is blocked by data movement")
    })?;
    let cache_name = path_join_buf(&[&bucket, DATA_USAGE_CACHE_NAME]);
    let guard = acquire_scanner_cache_locks(set.as_ref(), &cache_name, source)
        .await
        .map_err(|err| {
            if err.is_contention() {
                RemoteScannerServerError::retry_bucket(format!("remote namespace scanner cache lock contention: {err}"))
            } else {
                RemoteScannerServerError::worker(format!("remote namespace scanner cache lock acquisition failed: {err}"))
            }
        })?;
    let mut cache = DataUsageCache::default();
    let revisions = cache.load_with_revisions(set.clone(), &cache_name).await.map_err(|err| {
        RemoteScannerServerError::worker(format!("remote namespace scanner cache load or revision lookup failed: {err}"))
    })?;
    let scan_state = current_cache_root_or_prepare(&mut cache, &bucket, source, next_cycle, leader_epoch, scan_plan_digest, true);
    match scan_state {
        DataUsageCacheScanState::Current(usage) => {
            if guard.is_lock_lost() {
                return Err(RemoteScannerServerError::worker(
                    "remote namespace scanner cache lock was lost before reusing the current snapshot",
                ));
            }
            if scanner_publication_admission_for_epoch(set.clone(), expected_publication_epoch)
                .await
                .is_none()
            {
                return Err(RemoteScannerServerError::retry_bucket(
                    "remote namespace scanner cache publication epoch changed before reusing the current snapshot",
                ));
            }
            return Ok(RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                source,
                scan_plan_digest,
                usage: *usage,
                pending_maintenance_work: !cache.info.pending_heals.is_empty(),
            })));
        }
        DataUsageCacheScanState::Prepared { outcome, .. } => match outcome {
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
        },
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
    // Each physical main/backup PUT must still prove the epoch captured before
    // the scan. A movement transition that starts and ends during the scan
    // therefore cannot admit the stale cache under the new epoch.
    let save_result = cache
        .save_with_revisions_for_epoch(set.clone(), &cache_name, &revisions, expected_publication_epoch)
        .await;
    done_save();
    save_result.map_err(|err| RemoteScannerServerError::worker(format!("remote namespace scanner cache save failed: {err}")))?;
    if scanner_publication_admission_for_epoch(set, expected_publication_epoch)
        .await
        .is_none()
    {
        return Err(RemoteScannerServerError::retry_bucket(
            "remote namespace scanner cache publication epoch changed after persistence",
        ));
    }
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
            let failure = if error.retry_bucket {
                RemoteScannerFailure::retry_bucket(error.error)
            } else if error.retire_worker {
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
                let retry_bucket = error_frame.message.starts_with(NS_SCANNER_RETRY_BUCKET_ERROR_PREFIX);
                let message = error_frame
                    .message
                    .strip_prefix(NS_SCANNER_RETRY_BUCKET_ERROR_PREFIX)
                    .map(str::trim_start)
                    .unwrap_or(error_frame.message.as_str());
                let error =
                    StorageError::other(format!("remote namespace scanner failed: {}", limit_error_message(message.to_string())));
                return Err(match error_frame.scope {
                    RemoteScannerErrorScope::Bucket if retry_bucket => RemoteScannerStreamError::retry_bucket(error),
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
mod tests;
