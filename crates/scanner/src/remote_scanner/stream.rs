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
    DataUsageCacheReuseOptions, DataUsageCacheScanState, ScannerBucketPlanDigests, ScannerCheckpointPersistContext,
    ScannerCheckpointPersistResult, ScannerDiskScanOptions, ScannerDiskScanOutcome, ScannerIODisk, acquire_scanner_cache_locks,
    cache_root_entry_info, current_cache_root_or_prepare_with_generation, log_saved_partial_bucket_cache,
    persist_scanner_checkpoint, scanner_set_disk_inventory,
};
use crate::storage_api::owner::{EcstoreBackgroundEncodeBuffer, EcstoreBackgroundMemoryBudget, NS_SCANNER_PROTOCOL_VERSION};
use crate::{
    DATA_USAGE_CACHE_NAME, DataUsageCache, DataUsageCachePrepareOutcome, DataUsageCacheSource, DataUsageEntryInfo,
    DataUsageScanPlanDigest, Disk, ScannerError, StorageError, resolve_scanner_object_store_handle,
    scanner_publication_admission_for_epoch, scanner_publication_epoch,
};
use hmac::{Hmac, KeyInit, Mac};
use rustfs_credentials::try_get_rpc_token;
use rustfs_heal_contracts::heal_channel::HealScanMode;
use rustfs_scanner_metrics::metrics::{CloseDiskGuard, Metric, Metrics, current_path_updater};
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
use tokio::sync::{Notify, OwnedSemaphorePermit, mpsc};
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
            ..Default::default()
        }
    }
}

#[cfg(test)]
pub(crate) fn remote_scanner_budget_wire_round_trip_for_test(config: ScannerCycleBudgetConfig) -> ScannerCycleBudgetConfig {
    let bytes = rmp_serde::to_vec_named(&RemoteScannerBudget::from_config(config)).expect("remote budget request wire");
    rmp_serde::from_slice::<RemoteScannerBudget>(&bytes)
        .expect("decode actual remote budget wire")
        .into_config()
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_plan_digest: Option<DataUsageScanPlanDigest>,
    #[serde(default, skip_serializing_if = "capability_disabled")]
    supports_durable_progress: bool,
    #[serde(default, skip_serializing_if = "capability_disabled")]
    supports_complete_observation: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) usage_watch: Option<Box<crate::scanner_io::ScannerUsageWatchProof>>,
    skip_healing: bool,
    scan_mode: HealScanMode,
    budget: RemoteScannerBudget,
}

fn capability_disabled(value: &bool) -> bool {
    !*value
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

/// Only a successful durable cache CAS can produce this terminal receipt.
/// Progress accounting is separate from bucket usage and never certifies a root.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteScannerDurableProgress {
    source: DataUsageCacheSource,
    identity: crate::DataUsageScanIdentity,
    scan_plan_digest: DataUsageScanPlanDigest,
    checkpoint_plan_digest: DataUsageScanPlanDigest,
    leader_epoch: u64,
    next_cycle: u64,
    advanced: bool,
    resource_deferred: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observation: Option<Box<crate::DataUsageBucketObservation>>,
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
    DurableProgress(Box<RemoteScannerDurableProgress>),
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
    DurableProgress {
        advanced: bool,
        resource_deferred: bool,
        observation: Option<Box<crate::DataUsageBucketObservation>>,
    },
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
    pub(crate) plans: ScannerBucketPlanDigests,
    pub(crate) supports_durable_progress: bool,
    pub(crate) supports_complete_observation: bool,
    pub(crate) usage_watch: Option<&'a crate::scanner_io::ScannerUsageWatchProof>,
    pub(crate) checkpoint_identity: Option<crate::DataUsageScanIdentity>,
    pub(crate) tier_registry_generation: u64,
    pub(crate) skip_healing: bool,
    pub(crate) scan_mode: HealScanMode,
}

#[derive(Clone, Copy)]
struct RemoteScannerResponseExpectation<'a> {
    supports_durable_progress: bool,
    supports_complete_observation: bool,
    checkpoint_identity: Option<crate::DataUsageScanIdentity>,
    checkpoint_plan_digest: DataUsageScanPlanDigest,
    leader_epoch: u64,
    bucket: &'a str,
    source: DataUsageCacheSource,
    next_cycle: u64,
    scan_plan_digest: DataUsageScanPlanDigest,
    tier_registry_generation: u64,
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

/// Feed the real remote consumer a completed physical fixture root after its
/// count budget stops. Only the transport and terminal timing are controlled.
#[cfg(test)]
pub(crate) async fn write_budget_drain_fixture_frames<W: AsyncWrite + Unpin>(
    mut writer: W,
    request_body: &[u8],
    source: DataUsageCacheSource,
    mut cache: DataUsageCache,
    store: Arc<crate::SetDisks>,
    release_complete: CancellationToken,
) {
    let request = decode_remote_scanner_request(request_body)
        .expect("valid real scanner request")
        .0;
    assert!(request.supports_durable_progress || request.supports_complete_observation);
    assert_eq!(cache.info.name, request.bucket, "the terminal must come from the actual fixture bucket");
    assert_eq!(cache.info.source, Some(source));
    assert!(cache.info.snapshot_complete);
    let identity = cache
        .info
        .scan_identity
        .expect("the physical root carries a durable identity");
    assert_eq!(
        store
            .bucket_incarnation_id_from_disk(&request.bucket)
            .await
            .expect("physical bucket incarnation"),
        identity.bucket_incarnation
    );
    // The fixture namespace is quiescent after its real scan. Rebind that exact
    // root to this request and perform the producer-side cache CAS before a
    // response may claim completion; no in-memory root stands in for storage.
    let cache_name = path_join_buf(&[&request.bucket, DATA_USAGE_CACHE_NAME]);
    let revisions = DataUsageCache::read_revisions(store.clone(), &cache_name)
        .await
        .expect("physical root revisions");
    cache.info.next_cycle = request.next_cycle;
    cache.info.leader_epoch = request.leader_epoch;
    cache.info.scan_plan_digest = Some(request.scan_plan_digest);
    cache.info.last_update = Some(std::time::SystemTime::now());
    cache
        .save_with_revisions_for_epoch(store.clone(), &cache_name, &revisions, identity.publication_epoch)
        .await
        .expect("durable current-request root CAS");
    let mut persisted = DataUsageCache::default();
    persisted
        .load(store, &cache_name)
        .await
        .expect("read back the durable current-request root");
    assert_eq!(persisted.info.next_cycle, request.next_cycle);
    assert_eq!(persisted.info.scan_plan_digest, Some(request.scan_plan_digest));
    let usage = cache_root_entry_info(&persisted).expect("flatten the read-back physical root");
    assert!(usage.entry.children.is_empty());
    assert_eq!(usage.bucket_incarnation, Some(identity.bucket_incarnation));
    let authenticator = FrameAuthenticator::from_rpc_secret(request.request_id).expect("fixture frame authentication");
    let progress = RemoteScannerProgress {
        objects_scanned: 1,
        directories_started: 0,
        entries_visited: 0,
    };
    let mut sequence = 0;
    write_frame(&mut writer, &authenticator, &mut sequence, &RemoteScannerFrame::progress(progress))
        .await
        .expect("write authenticated budget-stopping progress");
    release_complete.cancelled().await;
    write_frame(
        &mut writer,
        &authenticator,
        &mut sequence,
        &RemoteScannerFrame::terminal(
            progress,
            RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                source,
                scan_plan_digest: request.scan_plan_digest,
                usage,
                pending_maintenance_work: false,
            })),
        ),
    )
    .await
    .expect("write authenticated complete after the parent count stop");
    writer.shutdown().await.expect("remote complete must have a clean EOF");
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
    writer: W,
    disconnect: CancellationToken,
) -> Result<(), ScannerError>
where
    W: AsyncWrite + Unpin + Send,
{
    serve_remote_scanner_request_with_memory(disk, request, writer, disconnect, None).await
}

pub async fn serve_remote_scanner_request_with_memory<W>(
    disk: Arc<Disk>,
    request: RemoteScannerRequest,
    mut writer: W,
    disconnect: CancellationToken,
    memory: Option<Arc<EcstoreBackgroundMemoryBudget>>,
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
                write_frame_bounded_with_memory(
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
                    memory.as_ref(),
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
                if let Err(err) = write_frame_bounded_with_memory(
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
                    memory.as_ref(),
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
                if let Err(err) = write_frame_bounded_with_memory(
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
                    memory.as_ref(),
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

/// This is the final worker-side classification before the durable cache CAS.
/// A failed producer witness retains an EOF observation and fresh-sweep debt.
async fn classify_remote_usage_watch_outcome(
    store: &crate::ECStore,
    outcome: ScannerDiskScanOutcome,
    usage_watch: Option<&crate::scanner_io::ScannerUsageWatchProof>,
    scan_plan_digest: DataUsageScanPlanDigest,
    checkpoint_plan_digest: Option<DataUsageScanPlanDigest>,
    resumed_sweep: bool,
) -> (DataUsageCache, Option<RemoteScannerFrameResult>) {
    match outcome {
        ScannerDiskScanOutcome::Complete(mut cache) => {
            if resumed_sweep
                && usage_watch.is_some()
                && match cache.info.scan_usage_watch.as_ref() {
                    Some(proof) => !proof.verify_with_store(store).await,
                    None => true,
                }
            {
                crate::scanner_io::invalidate_completed_usage_watch_cache(
                    &mut cache,
                    checkpoint_plan_digest.unwrap_or(scan_plan_digest),
                );
                (cache, Some(RemoteScannerFrameResult::Partial))
            } else {
                // The folder completes under its coverage plan; finished cache
                // reuse and the response remain bound to the requested reuse plan.
                cache.info.scan_plan_digest = Some(scan_plan_digest);
                (cache, None)
            }
        }
        ScannerDiskScanOutcome::Partial(cache) => (cache, Some(RemoteScannerFrameResult::Partial)),
        ScannerDiskScanOutcome::NamespaceNotFound(cache) => (cache, Some(RemoteScannerFrameResult::NamespaceNotFound)),
    }
}

async fn remote_disk_belongs_to_set(disk: &Disk, set: &crate::SetDisks) -> bool {
    match disk {
        Disk::Local(requested) => set.disks.read().await.iter().flatten().any(|candidate| {
            matches!(candidate.as_ref(), Disk::Local(local)
                if Arc::ptr_eq(&requested.get_disk(), &local.get_disk()))
        }),
        Disk::Remote(_) => false,
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
        checkpoint_plan_digest,
        supports_durable_progress,
        supports_complete_observation,
        usage_watch,
        skip_healing,
        scan_mode,
        ..
    } = request;
    // Keep the worker's cycle snapshot alive through cache reuse, scanning,
    // and persistence. Without the guard, a later cycle can prune this key
    // while this request is still running and allow a second registry to be
    // selected for the same cycle.
    let _tier_cycle_guard = crate::begin_tier_registry_cycle(next_cycle, leader_epoch);
    let store = resolve_scanner_object_store_handle()
        .ok_or_else(|| RemoteScannerServerError::worker("remote namespace scanner object layer is unavailable"))?;
    if let Some(proof) = usage_watch.as_deref()
        && !proof.verify_with_store(&store).await
    {
        return Err(RemoteScannerServerError::retry_bucket(
            "remote namespace scanner usage watch is unavailable at admission",
        ));
    }
    validate_remote_scanner_request_fence_with_store(next_cycle, leader_epoch, store.clone())
        .await
        .map_err(|err| {
            RemoteScannerServerError::worker(format!("remote namespace scanner leader fence verification failed: {err}"))
        })?;
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
    // A process-global store can belong to another in-process instance. Disk
    // coordinates alone do not identify the physical producer whose watch is
    // being verified: require the exact LocalDisk allocation in this set.
    if !remote_disk_belongs_to_set(disk.as_ref(), &set).await {
        return Err(RemoteScannerServerError::worker(
            "remote namespace scanner disk does not belong to its storage instance",
        ));
    }
    let expected_publication_epoch = scanner_publication_epoch(set.clone()).await.ok_or_else(|| {
        RemoteScannerServerError::worker("remote namespace scanner cache publication is blocked by data movement")
    })?;
    let cache_name = path_join_buf(&[&bucket, DATA_USAGE_CACHE_NAME]);
    let mut guard = acquire_scanner_cache_locks(set.as_ref(), &cache_name, source)
        .await
        .map_err(|err| {
            if err.is_contention() {
                RemoteScannerServerError::retry_bucket(format!("remote namespace scanner cache lock contention: {err}"))
            } else {
                RemoteScannerServerError::worker(format!("remote namespace scanner cache lock acquisition failed: {err}"))
            }
        })?;
    let mut cache = DataUsageCache::default();
    let mut revisions = cache.load_with_revisions(set.clone(), &cache_name).await.map_err(|err| {
        RemoteScannerServerError::worker(format!("remote namespace scanner cache load or revision lookup failed: {err}"))
    })?;
    if let Some(proof) = usage_watch.as_deref()
        && cache.info.snapshot_complete
        && cache
            .info
            .scan_usage_watch
            .as_ref()
            .is_none_or(|stored| stored.digest() != proof.digest())
    {
        cache.info.snapshot_complete = false;
        cache.info.lkg_snapshot_complete = false;
    }
    // Remote workers use the same cycle-frozen registry as `scan_data_folder`.
    // Requiring its generation here prevents a cache snapshot classified by an
    // older registry from being reused before the folder scan gets a chance to
    // refresh it.
    let tier_registry_generation = crate::runtime_tier_registry_for_cycle(next_cycle, leader_epoch)
        .await
        .generation;
    let scan_state = current_cache_root_or_prepare_with_generation(
        &mut cache,
        &bucket,
        source,
        next_cycle,
        leader_epoch,
        scan_plan_digest,
        DataUsageCacheReuseOptions {
            require_source: true,
            tier_registry_generation: Some(tier_registry_generation),
            checkpoint_identity: crate::scanner_io::scanner_bucket_checkpoint_identity(
                &set,
                &bucket,
                expected_publication_epoch,
                tier_registry_generation,
                scan_mode,
            )
            .await
            .map_err(|err| {
                RemoteScannerServerError::retry_bucket(format!(
                    "remote namespace scanner checkpoint identity unavailable during admission: {err}"
                ))
            })?,
            checkpoint_plan_digest,
        },
    );
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
            if let Some(proof) = usage_watch.as_deref()
                && !proof.verify_with_store(&store).await
            {
                return Err(RemoteScannerServerError::retry_bucket(
                    "remote namespace scanner usage watch changed before cache reuse",
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
    crate::scanner_io::bind_usage_watch_to_checkpoint(
        &mut cache,
        usage_watch.as_deref(),
        checkpoint_plan_digest.unwrap_or(scan_plan_digest),
    );
    let scan_position_before = cache.durable_scan_progress();
    let resumed_sweep = cache.has_scan_position();

    let set_disks = scanner_set_disk_inventory(set.as_ref()).await;
    let (checkpoint_tx, mut checkpoint_rx) = mpsc::channel::<DataUsageCache>(1);
    let (node_tx, mut node_rx) = crate::workload_admission::scanner_node_request_channel();
    let scan_ctx = ctx.child_token();
    #[cfg(feature = "e2e-test-hooks")]
    let _ = crate::e2e_barrier::checkpoint(
        "bucket_walk",
        serde_json::json!({
            "epoch": leader_epoch, "cycle": next_cycle,
            "pool": source.pool_index, "set": source.set_index,
            "bucket": bucket, "drive": disk.path().display().to_string(),
        }),
    )
    .await;
    let scan = crate::workload_admission::with_scanner_node_requests(
        node_tx,
        ScannerIODisk::nsscanner_disk(
            disk.clone(),
            scan_ctx.clone(),
            budget,
            set_disks,
            cache,
            None,
            ScannerDiskScanOptions {
                scan_mode,
                prefix_scan_scope: None,
                checkpoint_tx: Some(checkpoint_tx),
            },
        ),
    );
    tokio::pin!(scan);
    let fence_watch = watch_remote_scanner_request_fence(next_cycle, leader_epoch, store.clone(), NS_SCANNER_FENCE_POLL_INTERVAL);
    tokio::pin!(fence_watch);
    let mut lock_watch = tokio::time::interval(NS_SCANNER_LOCK_POLL_INTERVAL);
    lock_watch.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut checkpoint_channel_closed = false;
    let outcome = loop {
        tokio::select! {
            request = async {
                match node_rx.as_mut() {
                    Some(receiver) => receiver.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                let Some(request) = request else {
                    node_rx = None;
                    continue;
                };
                if let Err(error) = wait_for_remote_node_resumption(
                    guard.resume_node_operation(set.clone(), &cache_name, source, &revisions, request),
                    fence_watch.as_mut(),
                ).await {
                    scan_ctx.cancel();
                    let _ = tokio::time::timeout(NS_SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, scan.as_mut()).await;
                    return Err(RemoteScannerServerError::retry_bucket(format!(
                        "remote namespace scanner could not resume node admission: {error}",
                    )));
                }
            }
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
            checkpoint = checkpoint_rx.recv(), if !checkpoint_channel_closed => {
                let Some(checkpoint) = checkpoint else {
                    checkpoint_channel_closed = true;
                    continue;
                };
                if guard.is_lock_lost() {
                    scan_ctx.cancel();
                    let _ = tokio::time::timeout(NS_SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, scan.as_mut()).await;
                    return Err(RemoteScannerServerError::worker(
                        "remote namespace scanner cache lock was lost before checkpoint save",
                    ));
                }
                match persist_scanner_checkpoint(
                    set.clone(),
                    store.clone(),
                    ScannerCheckpointPersistContext {
                        ctx: &scan_ctx,
                        expected_publication_epoch,
                        cycle: next_cycle,
                        leader_epoch,
                    },
                    &cache_name,
                    &checkpoint,
                    &mut revisions,
                )
                .await
                {
                    ScannerCheckpointPersistResult::Saved | ScannerCheckpointPersistResult::Skipped => {
                        if guard.is_lock_lost() {
                            scan_ctx.cancel();
                            let _ = tokio::time::timeout(NS_SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, scan.as_mut()).await;
                            return Err(RemoteScannerServerError::retry_bucket(
                                "remote namespace scanner cache lock was lost after checkpoint save",
                            ));
                        }
                    }
                    ScannerCheckpointPersistResult::FenceChanged => {
                        scan_ctx.cancel();
                        let _ = tokio::time::timeout(NS_SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, scan.as_mut()).await;
                        return Err(RemoteScannerServerError::retry_bucket(
                            "remote namespace scanner cache fence changed during checkpoint save",
                        ));
                    }
                    ScannerCheckpointPersistResult::RetryBucket(error) => {
                        scan_ctx.cancel();
                        let _ = tokio::time::timeout(NS_SCANNER_LOCK_LOSS_SHUTDOWN_TIMEOUT, scan.as_mut()).await;
                        return Err(RemoteScannerServerError::retry_bucket(format!(
                            "remote namespace scanner checkpoint needs reload: {error}",
                        )));
                    }
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
                    "remote namespace scanner leader fence verification failed during bucket scan: {}",
                    err
                )));
            }
        }
    };
    let (cache, partial_result) = classify_remote_usage_watch_outcome(
        &store,
        outcome,
        usage_watch.as_deref(),
        scan_plan_digest,
        checkpoint_plan_digest,
        resumed_sweep,
    )
    .await;
    if let Some(expected) = cache.info.scan_identity
        && crate::scanner_io::scanner_bucket_checkpoint_identity(
            &set,
            &bucket,
            expected_publication_epoch,
            tier_registry_generation,
            scan_mode,
        )
        .await
        .map_err(|err| {
            RemoteScannerServerError::retry_bucket(format!(
                "remote namespace scanner checkpoint identity unavailable after scanning: {err}"
            ))
        })? != Some(expected)
    {
        return Err(RemoteScannerServerError::retry_bucket(
            "remote scanner checkpoint identity changed during scanning",
        ));
    }

    if guard.is_lock_lost() {
        return Err(RemoteScannerServerError::worker(
            "remote namespace scanner cache lock was lost before persistence",
        ));
    }
    phase.store(RemoteScannerPhase::Persisting as u8, Ordering::Release);
    phase_changed.notify_one();
    validate_remote_scanner_request_fence_with_store(next_cycle, leader_epoch, store.clone())
        .await
        .map_err(|err| {
            RemoteScannerServerError::worker(format!("remote namespace scanner leader fence verification failed: {err}"))
        })?;
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
        .map_err(|err| {
            RemoteScannerServerError::worker(format!("remote namespace scanner leader fence verification failed: {err}"))
        })?;
    if guard.is_lock_lost() {
        return Err(RemoteScannerServerError::worker(
            "remote namespace scanner cache lock was lost during persistence",
        ));
    }

    if let Some(partial_result) = partial_result {
        if matches!(partial_result, RemoteScannerFrameResult::Partial) {
            log_saved_partial_bucket_cache(&cache, &cache_name);
            if (supports_durable_progress || supports_complete_observation)
                && let (Some(identity), Some(checkpoint_plan_digest)) = (cache.info.scan_identity, checkpoint_plan_digest)
            {
                return Ok(RemoteScannerFrameResult::DurableProgress(Box::new(RemoteScannerDurableProgress {
                    source,
                    identity,
                    scan_plan_digest,
                    checkpoint_plan_digest,
                    leader_epoch,
                    next_cycle,
                    advanced: supports_durable_progress && cache.durable_scan_progress().advanced_from(&scan_position_before),
                    resource_deferred: cache.info.scan_resource_deferred,
                    observation: supports_complete_observation
                        .then(|| cache.completed_bucket_observation().cloned().map(Box::new))
                        .flatten(),
                })));
            }
        }
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
        plans,
        supports_durable_progress,
        supports_complete_observation,
        usage_watch,
        checkpoint_identity,
        tier_registry_generation,
        skip_healing,
        scan_mode,
    } = spec;
    let scan_plan_digest = plans.reuse;
    let checkpoint_plan_digest = plans.coverage;
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
        checkpoint_plan_digest: Some(checkpoint_plan_digest),
        supports_durable_progress,
        supports_complete_observation,
        usage_watch: usage_watch.cloned().map(Box::new),
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

    let disk_name = format!("{}{}", disk.host_name(), disk.path().display());
    let (_, close_path) = current_path_updater(&disk_name, bucket).await;
    let mut path_guard = CloseDiskGuard::new(close_path);
    let result = async {
        let rpc_deadline = Instant::now() + NS_SCANNER_MAX_RPC_LIFETIME;
        let open_deadline = (Instant::now() + NS_SCANNER_STALL_TIMEOUT).min(rpc_deadline);
        #[cfg(feature = "e2e-test-hooks")]
        if crate::e2e_barrier::worker_observation::enabled() {
            let location = disk.get_disk_location();
            // RemoteDisk::get_disk_id reads the same cached ID used by disk_ref; no RPC.
            let disk_uuid = crate::storage_api::owner::EcstoreDiskAPI::get_disk_id(disk)
                .await
                .ok()
                .flatten();
            crate::e2e_barrier::worker_observation::opening(serde_json::json!({
                "epoch": leader_epoch, "cycle": next_cycle, "request_id": request_id,
                "wire_session_id": session_id, "wire_session_sequence": session_sequence,
                "server_epoch": server_epoch, "bucket": bucket, "pool": expected_source.pool_index,
                "set": expected_source.set_index, "disk_index": location.disk_idx, "disk_uuid": disk_uuid,
                "worker_endpoint": crate::storage_api::owner::EcstoreDiskAPI::endpoint(disk).grid_host(),
                "worker_path": disk.path().display().to_string(),
                "reuse_plan_sha256": crate::e2e_barrier::hex(&scan_plan_digest.0),
                "coverage_plan_sha256": crate::e2e_barrier::hex(&checkpoint_plan_digest.0),
            }));
        }
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
                supports_durable_progress,
                supports_complete_observation,
                checkpoint_identity,
                checkpoint_plan_digest,
                leader_epoch,
                bucket,
                source: expected_source,
                next_cycle,
                scan_plan_digest,
                tier_registry_generation,
            },
            authenticator,
            rpc_deadline,
        )
        .await;
        finish_remote_scanner_stream(stream_result, budget.as_ref())
    }
    .await;
    path_guard.close().await;
    result
}

/// Resource waits must keep checking the persisted leader fence.
async fn wait_for_remote_node_resumption<F, G>(resume: F, fence_watch: Pin<&mut G>) -> Result<(), StorageError>
where
    F: Future<Output = Result<(), StorageError>>,
    G: Future<Output = Result<(), ScannerError>>,
{
    tokio::select! {
        biased;
        result = fence_watch => Err(match result {
            Ok(()) => StorageError::other("remote namespace scanner fence watcher stopped unexpectedly"),
            Err(error) => StorageError::other(error),
        }),
        result = resume => result,
    }
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
pub(crate) async fn checkpoint_fixture_partial_return(progress: (u64, u64), entries_visited: u64) {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let mut bytes = Vec::new();
    write_frame(
        &mut bytes,
        &writer_auth,
        &mut 0,
        &RemoteScannerFrame::terminal(
            RemoteScannerProgress {
                objects_scanned: progress.0,
                directories_started: progress.1,
                entries_visited,
            },
            RemoteScannerFrameResult::Partial,
        ),
    )
    .await
    .expect("checkpoint partial frame must encode");
    let frame = read_frame(&mut std::io::Cursor::new(bytes.as_slice()), &reader_auth, &mut 0, None)
        .await
        .expect("checkpoint progress frame must authenticate");
    assert_eq!(frame.progress.entries_visited, entries_visited);
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, Default::default());
    let result = consume_remote_scanner_stream(
        std::io::Cursor::new(bytes),
        parent,
        budget.clone(),
        "bucket",
        DataUsageCacheSource::new(0, 0),
        DataUsageScanPlanDigest([17; 32]),
        reader_auth,
    )
    .await
    .expect("checkpoint partial frame must decode");
    assert!(matches!(result, RemoteScannerOutcome::Partial));
    assert_eq!(budget.progress(), progress);
}

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
            supports_durable_progress: false,
            supports_complete_observation: false,
            checkpoint_identity: None,
            checkpoint_plan_digest: expected_scan_plan_digest,
            leader_epoch: 0,
            bucket: expected_bucket,
            source: expected_source,
            next_cycle: TEST_NEXT_CYCLE,
            scan_plan_digest: expected_scan_plan_digest,
            tier_registry_generation: 0,
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
    // Namespace consumption is background work even in a plain spawned worker.
    // Capture only its installed local memory policy, never scheduler receipts.
    let memory = EcstoreBackgroundMemoryBudget::current();
    let mut expected_sequence = 0_u64;
    let mut last_progress = RemoteScannerProgress::default();
    let mut last_phase = RemoteScannerPhase::Scanning;
    let mut semantic_progress_deadline =
        bounded_remote_scanner_deadline(Instant::now(), NS_SCANNER_SEMANTIC_STALL_TIMEOUT, rpc_deadline);

    let negotiated = expected.supports_durable_progress || expected.supports_complete_observation;
    loop {
        let frame = await_remote_scanner_read(
            read_frame(&mut reader, &authenticator, &mut expected_sequence, memory.as_ref()),
            &ctx,
            &budget,
            negotiated,
            last_phase,
            semantic_progress_deadline,
            rpc_deadline,
        )
        .await?;

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

        let terminal = match frame.result {
            RemoteScannerFrameResult::Progress => {
                if budget.budget_elapsed()
                    && frame.phase == RemoteScannerPhase::Scanning
                    && !(expected.supports_durable_progress || expected.supports_complete_observation)
                {
                    return Ok(RemoteScannerOutcome::Partial);
                }
                None
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
                if complete.usage.tier_registry_generation != Some(expected.tier_registry_generation) {
                    return Err(RemoteScannerStreamError::reconciled(StorageError::other(
                        "remote namespace scanner returned usage for a different tier registry generation",
                    )));
                }
                if !complete.usage.entry.children.is_empty() {
                    return Err(RemoteScannerStreamError::reconciled(StorageError::other(
                        "remote namespace scanner returned non-flattened bucket usage",
                    )));
                }
                if budget.budget_elapsed() && !(expected.supports_durable_progress || expected.supports_complete_observation) {
                    Some(Ok(RemoteScannerOutcome::Partial))
                } else {
                    Some(Ok(RemoteScannerOutcome::Complete {
                        usage: Box::new(complete.usage),
                        pending_maintenance_work: complete.pending_maintenance_work,
                    }))
                }
            }
            RemoteScannerFrameResult::DurableProgress(receipt) => {
                if !(expected.supports_durable_progress || expected.supports_complete_observation)
                    || (!expected.supports_durable_progress && receipt.advanced)
                    || (!expected.supports_complete_observation && receipt.observation.is_some())
                    || frame.phase != RemoteScannerPhase::Persisting
                    || receipt.source != expected.source
                    || Some(receipt.identity) != expected.checkpoint_identity
                    || receipt.scan_plan_digest != expected.scan_plan_digest
                    || receipt.checkpoint_plan_digest != expected.checkpoint_plan_digest
                    || receipt.leader_epoch != expected.leader_epoch
                    || receipt.next_cycle != expected.next_cycle
                    || receipt.identity.tier_registry_generation != expected.tier_registry_generation
                    || !receipt.identity.is_valid()
                    || receipt.observation.as_ref().is_some_and(|observation| {
                        !observation.matches(expected.source, receipt.identity)
                            || (observation.completed_epoch, observation.completed_cycle)
                                > (expected.leader_epoch, expected.next_cycle)
                    })
                {
                    return Err(RemoteScannerStreamError::reconciled(StorageError::other(
                        "remote namespace scanner returned an unnegotiated or mismatched durable progress receipt",
                    )));
                }
                Some(Ok(RemoteScannerOutcome::DurableProgress {
                    advanced: receipt.advanced,
                    resource_deferred: receipt.resource_deferred,
                    observation: receipt.observation,
                }))
            }
            RemoteScannerFrameResult::Partial => Some(Ok(RemoteScannerOutcome::Partial)),
            RemoteScannerFrameResult::NamespaceNotFound => Some(Ok(RemoteScannerOutcome::NamespaceNotFound)),
            RemoteScannerFrameResult::CycleAhead { required_cycle } => {
                if required_cycle <= expected.next_cycle || required_cycle == u64::MAX {
                    return Err(RemoteScannerStreamError::reconciled(StorageError::other(
                        "remote namespace scanner returned an invalid required cycle",
                    )));
                }
                Some(Ok(RemoteScannerOutcome::CycleAhead(required_cycle)))
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
                Some(Err(match error_frame.scope {
                    RemoteScannerErrorScope::Bucket if retry_bucket => RemoteScannerStreamError::retry_bucket(error),
                    RemoteScannerErrorScope::Bucket => RemoteScannerStreamError::bucket(error),
                    RemoteScannerErrorScope::Worker => RemoteScannerStreamError::reconciled(error),
                }))
            }
        };

        if let Some(result) = terminal {
            let mut trailing = [0_u8; 1];
            let eof = await_remote_scanner_read(
                reader.read(&mut trailing),
                &ctx,
                &budget,
                negotiated,
                last_phase,
                semantic_progress_deadline,
                rpc_deadline,
            )
            .await?;
            if eof != 0 {
                return Err(RemoteScannerStreamError::for_phase(
                    StorageError::other("remote namespace scanner returned trailing bytes after terminal frame"),
                    last_phase,
                ));
            }
            #[cfg(feature = "e2e-test-hooks")]
            if matches!(&result, Ok(RemoteScannerOutcome::Complete { .. })) && crate::e2e_barrier::worker_observation::enabled() {
                crate::e2e_barrier::worker_observation::complete(
                    authenticator.request_id,
                    [
                        last_progress.objects_scanned,
                        last_progress.directories_started,
                        last_progress.entries_visited,
                    ],
                );
            }
            return result;
        }
    }
}

/// Keep the in-flight decoder alive when a budget stop shortens its deadline.
/// Restarting `read_frame` after a partially consumed frame would corrupt framing.
async fn await_remote_scanner_read<T, F>(
    read: F,
    owner: &CancellationToken,
    budget: &ScannerCycleBudget,
    negotiated: bool,
    phase: RemoteScannerPhase,
    semantic_deadline: Instant,
    rpc_deadline: Instant,
) -> RemoteScannerStreamResult<T>
where
    F: Future<Output = std::io::Result<T>>,
{
    tokio::pin!(read);
    loop {
        let drain = (negotiated && budget.budget_elapsed()).then(|| budget.shutdown_deadline());
        let base = semantic_deadline.min(rpc_deadline);
        let deadline = drain.map_or(base, |drain| drain.min(base));
        tokio::select! {
            biased;
            _ = owner.cancelled(), if negotiated || budget.reason().is_none() => {
                return Err(RemoteScannerStreamError::for_phase(StorageError::other("remote namespace scanner cancelled"), phase));
            }
            _ = budget.wait_for_stop(), if negotiated && drain.is_none() => continue,
            result = tokio::time::timeout_at(deadline, read.as_mut()) => return match result {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(error)) => Err(RemoteScannerStreamError::for_phase(StorageError::other(error), phase)),
                Err(_) => {
                    let message = if drain.is_some_and(|drain| drain <= base) {
                        "remote namespace scanner budget drain deadline exceeded"
                    } else if rpc_deadline <= semantic_deadline {
                        "remote namespace scanner RPC lifetime exceeded"
                    } else {
                        "remote namespace scanner made no semantic progress"
                    };
                    Err(RemoteScannerStreamError::for_phase(StorageError::other(message), phase))
                }
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

#[cfg(test)]
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
    write_frame_bounded_with_memory(writer, authenticator, sequence, frame, disconnect, timeout_duration, None).await
}

async fn write_frame_bounded_with_memory<W>(
    writer: &mut W,
    authenticator: &FrameAuthenticator,
    sequence: &mut u64,
    frame: &RemoteScannerFrame,
    disconnect: &CancellationToken,
    timeout_duration: Duration,
    memory: Option<&Arc<EcstoreBackgroundMemoryBudget>>,
) -> Result<(), ScannerError>
where
    W: AsyncWrite + Unpin,
{
    tokio::select! {
        biased;
        _ = disconnect.cancelled() => Err(disconnected_writer_error()),
        result = tokio::time::timeout(timeout_duration, async {
            match memory {
                Some(memory) => write_background_frame(writer, authenticator, sequence, frame, memory).await,
                None => write_frame(writer, authenticator, sequence, frame).await,
            }
        }) => {
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

#[derive(Serialize)]
struct BorrowedRemoteScannerFrameEnvelope<'a> {
    version: u16,
    sequence: u64,
    payload: &'a [u8],
    mac: &'a [u8],
}

fn encode_background<T: Serialize>(value: &T, memory: &Arc<EcstoreBackgroundMemoryBudget>) -> Result<bytes::Bytes, ScannerError> {
    let mut buffer = EcstoreBackgroundEncodeBuffer::new(memory.clone(), NS_SCANNER_MAX_FRAME_SIZE);
    // Preserve the serializer's original I/O error and its memory-denial cause.
    rmp_serde::encode::write_named(&mut buffer, value).map_err(|error| match error {
        rmp_serde::encode::Error::InvalidValueWrite(error) => ScannerError::Io(error.into()),
        error => ScannerError::Io(IoError::other(error)),
    })?;
    Ok(buffer.into_bytes())
}

async fn write_background_frame<W>(
    writer: &mut W,
    authenticator: &FrameAuthenticator,
    sequence: &mut u64,
    frame: &RemoteScannerFrame,
    memory: &Arc<EcstoreBackgroundMemoryBudget>,
) -> Result<(), ScannerError>
where
    W: AsyncWrite + Unpin,
{
    let payload = encode_background(frame, memory)?;
    let mut mac = HmacSha256::new_from_slice(authenticator.secret.as_bytes())
        .map_err(|_| ScannerError::Other("invalid remote namespace scanner authentication key".to_string()))?;
    update_frame_mac(&mut mac, authenticator.request_id, *sequence, &payload);
    let signature = mac.finalize().into_bytes();
    let envelope = BorrowedRemoteScannerFrameEnvelope {
        version: NS_SCANNER_PROTOCOL_VERSION,
        sequence: *sequence,
        payload: &payload,
        mac: &signature,
    };
    let encoded = encode_background(&envelope, memory)?;
    if encoded.is_empty() {
        return Err(ScannerError::Other("remote namespace scanner encoded frame is empty".to_string()));
    }
    let len = u32::try_from(encoded.len())
        .map_err(|_| ScannerError::Other("remote namespace scanner frame is too large".to_string()))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&encoded).await?;
    drop(encoded);
    drop(payload);
    *sequence = sequence
        .checked_add(1)
        .ok_or_else(|| ScannerError::Other("remote namespace scanner frame sequence overflow".to_string()))?;
    Ok(())
}

// These owners account only envelope payload and MAC storage, not the decoded model.
struct BackgroundRemoteScannerEnvelope {
    version: u16,
    sequence: u64,
    payload: bytes::Bytes,
    mac: bytes::Bytes,
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum RemoteScannerEnvelopeField {
    Version,
    Sequence,
    Payload,
    Mac,
}

struct BackgroundEnvelopeSeed<'a> {
    memory: &'a Arc<EcstoreBackgroundMemoryBudget>,
    maximum: usize,
    failure: &'a mut Option<IoError>,
}

impl BackgroundEnvelopeSeed<'_> {
    fn bytes(&mut self) -> BackgroundEnvelopeBytesSeed<'_> {
        BackgroundEnvelopeBytesSeed {
            buffer: EcstoreBackgroundEncodeBuffer::new(self.memory.clone(), self.maximum),
            failure: self.failure,
        }
    }
}

impl<'de> serde::de::DeserializeSeed<'de> for BackgroundEnvelopeSeed<'_> {
    type Value = BackgroundRemoteScannerEnvelope;
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_struct("RemoteScannerFrameEnvelope", &["version", "sequence", "payload", "mac"], self)
    }
}

impl<'de> serde::de::Visitor<'de> for BackgroundEnvelopeSeed<'_> {
    type Value = BackgroundRemoteScannerEnvelope;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("remote namespace scanner frame envelope")
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(mut self, mut seq: A) -> Result<Self::Value, A::Error> {
        use serde::de::Error;
        let version = seq.next_element()?.ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let sequence = seq.next_element()?.ok_or_else(|| A::Error::invalid_length(1, &self))?;
        let payload = seq
            .next_element_seed(self.bytes())?
            .ok_or_else(|| A::Error::invalid_length(2, &self))?;
        let mac = seq
            .next_element_seed(self.bytes())?
            .ok_or_else(|| A::Error::invalid_length(3, &self))?;
        Ok(BackgroundRemoteScannerEnvelope {
            version,
            sequence,
            payload,
            mac,
        })
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(mut self, mut map: A) -> Result<Self::Value, A::Error> {
        use serde::de::Error;
        let (mut version, mut sequence, mut payload, mut mac) = (None, None, None, None);
        while let Some(field) = map.next_key()? {
            match field {
                RemoteScannerEnvelopeField::Version => {
                    if version.is_some() {
                        return Err(A::Error::duplicate_field("version"));
                    }
                    version = Some(map.next_value()?);
                }
                RemoteScannerEnvelopeField::Sequence => {
                    if sequence.is_some() {
                        return Err(A::Error::duplicate_field("sequence"));
                    }
                    sequence = Some(map.next_value()?);
                }
                RemoteScannerEnvelopeField::Payload => {
                    if payload.is_some() {
                        return Err(A::Error::duplicate_field("payload"));
                    }
                    payload = Some(map.next_value_seed(self.bytes())?);
                }
                RemoteScannerEnvelopeField::Mac => {
                    if mac.is_some() {
                        return Err(A::Error::duplicate_field("mac"));
                    }
                    mac = Some(map.next_value_seed(self.bytes())?);
                }
            }
        }
        Ok(BackgroundRemoteScannerEnvelope {
            version: version.ok_or_else(|| A::Error::missing_field("version"))?,
            sequence: sequence.ok_or_else(|| A::Error::missing_field("sequence"))?,
            payload: payload.ok_or_else(|| A::Error::missing_field("payload"))?,
            mac: mac.ok_or_else(|| A::Error::missing_field("mac"))?,
        })
    }
}

struct BackgroundEnvelopeBytesSeed<'a> {
    buffer: EcstoreBackgroundEncodeBuffer,
    failure: &'a mut Option<IoError>,
}

impl<'de> serde::de::DeserializeSeed<'de> for BackgroundEnvelopeBytesSeed<'_> {
    type Value = bytes::Bytes;
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        // deserialize_seq matches Vec<u8>, including MessagePack bin input.
        deserializer.deserialize_seq(self)
    }
}

impl<'de> serde::de::Visitor<'de> for BackgroundEnvelopeBytesSeed<'_> {
    type Value = bytes::Bytes;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a sequence of bytes")
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(mut self, mut seq: A) -> Result<Self::Value, A::Error> {
        use serde::de::Error;
        // Never allocate from an untrusted sequence size_hint.
        while let Some(byte) = seq.next_element::<u8>()? {
            if let Err(error) = std::io::Write::write_all(&mut self.buffer, &[byte]) {
                *self.failure = Some(error);
                return Err(A::Error::custom("background envelope byte storage unavailable"));
            }
        }
        Ok(self.buffer.into_bytes())
    }
}

fn decode_background_envelope(
    encoded: &[u8],
    memory: &Arc<EcstoreBackgroundMemoryBudget>,
) -> std::io::Result<BackgroundRemoteScannerEnvelope> {
    use serde::de::DeserializeSeed;
    let mut failure = None;
    let seed = BackgroundEnvelopeSeed {
        memory,
        maximum: encoded.len(),
        failure: &mut failure,
    };
    seed.deserialize(&mut rmp_serde::Deserializer::from_read_ref(encoded))
        .map_err(|_| {
            failure.unwrap_or_else(|| IoError::new(ErrorKind::InvalidData, "invalid remote namespace scanner frame envelope"))
        })
}

async fn read_frame<R>(
    reader: &mut R,
    authenticator: &FrameAuthenticator,
    expected_sequence: &mut u64,
    memory: Option<&Arc<EcstoreBackgroundMemoryBudget>>,
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

    let mut background = memory.map(|memory| EcstoreBackgroundEncodeBuffer::new(memory.clone(), len));
    let mut foreground;
    let encoded = match background.as_mut() {
        Some(buffer) => buffer.prepare_read(len)?,
        None => {
            foreground = vec![0_u8; len];
            foreground.as_mut_slice()
        }
    };
    reader.read_exact(encoded).await?;
    let background_envelope;
    let foreground_envelope;
    let (version, sequence, payload, mac) = match memory {
        Some(memory) => {
            background_envelope = decode_background_envelope(encoded, memory)?;
            (
                background_envelope.version,
                background_envelope.sequence,
                background_envelope.payload.as_ref(),
                background_envelope.mac.as_ref(),
            )
        }
        None => {
            foreground_envelope = rmp_serde::from_slice::<RemoteScannerFrameEnvelope>(encoded)
                .map_err(|_| IoError::new(ErrorKind::InvalidData, "invalid remote namespace scanner frame envelope"))?;
            (
                foreground_envelope.version,
                foreground_envelope.sequence,
                foreground_envelope.payload.as_slice(),
                foreground_envelope.mac.as_slice(),
            )
        }
    };
    if version != NS_SCANNER_PROTOCOL_VERSION {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("unsupported remote namespace scanner frame version: {}", version),
        ));
    }
    if sequence != *expected_sequence {
        return Err(IoError::new(ErrorKind::InvalidData, "remote namespace scanner frame sequence is invalid"));
    }
    authenticator.verify(sequence, payload, mac)?;
    let frame: RemoteScannerFrame = rmp_serde::from_slice(payload)
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
