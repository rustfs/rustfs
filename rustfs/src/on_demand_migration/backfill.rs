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

//! On-demand migration backfill job (ODM-12, rustfs/backlog#2159).
//!
//! Read-through only migrates objects somebody asked for; the backfill job
//! walks the source bucket with `ListObjectsV2` and queues every remaining
//! key through the write-back pipeline (`pull.rs`) with
//! [`PullReason::Backfill`]. One job per bucket, one runner per job:
//!
//! - The checkpoint `buckets/<bucket>/on-demand-migration-backfill.json`
//!   under the metadata bucket is the durable job record: cursor, counters,
//!   owner and lease. It is saved every [`BACKFILL_SAVE_EVERY_KEYS`] keys or
//!   [`BACKFILL_SAVE_INTERVAL`], and at every page end, with an `If-Match`
//!   compare-and-set so a concurrent cancel or takeover is never overwritten.
//! - The `continuation_token` only advances once every pull queued from the
//!   page before it has succeeded. After a failure it stays at that page,
//!   so crash recovery cannot skip failed pulls (existing keys are skipped).
//! - The owner holds a lease of [`BACKFILL_LEASE`] renewed by every save. The
//!   recovery loop ([`run_backfill_recovery_loop`]) scans the buckets this
//!   node has an ODM state for every [`BACKFILL_RECOVERY_INTERVAL`] and takes
//!   over a running job whose lease expired (or whose owner is this very node,
//!   which after a restart cannot still be running it). Start and takeover
//!   are serialized cluster-wide by the namespace lock `odm-backfill/<bucket>`.
//! - A config change or removal fires the bucket state's cancellation token;
//!   the job records `cancelled` and keeps the checkpoint for inspection. A
//!   takeover compares `config_updated_at` for the same reason.
//! - Backfill pulls take permits after online misses: [`PriorityPullPermits`]
//!   never hands a permit to a backfill waiter while an online request waits,
//!   and the job keeps at most `2 * max_concurrent_pulls` pulls outstanding.
//!
//! Failed keys are recorded as hashes only; object keys appear in logs at
//! `trace` and nowhere else.

use super::pull::{EnqueueOutcome, PullReason, QueuedPullOutcome};
use super::source_client::{SourceError, SourcePage};
use super::storage_api::{
    BUCKET_META_PREFIX, ECStore, HTTPPreconditions, NamespaceLocking as _, ObjectOperations as _, ObjectOptions,
    RUSTFS_META_BUCKET, StorageError, WriteCompletion, get_lock_acquire_timeout, get_on_demand_migration_config_in,
    local_node_name, read_config_with_metadata, save_config_with_opts,
};
use super::sys::{BucketOdmState, OnDemandMigrationSys};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use rustfs_utils::http::metadata_compat::{SUFFIX_ODM_SOURCE_ETAG, has_internal_suffix};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, oneshot, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

const EVENT_ODM_BACKFILL_STATE: &str = "odm_backfill_state";
const EVENT_ODM_BACKFILL_LEASE_TAKEOVER: &str = "odm_backfill_lease_takeover";
const EVENT_ODM_BACKFILL_CHECKPOINT: &str = "odm_backfill_checkpoint";
const EVENT_ODM_BACKFILL_RECOVERY: &str = "odm_backfill_recovery";
const EVENT_ODM_BACKFILL_KEY: &str = "odm_backfill_key";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_ON_DEMAND_MIGRATION: &str = "on_demand_migration";

/// Checkpoint file name under `buckets/<bucket>/` in the metadata bucket.
pub const BACKFILL_CHECKPOINT_FILE: &str = "on-demand-migration-backfill.json";
/// Checkpoint document version this build reads and writes.
pub const BACKFILL_CHECKPOINT_FORMAT_VERSION: u32 = 1;
/// Namespace lock prefix serializing start and takeover per bucket.
pub const BACKFILL_LEASE_LOCK_PREFIX: &str = "odm-backfill/";
/// Owner lease length; every checkpoint save renews it.
pub const BACKFILL_LEASE: Duration = Duration::from_secs(60);
/// Longest gap between two checkpoint saves of a running job.
pub const BACKFILL_SAVE_INTERVAL: Duration = Duration::from_secs(10);
/// Keys processed between two checkpoint saves.
pub const BACKFILL_SAVE_EVERY_KEYS: u64 = 1000;
/// Interval of the recovery scan.
pub const BACKFILL_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);
/// Retry interval of the recovery scan while a takeover had to be deferred
/// (the bucket state was not built yet, or the lock was busy).
pub const BACKFILL_RECOVERY_RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// `max-keys` of every source listing.
pub const BACKFILL_LIST_PAGE_SIZE: i32 = 1000;
/// Ring capacity of `failed_keys`.
pub const BACKFILL_FAILED_KEYS_CAPACITY: usize = 1000;
/// Retries after the first attempt of a retryable listing failure.
const LIST_MAX_RETRIES: usize = 3;
const LIST_RETRY_BASE_DELAYS: [Duration; LIST_MAX_RETRIES] =
    [Duration::from_secs(1), Duration::from_secs(4), Duration::from_secs(16)];
/// Pause between polls while the pull queue is full and nothing is
/// outstanding, or while the breaker rejects source traffic.
const BACKFILL_IDLE_POLL: Duration = Duration::from_millis(200);
/// Lock wait of a takeover attempt; a busy lock defers to the next scan.
const TAKEOVER_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
/// How long an admin cancel waits for the local job to write its final state.
const CANCEL_SETTLE_TIMEOUT: Duration = Duration::from_secs(5);

/// Lifecycle of a backfill job as persisted in the checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillState {
    Pending,
    Running,
    Paused,
    Cancelled,
    Completed,
    CompletedWithFailures,
    Failed,
}

impl BackfillState {
    pub fn as_str(self) -> &'static str {
        match self {
            BackfillState::Pending => "pending",
            BackfillState::Running => "running",
            BackfillState::Paused => "paused",
            BackfillState::Cancelled => "cancelled",
            BackfillState::Completed => "completed",
            BackfillState::CompletedWithFailures => "completed_with_failures",
            BackfillState::Failed => "failed",
        }
    }

    /// Whether a runner owns (or should own) the job.
    pub fn is_active(self) -> bool {
        matches!(self, BackfillState::Pending | BackfillState::Running)
    }
}

impl fmt::Display for BackfillState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What to do with a listed key that already exists locally.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipExisting {
    /// A current local version means the key is done.
    #[default]
    Always,
    /// Re-pull when the recorded source ETag or the size differs.
    EtagOrSize,
}

impl SkipExisting {
    pub fn as_str(self) -> &'static str {
        match self {
            SkipExisting::Always => "always",
            SkipExisting::EtagOrSize => "etag_or_size",
        }
    }

    pub fn parse(label: &str) -> Option<Self> {
        match label {
            "always" => Some(SkipExisting::Always),
            "etag_or_size" => Some(SkipExisting::EtagOrSize),
            _ => None,
        }
    }
}

/// Last failure recorded by the job. `key_hash` is the xxh3 of the key,
/// never the key itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillLastError {
    pub class: String,
    #[serde(default)]
    pub key_hash: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
}

/// Node running the job and the lease it holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillOwner {
    pub node: String,
    #[serde(with = "time::serde::rfc3339")]
    pub lease_until: OffsetDateTime,
}

/// Durable job record (see the module docs). Unknown fields are kept and
/// written back so a newer build's fields survive an older node's save.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillCheckpoint {
    pub format_version: u32,
    pub job_id: Uuid,
    pub state: BackfillState,
    /// `updated_at` of the ODM config the job was started for; a different
    /// value at takeover cancels the job.
    #[serde(with = "time::serde::rfc3339")]
    pub config_updated_at: OffsetDateTime,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub skip_existing: SkipExisting,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub continuation_token: Option<String>,
    #[serde(default)]
    pub listed: u64,
    #[serde(default)]
    pub enqueued: u64,
    #[serde(default)]
    pub pulled: u64,
    #[serde(default)]
    pub skipped_existing: u64,
    #[serde(default)]
    pub failed: u64,
    #[serde(default)]
    pub bytes: u64,
    #[serde(default)]
    pub last_key: Option<String>,
    #[serde(default)]
    pub last_error: Option<BackfillLastError>,
    /// Ring of at most [`BACKFILL_FAILED_KEYS_CAPACITY`] key hashes.
    #[serde(default)]
    pub failed_keys: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    #[serde(default)]
    pub owner: Option<BackfillOwner>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl BackfillCheckpoint {
    /// Decode a persisted checkpoint; a foreign `format_version` is a typed
    /// error rather than a best-effort read.
    pub fn from_json(bytes: &[u8]) -> Result<Self, BackfillError> {
        #[derive(Deserialize)]
        struct Header {
            format_version: u32,
        }
        let header: Header = serde_json::from_slice(bytes).map_err(|err| BackfillError::Malformed(err.to_string()))?;
        if header.format_version != BACKFILL_CHECKPOINT_FORMAT_VERSION {
            return Err(BackfillError::UnsupportedFormatVersion {
                found: header.format_version,
                supported: BACKFILL_CHECKPOINT_FORMAT_VERSION,
            });
        }
        serde_json::from_slice(bytes).map_err(|err| BackfillError::Malformed(err.to_string()))
    }

    pub fn to_json(&self) -> Result<Vec<u8>, BackfillError> {
        serde_json::to_vec(self).map_err(|err| BackfillError::Malformed(err.to_string()))
    }

    /// Whether the owner's lease still excludes a takeover at `now`.
    pub fn lease_valid_at(&self, now: OffsetDateTime) -> bool {
        self.owner.as_ref().is_some_and(|owner| owner.lease_until > now)
    }

    fn new(request: &BackfillRequest, config_updated_at: OffsetDateTime, node: &str, now: OffsetDateTime) -> Self {
        Self {
            format_version: BACKFILL_CHECKPOINT_FORMAT_VERSION,
            job_id: Uuid::new_v4(),
            state: BackfillState::Running,
            config_updated_at,
            prefix: request.prefix.clone(),
            skip_existing: request.skip_existing,
            dry_run: request.dry_run,
            continuation_token: None,
            listed: 0,
            enqueued: 0,
            pulled: 0,
            skipped_existing: 0,
            failed: 0,
            bytes: 0,
            last_key: None,
            last_error: None,
            failed_keys: Vec::new(),
            started_at: now,
            updated_at: now,
            owner: Some(BackfillOwner {
                node: node.to_string(),
                lease_until: now + BACKFILL_LEASE,
            }),
            extra: BTreeMap::new(),
        }
    }

    fn record_failure(&mut self, class: &str, key: Option<&str>, now: OffsetDateTime) {
        let key_hash = key.map(key_hash);
        if let Some(hash) = &key_hash {
            if self.failed_keys.len() >= BACKFILL_FAILED_KEYS_CAPACITY {
                self.failed_keys.remove(0);
            }
            self.failed_keys.push(hash.clone());
        }
        self.last_error = Some(BackfillLastError {
            class: class.to_string(),
            key_hash,
            at: now,
        });
    }
}

/// Stable, non-reversible identifier of a key for checkpoints and logs.
pub fn key_hash(key: &str) -> String {
    format!("{:016x}", xxhash_rust::xxh3::xxh3_64(key.as_bytes()))
}

/// Admin `start` parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackfillRequest {
    pub prefix: Option<String>,
    pub skip_existing: SkipExisting,
    /// List and count only; nothing is queued.
    pub dry_run: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    #[error("backfill checkpoint is malformed: {0}")]
    Malformed(String),
    #[error("unsupported backfill checkpoint format version {found} (this build supports {supported})")]
    UnsupportedFormatVersion { found: u32, supported: u32 },
    #[error("a backfill job is already running for bucket {bucket} (job {job_id}, owner {owner})")]
    AlreadyRunning { bucket: String, job_id: Uuid, owner: String },
    #[error("no backfill job recorded for bucket {0}")]
    NotFound(String),
    #[error("bucket {0} has no usable on-demand migration state on this node")]
    Unavailable(String),
    #[error("bucket {0} has no on-demand migration config")]
    NotConfigured(String),
    #[error("backfill lease lock for bucket {0} is busy")]
    LeaseBusy(String),
    #[error("backfill checkpoint for bucket {0} changed concurrently")]
    Conflict(String),
    #[error("backfill runner is not installed")]
    RunnerNotInstalled,
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Current local version of a key as the skip policy sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalBackfillObject {
    pub size: u64,
    /// The `odm-source-etag` provenance value when present, else the local ETag.
    pub source_etag: Option<String>,
}

/// Shared report of a new or coalesced pull; absent only when not admitted.
pub type PullReport = Option<super::pull::QueuedPullReport>;

/// Everything the job needs from its bucket, so the loop can run against a
/// mock in unit tests. Production: [`BucketBackfillContext`].
#[async_trait]
pub trait BackfillContext: Send + Sync {
    /// The bucket incarnation captured by this context.
    fn incarnation_id(&self) -> Uuid;
    /// One source page in the local key namespace.
    async fn list_page(&self, prefix: Option<&str>, token: Option<&str>, max_keys: i32) -> Result<SourcePage, SourceError>;
    /// Whether the breaker admits source traffic right now.
    fn source_available(&self) -> bool;
    /// Current local version of `key`; `None` when absent or a delete marker.
    async fn local_object(&self, key: &str) -> Result<Option<LocalBackfillObject>, StorageError>;
    /// Queue a backfill pull of `key`.
    fn enqueue(&self, key: &str) -> (EnqueueOutcome, PullReport);
    /// Backfill pulls in flight or queued before the job waits for a report.
    /// Bounding this keeps the queue free for online misses and makes a
    /// cancel take effect within a few pulls.
    fn max_outstanding(&self) -> usize {
        16
    }
    /// Fires when the bucket's ODM state is replaced or removed.
    fn cancel_token(&self) -> CancellationToken;
    /// `updated_at` of the bucket's ODM config; `None` when unconfigured.
    async fn config_updated_at(&self) -> Result<Option<OffsetDateTime>, StorageError>;
}

/// Resolves the buckets a runner may run jobs for.
pub trait BackfillContextFactory: Send + Sync {
    fn context(&self, bucket: &str) -> Option<Arc<dyn BackfillContext>>;
    /// Buckets with a usable ODM state on this node, for the recovery scan.
    fn buckets(&self) -> Vec<String>;
}

/// [`BackfillContext`] over a live [`BucketOdmState`].
pub struct BucketBackfillContext {
    api: Arc<ECStore>,
    state: Arc<BucketOdmState>,
}

impl BucketBackfillContext {
    pub fn new(api: Arc<ECStore>, state: Arc<BucketOdmState>) -> Self {
        Self { api, state }
    }
}

#[async_trait]
impl BackfillContext for BucketBackfillContext {
    fn incarnation_id(&self) -> Uuid {
        self.state.incarnation_id()
    }

    async fn list_page(&self, prefix: Option<&str>, token: Option<&str>, max_keys: i32) -> Result<SourcePage, SourceError> {
        let client = self.state.client().map_err(|err| SourceError::Unsupported(err.to_string()))?;
        let started = Instant::now();
        let result = client.list_objects_v2(prefix, token, max_keys).await;
        // A 404 on a listing is the bucket, not a key: keep it out of the
        // negative cache but let the breaker see everything else.
        match &result {
            Err(SourceError::NotFound) => {}
            other => self.state.observe_source(started.elapsed(), "", other.as_ref().err()),
        }
        result
    }

    fn source_available(&self) -> bool {
        self.state.breaker().allow_request()
    }

    async fn local_object(&self, key: &str) -> Result<Option<LocalBackfillObject>, StorageError> {
        let info = match self
            .api
            .get_object_info(self.state.bucket(), key, &ObjectOptions::default())
            .await
        {
            Ok(info) => info,
            Err(err) if err.is_not_found() => return Ok(None),
            Err(err) => return Err(err),
        };
        if info.delete_marker {
            return Ok(None);
        }
        let source_etag = info
            .user_defined
            .iter()
            .find(|(name, _)| has_internal_suffix(name, SUFFIX_ODM_SOURCE_ETAG))
            .map(|(_, value)| value.clone())
            .filter(|value| !value.is_empty())
            .or_else(|| info.etag.clone());
        Ok(Some(LocalBackfillObject {
            size: u64::try_from(info.size).unwrap_or(0),
            source_etag,
        }))
    }

    fn enqueue(&self, key: &str) -> (EnqueueOutcome, PullReport) {
        self.state.enqueue_pull_with_report(key, PullReason::Backfill)
    }

    fn max_outstanding(&self) -> usize {
        usize::try_from(self.state.config().policy.max_concurrent_pulls)
            .unwrap_or(usize::MAX)
            .saturating_mul(2)
            .max(2)
    }

    fn cancel_token(&self) -> CancellationToken {
        self.state.cancel_token()
    }

    async fn config_updated_at(&self) -> Result<Option<OffsetDateTime>, StorageError> {
        Ok(
            super::config::decode_stored_config(get_on_demand_migration_config_in(&self.api, self.state.bucket()).await?)?
                .map(|(_, updated_at)| updated_at),
        )
    }
}

/// Factory over the process-wide [`OnDemandMigrationSys`].
pub struct SysBackfillContexts {
    api: Arc<ECStore>,
    sys: &'static OnDemandMigrationSys,
}

impl SysBackfillContexts {
    pub fn new(api: Arc<ECStore>, sys: &'static OnDemandMigrationSys) -> Self {
        Self { api, sys }
    }
}

impl BackfillContextFactory for SysBackfillContexts {
    fn context(&self, bucket: &str) -> Option<Arc<dyn BackfillContext>> {
        let state = self.sys.state(bucket)?;
        state.client().ok()?;
        Some(Arc::new(BucketBackfillContext::new(Arc::clone(&self.api), state)))
    }

    fn buckets(&self) -> Vec<String> {
        self.sys.bucket_names()
    }
}

/// Two-tier pull permits: online misses queue on the semaphore, backfill
/// waiters only try for a permit while no online request is waiting, and
/// re-check on every release or online arrival/departure.
pub struct PriorityPullPermits {
    semaphore: Arc<Semaphore>,
    online_waiters: AtomicUsize,
    /// Bumped on every permit release and every online waiter change.
    epoch: watch::Sender<u64>,
}

impl fmt::Debug for PriorityPullPermits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PriorityPullPermits")
            .field("available", &self.semaphore.available_permits())
            .field("online_waiters", &self.online_waiters.load(Ordering::Relaxed))
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PullPriority {
    /// A request is waiting on this pull.
    Online,
    /// Nobody waits; yield to online requests.
    Backfill,
}

/// A held pull permit; dropping it wakes backfill waiters.
pub struct PullPermit {
    inner: Option<OwnedSemaphorePermit>,
    permits: Arc<PriorityPullPermits>,
}

impl fmt::Debug for PullPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PullPermit").finish_non_exhaustive()
    }
}

impl Drop for PullPermit {
    fn drop(&mut self) {
        // Release before waking so a woken backfill waiter finds the permit.
        drop(self.inner.take());
        self.permits.bump();
    }
}

struct OnlineWaiterGuard<'a>(&'a PriorityPullPermits);

impl Drop for OnlineWaiterGuard<'_> {
    fn drop(&mut self) {
        self.0.online_waiters.fetch_sub(1, Ordering::AcqRel);
        self.0.bump();
    }
}

impl PriorityPullPermits {
    pub fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(permits.max(1))),
            online_waiters: AtomicUsize::new(0),
            epoch: watch::channel(0).0,
        })
    }

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub fn online_waiters(&self) -> usize {
        self.online_waiters.load(Ordering::Acquire)
    }

    fn bump(&self) {
        self.epoch.send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }

    /// Resolves with a permit; `Err` only when the semaphore is closed.
    pub async fn acquire(self: &Arc<Self>, priority: PullPriority) -> Result<PullPermit, tokio::sync::AcquireError> {
        match priority {
            PullPriority::Online => {
                self.online_waiters.fetch_add(1, Ordering::AcqRel);
                self.bump();
                let _waiting = OnlineWaiterGuard(self);
                let inner = Arc::clone(&self.semaphore).acquire_owned().await?;
                Ok(PullPermit {
                    inner: Some(inner),
                    permits: Arc::clone(self),
                })
            }
            PullPriority::Backfill => {
                let mut epoch = self.epoch.subscribe();
                loop {
                    epoch.borrow_and_update();
                    if self.online_waiters.load(Ordering::Acquire) == 0 {
                        match Arc::clone(&self.semaphore).try_acquire_owned() {
                            Ok(inner) => {
                                return Ok(PullPermit {
                                    inner: Some(inner),
                                    permits: Arc::clone(self),
                                });
                            }
                            Err(TryAcquireError::Closed) => {
                                // Surface the same error an online waiter gets.
                                return Arc::clone(&self.semaphore).acquire_owned().await.map(|inner| PullPermit {
                                    inner: Some(inner),
                                    permits: Arc::clone(self),
                                });
                            }
                            Err(TryAcquireError::NoPermits) => {}
                        }
                    }
                    if epoch.changed().await.is_err() {
                        return Arc::clone(&self.semaphore).acquire_owned().await.map(|inner| PullPermit {
                            inner: Some(inner),
                            permits: Arc::clone(self),
                        });
                    }
                }
            }
        }
    }
}

fn checkpoint_path(bucket: &str) -> String {
    format!("{BUCKET_META_PREFIX}/{bucket}/{BACKFILL_CHECKPOINT_FILE}")
}

fn lease_lock_key(bucket: &str) -> String {
    format!("{BACKFILL_LEASE_LOCK_PREFIX}{bucket}")
}

/// A checkpoint together with the ETag the next save must match.
#[derive(Clone, Debug)]
pub struct StoredCheckpoint {
    pub checkpoint: BackfillCheckpoint,
    pub etag: String,
}

pub async fn read_checkpoint(api: &Arc<ECStore>, bucket: &str) -> Result<Option<StoredCheckpoint>, BackfillError> {
    match read_config_with_metadata(Arc::clone(api), &checkpoint_path(bucket), &ObjectOptions::default()).await {
        Ok((data, info)) => {
            let etag = info
                .etag
                .ok_or_else(|| BackfillError::Malformed("checkpoint has no entity tag".to_string()))?;
            Ok(Some(StoredCheckpoint {
                checkpoint: BackfillCheckpoint::from_json(&data)?,
                etag,
            }))
        }
        Err(StorageError::ConfigNotFound) | Err(StorageError::FileNotFound) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Compare-and-set save: `expected_etag` `None` requires the file to be
/// absent. Returns the ETag of the saved document.
async fn write_checkpoint(
    api: &Arc<ECStore>,
    bucket: &str,
    incarnation_id: Uuid,
    checkpoint: &BackfillCheckpoint,
    expected_etag: Option<&str>,
) -> Result<String, BackfillError> {
    let fence = api.acquire_bucket_incarnation_fence(bucket, incarnation_id).await?;
    fence
        .run(write_checkpoint_while_fenced(api, bucket, checkpoint, expected_etag))
        .await?
}

/// The caller holds the destination bucket's lifecycle fence through the CAS
/// write and its read-back, including the drained erasure write tail.
async fn write_checkpoint_while_fenced(
    api: &Arc<ECStore>,
    bucket: &str,
    checkpoint: &BackfillCheckpoint,
    expected_etag: Option<&str>,
) -> Result<String, BackfillError> {
    let data = checkpoint.to_json()?;
    let preconditions = match expected_etag {
        Some(etag) => HTTPPreconditions {
            if_match: Some(etag.to_string()),
            ..Default::default()
        },
        None => HTTPPreconditions {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        },
    };
    let opts = ObjectOptions {
        max_parity: true,
        write_completion: WriteCompletion::TailDrained,
        http_preconditions: Some(preconditions),
        ..Default::default()
    };
    match save_config_with_opts(Arc::clone(api), &checkpoint_path(bucket), data, &opts).await {
        Ok(()) => {}
        Err(StorageError::PreconditionFailed) => return Err(BackfillError::Conflict(bucket.to_string())),
        Err(err) => return Err(err.into()),
    }
    let stored = read_checkpoint(api, bucket)
        .await?
        .ok_or_else(|| BackfillError::Conflict(bucket.to_string()))?;
    if stored.checkpoint.job_id != checkpoint.job_id || stored.checkpoint.updated_at != checkpoint.updated_at {
        return Err(BackfillError::Conflict(bucket.to_string()));
    }
    Ok(stored.etag)
}

/// Runtime handle of a job running in this process.
struct JobHandle {
    job_id: Uuid,
    cancel: CancellationToken,
    snapshot: Mutex<BackfillCheckpoint>,
    done: watch::Sender<bool>,
}

impl JobHandle {
    fn is_done(&self) -> bool {
        *self.done.borrow()
    }

    async fn wait_done(&self) {
        let mut rx = self.done.subscribe();
        let _ = rx.wait_for(|done| *done).await;
    }
}

/// Per-node backfill coordinator: starts, cancels, reports and recovers jobs.
pub struct BackfillRunner {
    api: Arc<ECStore>,
    node: String,
    contexts: Arc<dyn BackfillContextFactory>,
    jobs: Mutex<HashMap<String, Arc<JobHandle>>>,
}

impl fmt::Debug for BackfillRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackfillRunner")
            .field("node", &self.node)
            .field("jobs", &self.jobs.lock().keys().collect::<Vec<_>>())
            .finish()
    }
}

static GLOBAL_BACKFILL_RUNNER: OnceLock<Arc<BackfillRunner>> = OnceLock::new();

/// Publishes the process-wide runner; `false` when one was already installed.
pub fn install_global_backfill_runner(runner: Arc<BackfillRunner>) -> bool {
    GLOBAL_BACKFILL_RUNNER.set(runner).is_ok()
}

pub fn global_backfill_runner() -> Option<Arc<BackfillRunner>> {
    GLOBAL_BACKFILL_RUNNER.get().cloned()
}

/// Outcome of one recovery scan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackfillRecoveryStats {
    pub scanned: usize,
    pub taken_over: usize,
    pub cancelled: usize,
    /// Expired jobs this pass could not take over yet (lock busy, no state).
    pub deferred: usize,
}

impl BackfillRunner {
    pub fn new(api: Arc<ECStore>, node: impl Into<String>, contexts: Arc<dyn BackfillContextFactory>) -> Arc<Self> {
        Arc::new(Self {
            api,
            node: node.into(),
            contexts,
            jobs: Mutex::new(HashMap::new()),
        })
    }

    /// Runner named after this node's endpoint.
    pub async fn for_local_node(api: Arc<ECStore>, contexts: Arc<dyn BackfillContextFactory>) -> Arc<Self> {
        let node = local_node_name().await;
        Self::new(api, node, contexts)
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    /// Whether a job for `bucket` runs in this process.
    pub fn is_running_locally(&self, bucket: &str) -> bool {
        self.live_job(bucket).is_some()
    }

    /// Last persisted progress of every job running in this process, by
    /// bucket name. Observability reads this instead of the checkpoint
    /// documents so a metrics cycle never touches storage; a job that has
    /// finished is dropped here exactly as [`Self::is_running_locally`]
    /// drops it. Lock order: `jobs` before a handle's `snapshot`.
    pub fn local_job_snapshots(&self) -> Vec<(String, BackfillCheckpoint)> {
        let mut jobs = self.jobs.lock();
        jobs.retain(|_, handle| !handle.is_done());
        let mut snapshots: Vec<_> = jobs
            .iter()
            .map(|(bucket, handle)| (bucket.clone(), handle.snapshot.lock().clone()))
            .collect();
        snapshots.sort_by(|left, right| left.0.cmp(&right.0));
        snapshots
    }

    fn live_job(&self, bucket: &str) -> Option<Arc<JobHandle>> {
        let mut jobs = self.jobs.lock();
        match jobs.get(bucket) {
            Some(handle) if !handle.is_done() => Some(Arc::clone(handle)),
            Some(_) => {
                jobs.remove(bucket);
                None
            }
            None => None,
        }
    }

    /// Resolves once no job for `bucket` runs in this process.
    pub async fn wait_until_idle(&self, bucket: &str) {
        if let Some(handle) = self.live_job(bucket) {
            handle.wait_done().await;
        }
    }

    async fn lease_lock(&self, bucket: &str, timeout: Duration) -> Result<rustfs_lock::NamespaceLockGuard, BackfillError> {
        let lock = self.api.new_ns_lock(RUSTFS_META_BUCKET, &lease_lock_key(bucket)).await?;
        lock.get_write_lock_quiet(timeout)
            .await
            .map_err(|_| BackfillError::LeaseBusy(bucket.to_string()))
    }

    /// Starts a job for `bucket`; `AlreadyRunning` while one holds a lease.
    pub async fn start(&self, bucket: &str, request: BackfillRequest) -> Result<BackfillCheckpoint, BackfillError> {
        let context = self
            .contexts
            .context(bucket)
            .ok_or_else(|| BackfillError::Unavailable(bucket.to_string()))?;
        let config_updated_at = context
            .config_updated_at()
            .await?
            .ok_or_else(|| BackfillError::NotConfigured(bucket.to_string()))?;

        let _lock = self.lease_lock(bucket, get_lock_acquire_timeout()).await?;
        let now = OffsetDateTime::now_utc();
        if let Some(handle) = self.live_job(bucket) {
            return Err(BackfillError::AlreadyRunning {
                bucket: bucket.to_string(),
                job_id: handle.job_id,
                owner: self.node.clone(),
            });
        }
        let stored = read_checkpoint(&self.api, bucket).await?;
        if let Some(stored) = &stored
            && stored.checkpoint.state.is_active()
            && stored.checkpoint.lease_valid_at(now)
        {
            return Err(BackfillError::AlreadyRunning {
                bucket: bucket.to_string(),
                job_id: stored.checkpoint.job_id,
                owner: stored.checkpoint.owner.as_ref().map(|o| o.node.clone()).unwrap_or_default(),
            });
        }
        let checkpoint = BackfillCheckpoint::new(&request, config_updated_at, &self.node, now);
        let etag = write_checkpoint(
            &self.api,
            bucket,
            context.incarnation_id(),
            &checkpoint,
            stored.as_ref().map(|s| s.etag.as_str()),
        )
        .await?;
        info!(
            event = EVENT_ODM_BACKFILL_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
            state = checkpoint.state.as_str(),
            result = "started",
            bucket = %bucket,
            job_id = %checkpoint.job_id,
            dry_run = checkpoint.dry_run,
            skip_existing = checkpoint.skip_existing.as_str(),
            "On-demand migration backfill job started"
        );
        self.spawn_job(
            bucket,
            context,
            StoredCheckpoint {
                checkpoint: checkpoint.clone(),
                etag,
            },
        );
        Ok(checkpoint)
    }

    /// Cancels the job for `bucket` wherever it runs. Idempotent on a
    /// finished job; `NotFound` when no checkpoint exists.
    pub async fn cancel(&self, bucket: &str) -> Result<BackfillCheckpoint, BackfillError> {
        if let Some(handle) = self.live_job(bucket) {
            handle.cancel.cancel();
            let _ = tokio::time::timeout(CANCEL_SETTLE_TIMEOUT, handle.wait_done()).await;
            if let Some(stored) = read_checkpoint(&self.api, bucket).await? {
                return Ok(stored.checkpoint);
            }
            return Ok(handle.snapshot.lock().clone());
        }
        let incarnation_id = self.api.bucket_incarnation_id_from_disk(bucket).await?;
        let _lock = self.lease_lock(bucket, get_lock_acquire_timeout()).await?;
        let fence = self.api.acquire_bucket_incarnation_fence(bucket, incarnation_id).await?;
        fence
            .run(async {
                let Some(stored) = read_checkpoint(&self.api, bucket).await? else {
                    return Err(BackfillError::NotFound(bucket.to_string()));
                };
                if !stored.checkpoint.state.is_active() {
                    return Ok(stored.checkpoint);
                }
                let mut checkpoint = stored.checkpoint;
                let now = OffsetDateTime::now_utc();
                checkpoint.state = BackfillState::Cancelled;
                checkpoint.updated_at = now;
                write_checkpoint_while_fenced(&self.api, bucket, &checkpoint, Some(&stored.etag)).await?;
                info!(
                    event = EVENT_ODM_BACKFILL_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                    state = checkpoint.state.as_str(),
                    result = "cancelled",
                    bucket = %bucket,
                    job_id = %checkpoint.job_id,
                    owner = %checkpoint.owner.as_ref().map(|o| o.node.as_str()).unwrap_or_default(),
                    "On-demand migration backfill job cancelled remotely"
                );
                Ok(checkpoint)
            })
            .await?
    }

    /// Latest checkpoint: the in-memory progress of a local job, else the
    /// persisted document.
    pub async fn status(&self, bucket: &str) -> Result<Option<BackfillCheckpoint>, BackfillError> {
        if let Some(handle) = self.live_job(bucket) {
            return Ok(Some(handle.snapshot.lock().clone()));
        }
        Ok(read_checkpoint(&self.api, bucket).await?.map(|stored| stored.checkpoint))
    }

    /// One recovery pass over the buckets this node has a state for.
    pub async fn recover_once(&self) -> BackfillRecoveryStats {
        let mut stats = BackfillRecoveryStats::default();
        for bucket in self.contexts.buckets() {
            if self.is_running_locally(&bucket) {
                continue;
            }
            stats.scanned += 1;
            match self.try_take_over(&bucket).await {
                Ok(TakeoverOutcome::NotNeeded) => {}
                Ok(TakeoverOutcome::TakenOver) => stats.taken_over += 1,
                Ok(TakeoverOutcome::Cancelled) => stats.cancelled += 1,
                Ok(TakeoverOutcome::Deferred) => stats.deferred += 1,
                Err(err) => {
                    stats.deferred += 1;
                    warn!(
                        event = EVENT_ODM_BACKFILL_RECOVERY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                        bucket = %bucket,
                        error = %err,
                        "On-demand migration backfill recovery failed for bucket"
                    );
                }
            }
        }
        debug!(
            event = EVENT_ODM_BACKFILL_RECOVERY,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
            scanned = stats.scanned,
            taken_over = stats.taken_over,
            cancelled = stats.cancelled,
            deferred = stats.deferred,
            "On-demand migration backfill recovery pass finished"
        );
        stats
    }

    fn takeover_due(&self, checkpoint: &BackfillCheckpoint, now: OffsetDateTime) -> bool {
        if !checkpoint.state.is_active() {
            return false;
        }
        match &checkpoint.owner {
            // A job this node owned before a restart is dead by construction.
            Some(owner) => owner.lease_until <= now || owner.node == self.node,
            None => true,
        }
    }

    async fn try_take_over(&self, bucket: &str) -> Result<TakeoverOutcome, BackfillError> {
        let now = OffsetDateTime::now_utc();
        let Some(stored) = read_checkpoint(&self.api, bucket).await? else {
            return Ok(TakeoverOutcome::NotNeeded);
        };
        if !self.takeover_due(&stored.checkpoint, now) {
            return Ok(TakeoverOutcome::NotNeeded);
        }
        let Some(context) = self.contexts.context(bucket) else {
            return Ok(TakeoverOutcome::Deferred);
        };
        let _lock = match self.lease_lock(bucket, TAKEOVER_LOCK_TIMEOUT).await {
            Ok(lock) => lock,
            Err(BackfillError::LeaseBusy(_)) => return Ok(TakeoverOutcome::Deferred),
            Err(err) => return Err(err),
        };
        // Re-read under the lock: another node may have taken over meanwhile.
        let now = OffsetDateTime::now_utc();
        let Some(stored) = read_checkpoint(&self.api, bucket).await? else {
            return Ok(TakeoverOutcome::NotNeeded);
        };
        if !self.takeover_due(&stored.checkpoint, now) || self.is_running_locally(bucket) {
            return Ok(TakeoverOutcome::NotNeeded);
        }
        let mut checkpoint = stored.checkpoint.clone();
        let previous_owner = checkpoint.owner.as_ref().map(|o| o.node.clone()).unwrap_or_default();
        let config_updated_at = context.config_updated_at().await?;
        if config_updated_at.map(|at| at.unix_timestamp_nanos()) != Some(checkpoint.config_updated_at.unix_timestamp_nanos()) {
            checkpoint.state = BackfillState::Cancelled;
            checkpoint.updated_at = now;
            checkpoint.record_failure("config_changed", None, now);
            write_checkpoint(&self.api, bucket, context.incarnation_id(), &checkpoint, Some(&stored.etag)).await?;
            info!(
                event = EVENT_ODM_BACKFILL_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                state = checkpoint.state.as_str(),
                result = "config_changed",
                bucket = %bucket,
                job_id = %checkpoint.job_id,
                "On-demand migration backfill job cancelled: config changed since it started"
            );
            return Ok(TakeoverOutcome::Cancelled);
        }
        checkpoint.state = BackfillState::Running;
        checkpoint.updated_at = now;
        checkpoint.owner = Some(BackfillOwner {
            node: self.node.clone(),
            lease_until: now + BACKFILL_LEASE,
        });
        let etag = write_checkpoint(&self.api, bucket, context.incarnation_id(), &checkpoint, Some(&stored.etag)).await?;
        warn!(
            event = EVENT_ODM_BACKFILL_LEASE_TAKEOVER,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
            bucket = %bucket,
            job_id = %checkpoint.job_id,
            previous_owner = %previous_owner,
            listed = checkpoint.listed,
            resumed_from_token = checkpoint.continuation_token.is_some(),
            "On-demand migration backfill lease taken over"
        );
        self.spawn_job(bucket, context, StoredCheckpoint { checkpoint, etag });
        Ok(TakeoverOutcome::TakenOver)
    }

    fn spawn_job(&self, bucket: &str, context: Arc<dyn BackfillContext>, stored: StoredCheckpoint) {
        let handle = Arc::new(JobHandle {
            job_id: stored.checkpoint.job_id,
            cancel: CancellationToken::new(),
            snapshot: Mutex::new(stored.checkpoint.clone()),
            done: watch::channel(false).0,
        });
        self.jobs.lock().insert(bucket.to_string(), Arc::clone(&handle));
        let api = Arc::clone(&self.api);
        let node = self.node.clone();
        let bucket = bucket.to_string();
        tokio::spawn(async move {
            let mut job = Job {
                api,
                bucket,
                node,
                context,
                handle: Arc::clone(&handle),
                checkpoint: stored.checkpoint,
                etag: stored.etag,
                keys_since_save: 0,
                last_save: Instant::now(),
                outstanding: FuturesUnordered::new(),
            };
            job.run().await;
            handle.done.send_replace(true);
        });
    }
}

enum TakeoverOutcome {
    NotNeeded,
    TakenOver,
    Cancelled,
    Deferred,
}

/// Why the loop stopped before the listing was exhausted.
enum Stop {
    /// Admin cancel (local token) or config change (state token).
    Cancelled(&'static str),
    /// The checkpoint on disk no longer belongs to this job.
    Lost,
    /// The listing failed for good.
    Failed(SourceError),
    /// The bucket state went away under us.
    Unavailable,
}

type OutstandingPull = Pin<Box<dyn Future<Output = (String, Result<QueuedPullOutcome, oneshot::error::RecvError>)> + Send>>;

struct Job {
    api: Arc<ECStore>,
    bucket: String,
    node: String,
    context: Arc<dyn BackfillContext>,
    handle: Arc<JobHandle>,
    checkpoint: BackfillCheckpoint,
    etag: String,
    keys_since_save: u64,
    last_save: Instant,
    outstanding: FuturesUnordered<OutstandingPull>,
}

impl Job {
    async fn run(&mut self) {
        let outcome = self.main_loop().await;
        let now = OffsetDateTime::now_utc();
        let (state, result) = match outcome {
            Ok(()) => {
                if self.checkpoint.failed > 0 {
                    (BackfillState::CompletedWithFailures, "completed_with_failures")
                } else {
                    (BackfillState::Completed, "completed")
                }
            }
            Err(Stop::Cancelled(reason)) => {
                if reason != "admin" {
                    self.checkpoint.record_failure(reason, None, now);
                }
                (BackfillState::Cancelled, reason)
            }
            Err(Stop::Unavailable) => {
                self.checkpoint.record_failure("state_unavailable", None, now);
                (BackfillState::Cancelled, "state_unavailable")
            }
            Err(Stop::Failed(err)) => {
                self.checkpoint.record_failure(err.class_label(), None, now);
                (BackfillState::Failed, "source_error")
            }
            Err(Stop::Lost) => {
                // The document on disk is authoritative; do not touch it.
                info!(
                    event = EVENT_ODM_BACKFILL_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                    state = "lost",
                    result = "superseded",
                    bucket = %self.bucket,
                    job_id = %self.checkpoint.job_id,
                    "On-demand migration backfill job stopped: checkpoint owned elsewhere"
                );
                return;
            }
        };
        self.checkpoint.state = state;
        if let Err(err) = self.save(now).await {
            warn!(
                event = EVENT_ODM_BACKFILL_CHECKPOINT,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                bucket = %self.bucket,
                job_id = %self.checkpoint.job_id,
                state = state.as_str(),
                error = %err,
                "On-demand migration backfill final checkpoint save failed"
            );
        }
        info!(
            event = EVENT_ODM_BACKFILL_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
            state = state.as_str(),
            result = result,
            bucket = %self.bucket,
            job_id = %self.checkpoint.job_id,
            listed = self.checkpoint.listed,
            enqueued = self.checkpoint.enqueued,
            pulled = self.checkpoint.pulled,
            skipped_existing = self.checkpoint.skipped_existing,
            failed = self.checkpoint.failed,
            bytes = self.checkpoint.bytes,
            "On-demand migration backfill job finished"
        );
    }

    fn cancel_reason(&self) -> Option<&'static str> {
        if self.handle.cancel.is_cancelled() {
            Some("admin")
        } else if self.context.cancel_token().is_cancelled() {
            Some("config_changed")
        } else {
            None
        }
    }

    async fn main_loop(&mut self) -> Result<(), Stop> {
        let mut cursor = self.checkpoint.continuation_token.clone();
        let failed_at_resume = self.checkpoint.failed;
        loop {
            self.check_cancel()?;
            let page = self.list_page(cursor.as_deref()).await?;
            for object in &page.objects {
                self.check_cancel()?;
                self.checkpoint.listed += 1;
                self.checkpoint.last_key = Some(object.key.clone());
                self.keys_since_save += 1;
                if !self.checkpoint.dry_run {
                    self.process_key(object).await?;
                }
                self.drain_ready();
                self.tick(false).await?;
            }
            // A persisted cursor certifies successful work, not just listing
            // progress. Keep it at the first failed page for crash recovery.
            self.drain_all().await?;
            cursor = page.next_continuation_token;
            if self.checkpoint.failed == failed_at_resume {
                self.checkpoint.continuation_token = cursor.clone();
            }
            self.tick(true).await?;
            if !page.is_truncated {
                return Ok(());
            }
        }
    }

    fn check_cancel(&self) -> Result<(), Stop> {
        match self.cancel_reason() {
            Some(reason) => Err(Stop::Cancelled(reason)),
            None => Ok(()),
        }
    }

    async fn list_page(&mut self, cursor: Option<&str>) -> Result<SourcePage, Stop> {
        let mut attempt = 0;
        loop {
            while !self.context.source_available() {
                self.sleep(BACKFILL_IDLE_POLL).await?;
                self.tick(false).await?;
            }
            let prefix = self.checkpoint.prefix.clone();
            let token = cursor.map(str::to_string);
            match self
                .context
                .list_page(prefix.as_deref(), token.as_deref(), BACKFILL_LIST_PAGE_SIZE)
                .await
            {
                Ok(page) => return Ok(page),
                Err(err) if err.is_retryable() && attempt < LIST_MAX_RETRIES => {
                    let delay = LIST_RETRY_BASE_DELAYS[attempt];
                    attempt += 1;
                    debug!(
                        event = EVENT_ODM_BACKFILL_CHECKPOINT,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                        bucket = %self.bucket,
                        job_id = %self.checkpoint.job_id,
                        error_class = err.class_label(),
                        attempt,
                        "On-demand migration backfill listing failed; retrying"
                    );
                    self.sleep(delay).await?;
                    self.tick(false).await?;
                }
                Err(err) => return Err(Stop::Failed(err)),
            }
        }
    }

    async fn process_key(&mut self, object: &super::source_client::SourceObject) -> Result<(), Stop> {
        let key = object.key.as_str();
        match self.context.local_object(key).await {
            Ok(Some(local)) => {
                let skip = match self.checkpoint.skip_existing {
                    SkipExisting::Always => true,
                    SkipExisting::EtagOrSize => local.size == object.size && local.source_etag == object.etag,
                };
                if skip {
                    self.checkpoint.skipped_existing += 1;
                    trace!(
                        event = EVENT_ODM_BACKFILL_KEY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                        result = "skipped_existing",
                        bucket = %self.bucket,
                        key = %key,
                        "On-demand migration backfill skipped an existing object"
                    );
                    return Ok(());
                }
            }
            Ok(None) => {}
            Err(err) => {
                let now = OffsetDateTime::now_utc();
                self.checkpoint.failed += 1;
                self.checkpoint.record_failure("local_lookup", Some(key), now);
                debug!(
                    event = EVENT_ODM_BACKFILL_KEY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                    result = "local_lookup_failed",
                    bucket = %self.bucket,
                    key_hash = %key_hash(key),
                    error = %err,
                    "On-demand migration backfill could not read the local object"
                );
                return Ok(());
            }
        }
        while self.outstanding.len() >= self.context.max_outstanding() {
            self.wait_one().await?;
            self.tick(false).await?;
        }
        loop {
            match self.context.enqueue(key) {
                (EnqueueOutcome::Enqueued | EnqueueOutcome::Coalesced, report) => {
                    self.checkpoint.enqueued += 1;
                    let rx = report.ok_or(Stop::Unavailable)?;
                    {
                        let key = key.to_string();
                        self.outstanding.push(Box::pin(async move { (key, rx.await) }));
                    }
                    trace!(
                        event = EVENT_ODM_BACKFILL_KEY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                        result = "enqueued",
                        bucket = %self.bucket,
                        key = %key,
                        "On-demand migration backfill queued a pull"
                    );
                    return Ok(());
                }
                (EnqueueOutcome::QueueFull, _) => {
                    // Wait, never drop: one completion frees a slot.
                    if self.outstanding.is_empty() {
                        self.sleep(BACKFILL_IDLE_POLL).await?;
                    } else {
                        self.wait_one().await?;
                    }
                    self.tick(false).await?;
                }
                (EnqueueOutcome::Unavailable, _) => return Err(Stop::Unavailable),
            }
        }
    }

    fn record_report(&mut self, key: &str, report: Result<QueuedPullOutcome, oneshot::error::RecvError>) {
        let now = OffsetDateTime::now_utc();
        match report {
            Ok(QueuedPullOutcome::Stored { size }) => {
                self.checkpoint.pulled += 1;
                self.checkpoint.bytes += size;
            }
            Ok(QueuedPullOutcome::AlreadyPresent) => {
                self.checkpoint.skipped_existing += 1;
            }
            Ok(QueuedPullOutcome::Failed(err)) => {
                self.checkpoint.failed += 1;
                self.checkpoint.record_failure(err.reason.as_str(), Some(key), now);
            }
            Err(_) => {
                self.checkpoint.failed += 1;
                self.checkpoint.record_failure("canceled", Some(key), now);
            }
        }
    }

    /// Consumes every report that already arrived without waiting.
    fn drain_ready(&mut self) {
        use futures::FutureExt;
        while let Some(Some((key, report))) = self.outstanding.next().now_or_never() {
            self.record_report(&key, report);
        }
    }

    /// Waits for one report, keeping the checkpoint fresh meanwhile.
    async fn wait_one(&mut self) -> Result<(), Stop> {
        let admin_cancel = self.handle.cancel.clone();
        let state_cancel = self.context.cancel_token();
        loop {
            self.check_cancel()?;
            let next = tokio::select! {
                next = self.outstanding.next() => next,
                _ = tokio::time::sleep(BACKFILL_SAVE_INTERVAL) => {
                    self.tick(false).await?;
                    continue;
                }
                _ = admin_cancel.cancelled() => return Err(Stop::Cancelled("admin")),
                _ = state_cancel.cancelled() => return Err(Stop::Cancelled("config_changed")),
            };
            match next {
                Some((key, report)) => {
                    self.record_report(&key, report);
                    return Ok(());
                }
                None => return Ok(()),
            }
        }
    }

    async fn drain_all(&mut self) -> Result<(), Stop> {
        while !self.outstanding.is_empty() {
            self.wait_one().await?;
            self.tick(false).await?;
        }
        Ok(())
    }

    async fn sleep(&mut self, duration: Duration) -> Result<(), Stop> {
        let state_cancel = self.context.cancel_token();
        tokio::select! {
            _ = tokio::time::sleep(duration) => Ok(()),
            _ = self.handle.cancel.cancelled() => Err(Stop::Cancelled("admin")),
            _ = state_cancel.cancelled() => Err(Stop::Cancelled("config_changed")),
        }
    }

    /// Saves when due (or `force`d); every save renews the lease.
    async fn tick(&mut self, force: bool) -> Result<(), Stop> {
        if !force && self.keys_since_save < BACKFILL_SAVE_EVERY_KEYS && self.last_save.elapsed() < BACKFILL_SAVE_INTERVAL {
            return Ok(());
        }
        let now = OffsetDateTime::now_utc();
        match self.save(now).await {
            Ok(()) => Ok(()),
            Err(BackfillError::Conflict(_)) => match self.reconcile_conflict().await {
                Ok(()) => Ok(()),
                Err(stop) => Err(stop),
            },
            Err(err) => {
                // Storage hiccup: keep going, the next tick retries.
                warn!(
                    event = EVENT_ODM_BACKFILL_CHECKPOINT,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                    bucket = %self.bucket,
                    job_id = %self.checkpoint.job_id,
                    error = %err,
                    "On-demand migration backfill checkpoint save failed"
                );
                Ok(())
            }
        }
    }

    /// The `If-Match` failed: adopt the on-disk ETag when the document is
    /// still ours and running, else stop.
    async fn reconcile_conflict(&mut self) -> Result<(), Stop> {
        let stored = read_checkpoint(&self.api, &self.bucket).await.map_err(|_| Stop::Lost)?;
        let Some(stored) = stored else {
            return Err(Stop::Lost);
        };
        let on_disk = &stored.checkpoint;
        if on_disk.job_id != self.checkpoint.job_id
            || on_disk.state != BackfillState::Running
            || on_disk.owner.as_ref().is_some_and(|owner| owner.node != self.node)
        {
            return Err(Stop::Lost);
        }
        self.etag = stored.etag;
        Ok(())
    }

    async fn save(&mut self, now: OffsetDateTime) -> Result<(), BackfillError> {
        self.checkpoint.updated_at = now;
        if self.checkpoint.state.is_active() {
            self.checkpoint.owner = Some(BackfillOwner {
                node: self.node.clone(),
                lease_until: now + BACKFILL_LEASE,
            });
        }
        let etag =
            write_checkpoint(&self.api, &self.bucket, self.context.incarnation_id(), &self.checkpoint, Some(&self.etag)).await?;
        self.etag = etag;
        self.keys_since_save = 0;
        self.last_save = Instant::now();
        *self.handle.snapshot.lock() = self.checkpoint.clone();
        Ok(())
    }
}

/// Spawns [`run_backfill_recovery_loop`] on the store's shutdown token;
/// `false` (nothing spawned) when the store has no background token.
pub fn spawn_backfill_recovery_loop(runner: Arc<BackfillRunner>) -> bool {
    let Some(cancel) = runner.api.background_cancel_token() else {
        return false;
    };
    tokio::spawn(run_backfill_recovery_loop(runner, cancel));
    true
}

/// Background recovery: scans every [`BACKFILL_RECOVERY_INTERVAL`], sooner
/// while a takeover is deferred. Registered by the binary at startup.
pub async fn run_backfill_recovery_loop(runner: Arc<BackfillRunner>, cancel: CancellationToken) {
    let mut wait = Duration::ZERO;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(wait) => {}
        }
        let stats = runner.recover_once().await;
        wait = if stats.deferred > 0 {
            BACKFILL_RECOVERY_RETRY_INTERVAL
        } else {
            BACKFILL_RECOVERY_INTERVAL
        };
    }
}

#[cfg(test)]
mod tests {
    use super::super::storage_api::test_support::{BucketOperations as _, isolated_store_over_temp_disks};
    use super::*;
    use crate::on_demand_migration::source_client::SourceObject;
    use crate::on_demand_migration::sys::PullError;
    use std::collections::{BTreeSet, HashSet};
    use std::sync::atomic::AtomicBool;

    const GOLDEN: &str = r#"{"format_version":1,"job_id":"11111111-1111-4111-8111-111111111111","state":"running","config_updated_at":"2026-09-02T10:00:00Z","prefix":"photos/","skip_existing":"always","dry_run":false,"continuation_token":"cGhvdG9zLzEwMDA=","listed":2000,"enqueued":1500,"pulled":1400,"skipped_existing":500,"failed":3,"bytes":73400320,"last_key":"photos/2024/02.jpg","last_error":{"class":"source_timeout","key_hash":"9f2c3b0a1d4e5f60","at":"2026-09-02T10:05:00Z"},"failed_keys":["9f2c3b0a1d4e5f60"],"started_at":"2026-09-02T10:00:30Z","updated_at":"2026-09-02T10:05:10Z","owner":{"node":"node-a:9000","lease_until":"2026-09-02T10:06:10Z"}}"#;

    fn ts(unix: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(unix).expect("timestamp")
    }

    #[test]
    fn checkpoint_golden_round_trips_byte_for_byte() {
        let checkpoint = BackfillCheckpoint::from_json(GOLDEN.as_bytes()).expect("golden decodes");
        assert_eq!(checkpoint.state, BackfillState::Running);
        assert_eq!(checkpoint.skip_existing, SkipExisting::Always);
        assert_eq!(checkpoint.listed, 2000);
        assert_eq!(checkpoint.failed_keys, vec!["9f2c3b0a1d4e5f60".to_string()]);
        assert_eq!(checkpoint.owner.as_ref().map(|o| o.node.as_str()), Some("node-a:9000"));
        assert!(checkpoint.extra.is_empty());
        let encoded = String::from_utf8(checkpoint.to_json().expect("encodes")).expect("utf-8");
        assert_eq!(encoded, GOLDEN, "field order and formatting are the on-disk contract");
    }

    #[test]
    fn checkpoint_keeps_unknown_fields_and_rejects_foreign_versions() {
        let newer = GOLDEN.replacen("\"listed\":2000", "\"listed\":2000,\"throttle_hint\":{\"mode\":\"soft\"}", 1);
        let checkpoint = BackfillCheckpoint::from_json(newer.as_bytes()).expect("unknown fields are tolerated");
        assert_eq!(checkpoint.listed, 2000);
        assert_eq!(checkpoint.extra.get("throttle_hint").and_then(|v| v["mode"].as_str()), Some("soft"));
        let re_encoded = String::from_utf8(checkpoint.to_json().expect("encodes")).expect("utf-8");
        assert!(
            re_encoded.contains("\"throttle_hint\":{\"mode\":\"soft\"}"),
            "unknown fields survive a save"
        );

        let foreign = GOLDEN.replacen("\"format_version\":1", "\"format_version\":2", 1);
        match BackfillCheckpoint::from_json(foreign.as_bytes()) {
            Err(BackfillError::UnsupportedFormatVersion { found: 2, supported: 1 }) => {}
            other => panic!("expected a typed version error, got {other:?}"),
        }
        assert!(matches!(
            BackfillCheckpoint::from_json(b"{\"format_version\":1,\"job_id\":42}"),
            Err(BackfillError::Malformed(_))
        ));
        assert!(matches!(BackfillCheckpoint::from_json(b"not json"), Err(BackfillError::Malformed(_))));
    }

    #[test]
    fn failed_keys_ring_holds_hashes_only() {
        let mut checkpoint = BackfillCheckpoint::from_json(GOLDEN.as_bytes()).expect("golden");
        checkpoint.failed_keys.clear();
        for i in 0..(BACKFILL_FAILED_KEYS_CAPACITY + 5) {
            checkpoint.record_failure("local_write", Some(&format!("secret/key-{i}")), ts(1));
        }
        assert_eq!(checkpoint.failed_keys.len(), BACKFILL_FAILED_KEYS_CAPACITY);
        assert_eq!(checkpoint.failed_keys.last(), Some(&key_hash("secret/key-1004")));
        assert_eq!(checkpoint.failed_keys.first(), Some(&key_hash("secret/key-5")));
        let json = String::from_utf8(checkpoint.to_json().expect("encodes")).expect("utf-8");
        assert!(!json.contains("secret/key-"), "plaintext keys must never reach the checkpoint");
        assert_eq!(key_hash("a").len(), 16);
    }

    #[test]
    fn skip_existing_labels_round_trip() {
        assert_eq!(SkipExisting::parse("always"), Some(SkipExisting::Always));
        assert_eq!(SkipExisting::parse("etag_or_size"), Some(SkipExisting::EtagOrSize));
        assert_eq!(SkipExisting::parse("never"), None);
        assert_eq!(SkipExisting::EtagOrSize.as_str(), "etag_or_size");
        assert_eq!(BackfillState::CompletedWithFailures.as_str(), "completed_with_failures");
        assert!(BackfillState::Pending.is_active() && BackfillState::Running.is_active());
        assert!(!BackfillState::Cancelled.is_active());
    }

    #[tokio::test]
    async fn online_waiters_take_permits_before_backfill() {
        let permits = PriorityPullPermits::new(1);
        let order = Arc::new(Mutex::new(Vec::new()));
        let held = permits.acquire(PullPriority::Online).await.expect("first online permit");

        let backfill = {
            let permits = Arc::clone(&permits);
            let order = Arc::clone(&order);
            tokio::spawn(async move {
                let permit = permits.acquire(PullPriority::Backfill).await.expect("backfill permit");
                order.lock().push("backfill");
                permit
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!backfill.is_finished(), "backfill must wait while the permit is held");

        let online = {
            let permits = Arc::clone(&permits);
            let order = Arc::clone(&order);
            tokio::spawn(async move {
                let permit = permits.acquire(PullPriority::Online).await.expect("online permit");
                order.lock().push("online");
                permit
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(permits.online_waiters(), 1);

        drop(held);
        let online_permit = online.await.expect("online task");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            order.lock().as_slice(),
            &["online"],
            "the later online waiter wins over the earlier backfill waiter"
        );
        assert!(!backfill.is_finished());

        drop(online_permit);
        let backfill_permit = backfill.await.expect("backfill task");
        assert_eq!(order.lock().as_slice(), &["online", "backfill"]);
        assert_eq!(permits.available_permits(), 0);
        drop(backfill_permit);
        assert_eq!(permits.available_permits(), 1);
    }

    /// Scripted source + local store + queue with a controllable report path.
    struct MockContext {
        incarnation_id: Mutex<Option<Uuid>>,
        objects: Vec<SourceObject>,
        page_size: usize,
        local: Mutex<HashMap<String, LocalBackfillObject>>,
        enqueued: Mutex<Vec<String>>,
        list_requests: Mutex<Vec<Option<String>>>,
        queue_capacity: usize,
        pending: Mutex<Vec<(String, oneshot::Sender<QueuedPullOutcome>)>>,
        fail_keys: HashSet<String>,
        coalesced: bool,
        auto_complete: AtomicBool,
        cancel: CancellationToken,
        config_updated_at: Mutex<Option<OffsetDateTime>>,
        list_error: Mutex<Option<SourceError>>,
    }

    impl MockContext {
        fn new(keys: usize, page_size: usize) -> Arc<Self> {
            let objects = (0..keys)
                .map(|i| SourceObject {
                    key: format!("k/{i:05}"),
                    etag: Some(format!("etag-{i}")),
                    size: 10 + i as u64,
                    last_modified: None,
                    storage_class: None,
                    is_multipart_etag: false,
                })
                .collect();
            Arc::new(Self {
                incarnation_id: Mutex::new(None),
                objects,
                page_size,
                local: Mutex::new(HashMap::new()),
                enqueued: Mutex::new(Vec::new()),
                list_requests: Mutex::new(Vec::new()),
                queue_capacity: usize::MAX,
                pending: Mutex::new(Vec::new()),
                fail_keys: HashSet::new(),
                coalesced: false,
                auto_complete: AtomicBool::new(true),
                cancel: CancellationToken::new(),
                config_updated_at: Mutex::new(Some(ts(1_700_000_000))),
                list_error: Mutex::new(None),
            })
        }

        fn complete_pending(&self) {
            for (key, tx) in self.pending.lock().drain(..) {
                let outcome = if self.fail_keys.contains(&key) {
                    QueuedPullOutcome::Failed(PullError::new(super::super::stats::PullFailureReason::LocalWrite, "disk full"))
                } else {
                    QueuedPullOutcome::Stored { size: 10 }
                };
                let _ = tx.send(outcome);
            }
        }

        fn enqueued_keys(&self) -> Vec<String> {
            self.enqueued.lock().clone()
        }
    }

    #[async_trait]
    impl BackfillContext for MockContext {
        fn incarnation_id(&self) -> Uuid {
            self.incarnation_id.lock().expect("test bucket initialized")
        }

        async fn list_page(&self, prefix: Option<&str>, token: Option<&str>, max_keys: i32) -> Result<SourcePage, SourceError> {
            if let Some(err) = self.list_error.lock().take() {
                return Err(err);
            }
            self.list_requests.lock().push(token.map(str::to_string));
            let start: usize = token.map(|t| t.parse().expect("mock token")).unwrap_or(0);
            let page = self.page_size.min(max_keys as usize);
            let objects: Vec<SourceObject> = self
                .objects
                .iter()
                .filter(|o| prefix.is_none_or(|p| o.key.starts_with(p)))
                .skip(start)
                .take(page)
                .cloned()
                .collect();
            let total = self
                .objects
                .iter()
                .filter(|o| prefix.is_none_or(|p| o.key.starts_with(p)))
                .count();
            let end = start + objects.len();
            let is_truncated = end < total;
            Ok(SourcePage {
                objects,
                is_truncated,
                next_continuation_token: is_truncated.then(|| end.to_string()),
                ..Default::default()
            })
        }

        fn source_available(&self) -> bool {
            true
        }

        async fn local_object(&self, key: &str) -> Result<Option<LocalBackfillObject>, StorageError> {
            Ok(self.local.lock().get(key).cloned())
        }

        fn enqueue(&self, key: &str) -> (EnqueueOutcome, PullReport) {
            if self.pending.lock().len() >= self.queue_capacity {
                return (EnqueueOutcome::QueueFull, None);
            }
            self.enqueued.lock().push(key.to_string());
            let (tx, rx) = oneshot::channel();
            if self.auto_complete.load(Ordering::Relaxed) {
                let outcome = if self.fail_keys.contains(key) {
                    QueuedPullOutcome::Failed(PullError::new(super::super::stats::PullFailureReason::LocalWrite, "disk full"))
                } else {
                    QueuedPullOutcome::Stored { size: 10 }
                };
                let _ = tx.send(outcome);
            } else {
                self.pending.lock().push((key.to_string(), tx));
            }
            let outcome = if self.coalesced {
                EnqueueOutcome::Coalesced
            } else {
                EnqueueOutcome::Enqueued
            };
            (outcome, Some(futures::FutureExt::shared(rx)))
        }

        fn cancel_token(&self) -> CancellationToken {
            self.cancel.clone()
        }

        async fn config_updated_at(&self) -> Result<Option<OffsetDateTime>, StorageError> {
            Ok(*self.config_updated_at.lock())
        }
    }

    struct MockContexts(Mutex<HashMap<String, Arc<MockContext>>>);

    impl BackfillContextFactory for MockContexts {
        fn context(&self, bucket: &str) -> Option<Arc<dyn BackfillContext>> {
            self.0
                .lock()
                .get(bucket)
                .map(|ctx| Arc::clone(ctx) as Arc<dyn BackfillContext>)
        }

        fn buckets(&self) -> Vec<String> {
            self.0.lock().keys().cloned().collect()
        }
    }

    async fn runner_with(
        node: &str,
        bucket: &str,
        context: Arc<MockContext>,
    ) -> (Vec<tempfile::TempDir>, Arc<ECStore>, Arc<BackfillRunner>) {
        let (dirs, store) = isolated_store_over_temp_disks().await;
        super::super::storage_api::test_support::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        store
            .make_bucket(bucket, &Default::default())
            .await
            .expect("create test bucket");
        *context.incarnation_id.lock() = Some(
            store
                .bucket_incarnation_id_from_disk(bucket)
                .await
                .expect("test bucket identity"),
        );
        let runner = runner_on(node, bucket, context, Arc::clone(&store));
        (dirs, store, runner)
    }

    fn runner_on(node: &str, bucket: &str, context: Arc<MockContext>, store: Arc<ECStore>) -> Arc<BackfillRunner> {
        let contexts = MockContexts(Mutex::new(HashMap::from([(bucket.to_string(), context)])));
        BackfillRunner::new(store, node, Arc::new(contexts))
    }

    #[tokio::test]
    async fn stale_checkpoint_writer_cannot_resurrect_or_overwrite_a_recreated_bucket() {
        let bucket = "backfill-incarnation";
        let context = MockContext::new(0, 1);
        let (_dirs, store, _runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        let old_incarnation = context.incarnation_id();
        let old = BackfillCheckpoint::new(&BackfillRequest::default(), ts(1_700_000_000), "node-a", ts(1_700_000_001));
        let old_etag = write_checkpoint(&store, bucket, old_incarnation, &old, None)
            .await
            .expect("old checkpoint");

        store
            .delete_bucket(bucket, &Default::default())
            .await
            .expect("delete original bucket");
        store.make_bucket(bucket, &Default::default()).await.expect("recreate bucket");
        let current_incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("new identity");
        assert_ne!(old_incarnation, current_incarnation);
        assert!(
            read_checkpoint(&store, bucket)
                .await
                .expect("read after recreation")
                .is_none()
        );
        for expected_etag in [None, Some(old_etag.as_str())] {
            let error = write_checkpoint(&store, bucket, old_incarnation, &old, expected_etag)
                .await
                .expect_err("stale writer rejected");
            assert!(matches!(error, BackfillError::Storage(StorageError::BucketNotFound(_))));
        }
        assert!(
            read_checkpoint(&store, bucket)
                .await
                .expect("stale writer left no checkpoint")
                .is_none()
        );

        let current = BackfillCheckpoint::new(&BackfillRequest::default(), ts(1_700_000_000), "node-b", ts(1_700_000_002));
        let current_etag = write_checkpoint(&store, bucket, current_incarnation, &current, None)
            .await
            .expect("current checkpoint");
        let error = write_checkpoint(&store, bucket, old_incarnation, &old, Some(&current_etag))
            .await
            .expect_err("old identity cannot overwrite a matching ETag");
        assert!(matches!(error, BackfillError::Storage(StorageError::BucketNotFound(_))));
        let stored = read_checkpoint(&store, bucket)
            .await
            .expect("read current checkpoint")
            .expect("current checkpoint remains");
        assert_eq!(stored.etag, current_etag);
        assert_eq!(stored.checkpoint, current);
    }

    #[tokio::test]
    async fn full_backfill_lists_pages_and_counts_every_key() {
        let bucket = "backfill-full";
        let context = MockContext::new(2500, 1000);
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;

        let started = runner.start(bucket, BackfillRequest::default()).await.expect("start");
        assert_eq!(started.state, BackfillState::Running);
        runner.wait_until_idle(bucket).await;

        let stored = read_checkpoint(&store, bucket).await.expect("read").expect("checkpoint");
        let cp = stored.checkpoint;
        assert_eq!(cp.state, BackfillState::Completed);
        assert_eq!((cp.listed, cp.enqueued, cp.pulled, cp.failed), (2500, 2500, 2500, 0));
        assert_eq!(cp.bytes, 25_000);
        assert_eq!(cp.continuation_token, None);
        assert_eq!(cp.last_key.as_deref(), Some("k/02499"));
        assert_eq!(context.list_requests.lock().len(), 3, "2500 keys at 1000 per page is three source lists");
        assert_eq!(context.enqueued_keys().len(), 2500);
        assert!(!runner.is_running_locally(bucket));
        let status = runner.status(bucket).await.expect("status").expect("present");
        assert_eq!(status.state, BackfillState::Completed);
    }

    #[tokio::test]
    async fn dry_run_lists_without_enqueueing_and_prefix_scopes_the_listing() {
        let bucket = "backfill-dry";
        let context = MockContext::new(300, 1000);
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        runner
            .start(
                bucket,
                BackfillRequest {
                    prefix: Some("k/001".to_string()),
                    skip_existing: SkipExisting::EtagOrSize,
                    dry_run: true,
                },
            )
            .await
            .expect("start");
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::Completed);
        assert!(cp.dry_run);
        assert_eq!(cp.listed, 100, "k/00100..k/00199");
        assert_eq!((cp.enqueued, cp.pulled), (0, 0));
        assert!(context.enqueued_keys().is_empty(), "a dry run never writes back");
    }

    #[tokio::test]
    async fn skip_existing_policies_decide_what_is_re_pulled() {
        let bucket = "backfill-skip";
        let context = MockContext::new(4, 1000);
        context.local.lock().insert(
            "k/00000".to_string(),
            LocalBackfillObject {
                size: 10,
                source_etag: Some("etag-0".to_string()),
            },
        );
        context.local.lock().insert(
            "k/00001".to_string(),
            LocalBackfillObject {
                size: 11,
                source_etag: Some("stale".to_string()),
            },
        );
        context.local.lock().insert(
            "k/00002".to_string(),
            LocalBackfillObject {
                size: 5,
                source_etag: Some("etag-2".to_string()),
            },
        );
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;

        runner.start(bucket, BackfillRequest::default()).await.expect("start");
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.skipped_existing, 3, "always: every existing key is skipped");
        assert_eq!(context.enqueued_keys(), vec!["k/00003".to_string()]);

        context.enqueued.lock().clear();
        runner
            .start(
                bucket,
                BackfillRequest {
                    skip_existing: SkipExisting::EtagOrSize,
                    ..Default::default()
                },
            )
            .await
            .expect("second start after completion");
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.skipped_existing, 1, "only the matching etag+size key is skipped");
        assert_eq!(
            context.enqueued_keys(),
            vec!["k/00001".to_string(), "k/00002".to_string(), "k/00003".to_string()],
            "etag mismatch and size mismatch are re-pulled"
        );
    }

    #[tokio::test]
    async fn failed_pulls_are_counted_hashed_and_finish_with_failures() {
        let bucket = "backfill-failed";
        let mut context = MockContext::new(5, 2);
        Arc::get_mut(&mut context)
            .expect("unshared")
            .fail_keys
            .insert("k/00002".to_string());
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        runner.start(bucket, BackfillRequest::default()).await.expect("start");
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::CompletedWithFailures);
        assert_eq!((cp.pulled, cp.failed), (4, 1));
        assert_eq!(cp.continuation_token.as_deref(), Some("2"), "retain the first failed page for recovery");
        assert_eq!(cp.failed_keys, vec![key_hash("k/00002")]);
        let last = cp.last_error.expect("last error");
        assert_eq!(last.class, "local_write");
        assert_eq!(last.key_hash.as_deref(), Some(key_hash("k/00002").as_str()));
    }

    #[tokio::test]
    async fn coalesced_pulls_block_the_checkpoint_and_report_failures() {
        let bucket = "backfill-coalesced";
        let mut context = MockContext::new(1, 1);
        {
            let ctx = Arc::get_mut(&mut context).expect("unshared");
            ctx.coalesced = true;
            ctx.auto_complete = AtomicBool::new(false);
            ctx.fail_keys.insert("k/00000".to_string());
        }
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        runner.start(bucket, BackfillRequest::default()).await.expect("start");
        tokio::time::timeout(Duration::from_secs(10), async {
            while context.pending.lock().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("job enqueued");
        assert!(runner.is_running_locally(bucket), "coalescing is not completion");
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert!(cp.state.is_active());
        assert!(cp.continuation_token.is_none());
        context.complete_pending();
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::CompletedWithFailures);
        assert_eq!((cp.enqueued, cp.pulled, cp.failed), (1, 0, 1));
        assert_eq!(cp.failed_keys, vec![key_hash("k/00000")]);
    }

    #[tokio::test]
    async fn listing_failure_marks_the_job_failed_with_the_error_class() {
        let bucket = "backfill-list-error";
        let context = MockContext::new(5, 1000);
        *context.list_error.lock() = Some(SourceError::AccessDenied);
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        runner.start(bucket, BackfillRequest::default()).await.expect("start");
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::Failed);
        assert_eq!(cp.last_error.map(|e| e.class), Some("access_denied".to_string()));
    }

    #[tokio::test]
    async fn queue_full_waits_instead_of_dropping() {
        let bucket = "backfill-queue-full";
        let mut context = MockContext::new(6, 1000);
        {
            let ctx = Arc::get_mut(&mut context).expect("unshared");
            ctx.queue_capacity = 2;
            ctx.auto_complete = AtomicBool::new(false);
        }
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        runner.start(bucket, BackfillRequest::default()).await.expect("start");

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(context.enqueued_keys().len(), 2, "the job blocks on a full queue");
        assert!(runner.is_running_locally(bucket));
        for _ in 0..3 {
            context.complete_pending();
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::Completed);
        assert_eq!((cp.enqueued, cp.pulled), (6, 6), "every key was queued exactly once");
    }

    #[tokio::test]
    async fn second_start_is_rejected_while_running_and_cancel_stops_enqueueing() {
        let bucket = "backfill-cancel";
        let mut context = MockContext::new(50, 1000);
        {
            let ctx = Arc::get_mut(&mut context).expect("unshared");
            ctx.queue_capacity = 3;
            ctx.auto_complete = AtomicBool::new(false);
        }
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        let first = runner.start(bucket, BackfillRequest::default()).await.expect("start");
        tokio::time::sleep(Duration::from_millis(200)).await;

        match runner.start(bucket, BackfillRequest::default()).await {
            Err(BackfillError::AlreadyRunning { job_id, owner, .. }) => {
                assert_eq!(job_id, first.job_id);
                assert_eq!(owner, "node-a");
            }
            other => panic!("expected AlreadyRunning, got {other:?}"),
        }

        let cancelled = runner.cancel(bucket).await.expect("cancel");
        assert_eq!(cancelled.state, BackfillState::Cancelled);
        assert_eq!(cancelled.job_id, first.job_id);
        let enqueued_at_cancel = context.enqueued_keys().len();
        assert!(enqueued_at_cancel <= 3);
        context.complete_pending();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(context.enqueued_keys().len(), enqueued_at_cancel, "nothing is queued after cancel");
        assert!(!runner.is_running_locally(bucket));
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::Cancelled);
        assert_eq!(runner.cancel(bucket).await.expect("idempotent cancel").state, BackfillState::Cancelled);

        assert!(matches!(
            runner.cancel("never-started").await,
            Err(BackfillError::NotFound(_)) | Err(BackfillError::Storage(_))
        ));
    }

    #[tokio::test]
    async fn config_change_cancels_a_running_job() {
        let bucket = "backfill-config-change";
        let mut context = MockContext::new(50, 1000);
        {
            let ctx = Arc::get_mut(&mut context).expect("unshared");
            ctx.queue_capacity = 2;
            ctx.auto_complete = AtomicBool::new(false);
        }
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        runner.start(bucket, BackfillRequest::default()).await.expect("start");
        tokio::time::sleep(Duration::from_millis(200)).await;
        context.cancel.cancel();
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::Cancelled);
        assert_eq!(cp.last_error.map(|e| e.class), Some("config_changed".to_string()));
    }

    #[tokio::test]
    async fn concurrent_starts_admit_exactly_one_job() {
        let bucket = "backfill-race";
        let mut context = MockContext::new(20, 1000);
        {
            let ctx = Arc::get_mut(&mut context).expect("unshared");
            ctx.queue_capacity = 1;
            ctx.auto_complete = AtomicBool::new(false);
        }
        let (_dirs, store, runner_a) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        let runner_b = runner_on("node-b", bucket, Arc::clone(&context), Arc::clone(&store));

        let (a, b) = tokio::join!(
            runner_a.start(bucket, BackfillRequest::default()),
            runner_b.start(bucket, BackfillRequest::default())
        );
        let rejected = match (a, b) {
            (Ok(_), Err(rejected)) | (Err(rejected), Ok(_)) => rejected,
            (a, b) => panic!("exactly one node must win: {a:?} / {b:?}"),
        };
        assert!(
            matches!(rejected, BackfillError::AlreadyRunning { .. }),
            "the loser sees the winner's lease: {rejected:?}"
        );
        context.cancel.cancel();
        runner_a.wait_until_idle(bucket).await;
        runner_b.wait_until_idle(bucket).await;
    }

    #[tokio::test]
    async fn recovery_takes_over_an_expired_lease_and_resumes_from_the_cursor() {
        let bucket = "backfill-takeover";
        let context = MockContext::new(2500, 1000);
        let (_dirs, store, runner) = runner_with("node-b", bucket, Arc::clone(&context)).await;

        // A crashed node-a left a running checkpoint: one page done, lease expired.
        let now = OffsetDateTime::now_utc();
        let mut crashed =
            BackfillCheckpoint::new(&BackfillRequest::default(), ts(1_700_000_000), "node-a", now - Duration::from_secs(300));
        crashed.continuation_token = Some("1000".to_string());
        crashed.listed = 1000;
        crashed.enqueued = 1000;
        crashed.pulled = 1000;
        crashed.owner = Some(BackfillOwner {
            node: "node-a".to_string(),
            lease_until: now - Duration::from_secs(120),
        });
        let etag = write_checkpoint(&store, bucket, context.incarnation_id(), &crashed, None)
            .await
            .expect("seed checkpoint");

        // A live lease is left alone.
        let mut live = crashed.clone();
        live.owner = Some(BackfillOwner {
            node: "node-a".to_string(),
            lease_until: now + Duration::from_secs(60),
        });
        live.updated_at = now;
        let etag = write_checkpoint(&store, bucket, context.incarnation_id(), &live, Some(&etag))
            .await
            .expect("live lease");
        assert_eq!(runner.recover_once().await.taken_over, 0, "unexpired lease must not be taken over");
        assert!(!runner.is_running_locally(bucket));
        match runner.start(bucket, BackfillRequest::default()).await {
            Err(BackfillError::AlreadyRunning { owner, .. }) => assert_eq!(owner, "node-a"),
            other => panic!("live lease must reject start, got {other:?}"),
        }

        // Expire it again and recover.
        let mut expired = live.clone();
        expired.owner = Some(BackfillOwner {
            node: "node-a".to_string(),
            lease_until: now - Duration::from_secs(1),
        });
        expired.updated_at = now + Duration::from_millis(1);
        write_checkpoint(&store, bucket, context.incarnation_id(), &expired, Some(&etag))
            .await
            .expect("expire lease");
        let stats = runner.recover_once().await;
        assert_eq!(stats.taken_over, 1);
        runner.wait_until_idle(bucket).await;

        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::Completed);
        assert_eq!(cp.job_id, crashed.job_id, "the same job continues");
        assert_eq!(cp.owner.as_ref().map(|o| o.node.as_str()), Some("node-b"));
        assert_eq!(cp.listed, 2500, "1000 from before the crash plus 1500 resumed");
        assert_eq!(
            context.list_requests.lock().as_slice(),
            &[Some("1000".to_string()), Some("2000".to_string())],
            "the resumed job lists from the persisted cursor, never from the start"
        );
        assert_eq!(runner.recover_once().await.taken_over, 0, "a finished job is not recovered");
    }

    #[tokio::test]
    async fn recovery_advances_past_historical_failures_but_pins_new_failures() {
        let bucket = "backfill-takeover-failed";
        let mut context = MockContext::new(8, 2);
        {
            let ctx = Arc::get_mut(&mut context).expect("unshared");
            ctx.auto_complete = AtomicBool::new(false);
            ctx.fail_keys.insert("k/00004".to_string());
        }
        let (_dirs, store, runner) = runner_with("node-b", bucket, Arc::clone(&context)).await;
        let crashed_at = OffsetDateTime::now_utc() - Duration::from_secs(300);
        let mut crashed = BackfillCheckpoint::new(&BackfillRequest::default(), ts(1_700_000_000), "node-a", crashed_at);
        crashed.continuation_token = Some("2".to_string());
        crashed.failed = 1;
        crashed.record_failure("local_write", Some("k/00002"), crashed_at);
        write_checkpoint(&store, bucket, context.incarnation_id(), &crashed, None)
            .await
            .expect("seed failed page with an expired lease");

        assert_eq!(runner.recover_once().await.taken_over, 1);
        for (page_start, durable_token, failures) in [(2, "2", 1), (4, "4", 1), (6, "4", 2)] {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if context.pending.lock().len() == 2 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("resumed page enqueued before its reports complete");
            assert_eq!(
                context.pending.lock().iter().map(|(key, _)| key.clone()).collect::<Vec<_>>(),
                vec![format!("k/{page_start:05}"), format!("k/{:05}", page_start + 1)]
            );
            let cp = read_checkpoint(&store, bucket)
                .await
                .expect("read persisted page boundary")
                .expect("checkpoint")
                .checkpoint;
            assert_eq!(cp.job_id, crashed.job_id);
            assert_eq!(cp.owner.as_ref().map(|owner| owner.node.as_str()), Some("node-b"));
            assert_eq!(cp.continuation_token.as_deref(), Some(durable_token));
            assert_eq!(cp.failed, failures);
            context.complete_pending();
        }
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read completed checkpoint")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::CompletedWithFailures);
        assert_eq!((cp.pulled, cp.failed), (5, 2));
        assert_eq!(cp.continuation_token.as_deref(), Some("4"));
        assert_eq!(cp.failed_keys, vec![key_hash("k/00002"), key_hash("k/00004")]);
        assert_eq!(
            context.list_requests.lock().as_slice(),
            &[Some("2".to_string()), Some("4".to_string()), Some("6".to_string())]
        );
    }

    #[tokio::test]
    async fn recovery_cancels_a_job_whose_config_changed_and_reclaims_own_node_jobs() {
        let bucket = "backfill-recovery-config";
        let context = MockContext::new(10, 1000);
        let (_dirs, store, runner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        let now = OffsetDateTime::now_utc();

        // Same node name, unexpired lease: only a restart can produce this.
        let own = BackfillCheckpoint::new(&BackfillRequest::default(), ts(1_700_000_000), "node-a", now);
        let etag = write_checkpoint(&store, bucket, context.incarnation_id(), &own, None)
            .await
            .expect("seed");
        assert_eq!(runner.recover_once().await.taken_over, 1, "own-node running job is reclaimed at once");
        runner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::Completed);
        let _ = etag;

        // Config changed since the (expired) job started: cancelled, not resumed.
        let stored = read_checkpoint(&store, bucket).await.expect("read").expect("checkpoint");
        let mut stale = BackfillCheckpoint::new(&BackfillRequest::default(), ts(1_600_000_000), "node-z", now);
        stale.owner = Some(BackfillOwner {
            node: "node-z".to_string(),
            lease_until: now - Duration::from_secs(1),
        });
        write_checkpoint(&store, bucket, context.incarnation_id(), &stale, Some(&stored.etag))
            .await
            .expect("seed stale");
        let stats = runner.recover_once().await;
        assert_eq!((stats.taken_over, stats.cancelled), (0, 1));
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::Cancelled);
        assert_eq!(cp.last_error.map(|e| e.class), Some("config_changed".to_string()));
        assert!(!runner.is_running_locally(bucket));
    }

    #[tokio::test]
    async fn remote_cancel_stops_the_owner_at_its_next_save() {
        let bucket = "backfill-remote-cancel";
        let mut context = MockContext::new(3000, 1000);
        {
            let ctx = Arc::get_mut(&mut context).expect("unshared");
            ctx.queue_capacity = 4;
            ctx.auto_complete = AtomicBool::new(false);
        }
        let (_dirs, store, owner) = runner_with("node-a", bucket, Arc::clone(&context)).await;
        let other = runner_on("node-b", bucket, Arc::clone(&context), Arc::clone(&store));
        owner.start(bucket, BackfillRequest::default()).await.expect("start");
        tokio::time::sleep(Duration::from_millis(200)).await;

        let cancelled = other.cancel(bucket).await.expect("remote cancel");
        assert_eq!(cancelled.state, BackfillState::Cancelled);
        // Let the owner's queue drain so it reaches a save and observes the conflict.
        for _ in 0..1000 {
            context.complete_pending();
            if !owner.is_running_locally(bucket) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        owner.wait_until_idle(bucket).await;
        let cp = read_checkpoint(&store, bucket)
            .await
            .expect("read")
            .expect("checkpoint")
            .checkpoint;
        assert_eq!(cp.state, BackfillState::Cancelled, "the owner must not overwrite the remote cancel");
        let keys: BTreeSet<String> = context.enqueued_keys().into_iter().collect();
        assert!(keys.len() < 3000, "the owner stopped before the listing ended");
    }
}
