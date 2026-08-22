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

use crate::heal::{
    DiskError, EcstoreError, ErasureSetHealer, HealDiskExt as _,
    erasure_healer::target_outcomes_complete,
    progress::HealProgress,
    resume::{
        CheckpointManager, ReplacementPhase, ReplacementTargetIdentity, ResumeManager, replacement_target_identities_match,
    },
    storage::{HealBucketUsageBaseline, HealStorageAPI, next_heal_listing_token},
};
use crate::{Error, Result};
use metrics::{counter, histogram};
use rustfs_common::heal_channel::{HealOpts, HealRequestSource, HealScanMode};
use rustfs_common::trace_bus::{TraceEvent, TraceFunc, TraceKind, trace_emit};
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_utils::path::SLASH_SEPARATOR;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME, RUSTFS_META_BUCKET};

const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_TASK: &str = "task";
const LOG_SUBSYSTEM_OBJECT: &str = "object";
const EVENT_HEAL_TASK_STATE: &str = "heal_task_state";
const EVENT_HEAL_OBJECT_STAGE: &str = "heal_object_stage";
const EVENT_HEAL_OBJECT_MISSING: &str = "heal_object_missing";
const MAX_RETAINED_HEAL_RESULT_ITEMS: usize = 1024;
const EVENT_HEAL_OBJECT_RESULT: &str = "heal_object_result";
const MAX_BUCKET_OBJECT_HEAL_RETRIES: u32 = 3;
const MAX_BUCKET_FAILURE_LOG_SAMPLES: u64 = 5;

/// Emits at `$level`, demoted to `debug!` when `$demote` is true. Keeps
/// per-object heal work — Object/Metadata/ECDecode tasks queued per
/// object by MRF/autoheal/scanner loops, and per-object sweep failures past
/// a sample cap — from amplifying into one info!/warn!/error! line per
/// object during mass recovery (rustfs/rustfs#5716). Aggregate task kinds
/// and foreground (admin/internal) requests keep operator-visible levels;
/// metrics and end-of-sweep summaries carry the aggregate signal for the
/// demoted paths.
macro_rules! demote_to_debug_when {
    ($demote:expr, $level:ident, target: $target:expr, { $($fields:tt)* }) => {
        if $demote {
            tracing::debug!(target: $target, $($fields)*);
        } else {
            tracing::$level!(target: $target, $($fields)*);
        }
    };
}
pub(crate) use demote_to_debug_when;
const EVENT_HEAL_BUCKET_STAGE: &str = "heal_bucket_stage";
const EVENT_HEAL_BUCKET_RESULT: &str = "heal_bucket_result";
const EVENT_HEAL_METADATA_STAGE: &str = "heal_metadata_stage";
const EVENT_HEAL_METADATA_RESULT: &str = "heal_metadata_result";
const EVENT_HEAL_EC_DECODE_STAGE: &str = "heal_ec_decode_stage";
const EVENT_HEAL_EC_DECODE_RESULT: &str = "heal_ec_decode_result";
const EVENT_HEAL_ERASURE_SET_STAGE: &str = "heal_erasure_set_stage";
const EVENT_HEAL_ERASURE_SET_RESULT: &str = "heal_erasure_set_result";

/// Heal type
#[derive(Debug, Clone)]
pub enum HealType {
    /// Cluster heal
    Cluster,
    /// Object heal
    Object {
        bucket: String,
        object: String,
        version_id: Option<String>,
    },
    /// Bucket heal
    Bucket { bucket: String },
    /// Prefix heal
    Prefix { bucket: String, prefix: String },
    /// Erasure Set heal (includes disk format repair)
    ErasureSet { buckets: Vec<String>, set_disk_id: String },
    /// Metadata heal
    Metadata { bucket: String, object: String },
    /// EC decode heal
    ECDecode {
        bucket: String,
        object: String,
        version_id: Option<String>,
    },
}

impl HealType {
    pub(crate) fn kind_label(&self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Object { .. } => "object",
            Self::Bucket { .. } => "bucket",
            Self::Prefix { .. } => "prefix",
            Self::ErasureSet { .. } => "erasure_set",
            Self::Metadata { .. } => "metadata",
            Self::ECDecode { .. } => "ec_decode",
        }
    }

    /// Task kinds enqueued at per-object granularity (MRF, autoheal, scanner,
    /// read-repair loops; the MRF loop queues Object/ECDecode/Metadata
    /// tasks). Their lifecycle and admission logs stay at `debug!`
    /// so a recovery loop queuing hundreds of thousands of object heal tasks
    /// cannot amplify into per-object `info!`/`warn!` lines; aggregate kinds
    /// (cluster/bucket/prefix/erasure-set) keep operator-visible levels.
    pub(crate) fn is_per_object(&self) -> bool {
        matches!(self, Self::Object { .. } | Self::Metadata { .. } | Self::ECDecode { .. })
    }
}

fn is_object_level_not_found_error(err: &Error) -> bool {
    match err {
        Error::Disk(DiskError::FileNotFound | DiskError::FileVersionNotFound) => true,
        Error::Storage(EcstoreError::FileNotFound | EcstoreError::FileVersionNotFound) => true,
        Error::Other(message) => matches!(message.as_str(), "File not found" | "File version not found"),
        _ => false,
    }
}

pub(crate) fn is_missing_object_dir_heal_result(object: &str, err: &Error) -> bool {
    object.ends_with(SLASH_SEPARATOR) && is_object_level_not_found_error(err)
}

/// Sample cap for per-object failure logs during a sweep: returns true (and
/// consumes a sample slot) for the first [`MAX_BUCKET_FAILURE_LOG_SAMPLES`]
/// calls, false afterwards so callers demote the remaining occurrences to
/// `debug!`. Aggregate failed/skipped counts still surface in end-of-sweep
/// summaries.
pub(crate) fn take_failure_log_sample(samples_logged: &mut u64) -> bool {
    if *samples_logged < MAX_BUCKET_FAILURE_LOG_SAMPLES {
        *samples_logged = samples_logged.saturating_add(1);
        true
    } else {
        false
    }
}

/// Heal priority
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum HealPriority {
    /// Low priority
    Low = 0,
    /// Normal priority
    #[default]
    Normal = 1,
    /// High priority
    High = 2,
    /// Urgent priority
    Urgent = 3,
}

impl HealPriority {
    fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Urgent => "urgent",
        }
    }
}

/// Heal options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealOptions {
    /// Scan mode
    pub scan_mode: HealScanMode,
    /// Whether to remove corrupted data
    pub remove_corrupted: bool,
    /// Whether to recreate
    pub recreate_missing: bool,
    /// Whether to update parity
    pub update_parity: bool,
    /// Whether to recursively process
    pub recursive: bool,
    /// Whether to dry run
    pub dry_run: bool,
    /// Whether to skip namespace locking
    #[serde(default)]
    pub no_lock: bool,
    /// Aggregate execution timeout across recoverable manager retries
    pub timeout: Option<Duration>,
    /// pool index
    pub pool_index: Option<usize>,
    /// set index
    pub set_index: Option<usize>,
}

impl Default for HealOptions {
    fn default() -> Self {
        Self {
            scan_mode: HealScanMode::Normal,
            remove_corrupted: false,
            recreate_missing: true,
            update_parity: true,
            recursive: false,
            dry_run: false,
            no_lock: false,
            timeout: None,
            pool_index: None,
            set_index: None,
        }
    }
}

impl HealOptions {
    pub(crate) fn set_key(&self) -> Option<String> {
        match (self.pool_index, self.set_index) {
            (Some(pool), Some(set)) => Some(format!("pool_{pool}_set_{set}")),
            _ => None,
        }
    }

    pub(crate) fn set_metric_label(&self) -> String {
        self.set_key().unwrap_or_else(|| "global".to_string())
    }
}

/// Heal task status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HealTaskStatus {
    /// Pending
    Pending,
    /// Running
    Running,
    /// Retrying after a recoverable failure
    Retrying { error: String, retry_attempt: u32 },
    /// Completed
    Completed,
    /// Failed
    Failed { error: String },
    /// Cancelled
    Cancelled,
    /// Timeout
    Timeout,
}

#[derive(Debug)]
pub(crate) struct BatchHealFailure {
    pub(crate) scope: String,
    pub(crate) failed: u64,
    pub(crate) retryable: u64,
    pub(crate) permanent: u64,
    pub(crate) first_object: String,
    pub(crate) first_error: String,
}

impl std::fmt::Display for BatchHealFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Heal batch failed for {}: {} failed ({} retryable, {} permanent); first failure at {}: {}",
            self.scope, self.failed, self.retryable, self.permanent, self.first_object, self.first_error
        )
    }
}

/// Heal request
#[derive(Debug, Clone)]
pub struct HealRequest {
    /// Request ID
    pub id: String,
    /// Heal type
    pub heal_type: HealType,
    /// Heal options
    pub options: HealOptions,
    /// Priority
    pub priority: HealPriority,
    /// Origin of the request for operational status.
    pub source: HealRequestSource,
    /// Whether this request should bypass queue admission dedup/full policies.
    pub force_start: bool,
    /// Number of recoverable retry attempts already scheduled for this request.
    pub retry_attempts: u32,
    /// Endpoints of the disks being rebuilt by an erasure-set heal. Used to
    /// write per-disk healing markers so `DiskInfo.healing` reflects reality;
    /// empty when the trigger doesn't know the specific disks (admin API,
    /// unclean-shutdown verification).
    pub heal_endpoints: Vec<String>,
    /// Created time
    pub created_at: SystemTime,
    /// Queue admission time used for scheduler delay metrics
    pub enqueued_at: SystemTime,
}

impl HealRequest {
    pub fn new(heal_type: HealType, options: HealOptions, priority: HealPriority) -> Self {
        let now = SystemTime::now();
        Self {
            id: Uuid::new_v4().to_string(),
            heal_type,
            options,
            priority,
            source: HealRequestSource::Internal,
            force_start: false,
            retry_attempts: 0,
            heal_endpoints: Vec::new(),
            created_at: now,
            enqueued_at: now,
        }
    }

    pub fn object(bucket: String, object: String, version_id: Option<String>) -> Self {
        Self::new(
            HealType::Object {
                bucket,
                object,
                version_id,
            },
            HealOptions::default(),
            HealPriority::Normal,
        )
    }

    pub fn bucket(bucket: String) -> Self {
        Self::new(HealType::Bucket { bucket }, HealOptions::default(), HealPriority::Normal)
    }

    pub fn metadata(bucket: String, object: String) -> Self {
        Self::new(HealType::Metadata { bucket, object }, HealOptions::default(), HealPriority::High)
    }

    pub fn ec_decode(bucket: String, object: String, version_id: Option<String>) -> Self {
        Self::new(
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            },
            HealOptions::default(),
            HealPriority::Urgent,
        )
    }
}

/// Heal task
/// Incremental view over a task's retained result items (HS-06).
///
/// `next_seq` is the cursor a client should pass on its next poll; `min_seq`
/// is the oldest sequence still retained; `lagged` means the client's cursor
/// fell behind `min_seq` and items were skipped — the client should restart
/// from `min_seq`.
#[derive(Debug, Clone)]
pub struct HealResultWindow {
    pub items: Vec<HealResultItem>,
    pub next_seq: u64,
    pub min_seq: u64,
    pub lagged: bool,
}

pub struct HealTask {
    /// Task ID
    pub id: String,
    /// Heal type
    pub heal_type: HealType,
    /// Heal options
    pub options: HealOptions,
    /// Priority inherited from the request
    pub priority: HealPriority,
    /// Origin inherited from the request
    pub source: HealRequestSource,
    /// Number of recoverable retry attempts already scheduled for this task.
    pub retry_attempts: u32,
    /// Endpoints of the disks being rebuilt (see `HealRequest::heal_endpoints`).
    pub heal_endpoints: Vec<String>,
    /// Durable resume anchor injected by the manager for an existing automatic
    /// replacement generation.
    replacement_resume_endpoint: Option<String>,
    /// Task status
    pub status: Arc<RwLock<HealTaskStatus>>,
    /// Progress tracking
    pub progress: Arc<RwLock<HealProgress>>,
    /// Result items collected from storage heal calls, each stamped with a
    /// monotonically increasing sequence number for incremental consumption
    /// (the client passes the last seen seq back and receives only newer
    /// items; see `get_result_items_since`).
    pub result_items: Arc<RwLock<VecDeque<(u64, HealResultItem)>>>,
    /// Next sequence number to assign; starts at 1.
    next_item_seq: Arc<AtomicU64>,
    /// Sequence number of the oldest item still inside the retention window;
    /// equals `next_item_seq` while the window is empty.
    min_available_seq: Arc<AtomicU64>,
    result_items_truncated: Arc<AtomicBool>,
    batch_failure: Arc<RwLock<Option<BatchHealFailure>>>,
    batch_failure_recorded: Arc<AtomicBool>,
    /// Created time
    pub created_at: SystemTime,
    /// Queue admission time
    pub enqueued_at: SystemTime,
    /// Started time
    pub started_at: Arc<RwLock<Option<SystemTime>>>,
    /// Completed time
    pub completed_at: Arc<RwLock<Option<SystemTime>>>,
    /// Task start instant for timeout calculation (monotonic)
    task_start_instant: Arc<RwLock<Option<Instant>>>,
    /// Cancel token
    pub cancel_token: tokio_util::sync::CancellationToken,
    /// Storage layer interface
    pub storage: Arc<dyn HealStorageAPI>,
}

impl HealTask {
    async fn verify_replacement_identity_fence(
        &self,
        expected_identities: &[ReplacementTargetIdentity],
        set_disk_id: &str,
        stage: &str,
    ) -> Result<()> {
        let actual_identities = self
            .await_with_control(self.storage.replacement_target_identities(&self.heal_endpoints))
            .await?;
        if replacement_target_identities_match(expected_identities, &actual_identities) {
            return Ok(());
        }

        Err(Error::TaskExecutionFailed {
            message: format!("Replacement target changed during {stage} for automatic heal {set_disk_id}"),
        })
    }

    pub fn from_request(request: HealRequest, storage: Arc<dyn HealStorageAPI>) -> Self {
        Self {
            id: request.id,
            heal_type: request.heal_type,
            options: request.options,
            priority: request.priority,
            source: request.source,
            retry_attempts: request.retry_attempts,
            heal_endpoints: request.heal_endpoints,
            replacement_resume_endpoint: None,
            status: Arc::new(RwLock::new(HealTaskStatus::Pending)),
            progress: Arc::new(RwLock::new(HealProgress::new())),
            result_items: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_RETAINED_HEAL_RESULT_ITEMS))),
            next_item_seq: Arc::new(AtomicU64::new(1)),
            min_available_seq: Arc::new(AtomicU64::new(1)),
            result_items_truncated: Arc::new(AtomicBool::new(false)),
            batch_failure: Arc::new(RwLock::new(None)),
            batch_failure_recorded: Arc::new(AtomicBool::new(false)),
            created_at: request.created_at,
            enqueued_at: request.enqueued_at,
            started_at: Arc::new(RwLock::new(None)),
            completed_at: Arc::new(RwLock::new(None)),
            task_start_instant: Arc::new(RwLock::new(None)),
            cancel_token: tokio_util::sync::CancellationToken::new(),
            storage,
        }
    }

    pub fn retry_request(&self) -> HealRequest {
        HealRequest {
            id: self.id.clone(),
            heal_type: self.heal_type.clone(),
            options: self.options.clone(),
            priority: self.priority,
            source: self.source,
            force_start: false,
            retry_attempts: self.retry_attempts.saturating_add(1),
            heal_endpoints: self.heal_endpoints.clone(),
            created_at: self.created_at,
            enqueued_at: SystemTime::now(),
        }
    }

    pub(crate) async fn retry_request_with_remaining_timeout(&self) -> Result<HealRequest> {
        let mut request = self.retry_request();
        if self.options.timeout.is_some() {
            request.options.timeout = self.remaining_timeout().await?;
        }
        Ok(request)
    }

    pub(crate) fn from_replacement_recovery_request(
        request: HealRequest,
        storage: Arc<dyn HealStorageAPI>,
        replacement_resume_endpoint: Option<String>,
    ) -> Self {
        let mut task = Self::from_request(request, storage);
        task.replacement_resume_endpoint = replacement_resume_endpoint;
        task
    }

    pub fn metric_type_label(&self) -> &'static str {
        self.heal_type.kind_label()
    }

    pub(crate) fn has_batch_failure(&self) -> bool {
        self.batch_failure_recorded.load(Ordering::Acquire)
    }

    pub(crate) async fn record_batch_failure(&self, failure: BatchHealFailure) -> Error {
        self.batch_failure_recorded.store(true, Ordering::Release);
        let message = failure.to_string();
        *self.batch_failure.write().await = Some(failure);
        Error::TaskExecutionFailed { message }
    }

    async fn take_batch_failure(&self) -> Option<BatchHealFailure> {
        self.batch_failure.write().await.take()
    }

    pub fn metric_set_label(&self) -> String {
        match &self.heal_type {
            HealType::ErasureSet { set_disk_id, .. } => set_disk_id.clone(),
            _ => self.options.set_metric_label(),
        }
    }

    fn emit_trace_task_state(&self, state: &'static str, duration: Duration, error: Option<&Error>) {
        trace_emit(|| {
            let mut event = TraceEvent::new(TraceKind::Heal, TraceFunc::HealTask)
                .with_duration(duration)
                .with_attr("task_id", self.id.as_str())
                .with_attr("heal_type", self.heal_type.kind_label())
                .with_attr("state", state)
                .with_attr("source", self.source.as_str())
                .with_attr("priority", self.priority.as_str())
                .with_attr("retry_attempts", u64::from(self.retry_attempts))
                .with_attr("dry_run", self.options.dry_run);

            event = match &self.heal_type {
                HealType::Cluster => event,
                HealType::Object {
                    bucket,
                    object,
                    version_id,
                } => {
                    let event = event.with_bucket(bucket.as_str()).with_object(object.as_str());
                    match version_id {
                        Some(version_id) => event.with_attr("version_id", version_id.as_str()),
                        None => event,
                    }
                }
                HealType::Bucket { bucket } => event.with_bucket(bucket.as_str()),
                HealType::Prefix { bucket, prefix } => event.with_bucket(bucket.as_str()).with_object(prefix.as_str()),
                HealType::ErasureSet { buckets, set_disk_id } => {
                    let bucket_count = u64::try_from(buckets.len()).unwrap_or(u64::MAX);
                    event
                        .with_attr("set_disk_id", set_disk_id.as_str())
                        .with_attr("bucket_count", bucket_count)
                }
                HealType::Metadata { bucket, object } => event.with_bucket(bucket.as_str()).with_object(object.as_str()),
                HealType::ECDecode {
                    bucket,
                    object,
                    version_id,
                } => {
                    let event = event.with_bucket(bucket.as_str()).with_object(object.as_str());
                    match version_id {
                        Some(version_id) => event.with_attr("version_id", version_id.as_str()),
                        None => event,
                    }
                }
            };

            match error {
                Some(error) => event.with_attr("error", error.to_string()),
                None => event,
            }
        });
    }

    async fn remaining_timeout(&self) -> Result<Option<Duration>> {
        if let Some(total) = self.options.timeout {
            let start_instant = { *self.task_start_instant.read().await };
            if let Some(started_at) = start_instant {
                let elapsed = started_at.elapsed();
                if elapsed >= total {
                    return Err(Error::TaskTimeout);
                }
                return Ok(Some(total - elapsed));
            }
            Ok(Some(total))
        } else {
            Ok(None)
        }
    }

    async fn check_control_flags(&self) -> Result<()> {
        if self.cancel_token.is_cancelled() {
            return Err(Error::TaskCancelled);
        }
        // Only interested in propagating an error if the timeout has expired;
        // the actual Duration value is not needed here
        let _ = self.remaining_timeout().await?;
        Ok(())
    }

    async fn await_with_control<F, T>(&self, fut: F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send,
        T: Send,
    {
        let cancel_token = self.cancel_token.clone();
        if let Some(remaining) = self.remaining_timeout().await? {
            if remaining.is_zero() {
                return Err(Error::TaskTimeout);
            }
            let mut fut = Box::pin(fut);
            tokio::select! {
                _ = cancel_token.cancelled() => Err(Error::TaskCancelled),
                _ = tokio::time::sleep(remaining) => Err(Error::TaskTimeout),
                result = &mut fut => result,
            }
        } else {
            tokio::select! {
                _ = cancel_token.cancelled() => Err(Error::TaskCancelled),
                result = fut => result,
            }
        }
    }

    async fn skip_due_to_transient_object_exists(&self, bucket: &str, object: &str, err: &Error) -> Result<()> {
        warn!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_RESULT,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            result = "transient_skip",
            error = %err,
            "Heal object skipped due to transient existence check error"
        );

        let mut progress = self.progress.write().await;
        progress.set_current_object(Some(format!("skipped: {bucket}/{object}")));
        progress.update_stage(1, 1);
        Ok(())
    }

    fn is_data_usage_cache_object(bucket: &str, object: &str) -> bool {
        bucket == RUSTFS_META_BUCKET
            && object
                .strip_prefix(BUCKET_META_PREFIX)
                .and_then(|suffix| suffix.strip_prefix('/'))
                .is_some_and(|name| name.contains(DATA_USAGE_CACHE_NAME))
    }

    fn is_transient_lock_or_timeout_error(err: &Error) -> bool {
        let message = err.to_string().to_ascii_lowercase();
        message.contains("lock acquisition timeout")
            || message.contains("lock acquisition failed")
            || message.contains("timed out")
            || message.contains("deadline has elapsed")
    }

    fn should_skip_data_usage_cache_heal_error(bucket: &str, object: &str, err: &Error) -> bool {
        Self::is_data_usage_cache_object(bucket, object) && Self::is_transient_lock_or_timeout_error(err)
    }

    fn is_no_heal_required_error(err: &Error) -> bool {
        match err {
            Error::Storage(EcstoreError::NoHealRequired) | Error::Disk(DiskError::NoHealRequired) => true,
            Error::Other(message) => matches!(message.as_str(), "No heal required" | "No healing is required"),
            _ => matches!(err.to_string().as_str(), "No heal required" | "No healing is required"),
        }
    }

    fn is_object_not_found_heal_error(err: &Error) -> bool {
        match err {
            Error::Disk(DiskError::FileNotFound | DiskError::FileVersionNotFound) => true,
            Error::Storage(
                EcstoreError::FileNotFound
                | EcstoreError::FileVersionNotFound
                | EcstoreError::ObjectNotFound(_, _)
                | EcstoreError::VersionNotFound(_, _, _),
            ) => true,
            Error::Other(message) => {
                message.contains("File not found")
                    || message.contains("file not found")
                    || message.contains("File version not found")
                    || message.contains("file version not found")
                    || message.contains("Object not found")
                    || message.contains("object not found")
            }
            _ => false,
        }
    }

    fn bucket_object_retry_delay(&self, retry_attempt: u32) -> Duration {
        let base = Duration::from_secs(2_u64.saturating_pow(retry_attempt.clamp(1, MAX_BUCKET_OBJECT_HEAL_RETRIES)));
        let jitter_seed = self
            .id
            .bytes()
            .fold(0_u64, |acc, byte| acc.wrapping_mul(31).wrapping_add(u64::from(byte)));
        Duration::from_millis(jitter_seed % 500).saturating_add(base)
    }

    fn should_return_typed_heal_error(err: &Error) -> bool {
        matches!(err, Error::Storage(_) | Error::Disk(_))
    }

    async fn skip_data_usage_cache_heal_error(&self, bucket: &str, object: &str, err: &Error) -> bool {
        if !Self::should_skip_data_usage_cache_heal_error(bucket, object, err) {
            return false;
        }

        warn!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_RESULT,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            result = "data_usage_cache_transient_skip",
            error = %err,
            "Heal object skipped for data usage cache after transient error"
        );
        let mut progress = self.progress.write().await;
        progress.update_stage(3, 3);
        true
    }

    async fn skip_scanner_synthetic_object_dir_missing(&self, bucket: &str, object: &str, err: &Error) -> bool {
        if self.source != HealRequestSource::Scanner || !is_missing_object_dir_heal_result(object, err) {
            return false;
        }

        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_OBJECT_RESULT,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            task_id = %self.id,
            bucket,
            object,
            source = self.source.as_str(),
            result = "synthetic_object_dir_missing",
            error = %err,
            "Heal recreate skipped scanner synthetic object-dir candidate after object-level not-found"
        );
        let mut progress = self.progress.write().await;
        progress.set_current_object(Some(format!("skipped: {bucket}/{object}")));
        progress.update_stage(4, 4);
        true
    }

    #[tracing::instrument(skip(self), fields(task_id = %self.id, heal_type = ?self.heal_type))]
    #[hotpath::measure]
    pub async fn execute(&self) -> Result<()> {
        // update status and timestamps atomically to avoid race conditions
        let now = SystemTime::now();
        let start_instant = Instant::now();
        let queue_delay = now.duration_since(self.enqueued_at).unwrap_or_default();
        let type_label = self.metric_type_label().to_string();
        let set_label = self.metric_set_label();
        {
            let mut status = self.status.write().await;
            let mut started_at = self.started_at.write().await;
            let mut task_start_instant = self.task_start_instant.write().await;
            *status = HealTaskStatus::Running;
            *started_at = Some(now);
            *task_start_instant = Some(start_instant);
        }

        histogram!(
            "rustfs_heal_queue_delay_seconds",
            "type" => type_label.clone(),
            "set" => set_label.clone()
        )
        .record(queue_delay.as_secs_f64());
        counter!(
            "rustfs_heal_task_start_total",
            "type" => type_label,
            "set" => set_label
        )
        .increment(1);

        demote_to_debug_when!(self.heal_type.is_per_object(), info, target: "rustfs::heal::task", {
            event = EVENT_HEAL_TASK_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            heal_type = self.heal_type.kind_label(),
            state = "started",
            queue_delay = ?queue_delay,
            "Heal task started"
        });
        self.emit_trace_task_state("started", Duration::ZERO, None);

        let result = match &self.heal_type {
            HealType::Cluster => self.heal_cluster().await,
            HealType::Object {
                bucket,
                object,
                version_id,
            } => self.heal_object(bucket, object, version_id.as_deref()).await,
            HealType::Bucket { bucket } => self.heal_bucket(bucket).await,
            HealType::Prefix { bucket, prefix } => self.heal_prefix(bucket, prefix).await,

            HealType::Metadata { bucket, object } => self.heal_metadata(bucket, object).await,
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            } => self.heal_ec_decode(bucket, object, version_id.as_deref()).await,
            HealType::ErasureSet { buckets, set_disk_id } => self.heal_erasure_set(buckets.clone(), set_disk_id.clone()).await,
        };

        // update completed time and status
        {
            let mut completed_at = self.completed_at.write().await;
            *completed_at = Some(SystemTime::now());
        }

        match &result {
            Ok(_) => {
                // A stage can reach its final step before the durable resume
                // ledger and cleanup fences commit. Publish terminal 100 only
                // after the enclosing operation has returned success.
                self.progress.write().await.mark_completed();
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Completed;
                demote_to_debug_when!(self.heal_type.is_per_object(), info, target: "rustfs::heal::task", {
                    event = EVENT_HEAL_TASK_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    heal_type = self.heal_type.kind_label(),
                    state = "completed",
                    "Heal task completed"
                });
            }
            Err(Error::TaskCancelled) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Cancelled;
                info!(
                    target: "rustfs::heal::task",
                    event = EVENT_HEAL_TASK_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    heal_type = self.heal_type.kind_label(),
                    state = "cancelled",
                    "Heal task cancelled"
                );
            }
            Err(Error::TaskTimeout) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Timeout;
                demote_to_debug_when!(self.heal_type.is_per_object(), warn, target: "rustfs::heal::task", {
                    event = EVENT_HEAL_TASK_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    heal_type = self.heal_type.kind_label(),
                    state = "timed_out",
                    "Heal task timed out"
                });
            }
            Err(e) => {
                let mut status = self.status.write().await;
                *status = HealTaskStatus::Failed { error: e.to_string() };
                // Per-object failures are already logged with full object
                // context by the heal_* implementations and terminally by the
                // scheduler's task_failed error!; this generic duplicate would
                // multiply every failed object by the retry count.
                demote_to_debug_when!(self.heal_type.is_per_object(), error, target: "rustfs::heal::task", {
                    event = EVENT_HEAL_TASK_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_TASK,
                    task_id = %self.id,
                    heal_type = self.heal_type.kind_label(),
                    state = "failed",
                    error = %e,
                    "Heal task failed"
                });
            }
        }

        let terminal_state = match &result {
            Ok(_) => "completed",
            Err(Error::TaskCancelled) => "cancelled",
            Err(Error::TaskTimeout) => "timed_out",
            Err(_) => "failed",
        };
        self.emit_trace_task_state(terminal_state, start_instant.elapsed(), result.as_ref().err());

        result
    }

    pub async fn cancel(&self) -> Result<()> {
        self.cancel_token.cancel();
        let mut status = self.status.write().await;
        *status = HealTaskStatus::Cancelled;
        debug!(
            target: "rustfs::heal::task",
            event = EVENT_HEAL_TASK_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_TASK,
            task_id = %self.id,
            heal_type = self.heal_type.kind_label(),
            state = "cancelled",
            source = "manual",
            "Heal task cancellation requested"
        );
        Ok(())
    }

    pub async fn get_status(&self) -> HealTaskStatus {
        self.status.read().await.clone()
    }

    pub async fn get_progress(&self) -> HealProgress {
        self.progress.read().await.clone()
    }

    pub async fn get_result_items(&self) -> Vec<HealResultItem> {
        self.result_items.read().await.iter().map(|(_, item)| item.clone()).collect()
    }

    /// Sequence-stamped retained window, used when archiving a completed
    /// task so incremental cursors survive the transition (HS-06).
    pub async fn get_seqed_result_items(&self) -> Vec<(u64, HealResultItem)> {
        self.result_items.read().await.iter().cloned().collect::<Vec<_>>()
    }

    /// Sequence cursors of the retained window (next to assign, oldest
    /// retained) — the same pair `get_result_items_since` reports, without
    /// copying the items. Used when archiving a finished task.
    pub fn result_seq_cursors(&self) -> (u64, u64) {
        (self.next_item_seq.load(Ordering::Relaxed), self.min_available_seq.load(Ordering::Relaxed))
    }

    /// Incremental result window (HS-06): `since = None` returns the full
    /// retained window (legacy snapshot semantics); `since = Some(seq)`
    /// returns only items stamped with a sequence greater than `seq`.
    /// `lagged` warns that the caller's cursor fell behind the window start
    /// and items were skipped (the response carries `min_seq` as the catch-up
    /// cursor).
    pub async fn get_result_items_since(&self, since: Option<u64>) -> HealResultWindow {
        let result_items = self.result_items.read().await;
        let next_seq = self.next_item_seq.load(Ordering::Relaxed);
        let min_seq = self.min_available_seq.load(Ordering::Relaxed);
        let mut lagged = false;
        let items = match since {
            None => result_items.iter().map(|(_, item)| item.clone()).collect::<Vec<_>>(),
            Some(cursor) => {
                if cursor + 1 < min_seq {
                    lagged = true;
                }
                result_items
                    .iter()
                    .filter(|(seq, _)| *seq > cursor)
                    .map(|(_, item)| item.clone())
                    .collect::<Vec<_>>()
            }
        };
        HealResultWindow {
            items,
            next_seq,
            min_seq,
            lagged,
        }
    }

    pub fn result_items_truncated(&self) -> bool {
        self.result_items_truncated.load(Ordering::Relaxed)
    }

    async fn record_result_item(&self, result: HealResultItem) {
        let seq = self.next_item_seq.fetch_add(1, Ordering::Relaxed);
        let mut result_items = self.result_items.write().await;
        if result_items.len() < MAX_RETAINED_HEAL_RESULT_ITEMS {
            result_items.push_back((seq, result));
        } else {
            // Slide the window: the oldest item leaves and the cursor for the
            // oldest still-available item moves forward with it.
            result_items.pop_front();
            self.min_available_seq
                .store(result_items.front().map_or(seq, |(oldest, _)| *oldest), Ordering::Relaxed);
            result_items.push_back((seq, result));
            self.result_items_truncated.store(true, Ordering::Relaxed);
        }
    }
}

impl std::fmt::Debug for HealTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealTask")
            .field("id", &self.id)
            .field("heal_type", &self.heal_type)
            .field("options", &self.options)
            .field("created_at", &self.created_at)
            .finish()
    }
}

mod heal_bucket;
mod heal_erasure_set;
mod heal_metadata;
mod heal_object;

#[cfg(test)]
mod tests;
